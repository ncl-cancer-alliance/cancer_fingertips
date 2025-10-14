#Imports
import ast
import fingertips_py as ftp
import numpy as np
import pandas as pd

from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

from dotenv import load_dotenv
from os import getenv

load_dotenv(override=True)

def upload_df(ctx, df, destination, replace=False):

    """
    Function to upload a dataframe to Snowflake.

    inputs:
    - ctx: Snowflake connection object 
    (https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect)
    - df: Dataframe object
    - destination: Full table name of the destination
    (e.g. DATABASE_NAME.SCHEMA_NAME.TABLE_NAME)
    - replace: If True, the destination is TRUNCATED before uploading new data
    (If the upload fails, the truncation is rollbacked)

    output:
    Returns Boolean value if the upload was successful
    """

    df = df.copy()
    df.reset_index(drop=True, inplace=True)
    #Needed to prevent "null" strings in the destination
    df = df.where(pd.notnull(df), None)

    cur = ctx.cursor()
    destination_segs = destination.split(".")
    success = False

    try:
        if replace:
            cur.execute(f"TRUNCATE TABLE {destination}")

        # Upload DataFrame
        success, nchunks, nrows, _ = write_pandas(
            conn=ctx,
            df=df,
            table_name=destination_segs[2],
            schema=destination_segs[1],
            database=destination_segs[0],
            overwrite=False
        )

        if not success:
            raise Exception("Failed to write DataFrame to Snowflake.")

        print(f"Uploaded {nrows} rows to {destination}")
    except Exception as e:
        print("Data ingestion failed with error:", e)
        cur.execute("ROLLBACK") #Undoes truncation on upload error

    finally:
        cur.close()
    
    return success

def update_meta_live(ctx, df_meta, destination_table):

    """
    Upload (replace) the existing tables containing online metadata
    (e.g. Indicator metadata, Area metadata)

    inputs:
    - ctx: Snowflake connection object 
    (https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect)
    - df_meta: Dataframe object containing metadata
    - destination_table: Table name for the destination
    """
    
    #Get destination table
    database = getenv("DATABASE") 
    schema = getenv("SCHEMA") 
    destination = f"{database}.{schema}.{destination_table}"

    success = upload_df(ctx, df_meta, destination, replace=True)

def load_query(sql_file_name):

    """
    Load SQL query string from .sql file

    inputs:
    -sql_file_name: (Full path) filename of the .sql file

    output: String containing the SQL query
    """

    data_dir = getenv("DATA_DIR_SCRIPTS")

    sql_full_path = f"./{data_dir}/{sql_file_name}"

    with open(sql_full_path, 'r') as file:
        sql_query = file.read()

    return sql_query

def get_local_meta(ctx):
    
    """
    Function to get local metadata to understand what data is currently ingested

    inputs:
    - ctx: Snowflake connection object 
    (https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect)
    output: Dataframe containing local metadata on ingested data
    """

    query = load_query("get_indicator_update_log.sql")

    print("\n Note: the warning below appears everytime the code is ran and can be ignored.")
    df_local = pd.read_sql(query, ctx)

    df_local.set_index("INDICATOR_ID", inplace=True)
    
    return df_local

def get_area_mismatch(ctx):
    
    """
    Function to get indicator-area pairs with outdated area boundaries.

    inputs:
    - ctx: Snowflake connection object 
    (https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect)
    output: Tuple pairs of indicator ids and area ids with outdated area boundaries.
    """

    query = f"""
        SELECT INDICATOR_ID, AREA_ID 
        FROM {getenv("TABLE_AREA_MISMATCH")}
    """

    df_am = pd.read_sql(query, ctx)
    list_am = df_am.values.tolist()
    
    return [(x, y) for [x,y] in list_am]

def get_ingestion_error(ctx):

    """
    Function to get indicator-area pairs with unresolved entries in the 
    ingestion error log.

    inputs:
    - ctx: Snowflake connection object 
    (https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect)
    output: Tuple pairs of indicator ids and area ids with previous unresolved 
    ingestion errors.
    """

    #Query to get unresolved ingestion errors
    query = f"""
        SELECT DISTINCT iel.INDICATOR_ID, iel.AREA_ID 
        FROM DATA_LAKE__NCL.FINGERTIPS.{getenv("TABLE_INGESTION_ERROR_LOG")} iel

        --Join to indicator data to compare if the data was updated after the error was raised
        LEFT JOIN (
            SELECT "Indicator ID", AREA_ID, MAX(_TIMESTAMP) AS _TIMESTAMP
            FROM DATA_LAKE__NCL.FINGERTIPS.{getenv("TABLE_DATA")}
            GROUP BY "Indicator ID", AREA_ID
        ) fin
        ON iel.INDICATOR_ID = fin."Indicator ID"
        AND iel.AREA_ID = fin.AREA_ID

        WHERE iel._TIMESTAMP >= fin._TIMESTAMP
    """

    df_ie = pd.read_sql(query, ctx)
    list_ie = df_ie.values.tolist()
    
    return [(x, y) for [x,y] in list_ie]

def check_for_updates(ctx, df_meta, ids=[]):
    """
    For given indicators, compare the online "Date updated" with the local 
    (most recent) "Date updated" values to determine which indicators have new 
    data

    inputs:
    - ctx: Snowflake connection object 
    (https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect)
    - df_meta: Dataframe object 
    (sourced using fingertips_py.get_metadata_for_all_indicators_from_csv(),
    I made this a function input instead of pulling the data in the function to 
    avoid pulling it multiple times in the code since it's used elsewhere)
    - ids: List of indicators to consider. If this is not provider, the code 
    will use all active ids from the df_meta object.

    output: List of indicators with new data available
    """

    #Process the ids input
    if ids == []:
        ids = list(df_meta["Indicator ID"])

    #Get local metadata
    df_local_meta = get_local_meta(ctx)

    #Filter the live metadata to relevant indicators
    df_meta_scope = df_meta.copy()
    df_meta_scope = df_meta_scope[["Indicator ID", "Date updated"]]
    df_meta_scope = df_meta_scope.rename(columns={
        "Indicator ID": "INDICATOR_ID",
        "Date updated": "DATE_UPDATED_LIVE"
    })

    #Filter to only the target indicators
    df_meta_scope = df_meta_scope[df_meta_scope["INDICATOR_ID"].isin(ids)]

    #Combine the live and local metadata
    df_meta_scope = df_meta_scope.join(df_local_meta, on="INDICATOR_ID")
    
    #Calculate if the data online (live) is more recent than what is locally available
    df_meta_scope["NEW_DATA"] = (
        (
            df_meta_scope["DATE_UPDATED_LIVE"] > 
            df_meta_scope["DATE_UPDATED_LOCAL"]
        ) | (
            df_meta_scope["DATE_UPDATED_LOCAL"].isnull()
        )
    )

    #Filter to indicators with either no local records or outdated records
    ids_to_update = df_meta_scope["INDICATOR_ID"][df_meta_scope["NEW_DATA"] == True].to_list()

    return ids_to_update

def get_target_pairs(ctx, df_meta, 
                     flag_ui=True, flag_am=True, flag_ie=True):

    """
    Using the three criterias:
    - Updated Indicators: Newer data exists via the API than is locally stored 
      in Snowflake
    - Area Mismatch: Existing data are using outdated boundaries
      (i.e. PCN Groupings)
    - Ingestion Error: Previous attempts at ingesting data failed
    Determine a list of Indicator ID and Area ID pairs to be pulled and ingested
    via the API.

    inputs:
    - ctx: Snowflake connection object 
    (https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect)
    - df_meta: Dataframe object 
    (sourced using fingertips_py.get_metadata_for_all_indicators_from_csv(),
    I made this a function input instead of pulling the data in the function to 
    avoid pulling it multiple times in the code since it's used elsewhere)
    - flag variables that selects which criterias to consider when fetching 
      target pairs

    output: List of Indicator-Area pairs
    """

    target_pairs = []

    if flag_ui:
        #Get list of updated indicators
        updated_indicators = check_for_updates(ctx, df_meta)

        all_area_for_all_indicators = ftp.get_all_areas_for_all_indicators()
        
        #Expand list of updated indicators
        for indicator in updated_indicators:
            areas_to_get = all_area_for_all_indicators.get(indicator)
            indicator_pairs = [(indicator, x) for x in areas_to_get]
            target_pairs += indicator_pairs

    if flag_am:
        #Get target paris from area mismatches
        area_mismatch_pairs = get_area_mismatch(ctx)
        target_pairs += area_mismatch_pairs

    if flag_ie:
        #Get target pairs from ingestion errors
        ingestion_error_pairs = get_ingestion_error(ctx)
        target_pairs += ingestion_error_pairs

    return target_pairs


def ingest_ft_data(ctx, df, date_updated_local):
    """
    Function to ingest a fingertips dataframe.
    Loads the destination table and adds the "Date updated" to the data.

    inputs:
    - ctx: Snowflake connection object 
    (https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect)
    - df: Data Frame containing fingertips metric data
    - date_updated_local: Date of when this metric was updated in fingertips
    """

    #Add the date updated to the data
    df["DATE_UPDATED_LOCAL"] = date_updated_local

    #Get destination tables
    database = getenv("DATABASE") 
    schema = getenv("SCHEMA") 
    destination_table = getenv("TABLE_DATA")
    destination = f"{database}.{schema}.{destination_table}"

    #Upload the new fingertips data
    success = upload_df(ctx, df, destination, replace=False)
    return success

def log_error(ctx, indicator_id, area_id):

    """
    Code to maintain a local table tracking indicator + area combinations that fail to ingest on multiple attempts.
    Such entries will attempt to be uploaded on future executions.

    inputs:
    - ctx: Snowflake connection object 
    (https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect)
    - indicator_id: Indicator ID
    - area_id: Area Type ID
    """

    #Get destination tables
    database = getenv("DATABASE") 
    schema = getenv("SCHEMA") 
    destination_table = getenv("TABLE_INGESTION_ERROR_LOG")
    destination = f"{database}.{schema}.{destination_table}"

    #Insert new row
    insert_query = f"""
    INSERT INTO {destination} (INDICATOR_ID, AREA_ID)
    VALUES ({indicator_id}, {area_id})
    """

    cur = ctx.cursor()

    try:
        cur.execute(insert_query)
    except Exception as e:
        print("SQL failed with this message:", e)
        cur.execute("ROLLBACK") 

    finally:
        cur.close()

def main(limit=False):

    #Get live meta data
    df_meta = ftp.get_metadata_for_all_indicators_from_csv()
    df_area = pd.DataFrame.from_dict(
        ftp.get_all_areas(), orient='index').reset_index(names="AREA_ID")

    #Establish Snowflake connection
    ctx = connect(
        account=getenv("ACCOUNT"),
        user=getenv("USER"),
        authenticator=getenv("AUTHENTICATOR"),
        role=getenv("ROLE"),
        warehouse=getenv("WAREHOUSE"),
        database=getenv("DATABASE"),
        schema=getenv("SCHEMA")
    )

    #Convert date columns to date type
    df_meta["Date updated"] = pd.to_datetime(
        df_meta["Date updated"], format="%d/%m/%Y").dt.date

    #Replace existing live meta table references
    print("\nProcessing latest metadata:")
    
    print("Indicator Metadata...")

    update_meta_live(ctx, df_meta, 
                     destination_table=getenv("TABLE_META_INDICATOR"))
    
    print("Area Metadata...")
    update_meta_live(ctx, df_area,
                     destination_table=getenv("TABLE_META_AREA"))

    #Filter indicators to only ones with new data
    target_pairs = get_target_pairs(ctx, df_meta, 
                                    flag_ui=True, flag_am=True, flag_ie=True)

    if limit and int(limit) < len(target_pairs):
        limit_indicator = target_pairs[limit][0]
        target_pairs = [x for idx, x in enumerate(target_pairs) 
                        if (idx < limit) or (x[0]==limit_indicator)]

    if target_pairs == []:
        print("\nNo new indicator data found")

    #Handle each indicator indivdually
    for idx, pair in enumerate(target_pairs):

        df_id = pd.DataFrame()

        indicator_id = pair[0]
        area_id = pair[1]

        print(f"\n{idx} Processing: Indicator - {indicator_id}, Area - {area_id}")

        ##Custom code to add failsafe to ftp code
        success_area = True
        
        try:
            df_id = ftp.get_data_by_indicator_ids(indicator_id, area_id)
        except:
            print(f"Download failed for id {indicator_id} and area {area_id}. Retrying (2/2).")
            try:
                df_id = ftp.get_data_by_indicator_ids(indicator_id, area_id)
            except:
                print("Download failed again. Data for this area will be skipped.")
                log_error(ctx, indicator_id, area_id)
                success_area = False

        if success_area:

            #Remove duplicated England data
            if area_id != 15:
                df_id = df_id[df_id["Area Code"] != "E92000001"]

            df_id["AREA_ID"] = area_id

        df_id.drop_duplicates(inplace=True)

        if not df_id.empty:
            #Get the update date
            date_updated_local = (
                df_meta[df_meta["Indicator ID"] == indicator_id]["Date updated"].values[0])
            
            #Ingest that data (and update local metadata)
            ingest_ft_data(ctx, df_id, date_updated_local)

        else:
            print(f"No data found for Indicator ID {indicator_id} and Area ID {area_id}.")

    ctx.close()

#Main parent function for the update process
main(limit=200)