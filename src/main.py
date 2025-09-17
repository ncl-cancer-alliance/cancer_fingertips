#Imports
import ast
import fingertips_py as ftp
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

def check_for_updates(ctx, ids, df_meta):
    """
    For given indicators, compare the online "Date updated" with the local 
    (most recent) "Date updated" values to determine which indicators have new 
    data

    inputs:
    - ctx: Snowflake connection object 
    (https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect)
    - ids: List of indicators to consider
    - df_meta: Dataframe object 
    (sourced using fingertips_py.get_metadata_for_all_indicators_from_csv(),
    I made this a function input instead of pulling the data in the function to 
    avoid pulling it multiple times in the code since it's used elsewhere)

    output: List of indicators with new data available
    """

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

def update_meta_local(ctx, id, date_updated_local):

    """
    Code to maintain a local metadata table tracking what indicator data has
    been ingested. Needed to detect whether the latest data online is more
    recent that what is already ingested.

    This function also marks newly inserted data as "IS_LATEST" and unmarks
    existing entries for this indicator as not "IS_LATEST"

    inputs:
    - ctx: Snowflake connection object 
    (https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect)
    - id: Indicator ID
    - date_updated_local: "Date updated" value for the given Indicator
    """

    #Get destination tables
    database = getenv("DATABASE") 
    schema = getenv("SCHEMA") 
    destination_table = getenv("TABLE_LOCAL_META")
    destination = f"{database}.{schema}.{destination_table}"

    #Update existing entries
    alter_query = f"""
    UPDATE {destination} 
    SET IS_LATEST = False 
    WHERE INDICATOR_ID = {id};
    """

    #Insert new row
    insert_query = f"""
    INSERT INTO {destination} (INDICATOR_ID, DATE_UPDATED_LOCAL, IS_LATEST)
    VALUES ({id}, '{date_updated_local}', True)
    """

    cur = ctx.cursor()

    try:
        cur.execute(alter_query)
        cur.execute(insert_query)
    except Exception as e:
        print("SQL failed with this message:", e)
        #Needed to undo editing existing data as not IS_LATEST if upload fails
        cur.execute("ROLLBACK") 

    finally:
        cur.close()

def main():

    #Get indicator IDs to process
    indicator_ids = ast.literal_eval(getenv("INDICATORS"))

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
    target_ids = check_for_updates(ctx, indicator_ids, df_meta)

    if target_ids == []:
        print("\nNo new indicator data found")

    #Handle each indicator indivdually
    for id in target_ids:
        print(f"\nProcessing {id}")

        #Get data for the target indicator
        df_id = ftp.get_data_for_indicator_at_all_available_geographies(id)

        #Get the update date
        date_updated_local = (
            df_meta[df_meta["Indicator ID"] == id]["Date updated"].values[0])
        
        #Ingest that data (and update local metadata)
        success = ingest_ft_data(ctx, df_id, date_updated_local)
        
        #Update the local meta table if successful
        if success:
            print("Updating local metadata")
            update_meta_local(ctx, id, date_updated_local)

    ctx.close()

#Main parent function for the update process
main()