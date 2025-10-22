create or replace view DATA_LAKE__NCL.FINGERTIPS.INDICATOR_AREA_MISMATCH_LOG(
	INDICATOR_ID,
	AREA_ID,
	AREA_TYPE_DATA,
	AREA_TYPE_META
) COMMENT='Flags indicators where the area boundaries are not up to date (typically PCN groups).\nIdeally this table is empty.\nContact: jake.kealey@nhs.net'
 as

SELECT DISTINCT 
    fin."Indicator ID" AS INDICATOR_ID, fin.AREA_ID, 
    fin."Area Type" AS AREA_TYPE_DATA, ma."Short" AS AREA_TYPE_META
FROM DATA_LAKE__NCL.FINGERTIPS.INDICATOR_DATA fin

LEFT JOIN DATA_LAKE__NCL.FINGERTIPS.METADATA_AREA ma
ON fin.AREA_ID = ma.AREA_ID

WHERE fin."Area Type" != ma."Short";