create or replace TABLE DATA_LAKE__NCL.FINGERTIPS.INGESTION_ERROR_LOG (
	INDICATOR_ID NUMBER(38,0),
	AREA_ID NUMBER(38,0),
	_TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
)COMMENT='Table to track when the ingestion of an indicator fails.\nContact: jake.kealey@nhs.net'
;