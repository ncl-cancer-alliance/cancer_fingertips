create or replace TABLE DATA_LAKE__NCL.FINGERTIPS.METADATA_AREA (
	AREA_ID NUMBER(38,0),
	"Name" VARCHAR(16777216),
	"Short" VARCHAR(16777216),
	"Class" VARCHAR(16777216),
	"Sequence" NUMBER(38,0),
	"CanBeDisplayedOnMap" BOOLEAN
)COMMENT='Lookup table for Indicator Areas.\nContact: jake.kealey@nhs.net'
;