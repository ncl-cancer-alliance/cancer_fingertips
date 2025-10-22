create or replace view DATA_LAKE__NCL.FINGERTIPS.INDICATOR_UPDATE_LOG(
	INDICATOR_ID,
	AREA_ID,
	AREA_TYPE,
	DATE_UPDATED_LOCAL,
	IS_LATEST,
	_TIMESTAMP
) COMMENT='\nContains a log of when metric data is added to the INDICATOR_DATA table.\n\nContact: jake.kealey@nhs.net\n'
 as 

WITH latest AS (
    SELECT
        "Indicator ID",
        AREA_ID,
        MAX(_TIMESTAMP) AS _TIMESTAMP

    FROM DATA_LAKE__NCL.FINGERTIPS.INDICATOR_DATA
    GROUP BY "Indicator ID", AREA_ID
)

SELECT
    fin."Indicator ID" AS INDICATOR_ID,
    fin.AREA_ID,
    fin."Area Type" AS AREA_TYPE,
    fin.DATE_UPDATED_LOCAL,
    latest._TIMESTAMP IS NOT NULL AS IS_LATEST,
    latest._TIMESTAMP

FROM DATA_LAKE__NCL.FINGERTIPS.INDICATOR_DATA fin

LEFT JOIN latest
ON fin."Indicator ID" = latest."Indicator ID"
AND fin.AREA_ID = latest.AREA_ID
AND fin._TIMESTAMP = latest._TIMESTAMP

GROUP BY fin."Indicator ID", fin.AREA_ID, fin."Area Type", fin.DATE_UPDATED_LOCAL, latest._TIMESTAMP
ORDER BY latest._TIMESTAMP DESC;