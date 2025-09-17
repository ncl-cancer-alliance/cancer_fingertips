-- Create table to store information on what fingertips data has been loaded into snowflake
-- Contact: jake.kealey@nhs.net

CREATE OR REPLACE TABLE DATA_LAKE__NCL.FINGERTIPS.INDICATOR_UPDATE_LOG (
    INDICATOR_ID NUMBER,
    DATE_UPDATED_LOCAL DATE,
    LATEST BOOLEAN,    
	_TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
);