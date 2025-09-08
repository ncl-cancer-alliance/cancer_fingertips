-- Create table to store fingertip area metadata
-- Contact: jake.kealey@nhs.net

CREATE OR REPLACE TABLE DATA_LAKE__NCL.CANCER__FINGERTIPS.METADATA_AREA (
    AREA_ID NUMBER,
    "Name" VARCHAR,
    "Short" VARCHAR,
    "Class" VARCHAR,
    "Sequence" NUMBER,
    "CanBeDisplayedOnMap" BOOLEAN
);