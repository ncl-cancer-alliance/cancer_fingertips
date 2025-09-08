-- Create table to store fingertips data for select cancer metrics
-- Contact: jake.kealey@nhs.net

CREATE OR REPLACE TABLE DATA_LAKE__NCL.CANCER__FINGERTIPS.CANCER_FINGERTIPS (
    "Indicator ID" NUMBER,
    "Indicator Name" VARCHAR,
    "Parent Code" VARCHAR,
    "Parent Name" VARCHAR,
    "Area Code" VARCHAR,
    "Area Name" VARCHAR,
    "Area Type" VARCHAR,
    "Sex" VARCHAR,
    "Age" VARCHAR,
    "Category Type" VARCHAR,
    "Category" VARCHAR,
    "Time period" VARCHAR,
    "Value" FLOAT, --This may need to be a VARCHAR?
    "Lower CI 95.0 limit" FLOAT,
    "Upper CI 95.0 limit" FLOAT,
    "Lower CI 99.8 limit" FLOAT,
    "Upper CI 99.8 limit" FLOAT,
    "Count" NUMBER,
    "Denominator" NUMBER,
    "Value note" VARCHAR,
    "Recent Trend" VARCHAR,
    "Compared to England value or percentiles" VARCHAR,
    "Compared to percentiles" VARCHAR,
    "Time period Sortable" NUMBER,
    "New data" VARCHAR,
    "Compared to goal" VARCHAR,
    "Time period range" VARCHAR,
    "DATE_UPDATED_LOCAL" DATE,
    _TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
);