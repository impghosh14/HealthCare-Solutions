CREATE OR REPLACE TABLE HCS_DISEASE_CORE (
    Disease_ID STRING,
    Disease_Name STRING,
    Lab_Parameter STRING,
    Lab_Report_Parameter STRING,
    Normal_Min FLOAT,
    Normal_Max FLOAT,
    Abnormal_Criteria STRING,
    Reason STRING,
    Recommended_Doctor_Specialization STRING,
    SOURCE_FILE STRING,
    INGESTION_ID STRING,
    INGESTION_TS TIMESTAMP
);
