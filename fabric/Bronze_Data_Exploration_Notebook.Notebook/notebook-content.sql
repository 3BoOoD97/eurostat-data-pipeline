-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "a904ab17-bc40-4b65-8d6f-bccceb88dcd3",
-- META       "default_lakehouse_name": "Eurostat_Asylum_Lakehouse",
-- META       "default_lakehouse_workspace_id": "4b2e584f-8b24-4a80-9eaa-92484d08b3a7",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "a904ab17-bc40-4b65-8d6f-bccceb88dcd3"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- Count rows
SELECT COUNT (*) FROM bronze.asyappctzm;
SELECT COUNT (*) FROM bronze.asydcfstq;
SELECT COUNT (*) FROM bronze.asypenctzm;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- Check if there are duplicate rows 

SELECT (SELECT COUNT(*) FROM bronze.asyappctzm) - (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM bronze.asyappctzm) sub) AS asyappctzm_duplicate_rows_count;
SELECT (SELECT COUNT(*) FROM bronze.asydcfstq) - (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM bronze.asydcfstq) sub) AS asydcfstq_duplicate_rows_count;
SELECT (SELECT COUNT(*) FROM bronze.asypenctzm) - (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM bronze.asypenctzm) sub) AS asypenctzm_duplicate_rows_count;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- asyappctzm exploration
SELECT * FROM bronze.asyappctzm WHERE applicant_type is NULL OR applicant_type = ''
OR applicant_count is NULL OR applicant_count = '' OR applicant_count < 0 
OR country_code is NULL OR country_code =''
OR nationality_code is null or nationality_code = ''
OR time_period IS NULL OR time_period ='' OR time_period NOT REGEXP '^[0-9]{4}-(0[1-9]|1[0-2])$' OR time_period > CURDATE();


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- asydcfstq exploration
SELECT * FROM bronze.asydcfstq WHERE country_code is NULL OR country_code =''
OR nationality_code is null or nationality_code = ''
OR decision is null or decision =''
OR decision_count = '' OR decision_count < 0
OR time_period is null or time_period = '' or time_period NOT REGEXP '^[0-9]{4}-(Q[1-4])$' OR LEFT(time_period, 4) > YEAR(CURDATE())

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- asypenctzm exploration
SELECT * FROM bronze.asypenctzm WHERE country_code is NULL OR country_code =''
OR nationality_code is null or nationality_code = ''
OR pending_count is null or pending_count = '' or pending_count <0
OR time_period is null or time_period = '' or time_period NOT REGEXP '^[0-9]{4}-(0[1-9]|1[0-2])$' OR time_period > CURDATE();

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT nationality_code, country_code, time_period, applicant_type, COUNT(*) AS count_duplicates
FROM bronze.asyappctzm
GROUP BY nationality_code, country_code, time_period, applicant_type
HAVING COUNT(*) > 1;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT nationality_code, country_code, time_period, decision, COUNT(*) AS count_duplicates
FROM bronze.asydcfstq
GROUP BY nationality_code, country_code, time_period, decision
HAVING COUNT(*) > 1;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT nationality_code, country_code, time_period, COUNT(*) AS count_duplicates
FROM bronze.asypenctzm
GROUP BY nationality_code, country_code, time_period
HAVING COUNT(*) > 1;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
