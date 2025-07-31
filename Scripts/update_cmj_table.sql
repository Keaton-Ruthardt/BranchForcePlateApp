-- Update CMJ Results Table Schema
-- Run this query in BigQuery to update the table schema

-- First, let's check the current schema
SELECT column_name, data_type 
FROM `vald-ref-data.athlete_performance_db.INFORMATION_SCHEMA.COLUMNS` 
WHERE table_name = 'cmj_results'
ORDER BY ordinal_position;

-- Update the column name from CON_P2_CON_P1_IMPULSE_RATIO_Trial_ to CON_P2_CON_P1_IMPULSE_RATIO_Trial
ALTER TABLE `vald-ref-data.athlete_performance_db.cmj_results` 
RENAME COLUMN CON_P2_CON_P1_IMPULSE_RATIO_Trial_ TO CON_P2_CON_P1_IMPULSE_RATIO_Trial;

-- Verify the change
SELECT column_name, data_type 
FROM `vald-ref-data.athlete_performance_db.INFORMATION_SCHEMA.COLUMNS` 
WHERE table_name = 'cmj_results' 
AND column_name LIKE '%CON_P2_CON_P1_IMPULSE_RATIO%'
ORDER BY ordinal_position; 