-- Create CMJ Results Table with All 19 Metrics
-- Run this query in BigQuery to create the table schema
-- This schema matches exactly what the enhanced_cmj_processor.py will upload

CREATE TABLE IF NOT EXISTS `vald-ref-data.athlete_performance_db.cmj_results` (
  -- Standard fields
  result_id STRING NOT NULL,
  assessment_id STRING NOT NULL,
  athlete_name STRING,
  test_date DATE,
  age_at_test INT64,
  cmj_composite_score FLOAT64,
  
  -- All 19 CMJ metrics (exact column names that will be uploaded)
  CONCENTRIC_IMPULSE_Trial_Ns FLOAT64,
  ECCENTRIC_BRAKING_RFD_Trial_N_s FLOAT64,
  PEAK_CONCENTRIC_FORCE_Trial_N FLOAT64,
  BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg FLOAT64,
  RSI_MODIFIED_Trial_RSI_mod FLOAT64,
  ECCENTRIC_BRAKING_IMPULSE_Trial_Ns FLOAT64,
  BODY_WEIGHT_LBS_Trial_lb FLOAT64,
  CONCENTRIC_DURATION_Trial_ms FLOAT64,
  CONCENTRIC_RFD_Trial_N_s FLOAT64,
  JUMP_HEIGHT_IMP_MOM_Trial_cm FLOAT64,
  PEAK_TAKEOFF_POWER_Trial_W FLOAT64,
  CONCENTRIC_IMPULSE_P1_Trial_Ns FLOAT64,
  CONCENTRIC_IMPULSE_P2_Trial_Ns FLOAT64,
  RSI_MODIFIED_IMP_MOM_Trial_RSI_mod FLOAT64,
  CON_P2_CON_P1_IMPULSE_RATIO_Trial_ FLOAT64,
  CONCENTRIC_IMPULSE_Asym_Ns FLOAT64,
  ECCENTRIC_BRAKING_IMPULSE_Asym_Ns FLOAT64,
  CONCENTRIC_IMPULSE_P1_Asym_Ns FLOAT64,
  CONCENTRIC_IMPULSE_P2_Asym_Ns FLOAT64
);

-- Optional: Add description to the table
ALTER TABLE `vald-ref-data.athlete_performance_db.cmj_results` 
SET OPTIONS(
  description = 'CMJ test results with all 19 metrics and composite scores'
);

-- Optional: Add descriptions to columns
ALTER TABLE `vald-ref-data.athlete_performance_db.cmj_results` 
SET OPTIONS(
  labels = [
    ('description', 'CMJ Results with 19 metrics'),
    ('data_source', 'VALD ForceDecks'),
    ('composite_score_metrics', '6 metrics used for scoring'),
    ('total_metrics', '19 total metrics stored')
  ]
); 