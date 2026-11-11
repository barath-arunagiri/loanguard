# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW loanguard.gold.fraud_detection_results_masked AS
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     full_name,
# MAGIC     scenario_type,
# MAGIC
# MAGIC     fraud_score,
# MAGIC     fraud_status,
# MAGIC     total_penalty,
# MAGIC
# MAGIC     flag_duplicate_pan,
# MAGIC     flag_duplicate_aadhaar,
# MAGIC     flag_income_manipulation,
# MAGIC     flag_identity_inconsistency,
# MAGIC     flag_multiple_applications,
# MAGIC     flag_address_mismatch,
# MAGIC     flag_loan_amount_mismatch,
# MAGIC     flag_loan_purpose_mismatch,
# MAGIC     flag_income_not_verified,
# MAGIC     flag_multiple_risk_factors,
# MAGIC
# MAGIC     income_diff_pct,
# MAGIC     loan_diff_pct,
# MAGIC     total_flags,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN pan_number IS NOT NULL
# MAGIC         THEN CONCAT('XXXXXX', RIGHT(pan_number, 4))
# MAGIC         ELSE NULL
# MAGIC     END AS pan_number,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN aadhaar_number IS NOT NULL
# MAGIC         THEN CONCAT(
# MAGIC             'XXXX-XXXX-',
# MAGIC             RIGHT(REGEXP_REPLACE(aadhaar_number, '[^0-9]', ''), 4)
# MAGIC         )
# MAGIC         ELSE NULL
# MAGIC     END AS aadhaar_number,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN phone_number IS NOT NULL
# MAGIC         THEN CONCAT('XXXXXX', RIGHT(phone_number, 4))
# MAGIC         ELSE NULL
# MAGIC     END AS phone_number,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN email IS NOT NULL
# MAGIC         THEN CONCAT(
# MAGIC             LEFT(email, 1),
# MAGIC             '*****',
# MAGIC             SUBSTRING(email, INSTR(email, '@'))
# MAGIC         )
# MAGIC         ELSE NULL
# MAGIC     END AS email
# MAGIC
# MAGIC FROM loanguard.gold.fraud_detection_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from loanguard.gold.fraud_detection_results_masked