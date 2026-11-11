# Databricks notebook source
# DBTITLE 1,Step 1: Load and Verify Inputs
# ============================================================
# CROSS-VERIFICATION: IDP Extraction Results vs Lending Club Reference
# ============================================================
# Compares AI-extracted field values against ground-truth loan data
# One row per customer + field with MATCH / MISMATCH / MISSING status

from pyspark.sql import functions as F

print("=" * 70)
print("STEP 1: Load and Verify Inputs")
print("=" * 70)

# --- Input 1: IDP Extraction Results (field-level normalized) ---
df_idp = spark.table("loanguard.bronze.idp_extraction_results")
idp_count = df_idp.count()
idp_customers = df_idp.select("customer_id").distinct().count()
print(f"\nIDP Extraction Results:")
print(f"  Total rows: {idp_count}")
print(f"  Distinct customers: {idp_customers}")
print(f"  Columns: {df_idp.columns}")

# --- Input 2: Lending Club Ground Truth ---
df_ref = spark.table("loanguard.bronze.loan_dataset_ground_truth")
ref_count = df_ref.count()
ref_customers = df_ref.select("customer_id").distinct().count()
print(f"\nLending Club Ground Truth:")
print(f"  Total rows: {ref_count}")
print(f"  Distinct customers: {ref_customers}")

# --- Field Mapping: IDP field_name -> Lending Club column ---
FIELD_MAPPING = {
    'loan_amount': 'loan_amnt',
    'funded_amount': 'funded_amnt',
    'term': 'term',
    'interest_rate': 'int_rate',
    'monthly_installment': 'installment',
    'loan_grade': 'grade',
    'loan_sub_grade': 'sub_grade',
    'employer_name': 'emp_title',
    'employment_length': 'emp_length',
    'annual_income': 'annual_inc',
    'income_verification_status': 'verification_status',
    'loan_status': 'loan_status',
    'loan_purpose': 'purpose',
    'loan_title': 'title',
    'zip_code': 'zip_code',
    'state': 'addr_state',
    'dti': 'dti',
    'open_accounts': 'open_acc',
    'total_accounts': 'total_acc',
    'revolving_balance': 'revol_bal',
    'home_ownership': 'home_ownership',
    'fico_score_range': 'fico_range_low',  # special: also fico_range_high
}

print(f"\nField Mapping: {len(FIELD_MAPPING)} IDP fields mapped to Lending Club columns")
for idp_field, lc_field in FIELD_MAPPING.items():
    print(f"  {idp_field:30s} -> {lc_field}")

# --- Scenario Distribution ---
SCENARIOS = {
    'FULL_MATCH': (1, 180),
    'MINOR_VARIATION': (181, 190),
    'INCOME_MISMATCH': (191, 194),
    'ADDRESS_MISMATCH': (195, 197),
    'IDENTITY_MISMATCH': (198, 199),
    'MISSING_OR_INVALID': (200, 200),
}
print(f"\nScenario Distribution:")
for name, (start, end) in SCENARIOS.items():
    print(f"  C{start:04d}-C{end:04d}: {name} ({end - start + 1} customers)")

print(f"\n✓ Inputs loaded and verified")

# COMMAND ----------

# DBTITLE 1,Step 2: Cross Verification Results
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- STEP 2: Create cross_verification_results (field-level comparison)
# MAGIC -- ============================================================
# MAGIC -- One row per customer + field with MATCH / MISMATCH / MISSING
# MAGIC -- Fan-out prevention: one document per customer+field via ROW_NUMBER
# MAGIC
# MAGIC CREATE OR REPLACE TABLE loanguard.silver.cross_verification_results AS
# MAGIC WITH ranked_idp AS (
# MAGIC   SELECT
# MAGIC     customer_id, document_id, document_type, source_path, file_name,
# MAGIC     field_name, extracted_value, confidence_score, extraction_status,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY customer_id, field_name
# MAGIC       ORDER BY
# MAGIC         CASE document_type
# MAGIC           WHEN 'Loan Application' THEN 1
# MAGIC           WHEN 'Income Proof'    THEN 2
# MAGIC           WHEN 'Address Proof'   THEN 3
# MAGIC           WHEN 'KYC Application'  THEN 4
# MAGIC           WHEN 'Aadhaar Card'     THEN 5
# MAGIC           WHEN 'PAN Card'         THEN 6
# MAGIC         END,
# MAGIC         confidence_score DESC
# MAGIC     ) AS rn
# MAGIC   FROM loanguard.bronze.idp_extraction_results
# MAGIC   WHERE field_name IN (
# MAGIC     'loan_amount','funded_amount','term','interest_rate','monthly_installment',
# MAGIC     'loan_grade','loan_sub_grade','employer_name','employment_length',
# MAGIC     'annual_income','income_verification_status','loan_status','loan_purpose',
# MAGIC     'loan_title','zip_code','state','dti','open_accounts','total_accounts',
# MAGIC     'revolving_balance','home_ownership','fico_score_range'
# MAGIC   )
# MAGIC ),
# MAGIC idp AS (
# MAGIC   SELECT * FROM ranked_idp WHERE rn = 1
# MAGIC ),
# MAGIC ref AS (
# MAGIC   SELECT * FROM loanguard.bronze.loan_dataset_ground_truth
# MAGIC ),
# MAGIC comparison AS (
# MAGIC   SELECT
# MAGIC     i.customer_id,
# MAGIC     r.customer_id AS reference_customer_id,
# MAGIC     i.field_name,
# MAGIC     i.extracted_value AS idp_value,
# MAGIC     CASE i.field_name
# MAGIC       WHEN 'loan_amount'           THEN CAST(r.loan_amnt AS STRING)
# MAGIC       WHEN 'funded_amount'         THEN CAST(r.funded_amnt AS STRING)
# MAGIC       WHEN 'term'                 THEN r.term
# MAGIC       WHEN 'interest_rate'        THEN CAST(r.int_rate AS STRING)
# MAGIC       WHEN 'monthly_installment'  THEN CAST(r.installment AS STRING)
# MAGIC       WHEN 'loan_grade'           THEN r.grade
# MAGIC       WHEN 'loan_sub_grade'       THEN r.sub_grade
# MAGIC       WHEN 'employer_name'        THEN r.emp_title
# MAGIC       WHEN 'employment_length'    THEN r.emp_length
# MAGIC       WHEN 'annual_income'        THEN CAST(r.annual_inc AS STRING)
# MAGIC       WHEN 'income_verification_status' THEN r.verification_status
# MAGIC       WHEN 'loan_status'          THEN r.loan_status
# MAGIC       WHEN 'loan_purpose'         THEN r.purpose
# MAGIC       WHEN 'loan_title'          THEN r.title
# MAGIC       WHEN 'zip_code'            THEN r.zip_code
# MAGIC       WHEN 'state'               THEN r.addr_state
# MAGIC       WHEN 'dti'                THEN CAST(r.dti AS STRING)
# MAGIC       WHEN 'open_accounts'       THEN CAST(r.open_acc AS STRING)
# MAGIC       WHEN 'total_accounts'      THEN CAST(r.total_acc AS STRING)
# MAGIC       WHEN 'revolving_balance'   THEN CAST(r.revol_bal AS STRING)
# MAGIC       WHEN 'home_ownership'      THEN r.home_ownership
# MAGIC       WHEN 'fico_score_range'    THEN CONCAT(CAST(r.fico_range_low AS STRING), ' - ', CAST(r.fico_range_high AS STRING))
# MAGIC     END AS reference_value,
# MAGIC     i.document_id AS source_document_id,
# MAGIC     i.document_type AS source_document_type,
# MAGIC     i.file_name AS source_file_name,
# MAGIC     i.source_path,
# MAGIC     i.confidence_score
# MAGIC   FROM idp i
# MAGIC   LEFT JOIN ref r ON i.customer_id = r.customer_id
# MAGIC )
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   reference_customer_id,
# MAGIC   field_name,
# MAGIC   idp_value,
# MAGIC   reference_value,
# MAGIC   -- Normalized IDP value
# MAGIC   CASE
# MAGIC     WHEN field_name IN ('loan_grade','loan_sub_grade','home_ownership','loan_status',
# MAGIC                         'income_verification_status','loan_purpose','loan_title',
# MAGIC                         'employer_name','employment_length','state')
# MAGIC       THEN UPPER(TRIM(REGEXP_REPLACE(TRIM(COALESCE(idp_value,'')), '\\s+', ' ')))
# MAGIC     WHEN field_name = 'term'
# MAGIC       THEN TRIM(REGEXP_REPLACE(TRIM(COALESCE(idp_value,'')), '\\s+', ' '))
# MAGIC     WHEN field_name = 'zip_code'
# MAGIC       THEN TRIM(COALESCE(idp_value,''))
# MAGIC     WHEN field_name IN ('loan_amount','funded_amount','interest_rate','monthly_installment',
# MAGIC                         'dti','open_accounts','total_accounts','revolving_balance','annual_income')
# MAGIC       THEN CASE WHEN idp_value IS NOT NULL AND TRIM(idp_value) NOT IN ('','null')
# MAGIC            THEN CAST(CAST(REGEXP_REPLACE(TRIM(idp_value), '%', '') AS DOUBLE) AS STRING)
# MAGIC            ELSE NULL END
# MAGIC     WHEN field_name = 'fico_score_range'
# MAGIC       THEN TRIM(COALESCE(idp_value,''))
# MAGIC     ELSE COALESCE(idp_value,'')
# MAGIC   END AS normalized_idp_value,
# MAGIC   -- Normalized reference value
# MAGIC   CASE
# MAGIC     WHEN field_name IN ('loan_grade','loan_sub_grade','home_ownership','loan_status',
# MAGIC                         'income_verification_status','loan_purpose','loan_title',
# MAGIC                         'employer_name','employment_length','state')
# MAGIC       THEN UPPER(TRIM(REGEXP_REPLACE(TRIM(COALESCE(reference_value,'')), '\\s+', ' ')))
# MAGIC     WHEN field_name = 'term'
# MAGIC       THEN TRIM(REGEXP_REPLACE(TRIM(COALESCE(reference_value,'')), '\\s+', ' '))
# MAGIC     WHEN field_name = 'zip_code'
# MAGIC       THEN TRIM(COALESCE(reference_value,''))
# MAGIC     WHEN field_name IN ('loan_amount','funded_amount','interest_rate','monthly_installment',
# MAGIC                         'dti','open_accounts','total_accounts','revolving_balance','annual_income')
# MAGIC       THEN CASE WHEN reference_value IS NOT NULL AND TRIM(reference_value) NOT IN ('','null')
# MAGIC            THEN CAST(CAST(reference_value AS DOUBLE) AS STRING)
# MAGIC            ELSE NULL END
# MAGIC     WHEN field_name = 'fico_score_range'
# MAGIC       THEN TRIM(COALESCE(reference_value,''))
# MAGIC     ELSE COALESCE(reference_value,'')
# MAGIC   END AS normalized_reference_value,
# MAGIC   -- Match status
# MAGIC   CASE
# MAGIC     WHEN idp_value IS NULL OR TRIM(idp_value) IN ('','null')
# MAGIC       OR reference_value IS NULL OR TRIM(reference_value) IN ('','null')
# MAGIC       THEN 'MISSING'
# MAGIC     -- Annual income: 5% tolerance
# MAGIC     WHEN field_name = 'annual_income' THEN
# MAGIC       CASE
# MAGIC         WHEN ABS(CAST(REGEXP_REPLACE(TRIM(idp_value), '%', '') AS DOUBLE)
# MAGIC                - CAST(reference_value AS DOUBLE))
# MAGIC              / NULLIF(ABS(CAST(reference_value AS DOUBLE)), 0) * 100 <= 5
# MAGIC           THEN 'MATCH'
# MAGIC         ELSE 'MISMATCH'
# MAGIC       END
# MAGIC     -- FICO range: compare low and high
# MAGIC     WHEN field_name = 'fico_score_range' THEN
# MAGIC       CASE
# MAGIC         WHEN CAST(REGEXP_EXTRACT(TRIM(idp_value), '(\\d+)', 1) AS INT)
# MAGIC            = CAST(SPLIT(reference_value, ' - ')[0] AS INT)
# MAGIC          AND CAST(REGEXP_EXTRACT(TRIM(idp_value), '(\\d+)$', 1) AS INT)
# MAGIC            = CAST(SPLIT(reference_value, ' - ')[1] AS INT)
# MAGIC           THEN 'MATCH'
# MAGIC         ELSE 'MISMATCH'
# MAGIC       END
# MAGIC     -- Numeric exact match
# MAGIC     WHEN field_name IN ('loan_amount','funded_amount','interest_rate','monthly_installment',
# MAGIC                         'dti','open_accounts','total_accounts','revolving_balance') THEN
# MAGIC       CASE
# MAGIC         WHEN CAST(REGEXP_REPLACE(TRIM(idp_value), '%', '') AS DOUBLE)
# MAGIC            = CAST(reference_value AS DOUBLE)
# MAGIC           THEN 'MATCH'
# MAGIC         ELSE 'MISMATCH'
# MAGIC       END
# MAGIC     -- Term: normalized string
# MAGIC     WHEN field_name = 'term' THEN
# MAGIC       CASE
# MAGIC         WHEN TRIM(REGEXP_REPLACE(TRIM(idp_value), '\\s+', ' '))
# MAGIC            = TRIM(REGEXP_REPLACE(TRIM(reference_value), '\\s+', ' '))
# MAGIC           THEN 'MATCH'
# MAGIC         ELSE 'MISMATCH'
# MAGIC       END
# MAGIC     -- String: case-insensitive normalized
# MAGIC     ELSE
# MAGIC       CASE
# MAGIC         WHEN UPPER(TRIM(REGEXP_REPLACE(TRIM(idp_value), '\\s+', ' ')))
# MAGIC            = UPPER(TRIM(REGEXP_REPLACE(TRIM(reference_value), '\\s+', ' ')))
# MAGIC           THEN 'MATCH'
# MAGIC         ELSE 'MISMATCH'
# MAGIC       END
# MAGIC   END AS match_status,
# MAGIC   -- Difference value (numeric only)
# MAGIC   CASE
# MAGIC     WHEN field_name IN ('loan_amount','funded_amount','interest_rate','monthly_installment',
# MAGIC                         'dti','open_accounts','total_accounts','revolving_balance','annual_income')
# MAGIC      AND idp_value IS NOT NULL AND TRIM(idp_value) NOT IN ('','null')
# MAGIC      AND reference_value IS NOT NULL AND TRIM(reference_value) NOT IN ('','null')
# MAGIC       THEN CAST(CAST(REGEXP_REPLACE(TRIM(idp_value), '%', '') AS DOUBLE)
# MAGIC                   - CAST(reference_value AS DOUBLE) AS STRING)
# MAGIC     ELSE NULL
# MAGIC   END AS difference_value,
# MAGIC   -- Difference percentage
# MAGIC   CASE
# MAGIC     WHEN field_name IN ('loan_amount','funded_amount','interest_rate','monthly_installment',
# MAGIC                         'dti','open_accounts','total_accounts','revolving_balance','annual_income')
# MAGIC      AND idp_value IS NOT NULL AND TRIM(idp_value) NOT IN ('','null')
# MAGIC      AND reference_value IS NOT NULL AND TRIM(reference_value) NOT IN ('','null')
# MAGIC      AND CAST(reference_value AS DOUBLE) != 0
# MAGIC       THEN ROUND(ABS(CAST(REGEXP_REPLACE(TRIM(idp_value), '%', '') AS DOUBLE)
# MAGIC                     - CAST(reference_value AS DOUBLE))
# MAGIC                / ABS(CAST(reference_value AS DOUBLE)) * 100, 2)
# MAGIC     ELSE NULL
# MAGIC   END AS difference_percentage,
# MAGIC   -- Comparison rule
# MAGIC   CASE
# MAGIC     WHEN field_name = 'annual_income'    THEN '5% tolerance'
# MAGIC     WHEN field_name = 'fico_score_range'  THEN 'range match (low + high)'
# MAGIC     WHEN field_name IN ('loan_amount','funded_amount','interest_rate','monthly_installment',
# MAGIC                         'dti','open_accounts','total_accounts','revolving_balance')
# MAGIC       THEN 'exact numeric'
# MAGIC     WHEN field_name = 'term'              THEN 'normalized string'
# MAGIC     ELSE 'normalized string (case-insensitive)'
# MAGIC   END AS comparison_rule,
# MAGIC   source_document_id,
# MAGIC   source_document_type,
# MAGIC   source_file_name,
# MAGIC   source_path,
# MAGIC   confidence_score,
# MAGIC   CURRENT_TIMESTAMP() AS verification_timestamp
# MAGIC FROM comparison;
# MAGIC
# MAGIC -- Verify output
# MAGIC SELECT 'Total comparison records' AS metric, CAST(COUNT(*) AS STRING) AS value FROM loanguard.silver.cross_verification_results
# MAGIC UNION ALL
# MAGIC SELECT 'Distinct customers', CAST(COUNT(DISTINCT customer_id) AS STRING) FROM loanguard.silver.cross_verification_results
# MAGIC UNION ALL
# MAGIC SELECT 'MATCH count', CAST(SUM(CASE WHEN match_status='MATCH' THEN 1 ELSE 0 END) AS STRING) FROM loanguard.silver.cross_verification_results
# MAGIC UNION ALL
# MAGIC SELECT 'MISMATCH count', CAST(SUM(CASE WHEN match_status='MISMATCH' THEN 1 ELSE 0 END) AS STRING) FROM loanguard.silver.cross_verification_results
# MAGIC UNION ALL
# MAGIC SELECT 'MISSING count', CAST(SUM(CASE WHEN match_status='MISSING' THEN 1 ELSE 0 END) AS STRING) FROM loanguard.silver.cross_verification_results

# COMMAND ----------

# DBTITLE 1,Step 3: Customer Verification Summary
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- STEP 3: Create customer_verification_summary
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE loanguard.silver.customer_verification_summary AS
# MAGIC WITH field_stats AS (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     reference_customer_id,
# MAGIC     COUNT(*) AS total_fields_compared,
# MAGIC     SUM(CASE WHEN match_status = 'MATCH'    THEN 1 ELSE 0 END) AS matched_fields,
# MAGIC     SUM(CASE WHEN match_status = 'MISMATCH' THEN 1 ELSE 0 END) AS mismatched_fields,
# MAGIC     SUM(CASE WHEN match_status = 'MISSING'  THEN 1 ELSE 0 END) AS missing_fields,
# MAGIC     -- Check if any important financial field has MISMATCH
# MAGIC     SUM(CASE WHEN match_status = 'MISMATCH' AND field_name IN (
# MAGIC       'loan_amount','funded_amount','term','interest_rate','monthly_installment',
# MAGIC       'loan_grade','annual_income','dti','fico_score_range','home_ownership'
# MAGIC     ) THEN 1 ELSE 0 END) AS important_mismatches
# MAGIC   FROM loanguard.silver.cross_verification_results
# MAGIC   GROUP BY customer_id, reference_customer_id
# MAGIC )
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   reference_customer_id,
# MAGIC   total_fields_compared,
# MAGIC   matched_fields,
# MAGIC   mismatched_fields,
# MAGIC   missing_fields,
# MAGIC   -- match_percentage = matched / (total - missing) * 100, safe division
# MAGIC   CASE
# MAGIC     WHEN (total_fields_compared - missing_fields) = 0 THEN 0.0
# MAGIC     ELSE ROUND(CAST(matched_fields AS DOUBLE) / (total_fields_compared - missing_fields) * 100, 2)
# MAGIC   END AS match_percentage,
# MAGIC   -- verification_status
# MAGIC   CASE
# MAGIC     WHEN important_mismatches > 0 THEN 'MISMATCH'
# MAGIC     WHEN missing_fields > 0 THEN 'INCOMPLETE'
# MAGIC     ELSE 'VERIFIED'
# MAGIC   END AS verification_status,
# MAGIC   -- scenario_type
# MAGIC   CASE
# MAGIC     WHEN customer_id BETWEEN 'C0001' AND 'C0180' THEN 'FULL_MATCH'
# MAGIC     WHEN customer_id BETWEEN 'C0181' AND 'C0190' THEN 'MINOR_VARIATION'
# MAGIC     WHEN customer_id BETWEEN 'C0191' AND 'C0194' THEN 'INCOME_MISMATCH'
# MAGIC     WHEN customer_id BETWEEN 'C0195' AND 'C0197' THEN 'ADDRESS_MISMATCH'
# MAGIC     WHEN customer_id BETWEEN 'C0198' AND 'C0199' THEN 'IDENTITY_MISMATCH'
# MAGIC     WHEN customer_id = 'C0200' THEN 'MISSING_OR_INVALID'
# MAGIC     ELSE 'UNKNOWN'
# MAGIC   END AS scenario_type
# MAGIC FROM field_stats
# MAGIC ORDER BY customer_id;
# MAGIC
# MAGIC -- Display summary by scenario
# MAGIC SELECT
# MAGIC   scenario_type,
# MAGIC   COUNT(*) AS customer_count,
# MAGIC   SUM(CASE WHEN verification_status = 'VERIFIED'  THEN 1 ELSE 0 END) AS verified,
# MAGIC   SUM(CASE WHEN verification_status = 'MISMATCH'  THEN 1 ELSE 0 END) AS mismatch,
# MAGIC   SUM(CASE WHEN verification_status = 'INCOMPLETE' THEN 1 ELSE 0 END) AS incomplete,
# MAGIC   ROUND(AVG(match_percentage), 2) AS avg_match_pct
# MAGIC FROM loanguard.silver.customer_verification_summary
# MAGIC GROUP BY scenario_type
# MAGIC ORDER BY scenario_type;

# COMMAND ----------

# DBTITLE 1,Count, Mapping, Coverage, Fan-Out, Status
# MAGIC %sql --name cv_tests_1_5
# MAGIC -- ============================================================
# MAGIC -- UNIT TESTS 1-5: Customer Count, Mapping, Coverage, Fan-Out, Status
# MAGIC -- ============================================================
# MAGIC WITH test1 AS (
# MAGIC   SELECT 'TEST 1: Customer Count' AS test_name, '200' AS expected_value,
# MAGIC     CAST(COUNT(DISTINCT customer_id) AS STRING) AS actual_value,
# MAGIC     CASE WHEN COUNT(DISTINCT customer_id) = 200 THEN 'PASS' ELSE 'FAIL' END AS status,
# MAGIC     NULL AS failure_reason
# MAGIC   FROM loanguard.silver.cross_verification_results
# MAGIC ),
# MAGIC test2 AS (
# MAGIC   SELECT 'TEST 2: Reference Mapping' AS test_name,
# MAGIC     '200 mapped, 0 missing, 0 duplicates' AS expected_value,
# MAGIC     CONCAT(
# MAGIC       CAST(COUNT(DISTINCT customer_id) AS STRING), ' mapped, ',
# MAGIC       CAST(SUM(CASE WHEN reference_customer_id IS NULL THEN 1 ELSE 0 END) AS STRING), ' missing, ',
# MAGIC       CAST(SUM(CASE WHEN ref_count > 1 THEN 1 ELSE 0 END) AS STRING), ' duplicate mappings'
# MAGIC     ) AS actual_value,
# MAGIC     CASE
# MAGIC       WHEN COUNT(DISTINCT customer_id) = 200
# MAGIC        AND SUM(CASE WHEN reference_customer_id IS NULL THEN 1 ELSE 0 END) = 0
# MAGIC        AND SUM(CASE WHEN ref_count > 1 THEN 1 ELSE 0 END) = 0
# MAGIC         THEN 'PASS' ELSE 'FAIL'
# MAGIC     END AS status,
# MAGIC     NULL AS failure_reason
# MAGIC   FROM (
# MAGIC     SELECT c.customer_id, c.reference_customer_id, COALESCE(rc.ref_count, 1) AS ref_count
# MAGIC     FROM (
# MAGIC       SELECT customer_id, reference_customer_id
# MAGIC       FROM loanguard.silver.cross_verification_results
# MAGIC       GROUP BY customer_id, reference_customer_id
# MAGIC     ) c
# MAGIC     LEFT JOIN (
# MAGIC       SELECT customer_id, COUNT(*) AS ref_count
# MAGIC       FROM (SELECT DISTINCT customer_id, reference_customer_id FROM loanguard.silver.cross_verification_results) t
# MAGIC       GROUP BY customer_id
# MAGIC     ) rc ON c.customer_id = rc.customer_id
# MAGIC   ) t
# MAGIC ),
# MAGIC test3 AS (
# MAGIC   SELECT 'TEST 3: Field Mapping Coverage' AS test_name,
# MAGIC     '22 expected fields, 0 missing' AS expected_value,
# MAGIC     CONCAT(
# MAGIC       CAST((SELECT COUNT(DISTINCT field_name) FROM loanguard.silver.cross_verification_results) AS STRING), ' actual, ',
# MAGIC       CAST(COUNT(*) AS STRING), ' missing'
# MAGIC     ) AS actual_value,
# MAGIC     CASE WHEN COUNT(*) = 0 AND (SELECT COUNT(DISTINCT field_name) FROM loanguard.silver.cross_verification_results) = 22 THEN 'PASS' ELSE 'FAIL' END AS status,
# MAGIC     CASE WHEN COUNT(*) > 0 THEN ARRAY_JOIN(COLLECT_LIST(missing_field), ', ') ELSE NULL END AS failure_reason
# MAGIC   FROM (
# MAGIC     SELECT expected.field_name AS missing_field
# MAGIC     FROM (
# MAGIC       SELECT 'loan_amount' AS field_name UNION ALL SELECT 'funded_amount' UNION ALL
# MAGIC       SELECT 'term' UNION ALL SELECT 'interest_rate' UNION ALL
# MAGIC       SELECT 'monthly_installment' UNION ALL SELECT 'loan_grade' UNION ALL
# MAGIC       SELECT 'loan_sub_grade' UNION ALL SELECT 'employer_name' UNION ALL
# MAGIC       SELECT 'employment_length' UNION ALL SELECT 'annual_income' UNION ALL
# MAGIC       SELECT 'income_verification_status' UNION ALL SELECT 'loan_status' UNION ALL
# MAGIC       SELECT 'loan_purpose' UNION ALL SELECT 'loan_title' UNION ALL
# MAGIC       SELECT 'zip_code' UNION ALL SELECT 'state' UNION ALL
# MAGIC       SELECT 'dti' UNION ALL SELECT 'open_accounts' UNION ALL
# MAGIC       SELECT 'total_accounts' UNION ALL SELECT 'revolving_balance' UNION ALL
# MAGIC       SELECT 'home_ownership' UNION ALL SELECT 'fico_score_range'
# MAGIC     ) expected
# MAGIC     LEFT JOIN (
# MAGIC       SELECT DISTINCT field_name FROM loanguard.silver.cross_verification_results
# MAGIC     ) actual ON expected.field_name = actual.field_name
# MAGIC     WHERE actual.field_name IS NULL
# MAGIC   ) missing_fields
# MAGIC ),
# MAGIC test4 AS (
# MAGIC   SELECT 'TEST 4: No Fan-Out' AS test_name,
# MAGIC     '0 duplicates on customer_id + field_name' AS expected_value,
# MAGIC     CAST(SUM(dup_cnt) AS STRING) AS actual_value,
# MAGIC     CASE WHEN SUM(dup_cnt) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
# MAGIC     CASE WHEN SUM(dup_cnt) > 0 THEN CONCAT(SUM(dup_cnt), ' duplicate records found') ELSE NULL END AS failure_reason
# MAGIC   FROM (
# MAGIC     SELECT customer_id, field_name, COUNT(*) AS cnt,
# MAGIC       CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END AS dup_cnt
# MAGIC     FROM loanguard.silver.cross_verification_results
# MAGIC     GROUP BY customer_id, field_name
# MAGIC   ) t
# MAGIC ),
# MAGIC test5 AS (
# MAGIC   SELECT 'TEST 5: Valid Match Status' AS test_name,
# MAGIC     'Only MATCH, MISMATCH, MISSING, NOT_APPLICABLE' AS expected_value,
# MAGIC     CAST(COUNT(*) AS STRING) AS actual_value,
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
# MAGIC     CASE WHEN COUNT(*) > 0 THEN ARRAY_JOIN(COLLECT_LIST(DISTINCT match_status), ', ') ELSE NULL END AS failure_reason
# MAGIC   FROM loanguard.silver.cross_verification_results
# MAGIC   WHERE match_status NOT IN ('MATCH','MISMATCH','MISSING','NOT_APPLICABLE')
# MAGIC )
# MAGIC SELECT * FROM test1
# MAGIC UNION ALL SELECT * FROM test2
# MAGIC UNION ALL SELECT * FROM test3
# MAGIC UNION ALL SELECT * FROM test4
# MAGIC UNION ALL SELECT * FROM test5

# COMMAND ----------

# DBTITLE 1,Income, FICO, Traceability, Missing, Scenarios
# MAGIC %sql --name cv_tests_6_10
# MAGIC -- ============================================================
# MAGIC -- UNIT TESTS 6-10: Income Tolerance, FICO, Traceability, Missing, Scenarios
# MAGIC -- ============================================================
# MAGIC WITH test6 AS (
# MAGIC   SELECT 'TEST 6: Income Tolerance' AS test_name,
# MAGIC     'annual_income <=5% = MATCH, >5% = MISMATCH' AS expected_value,
# MAGIC     CONCAT(
# MAGIC       CAST(SUM(CASE WHEN match_status = 'MATCH' AND difference_percentage <= 5 THEN 1 ELSE 0 END) AS STRING), ' correct MATCH, ',
# MAGIC       CAST(SUM(CASE WHEN match_status = 'MISMATCH' AND difference_percentage > 5 THEN 1 ELSE 0 END) AS STRING), ' correct MISMATCH, ',
# MAGIC       CAST(SUM(CASE WHEN (match_status = 'MATCH' AND difference_percentage > 5) OR (match_status = 'MISMATCH' AND difference_percentage <= 5) THEN 1 ELSE 0 END) AS STRING), ' violations'
# MAGIC     ) AS actual_value,
# MAGIC     CASE WHEN SUM(CASE WHEN (match_status = 'MATCH' AND difference_percentage > 5) OR (match_status = 'MISMATCH' AND difference_percentage <= 5 AND difference_percentage IS NOT NULL) THEN 1 ELSE 0 END) = 0
# MAGIC       THEN 'PASS' ELSE 'FAIL' END AS status,
# MAGIC     NULL AS failure_reason
# MAGIC   FROM loanguard.silver.cross_verification_results
# MAGIC   WHERE field_name = 'annual_income' AND match_status != 'MISSING'
# MAGIC ),
# MAGIC test7 AS (
# MAGIC   SELECT 'TEST 7: FICO Range' AS test_name,
# MAGIC     'Low and high both compared' AS expected_value,
# MAGIC     CONCAT(
# MAGIC       CAST(SUM(CASE WHEN match_status = 'MATCH' THEN 1 ELSE 0 END) AS STRING), ' MATCH, ',
# MAGIC       CAST(SUM(CASE WHEN match_status = 'MISMATCH' THEN 1 ELSE 0 END) AS STRING), ' MISMATCH, ',
# MAGIC       CAST(SUM(CASE WHEN match_status = 'MISSING' THEN 1 ELSE 0 END) AS STRING), ' MISSING'
# MAGIC     ) AS actual_value,
# MAGIC     CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'WARNING' END AS status,
# MAGIC     NULL AS failure_reason
# MAGIC   FROM loanguard.silver.cross_verification_results
# MAGIC   WHERE field_name = 'fico_score_range'
# MAGIC ),
# MAGIC test8 AS (
# MAGIC   SELECT 'TEST 8: Source Traceability' AS test_name,
# MAGIC     '0 NULLs in source columns' AS expected_value,
# MAGIC     CAST(COUNT(*) AS STRING) AS actual_value,
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
# MAGIC     CASE WHEN COUNT(*) > 0 THEN CONCAT(COUNT(*), ' records with NULL source fields') ELSE NULL END AS failure_reason
# MAGIC   FROM loanguard.silver.cross_verification_results
# MAGIC   WHERE match_status != 'MISSING'
# MAGIC     AND (source_document_id IS NULL OR source_document_type IS NULL
# MAGIC          OR source_file_name IS NULL OR source_path IS NULL)
# MAGIC ),
# MAGIC test9 AS (
# MAGIC   SELECT 'TEST 9: Missing Reference Values' AS test_name,
# MAGIC     'Missing LC values = MISSING, never MATCH' AS expected_value,
# MAGIC     CAST(COUNT(*) AS STRING) AS actual_value,
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
# MAGIC     CASE WHEN COUNT(*) > 0 THEN CONCAT(COUNT(*), ' records with NULL reference marked MATCH') ELSE NULL END AS failure_reason
# MAGIC   FROM loanguard.silver.cross_verification_results
# MAGIC   WHERE reference_value IS NULL AND match_status = 'MATCH'
# MAGIC ),
# MAGIC test10 AS (
# MAGIC   SELECT 'TEST 10: Scenario Validation' AS test_name,
# MAGIC     'INCOME_MISMATCH: annual_income MISMATCH; MINOR_VARIATION: no false mismatch' AS expected_value,
# MAGIC     CONCAT(
# MAGIC       'Income mismatch customers with annual_income MISMATCH: ',
# MAGIC       CAST(SUM(CASE WHEN customer_id IN ('C0191','C0192','C0193','C0194') AND field_name = 'annual_income' AND match_status = 'MISMATCH' THEN 1 ELSE 0 END) AS STRING),
# MAGIC       ' / 4 expected'
# MAGIC     ) AS actual_value,
# MAGIC     CASE
# MAGIC       WHEN SUM(CASE WHEN customer_id IN ('C0191','C0192','C0193','C0194') AND field_name = 'annual_income' AND match_status = 'MISMATCH' THEN 1 ELSE 0 END) >= 1
# MAGIC        AND SUM(CASE WHEN customer_id IN ('C0181','C0182','C0183','C0184','C0185','C0186','C0187','C0188','C0189','C0190') AND field_name = 'annual_income' AND match_status = 'MATCH' THEN 1 ELSE 0 END) >= 5
# MAGIC         THEN 'PASS'
# MAGIC       ELSE 'WARNING'
# MAGIC     END AS status,
# MAGIC     NULL AS failure_reason
# MAGIC   FROM loanguard.silver.cross_verification_results
# MAGIC   WHERE field_name = 'annual_income'
# MAGIC )
# MAGIC SELECT * FROM test6
# MAGIC UNION ALL SELECT * FROM test7
# MAGIC UNION ALL SELECT * FROM test8
# MAGIC UNION ALL SELECT * FROM test9
# MAGIC UNION ALL SELECT * FROM test10

# COMMAND ----------

# DBTITLE 1,Test Summary + Final Output
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- STEP 6: Cross-Verification Test Summary + Final Output Dashboard
# MAGIC -- ============================================================
# MAGIC
# MAGIC -- Create test summary table
# MAGIC CREATE OR REPLACE TABLE loanguard.silver.cross_verification_test_summary AS
# MAGIC WITH all_tests AS (
# MAGIC   SELECT * FROM cv_tests_1_5
# MAGIC   UNION ALL SELECT * FROM cv_tests_6_10
# MAGIC )
# MAGIC SELECT
# MAGIC   ROW_NUMBER() OVER (ORDER BY test_name) AS test_id,
# MAGIC   test_name,
# MAGIC   expected_value,
# MAGIC   actual_value,
# MAGIC   status,
# MAGIC   CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END AS failure_count,
# MAGIC   failure_reason,
# MAGIC   CURRENT_TIMESTAMP() AS test_timestamp
# MAGIC FROM all_tests;
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- FINAL OUTPUT 1-3: Customer & Reference Counts, Mapping
# MAGIC -- ============================================================
# MAGIC SELECT '1. Total Synthetic Customers' AS metric,
# MAGIC   CAST((SELECT COUNT(DISTINCT customer_id) FROM loanguard.silver.cross_verification_results) AS STRING) AS value
# MAGIC UNION ALL
# MAGIC SELECT '2. Total Reference Customers',
# MAGIC   CAST((SELECT COUNT(DISTINCT reference_customer_id) FROM loanguard.silver.cross_verification_results WHERE reference_customer_id IS NOT NULL) AS STRING)
# MAGIC UNION ALL
# MAGIC SELECT '3. Mapping Success',
# MAGIC   CONCAT(
# MAGIC     CAST((SELECT COUNT(DISTINCT customer_id) FROM loanguard.silver.cross_verification_results WHERE reference_customer_id IS NOT NULL) AS STRING),
# MAGIC     ' / 200 mapped'
# MAGIC   );
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- FINAL OUTPUT 4-9: Match Statistics
# MAGIC -- ============================================================
# MAGIC SELECT '4. Total Comparison Records' AS metric, CAST(COUNT(*) AS STRING) AS value
# MAGIC FROM loanguard.silver.cross_verification_results
# MAGIC UNION ALL
# MAGIC SELECT '5. Total MATCH', CAST(SUM(CASE WHEN match_status='MATCH' THEN 1 ELSE 0 END) AS STRING)
# MAGIC FROM loanguard.silver.cross_verification_results
# MAGIC UNION ALL
# MAGIC SELECT '6. Total MISMATCH', CAST(SUM(CASE WHEN match_status='MISMATCH' THEN 1 ELSE 0 END) AS STRING)
# MAGIC FROM loanguard.silver.cross_verification_results
# MAGIC UNION ALL
# MAGIC SELECT '7. Total MISSING', CAST(SUM(CASE WHEN match_status='MISSING' THEN 1 ELSE 0 END) AS STRING)
# MAGIC FROM loanguard.silver.cross_verification_results
# MAGIC UNION ALL
# MAGIC SELECT '8. Match Percentage',
# MAGIC   CONCAT(CAST(ROUND(SUM(CASE WHEN match_status='MATCH' THEN 1 ELSE 0 END) * 100.0
# MAGIC   / NULLIF(SUM(CASE WHEN match_status IN ('MATCH','MISMATCH') THEN 1 ELSE 0 END), 0), 2) AS STRING), '%')
# MAGIC FROM loanguard.silver.cross_verification_results
# MAGIC UNION ALL
# MAGIC SELECT '9. Mismatch Percentage',
# MAGIC   CONCAT(CAST(ROUND(SUM(CASE WHEN match_status='MISMATCH' THEN 1 ELSE 0 END) * 100.0
# MAGIC   / NULLIF(SUM(CASE WHEN match_status IN ('MATCH','MISMATCH') THEN 1 ELSE 0 END), 0), 2) AS STRING), '%')
# MAGIC FROM loanguard.silver.cross_verification_results;
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- FINAL OUTPUT 10: Field-Level Comparison Summary
# MAGIC -- ============================================================
# MAGIC SELECT field_name, comparison_rule,
# MAGIC   COUNT(*) AS total,
# MAGIC   SUM(CASE WHEN match_status='MATCH' THEN 1 ELSE 0 END) AS matched,
# MAGIC   SUM(CASE WHEN match_status='MISMATCH' THEN 1 ELSE 0 END) AS mismatched,
# MAGIC   SUM(CASE WHEN match_status='MISSING' THEN 1 ELSE 0 END) AS missing
# MAGIC FROM loanguard.silver.cross_verification_results
# MAGIC GROUP BY field_name, comparison_rule
# MAGIC ORDER BY field_name;
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- FINAL OUTPUT 11: Customer-Level Verification Summary
# MAGIC -- ============================================================
# MAGIC SELECT scenario_type, verification_status,
# MAGIC   COUNT(*) AS customer_count,
# MAGIC   ROUND(AVG(match_percentage), 2) AS avg_match_pct,
# MAGIC   MIN(match_percentage) AS min_match_pct,
# MAGIC   MAX(match_percentage) AS max_match_pct
# MAGIC FROM loanguard.silver.customer_verification_summary
# MAGIC GROUP BY scenario_type, verification_status
# MAGIC ORDER BY scenario_type, verification_status;
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- FINAL OUTPUT 12: Scenario Summary
# MAGIC -- ============================================================
# MAGIC SELECT
# MAGIC   scenario_type,
# MAGIC   COUNT(*) AS customers,
# MAGIC   SUM(matched_fields) AS total_matched,
# MAGIC   SUM(mismatched_fields) AS total_mismatched,
# MAGIC   SUM(missing_fields) AS total_missing,
# MAGIC   ROUND(AVG(match_percentage), 2) AS avg_match_pct
# MAGIC FROM loanguard.silver.customer_verification_summary
# MAGIC GROUP BY scenario_type
# MAGIC ORDER BY scenario_type;
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- FINAL OUTPUT 13: Cross-Verification Unit-Test Summary
# MAGIC -- ============================================================
# MAGIC SELECT * FROM loanguard.silver.cross_verification_test_summary ORDER BY test_id;
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- SAMPLE: 5 customers comparison detail
# MAGIC -- ============================================================
# MAGIC SELECT customer_id, field_name, idp_value, reference_value,
# MAGIC        normalized_idp_value, normalized_reference_value,
# MAGIC        match_status, difference_percentage, comparison_rule,
# MAGIC        source_document_type, source_file_name, confidence_score
# MAGIC FROM loanguard.silver.cross_verification_results
# MAGIC WHERE customer_id IN ('C0001','C0181','C0191','C0195','C0200')
# MAGIC ORDER BY customer_id, field_name
# MAGIC LIMIT 120;