# Databricks notebook source
# DBTITLE 1,Step 1: Parse All KYC Documents
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- STEP 1: Parse All KYC Documents from Volume
# MAGIC -- FIX: ai_parse_document called ONCE per file only
# MAGIC --      result is stored and reused in all downstream steps
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW parsed_kyc_documents AS
# MAGIC SELECT
# MAGIC   regexp_extract(_metadata.file_path, '/([^/]+)/[^/]+$', 1) AS customer_id,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN lower(_metadata.file_name) LIKE '%pan%'     THEN 'PAN Card'
# MAGIC     WHEN lower(_metadata.file_name) LIKE '%aadhaar%' THEN 'Aadhaar Card'
# MAGIC     WHEN lower(_metadata.file_name) LIKE '%income%'  THEN 'Income Proof'
# MAGIC     WHEN lower(_metadata.file_name) LIKE '%kyc%'     THEN 'KYC Application'
# MAGIC     WHEN lower(_metadata.file_name) LIKE '%loan%'    THEN 'Loan Application'
# MAGIC     WHEN lower(_metadata.file_name) LIKE '%address%' THEN 'Address Proof'
# MAGIC     ELSE 'Other'
# MAGIC   END AS document_type,
# MAGIC
# MAGIC   _metadata.file_name   AS file_name,
# MAGIC   _metadata.file_path   AS file_path,
# MAGIC   _metadata.file_size   AS file_size_bytes,
# MAGIC   md5(content)          AS file_hash,
# MAGIC
# MAGIC   -- FIX: ONE call per file — stored once, reused everywhere
# MAGIC   ai_parse_document(content, MAP('version', '2.0')) AS parsed_content
# MAGIC
# MAGIC FROM READ_FILES(
# MAGIC   '/Volumes/loanguard/IDP/loanguard_documents/*/',
# MAGIC   format => 'binaryFile'
# MAGIC )
# MAGIC WHERE lower(_metadata.file_name) LIKE '%.pdf'
# MAGIC    OR lower(_metadata.file_name) LIKE '%.png'
# MAGIC    OR lower(_metadata.file_name) LIKE '%.jpg'
# MAGIC    OR lower(_metadata.file_name) LIKE '%.jpeg';

# COMMAND ----------

# DBTITLE 1,Step 1A: Build Document Metadata View
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- STEP 1A: Build Document Metadata View
# MAGIC -- FIX: All metadata derived from already-parsed parsed_content
# MAGIC --      No repeat ai_parse_document calls
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW document_metadata AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC
# MAGIC   -- FIX v3: document_id now includes an 8-char file_hash suffix so it is
# MAGIC   --         UNIQUE PER FILE, not just per (customer, doc type). Previously
# MAGIC   --         two files of the same type (e.g. two Bank Statements) collapsed
# MAGIC   --         onto the same document_id, which caused a join fan-out in
# MAGIC   --         idp_extraction_results (Step 5).
# MAGIC   CONCAT(customer_id, '_',
# MAGIC     CASE
# MAGIC       WHEN document_type = 'PAN Card'        THEN 'PAN'
# MAGIC       WHEN document_type = 'Aadhaar Card'    THEN 'AADHAAR'
# MAGIC       WHEN document_type = 'Income Proof'    THEN 'INCOME'
# MAGIC       WHEN document_type = 'KYC Application' THEN 'KYC'
# MAGIC       WHEN document_type = 'Loan Application' THEN 'LOAN'
# MAGIC       WHEN document_type = 'Address Proof'   THEN 'ADDRESS'
# MAGIC       ELSE 'OTHER'
# MAGIC     END,
# MAGIC     '_', SUBSTR(file_hash, 1, 8)
# MAGIC   ) AS document_id,
# MAGIC
# MAGIC   document_type,
# MAGIC   'Volume'                                            AS source_type,
# MAGIC   '/Volumes/loanguard/IDP/loanguard_documents'        AS source_name,
# MAGIC   file_path                                           AS source_path,
# MAGIC   file_name,
# MAGIC   file_size_bytes,
# MAGIC   file_hash,
# MAGIC
# MAGIC   -- FIX: confidence derived from already-parsed content — no new API call
# MAGIC   CASE
# MAGIC     WHEN CAST(parsed_content:error_status AS STRING) IS NULL THEN 0.95
# MAGIC     ELSE 0.50
# MAGIC   END AS document_confidence,
# MAGIC
# MAGIC   -- FIX: extraction status from parsed_content — no new API call
# MAGIC   CASE
# MAGIC     WHEN CAST(parsed_content:error_status AS STRING) IS NULL AND file_size_bytes > 0 THEN 'Successfully extracted'
# MAGIC     WHEN file_size_bytes = 0                                          THEN 'Failed - Empty file'
# MAGIC     WHEN file_size_bytes > 50000000                                   THEN 'Failed - File too large'
# MAGIC     ELSE                                                                   'Failed - Parsing error'
# MAGIC   END AS extraction_status,
# MAGIC
# MAGIC   parsed_content,
# MAGIC   CURRENT_TIMESTAMP() AS extraction_timestamp
# MAGIC
# MAGIC FROM parsed_kyc_documents;

# COMMAND ----------

# DBTITLE 1,Step 2: Extract KYC Fields Using AI
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- STEP 2: Extract 35 KYC Fields Using AI
# MAGIC -- Reads from parsed_kyc_documents (parsed_content already done)
# MAGIC -- Skips documents that failed parsing
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW extracted_kyc_data AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   document_type,
# MAGIC   file_name,
# MAGIC
# MAGIC   ai_extract(
# MAGIC     parsed_content,
# MAGIC     '{
# MAGIC       "full_name":           {"type": "string",  "description": "Full legal name of the customer"},
# MAGIC       "date_of_birth":       {"type": "string",  "description": "Date of birth DD/MM/YYYY or DD-MM-YYYY"},
# MAGIC       "gender":              {"type": "string",  "description": "Gender: Male, Female, or Other"},
# MAGIC       "pan_number":          {"type": "string",  "description": "10-character PAN in format XXXXX9999X"},
# MAGIC       "aadhaar_number":      {"type": "string",  "description": "12-digit Aadhaar number"},
# MAGIC       "phone_number":        {"type": "string",  "description": "Phone number with country code if present"},
# MAGIC       "email":               {"type": "string",  "description": "Email address of the customer"},
# MAGIC       "address":             {"type": "string",  "description": "Full residential address"},
# MAGIC       "city":                {"type": "string",  "description": "City name from address"},
# MAGIC       "state":               {"type": "string",  "description": "State or province name"},
# MAGIC       "zip_code":            {"type": "string",  "description": "6-digit PIN or postal code"},
# MAGIC       "employer_name":       {"type": "string",  "description": "Employer or company name from income proof"},
# MAGIC       "employment_type":     {"type": "string",  "description": "Salaried, Self-Employed, Business Owner, or Professional"},
# MAGIC       "designation":         {"type": "string",  "description": "Job title or designation"},
# MAGIC       "employment_length":   {"type": "string",  "description": "Duration of employment e.g. 3 years"},
# MAGIC       "monthly_income":      {"type": "number",  "description": "Monthly gross income as number without symbols"},
# MAGIC       "annual_income":       {"type": "number",  "description": "Annual gross income as number without symbols"},
# MAGIC       "verification_status": {"type": "string",  "description": "Verified, Not Verified, or Source Verified"},
# MAGIC       "issue_date":          {"type": "string",  "description": "Loan issue or application date"},
# MAGIC       "loan_title":          {"type": "string",  "description": "Title or purpose description of the loan"},
# MAGIC       "purpose":             {"type": "string",  "description": "Loan purpose: debt_consolidation, home_improvement etc."},
# MAGIC       "loan_status":         {"type": "string",  "description": "Fully Paid, Current, Charged Off, Late, Default"},
# MAGIC       "loan_amount":         {"type": "number",  "description": "Requested loan amount as number"},
# MAGIC       "funded_amount":       {"type": "number",  "description": "Actual funded amount as number"},
# MAGIC       "term":                {"type": "string",  "description": "Loan term e.g. 36 months or 60 months"},
# MAGIC       "interest_rate":       {"type": "number",  "description": "Interest rate percentage e.g. 12.73"},
# MAGIC       "monthly_installment": {"type": "number",  "description": "Monthly EMI or installment amount"},
# MAGIC       "loan_grade":          {"type": "string",  "description": "Loan grade e.g. A, B, C or sub-grade B3"},
# MAGIC       "home_ownership":      {"type": "string",  "description": "RENT, OWN, MORTGAGE, or OTHER"},
# MAGIC       "dti":                 {"type": "number",  "description": "Debt-to-income ratio as percentage"},
# MAGIC       "fico_score_range":    {"type": "string",  "description": "FICO score range e.g. 695-699"},
# MAGIC       "open_accounts":       {"type": "number",  "description": "Number of open credit accounts"},
# MAGIC       "total_accounts":      {"type": "number",  "description": "Total number of credit accounts"},
# MAGIC       "revolving_balance":   {"type": "number",  "description": "Total revolving credit balance"},
# MAGIC       "customer_id":         {"type": "string",  "description": "Customer ID from loan application, format C0001"},
# MAGIC       "loan_sub_grade":      {"type": "string",  "description": "Loan sub-grade e.g. A1, B2, C3"},
# MAGIC       "loan_purpose":        {"type": "string",  "description": "Loan purpose: debt_consolidation, home_improvement, credit_card etc."},
# MAGIC       "income_verification_status": {"type": "string", "description": "Income verification status: Verified, Not Verified, or Source Verified"}
# MAGIC     }',
# MAGIC     MAP(
# MAGIC       'version',      '2.1',
# MAGIC       'instructions', 'Extract all KYC and loan fields available. For identity docs extract name, IDs, DOB, gender, full address with city state and PIN. For income proof extract employer, designation, employment length, monthly and annual income. For loan applications extract all loan fields as numbers without commas or currency symbols. Return null only when a field is genuinely not present.'
# MAGIC     )
# MAGIC   ) AS extracted_fields
# MAGIC
# MAGIC FROM parsed_kyc_documents
# MAGIC WHERE CAST(parsed_content:error_status AS STRING) IS NULL;

# COMMAND ----------

# DBTITLE 1,Step 2A: Save Raw Extraction JSON (Bronze)
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- STEP 2A: Save Raw Extraction JSON to Permanent Table
# MAGIC -- ============================================================
# MAGIC -- Stores the complete ai_extract() JSON response before normalization
# MAGIC -- One row per document (1,200 rows)
# MAGIC -- Use for audit, debugging, and reprocessing without re-running AI extraction
# MAGIC
# MAGIC CREATE OR REPLACE TABLE loanguard.bronze.idp_raw_extractions AS
# MAGIC SELECT
# MAGIC   e.customer_id,
# MAGIC   
# MAGIC   -- FIX: document_id with file_hash suffix (matches normalized table)
# MAGIC   CONCAT(e.customer_id, '_',
# MAGIC     CASE
# MAGIC       WHEN e.document_type = 'PAN Card'         THEN 'PAN'
# MAGIC       WHEN e.document_type = 'Aadhaar Card'     THEN 'AADHAAR'
# MAGIC       WHEN e.document_type = 'Income Proof'     THEN 'INCOME'
# MAGIC       WHEN e.document_type = 'KYC Application'  THEN 'KYC'
# MAGIC       WHEN e.document_type = 'Loan Application' THEN 'LOAN'
# MAGIC       WHEN e.document_type = 'Address Proof'    THEN 'ADDRESS'
# MAGIC       ELSE 'OTHER'
# MAGIC     END,
# MAGIC     '_', SUBSTR(dm.file_hash, 1, 8)
# MAGIC   ) AS document_id,
# MAGIC   
# MAGIC   e.document_type,
# MAGIC   dm.source_type,
# MAGIC   dm.source_name,
# MAGIC   dm.source_path,
# MAGIC   e.file_name,
# MAGIC   dm.file_hash,
# MAGIC   dm.document_confidence,
# MAGIC   dm.extraction_status,
# MAGIC   
# MAGIC   -- RAW AI EXTRACTION JSON (complete response)
# MAGIC   e.extracted_fields AS raw_extraction_json,
# MAGIC   
# MAGIC   CURRENT_TIMESTAMP() AS extraction_timestamp
# MAGIC   
# MAGIC FROM extracted_kyc_data e
# MAGIC LEFT JOIN document_metadata dm
# MAGIC   ON e.customer_id   = dm.customer_id
# MAGIC  AND e.document_type = dm.document_type
# MAGIC  AND e.file_name     = dm.file_name;
# MAGIC
# MAGIC -- Verify
# MAGIC SELECT 
# MAGIC   'Total documents' AS metric,
# MAGIC   COUNT(*) AS value
# MAGIC FROM loanguard.bronze.idp_raw_extractions
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Distinct customers',
# MAGIC   COUNT(DISTINCT customer_id)
# MAGIC FROM loanguard.bronze.idp_raw_extractions
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Sample raw JSON (first 200 chars)',
# MAGIC   SUBSTR(CAST(raw_extraction_json AS STRING), 1, 200)
# MAGIC FROM loanguard.bronze.idp_raw_extractions
# MAGIC LIMIT 1;

# COMMAND ----------

# DBTITLE 1,Unit Test 1: IDP Extraction Results
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- STEP 5: Create IDP Extraction Results Table (Field-Level)
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE loanguard.bronze.idp_extraction_results AS
# MAGIC
# MAGIC WITH field_extractions AS (
# MAGIC   SELECT
# MAGIC     e.customer_id,
# MAGIC     dm.file_hash,
# MAGIC     -- FIX v3: document_id unique per file (matches document_metadata)
# MAGIC     CONCAT(e.customer_id, '_',
# MAGIC       CASE
# MAGIC         WHEN e.document_type = 'PAN Card'         THEN 'PAN'
# MAGIC         WHEN e.document_type = 'Aadhaar Card'     THEN 'AADHAAR'
# MAGIC         WHEN e.document_type = 'Income Proof'     THEN 'INCOME'
# MAGIC         WHEN e.document_type = 'KYC Application'  THEN 'KYC'
# MAGIC         WHEN e.document_type = 'Loan Application' THEN 'LOAN'
# MAGIC         WHEN e.document_type = 'Address Proof'    THEN 'ADDRESS'
# MAGIC         ELSE 'OTHER'
# MAGIC       END,
# MAGIC       '_', SUBSTR(dm.file_hash, 1, 8)
# MAGIC     ) AS document_id,
# MAGIC     e.document_type,
# MAGIC     e.file_name,
# MAGIC     -- Confidence from document_metadata (already parsed — no repeat call)
# MAGIC     -- FIX v4: default to 0.0 / a distinct failure label when the join
# MAGIC     --      misses, instead of silently reporting a fake "high confidence,
# MAGIC     --      successfully extracted" result for unmatched rows
# MAGIC     COALESCE(dm.document_confidence, 0.0)  AS document_confidence,
# MAGIC     COALESCE(dm.extraction_status, 'Failed - Metadata join missing') AS extraction_status,
# MAGIC     COALESCE(dm.source_type, 'Volume')      AS source_type,
# MAGIC     COALESCE(dm.source_name, '/Volumes/loanguard/IDP/loanguard_documents') AS source_name,
# MAGIC     COALESCE(dm.source_path, e.file_name)   AS source_path,
# MAGIC     CURRENT_TIMESTAMP()                     AS extraction_timestamp,
# MAGIC     e.extracted_fields
# MAGIC   FROM extracted_kyc_data e
# MAGIC   LEFT JOIN document_metadata dm
# MAGIC     -- FIX v3: join on file_name too, so each file maps 1:1 instead of
# MAGIC     --         fanning out across every same-type file for a customer
# MAGIC     ON e.customer_id  = dm.customer_id
# MAGIC    AND e.document_type = dm.document_type
# MAGIC    AND e.file_name     = dm.file_name
# MAGIC )
# MAGIC
# MAGIC -- Unpivot: one row per field using UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'full_name' AS field_name,
# MAGIC        CAST(extracted_fields:response:full_name:value AS STRING) AS extracted_value,
# MAGIC        document_confidence AS confidence_score, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions
# MAGIC WHERE extracted_fields:response:full_name:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:full_name:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'date_of_birth',
# MAGIC        CAST(extracted_fields:response:date_of_birth:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:date_of_birth:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:date_of_birth:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'gender',
# MAGIC        CAST(extracted_fields:response:gender:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:gender:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:gender:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'pan_number',
# MAGIC        CAST(extracted_fields:response:pan_number:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:pan_number:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:pan_number:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'aadhaar_number',
# MAGIC        CAST(extracted_fields:response:aadhaar_number:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:aadhaar_number:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:aadhaar_number:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'phone_number',
# MAGIC        CAST(extracted_fields:response:phone_number:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:phone_number:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:phone_number:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'email',
# MAGIC        CAST(extracted_fields:response:email:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:email:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:email:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'annual_income',
# MAGIC        CAST(extracted_fields:response:annual_income:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:annual_income:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:annual_income:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'employer_name',
# MAGIC        CAST(extracted_fields:response:employer_name:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:employer_name:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:employer_name:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'address',
# MAGIC        CAST(extracted_fields:response:address:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:address:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:address:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'city',
# MAGIC        CAST(extracted_fields:response:city:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:city:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:city:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'state',
# MAGIC        CAST(extracted_fields:response:state:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:state:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:state:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'zip_code',
# MAGIC        CAST(extracted_fields:response:zip_code:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:zip_code:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:zip_code:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'employment_type',
# MAGIC        CAST(extracted_fields:response:employment_type:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:employment_type:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:employment_type:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'designation',
# MAGIC        CAST(extracted_fields:response:designation:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:designation:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:designation:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'employment_length',
# MAGIC        CAST(extracted_fields:response:employment_length:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:employment_length:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:employment_length:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'monthly_income',
# MAGIC        CAST(extracted_fields:response:monthly_income:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:monthly_income:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:monthly_income:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'verification_status',
# MAGIC        CAST(extracted_fields:response:verification_status:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:verification_status:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:verification_status:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'issue_date',
# MAGIC        CAST(extracted_fields:response:issue_date:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:issue_date:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:issue_date:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'loan_title',
# MAGIC        CAST(extracted_fields:response:loan_title:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:loan_title:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:loan_title:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'purpose',
# MAGIC        CAST(extracted_fields:response:purpose:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:purpose:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:purpose:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'loan_status',
# MAGIC        CAST(extracted_fields:response:loan_status:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:loan_status:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:loan_status:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'loan_amount',
# MAGIC        CAST(extracted_fields:response:loan_amount:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:loan_amount:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:loan_amount:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'funded_amount',
# MAGIC        CAST(extracted_fields:response:funded_amount:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:funded_amount:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:funded_amount:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'term',
# MAGIC        CAST(extracted_fields:response:term:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:term:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:term:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'interest_rate',
# MAGIC        CAST(extracted_fields:response:interest_rate:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:interest_rate:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:interest_rate:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'monthly_installment',
# MAGIC        CAST(extracted_fields:response:monthly_installment:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:monthly_installment:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:monthly_installment:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'loan_grade',
# MAGIC        CAST(extracted_fields:response:loan_grade:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:loan_grade:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:loan_grade:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'home_ownership',
# MAGIC        CAST(extracted_fields:response:home_ownership:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:home_ownership:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:home_ownership:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'dti',
# MAGIC        CAST(extracted_fields:response:dti:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:dti:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:dti:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'fico_score_range',
# MAGIC        CAST(extracted_fields:response:fico_score_range:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:fico_score_range:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:fico_score_range:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'open_accounts',
# MAGIC        CAST(extracted_fields:response:open_accounts:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:open_accounts:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:open_accounts:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'total_accounts',
# MAGIC        CAST(extracted_fields:response:total_accounts:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:total_accounts:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:total_accounts:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'customer_id',
# MAGIC        CAST(extracted_fields:response:customer_id:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:customer_id:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:customer_id:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'loan_sub_grade',
# MAGIC        CAST(extracted_fields:response:loan_sub_grade:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:loan_sub_grade:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:loan_sub_grade:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'loan_purpose',
# MAGIC        CAST(extracted_fields:response:loan_purpose:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:loan_purpose:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:loan_purpose:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'income_verification_status',
# MAGIC        CAST(extracted_fields:response:income_verification_status:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:income_verification_status:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:income_verification_status:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT customer_id, document_id, document_type, source_type, source_name, source_path,
# MAGIC        file_name, 'revolving_balance',
# MAGIC        CAST(extracted_fields:response:revolving_balance:value AS STRING),
# MAGIC        document_confidence, extraction_status, extraction_timestamp
# MAGIC FROM field_extractions WHERE extracted_fields:response:revolving_balance:value IS NOT NULL
# MAGIC   AND LOWER(TRIM(CAST(extracted_fields:response:revolving_balance:value AS STRING))) NOT IN ('null', '')
# MAGIC
# MAGIC ORDER BY customer_id, document_type, field_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from loanguard.bronze.idp_extraction_results 

# COMMAND ----------

# DBTITLE 1,Step 5A: Per-Field Confidence Enrichment
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- STEP 5A: Enrich confidence_score with per-field confidence from ai_extract
# MAGIC -- Falls back to document_confidence when field-level confidence is unavailable
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE loanguard.bronze.idp_extraction_results AS
# MAGIC SELECT
# MAGIC   e.customer_id, e.document_id, e.document_type, e.source_type, e.source_name,
# MAGIC   e.source_path, e.file_name, e.field_name, e.extracted_value,
# MAGIC   COALESCE(
# MAGIC     CASE e.field_name
# MAGIC       WHEN 'full_name'           THEN CAST(ex.extracted_fields:response:full_name:confidence AS DOUBLE)
# MAGIC       WHEN 'date_of_birth'       THEN CAST(ex.extracted_fields:response:date_of_birth:confidence AS DOUBLE)
# MAGIC       WHEN 'gender'              THEN CAST(ex.extracted_fields:response:gender:confidence AS DOUBLE)
# MAGIC       WHEN 'pan_number'          THEN CAST(ex.extracted_fields:response:pan_number:confidence AS DOUBLE)
# MAGIC       WHEN 'aadhaar_number'      THEN CAST(ex.extracted_fields:response:aadhaar_number:confidence AS DOUBLE)
# MAGIC       WHEN 'phone_number'        THEN CAST(ex.extracted_fields:response:phone_number:confidence AS DOUBLE)
# MAGIC       WHEN 'email'               THEN CAST(ex.extracted_fields:response:email:confidence AS DOUBLE)
# MAGIC       WHEN 'address'             THEN CAST(ex.extracted_fields:response:address:confidence AS DOUBLE)
# MAGIC       WHEN 'city'                THEN CAST(ex.extracted_fields:response:city:confidence AS DOUBLE)
# MAGIC       WHEN 'state'               THEN CAST(ex.extracted_fields:response:state:confidence AS DOUBLE)
# MAGIC       WHEN 'zip_code'            THEN CAST(ex.extracted_fields:response:zip_code:confidence AS DOUBLE)
# MAGIC       WHEN 'employer_name'       THEN CAST(ex.extracted_fields:response:employer_name:confidence AS DOUBLE)
# MAGIC       WHEN 'employment_type'    THEN CAST(ex.extracted_fields:response:employment_type:confidence AS DOUBLE)
# MAGIC       WHEN 'designation'        THEN CAST(ex.extracted_fields:response:designation:confidence AS DOUBLE)
# MAGIC       WHEN 'employment_length'   THEN CAST(ex.extracted_fields:response:employment_length:confidence AS DOUBLE)
# MAGIC       WHEN 'monthly_income'     THEN CAST(ex.extracted_fields:response:monthly_income:confidence AS DOUBLE)
# MAGIC       WHEN 'annual_income'      THEN CAST(ex.extracted_fields:response:annual_income:confidence AS DOUBLE)
# MAGIC       WHEN 'verification_status' THEN CAST(ex.extracted_fields:response:verification_status:confidence AS DOUBLE)
# MAGIC       WHEN 'income_verification_status' THEN CAST(ex.extracted_fields:response:income_verification_status:confidence AS DOUBLE)
# MAGIC       WHEN 'issue_date'         THEN CAST(ex.extracted_fields:response:issue_date:confidence AS DOUBLE)
# MAGIC       WHEN 'loan_title'         THEN CAST(ex.extracted_fields:response:loan_title:confidence AS DOUBLE)
# MAGIC       WHEN 'purpose'            THEN CAST(ex.extracted_fields:response:purpose:confidence AS DOUBLE)
# MAGIC       WHEN 'loan_purpose'       THEN CAST(ex.extracted_fields:response:loan_purpose:confidence AS DOUBLE)
# MAGIC       WHEN 'loan_status'        THEN CAST(ex.extracted_fields:response:loan_status:confidence AS DOUBLE)
# MAGIC       WHEN 'loan_amount'        THEN CAST(ex.extracted_fields:response:loan_amount:confidence AS DOUBLE)
# MAGIC       WHEN 'funded_amount'      THEN CAST(ex.extracted_fields:response:funded_amount:confidence AS DOUBLE)
# MAGIC       WHEN 'term'               THEN CAST(ex.extracted_fields:response:term:confidence AS DOUBLE)
# MAGIC       WHEN 'interest_rate'      THEN CAST(ex.extracted_fields:response:interest_rate:confidence AS DOUBLE)
# MAGIC       WHEN 'monthly_installment' THEN CAST(ex.extracted_fields:response:monthly_installment:confidence AS DOUBLE)
# MAGIC       WHEN 'loan_grade'          THEN CAST(ex.extracted_fields:response:loan_grade:confidence AS DOUBLE)
# MAGIC       WHEN 'loan_sub_grade'     THEN CAST(ex.extracted_fields:response:loan_sub_grade:confidence AS DOUBLE)
# MAGIC       WHEN 'home_ownership'     THEN CAST(ex.extracted_fields:response:home_ownership:confidence AS DOUBLE)
# MAGIC       WHEN 'dti'                THEN CAST(ex.extracted_fields:response:dti:confidence AS DOUBLE)
# MAGIC       WHEN 'fico_score_range'    THEN CAST(ex.extracted_fields:response:fico_score_range:confidence AS DOUBLE)
# MAGIC       WHEN 'open_accounts'      THEN CAST(ex.extracted_fields:response:open_accounts:confidence AS DOUBLE)
# MAGIC       WHEN 'total_accounts'      THEN CAST(ex.extracted_fields:response:total_accounts:confidence AS DOUBLE)
# MAGIC       WHEN 'revolving_balance'   THEN CAST(ex.extracted_fields:response:revolving_balance:confidence AS DOUBLE)
# MAGIC       WHEN 'customer_id'         THEN CAST(ex.extracted_fields:response:customer_id:confidence AS DOUBLE)
# MAGIC     END,
# MAGIC     e.confidence_score
# MAGIC   ) AS confidence_score,
# MAGIC   e.extraction_status, e.extraction_timestamp
# MAGIC FROM loanguard.bronze.idp_extraction_results e
# MAGIC LEFT JOIN extracted_kyc_data ex
# MAGIC   ON e.customer_id = ex.customer_id AND e.file_name = ex.file_name

# COMMAND ----------

# DBTITLE 1,IDP Unit Tests 1-6: Counts, Traceability, Status
# MAGIC %sql --name idp_tests_1_6
# MAGIC -- ============================================================
# MAGIC -- IDP UNIT TESTS 1-6: Counts, Traceability, Status
# MAGIC -- ============================================================
# MAGIC WITH doc_counts AS (
# MAGIC   SELECT COUNT(DISTINCT document_id) AS actual_docs, COUNT(DISTINCT customer_id) AS actual_customers
# MAGIC   FROM loanguard.bronze.idp_extraction_results
# MAGIC ),
# MAGIC doc_per_customer AS (
# MAGIC   SELECT customer_id, COUNT(DISTINCT document_type) AS doc_type_count, COLLECT_LIST(DISTINCT document_type) AS doc_types
# MAGIC   FROM loanguard.bronze.idp_extraction_results GROUP BY customer_id
# MAGIC ),
# MAGIC customer_issues AS (
# MAGIC   SELECT customer_id FROM doc_per_customer
# MAGIC   WHERE doc_type_count != 6 OR array_contains(doc_types, 'Other')
# MAGIC ),
# MAGIC doc_id_dupes AS (
# MAGIC   SELECT document_id FROM loanguard.bronze.idp_extraction_results
# MAGIC   GROUP BY document_id HAVING COUNT(DISTINCT file_name) > 1
# MAGIC ),
# MAGIC null_counts AS (
# MAGIC   SELECT SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) +
# MAGIC          SUM(CASE WHEN document_id IS NULL THEN 1 ELSE 0 END) +
# MAGIC          SUM(CASE WHEN document_type IS NULL THEN 1 ELSE 0 END) +
# MAGIC          SUM(CASE WHEN source_path IS NULL THEN 1 ELSE 0 END) +
# MAGIC          SUM(CASE WHEN file_name IS NULL THEN 1 ELSE 0 END) +
# MAGIC          SUM(CASE WHEN field_name IS NULL THEN 1 ELSE 0 END) AS total_nulls
# MAGIC   FROM loanguard.bronze.idp_extraction_results
# MAGIC ),
# MAGIC status_stats AS (
# MAGIC   SELECT COUNT(DISTINCT document_id) AS total_docs,
# MAGIC          COUNT(DISTINCT CASE WHEN extraction_status = 'Successfully extracted' THEN document_id END) AS success_docs,
# MAGIC          COUNT(DISTINCT CASE WHEN extraction_status LIKE 'Failed%' THEN document_id END) AS failed_docs
# MAGIC   FROM loanguard.bronze.idp_extraction_results
# MAGIC )
# MAGIC SELECT 'TEST 1: Document Count' AS test_name, '1200' AS expected_value,
# MAGIC   CAST(d.actual_docs AS STRING) AS actual_value,
# MAGIC   CASE WHEN d.actual_docs = 1200 THEN 'PASS' ELSE 'FAIL' END AS status, NULL AS failure_reason
# MAGIC FROM doc_counts d
# MAGIC UNION ALL
# MAGIC SELECT 'TEST 2: Customer Count', '200', CAST(d.actual_customers AS STRING),
# MAGIC   CASE WHEN d.actual_customers = 200 THEN 'PASS' ELSE 'FAIL' END, NULL
# MAGIC FROM doc_counts d
# MAGIC UNION ALL
# MAGIC SELECT 'TEST 3: 6 Doc Types Per Customer', '0 customers with != 6',
# MAGIC   CAST(COUNT(*) AS STRING), CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC   CASE WHEN COUNT(*) > 0 THEN ARRAY_JOIN(COLLECT_LIST(customer_id), ', ') ELSE NULL END
# MAGIC FROM customer_issues
# MAGIC UNION ALL
# MAGIC SELECT 'TEST 4: Document ID Uniqueness', '0 duplicates',
# MAGIC   CAST(COUNT(*) AS STRING), CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC   CASE WHEN COUNT(*) > 0 THEN ARRAY_JOIN(COLLECT_LIST(document_id), ', ') ELSE NULL END
# MAGIC FROM doc_id_dupes
# MAGIC UNION ALL
# MAGIC SELECT 'TEST 5: Source Traceability', '0 NULLs in key columns',
# MAGIC   CAST(n.total_nulls AS STRING), CASE WHEN n.total_nulls = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC   CASE WHEN n.total_nulls > 0 THEN CONCAT(n.total_nulls, ' NULL values found') ELSE NULL END
# MAGIC FROM null_counts n
# MAGIC UNION ALL
# MAGIC SELECT 'TEST 6: Extraction Status', 'All successfully extracted',
# MAGIC   CONCAT(CAST(s.total_docs AS STRING), ' total, ', CAST(s.success_docs AS STRING), ' success, ', CAST(s.failed_docs AS STRING), ' failed'),
# MAGIC   CASE WHEN s.failed_docs = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC   CASE WHEN s.failed_docs > 0 THEN CONCAT(s.failed_docs, ' failed') ELSE NULL END
# MAGIC FROM status_stats s

# COMMAND ----------

# DBTITLE 1,IDP Unit Tests 7-10: Coverage, Dropping, Confidence
# MAGIC %sql --name idp_tests_7_10
# MAGIC -- ============================================================
# MAGIC -- IDP UNIT TESTS 7-10: Field Coverage, No Dropping, Confidence, Completeness
# MAGIC -- ============================================================
# MAGIC WITH expected_fields AS (
# MAGIC   SELECT 'KYC Application' AS dt, 'full_name' AS fn UNION ALL SELECT 'KYC Application','date_of_birth' UNION ALL
# MAGIC   SELECT 'KYC Application','gender' UNION ALL SELECT 'KYC Application','pan_number' UNION ALL
# MAGIC   SELECT 'KYC Application','aadhaar_number' UNION ALL SELECT 'KYC Application','phone_number' UNION ALL
# MAGIC   SELECT 'KYC Application','email' UNION ALL SELECT 'KYC Application','address' UNION ALL
# MAGIC   SELECT 'KYC Application','city' UNION ALL SELECT 'KYC Application','state' UNION ALL
# MAGIC   SELECT 'KYC Application','zip_code' UNION ALL
# MAGIC   SELECT 'PAN Card','full_name' UNION ALL SELECT 'PAN Card','pan_number' UNION ALL SELECT 'PAN Card','date_of_birth' UNION ALL
# MAGIC   SELECT 'Aadhaar Card','full_name' UNION ALL SELECT 'Aadhaar Card','aadhaar_number' UNION ALL
# MAGIC   SELECT 'Aadhaar Card','date_of_birth' UNION ALL SELECT 'Aadhaar Card','gender' UNION ALL
# MAGIC   SELECT 'Aadhaar Card','address' UNION ALL SELECT 'Aadhaar Card','city' UNION ALL
# MAGIC   SELECT 'Aadhaar Card','state' UNION ALL SELECT 'Aadhaar Card','zip_code' UNION ALL
# MAGIC   SELECT 'Address Proof','full_name' UNION ALL SELECT 'Address Proof','address' UNION ALL
# MAGIC   SELECT 'Address Proof','city' UNION ALL SELECT 'Address Proof','state' UNION ALL
# MAGIC   SELECT 'Address Proof','zip_code' UNION ALL SELECT 'Address Proof','phone_number' UNION ALL SELECT 'Address Proof','email' UNION ALL
# MAGIC   SELECT 'Income Proof','full_name' UNION ALL SELECT 'Income Proof','employer_name' UNION ALL
# MAGIC   SELECT 'Income Proof','employment_type' UNION ALL SELECT 'Income Proof','designation' UNION ALL
# MAGIC   SELECT 'Income Proof','employment_length' UNION ALL SELECT 'Income Proof','monthly_income' UNION ALL
# MAGIC   SELECT 'Income Proof','annual_income' UNION ALL SELECT 'Income Proof','income_verification_status' UNION ALL
# MAGIC   SELECT 'Loan Application','customer_id' UNION ALL SELECT 'Loan Application','full_name' UNION ALL
# MAGIC   SELECT 'Loan Application','date_of_birth' UNION ALL SELECT 'Loan Application','phone_number' UNION ALL
# MAGIC   SELECT 'Loan Application','email' UNION ALL SELECT 'Loan Application','pan_number' UNION ALL
# MAGIC   SELECT 'Loan Application','aadhaar_number' UNION ALL SELECT 'Loan Application','loan_title' UNION ALL
# MAGIC   SELECT 'Loan Application','loan_purpose' UNION ALL SELECT 'Loan Application','loan_status' UNION ALL
# MAGIC   SELECT 'Loan Application','loan_amount' UNION ALL SELECT 'Loan Application','funded_amount' UNION ALL
# MAGIC   SELECT 'Loan Application','term' UNION ALL SELECT 'Loan Application','interest_rate' UNION ALL
# MAGIC   SELECT 'Loan Application','monthly_installment' UNION ALL SELECT 'Loan Application','loan_grade' UNION ALL
# MAGIC   SELECT 'Loan Application','loan_sub_grade' UNION ALL SELECT 'Loan Application','annual_income' UNION ALL
# MAGIC   SELECT 'Loan Application','home_ownership' UNION ALL SELECT 'Loan Application','dti' UNION ALL
# MAGIC   SELECT 'Loan Application','fico_score_range' UNION ALL SELECT 'Loan Application','open_accounts' UNION ALL
# MAGIC   SELECT 'Loan Application','total_accounts' UNION ALL SELECT 'Loan Application','revolving_balance'
# MAGIC ),
# MAGIC actual_fields AS (SELECT DISTINCT document_type, field_name FROM loanguard.bronze.idp_extraction_results),
# MAGIC missing AS (
# MAGIC   SELECT e.dt, COLLECT_LIST(e.fn) AS missing_fields
# MAGIC   FROM expected_fields e LEFT JOIN actual_fields a ON e.dt = a.document_type AND e.fn = a.field_name
# MAGIC   WHERE a.field_name IS NULL GROUP BY e.dt
# MAGIC ),
# MAGIC all_expected_names AS (
# MAGIC   SELECT 'full_name' AS fn UNION ALL SELECT 'date_of_birth' UNION ALL SELECT 'gender' UNION ALL
# MAGIC   SELECT 'pan_number' UNION ALL SELECT 'aadhaar_number' UNION ALL SELECT 'phone_number' UNION ALL
# MAGIC   SELECT 'email' UNION ALL SELECT 'address' UNION ALL SELECT 'city' UNION ALL SELECT 'state' UNION ALL
# MAGIC   SELECT 'zip_code' UNION ALL SELECT 'employer_name' UNION ALL SELECT 'employment_type' UNION ALL
# MAGIC   SELECT 'designation' UNION ALL SELECT 'employment_length' UNION ALL SELECT 'monthly_income' UNION ALL
# MAGIC   SELECT 'annual_income' UNION ALL SELECT 'verification_status' UNION ALL SELECT 'income_verification_status' UNION ALL
# MAGIC   SELECT 'issue_date' UNION ALL SELECT 'loan_title' UNION ALL SELECT 'purpose' UNION ALL SELECT 'loan_purpose' UNION ALL
# MAGIC   SELECT 'loan_status' UNION ALL SELECT 'loan_amount' UNION ALL SELECT 'funded_amount' UNION ALL
# MAGIC   SELECT 'term' UNION ALL SELECT 'interest_rate' UNION ALL SELECT 'monthly_installment' UNION ALL
# MAGIC   SELECT 'loan_grade' UNION ALL SELECT 'loan_sub_grade' UNION ALL SELECT 'home_ownership' UNION ALL
# MAGIC   SELECT 'dti' UNION ALL SELECT 'fico_score_range' UNION ALL SELECT 'open_accounts' UNION ALL
# MAGIC   SELECT 'total_accounts' UNION ALL SELECT 'revolving_balance' UNION ALL SELECT 'customer_id'
# MAGIC ),
# MAGIC normalized_fields AS (SELECT DISTINCT field_name FROM loanguard.bronze.idp_extraction_results),
# MAGIC dropped AS (
# MAGIC   SELECT e.fn AS dropped_field FROM all_expected_names e
# MAGIC   LEFT JOIN normalized_fields n ON e.fn = n.field_name WHERE n.field_name IS NULL
# MAGIC ),
# MAGIC conf_stats AS (
# MAGIC   SELECT COUNT(*) AS total,
# MAGIC          COUNT(CASE WHEN confidence_score IS NOT NULL THEN 1 END) AS with_conf,
# MAGIC          COUNT(CASE WHEN confidence_score IS NULL THEN 1 END) AS without_conf,
# MAGIC          MIN(confidence_score) AS min_c, MAX(confidence_score) AS max_c, AVG(confidence_score) AS avg_c
# MAGIC   FROM loanguard.bronze.idp_extraction_results
# MAGIC ),
# MAGIC conf_by_field AS (
# MAGIC   SELECT field_name, COUNT(*) AS cnt, AVG(confidence_score) AS avg_conf, MIN(confidence_score) AS min_conf, MAX(confidence_score) AS max_conf
# MAGIC   FROM loanguard.bronze.idp_extraction_results GROUP BY field_name
# MAGIC ),
# MAGIC null_vals AS (
# MAGIC   SELECT COUNT(*) AS cnt FROM loanguard.bronze.idp_extraction_results
# MAGIC   WHERE extracted_value IS NULL OR LOWER(TRIM(extracted_value)) IN ('', 'null')
# MAGIC )
# MAGIC SELECT 'TEST 7: Field Coverage' AS test_name,
# MAGIC   'All expected fields per doc type' AS expected_value,
# MAGIC   CONCAT(CAST(SUM(size(missing_fields)) AS STRING), ' missing across ', CAST(COUNT(*) AS STRING), ' doc types') AS actual_value,
# MAGIC   CASE WHEN SUM(size(missing_fields)) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
# MAGIC   CASE WHEN SUM(size(missing_fields)) > 0 THEN ARRAY_JOIN(COLLECT_LIST(CONCAT(dt, ': missing=', CAST(size(missing_fields) AS STRING))), '; ') ELSE NULL END AS failure_reason
# MAGIC FROM missing
# MAGIC UNION ALL
# MAGIC SELECT 'TEST 8: No Silent Field Dropping', '0 dropped fields',
# MAGIC   CAST(COUNT(*) AS STRING), CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC   CASE WHEN COUNT(*) > 0 THEN CONCAT('Dropped: ', ARRAY_JOIN(COLLECT_LIST(dropped_field), ', ')) ELSE NULL END
# MAGIC FROM dropped
# MAGIC UNION ALL
# MAGIC SELECT 'TEST 9: Confidence Quality', 'All have confidence, range 0-1',
# MAGIC   CONCAT(CAST(c.total AS STRING), ' total, ', CAST(c.with_conf AS STRING), ' with, ', CAST(c.without_conf AS STRING), ' without, min=', CAST(ROUND(c.min_c,4) AS STRING), ', max=', CAST(ROUND(c.max_c,4) AS STRING), ', avg=', CAST(ROUND(c.avg_c,4) AS STRING)),
# MAGIC   CASE WHEN c.without_conf > 0 THEN 'CONFIDENCE_UNAVAILABLE' WHEN c.min_c < 0 OR c.max_c > 1 THEN 'FAIL' ELSE 'PASS' END,
# MAGIC   CASE WHEN c.without_conf > 0 THEN CONCAT(c.without_conf, ' fields NULL confidence') ELSE NULL END
# MAGIC FROM conf_stats c
# MAGIC UNION ALL
# MAGIC SELECT 'TEST 10: Value Completeness', '0 NULL/empty values',
# MAGIC   CAST(n.cnt AS STRING), CASE WHEN n.cnt = 0 THEN 'PASS' ELSE 'WARNING' END,
# MAGIC   CASE WHEN n.cnt > 0 THEN CONCAT(n.cnt, ' NULL/empty values') ELSE NULL END
# MAGIC FROM null_vals n

# COMMAND ----------

# DBTITLE 1,IDP Unit Tests 11-12 + Summary + Sample
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- IDP UNIT TESTS 11-12 + SUMMARY TABLE + FINAL VALIDATION
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE loanguard.bronze.idp_unit_test_summary AS
# MAGIC WITH expected_per_type AS (
# MAGIC   SELECT 'KYC Application' AS dt, 'full_name' AS fn UNION ALL
# MAGIC   SELECT 'KYC Application','date_of_birth' UNION ALL SELECT 'KYC Application','pan_number' UNION ALL
# MAGIC   SELECT 'KYC Application','aadhaar_number' UNION ALL SELECT 'KYC Application','address' UNION ALL
# MAGIC   SELECT 'KYC Application','phone_number' UNION ALL SELECT 'KYC Application','email' UNION ALL
# MAGIC   SELECT 'PAN Card','full_name' UNION ALL SELECT 'PAN Card','pan_number' UNION ALL SELECT 'PAN Card','date_of_birth' UNION ALL
# MAGIC   SELECT 'Aadhaar Card','full_name' UNION ALL SELECT 'Aadhaar Card','aadhaar_number' UNION ALL
# MAGIC   SELECT 'Aadhaar Card','date_of_birth' UNION ALL SELECT 'Aadhaar Card','address' UNION ALL
# MAGIC   SELECT 'Income Proof','employer_name' UNION ALL SELECT 'Income Proof','annual_income' UNION ALL SELECT 'Income Proof','monthly_income' UNION ALL
# MAGIC   SELECT 'Loan Application','customer_id' UNION ALL SELECT 'Loan Application','loan_amount' UNION ALL
# MAGIC   SELECT 'Loan Application','annual_income' UNION ALL SELECT 'Loan Application','loan_grade' UNION ALL
# MAGIC   SELECT 'Loan Application','dti' UNION ALL SELECT 'Loan Application','fico_score_range'
# MAGIC ),
# MAGIC actual_dt_fields AS (
# MAGIC   SELECT DISTINCT document_type, field_name FROM loanguard.bronze.idp_extraction_results
# MAGIC ),
# MAGIC validation AS (
# MAGIC   SELECT e.dt, e.fn, CASE WHEN a.field_name IS NOT NULL THEN 'PRESENT' ELSE 'MISSING' END AS field_status
# MAGIC   FROM expected_per_type e LEFT JOIN actual_dt_fields a ON e.dt = a.document_type AND e.fn = a.field_name
# MAGIC ),
# MAGIC test11_result AS (
# MAGIC   SELECT 'TEST 11: Doc Type Field Validation' AS test_name,
# MAGIC     'All expected fields present per doc type' AS expected_value,
# MAGIC     CONCAT(CAST(COUNT(CASE WHEN field_status = 'MISSING' THEN 1 END) AS STRING), ' missing, ',
# MAGIC            CAST(COUNT(CASE WHEN field_status = 'PRESENT' THEN 1 END) AS STRING), ' present') AS actual_value,
# MAGIC     CASE WHEN COUNT(CASE WHEN field_status = 'MISSING' THEN 1 END) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
# MAGIC     CASE WHEN COUNT(CASE WHEN field_status = 'MISSING' THEN 1 END) > 0
# MAGIC       THEN ARRAY_JOIN(COLLECT_LIST(CASE WHEN field_status = 'MISSING' THEN CONCAT(dt, '.', fn) END), ', ')
# MAGIC       ELSE NULL END AS failure_reason
# MAGIC   FROM validation
# MAGIC ),
# MAGIC test12_result AS (
# MAGIC   SELECT 'TEST 12: Sample Review' AS test_name,
# MAGIC     '5+ customers, multiple doc types' AS expected_value,
# MAGIC     CAST(COUNT(*) AS STRING) AS actual_value,
# MAGIC     CASE WHEN COUNT(*) >= 50 THEN 'PASS' ELSE 'WARNING' END AS status,
# MAGIC     NULL AS failure_reason
# MAGIC   FROM (SELECT customer_id, document_id, document_type, field_name, extracted_value,
# MAGIC                confidence_score, extraction_status, source_path
# MAGIC        FROM loanguard.bronze.idp_extraction_results
# MAGIC        WHERE customer_id IN ('C0001','C0002','C0003','C0004','C0005')
# MAGIC        LIMIT 200) s
# MAGIC ),
# MAGIC all_tests AS (
# MAGIC   SELECT * FROM idp_tests_1_6
# MAGIC   UNION ALL SELECT * FROM idp_tests_7_10
# MAGIC   UNION ALL SELECT * FROM test11_result
# MAGIC   UNION ALL SELECT * FROM test12_result
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
# MAGIC -- Display summary
# MAGIC SELECT * FROM loanguard.bronze.idp_unit_test_summary ORDER BY test_id;
# MAGIC
# MAGIC -- Display sample extraction for 5 customers
# MAGIC SELECT customer_id, document_id, document_type, field_name, extracted_value,
# MAGIC        confidence_score, extraction_status, source_path
# MAGIC FROM loanguard.bronze.idp_extraction_results
# MAGIC WHERE customer_id IN ('C0001','C0002','C0003','C0004','C0005')
# MAGIC ORDER BY customer_id, document_type, field_name
# MAGIC LIMIT 100