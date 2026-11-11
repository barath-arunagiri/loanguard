# POD — LoanGuard 

## What We Built

1. **Loan_Guard_synthetics** → Generate 200 customers with 6 KYC documents each (1,200 total)
2. **KYC_Extraction_w1** → AI-powered extraction of 35 fields per document using `ai_parse_document()` and `ai_extract()`
3. **Cross_Verification** → Compare 22 extracted fields against Lending Club ground truth (Kaggle dataset)
4. **Fraud_Detection_Scoring** → Apply 8 fraud detection rules and produce loan decisions

**In Unity Catalog  Tables Created:**
- 🟧 **Bronze (4 tables):** 200 ground truth records, 13,210 field extractions, 1,200 document responses, 12 test results
- 🟦 **Silver (3 tables):** 4,330 field comparisons, 200 customer summaries, 10 test results
- 🟨 **Gold (2 tables):** 200 fraud scores, 200 loan decisions

**Source Data:**
- Lending Club dataset from Kaggle (.xlsx, 50 columns)
- 1,200 synthetic KYC documents (PDF/JPEG/PNG) in `/Volumes/loanguard/IDP/loanguard_documents/`

---

## Output Tables & Samples

### 🟧 Bronze Layer (4 Tables)

| Table | Rows | Description |
|-------|------|-------------|
| `loanguard.bronze.loan_dataset_ground_truth` | 200 | Kaggle Lending Club ground truth (50 columns) |
| `loanguard.bronze.idp_extraction_results` | 13,210 | Field-level AI extractions with confidence |
| `loanguard.bronze.idp_raw_extractions` | 1,200 | Raw JSON extraction responses |
| `loanguard.bronze.idp_unit_test_summary` | 12 | Extraction test results |

**Sample: Ground Truth Data (C0001-C0003)**

| customer_id | scenario_type | annual_inc | loan_amnt | int_rate | grade | term | loan_status | purpose | addr_state | zip_code |
|-------------|---------------|------------|-----------|----------|-------|------|-------------|---------|------------|----------|
| C0001 | FULL_MATCH | 120,510.76 | 37,536.56 | 6.3 | A | 60 months | Fully Paid | small_business | FL | 88276 |
| C0002 | FULL_MATCH | 131,872.96 | 16,073.14 | 18.63 | D | 60 months | Fully Paid | wedding | TX | 89636 |
| C0003 | FULL_MATCH | 47,122.28 | 11,437.16 | 24.73 | D | 60 months | Fully Paid | other | IL | 49169 |

**Sample: AI Extraction Results (C0001, first 5 fields)**

| customer_id | document_type | field_name | extracted_value | confidence_score | extraction_status |
|-------------|---------------|------------|-----------------|------------------|-------------------|
| C0001 | Aadhaar Card | aadhaar_number | 313752417359 | 0.95 | Successfully extracted |
| C0001 | Aadhaar Card | full_name | Janani Ben | 0.95 | Successfully extracted |
| C0001 | Aadhaar Card | zip_code | 88276 | 0.95 | Successfully extracted |
| C0001 | PAN Card | pan_number | NBJPS7367C | 0.95 | Successfully extracted |
| C0001 | Loan Application | annual_income | 120510.76 | 0.95 | Successfully extracted |

**IDP Unit Test Results**

| test_id | test_name | expected | actual | status |
|---------|-----------|----------|--------|--------|
| 1 | Document Count | 1,200 | 1,200 | PASS |
| 2 | Customer Count | 200 | 200 | PASS |
| 3 | 6 Doc Types Per Customer | 0 customers with != 6 | 0 | PASS |
| 4 | Document ID Uniqueness | 0 duplicates | 0 | PASS |
| 9 | Confidence Quality | All 0-1, avg=0.95 | min=0.95, max=0.95 | PASS |
| 12 | Per-Customer Completeness | All complete | 200 | PASS |

### 🟦 Silver Layer (3 Tables)

| Table | Rows | Description |
|-------|------|-------------|
| `loanguard.silver.cross_verification_results` | 4,330 | Field-level validation (MATCH/MISMATCH) |
| `loanguard.silver.customer_verification_summary` | 200 | Per-customer match % and scenario |
| `loanguard.silver.cross_verification_test_summary` | 10 | Verification test results |

**Sample: Cross-Verification Results (C0001 FULL_MATCH, selected fields)**

| field_name | idp_value | reference_value | match_status | diff_% | comparison_rule | source_doc |
|------------|-----------|----------------|--------------|--------|-----------------|-----------|
| annual_income | 120510.76 | 120510.76 | MATCH | 0.0 | 5% tolerance | Loan Application |
| loan_amount | 37536.56 | 37536.56 | MATCH | 0.0 | exact numeric | Loan Application |
| interest_rate | 6.3 | 6.3 | MATCH | 0.0 | exact numeric | Loan Application |
| state | FL | FL | MATCH | NULL | normalized string | Address Proof |
| zip_code | 88276 | 88276 | MATCH | NULL | normalized string | Address Proof |

**Sample: Cross-Verification Results (C0191 INCOME_MISMATCH)**

| field_name | idp_value | reference_value | match_status | diff_% | comparison_rule |
|------------|-----------|----------------|--------------|--------|-----------------|
| annual_income | 58077.14 | 74214.17 | MISMATCH | 21.74 | 5% tolerance |
| loan_amount | 32118.84 | 32118.84 | MATCH | 0.0 | exact numeric |
| state | IL | IL | MATCH | NULL | normalized string |

**Sample: Customer Verification Summary**

| customer_id | scenario_type | matched | mismatched | match_pct | verification_status |
|-------------|---------------|---------|------------|-----------|---------------------|
| C0001 | FULL_MATCH | 19 | 2 | 90.48 | MISMATCH |
| C0191 | INCOME_MISMATCH | 19 | 2 | 90.48 | MISMATCH |
| C0198 | IDENTITY_MISMATCH | 19 | 3 | 86.36 | MISMATCH |

**Cross-Verification Unit Tests**

| test_id | test_name | expected | actual | status |
|---------|-----------|----------|--------|--------|
| 1 | Customer Count | 200 | 200 | PASS |
| 3 | Field Mapping Coverage | 22 fields, 0 missing | 22 actual, 0 missing | PASS |
| 4 | No Fan-Out | 0 duplicates | 0 | PASS |
| 6 | Income Tolerance | <=5% MATCH, >5% MISMATCH | 191 correct, 9 correct | PASS |
| 10 | Scenario Validation | INCOME_MISMATCH has income MISMATCH | 4/4 expected | PASS |

### 🟨 Gold Layer (2 Tables)

| Table | Rows | Description |
|-------|------|-------------|
| `loanguard.gold.fraud_detection_results` | 200 | Fraud scores with 10 flags |
| `loanguard.gold.loan_approval_decisions` | 200 | Final loan decisions with reasons |

**Total Tables:** 9 (4 Bronze + 3 Silver + 2 Gold)

---

## Validation & Testing

### Unit Test Summary

| Notebook | Tests | Pass Rate | Key Tests |
|----------|-------|-----------|----------|
| KYC_Extraction_w1 | 12 | 11/12 (91.7%) | Document count, customer count, confidence quality |
| Cross_Verification | 10 | 10/10 (100%) | Field coverage, fan-out prevention, income tolerance |
| Fraud_Detection_Scoring | N/A | Rule-based | 8 fraud rules applied |
| **Total** | **22** | **21/22 (95.5%)** | |

### 8 Fraud Detection Rules

| Rule | Penalty | What It Detects |
|------|---------|----------------|
| Duplicate Aadhaar/PAN | -30 | Same ID across customers |
| Income Manipulation | -10/-30/-35 | >5%, >15%, >25% difference |
| Identity Inconsistency | -30 | Name varies across documents |
| Address Mismatch | -15 | State/zip mismatch |
| Loan Amount Mismatch | -15 | Loan amount >10% difference |
| Multiple Applications | -20 | Same phone/email |
| Loan Purpose Mismatch | -15 | Purpose differs |
| Income Not Verified | -10 | Unverified income |
| Multiple Risk Factors | -10 | 2+ flags triggered |

**Risk Thresholds:**
- 🟢 CLEAR (90-100) → APPROVED
- 🟠 LOW RISK (75-89) → APPROVED  
- 🟡 MEDIUM RISK (50-74) → REVIEW
- 🔴 HIGH RISK (<50) → REJECTED

---

## Data Lineage

```
Kaggle Lending Club Dataset (.xlsx)
  + 1,200 Synthetic KYC Documents
        |
        v
  🟧 BRONZE (4 tables)
    - loan_dataset_ground_truth (200)
    - idp_extraction_results (13,210)  
    - idp_raw_extractions (1,200)
    - idp_unit_test_summary (12)
        |
        v
  🟦 SILVER (3 tables)
    - cross_verification_results (4,330)
    - customer_verification_summary (200)
    - cross_verification_test_summary (10)
        |
        v
  🟨 GOLD (2 tables)
    - fraud_detection_results (200)
    - loan_approval_decisions (200)
        |
        v
Final: 177 APPROVED | 21 REVIEW | 2 REJECTED
```

---

## Quick Reference

### Notebooks

1. **Loan_Guard_synthetics** — Generate 200 customers + 1,200 documents
2. **KYC_Extraction_w1** — AI extraction (13,210 fields)
3. **Cross_Verification** — Compare 22 fields (4,330 comparisons)
4. **Fraud_Detection_Scoring** — 8 rules, 200 decisions

### Unity Catalog Structure

**Catalog:** `loanguard`  
**Schemas:** `bronze`, `silver`, `gold`  
**Volume:** `/Volumes/loanguard/IDP/loanguard_documents/`

### Key Technologies

* AI Functions: `ai_parse_document()`, `ai_extract()`
* Medallion Architecture: Bronze → Silver → Gold
* Data Source: Kaggle Lending Club dataset
* Document Generation: Faker, reportlab, pillow
