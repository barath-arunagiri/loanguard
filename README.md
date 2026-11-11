# LoanGuard — Banking KYC Verification & Fraud Detection

An intelligent document processing (IDP) pipeline built on Databricks that performs automated KYC verification and fraud detection for loan applications using AI functions and medallion architecture.

## Pipeline

| # | Notebook | Purpose |
|---|----------|---------|
| 1 | `Loan_Guard_synthetics` | Generate 200 customers with 6 KYC documents each (1,200 total) |
| 2 | `KYC_Extraction_w1` | AI-powered extraction of 35 fields per document via `ai_parse_document()` and `ai_extract()` |
| 3 | `Cross_Verification` | Compare 22 extracted fields against Lending Club ground truth |
| 4 | `Fraud_Detection_Scoring` | Apply 8 fraud detection rules and produce loan decisions |
| 5 | `PII_Masking` | PII redaction utilities |

## Architecture

```
Kaggle Lending Club Dataset + 1,200 synthetic KYC documents
        |
        v
  BRONZE (4 tables) -> SILVER (3 tables) -> GOLD (2 tables)
        |
        v
Final: 177 APPROVED | 21 REVIEW | 2 REJECTED
```

* **Catalog:** `loanguard` (schemas: `bronze`, `silver`, `gold`)
* **Volume:** `/Volumes/loanguard/IDP/loanguard_documents/`
* **Risk thresholds:** CLEAR (90-100) | LOW RISK (75-89) | MEDIUM RISK (50-74) | HIGH RISK (<50)

See `POD_IDP_Architecture.md` for full architecture details.
