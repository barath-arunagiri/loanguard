# Databricks notebook source
# DBTITLE 1,Step 1: Load Data & Apply Fraud Rules
# ============================================================
# FRAUD DETECTION - BANKING-GRADE PENALTY SCORING SYSTEM
# ============================================================
# Start: 100 points. Deduct for fraud indicators.
# 8 rules: duplicate identity, income (tiered), identity inconsistency,
#          address, loan, multiple apps, loan purpose, income not verified,
#          multiple risk factors
# Output: fraud_score (0-100), fraud_status (CLEAR/LOW/MEDIUM/HIGH)

from pyspark.sql.functions import *

print("=" * 70)
print("FRAUD DETECTION - BANKING-GRADE PENALTY SCORING")
print("=" * 70)

# --- Load data from Silver tables ---
cv_results = spark.table("loanguard.silver.cross_verification_results")
idp_results = spark.table("loanguard.bronze.idp_extraction_results")
cv_summary = spark.table("loanguard.silver.customer_verification_summary")

print(f"✓ Cross-verification results: {cv_results.count()} records")
print(f"✓ IDP extraction results: {idp_results.count()} records")
print(f"✓ Customer verification summary: {cv_summary.count()} records")

# --- Pivot KYC fields per customer (from idp_extraction_results) ---
kyc_fields = idp_results.filter(
    col("field_name").isin("pan_number", "aadhaar_number", "phone_number", "email", "full_name")
).groupBy("customer_id").agg(
    max(when(col("field_name") == "pan_number", col("extracted_value"))).alias("pan_number"),
    max(when(col("field_name") == "aadhaar_number", col("extracted_value"))).alias("aadhaar_number"),
    max(when(col("field_name") == "phone_number", col("extracted_value"))).alias("phone_number"),
    max(when(col("field_name") == "email", col("extracted_value"))).alias("email"),
    max(when(col("field_name") == "full_name", col("extracted_value"))).alias("full_name")
)

# --- Pivot mismatch flags per customer (from cross_verification_results) ---
cv_flags = cv_results.groupBy("customer_id").agg(
    max(when(col("field_name") == "annual_income", col("difference_percentage"))).alias("income_diff_pct"),
    sum(when((col("field_name") == "annual_income") & (col("match_status") == "MISMATCH"), 1).otherwise(0)).alias("income_mismatch_count"),
    sum(when((col("field_name").isin("state", "zip_code")) & (col("match_status") == "MISMATCH"), 1).otherwise(0)).alias("address_mismatch_count"),
    sum(when((col("field_name") == "loan_amount") & (col("match_status") == "MISMATCH"), 1).otherwise(0)).alias("loan_mismatch_count"),
    max(when(col("field_name") == "loan_amount", col("difference_percentage"))).alias("loan_diff_pct"),
    sum(when((col("field_name") == "loan_purpose") & (col("match_status") == "MISMATCH"), 1).otherwise(0)).alias("loan_purpose_mismatch_count"),
    max(when(col("field_name") == "income_verification_status", col("idp_value"))).alias("income_verification_status")
)

# --- Cross-document name consistency: count distinct normalized names ---
name_consistency = idp_results.filter(
    col("field_name") == "full_name"
).groupBy("customer_id").agg(
    count_distinct(upper(trim(col("extracted_value")))).alias("distinct_name_count")
)

# --- Join everything ---
fraud_base = (cv_summary
    .join(kyc_fields, "customer_id", "left")
    .join(cv_flags, "customer_id", "left")
    .join(name_consistency, "customer_id", "left")
    .fillna({
        "income_diff_pct": 0, "income_mismatch_count": 0,
        "address_mismatch_count": 0, "loan_mismatch_count": 0, "loan_diff_pct": 0,
        "loan_purpose_mismatch_count": 0, "distinct_name_count": 1
    })
)

print(f"\n📊 Total customers for fraud analysis: {fraud_base.count()}")

# --- RULE 1: Duplicate Aadhaar/PAN (-30 each) ---
print("\n🚨 RULE 1: DUPLICATE IDENTITY CHECKS (Penalty: -30 each)")

aadhaar_dups = fraud_base.filter(
    col("aadhaar_number").isNotNull() & (col("aadhaar_number") != "")
).groupBy("aadhaar_number").agg(
    count("*").alias("cnt"), collect_list("customer_id").alias("customers")
).filter(col("cnt") > 1)

pan_dups = fraud_base.filter(
    col("pan_number").isNotNull() & (col("pan_number") != "")
).groupBy("pan_number").agg(
    count("*").alias("cnt"), collect_list("customer_id").alias("customers")
).filter(col("cnt") > 1)

print(f"  Duplicate Aadhaar: {aadhaar_dups.count()} cases")
print(f"  Duplicate PAN: {pan_dups.count()} cases")

fraud_base = fraud_base.join(
    aadhaar_dups.select("aadhaar_number").withColumn("flag_duplicate_aadhaar", lit(1)),
    "aadhaar_number", "left"
).join(
    pan_dups.select("pan_number").withColumn("flag_duplicate_pan", lit(1)),
    "pan_number", "left"
).fillna({"flag_duplicate_aadhaar": 0, "flag_duplicate_pan": 0})

# --- RULE 2: Income manipulation (tiered: >25% -35, >15% -30, >5% -10) ---
print("\n🚨 RULE 2: INCOME MANIPULATION (Tiered: >25% -35 | >15% -30 | >5% -10)")
fraud_base = fraud_base.withColumn(
    "income_penalty",
    when(col("income_diff_pct") > 25, lit(35))
    .when(col("income_diff_pct") > 15, lit(30))
    .when(col("income_diff_pct") > 5, lit(10))
    .otherwise(lit(0))
).withColumn(
    "flag_income_manipulation",
    when(col("income_diff_pct") > 5, lit(1)).otherwise(lit(0))
)
for row in fraud_base.filter(col("income_penalty") > 0).groupBy("income_penalty").agg(
    count("*").alias("cnt"), collect_list("customer_id").alias("customers")
).orderBy(desc("income_penalty")).collect():
    tier = {35: ">25%", 30: ">15%", 10: ">5%"}.get(row['income_penalty'], "")
    print(f"  Income {tier} (-{row['income_penalty']}): {row['cnt']} customers — {row['customers']}")

# --- RULE 3: Cross-document identity inconsistency (-30) ---
print("\n🚨 RULE 3: CROSS-DOCUMENT IDENTITY INCONSISTENCY (Penalty: -30)")
fraud_base = fraud_base.withColumn(
    "flag_identity_inconsistency",
    when(col("distinct_name_count") > 1, lit(1)).otherwise(lit(0))
)
identity_violations = fraud_base.filter(col("flag_identity_inconsistency") == 1)
print(f"  Identity inconsistency: {identity_violations.count()} customers")
for row in identity_violations.select("customer_id", "distinct_name_count").collect():
    print(f"    {row['customer_id']}: {row['distinct_name_count']} distinct name variants across documents")

# --- RULE 4: Address mismatch + loan amount mismatch (-15 each) ---
print("\n🚨 RULE 4: ADDRESS & LOAN AMOUNT MISMATCH (Penalty: -15 each)")
fraud_base = fraud_base.withColumn(
    "flag_address_mismatch",
    when(col("address_mismatch_count") > 0, lit(1)).otherwise(lit(0))
).withColumn(
    "flag_loan_amount_mismatch",
    when((col("loan_mismatch_count") > 0) & (col("loan_diff_pct") > 10), lit(1)).otherwise(lit(0))
)
print(f"  Address mismatch: {fraud_base.filter(col('flag_address_mismatch') == 1).count()} customers")
print(f"  Loan amount mismatch: {fraud_base.filter(col('flag_loan_amount_mismatch') == 1).count()} customers")

# --- RULE 5: Multiple applications same phone/email (-20) ---
print("\n🚨 RULE 5: MULTIPLE APPLICATIONS (Penalty: -20)")
phone_dups = fraud_base.filter(
    col("phone_number").isNotNull() & (col("phone_number") != "")
).groupBy("phone_number").agg(count("*").alias("cnt")).filter(col("cnt") > 1)

email_dups = fraud_base.filter(
    col("email").isNotNull() & (col("email") != "")
).groupBy("email").agg(count("*").alias("cnt")).filter(col("cnt") > 1)

fraud_base = fraud_base.join(
    phone_dups.select("phone_number").withColumn("flag_multiple_phone", lit(1)),
    "phone_number", "left"
).join(
    email_dups.select("email").withColumn("flag_multiple_email", lit(1)),
    "email", "left"
).fillna({"flag_multiple_phone": 0, "flag_multiple_email": 0})

fraud_base = fraud_base.withColumn(
    "flag_multiple_applications",
    when((col("flag_multiple_phone") == 1) | (col("flag_multiple_email") == 1), lit(1)).otherwise(lit(0))
)
print(f"  Multiple applications: {fraud_base.filter(col('flag_multiple_applications') == 1).count()} customers")

# --- RULE 6: Loan purpose mismatch (-15) ---
print("\n🚨 RULE 6: LOAN PURPOSE MISMATCH (Penalty: -15)")
fraud_base = fraud_base.withColumn(
    "flag_loan_purpose_mismatch",
    when(col("loan_purpose_mismatch_count") > 0, lit(1)).otherwise(lit(0))
)
print(f"  Loan purpose mismatch: {fraud_base.filter(col('flag_loan_purpose_mismatch') == 1).count()} customers")

# --- RULE 7: Income not verified (-10) ---
print("\n🚨 RULE 7: INCOME NOT VERIFIED (Penalty: -10)")
fraud_base = fraud_base.withColumn(
    "flag_income_not_verified",
    when(upper(trim(col("income_verification_status"))) == "NOT VERIFIED", lit(1)).otherwise(lit(0))
)
print(f"  Income not verified: {fraud_base.filter(col('flag_income_not_verified') == 1).count()} customers")

# --- RULE 8: Multiple risk factors (-10 for 2+ different flags) ---
print("\n🚨 RULE 8: MULTIPLE RISK FACTORS (Penalty: -10 for 2+ flags)")
fraud_base = fraud_base.withColumn(
    "total_flags",
    col("flag_duplicate_pan") + col("flag_duplicate_aadhaar") +
    col("flag_income_manipulation") + col("flag_identity_inconsistency") +
    col("flag_address_mismatch") + col("flag_loan_amount_mismatch") +
    col("flag_multiple_applications") + col("flag_loan_purpose_mismatch") +
    col("flag_income_not_verified")
).withColumn(
    "flag_multiple_risk_factors",
    when(col("total_flags") >= 2, lit(1)).otherwise(lit(0))
)
multi = fraud_base.filter(col("flag_multiple_risk_factors") == 1)
print(f"  Multiple risk factors (2+ flags): {multi.count()} customers")
for row in multi.select("customer_id", "total_flags").orderBy(desc("total_flags")).collect():
    print(f"    {row['customer_id']}: {row['total_flags']} fraud flags")

print("\n✅ All 8 fraud rules applied")

# COMMAND ----------

# DBTITLE 1,Step 2: Fraud Score & Save Results
# ============================================================
# FRAUD SCORE CALCULATION + SAVE RESULTS
# ============================================================
import builtins

print("=" * 70)
print("FRAUD SCORE CALCULATION")
print("=" * 70)
print("Starting: 100")
print("Penalties: PAN -30 | Aadhaar -30 | Identity -30 | Income tiered -10/-30/-35")
print("           Address -15 | Loan -15 | Purpose -15 | Not Verified -10")
print("           Multiple Apps -20 | Multiple Risk Factors -10")
print("=" * 70)

# Calculate fraud score: 100 minus penalties
fraud_scored = fraud_base.withColumn(
    "fraud_score",
    lit(100)
    - (col("flag_duplicate_pan") * 30)
    - (col("flag_duplicate_aadhaar") * 30)
    - (col("flag_identity_inconsistency") * 30)
    - col("income_penalty")
    - (col("flag_multiple_applications") * 20)
    - (col("flag_address_mismatch") * 15)
    - (col("flag_loan_amount_mismatch") * 15)
    - (col("flag_loan_purpose_mismatch") * 15)
    - (col("flag_income_not_verified") * 10)
    - (col("flag_multiple_risk_factors") * 10)
)

# Clamp to 0, calculate penalty
fraud_scored = fraud_scored.withColumn(
    "fraud_score",
    when(col("fraud_score") < 0, lit(0)).otherwise(col("fraud_score"))
).withColumn(
    "total_penalty", lit(100) - col("fraud_score")
)

# Classify fraud status (banking-grade thresholds)
# CLEAR: 90-100, LOW RISK: 75-89, MEDIUM RISK: 50-74, HIGH RISK: <50
fraud_scored = fraud_scored.withColumn(
    "fraud_status",
    when(col("fraud_score") >= 90, lit("CLEAR"))
    .when(col("fraud_score") >= 75, lit("LOW RISK"))
    .when(col("fraud_score") >= 50, lit("MEDIUM RISK"))
    .otherwise(lit("HIGH RISK"))
)

# Create gold schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS loanguard.gold")

# Select output columns
fraud_output = fraud_scored.select(
    "customer_id", "full_name", "scenario_type",
    "fraud_score", "fraud_status", "total_penalty",
    "flag_duplicate_pan", "flag_duplicate_aadhaar",
    "flag_income_manipulation", "flag_identity_inconsistency",
    "flag_multiple_applications", "flag_address_mismatch",
    "flag_loan_amount_mismatch", "flag_loan_purpose_mismatch",
    "flag_income_not_verified", "flag_multiple_risk_factors",
    "income_diff_pct", "loan_diff_pct", "total_flags",
    "pan_number", "aadhaar_number", "phone_number", "email",
    current_timestamp().alias("fraud_check_timestamp")
)

# Save to Gold table (decision-ready business insights)
fraud_output.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "loanguard.gold.fraud_detection_results"
)

record_count = fraud_output.count()
print(f"\n✓ Saved {record_count} records to loanguard.gold.fraud_detection_results")

# Fraud status distribution
print("\n📊 FRAUD STATUS DISTRIBUTION:")
status_dist = fraud_output.groupBy("fraud_status").agg(
    count("*").alias("cnt"), round(avg("fraud_score"), 2).alias("avg_score")
).orderBy(
    when(col("fraud_status") == "HIGH RISK", 1)
    .when(col("fraud_status") == "MEDIUM RISK", 2)
    .when(col("fraud_status") == "LOW RISK", 3).otherwise(4)
)

for row in status_dist.collect():
    icon = {"HIGH RISK": "🔴", "MEDIUM RISK": "🟡", "LOW RISK": "🟠", "CLEAR": "🟢"}.get(row['fraud_status'], "")
    print(f"  {icon} {row['fraud_status']:12}: {row['cnt']:3} customers | Avg Score: {row['avg_score']}")

# By scenario type
print("\n📊 BY SCENARIO TYPE:")
scenario_dist = fraud_output.groupBy("scenario_type", "fraud_status").agg(
    count("*").alias("cnt"), round(avg("fraud_score"), 2).alias("avg_score")
).orderBy("scenario_type", "fraud_status")
display(scenario_dist)

# Total fraud alerts
total = fraud_output.count()
high_risk = fraud_output.filter(col("fraud_status") == "HIGH RISK").count()
medium_risk = fraud_output.filter(col("fraud_status") == "MEDIUM RISK").count()
alerts = high_risk + medium_risk
pct = builtins.round(alerts / total * 100, 1) if total > 0 else 0
print(f"\n⚠️  FRAUD ALERTS: {alerts} customers ({pct}%) — {high_risk} HIGH RISK (reject), {medium_risk} MEDIUM RISK (review)")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from loanguard.gold.fraud_detection_results

# COMMAND ----------

# DBTITLE 1,Step 3: Loan Approval Decision Engine
# ============================================================
# LOAN APPROVAL DECISION ENGINE + FINAL SUMMARY
# ============================================================
from pyspark.sql.functions import *
import builtins

print("=" * 70)
print("LOAN APPROVAL DECISION ENGINE")
print("=" * 70)
print("  CLEAR + LOW RISK  → APPROVED (auto-approve)")
print("  MEDIUM RISK       → REVIEW  (manual verification)")
print("  HIGH RISK         → REJECTED (deny application)")
print("=" * 70)

# Load fraud results from Gold
fraud_results = spark.table("loanguard.gold.fraud_detection_results")

# Create loan decisions
loan_decisions = fraud_results.withColumn(
    "loan_decision",
    when(col("fraud_status").isin(["CLEAR", "LOW RISK"]), lit("APPROVED"))
    .when(col("fraud_status") == "MEDIUM RISK", lit("REVIEW"))
    .when(col("fraud_status") == "HIGH RISK", lit("REJECTED"))
    .otherwise(lit("REVIEW"))
).withColumn(
    "decision_reason",
    when(col("loan_decision") == "APPROVED",
         concat(lit("Low fraud risk - Score: "), col("fraud_score").cast("string")))
    .when(col("loan_decision") == "REVIEW",
         concat(lit("Medium fraud risk - Score: "), col("fraud_score").cast("string"), lit(" - Manual review required")))
    .when(col("loan_decision") == "REJECTED",
         concat(lit("High fraud risk - Score: "), col("fraud_score").cast("string"), lit(" - Multiple fraud indicators")))
    .otherwise(lit("Pending review"))
).withColumn(
    "decision_timestamp", current_timestamp()
)

# Add explanation column based on scenario_type and loan_decision
loan_decisions = loan_decisions.withColumn(
    "explanation",
    when((col("scenario_type") == "FULL_MATCH") & (col("loan_decision") == "APPROVED"),
         lit("No fraud flags — clean application, auto-approved"))
    .when((col("scenario_type") == "FULL_MATCH") & (col("loan_decision") == "REVIEW"),
         lit("Identity/loan purpose/income flags triggered — needs verification"))
    .when((col("scenario_type") == "FULL_MATCH") & (col("loan_decision") == "REJECTED"),
         lit("Multiple fraud indicators (identity + purpose + risk factors)"))
    .when((col("scenario_type") == "IDENTITY_MISMATCH") & (col("loan_decision") == "REJECTED"),
         lit("Name varies across documents + loan purpose mismatch"))
    .when((col("scenario_type") == "IDENTITY_MISMATCH") & (col("loan_decision") == "REVIEW"),
         lit("Address mismatch + unverified income"))
    .when((col("scenario_type") == "INCOME_MISMATCH") & (col("loan_decision") == "APPROVED"),
         lit("Income difference <15% — minor penalty only"))
    .when((col("scenario_type") == "INCOME_MISMATCH") & (col("loan_decision") == "REVIEW"),
         lit("Income difference >15% — significant penalty"))
    .when((col("scenario_type") == "ADDRESS_MISMATCH") & (col("loan_decision") == "APPROVED"),
         lit("Address mismatch only — low risk"))
    .when((col("scenario_type") == "MINOR_VARIATION") & (col("loan_decision") == "APPROVED"),
         lit("Minor variations, no fraud flags"))
    .when((col("scenario_type") == "MINOR_VARIATION") & (col("loan_decision") == "REVIEW"),
         lit("Income diff >5% or identity inconsistency"))
    .when((col("scenario_type") == "MISSING_OR_INVALID") & (col("loan_decision") == "REJECTED"),
         lit("100% income + loan discrepancy — invalid data"))
    .otherwise(lit("Other"))
)

loan_output = loan_decisions.select(
    "customer_id", "full_name", "scenario_type",
    "loan_decision", "decision_reason", "explanation", "fraud_status",
    "fraud_score", "total_penalty", "decision_timestamp"
)

# Save to Gold table (final loan decisions)
loan_output.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "loanguard.gold.loan_approval_decisions"
)

print(f"\n✓ Saved {loan_output.count()} records to loanguard.gold.loan_approval_decisions")

# Decision summary
print("\n📊 LOAN DECISION SUMMARY:")
decision_dist = loan_output.groupBy("loan_decision", "fraud_status").agg(
    count("*").alias("cnt"), round(avg("fraud_score"), 2).alias("avg_score")
).orderBy(
    when(col("loan_decision") == "APPROVED", 1)
    .when(col("loan_decision") == "REVIEW", 2).otherwise(3), "fraud_status"
)

for row in decision_dist.collect():
    icon = {"APPROVED": "✅", "REVIEW": "⚠️", "REJECTED": "❌"}.get(row['loan_decision'], "")
    print(f"  {icon} {row['loan_decision']:8} | {row['fraud_status']:12}: {row['cnt']:3} customers | Avg Score: {row['avg_score']}")

# Overall stats
total = loan_output.count()
approved = loan_output.filter(col("loan_decision") == "APPROVED").count()
review = loan_output.filter(col("loan_decision") == "REVIEW").count()
rejected = loan_output.filter(col("loan_decision") == "REJECTED").count()

print(f"\n{'=' * 70}")
print(f"FINAL SUMMARY:")
print(f"  Total Customers:    {total}")
print(f"  ✅ Approved:         {approved} ({builtins.round(approved/total*100, 1)}%)")
print(f"  ⚠️  Review:           {review} ({builtins.round(review/total*100, 1)}%)")
print(f"  ❌ Rejected:         {rejected} ({builtins.round(rejected/total*100, 1)}%)")
print(f"{'=' * 70}")

# By scenario type with explanation column
print("\n📊 BY SCENARIO TYPE WITH EXPLANATION:")
spark.sql("""
WITH decision_counts AS (
    SELECT scenario_type, loan_decision, COUNT(*) as cnt
    FROM loanguard.gold.loan_approval_decisions
    GROUP BY scenario_type, loan_decision
)
SELECT 
    scenario_type,
    loan_decision,
    cnt,
    CASE 
        WHEN scenario_type = 'FULL_MATCH' AND loan_decision = 'APPROVED' THEN 'No fraud flags — clean application, auto-approved'
        WHEN scenario_type = 'FULL_MATCH' AND loan_decision = 'REVIEW' THEN 'Identity/loan purpose/income flags triggered — needs verification'
        WHEN scenario_type = 'FULL_MATCH' AND loan_decision = 'REJECTED' THEN 'Multiple fraud indicators (identity + purpose + risk factors)'
        WHEN scenario_type = 'IDENTITY_MISMATCH' AND loan_decision = 'REJECTED' THEN 'Name varies across documents + loan purpose mismatch'
        WHEN scenario_type = 'IDENTITY_MISMATCH' AND loan_decision = 'REVIEW' THEN 'Address mismatch + unverified income'
        WHEN scenario_type = 'INCOME_MISMATCH' AND loan_decision = 'APPROVED' THEN 'Income difference <15% — minor penalty only'
        WHEN scenario_type = 'INCOME_MISMATCH' AND loan_decision = 'REVIEW' THEN 'Income difference >15% — significant penalty'
        WHEN scenario_type = 'ADDRESS_MISMATCH' AND loan_decision = 'APPROVED' THEN 'Address mismatch only — low risk'
        WHEN scenario_type = 'MINOR_VARIATION' AND loan_decision = 'APPROVED' THEN 'Minor variations, no fraud flags'
        WHEN scenario_type = 'MINOR_VARIATION' AND loan_decision = 'REVIEW' THEN 'Income diff >5% or identity inconsistency'
        WHEN scenario_type = 'MISSING_OR_INVALID' AND loan_decision = 'REJECTED' THEN '100% income + loan discrepancy — invalid data'
        ELSE 'Other'
    END AS explanation
FROM decision_counts
ORDER BY scenario_type, 
    CASE loan_decision 
        WHEN 'APPROVED' THEN 1 
        WHEN 'REVIEW' THEN 2 
        WHEN 'REJECTED' THEN 3 
    END
""").show(30, truncate=False)

# Sample results — include all special scenario customers
print("\n📋 SAMPLE RESULTS (special scenario customers):")
# Reload from Gold table to show persisted data
sample = spark.table("loanguard.gold.loan_approval_decisions").filter(
    col("customer_id").isin(["C0001", "C0191", "C0195", "C0198", "C0199", "C0200"])
).orderBy("customer_id")
display(sample)