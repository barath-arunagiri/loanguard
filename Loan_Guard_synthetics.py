# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "Faker",
#   "reportlab",
#   "pillow",
# ]
# ///
# MAGIC %pip install Faker reportlab pillow

# COMMAND ----------

# DBTITLE 1,Generate Customer Data from Excel
# ============================================================
# LOANGUARD - CUSTOMER KYC DOCUMENT GENERATOR
# ============================================================

from faker import Faker
from datetime import datetime, timedelta
import random
import os
import shutil
from PIL import Image, ImageDraw, ImageFont
import pandas as pd

# ============================================================
# GENERATE SYNTHETIC LOAN DATA (NO EXTERNAL FILE NEEDED)
# ============================================================

print("📂 Generating synthetic loan dataset...")

# Generate 400 synthetic loan records
import numpy as np

num_records = 200
np.random.seed(12345)

accepted_loans = pd.DataFrame({
    'customer_id': [f'C{i:04d}' for i in range(1, num_records + 1)],
    'loan_amnt': np.random.uniform(5000, 40000, num_records).round(2),
    'funded_amnt': np.random.uniform(5000, 40000, num_records).round(2),
    'funded_amnt_inv': np.random.uniform(5000, 40000, num_records).round(2),
    'term': np.random.choice([' 36 months', ' 60 months'], num_records),
    'int_rate': np.random.uniform(5.0, 25.0, num_records).round(2),
    'installment': np.random.uniform(150, 1200, num_records).round(2),
    'grade': np.random.choice(['A', 'B', 'C', 'D', 'E', 'F', 'G'], num_records),
    'emp_title': np.random.choice(['Manager', 'Engineer', 'Analyst', 'Consultant', 'Director', 
                                   'Specialist', 'Coordinator', 'Administrator'], num_records),
    'emp_length': np.random.choice(['< 1 year', '1 year', '2 years', '3 years', '4 years', 
                                    '5 years', '6 years', '7 years', '8 years', '9 years', 
                                    '10+ years'], num_records),
    'home_ownership': np.random.choice(['RENT', 'OWN', 'MORTGAGE', 'OTHER'], num_records),
    'annual_inc': np.random.uniform(30000, 150000, num_records).round(2),
    'verification_status': np.random.choice(['Verified', 'Not Verified', 'Source Verified'], num_records),
    'issue_d': np.random.choice(['Jan-2020', 'Feb-2020', 'Mar-2020', 'Apr-2020', 'May-2020', 
                                 'Jun-2020', 'Jul-2020', 'Aug-2020', 'Sep-2020', 'Oct-2020', 
                                 'Nov-2020', 'Dec-2020'], num_records),
    'loan_status': np.random.choice(['Fully Paid', 'Current', 'Charged Off', 'Late (31-120 days)'], 
                                    num_records, p=[0.6, 0.25, 0.1, 0.05]),
    'pymnt_plan': np.random.choice(['n', 'y'], num_records, p=[0.95, 0.05]),
    'purpose': np.random.choice(['debt_consolidation', 'credit_card', 'home_improvement', 'other', 
                                 'medical', 'car', 'small_business', 'wedding', 'vacation', 'moving'], num_records),
    'title': np.random.choice(['Debt consolidation', 'Credit card refinancing', 'Home improvement', 
                               'Other', 'Medical expenses', 'Car financing'], num_records),
    'zip_code': [f'{np.random.randint(10000, 99999)}' for _ in range(num_records)],
    'addr_state': np.random.choice(['CA', 'NY', 'TX', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI'], num_records),
    'dti': np.random.uniform(0, 40, num_records).round(2),
    'earliest_cr_line': np.random.choice(['Jan-2000', 'Jan-2005', 'Jan-2010', 'Jan-2015'], num_records),
    'fico_range_low': np.random.randint(600, 800, num_records),
    'fico_range_high': np.random.randint(604, 804, num_records),
    'inq_last_6mths': np.random.randint(0, 5, num_records),
    'mths_since_last_delinq': ['null' if np.random.random() > 0.3 else str(np.random.randint(1, 100)) 
                                for _ in range(num_records)],
    'mths_since_last_record': ['null' if np.random.random() > 0.8 else str(np.random.randint(1, 100)) 
                               for _ in range(num_records)],
    'open_acc': np.random.randint(1, 30, num_records),
    'pub_rec': np.random.randint(0, 3, num_records),
    'revol_bal': np.random.uniform(0, 50000, num_records).round(2),
    'revol_util': np.random.uniform(0, 100, num_records).round(2),
    'total_acc': np.random.randint(5, 50, num_records),
    'initial_list_status': np.random.choice(['f', 'w'], num_records, p=[0.85, 0.15]),
    'out_prncp': np.random.uniform(0, 5000, num_records).round(2),
    'out_prncp_inv': np.random.uniform(0, 5000, num_records).round(2),
    'total_pymnt': np.random.uniform(1000, 50000, num_records).round(2),
    'total_pymnt_inv': np.random.uniform(1000, 50000, num_records).round(2),
    'total_rec_prncp': np.random.uniform(1000, 40000, num_records).round(2),
    'total_rec_int': np.random.uniform(100, 5000, num_records).round(2),
    'total_rec_late_fee': np.random.uniform(0, 100, num_records).round(2),
    'recoveries': np.random.uniform(0, 1000, num_records).round(2),
    'collection_recovery_fee': np.random.uniform(0, 200, num_records).round(2),
    'last_pymnt_d': np.random.choice(['Jan-2020', 'Feb-2020', 'Mar-2020', 'Apr-2020'], num_records),
    'last_pymnt_amnt': np.random.uniform(100, 2000, num_records).round(2),
    'next_pymnt_d': ['null' if np.random.random() > 0.7 else np.random.choice(['Jan-2021', 'Feb-2021']) 
                     for _ in range(num_records)],
    'last_credit_pull_d': np.random.choice(['Jan-2020', 'Feb-2020', 'Mar-2020'], num_records),
    'last_fico_range_high': np.random.randint(604, 804, num_records),
    'last_fico_range_low': np.random.randint(600, 800, num_records),
    'desc': ['Borrower added description' if np.random.random() > 0.7 else '' for _ in range(num_records)]
})

# FIX: Generate sub_grade derived from grade (e.g., 'D' -> 'D3')
# Previously sub_grade was missing from the DataFrame, so customer
# profiles always defaulted to 'C1' regardless of actual grade
accepted_loans['sub_grade'] = accepted_loans['grade'] + \
    np.random.randint(1, 6, num_records).astype(str)

print(f"✓ Generated {len(accepted_loans)} synthetic loan records")
print(f"✓ Columns: {len(accepted_loans.columns)} fields")
print(f"✓ Customer IDs: {accepted_loans['customer_id'].iloc[0]} to {accepted_loans['customer_id'].iloc[-1]}")
print(f"\n📋 Scenario Distribution:")
print(f"   • C0001-C0180: FULL_MATCH (180 records) — exact match with ground truth")
print(f"   • C0181-C0190: MINOR_VARIATION (10 records) — small ±3-8% variations")
print(f"   • C0191-C0194: INCOME_MISMATCH (4 records) — income differs 20-50%")
print(f"   • C0195-C0197: ADDRESS_MISMATCH (3 records) — different address/state/zip")
print(f"   • C0198-C0199: IDENTITY_MISMATCH (2 records) — different name/PAN/Aadhaar")
print(f"   • C0200:       MISSING_OR_INVALID (1 record) — blank/invalid document fields")
print(f"   Total: 180 + 10 + 4 + 3 + 2 + 1 = 200 records")

# ============================================================
# CONFIGURATION
# ============================================================

CATALOG = "loanguard"
SCHEMA = "IDP"
VOLUME_NAME = "loanguard_documents"
BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"
PNG_PATH = f"{BASE_PATH}/png"

# Note: We'll use customer-specific seeding for deterministic names
# Each customer_id will always generate the same name every time

# ============================================================
# CREATE SCHEMA AND VOLUME
# ============================================================

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME_NAME}")

os.makedirs(PNG_PATH, exist_ok=True)

print(f"✓ Schema, Volume, and PNG path ready")

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_font(size):
    possible_fonts = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf"
    ]
    for font_path in possible_fonts:
        if os.path.exists(font_path):
            return ImageFont.truetype(font_path, size)
    return ImageFont.load_default()

def generate_date(start_year=2020, end_year=2024):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def generate_synthetic_pan(customer_num, name_initial):
    prefixes = ["BRT", "DEM", "SYN", "LOA", "KYC", "IDX", "TST", "GEN", "ART", "FIN"]
    prefix = prefixes[(customer_num - 1) % len(prefixes)]
    check_letters = "ABCDEFGHJKLMNPQRSTUVWXYZ"
    check_letter = check_letters[(customer_num * 7) % len(check_letters)]
    return f"{prefix}P{name_initial}{customer_num:04d}{check_letter}"

def generate_synthetic_aadhaar(customer_num):
    part1 = (3000 + customer_num * 137) % 10000
    part2 = (5000 + customer_num * 241) % 10000
    part3 = (7000 + customer_num * 359) % 10000
    return f"{part1:04d} {part2:04d} {part3:04d}"

def generate_phone(customer_num):
    return f"+91-{random.randint(70000, 99999)}-{customer_num:05d}"

def generate_email(customer_id, name):
    clean_name = name.lower().replace(" ", ".").replace("'", "")
    return f"{clean_name}@loanguard.com"

# ============================================================
# GENERATE CUSTOMER DATA FROM ACCEPTED LOANS
# ============================================================

print("\n👥 Generating customer profiles...")

customers = []

# Process all 200 customers
for idx, row in accepted_loans.iterrows():
    customer_num = idx + 1
    
    # Use customer_id from dataset
    customer_id = str(row['customer_id']) if pd.notna(row['customer_id']) else f"C{customer_num:04d}"
    
    # Generate DETERMINISTIC customer profile using customer_num as seed
    # Same customer_num = Same name, DOB, gender every time
    random.seed(customer_num + 2000)
    fake_customer = Faker("en_IN")
    Faker.seed(customer_num + 2000)
    
    full_name = fake_customer.name()
    name_initial = full_name[0].upper() if full_name else 'X'
    date_of_birth = fake_customer.date_of_birth(minimum_age=21, maximum_age=60)
    gender = random.choice(["Male", "Female"])
    
    # ID documents
    pan_number = generate_synthetic_pan(customer_num, name_initial)
    aadhaar_number = generate_synthetic_aadhaar(customer_num)
    phone_number = generate_phone(customer_num)
    email = generate_email(customer_id, full_name)
    
    # Address from Excel (deterministic synthetic parts)
    zip_code = str(row['zip_code']) if pd.notna(row['zip_code']) and str(row['zip_code']) != 'null' else fake_customer.postcode()[:3] + "xx"
    addr_state = str(row['addr_state']) if pd.notna(row['addr_state']) and str(row['addr_state']) != 'null' else fake_customer.state_abbr()
    address = fake_customer.street_address()
    city = fake_customer.city()
    
    # Get ALL loan fields from Excel (use actual data where available)
    def safe_val(col, default="Not Specified"):
        val = row.get(col)
        if pd.notna(val) and str(val) != 'null':
            return str(val)
        return default
    
    def safe_float(col, default=0.0):
        val = row.get(col)
        try:
            if pd.notna(val) and str(val) != 'null':
                return float(val)
        except:
            pass
        return default
    
    # Employment
    emp_title = safe_val('emp_title')
    emp_length = safe_val('emp_length')
    annual_inc = safe_float('annual_inc', 50000.0)
    monthly_income = round(annual_inc / 12, 2)
    
    # Loan basics
    loan_amnt = safe_float('loan_amnt', 10000.0)
    funded_amnt = safe_float('funded_amnt', loan_amnt)
    funded_amnt_inv = safe_float('funded_amnt_inv', loan_amnt)
    term = safe_val('term', ' 36 months')
    int_rate = safe_float('int_rate', 10.0)
    installment = safe_float('installment', 300.0)
    grade = safe_val('grade', 'C')
    sub_grade = safe_val('sub_grade', 'C1')
    
    # Loan status
    home_ownership = safe_val('home_ownership', 'RENT')
    verification_status = safe_val('verification_status', 'Not Verified')
    issue_d = safe_val('issue_d', 'Jan-2020')
    loan_status = safe_val('loan_status', 'Current')
    pymnt_plan = safe_val('pymnt_plan', 'n')
    purpose = safe_val('purpose', 'debt_consolidation')
    title = safe_val('title', 'Loan')
    desc = safe_val('desc', '')
    
    # Credit metrics
    dti = safe_float('dti', 15.0)
    earliest_cr_line = safe_val('earliest_cr_line', 'Jan-2000')
    fico_range_low = safe_float('fico_range_low', 680)
    fico_range_high = safe_float('fico_range_high', 684)
    inq_last_6mths = safe_float('inq_last_6mths', 0)
    mths_since_last_delinq = safe_val('mths_since_last_delinq', 'null')
    mths_since_last_record = safe_val('mths_since_last_record', 'null')
    open_acc = safe_float('open_acc', 10)
    pub_rec = safe_float('pub_rec', 0)
    revol_bal = safe_float('revol_bal', 5000)
    revol_util = safe_float('revol_util', 50.0)
    total_acc = safe_float('total_acc', 20)
    
    # Payment tracking
    initial_list_status = safe_val('initial_list_status', 'f')
    out_prncp = safe_float('out_prncp', 0)
    out_prncp_inv = safe_float('out_prncp_inv', 0)
    total_pymnt = safe_float('total_pymnt', loan_amnt)
    total_pymnt_inv = safe_float('total_pymnt_inv', loan_amnt)
    total_rec_prncp = safe_float('total_rec_prncp', loan_amnt)
    total_rec_int = safe_float('total_rec_int', 500.0)
    total_rec_late_fee = safe_float('total_rec_late_fee', 0)
    recoveries = safe_float('recoveries', 0)
    collection_recovery_fee = safe_float('collection_recovery_fee', 0)
    last_pymnt_d = safe_val('last_pymnt_d', 'Jan-2020')
    last_pymnt_amnt = safe_float('last_pymnt_amnt', installment)
    next_pymnt_d = safe_val('next_pymnt_d', 'null')
    last_credit_pull_d = safe_val('last_credit_pull_d', 'Jan-2020')
    last_fico_range_high = safe_float('last_fico_range_high', fico_range_high)
    last_fico_range_low = safe_float('last_fico_range_low', fico_range_low)
    
    # Synthetic employer details (deterministic)
    employer_name = fake_customer.company()
    employment_type = random.choice(["Salaried", "Self Employed"])
    designation = emp_title
    application_date = fake_customer.date_between(start_date='-3y', end_date='today').strftime("%Y-%m-%d")
    risk_score = int((fico_range_low + fico_range_high) / 2)
    
    # ============================================================
    # SCENARIO PATTERNS APPLIED AFTER CUSTOMER DICT (see below)
    # ============================================================
    
    customer = {
        # Personal info
        "customer_id": customer_id,
        "full_name": full_name,
        "date_of_birth": date_of_birth,
        "gender": gender,
        "pan_number": pan_number,
        "aadhaar_number": aadhaar_number,
        "phone_number": phone_number,
        "email": email,
        "address": address,
        "city": city,
        "state": addr_state,
        "zip_code": zip_code,
        
        # Employment
        "employer_name": employer_name,
        "employment_type": employment_type,
        "designation": designation,
        "emp_title": emp_title,
        "emp_length": emp_length,
        "monthly_income": monthly_income,
        "annual_inc": annual_inc,
        
        # Loan basics
        "loan_amnt": loan_amnt,
        "funded_amnt": funded_amnt,
        "funded_amnt_inv": funded_amnt_inv,
        "term": term,
        "int_rate": int_rate,
        "installment": installment,
        "grade": grade,
        "sub_grade": sub_grade,
        
        # Loan details
        "home_ownership": home_ownership,
        "verification_status": verification_status,
        "issue_d": issue_d,
        "loan_status": loan_status,
        "pymnt_plan": pymnt_plan,
        "purpose": purpose,
        "title": title,
        "desc": desc,
        "application_date": application_date,
        
        # Credit metrics
        "dti": dti,
        "earliest_cr_line": earliest_cr_line,
        "fico_range_low": fico_range_low,
        "fico_range_high": fico_range_high,
        "inq_last_6mths": inq_last_6mths,
        "mths_since_last_delinq": mths_since_last_delinq,
        "mths_since_last_record": mths_since_last_record,
        "open_acc": open_acc,
        "pub_rec": pub_rec,
        "revol_bal": revol_bal,
        "revol_util": revol_util,
        "total_acc": total_acc,
        "risk_score": risk_score,
        
        # Payment tracking
        "initial_list_status": initial_list_status,
        "out_prncp": out_prncp,
        "out_prncp_inv": out_prncp_inv,
        "total_pymnt": total_pymnt,
        "total_pymnt_inv": total_pymnt_inv,
        "total_rec_prncp": total_rec_prncp,
        "total_rec_int": total_rec_int,
        "total_rec_late_fee": total_rec_late_fee,
        "recoveries": recoveries,
        "collection_recovery_fee": collection_recovery_fee,
        "last_pymnt_d": last_pymnt_d,
        "last_pymnt_amnt": last_pymnt_amnt,
        "next_pymnt_d": next_pymnt_d,
        "last_credit_pull_d": last_credit_pull_d,
        "last_fico_range_high": last_fico_range_high,
        "last_fico_range_low": last_fico_range_low
    }
    
    # ============================================================
    # SCENARIO 1: MINOR_VARIATION (C0181-C0190) — 10 customers
    # ============================================================
    if 181 <= customer_num <= 190:
        random.seed(customer_num + 9000)
        # Small income variation ±3-8%
        customer['annual_inc'] = round(annual_inc * random.uniform(0.92, 1.08), 2)
        customer['monthly_income'] = round(customer['annual_inc'] / 12, 2)
        # Small loan amount variation ±3-8%
        customer['loan_amnt'] = round(loan_amnt * random.uniform(0.92, 1.08), 2)
        customer['funded_amnt'] = round(funded_amnt * random.uniform(0.92, 1.08), 2)
        # Small interest rate variation ±0.5-1.5%
        customer['int_rate'] = round(max(5.0, min(25.0, int_rate + random.uniform(-1.5, 1.5))), 2)
    
    # ============================================================
    # SCENARIO 2: INCOME_MISMATCH (C0191-C0194) — 4 customers
    # ============================================================
    if 191 <= customer_num <= 194:
        random.seed(customer_num + 9000)
        # Income mismatch 20-50% from ground truth
        customer['annual_inc'] = round(annual_inc * random.uniform(0.5, 1.5), 2)
        customer['monthly_income'] = round(customer['annual_inc'] / 12, 2)

    # ============================================================
    # SCENARIO 3: ADDRESS_MISMATCH (C0195-C0197) — 3 customers
    # ============================================================
    if 195 <= customer_num <= 197:
        random.seed(customer_num + 9000)
        other_states = ['CA', 'NY', 'TX', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI']
        if addr_state in other_states:
            other_states.remove(addr_state)
        customer['state'] = random.choice(other_states)
        customer['city'] = fake_customer.city()
        customer['zip_code'] = str(random.randint(10000, 99999))
        customer['address'] = fake_customer.street_address()

    # ============================================================
    # SCENARIO 4: IDENTITY_MISMATCH (C0198-C0199) — 2 customers
    # ============================================================
    if 198 <= customer_num <= 199:
        random.seed(customer_num + 9000)
        fake_identity = Faker("en_IN")
        Faker.seed(customer_num + 9500)
        customer['full_name'] = fake_identity.name()
        new_initial = customer['full_name'][0].upper()
        customer['pan_number'] = generate_synthetic_pan(customer_num + 500, new_initial)
        customer['aadhaar_number'] = generate_synthetic_aadhaar(customer_num + 500)

    # ============================================================
    # SCENARIO 5: MISSING_OR_INVALID (C0200) — 1 customer
    # ============================================================
    if customer_num == 200:
        customer['full_name'] = ""
        customer['pan_number'] = "INVALID"
        customer['aadhaar_number'] = ""
        customer['annual_inc'] = 0
        customer['monthly_income'] = 0
        customer['loan_amnt'] = 0
        customer['address'] = ""
    
    # (FRAUD PATTERN 5 removed — replaced by SCENARIOS 1-5 above)
    
    customers.append(customer)
    
    if (idx + 1) % 50 == 0:
        print(f"  Processed {idx + 1}/{len(accepted_loans)} customers")

print(f"✓ Generated {len(customers)} customer profiles")

# ============================================================
# SAVE GROUND TRUTH TO DELTA TABLE
# ============================================================
# Persist the dataset so IDP extraction results can be compared
# against a stable ground truth, not just in-memory variables
spark.sql("CREATE SCHEMA IF NOT EXISTS loanguard.bronze")
spark.createDataFrame(accepted_loans).write \
    .mode("overwrite") \
    .saveAsTable("loanguard.bronze.loan_dataset_ground_truth")
print(f"✓ Saved ground truth to loanguard.bronze.loan_dataset_ground_truth")

print(f"\n✅ Customer data generation complete!")
print(f"✅ Ready to generate PNG documents in next cell")

# COMMAND ----------

# DBTITLE 1,Cleanup Old Folders and Regenerate
import os
import shutil

BASE_PATH = "/Volumes/loanguard/IDP/loanguard_documents"

# ============================================================
# CLEANUP OLD FORMAT-BASED FOLDER STRUCTURE
# ============================================================

print("🧹 Cleaning up old format-based folder structure...\n")

old_folders = [
    f"{BASE_PATH}/png",
    f"{BASE_PATH}/pdf",
    f"{BASE_PATH}/jpeg"
]

for folder in old_folders:
    if os.path.exists(folder):
        try:
            shutil.rmtree(folder)
            print(f"✓ Removed: {folder}")
        except Exception as e:
            print(f"⚠️  Could not remove {folder}: {e}")
    else:
        print(f"○ Not found: {folder}")

# ============================================================
# CLEANUP OLD CUSTOMER FOLDERS (from previous 400-customer run)
# ============================================================

import re

print("\n🧹 Cleaning up old customer folders (C0201-C0400 from previous run)...\n")

current_customer_ids = {f"C{i:04d}" for i in range(1, 201)}
removed_count = 0
kept_count = 0

if os.path.exists(BASE_PATH):
    for entry in os.listdir(BASE_PATH):
        full_path = os.path.join(BASE_PATH, entry)
        # Only check directories that match customer folder pattern C####
        if os.path.isdir(full_path) and re.match(r'^C\d{4}$', entry):
            if entry not in current_customer_ids:
                try:
                    shutil.rmtree(full_path)
                    removed_count += 1
                except Exception as e:
                    print(f"⚠️  Could not remove {entry}: {e}")
            else:
                kept_count += 1

print(f"✓ Removed {removed_count} old customer folders (C0201-C0400)")
print(f"✓ Kept {kept_count} current customer folders (C0001-C0200)")

print(f"\n✅ Cleanup complete!")
print(f"\n📂 Ready to generate documents in customer-organized structure")
print(f"   Each customer will have their own folder: {BASE_PATH}/<customer_id>/")

# COMMAND ----------

# DBTITLE 1,Generate PNG Documents in Customer Folders
# ============================================================
# MULTI-FORMAT DOCUMENT GENERATION FUNCTIONS
# ============================================================

from reportlab.lib.pagesizes import letter, A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch

# Documents will be organized by customer folders
# Each customer gets their own folder with all 6 documents

def create_document_base(width=1200, height=1400, title="", customer=None):
    """Create base PNG document with header"""
    img = Image.new('RGB', (width, height), color='white')
    draw = ImageDraw.Draw(img)
    
    # Fonts
    title_font = get_font(24)
    header_font = get_font(16)
    text_font = get_font(12)
    
    # Border
    draw.rectangle([20, 20, width-20, height-20], outline='black', width=2)
    
    # Title
    y = 40
    draw.text((width//2 - 200, y), title, font=title_font, fill='black')
    y += 50
    
    # Customer ID
    draw.text((40, y), f"Customer ID: {customer['customer_id']}", font=header_font, fill='blue')
    y += 40
    
    return img, draw, text_font, header_font, y

def create_kyc_application(customer, output_path):
    """KYC Application Form"""
    img, draw, text_font, header_font, y = create_document_base(
        title="KYC APPLICATION FORM", customer=customer
    )
    
    # Personal Details
    draw.text((40, y), "PERSONAL INFORMATION", font=header_font, fill='darkblue')
    y += 30
    
    fields = [
        ("Full Name", customer['full_name']),
        ("Date of Birth", str(customer['date_of_birth'])),
        ("Gender", customer['gender']),
        ("PAN Number", customer['pan_number']),
        ("Aadhaar Number", customer['aadhaar_number']),
        ("Phone", customer['phone_number']),
        ("Email", customer['email']),
        ("Address", customer['address']),
        ("City", customer['city']),
        ("State", customer['state']),
        ("Zip Code", customer['zip_code']),
    ]
    
    for label, value in fields:
        draw.text((40, y), f"{label}:", font=text_font, fill='black')
        draw.text((250, y), str(value), font=text_font, fill='darkgreen')
        y += 25
    
    img.save(output_path)

def create_pan_card_pdf(customer, output_path):
    """PAN Card - PDF Format"""
    c = canvas.Canvas(output_path, pagesize=letter)
    width, height = letter
    
    # Background color (light blue)
    c.setFillColorRGB(0.91, 0.96, 0.97)
    c.rect(50, height - 400, 500, 300, fill=1, stroke=0)
    
    # Border
    c.setStrokeColorRGB(0, 0, 0.5)  # Navy
    c.setLineWidth(3)
    c.rect(50, height - 400, 500, 300)
    
    # Title
    c.setFillColorRGB(0, 0, 0.5)  # Navy
    c.setFont("Helvetica-Bold", 18)
    c.drawString(150, height - 130, "PERMANENT ACCOUNT NUMBER")
    c.drawString(260, height - 155, "CARD")
    
    # Content
    c.setFillColorRGB(0, 0, 0)  # Black
    c.setFont("Helvetica", 12)
    
    y_pos = height - 210
    c.drawString(80, y_pos, "Name:")
    c.setFont("Helvetica-Bold", 14)
    c.drawString(200, y_pos, customer['full_name'].upper())
    
    y_pos -= 40
    c.setFont("Helvetica", 12)
    c.drawString(80, y_pos, "PAN:")
    c.setFont("Helvetica-Bold", 16)
    c.setFillColorRGB(0, 0, 0.5)  # Navy
    c.drawString(200, y_pos, customer['pan_number'])
    
    y_pos -= 40
    c.setFillColorRGB(0, 0, 0)  # Black
    c.setFont("Helvetica", 12)
    c.drawString(80, y_pos, "Date of Birth:")
    c.drawString(200, y_pos, str(customer['date_of_birth']))
    
    c.save()

def create_aadhaar_card_pdf(customer, output_path):
    """Aadhaar Card - PDF Format"""
    c = canvas.Canvas(output_path, pagesize=letter)
    width, height = letter
    
    # Border
    c.setStrokeColorRGB(0.54, 0, 0)  # Dark red
    c.setLineWidth(3)
    c.rect(50, height - 450, 520, 350)
    
    # Title
    c.setFillColorRGB(0.54, 0, 0)  # Dark red
    c.setFont("Helvetica-Bold", 20)
    c.drawString(230, height - 100, "AADHAAR CARD")
    
    # Content
    c.setFillColorRGB(0, 0, 0)  # Black
    c.setFont("Helvetica", 12)
    
    y_pos = height - 160
    c.drawString(80, y_pos, "Name:")
    c.setFont("Helvetica-Bold", 14)
    c.drawString(220, y_pos, customer['full_name'])
    
    y_pos -= 40
    c.setFont("Helvetica", 12)
    c.drawString(80, y_pos, "Aadhaar Number:")
    c.setFont("Helvetica-Bold", 16)
    c.setFillColorRGB(0.54, 0, 0)  # Dark red
    c.drawString(250, y_pos, customer['aadhaar_number'])
    
    y_pos -= 40
    c.setFillColorRGB(0, 0, 0)  # Black
    c.setFont("Helvetica", 12)
    c.drawString(80, y_pos, "Date of Birth:")
    c.drawString(250, y_pos, str(customer['date_of_birth']))
    
    y_pos -= 35
    c.drawString(80, y_pos, "Gender:")
    c.drawString(250, y_pos, customer['gender'])
    
    y_pos -= 35
    c.drawString(80, y_pos, "Address:")
    c.drawString(250, y_pos, f"{customer['address']}, {customer['city']}")
    y_pos -= 20
    c.drawString(250, y_pos, f"{customer['state']} - {customer['zip_code']}")
    
    c.save()

def create_address_proof_jpeg(customer, output_path):
    """Address Proof Document - JPEG Format"""
    img, draw, text_font, header_font, y = create_document_base(
        title="ADDRESS PROOF", customer=customer
    )
    
    draw.text((40, y), "RESIDENTIAL ADDRESS VERIFICATION", font=header_font, fill='darkblue')
    y += 40
    
    fields = [
        ("Full Name", customer['full_name']),
        ("Address", customer['address']),
        ("City", customer['city']),
        ("State", customer['state']),
        ("Zip Code", customer['zip_code']),
        ("Phone", customer['phone_number']),
        ("Email", customer['email']),
    ]
    
    for label, value in fields:
        draw.text((40, y), f"{label}:", font=text_font, fill='black')
        draw.text((250, y), str(value), font=text_font, fill='darkgreen')
        y += 30
    
    y += 40
    draw.text((40, y), "This is to certify that the above address is verified.", font=text_font, fill='gray')
    
    # Save as JPEG
    img = img.convert('RGB')
    img.save(output_path, 'JPEG', quality=95)

def create_income_proof_jpeg(customer, output_path):
    """Income Proof Document - JPEG Format"""
    img, draw, text_font, header_font, y = create_document_base(
        title="INCOME PROOF CERTIFICATE", customer=customer
    )
    
    draw.text((40, y), "EMPLOYMENT & INCOME DETAILS", font=header_font, fill='darkblue')
    y += 40
    
    fields = [
        ("Full Name", customer['full_name']),
        ("Employer Name", customer['employer_name']),
        ("Employment Type", customer['employment_type']),
        ("Designation", customer['designation']),
        ("Employment Length", customer['emp_length']),
        ("", ""),
        ("Monthly Income", f"${customer['monthly_income']:,.2f}"),
        ("Annual Income", f"${customer['annual_inc']:,.2f}"),
        ("", ""),
        ("Verification Status", customer['verification_status']),
    ]
    
    for label, value in fields:
        if label:
            draw.text((40, y), f"{label}:", font=text_font, fill='black')
            draw.text((250, y), str(value), font=text_font, fill='darkgreen')
        y += 25
    
    y += 40
    draw.text((40, y), f"This income certificate is issued on {customer['application_date']}", 
              font=text_font, fill='gray')
    
    # Save as JPEG
    img = img.convert('RGB')
    img.save(output_path, 'JPEG', quality=95)

def create_loan_application_pdf(customer, output_path):
    """Loan Application Document - PDF Format"""
    c = canvas.Canvas(output_path, pagesize=letter)
    width, height = letter
    
    # Border
    c.setStrokeColorRGB(0, 0, 0)
    c.setLineWidth(2)
    c.rect(30, 30, width - 60, height - 60)
    
    # Title
    c.setFillColorRGB(0, 0, 0)
    c.setFont("Helvetica-Bold", 20)
    c.drawString(220, height - 60, "LOAN APPLICATION")
    
    # Customer ID
    c.setFillColorRGB(0, 0, 1)  # Blue
    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, height - 90, f"Customer ID: {customer['customer_id']}")
    
    c.setFillColorRGB(0, 0, 0)  # Black
    c.setFont("Helvetica", 10)
    
    # Applicant Info
    y_pos = height - 130
    c.setFont("Helvetica-Bold", 12)
    c.setFillColorRGB(0, 0, 0.5)  # Dark blue
    c.drawString(50, y_pos, "APPLICANT INFORMATION")
    y_pos -= 20
    
    c.setFont("Helvetica", 10)
    c.setFillColorRGB(0, 0, 0)  # Black
    applicant_fields = [
        ("Full Name:", customer['full_name']),
        ("Date of Birth:", str(customer['date_of_birth'])),
        ("Phone:", customer['phone_number']),
        ("Email:", customer['email']),
        ("PAN:", customer['pan_number']),
        ("Aadhaar:", customer['aadhaar_number']),
    ]
    
    for label, value in applicant_fields:
        c.drawString(60, y_pos, label)
        c.setFillColorRGB(0, 0.5, 0)  # Dark green
        c.drawString(200, y_pos, str(value))
        c.setFillColorRGB(0, 0, 0)  # Black
        y_pos -= 18
    
    y_pos -= 10
    
    # Loan Details
    c.setFont("Helvetica-Bold", 12)
    c.setFillColorRGB(0, 0, 0.5)  # Dark blue
    c.drawString(50, y_pos, "LOAN DETAILS")
    y_pos -= 20
    
    c.setFont("Helvetica", 10)
    c.setFillColorRGB(0, 0, 0)  # Black
    loan_fields = [
        ("Loan Title:", customer['title']),
        ("Purpose:", customer['purpose']),
        ("Loan Status:", customer['loan_status']),
        ("", ""),
        ("Loan Amount:", f"${customer['loan_amnt']:,.2f}"),
        ("Funded Amount:", f"${customer['funded_amnt']:,.2f}"),
        ("Term:", customer['term']),
        ("Interest Rate:", f"{customer['int_rate']}%"),
        ("Installment:", f"${customer['installment']:,.2f}"),
        ("Grade:", f"{customer['grade']} ({customer['sub_grade']})"),
    ]
    
    for label, value in loan_fields:
        if label:
            c.drawString(60, y_pos, label)
            c.setFillColorRGB(0, 0.5, 0)  # Dark green
            c.drawString(200, y_pos, str(value))
            c.setFillColorRGB(0, 0, 0)  # Black
        y_pos -= 18
    
    y_pos -= 10
    
    # Financial Info
    c.setFont("Helvetica-Bold", 12)
    c.setFillColorRGB(0, 0, 0.5)  # Dark blue
    c.drawString(50, y_pos, "FINANCIAL INFORMATION")
    y_pos -= 20
    
    c.setFont("Helvetica", 10)
    c.setFillColorRGB(0, 0, 0)  # Black
    financial_fields = [
        ("Annual Income:", f"${customer['annual_inc']:,.2f}"),
        ("Home Ownership:", customer['home_ownership']),
        ("DTI Ratio:", f"{customer['dti']}%"),
        ("FICO Score:", f"{int(customer['fico_range_low'])} - {int(customer['fico_range_high'])}"),
        ("Risk Score:", str(customer['risk_score'])),
        ("Open Accounts:", str(int(customer['open_acc']))),
        ("Total Accounts:", str(int(customer['total_acc']))),
        ("Revolving Balance:", f"${customer['revol_bal']:,.2f}"),
    ]
    
    for label, value in financial_fields:
        c.drawString(60, y_pos, label)
        c.setFillColorRGB(0, 0.5, 0)  # Dark green
        c.drawString(200, y_pos, str(value))
        c.setFillColorRGB(0, 0, 0)  # Black
        y_pos -= 18
    
    c.save()

# ============================================================
# GENERATE DOCUMENTS FOR EACH CUSTOMER IN MULTIPLE FORMATS
# ============================================================

print("\n📄 Generating documents organized by customer folders...")
print("   Structure: /Volumes/loanguard/IDP/loanguard_documents/<customer_id>/")
print("   Each folder contains exactly 6 documents (1 PNG + 3 PDFs + 2 JPEGs)")
print(f"   All 200 customers get 6 documents each = 1,200 total\n")

total_docs_generated = 0
for idx, customer in enumerate(customers):
    customer_num = idx + 1
    cust_id = customer['customer_id']
    customer_folder = os.path.join(BASE_PATH, cust_id)
    os.makedirs(customer_folder, exist_ok=True)
    
    # Generate all 6 documents for every customer
    create_kyc_application(customer, os.path.join(customer_folder, f"{cust_id}_kyc_application.png"))
    create_pan_card_pdf(customer, os.path.join(customer_folder, f"{cust_id}_pan_card.pdf"))
    create_aadhaar_card_pdf(customer, os.path.join(customer_folder, f"{cust_id}_aadhaar_card.pdf"))
    create_loan_application_pdf(customer, os.path.join(customer_folder, f"{cust_id}_loan_application.pdf"))
    create_address_proof_jpeg(customer, os.path.join(customer_folder, f"{cust_id}_address_proof.jpg"))
    create_income_proof_jpeg(customer, os.path.join(customer_folder, f"{cust_id}_income_proof.jpg"))
    
    total_docs_generated += 6
    
    if (idx + 1) % 50 == 0:
        print(f"  Generated documents for {idx + 1}/{len(customers)} customers")

print(f"\n✅ SUCCESS! Document generation complete!")
print(f"="*70)
print(f"📊 Summary:")
print(f"   Total customers: {len(customers)}")
print(f"   Documents per customer: 6")
print(f"   Total documents generated: {total_docs_generated}")
print(f"\n📋 Scenario Breakdown:")
print(f"   ✅ FULL_MATCH (C0001-C0180): 180 customers × 6 docs = 1,080 docs")
print(f"   ⚠️  MINOR_VARIATION (C0181-C0190): 10 customers × 6 docs = 60 docs")
print(f"   ⚠️  INCOME_MISMATCH (C0191-C0194): 4 customers × 6 docs = 24 docs")
print(f"   ⚠️  ADDRESS_MISMATCH (C0195-C0197): 3 customers × 6 docs = 18 docs")
print(f"   ⚠️  IDENTITY_MISMATCH (C0198-C0199): 2 customers × 6 docs = 12 docs")
print(f"   ⚠️  MISSING_OR_INVALID (C0200): 1 customer × 6 docs = 6 docs")
print(f"   Total: {total_docs_generated} documents")
print(f"\n📂 Folder Structure:")
print(f"   Base path: {BASE_PATH}/")
print(f"   Organization: By customer ID")
print(f"\n📁 Example complete customer folder ({customers[0]['customer_id']}):")
print(f"   {BASE_PATH}/{customers[0]['customer_id']}/")
print(f"      ├── {customers[0]['customer_id']}_kyc_application.png")
print(f"      ├── {customers[0]['customer_id']}_pan_card.pdf")
print(f"      ├── {customers[0]['customer_id']}_aadhaar_card.pdf")
print(f"      ├── {customers[0]['customer_id']}_loan_application.pdf")
print(f"      ├── {customers[0]['customer_id']}_address_proof.jpg")
print(f"      └── {customers[0]['customer_id']}_income_proof.jpg")
print(f"\n🔍 Validation Testing Scenarios:")
print(f"   1. ✅ C0001-C0180: FULL_MATCH — exact match with ground truth")
print(f"   2. ⚠️  C0181-C0190: MINOR_VARIATION — small ±3-8% variations")
print(f"   3. ⚠️  C0191-C0194: INCOME_MISMATCH — income differs 20-50%")
print(f"   4. ⚠️  C0195-C0197: ADDRESS_MISMATCH — different address/state/zip")
print(f"   5. ⚠️  C0198-C0199: IDENTITY_MISMATCH — different name/PAN/Aadhaar")
print(f"   6. ⚠️  C0200: MISSING_OR_INVALID — blank/invalid document fields")

# COMMAND ----------

# DBTITLE 1,Fraud Pattern Summary
# MAGIC %md
# MAGIC # Synthetic Data Scenario Distribution
# MAGIC
# MAGIC This dataset contains **200 customers** with **6 documents each** = **1,200 documents total**.
# MAGIC
# MAGIC ## FULL_MATCH (180 customers)
# MAGIC - **C0001-C0180**: Document values exactly match ground-truth loan dataset
# MAGIC - All 6 documents present, all fields consistent
# MAGIC - Expected cross-verification score: 20/20 (100%)
# MAGIC
# MAGIC ## Mismatch Scenarios (20 customers)
# MAGIC
# MAGIC ### 1. MINOR_VARIATION (10 customers)
# MAGIC - **C0181-C0190**: Small ±3-8% variations in income, loan amount, interest rate
# MAGIC - All 6 documents present, values slightly off from ground truth
# MAGIC - Expected cross-verification score: 17-19/20 (85-95%)
# MAGIC
# MAGIC ### 2. INCOME_MISMATCH (4 customers)
# MAGIC - **C0191-C0194**: Income differs 20-50% from ground truth
# MAGIC - All 6 documents present, income field mismatched
# MAGIC - Expected cross-verification score: 18-19/20 (90-95%)
# MAGIC
# MAGIC ### 3. ADDRESS_MISMATCH (3 customers)
# MAGIC - **C0195-C0197**: Different state, city, zip code from ground truth
# MAGIC - All 6 documents present, address fields mismatched
# MAGIC - Expected cross-verification score: 18/20 (90%)
# MAGIC
# MAGIC ### 4. IDENTITY_MISMATCH (2 customers)
# MAGIC - **C0198-C0199**: Different name, PAN, Aadhaar from ground truth
# MAGIC - All 6 documents present, identity fields mismatched
# MAGIC - Expected cross-verification score: 17-18/20 (85-90%)
# MAGIC
# MAGIC ### 5. MISSING_OR_INVALID (1 customer)
# MAGIC - **C0200**: Blank/invalid fields in documents (name, PAN, income, address)
# MAGIC - All 6 documents present but with empty or invalid values
# MAGIC - Expected cross-verification score: 14-16/20 (70-80%)
# MAGIC
# MAGIC ## Ground Truth
# MAGIC - Reference data saved to `loanguard.bronze.loan_dataset_ground_truth`
# MAGIC - Lending Club field format: loan_amnt, funded_amnt, int_rate, installment, grade, sub_grade, emp_length, home_ownership, annual_inc, verification_status, purpose, zip_code, addr_state, dti, fico_range_low, fico_range_high, open_acc, total_acc, revol_bal
# MAGIC
# MAGIC ## Document Types (6 per customer)
# MAGIC | # | Document | Format | File Name |
# MAGIC |---|---|---|---|
# MAGIC | 1 | KYC Application | PNG | `{cust}_kyc_application.png` |
# MAGIC | 2 | PAN Card | PDF | `{cust}_pan_card.pdf` |
# MAGIC | 3 | Aadhaar Card | PDF | `{cust}_aadhaar_card.pdf` |
# MAGIC | 4 | Loan Application | PDF | `{cust}_loan_application.pdf` |
# MAGIC | 5 | Address Proof | JPEG | `{cust}_address_proof.jpg` |
# MAGIC | 6 | Income Proof | JPEG | `{cust}_income_proof.jpg` |