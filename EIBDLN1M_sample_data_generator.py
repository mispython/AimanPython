#!/usr/bin/env python3
"""
Sample Data Generator for EIBDLN1M Testing
Generates synthetic test data in the required parquet and text formats
"""

import duckdb
import random
from datetime import datetime, timedelta
from pathlib import Path

# Configuration
OUTPUT_DIR = Path("input")
REPORT_DATE = datetime(2026, 1, 28)
NUM_CUSTOMERS = 1000
NUM_BRANCHES = 50
MOVEMENT_THRESHOLD_PERCENT = 0.05  # 5% of accounts will have movements >= 1M

# Product codes
OD_PRODUCTS = [101, 102, 103, 105, 110, 120]  # Excluding 107, 173
RC_PRODUCTS = [302, 350, 364, 365, 506, 902, 903, 910, 925, 951]
HP_PRODUCTS = [128, 130, 131, 132, 380, 381, 700, 705, 720, 725]
TL_PRODUCTS = [201, 202, 203, 210, 220, 230, 240, 250, 260, 270]

# Create output directory
OUTPUT_DIR.mkdir(exist_ok=True)
print(f"Generating sample data in {OUTPUT_DIR}/")

# Initialize DuckDB
con = duckdb.connect(':memory:')

# ============================================================================
# 1. Generate Report Date
# ============================================================================
print("\n1. Generating report date file...")
con.execute(f"""
    CREATE TABLE reptdate AS
    SELECT DATE '{REPORT_DATE.strftime('%Y-%m-%d')}' as REPTDATE
""")

output_file = OUTPUT_DIR / "loan_reptdate.parquet"
con.execute(f"COPY reptdate TO '{output_file}' (FORMAT PARQUET)")
print(f"   Created: {output_file}")

# ============================================================================
# 2. Generate Branch Data
# ============================================================================
print("\n2. Generating branch data...")
branches = []
branch_names = ['HQ', 'JB', 'PG', 'ML', 'IPH', 'KL', 'KT', 'KCH', 'SGR', 'TRG']

with open(OUTPUT_DIR / "branch.txt", 'w') as f:
    for i in range(1, NUM_BRANCHES + 1):
        bank = 'A'
        branch = i
        brname = branch_names[(i - 1) % len(branch_names)]
        f.write(f"{bank}{branch:03d} {brname}\n")
        branches.append(branch)

print(f"   Created: {OUTPUT_DIR / 'branch.txt'} ({NUM_BRANCHES} branches)")

# ============================================================================
# 3. Generate Customer Data
# ============================================================================
print("\n3. Generating customer data...")

# Generate deposit customers
con.execute(f"""
    CREATE TABLE deposit_customers AS
    SELECT 
        1000000 + row_number() OVER () as ACCTNO,
        'CUSTOMER ' || LPAD((row_number() OVER ())::VARCHAR, 6, '0') || ' SDN BHD' as CUSTNAM1,
        CASE WHEN random() < 0.1 THEN 'Y' ELSE 'N' END as SECCUST
    FROM range({NUM_CUSTOMERS // 2})
""")

output_file = OUTPUT_DIR / "cisdp_deposit.parquet"
con.execute(f"COPY deposit_customers TO '{output_file}' (FORMAT PARQUET)")
print(f"   Created: {output_file}")

# Generate loan customers
con.execute(f"""
    CREATE TABLE loan_customers AS
    SELECT 
        2000000 + row_number() OVER () as ACCTNO,
        'BORROWER ' || LPAD((row_number() OVER ())::VARCHAR, 6, '0') || ' LIMITED' as CUSTNAM1,
        CASE WHEN random() < 0.15 THEN 'Y' ELSE 'N' END as SECCUST
    FROM range({NUM_CUSTOMERS // 2})
""")

output_file = OUTPUT_DIR / "cisln_loan.parquet"
con.execute(f"COPY loan_customers TO '{output_file}' (FORMAT PARQUET)")
print(f"   Created: {output_file}")

# ============================================================================
# 4. Generate Current Period Loan Data
# ============================================================================
print("\n4. Generating current period loan data...")

# Helper function to randomly select products
def get_random_product(acctype):
    if acctype == 'OD':
        return random.choice(OD_PRODUCTS)
    else:  # LN
        all_products = RC_PRODUCTS + HP_PRODUCTS + TL_PRODUCTS
        return random.choice(all_products)

# Create mixed account types
od_count = NUM_CUSTOMERS // 4
ln_count = NUM_CUSTOMERS - od_count

accounts = []
for i in range(NUM_CUSTOMERS):
    acctno = 1000000 + i
    branch = random.choice(branches)
    acctype = 'OD' if i < od_count else 'LN'
    product = get_random_product(acctype)
    
    # Generate realistic balances
    if acctype == 'OD':
        apprlimt = random.randint(100000, 50000000)
        balance = random.randint(0, int(apprlimt * 0.95))
    else:
        apprlimt = random.randint(50000, 100000000)
        balance = random.randint(0, apprlimt)
    
    apprlim2 = apprlimt * random.uniform(0.8, 1.2)
    
    accounts.append({
        'ACCTNO': acctno,
        'BRANCH': branch,
        'ACCTYPE': acctype,
        'PRODUCT': product,
        'APPRLIMT': apprlimt,
        'APPRLIM2': apprlim2,
        'BALANCE': float(balance)
    })

con.execute("CREATE TABLE current_loans AS SELECT * FROM accounts")

# Save to dated file
month = f"{REPORT_DATE.month:02d}"
day = f"{REPORT_DATE.day:02d}"
output_file = OUTPUT_DIR / f"loan_{month}{day}.parquet"
con.execute(f"COPY current_loans TO '{output_file}' (FORMAT PARQUET)")
print(f"   Created: {output_file} ({NUM_CUSTOMERS} accounts)")

# ============================================================================
# 5. Generate Previous Period Loan Data
# ============================================================================
print("\n5. Generating previous period loan data...")

prev_date = REPORT_DATE - timedelta(days=1)
prev_month = f"{prev_date.month:02d}"
prev_day = f"{prev_date.day:02d}"

# Generate previous balances with movements
# Most accounts have small movements, some have large movements
previous_accounts = []
num_large_movements = int(NUM_CUSTOMERS * MOVEMENT_THRESHOLD_PERCENT)

for i, acc in enumerate(accounts):
    prev_balance = acc['BALANCE']
    
    # Create large movement for selected accounts
    if i < num_large_movements:
        # Generate movement >= 1M
        movement = random.uniform(1000000, 10000000)
        if random.random() < 0.5:
            prev_balance = acc['BALANCE'] - movement
        else:
            prev_balance = acc['BALANCE'] + movement
        prev_balance = max(0, prev_balance)  # Ensure non-negative
    else:
        # Small random movement for others
        movement = random.uniform(-500000, 500000)
        prev_balance = max(0, acc['BALANCE'] - movement)
    
    previous_accounts.append({
        'ACCTNO': acc['ACCTNO'],
        'BRANCH': acc['BRANCH'],
        'ACCTYPE': acc['ACCTYPE'],
        'PRODUCT': acc['PRODUCT'],
        'APPRLIMT': acc['APPRLIMT'] * random.uniform(0.95, 1.05),  # Slight variation
        'APPRLIM2': acc['APPRLIM2'],
        'BALANCE': float(prev_balance)
    })

con.execute("CREATE TABLE previous_loans AS SELECT * FROM previous_accounts")

output_file = OUTPUT_DIR / f"loanx_{prev_month}{prev_day}.parquet"
con.execute(f"COPY previous_loans TO '{output_file}' (FORMAT PARQUET)")
print(f"   Created: {output_file} ({NUM_CUSTOMERS} accounts)")

# ============================================================================
# 6. Generate Summary Statistics
# ============================================================================
print("\n" + "="*70)
print("SAMPLE DATA GENERATION COMPLETE")
print("="*70)

# Calculate statistics
stats = con.execute("""
    SELECT 
        COUNT(*) as total_accounts,
        SUM(CASE WHEN c.ACCTYPE = 'OD' THEN 1 ELSE 0 END) as od_accounts,
        SUM(CASE WHEN c.ACCTYPE = 'LN' THEN 1 ELSE 0 END) as ln_accounts,
        SUM(CASE WHEN ABS(c.BALANCE - p.BALANCE) >= 1000000 THEN 1 ELSE 0 END) as large_movements,
        AVG(ABS(c.BALANCE - p.BALANCE)) as avg_movement,
        MAX(ABS(c.BALANCE - p.BALANCE)) as max_movement
    FROM current_loans c
    JOIN previous_loans p ON c.ACCTNO = p.ACCTNO
""").fetchone()

print(f"\nData Statistics:")
print(f"  Total Accounts: {stats[0]:,}")
print(f"  OD Accounts: {stats[1]:,}")
print(f"  LN Accounts: {stats[2]:,}")
print(f"  Accounts with movements >= RM1M: {stats[3]:,}")
print(f"  Average Movement: RM {stats[4]:,.2f}")
print(f"  Maximum Movement: RM {stats[5]:,.2f}")

print(f"\nGenerated Files:")
print(f"  {OUTPUT_DIR / 'loan_reptdate.parquet'}")
print(f"  {OUTPUT_DIR / 'cisdp_deposit.parquet'}")
print(f"  {OUTPUT_DIR / 'cisln_loan.parquet'}")
print(f"  {OUTPUT_DIR / 'branch.txt'}")
print(f"  {OUTPUT_DIR / f'loan_{month}{day}.parquet'}")
print(f"  {OUTPUT_DIR / f'loanx_{prev_month}{prev_day}.parquet'}")

print(f"\nYou can now run: python eibdln1m_converter.py")
print("="*70)

# Close connection
con.close()
