#!/usr/bin/env python3
"""
Sample Data Generator for Loan Foreign Entities Report
Generates synthetic test data for the three reports
"""

import duckdb
import random
import pandas as pd
from datetime import datetime
from pathlib import Path

# Configuration
OUTPUT_DIR = Path("input/EIBHLNGR")
REPORT_DATE = datetime(2026, 1, 28)
NUM_LOANS = 500
NUM_GUARANTEES_PCT = 0.4  # 40% of loans will have guarantees

# Customer codes for foreign entities (80-99)
FOREIGN_BANKING_CODES = ['80', '81', '82', '83', '84']
NON_FOREIGN_BANKING_CODES = ['85', '86', '87', '88', '89', '90', '91', '92', '95', '96', '98', '99']

# Product codes
PRODUCT_CODES = ['302', '350', '364', '365', '506', '128', '130', '131', '201', '210']

# Paid indicators
PAID_INDICATORS = ['A', 'B', 'C', 'D', 'P', 'C']  # Some paid, some active

# Guarantee nature codes
GUARANTEE_NATURE_CODES = ['01', '02', '03', '04', '05', '06', '07']

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
# 2. Generate Loan Data
# ============================================================================
print("\n2. Generating loan data...")

loans = []
for i in range(NUM_LOANS):
    acctno = 1000000 + i
    noteno = f"{random.randint(1, 999):03d}"

    # Mix of foreign banking and non-foreign banking
    if random.random() < 0.3:
        custcd = random.choice(FOREIGN_BANKING_CODES)
    else:
        custcd = random.choice(NON_FOREIGN_BANKING_CODES)

    product = random.choice(PRODUCT_CODES)
    paidind = random.choice(PAID_INDICATORS)

    # Some loans have EIR adjustments
    eir_adj = random.uniform(-10000, 10000) if random.random() < 0.3 else None

    # Generate realistic balances
    bal_aft_eir = random.uniform(50000, 50000000)

    loans.append({
        'ACCTNO': acctno,
        'NOTENO': noteno,
        'PRODUCT': product,
        'CUSTCD': custcd,
        'PAIDIND': paidind,
        'EIR_ADJ': eir_adj,
        'BAL_AFT_EIR': bal_aft_eir
    })

# con.execute("CREATE TABLE loan_data AS SELECT * FROM loans")
df_loans = pd.DataFrame(loans)
con.execute("CREATE TABLE loan_data AS SELECT * FROM df_loans")

# Filter to only include foreign entities (CUSTCD >= 80) and not fully paid
# This matches the SAS WHERE clause
filtered_loans = con.execute("""
    SELECT * FROM loan_data
    WHERE (PAIDIND NOT IN ('P', 'C') OR EIR_ADJ IS NOT NULL)
      AND CAST(CUSTCD AS INTEGER) >= 80
""").df()

print(f"   Total loans generated: {len(loans):,}")
print(f"   Loans meeting criteria: {len(filtered_loans):,}")

# Save to dated file (month 01, day 4)
month = f"{REPORT_DATE.month:02d}"
output_file = OUTPUT_DIR / f"loan_{month}4.parquet"
con.register('filtered_loans', filtered_loans)
con.execute(f"COPY filtered_loans TO '{output_file}' (FORMAT PARQUET)")
print(f"   Created: {output_file}")

# ============================================================================
# 3. Generate Collateral/Guarantee Data
# ============================================================================
print("\n3. Generating collateral/guarantee data...")

guarantees = []
num_guarantees = int(len(filtered_loans) * NUM_GUARANTEES_PCT)

# Select random subset of loans to have guarantees
sampled_loans = filtered_loans.sample(n=num_guarantees)

for idx, loan in sampled_loans.iterrows():
    # Generate 1-3 collateral records per loan
    num_coll = random.randint(1, 3)

    for j in range(num_coll):
        ccollno = f"COLL{loan['ACCTNO']}{j + 1:02d}"
        cdolarv = random.uniform(10000, loan['BAL_AFT_EIR'] * 1.5)
        cguarnat = random.choice(GUARANTEE_NATURE_CODES)

        guarantees.append({
            'ACCTNO': loan['ACCTNO'],
            'NOTENO': loan['NOTENO'],
            'CCOLLNO': ccollno,
            'CDOLARV': cdolarv,
            'CGUARNAT': cguarnat
        })

# con.execute("CREATE TABLE guarantee_data AS SELECT * FROM guarantees")
df_guarantees = pd.DataFrame(guarantees)
con.execute("CREATE TABLE guarantee_data AS SELECT * FROM df_guarantees")

output_file = OUTPUT_DIR / "coll_collater.parquet"
con.execute(f"COPY guarantee_data TO '{output_file}' (FORMAT PARQUET)")
print(f"   Created: {output_file}")
print(f"   Collateral records: {len(guarantees):,}")

# ============================================================================
# 4. Generate Statistics
# ============================================================================
print("\n" + "=" * 70)
print("SAMPLE DATA GENERATION COMPLETE")
print("=" * 70)

# Calculate statistics
stats = con.execute("""
    SELECT 
        COUNT(*) as total_loans,
        COUNT(DISTINCT ACCTNO) as unique_accounts,
        SUM(CASE WHEN CAST(CUSTCD AS INTEGER) < 85 THEN 1 ELSE 0 END) as foreign_banking,
        SUM(CASE WHEN CAST(CUSTCD AS INTEGER) >= 85 THEN 1 ELSE 0 END) as non_foreign_banking,
        AVG(BAL_AFT_EIR) as avg_balance,
        SUM(BAL_AFT_EIR) as total_outstanding
    FROM filtered_loans
""").fetchone()

print(f"\nLoan Data Statistics:")
print(f"  Total loans: {stats[0]:,}")
print(f"  Unique accounts: {stats[1]:,}")
print(f"  Foreign banking institutions (< 85): {stats[2]:,}")
print(f"  Non-foreign banking institutions (>= 85): {stats[3]:,}")
print(f"  Average balance: ${stats[4]:,.2f}")
print(f"  Total outstanding: ${stats[5]:,.2f}")

# Guarantee statistics
guar_stats = con.execute("""
    SELECT 
        COUNT(DISTINCT l.ACCTNO) as loans_with_guarantee,
        COUNT(DISTINCT l.ACCTNO) * 100.0 / (SELECT COUNT(DISTINCT ACCTNO) FROM filtered_loans) as pct_guaranteed,
        AVG(g.CDOLARV) as avg_collateral_value,
        SUM(g.CDOLARV) as total_collateral_value
    FROM filtered_loans l
    INNER JOIN guarantee_data g ON l.ACCTNO = g.ACCTNO
""").fetchone()

print(f"\nGuarantee Statistics:")
print(f"  Loans with guarantees: {guar_stats[0]:,}")
print(f"  Percentage guaranteed: {guar_stats[1]:.1f}%")
print(f"  Average collateral value: ${guar_stats[2]:,.2f}")
print(f"  Total collateral value: ${guar_stats[3]:,.2f}")

# Customer code distribution
print(f"\nCustomer Code Distribution:")
custcd_dist = con.execute("""
    SELECT 
        CUSTCD,
        COUNT(*) as count,
        SUM(BAL_AFT_EIR) as total_balance
    FROM filtered_loans
    GROUP BY CUSTCD
    ORDER BY CAST(CUSTCD AS INTEGER)
""").fetchall()

for custcd, count, balance in custcd_dist:
    print(f"  Code {custcd}: {count:,} loans, ${balance:,.2f}")

print(f"\nGenerated Files:")
print(f"  {OUTPUT_DIR / 'loan_reptdate.parquet'}")
print(f"  {OUTPUT_DIR / f'loan_{month}4.parquet'}")
print(f"  {OUTPUT_DIR / 'coll_collater.parquet'}")

print(f"\nYou can now run: EIBHLNGR.py")
print("=" * 70)

# Close connection
con.close()
