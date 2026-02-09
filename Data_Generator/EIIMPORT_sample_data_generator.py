"""
Sample Data Generator for EIIMPORT Report
Generates realistic test data for Islamic Banking Portfolio Exposure Report
"""

import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import random
import numpy as np

# ============================================================================
# CONFIGURATION
# ============================================================================
OUTPUT_PATH = Path("/data/input")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# Report parameters
REPORT_DATE = datetime(2024, 6, 25)  # Week 4 of June 2024
REPORT_YEAR_2 = "24"
REPORT_MONTH = "06"
REPORT_DAY = "25"
WEEK = "4"

NUM_SAVING_ACCTS = 500
NUM_CURRENT_ACCTS = 800
NUM_FD_ACCTS = 300
NUM_EQUT_TRANS = 50
NUM_INI_TRANS = 20


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def generate_account_number(prefix, idx):
    """Generate account number"""
    return f"{prefix}{idx:010d}"


def generate_branch():
    """Generate branch code"""
    branches = [f"BR{i:03d}" for i in range(1, 21)]
    return random.choice(branches)


def generate_custcode():
    """Generate customer code"""
    return f"CUST{random.randint(100000, 999999)}"


# ============================================================================
# 1. GENERATE REPTDATE (REPORTING DATE)
# ============================================================================

def generate_reptdate():
    """Generate reporting date file"""
    reptdate_df = pl.DataFrame({
        "REPTDATE": [REPORT_DATE]
    })

    output_file = OUTPUT_PATH / "reptdate.parquet"
    reptdate_df.write_parquet(output_file)
    print(f"✓ Generated: {output_file}")
    return reptdate_df


# ============================================================================
# 2. GENERATE SAVING ACCOUNTS
# ============================================================================

def generate_saving_data(num_records=NUM_SAVING_ACCTS):
    """Generate saving account data for products 204, 207, 214, 215"""

    np.random.seed(42)
    random.seed(42)

    # Products: 204, 207, 214, 215 (Wadiah Savings)
    products = [204, 207, 214, 215]

    # Open indicators (exclude B, C, P)
    openinds = ['A', 'O', 'N']

    # Race codes
    races = ['1', '2', '3', '4']  # Malay, Chinese, Indian, Other

    records = []

    for i in range(num_records):
        product = random.choice(products)
        openind = random.choice(openinds)

        # Balance - positive values
        curbal = round(random.uniform(100, 500000), 2)

        # Interest payable
        intpaybl = round(curbal * random.uniform(0.01, 0.03), 2)

        records.append({
            "ACCTNO": generate_account_number("SAV", i),
            "PRODUCT": product,
            "OPENIND": openind,
            "CURBAL": curbal,
            "INTPAYBL": intpaybl,
            "CUSTCODE": generate_custcode(),
            "BRANCH": generate_branch(),
            "RACE": random.choice(races)
        })

    saving_df = pl.DataFrame(records)

    output_file = OUTPUT_PATH / "saving.parquet"
    saving_df.write_parquet(output_file)
    print(f"✓ Generated: {output_file}")
    return saving_df


# ============================================================================
# 3. GENERATE CURRENT ACCOUNTS
# ============================================================================

def generate_current_data(num_records=NUM_CURRENT_ACCTS):
    """Generate current account data for various Islamic products"""

    np.random.seed(50)
    random.seed(50)

    # Products by concept
    wadiah_products = [60, 61, 62, 63, 64, 66, 67, 93, 96, 97, 160, 161, 162, 163, 164, 165, 182]
    bba_products = [32, 70, 73, 94, 95, 166, 183, 184, 185, 187, 188]
    murabahah_products = [169]
    inah_products = [33, 71, 167, 168]
    qard_products = [440, 441, 442, 443, 444]
    musyarakah_products = [186]

    all_products = (wadiah_products + bba_products + murabahah_products +
                    inah_products + qard_products + musyarakah_products)

    openinds = ['A', 'O', 'N']
    races = ['1', '2', '3', '4']

    records = []

    for i in range(num_records):
        product = random.choice(all_products)
        openind = random.choice(openinds)

        # Balance
        curbal = round(random.uniform(500, 1000000), 2)

        # Interest payable
        intpaybl = round(curbal * random.uniform(0.005, 0.025), 2)

        records.append({
            "ACCTNO": generate_account_number("CUR", i),
            "PRODUCT": product,
            "OPENIND": openind,
            "CURBAL": curbal,
            "INTPAYBL": intpaybl,
            "CUSTCODE": generate_custcode(),
            "BRANCH": generate_branch(),
            "RACE": random.choice(races)
        })

    current_df = pl.DataFrame(records)

    output_file = OUTPUT_PATH / "current.parquet"
    current_df.write_parquet(output_file)
    print(f"✓ Generated: {output_file}")
    return current_df


# ============================================================================
# 4. GENERATE FIXED DEPOSIT ACCOUNTS
# ============================================================================

def generate_fd_data(num_records=NUM_FD_ACCTS):
    """Generate FD data for MGIA, Commodity Murabahah products"""

    np.random.seed(60)
    random.seed(60)

    # Products
    mgia_products = [302, 396, 315, 394]  # Mudarabah, Istismar
    commodity_products = [316, 393]  # Commodity Murabahah

    all_products = mgia_products + commodity_products

    openinds = ['A', 'O', 'N']
    races = ['1', '2', '3', '4']

    records = []

    for i in range(num_records):
        product = random.choice(all_products)
        openind = random.choice(openinds)

        # FD balances typically higher
        curbal = round(random.uniform(10000, 5000000), 2)

        # Interest payable
        intpaybl = round(curbal * random.uniform(0.02, 0.05), 2)

        records.append({
            "ACCTNO": generate_account_number("FD", i),
            "PRODUCT": product,
            "OPENIND": openind,
            "CURBAL": curbal,
            "INTPAYBL": intpaybl,
            "CUSTCODE": generate_custcode(),
            "BRANCH": generate_branch(),
            "RACE": random.choice(races)
        })

    fd_df = pl.DataFrame(records)

    output_file = OUTPUT_PATH / "fd.parquet"
    fd_df.write_parquet(output_file)
    print(f"✓ Generated: {output_file}")
    return fd_df


# ============================================================================
# 5. GENERATE EQUITY/DERIVATIVES DATA (EQUT)
# ============================================================================

def generate_equt_data(num_records=NUM_EQUT_TRANS):
    """Generate equity/derivatives transactions"""

    np.random.seed(70)
    random.seed(70)

    # Deal types
    deal_types = ['BCS', 'BCI', 'BCT', 'BCW']  # Istismar, Mudarabah, Commodity, Wadiah

    records = []

    for i in range(num_records):
        dealtype = random.choice(deal_types)

        # Amount paid
        amtpay = round(random.uniform(50000, 10000000), 2)

        records.append({
            "DEALID": f"DEAL{i:06d}",
            "DEALTYPE": dealtype,
            "AMTPAY": amtpay,
            "DEALDATE": REPORT_DATE - timedelta(days=random.randint(0, 30))
        })

    equt_df = pl.DataFrame(records)

    # Generate filename with date
    output_file = OUTPUT_PATH / f"iutfx{REPORT_YEAR_2}{REPORT_MONTH}{REPORT_DAY}.parquet"
    equt_df.write_parquet(output_file)
    print(f"✓ Generated: {output_file}")
    return equt_df


# ============================================================================
# 6. GENERATE W1AL (INI) DATA
# ============================================================================

def generate_w1al_data(num_records=NUM_INI_TRANS):
    """Generate W1AL (Bai Al Inah) data"""

    np.random.seed(80)
    random.seed(80)

    records = []

    for i in range(num_records):
        # Amount
        amount = round(random.uniform(100000, 5000000), 2)

        records.append({
            "SET_ID": "F142150NIDI",
            "TRANS_ID": f"INI{i:08d}",
            "AMOUNT": amount,
            "TRANS_DATE": REPORT_DATE - timedelta(days=random.randint(0, 7))
        })

    w1al_df = pl.DataFrame(records)

    # Generate filename with month and week
    output_file = OUTPUT_PATH / f"w1al{REPORT_MONTH}{WEEK}.parquet"
    w1al_df.write_parquet(output_file)
    print(f"✓ Generated: {output_file}")
    return w1al_df


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Generate all sample data files"""

    print("=" * 70)
    print("EIIMPORT Sample Data Generator")
    print("=" * 70)
    print(f"Report Date: {REPORT_DATE.strftime('%Y-%m-%d')}")
    print(f"Week: {WEEK}")
    print("=" * 70)
    print()

    # 1. Generate reporting date
    print("Generating reporting date file...")
    reptdate_df = generate_reptdate()
    print()

    # 2. Generate saving accounts
    print("Generating saving account data...")
    saving_df = generate_saving_data(NUM_SAVING_ACCTS)
    print()

    # 3. Generate current accounts
    print("Generating current account data...")
    current_df = generate_current_data(NUM_CURRENT_ACCTS)
    print()

    # 4. Generate fixed deposits
    print("Generating fixed deposit data...")
    fd_df = generate_fd_data(NUM_FD_ACCTS)
    print()

    # 5. Generate equity/derivatives
    print("Generating equity/derivatives data...")
    equt_df = generate_equt_data(NUM_EQUT_TRANS)
    print()

    # 6. Generate W1AL (INI) data
    print("Generating W1AL (Bai Al Inah) data...")
    w1al_df = generate_w1al_data(NUM_INI_TRANS)
    print()

    # Summary statistics
    print("=" * 70)
    print("DATA GENERATION SUMMARY")
    print("=" * 70)
    print(f"Reporting Date Records: {len(reptdate_df)}")
    print(f"Saving Accounts: {len(saving_df)}")
    print(f"Current Accounts: {len(current_df)}")
    print(f"Fixed Deposit Accounts: {len(fd_df)}")
    print(f"Equity/Derivatives Transactions: {len(equt_df)}")
    print(f"W1AL (INI) Transactions: {len(w1al_df)}")
    print()

    # Distribution by product type
    print("SAVINGS PRODUCT DISTRIBUTION:")
    sav_dist = saving_df.group_by("PRODUCT").agg(pl.len().alias("count")).sort("PRODUCT")
    print(sav_dist)
    print()

    print("CURRENT PRODUCT DISTRIBUTION (Top 10):")
    cur_dist = current_df.group_by("PRODUCT").agg(pl.len().alias("count")).sort("count", descending=True).head(10)
    print(cur_dist)
    print()

    print("FD PRODUCT DISTRIBUTION:")
    fd_dist = fd_df.group_by("PRODUCT").agg(pl.len().alias("count")).sort("PRODUCT")
    print(fd_dist)
    print()

    print("EQUITY DEALTYPE DISTRIBUTION:")
    equt_dist = equt_df.group_by("DEALTYPE").agg(pl.len().alias("count")).sort("DEALTYPE")
    print(equt_dist)
    print()

    # Balance statistics
    print("BALANCE STATISTICS:")
    print(f"Savings Total: RM {saving_df.select(pl.col('CURBAL').sum()).item(0, 0):,.2f}")
    print(f"Current Total: RM {current_df.select(pl.col('CURBAL').sum()).item(0, 0):,.2f}")
    print(f"FD Total: RM {fd_df.select(pl.col('CURBAL').sum()).item(0, 0):,.2f}")
    print(f"Equity Total: RM {equt_df.select(pl.col('AMTPAY').sum()).item(0, 0):,.2f}")
    print(f"INI Total: RM {w1al_df.select(pl.col('AMOUNT').sum()).item(0, 0):,.2f}")
    print()

    print("=" * 70)
    print("✓ All sample data files generated successfully!")
    print(f"✓ Output directory: {OUTPUT_PATH}")
    print("=" * 70)


if __name__ == "__main__":
    main()
