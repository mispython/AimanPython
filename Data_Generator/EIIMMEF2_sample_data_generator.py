"""
Sample Data Generator for EIIMMEF2 Report
Generates realistic test data in Parquet format for testing the report generation
"""

import polars as pl
import pyarrow.parquet as pq
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
REPORT_DATE = datetime(2024, 3, 31)  # March 2024
CURRENT_MONTH = 3
PREVIOUS_MONTH = 2
WEEK = 4
REPORT_YEAR = "24"

NUM_ACCOUNTS = 500  # Number of sample accounts to generate


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def generate_account_number(idx):
    """Generate realistic account number"""
    return f"ACC{idx:08d}"


def generate_note_number(idx):
    """Generate note number"""
    return f"NOTE{idx:05d}"


def generate_branch_code():
    """Generate branch code"""
    branches = ["BR001", "BR002", "BR003", "BR004", "BR005"]
    return random.choice(branches)


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
# 2. GENERATE LOAN DATA (CURRENT AND PREVIOUS MONTH)
# ============================================================================

def generate_loan_data(month, num_records=NUM_ACCOUNTS):
    """Generate loan data for a specific month"""

    np.random.seed(42 + month)
    random.seed(42 + month)

    accounts = []

    for i in range(num_records):
        acctno = generate_account_number(i)
        noteno = generate_note_number(i)
        branch = generate_branch_code()

        # Product selection (428 or 439)
        product = random.choice([428, 439])

        # CENSUS codes based on product
        if product == 428:
            census = random.choice([428.00, 428.01, 428.02, 428.03])
        else:  # 439
            census = 439.00

        # Balance (some with 0 balance)
        balance = round(random.uniform(1000, 500000), 2) if random.random() > 0.1 else 0

        # PAIDIND (Paid indicator)
        paidind = random.choice(['A', 'P', 'C', 'A', 'A', 'A'])  # Mostly active

        # LOANSTAT (Loan status: 1=Performing, 3=Non-performing)
        loanstat = random.choices([1, 3], weights=[0.85, 0.15])[0]

        accounts.append({
            "ACCTNO": acctno,
            "NOTENO": noteno,
            "BRANCH": branch,
            "PRODUCT": product,
            "CENSUS": census,
            "BALANCE": balance,
            "PAIDIND": paidind,
            "LOANSTAT": loanstat,
            "CUSTNAME": f"Customer {i}",
            "OPENDATE": REPORT_DATE - timedelta(days=random.randint(30, 1825))
        })

    loan_df = pl.DataFrame(accounts)

    output_file = OUTPUT_PATH / f"loan_{month:02d}{WEEK}.parquet"
    loan_df.write_parquet(output_file)
    print(f"✓ Generated: {output_file}")
    return loan_df


# ============================================================================
# 3. GENERATE DISPAY DATA (DISBURSEMENT AND PAYMENT)
# ============================================================================

def generate_dispay_data(loan_df, month):
    """Generate disbursement and repayment data"""

    np.random.seed(100 + month)
    random.seed(100 + month)

    # Select subset of accounts with transactions
    transaction_accounts = loan_df.sample(fraction=0.4)

    dispay_records = []

    for row in transaction_accounts.iter_rows(named=True):
        acctno = row["ACCTNO"]
        noteno = row["NOTENO"]

        # Randomly generate disbursement or repayment
        has_disburse = random.random() > 0.7
        has_repaid = random.random() > 0.5

        disburse = round(random.uniform(5000, 100000), 2) if has_disburse else 0
        repaid = round(random.uniform(500, 20000), 2) if has_repaid else 0

        if disburse > 0 or repaid > 0:
            dispay_records.append({
                "ACCTNO": acctno,
                "NOTENO": noteno,
                "DISBURSE": disburse,
                "REPAID": repaid,
                "TRANSDATE": REPORT_DATE.replace(day=random.randint(1, 28))
            })

    dispay_df = pl.DataFrame(dispay_records)

    output_file = OUTPUT_PATH / f"idispaymth{month:02d}.parquet"
    dispay_df.write_parquet(output_file)
    print(f"✓ Generated: {output_file}")
    return dispay_df


# ============================================================================
# 4. GENERATE CCRIS DATA (CREDIT BUREAU DATA)
# ============================================================================

def generate_ccris_data(loan_df, month, year):
    """Generate CCRIS credit submission data"""

    np.random.seed(200)
    random.seed(200)

    ccris_records = []

    # Only include non-performing loans and some performing ones
    for row in loan_df.iter_rows(named=True):
        if row["LOANSTAT"] == 3 or random.random() > 0.7:
            acctno = row["ACCTNO"]
            noteno = row["NOTENO"]
            branch = row["BRANCH"]

            # Generate arrears information
            if row["LOANSTAT"] == 3:
                # Non-performing: higher arrears
                mtharr = random.randint(1, 18)
                daysarr = mtharr * 30 + random.randint(0, 29)
            else:
                # Performing: low or no arrears
                mtharr = random.randint(0, 2)
                daysarr = mtharr * 30 + random.randint(0, 29)

            ccris_records.append({
                "ACCTNUM": acctno,
                "NOTENO": noteno,
                "BRANCH": branch,
                "DAYSARR": daysarr,
                "MTHARR": mtharr,
                "REPORTDATE": REPORT_DATE
            })

    ccris_df = pl.DataFrame(ccris_records)

    output_file = OUTPUT_PATH / f"icredmsubac{month:02d}{year}.parquet"
    ccris_df.write_parquet(output_file)
    print(f"✓ Generated: {output_file}")
    return ccris_df


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Generate all sample data files"""

    print("=" * 70)
    print("EIIMMEF2 Sample Data Generator")
    print("=" * 70)
    print(f"Report Date: {REPORT_DATE.strftime('%Y-%m-%d')}")
    print(f"Current Month: {CURRENT_MONTH:02d}")
    print(f"Previous Month: {PREVIOUS_MONTH:02d}")
    print(f"Number of Accounts: {NUM_ACCOUNTS}")
    print("=" * 70)
    print()

    # 1. Generate reporting date
    print("Generating reporting date file...")
    reptdate_df = generate_reptdate()
    print()

    # 2. Generate loan data for previous month
    print("Generating previous month loan data...")
    prvmth_df = generate_loan_data(PREVIOUS_MONTH, NUM_ACCOUNTS)
    print()

    # 3. Generate loan data for current month
    print("Generating current month loan data...")
    curmth_df = generate_loan_data(CURRENT_MONTH, NUM_ACCOUNTS)
    print()

    # 4. Generate disbursement/payment data
    print("Generating disbursement and payment data...")
    dispay_df = generate_dispay_data(curmth_df, CURRENT_MONTH)
    print()

    # 5. Generate CCRIS data
    print("Generating CCRIS credit submission data...")
    ccris_df = generate_ccris_data(curmth_df, CURRENT_MONTH, REPORT_YEAR)
    print()

    # Summary statistics
    print("=" * 70)
    print("DATA GENERATION SUMMARY")
    print("=" * 70)
    print(f"Reporting Date Records: {len(reptdate_df)}")
    print(f"Previous Month Loans: {len(prvmth_df)}")
    print(f"Current Month Loans: {len(curmth_df)}")
    print(f"Disbursement/Payment Records: {len(dispay_df)}")
    print(f"CCRIS Records: {len(ccris_df)}")
    print()

    # Distribution statistics
    print("PRODUCT DISTRIBUTION (Current Month):")
    product_dist = curmth_df.group_by("PRODUCT").agg(pl.len().alias("count"))
    print(product_dist)
    print()

    print("CENSUS DISTRIBUTION (Current Month):")
    census_dist = curmth_df.group_by("CENSUS").agg(pl.len().alias("count"))
    print(census_dist)
    print()

    print("LOAN STATUS DISTRIBUTION (Current Month):")
    status_dist = curmth_df.group_by("LOANSTAT").agg(pl.len().alias("count"))
    print(status_dist)
    print()

    print("CGC CLASSIFICATION:")
    with_cgc = curmth_df.filter(
        pl.col("CENSUS").is_in([428.00, 428.02])
    ).height
    without_cgc = curmth_df.filter(
        (pl.col("CENSUS").is_in([428.01, 428.03])) | (pl.col("PRODUCT") == 439)
    ).height
    print(f"With CGC Guaranteed: {with_cgc}")
    print(f"Without CGC Guaranteed: {without_cgc}")
    print()

    print("=" * 70)
    print("✓ All sample data files generated successfully!")
    print(f"✓ Output directory: {OUTPUT_PATH}")
    print("=" * 70)


if __name__ == "__main__":
    main()
