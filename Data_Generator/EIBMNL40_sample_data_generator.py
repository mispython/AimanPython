#!/usr/bin/env python3
"""
For EIBMNL40
Sample Test Data Generator for BNM Note Conversion
Creates synthetic data to test the Python conversion
"""

import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import random

# Set up directories
INPUT_DIR = Path("./input")
BNM_DIR = INPUT_DIR / "bnm"
BNM1_DIR = INPUT_DIR / "bnm1"

BNM_DIR.mkdir(parents=True, exist_ok=True)
BNM1_DIR.mkdir(parents=True, exist_ok=True)


def create_reptdate():
    """Create sample reporting date"""
    reptdate = datetime(2024, 1, 15)  # Week 2 of January 2024

    df = pl.DataFrame({
        'reptdate': [reptdate]
    })

    df.write_parquet(BNM_DIR / "reptdate.parquet")
    print(f"Created reptdate.parquet with date: {reptdate}")

    return reptdate


def create_sample_loans(reptdate):
    """Create sample loan data"""

    # Generate 100 sample loans
    n_loans = 100

    # Generate realistic loan data
    data = []

    for i in range(n_loans):
        # Random loan parameters
        product = random.choice([100, 101, 200, 201, 300, 350, 225, 226, 910, 925])
        acctype = random.choice(['LN', 'LN', 'LN', 'LN', 'OD'])  # 80% loans, 20% OD
        custcd = random.choice(['10', '20', '30', '77', '78', '95', '96'])

        # Product code
        if product in [100, 101]:
            prodcd = '3410' + str(random.randint(0, 9))
        elif product in [200, 201]:
            prodcd = '3420' + str(random.randint(0, 9))
        elif product == 300:
            prodcd = '34240'
        else:
            prodcd = '3429' + str(random.randint(0, 9))

        # Dates
        issue_date = reptdate - timedelta(days=random.randint(30, 1825))  # 1 month to 5 years ago

        if acctype == 'OD':
            expr_date = reptdate + timedelta(days=random.randint(7, 365))
            bl_date = None
            payfreq = None
            payamt = 0
        else:
            expr_date = issue_date + timedelta(days=random.randint(180, 3650))  # 6 months to 10 years
            bl_date = issue_date + timedelta(days=random.randint(0, 30))
            payfreq = random.choice(['1', '2', '3', '4', '6'])
            payamt = random.randint(500, 5000)

        # Balance
        balance = random.randint(5000, 500000)

        # Loan status
        loanstat = random.choice([1, 1, 1, 2])  # 75% performing

        # Paid indicator
        paidind = random.choice(['A', 'A', 'A', 'P', 'C'])  # Mostly active

        # EIR adjustment (only for some loans)
        eir_adj = random.randint(-1000, 1000) if random.random() < 0.1 else None

        data.append({
            'loanid': f'L{i + 1:06d}',
            'prodcd': prodcd,
            'product': product,
            'custcd': custcd,
            'acctype': acctype,
            'balance': balance,
            'payamt': payamt,
            'bldate': bl_date,
            'issdte': issue_date,
            'exprdate': expr_date,
            'payfreq': payfreq,
            'loanstat': loanstat,
            'paidind': paidind,
            'eir_adj': eir_adj
        })

    df = pl.DataFrame(data)

    # Save to parquet
    filename = f"loan{reptdate.strftime('%m')}2.parquet"
    df.write_parquet(BNM1_DIR / filename)
    print(f"Created {filename} with {n_loans} loans")
    print(f"  - Loan accounts: {len(df.filter(pl.col('acctype') == 'LN'))}")
    print(f"  - OD accounts: {len(df.filter(pl.col('acctype') == 'OD'))}")
    print(f"  - Staff customers: {len(df.filter(pl.col('custcd').is_in(['77', '78', '95', '96'])))}")

    return df


def main():
    """Generate all test data"""
    print("Generating test data...")
    print("=" * 60)

    reptdate = create_reptdate()
    print()

    loan_df = create_sample_loans(reptdate)
    print()

    print("=" * 60)
    print("Test data generation complete!")
    print()
    print("You can now run: python bnm_note_conversion.py")


if __name__ == "__main__":
    main()
