#!/usr/bin/env python3
"""
For EIBMNL41
Sample Test Data Generator for BNM Calc Conversion
Creates synthetic data to test the maturity profile calculations
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


def create_table():
    """Create maturity bucket reference table"""

    # Define maturity buckets
    data = [
        {'basket': 1, 'remmth': 0.1},  # Up to 1 week
        {'basket': 2, 'remmth': 0.5},  # >1 week to 1 month
        {'basket': 3, 'remmth': 2.0},  # >1 month to 3 months
        {'basket': 4, 'remmth': 4.5},  # >3 months to 6 months
        {'basket': 5, 'remmth': 9.0},  # >6 months to 1 year
        {'basket': 6, 'remmth': 13.0},  # >1 year
    ]

    df = pl.DataFrame(data)
    df.write_parquet(BNM_DIR / "table.parquet")
    print(f"Created table.parquet with {len(df)} maturity buckets")

    return df


def create_note():
    """Create sample NOTE data (output from previous program)"""

    data = []

    # Create sample records for different products and customer types
    products = [
        ('9521308', 100000),  # OD Individual
        ('9521309', 500000),  # OD Corporate
        ('9521908', 50000),  # SMC Individual
        ('9521909', 200000),  # SMC Corporate
    ]

    for prefix, base_amount in products:
        # Create records for different maturity buckets
        for bucket in ['01', '02', '03', '04', '05', '06']:
            amount = base_amount * random.uniform(0.8, 1.2)
            bnmcode = f"{prefix}{bucket}0000Y"
            data.append({
                'bnmcode': bnmcode,
                'amount': amount
            })

    df = pl.DataFrame(data)
    df.write_parquet(BNM_DIR / "note.parquet")
    print(f"Created note.parquet with {len(df)} records")

    return df


def create_walk_file():
    """Create sample WALK file for PBCARD data"""

    walk_path = INPUT_DIR / "walk.txt"

    # Create sample records in fixed-width format
    # @2 DESC $19., @21 DD 2., @24 MM 2., @27 YY 2., @42 AMOUNT COMMA20.2

    lines = [
        # Header or other records
        " 34100              010124           1,234,567.89",
        " 34200              150124           5,678,901.23",  # This is the PBCARD record
        " 34300              150124             987,654.32",
    ]

    with open(walk_path, 'w') as f:
        for line in lines:
            f.write(line + '\n')

    print(f"Created walk.txt with {len(lines)} records")
    print(f"  PBCARD amount: 5,678,901.23")


def create_historical_store_data():
    """Create 48 weeks of historical data for each product"""

    base_date = datetime(2024, 1, 15)  # Current reporting date

    products = ['CARD', 'ODCORP', 'ODIND', 'SMCCORP', 'SMCIND']
    base_amounts = {
        'CARD': 5000000,
        'ODCORP': 500000,
        'ODIND': 100000,
        'SMCCORP': 200000,
        'SMCIND': 50000
    }

    for prod in products:
        base_amount = base_amounts[prod]

        # Generate 48 weeks of history
        data = []
        for week in range(48, 0, -1):
            # Calculate date (going back week by week)
            week_date = base_date - timedelta(weeks=week)
            julian_date = int(week_date.strftime('%y%j'))

            # Create varying amounts (trend + noise)
            # Minimum will be around week 24
            if week == 24:
                amount = base_amount * 0.75  # Minimum
            else:
                trend = 1.0 + (48 - week) * 0.005  # Slight upward trend
                noise = random.uniform(0.95, 1.05)
                amount = base_amount * trend * noise

            data.append({
                'reptdate': julian_date,
                'amount': amount
            })

        # Create base and store files
        df = pl.DataFrame(data)
        df = df.sort('reptdate')

        base_path = BNM_DIR / f"base_{prod}.parquet"
        store_path = BNM_DIR / f"store_{prod}.parquet"

        df.write_parquet(base_path)
        df.write_parquet(store_path)

        print(f"Created historical data for {prod}:")
        print(f"  Records: {len(df)}")
        print(f"  Min amount: {df['amount'].min():,.2f}")
        print(f"  Max amount: {df['amount'].max():,.2f}")
        print(f"  Current: {df['amount'][-1]:,.2f}")


def create_loan_data(reptdate):
    """Create sample loan data for SMC processing"""

    # Generate sample loans
    n_loans = 50

    data = []

    for i in range(n_loans):
        # Random loan parameters
        product = random.choice([100, 101, 200, 225, 226, 300, 350])
        acctype = random.choice(['LN', 'LN', 'OD'])  # 67% loans, 33% OD
        custcd = random.choice(['10', '20', '30', '77', '78', '95', '96'])

        # Product code - some are SMC (34240)
        if random.random() < 0.3:  # 30% are SMC
            prodcd = '34240'
            product = 300
        elif product in [100, 101]:
            prodcd = '3410' + str(random.randint(0, 9))
        elif product in [200, 225, 226]:
            prodcd = '3420' + str(random.randint(0, 9))
        else:
            prodcd = '3429' + str(random.randint(0, 9))

        # For SMC, must be OD
        if prodcd == '34240':
            acctype = 'OD'

        # Dates
        issue_date = reptdate - timedelta(days=random.randint(30, 1825))
        expr_date = reptdate + timedelta(days=random.randint(30, 730))

        # Balance
        if prodcd == '34240':  # SMC
            balance = random.randint(10000, 100000)
        else:
            balance = random.randint(5000, 500000)

        # Paid indicator
        paidind = random.choice(['A', 'A', 'A', 'P'])  # Mostly active

        # Exclude certain products from SMC
        if product in [151, 152, 181]:
            product = 300  # Change to non-excluded

        data.append({
            'loanid': f'L{i + 1:06d}',
            'prodcd': prodcd,
            'product': product,
            'custcd': custcd,
            'acctype': acctype,
            'balance': balance,
            'payamt': None,
            'bldate': None,
            'issdte': issue_date,
            'exprdate': expr_date,
            'payfreq': None,
            'loanstat': 1,
            'paidind': paidind,
            'eir_adj': None
        })

    df = pl.DataFrame(data)

    # Save to parquet
    filename = f"loan{reptdate.strftime('%m')}2.parquet"
    df.write_parquet(BNM1_DIR / filename)

    print(f"\nCreated {filename} with {n_loans} loans")
    print(f"  - Loan accounts: {len(df.filter(pl.col('acctype') == 'LN'))}")
    print(f"  - OD accounts: {len(df.filter(pl.col('acctype') == 'OD'))}")
    print(f"  - SMC products (34240): {len(df.filter(pl.col('prodcd') == '34240'))}")

    return df


def main():
    """Generate all test data"""
    print("Generating test data for BNM Calc Conversion...")
    print("=" * 80)

    # Create reptdate
    reptdate = create_reptdate()
    print()

    # Create table (maturity buckets)
    table_df = create_table()
    print()

    # Create note data
    note_df = create_note()
    print()

    # Create WALK file
    create_walk_file()
    print()

    # Create historical store data
    print("Creating 48 weeks of historical data...")
    create_historical_store_data()
    print()

    # Create loan data
    loan_df = create_loan_data(reptdate)
    print()

    print("=" * 80)
    print("Test data generation complete!")
    print()
    print("File structure created:")
    print("./input/")
    print("  bnm/")
    print("    reptdate.parquet")
    print("    table.parquet")
    print("    note.parquet")
    print("    base_{PROD}.parquet (5 files)")
    print("    store_{PROD}.parquet (5 files)")
    print("  bnm1/")
    print("    loan0102.parquet")
    print("  walk.txt")
    print()
    print("You can now run: python bnm_calc_conversion.py")


if __name__ == "__main__":
    main()
