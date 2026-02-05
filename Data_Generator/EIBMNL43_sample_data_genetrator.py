#!/usr/bin/env python3
"""
For EIBMNL43
Sample Test Data Generator for BNM Report Generation
Creates synthetic FINALSUM data for testing report generation
"""

import polars as pl
from pathlib import Path
import random

# Set up directories
INPUT_DIR = Path("./input")
STORE_DIR = INPUT_DIR / "store"

STORE_DIR.mkdir(parents=True, exist_ok=True)


def create_finalsum_data():
    """Create sample FINALSUM data with all product types"""

    print("Generating FINALSUM test data...")
    print("=" * 60)

    data = []

    # Define all product categories and their codes
    product_categories = [
        # RM Non-Individual (09)
        ('9321109', '9521109'),  # Fixed Term Loans
        ('9321209', '9521209'),  # Revolving Loans
        ('9321309', '9521309'),  # Overdrafts
        ('9321909', '9521909'),  # Others

        # RM Individual (08)
        ('9321408', '9521408'),  # Housing Loans
        ('9321508', '9521508'),  # Credit Cards
        ('9321308', '9521308'),  # Overdrafts
        ('9321908', '9521908'),  # Others

        # RM Miscellaneous (00)
        ('9322100', '9522100'),  # Cash Holdings
        ('9322200', '9522200'),  # SRR
        ('9322900', '9522900'),  # Other Assets

        # FCY Non-Individual (09)
        ('9421109', '9621109'),  # Fixed Term Loans
        ('9421209', '9621209'),  # Revolving Loans
        ('9421309', '9621309'),  # Overdrafts
        ('9421909', '9621909'),  # Others

        # FCY Individual (08)
        ('9421408', '9621408'),  # Housing Loans
        ('9421508', '9621508'),  # Credit Cards
        ('9421308', '9621308'),  # Overdrafts
        ('9421908', '9621908'),  # Others

        # FCY Miscellaneous (00)
        ('9422100', '9622100'),  # Cash Holdings
        ('9422200', '9622200'),  # SRR
        ('9422900', '9622900'),  # Other Assets
    ]

    # Maturity buckets
    buckets = ['01', '02', '03', '04', '05', '06']

    # Generate data for each product category
    for npl_code, gross_code in product_categories:
        # Determine base amount based on product type
        if '211' in npl_code:  # Fixed loans - largest
            base_npl = random.uniform(500000, 2000000)
            base_gross = random.uniform(5000000, 20000000)
        elif '214' in npl_code or '215' in npl_code:  # Housing/CC - medium
            base_npl = random.uniform(200000, 1000000)
            base_gross = random.uniform(2000000, 10000000)
        elif '222' in npl_code:  # SRR/Cash - varies
            base_npl = random.uniform(100000, 500000)
            base_gross = random.uniform(1000000, 5000000)
        else:  # Others
            base_npl = random.uniform(100000, 800000)
            base_gross = random.uniform(1000000, 8000000)

        # Generate amounts for each bucket
        for bucket in buckets:
            # Distribution pattern: more in later buckets for loans
            if '21' in npl_code[:4]:  # Loan products
                if bucket in ['01', '02']:  # Short term
                    npl_mult = random.uniform(0.05, 0.15)
                    gross_mult = random.uniform(0.05, 0.15)
                elif bucket in ['03', '04']:  # Medium term
                    npl_mult = random.uniform(0.10, 0.25)
                    gross_mult = random.uniform(0.10, 0.25)
                else:  # Long term (05, 06)
                    npl_mult = random.uniform(0.20, 0.40)
                    gross_mult = random.uniform(0.20, 0.40)
            else:  # Cash/Reserves - mostly short term
                if bucket == '01':
                    npl_mult = random.uniform(0.40, 0.60)
                    gross_mult = random.uniform(0.40, 0.60)
                elif bucket == '06':
                    npl_mult = random.uniform(0.20, 0.40)
                    gross_mult = random.uniform(0.20, 0.40)
                else:
                    npl_mult = random.uniform(0.02, 0.10)
                    gross_mult = random.uniform(0.02, 0.10)

            # NPL record
            npl_amount = base_npl * npl_mult
            npl_bnmcode = f"{npl_code}{bucket}0000Y"
            data.append({
                'bnmcode': npl_bnmcode,
                'amount': npl_amount
            })

            # Gross record
            gross_amount = base_gross * gross_mult
            gross_bnmcode = f"{gross_code}{bucket}0000Y"
            data.append({
                'bnmcode': gross_bnmcode,
                'amount': gross_amount
            })

    # Create DataFrame
    df = pl.DataFrame(data)

    # Save to parquet
    df.write_parquet(STORE_DIR / "finalsum.parquet")

    print(f"Created finalsum.parquet with {len(df)} records")
    print()

    # Print summary statistics
    print("Summary by report type:")
    for report_type in ['93', '94', '95', '96']:
        filtered = df.filter(pl.col('bnmcode').str.slice(0, 2) == report_type)
        count = len(filtered)
        total = filtered['amount'].sum()

        type_desc = {
            '93': 'RM NPL',
            '94': 'FCY NPL',
            '95': 'RM Gross',
            '96': 'FCY Gross'
        }[report_type]

        print(f"  {report_type} ({type_desc}): {count} records, total = {total:,.2f}")

    print()

    # Print sample by product
    print("Sample records by product (first of each type):")
    unique_prods = df['bnmcode'].str.slice(0, 7).unique().sort()

    for prod in unique_prods[:5]:  # Show first 5
        sample = df.filter(pl.col('bnmcode').str.slice(0, 7) == prod).head(1)
        if len(sample) > 0:
            bnmcode = sample['bnmcode'][0]
            amount = sample['amount'][0]
            print(f"  {bnmcode}: {amount:,.2f}")

    print("  ...")

    return df


def main():
    """Generate all test data"""
    print("Generating test data for BNM Report Generation...")
    print("=" * 80)
    print()

    # Create FINALSUM data
    finalsum_df = create_finalsum_data()

    print()
    print("=" * 80)
    print("Test data generation complete!")
    print()
    print("File structure created:")
    print("./input/")
    print("  store/")
    print("    finalsum.parquet")
    print()
    print("This file contains:")
    print("  - All RM product categories (A1.01 - A1.11)")
    print("  - All FCY product categories (B1.01 - B1.11)")
    print("  - Both NPL (93/94) and Gross (95/96) positions")
    print("  - All 6 maturity buckets for each product")
    print()
    print("Total unique product-bucket combinations:")
    print(f"  - 22 product categories")
    print(f"  - 2 report types per category (NPL + Gross)")
    print(f"  - 6 buckets per type")
    print(f"  - Total: 22 × 2 × 6 = {len(finalsum_df)} records")
    print()
    print("You can now run: python bnm_report_generation.py")


if __name__ == "__main__":
    main()
