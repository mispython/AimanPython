#!/usr/bin/env python3
"""
Test script for DMMISRI3 report generation
Creates sample data and runs the report
"""

import polars as pl
from datetime import datetime
from pathlib import Path
import random


def create_sample_reptdate():
    """Create sample REPTDATE file"""
    input_dir = Path("input")
    input_dir.mkdir(exist_ok=True)

    # Create sample report date (February 12, 2024 - Week 2)
    df = pl.DataFrame({
        'REPTDATE': [datetime(2024, 2, 12)]
    })

    df.write_parquet(input_dir / "reptdate.parquet")
    print(f"✓ Created reptdate.parquet with date: 2024-02-12")


def create_sample_isa_data():
    """Create sample ISA data file"""
    input_dir = Path("input")
    input_dir.mkdir(exist_ok=True)

    # Generate sample data with various deposit amounts
    random.seed(42)

    records = []

    # Product 204: Islamic Savings Account
    # Generate accounts across different ranges
    deposit_ranges = [
        (50, 99, 150),  # Less than RM100
        (100, 499, 200),  # RM100-499
        (500, 999, 180),  # RM500-999
        (1000, 4999, 300),  # RM1,000-4,999
        (5000, 9999, 250),  # RM5,000-9,999
        (10000, 49999, 200),  # RM10,000-49,999
        (50000, 99999, 100),  # RM50,000-99,999
        (100000, 499999, 80),  # RM100,000-499,999
        (500000, 999999, 40),  # RM500,000-999,999
        (1000000, 4999999, 20),  # RM1,000,000-4,999,999
        (5000000, 10000000, 10)  # RM5,000,000 and above
    ]

    for min_amt, max_amt, count in deposit_ranges:
        for _ in range(count):
            amount = random.uniform(min_amt, max_amt)
            records.append({
                'PRODUCT': 204,
                'CURBAL': round(amount, 2)
            })

    # Product 215: Islamic Savings Account-i
    for min_amt, max_amt, count in deposit_ranges:
        # Slightly fewer accounts for product 215
        for _ in range(int(count * 0.7)):
            amount = random.uniform(min_amt, max_amt)
            records.append({
                'PRODUCT': 215,
                'CURBAL': round(amount, 2)
            })

    # Add a few accounts with other products (should be filtered out)
    for _ in range(10):
        records.append({
            'PRODUCT': 100,
            'CURBAL': random.uniform(1000, 50000)
        })

    # Add a few negative balance accounts (should be filtered out)
    for _ in range(5):
        records.append({
            'PRODUCT': 204,
            'CURBAL': -random.uniform(100, 1000)
        })

    df = pl.DataFrame(records)

    # Save as isa0222024.parquet (February, Week 2, 2024)
    df.write_parquet(input_dir / "isa0222024.parquet")
    print(f"✓ Created isa0222024.parquet with {len(df)} records")
    print(f"  - Product 204 records: {len(df.filter(pl.col('PRODUCT') == 204))}")
    print(f"  - Product 215 records: {len(df.filter(pl.col('PRODUCT') == 215))}")
    print(f"  - Other products: {len(df.filter(~pl.col('PRODUCT').is_in([204, 215])))}")
    print(f"  - Negative balances: {len(df.filter(pl.col('CURBAL') < 0))}")


def display_sample_output():
    """Display sample of the generated report"""
    output_file = Path("output/dmmisri3_report.txt")

    if output_file.exists():
        print("\n" + "=" * 80)
        print("SAMPLE OUTPUT (first 50 lines):")
        print("=" * 80)

        with open(output_file, 'r') as f:
            lines = f.readlines()
            for i, line in enumerate(lines[:50], 1):
                # Show ASA control character
                asa_char = line[0] if line else ' '
                asa_label = {
                    '1': '[NEW PAGE]',
                    ' ': '[SINGLE]  ',
                    '0': '[DOUBLE]  ',
                    '-': '[TRIPLE]  '
                }.get(asa_char, '[UNKNOWN] ')

                print(f"{asa_label} {line.rstrip()}")

        if len(lines) > 50:
            print(f"\n... ({len(lines) - 50} more lines)")

        print("=" * 80)


def main():
    """Run test"""
    print("DMMISRI3 Report Test")
    print("=" * 80)

    # Step 1: Create sample data
    print("\n1. Creating sample data...")
    create_sample_reptdate()
    create_sample_isa_data()

    # Step 2: Run the report
    print("\n2. Running report generation...")
    import subprocess
    result = subprocess.run(['python3', 'dmmisri3_report.py'],
                            capture_output=True, text=True)

    if result.returncode == 0:
        print(result.stdout)

        # Step 3: Display sample output
        print("\n3. Displaying sample output...")
        display_sample_output()

        # Step 4: Verify file
        output_file = Path("output/dmmisri3_report.txt")
        if output_file.exists():
            size = output_file.stat().st_size
            print(f"\n✓ Test completed successfully!")
            print(f"  Output file: {output_file}")
            print(f"  File size: {size:,} bytes")
    else:
        print(f"✗ Error running report:")
        print(result.stderr)


if __name__ == "__main__":
    main()
