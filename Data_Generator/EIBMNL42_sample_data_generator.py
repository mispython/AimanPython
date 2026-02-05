#!/usr/bin/env python3
"""
For EIBMNL42
Sample Test Data Generator for BNM GL Set and Consolidation
Creates synthetic WALK file and prerequisite data
"""

import polars as pl
from datetime import datetime
from pathlib import Path
import random

# Set up directories
INPUT_DIR = Path("./input")
BNM_DIR = INPUT_DIR / "bnm"
WALK_FILE = INPUT_DIR / "walk.txt"

INPUT_DIR.mkdir(parents=True, exist_ok=True)
BNM_DIR.mkdir(parents=True, exist_ok=True)


def create_reptdate():
    """Create sample reporting date"""
    reptdate = datetime(2024, 1, 15)  # Week 2 of January 2024

    df = pl.DataFrame({
        'reptdate': [reptdate]
    })

    df.write_parquet(BNM_DIR / "reptdate.parquet")
    print(f"Created reptdate.parquet with date: {reptdate}")

    return reptdate


def create_walk_file(reptdate):
    """Create sample WALK file with GL account balances"""

    # Define GL accounts and sample amounts
    gl_accounts = [
        # RM accounts
        ('39110', 12345678.90),  # Cash Holdings
        ('32110', 5000000.00),  # SRR
        ('ML34170', 1250000.00),  # Floor Stocking
        ('ML34111', 987654.32),  # Hire Purchase Agency
        ('F137010SH', 123456.78),  # Other Assets (Shares)

        # FCY accounts
        ('F13460081BCB', 500000.00),  # FCY Fixed Loan
        ('F13460064FLB', 750000.00),  # FCY Fixed Loan
        ('F134600RC', 250000.00),  # FCY RC
        ('F139610FXNC', 100000.00),  # FCY Cash

        # Additional records (for testing multiple entries)
        ('39110', 234567.10),  # More cash
        ('ML34170', 50000.00),  # More floor stocking
    ]

    # Format: @2 DESC $19., @21 DD 2., @24 MM 2., @27 YY 2., @42 AMOUNT COMMA20.2
    lines = []

    dd = f"{reptdate.day:02d}"
    mm = f"{reptdate.month:02d}"
    yy = f"{reptdate.year % 100:02d}"

    for desc, amount in gl_accounts:
        # Format the line according to SAS INPUT specifications
        # Position 1: space
        # Position 2-20: DESC (19 characters, left-aligned)
        # Position 21-22: DD
        # Position 23: space
        # Position 24-25: MM
        # Position 26: space
        # Position 27-28: YY
        # Position 29-41: spaces
        # Position 42-61: AMOUNT (right-aligned, comma format)

        desc_padded = f"{desc:<19}"  # Left-align, pad to 19 chars
        amount_formatted = f"{amount:>20,.2f}"  # Right-align, comma format

        # Build the line
        line = f" {desc_padded}{dd} {mm} {yy}             {amount_formatted}"
        lines.append(line)

    # Write to file
    with open(WALK_FILE, 'w') as f:
        for line in lines:
            f.write(line + '\n')

    print(f"\nCreated walk.txt with {len(lines)} records")
    print(f"  Sample GL accounts:")

    # Summarize by GL code
    gl_summary = {}
    for desc, amount in gl_accounts:
        if desc not in gl_summary:
            gl_summary[desc] = 0
        gl_summary[desc] += amount

    for desc, total in sorted(gl_summary.items()):
        print(f"    {desc:<20} {total:>15,.2f}")

    return lines


def create_prerequisite_datasets():
    """Create prerequisite BNM datasets (NOTE, CALC, etc.)"""

    print("\nCreating prerequisite BNM datasets...")

    # Create NOTE data (sample from bnm_note_conversion.py output)
    note_data = []

    # Sample loan records
    for item in ['211', '212', '213', '219']:
        for cust in ['08', '09']:
            for bucket in ['01', '02', '03', '04', '05', '06']:
                # RM - Gross
                note_data.append({
                    'bnmcode': f'95{item}{cust}{bucket}0000Y',
                    'amount': random.uniform(100000, 1000000)
                })
                # RM - NPL
                note_data.append({
                    'bnmcode': f'93{item}{cust}{bucket}0000Y',
                    'amount': random.uniform(10000, 100000)
                })

    note_df = pl.DataFrame(note_data)
    note_df.write_parquet(BNM_DIR / "note.parquet")
    print(f"  Created note.parquet with {len(note_df)} records")

    # Create CALC data (sample from bnm_calc_conversion.py output)
    calc_data = []

    # PBCARD, OD, SMC records
    products = [
        ('215', '08'),  # PBCARD Individual
        ('213', '08'),  # OD Individual
        ('213', '09'),  # OD Corporate
        ('219', '08'),  # SMC Individual
        ('219', '09'),  # SMC Corporate
    ]

    for prod, cust in products:
        for bucket in ['01', '02', '03', '04', '05', '06']:
            calc_data.append({
                'bnmcode': f'93{prod}{cust}{bucket}0000Y',
                'amount': random.uniform(5000, 50000)
            })
            calc_data.append({
                'bnmcode': f'95{prod}{cust}{bucket}0000Y',
                'amount': random.uniform(50000, 500000)
            })

    calc_df = pl.DataFrame(calc_data)
    calc_df.write_parquet(BNM_DIR / "calc.parquet")
    print(f"  Created calc.parquet with {len(calc_df)} records")

    # Create optional PBIF data (placeholder)
    pbif_data = []
    for bucket in ['01', '02', '03', '04', '05', '06']:
        pbif_data.append({
            'bnmcode': f'95216080{bucket}0000Y',
            'amount': random.uniform(10000, 100000)
        })

    pbif_df = pl.DataFrame(pbif_data)
    pbif_df.write_parquet(BNM_DIR / "pbif.parquet")
    print(f"  Created pbif.parquet with {len(pbif_df)} records")

    # Create optional BT data (placeholder)
    bt_data = []
    for bucket in ['01', '02', '03', '04', '05', '06']:
        bt_data.append({
            'bnmcode': f'95217080{bucket}0000Y',
            'amount': random.uniform(5000, 50000)
        })

    bt_df = pl.DataFrame(bt_data)
    bt_df.write_parquet(BNM_DIR / "bt.parquet")
    print(f"  Created bt.parquet with {len(bt_df)} records")


def main():
    """Generate all test data"""
    print("Generating test data for BNM GL Set and Consolidation...")
    print("=" * 80)

    # Create reptdate
    reptdate = create_reptdate()
    print()

    # Create WALK file
    create_walk_file(reptdate)
    print()

    # Create prerequisite datasets
    create_prerequisite_datasets()
    print()

    print("=" * 80)
    print("Test data generation complete!")
    print()
    print("File structure created:")
    print("./input/")
    print("  bnm/")
    print("    reptdate.parquet")
    print("    note.parquet")
    print("    calc.parquet")
    print("    pbif.parquet")
    print("    bt.parquet")
    print("  walk.txt")
    print()
    print("Expected GL accounts in walk.txt:")
    print("  RM:")
    print("    - 39110 (Cash Holdings)")
    print("    - 32110 (SRR)")
    print("    - ML34170 (Floor Stocking)")
    print("    - ML34111 (Hire Purchase)")
    print("    - F137010SH (Other Assets)")
    print("  FCY:")
    print("    - F13460081BCB, F13460064FLB (Fixed Loan)")
    print("    - F134600RC (Revolving Credit)")
    print("    - F139610FXNC (Cash Holdings)")
    print()
    print("You can now run: python bnm_glset_consolidation.py")


if __name__ == "__main__":
    main()
