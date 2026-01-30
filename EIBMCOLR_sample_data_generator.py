#!/usr/bin/env python3
"""
Test script for EIBMCOLR report generation
Creates sample data and runs the report
"""

import polars as pl
from datetime import datetime
from pathlib import Path
import random
import sys


def create_sample_reptdate():
    """Create sample REPTDATE file"""
    input_dir = Path("input")
    input_dir.mkdir(exist_ok=True)

    # Create sample report date (February 15, 2024 - Week 2)
    df = pl.DataFrame({
        'REPTDATE': [datetime(2024, 2, 15)]
    })

    df.write_parquet(input_dir / "reptdate.parquet")
    print(f"✓ Created reptdate.parquet with date: 2024-02-15 (Week 2)")


def create_sample_loan_data():
    """Create sample loan data file"""
    input_dir = Path("input")
    input_dir.mkdir(exist_ok=True)

    random.seed(42)

    records = []

    # Sample branches
    branches = [1, 2, 3, 5, 10]

    # Sample liability codes (various risk categories)
    od_liab_codes = {
        '007': ('30307', 0),  # 0%
        '021': ('30309', 0),  # 0%
        '017': ('30009/17', 10),  # 10%
        '006': ('30323', 20),  # 20%
        '011': ('30325', 20),  # 20%
        '050': ('30341', 50),  # 50% or 100%
        '019': ('30351', 100),  # 100%
        '099': ('30359', 100),  # Others
    }

    acct_counter = 1000

    # Generate OD accounts
    for branch in branches:
        for liab_code, (bnmcode, riskcat) in od_liab_codes.items():
            for i in range(random.randint(5, 15)):
                balance = random.uniform(10000, 500000)

                # Place liab code in COL1 or COL2
                if random.random() > 0.5:
                    col1 = f"{liab_code}  "
                    col2 = "    "
                else:
                    col1 = "    "
                    col2 = f"{liab_code}  "

                records.append({
                    'ACCTNO': acct_counter,
                    'NOTENO': 0,
                    'CUSTCODE': f'CUST{acct_counter:06d}',
                    'RISKCD': 1,
                    'RISKRTE': 0.0,
                    'NAME': f'Customer {acct_counter}',
                    'PRODUCT': random.choice([100, 101, 102]),
                    'PRODCD': 'OD',
                    'BRANCH': branch,
                    'CURBAL': balance,
                    'INTAMT': 0.0,
                    'NETPROC': balance,
                    'LOANSTAT': 'A',
                    'APPVALUE': 0.0,
                    'FEEAMT': 0.0,
                    'BALANCE': balance,
                    'CLOSEDTE': None,
                    'APPRLIMT': balance * 1.2,
                    'ISSDTE': datetime(2023, 1, 1),
                    'RLEASAMT': 0.0,
                    'APPRLIM2': 0.0,
                    'ACCTYPE': 'OD',
                    'COL1': col1,
                    'COL2': col2,
                    'AMTIND': random.choice(['D', 'I']),  # Conventional or Islamic
                    'VBAL': 0.0,
                    'LIMIT1': balance,
                    'LIMIT2': 0.0,
                    'FISSPURP': random.choice(['0311', '0500', '0600']) if liab_code == '050' else ''
                })
                acct_counter += 1

    # Generate LN accounts
    for branch in branches:
        for liab_code, (bnmcode, riskcat) in list(od_liab_codes.items())[:5]:
            for i in range(random.randint(10, 20)):
                balance = random.uniform(50000, 1000000)
                appvalue = random.uniform(0, balance * 1.5)

                # Place liab code in COL1
                col1 = f"{liab_code}  "
                col2 = "    "

                records.append({
                    'ACCTNO': acct_counter,
                    'NOTENO': random.randint(1, 5),
                    'CUSTCODE': f'CUST{acct_counter:06d}',
                    'RISKCD': 1,
                    'RISKRTE': 0.0,
                    'NAME': f'Customer {acct_counter}',
                    'PRODUCT': random.choice([200, 201, 202]),
                    'PRODCD': 'LN',
                    'BRANCH': branch,
                    'CURBAL': balance,
                    'INTAMT': balance * 0.05,
                    'NETPROC': balance * 0.95,
                    'LOANSTAT': 'A',
                    'APPVALUE': appvalue,
                    'FEEAMT': balance * 0.01,
                    'BALANCE': balance,
                    'CLOSEDTE': None,
                    'APPRLIMT': balance * 1.2,
                    'ISSDTE': datetime(2023, 1, 1),
                    'RLEASAMT': 0.0,
                    'APPRLIM2': 0.0,
                    'ACCTYPE': 'LN',
                    'COL1': col1,
                    'COL2': col2,
                    'AMTIND': random.choice(['D', 'I']),
                    'VBAL': 0.0,
                    'LIMIT1': balance,
                    'LIMIT2': 0.0,
                    'FISSPURP': random.choice(['0311', '0500', '0600']) if liab_code == '050' else ''
                })
                acct_counter += 1

    # Add some excluded products (should be filtered out)
    for i in range(10):
        records.append({
            'ACCTNO': acct_counter,
            'NOTENO': 0,
            'CUSTCODE': f'CUST{acct_counter:06d}',
            'RISKCD': 1,
            'RISKRTE': 0.0,
            'NAME': f'Customer {acct_counter}',
            'PRODUCT': 500,  # Excluded
            'PRODCD': 'XX',
            'BRANCH': 1,
            'CURBAL': 10000,
            'INTAMT': 0,
            'NETPROC': 10000,
            'LOANSTAT': 'A',
            'APPVALUE': 0,
            'FEEAMT': 0,
            'BALANCE': 10000,
            'CLOSEDTE': None,
            'APPRLIMT': 10000,
            'ISSDTE': datetime(2023, 1, 1),
            'RLEASAMT': 0,
            'APPRLIM2': 0,
            'ACCTYPE': 'OD',
            'COL1': '007  ',
            'COL2': '    ',
            'AMTIND': 'D',
            'VBAL': 0,
            'LIMIT1': 10000,
            'LIMIT2': 0,
            'FISSPURP': ''
        })
        acct_counter += 1

    df = pl.DataFrame(records)

    # Save as loan022.parquet (February, Week 2)
    df.write_parquet(input_dir / "loan022.parquet")
    print(f"✓ Created loan022.parquet with {len(df)} records")
    print(f"  - OD accounts: {len(df.filter(pl.col('ACCTYPE') == 'OD'))}")
    print(f"  - LN accounts: {len(df.filter(pl.col('ACCTYPE') == 'LN'))}")
    print(f"  - Excluded products: {len(df.filter(pl.col('PRODUCT') == 500))}")


def create_sample_branch_data():
    """Create sample branch data"""
    input_dir = Path("input")
    input_dir.mkdir(exist_ok=True)

    branches = [
        {'BRANCH': 1, 'BRHCODE': 'HQ'},
        {'BRANCH': 2, 'BRHCODE': 'KLG'},
        {'BRANCH': 3, 'BRHCODE': 'PEN'},
        {'BRANCH': 5, 'BRHCODE': 'JHR'},
        {'BRANCH': 10, 'BRHCODE': 'MLK'},
    ]

    df = pl.DataFrame(branches)
    df.write_parquet(input_dir / "branch.parquet")
    print(f"✓ Created branch.parquet with {len(df)} branches")


def create_sample_totiis_data():
    """Create sample TOTIIS data"""
    input_dir = Path("input")
    input_dir.mkdir(exist_ok=True)

    random.seed(42)

    records = []

    # Create TOTIIS for some LN accounts
    for acctno in range(1100, 1300, 5):
        for noteno in range(1, 3):
            totiis = random.uniform(1000, 50000)
            records.append({
                'ACCTNO': acctno,
                'NOTENO': noteno,
                'TOTIIS': totiis
            })

    df = pl.DataFrame(records)
    df.write_parquet(input_dir / "totiis02.parquet")
    print(f"✓ Created totiis02.parquet with {len(df)} records")


def create_sample_prev_rcat():
    """Create sample previous month RCAT file"""
    input_dir = Path("input")
    input_dir.mkdir(exist_ok=True)

    random.seed(41)  # Different seed for different values

    with open(input_dir / "eibmcolr_rcat_prev.dat", 'wb') as f:
        # Date header (January 2024)
        f.write(b"20240115\n")

        # Sample data
        branches = [1, 2, 3, 5, 10]
        riskcats = [0, 10, 20, 50, 100]
        bnmcodes = ['30307', '30309', '30009/17', '30323', '30325', '30341', '30351', '30359']

        for branch in branches:
            for riskcat in riskcats:
                for bnmcode in random.sample(bnmcodes, 3):
                    prevbal = random.uniform(100000, 5000000)
                    prevnetb = prevbal * 0.95

                    # Format record
                    branch_str = f"{branch:03d}"
                    riskcat_str = f"{riskcat:03d}"
                    bnmcode_str = f"{bnmcode:8s}"
                    prevbal_str = f"{prevbal:015.2f}".replace('.', '')
                    prevnetb_str = f"{prevnetb:015.2f}".replace('.', '')

                    # Write fixed-width record
                    # Positions: 1-3 branch, 4-6 riskcat, 7-14 bnmcode, 15-29 balance, 46-60 netbal
                    record = f"{branch_str}{riskcat_str}{bnmcode_str}{prevbal_str}                {prevnetb_str}\n"
                    f.write(record.encode('utf-8'))

        # EOF marker
        f.write(b"EOF\n")

    print(f"✓ Created eibmcolr_rcat_prev.dat (previous month data)")


def display_sample_outputs():
    """Display samples from generated reports"""
    output_dir = Path("output")

    if not output_dir.exists():
        print("No output directory found")
        return

    print("\n" + "=" * 80)
    print("SAMPLE OUTPUT FILES:")
    print("=" * 80)

    for file in output_dir.glob("*.txt"):
        print(f"\n{file.name}:")
        print("-" * 40)

        with open(file, 'r') as f:
            lines = f.readlines()
            # Show first 30 lines
            for i, line in enumerate(lines[:30], 1):
                asa_char = line[0] if line else ' '
                asa_label = {
                    '1': '[PAGE]',
                    ' ': '[SPC1]',
                    '0': '[SPC2]',
                    '-': '[SPC3]'
                }.get(asa_char, '[????]')

                print(f"{asa_label} {line.rstrip()}")

            if len(lines) > 30:
                print(f"\n... ({len(lines) - 30} more lines)")

    # Check RCAT file
    rcat_file = output_dir / "eibmcolr_rcat.dat"
    if rcat_file.exists():
        print(f"\n{rcat_file.name}:")
        print("-" * 40)

        with open(rcat_file, 'rb') as f:
            lines = f.readlines()
            for i, line in enumerate(lines[:20], 1):
                print(f"{i:3d}: {line.decode('utf-8').rstrip()}")

            if len(lines) > 20:
                print(f"\n... ({len(lines) - 20} more lines)")


def main():
    """Run test"""
    print("EIBMCOLR Report Test")
    print("=" * 80)

    # Step 1: Create sample data
    print("\n1. Creating sample data files...")
    create_sample_reptdate()
    create_sample_loan_data()
    create_sample_branch_data()
    create_sample_totiis_data()
    create_sample_prev_rcat()

    # Step 2: Run the report
    print("\n2. Running EIBMCOLR report generation...")
    print("=" * 80)
    import subprocess
    # result = subprocess.run(['python3', 'eibmcolr_report.py'],
    #                         capture_output=True, text=True)
    result = subprocess.run([sys.executable, 'dmmisri3_report.py'],
                            capture_output=True, text=True)

    if result.returncode == 0:
        print(result.stdout)

        # Step 3: Display sample outputs
        print("\n3. Displaying sample outputs...")
        display_sample_outputs()

        # Step 4: Summary
        output_dir = Path("output")
        if output_dir.exists():
            files = list(output_dir.glob("*"))
            print("\n" + "=" * 80)
            print("✓ Test completed successfully!")
            print(f"\nGenerated {len(files)} output files:")
            for file in sorted(files):
                size = file.stat().st_size
                print(f"  - {file.name} ({size:,} bytes)")
    else:
        print(f"✗ Error running report:")
        print(result.stderr)


if __name__ == "__main__":
    main()
