#!/usr/bin/env python3
"""
Test script for Loan FX Processing
Creates sample data and runs the processing
"""

import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import random


def create_sample_fxrate_data():
    """Create sample foreign exchange rate data"""
    input_dir = Path("input")
    input_dir.mkdir(exist_ok=True)

    random.seed(42)

    # Sample currencies with rates
    currencies = ['USD', 'EUR', 'GBP', 'SGD', 'CNY', 'JPY', 'AUD', 'THB']

    records = []

    # Generate historical rates for each currency
    for currency in currencies:
        base_rate = {
            'USD': 4.12,
            'EUR': 4.56,
            'GBP': 5.23,
            'SGD': 3.08,
            'CNY': 0.57,
            'JPY': 0.038,
            'AUD': 2.75,
            'THB': 0.12
        }[currency]

        # Generate rates for last 30 days
        for days_ago in range(30):
            date = datetime.now().date() - timedelta(days=days_ago)
            # Add some variation
            rate = base_rate * (1 + random.uniform(-0.02, 0.02))

            records.append({
                'CURCODE': currency,
                'REPTDATE': date,
                'SPOTRATE': round(rate, 6)
            })

    df = pl.DataFrame(records)
    df.write_parquet(input_dir / "foratebkp.parquet")
    print(f"✓ Created foratebkp.parquet with {len(df)} FX rate records")
    print(f"  - Currencies: {len(currencies)}")
    print(f"  - Date range: {df['REPTDATE'].min()} to {df['REPTDATE'].max()}")


def create_sample_lnnote_data():
    """Create sample conventional loan note data"""
    input_dir = Path("input")
    input_dir.mkdir(exist_ok=True)

    random.seed(43)

    currencies = ['MYR', 'USD', 'EUR', 'GBP', 'SGD', 'CNY']

    records = []

    # Generate loan notes
    for acctno in range(1000, 1500):
        for noteno in range(1, random.randint(1, 4)):
            curcode = random.choice(currencies)

            records.append({
                'ACCTNO': acctno,
                'NOTENO': noteno,
                'LOANTYPE': random.choice([100, 200, 300, 400, 500]),
                'CENSUS': round(random.uniform(1.0, 50.0), 2),
                'CURCODE': curcode,
                'SPOTRATE': None if curcode == 'MYR' else random.uniform(0.5, 5.5),
                'BALANCE': random.uniform(10000, 1000000),
                'CUSTCODE': random.randint(1, 100),
                'BRANCH': random.randint(1, 50)
            })

    df = pl.DataFrame(records)
    df.write_parquet(input_dir / "lnnote.parquet")
    print(f"✓ Created lnnote.parquet with {len(df)} loan note records")
    print(f"  - MYR records: {len(df.filter(pl.col('CURCODE') == 'MYR'))}")
    print(f"  - Non-MYR records: {len(df.filter(pl.col('CURCODE') != 'MYR'))}")


def create_sample_lnwof_data():
    """Create sample conventional write-off data"""
    input_dir = Path("input")
    input_dir.mkdir(exist_ok=True)

    random.seed(44)

    records = []

    # Generate write-off records for subset of loan notes
    # Covers accounts 1000-1200 (partial coverage to test merge logic)
    for acctno in range(1000, 1200):
        if random.random() > 0.3:  # 70% of accounts have write-off data
            for noteno in range(1, random.randint(1, 3)):
                # Sometimes PRODUCT is set, sometimes ORICODE
                product = random.choice([0, 600, 700, 800, 900, 0, 0])
                oricode = random.choice([0, 0, 0, 1000, 1100, 1200])
                census_trt = random.choice([0.0, 0.0, round(random.uniform(10.0, 100.0), 2)])

                records.append({
                    'ACCTNO': acctno,
                    'NOTENO': noteno,
                    'WRITE_DOWN_BAL': random.uniform(1000, 100000),
                    'PRODUCT': product if product > 0 else None,
                    'CENSUS_TRT': census_trt if census_trt > 0 else None,
                    'ORICODE': oricode if oricode > 0 else None
                })

    df = pl.DataFrame(records)
    df.write_parquet(input_dir / "lnwof.parquet")
    print(f"✓ Created lnwof.parquet with {len(df)} write-off records")
    print(f"  - With PRODUCT: {len(df.filter(pl.col('PRODUCT').is_not_null()))}")
    print(f"  - With ORICODE: {len(df.filter(pl.col('ORICODE').is_not_null()))}")
    print(f"  - With CENSUS_TRT: {len(df.filter(pl.col('CENSUS_TRT').is_not_null()))}")


def create_sample_ilnnote_data():
    """Create sample Islamic loan note data"""
    input_dir = Path("input")
    input_dir.mkdir(exist_ok=True)

    random.seed(45)

    currencies = ['MYR', 'USD', 'EUR', 'SGD', 'CNY']

    records = []

    # Generate Islamic loan notes (smaller dataset)
    for acctno in range(2000, 2300):
        for noteno in range(1, random.randint(1, 3)):
            curcode = random.choice(currencies)

            records.append({
                'ACCTNO': acctno,
                'NOTENO': noteno,
                'LOANTYPE': random.choice([150, 250, 350, 450]),
                'CENSUS': round(random.uniform(1.0, 50.0), 2),
                'CURCODE': curcode,
                'SPOTRATE': None if curcode == 'MYR' else random.uniform(0.5, 5.5),
                'BALANCE': random.uniform(10000, 500000),
                'CUSTCODE': random.randint(1, 100),
                'BRANCH': random.randint(1, 50)
            })

    df = pl.DataFrame(records)
    df.write_parquet(input_dir / "ilnnote.parquet")
    print(f"✓ Created ilnnote.parquet with {len(df)} Islamic loan records")
    print(f"  - MYR records: {len(df.filter(pl.col('CURCODE') == 'MYR'))}")
    print(f"  - Non-MYR records: {len(df.filter(pl.col('CURCODE') != 'MYR'))}")


def create_sample_ilnwof_data():
    """Create sample Islamic write-off data"""
    input_dir = Path("input")
    input_dir.mkdir(exist_ok=True)

    random.seed(46)

    records = []

    # Generate Islamic write-off records
    for acctno in range(2000, 2150):
        if random.random() > 0.4:  # 60% coverage
            for noteno in range(1, random.randint(1, 2)):
                product = random.choice([0, 0, 550, 650, 750])
                oricode = random.choice([0, 0, 0, 1050, 1150])
                census_trt = random.choice([0.0, round(random.uniform(10.0, 100.0), 2)])

                records.append({
                    'ACCTNO': acctno,
                    'NOTENO': noteno,
                    'WRITE_DOWN_BAL': random.uniform(1000, 100000),
                    'PRODUCT': product if product > 0 else None,
                    'CENSUS_TRT': census_trt if census_trt > 0 else None,
                    'ORICODE': oricode if oricode > 0 else None
                })

    df = pl.DataFrame(records)
    df.write_parquet(input_dir / "ilnwof.parquet")
    print(f"✓ Created ilnwof.parquet with {len(df)} Islamic write-off records")
    print(f"  - With PRODUCT: {len(df.filter(pl.col('PRODUCT').is_not_null()))}")
    print(f"  - With ORICODE: {len(df.filter(pl.col('ORICODE').is_not_null()))}")


def verify_outputs():
    """Verify and display sample of outputs"""
    output_dir = Path("output")

    if not output_dir.exists():
        print("\nNo output directory found")
        return

    print("\n" + "=" * 80)
    print("OUTPUT VERIFICATION")
    print("=" * 80)

    # Check FX rates
    if (output_dir / "fxrate_current.parquet").exists():
        df = pl.read_parquet(output_dir / "fxrate_current.parquet")
        print(f"\nFX Rates (fxrate_current.parquet):")
        print(f"  Records: {len(df)}")
        print(f"  Sample rates:")
        for row in df.head(5).iter_rows(named=True):
            print(f"    {row['CURCODE']}: {row['SPOTRATE']:.6f}")

    # Check conventional loans
    if (output_dir / "lnnote_updated.parquet").exists():
        df = pl.read_parquet(output_dir / "lnnote_updated.parquet")
        print(f"\nConventional Loans (lnnote_updated.parquet):")
        print(f"  Total records: {len(df)}")

        # Check updates
        if 'LOANTYPE' in df.columns:
            loantype_dist = df.group_by('LOANTYPE').agg(pl.count()).sort('LOANTYPE')
            print(f"  LOANTYPE distribution:")
            for row in loantype_dist.head(5).iter_rows(named=True):
                print(f"    {row['LOANTYPE']}: {row['count']} records")

        # Check FX application
        non_myr = df.filter(pl.col('CURCODE') != 'MYR')
        with_spotrate = non_myr.filter(pl.col('SPOTRATE').is_not_null())
        print(f"  Non-MYR with SPOTRATE: {len(with_spotrate)} / {len(non_myr)}")

    # Check Islamic loans
    if (output_dir / "ilnnote_updated.parquet").exists():
        df = pl.read_parquet(output_dir / "ilnnote_updated.parquet")
        print(f"\nIslamic Loans (ilnnote_updated.parquet):")
        print(f"  Total records: {len(df)}")

        # Check updates
        if 'LOANTYPE' in df.columns:
            loantype_dist = df.group_by('LOANTYPE').agg(pl.count()).sort('LOANTYPE')
            print(f"  LOANTYPE distribution:")
            for row in loantype_dist.head(5).iter_rows(named=True):
                print(f"    {row['LOANTYPE']}: {row['count']} records")

        # Check FX application
        non_myr = df.filter(pl.col('CURCODE') != 'MYR')
        with_spotrate = non_myr.filter(pl.col('SPOTRATE').is_not_null())
        print(f"  Non-MYR with SPOTRATE: {len(with_spotrate)} / {len(non_myr)}")


def main():
    """Run test"""
    print("Loan FX Processing Test")
    print("=" * 80)

    # Step 1: Create sample data
    print("\n1. Creating sample data files...")
    create_sample_fxrate_data()
    create_sample_lnnote_data()
    create_sample_lnwof_data()
    create_sample_ilnnote_data()
    create_sample_ilnwof_data()

    # Step 2: Run the processing
    print("\n2. Running loan FX processing...")
    print("=" * 80)
    import subprocess
    result = subprocess.run(['python3', 'loan_fx_processing.py'],
                            capture_output=True, text=True)

    if result.returncode == 0:
        print(result.stdout)

        # Step 3: Verify outputs
        print("\n3. Verifying outputs...")
        verify_outputs()

        # Step 4: Summary
        output_dir = Path("output")
        if output_dir.exists():
            files = list(output_dir.glob("*.parquet"))
            print("\n" + "=" * 80)
            print("✓ Test completed successfully!")
            print(f"\nGenerated {len(files)} output files:")
            for file in sorted(files):
                size = file.stat().st_size
                print(f"  - {file.name} ({size:,} bytes)")
    else:
        print(f"✗ Error running processing:")
        print(result.stderr)


if __name__ == "__main__":
    main()
