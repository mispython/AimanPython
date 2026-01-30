#!/usr/bin/env python3
"""
File Name: EIBDLNEX
Loan Data Processing with Foreign Exchange Rates

1. Processes loan note data (conventional and Islamic)
2. Updates product codes and census tracts from write-off data
3. Applies foreign exchange rates for non-MYR currencies

The program handles both conventional (PBB) and Islamic (PIBB) loan data.
"""

import duckdb
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import sys

# ============================================================================
# Configuration and Path Setup
# ============================================================================

# Input/Output paths
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input files - Conventional (PBB)
LNNOTE_FILE = INPUT_DIR / "lnnote.parquet"
LNWOF_FILE = INPUT_DIR / "lnwof.parquet"

# Input files - Islamic (PIBB)
ILNNOTE_FILE = INPUT_DIR / "ilnnote.parquet"
ILNWOF_FILE = INPUT_DIR / "ilnwof.parquet"

# Foreign exchange rate file
FXRATE_FILE = INPUT_DIR / "foratebkp.parquet"

# Optional: Date file (for conditional processing)
DATEFILE = INPUT_DIR / "datefile.txt"

# Output files
LNNOTE_OUTPUT = OUTPUT_DIR / "lnnote_updated.parquet"
ILNNOTE_OUTPUT = OUTPUT_DIR / "ilnnote_updated.parquet"
FXRATE_OUTPUT = OUTPUT_DIR / "fxrate_current.parquet"

# Processing flag (simulates OPC conditional logic)
# Set to True to always process, False to check date file
ALWAYS_RUN = True


# ============================================================================
# Date Processing Functions
# ============================================================================

def check_run_date():
    """
    Check if processing should run based on date file
    Simulates OPC conditional logic: run on 8th, 15th, 22nd, or last day of month
    Returns: True if should run, False otherwise
    """
    if ALWAYS_RUN:
        return True

    if not DATEFILE.exists():
        print("Warning: Date file not found, defaulting to run=True")
        return True

    try:
        with open(DATEFILE, 'r') as f:
            line = f.readline().strip()
            # Extract date from first 11 characters (format varies)
            # EXTDATE format: typically MMDDYYYYHHMMSS or similar
            if len(line) >= 8:
                date_str = line[0:8]
                # Try parsing as MMDDYYYY
                try:
                    reptdate = datetime.strptime(date_str, '%m%d%Y')
                except:
                    # Try DDMMYYYY
                    try:
                        reptdate = datetime.strptime(date_str, '%d%m%Y')
                    except:
                        print(f"Warning: Could not parse date from: {date_str}")
                        return True

                day = reptdate.day

                # Check if it's 8th, 15th, 22nd, or last day of month
                last_day = (reptdate.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)

                if day in [8, 15, 22] or day == last_day.day:
                    print(f"Run date check: {reptdate.date()} (day {day}) - PROCESS")
                    return True
                else:
                    print(f"Run date check: {reptdate.date()} (day {day}) - SKIP")
                    return False
    except Exception as e:
        print(f"Warning: Error checking date file: {e}")
        return True

    return True


# ============================================================================
# Foreign Exchange Rate Processing
# ============================================================================

def process_fx_rates():
    """
    Process foreign exchange rates
    1. Filter rates <= yesterday
    2. Get most recent rate per currency
    3. Create format lookup (simulates PROC FORMAT)
    Returns: DataFrame with CURCODE and SPOTRATE
    """
    if not FXRATE_FILE.exists():
        print(f"Warning: FX rate file not found: {FXRATE_FILE}")
        return pl.DataFrame({'CURCODE': [], 'SPOTRATE': []})

    print("\n1. Processing FX rates...")

    # Read FX rate backup data
    fxrate_df = pl.read_parquet(FXRATE_FILE)

    # Filter: REPTDATE <= TODAY()-1 (yesterday)
    yesterday = datetime.now().date() - timedelta(days=1)
    fxrate_df = fxrate_df.filter(pl.col('REPTDATE').cast(pl.Date) <= yesterday)

    # Sort by CURCODE and REPTDATE descending (most recent first)
    fxrate_df = fxrate_df.sort(['CURCODE', 'REPTDATE'], descending=[False, True])

    # Keep only most recent rate per currency (NODUPKEY equivalent)
    fxrate_df = fxrate_df.unique(subset=['CURCODE'], keep='first')

    # Select only needed columns
    fxrate_df = fxrate_df.select(['CURCODE', 'SPOTRATE'])

    print(f"   Found {len(fxrate_df)} currency rates")

    # Save current FX rates
    fxrate_df.write_parquet(FXRATE_OUTPUT)

    return fxrate_df


def create_fx_lookup(fxrate_df):
    """
    Create FX rate lookup dictionary (simulates PROC FORMAT)
    Returns: dict mapping CURCODE to SPOTRATE
    """
    fx_lookup = {}
    for row in fxrate_df.iter_rows(named=True):
        fx_lookup[row['CURCODE']] = row['SPOTRATE']

    return fx_lookup


# ============================================================================
# Loan Note Processing - Conventional (PBB)
# ============================================================================

def process_conventional_loans(fx_lookup):
    """
    Process conventional loan notes (PBB)
    Updates: LOANTYPE (from PRODUCT/ORICODE), CENSUS (from CENSUS_TRT), SPOTRATE
    """
    print("\n2. Processing conventional loans (PBB)...")

    if not LNNOTE_FILE.exists():
        print(f"   Warning: LNNOTE file not found: {LNNOTE_FILE}")
        return

    # Read loan note data
    lnnote = pl.read_parquet(LNNOTE_FILE)
    print(f"   LNNOTE records: {len(lnnote)}")

    # Read write-off data if exists
    if LNWOF_FILE.exists():
        lnwof = pl.read_parquet(LNWOF_FILE)
        lnwof = lnwof.select(['ACCTNO', 'NOTENO', 'WRITE_DOWN_BAL', 'PRODUCT',
                              'CENSUS_TRT', 'ORICODE'])
        print(f"   LNWOF records: {len(lnwof)}")

        # Merge with write-off data (LEFT JOIN, keep all LNNOTE records)
        lnnote = lnnote.join(lnwof, on=['ACCTNO', 'NOTENO'], how='left', suffix='_WOF')
    else:
        print("   Warning: LNWOF file not found, skipping write-off updates")
        # Add placeholder columns
        lnnote = lnnote.with_columns([
            pl.lit(None).cast(pl.Int64).alias('PRODUCT'),
            pl.lit(None).cast(pl.Float64).alias('CENSUS_TRT'),
            pl.lit(None).cast(pl.Int64).alias('ORICODE')
        ])

    # Update LOANTYPE from PRODUCT
    # IF PRODUCT NOT IN (.,0) THEN LOANTYPE=PRODUCT
    lnnote = lnnote.with_columns([
        pl.when(
            (pl.col('PRODUCT').is_not_null()) &
            (pl.col('PRODUCT') != 0)
        )
        .then(pl.col('PRODUCT'))
        .otherwise(pl.col('LOANTYPE'))
        .alias('LOANTYPE')
    ])

    # Update LOANTYPE from ORICODE (overwrites PRODUCT if ORICODE exists)
    # IF ORICODE NOT IN (.,0) THEN LOANTYPE=ORICODE
    lnnote = lnnote.with_columns([
        pl.when(
            (pl.col('ORICODE').is_not_null()) &
            (pl.col('ORICODE') != 0)
        )
        .then(pl.col('ORICODE'))
        .otherwise(pl.col('LOANTYPE'))
        .alias('LOANTYPE')
    ])

    # Update CENSUS from CENSUS_TRT
    # IF CENSUS_TRT NOT IN (.,0) THEN CENSUS=CENSUS_TRT
    lnnote = lnnote.with_columns([
        pl.when(
            (pl.col('CENSUS_TRT').is_not_null()) &
            (pl.col('CENSUS_TRT') != 0)
        )
        .then(pl.col('CENSUS_TRT'))
        .otherwise(pl.col('CENSUS'))
        .alias('CENSUS')
    ])

    # Apply FX rates for non-MYR currencies
    # IF CURCODE NE 'MYR' THEN SPOTRATE = PUT(CURCODE,$FORATE.)*1
    def get_spotrate(curcode):
        """Get spotrate from FX lookup"""
        if curcode == 'MYR' or curcode is None:
            return None
        return fx_lookup.get(curcode, None)

    # Create SPOTRATE column for non-MYR
    lnnote = lnnote.with_columns([
        pl.when(pl.col('CURCODE') != 'MYR')
        .then(
            pl.col('CURCODE').map_elements(
                lambda x: get_spotrate(x),
                return_dtype=pl.Float64
            )
        )
        .otherwise(pl.col('SPOTRATE') if 'SPOTRATE' in lnnote.columns else None)
        .alias('SPOTRATE')
    ])

    # Drop temporary columns (PRODUCT, CENSUS_TRT from merge)
    columns_to_drop = []
    if 'PRODUCT' in lnnote.columns:
        columns_to_drop.append('PRODUCT')
    if 'CENSUS_TRT' in lnnote.columns:
        columns_to_drop.append('CENSUS_TRT')
    if 'ORICODE' in lnnote.columns:
        columns_to_drop.append('ORICODE')

    if columns_to_drop:
        lnnote = lnnote.drop(columns_to_drop)

    # Save updated loan notes
    lnnote.write_parquet(LNNOTE_OUTPUT)
    print(f"   ✓ Updated LNNOTE saved to {LNNOTE_OUTPUT}")
    print(f"   Final records: {len(lnnote)}")

    return lnnote


# ============================================================================
# Loan Note Processing - Islamic (PIBB)
# ============================================================================

def process_islamic_loans(fx_lookup):
    """
    Process Islamic loan notes (PIBB)
    Updates: LOANTYPE (from PRODUCT/ORICODE), CENSUS (from CENSUS_TRT), SPOTRATE
    """
    print("\n3. Processing Islamic loans (PIBB)...")

    if not ILNNOTE_FILE.exists():
        print(f"   Warning: ILNNOTE file not found: {ILNNOTE_FILE}")
        return

    # Read Islamic loan note data
    ilnnote = pl.read_parquet(ILNNOTE_FILE)
    print(f"   ILNNOTE records: {len(ilnnote)}")

    # Read Islamic write-off data if exists
    if ILNWOF_FILE.exists():
        ilnwof = pl.read_parquet(ILNWOF_FILE)
        ilnwof = ilnwof.select(['ACCTNO', 'NOTENO', 'WRITE_DOWN_BAL', 'PRODUCT',
                                'CENSUS_TRT', 'ORICODE'])
        print(f"   ILNWOF records: {len(ilnwof)}")

        # Merge with write-off data (LEFT JOIN, keep all ILNNOTE records)
        ilnnote = ilnnote.join(ilnwof, on=['ACCTNO', 'NOTENO'], how='left', suffix='_WOF')
    else:
        print("   Warning: ILNWOF file not found, skipping write-off updates")
        # Add placeholder columns
        ilnnote = ilnnote.with_columns([
            pl.lit(None).cast(pl.Int64).alias('PRODUCT'),
            pl.lit(None).cast(pl.Float64).alias('CENSUS_TRT'),
            pl.lit(None).cast(pl.Int64).alias('ORICODE')
        ])

    # Update LOANTYPE from PRODUCT
    ilnnote = ilnnote.with_columns([
        pl.when(
            (pl.col('PRODUCT').is_not_null()) &
            (pl.col('PRODUCT') != 0)
        )
        .then(pl.col('PRODUCT'))
        .otherwise(pl.col('LOANTYPE'))
        .alias('LOANTYPE')
    ])

    # Update LOANTYPE from ORICODE (overwrites PRODUCT if ORICODE exists)
    ilnnote = ilnnote.with_columns([
        pl.when(
            (pl.col('ORICODE').is_not_null()) &
            (pl.col('ORICODE') != 0)
        )
        .then(pl.col('ORICODE'))
        .otherwise(pl.col('LOANTYPE'))
        .alias('LOANTYPE')
    ])

    # Update CENSUS from CENSUS_TRT
    ilnnote = ilnnote.with_columns([
        pl.when(
            (pl.col('CENSUS_TRT').is_not_null()) &
            (pl.col('CENSUS_TRT') != 0)
        )
        .then(pl.col('CENSUS_TRT'))
        .otherwise(pl.col('CENSUS'))
        .alias('CENSUS')
    ])

    # Apply FX rates for non-MYR currencies
    def get_spotrate(curcode):
        """Get spotrate from FX lookup"""
        if curcode == 'MYR' or curcode is None:
            return None
        return fx_lookup.get(curcode, None)

    # Create SPOTRATE column for non-MYR
    ilnnote = ilnnote.with_columns([
        pl.when(pl.col('CURCODE') != 'MYR')
        .then(
            pl.col('CURCODE').map_elements(
                lambda x: get_spotrate(x),
                return_dtype=pl.Float64
            )
        )
        .otherwise(pl.col('SPOTRATE') if 'SPOTRATE' in ilnnote.columns else None)
        .alias('SPOTRATE')
    ])

    # Drop temporary columns
    columns_to_drop = []
    if 'PRODUCT' in ilnnote.columns:
        columns_to_drop.append('PRODUCT')
    if 'CENSUS_TRT' in ilnnote.columns:
        columns_to_drop.append('CENSUS_TRT')
    if 'ORICODE' in ilnnote.columns:
        columns_to_drop.append('ORICODE')

    if columns_to_drop:
        ilnnote = ilnnote.drop(columns_to_drop)

    # Save updated Islamic loan notes
    ilnnote.write_parquet(ILNNOTE_OUTPUT)
    print(f"   ✓ Updated ILNNOTE saved to {ILNNOTE_OUTPUT}")
    print(f"   Final records: {len(ilnnote)}")

    return ilnnote


# ============================================================================
# Summary and Validation
# ============================================================================

def generate_summary(lnnote, ilnnote, fx_lookup):
    """
    Generate processing summary
    """
    print("\n" + "=" * 80)
    print("PROCESSING SUMMARY")
    print("=" * 80)

    if lnnote is not None:
        print(f"\nConventional Loans (PBB):")
        print(f"  Total records: {len(lnnote):,}")

        # Count non-MYR records
        non_myr = lnnote.filter(pl.col('CURCODE') != 'MYR')
        print(f"  Non-MYR records: {len(non_myr):,}")

        # Count records with updated LOANTYPE
        if 'LOANTYPE' in lnnote.columns:
            with_loantype = lnnote.filter(pl.col('LOANTYPE').is_not_null())
            print(f"  Records with LOANTYPE: {len(with_loantype):,}")

        # Count records with updated CENSUS
        if 'CENSUS' in lnnote.columns:
            with_census = lnnote.filter(pl.col('CENSUS').is_not_null())
            print(f"  Records with CENSUS: {len(with_census):,}")

    if ilnnote is not None:
        print(f"\nIslamic Loans (PIBB):")
        print(f"  Total records: {len(ilnnote):,}")

        # Count non-MYR records
        non_myr = ilnnote.filter(pl.col('CURCODE') != 'MYR')
        print(f"  Non-MYR records: {len(non_myr):,}")

        # Count records with updated LOANTYPE
        if 'LOANTYPE' in ilnnote.columns:
            with_loantype = ilnnote.filter(pl.col('LOANTYPE').is_not_null())
            print(f"  Records with LOANTYPE: {len(with_loantype):,}")

        # Count records with updated CENSUS
        if 'CENSUS' in ilnnote.columns:
            with_census = ilnnote.filter(pl.col('CENSUS').is_not_null())
            print(f"  Records with CENSUS: {len(with_census):,}")

    print(f"\nForeign Exchange Rates:")
    print(f"  Currencies loaded: {len(fx_lookup)}")
    if len(fx_lookup) > 0:
        print(f"  Sample rates:")
        for i, (cur, rate) in enumerate(list(fx_lookup.items())[:5]):
            print(f"    {cur}: {rate:.6f}")
        if len(fx_lookup) > 5:
            print(f"    ... and {len(fx_lookup) - 5} more")


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Main execution function"""
    try:
        print("Loan Data Processing with FX Rates")
        print("=" * 80)

        # Check if processing should run (OPC conditional logic)
        if not check_run_date():
            print("\nProcessing skipped based on date criteria")
            print("Set ALWAYS_RUN = True to override")
            return

        print("\nStarting loan data processing...")

        # Step 1: Process FX rates
        fxrate_df = process_fx_rates()
        fx_lookup = create_fx_lookup(fxrate_df)

        # Step 2: Process conventional loans
        lnnote = process_conventional_loans(fx_lookup)

        # Step 3: Process Islamic loans
        ilnnote = process_islamic_loans(fx_lookup)

        # Step 4: Generate summary
        generate_summary(lnnote, ilnnote, fx_lookup)

        print("\n" + "=" * 80)
        print("✓ Loan Data Processing Complete!")
        print("\nGenerated files:")
        if LNNOTE_OUTPUT.exists():
            print(f"  - {LNNOTE_OUTPUT}")
        if ILNNOTE_OUTPUT.exists():
            print(f"  - {ILNNOTE_OUTPUT}")
        if FXRATE_OUTPUT.exists():
            print(f"  - {FXRATE_OUTPUT}")

    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
