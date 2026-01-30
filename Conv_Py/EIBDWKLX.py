#!/usr/bin/env python3
"""
File Name: EIBDWKLX
Loan Data Processing with Date Validation

1. Validates loan extraction date matches report date
2. Copies SME08 data
3. Executes LALWPBBC processing if dates match
4. Aborts with error code if dates don't match

ESMR: 06-1428
Run after: EIBDLNEX
"""

import duckdb
import polars as pl
from datetime import datetime
from pathlib import Path
import sys


# ============================================================================
# Configuration and Path Setup
# ============================================================================

# Input/Output paths
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input files
REPTDATE_FILE = INPUT_DIR / "reptdate.parquet"
SME08_FILE = INPUT_DIR / "sme08.parquet"

# Output files
REPTDATE_OUTPUT = OUTPUT_DIR / "reptdate.parquet"
SME08_OUTPUT = OUTPUT_DIR / "sme08.parquet"

# External program to include (LALWPBBC)
LALWPBBC_SCRIPT = Path("lalwpbbc_processing.py")

# Exit codes
EXIT_SUCCESS = 0
EXIT_DATE_MISMATCH = 77


# ============================================================================
# Date Processing Functions
# ============================================================================

def process_reptdate():
    """
    Process REPTDATE to extract macro variables
    Returns: dict with macro variables
    """
    if not REPTDATE_FILE.exists():
        raise FileNotFoundError(f"REPTDATE file not found: {REPTDATE_FILE}")

    print("Processing REPTDATE...")

    # Read REPTDATE
    df = pl.read_parquet(REPTDATE_FILE)

    if len(df) == 0:
        raise ValueError("REPTDATE file is empty")

    reptdate = df['REPTDATE'][0]

    # Extract date components
    wk = reptdate.day
    mm = reptdate.month
    year = reptdate.year

    # Format macro variables
    nowk = f"{wk:02d}"  # Z2. format
    reptmon = f"{mm:02d}"  # Z2. format
    reptyear = str(year)  # YEAR4. format
    rdate = f"{reptdate.day:02d}/{reptdate.month:02d}/{year}"  # DDMMYY8.
    sdate = rdate  # Same as RDATE
    tdate = f"{reptdate.year}{reptdate.month:02d}{reptdate.day:02d}"[2:]  # Z5. format (YMMDD)

    # Save REPTDATE to output
    df.write_parquet(REPTDATE_OUTPUT)

    return {
        'NOWK': nowk,
        'REPTMON': reptmon,
        'REPTYEAR': reptyear,
        'RDATE': rdate,
        'SDATE': sdate,
        'TDATE': tdate,
        'reptdate_obj': reptdate
    }


def get_loan_date():
    """
    Get loan extraction date from REPTDATE
    Returns: loan date string in DDMMYY8 format
    """
    if not REPTDATE_FILE.exists():
        raise FileNotFoundError(f"REPTDATE file not found: {REPTDATE_FILE}")

    # Read REPTDATE
    df = pl.read_parquet(REPTDATE_FILE)

    if len(df) == 0:
        raise ValueError("REPTDATE file is empty")

    reptdate = df['REPTDATE'][0]

    # Format as DDMMYY8
    loan_date = f"{reptdate.day:02d}/{reptdate.month:02d}/{reptdate.year}"

    return loan_date


# ============================================================================
# Data Processing Functions
# ============================================================================

def copy_sme08():
    """
    Copy SME08 data from input to output
    Equivalent to: DATA BNM.SME08; SET BNM1.SME08; RUN;
    """
    if not SME08_FILE.exists():
        print(f"Warning: SME08 file not found: {SME08_FILE}")
        print("Creating empty SME08 dataset")
        # Create empty dataset with schema if file doesn't exist
        empty_df = pl.DataFrame()
        empty_df.write_parquet(SME08_OUTPUT)
        return

    print(f"Copying SME08 data...")

    # Read SME08 data
    sme08 = pl.read_parquet(SME08_FILE)

    print(f"  Records: {len(sme08):,}")

    # Write to output
    sme08.write_parquet(SME08_OUTPUT)

    print(f"  ✓ SME08 copied to {SME08_OUTPUT}")


def execute_lalwpbbc():
    """
    Execute LALWPBBC processing script
    Equivalent to: %INC PGM(LALWPBBC);
    """
    if not LALWPBBC_SCRIPT.exists():
        print(f"Warning: LALWPBBC script not found: {LALWPBBC_SCRIPT}")
        print("Skipping LALWPBBC processing")
        return

    print(f"\nExecuting LALWPBBC processing...")
    print("=" * 80)

    import subprocess

    # Execute LALWPBBC script
    result = subprocess.run(
        ['python3', str(LALWPBBC_SCRIPT)],
        capture_output=True,
        text=True
    )

    if result.returncode == 0:
        print(result.stdout)
        print("=" * 80)
        print("✓ LALWPBBC processing completed successfully")
    else:
        print(result.stderr)
        print("=" * 80)
        raise RuntimeError(f"LALWPBBC processing failed with code {result.returncode}")


# ============================================================================
# Main Processing Logic
# ============================================================================

def validate_dates(loan_date, rdate):
    """
    Validate that loan extraction date matches report date
    Returns: True if dates match, False otherwise
    """
    if loan_date == rdate:
        print(f"✓ Date validation passed: LOAN={loan_date}, RDATE={rdate}")
        return True
    else:
        print(f"✗ Date validation failed!")
        print(f"  LOAN extraction date: {loan_date}")
        print(f"  Expected date (RDATE): {rdate}")
        return False


def process_conditional(macro_vars):
    """
    Process data conditionally based on date validation
    Equivalent to %MACRO PROCESS logic
    """
    print("\n" + "=" * 80)
    print("CONDITIONAL PROCESSING")
    print("=" * 80)

    # Get loan extraction date
    loan_date = get_loan_date()
    rdate = macro_vars['RDATE']

    print(f"\nValidating dates...")
    print(f"  LOAN extraction: {loan_date}")
    print(f"  Report date (RDATE): {rdate}")

    # Check if dates match
    if validate_dates(loan_date, rdate):
        print("\n✓ Dates match - proceeding with processing")
        print("-" * 80)

        # Step 1: Copy SME08 data
        print("\n1. Copying SME08 data...")
        copy_sme08()

        # Step 2: Execute LALWPBBC
        print("\n2. Executing LALWPBBC processing...")
        execute_lalwpbbc()

        print("\n" + "=" * 80)
        print("✓ Processing completed successfully")
        print("=" * 80)

        return EXIT_SUCCESS
    else:
        print("\n✗ Dates do not match - aborting processing")
        print(f"  THE LOAN EXTRACTION IS NOT DATED {rdate}")
        print("  THE JOB IS NOT DONE !!")
        print("=" * 80)

        return EXIT_DATE_MISMATCH


# ============================================================================
# Summary Functions
# ============================================================================

def generate_summary(macro_vars):
    """
    Generate processing summary
    """
    print("\n" + "=" * 80)
    print("PROCESSING SUMMARY")
    print("=" * 80)

    print(f"\nDate Information:")
    print(f"  Week (NOWK): {macro_vars['NOWK']}")
    print(f"  Month (REPTMON): {macro_vars['REPTMON']}")
    print(f"  Year (REPTYEAR): {macro_vars['REPTYEAR']}")
    print(f"  Report Date (RDATE): {macro_vars['RDATE']}")
    print(f"  Start Date (SDATE): {macro_vars['SDATE']}")
    print(f"  Timestamp (TDATE): {macro_vars['TDATE']}")

    print(f"\nGenerated Files:")
    if REPTDATE_OUTPUT.exists():
        print(f"  - {REPTDATE_OUTPUT}")
    if SME08_OUTPUT.exists():
        size = SME08_OUTPUT.stat().st_size
        print(f"  - {SME08_OUTPUT} ({size:,} bytes)")

    # Check for LALWPBBC outputs
    lalwpbbc_outputs = list(OUTPUT_DIR.glob("lalwpbbc_*.parquet"))
    if lalwpbbc_outputs:
        print(f"\n  LALWPBBC outputs:")
        for file in sorted(lalwpbbc_outputs):
            size = file.stat().st_size
            print(f"    - {file.name} ({size:,} bytes)")


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Main execution function"""
    try:
        print("EIBDWKLX - Loan Data Processing with Date Validation")
        print("=" * 80)
        print("ESMR: 06-1428")
        print("Run after: EIBDLNEX")
        print("=" * 80)

        # Step 1: Process REPTDATE and extract macro variables
        print("\n1. Processing REPTDATE and extracting macro variables...")
        macro_vars = process_reptdate()

        print(f"\n   Macro Variables:")
        print(f"     NOWK (day): {macro_vars['NOWK']}")
        print(f"     REPTMON (month): {macro_vars['REPTMON']}")
        print(f"     REPTYEAR (year): {macro_vars['REPTYEAR']}")
        print(f"     RDATE (formatted): {macro_vars['RDATE']}")
        print(f"     SDATE (formatted): {macro_vars['SDATE']}")
        print(f"     TDATE (timestamp): {macro_vars['TDATE']}")

        # Step 2: Process conditionally based on date validation
        exit_code = process_conditional(macro_vars)

        # Step 3: Generate summary (only if successful)
        if exit_code == EXIT_SUCCESS:
            generate_summary(macro_vars)

        # Exit with appropriate code
        if exit_code != EXIT_SUCCESS:
            print(f"\n✗ Program exiting with code {exit_code}")
            sys.exit(exit_code)

        print("\n✓ Program completed successfully")

    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
