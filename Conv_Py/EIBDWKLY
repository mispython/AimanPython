#!/usr/bin/env python3
"""
File Name: EIBDWKLY
Loan and Deposit Data Processing with Dual Date Validation

1. Validates both loan and deposit extraction dates match report date
2. Executes LALWPBBD (loan processing) if validation passes
3. Executes LALWEIRC (EIR calculation) if validation passes
4. Aborts with error code 77 if any date mismatch

ESMR: 06-1428
Run after: EIBDLNEX
"""

import polars as pl
from pathlib import Path
import sys
import subprocess

# ============================================================================
# Configuration and Path Setup
# ============================================================================

# Input/Output paths
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input files
LOAN_REPTDATE_FILE = INPUT_DIR / "loan_reptdate.parquet"
DEPOSIT_REPTDATE_FILE = INPUT_DIR / "deposit_reptdate.parquet"

# Output files
REPTDATE_OUTPUT = OUTPUT_DIR / "reptdate.parquet"

# External programs to include
LALWPBBD_SCRIPT = Path("lalwpbbd_processing.py")
LALWEIRC_SCRIPT = Path("lalweirc_processing.py")

# Exit codes
EXIT_SUCCESS = 0
EXIT_DATE_MISMATCH = 77


# ============================================================================
# Date Processing Functions
# ============================================================================

def process_reptdate():
    """
    Process REPTDATE from LOAN to extract macro variables
    Returns: dict with macro variables
    """
    if not LOAN_REPTDATE_FILE.exists():
        raise FileNotFoundError(f"LOAN REPTDATE file not found: {LOAN_REPTDATE_FILE}")

    print("Processing REPTDATE from LOAN...")

    # Read REPTDATE from LOAN
    df = pl.read_parquet(LOAN_REPTDATE_FILE)

    if len(df) == 0:
        raise ValueError("LOAN REPTDATE file is empty")

    reptdate = df['REPTDATE'][0]

    # Extract date components
    wk = reptdate.day
    mm = reptdate.month
    year = reptdate.year

    # Format macro variables
    nowk = f"{wk:02d}"  # Z2. format
    reptmon = f"{mm:02d}"  # Z2. format
    reptyear = str(year)  # YEAR4. format
    reptyr = str(year)[-2:]  # YEAR2. format (last 2 digits)
    rdate = f"{reptdate.day:02d}/{reptdate.month:02d}/{year}"  # DDMMYY8.
    sdate = rdate  # Same as RDATE
    tdate = f"{year}{mm:02d}{wk:02d}"[2:]  # Z5. format (YMMDD)

    # Save REPTDATE to output (BNM.REPTDATE)
    df.write_parquet(REPTDATE_OUTPUT)

    return {
        'NOWK': nowk,
        'REPTMON': reptmon,
        'REPTYEAR': reptyear,
        'REPTYR': reptyr,
        'RDATE': rdate,
        'SDATE': sdate,
        'TDATE': tdate,
        'reptdate_obj': reptdate
    }


def get_loan_date():
    """
    Get loan extraction date from LOAN.REPTDATE
    Returns: loan date string in DDMMYY8 format
    """
    if not LOAN_REPTDATE_FILE.exists():
        raise FileNotFoundError(f"LOAN REPTDATE file not found: {LOAN_REPTDATE_FILE}")

    # Read REPTDATE from LOAN
    df = pl.read_parquet(LOAN_REPTDATE_FILE)

    if len(df) == 0:
        raise ValueError("LOAN REPTDATE file is empty")

    reptdate = df['REPTDATE'][0]

    # Format as DDMMYY8
    loan_date = f"{reptdate.day:02d}/{reptdate.month:02d}/{reptdate.year}"

    return loan_date


def get_deposit_date():
    """
    Get deposit extraction date from DEPOSIT.REPTDATE
    Returns: deposit date string in DDMMYY8 format
    """
    if not DEPOSIT_REPTDATE_FILE.exists():
        raise FileNotFoundError(f"DEPOSIT REPTDATE file not found: {DEPOSIT_REPTDATE_FILE}")

    # Read REPTDATE from DEPOSIT
    df = pl.read_parquet(DEPOSIT_REPTDATE_FILE)

    if len(df) == 0:
        raise ValueError("DEPOSIT REPTDATE file is empty")

    reptdate = df['REPTDATE'][0]

    # Format as DDMMYY8
    deposit_date = f"{reptdate.day:02d}/{reptdate.month:02d}/{reptdate.year}"

    return deposit_date


# ============================================================================
# Processing Functions
# ============================================================================

def execute_lalwpbbd():
    """
    Execute LALWPBBD processing script
    Equivalent to: %INC PGM(LALWPBBD);
    """
    if not LALWPBBD_SCRIPT.exists():
        print(f"Warning: LALWPBBD script not found: {LALWPBBD_SCRIPT}")
        print("Skipping LALWPBBD processing")
        return

    print(f"\nExecuting LALWPBBD processing...")
    print("=" * 80)

    # Execute LALWPBBD script
    result = subprocess.run(
        ['python3', str(LALWPBBD_SCRIPT)],
        capture_output=True,
        text=True
    )

    if result.returncode == 0:
        print(result.stdout)
        print("=" * 80)
        print("✓ LALWPBBD processing completed successfully")
    else:
        print(result.stderr)
        print("=" * 80)
        raise RuntimeError(f"LALWPBBD processing failed with code {result.returncode}")


def execute_lalweirc():
    """
    Execute LALWEIRC processing script
    Equivalent to: %INC PGM(LALWEIRC);
    """
    if not LALWEIRC_SCRIPT.exists():
        print(f"Warning: LALWEIRC script not found: {LALWEIRC_SCRIPT}")
        print("Skipping LALWEIRC processing")
        return

    print(f"\nExecuting LALWEIRC processing...")
    print("=" * 80)

    # Execute LALWEIRC script
    result = subprocess.run(
        ['python3', str(LALWEIRC_SCRIPT)],
        capture_output=True,
        text=True
    )

    if result.returncode == 0:
        print(result.stdout)
        print("=" * 80)
        print("✓ LALWEIRC processing completed successfully")
    else:
        print(result.stderr)
        print("=" * 80)
        raise RuntimeError(f"LALWEIRC processing failed with code {result.returncode}")


# ============================================================================
# Main Processing Logic
# ============================================================================

def validate_dates(loan_date, deposit_date, rdate):
    """
    Validate that both loan and deposit extraction dates match report date
    Returns: (all_match, loan_match, deposit_match)
    """
    loan_match = (loan_date == rdate)
    deposit_match = (deposit_date == rdate)
    all_match = loan_match and deposit_match

    print(f"\nDate Validation:")
    print(f"  LOAN extraction:    {loan_date}")
    print(f"  DEPOSIT extraction: {deposit_date}")
    print(f"  Expected (RDATE):   {rdate}")
    print()

    if loan_match:
        print(f"  ✓ LOAN date matches")
    else:
        print(f"  ✗ LOAN date does NOT match")

    if deposit_match:
        print(f"  ✓ DEPOSIT date matches")
    else:
        print(f"  ✗ DEPOSIT date does NOT match")

    return all_match, loan_match, deposit_match


def process_conditional(macro_vars):
    """
    Process data conditionally based on dual date validation
    Equivalent to %MACRO PROCESS logic
    """
    print("\n" + "=" * 80)
    print("CONDITIONAL PROCESSING")
    print("=" * 80)

    # Get extraction dates
    loan_date = get_loan_date()
    deposit_date = get_deposit_date()
    rdate = macro_vars['RDATE']

    # Validate dates
    all_match, loan_match, deposit_match = validate_dates(loan_date, deposit_date, rdate)

    # Check if both dates match
    if all_match:
        print("\n✓ All dates match - proceeding with processing")
        print("-" * 80)

        # Step 1: Execute LALWPBBD
        print("\n1. Executing LALWPBBD processing...")
        execute_lalwpbbd()

        # Step 2: Execute LALWEIRC
        print("\n2. Executing LALWEIRC processing...")
        execute_lalweirc()

        # Note: LALBDBAL and LALWPBBU are commented out in original
        # SMR2016-1430

        print("\n" + "=" * 80)
        print("✓ All processing completed successfully")
        print("=" * 80)

        return EXIT_SUCCESS
    else:
        print("\n✗ Date validation failed - aborting processing")
        print("-" * 80)

        # Print specific error messages
        if not loan_match:
            print(f"  THE LOAN EXTRACTION IS NOT DATED {rdate}")
        if not deposit_match:
            print(f"  THE DEPOSIT EXTRACTION IS NOT DATED {rdate}")

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
    print(f"  Year 4-digit (REPTYEAR): {macro_vars['REPTYEAR']}")
    print(f"  Year 2-digit (REPTYR): {macro_vars['REPTYR']}")
    print(f"  Report Date (RDATE): {macro_vars['RDATE']}")
    print(f"  Start Date (SDATE): {macro_vars['SDATE']}")
    print(f"  Timestamp (TDATE): {macro_vars['TDATE']}")

    print(f"\nGenerated Files:")
    if REPTDATE_OUTPUT.exists():
        size = REPTDATE_OUTPUT.stat().st_size
        print(f"  - {REPTDATE_OUTPUT} ({size:,} bytes)")

    # Check for processing outputs
    processing_outputs = []

    # LALWPBBD outputs
    lalwpbbd_files = list(OUTPUT_DIR.glob("lalwpbbd_*.parquet"))
    processing_outputs.extend(lalwpbbd_files)

    # LALWEIRC outputs
    lalweirc_files = list(OUTPUT_DIR.glob("lalweirc_*.parquet"))
    processing_outputs.extend(lalweirc_files)

    if processing_outputs:
        print(f"\n  Processing outputs:")
        for file in sorted(processing_outputs):
            size = file.stat().st_size
            print(f"    - {file.name} ({size:,} bytes)")


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Main execution function"""
    try:
        print("EIBDWKLY - Loan and Deposit Processing with Dual Date Validation")
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
        print(f"     REPTYEAR (year 4-digit): {macro_vars['REPTYEAR']}")
        print(f"     REPTYR (year 2-digit): {macro_vars['REPTYR']}")
        print(f"     RDATE (formatted): {macro_vars['RDATE']}")
        print(f"     SDATE (formatted): {macro_vars['SDATE']}")
        print(f"     TDATE (timestamp): {macro_vars['TDATE']}")

        # Step 2: Process conditionally based on dual date validation
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
