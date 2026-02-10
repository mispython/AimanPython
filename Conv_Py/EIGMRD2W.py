#!/usr/bin/env python3
"""
File Name: EIGMRD2W
ALM Walker Data Extraction and Conversion
Reads Walker ALM file, validates date, and splits into multiple datasets
"""

import polars as pl
import duckdb
from datetime import datetime, timedelta
from pathlib import Path
import sys

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

# Base paths
BASE_PATH = Path("./data")
INPUT_PATH = BASE_PATH / "input"
OUTPUT_PATH = BASE_PATH / "output"

# Input file
WALALM_PATH = INPUT_PATH / "walker" / "WALALM.txt"

# Output paths - ALM library
ALM_PATH = OUTPUT_PATH / "alm"
ALM_REPTDATE = ALM_PATH / "REPTDATE.parquet"
ALM_R2GL4000 = ALM_PATH / "R2GL4000.parquet"
ALM_R2KEY913 = ALM_PATH / "R2KEY913.parquet"
ALM_R2R115 = ALM_PATH / "R2R115.parquet"
ALM_R2R913 = ALM_PATH / "R2R913.parquet"

# Ensure output directories exist
ALM_PATH.mkdir(parents=True, exist_ok=True)


# ============================================================================
# DATE CALCULATION
# ============================================================================

def calculate_current_reporting_date() -> datetime:
    """
    Calculate current reporting date as last day of previous month
    Mimics SAS: INPUT('01'||PUT(MONTH(TODAY()), Z2.)||PUT(YEAR(TODAY()), 4.), DDMMYY8.)-1
    """
    today = datetime.now()

    # First day of current month
    first_of_month = datetime(today.year, today.month, 1)

    # Last day of previous month
    reptdate = first_of_month - timedelta(days=1)

    return reptdate


# ============================================================================
# WALKER FILE READING
# ============================================================================

def extract_walker_date(filepath: Path) -> datetime:
    """
    Extract REPTDATE from first line of Walker ALM file
    """
    if not filepath.exists():
        raise FileNotFoundError(f"Walker ALM file not found: {filepath}")

    with open(filepath, 'r') as f:
        first_line = f.readline()

    # Extract WALKDATE from position 2 (8 characters)
    # INPUT @2 WALKDATE $8.
    if len(first_line) < 9:
        raise ValueError(f"Invalid Walker ALM file format: first line too short")

    walkdate = first_line[1:9]  # Position 2-9 (0-indexed: 1-9)

    # Parse DDMMYY8. format (e.g., "31012025")
    try:
        reptdate = datetime.strptime(walkdate, '%d%m%Y')
    except ValueError:
        raise ValueError(f"Could not parse date from Walker ALM file: {walkdate}")

    return reptdate


def save_reptdate(reptdate: datetime, alm_path: Path):
    """
    Save REPTDATE to ALM library
    """
    df = pl.DataFrame({
        'REPTDATE': [reptdate]
    })

    df.write_parquet(alm_path)

    print(f"REPTDATE saved: {reptdate.strftime('%d%m%Y')}")
    print(f"  ALM: {alm_path}")


# ============================================================================
# WALKER ALM DATA PARSING
# ============================================================================

def parse_walker_alm_file(filepath: Path) -> dict:
    """
    Parse Walker ALM file and split into multiple datasets
    Returns dict of DataFrames
    """
    print(f"\nParsing Walker ALM file: {filepath}")

    # Storage for different record types
    r2gl4000_records = []
    r2key913_records = []
    r2r115_records = []
    r2r913_records = []

    setid = None
    record_count = 0

    with open(filepath, 'r') as f:
        for line in f:
            if len(line) < 2:
                continue

            record_count += 1
            id_char = line[1]  # Position 2 (0-indexed: 1)

            # Record type '1' sets the SETID
            if id_char == '1':
                if len(line) >= 10:
                    setid = line[2:10].strip()  # Position 3-10 (8 chars)

            # Record type '+' contains data
            elif id_char == '+' and setid:

                if setid == 'R2GL4000':
                    # INPUT @3 ACCT_NO $22. @29 ACKIND $1. @30 GLAMT COMMA21.2
                    if len(line) >= 51:
                        acct_no = line[2:24].strip()  # Pos 3-24 (22 chars)
                        ackind = line[28:29]  # Pos 29 (1 char)
                        glamt_str = line[29:51].strip()  # Pos 30-50 (21 chars for COMMA21.2)

                        # Parse amount (remove commas)
                        try:
                            glamt = float(glamt_str.replace(',', ''))
                        except ValueError:
                            glamt = 0.0

                        r2gl4000_records.append({
                            'ACCT_NO': acct_no,
                            'ACKIND': ackind,
                            'GLAMT': glamt
                        })

                elif setid == 'R2KEY913':
                    # INPUT @3 ACCT_NO $22.
                    if len(line) >= 24:
                        acct_no = line[2:24].strip()  # Pos 3-24 (22 chars)

                        r2key913_records.append({
                            'ACCT_NO': acct_no
                        })

                elif setid == 'R2R115':
                    # INPUT @3 SET_ID $12. @17 USERCD $4.
                    if len(line) >= 20:
                        set_id = line[2:14].strip()  # Pos 3-14 (12 chars)
                        usercd = line[16:20].strip()  # Pos 17-20 (4 chars)

                        r2r115_records.append({
                            'SET_ID': set_id,
                            'USERCD': usercd
                        })

                elif setid == 'R2R913':
                    # INPUT @3 ACCT_NO $22. @28 SET_ID $12.
                    if len(line) >= 39:
                        acct_no = line[2:24].strip()  # Pos 3-24 (22 chars)
                        set_id = line[27:39].strip()  # Pos 28-39 (12 chars)

                        r2r913_records.append({
                            'ACCT_NO': acct_no,
                            'SET_ID': set_id
                        })

    print(f"Total lines processed: {record_count:,}")
    print(f"Records extracted:")
    print(f"  R2GL4000: {len(r2gl4000_records):,}")
    print(f"  R2KEY913: {len(r2key913_records):,}")
    print(f"  R2R115:   {len(r2r115_records):,}")
    print(f"  R2R913:   {len(r2r913_records):,}")

    # Convert to Polars DataFrames
    datasets = {}

    if r2gl4000_records:
        datasets['R2GL4000'] = pl.DataFrame(r2gl4000_records)
    else:
        datasets['R2GL4000'] = pl.DataFrame({
            'ACCT_NO': pl.Series([], dtype=pl.Utf8),
            'ACKIND': pl.Series([], dtype=pl.Utf8),
            'GLAMT': pl.Series([], dtype=pl.Float64)
        })

    if r2key913_records:
        datasets['R2KEY913'] = pl.DataFrame(r2key913_records)
    else:
        datasets['R2KEY913'] = pl.DataFrame({
            'ACCT_NO': pl.Series([], dtype=pl.Utf8)
        })

    if r2r115_records:
        datasets['R2R115'] = pl.DataFrame(r2r115_records)
    else:
        datasets['R2R115'] = pl.DataFrame({
            'SET_ID': pl.Series([], dtype=pl.Utf8),
            'USERCD': pl.Series([], dtype=pl.Utf8)
        })

    if r2r913_records:
        datasets['R2R913'] = pl.DataFrame(r2r913_records)
    else:
        datasets['R2R913'] = pl.DataFrame({
            'ACCT_NO': pl.Series([], dtype=pl.Utf8),
            'SET_ID': pl.Series([], dtype=pl.Utf8)
        })

    return datasets


# ============================================================================
# DATA SORTING AND SAVING
# ============================================================================

def sort_and_save_datasets(datasets: dict):
    """
    Sort datasets according to SAS PROC SORT specifications and save
    """
    print("\nSorting and saving datasets...")

    # R2GL4000: Sort by ACCT_NO, ACKIND, GLAMT
    if len(datasets['R2GL4000']) > 0:
        df = datasets['R2GL4000'].sort(['ACCT_NO', 'ACKIND', 'GLAMT'])
        df.write_parquet(ALM_R2GL4000)
        print(f"  R2GL4000: {len(df):,} records -> {ALM_R2GL4000}")
    else:
        datasets['R2GL4000'].write_parquet(ALM_R2GL4000)
        print(f"  R2GL4000: 0 records (empty) -> {ALM_R2GL4000}")

    # R2KEY913: Sort by ACCT_NO
    if len(datasets['R2KEY913']) > 0:
        df = datasets['R2KEY913'].sort('ACCT_NO')
        df.write_parquet(ALM_R2KEY913)
        print(f"  R2KEY913: {len(df):,} records -> {ALM_R2KEY913}")
    else:
        datasets['R2KEY913'].write_parquet(ALM_R2KEY913)
        print(f"  R2KEY913: 0 records (empty) -> {ALM_R2KEY913}")

    # R2R115: Sort by SET_ID
    if len(datasets['R2R115']) > 0:
        df = datasets['R2R115'].sort('SET_ID')
        df.write_parquet(ALM_R2R115)
        print(f"  R2R115:   {len(df):,} records -> {ALM_R2R115}")
    else:
        datasets['R2R115'].write_parquet(ALM_R2R115)
        print(f"  R2R115:   0 records (empty) -> {ALM_R2R115}")

    # R2R913: Sort by ACCT_NO, SET_ID
    if len(datasets['R2R913']) > 0:
        df = datasets['R2R913'].sort(['ACCT_NO', 'SET_ID'])
        df.write_parquet(ALM_R2R913)
        print(f"  R2R913:   {len(df):,} records -> {ALM_R2R913}")
    else:
        datasets['R2R913'].write_parquet(ALM_R2R913)
        print(f"  R2R913:   0 records (empty) -> {ALM_R2R913}")


# ============================================================================
# CONVERSION PROCESS
# ============================================================================

def convert():
    """
    Main conversion process - parse and split Walker ALM file
    Mimics %CONVERT macro
    """
    print("\n" + "=" * 70)
    print("CONVERTING WALKER ALM FILE")
    print("=" * 70)

    # Parse Walker ALM file
    datasets = parse_walker_alm_file(WALALM_PATH)

    # Sort and save datasets
    sort_and_save_datasets(datasets)

    print("\n" + "=" * 70)
    print("CONVERSION COMPLETE")
    print("=" * 70)


# ============================================================================
# MAIN PROCESS
# ============================================================================

def process():
    """
    Main process - validates date and converts if match
    Mimics %PROCESS macro
    """
    print("=" * 70)
    print("WALKER ALM DATA EXTRACTION")
    print("=" * 70)

    # Calculate current reporting date (NDATE) - last day of previous month
    ndate = calculate_current_reporting_date()
    print(f"\nCurrent reporting date (NDATE): {ndate.strftime('%d%m%Y')}")

    # Extract date from Walker ALM file (RDATE)
    try:
        rdate = extract_walker_date(WALALM_PATH)
        print(f"Walker ALM file date (RDATE):   {rdate.strftime('%d%m%Y')}")

        # Save REPTDATE
        save_reptdate(rdate, ALM_REPTDATE)

    except Exception as e:
        print(f"ERROR: Could not extract date from Walker ALM file: {e}")
        sys.exit(1)

    # Validate dates match
    print("\n" + "=" * 70)
    print("DATE VALIDATION")
    print("=" * 70)

    if ndate == rdate:
        print(f"✓ Dates match: {ndate.strftime('%d%m%Y')}")
        print("  Proceeding with conversion...")

        # Execute conversion
        convert()

    else:
        print(f"✗ Date mismatch!")
        print(f"  Expected: {ndate.strftime('%d%m%Y')}")
        print(f"  Found:    {rdate.strftime('%d%m%Y')}")
        print("\nWARNING: WALKER SOURCE IS NOT AS AT CURRENT REPORTING DATE")
        print("\nConversion aborted.")
        sys.exit(1)

    # Summary
    print("\n" + "=" * 70)
    print("PROCESSING SUMMARY")
    print("=" * 70)
    print(f"Report Date:  {rdate.strftime('%d%m%Y')}")
    print(f"\nOutput files generated:")
    print(f"  REPTDATE:  {ALM_REPTDATE}")
    print(f"  R2GL4000:  {ALM_R2GL4000}")
    print(f"  R2KEY913:  {ALM_R2KEY913}")
    print(f"  R2R115:    {ALM_R2R115}")
    print(f"  R2R913:    {ALM_R2R913}")
    print("=" * 70)


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    try:
        process()
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
