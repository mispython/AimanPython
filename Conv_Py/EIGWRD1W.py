#!/usr/bin/env python3
"""
File Name: EIGWRD1W
Walker Data Extraction and Conversion
Reads Walker ALW file, validates date, and splits into multiple datasets
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
WALALW_PATH = INPUT_PATH / "walker" / "WALALW.txt"

# Output paths - ALWZ library
ALWZ_PATH = OUTPUT_PATH / "alwz"
ALWZ_REPTDATE = ALWZ_PATH / "REPTDATE.parquet"

# Output paths - ALW library
ALW_PATH = OUTPUT_PATH / "alw"
ALW_REPTDATE = ALW_PATH / "REPTDATE.parquet"
ALW_R1GL4000 = ALW_PATH / "R1GL4000.parquet"
ALW_R1GL4100 = ALW_PATH / "R1GL4100.parquet"
ALW_R1KEY913 = ALW_PATH / "R1KEY913.parquet"
ALW_R1R115 = ALW_PATH / "R1R115.parquet"
ALW_R1R913 = ALW_PATH / "R1R913.parquet"

# Ensure output directories exist
ALWZ_PATH.mkdir(parents=True, exist_ok=True)
ALW_PATH.mkdir(parents=True, exist_ok=True)


# ============================================================================
# DATE CALCULATION
# ============================================================================

def calculate_current_reporting_date() -> datetime:
    """
    Calculate current reporting date based on today's date
    Mimics SAS date calculation logic
    """
    today = datetime.now()
    dd = today.day
    mm = today.month
    yy = today.year

    # Determine reporting date based on day of month
    if dd < 8:
        # Previous month's last day
        first_of_month = datetime(yy, mm, 1)
        reptdate = first_of_month - timedelta(days=1)
    elif dd < 15:
        reptdate = datetime(yy, mm, 8)
    elif dd < 22:
        reptdate = datetime(yy, mm, 15)
    else:
        reptdate = datetime(yy, mm, 22)

    return reptdate


# ============================================================================
# WALKER FILE READING
# ============================================================================

def extract_walker_date(filepath: Path) -> datetime:
    """
    Extract REPTDATE from first line of Walker file
    """
    if not filepath.exists():
        raise FileNotFoundError(f"Walker file not found: {filepath}")

    with open(filepath, 'r') as f:
        first_line = f.readline()

    # Extract WALKDATE from position 2 (8 characters)
    # INPUT @2 WALKDATE $8.
    if len(first_line) < 9:
        raise ValueError(f"Invalid Walker file format: first line too short")

    walkdate = first_line[1:9]  # Position 2-9 (0-indexed: 1-9)

    # Parse DDMMYY8. format (e.g., "31012025")
    try:
        reptdate = datetime.strptime(walkdate, '%d%m%Y')
    except ValueError:
        raise ValueError(f"Could not parse date from Walker file: {walkdate}")

    return reptdate


def save_reptdate(reptdate: datetime, alwz_path: Path, alw_path: Path):
    """
    Save REPTDATE to both ALWZ and ALW libraries
    """
    df = pl.DataFrame({
        'REPTDATE': [reptdate]
    })

    df.write_parquet(alwz_path)
    df.write_parquet(alw_path)

    print(f"REPTDATE saved: {reptdate.strftime('%d%m%Y')}")
    print(f"  ALWZ: {alwz_path}")
    print(f"  ALW:  {alw_path}")


# ============================================================================
# WALKER DATA PARSING
# ============================================================================

def parse_walker_file(filepath: Path) -> dict:
    """
    Parse Walker file and split into multiple datasets
    Returns dict of DataFrames
    """
    print(f"\nParsing Walker file: {filepath}")

    # Storage for different record types
    r1gl4000_records = []
    r1gl4100_records = []
    r1key913_records = []
    r1r115_records = []
    r1r913_records = []

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

                if setid == 'R1GL4000':
                    # INPUT @3 ACCT_NO $22. @29 ACKIND $1. @31 GLAMT COMMA21.2
                    if len(line) >= 52:
                        acct_no = line[2:24].strip()  # Pos 3-24 (22 chars)
                        ackind = line[28:29]  # Pos 29 (1 char)
                        glamt_str = line[30:52].strip()  # Pos 31-51 (21 chars for COMMA21.2)

                        # Parse amount (remove commas)
                        try:
                            glamt = float(glamt_str.replace(',', ''))
                        except ValueError:
                            glamt = 0.0

                        r1gl4000_records.append({
                            'ACCT_NO': acct_no,
                            'ACKIND': ackind,
                            'GLAMT': glamt
                        })

                elif setid == 'R1GL4100':
                    # INPUT @3 ACCT_NO $22. @30 EFFDATE DDMMYY8. @47 JEAMT COMMA21.2 @75 ACKIND $1.
                    if len(line) >= 75:
                        acct_no = line[2:24].strip()  # Pos 3-24 (22 chars)
                        effdate_str = line[29:37]  # Pos 30-37 (8 chars)
                        jeamt_str = line[46:68].strip()  # Pos 47-67 (21 chars)
                        ackind = line[74:75]  # Pos 75 (1 char)

                        # Parse date
                        try:
                            effdate = datetime.strptime(effdate_str, '%d%m%Y')
                        except ValueError:
                            effdate = None

                        # Parse amount
                        try:
                            jeamt = float(jeamt_str.replace(',', ''))
                        except ValueError:
                            jeamt = 0.0

                        r1gl4100_records.append({
                            'ACCT_NO': acct_no,
                            'EFFDATE': effdate,
                            'JEAMT': jeamt,
                            'ACKIND': ackind
                        })

                elif setid == 'R1KEY913':
                    # INPUT @3 ACCT_NO $22.
                    if len(line) >= 24:
                        acct_no = line[2:24].strip()  # Pos 3-24 (22 chars)

                        r1key913_records.append({
                            'ACCT_NO': acct_no
                        })

                elif setid == 'R1R115':
                    # INPUT @3 SET_ID $12. @17 USERCD $4.
                    if len(line) >= 20:
                        set_id = line[2:14].strip()  # Pos 3-14 (12 chars)
                        usercd = line[16:20].strip()  # Pos 17-20 (4 chars)

                        r1r115_records.append({
                            'SET_ID': set_id,
                            'USERCD': usercd
                        })

                elif setid == 'R1R913':
                    # INPUT @3 ACCT_NO $22. @28 SET_ID $12.
                    if len(line) >= 39:
                        acct_no = line[2:24].strip()  # Pos 3-24 (22 chars)
                        set_id = line[27:39].strip()  # Pos 28-39 (12 chars)

                        r1r913_records.append({
                            'ACCT_NO': acct_no,
                            'SET_ID': set_id
                        })

    print(f"Total lines processed: {record_count:,}")
    print(f"Records extracted:")
    print(f"  R1GL4000: {len(r1gl4000_records):,}")
    print(f"  R1GL4100: {len(r1gl4100_records):,}")
    print(f"  R1KEY913: {len(r1key913_records):,}")
    print(f"  R1R115:   {len(r1r115_records):,}")
    print(f"  R1R913:   {len(r1r913_records):,}")

    # Convert to Polars DataFrames
    datasets = {}

    if r1gl4000_records:
        datasets['R1GL4000'] = pl.DataFrame(r1gl4000_records)
    else:
        datasets['R1GL4000'] = pl.DataFrame({
            'ACCT_NO': pl.Series([], dtype=pl.Utf8),
            'ACKIND': pl.Series([], dtype=pl.Utf8),
            'GLAMT': pl.Series([], dtype=pl.Float64)
        })

    if r1gl4100_records:
        datasets['R1GL4100'] = pl.DataFrame(r1gl4100_records)
    else:
        datasets['R1GL4100'] = pl.DataFrame({
            'ACCT_NO': pl.Series([], dtype=pl.Utf8),
            'EFFDATE': pl.Series([], dtype=pl.Date),
            'JEAMT': pl.Series([], dtype=pl.Float64),
            'ACKIND': pl.Series([], dtype=pl.Utf8)
        })

    if r1key913_records:
        datasets['R1KEY913'] = pl.DataFrame(r1key913_records)
    else:
        datasets['R1KEY913'] = pl.DataFrame({
            'ACCT_NO': pl.Series([], dtype=pl.Utf8)
        })

    if r1r115_records:
        datasets['R1R115'] = pl.DataFrame(r1r115_records)
    else:
        datasets['R1R115'] = pl.DataFrame({
            'SET_ID': pl.Series([], dtype=pl.Utf8),
            'USERCD': pl.Series([], dtype=pl.Utf8)
        })

    if r1r913_records:
        datasets['R1R913'] = pl.DataFrame(r1r913_records)
    else:
        datasets['R1R913'] = pl.DataFrame({
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

    # R1GL4000: Sort by ACCT_NO, ACKIND, GLAMT
    if len(datasets['R1GL4000']) > 0:
        df = datasets['R1GL4000'].sort(['ACCT_NO', 'ACKIND', 'GLAMT'])
        df.write_parquet(ALW_R1GL4000)
        print(f"  R1GL4000: {len(df):,} records -> {ALW_R1GL4000}")
    else:
        datasets['R1GL4000'].write_parquet(ALW_R1GL4000)
        print(f"  R1GL4000: 0 records (empty) -> {ALW_R1GL4000}")

    # R1GL4100: Sort by ACCT_NO, EFFDATE, JEAMT, ACKIND
    if len(datasets['R1GL4100']) > 0:
        df = datasets['R1GL4100'].sort(['ACCT_NO', 'EFFDATE', 'JEAMT', 'ACKIND'])
        df.write_parquet(ALW_R1GL4100)
        print(f"  R1GL4100: {len(df):,} records -> {ALW_R1GL4100}")
    else:
        datasets['R1GL4100'].write_parquet(ALW_R1GL4100)
        print(f"  R1GL4100: 0 records (empty) -> {ALW_R1GL4100}")

    # R1KEY913: Sort by ACCT_NO
    if len(datasets['R1KEY913']) > 0:
        df = datasets['R1KEY913'].sort('ACCT_NO')
        df.write_parquet(ALW_R1KEY913)
        print(f"  R1KEY913: {len(df):,} records -> {ALW_R1KEY913}")
    else:
        datasets['R1KEY913'].write_parquet(ALW_R1KEY913)
        print(f"  R1KEY913: 0 records (empty) -> {ALW_R1KEY913}")

    # R1R115: Sort by SET_ID
    if len(datasets['R1R115']) > 0:
        df = datasets['R1R115'].sort('SET_ID')
        df.write_parquet(ALW_R1R115)
        print(f"  R1R115:   {len(df):,} records -> {ALW_R1R115}")
    else:
        datasets['R1R115'].write_parquet(ALW_R1R115)
        print(f"  R1R115:   0 records (empty) -> {ALW_R1R115}")

    # R1R913: Sort by ACCT_NO, SET_ID
    if len(datasets['R1R913']) > 0:
        df = datasets['R1R913'].sort(['ACCT_NO', 'SET_ID'])
        df.write_parquet(ALW_R1R913)
        print(f"  R1R913:   {len(df):,} records -> {ALW_R1R913}")
    else:
        datasets['R1R913'].write_parquet(ALW_R1R913)
        print(f"  R1R913:   0 records (empty) -> {ALW_R1R913}")


# ============================================================================
# CONVERSION PROCESS
# ============================================================================

def convert():
    """
    Main conversion process - parse and split Walker file
    Mimics %CONVERT macro
    """
    print("\n" + "=" * 70)
    print("CONVERTING WALKER FILE")
    print("=" * 70)

    # Parse Walker file
    datasets = parse_walker_file(WALALW_PATH)

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
    print("WALKER DATA EXTRACTION")
    print("=" * 70)

    # Calculate current reporting date (NDATE)
    ndate = calculate_current_reporting_date()
    print(f"\nCurrent reporting date (NDATE): {ndate.strftime('%d%m%Y')}")

    # Extract date from Walker file (RDATE)
    try:
        rdate = extract_walker_date(WALALW_PATH)
        print(f"Walker file date (RDATE):       {rdate.strftime('%d%m%Y')}")

        # Save REPTDATE
        save_reptdate(rdate, ALWZ_REPTDATE, ALW_REPTDATE)

    except Exception as e:
        print(f"ERROR: Could not extract date from Walker file: {e}")
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
    print(f"  REPTDATE:  {ALWZ_REPTDATE}")
    print(f"  REPTDATE:  {ALW_REPTDATE}")
    print(f"  R1GL4000:  {ALW_R1GL4000}")
    print(f"  R1GL4100:  {ALW_R1GL4100}")
    print(f"  R1KEY913:  {ALW_R1KEY913}")
    print(f"  R1R115:    {ALW_R1R115}")
    print(f"  R1R913:    {ALW_R1R913}")
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
