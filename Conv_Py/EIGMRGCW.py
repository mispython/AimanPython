#!/usr/bin/env python3
"""
File Name: EIGMRGCW
RGAC (Global Assets and Capital) Walker Data Extraction
Reads Walker GAY file, validates date, and splits into multiple datasets
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
WALGAY_PATH = INPUT_PATH / "walker" / "WALGAY.txt"

# Output paths - GAY library
GAY_PATH = OUTPUT_PATH / "gay"
GAY_REPTDATE = GAY_PATH / "REPTDATE.parquet"
GAY_RGGL4000 = GAY_PATH / "RGGL4000.parquet"
GAY_RGGL4100 = GAY_PATH / "RGGL4100.parquet"
GAY_RGKEY913 = GAY_PATH / "RGKEY913.parquet"
GAY_RGR115 = GAY_PATH / "RGR115.parquet"
GAY_RGR901 = GAY_PATH / "RGR901.parquet"
GAY_RGR913 = GAY_PATH / "RGR913.parquet"

# Ensure output directories exist
GAY_PATH.mkdir(parents=True, exist_ok=True)


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
    Extract REPTDATE from first line of Walker GAY file
    """
    if not filepath.exists():
        raise FileNotFoundError(f"Walker GAY file not found: {filepath}")

    with open(filepath, 'r') as f:
        first_line = f.readline()

    # Extract WALKDATE from position 2 (8 characters)
    # INPUT @2 WALKDATE $8.
    if len(first_line) < 9:
        raise ValueError(f"Invalid Walker GAY file format: first line too short")

    walkdate = first_line[1:9]  # Position 2-9 (0-indexed: 1-9)

    # Parse DDMMYY8. format (e.g., "31012025")
    try:
        reptdate = datetime.strptime(walkdate, '%d%m%Y')
    except ValueError:
        raise ValueError(f"Could not parse date from Walker GAY file: {walkdate}")

    return reptdate


def save_reptdate(reptdate: datetime, gay_path: Path):
    """
    Save REPTDATE to GAY library
    """
    df = pl.DataFrame({
        'REPTDATE': [reptdate]
    })

    df.write_parquet(gay_path)

    print(f"REPTDATE saved: {reptdate.strftime('%d%m%Y')}")
    print(f"  GAY: {gay_path}")


# ============================================================================
# WALKER GAY DATA PARSING
# ============================================================================

def parse_walker_gay_file(filepath: Path) -> dict:
    """
    Parse Walker GAY file and split into multiple datasets
    Returns dict of DataFrames

    Record types:
    - '1' at position 2: Section header (SETID)
    - '+' at position 2: Data record
    """
    print(f"\nParsing Walker GAY file: {filepath}")

    # Storage for different record types
    rggl4000_records = []
    rggl4100_records = []
    rgkey913_records = []
    rgr115_records = []
    rgr901_records = []
    rgr913_records = []

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

                if setid == 'RGGL4000':
                    # INPUT @3 ACCT_NO $22. @29 ACKIND $1. @31 GLAMT COMMA21.2
                    if len(line) >= 52:
                        acct_no = line[2:24].strip()  # Pos 3-24 (22 chars)
                        ackind = line[28:29]  # Pos 29 (1 char)
                        glamt_str = line[30:52].strip()  # Pos 31-51 (21 chars)

                        # Parse amount (remove commas)
                        try:
                            glamt = float(glamt_str.replace(',', ''))
                        except ValueError:
                            glamt = 0.0

                        rggl4000_records.append({
                            'ACCT_NO': acct_no,
                            'ACKIND': ackind,
                            'GLAMT': glamt
                        })

                elif setid == 'RGGL4100':
                    # INPUT @3 ACCT_NO $22. @30 EFFDATE DDMMYY8. @48 JEAMT COMMA21.2 @75 ACKIND $1.
                    if len(line) >= 75:
                        acct_no = line[2:24].strip()  # Pos 3-24 (22 chars)
                        effdate_str = line[29:37]  # Pos 30-37 (8 chars)
                        jeamt_str = line[47:69].strip()  # Pos 48-68 (21 chars)
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

                        rggl4100_records.append({
                            'ACCT_NO': acct_no,
                            'EFFDATE': effdate,
                            'JEAMT': jeamt,
                            'ACKIND': ackind
                        })

                elif setid == 'RGKEY913':
                    # INPUT @3 ACCT_NO $22.
                    if len(line) >= 24:
                        acct_no = line[2:24].strip()  # Pos 3-24 (22 chars)

                        rgkey913_records.append({
                            'ACCT_NO': acct_no
                        })

                elif setid == 'RGR115':
                    # INPUT @3 SET_ID $12. @17 USERCD $4.
                    if len(line) >= 20:
                        set_id = line[2:14].strip()  # Pos 3-14 (12 chars)
                        usercd = line[16:20].strip()  # Pos 17-20 (4 chars)

                        rgr115_records.append({
                            'SET_ID': set_id,
                            'USERCD': usercd
                        })

                elif setid == 'RGR901':
                    # INPUT @3 SEG_VAL $5. @11 SEG_DESC $40.
                    if len(line) >= 50:
                        seg_val = line[2:7].strip()  # Pos 3-7 (5 chars)
                        seg_desc = line[10:50].strip()  # Pos 11-50 (40 chars)

                        rgr901_records.append({
                            'SEG_VAL': seg_val,
                            'SEG_DESC': seg_desc
                        })

                elif setid == 'RGR913':
                    # INPUT @3 ACCT_NO $22. @28 SET_ID $12.
                    if len(line) >= 39:
                        acct_no = line[2:24].strip()  # Pos 3-24 (22 chars)
                        set_id = line[27:39].strip()  # Pos 28-39 (12 chars)

                        rgr913_records.append({
                            'ACCT_NO': acct_no,
                            'SET_ID': set_id
                        })

    print(f"Total lines processed: {record_count:,}")
    print(f"Records extracted:")
    print(f"  RGGL4000: {len(rggl4000_records):,}")
    print(f"  RGGL4100: {len(rggl4100_records):,}")
    print(f"  RGKEY913: {len(rgkey913_records):,}")
    print(f"  RGR115:   {len(rgr115_records):,}")
    print(f"  RGR901:   {len(rgr901_records):,}")
    print(f"  RGR913:   {len(rgr913_records):,}")

    # Convert to Polars DataFrames
    datasets = {}

    if rggl4000_records:
        datasets['RGGL4000'] = pl.DataFrame(rggl4000_records)
    else:
        datasets['RGGL4000'] = pl.DataFrame({
            'ACCT_NO': pl.Series([], dtype=pl.Utf8),
            'ACKIND': pl.Series([], dtype=pl.Utf8),
            'GLAMT': pl.Series([], dtype=pl.Float64)
        })

    if rggl4100_records:
        datasets['RGGL4100'] = pl.DataFrame(rggl4100_records)
    else:
        datasets['RGGL4100'] = pl.DataFrame({
            'ACCT_NO': pl.Series([], dtype=pl.Utf8),
            'EFFDATE': pl.Series([], dtype=pl.Date),
            'JEAMT': pl.Series([], dtype=pl.Float64),
            'ACKIND': pl.Series([], dtype=pl.Utf8)
        })

    if rgkey913_records:
        datasets['RGKEY913'] = pl.DataFrame(rgkey913_records)
    else:
        datasets['RGKEY913'] = pl.DataFrame({
            'ACCT_NO': pl.Series([], dtype=pl.Utf8)
        })

    if rgr115_records:
        datasets['RGR115'] = pl.DataFrame(rgr115_records)
    else:
        datasets['RGR115'] = pl.DataFrame({
            'SET_ID': pl.Series([], dtype=pl.Utf8),
            'USERCD': pl.Series([], dtype=pl.Utf8)
        })

    if rgr901_records:
        datasets['RGR901'] = pl.DataFrame(rgr901_records)
    else:
        datasets['RGR901'] = pl.DataFrame({
            'SEG_VAL': pl.Series([], dtype=pl.Utf8),
            'SEG_DESC': pl.Series([], dtype=pl.Utf8)
        })

    if rgr913_records:
        datasets['RGR913'] = pl.DataFrame(rgr913_records)
    else:
        datasets['RGR913'] = pl.DataFrame({
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

    # RGGL4000: Sort by ACCT_NO, ACKIND, GLAMT
    if len(datasets['RGGL4000']) > 0:
        df = datasets['RGGL4000'].sort(['ACCT_NO', 'ACKIND', 'GLAMT'])
        df.write_parquet(GAY_RGGL4000)
        print(f"  RGGL4000: {len(df):,} records -> {GAY_RGGL4000}")
    else:
        datasets['RGGL4000'].write_parquet(GAY_RGGL4000)
        print(f"  RGGL4000: 0 records (empty) -> {GAY_RGGL4000}")

    # RGGL4100: Sort by ACCT_NO, EFFDATE, JEAMT, ACKIND
    if len(datasets['RGGL4100']) > 0:
        df = datasets['RGGL4100'].sort(['ACCT_NO', 'EFFDATE', 'JEAMT', 'ACKIND'])
        df.write_parquet(GAY_RGGL4100)
        print(f"  RGGL4100: {len(df):,} records -> {GAY_RGGL4100}")
    else:
        datasets['RGGL4100'].write_parquet(GAY_RGGL4100)
        print(f"  RGGL4100: 0 records (empty) -> {GAY_RGGL4100}")

    # RGR115: Sort by SET_ID, USERCD
    if len(datasets['RGR115']) > 0:
        df = datasets['RGR115'].sort(['SET_ID', 'USERCD'])
        df.write_parquet(GAY_RGR115)
        print(f"  RGR115:   {len(df):,} records -> {GAY_RGR115}")
    else:
        datasets['RGR115'].write_parquet(GAY_RGR115)
        print(f"  RGR115:   0 records (empty) -> {GAY_RGR115}")

    # RGR901: Sort by SEG_VAL, SEG_DESC
    if len(datasets['RGR901']) > 0:
        df = datasets['RGR901'].sort(['SEG_VAL', 'SEG_DESC'])
        df.write_parquet(GAY_RGR901)
        print(f"  RGR901:   {len(df):,} records -> {GAY_RGR901}")
    else:
        datasets['RGR901'].write_parquet(GAY_RGR901)
        print(f"  RGR901:   0 records (empty) -> {GAY_RGR901}")

    # RGKEY913: Sort by ACCT_NO
    if len(datasets['RGKEY913']) > 0:
        df = datasets['RGKEY913'].sort('ACCT_NO')
        df.write_parquet(GAY_RGKEY913)
        print(f"  RGKEY913: {len(df):,} records -> {GAY_RGKEY913}")
    else:
        datasets['RGKEY913'].write_parquet(GAY_RGKEY913)
        print(f"  RGKEY913: 0 records (empty) -> {GAY_RGKEY913}")

    # RGR913: Sort by ACCT_NO, SET_ID
    if len(datasets['RGR913']) > 0:
        df = datasets['RGR913'].sort(['ACCT_NO', 'SET_ID'])
        df.write_parquet(GAY_RGR913)
        print(f"  RGR913:   {len(df):,} records -> {GAY_RGR913}")
    else:
        datasets['RGR913'].write_parquet(GAY_RGR913)
        print(f"  RGR913:   0 records (empty) -> {GAY_RGR913}")


# ============================================================================
# CONVERSION PROCESS
# ============================================================================

def convert():
    """
    Main conversion process - parse and split Walker GAY file
    Mimics %CONVERT macro
    """
    print("\n" + "=" * 70)
    print("CONVERTING WALKER GAY FILE")
    print("=" * 70)

    # Parse Walker GAY file
    datasets = parse_walker_gay_file(WALGAY_PATH)

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
    print("WALKER GAY (RGAC) DATA EXTRACTION")
    print("=" * 70)

    # Calculate current reporting date (NDATE) - last day of previous month
    ndate = calculate_current_reporting_date()
    print(f"\nCurrent reporting date (NDATE): {ndate.strftime('%d%m%Y')}")

    # Extract date from Walker GAY file (RDATE)
    try:
        rdate = extract_walker_date(WALGAY_PATH)
        print(f"Walker GAY file date (RDATE):   {rdate.strftime('%d%m%Y')}")

        # Save REPTDATE
        save_reptdate(rdate, GAY_REPTDATE)

    except Exception as e:
        print(f"ERROR: Could not extract date from Walker GAY file: {e}")
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
    print(f"  REPTDATE:  {GAY_REPTDATE}")
    print(f"  RGGL4000:  {GAY_RGGL4000}")
    print(f"  RGGL4100:  {GAY_RGGL4100}")
    print(f"  RGKEY913:  {GAY_RGKEY913}")
    print(f"  RGR115:    {GAY_RGR115}")
    print(f"  RGR901:    {GAY_RGR901}")
    print(f"  RGR913:    {GAY_RGR913}")
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
