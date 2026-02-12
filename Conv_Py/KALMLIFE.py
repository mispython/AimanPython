#!/usr/bin/env python3
"""
Program: K3TBLFE
Processes K3TBL data to extract and classify transactions by security type and maturity
"""

import polars as pl
import duckdb
from pathlib import Path
from datetime import datetime

# Setup paths
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
BNMK_DIR = INPUT_DIR / "bnmk"

# Create directories if they don't exist
INPUT_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)
BNMK_DIR.mkdir(exist_ok=True)

# Define macro variables (these would typically come from environment or config)
REPTMON = "01"  # Report month
NOWK = "4"  # Week indicator
REPTYEAR = "2025"  # Report year
REPTDATE_STR = "2025-01-15"  # Report date

# Parse report date
REPTDATE = datetime.strptime(REPTDATE_STR, "%Y-%m-%d")

# Define output file path
K3FEI_FILE = OUTPUT_DIR / "K3FEI.parquet"

# Initialize DuckDB connection
conn = duckdb.connect()


def load_parquet(filepath):
    """Load parquet file using DuckDB and return Polars DataFrame"""
    return conn.execute(f"SELECT * FROM '{filepath}'").pl()


def parse_yymmdd_date(date_str):
    """Parse date in YYMMDD format with year cutoff"""
    if date_str is None or date_str.strip() == '':
        return None
    try:
        year = int(date_str[0:2])
        month = int(date_str[2:4])
        day = int(date_str[4:6])

        # Handle year cutoff (YEARCUTOFF=1950)
        if year >= 50:
            year += 1900
        else:
            year += 2000

        return datetime(year, month, day)
    except:
        return None


print("Loading K3TBL dataset...")

# Load K3TBL data
k3tbl = load_parquet(BNMK_DIR / f"K3TBL{REPTMON}{NOWK}.parquet")

print(f"K3TBL records: {len(k3tbl)}")

print("Processing K3TBLFE...")

# Filter and process K3TBL data
k3tblfe_records = []

for row in k3tbl.iter_rows(named=True):
    utctp = row.get("UTCTP", "").strip()

    # Filter by UTCTP
    if utctp not in ['BW', 'BA', 'BE', 'EB', 'CE', 'GA']:
        continue

    utref = row.get("UTREF", "")
    utmdt = row.get("UTMDT", "")
    utosd = row.get("UTOSD", "")
    utamts = row.get("UTAMTS", 0.0)
    utdlp = row.get("UTDLP", "").strip()
    utsty = row.get("UTSTY", "").strip()

    # Calculate ISLM
    isl1 = utref[0:1] if len(utref) >= 1 else ""
    isl2 = utref[3:4] if len(utref) >= 4 else ""
    islm = 'Y' if isl1 == 'I' and isl2.strip() != '' else 'N'

    # Parse dates
    matdt = parse_yymmdd_date(utmdt)
    issdt = parse_yymmdd_date(utosd) if utosd and utosd.strip() != '' else None

    if issdt is None:
        continue

    trmm = issdt.month
    tryy = issdt.year

    # Filter by issue date month and year
    if trmm != int(REPTMON) or tryy != int(REPTYEAR):
        continue

    amount = utamts

    # Calculate REMMTH based on days to maturity
    if matdt:
        days = (matdt - REPTDATE).days
        remmth = '05' if days <= 365 else '06'
    else:
        continue

    # Determine BNMCODE based on UTDLP and UTSTY
    bnmcode = ''

    if utdlp in ['MOP', 'MSP', 'IUP', 'IOP', 'ISP']:
        if utsty in ['SSD', 'SLD', 'SFD', 'SZD']:
            bnmcode = f'687038{remmth}00000Y'
        elif utsty == 'SBA' and remmth == '05':
            bnmcode = '6870480500000Y'
        elif utsty == 'FCF':
            bnmcode = f'687108{remmth}00000Y'
        elif utsty == 'MGS':
            bnmcode = f'687218{remmth}00000Y'
        elif utsty == 'MTB' and remmth == '05':
            bnmcode = '6872280500000Y'
        elif utsty == 'MGI':
            bnmcode = f'687238{remmth}00000Y'
        elif utsty in ['CB1', 'CB2', 'PNB', 'SMC']:
            bnmcode = f'687508{remmth}00000Y'
        elif utsty == 'SAC' and islm == 'N':
            bnmcode = f'687508{remmth}00000Y'
        elif utsty in ['SMC', 'KHA', 'DMB', 'DHB']:
            bnmcode = f'687708{remmth}00000Y'
        elif utsty == 'SAC' and islm == 'Y':
            bnmcode = f'687708{remmth}00000Y'
        elif utsty in ['BNB', 'BNN'] and remmth == '05':
            bnmcode = '6879080500000Y'
        elif utsty in ['BMN', 'BMC', 'BMF'] and remmth == '05':
            bnmcode = '6879080500000Y'

    elif utdlp in ['MOS', 'MSS', 'IUS', 'IOS', 'ISS']:
        if utsty in ['SSD', 'SLD', 'SFD', 'SZD']:
            bnmcode = f'787038{remmth}00000Y'
        elif utsty == 'SBA' and remmth == '05':
            bnmcode = '7870480500000Y'
        elif utsty == 'FCF':
            bnmcode = f'787108{remmth}00000Y'
        elif utsty == 'MGS':
            bnmcode = f'787218{remmth}00000Y'
        elif utsty == 'MTB' and remmth == '05':
            bnmcode = '7872280500000Y'
        elif utsty == 'MGI':
            bnmcode = f'787238{remmth}00000Y'
        elif utsty in ['CB1', 'CB2', 'PNB', 'SMC']:
            bnmcode = f'787508{remmth}00000Y'
        elif utsty == 'SAC' and islm == 'N':
            bnmcode = f'787508{remmth}00000Y'
        elif utsty in ['SMC', 'KHA', 'DMB', 'DHB']:
            bnmcode = f'787708{remmth}00000Y'
        elif utsty == 'SAC' and islm == 'Y':
            bnmcode = f'787708{remmth}00000Y'
        elif utsty in ['BNB', 'BNN'] and remmth == '05':
            bnmcode = '7879080500000Y'
        elif utsty in ['BMN', 'BMC', 'BMF'] and remmth == '05':
            bnmcode = '7879080500000Y'

    # Only output if BNMCODE is assigned
    if bnmcode:
        k3tblfe_records.append({
            'ITCODE': bnmcode,
            'AMOUNT': amount,
            'AMTIND': 'D'
        })

k3tblfe = pl.DataFrame(k3tblfe_records)

print(f"K3TBLFE records: {len(k3tblfe)}")

print("Summarizing K3FEI...")

# Summarize by ITCODE and AMTIND
k3fei = k3tblfe.group_by(['ITCODE', 'AMTIND']).agg([
    pl.col('AMOUNT').sum()
])

print(f"K3FEI records: {len(k3fei)}")

# Save K3FEI
k3fei.write_parquet(K3FEI_FILE)
print(f"Saved {K3FEI_FILE}")

# Close DuckDB connection
conn.close()

print("\nProcessing complete!")
print(f"Total K3FEI records: {len(k3fei)}")
