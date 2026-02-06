#!/usr/bin/env python3
"""
File Name: EISMEPBF
New Liquidity Framework for PBIF (SME Only)
Produces breakdown by maturity profile reports
"""

import duckdb
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import calendar


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# INPUT_DIR = Path("/mnt/user-data/uploads")
# OUTPUT_DIR = Path("/mnt/user-data/outputs")

BASE_DIR = Path(__file__).resolve().parent

INPUT_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

# Input files
PBIF_FILE = INPUT_DIR / "sap_pbb_pbif_sasdata_0.parquet"
MECHRG_FILE = INPUT_DIR / "sap_pbb_pbif_mechrg_0.parquet"

# Output file
OUTPUT_FILE = OUTPUT_DIR / "smepbif.txt"


# ============================================================================
# INITIALIZE DUCKDB CONNECTION
# ============================================================================
con = duckdb.connect()


# ============================================================================
# FORMATTING FUNCTIONS AND LOOKUPS
# ============================================================================

def get_remfmt(remmth):
    """Format remaining months into buckets"""
    if remmth < 0.255:
        return 'UP TO 1 WK'
    elif remmth < 1:
        return '>1 WK - 1 MTH'
    elif remmth < 3:
        return '>1 MTH - 3 MTHS'
    elif remmth < 6:
        return '>3 - 6 MTHS'
    elif remmth < 9:
        return '>6 - 9 MTHS'
    elif remmth < 12:
        return '>9 MTHS - 1 YR'
    else:
        return '> 1 YEAR'


ITEM_FORMATS = {
    'A1.01': 'A1.01  LOANS: CORP - FIXED TERM LOANS',
    'A1.02': 'A1.02  LOANS: CORP - REVOLVING LOANS',
    'A1.03': 'A1.03  LOANS: CORP - OVERDRAFTS',
    'A1.04': 'A1.04  LOANS: CORP - OTHERS',
    'A1.05': 'A1.05  LOANS: IND  - HOUSING LOANS',
    'A1.07': 'A1.07  LOANS: IND  - OVERDRAFTS',
    'A1.08': 'A1.08  LOANS: IND  - OTHERS',
    'A1.08A': 'A1.08A LOANS: IND  - REVOLVING LOANS',
    'A1.12': 'A1.12  DEPOSITS: CORP - FIXED',
    'A1.13': 'A1.13  DEPOSITS: CORP - SAVINGS',
    'A1.14': 'A1.14  DEPOSITS: CORP - CURRENT',
    'A1.15': 'A1.15  DEPOSITS: IND  - FIXED',
    'A1.16': 'A1.16  DEPOSITS: IND  - SAVINGS',
    'A1.17': 'A1.17  DEPOSITS: IND  - CURRENT',
    'A1.25': 'A1.25  UNDRAWN OD FACILITIES GIVEN',
    'A1.28': 'A1.28  UNDRAWN PORTION OF OTHER C/F GIVEN',
    'A2.01': 'A2.01  INTERBANK LENDING/DEPOSITS',
    'A2.02': 'A2.02  REVERSE REPO',
    'A2.03': 'A2.03  DEBT SEC: GOVT PP/BNM BILLS/CAG',
    'A2.04': 'A2.04  DECT SEC: FIN INST PAPERS',
    'A2.05': 'A2.05  DEBT SEC: TRADE PAPERS',
    'A2.06': 'A2.06  CORP DEBT: GOVT-GUARANTEED',
    'A2.08': 'A2.08  CORP DEBT: NON-GUARANTEED',
    'A2.09': 'A2.09  FX EXCHG CONTRACTS RECEIVABLE',
    'A2.14': 'A2.14  INTERBANK BORROWINGS/DEPOSITS',
    'A2.15': 'A2.15  INTERBANK REPOS',
    'A2.16': 'A2.16  NON-INTERBANK REPOS',
    'A2.17': 'A2.17  NIDS ISSUED',
    'A2.18': 'A2.18  BAS PAYABLE',
    'A2.19': 'A2.19  FX EXCHG CONTRACTS PAYABLE',
    'B1.12': 'B1.12  DEPOSITS: CORP - FIXED',
    'B1.15': 'B1.15  DEPOSITS: IND  - FIXED',
    'B2.01': 'B2.01  INTERBANK LENDING/DEPOSITS',
    'B2.09': 'B2.09  FX EXCHG CONTRACTS RECEIVABLE',
    'B2.14': 'B2.14  INTERBANK BORROWINGS/DEPOSITS',
    'B2.19': 'B2.19  FX EXCHG CONTRACTS PAYABLE'
}


# ============================================================================
# STEP 1: READ REPTDATE AND SET VARIABLES
# ============================================================================
print("Step 1: Reading report date...")

reptdate_df = pl.read_parquet(PBIF_FILE).select(['REPTDATE']).head(1)
reptdate = reptdate_df['REPTDATE'][0]

# Set macro variables
REPTYR = reptdate.year
REPTMON = reptdate.month
REPTDAY = reptdate.day
RDATX = (reptdate - datetime(1960, 1, 1)).days  # SAS date value
RDATE = reptdate.strftime('%d/%m/%y')

# Determine if this is a quarterly report
reptq = 'Y' if (reptdate + timedelta(days=1)).day == 1 else 'N'

# Determine week number
if REPTDAY == 8:
    NOWK = '1'
elif REPTDAY == 15:
    NOWK = '2'
elif REPTDAY == 22:
    NOWK = '3'
else:
    NOWK = '4'

print(f"Report Date: {reptdate} (RDATX: {RDATX})")
print(f"Quarter End: {reptq}, Week: {NOWK}")


# ============================================================================
# STEP 2: READ PBIF DATA
# ============================================================================
print("Step 2: Reading PBIF data...")

# Note: The actual data reading depends on whether REPTQ='Y' or 'N'
# The program includes RSMEPBIF or RSMMPBIF based on REPTQ
# For this conversion, we'll assume the PBIF data is already prepared
pbif_data = pl.read_parquet(PBIF_FILE)

# Verify required columns exist
required_cols = ['MATDTE', 'CUSTCX', 'BALANCE', 'INLIMIT']
for col in required_cols:
    if col not in pbif_data.columns:
        print(f"Warning: Column {col} not found in PBIF data")


# ============================================================================
# STEP 3: CALCULATE REMAINING MONTHS
# ============================================================================
print("Step 3: Calculating remaining months...")


def calculate_remmth(matdte, rpyr, rpmth, rpday, rdatx):
    """Calculate remaining months to maturity"""
    if matdte is None:
        return 0.0

    # Convert to datetime if needed
    if isinstance(matdte, (int, float)):
        base_date = datetime(1960, 1, 1).date()
        matdte = base_date + timedelta(days=int(matdte))

    # Calculate days difference
    if isinstance(matdte, datetime):
        matdte = matdte.date()

    reptdate = datetime(rpyr, rpmth, rpday).date()
    days = (matdte - reptdate).days

    # If less than 8 days, return 0.1 (UP TO 1 WK)
    if days < 8:
        return 0.1

    # Calculate remaining months
    mdyr = matdte.year
    mdmth = matdte.month
    mdday = matdte.day

    # Get days in report month
    rpdays = calendar.monthrange(rpyr, rpmth)[1]

    # Adjust day if needed
    if mdday > rpdays:
        mdday = rpdays

    # Calculate difference
    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday

    remmth = remy * 12 + remm + remd / rpdays

    return remmth


# Calculate REMMTH and DAYS for each record
pbif_data = pbif_data.with_columns([
    (pl.col('MATDTE') - pl.lit(RDATX)).alias('DAYS')
])

pbif_data = pbif_data.with_columns([
    pl.struct(['MATDTE', 'DAYS']).map_elements(
        lambda x: calculate_remmth(x['MATDTE'], REPTYR, REPTMON, REPTDAY, RDATX),
        return_dtype=pl.Float64
    ).alias('REMMTH')
])


# ============================================================================
# STEP 4: ASSIGN ITEM CODES AND CREATE OUTPUT RECORDS
# ============================================================================
print("Step 4: Assigning item codes...")

# Assign ITEM based on CUSTCX
pbif_data = pbif_data.with_columns([
    pl.when(pl.col('CUSTCX').is_in(['77', '78', '95', '96']))
    .then(pl.lit('A1.08'))
    .otherwise(pl.lit('A1.04')).alias('ITEM'),
    pl.col('BALANCE').alias('AMOUNT')
])

# Create records for Part 1-RM and Part 2-RM
pbif_part1 = pbif_data.with_columns([
    pl.lit('1-RM').alias('PART')
])

pbif_part2 = pbif_data.with_columns([
    pl.lit('2-RM').alias('PART')
])

# Create undrawn portion records for Part 2-RM
pbif_undrawn = pbif_data.with_columns([
    pl.lit('A1.28').alias('ITEM'),
    pl.lit('2-RM').alias('PART'),
    pl.when((pl.col('INLIMIT') - pl.col('BALANCE')) < 0)
    .then(pl.lit(0.0))
    .otherwise(pl.col('INLIMIT') - pl.col('BALANCE')).alias('AMOUNT')
])

# Combine all records
pbif_combined = pl.concat([
    pbif_part1.select(['PART', 'ITEM', 'REMMTH', 'AMOUNT']),
    pbif_part2.select(['PART', 'ITEM', 'REMMTH', 'AMOUNT']),
    pbif_undrawn.select(['PART', 'ITEM', 'REMMTH', 'AMOUNT'])
])


# ============================================================================
# STEP 5: SUMMARIZE BY PART, ITEM, AND REMMTH
# ============================================================================
print("Step 5: Summarizing data...")

pbif_summary = pbif_combined.group_by(['PART', 'ITEM', 'REMMTH']).agg([
    pl.col('AMOUNT').sum().alias('AMOUNT')
])


# ============================================================================
# STEP 6: ASSIGN PERIOD BUCKETS
# ============================================================================
print("Step 6: Assigning period buckets...")

pbif_summary = pbif_summary.with_columns([
    pl.struct(['REMMTH']).map_elements(
        lambda x: get_remfmt(x['REMMTH']),
        return_dtype=pl.Utf8
    ).alias('PERIOD')
])

# Create amount columns based on period
pbif_summary = pbif_summary.with_columns([
    pl.when(pl.col('PERIOD') == 'UP TO 1 WK')
    .then(pl.col('AMOUNT'))
    .otherwise(None).alias('AMTWEEK'),
    pl.when(pl.col('PERIOD') == '>1 WK - 1 MTH')
    .then(pl.col('AMOUNT'))
    .otherwise(None).alias('AMTMONTH'),
    pl.when(pl.col('PERIOD') == '>1 MTH - 3 MTHS')
    .then(pl.col('AMOUNT'))
    .otherwise(None).alias('AMTQUAT'),
    pl.when(pl.col('PERIOD') == '>3 - 6 MTHS')
    .then(pl.col('AMOUNT'))
    .otherwise(None).alias('AMTHALF'),
    pl.when(pl.col('PERIOD') == '>6 - 9 MTHS')
    .then(pl.col('AMOUNT'))
    .otherwise(None).alias('AMTMHF'),
    pl.when(pl.col('PERIOD') == '>9 MTHS - 1 YR')
    .then(pl.col('AMOUNT'))
    .otherwise(None).alias('AMTYEAR'),
    pl.when(pl.col('PERIOD') == '> 1 YEAR')
    .then(pl.col('AMOUNT'))
    .otherwise(None).alias('AMTYEARS')
])


# ============================================================================
# STEP 7: SUMMARIZE BY ITEM FOR PART 1-RM
# ============================================================================
print("Step 7: Creating Part 1-RM summary...")

pbif1 = pbif_summary.filter(
    (pl.col('PART') == '1-RM') &
    (~pl.col('ITEM').is_in(['B1.12', 'B1.15']))
).group_by('ITEM').agg([
    pl.col('AMTWEEK').sum().alias('AMTWEEK'),
    pl.col('AMTMONTH').sum().alias('AMTMONTH'),
    pl.col('AMTQUAT').sum().alias('AMTQUAT'),
    pl.col('AMTHALF').sum().alias('AMTHALF'),
    pl.col('AMTMHF').sum().alias('AMTMHF'),
    pl.col('AMTYEAR').sum().alias('AMTYEAR'),
    pl.col('AMTYEARS').sum().alias('AMTYEARS')
])

# Fill nulls with 0
pbif1 = pbif1.fill_null(0.0)


# ============================================================================
# STEP 8: SUMMARIZE BY ITEM FOR PART 2-RM
# ============================================================================
print("Step 8: Creating Part 2-RM summary...")

pbif2 = pbif_summary.filter(
    (pl.col('PART') == '2-RM') &
    (~pl.col('ITEM').is_in(['B1.12', 'B1.15']))
).group_by('ITEM').agg([
    pl.col('AMTWEEK').sum().alias('AMTWEEK'),
    pl.col('AMTMONTH').sum().alias('AMTMONTH'),
    pl.col('AMTQUAT').sum().alias('AMTQUAT'),
    pl.col('AMTHALF').sum().alias('AMTHALF'),
    pl.col('AMTMHF').sum().alias('AMTMHF'),
    pl.col('AMTYEAR').sum().alias('AMTYEAR'),
    pl.col('AMTYEARS').sum().alias('AMTYEARS')
])

# Fill nulls with 0
pbif2 = pbif2.fill_null(0.0)


# ============================================================================
# STEP 9: WRITE OUTPUT REPORT
# ============================================================================
print("Step 9: Writing output report...")


def format_amount(amt):
    """Format amount with proper precision"""
    if amt is None:
        return '0'
    return f'{amt:.2f}'


# Open output file
with open(OUTPUT_FILE, 'w') as f:
    # Write Part 1-RM Report
    f.write('PUBLIC BANK BERHAD\n')
    f.write(f'NEW LIQUIDITY FRAMEWORK (SME LOAN) AS AT {RDATE}\n')
    f.write('BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE (PART 1-RM)\n')
    f.write('CORE (NON-TRADING) BANKING ACTIVITIES;UP TO 1 WK;>1 WK - 1 MTH;'
            '>1 MTH - 3 MTHS;>3 - 6 MTHS;>6 - 9 MTHS;9 - 12 MTHS;> 1 YEAR;TOTAL\n')

    # Write data rows for Part 1
    for row in pbif1.iter_rows(named=True):
        item = row['ITEM']
        desc = ITEM_FORMATS.get(item, item)

        amtweek = row['AMTWEEK'] or 0.0
        amtmonth = row['AMTMONTH'] or 0.0
        amtquat = row['AMTQUAT'] or 0.0
        amthalf = row['AMTHALF'] or 0.0
        amtmhf = row['AMTMHF'] or 0.0
        amtyear = row['AMTYEAR'] or 0.0
        amtyears = row['AMTYEARS'] or 0.0

        totalamt = amtweek + amtmonth + amtquat + amthalf + amtmhf + amtyear + amtyears

        f.write(f'{desc};{format_amount(amtweek)};{format_amount(amtmonth)};'
                f'{format_amount(amtquat)};{format_amount(amthalf)};'
                f'{format_amount(amtmhf)};{format_amount(amtyear)};'
                f'{format_amount(amtyears)};{format_amount(totalamt)}\n')

    # Blank line between reports
    f.write(' \n')

    # Write Part 2-RM Report
    f.write('PUBLIC BANK BERHAD\n')
    f.write(f'NEW LIQUIDITY FRAMEWORK (SME LOAN) AS AT {RDATE}\n')
    f.write('BREAKDOWN BY PURE CONTRACTUAL MATURITY PROFILE (PART 2-RM)\n')
    f.write('CORE (NON-TRADING) BANKING ACTIVITIES;UP TO 1 WK;>1 WK - 1 MTH;'
            '>1 MTH - 3 MTHS;>3 - 6 MTHS;>6 - 9 MTHS;9 - 12 MTHS;> 1 YEAR;TOTAL\n')

    # Write data rows for Part 2
    for row in pbif2.iter_rows(named=True):
        item = row['ITEM']
        desc = ITEM_FORMATS.get(item, item)

        amtweek = row['AMTWEEK'] or 0.0
        amtmonth = row['AMTMONTH'] or 0.0
        amtquat = row['AMTQUAT'] or 0.0
        amthalf = row['AMTHALF'] or 0.0
        amtmhf = row['AMTMHF'] or 0.0
        amtyear = row['AMTYEAR'] or 0.0
        amtyears = row['AMTYEARS'] or 0.0

        totalamt = amtweek + amtmonth + amtquat + amthalf + amtmhf + amtyear + amtyears

        f.write(f'{desc};{format_amount(amtweek)};{format_amount(amtmonth)};'
                f'{format_amount(amtquat)};{format_amount(amthalf)};'
                f'{format_amount(amtmhf)};{format_amount(amtyear)};'
                f'{format_amount(amtyears)};{format_amount(totalamt)}\n')

print(f"Output written to: {OUTPUT_FILE}")
print(f"Part 1-RM records: {len(pbif1)}")
print(f"Part 2-RM records: {len(pbif2)}")
print("\nProcessing complete!")

# Close DuckDB connection
con.close()
