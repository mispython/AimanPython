#!/usr/bin/env python3
"""
File Name: EIIDNLF1
Overdraft Liquidity Calculation for BNM Reporting
Calculates OD liquidity using 48-week rolling minimum methodology
Converted from SAS to Python
"""

import duckdb
import polars as pl
from datetime import date, timedelta
from pathlib import Path
import calendar


# ============================================================================
# CONFIGURATION AND PATHS
# ============================================================================

# Input paths
LOAN_REPTDATE_PATH = "/data/input/loan_reptdate.parquet"
BNM_NOTE_PATH = "/data/input/bnm_note.parquet"
BNM_BASE_ODCORP_PATH = "/data/input/bnm_base_odcorp.parquet"
BNM_BASE_ODIND_PATH = "/data/input/bnm_base_odind.parquet"
BNM_TABLE_PATH = "/data/input/bnm_table.parquet"

# Output paths
OUTPUT_DIR = "/data/output"
BNM_STORE_ODCORP_PATH = f"{OUTPUT_DIR}/bnm_store_odcorp.parquet"
BNM_STORE_ODIND_PATH = f"{OUTPUT_DIR}/bnm_store_odind.parquet"
BNM_BASE_ODCORP_OUT_PATH = f"{OUTPUT_DIR}/bnm_base_odcorp.parquet"
BNM_BASE_ODIND_OUT_PATH = f"{OUTPUT_DIR}/bnm_base_odind.parquet"
BNM_CALC_PATH = f"{OUTPUT_DIR}/bnm_calc.parquet"

REPORT_ODCORP_48WK_PATH = f"{OUTPUT_DIR}/odcorp_48weeks_report.txt"
REPORT_ODCORP_CUR_PATH = f"{OUTPUT_DIR}/odcorp_current_report.txt"
REPORT_ODCORP_MIN_PATH = f"{OUTPUT_DIR}/odcorp_minimum_report.txt"
REPORT_ODCORP_MATURITY_PATH = f"{OUTPUT_DIR}/odcorp_maturity_profile.txt"
REPORT_ODCORP_DATA_PATH = f"{OUTPUT_DIR}/odcorp_data_report.txt"

REPORT_ODIND_48WK_PATH = f"{OUTPUT_DIR}/odind_48weeks_report.txt"
REPORT_ODIND_CUR_PATH = f"{OUTPUT_DIR}/odind_current_report.txt"
REPORT_ODIND_MIN_PATH = f"{OUTPUT_DIR}/odind_minimum_report.txt"
REPORT_ODIND_MATURITY_PATH = f"{OUTPUT_DIR}/odind_maturity_profile.txt"
REPORT_ODIND_DATA_PATH = f"{OUTPUT_DIR}/odind_data_report.txt"

# Create output directory
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def classify_remmth(remmth):
    """Classify remaining months into BNM categories"""
    if remmth < 0.1:
        return '01'  # UP TO 1 WK
    elif remmth < 1:
        return '02'  # >1 WK - 1 MTH
    elif remmth < 3:
        return '03'  # >1 MTH - 3 MTHS
    elif remmth < 6:
        return '04'  # >3 - 6 MTHS
    elif remmth < 12:
        return '05'  # >6 MTHS - 1 YR
    else:
        return '06'  # > 1 YEAR


def format_remmth_desc(remmth):
    """Format remaining months description"""
    if remmth < 0.255:
        return 'UP TO 1 WK'
    elif remmth < 1:
        return '>1 WK - 1 MTH'
    elif remmth < 3:
        return '>1 MTH - 3 MTHS'
    elif remmth < 6:
        return '>3 - 6 MTHS'
    elif remmth < 12:
        return '>6 MTHS - 1 YR'
    else:
        return '> 1 YEAR'


# ============================================================================
# DATE PROCESSING
# ============================================================================

def get_report_dates():
    """Get report dates and determine if insertion is needed"""
    # Load LOAN.REPTDATE
    reptdate_df = pl.read_parquet(LOAN_REPTDATE_PATH)
    reptdate_row = reptdate_df.row(0, named=True)

    reptdate = reptdate_row['REPTDATE']
    wk = reptdate_row.get('WK', 0)

    if isinstance(reptdate, str):
        from datetime import datetime
        reptdate = datetime.strptime(reptdate, '%Y-%m-%d').date()

    # Get today's date for comparison
    today = date.today()

    # Calculate last day of previous month
    first_of_month = date(today.year, today.month, 1)
    last_of_prev_month = first_of_month - timedelta(days=1)
    last_day = last_of_prev_month.day

    reptday = reptdate.day

    # Determine if we should insert
    # Insert if: reptday in (8, 15, 22) OR (reptday is last day of month AND today's day < 8)
    if reptday in [8, 15, 22]:
        insert = 'Y'
    elif reptday == last_day and today.day < 8:
        insert = 'Y'
    else:
        insert = 'N'

    nowk = str(wk)
    rdate = reptdate.strftime('%d/%m/%y')
    rdat1 = int(reptdate.strftime('%y%m%d'))  # YYMDD as 5-digit number
    reptmon = f"{reptdate.month:02d}"
    reptday_str = f"{reptdate.day:02d}"

    return {
        'reptdate': reptdate,
        'insert': insert,
        'nowk': nowk,
        'rdate': rdate,
        'rdat1': rdat1,
        'reptmon': reptmon,
        'reptday': reptday_str
    }


dates = get_report_dates()
REPTDATE = dates['reptdate']
INSERT = dates['insert']
NOWK = dates['nowk']
RDATE = dates['rdate']
RDAT1 = dates['rdat1']
REPTMON = dates['reptmon']
REPTDAY = dates['reptday']

print(f"Report Date: {RDATE}")
print(f"Insert Flag: {INSERT}")
print(f"Week Number: {NOWK}")
print(f"RDAT1: {RDAT1}")


# ============================================================================
# CALCULATE FUNCTION
# ============================================================================

def calculate_product(prod_name, store_df, table_df):
    """
    Calculate liquidity metrics for a product using 48-week methodology

    Parameters:
    - prod_name: Product name (e.g., 'ODCORP', 'ODIND')
    - store_df: Historical data
    - table_df: BNM table with REMMTH values

    Returns:
    - result_df: DataFrame with BNMCODE and AMOUNT
    - curbal: Current balance
    - minbal: Minimum balance
    """

    print(f"\n  Processing {prod_name}...")

    # Sort by descending REPTDATE
    store_sorted = store_df.sort('REPTDATE', descending=True)

    # Get last 48 weeks
    week48 = store_sorted.head(48)
    print(f"    48-week records: {len(week48)}")

    # Get current value (most recent)
    cur = week48.head(1)
    curbal = cur.select('AMOUNT').item(0, 0)
    print(f"    Current balance: {curbal:,.2f}")

    # Get minimum value
    min_record = week48.sort('AMOUNT').head(1)
    minbal = min_record.select('AMOUNT').item(0, 0)
    print(f"    Minimum balance: {minbal:,.2f}")

    # Calculate amounts for each time bucket
    table_with_amounts = table_df.with_columns([
        pl.when(pl.col('REMMTH') < 12)
        .then((curbal - minbal) / 5)
        .otherwise(minbal)
        .alias('AMOUNT')
    ])

    return table_with_amounts, curbal, minbal, week48, cur, min_record


# ============================================================================
# APPEND FUNCTION
# ============================================================================

def append_to_base(prod_name, newrec_df, base_path, store_path, base_out_path, store_out_path):
    """
    Append new record to base and create store

    Parameters:
    - prod_name: Product name
    - newrec_df: New record to append
    - base_path: Path to base parquet
    - store_path: Path to store parquet (not used for input)
    - base_out_path: Output path for base
    - store_out_path: Output path for store
    """

    print(f"\n  Appending {prod_name} to base...")

    try:
        base_df = pl.read_parquet(base_path)
        print(f"    Loaded base records: {len(base_df)}")
    except:
        # If base doesn't exist, create empty
        base_df = pl.DataFrame({
            'REPTDATE': [],
            'AMOUNT': []
        })
        print(f"    Created new base")

    if INSERT == 'Y':
        print(f"    INSERT = Y: Updating existing record if present")
        # Remove existing record for this date if it exists
        base_df = base_df.filter(pl.col('REPTDATE') != RDAT1)

        # Append new record
        base_df = pl.concat([base_df, newrec_df]).sort('REPTDATE')
    else:
        print(f"    INSERT = N: No permanent update")
        # Don't modify base, just combine for store
        pass

    # Create store (all records up to and including RDAT1)
    if INSERT == 'Y':
        store_df = base_df.filter(pl.col('REPTDATE') <= RDAT1)
    else:
        store_df = pl.concat([base_df, newrec_df]).filter(
            pl.col('REPTDATE') <= RDAT1
        ).sort('REPTDATE')

    print(f"    Store records: {len(store_df)}")

    # Save outputs
    if INSERT == 'Y':
        base_df.write_parquet(base_out_path)
        print(f"    Saved base to: {base_out_path}")

    store_df.write_parquet(store_out_path)
    print(f"    Saved store to: {store_out_path}")

    return store_df


# ============================================================================
# REPORT GENERATION
# ============================================================================

def write_simple_report(filepath, title, data_df):
    """Write a simple data report with ASA carriage control"""
    with open(filepath, 'w') as f:
        # Header
        f.write(f"1{' ' * 80}\n")  # Form feed
        f.write(f" {title.center(80)}\n")
        f.write(f" {'-' * 80}\n")

        # Data
        for row in data_df.iter_rows(named=True):
            line_parts = []
            for col_name, col_val in row.items():
                if isinstance(col_val, float):
                    line_parts.append(f"{col_name}: {col_val:,.2f}")
                else:
                    line_parts.append(f"{col_name}: {col_val}")
            f.write(f" {' | '.join(line_parts)}\n")


def write_maturity_profile_report(filepath, title, table_df):
    """Write maturity profile report (PROC TABULATE style)"""
    with open(filepath, 'w') as f:
        # Header
        f.write(f"1{' ' * 100}\n")  # Form feed
        f.write(f" {title.center(100)}\n")
        f.write(f" {'BREAKDOWN BY PURE CONTRACTUAL MATURITY PROFILE'.center(100)}\n")
        f.write(f" {RDATE.center(100)}\n")
        f.write(f" {'-' * 100}\n")
        f.write(f" {'CORE (NON-TRADING) BANKING ACTIVITIES'.ljust(45)} {'AMOUNT':>20}\n")
        f.write(f" {'-' * 100}\n")

        # Calculate totals by REMMTH category
        table_with_desc = table_df.with_columns([
            pl.col('REMMTH').map_elements(format_remmth_desc, return_dtype=pl.Utf8).alias('REMMTH_DESC')
        ])

        summary = table_with_desc.group_by('REMMTH_DESC').agg([
            pl.col('AMOUNT').sum().alias('TOTAL_AMOUNT')
        ]).sort('REMMTH_DESC')

        # Write detail lines
        for row in summary.iter_rows(named=True):
            desc = row['REMMTH_DESC']
            amount = row['TOTAL_AMOUNT']
            f.write(f" {desc.ljust(45)} {amount:20,.2f}\n")

        # Write grand total
        grand_total = table_df.select(pl.col('AMOUNT').sum()).item(0, 0)
        f.write(f" {'-' * 100}\n")
        f.write(f" {'TOTAL'.ljust(45)} {grand_total:20,.2f}\n")


def write_data_report(filepath, title, data_df):
    """Write detailed data report"""
    with open(filepath, 'w') as f:
        # Header
        f.write(f"1{' ' * 100}\n")  # Form feed
        f.write(f" {title.center(100)}\n")
        f.write(f" {RDATE.center(100)}\n")
        f.write(f" {'-' * 100}\n")
        f.write(f" {'BNMCODE':<20} {'AMOUNT':>20}\n")
        f.write(f" {'-' * 100}\n")

        # Data
        for row in data_df.iter_rows(named=True):
            bnmcode = row['BNMCODE']
            amount = row['AMOUNT']
            f.write(f" {bnmcode:<20} {amount:20,.2f}\n")

        # Total
        total = data_df.select(pl.col('AMOUNT').sum()).item(0, 0)
        f.write(f" {'-' * 100}\n")
        f.write(f" {'TOTAL':<20} {total:20,.2f}\n")


# ============================================================================
# LOAD INPUT DATA
# ============================================================================

print("\nStep 1: Loading input data...")

# Load BNM NOTE data
bnm_note = pl.read_parquet(BNM_NOTE_PATH)

# Extract first 7 characters of BNMCODE
notex = bnm_note.with_columns([
    pl.col('BNMCODE').str.slice(0, 7).alias('BNMCODE1')
])

print(f"  Loaded BNM NOTE records: {len(notex)}")

# Load BNM TABLE
bnm_table = pl.read_parquet(BNM_TABLE_PATH)
print(f"  Loaded BNM TABLE records: {len(bnm_table)}")


# ============================================================================
# PROCESS OVERDRAFT - CORPORATE
# ============================================================================

print("\nStep 2: Processing Overdraft - Corporate (ODCORP)...")

# Summarize for BNMCODE1 = '9521309'
odcorp_summary = notex.filter(pl.col('BNMCODE1') == '9521309').group_by('BNMCODE1').agg([
    pl.col('AMOUNT').sum().alias('AMOUNT')
])

if len(odcorp_summary) == 0:
    print("  No ODCORP records found, creating zero record")
    odcorp_summary = pl.DataFrame({
        'BNMCODE1': ['9521309'],
        'AMOUNT': [0.0]
    })

# Create new record
odcorp_newrec = odcorp_summary.select([
    pl.lit(RDAT1).alias('REPTDATE'),
    pl.col('AMOUNT')
])

print(f"  ODCORP new record amount: {odcorp_newrec.select('AMOUNT').item(0, 0):,.2f}")

# Append to base and create store
odcorp_store = append_to_base(
    'ODCORP',
    odcorp_newrec,
    BNM_BASE_ODCORP_PATH,
    BNM_STORE_ODCORP_PATH,
    BNM_BASE_ODCORP_OUT_PATH,
    BNM_STORE_ODCORP_PATH
)

# Calculate using 48-week methodology
odcorp_table, odcorp_curbal, odcorp_minbal, odcorp_week48, odcorp_cur, odcorp_min = calculate_product(
    'ODCORP',
    odcorp_store,
    bnm_table
)

# Create BNMCODE records
odcorp_final = odcorp_table.with_columns([
    (pl.lit('9321309') +
     pl.col('REMMTH').map_elements(classify_remmth, return_dtype=pl.Utf8) +
     pl.lit('0000Y')).alias('BNMCODE')
]).select(['BNMCODE', 'AMOUNT'])

print(f"  ODCORP final records: {len(odcorp_final)}")

# Generate reports
write_simple_report(
    REPORT_ODCORP_48WK_PATH,
    f"48 WEEKS TABLE - ODCORP {RDATE}",
    odcorp_week48
)

write_simple_report(
    REPORT_ODCORP_CUR_PATH,
    f"CURRENT VALUE - ODCORP {RDATE}",
    odcorp_cur
)

write_simple_report(
    REPORT_ODCORP_MIN_PATH,
    f"MINIMUM VALUE - ODCORP {RDATE}",
    odcorp_min
)

write_maturity_profile_report(
    REPORT_ODCORP_MATURITY_PATH,
    f"ODCORP MATURITY PROFILE",
    odcorp_table
)

write_data_report(
    REPORT_ODCORP_DATA_PATH,
    f"ODCORP - {RDATE}",
    odcorp_final
)


# ============================================================================
# PROCESS OVERDRAFT - INDIVIDUAL
# ============================================================================

print("\nStep 3: Processing Overdraft - Individual (ODIND)...")

# Summarize for BNMCODE1 = '9521308'
odind_summary = notex.filter(pl.col('BNMCODE1') == '9521308').group_by('BNMCODE1').agg([
    pl.col('AMOUNT').sum().alias('AMOUNT')
])

if len(odind_summary) == 0:
    print("  No ODIND records found, creating zero record")
    odind_summary = pl.DataFrame({
        'BNMCODE1': ['9521308'],
        'AMOUNT': [0.0]
    })

# Create new record
odind_newrec = odind_summary.select([
    pl.lit(RDAT1).alias('REPTDATE'),
    pl.col('AMOUNT')
])

print(f"  ODIND new record amount: {odind_newrec.select('AMOUNT').item(0, 0):,.2f}")

# Append to base and create store
odind_store = append_to_base(
    'ODIND',
    odind_newrec,
    BNM_BASE_ODIND_PATH,
    BNM_STORE_ODIND_PATH,
    BNM_BASE_ODIND_OUT_PATH,
    BNM_STORE_ODIND_PATH
)

# Calculate using 48-week methodology
odind_table, odind_curbal, odind_minbal, odind_week48, odind_cur, odind_min = calculate_product(
    'ODIND',
    odind_store,
    bnm_table
)

# Create BNMCODE records
odind_final = odind_table.with_columns([
    (pl.lit('9321308') +
     pl.col('REMMTH').map_elements(classify_remmth, return_dtype=pl.Utf8) +
     pl.lit('0000Y')).alias('BNMCODE')
]).select(['BNMCODE', 'AMOUNT'])

print(f"  ODIND final records: {len(odind_final)}")

# Generate reports
write_simple_report(
    REPORT_ODIND_48WK_PATH,
    f"48 WEEKS TABLE - ODIND {RDATE}",
    odind_week48
)

write_simple_report(
    REPORT_ODIND_CUR_PATH,
    f"CURRENT VALUE - ODIND {RDATE}",
    odind_cur
)

write_simple_report(
    REPORT_ODIND_MIN_PATH,
    f"MINIMUM VALUE - ODIND {RDATE}",
    odind_min
)

write_maturity_profile_report(
    REPORT_ODIND_MATURITY_PATH,
    f"ODIND MATURITY PROFILE",
    odind_table
)

write_data_report(
    REPORT_ODIND_DATA_PATH,
    f"ODIND - {RDATE}",
    odind_final
)


# ============================================================================
# COMBINE RESULTS
# ============================================================================

print("\nStep 4: Combining results...")

bnm_calc = pl.concat([odcorp_final, odind_final])
bnm_calc.write_parquet(BNM_CALC_PATH)

print(f"  BNM CALC records: {len(bnm_calc)}")
print(f"  Saved to: {BNM_CALC_PATH}")


# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 70)
print("Overdraft Liquidity Calculation completed successfully!")
print("=" * 70)

print(f"\nODCORP Summary:")
print(f"  Current Balance: {odcorp_curbal:,.2f}")
print(f"  Minimum Balance: {odcorp_minbal:,.2f}")
print(f"  Spread Amount: {(odcorp_curbal - odcorp_minbal) / 5:,.2f} per bucket (<12 months)")

print(f"\nODIND Summary:")
print(f"  Current Balance: {odind_curbal:,.2f}")
print(f"  Minimum Balance: {odind_minbal:,.2f}")
print(f"  Spread Amount: {(odind_curbal - odind_minbal) / 5:,.2f} per bucket (<12 months)")

print(f"\nOutput files:")
print(f"  - BNM CALC: {BNM_CALC_PATH}")
print(f"  - ODCORP Store: {BNM_STORE_ODCORP_PATH}")
print(f"  - ODIND Store: {BNM_STORE_ODIND_PATH}")
if INSERT == 'Y':
    print(f"  - ODCORP Base: {BNM_BASE_ODCORP_OUT_PATH}")
    print(f"  - ODIND Base: {BNM_BASE_ODIND_OUT_PATH}")
print(f"\nReports generated:")
print(f"  - ODCORP: 5 reports")
print(f"  - ODIND: 5 reports")
