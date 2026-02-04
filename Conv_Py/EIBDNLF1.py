#!/usr/bin/env python3
"""
File Name: EIBDNLF1
Behavioral Run-Offs Calculator
Calculates behavioral run-offs for OD, PBCARD, and SMC products using 48-week methodology
"""

import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import sys
import calendar


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# INPUT_DIR = Path("/path/to/input")
# OUTPUT_DIR = Path("/path/to/output")
# BASE_DIR = Path("/path/to/base")

BASE_DIR = Path(__file__).resolve().parent

INPUT_DIR = BASE_DIR / "data" / "input"
OUTPUT_DIR = BASE_DIR / "data" / "output"


# Input files
REPTDATE_PATH = INPUT_DIR / "REPTDATE.parquet"
WALK_PATH = INPUT_DIR / "WALK.txt"
NOTE_PATH = INPUT_DIR / "NOTE.parquet"
LOAN_PATH_TEMPLATE = INPUT_DIR / "LOAN{}{}.parquet"
TABLE_PATH = INPUT_DIR / "TABLE.parquet"

# Base files (persistent storage)
BASE_PATH_TEMPLATE = BASE_DIR / "BASE_{}.parquet"
STORE_PATH_TEMPLATE = BASE_DIR / "STORE_{}.parquet"

# Output files
CALC_OUTPUT_PATH = OUTPUT_DIR / "CALC.parquet"
REPORT_PATH_TEMPLATE = OUTPUT_DIR / "{}_REPORT.txt"

# Ensure directories exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
BASE_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# FORMAT DEFINITIONS
# ============================================================================

def get_rem_format(remmth):
    """
    Convert remaining months to maturity bucket code.

    LOW-0.1 = '01'   # UP TO 1 WK
    0.1-1   = '02'   # >1 WK - 1 MTH
    1-3     = '03'   # >1 MTH - 3 MTHS
    3-6     = '04'   # >3 - 6 MTHS
    6-12    = '05'   # >6 MTHS - 1 YR
    OTHER   = '06'   # > 1 YEAR
    """
    if remmth <= 0.1:
        return '01'
    elif remmth <= 1:
        return '02'
    elif remmth <= 3:
        return '03'
    elif remmth <= 6:
        return '04'
    elif remmth <= 12:
        return '05'
    else:
        return '06'


def get_rem_format_label(remmth):
    """Get descriptive label for maturity bucket."""
    if remmth <= 0.255:
        return 'UP TO 1 WK'
    elif remmth <= 1:
        return '>1 WK - 1 MTH'
    elif remmth <= 3:
        return '>1 MTH - 3 MTHS'
    elif remmth <= 6:
        return '>3 - 6 MTHS'
    elif remmth <= 12:
        return '>6 MTHS - 1 YR'
    else:
        return '> 1 YEAR'


# ============================================================================
# READ REPTDATE AND CALCULATE PARAMETERS
# ============================================================================

def assert_file_exists(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

def read_reptdate():
    """Read report date and calculate parameters."""
    print("Reading REPTDATE...")

    assert_file_exists(REPTDATE_PATH)

    df = pl.read_parquet(REPTDATE_PATH)
    reptdate = df['REPTDATE'][0]

    if isinstance(reptdate, str):
        reptdate = datetime.strptime(reptdate, "%Y-%m-%d").date()

    # Calculate week number
    day = reptdate.day
    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'

    # Calculate last day of current month
    today = datetime.now().date()
    first_of_month = datetime(today.year, today.month, 1).date()
    last_day = (first_of_month - timedelta(days=1)).day
    last_day_str = f"{last_day:02d}"

    reptday = f"{reptdate.day:02d}"

    # Determine if this should be inserted into base
    insert = 'N'
    if reptday in ('08', '15', '22'):
        insert = 'Y'
    elif reptday == last_day_str and today.day < 8:
        insert = 'Y'

    reptmon = f"{reptdate.month:02d}"
    rdate = reptdate.strftime("%d%m%Y")
    rdat1 = int(reptdate.strftime("%y%m%d"))

    print(f"Report Date: {reptdate} (Week {nowk})")
    print(f"Insert into base: {insert}")
    print(f"RDAT1: {rdat1}")

    return {
        'reptdate': reptdate,
        'nowk': nowk,
        'reptmon': reptmon,
        'reptday': reptday,
        'rdate': rdate,
        'rdat1': rdat1,
        'insert': insert,
    }


# ============================================================================
# 48-WEEK CALCULATION LOGIC
# ============================================================================

def calculate_product(prod_name, newrec_df, table_df, params):
    """
    Calculate behavioral run-off using 48-week methodology.

    Args:
        prod_name: Product name (CARD, ODCORP, ODIND, SMCCORP, SMCIND)
        newrec_df: New record to potentially add
        table_df: Table with REMMTH and BASKET
        params: Report parameters

    Returns:
        DataFrame with BNMCODE and AMOUNT
    """
    print(f"\nCalculating {prod_name}...")

    store_path = Path(str(STORE_PATH_TEMPLATE).format(prod_name))
    base_path = Path(str(BASE_PATH_TEMPLATE).format(prod_name))

    # Update base and store if INSERT = 'Y'
    if params['insert'] == 'Y':
        print(f"Updating base for {prod_name}")

        # Read existing base
        if base_path.exists():
            df_base = pl.read_parquet(base_path)
            # Delete existing record with same REPTDATE
            df_base = df_base.filter(pl.col('REPTDATE') != params['rdat1'])
        else:
            df_base = pl.DataFrame({'REPTDATE': [], 'AMOUNT': []})

        # Append new record
        df_base = pl.concat([df_base, newrec_df])
        df_base = df_base.sort('REPTDATE')

        # Write back to base
        df_base.write_parquet(base_path)

        # Create store (all records <= RDAT1)
        df_store = df_base.filter(pl.col('REPTDATE') <= params['rdat1'])
        df_store.write_parquet(store_path)
    else:
        print(f"Not updating base for {prod_name}, using existing + new")

        # Read existing base
        if base_path.exists():
            df_base = pl.read_parquet(base_path)
        else:
            df_base = pl.DataFrame({'REPTDATE': [], 'AMOUNT': []})

        # Combine with new record and filter
        df_store = pl.concat([df_base, newrec_df])
        df_store = df_store.filter(pl.col('REPTDATE') <= params['rdat1'])
        df_store = df_store.sort('REPTDATE')

        # Write store
        df_store.write_parquet(store_path)

    # Get 48 most recent weeks
    df_week48 = df_store.sort('REPTDATE', descending=True).head(48)

    print(f"48-week records: {len(df_week48)}")

    # Get current value (most recent)
    if len(df_week48) > 0:
        curbal = df_week48['AMOUNT'][0]
        minbal = df_week48['AMOUNT'].min()
    else:
        curbal = 0.0
        minbal = 0.0

    print(f"Current balance: {curbal:,.2f}")
    print(f"Minimum balance: {minbal:,.2f}")

    # Calculate amounts for each maturity bucket
    df_table = table_df.with_columns([
        pl.lit(curbal).alias('CURBAL'),
        pl.lit(minbal).alias('MINBAL'),
    ])

    # Apply calculation logic
    df_table = df_table.with_columns(
        pl.when(pl.col('REMMTH') < 12)
        .then((pl.col('CURBAL') - pl.col('MINBAL')) / 5)
        .otherwise(pl.col('MINBAL'))
        .alias('AMOUNT')
    )

    # Generate tabulation report
    generate_tabulation_report(prod_name, df_table, params['rdate'])

    return df_table


def generate_tabulation_report(prod_name, df_table, rdate):
    """Generate tabulation report for a product."""
    report_path = Path(str(REPORT_PATH_TEMPLATE).format(prod_name))

    with open(report_path, 'w') as f:
        # Header with ASA carriage control
        f.write(' ' + '=' * 80 + '\n')
        f.write(f' 48 WEEKS TABLE - {prod_name} {rdate}\n')
        f.write(' ' + '=' * 80 + '\n')
        f.write(' \n')
        f.write(' BREAKDOWN BY PURE CONTRACTUAL MATURITY PROFILE\n')
        f.write(' CORE (NON-TRADING) BANKING ACTIVITIES\n')
        f.write(' ' + '-' * 80 + '\n')
        f.write(f' {"MATURITY BUCKET":<25} {"AMOUNT":>20}\n')
        f.write(' ' + '-' * 80 + '\n')

        # Group by REMMTH and sum
        df_summary = df_table.group_by('REMMTH').agg([
            pl.col('AMOUNT').sum()
        ]).sort('REMMTH')

        total = 0.0
        for row in df_summary.iter_rows(named=True):
            remmth = row['REMMTH']
            amount = row['AMOUNT']
            label = get_rem_format_label(remmth)
            total += amount
            f.write(f' {label:<25} {amount:>20,.2f}\n')

        # Total
        f.write(' ' + '-' * 80 + '\n')
        f.write(f' {"TOTAL":<25} {total:>20,.2f}\n')
        f.write(' ' + '=' * 80 + '\n')


# ============================================================================
# PROCESS PBCARD (Credit Cards)
# ============================================================================

def process_pbcard(params, table_df):
    """Process PBCARD (credit card) data."""
    print("\n" + "=" * 80)
    print("Processing PBCARD")
    print("=" * 80)

    # Read WALK file to get credit card balance
    cardamt = 0.0

    with open(WALK_PATH, 'r') as f:
        for line in f:
            if len(line) >= 42:
                desc = line[1:20].strip()
                dd = int(line[20:22]) if line[20:22].strip() else 0
                mm = int(line[23:25]) if line[23:25].strip() else 0
                yy = int(line[26:28]) if line[26:28].strip() else 0
                amount_str = line[41:61].strip()

                try:
                    amount = float(amount_str.replace(',', ''))
                except:
                    amount = 0.0

                if desc == '34200':
                    # Construct date
                    year = 2000 + yy if yy < 50 else 1900 + yy
                    reptdate_val = datetime(year, mm, dd).date()
                    reptdate_int = int(reptdate_val.strftime("%y%m%d"))

                    if reptdate_int == params['rdat1']:
                        cardamt = amount
                        break

    print(f"Card amount from WALK: {cardamt:,.2f}")

    # Create new record
    newrec_df = pl.DataFrame({
        'REPTDATE': [params['rdat1']],
        'AMOUNT': [cardamt]
    })

    # Calculate using 48-week methodology
    df_table = calculate_product('CARD', newrec_df, table_df, params)

    # Create PBCARD output (NPL - prefix 93)
    df_pbcard = df_table.with_columns(
        (pl.lit('9321508') +
         pl.col('REMMTH').map_elements(lambda x: get_rem_format(x), return_dtype=pl.Utf8) +
         pl.lit('0000Y')).alias('BNMCODE')
    ).select(['BNMCODE', 'AMOUNT'])

    # Create PBCARD2 output (Regular - prefix 95) with basket distribution
    pbcard2_records = []

    basket_pcts = {
        1: 0.15,
        2: 0.69,
        3: 0.01,
        4: 0.03,
        5: 0.07,
        6: 0.05,
    }

    for row in table_df.iter_rows(named=True):
        basket = row.get('BASKET')
        remmth = row['REMMTH']

        if basket in basket_pcts:
            bnmcode = f"9521508{get_rem_format(remmth)}0000Y"
            amount = cardamt * basket_pcts[basket]
            pbcard2_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

    df_pbcard2 = pl.DataFrame(pbcard2_records) if pbcard2_records else pl.DataFrame({'BNMCODE': [], 'AMOUNT': []})

    print(f"PBCARD records: {len(df_pbcard)}")
    print(f"PBCARD2 records: {len(df_pbcard2)}")

    return df_pbcard, df_pbcard2


# ============================================================================
# PROCESS OVERDRAFT (OD)
# ============================================================================

def process_overdraft(params, table_df, note_df):
    """Process overdraft data for corporate and individual."""
    print("\n" + "=" * 80)
    print("Processing Overdraft")
    print("=" * 80)

    # Add BNMCODE1 (first 7 characters)
    note_df = note_df.with_columns(
        pl.col('BNMCODE').str.slice(0, 7).alias('BNMCODE1')
    )

    # Process ODCORP (Corporate OD)
    df_odcorp_sum = note_df.filter(
        pl.col('BNMCODE1') == '9521309'
    ).group_by('BNMCODE1').agg([
        pl.col('AMOUNT').sum()
    ])

    if len(df_odcorp_sum) > 0:
        odcorp_amount = df_odcorp_sum['AMOUNT'][0]
    else:
        odcorp_amount = 0.0

    newrec_odcorp = pl.DataFrame({
        'REPTDATE': [params['rdat1']],
        'AMOUNT': [odcorp_amount]
    })

    df_table_odcorp = calculate_product('ODCORP', newrec_odcorp, table_df, params)

    df_odcorp = df_table_odcorp.with_columns(
        (pl.lit('9321309') +
         pl.col('REMMTH').map_elements(lambda x: get_rem_format(x), return_dtype=pl.Utf8) +
         pl.lit('0000Y')).alias('BNMCODE')
    ).select(['BNMCODE', 'AMOUNT'])

    # Process ODIND (Individual OD)
    df_odind_sum = note_df.filter(
        pl.col('BNMCODE1') == '9521308'
    ).group_by('BNMCODE1').agg([
        pl.col('AMOUNT').sum()
    ])

    if len(df_odind_sum) > 0:
        odind_amount = df_odind_sum['AMOUNT'][0]
    else:
        odind_amount = 0.0

    newrec_odind = pl.DataFrame({
        'REPTDATE': [params['rdat1']],
        'AMOUNT': [odind_amount]
    })

    df_table_odind = calculate_product('ODIND', newrec_odind, table_df, params)

    df_odind = df_table_odind.with_columns(
        (pl.lit('9321308') +
         pl.col('REMMTH').map_elements(lambda x: get_rem_format(x), return_dtype=pl.Utf8) +
         pl.lit('0000Y')).alias('BNMCODE')
    ).select(['BNMCODE', 'AMOUNT'])

    print(f"ODCORP amount: {odcorp_amount:,.2f}")
    print(f"ODIND amount: {odind_amount:,.2f}")

    return df_odcorp, df_odind


# ============================================================================
# PROCESS SMC (Small and Medium Credit)
# ============================================================================

def process_smc(params, table_df):
    """Process SMC data for corporate and individual."""
    print("\n" + "=" * 80)
    print("Processing SMC")
    print("=" * 80)

    # Read loan data
    loan_path = Path(str(LOAN_PATH_TEMPLATE).format(params['reptmon'], params['reptday']))
    df_loan = pl.read_parquet(loan_path)

    # Filter loans
    df_loan = df_loan.filter(
        ~pl.col('PAIDIND').is_in(['P', 'C'])
    )

    df_loan = df_loan.filter(
        (pl.col('PRODCD').str.slice(0, 2) == '34') |
        (pl.col('PRODUCT').is_in([225, 226]))
    )

    # Process only OD accounts
    df_loan = df_loan.filter(pl.col('ACCTYPE') == 'OD')

    # Exclude specific products
    df_loan = df_loan.filter(
        ~pl.col('PRODUCT').is_in([151, 152, 181])
    )

    # Determine CUST
    df_loan = df_loan.with_columns(
        pl.when(pl.col('CUSTCD').is_in(['77', '78', '95', '96']))
        .then(pl.lit('08'))
        .otherwise(pl.lit('09'))
        .alias('CUST')
    )

    # Set REMMTH and create BNMCODE
    df_loan = df_loan.with_columns([
        pl.lit(0.1).alias('REMMTH'),
        pl.col('BALANCE').alias('AMOUNT'),
    ])

    df_loan = df_loan.with_columns(
        pl.when(pl.col('PRODCD') == '34240')
        .then(
            pl.lit('95219') + pl.col('CUST') +
            pl.col('REMMTH').map_elements(lambda x: get_rem_format(x), return_dtype=pl.Utf8) +
            pl.lit('0000Y')
        )
        .otherwise(None)
        .alias('BNMCODE')
    )

    df_loan = df_loan.filter(pl.col('BNMCODE').is_not_null())

    # Process SMCCORP
    df_smccorp_sum = df_loan.filter(
        pl.col('BNMCODE').str.slice(0, 7) == '9521909'
    ).group_by('BNMCODE').agg([
        pl.col('AMOUNT').sum()
    ]).group_by(pl.lit('TOTAL')).agg([
        pl.col('AMOUNT').sum()
    ])

    if len(df_smccorp_sum) > 0:
        smccorp_amount = df_smccorp_sum['AMOUNT'][0]
    else:
        smccorp_amount = 0.0

    newrec_smccorp = pl.DataFrame({
        'REPTDATE': [params['rdat1']],
        'AMOUNT': [smccorp_amount]
    })

    df_table_smccorp = calculate_product('SMCCORP', newrec_smccorp, table_df, params)

    df_smccorp = df_table_smccorp.with_columns(
        (pl.lit('9321909') +
         pl.col('REMMTH').map_elements(lambda x: get_rem_format(x), return_dtype=pl.Utf8) +
         pl.lit('0000Y')).alias('BNMCODE')
    ).select(['BNMCODE', 'AMOUNT'])

    # Process SMCIND
    df_smcind_sum = df_loan.filter(
        pl.col('BNMCODE').str.slice(0, 7) == '9521908'
    ).group_by('BNMCODE').agg([
        pl.col('AMOUNT').sum()
    ]).group_by(pl.lit('TOTAL')).agg([
        pl.col('AMOUNT').sum()
    ])

    if len(df_smcind_sum) > 0:
        smcind_amount = df_smcind_sum['AMOUNT'][0]
    else:
        smcind_amount = 0.0

    newrec_smcind = pl.DataFrame({
        'REPTDATE': [params['rdat1']],
        'AMOUNT': [smcind_amount]
    })

    df_table_smcind = calculate_product('SMCIND', newrec_smcind, table_df, params)

    df_smcind = df_table_smcind.with_columns(
        (pl.lit('9321908') +
         pl.col('REMMTH').map_elements(lambda x: get_rem_format(x), return_dtype=pl.Utf8) +
         pl.lit('0000Y')).alias('BNMCODE')
    ).select(['BNMCODE', 'AMOUNT'])

    print(f"SMCCORP amount: {smccorp_amount:,.2f}")
    print(f"SMCIND amount: {smcind_amount:,.2f}")

    return df_smccorp, df_smcind


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function."""
    try:
        print("=" * 80)
        print("Behavioral Run-Offs Calculator")
        print("=" * 80)

        # Read reptdate
        params = read_reptdate()

        # Read table
        table_df = pl.read_parquet(TABLE_PATH)
        print(f"\nTable records: {len(table_df)}")

        # Read NOTE data
        note_df = pl.read_parquet(NOTE_PATH)
        print(f"NOTE records: {len(note_df)}")

        # Process PBCARD
        df_pbcard, df_pbcard2 = process_pbcard(params, table_df)

        # Process Overdraft
        df_odcorp, df_odind = process_overdraft(params, table_df, note_df)

        # Process SMC
        df_smccorp, df_smcind = process_smc(params, table_df)

        # Combine all outputs
        df_calc = pl.concat([
            df_pbcard,
            df_pbcard2,
            df_odcorp,
            df_odind,
            df_smccorp,
            df_smcind,
        ])

        # Write output
        df_calc.write_parquet(CALC_OUTPUT_PATH)
        print(f"\n" + "=" * 80)
        print(f"Output written to {CALC_OUTPUT_PATH}")
        print(f"Total records: {len(df_calc)}")
        print("=" * 80)

        return 0

    except Exception as e:
        print(f"\nERROR: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
