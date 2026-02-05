#!/usr/bin/env python3
"""
File Name: EIBMNL41
BNM Maturity Profile Calculation for Various Products
Handles PBCARD, Overdraft (Corporate/Individual), and SMC (Corporate/Individual)
"""

import duckdb
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import sys


# ==============================================================================
# PATH CONFIGURATION
# ==============================================================================
INPUT_DIR = Path("./input")
OUTPUT_DIR = Path("./output")
LOAN_DIR = INPUT_DIR / "loan"
BNM_DIR = INPUT_DIR / "bnm"
BNM1_DIR = INPUT_DIR / "bnm1"
WALK_FILE = INPUT_DIR / "walk.txt"

# Create output directory if it doesn't exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ==============================================================================
# CONSTANTS AND CONFIGURATIONS
# ==============================================================================

# REMFMT format mapping
def remfmt_format(value):
    """Convert remaining months to BNM format code"""
    if value <= 0.1:
        return '01'  # UP TO 1 WK
    elif value <= 1:
        return '02'  # >1 WK - 1 MTH
    elif value <= 3:
        return '03'  # >1 MTH - 3 MTHS
    elif value <= 6:
        return '04'  # >3 - 6 MTHS
    elif value <= 12:
        return '05'  # >6 MTHS - 1 YR
    else:
        return '06'  # > 1 YEAR


# REMFMTB format mapping (for display)
def remfmtb_format(value):
    """Convert remaining months to display format"""
    if value <= 0.255:
        return 'UP TO 1 WK'
    elif value <= 1:
        return '>1 WK - 1 MTH'
    elif value <= 3:
        return '>1 MTH - 3 MTHS'
    elif value <= 6:
        return '>3 - 6 MTHS'
    elif value <= 12:
        return '>6 MTHS - 1 YR'
    else:
        return '> 1 YEAR'


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def get_last_day_of_month(year, month):
    """Get the last day of a given month"""
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    last_day = next_month - timedelta(days=1)
    return last_day.day


def get_reptdate():
    """Get reporting date and calculate parameters"""
    # Read REPTDATE from parquet file
    reptdate_path = BNM_DIR / "reptdate.parquet"

    conn = duckdb.connect()
    reptdate_df = conn.execute(f"""
        SELECT * FROM read_parquet('{reptdate_path}')
    """).pl()

    reptdate = reptdate_df['reptdate'][0]

    # Calculate week number based on day
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
    today = datetime.now()
    last_day_of_month = get_last_day_of_month(today.year, today.month)

    reptday = f"{reptdate.day:02d}"

    # Determine if this is an insert week
    if reptday in ['08', '15', '22']:
        insert = 'Y'
    elif reptday == f"{last_day_of_month:02d}" and today.day < 8:
        insert = 'Y'
    else:
        insert = 'N'

    rdate = reptdate.strftime('%d%m%Y')
    rdat1 = int(reptdate.strftime('%y%j'))  # Julian date format (YYDDD)
    reptmon = f"{reptdate.month:02d}"

    return {
        'reptdate': reptdate,
        'nowk': nowk,
        'insert': insert,
        'rdate': rdate,
        'rdat1': rdat1,
        'reptmon': reptmon,
        'reptday': reptday
    }


def get_store_data(prod, base_dir=BNM_DIR):
    """Get historical store data for a product"""
    store_path = base_dir / f"store_{prod}.parquet"

    if not store_path.exists():
        # Return empty DataFrame if no historical data
        return pl.DataFrame({
            'reptdate': [],
            'amount': []
        })

    conn = duckdb.connect()
    store_df = conn.execute(f"""
        SELECT * FROM read_parquet('{store_path}')
        ORDER BY reptdate DESC
    """).pl()

    return store_df


def calculate_maturity_profile(store_df, table_df, curbal):
    """
    Calculate maturity profile using min/max of last 48 weeks

    Logic:
    - Get current balance (most recent week)
    - Get minimum balance from last 48 weeks
    - For buckets < 12 months: amount = (curbal - minbal) / 5
    - For buckets >= 12 months: amount = minbal
    """
    # Get last 48 weeks
    week48 = store_df.head(48)

    # Get current balance (most recent)
    if len(week48) > 0:
        curbal_from_store = week48['amount'][0]
    else:
        curbal_from_store = curbal

    # Get minimum balance
    if len(week48) > 0:
        minbal = week48['amount'].min()
    else:
        minbal = curbal

    # Calculate amounts for each maturity bucket
    result = table_df.with_columns([
        pl.col('remmth'),
        pl.when(pl.col('remmth') < 12)
        .then((curbal_from_store - minbal) / 5)
        .otherwise(minbal)
        .alias('amount')
    ])

    return result, curbal_from_store, minbal


def append_to_base(prod, newrec_df, rdat1, insert, base_dir=BNM_DIR):
    """
    Append new record to base table

    Logic:
    - If INSERT = 'Y': Update base by removing existing record for rdat1, then append
    - If INSERT = 'N': Just combine base and new record
    """
    # Ensure schema consistency
    newrec_df = newrec_df.with_columns(
        pl.col("amount").cast(pl.Float64)
    )

    base_path = base_dir / f"base_{prod}.parquet"
    store_path = base_dir / f"store_{prod}.parquet"

    # Read existing base if it exists
    if base_path.exists():
        conn = duckdb.connect()
        base_df = conn.execute(f"""
            SELECT * FROM read_parquet('{base_path}')
        """).pl()

        # Ensure base_df also Float64 (safety)
        base_df = base_df.with_columns(
            pl.col("amount").cast(pl.Float64)
        )
    else:
        # base_df = pl.DataFrame({
        #     'reptdate': [],
        #     'amount': []
        # })

        base_df = pl.DataFrame({
                "reptdate": pl.Series([], dtype=pl.Int64),
                "amount": pl.Series([], dtype=pl.Float64),
        })

    if insert == 'Y':
        # Remove existing record for this reptdate
        base_df = base_df.filter(pl.col('reptdate') != rdat1)
        # Append new record
        updated_base = pl.concat([base_df, newrec_df])
        updated_base = updated_base.sort('reptdate')
        # Save updated base
        updated_base.write_parquet(base_path)
        # Create store (all records <= rdat1)
        store_df = updated_base.filter(pl.col('reptdate') <= rdat1)
    else:
        # Combine base and new record
        store_df = pl.concat([base_df, newrec_df])
        store_df = store_df.filter(pl.col('reptdate') <= rdat1)
        store_df = store_df.sort('reptdate')

    # Save store
    store_df.write_parquet(store_path)

    return store_df


def print_tabulation(table_df, title):
    """Print tabulation report similar to PROC TABULATE"""
    print(f"\n{'=' * 80}")
    print(f"{title}")
    print(f"{'=' * 80}")
    print(f"BREAKDOWN BY PURE CONTRACTUAL MATURITY PROFILE")
    print(f"{'-' * 80}")
    print(f"{'CORE (NON-TRADING) BANKING ACTIVITIES':<45} {'AMOUNT':>20}")
    print(f"{'-' * 80}")

    # Group by maturity bucket
    summary = table_df.with_columns(
        pl.col('remmth').map_elements(remfmtb_format, return_dtype=pl.Utf8).alias('bucket')
    ).group_by('bucket').agg(
        pl.col('amount').sum().alias('total')
    )

    # Define order
    bucket_order = [
        'UP TO 1 WK',
        '>1 WK - 1 MTH',
        '>1 MTH - 3 MTHS',
        '>3 - 6 MTHS',
        '>6 MTHS - 1 YR',
        '> 1 YEAR'
    ]

    total = 0
    for bucket in bucket_order:
        row = summary.filter(pl.col('bucket') == bucket)
        if len(row) > 0:
            amount = row['total'][0]
            total += amount
            print(f"{bucket:<45} {amount:>20,.2f}")

    print(f"{'-' * 80}")
    print(f"{'TOTAL':<45} {total:>20,.2f}")
    print(f"{'=' * 80}\n")


# ==============================================================================
# MAIN PROCESSING FUNCTIONS
# ==============================================================================

def process_pbcard(params, table_df):
    """Process PBCARD (Credit Card) data"""
    print("\nProcessing PBCARD...")

    # Read WALK file (fixed-width format)
    # @2 DESC $19., @21 DD 2., @24 MM 2., @27 YY 2., @42 AMOUNT COMMA20.2
    walk_path = WALK_FILE

    if not walk_path.exists():
        print(f"Warning: WALK file not found at {walk_path}")
        return None, None, 0

    # Read walk file
    with open(walk_path, 'r') as f:
        lines = f.readlines()

    # Parse relevant lines (DESC = '34200')
    cardamt = 0
    newrec_data = []

    for line in lines:
        if len(line) < 50:
            continue

        desc = line[1:20].strip()
        if desc == '34200':
            # dd = int(line[20:22])
            # mm = int(line[23:25])
            # yy = int(line[26:28])

            # dd_str = line[20:22].strip()
            # mm_str = line[23:25].strip()
            # yy_str = line[26:28].strip()
            #
            # # Skip invalid / blank date rows (SAS would treat as missing)
            # if not dd_str or not mm_str or not yy_str:
            #     continue
            #
            # dd = int(dd_str)
            # mm = int(mm_str)
            # yy = int(yy_str)

            try:
                dd = int(line[20:22].strip())
                mm = int(line[23:25].strip())
                yy = int(line[26:28].strip())
            except ValueError:
                continue

            # Handle 2-digit year
            if yy < 50:
                year = 2000 + yy
            else:
                year = 1900 + yy

            amount_str = line[41:61].strip().replace(',', '')
            amount = float(amount_str)

            reptdate_file = datetime(year, mm, dd)
            reptdate_julian = int(reptdate_file.strftime('%y%j'))

            if reptdate_julian == params['rdat1']:
                cardamt = amount
                newrec_data.append({
                    'reptdate': reptdate_julian,
                    'amount': amount
                })

    if not newrec_data:
        print("Warning: No matching PBCARD record found in WALK file")
        return None, None, 0

    newrec_df = pl.DataFrame(newrec_data)

    # Append to base and get store data
    store_df = append_to_base('CARD', newrec_df, params['rdat1'], params['insert'])

    # Calculate maturity profile
    table_result, curbal, minbal = calculate_maturity_profile(store_df, table_df, cardamt)

    print(f"  Current Balance: {curbal:,.2f}")
    print(f"  Minimum Balance (48 weeks): {minbal:,.2f}")

    # Print tabulation
    print_tabulation(table_result, "PBCARD")

    # Create PBCARD dataset (NPL - Non-Performing Loans)
    pbcard_df = table_result.with_columns(
        pl.concat_str([
            pl.lit('9321508'),
            pl.col('remmth').map_elements(remfmt_format, return_dtype=pl.Utf8),
            pl.lit('0000Y')
        ]).alias('bnmcode')
    ).select(['bnmcode', 'amount'])

    # Create PBCARD2 dataset (Gross loans by basket)
    # Baskets: 1=15%, 2=69%, 3=1%, 4=3%, 5=7%, 6=5%
    basket_percentages = {
        1: 0.15,
        2: 0.69,
        3: 0.01,
        4: 0.03,
        5: 0.07,
        6: 0.05
    }

    pbcard2_data = []
    for basket, pct in basket_percentages.items():
        for _, row in table_df.iter_rows(named=True):
            pbcard2_data.append({
                'bnmcode': f"9521508{remfmt_format(row['remmth'])}0000Y",
                'amount': cardamt * pct
            })

    pbcard2_df = pl.DataFrame(pbcard2_data)

    return pbcard_df, pbcard2_df, cardamt


def process_overdraft(params, table_df, note_df, cust_type):
    """
    Process Overdraft data (Corporate or Individual)

    cust_type: '09' for Corporate, '08' for Individual
    """
    prod_name = 'ODCORP' if cust_type == '09' else 'ODIND'
    bnmcode_prefix = f'952130{cust_type}'

    print(f"\nProcessing {prod_name}...")

    # Filter note data for this customer type
    note_filtered = note_df.filter(
        pl.col('bnmcode').str.slice(0, 7) == bnmcode_prefix
    )

    # Summarize
    newrec_data = note_filtered.group_by('bnmcode').agg(
        pl.col('amount').sum()
    ).select(
        pl.lit(params['rdat1']).alias('reptdate'),
        pl.col('amount').sum().alias('amount')
    )

    if len(newrec_data) == 0:
        print(f"Warning: No {prod_name} data found")
        return None

    # Get total amount
    total_amount = newrec_data['amount'][0]

    # Create newrec dataframe
    # newrec_df = pl.DataFrame({
    #     'reptdate': [params['rdat1']],
    #     'amount': [total_amount]
    # })

    newrec_df = pl.DataFrame({
        'reptdate': [params['rdat1']],
        'amount': [float(total_amount)]
    })

    # Append to base and get store data
    store_df = append_to_base(prod_name, newrec_df, params['rdat1'], params['insert'])

    # Calculate maturity profile
    table_result, curbal, minbal = calculate_maturity_profile(store_df, table_df, total_amount)

    print(f"  Current Balance: {curbal:,.2f}")
    print(f"  Minimum Balance (48 weeks): {minbal:,.2f}")

    # Print tabulation
    print_tabulation(table_result, prod_name)

    # Create output dataset (NPL)
    od_df = table_result.with_columns(
        pl.concat_str([
            pl.lit(f'932130{cust_type}'),
            pl.col('remmth').map_elements(remfmt_format, return_dtype=pl.Utf8),
            pl.lit('0000Y')
        ]).alias('bnmcode')
    ).select(['bnmcode', 'amount'])

    return od_df


def process_smc(params, table_df, reptmon, nowk, cust_type):
    """
    Process SMC (Share Margin Credit) data (Corporate or Individual)

    cust_type: '09' for Corporate, '08' for Individual
    """
    prod_name = 'SMCCORP' if cust_type == '09' else 'SMCIND'
    bnmcode_prefix = f'952190{cust_type}'

    print(f"\nProcessing {prod_name}...")

    # Read loan data
    loan_path = BNM1_DIR / f"loan{reptmon}{nowk}.parquet"

    conn = duckdb.connect()
    loan_df = conn.execute(f"""
        SELECT * FROM read_parquet('{loan_path}')
    """).pl()

    # Filter for SMC products
    loan_df = loan_df.filter(
        ~pl.col('paidind').is_in(['P', 'C'])
    ).filter(
        (pl.col('prodcd').str.slice(0, 2) == '34') | (pl.col('product').is_in([225, 226]))
    )

    # Process SMC records
    smc_data = []

    for row in loan_df.iter_rows(named=True):
        # Determine customer type
        if row['custcd'] in ['77', '78', '95', '96']:
            cust = '08'
        else:
            cust = '09'

        # Only process overdrafts
        if row['acctype'] == 'OD':
            # Exclude certain products
            if row['product'] in [151, 152, 181]:
                continue

            remmth = 0.1
            amount = row['balance']

            # Only create for SMC products (34240)
            if row['prodcd'] == '34240':
                bnmcode = f"95219{cust}{remfmt_format(remmth)}0000Y"
                smc_data.append({
                    'bnmcode': bnmcode,
                    'amount': amount
                })

    if not smc_data:
        print(f"Warning: No {prod_name} data found")
        return None

    smc_df = pl.DataFrame(smc_data)

    # Filter for specific customer type
    smc_filtered = smc_df.filter(
        pl.col('bnmcode').str.slice(0, 7) == bnmcode_prefix
    )

    # Summarize
    newrec_data = smc_filtered.group_by('bnmcode').agg(
        pl.col('amount').sum()
    ).select(
        pl.lit(params['rdat1']).alias('reptdate'),
        pl.col('amount').sum().alias('amount')
    )

    if len(newrec_data) == 0:
        print(f"Warning: No {prod_name} summary data")
        return None

    # Get total amount
    total_amount = newrec_data['amount'][0]

    # Create newrec dataframe
    newrec_df = pl.DataFrame({
        'reptdate': [params['rdat1']],
        'amount': [total_amount]
    })

    # Append to base and get store data
    store_df = append_to_base(prod_name, newrec_df, params['rdat1'], params['insert'])

    # Calculate maturity profile
    table_result, curbal, minbal = calculate_maturity_profile(store_df, table_df, total_amount)

    print(f"  Current Balance: {curbal:,.2f}")
    print(f"  Minimum Balance (48 weeks): {minbal:,.2f}")

    # Print tabulation
    print_tabulation(table_result, prod_name)

    # Create output dataset (NPL)
    smc_out_df = table_result.with_columns(
        pl.concat_str([
            pl.lit(f'932190{cust_type}'),
            pl.col('remmth').map_elements(remfmt_format, return_dtype=pl.Utf8),
            pl.lit('0000Y')
        ]).alias('bnmcode')
    ).select(['bnmcode', 'amount'])

    return smc_out_df


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

def main():
    """Main execution function"""

    print("BNM Maturity Profile Calculation")
    print("=" * 80)

    # Step 1: Get reporting date parameters
    print("\n1. Getting reporting date parameters...")
    params = get_reptdate()
    print(f"   Reporting Date: {params['rdate']}")
    print(f"   Week Number: {params['nowk']}")
    print(f"   Insert Flag: {params['insert']}")
    print(f"   Julian Date: {params['rdat1']}")

    # Save REPTDATE to output
    reptdate_output = pl.DataFrame({
        'reptdate': [params['reptdate']]
    })
    reptdate_output.write_parquet(OUTPUT_DIR / "reptdate.parquet")

    # Step 2: Load TABLE (maturity buckets reference)
    print("\n2. Loading maturity bucket reference table...")
    table_path = BNM_DIR / "table.parquet"

    conn = duckdb.connect()
    table_df = conn.execute(f"""
        SELECT * FROM read_parquet('{table_path}')
    """).pl()

    print(f"   Maturity buckets loaded: {len(table_df)}")

    # Step 3: Load NOTE data (from previous processing)
    print("\n3. Loading NOTE data...")
    note_path = BNM_DIR / "note.parquet"

    note_df = conn.execute(f"""
        SELECT * FROM read_parquet('{note_path}')
    """).pl()

    print(f"   NOTE records loaded: {len(note_df)}")

    # Step 4: Process PBCARD
    pbcard_df, pbcard2_df, cardamt = process_pbcard(params, table_df)

    # Step 5: Process Overdraft - Corporate
    odcorp_df = process_overdraft(params, table_df, note_df, '09')

    # Step 6: Process Overdraft - Individual
    odind_df = process_overdraft(params, table_df, note_df, '08')

    # Step 7: Process SMC - Corporate
    smccorp_df = process_smc(params, table_df, params['reptmon'], params['nowk'], '09')

    # Step 8: Process SMC - Individual
    smcind_df = process_smc(params, table_df, params['reptmon'], params['nowk'], '08')

    # Step 9: Combine all results
    print("\n9. Combining all results...")

    all_dfs = []
    if pbcard_df is not None:
        all_dfs.append(pbcard_df)
    if pbcard2_df is not None:
        all_dfs.append(pbcard2_df)
    if odcorp_df is not None:
        all_dfs.append(odcorp_df)
    if odind_df is not None:
        all_dfs.append(odind_df)
    if smccorp_df is not None:
        all_dfs.append(smccorp_df)
    if smcind_df is not None:
        all_dfs.append(smcind_df)

    if all_dfs:
        calc_df = pl.concat(all_dfs)
        print(f"   Total records: {len(calc_df)}")

        # Step 10: Save output
        print("\n10. Saving output...")
        output_path = OUTPUT_DIR / "calc.parquet"
        calc_df.write_parquet(output_path)
        print(f"   Output saved to: {output_path}")

        # Print sample
        print("\n11. Sample output:")
        print(calc_df.head(20))
    else:
        print("   Warning: No data to output")
        calc_df = None

    print("\n" + "=" * 80)
    print("Processing complete!")

    return calc_df


if __name__ == "__main__":
    try:
        result_df = main()
    except Exception as e:
        print(f"\nError occurred: {str(e)}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)
