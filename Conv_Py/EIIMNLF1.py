#!/usr/bin/env python3
"""
File Name: EIIMNLF1
BNM Overdraft Maturity Profile Calculation
Simplified version  from EIBMNL41 - processes only OD (Overdraft) products for Corporate and Individual
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
    reptdate_path = LOAN_DIR / "reptdate.parquet"

    conn = duckdb.connect()
    reptdate_df = conn.execute(f"""
        SELECT * FROM read_parquet('{reptdate_path}')
    """).pl()

    reptdate = reptdate_df['reptdate'][0]

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

    # Calculate week number (from document #2 logic, but simplified)
    # Note: This version doesn't explicitly set WK, so using default logic
    day = reptdate.day
    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'

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

    print(f"  Current Balance: {curbal_from_store:,.2f}")
    print(f"  Minimum Balance (48 weeks): {minbal:,.2f}")

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
    base_path = base_dir / f"base_{prod}.parquet"
    store_path = base_dir / f"store_{prod}.parquet"

    # Read existing base if it exists
    if base_path.exists():
        conn = duckdb.connect()
        base_df = conn.execute(f"""
            SELECT * FROM read_parquet('{base_path}')
        """).pl()
    else:
        base_df = pl.DataFrame({
            'reptdate': [],
            'amount': []
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

def process_overdraft(params, table_df, note_df, cust_type):
    """
    Process Overdraft data (Corporate or Individual)

    cust_type: '09' for Corporate, '08' for Individual
    """
    prod_name = 'ODCORP' if cust_type == '09' else 'ODIND'
    bnmcode_prefix = f'952130{cust_type}'

    print(f"\nProcessing {prod_name}...")

    # Add BNMCODE1 column (first 7 characters)
    notex_df = note_df.with_columns(
        pl.col('bnmcode').str.slice(0, 7).alias('bnmcode1')
    )

    # Filter note data for this customer type
    note_filtered = notex_df.filter(
        pl.col('bnmcode1') == bnmcode_prefix
    )

    # Summarize
    newrec_data = note_filtered.group_by('bnmcode1').agg(
        pl.col('amount').sum()
    ).select(
        pl.lit(params['rdat1']).alias('reptdate'),
        pl.col('amount').sum().alias('amount')
    )

    if len(newrec_data) == 0:
        print(f"  Warning: No {prod_name} data found")
        return None

    # Get total amount
    total_amount = newrec_data['amount'][0]

    # Create newrec dataframe
    newrec_df = pl.DataFrame({
        'reptdate': [params['rdat1']],
        'amount': [total_amount]
    })

    print(f"  Total amount from NOTE: {total_amount:,.2f}")

    # Append to base and get store data
    store_df = append_to_base(prod_name, newrec_df, params['rdat1'], params['insert'])

    # Calculate maturity profile
    table_result, curbal, minbal = calculate_maturity_profile(store_df, table_df, total_amount)

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


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

def main():
    """Main execution function"""

    print("BNM Overdraft Maturity Profile Calculation (Simplified)")
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

    # Step 4: Process Overdraft - Corporate
    odcorp_df = process_overdraft(params, table_df, note_df, '09')

    # Step 5: Process Overdraft - Individual
    odind_df = process_overdraft(params, table_df, note_df, '08')

    # Step 6: Combine all results
    print("\n6. Combining all results...")

    all_dfs = []
    if odcorp_df is not None:
        all_dfs.append(odcorp_df)
        print(f"   - ODCORP: {len(odcorp_df)} records")

    if odind_df is not None:
        all_dfs.append(odind_df)
        print(f"   - ODIND: {len(odind_df)} records")

    if all_dfs:
        calc_df = pl.concat(all_dfs)
        print(f"   Total records: {len(calc_df)}")

        # Step 7: Save output
        print("\n7. Saving output...")
        output_path = OUTPUT_DIR / "calc.parquet"
        calc_df.write_parquet(output_path)
        print(f"   Output saved to: {output_path}")

        # Also save to BNM directory
        bnm_output_path = BNM_DIR / "calc.parquet"
        calc_df.write_parquet(bnm_output_path)
        print(f"   Output also saved to: {bnm_output_path}")

        # Print sample
        print("\n8. Sample output:")
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
