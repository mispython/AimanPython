#!/usr/bin/env python3
"""
File Name: EIBMNL43
BNM Report Generation - Behavioral Maturity Profile Reports
Creates formatted CSV reports for RM and FCY positions
"""

import duckdb
import polars as pl
from pathlib import Path
import sys


# ==============================================================================
# PATH CONFIGURATION
# ==============================================================================
INPUT_DIR = Path("./input")
OUTPUT_DIR = Path("./output")
STORE_DIR = INPUT_DIR / "store"

# Create output directory if it doesn't exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
STORE_DIR.mkdir(parents=True, exist_ok=True)


# ==============================================================================
# PRODUCT CODE TO DESCRIPTION MAPPING
# ==============================================================================

PRODUCT_MAPPING = {
    # RM Non-Individual products
    ('9321109', '9521109'): {
        'desc': 'NONRMFL',
        'item': 'A1.01',
        'item4': '- FIXED TERM LOANS  '
    },
    ('9321209', '9521209'): {
        'desc': 'NONRMRC',
        'item': 'A1.02',
        'item4': '- REVOLVING LOANS  '
    },
    ('9321309', '9521309'): {
        'desc': 'NONRMOD',
        'item': 'A1.03',
        'item4': '- OVERDRAFTS'
    },
    ('9321909', '9521909'): {
        'desc': 'NONRMOT',
        'item': 'A1.04',
        'item4': '- OTHERS'
    },

    # RM Individual products
    ('9321408', '9521408'): {
        'desc': 'INDRMHL',
        'item': 'A1.05',
        'item4': '- HOUSING LOANS'
    },
    ('9321508', '9521508'): {
        'desc': 'INDRMCC',
        'item': 'A1.06',
        'item4': '- CREDIT CARDS'
    },
    ('9321308', '9521308'): {
        'desc': 'INDRMOD',
        'item': 'A1.07',
        'item4': '- OVERDRAFTS'
    },
    ('9321908', '9521908'): {
        'desc': 'INDRMOT',
        'item': 'A1.08',
        'item4': '- OTHERS'
    },

    # RM Miscellaneous
    ('9322100', '9522100'): {
        'desc': 'MISRMCH',
        'item': 'A1.09',
        'item4': '- CASH HOLDINGS'
    },
    ('9322200', '9522200'): {
        'desc': 'MISRMSR',
        'item': 'A1.10',
        'item4': '- SRR'
    },
    ('9322900', '9522900'): {
        'desc': 'MISRMOT',
        'item': 'A1.11',
        'item4': '- OTHER ASSETS'
    },

    # FCY Non-Individual products
    ('9421109', '9621109'): {
        'desc': 'NONFXFL',
        'item': 'B1.01',
        'item4': '- FIXED TERM LOANS  '
    },
    ('9421209', '9621209'): {
        'desc': 'NONFXRC',
        'item': 'B1.02',
        'item4': '- REVOLVING LOANS  '
    },
    ('9421309', '9621309'): {
        'desc': 'NONFXOD',
        'item': 'B1.03',
        'item4': '- OVERDRAFTS'
    },
    ('9421909', '9621909'): {
        'desc': 'NONFXOT',
        'item': 'B1.04',
        'item4': '- OTHERS'
    },

    # FCY Individual products
    ('9421408', '9621408'): {
        'desc': 'INDFXHL',
        'item': 'B1.05',
        'item4': '- HOUSING LOANS'
    },
    ('9421508', '9621508'): {
        'desc': 'INDFXCC',
        'item': 'B1.06',
        'item4': '- CREDIT CARDS'
    },
    ('9421308', '9621308'): {
        'desc': 'INDFXOD',
        'item': 'B1.07',
        'item4': '- OVERDRAFTS'
    },
    ('9421908', '9621908'): {
        'desc': 'INDFXOT',
        'item': 'B1.08',
        'item4': '- OTHERS'
    },

    # FCY Miscellaneous
    ('9422100', '9622100'): {
        'desc': 'MISFXCH',
        'item': 'B1.09',
        'item4': '- CASH HOLDINGS'
    },
    ('9422200', '9622200'): {
        'desc': 'MISFXSR',
        'item': 'B1.10',
        'item4': '- SRR'
    },
    ('9422900', '9622900'): {
        'desc': 'MISFXOT',
        'item': 'B1.11',
        'item4': '- OTHER ASSETS'
    },
}


# ==============================================================================
# BUCKET CODE TO COLUMN NAME MAPPING
# ==============================================================================

BUCKET_MAPPING = {
    '01': 'WEEK',  # UP TO 1 WK
    '02': 'MONTH',  # >1 WK - 1 MTH
    '03': 'QTR',  # >1 MTH - 3 MTHS
    '04': 'HALFYR',  # >3 MTHS - 6 MTHS
    '05': 'YEAR',  # >6 MTHS - 1 YR
    '06': 'LAST'  # >1 YEAR
}


# ==============================================================================
# PROCESSING FUNCTIONS
# ==============================================================================

def get_product_info(prod_code):
    """Get description, item, and item4 for a product code"""
    for key, value in PRODUCT_MAPPING.items():
        if prod_code in key:
            return value
    return None


def process_loan_data():
    """Load and process FINALSUM data"""
    print("\n1. Loading FINALSUM data...")

    # Read FINALSUM from parquet
    finalsum_path = STORE_DIR / "finalsum.parquet"

    conn = duckdb.connect()
    loan_df = conn.execute(f"""
        SELECT * FROM read_parquet('{finalsum_path}')
    """).pl()

    print(f"   Records loaded: {len(loan_df)}")

    # Extract PROD (first 7 characters) and INDNON (positions 6-7)
    loan_df = loan_df.with_columns([
        pl.col('bnmcode').str.slice(0, 7).alias('prod'),
        pl.col('bnmcode').str.slice(5, 2).alias('indnon')
    ])

    # Add descriptions based on PROD code
    desc_list = []
    for prod in loan_df['prod'].to_list():
        info = get_product_info(prod)
        desc_list.append(info['desc'] if info else '')

    loan_df = loan_df.with_columns(
        pl.Series('desc', desc_list)
    )

    # Round amounts to thousands (divide by 1000 and round)
    loan_df = loan_df.with_columns(
        (pl.col('amount') / 1000).round(0).alias('amount')
    )

    print(f"   Unique products: {loan_df['prod'].n_unique()}")

    return loan_df


def transpose_data(loan_df):
    """Transpose data to get maturity buckets as columns"""
    print("\n2. Transposing data by maturity bucket...")

    # Extract bucket code (positions 8-9 of BNMCODE)
    loan_df = loan_df.with_columns(
        pl.col('bnmcode').str.slice(7, 2).alias('bucket')
    )

    # Map bucket codes to column names
    loan_df = loan_df.with_columns(
        pl.col('bucket').replace(BUCKET_MAPPING).alias('bucket_name')
    )

    # Pivot to get buckets as columns
    transposed_df = loan_df.pivot(
        values='amount',
        index=['prod', 'desc'],
        # columns='bucket_name',
        on='bucket_name',
        aggregate_function='sum'
    )

    # Ensure all columns exist (fill missing with 0)
    for col in ['WEEK', 'MONTH', 'QTR', 'HALFYR', 'YEAR', 'LAST']:
        if col not in transposed_df.columns:
            transposed_df = transposed_df.with_columns(
                pl.lit(0.0).alias(col)
            )

    # Fill nulls with 0
    transposed_df = transposed_df.fill_null(0)

    print(f"   Transposed records: {len(transposed_df)}")

    return transposed_df


def add_metadata(store_df):
    """Add ITEM, ITEM2, ITEM3, ITEM4, and BALANCE columns"""
    print("\n3. Adding metadata and calculated fields...")

    # Add product information
    item_list = []
    item4_list = []

    for prod in store_df['prod'].to_list():
        info = get_product_info(prod)
        if info:
            item_list.append(info['item'])
            item4_list.append(info['item4'])
        else:
            item_list.append('')
            item4_list.append('')

    store_df = store_df.with_columns([
        pl.Series('item', item_list),
        pl.Series('item4', item4_list)
    ])

    # Add ITEM2 (constant for all)
    store_df = store_df.with_columns(
        pl.lit('LOAN :').alias('item2')
    )

    # Add INDNON (extract from PROD)
    store_df = store_df.with_columns(
        pl.col('prod').str.slice(5, 2).alias('indnon')
    )

    # Add ITEM3 based on INDNON
    store_df = store_df.with_columns(
        pl.when(pl.col('indnon') == '08')
        .then(pl.lit('INDIVIDUALS    '))
        .when(pl.col('indnon') == '09')
        .then(pl.lit('NON-INDIVIDUALS'))
        .otherwise(pl.lit('               '))
        .alias('item3')
    )

    # Calculate BALANCE (sum of all maturity buckets)
    store_df = store_df.with_columns(
        (
                pl.col('WEEK') +
                pl.col('MONTH') +
                pl.col('QTR') +
                pl.col('HALFYR') +
                pl.col('YEAR') +
                pl.col('LAST')
        ).alias('balance')
    )

    print(f"   Records with metadata: {len(store_df)}")

    return store_df


def filter_and_sort_by_type(store_df, report_type):
    """
    Filter and sort data by report type

    report_type: '93' (RM Part 1), '95' (RM Part 2), '94' (FX Part 1), '96' (FX Part 2)
    """
    # Filter by report type and remove empty items
    filtered_df = store_df.filter(
        (pl.col('prod').str.slice(0, 2) == report_type) &
        (pl.col('item') != '')
    )

    # Sort by INDNON descending (Non-individuals first, then individuals)
    filtered_df = filtered_df.sort('indnon', descending=True)

    return filtered_df


def save_intermediate_files(store_df):
    """Save intermediate parquet files"""
    print("\n4. Saving intermediate files...")

    # RM Part 1 (NPL)
    notermp1_df = filter_and_sort_by_type(store_df, '93')
    notermp1_df.write_parquet(STORE_DIR / "notermp1.parquet")
    print(f"   NOTERMP1: {len(notermp1_df)} records")

    # RM Part 2 (Gross)
    notermp2_df = filter_and_sort_by_type(store_df, '95')
    notermp2_df.write_parquet(STORE_DIR / "notermp2.parquet")
    print(f"   NOTERMP2: {len(notermp2_df)} records")

    # FX Part 1 (NPL)
    notefxp1_df = filter_and_sort_by_type(store_df, '94')
    notefxp1_df.write_parquet(STORE_DIR / "notefxp1.parquet")
    print(f"   NOTEFXP1: {len(notefxp1_df)} records")

    # FX Part 2 (Gross)
    notefxp2_df = filter_and_sort_by_type(store_df, '96')
    notefxp2_df.write_parquet(STORE_DIR / "notefxp2.parquet")
    print(f"   NOTEFXP2: {len(notefxp2_df)} records")

    return notermp1_df, notermp2_df, notefxp1_df, notefxp2_df


def generate_report(report_df, title, output_filename):
    """
    Generate CSV report with headers

    Args:
        report_df: DataFrame with report data
        title: Report title
        output_filename: Output file name
    """
    print(f"\n   Generating {output_filename}...")

    output_path = OUTPUT_DIR / output_filename

    # Open file for writing
    with open(output_path, 'w') as f:
        # Write header
        f.write(' \n')
        f.write(f'{title}\n')
        f.write(' \n')
        f.write('INFLOW(ASSETS)\n')
        f.write('ON BALANCE SHEET\n')

        # Write column headers
        headers = [
            '    ',
            '    ',
            '    ',
            '    ',
            'UP TO 1 WK ',
            '>1 WK - 1 MTH',
            '>1 MTH - 3 MTHS',
            '>3 MTHS - 6 MTHS',
            '>6 MTHS - 1 YR',
            '>1 YEAR',
            'TOTAL'
        ]
        f.write(';'.join(headers) + ';\n')

        # Write data rows
        for row in report_df.iter_rows(named=True):
            line_parts = [
                row['item'],
                row['item2'],
                row['item3'],
                row['item4'],
                str(row['WEEK']),
                str(row['MONTH']),
                str(row['QTR']),
                str(row['HALFYR']),
                str(row['YEAR']),
                str(row['LAST']),
                str(row['balance'])
            ]
            f.write(';'.join(line_parts) + ';\n')

    print(f"   Saved: {output_path}")
    print(f"   Records: {len(report_df)}")


def generate_all_reports(notermp1_df, notermp2_df, notefxp1_df, notefxp2_df):
    """Generate all four reports"""
    print("\n5. Generating reports...")

    # RM Part 1 (NPL)
    generate_report(
        notermp1_df,
        'BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE - RINGGIT PART 1-RM',
        'report_rm_part1.csv'
    )

    # RM Part 2 (Gross)
    generate_report(
        notermp2_df,
        'BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE - RINGGIT PART 2-RM',
        'report_rm_part2.csv'
    )

    # FX Part 1 (NPL)
    generate_report(
        notefxp1_df,
        'BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE - FCY PART 1-FX',
        'report_fx_part1.csv'
    )

    # FX Part 2 (Gross)
    generate_report(
        notefxp2_df,
        'BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE - FCY PART 2-FX',
        'report_fx_part2.csv'
    )


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

def main():
    """Main execution function"""

    print("BNM Report Generation - Behavioral Maturity Profile")
    print("=" * 80)

    # Step 1: Load and process data
    loan_df = process_loan_data()

    # Step 2: Transpose data
    transposed_df = transpose_data(loan_df)

    # Step 3: Add metadata
    store_df = add_metadata(transposed_df)

    # Step 4: Save intermediate files and filter by report type
    notermp1_df, notermp2_df, notefxp1_df, notefxp2_df = save_intermediate_files(store_df)

    # Step 5: Generate reports
    generate_all_reports(notermp1_df, notermp2_df, notefxp1_df, notefxp2_df)

    print("\n" + "=" * 80)
    print("Report generation complete!")
    print("\nOutput files:")
    print("  - output/report_rm_part1.csv (RM Non-Performing Loans)")
    print("  - output/report_rm_part2.csv (RM Gross Loans)")
    print("  - output/report_fx_part1.csv (FCY Non-Performing Loans)")
    print("  - output/report_fx_part2.csv (FCY Gross Loans)")
    print("\nIntermediate files:")
    print("  - input/store/notermp1.parquet")
    print("  - input/store/notermp2.parquet")
    print("  - input/store/notefxp1.parquet")
    print("  - input/store/notefxp2.parquet")

    return store_df


if __name__ == "__main__":
    try:
        result_df = main()
    except Exception as e:
        print(f"\nError occurred: {str(e)}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)
