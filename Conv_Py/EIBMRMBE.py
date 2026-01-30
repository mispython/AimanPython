#!/usr/bin/env python3
"""
File Name: EIBMRMBE
STATEMENT OF FINANCIAL POSITION
Generates a formatted report showing CNY (Chinese Yuan) financial position
broken down by resident/non-resident and customer categories.

Generating Table 1 reports.
"""

import duckdb
import polars as pl
from datetime import datetime
from pathlib import Path
import sys


# ============================================================================
# Configuration and Path Setup
# ============================================================================

# Input/Output paths
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input files
REPTDATE_FILE = INPUT_DIR / "reptdate.parquet"
BTRAD_FILE_TEMPLATE = INPUT_DIR / "btrad{month}{week}{year}.parquet"
FCY_FILE_TEMPLATE = INPUT_DIR / "fcy{month}{week}{year}.parquet"

# Output file (report with ASA carriage control)
OUTPUT_FILE = OUTPUT_DIR / "table1_financial_position.txt"

# Report configuration
PAGE_LENGTH = 60  # Default page length
REPORT_WIDTH = 220  # Width of the report

# ============================================================================
# Customer Code Classifications
# ============================================================================

# Banking Institutions
BNKINSTI_CODES = [1, 2, 3, 12, 7]

# Corporate customers
CORPORATE_CODES = [
    20, 17, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 45, 4,
    5, 6, 60, 61, 62, 63, 64, 41, 42, 43, 44, 46, 47, 48, 49,
    51, 52, 53, 54, 57, 59, 75, 70, 71, 72, 73, 74, 66, 67, 68, 69
]

# Retail customers
RETAIL_CODES = [77, 78, 79]

# Foreign Banking Institutions
FORBNKINSTI_CODES = [81, 82, 83, 84]

# Foreign Non-Bank entities
FORNONBNK_CODES = [85, 86, 87, 88, 89, 90, 91, 92, 95, 96, 98, 99]

# Fixed deposit products
FD_PRODUCTS = [360, 350, 351, 352, 353, 354, 355, 356, 357, 358]


# ============================================================================
# Main Processing Functions
# ============================================================================

def process_reptdate():
    """
    Process REPTDATE to extract macro variables
    Returns: dict with macro variables
    """
    df = pl.read_parquet(REPTDATE_FILE)

    if len(df) == 0:
        raise ValueError("REPTDATE file is empty")

    reptdate = df['REPTDATE'][0]
    day = reptdate.day

    # Determine week number
    if 1 <= day <= 8:
        nowk = '1'
    elif 9 <= day <= 15:
        nowk = '2'
    elif 16 <= day <= 22:
        nowk = '3'
    else:
        nowk = '4'

    # Extract date components
    reptyear = str(reptdate.year)[-2:]  # Last 2 digits (YEAR2.)
    reptyr1 = str(reptdate.year)  # Full year (YEAR4.)
    reptmon = f"{reptdate.month:02d}"
    reptday = f"{reptdate.day:02d}"
    rdate = f"{reptday}/{reptmon}/{reptyear}"

    return {
        'NOWK': nowk,
        'REPTYEAR': reptyear,
        'REPTYR1': reptyr1,
        'REPTMON': reptmon,
        'REPTDAY': reptday,
        'RDATE': rdate,
        'REPTDT': reptdate
    }


def classify_customer(custcd, amount):
    """
    Classify customer and return amounts for each category
    Returns: dict with BNKINSTI, CORPORATE, RETAIL, FORBNKINSTI, FORNONBNK
    """
    result = {
        'BNKINSTI': 0.0,
        'CORPORATE': 0.0,
        'RETAIL': 0.0,
        'FORBNKINSTI': 0.0,
        'FORNONBNK': 0.0
    }

    if custcd in BNKINSTI_CODES:
        result['BNKINSTI'] = amount
    elif custcd in CORPORATE_CODES:
        result['CORPORATE'] = amount
    elif custcd in RETAIL_CODES:
        result['RETAIL'] = amount
    elif custcd in FORBNKINSTI_CODES:
        result['FORBNKINSTI'] = amount
    elif custcd in FORNONBNK_CODES:
        result['FORNONBNK'] = amount

    return result


def process_btrad_data(macro_vars):
    """
    Process BTRAD (Bills/Trade) data for CNY currency
    """
    # Construct BTRAD filename
    btrad_file = INPUT_DIR / f"btrad{macro_vars['REPTMON']}{macro_vars['NOWK']}{macro_vars['REPTYEAR']}.parquet"

    if not btrad_file.exists():
        print(f"Warning: BTRAD file not found: {btrad_file}")
        return pl.DataFrame()

    # Read BTRAD data
    con = duckdb.connect()

    # Filter for CNY currency and specific liability codes
    query = f"""
    SELECT 
        LIABCODE,
        FORCURR,
        CUSTCD,
        TRANXMT,
        INTAMT
    FROM read_parquet('{btrad_file}')
    WHERE LIABCODE IN ('FFS', 'FFU', 'FCS', 'FCU', 'FFL', 'FTL', 'FTI')
      AND FORCURR = 'CNY'
    """

    df = con.execute(query).pl()
    con.close()

    if len(df) == 0:
        return pl.DataFrame()

    # Calculate TRXAMT
    df = df.with_columns([
        (pl.col('TRANXMT') + pl.col('INTAMT')).alias('TRXAMT')
    ])

    # Classify customers and aggregate
    records = []
    for row in df.iter_rows(named=True):
        custcd = row['CUSTCD']
        trxamt = row['TRXAMT']

        classification = classify_customer(custcd, trxamt)

        records.append({
            'CATEGORY': 'BTAC',
            'GROUP': 'FCYBT',
            'BNKINSTI': classification['BNKINSTI'],
            'CORPORATE': classification['CORPORATE'],
            'RETAIL': classification['RETAIL'],
            'FORBNKINSTI': classification['FORBNKINSTI'],
            'FORNONBNK': classification['FORNONBNK']
        })

    return pl.DataFrame(records) if records else pl.DataFrame()


def process_fcy_data(macro_vars):
    """
    Process FCY (Foreign Currency) data for CNY currency
    """
    # Construct FCY filename
    fcy_file = INPUT_DIR / f"fcy{macro_vars['REPTMON']}{macro_vars['NOWK']}{macro_vars['REPTYEAR']}.parquet"

    if not fcy_file.exists():
        print(f"Warning: FCY file not found: {fcy_file}")
        return pl.DataFrame()

    # Read FCY data
    con = duckdb.connect()

    # Filter for CNY currency
    query = f"""
    SELECT 
        PRODUCT,
        CURCODE,
        CUSTCD,
        FORBAL
    FROM read_parquet('{fcy_file}')
    WHERE CURCODE = 'CNY'
    """

    df = con.execute(query).pl()
    con.close()

    if len(df) == 0:
        return pl.DataFrame()

    # Process records
    records = []
    for row in df.iter_rows(named=True):
        product = row['PRODUCT']
        custcd = row['CUSTCD']
        forbal = row['FORBAL']

        # Determine category and group based on product
        if product in FD_PRODUCTS:
            category = 'DPAC'
            group = 'FCYFD'
        else:
            category = 'DPAC'
            group = 'FCYDD'

        classification = classify_customer(custcd, forbal)

        records.append({
            'CATEGORY': category,
            'GROUP': group,
            'BNKINSTI': classification['BNKINSTI'],
            'CORPORATE': classification['CORPORATE'],
            'RETAIL': classification['RETAIL'],
            'FORBNKINSTI': classification['FORBNKINSTI'],
            'FORNONBNK': classification['FORNONBNK']
        })

    return pl.DataFrame(records) if records else pl.DataFrame()


def aggregate_table1(btrad_df, fcy_df):
    """
    Combine BTRAD and FCY data and calculate totals
    """
    # Combine datasets
    if len(btrad_df) > 0 and len(fcy_df) > 0:
        table1 = pl.concat([btrad_df, fcy_df])
    elif len(btrad_df) > 0:
        table1 = btrad_df
    elif len(fcy_df) > 0:
        table1 = fcy_df
    else:
        return pl.DataFrame()

    # Calculate derived columns
    table1 = table1.with_columns([
        (pl.col('CORPORATE') + pl.col('RETAIL')).alias('CORPRETAIL'),
    ])

    table1 = table1.with_columns([
        (pl.col('BNKINSTI') + pl.col('CORPRETAIL')).alias('TOTAL_A'),
        (pl.col('FORBNKINSTI') + pl.col('FORNONBNK')).alias('TOTAL_F')
    ])

    table1 = table1.with_columns([
        (pl.col('TOTAL_A') + pl.col('TOTAL_F')).alias('TOTAL')
    ])

    return table1


def summarize_data(table1):
    """
    Summarize data by CATEGORY and GROUP, plus grand total
    """
    # SUMCOM1: Summarize by CATEGORY and GROUP
    sumcom1 = table1.group_by(['CATEGORY', 'GROUP']).agg([
        pl.col('TOTAL').sum(),
        pl.col('TOTAL_A').sum(),
        pl.col('BNKINSTI').sum(),
        pl.col('CORPRETAIL').sum(),
        pl.col('CORPORATE').sum(),
        pl.col('RETAIL').sum(),
        pl.col('TOTAL_F').sum(),
        pl.col('FORBNKINSTI').sum(),
        pl.col('FORNONBNK').sum()
    ])

    # SUMCOM2: Grand total (all records)
    sumcom2 = table1.select([
        pl.col('TOTAL').sum(),
        pl.col('TOTAL_A').sum(),
        pl.col('BNKINSTI').sum(),
        pl.col('CORPRETAIL').sum(),
        pl.col('CORPORATE').sum(),
        pl.col('RETAIL').sum(),
        pl.col('TOTAL_F').sum(),
        pl.col('FORBNKINSTI').sum(),
        pl.col('FORNONBNK').sum()
    ])

    # Add CATEGORY and GROUP for grand total
    sumcom2 = sumcom2.with_columns([
        pl.lit('TOTAL').alias('CATEGORY'),
        pl.lit('TOTAL').alias('GROUP')
    ])

    # Combine
    sumcom = pl.concat([sumcom1, sumcom2])

    return sumcom


def create_category_master():
    """
    Create master category list with ordering
    """
    categories = [
        {'NO': 1, 'CATEGORY': 'BTAC', 'GROUP': 'FCYBT'},
        {'NO': 2, 'CATEGORY': 'DPAC', 'GROUP': 'FCYFD'},
        {'NO': 3, 'CATEGORY': 'DPAC', 'GROUP': 'FCYDD'},
        {'NO': 4, 'CATEGORY': 'TOTAL', 'GROUP': 'TOTAL'},
    ]

    return pl.DataFrame(categories)


def merge_with_master(sumcom):
    """
    Merge summary with category master to ensure all categories present
    """
    cat = create_category_master()

    # Merge
    sumtable1 = cat.join(sumcom, on=['CATEGORY', 'GROUP'], how='left')

    # Fill nulls with 0
    for col in ['CORPORATE', 'RETAIL', 'CORPRETAIL', 'BNKINSTI',
                'FORBNKINSTI', 'FORNONBNK', 'TOTAL_A', 'TOTAL_F', 'TOTAL']:
        sumtable1 = sumtable1.with_columns([
            pl.col(col).fill_null(0.0)
        ])

    # Sort by NO
    sumtable1 = sumtable1.sort('NO')

    return sumtable1


def format_amount(value):
    """Format amount with comma separator"""
    if value is None:
        value = 0.0
    return f"{value:20,.2f}"


def generate_report(sumtable1, output_file):
    """
    Generate the formatted Table 1 report with ASA carriage control
    """
    lines = []

    # Process each record
    for idx, row in enumerate(sumtable1.iter_rows(named=True), 1):
        category = row['CATEGORY']
        group = row['GROUP']

        # Get values (handle None)
        total = row['TOTAL'] if row['TOTAL'] is not None else 0.0
        total_a = row['TOTAL_A'] if row['TOTAL_A'] is not None else 0.0
        bnkinsti = row['BNKINSTI'] if row['BNKINSTI'] is not None else 0.0
        corpretail = row['CORPRETAIL'] if row['CORPRETAIL'] is not None else 0.0
        corporate = row['CORPORATE'] if row['CORPORATE'] is not None else 0.0
        retail = row['RETAIL'] if row['RETAIL'] is not None else 0.0
        total_f = row['TOTAL_F'] if row['TOTAL_F'] is not None else 0.0
        forbnkinsti = row['FORBNKINSTI'] if row['FORBNKINSTI'] is not None else 0.0
        fornonbnk = row['FORNONBNK'] if row['FORNONBNK'] is not None else 0.0

        # Print header on first record
        if idx == 1:
            lines.append(' ' + 'TABLE 1 - STATEMENT OF FINANCIAL POSITION')
            lines.append(' ')
            lines.append(' ' + '-' * 219)

            # Header row 1
            line = (' |' + ' ' * 29 + '|' + ' ' * 20 + '|' +
                    ' ' * 41 + 'RESIDENT' + ' ' * 62 + '|' +
                    ' ' * 20 + 'NON RESIDENT' + ' ' * 41 + '|')
            lines.append(' ' + line)

            # Header row 2 - separator
            line = (' |' + ' ' * 29 + '|' + ' ' * 20 + '|' +
                    '-' * 104 + '|' + '-' * 62 + '|')
            lines.append(' ' + line)

            # Header row 3
            line = (' |' + ' ' * 29 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' +
                    ' ' * 20 + '|' + ' ' * 62 + '|' + ' ' * 20 +
                    '| FOREIGN  ' + ' ' * 20 + '| FOREIGN  ' + '|')
            lines.append(' ' + line)

            # Header row 4
            line = (' |' + ' ' * 29 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' +
                    ' ' * 20 + '|   NON-BANK ENTITY  ' + ' ' * 41 +
                    '| TOTAL(F) ' + ' ' * 20 + '| BANKING ' + ' ' * 20 +
                    '| NON-BANK ' + '|')
            lines.append(' ' + line)

            # Header row 5
            line = (' |' + ' ' * 29 + '| TOTAL=TOTAL(A)' + ' ' * 6 +
                    '| TOTAL(A) ' + ' ' * 9 + '| BANKING ' + ' ' * 10 + '|' +
                    ' ' * 62 + '| =(G)+(H)' + ' ' * 10 + '| INSTITUTION(G)' +
                    ' ' * 6 + '| ENTITY (H)' + ' ' * 8 + '|')
            lines.append(' ' + line)

            # Header row 6
            line = (' | ITEM ' + ' ' * 22 + '| + TOTAL(F)' + ' ' * 10 +
                    '| = (B) + (C)' + ' ' * 7 + '| INSTITUTION(B)' + ' ' * 6 + '|' +
                    '-' * 62 + '|' + '-' * 20 + '|' + '-' * 20 + '|' + '-' * 20 + '|')
            lines.append(' ' + line)

            # Header row 7
            line = (' |' + ' ' * 29 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' +
                    ' ' * 20 + '| TOTAL(C)' + ' ' * 11 + '| CORPOTATE(D)' +
                    ' ' * 8 + '| RETAIL(E)' + ' ' * 9 + '|' + ' ' * 20 + '|' +
                    ' ' * 20 + '|' + ' ' * 20 + '|')
            lines.append(' ' + line)

            # Header row 8
            line = (' |' + ' ' * 29 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' +
                    ' ' * 20 + '| = (D) + (E)' + ' ' * 9 + '|' + ' ' * 20 + '|' +
                    ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|')
            lines.append(' ' + line)

            # Header separator
            line = (' |' + '-' * 29 + '|' + '-' * 20 + '|' + '-' * 20 + '|' +
                    '-' * 20 + '|' + '-' * 20 + '|' + '-' * 20 + '|' + '-' * 20 + '|' +
                    '-' * 20 + '|' + '-' * 20 + '|' + '-' * 20 + '|')
            lines.append(' ' + line)

        # Data rows based on CATEGORY and GROUP
        if category == 'BTAC' and group == 'FCYBT':
            # Total Assets section
            line = ' | TOTAL ASSETS' + ' ' * 16 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|'
            lines.append(' ' + line)

            line = ' |' + ' ' * 29 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|'
            lines.append(' ' + line)

            line = ' | - LOANS/FINANCING AND' + ' ' * 7 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|'
            lines.append(' ' + line)

            line = ' |   RECEIVABLES' + ' ' * 15 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|'
            lines.append(' ' + line)

            # Data line
            line = (' |   (NET OF PROVISION)' + ' ' * 8 + '|' +
                    format_amount(total) + '|' +
                    format_amount(total_a) + '|' +
                    format_amount(bnkinsti) + '|' +
                    format_amount(corpretail) + '|' +
                    format_amount(corporate) + '|' +
                    format_amount(retail) + '|' +
                    format_amount(total_f) + '|' +
                    format_amount(forbnkinsti) + '|' +
                    format_amount(fornonbnk) + '|')
            lines.append(' ' + line)

            lines.append(' ' + '-' * 219)

        elif category == 'DPAC' and group == 'FCYFD':
            # Deposits Accepted header
            line = ' | DEPOSITS ACCEPTED' + ' ' * 11 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|'
            lines.append(' ' + line)

            line = ' |' + ' ' * 29 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|' + ' ' * 20 + '|'
            lines.append(' ' + line)

            # FX Fixed Deposits
            line = (' |    - FX FIXED DEPOSITS' + ' ' * 6 + '|' +
                    format_amount(total) + '|' +
                    format_amount(total_a) + '|' +
                    format_amount(bnkinsti) + '|' +
                    format_amount(corpretail) + '|' +
                    format_amount(corporate) + '|' +
                    format_amount(retail) + '|' +
                    format_amount(total_f) + '|' +
                    format_amount(forbnkinsti) + '|' +
                    format_amount(fornonbnk) + '|')
            lines.append(' ' + line)

        elif category == 'DPAC' and group == 'FCYDD':
            # FX Demand Deposits
            line = (' |    - FX DEMAND DEPOSITS' + ' ' * 5 + '|' +
                    format_amount(total) + '|' +
                    format_amount(total_a) + '|' +
                    format_amount(bnkinsti) + '|' +
                    format_amount(corpretail) + '|' +
                    format_amount(corporate) + '|' +
                    format_amount(retail) + '|' +
                    format_amount(total_f) + '|' +
                    format_amount(forbnkinsti) + '|' +
                    format_amount(fornonbnk) + '|')
            lines.append(' ' + line)

            # Separator after demand deposits
            line = ' |' + '-' * 29 + '|' + '-' * 20 + '|' + '-' * 20 + '|' + '-' * 20 + '|' + '-' * 20 + '|' + '-' * 20 + '|' + '-' * 20 + '|' + '-' * 20 + '|' + '-' * 20 + '|' + '-' * 20 + '|'
            lines.append(' ' + line)

        elif category == 'TOTAL' and group == 'TOTAL':
            # Grand Total
            line = (' | TOTAL' + ' ' * 23 + '|' +
                    format_amount(total) + '|' +
                    format_amount(total_a) + '|' +
                    format_amount(bnkinsti) + '|' +
                    format_amount(corpretail) + '|' +
                    format_amount(corporate) + '|' +
                    format_amount(retail) + '|' +
                    format_amount(total_f) + '|' +
                    format_amount(forbnkinsti) + '|' +
                    format_amount(fornonbnk) + '|')
            lines.append(' ' + line)

            lines.append(' ' + '-' * 219)

    # Write to file
    with open(output_file, 'w', encoding='utf-8') as f:
        for line in lines:
            f.write(line + '\n')


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Main execution function"""
    try:
        print("Starting Table 1 - Statement of Financial Position Report...")
        print("=" * 80)

        # Step 1: Process REPTDATE
        print("\n1. Processing REPTDATE...")
        macro_vars = process_reptdate()
        print(f"   Report Date: {macro_vars['RDATE']}")
        print(f"   Week: {macro_vars['NOWK']}, Month: {macro_vars['REPTMON']}, Year: {macro_vars['REPTYR1']}")

        # Step 2: Process BTRAD data
        print("\n2. Processing BTRAD data (CNY Bills/Trade)...")
        btrad_df = process_btrad_data(macro_vars)
        print(f"   BTRAD records: {len(btrad_df)}")

        # Step 3: Process FCY data
        print("\n3. Processing FCY data (CNY Foreign Currency)...")
        fcy_df = process_fcy_data(macro_vars)
        print(f"   FCY records: {len(fcy_df)}")

        # Step 4: Aggregate TABLE1
        print("\n4. Aggregating Table 1 data...")
        table1 = aggregate_table1(btrad_df, fcy_df)
        print(f"   Total records: {len(table1)}")

        # Step 5: Summarize data
        print("\n5. Summarizing by category and group...")
        sumcom = summarize_data(table1)

        # Step 6: Merge with master categories
        print("\n6. Merging with category master...")
        sumtable1 = merge_with_master(sumcom)
        print(f"   Summary records: {len(sumtable1)}")

        # Step 7: Generate report
        print("\n7. Generating formatted report...")
        generate_report(sumtable1, OUTPUT_FILE)

        print("\n" + "=" * 80)
        print("✓ Table 1 Report Generation Complete!")
        print(f"\nGenerated file:")
        print(f"  - {OUTPUT_FILE}")

        # Display summary
        print(f"\nReport Summary:")
        for row in sumtable1.iter_rows(named=True):
            cat = row['CATEGORY']
            grp = row['GROUP']
            tot = row['TOTAL'] if row['TOTAL'] is not None else 0.0
            print(f"  {cat:8s} / {grp:8s}: {tot:20,.2f}")

    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
