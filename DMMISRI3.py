#!/usr/bin/env python3
"""
File Name : DMMISRI3
PUBLIC ISLAMIC BANK BERHAD (IBU)
BREAKDOWN OF ISLAMIC SAVINGS ACCOUNT DEPOSIT
Generating Islamic Savings Account deposit reports.
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
ISA_FILE_TEMPLATE = INPUT_DIR / "isa{month}{week}{year}.parquet"

# Output file (report with ASA carriage control)
OUTPUT_FILE = OUTPUT_DIR / "dmmisri3_report.txt"

# Report configuration
PAGE_LENGTH = 60  # lines per page
MISSING_VALUE = 0


# ============================================================================
# Format Definitions (equivalent to SAS formats)
# ============================================================================

def get_deposit_range(amount):
    """
    Equivalent to SDRANGE. format
    Maps deposit amount to range code
    """
    if amount < 0:
        return 0
    elif amount < 100:
        return 1
    elif amount < 500:
        return 2
    elif amount < 1000:
        return 3
    elif amount < 5000:
        return 4
    elif amount < 10000:
        return 5
    elif amount < 50000:
        return 6
    elif amount < 100000:
        return 7
    elif amount < 500000:
        return 8
    elif amount < 1000000:
        return 9
    elif amount < 5000000:
        return 10
    else:
        return 11


def format_deposit_range(range_code):
    """
    Equivalent to SADPRG. format
    Maps range code to display string
    """
    range_labels = {
        1: "LESS THAN RM100",
        2: "RM100 - RM499",
        3: "RM500 - RM999",
        4: "RM1,000 - RM4,999",
        5: "RM5,000 - RM9,999",
        6: "RM10,000 - RM49,999",
        7: "RM50,000 - RM99,999",
        8: "RM100,000 - RM499,999",
        9: "RM500,000 - RM999,999",
        10: "RM1,000,000 - RM4,999,999",
        11: "RM5,000,000 AND ABOVE",
    }
    return range_labels.get(range_code, "UNKNOWN")


def format_product(product_code):
    """
    Equivalent to SDNAME. format
    Maps product code to product name
    """
    product_names = {
        204: "ISLAMIC SAVINGS ACCOUNT",
        215: "ISLAMIC SAVINGS ACCOUNT-i",
    }
    return product_names.get(product_code, f"PRODUCT {product_code}")


# ============================================================================
# Main Processing Functions
# ============================================================================

def process_reptdate():
    """
    Process REPTDATE to extract macro variables
    Returns: dict with NOWK, REPTYEAR, REPTMON, REPTDAY, RDATE
    """
    # Read REPTDATE file
    df = pl.read_parquet(REPTDATE_FILE)

    if len(df) == 0:
        raise ValueError("REPTDATE file is empty")

    reptdate = df['REPTDATE'][0]

    # Extract day
    day = reptdate.day

    # Determine week number (NOWK)
    if 1 <= day <= 8:
        nowk = '1'
    elif 9 <= day <= 15:
        nowk = '2'
    elif 16 <= day <= 22:
        nowk = '3'
    else:
        nowk = '4'

    # Extract year (2-digit), month, day
    reptyear = str(reptdate.year)[-2:]  # Last 2 digits
    reptmon = f"{reptdate.month:02d}"
    reptday = f"{reptdate.day:02d}"

    # Format date as DDMMYY (8 chars with slashes: DD/MM/YY)
    rdate = f"{reptday}/{reptmon}/{reptyear}"

    return {
        'NOWK': nowk,
        'REPTYEAR': reptyear,
        'REPTMON': reptmon,
        'REPTDAY': reptday,
        'RDATE': rdate,
        'reptdate_obj': reptdate
    }


def process_isa_data(macro_vars):
    """
    Process ISA data: filter, classify into ranges, and summarize
    """
    # Construct ISA filename
    isa_file = INPUT_DIR / f"isa{macro_vars['REPTMON']}{macro_vars['NOWK']}{macro_vars['REPTYEAR']}.parquet"

    if not isa_file.exists():
        raise FileNotFoundError(f"ISA file not found: {isa_file}")

    # Use DuckDB to process the parquet file
    con = duckdb.connect()

    # Register UDF for deposit range calculation
    con.create_function("get_deposit_range", get_deposit_range, return_type="INTEGER")
    con.create_function("format_deposit_range", format_deposit_range, return_type="VARCHAR")

    # Read and filter data, then add deposit range
    query = f"""
    SELECT 
        PRODUCT,
        CURBAL,
        get_deposit_range(CURBAL) as RANGE,
        format_deposit_range(get_deposit_range(CURBAL)) as DEPRANGE
    FROM read_parquet('{isa_file}')
    WHERE PRODUCT IN (204, 215)
      AND CURBAL >= 0
    """

    df = con.execute(query).pl()

    # Summarize by DEPRANGE and PRODUCT
    summary = df.group_by(['DEPRANGE', 'RANGE', 'PRODUCT']).agg([
        pl.count().alias('NOACCT'),
        pl.col('CURBAL').sum().alias('CURBAL')
    ]).sort(['PRODUCT', 'RANGE'])

    con.close()

    return summary


def format_number(value, width=12, decimals=0, with_commas=True):
    """Format number with commas"""
    if decimals > 0:
        formatted = f"{value:,.{decimals}f}"
    else:
        formatted = f"{value:,.0f}"

    return formatted.rjust(width)


def format_currency(value, width=20, decimals=2):
    """Format currency with $ and commas"""
    formatted = f"${value:,.{decimals}f}"
    return formatted.rjust(width)


def generate_report(summary_df, macro_vars):
    """
    Generate tabular report with ASA carriage control characters
    Equivalent to PROC TABULATE output
    """
    lines = []

    # ASA carriage control: '1' = new page, ' ' = single space, '0' = double space, '-' = triple space

    # Title section (new page)
    lines.append('1' + ' ' * 131)  # New page
    lines.append(' ' + 'REPORT ID : DMMISRI3'.center(131))
    lines.append(' ' + 'PUBLIC ISLAMIC BANK BERHAD (IBU)'.center(131))
    lines.append(' ' + 'BREAKDOWN OF ISLAMIC SAVINGS ACCOUNT DEPOSIT'.center(131))
    lines.append(' ' + f"REPORT AS AT {macro_vars['RDATE']}".center(131))
    lines.append('0' + ' ' * 131)  # Double space

    # Get unique products
    products = sorted(summary_df['PRODUCT'].unique().to_list())

    # Build deposit range order (for proper display order)
    range_order = [
        "LESS THAN RM100",
        "RM100 - RM499",
        "RM500 - RM999",
        "RM1,000 - RM4,999",
        "RM5,000 - RM9,999",
        "RM10,000 - RM49,999",
        "RM50,000 - RM99,999",
        "RM100,000 - RM499,999",
        "RM500,000 - RM999,999",
        "RM1,000,000 - RM4,999,999",
        "RM5,000,000 AND ABOVE",
    ]

    # Process each product
    for product in products:
        product_name = format_product(product)
        product_data = summary_df.filter(pl.col('PRODUCT') == product)

        # Header for product section
        lines.append(' ' + '-' * 131)
        header_line = ' ' + product_name.ljust(35) + '|' + 'NO OF A/C'.rjust(12) + ' |' + 'AMOUNT'.rjust(20) + ' |'
        lines.append(header_line)
        lines.append(' ' + '-' * 131)

        # Initialize product totals
        product_total_acct = 0
        product_total_amt = 0

        # Process each deposit range in order
        for deprange in range_order:
            range_data = product_data.filter(pl.col('DEPRANGE') == deprange)

            if len(range_data) > 0:
                noacct = range_data['NOACCT'][0]
                curbal = range_data['CURBAL'][0]
            else:
                noacct = 0
                curbal = 0.0

            product_total_acct += noacct
            product_total_amt += curbal

            # Format and add line
            deprange_display = 'DEPOSIT RANGE/' + deprange
            line = ' ' + deprange_display.ljust(35) + '|' + format_number(noacct, 12) + ' |' + format_currency(curbal,
                                                                                                               20) + ' |'
            lines.append(line)

        # Product total line
        lines.append(' ' + '-' * 131)
        total_line = ' ' + 'TOTAL'.ljust(35) + '|' + format_number(product_total_acct, 12) + ' |' + format_currency(
            product_total_amt, 20) + ' |'
        lines.append(total_line)
        lines.append(' ' + '-' * 131)
        lines.append(' ')

    # Bank total section
    lines.append(' ' + '=' * 131)
    lines.append(' ' + 'BANK TOTAL'.ljust(35) + '|' + 'NO OF A/C'.rjust(12) + ' |' + 'AMOUNT'.rjust(20) + ' |')
    lines.append(' ' + '=' * 131)

    # Calculate bank totals by deposit range
    bank_total_acct = 0
    bank_total_amt = 0

    for deprange in range_order:
        range_data = summary_df.filter(pl.col('DEPRANGE') == deprange)

        if len(range_data) > 0:
            noacct = range_data['NOACCT'].sum()
            curbal = range_data['CURBAL'].sum()
        else:
            noacct = 0
            curbal = 0.0

        bank_total_acct += noacct
        bank_total_amt += curbal

        # Format and add line
        deprange_display = 'DEPOSIT RANGE/' + deprange
        line = ' ' + deprange_display.ljust(35) + '|' + format_number(noacct, 12) + ' |' + format_currency(curbal,
                                                                                                           20) + ' |'
        lines.append(line)

    # Grand total line
    lines.append(' ' + '=' * 131)
    total_line = ' ' + 'TOTAL'.ljust(35) + '|' + format_number(bank_total_acct, 12) + ' |' + format_currency(
        bank_total_amt, 20) + ' |'
    lines.append(total_line)
    lines.append(' ' + '=' * 131)

    return lines


def write_report_with_asa(lines, output_file):
    """
    Write report with ASA carriage control characters
    Each line already has ASA control character as first character
    """
    with open(output_file, 'w', encoding='utf-8') as f:
        for line in lines:
            f.write(line + '\n')


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Main execution function"""
    try:
        print("Starting DMMISRI3 Report Generation...")

        # Step 1: Process REPTDATE to get macro variables
        print("Processing REPTDATE...")
        macro_vars = process_reptdate()
        print(f"  Report Date: {macro_vars['RDATE']}")
        print(f"  Week: {macro_vars['NOWK']}, Month: {macro_vars['REPTMON']}, Year: {macro_vars['REPTYEAR']}")

        # Step 2: Process ISA data
        print("\nProcessing ISA data...")
        summary_df = process_isa_data(macro_vars)
        print(f"  Processed {len(summary_df)} summary records")

        # Step 3: Generate report
        print("\nGenerating report...")
        report_lines = generate_report(summary_df, macro_vars)

        # Step 4: Write report with ASA carriage control
        print(f"\nWriting report to {OUTPUT_FILE}...")
        write_report_with_asa(report_lines, OUTPUT_FILE)

        print(f"\n✓ Report generation complete!")
        print(f"  Output file: {OUTPUT_FILE}")
        print(f"  Total lines: {len(report_lines)}")

    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
