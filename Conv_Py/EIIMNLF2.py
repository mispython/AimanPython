#!/usr/bin/env python3
"""
EIIMNLF2 - BNM Cash and SRR Processing with Consolidation
Simplified version - processes only Cash Holdings and SRR from WALK file
Then consolidates with NOTE and CALC to produce FINALSUM
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


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def get_reptdate():
    """Get reporting date and calculate week number"""
    # Read REPTDATE from parquet file
    reptdate_path = LOAN_DIR / "reptdate.parquet"

    conn = duckdb.connect()
    reptdate_df = conn.execute(f"""
        SELECT * FROM read_parquet('{reptdate_path}')
    """).pl()

    reptdate = reptdate_df['reptdate'][0]

    # Calculate week number based on day
    day = reptdate.day
    if day == 8:
        wk = '1'
    elif day == 15:
        wk = '2'
    elif day == 22:
        wk = '3'
    else:
        wk = '0'

    # Check if this is the last day of the month (week 4)
    # Calculate the first day of next month
    if reptdate.month == 12:
        sreptdate = datetime(reptdate.year + 1, 1, 1)
    else:
        sreptdate = datetime(reptdate.year, reptdate.month + 1, 1)

    # Previous day is last day of current month
    prvrptdate = sreptdate - timedelta(days=1)

    if reptdate == prvrptdate:
        wk = '4'

    nowk = wk
    rdate = reptdate.strftime('%d%m%Y')
    rdat1 = int(reptdate.strftime('%y%j'))  # Julian date format (YYDDD)
    reptmon = f"{reptdate.month:02d}"
    reptday = f"{reptdate.day:02d}"

    return {
        'reptdate': reptdate,
        'nowk': nowk,
        'rdate': rdate,
        'rdat1': rdat1,
        'reptmon': reptmon,
        'reptday': reptday
    }


def parse_walk_file(walk_path, rdat1):
    """
    Parse WALK file in fixed-width format

    Format:
    @2   DESC          $19.
    @21  DD              2.
    @24  MM              2.
    @27  YY              2.
    @42  AMOUNT    COMMA20.2
    """
    if not walk_path.exists():
        print(f"Warning: WALK file not found at {walk_path}")
        return pl.DataFrame({
            'desc': [],
            'reptdate': [],
            'amount': []
        })

    # Read walk file
    with open(walk_path, 'r') as f:
        lines = f.readlines()

    # Parse lines
    data = []

    for line in lines:
        if len(line) < 50:
            continue

        desc = line[1:20].strip()

        try:
            dd = int(line[20:22])
            mm = int(line[23:25])
            yy = int(line[26:28])

            # Handle 2-digit year
            if yy < 50:
                year = 2000 + yy
            else:
                year = 1900 + yy

            amount_str = line[41:61].strip().replace(',', '')
            amount = float(amount_str)

            reptdate_file = datetime(year, mm, dd)
            reptdate_julian = int(reptdate_file.strftime('%y%j'))

            # Only keep records matching reporting date
            if reptdate_julian == rdat1:
                data.append({
                    'desc': desc,
                    'reptdate': reptdate_julian,
                    'amount': amount
                })
        except (ValueError, IndexError):
            # Skip malformed lines
            continue

    # walk_df = pl.DataFrame(data)
    #
    # return walk_df

    walk_df = pl.DataFrame(
        data,
        schema={
            "desc": pl.Utf8,
            "reptdate": pl.Int32,
            "amount": pl.Float64,
        },
    )
    return walk_df


def create_default_records(bnmcode_prefix1, bnmcode_prefix2):
    """
    Create default zero-value records for all maturity buckets

    Args:
        bnmcode_prefix1: First 8 characters of BNMCODE (e.g., '93222000')
        bnmcode_prefix2: First 8 characters of BNMCODE (e.g., '95222000')

    Returns:
        DataFrame with default records
    """
    data = []

    for n in range(1, 7):
        data.append({
            'bnmcode': f"{bnmcode_prefix1}{n}0000Y",
            'amount': 0.0
        })
        data.append({
            'bnmcode': f"{bnmcode_prefix2}{n}0000Y",
            'amount': 0.0
        })

    return pl.DataFrame(data).sort('bnmcode')


# ==============================================================================
# PROCESSING FUNCTIONS FOR EACH PRODUCT
# ==============================================================================

def process_cash_holdings(walk_df):
    """Process Cash Holdings (CASH) from WALK file"""
    if walk_df.is_empty():
        return pl.DataFrame(
            {"bnmcode": [], "amount": []},
            schema={"bnmcode": pl.Utf8, "amount": pl.Float64},
        )

    print("\nProcessing Cash Holdings...")

    # Filter for 39110
    cash_data = walk_df.filter(pl.col('desc') == '39110')

    if len(cash_data) == 0:
        print("  Warning: No Cash Holdings data found")
        return pl.DataFrame({
            'bnmcode': [],
            'amount': []
        })

    # Sum amount
    total_amount = cash_data['amount'].sum()

    print(f"  Total amount: {total_amount:,.2f}")

    # Create CASH records (bucket 01 = up to 1 week)
    cash_records = pl.DataFrame([
        {'bnmcode': '93221000100Y', 'amount': total_amount},
        {'bnmcode': '95221000100Y', 'amount': total_amount}
    ])

    return cash_records


def process_srr(walk_df):
    """Process Statutory Reserve Requirement (SRR) from WALK file"""
    if walk_df.is_empty():
        return pl.DataFrame(
            {"bnmcode": [], "amount": []},
            schema={"bnmcode": pl.Utf8, "amount": pl.Float64},
        )

    print("\nProcessing SRR...")

    # Create default records
    default_df = create_default_records('93222000', '95222000')

    # Filter for 32110
    srr_data = walk_df.filter(pl.col('desc') == '32110')

    if len(srr_data) == 0:
        print("  Warning: No SRR data found")
        # Return defaults only
        return default_df

    # Sum amount
    total_amount = srr_data['amount'].sum()

    print(f"  Total amount: {total_amount:,.2f}")

    # Create SRR records (bucket 06 = > 1 year)
    srr_records = pl.DataFrame([
        {'bnmcode': '93222000600Y', 'amount': total_amount},
        {'bnmcode': '95222000600Y', 'amount': total_amount}
    ])

    # Merge with defaults
    result_df = default_df.join(
        srr_records,
        on='bnmcode',
        how='left',
        suffix='_new'
    ).with_columns(
        pl.coalesce([pl.col('amount_new'), pl.col('amount')]).alias('amount')
    ).select(['bnmcode', 'amount'])

    return result_df


# ==============================================================================
# CONSOLIDATION
# ==============================================================================

def consolidate_all_outputs():
    """Consolidate all BNM outputs into final dataset"""
    print("\nConsolidating all BNM outputs...")

    conn = duckdb.connect()

    all_dfs = []

    # List of all BNM output files to consolidate
    output_files = [
        ('note.parquet', 'NOTE'),
        ('calc.parquet', 'CALC'),
        ('glset.parquet', 'GLSET')
    ]

    for filename, name in output_files:
        filepath = BNM_DIR / filename

        if filepath.exists():
            df = conn.execute(f"""
                SELECT * FROM read_parquet('{filepath}')
            """).pl()
            all_dfs.append(df)
            print(f"  Loaded {name}: {len(df)} records")
        else:
            print(f"  Warning: {name} not found at {filepath}")

    if not all_dfs:
        print("  Error: No input files found for consolidation")
        return None

    # Concatenate all dataframes
    final_df = pl.concat(all_dfs)

    print(f"\n  Total records before summarization: {len(final_df)}")

    # Summarize by BNMCODE
    finalsum_df = final_df.group_by('bnmcode').agg(
        pl.col('amount').sum().alias('amount')
    ).sort('bnmcode')

    print(f"  Total unique BNMCODEs: {len(finalsum_df)}")

    return final_df, finalsum_df


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

def main():
    """Main execution function"""

    print("BNM Cash and SRR Processing with Consolidation (EIIMNLF2)")
    print("=" * 80)

    # Step 1: Get reporting date parameters
    print("\n1. Getting reporting date parameters...")
    params = get_reptdate()
    print(f"   Reporting Date: {params['rdate']}")
    print(f"   Week Number: {params['nowk']}")
    print(f"   Julian Date: {params['rdat1']}")

    # Save REPTDATE to output with XXX column
    reptdate_output = pl.DataFrame({
        'reptdate': [params['reptdate']],
        'xxx': [int(params['nowk'])]
    })
    reptdate_output.write_parquet(OUTPUT_DIR / "reptdate.parquet")

    # Print REPTDATE
    print("\n   REPTDATE dataset:")
    print(reptdate_output)

    # Step 2: Parse WALK file
    print("\n2. Parsing WALK file...")
    walk_df = parse_walk_file(WALK_FILE, params['rdat1'])
    print(f"   Records parsed: {len(walk_df)}")

    if len(walk_df) > 0:
        # Show unique descriptions found
        unique_descs = walk_df['desc'].unique().to_list()
        print(f"   Unique descriptions: {len(unique_descs)}")
        for desc in sorted(unique_descs):
            count = len(walk_df.filter(pl.col('desc') == desc))
            total = walk_df.filter(pl.col('desc') == desc)['amount'].sum()
            print(f"     - {desc}: {count} record(s), total = {total:,.2f}")

    # Step 3: Process Cash Holdings
    cash_df = process_cash_holdings(walk_df)

    # Step 4: Process SRR
    srr_df = process_srr(walk_df)

    # Step 5: Combine GL set results
    print("\n5. Combining GL set results...")

    all_gl_dfs = []

    if cash_df is not None and len(cash_df) > 0:
        all_gl_dfs.append(cash_df)
        print(f"   - Cash Holdings: {len(cash_df)} records")

    if srr_df is not None and len(srr_df) > 0:
        all_gl_dfs.append(srr_df)
        print(f"   - SRR: {len(srr_df)} records")

    if all_gl_dfs:
        glset_df = pl.concat(all_gl_dfs)
        print(f"\n   Total GL set records: {len(glset_df)}")

        # Save GL set
        glset_path = BNM_DIR / "glset.parquet"
        glset_df.write_parquet(glset_path)
        print(f"   GL set saved to: {glset_path}")

        # Also save to output
        glset_df.write_parquet(OUTPUT_DIR / "glset.parquet")

        # Print GLSET
        print("\n   GLSET dataset:")
        print(glset_df)
    else:
        print("\n   Warning: No GL set data generated")
        glset_df = None

    # Step 6: Consolidate all BNM outputs
    print("\n6. Final consolidation...")
    result = consolidate_all_outputs()

    if result is not None:
        final_df, finalsum_df = result

        # Save FINAL (all records)
        final_path = BNM_DIR / "final.parquet"
        final_df.write_parquet(final_path)
        print(f"\n   FINAL saved to: {final_path}")

        # Also save to output
        final_df.write_parquet(OUTPUT_DIR / "final.parquet")

        # Save FINALSUM (summarized by BNMCODE)
        finalsum_path = BNM_DIR / "finalsum.parquet"
        finalsum_df.write_parquet(finalsum_path)
        print(f"   FINALSUM saved to: {finalsum_path}")

        # Also save to output
        finalsum_df.write_parquet(OUTPUT_DIR / "finalsum.parquet")

        # Print sample
        print("\n7. Sample of FINALSUM:")
        print(finalsum_df.head(20))

        # Print summary statistics
        print("\n8. Summary Statistics:")
        print(f"   Total amount (all records): {finalsum_df['amount'].sum():,.2f}")
        print(f"   Number of unique BNMCODEs: {len(finalsum_df)}")

        # Count by report type
        report_types = {
            '93': 'NPL (RM)',
            '94': 'NPL (FCY)',
            '95': 'Gross (RM)',
            '96': 'Gross (FCY)'
        }

        print("\n   Breakdown by report type:")
        for code, desc in report_types.items():
            count = len(finalsum_df.filter(pl.col('bnmcode').str.slice(0, 2) == code))
            total = finalsum_df.filter(pl.col('bnmcode').str.slice(0, 2) == code)['amount'].sum()
            print(f"     {code} ({desc}): {count} codes, total = {total:,.2f}")

    else:
        print("\n   Error: Consolidation failed")
        final_df = None
        finalsum_df = None

    print("\n" + "=" * 80)
    print("Processing complete!")

    return finalsum_df


if __name__ == "__main__":
    try:
        result_df = main()
    except Exception as e:
        print(f"\nError occurred: {str(e)}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)
