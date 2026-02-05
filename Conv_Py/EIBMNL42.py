#!/usr/bin/env python3
"""
File Name: EIBMNL42
BNM GL Set and Final Consolidation
Processes WALK file for various GL accounts and consolidates all BNM outputs
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
    reptdate_path = BNM_DIR / "reptdate.parquet"

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

    walk_df = pl.DataFrame(data)

    return walk_df


def create_default_records(bnmcode_prefix1, bnmcode_prefix2):
    """
    Create default zero-value records for all maturity buckets

    Args:
        bnmcode_prefix1: First 8 characters of BNMCODE (e.g., '93219090')
        bnmcode_prefix2: First 8 characters of BNMCODE (e.g., '95219090')

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

def process_floor_stocking(walk_df):
    """Process Floor Stocking (FS) from WALK file"""
    print("\nProcessing Floor Stocking...")

    # Create default records
    default_df = create_default_records('93219090', '95219090')

    # Filter for ML34170
    fs_data = walk_df.filter(pl.col('desc') == 'ML34170')

    if len(fs_data) == 0:
        print("  Warning: No Floor Stocking data found")
        # Return defaults only
        return default_df

    # Sum amount
    total_amount = fs_data['amount'].sum()

    print(f"  Total amount: {total_amount:,.2f}")

    # Create FS records (bucket 05 = >6 months to 1 year)
    fs_records = pl.DataFrame([
        {'bnmcode': '93219090500Y', 'amount': total_amount},
        {'bnmcode': '95219090500Y', 'amount': total_amount}
    ])

    # Merge with defaults
    result_df = default_df.join(
        fs_records,
        on='bnmcode',
        how='left',
        suffix='_new'
    ).with_columns(
        pl.coalesce([pl.col('amount_new'), pl.col('amount')]).alias('amount')
    ).select(['bnmcode', 'amount'])

    return result_df


def process_hire_purchase(walk_df):
    """Process Hire Purchase Agency (HP) from WALK file"""
    print("\nProcessing Hire Purchase Agency...")

    # Create default records
    default_df = create_default_records('93211090', '95211090')

    # Filter for ML34111
    hp_data = walk_df.filter(pl.col('desc') == 'ML34111')

    if len(hp_data) == 0:
        print("  Warning: No Hire Purchase data found")
        # Return defaults only
        return default_df

    # Sum amount
    total_amount = hp_data['amount'].sum()

    print(f"  Total amount: {total_amount:,.2f}")

    # Create HP records (bucket 06 = > 1 year)
    hp_records = pl.DataFrame([
        {'bnmcode': '93211090600Y', 'amount': total_amount},
        {'bnmcode': '95211090600Y', 'amount': total_amount}
    ])

    # Merge with defaults
    result_df = default_df.join(
        hp_records,
        on='bnmcode',
        how='left',
        suffix='_new'
    ).with_columns(
        pl.coalesce([pl.col('amount_new'), pl.col('amount')]).alias('amount')
    ).select(['bnmcode', 'amount'])

    return result_df


def process_cash_holdings(walk_df):
    """Process Cash Holdings (CASH) from WALK file"""
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


def process_other_assets(walk_df):
    """Process Other Assets (Shares Held) from WALK file"""
    print("\nProcessing Other Assets (Shares Held)...")

    # Filter for F137010SH
    oth_data = walk_df.filter(pl.col('desc') == 'F137010SH')

    if len(oth_data) == 0:
        print("  Warning: No Other Assets data found")
        return pl.DataFrame({
            'bnmcode': [],
            'amount': []
        })

    # Sum amount
    total_amount = oth_data['amount'].sum()

    print(f"  Total amount: {total_amount:,.2f}")

    # Create OTH records (bucket 01 = up to 1 week)
    oth_records = pl.DataFrame([
        {'bnmcode': '93229000100Y', 'amount': total_amount},
        {'bnmcode': '95229000100Y', 'amount': total_amount}
    ])

    return oth_records


def process_fcy_fl(walk_df):
    """Process FCY - Fixed Loan from WALK file"""
    print("\nProcessing FCY - Fixed Loan...")

    # Create default records
    default_df = create_default_records('94211090', '96211090')

    # Filter for F13460081BCB and F13460064FLB
    fcyfl_data = walk_df.filter(
        pl.col('desc').is_in(['F13460081BCB', 'F13460064FLB'])
    )

    if len(fcyfl_data) == 0:
        print("  Warning: No FCY FL data found")
        # Return defaults only
        return default_df

    # Sum amount
    total_amount = fcyfl_data['amount'].sum()

    print(f"  Total amount: {total_amount:,.2f}")

    # Create FCY FL records (bucket 06 = > 1 year)
    fcyfl_records = pl.DataFrame([
        {'bnmcode': '94211090600Y', 'amount': total_amount},
        {'bnmcode': '96211090600Y', 'amount': total_amount}
    ])

    # Merge with defaults
    result_df = default_df.join(
        fcyfl_records,
        on='bnmcode',
        how='left',
        suffix='_new'
    ).with_columns(
        pl.coalesce([pl.col('amount_new'), pl.col('amount')]).alias('amount')
    ).select(['bnmcode', 'amount'])

    return result_df


def process_fcy_rc(walk_df):
    """Process FCY - Revolving Credit from WALK file"""
    print("\nProcessing FCY - Revolving Credit...")

    # Create default records
    default_df = create_default_records('94212090', '96212090')

    # Filter for F134600RC
    fcyrc_data = walk_df.filter(pl.col('desc') == 'F134600RC')

    if len(fcyrc_data) == 0:
        print("  Warning: No FCY RC data found")
        # Return defaults only
        return default_df

    # Sum amount
    total_amount = fcyrc_data['amount'].sum()

    print(f"  Total amount: {total_amount:,.2f}")

    # Create FCY RC records (bucket 06 = > 1 year)
    fcyrc_records = pl.DataFrame([
        {'bnmcode': '94212090600Y', 'amount': total_amount},
        {'bnmcode': '96212090600Y', 'amount': total_amount}
    ])

    # Merge with defaults
    result_df = default_df.join(
        fcyrc_records,
        on='bnmcode',
        how='left',
        suffix='_new'
    ).with_columns(
        pl.coalesce([pl.col('amount_new'), pl.col('amount')]).alias('amount')
    ).select(['bnmcode', 'amount'])

    return result_df


def process_fcy_cash(walk_df):
    """Process FCY - Cash Holdings from WALK file"""
    print("\nProcessing FCY - Cash Holdings...")

    # Create default records
    default_df = create_default_records('94221000', '96221000')

    # Filter for F139610FXNC
    fcycash_data = walk_df.filter(pl.col('desc') == 'F139610FXNC')

    if len(fcycash_data) == 0:
        print("  Warning: No FCY Cash data found")
        # Return defaults only
        return default_df

    # Sum amount
    total_amount = fcycash_data['amount'].sum()

    print(f"  Total amount: {total_amount:,.2f}")

    # Create FCY CASH records (bucket 01 = up to 1 week)
    fcycash_records = pl.DataFrame([
        {'bnmcode': '94221000100Y', 'amount': total_amount},
        {'bnmcode': '96221000100Y', 'amount': total_amount}
    ])

    # Merge with defaults
    result_df = default_df.join(
        fcycash_records,
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
        ('pbif.parquet', 'PBIF'),
        ('bt.parquet', 'BT'),
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

    print("BNM GL Set and Final Consolidation")
    print("=" * 80)

    # Step 1: Get reporting date parameters
    print("\n1. Getting reporting date parameters...")
    params = get_reptdate()
    print(f"   Reporting Date: {params['rdate']}")
    print(f"   Week Number: {params['nowk']}")
    print(f"   Julian Date: {params['rdat1']}")

    # Save REPTDATE to output
    reptdate_output = pl.DataFrame({
        'reptdate': [params['reptdate']]
    })
    reptdate_output.write_parquet(OUTPUT_DIR / "reptdate.parquet")

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

    # Step 3: Process Floor Stocking
    fs_df = process_floor_stocking(walk_df)

    # Step 4: Process Hire Purchase
    hp_df = process_hire_purchase(walk_df)

    # Step 5: Process Cash Holdings
    cash_df = process_cash_holdings(walk_df)

    # Step 6: Process SRR
    srr_df = process_srr(walk_df)

    # Step 7: Process Other Assets
    oth_df = process_other_assets(walk_df)

    # Step 8: Process FCY - Fixed Loan
    fcyfl_df = process_fcy_fl(walk_df)

    # Step 9: Process FCY - Revolving Credit
    fcyrc_df = process_fcy_rc(walk_df)

    # Step 10: Process FCY - Cash Holdings
    fcycash_df = process_fcy_cash(walk_df)

    # Step 11: Combine all GL set results
    print("\n11. Combining all GL set results...")

    all_gl_dfs = []

    if fs_df is not None and len(fs_df) > 0:
        all_gl_dfs.append(fs_df)
        print(f"   - Floor Stocking: {len(fs_df)} records")

    if hp_df is not None and len(hp_df) > 0:
        all_gl_dfs.append(hp_df)
        print(f"   - Hire Purchase: {len(hp_df)} records")

    if cash_df is not None and len(cash_df) > 0:
        all_gl_dfs.append(cash_df)
        print(f"   - Cash Holdings: {len(cash_df)} records")

    if srr_df is not None and len(srr_df) > 0:
        all_gl_dfs.append(srr_df)
        print(f"   - SRR: {len(srr_df)} records")

    if oth_df is not None and len(oth_df) > 0:
        all_gl_dfs.append(oth_df)
        print(f"   - Other Assets: {len(oth_df)} records")

    if fcyfl_df is not None and len(fcyfl_df) > 0:
        all_gl_dfs.append(fcyfl_df)
        print(f"   - FCY FL: {len(fcyfl_df)} records")

    if fcyrc_df is not None and len(fcyrc_df) > 0:
        all_gl_dfs.append(fcyrc_df)
        print(f"   - FCY RC: {len(fcyrc_df)} records")

    if fcycash_df is not None and len(fcycash_df) > 0:
        all_gl_dfs.append(fcycash_df)
        print(f"   - FCY Cash: {len(fcycash_df)} records")

    if all_gl_dfs:
        glset_df = pl.concat(all_gl_dfs)
        print(f"\n   Total GL set records: {len(glset_df)}")

        # Save GL set
        glset_path = BNM_DIR / "glset.parquet"
        glset_df.write_parquet(glset_path)
        print(f"   GL set saved to: {glset_path}")

        # Also save to output
        glset_df.write_parquet(OUTPUT_DIR / "glset.parquet")
    else:
        print("\n   Warning: No GL set data generated")
        glset_df = None

    # Step 12: Consolidate all BNM outputs
    print("\n12. Final consolidation...")
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
        print("\n13. Sample of FINALSUM:")
        print(finalsum_df.head(20))

        # Print summary statistics
        print("\n14. Summary Statistics:")
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
