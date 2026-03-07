#!usr/bin/env python 3
"""
File Name: ERBDRBDP_EIBDRB02
FD Withdrawal Summary Report Generator
Generates daily summary reports on reasons for FD withdrawals (PBB & PIBB)
Produces two reports: RM Fixed Deposit and FCY Fixed Deposit
"""

import duckdb
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path


# ============================================================================
# CONFIGURATION - Define all paths early
# ============================================================================

# Input paths
INPUT_REPTDATE = "data/DEPOSIT/REPTDATE.parquet"
INPUT_FDWDRW_TEMPLATE = "data/MIS/FDWDRW{mon}.parquet"

# Format lookup files (for BRCHCD and REGNEW formats)
# These would typically be separate parquet files containing format mappings
FORMAT_BRCHCD = "data/formats/BRCHCD.parquet"  # BRANCH -> BRABBR mapping
FORMAT_REGNEW = "data/formats/REGNEW.parquet"  # BRANCH -> REGION mapping

# Output paths
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_RMWDRAW = OUTPUT_DIR / "RMWDRAW.txt"
OUTPUT_FCYWDRAW = OUTPUT_DIR / "FCYWDRAW.txt"


# ============================================================================
# FORMAT MAPPINGS (PBBELF include file simulation)
# ============================================================================

def load_format_mappings():
    """
    Load format mappings for BRANCH codes
    Returns dictionaries for BRCHCD and REGNEW formats
    """
    brchcd_map = {}
    regnew_map = {}

    try:
        # Try to load from parquet files
        con = duckdb.connect()

        if Path(FORMAT_BRCHCD).exists():
            brchcd_df = con.execute(f"SELECT * FROM '{FORMAT_BRCHCD}'").pl()
            brchcd_map = dict(zip(brchcd_df['BRANCH'], brchcd_df['BRABBR']))

        if Path(FORMAT_REGNEW).exists():
            regnew_df = con.execute(f"SELECT * FROM '{FORMAT_REGNEW}'").pl()
            regnew_map = dict(zip(regnew_df['BRANCH'], regnew_df['REGION']))

    except Exception as e:
        print(f"Warning: Could not load format files: {e}")
        print("Using default mappings...")

    # Return maps (will use default formatting if empty)
    return brchcd_map, regnew_map


# Load format mappings
BRCHCD_MAP, REGNEW_MAP = load_format_mappings()


def apply_brchcd_format(branch):
    """Apply BRCHCD format to branch code"""
    if branch in BRCHCD_MAP:
        return BRCHCD_MAP[branch]
    # Default: return branch code as string
    return str(branch) if branch else ""


def apply_regnew_format(branch):
    """Apply REGNEW format to branch code"""
    if branch in REGNEW_MAP:
        return REGNEW_MAP[branch]
    # Default: return empty or branch-based region
    return ""


# ============================================================================
# STEP 1: Read REPTDATE and create macro variables
# ============================================================================

print("Step 1: Reading REPTDATE...")

con = duckdb.connect()

# Read REPTDATE
reptdate_df = con.execute(f"SELECT * FROM '{INPUT_REPTDATE}'").pl()

# Extract the first REPTDATE value
reptdate_row = reptdate_df.row(0, named=True)
reptdate = reptdate_row['REPTDATE']

# Convert to datetime if needed (SAS date = days since 1960-01-01)
if isinstance(reptdate, (int, float)):
    base_date = datetime(1960, 1, 1)
    reptdate_dt = base_date + timedelta(days=int(reptdate))
else:
    reptdate_dt = reptdate

# Create macro variable equivalents
REPTYEAR = reptdate_dt.year
REPTMON = f"{reptdate_dt.month:02d}"
RPTDATE = int(reptdate)  # Keep original numeric value
RDATE = reptdate_dt.strftime('%d/%m/%y')

print(f"  REPTDATE: {RPTDATE}")
print(f"  Report Date: {RDATE}")
print(f"  Year: {REPTYEAR}, Month: {REPTMON}")


# ============================================================================
# STEP 2: Read and filter withdrawal data
# ============================================================================

print("\nStep 2: Reading withdrawal data...")

# Read FDWDRW file for the reporting month
fdwdrw_file = INPUT_FDWDRW_TEMPLATE.format(mon=REPTMON)

wdraw_df = con.execute(f"""
    SELECT *
    FROM '{fdwdrw_file}'
    WHERE REPTDATE = {RPTDATE}
""").pl()

print(f"  Total withdrawal records: {len(wdraw_df)}")

# Add TYPE, BRABBR, and REGION columns
wdraw_df = wdraw_df.with_columns([
    pl.lit(1).alias('TYPE')
])

# Apply format mappings
wdraw_pd = wdraw_df.to_pandas()
wdraw_pd['BRABBR'] = wdraw_pd['BRANCH'].apply(apply_brchcd_format)
wdraw_pd['REGION'] = wdraw_pd['BRANCH'].apply(apply_regnew_format)
wdraw_df = pl.from_pandas(wdraw_pd)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def group_withdrawals(wdraw_df, currency_filter='MYR', exclude_prodcd_394=True):
    """
    Group withdrawal data into 4 categories:
    1. Corporate, Individual customers (77,78,95,96) - PBB
    2. Individual, Individual customers (77,78,95,96) - PIBB
    3. Corporate, Non-individual customers - PBB
    4. Individual, Non-individual customers - PIBB

    Args:
        currency_filter: 'MYR' or 'FCY'
        exclude_prodcd_394: Whether to exclude PRODCD 394 (RM only)
    """
    # Filter by currency
    if currency_filter == 'MYR':
        filtered_df = wdraw_df.filter(pl.col('CURCODE') == 'MYR')
        if exclude_prodcd_394:
            filtered_df = filtered_df.filter(pl.col('PRODCD') != 394)
    else:  # FCY
        filtered_df = wdraw_df.filter(pl.col('CURCODE') != 'MYR')

    # Group 1: ACCTTYPE='C' and CUSTCODE in (77,78,95,96)
    wdraw1 = filtered_df.filter(
        (pl.col('ACCTTYPE') == 'C') &
        (pl.col('CUSTCODE').is_in([77, 78, 95, 96]))
    )

    # Group 2: ACCTTYPE='I' and CUSTCODE in (77,78,95,96)
    wdraw2 = filtered_df.filter(
        (pl.col('ACCTTYPE') == 'I') &
        (pl.col('CUSTCODE').is_in([77, 78, 95, 96]))
    )

    # Group 3: ACCTTYPE='C' and CUSTCODE not in (77,78,95,96)
    wdraw3 = filtered_df.filter(
        (pl.col('ACCTTYPE') == 'C') &
        (~pl.col('CUSTCODE').is_in([77, 78, 95, 96]))
    )

    # Group 4: ACCTTYPE='I' and CUSTCODE not in (77,78,95,96)
    wdraw4 = filtered_df.filter(
        (pl.col('ACCTTYPE') == 'I') &
        (~pl.col('CUSTCODE').is_in([77, 78, 95, 96]))
    )

    return wdraw1, wdraw2, wdraw3, wdraw4


def process_group(wdraw_group):
    """
    Process a single withdrawal group:
    - Aggregate by BRANCH and RSONCODE
    - Create columns C1-C16 (counts) and A1-A16 (amounts) for reason codes W01-W16
    - Calculate totals
    """
    if len(wdraw_group) == 0:
        # Return empty dataframe with required structure
        return pl.DataFrame()

    # Convert to pandas for easier aggregation
    df = wdraw_group.to_pandas()

    # Initialize result list
    results = []

    # Group by BRANCH
    for branch, branch_data in df.groupby('BRANCH'):
        row = {
            'BRANCH': branch,
            'BRABBR': branch_data['BRABBR'].iloc[0],
            'REGION': branch_data['REGION'].iloc[0],
            'TYPE': branch_data['TYPE'].iloc[0]
        }

        # Initialize count and amount columns for W01-W16
        for i in range(1, 17):
            row[f'C{i}'] = 0
            row[f'A{i}'] = 0.0

        row['TOT_CNT'] = 0
        row['TOT_AMT'] = 0.0

        # Process each transaction
        for _, txn in branch_data.iterrows():
            rsoncode = str(txn['RSONCODE']).strip()
            tranamt = txn['TRANAMT'] if 'TRANAMT' in txn else 0

            # Check if RSONCODE matches W01-W16 pattern
            for i in range(1, 17):
                if rsoncode == f'W{i:02d}':
                    row[f'C{i}'] += 1
                    row[f'A{i}'] += tranamt
                    break

            row['TOT_CNT'] += 1
            row['TOT_AMT'] += tranamt

        results.append(row)

    if not results:
        return pl.DataFrame()

    # Convert to polars
    result_df = pl.from_pandas(pl.DataFrame(results).to_pandas())
    return result_df


def calculate_totals(fdraw_df):
    """
    Calculate summary totals and percentages for a processed group
    """
    if len(fdraw_df) == 0:
        return pl.DataFrame()

    # Convert to pandas for aggregation
    df = fdraw_df.to_pandas()

    # Sum all numeric columns
    totals = {}

    for i in range(1, 17):
        totals[f'RC{i}'] = df[f'C{i}'].sum()
        totals[f'RA{i}'] = df[f'A{i}'].sum()

    totals['GTC'] = df['TOT_CNT'].sum()  # Grand Total Count
    totals['GTA'] = df['TOT_AMT'].sum()  # Grand Total Amount

    # Calculate percentages
    for i in range(1, 17):
        if totals['GTC'] > 0:
            totals[f'PC{i}'] = (totals[f'RC{i}'] / totals['GTC']) * 100
        else:
            totals[f'PC{i}'] = 0

        if totals['GTA'] > 0:
            totals[f'PA{i}'] = (totals[f'RA{i}'] / totals['GTA']) * 100
        else:
            totals[f'PA{i}'] = 0

    # Calculate PGTC and PGTA (should sum to 100)
    totals['PGTC'] = sum(totals[f'PC{i}'] for i in range(1, 17))
    totals['PGTA'] = sum(totals[f'PA{i}'] for i in range(1, 17))

    # Convert to DataFrame
    total_df = pl.DataFrame([totals])
    return total_df


# ============================================================================
# REPORT GENERATION FUNCTION
# ============================================================================

def generate_report(fdraw_groups, total_groups, output_file, report_title, headers):
    """
    Generate formatted report with ASA carriage control

    Args:
        fdraw_groups: List of 4 processed dataframes (one per group)
        total_groups: List of 4 total dataframes
        output_file: Path to output file
        report_title: Title for the report
        headers: List of 4 headers for each group
    """
    with open(output_file, 'w') as f:
        line_count = 0
        page_length = 60

        def write_line(line, carriage_control=' '):
            """Write line with ASA carriage control character"""
            nonlocal line_count
            f.write(f"{carriage_control}{line}\n")
            line_count += 1
            if line_count >= page_length:
                line_count = 0

        # Write main header (only once)
        write_line('REPORT ID : EIBDRB01', ' ')
        write_line(
            f'TITLE : DAILY SUMMARY REPORT ON REASONS FOR {report_title} OVER-THE-COUNTER BASED ON RECEIPTS BY BRANCH',
            ' ')
        write_line(f'REPORTING DATE : {RDATE}', ' ')
        write_line(' ', ' ')

        # Process each of the 4 groups
        for j in range(4):
            fdraw_df = fdraw_groups[j]
            total_df = total_groups[j]

            # Section headers
            if j == 0:
                write_line('(A) INDIVIDUAL CUSTOMER (CUSTOMER CODE: 77,78,95 AND 96)', ' ')
            elif j == 2:
                write_line(' ', ' ')
                write_line('(B) NON-INDIVIDUAL CUSTOMER', ' ')

            # Skip if no data
            if len(fdraw_df) == 0:
                continue

            # Write group header
            write_line(' ', ' ')
            write_line(headers[j], ' ')

            # Column headers - Line 1
            header_line1 = 'BRCH;BRCH;REGION;;;;;;;;;;;;;;;BY REASON CODE'
            write_line(header_line1, ' ')

            # Column headers - Line 2
            header_line2 = 'CODE;ABBR;;'
            for i in range(1, 17):
                header_line2 += f'W{i:02d};W{i:02d};'
            header_line2 += 'TOTAL;TOTAL'
            write_line(header_line2, ' ')

            # Column headers - Line 3
            header_line3 = ';;;'
            for i in range(1, 17):
                header_line3 += 'NO.;RM;'
            header_line3 += 'NO.;RM'
            write_line(header_line3, ' ')

            # Write data rows
            fdraw_pd = fdraw_df.to_pandas()
            for _, row in fdraw_pd.iterrows():
                data_line = f"{int(row['BRANCH'])};{row['BRABBR']};{row['REGION']};{int(row['C1'])};{int(row['A1'])}"

                for i in range(2, 17):
                    data_line += f";{int(row[f'C{i}'])};{int(row[f'A{i}'])}"

                data_line += f";{int(row['TOT_CNT'])};{int(row['TOT_AMT'])}"
                write_line(data_line, ' ')

            # Write totals
            if len(total_df) > 0:
                total_pd = total_df.to_pandas()
                total_row = total_pd.iloc[0]

                # Total line
                total_line = ';TOTAL;;'
                for i in range(1, 17):
                    total_line += f"{int(total_row[f'RC{i}'])};{int(total_row[f'RA{i}'])};"
                total_line += f"{int(total_row['GTC'])};{int(total_row['GTA'])}"
                write_line(total_line, ' ')

                # Percentage composition line
                pct_line = ';% COMPOSITION;;'
                for i in range(1, 17):
                    pct_line += f"{total_row[f'PC{i}']:.0f};{total_row[f'PA{i}']:.0f};"
                pct_line += f"{total_row['PGTC']:.0f};{total_row['PGTA']:.0f}"
                write_line(pct_line, ' ')


# ============================================================================
# MAIN PROCESSING - RM FIXED DEPOSIT
# ============================================================================

print("\n" + "=" * 80)
print("PROCESSING RM FIXED DEPOSIT WITHDRAWALS")
print("=" * 80)

# Group data for RM
print("\nGrouping RM withdrawal data...")
rm_wdraw1, rm_wdraw2, rm_wdraw3, rm_wdraw4 = group_withdrawals(
    wdraw_df,
    currency_filter='MYR',
    exclude_prodcd_394=True
)

print(f"  Group 1 (PBB Individual): {len(rm_wdraw1)} records")
print(f"  Group 2 (PIBB Individual): {len(rm_wdraw2)} records")
print(f"  Group 3 (PBB Non-Individual): {len(rm_wdraw3)} records")
print(f"  Group 4 (PIBB Non-Individual): {len(rm_wdraw4)} records")

# Process each group
print("\nProcessing RM groups...")
rm_fdraw1 = process_group(rm_wdraw1)
rm_fdraw2 = process_group(rm_wdraw2)
rm_fdraw3 = process_group(rm_wdraw3)
rm_fdraw4 = process_group(rm_wdraw4)

# Calculate totals
print("Calculating RM totals...")
rm_total1 = calculate_totals(rm_fdraw1)
rm_total2 = calculate_totals(rm_fdraw2)
rm_total3 = calculate_totals(rm_fdraw3)
rm_total4 = calculate_totals(rm_fdraw4)

# Generate RM report
print(f"\nGenerating RM report: {OUTPUT_RMWDRAW}")
rm_headers = [
    '(I) PBB',
    '(II) PIBB',
    '(I) PBB',
    '(II) PIBB'
]

generate_report(
    [rm_fdraw1, rm_fdraw2, rm_fdraw3, rm_fdraw4],
    [rm_total1, rm_total2, rm_total3, rm_total4],
    OUTPUT_RMWDRAW,
    'FD WITHDRAWALS (PBB&PIBB)',
    rm_headers
)

print(f"RM report generated: {OUTPUT_RMWDRAW}")


# ============================================================================
# MAIN PROCESSING - FCY FIXED DEPOSIT
# ============================================================================

print("\n" + "=" * 80)
print("PROCESSING FCY FIXED DEPOSIT WITHDRAWALS")
print("=" * 80)

# Group data for FCY
print("\nGrouping FCY withdrawal data...")
fcy_wdraw1, fcy_wdraw2, fcy_wdraw3, fcy_wdraw4 = group_withdrawals(
    wdraw_df,
    currency_filter='FCY',
    exclude_prodcd_394=False
)

print(f"  Group 1 (PBB Individual): {len(fcy_wdraw1)} records")
print(f"  Group 2 (PIBB Individual): {len(fcy_wdraw2)} records")
print(f"  Group 3 (PBB Non-Individual): {len(fcy_wdraw3)} records")
print(f"  Group 4 (PIBB Non-Individual): {len(fcy_wdraw4)} records")

# Process each group
print("\nProcessing FCY groups...")
fcy_fdraw1 = process_group(fcy_wdraw1)
fcy_fdraw2 = process_group(fcy_wdraw2)
fcy_fdraw3 = process_group(fcy_wdraw3)
fcy_fdraw4 = process_group(fcy_wdraw4)

# Calculate totals
print("Calculating FCY totals...")
fcy_total1 = calculate_totals(fcy_fdraw1)
fcy_total2 = calculate_totals(fcy_fdraw2)
fcy_total3 = calculate_totals(fcy_fdraw3)
fcy_total4 = calculate_totals(fcy_fdraw4)

# Generate FCY report
print(f"\nGenerating FCY report: {OUTPUT_FCYWDRAW}")
fcy_headers = [
    '(I) PBB',
    '(II) PIBB',
    '(I) PBB',
    '(II) PIBB'
]

generate_report(
    [fcy_fdraw1, fcy_fdraw2, fcy_fdraw3, fcy_fdraw4],
    [fcy_total1, fcy_total2, fcy_total3, fcy_total4],
    OUTPUT_FCYWDRAW,
    'FCY FD WITHDRAWALS',
    fcy_headers
)

print(f"FCY report generated: {OUTPUT_FCYWDRAW}")


# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 80)
print("CONVERSION COMPLETE")
print("=" * 80)
print(f"\nReports generated:")
print(f"  1. RM Fixed Deposit: {OUTPUT_RMWDRAW}")
print(f"  2. FCY Fixed Deposit: {OUTPUT_FCYWDRAW}")
print(f"\nReporting Date: {RDATE}")
