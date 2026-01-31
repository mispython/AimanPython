#!usr/bin/env python 3
"""
File Name: ERBDRBDP_EIBDRB01
FCY FD Report Generator
Converts SAS program for generating daily total outstanding balance/account on FCY FD & Foreign Companies
"""

import duckdb
import polars as pl
from datetime import datetime
from pathlib import Path


# ============================================================================
# CONFIGURATION - Define all paths early
# ============================================================================

# Input paths
INPUT_REPTDATE = "data/DEPO/REPTDATE.parquet"
INPUT_FD = "data/DEPO/FD.parquet"
INPUT_WKDTL_TEMPLATE = "data/WALK/WKDTL{yy}{mon}{day}.parquet"

# Output paths
OUTPUT_STORE_DIR = Path("data/STORE")
OUTPUT_STORE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_REPORT = "output/EIBDRB01_report.txt"
Path(OUTPUT_REPORT).parent.mkdir(parents=True, exist_ok=True)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def parse_date_from_numeric(date_num):
    """
    Parse date from numeric format (MMDDYYYY or similar)
    Returns datetime object or None
    """
    if date_num is None or date_num == 0:
        return None

    date_str = str(int(date_num)).zfill(11)[:8]
    try:
        return datetime.strptime(date_str, '%m%d%Y')
    except:
        return None


def format_date_ddmmyy(dt):
    """Format datetime as DD/MM/YY"""
    if dt is None:
        return ""
    return dt.strftime('%d/%m/%y')


# ============================================================================
# STEP 1: Read REPTDATE and create macro variables
# ============================================================================

print("Step 1: Reading REPTDATE...")

con = duckdb.connect()

# Read REPTDATE
reptdate_df = con.execute(f"SELECT * FROM '{INPUT_REPTDATE}'").pl()

# Extract the first (and typically only) REPTDATE value
reptdate_row = reptdate_df.row(0, named=True)
reptdate = reptdate_row['REPTDATE']

# Convert to datetime if needed
if isinstance(reptdate, (int, float)):
    # Assume SAS date (days since 1960-01-01)
    base_date = datetime(1960, 1, 1)
    from datetime import timedelta
    reptdate_dt = base_date + timedelta(days=int(reptdate))
else:
    reptdate_dt = reptdate

# Create macro variable equivalents
REPTYEAR = reptdate_dt.year
REPTYY = str(reptdate_dt.year)[2:4]
REPTMON = f"{reptdate_dt.month:02d}"
REPTDAY = f"{reptdate_dt.day:02d}"
REPTDATE = int(reptdate)  # Keep original numeric value
RDATE = format_date_ddmmyy(reptdate_dt)

print(f"  REPTDATE: {REPTDATE}")
print(f"  Report Date: {RDATE}")
print(f"  Year: {REPTYEAR}, Month: {REPTMON}, Day: {REPTDAY}")


# ============================================================================
# STEP 2: Process FCY data from FD file
# ============================================================================

print("\nStep 2: Processing FCY data from FD...")

# Read FD file and filter
fd_query = f"""
SELECT *
FROM '{INPUT_FD}'
WHERE CURCODE != 'MYR' AND CURBAL >= 0
"""

fcy_df = con.execute(fd_query).pl()

# Add computed columns
fcy_df = fcy_df.with_columns([
    pl.lit(3).alias('ID'),
    pl.lit(REPTDATE).alias('REPTDATE'),
    (pl.col('CURBAL') / 1000).alias('CURBAL')
])

# Parse OPENDT
def parse_opendt(opendt):
    if opendt is None or opendt == 0:
        return None
    return parse_date_from_numeric(opendt)

def parse_closedt(closedt):
    if closedt is None or closedt == 0:
        return None
    return parse_date_from_numeric(closedt)

# Process dates using apply (converting to pandas for complex operations, then back to polars)
fcy_pd = fcy_df.to_pandas()

# Parse dates
fcy_pd['OPENDT_PARSED'] = fcy_pd['OPENDT'].apply(parse_opendt)
fcy_pd['CLOSEDT_PARSED'] = fcy_pd['CLOSEDT'].apply(parse_closedt)

# Calculate OPCLMH flag
fcy_pd['OPCLMH'] = 0
mask_open = (fcy_pd['OPENDT_PARSED'].notna()) & \
            (fcy_pd['OPENDT_PARSED'].apply(lambda x: x.year if x else None) == REPTYEAR) & \
            (fcy_pd['OPENDT_PARSED'].apply(lambda x: x.month if x else None) == int(REPTMON))

mask_close = (fcy_pd['CLOSEDT_PARSED'].notna()) & \
             (fcy_pd['CLOSEDT_PARSED'].apply(lambda x: x.year if x else None) == REPTYEAR) & \
             (fcy_pd['CLOSEDT_PARSED'].apply(lambda x: x.month if x else None) == int(REPTMON))

fcy_pd.loc[mask_open & mask_close, 'OPCLMH'] = 1

# Calculate OSACCT flag
fcy_pd['OSACCT'] = 0
if 'OPENIND' in fcy_pd.columns:
    fcy_pd.loc[~fcy_pd['OPENIND'].isin(['B', 'C', 'P']), 'OSACCT'] = 1

# Calculate NOACCT flag
fcy_pd['NOACCT'] = 0
mask_noacct = (fcy_pd['OPCLMH'] != 1) & (fcy_pd['CURBAL'] > 0)
fcy_pd.loc[mask_noacct, 'NOACCT'] = 1

# Update OPENDT and CLOSEDT with parsed values (convert back to numeric for consistency)
# For simplicity, we'll keep the parsed datetime objects
fcy_pd['OPENDT'] = fcy_pd['OPENDT_PARSED']
fcy_pd['CLOSEDT'] = fcy_pd['CLOSEDT_PARSED']
fcy_pd = fcy_pd.drop(columns=['OPENDT_PARSED', 'CLOSEDT_PARSED'])

# Convert back to polars
fcy_df = pl.from_pandas(fcy_pd)

print(f"  FCY records: {len(fcy_df)}")


# ============================================================================
# STEP 3: Process WK (Walk) data
# ============================================================================

print("\nStep 3: Processing WK data...")

# Build WKDTL filename
wkdtl_file = INPUT_WKDTL_TEMPLATE.format(yy=REPTYY, mon=REPTMON, day=REPTDAY)

try:
    wk_df = con.execute(f"SELECT * FROM '{wkdtl_file}'").pl()

    # Process WK data
    wk_df = wk_df.with_columns([
        pl.lit(2).alias('ID'),
        pl.lit(REPTDATE).alias('REPTDATE'),
        (-1 * pl.col('CURBAL') / 1000).alias('CURBAL')
    ])

    # Extract CURCODE from CURR (last 3 characters)
    wk_df = wk_df.with_columns([
        pl.col('CURR').str.slice(3, 3).alias('CURCODE')
    ])

    print(f"  WK records: {len(wk_df)}")

    # Combine FCY and WK
    # Select common columns
    common_cols = ['REPTDATE', 'ID', 'CURCODE', 'CURBAL', 'NOACCT', 'OSACCT']

    # Ensure NOACCT and OSACCT exist in wk_df
    if 'NOACCT' not in wk_df.columns:
        wk_df = wk_df.with_columns(pl.lit(0).alias('NOACCT'))
    if 'OSACCT' not in wk_df.columns:
        wk_df = wk_df.with_columns(pl.lit(0).alias('OSACCT'))

    fcy_subset = fcy_df.select(common_cols)
    wk_subset = wk_df.select(common_cols)

    fcy_combined = pl.concat([fcy_subset, wk_subset])

except Exception as e:
    print(f"  Warning: Could not read WK file {wkdtl_file}: {e}")
    print("  Continuing with FCY data only...")
    common_cols = ['REPTDATE', 'ID', 'CURCODE', 'CURBAL', 'NOACCT', 'OSACCT']
    fcy_combined = fcy_df.select(common_cols)

print(f"  Combined records: {len(fcy_combined)}")


# ============================================================================
# STEP 4: Summarize by REPTDATE, ID, CURCODE
# ============================================================================

print("\nStep 4: Summarizing data...")

fcy_summary = fcy_combined.group_by(['REPTDATE', 'ID', 'CURCODE']).agg([
    pl.col('CURBAL').sum().alias('CURBAL'),
    pl.col('NOACCT').sum().alias('NOACCT'),
    pl.col('OSACCT').sum().alias('OSACCT')
]).sort(['REPTDATE', 'ID', 'CURCODE'])

# Convert CURBAL to millions and round
fcy_summary = fcy_summary.with_columns([
    (pl.col('CURBAL') / 1000).round(3).alias('CURBAL')
])

# Duplicate rows: original ID and ID=1 (for totals)
fcy_expanded = pl.concat([
    fcy_summary,
    fcy_summary.with_columns(pl.lit(1).alias('ID'))
]).sort(['REPTDATE', 'ID', 'CURCODE'])

print(f"  Summary records (expanded): {len(fcy_expanded)}")


# ============================================================================
# STEP 5: Calculate totals by REPTDATE and ID
# ============================================================================

print("\nStep 5: Calculating totals...")

total_df = fcy_expanded.group_by(['REPTDATE', 'ID']).agg([
    pl.col('CURBAL').sum().alias('TOTFCYFD'),
    pl.col('NOACCT').sum().alias('NOACCT'),
    pl.col('OSACCT').sum().alias('OSACCT')
])

print(f"  Total records: {len(total_df)}")


# ============================================================================
# STEP 6: Transpose currency data (pivot)
# ============================================================================

print("\nStep 6: Transposing currency data...")

# Re-summarize for pivot (without the ID=1 duplicates for currency columns)
fcy_for_pivot = fcy_combined.group_by(['REPTDATE', 'ID', 'CURCODE']).agg([
    pl.col('CURBAL').sum().alias('CURBAL')
])

fcy_for_pivot = fcy_for_pivot.with_columns([
    (pl.col('CURBAL') / 1000).round(3).alias('CURBAL')
])

# Duplicate for ID=1
fcy_for_pivot = pl.concat([
    fcy_for_pivot,
    fcy_for_pivot.with_columns(pl.lit(1).alias('ID'))
])

# Pivot to wide format
fd_pivot = fcy_for_pivot.pivot(
    values='CURBAL',
    index=['REPTDATE', 'ID'],
    columns='CURCODE',
    aggregate_function='sum'
)

print(f"  Pivoted records: {len(fd_pivot)}")


# ============================================================================
# STEP 7: Merge pivoted data with totals
# ============================================================================

print("\nStep 7: Merging data...")

rbdp = fd_pivot.join(total_df, on=['REPTDATE', 'ID'], how='inner')

# Fill missing currency columns with 0
currency_cols = ['USD', 'NZD', 'AUD', 'GBP', 'HKD', 'SGD', 'EUR', 'JPY', 'CAD', 'CNY', 'CHF', 'THB']

for curr in currency_cols:
    if curr not in rbdp.columns:
        rbdp = rbdp.with_columns(pl.lit(0.0).alias(curr))
    else:
        rbdp = rbdp.with_columns(pl.col(curr).fill_null(0.0))

print(f"  Final RBDP records: {len(rbdp)}")


# ============================================================================
# STEP 8: Save to STORE dataset (append logic)
# ============================================================================

print("\nStep 8: Saving to STORE...")

store_file = OUTPUT_STORE_DIR / f"RB01DP{REPTMON}.parquet"

if REPTDAY == "01":
    # First day of month - create new file
    rbdp.write_parquet(store_file)
    print(f"  Created new file: {store_file}")
else:
    # Append and remove duplicates
    if store_file.exists():
        existing_df = pl.read_parquet(store_file)
        combined_df = pl.concat([rbdp, existing_df])
        # Remove duplicates by REPTDATE and ID
        combined_df = combined_df.unique(subset=['REPTDATE', 'ID'], keep='first')
        combined_df.write_parquet(store_file)
        print(f"  Appended to existing file: {store_file}")
    else:
        rbdp.write_parquet(store_file)
        print(f"  Created new file: {store_file}")


# ============================================================================
# STEP 9: Generate report with ASA carriage control
# ============================================================================

print("\nStep 9: Generating report...")

# Read the stored data
report_df = pl.read_parquet(store_file).sort(['REPTDATE', 'ID'])

# Convert to pandas for easier formatting
report_pd = report_df.to_pandas()

# Open output file with ASA carriage control
def generate_report(report_pd, OUTPUT_REPORT, RDATE):
    with open(OUTPUT_REPORT, 'w') as f:
        line_count = 0
        page_length = 60

        def write_line(line, carriage_control=' '):
            nonlocal line_count
            f.write(f"{carriage_control}{line}\n")
            line_count += 1
            if line_count >= page_length:
                line_count = 0

        # Write header (only once at start)
        write_line('REPORT ID : EIBDRB01', ' ')
        write_line('DAILY TOTAL OUTSTANDING BALANCE/ACCOUNT ON FCY FD & FOREIGN COMPANIES', ' ')
        write_line(f'AS AT {RDATE}', ' ')
        write_line(' ', ' ')

        # Column headers
        header1 = ' DATE' + ' ' * 54 + "OUTSTANDING AMOUNT (RM'MIL) (3-decimal)"
        header1 += ' ' * (212 - len(header1)) + 'TOTAL NO OF O/S ACCT'
        write_line(header1, ' ')

        header2 = ' ' * 14 + 'USD' + ' ' * 12 + 'NZD' + ' ' * 12 + 'AUD' + ' ' * 12 + 'GBP' + ' ' * 12 + 'HKD'
        header2 += ' ' * 12 + 'SGD' + ' ' * 12 + 'EUR' + ' ' * 12 + 'JPY' + ' ' * 12 + 'CAD' + ' ' * 12 + 'CNY'
        header2 += ' ' * 12 + 'CHF' + ' ' * 12 + 'THB' + ' ' * 12 + 'TOT AMT O/S RM'
        header2 += ' ' * (212 - len(header2)) + 'NO OF A/C' + ' ' * 14 + 'NO OF A/C'
        write_line(header2, ' ')

        header3 = ' (A)+(B)'
        header3 += ' ' * 13 + 'TOTAL' + ' ' * 10 + 'TOTAL' + ' ' * 10 + 'TOTAL' + ' ' * 10 + 'TOTAL'
        header3 += ' ' * 10 + 'TOTAL' + ' ' * 10 + 'TOTAL' + ' ' * 10 + 'TOTAL' + ' ' * 10 + 'TOTAL'
        header3 += ' ' * 10 + 'TOTAL' + ' ' * 10 + 'TOTAL' + ' ' * 10 + 'TOTAL' + ' ' * 10 + 'TOTAL'
        header3 += ' ' * 10 + 'TOTAL' + ' ' * (212 - len(header3)) + '(EXCL' + ' ' * 14 + '(INCL'
        write_line(header3, ' ')

        header4 = '   (A)'
        for _ in range(13):
            header4 += ' ' * 11 + 'FRGN CO.'
        header4 += ' ' * (212 - len(header4)) + 'ZERO' + ' ' * 15 + 'ZERO'
        write_line(header4, ' ')

        header5 = '   (B)'
        for _ in range(13):
            header5 += ' ' * 11 + 'FCY FD'
        header5 += ' ' * (212 - len(header5)) + 'BALANCE)' + ' ' * 11 + 'BALANCE)'
        write_line(header5, ' ')

        write_line('-' * 275, ' ')

        # Write data rows
        for idx, row in report_pd.iterrows():
            rpdate = ''
            if row['ID'] == 1:
                # Format date for display
                dt = datetime(1960, 1, 1) + timedelta(days=int(row['REPTDATE']))
                rpdate = format_date_ddmmyy(dt)

            # Format numbers
            def fmt_curr(val):
                if val is None or (isinstance(val, float) and val == 0.0):
                    return '0.000'
                return f"{val:,.3f}".replace(',', '_').replace('.', ',').replace('_', '.')

            def fmt_int(val):
                if val is None or val == 0:
                    return '0'
                return f"{int(val):,}".replace(',', '.')

            line = f' {rpdate:8s}'
            line += f'{fmt_curr(row["USD"]):>15s}'
            line += f'{fmt_curr(row["NZD"]):>15s}'
            line += f'{fmt_curr(row["AUD"]):>15s}'
            line += f'{fmt_curr(row["GBP"]):>15s}'
            line += f'{fmt_curr(row["HKD"]):>15s}'
            line += f'{fmt_curr(row["SGD"]):>15s}'
            line += f'{fmt_curr(row["EUR"]):>15s}'
            line += f'{fmt_curr(row["JPY"]):>15s}'
            line += f'{fmt_curr(row["CAD"]):>15s}'
            line += f'{fmt_curr(row["CNY"]):>15s}'
            line += f'{fmt_curr(row["CHF"]):>15s}'
            line += f'{fmt_curr(row["THB"]):>15s}'
            line += f'{fmt_curr(row["TOTFCYFD"]):>18s}'
            line += f'{fmt_int(row["NOACCT"]):>14s}'
            line += f'{fmt_int(row["OSACCT"]):>14s}'

            write_line(line, ' ')

            if row['ID'] == 3:
                write_line('-' * 275, ' ')

print(f"\nReport generated: {OUTPUT_REPORT}")
print("\nConversion complete!")
print(f"Store file: {store_file}")
print(f"Report file: {OUTPUT_REPORT}")
