#!usr/bin/env python 3
"""
File Name: EIIMRPTC_EIIMDPN1
Islamic Bank Savings Account Opened/Closed Report
Generates monthly report on savings account activity with cumulative tracking
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
INPUT_SAVING = "data/DEPOSIT/SAVING.parquet"
INPUT_SAVGF_TEMPLATE = "data/MIS/SAVGF{mon}.parquet"

# Output paths
OUTPUT_DIR_MIS = Path("data/MIS")
OUTPUT_DIR_MIS.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_SAVGC_TEMPLATE = OUTPUT_DIR_MIS / "SAVGC{mon}.parquet"
OUTPUT_SAVGF_TEMPLATE = OUTPUT_DIR_MIS / "SAVGF{mon}.parquet"
OUTPUT_REPORT = OUTPUT_DIR / "SAVINGS_ACCOUNT_REPORT.txt"

# Report settings
PAGE_LENGTH = 60
ASA_SPACE = ' '
ASA_NEW_PAGE = '1'


# ============================================================================
# STEP 1: Read REPTDATE and create macro variables
# ============================================================================

print("Step 1: Reading REPTDATE...")

con = duckdb.connect()

reptdate_df = con.execute(f"SELECT * FROM '{INPUT_REPTDATE}'").pl()
reptdate_row = reptdate_df.row(0, named=True)
reptdate = reptdate_row['REPTDATE']

# Convert to datetime (SAS date = days since 1960-01-01)
if isinstance(reptdate, (int, float)):
    base_date = datetime(1960, 1, 1)
    reptdate_dt = base_date + timedelta(days=int(reptdate))
else:
    reptdate_dt = reptdate

# Create macro variable equivalents
MM = reptdate_dt.month
MM1 = MM - 1 if MM > 1 else 12

RDATE = reptdate_dt.strftime('%d/%m/%y')
RYEAR = reptdate_dt.year
RMONTH = reptdate_dt.month
REPTMON = f"{MM:02d}"
REPTMON1 = f"{MM1:02d}"
RDAY = reptdate_dt.day

print(f"  Report Date: {RDATE}")
print(f"  Report Month: {REPTMON}, Previous Month: {REPTMON1}")
print(f"  Year: {RYEAR}, Month: {RMONTH}, Day: {RDAY}")


# ============================================================================
# STEP 2: Load and process SAVING data
# ============================================================================

print("\nStep 2: Loading and processing SAVING data...")

# Load SAVING data with product filters
saving_query = f"""
SELECT *
FROM '{INPUT_SAVING}'
WHERE ((PRODUCT BETWEEN 200 AND 207) OR
       (PRODUCT BETWEEN 212 AND 216) OR
       (PRODUCT BETWEEN 220 AND 223) OR
       PRODUCT = 218)
  AND BRANCH != 227
"""

savg_df = con.execute(saving_query).pl()

print(f"  Initial SAVING records: {len(savg_df)}")

# Branch remapping: 250 -> 092
savg_df = savg_df.with_columns([
    pl.when(pl.col('BRANCH') == 250)
    .then(pl.lit(92))
    .otherwise(pl.col('BRANCH'))
    .alias('BRANCH')
])

# Process OPENIND and CLOSEMH
savg_pd = savg_df.to_pandas()

# Handle OPENIND='Z' -> 'O' and CLOSEMH=0
mask_z = savg_pd['OPENIND'] == 'Z'
savg_pd.loc[mask_z, 'OPENIND'] = 'O'
savg_pd.loc[mask_z, 'CLOSEMH'] = 0

# Filter: Keep only OPENIND='O' or (OPENIND in ('B','C','P') and CLOSEMH=1)
mask_keep = (savg_pd['OPENIND'] == 'O') | \
            ((savg_pd['OPENIND'].isin(['B', 'C', 'P'])) & (savg_pd['CLOSEMH'] == 1))

savg_pd = savg_pd[mask_keep].copy()

# Initialize new columns
savg_pd['NOACCT'] = 0
savg_pd['BCLOSE'] = 0
savg_pd['CCLOSE'] = 0

# Calculate BCLOSE and CCLOSE
mask_bcp = savg_pd['OPENIND'].isin(['B', 'C', 'P'])

# For OPENIND='C', CCLOSE=CLOSEMH
mask_c = savg_pd['OPENIND'] == 'C'
savg_pd.loc[mask_c, 'CCLOSE'] = savg_pd.loc[mask_c, 'CLOSEMH']

# For OPENIND in ('B','P'), BCLOSE=CLOSEMH
mask_bp = savg_pd['OPENIND'].isin(['B', 'P'])
savg_pd.loc[mask_bp, 'BCLOSE'] = savg_pd.loc[mask_bp, 'CLOSEMH']

# For OPENIND not in ('B','C','P'), NOACCT=1
mask_not_bcp = ~savg_pd['OPENIND'].isin(['B', 'C', 'P'])
savg_pd.loc[mask_not_bcp, 'NOACCT'] = 1

savg_df = pl.from_pandas(savg_pd)

print(f"  Processed SAVING records: {len(savg_df)}")


# ============================================================================
# STEP 3: Create SAVGC dataset (closed accounts in current month)
# ============================================================================

print("\nStep 3: Creating SAVGC dataset...")

# Filter for closed accounts in current month
savgc_df = savg_df.filter(
    (pl.col('OPENIND').is_in(['B', 'C', 'P'])) &
    (pl.col('CLOSEMH') == 1)
)

# Add YTDAVBAL and CUSTFISS
savgc_df = savgc_df.with_columns([
    pl.col('YTDAVAMT').alias('YTDAVBAL'),
    pl.lit(0).alias('CUSTFISS')
])

# Process LASTTRAN date
savgc_pd = savgc_df.to_pandas()


def parse_lasttran(lasttran):
    """Parse LASTTRAN from numeric format MMDDYY"""
    if lasttran is None or lasttran == 0:
        return None
    lasttran_str = str(int(lasttran)).zfill(9)[:6]
    try:
        return datetime.strptime(lasttran_str, '%m%d%y')
    except:
        return None


def parse_bdate(bdate):
    """Parse BDATE from numeric format MMDDYYYY"""
    if bdate is None or bdate == 0:
        return None
    bdate_str = str(int(bdate)).zfill(11)[:8]
    try:
        return datetime.strptime(bdate_str, '%m%d%Y')
    except:
        return None


if 'LASTTRAN' in savgc_pd.columns:
    savgc_pd['LASTTRAN'] = savgc_pd['LASTTRAN'].apply(parse_lasttran)

if 'BDATE' in savgc_pd.columns:
    savgc_pd['DOBMNI'] = savgc_pd['BDATE'].apply(parse_bdate)
else:
    savgc_pd['DOBMNI'] = None

savgc_df = pl.from_pandas(savgc_pd)

# Select specific columns for SAVGC
savgc_columns = [
    'BRANCH', 'ACCTNO', 'OPENDT', 'CLOSEDT', 'OPENIND', 'CURBAL', 'CUSTCODE',
    'MTDAVBAL', 'YTDAVBAL', 'CUSTFISS', 'DOBMNI', 'PRODUCT', 'CHGIND', 'COSTCTR',
    'DEPTYPE', 'DNBFISME', 'INTPD', 'INTPLAN', 'INTRSTPD', 'INTYTD', 'LASTTRAN',
    'ORGCODE', 'ORGTYPE', 'PURPOSE', 'RISKCODE', 'SECOND', 'SECTOR', 'SERVICE',
    'STATE', 'USER2', 'USER3', 'USER5', 'STMT_CYCLE', 'NXT_STMT_CYCLE_DT'
]

# Keep only available columns
available_cols = [col for col in savgc_columns if col in savgc_df.columns]
savgc_df = savgc_df.select(available_cols)

# Save SAVGC dataset
savgc_output = str(OUTPUT_SAVGC_TEMPLATE).format(mon=REPTMON)
savgc_df.write_parquet(savgc_output)

print(f"  SAVGC records: {len(savgc_df)}")
print(f"  SAVGC saved to: {savgc_output}")


# ============================================================================
# STEP 4: Aggregate SAVG by BRANCH and PRODUCT
# ============================================================================

print("\nStep 4: Aggregating SAVG data...")

savg_summary = savg_df.group_by(['BRANCH', 'PRODUCT']).agg([
    pl.col('OPENMH').sum().alias('OPENMH'),
    pl.col('CURBAL').sum().alias('CURBAL'),
    pl.col('NOACCT').sum().alias('NOACCT'),
    pl.col('CLOSEMH').sum().alias('CLOSEMH'),
    pl.col('BCLOSE').sum().alias('BCLOSE'),
    pl.col('CCLOSE').sum().alias('CCLOSE')
])

print(f"  Aggregated records: {len(savg_summary)}")


# ============================================================================
# STEP 5: Process cumulative data (if not first month)
# ============================================================================

print("\nStep 5: Processing cumulative data...")

if int(REPTMON) > 1:
    print(f"  Loading previous month data (SAVGF{REPTMON1})...")

    # Load previous month cumulative data
    savgf_prev_file = str(INPUT_SAVGF_TEMPLATE).format(mon=REPTMON1)

    try:
        savp_df = con.execute(f"SELECT * FROM '{savgf_prev_file}'").pl()

        # Rename cumulative columns
        savp_df = savp_df.with_columns([
            pl.col('OPENCUM').alias('OPENCUX'),
            pl.col('CLOSECUM').alias('CLOSECUX')
        ])

        # Apply branch remapping
        savp_df = savp_df.with_columns([
            pl.when(pl.col('BRANCH') == 227)
            .then(pl.lit(81))
            .otherwise(pl.col('BRANCH'))
            .alias('BRANCH'),

            pl.when(pl.col('BRANCH') == 250)
            .then(pl.lit(92))
            .otherwise(pl.col('BRANCH'))
            .alias('BRANCH')
        ])

        # Aggregate previous month data
        savp_summary = savp_df.group_by(['BRANCH', 'PRODUCT']).agg([
            pl.col('OPENCUX').sum().alias('OPENCUX'),
            pl.col('CLOSECUX').sum().alias('CLOSECUX')
        ])

        print(f"  Previous month records: {len(savp_summary)}")

        # Merge with current month
        savg_final = savg_summary.join(
            savp_summary,
            on=['BRANCH', 'PRODUCT'],
            how='outer'
        )

        # Fill nulls with 0
        fill_columns = ['CLOSECUX', 'OPENCUX', 'OPENMH', 'CLOSEMH', 'BCLOSE',
                        'CCLOSE', 'NOACCT', 'CURBAL']

        for col in fill_columns:
            if col in savg_final.columns:
                savg_final = savg_final.with_columns([
                    pl.col(col).fill_null(0).alias(col)
                ])

        # Calculate cumulative values
        savg_final = savg_final.with_columns([
            (pl.col('OPENMH') + pl.col('OPENCUX')).alias('OPENCUM'),
            (pl.col('CLOSEMH') + pl.col('CLOSECUX')).alias('CLOSECUM'),
            (pl.col('OPENMH') - pl.col('CLOSEMH')).alias('NETCHGMH'),
        ])

        # Calculate NETCHGYR
        savg_final = savg_final.with_columns([
            (pl.col('OPENCUM') - pl.col('CLOSECUM')).alias('NETCHGYR')
        ])

        print(f"  Merged records: {len(savg_final)}")

    except Exception as e:
        print(f"  Warning: Could not load previous month data: {e}")
        print("  Treating as first month...")

        savg_final = savg_summary.with_columns([
            pl.col('OPENMH').alias('OPENCUM'),
            pl.col('CLOSEMH').alias('CLOSECUM'),
            (pl.col('OPENMH') - pl.col('CLOSEMH')).alias('NETCHGMH'),
            (pl.col('OPENMH') - pl.col('CLOSEMH')).alias('NETCHGYR')
        ])

else:
    print("  First month - no previous data to load")

    savg_final = savg_summary.with_columns([
        pl.col('OPENMH').alias('OPENCUM'),
        pl.col('CLOSEMH').alias('CLOSECUM'),
        (pl.col('OPENMH') - pl.col('CLOSEMH')).alias('NETCHGMH'),
        (pl.col('OPENMH') - pl.col('CLOSEMH')).alias('NETCHGYR')
    ])


# ============================================================================
# STEP 6: Save SAVGF dataset
# ============================================================================

print("\nStep 6: Saving SAVGF dataset...")

savgf_output = str(OUTPUT_SAVGF_TEMPLATE).format(mon=REPTMON)
savg_final.write_parquet(savgf_output)

print(f"  SAVGF saved to: {savgf_output}")


# ============================================================================
# STEP 7: Generate tabular report
# ============================================================================

print("\nStep 7: Generating tabular report...")

# Aggregate by BRANCH only (sum across all products)
report_data = savg_final.group_by('BRANCH').agg([
    pl.col('OPENMH').sum().alias('OPENMH'),
    pl.col('OPENCUM').sum().alias('OPENCUM'),
    pl.col('CLOSEMH').sum().alias('CLOSEMH'),
    pl.col('BCLOSE').sum().alias('BCLOSE'),
    pl.col('CCLOSE').sum().alias('CCLOSE'),
    pl.col('CLOSECUM').sum().alias('CLOSECUM'),
    pl.col('NOACCT').sum().alias('NOACCT'),
    pl.col('CURBAL').sum().alias('CURBAL'),
    pl.col('NETCHGMH').sum().alias('NETCHGMH'),
    pl.col('NETCHGYR').sum().alias('NETCHGYR')
]).sort('BRANCH')

# Calculate totals
totals = report_data.select([
    pl.col('OPENMH').sum().alias('OPENMH'),
    pl.col('OPENCUM').sum().alias('OPENCUM'),
    pl.col('CLOSEMH').sum().alias('CLOSEMH'),
    pl.col('BCLOSE').sum().alias('BCLOSE'),
    pl.col('CCLOSE').sum().alias('CCLOSE'),
    pl.col('CLOSECUM').sum().alias('CLOSECUM'),
    pl.col('NOACCT').sum().alias('NOACCT'),
    pl.col('CURBAL').sum().alias('CURBAL'),
    pl.col('NETCHGMH').sum().alias('NETCHGMH'),
    pl.col('NETCHGYR').sum().alias('NETCHGYR')
])

print(f"  Report branches: {len(report_data)}")


# ============================================================================
# STEP 8: Write formatted report with ASA carriage control
# ============================================================================

print("\nStep 8: Writing formatted report...")


def format_number(value, decimals=0):
    """Format number with commas"""
    if value is None or (isinstance(value, float) and value != value):
        return '.' if decimals == 0 else ('.' * (10 + decimals + 1))

    if decimals == 0:
        return f"{int(value):,}".replace(',', '.')
    else:
        return f"{value:,.{decimals}f}".replace(',', '_').replace('.', ',').replace('_', '.')


with open(OUTPUT_REPORT, 'w') as f:
    # Title
    f.write(f"{ASA_NEW_PAGE}PUBLIC ISLAMIC BANK BERHAD\n")
    f.write(f"{ASA_SPACE}SAVINGS ACCOUNT OPENED/CLOSED FOR THE MONTH AS AT {RDATE}\n")
    f.write(f"{ASA_SPACE}\n")

    # Table header
    header_line1 = f"{ASA_SPACE}{'BRANCH':<8}"
    header_line1 += f"{'CURRENT':>10} {'CUMULATIVE':>10} {'CURRENT':>10} {'CLOSED':>10} {'CLOSED':>10} "
    header_line1 += f"{'CUMULATIVE':>10} {'NO.OF':>10} {'TOTAL (RM)':>17} {'NET CHANGE':>10} {'NET CHANGE':>10}\n"
    f.write(header_line1)

    header_line2 = f"{ASA_SPACE}{' ':<8}"
    header_line2 += f"{'MONTH':>10} {'OPENED':>10} {'MONTH':>10} {'BY':>10} {'BY':>10} "
    header_line2 += f"{'CLOSED':>10} {'ACCTS':>10} {'O/S':>17} {'FOR THE':>10} {'YEAR TO':>10}\n"
    f.write(header_line2)

    header_line3 = f"{ASA_SPACE}{' ':<8}"
    header_line3 += f"{'OPENED':>10} {' ':>10} {'CLOSED':>10} {'BANK':>10} {'CUSTOMER':>10} "
    header_line3 += f"{' ':>10} {' ':>10} {' ':>17} {'MONTH':>10} {'DATE':>10}\n"
    f.write(header_line3)

    # Separator
    f.write(f"{ASA_SPACE}{'-' * 127}\n")

    # Data rows
    report_pd = report_data.to_pandas()

    for idx, row in report_pd.iterrows():
        branch = str(int(row['BRANCH'])).rjust(8)
        openmh = format_number(row['OPENMH']).rjust(10)
        opencum = format_number(row['OPENCUM']).rjust(10)
        closemh = format_number(row['CLOSEMH']).rjust(10)
        bclose = format_number(row['BCLOSE']).rjust(10)
        cclose = format_number(row['CCLOSE']).rjust(10)
        closecum = format_number(row['CLOSECUM']).rjust(10)
        noacct = format_number(row['NOACCT']).rjust(10)
        curbal = format_number(row['CURBAL'], 2).rjust(17)
        netchgmh = format_number(row['NETCHGMH']).rjust(10)
        netchgyr = format_number(row['NETCHGYR']).rjust(10)

        line = f"{ASA_SPACE}{branch}{openmh}{opencum}{closemh}{bclose}{cclose}"
        line += f"{closecum}{noacct}{curbal}{netchgmh}{netchgyr}\n"
        f.write(line)

    # Separator before totals
    f.write(f"{ASA_SPACE}{'-' * 127}\n")

    # Total row
    totals_row = totals.row(0, named=True)

    total_branch = 'TOTAL'.rjust(8)
    total_openmh = format_number(totals_row['OPENMH']).rjust(10)
    total_opencum = format_number(totals_row['OPENCUM']).rjust(10)
    total_closemh = format_number(totals_row['CLOSEMH']).rjust(10)
    total_bclose = format_number(totals_row['BCLOSE']).rjust(10)
    total_cclose = format_number(totals_row['CCLOSE']).rjust(10)
    total_closecum = format_number(totals_row['CLOSECUM']).rjust(10)
    total_noacct = format_number(totals_row['NOACCT']).rjust(10)
    total_curbal = format_number(totals_row['CURBAL'], 2).rjust(17)
    total_netchgmh = format_number(totals_row['NETCHGMH']).rjust(10)
    total_netchgyr = format_number(totals_row['NETCHGYR']).rjust(10)

    total_line = f"{ASA_SPACE}{total_branch}{total_openmh}{total_opencum}{total_closemh}{total_bclose}{total_cclose}"
    total_line += f"{total_closecum}{total_noacct}{total_curbal}{total_netchgmh}{total_netchgyr}\n"
    f.write(total_line)

print(f"Report generated: {OUTPUT_REPORT}")


# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 80)
print("CONVERSION COMPLETE")
print("=" * 80)
print(f"\nFiles generated:")
print(f"  1. SAVGC dataset: {savgc_output}")
print(f"  2. SAVGF dataset: {savgf_output}")
print(f"  3. Report: {OUTPUT_REPORT}")
print(f"\nReporting Date: {RDATE}")
print(f"Report Month: {REPTMON}")
