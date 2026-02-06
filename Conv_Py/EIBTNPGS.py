#!/usr/bin/env python3
"""
File Name: EIBTNPGS
Non-Performing Government Scheme Trade Finance Processing
"""

import duckdb
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import calendar


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# INPUT_DIR = Path("/mnt/user-data/uploads")
# OUTPUT_DIR = Path("/mnt/user-data/outputs")

BASE_DIR = Path(__file__).resolve().parent

INPUT_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

# Input files
MNILN_FILE = INPUT_DIR / "sap_pbb_mniln_0.parquet"
CRFTABL_FILE = INPUT_DIR / "sap_pbb_btrade_crftabl.parquet"
BTRSA_MAST_FILE = INPUT_DIR / "sap_pbb_btrade_sasdata_mast.parquet"
BTRSA_CRED_FILE = INPUT_DIR / "sap_pbb_btrade_sasdata_cred.parquet"
BTRSA_PROV_FILE = INPUT_DIR / "sap_pbb_btrade_sasdata_prov.parquet"
BTRSA_SUBA_FILE = INPUT_DIR / "sap_pbb_btrade_sasdata_suba.parquet"
COLL_FILE = INPUT_DIR / "rbp2_b033_mst00_lccrisex.parquet"
DESC_FILE = INPUT_DIR / "rbp2_b033_mst00_lccrisex_desc.parquet"
MICR_FILE = INPUT_DIR / "sap_bopess_branch_micr_code.parquet"
NPLA_FILE = INPUT_DIR / "sap_pbb_npgs_sasdata_npla.parquet"

# Output file - will be determined based on report month
OUTPUT_FILE = None  # Set after reading REPTDATE


# ============================================================================
# INITIALIZE DUCKDB CONNECTION
# ============================================================================
con = duckdb.connect()


# ============================================================================
# STEP 1: READ REPTDATE AND SET MACRO VARIABLES
# ============================================================================
print("Step 1: Reading report date...")

print("BASE_DIR:", BASE_DIR)
print("INPUT_DIR:", INPUT_DIR)
print("MNILN_FILE:", MNILN_FILE)
print("Exists?", MNILN_FILE.exists(), "\n")

reptdate_df = pl.read_parquet(MNILN_FILE).select(['REPTDATE']).head(1)
reptdate = reptdate_df['REPTDATE'][0]

REPTMON = f"{reptdate.month:02d}"
REPTDAY = f"{reptdate.day:02d}"
REPTYEAR = f"{reptdate.year:04d}"
RDATE = (reptdate - datetime(1960, 1, 1)).days  # SAS date value

print(f"Report Date: {reptdate}, RDATE: {RDATE}")

# Set output file name
OUTPUT_FILE = OUTPUT_DIR / f"btnpgs{REPTMON}.parquet"

# Update BTRSA file paths with date suffix
BTRSA_MAST_FILE = INPUT_DIR / f"sap_pbb_btrade_sasdata_mast{REPTDAY}{REPTMON}.parquet"
BTRSA_CRED_FILE = INPUT_DIR / f"sap_pbb_btrade_sasdata_cred{REPTDAY}{REPTMON}.parquet"
BTRSA_PROV_FILE = INPUT_DIR / f"sap_pbb_btrade_sasdata_prov{REPTDAY}{REPTMON}.parquet"
BTRSA_SUBA_FILE = INPUT_DIR / f"sap_pbb_btrade_sasdata_suba{REPTDAY}{REPTMON}.parquet"


# ============================================================================
# STEP 2: PROCESS CRFTABL (Credit Facility Table)
# ============================================================================
print("Step 2: Processing credit facility table...")

crft_data = pl.read_parquet(CRFTABL_FILE).select([
    'RECTYP1', 'TFID', 'SUBACCT', 'PREIND', 'CENSUST', 'ACCTNO'
])

# Filter out records where RECTYP1='1'
crft_data = crft_data.filter(pl.col('RECTYP1') != '1')


# Assign SCH based on CENSUST
def assign_sch(censust):
    """Assign scheme code based on census type"""
    if censust == 3:
        return 'P51'
    elif censust == 4:
        return 'P72'
    elif censust == 5:
        return 'P65'
    elif censust == 6:
        return 'P85'
    elif censust == 7:
        return 'P53'
    else:
        return '   '


crft_data = crft_data.with_columns([
    pl.struct(['CENSUST']).map_elements(
        lambda x: assign_sch(x['CENSUST']),
        return_dtype=pl.Utf8
    ).alias('SCH')
])

# Filter where SCH is not blank
crft_data = crft_data.filter(pl.col('SCH') != '   ')

# Remove duplicates
crft_data = crft_data.unique(subset=['ACCTNO', 'CENSUST', 'SUBACCT'], keep='first')


# ============================================================================
# STEP 3: MERGE WITH MAST (Master Account Data)
# ============================================================================
print("Step 3: Merging with master account data...")

mast_data = pl.read_parquet(BTRSA_MAST_FILE).select([
    'ACCTNO', 'FICODE', 'NAME', 'BUSREGN'
]).unique(subset=['ACCTNO'], keep='first')

crft_merged = crft_data.join(mast_data, on='ACCTNO', how='inner')

# Rename FICODE to BRANCH
crft_merged = crft_merged.with_columns([
    pl.col('FICODE').alias('BRANCH')
])

# Filter where ACCTNO > 0
crft_merged = crft_merged.filter(pl.col('ACCTNO') > 0)

# Select columns for CRFT
crft_final = crft_merged.select([
    'BRANCH', 'ACCTNO', 'SUBACCT', 'NAME', 'BUSREGN', 'CENSUST', 'TFID', 'SCH'
]).unique(subset=['ACCTNO', 'SUBACCT'], keep='first')

# Create CRFT1 with modified SUBACCT
crft1_data = crft_merged.with_columns([
    (pl.lit('FAC') + pl.col('SUBACCT').str.slice(0, 1)).alias('SUBACCT')
]).select([
    'BRANCH', 'ACCTNO', 'SUBACCT', 'NAME', 'BUSREGN', 'CENSUST', 'TFID', 'SCH'
]).unique(subset=['ACCTNO', 'SUBACCT'], keep='first')


# ============================================================================
# STEP 4: PROCESS CREDIT DATA
# ============================================================================
print("Step 4: Processing credit data...")

cred_data = pl.read_parquet(BTRSA_CRED_FILE)

# Merge with CRFT
cred_data = cred_data.join(crft_final, on=['ACCTNO', 'SUBACCT'], how='inner')

# Filter conditions
cred_data = cred_data.filter(
    (pl.col('SUBACCT').str.slice(0, 3) != 'FAC') &
    (pl.col('TRANSREF') != '  ')
)

# Create TRANSREX
cred_data = cred_data.with_columns([
    pl.col('TRANSREF').str.slice(0, 7).alias('TRANSREX')
])

# Remove duplicates
cred_data = cred_data.unique(subset=['ACCTNO', 'TRANSREF'], keep='first')


# ============================================================================
# STEP 5: SUMMARIZE CREDIT OUTSTAND (CRED1)
# ============================================================================
print("Step 5: Summarizing credit outstanding...")

cred1_data = cred_data.filter(
    pl.col('SUBACCT').str.slice(1, 3) != 'SGL'
).group_by('ACCTNO').agg([
    pl.col('OUTSTAND').sum().alias('OUTSTAND')
])


# ============================================================================
# STEP 6: PROCESS PROVISION DATA (CRED2)
# ============================================================================
print("Step 6: Processing provision data...")

prov_data = pl.read_parquet(BTRSA_PROV_FILE).filter(
    ~pl.col('NPLIND').is_in(['P', 'F'])
)

# Merge PROV with CRED2
cred2_data = prov_data.join(
    cred_data.select(['ACCTNO', 'TRANSREX', 'SUBACCT', 'OUTSTAND']),
    on=['ACCTNO', 'TRANSREX'],
    how='inner'
)

# Filter out FAC and OV subaccounts
cred2_data = cred2_data.filter(
    ~pl.col('SUBACCT').str.slice(0, 3).is_in(['OV ', 'FAC'])
)

# Sort by MATUREDS and keep first per ACCTNO
cred2_data = cred2_data.sort(['ACCTNO', 'MATUREDS']).unique(
    subset=['ACCTNO'], keep='first'
)


# Calculate arrears
def calculate_arrears(nodays):
    """Calculate arrears based on number of days"""
    if nodays < 30:
        return 0
    elif nodays < 60:
        return 1
    elif nodays < 90:
        return 2
    elif nodays < 120:
        return 3
    elif nodays < 150:
        return 4
    elif nodays < 180:
        return 5
    elif nodays < 365:
        return 6
    else:
        return round(nodays / 30)


cred2_data = cred2_data.with_columns([
    pl.when((pl.col('MATUREDS').is_not_null()) & (pl.col('MATUREDS') > 0) & (pl.lit(RDATE) > pl.col('MATUREDS')))
    .then((pl.lit(RDATE) - pl.col('MATUREDS')) + 1)
    .otherwise(0).alias('NODAYS')
])

cred2_data = cred2_data.with_columns([
    pl.when(pl.col('NODAYS') > 0)
    .then(pl.struct(['NODAYS']).map_elements(
        lambda x: calculate_arrears(x['NODAYS']),
        return_dtype=pl.Int64
    ))
    .otherwise(0).alias('ARREARS')
])

cred2_final = cred2_data.select(['ACCTNO', 'ARREARS', 'MATUREDS', 'NODAYS'])


# ============================================================================
# STEP 7: PROCESS SUBACCOUNT DATA (SUBA)
# ============================================================================
print("Step 7: Processing subaccount data...")

suba_data = pl.read_parquet(BTRSA_SUBA_FILE)

# Merge with CRFT1
suba_data = suba_data.join(crft1_data, on=['ACCTNO', 'SUBACCT'], how='inner')

# Create SUBA1 (FAC subaccounts)
suba1_data = suba_data.filter(
    pl.col('SUBACCT').str.slice(0, 3) == 'FAC'
).unique(subset=['ACCTNO', 'SUBACCT'], keep='first')

# Summarize LIMTCURM for SUBA1
suba1_summary = suba1_data.group_by('ACCTNO').agg([
    pl.col('LIMTCURM').sum().alias('LIMTCURM')
])

# Create SUBA2 (non-FAC, non-SGL subaccounts with no TRANSREF)
suba2_data = suba_data.filter(
    (pl.col('TRANSREF') == '  ') &
    (pl.col('SUBACCT').str.slice(0, 3) != 'FAC') &
    (pl.col('SUBACCT').str.slice(1, 3) != 'SGL')
).unique(subset=['ACCTNO', 'SUBACCT'], keep='first')

# Summarize LIMITS for SUBA2
suba2_summary = suba2_data.group_by('ACCTNO').agg([
    pl.col('LIMTCURM').sum().alias('LIMITS')
])

# Merge SUBA1 and SUBA2 summaries
subalmt_data = suba1_summary.join(suba2_summary, on='ACCTNO', how='outer')

# Use LIMITS if LIMTCURM is null
subalmt_data = subalmt_data.with_columns([
    pl.when(pl.col('LIMTCURM').is_null())
    .then(pl.col('LIMITS'))
    .otherwise(pl.col('LIMTCURM')).alias('LIMTCURM')
])

# Process SUBA for issue dates
suba_issue = suba_data.filter(pl.col('TRANSREF') != '   ')


def calculate_issue_date(creatds, transref):
    """Calculate issue date from creation date"""
    if creatds is None or creatds <= 0:
        return None, 99999

    # Convert CREATDS (YYMMDD) to date
    date_str = str(int(creatds)).zfill(6)
    try:
        year = int(date_str[:2])
        month = int(date_str[2:4])
        day = int(date_str[4:6])

        # Handle 2-digit year (cutoff 1940)
        if year >= 40:
            year += 1900
        else:
            year += 2000

        issue_date = datetime(year, month, day).date()
        issue_sas = (datetime.combine(issue_date, datetime.min.time()) - datetime(1960, 1, 1)).days

        # If TRANSREF starts with 'Y', set MATURED1 to ISSUEDT
        if transref and len(transref) > 0 and transref[0] == 'Y':
            matured1 = issue_sas
        else:
            matured1 = 99999

        return issue_sas, matured1
    except:
        return None, 99999


suba_issue = suba_issue.with_columns([
    pl.struct(['CREATDS', 'TRANSREF']).map_elements(
        lambda x: calculate_issue_date(x['CREATDS'], x['TRANSREF']),
        return_dtype=pl.Struct([pl.Field('ISSUEDT', pl.Int64), pl.Field('MATURED1', pl.Int64)])
    ).alias('_dates')
])

suba_issue = suba_issue.with_columns([
    pl.col('_dates').struct.field('ISSUEDT').alias('ISSUEDT'),
    pl.col('_dates').struct.field('MATURED1').alias('MATURED1')
]).drop('_dates')

# Sort and keep first per ACCTNO
suba_issue = suba_issue.sort(['ACCTNO', 'ISSUEDT']).unique(
    subset=['ACCTNO'], keep='first'
)

suba_final = suba_issue.select(['ACCTNO', 'ISSUEDT', 'MATURED1'])


# ============================================================================
# STEP 8: PROCESS COLLATERAL DATA
# ============================================================================
print("Step 8: Processing collateral data...")

coll_data = pl.read_parquet(COLL_FILE).select(['CCOLLNO', 'ACCTNO'])
desc_data = pl.read_parquet(DESC_FILE).select(['CCOLLNO', 'CINSTCL', 'NATGUAR', 'CENSUS'])


# Assign CR based on CENSUS
def assign_cr(census):
    """Assign CR code based on census value"""
    if census is None:
        return '  '
    census_int = int(census)
    if 51000000 <= census_int <= 51999999:
        return '51'
    elif 72000000 <= census_int <= 72999999:
        return '72'
    elif 1000000000 <= census_int <= 1099999999:
        return '10'
    else:
        return '  '


desc_data = desc_data.with_columns([
    pl.struct(['CENSUS']).map_elements(
        lambda x: assign_cr(x['CENSUS']),
        return_dtype=pl.Utf8
    ).alias('CR')
])

# Filter for specific CR codes
desc_data = desc_data.filter(pl.col('CR') != '  ')

# Merge collateral data
coll_combined = coll_data.join(desc_data, on='CCOLLNO', how='inner')

# Filter for specific collateral types
coll_combined = coll_combined.filter(
    (pl.col('CINSTCL') == '18') & (pl.col('NATGUAR') == '06')
)


# ============================================================================
# STEP 9: MERGE MAST WITH COLL
# ============================================================================
print("Step 9: Merging master with collateral...")

mast_final = crft_final.join(coll_combined, on='ACCTNO', how='inner')

# Remove duplicates
mast_final = mast_final.unique(subset=['ACCTNO', 'CENSUS'], keep='first')


# ============================================================================
# STEP 10: MERGE WITH MICR DATA
# ============================================================================
print("Step 10: Merging MICR codes...")

micr_data = pl.read_parquet(MICR_FILE).select(['BRANCH', 'MICRCD'])

mast_final = mast_final.join(micr_data, on='BRANCH', how='left')


# ============================================================================
# STEP 11: MERGE ALL DATA TO CREATE NPGS
# ============================================================================
print("Step 11: Merging all data...")

npgs_data = mast_final.join(cred1_data, on='ACCTNO', how='left')
npgs_data = npgs_data.join(cred2_final, on='ACCTNO', how='left')
npgs_data = npgs_data.join(suba_final, on='ACCTNO', how='left')
npgs_data = npgs_data.join(subalmt_data, on='ACCTNO', how='left')


# ============================================================================
# STEP 12: ASSIGN CVAR02 BASED ON SCH AND CR
# ============================================================================
print("Step 12: Assigning CVAR02...")


def assign_cvar02(sch, cr):
    """Assign CVAR02 based on scheme and CR"""
    if sch == 'P51' and cr in ['10', '51']:
        return '51'
    elif sch == 'P72' and cr in ['10', '72']:
        return '72'
    elif sch == 'P85' and cr == '10':
        return '85'
    elif sch == 'P53' and cr == '10':
        return '53'
    elif sch == 'P65' and cr == '10':
        return '65'
    else:
        return '  '


npgs_data = npgs_data.with_columns([
    pl.struct(['SCH', 'CR']).map_elements(
        lambda x: assign_cvar02(x['SCH'], x['CR']),
        return_dtype=pl.Utf8
    ).alias('CVAR02')
])

# Filter where CVAR02 is not blank
npgs_data = npgs_data.filter(pl.col('CVAR02') != '  ')


# ============================================================================
# STEP 13: CREATE FINAL CVAR COLUMNS
# ============================================================================
print("Step 13: Creating final output columns...")


def format_date(date_obj):
    """Format date as DD/MM/YYYY"""
    if date_obj is None:
        return '          '
    # Convert SAS date to Python date if needed
    if isinstance(date_obj, (int, float)):
        base_date = datetime(1960, 1, 1).date()
        date_obj = base_date + timedelta(days=int(date_obj))
    return date_obj.strftime('%d/%m/%Y')


normdt = f"{REPTDAY}/{REPTMON}/{REPTYEAR}"

# Handle MATURED1 vs MATUREDS
npgs_data = npgs_data.with_columns([
    pl.when((pl.col('MATURED1').is_not_null()) & (pl.col('MATUREDS').is_not_null()) & (
                pl.col('MATURED1') < pl.col('MATUREDS')))
    .then(pl.col('MATURED1'))
    .otherwise(pl.col('MATUREDS')).alias('MATUREDS'),
    pl.when(pl.col('ARREARS').is_null())
    .then(0)
    .otherwise(pl.col('ARREARS')).alias('ARREARS')
])


# Calculate NPL date and status
def calculate_npl_info(matureds, nodays, rdate):
    """Calculate NPL date and return formatted string"""
    if nodays is None or nodays <= 89:
        return None, '   '

    # Add 89 days to MATUREDS
    if matureds is None or matureds <= 0:
        return None, '   '

    # Convert SAS date to Python date
    base_date = datetime(1960, 1, 1).date()
    mature_date = base_date + timedelta(days=int(matureds))
    npl_date = mature_date + timedelta(days=89)

    return npl_date, 'NPL'


npgs_data = npgs_data.with_columns([
    pl.struct(['MATUREDS', 'NODAYS']).map_elements(
        lambda x: calculate_npl_info(x['MATUREDS'], x['NODAYS'], RDATE),
        return_dtype=pl.Struct([pl.Field('NPLDATE', pl.Date), pl.Field('NPL_STATUS', pl.Utf8)])
    ).alias('_npl_info')
])

npgs_data = npgs_data.with_columns([
    pl.col('_npl_info').struct.field('NPLDATE').alias('NPLDATE'),
    pl.col('_npl_info').struct.field('NPL_STATUS').alias('NPL_STATUS')
]).drop('_npl_info')

# Create final columns
npgs_data = npgs_data.with_columns([
    pl.lit(0).alias('PRODUCT'),
    pl.col('CENSUS').cast(pl.Int64).alias('CVAR01'),
    pl.col('BUSREGN').cast(pl.Utf8).alias('CVAR03'),
    pl.col('NAME').cast(pl.Utf8).alias('CVAR04'),
    pl.col('ISSUEDT').alias('CVAR05'),
    pl.col('ACCTNO').cast(pl.Int64).alias('CVAR06'),
    pl.lit('TF').alias('CVAR07'),
    pl.col('LIMTCURM').cast(pl.Float64).alias('CVAR08'),
    pl.col('OUTSTAND').cast(pl.Float64).alias('CVAR09'),
    pl.lit(0.00).alias('CVAR10'),
    pl.col('ARREARS').cast(pl.Int64).alias('CVAR11'),
    pl.when((pl.col('ARREARS') >= 3) & (pl.col('NPLDATE').is_not_null()))
    .then(pl.lit('NPL'))
    .otherwise(pl.col('NPL_STATUS')).alias('CVAR12'),
    pl.struct(['NPLDATE']).map_elements(
        lambda x: format_date(x['NPLDATE']),
        return_dtype=pl.Utf8
    ).alias('CVAR13'),
    pl.lit('0233').alias('CVAR14'),
    pl.col('MICRCD').alias('CVAR15')
])

# Fill CVAR12 default
npgs_data = npgs_data.with_columns([
    pl.when(pl.col('CVAR12').is_null())
    .then(pl.lit('   '))
    .otherwise(pl.col('CVAR12')).alias('CVAR12')
])

# Filter out null OUTSTAND
npgs_data = npgs_data.filter(pl.col('OUTSTAND').is_not_null())


# ============================================================================
# STEP 14: MERGE WITH NPLA (Previous NPL Status)
# ============================================================================
print("Step 14: Merging with NPLA...")

try:
    npla_data = pl.read_parquet(NPLA_FILE).select(['CVAR06', 'CVAR01', 'STATUS', 'NDATE'])
    npgs_data = npgs_data.join(npla_data, on=['CVAR06', 'CVAR01'], how='left')

    # Update CVAR13 based on NPL status
    npgs_data = npgs_data.with_columns([
        pl.when((pl.col('CVAR12') == 'NPL') & (pl.col('STATUS') == 'NPL'))
        .then(pl.col('NDATE'))
        .when((pl.col('CVAR12') == '   ') & (pl.col('STATUS') == 'NPL'))
        .then(pl.lit(normdt))
        .when((pl.col('CVAR12') == '   ') & (pl.col('STATUS') != 'NPL') & (pl.col('NDATE').is_not_null()) & (
                    pl.col('NDATE') != '          '))
        .then(pl.col('NDATE'))
        .otherwise(pl.col('CVAR13')).alias('CVAR13')
    ])
except Exception as e:
    print(f"Warning: Could not read NPLA file: {e}")


# ============================================================================
# STEP 15: FINAL OUTPUT
# ============================================================================
print("Step 15: Writing output...")

# Select and order final columns
final_columns = [
    'CVAR01', 'CVAR02', 'CVAR03', 'CVAR04', 'CVAR05', 'CVAR06', 'CVAR07',
    'CVAR08', 'CVAR09', 'CVAR10', 'CVAR11', 'CVAR12', 'CVAR13', 'CVAR14',
    'SCH', 'CR', 'BRANCH', 'CVAR15', 'CENSUST', 'NATGUAR', 'CINSTCL', 'PRODUCT'
]

output_data = npgs_data.select([col for col in final_columns if col in npgs_data.columns])

# Sort by CVAR01
output_data = output_data.sort('CVAR01')

# Write output
output_data.write_parquet(OUTPUT_FILE)

print(f"Output written to: {OUTPUT_FILE}")
print(f"Total records: {len(output_data)}")
print("\nProcessing complete!")

# Close DuckDB connection
con.close()
