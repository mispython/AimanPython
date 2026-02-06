#!/usr/bin/env python3
"""
File Name: EIBLNPGS
Non-Performing Government Scheme Loan Processing
Converts SAS program to Python using DuckDB and Polars
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
LOAN_FILE = INPUT_DIR / "sap_pbb_mniln_0.parquet"
LOANI_FILE = INPUT_DIR / "sap_pibb_mniln_0.parquet"
MICR_FILE = INPUT_DIR / "sap_pbb_micr_code_ccris_text.parquet"
COLL_FILE = INPUT_DIR / "rbp2_b033_mst00_lccrisex.parquet"
DESC_FILE = INPUT_DIR / "rbp2_b033_mst00_lccrisex_desc.parquet"
CISLN_FILE = INPUT_DIR / "sap_pbb_cisbext_ln.parquet"
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

reptdate_df = pl.read_parquet(LOAN_FILE).select(['REPTDATE']).head(1)
reptdate = reptdate_df['REPTDATE'][0]

REPTMON = f"{reptdate.month:02d}"
REPTDAY = f"{reptdate.day:02d}"
REPTYEAR = f"{reptdate.year:04d}"
SDATE = (reptdate - datetime(1960, 1, 1)).days  # SAS date value

print(f"Report Date: {reptdate}, SDATE: {SDATE}")

# Set output file name
OUTPUT_FILE = OUTPUT_DIR / f"lnnpgs{REPTMON}.parquet"


# ============================================================================
# STEP 2: PROCESS LOAN DATA (LOAN0 and LOAN1)
# ============================================================================
print("Step 2: Processing loan data...")

# Read loan data from both sources
loan_query = """
SELECT 
    LOANTYPE,
    CENSUS,
    LOANTYPE as PRODUCT,
    CENSUS as CENSUST,
    ACCTNO,
    NOTENO,
    COMMNO,
    ISSUEDT,
    BLDATE,
    COSTCTR,
    PENDBRH,
    BALANCE,
    CUSTCODE
FROM read_parquet(?)
UNION ALL
SELECT 
    LOANTYPE,
    CENSUS,
    LOANTYPE as PRODUCT,
    CENSUS as CENSUST,
    ACCTNO,
    NOTENO,
    COMMNO,
    ISSUEDT,
    BLDATE,
    COSTCTR,
    PENDBRH,
    BALANCE,
    CUSTCODE
FROM read_parquet(?)
"""

loan_data = con.execute(loan_query, [str(LOAN_FILE), str(LOANI_FILE)]).pl()


# Assign SCH based on LOANTYPE and CENSUS
def assign_sch(loantype, census):
    """Assign scheme code based on loantype and census"""
    if loantype == 575 and census == 575.00:
        return 'PS2'
    elif loantype == 144 and census == 144.00:
        return 'PS3'
    elif loantype == 575 and census == 575.01:
        return 'PH4'
    elif loantype == 144 and census == 144.01:
        return 'PH5'
    elif loantype == 575 and census == 575.02:
        return 'PH6'
    elif loantype == 169 and census == 169.01:
        return 'PH7'
    elif loantype == 510 and census in [510.02, 510.03]:
        return 'P70'
    elif loantype == 301 and census == 301.04:
        return 'P85'
    elif loantype == 532 and census == 532.00:
        return 'P51'
    elif loantype == 532 and census == 532.01:
        return 'P53'
    elif loantype == 532 and census == 532.03:
        return 'E6'
    elif loantype == 524 and census == 524.01:
        return 'P72'
    elif loantype == 527 and census == 527.01:
        return 'P72'
    elif loantype == 529 and census == 529.00:
        return 'P81'
    elif loantype == 529 and census == 529.01:
        return 'P83'
    elif loantype == 531 and census == 531.02:
        return 'P63'
    elif loantype == 533 and census == 533.01:
        return 'P64'
    elif loantype == 533 and census == 533.00:
        return 'P65'
    elif loantype == 574 and census == 574.00:
        return 'P57'
    elif loantype == 900 and census == 900.01:
        return 'P81'
    elif loantype == 900 and census == 900.02:
        return 'P83'
    elif loantype == 568 and census in [568.02, 568.06, 568.14]:
        return 'F5'
    elif loantype == 438 and census in [438.02, 438.03]:
        return 'F6'
    elif loantype == 575 and census == 575.06:
        return '1Z'
    elif loantype == 575 and census == 575.08:
        return '5S'
    elif loantype == 434 and census == 434.04:
        return '2Z'
    elif loantype == 434 and census == 434.06:
        return '6S'
    elif loantype == 575 and census == 575.10:
        return '1H'
    elif loantype == 434 and census == 434.07:
        return '2H'
    elif loantype == 578 and census == 578.00:
        return '5Z'
    elif loantype == 570 and census == 570.02:
        return '5H'
    elif loantype == 172 and census == 172.04:
        return '6H'
    else:
        return '   '


loan_data = loan_data.with_columns([
    pl.struct(['LOANTYPE', 'CENSUS']).map_elements(
        lambda x: assign_sch(x['LOANTYPE'], x['CENSUS']),
        return_dtype=pl.Utf8
    ).alias('SCH')
])

# Filter where SCH is not blank
loan_data = loan_data.filter(pl.col('SCH') != '   ')

# Split into LOAN0 and LOAN1
loan0 = loan_data.filter((pl.col('COMMNO').is_null()) | (pl.col('COMMNO') <= 0))
loan1 = loan_data.filter(pl.col('COMMNO') > 0)


# ============================================================================
# STEP 3: PROCESS COMMISSION DATA
# ============================================================================
print("Step 3: Processing commission data...")

comm_query = """
SELECT 
    ACCTNO,
    COMMNO,
    COALESCE(CORGAMT, 0.0) - COALESCE(INTAMT, 0.0) as NETPROC
FROM read_parquet(?)
UNION ALL
SELECT 
    ACCTNO,
    COMMNO,
    COALESCE(CORGAMT, 0.0) - COALESCE(INTAMT, 0.0) as NETPROC
FROM read_parquet(?)
"""

comm_data = con.execute(comm_query, [str(LOAN_FILE), str(LOANI_FILE)]).pl()

# Merge LOAN1 with COMM
loan1 = loan1.join(comm_data, on=['ACCTNO', 'COMMNO'], how='inner')

# Combine LOAN0 and LOAN1
loan_combined = pl.concat([
    loan0.with_columns(pl.lit(None).cast(pl.Float64).alias('NETPROC')),
    loan1
])


# ============================================================================
# STEP 4: CALCULATE ARREARS AND NPL DATE
# ============================================================================
print("Step 4: Calculating arrears...")


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
        return round((nodays / 365) * 12)


# def get_issue_date(issuedt):
# # If uncomment this part, uncomment this line also -> lambda x: get_issue_date(x['ISSUEDT']),
#     """Convert SAS datetime to date"""
#     if issuedt is None or issuedt <= 0:
#         return None
#     # SAS datetime: convert from SAS format (MMDDYY with time)
#     date_str = str(int(issuedt)).zfill(11)[:8]
#     try:
#         return datetime.strptime(date_str, '%m%d%Y').date()
#     except:
#         return None

def sas_datetime_to_date(value):
    if value is None or value <= 0:
        return None
    return (datetime(1960, 1, 1) + timedelta(seconds=int(value))).date()



def calculate_npl_date(bldate, sdate):
    """Calculate NPL date (90 days after BLDATE)"""
    if bldate is None or bldate <= 0 or sdate <= bldate:
        return None

    nodays = sdate - bldate
    if nodays <= 89:
        return None

    # Convert SAS date to Python date
    base_date = datetime(1960, 1, 1).date()
    bl_date = base_date + timedelta(days=int(bldate))

    # Add 90 days
    npl_date = bl_date + timedelta(days=90)

    # Get last day of month
    last_day = calendar.monthrange(npl_date.year, npl_date.month)[1]
    npl_date = npl_date.replace(day=last_day)

    return npl_date


loan_combined = loan_combined.with_columns([
    pl.struct(['ISSUEDT']).map_elements(
        # lambda x: get_issue_date(x['ISSUEDT']),
        lambda x: sas_datetime_to_date(x['ISSUEDT']),
        return_dtype=pl.Date
    ).alias('ISSUED'),

    pl.when((pl.col('BLDATE').is_not_null()) & (pl.col('BLDATE') > 0) & (pl.lit(SDATE) > pl.col('BLDATE')))
    .then(pl.lit(SDATE) - pl.col('BLDATE'))
    .otherwise(0).alias('NODAYS')
])

loan_combined = loan_combined.with_columns([
    pl.struct(['NODAYS']).map_elements(
        lambda x: calculate_arrears(x['NODAYS']),
        return_dtype=pl.Int64
    ).alias('ARREARS'),
    pl.struct(['BLDATE']).map_elements(
        lambda x: calculate_npl_date(x['BLDATE'], SDATE),
        return_dtype=pl.Date
    ).alias('NPLDATE')
])


# ============================================================================
# STEP 5: MERGE WITH CISLN (Customer Information)
# ============================================================================
print("Step 5: Merging customer information...")

cisln_query = """
SELECT DISTINCT
    ACCTNO,
    NEWIC,
    CUSTNAME
FROM read_parquet(?)
WHERE SECCUST = '901'
"""

cisln_data = con.execute(cisln_query, [str(CISLN_FILE)]).pl()

loan_combined = loan_combined.join(cisln_data, on='ACCTNO', how='left')


# ============================================================================
# STEP 6: PROCESS COLLATERAL DATA
# ============================================================================
print("Step 6: Processing collateral data...")

coll_data = pl.read_parquet(COLL_FILE).select(['CCOLLNO', 'ACCTNO', 'NOTENO'])
desc_data = pl.read_parquet(DESC_FILE).select(['CCOLLNO', 'CINSTCL', 'NATGUAR', 'GRNTCVR', 'CENSUS'])


# Assign CR based on CENSUS
def assign_cr(census):
    """Assign CR code based on census value"""
    if census is None:
        return '  '
    census_int = int(census)
    if 51000000 <= census_int <= 51999999:
        return '51'
    elif 63000000 <= census_int <= 63999999:
        return '63'
    elif 70000000 <= census_int <= 70999999:
        return '70'
    elif 71000000 <= census_int <= 71999999:
        return '71'
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

# Filter for specific collateral types
desc_data = desc_data.filter(pl.col('CR') != '  ')
desc_data = desc_data.filter((pl.col('CINSTCL') == '18') & (pl.col('NATGUAR') == '06'))

# Merge collateral data
coll_combined = coll_data.join(desc_data, on='CCOLLNO', how='inner')

# Merge with loan data
npgs_data = loan_combined.join(coll_combined, on=['ACCTNO', 'NOTENO'], how='inner')


# ============================================================================
# STEP 7: MERGE WITH MICR DATA
# ============================================================================
print("Step 7: Merging MICR codes...")

micr_data = pl.read_parquet(MICR_FILE).select(['PENDBRH', 'BRCHCDI', 'MICRCDC', 'MICRCDI'])
# Remove last row (EOF handling in SAS)
if len(micr_data) > 0:
    micr_data = micr_data[:-1]

npgs_data = npgs_data.join(micr_data, on='PENDBRH', how='left')

# Assign MICRCD and FICODE based on COSTCTR
npgs_data = npgs_data.with_columns([
    pl.when((pl.col('COSTCTR') >= 3000) & (pl.col('COSTCTR') <= 4999))
    .then(pl.col('MICRCDI'))
    .otherwise(pl.col('MICRCDC')).alias('MICRCD'),
    pl.when((pl.col('COSTCTR') >= 3000) & (pl.col('COSTCTR') <= 4999))
    .then(pl.lit('0351'))
    .otherwise(pl.lit('0233')).alias('FICODE')
])


# ============================================================================
# STEP 8: ASSIGN CVAR02 BASED ON SCH AND CR
# ============================================================================
print("Step 8: Assigning CVAR02...")


def assign_cvar02(sch, cr, grntcvr, custcode):
    """Assign CVAR02 based on scheme, CR, and other factors"""
    if sch == 'P51' and cr in ['10', '51']:
        return '51'
    elif sch == 'P63' and cr in ['10', '63']:
        return '63'
    elif sch == 'P53' and cr == '10':
        return '53'
    elif sch == 'P64' and cr == '10':
        return '64'
    elif sch == 'P65' and cr == '10':
        return '65'
    elif sch == 'P85' and cr == '10':
        return '85'
    elif sch == 'P70' and cr == '70':
        return '70'
    elif sch == 'P70' and cr == '71':
        return '71'
    elif sch == 'P72' and cr in ['10', '72']:
        return '72'
    elif sch == 'P70' and cr == '10':
        return 'XX'
    elif sch == 'P81' and cr == '10':
        return '81'
    elif sch == 'P83' and cr == '10':
        return '83'
    elif sch == 'P57' and cr == '10':
        return 'YY'
    elif sch == 'PS2' and cr == '10':
        return 'S2'
    elif sch == 'PS3' and cr == '10':
        return 'S3'
    elif sch == 'PH4' and cr == '10':
        return 'H4'
    elif sch == 'PH5' and cr == '10':
        return 'H5'
    elif sch == 'PH6' and cr == '10':
        return 'H6'
    elif sch == 'PH7' and cr == '10':
        return 'H7'
    elif sch == 'F5' and cr == '10':
        return 'F5'
    elif sch == 'F6' and cr == '10':
        return 'F6'
    elif sch == '1Z' and cr == '10':
        if grntcvr == 80:
            return '1Z'
        elif grntcvr == 90:
            return '3Z'
    elif sch == '2Z' and cr == '10':
        if grntcvr == 80:
            return '2Z'
        elif grntcvr == 90:
            return '4Z'
    elif sch == '5S' and cr == '10':
        return '5S'
    elif sch == '6S' and cr == '10':
        return '6S'
    elif sch == '1H' and cr == '10':
        if custcode in [42, 43, 46, 47, 49, 51, 53, 54]:
            return '1H'
        elif custcode in [41, 44, 48, 52]:
            return '3H'
    elif sch == '2H' and cr == '10':
        if custcode in [42, 43, 46, 47, 49, 51, 53, 54]:
            return '2H'
        elif custcode in [41, 44, 48, 52]:
            return '4H'
    elif sch == 'E6' and cr == '10':
        return 'E6'
    elif sch == '5Z' and cr == '10':
        return '5Z'
    elif sch == '5H' and cr == '10':
        return '5H'
    elif sch == '6H' and cr == '10':
        return '6H'
    return '  '


npgs_data = npgs_data.with_columns([
    pl.struct(['SCH', 'CR', 'GRNTCVR', 'CUSTCODE']).map_elements(
        lambda x: assign_cvar02(x['SCH'], x['CR'], x['GRNTCVR'], x['CUSTCODE']),
        return_dtype=pl.Utf8
    ).alias('CVAR02')
])

# Filter where CVAR02 is not blank
npgs_data = npgs_data.filter(pl.col('CVAR02') != '  ')


# ============================================================================
# STEP 9: CREATE FINAL CVAR COLUMNS
# ============================================================================
print("Step 9: Creating final output columns...")


def format_date(date_obj):
    """Format date as DD/MM/YYYY"""
    if date_obj is None:
        return '          '
    return date_obj.strftime('%d/%m/%Y')


normdt = f"{REPTDAY}/{REPTMON}/{REPTYEAR}"

npgs_data = npgs_data.with_columns([
    pl.col('CENSUS').cast(pl.Int64).alias('CVAR01'),
    pl.col('NEWIC').cast(pl.Utf8).alias('CVAR03'),
    pl.col('CUSTNAME').cast(pl.Utf8).alias('CVAR04'),
    pl.col('ISSUED').alias('CVAR05'),
    pl.col('ACCTNO').cast(pl.Int64).alias('CVAR06'),
    pl.lit('FL').alias('CVAR07'),
    pl.col('NETPROC').cast(pl.Float64).alias('CVAR08'),
    pl.col('BALANCE').cast(pl.Float64).alias('CVAR09'),
    pl.lit(0.00).alias('CVAR10'),
    pl.col('ARREARS').cast(pl.Int64).alias('CVAR11'),
    pl.when(pl.col('ARREARS') >= 3)
    .then(pl.lit('NPL'))
    .otherwise(pl.lit('   ')).alias('CVAR12'),
    pl.struct(['NPLDATE']).map_elements(
        lambda x: format_date(x['NPLDATE']),
        return_dtype=pl.Utf8
    ).alias('CVAR13'),
    pl.col('FICODE').alias('CVAR14'),
    pl.col('MICRCD').alias('CVAR15'),
    pl.col('PENDBRH').alias('BRANCH')
])


# ============================================================================
# STEP 10: MERGE WITH NPLA (Previous NPL Status)
# ============================================================================
print("Step 10: Merging with NPLA...")

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
    # Continue without NPLA data


# ============================================================================
# STEP 11: FINAL OUTPUT
# ============================================================================
print("Step 11: Writing output...")

# Select and order final columns
final_columns = [
    'CVAR01', 'CVAR02', 'CVAR03', 'CVAR04', 'CVAR05', 'CVAR06', 'CVAR07',
    'CVAR08', 'CVAR09', 'CVAR10', 'CVAR11', 'CVAR12', 'CVAR13', 'CVAR14',
    'BRANCH', 'CVAR15', 'CENSUST', 'PRODUCT', 'NATGUAR', 'CINSTCL', 'CR', 'SCH'
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
