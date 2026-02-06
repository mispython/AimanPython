#!/usr/bin/env python3
"""
File Name: EIQPROM2
Promotional Report for Customers with Good Repayment Records
Generates reports for HL/FL/TL customers with minimum 2.5 years prompt repayment
"""

import duckdb
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import re


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# INPUT_DIR = Path("/mnt/user-data/uploads")
# OUTPUT_DIR = Path("/mnt/user-data/outputs")

BASE_DIR = Path(__file__).resolve().parent

INPUT_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

# Input files
PBBLN_FILE = INPUT_DIR / "sap_pbb_mniln_0.parquet"
PIBLN_FILE = INPUT_DIR / "sap_pibb_mniln_0.parquet"
PBBSAS_LOAN_FILE = INPUT_DIR / "sap_pbb_sasdata_loan.parquet"
PIBSAS_LOAN_FILE = INPUT_DIR / "sap_pibb_sasdata_loan.parquet"
COLL_FILE = INPUT_DIR / "sap_pbb_mnicol_0.parquet"
COLLI_FILE = INPUT_DIR / "sap_pibb_mnicol_0.parquet"
ELDS_FILE = INPUT_DIR / "sap_elds_elaa_0.parquet"
CIS_FILE = INPUT_DIR / "sap_pbb_cisbext_ln.parquet"
BILL_FILE = INPUT_DIR / "sap_pbb_promote_bill.parquet"
PREV_LOAN_FILE = INPUT_DIR / "sap_pbb_promote_loan_prev.parquet"

# Output files
SUMMARY_FILE = OUTPUT_DIR / "promote_summary.txt"
PBB_REPORT_FILE = OUTPUT_DIR / "promote_pbb.txt"
PIB_REPORT_FILE = OUTPUT_DIR / "promote_pib.txt"
LOAN_OUTPUT_FILE = OUTPUT_DIR / "promote_loan.parquet"


# ============================================================================
# PRODUCT CODE DEFINITIONS
# ============================================================================
PBBPROD = [200, 201, 204, 205, 209, 210, 211, 212, 214, 215, 219, 220,
           225, 226, 227, 228, 230, 233, 234, 235, 236, 237, 238, 239,
           240, 241, 242, 243, 300, 301, 304, 305, 359, 361, 363, 213,
           216, 217, 218, 231, 232, 244, 245, 246, 247, 315, 568, 248,
           249, 250, 348, 349, 368]

PIBPROD = [152, 153, 154, 155, 423, 423, 424, 425, 426, 175, 176, 177,
           178, 400, 401, 402, 406, 407, 408, 409, 410, 411, 412, 413,
           414, 415, 416, 419, 420, 422, 429, 430, 464]


# ============================================================================
# INITIALIZE DUCKDB CONNECTION
# ============================================================================
con = duckdb.connect()


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_branch_code(branch_num):
    """Format branch code"""
    if branch_num is None:
        return '   '
    return f'{int(branch_num):03d}'


def format_date(date_val):
    """Format date as DDMMMYY"""
    if date_val is None:
        return '        '
    if isinstance(date_val, (int, float)):
        base_date = datetime(1960, 1, 1).date()
        date_val = base_date + timedelta(days=int(date_val))
    if isinstance(date_val, datetime):
        date_val = date_val.date()
    return date_val.strftime('%d%b%y').upper()


def write_asa_line(f, line, line_type=' '):
    """Write line with ASA carriage control character"""
    f.write(f"{line_type}{line}\n")


# ============================================================================
# STEP 1: READ REPTDATE AND SET VARIABLES
# ============================================================================
print("Step 1: Reading report date...")

reptdate_df = pl.read_parquet(PBBLN_FILE).select(['REPTDATE']).head(1)
reptdate = reptdate_df['REPTDATE'][0]

# Determine week number
if 1 <= reptdate.day <= 8:
    NOWK = '1'
elif 9 <= reptdate.day <= 15:
    NOWK = '2'
elif 16 <= reptdate.day <= 22:
    NOWK = '3'
else:
    NOWK = '4'

# Calculate previous month date
prevdate = reptdate.replace(day=1) - timedelta(days=1)
PREVMON = f"{prevdate.month:02d}"

# Calculate promotion date (2.5 years back)
if reptdate.month >= 6:
    pmth = reptdate.month - 5
    pyear = reptdate.year - 2
else:
    pmth = reptdate.month + 7
    pyear = reptdate.year - 3

prdate = datetime(pyear, pmth, 1).date()
PRDATE = (prdate - datetime(1960, 1, 1).date()).days  # SAS date

REPTMON = f"{reptdate.month:02d}"
RDATE = reptdate.strftime('%d/%m/%y')
REPTDT = (reptdate - datetime(1960, 1, 1).date()).days

print(f"Report Date: {reptdate} ({RDATE})")
print(f"Promotion Date Cutoff: {prdate}")
print(f"Week: {NOWK}, Previous Month: {PREVMON}")

# Update file paths with week suffix
PBBSAS_LOAN_FILE = INPUT_DIR / f"sap_pbb_sasdata_loan{REPTMON}{NOWK}.parquet"
PIBSAS_LOAN_FILE = INPUT_DIR / f"sap_pibb_sasdata_loan{REPTMON}{NOWK}.parquet"
PREV_LOAN_FILE = INPUT_DIR / f"sap_pbb_promote_loan{PREVMON}.parquet"


# ============================================================================
# STEP 2: PROCESS LNNOTE DATA
# ============================================================================
print("Step 2: Processing loan note data...")

# Read PBB loan notes
pbb_lnnote = pl.read_parquet(PBBLN_FILE).filter(
    (pl.col('LOANTYPE').is_in(PBBPROD + PIBPROD)) &
    (pl.col('LOANSTAT') != 3) &
    (pl.col('CURBAL') > 0) &
    (pl.col('LSTTRNCD') != 661)
)

# Read PIB loan notes
pib_lnnote = pl.read_parquet(PIBLN_FILE).filter(
    (pl.col('LOANTYPE').is_in(PBBPROD + PIBPROD)) &
    (pl.col('LOANSTAT') != 3) &
    (pl.col('CURBAL') > 0) &
    (pl.col('LSTTRNCD') != 661)
)

lnnote_data = pl.concat([pbb_lnnote, pib_lnnote])


# Process AANO (extract and clean VINNO)
def extract_aano(vinno):
    """Extract A/A number from VINNO"""
    if vinno is None:
        return ' '
    vinno_str = str(vinno).strip()[:13]
    # Remove special characters
    comp = re.sub(r'[@*(#)\-]', '', vinno_str)
    comp = comp.strip()
    if len(comp) == 13:
        return comp
    return ' '


lnnote_data = lnnote_data.with_columns([
    pl.struct(['VINNO']).map_elements(
        lambda x: extract_aano(x['VINNO']),
        return_dtype=pl.Utf8
    ).alias('AANO')
])

# Filter where COLLMAKE and COLLYEAR are empty/zero
lnnote_data = lnnote_data.filter(
    (pl.col('COLLMAKE').is_in(['.', '0', '', None])) &
    ((pl.col('COLLYEAR').is_null()) | (pl.col('COLLYEAR') == 0))
)

lnnote_data = lnnote_data.select([
    'ACCTNO', 'NOTENO', 'NAME', 'FLAG1', 'ORGBAL', 'NETPROC', 'AANO',
    'COLLMAKE', 'COLLYEAR', 'NTINDEX', 'SPREAD', 'MODELDES',
    'SCORE1', 'IA_LRU', 'BORSTAT', 'DELQCD', 'GUAREND', 'MAILCODE'
])

print(f"LNNOTE records: {len(lnnote_data)}")


# ============================================================================
# STEP 3: PROCESS LOAN DATA
# ============================================================================
print("Step 3: Processing loan data...")

# Read PBB loans
pbb_loan = pl.read_parquet(PBBSAS_LOAN_FILE).filter(
    (pl.col('PRODUCT').is_in(PBBPROD + PIBPROD)) &
    (pl.col('LOANSTAT') != 3) &
    (pl.col('CURBAL') > 0)
)

# Read PIB loans
pib_loan = pl.read_parquet(PIBSAS_LOAN_FILE).filter(
    (pl.col('PRODUCT').is_in(PBBPROD + PIBPROD)) &
    (pl.col('LOANSTAT') != 3) &
    (pl.col('CURBAL') > 0)
)

loan_data = pl.concat([pbb_loan, pib_loan])

# Calculate DAYDIFF
loan_data = loan_data.with_columns([
    pl.when((pl.col('BLDATE').is_not_null()) & (pl.col('BLDATE') > 0))
    .then(pl.lit(REPTDT) - pl.col('BLDATE'))
    .otherwise(0).alias('DAYDIFF'),
    pl.when((pl.col('EXPRDATE').is_not_null()) & (pl.col('EXPRDATE') > 0))
    .then(
        pl.struct(['EXPRDATE']).map_elements(
            lambda x: (datetime(1960, 1, 1).date() + timedelta(days=int(x['EXPRDATE']))).year - reptdate.year,
            return_dtype=pl.Int64
        )
    )
    .otherwise(None).alias('REMTERM'),
    pl.col('NOTENO').cast(pl.Utf8).str.slice(0, 1).alias('NOTE1'),
    pl.col('NOTENO').cast(pl.Utf8).str.slice(1, 1).alias('NOTE2')
])

loan_data = loan_data.select([
    'ACCTNO', 'NOTENO', 'COMMNO', 'ISSDTE', 'DAYDIFF', 'PRODUCT',
    'NOTE1', 'NOTE2', 'BRANCH', 'APPRLIMT', 'BALANCE', 'EXPRDATE', 'REMTERM'
])

print(f"LOAN records: {len(loan_data)}")


# ============================================================================
# STEP 4: FILTER OUT BILLED ACCOUNTS
# ============================================================================
print("Step 4: Filtering billed accounts...")

try:
    billbad_data = pl.read_parquet(BILL_FILE).select(['ACCTNO', 'NOTENO']).unique()
    loan_data = loan_data.join(
        billbad_data.with_columns(pl.lit(True).alias('_bill_flag')),
        on=['ACCTNO', 'NOTENO'],
        how='left'
    ).filter(pl.col('_bill_flag').is_null()).drop('_bill_flag')
except Exception as e:
    print(f"Warning: Could not read BILL file: {e}")


# ============================================================================
# STEP 5: MERGE LOAN WITH LNNOTE
# ============================================================================
print("Step 5: Merging loan with note data...")

loan_data = loan_data.join(lnnote_data, on=['ACCTNO', 'NOTENO'], how='inner')


# ============================================================================
# STEP 6: PROCESS COMMISSION DATA
# ============================================================================
print("Step 6: Processing commission data...")

# Read commission data
pbb_lncomm = pl.read_parquet(PBBLN_FILE).select(['ACCTNO', 'COMMNO', 'CORGAMT'])
pib_lncomm = pl.read_parquet(PIBLN_FILE).select(['ACCTNO', 'COMMNO', 'CORGAMT'])

lncomm_data = pl.concat([pbb_lncomm, pib_lncomm]).unique(subset=['ACCTNO', 'COMMNO'])

# Merge with loan data
loan_data = loan_data.join(lncomm_data, on=['ACCTNO', 'COMMNO'], how='left')


# ============================================================================
# STEP 7: APPLY FILTERING CRITERIA
# ============================================================================
print("Step 7: Applying filtering criteria...")

# Calculate LMTAPPR and REPAID
loan_data = loan_data.with_columns([
    pl.max_horizontal(['CORGAMT', 'ORGBAL', 'NETPROC', 'APPRLIMT']).alias('LMTAPPR')
])

loan_data = loan_data.with_columns([
    (pl.col('LMTAPPR') - pl.col('BALANCE')).alias('REPAID')
])

# Apply main filters
loan_data = loan_data.filter(
    (pl.col('BALANCE') > 30000) &
    (pl.col('ISSDTE') < pl.lit(PRDATE)) &
    (pl.col('FLAG1') == 'F') &
    (pl.col('DAYDIFF') <= 0) &
    (pl.col('PRODUCT').is_in(PBBPROD + PIBPROD)) &
    (pl.col('NOTE1') != '1') &
    (pl.col('NOTE2') != '2')
)

# Apply exclusion criteria
loan_data = loan_data.filter(
    ~(
            ((pl.col('NTINDEX') == 1) & (pl.col('SPREAD') == 0.00)) |
            ((pl.col('NTINDEX') == 1) & (pl.col('SPREAD') == 3.50)) |
            ((pl.col('NTINDEX') == 38) & (pl.col('SPREAD') == 3.20)) |
            ((pl.col('NTINDEX') == 38) & (pl.col('SPREAD') == 6.70)) |
            (pl.col('MODELDES').str.slice(0, 1).is_in(['S', 'T', 'R', 'C'])) |
            (pl.col('MODELDES') == 'Z') |
            (pl.col('MODELDES').str.slice(4, 1) == 'F') |
            (pl.col('SCORE1').str.slice(0, 1).is_in(['D', 'E', 'F', 'G', 'H', 'I'])) |
            (pl.col('IA_LRU') == 'I') |
            (pl.col('BORSTAT') == 'K') |
            (pl.col('DELQCD').is_in(['9', '09', '10', '11', '12', '13', '14',
                                     '15', '16', '17', '18', '19', '20']))
    )
)

print(f"After filtering: {len(loan_data)} records")


# ============================================================================
# STEP 8: PROCESS COLLATERAL DATA
# ============================================================================
print("Step 8: Processing collateral data...")

coll_data = pl.read_parquet(COLL_FILE).select([
    'ACCTNO', 'NOTENO', 'CPRPROPD', 'MRESERVE', 'CPRLANDU', 'HOLDEXPD', 'EXPDATE'
])
colli_data = pl.read_parquet(COLLI_FILE).select([
    'ACCTNO', 'NOTENO', 'CPRPROPD', 'MRESERVE', 'CPRLANDU', 'HOLDEXPD', 'EXPDATE'
])

coll_combined = pl.concat([coll_data, colli_data])

# Mark exclusions
coll_combined = coll_combined.with_columns([
    pl.when(
        ((pl.col('CPRPROPD').is_in(['10', '11', '32', '33', '34', '35'])) &
         (pl.col('CPRLANDU').is_in(['10', '11', '32', '33', '34', '35']))) |
        (pl.col('MRESERVE') == 'Y')
    ).then(pl.lit('Y')).otherwise(None).alias('EXCL')
])


# Convert EXPDATE if needed
def convert_expdate(expdate):
    """Convert EXPDATE to date"""
    if expdate is None or expdate <= 0:
        return None
    date_str = str(int(expdate)).zfill(8)
    try:
        return datetime.strptime(date_str, '%d%m%Y').date()
    except:
        return None


coll_combined = coll_combined.with_columns([
    pl.struct(['EXPDATE']).map_elements(
        lambda x: convert_expdate(x['EXPDATE']),
        return_dtype=pl.Date
    ).alias('EXPDT')
])

# Filter collateral
coll_combined = coll_combined.filter(
    (pl.col('EXCL') == 'Y') | (pl.col('HOLDEXPD') == 'L')
)

# Merge with loan and apply exclusions
loan_data = loan_data.join(coll_combined, on=['ACCTNO', 'NOTENO'], how='left')

# Calculate year difference for expiry date check
loan_data = loan_data.filter(
    (pl.col('EXCL') != 'Y') &
    ~(
            (pl.col('HOLDEXPD') == 'L') &
            (pl.col('EXPRDATE').is_not_null()) &
            (pl.col('EXPDT').is_not_null()) &
            (pl.struct(['EXPRDATE', 'EXPDT']).map_elements(
                lambda x: abs((datetime(1960, 1, 1).date() + timedelta(days=int(x['EXPRDATE'])) - x[
                    'EXPDT']).days / 365.25) < 30 if x['EXPDT'] else False,
                return_dtype=pl.Boolean
            ))
    )
)

print(f"After collateral filter: {len(loan_data)} records")


# ============================================================================
# STEP 9: FILTER CIS CUSTOMERS
# ============================================================================
print("Step 9: Filtering CIS customers...")

cis_data = pl.read_parquet(CIS_FILE).select(['ACCTNO', 'CUSTNO', 'EMAILADD', 'SECCUST', 'NEWIC'])

# Separate CISBILL (bill accounts)
cisbill = cis_data.filter(
    ((pl.col('ACCTNO') >= 2500000000) & (pl.col('ACCTNO') <= 2599999999)) |
    ((pl.col('ACCTNO') >= 2850000000) & (pl.col('ACCTNO') <= 2859999999))
).select(['CUSTNO']).unique()

cisln = cis_data.filter(
    ~(
            ((pl.col('ACCTNO') >= 2500000000) & (pl.col('ACCTNO') <= 2599999999)) |
            ((pl.col('ACCTNO') >= 2850000000) & (pl.col('ACCTNO') <= 2859999999))
    )
)

# Find accounts that are also bill accounts
cisln_bill = cisln.join(cisbill, on='CUSTNO', how='inner').select(['ACCTNO']).unique()

# Exclude these accounts
loan_data = loan_data.join(
    cisln_bill.with_columns(pl.lit(True).alias('_cisbill_flag')),
    on='ACCTNO',
    how='left'
).filter(pl.col('_cisbill_flag').is_null()).drop('_cisbill_flag')

# Merge with CIS data for SECCUST='901'
cis_901 = cis_data.filter(pl.col('SECCUST') == '901')
loan_data = loan_data.join(cis_901.select(['ACCTNO', 'NEWIC', 'EMAILADD']), on='ACCTNO', how='left')

print(f"After CIS filter: {len(loan_data)} records")


# ============================================================================
# STEP 10: FILTER ELDS REINSTATEMENT
# ============================================================================
print("Step 10: Filtering ELDS reinstatement...")

try:
    elds_data = pl.read_parquet(ELDS_FILE).select(['AANO', 'REINPROD']).unique()
    loan_data = loan_data.join(elds_data, on='AANO', how='left')
    loan_data = loan_data.filter(pl.col('REINPROD') != 'Y')
except Exception as e:
    print(f"Warning: Could not read ELDS file: {e}")

print(f"Final loan records: {len(loan_data)}")


# ============================================================================
# STEP 11: GENERATE SUMMARY REPORT
# ============================================================================
print("Step 11: Generating summary report...")

# Add BRCH formatted field
loan_data = loan_data.with_columns([
    pl.struct(['BRANCH']).map_elements(
        lambda x: get_branch_code(x['BRANCH']),
        return_dtype=pl.Utf8
    ).alias('BRCH')
])

# Summarize by branch
billsum = loan_data.group_by('BRCH').agg([
    pl.count().alias('NOACCT'),
    pl.col('LMTAPPR').sum().alias('LMTAPPR'),
    pl.col('BALANCE').sum().alias('BALANCE'),
    pl.col('REPAID').sum().alias('REPAID')
]).sort('BRCH')

# Write summary report
with open(SUMMARY_FILE, 'w') as f:
    write_asa_line(f, '1' + ' ' * 134, '1')  # Page eject
    write_asa_line(f, 'PBB HL/FL/TL CUSTOMER WITH MINIMUM 2 1/2 YEARS')
    write_asa_line(f, f'PROMPT REPAYMENT RECORD AS AT {RDATE}')
    write_asa_line(f, 'REPORT ID : EIQPROM2')
    write_asa_line(f, '')
    write_asa_line(f, f"{'BRANCH':<10}{'TOTAL NO.':<12}{'FACILITY/':<20}{'OUTSTANDING':<20}{'AMOUNT':<20}")
    write_asa_line(f, f"{' ' * 10}{'OF A/C':<12}{'APPROVED LIMIT':<20}{'BALANCE':<20}{'REPAID':<20}")
    write_asa_line(f, '-' * 82)

    total_noacct = 0
    total_lmtappr = 0.0
    total_balance = 0.0
    total_repaid = 0.0

    for row in billsum.iter_rows(named=True):
        brch = row['BRCH']
        noacct = row['NOACCT']
        lmtappr = row['LMTAPPR'] or 0.0
        balance = row['BALANCE'] or 0.0
        repaid = row['REPAID'] or 0.0

        write_asa_line(f, f"{brch:<10}{noacct:>12,}{lmtappr:>20,.2f}{balance:>20,.2f}{repaid:>20,.2f}")

        total_noacct += noacct
        total_lmtappr += lmtappr
        total_balance += balance
        total_repaid += repaid

    write_asa_line(f, '=' * 82)
    write_asa_line(f,
                   f"{'TOTAL':<10}{total_noacct:>12,}{total_lmtappr:>20,.2f}{total_balance:>20,.2f}{total_repaid:>20,.2f}")

print(f"Summary report written to: {SUMMARY_FILE}")


# ============================================================================
# STEP 12: MARK NEW ACCOUNTS
# ============================================================================
print("Step 12: Marking new accounts...")

try:
    prev_loan = pl.read_parquet(PREV_LOAN_FILE).select(['ACCTNO', 'NOTENO'])
    loan_data = loan_data.join(
        prev_loan.with_columns(pl.lit('').alias('NEW')),
        on=['ACCTNO', 'NOTENO'],
        how='left'
    )
    loan_data = loan_data.with_columns([
        pl.when(pl.col('NEW').is_null()).then(pl.lit('Y')).otherwise(pl.lit('')).alias('NEW')
    ])
except Exception as e:
    print(f"Warning: Could not read previous month file: {e}")
    loan_data = loan_data.with_columns([pl.lit('Y').alias('NEW')])

# Save current month loan data
loan_data.write_parquet(LOAN_OUTPUT_FILE)
print(f"Loan data saved to: {LOAN_OUTPUT_FILE}")


# ============================================================================
# STEP 13: SPLIT INTO PBB AND PIB
# ============================================================================
print("Step 13: Splitting PBB and PIB data...")

loan_pbb = loan_data.filter(pl.col('PRODUCT').is_in(PBBPROD)).sort(['BRANCH', 'ACCTNO'])
loan_pib = loan_data.filter(pl.col('PRODUCT').is_in(PIBPROD)).sort(['BRANCH', 'ACCTNO'])

print(f"PBB records: {len(loan_pbb)}, PIB records: {len(loan_pib)}")


# ============================================================================
# STEP 14: GENERATE DETAIL REPORTS
# ============================================================================
print("Step 14: Generating detail reports...")


def write_detail_report(data, filename, bank_name):
    """Write detailed report for PBB or PIB"""
    with open(filename, 'w') as f:
        current_branch = None
        linecnt = 0
        pagecnt = 0
        accnt = 0
        aclimt = 0.0
        acbal = 0.0
        acpaid = 0.0

        for row in data.iter_rows(named=True):
            branch = row['BRANCH']

            # Check if new branch
            if current_branch != branch:
                # Print branch totals for previous branch
                if current_branch is not None:
                    write_asa_line(f, ' ' + '-' * 131)
                    write_asa_line(f,
                                   f" {'NO OF A/C :':<15}{accnt:>8,}{' ' * 54}{aclimt:>12.2f}{acbal:>13.2f}{acpaid:>12.2f}")
                    write_asa_line(f, ' ' + '=' * 131)
                    linecnt += 4

                # Reset counters for new branch
                current_branch = branch
                pagecnt = 0
                accnt = 0
                aclimt = 0.0
                acbal = 0.0
                acpaid = 0.0
                linecnt = 999  # Force new page

            # Check if new page needed
            if linecnt > 55:
                pagecnt += 1
                write_asa_line(f, f"1BRANCH    : {branch:3.0f}{' ' * 36}{bank_name:^50}{' ' * 24}PAGE NO : {pagecnt}",
                               '1')
                write_asa_line(f, f" REPORT NO : EIQPROM2{' ' * 22}HL/FL/TL CUSTOMER WITH MINIMUM 2 1/2 YEARS")
                write_asa_line(f, f"{' ' * 47}PROMPT REPAYMENT RECORD AS AT {RDATE}")
                write_asa_line(f, ' ')
                write_asa_line(f,
                               f"{' ' * 19}NOTE{' ' * 15}PRODUCT{' ' * 30}RELEASED{' ' * 8}APPROVED{' ' * 13}O/S{' ' * 13}AMT{' ' * 5}REMAINING")
                write_asa_line(f,
                               f" BRANCH{' ' * 7}A/C NO{' ' * 8}NO{' ' * 5}A/A NO{' ' * 13}CODE{' ' * 4}NAME OF CUSTOMER{' ' * 20}DATE{' ' * 7}LIMIT{' ' * 9}BALANCE{' ' * 7}REPAID{' ' * 5}TENURE{' ' * 2}NEW")
                write_asa_line(f, ' ' + '-' * 131)
                write_asa_line(f, ' ')
                linecnt = 10

            # Write detail line
            acctno = row['ACCTNO']
            noteno = row['NOTENO']
            aano = row['AANO'] or ' '
            product = row['PRODUCT']
            name = (row['NAME'] or '')[:23]
            issdte = format_date(row['ISSDTE'])
            lmtappr = row['LMTAPPR'] or 0.0
            balance = row['BALANCE'] or 0.0
            repaid = row['REPAID'] or 0.0
            remterm = row['REMTERM'] or 0
            new = row['NEW'] or ''

            line = f" {branch:3.0f}{acctno:>14.0f}{noteno:>6.0f}{aano:>15}{product:>5.0f}  {name:<23}{issdte:>9}{lmtappr:>13.2f}{balance:>13.2f}{repaid:>12.2f}{remterm:>6.0f}   {new:1}"
            write_asa_line(f, line)
            linecnt += 1

            # Update totals
            accnt += 1
            aclimt += lmtappr
            acbal += balance
            acpaid += repaid

        # Print final branch totals
        if current_branch is not None:
            write_asa_line(f, ' ' + '-' * 131)
            write_asa_line(f, f" {'NO OF A/C :':<15}{accnt:>8,}{' ' * 54}{aclimt:>12.2f}{acbal:>13.2f}{acpaid:>12.2f}")
            write_asa_line(f, ' ' + '=' * 131)


# Write PBB report
write_detail_report(loan_pbb, PBB_REPORT_FILE, 'P U B L I C   B A N K   B E R H A D')
print(f"PBB report written to: {PBB_REPORT_FILE}")

# Write PIB report
write_detail_report(loan_pib, PIB_REPORT_FILE, 'P U B L I C   I S L A M I C   B A N K   B E R H A D')
print(f"PIB report written to: {PIB_REPORT_FILE}")

print("\nProcessing complete!")

# Close DuckDB connection
con.close()
