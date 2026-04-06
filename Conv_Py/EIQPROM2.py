#!/usr/bin/env python3
"""
Program : EIQPROM2.py
Purpose : Promotional Report for Customers with Good Repayment Records
          Generates reports for HL/FL/TL customers with minimum 2.5 years
          prompt repayment
"""

# %INC PGM(PBBELF)
#
# PBBELF: provides the BRCHCD format — PUT(BRANCH, BRCHCD.) — actively used
#         in the DATA LOAN step:
#             BRCH = PUT(BRANCH, BRCHCD.);
#         BRCH is then used as the CLASS variable in PROC SUMMARY and as the
#         ORDER variable in PROC REPORT (DEFINE BRCH / ORDER FORMAT=$6.).
#         Without the correct branch-name mapping the summary report shows
#         raw numeric codes instead of 3-letter branch names.
#         Imported below as format_brchcd().

from PBBELF import format_brchcd   # BRCHCD format: branch numeric -> 3-letter name

import duckdb
import polars as pl
from datetime import datetime, timedelta, date
from pathlib import Path


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path(__file__).resolve().parent

INPUT_DIR  = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input files
PBBLN_FILE      = INPUT_DIR / "sap_pbb_mniln_0.parquet"
PIBLN_FILE      = INPUT_DIR / "sap_pibb_mniln_0.parquet"
COLL_FILE       = INPUT_DIR / "sap_pbb_mnicol_0.parquet"
COLLI_FILE      = INPUT_DIR / "sap_pibb_mnicol_0.parquet"
ELDS_FILE       = INPUT_DIR / "sap_elds_elaa_0.parquet"
CIS_FILE        = INPUT_DIR / "sap_pbb_cisbext_ln.parquet"
BILL_FILE       = INPUT_DIR / "sap_pbb_promote_bill.parquet"

# Dynamic input files — resolved after REPTMON/NOWK are known
# PBBSAS_LOAN_FILE = INPUT_DIR / f"sap_pbb_sasdata_loan{REPTMON}{NOWK}.parquet"
# PIBSAS_LOAN_FILE = INPUT_DIR / f"sap_pibb_sasdata_loan{REPTMON}{NOWK}.parquet"
# PREV_LOAN_FILE   = INPUT_DIR / f"sap_pbb_promote_loan{PREVMON}.parquet"

# Output files
SUMMARY_FILE   = OUTPUT_DIR / "promote_summary.txt"
PBB_REPORT_FILE = OUTPUT_DIR / "promote_pbb.txt"
PIB_REPORT_FILE = OUTPUT_DIR / "promote_pib.txt"
LOAN_OUTPUT_FILE = OUTPUT_DIR / "promote_loan.parquet"


# ============================================================================
# PRODUCT CODE DEFINITIONS
# %LET PBBPROD=(200,201,204,...) / %LET PIBPROD=(152,153,...)
# ============================================================================
PBBPROD = [200, 201, 204, 205, 209, 210, 211, 212, 214, 215, 219, 220,
           225, 226, 227, 228, 230, 233, 234, 235, 236, 237, 238, 239,
           240, 241, 242, 243, 300, 301, 304, 305, 359, 361, 363, 213,
           216, 217, 218, 231, 232, 244, 245, 246, 247, 315, 568, 248,
           249, 250, 348, 349, 368]

PIBPROD = [152, 153, 154, 155, 423, 423, 424, 425, 426, 175, 176, 177,
           178, 400, 401, 402, 406, 407, 408, 409, 410, 411, 412, 413,
           414, 415, 416, 419, 420, 422, 429, 430, 464]

ALL_PROD = list(set(PBBPROD + PIBPROD))


# ============================================================================
# INITIALIZE DUCKDB CONNECTION
# ============================================================================
con = duckdb.connect()


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def format_date(date_val) -> str:
    """Format date as DATE8. equivalent: DDMMMYY (e.g. 15JAN24)."""
    if date_val is None:
        return '        '
    if isinstance(date_val, (int, float)):
        date_val = date(1960, 1, 1) + timedelta(days=int(date_val))
    if isinstance(date_val, datetime):
        date_val = date_val.date()
    return date_val.strftime('%d%b%y').upper()


def sas_date_int(d: date) -> int:
    """Convert Python date to SAS date integer (days since 1960-01-01)."""
    return (d - date(1960, 1, 1)).days


def fmt_put5(noteno) -> str:
    """
    Replicate PUT(NOTENO, 5.) — right-justify numeric in 5-char field.
    SAS PUT with a w. format right-justifies with leading spaces.
    """
    try:
        return f"{int(noteno):5d}"
    except (ValueError, TypeError):
        return '     '


# ============================================================================
# STEP 1: READ REPTDATE AND SET MACRO VARIABLES
# DATA REPTDATE (KEEP=REPTDATE):
#   SET PBBLN.REPTDATE;
#   WHEN(1<=DAY(REPTDATE)<=8) CALL SYMPUT('NOWK','1');
#   ...
#   CALL SYMPUT('PREVMON', PUT(MONTH(PREVDATE), Z2.));
# ============================================================================
print("Step 1: Reading report date...")

reptdate_df = pl.read_parquet(PBBLN_FILE).select(['REPTDATE']).head(1)
reptdate    = reptdate_df['REPTDATE'][0]

# Ensure Python date object
if isinstance(reptdate, datetime):
    reptdate = reptdate.date()

# Determine week number
d = reptdate.day
if   1 <= d <= 8:  NOWK = '1'
elif 9 <= d <= 15: NOWK = '2'
elif 16 <= d <= 22: NOWK = '3'
else:               NOWK = '4'

# PREVDATE = REPTDATE - DAY(REPTDATE)  (last day of previous month)
prevdate = reptdate.replace(day=1) - timedelta(days=1)
PREVMON  = f"{prevdate.month:02d}"

# Promotion date cutoff: 2.5 years back
# IF MONTH(REPTDATE) >= 6 THEN PMTH=MONTH-5, PYEAR=YEAR-2
# ELSE                         PMTH=MONTH+7, PYEAR=YEAR-3
if reptdate.month >= 6:
    pmth  = reptdate.month - 5
    pyear = reptdate.year  - 2
else:
    pmth  = reptdate.month + 7
    pyear = reptdate.year  - 3

prdate   = date(pyear, pmth, 1)
PRDATE   = sas_date_int(prdate)   # &PRDATE as SAS integer
REPTMON  = f"{reptdate.month:02d}"
RDATE    = reptdate.strftime('%d/%m/%y')
REPTDT   = sas_date_int(reptdate)  # &REPTDT as SAS integer

print(f"Report Date : {reptdate} ({RDATE})")
print(f"Promo Cutoff: {prdate}")
print(f"Week        : {NOWK}, Previous Month: {PREVMON}")

# Resolve dynamic input file paths now that REPTMON/NOWK are known
PBBSAS_LOAN_FILE = INPUT_DIR / f"sap_pbb_sasdata_loan{REPTMON}{NOWK}.parquet"
PIBSAS_LOAN_FILE = INPUT_DIR / f"sap_pibb_sasdata_loan{REPTMON}{NOWK}.parquet"
PREV_LOAN_FILE   = INPUT_DIR / f"sap_pbb_promote_loan{PREVMON}.parquet"


# ============================================================================
# STEP 2: PROCESS LNNOTE DATA
# DATA LNNOTE(KEEP=ACCTNO NOTENO NAME FLAG1 ORGBAL NETPROC AANO
#                  COLLMAKE COLLYEAR NTINDEX SPREAD MODELDES
#                  SCORE1 IA_LRU BORSTAT DELQCD GUAREND MAILCODE):
#   SET PBBLN.LNNOTE PIBLN.LNNOTE;
#   WHERE (LOANTYPE IN &PBBPROD OR LOANTYPE IN &PIBPROD) AND
#         LOANSTAT NE 3 AND CURBAL GT 0 AND LSTTRNCD NE 661;
#   AANO1 = SUBSTR(LEFT(VINNO),1,13);
#   COMP  = COMPRESS(AANO1,'@*(#)-');
#   COMP  = COMPRESS(COMP);          <- removes ALL spaces from COMP
#   IF LENGTH(COMP) = 13 THEN AANO = COMP; ELSE AANO = ' ';
#   IF COLLMAKE IN ('.','0','') AND COLLYEAR IN (.,0);
# ============================================================================
print("Step 2: Processing loan note data...")


def extract_aano(vinno) -> str:
    """
    Replicate SAS AANO derivation:
      AANO1 = SUBSTR(LEFT(VINNO), 1, 13)   <- left-align then take first 13 chars
      COMP  = COMPRESS(AANO1, '@*(#)-')    <- remove those specific chars
      COMP  = COMPRESS(COMP)               <- remove ALL whitespace (not just strip)
      IF LENGTH(COMP) = 13 THEN AANO = COMP; ELSE AANO = ' '
    Key: second COMPRESS() removes ALL embedded spaces, not just leading/trailing.
    """
    if vinno is None:
        return ' '
    vinno_str = str(vinno).strip()          # LEFT() equivalent — strip leading spaces
    vinno_str = vinno_str[:13]              # SUBSTR(...,1,13)
    # COMPRESS(AANO1,'@*(#)-') — remove these specific characters
    for ch in '@*(#)-':
        vinno_str = vinno_str.replace(ch, '')
    # COMPRESS(COMP) with no list arg — removes ALL whitespace characters
    vinno_str = vinno_str.replace(' ', '').replace('\t', '')
    if len(vinno_str) == 13:
        return vinno_str
    return ' '


pbb_lnnote = pl.read_parquet(PBBLN_FILE).filter(
    (pl.col('LOANTYPE').is_in(ALL_PROD)) &
    (pl.col('LOANSTAT') != 3) &
    (pl.col('CURBAL') > 0) &
    (pl.col('LSTTRNCD') != 661)
)
pib_lnnote = pl.read_parquet(PIBLN_FILE).filter(
    (pl.col('LOANTYPE').is_in(ALL_PROD)) &
    (pl.col('LOANSTAT') != 3) &
    (pl.col('CURBAL') > 0) &
    (pl.col('LSTTRNCD') != 661)
)

lnnote_data = pl.concat([pbb_lnnote, pib_lnnote])

lnnote_data = lnnote_data.with_columns([
    pl.col('VINNO').map_elements(extract_aano, return_dtype=pl.Utf8).alias('AANO')
])

# IF COLLMAKE IN ('.','0','') AND COLLYEAR IN (.,0)
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
# DATA LOAN(KEEP=ACCTNO NOTENO COMMNO ISSDTE DAYDIFF PRODUCT
#                NOTE1 NOTE2 BRANCH APPRLIMT BALANCE EXPRDATE REMTERM):
#   SET PBBSAS.LOAN&REPTMON&NOWK PIBSAS.LOAN&REPTMON&NOWK;
#   WHERE (PRODUCT IN &PBBPROD OR PRODUCT IN &PIBPROD) AND
#         LOANSTAT NE 3 AND CURBAL GT 0;
#   THISDATE = INPUT("&RDATE", DDMMYY8.);
#   IF BLDATE > 0 THEN DAYDIFF = THISDATE - BLDATE; ELSE DAYDIFF = 0;
#   IF EXPRDATE > 0 THEN REMTERM = YEAR(EXPRDATE) - YEAR(&REPTDT);
#   NOTE1 = SUBSTR(PUT(NOTENO,5.),1,1);
#   NOTE2 = SUBSTR(PUT(NOTENO,5.),2,1);
# ============================================================================
print("Step 3: Processing loan data...")


def compute_daydiff(bldate_val, reptdt_int: int):
    """
    SAS: IF BLDATE > 0 THEN DAYDIFF = THISDATE - BLDATE; ELSE DAYDIFF = 0;
    Both THISDATE and BLDATE are SAS date integers.
    """
    try:
        bd = int(bldate_val)
        if bd > 0:
            return reptdt_int - bd
    except (TypeError, ValueError):
        pass
    return 0


def compute_remterm(exprdate_val, reptdate: date):
    """
    SAS: IF EXPRDATE > 0 THEN REMTERM = YEAR(EXPRDATE) - YEAR(&REPTDT);
    EXPRDATE is a SAS date integer.
    """
    try:
        ed = int(exprdate_val)
        if ed > 0:
            expr_dt = date(1960, 1, 1) + timedelta(days=ed)
            return expr_dt.year - reptdate.year
    except (TypeError, ValueError):
        pass
    return None


pbb_loan = pl.read_parquet(PBBSAS_LOAN_FILE).filter(
    (pl.col('PRODUCT').is_in(ALL_PROD)) &
    (pl.col('LOANSTAT') != 3) &
    (pl.col('CURBAL') > 0)
)
pib_loan = pl.read_parquet(PIBSAS_LOAN_FILE).filter(
    (pl.col('PRODUCT').is_in(ALL_PROD)) &
    (pl.col('LOANSTAT') != 3) &
    (pl.col('CURBAL') > 0)
)

loan_data = pl.concat([pbb_loan, pib_loan])

# DAYDIFF = THISDATE - BLDATE  (both are SAS date integers in the parquet)
loan_data = loan_data.with_columns([
    pl.col('BLDATE').map_elements(
        lambda v: compute_daydiff(v, REPTDT), return_dtype=pl.Int64
    ).alias('DAYDIFF'),
    pl.col('EXPRDATE').map_elements(
        lambda v: compute_remterm(v, reptdate), return_dtype=pl.Int64
    ).alias('REMTERM'),
    # NOTE1 = SUBSTR(PUT(NOTENO,5.),1,1)
    # PUT(NOTENO,5.) right-justifies in 5 chars with leading spaces
    pl.col('NOTENO').map_elements(
        lambda v: fmt_put5(v)[0], return_dtype=pl.Utf8
    ).alias('NOTE1'),
    # NOTE2 = SUBSTR(PUT(NOTENO,5.),2,1)
    pl.col('NOTENO').map_elements(
        lambda v: fmt_put5(v)[1], return_dtype=pl.Utf8
    ).alias('NOTE2'),
])

loan_data = loan_data.select([
    'ACCTNO', 'NOTENO', 'COMMNO', 'ISSDTE', 'DAYDIFF', 'PRODUCT',
    'NOTE1', 'NOTE2', 'BRANCH', 'APPRLIMT', 'BALANCE', 'EXPRDATE', 'REMTERM'
])

print(f"LOAN records: {len(loan_data)}")


# ============================================================================
# STEP 4: FILTER OUT BILLED ACCOUNTS
# PROC SORT DATA=LNBILL.BILL OUT=BILLBAD NODUPKEYS; BY ACCTNO NOTENO;
# DATA LOAN: MERGE LNNOTE LOAN BILLBAD(IN=A); BY ACCTNO NOTENO; IF A THEN DELETE;
# ============================================================================
print("Step 4: Filtering billed accounts...")

try:
    billbad = pl.read_parquet(BILL_FILE).select(['ACCTNO', 'NOTENO']).unique(
        subset=['ACCTNO', 'NOTENO'], keep='first'
    )
    loan_data = loan_data.join(
        billbad.with_columns(pl.lit(True).alias('_bill')),
        on=['ACCTNO', 'NOTENO'], how='left'
    ).filter(pl.col('_bill').is_null()).drop('_bill')
except Exception as e:
    print(f"Warning: Could not read BILL file: {e}")


# ============================================================================
# STEP 5: MERGE LNNOTE + LOAN
# PROC SORT DATA=LOAN; BY ACCTNO NOTENO;
# PROC SORT DATA=LNNOTE; BY ACCTNO NOTENO;
# DATA LOAN: MERGE LNNOTE LOAN BILLBAD(IN=A); BY ACCTNO NOTENO; IF A THEN DELETE;
# (BILLBAD already applied above; this is the LNNOTE + LOAN merge)
# ============================================================================
print("Step 5: Merging loan with note data...")

loan_data = loan_data.join(lnnote_data, on=['ACCTNO', 'NOTENO'], how='inner')


# ============================================================================
# STEP 6: PROCESS COMMISSION DATA
# DATA LNCOMM: SET PBBLN.LNCOMM PIBLN.LNCOMM; KEEP ACCTNO COMMNO CORGAMT;
# PROC SORT DATA=LNCOMM NODUPKEY; BY ACCTNO COMMNO;
# DATA LOAN: MERGE LOAN(IN=A) LNCOMM; BY ACCTNO COMMNO; IF A;
# Note: PBBLN.LNCOMM and PIBLN.LNCOMM are separate SAS library members.
#       The parquet equivalents are assumed to contain CORGAMT column.
# ============================================================================
print("Step 6: Processing commission data...")

pbb_lncomm = pl.read_parquet(PBBLN_FILE).select(['ACCTNO', 'COMMNO', 'CORGAMT'])
pib_lncomm = pl.read_parquet(PIBLN_FILE).select(['ACCTNO', 'COMMNO', 'CORGAMT'])
lncomm     = pl.concat([pbb_lncomm, pib_lncomm]).unique(
    subset=['ACCTNO', 'COMMNO'], keep='first'
)

loan_data = loan_data.join(lncomm, on=['ACCTNO', 'COMMNO'], how='left')


# ============================================================================
# STEP 7: APPLY FILTERING CRITERIA
# DATA LOAN: SET LOAN;
#   WHERE BALANCE > 30000 AND ISSDTE < &PRDATE AND FLAG1='F'
#         AND DAYDIFF <= 0
#         AND (PRODUCT IN &PBBPROD OR PRODUCT IN &PIBPROD)
#         AND (NOTE1 NE '1' AND NOTE2 NE '2');
#   LMTAPPR = MAX(CORGAMT, ORGBAL, NETPROC, APPRLIMT);
#   REPAID  = LMTAPPR - BALANCE;
#   BRCH    = PUT(BRANCH, BRCHCD.);    <- from PBBELF
#   *18-2927 AUTO REPRICE/RNR/WATCH LIST/SM/AKPK/LEGALCD;
#   *19-1923 SCORE1;
#   IF ... exclusion criteria ... THEN DELETE;
# ============================================================================
print("Step 7: Applying filtering criteria...")

loan_data = loan_data.with_columns([
    pl.max_horizontal(['CORGAMT', 'ORGBAL', 'NETPROC', 'APPRLIMT']).alias('LMTAPPR')
])
loan_data = loan_data.with_columns([
    (pl.col('LMTAPPR') - pl.col('BALANCE')).alias('REPAID')
])

# WHERE filter
loan_data = loan_data.filter(
    (pl.col('BALANCE') > 30000) &
    (pl.col('ISSDTE') < PRDATE) &
    (pl.col('FLAG1') == 'F') &
    (pl.col('DAYDIFF') <= 0) &
    (pl.col('PRODUCT').is_in(ALL_PROD)) &
    (pl.col('NOTE1') != '1') &
    (pl.col('NOTE2') != '2')
)

# Exclusion criteria (*18-2927 AUTO REPRICE/RNR/WATCH LIST/SM/AKPK/LEGALCD)
# (*19-1923 SCORE1)
loan_data = loan_data.filter(
    ~(
        ((pl.col('NTINDEX') == 1)  & (pl.col('SPREAD') == 0.00)) |
        ((pl.col('NTINDEX') == 1)  & (pl.col('SPREAD') == 3.50)) |
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

# BRCH = PUT(BRANCH, BRCHCD.)  — format provided by PBBELF
# SAS BRCHCD. is a numeric-to-character format: 2->'JSS', 3->'JRC', etc.
loan_data = loan_data.with_columns([
    pl.col('BRANCH').map_elements(
        lambda v: format_brchcd(int(v)) if v is not None else '',
        return_dtype=pl.Utf8
    ).alias('BRCH')
])

print(f"After filtering: {len(loan_data)} records")


# ============================================================================
# STEP 8: PROCESS COLLATERAL DATA
# DATA COLL(KEEP=ACCTNO NOTENO CPRPROPD MRESERVE CPRLANDU HOLDEXPD EXPDT):
#   SET COLL.COLLATER COLLI.COLLATER;
#   IF (CPRPROPD IN ('10','11','32','33','34','35') AND
#       CPRLANDU IN ('10','11','32','33','34','35')) OR
#       MRESERVE='Y' THEN EXCL='Y';
#   IF EXPDATE > 0 THEN
#     EXPDT = INPUT(SUBSTR(PUT(EXPDATE,$8.),1,8),DDMMYY8.);
#   IF EXCL='Y' OR HOLDEXPD='L' THEN OUTPUT;  *19-1923
# DATA LOAN: MERGE LOAN(IN=A) COLL(IN=B); BY ACCTNO NOTENO; IF A;
#   IF EXCL='Y' OR
#     (HOLDEXPD='L' AND YRDIF(EXPRDATE,EXPDT,'ACT/ACT') < 30) THEN DELETE;
# ============================================================================
print("Step 8: Processing collateral data...")

PROP_CODES = ['10', '11', '32', '33', '34', '35']


def parse_expdt(expdate_val) -> date | None:
    """
    SAS: INPUT(SUBSTR(PUT(EXPDATE,$8.),1,8), DDMMYY8.)
    EXPDATE is stored as a numeric value representing DDMMYYYY.
    """
    if expdate_val is None:
        return None
    try:
        ed = int(expdate_val)
        if ed <= 0:
            return None
        s = str(ed).zfill(8)  # PUT(EXPDATE,$8.) zero-padded to 8 chars
        return datetime.strptime(s, '%d%m%Y').date()
    except Exception:
        return None


def yrdif_act_act(d1_sas_int, d2: date) -> float:
    """
    SAS YRDIF(EXPRDATE, EXPDT, 'ACT/ACT'):
    EXPRDATE is a SAS integer; EXPDT is a Python date.
    Returns fractional years between the two dates.
    """
    if d2 is None:
        return 999.0
    try:
        d1 = date(1960, 1, 1) + timedelta(days=int(d1_sas_int))
        return abs((d1 - d2).days) / 365.25
    except Exception:
        return 999.0


coll_data  = pl.read_parquet(COLL_FILE).select(
    ['ACCTNO', 'NOTENO', 'CPRPROPD', 'MRESERVE', 'CPRLANDU', 'HOLDEXPD', 'EXPDATE']
)
colli_data = pl.read_parquet(COLLI_FILE).select(
    ['ACCTNO', 'NOTENO', 'CPRPROPD', 'MRESERVE', 'CPRLANDU', 'HOLDEXPD', 'EXPDATE']
)
coll_combined = pl.concat([coll_data, colli_data])

coll_combined = coll_combined.with_columns([
    pl.when(
        (pl.col('CPRPROPD').is_in(PROP_CODES) &
         pl.col('CPRLANDU').is_in(PROP_CODES)) |
        (pl.col('MRESERVE') == 'Y')
    ).then(pl.lit('Y')).otherwise(pl.lit(None)).alias('EXCL'),
    pl.col('EXPDATE').map_elements(parse_expdt, return_dtype=pl.Date).alias('EXPDT')
])

# IF EXCL='Y' OR HOLDEXPD='L' THEN OUTPUT
coll_combined = coll_combined.filter(
    (pl.col('EXCL') == 'Y') | (pl.col('HOLDEXPD') == 'L')
)

# Sort both by ACCTNO NOTENO before merge
loan_data     = loan_data.sort(['ACCTNO', 'NOTENO'])
coll_combined = coll_combined.sort(['ACCTNO', 'NOTENO'])

loan_data = loan_data.join(coll_combined, on=['ACCTNO', 'NOTENO'], how='left')

# IF EXCL='Y' OR
#   (HOLDEXPD='L' AND YRDIF(EXPRDATE,EXPDT,'ACT/ACT') < 30) THEN DELETE;
def should_delete_coll(row) -> bool:
    if row.get('EXCL') == 'Y':
        return True
    if row.get('HOLDEXPD') == 'L':
        expdt    = row.get('EXPDT')
        exprdate = row.get('EXPRDATE')
        if expdt is not None and exprdate is not None:
            if yrdif_act_act(exprdate, expdt) < 30:
                return True
    return False

keep_mask = [not should_delete_coll(r) for r in loan_data.iter_rows(named=True)]
loan_data = loan_data.filter(pl.Series(keep_mask))

print(f"After collateral filter: {len(loan_data)} records")


# ============================================================================
# STEP 9: FILTER CIS CUSTOMERS
# PROC SORT DATA=CIS.LOAN(KEEP=ACCTNO CUSTNO EMAILADD SECCUST NEWIC)
#           OUT=CIS; BY CUSTNO;
# DATA CISLN CISBILL(KEEP=CUSTNO): SET CIS;
#   IF (2500000000<=ACCTNO<=2599999999) OR
#      (2850000000<=ACCTNO<=2859999999) THEN OUTPUT CISBILL;
#   ELSE OUTPUT CISLN;
# PROC SORT DATA=CISBILL NODUPKEY; BY CUSTNO;
# DATA CISLN(KEEP=ACCTNO): MERGE CISLN(IN=A) CISBILL(IN=B); BY CUSTNO;
#   IF A AND B;
# PROC SORT DATA=CISLN NODUPKEY; BY ACCTNO;
# PROC SORT DATA=CIS; WHERE SECCUST='901'; BY ACCTNO;
# DATA LOAN: MERGE LOAN(IN=A) CISLN(IN=B) CIS; BY ACCTNO;
#   IF A; IF A AND B THEN DELETE;
# ============================================================================
print("Step 9: Filtering CIS customers...")

cis_data = pl.read_parquet(CIS_FILE).select(
    ['ACCTNO', 'CUSTNO', 'EMAILADD', 'SECCUST', 'NEWIC']
)

bill_range = (
    ((cis_data['ACCTNO'] >= 2500000000) & (cis_data['ACCTNO'] <= 2599999999)) |
    ((cis_data['ACCTNO'] >= 2850000000) & (cis_data['ACCTNO'] <= 2859999999))
)

cisbill = cis_data.filter(bill_range).select(['CUSTNO']).unique(
    subset=['CUSTNO'], keep='first'
)
cisln_all = cis_data.filter(~bill_range)

# CISLN = accounts that appear in both cisln_all and cisbill (same CUSTNO)
cisln = cisln_all.join(cisbill, on='CUSTNO', how='inner').select(
    ['ACCTNO']
).unique(subset=['ACCTNO'], keep='first')

# CIS for SECCUST='901' merge
cis_901 = cis_data.filter(pl.col('SECCUST') == '901').sort('ACCTNO')

loan_data = loan_data.sort('ACCTNO')

# DATA LOAN: MERGE LOAN(IN=A) CISLN(IN=B) CIS; BY ACCTNO; IF A; IF A AND B THEN DELETE
loan_data = loan_data.join(
    cisln.with_columns(pl.lit(True).alias('_cis_excl')),
    on='ACCTNO', how='left'
).filter(pl.col('_cis_excl').is_null()).drop('_cis_excl')

loan_data = loan_data.join(
    cis_901.select(['ACCTNO', 'NEWIC', 'EMAILADD']),
    on='ACCTNO', how='left'
)

print(f"After CIS filter: {len(loan_data)} records")


# ============================================================================
# STEP 10: FILTER ELDS REINSTATEMENT
# PROC SORT DATA=ELDS.ELBNMAX(KEEP=AANO REINPROD) OUT=ELDS; BY AANO;
# DATA LOAN: MERGE LOAN(IN=A) ELDS(IN=B); BY AANO; IF A;
#   IF REINPROD='Y' THEN DELETE;
# ============================================================================
print("Step 10: Filtering ELDS reinstatement...")

try:
    elds_data = pl.read_parquet(ELDS_FILE).select(['AANO', 'REINPROD']).unique(
        subset=['AANO'], keep='first'
    )
    loan_data = loan_data.sort('AANO').join(
        elds_data, on='AANO', how='left'
    ).filter(pl.col('REINPROD') != 'Y')
except Exception as e:
    print(f"Warning: Could not read ELDS file: {e}")

print(f"Final loan records: {len(loan_data)}")


# ============================================================================
# STEP 11: GENERATE SUMMARY REPORT
# PROC SUMMARY DATA=LOAN NWAY;
#   CLASS BRCH;
#   VAR LMTAPPR BALANCE REPAID;
#   OUTPUT OUT=BILLSUM (RENAME=(_FREQ_=NOACCT)) SUM=;
#
# PROC REPORT DATA=BILLSUM HEADLINE HEADSKIP NOWD SPLIT='*' MISSING;
#   COLUMN BRCH NOACCT LMTAPPR BALANCE REPAID;
#   DEFINE BRCH    / ORDER FORMAT=$6.   'BRANCH' ID;
#   DEFINE NOACCT  / FORMAT=COMMA9.     'TOTAL NO.*OF A/C';
#   DEFINE LMTAPPR / SUM FORMAT=COMMA15.2 'FACILITY/*APPROVED LIMIT';
#   DEFINE BALANCE / SUM FORMAT=COMMA15.2 'OUTSTANDING*BALANCE';
#   DEFINE REPAID  / SUM FORMAT=COMMA15.2 'AMOUNT*REPAID';
#   RBREAK AFTER / DUL DOL SUMMARIZE;
#   TITLE1 'PBB HL/FL/TL CUSTOMER WITH MINIMUM 2 1/2 YEARS';
#   TITLE2 'PROMPT REPAYMENT RECORD AS AT ' &RDATE;
#   TITLE3 'REPORT ID : EIQPROM2';
# Column widths derived from FORMAT specs:
#   BRCH    $6.     -> 6 chars
#   NOACCT  COMMA9. -> 9 chars
#   LMTAPPR COMMA15.2 -> 15 chars
#   BALANCE COMMA15.2 -> 15 chars
#   REPAID  COMMA15.2 -> 15 chars
# ============================================================================
print("Step 11: Generating summary report...")

billsum = (
    loan_data.group_by('BRCH')
    .agg([
        pl.len().alias('NOACCT'),
        pl.col('LMTAPPR').sum().alias('LMTAPPR'),
        pl.col('BALANCE').sum().alias('BALANCE'),
        pl.col('REPAID').sum().alias('REPAID'),
    ])
    .sort('BRCH')
)

# Column widths matching SAS FORMAT specs
# BRCH=$6. NOACCT=COMMA9. LMTAPPR=COMMA15.2 BALANCE=COMMA15.2 REPAID=COMMA15.2
W_BRCH   = 6
W_NOACCT = 9
W_AMT    = 15

def fmt_comma9(v) -> str:
    return f"{int(v or 0):>{W_NOACCT},}"

def fmt_comma15_2(v) -> str:
    return f"{float(v or 0.0):>{W_AMT},.2f}"

with open(SUMMARY_FILE, 'w') as f:
    # ASA carriage control: '1' = form-feed / new page
    f.write('1' + 'PBB HL/FL/TL CUSTOMER WITH MINIMUM 2 1/2 YEARS' + '\n')
    f.write(' ' + f'PROMPT REPAYMENT RECORD AS AT {RDATE}' + '\n')
    f.write(' ' + 'REPORT ID : EIQPROM2' + '\n')
    f.write(' ' + '\n')
    # Column headers — SPLIT='*' means * is a line-break within header label
    hdr1 = (f"{'BRANCH':<{W_BRCH}}  "
            f"{'TOTAL NO.':{W_NOACCT}}  "
            f"{'FACILITY/':{W_AMT}}  "
            f"{'OUTSTANDING':{W_AMT}}  "
            f"{'AMOUNT':{W_AMT}}")
    hdr2 = (f"{'':{W_BRCH}}  "
            f"{'OF A/C':{W_NOACCT}}  "
            f"{'APPROVED LIMIT':{W_AMT}}  "
            f"{'BALANCE':{W_AMT}}  "
            f"{'REPAID':{W_AMT}}")
    f.write(' ' + hdr1 + '\n')
    f.write(' ' + hdr2 + '\n')
    f.write(' ' + '-' * (W_BRCH + 2 + W_NOACCT + 2 + W_AMT + 2 + W_AMT + 2 + W_AMT) + '\n')

    tot_noacct = 0
    tot_lmtappr = 0.0
    tot_balance = 0.0
    tot_repaid  = 0.0

    for row in billsum.iter_rows(named=True):
        brch    = (row['BRCH'] or '').ljust(W_BRCH)[:W_BRCH]
        noacct  = row['NOACCT']  or 0
        lmtappr = row['LMTAPPR'] or 0.0
        balance = row['BALANCE'] or 0.0
        repaid  = row['REPAID']  or 0.0
        f.write(' ' + f"{brch}  {fmt_comma9(noacct)}  "
                      f"{fmt_comma15_2(lmtappr)}  "
                      f"{fmt_comma15_2(balance)}  "
                      f"{fmt_comma15_2(repaid)}\n")
        tot_noacct  += noacct
        tot_lmtappr += lmtappr
        tot_balance += balance
        tot_repaid  += repaid

    # RBREAK AFTER / DUL DOL SUMMARIZE  (double underline + summarize row)
    f.write(' ' + '=' * (W_BRCH + 2 + W_NOACCT + 2 + W_AMT + 2 + W_AMT + 2 + W_AMT) + '\n')
    f.write(' ' + '=' * (W_BRCH + 2 + W_NOACCT + 2 + W_AMT + 2 + W_AMT + 2 + W_AMT) + '\n')
    f.write(' ' + f"{'':6}  {fmt_comma9(tot_noacct)}  "
                  f"{fmt_comma15_2(tot_lmtappr)}  "
                  f"{fmt_comma15_2(tot_balance)}  "
                  f"{fmt_comma15_2(tot_repaid)}\n")

print(f"Summary report written to: {SUMMARY_FILE}")


# ============================================================================
# STEP 12: MARK NEW ACCOUNTS
# PROC SORT DATA=LOAN; BY ACCTNO NOTENO;
# DATA LOAN LNBILL.LOAN&REPTMON:
#   MERGE LOAN(IN=A) LNBILL.LOAN&PREVMON(IN=B KEEP=ACCTNO NOTENO);
#   BY ACCTNO NOTENO; IF A;
#   NEW = ''; IF A AND NOT B THEN NEW = 'Y';
# ============================================================================
print("Step 12: Marking new accounts...")

loan_data = loan_data.sort(['ACCTNO', 'NOTENO'])

try:
    prev_loan = pl.read_parquet(PREV_LOAN_FILE).select(['ACCTNO', 'NOTENO'])
    loan_data = loan_data.join(
        prev_loan.with_columns(pl.lit(True).alias('_prev')),
        on=['ACCTNO', 'NOTENO'], how='left'
    ).with_columns(
        pl.when(pl.col('_prev').is_null())
          .then(pl.lit('Y'))
          .otherwise(pl.lit(''))
          .alias('NEW')
    ).drop('_prev')
except Exception as e:
    print(f"Warning: Could not read previous month file: {e}")
    loan_data = loan_data.with_columns(pl.lit('Y').alias('NEW'))

# Save current month loan data (LNBILL.LOAN&REPTMON)
loan_data.write_parquet(LOAN_OUTPUT_FILE)
print(f"Loan data saved to: {LOAN_OUTPUT_FILE}")


# ============================================================================
# STEP 13: SPLIT INTO PBB AND PIB
# DATA LNPBB LNPIB: SET LOAN;
#   IF PRODUCT IN &PBBPROD THEN OUTPUT LNPBB;
#   ELSE                        OUTPUT LNPIB;
# PROC SORT DATA=LOAN; BY BRANCH ACCTNO;
# ============================================================================
print("Step 13: Splitting PBB and PIB data...")

loan_sorted = loan_data.sort(['BRANCH', 'ACCTNO'])
loan_pbb    = loan_sorted.filter(pl.col('PRODUCT').is_in(PBBPROD))
loan_pib    = loan_sorted.filter(pl.col('PRODUCT').is_in(PIBPROD))

print(f"PBB records: {len(loan_pbb)}, PIB records: {len(loan_pib)}")


# ============================================================================
# STEP 14: GENERATE DETAIL REPORTS  — %MACRO PRINTER(DSN, FLNAME)
#
# SAS PUT column positions (1-based, @col notation):
#   @4   BRANCH   3.        col  4- 6   (3 chars)
#   @9   ACCTNO   10.       col  9-18   (10 chars)
#   @20  NOTENO   5.        col 20-24   (5 chars)
#   @26  AANO               col 26-38  (13 chars, $13.)
#   @42  PRODUCT  3.        col 42-44  (3 chars)
#   @48  NAME               col 48-70  (23 chars implied from header)
#   @72  ISSDTE   DATE8.    col 72-79  (8 chars)
#   @80  LMTAPPR  12.2      col 80-91  (12 chars)
#   @93  BALANCE  12.2      col 93-104 (12 chars)
#   @106 REPAID   12.2      col 106-117 (12 chars)
#   @123 REMTERM  3.        col 123-125 (3 chars)
#   @130 NEW      $1.       col 130    (1 char)
#
# NEWPAGE link (1-based @col positions):
#   @1   'BRANCH    : ' BRANCH 3.         -> col 1
#   @50  bank name centred                -> col 50
#   @120 'PAGE NO : ' PAGECNT             -> col 120
#   @1   'REPORT NO : EIQPROM2'           -> col 1
#   @47  'HL/FL/TL CUSTOMER...'           -> col 47
#   @49  'PROMPT REPAYMENT...'            -> col 49
#   col headers at @21 @40 @73 @84 @100 @111 @119
#   @1   132*'-'
#
# FILE DCB: RECFM=FBA LRECL=136 BLKSIZE=13600
# FBA = Fixed Blocked with ASA carriage control
# First character of each record is ASA control character
# ============================================================================
print("Step 14: Generating detail reports...")

LRECL = 135   # printable width (LRECL=136 includes 1-char ASA prefix)


def write_detail_report(data: pl.DataFrame, filename: Path, bank_name: str):
    """
    Replicates %MACRO PRINTER(DSN,FLNAME) exactly.
    All column positions are 1-based as per SAS @col notation.
    Record format: FBA LRECL=136 — first char is ASA carriage control.
    '1' in column 1 = ASA form-feed (new page).
    ' ' in column 1 = single space advance (normal line).
    """
    def put_at(buf: list, col1: int, text: str, width: int = None):
        """
        Place text at 1-based column position col1 in the buffer.
        buf is a list of single characters, length = LRECL.
        """
        idx = col1 - 1   # convert to 0-based
        s   = text if width is None else text[:width].ljust(width)
        for i, ch in enumerate(s):
            if idx + i < len(buf):
                buf[idx + i] = ch

    def flush_line(buf: list, asa: str = ' ') -> str:
        return asa + ''.join(buf) + '\n'

    def blank_buf() -> list:
        return [' '] * LRECL

    with open(filename, 'w') as f:
        current_branch = None
        linecnt  = 0
        pagecnt  = 0
        accnt    = 0
        aclimt   = 0.0
        acbal    = 0.0
        acpaid   = 0.0

        def write_newpage(branch_val):
            nonlocal pagecnt, linecnt
            pagecnt += 1
            # Line 1: BRANCH / bank name / PAGE NO
            buf = blank_buf()
            put_at(buf,   1, f'BRANCH    : {int(branch_val or 0):3d}')
            put_at(buf,  50, bank_name)
            put_at(buf, 120, f'PAGE NO : {pagecnt}')
            f.write(flush_line(buf, '1'))   # ASA '1' = form-feed

            # Line 2: REPORT NO / sub-title
            buf = blank_buf()
            put_at(buf,  1, 'REPORT NO : EIQPROM2')
            put_at(buf, 47, 'HL/FL/TL CUSTOMER WITH MINIMUM 2 1/2 YEARS')
            f.write(flush_line(buf))

            # Line 3: prompt repayment date
            buf = blank_buf()
            put_at(buf, 49, f'PROMPT REPAYMENT RECORD AS AT {RDATE}')
            f.write(flush_line(buf))

            # Line 4: blank
            f.write(flush_line(blank_buf()))

            # Line 5: column header row 1
            buf = blank_buf()
            put_at(buf,  21, 'NOTE')
            put_at(buf,  40, 'PRODUCT')
            put_at(buf,  73, 'RELEASED')
            put_at(buf,  84, 'APPROVED')
            put_at(buf, 100, 'O/S')
            put_at(buf, 111, 'AMT')
            put_at(buf, 119, 'REMAINING')
            f.write(flush_line(buf))

            # Line 6: column header row 2
            buf = blank_buf()
            put_at(buf,   1, 'BRANCH')
            put_at(buf,  11, 'A/C NO')
            put_at(buf,  22, 'NO')
            put_at(buf,  29, 'A/A NO')
            put_at(buf,  42, 'CODE')
            put_at(buf,  48, 'NAME OF CUSTOMER')
            put_at(buf,  75, 'DATE')
            put_at(buf,  86, 'LIMIT')
            put_at(buf,  98, 'BALANCE')
            put_at(buf, 109, 'REPAID')
            put_at(buf, 121, 'TENURE')
            put_at(buf, 129, 'NEW')
            f.write(flush_line(buf))

            # Line 7: separator (132 dashes)
            buf = blank_buf()
            put_at(buf, 1, '-' * 132)
            f.write(flush_line(buf))

            # Line 8: blank
            f.write(flush_line(blank_buf()))

            linecnt = 10  # header occupies ~10 lines

        for row in data.iter_rows(named=True):
            branch = row['BRANCH']

            # FIRST.BRANCH
            if current_branch != branch:
                # Print branch totals for previous branch (LAST.BRANCH)
                if current_branch is not None:
                    buf = blank_buf()
                    put_at(buf, 1, '-' * 132)
                    f.write(flush_line(buf))

                    buf = blank_buf()
                    put_at(buf,  1, 'NO OF A/C :')
                    put_at(buf, 17, f'{accnt:>8,}')
                    put_at(buf, 80, f'{aclimt:>12.2f}')
                    put_at(buf, 93, f'{acbal:>12.2f}')
                    put_at(buf, 106, f'{acpaid:>12.2f}')
                    f.write(flush_line(buf))

                    buf = blank_buf()
                    put_at(buf, 1, '=' * 132)
                    f.write(flush_line(buf))
                    linecnt += 4

                current_branch = branch
                pagecnt = 0
                accnt   = 0
                aclimt  = 0.0
                acbal   = 0.0
                acpaid  = 0.0
                linecnt = 999   # force new page at start of branch

            if linecnt > 55:
                write_newpage(branch)

            # Detail line — PUT @col format positions
            buf = blank_buf()
            put_at(buf,   4, f'{int(branch or 0):3d}',      3)
            put_at(buf,   9, f'{int(row["ACCTNO"] or 0):10d}', 10)
            put_at(buf,  20, f'{int(row["NOTENO"] or 0):5d}',  5)
            put_at(buf,  26, str(row.get('AANO') or ''),     13)
            put_at(buf,  42, f'{int(row["PRODUCT"] or 0):3d}',  3)
            put_at(buf,  48, str(row.get('NAME') or '')[:23], 23)
            put_at(buf,  72, format_date(row.get('ISSDTE')),   8)
            put_at(buf,  80, f'{float(row.get("LMTAPPR") or 0.0):12.2f}', 12)
            put_at(buf,  93, f'{float(row.get("BALANCE") or 0.0):12.2f}', 12)
            put_at(buf, 106, f'{float(row.get("REPAID")  or 0.0):12.2f}', 12)
            put_at(buf, 123, f'{int(row.get("REMTERM") or 0):3d}',          3)
            put_at(buf, 130, str(row.get('NEW') or ''),                      1)
            f.write(flush_line(buf))

            linecnt += 1
            accnt   += 1
            aclimt  += float(row.get('LMTAPPR') or 0.0)
            acbal   += float(row.get('BALANCE')  or 0.0)
            acpaid  += float(row.get('REPAID')   or 0.0)

        # Print final branch totals
        if current_branch is not None:
            buf = blank_buf()
            put_at(buf, 1, '-' * 132)
            f.write(flush_line(buf))

            buf = blank_buf()
            put_at(buf,  1, 'NO OF A/C :')
            put_at(buf, 17, f'{accnt:>8,}')
            put_at(buf, 80, f'{aclimt:>12.2f}')
            put_at(buf, 93, f'{acbal:>12.2f}')
            put_at(buf, 106, f'{acpaid:>12.2f}')
            f.write(flush_line(buf))

            buf = blank_buf()
            put_at(buf, 1, '=' * 132)
            f.write(flush_line(buf))


write_detail_report(loan_pbb, PBB_REPORT_FILE, 'P U B L I C   B A N K   B E R H A D')
print(f"PBB report written to: {PBB_REPORT_FILE}")

write_detail_report(loan_pib, PIB_REPORT_FILE, 'P U B L I C   I S L A M I C   B A N K   B E R H A D')
print(f"PIB report written to: {PIB_REPORT_FILE}")

# Close DuckDB connection
con.close()

print("\nProcessing complete!")
