#!/usr/bin/env python3
"""
Program : EIIMRM01.py (ISLAMIC)
Date    : 01.04.08
Report  : DEPOSITS, BY TIME TO MATURITY FOR ALCO
          (WEIGHTED AVERAGE COST BY MATURITY PROFILE)
"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
# PBBDPFMT - provides fdprod_format (PUT(x, FDPROD.)),
#                      saprod_format (PUT(x, SAPROD.)),
#                      caprod_format (PUT(x, CAPROD.))
from PBBDPFMT import fdprod_format, saprod_format, caprod_format

import duckdb
import polars as pl
from datetime import date, datetime
import math
import os

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
PARQUET_DIR  = os.path.join(BASE_DIR, "data")
OUTPUT_DIR   = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

REPTDATE_PARQUET = os.path.join(PARQUET_DIR, "REPTDATE.parquet")
FD_PARQUET       = os.path.join(PARQUET_DIR, "FD.parquet")
SAVING_PARQUET   = os.path.join(PARQUET_DIR, "SAVING.parquet")
CURRENT_PARQUET  = os.path.join(PARQUET_DIR, "CURRENT.parquet")

OUTPUT_FILE = os.path.join(OUTPUT_DIR, "EIIMRM01.txt")

# ============================================================================
# FORMAT DEFINITIONS
# ============================================================================

def remfmt(remmth):
    """
    REMFMT format: maps remaining months value to display label.
    Special values 91-97, 99 are sentinel codes set during processing.
    """
    if remmth is None:
        return '  '
    if remmth <= 0:
        return '       '
    if remmth <= 1:
        return '>  0-1 MTH'
    if remmth <= 2:
        return '>  1-2 MTHS'
    if remmth <= 3:
        return '>  2-3 MTHS'
    if remmth <= 4:
        return '>  3-4 MTHS'
    if remmth <= 5:
        return '>  4-5 MTHS'
    if remmth <= 6:
        return '>  5-6 MTHS'
    if remmth <= 7:
        return '>  6-7 MTHS'
    if remmth <= 8:
        return '>  7-8 MTHS'
    if remmth <= 9:
        return '>  8-9 MTHS'
    if remmth <= 10:
        return '>  9-10 MTHS'
    if remmth <= 11:
        return '> 10-11 MTHS'
    if remmth <= 12:
        return '> 11-12 MTHS'
    if remmth <= 13:
        return '> 12-13 MTHS'
    if remmth <= 14:
        return '> 13-14 MTHS'
    if remmth <= 15:
        return '> 14-15 MTHS'
    if remmth <= 16:
        return '> 15-16 MTHS'
    if remmth <= 17:
        return '> 16-17 MTHS'
    if remmth <= 18:
        return '> 17-18 MTHS'
    if remmth <= 19:
        return '> 18-19 MTHS'
    if remmth <= 20:
        return '> 19-20 MTHS'
    if remmth <= 21:
        return '> 20-21 MTHS'
    if remmth <= 22:
        return '> 21-22 MTHS'
    if remmth <= 23:
        return '> 22-23 MTHS'
    if remmth <= 24:
        return '> 23-24 MTHS'
    if remmth <= 36:
        return '>2-3 YRS'
    if remmth <= 48:
        return '>3-4 YRS'
    if remmth <= 60:
        return '>4-5 YRS'
    if remmth == 91:
        return ' 1 MONTH'
    if remmth == 92:
        return ' 3 MONTHS'
    if remmth == 93:
        return ' 6 MONTHS'
    if remmth == 94:
        return ' 9 MONTHS'
    if remmth == 95:
        return '12 MONTHS'
    if remmth == 96:
        return '15 MONTHS'
    if remmth == 97:
        return 'ABOVE 15 MONTHS'
    if remmth == 99:
        return 'OVERDUE FD'
    return '  '


def subttl_format(code):
    """
    $SUBTTL format: maps subtotal code to display label.
    """
    _map = {
        'A':  'REMAINING MATURITY',
        'B':  'OVERDUE FD',
        'C':  'NEW FD FOR THE MONTH',
        'D1': 'SAVING ACCOUNTS  ',
        'D2': 'WADIAH SAVING A/C',
        'E1': 'NORMAL CURRENT A/C',
        'E2': 'WADIAH CURRENT A/C',
        'E3': 'FCY CURRENT A/C',
        'E4': 'OD A/C',
        'F1': 'INT-BEAR. GOV.  ACCT',
        'F2': 'INT-BEAR. HSING ACCT',
        'F3': 'ACE < 5K            ',
        'F4': 'ACE > 5K            ',
        'F5': 'VOSTRO LOCAL        ',
        'F6': 'VOSTRO FOREIGN      ',
        'F7': 'PB SHARE LINK       ',
        'H':  'PORTION FROM ACE ACC',
        'I':  'SUB-TOTAL',
    }
    return _map.get(code, code if code else '')


# TERMFMT: maps FCY interest plan codes to term in months
_TERMFMT_MAP = {}
for _c in [470, 471, 476, 477, 482, 483, 488, 489, 494, 495, 548, 549, 554, 555]:
    _TERMFMT_MAP[_c] = 1
for _c in [472, 473, 478, 479, 484, 485, 490, 491, 496, 497, 550, 551, 556, 557]:
    _TERMFMT_MAP[_c] = 3
for _c in [474, 475, 480, 481, 486, 487, 492, 493, 498, 499, 552, 553, 558, 559]:
    _TERMFMT_MAP[_c] = 6


def termfmt(intplan):
    """TERMFMT: maps FCY intplan code to term in months (1, 3, or 6)."""
    return _TERMFMT_MAP.get(intplan, None)


# ============================================================================
# HELPER: DAYS IN MONTH (SAS simple leap-year rule: year % 4 == 0)
# ============================================================================

def days_in_month(year, month):
    """Returns days in the given month using SAS simple leap-year rule."""
    if month in (1, 3, 5, 7, 8, 10, 12):
        return 31
    elif month in (4, 6, 9, 11):
        return 30
    elif month == 2:
        return 29 if (year % 4 == 0) else 28
    return 30


# ============================================================================
# HELPER: PARSE YYMMDD8 WITH YEARCUTOFF=1950
# ============================================================================

def parse_yymmdd8(val):
    """
    Parse an integer in YYMMDD format using YEARCUTOFF=1950.
    Year 50-99 => 1950-1999; year 00-49 => 2000-2049.
    Returns a date object or None.
    """
    if val is None or val == 0:
        return None
    s = f"{int(val):08d}"
    yy = int(s[0:2])
    mm = int(s[2:4])
    dd = int(s[4:6])
    if yy >= 50:
        yyyy = 1900 + yy
    else:
        yyyy = 2000 + yy
    try:
        return date(yyyy, mm, dd)
    except ValueError:
        return None


# ============================================================================
# HELPER: CALCULATE REMAINING MONTHS (REMMTH macro)
# ============================================================================

def calc_remmth(matdt, reptdate):
    """
    Replicates the SAS %REMMTH macro.
    Returns fractional remaining months from reptdate to matdt.

    Logic:
      MDDAY is capped at RPDAYS(RPMTH) - days in reptdate's month.
      REMMTH = (MDYR - RPYR)*12 + (MDMTH - RPMTH) + (MDDAY - RPDAY) / RPDAYS(RPMTH)
    """
    if matdt is None or reptdate is None:
        return 0.0

    rpyr  = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day

    # RPDAYS array: days in each month of the report year (using SAS rule)
    rpdays = [days_in_month(rpyr, m) for m in range(1, 13)]  # index 0=Jan

    mdyr  = matdt.year
    mdmth = matdt.month
    mdday = matdt.day

    # Cap MDDAY at days in report month
    rp_mth_days = rpdays[rpmth - 1]
    if mdday > rp_mth_days:
        mdday = rp_mth_days

    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday

    return remy * 12 + remm + remd / rp_mth_days


# ============================================================================
# STEP 1: READ REPTDATE
# ============================================================================

def get_reptdate():
    """Load REPTDATE from parquet and compute macro variables."""
    con = duckdb.connect()
    row = con.execute(f"SELECT reptdate FROM '{REPTDATE_PARQUET}' LIMIT 1").fetchone()
    con.close()

    # reptdate stored as integer days since 1960-01-01
    epoch = date(1960, 1, 1)
    reptdate = epoch + __import__('datetime').timedelta(days=int(row[0]))

    day = reptdate.day
    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'

    reptyrs   = str(reptdate.year % 100).zfill(2)
    reptyear  = str(reptdate.year)
    reptmon   = str(reptdate.month).zfill(2)
    reptday   = str(reptdate.day).zfill(2)
    rdate     = f"{reptdate.day:02d}/{reptdate.month:02d}/{reptdate.year}"

    return reptdate, nowk, reptyrs, reptyear, reptmon, reptday, rdate


# ============================================================================
# STEP 2: PROCESS FIXED DEPOSITS
# ============================================================================

def process_fd(reptdate):
    """
    Replicates the DATA FD / TD / FDN step.
    Returns three lists of dicts: fd_rows, td_rows, fdn_rows.
    """
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM '{FD_PARQUET}'").df()
    con.close()
    df = pl.from_pandas(df)

    rpyr  = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day
    rpdays = [days_in_month(rpyr, m) for m in range(1, 13)]

    fd_rows  = []
    td_rows  = []
    fdn_rows = []

    _keep = ['prodtyp', 'subtyp', 'subttl', 'remmth', 'term', 'amount',
             'cost', 'matdt', 'intdate', 'intplan', 'openind', 'reptdate',
             'rate', 'acctno', 'cdno', 'remm']

    for row in df.iter_rows(named=True):
        intplan  = row.get('intplan')
        curbal   = row.get('curbal', 0) or 0
        openind  = (row.get('openind') or '').strip()
        matdate  = row.get('matdate')
        rate     = row.get('rate', 0) or 0
        acctno   = row.get('acctno')
        cdno     = row.get('cdno')
        intdate  = row.get('intdate')

        bnmcode = fdprod_format(intplan)

        if bnmcode == '42630':
            prodtyp = 'FIXED DEPT(FCY)'
            term = termfmt(intplan)
            if term is None:
                term = 0
        else:
            prodtyp = 'FIXED DEPT(RM)'
            term = 0

        if openind not in ('O', 'D') or curbal <= 0:
            continue

        matdt = parse_yymmdd8(matdate)

        rec_base = {
            'prodtyp':  prodtyp,
            'intdate':  intdate,
            'intplan':  intplan,
            'openind':  openind,
            'reptdate': reptdate,
            'rate':     rate,
            'acctno':   acctno,
            'cdno':     cdno,
            'term':     term,
            'matdt':    matdt,
            'amount':   curbal,
        }

        if openind == 'D' or (matdt is not None and matdt < reptdate):
            # Overdue / deleted → TD dataset
            subttl = 'B'
            subtyp = 'SPTF' if bnmcode == '42132' else 'CONVENTIONAL'
            cost   = curbal * rate
            remmth = 99
            remm   = 0.0
            rec = {**rec_base,
                   'subttl': subttl, 'subtyp': subtyp,
                   'cost': cost, 'remmth': remmth, 'remm': remm}
            td_rows.append(rec)
        else:
            remmth = calc_remmth(matdt, reptdate)
            subttl = 'A'
            subtyp = 'SPTF' if bnmcode == '42132' else 'CONVENTIONAL'
            cost   = curbal * rate
            remm   = curbal * remmth
            rec = {**rec_base,
                   'subttl': subttl, 'subtyp': subtyp,
                   'cost': cost, 'remmth': remmth, 'remm': remm}
            fd_rows.append(rec)

            # New FD for the month (term - remmth < 1)
            if (term - remmth) < 1:
                subttl2 = 'C'
                subtyp2 = 'SPTF' if bnmcode == '42132' else 'CONVENTIONAL'
                cost2   = curbal * rate
                # REM = REMMTH * CURBAL  (original REM variable, not REMM)
                # REMMTH = TERM for FDN output
                rec2 = {**rec_base,
                        'subttl': subttl2, 'subtyp': subtyp2,
                        'cost': cost2, 'remmth': term,
                        'remm': remmth * curbal}
                fdn_rows.append(rec2)

    return fd_rows, td_rows, fdn_rows


# ============================================================================
# STEP 3: PROCESS SAVINGS
# ============================================================================

def process_savings():
    """Replicates DATA SA step."""
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM '{SAVING_PARQUET}'").df()
    con.close()
    df = pl.from_pandas(df)

    sa_rows = []
    for row in df.iter_rows(named=True):
        openind = (row.get('openind') or '').strip()
        curbal  = row.get('curbal', 0) or 0
        product = row.get('product')
        intrate = row.get('intrate', 0) or 0

        if openind in ('B', 'C', 'P'):
            continue
        if curbal < 0:
            continue

        if product in (204, 214, 215):
            subtyp = 'SPTF'
            subttl = 'D2'
        else:
            subtyp = 'CONVENTIONAL'
            subttl = 'D1'

        cost   = curbal * intrate
        remmth = 0
        remm   = remmth * curbal

        sa_rows.append({
            'prodtyp': 'SAVINGS DEPOSIT',
            'subtyp':  subtyp,
            'subttl':  subttl,
            'amount':  curbal,
            'cost':    cost,
            'remmth':  remmth,
            'remm':    remm,
        })

    return sa_rows


# ============================================================================
# STEP 4: PROCESS CURRENT ACCOUNTS
# ============================================================================

def process_current():
    """
    Replicates DATA CA / CAG / CAS step.
    Filter: BNMCODE='42310' OR BNMCODE='42180' OR PRODUCT=166
    """
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM '{CURRENT_PARQUET}'").df()
    con.close()
    df = pl.from_pandas(df)

    ca_rows  = []
    cag_rows = []
    cas_rows = []

    for row in df.iter_rows(named=True):
        product = row.get('product')
        openind = (row.get('openind') or '').strip()
        curbal  = row.get('curbal', 0) or 0
        intrate = row.get('intrate', 0) or 0
        remmth  = 0

        bnmcode = caprod_format(product)

        # Filter: BNMCODE='42310' OR BNMCODE='42180' OR PRODUCT=166
        if bnmcode not in ('42310', '42180') and product != 166:
            continue

        if openind in ('B', 'C', 'P'):
            continue
        if bnmcode == 'N':
            continue

        def _ca_rec(prodtyp, subtyp, subttl, bal, cost_, remm_):
            return {
                'prodtyp': prodtyp,
                'subtyp':  subtyp,
                'subttl':  subttl,
                'amount':  bal,
                'cost':    cost_,
                'remmth':  remmth,
                'remm':    remm_,
            }

        if curbal > 0:
            if product in (101, 103, 161, 163):
                subtyp2 = 'SPTF'
                if product in (101, 103):
                    subtyp2 = 'CONVENTIONAL'
                prodtyp2 = 'DEMAND DEPOSIT'
                subttl2 = 'F1' if product in (101, 161) else 'F2'
                cost2  = curbal * intrate
                remm2  = curbal * remmth
                cag_rows.append(_ca_rec(prodtyp2, subtyp2, subttl2, curbal, cost2, remm2))

            elif product in (150, 151, 152, 181):
                subtyp2 = 'CONVENTIONAL'
                if curbal <= 5000:
                    ca_rows.append(_ca_rec('DEMAND DEPOSIT', subtyp2, 'F3',
                                           curbal, 0.0, 0.0))
                else:
                    bal_over = curbal - 5000
                    cost_over = bal_over * intrate
                    remm_over = bal_over * remmth
                    cas_rows.append(_ca_rec('SAVINGS DEPOSIT', subtyp2, 'H',
                                            bal_over, cost_over, remm_over))
                    cost_5k = 0.0
                    remm_5k = 5000 * remmth
                    ca_rows.append(_ca_rec('DEMAND DEPOSIT', subtyp2, 'F4',
                                           5000, cost_5k, remm_5k))

            elif product in (60, 61, 62, 63, 64, 160, 162, 164, 165, 166, 182):
                cost2 = curbal * intrate
                remm2 = curbal * remmth
                ca_rows.append(_ca_rec('DEMAND DEPOSIT', 'SPTF', 'E2',
                                       curbal, cost2, remm2))

            elif 400 <= product <= 410:
                cost2 = curbal * intrate
                remm2 = curbal * remmth
                ca_rows.append(_ca_rec('DEMAND DEPOSIT', 'CONVENTIONAL', 'E3',
                                       curbal, cost2, remm2))

            elif product in (104, 105, 177, 189, 190, 178):
                subttl2 = 'F7'
                if product == 104:
                    subttl2 = 'F5'
                elif product == 105:
                    subttl2 = 'F6'
                cost2 = curbal * intrate
                remm2 = curbal * remmth
                ca_rows.append(_ca_rec('DEMAND DEPOSIT', 'CONVENTIONAL', subttl2,
                                       curbal, cost2, remm2))

            elif product not in (101, 104, 105, 107, 113, 150, 151, 152,
                                  178, 189, 190):
                cost2 = curbal * intrate
                remm2 = curbal * remmth
                ca_rows.append(_ca_rec('DEMAND DEPOSIT', 'CONVENTIONAL', 'E1',
                                       curbal, cost2, remm2))

        if curbal <= 0:
            cost2 = curbal * intrate
            remm2 = curbal * remmth
            ca_rows.append(_ca_rec('DEMAND DEPOSIT', 'SPTF', 'E4',
                                   curbal, cost2, remm2))

    return ca_rows, cag_rows, cas_rows


# ============================================================================
# STEP 5: SUMMARISE USING PROC SUMMARY equivalents
# ============================================================================

def summarise(rows, group_cols, sum_cols):
    """Group-by sum over rows (list of dicts)."""
    if not rows:
        return []
    df = pl.DataFrame(rows)
    # Ensure all sum_cols exist
    for c in sum_cols:
        if c not in df.columns:
            df = df.with_columns(pl.lit(0.0).alias(c))
    agg = df.group_by(group_cols).agg(
        [pl.col(c).sum().alias(c) for c in sum_cols]
    )
    return agg.to_dicts()


def apply_remfmt_label(rows):
    """Add remmth1 column (REMFMT label) to each row."""
    out = []
    for r in rows:
        r2 = dict(r)
        r2['remmth1'] = remfmt(r.get('remmth'))
        out.append(r2)
    return out


# ============================================================================
# STEP 6: BUILD DUMMY ROWS (expanding maturity labels 1..60)
# ============================================================================

def build_dummy(fd_summarised):
    """
    Replicates DATA DUMMY step.
    For each unique (PRODTYP, SUBTTL, SUBTYP) where SUBTTL IN ('A','C','E3'),
    emit rows for remmth=1..60 with their remmth1 label.
    """
    seen = {}
    for r in fd_summarised:
        subttl = r.get('subttl', '')
        if subttl not in ('A', 'C', 'E3'):
            continue
        key = (r.get('prodtyp'), r.get('subttl'), r.get('subtyp'))
        seen[key] = True

    dummy = []
    for (prodtyp, subttl, subtyp) in seen:
        for rm in range(1, 61):
            dummy.append({
                'prodtyp': prodtyp,
                'subttl':  subttl,
                'subtyp':  subtyp,
                'remmth':  rm,
                'remmth1': remfmt(rm),
                'amount':  None,
                'cost':    None,
                'remm':    None,
            })

    # PROC SORT NODUPKEYS by prodtyp, subttl, subtyp, remmth1
    seen_keys = set()
    deduped = []
    for r in dummy:
        k = (r['prodtyp'], r['subttl'], r['subtyp'], r['remmth1'])
        if k not in seen_keys:
            seen_keys.add(k)
            deduped.append(r)
    return deduped


# ============================================================================
# STEP 7: MERGE FD with DUMMY
# ============================================================================

def merge_fd_dummy(fd_sum, dummy):
    """
    MERGE FD DUMMY BY PRODTYP SUBTTL SUBTYP REMMTH1.
    Dummy rows fill in any missing remmth1 labels (amount/cost/remm=None→0).
    """
    # Build lookup from fd_sum keyed by (prodtyp, subttl, subtyp, remmth1)
    fd_lookup = {}
    for r in fd_sum:
        k = (r.get('prodtyp'), r.get('subttl'), r.get('subtyp'), r.get('remmth1'))
        fd_lookup[k] = r

    # Collect all keys from both
    all_keys = set(fd_lookup.keys())
    for r in dummy:
        k = (r.get('prodtyp'), r.get('subttl'), r.get('subtyp'), r.get('remmth1'))
        all_keys.add(k)

    merged = []
    for k in all_keys:
        if k in fd_lookup:
            merged.append(dict(fd_lookup[k]))
        else:
            prodtyp, subttl, subtyp, remmth1 = k
            merged.append({
                'prodtyp': prodtyp, 'subttl': subttl,
                'subtyp':  subtyp,  'remmth1': remmth1,
                'remmth':  0,
                'amount':  0.0, 'cost': 0.0, 'remm': 0.0,
            })
    return merged


# ============================================================================
# REPORT HELPERS
# ============================================================================

PAGE_WIDTH = 132
LINES_PER_PAGE = 60

_SUBTYP_ORDER = ['CONVENTIONAL', 'SPTF', 'TOTAL']


def fmt_comma12(val):
    if val is None:
        return ' ' * 12
    return f"{round(val):>12,.0f}"


def fmt_comma12_2(val):
    if val is None:
        return ' ' * 12
    return f"{val:>12,.2f}"


def fmt_comma5_2(val):
    if val is None:
        return ' ' * 5
    return f"{val:>5,.2f}"


def safe_div(num, denom):
    if denom and denom != 0:
        return num / denom
    return 0.0


# ============================================================================
# STEP 8: BUILD DEP DATASET AND COMPUTE WACOST / WAREMM
# ============================================================================

def build_dep(fd_merged, sa_summarised, ca_summarised,
              cag_summarised, cas_summarised):
    """
    DATA DEP = SET FD SA CA CAG CAS, then compute WACOST, WAREMM, AMOUNT/1000,
    reclassify PRODTYP for CA lines.
    """
    dep = []
    for r in (fd_merged + sa_summarised + ca_summarised +
              cag_summarised + cas_summarised):
        r2 = dict(r)
        amount = r2.get('amount') or 0.0
        cost   = r2.get('cost')   or 0.0
        remm   = r2.get('remm')   or 0.0
        subtyp = r2.get('subtyp', '')
        subttl = r2.get('subttl', '')

        if subtyp in ('SPTF', 'CONVENTIONAL'):
            wacost = safe_div(cost, amount) if amount > 0 else 0.0
        else:
            wacost = 0.0
        waremm = safe_div(remm, amount) if amount != 0 else 0.0

        r2['wacost']  = wacost
        r2['waremm']  = waremm
        r2['amount']  = round(amount / 1000)

        if subttl in ('E1', 'E2', 'E3'):
            r2['prodtyp'] = 'CA NON-INT BEARING'
        elif subttl in ('F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7'):
            r2['prodtyp'] = 'CA INT BEARING'

        dep.append(r2)
    return dep


# ============================================================================
# STEP 9: BUILD DEPTOTAL (SA & CA totals per PRODTYP/SUBTTL/REMMTH)
# ============================================================================

def build_deptotal(dep):
    """
    PROC SUMMARY DATA=DEP NWAY CLASS PRODTYP SUBTTL REMMTH → DEPTOTAL
    Then compute WACOST/WAREMM from raw cost/remm over amount*1000.
    """
    groups = {}
    for r in dep:
        k = (r.get('prodtyp'), r.get('subttl'), r.get('remmth'))
        if k not in groups:
            groups[k] = {'amount': 0.0, 'cost': 0.0, 'remm': 0.0}
        groups[k]['amount'] += r.get('amount') or 0.0
        groups[k]['cost']   += r.get('cost')   or 0.0
        groups[k]['remm']   += r.get('remm')   or 0.0

    rows = []
    for (prodtyp, subttl, remmth), vals in groups.items():
        amt_raw = round(vals['amount'] * 1000)
        wacost  = safe_div(vals['cost'], amt_raw)
        waremm  = safe_div(vals['remm'], amt_raw)
        rows.append({
            'prodtyp': prodtyp, 'subttl': subttl,
            'remmth':  remmth,
            'amount':  vals['amount'],
            'cost':    vals['cost'],
            'remm':    vals['remm'],
            'wacost':  wacost,
            'waremm':  waremm,
            'subtyp':  'TOTAL',
            'remmth1': remfmt(remmth) if remmth is not None else '',
        })
    return rows


# ============================================================================
# STEP 10: BUILD DEPTOTA2 (grand subtotal per PRODTYP/SUBTYP)
# ============================================================================

def build_deptota2(depfinal):
    """
    PROC SUMMARY DATA=DEPFINAL NWAY CLASS PRODTYP SUBTYP → DEPTOTA2
    SUBTTL='I'
    """
    groups = {}
    for r in depfinal:
        k = (r.get('prodtyp'), r.get('subtyp'))
        if k not in groups:
            groups[k] = {'amount': 0.0, 'cost': 0.0, 'remm': 0.0}
        groups[k]['amount'] += r.get('amount') or 0.0
        groups[k]['cost']   += r.get('cost')   or 0.0
        groups[k]['remm']   += r.get('remm')   or 0.0

    rows = []
    for (prodtyp, subtyp), vals in groups.items():
        amt_raw = round(vals['amount'] * 1000)
        wacost  = safe_div(vals['cost'], amt_raw)
        waremm  = safe_div(vals['remm'], amt_raw)
        rows.append({
            'prodtyp': prodtyp, 'subtyp': subtyp,
            'subttl':  'I',
            'amount':  vals['amount'],
            'cost':    vals['cost'],
            'remm':    vals['remm'],
            'wacost':  wacost,
            'waremm':  waremm,
            'remmth':  None,
            'remmth1': '',
        })
    return rows


# ============================================================================
# STEP 11: FD TOTALS
# ============================================================================

def build_fdtotal(dep, deptotal_sum):
    """
    FDTOTAL = DEP + DEPTOTAL rows where PRODTYP IN ('FIXED DEPT(RM)','FIXED DEPT(FCY)')
    """
    fd_prodtyps = {'FIXED DEPT(RM)', 'FIXED DEPT(FCY)'}
    fd_dep = [r for r in dep if r.get('prodtyp') in fd_prodtyps]
    fd_tot = [r for r in deptotal_sum if r.get('prodtyp') in fd_prodtyps]
    return fd_dep + fd_tot


def build_fdtota2(fdtotal):
    """
    PROC SUMMARY DATA=FDTOTAL NWAY CLASS PRODTYP SUBTTL SUBTYP → FDTOTA2
    REMMTH1='SUB-TOTAL'
    """
    groups = {}
    for r in fdtotal:
        k = (r.get('prodtyp'), r.get('subttl'), r.get('subtyp'))
        if k not in groups:
            groups[k] = {'amount': 0.0, 'cost': 0.0, 'remm': 0.0}
        groups[k]['amount'] += r.get('amount') or 0.0
        groups[k]['cost']   += r.get('cost')   or 0.0
        groups[k]['remm']   += r.get('remm')   or 0.0

    rows = []
    for (prodtyp, subttl, subtyp), vals in groups.items():
        amt_raw = round(vals['amount'] * 1000)
        wacost  = safe_div(vals['cost'], amt_raw)
        waremm  = safe_div(vals['remm'], amt_raw)
        rows.append({
            'prodtyp': prodtyp, 'subttl': subttl, 'subtyp': subtyp,
            'amount':  vals['amount'],
            'cost':    vals['cost'],
            'remm':    vals['remm'],
            'wacost':  wacost,
            'waremm':  waremm,
            'remmth':  None,
            'remmth1': 'SUB-TOTAL',
        })
    return rows


# ============================================================================
# REPORT RENDERING
# ============================================================================

class ReportWriter:
    """ASA carriage-control report writer."""

    ASA_SINGLE  = ' '   # single space (next line)
    ASA_DOUBLE  = '0'   # double space (skip 1 line)
    ASA_NEW_PAGE = '1'  # new page

    def __init__(self, filepath, lines_per_page=LINES_PER_PAGE):
        self.filepath       = filepath
        self.lines_per_page = lines_per_page
        self.lines          = []
        self.page_line_count = 0
        self.page_num        = 1

    def _emit(self, asa, text):
        self.lines.append(asa + text)
        if asa == self.ASA_NEW_PAGE:
            self.page_line_count = 1
        elif asa == self.ASA_DOUBLE:
            self.page_line_count += 2
        else:
            self.page_line_count += 1

    def new_page(self, text=''):
        self._emit(self.ASA_NEW_PAGE, text)
        self.page_num += 1

    def single(self, text=''):
        self._emit(self.ASA_SINGLE, text)

    def double(self, text=''):
        self._emit(self.ASA_DOUBLE, text)

    def check_page(self, title_lines):
        """Start a new page if close to bottom."""
        if self.page_line_count >= self.lines_per_page - 4:
            for tl in title_lines:
                self.new_page(tl) if self.page_line_count == 0 else self.single(tl)
            self.page_line_count = len(title_lines)

    def save(self):
        with open(self.filepath, 'w', encoding='utf-8') as f:
            f.write('\n'.join(self.lines))
            f.write('\n')


# ============================================================================
# REPORT 1: DEPFINAL (non-FD deposits)
# ============================================================================

def report_depfinal(writer, depfinal, titles):
    """
    PROC TABULATE DATA=DEPFINAL WHERE PRODTYP NOT IN FD types.
    Rows: PRODTYP * SUBTTL * REMMTH
    Cols: SUBTYP * (AMOUNT, WACOST, WAREMM)
    """
    fd_prodtyps = {'FIXED DEPT(RM)', 'FIXED DEPT(FCY)'}
    rows = [r for r in depfinal if r.get('prodtyp') not in fd_prodtyps]

    # Sort by PRODTYP, SUBTTL, SUBTYP, REMMTH
    def sort_key(r):
        return (r.get('prodtyp', ''), r.get('subttl', ''),
                r.get('subtyp', ''), r.get('remmth') or 0)
    rows.sort(key=sort_key)

    # Group by PRODTYP > SUBTTL > REMMTH, cols = SUBTYP
    header = _build_header_depfinal()

    def _write_titles(w):
        for t in titles:
            w.single(t)
        w.single('')

    _write_titles(writer)
    writer.single(header['separator'])
    writer.single(header['col_header1'])
    writer.single(header['col_header2'])
    writer.single(header['separator'])

    # Pivot: for each (prodtyp, subttl, remmth) row → show CONVENTIONAL, SPTF, TOTAL cols
    # Build pivot table
    pivot = {}
    for r in rows:
        k = (r.get('prodtyp', ''), r.get('subttl', ''), r.get('remmth1', ''))
        s = r.get('subtyp', '')
        if k not in pivot:
            pivot[k] = {}
        pivot[k][s] = r

    # Sort keys
    def pkey(k):
        prodtyp, subttl, remmth1 = k
        return (prodtyp, subttl, remmth1)
    sorted_keys = sorted(pivot.keys(), key=pkey)

    last_prodtyp = None
    last_subttl  = None

    for k in sorted_keys:
        prodtyp, subttl, remmth1 = k
        subtypes = pivot[k]

        if prodtyp != last_prodtyp:
            writer.double(f"  {prodtyp}")
            last_prodtyp = prodtyp
            last_subttl  = None

        if subttl != last_subttl:
            writer.single(f"    {subttl_format(subttl)}")
            last_subttl = subttl

        row_parts = []
        for st in _SUBTYP_ORDER:
            r = subtypes.get(st, {})
            amt    = r.get('amount', 0.0) or 0.0
            wacost = r.get('wacost', 0.0) or 0.0
            waremm = r.get('waremm', 0.0) or 0.0
            row_parts.append(
                f"{fmt_comma12(amt)}{fmt_comma12_2(wacost)}{fmt_comma5_2(waremm)}"
            )
        label = f"{remmth1:<20}" if remmth1 else ' ' * 20
        writer.single(f"      {label}  {'  '.join(row_parts)}")

    writer.single(header['separator'])


def _build_header_depfinal():
    col_labels = ['CONVENTIONAL', 'SPTF', 'TOTAL']
    sub_headers = ['BAL OUSTANDING (RM\'000)', 'W.A. COST %', 'REMAINING MATURITY']
    widths       = [12, 12, 5]
    sep = '-' * PAGE_WIDTH
    h1 = 'DEPOSITS' + ' ' * 57
    for lbl in col_labels:
        h1 += f"{lbl:^31}"
    h2 = ' ' * 65
    for _ in col_labels:
        for sub, w in zip(sub_headers, widths):
            h2 += f"{sub:>{w+2}}"
    return {'separator': sep, 'col_header1': h1, 'col_header2': h2}


# ============================================================================
# REPORT 2: FD dataset
# ============================================================================

def report_fd(writer, fd_data, titles):
    """
    PROC TABULATE DATA=FD WHERE PRODTYP IN FD types.
    Rows: PRODTYP * SUBTTL * REMMTH1
    Cols: SUBTYP * (AMOUNT, WACOST, WAREMM)
    """
    fd_prodtyps = {'FIXED DEPT(RM)', 'FIXED DEPT(FCY)'}
    rows = [r for r in fd_data if r.get('prodtyp') in fd_prodtyps]

    def sort_key(r):
        return (r.get('prodtyp', ''), r.get('subttl', ''),
                r.get('remmth1', ''), r.get('subtyp', ''))
    rows.sort(key=sort_key)

    header = _build_header_depfinal()

    def _write_titles(w):
        for t in titles:
            w.single(t)
        w.single('')

    _write_titles(writer)
    writer.single(header['separator'])
    writer.single(header['col_header1'])
    writer.single(header['col_header2'])
    writer.single(header['separator'])

    pivot = {}
    for r in rows:
        k = (r.get('prodtyp', ''), r.get('subttl', ''), r.get('remmth1', ''))
        s = r.get('subtyp', '')
        if k not in pivot:
            pivot[k] = {}
        pivot[k][s] = r

    def pkey(k):
        return k
    sorted_keys = sorted(pivot.keys(), key=pkey)

    last_prodtyp = None
    last_subttl  = None

    for k in sorted_keys:
        prodtyp, subttl, remmth1 = k
        subtypes = pivot[k]

        if prodtyp != last_prodtyp:
            writer.double(f"  {prodtyp}")
            last_prodtyp = prodtyp
            last_subttl  = None

        if subttl != last_subttl:
            writer.single(f"    {subttl_format(subttl)}")
            last_subttl = subttl

        row_parts = []
        for st in _SUBTYP_ORDER:
            r = subtypes.get(st, {})
            amt    = r.get('amount', 0.0) or 0.0
            wacost = r.get('wacost', 0.0) or 0.0
            waremm = r.get('waremm', 0.0) or 0.0
            row_parts.append(
                f"{fmt_comma12(amt)}{fmt_comma12_2(wacost)}{fmt_comma5_2(waremm)}"
            )
        label = f"{remmth1:<20}" if remmth1 else ' ' * 20
        writer.single(f"      {label}  {'  '.join(row_parts)}")

    writer.single(header['separator'])


# ============================================================================
# MAIN
# ============================================================================

def main():
    # ---- REPTDATE ----
    reptdate, nowk, reptyrs, reptyear, reptmon, reptday, rdate = get_reptdate()

    # ---- FIXED DEPOSITS ----
    fd_rows, td_rows, fdn_rows = process_fd(reptdate)

    # Summarise TD, FD, FDN
    grp = ['prodtyp', 'subtyp', 'subttl', 'remmth']
    sv  = ['amount', 'cost', 'remm']
    td_sum  = apply_remfmt_label(summarise(td_rows,  grp, sv))
    fd_sum  = apply_remfmt_label(summarise(fd_rows,  grp, sv))
    fdn_sum = apply_remfmt_label(summarise(fdn_rows, grp, sv))

    # Combine TD + FD + FDN, add remmth1
    fd_all = td_sum + fd_sum + fdn_sum
    # Sort by prodtyp, subttl, subtyp, remmth1
    fd_all.sort(key=lambda r: (r.get('prodtyp',''), r.get('subttl',''),
                                r.get('subtyp',''), r.get('remmth1','')))

    # Build DUMMY and MERGE
    dummy     = build_dummy(fd_all)
    fd_merged = merge_fd_dummy(fd_all, dummy)

    # ---- SAVINGS ----
    sa_rows = process_savings()
    sa_sum  = apply_remfmt_label(summarise(sa_rows, grp, sv))

    # ---- CURRENT ----
    ca_rows, cag_rows, cas_rows = process_current()
    ca_sum  = apply_remfmt_label(summarise(ca_rows,  grp, sv))
    cag_sum = apply_remfmt_label(summarise(cag_rows, grp, sv))
    cas_sum = apply_remfmt_label(summarise(cas_rows, grp, sv))

    # ---- DEP ----
    dep = build_dep(fd_merged, sa_sum, ca_sum, cag_sum, cas_sum)

    # ---- DEPTOTAL (SA & CA per PRODTYP/SUBTTL/REMMTH) ----
    deptotal = build_deptotal(dep)

    # ---- DEPFINAL = DEP + DEPTOTAL ----
    depfinal = dep + deptotal

    # ---- DEPTOTA2 (grand subtotal per PRODTYP/SUBTYP) ----
    deptota2 = build_deptota2(depfinal)

    # ---- DEPFINAL = DEPFINAL + DEPTOTA2 ----
    depfinal = depfinal + deptota2

    # ---- FD TOTALS ----
    # PROC SUMMARY on DEP for FD prodtyps by PRODTYP/SUBTTL/REMMTH1
    fd_prodtyps = {'FIXED DEPT(RM)', 'FIXED DEPT(FCY)'}
    dep_fd_only = [r for r in dep if r.get('prodtyp') in fd_prodtyps]

    # build deptotal (subtyp=TOTAL) for FD rows
    fd_deptotal = build_deptotal([r for r in dep if r.get('prodtyp') in fd_prodtyps])

    fdtotal  = build_fdtotal(dep, fd_deptotal)
    fdtota2  = build_fdtota2(fdtotal)
    fd_final = fdtotal + fdtota2

    # ---- TITLES ----
    title1 = 'PUBLIC ISLAMIC BANK BERHAD'
    title2 = f'TIME TO MATURITY AS AT {rdate}'
    title3 = 'RISK MANAGEMENT REPORT : EIIMRM01'
    title4 = 'RM DENOMINATION'
    titles = [title1, title2, title3, title4]

    # ---- WRITE REPORT ----
    writer = ReportWriter(OUTPUT_FILE, LINES_PER_PAGE)
    writer.new_page(title1)
    writer.single(title2)
    writer.single(title3)
    writer.single(title4)

    report_depfinal(writer, depfinal, titles)

    writer.new_page(title1)
    writer.single(title2)
    writer.single(title3)
    writer.single(title4)

    report_fd(writer, fd_final, titles)

    writer.save()
    print(f"Report written to {OUTPUT_FILE}")


if __name__ == '__main__':
    main()
