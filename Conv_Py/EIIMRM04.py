#!/usr/bin/env python3
"""
Program : EIIMRM04.py (ISLAMIC)
Report  : REPRICING GAP AS AT <RDATE>
          RISK MANAGEMENT REPORT : EIIMRM04
          RM DENOMINATION
"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
# PBBLNFMT - provides format_lnprod (PUT(x, LNPROD.)) and
#                      format_odprod (PUT(x, ODPROD.))
#            Both are called in the DATA START step.
#            format_lnrate is NOT called directly here; repricing type is
#            derived from NTINDEX/INTTYPE logic embedded in the program.
from PBBLNFMT import format_lnprod, format_odprod

import duckdb
import polars as pl
from datetime import date, datetime, timedelta
import os

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
PARQUET_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR  = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

REPTDATE_PARQUET = os.path.join(PARQUET_DIR, "REPTDATE.parquet")
OVERDFT_PARQUET  = os.path.join(PARQUET_DIR, "OVERDFT.parquet")
LNNOTE_PARQUET   = os.path.join(PARQUET_DIR, "LNNOTE.parquet")
PEND_PARQUET     = os.path.join(PARQUET_DIR, "PEND.parquet")

OUTPUT_FILE = os.path.join(OUTPUT_DIR, "EIIMRM04.txt")

# Loan monthly snapshot parquet is named by reptmon+nowk; resolved after
# REPTDATE is read.

# ============================================================================
# FORMAT DEFINITIONS
# ============================================================================

def slnprdf(product):
    """SLNPRDF format: summary loan product group (used as PRODBIG)."""
    p = product
    if p in (227, 228, 230, 231, 232, 233, 241, 243):
        return '  1.HOME 1YR FIX'
    if p in (237, 238):
        return '  1.HOME 3YRS FIX'
    if p in (239, 240):
        return '  1.HOME 5YRS FIX'
    if p == 234:
        return '  1.MORE 1YR FIX'
    if p == 235:
        return '  1.MORE 3YRS FIX'
    if p == 236:
        return '  1.MORE 5YRS FIX'
    if p == 242:
        return '  1.MORE 1YR FIX'
    if p in (380, 381):
        return '  2.HIRE PURCHASE'
    if p == 390:
        return '  9.LEASING'
    if p in (209, 210, 211, 212, 214, 215, 204, 205, 200, 201,
             219, 220, 225, 226, 245, 246, 247):
        return '  1.OTHER HOUSING'
    if p in (309, 310, 904, 905):
        return '  5.BRIDGING'
    if p in (300, 301, 900, 901, 530, 362):
        return '  6.FIXED TIER'
    if 1 <= p <= 100:
        return ' 10.STAFF'
    if p in (359, 906, 363):
        return '  3.SWIFT'
    if p in (360, 908):
        return '  6.FIXED LOAN'
    if p in (361, 907):
        return '  4.SMILAX'
    if p == 531:
        return ' 12.FUNDED BNM'
    if 110 <= p <= 118 or p in (139, 140):
        return ' 11.ABBA HOUSE'
    if p == 120:
        return ' 11.ABBA OTH TERM'
    if p in (194, 195, 196):
        return ' 11.ABBA CONSUMER'
    if p == 181:
        return ' 11.ABBA SYNDICATE'
    if p in (180, 183):
        return ' 11.ABBA SYNDICATE(FIXED)'
    if p in (127, 126):
        return ' 11.ABBA SWIFT'
    if p == 129:
        return ' 11.ABBA SMILAX'
    if p == 193:
        return ' 11.ABBA OTH TERM'
    if p == 137:
        return ' 11.ABBA OTH TERM'
    if p in (135, 136, 138):
        return ' 11.ABBA PERSONAL'
    if p in (197, 170):
        return ' 11.ABBA OTH TERM'
    if p == 122:
        return ' 11.ABBA UNIT TRST'
    if p in (141, 142):
        return ' 11.ABBA HOUSE BFR'
    if p == 143:
        return ' 11.ABBA TERM BFR'
    if p == 182:
        return ' 11.ABBA SYN.BULLET(FIXED)'
    if p in (564, 565, 569, 561, 559, 560, 567, 555, 556,
             566, 568, 570, 573, 909):
        return ' 12.FUNDED BNM'
    if p in (521, 522, 523, 528, 517, 527, 524, 525, 526):
        return ' 13.CGC'
    if p in (910, 350, 925, 302, 902, 903, 951):
        return '  8.REVOLVING CRDT'
    if p in (914, 915, 919, 920, 950):
        return '  7.SYNDICATED'
    if p in (345, 304, 305, 355, 356, 504, 505, 509, 510, 515,
             325, 357, 518, 519, 335, 358, 320, 391, 330, 364, 365, 506):
        return '  6.FIXED LOAN'
    if p in (131, 132):
        return ' 14.AITAB VARIABLE'
    if p in (720, 725):
        return ' 15.HP VARIABLE'
    return ' 16.OTHERS'


def lnprdf(product):
    """LNPRDF format: detailed loan product group (used as PRODTYP)."""
    p = product
    if p in (227, 228):
        return '  1.HOME PLAN 1'
    if p in (230, 231):
        return '  1.HOME PLAN 2'
    if p in (232, 233):
        return '  1.HOME PLAN 3'
    if p in (237, 238):
        return '  1.HOME PLAN 6'
    if p in (239, 240):
        return '  1.HOME PLAN 7'
    if p in (241, 243):
        return '  1.HOME PLAN 8'
    if p == 234:
        return '  1.MORE PLAN 1'
    if p == 235:
        return '  1.MORE PLAN 2'
    if p == 236:
        return '  1.MORE PLAN 3'
    if p == 242:
        return '  1.MORE PLAN 4'
    if p in (380, 381):
        return '  2.HIRE PURCHASE'
    if p == 390:
        return '  3.LEASING'
    if p in (209, 210, 211, 212, 214, 215):
        return '  1.HOME OWN BEF 5'
    if p in (204, 205, 200, 201, 219, 220, 225, 226, 245, 246, 247):
        return '  1.OTHER HOUSING'
    if p in (309, 310, 904, 905):
        return '  6.BRIDGING'
    if p in (300, 301, 900, 901, 530, 362, 364, 365, 506):
        return '  7.FIXED'
    if 1 <= p <= 100:
        return '  4.STAFF'
    if p in (359, 906, 363):
        return '  5.SWIFT'
    if p in (360, 908):
        return ' 17.BLOCK DISC'
    if p in (361, 907):
        return '  9.SMILAX'
    if p == 531:
        return ' 11.SRGF'
    if p in (160, 162, 163, 164):
        return ' 12.ABBA OD'
    if 110 <= p <= 118 or p in (139, 140):
        return ' 12.ABBA HOUSE'
    if p == 120:
        return ' 12.ABBA TERM'
    if p in (194, 195, 196):
        return ' 12.ABBA CONSUMER'
    if p == 181:
        return ' 12.ABBA SYNDICATE'
    if p in (180, 183):
        return ' 12.ABBA SYNDICATE(FIXED)'
    if p in (127, 126):
        return ' 12.ABBA SWIFT'
    if p == 129:
        return ' 12.ABBA SMILAX'
    if p == 193:
        return ' 12.ABBA LEASE'
    if p == 137:
        return ' 12.ABBA OTHR PLAN'
    if p in (135, 136, 138):
        return ' 12.ABBA PERSONAL'
    if p in (197, 170):
        return ' 12.ABBA OTHER'
    if p == 122:
        return ' 12.ABBA UNIT TRST'
    if p in (141, 142):
        return ' 12.ABBA HOUSE BFR'
    if p == 143:
        return ' 12.ABBA TERM BFR'
    if p == 182:
        return ' 12.ABBA SYN.BULLET(FIXED)'
    if p in (564, 565):
        return ' 13.FUND FOR FOOD'
    if p == 561:
        return ' 13.L&MCOST HOUSE'
    if p in (559, 560, 567):
        return ' 13.NEF LOAN'
    if p in (555, 556):
        return ' 14.SFT LOAN'
    if p in (566, 568, 570, 573, 909):
        return ' 15.SMI'
    if p == 569:
        return ' 15.SFSMI2'
    if p in (521, 522, 523, 528):
        return ' 16.CGC TUK'
    if p == 517:
        return ' 16.CGC ASL'
    if p == 527:
        return ' 16.CGC NEF'
    if p in (524, 525):
        return ' 16.CGC FSMI'
    if p == 526:
        return ' 16.CGC FFF'
    if p in (910, 350, 925, 302, 902, 903, 951):
        return ' 10.REVOLVING CRDT'
    if p in (914, 915, 919, 920, 950):
        return '  8.SYNDICATED'
    if p == 345:
        return ' 18.(MISC)CONTRACT'
    if p in (304, 305):
        return ' 18.(MISC)FLASH'
    if p == 355:
        return ' 18.(MISC)PB EXEC'
    if p == 356:
        return ' 18.(MISC)HOME FURNH'
    if p in (504, 505, 509, 510, 515):
        return ' 18.(MISC)PRIN GUARAN'
    if p == 325:
        return ' 18.(MISC)PROF.ADVAN'
    if p == 357:
        return ' 18.(MISC)SHARE'
    if p in (518, 519):
        return ' 18.(MISC)SLS-FIXED'
    if p == 335:
        return ' 18.(MISC)UNIT TRUST'
    if p == 358:
        return ' 18.(MISC)UNIFLEX'
    if p == 320:
        return ' 18.(MISC)UNSECURED'
    if p == 391:
        return ' 18.(MISC)CON.DURABLE'
    if p == 330:
        return ' 18.(MISC)QUICK CASH'
    if p in (131, 132):
        return ' 19.AITAB VARIABLE'
    if p in (720, 725):
        return ' 20.HP VARIABLE'
    return ' 21.OTHERS'


def odprdf(product):
    """ODPRDF format: OD product type for PRODTYP."""
    if 60 <= product <= 64 or 160 <= product <= 166:
        return ' 12.ABBA OD'
    return '  1.CONV OD'


def sodprdf(product):
    """SODPRDF format: OD product type for PRODBIG."""
    if 60 <= product <= 64 or 160 <= product <= 166:
        return ' 11.ABBA OD'
    return '  1.CONV OD'


def subtypf(subtyp):
    """SUBTYPF format: subtype numeric code to label."""
    _map = {
        5:    'PRINCIPAL',
        5.5:  'WAREMM(MTH)',
        11:   'INSTALMENT ',
        12:   'REPRICING  ',
        13:   'NO-REPRICE',
        6:    'UNEARN INT',
        7:    'ACCRUED INT',
        8:    'FEE AMOUNT',
        9:    'NPL',
    }
    return _map.get(subtyp, str(subtyp))


def remfmt(remmth):
    """
    REMFMT format: remaining months to maturity bucket label.
    Note: LOW-1 maps to '>  0-1 MTH' (i.e. anything <= 1).
    """
    if remmth is None:
        return '  '
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
        return '>2-3 YRS    '
    if remmth <= 48:
        return '>3-4 YRS    '
    if remmth <= 60:
        return '>4-5 YRS    '
    return '>5 YRS      '


# ============================================================================
# HELPERS: DATE ARITHMETIC
# ============================================================================

_SAS_EPOCH = date(1960, 1, 1)


def sas_to_date(sas_int):
    """Convert SAS date integer (days since 1960-01-01) to Python date."""
    if sas_int is None or sas_int == 0:
        return None
    try:
        return _SAS_EPOCH + timedelta(days=int(sas_int))
    except (TypeError, ValueError, OverflowError):
        return None


def days_in_month(year, month):
    """Days in month using SAS simple leap-year rule (year % 4 == 0)."""
    if month in (1, 3, 5, 7, 8, 10, 12):
        return 31
    if month in (4, 6, 9, 11):
        return 30
    if month == 2:
        return 29 if (year % 4 == 0) else 28
    return 30


def calc_remmth(matdt, reptdate):
    """
    Replicates %REMMTH macro.
    REMMTH = (MDYR-RPYR)*12 + (MDMTH-RPMTH) + (MDDAY-RPDAY) / RPDAYS(RPMTH)
    MDDAY capped at RPDAYS(RPMTH).
    """
    if matdt is None or reptdate is None:
        return 0.0
    rpyr  = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day
    rp_days = days_in_month(rpyr, rpmth)

    mdyr  = matdt.year
    mdmth = matdt.month
    mdday = matdt.day

    if mdday > rp_days:
        mdday = rp_days

    return (mdyr - rpyr) * 12 + (mdmth - rpmth) + (mdday - rpday) / rp_days


def add_months_day(base_date, months, day_anchor):
    """
    Advance base_date by <months> months, using day_anchor as the target day.
    Caps day at month end. Used in %NXTBLDT for non-biweekly PAYFREQ.
    """
    mm = base_date.month + months
    yy = base_date.year
    while mm > 12:
        mm -= 12
        yy += 1
    dd = day_anchor
    max_dd = days_in_month(yy, mm)
    if dd > max_dd:
        dd = max_dd
    return date(yy, mm, dd)


def add_biweekly(bldate):
    """
    %NXTBLDT for PAYFREQ='6': add 14 days, overflow to next month.
    Uses SAS simple leap-year rule for February.
    """
    dd = bldate.day + 14
    mm = bldate.month
    yy = bldate.year
    max_dd = days_in_month(yy, mm)
    if dd > max_dd:
        dd -= max_dd
        mm += 1
        if mm > 12:
            mm -= 12
            yy += 1
    max_dd2 = days_in_month(yy, mm)
    if dd > max_dd2:
        dd = max_dd2
    return date(yy, mm, dd)


def next_bldate(bldate, issdte, payfreq, freq):
    """
    Replicates %NXTBLDT macro.
    payfreq: string '1','2','3','4','6'
    freq: integer months per period (from PAYFREQ mapping)
    issdte: issue date (date object), used as day anchor for non-biweekly
    """
    if payfreq == '6':
        return add_biweekly(bldate)
    else:
        day_anchor = issdte.day if issdte else bldate.day
        return add_months_day(bldate, freq, day_anchor)


def parse_mmddyy8_from_z11(val):
    """
    Replicates INPUT(SUBSTR(PUT(val,Z11.),1,8), MMDDYY8.) pattern.
    Z11 pads to 11 digits; MMDDYY8 reads MMDDYYYY from first 8 chars.
    YEARCUTOFF=1950: YY>=50 → 19YY, else 20YY.
    """
    if val is None or val == 0:
        return None
    s = f"{int(val):011d}"
    mmddyyyy = s[:8]
    try:
        mm = int(mmddyyyy[0:2])
        dd = int(mmddyyyy[2:4])
        yy = int(mmddyyyy[4:8])
        if yy == 0:
            return None
        return date(yy, mm, dd)
    except (ValueError, TypeError):
        return None


def parse_reldte_to_date(reldte):
    """
    Replicates INPUT(SUBSTR(RELDTE,6,4)||SUBSTR(RELDTE,2,4), MMDDYY8.).
    RELDTE is a string; chars are 1-indexed in SAS.
    Result = SUBSTR(RELDTE,6,4) || SUBSTR(RELDTE,2,4) → MMDDYYYY.
    """
    if not reldte or len(reldte) < 9:
        return None
    # SAS 1-indexed: substr(reldte,6,4) = chars 5-8 (0-indexed)
    #                substr(reldte,2,4) = chars 1-4 (0-indexed)
    part1 = reldte[5:9]   # 4 chars starting at index 5 (SAS pos 6)
    part2 = reldte[1:5]   # 4 chars starting at index 1 (SAS pos 2)
    mmddyyyy = part1 + part2
    try:
        mm = int(mmddyyyy[0:2])
        dd = int(mmddyyyy[2:4])
        yyyy = int(mmddyyyy[4:8])
        if yyyy == 0:
            return None
        return date(yyyy, mm, dd)
    except (ValueError, TypeError):
        return None


def fix_payeffdt(payeffdt_int):
    """
    Replicates LNNOTE payeffdt parsing:
      PAYCY = SUBSTR(PUT(PAYEFFDT,Z11.),1,4)
      PAYMM = SUBSTR(PUT(PAYEFFDT,Z11.),8,2)
      PAYDD = SUBSTR(PUT(PAYEFFDT,Z11.),10,2)
    Then clamped to valid day for the month.
    Returns a date or None.
    """
    if payeffdt_int is None or payeffdt_int == 0:
        return None
    s = f"{int(payeffdt_int):011d}"
    try:
        paycy = int(s[0:4])
        paymm = int(s[7:9])
        paydd = int(s[9:11])
    except (ValueError, IndexError):
        return None
    if paymm == 0 or paycy == 0:
        return None
    # Clamp day
    if paymm == 2:
        max_dd = 29 if (paycy % 4 == 0) else 28
        if paydd > max_dd:
            paydd = max_dd
    elif paymm in (1, 3, 5, 7, 8, 10, 12):
        if paydd > 31:
            paydd = 31
    elif paymm in (4, 6, 9, 11):
        if paydd > 30:
            paydd = 30
    try:
        return date(paycy, paymm, paydd)
    except ValueError:
        return None


def safe_div(num, denom):
    if denom and denom != 0:
        return (num or 0.0) / denom
    return 0.0


# ============================================================================
# STEP 1: REPTDATE
# ============================================================================

def get_reptdate():
    con = duckdb.connect()
    row = con.execute(f"SELECT reptdate FROM '{REPTDATE_PARQUET}' LIMIT 1").fetchone()
    con.close()

    reptdate = sas_to_date(row[0])
    day = reptdate.day

    if day == 8:
        sdd, wk, wk1 = 1, '1', '4'
    elif day == 15:
        sdd, wk, wk1 = 9, '2', '1'
    elif day == 22:
        sdd, wk, wk1 = 16, '3', '2'
    else:
        sdd, wk, wk1 = 23, '4', '3'

    mm = reptdate.month
    if wk == '1':
        mm1 = mm - 1
        if mm1 == 0:
            mm1 = 12
    else:
        mm1 = mm

    sdate = date(reptdate.year, mm, sdd)
    reptmon  = str(mm).zfill(2)
    reptmon1 = str(mm1).zfill(2)
    reptyear = str(reptdate.year)
    reptday  = str(reptdate.day).zfill(2)
    rdate    = f"{reptdate.day:02d}/{reptdate.month:02d}/{reptdate.year}"
    sdate_str = f"{sdate.day:02d}/{sdate.month:02d}/{sdate.year}"
    btype    = 'PBB'

    return (reptdate, wk, wk1, reptmon, reptmon1, reptyear,
            reptday, rdate, sdate_str, btype)


# ============================================================================
# STEP 2: OD – filter, parse LMTEND, dedup by ACCTNO
# ============================================================================

def load_od():
    """
    DATA OD: WHERE LMTENDDT NE . AND LMTENDDT > 0
    LMTEND = INPUT(SUBSTR(PUT(LMTENDDT,Z11.),1,8), MMDDYY8.)
    PROC SORT NODUPKEYS BY ACCTNO → keep ACCTNO, LMTEND, LMTENDDT, RISKCODE
    """
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM '{OVERDFT_PARQUET}'").df()
    con.close()
    df = pl.from_pandas(df)

    rows = {}
    for r in df.iter_rows(named=True):
        lmtenddt = r.get('lmtenddt')
        if lmtenddt is None or lmtenddt == 0:
            continue
        lmtend = parse_mmddyy8_from_z11(lmtenddt)
        if lmtend is None:
            continue
        acctno = r.get('acctno')
        # NODUPKEYS: keep first occurrence
        if acctno not in rows:
            rows[acctno] = {
                'acctno':   acctno,
                'lmtend':   lmtend,
                'lmtenddt': lmtenddt,
                'riskcode': r.get('riskcode'),
            }
    return rows  # dict keyed by acctno


# ============================================================================
# STEP 3: LNNOTE – load and merge PEND
# ============================================================================

def load_lnnote_pend():
    """
    Loads LNNOTE and PEND, replicates:
      DATA REALPEND / SECOND / THIRD / REPRPEND from PEND
      DATA REALPEN2 (UPDATE REALPEND + SECOND)
      DATA REALPEN2 (UPDATE REALPEN2 + THIRD)
      DATA PENDFIN (UPDATE REALPEN2 + REPRPEND)
      DATA LNNOTE (MERGE LNNOTE + PENDFIN, parse PAYEFFDT → PAYEFDT)
    Returns dict keyed by (acctno, noteno).
    """
    con = duckdb.connect()
    pend_df   = con.execute(f"SELECT acctno, noteno, rateover, reldte FROM '{PEND_PARQUET}'").df()
    lnnote_df = con.execute(
        f"SELECT acctno, noteno, ntint, payeffdt, ntindex, loantype, census "
        f"FROM '{LNNOTE_PARQUET}' "
        f"WHERE loantype NOT IN (700,705,380,381,128,130,500,520)"
    ).df()
    con.close()

    pend_df   = pl.from_pandas(pend_df)
    lnnote_df = pl.from_pandas(lnnote_df)

    # ---- PEND processing ----
    # Sort by acctno, noteno
    pend_rows = sorted(pend_df.iter_rows(named=True),
                       key=lambda r: (r.get('acctno',''), r.get('noteno',0)))

    # Group by (acctno, noteno) to identify first/last
    from itertools import groupby

    realpend  = {}   # keyed by (acctno, noteno) → first rateover>0
    second    = {}   # keyed by (acctno, noteno) → intermediate rateover>0
    third     = {}   # keyed by (acctno, noteno) → last rateover>0
    reprpend  = {}   # keyed by (acctno, noteno) → rateover=0

    def _grp_key(r):
        return (r.get('acctno',''), r.get('noteno', 0))

    for key, grp in groupby(pend_rows, key=_grp_key):
        items = list(grp)
        positive = [x for x in items if (x.get('rateover') or 0) > 0]
        zero_rte = [x for x in items if (x.get('rateover') or 0) <= 0]

        for x in zero_rte:
            repricdt = parse_reldte_to_date(x.get('reldte',''))
            reprpend[key] = {'acctno': key[0], 'noteno': key[1],
                             'repricdt': repricdt}

        if positive:
            # first
            x = positive[0]
            realpend[key] = {
                'acctno':    key[0], 'noteno': key[1],
                'rateover':  x.get('rateover'),
                'realisdt':  parse_reldte_to_date(x.get('reldte','')),
            }
            # middle items → second (keep last middle)
            if len(positive) > 2:
                x2 = positive[1]
                second[key] = {
                    'acctno': key[0], 'noteno': key[1],
                    'rateove2': x2.get('rateover'),
                    'realisd2': parse_reldte_to_date(x2.get('reldte','')),
                }
            # last
            if len(positive) > 1:
                x3 = positive[-1]
                third[key] = {
                    'acctno': key[0], 'noteno': key[1],
                    'rateove3': x3.get('rateover'),
                    'realisd3': parse_reldte_to_date(x3.get('reldte','')),
                }

    # UPDATE REALPEND + SECOND → REALPEN2
    realpen2 = {k: dict(v) for k, v in realpend.items()}
    for k, v in second.items():
        if k in realpen2:
            realpen2[k]['rateove2'] = v.get('rateove2')
            realpen2[k]['realisd2'] = v.get('realisd2')
        else:
            realpen2[k] = {'acctno': k[0], 'noteno': k[1],
                           'rateove2': v.get('rateove2'),
                           'realisd2': v.get('realisd2')}

    # UPDATE REALPEN2 + THIRD
    for k, v in third.items():
        if k in realpen2:
            realpen2[k]['rateove3'] = v.get('rateove3')
            realpen2[k]['realisd3'] = v.get('realisd3')
        else:
            realpen2[k] = {'acctno': k[0], 'noteno': k[1],
                           'rateove3': v.get('rateove3'),
                           'realisd3': v.get('realisd3')}

    # UPDATE REALPEN2 + REPRPEND → PENDFIN
    pendfin = {k: dict(v) for k, v in realpen2.items()}
    for k, v in reprpend.items():
        if k in pendfin:
            pendfin[k]['repricdt'] = v.get('repricdt')
        else:
            pendfin[k] = {'acctno': k[0], 'noteno': k[1],
                          'repricdt': v.get('repricdt')}

    # ---- LNNOTE merge with PENDFIN ----
    lnnote_out = {}
    for r in lnnote_df.iter_rows(named=True):
        acctno = r.get('acctno')
        noteno = r.get('noteno')
        key    = (acctno, noteno)

        pf = pendfin.get(key, {})
        pendind = 'Y' if key in pendfin else None

        payeffdt_int = r.get('payeffdt')
        payefdt = fix_payeffdt(payeffdt_int)

        lnnote_out[key] = {
            'acctno':   acctno,
            'noteno':   noteno,
            'ntint':    r.get('ntint'),
            'ntindex':  r.get('ntindex'),
            'loantype': r.get('loantype'),
            'census':   r.get('census'),
            'pendind':  pendind,
            'payefdt':  payefdt,
            'repricdt': pf.get('repricdt'),
        }

    return lnnote_out


# ============================================================================
# STEP 4: BUILD START DATASET
# ============================================================================

_RC_PRODUCTS = {350, 910, 925, 302, 902, 903, 951}
_EXCL_LOAN   = {700, 705, 380, 381, 128, 130, 500, 520}
_HP_VAR      = {131, 132, 720, 725}


def _payfreq_to_freq(payfreq):
    """Map PAYFREQ code to months per period."""
    return {'1': 1, '2': 3, '3': 6, '4': 12}.get(payfreq, None)


def build_start(reptdate, reptmon, nowk):
    """
    Loads BNM.LOAN{reptmon}{nowk} parquet, merges OD and LNNOTE,
    builds the START dataset with instalment schedule expansion.
    Returns list of output row dicts.
    """
    loan_parquet = os.path.join(PARQUET_DIR, f"LOAN{reptmon}{nowk}.parquet")

    con = duckdb.connect()
    loan_df = con.execute(
        f"SELECT * FROM '{loan_parquet}' "
        f"WHERE product NOT IN (700,705,380,381,128,130,500,520)"
    ).df()
    con.close()
    loan_df = pl.from_pandas(loan_df)

    od_map     = load_od()
    lnnote_map = load_lnnote_pend()

    start_rows = []

    for row in loan_df.iter_rows(named=True):
        acctno   = row.get('acctno')
        noteno   = row.get('noteno')
        product  = row.get('product') or 0
        acctype  = (row.get('acctype') or '').strip()
        prodcd   = (row.get('prodcd') or '').strip()
        loantype = row.get('loantype') or product

        # WHERE clause from DATA START:
        # (SUBSTR(PRODCD,1,2) IN ('34','54') AND LOANTYPE NOT IN excl)
        # OR LOANTYPE IN (131,132,720,725)
        prodcd_prefix = prodcd[:2] if len(prodcd) >= 2 else ''
        qualifies = (
            (prodcd_prefix in ('34', '54') and loantype not in _EXCL_LOAN)
            or loantype in _HP_VAR
        )
        if not qualifies:
            continue

        # Merge OD
        od_rec    = od_map.get(acctno, {})
        lmtend    = od_rec.get('lmtend')
        riskcode  = od_rec.get('riskcode')

        # Merge LNNOTE
        ln_key = (acctno, noteno)
        ln_rec = lnnote_map.get(ln_key, {})
        ntindex  = ln_rec.get('ntindex') or row.get('ntindex')
        ntint    = (ln_rec.get('ntint') or row.get('ntint') or '').strip()
        census   = (ln_rec.get('census') or row.get('census') or '').strip()
        payefdt  = ln_rec.get('payefdt')
        repricdt_ln = ln_rec.get('repricdt')

        # Loan fields
        balance  = row.get('balance', 0) or 0.0
        curbal   = row.get('curbal', 0) or 0.0
        intamt   = row.get('intamt', 0) or 0.0
        intearn  = row.get('intearn', 0) or 0.0
        intearn2 = row.get('intearn2', 0) or 0.0
        intearn3 = row.get('intearn3', 0) or 0.0
        feeamt   = row.get('feeamt', 0) or 0.0
        intrate  = row.get('intrate', 0) or 0.0
        payfreq  = (row.get('payfreq') or '').strip()
        payamt   = row.get('payamt', 0) or 0.0
        costfund = row.get('costfund', 0) or 0.0
        amtind   = (row.get('amtind') or '').strip()

        exprdate_raw = row.get('exprdate')
        exprdate = sas_to_date(exprdate_raw)
        issdte_raw = row.get('issdte')
        issdte   = sas_to_date(issdte_raw)
        bldate_raw = row.get('bldate')
        bldate0  = sas_to_date(bldate_raw)

        # Determine INTTYPE
        if ntindex in (1, 30, 997) or (acctype == 'OD' and amtind != 'I'):
            inttype = 'BLR'
        elif ntindex != 1 or (acctype == 'OD' and amtind == 'I'):
            inttype = 'FIX'
        else:
            inttype = 'OTH'
        if product in _RC_PRODUCTS:
            inttype = 'FIX'

        # Adjust REPRICDT / EXPRDATE from LNNOTE (DATA LOAN step)
        repricdt = repricdt_ln
        if payfreq in ('5', '9', '') or product in _RC_PRODUCTS:
            repricdt = payefdt

        # ---- ACCTYPE = 'LN' branch ----
        if acctype == 'LN':
            if format_lnprod(product) == 'N':
                continue
            prodtyp = lnprdf(product)
            prodbig = slnprdf(product)

            # CBL loans override
            if loantype in (900, 901):
                if ntindex == '1':
                    prodtyp = '  6.FIXED CORPORATE BLR'
                    prodbig = '  7.FIXED CORPORATE BLR'
                elif costfund == 0 and (payfreq == '5' or payamt == 0):
                    prodtyp = '  6.FIXED CORPORATE BULLET(FIXED RATE)'
                    prodbig = '  7.FIXED CORPORATE BULLET(FIXED RATE)'
                    repricdt = exprdate
                elif costfund != 0 and (payfreq == '5' or payamt == 0):
                    prodtyp = '  6.FIXED COF BULLET(FIXED RATE)'
                    prodbig = '  7.FIXED COF BULLET(FIXED RATE)'
                    repricdt = exprdate
                else:
                    prodtyp = '  6.CORPORATE FIXED'
                    prodbig = '  7.CORPORATE FIXED'

            # HOME/MORE plan override for products 244,245,247
            if loantype in (244, 245, 247):
                censusx = census.lstrip()
                if censusx and censusx[0] == '8':
                    plan = censusx[1] if len(censusx) > 1 else '1'
                    prodtyp = f'  1.HOME PLAN {plan}'
                    pi = int(plan) if plan.isdigit() else 1
                    if pi <= 3:
                        prodbig = '  1.HOME 1YR FIX'
                    elif pi <= 6:
                        prodbig = '  1.HOME 3YRS FIX'
                    elif pi <= 7:
                        prodbig = '  1.HOME 5YRS FIX'
                    else:
                        prodbig = '  1.HOME 1YR FIX'
                elif censusx and censusx[0] == '3':
                    plan = censusx[1] if len(censusx) > 1 else '1'
                    prodtyp = f'  1.MORE PLAN {plan}'
                    pi = int(plan) if plan.isdigit() else 1
                    if pi == 1:
                        prodbig = '  1.MORE 1YR FIX'
                    elif pi == 2:
                        prodbig = '  1.MORE 3YRS FIX'
                    elif pi == 3:
                        prodbig = '  1.HOME 5YRS FIX'
                    else:
                        prodbig = '  1.MORE 1YR FIX'
                elif censusx and censusx[0] == '2':
                    plan = censusx[3] if len(censusx) > 3 else ''
                    if plan in ('1', '2'):
                        prodbig = '  1.HOME 9 FIX'
                        prodtyp = '  1.HOME PLAN 9'
                    elif plan in ('3', '4'):
                        prodbig = '  1.MORE 9 FIX'
                        prodtyp = '  1.MORE PLAN 9'
                else:
                    prodtyp = '  1.HOME PLAN 1'
                    prodbig = '  1.HOME 1YR FIX'

            if prodbig == '  1.OTHER HOUSING' and ntindex == 2:
                prodbig = '  1.OTHER PRESCRB'

            # Accrued interest / unearned
            if ntint != 'A':
                acrint = balance - curbal - feeamt
                unearn = 0.0
            else:
                acrint  = intearn
                unearn  = intamt - intearn2 + intearn3

            matdt = exprdate
            if repricdt:
                exprdate = repricdt
            riskrte = row.get('riskrte') or row.get('riskcode') or 0

        # ---- ACCTYPE = 'OD' branch ----
        else:
            if format_odprod(product) == 'N':
                continue
            prodtyp  = odprdf(product)
            prodbig  = sodprdf(product)
            exprdate = lmtend
            riskrte  = riskcode or 0
            curbal   = balance
            acrint   = 0.0
            feeamt   = 0.0
            unearn   = 0.0
            matdt    = exprdate

        def _out(subtyp, amount, yield_val, remmth1_val, matdt_val=None):
            return {
                'prodtyp':  prodtyp,
                'prodbig':  prodbig,
                'product':  product,
                'ntindex':  ntindex,
                'inttype':  inttype,
                'subtyp':   subtyp,
                'amount':   amount if amount is not None else 0.0,
                'yield':    yield_val if yield_val is not None else 0.0,
                'remmth1':  remmth1_val,
                'intrate':  intrate,
            }

        # NPL output (RISKRTE in 1,2,3,4)
        if riskrte in (1, 2, 3, 4):
            amt = curbal if curbal is not None else 0.0
            start_rows.append(_out(9, amt, 0.0, '     TOTAL'))

        # SUBTYP 7,8,6 (accrued int, fee, unearned)
        start_rows.append(_out(7, acrint, 0.0, '     TOTAL'))
        start_rows.append(_out(8, feeamt, 0.0, '     TOTAL'))
        start_rows.append(_out(6, unearn, 0.0, '     TOTAL'))

        # %REMMTH for maturity
        remmth_val = calc_remmth(matdt, reptdate)

        # SUBTYP 5.5 (WAREMM)
        if riskrte not in (1, 2, 3, 4):
            amt_55  = curbal if curbal is not None else 0.0
            yld_55  = amt_55 * remmth_val
        else:
            amt_55 = 0.0
            yld_55 = 0.0
        start_rows.append(_out(5.5, amt_55, yld_55, '     TOTAL'))

        # SUBTYP 5 (PRINCIPAL)
        amt_5  = curbal if curbal is not None else 0.0
        yld_5  = (amt_5 * intrate) if riskrte not in (1, 2, 3, 4) else 0.0
        start_rows.append(_out(5, amt_5, yld_5, '     TOTAL'))

        # Instalment schedule (RISKRTE not in 1,2,3,4 only)
        if riskrte in (1, 2, 3, 4):
            continue

        # Determine FREQ
        freq = _payfreq_to_freq(payfreq)

        # Determine starting BLDATE
        if payfreq in ('5', '9', '') or product in _RC_PRODUCTS:
            bldate = exprdate
        elif bldate0 is None:
            # Advance from ISSDTE until > reptdate
            bldate = issdte
            if bldate is None:
                bldate = reptdate
            safety = 0
            while bldate is not None and bldate <= reptdate:
                bldate = next_bldate(bldate, issdte, payfreq, freq or 1)
                safety += 1
                if safety > 600:
                    break
        else:
            bldate = bldate0

        if bldate is None or exprdate is None:
            # Cannot schedule; emit repricing/no-reprice only
            pass
        else:
            if bldate > exprdate or curbal <= payamt:
                bldate = exprdate

            totbal = curbal
            cur    = curbal
            subtyp_inst = 11 if acctype == 'LN' else None

            safety = 0
            while bldate is not None and bldate <= exprdate:
                mat2   = bldate
                rem2   = calc_remmth(mat2, reptdate)
                r1     = remfmt(rem2)

                if bldate == exprdate:
                    # Last instalment: emit repricing/no-reprice outside loop
                    break

                amt_i = payamt
                yld_i = amt_i * intrate
                if subtyp_inst is not None:
                    start_rows.append(_out(subtyp_inst, amt_i, yld_i, r1))

                cur = cur - payamt
                if cur <= payamt:
                    amt_i = cur

                bldate = next_bldate(bldate, issdte, payfreq, freq or 1)
                if bldate is None:
                    break
                if bldate > exprdate or cur <= payamt:
                    bldate = exprdate

                safety += 1
                if safety > 600:
                    break

        # Repricing / No-reprice row
        remmth_reprice = calc_remmth(exprdate, reptdate) if exprdate else 0.0
        r_label = remfmt(remmth_reprice)

        if repricdt is not None or product in _RC_PRODUCTS or inttype == 'BLR':
            amt_r = totbal if 'totbal' in dir() else curbal
            yld_r = (amt_r * intrate) if riskrte not in (1, 2, 3, 4) else 0.0
            subtyp_r = 12
            if inttype == 'BLR':
                r_label = '>  0-1 MTH'
        elif repricdt is None and ntindex != 1:
            amt_r = totbal if 'totbal' in dir() else curbal
            yld_r = (amt_r * intrate) if riskrte not in (1, 2, 3, 4) else 0.0
            subtyp_r = 13
        else:
            amt_r = None

        if amt_r is not None:
            start_rows.append(_out(subtyp_r, amt_r, yld_r, r_label))

    return start_rows


# ============================================================================
# STEP 5: SUMMARISE HELPERS
# ============================================================================

def summarise(rows, group_cols, sum_cols):
    """Group-by SUM over list of dicts."""
    if not rows:
        return []
    groups = {}
    for r in rows:
        key = tuple(r.get(c) for c in group_cols)
        if key not in groups:
            groups[key] = {c: 0.0 for c in sum_cols}
            for c in group_cols:
                groups[key][c] = r.get(c)
        for c in sum_cols:
            groups[key][c] = (groups[key][c] or 0.0) + (r.get(c) or 0.0)
    out = []
    for key, vals in groups.items():
        rec = {}
        for i, c in enumerate(group_cols):
            rec[c] = key[i]
        for c in sum_cols:
            rec[c] = vals[c]
        out.append(rec)
    return out


def add_wayld(rows):
    """Compute WAYLD = YIELD / AMOUNT for each row."""
    out = []
    for r in rows:
        r2 = dict(r)
        r2['wayld'] = safe_div(r2.get('yield', 0.0), r2.get('amount'))
        out.append(r2)
    return out


def build_subtotals(rows, group_cols_detail, sum_cols):
    """
    Replicates the 'TOTAL FOR INSTALMENT & REPRICING' PROC SUMMARY+DATA step.
    Groups by <group_cols_detail> (no REMMTH1), sums, sets REMMTH1='     TOTAL'.
    Then concatenates original + totals, filters REMMTH1 != '          ',
    sets GRANDTOT='GRAND TOTAL', zeroes rows where REMMTH1!='     TOTAL' & SUBTYP=13.
    """
    total_rows = summarise(
        [r for r in rows if r.get('subtyp') in (11, 12, 13)],
        group_cols_detail, sum_cols
    )
    for r in total_rows:
        r['remmth1'] = '     TOTAL'
        r['wayld']   = safe_div(r.get('yield', 0.0), r.get('amount'))

    combined = rows + total_rows
    combined = [r for r in combined if r.get('remmth1', '').rstrip() != '']

    for r in combined:
        r['grandtot'] = 'GRAND TOTAL'
        if r.get('remmth1') != '     TOTAL' and r.get('subtyp') == 13:
            r['amount'] = 0.0
            r['wayld']  = 0.0

    return combined


def build_grandtotal(rows, group_cols, sum_cols, label_col):
    """
    Replicates PROC SUMMARY CLASS GRANDTOT SUBTYP REMMTH1 → FIX3/BLR3
    Then SET original + grand totals, rename blank PRODTYP/PRODBIG → 'GRAND TOTAL'.
    """
    gt_rows = summarise(rows, ['grandtot', 'subtyp', 'remmth1'], sum_cols)
    for r in gt_rows:
        r['wayld'] = safe_div(r.get('yield', 0.0), r.get('amount'))
        if not r.get(label_col, '').strip():
            r[label_col] = 'GRAND TOTAL'

    combined = rows + gt_rows
    for r in combined:
        if not r.get(label_col, '').strip():
            r[label_col] = 'GRAND TOTAL'
    return combined


# ============================================================================
# REPORT WRITER
# ============================================================================

LINES_PER_PAGE = 60
PAGE_WIDTH     = 132


class ReportWriter:
    """ASA carriage-control fixed-width report writer."""
    def __init__(self, filepath):
        self.filepath = filepath
        self.lines    = []
        self.page_lines = 0

    def _emit(self, asa, text):
        self.lines.append(asa + text)
        if asa == '1':
            self.page_lines = 1
        elif asa == '0':
            self.page_lines += 2
        else:
            self.page_lines += 1

    def new_page(self, text=''):  self._emit('1', text)
    def single(self, text=''):    self._emit(' ', text)
    def double(self, text=''):    self._emit('0', text)

    def save(self):
        with open(self.filepath, 'w', encoding='utf-8') as f:
            f.write('\n'.join(self.lines) + '\n')


# ============================================================================
# REPORT RENDERING
# ============================================================================

_REMMTH_ORDER = [
    '>  0-1 MTH', '>  1-2 MTHS', '>  2-3 MTHS', '>  3-4 MTHS',
    '>  4-5 MTHS', '>  5-6 MTHS', '>  6-7 MTHS', '>  7-8 MTHS',
    '>  8-9 MTHS', '>  9-10 MTHS', '> 10-11 MTHS', '> 11-12 MTHS',
    '> 12-13 MTHS', '> 13-14 MTHS', '> 14-15 MTHS', '> 15-16 MTHS',
    '> 16-17 MTHS', '> 17-18 MTHS', '> 18-19 MTHS', '> 19-20 MTHS',
    '> 20-21 MTHS', '> 21-22 MTHS', '> 22-23 MTHS', '> 23-24 MTHS',
    '>2-3 YRS    ', '>3-4 YRS    ', '>4-5 YRS    ', '>5 YRS      ',
    '     TOTAL',
]

_SUBTYP_LABEL_ORDER = [
    (5,   'PRINCIPAL'),
    (5.5, 'WAREMM(MTH)'),
    (11,  'INSTALMENT '),
    (12,  'REPRICING  '),
    (13,  'NO-REPRICE'),
    (6,   'UNEARN INT'),
    (7,   'ACCRUED INT'),
    (8,   'FEE AMOUNT'),
    (9,   'NPL'),
]


def fmt_comma12(val):
    if val is None:
        return ' ' * 12
    return f"{round(val or 0):>12,}"


def fmt_4_2(val):
    if val is None:
        return '    '
    v = val or 0.0
    return f"{v:>4.2f}"


def _write_report_block(writer, data, group_col, title_str):
    """
    Renders one PROC TABULATE block:
    Rows: <group_col> * SUBTYP
    Cols: REMMTH1 * (AMOUNT, WAYLD)
    """
    # Build pivot: key=(group_val, subtyp) → {remmth1: (amount, wayld)}
    pivot = {}
    for r in data:
        gv  = r.get(group_col, '')
        st  = r.get('subtyp')
        rm1 = r.get('remmth1', '')
        key = (gv, st)
        if key not in pivot:
            pivot[key] = {}
        pivot[key][rm1] = (r.get('amount', 0.0) or 0.0,
                           r.get('wayld', 0.0) or 0.0)

    # Collect all REMMTH1 labels present, preserve order
    all_rm1 = []
    seen_rm1 = set()
    for lbl in _REMMTH_ORDER:
        for key in pivot:
            if lbl in pivot[key] and lbl not in seen_rm1:
                all_rm1.append(lbl)
                seen_rm1.add(lbl)

    if not all_rm1:
        return

    # Header
    writer.new_page(title_str)
    sep = '-' * min(PAGE_WIDTH, 40 + len(all_rm1) * 18)
    writer.single(sep)

    # Column header: REMMTH1 labels
    hdr = f"{'LOANS AND ADVANCES':<40}"
    for lbl in all_rm1:
        hdr += f"{'BALANCE O/S (RM)':>12}{'W.A. YIELD':>6}"
    writer.single(hdr)

    sub_hdr = ' ' * 40
    for lbl in all_rm1:
        sub_hdr += f"{lbl[:12]:>12}{'':>6}"
    writer.single(sub_hdr)
    writer.single(sep)

    # Group rows
    current_group = None
    for gv, st in sorted(pivot.keys(),
                         key=lambda k: (k[0] or '', _subtyp_sort(k[1]))):
        if gv != current_group:
            writer.double(f"  {gv}")
            current_group = gv

        st_lbl = subtypf(st)
        line = f"    {st_lbl:<36}"
        for lbl in all_rm1:
            amt, wld = pivot.get((gv, st), {}).get(lbl, (0.0, 0.0))
            line += f"{fmt_comma12(amt)}{fmt_4_2(wld):>6}"
        writer.single(line)

    writer.single(sep)


def _subtyp_sort(st):
    order = {5: 0, 5.5: 1, 11: 2, 12: 3, 13: 4, 6: 5, 7: 6, 8: 7, 9: 8}
    return order.get(st, 99)


# ============================================================================
# MAIN
# ============================================================================

def main():
    # ---- REPTDATE ----
    (reptdate, nowk, nowk1, reptmon, reptmon1,
     reptyear, reptday, rdate, sdate_str, btype) = get_reptdate()

    title1 = 'PUBLIC ISLAMIC BANK BERHAD'
    title2 = f'REPRICING GAP AS AT {rdate}'
    title3 = 'RISK MANAGEMENT REPORT : EIIMRM04'

    # ---- BUILD START ----
    start_rows = build_start(reptdate, reptmon, nowk)

    sv = ['amount', 'yield']

    # ============================================================
    # DETAILED SECTION: CLASS PRODTYP PRODUCT NTINDEX SUBTYP REMMTH1
    # ============================================================
    fix_start = [r for r in start_rows if r.get('inttype') == 'FIX']
    blr_start = [r for r in start_rows if r.get('inttype') == 'BLR']

    grp_det = ['prodtyp', 'product', 'ntindex', 'subtyp', 'remmth1']

    fix_sum = add_wayld(summarise(fix_start, grp_det, sv))
    blr_sum = add_wayld(summarise(blr_start, grp_det, sv))

    fix2 = build_subtotals(fix_sum, ['prodtyp', 'product', 'ntindex', 'subtyp'], sv)
    blr2 = build_subtotals(blr_sum, ['prodtyp', 'product', 'ntindex', 'subtyp'], sv)

    fix3 = build_grandtotal(fix2, ['grandtot', 'subtyp', 'remmth1'], sv, 'prodtyp')
    blr3 = build_grandtotal(blr2, ['grandtot', 'subtyp', 'remmth1'], sv, 'prodtyp')

    # ============================================================
    # SUMMARY SECTION: CLASS PRODBIG SUBTYP REMMTH1
    # ============================================================
    grp_sum = ['prodbig', 'subtyp', 'remmth1']

    fix_s   = add_wayld(summarise(fix_start, grp_sum, sv))
    blr_s   = add_wayld(summarise(blr_start, grp_sum, sv))

    fix2s   = build_subtotals(fix_s, ['prodbig', 'subtyp'], sv)
    blr2s   = build_subtotals(blr_s, ['prodbig', 'subtyp'], sv)

    fix3s   = build_grandtotal(fix2s, ['grandtot', 'subtyp', 'remmth1'], sv, 'prodbig')
    blr3s   = build_grandtotal(blr2s, ['grandtot', 'subtyp', 'remmth1'], sv, 'prodbig')

    # ============================================================
    # WRITE REPORTS
    # ============================================================
    writer = ReportWriter(OUTPUT_FILE)

    # Report 1: Detailed Fixed Rate
    _write_report_block(
        writer, fix3, 'prodtyp',
        f"{title1}  |  {title2}  |  {title3}  |  RM DENOMINATION (FIXED RATE)"
    )

    # Report 2: Detailed BLR
    _write_report_block(
        writer, blr3, 'prodtyp',
        f"{title1}  |  {title2}  |  {title3}  |  RM DENOMINATION (BLR)"
    )

    # Report 3: Summary Fixed Rate
    _write_report_block(
        writer, fix3s, 'prodbig',
        f"{title1}  |  {title2}  |  {title3}  |  RM DENOMINATION (FIXED RATE) SUMMARY"
    )

    # Report 4: Summary BLR
    _write_report_block(
        writer, blr3s, 'prodbig',
        f"{title1}  |  {title2}  |  {title3}  |  RM DENOMINATION (BLR) SUMMARY"
    )

    writer.save()
    print(f"Report written to {OUTPUT_FILE}")


if __name__ == '__main__':
    main()
