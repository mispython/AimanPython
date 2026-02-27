#!/usr/bin/env python3
"""
PROGRAM : EIIMRLFM
DATE    : 05.07.13
REPORT  : FISS - NEW LIQUIDITY FRAMEWORK (PIBB)
MODIFY  : 13.08.04 (SMR-A520)
"""

# OPTIONS YEARCUTOFF=1950  (handled via Python date parsing with 2-digit year offset)

# %INC PGM(PBBLNFMT,PBBELF,PBBDPFMT) — dependency modules imported below
# from PBBLNFMT import format_lnprod  (LIQPFMT equivalent)
# from PBBELF import ...
# from PBBDPFMT import ...

import sys
import os
import math
import duckdb
import polars as pl
from datetime import date, timedelta
from calendar import monthrange
from pathlib import Path

# NOTE: This program follows EIBMRLFM structure, but logic is adapted to EIIMRLFM
# (notably FCY list and exclusion of DCI/VOSTRO blocks that are not present in EIIMRLFM).

# ===========================================================================
# PATH CONFIGURATION
# ===========================================================================
DATA_DIR   = Path("data")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input parquet paths (dynamic, resolved after macro vars are set)
REPTDATE_PARQUET = DATA_DIR / "REPTDATE.parquet"

# Output file paths
FISS_OUTPUT  = OUTPUT_DIR / "FISS.txt"
NSRS_OUTPUT  = OUTPUT_DIR / "NSRS.txt"
FD11_OUTPUT  = OUTPUT_DIR / "FD11TEXT.txt"
FD12_OUTPUT  = OUTPUT_DIR / "FD12TEXT.txt"

# LCR intermediate outputs
LCR_FD_PARQUET     = DATA_DIR / "LCR_FD.parquet"
LCR_SA_PARQUET     = DATA_DIR / "LCR_SA.parquet"
LCR_CA_PARQUET     = DATA_DIR / "LCR_CA.parquet"
LCR_FCYCA_PARQUET  = DATA_DIR / "LCR_FCYCA.parquet"
LCR_NID_PARQUET    = DATA_DIR / "LCR_NID.parquet"
LCR_K1TBL_PARQUET  = DATA_DIR / "LCR_K1TBL.parquet"
LCR_K3TBL_PARQUET  = DATA_DIR / "LCR_K3TBL.parquet"

# ===========================================================================
# FCY PRODUCT LIST
# %LET FCY=(800,801,802,803,...,814)
# ===========================================================================
FCY_PRODUCTS = {
    800, 801, 802, 803, 804, 805, 806, 851, 852, 853, 854, 855, 856, 857, 858,
    859, 860, 807, 808, 809, 810, 811, 812, 813, 814
}

# ===========================================================================
# FORMAT: REMFMT — remaining maturity buckets
# ===========================================================================
def fmt_remfmt(remmth: float) -> str:
    """
    REMFMT:
      LOW-0.1 = '01'   UP TO 1 WK
      0.1-1   = '02'   >1 WK - 1 MTH
      1-3     = '03'   >1 MTH - 3 MTHS
      3-6     = '04'   >3 - 6 MTHS
      6-12    = '05'   >6 MTHS - 1 YR
      OTHER   = '06'   > 1 YEAR
    SAS inclusive upper bound, exclusive lower bound (LOW-0.1 = up to and including 0.1)
    """
    if remmth <= 0.1:
        return '01'
    elif remmth <= 1:
        return '02'
    elif remmth <= 3:
        return '03'
    elif remmth <= 6:
        return '04'
    elif remmth <= 12:
        return '05'
    else:
        return '06'

# ===========================================================================
# FORMAT: GLPROD — product code to GL code mapping
# ===========================================================================
_GLPROD_MAP = {
    117: '3301',  # M&I --> WALKER
    110: '3302', 108: '3303', 118: '3304', 157: '3305', 102: '3305',
    101: '3306', 121: '3307',
    194: '3308', 195: '3308', 155: '3308', 192: '3308', 137: '3308',
    154: '3308', 119: '3308', 120: '3308', 138: '3308', 193: '3308',
    116: '3309', 114: '3311', 85: '3311', 86: '3311',
    87: '3313', 88: '3313', 89: '3313', 91: '3313', 179: '3313',
    174: '3313', 175: '3313', 100: '3313', 156: '3313', 198: '3313',
    90: '3313', 93: '3313', 180: '3313', 197: '3313',
    123: '3314', 176: '3314', 196: '3314',
    112: '3315', 115: '3316', 111: '3317',
    113: '3318', 135: '3318', 189: '3318', 177: '3318', 190: '3318', 178: '3318',
    122: '3319', 109: '3320',
    165: '3322', 124: '3322', 191: '3322',
    159: '3323', 125: '3323',
    150: '3324', 181: '3324',
    151: '3325', 152: '3326', 170: '3327', 153: '3328',
    182: '3330', 183: '3330', 160: '3330', 166: '3330', 167: '3330',
    168: '3330', 169: '3330',
    161: '3331', 162: '3332', 164: '3334',
    106: '7101', 158: '7101',
    50: 'C001', 51: 'C002', 55: 'C006', 56: 'C007',
    65: 'C008', 57: 'C008', 58: 'C009',
    60: 'CI01',
    64: 'CI06', 66: 'CI06', 67: 'CI06', 68: 'CI06', 69: 'CI06',
    70: 'CI06', 71: 'CI06', 77: 'CI06', 78: 'CI06', 81: 'CI06',
    82: 'CI06', 83: 'CI06', 84: 'CI06', 94: 'CI06', 95: 'CI06',
    96: 'CI06', 97: 'CI06', 131: 'CI06', 132: 'CI06', 133: 'CI06',
    134: 'CI06', 184: 'CI06', 40: 'CI06', 41: 'CI06', 35: 'CI06',
    36: 'CI06', 37: 'CI06', 38: 'CI06', 39: 'CI06', 42: 'CI06',
    43: 'CI06', 26: 'CI06', 27: 'CI06', 3: 'CI06', 4: 'CI06',
    9: 'CI06', 10: 'CI06', 11: 'CI06', 12: 'CI06',
    53: 'HDA0', 63: 'HDA0', 103: 'HDA0', 163: 'HDA0',  # EXCLUDE FROM RDAL
}

def fmt_glprod(product: int) -> str:
    return _GLPROD_MAP.get(product, 'C999')

# ===========================================================================
# LIQPFMT — liquidity product format (from PBBLNFMT dependency)
# Maps product code to loan type category (HL, FL, RC, etc.)
# ===========================================================================
# Dependency: PBBLNFMT.format_lnprod provides the LNPROD format.
# LIQPFMT maps product to a liquidity category used to pick ITEM codes.
# Based on context, HL = housing loan, FL = finance lease, RC = revolving credit
_LIQPFMT_MAP = {
    # Housing loans
    **{k: 'HL' for k in [110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
                          139, 140, 141, 142, 147, 173, 445, 446] +
       list(range(200, 249)) + list(range(250, 261)) +
       [400, 409, 410, 412, 413, 414, 415, 423, 431, 432, 433, 440, 466,
        472, 473, 474, 479, 484, 486, 489, 494, 600, 638, 650, 651, 664, 677, 911]},
    # Revolving credit
    **{k: 'RC' for k in [146, 184, 190, 192, 195, 196, 302, 350, 351, 364, 365,
                          506, 495, 604, 605, 634, 641, 660, 685, 689,
                          802, 803, 806, 808, 810, 812, 814, 817, 818,
                          856, 857, 858, 859, 860, 902, 903, 910, 917, 925, 951]},
    # Finance lease (FL) — Personal loans and others default
}

def fmt_liqpfmt(product: int) -> str:
    """LIQPFMT format: returns HL, RC, FL, or other loan category."""
    return _LIQPFMT_MAP.get(product, 'FL')

# ===========================================================================
# DDCUSTCD format (from PBBDPFMT dependency)
# Maps numeric customer code to 2-char string code
# ===========================================================================
_DDCUSTCD_MAP = {
    77: '77', 78: '78', 95: '95', 96: '96',
    1: '01', 2: '02', 3: '03', 4: '04', 5: '05', 6: '06',
    10: '11', 11: '11', 12: '12', 13: '13', 15: '79', 17: '17',
    20: '30', 30: '30', 32: '32', 33: '33', 34: '34', 35: '35',
    36: '04', 37: '37', 38: '38', 39: '39', 40: '40',
    60: '62', 61: '61', 62: '62', 63: '63', 64: '64',
    70: '71', 71: '71', 72: '72', 73: '73', 74: '74', 75: '75',
    76: '78', 80: '86', 81: '86', 85: '86', 86: '86',
}

def fmt_ddcustcd(custcode) -> str:
    if custcode is None:
        return '79'
    try:
        c = int(custcode)
    except (ValueError, TypeError):
        return '79'
    return _DDCUSTCD_MAP.get(c, '79')

# ===========================================================================
# FDPROD format (from PBBDPFMT dependency)
# ===========================================================================
# Dependency: PBBDPFMT.FDProductFormat.format
# Placeholder — maps FD intplan to BNM code for DCI/FD use
def fmt_fdprod(intplan: int) -> str:
    # 42630 maps to FCY FD product
    _map = {
        # Simplified: actual mapping in PBBDPFMT
        # Products that map to '42630' (FCY FD)
    }
    # For '42630' check based on intplan: if FDPROD returns '42630'
    # we treat as FCY FD. Placeholder using PBBDPFMT reference:
    # from PBBDPFMT import FDProductFormat; return FDProductFormat.format(intplan)
    return _map.get(intplan, '42130')

def is_fdprod_42630(intplan: int) -> bool:
    """Check if intplan maps to FCY FD code '42630'."""
    # from PBBDPFMT import FDProductFormat
    # return FDProductFormat.format(intplan) == '42630'
    # Placeholder: FCY FD intplans — to be confirmed via PBBDPFMT
    FCY_FD_INTPLANS = {420, 421, 422, 423, 424, 425, 426, 427, 428, 429, 430,
                       431, 432, 433, 434, 435, 436, 437, 439, 440, 441, 442,
                       443, 444, 446, 470, 471, 472, 473, 474, 475, 476, 477,
                       478, 479, 480, 481, 482, 483, 484, 485, 486, 487, 488,
                       489, 490, 491, 492, 493, 494, 495, 496, 497, 498, 499}
    return intplan in FCY_FD_INTPLANS

def is_fdprod_42132(intplan: int) -> bool:
    """Check if intplan maps to '42132' (NID)."""
    # from PBBDPFMT import FDProductFormat
    # return FDProductFormat.format(intplan) == '42132'
    return False  # Placeholder

# ===========================================================================
# MACRO %DCLVAR — day arrays for months (31 days default, adjust Feb/Apr/Jun etc.)
# ===========================================================================
# RETAIN D1-D12 31; D4=D6=D9=D11=30; D2=28 (leap: 29)
# RD and MD arrays have the same initialisation.

def _make_lday(yr: int):
    """Returns dict of month -> days in month for given year."""
    return {m: monthrange(yr, m)[1] for m in range(1, 13)}

# ===========================================================================
# MACRO %NXTBLDT — calculate next billing date
# ===========================================================================
def nxt_bldate(bldate: date, payfreq: str, freq: int, payday,
               lday: dict) -> date:
    """Python equivalent of %NXTBLDT macro."""
    if payfreq == '6':
        dd = bldate.day + 14
        mm = bldate.month
        yy = bldate.year
        if dd > lday[mm]:
            dd -= lday[mm]
            mm += 1
            if mm > 12:
                mm -= 12
                yy += 1
    else:
        mm = bldate.month + freq
        yy = bldate.year
        if mm > 12:
            mm -= 12
            yy += 1
        if payday is not None:
            if payday == 99:
                dd = monthrange(yy, mm)[1]
            else:
                dd = payday
        else:
            dd = bldate.day
    # Recompute lday for new month/year (handles Feb leap)
    max_dd = monthrange(yy, mm)[1]
    if dd > max_dd:
        dd = max_dd
    return date(yy, mm, dd)

# ===========================================================================
# MACRO %REMMTH — calculate remaining months
# ===========================================================================
def calc_remmth(matdt: date, reptdate: date) -> tuple:
    """
    Python equivalent of %REMMTH macro.
    Returns (remmth, rem30d).
    """
    rpyr  = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day
    rpdays = _make_lday(rpyr)

    mdyr  = matdt.year
    mdmth = matdt.month
    mdday = matdt.day

    # If MDDAY > RPDAYS(RPMTH) then MDDAY = RPDAYS(RPMTH)
    if mdday > rpdays[rpmth]:
        mdday = rpdays[rpmth]

    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday
    remmth = remy * 12 + remm + remd / rpdays[rpmth]
    rem30d = (matdt - reptdate).days / 30.0
    return remmth, rem30d

# ===========================================================================
# LOAD REPTDATE
# ===========================================================================
def load_reptdate() -> dict:
    """Load REPTDATE from parquet and extract macro variables."""
    con = duckdb.connect()
    row = con.execute(
        f"SELECT * FROM read_parquet('{REPTDATE_PARQUET}') LIMIT 1"
    ).fetchone()
    cols = [d[0] for d in con.description]
    con.close()
    rec = dict(zip(cols, row))

    reptdate_val = rec['REPTDATE']
    if not isinstance(reptdate_val, date):
        import pandas as pd
        reptdate_val = pd.Timestamp(reptdate_val).date()

    d = reptdate_val.day
    if d == 8:
        nowk = '1'
    elif d == 15:
        nowk = '2'
    elif d == 22:
        nowk = '3'
    else:
        nowk = '4'

    return {
        'REPTDATE': reptdate_val,
        'NOWK':     nowk,
        'REPTYEAR': str(reptdate_val.year),
        'REPTYEA2': str(reptdate_val.year)[-2:],
        'REPTMON':  str(reptdate_val.month).zfill(2),
        'REPTDAY':  str(reptdate_val.day).zfill(2),
        'RDATE':    reptdate_val.strftime('%d/%m/%Y'),
        'TDATE':    reptdate_val,
    }

# ===========================================================================
# PROCESS LOANS (NOTE)
# LOANS - FL/HL USE REPAYMENT DATE; OD/RC USE EXPIRY DATE
# ===========================================================================
def process_loans(macro: dict) -> pl.DataFrame:
    """Process NOTE (loan) records to produce BNMCODE rows."""
    reptdate  = macro['REPTDATE']
    reptmon   = macro['REPTMON']
    nowk      = macro['NOWK']
    reptyea2  = macro['REPTYEA2']
    tdate     = macro['TDATE']

    loan_parquet   = DATA_DIR / f"LOAN{reptmon}{nowk}.parquet"
    lncomm_parquet = DATA_DIR / "LNCOMM.parquet"
    pay_parquet    = DATA_DIR / f"LNPAY{reptmon}{nowk}{reptyea2}.parquet"
    provsub_txt    = DATA_DIR / "PROVSUB.txt"

    con = duckdb.connect()

    # -------------------------------------------------------------------
    # PROC SORT DATA=BNM1.LOAN WHERE PRODCD IN ('34190','34690')
    # MERGE with LNCOMM for expiry date
    # -------------------------------------------------------------------
    rcloan_sql = f"""
        SELECT l.*
        FROM read_parquet('{loan_parquet}') l
        WHERE l.PRODCD IN ('34190','34690')
    """
    rcloan = con.execute(rcloan_sql).pl()

    lncomm = con.execute(
        f"SELECT ACCTNO, COMMNO, EXPIREDT FROM read_parquet('{lncomm_parquet}')"
    ).pl()

    # Convert EXPIREDT: PUT(EXPIREDT, Z11.) -> substr(1,8) -> MMDDYY8.
    # EXPIREDT is numeric 11-digit field; first 8 chars = MMDDYYYY
    lncomm = lncomm.with_columns([
        pl.col('EXPIREDT').cast(pl.Utf8).str.zfill(11)
        .str.slice(0, 8)
        .str.strptime(pl.Date, format='%m%d%Y', strict=False)
        .alias('EXPRDATE')
    ]).drop('EXPIREDT')

    # Dedup lncomm by ACCTNO, COMMNO (NODUPKEYS)
    lncomm = lncomm.unique(subset=['ACCTNO', 'COMMNO'], keep='first')

    # MERGE LNCOMM + RCLOAN to produce RCNOTE
    rcnote = lncomm.join(
        rcloan.select(['ACCTNO', 'COMMNO']).unique(subset=['ACCTNO', 'COMMNO']),
        on=['ACCTNO', 'COMMNO'], how='inner'
    ).select(['ACCTNO', 'COMMNO', 'EXPRDATE'])

    # Full loan file
    loan_all = con.execute(f"SELECT * FROM read_parquet('{loan_parquet}')").pl()

    # Dedup RCNOTE by ACCTNO, NOTENO (NODUPKEYS)
    # RCNOTE has NOTENO — but RCNOTE was built from LNCOMM which has COMMNO
    # The SAS uses NOTENO for the merge below; treat COMMNO=NOTENO here
    rcnote = rcnote.rename({'COMMNO': 'NOTENO'})
    rcnote = rcnote.unique(subset=['ACCTNO', 'NOTENO'], keep='first')

    # -------------------------------------------------------------------
    # PROVSUB: read impaired loan file
    # -------------------------------------------------------------------
    provsub_rows = []
    if provsub_txt.exists():
        with open(provsub_txt, 'r') as f:
            lines = f.readlines()[1:]  # FIRSTOBS=2
        for line in lines:
            if len(line) >= 18:
                try:
                    acctno = int(line[0:10].strip())
                    noteno = int(line[11:16].strip())
                    imloan = line[17:18].strip()
                    if imloan == 'Y':
                        provsub_rows.append({'ACCTNO': acctno, 'NOTENO': noteno, 'IMLOAN': imloan})
                except ValueError:
                    continue
    if provsub_rows:
        provsub = pl.DataFrame(provsub_rows)
        provsub = provsub.unique(subset=['ACCTNO', 'NOTENO'], keep='first')
    else:
        provsub = pl.DataFrame({'ACCTNO': pl.Series([], dtype=pl.Int64),
                                'NOTENO': pl.Series([], dtype=pl.Int64),
                                'IMLOAN': pl.Series([], dtype=pl.Utf8)})

    # -------------------------------------------------------------------
    # MERGE LOAN + RCNOTE + PROVSUB -> NOTE
    # -------------------------------------------------------------------
    note = loan_all.join(rcnote, on=['ACCTNO', 'NOTENO'], how='left')
    note = note.join(provsub, on=['ACCTNO', 'NOTENO'], how='left')

    # -------------------------------------------------------------------
    # PAY file: LNPAY
    # -------------------------------------------------------------------
    pay_raw = con.execute(f"SELECT * FROM read_parquet('{pay_parquet}')").pl()

    tdate_int = (tdate - date(1960, 1, 1)).days  # SAS date integer ref

    pay_raw = pay_raw.with_columns([
        pl.when(pl.col('EFFDATE') <= pl.lit(tdate))
          .then(pl.lit(1))
          .otherwise(pl.lit(0))
          .alias('SORT_IND'),
        pl.when(pl.col('EFFDATE') <= pl.lit(tdate))
          .then(pl.col('EFFDATE').cast(pl.Int64))
          .otherwise(-pl.col('EFFDATE').cast(pl.Int64))
          .alias('MANI_EFFDATE'),
    ])

    # PROC SORT BY ACCTNO NOTENO PAYAMT DESCENDING SORT_IND DESCENDING MANI_EFFDATE
    pay_sorted = pay_raw.sort(
        ['ACCTNO', 'NOTENO', 'PAYAMT', 'SORT_IND', 'MANI_EFFDATE'],
        descending=[False, False, False, True, True]
    )
    # NODUPKEY BY ACCTNO NOTENO PAYAMT
    pay = pay_sorted.unique(subset=['ACCTNO', 'NOTENO', 'PAYAMT'], keep='first')

    # PROC SORT NOTE BY ACCTNO NOTENO PAYAMT
    note = note.sort(['ACCTNO', 'NOTENO', 'PAYAMT'])
    pay  = pay.sort(['ACCTNO', 'NOTENO', 'PAYAMT'])

    # MERGE NOTE + PAY
    pay_keep = pay.select(['ACCTNO', 'NOTENO', 'PAYAMT', 'PAYDAY', 'DAY_DIFF', 'SORT_IND', 'MANI_EFFDATE'])
    note = note.join(pay_keep, on=['ACCTNO', 'NOTENO', 'PAYAMT'], how='left')

    # IF PRODUCT IN (800:899) THEN PAYAMT = PAYAMT * FORATE
    note = note.with_columns([
        pl.when((pl.col('PRODUCT') >= 800) & (pl.col('PRODUCT') <= 899))
          .then(pl.col('PAYAMT') * pl.col('FORATE'))
          .otherwise(pl.col('PAYAMT'))
          .alias('PAYAMT')
    ])

    con.close()

    # -------------------------------------------------------------------
    # Process rows to generate BNMCODE records
    # -------------------------------------------------------------------
    output_rows = []
    lday = _make_lday(reptdate.year)
    rpyr  = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day
    if rpyr % 4 == 0:
        lday[2] = 29

    for row in note.iter_rows(named=True):
        # IF PAIDIND NOT IN ('P','C') OR EIR_ADJ NE .
        paidind  = (row.get('PAIDIND') or '').strip()
        eir_adj  = row.get('EIR_ADJ')
        if paidind in ('P', 'C') and (eir_adj is None or eir_adj == ''):
            continue

        amtusd = amtsgd = amthkd = amtaud = 0.0

        prodcd  = str(row.get('PRODCD') or '').strip()
        product = row.get('PRODUCT') or 0

        # IF SUBSTR(PRODCD,1,2) = '34' OR PRODUCT IN (225,226)
        if not (prodcd[:2] == '34' or product in (225, 226)):
            continue

        custcd_raw = str(row.get('CUSTCD') or '').strip()
        if custcd_raw in ('77', '78', '95', '96'):
            cust = '08'
        else:
            cust = '09'

        acctype  = str(row.get('ACCTYPE') or '').strip()
        balance  = row.get('BALANCE') or 0.0
        bldate   = row.get('BLDATE')
        exprdate = row.get('EXPRDATE')
        issdte   = row.get('ISSDTE')
        payfreq  = str(row.get('PAYFREQ') or '').strip()
        payamt   = row.get('PAYAMT') or 0.0
        payday   = row.get('PAYDAY')
        loanstat = row.get('LOANSTAT')
        imloan   = str(row.get('IMLOAN') or '').strip()
        ccy      = str(row.get('CCY') or '').strip()
        days_val = row.get('DAYS')

        # DAYS = REPTDATE - BLDATE
        if bldate and bldate > date(1960, 1, 1):
            days_calc = (reptdate - bldate).days
        else:
            days_calc = days_val or 0

        # ACCTYPE = 'OD'
        if acctype == 'OD':
            remmth_val = 0.1
            amount = balance
            bnmcode = '95213' + cust + fmt_remfmt(remmth_val) + '0000Y'
            _append_note(output_rows, bnmcode, amount, amtusd, amtsgd, amthkd, amtaud)
            continue

        # IF ACCTYPE = 'LN'
        if acctype != 'LN':
            continue

        prod = fmt_liqpfmt(product)
        if custcd_raw in ('77', '78', '95', '96'):
            if prod == 'HL':
                item = '214'
            else:
                item = '219'
        else:
            if prod in ('FL', 'HL'):
                item = '211'
            elif prod == 'RC':
                item = '212'
            else:
                item = '219'

        # Convert PAYFREQ to FREQ months
        freq = 0
        if payfreq == '1':
            freq = 1
        elif payfreq == '2':
            freq = 3
        elif payfreq == '3':
            freq = 6
        elif payfreq == '4':
            freq = 12

        # Determine expiry/maturity handling
        if exprdate is not None and (exprdate - reptdate).days < 8:
            remmth_val = 0.1
        else:
            # Adjust bldate
            if payfreq in ('5', '9', ' ', '') or product in (350, 910, 925):
                bldate = exprdate
            elif bldate is None or (isinstance(bldate, (int, float)) and bldate <= 0) or \
                 (isinstance(bldate, date) and bldate <= date(1960, 1, 1)):
                bldate = issdte
                if bldate:
                    local_lday = _make_lday(bldate.year)
                    while bldate is not None and bldate <= reptdate:
                        bldate = nxt_bldate(bldate, payfreq, freq, payday, local_lday)

            if payamt is None or payamt < 0:
                payamt = 0.0

            if exprdate and bldate and (bldate > exprdate or balance <= payamt):
                bldate = exprdate

            # Loop while bldate <= exprdate
            if exprdate and bldate:
                local_lday = _make_lday(bldate.year if bldate else reptdate.year)
                while bldate <= exprdate:
                    matdt = bldate
                    remmth_val, rem30d = calc_remmth(matdt, reptdate)

                    if remmth_val > 12 or bldate == exprdate:
                        break
                    if remmth_val > 0.1 and (bldate - reptdate).days < 8:
                        remmth_val = 0.1

                    amount = payamt
                    balance = balance - payamt

                    # FCY currency amounts
                    amtusd = amtsgd = amthkd = amtaud = 0.0
                    if 800 <= product <= 899:
                        if ccy == 'USD': amtusd = amount
                        elif ccy == 'HKD': amthkd = amount
                        elif ccy == 'AUD': amtaud = amount
                        elif ccy == 'SGD': amtsgd = amount

                    if product not in FCY_PRODUCTS:
                        bnmcode = '95' + item + cust + fmt_remfmt(remmth_val) + '0000Y'
                        _append_note(output_rows, bnmcode, amount, amtusd, amtsgd, amthkd, amtaud)
                    if product in FCY_PRODUCTS:
                        bnmcode = '94' + item + cust + fmt_remfmt(remmth_val) + '0000Y'
                        _append_note(output_rows, bnmcode, amount, amtusd, amtsgd, amthkd, amtaud)

                    # 93/96 bucket (overdue/non-performing)
                    if days_calc > 89 or loanstat != 1 or imloan == 'Y':
                        remmth_val = 13
                    if product not in FCY_PRODUCTS:
                        bnmcode = '93' + item + cust + fmt_remfmt(remmth_val) + '0000Y'
                        _append_note(output_rows, bnmcode, amount, amtusd, amtsgd, amthkd, amtaud)
                    if product in FCY_PRODUCTS:
                        bnmcode = '96' + item + cust + fmt_remfmt(remmth_val) + '0000Y'
                        _append_note(output_rows, bnmcode, amount, amtusd, amtsgd, amthkd, amtaud)

                    # Next billing date
                    local_lday = _make_lday(bldate.year)
                    bldate = nxt_bldate(bldate, payfreq, freq, payday, local_lday)
                    if exprdate and (bldate > exprdate or balance <= amount):
                        bldate = exprdate

            # End of loop — process remaining balance
            remmth_val, _ = calc_remmth(exprdate, reptdate) if exprdate else (0.1, 0.0)

        # Final balance output
        amount = balance
        amtusd = amtsgd = amthkd = amtaud = 0.0
        if 800 <= product <= 899:
            if ccy == 'USD': amtusd = amount
            elif ccy == 'HKD': amthkd = amount
            elif ccy == 'AUD': amtaud = amount
            elif ccy == 'SGD': amtsgd = amount

        if product not in FCY_PRODUCTS:
            bnmcode = '95' + item + cust + fmt_remfmt(remmth_val) + '0000Y'
            _append_note(output_rows, bnmcode, amount, amtusd, amtsgd, amthkd, amtaud)
        if product in FCY_PRODUCTS:
            bnmcode = '94' + item + cust + fmt_remfmt(remmth_val) + '0000Y'
            _append_note(output_rows, bnmcode, amount, amtusd, amtsgd, amthkd, amtaud)

        if days_calc > 89 or loanstat != 1 or imloan == 'Y':
            remmth_val = 13
        if product not in FCY_PRODUCTS:
            bnmcode = '93' + item + cust + fmt_remfmt(remmth_val) + '0000Y'
            _append_note(output_rows, bnmcode, amount, amtusd, amtsgd, amthkd, amtaud)
        if product in FCY_PRODUCTS:
            bnmcode = '96' + item + cust + fmt_remfmt(remmth_val) + '0000Y'
            _append_note(output_rows, bnmcode, amount, amtusd, amtsgd, amthkd, amtaud)

        # EIR_ADJ
        if eir_adj is not None and eir_adj != '' and not (isinstance(eir_adj, float) and math.isnan(eir_adj)):
            amt_eir = float(eir_adj)
            bnmcode = '95' + item + cust + '060000Y'
            _append_note(output_rows, bnmcode, amt_eir, 0.0, 0.0, 0.0, 0.0)
            bnmcode = '93' + item + cust + '060000Y'
            _append_note(output_rows, bnmcode, amt_eir, 0.0, 0.0, 0.0, 0.0)

    if not output_rows:
        return pl.DataFrame({'BNMCODE': pl.Series([], dtype=pl.Utf8),
                             'AMOUNT': pl.Series([], dtype=pl.Float64),
                             'AMTUSD': pl.Series([], dtype=pl.Float64),
                             'AMTSGD': pl.Series([], dtype=pl.Float64),
                             'AMTHKD': pl.Series([], dtype=pl.Float64),
                             'AMTAUD': pl.Series([], dtype=pl.Float64)})
    return pl.DataFrame(output_rows)

def _append_note(rows: list, bnmcode: str, amount, amtusd, amtsgd, amthkd, amtaud):
    rows.append({
        'BNMCODE': bnmcode,
        'AMOUNT':  float(amount or 0.0),
        'AMTUSD':  float(amtusd or 0.0),
        'AMTSGD':  float(amtsgd or 0.0),
        'AMTHKD':  float(amthkd or 0.0),
        'AMTAUD':  float(amtaud or 0.0),
    })

# ===========================================================================
# FIXED DEPOSITS (FD)
# ===========================================================================
def process_fd(macro: dict) -> tuple:
    """Process Fixed Deposits. Returns (FD summary df, LCR_FD df)."""
    reptdate = macro['REPTDATE']
    con = duckdb.connect()
    fd_parquet = DATA_DIR / "FD.parquet"

    fd_raw = con.execute(f"SELECT * FROM read_parquet('{fd_parquet}')").pl()
    con.close()

    rpyr  = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day

    output_fd  = []
    output_lcr = []

    for row in fd_raw.iter_rows(named=True):
        accttype = row.get('ACCTTYPE') or 0
        if accttype == 397:
            continue
        curbal = row.get('CURBAL') or 0.0
        if curbal <= 0:
            continue

        custcd = row.get('CUSTCD') or 0
        if isinstance(custcd, str):
            custcd_int = int(custcd.strip()) if custcd.strip().isdigit() else 0
        else:
            custcd_int = int(custcd)

        cust = '08' if custcd_int in (77, 78, 95, 96) else '09'

        matdate = row.get('MATDATE') or 0
        matdt = None
        try:
            matdt_str = str(int(matdate)).zfill(8)
            matdt = date(int(matdt_str[:4]), int(matdt_str[4:6]), int(matdt_str[6:8]))
        except Exception:
            matdt = None

        openind = str(row.get('OPENIND') or '').strip()
        amtusd = amtsgd = amthkd = amtaud = 0.0

        if openind == 'D' or (matdt and (matdt - reptdate).days < 8):
            remmth_val = 0.1
            rem30d_val = 0.0
        elif matdt:
            remmth_val, rem30d_val = calc_remmth(matdt, reptdate)
        else:
            remmth_val = 0.1
            rem30d_val = 0.0

        intplan = row.get('INTPLAN') or 0
        curcode = str(row.get('CURCODE') or '').strip()
        branch  = row.get('BRANCH')
        acctno  = row.get('ACCTNO')
        fdhold  = row.get('FDHOLD')
        cdno    = row.get('CDNO')

        bic = '42630' if is_fdprod_42630(intplan) else (
              '42132' if is_fdprod_42132(intplan) else '42130')

        if bic == '42630':
            if curcode == 'USD': amtusd = curbal
            elif curcode == 'SGD': amtsgd = curbal
            elif curcode == 'HKD': amthkd = curbal
            elif curcode == 'AUD': amtaud = curbal
            bnmcode = '96311' + cust + fmt_remfmt(remmth_val) + '0000Y'
        else:
            if bic == '42132':
                bnmcode = '95315' + cust + fmt_remfmt(remmth_val) + '0000Y'
            else:
                bnmcode = '95311' + cust + fmt_remfmt(remmth_val) + '0000Y'

        if accttype in (315, 394):
            bnmcode = '95315' + cust + fmt_remfmt(remmth_val) + '0000Y'

        output_fd.append({
            'BNMCODE': bnmcode, 'AMOUNT': curbal,
            'AMTUSD': amtusd, 'AMTSGD': amtsgd,
            'AMTHKD': amthkd, 'AMTAUD': amtaud
        })
        output_lcr.append({
            'BNMCODE': bnmcode, 'BRANCH': branch, 'ACCTNO': acctno,
            'AMOUNT': curbal, 'CURCODE': curcode, 'CUSTCD': str(custcd_int),
            'PRODUCT': accttype, 'REMMTH': remmth_val, 'REM30D': rem30d_val,
            'FDHOLD': fdhold, 'CDNO': cdno, 'MATDT': matdt, 'INTPLAN': intplan
        })

    fd_df  = pl.DataFrame(output_fd)  if output_fd  else _empty_bnm_df()
    lcr_df = pl.DataFrame(output_lcr) if output_lcr else pl.DataFrame()
    return fd_df, lcr_df

# ===========================================================================
# SAVINGS (SA)
# ===========================================================================
def process_sa(macro: dict) -> tuple:
    """Process Savings accounts."""
    reptdate = macro['REPTDATE']
    reptmon  = macro['REPTMON']
    nowk     = macro['NOWK']

    sa_parquet = DATA_DIR / f"SAVG{reptmon}{nowk}.parquet"
    con = duckdb.connect()
    sa_raw = con.execute(f"SELECT * FROM read_parquet('{sa_parquet}')").pl()
    con.close()

    output_sa  = []
    output_lcr = []

    for row in sa_raw.iter_rows(named=True):
        custcd = str(row.get('CUSTCD') or '').strip()
        curbal = row.get('CURBAL') or 0.0
        prodcd = str(row.get('PRODCD') or '').strip()
        curcode = str(row.get('CURCODE') or '').strip()
        branch  = row.get('BRANCH')
        acctno  = row.get('ACCTNO')
        product = row.get('PRODUCT')

        cust = '08' if custcd in ('77', '78', '95', '96') else '09'
        bnmcode = '95312' + cust + '01' + '0000Y'

        # IF PRODCD NE 'N' OR CURCODE='XAU' THEN OUTPUT LCR.SA
        if prodcd != 'N' or curcode == 'XAU':
            output_lcr.append({
                'BNMCODE': bnmcode, 'BRANCH': branch, 'ACCTNO': acctno,
                'AMOUNT': curbal, 'CURCODE': curcode, 'CUSTCD': custcd,
                'PRODUCT': product, 'REMMTH': 0.1, 'REM30D': 0.0
            })
        # IF PRODCD NE 'N' THEN OUTPUT SA
        if prodcd != 'N':
            output_sa.append({
                'BNMCODE': bnmcode, 'AMOUNT': curbal,
                'AMTUSD': 0.0, 'AMTSGD': 0.0, 'AMTHKD': 0.0, 'AMTAUD': 0.0
            })

    sa_df  = pl.DataFrame(output_sa)  if output_sa  else _empty_bnm_df()
    lcr_df = pl.DataFrame(output_lcr) if output_lcr else pl.DataFrame()
    return sa_df, lcr_df

# ===========================================================================
# CURRENT ACCOUNTS (CA)
# ===========================================================================
def process_ca(macro: dict) -> tuple:
    """Process Current accounts."""
    reptmon = macro['REPTMON']
    nowk    = macro['NOWK']

    ca_parquet = DATA_DIR / f"CURN{reptmon}{nowk}.parquet"
    con = duckdb.connect()
    ca_raw = con.execute(f"SELECT * FROM read_parquet('{ca_parquet}')").pl()
    con.close()

    output_ca  = []
    output_lcr = []

    for row in ca_raw.iter_rows(named=True):
        product = row.get('PRODUCT') or 0
        glprox  = fmt_glprod(product)
        if glprox == 'C999':
            continue

        prodcd  = str(row.get('PRODCD') or '').strip()
        if prodcd[:3] not in ('421', '423'):
            continue

        custcd  = str(row.get('CUSTCD') or '').strip()
        curbal  = row.get('CURBAL') or 0.0
        curcode = str(row.get('CURCODE') or '').strip()
        branch  = row.get('BRANCH')
        acctno  = row.get('ACCTNO')
        intrate = row.get('INTRATE')
        billerind = row.get('BILLERIND')

        cust = '08' if custcd in ('77', '78', '95', '96') else '09'
        bnmcode = '95313' + cust + '01' + '0000Y'

        output_ca.append({
            'BNMCODE': bnmcode, 'AMOUNT': curbal,
            'AMTUSD': 0.0, 'AMTSGD': 0.0, 'AMTHKD': 0.0, 'AMTAUD': 0.0
        })
        output_lcr.append({
            'BNMCODE': bnmcode, 'BRANCH': branch, 'ACCTNO': acctno,
            'AMOUNT': curbal, 'CURCODE': curcode, 'CUSTCD': custcd,
            'PRODUCT': product, 'REMMTH': 0.1, 'REM30D': 0.0,
            'INTRATE': intrate, 'BILLERIND': billerind
        })

    ca_df  = pl.DataFrame(output_ca)  if output_ca  else _empty_bnm_df()
    lcr_df = pl.DataFrame(output_lcr) if output_lcr else pl.DataFrame()
    return ca_df, lcr_df

# ===========================================================================
# FCY CURRENT ACCOUNTS (FCYCA)
# ===========================================================================
def process_fcyca(macro: dict) -> tuple:
    """Process FCY Current accounts."""
    current_parquet = DATA_DIR / "CURRENT.parquet"
    con = duckdb.connect()

    # DEPOSIT.CURRENT: WHERE (400<=PRODUCT<=444) AND PRODUCT!=413
    fcyca_raw = con.execute(
        f"SELECT * FROM read_parquet('{current_parquet}') "
        f"WHERE PRODUCT BETWEEN 400 AND 444 AND PRODUCT != 413"
    ).pl()
    con.close()

    output_fcyca = []
    output_lcr   = []

    for row in fcyca_raw.iter_rows(named=True):
        custcode = row.get('CUSTCODE') or 0
        custcd   = fmt_ddcustcd(custcode)
        curbal   = row.get('CURBAL') or 0.0
        product  = row.get('PRODUCT') or 0
        curcode  = str(row.get('CURCODE') or '').strip()
        branch   = row.get('BRANCH')
        acctno   = row.get('ACCTNO')
        intrate  = row.get('INTRATE')
        billerind = row.get('BILLERIND')

        cust = '08' if custcd in ('77', '78', '95', '96') else '09'
        bnmcode = '96313' + cust + '01' + '0000Y'

        amtusd = amtsgd = amthkd = amtaud = 0.0
        if product in (400, 420, 440): amtusd = curbal
        if product in (403, 423):      amtsgd = curbal
        if product in (406, 426):      amthkd = curbal
        if product in (402, 422, 442): amtaud = curbal

        output_fcyca.append({
            'BNMCODE': bnmcode, 'AMOUNT': curbal,
            'AMTUSD': amtusd, 'AMTSGD': amtsgd,
            'AMTHKD': amthkd, 'AMTAUD': amtaud
        })
        output_lcr.append({
            'BNMCODE': bnmcode, 'BRANCH': branch, 'ACCTNO': acctno,
            'AMOUNT': curbal, 'CURCODE': curcode, 'CUSTCD': custcd,
            'PRODUCT': product, 'REMMTH': 0.1, 'REM30D': 0.0,
            'INTRATE': intrate, 'BILLERIND': billerind
        })

    fcyca_df = pl.DataFrame(output_fcyca) if output_fcyca else _empty_bnm_df()
    lcr_df   = pl.DataFrame(output_lcr)   if output_lcr   else pl.DataFrame()
    return fcyca_df, lcr_df

# ===========================================================================
# UNDRAWN PORTION (UNOTE)
# ===========================================================================
def process_unote(macro: dict) -> pl.DataFrame:
    """Process undrawn loan commitments."""
    reptdate = macro['REPTDATE']
    reptmon  = macro['REPTMON']
    nowk     = macro['NOWK']

    loan_parquet  = DATA_DIR / f"LOAN{reptmon}{nowk}.parquet"
    uloan_parquet = DATA_DIR / f"ULOAN{reptmon}{nowk}.parquet"
    lncomm_parquet = DATA_DIR / "LNCOMM.parquet"

    con = duckdb.connect()

    alw = con.execute(
        f"SELECT * FROM read_parquet('{loan_parquet}') "
        f"WHERE NOT (PRODUCT IN (151,152,181) AND ACCTYPE='OD') "
        f"AND PAIDIND NOT IN ('P','C')"
    ).pl()

    lncomm = con.execute(f"SELECT * FROM read_parquet('{lncomm_parquet}')").pl()
    lncomm = lncomm.with_columns([
        pl.col('EXPIREDT').cast(pl.Utf8).str.zfill(11)
        .str.slice(0, 8)
        .str.strptime(pl.Date, format='%m%d%Y', strict=False)
        .alias('EXPRDATE')
    ])

    # ALWCOM: COMMNO > 0
    alwcom  = alw.filter(pl.col('COMMNO') > 0).sort(['ACCTNO', 'COMMNO'])
    alwnocom = alw.filter(pl.col('COMMNO') <= 0).sort(['ACCTNO', 'COMMNO'])

    # APPR: merge ALWCOM + LNCOMM
    appr_rows = []
    lncomm_dict = {(r['ACCTNO'], r['COMMNO']): r
                   for r in lncomm.iter_rows(named=True)}
    for r in alwcom.iter_rows(named=True):
        key = (r['ACCTNO'], r['COMMNO'])
        lc  = lncomm_dict.get(key, {})
        combined = {**r, **lc}
        prodcd = str(r.get('PRODCD') or '').strip()
        if prodcd in ('34190', '34690'):
            # FIRST.ACCTNO or FIRST.COMMNO output
            appr_rows.append(combined)
        else:
            appr_rows.append(combined)

    appr = pl.DataFrame(appr_rows) if appr_rows else pl.DataFrame()

    # APPR1: alwnocom with dedup logic for RC
    alwnocom_sorted = alwnocom.sort(['ACCTNO', 'APPRLIM2'])
    appr1_rows = []
    dup_rows   = []
    seen_keys  = {}
    for r in alwnocom_sorted.iter_rows(named=True):
        prodcd = str(r.get('PRODCD') or '').strip()
        if prodcd in ('34190', '34690'):
            key = (r['ACCTNO'], r.get('APPRLIM2'))
            if key not in seen_keys:
                seen_keys[key] = True
                appr1_rows.append(r)
            else:
                dup_rows.append({**r, 'DUPLI': 1})
        # else: non-RC records handled below

    # DUPLI: WHERE BALANCE >= APPRLIM2
    dupli_set = {(r['ACCTNO'], r.get('APPRLIM2'))
                 for r in dup_rows
                 if (r.get('BALANCE') or 0) >= (r.get('APPRLIM2') or 0)}

    # Re-process alwnocom for APPR1 final
    appr1_final = []
    seen2 = {}
    for r in alwnocom_sorted.iter_rows(named=True):
        prodcd = str(r.get('PRODCD') or '').strip()
        if prodcd in ('34190', '34690'):
            key = (r['ACCTNO'], r.get('APPRLIM2'))
            if key in dupli_set:
                if (r.get('BALANCE') or 0) >= (r.get('APPRLIM2') or 0):
                    appr1_final.append(r)
            else:
                if key not in seen2:
                    seen2[key] = True
                    appr1_final.append(r)
        else:
            appr1_final.append(r)

    appr1 = pl.DataFrame(appr1_final) if appr1_final else pl.DataFrame()

    # ULOAN
    uloan = con.execute(
        f"SELECT * FROM read_parquet('{uloan_parquet}') "
        f"WHERE NOT ((ACCTNO BETWEEN 3000000000 AND 3999999999) "
        f"AND PRODUCT IN (151,152,181) AND ACCTYPE='OD')"
    ).pl()
    con.close()

    # LOAN = APPR + APPR1
    loan_combined = pl.concat([appr, appr1], how='diagonal') if (len(appr) > 0 or len(appr1) > 0) else pl.DataFrame()
    all_unote = pl.concat([loan_combined, uloan], how='diagonal') if len(loan_combined) > 0 else uloan

    rpyr  = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day

    output_rows = []
    for row in all_unote.iter_rows(named=True):
        prodcd  = str(row.get('PRODCD') or '').strip()
        product = row.get('PRODUCT') or 0

        if not (prodcd[:2] == '34' or product in (225, 226)):
            continue

        acctype   = str(row.get('ACCTYPE') or '').strip()
        exprdate  = row.get('EXPRDATE') or row.get('EXPIREDT')
        apprdate  = row.get('APPRDATE')
        undrawn   = row.get('UNDRAWN') or 0.0
        loanstat  = row.get('LOANSTAT')
        imloan    = str(row.get('IMLOAN') or '').strip()
        days_val  = row.get('DAYS') or 0
        bldate    = row.get('BLDATE')

        amtusd = amtsgd = amthkd = amtaud = 0.0
        days_calc = (reptdate - bldate).days if isinstance(bldate, date) else (days_val or 0)

        if acctype == 'LN':
            matdt = exprdate
            item  = '429'
            if prodcd in ('34190', '34690'):
                item = '424'
        else:
            matdt = (apprdate + timedelta(days=365)) if isinstance(apprdate, date) else None
            item  = '423'

        if prodcd == '34240':
            item = '429'

        if matdt is None or (matdt - reptdate).days < 8:
            remmth_val = 0.1
        else:
            remmth_val, _ = calc_remmth(matdt, reptdate)

        if product not in FCY_PRODUCTS:
            bnmcode = '95' + item + '00' + fmt_remfmt(remmth_val) + '0000Y'
            _append_note(output_rows, bnmcode, undrawn, 0.0, 0.0, 0.0, 0.0)
        elif product in FCY_PRODUCTS:
            bnmcode = '94' + item + '00' + fmt_remfmt(remmth_val) + '0000Y'
            _append_note(output_rows, bnmcode, undrawn, 0.0, 0.0, 0.0, 0.0)

        if days_calc > 89 or loanstat != 1 or imloan == 'Y':
            remmth_val = 13
        if product not in FCY_PRODUCTS:
            bnmcode = '93' + item + '00' + fmt_remfmt(remmth_val) + '0000Y'
            _append_note(output_rows, bnmcode, undrawn, 0.0, 0.0, 0.0, 0.0)
        elif product in FCY_PRODUCTS:
            bnmcode = '96' + item + '00' + fmt_remfmt(remmth_val) + '0000Y'
            _append_note(output_rows, bnmcode, undrawn, 0.0, 0.0, 0.0, 0.0)

    return pl.DataFrame(output_rows) if output_rows else _empty_bnm_df()

# ===========================================================================
# DUAL CURRENCY INVESTMENT (DCI)  (ESMR 2013-1184)
# ===========================================================================
def process_dci(macro: dict) -> tuple:
    """Process DCI and DCIW records."""
    reptdate = macro['REPTDATE']
    reptmon  = macro['REPTMON']
    nowk     = macro['NOWK']
    tdate    = macro['TDATE']

    dci_parquet  = DATA_DIR / f"DCI{reptmon}{nowk}.parquet"
    dciw_parquet = DATA_DIR / f"DCIWTB{reptmon}{nowk}.parquet"
    forate_parquet     = DATA_DIR / "FORATE.parquet"
    foratebkp_parquet  = DATA_DIR / "FORATEBKP.parquet"

    # Load FORATE for spot rates
    con = duckdb.connect()
    # %FORATE macro: check if FORATE.REPTDATE <= TDATE
    try:
        fdate_row = con.execute(
            f"SELECT REPTDATE FROM read_parquet('{forate_parquet}') LIMIT 1"
        ).fetchone()
        fdate = fdate_row[0] if fdate_row else tdate + timedelta(days=1)
        if not isinstance(fdate, date):
            import pandas as pd
            fdate = pd.Timestamp(fdate).date()
    except Exception:
        fdate = tdate + timedelta(days=1)

    if fdate <= tdate:
        fcyrt_df = con.execute(
            f"SELECT CURCODE, SPOTRATE FROM read_parquet('{forate_parquet}') "
            f"ORDER BY CURCODE"
        ).pl()
    else:
        fcyrt_df = con.execute(
            f"SELECT CURCODE, SPOTRATE FROM read_parquet('{foratebkp_parquet}') "
            f"WHERE REPTDATE <= DATE '{tdate}' "
            f"QUALIFY ROW_NUMBER() OVER (PARTITION BY CURCODE ORDER BY REPTDATE DESC) = 1"
        ).pl()

    # Build FCYRT dict
    fcyrt = {r['CURCODE'].strip(): float(r['SPOTRATE'])
             for r in fcyrt_df.iter_rows(named=True)}

    # DCI records
    dci_raw = con.execute(
        f"SELECT * FROM read_parquet('{dci_parquet}')"
    ).pl()
    con.close()

    output_dci  = []
    output_lcr  = []

    rpyr  = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day

    for row in dci_raw.iter_rows(named=True):
        matdt   = row.get('MATDT')
        startdt = row.get('STARTDT')
        if matdt is None or startdt is None:
            continue
        if not (matdt > reptdate and startdt <= reptdate):
            continue

        if (matdt - reptdate).days < 8:
            remmth_val = 0.1
        else:
            remmth_val, _ = calc_remmth(matdt, reptdate)

        invcurr  = str(row.get('INVCURR') or '').strip()
        invamt   = float(row.get('INVAMT') or 0.0)
        custcode = row.get('CUSTCODE') or 0
        custfiss = str(custcode).zfill(2)
        product  = row.get('PRODUCT')
        ticketno = row.get('TICKETNO')
        custname = row.get('CUSTNAME')
        newic    = row.get('NEWIC')

        amtusd = amtsgd = amthkd = amtaud = 0.0

        if invcurr == 'MYR':
            spotrt = 1.0
            amount = invamt
            for prefix in ('9332900', '9532900'):
                bnmcode = prefix + fmt_remfmt(remmth_val) + '0000Y'
                _append_note(output_dci, bnmcode, amount, 0.0, 0.0, 0.0, 0.0)
                output_lcr.append({
                    'BNMCODE': bnmcode, 'AMOUNT': amount, 'CURCODE': invcurr,
                    'CUSTFISS': custfiss, 'DEALTYPE': str(product or ''),
                    'DEALREF': str(ticketno or ''),
                    'REMMTH': remmth_val, 'REM30D': (matdt - reptdate).days / 30.0,
                    'CUSTNAME': custname, 'NEWIC': newic
                })
        else:
            spotrt = fcyrt.get(invcurr, 1.0)
            if invcurr == 'JPY':
                invamt = round(invamt, 0)
            else:
                invamt = round(invamt, 2)
            amount = invamt * spotrt
            if invcurr == 'USD': amtusd = amount
            if invcurr == 'SGD': amtsgd = amount
            if invcurr == 'HKD': amthkd = amount
            if invcurr == 'AUD': amtaud = amount
            for prefix in ('9432900', '9632900'):
                bnmcode = prefix + fmt_remfmt(remmth_val) + '0000Y'
                _append_note(output_dci, bnmcode, amount, amtusd, amtsgd, amthkd, amtaud)
                output_lcr.append({
                    'BNMCODE': bnmcode, 'AMOUNT': amount, 'CURCODE': invcurr,
                    'CUSTFISS': custfiss, 'DEALTYPE': str(product or ''),
                    'DEALREF': str(ticketno or ''),
                    'REMMTH': remmth_val, 'REM30D': (matdt - reptdate).days / 30.0,
                    'CUSTNAME': custname, 'NEWIC': newic
                })

    # DCIW (ESMR 2013-1446)
    con2 = duckdb.connect()
    try:
        dciw_raw = con2.execute(
            f"SELECT * FROM read_parquet('{dciw_parquet}') "
            f"WHERE DCDLP='DCI' AND DCBSI='S' AND DCTRNT='I'"
        ).pl()
        # Rename DCMTYD -> MATDT handled via column reference
        dciw_raw = dciw_raw.rename({'DCMTYD': 'MATDT'}) \
            if 'DCMTYD' in dciw_raw.columns else dciw_raw
    except Exception:
        dciw_raw = pl.DataFrame()
    con2.close()

    for row in dciw_raw.iter_rows(named=True):
        matdt = row.get('MATDT')
        if matdt is None:
            continue
        if (matdt - reptdate).days < 8:
            remmth_val = 0.1
        else:
            remmth_val, _ = calc_remmth(matdt, reptdate)

        dcbccy = str(row.get('DCBCCY') or '').strip()
        dcbamt = float(row.get('DCBAMT') or 0.0)
        c8spt  = float(row.get('C8SPT') or 1.0)

        amtusd = amtsgd = amthkd = amtaud = 0.0
        rem_str = fmt_remfmt(remmth_val)

        if dcbccy == 'MYR':
            amount = dcbamt
            for bnmcode in ('9392100' + rem_str + '0000Y',
                            '9592100' + rem_str + '0000Y',
                            '9472200' + rem_str + '0000Y',
                            '9672200' + rem_str + '0000Y'):
                _append_note(output_dci, bnmcode, amount, 0.0, 0.0, 0.0, 0.0)
        else:
            if dcbccy == 'JPY':
                dcbamt = round(dcbamt, 0)
            else:
                dcbamt = round(dcbamt, 2)
            amount = dcbamt * c8spt
            if dcbccy == 'USD': amtusd = amount
            if dcbccy == 'SGD': amtsgd = amount
            if dcbccy == 'HKD': amthkd = amount
            if dcbccy == 'AUD': amtaud = amount
            for bnmcode in ('9492200' + rem_str + '0000Y',
                            '9692200' + rem_str + '0000Y',
                            '9372100' + rem_str + '0000Y',
                            '9572100' + rem_str + '0000Y'):
                _append_note(output_dci, bnmcode, amount, amtusd, amtsgd, amthkd, amtaud)

    dci_df  = pl.DataFrame(output_dci)  if output_dci  else _empty_bnm_df()
    lcr_df  = pl.DataFrame(output_lcr)  if output_lcr  else pl.DataFrame()
    return dci_df, lcr_df

# ===========================================================================
# NID (Negotiable Instruments of Deposit)
# ===========================================================================
def process_nid(macro: dict) -> tuple:
    """Process NID records."""
    reptdate = macro['REPTDATE']
    reptday  = macro['REPTDAY']

    nid_parquet = DATA_DIR / f"RNID{reptday}.parquet"
    con = duckdb.connect()

    try:
        nid_raw = con.execute(
            f"SELECT * FROM read_parquet('{nid_parquet}') "
            f"WHERE NIDSTAT='N' AND CURBAL>0"
        ).pl()
    except Exception:
        nid_raw = pl.DataFrame()
    con.close()

    output_nid = []
    output_lcr = []

    for row in nid_raw.iter_rows(named=True):
        matdt   = row.get('MATDT')
        startdt = row.get('STARTDT')
        if matdt is None or startdt is None:
            continue
        if not (matdt > reptdate and startdt <= reptdate):
            continue

        curbal   = float(row.get('CURBAL') or 0.0)
        curcode  = str(row.get('CURCODE') or '').strip()
        custcd   = row.get('CUSTCD')
        product  = row.get('PRODUCT')
        branch   = row.get('BRANCH')
        nid_acctno = row.get('NID_ACCTNO')
        nid_cdno   = row.get('NID_CDNO')

        if (matdt - reptdate).days < 8:
            remmth_val = 0.1
        else:
            remmth_val, rem30d_val = calc_remmth(matdt, reptdate)
            _ = rem30d_val

        rem30d_val = (matdt - reptdate).days / 30.0
        rem_str    = fmt_remfmt(remmth_val)

        for bnmcode in ('9384000' + rem_str + '0000Y',
                        '9584000' + rem_str + '0000Y'):
            _append_note(output_nid, bnmcode, curbal, 0.0, 0.0, 0.0, 0.0)
            output_lcr.append({
                'BNMCODE': bnmcode, 'BRANCH': branch,
                'NID_ACCTNO': nid_acctno, 'NID_CDNO': nid_cdno,
                'AMOUNT': curbal, 'CURCODE': curcode,
                'CUSTCD': str(custcd or ''), 'PRODUCT': product,
                'REMMTH': remmth_val, 'REM30D': rem30d_val
            })

    nid_df  = pl.DataFrame(output_nid) if output_nid else _empty_bnm_df()
    lcr_df  = pl.DataFrame(output_lcr) if output_lcr else pl.DataFrame()
    return nid_df, lcr_df

# ===========================================================================
# HELPER: empty BNM dataframe
# ===========================================================================
def _empty_bnm_df() -> pl.DataFrame:
    return pl.DataFrame({
        'BNMCODE': pl.Series([], dtype=pl.Utf8),
        'AMOUNT':  pl.Series([], dtype=pl.Float64),
        'AMTUSD':  pl.Series([], dtype=pl.Float64),
        'AMTSGD':  pl.Series([], dtype=pl.Float64),
        'AMTHKD':  pl.Series([], dtype=pl.Float64),
        'AMTAUD':  pl.Series([], dtype=pl.Float64),
    })

# ===========================================================================
# SUMMARISE (PROC SUMMARY NWAY by BNMCODE)
# ===========================================================================
def summarise_bnm(df: pl.DataFrame) -> pl.DataFrame:
    if len(df) == 0:
        return df
    return (
        df.group_by('BNMCODE')
        .agg([
            pl.col('AMOUNT').sum(),
            pl.col('AMTUSD').sum(),
            pl.col('AMTSGD').sum(),
            pl.col('AMTHKD').sum(),
            pl.col('AMTAUD').sum(),
        ])
    )

# ===========================================================================
# KAPITI ITEMS — %INC PGM(KALMLIQ) and %INC PGM(KALMLIFE)
# These are separate programs included here; results loaded from their outputs
# ===========================================================================
def load_kapiti_items(macro: dict) -> tuple:
    """
    Load KTBL and K1TBL results produced by KALMLIQ and KALMLIFE programs.
    %INC PGM(KALMLIQ);
    %INC PGM(KALMLIFE);
    These programs produce KTBLALL and K1TBL outputs consumed here.
    """
    # Dependency: KALMLIQ.py and KALMLIFE.py must be run first.
    # Their outputs are loaded from the data directory.
    # LCR.K1TBL and LCR.K3TBL are written by KALMLIQ/KALMLIFE.

    ktblall_path = DATA_DIR / "KTBLALL.parquet"
    k1tbl_path   = DATA_DIR / "K1TBL.parquet"

    ktbl_rows = []
    suppl_rows = []

    con = duckdb.connect()

    try:
        # DATA LCR.K1TBL(RENAME=...) LCR.K3TBL(RENAME=...): SET KTBLALL
        # IF TBL='1' THEN OUTPUT LCR.K1TBL; ELSE IF TBL='3' THEN OUTPUT LCR.K3TBL
        ktblall = con.execute(f"SELECT * FROM read_parquet('{ktblall_path}')").pl()
        lcr_k1tbl = ktblall.filter(pl.col('TBL') == '1')
        lcr_k3tbl = ktblall.filter(pl.col('TBL') == '3')

        # Rename columns per SAS DATA step
        if 'GWCCY' in lcr_k1tbl.columns:
            lcr_k1tbl = lcr_k1tbl.rename({
                'GWCCY': 'CURCODE', 'GWDLP': 'DEALTYPE',
                'GWDLR': 'DEALREF', 'GWC2R': 'CUSTFISS'
            })
        if 'UTCCY' in lcr_k3tbl.columns:
            lcr_k3tbl = lcr_k3tbl.rename({
                'UTCCY': 'CURCODE', 'UTSTY': 'DEALTYPE',
                'UTDLR': 'DEALREF', 'UTCUS': 'CUSTNO'
            })

        # Drop day-array columns
        drop_cols = [c for c in lcr_k1tbl.columns
                     if c.startswith('D') and c[1:].isdigit()]
        lcr_k1tbl = lcr_k1tbl.drop([c for c in drop_cols if c in lcr_k1tbl.columns])
        lcr_k1tbl.write_parquet(str(LCR_K1TBL_PARQUET))
        lcr_k3tbl.write_parquet(str(LCR_K3TBL_PARQUET))

        # KTBL for BNM FISS output — extract BNMCODE/AMOUNT columns
        for row in ktblall.iter_rows(named=True):
            bnmcode = str(row.get('BNMCODE') or '').strip()
            amount  = float(row.get('AMOUNT') or 0.0)
            amtusd  = float(row.get('AMTUSD') or 0.0)
            amtsgd  = float(row.get('AMTSGD') or 0.0)
            amthkd  = float(row.get('AMTHKD') or 0.0)
            # AMTAUD=0 per DATA KTBL; SET KTBL; AMTAUD=0
            ktbl_rows.append({
                'BNMCODE': bnmcode, 'AMOUNT': amount,
                'AMTUSD': amtusd, 'AMTSGD': amtsgd,
                'AMTHKD': amthkd, 'AMTAUD': 0.0
            })

        # SUPPL: SET K1TBL; WHERE ABS(AMOUNT) >= 5000000
        k1tbl = con.execute(f"SELECT * FROM read_parquet('{k1tbl_path}')").pl()
        for row in k1tbl.iter_rows(named=True):
            if abs(row.get('AMOUNT') or 0.0) >= 5000000:
                suppl_rows.append(row)

    except Exception as e:
        print(f"Warning: Could not load KAPITI items: {e}", file=sys.stderr)

    con.close()

    ktbl_df   = pl.DataFrame(ktbl_rows)  if ktbl_rows  else _empty_bnm_df()
    suppl_df  = pl.DataFrame(suppl_rows) if suppl_rows else pl.DataFrame()
    return ktbl_df, suppl_df

# ===========================================================================
# WRITE FISS OUTPUT
# ===========================================================================
def write_fiss(note_final: pl.DataFrame, macro: dict, outpath: Path,
               divide_by_1000: bool = True):
    """
    DATA _NULL_; SET NOTE; FILE FISS/NSRS;
    Writes BNM FISS format: RLFM header + BNMCODE;AMOUNT;AMTUSD;AMTSGD;AMTHKD;AMTAUD
    """
    reptday  = macro['REPTDAY']
    reptmon  = macro['REPTMON']
    reptyear = macro['REPTYEAR']

    with open(outpath, 'w') as f:
        # Header line
        f.write(f"RLFM{reptday}{reptmon}{reptyear}\n")

        for row in note_final.iter_rows(named=True):
            bnmcode = str(row['BNMCODE'] or '').ljust(14)[:14]
            def fmt_amt(v):
                v = v or 0.0
                if divide_by_1000:
                    v = abs(round(v / 1000))
                else:
                    v = abs(round(v))
                return str(int(v)) if v == int(v) else str(v)

            amount = fmt_amt(row['AMOUNT'])
            amtusd = fmt_amt(row['AMTUSD'] if row['AMTUSD'] is not None else 0.0)
            amtsgd = fmt_amt(row['AMTSGD'] if row['AMTSGD'] is not None else 0.0)
            amthkd = fmt_amt(row['AMTHKD'] if row['AMTHKD'] is not None else 0.0)
            amtaud = fmt_amt(row['AMTAUD'] if row['AMTAUD'] is not None else 0.0)

            f.write(f"{bnmcode};{amount};{amtusd};{amtsgd};{amthkd};{amtaud}\n")

# ===========================================================================
# REPORT: TOP 100 DEPOSITORS (PART 3)
# ===========================================================================
PAGE_LENGTH = 60

def write_top100_report(title: str, data_rows: list, outpath: Path):
    """
    PROC PRINT with ASA carriage control characters.
    Reports top 100 depositors with DEPOSITOR, TOTAL BALANCE, FD BALANCE, CA BALANCE.
    """
    col_widths = {'CUSTNAME': 30, 'CURBAL': 18, 'FDBAL': 18, 'CABAL': 18}
    header1 = title
    header2 = f"{'NAME OF DEPOSITOR':<30}  {'DEPOSITOR':<30}  {'TOTAL BALANCE':>18}  {'FD BALANCE':>18}  {'CA BALANCE':>18}"
    sep_line = '-' * (30 + 2 + 30 + 2 + 18 + 2 + 18 + 2 + 18)

    lines = []
    # ASA '1' = new page
    lines.append('1' + header1)
    lines.append(' ' + header2)
    lines.append(' ' + sep_line)

    line_count = 3
    for row in data_rows:
        if line_count >= PAGE_LENGTH - 2:
            lines.append('1' + header1)
            lines.append(' ' + header2)
            lines.append(' ' + sep_line)
            line_count = 3

        custname = str(row.get('CUSTNAME') or '').ljust(30)[:30]
        curbal   = f"{float(row.get('CURBAL') or 0.0):>18,.2f}"
        fdbal    = f"{float(row.get('FDBAL')  or 0.0):>18,.2f}"
        cabal    = f"{float(row.get('CABAL')  or 0.0):>18,.2f}"
        line = f"{custname}  {curbal}  {fdbal}  {cabal}"
        lines.append(' ' + line)
        line_count += 1

    # Summary total
    total_curbal = sum(float(r.get('CURBAL') or 0.0) for r in data_rows)
    lines.append(' ' + sep_line)
    lines.append(' ' + f"{'TOTAL':<30}  {total_curbal:>18,.2f}")

    with open(outpath, 'w') as f:
        f.write('\n'.join(lines) + '\n')

def process_top100(macro: dict, suppl_df: pl.DataFrame):
    """
    NEW SMR-A520: Top 100 FD+CA individual and corporate customers.
    """
    reptdate = macro['REPTDATE']
    rdate    = macro['RDATE']

    cisca_parquet = DATA_DIR / "CISLN_DEPOSIT.parquet"
    cisfd_parquet = DATA_DIR / "CISDP_DEPOSIT.parquet"
    current_parquet = DATA_DIR / "CURRENT.parquet"
    fd_parquet_dep  = DATA_DIR / "FD_DEPOSIT.parquet"

    con = duckdb.connect()

    try:
        cisca = con.execute(
            f"SELECT CUSTNO, ACCTNO, CUSTNAME, NEWIC, OLDIC, INDORG, "
            f"CASE WHEN NEWIC IS NOT NULL AND NEWIC != '' THEN NEWIC ELSE OLDIC END AS ICNO "
            f"FROM read_parquet('{cisca_parquet}') "
            f"WHERE ACCTNO BETWEEN 3000000000 AND 3999999999"
        ).pl()
    except Exception:
        cisca = pl.DataFrame()

    try:
        cisfd = con.execute(
            f"SELECT CUSTNO, ACCTNO, CUSTNAME, NEWIC, OLDIC, INDORG, "
            f"CASE WHEN NEWIC IS NOT NULL AND NEWIC != '' THEN NEWIC ELSE OLDIC END AS ICNO "
            f"FROM read_parquet('{cisfd_parquet}') "
            f"WHERE (ACCTNO BETWEEN 1000000000 AND 1999999999) "
            f"OR (ACCTNO BETWEEN 7000000000 AND 7999999999)"
        ).pl()
    except Exception:
        cisfd = pl.DataFrame()

    try:
        ca_data = con.execute(
            f"SELECT ACCTNO, CURBAL, PRODUCT, PURPOSE, CUSTCODE "
            f"FROM read_parquet('{current_parquet}') WHERE CURBAL > 0"
        ).pl()
    except Exception:
        ca_data = pl.DataFrame()

    try:
        fd_data = con.execute(
            f"SELECT ACCTNO, CURBAL, PRODUCT, PURPOSE "
            f"FROM read_parquet('{fd_parquet_dep}') WHERE CURBAL > 0"
        ).pl()
    except Exception:
        fd_data = pl.DataFrame()

    con.close()

    if len(ca_data) == 0 or len(cisca) == 0:
        caind = pl.DataFrame()
        caorg = pl.DataFrame()
    else:
        ca_merged = ca_data.join(cisca.select(['ACCTNO', 'CUSTNAME', 'ICNO', 'NEWIC',
                                               'OLDIC', 'INDORG', 'CUSTNO', 'CUSTCODE']),
                                 on='ACCTNO', how='left')
        # Filter: PURPOSE != '2' AND PRODUCT NOT IN (400..411)
        excl_products = set(range(400, 412))
        ca_filtered = ca_merged.filter(
            (pl.col('PURPOSE') != '2') &
            (~pl.col('PRODUCT').is_in(list(excl_products)))
        ).with_columns(pl.col('CURBAL').alias('CABAL'))

        caind = ca_filtered.filter(pl.col('CUSTCODE').cast(pl.Int64, strict=False).is_in([77, 78, 95, 96]))
        caorg = ca_filtered.filter(
            (~pl.col('CUSTCODE').cast(pl.Int64, strict=False).is_in([77, 78, 95, 96])) &
            (pl.col('INDORG') == 'O')
        )

    if len(fd_data) == 0 or len(cisfd) == 0:
        fdind = pl.DataFrame()
        fdorg = pl.DataFrame()
    else:
        fd_merged = fd_data.join(cisfd.select(['ACCTNO', 'CUSTNAME', 'ICNO', 'NEWIC',
                                               'OLDIC', 'INDORG', 'CUSTNO']),
                                 on='ACCTNO', how='left')
        excl_fd = set(range(350, 358))
        fd_filtered = fd_merged.filter(
            (pl.col('PURPOSE') != '2') &
            (~pl.col('PRODUCT').is_in(list(excl_fd)))
        ).with_columns(pl.col('CURBAL').alias('FDBAL'))

        # Simulate CUSTCODE from fd_data — use INDORG split
        fdind = fd_filtered.filter(pl.col('INDORG').is_null() | (pl.col('INDORG') != 'O'))
        fdorg = fd_filtered.filter(pl.col('INDORG') == 'O')

    def build_top100(fd_part, ca_part, is_corporate: bool) -> list:
        """Merge FD+CA, summarise by ICNO+CUSTNAME, top 100 by CURBAL."""
        rows_all = []

        def safe_select(df, cols):
            return df.select([c for c in cols if c in df.columns]) if len(df) > 0 else pl.DataFrame()

        for r in safe_select(fd_part, ['ACCTNO', 'ICNO', 'CUSTNAME', 'FDBAL', 'CURBAL',
                                        'NEWIC', 'OLDIC', 'CUSTNO']).iter_rows(named=True):
            if not r.get('ICNO') or str(r.get('ICNO')).strip() == '':
                r = {**r, 'ICNO': 'XX'}
            rows_all.append({**r, 'CABAL': 0.0})

        for r in safe_select(ca_part, ['ACCTNO', 'ICNO', 'CUSTNAME', 'CABAL', 'CURBAL',
                                        'NEWIC', 'OLDIC', 'CUSTNO']).iter_rows(named=True):
            if not r.get('ICNO') or str(r.get('ICNO')).strip() == '':
                r = {**r, 'ICNO': 'XX'}
            rows_all.append({**r, 'FDBAL': 0.0})

        if is_corporate:
            # Exclude specific ACCTNO ranges
            rows_all = [r for r in rows_all
                        if not (1590000000 <= (r.get('ACCTNO') or 0) <= 1599999999 or
                                1689999999 <= (r.get('ACCTNO') or 0) <= 1699999999 or
                                1789999999 <= (r.get('ACCTNO') or 0) <= 1799999999)]

        if not rows_all:
            return []

        # Group by ICNO, CUSTNAME; SUM CURBAL, FDBAL, CABAL
        summary: dict = {}
        for r in rows_all:
            key = (r.get('ICNO', ''), r.get('CUSTNAME', ''))
            if key not in summary:
                summary[key] = {'ICNO': r.get('ICNO'), 'CUSTNAME': r.get('CUSTNAME'),
                                 'CURBAL': 0.0, 'FDBAL': 0.0, 'CABAL': 0.0}
            summary[key]['CURBAL'] += float(r.get('CURBAL') or 0.0)
            summary[key]['FDBAL']  += float(r.get('FDBAL')  or 0.0)
            summary[key]['CABAL']  += float(r.get('CABAL')  or 0.0)

        sorted_rows = sorted(summary.values(), key=lambda x: x['CURBAL'], reverse=True)
        return sorted_rows[:100]

    # *** FD+CA INDIVIDUAL CUSTOMERS ***
    ind_top100 = build_top100(fdind, caind, is_corporate=False)
    write_top100_report(
        f"TOP 100 LARGEST FD+CA INDIVIDUAL CUSTOMERS AS AT {rdate}",
        ind_top100,
        FD11_OUTPUT
    )

    # *** FD+CA CORPORATE CUSTOMERS ***
    corp_top100 = build_top100(fdorg, caorg, is_corporate=True)
    write_top100_report(
        f"TOP 100 LARGEST FD+CA CORPORATE CUSTOMERS AS AT {rdate}",
        corp_top100,
        FD12_OUTPUT
    )

# ===========================================================================
# MAIN
# ===========================================================================
def main():
    # -----------------------------------------------------------------------
    # GET REPTDATE macro variables
    # -----------------------------------------------------------------------
    macro = load_reptdate()
    print(f"Report date: {macro['RDATE']} | NOWK={macro['NOWK']}")

    # -----------------------------------------------------------------------
    # BREAKDOWN BY MATURITY PROFILE (PART 1 & 2 - RM)
    # -----------------------------------------------------------------------
    print("Processing loans (NOTE)...")
    note_df = process_loans(macro)
    note_sum = summarise_bnm(note_df)

    print("Processing fixed deposits (FD)...")
    fd_df, lcr_fd = process_fd(macro)
    fd_sum = summarise_bnm(fd_df)
    if len(lcr_fd) > 0:
        lcr_fd.write_parquet(str(LCR_FD_PARQUET))

    print("Processing savings accounts (SA)...")
    sa_df, lcr_sa = process_sa(macro)
    sa_sum = summarise_bnm(sa_df)
    if len(lcr_sa) > 0:
        lcr_sa.write_parquet(str(LCR_SA_PARQUET))

    print("Processing current accounts (CA)...")
    ca_df, lcr_ca = process_ca(macro)
    ca_sum = summarise_bnm(ca_df)
    if len(lcr_ca) > 0:
        lcr_ca.write_parquet(str(LCR_CA_PARQUET))

    print("Processing FCY current accounts (FCYCA)...")
    fcyca_df, lcr_fcyca = process_fcyca(macro)
    fcyca_sum = summarise_bnm(fcyca_df)
    if len(lcr_fcyca) > 0:
        lcr_fcyca.write_parquet(str(LCR_FCYCA_PARQUET))

    print("Processing undrawn commitments (UNOTE)...")
    unote_df = process_unote(macro)
    unote_sum = summarise_bnm(unote_df)

    # EIIMRLFM variant does not include DCI block from EIBMRLFM.
    print("Processing NID...")
    nid_df, lcr_nid = process_nid(macro)
    nid_sum = summarise_bnm(nid_df)
    if len(lcr_nid) > 0:
        lcr_nid.write_parquet(str(LCR_NID_PARQUET))

    # -----------------------------------------------------------------------
    # SUMMARISE AND CONSOLIDATE
    # DATA NOTE; SET NOTE FD SA CA UNOTE FCYCA NID;
    # -----------------------------------------------------------------------
    all_parts = [df for df in [note_sum, fd_sum, sa_sum, ca_sum,
                                unote_sum, fcyca_sum, nid_sum]
                 if len(df) > 0]
    if all_parts:
        note_combined = pl.concat(all_parts, how='diagonal')
    else:
        note_combined = _empty_bnm_df()

    # -----------------------------------------------------------------------
    # TO REPLACE BY NEW REPORT SMR-A520
    # (PART 3 distribution profile — replaced by top 50 depositor report below)
    # -----------------------------------------------------------------------

    # -----------------------------------------------------------------------
    # KAPITI ITEMS FOR PART 2 & 3
    # %LET INST = 'PBB';
    # %INC PGM(KALMLIQ);
    # %INC PGM(KALMLIFE);
    # -----------------------------------------------------------------------
    INST = 'PBB'  # noqa: F841  (used by KALMLIQ/KALMLIFE dependencies)
    print("Loading KAPITI items...")
    ktbl_df, suppl_df = load_kapiti_items(macro)

    # DATA KTBL; SET KTBL; AMTAUD=0;
    # (already set AMTAUD=0 in load_kapiti_items)

    # DATA NOTE; SET NOTE KTBL;
    ktbl_parts = [note_combined]
    if len(ktbl_df) > 0:
        ktbl_parts.append(ktbl_df)
    note_with_ktbl = pl.concat(ktbl_parts, how='diagonal')

    # PROC SUMMARY FINAL
    note_final = summarise_bnm(note_with_ktbl)

    # -----------------------------------------------------------------------
    # OUTPUT DATA IN BNM FISS FORMAT
    # -----------------------------------------------------------------------
    print(f"Writing FISS output to {FISS_OUTPUT}...")
    write_fiss(note_final, macro, FISS_OUTPUT, divide_by_1000=True)

    print(f"Writing NSRS output to {NSRS_OUTPUT}...")
    write_fiss(note_final, macro, NSRS_OUTPUT, divide_by_1000=False)

    # -----------------------------------------------------------------------
    # PRODUCE REPORTS — PROC TABULATE (PART 3)
    # OPTIONS NOCENTER NODATE NONUMBER MISSING=0;
    # TITLE1 'PUBLIC BANK BERHAD';
    # TITLE2 'NEW LIQUIDITY FRAMEWORK AS AT' &RDATE;
    #
    # /* TO REPLACE BY NEW REPORT SMR-A520
    # PROC TABULATE DATA=SUPPL ...
    # TITLE4 'CUSTOMER DEPOSITS >= 1% OF TOTAL (PART 3)';
    # SMR-A520 */
    # -----------------------------------------------------------------------

    # -----------------------------------------------------------------------
    # PRODUCE REPORTS TOP 50 (100) DEPOSITOR  /* NEW SMR-A520 */
    # -----------------------------------------------------------------------
    print("Producing top 100 depositor reports...")
    process_top100(macro, suppl_df)

    print("Done.")
    print(f"  FISS output : {FISS_OUTPUT}")
    print(f"  NSRS output : {NSRS_OUTPUT}")
    print(f"  FD11 report : {FD11_OUTPUT}")
    print(f"  FD12 report : {FD12_OUTPUT}")

if __name__ == '__main__':
    main()
