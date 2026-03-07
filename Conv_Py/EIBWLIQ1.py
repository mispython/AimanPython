#!/usr/bin/env python3
"""
Program  : EIBWLIQ1.py
Date     : 25.05.98
Report   : NEW LIQUIDITY FRAMEWORK
           (TO RUN ON 22ND OF THE MONTH)

Purpose  : Generates New Liquidity Framework report covering:
           - Part 1 & 2 RM: Loans (FL/HL use repayment date, OD/RC use expiry date)
           - Fixed Deposits, Savings, Current Accounts, FCY Current Accounts
           - Undrawn portions of loan facilities
           - KAPITI items for Part 2 & 3 (via KALMLIQS / KALMLIQ1)
           - Distribution profile of customer deposits (Part 3)
           Output: ASA carriage-control text reports (60 lines/page)
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import sys
import math
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Optional

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
import duckdb
import polars as pl

# ============================================================================
# DEPENDENCY IMPORTS
# ============================================================================
# From PBBLNFMT: loan product / denomination format functions
from PBBLNFMT import (
    format_lnprod,          # PUT(PRODUCT, LNPROD.) -> prodcd string
    FCY_PRODUCTS,           # &FCY list equivalent
)

# From PBBDPFMT: deposit format functions
from PBBDPFMT import (
    FDProductFormat,        # PUT(INTPLAN, FDPROD.)  -> BIC string
    ProductLists,           # CURX_PRODUCTS set
)

# From PBBELF: EL/branch/customer-type format functions
from PBBELF import (
    format_ctype,           # PUT(GWCTP, $CTYPE.)
)

# From KALMLIQS: K1TBL and K3TBL builders
from KALMLIQS import build_k1tbl, build_k3tbl

# NOTE: The original SAS has %INC PGM(KALMLIQ1) here, which generates K3TBL3
#         (MGS/RRS repos). However, K3TBL3 is never referenced anywhere in EIBWLIQ1 —
#         DATA KTBL only sets from K1TBL and K3TBL (both from KALMLIQS). The include
#         is therefore a dead reference in the original program and is not called here.
# From KALMLIQ1: K3TBL3 builder
# from KALMLIQ1 import build_k3tbl3

# ============================================================================
# PATH CONFIGURATION  –  adjust as needed for your environment
# ============================================================================
BASE_DIR    = Path(".")
BNM_DIR     = BASE_DIR / "data" / "bnm"       # BNM library
BNM1_DIR    = BASE_DIR / "data" / "bnm1"      # BNM1 library  (LOAN, ULOAN)
FD_DIR      = BASE_DIR / "data" / "fd"        # FD library
LOAN_DIR    = BASE_DIR / "data" / "loan"      # LOAN library  (LNCOMM)
DEPOSIT_DIR = BASE_DIR / "data" / "deposit"   # DEPOSIT library (CURRENT)
BNMK_DIR    = BASE_DIR / "data" / "bnmk"      # BNMK library  (K1TBL, K3TBL)
OUTPUT_DIR  = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# GLOBAL CONSTANTS
# ============================================================================
YEARCUTOFF = 1950
PAGE_LINES  = 60          # ASA report page length

# OPTIONS YEARCUTOFF=1950
# (applied implicitly when parsing 2-digit years)

# %LET FCY = (800,801,...,814)
FCY_SET = {
    800, 801, 802, 803, 804, 805, 806, 851, 852, 853, 854, 855, 856, 857, 858,
    859, 860, 807, 808, 809, 810, 811, 812, 813, 814,
}

# %LET IREP / NREP
IREP = {'01', '02', '11', '12', '81'}
NREP = {'13', '17', '20', '60', '71', '72', '73', '74', '76', '79', '85'}

# %LET INST
INST = 'PBB'

# Current account products used in CURX filter  (from PBBDPFMT ProductLists)
CURX_PRODUCTS = ProductLists.CURX_PRODUCTS

# FCY current-account products
FCYCX_PROD = {400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410,
              440, 441, 442, 443, 444}

# ============================================================================
# PROC FORMAT: REMFMT  (REMMTH -> maturity bucket label)
# ============================================================================
def remfmt(remmth: Optional[float]) -> str:
    """
    VALUE REMFMT
       LOW-0.255 = 'UP TO 1 WK'
       0.255-1   = '>1 WK - 1 MTH'
       1-3       = '>1 MTH - 3 MTHS'
       3-6       = '>3 - 6 MTHS'
       6-12      = '>6 MTHS - 1 YR'
       OTHER     = '> 1 YEAR';
    """
    if remmth is None:
        return '> 1 YEAR'
    if remmth <= 0.255:
        return 'UP TO 1 WK'
    elif remmth <= 1:
        return '>1 WK - 1 MTH'
    elif remmth <= 3:
        return '>1 MTH - 3 MTHS'
    elif remmth <= 6:
        return '>3 - 6 MTHS'
    elif remmth <= 12:
        return '>6 MTHS - 1 YR'
    else:
        return '> 1 YEAR'

REMFMT_ORDER = [
    'UP TO 1 WK', '>1 WK - 1 MTH', '>1 MTH - 3 MTHS',
    '>3 - 6 MTHS', '>6 MTHS - 1 YR', '> 1 YEAR',
]

# ============================================================================
# PROC FORMAT: $ITEMF  (ITEM code -> description)
# ============================================================================
ITEMF = {
    'A1.01':  'A1.01  LOANS: CORP - FIXED TERM LOANS',
    'A1.02':  'A1.02  LOANS: CORP - REVOLVING LOANS',
    'A1.03':  'A1.03  LOANS: CORP - OVERDRAFTS',
    'A1.04':  'A1.04  LOANS: CORP - OTHERS',
    'A1.04A': 'A1.04A LOANS: CORP - SHARE FINANCING',
    'A1.05':  'A1.05  LOANS: IND  - HOUSING LOANS',
    'A1.07':  'A1.07  LOANS: IND  - OVERDRAFTS',
    'A1.08':  'A1.08  LOANS: IND  - OTHERS',
    'A1.08A': 'A1.08A LOANS: IND  - REVOLVING LOANS',
    'A1.08B': 'A1.08B LOANS: IND  - SHARE FINANCING',
    'A1.12A': 'A1.12A DEPOSITS: CORP - GID',
    'A1.12':  'A1.12  DEPOSITS: CORP - FIXED',
    'A1.13':  'A1.13  DEPOSITS: CORP - SAVINGS',
    'A1.14':  'A1.14  DEPOSITS: CORP - CURRENT',
    'A1.15':  'A1.15  DEPOSITS: IND  - FIXED',
    'A1.15A': 'A1.15A DEPOSITS: IND  - GID',
    'A1.16':  'A1.16  DEPOSITS: IND  - SAVINGS',
    'A1.17':  'A1.17  DEPOSITS: IND  - CURRENT',
    'A1.20':  'A1.20  SHAREHOLDER FUND & OTHER LIABILITIES',
    'A1.25':  'A1.25  UNDRAWN OD FACILITIES GIVEN',
    'A1.26':  'A1.26  UNDRAWN PORTION FOR RC FACILITIES',
    'A1.28':  'A1.28  UNDRAWN PORTION OF OTHER C/F GIVEN',
    'A1.28A': 'A1.28A UNDRAWN MARGIN FINANCING          ',
    'A2.01':  'A2.01  INTERBANK LENDING/DEPOSITS',
    'A2.02':  'A2.02  REVERSE REPO',
    'A2.03':  'A2.03  DEBT SEC: GOVT PP/BNM BILLS/CAG',
    'A2.04':  'A2.04  DECT SEC: FIN INST PAPERS',
    'A2.05':  'A2.05  DEBT SEC: TRADE PAPERS',
    'A2.06':  'A2.06  CORP DEBT: GOVT-GUARANTEED',
    'A2.08':  'A2.08  CORP DEBT: NON-GUARANTEED',
    'A2.09':  'A2.09  FX EXCHG CONTRACTS RECEIVABLE',
    'A2.14':  'A2.14  INTERBANK BORROWINGS/DEPOSITS',
    'A2.15':  'A2.15  INTERBANK REPOS',
    'A2.16':  'A2.16  NON-INTERBANK REPOS',
    'A2.17':  'A2.17  NIDS ISSUED',
    'A2.18':  'A2.18  BAS PAYABLE',
    'A2.19':  'A2.19  FX EXCHG CONTRACTS PAYABLE',
    'B1.01':  'B1.01  LOANS: NON-INDIVIDUALS - FIXED TERM LOANS',
    'B1.02':  'B1.02  LOANS: NON-INDIVIDUALS - REVOLVING LOANS',
    'B1.12':  'B1.12  DEPOSITS: CORP - FIXED (ALL CCY)',
    'B1.13':  'B1.13  DEPOSITS: CORP - CURRENT (ALL CCY)',
    'B1.15':  'B1.15  DEPOSITS: IND  - FIXED (ALL CCY)',
    'B1.16':  'B1.16  DEPOSITS: IND  - CURRENT (ALL CCY)',
    'B1.17':  'B1.17  DEPOSITS: CORP - FIXED ($USD)',
    'B1.18':  'B1.18  DEPOSITS: IND  - FIXED ($USD)',
    'B1.19':  'B1.19  DEPOSITS: CORP - FIXED ($SGD)',
    'B1.20':  'B1.20  DEPOSITS: IND  - FIXED ($SGD)',
    'B1.21':  'B1.21  DEPOSITS: CORP - CURRENT ($USD)',
    'B1.22':  'B1.22  DEPOSITS: IND  - CURRENT ($USD)',
    'B1.23':  'B1.23  DEPOSITS: CORP - CURRENT ($SGD)',
    'B1.24':  'B1.24  DEPOSITS: IND  - CURRENT ($SGD)',
    'B1.25':  'B1.25  DEPOSITS: CORP - FIXED ($HKD)',
    'B1.26':  'B1.26  DEPOSITS: IND  - FIXED ($HKD)',
    'B1.27':  'B1.27  DEPOSITS: CORP - CURRENT ($HKD)',
    'B1.28':  'B1.28  DEPOSITS: IND  - CURRENT ($HKD)',
    'B1.29':  'B1.29  DEPOSITS: CORP - FIXED   ($AUD)',
    'B1.30':  'B1.30  DEPOSITS: IND  - FIXED   ($AUD)',
    'B1.31':  'B1.31  DEPOSITS: CORP - CURRENT ($AUD)',
    'B1.32':  'B1.32  DEPOSITS: IND  - CURRENT ($AUD)',
    'B2.01':  'B2.01  INTERBANK LENDING/DEPOSITS',
    'B2.09':  'B2.09  FX EXCHG CONTRACTS RECEIVABLE',
    'B2.14':  'B2.14  INTERBANK BORROWINGS/DEPOSITS',
    'B2.19':  'B2.19  FX EXCHG CONTRACTS PAYABLE',
    'B2.26':  'B2.26  UNDRAWN PORTION FOR RC FACILITIES',
    'B2.28':  'B2.28  UNDRAWN PORTION OF OTHER C/F GIVEN',
}

# ============================================================================
# HELPER: LIQPFMT — maps product to loan type label (FL, HL, RC, OD, etc.)
# Used as PUT(PRODUCT, LIQPFMT.)
# ============================================================================
def liqpfmt(product: int) -> str:
    """Map product code to liquidity product type (FL/HL/RC/OD/PL/...)"""
    prodcd = format_lnprod(product)
    if prodcd == '34120':
        # Housing loans
        return 'HL'
    elif prodcd == '34190' or prodcd == '34690':
        return 'RC'
    elif prodcd == '34111':
        return 'HP'
    return 'FL'

# ============================================================================
# HELPER: days_in_month
# ============================================================================
def days_in_month(year: int, month: int) -> int:
    """Return the number of days in a given month/year (leap-year aware)."""
    if month == 2:
        return 29 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 28
    elif month in (4, 6, 9, 11):
        return 30
    else:
        return 31

# ============================================================================
# MACRO %REMMTH — compute remaining months from matdt to reptdate
# ============================================================================
def calc_remmth(matdt: date, reptdate: date) -> float:
    """
    Replicate %REMMTH macro:
      REMY  = MDYR  - RPYR
      REMM  = MDMTH - RPMTH
      REMD  = MDDAY - RPDAY   (capped to rpdays(rpmth))
      REMMTH= REMY*12 + REMM + REMD/RPDAYS(RPMTH)
    """
    rpyr  = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day
    rp_days_in_mth = days_in_month(rpyr, rpmth)

    mdyr  = matdt.year
    mdmth = matdt.month
    mdday = matdt.day

    # IF MDDAY > RPDAYS(RPMTH) THEN MDDAY = RPDAYS(RPMTH)
    if mdday > rp_days_in_mth:
        mdday = rp_days_in_mth

    remy = mdyr  - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday
    return remy * 12 + remm + remd / rp_days_in_mth

# ============================================================================
# MACRO %NXTBLDT — compute next billing date
# ============================================================================
def nxtbldt(bldate: date, issdte: date, payfreq: str, freq: int) -> date:
    """
    Replicate %NXTBLDT macro:
    PAYFREQ='6' (bi-weekly) -> add 14 days
    Otherwise              -> advance by FREQ months from ISSDTE day
    """
    if payfreq == '6':
        dd = bldate.day + 14
        mm = bldate.month
        yy = bldate.year
        dim = days_in_month(yy, mm)
        if dd > dim:
            dd -= dim
            mm += 1
            if mm > 12:
                mm -= 12
                yy += 1
        dim2 = days_in_month(yy, mm)
        if dd > dim2:
            dd = dim2
        return date(yy, mm, dd)
    else:
        dd = issdte.day
        mm = bldate.month + freq
        yy = bldate.year
        if mm > 12:
            mm -= 12
            yy += 1
        dim = days_in_month(yy, mm)
        if dd > dim:
            dd = dim
        return date(yy, mm, dd)

# ============================================================================
# REPTDATE STEP — read reptdate and derive macro variables
# ============================================================================
def get_reptdate(bnm_dir: Path):
    """
    DATA REPTDATE; SET BNM.REPTDATE;
    Returns (reptdate, nowk, reptyear, reptmon, reptday, rdate)
    """
    reptdate_file = bnm_dir / "reptdate.parquet"
    con = duckdb.connect()
    df  = con.execute(f"SELECT REPTDATE FROM read_parquet('{reptdate_file}') LIMIT 1").pl()
    raw = df["REPTDATE"][0]
    if isinstance(raw, (date, datetime)):
        reptdate = raw.date() if isinstance(raw, datetime) else raw
    else:
        reptdate = date.fromisoformat(str(raw))

    day = reptdate.day
    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'

    reptyear = str(reptdate.year)
    reptmon  = str(reptdate.month).zfill(2)
    reptday  = str(reptdate.day).zfill(2)
    rdate    = reptdate.strftime('%d/%m/%y')   # DDMMYY8. approximation
    return reptdate, nowk, reptyear, reptmon, reptday, rdate

# ============================================================================
# DATA NOTE — Loans breakdown by maturity profile
# ============================================================================
def build_note(bnm1_dir: Path, reptmon: str, nowk: str, reptdate: date) -> pl.DataFrame:
    """
    PROC SORT DATA=BNM1.LOAN&REPTMON&NOWK OUT=NOTEX; BY ACCTNO;
    WHERE PAIDIND NOT IN ('P','C');
    DATA NOTE (KEEP=PART ITEM REMMTH AMOUNT);
    """
    loan_file = bnm1_dir / f"loan{reptmon}{nowk}.parquet"
    con = duckdb.connect()
    df  = con.execute(
        f"SELECT * FROM read_parquet('{loan_file}') "
        f"WHERE PAIDIND NOT IN ('P','C')"
    ).pl()

    rows = []

    for r in df.to_dicts():
        prodcd  = str(r.get("PRODCD") or "").strip()
        product = int(r.get("PRODUCT") or 0)
        acctype = str(r.get("ACCTYPE") or "").strip()
        custcd  = str(r.get("CUSTCD") or "").strip()
        balance = float(r.get("BALANCE") or 0.0)
        payamt  = float(r.get("PAYAMT") or 0.0)
        loanstat= int(r.get("LOANSTAT") or 0)

        # Parse dates
        def _to_date(val) -> Optional[date]:
            if val is None:
                return None
            if isinstance(val, (date, datetime)):
                return val.date() if isinstance(val, datetime) else val
            try:
                return date.fromisoformat(str(val)[:10])
            except Exception:
                return None

        bldate_raw  = r.get("BLDATE")
        issdte_raw  = r.get("ISSDTE")
        exprdate_raw= r.get("EXPRDATE")
        bldate   = _to_date(bldate_raw)
        issdte   = _to_date(issdte_raw)
        exprdate = _to_date(exprdate_raw)
        payfreq  = str(r.get("PAYFREQ") or "").strip()

        # IF SUBSTR(PRODCD,1,2) = '34' OR PRODUCT IN (225,226)
        if not (prodcd[:2] == '34' or product in (225, 226)):
            continue

        # Determine FREQ from PAYFREQ
        freq_map = {'1': 1, '2': 3, '3': 6, '4': 12}
        freq = freq_map.get(payfreq, 0)

        prod = liqpfmt(product)

        # ---- ACCTYPE = 'OD' ----
        if acctype == 'OD':
            if product in (151, 152, 181):
                continue
            remmth = 0.1
            amount = balance
            part   = '2-RM'
            if custcd in ('77', '78', '95', '96'):
                item = 'A1.07'
            else:
                item = 'A1.03'
            if prodcd == '34240':
                if custcd in ('77', '78', '95', '96'):
                    item = 'A1.08B'
                else:
                    item = 'A1.04A'
            rows.append({'PART': part, 'ITEM': item, 'REMMTH': remmth, 'AMOUNT': amount})
            continue

        # ---- ACCTYPE = 'LN' ----
        if acctype != 'LN':
            continue

        # Determine ITEM for LN
        if product not in FCY_SET:
            if custcd in ('77', '78', '95', '96'):
                if prod == 'HL':
                    item = 'A1.05'
                elif prod == 'RC':
                    item = 'A1.08A'
                else:
                    item = 'A1.08'
            else:
                if prod in ('FL', 'HL'):
                    item = 'A1.01'
                elif prod == 'RC':
                    item = 'A1.02'
                else:
                    item = 'A1.04'
        else:
            item = 'B1.01' if prod == 'FL' else ('B1.02' if prod == 'RC' else 'A1.04')

        # IF PRODUCT = 100 THEN ITEM = 'A1.05' (hardcode by Mazni)
        if product == 100:
            item = 'A1.05'

        # Compute days since last billing date
        days = 0
        if bldate and bldate > date(1900, 1, 1):
            days = (reptdate - bldate).days

        if exprdate is None:
            exprdate = reptdate  # fallback

        # IF EXPRDATE - REPTDATE < 8 THEN REMMTH = 0.1
        if (exprdate - reptdate).days < 8:
            remmth_final = 0.1
            amount_final = balance
            _output_ln_row(rows, product, prod, custcd, '2-RM', item,
                           remmth_final, amount_final, FCY_SET)
            if days > 89 or loanstat != 1:
                remmth_final = 13
            _output_ln_row(rows, product, prod, custcd, '1-RM', item,
                           remmth_final, amount_final, FCY_SET)
        else:
            # Determine initial bldate for iteration
            if payfreq in ('5', '9', ' ', '') or product in (350, 910, 925):
                bldate = exprdate
            elif bldate is None or (bldate <= date(1900, 1, 1)):
                bldate = issdte
                if bldate:
                    while bldate <= reptdate:
                        bldate = nxtbldt(bldate, issdte, payfreq, freq)

            if payamt < 0:
                payamt = 0

            if bldate and (bldate > exprdate or balance <= payamt):
                bldate = exprdate

            # Iteration over billing dates
            while bldate and bldate <= exprdate:
                matdt  = bldate
                remmth = calc_remmth(matdt, reptdate)

                if remmth > 12 or bldate == exprdate:
                    break

                if remmth > 0.255 and (bldate - reptdate).days < 8:
                    remmth = 0.255

                amount    = payamt
                balance  -= payamt

                _output_ln_row(rows, product, prod, custcd, '2-RM', item,
                               remmth, amount, FCY_SET)
                if days > 89 or loanstat != 1:
                    remmth = 13
                _output_ln_row(rows, product, prod, custcd, '1-RM', item,
                               remmth, amount, FCY_SET)

                bldate = nxtbldt(bldate, issdte, payfreq, freq)
                if bldate > exprdate or balance <= payamt:
                    bldate = exprdate

            # Remaining balance at expiry
            amount = balance
            remmth = calc_remmth(exprdate, reptdate) if exprdate else 0.1
            _output_ln_row(rows, product, prod, custcd, '2-RM', item,
                           remmth, amount, FCY_SET)
            if days > 89 or loanstat != 1:
                remmth = 13
            _output_ln_row(rows, product, prod, custcd, '1-RM', item,
                           remmth, amount, FCY_SET)

    if not rows:
        return pl.DataFrame(schema={'PART': pl.Utf8, 'ITEM': pl.Utf8,
                                    'REMMTH': pl.Float64, 'AMOUNT': pl.Float64})
    return pl.DataFrame(rows)


def _output_ln_row(rows, product, prod, custcd, base_part, item,
                   remmth, amount, fcy_set):
    """Helper: determine PART and ITEM for FCY vs RM loan output."""
    if product in fcy_set:
        part = base_part.replace('RM', 'FC')
        if prod == 'FL':
            item_out = 'B1.01'
        elif prod == 'RC':
            item_out = 'B1.02'
        else:
            item_out = item
    else:
        part     = base_part
        item_out = item
    rows.append({'PART': part, 'ITEM': item_out, 'REMMTH': remmth, 'AMOUNT': amount})


# ============================================================================
# DATA FD — Fixed Deposits
# ============================================================================
def build_fd(fd_dir: Path, reptdate: date) -> pl.DataFrame:
    """
    DATA FD (KEEP=BIC PART ITEM REMMTH AMOUNT);
    SET FD.FD;
    """
    fd_file = fd_dir / "fd.parquet"
    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{fd_file}')").pl()

    # USD plan groups
    USD_PLANS = {429, 470, 471, 472, 473, 474, 475, 540, 560, 751, 752, 753, 754}
    SGD_PLANS = {432, 488, 489, 490, 491, 492, 493, 543, 563}
    HKD_PLANS = {435, 546, 554, 555, 556, 557, 558, 559, 566}
    AUD_PLANS = {431, 542, 562, 482, 483, 484, 485, 486, 487, 759, 760, 761, 762}

    rows = []
    for r in df.to_dicts():
        curbal   = float(r.get("CURBAL") or 0.0)
        if curbal <= 0:
            continue

        intplan  = int(r.get("INTPLAN") or 0)
        custcd   = int(r.get("CUSTCD") or 0)
        openind  = str(r.get("OPENIND") or "").strip()
        matdate_raw = r.get("MATDATE")
        accttype = int(r.get("ACCTTYPE") or 0)

        # MATDT = INPUT(PUT(MATDATE,Z8.),YYMMDD8.)
        def _parse_matdate(val) -> Optional[date]:
            if val is None:
                return None
            if isinstance(val, (date, datetime)):
                return val.date() if isinstance(val, datetime) else val
            s = str(int(val)).zfill(8)
            try:
                return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
            except Exception:
                return None

        matdt = _parse_matdate(matdate_raw)

        # IF OPENIND = 'D' OR MATDT - REPTDATE < 8 THEN REMMTH = 0.1
        if matdt is None or openind == 'D' or (matdt - reptdate).days < 8:
            remmth = 0.1
        else:
            remmth = calc_remmth(matdt, reptdate)

        amount = curbal
        bic    = FDProductFormat.format(intplan)
        ind    = custcd in (77, 78, 95, 96)

        if bic != '42630':
            part = '2-RM'
            if bic == '42132':
                item = 'A1.15A' if ind else 'A1.12A'
            else:
                item = 'A1.15' if ind else 'A1.12'
            # IF ACCTTYPE IN (315,394)
            if accttype in (315, 394):
                item = 'A1.15' if ind else 'A1.12'
            rows.append({'BIC': bic, 'PART': part, 'ITEM': item,
                         'REMMTH': remmth, 'AMOUNT': amount})
        else:
            part = '2-F$'
            if ind:
                rows.append({'BIC': bic, 'PART': part, 'ITEM': 'B1.15',
                             'REMMTH': remmth, 'AMOUNT': amount})
                if intplan in USD_PLANS:
                    rows.append({'BIC': bic, 'PART': part, 'ITEM': 'B1.18',
                                 'REMMTH': remmth, 'AMOUNT': amount})
                if intplan in SGD_PLANS:
                    rows.append({'BIC': bic, 'PART': part, 'ITEM': 'B1.20',
                                 'REMMTH': remmth, 'AMOUNT': amount})
                if intplan in HKD_PLANS:
                    rows.append({'BIC': bic, 'PART': part, 'ITEM': 'B1.26',
                                 'REMMTH': remmth, 'AMOUNT': amount})
                if intplan in AUD_PLANS:
                    rows.append({'BIC': bic, 'PART': part, 'ITEM': 'B1.30',
                                 'REMMTH': remmth, 'AMOUNT': amount})
            else:
                rows.append({'BIC': bic, 'PART': part, 'ITEM': 'B1.12',
                             'REMMTH': remmth, 'AMOUNT': amount})
                if intplan in USD_PLANS:
                    rows.append({'BIC': bic, 'PART': part, 'ITEM': 'B1.17',
                                 'REMMTH': remmth, 'AMOUNT': amount})
                if intplan in SGD_PLANS:
                    rows.append({'BIC': bic, 'PART': part, 'ITEM': 'B1.19',
                                 'REMMTH': remmth, 'AMOUNT': amount})
                if intplan in HKD_PLANS:
                    rows.append({'BIC': bic, 'PART': part, 'ITEM': 'B1.25',
                                 'REMMTH': remmth, 'AMOUNT': amount})
                if intplan in AUD_PLANS:
                    rows.append({'BIC': bic, 'PART': part, 'ITEM': 'B1.29',
                                 'REMMTH': remmth, 'AMOUNT': amount})

    if not rows:
        return pl.DataFrame(schema={'BIC': pl.Utf8, 'PART': pl.Utf8,
                                    'ITEM': pl.Utf8, 'REMMTH': pl.Float64,
                                    'AMOUNT': pl.Float64})
    return pl.DataFrame(rows)


# ============================================================================
# DATA SA — Savings Accounts
# ============================================================================
def build_sa(bnm_dir: Path, reptmon: str, nowk: str) -> pl.DataFrame:
    """
    DATA SA (KEEP=PART ITEM REMMTH AMOUNT);
    RETAIN PART '2-RM' REMMTH 0.1;
    SET BNM.SAVG&REPTMON&NOWK;
    IF PRODCD='N' THEN DELETE;
    """
    sa_file = bnm_dir / f"savg{reptmon}{nowk}.parquet"
    con = duckdb.connect()
    df  = con.execute(
        f"SELECT PRODCD, CUSTCD, CURBAL FROM read_parquet('{sa_file}') "
        f"WHERE PRODCD <> 'N'"
    ).pl()

    rows = []
    for r in df.to_dicts():
        custcd = str(r.get("CUSTCD") or "").strip()
        curbal = float(r.get("CURBAL") or 0.0)
        item   = 'A1.16' if custcd in ('77', '78', '95', '96') else 'A1.13'
        rows.append({'PART': '2-RM', 'ITEM': item, 'REMMTH': 0.1, 'AMOUNT': curbal})

    if not rows:
        return pl.DataFrame(schema={'PART': pl.Utf8, 'ITEM': pl.Utf8,
                                    'REMMTH': pl.Float64, 'AMOUNT': pl.Float64})
    return pl.DataFrame(rows)


# ============================================================================
# DATA CA — Current Accounts (RM)
# ============================================================================
def build_ca(bnm_dir: Path, reptmon: str, nowk: str) -> pl.DataFrame:
    """
    DATA CA (KEEP=PART ITEM REMMTH AMOUNT);
    RETAIN PART '2-RM' REMMTH 0.1;
    SET BNM.CURN&REPTMON&NOWK;
    IF PRODUCT IN &CURX OR PRODUCT IN (53,63,103);
    IF SUBSTR(PRODCD,1,3) IN ('421','423');
    """
    ca_file = bnm_dir / f"curn{reptmon}{nowk}.parquet"
    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{ca_file}')").pl()

    rows = []
    for r in df.to_dicts():
        product = int(r.get("PRODUCT") or 0)
        prodcd  = str(r.get("PRODCD") or "").strip()
        custcd  = str(r.get("CUSTCD") or "").strip()
        curbal  = float(r.get("CURBAL") or 0.0)

        if product not in CURX_PRODUCTS and product not in (53, 63, 103):
            continue
        if prodcd[:3] not in ('421', '423'):
            continue

        item = 'A1.17' if custcd in ('77', '78', '95', '96') else 'A1.14'
        rows.append({'PART': '2-RM', 'ITEM': item, 'REMMTH': 0.1, 'AMOUNT': curbal})

    if not rows:
        return pl.DataFrame(schema={'PART': pl.Utf8, 'ITEM': pl.Utf8,
                                    'REMMTH': pl.Float64, 'AMOUNT': pl.Float64})
    return pl.DataFrame(rows)


# ============================================================================
# DATA FCYCA — FCY Current Accounts
# ============================================================================
def build_fcyca(deposit_dir: Path) -> pl.DataFrame:
    """
    DATA FCYCX;  SET DEPOSIT.CURRENT;
    DATA FCYCA;  KEEP PART ITEM REMMTH AMOUNT; SET FCYCX;
    %LET PROD=(400,401,...,444)
    """
    current_file = deposit_dir / "current.parquet"
    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{current_file}')").pl()

    rows = []
    for r in df.to_dicts():
        product  = int(r.get("PRODUCT") or 0)
        custcode = int(r.get("CUSTCODE") or 0)
        curbal   = float(r.get("CURBAL") or 0.0)
        amount   = curbal
        ind      = custcode in (77, 78, 95, 96)

        if product in FCYCX_PROD:
            item = 'B1.16' if ind else 'B1.13'
            rows.append({'PART': '2-F$', 'ITEM': item, 'REMMTH': 0.1, 'AMOUNT': amount})

        if product in (400, 402, 403, 406, 440):
            if ind:
                if   product == 400: item = 'B1.22'
                elif product == 403: item = 'B1.24'
                elif product == 406: item = 'B1.28'
                elif product == 402: item = 'B1.32'
                elif product == 440: item = 'B1.22'
                else:                item = 'B1.22'
            else:
                if   product == 400: item = 'B1.21'
                elif product == 403: item = 'B1.23'
                elif product == 406: item = 'B1.27'
                elif product == 402: item = 'B1.31'
                elif product == 440: item = 'B1.21'
                else:                item = 'B1.21'
            rows.append({'PART': '2-F$', 'ITEM': item, 'REMMTH': 0.1, 'AMOUNT': amount})

    if not rows:
        return pl.DataFrame(schema={'PART': pl.Utf8, 'ITEM': pl.Utf8,
                                    'REMMTH': pl.Float64, 'AMOUNT': pl.Float64})
    return pl.DataFrame(rows)


# ============================================================================
# DATA UNOTE — Undrawn portions
# ============================================================================
def build_unote(bnm1_dir: Path, loan_dir: Path, reptmon: str, nowk: str,
                reptdate: date) -> pl.DataFrame:
    """
    DATA ALW; DATA UNOTE; — undrawn portions of loan facilities.
    Merges LOAN&REPTMON&NOWK (ACCTYPE filter) with LNCOMM.
    Also includes ULOAN&REPTMON&NOWK.
    """
    loan_file  = bnm1_dir / f"loan{reptmon}{nowk}.parquet"
    uloan_file = bnm1_dir / f"uloan{reptmon}{nowk}.parquet"
    lncomm_file = loan_dir / "lncomm.parquet"

    con = duckdb.connect()

    # DATA ALW: load loan data excluding OD products 151,152,181
    alw_df = con.execute(
        f"SELECT * FROM read_parquet('{loan_file}') "
        f"WHERE PAIDIND NOT IN ('P','C') "
        f"AND NOT (ACCTYPE='OD' AND PRODUCT IN (151,152,181))"
    ).pl()

    # DATA UNOTEX
    unotex_df = con.execute(
        f"SELECT * FROM read_parquet('{uloan_file}') "
        f"WHERE NOT ((ACCTNO BETWEEN 3000000000 AND 3999999999) "
        f"AND PRODUCT IN (151,152,181) AND ACCTYPE='OD')"
    ).pl()

    # Load LNCOMM
    lncomm_df = con.execute(f"SELECT * FROM read_parquet('{lncomm_file}')").pl()

    # Build APPR: merge ALWCOM (COMMNO > 0) with LNCOMM
    alwcom    = alw_df.filter(pl.col("COMMNO") > 0)
    lncomm_pd = {(r["ACCTNO"], r["COMMNO"]): r
                 for r in lncomm_df.to_dicts()}

    appr_rows = []
    for r in alwcom.sort(["ACCTNO", "COMMNO"]).to_dicts():
        prodcd  = str(r.get("PRODCD") or "").strip()
        acctno  = r.get("ACCTNO")
        commno  = r.get("COMMNO")
        if prodcd in ('34190', '34690'):
            key = (acctno, commno)
            if key in lncomm_pd:
                appr_rows.append(r)
        else:
            appr_rows.append(r)

    # APPR1: COMMNO <= 0, deduplicate RC accounts by APPRLIM2
    alwnocom = alw_df.filter(pl.col("COMMNO") <= 0).sort(["ACCTNO", "APPRLIM2"])
    seen_rc: dict = {}   # (acctno, apprlim2) -> first flag
    appr1_rows = []
    dup_rows   = []

    for r in alwnocom.to_dicts():
        prodcd   = str(r.get("PRODCD") or "").strip()
        acctno   = r.get("ACCTNO")
        apprlim2 = r.get("APPRLIM2")
        balance  = float(r.get("BALANCE") or 0.0)
        key      = (acctno, apprlim2)

        if prodcd in ('34190', '34690'):
            if key not in seen_rc:
                seen_rc[key] = True
                appr1_rows.append(r)
            else:
                dup_rows.append({**r, 'DUPLI': 1})
        else:
            appr1_rows.append(r)

    # Apply DUPLI logic: where balance >= apprlim2
    dup_keep = {(r["ACCTNO"], r["APPRLIM2"])
                for r in dup_rows
                if float(r.get("BALANCE") or 0) >= float(r.get("APPRLIM2") or 0)}

    final_appr1 = []
    for r in alwnocom.to_dicts():
        prodcd   = str(r.get("PRODCD") or "").strip()
        acctno   = r.get("ACCTNO")
        apprlim2 = r.get("APPRLIM2")
        balance  = float(r.get("BALANCE") or 0.0)
        key      = (acctno, apprlim2)

        if prodcd in ('34190', '34690'):
            if key in dup_keep:
                if balance >= (apprlim2 or 0):
                    final_appr1.append(r)
            else:
                if key in {(x["ACCTNO"], x["APPRLIM2"]) for x in appr1_rows}:
                    final_appr1.append(r)
        else:
            final_appr1.append(r)

    # Combine APPR + final_appr1 + UNOTEX
    combined = appr_rows + final_appr1 + unotex_df.to_dicts()

    rows = []
    for r in combined:
        prodcd  = str(r.get("PRODCD") or "").strip()
        product = int(r.get("PRODUCT") or 0)
        acctype = str(r.get("ACCTYPE") or "").strip()
        undrawn = float(r.get("UNDRAWN") or 0.0)

        # IF SUBSTR(PRODCD,1,2) = '34' OR PRODUCT IN (225,226)
        if not (prodcd[:2] == '34' or product in (225, 226)):
            continue

        def _to_date(val) -> Optional[date]:
            if val is None:
                return None
            if isinstance(val, (date, datetime)):
                return val.date() if isinstance(val, datetime) else val
            try:
                return date.fromisoformat(str(val)[:10])
            except Exception:
                return None

        exprdate  = _to_date(r.get("EXPRDATE"))
        apprdate  = _to_date(r.get("APPRDATE"))

        if acctype == 'LN':
            matdt = exprdate
        else:
            matdt = (apprdate + timedelta(days=365)) if apprdate else None

        if matdt is None or (matdt - reptdate).days < 8:
            remmth = 0.1
        else:
            remmth = calc_remmth(matdt, reptdate)

        if acctype == 'LN':
            item = 'A1.28'
            if prodcd == '34190':
                item = 'A1.26'
            elif prodcd == '34690':
                item = 'B2.26'
            elif prodcd == '34600':
                item = 'B2.28'
        else:
            item = 'A1.25'
            if prodcd == '34240':
                item = 'A1.28A'

        part = '2-RM'
        if prodcd in ('34600', '34690'):
            part = '2-FC'

        rows.append({'PART': part, 'ITEM': item, 'REMMTH': remmth, 'AMOUNT': undrawn})

    if not rows:
        return pl.DataFrame(schema={'PART': pl.Utf8, 'ITEM': pl.Utf8,
                                    'REMMTH': pl.Float64, 'AMOUNT': pl.Float64})
    return pl.DataFrame(rows)


# ============================================================================
# PROC SUMMARY  — aggregate by PART, ITEM, REMMTH
# ============================================================================
def summarise(df: pl.DataFrame) -> pl.DataFrame:
    """Equivalent to PROC SUMMARY NWAY; CLASS PART ITEM REMMTH; VAR AMOUNT; OUTPUT SUM="""
    if df.is_empty():
        return df
    return (
        df.group_by(["PART", "ITEM", "REMMTH"])
          .agg(pl.col("AMOUNT").sum())
    )


# ============================================================================
# DATA KTBL — KAPITI K1TBL / K3TBL with REMMTH computed
# ============================================================================
def build_ktbl(bnmk_dir: Path, reptmon: str, nowk: str,
               reptdate: date) -> pl.DataFrame:
    """
    DATA KTBL (KEEP=PART ITEM REMMTH AMOUNT);
    SET K1TBL K3TBL;
    IF ITEM ^= ' ';
    IF MATDT - REPTDATE < 8 THEN REMMTH = 0.1;
    ELSE REMMTH from %REMMTH macro;
    """
    k1 = build_k1tbl(bnmk_dir, reptmon, nowk, NREP, IREP, format_ctype)
    k3 = build_k3tbl(bnmk_dir, reptmon, nowk, INST)

    combined = pl.concat([k1, k3], how="diagonal")
    combined = combined.filter(pl.col("ITEM").is_not_null() & (pl.col("ITEM") != ""))

    rows = []
    for r in combined.to_dicts():
        matdt_raw = r.get("MATDT")
        if isinstance(matdt_raw, (date, datetime)):
            matdt = matdt_raw.date() if isinstance(matdt_raw, datetime) else matdt_raw
        else:
            matdt = None

        item   = r.get("ITEM", "")
        amount = float(r.get("AMOUNT") or 0.0)
        part   = str(r.get("PART") or "")

        if matdt is None or (matdt - reptdate).days < 8:
            remmth = 0.1
        else:
            remmth = calc_remmth(matdt, reptdate)

        rows.append({'PART': part, 'ITEM': item, 'REMMTH': remmth, 'AMOUNT': amount})

    if not rows:
        return pl.DataFrame(schema={'PART': pl.Utf8, 'ITEM': pl.Utf8,
                                    'REMMTH': pl.Float64, 'AMOUNT': pl.Float64})
    return pl.DataFrame(rows)


# ============================================================================
# PART 3 — Distribution profile: Non-Interbank Repos / NIDs
# ============================================================================
def build_part3_dist(bnmk_dir: Path, reptmon: str, nowk: str,
                     reptdate: date) -> pl.DataFrame:
    """
    DATA K1TBL (KEEP CAT NAME AMOUNT): NON-INTERBANK REPOS from K1TBL source
    DATA K3TBL (KEEP CAT NAME AMOUNT): NON-INTERBANK NIDS  from K3TBL source
    PROC APPEND; PROC SUMMARY NWAY CLASS CAT NAME; VAR AMOUNT; SUM=
    """
    k1_file = bnmk_dir / f"k1tbl{reptmon}{nowk}.parquet"
    k3_file = bnmk_dir / f"k3tbl{reptmon}{nowk}.parquet"
    con = duckdb.connect()

    k1_raw = con.execute(f"SELECT * FROM read_parquet('{k1_file}')").pl()
    k3_raw = con.execute(f"SELECT * FROM read_parquet('{k3_file}')").pl()

    repos_rows = []
    for r in k1_raw.to_dicts():
        gwccy  = str(r.get("GWCCY")  or "").strip()
        gwmvt  = str(r.get("GWMVT")  or "").strip()
        gwmvts = str(r.get("GWMVTS") or "").strip()
        gwctp  = str(r.get("GWCTP")  or "").strip()
        gwdlp  = str(r.get("GWDLP")  or "").strip()
        gwshn  = str(r.get("GWSHN")  or "").strip()
        amount = float(r.get("GWBALC") or 0.0)

        # IF GWCCY='MYR' AND GWMVT='P' AND GWMVTS='M'
        if gwccy != 'MYR' or gwmvt != 'P' or gwmvts != 'M':
            continue
        # IF SUBSTR(GWCTP,1,1) ^= 'B' AND SUBSTR(GWDLP,2,2) IN ('MI','MT')
        if gwctp[:1] == 'B':
            continue
        gwdlp_sub = gwdlp[1:3] if len(gwdlp) >= 3 else ""
        if gwdlp_sub not in ('MI', 'MT'):
            continue

        repos_rows.append({'CAT': 'NON-INTERBANK REPOS', 'NAME': gwshn, 'AMOUNT': amount})

    nids_rows = []
    for r in k3_raw.to_dicts():
        utctp  = str(r.get("UTCTP")  or "").strip()
        utref  = str(r.get("UTREF")  or "").strip()
        utsty  = str(r.get("UTSTY")  or "").strip()
        utamoc = float(r.get("UTAMOC") or 0.0)
        utdpf  = float(r.get("UTDPF")  or 0.0)
        utcus  = str(r.get("UTCUS")  or "").strip()
        utclc  = str(r.get("UTCLC")  or "").strip()

        # IF SUBSTR(UTCTP,1,1) ^= 'B'
        if utctp[:1] == 'B':
            continue
        # AND UTREF IN ('PFD','PLD','PSD','PZD')
        if utref not in ('PFD', 'PLD', 'PSD', 'PZD'):
            continue
        # AND UTSTY IN ('IFD','ILD','ISD','IZD','IZP')
        if utsty not in ('IFD', 'ILD', 'ISD', 'IZD', 'IZP'):
            continue

        amount = utamoc - utdpf
        name   = (utcus + utclc)[:24]
        nids_rows.append({'CAT': 'NON-INTERBANK NIDS', 'NAME': name, 'AMOUNT': amount})

    combined_rows = repos_rows + nids_rows
    if not combined_rows:
        return pl.DataFrame(schema={'CAT': pl.Utf8, 'NAME': pl.Utf8, 'AMOUNT': pl.Float64})

    df = pl.DataFrame(combined_rows)
    return (
        df.group_by(["CAT", "NAME"])
          .agg(pl.col("AMOUNT").sum())
          .sort(["CAT", "NAME"])
    )


# ============================================================================
# REPORT OUTPUT HELPERS (ASA carriage control, 60 lines/page)
# ============================================================================
ASA_FIRST  = '1'   # new page
ASA_NORMAL = ' '   # single spacing
ASA_DOUBLE = '0'   # double spacing (skip one line)

def _asa_line(cc: str, text: str) -> str:
    return cc + text + '\n'

def format_amount(val: float) -> str:
    """COMMA20.2 format"""
    if val == 0:
        return '0.00'.rjust(20)
    return f"{val:,.2f}".rjust(20)

def write_tabulate_report(
    out_fh,
    df: pl.DataFrame,
    title1: str,
    title2: str,
    title4: str,
    box_label: str,
    part_filter: str,
    exclude_items: set,
    page_lines: int = PAGE_LINES,
    show_part_dim: bool = False,
):
    """
    Replicate PROC TABULATE output:
      TABLE ITEM=' ', (REMMTH=' ' ALL='TOTAL')*(SUM=' '*AMOUNT=' '*F=COMMA20.2)
      / BOX=<box_label> RTS=45 CONDENSE

    Writes ASA carriage-control lines to out_fh.
    """
    # Filter
    if part_filter:
        sub = df.filter(pl.col("PART") == part_filter)
    else:
        sub = df.clone()

    if exclude_items:
        sub = sub.filter(~pl.col("ITEM").is_in(list(exclude_items)))

    if sub.is_empty():
        return

    sub = sub.with_columns(pl.col("REMMTH").map_elements(remfmt, return_dtype=pl.Utf8).alias("BUCKET"))

    # Aggregate
    agg = (
        sub.group_by(["ITEM", "BUCKET"])
           .agg(pl.col("AMOUNT").sum())
    )

    items   = sorted(agg["ITEM"].unique().to_list(), key=lambda x: list(ITEMF.keys()).index(x) if x in ITEMF else 999)
    buckets = [b for b in REMFMT_ORDER if b in agg["BUCKET"].to_list()]

    # Build pivot
    pivot: dict = {it: {bk: 0.0 for bk in REMFMT_ORDER} for it in items}
    totals: dict= {bk: 0.0 for bk in REMFMT_ORDER}
    for r in agg.to_dicts():
        pivot[r["ITEM"]][r["BUCKET"]] += r["AMOUNT"]
        totals[r["BUCKET"]] += r["AMOUNT"]

    col_headers = buckets + ['TOTAL']
    col_width   = 21
    label_width = 45

    lines_written = 0

    def page_header():
        nonlocal lines_written
        out_fh.write(_asa_line(ASA_FIRST, title1))
        out_fh.write(_asa_line(ASA_NORMAL, title2))
        out_fh.write(_asa_line(ASA_NORMAL, ''))
        out_fh.write(_asa_line(ASA_NORMAL, title4))
        out_fh.write(_asa_line(ASA_NORMAL, ''))
        # Box header row
        header  = box_label.ljust(label_width)
        header += ''.join(c.rjust(col_width) for c in col_headers)
        out_fh.write(_asa_line(ASA_NORMAL, header))
        out_fh.write(_asa_line(ASA_NORMAL, '-' * (label_width + col_width * len(col_headers))))
        lines_written = 8

    page_header()

    for item in items:
        if lines_written >= page_lines - 2:
            page_header()
        desc = ITEMF.get(item, item).ljust(label_width)
        row_total = sum(pivot[item].values())
        vals = [pivot[item].get(bk, 0.0) for bk in buckets] + [row_total]
        line = desc + ''.join(format_amount(v) for v in vals)
        out_fh.write(_asa_line(ASA_NORMAL, line))
        lines_written += 1

    # TOTAL row
    grand_total = sum(totals.values())
    vals = [totals.get(bk, 0.0) for bk in buckets] + [grand_total]
    line = 'TOTAL'.ljust(label_width) + ''.join(format_amount(v) for v in vals)
    out_fh.write(_asa_line(ASA_DOUBLE, line))


def write_part_tabulate_report(
    out_fh,
    df: pl.DataFrame,
    title1: str,
    title2: str,
    title4: str,
    box_label: str,
    page_lines: int = PAGE_LINES,
):
    """
    PROC TABULATE with PART as outer dimension:
      TABLE PART, ITEM=' ', (REMMTH=' ' ALL='TOTAL')*(SUM=...)*COMMA20.2
    """
    parts = sorted(df["PART"].unique().to_list())
    for part in parts:
        write_tabulate_report(
            out_fh, df, title1, title2, title4 + f' [{part}]',
            box_label, part_filter=part, exclude_items=set(), page_lines=page_lines,
        )


def write_fcyfd_print(out_fh, df: pl.DataFrame, title1: str, title2: str):
    """PROC PRINT DATA=FCYFD; VAR BICCODE AMOUNT; SUM AMOUNT;"""
    out_fh.write(_asa_line(ASA_FIRST, title1))
    out_fh.write(_asa_line(ASA_NORMAL, title2))
    out_fh.write(_asa_line(ASA_NORMAL, ''))
    out_fh.write(_asa_line(ASA_NORMAL, 'FCY FD BICCODE SUMMARY'))
    out_fh.write(_asa_line(ASA_NORMAL, f"{'BICCODE':<20} {'AMOUNT':>20}"))
    out_fh.write(_asa_line(ASA_NORMAL, '-' * 42))

    total = 0.0
    for r in df.sort("BICCODE").to_dicts():
        bic    = str(r.get("BICCODE") or "")
        amount = float(r.get("AMOUNT") or 0.0)
        total += amount
        out_fh.write(_asa_line(ASA_NORMAL, f"{bic:<20} {format_amount(amount)}"))

    out_fh.write(_asa_line(ASA_DOUBLE, f"{'SUM':<20} {format_amount(total)}"))


# ============================================================================
# MAIN
# ============================================================================
def main():
    # ---- Get reporting date ----
    reptdate, nowk, reptyear, reptmon, reptday, rdate = get_reptdate(BNM_DIR)

    sdesc = f'NEW LIQUIDITY FRAMEWORK - {reptyear}'
    title1 = sdesc
    title2 = f'NEW LIQUIDITY FRAMEWORK AS AT {rdate}'

    # ---- Build data sources ----
    print("Building NOTE (loans)...")
    note_raw = build_note(BNM1_DIR, reptmon, nowk, reptdate)
    note_sum  = summarise(note_raw.select(["PART", "ITEM", "REMMTH", "AMOUNT"]))

    print("Building FD...")
    fd_raw   = build_fd(FD_DIR, reptdate)
    fd_sum   = summarise(fd_raw.select(["PART", "ITEM", "REMMTH", "AMOUNT"]))

    print("Building SA...")
    sa_sum   = summarise(build_sa(BNM_DIR, reptmon, nowk))

    print("Building CA...")
    ca_sum   = summarise(build_ca(BNM_DIR, reptmon, nowk))

    print("Building FCYCA...")
    fcyca_sum= summarise(build_fcyca(DEPOSIT_DIR))

    print("Building UNOTE...")
    unote_sum= summarise(build_unote(BNM1_DIR, LOAN_DIR, reptmon, nowk, reptdate))

    # DATA NOTE: combine all sources
    note = pl.concat([note_sum, fd_sum, sa_sum, ca_sum, fcyca_sum, unote_sum],
                     how="diagonal")

    # ---- KAPITI Part 2 & 3 ----
    print("Building KTBL (K1TBL + K3TBL)...")
    ktbl = build_ktbl(BNMK_DIR, reptmon, nowk, reptdate)

    # ---- Part 3: Distribution profile ----
    print("Building Part 3 distribution...")
    dist_df = build_part3_dist(BNMK_DIR, reptmon, nowk, reptdate)

    # ---- FCY FD BICCODE mapping ----
    fcyfd_rows = []
    for r in note.filter(pl.col("PART") == '2-F$').to_dicts():
        item   = str(r.get("ITEM") or "")
        remmth = float(r.get("REMMTH") or 0.0)
        amount = float(r.get("AMOUNT") or 0.0)
        biccode = None
        if item == 'B1.13':
            biccode = '9631309010000Y'
        elif item == 'B1.16':
            biccode = '9631308010000Y'
        elif item == 'B1.12':
            if remmth <= 0.255:             biccode = '9631109010000Y'
            elif 0.255 < remmth <= 1:       biccode = '9631109020000Y'
            elif 1    < remmth <= 3:        biccode = '9631109030000Y'
            elif 3    < remmth <= 6:        biccode = '9631109040000Y'
            elif 6    < remmth <= 12:       biccode = '9631109050000Y'
            elif 12   < remmth:             biccode = '9631109060000Y'
        elif item == 'B1.15':
            if remmth <= 0.255:             biccode = '9631108010000Y'
            elif 0.255 < remmth <= 1:       biccode = '9631108020000Y'
            elif 1    < remmth <= 3:        biccode = '9631108030000Y'
            elif 3    < remmth <= 6:        biccode = '9631108040000Y'
            elif 6    < remmth <= 12:       biccode = '9631108050000Y'
            elif 12   < remmth:             biccode = '9631108060000Y'
        if biccode:
            fcyfd_rows.append({'BICCODE': biccode, 'AMOUNT': amount})

    if fcyfd_rows:
        fcyfd_df = (
            pl.DataFrame(fcyfd_rows)
              .group_by("BICCODE")
              .agg(pl.col("AMOUNT").sum())
        )
    else:
        fcyfd_df = pl.DataFrame(schema={'BICCODE': pl.Utf8, 'AMOUNT': pl.Float64})

    # ---- Write reports ----
    report_file = OUTPUT_DIR / f"EIBWLIQ1_{reptmon}{nowk}.txt"
    print(f"Writing report to {report_file}...")

    with open(report_file, 'w', encoding='utf-8') as fh:

        # OPTIONS NOCENTER NODATE NONUMBER MISSING=0

        # PART 1-RM
        write_tabulate_report(
            fh, note, title1, title2,
            title4='BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE (PART 1-RM)',
            box_label='CORE (NON-TRADING) BANKING ACTIVITIES',
            part_filter='1-RM',
            exclude_items={'B1.12', 'B1.15'},
        )

        # PART 2-RM
        write_tabulate_report(
            fh, note, title1, title2,
            title4='BREAKDOWN BY PURE CONTRACTUAL MATURITY PROFILE (PART 2-RM)',
            box_label='CORE (NON-TRADING) BANKING ACTIVITIES',
            part_filter='2-RM',
            exclude_items={'B2.26', 'B2.28'},
        )

        # PART 2-F$
        write_tabulate_report(
            fh, note, title1, title2,
            title4='BREAKDOWN BY PURE CONTRACTUAL MATURITY PROFILE (PART 2-F$)',
            box_label='CORE (NON-TRADING) BANKING ACTIVITIES',
            part_filter='2-F$',
            exclude_items=set(),
        )

        # KTBL — treasury/capital markets
        write_part_tabulate_report(
            fh, ktbl, title1, title2,
            title4='BREAKDOWN BY PURE CONTRACTUAL MATURITY PROFILE',
            box_label='TREASURY AND CAPITAL MARKET ACTIVITIES',
        )

        # FCY FD PROC PRINT
        write_fcyfd_print(fh, fcyfd_df, title1, title2)

        # PART 1-FC
        write_tabulate_report(
            fh, note, title1, title2,
            title4='BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE (PART 1-FCY)',
            box_label='CORE (NON-TRADING) BANKING ACTIVITIES',
            part_filter='1-FC',
            exclude_items=set(),
        )

        # PART 2-FC
        write_tabulate_report(
            fh, note, title1, title2,
            title4='BREAKDOWN BY PURE CONTRACTUAL MATURITY PROFILE (PART 2-FCY)',
            box_label='CORE (NON-TRADING) BANKING ACTIVITIES',
            part_filter='2-FC',
            exclude_items=set(),
        )

    print(f"Report written: {report_file}")


if __name__ == '__main__':
    main()
