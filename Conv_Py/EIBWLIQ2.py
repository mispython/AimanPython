#!/usr/bin/env python3
"""
Program  : EIBWLIQ2.py
Date     : 25.05.98
Report   : NEW LIQUIDITY FRAMEWORK
           CONTRACTUAL RUN-OFFS AS OF END OF THE MONTH

Purpose  : Generates the New Liquidity Framework Contractual Run-Off report.
           Uses RUNOFFDT (last day of reporting month) as the maturity reference
            instead of REPTDATE.
           Covers:
           - Part 1 & 2 RM: Loans (FL/HL use repayment date, OD/RC use expiry date)
           - Fixed Deposits maturing after RUNOFFDT
           - KAPITI treasury items (K1TBL + K3TBL from KALMLIQS,
             plus K3TBL3 from KALMLIQ3 — both used in KTBL)
           Output: ASA carriage-control text report (60 lines/page)
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
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
# From PBBLNFMT: loan product format and FCY product list
from PBBLNFMT import format_lnprod, FCY_PRODUCTS

# From PBBDPFMT: FD product format and CURX product list
from PBBDPFMT import FDProductFormat, ProductLists

# From PBBELF: customer type format function
from PBBELF import format_ctype

# From KALMLIQS: K1TBL and K3TBL builders (KAPITI money-market / investment items)
from KALMLIQS import build_k1tbl, build_k3tbl

# From KALMLIQ3: K3TBL3 builder with REMMTH derivation (MGS/RRS repos)
# NOTE: Unlike EIBWLIQ1 (where %INC PGM(KALMLIQ1) was a dead include),
#       %INC PGM(KALMLIQ3) is actively used here — its output contributes
#       to KTBL alongside K1TBL and K3TBL from KALMLIQS.
from KALMLIQ3 import build_k3tbl3_with_remmth

# ============================================================================
# PATH CONFIGURATION  — adjust as needed for your environment
# ============================================================================
BASE_DIR    = Path(".")
BNM_DIR     = BASE_DIR / "data" / "bnm"      # BNM library  (REPTDATE)
BNM1_DIR    = BASE_DIR / "data" / "bnm1"     # BNM1 library (LOAN)
FD_DIR      = BASE_DIR / "data" / "fd"       # FD library   (FD)
BNMK_DIR    = BASE_DIR / "data" / "bnmk"     # BNMK library (K1TBL, K3TBL)
OUTPUT_DIR  = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# GLOBAL CONSTANTS
# ============================================================================
# OPTIONS YEARCUTOFF=1950
YEARCUTOFF = 1950

PAGE_LINES = 60   # ASA report page length

# %LET FCY = (800,801,...,814)
FCY_SET = {
    800, 801, 802, 803, 804, 805, 806, 851, 852, 853, 854, 855, 856, 857, 858,
    859, 860, 807, 808, 809, 810, 811, 812, 813, 814,
}

# %LET INST
INST = 'PBB'

# %LET IREP / NREP
IREP = {'01', '02', '11', '12', '81'}
NREP = {'13', '17', '20', '60', '71', '72', '74', '76', '79', '85'}

# ============================================================================
# PROC FORMAT: REMFMT
# NOTE: EIBWLIQ2 uses a SIMPLER 3-bucket REMFMT than EIBWLIQ1:
#   LOW-0.255  = 'UP TO 1 WK'
#   0.255-1    = '>1 WK - 1 MTH'
#   OTHER      = '>1 MTH'
# ============================================================================
def remfmt(remmth: Optional[float]) -> str:
    """
    VALUE REMFMT
       LOW-0.255 = 'UP TO 1 WK'
       0.255-1   = '>1 WK - 1 MTH'
       OTHER     = '>1 MTH';
    """
    if remmth is None:
        return '>1 MTH'
    if remmth <= 0.255:
        return 'UP TO 1 WK'
    elif remmth <= 1:
        return '>1 WK - 1 MTH'
    else:
        return '>1 MTH'

REMFMT_ORDER = ['UP TO 1 WK', '>1 WK - 1 MTH', '>1 MTH']

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
    'A1.12':  'A1.12  DEPOSITS: CORP - FIXED',
    'A1.12A': 'A1.12A DEPOSITS: CORP - GID  ',
    'A1.13':  'A1.13  DEPOSITS: CORP - SAVINGS',
    'A1.14':  'A1.14  DEPOSITS: CORP - CURRENT',
    'A1.15':  'A1.15  DEPOSITS: IND  - FIXED',
    'A1.15A': 'A1.15A DEPOSITS: IND  - GID  ',
    'A1.16':  'A1.16  DEPOSITS: IND  - SAVINGS',
    'A1.17':  'A1.17  DEPOSITS: IND  - CURRENT',
    'A1.20':  'A1.20  SHAREHOLDER FUND & OTHER LIABILITIES',
    'A1.25':  'A1.25  UNDRAWN OD FACILITIES GIVEN',
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
    'B1.12':  'B1.12  DEPOSITS: CORP - FIXED',
    'B1.15':  'B1.15  DEPOSITS: IND  - FIXED',
    'B2.01':  'B2.01  INTERBANK LENDING/DEPOSITS',
    'B2.09':  'B2.09  FX EXCHG CONTRACTS RECEIVABLE',
    'B2.14':  'B2.14  INTERBANK BORROWINGS/DEPOSITS',
    'B2.19':  'B2.19  FX EXCHG CONTRACTS PAYABLE',
}

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
# MACRO %REMMTH — compute remaining months relative to runoffdt
# ============================================================================
def calc_remmth(matdt: date, runoffdt: date) -> float:
    """
    Replicate %REMMTH macro against &RUNOFFDT:
      REMY  = MDYR  - RPYR
      REMM  = MDMTH - RPMTH
      REMD  = MDDAY - RPDAY  (capped to rpdays(rpmth))
      REMMTH= REMY*12 + REMM + REMD/RPDAYS(RPMTH)
    """
    rpyr   = runoffdt.year
    rpmth  = runoffdt.month
    rpday  = runoffdt.day
    rp_dim = days_in_month(rpyr, rpmth)

    mdyr  = matdt.year
    mdmth = matdt.month
    mdday = matdt.day

    # IF MDDAY > RPDAYS(RPMTH) THEN MDDAY = RPDAYS(RPMTH)
    if mdday > rp_dim:
        mdday = rp_dim

    remy = mdyr  - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday
    return remy * 12 + remm + remd / rp_dim

# ============================================================================
# MACRO %NXTBLDT — compute next billing date
# ============================================================================
def nxtbldt(bldate: date, issdte: date, payfreq: str, freq: int) -> date:
    """
    Replicate %NXTBLDT macro:
    PAYFREQ='6' (bi-weekly) -> add 14 days
    Otherwise              -> advance by FREQ months, using ISSDTE day-of-month
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
# LIQPFMT helper — maps product to loan type label (FL/HL/RC/etc.)
# Replicates PUT(PRODUCT, LIQPFMT.)
# ============================================================================
def liqpfmt(product: int) -> str:
    """Map product code to liquidity product type using LNPROD_MAP from PBBLNFMT."""
    prodcd = format_lnprod(product)
    if prodcd == '34120':
        return 'HL'
    elif prodcd in ('34190', '34690'):
        return 'RC'
    return 'FL'

# ============================================================================
# GET REPTDATE — read reporting date and derive macro variables
# ============================================================================
def get_reptdate(bnm_dir: Path):
    """
    DATA REPTDATE; SET BNM.REPTDATE;
    Returns (reptdate, nowk, reptyear, reptmon, reptday, rdate)
    """
    reptdate_file = bnm_dir / "reptdate.parquet"
    con  = duckdb.connect()
    df   = con.execute(f"SELECT REPTDATE FROM read_parquet('{reptdate_file}') LIMIT 1").pl()
    raw  = df["REPTDATE"][0]
    if isinstance(raw, (date, datetime)):
        reptdate = raw.date() if isinstance(raw, datetime) else raw
    else:
        reptdate = date.fromisoformat(str(raw)[:10])

    day = reptdate.day
    if   day == 8:  nowk = '1'
    elif day == 15: nowk = '2'
    elif day == 22: nowk = '3'
    else:           nowk = '4'

    reptyear = str(reptdate.year)
    reptmon  = str(reptdate.month).zfill(2)
    reptday  = str(reptdate.day).zfill(2)
    rdate    = reptdate.strftime('%d/%m/%y')   # DDMMYY8. approximation
    return reptdate, nowk, reptyear, reptmon, reptday, rdate

# ============================================================================
# DETERMINE RUNOFFDT — last calendar day of the reporting month
# DATA _NULL_: RUNOFFDT = MDY(RPMTH, RPDAYS(RPMTH), RPYR)
# ============================================================================
def get_runoffdt(reptdate: date) -> date:
    """
    DATA _NULL_;
       %DCLVAR
       SET BNM.REPTDATE;
       RPYR  = YEAR(REPTDATE); RPMTH = MONTH(REPTDATE); RPDAY = DAY(REPTDATE);
       IF MOD(RPYR,4) = 0 THEN RD2 = 29;
       RUNOFFDT = MDY(RPMTH,RPDAYS(RPMTH),RPYR);
    Returns the last day of the reporting month.
    """
    last_day = days_in_month(reptdate.year, reptdate.month)
    return date(reptdate.year, reptdate.month, last_day)

# ============================================================================
# DATA NOTE — Loans, contractual run-off against RUNOFFDT
# ============================================================================
def build_note(bnm1_dir: Path, reptmon: str, nowk: str,
               reptdate: date, runoffdt: date) -> pl.DataFrame:
    """
    DATA NOTEX; SET BNM1.LOAN&REPTMON&NOWK;
    IF PRODUCT IN (151,152,181) AND ACCTYPE='OD' THEN DELETE;
    IF PAIDIND NOT IN ('P','C');

    DATA NOTE (KEEP=PART ITEM REMMTH AMOUNT);
    Key difference from EIBWLIQ1:
      - All REMMTH calculated relative to &RUNOFFDT (not REPTDATE)
      - Iteration exits when REMMTH > 1 (not > 12)
      - PRODCD='34240' gets special OD + LN output for Part 1-RM
      - No Part 2-RM output for regular LN (only Part 1-RM/1-FC)
    """
    loan_file = bnm1_dir / f"loan{reptmon}{nowk}.parquet"
    con = duckdb.connect()
    df  = con.execute(
        f"SELECT * FROM read_parquet('{loan_file}') "
        f"WHERE PAIDIND NOT IN ('P','C') "
        f"AND NOT (PRODUCT IN (151,152,181) AND ACCTYPE='OD')"
    ).pl()

    rows = []

    for r in df.to_dicts():
        prodcd   = str(r.get("PRODCD")   or "").strip()
        product  = int(r.get("PRODUCT")  or 0)
        acctype  = str(r.get("ACCTYPE")  or "").strip()
        custcd   = str(r.get("CUSTCD")   or "").strip()
        balance  = float(r.get("BALANCE") or 0.0)
        payamt   = float(r.get("PAYAMT")  or 0.0)
        loanstat = int(r.get("LOANSTAT") or 0)
        payfreq  = str(r.get("PAYFREQ")  or "").strip()

        def _to_date(val) -> Optional[date]:
            if val is None:
                return None
            if isinstance(val, (date, datetime)):
                return val.date() if isinstance(val, datetime) else val
            try:
                return date.fromisoformat(str(val)[:10])
            except Exception:
                return None

        bldate   = _to_date(r.get("BLDATE"))
        issdte   = _to_date(r.get("ISSDTE"))
        exprdate = _to_date(r.get("EXPRDATE"))

        # IF SUBSTR(PRODCD,1,2) = '34' OR PRODUCT IN (225,226)
        if not (prodcd[:2] == '34' or product in (225, 226)):
            continue

        freq_map = {'1': 1, '2': 3, '3': 6, '4': 12}
        freq = freq_map.get(payfreq, 0)
        prod = liqpfmt(product)

        ind = custcd in ('77', '78', '95', '96')

        # ---- ACCTYPE = 'OD' ----
        if acctype == 'OD':
            # IF PRODUCT IN (151,152,181) THEN DELETE  (already filtered above)
            remmth = 0.1
            amount = balance
            part   = '2-RM'
            item   = 'A1.07' if ind else 'A1.03'
            rows.append({'PART': part, 'ITEM': item, 'REMMTH': remmth, 'AMOUNT': amount})
            continue

        # ---- PRODCD = '34240': share financing — special Part 1-RM output ----
        if prodcd == '34240':
            item   = 'A1.08B' if ind else 'A1.04A'
            rows.append({'PART': '1-RM', 'ITEM': item, 'REMMTH': 0.1, 'AMOUNT': balance})
            # Falls through to ACCTYPE='LN' check below (no continue here in SAS)

        # ---- ACCTYPE = 'LN' ----
        if acctype != 'LN':
            continue

        # Determine ITEM
        if product not in FCY_SET:
            if ind:
                if   prod == 'HL': item = 'A1.05'
                elif prod == 'RC': item = 'A1.08A'
                else:              item = 'A1.08'
            else:
                if   prod in ('FL', 'HL'): item = 'A1.01'
                elif prod == 'RC':         item = 'A1.02'
                else:                      item = 'A1.04'
        else:
            item = 'B1.01' if prod == 'FL' else ('B1.02' if prod == 'RC' else 'A1.04')

        # IF PRODUCT = 100 THEN ITEM = 'A1.05'  /* HARDCODE BY MAZNI */
        if product == 100:
            item = 'A1.05'

        # Days since last billing date (used for behavioural flag)
        days = 0
        if bldate and bldate > date(1900, 1, 1):
            days = (reptdate - bldate).days

        if exprdate is None:
            exprdate = reptdate  # fallback

        # IF EXPRDATE <= &RUNOFFDT THEN REMMTH = .  (record skipped — missing remmth)
        if exprdate <= runoffdt:
            # Still output with DAYS>89 / LOANSTAT logic, but REMMTH is missing (None)
            amount = balance
            remmth: Optional[float] = None
            if days > 89 or loanstat != 1:
                remmth = 13
            _output_ln_row(rows, product, prod, '1-RM', item, remmth, amount, FCY_SET)
            continue

        # IF EXPRDATE - &RUNOFFDT < 8 THEN REMMTH = 0.1
        if (exprdate - runoffdt).days < 8:
            amount = balance
            remmth_fin: Optional[float] = 0.1
            if days > 89 or loanstat != 1:
                remmth_fin = 13
            _output_ln_row(rows, product, prod, '1-RM', item, remmth_fin, amount, FCY_SET)
            continue

        # Set initial bldate for iteration
        if payfreq in ('5', '9', ' ', '') or product in (350, 910, 925):
            bldate = exprdate
        elif bldate is None or bldate <= date(1900, 1, 1):
            bldate = issdte
            if bldate:
                while bldate <= reptdate:
                    bldate = nxtbldt(bldate, issdte, payfreq, freq)

        if payamt < 0:
            payamt = 0

        if bldate and (bldate > exprdate or balance <= payamt):
            bldate = exprdate

        # Iteration over billing dates up to end of month (RUNOFFDT)
        while bldate and bldate <= exprdate:
            # IF BLDATE <= &RUNOFFDT THEN REMMTH = .
            if bldate <= runoffdt:
                remmth_iter: Optional[float] = None
            # ELSE IF BLDATE - &RUNOFFDT < 8 THEN REMMTH = 0.1
            elif (bldate - runoffdt).days < 8:
                remmth_iter = 0.1
            else:
                matdt      = bldate
                remmth_iter = calc_remmth(matdt, runoffdt)

            # IF REMMTH > 1 OR BLDATE = EXPRDATE THEN LEAVE
            if (remmth_iter is not None and remmth_iter > 1) or bldate == exprdate:
                break

            amount    = payamt
            balance  -= payamt

            r_out = remmth_iter
            if days > 89 or loanstat != 1:
                r_out = 13

            _output_ln_row(rows, product, prod, '1-RM', item, r_out, amount, FCY_SET)

            bldate = nxtbldt(bldate, issdte, payfreq, freq)
            if bldate > exprdate or balance <= payamt:
                bldate = exprdate

        # Remaining balance — always output
        amount = balance
        r_out  = None
        if days > 89 or loanstat != 1:
            r_out = 13
        _output_ln_row(rows, product, prod, '1-RM', item, r_out, amount, FCY_SET)

    if not rows:
        return pl.DataFrame(schema={'PART': pl.Utf8, 'ITEM': pl.Utf8,
                                    'REMMTH': pl.Float64, 'AMOUNT': pl.Float64})
    return pl.DataFrame(rows)


def _output_ln_row(rows, product, prod, base_part, item,
                   remmth, amount, fcy_set):
    """Determine PART and ITEM for FCY vs RM loan output (Part 1 only)."""
    if product in fcy_set:
        part     = base_part.replace('RM', 'FC')
        item_out = 'B1.01' if prod == 'FL' else ('B1.02' if prod == 'RC' else item)
    else:
        part     = base_part
        item_out = item
    rows.append({'PART': part, 'ITEM': item_out, 'REMMTH': remmth, 'AMOUNT': amount})

# ============================================================================
# DATA FD — Fixed Deposits maturing after RUNOFFDT
# ============================================================================
def build_fd(fd_dir: Path, reptdate: date, runoffdt: date) -> pl.DataFrame:
    """
    DATA FD (KEEP=PART ITEM REMMTH AMOUNT);
    RETAIN PART '2-RM';
    SET FD.FD;
    IF CURBAL > 0;
    MATDT = INPUT(PUT(MATDATE,Z8.),YYMMDD8.);
    IF MATDT > &RUNOFFDT;     <- key filter: only FDs not yet matured at month-end
    IF OPENIND='D' OR MATDT - &RUNOFFDT < 8 THEN REMMTH=0.1;
    ELSE %REMMTH (against RUNOFFDT);
    Note: EIBWLIQ2 FD does NOT output BIC='42630' FCY breakdown rows;
          it simply assigns B1.15 / B1.12 directly.
    """
    fd_file = fd_dir / "fd.parquet"
    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{fd_file}')").pl()

    rows = []
    for r in df.to_dicts():
        curbal   = float(r.get("CURBAL") or 0.0)
        if curbal <= 0:
            continue

        intplan   = int(r.get("INTPLAN")  or 0)
        custcd    = int(r.get("CUSTCD")   or 0)
        openind   = str(r.get("OPENIND")  or "").strip()
        matdate_raw = r.get("MATDATE")
        accttype  = int(r.get("ACCTTYPE") or 0)

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

        # IF MATDT > &RUNOFFDT
        if matdt is None or matdt <= runoffdt:
            continue

        # IF OPENIND='D' OR MATDT - &RUNOFFDT < 8 THEN REMMTH=0.1; ELSE %REMMTH
        if openind == 'D' or (matdt - runoffdt).days < 8:
            remmth = 0.1
        else:
            remmth = calc_remmth(matdt, runoffdt)

        amount = curbal
        bic    = FDProductFormat.format(intplan)
        ind    = custcd in (77, 78, 95, 96)

        if bic != '42630':
            if bic == '42132':
                item = 'A1.15A' if ind else 'A1.12A'
            else:
                item = 'A1.15' if ind else 'A1.12'
            # IF ACCTTYPE IN (315,394)
            if accttype in (315, 394):
                item = 'A1.15' if ind else 'A1.12'
        else:
            # FCY fixed deposit: simplified to B1.15 / B1.12 (no sub-currency rows)
            item = 'B1.15' if ind else 'B1.12'

        rows.append({'PART': '2-RM', 'ITEM': item, 'REMMTH': remmth, 'AMOUNT': amount})

    if not rows:
        return pl.DataFrame(schema={'PART': pl.Utf8, 'ITEM': pl.Utf8,
                                    'REMMTH': pl.Float64, 'AMOUNT': pl.Float64})
    return pl.DataFrame(rows)

# ============================================================================
# PROC SUMMARY — aggregate by PART, ITEM, REMMTH
# ============================================================================
def summarise(df: pl.DataFrame) -> pl.DataFrame:
    """PROC SUMMARY NWAY; CLASS PART ITEM REMMTH; VAR AMOUNT; OUTPUT SUM="""
    if df.is_empty():
        return df
    return (
        df.group_by(["PART", "ITEM", "REMMTH"])
          .agg(pl.col("AMOUNT").sum())
    )

# ============================================================================
# DATA KTBL — KAPITI K1TBL + K3TBL (KALMLIQS) + K3TBL3 (KALMLIQ3)
# ============================================================================
def build_ktbl(bnmk_dir: Path, reptmon: str, nowk: str,
               reptdate: date, runoffdt: date) -> pl.DataFrame:
    """
    %INC PGM(KALMLIQS);   -> build_k1tbl + build_k3tbl (return MATDT)
    %INC PGM(KALMLIQ3);   -> build_k3tbl3_with_remmth (returns REMMTH directly)

    DATA KTBL (KEEP=PART ITEM REMMTH AMOUNT);
       %DCLVAR
       SET K1TBL K3TBL;
       IF _N_ = 1 THEN DO;
          SET REPTDATE;
          RPYR  = YEAR(&RUNOFFDT); RPMTH = MONTH(&RUNOFFDT); RPDAY = DAY(&RUNOFFDT);
          IF MOD(RPYR,4) = 0 THEN RD2 = 29;
       END;
       IF ITEM ^= ' ';
       IF MATDT > &RUNOFFDT;
       IF MATDT - &RUNOFFDT < 8 THEN REMMTH = 0.1;
       ELSE DO; %REMMTH END;

    KALMLIQ3 output already carries REMMTH (computed against RUNOFFDT),
    so it is appended after the MATDT-based REMMTH derivation for K1TBL/K3TBL.
    """
    # K1TBL and K3TBL return PART, ITEM, MATDT, AMOUNT
    k1 = build_k1tbl(bnmk_dir, reptmon, nowk, NREP, IREP, format_ctype)
    k3 = build_k3tbl(bnmk_dir, reptmon, nowk, INST)

    combined = pl.concat([k1, k3], how="diagonal")
    combined = combined.filter(
        pl.col("ITEM").is_not_null() & (pl.col("ITEM") != "")
    )

    rows = []
    for r in combined.to_dicts():
        matdt_raw = r.get("MATDT")
        if isinstance(matdt_raw, (date, datetime)):
            matdt = matdt_raw.date() if isinstance(matdt_raw, datetime) else matdt_raw
        else:
            matdt = None

        item   = str(r.get("ITEM")   or "")
        amount = float(r.get("AMOUNT") or 0.0)
        part   = str(r.get("PART")   or "")

        # IF MATDT > &RUNOFFDT
        if matdt is None or matdt <= runoffdt:
            continue

        # IF MATDT - &RUNOFFDT < 8 THEN REMMTH = 0.1; ELSE %REMMTH
        if (matdt - runoffdt).days < 8:
            remmth = 0.1
        else:
            remmth = calc_remmth(matdt, runoffdt)

        rows.append({'PART': part, 'ITEM': item, 'REMMTH': remmth, 'AMOUNT': amount})

    # %INC PGM(KALMLIQ3): K3TBL3 already has REMMTH derived against RUNOFFDT
    k3tbl3 = build_k3tbl3_with_remmth(
        bnmk_dir, reptmon, nowk, reptdate, runoffdt, NREP, IREP, format_ctype
    )
    for r in k3tbl3.to_dicts():
        rows.append({
            'PART':   str(r.get("PART")   or ""),
            'ITEM':   str(r.get("ITEM")   or ""),
            'REMMTH': float(r.get("REMMTH") or 0.0),
            'AMOUNT': float(r.get("AMOUNT") or 0.0),
        })

    if not rows:
        return pl.DataFrame(schema={'PART': pl.Utf8, 'ITEM': pl.Utf8,
                                    'REMMTH': pl.Float64, 'AMOUNT': pl.Float64})
    return pl.DataFrame(rows)

# ============================================================================
# REPORT OUTPUT HELPERS (ASA carriage control, 60 lines/page)
# ============================================================================
ASA_FIRST  = '1'   # new page
ASA_NORMAL = ' '   # single spacing
ASA_DOUBLE = '0'   # double spacing (skip one line before)

def _asa_line(cc: str, text: str) -> str:
    return cc + text + '\n'

def format_amount(val: float) -> str:
    """COMMA20.2 format; MISSING=0 means None/NaN renders as 0.00"""
    if val is None or (isinstance(val, float) and (val != val)):  # NaN check
        val = 0.0
    return f"{val:,.2f}".rjust(20)

def write_tabulate_report(
    out_fh,
    df: pl.DataFrame,
    title1: str,
    title2: str,
    title3: str,
    title4: str,
    box_label: str,
    part_filter: str,
    page_lines: int = PAGE_LINES,
):
    """
    Replicate PROC TABULATE:
      TABLE ITEM=' ', (REMMTH=' ' ALL='TOTAL')*(SUM=' '*AMOUNT=' '*F=COMMA20.2)
      / BOX=<box_label> RTS=45 CONDENSE
    OPTIONS MISSING=0: missing REMMTH values rendered as 0.
    Writes ASA carriage-control lines to out_fh.
    """
    sub = df.filter(pl.col("PART") == part_filter)
    if sub.is_empty():
        return

    # OPTIONS MISSING=0: replace null REMMTH with 0 for bucketing
    sub = sub.with_columns(
        pl.col("REMMTH").fill_null(0.0).alias("REMMTH")
    )
    sub = sub.with_columns(
        pl.col("REMMTH").map_elements(remfmt, return_dtype=pl.Utf8).alias("BUCKET")
    )

    agg = (
        sub.group_by(["ITEM", "BUCKET"])
           .agg(pl.col("AMOUNT").sum())
    )

    items   = sorted(
        agg["ITEM"].unique().to_list(),
        key=lambda x: list(ITEMF.keys()).index(x) if x in ITEMF else 999
    )
    buckets = [b for b in REMFMT_ORDER if b in agg["BUCKET"].to_list()]

    # Build pivot
    pivot: dict  = {it: {bk: 0.0 for bk in REMFMT_ORDER} for it in items}
    totals: dict = {bk: 0.0 for bk in REMFMT_ORDER}
    for r in agg.to_dicts():
        pivot[r["ITEM"]][r["BUCKET"]] += r["AMOUNT"]
        totals[r["BUCKET"]]           += r["AMOUNT"]

    col_headers = buckets + ['TOTAL']
    col_width   = 21
    label_width = 45

    lines_written = 0

    def page_header():
        nonlocal lines_written
        out_fh.write(_asa_line(ASA_FIRST,  title1))
        out_fh.write(_asa_line(ASA_NORMAL, title2))
        out_fh.write(_asa_line(ASA_NORMAL, title3))
        out_fh.write(_asa_line(ASA_NORMAL, ''))
        out_fh.write(_asa_line(ASA_NORMAL, title4))
        out_fh.write(_asa_line(ASA_NORMAL, ''))
        header = box_label.ljust(label_width)
        header += ''.join(c.rjust(col_width) for c in col_headers)
        out_fh.write(_asa_line(ASA_NORMAL, header))
        out_fh.write(_asa_line(ASA_NORMAL, '-' * (label_width + col_width * len(col_headers))))
        lines_written = 9

    page_header()

    for item in items:
        if lines_written >= page_lines - 2:
            page_header()
        desc      = ITEMF.get(item, item).ljust(label_width)
        row_total = sum(pivot[item].values())
        vals      = [pivot[item].get(bk, 0.0) for bk in buckets] + [row_total]
        line      = desc + ''.join(format_amount(v) for v in vals)
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
    title3: str,
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
            out_fh, df, title1, title2, title3,
            title4=f'{title4} [{part}]',
            box_label=box_label,
            part_filter=part,
            page_lines=page_lines,
        )

# ============================================================================
# MAIN
# ============================================================================
def main():
    # ---- Get reporting date and derived variables ----
    reptdate, nowk, reptyear, reptmon, reptday, rdate = get_reptdate(BNM_DIR)

    # ---- Determine run-off date (last day of reporting month) ----
    runoffdt = get_runoffdt(reptdate)
    print(f"REPTDATE : {reptdate}  |  RUNOFFDT : {runoffdt}  |  NOWK : {nowk}")

    sdesc  = f'NEW LIQUIDITY FRAMEWORK - {reptyear}'
    title1 = sdesc
    title2 = f'NEW LIQUIDITY FRAMEWORK AS AT {rdate}'
    title3 = '(CONTRACTUAL RUN-OFF)'

    # ---- Build NOTE (loans) ----
    print("Building NOTE (loans — contractual run-off)...")
    note_raw = build_note(BNM1_DIR, reptmon, nowk, reptdate, runoffdt)
    note_sum = summarise(note_raw)

    # ---- Build FD ----
    print("Building FD (fixed deposits maturing after RUNOFFDT)...")
    fd_raw  = build_fd(FD_DIR, reptdate, runoffdt)
    fd_sum  = summarise(fd_raw)

    # PROC APPEND BASE=NOTE DATA=FD
    note = pl.concat([note_sum, fd_sum], how="diagonal")

    # ---- Build KTBL (KAPITI treasury items) ----
    print("Building KTBL (K1TBL + K3TBL from KALMLIQS, K3TBL3 from KALMLIQ3)...")
    ktbl = build_ktbl(BNMK_DIR, reptmon, nowk, reptdate, runoffdt)

    # ---- Write reports ----
    report_file = OUTPUT_DIR / f"EIBWLIQ2_{reptmon}{nowk}.txt"
    print(f"Writing report to {report_file}...")

    # OPTIONS NOCENTER NODATE NONUMBER MISSING=0
    with open(report_file, 'w', encoding='utf-8') as fh:

        # PART 1-RM: Behavioural Maturity Profile
        write_tabulate_report(
            fh, note, title1, title2, title3,
            title4='BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE (PART 1-RM)',
            box_label='CORE (NON-TRADING) BANKING ACTIVITIES',
            part_filter='1-RM',
        )

        # PART 2-RM: Pure Contractual Maturity Profile
        write_tabulate_report(
            fh, note, title1, title2, title3,
            title4='BREAKDOWN BY PURE CONTRACTUAL MATURITY PROFILE (PART 2-RM)',
            box_label='CORE (NON-TRADING) BANKING ACTIVITIES',
            part_filter='2-RM',
        )

        # KTBL: Treasury and Capital Market Activities (all PARTs)
        write_part_tabulate_report(
            fh, ktbl, title1, title2, title3,
            title4='BREAKDOWN BY PURE CONTRACTUAL MATURITY PROFILE',
            box_label='TREASURY AND CAPITAL MARKET ACTIVITIES',
        )

        # PART 1-FC: Behavioural Maturity Profile (FCY)
        write_tabulate_report(
            fh, note, title1, title2, title3,
            title4='BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE (PART 1-FCY)',
            box_label='CORE (NON-TRADING) BANKING ACTIVITIES',
            part_filter='1-FC',
        )

    print(f"Report written: {report_file}")


if __name__ == '__main__':
    main()
