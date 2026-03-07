#!/usr/bin/env python3
"""
Program  : EIBWLQP2.py
Date     : 29.07.2005
Report   : NEW LIQUIDITY FRAMEWORK (PART 2-RM LOANS)
           CONTRACTUAL RUN-OFFS AS OF END OF THE MONTH

Purpose  : Generates contractual run-off report for loans only (no deposits,
           no KAPITI items). Covers Part 2-RM and Part 2-FC loan breakdowns.

           Key distinctions from EIBWLIQ2:
           - Only ACCTYPE='LN' is processed (no OD, no PRODCD='34240' special case)
           - Outputs Part 2-RM / 2-FC (not Part 1)
           - DAYS>89 or LOANSTAT!=1 sets REMMTH=0.1 (not 13 as in EIBWLIQ1/2)
           - Item codes use A2.xx prefix (programme-specific mapping)
           - No FD, SA, CA, UNOTE, or KAPITI KTBL sections
           - RUNOFFDT = last calendar day of the reporting month
           Output: ASA carriage-control text report (60 lines/page)
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
from pathlib import Path
from datetime import date, datetime
from typing import Optional

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
import duckdb
import polars as pl

# ============================================================================
# DEPENDENCY IMPORTS
# ============================================================================
# From PBBLNFMT: loan product format function
from PBBLNFMT_clau import format_lnprod

# From PBBDPFMT: included in SAS but no deposit formats are used in this program;
#   imported for completeness as declared in %INC PGM(PBBLNFMT,PBBDPFMT)
# from PBBDPFMT import FDProductFormat  # (not directly used — included for consistency)

# ============================================================================
# PATH CONFIGURATION  — adjust as needed for your environment
# ============================================================================
BASE_DIR   = Path(".")
BNM_DIR    = BASE_DIR / "data" / "bnm"     # BNM library  (REPTDATE)
BNM1_DIR   = BASE_DIR / "data" / "bnm1"    # BNM1 library (LOAN)
OUTPUT_DIR = BASE_DIR / "output"
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

# ============================================================================
# PROC FORMAT: REMFMT  (3-bucket, same as EIBWLIQ2)
# VALUE REMFMT
#    LOW-0.255 = 'UP TO 1 WK'
#    0.255-1   = '>1 WK - 1 MTH'
#    OTHER     = '>1 MTH';
# ============================================================================
def remfmt(remmth: Optional[float]) -> str:
    """Apply REMFMT format — 3 buckets."""
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
# PROC FORMAT: $ITEMF  (programme-specific item codes — A2.xx prefix)
# NOTE: These item codes differ from EIBWLIQ1/2. They are unique to EIBWLQP2.
# ============================================================================
ITEMF = {
    'A2.01':  'A2.01  LOANS: CORP - FIXED TERM LOANS',
    'A2.02':  'A2.02  LOANS: CORP - REVOLVING LOANS',
    'A2.04':  'A2.04  LOANS: CORP - OTHERS',
    'A2.05':  'A2.05  LOANS: IND  - HOUSING LOANS',
    'A2.08':  'A2.08  LOANS: IND  - OTHERS',
    'A2.08A': 'A2.08A LOANS: IND  - REVOLVING LOANS',
    'B1.01':  'B1.01  LOANS: NON-INDIVIDUALS - FIXED TERM LOANS',
    'B1.02':  'B1.02  LOANS: NON-INDIVIDUALS - REVOLVING LOANS',
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
       RPYR  = YEAR(REPTDATE); RPMTH = MONTH(REPTDATE);
       IF MOD(RPYR,4) = 0 THEN RD2 = 29;
       RUNOFFDT = MDY(RPMTH,RPDAYS(RPMTH),RPYR);
    Returns the last day of the reporting month.
    """
    last_day = days_in_month(reptdate.year, reptdate.month)
    return date(reptdate.year, reptdate.month, last_day)

# ============================================================================
# DATA NOTE — Loans only (ACCTYPE='LN'), Part 2-RM / 2-FC
# ============================================================================
def build_note(bnm1_dir: Path, reptmon: str, nowk: str,
               reptdate: date, runoffdt: date) -> pl.DataFrame:
    """
    PROC SORT DATA=BNM1.LOAN&REPTMON&NOWK OUT=NOTEX; BY ACCTNO;
    WHERE PAIDIND NOT IN ('P','C');

    DATA NOTE (KEEP=PART ITEM REMMTH AMOUNT);
    Key logic:
      - IF ACCTYPE = 'LN'  (no OD processing at all)
      - REMMTH reference is &RUNOFFDT throughout
      - Iteration exits when REMMTH > 1
      - DAYS>89 OR LOANSTAT!=1 -> REMMTH = 0.1  (not 13)
      - Outputs Part 2-RM or 2-FC (never Part 1)
    """
    loan_file = bnm1_dir / f"loan{reptmon}{nowk}.parquet"
    con = duckdb.connect()
    df  = con.execute(
        f"SELECT * FROM read_parquet('{loan_file}') "
        f"WHERE PAIDIND NOT IN ('P','C')"
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

        # IF ACCTYPE = 'LN'  (strict — no OD branch, no 34240 special case)
        if acctype != 'LN':
            continue

        freq_map = {'1': 1, '2': 3, '3': 6, '4': 12}
        freq = freq_map.get(payfreq, 0)
        prod = liqpfmt(product)
        ind  = custcd in ('77', '78', '95', '96')

        # Determine ITEM for non-FCY products
        if product not in FCY_SET:
            if ind:
                if   prod == 'HL': item = 'A2.05'
                elif prod == 'RC': item = 'A2.08A'
                else:              item = 'A2.08'
            else:
                if   prod in ('FL', 'HL'): item = 'A2.01'
                elif prod == 'RC':         item = 'A2.02'
                else:                      item = 'A2.04'
        else:
            # FCY products: item assigned later per PROD in output block
            item = ''

        # IF PRODUCT = 100 THEN ITEM = 'A2.05'  /* HARDCODE BY MAZNI */
        if product == 100:
            item = 'A2.05'

        # Days since last billing date (used for behavioural flag)
        days = 0
        if bldate and bldate > date(1900, 1, 1):
            days = (reptdate - bldate).days

        if exprdate is None:
            exprdate = reptdate  # fallback

        # IF EXPRDATE <= &RUNOFFDT THEN REMMTH = .
        if exprdate <= runoffdt:
            # REMMTH is missing — still output remaining balance
            amount = balance
            remmth: Optional[float] = None
            # DAYS>89 OR LOANSTAT!=1 -> REMMTH = 0.1
            if days > 89 or loanstat != 1:
                remmth = 0.1
            _append_ln_row(rows, product, prod, item, '2-RM', remmth, amount, FCY_SET)
            continue

        # IF EXPRDATE - &RUNOFFDT < 8 THEN REMMTH = 0.1
        if (exprdate - runoffdt).days < 8:
            amount = balance
            remmth_fin: Optional[float] = 0.1
            if days > 89 or loanstat != 1:
                remmth_fin = 0.1   # already 0.1; no change
            _append_ln_row(rows, product, prod, item, '2-RM', remmth_fin, amount, FCY_SET)
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

        # Iteration over billing dates within run-off window (REMMTH <= 1)
        while bldate and bldate <= exprdate:
            # IF BLDATE <= &RUNOFFDT THEN REMMTH = .
            if bldate <= runoffdt:
                remmth_iter: Optional[float] = None
            # ELSE IF BLDATE - &RUNOFFDT < 8 THEN REMMTH = 0.1
            elif (bldate - runoffdt).days < 8:
                remmth_iter = 0.1
            else:
                matdt       = bldate
                remmth_iter = calc_remmth(matdt, runoffdt)

            # IF REMMTH > 1 OR BLDATE = EXPRDATE THEN LEAVE
            if (remmth_iter is not None and remmth_iter > 1) or bldate == exprdate:
                break

            amount    = payamt
            balance  -= payamt

            # IF DAYS > 89 OR LOANSTAT ^= 1 THEN REMMTH = 0.1
            r_out = remmth_iter
            if days > 89 or loanstat != 1:
                r_out = 0.1

            _append_ln_row(rows, product, prod, item, '2-RM', r_out, amount, FCY_SET)

            bldate = nxtbldt(bldate, issdte, payfreq, freq)
            if bldate > exprdate or balance <= payamt:
                bldate = exprdate

        # Remaining balance — always output after loop
        amount = balance
        r_out  = None
        if days > 89 or loanstat != 1:
            r_out = 0.1
        _append_ln_row(rows, product, prod, item, '2-RM', r_out, amount, FCY_SET)

    if not rows:
        return pl.DataFrame(schema={'PART': pl.Utf8, 'ITEM': pl.Utf8,
                                    'REMMTH': pl.Float64, 'AMOUNT': pl.Float64})
    return pl.DataFrame(rows)


def _append_ln_row(rows, product, prod, item, base_part, remmth, amount, fcy_set):
    """
    Determine PART and ITEM, then append to rows.
    For FCY products: PART becomes '2-FC', ITEM set from PROD.
    For RM products:  PART stays '2-RM', ITEM unchanged.
    """
    if product in fcy_set:
        part     = base_part.replace('RM', 'FC')
        item_out = 'B1.01' if prod == 'FL' else ('B1.02' if prod == 'RC' else item)
    else:
        part     = base_part
        item_out = item
    rows.append({'PART': part, 'ITEM': item_out, 'REMMTH': remmth, 'AMOUNT': amount})

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
# REPORT OUTPUT HELPERS (ASA carriage control, 60 lines/page)
# ============================================================================
ASA_FIRST  = '1'   # new page
ASA_NORMAL = ' '   # single spacing
ASA_DOUBLE = '0'   # double spacing (skip one line before)

def _asa_line(cc: str, text: str) -> str:
    return cc + text + '\n'

def format_amount(val: float) -> str:
    """COMMA20.2 format; MISSING=0 means None/NaN renders as 0.00"""
    if val is None or (isinstance(val, float) and val != val):
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
      FORMAT REMMTH REMFMT. ITEM $ITEMF.;
      CLASS ITEM REMMTH; VAR AMOUNT;
      TABLE ITEM=' ', (REMMTH=' ' ALL='TOTAL')*(SUM=' '*AMOUNT=' '*F=COMMA20.2)
      / BOX=<box_label> RTS=45 CONDENSE;
      WHERE PART = '<part_filter>';
    OPTIONS MISSING=0: null REMMTH rendered as 0 before bucketing.
    Writes ASA carriage-control lines to out_fh.
    """
    sub = df.filter(pl.col("PART") == part_filter)
    if sub.is_empty():
        return

    # OPTIONS MISSING=0: replace null REMMTH with 0.0 for format bucketing
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

    # ---- Build NOTE (loans only) ----
    print("Building NOTE (loans — Part 2-RM/2-FC contractual run-off)...")
    note_raw = build_note(BNM1_DIR, reptmon, nowk, reptdate, runoffdt)
    note     = summarise(note_raw)

    # ---- Write reports ----
    report_file = OUTPUT_DIR / f"EIBWLQP2_{reptmon}{nowk}.txt"
    print(f"Writing report to {report_file}...")

    # OPTIONS NOCENTER NODATE NONUMBER MISSING=0
    with open(report_file, 'w', encoding='utf-8') as fh:

        # PART 2-RM: Behavioural Maturity Profile
        # NOTE: SAS title says "BEHAVIOURAL" but WHERE clause is PART='2-RM'
        #       and the data is pure contractual run-off loans — preserved as-is.
        write_tabulate_report(
            fh, note, title1, title2, title3,
            title4='BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE (PART 2-RM)',
            box_label='CORE (NON-TRADING) BANKING ACTIVITIES',
            part_filter='2-RM',
        )

        # PART 2-FC: Pure Contractual Maturity Profile (FCY Loans)
        write_tabulate_report(
            fh, note, title1, title2, title3,
            title4='BREAKDOWN BY PURE CONTRACTUAL MATURITY PROFILE (PART 2-FCY)',
            box_label='CORE (NON-TRADING) BANKING ACTIVITIES',
            part_filter='2-FC',
        )

    print(f"Report written: {report_file}")


if __name__ == '__main__':
    main()
