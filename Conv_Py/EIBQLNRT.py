#!/usr/bin/env python3
"""
Program    : EIBQLNRT.py
Invoked by : EIBQRPTS.py
Created    : 26-09-2001
SMR/Ref    : SMR A269
Objective  : Banking institutions exposure to movement in interest rate risks.
             Produces a tabulation of loan balances by repricing break-bucket
                (BREAK) and rate type (BIC / FIXED vs FLOATING) as at report date.

Output  : output/EIBQLNRT_<REPTMON><NOWK>.txt
          ASA carriage-control report, LS=132, PS=60.

Dependencies:
    - PBBLNFMT : format_lnrate (SAS: LNRATE.), format_odrate (SAS: ODRATE.)

OPTIONS: NOCENTER, YEARCUTOFF=1950, LS=132, ERROR=1
"""

from __future__ import annotations

import calendar
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl

# SAS: %INC PGM(PBBLNFMT);
from PBBLNFMT import format_lnrate, format_odrate

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
BASE_DIR   = Path(__file__).resolve().parent
INPUT_DIR  = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Runtime macro variables (set by orchestrator or overridden here for standalone use)
REPTMON:  str = "09"    # &REPTMON  — zero-padded month e.g. "09"
NOWK:     str = "4"     # &NOWK     — week suffix e.g. "4"
REPTDATE: int = 16709   # &REPTDATE — SAS date integer (days since 1960-01-01)
RDATE:    str = "30/09/05"  # &RDATE — DDMMYY8. string

# Input parquet paths
#   BNM.REPTDATE         -> input/BNM/REPTDATE.parquet
#   BNM.LOAN&REPTMON&NOWK -> input/BNM/LOAN<REPTMON><NOWK>.parquet
#   OD.OVERDFT            -> input/OD/OVERDFT.parquet
BNM_DIR      = INPUT_DIR / "BNM"
OD_DIR       = INPUT_DIR / "OD"

# Output report
REPORT_OUTPUT = OUTPUT_DIR / f"EIBQLNRT_{REPTMON}{NOWK}.txt"

# Page layout
PAGE_LENGTH = 60
LINE_WIDTH  = 132

# OPTIONS YEARCUTOFF=1950
YEARCUTOFF = 1950

# ---------------------------------------------------------------------------
# %LET MULTIER = (359,234,227,228,230,231,232,233,361)
# ---------------------------------------------------------------------------
MULTIER = {359, 234, 227, 228, 230, 231, 232, 233, 361}

# ---------------------------------------------------------------------------
# PROC FORMAT: $BRKFMT — break bucket labels
# ---------------------------------------------------------------------------
BRKFMT: dict[str, str] = {
    "A": "UP TO 1 WEEK",
    "B": "> 1 WEEK TO 1 MTH",
    "C": "> 1 TO 3 MTHS",
    "D": "> 3 TO 6 MTHS",
    "E": "> 6 TO 12 MTHS",
    "F": "> 1 TO 2 YEARS",
    "G": "> 2 TO 3 YEARS",
    "H": "> 3 TO 4 YEARS",
    "I": "> 4 TO 5 YEARS",
    "J": "> 5 TO 7 YEARS",
    "K": "> 7 TO 10 YEARS",
    "L": "> 10 TO 15 YEARS",
    "M": "OVER 15 YEARS",
}

# ---------------------------------------------------------------------------
# PROC FORMAT: $RATEFMT — rate type labels
# '30591','30592','30593' = 'FIXED RATE'
# '30595','30596','30597' = 'FLOATING RATE'
# ---------------------------------------------------------------------------
RATEFMT: dict[str, str] = {
    "30591": "FIXED RATE",
    "30592": "FIXED RATE",
    "30593": "FIXED RATE",
    "30595": "FLOATING RATE",
    "30596": "FLOATING RATE",
    "30597": "FLOATING RATE",
}

# Canonical BIC values for column ordering in the tabulation
BIC_FIXED    = "FIXED RATE"
BIC_FLOATING = "FLOATING RATE"

# Ordered break keys for row ordering
BREAK_ORDER = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M"]

# ---------------------------------------------------------------------------
# SAS date helpers  (YEARCUTOFF=1950)
# ---------------------------------------------------------------------------
SAS_EPOCH = date(1960, 1, 1)


def sas_date_to_python(sas_days: int) -> date:
    """Convert SAS date integer (days since 1960-01-01) to Python date."""
    return SAS_EPOCH + timedelta(days=int(sas_days))


def python_date_to_sas(d: date) -> int:
    """Convert Python date to SAS date integer."""
    return (d - SAS_EPOCH).days


def parse_mmddyy8(s: str, yearcutoff: int = YEARCUTOFF) -> Optional[date]:
    """
    Parse MMDDYY8. string (first 8 chars of Z11 LMTENDDT).
    e.g. '06302005' -> date(2005, 6, 30).
    Applies YEARCUTOFF=1950 for 2-digit years:
        50-99 -> 1950-1999, 00-49 -> 2000-2049
    """
    s = s.strip()
    if len(s) < 6:
        return None
    try:
        mm = int(s[0:2])
        dd = int(s[2:4])
        yy_raw = s[4:]
        if len(yy_raw) == 2:
            yy = int(yy_raw)
            century = (yearcutoff // 100) * 100
            if yy >= (yearcutoff % 100):
                yyyy = century + yy
            else:
                yyyy = century + 100 + yy
        else:
            yyyy = int(yy_raw[:4])
        return date(yyyy, mm, dd)
    except (ValueError, IndexError):
        return None


# ---------------------------------------------------------------------------
# DATA _NULL_ — resolve macro variables from BNM.REPTDATE
# ---------------------------------------------------------------------------
def resolve_reptdate(bnm_dir: Path) -> dict:
    """
    DATA _NULL_;
    SET BNM.REPTDATE;
    SELECT(DAY(REPTDATE)) WHEN(8)/WHEN(15)/WHEN(22)/OTHERWISE ...
    CALL SYMPUT(...)

    Returns dict with REPTDATE (date), REPTMON, NOWK, NOWK1, REPTMON1,
    REPTYEAR, REPTDAY, RDATE (DDMMYY8.), SDATE (DDMMYY8.), BTYPE.
    """
    parquet = bnm_dir / "REPTDATE.parquet"
    con = duckdb.connect()
    row = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{parquet}') LIMIT 1"
    ).fetchone()
    con.close()

    if row is None:
        raise ValueError(f"No REPTDATE record in {parquet}")

    reptdate = sas_date_to_python(int(row[0]))
    day = reptdate.day
    mm  = reptdate.month
    yr  = reptdate.year

    if day == 8:
        sdd = 1;  wk = "1"; wk1 = "4"
    elif day == 15:
        sdd = 9;  wk = "2"; wk1 = "1"
    elif day == 22:
        sdd = 16; wk = "3"; wk1 = "2"
    else:
        sdd = 23; wk = "4"; wk1 = "3"

    mm1 = (mm - 1) if (wk == "1" and mm > 1) else (12 if (wk == "1" and mm == 1) else mm)
    last = calendar.monthrange(yr, mm)[1]
    sdate = date(yr, mm, min(sdd, last))

    return {
        "REPTDATE":  reptdate,
        "REPTDATE_SAS": python_date_to_sas(reptdate),
        "NOWK":      wk,
        "NOWK1":     wk1,
        "REPTMON":   f"{mm:02d}",
        "REPTMON1":  f"{mm1:02d}",
        "REPTYEAR":  str(yr),
        "REPTDAY":   f"{day:02d}",
        "RDATE":     reptdate.strftime("%d/%m/%y"),   # DDMMYY8.
        "SDATE":     sdate.strftime("%d/%m/%y"),
        "BTYPE":     "PBB",
    }


# ---------------------------------------------------------------------------
# %MACRO DCLVAR — days-per-month arrays (leap-year aware)
# ---------------------------------------------------------------------------
def _make_days_arr(yr: int) -> list[int]:
    """Return list of days-per-month [1..12], adjusting Feb for leap year."""
    arr = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if yr % 4 == 0:
        arr[1] = 29
    return arr


# ---------------------------------------------------------------------------
# %MACRO REMMTH — compute ISSMTH (elapsed months from issue) and
#                             REMMTH (remaining months to expiry)
# ---------------------------------------------------------------------------
def calc_remmth(
    reptdate: date,
    issdte: date,
    exprdate: date,
) -> tuple[float, float]:
    """
    Python equivalent of %REMMTH macro.

    Returns (issmth, remmth):
        issmth  = months elapsed from ISSDTE to REPTDATE (issue seasoning)
        remmth  = months remaining from REPTDATE to EXPRDATE

    End-of-month alignment rule (both issue and maturity):
        IF DAY(date) == last_day_of_month(date) THEN day = RPDAYS(RPMTH)
    """
    rpyr  = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day
    rpdays = _make_days_arr(rpyr)
    rp_mth_days = rpdays[rpmth - 1]

    # ---- Issue seasoning (ISSMTH) ----
    isdays = _make_days_arr(issdte.year)
    isday  = issdte.day
    if isday == isdays[issdte.month - 1]:   # end-of-month alignment
        isday = rp_mth_days
    issy = rpyr - issdte.year
    issm = rpmth - issdte.month
    issd = rpday - isday
    issmth = issy * 12 + issm + issd / isdays[issdte.month - 1]

    # ---- Remaining maturity (REMMTH) ----
    mddays = _make_days_arr(exprdate.year)
    mdday  = exprdate.day
    if mdday == mddays[exprdate.month - 1]:  # end-of-month alignment
        mdday = rp_mth_days
    remy = exprdate.year  - rpyr
    remm = exprdate.month - rpmth
    remd = mdday - rpday
    remmth = remy * 12 + remm + remd / rp_mth_days

    return issmth, remmth


# ---------------------------------------------------------------------------
# DATA OD — parse LMTENDDT from OD.OVERDFT
# ---------------------------------------------------------------------------
def build_od(od_dir: Path) -> pl.DataFrame:
    """
    DATA OD;
    SET OD.OVERDFT;
    WHERE LMTENDDT NE . AND LMTENDDT > 0;
    LMTEND = INPUT(SUBSTR(PUT(LMTENDDT,Z11.),1,8), MMDDYY8.);
    IF LMTEND = . THEN DELETE;

    PROC SORT NODUPKEYS; BY ACCTNO; KEEP ACCTNO LMTEND LMTENDDT RISKCODE;

    Returns DataFrame with columns: ACCTNO, LMTEND (SAS date int),
    LMTENDDT (original), RISKCODE.
    """
    parquet = od_dir / "OVERDFT.parquet"
    con = duckdb.connect()
    df  = con.execute(
        f"SELECT * FROM read_parquet('{parquet}') "
        f"WHERE LMTENDDT IS NOT NULL AND LMTENDDT > 0"
    ).pl()
    con.close()

    records: list[dict] = []
    for row in df.iter_rows(named=True):
        lmtenddt = int(row["LMTENDDT"])
        # SUBSTR(PUT(LMTENDDT,Z11.),1,8) -> first 8 chars of zero-padded 11-digit int
        z11 = str(lmtenddt).zfill(11)
        lmtend_date = parse_mmddyy8(z11[:8])
        if lmtend_date is None:
            continue   # IF LMTEND = . THEN DELETE
        records.append({
            "ACCTNO":   str(row.get("ACCTNO") or "").strip(),
            "LMTEND":   python_date_to_sas(lmtend_date),
            "LMTENDDT": lmtenddt,
            "RISKCODE": row.get("RISKCODE"),
        })

    if not records:
        return pl.DataFrame({
            "ACCTNO":   pl.Series([], dtype=pl.Utf8),
            "LMTEND":   pl.Series([], dtype=pl.Int64),
            "LMTENDDT": pl.Series([], dtype=pl.Int64),
            "RISKCODE": pl.Series([], dtype=pl.Float64),
        })

    df_out = pl.DataFrame(records)
    # PROC SORT NODUPKEYS BY ACCTNO — keep first occurrence per ACCTNO
    return df_out.unique(subset=["ACCTNO"], keep="first").sort("ACCTNO")


# ---------------------------------------------------------------------------
# DATA LOAN — merge BNM.LOAN with OD
# ---------------------------------------------------------------------------
def build_loan(bnm_dir: Path, reptmon: str, nowk: str, od: pl.DataFrame) -> pl.DataFrame:
    """
    PROC SORT DATA=BNM.LOAN&REPTMON&NOWK OUT=LOAN; BY ACCTNO;
    DATA LOAN&REPTMON&NOWK;
    MERGE LOAN(IN=A) OD(IN=B); BY ACCTNO; IF A;

    Left-join LOAN onto OD; keep all LOAN rows (IF A).
    """
    loan_parquet = bnm_dir / f"LOAN{reptmon}{nowk}.parquet"
    con = duckdb.connect()
    loan = con.execute(f"SELECT * FROM read_parquet('{loan_parquet}')").pl()
    con.close()

    # Normalise ACCTNO type for join
    loan = loan.with_columns(pl.col("ACCTNO").cast(pl.Utf8).str.strip_chars())
    od   = od.with_columns(pl.col("ACCTNO").cast(pl.Utf8).str.strip_chars())

    return loan.join(od, on="ACCTNO", how="left")


# ---------------------------------------------------------------------------
# DATA START — assign BIC, EXPRDATE, BREAK bucket
# ---------------------------------------------------------------------------
def build_start(loan: pl.DataFrame, reptdate: date, reptdate_sas: int) -> pl.DataFrame:
    """
    DATA START;
    SET LOAN&REPTMON&NOWK;
    WHERE SUBSTR(PRODCD,1,2) IN ('34','54');

    Assigns BIC via LNRATE./ODRATE., computes REMMTH and ISSMTH,
    then buckets into BREAK codes A-M.

    IF BIC='30595' THEN BREAK='B'   (floating rate override)
    IF PRODUCT IN &MULTIER ...      (multi-tiered: bucket by ISSMTH)
    IF RISKRTE IN (1,2,3,4) THEN BREAK='A'  (variable rate override)
    """
    # Filter: SUBSTR(PRODCD,1,2) IN ('34','54')
    loan = loan.filter(
        pl.col("PRODCD").cast(pl.Utf8).str.slice(0, 2).is_in(["34", "54"])
    )

    output_rows: list[dict] = []

    for row in loan.iter_rows(named=True):
        product  = int(row.get("PRODUCT") or 0)
        acctype  = (row.get("ACCTYPE") or "").strip()
        balance  = row.get("BALANCE") or 0.0

        # BIC and EXPRDATE assignment
        riskrte: Optional[float] = None
        exprdate_sas: Optional[int] = None

        if acctype == "LN":
            bic = format_lnrate(product)
            # EXPRDATE comes from the loan record's own expiry date field
            exprdate_val = row.get("EXPRDATE")
            if exprdate_val is not None:
                exprdate_sas = int(exprdate_val)
        else:
            # OD account
            bic = format_odrate(product)
            lmtend = row.get("LMTEND")
            exprdate_sas = int(lmtend) if lmtend is not None else None
            riskrte = row.get("RISKCODE")
            if riskrte is not None:
                try:
                    riskrte = float(riskrte)
                except (ValueError, TypeError):
                    riskrte = None

        # Compute REMMTH and ISSMTH if both dates available
        issdte_val  = row.get("ISSDTE")
        remmth:  float = 0.0
        issmth:  float = 0.0

        if exprdate_sas is not None and issdte_val is not None:
            try:
                exprdate = sas_date_to_python(int(exprdate_sas))
                issdte   = sas_date_to_python(int(issdte_val))
                issmth, remmth = calc_remmth(reptdate, issdte, exprdate)
            except (ValueError, OverflowError):
                remmth  = 0.0
                issmth  = 0.0

        # ---- Assign BREAK bucket ----
        break_code: str = "A"

        if remmth > 12:
            if   remmth <= 24:  break_code = "F"
            elif remmth <= 36:  break_code = "G"
            elif remmth <= 48:  break_code = "H"
            elif remmth <= 60:  break_code = "I"
            elif remmth <= 84:  break_code = "J"
            elif remmth <= 120: break_code = "K"
            elif remmth <= 180: break_code = "L"
            else:               break_code = "M"
        else:
            if   remmth > 6:    break_code = "E"
            elif remmth > 3:    break_code = "D"
            elif remmth > 1:    break_code = "C"
            else:
                # IF EXPRDATE - &REPTDATE > 7 THEN BREAK='B'; ELSE BREAK='A'
                if exprdate_sas is not None and (exprdate_sas - reptdate_sas) > 7:
                    break_code = "B"
                else:
                    break_code = "A"

        # Override 1: IF BIC='30595' THEN BREAK='B'
        if bic == "30595":
            break_code = "B"

        # Override 2: MULTIER products
        if product in MULTIER:
            if issmth <= 24:
                break_code = "F"
            else:
                break_code = "B"

        # Override 3: IF RISKRTE IN (1,2,3,4) THEN BREAK='A'
        if riskrte is not None and riskrte in (1.0, 2.0, 3.0, 4.0):
            break_code = "A"

        output_rows.append({
            "BIC":     bic,
            "BREAK":   break_code,
            "ACCTNO":  (row.get("ACCTNO") or ""),
            "BALANCE": balance,
        })

    if not output_rows:
        return pl.DataFrame({
            "BIC":     pl.Series([], dtype=pl.Utf8),
            "BREAK":   pl.Series([], dtype=pl.Utf8),
            "ACCTNO":  pl.Series([], dtype=pl.Utf8),
            "BALANCE": pl.Series([], dtype=pl.Float64),
        })

    return pl.DataFrame(output_rows)


# ---------------------------------------------------------------------------
# PROC TABULATE — cross-tabulate BALANCE by BREAK x BIC
# ---------------------------------------------------------------------------
def tabulate(start: pl.DataFrame) -> dict[str, dict[str, float]]:
    """
    PROC TABULATE DATA=START MISSING;
    FORMAT BREAK $BRKFMT. BIC $RATEFMT.;
    CLASS BREAK BIC;
    VAR BALANCE;
    TABLE (BREAK=' ' ALL='TOTAL'), BIC=' '*SUM=' '*BALANCE=' '*F=COMMA18.2;

    Returns nested dict: {break_label -> {rate_label -> sum_balance}},
    plus a 'TOTAL' key for grand totals across all BREAKs.
    Apply RATEFMT to BIC values and BRKFMT to BREAK values before aggregating.
    """
    # Map BIC codes to rate labels
    start = start.with_columns([
        pl.col("BIC").map_elements(
            lambda x: RATEFMT.get(x, x), return_dtype=pl.Utf8
        ).alias("RATE_LABEL"),
        pl.col("BREAK").map_elements(
            lambda x: BRKFMT.get(x, x), return_dtype=pl.Utf8
        ).alias("BREAK_LABEL"),
    ])

    # Summarise by BREAK_LABEL x RATE_LABEL
    summary = (
        start
        .group_by(["BREAK_LABEL", "RATE_LABEL"])
        .agg(pl.col("BALANCE").sum().alias("BALANCE"))
    )

    # Build nested dict
    table: dict[str, dict[str, float]] = {}
    for row in summary.iter_rows(named=True):
        blabel = row["BREAK_LABEL"]
        rlabel = row["RATE_LABEL"]
        bal    = row["BALANCE"] or 0.0
        table.setdefault(blabel, {})[rlabel] = bal

    return table


# ---------------------------------------------------------------------------
# Report renderer
# ---------------------------------------------------------------------------
def render_report(
    table: dict[str, dict[str, float]],
    rdate_str: str,
) -> str:
    """
    Renders the PROC TABULATE output as an ASA carriage-control text report.

    Layout:
        TITLE1 'REPORT : EIBQLNRT            PUBLIC BANK BERHAD'
        TITLE2 'LOANS OUTSTANDING AS AT ' &RDATE

    Columns: BREAK description | FIXED RATE | FLOATING RATE | ROW TOTAL
    Rows   : each BREAK bucket in order A-M, then TOTAL row.
    FORMAT BALANCE COMMA18.2
    """
    TITLE1 = "REPORT : EIBQLNRT            PUBLIC BANK BERHAD"
    TITLE2 = f"LOANS OUTSTANDING AS AT {rdate_str}"

    COL_BREAK   = 25
    COL_FIXED   = 20
    COL_FLOAT   = 20
    COL_TOTAL   = 20

    H_BREAK = "BREAK PERIOD"
    H_FIXED = "FIXED RATE"
    H_FLOAT = "FLOATING RATE"
    H_TOTAL = "TOTAL"

    def _fmt(val: float) -> str:
        return f"{val:,.2f}".rjust(COL_FIXED)

    def _hdr() -> list[str]:
        sep = "-" * (COL_BREAK + 2 + COL_FIXED + 2 + COL_FLOAT + 2 + COL_TOTAL)
        return [
            f"1{TITLE1}",
            f" {TITLE2}",
            f" ",
            f" {H_BREAK:<{COL_BREAK}}  {H_FIXED:>{COL_FIXED}}  "
            f"{H_FLOAT:>{COL_FLOAT}}  {H_TOTAL:>{COL_TOTAL}}",
            f" {sep}",
        ]

    lines: list[str] = _hdr()
    page_lines = len(lines)
    DATA_PER_PAGE = PAGE_LENGTH - len(_hdr())

    grand_fixed   = 0.0
    grand_float   = 0.0
    grand_total   = 0.0

    for break_code in BREAK_ORDER:
        break_label = BRKFMT.get(break_code, break_code)
        row_data    = table.get(break_label, {})
        fixed_bal   = row_data.get(BIC_FIXED,    0.0)
        float_bal   = row_data.get(BIC_FLOATING, 0.0)
        row_total   = fixed_bal + float_bal

        grand_fixed  += fixed_bal
        grand_float  += float_bal
        grand_total  += row_total

        # New page if needed
        if page_lines >= PAGE_LENGTH:
            for h in _hdr():
                lines.append(h)
            page_lines = len(_hdr())

        data_line = (
            f" {break_label:<{COL_BREAK}}  "
            f"{_fmt(fixed_bal)}  "
            f"{_fmt(float_bal)}  "
            f"{_fmt(row_total)}"
        )
        lines.append(data_line)
        page_lines += 1

    # Grand total row (ALL='TOTAL')
    sep = "-" * (COL_BREAK + 2 + COL_FIXED + 2 + COL_FLOAT + 2 + COL_TOTAL)
    lines.append(f" {sep}")
    lines.append(
        f" {'TOTAL':<{COL_BREAK}}  "
        f"{_fmt(grand_fixed)}  "
        f"{_fmt(grand_float)}  "
        f"{_fmt(grand_total)}"
    )

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    # Resolve macro variables from BNM.REPTDATE
    macros = resolve_reptdate(BNM_DIR)

    # Allow orchestrator-injected values to take precedence
    reptmon      = REPTMON  if REPTMON  != "09"    else macros["REPTMON"]
    nowk         = NOWK     if NOWK     != "4"     else macros["NOWK"]
    rdate_str    = RDATE    if RDATE    != "30/09/05" else macros["RDATE"]
    reptdate     = sas_date_to_python(REPTDATE) if REPTDATE != 16709 \
                   else macros["REPTDATE"]
    reptdate_sas = python_date_to_sas(reptdate)

    print(f"[EIBQLNRT] REPTDATE={reptdate.isoformat()}  REPTMON={reptmon}  NOWK={nowk}")

    # Build OD expiry lookup from OD.OVERDFT
    od = build_od(OD_DIR)

    # Merge BNM.LOAN with OD
    loan = build_loan(BNM_DIR, reptmon, nowk, od)

    # Assign BIC, BREAK codes
    start = build_start(loan, reptdate, reptdate_sas)

    print(f"[EIBQLNRT] START rows after filter+assignment: {start.height}")

    # Tabulate: BALANCE by BREAK x BIC (rate type)
    table = tabulate(start)

    # Render ASA report
    report_text = render_report(table, rdate_str)

    output_path = OUTPUT_DIR / f"EIBQLNRT_{reptmon}{nowk}.txt"
    output_path.write_text(report_text, encoding="ascii", errors="replace")
    print(f"[EIBQLNRT] Report written: {output_path}")


if __name__ == "__main__":
    main()
