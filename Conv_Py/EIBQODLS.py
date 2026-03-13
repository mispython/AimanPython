#!/usr/bin/env python3
"""
Program : EIBQODLS.py (EIBQOD01)
Schedule: DPEX2000 => DALMLIMT => EIBQOD01
Date    : 29.06.99
Report  : LIST OF OD ACCOUNTS WITH EXPIRY DATES
          Quarterly listing of OD accounts due for review in next 6 months.

Output  : Two identical reports written to:
            1. output/ODEXLIST.txt  (equivalent of PROC PRINTTO PRINT=ODEXLIST)
            2. output/PRINT.txt     (equivalent of PROC PRINTTO PRINT=PRINT)

          Each report is an ASA carriage-control text file broken by BRANCH,
          with subtotals per branch and grand totals at the end.

Dependencies:
    None. No %INC PGM() references in the SAS source.
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
BASE_DIR    = Path(__file__).resolve().parent
INPUT_DIR   = BASE_DIR / "input"
OUTPUT_DIR  = BASE_DIR / "output"
INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input parquet paths
#   //LOAN    DD DSN=SAP.PBB.MNILN(0)    -> LOAN.REPTDATE, LOAN.CURRENT
#   //DEPOSIT DD DSN=SAP.PBB.DEPOSIT(0)  -> DEPOSIT.OVERDFT
REPTDATE_PARQUET = INPUT_DIR / "LOAN"    / "REPTDATE.parquet"
CURRENT_PARQUET  = INPUT_DIR / "LOAN"    / "CURRENT.parquet"
OVERDFT_PARQUET  = INPUT_DIR / "DEPOSIT" / "OVERDFT.parquet"

# Output report paths
ODEXLIST_TXT = OUTPUT_DIR / "ODEXLIST.txt"
PRINT_TXT    = OUTPUT_DIR / "PRINT.txt"

# Page layout for ASA report (OPTIONS PS=60 default)
PAGE_LENGTH = 60

# OPTIONS YEARCUTOFF=1930
# 2-digit years 00-29 -> 2000-2029, 30-99 -> 1930-1999
YEARCUTOFF = 1930

# ---------------------------------------------------------------------------
# SAS date helpers
# ---------------------------------------------------------------------------
SAS_EPOCH = date(1960, 1, 1)


def sas_date_to_python(sas_days: int) -> date:
    """Convert SAS date (days since 1960-01-01) to Python date."""
    return SAS_EPOCH + timedelta(days=int(sas_days))


def python_date_to_sas(d: date) -> int:
    """Convert Python date to SAS date integer."""
    return (d - SAS_EPOCH).days


def parse_mmddyy8(s: str, yearcutoff: int = YEARCUTOFF) -> Optional[date]:
    """
    Parse MMDDYY8. format string (e.g. '06302005' -> 2005-06-30).
    Applies SAS YEARCUTOFF=1930 for 2-digit year interpretation:
        00-29  -> 2000-2029
        30-99  -> 1930-1999
    """
    s = s.strip()
    if len(s) < 6:
        return None
    try:
        mm = int(s[0:2])
        dd = int(s[2:4])
        yy_str = s[4:6] if len(s) == 6 else s[4:8]
        if len(yy_str) == 2:
            yy = int(yy_str)
            century = (yearcutoff // 100) * 100
            if yy >= (yearcutoff % 100):
                yyyy = century + yy
            else:
                yyyy = century + 100 + yy
        else:
            yyyy = int(yy_str)
        return date(yyyy, mm, dd)
    except (ValueError, IndexError):
        return None


def format_ddmmyy10(d: Optional[date]) -> str:
    """Format date as DD/MM/YYYY (SAS DDMMYY10.)."""
    if d is None:
        return " " * 10
    return d.strftime("%d/%m/%Y")


def format_comma18(value: Optional[float], decimals: int = 0) -> str:
    """Format numeric as COMMA18. or COMMA18.2 (right-aligned, 18 chars)."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return " " * 18
    if decimals == 0:
        s = f"{round(value):,}"
    else:
        s = f"{value:,.{decimals}f}"
    return s.rjust(18)


def format_z3(branch: Optional[int]) -> str:
    """Format branch as Z3. (zero-padded 3-digit)."""
    if branch is None:
        return "000"
    return f"{int(branch):03d}"


# ---------------------------------------------------------------------------
# Step 1 — DATA _NULL_: resolve macro variables from LOAN.REPTDATE
# ---------------------------------------------------------------------------
def resolve_reptdate(parquet_path: Path) -> dict:
    """
    DATA _NULL_;
    SET LOAN.REPTDATE;
    CALL SYMPUT('RDATE',  PUT(REPTDATE, DDMMYY10.));
    CALL SYMPUT('RMONTH', PUT(REPTDATE, MONTH2.));
    CALL SYMPUT('RYEAR',  PUT(REPTDATE, YEAR4.));
    CALL SYMPUT('RDAY',   PUT(REPTDATE, DAY2.));
    """
    con = duckdb.connect()
    row = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{parquet_path}') LIMIT 1"
    ).fetchone()
    con.close()

    if row is None:
        raise ValueError(f"No REPTDATE record in {parquet_path}")

    reptdate = sas_date_to_python(int(row[0]))

    return {
        "REPTDATE": reptdate,
        "RDATE":    format_ddmmyy10(reptdate),      # DD/MM/YYYY
        "RMONTH":   reptdate.month,                  # integer
        "RYEAR":    reptdate.year,                   # integer
        "RDAY":     reptdate.day,                    # integer
    }


# ---------------------------------------------------------------------------
# LMTCOLL exclusion set
# ---------------------------------------------------------------------------
_EXCL_LMTCOLL = {
    "00 07", "00 09", "00 14", "07 00", "07 07", "07 09",
    "07 14", "09 00", "09 07", "09 09", "09 14", "14 00",
    "14 07", "14 09", "14 14", "07   ", "09   ", "14   ",
    "   07", "   09", "   14",
}

_EXCL_PRODUCT = {116, 119, 120, 125, 154, 155}

# LMTENDDT sentinel values that mean "no expiry"
_NO_EXPIRY = {999_999_999, 99_999_999_999, 0}


# ---------------------------------------------------------------------------
# Step 2 — DATA OD: filter and derive expiry fields
# ---------------------------------------------------------------------------
def build_od(overdft_path: Path, macros: dict) -> pl.DataFrame:
    """
    DATA OD;
    KEEP BRANCH ACCTNO NAME APPRLIMT LMTAMT LMTID LMTCOLL EXPRDTE
         EXPIRMH EXYEAR EXDAY EXMTH ACCTNO1 LMTINCR;
    SET DEPOSIT.OVERDFT;

    Exclusion rules:
        - ODSTATUS IN ('RI')
        - PRODUCT IN (116,119,120,125,154,155)
        - LMTCOLL in exclusion list
        - RISKCODE >= 2

    Derives expiry fields from LMTENDDT (11-digit integer, MMDDYYYY stored
    in the first 8 significant digits: positions 1-2=MM, 3-4=DD, 5-8=YYYY).

    ACCTNO1 is decorated with '#' (product 112) and/or '*' (LMTINCR='R').

    EXPIRMH=1 flags accounts due for review within 6 months.
    """
    ryear  = macros["RYEAR"]
    rmonth = macros["RMONTH"]

    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{overdft_path}')").pl()
    con.close()

    output_rows: list[dict] = []

    for row in df.iter_rows(named=True):
        # ---- Exclusion filters ----
        odstatus = (row.get("ODSTATUS") or "").strip()
        if odstatus == "RI":
            continue

        product = row.get("PRODUCT")
        if product is not None and int(product) in _EXCL_PRODUCT:
            continue

        lmtcoll = (row.get("LMTCOLL") or "").strip()
        # Match on the raw (possibly padded) value as well as stripped
        lmtcoll_raw = (row.get("LMTCOLL") or "")
        if lmtcoll_raw in _EXCL_LMTCOLL or lmtcoll in _EXCL_LMTCOLL:
            continue

        riskcode = row.get("RISKCODE")
        if riskcode is not None and float(riskcode) >= 2:
            continue

        # ---- Derived fields ----
        acctno  = (row.get("ACCTNO")  or "").strip()
        name    = (row.get("NAME")    or "").strip()
        branch  = row.get("BRANCH")
        apprlimt = row.get("APPRLIMT") or 0.0
        lmtamt   = row.get("LMTAMT")   or 0.0
        lmtid    = (row.get("LMTID")   or "").strip()
        lmtincr  = (row.get("LMTINCR") or "").strip()

        exprdte: Optional[date] = None
        exyear: Optional[str]  = None
        exmth:  Optional[str]  = None
        exday:  Optional[str]  = None

        lmtenddt = row.get("LMTENDDT")
        if lmtenddt is not None:
            lmtenddt_int = int(lmtenddt)
            if lmtenddt_int not in _NO_EXPIRY:
                # LMTENDDT stored as Z11. (zero-padded 11-digit integer)
                # First 8 chars of Z11 representation: MMDDYYYY
                lmtenddt_str = str(lmtenddt_int).zfill(11)
                mm_str   = lmtenddt_str[0:2]   # SUBSTR(...,1,2)
                dd_str   = lmtenddt_str[2:4]   # SUBSTR(...,3,2)
                yyyy_str = lmtenddt_str[4:8]   # SUBSTR(...,5,4)

                # EXPRDTE = INPUT(SUBSTR(PUT(LMTENDDT,Z11.),1,8), MMDDYY8.)
                # The first 8 chars of Z11 are MMDDYYYY -> parse as MMDDYY8.
                exprdte = parse_mmddyy8(mm_str + dd_str + yyyy_str)
                exyear  = yyyy_str
                exmth   = mm_str
                exday   = dd_str

        # ---- ACCTNO1 decoration ----
        prod_int = int(product) if product is not None else 0
        is_112   = prod_int == 112
        is_r     = lmtincr == "R"

        if is_112 or is_r:
            if is_112 and is_r:
                prefix = "#*"
            elif is_112:
                prefix = "# "
            else:
                prefix = "* "
            acctno1 = (prefix + acctno).lstrip()
        else:
            acctno1 = acctno

        # ---- EXPIRMH logic ----
        # Accounts expiring within the next ~6 months are flagged EXPIRMH=1.
        # EXPIRMH=. (missing) if already in the current month.
        expirmh: Optional[int] = None

        ex_year_int = int(exyear)  if exyear else None
        ex_mth_int  = int(exmth)   if exmth  else None

        if ex_year_int is not None and ex_mth_int is not None:
            if (ex_year_int - ryear) == 1:
                # Expiry is next year
                # IF (&RMONTH + 6 >= 12) THEN RMONTH = &RMONTH - 6;
                rmonth_adj = rmonth - 6 if (rmonth + 6 >= 12) else rmonth
                if rmonth_adj >= ex_mth_int:
                    expirmh = 1

            if ryear == ex_year_int:
                # Expiry is this year
                if (rmonth + 6) >= ex_mth_int:
                    expirmh = 1
                if rmonth == ex_mth_int:
                    expirmh = None   # SAS missing (.)

        output_rows.append({
            "BRANCH":   branch,
            "ACCTNO":   acctno,
            "NAME":     name,
            "APPRLIMT": apprlimt,
            "LMTAMT":   lmtamt,
            "LMTID":    lmtid,
            "LMTCOLL":  lmtcoll_raw,
            "EXPRDTE":  python_date_to_sas(exprdte) if exprdte else None,
            "EXPIRMH":  expirmh,
            "EXYEAR":   exyear,
            "EXDAY":    exday,
            "EXMTH":    exmth,
            "ACCTNO1":  acctno1,
            "LMTINCR":  lmtincr,
        })

    if not output_rows:
        return pl.DataFrame({
            "BRANCH":   pl.Series([], dtype=pl.Int64),
            "ACCTNO":   pl.Series([], dtype=pl.Utf8),
            "NAME":     pl.Series([], dtype=pl.Utf8),
            "APPRLIMT": pl.Series([], dtype=pl.Float64),
            "LMTAMT":   pl.Series([], dtype=pl.Float64),
            "LMTID":    pl.Series([], dtype=pl.Utf8),
            "LMTCOLL":  pl.Series([], dtype=pl.Utf8),
            "EXPRDTE":  pl.Series([], dtype=pl.Int64),
            "EXPIRMH":  pl.Series([], dtype=pl.Int64),
            "EXYEAR":   pl.Series([], dtype=pl.Utf8),
            "EXDAY":    pl.Series([], dtype=pl.Utf8),
            "EXMTH":    pl.Series([], dtype=pl.Utf8),
            "ACCTNO1":  pl.Series([], dtype=pl.Utf8),
            "LMTINCR":  pl.Series([], dtype=pl.Utf8),
        })

    return pl.DataFrame(output_rows)


# ---------------------------------------------------------------------------
# Step 3 — PROC SORT BY ACCTNO; merge with LOAN.CURRENT for LEDGBAL
# ---------------------------------------------------------------------------
def merge_current(od: pl.DataFrame, current_path: Path) -> pl.DataFrame:
    """
    PROC SORT DATA=LOAN.CURRENT OUT=CURRENT (KEEP=ACCTNO LEDGBAL); BY ACCTNO;
    DATA OD; MERGE OD(IN=A) CURRENT; BY ACCTNO; IF A;

    Left-join OD onto CURRENT to attach LEDGBAL; keep all OD rows (IF A).
    """
    con = duckdb.connect()
    current = con.execute(
        f"SELECT ACCTNO, LEDGBAL FROM read_parquet('{current_path}')"
    ).pl()
    con.close()

    od_joined = od.join(current, on="ACCTNO", how="left")
    return od_joined


# ---------------------------------------------------------------------------
# Step 4 — PROC SORT BY BRANCH EXPRDTE; filter WHERE EXPIRMH=1
# ---------------------------------------------------------------------------
def build_odrpt(od: pl.DataFrame) -> pl.DataFrame:
    """
    PROC SORT DATA=OD; BY BRANCH EXPRDTE;
    DATA ODRPT; SET OD; WHERE EXPIRMH=1;
    """
    od_sorted = od.sort(["BRANCH", "EXPRDTE"], nulls_last=True)
    return od_sorted.filter(pl.col("EXPIRMH") == 1)


# ---------------------------------------------------------------------------
# Report renderer — PROC PRINT LABEL NOOBS with BY BRANCH / PAGEBY / SUMBY
# ---------------------------------------------------------------------------
def _render_report(odrpt: pl.DataFrame, rdate_str: str) -> str:
    """
    PROC PRINT LABEL NOOBS;
    FORMAT BRANCH Z3. APPRLIMT LMTAMT COMMA18. LEDGBAL COMMA18.2
           EXPRDTE DDMMYY10.;
    LABEL ACCTNO1='ACCOUNT NUMBER' APPRLIMT='APPROVED LIMIT'
          LMTAMT='LIMIT AMOUNT'   LEDGBAL='BALANCE'
          LMTID='LIMIT ID'        LMTCOLL='COLL ID'
          EXPRDTE='EXPIRY DATE';
    VAR ACCTNO1 NAME APPRLIMT LMTAMT LEDGBAL LMTID LMTCOLL EXPRDTE;
    BY BRANCH;
    PAGEBY BRANCH;    <- new page per branch
    SUMBY BRANCH;     <- branch subtotals
    SUM APPRLIMT LMTAMT LEDGBAL;
    TITLE1 'PUBLIC BANK BERHAD';
    TITLE2 'QUARTERLY LISTING OF OD ACCOUNTS';
    TITLE3 'DUE FOR REVIEW NEXT 6 MONTHS ' &RDATE;
    FOOTNOTE1 'NOTE: ("*"  --  OD UNDER REDUCTION PROGRAM';
    FOOTNOTE2 '       "#"  --  OD UNDER NPGS)';

    Column widths chosen to fit LS=132.
    """
    TITLE1 = "PUBLIC BANK BERHAD"
    TITLE2 = "QUARTERLY LISTING OF OD ACCOUNTS "
    TITLE3 = f"DUE FOR REVIEW NEXT 6 MONTHS {rdate_str}"
    FN1    = 'NOTE: ("*"  --  OD UNDER REDUCTION PROGRAM'
    FN2    = '       "#"  --  OD UNDER NPGS)'

    # Column headers (labels)
    H_ACCTNO1  = "ACCOUNT NUMBER"
    H_NAME     = "NAME"
    H_APPRLIMT = "APPROVED LIMIT"
    H_LMTAMT   = "LIMIT AMOUNT"
    H_LEDGBAL  = "BALANCE"
    H_LMTID    = "LIMIT ID"
    H_LMTCOLL  = "COLL ID"
    H_EXPRDTE  = "EXPIRY DATE"

    # Column widths
    W_ACCTNO1  = 20
    W_NAME     = 30
    W_APPRLIMT = 18
    W_LMTAMT   = 18
    W_LEDGBAL  = 18
    W_LMTID    = 8
    W_LMTCOLL  = 8
    W_EXPRDTE  = 10

    def _col_header() -> str:
        return (
            f"{H_ACCTNO1:<{W_ACCTNO1}}  "
            f"{H_NAME:<{W_NAME}}  "
            f"{H_APPRLIMT:>{W_APPRLIMT}}  "
            f"{H_LMTAMT:>{W_LMTAMT}}  "
            f"{H_LEDGBAL:>{W_LEDGBAL}}  "
            f"{H_LMTID:<{W_LMTID}}  "
            f"{H_LMTCOLL:<{W_LMTCOLL}}  "
            f"{H_EXPRDTE:<{W_EXPRDTE}}"
        )

    def _sep() -> str:
        return "-" * len(_col_header())

    def _data_line(row: dict) -> str:
        acctno1  = (row.get("ACCTNO1")  or "")[:W_ACCTNO1]
        name     = (row.get("NAME")     or "")[:W_NAME]
        apprlimt = row.get("APPRLIMT") or 0.0
        lmtamt   = row.get("LMTAMT")   or 0.0
        ledgbal  = row.get("LEDGBAL")  or 0.0
        lmtid    = (row.get("LMTID")   or "")[:W_LMTID]
        lmtcoll  = (row.get("LMTCOLL") or "")[:W_LMTCOLL]
        exprdte_sas = row.get("EXPRDTE")
        exprdte_str = format_ddmmyy10(sas_date_to_python(int(exprdte_sas))) \
            if exprdte_sas is not None else " " * W_EXPRDTE

        return (
            f"{acctno1:<{W_ACCTNO1}}  "
            f"{name:<{W_NAME}}  "
            f"{format_comma18(apprlimt, 0):>{W_APPRLIMT}}  "
            f"{format_comma18(lmtamt,   0):>{W_LMTAMT}}  "
            f"{format_comma18(ledgbal,  2):>{W_LEDGBAL}}  "
            f"{lmtid:<{W_LMTID}}  "
            f"{lmtcoll:<{W_LMTCOLL}}  "
            f"{exprdte_str:<{W_EXPRDTE}}"
        )

    def _sum_line(apprlimt_sum: float, lmtamt_sum: float, ledgbal_sum: float,
                  label: str = "") -> str:
        pad = " " * (W_ACCTNO1 + 2 + W_NAME + 2)
        return (
            f"{pad}"
            f"{format_comma18(apprlimt_sum, 0):>{W_APPRLIMT}}  "
            f"{format_comma18(lmtamt_sum,   0):>{W_LMTAMT}}  "
            f"{format_comma18(ledgbal_sum,  2):>{W_LEDGBAL}}"
            f"  {label}"
        )

    # Group rows by BRANCH (data is already sorted BY BRANCH EXPRDTE)
    lines: list[str] = []
    grand_apprlimt = 0.0
    grand_lmtamt   = 0.0
    grand_ledgbal  = 0.0

    # Build branch groups
    branches: list[tuple[int, list[dict]]] = []
    current_branch: Optional[int] = None
    current_group:  list[dict]    = []

    for row in odrpt.iter_rows(named=True):
        b = row.get("BRANCH")
        if b != current_branch:
            if current_group:
                branches.append((current_branch, current_group))
            current_branch = b
            current_group  = [row]
        else:
            current_group.append(row)
    if current_group:
        branches.append((current_branch, current_group))

    # Render each branch on its own page (PAGEBY BRANCH)
    for branch_val, rows in branches:
        branch_apprlimt = 0.0
        branch_lmtamt   = 0.0
        branch_ledgbal  = 0.0

        # --- Page header (ASA '1' = form feed) ---
        col_hdr = _col_header()
        lines.append(f"1{TITLE1}")
        lines.append(f" {TITLE2}")
        lines.append(f" {TITLE3}")
        lines.append(f" ")
        lines.append(f" BRANCH: {format_z3(branch_val)}")
        lines.append(f" ")
        lines.append(f" {col_hdr}")
        lines.append(f" {_sep()}")

        # Track lines used on this page
        page_lines_used = 8
        DATA_ROWS_PER_PAGE = PAGE_LENGTH - 8   # header block above uses 8 lines

        for i, row in enumerate(rows):
            if page_lines_used >= PAGE_LENGTH:
                # Continuation page for same branch
                lines.append(f"1{TITLE1}")
                lines.append(f" {TITLE2}")
                lines.append(f" {TITLE3}")
                lines.append(f" ")
                lines.append(f" BRANCH: {format_z3(branch_val)} (continued)")
                lines.append(f" ")
                lines.append(f" {col_hdr}")
                lines.append(f" {_sep()}")
                page_lines_used = 8

            dline = _data_line(row)
            lines.append(f" {dline}")
            page_lines_used += 1

            apprlimt = row.get("APPRLIMT") or 0.0
            lmtamt   = row.get("LMTAMT")   or 0.0
            ledgbal  = row.get("LEDGBAL")  or 0.0
            branch_apprlimt += apprlimt
            branch_lmtamt   += lmtamt
            branch_ledgbal  += ledgbal

        # Branch subtotal (SUMBY BRANCH)
        lines.append(f" {_sep()}")
        lines.append(f" {_sum_line(branch_apprlimt, branch_lmtamt, branch_ledgbal)}")
        lines.append(f" ")

        grand_apprlimt += branch_apprlimt
        grand_lmtamt   += branch_lmtamt
        grand_ledgbal  += branch_ledgbal

    # Grand total
    if branches:
        lines.append(f" {'=' * len(_sep())}")
        lines.append(f" {_sum_line(grand_apprlimt, grand_lmtamt, grand_ledgbal, '(GRAND TOTAL)')}")

    # Footnotes (appended at end of last page)
    lines.append(f" ")
    lines.append(f" {FN1}")
    lines.append(f" {FN2}")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    # Step 1 — resolve reporting date macro variables
    macros = resolve_reptdate(REPTDATE_PARQUET)
    rdate_str = macros["RDATE"]
    print(f"[EIBQOD01] RDATE={rdate_str}  RYEAR={macros['RYEAR']}  RMONTH={macros['RMONTH']}")

    # Step 2 — DATA OD: filter DEPOSIT.OVERDFT
    od = build_od(OVERDFT_PARQUET, macros)

    # Step 3 — merge with LOAN.CURRENT for LEDGBAL (PROC SORT + DATA MERGE)
    od = merge_current(od, CURRENT_PARQUET)

    # Step 4 — sort BY BRANCH EXPRDTE; filter EXPIRMH=1
    odrpt = build_odrpt(od)

    print(f"[EIBQOD01] OD accounts due for review: {odrpt.height}")

    # Step 5 — render report
    report_text = _render_report(odrpt, rdate_str)

    # PROC PRINTTO PRINT=ODEXLIST NEW
    ODEXLIST_TXT.write_text(report_text, encoding="ascii", errors="replace")
    print(f"[EIBQOD01] Written: {ODEXLIST_TXT}")

    # PROC PRINTTO PRINT=PRINT NEW  (identical report to second destination)
    PRINT_TXT.write_text(report_text, encoding="ascii", errors="replace")
    print(f"[EIBQOD01] Written: {PRINT_TXT}")


if __name__ == "__main__":
    main()
