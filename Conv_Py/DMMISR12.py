#!/usr/bin/env python3
"""
Program : DMMISR12.py
Purpose : REPORT ON WISE ACCOUNTS BY STATE AND AGE RANGE
          REPORT ON YAA ACCOUNTS BY STATE AND AGE RANGE
          Reads SDPAGE<MM> parquet, produces two cross-tab reports
          (COLD1 and COLD2) with ASA carriage control characters.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import duckdb
import polars as pl

from PBMISFMT import (
    format_brchcd,
    format_state,
    format_agedesc,
    AGELIMIT,
    MAXAGE,
    AGEBELOW,
)

# ---------------------------------------------------------------------------
# OPTIONS equivalent
# ---------------------------------------------------------------------------
# NOCENTER NODATE PAGESIZE=60 LS=134 YEARCUTOFF=1930
PAGE_SIZE  = 60
LINE_SIZE  = 134
YEARCUTOFF = 1930

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
BASE_DIR   = Path(__file__).resolve().parent
INPUT_DIR  = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"

INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPTDATE_PARQUET = INPUT_DIR / "REPTDATE.parquet"   # BNM.REPTDATE
# MIS.SDPAGE<MM> parquet  → resolved after reading REPTMON
# e.g.  input/SDPAGE03.parquet
COLD1_TXT = OUTPUT_DIR / "COLD1.txt"
COLD2_TXT = OUTPUT_DIR / "COLD2.txt"

# ---------------------------------------------------------------------------
# AGE group codes used in AGEDESC format (from PBMISFMT)
# ---------------------------------------------------------------------------
AGE_ORDER = [0, AGEBELOW, AGELIMIT, MAXAGE]   # [0, 11, 12, 18]


# ---------------------------------------------------------------------------
# Read REPTDATE
# ---------------------------------------------------------------------------
def read_reptdate() -> dict:
    """Read BNM.REPTDATE parquet and derive macro variables."""
    con = duckdb.connect()
    row = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{REPTDATE_PARQUET}') LIMIT 1"
    ).fetchone()
    con.close()
    if row is None:
        raise ValueError("REPTDATE table is empty.")

    reptdate = row[0]   # expected as Python date or datetime
    # Ensure we have a date object
    if hasattr(reptdate, "date"):
        reptdate = reptdate.date()

    reptmon  = f"{reptdate.month:02d}"
    reptyear = str(reptdate.year)
    rdate    = reptdate.strftime("%d/%m/%Y")   # DDMMYY10. → DD/MM/YYYY
    zdate    = f"{reptdate.toordinal():05d}"   # Z5. equivalent (SAS ordinal)

    return {
        "reptdate":  reptdate,
        "reptmon":   reptmon,
        "reptyear":  reptyear,
        "rdate":     rdate,
        "zdate":     zdate,
    }


# ---------------------------------------------------------------------------
# Load SDPAGE<MM>
# ---------------------------------------------------------------------------
def load_sdpage(reptmon: str) -> pl.DataFrame:
    """
    Read MIS.SDPAGE<MM> parquet.
    Expected columns: BRANCH, STATECD, AGE, PRODUCT, NOACCT, CURBAL
    """
    path = INPUT_DIR / f"SDPAGE{reptmon}.parquet"
    con  = duckdb.connect()
    df   = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()

    # BRCH = PUT(BRANCH, BRCHCD.)
    df = df.with_columns(
        pl.col("BRANCH").map_elements(
            lambda b: format_brchcd(int(b)) if b is not None else "",
            return_dtype=pl.Utf8,
        ).alias("BRCH")
    )
    return df


# ---------------------------------------------------------------------------
# Format helpers
# ---------------------------------------------------------------------------
def _fmt_noacct(val: float) -> str:
    """COMMA8. format for number of accounts."""
    if val == 0:
        return "       0"
    return f"{int(val):>8,}"


def _fmt_curbal(val: float) -> str:
    """COMMA18.2 format for current balance."""
    if val == 0:
        return f"{'0':>18}"
    return f"{val:>18,.2f}"


def _misstext(val: Optional[float], fmt_fn) -> str:
    """Apply MISSTEXT='0': replace None/NaN with '0' formatted."""
    if val is None:
        return fmt_fn(0)
    return fmt_fn(val)


# ---------------------------------------------------------------------------
# PROC TABULATE renderer
# ---------------------------------------------------------------------------
# Column structure:  for each AGE group: NOACCT, CURBAL  |  TOTAL: NOACCT, CURBAL
# RTS = 20  (row title size)
RTS = 20
# Column widths
W_NOACCT = 8
W_CURBAL = 18
W_SEP    = 1


def _col_header_lines(age_order: list[int]) -> list[str]:
    """Build two-line column header matching PROC TABULATE layout."""
    # Line 1: age group labels spanning two sub-cols each, then TOTAL
    ln1_parts = [" " * RTS]
    for age in age_order:
        lbl = format_agedesc(age)
        span = W_NOACCT + W_SEP + W_CURBAL
        ln1_parts.append(f"{lbl:^{span}}")
    span = W_NOACCT + W_SEP + W_CURBAL
    ln1_parts.append(f"{'TOTAL':^{span}}")

    # Line 2: sub-column labels
    ln2_parts = [" " * RTS]
    for _ in range(len(age_order) + 1):   # +1 for TOTAL
        ln2_parts.append(f"{'NO OF A/C':>{W_NOACCT}}")
        ln2_parts.append(f"{'BALANCE':>{W_CURBAL}}")

    return [" ".join(ln1_parts), " ".join(ln2_parts)]


def _rule_line() -> str:
    total_width = RTS + (len(AGE_ORDER) + 1) * (W_NOACCT + W_SEP + W_CURBAL + W_SEP)
    return "-" * min(total_width, LINE_SIZE)


def _data_row(
    label: str,
    age_data: dict[int, tuple[float, float]],
    age_order: list[int],
) -> str:
    """Build a single data row."""
    parts = [f"{label:<{RTS}}"]
    tot_noacct = 0.0
    tot_curbal = 0.0
    for age in age_order:
        noacct, curbal = age_data.get(age, (0.0, 0.0))
        tot_noacct += noacct
        tot_curbal += curbal
        parts.append(_fmt_noacct(noacct))
        parts.append(_fmt_curbal(curbal))
    # TOTAL columns
    parts.append(_fmt_noacct(tot_noacct))
    parts.append(_fmt_curbal(tot_curbal))
    return " ".join(parts)


def _build_tabulate_report_1(
    df: pl.DataFrame,
    product: int,
    rdate: str,
    report_title: str,
) -> list[str]:
    """
    PROC TABULATE for WISE (product=212).
    TABLE STATECD, (BRANCH*BRCH ALL), AGE*(NOACCT CURBAL) ALL*(NOACCT CURBAL)
    FORMAT STATECD $STATE.  BRANCH BRCHCD.  AGE AGEDESC.
    """
    src = df.filter(pl.col("PRODUCT") == product)

    lines: list[str] = []
    page_lines = 0

    def emit_header(page_title: str) -> None:
        nonlocal page_lines
        asa = "1" if not lines else "1"
        lines.append(f"{asa}{page_title}")
        lines.append(f" PROGRAM ID:")
        lines.append(f" BRANCH NO :")
        lines.append(f" ")
        for hdr in _col_header_lines(AGE_ORDER):
            lines.append(f" {hdr}")
        lines.append(f" {_rule_line()}")
        page_lines = 7

    emit_header(report_title)

    # Group by STATECD
    states = sorted(src["STATECD"].unique().to_list())

    for statecd in states:
        state_df   = src.filter(pl.col("STATECD") == statecd)
        state_name = format_state(str(statecd))
        state_label = f"STATE : {state_name}"

        # State header
        if page_lines >= PAGE_SIZE - 4:
            emit_header(report_title)
        lines.append(f" {state_label}")
        page_lines += 1

        # Branches within this state
        branches = sorted(state_df["BRANCH"].unique().to_list())
        state_noacct: dict[int, float] = {a: 0.0 for a in AGE_ORDER}
        state_curbal: dict[int, float] = {a: 0.0 for a in AGE_ORDER}

        for branch in branches:
            brch_df  = state_df.filter(pl.col("BRANCH") == branch)
            brch_lbl = format_brchcd(int(branch))
            age_data: dict[int, tuple[float, float]] = {}
            for age in AGE_ORDER:
                age_df = brch_df.filter(pl.col("AGE") == age)
                na = float(age_df["NOACCT"].sum()) if len(age_df) else 0.0
                cb = float(age_df["CURBAL"].sum()) if len(age_df) else 0.0
                age_data[age] = (na, cb)
                state_noacct[age] = state_noacct.get(age, 0.0) + na
                state_curbal[age] = state_curbal.get(age, 0.0) + cb

            row_lbl = f"  {branch} {brch_lbl}"[:RTS]
            if page_lines >= PAGE_SIZE - 2:
                emit_header(report_title)
            lines.append(f" {_data_row(row_lbl, age_data, AGE_ORDER)}")
            page_lines += 1

        # State TOTAL row (ALL branches)
        all_age_data = {a: (state_noacct[a], state_curbal[a]) for a in AGE_ORDER}
        if page_lines >= PAGE_SIZE - 2:
            emit_header(report_title)
        lines.append(f" {_data_row('  TOTAL', all_age_data, AGE_ORDER)}")
        lines.append(f" {_rule_line()}")
        page_lines += 2

    # Grand total across all states
    grand_noacct: dict[int, float] = {}
    grand_curbal: dict[int, float] = {}
    for age in AGE_ORDER:
        age_df = src.filter(pl.col("AGE") == age)
        grand_noacct[age] = float(age_df["NOACCT"].sum()) if len(age_df) else 0.0
        grand_curbal[age] = float(age_df["CURBAL"].sum()) if len(age_df) else 0.0

    grand_data = {a: (grand_noacct[a], grand_curbal[a]) for a in AGE_ORDER}
    if page_lines >= PAGE_SIZE - 3:
        emit_header(report_title)
    lines.append(f" {_rule_line()}")
    lines.append(f" {_data_row('TOTAL', grand_data, AGE_ORDER)}")

    return lines


def _build_tabulate_report_2(
    df: pl.DataFrame,
    product: int,
    rdate: str,
    report_title: str,
) -> list[str]:
    """
    PROC TABULATE for YAA (product=202).
    TABLE STATECD, BRANCH ALL, AGE*(NOACCT CURBAL) ALL*(NOACCT CURBAL)
    FORMAT STATECD $STATE.  BRANCH BRCHCD.  AGE AGEDESC.
    Uses SDPAGE directly (no BRCH variable; BRANCH formatted via BRCHCD.).
    """
    src = df.filter(pl.col("PRODUCT") == product)

    lines: list[str] = []
    page_lines = 0

    def emit_header(page_title: str) -> None:
        nonlocal page_lines
        asa = "1"
        lines.append(f"{asa}{page_title}")
        lines.append(f" PROGRAM ID:")
        lines.append(f" BRANCH NO :")
        lines.append(f" ")
        for hdr in _col_header_lines(AGE_ORDER):
            lines.append(f" {hdr}")
        lines.append(f" {_rule_line()}")
        page_lines = 7

    emit_header(report_title)

    states = sorted(src["STATECD"].unique().to_list())

    for statecd in states:
        state_df   = src.filter(pl.col("STATECD") == statecd)
        state_name = format_state(str(statecd))
        state_label = f"STATE : {state_name}"

        if page_lines >= PAGE_SIZE - 4:
            emit_header(report_title)
        lines.append(f" {state_label}")
        page_lines += 1

        branches = sorted(state_df["BRANCH"].unique().to_list())
        state_noacct: dict[int, float] = {a: 0.0 for a in AGE_ORDER}
        state_curbal: dict[int, float] = {a: 0.0 for a in AGE_ORDER}

        for branch in branches:
            brch_df  = state_df.filter(pl.col("BRANCH") == branch)
            brch_lbl = format_brchcd(int(branch))
            row_lbl  = f"  {branch} {brch_lbl}"[:RTS]
            age_data: dict[int, tuple[float, float]] = {}

            for age in AGE_ORDER:
                age_df = brch_df.filter(pl.col("AGE") == age)
                na = float(age_df["NOACCT"].sum()) if len(age_df) else 0.0
                cb = float(age_df["CURBAL"].sum()) if len(age_df) else 0.0
                age_data[age] = (na, cb)
                state_noacct[age] = state_noacct.get(age, 0.0) + na
                state_curbal[age] = state_curbal.get(age, 0.0) + cb

            if page_lines >= PAGE_SIZE - 2:
                emit_header(report_title)
            lines.append(f" {_data_row(row_lbl, age_data, AGE_ORDER)}")
            page_lines += 1

        # ALL (TOTAL per state)
        all_age_data = {a: (state_noacct[a], state_curbal[a]) for a in AGE_ORDER}
        if page_lines >= PAGE_SIZE - 2:
            emit_header(report_title)
        lines.append(f" {_data_row('  TOTAL', all_age_data, AGE_ORDER)}")
        lines.append(f" {_rule_line()}")
        page_lines += 2

    # Grand total (ALL states)
    grand_noacct: dict[int, float] = {}
    grand_curbal: dict[int, float] = {}
    for age in AGE_ORDER:
        age_df = src.filter(pl.col("AGE") == age)
        grand_noacct[age] = float(age_df["NOACCT"].sum()) if len(age_df) else 0.0
        grand_curbal[age] = float(age_df["CURBAL"].sum()) if len(age_df) else 0.0

    grand_data = {a: (grand_noacct[a], grand_curbal[a]) for a in AGE_ORDER}
    if page_lines >= PAGE_SIZE - 3:
        emit_header(report_title)
    lines.append(f" {_rule_line()}")
    lines.append(f" {_data_row('TOTAL', grand_data, AGE_ORDER)}")

    return lines


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    # Read report date metadata
    meta     = read_reptdate()
    reptmon  = meta["reptmon"]
    rdate    = meta["rdate"]
    reptyear = meta["reptyear"]

    print(f"REPTMON={reptmon}  REPTYEAR={reptyear}  RDATE={rdate}")

    # Load SDPAGE<MM>
    sdpage = load_sdpage(reptmon)

    # -------------------------------------------------------------------------
    # PROC PRINTTO PRINT=COLD1 – WISE report (PRODUCT=212)
    # -------------------------------------------------------------------------
    # TITLE1 'REPORT ID : DMMISR12     REPORT ON WISE ACCOUNTS BY STATE AND AGE RANGE' &RDATE
    # TITLE2 'PROGRAM ID:'
    # TITLE3 'BRANCH NO :'
    # Commented-out alternative titles in original SAS are preserved as comments:
    #   TITLE1 'REPORT ID : DMMISR12'
    #   TITLE2 'PUBLIC BANK BERHAD - FINANCIAL PLANNING SERVICES & SUPPORT'
    #   TITLE3 'REPORT ON WISE ACCOUNTS BY STATE AND AGE RANGE'
    #   TITLE4 'AS AT ' &RDATE
    wise_title = (
        f"REPORT ID : DMMISR12     REPORT ON WISE ACCOUNTS BY STATE AND AGE RANGE"
        f"                                                   {rdate}"
    )
    cold1_lines = _build_tabulate_report_1(sdpage, 212, rdate, wise_title)
    COLD1_TXT.write_text("\n".join(cold1_lines) + "\n", encoding="utf-8")
    print(f"Written: {COLD1_TXT}")

    # -------------------------------------------------------------------------
    # PROC PRINTTO PRINT=COLD2 – YAA report (PRODUCT=202)
    # -------------------------------------------------------------------------
    # TITLE1 'REPORT ID : DMMISR12     REPORT ON YAA ACCOUNTS BY STATE AND AGE RANGE AS AT' &RDATE
    # TITLE2 'PROGRAM ID:'
    # TITLE3 'BRANCH NO :'
    # TITLE4  (blank)
    # Commented-out alternative titles in original SAS are preserved as comments:
    #   TITLE2 'PUBLIC BANK BERHAD - FINANCIAL PLANNING SERVICES & SUPPORT'
    #   TITLE3 'REPORT ON YAA ACCOUNTS BY STATE AND AGE RANGE'
    #   TITLE4 'AS AT ' &RDATE
    yaa_title = (
        f"REPORT ID : DMMISR12     REPORT ON YAA ACCOUNTS BY STATE AND AGE RANGE"
        f" AS AT                                              {rdate}"
    )
    cold2_lines = _build_tabulate_report_2(sdpage, 202, rdate, yaa_title)
    COLD2_TXT.write_text("\n".join(cold2_lines) + "\n", encoding="utf-8")
    print(f"Written: {COLD2_TXT}")


if __name__ == "__main__":
    main()
