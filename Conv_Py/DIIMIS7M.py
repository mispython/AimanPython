#!/usr/bin/env python3
"""
Program : DIIMIS7M.py
Date    : 11.01.12
SMR     : 2011-1586
Report  : PIBB Al-Mudharabah FD Account
          Period 16-JAN-12 Until 15-JUN-13

Function: Produces four PROC TABULATE-style report sections:
            (OLD) To-Date Balance       - %TDBAL  / DYIBUAO
            (OLD) Daily Average Balance - %AVG    / DYIBUA1O
            (NEW) To-Date Balance       - %TDBAL1 / DYIBUAN  (by CUSTCD)
            (NEW) Daily Average Balance - %AVG1   / DYIBUA1N (by CUSTCD)

          Execution is conditional on REPTFQ = 'Y' (%CHKRPTDT).
          REPTFQ is hard-coded to 'Y' in the DATA REPTDATE step.
"""

import duckdb
import polars as pl
import os
from datetime import datetime
from typing import Optional

# %INC PGM(PBMISFMT)
from PBMISFMT import format_brchcd

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR      = os.environ.get("BASE_DIR",   "/data")
DEPOSIT_DIR   = os.path.join(BASE_DIR, "deposit")   # DEPOSIT library
MIS_DIR       = os.path.join(BASE_DIR, "mis")        # MIS library
OUTPUT_DIR    = os.path.join(BASE_DIR, "output")

# Input files
REPTDATE_FILE = os.path.join(DEPOSIT_DIR, "REPTDATE.parquet")  # DEPOSIT.REPTDATE

# Output report
REPORT_FILE   = os.path.join(OUTPUT_DIR, "DIIMIS7M.txt")

# REPTFQ is always 'Y' (hard-coded in SAS DATA REPTDATE step)
REPTFQ = "Y"

# ASA carriage-control
ASA_NEWPAGE = "1"
ASA_NEWLINE = " "

PAGE_LINES  = 60

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================================
# FORMAT: FDFMT  (INTPLAN month-code -> month integer 1-60)
# Maps each INTPLAN value to its MTH (term in months).
# OTHERWISE -> MTH = 0
# ============================================================================

INTPLAN_TO_MTH: dict[int, int] = {
    # MTH 1
    340: 1, 448: 1, 660: 1, 720: 1,
    # MTH 2
    352: 2, 449: 2, 661: 2, 721: 2,
    # MTH 3
    341: 3, 450: 3, 662: 3, 722: 3,
    # MTH 4
    353: 4, 451: 4, 663: 4, 723: 4,
    # MTH 5
    354: 5, 452: 5, 664: 5, 724: 5,
    # MTH 6
    342: 6, 453: 6, 665: 6, 725: 6,
    # MTH 7
    355: 7, 454: 7, 666: 7, 726: 7,
    # MTH 8
    356: 8, 455: 8, 667: 8, 727: 8,
    # MTH 9
    343: 9, 456: 9, 668: 9, 728: 9,
    # MTH 10
    357: 10, 457: 10, 669: 10, 729: 10,
    # MTH 11
    358: 11, 458: 11, 670: 11, 730: 11,
    # MTH 12
    344: 12, 459: 12, 671: 12, 731: 12,
    # MTH 13
    588: 13, 461: 13, 672: 13, 732: 13,
    # MTH 14
    589: 14, 462: 14, 673: 14, 733: 14,
    # MTH 15
    345: 15, 463: 15, 674: 15, 734: 15,
    # MTH 16
    590: 16, 675: 16,
    # MTH 17
    591: 17, 676: 17,
    # MTH 18
    346: 18, 464: 18, 677: 18, 735: 18,
    # MTH 19
    592: 19, 678: 19,
    # MTH 20
    593: 20, 679: 20,
    # MTH 21
    347: 21, 465: 21, 680: 21, 736: 21,
    # MTH 22
    594: 22, 681: 22,
    # MTH 23
    595: 23, 682: 23,
    # MTH 24
    348: 24, 466: 24, 683: 24, 737: 24,
    # MTH 25
    596: 25, 684: 25,
    # MTH 26
    597: 26, 685: 26,
    # MTH 27
    359: 27, 686: 27,
    # MTH 28
    598: 28, 687: 28,
    # MTH 29
    599: 29, 688: 29,
    # MTH 30
    540: 30, 580: 30, 689: 30,
    # MTH 31
    690: 31,
    # MTH 32
    691: 32,
    # MTH 33
    541: 33, 581: 33, 692: 33,
    # MTH 34
    693: 34,
    # MTH 35
    694: 35,
    # MTH 36
    349: 36, 467: 36, 695: 36, 738: 36,
    # MTH 37
    696: 37,
    # MTH 38
    697: 38,
    # MTH 39
    542: 39, 582: 39, 698: 39,
    # MTH 40
    699: 40,
    # MTH 41
    700: 41,
    # MTH 42
    543: 42, 583: 42, 701: 42,
    # MTH 43
    702: 43,
    # MTH 44
    703: 44,
    # MTH 45
    544: 45, 584: 45, 704: 45,
    # MTH 46
    705: 46,
    # MTH 47
    706: 47,
    # MTH 48
    350: 48, 468: 48, 707: 48, 739: 48,
    # MTH 49
    708: 49,
    # MTH 50
    709: 50,
    # MTH 51
    545: 51, 585: 51, 710: 51,
    # MTH 52
    711: 52,
    # MTH 53
    712: 53,
    # MTH 54
    546: 54, 586: 54, 713: 54,
    # MTH 55
    714: 55,
    # MTH 56
    715: 56,
    # MTH 57
    547: 57, 587: 57, 716: 57,
    # MTH 58
    717: 58,
    # MTH 59
    718: 59,
    # MTH 60
    351: 60, 719: 60, 740: 60,
}


def fdfmt(mth: Optional[int]) -> str:
    """FDFMT: month integer -> label string (e.g. 1 -> '1 MONTH', 3 -> '3 MONTHS')."""
    if mth is None or mth == 0:
        return " "
    if mth == 1:
        return "1 MONTH"
    if 2 <= mth <= 60:
        return f"{mth} MONTHS"
    return " "


def cdfmt(custcd: Optional[int]) -> str:
    """CDFMT: 77,78,95,96 -> 'RETAIL'  OTHER -> 'CORPORATE'."""
    if custcd in (77, 78, 95, 96):
        return "RETAIL"
    return "CORPORATE"


# ============================================================================
# HELPERS
# ============================================================================

def fmt_comma9(value: Optional[float], width: int = 9) -> str:
    """COMMA9. - right-aligned integer with commas."""
    if value is None:
        return " " * width
    s = f"{int(round(value)):,}"
    return s.rjust(width)


def fmt_comma14_2(value: Optional[float], width: int = 14) -> str:
    """COMMA14.2 - right-aligned two-decimal with commas."""
    if value is None:
        return " " * width
    s = f"{value:,.2f}"
    return s.rjust(width)


def _parse_reptdate(val) -> datetime:
    if isinstance(val, datetime):
        return val
    if isinstance(val, (int, float)):
        return datetime.strptime(str(int(val)), "%Y%m%d")
    return datetime.strptime(str(val)[:10], "%Y-%m-%d")


# ============================================================================
# STEP 1 - DERIVE REPORT DATE FROM DEPOSIT.REPTDATE
# ============================================================================

def derive_report_date() -> dict:
    """
    Read DEPOSIT.REPTDATE.
    REPTFQ is always 'Y' (hard-coded in SAS DATA step).
    Derive: REPTMON, RDATE, SDATE (1st of month), EDATE (= report date).
    """
    con = duckdb.connect()
    row = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{REPTDATE_FILE}') LIMIT 1"
    ).fetchone()
    con.close()

    if row is None:
        raise ValueError("REPTDATE file is empty.")

    reptdate = _parse_reptdate(row[0])
    strtdte  = reptdate.replace(day=1)      # MDY(MONTH,1,YEAR)

    return {
        "reptdate" : reptdate,
        "reptmon"  : reptdate.strftime("%m"),
        "reptfq"   : REPTFQ,
        "rdate"    : reptdate.strftime("%d/%m/%Y"),
        "sdate"    : int(strtdte.strftime("%Y%m%d")),   # numeric YYYYMMDD
        "edate"    : int(reptdate.strftime("%Y%m%d")),  # numeric YYYYMMDD
        "rpdate"   : int(reptdate.strftime("%Y%m%d")),
        "edate_day": reptdate.day,                       # DAY(&EDATE) for divisor
    }


# ============================================================================
# STEP 2 - LOAD MIS.DYIBUM<MM> AND BUILD DYIBUA / DYIBUA1
# ============================================================================

def _parse_yyyymmdd(val) -> Optional[int]:
    """Normalise REPTDATE to YYYYMMDD integer for range comparison."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        s = str(int(val))
        if len(s) == 8:
            return int(s)
        return int(val)
    if isinstance(val, datetime):
        return int(val.strftime("%Y%m%d"))
    try:
        return int(datetime.strptime(str(val)[:10], "%Y-%m-%d").strftime("%Y%m%d"))
    except ValueError:
        return None


def load_dyibum(ctx: dict) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Load MIS.DYIBUM<MM>.
    Apply BRCHCD format, compute BRCH = Z3(BRANCH) + '/' + BRCHCD.
    Map INTPLAN -> MTH via INTPLAN_TO_MTH.
    Split:
      DYIBUA  - rows where REPTDATE = EDATE
      DYIBUA1 - rows where SDATE <= REPTDATE <= EDATE
    """
    path = os.path.join(MIS_DIR, f"DYIBUM{ctx['reptmon']}.parquet")
    con  = duckdb.connect()
    df   = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()

    # Normalise REPTDATE to YYYYMMDD integer
    df = df.with_columns(
        pl.col("REPTDATE")
          .map_elements(_parse_yyyymmdd, return_dtype=pl.Int64)
          .alias("REPTDATE_N")
    )

    # BRCHCD and BRCH
    def get_brchcd(branch) -> str:
        return format_brchcd(int(branch) if branch is not None else None)

    df = df.with_columns([
        pl.col("BRANCH")
          .map_elements(get_brchcd, return_dtype=pl.Utf8)
          .alias("BRCHCD"),
    ])
    df = df.with_columns(
        (pl.col("BRANCH").cast(pl.Utf8).str.zfill(3) + pl.lit("/") + pl.col("BRCHCD"))
        .alias("BRCH")
    )

    # MTH from INTPLAN
    df = df.with_columns(
        pl.col("INTPLAN")
          .map_elements(lambda v: INTPLAN_TO_MTH.get(int(v), 0) if v is not None else 0,
                        return_dtype=pl.Int64)
          .alias("MTH")
    )

    # CUSTCD from CUSTCODE (CDFMT applied in rendering)
    if "CUSTCODE" in df.columns and "CUSTCD" not in df.columns:
        df = df.with_columns(pl.col("CUSTCODE").alias("CUSTCD"))

    sdate = ctx["sdate"]
    edate = ctx["edate"]

    # DYIBUA: REPTDATE = EDATE
    dyibua  = df.filter(pl.col("REPTDATE_N") == edate)

    # DYIBUA1: SDATE <= REPTDATE <= EDATE
    dyibua1 = df.filter(
        (pl.col("REPTDATE_N") >= sdate) &
        (pl.col("REPTDATE_N") <= edate)
    )

    return dyibua, dyibua1


# ============================================================================
# STEP 3 - AGGREGATE HELPERS
# ============================================================================

def _agg_by_brch_mth(df: pl.DataFrame) -> pl.DataFrame:
    """
    PROC SORT by BRANCH then PROC SUMMARY aggregate:
    SUM of FDI, FDINO, FDINO2 grouped by BRCH x MTH.
    """
    agg_cols = [c for c in ("FDI", "FDINO", "FDINO2") if c in df.columns]
    if not agg_cols:
        return pl.DataFrame()
    return (
        df.select([c for c in ("BRCH", "MTH") + tuple(agg_cols) if c in df.columns])
          .group_by(["BRCH", "MTH"])
          .agg([pl.col(c).sum() for c in agg_cols])
          .sort(["BRCH", "MTH"])
    )


def _agg_by_brch_mth_custcd(df: pl.DataFrame) -> pl.DataFrame:
    """
    PROC SUMMARY aggregate: SUM of FDI, FDINO, FDINO2 grouped by BRCH x MTH x CUSTCD.
    """
    agg_cols = [c for c in ("FDI", "FDINO", "FDINO2") if c in df.columns]
    if not agg_cols:
        return pl.DataFrame()
    return (
        df.select([c for c in ("BRCH", "MTH", "CUSTCD") + tuple(agg_cols) if c in df.columns])
          .group_by(["BRCH", "MTH", "CUSTCD"])
          .agg([pl.col(c).sum() for c in agg_cols])
          .sort(["BRCH", "MTH", "CUSTCD"])
    )


def _divide_by_days(df: pl.DataFrame, days: int) -> pl.DataFrame:
    """
    DATA step: divide FDI, FDINO, FDINO2 by DAYS (= DAY(EDATE)).
    Matches the ARRAY VBL / DO OVER VBL division.
    """
    for col in ("FDI", "FDINO", "FDINO2"):
        if col in df.columns:
            df = df.with_columns(
                pl.when(pl.col(col).is_not_null())
                  .then(pl.col(col) / days)
                  .otherwise(None)
                  .alias(col)
            )
    return df


# ============================================================================
# STEP 4 - PROC TABULATE RENDERER
# ============================================================================

def _render_tabulate(df: pl.DataFrame,
                     section_label: str,
                     by_custcd: bool,
                     lines: list[str]) -> None:
    """
    Render a PROC TABULATE output equivalent.

    TABLE layout (OLD / no CUSTCD):
      BRCH (rows) x MTH columns + TOTAL column
      Measures: FDINO (NO OF A/C), FDINO2 (NO OF RECEIPT), FDI (balance, per MTH + TOTAL)

    TABLE layout (NEW / by CUSTCD):
      CUSTCD (outer row) x BRCH (inner row) x same MTH columns + TOTAL

    BOX='BRANCH NO/CODE'  RTS=10
    NOSEPS (no separator lines between cells)
    """
    if df.height == 0:
        lines.append(f"{ASA_NEWLINE}  (NO DATA)")
        return

    # Determine which MTH values actually appear
    mths_present = sorted(df["MTH"].unique().to_list())
    mths_present = [m for m in mths_present if m != 0]

    # Column widths
    BRCH_W = 10   # RTS=10
    NO_W   =  9   # COMMA9.
    BAL_W  = 14   # COMMA14.2

    # Column group header line
    mth_labels = [fdfmt(m) for m in mths_present] + ["TOTAL"]
    col_hdr = " " * BRCH_W
    for lbl in mth_labels:
        col_hdr += f" {lbl:^{NO_W + NO_W + BAL_W + 2}}"
    lines.append(f"{ASA_NEWLINE}{col_hdr}")

    sub_hdr = f"{'BRANCH NO/CODE':<{BRCH_W}}"
    sub_hdr += f" {section_label}"
    lines.append(f"{ASA_NEWLINE}{sub_hdr}")

    measure_hdr = " " * BRCH_W
    for _ in mth_labels:
        measure_hdr += (
            f" {'NO OF A/C':>{NO_W}}"
            f" {'NO OF RECEIPT':>{NO_W}}"
            f" {'':>{BAL_W}}"
        )
    lines.append(f"{ASA_NEWLINE}{measure_hdr}")

    sep = "-" * (BRCH_W + len(mth_labels) * (NO_W + NO_W + BAL_W + 3))
    lines.append(f"{ASA_NEWLINE}{sep}")

    def _render_rows(subset: pl.DataFrame, row_col: str) -> dict:
        """Render data rows for a given grouping; return grand totals dict."""
        row_keys    = sorted(subset[row_col].unique().to_list())
        grand: dict = {"FDINO": 0.0, "FDINO2": 0.0, "FDI": 0.0}
        row_totals  = {k: {"FDINO": 0.0, "FDINO2": 0.0, "FDI": 0.0} for k in row_keys}
        mth_cell    = {k: {m: {"FDINO": 0.0, "FDINO2": 0.0, "FDI": 0.0}
                            for m in mths_present} for k in row_keys}

        for row in subset.to_dicts():
            k   = row[row_col]
            m   = row.get("MTH", 0)
            fdi = float(row.get("FDI",    0) or 0)
            fno = float(row.get("FDINO",  0) or 0)
            fn2 = float(row.get("FDINO2", 0) or 0)
            if m in mths_present:
                mth_cell[k][m]["FDI"]    += fdi
                mth_cell[k][m]["FDINO"]  += fno
                mth_cell[k][m]["FDINO2"] += fn2
            row_totals[k]["FDI"]    += fdi
            row_totals[k]["FDINO"]  += fno
            row_totals[k]["FDINO2"] += fn2
            grand["FDI"]    += fdi
            grand["FDINO"]  += fno
            grand["FDINO2"] += fn2

        for k in row_keys:
            line = f"{str(k):<{BRCH_W}}"
            for m in mths_present:
                cell = mth_cell[k][m]
                line += (
                    f" {fmt_comma9(cell['FDINO'],  NO_W)}"
                    f" {fmt_comma9(cell['FDINO2'], NO_W)}"
                    f" {fmt_comma14_2(cell['FDI'], BAL_W)}"
                )
            # TOTAL column
            rt = row_totals[k]
            line += (
                f" {fmt_comma9(rt['FDINO'],  NO_W)}"
                f" {fmt_comma9(rt['FDINO2'], NO_W)}"
                f" {fmt_comma14_2(rt['FDI'], BAL_W)}"
            )
            lines.append(f"{ASA_NEWLINE}{line}")

        return grand

    if not by_custcd:
        # -- OLD: no CUSTCD breakdown -----------------------------------------
        grand = _render_rows(df, "BRCH")
        lines.append(f"{ASA_NEWLINE}{sep}")
        tot_line = f"{'TOTAL':<{BRCH_W}}"
        for m in mths_present:
            m_sub = df.filter(pl.col("MTH") == m)
            fdi   = m_sub["FDI"].sum()    if "FDI"    in m_sub.columns else 0.0
            fno   = m_sub["FDINO"].sum()  if "FDINO"  in m_sub.columns else 0.0
            fn2   = m_sub["FDINO2"].sum() if "FDINO2" in m_sub.columns else 0.0
            tot_line += (
                f" {fmt_comma9(fno,  NO_W)}"
                f" {fmt_comma9(fn2,  NO_W)}"
                f" {fmt_comma14_2(fdi, BAL_W)}"
            )
        tot_line += (
            f" {fmt_comma9(grand['FDINO'],  NO_W)}"
            f" {fmt_comma9(grand['FDINO2'], NO_W)}"
            f" {fmt_comma14_2(grand['FDI'], BAL_W)}"
        )
        lines.append(f"{ASA_NEWLINE}{tot_line}")
        lines.append(f"{ASA_NEWLINE}{sep}")

    else:
        # -- NEW: outer CUSTCD x inner BRCH -----------------------------------
        custcds = sorted(df["CUSTCD"].cast(pl.Int64, strict=False).unique().to_list())
        for cd in custcds:
            cd_label = cdfmt(cd)
            lines.append(f"{ASA_NEWLINE}{cd_label}")
            subset = df.filter(pl.col("CUSTCD").cast(pl.Int64, strict=False) == cd)
            grand  = _render_rows(subset, "BRCH")
            lines.append(f"{ASA_NEWLINE}{sep}")
            tot_line = f"{'TOTAL':<{BRCH_W}}"
            for m in mths_present:
                m_sub = subset.filter(pl.col("MTH") == m)
                fdi   = m_sub["FDI"].sum()    if "FDI"    in m_sub.columns else 0.0
                fno   = m_sub["FDINO"].sum()  if "FDINO"  in m_sub.columns else 0.0
                fn2   = m_sub["FDINO2"].sum() if "FDINO2" in m_sub.columns else 0.0
                tot_line += (
                    f" {fmt_comma9(fno,  NO_W)}"
                    f" {fmt_comma9(fn2,  NO_W)}"
                    f" {fmt_comma14_2(fdi, BAL_W)}"
                )
            tot_line += (
                f" {fmt_comma9(grand['FDINO'],  NO_W)}"
                f" {fmt_comma9(grand['FDINO2'], NO_W)}"
                f" {fmt_comma14_2(grand['FDI'], BAL_W)}"
            )
            lines.append(f"{ASA_NEWLINE}{tot_line}")
            lines.append(f"{ASA_NEWLINE}{sep}")


# ============================================================================
# STEP 5 - MACRO EQUIVALENTS
# ============================================================================

def tdbal(dyibua: pl.DataFrame, rdate: str, lines: list[str]) -> None:
    """
    %TDBAL - To-Date Balance (OLD).
    PROC SORT by BRANCH (done in load), PROC TABULATE BRCH x MTH.
    """
    agg = _agg_by_brch_mth(dyibua)
    lines.append(f"{ASA_NEWPAGE}REPORT ID : DIIMIS7M (OLD)")
    lines.append(f"{ASA_NEWLINE}PUBLIC ISLAMIC BANK BERHAD - IBU")
    lines.append(f"{ASA_NEWLINE}NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION")
    lines.append(f"{ASA_NEWLINE}(16-JAN-12 UNTIL 15-JUN-13) AS AT {rdate}")
    lines.append(f"{ASA_NEWLINE}")
    _render_tabulate(agg, "TODATE BALANCE", by_custcd=False, lines=lines)


def avg(dyibua1: pl.DataFrame, edate_day: int, rdate: str, lines: list[str]) -> None:
    """
    %AVG - Daily Average Balance (OLD).
    Only executed if DYIBUA1 is not empty (%IF &N NE 0).
    PROC SUMMARY then divide by DAY(&EDATE).
    """
    if dyibua1.height == 0:
        return   # %IF "&N" NE "0" guard

    agg = _agg_by_brch_mth(dyibua1)
    agg = _divide_by_days(agg, edate_day)

    lines.append(f"{ASA_NEWPAGE}REPORT ID : DIIMIS7M (OLD)")
    lines.append(f"{ASA_NEWLINE}PUBLIC ISLAMIC BANK BERHAD - IBU")
    lines.append(f"{ASA_NEWLINE}NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION")
    lines.append(f"{ASA_NEWLINE}(16-JAN-12 UNTIL 15-JUN-13) AS AT {rdate}")
    lines.append(f"{ASA_NEWLINE}")
    _render_tabulate(agg, "DAILY AVERAGE", by_custcd=False, lines=lines)


def tdbal1(dyibua: pl.DataFrame, rdate: str, lines: list[str]) -> None:
    """
    %TDBAL1 - To-Date Balance (NEW, with CUSTCD breakdown).
    PROC SORT by BRANCH, PROC TABULATE with CUSTCD outer dimension.
    """
    agg = _agg_by_brch_mth_custcd(dyibua)
    lines.append(f"{ASA_NEWPAGE}REPORT ID : DIIMIS7M (NEW)")
    lines.append(f"{ASA_NEWLINE}PUBLIC ISLAMIC BANK BERHAD - IBU")
    lines.append(f"{ASA_NEWLINE}NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION")
    lines.append(f"{ASA_NEWLINE}(16-JAN-12 UNTIL 15-JUN-13) AS AT {rdate}")
    lines.append(f"{ASA_NEWLINE}")
    _render_tabulate(agg, "TODATE BALANCE", by_custcd=True, lines=lines)


def avg1(dyibua1: pl.DataFrame, edate_day: int, rdate: str, lines: list[str]) -> None:
    """
    %AVG1 - Daily Average Balance (NEW).
    Uses PROC CONTENTS NOBS to check row count (equivalent: check height > 0).
    PROC SUMMARY then divide by DAY(&EDATE), PROC TABULATE with CUSTCD.
    """
    if dyibua1.height == 0:
        return   # %IF "&N" NE "0" / NOBS guard

    agg = _agg_by_brch_mth_custcd(dyibua1)
    agg = _divide_by_days(agg, edate_day)

    lines.append(f"{ASA_NEWPAGE}REPORT ID : DIIMIS7M (NEW)")
    lines.append(f"{ASA_NEWLINE}PUBLIC ISLAMIC BANK BERHAD - IBU")
    lines.append(f"{ASA_NEWLINE}NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION")
    lines.append(f"{ASA_NEWLINE}(16-JAN-12 UNTIL 15-JUN-13) AS AT {rdate}")
    lines.append(f"{ASA_NEWLINE}")
    _render_tabulate(agg, "DAILY AVERAGE", by_custcd=True, lines=lines)


# ============================================================================
# %CHKRPTDT / %DIBMIS02 - MAIN CONDITIONAL EXECUTION
# ============================================================================

def dibmis02(ctx: dict) -> None:
    """
    %DIBMIS02 - Main program body.
    Loads data, runs all four tabulate sections.
    Only called when REPTFQ = 'Y' (%CHKRPTDT guard).
    """
    dyibua, dyibua1 = load_dyibum(ctx)
    lines: list[str] = []

    # -- (OLD) To-Date Balance ------------------------------------------------
    tdbal(dyibua, ctx["rdate"], lines)

    # -- (OLD) Daily Average (%AVG - only if DYIBUA1 not empty) --------------
    avg(dyibua1, ctx["edate_day"], ctx["rdate"], lines)

    # -- (NEW) To-Date Balance by CUSTCD --------------------------------------
    tdbal1(dyibua, ctx["rdate"], lines)

    # -- (NEW) Daily Average by CUSTCD (%AVG1) --------------------------------
    avg1(dyibua1, ctx["edate_day"], ctx["rdate"], lines)

    with open(REPORT_FILE, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"Report written: {REPORT_FILE}")


# ============================================================================
# MAIN  (%CHKRPTDT)
# ============================================================================

def main() -> None:
    print("DIIMIS7M - PIBB Al-Mudharabah FD Account Report starting...")

    ctx = derive_report_date()
    print(f"  Report date  : {ctx['rdate']}")
    print(f"  Period start : {ctx['sdate']}  end : {ctx['edate']}")
    print(f"  REPTFQ       : {ctx['reptfq']}")

    # %CHKRPTDT: only execute if REPTFQ = 'Y'
    if ctx["reptfq"] == "Y":
        dibmis02(ctx)
    else:
        print("  REPTFQ != 'Y' - report generation skipped (%CHKRPTDT guard).")

    print("DIIMIS7M - Done.")


if __name__ == "__main__":
    main()
