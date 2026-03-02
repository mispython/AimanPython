#!/usr/bin/env python3
"""
Program : EIBDDPMV
Function: Daily Savings & ACE Deposits Movements by Range
          Report ID : EIBDDPMV
          Datasets DYDPS & DYACE generated from program: DALDPBBE
          Report Owner: Product Development Division, PBB
"""

import duckdb
import polars as pl
import os
from datetime import datetime
from typing import Optional

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR      = os.environ.get("BASE_DIR",  "/data")
DEPOSIT_DIR   = os.path.join(BASE_DIR, "deposit")
MIS_DIR       = os.path.join(BASE_DIR, "mis")       # SAP.PBB.MIS.D<YEAR>
OUTPUT_DIR    = os.path.join(BASE_DIR, "output")

# Input – raw transaction file used only to derive REPTDATE
DPTRBL_FILE   = os.path.join(BASE_DIR, "DPTRBL.parquet")

# Output report
REPORT_FILE   = os.path.join(OUTPUT_DIR, "EIBDDPMV.txt")

# ASA carriage-control
ASA_NEWPAGE = "1"
ASA_NEWLINE = " "

PAGE_LINES  = 60

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================================
# FORMAT TABLES
# ============================================================================

# MVTFDESA – Savings deposit movement ranges
MVTFDESA: dict[int, str] = {
     5_000: "RM 5,000 & BELOW",
    10_000: "> RM 5,000 TO RM10,000",
    30_000: "> RM10,000 TO RM30,000",
    50_000: "> RM30,000 TO RM50,000",
    75_000: "> RM50,000 TO RM75,000",
    80_000: "ABOVE RM75,000",
}

# MVTFDESB – ACE deposit movement ranges
MVTFDESB: dict[int, str] = {
      5_000: "RM 5,000 & BELOW",
     10_000: "> RM 5,000 TO RM10,000",
     30_000: "> RM10,000 TO RM30,000",
     50_000: "> RM30,000 TO RM50,000",
     75_000: "> RM50,000 TO RM75,000",
    100_000: "> RM75,000 TO RM100,000",
    200_000: "ABOVE RM100,000",
}

# Display order for each format (keys in ascending order)
MVTFDESA_ORDER = sorted(MVTFDESA.keys())
MVTFDESB_ORDER = sorted(MVTFDESB.keys())


# ============================================================================
# HELPERS
# ============================================================================

def fmt_comma(value: Optional[float], width: int = 18, dec: int = 2) -> str:
    """COMMA18.2 – right-aligned."""
    if value is None:
        return " " * width
    s = f"{value:,.{dec}f}"
    return s.rjust(width)


# ============================================================================
# STEP 1 – DERIVE REPORT DATE FROM DPTRBL (first record, @106 TBDATE PD6.)
# ============================================================================

def derive_report_date() -> dict:
    """
    DPTRBL has already been converted to parquet with column TBDATE
    (packed-decimal value decoded as integer, MMDDYYYY format).
    Read first record only (OBS=1).
    Derive week number within month (NOWK).
    """
    con = duckdb.connect()
    row = con.execute(
        f"SELECT TBDATE FROM read_parquet('{DPTRBL_FILE}') LIMIT 1"
    ).fetchone()
    con.close()

    if row is None:
        raise ValueError("DPTRBL file is empty – cannot derive report date.")

    tbdate_raw = row[0]
    tbdate_str = str(int(tbdate_raw)).zfill(8)   # MMDDYYYY
    reptdate   = datetime.strptime(tbdate_str, "%m%d%Y")

    day = reptdate.day
    if   1  <= day <= 8:  nowk = "1"
    elif 9  <= day <= 15: nowk = "2"
    elif 16 <= day <= 22: nowk = "3"
    else:                 nowk = "4"

    return {
        "reptdate" : reptdate,
        "reptyear" : reptdate.strftime("%Y"),
        "reptmon"  : reptdate.strftime("%m"),
        "reptday"  : reptdate.strftime("%d"),
        "rdate"    : reptdate.strftime("%d/%m/%Y"),
        "nowk"     : nowk,
    }


# ============================================================================
# STEP 2 – LOAD & FILTER SOURCE DATASETS
# ============================================================================

def load_dydps(ctx: dict) -> pl.DataFrame:
    """Load MIS.DYDPS<MM> and filter to REPTDATE matching the report date."""
    mon       = ctx["reptmon"]
    reptdate  = ctx["reptdate"]
    dydps_file = os.path.join(MIS_DIR, f"DYDPS{mon}.parquet")

    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{dydps_file}')").pl()
    con.close()

    # Normalise REPTDATE column to datetime for comparison
    def parse_dt(val) -> Optional[datetime]:
        if val is None:
            return None
        if isinstance(val, datetime):
            return val
        if isinstance(val, (int, float)):
            try:
                return datetime.strptime(str(int(val)), "%Y%m%d")
            except ValueError:
                return None
        try:
            return datetime.strptime(str(val)[:10], "%Y-%m-%d")
        except ValueError:
            return None

    df = df.with_columns(
        pl.col("REPTDATE").map_elements(parse_dt, return_dtype=pl.Datetime).alias("REPTDATE_DT")
    )
    return df.filter(pl.col("REPTDATE_DT") == reptdate)


def load_dyace(ctx: dict) -> pl.DataFrame:
    """Load MIS.DYACE<MM> and filter to REPTDATE matching the report date."""
    mon       = ctx["reptmon"]
    reptdate  = ctx["reptdate"]
    dyace_file = os.path.join(MIS_DIR, f"DYACE{mon}.parquet")

    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{dyace_file}')").pl()
    con.close()

    def parse_dt(val) -> Optional[datetime]:
        if val is None:
            return None
        if isinstance(val, datetime):
            return val
        if isinstance(val, (int, float)):
            try:
                return datetime.strptime(str(int(val)), "%Y%m%d")
            except ValueError:
                return None
        try:
            return datetime.strptime(str(val)[:10], "%Y-%m-%d")
        except ValueError:
            return None

    df = df.with_columns(
        pl.col("REPTDATE").map_elements(parse_dt, return_dtype=pl.Datetime).alias("REPTDATE_DT")
    )
    return df.filter(pl.col("REPTDATE_DT") == reptdate)


# ============================================================================
# STEP 3 – TABULATE: SUM MOVEMENT BY MVRANGE
# ============================================================================

def tabulate_movement(df: pl.DataFrame,
                      fmt_map: dict[int, str],
                      fmt_order: list[int]) -> pl.DataFrame:
    """
    Replicate PROC TABULATE:
      CLASS MVRANGE; VAR MOVEMENT;
      TABLE MVRANGE='', MOVEMENT="TOTAL MOVEMENTS"*SUM=*F=COMMA18.2
      WHERE REPTDATE = <rdate>;

    Returns a DataFrame with columns: MVRANGE_LBL, MOVEMENT_SUM.
    Rows ordered by fmt_order keys, followed by a TOTAL row.
    """
    if df.height == 0 or "MVRANGE" not in df.columns or "MOVEMENT" not in df.columns:
        rows = [{"MVRANGE_LBL": fmt_map.get(k, str(k)), "MOVEMENT_SUM": 0.0}
                for k in fmt_order]
        rows.append({"MVRANGE_LBL": "TOTAL", "MOVEMENT_SUM": 0.0})
        return pl.DataFrame(rows)

    agg = (
        df.group_by("MVRANGE")
          .agg(pl.col("MOVEMENT").sum().alias("MOVEMENT_SUM"))
    )

    # Build ordered result
    result_rows = []
    grand_total = 0.0
    for key in fmt_order:
        label = fmt_map.get(key, str(key))
        match = agg.filter(pl.col("MVRANGE") == key)
        val   = match["MOVEMENT_SUM"][0] if match.height > 0 else 0.0
        grand_total += val
        result_rows.append({"MVRANGE_LBL": label, "MOVEMENT_SUM": float(val)})

    result_rows.append({"MVRANGE_LBL": "TOTAL", "MOVEMENT_SUM": grand_total})
    return pl.DataFrame(result_rows)


# ============================================================================
# STEP 4 – WRITE REPORT
# ============================================================================

# Table layout constants
BOX_WIDTH  = 30   # RTS=30 (row title space)
DATA_WIDTH = 18   # COMMA18.2

BOX_LABEL_SA  = "NET INCREASE/(DECREASE)"
BOX_LABEL_ACE = "NET INCREASE/(DECREASE)"
DATA_HEADER   = "TOTAL MOVEMENTS"


def _table_lines(title_row: str, box_label: str,
                 tab_df: pl.DataFrame, asa_first: str = ASA_NEWLINE) -> list[str]:
    """
    Render a single PROC TABULATE table block as fixed-width text lines.
    """
    lines: list[str] = []
    sep_box  = "-" * BOX_WIDTH
    sep_data = "-" * DATA_WIDTH
    sep_line = f"{sep_box}-+-{sep_data}"

    # Table header
    lines.append(f"{asa_first}{title_row}")
    lines.append(f"{ASA_NEWLINE}{box_label:<{BOX_WIDTH}} | {DATA_HEADER:>{DATA_WIDTH}}")
    lines.append(f"{ASA_NEWLINE}{sep_line}")

    for row in tab_df.to_dicts():
        lbl = str(row.get("MVRANGE_LBL") or "")
        val = row.get("MOVEMENT_SUM")
        is_total = lbl == "TOTAL"

        if is_total:
            lines.append(f"{ASA_NEWLINE}{sep_line}")

        lines.append(
            f"{ASA_NEWLINE}{lbl:<{BOX_WIDTH}} | {fmt_comma(val, DATA_WIDTH)}"
        )

    lines.append(f"{ASA_NEWLINE}{sep_line}")
    return lines


def write_report(tab_savings: pl.DataFrame,
                 tab_ace: pl.DataFrame,
                 rdate: str) -> None:
    lines: list[str] = []

    # ── Report 1: Savings ────────────────────────────────────────────────
    lines.append(f"{ASA_NEWPAGE}REPORT ID : EIBDDPMV")
    lines.append(f"{ASA_NEWLINE}DAILY SUMMARY REPORT ON SAVINGS DEPOSITS CUSTOMERS' MOVEMENTS")
    lines.append(f"{ASA_NEWLINE}AS AT {rdate} EXCLUDE ACE & AL-WADIAH")
    lines.append(f"{ASA_NEWLINE}")

    lines.extend(
        _table_lines(
            title_row = f"AS AT {rdate} EXCLUDE ACE & AL-WADIAH",
            box_label = BOX_LABEL_SA,
            tab_df    = tab_savings,
            asa_first = ASA_NEWLINE,
        )
    )

    # ── Report 2: ACE ────────────────────────────────────────────────────
    lines.append(f"{ASA_NEWLINE}")
    lines.append(f"{ASA_NEWLINE}REPORT ID : EIBDDPMV")
    lines.append(f"{ASA_NEWLINE}DAILY SUMMARY REPORT ON ACE DEPOSITS CUSTOMERS' MOVEMENTS")
    lines.append(f"{ASA_NEWLINE}AS AT {rdate}")
    lines.append(f"{ASA_NEWLINE}")

    lines.extend(
        _table_lines(
            title_row = f"AS AT {rdate}",
            box_label = BOX_LABEL_ACE,
            tab_df    = tab_ace,
            asa_first = ASA_NEWLINE,
        )
    )

    with open(REPORT_FILE, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"Report written: {REPORT_FILE}")


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    print("EIBDDPMV – Daily Savings & ACE Deposits Movements by Range starting...")

    # Step 1: derive report date from DPTRBL first record
    ctx = derive_report_date()
    print(f"  Report date : {ctx['rdate']}  (week {ctx['nowk']} of month)")

    # Step 2: load source datasets filtered to report date
    dydps = load_dydps(ctx)
    dyace = load_dyace(ctx)

    # Step 3: tabulate by movement range
    tab_savings = tabulate_movement(dydps, MVTFDESA, MVTFDESA_ORDER)
    tab_ace     = tabulate_movement(dyace, MVTFDESB, MVTFDESB_ORDER)

    # Step 4: write report
    write_report(tab_savings, tab_ace, ctx["rdate"])

    print("EIBDDPMV – Done.")


if __name__ == "__main__":
    main()
