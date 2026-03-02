#!/usr/bin/env python3
"""
Program : DMMISR09
Function: Demand Deposits Credit/OD Movement of 1 Million and Above
            in Current Accounts (previous day to today)
          Produces two report sections:
            - Credit Movement (CRMOVE)
            - OD Movement     (ODMOVE)
"""

import duckdb
import polars as pl
import os
from datetime import datetime, timedelta
from typing import Optional

# Import branch-code format from PBMISFMT
from PBMISFMT import format_brchcd

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR      = os.environ.get("BASE_DIR",  "/data")
MIS_DIR       = os.path.join(BASE_DIR, "mis")        # contains DDMV dataset
OUTPUT_DIR    = os.path.join(BASE_DIR, "output")

# Input files
DPTRBL_FILE   = os.path.join(BASE_DIR, "DPTRBL.parquet")   # @106 TBDATE PD6.
DDMV_FILE     = os.path.join(MIS_DIR,  "DDMV.parquet")     # MIS.DDMV

# Output report
REPORT_FILE   = os.path.join(OUTPUT_DIR, "DMMISR09.txt")

# ASA carriage-control
ASA_NEWPAGE = "1"
ASA_NEWLINE = " "

PAGE_LINES  = 60

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================================
# HELPERS
# ============================================================================

def fmt_comma(value: Optional[float], width: int = 16, dec: int = 2) -> str:
    """COMMA16.2 right-aligned."""
    if value is None:
        return " " * width
    s = f"{value:,.{dec}f}"
    return s.rjust(width)


def fmt_10_1(value: Optional[float], width: int = 10) -> str:
    """10.1 format – right-aligned one decimal place."""
    if value is None:
        return " " * width
    s = f"{value:.1f}"
    return s.rjust(width)


# ============================================================================
# STEP 1 – DERIVE REPORT DATE FROM DPTRBL (first record, @106 TBDATE PD6.)
# ============================================================================

def derive_report_date() -> dict:
    """
    Read DPTRBL first record (OBS=1), column TBDATE (packed-decimal decoded
    as integer MMDDYYYY).
    REPTDATE  = today's report date
    REPTDAT1  = REPTDATE - 1
    """
    con = duckdb.connect()
    row = con.execute(
        f"SELECT TBDATE FROM read_parquet('{DPTRBL_FILE}') LIMIT 1"
    ).fetchone()
    con.close()

    if row is None:
        raise ValueError("DPTRBL file is empty.")

    tbdate_str = str(int(row[0])).zfill(8)          # MMDDYYYY
    reptdate   = datetime.strptime(tbdate_str, "%m%d%Y")
    reptdat1   = reptdate - timedelta(days=1)

    return {
        "reptdate"  : reptdate,
        "reptdat1"  : reptdat1,
        "rdatea"    : reptdate.strftime("%d/%m/%Y"),   # DDMMYY8 today
        "rdateb"    : reptdat1.strftime("%d/%m/%Y"),   # DDMMYY8 yesterday
        "rdate1"    : int(reptdate.strftime("%j")),    # Z5 today
        "rdate2"    : int(reptdat1.strftime("%j")),    # Z5 yesterday
    }


# ============================================================================
# STEP 2 – LOAD MIS.DDMV AND SPLIT INTO PREV / CURR
# ============================================================================

def load_ddmv() -> pl.DataFrame:
    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{DDMV_FILE}')").pl()
    con.close()
    return df


def _parse_reptdate(val) -> Optional[int]:
    """Normalise REPTDATE column to Z5 integer (Julian day-of-year)."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        # If stored as YYYYMMDD integer, convert to Julian Z5
        s = str(int(val))
        if len(s) == 8:
            try:
                return int(datetime.strptime(s, "%Y%m%d").strftime("%j"))
            except ValueError:
                pass
        return int(val)   # already Z5
    if isinstance(val, datetime):
        return int(val.strftime("%j"))
    try:
        return int(datetime.strptime(str(val)[:10], "%Y-%m-%d").strftime("%j"))
    except ValueError:
        return None


def build_prev_curr(ddmv: pl.DataFrame,
                    rdate1: int,
                    rdate2: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    DATA PREV – filter REPTDATE = &RDATE2, keep ACCTNO BRANCH OLDBAL NAME
    DATA CURR – filter REPTDATE = &RDATE1, keep ACCTNO BRANCH NEWBAL NAME
    """
    ddmv = ddmv.with_columns(
        pl.col("REPTDATE")
          .map_elements(_parse_reptdate, return_dtype=pl.Int64)
          .alias("REPTDATE_Z5")
    )

    prev = (
        ddmv.filter(pl.col("REPTDATE_Z5") == rdate2)
            .select(["ACCTNO", "BRANCH", "CURBAL", "NAME"])
            .rename({"CURBAL": "OLDBAL"})
            .sort("ACCTNO")
    )

    curr = (
        ddmv.filter(pl.col("REPTDATE_Z5") == rdate1)
            .select(["ACCTNO", "BRANCH", "CURBAL", "NAME"])
            .rename({"CURBAL": "NEWBAL"})
            .sort("ACCTNO")
    )

    return prev, curr


# ============================================================================
# STEP 3 – MERGE & FILTER ABS(NEWBAL - OLDBAL) >= 1,000,000
# ============================================================================

def build_ddmv(prev: pl.DataFrame, curr: pl.DataFrame) -> pl.DataFrame:
    """
    Outer merge on ACCTNO, fill nulls, filter movement >= RM1M.
    """
    merged = prev.join(curr, on="ACCTNO", how="outer", suffix="_C")

    # Resolve NAME and BRANCH from whichever side has data
    for col in ("NAME", "BRANCH"):
        col_c = f"{col}_C"
        if col_c in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(col).is_null())
                  .then(pl.col(col_c))
                  .otherwise(pl.col(col))
                  .alias(col)
            ).drop(col_c)

    merged = merged.with_columns([
        pl.col("OLDBAL").fill_null(0.0),
        pl.col("NEWBAL").fill_null(0.0),
    ])

    # ABS(NEWBAL - OLDBAL) >= 1,000,000
    merged = merged.filter(
        (pl.col("NEWBAL") - pl.col("OLDBAL")).abs() >= 1_000_000
    )

    return merged


# ============================================================================
# STEP 4 – SPLIT INTO CRMOVE / ODMOVE WITH MOVEMENT CALCULATION
# ============================================================================

def split_crmove_odmove(
        ddmv: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Replicate the DATA CRMOVE ODMOVE step exactly.

    Sign logic (all MOVEMENT values in RM millions, 2dp):

    Case A: OLDBAL < 0 AND NEWBAL >= 0
        ODMOVE: MOVEMENT = ROUND((0 + OLDBAL) * 0.000001, .01)
        CRMOVE: MOVEMENT = ROUND((0 + NEWBAL) * 0.000001, .01)  -- only if NEWBAL > 0

    Case B: OLDBAL >= 0 AND NEWBAL <= 0
        CRMOVE: MOVEMENT = ROUND((0 - OLDBAL) * 0.000001, .01)  -- only if OLDBAL > 0
        ODMOVE: MOVEMENT = ROUND((0 - NEWBAL) * 0.000001, .01)

    Case C: OLDBAL < 0 AND NEWBAL <= 0
        ODMOVE: MOVEMENT = ROUND((OLDBAL - NEWBAL) * 0.000001, .01)

    Case D (else – both positive or OLDBAL=0):
        CRMOVE: MOVEMENT = ROUND((NEWBAL - OLDBAL) * 0.000001, .01)
    """
    # Apply BRCHCD format from PBMISFMT
    def get_brch(branch) -> str:
        return format_brchcd(int(branch) if branch is not None else None)

    cr_rows: list[dict] = []
    od_rows: list[dict] = []

    for row in ddmv.to_dicts():
        oldbal = float(row.get("OLDBAL") or 0.0)
        newbal = float(row.get("NEWBAL") or 0.0)
        acctno = row.get("ACCTNO")
        name   = row.get("NAME") or ""
        branch = row.get("BRANCH")
        brch   = get_brch(branch)

        base = {"ACCTNO": acctno, "NAME": name,
                "BRANCH": branch, "BRCH": brch,
                "OLDBAL": oldbal, "NEWBAL": newbal}

        if oldbal < 0 and newbal >= 0:
            # Case A
            od_rows.append({**base,
                            "MOVEMENT": round((0 + oldbal) * 0.000001, 2)})
            if newbal > 0:
                cr_rows.append({**base,
                                "MOVEMENT": round((0 + newbal) * 0.000001, 2)})

        elif oldbal >= 0 and newbal <= 0:
            # Case B
            if oldbal > 0:
                cr_rows.append({**base,
                                "MOVEMENT": round((0 - oldbal) * 0.000001, 2)})
            od_rows.append({**base,
                            "MOVEMENT": round((0 - newbal) * 0.000001, 2)})

        elif oldbal < 0 and newbal <= 0:
            # Case C
            od_rows.append({**base,
                            "MOVEMENT": round((oldbal - newbal) * 0.000001, 2)})

        else:
            # Case D
            cr_rows.append({**base,
                            "MOVEMENT": round((newbal - oldbal) * 0.000001, 2)})

    schema = {
        "ACCTNO": pl.Int64, "NAME": pl.Utf8,
        "BRANCH": pl.Int64, "BRCH": pl.Utf8,
        "OLDBAL": pl.Float64, "NEWBAL": pl.Float64,
        "MOVEMENT": pl.Float64,
    }

    crmove = pl.DataFrame(cr_rows, schema=schema) if cr_rows else pl.DataFrame(schema=schema)
    odmove = pl.DataFrame(od_rows, schema=schema) if od_rows else pl.DataFrame(schema=schema)

    return crmove, odmove


# ============================================================================
# STEP 5 – WRITE REPORT  (two PROC REPORT sections)
# ============================================================================

# Column widths matching PROC REPORT DEFINE statements
COL_DEFS = [
    ("BRCH",     "$6",   6),
    ("ACCTNO",   "20.",  20),
    ("NAME",     "$25.", 25),
    ("OLDBAL",   "16.2", 16),
    ("NEWBAL",   "16.2", 16),
    ("MOVEMENT", "10.1", 10),
]

HEADERS = [
    ("BRANCH",          ""),
    ("ACCOUNT NUMBER",  ""),
    ("NAME OF CUSTOMER",""),
    ("YESTERDAYS BALANCE", ""),
    ("TODAYS     BALANCE", ""),
    ("INCREASE/",        "DECREASE/(RM MILLION)"),
]


def _col_widths() -> list[int]:
    return [c[2] for c in COL_DEFS]


def _header_lines() -> list[str]:
    widths = _col_widths()
    h1 = " ".join(HEADERS[i][0].center(widths[i])[:widths[i]]
                  for i in range(len(COL_DEFS)))
    h2 = " ".join(HEADERS[i][1].center(widths[i])[:widths[i]]
                  for i in range(len(COL_DEFS)))
    sep = " ".join("-" * w for w in widths)
    return [
        f"{ASA_NEWLINE}{h1}",
        f"{ASA_NEWLINE}{h2}",
        f"{ASA_NEWLINE}{sep}",
    ]


def _cell(col: str, val, width: int) -> str:
    if col == "BRCH":
        return str(val or "").ljust(width)[:width]
    if col == "ACCTNO":
        return str(int(val) if val is not None else "").rjust(width)
    if col == "NAME":
        return str(val or "").ljust(width)[:width]
    if col in ("OLDBAL", "NEWBAL"):
        return fmt_comma(float(val) if val is not None else None, width, 2)
    if col == "MOVEMENT":
        return fmt_10_1(float(val) if val is not None else None, width)
    return str(val or "").rjust(width)


def _write_section(df: pl.DataFrame,
                   title3: str,
                   title4_extra: str,
                   rdateb: str,
                   rdatea: str,
                   lines: list[str]) -> None:
    """Write one PROC REPORT block into `lines`."""
    widths = _col_widths()

    lines.append(f"{ASA_NEWPAGE}REPORT ID : DMMISR09")
    lines.append(f"{ASA_NEWLINE}")
    lines.append(f"{ASA_NEWLINE}{title3}")
    lines.append(
        f"{ASA_NEWLINE}IN THEIR CURRENT ACCOUNTS {rdateb} TO {rdatea}"
    )
    lines.append(f"{ASA_NEWLINE}")
    lines.extend(_header_lines())
    line_count = 8

    tot_old = tot_new = tot_mov = 0.0

    records = df.to_dicts()
    if not records:
        lines.append(f"{ASA_NEWLINE}  (NO RECORDS)")

    for row in records:
        if line_count >= PAGE_LINES:
            lines.append(f"{ASA_NEWPAGE}")
            lines.extend(_header_lines())
            line_count = 3

        old = float(row.get("OLDBAL")   or 0.0)
        new = float(row.get("NEWBAL")   or 0.0)
        mov = float(row.get("MOVEMENT") or 0.0)
        tot_old += old
        tot_new += new
        tot_mov += mov

        detail = " ".join(
            _cell(col, row.get(col), width)
            for col, _, width in COL_DEFS
        )
        lines.append(f"{ASA_NEWLINE}{detail}")
        line_count += 1

    # RBREAK AFTER / DUL OL SUMMARIZE
    sep_sgl = " ".join("-" * w for w in widths)
    sep_dbl = " ".join("=" * w for w in widths)
    blank_w = (COL_DEFS[0][2] + 1 + COL_DEFS[1][2] + 1 + COL_DEFS[2][2])
    summary = (
        f"{' ' * blank_w} "
        f"{fmt_comma(tot_old, COL_DEFS[3][2], 2)} "
        f"{fmt_comma(tot_new, COL_DEFS[4][2], 2)} "
        f"{fmt_10_1(tot_mov, COL_DEFS[5][2])}"
    )
    lines.append(f"{ASA_NEWLINE}{sep_sgl}")
    lines.append(f"{ASA_NEWLINE}{sep_dbl}")
    lines.append(f"{ASA_NEWLINE}{summary}")
    lines.append(f"{ASA_NEWLINE}{sep_dbl}")


def write_report(crmove: pl.DataFrame,
                 odmove: pl.DataFrame,
                 ctx: dict) -> None:
    lines: list[str] = []

    _write_section(
        crmove,
        title3      = "DEMAND DEPOSITS CREDIT MOVEMENT OF 1 MILLION AND ABOVE",
        title4_extra= "",
        rdateb      = ctx["rdateb"],
        rdatea      = ctx["rdatea"],
        lines       = lines,
    )

    _write_section(
        odmove,
        title3      = "DEMAND DEPOSITS OD MOVEMENT OF 1 MILLION AND ABOVE",
        title4_extra= "",
        rdateb      = ctx["rdateb"],
        rdatea      = ctx["rdatea"],
        lines       = lines,
    )

    with open(REPORT_FILE, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"Report written: {REPORT_FILE}")


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    print("DMMISR09 – Demand Deposits Credit/OD Movement starting...")

    ctx          = derive_report_date()
    print(f"  Report date : {ctx['rdatea']}  (previous: {ctx['rdateb']})")

    ddmv_raw     = load_ddmv()
    prev, curr   = build_prev_curr(ddmv_raw, ctx["rdate1"], ctx["rdate2"])
    ddmv         = build_ddmv(prev, curr)
    crmove, odmove = split_crmove_odmove(ddmv)

    write_report(crmove, odmove, ctx)

    print("DMMISR09 – Done.")


if __name__ == "__main__":
    main()
