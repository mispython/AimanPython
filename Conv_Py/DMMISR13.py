#!/usr/bin/env python3
"""
Program : DMMISR13
Function: Daily Movement of Saving Deposits and Demand Deposits by Branch
          Report ID : DMMISR13
          Produces two report sections:
            - Saving Deposits  (SAVMVNT) → PMMISR13
            - Demand Deposits  (DDMVNT)  → PMMISR23
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

BASE_DIR      = os.environ.get("BASE_DIR",   "/data")
DEPOSIT_DIR   = os.path.join(BASE_DIR, "deposit")
MIS_DIR       = os.path.join(BASE_DIR, "mis")        # SAP.PBB.MIS.D<YEAR>
OUTPUT_DIR    = os.path.join(BASE_DIR, "output")

# Input files
REPTDATE_FILE = os.path.join(DEPOSIT_DIR, "REPTDATE.parquet")  # DEPOSIT.REPTDATE

# Output report files (PMMISR13 and PMMISR23 per PROC PRINTTO)
PMMISR13_FILE = os.path.join(OUTPUT_DIR, "PMMISR13.txt")   # Saving deposits report
PMMISR23_FILE = os.path.join(OUTPUT_DIR, "PMMISR23.txt")   # Demand deposits report

# ASA carriage-control
ASA_NEWPAGE = "1"
ASA_NEWLINE = " "

PAGE_LINES  = 60

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================================
# HELPERS
# ============================================================================

def fmt_comma(value: Optional[float], width: int = 18, dec: int = 2) -> str:
    """COMMA18.2 right-aligned."""
    if value is None:
        return " " * width
    s = f"{value:,.{dec}f}"
    return s.rjust(width)


def fmt_comma20(value: Optional[float]) -> str:
    """COMMA20.2 right-aligned."""
    return fmt_comma(value, 20, 2)


# ============================================================================
# STEP 1 – DERIVE REPORT DATE FROM DEPOSIT.REPTDATE
# ============================================================================

def derive_report_date() -> dict:
    con = duckdb.connect()
    row = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{REPTDATE_FILE}') LIMIT 1"
    ).fetchone()
    con.close()

    if row is None:
        raise ValueError("REPTDATE file is empty.")

    val = row[0]
    if isinstance(val, (int, float)):
        reptdate = datetime.strptime(str(int(val)), "%Y%m%d")
    elif isinstance(val, datetime):
        reptdate = val
    else:
        reptdate = datetime.strptime(str(val)[:10], "%Y-%m-%d")

    reptdat1 = reptdate - timedelta(days=1)

    return {
        "reptdate" : reptdate,
        "reptdat1" : reptdat1,
        "reptmon"  : reptdate.strftime("%m"),
        "reptyear" : reptdate.strftime("%Y"),
        "rdate"    : reptdate.strftime("%d/%m/%Y"),
        "zdate"    : int(reptdate.strftime("%j")),    # Z5 today
        "pdate"    : int(reptdat1.strftime("%j")),    # Z5 previous day
    }


# ============================================================================
# STEP 2 – LOAD MIS.DYDPBS<MM> AND SPLIT INTO SAV / DD × PREV / TODAY
# ============================================================================

def _parse_reptdate_z5(val) -> Optional[int]:
    """Normalise REPTDATE to Z5 integer (Julian day-of-year)."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        s = str(int(val))
        if len(s) == 8:
            try:
                return int(datetime.strptime(s, "%Y%m%d").strftime("%j"))
            except ValueError:
                pass
        return int(val)
    if isinstance(val, datetime):
        return int(val.strftime("%j"))
    try:
        return int(datetime.strptime(str(val)[:10], "%Y-%m-%d").strftime("%j"))
    except ValueError:
        return None


def load_and_split(ctx: dict) -> tuple[pl.DataFrame, pl.DataFrame,
                                       pl.DataFrame, pl.DataFrame]:
    """
    Load MIS.DYDPBS<MM>, split by DPTYPE and REPTDATE into:
      SAVPRE  – DPTYPE='S', REPTDATE=PDATE  (BALANCE renamed PREBAL)
      SAVTOD  – DPTYPE='S', REPTDATE=ZDATE
      DDPRE   – DPTYPE in ('D','N'), REPTDATE=PDATE  (BALANCE renamed PREBAL)
      DDTOD   – DPTYPE in ('D','N'), REPTDATE=ZDATE
    """
    mon      = ctx["reptmon"]
    zdate    = ctx["zdate"]
    pdate    = ctx["pdate"]
    src_file = os.path.join(MIS_DIR, f"DYDPBS{mon}.parquet")

    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{src_file}')").pl()
    con.close()

    df = df.with_columns(
        pl.col("REPTDATE")
          .map_elements(_parse_reptdate_z5, return_dtype=pl.Int64)
          .alias("REPTDATE_Z5")
    )

    savpre = (
        df.filter((pl.col("DPTYPE") == "S") & (pl.col("REPTDATE_Z5") == pdate))
          .rename({"BALANCE": "PREBAL"})
          .sort("BRANCH")
    )
    savtod = (
        df.filter((pl.col("DPTYPE") == "S") & (pl.col("REPTDATE_Z5") == zdate))
          .sort("BRANCH")
    )
    ddpre = (
        df.filter(pl.col("DPTYPE").is_in(["D", "N"]) & (pl.col("REPTDATE_Z5") == pdate))
          .rename({"BALANCE": "PREBAL"})
          .sort("BRANCH")
    )
    ddtod = (
        df.filter(pl.col("DPTYPE").is_in(["D", "N"]) & (pl.col("REPTDATE_Z5") == zdate))
          .sort("BRANCH")
    )

    return savpre, savtod, ddpre, ddtod


# ============================================================================
# STEP 3 – MERGE AND APPLY BRCHCD FORMAT
# ============================================================================

def build_movement_df(pre: pl.DataFrame,
                      tod: pl.DataFrame) -> pl.DataFrame:
    """
    Outer merge on BRANCH, apply BRCHCD format from PBMISFMT.
    BRCH = format_brchcd(BRANCH).
    """
    merged = pre.join(tod, on="BRANCH", how="outer", suffix="_T")

    # Resolve REPTDATE from either side (keep today's if present)
    for col in ("REPTDATE", "REPTDATE_Z5"):
        col_t = f"{col}_T"
        if col_t in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(col).is_null())
                  .then(pl.col(col_t))
                  .otherwise(pl.col(col))
                  .alias(col)
            ).drop(col_t)

    # Apply BRCHCD format
    def get_brch(branch) -> str:
        return format_brchcd(int(branch) if branch is not None else None)

    merged = merged.with_columns(
        pl.col("BRANCH")
          .map_elements(get_brch, return_dtype=pl.Utf8)
          .alias("BRCH")
    )

    return merged.sort("BRANCH")


# ============================================================================
# STEP 4 – WRITE REPORT SECTION  (PROC REPORT equivalent)
# ============================================================================

# Column widths matching PROC REPORT DEFINE statements
# REPTDATE is NOPRINT so omitted from rendered columns
REPORT_COL_DEFS = [
    ("BRANCH",   "6.",    6),
    ("BRCH",     "$6.",   6),
    ("PREBAL",   "18.2", 18),
    ("BALANCE",  "18.2", 18),
    ("MOVEMENT", "20.2", 20),
]

REPORT_HEADERS = [
    ("BRANCH",       ""),
    ("(ABBR)",        ""),
    ("PREBAL",        ""),     # no explicit header label in DEFINE
    ("BALANCE",       ""),     # no explicit header label in DEFINE
    ("NET INCREASE/", "DECREASE (A-B)"),
]


def _col_widths() -> list[int]:
    return [c[2] for c in REPORT_COL_DEFS]


def _header_lines() -> list[str]:
    widths = _col_widths()
    h1 = " ".join(REPORT_HEADERS[i][0].center(widths[i])[:widths[i]]
                  for i in range(len(REPORT_COL_DEFS)))
    h2 = " ".join(REPORT_HEADERS[i][1].center(widths[i])[:widths[i]]
                  for i in range(len(REPORT_COL_DEFS)))
    sep = " ".join("-" * w for w in widths)
    return [
        f"{ASA_NEWLINE}{h1}",
        f"{ASA_NEWLINE}{h2}",
        f"{ASA_NEWLINE}{sep}",
    ]


def _cell_val(col: str, val, width: int) -> str:
    if col == "BRANCH":
        return str(int(val) if val is not None else "").rjust(width)
    if col == "BRCH":
        return str(val or "").ljust(width)[:width]
    if col in ("PREBAL", "BALANCE"):
        return fmt_comma(float(val) if val is not None else None, width, 2)
    if col == "MOVEMENT":
        return fmt_comma20(float(val) if val is not None else None)
    return str(val or "").rjust(width)


def write_section(df: pl.DataFrame,
                  title1: str,
                  title4: str,
                  rdate: str,
                  output_file: str) -> None:
    """
    Write one PROC REPORT block (RBREAK AFTER / DUL OL SUMMARIZE) to
    the designated output file.
    """
    lines: list[str] = []
    lines.append(f"{ASA_NEWPAGE}REPORT ID : DMMISR13")
    lines.append(f"{ASA_NEWLINE}PUBLIC BANK BERHAD")
    lines.append(f"{ASA_NEWLINE}PRODUCT DEVELOPMENT & MARKETING")
    lines.append(f"{ASA_NEWLINE}{title4} {rdate}")
    lines.append(f"{ASA_NEWLINE}")
    lines.extend(_header_lines())
    line_count = 7

    tot_pre = tot_bal = 0.0
    widths   = _col_widths()
    records  = df.to_dicts()

    if not records:
        lines.append(f"{ASA_NEWLINE}  (NO RECORDS)")

    for row in records:
        if line_count >= PAGE_LINES:
            lines.append(f"{ASA_NEWPAGE}")
            lines.extend(_header_lines())
            line_count = 3

        prebal  = float(row.get("PREBAL")  or 0.0)
        balance = float(row.get("BALANCE") or 0.0)
        movement = balance - prebal

        tot_pre += prebal
        tot_bal += balance

        # Render row: BRANCH BRCH PREBAL BALANCE MOVEMENT
        vals = {
            "BRANCH"  : row.get("BRANCH"),
            "BRCH"    : row.get("BRCH"),
            "PREBAL"  : prebal,
            "BALANCE" : balance,
            "MOVEMENT": movement,
        }
        detail = " ".join(
            _cell_val(col, vals[col], width)
            for col, _, width in REPORT_COL_DEFS
        )
        lines.append(f"{ASA_NEWLINE}{detail}")
        line_count += 1

    # RBREAK AFTER / DUL OL SUMMARIZE
    sep_sgl  = " ".join("-" * w for w in widths)
    sep_dbl  = " ".join("=" * w for w in widths)
    tot_move = tot_bal - tot_pre
    blank_w  = widths[0] + 1 + widths[1]
    summary  = (
        f"{' ' * blank_w} "
        f"{fmt_comma(tot_pre, widths[2], 2)} "
        f"{fmt_comma(tot_bal, widths[3], 2)} "
        f"{fmt_comma20(tot_move)}"
    )
    lines.append(f"{ASA_NEWLINE}{sep_sgl}")
    lines.append(f"{ASA_NEWLINE}{sep_dbl}")
    lines.append(f"{ASA_NEWLINE}{summary}")
    lines.append(f"{ASA_NEWLINE}{sep_dbl}")

    with open(output_file, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"Report written: {output_file}")


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    print("DMMISR13 – Daily Movement of Saving/Demand Deposits by Branch starting...")

    # Step 1: derive dates
    ctx = derive_report_date()
    print(f"  Report date : {ctx['rdate']}")

    # Step 2: load and split MIS.DYDPBS<MM>
    savpre, savtod, ddpre, ddtod = load_and_split(ctx)

    # Step 3: merge and apply BRCHCD format
    savmvnt = build_movement_df(savpre, savtod)
    ddmvnt  = build_movement_df(ddpre,  ddtod)

    # Step 4: write reports
    # PROC PRINTTO PRINT=PMMISR13 – Saving deposits
    write_section(
        savmvnt,
        title1  = "REPORT ID : DMMISR13",
        title4  = "DAILY MOVEMENT OF SAVING DEPOSITS BY BRANCH AS AT",
        rdate   = ctx["rdate"],
        output_file = PMMISR13_FILE,
    )

    # PROC PRINTTO PRINT=PMMISR23 – Demand deposits
    write_section(
        ddmvnt,
        title1  = "REPORT ID : DMMISR13",
        title4  = "DAILY MOVEMENT OF DEMAND DEPOSITS BY BRANCH AS AT",
        rdate   = ctx["rdate"],
        output_file = PMMISR23_FILE,
    )

    print("DMMISR13 – Done.")


if __name__ == "__main__":
    main()
