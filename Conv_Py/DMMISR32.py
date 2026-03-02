#!/usr/bin/env python3
"""
Program : DMMISR32
Function: Credit Movement in Public Islamic Bank Berhad's Demand Deposits
          Net Increase/Decrease of RM 1 Million & Above per Customer
          Report ID : DMMISR32 (ISLAMIC)
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
DEP0_DIR      = os.path.join(BASE_DIR, "dep0")
CISC_DIR      = os.path.join(BASE_DIR, "cisc")     # SAP.PBB.CISBEXT.DP
OUTPUT_DIR    = os.path.join(BASE_DIR, "output")

# Input files
REPTDATE_FILE   = os.path.join(DEPOSIT_DIR, "REPTDATE.parquet")   # DEPOSIT.REPTDATE
CURRENT_FILE    = os.path.join(DEPOSIT_DIR, "CURRENT.parquet")    # DEPOSIT.CURRENT
DEP0_CURRENT    = os.path.join(DEP0_DIR,    "CURRENT.parquet")    # DEP0.CURRENT (previous)
CISC_DEPOSIT    = os.path.join(CISC_DIR,    "DEPOSIT.parquet")    # CISC.DEPOSIT

# Output report
REPORT_FILE     = os.path.join(OUTPUT_DIR, "DMMISR32.txt")

# Exclusion – account range
ACCTNO_EXCL_LO  = 3_790_000_000
ACCTNO_EXCL_HI  = 3_799_999_999

# Product exclusion
PRODUCT_EXCL    = {66, 67}

# ASA carriage-control
ASA_NEWPAGE = "1"
ASA_NEWLINE = " "

PAGE_LINES  = 60

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================================
# HELPERS
# ============================================================================

def fmt_comma(value: Optional[float], width: int = 15, dec: int = 2) -> str:
    if value is None:
        return " " * width
    s = f"{value:,.{dec}f}"
    return s.rjust(width)


def fmt_10_2(value: Optional[float], width: int = 10) -> str:
    if value is None:
        return " " * width
    s = f"{value:.2f}"
    return s.rjust(width)


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

    return {
        "reptdate" : reptdate,
        "reptmon"  : reptdate.strftime("%m"),
        "reptyear" : reptdate.strftime("%Y"),
        "rdate"    : reptdate.strftime("%d/%m/%Y"),
        "zdate"    : int(reptdate.strftime("%j")),
    }


# ============================================================================
# STEP 2 – LOAD PREV (DEP0.CURRENT)
# ============================================================================

def load_prev() -> pl.DataFrame:
    """Keep ACCTNO and PREBAL (= CURBAL, floor at 0 not applied here –
    same as SAS: PREBAL=CURBAL with no floor)."""
    con  = duckdb.connect()
    prev = con.execute(
        f"SELECT ACCTNO, CURBAL AS PREBAL FROM read_parquet('{DEP0_CURRENT}')"
    ).pl()
    con.close()
    return prev.sort("ACCTNO")


# ============================================================================
# STEP 3 – BUILD CRMOVE
# ============================================================================

def build_crmove(prev: pl.DataFrame) -> pl.DataFrame:
    """
    Load DEPOSIT.CURRENT, merge PREV by ACCTNO.
    Apply BRCHCD, MOVEMENT, MOVEX filters.
    Exclude products 66/67 and the 379x account range.
    """
    con  = duckdb.connect()
    curr = con.execute(f"SELECT * FROM read_parquet('{CURRENT_FILE}')").pl()
    con.close()

    # Merge PREV (left join, keep A = CURRENT rows)
    df = curr.join(prev, on="ACCTNO", how="left")

    # BRCH from PBMISFMT
    df = df.with_columns(
        pl.col("BRANCH")
          .map_elements(lambda b: format_brchcd(int(b) if b is not None else None),
                        return_dtype=pl.Utf8)
          .alias("BRCH")
    )

    # Fill missing PREBAL with 0
    df = df.with_columns(pl.col("PREBAL").fill_null(0.0))

    # Compute MOVEMENT and MOVEX
    df = df.with_columns([
        (pl.col("CURBAL") - pl.col("PREBAL")).alias("MOVEMENT"),
        (pl.col("CURBAL") - pl.col("PREBAL")).abs().alias("MOVEX"),
    ])

    # Apply filters
    df = df.filter(
        (pl.col("MOVEX") >= 1_000_000) &
        (~pl.col("PRODUCT").cast(pl.Int64).is_in(list(PRODUCT_EXCL))) &
        ~((pl.col("ACCTNO") >= ACCTNO_EXCL_LO) & (pl.col("ACCTNO") <= ACCTNO_EXCL_HI))
    )

    return df.sort("ACCTNO")


# ============================================================================
# STEP 4 – LOAD CIS CUSTOMER NAMES (CISC.DEPOSIT)
# ============================================================================

def load_cisn() -> pl.DataFrame:
    if not os.path.exists(CISC_DEPOSIT):
        return pl.DataFrame({
            "ACCTNO"  : pl.Series([], dtype=pl.Int64),
            "CUSTNAM1": pl.Series([], dtype=pl.Utf8),
        })

    con = duckdb.connect()
    cisn = con.execute(f"""
        SELECT ACCTNO, CUSTNAM1
        FROM   read_parquet('{CISC_DEPOSIT}')
        WHERE  SECCUST = '901'
    """).pl()
    con.close()
    return cisn.unique(subset=["ACCTNO"], keep="first").sort("ACCTNO")


# ============================================================================
# STEP 5 – ENRICH CRMOVE WITH CIS, SORT
# ============================================================================

def enrich_crmove(crmove: pl.DataFrame, cisn: pl.DataFrame) -> pl.DataFrame:
    """
    Merge CISN onto CRMOVE (MERGE CISN CRMOVE(IN=A); IF A).
    Apply CUSTNAM1 fallback to NAME if blank.
    Sort DESCENDING MOVEMENT / ACCTNO.
    """
    crmove = crmove.join(cisn, on="ACCTNO", how="left")

    # IF CUSTNAM1=' ' THEN CUSTNAM1=NAME
    if "CUSTNAM1" not in crmove.columns:
        crmove = crmove.with_columns(pl.lit(None).cast(pl.Utf8).alias("CUSTNAM1"))

    crmove = crmove.with_columns(
        pl.when(pl.col("CUSTNAM1").is_null() | (pl.col("CUSTNAM1").str.strip_chars() == ""))
          .then(pl.col("NAME") if "NAME" in crmove.columns else pl.lit(""))
          .otherwise(pl.col("CUSTNAM1"))
          .alias("CUSTNAM1")
    )

    return crmove.sort(["MOVEMENT", "ACCTNO"], descending=[True, False])


# ============================================================================
# STEP 6 – WRITE REPORT  (PROC REPORT equivalent)
# ============================================================================

COL_DEFS = [
    ("BRANCH",   6),
    ("BRCH",     6),
    ("CUSTNAM1", 40),
    ("ACCTNO",   20),
    ("PREBAL",   15),
    ("CURBAL",   15),
    ("MOVEMT",   10),
]

HEADERS = [
    ("BRANCH",           ""),
    ("/CODE ",            ""),
    ("NAME OF CUSTOMER", ""),
    ("ACCOUNT NUMBER",   ""),
    ("PREVIOUS BALANCE", ""),
    ("CURRENT BALANCE",  ""),
    ("INCREASE/",        "DECREASE/(RM MILLION)"),
]


def _widths() -> list[int]:
    return [c[1] for c in COL_DEFS]


def _header_lines() -> list[str]:
    widths = _widths()
    h1 = " ".join(HEADERS[i][0].center(widths[i])[:widths[i]]
                  for i in range(len(COL_DEFS)))
    h2 = " ".join(HEADERS[i][1].center(widths[i])[:widths[i]]
                  for i in range(len(COL_DEFS)))
    sep = " ".join("-" * w for w in widths)
    return [f"{ASA_NEWLINE}{h1}", f"{ASA_NEWLINE}{h2}", f"{ASA_NEWLINE}{sep}"]


def _cell(col: str, val, width: int) -> str:
    if col == "BRANCH":
        return str(int(val) if val is not None else "").rjust(width)
    if col == "BRCH":
        return str(val or "").ljust(width)[:width]
    if col == "CUSTNAM1":
        return str(val or "").ljust(width)[:width]
    if col == "ACCTNO":
        return str(int(val) if val is not None else "").rjust(width)
    if col in ("PREBAL", "CURBAL"):
        return fmt_comma(float(val) if val is not None else None, width, 2)
    if col == "MOVEMT":
        return fmt_10_2(float(val) if val is not None else None, width)
    return str(val or "").rjust(width)


def write_report(crmove: pl.DataFrame, rdate: str) -> None:
    lines: list[str] = []
    widths = _widths()

    # Title block
    lines.append(f"{ASA_NEWPAGE}REPORT ID : DMMISR32(ISLAMIC)")
    lines.append(f"{ASA_NEWLINE}SALES ADMINISTRATION & SUPPORT")
    lines.append(f"{ASA_NEWLINE}PUBLIC ISLAMIC BANK BERHAD")
    lines.append(f"{ASA_NEWLINE}CREDIT MOVEMENT IN BANK'S DEMAND DEPOSITS AS AT {rdate}")
    lines.append(f"{ASA_NEWLINE}NET INCREASE/DECREASE OF RM 1 MILLION & ABOVE PER CUSTOMER")
    lines.append(f"{ASA_NEWLINE}")

    # Empty-data message
    if crmove.height == 0:
        lines.append(f"{ASA_NEWLINE}")
        lines.append(f"{ASA_NEWLINE}")
        lines.append(f"{ASA_NEWLINE}     ************************************************************")
        lines.append(f"{ASA_NEWLINE}     NO CUSTOMER WITH SPTF CREDIT MOVEMENT OF 1 MILLION AND ABOVE")
        lines.append(f"{ASA_NEWLINE}     AT {rdate}")
        lines.append(f"{ASA_NEWLINE}     ************************************************************")
        with open(REPORT_FILE, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
        print(f"Report written (empty): {REPORT_FILE}")
        return

    lines.extend(_header_lines())
    line_count = 8

    tot_pre = tot_cur = tot_mov = 0.0

    for row in crmove.to_dicts():
        if line_count >= PAGE_LINES:
            lines.extend(_header_lines())
            line_count = 3

        prebal   = float(row.get("PREBAL")   or 0.0)
        curbal   = float(row.get("CURBAL")   or 0.0)
        movement = float(row.get("MOVEMENT") or 0.0)
        movemt   = movement * 0.000001

        tot_pre += prebal
        tot_cur += curbal
        tot_mov += movemt

        vals = {
            "BRANCH"  : row.get("BRANCH"),
            "BRCH"    : row.get("BRCH"),
            "CUSTNAM1": row.get("CUSTNAM1"),
            "ACCTNO"  : row.get("ACCTNO"),
            "PREBAL"  : prebal,
            "CURBAL"  : curbal,
            "MOVEMT"  : movemt,
        }
        detail = " ".join(_cell(col, vals[col], width) for col, width in COL_DEFS)
        lines.append(f"{ASA_NEWLINE}{detail}")
        line_count += 1

    # RBREAK AFTER / PAGE DUL OL SUMMARIZE
    sep_sgl = " ".join("-" * w for w in widths)
    sep_dbl = " ".join("=" * w for w in widths)

    blank_w = widths[0] + 1 + widths[1] + 1 + widths[2] + 1 + widths[3]
    summary = (
        f"{' ' * blank_w} "
        f"{fmt_comma(tot_pre, widths[4], 2)} "
        f"{fmt_comma(tot_cur, widths[5], 2)} "
        f"{fmt_10_2(tot_mov, widths[6])}"
    )
    lines.append(f"{ASA_NEWLINE}{sep_sgl}")
    lines.append(f"{ASA_NEWLINE}{sep_dbl}")
    lines.append(f"{ASA_NEWLINE}{summary}")
    lines.append(f"{ASA_NEWLINE}{sep_dbl}")

    with open(REPORT_FILE, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"Report written: {REPORT_FILE}")


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    print("DMMISR32 – Islamic Demand Deposits Credit Movement (RM1M+) starting...")

    ctx    = derive_report_date()
    print(f"  Report date : {ctx['rdate']}")

    prev   = load_prev()
    crmove = build_crmove(prev)
    cisn   = load_cisn()
    crmove = enrich_crmove(crmove, cisn)

    write_report(crmove, ctx["rdate"])

    print("DMMISR32 – Done.")


if __name__ == "__main__":
    main()
