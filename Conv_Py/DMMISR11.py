#!/usr/bin/env python3
"""
Program : DMMISR11
Function: Movement in Bank's ACE Account Balance
          Net Increase/Decrease of RM100K & Above per Customer
          Report ID : DMMISR11
"""

import duckdb
import polars as pl
import os
from datetime import datetime
from typing import Optional

# Import format definitions from PBBDPFMT and PBMISFMT
from PBBDPFMT import ProductLists
from PBMISFMT import format_brchcd

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR      = os.environ.get("BASE_DIR",  "/data")
DEPOSIT_DIR   = os.path.join(BASE_DIR, "deposit")
MIS_DIR       = os.path.join(BASE_DIR, "mis")        # SAP.PBB.MIS.D<YEAR>
OUTPUT_DIR    = os.path.join(BASE_DIR, "output")

# Input files
REPTDATE_FILE = os.path.join(DEPOSIT_DIR, "REPTDATE.parquet")  # DEPOSIT.REPTDATE

# Output report
REPORT_FILE   = os.path.join(OUTPUT_DIR, "DMMISR11.txt")

# ACE products from PBBDPFMT ProductLists
ACE_PRODUCTS  = ProductLists.ACE_PRODUCTS   # {40, 42, 43, 150, 151, 152, 181}

# Minimum absolute movement threshold
MOVEMENT_THRESHOLD = 100_000

# ASA carriage-control
ASA_NEWPAGE = "1"
ASA_NEWLINE = " "

PAGE_LINES  = 60

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================================
# HELPERS
# ============================================================================

def fmt_comma10_2(value: Optional[float], width: int = 10) -> str:
    """COMMA10.2 right-aligned."""
    if value is None:
        return " " * width
    s = f"{value:,.2f}"
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
        "zdate"    : int(reptdate.strftime("%j")),    # Z5 Julian
    }


# ============================================================================
# STEP 2 – LOAD MIS.DYMVNT<MM> AND BUILD ACEMOVE
# ============================================================================

def _parse_z5(val) -> Optional[int]:
    """Normalise REPTDATE to Z5 Julian integer."""
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


def build_acemove(ctx: dict) -> pl.DataFrame:
    """
    Load MIS.DYMVNT<MM>.
    Filter: DEPTYPE in ('D','N') AND PRODUCT in ACE AND REPTDATE = ZDATE.
    Filter: ABS(CREDIT-DEBIT) >= 100,000.
    Compute: MOVEMENT = ROUND(CREDIT-DEBIT, 10000) / 1,000,000  (RM millions, 2dp).
    Apply BRCHCD format.
    Sort DESCENDING MOVEMENT.
    """
    path = os.path.join(MIS_DIR, f"DYMVNT{ctx['reptmon']}.parquet")
    con  = duckdb.connect()
    df   = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()

    df = df.with_columns(
        pl.col("REPTDATE")
          .map_elements(_parse_z5, return_dtype=pl.Int64)
          .alias("REPTDATE_Z5")
    )

    # Filter DEPTYPE, PRODUCT (ACE), REPTDATE
    df = df.filter(
        pl.col("DEPTYPE").is_in(["D", "N"]) &
        pl.col("PRODUCT").cast(pl.Int64).is_in(list(ACE_PRODUCTS)) &
        (pl.col("REPTDATE_Z5") == ctx["zdate"])
    )

    # Filter ABS(CREDIT-DEBIT) >= 100,000
    df = df.filter(
        (pl.col("CREDIT") - pl.col("DEBIT")).abs() >= MOVEMENT_THRESHOLD
    )

    # MOVEMENT = ROUND(CREDIT-DEBIT, 10000) / 1,000,000
    # SAS ROUND(x, 10000) rounds to nearest 10,000
    df = df.with_columns(
        (((pl.col("CREDIT") - pl.col("DEBIT")) / 10_000).round(0) * 10_000 / 1_000_000)
        .alias("MOVEMENT")
    )

    # BRCH from PBMISFMT
    df = df.with_columns(
        pl.col("BRANCH")
          .map_elements(lambda b: format_brchcd(int(b) if b is not None else None),
                        return_dtype=pl.Utf8)
          .alias("BRCH")
    )

    return df.sort("MOVEMENT", descending=True)


# ============================================================================
# STEP 3 – WRITE REPORT  (PROC PRINT equivalent)
# ============================================================================

# Columns rendered: REPTDATE BRANCH BRCH NAME ACCTNO MOVEMENT
# Widths match SAS PROC PRINT label/format widths
COL_DEFS = [
    ("REPTDATE", 10),   # DDMMYY8. = 8 chars, padded to 10
    ("BRANCH",    6),   # FORMAT=6.
    ("BRCH",      6),   # $6.  (label "/CODE")
    ("NAME",     32),   # default character width
    ("ACCTNO",   20),   # default
    ("MOVEMENT", 10),   # COMMA10.2
]

LABELS = [
    ("DATE",          ""),
    ("BRANCH",        ""),
    ("/CODE",         ""),
    ("NAME",          ""),
    ("ACCTNO",        ""),
    ("INC/DEC",       "(RM 'MIL)"),
]


def _widths() -> list[int]:
    return [c[1] for c in COL_DEFS]


def _header_lines() -> list[str]:
    widths = _widths()
    h1  = " ".join(LABELS[i][0].center(widths[i])[:widths[i]]
                   for i in range(len(COL_DEFS)))
    h2  = " ".join(LABELS[i][1].center(widths[i])[:widths[i]]
                   for i in range(len(COL_DEFS)))
    sep = " ".join("-" * w for w in widths)
    return [f"{ASA_NEWLINE}{h1}", f"{ASA_NEWLINE}{h2}", f"{ASA_NEWLINE}{sep}"]


def _fmt_reptdate(val) -> str:
    """Format REPTDATE as DDMMYY8. (DD/MM/YY)."""
    if val is None:
        return " " * 10
    if isinstance(val, datetime):
        return val.strftime("%d/%m/%y").rjust(10)
    if isinstance(val, (int, float)):
        s = str(int(val))
        if len(s) == 8:
            try:
                dt = datetime.strptime(s, "%Y%m%d")
                return dt.strftime("%d/%m/%y").rjust(10)
            except ValueError:
                pass
    try:
        dt = datetime.strptime(str(val)[:10], "%Y-%m-%d")
        return dt.strftime("%d/%m/%y").rjust(10)
    except ValueError:
        return str(val)[:10].rjust(10)


def _cell(col: str, val, width: int) -> str:
    if col == "REPTDATE":
        return _fmt_reptdate(val)
    if col == "BRANCH":
        return str(int(val) if val is not None else "").rjust(width)
    if col == "BRCH":
        return str(val or "").ljust(width)[:width]
    if col == "NAME":
        return str(val or "").ljust(width)[:width]
    if col == "ACCTNO":
        return str(int(val) if val is not None else "").rjust(width)
    if col == "MOVEMENT":
        return fmt_comma10_2(float(val) if val is not None else None, width)
    return str(val or "").rjust(width)


def write_report(acemove: pl.DataFrame, ctx: dict) -> None:
    lines: list[str] = []
    widths = _widths()
    rdate  = ctx["rdate"]

    # Title block
    lines.append(f"{ASA_NEWPAGE}REPORT ID : DMMISR11")
    lines.append(f"{ASA_NEWLINE}PUBLIC BANK BERHAD")
    lines.append(f"{ASA_NEWLINE}PRODUCT DEVELOPMENT & MARKETING")
    lines.append(f"{ASA_NEWLINE}MOVEMENT IN BANK'S ACE ACCOUNT BALANCE AS AT {rdate}")
    lines.append(f"{ASA_NEWLINE}NET INCREASE/DECREASE OF RM100K & ABOVE PER CUSTOMER")
    lines.append(f"{ASA_NEWLINE}")

    # Empty-data guard
    if acemove.height == 0:
        lines.append(f"{ASA_NEWLINE}")
        lines.append(f"{ASA_NEWLINE}")
        lines.append(f"{ASA_NEWLINE}     ***************************************************")
        lines.append(f"{ASA_NEWLINE}     NO CUSTOMER WITH MOVEMENT OF 100 THOUSAND AND ABOVE")
        lines.append(f"{ASA_NEWLINE}     AT {rdate}")
        lines.append(f"{ASA_NEWLINE}     ***************************************************")
        with open(REPORT_FILE, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
        print(f"Report written (empty): {REPORT_FILE}")
        return

    lines.extend(_header_lines())
    line_count = 9

    tot_movement = 0.0

    for row in acemove.to_dicts():
        if line_count >= PAGE_LINES:
            lines.extend(_header_lines())
            line_count = 3

        movement = float(row.get("MOVEMENT") or 0.0)
        tot_movement += movement

        detail = " ".join(
            _cell(col, row.get(col), width)
            for col, width in COL_DEFS
        )
        lines.append(f"{ASA_NEWLINE}{detail}")
        line_count += 1

    # SUM MOVEMENT – single underline then total row (PROC PRINT SUM)
    sep = " ".join("-" * w for w in widths)
    blank_w = sum(widths[:5]) + 5  # 5 gap spaces for first 5 cols
    sum_row = (
        f"{' ' * blank_w}"
        f"{fmt_comma10_2(tot_movement, widths[5])}"
    )
    lines.append(f"{ASA_NEWLINE}{sep}")
    lines.append(f"{ASA_NEWLINE}{sum_row}")
    lines.append(f"{ASA_NEWLINE}{sep}")

    with open(REPORT_FILE, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"Report written: {REPORT_FILE}")


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    print("DMMISR11 – ACE Account Balance Movement (RM100K+) starting...")

    ctx     = derive_report_date()
    print(f"  Report date : {ctx['rdate']}")

    acemove = build_acemove(ctx)
    write_report(acemove, ctx)

    print("DMMISR11 – Done.")


if __name__ == "__main__":
    main()
