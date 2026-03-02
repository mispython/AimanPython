#!/usr/bin/env python3
"""
Program : DMMISR42
Function: Credit Movement in Bank's Demand Deposits
          Daily Net Increase/Decrease of RM 1 Million & Above per Account (Conventional)
          Report ID : DMMISR42
"""

import duckdb
import polars as pl
import os
from datetime import datetime, timedelta
from typing import Optional

# Import format definitions from PBBDPFMT
from PBBDPFMT import CAProductFormat

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR        = os.environ.get("BASE_DIR",     "/data")
DEPOSIT_DIR     = os.path.join(BASE_DIR, "deposit")
DEP0_DIR        = os.path.join(BASE_DIR, "dep0")
CIS_DIR         = os.path.join(BASE_DIR, "cis")
OUTPUT_DIR      = os.path.join(BASE_DIR, "output")

# Input files
REPTDATE_FILE   = os.path.join(DEPOSIT_DIR, "REPTDATE.parquet")    # DEPOSIT.REPTDATE
CURRENT_FILE    = os.path.join(DEPOSIT_DIR, "CURRENT.parquet")     # DEPOSIT.CURRENT
DEP0_CURRENT    = os.path.join(DEP0_DIR,    "CURRENT.parquet")     # DEP0.CURRENT (previous day)
BRHFILE         = os.path.join(BASE_DIR,    "BRHFILE.txt")         # Branch reference file

# Output report
REPORT_FILE     = os.path.join(OUTPUT_DIR, "DMMISR42.txt")

# ASA carriage-control
ASA_NEWPAGE  = "1"
ASA_NEWLINE  = " "
ASA_SKIP2    = "0"

PAGE_LINES   = 60   # lines per page (default)

# Products / PRODCD values to delete (exclude)
PRODCD_EXCL  = {"N", "42610"}
PRODUCT_EXCL = {30, 31, 34, 79, 68, 69, 72, 104, 105}

# Account number range to exclude
ACCTNO_EXCL_LO = 3590000000
ACCTNO_EXCL_HI = 3599999999

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================================
# HELPERS
# ============================================================================

def fmt_comma(value: Optional[float], width: int = 14, dec: int = 2) -> str:
    """Right-aligned comma-formatted number."""
    if value is None:
        return " " * width
    s = f"{value:,.{dec}f}"
    return s.rjust(width)


def fmt_negparen(value: Optional[float], width: int = 11, dec: int = 2) -> str:
    """Parentheses for negatives, right-aligned."""
    if value is None:
        return " " * width
    if value < 0:
        s = f"({abs(value):,.{dec}f})"
    else:
        s = f"{value:,.{dec}f}"
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

    reptdate_val = row[0]
    # REPTDATE may be stored as an integer YYYYMMDD or a date object
    if isinstance(reptdate_val, (int, float)):
        reptdate = datetime.strptime(str(int(reptdate_val)), "%Y%m%d")
    elif isinstance(reptdate_val, datetime):
        reptdate = reptdate_val
    else:
        reptdate = datetime.strptime(str(reptdate_val)[:10], "%Y-%m-%d")

    return {
        "reptdate" : reptdate,
        "reptmon"  : reptdate.strftime("%m"),
        "reptyear" : reptdate.strftime("%Y"),
        "rdate"    : reptdate.strftime("%d/%m/%Y"),
        "zdate"    : reptdate.strftime("%j").zfill(5),
        "reptdt6"  : reptdate.strftime("%y%m%d"),
    }


# ============================================================================
# STEP 2 – READ BRANCH FILE
# ============================================================================

def read_branch_file() -> pl.DataFrame:
    """
    Fixed-width branch reference file.
      col 2-4 : BRANCH (numeric 3)
      col 6-8 : BRCH ($3)
    """
    rows = []
    with open(BRHFILE, "r") as fh:
        for line in fh:
            if len(line) < 8:
                continue
            try:
                branch = int(line[1:4].strip())
            except ValueError:
                continue
            brch = line[5:8]
            rows.append({"BRANCH": branch, "BRCH": brch})
    df = pl.DataFrame(rows)
    return df.sort("BRANCH")


# ============================================================================
# STEP 3 – LOAD PREV (previous day balances from DEP0.CURRENT)
# ============================================================================

def load_prev() -> pl.DataFrame:
    """
    Read DEP0.CURRENT, keep ACCTNO and PREBAL (=CURBAL, floored at 0).
    """
    con = duckdb.connect()
    prev = con.execute(
        f"SELECT ACCTNO, CURBAL AS PREBAL FROM read_parquet('{DEP0_CURRENT}')"
    ).pl()
    con.close()

    prev = prev.with_columns(
        pl.when(pl.col("PREBAL") < 0)
          .then(0.0)
          .otherwise(pl.col("PREBAL"))
          .alias("PREBAL")
    )
    return prev.sort("ACCTNO")


# ============================================================================
# STEP 4 – LOAD & FILTER DEPOSIT.CURRENT → CRMOVE
# ============================================================================

def build_crmove(brhdata: pl.DataFrame, prev: pl.DataFrame) -> pl.DataFrame:
    """
    Load DEPOSIT.CURRENT, apply CAPROD format, apply all exclusion filters,
    merge branch codes and previous balances, compute MOVEMENT.
    Keep only records where ABS(MOVEMENT) >= 999,999.99.
    """
    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{CURRENT_FILE}')").pl()
    con.close()

    # Apply CAPROD format
    def caprod(product) -> str:
        return CAProductFormat.format(int(product) if product is not None else None)

    df = df.with_columns(
        pl.col("PRODUCT").map_elements(caprod, return_dtype=pl.Utf8).alias("PRODCD")
    )

    # IF OPENIND NOT IN ('B','C','P') – keep those NOT in ('B','C','P')
    df = df.filter(~pl.col("OPENIND").is_in(["B", "C", "P"]))

    # Delete rows where PRODCD IN ('N','42610') OR PRODUCT IN (excl set)
    df = df.filter(
        ~(
            pl.col("PRODCD").is_in(list(PRODCD_EXCL)) |
            pl.col("PRODUCT").cast(pl.Int64).is_in(list(PRODUCT_EXCL))
        )
    )

    # Floor negative CURBAL to 0
    df = df.with_columns(
        pl.when(pl.col("CURBAL") < 0)
          .then(0.0)
          .otherwise(pl.col("CURBAL"))
          .alias("CURBAL")
    )

    # Exclude account number range 3590000000–3599999999
    df = df.filter(
        ~((pl.col("ACCTNO") >= ACCTNO_EXCL_LO) & (pl.col("ACCTNO") <= ACCTNO_EXCL_HI))
    )

    # Merge branch codes (BY BRANCH, keep A)
    df = df.sort("BRANCH")
    df = df.join(brhdata, on="BRANCH", how="left")

    # Merge previous balances (BY ACCTNO, keep A)
    df = df.sort("ACCTNO")
    df = df.join(prev, on="ACCTNO", how="left")
    df = df.with_columns(
        pl.col("PREBAL").fill_null(0.0)
    )

    # Compute MOVEMENT
    df = df.with_columns(
        (pl.col("CURBAL") - pl.col("PREBAL")).alias("MOVEMENT")
    )

    # Keep only ABS(MOVEMENT) >= 999,999.99
    df = df.filter(pl.col("MOVEMENT").abs() >= 999_999.99)

    return df.sort("ACCTNO")


# ============================================================================
# STEP 5 – LOAD CRM DATA (CIS.CISR1CA<REPTDT6>)
# ============================================================================

def load_cname(reptdt6: str) -> pl.DataFrame:
    cis_file = os.path.join(CIS_DIR, f"CISR1CA{reptdt6}.parquet")
    if not os.path.exists(cis_file):
        return pl.DataFrame({
            "ACCTNO":   pl.Series([], dtype=pl.Int64),
            "CUSTNAME": pl.Series([], dtype=pl.Utf8),
            "CUSTNO":   pl.Series([], dtype=pl.Utf8),
            "MNIADDL1": pl.Series([], dtype=pl.Utf8),
            "MNIADDL2": pl.Series([], dtype=pl.Utf8),
        })

    con = duckdb.connect()
    cname = con.execute(f"""
        SELECT ACCTNO, CUSTNAME, CUSTNO, MNIADDL1, MNIADDL2
        FROM   read_parquet('{cis_file}')
        WHERE  SECCUST = '901'
    """).pl()
    con.close()

    return cname.unique(subset=["ACCTNO"], keep="first").sort("ACCTNO")


# ============================================================================
# STEP 6 – MERGE CRM INTO CRMOVE
# ============================================================================

def enrich_crmove(crmove: pl.DataFrame, cname: pl.DataFrame) -> pl.DataFrame:
    """
    Merge CRM onto CRMOVE by ACCTNO (keep A = CRMOVE rows).
    Sort by DESCENDING MOVEMENT, then ACCTNO.
    Add COUNT and MOVEMT (RM millions, rounded to 2dp).
    """
    crmove = crmove.join(cname, on="ACCTNO", how="left")

    # Sort: DESCENDING MOVEMENT, then ACCTNO ascending
    crmove = crmove.sort(["MOVEMENT", "ACCTNO"], descending=[True, False])

    # COUNT (row number) and MOVEMT in RM millions
    crmove = crmove.with_row_index(name="COUNT", offset=1)
    crmove = crmove.with_columns(
        ((pl.col("MOVEMENT") * 0.000001).round(2)).alias("MOVEMT")
    )

    return crmove


# ============================================================================
# STEP 7 – WRITE REPORT (PROC REPORT equivalent)
# ============================================================================

# Column widths matching PROC REPORT DEFINE statements
COL_W = {
    "COUNT"    :  5,
    "BRANCH"   :  6,
    "ACCTNO"   : 10,
    "BRCH"     :  6,
    "MNIADDL1" : 40,
    "MOVEMT"   : 11,   # COMMA11.2
    "CUSTNAME" : 50,
    "MNIADDL2" : 40,
    "CUSTNO"   :  8,
    "CURBAL"   : 14,   # COMMA14.2
    "PREBAL"   : 14,   # COMMA14.2
    "PRODUCT"  :  7,
    "CUSTCODE" :  8,
    "SECOND"   :  9,
}

# Two-line column headers (split on '*')
HEADERS = [
    ("NO.",           ""),
    ("BRANCH CODE",   ""),
    ("ACCOUNT",       "NO."),
    ("BRANCH ABBR",   ""),
    ("NAME1 OF CUSTOMER (FR. A/C LVL)", ""),
    ("DAILY NET",     "INC./(DEC.) (RM'MIL)"),
    ("NAME OF CUSTOMER (FR. CIS LVL)", ""),
    ("NAME2 OF CUSTOMER (FR. A/C LVL)", ""),
    ("CUSTOMER CIS NO.", ""),
    ("CURRENT DAY",   "BALANCE (RM)"),
    ("PREVIOUS DAY",  "BALANCE (RM)"),
    ("PRODUCT CODE",  ""),
    ("CUSTOMER CODE", ""),
    ("SECONDARY OFFICER NO.", ""),
]

COLUMNS = [
    "COUNT", "BRANCH", "ACCTNO", "BRCH", "MNIADDL1",
    "MOVEMT", "CUSTNAME", "MNIADDL2", "CUSTNO",
    "CURBAL", "PREBAL", "PRODUCT", "CUSTCODE", "SECOND",
]


def _cell(col: str, row: dict) -> str:
    """Format a single cell value."""
    val = row.get(col)
    w   = COL_W[col]

    if col == "COUNT":
        return str(int(val) if val is not None else "").rjust(w)
    if col == "BRANCH":
        return str(int(val) if val is not None else "").rjust(w)
    if col == "ACCTNO":
        return str(int(val) if val is not None else "").rjust(w)
    if col == "BRCH":
        return str(val or "").ljust(w)[:w]
    if col == "MNIADDL1":
        return str(val or "").ljust(w)[:w]
    if col == "MOVEMT":
        return fmt_comma(float(val) if val is not None else None, w, 2)
    if col == "CUSTNAME":
        return str(val or "").ljust(w)[:w]
    if col == "MNIADDL2":
        return str(val or "").ljust(w)[:w]
    if col == "CUSTNO":
        return str(val or "").rjust(w)
    if col == "CURBAL":
        return fmt_comma(float(val) if val is not None else None, w, 2)
    if col == "PREBAL":
        return fmt_comma(float(val) if val is not None else None, w, 2)
    if col == "PRODUCT":
        return str(int(val) if val is not None else "").rjust(w)
    if col == "CUSTCODE":
        return str(val or "").rjust(w)
    if col == "SECOND":
        return str(val or "").rjust(w)
    return str(val or "").ljust(w)[:w]


def _separator(char: str = "-") -> str:
    return " ".join(char * COL_W[c] for c in COLUMNS)


def write_report(crmove: pl.DataFrame, rdate: str) -> None:
    """
    Write the PROC REPORT-equivalent fixed-width text report with ASA
    carriage-control characters.

    When CRMOVE is empty, writes the 'no customer' message block.
    """
    lines: list[str] = []

    # ── Title block ───────────────────────────────────────────────────────
    def title_block(asa: str = ASA_NEWPAGE) -> list[str]:
        return [
            f"{asa}REPORT ID : DMMISR42",
            f"{ASA_NEWLINE}CREDIT MOVEMENT IN BANK'S DEMAND DEPOSITS AS AT {rdate}",
            f"{ASA_NEWLINE}DAILY NET INCREASE/DECREASE OF RM 1 MILLION & ABOVE "
            "PER ACCOUNT (CONVENTIONAL)",
            f"{ASA_NEWLINE}",
            f"{ASA_NEWLINE}",
            f"{ASA_NEWLINE}",
        ]

    lines.extend(title_block())

    # ── Empty-data message ────────────────────────────────────────────────
    if crmove.height == 0:
        lines.append(f"{ASA_NEWLINE}")
        lines.append(f"{ASA_NEWLINE}")
        lines.append(f"{ASA_NEWLINE}     *******************************************************")
        lines.append(f"{ASA_NEWLINE}     NO CUSTOMER WITH CREDIT MOVEMENT OF 1 MILLION AND ABOVE")
        lines.append(f"{ASA_NEWLINE}     AT {rdate}")
        lines.append(f"{ASA_NEWLINE}     *******************************************************")
        with open(REPORT_FILE, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
        print(f"Report written (empty): {REPORT_FILE}")
        return

    # ── Column headers (HEADLINE) ─────────────────────────────────────────
    def header_row() -> list[str]:
        h1 = " ".join(h[0].ljust(COL_W[COLUMNS[i]])[:COL_W[COLUMNS[i]]]
                      for i, h in enumerate(HEADERS))
        h2 = " ".join(h[1].ljust(COL_W[COLUMNS[i]])[:COL_W[COLUMNS[i]]]
                      for i, h in enumerate(HEADERS))
        sep = _separator("-")
        return [
            f"{ASA_NEWLINE}{h1}",
            f"{ASA_NEWLINE}{h2}",
            f"{ASA_NEWLINE}{sep}",
        ]

    lines.extend(header_row())
    line_count = len(lines)

    # Running totals for RBREAK AFTER / SUMMARIZE
    tot_movemt  = 0.0
    tot_curbal  = 0.0
    tot_prebal  = 0.0

    records = crmove.to_dicts()
    for row in records:
        # Page break
        if line_count >= PAGE_LINES:
            lines.extend(title_block(ASA_NEWPAGE))
            lines.extend(header_row())
            line_count = len(lines)

        # Accumulate summary totals
        tot_movemt += float(row.get("MOVEMT") or 0)
        tot_curbal += float(row.get("CURBAL") or 0)
        tot_prebal += float(row.get("PREBAL") or 0)

        detail = " ".join(_cell(c, row) for c in COLUMNS)
        lines.append(f"{ASA_NEWLINE}{detail}")
        line_count += 1

    # ── RBREAK AFTER / PAGE DUL OL SUMMARIZE ─────────────────────────────
    # DUL = double underline, OL = overline on summary row
    sep_double = _separator("=")
    sep_single = _separator("-")
    lines.append(f"{ASA_NEWLINE}{sep_single}")
    lines.append(f"{ASA_NEWLINE}{sep_double}")

    # Summary row: sum MOVEMT, CURBAL, PREBAL; other columns blank
    sum_row_vals: dict = {c: None for c in COLUMNS}
    sum_row_vals["MOVEMT"] = tot_movemt
    sum_row_vals["CURBAL"] = tot_curbal
    sum_row_vals["PREBAL"] = tot_prebal

    summary = " ".join(_cell(c, sum_row_vals) for c in COLUMNS)
    lines.append(f"{ASA_NEWLINE}{summary}")
    lines.append(f"{ASA_NEWLINE}{sep_double}")

    with open(REPORT_FILE, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"Report written: {REPORT_FILE}")


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    print("DMMISR42 – Daily CA Credit Movement (RM1M+) starting...")

    # Step 1: derive report date
    ctx = derive_report_date()
    print(f"  Report date : {ctx['rdate']}")

    # Step 2: branch reference
    brhdata = read_branch_file()

    # Step 3: previous-day balances
    prev = load_prev()

    # Step 4: build filtered current-day dataset
    crmove = build_crmove(brhdata, prev)

    # Step 5: CRM enrichment
    cname  = load_cname(ctx["reptdt6"])

    # Step 6: merge CRM + sort + add COUNT / MOVEMT
    crmove = enrich_crmove(crmove, cname)

    # Step 7: write report
    write_report(crmove, ctx["rdate"])

    print("DMMISR42 – Done.")


if __name__ == "__main__":
    main()
