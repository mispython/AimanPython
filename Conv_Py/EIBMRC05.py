#!/usr/bin/env python3
"""
Program  : EIBMRC05.py
Purpose  : Loans & Advances by Type of Security.
           USER      : PUBLIC BANK BERHAD, RETAIL CREDIT DIVISION
           OBJECTIVE : LOANS & ADVANCES BY TYPE OF SECURITY
           FREQUENCY : MONTHLY
           SELECTION CRITERIA : LIABCODE, ALL LOANS & OD
"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
from EIBMMISF import (
    fmt_lnfacil,
    fmt_colldes,
)

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR       = Path("/data")
BNM_DIR        = BASE_DIR / "bnm"
LOAN_DIR       = BASE_DIR / "loan"

REPTDATE_FILE  = BNM_DIR  / "reptdate.parquet"
LNNOTE_FILE    = BNM_DIR  / "lnnote.parquet"
LNCOMM_FILE    = BNM_DIR  / "lncomm.parquet"

OUTPUT_DIR     = BASE_DIR / "output"
OUTPUT_REPORT  = OUTPUT_DIR / "eibmrc05_report.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# REPORT CONSTANTS
# ============================================================================

PAGE_LENGTH = 60
LINE_WIDTH  = 132

# Columns kept from loan/uloan per %LET LOAN macro
LOAN_KEEP = [
    "BRANCH", "ACCTNO", "STATECD", "PRODUCT", "SECTORCD", "COL1",
    "ACCTYPE", "LIABCODE", "APPRLIMT", "FACTYPE",
    "BALANCE", "RLEASAMT", "UNDRAWN", "PRODCD",
]

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def fmt_ddmmyy10(d: date) -> str:
    """Format date as DD/MM/YYYY (SAS DDMMYY10.)"""
    return d.strftime("%d/%m/%Y")


def fmt_z2(n: int) -> str:
    return f"{n:02d}"


def fmt_z4(n: int) -> str:
    return f"{n:04d}"


def fmt_comma8(val) -> str:
    if val is None:
        return " " * 8
    try:
        return f"{int(val):,}".rjust(8)
    except (TypeError, ValueError):
        return " " * 8


def fmt_comma17_2(val) -> str:
    if val is None:
        return " " * 17
    try:
        return f"{float(val):,.2f}".rjust(17)
    except (TypeError, ValueError):
        return " " * 17


# ============================================================================
# STEP 1: READ REPTDATE & DERIVE MACRO VARIABLES
# ============================================================================

con = duckdb.connect()

reptdate_df = con.execute(f"SELECT * FROM read_parquet('{REPTDATE_FILE}')").pl()
row = reptdate_df.row(0, named=True)

REPTDATE: date = row["REPTDATE"]
if isinstance(REPTDATE, datetime):
    REPTDATE = REPTDATE.date()

NOWK    = "4"
RDATE   = fmt_ddmmyy10(REPTDATE)
REPTMON = fmt_z2(REPTDATE.month)
RYEAR   = fmt_z4(REPTDATE.year)

# ============================================================================
# STEP 2: LOAD LNNOTE (KEEP ACCTNO, NOTENO, LIABCODE)
# ============================================================================

lnnote_query = f"""
SELECT ACCTNO, NOTENO, LIABCODE
FROM read_parquet('{LNNOTE_FILE}')
"""
lnnote = con.execute(lnnote_query).pl()

# ============================================================================
# STEP 3: LOAD & SORT LOAN DATA, MERGE WITH LNNOTE
# ============================================================================

loan_file = LOAN_DIR / f"loan{REPTMON}{NOWK}.parquet"

sasloan_query = f"""
SELECT *
FROM read_parquet('{loan_file}')
ORDER BY ACCTNO, NOTENO
"""
sasloan = con.execute(sasloan_query).pl()

# Merge SASLOAN (IN=A) with LNNOTE; keep only A records
loan = sasloan.join(lnnote, on=["ACCTNO", "NOTENO"], how="left", suffix="_NOTE")

# Resolve LIABCODE: prefer base table, fallback to NOTE
if "LIABCODE_NOTE" in loan.columns:
    loan = loan.with_columns(
        pl.when(pl.col("LIABCODE").is_null())
          .then(pl.col("LIABCODE_NOTE"))
          .otherwise(pl.col("LIABCODE"))
          .alias("LIABCODE")
    ).drop("LIABCODE_NOTE")

# ============================================================================
# STEP 4: LOAD ULOAN DATA, MERGE WITH LNCOMM (KEEP CCOLLTRL)
# ============================================================================

uloan_file = LOAN_DIR / f"uloan{REPTMON}{NOWK}.parquet"

sasuloan_query = f"""
SELECT *
FROM read_parquet('{uloan_file}')
ORDER BY ACCTNO, COMMNO
"""
sasuloan = con.execute(sasuloan_query).pl()

lncomm_query = f"""
SELECT ACCTNO, COMMNO, CCOLLTRL
FROM read_parquet('{LNCOMM_FILE}')
"""
lncomm = con.execute(lncomm_query).pl()

# Merge SASULOAN (IN=A) with LNCOMM; keep only A records
uloan = sasuloan.join(lncomm, on=["ACCTNO", "COMMNO"], how="left")

# ============================================================================
# STEP 5: BUILD FACIL DATASET (LOAN + ULOAN), ASSIGN FACTYPE & LIABCODE
# ============================================================================

# Align schemas: ensure both have CCOLLTRL and COL1 columns
for col in ["CCOLLTRL", "COL1", "FACTYPE"]:
    if col not in loan.columns:
        loan = loan.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
    if col not in uloan.columns:
        uloan = uloan.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))

# Combine LOAN and ULOAN
combined = pl.concat([loan, uloan], how="diagonal")

# Filter: PRODCD prefix '34' OR PRODCD = '54120'
combined = combined.filter(
    (pl.col("PRODCD").cast(pl.Utf8).str.slice(0, 2) == "34") |
    (pl.col("PRODCD").cast(pl.Utf8) == "54120")
)

# Apply CCOLLTRL override, FACTYPE and LIABCODE logic row by row
def apply_factype_liabcode(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        ccolltrl = r.get("CCOLLTRL")
        acctype  = str(r.get("ACCTYPE") or "")
        prodcd   = str(r.get("PRODCD")  or "").strip()
        col1     = str(r.get("COL1")    or "")

        # IF CCOLLTRL NE . THEN LIABCODE = CCOLLTRL
        if ccolltrl is not None and ccolltrl != "":
            r["LIABCODE"] = str(ccolltrl)

        factype = r.get("FACTYPE") or ""

        if acctype == "OD" and prodcd == "34180":
            r["LIABCODE"] = col1[:2] if len(col1) >= 2 else col1
            r["FACTYPE"]  = "OD"
        elif acctype == "LN":
            r["FACTYPE"]  = fmt_lnfacil(prodcd)

        out.append(r)
    return out


facil_rows = apply_factype_liabcode(combined.to_dicts())
facil_df   = pl.DataFrame(facil_rows)

# ============================================================================
# STEP 6: SORT BY LIABCODE, FACTYPE, BRANCH
# ============================================================================

facil_df = facil_df.sort(["LIABCODE", "FACTYPE", "BRANCH"])

# ============================================================================
# STEP 7: PRODUCE TABULATE REPORT
# ============================================================================

def build_col_header() -> tuple[str, str]:
    col = (
        f"{'SECURITY / FACILITY TYPE / BRANCH':<40}  "
        f"{'NO OF ACCOUNTS':>8}  "
        f"{'APPROVED LIMIT':>17}  "
        f"{'OUTSTANDING BALANCE':>17}  "
        f"{'UNDRAWN':>17}"
    )
    sep = "-" * LINE_WIDTH
    return col, sep


def write_report(df: pl.DataFrame, output_path: Path) -> None:
    lines      = []
    line_count = PAGE_LENGTH  # trigger new page immediately
    col_header, separator = build_col_header()

    title1 = "PBB - RETAIL CREDIT DIVISION   ATTN: MS KHOO"
    title2 = f"005 LOANS & ADVANCES BY TYPE OF SECURITY AS AT {RDATE}"

    def new_page() -> None:
        nonlocal line_count
        lines.append(f"1{title1.center(LINE_WIDTH)}")
        lines.append(f" {title2.center(LINE_WIDTH)}")
        lines.append(f" ")
        lines.append(f" {col_header}")
        lines.append(f" {separator}")
        line_count = 5

    # Title section: COLLATERAL CODES UNDER OTHERS CATEGORY
    lines.append(f"1{'COLLATERAL CODES UNDER OTHERS CATEGORY'.center(LINE_WIDTH)}")
    lines.append(f" {'FOR VERIFICATION'.center(LINE_WIDTH)}")
    lines.append(f" ")
    line_count = 3

    # Grand totals accumulator
    grand_n   = 0
    grand_lmt = 0.0
    grand_bal = 0.0
    grand_und = 0.0

    for liab_val, liab_grp in df.group_by("LIABCODE", maintain_order=True):
        liab_label = fmt_colldes(str(liab_val or ""))

        liab_n   = 0
        liab_lmt = 0.0
        liab_bal = 0.0
        liab_und = 0.0

        for factype_val, fac_grp in liab_grp.group_by("FACTYPE", maintain_order=True):
            fac_n   = 0
            fac_lmt = 0.0
            fac_bal = 0.0
            fac_und = 0.0

            for branch_val, br_grp in fac_grp.group_by("BRANCH", maintain_order=True):
                n   = len(br_grp)
                lmt = float(br_grp["APPRLIMT"].sum() or 0)
                bal = float(br_grp["BALANCE"].sum()   or 0)
                und = float(br_grp["UNDRAWN"].sum()   or 0)

                if line_count >= PAGE_LENGTH - 4:
                    new_page()

                label = f"  {liab_label} : {factype_val} / {branch_val}"
                lines.append(
                    f" {label:<40}  "
                    f"{fmt_comma8(n)}  "
                    f"{fmt_comma17_2(lmt)}  "
                    f"{fmt_comma17_2(bal)}  "
                    f"{fmt_comma17_2(und)}"
                )
                line_count += 1
                fac_n   += n
                fac_lmt += lmt
                fac_bal += bal
                fac_und += und

            # SUB-TOTAL per facility type
            if line_count >= PAGE_LENGTH - 4:
                new_page()
            lines.append(
                f" {'    SUB-TOTAL':<40}  "
                f"{fmt_comma8(fac_n)}  "
                f"{fmt_comma17_2(fac_lmt)}  "
                f"{fmt_comma17_2(fac_bal)}  "
                f"{fmt_comma17_2(fac_und)}"
            )
            line_count += 1
            liab_n   += fac_n
            liab_lmt += fac_lmt
            liab_bal += fac_bal
            liab_und += fac_und

        # TOTAL per LIABCODE
        if line_count >= PAGE_LENGTH - 4:
            new_page()
        lines.append(
            f" {'  TOTAL':<40}  "
            f"{fmt_comma8(liab_n)}  "
            f"{fmt_comma17_2(liab_lmt)}  "
            f"{fmt_comma17_2(liab_bal)}  "
            f"{fmt_comma17_2(liab_und)}"
        )
        line_count += 1
        grand_n   += liab_n
        grand_lmt += liab_lmt
        grand_bal += liab_bal
        grand_und += liab_und

    # GRAND TOTAL
    if line_count >= PAGE_LENGTH - 4:
        new_page()
    lines.append(f" {separator}")
    lines.append(
        f" {'GRAND TOTAL':<40}  "
        f"{fmt_comma8(grand_n)}  "
        f"{fmt_comma17_2(grand_lmt)}  "
        f"{fmt_comma17_2(grand_bal)}  "
        f"{fmt_comma17_2(grand_und)}"
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


write_report(facil_df, OUTPUT_REPORT)

print(f"Report written to: {OUTPUT_REPORT}")
