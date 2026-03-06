#!/usr/bin/env python3
"""
Program  : EIBMRC04.py
Purpose  : Loans & Advances by Interest Rate as at reporting date.
           Produces tabular reports segmented by interest type:
           BLR, COF, FIX1, FIX2, BNM - for both LN and OD account types.
"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
from EIBMMISF import (
    fmt_blr,    fmt_blr_label,
    fmt_cof,    fmt_cof_label,
    fmt_bnm,    fmt_bnm_label,
    fmt_fixedf, fmt_fixedf_label,
    fmt_lndesc,
    fmt_oddesc,
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
ODLIMT_DIR     = BASE_DIR / "odlimt"

REPTDATE_FILE  = BNM_DIR  / "reptdate.parquet"
SDESC_FILE     = LOAN_DIR / "sdesc.parquet"
LNNOTE_FILE    = BNM_DIR  / "lnnote.parquet"

OUTPUT_DIR     = BASE_DIR / "output"
OUTPUT_REPORT  = OUTPUT_DIR / "eibmrc04_report.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# REPORT CONSTANTS
# ============================================================================

PAGE_LENGTH = 60
LINE_WIDTH  = 132

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


def fmt_comma18_2(val) -> str:
    if val is None:
        return " " * 18
    try:
        return f"{float(val):,.2f}".rjust(18)
    except (TypeError, ValueError):
        return " " * 18


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
# STEP 2: READ SDESC
# ============================================================================

sdesc_df = con.execute(f"SELECT * FROM read_parquet('{SDESC_FILE}')").pl()
SDESC = str(sdesc_df["SDESC"][0]).ljust(26)[:26]

# ============================================================================
# STEP 3: LOAD LNNOTE (KEEP ACCTNO, NOTENO, NTAPR)
# ============================================================================

lnnote_query = f"""
SELECT ACCTNO, NOTENO, NTAPR
FROM read_parquet('{LNNOTE_FILE}')
ORDER BY ACCTNO, NOTENO
"""
lnnote = con.execute(lnnote_query).pl()

# ============================================================================
# STEP 4: LOAD LOAN DATA FOR CURRENT MONTH
# ============================================================================

loan_file = LOAN_DIR / f"loan{REPTMON}{NOWK}.parquet"

loan_query = f"""
SELECT *
FROM read_parquet('{loan_file}')
ORDER BY ACCTNO, NOTENO
"""
loan = con.execute(loan_query).pl()

# ============================================================================
# STEP 5: MERGE LOAN + LNNOTE, FILTER PRODCD PREFIX '34' OR '54120'
# ============================================================================

merged = loan.join(lnnote, on=["ACCTNO", "NOTENO"], how="left")
merged = merged.filter(
    (pl.col("PRODCD").cast(pl.Utf8).str.slice(0, 2) == "34") |
    (pl.col("PRODCD").cast(pl.Utf8) == "54120")
)

# ============================================================================
# STEP 6: BUILD RATE TABLE - SEGREGATION OF BLR, SPREAD & MANIPULATION
# ============================================================================

def classify_rate(rows: list[dict]) -> list[dict]:
    """Apply SAS interest-type classification logic row by row."""
    out = []
    for r in rows:
        if r.get("ACCTYPE") != "LN":
            out.append(r)
            continue

        product  = int(r.get("PRODUCT")  or 0)
        intrate  = float(r.get("INTRATE") or 0.0)
        spread   = float(r.get("SPREAD")  or 0.0)
        ntapr    = float(r.get("NTAPR")   or 0.0)
        ntindex  = int(r.get("NTINDEX")   or 0)

        # IF PRODUCT < 100 THEN NTINDEX = 10
        if product < 100:
            ntindex = 10

        r["PRODUCT1"] = product
        inttype = ""
        spread1 = ""
        bnm1    = ""
        rates   = ""

        if product in (350, 910, 925):
            inttype = "COF"
            spread1 = fmt_cof(intrate)
        elif product in (170, 171, 172, 524, 525, 526, 527) or (555 <= product <= 599):
            inttype = "BNM"
            bnm1    = fmt_bnm(intrate)
        elif (100 <= product <= 199) and spread <= 0:
            inttype = "FIX1"
            rates   = fmt_fixedf(ntapr)
        else:
            if ntindex == 1:
                inttype = "BLR"
                spread1 = fmt_blr(spread)
            elif ntindex in (10, 11):
                if spread <= 0:
                    inttype = "FIX2"
                rates = fmt_fixedf(intrate)
            else:
                inttype = "FIX2"
                rates   = fmt_fixedf(intrate)

        # IF INTTYPE = ' ' fallback
        if not inttype.strip():
            inttype = "FIX2"
            rates   = fmt_fixedf(intrate)

        r["INTTYPE"] = inttype
        r["SPREAD1"] = spread1
        r["BNM1"]    = bnm1
        r["RATES"]   = rates
        out.append(r)
    return out


rate_rows   = classify_rate(merged.to_dicts())
rate_df     = pl.DataFrame(rate_rows)

# ============================================================================
# STEP 7: BUILD OD DATASET (ACCTYPE='OD', PRODCD IN ('34180','34240'))
# ============================================================================

od_file = LOAN_DIR / f"loan{REPTMON}{NOWK}.parquet"

od_query = f"""
SELECT ACCTNO, PRODUCT, APPRLIMT, BALANCE, ACCTYPE, UNDRAWN
FROM read_parquet('{od_file}')
WHERE ACCTYPE = 'OD'
  AND PRODCD IN ('34180','34240')
  AND (SUBSTR(CAST(PRODCD AS VARCHAR), 1, 2) = '34' OR PRODCD = '54120')
"""
od = con.execute(od_query).pl()

# ============================================================================
# STEP 8: MERGE OD WITH OVERDFT (ODLIMT), CLASSIFY OD RATES
# ============================================================================

overdft_file = ODLIMT_DIR / "overdft.parquet"

overdft_query = f"""
SELECT * EXCLUDE(APPRLIMT)
FROM read_parquet('{overdft_file}')
"""
overdft = con.execute(overdft_query).pl()

od_merged = od.join(overdft, on="ACCTNO", how="left")

def classify_od_rate(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        lmtrate  = float(r.get("LMTRATE")  or 0.0)
        lmtadjf  = float(r.get("LMTADJF")  or 0.0)
        product  = int(r.get("PRODUCT")     or 0)

        rate     = lmtadjf
        product1 = product
        inttype  = ""
        spread1  = ""
        bnm1     = ""

        if product == 114:
            rate    = lmtrate
            inttype = "BNM"
            bnm1    = fmt_bnm(rate)
        else:
            inttype = "BLR"
            spread1 = fmt_blr(rate)

        r["RATE"]     = rate
        r["PRODUCT1"] = product1
        r["INTTYPE"]  = inttype
        r["SPREAD1"]  = spread1
        r["BNM1"]     = bnm1
        out.append(r)
    return out


od_rate_rows = classify_od_rate(od_merged.to_dicts())
od_rate_df   = pl.DataFrame(od_rate_rows)

# NODUPKEY on ACCTNO
od_rate_df = (
    od_rate_df.sort("ACCTNO")
              .unique(subset=["ACCTNO"], keep="first")
)

# ============================================================================
# STEP 9: REPORT RENDERING HELPERS
# ============================================================================

def render_table_header(lines: list, title_lines: list[str], col_header: str,
                        separator: str) -> int:
    """Append page header (ASA '1' + titles + col header). Returns line count used."""
    lines.append(f"1{title_lines[0].center(LINE_WIDTH)}")
    for t in title_lines[1:]:
        lines.append(f" {t.center(LINE_WIDTH)}")
    lines.append(f" ")
    lines.append(f" {col_header}")
    lines.append(f" {separator}")
    return 4 + len(title_lines)


def build_col_header() -> tuple[str, str]:
    col = (
        f"{'LOAN TYPE':<60}  "
        f"{'NO OF ACCOUNTS':>12}  "
        f"{'APPROVED LIMIT':>18}  "
        f"{'TOTAL OUTSTANDING':>18}  "
        f"{'AMOUNT UNDRAWN':>18}"
    )
    sep = "-" * LINE_WIDTH
    return col, sep


def render_tabulate_blr_ln(df: pl.DataFrame, titles: list[str],
                            lines: list, pricing_label: str,
                            spread_col: str, spread_fmt_fn,
                            product_fmt_fn) -> None:
    """
    Replicate PROC TABULATE nested structure:
    ACCTYPE * (SPREAD1 * (PRODUCT * PRODUCT1  ALL='SUBTOTAL') ALL='TOTAL')
    """
    col_header, separator = build_col_header()
    line_count = PAGE_LENGTH  # force new page at start

    subset = df.clone()
    if subset.is_empty():
        return

    for acctype, acc_grp in subset.group_by("ACCTYPE", maintain_order=True):
        for spread_val in sorted(acc_grp[spread_col].unique().to_list()):
            spread_label = spread_fmt_fn(spread_val) if spread_val else ""
            spread_grp   = acc_grp.filter(pl.col(spread_col) == spread_val)

            for product_val in sorted(spread_grp["PRODUCT"].unique().to_list()):
                prod_grp = spread_grp.filter(pl.col("PRODUCT") == product_val)
                prod_desc = product_fmt_fn(product_val)

                n      = len(prod_grp)
                lmt    = prod_grp["APPRLIMT"].sum()
                bal    = prod_grp["BALANCE"].sum()
                undrwn = prod_grp["UNDRAWN"].sum()

                if line_count >= PAGE_LENGTH - 4:
                    lines.extend([""])
                    render_table_header(lines, titles, col_header, separator)
                    line_count = 4 + len(titles)

                row_label = f"  {acctype}  {pricing_label}={spread_label:<20}  {prod_desc}"
                lines.append(
                    f" {row_label:<60}  "
                    f"{fmt_comma8(n)}  "
                    f"{fmt_comma18_2(lmt)}  "
                    f"{fmt_comma18_2(bal)}  "
                    f"{fmt_comma18_2(undrwn)}"
                )
                line_count += 1

            # SUBTOTAL per spread
            sub_n   = len(spread_grp)
            sub_lmt = spread_grp["APPRLIMT"].sum()
            sub_bal = spread_grp["BALANCE"].sum()
            sub_und = spread_grp["UNDRAWN"].sum()
            lines.append(
                f" {'    SUBTOTAL':<60}  "
                f"{fmt_comma8(sub_n)}  "
                f"{fmt_comma18_2(sub_lmt)}  "
                f"{fmt_comma18_2(sub_bal)}  "
                f"{fmt_comma18_2(sub_und)}"
            )
            line_count += 1

        # TOTAL per acctype
        tot_n   = len(acc_grp)
        tot_lmt = acc_grp["APPRLIMT"].sum()
        tot_bal = acc_grp["BALANCE"].sum()
        tot_und = acc_grp["UNDRAWN"].sum()
        lines.append(
            f" {'  TOTAL':<60}  "
            f"{fmt_comma8(tot_n)}  "
            f"{fmt_comma18_2(tot_lmt)}  "
            f"{fmt_comma18_2(tot_bal)}  "
            f"{fmt_comma18_2(tot_und)}"
        )
        line_count += 1


# ============================================================================
# STEP 10: WRITE ALL REPORT SECTIONS
# ============================================================================

report_lines: list[str] = []

# --- Section 1: LN BLR ---
blr_ln = rate_df.filter(
    (pl.col("ACCTYPE") == "LN") & (pl.col("INTTYPE") == "BLR")
)
render_tabulate_blr_ln(
    blr_ln,
    titles=[
        f"{SDESC.strip()}  - RETAIL CREDIT DIVISION",
        f"CDR004 LOANS AND ADVANCES BY INTEREST RATE AS AT {RDATE}",
        "ATTENTION : MS KHOO TAI FUNG",
        "LOANS AND ADVANCES BY INTEREST RATE - (%) + BLR",
        "-----------------------------------------------",
    ],
    lines=report_lines,
    pricing_label="PRICING (%)+BLR",
    spread_col="SPREAD1",
    spread_fmt_fn=fmt_blr_label,
    product_fmt_fn=fmt_lndesc,
)

# --- Section 2: OD BLR ---
blr_od = od_rate_df.filter(
    (pl.col("ACCTYPE") == "OD") & (pl.col("INTTYPE") == "BLR")
)
render_tabulate_blr_ln(
    blr_od,
    titles=[
        "LOANS AND ADVANCES BY INTEREST RATE - (%) + BLR",
        "-----------------------------------------------",
    ],
    lines=report_lines,
    pricing_label="PRICING (%)+BLR",
    spread_col="SPREAD1",
    spread_fmt_fn=fmt_blr_label,
    product_fmt_fn=fmt_oddesc,
)

# --- Section 3: LN COF ---
cof_ln = rate_df.filter(
    (pl.col("ACCTYPE") == "LN") & (pl.col("INTTYPE") == "COF")
)
render_tabulate_blr_ln(
    cof_ln,
    titles=[
        "LOANS AND ADVANCES BY INTEREST RATE - (%) + COF",
        "-----------------------------------------------",
    ],
    lines=report_lines,
    pricing_label="PRICING (%)+COF",
    spread_col="SPREAD1",
    spread_fmt_fn=fmt_cof_label,
    product_fmt_fn=fmt_lndesc,
)

# --- Section 4: OD COF ---
cof_od = od_rate_df.filter(
    (pl.col("ACCTYPE") == "OD") & (pl.col("INTTYPE") == "COF")
)
render_tabulate_blr_ln(
    cof_od,
    titles=[
        "LOANS AND ADVANCES BY INTEREST RATE - (%) + COF",
        "-----------------------------------------------",
    ],
    lines=report_lines,
    pricing_label="PRICING (%)+COF",
    spread_col="SPREAD1",
    spread_fmt_fn=fmt_cof_label,
    product_fmt_fn=fmt_oddesc,
)

# --- Section 5: LN FIX1 (Interest Free Banking) ---
fix1_ln = rate_df.filter(
    (pl.col("ACCTYPE") == "LN") & (pl.col("INTTYPE") == "FIX1")
)
render_tabulate_blr_ln(
    fix1_ln,
    titles=[
        "LOANS AND ADVANCES BY FIXED/PRESCRIBED RATE",
        "-------------------------------------------------------",
    ],
    lines=report_lines,
    pricing_label="INTEREST FREE BANKING",
    spread_col="RATES",
    spread_fmt_fn=fmt_fixedf_label,
    product_fmt_fn=fmt_lndesc,
)

# --- Section 6: LN FIX2 (Non-Interest Free Banking) ---
fix2_ln = rate_df.filter(
    (pl.col("ACCTYPE") == "LN") & (pl.col("INTTYPE") == "FIX2")
)
render_tabulate_blr_ln(
    fix2_ln,
    titles=[
        "LOANS AND ADVANCES BY FIXED/PRESCRIBED RATE",
        "-------------------------------------------------------",
    ],
    lines=report_lines,
    pricing_label="NON INTEREST FREE BANKING",
    spread_col="RATES",
    spread_fmt_fn=fmt_fixedf_label,
    product_fmt_fn=fmt_lndesc,
)

# --- Section 7: LN BNM ---
bnm_ln = rate_df.filter(
    (pl.col("ACCTYPE") == "LN") & (pl.col("INTTYPE") == "BNM")
)
render_tabulate_blr_ln(
    bnm_ln,
    titles=[
        "LOANS AND ADVANCES FUNDED BY BNM",
        "--------------------------------",
    ],
    lines=report_lines,
    pricing_label="LOANS FUNDED BY BNM",
    spread_col="BNM1",
    spread_fmt_fn=fmt_bnm_label,
    product_fmt_fn=fmt_lndesc,
)

# --- Section 8: OD BNM ---
bnm_od = od_rate_df.filter(
    (pl.col("ACCTYPE") == "OD") & (pl.col("INTTYPE") == "BNM")
)
render_tabulate_blr_ln(
    bnm_od,
    titles=[
        "LOANS AND ADVANCES FUNDED BY BNM",
        "--------------------------------",
    ],
    lines=report_lines,
    pricing_label="LOANS FUNDED BY BNM",
    spread_col="BNM1",
    spread_fmt_fn=fmt_bnm_label,
    product_fmt_fn=fmt_oddesc,
)

# ============================================================================
# STEP 11: WRITE REPORT TO FILE
# ============================================================================

with open(OUTPUT_REPORT, "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines) + "\n")

print(f"Report written to: {OUTPUT_REPORT}")
