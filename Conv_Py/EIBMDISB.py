#!/usr/bin/env python3
"""
Program  : EIBMDISB.py
Purpose  : TOP 20 DISBURSEMENTS & REPAYMENTS - FOR EMILY ONG
"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
# from PBBLNFMT import format_lnprod  # LIQPFMT equivalent not directly mapped;
#                                     # RC detection logic handled inline below

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime
import calendar

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR       = Path("/data")
BNM_DIR        = BASE_DIR / "bnm"
LOAN_DIR       = BASE_DIR / "loan"
CISDP_DIR      = BASE_DIR / "cisdp"
CISLN_DIR      = BASE_DIR / "cisln"
DISB_DIR       = BASE_DIR / "disb"

REPTDATE_FILE  = BNM_DIR  / "reptdate.parquet"
SDESC_FILE     = BNM_DIR  / "sdesc.parquet"
LNCOMM_FILE    = LOAN_DIR / "lncomm.parquet"
DEPOSIT_FILE   = CISDP_DIR / "deposit.parquet"
LOAN_CIS_FILE  = CISLN_DIR / "loan.parquet"

OUTPUT_REPORT  = DISB_DIR / "EIBMDISB_report.txt"
OUTPUT_DISB    = DISB_DIR / "disb.parquet"

# Ensure output directory exists
DISB_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# PAGE / REPORT CONSTANTS
# ============================================================================

PAGE_LENGTH = 60   # lines per page (default)
LINE_WIDTH  = 132

# ============================================================================
# LIQPFMT equivalent: Revolving Credit (RC) product codes
# Products mapped to '34190' in LNPROD_MAP (revolving credit category)
# ============================================================================

RC_PRODUCTS = {
    146, 184, 190, 192, 195, 196, 302, 350, 351, 364, 365, 506,
    495, 604, 605, 634, 641, 660, 685, 689, 802, 803, 806, 808,
    810, 812, 814, 817, 818, 856, 857, 858, 859, 860, 902, 903,
    910, 917, 925, 951,
}

# ============================================================================
# HELPER: format date as DDMMYYYY (SAS DDMMYY8. = DD/MM/YY, here we do full)
# ============================================================================

def fmt_ddmmyy8(d: date) -> str:
    """Format date as DD/MM/YY (SAS DDMMYY8.)"""
    return d.strftime("%d/%m/%y")


def fmt_z2(n: int) -> str:
    """Zero-padded 2-digit integer"""
    return f"{n:02d}"


# ============================================================================
# STEP 1: READ REPTDATE & DERIVE MACRO VARIABLES
# ============================================================================

con = duckdb.connect()

reptdate_df = con.execute(f"SELECT * FROM read_parquet('{REPTDATE_FILE}')").pl()
row = reptdate_df.row(0, named=True)

REPTDATE: date = row["REPTDATE"]
if isinstance(REPTDATE, datetime):
    REPTDATE = REPTDATE.date()

MM   = REPTDATE.month
MM1  = MM - 1 if MM > 1 else 12
YEAR = REPTDATE.year if MM > 1 else (REPTDATE.year - 1)

SDATE = date(REPTDATE.year, MM, 1)

NOWK      = "4"
REPTMON   = fmt_z2(MM)
REPTMON1  = fmt_z2(MM1)
RDATE     = fmt_ddmmyy8(REPTDATE)
SDATE_STR = fmt_ddmmyy8(SDATE)

# ============================================================================
# STEP 2: READ SDESC
# ============================================================================

sdesc_df = con.execute(f"SELECT * FROM read_parquet('{SDESC_FILE}')").pl()
SDESC = str(sdesc_df["SDESC"][0]).ljust(26)[:26]

# ============================================================================
# STEP 3: LOAD PREVIOUS MONTH LOAN DATA (ALW1)
# ============================================================================

prev_loan_file = BNM_DIR / f"loan{REPTMON1}{NOWK}.parquet"

alw1_query = f"""
SELECT *
FROM read_parquet('{prev_loan_file}')
WHERE SUBSTR(CAST(PRODCD AS VARCHAR), 1, 3) IN ('341','342','343','344')
   OR PRODUCT IN (225, 226)
"""
alw1 = con.execute(alw1_query).pl().rename({
    "CURBAL":   "LASTBAL",
    "BALANCE":  "LSTODBAL",
    "NOTETERM": "LASTNOTE",
    "INTAMT":   "INTAMTO",
})

# ============================================================================
# STEP 4: LOAD CURRENT MONTH LOAN DATA (ALW)
# ============================================================================

curr_loan_file = BNM_DIR / f"loan{REPTMON}{NOWK}.parquet"

alw_query = f"""
SELECT *
FROM read_parquet('{curr_loan_file}')
WHERE SUBSTR(CAST(PRODCD AS VARCHAR), 1, 3) IN ('341','342','343','344')
   OR PRODUCT IN (225, 226)
"""
alw = con.execute(alw_query).pl()

# ============================================================================
# STEP 5: MERGE ALW1 & ALW, COMPUTE DISBURSE / REPAID
# ============================================================================

# Join on ACCTNO + NOTENO (full outer to replicate SAS MERGE with IN= flags)
merged = alw.join(
    alw1.select(["ACCTNO", "NOTENO", "LASTBAL", "LSTODBAL", "LASTNOTE", "INTAMTO"]),
    on=["ACCTNO", "NOTENO"],
    how="full",
)

# Filter: APPRDATE <= RDATE
merged = merged.filter(pl.col("APPRDATE") <= REPTDATE)

# If APPRDATE < SDATE then APPRLIMT = 0
merged = merged.with_columns(
    pl.when(pl.col("APPRDATE") < SDATE)
      .then(pl.lit(0.0))
      .otherwise(pl.col("APPRLIMT"))
      .alias("APPRLIMT")
)

# Islamic interest adjustment (ACCTYPE='LN' AND AMTIND='I')
merged = merged.with_columns(
    pl.when((pl.col("ACCTYPE") == "LN") & (pl.col("AMTIND") == "I"))
      .then(pl.col("CURBAL") - pl.col("INTAMT"))
      .otherwise(pl.col("CURBAL"))
      .alias("CURBAL"),
    pl.when((pl.col("ACCTYPE") == "LN") & (pl.col("AMTIND") == "I"))
      .then(pl.col("LASTBAL") - pl.col("INTAMTO"))
      .otherwise(pl.col("LASTBAL"))
      .alias("LASTBAL"),
)

# Flags: in_A = present in current, in_B = present in prev
in_a = merged["CURBAL"].is_not_null()    # present in current (B in SAS = current)
in_b = merged["LASTBAL"].is_not_null()   # present in previous (A in SAS = prev)

# DISBURSE / REPAID logic
def compute_disbrepaid(df: pl.DataFrame) -> pl.DataFrame:
    rows = df.to_dicts()
    out = []
    for r in rows:
        a = r.get("LASTBAL") is not None   # in previous
        b = r.get("CURBAL")  is not None   # in current
        acc  = r.get("ACCTYPE", "")
        lb   = r.get("LASTBAL")  or 0.0
        cb   = r.get("CURBAL")   or 0.0
        lodbal = r.get("LSTODBAL") or 0.0
        bal    = r.get("BALANCE")  or 0.0

        disb = 0.0
        repd = 0.0

        if a and b:
            if acc == "LN":
                if lb > cb:
                    repd = lb - cb
                else:
                    disb = cb - lb
            else:
                if lodbal > bal:
                    repd = lodbal - bal
                else:
                    disb = bal - lodbal
        elif not b:   # only in previous
            if acc == "LN":
                repd = lb
            else:
                repd = lodbal
        elif not a:   # only in current
            if acc == "LN":
                disb = cb
            else:
                disb = bal

        r["DISBURSE"] = disb
        r["REPAID"]   = repd
        out.append(r)
    return pl.DataFrame(out)

merged = compute_disbrepaid(merged)

# LIMIT derivation
merged = merged.with_columns(
    pl.when(pl.col("APPRLIMT").is_null() | (pl.col("APPRLIMT") == 0))
      .then(pl.col("NETPROC"))
      .otherwise(pl.col("APPRLIMT"))
      .alias("LIMIT")
)

# Override LIMIT with NETPROC for certain account/product combos
merged = merged.with_columns(
    pl.when(
        ((pl.col("ACCTNO") > 8_000_000_000) &
         pl.col("PRODUCT").is_in([122,106,128,130,120,121,700,705,709,710])) |
        ((pl.col("ACCTNO") < 2_999_999_999) &
         pl.col("PRODUCT").is_in([100,101,102,103,110,111,112,113,114,115,
                                   116,117,118,120,121,122,123,124,125,127,
                                   135,136,170,180,106,128,130,700,705,709,
                                   710,194,195,380,381]))
    )
    .then(pl.col("NETPROC"))
    .otherwise(pl.col("LIMIT"))
    .alias("LIMIT")
)

# ============================================================================
# STEP 6: MERGE WITH LNCOMM (NODUPKEY on ACCTNO+COMMNO)
# ============================================================================

comm_query = f"""
SELECT ACCTNO, COMMNO, CORGAMT, INTAMT
FROM read_parquet('{LNCOMM_FILE}')
QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO, COMMNO ORDER BY ACCTNO) = 1
"""
comm = con.execute(comm_query).pl()

alw2 = merged.join(comm, on=["ACCTNO", "COMMNO"], how="left", suffix="_COMM")

# Update LIMIT for specific NOTENO ranges when COMM match found
alw2 = alw2.with_columns(
    pl.when(
        pl.col("CORGAMT").is_not_null() &
        (
            ((pl.col("NOTENO") >= 10010) & (pl.col("NOTENO") <= 10099)) |
            ((pl.col("NOTENO") >= 30010) & (pl.col("NOTENO") <= 30099))
        )
    )
    .then(pl.col("CORGAMT") - pl.col("INTAMT_COMM").fill_null(0))
    .otherwise(pl.col("LIMIT"))
    .alias("LIMIT")
)

# Filter: DISBURSE > 0
alw2 = alw2.filter(pl.col("DISBURSE") > 0)

# Save DISB output sorted by DESCENDING DISBURSE
disb_sorted = alw2.sort("DISBURSE", descending=True)
disb_sorted.write_parquet(str(OUTPUT_DISB))

# ============================================================================
# STEP 7: BUILD CIS (from DEPOSIT + LOAN, SECCUST='901')
# ============================================================================

cis_dep_query = f"""
SELECT ACCTNO, CUSTNO, CUSTNAME
FROM read_parquet('{DEPOSIT_FILE}')
WHERE SECCUST = '901'
"""
cis_ln_query = f"""
SELECT ACCTNO, CUSTNO, CUSTNAME
FROM read_parquet('{LOAN_CIS_FILE}')
WHERE SECCUST = '901'
"""
cis = pl.concat([
    con.execute(cis_dep_query).pl(),
    con.execute(cis_ln_query).pl(),
])

# ============================================================================
# STEP 8: MERGE ALW2 WITH CIS (inner join)
# ============================================================================

disb = alw2.join(cis, on="ACCTNO", how="inner")

# ============================================================================
# STEP 9: SUMMARISE BY CUSTNO, RANK TOP 40
# ============================================================================

topdb = (
    disb.group_by("CUSTNO")
        .agg(pl.col("DISBURSE").sum())
        .sort("DISBURSE", descending=True)
        .with_row_index("RANK", offset=1)
        .filter(pl.col("RANK") < 41)
)

# ============================================================================
# STEP 10: MERGE TOPDB BACK TO DISB DETAIL
# ============================================================================

disb = topdb.join(disb, on="CUSTNO", how="inner", suffix="_D")

# Fill nulls for LIMIT
disb = disb.with_columns(pl.col("LIMIT").fill_null(0))

# ============================================================================
# STEP 11: RC / NR SPLIT using LIQPFMT equivalent
# ============================================================================

disb = disb.with_columns(
    pl.when(pl.col("PRODUCT").is_in(list(RC_PRODUCTS)))
      .then(pl.lit("RC"))
      .otherwise(pl.lit("NR"))
      .alias("RC")
)

nr_df = disb.filter(pl.col("RC") != "RC")
rc_df = disb.filter(pl.col("RC") == "RC")

# ============================================================================
# STEP 12: AGGREGATE NR (take first per ACCTNO, sum LIMIT/DISBURSE/BALANCE)
# ============================================================================

nr_agg = (
    nr_df.sort("ACCTNO")
         .group_by("ACCTNO")
         .agg([
             pl.col("LIMIT").sum().alias("TLIMIT"),
             pl.col("DISBURSE").sum().alias("TDISBU"),
             pl.col("BALANCE").sum().alias("TBALAN"),
             pl.col("RANK").first(),
             pl.col("CUSTNO").first(),
             pl.col("CUSTNAME").first(),
             pl.col("BRANCH").first(),
             pl.col("SECTORMA").first(),
         ])
)

# ============================================================================
# STEP 13: AGGREGATE RC (last LIMIT per ACCTNO, sum DISBURSE/BALANCE)
# ============================================================================

rc_agg = (
    rc_df.sort(["ACCTNO", "LIMIT"])
         .group_by("ACCTNO")
         .agg([
             pl.col("LIMIT").last().alias("TLIMIT"),
             pl.col("DISBURSE").sum().alias("TDISBU"),
             pl.col("BALANCE").sum().alias("TBALAN"),
             pl.col("RANK").first(),
             pl.col("CUSTNO").first(),
             pl.col("CUSTNAME").first(),
             pl.col("BRANCH").first(),
             pl.col("SECTORMA").first(),
         ])
)

# ============================================================================
# STEP 14: COMBINE RC + NR, AGGREGATE BY ACCTNO
# ============================================================================

combined = pl.concat([rc_agg, nr_agg])

combined = (
    combined.sort("ACCTNO")
            .group_by("ACCTNO")
            .agg([
                pl.col("TLIMIT").sum().alias("LIMIT"),
                pl.col("TBALAN").sum().alias("BALANCE"),
                pl.col("TDISBU").sum().alias("DISBURSE"),
                pl.col("RANK").first(),
                pl.col("CUSTNO").first(),
                pl.col("CUSTNAME").first(),
                pl.col("BRANCH").first(),
                pl.col("SECTORMA").first(),
            ])
)

# ============================================================================
# STEP 15: TOP 20 by DISBURSE (N < 21)
# ============================================================================

final = (
    combined.sort(["DISBURSE", "BALANCE"], descending=[True, True])
            .with_row_index("N", offset=1)
            .filter(pl.col("N") <= 20)
)

# ============================================================================
# STEP 16: PRODUCE REPORT WITH ASA CARRIAGE CONTROL CHARACTERS
# ============================================================================

def fmt_comma15_2(val) -> str:
    """Format number as COMMA15.2 (right-justified, 15 chars)"""
    if val is None:
        return " " * 15
    try:
        formatted = f"{float(val):,.2f}"
        return formatted.rjust(15)
    except (TypeError, ValueError):
        return " " * 15


def write_report(df: pl.DataFrame, output_path: Path) -> None:
    """Write the PROC PRINT equivalent report with ASA carriage control."""
    lines = []
    page_num    = 0
    line_count  = PAGE_LENGTH  # trigger new page on first record

    title1 = "REPORT ID: EIBMDISB(2)"
    title2 = SDESC.strip()
    title3 = "FINANCE DIVISION"
    title4 = f"TOP 20 ACCOUNTS DISBURSED FOR THE MONTH AS AT {RDATE}"

    header_cols = (
        f"{'BRANCH':<10}  {'ACCTNO':<15}  {'CUSTNAME':<30}  "
        f"{'CUSTNO':<12}  {'SECTORMA':<10}  "
        f"{'LIMIT':>15}  {'BALANCE':>15}  {'DISBURSE':>15}"
    )
    separator = "-" * LINE_WIDTH

    def new_page() -> list:
        nonlocal page_num
        page_num += 1
        pg_lines = []
        # ASA '1' = form feed / new page
        pg_lines.append(f"1{title1.center(LINE_WIDTH)}")
        pg_lines.append(f" {title2.center(LINE_WIDTH)}")
        pg_lines.append(f" {title3.center(LINE_WIDTH)}")
        pg_lines.append(f" {title4.center(LINE_WIDTH)}")
        pg_lines.append(f" ")
        pg_lines.append(f" {header_cols}")
        pg_lines.append(f" {separator}")
        return pg_lines

    for row in df.iter_rows(named=True):
        if line_count >= PAGE_LENGTH - 4:
            lines.extend(new_page())
            line_count = 7  # header takes ~7 lines

        branch   = str(row.get("BRANCH",   "") or "").ljust(10)
        acctno   = str(row.get("ACCTNO",   "") or "").ljust(15)
        custname = str(row.get("CUSTNAME", "") or "").ljust(30)
        custno   = str(row.get("CUSTNO",   "") or "").ljust(12)
        sectorma = str(row.get("SECTORMA", "") or "").ljust(10)
        limit    = fmt_comma15_2(row.get("LIMIT"))
        balance  = fmt_comma15_2(row.get("BALANCE"))
        disburse = fmt_comma15_2(row.get("DISBURSE"))

        detail = (
            f"{branch}  {acctno}  {custname}  "
            f"{custno}  {sectorma}  "
            f"{limit}  {balance}  {disburse}"
        )
        # ASA ' ' = single space (advance one line)
        lines.append(f" {detail}")
        line_count += 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


write_report(final, OUTPUT_REPORT)

print(f"Report written to : {OUTPUT_REPORT}")
print(f"DISB parquet saved: {OUTPUT_DISB}")
