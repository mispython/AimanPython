#!/usr/bin/env python3
"""
Program  : EIBMPB02.py
Purpose  : Weighted Average Lending Rate (Factoring System) reporting.
           Processes PBIF (Public Bank Islamic Financing) data to produce:
           1. Weighted Average Lending Rate report (by LOANTYP/INTRATE)
           2. Bumi Loans tabulation (by BRANCH/LNTYP)
           3. Amount Undrawn and No. of Accounts tabulation (by BRANCH)
           4. Listing of PBIF accounts for M9 reporting (by BRANCH/CLNAMES)
"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
# %INC PGM(PBBLNFMT) - Format definitions for loan processing
# %INC PGM(RDLMPBIF) - Process PBIF client data (merge, compute balances, etc.)
# Both are referenced below; PBBLNFMT format functions are not directly called
# in this program (inline logic used instead), but imported for completeness.
#
# from PBBLNFMT import (
#     format_lndenom, format_lnprod, format_odcustcd, format_locustcd,
#     format_lncustcd, format_statecd, format_apprlimt, format_loansize,
#     format_mthpass, format_lnormt, format_lnrmmt, format_collcd,
#     format_riskcd, format_busind,
#     MORE_PLAN, MORE_ISLAM, HP_ALL, HP_ACTIVE, AITAB,
#     HOME_ISLAMIC, HOME_CONVENTIONAL, SWIFT_ISLAMIC, SWIFT_CONVENTIONAL,
#     FCY_PRODUCTS,
# )
#
# RDLMPBIF is executed as a dependency prior to this program.
# Its output (pbif.parquet) is read below as the PBIF dataset.

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime

# ============================================================================
# OPTIONS (SAS equivalents)
# YEARCUTOFF=1950  -> handled via date parsing
# NOCENTER NODATE NONUMBER MISSING=0 -> handled in report formatting
# ============================================================================

MISSING_NUM = 0  # SAS MISSING=0: numeric missing displayed as 0

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR      = Path("/data")
OUTPUT_DIR    = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPTDATE_FILE = BASE_DIR / "reptdate.parquet"
PBIF_FILE     = OUTPUT_DIR / "pbif.parquet"      # Output of RDLMPBIF

OUTPUT_REPORT = OUTPUT_DIR / "eibmpb02_report.txt"

# ============================================================================
# PROC FORMAT: $LNFMT
# ============================================================================

LNFMT_MAP = {
    'P1': 'PRESCRIBED RATE (HOUSING LOANS)',
    'P2': 'PRESCRIBED RATE (BNM FUNDED LOANS)',
    'P3': 'NON-PRESCRIBED RATE (HOUSING LOANS)',
    'P4': 'NON-PRESCRIBED RATE (FACTORING)',
}


def format_lnfmt(code: str) -> str:
    """$LNFMT format: map loan type code to description."""
    return LNFMT_MAP.get(str(code or '').strip(), str(code or '').strip())


# ============================================================================
# ASA CARRIAGE CONTROL CONSTANTS
# ============================================================================

ASA_NEWPAGE     = '1'  # Form feed / new page
ASA_SINGLESPACE = ' '  # Single space (advance 1 line before print)
ASA_DOUBLESPACE = '0'  # Double space (advance 2 lines before print)
ASA_NOSPACE     = '+'  # No advance (overprint)

PAGE_LENGTH = 60  # lines per page (default)
PAGE_WIDTH  = 132

# ============================================================================
# REPTDATE DERIVATION
# ============================================================================

con = duckdb.connect()

reptdate_df = con.execute(
    f"SELECT * FROM read_parquet('{REPTDATE_FILE}')"
).pl()
row_rep = reptdate_df.row(0, named=True)

REPTDATE_VAL: date = row_rep["REPTDATE"]
if isinstance(REPTDATE_VAL, datetime):
    REPTDATE_VAL = REPTDATE_VAL.date()

day_of_month = REPTDATE_VAL.day
mm           = REPTDATE_VAL.month
yy           = REPTDATE_VAL.year

# Derive week/start-day variables (replicating SELECT(DAY(REPTDATE)))
if day_of_month == 8:
    SDD = 1;  WK = '1'; WK1 = '4'; WK2 = '';  WK3 = ''
elif day_of_month == 15:
    SDD = 9;  WK = '2'; WK1 = '1'; WK2 = '';  WK3 = ''
elif day_of_month == 22:
    SDD = 16; WK = '3'; WK1 = '2'; WK2 = '';  WK3 = ''
else:
    SDD = 23; WK = '4'; WK1 = '3'; WK2 = '2'; WK3 = '1'

MM = mm
if WK == '1':
    MM1 = MM - 1
    if MM1 == 0:
        MM1 = 12
else:
    MM1 = MM

SDATE    = date(yy, mm, SDD)
QTR      = 'Y' if MM in (3, 6, 9, 12) else 'N'
REPTMON  = f"{MM:02d}"
REPTMON1 = f"{MM1:02d}"
REPTYEAR = f"{yy % 100:02d}"   # YEAR2. format: 2-digit year
REPTDAY  = f"{day_of_month:02d}"
# SDATE/MDATE as Z5. (SAS date numeric zero-padded 5-wide)
SDATE_Z5 = f"{(SDATE - date(1960, 1, 1)).days:05d}"
MDATE_Z5 = f"{(REPTDATE_VAL - date(1960, 1, 1)).days:05d}"
# RDATE as DDMMYY8. e.g. '01/06/24'
RDATE    = REPTDATE_VAL.strftime("%d/%m/%y")

NOWK  = WK
NOWK1 = WK1
NOWK2 = WK2
NOWK3 = WK3

# ============================================================================
# READ PBIF (output of RDLMPBIF)
# ============================================================================

pbif_raw = con.execute(
    f"SELECT * FROM read_parquet('{PBIF_FILE}')"
).pl()

# ============================================================================
# DATA PBIF (post-RDLMPBIF transformations)
#   LOANTYP = 'P4'
#   AMTIND  = 'D'
#   APPRLIMT = INLIMIT * 0.80
#   AMOUNT   = BALANCE
#   INTRATE  = 6.75 + DISCHRG   /* BLR=6.75 */
#   UNDRAWN  = (INLIMIT - BALANCE)
#   IF UNDRAWN < 0 THEN UNDRAWN = 0
# ============================================================================

def build_pbif(df: pl.DataFrame) -> pl.DataFrame:
    rows = df.to_dicts()
    out  = []
    for r in rows:
        r["LOANTYP"]  = 'P4'
        r["AMTIND"]   = 'D'
        inlimit       = float(r.get("INLIMIT")  or 0.0)
        balance       = float(r.get("BALANCE")  or 0.0)
        dischrg       = float(r.get("DISCHRG")  or 0.0)
        r["APPRLIMT"] = inlimit * 0.80
        r["AMOUNT"]   = balance
        r["INTRATE"]  = 6.75 + dischrg   # BLR=6.75
        undrawn       = inlimit - balance
        r["UNDRAWN"]  = undrawn if undrawn >= 0.0 else 0.0
        out.append(r)
    return pl.DataFrame(out) if out else pl.DataFrame()


pbif = build_pbif(pbif_raw)

# ============================================================================
# DATA BUMI
#   SET PBIF
#   IF CUSTFISS IN ('61','66','77')
#   LNTYP = 'FAC'
# ============================================================================

bumi_rows = []
for r in pbif.to_dicts():
    custfiss = str(r.get("CUSTFISS") or "").strip()
    if custfiss in ('61', '66', '77'):
        r["LNTYP"] = 'FAC'
        bumi_rows.append(r)

bumi = pl.DataFrame(bumi_rows) if bumi_rows else pl.DataFrame()

# ============================================================================
# PROC SUMMARY DATA=PBIF NWAY: CLASS LOANTYP INTRATE; VAR BALANCE; SUM= -> AVGD
# DATA AVGD: PRODUCT = INTRATE * BALANCE
# ============================================================================

if not pbif.is_empty() and "LOANTYP" in pbif.columns and "INTRATE" in pbif.columns:
    avgd = (
        pbif.group_by(["LOANTYP", "INTRATE"])
            .agg(pl.col("BALANCE").sum())
    )
    avgd = avgd.with_columns(
        (pl.col("INTRATE") * pl.col("BALANCE")).alias("PRODUCT")
    ).sort(["LOANTYP", "INTRATE"])
else:
    avgd = pl.DataFrame()

# ============================================================================
# PROC SORT DATA=PBIF (for M9 listing)
#   WHERE CUSTFISS IN ('11','12','13','30','32','33','34','35',
#                      '36','04','05','06','37','38','39','40')
#   BY BRANCH CLNAMES
# ============================================================================

M9_CUSTFISS = {
    '11', '12', '13', '30', '32', '33', '34', '35',
    '36', '04', '05', '06', '37', '38', '39', '40',
}

pbif_m9_rows = [
    r for r in pbif.to_dicts()
    if str(r.get("CUSTFISS") or "").strip() in M9_CUSTFISS
]
pbif_m9 = (
    pl.DataFrame(pbif_m9_rows).sort(["BRANCH", "CLNAMES"])
    if pbif_m9_rows else pl.DataFrame()
)

# ============================================================================
# PROC SUMMARY DATA=PBIF (M9) NWAY
#   CLASS BRANCH CLNAMES CUSTFISS; VAR BALANCE; SUM= -> PBIX (DROP=_TYPE_ _FREQ_)
# ============================================================================

if not pbif_m9.is_empty():
    pbix = (
        pbif_m9.group_by(["BRANCH", "CLNAMES", "CUSTFISS"])
               .agg(pl.col("BALANCE").sum())
               .sort(["BRANCH", "CLNAMES", "CUSTFISS"])
    )
else:
    pbix = pl.DataFrame()

# ============================================================================
# REPORT GENERATION HELPERS
# ============================================================================

TITLE1  = "PUBLIC BANK BERHAD PROGRAM-ID: EIBMPB02"
COL_W   = 17
CNT_W   = 9


def fmt_comma(val, width: int = 18, dec: int = 2) -> str:
    """Format number as COMMA18.2 (with commas, 2 decimal places)."""
    try:
        f = float(val if val is not None else MISSING_NUM)
        return f"{f:,.{dec}f}".rjust(width)
    except (TypeError, ValueError):
        return str(MISSING_NUM).rjust(width)


def fmt_comma9(val) -> str:
    """Format number as COMMA9. (integer with commas)."""
    try:
        f = float(val if val is not None else MISSING_NUM)
        return f"{int(round(f)):,}".rjust(CNT_W)
    except (TypeError, ValueError):
        return str(MISSING_NUM).rjust(CNT_W)


def page_header(title2: str, title3: str = "") -> list:
    """Generate page header lines with ASA carriage control."""
    lines = []
    lines.append(f"{ASA_NEWPAGE}{TITLE1.center(PAGE_WIDTH - 1)}")
    lines.append(f"{ASA_SINGLESPACE}{title2.center(PAGE_WIDTH - 1)}")
    if title3:
        lines.append(f"{ASA_SINGLESPACE}{title3.center(PAGE_WIDTH - 1)}")
    lines.append(f"{ASA_DOUBLESPACE}")
    return lines


# ============================================================================
# REPORT 1: PROC PRINT DATA=AVGD
#   TITLE2 'WEIGHTED AVERAGE LENDING RATE (FACTORING SYSTEM)'
#   TITLE3 'AS AT ' &RDATE
#   BY LOANTYP; PAGEBY LOANTYP; SUMBY LOANTYP
#   SUM BALANCE PRODUCT
#   FORMAT LOANTYP $LNFMT. BALANCE PRODUCT COMMA18.2
# ============================================================================

report_lines: list = []

title2_r1 = "WEIGHTED AVERAGE LENDING RATE (FACTORING SYSTEM)"
title3_r1 = f"AS AT {RDATE}"

if not avgd.is_empty():
    # Group by LOANTYP for PAGEBY / SUMBY
    loantyp_groups: dict = {}
    for r in avgd.to_dicts():
        lt = str(r.get("LOANTYP") or "")
        loantyp_groups.setdefault(lt, []).append(r)

    grand_balance = 0.0
    grand_product = 0.0

    for lt, rows in sorted(loantyp_groups.items()):
        # PAGEBY LOANTYP -> new page per group
        report_lines.extend(page_header(title2_r1, title3_r1))

        # Column header
        report_lines.append(
            f"{ASA_SINGLESPACE}{'LOANTYP':<40}  {'INTRATE':>10}  {'BALANCE':>18}  {'PRODUCT':>18}"
        )
        report_lines.append(
            f"{ASA_SINGLESPACE}{'-'*40}  {'-'*10}  {'-'*18}  {'-'*18}"
        )

        sub_balance = 0.0
        sub_product = 0.0

        for r in rows:
            lnfmt_label = format_lnfmt(lt)
            intrate     = float(r.get("INTRATE") or 0.0)
            balance     = float(r.get("BALANCE") or 0.0)
            product     = float(r.get("PRODUCT") or 0.0)
            sub_balance += balance
            sub_product += product
            grand_balance += balance
            grand_product += product

            report_lines.append(
                f"{ASA_SINGLESPACE}{lnfmt_label:<40}  {intrate:>10.2f}  "
                f"{fmt_comma(balance)}  {fmt_comma(product)}"
            )

        # SUMBY LOANTYP subtotal
        report_lines.append(
            f"{ASA_SINGLESPACE}{'-'*40}  {'-'*10}  {'-'*18}  {'-'*18}"
        )
        report_lines.append(
            f"{ASA_SINGLESPACE}{'SUBTOTAL':<40}  {'':>10}  "
            f"{fmt_comma(sub_balance)}  {fmt_comma(sub_product)}"
        )
        report_lines.append(f"{ASA_DOUBLESPACE}")

    # Grand total (SUM)
    report_lines.append(
        f"{ASA_SINGLESPACE}{'='*40}  {'':>10}  {'='*18}  {'='*18}"
    )
    report_lines.append(
        f"{ASA_SINGLESPACE}{'GRAND TOTAL':<40}  {'':>10}  "
        f"{fmt_comma(grand_balance)}  {fmt_comma(grand_product)}"
    )
else:
    report_lines.extend(page_header(title2_r1, title3_r1))
    report_lines.append(f"{ASA_SINGLESPACE}NO DATA")

# ============================================================================
# REPORT 2: PROC TABULATE DATA=BUMI MISSING NOSEPS
#   TITLE2 'BUMI LOANS (FACTORING SYSTEM) AS AT ' &RDATE
#   CLASS BRANCH LNTYP; VAR BALANCE
#   TABLE BRANCH=' ' ALL='TOTAL', (LNTYP=' ' ALL='TOTAL')*
#         (SUM=' '*BALANCE*F=COMMA17.2 N='NUMBER'*F=COMMA9.)
#         / BOX='BRANCH' RTS=9
# ============================================================================

title2_r2 = f"BUMI LOANS (FACTORING SYSTEM) AS AT {RDATE}"

report_lines.extend(page_header(title2_r2))


def tabulate_branch_lntyp(df: pl.DataFrame) -> list:
    """Generate PROC TABULATE style report: BRANCH x LNTYP with SUM BALANCE and N."""
    lines = []
    if df.is_empty():
        lines.append(f"{ASA_SINGLESPACE}NO DATA")
        return lines

    branches = sorted(df["BRANCH"].unique().to_list())
    lntyps   = sorted(df["LNTYP"].unique().to_list()) if "LNTYP" in df.columns else []

    # Build lookup: (branch, lntyp) -> (sum, count)
    lookup: dict = {}
    for r in df.to_dicts():
        br  = str(r.get("BRANCH") or "")
        lt  = str(r.get("LNTYP")  or "")
        bal = float(r.get("BALANCE") or 0.0)
        ps, pc = lookup.get((br, lt), (0.0, 0))
        lookup[(br, lt)] = (ps + bal, pc + 1)

    # Column headers: each LNTYP + TOTAL
    col_headers = lntyps + ['TOTAL']
    header_line = f"{ASA_SINGLESPACE}{'BRANCH':<9}"
    for col in col_headers:
        header_line += f"  {col:>{COL_W}}  {'NUMBER':>{CNT_W}}"
    lines.append(header_line)
    sep_len = 9 + (COL_W + CNT_W + 4) * len(col_headers)
    lines.append(f"{ASA_SINGLESPACE}{'-' * sep_len}")

    grand_sums: dict = {col: 0.0 for col in col_headers}
    grand_cnts: dict = {col: 0   for col in col_headers}

    for br in branches:
        row_str          = f"{ASA_SINGLESPACE}{str(br):<9}"
        row_total_sum    = 0.0
        row_total_cnt    = 0
        for lt in lntyps:
            s, c = lookup.get((br, lt), (0.0, 0))
            row_str          += f"  {fmt_comma(s, COL_W)}  {fmt_comma9(c)}"
            grand_sums[lt]   += s
            grand_cnts[lt]   += c
            row_total_sum    += s
            row_total_cnt    += c
        # ALL='TOTAL' column
        row_str               += f"  {fmt_comma(row_total_sum, COL_W)}  {fmt_comma9(row_total_cnt)}"
        grand_sums['TOTAL']   += row_total_sum
        grand_cnts['TOTAL']   += row_total_cnt
        lines.append(row_str)

    # TOTAL row
    lines.append(f"{ASA_SINGLESPACE}{'-' * sep_len}")
    total_str = f"{ASA_SINGLESPACE}{'TOTAL':<9}"
    for col in col_headers:
        total_str += f"  {fmt_comma(grand_sums[col], COL_W)}  {fmt_comma9(grand_cnts[col])}"
    lines.append(total_str)

    return lines


report_lines.extend(tabulate_branch_lntyp(bumi))

# ============================================================================
# REPORT 3: PROC TABULATE DATA=PBIF MISSING NOSEPS
#   TITLE2 'AMOUNT UNDRAWN AND NO. OF ACCOUNTS'
#   TITLE3 'FOR FACTORING SYSTEM AS AT ' &RDATE
#   CLASS BRANCH; VAR UNDRAWN
#   TABLE BRANCH=' ' ALL='TOTAL',
#         (SUM=' '*UNDRAWN='AMOUNT'*F=COMMA17.2 N='NUMBER'*F=COMMA9.)
#         / BOX='BRANCH' RTS=9
# ============================================================================

title2_r3 = "AMOUNT UNDRAWN AND NO. OF ACCOUNTS"
title3_r3 = f"FOR FACTORING SYSTEM AS AT {RDATE}"

report_lines.extend(page_header(title2_r3, title3_r3))


def tabulate_branch_undrawn(df: pl.DataFrame) -> list:
    """Generate PROC TABULATE: BRANCH x (SUM UNDRAWN='AMOUNT', N='NUMBER')."""
    lines = []
    if df.is_empty():
        lines.append(f"{ASA_SINGLESPACE}NO DATA")
        return lines

    branches = sorted(df["BRANCH"].unique().to_list())

    # Build lookup: branch -> (sum_undrawn, count)
    lookup: dict = {}
    for r in df.to_dicts():
        br  = str(r.get("BRANCH") or "")
        udr = float(r.get("UNDRAWN") or 0.0)
        ps, pc = lookup.get(br, (0.0, 0))
        lookup[br] = (ps + udr, pc + 1)

    sep_len = 9 + COL_W + CNT_W + 4
    lines.append(
        f"{ASA_SINGLESPACE}{'BRANCH':<9}  {'AMOUNT':>{COL_W}}  {'NUMBER':>{CNT_W}}"
    )
    lines.append(f"{ASA_SINGLESPACE}{'-' * sep_len}")

    grand_sum = 0.0
    grand_cnt = 0

    for br in branches:
        s, c = lookup.get(br, (0.0, 0))
        lines.append(
            f"{ASA_SINGLESPACE}{str(br):<9}  {fmt_comma(s, COL_W)}  {fmt_comma9(c)}"
        )
        grand_sum += s
        grand_cnt += c

    lines.append(f"{ASA_SINGLESPACE}{'-' * sep_len}")
    lines.append(
        f"{ASA_SINGLESPACE}{'TOTAL':<9}  {fmt_comma(grand_sum, COL_W)}  {fmt_comma9(grand_cnt)}"
    )

    return lines


report_lines.extend(tabulate_branch_undrawn(pbif))

# ============================================================================
#  LISTING OF LOAN ACCOUNTS FOR M9 REPORTING
# ============================================================================

# ============================================================================
# REPORT 4: PROC PRINT DATA=PBIX
#   TITLE2 'LISTING OF PBIF ACCOUNTS FOR M9 REPORTING AS AT' &RDATE
#   TITLE3 '(FOR BNM/BRG/M9 REPORT - LOANS TO BUMIPUTRA COMMUNITY)'
#   FORMAT BALANCE COMMA17.2
#   VAR CLNAMES BALANCE CUSTFISS; BY BRANCH; SUM BALANCE
# ============================================================================

title2_r4 = f"LISTING OF PBIF ACCOUNTS FOR M9 REPORTING AS AT {RDATE}"
title3_r4 = "(FOR BNM/BRG/M9 REPORT - LOANS TO BUMIPUTRA COMMUNITY)"

report_lines.extend(page_header(title2_r4, title3_r4))

if not pbix.is_empty():
    branches_m9 = sorted(pbix["BRANCH"].unique().to_list())
    grand_bal   = 0.0

    for br in branches_m9:
        # BY BRANCH group header
        report_lines.append(f"{ASA_DOUBLESPACE}BRANCH={br}")
        report_lines.append(
            f"{ASA_SINGLESPACE}{'CLNAMES':<40}  {'BALANCE':>17}  {'CUSTFISS':>8}"
        )
        report_lines.append(
            f"{ASA_SINGLESPACE}{'-'*40}  {'-'*17}  {'-'*8}"
        )

        branch_bal  = 0.0
        branch_rows = [
            r for r in pbix.to_dicts()
            if str(r.get("BRANCH") or "") == str(br)
        ]

        for r in branch_rows:
            clnames  = str(r.get("CLNAMES")  or "")
            balance  = float(r.get("BALANCE")  or 0.0)
            custfiss = str(r.get("CUSTFISS") or "")
            branch_bal += balance
            grand_bal  += balance
            report_lines.append(
                f"{ASA_SINGLESPACE}{clnames:<40}  {fmt_comma(balance, 17)}  {custfiss:>8}"
            )

        # SUM per BRANCH
        report_lines.append(
            f"{ASA_SINGLESPACE}{'-'*40}  {'-'*17}  {'-'*8}"
        )
        report_lines.append(
            f"{ASA_SINGLESPACE}{'BRANCH TOTAL':<40}  {fmt_comma(branch_bal, 17)}  {'':>8}"
        )
        report_lines.append(f"{ASA_DOUBLESPACE}")

    # Grand total (SUM BALANCE)
    report_lines.append(
        f"{ASA_SINGLESPACE}{'='*40}  {'='*17}  {'='*8}"
    )
    report_lines.append(
        f"{ASA_SINGLESPACE}{'GRAND TOTAL':<40}  {fmt_comma(grand_bal, 17)}  {'':>8}"
    )
else:
    report_lines.append(f"{ASA_SINGLESPACE}NO DATA")

# ============================================================================
# WRITE REPORT OUTPUT
# ============================================================================

with open(OUTPUT_REPORT, "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines) + "\n")

print(f"Report written to: {OUTPUT_REPORT}")
