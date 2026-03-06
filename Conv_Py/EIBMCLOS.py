#!/usr/bin/env python3
"""
Program  : EIBMCLOS.py
Purpose  : Monthly Report on Reasons of Closed Savings and Current Accounts
           Breakdown by Products.
           Produces two PROC TABULATE reports:
           1. Closed Savings Accounts (CLOSSAV) - by RCODE x TYPEX
           2. Closed Current Accounts (CLOSCUR) - by RCODE x TYPEX
"""

# ============================================================================
# No external program dependencies (%INC) declared in SAS source.
# All formats ($TYPEC, $CODEC) are defined inline within this program.
# ============================================================================

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime

# ============================================================================
# OPTIONS (SAS equivalents)
# YEARCUTOFF=1930 NOCENTER NODATE -> handled via date parsing and formatting
# MISSING=0 -> missing numerics displayed as 0
# ============================================================================

MISSING_NUM = 0  # SAS MISSING=0

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR   = Path("/data")
MNITB_DIR  = BASE_DIR / "mnitb"    # SAP.PBB.MNITB(0)
PRODEV_DIR = BASE_DIR / "prodev"   # SAP.PBB.CLOSED.ACCTS

OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPTDATE_FILE = MNITB_DIR  / "reptdate.parquet"
SAVNG_FILE    = PRODEV_DIR / "savng.parquet"     # PRODEV.SAVNG
CURRN_FILE    = PRODEV_DIR / "currn.parquet"     # PRODEV.CURRN

OUTPUT_REPORT = OUTPUT_DIR / "eibmclos_report.txt"

# ============================================================================
# ASA CARRIAGE CONTROL CONSTANTS
# ============================================================================

ASA_NEWPAGE     = '1'  # Form feed / new page
ASA_SINGLESPACE = ' '  # Single space (advance 1 line before print)
ASA_DOUBLESPACE = '0'  # Double space (advance 2 lines before print)

PAGE_LENGTH = 60   # lines per page (default)
PAGE_WIDTH  = 132

# ============================================================================
# PROC FORMAT: $TYPEC
# Maps 2-character TYPEX code to product description
# ============================================================================

TYPEC_MAP = {
    '01': 'PLUS CURRENT',
    '02': 'PLUS OTHER',
    '03': 'ACE',
    '04': 'PB CURRENTLINK',
    '05': 'PB SHARELINK',
    '06': 'WADIAH CURRENT-i',
    '07': 'BASIC CA (90)',
    '08': 'BASIC CA55 (91)',
    '31': 'PLUS SAVINGS',
    '32': 'YOUNG ACHIEVER',
    '33': '50 PLUS',
    '34': 'WISE',
    '35': 'PB SAVELINK',
    '36': 'WADIAH SAVINGS-i',
    '37': 'BESTARI SAVINGS-i',
    '38': 'UMA1 (297)    ',
    '39': 'UMA3 (298)    ',
    '40': 'BASIC SA (205)',
    '41': 'BASIC SA55 (206)',
}


def fmt_typec(code: str) -> str:
    """$TYPEC format: map 2-char product code to description."""
    return TYPEC_MAP.get(str(code or '').strip(), str(code or '').strip())


# ============================================================================
# PROC FORMAT: $CODEC
# Maps reason code (B001-B006, C001-C009, P001) to description
# ============================================================================

CODEC_MAP = {
    'B001': ' B001 -BMC                    ',
    'B002': ' B002 -BANKRUPT               ',
    'B003': ' B003 -INACTIVE               ',
    'B004': ' B004 -DORMANT/UNCLAIMED MONEY',
    'B005': ' B005 -BY OPERATION OF LAW    ',
    'B006': ' B006 -OVERDRAWN/TOD          ',
    'C001': ' C001 -INCONVENIENT LOCATION',
    'C002': ' C002 -MOVING AWAY/SHIFTING/CHANGE OF EMPLOYMENT',
    'C003': ' C003 -LOW DEPOSIT RATES',
    'C004': ' C004 -UNATTRACTIVE PRODUCT FEATURES/BENEFITS',
    'C005': ' C005 -LIMITED RANGE OF BANKING PRODUCTS',
    'C006': ' C006 -UNSATISFACTORY CUSTOMER SERVICE',
    'C007': ' C007 -TOO MANY ACCOUNTS MAINTAINED',
    'C008': ' C008 -SWITCHING OF PRODUCTS',
    'C009': ' C009 -CHANGE OF STATUS EG. FROM SOLE-PROP TO PARTNERSHIP',
    'P001': ' P001 -CLOSED BY PROCESSES',
}


def fmt_codec(code: str) -> str:
    """$CODEC format: map reason code to description."""
    return CODEC_MAP.get(str(code or '').strip(), str(code or '').strip())


# ============================================================================
# DATA REPTDATE: derive date macro variables
# ============================================================================

con = duckdb.connect()

reptdate_df = con.execute(
    f"SELECT * FROM read_parquet('{REPTDATE_FILE}')"
).pl()
row_rep = reptdate_df.row(0, named=True)

REPTDATE_VAL: date = row_rep["REPTDATE"]
if isinstance(REPTDATE_VAL, datetime):
    REPTDATE_VAL = REPTDATE_VAL.date()

REPDD = f"{REPTDATE_VAL.day:02d}"           # Z2.   -> e.g. '08'
REPMM = f"{REPTDATE_VAL.month:02d}"         # Z2.   -> e.g. '06'
REPYY = f"{REPTDATE_VAL.year}"              # YEAR4.-> e.g. '2024'
REPDT = REPTDATE_VAL.strftime("%d/%m/%y")   # DDMMYY8. -> e.g. '08/06/24'

# ============================================================================
# DATA DUMMY (KEEP=RCOTYP CDATE VC) - first block: I=1 TO 8 (products 01-08)
#   B{J:03d}{I:02d} for J=1..8
#   C{K:03d}{I:02d} for K=1..9
#   CDATE = REPMM || REPDD || REPYY   (format $8. -> MMDDYYYY)
# ============================================================================

CDATE_VAL = f"{REPMM}{REPDD}{REPYY}"  # MMDDYYYY


def build_dummy_ca() -> list:
    """Build DUMMYCA skeleton rows: products I=1..8, B codes J=1..8, C codes K=1..9."""
    rows = []
    for i in range(1, 9):       # I = 1 TO 8 (product)
        for j in range(1, 9):   # J = 1 TO 8
            rows.append({
                "RCOTYP": f"B{j:03d}{i:02d}",
                "CDATE":  CDATE_VAL,
                "VC":     0.0,
            })
        for k in range(1, 10):  # K = 1 TO 9
            rows.append({
                "RCOTYP": f"C{k:03d}{i:02d}",
                "CDATE":  CDATE_VAL,
                "VC":     0.0,
            })
    return rows


# ============================================================================
# DATA DUMMY (KEEP=RCOTYP CDATE VC) - second block: I=31 TO 41 (products 31-41)
#   B{J:03d}{I:02d} for J=1..6
#   C{K:03d}{I:02d} for K=1..9
# ============================================================================

def build_dummy_sa() -> list:
    """Build DUMMYSA skeleton rows: products I=31..41, B codes J=1..6, C codes K=1..9."""
    rows = []
    for i in range(31, 42):     # I = 31 TO 41 (product)
        for j in range(1, 7):   # J = 1 TO 6
            rows.append({
                "RCOTYP": f"B{j:03d}{i:02d}",
                "CDATE":  CDATE_VAL,
                "VC":     0.0,
            })
        for k in range(1, 10):  # K = 1 TO 9
            rows.append({
                "RCOTYP": f"C{k:03d}{i:02d}",
                "CDATE":  CDATE_VAL,
                "VC":     0.0,
            })
    return rows


dummyca_rows = build_dummy_ca()
dummysa_rows = build_dummy_sa()

# Sort by RCOTYP (PROC SORT DATA=DUMMY OUT=DUMMYCA/DUMMYSA)
dummyca = sorted(dummyca_rows, key=lambda r: r["RCOTYP"])
dummysa = sorted(dummysa_rows, key=lambda r: r["RCOTYP"])

# ============================================================================
# DATA CLOSSAV
#   SET DUMMYSA PRODEV.SAVNG
#   IF SUBSTR(RCOTYP,5,2) IN ('31'..'41')
#      AND SUBSTR(CDATE,5,4) = REPYY
#      AND SUBSTR(CDATE,1,2) <= REPMM
# Note: SAS SUBSTR is 1-based; Python slicing is 0-based
#   RCOTYP[4:6]  = positions 5-6 in SAS (product code)
#   CDATE[4:8]   = positions 5-8 in SAS (year part of MMDDYYYY)
#   CDATE[0:2]   = positions 1-2 in SAS (month part of MMDDYYYY)
# ============================================================================

SAV_TYPEX_VALID = {'31','32','33','34','35','36','37','38','39','40','41'}

savng_raw = con.execute(
    f"SELECT * FROM read_parquet('{SAVNG_FILE}')"
).pl()

# Merge: DUMMYSA rows then PRODEV.SAVNG rows
clossav_raw = list(dummysa) + [
    {
        "RCOTYP": str(r.get("RCOTYP") or ""),
        "CDATE":  str(r.get("CDATE")  or ""),
        "VC":     float(r.get("VC")   or 0.0),
    }
    for r in savng_raw.to_dicts()
]

# Apply WHERE filter
clossav_filtered = [
    r for r in clossav_raw
    if r["RCOTYP"][4:6] in SAV_TYPEX_VALID
    and r["CDATE"][4:8] == REPYY
    and r["CDATE"][0:2] <= REPMM
]

# PROC SORT NODUPKEYS BY RCOTYP CDATE: keep first occurrence per (RCOTYP, CDATE)
clossav_deduped: dict = {}
for r in sorted(clossav_filtered, key=lambda x: (x["RCOTYP"], x["CDATE"])):
    key = (r["RCOTYP"], r["CDATE"])
    if key not in clossav_deduped:
        clossav_deduped[key] = r

clossav_sorted = sorted(clossav_deduped.values(), key=lambda x: (x["RCOTYP"], x["CDATE"]))

# ============================================================================
# DATA CLOSCUR
#   SET DUMMYCA PRODEV.CURRN
#   IF SUBSTR(RCOTYP,5,2) IN ('01'..'08')
#      AND SUBSTR(CDATE,5,4) = REPYY
#      AND SUBSTR(CDATE,1,2) <= REPMM
# ============================================================================

CUR_TYPEX_VALID = {'01','02','03','04','05','06','07','08'}

currn_raw = con.execute(
    f"SELECT * FROM read_parquet('{CURRN_FILE}')"
).pl()

closcur_raw = list(dummyca) + [
    {
        "RCOTYP": str(r.get("RCOTYP") or ""),
        "CDATE":  str(r.get("CDATE")  or ""),
        "VC":     float(r.get("VC")   or 0.0),
    }
    for r in currn_raw.to_dicts()
]

closcur_filtered = [
    r for r in closcur_raw
    if r["RCOTYP"][4:6] in CUR_TYPEX_VALID
    and r["CDATE"][4:8] == REPYY
    and r["CDATE"][0:2] <= REPMM
]

closcur_deduped: dict = {}
for r in sorted(closcur_filtered, key=lambda x: (x["RCOTYP"], x["CDATE"])):
    key = (r["RCOTYP"], r["CDATE"])
    if key not in closcur_deduped:
        closcur_deduped[key] = r

closcur_sorted = sorted(closcur_deduped.values(), key=lambda x: (x["RCOTYP"], x["CDATE"]))

# ============================================================================
# DATA CLOSSAV (second pass)
#   KEEP RCOTYP CDATE RCODE TYPEX VS SM MS YS
#   BY RCOTYP (retained accumulation within group)
#   RCODE = SUBSTR(RCOTYP,1,4)   -> RCOTYP[0:4]
#   TYPEX = SUBSTR(RCOTYP,5,2)   -> RCOTYP[4:6]
#   IF FIRST.RCOTYP THEN MS=0; YS=0;
#   YS + VS    (retain accumulator)
#   IF SUBSTR(CDATE,1,2) = REPMM THEN MS + VS
#   IF LAST.RCOTYP THEN OUTPUT
# ============================================================================

def accumulate_group(rows: list, val_col: str, month_acc: str, year_acc: str) -> list:
    """
    Replicate SAS BY-group retained accumulation:
      - Reset accumulators on FIRST.RCOTYP
      - Accumulate year total always; month total only when CDATE month == REPMM
      - Output only on LAST.RCOTYP
    Returns list of dicts with RCOTYP, CDATE, RCODE, TYPEX, and the two accumulators.
    """
    result = []
    # Group by RCOTYP (already sorted)
    from itertools import groupby
    for rcotyp, group_iter in groupby(rows, key=lambda r: r["RCOTYP"]):
        group = list(group_iter)
        acc_month = 0.0
        acc_year  = 0.0
        last_row  = None
        for r in group:
            val   = float(r.get(val_col) or 0.0)
            cdate = str(r.get("CDATE") or "")
            acc_year  += val
            if cdate[0:2] == REPMM:
                acc_month += val
            last_row = r
        if last_row is not None:
            result.append({
                "RCOTYP": rcotyp,
                "CDATE":  last_row["CDATE"],
                "RCODE":  rcotyp[0:4],
                "TYPEX":  rcotyp[4:6],
                month_acc: acc_month,
                year_acc:  acc_year,
            })
    return result


clossav_agg = accumulate_group(clossav_sorted, val_col="VC",
                                month_acc="MS", year_acc="YS")

# ============================================================================
# DATA CLOSCUR (second pass)
#   KEEP RCOTYP CDATE RCODE TYPEX VC SC MC YC
#   Same BY-group accumulation logic as CLOSSAV but for MC/YC
# ============================================================================

closcur_agg = accumulate_group(closcur_sorted, val_col="VC",
                                month_acc="MC", year_acc="YC")

# ============================================================================
# REPORT GENERATION HELPERS
# ============================================================================

RTS_W    = 20   # RTS=20
VAL_W    = 8    # FORMAT=8.  (integer count)

# Common title block
TITLE_LINE1 = 'P U B L I C   B A N K   B E R H A D'
TITLE_LINE2 = '-----------------------------------'
TITLE_LINE5 = 'REPORT ID: EIBMCLOS'
TITLE_LINE6 = '(ATTN: MS. LIM POH LAN, PRODUCT DEV., MENARA PBB)'
TITLE_LINE7 = ' '
TITLE_LINE8 = (
    'CATEGORY : B001-B009 - CLOSED BY BANK.'
    '   C001-C010 - CLOSED BY CUSTOMER.'
    '   P001 - CLOSED BY PROCESSES.'
)


def fmt_val(val, width: int = VAL_W) -> str:
    """Format integer count as FORMAT=8. with MISSTEXT='0'."""
    try:
        v = int(round(float(val if val is not None else MISSING_NUM)))
        return str(v).rjust(width)
    except (TypeError, ValueError):
        return '0'.rjust(width)


def page_header(title3: str) -> list:
    """Build page header lines with ASA carriage control."""
    lines = []
    lines.append(f"{ASA_NEWPAGE}{TITLE_LINE1}")
    lines.append(f"{ASA_SINGLESPACE}{TITLE_LINE2}")
    lines.append(f"{ASA_SINGLESPACE}{title3}")
    lines.append(f"{ASA_SINGLESPACE} ")
    lines.append(f"{ASA_SINGLESPACE}{TITLE_LINE5}")
    lines.append(f"{ASA_SINGLESPACE}{TITLE_LINE6}")
    lines.append(f"{ASA_SINGLESPACE} ")
    lines.append(f"{ASA_SINGLESPACE}{TITLE_LINE8}")
    lines.append(f"{ASA_DOUBLESPACE}")
    return lines


def build_tabulate(
    agg_rows: list,
    rcode_key: str,
    typex_key: str,
    month_col: str,
    year_col: str,
    typex_valid: set,
    title3: str,
) -> list:
    """
    Replicate PROC TABULATE NOSEPS FORMAT=8. MISSING with MISSTEXT='0' PRINTMISS.
      TABLE RCODE=' ' ALL='TOTAL',
            TYPEX=' '*(MONTH_COL='CURRENT MONTH' YEAR_COL='YEAR-TO-DATE')*SUM=' '
            ALL='TOTAL'*(MONTH_COL='CURRENT MONTH' YEAR_COL='YEAR-TO-DATE')*SUM=' '
            / RTS=20
    """
    lines = page_header(title3)

    # Collect all RCODE and TYPEX values (PRINTMISS -> include all defined codes)
    all_rcodes = sorted(
        set(CODEC_MAP.keys()),
        key=lambda c: (c[0], c[1:])
    )
    all_typex  = sorted(typex_valid)

    # Build lookup: (rcode, typex) -> {month: val, year: val}
    lookup: dict = {}
    for r in agg_rows:
        rcode = str(r.get(rcode_key) or "").strip()
        typex = str(r.get(typex_key) or "").strip()
        m_val = float(r.get(month_col) or 0.0)
        y_val = float(r.get(year_col)  or 0.0)
        key   = (rcode, typex)
        prev  = lookup.get(key, {month_col: 0.0, year_col: 0.0})
        lookup[key] = {
            month_col: prev[month_col] + m_val,
            year_col:  prev[year_col]  + y_val,
        }

    # Column header: each TYPEX has two sub-cols (CURRENT MONTH, YEAR-TO-DATE)
    # plus TOTAL*(CURRENT MONTH, YEAR-TO-DATE)
    col_labels_top = []
    col_labels_sub = []
    for tx in all_typex:
        label = fmt_typec(tx)
        col_labels_top.append(label.center(VAL_W * 2 + 3))
        col_labels_sub.append(f"{'CURRENT MONTH':>{VAL_W}}  {'YEAR-TO-DATE':>{VAL_W}}")
    col_labels_top.append("TOTAL".center(VAL_W * 2 + 3))
    col_labels_sub.append(f"{'CURRENT MONTH':>{VAL_W}}  {'YEAR-TO-DATE':>{VAL_W}}")

    hdr1 = f"{ASA_SINGLESPACE}{'':<{RTS_W}}  " + "  ".join(col_labels_top)
    hdr2 = f"{ASA_SINGLESPACE}{'':<{RTS_W}}  " + "  ".join(col_labels_sub)
    sep  = f"{ASA_SINGLESPACE}{'-' * (RTS_W + (VAL_W * 2 + 5) * (len(all_typex) + 1))}"

    lines.append(hdr1)
    lines.append(hdr2)
    lines.append(sep)

    # Grand totals
    grand_month: dict = {tx: 0.0 for tx in all_typex}
    grand_year:  dict = {tx: 0.0 for tx in all_typex}
    grand_total_month = 0.0
    grand_total_year  = 0.0

    for rcode in all_rcodes:
        rcode_label = fmt_codec(rcode).strip()
        row_str     = f"{ASA_SINGLESPACE}{rcode_label:<{RTS_W}}  "

        row_month_total = 0.0
        row_year_total  = 0.0

        cols = []
        for tx in all_typex:
            vals  = lookup.get((rcode, tx), {month_col: 0.0, year_col: 0.0})
            m_val = vals[month_col]
            y_val = vals[year_col]
            cols.append(f"{fmt_val(m_val)}  {fmt_val(y_val)}")
            grand_month[tx]  += m_val
            grand_year[tx]   += y_val
            row_month_total  += m_val
            row_year_total   += y_val

        # Row TOTAL columns
        cols.append(f"{fmt_val(row_month_total)}  {fmt_val(row_year_total)}")
        grand_total_month += row_month_total
        grand_total_year  += row_year_total

        row_str += "  ".join(cols)
        lines.append(row_str)

    # ALL='TOTAL' row
    lines.append(sep)
    total_str = f"{ASA_SINGLESPACE}{'TOTAL':<{RTS_W}}  "
    total_cols = []
    for tx in all_typex:
        total_cols.append(f"{fmt_val(grand_month[tx])}  {fmt_val(grand_year[tx])}")
    total_cols.append(f"{fmt_val(grand_total_month)}  {fmt_val(grand_total_year)}")
    total_str += "  ".join(total_cols)
    lines.append(total_str)

    return lines


# ============================================================================
# REPORT 1: PROC TABULATE DATA=CLOSSAV
#   TITLE3 'MONTHLY REPORT ON REASONS OF CLOSED SAVINGS ACCOUNTS
#            BREAKDOWN BY PRODUCTS AS AT: ' &REPDT
# ============================================================================

title3_sav = (
    f"MONTHLY REPORT ON REASONS OF CLOSED SAVINGS ACCOUNTS "
    f"BREAKDOWN BY PRODUCTS AS AT: {REPDT}"
)

report_lines = build_tabulate(
    agg_rows    = clossav_agg,
    rcode_key   = "RCODE",
    typex_key   = "TYPEX",
    month_col   = "MS",
    year_col    = "YS",
    typex_valid = SAV_TYPEX_VALID,
    title3      = title3_sav,
)

# ============================================================================
# REPORT 2: PROC TABULATE DATA=CLOSCUR
#   TITLE3 'MONTHLY REPORT ON REASONS OF CLOSED CURRENT ACCOUNTS
#            BREAKDOWN BY PRODUCTS AS AT: ' &REPDT
# ============================================================================

title3_cur = (
    f"MONTHLY REPORT ON REASONS OF CLOSED CURRENT ACCOUNTS "
    f"BREAKDOWN BY PRODUCTS AS AT: {REPDT}"
)

report_lines += build_tabulate(
    agg_rows    = closcur_agg,
    rcode_key   = "RCODE",
    typex_key   = "TYPEX",
    month_col   = "MC",
    year_col    = "YC",
    typex_valid = CUR_TYPEX_VALID,
    title3      = title3_cur,
)

# ============================================================================
# WRITE REPORT OUTPUT
# ============================================================================

with open(OUTPUT_REPORT, "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines) + "\n")

print(f"Report written to: {OUTPUT_REPORT}")
