#!/usr/bin/env python3
"""
Program  : EIBMICLS.py
Purpose  : Monthly Report on Reasons of Closed Current & Savings Accounts
           Breakdown by Products (Islamic) as at reporting date.
           REPORT ID: EIBMCLOS
"""

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR       = Path("/data")
MNITB_DIR      = BASE_DIR / "mnitb"
PRODEV_DIR     = BASE_DIR / "prodev"

REPTDATE_FILE  = MNITB_DIR / "reptdate.parquet"
CURRN_FILE     = PRODEV_DIR / "currn.parquet"
SAVNG_FILE     = PRODEV_DIR / "savng.parquet"

OUTPUT_DIR     = BASE_DIR / "output"
OUTPUT_REPORT  = OUTPUT_DIR / "eibmicls_report.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# REPORT / PAGE CONSTANTS
# ============================================================================

PAGE_LENGTH = 60
LINE_WIDTH  = 132

# ============================================================================
# FORMAT DICTIONARIES
# ============================================================================

TYPEC: dict[str, str] = {
    "01": "PLUS CURRENT",
    "02": "PLUS OTHER",
    "03": "ACE",
    "04": "PB CURRENTLINK",
    "05": "PB SHARELINK",
    "06": "WADIAH CURRENT-i",
    "07": "BASIC CA (90)",
    "08": "BASIC CA55 (91)",
    "09": "BASIC WADIAH CA-I (93)",
    "10": "MUDHARABAH CA-I (96)",
    "11": "STAFF MUDHARABAH CA-I (97)",
    "31": "PLUS SAVINGS",
    "32": "YOUNG ACHIEVER",
    "33": "50 PLUS",
    "34": "WISE",
    "35": "PB SAVELINK",
    "36": "WADIAH SAVINGS-i",
    "37": "BESTARI SAVINGS-i",
    "38": "UMA3 (297)    ",
    "39": "UMA3 (298)    ",
    "40": "BASIC SA (205)",
    "41": "BASIC SA55 (206)",
    "42": "PB BR STAR (208)",
    "44": "BASIC WADIAH SA (207)",
}

CODEC: dict[str, str] = {
    "B001": " B001 -DCHEQ                  ",
    "B002": " B002 -BANKRUPT               ",
    "B003": " B003 -INACTIVE               ",
    "B004": " B004 -DORMANT/UNCLAIMED MONEY",
    "B005": " B005 -BY OPERATION OF LAW    ",
    "B006": " B006 -OVERDRAWN/TOD          ",
    "B007": " B007 -INADEQUATE CUSTOMER DUE DILIGENCE(B)",
    "C001": " C001 -UNATTRACTIVE INTEREST/PROFIT RATES",
    "C002": " C002 -UNATTRACTIVE PRODUCT FEATURES/BENEFITS",
    "C003": " C003 -INCONVENIENT BRANCH LOCATION",
    "C004": " C004 -CHANGE OF EMPLOYMENT/PAYROLL ACCOUNT",
    "C005": " C005 -LOAN/FINANCING FULLY SETTLED",
    "C006": " C006 -CESSATION OF COMPANY'S BUSINESS",
    "C007": " C007 -SWITCHED TO ANOTHER PBB/PIBB BRANCH/ACCOUNT/PRODUCTS",
    "C008": " C008 -UNSATISFIED WITH CUSTOMER SERVICE",
    "C009": " C009 -INADEQUATE CUSTOMER DUE DILIGENCE(C)",
    "P001": " P001 -CLOSED BY PROCESSES",
}

CODES: dict[str, str] = {
    "B001": " B001 -BANKRUPT               ",
    "B002": " B002 -BY OPERATION OF LAW    ",
    "B003": " B003 -INACTIVE               ",
    "B004": " B004 -DORMANT/UNCLAIMED MONEY",
    "B005": " B005 -INADEQUATE CUSTOMER DUE DILIGENCE(B)",
    "C001": " C001 -UNATTRACTIVE INTEREST/PROFIT RATES",
    "C002": " C002 -UNATTRACTIVE PRODUCT FEATURES/BENEFITS",
    "C003": " C003 -INCONVENIENT BRANCH LOCATION",
    "C004": " C004 -CHANGE OF EMPLOYMENT/PAYROLL ACCOUNT",
    "C005": " C005 -LOAN/FINANCING FULLY SETTLED",
    "C006": " C006 -SWITCHED TO ANOTHER PBB/PIBB BRANCH/ACCOUNT/PRODUCTS",
    "C007": " C007 -UNSATISFIED WITH CUSTOMER SERVICE",
    "C008": " C008 -INADEQUATE CUSTOMER DUE DILIGENCE(C)",
    "P001": " P001 -CLOSED BY PROCESSES",
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def fmt_ddmmyy8(d: date) -> str:
    """Format date as DD/MM/YY (SAS DDMMYY8.)"""
    return d.strftime("%d/%m/%y")


def fmt_z2(n: int) -> str:
    return f"{n:02d}"


def fmt_z3(n: int) -> str:
    return f"{n:03d}"


def fmt_8(val) -> str:
    """Format as integer width 8 (SAS FORMAT=8.)"""
    if val is None:
        return "0".rjust(8)
    try:
        return str(int(val)).rjust(8)
    except (TypeError, ValueError):
        return "0".rjust(8)


def fmt_typec(val: str) -> str:
    return TYPEC.get(str(val).strip(), str(val).strip())


def fmt_codec(val: str) -> str:
    return CODEC.get(str(val).strip(), str(val).strip())


def fmt_codes(val: str) -> str:
    return CODES.get(str(val).strip(), str(val).strip())


# ============================================================================
# STEP 1: READ REPTDATE & DERIVE MACRO VARIABLES
# ============================================================================

con = duckdb.connect()

reptdate_df = con.execute(
    f"SELECT * FROM read_parquet('{REPTDATE_FILE}')"
).pl()
row_rep = reptdate_df.row(0, named=True)

REPTDATE: date = row_rep["REPTDATE"]
if isinstance(REPTDATE, datetime):
    REPTDATE = REPTDATE.date()

REPDD = fmt_z2(REPTDATE.day)
REPMM = fmt_z2(REPTDATE.month)
REPYY = str(REPTDATE.year)       # YEAR4.
REPDT = fmt_ddmmyy8(REPTDATE)

# ============================================================================
# STEP 2: BUILD DUMMY RCOTYP SCAFFOLDS
#
# DUMMYCA: I=1..11 (Islamic extends to 11 vs EIBMCCLS's 8)
#          J=1..7  -> 'B'||ZZZ(J)||ZZ(I)
#          K=1..9  -> 'C'||ZZZ(K)||ZZ(I)
# DUMMYSA: I=31..44 (Islamic extends to 44 vs EIBMCCLS's 42)
#          J=1..5  -> 'B'||...
#          K=1..8  -> 'C'||...
# DUMMYPA: J=1     -> 'P'||ZZZ(J)
# ============================================================================

def build_dummyca() -> list[str]:
    rows = []
    for i in range(1, 12):         # I = 1 TO 11
        for j in range(1, 8):      # J = 1 TO 7
            rows.append(f"B{fmt_z3(j)}{fmt_z2(i)}")
        for k in range(1, 10):     # K = 1 TO 9
            rows.append(f"C{fmt_z3(k)}{fmt_z2(i)}")
    return rows


def build_dummysa() -> list[str]:
    rows = []
    for i in range(31, 45):        # I = 31 TO 44
        for j in range(1, 6):      # J = 1 TO 5
            rows.append(f"B{fmt_z3(j)}{fmt_z2(i)}")
        for k in range(1, 9):      # K = 1 TO 8
            rows.append(f"C{fmt_z3(k)}{fmt_z2(i)}")
    return rows


def build_dummypa() -> list[str]:
    return [f"P{fmt_z3(1)}"]


dummyca = pl.DataFrame({"RCOTYP": build_dummyca()})
dummysa = pl.DataFrame({"RCOTYP": build_dummysa()})
dummypa = pl.DataFrame({"RCOTYP": build_dummypa()})

# ============================================================================
# STEP 3: PROCESS CURRN -> RCA (CURRENT ACCOUNTS)
# ============================================================================

currn_query = f"""
SELECT RCOTYP, CDATE, VC
FROM read_parquet('{CURRN_FILE}')
ORDER BY RCOTYP, CDATE
"""
currn = con.execute(currn_query).pl()


def accumulate_vc(df: pl.DataFrame, repyy: str, repmm: str,
                  yc_col: str, mc_col: str) -> pl.DataFrame:
    """
    Replicate SAS BY-group accumulation:
      - Filter CDATE year = REPYY
      - YC = sum of VC per RCOTYP for the year
      - MC = sum of VC per RCOTYP where CDATE month = REPMM
      - Output LAST.RCOTYP only (one row per RCOTYP)
    """
    df = df.filter(pl.col("CDATE").cast(pl.Utf8).str.slice(4, 4) == repyy)

    yc_df = (
        df.group_by("RCOTYP")
          .agg(pl.col("VC").sum().alias(yc_col))
    )
    mc_df = (
        df.filter(pl.col("CDATE").cast(pl.Utf8).str.slice(0, 2) == repmm)
          .group_by("RCOTYP")
          .agg(pl.col("VC").sum().alias(mc_col))
    )
    result = yc_df.join(mc_df, on="RCOTYP", how="left")
    result = result.with_columns(pl.col(mc_col).fill_null(0))
    return result


rca_raw = accumulate_vc(currn, REPYY, REPMM, "YC", "MC")

rca = dummyca.join(rca_raw, on="RCOTYP", how="left").with_columns([
    pl.col("YC").fill_null(0),
    pl.col("MC").fill_null(0),
])
rca = rca.with_columns([
    pl.col("RCOTYP").str.slice(4, 2).alias("TYPEX"),
    pl.col("RCOTYP").str.slice(0, 4).alias("RCODE"),
])
# Islamic current account products: 06, 09, 10, 11
rca = rca.filter(pl.col("TYPEX").is_in(["06", "09", "10", "11"]))

# ============================================================================
# STEP 4: PROCESS SAVNG -> RSA (SAVINGS ACCOUNTS)
# ============================================================================

savng_query = f"""
SELECT RCOTYP, CDATE, VS
FROM read_parquet('{SAVNG_FILE}')
ORDER BY RCOTYP, CDATE
"""
savng = con.execute(savng_query).pl()

savng_vc = savng.rename({"VS": "VC"})
rsa_raw  = accumulate_vc(savng_vc, REPYY, REPMM, "YS", "MS")
rsa_raw  = rsa_raw.rename({"YC": "YS", "MC": "MS"})

dummy_sa_pa = pl.concat([dummysa, dummypa], how="diagonal")
rsa = dummy_sa_pa.join(rsa_raw, on="RCOTYP", how="left").with_columns([
    pl.col("YS").fill_null(0),
    pl.col("MS").fill_null(0),
])
rsa = rsa.with_columns([
    pl.col("RCOTYP").str.slice(4, 2).alias("TYPEX"),
    pl.col("RCOTYP").str.slice(0, 4).alias("RCODE"),
])
# Islamic savings account products: 36, 37, 39, 44
rsa = rsa.filter(pl.col("TYPEX").is_in(["36", "37", "39", "44"]))

# ============================================================================
# STEP 5: REPORT RENDERING (shared tabulate logic)
# ============================================================================

def render_tabulate(
    df: pl.DataFrame,
    rcode_col: str,
    typex_col: str,
    mc_col: str,
    yc_col: str,
    rcode_fmt_fn,
    typex_fmt_fn,
    mc_label: str,
    yc_label: str,
    titles: list[str],
    lines: list[str],
) -> None:
    """
    Replicate PROC TABULATE:
      TABLE RCODE * (TYPEX * (MC YC) ALL * (MC YC))
    PRINTMISS / MISSTEXT='0': show 0 for missing cells.
    ASA '1' = form-feed, ' ' = advance one line.
    """

    typex_vals = sorted(df[typex_col].unique().to_list())
    rcode_vals = sorted(df[rcode_col].unique().to_list())

    # Build pivot: RCODE x TYPEX -> {mc, yc}
    pivot: dict[str, dict[str, dict[str, float]]] = {}
    for row in df.iter_rows(named=True):
        rc = str(row[rcode_col])
        tx = str(row[typex_col])
        mc = float(row.get(mc_col) or 0)
        yc = float(row.get(yc_col) or 0)
        pivot.setdefault(rc, {}).setdefault(tx, {"MC": 0.0, "YC": 0.0})
        pivot[rc][tx]["MC"] += mc
        pivot[rc][tx]["YC"] += yc

    col_width   = 9
    rcode_width = 35
    sep_line    = "-" * LINE_WIDTH

    def col_header_lines() -> list[str]:
        h1 = " " + "REASON DESCRIPTION".ljust(rcode_width)
        for tx in typex_vals:
            label = typex_fmt_fn(tx)[:16]
            h1 += f"  {label:>{col_width * 2 + 2}}"
        h1 += f"  {'TOTAL':>{col_width * 2 + 2}}"

        h2 = " " + " " * rcode_width
        for _ in typex_vals:
            h2 += f"  {mc_label:>{col_width}}  {yc_label:>{col_width}}"
        h2 += f"  {mc_label:>{col_width}}  {yc_label:>{col_width}}"

        return [h1, h2, " " + sep_line]

    def new_page() -> None:
        nonlocal line_count
        lines.append(
            f"1{'P U B L I C   B A N K   B E R H A D'.center(LINE_WIDTH)}"
        )
        for t in titles:
            lines.append(f" {t.center(LINE_WIDTH)}")
        lines.append(f" ")
        lines.extend(col_header_lines())
        line_count = 4 + len(titles) + 3

    line_count = PAGE_LENGTH  # force new page on first call

    grand_mc: dict[str, float] = {tx: 0.0 for tx in typex_vals}
    grand_yc: dict[str, float] = {tx: 0.0 for tx in typex_vals}
    grand_mc_tot = 0.0
    grand_yc_tot = 0.0

    for rc in rcode_vals:
        if line_count >= PAGE_LENGTH - 4:
            new_page()

        rc_label    = rcode_fmt_fn(rc)[:rcode_width].ljust(rcode_width)
        row_mc_tot  = 0.0
        row_yc_tot  = 0.0
        cells       = ""

        for tx in typex_vals:
            mc_val = pivot.get(rc, {}).get(tx, {}).get("MC", 0.0)
            yc_val = pivot.get(rc, {}).get(tx, {}).get("YC", 0.0)
            cells += f"  {fmt_8(mc_val)}  {fmt_8(yc_val)}"
            row_mc_tot += mc_val
            row_yc_tot += yc_val
            grand_mc[tx] = grand_mc.get(tx, 0.0) + mc_val
            grand_yc[tx] = grand_yc.get(tx, 0.0) + yc_val

        cells += f"  {fmt_8(row_mc_tot)}  {fmt_8(row_yc_tot)}"
        grand_mc_tot += row_mc_tot
        grand_yc_tot += row_yc_tot

        lines.append(f" {rc_label}{cells}")
        line_count += 1

    # TOTAL row
    if line_count >= PAGE_LENGTH - 4:
        new_page()

    lines.append(f" {sep_line}")
    total_label = "    TOTAL".ljust(rcode_width)
    total_cells = ""
    for tx in typex_vals:
        total_cells += f"  {fmt_8(grand_mc[tx])}  {fmt_8(grand_yc[tx])}"
    total_cells += f"  {fmt_8(grand_mc_tot)}  {fmt_8(grand_yc_tot)}"
    lines.append(f" {total_label}{total_cells}")
    line_count += 2


report_lines: list[str] = []

# ============================================================================
# REPORT 1: CURRENT ACCOUNTS (ISLAMIC)
# ============================================================================

render_tabulate(
    df=rca,
    rcode_col="RCODE",
    typex_col="TYPEX",
    mc_col="MC",
    yc_col="YC",
    rcode_fmt_fn=fmt_codec,
    typex_fmt_fn=fmt_typec,
    mc_label="CURRENT MONTH",
    yc_label="YEAR-TO-DATE",
    titles=[
        "-----------------------------------",
        f"MONTHLY REPORT ON REASONS OF CLOSED CURRENT ACCOUNTS "
        f"BREAKDOWN BY PRODUCTS AS AT: {REPDT}",
        "(ISLAMIC) ",
        "REPORT ID: EIBMCLOS",
        "(ATTN: MS. LIM POH LAN, PRODUCT DEV., MENARA PBB)",
        " ",
        "CATEGORY : B001-B007 - CLOSED BY BANK."
        "   C001-C009 - CLOSED BY CUSTOMER.",
    ],
    lines=report_lines,
)

# ============================================================================
# REPORT 2: SAVINGS ACCOUNTS (ISLAMIC)
# ============================================================================

render_tabulate(
    df=rsa,
    rcode_col="RCODE",
    typex_col="TYPEX",
    mc_col="MS",
    yc_col="YS",
    rcode_fmt_fn=fmt_codes,
    typex_fmt_fn=fmt_typec,
    mc_label="CURRENT MONTH",
    yc_label="YEAR-TO-DATE",
    titles=[
        "-----------------------------------",
        f"MONTHLY REPORT ON REASONS OF CLOSED SAVINGS ACCOUNTS "
        f"BREAKDOWN BY PRODUCTS AS AT: {REPDT}",
        "(ISLAMIC) ",
        "REPORT ID: EIBMCLOS",
        "(ATTN: MS. LIM POH LAN, PRODUCT DEV., MENARA PBB)",
        " ",
        "CATEGORY : B001-B005 - CLOSED BY BANK."
        "   C001-C008 - CLOSED BY CUSTOMER."
        "   P001 - CLOSED BY PROCESSES.",
    ],
    lines=report_lines,
)

# ============================================================================
# WRITE REPORT
# ============================================================================

with open(OUTPUT_REPORT, "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines) + "\n")

print(f"Report written to: {OUTPUT_REPORT}")
