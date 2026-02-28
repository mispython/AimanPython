# !/usr/bin/env python3
"""
PROGRAM  : LNICDW10.py
FUNCTION : DETAILS FOR WRITTEN-OFF A/CS FOR 983 & 993
           HP ONLY
           (A MONTHLY REPORT)
"""

import duckdb
import polars as pl
import os
from datetime import date
from collections import defaultdict

from PBBLNFMT import (
    HP_ALL,
)
from PBBELF import (
    format_brchcd,
    format_cacname,
)

# ─────────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────────
BASE_DIR   = r"C:/data"
BNM_DIR    = os.path.join(BASE_DIR, "bnm")
LOAN_DIR   = os.path.join(BASE_DIR, "loan")
WOFF_DIR   = os.path.join(BASE_DIR, "woff")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

BRHFILE_PATH  = os.path.join(BASE_DIR, "brhfile", "BRHFILE.txt")
OUTWOFF_PATH  = os.path.join(OUTPUT_DIR, "X_LNICDW10.txt")
PRINT_PATH    = os.path.join(OUTPUT_DIR, "X_LNICDW10_NONCAC.txt")
SUMMARY_PATH  = os.path.join(OUTPUT_DIR, "X_LNICDW10_SUMMARY.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# RDATE macro – supplied via environment variable or defaults to today
RDATE = os.environ.get("RDATE", date.today().strftime("%d/%m/%Y"))

con = duckdb.connect()

# ─────────────────────────────────────────────
# BRHDATA  –  INFILE BRHFILE LRECL=80
#   @2  BRANCH  3.
#   @6  BRHCODE $3.
# ─────────────────────────────────────────────
brhdata_rows = []
if os.path.exists(BRHFILE_PATH):
    with open(BRHFILE_PATH, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n").ljust(80)
            try:
                branch  = int(line[1:4].strip())   # @2, width 3
                brhcode = line[5:8]                 # @6, width 3
                brhdata_rows.append({"BRANCH": branch, "BRHCODE": brhcode})
            except (ValueError, IndexError):
                continue

brhdata_df = (
    pl.DataFrame(brhdata_rows).sort("BRANCH")
    if brhdata_rows
    else pl.DataFrame(schema={"BRANCH": pl.Int64, "BRHCODE": pl.Utf8})
)

# +---------------------------------------------------------------+
# |  THIS PART IS FOR HP PRODUCT CODE 983 & 993                   |
# +---------------------------------------------------------------+

# ─────────────────────────────────────────────
# LOAN1 – BNM.LOANTEMP filtered for products 983,993,678,679,698,699
# ─────────────────────────────────────────────
loantemp_path = os.path.join(BNM_DIR, "LOANTEMP.parquet")
loantemp_df   = con.execute(f"SELECT * FROM '{loantemp_path}'").pl()

loan1_df = loantemp_df.filter(
    pl.col("PRODUCT").is_in([983, 993, 678, 679, 698, 699]) &
    (pl.col("BORSTAT") != "E")
)

# CATGY classification
def get_catgy(borstat: str) -> str:
    mapping = {
        "F": "DEFICIT",
        "I": "IRREGULAR",
        "R": "REPO",
        "T": "TOT LOSS",
        "E": "B.STATUS=E",
    }
    return mapping.get(borstat, "NON REPO")

catgy_list = [get_catgy(str(v or "")) for v in loan1_df["BORSTAT"].to_list()]
loan1_df   = loan1_df.with_columns(pl.Series("CATGY", catgy_list))

# * BALANCE = SUM(BALANCE, FEEDUE);  -- commented out in original, not applied

# Sort BY BRANCH then merge BRHDATA
loan1_df = loan1_df.sort("BRANCH")
loan1_df = loan1_df.join(brhdata_df, on="BRANCH", how="left")

# ─────────────────────────────────────────────
# Merge LOAN1 ← LOAN.LNNOTE  (KEEP=ACCTNO NOTENO MODELDES)
# ─────────────────────────────────────────────
lnnote_path = os.path.join(LOAN_DIR, "LNNOTE.parquet")
lnnote_df   = con.execute(f"""
    SELECT ACCTNO, NOTENO, MODELDES FROM '{lnnote_path}'
""").pl()

loan1_df = (
    loan1_df
    .sort(["ACCTNO", "NOTENO"])
    .join(lnnote_df.sort(["ACCTNO", "NOTENO"]), on=["ACCTNO", "NOTENO"], how="left")
)

# ─────────────────────────────────────────────
# MERGER WITH HARD CODE DATA
# Merge LOAN1 ← WOFF.HPWOFF  (sorted BY ACCTNO)
# ─────────────────────────────────────────────
hpwoff_path = os.path.join(WOFF_DIR, "HPWOFF.parquet")
hpwoff_df   = con.execute(f"SELECT * FROM '{hpwoff_path}'").pl().sort("ACCTNO")

loan1_df = (
    loan1_df
    .sort("ACCTNO")
    .join(hpwoff_df, on="ACCTNO", how="left")
)

# TOTWOFF = SUM(SPALLOW, IIS)
# IF CAPBAL > 0 THEN TOTWOFF = CAPBAL;  /* WEF Q3-2014 REPLACE IIS/SP */
def compute_totwoff(spallow, iis, capbal) -> float:
    spallow = float(spallow or 0.0)
    iis     = float(iis     or 0.0)
    capbal  = float(capbal  or 0.0)
    totwoff = spallow + iis
    if capbal > 0:
        totwoff = capbal
    return totwoff

totwoff_list = [
    compute_totwoff(r.get("SPALLOW"), r.get("IIS"), r.get("CAPBAL"))
    for r in loan1_df.iter_rows(named=True)
]
loan1_df = loan1_df.with_columns(pl.Series("TOTWOFF", totwoff_list, dtype=pl.Float64))

# CAC = PUT(BRANCH, CACNAME.)
# BRABBR = PUT(BRANCH, BRCHCD.)
cac_list    = [format_cacname(int(b)) if b is not None else "" for b in loan1_df["BRANCH"].to_list()]
brabbr_list = [format_brchcd(int(b))  if b is not None else "" for b in loan1_df["BRANCH"].to_list()]
loan1_df = loan1_df.with_columns([
    pl.Series("CAC",    cac_list),
    pl.Series("BRABBR", brabbr_list),
])

# ─────────────────────────────────────────────
# Report helpers – shared by all output sections
# ─────────────────────────────────────────────
PAGE_LINES = 56
LINE_WIDTH  = 140

ASA_PAGE = "1"
ASA_NORM = " "

def wr_to(lines_list: list, text: str = "", asa: str = ASA_NORM):
    lines_list.append(asa + text)

def fmt_comma(val, width: int, decimals: int = 0) -> str:
    if val is None:
        return " " * width
    try:
        s = f"{float(val):,.{decimals}f}" if decimals else f"{int(val):,}"
        return f"{s:>{width}}"[:width]
    except (TypeError, ValueError):
        return " " * width

def fmt_plain(val, width: int, decimals: int = 0) -> str:
    """Right-justified fixed-decimal without commas (SAS 12.2 / 10.2 format)."""
    if val is None:
        return " " * width
    try:
        s = f"{float(val):.{decimals}f}" if decimals else f"{int(val)}"
        return f"{s:>{width}}"[:width]
    except (TypeError, ValueError):
        return " " * width

def fmt_date_ddmmyy(val) -> str:
    if val is None:
        return "        "
    if isinstance(val, date):
        return val.strftime("%d/%m/%y")
    try:
        return date.fromordinal(int(val)).strftime("%d/%m/%y")
    except Exception:
        return "        "

def make_buf(width: int = LINE_WIDTH) -> list:
    return [" "] * width

def place(text: str, col: int, buf: list):
    for i, ch in enumerate(str(text)):
        pos = col - 1 + i
        if 0 <= pos < len(buf):
            buf[pos] = ch

def render(buf: list) -> str:
    return "".join(buf)

SEP91 = "-" * 91

def sep91_line() -> str:
    b = make_buf()
    place(SEP91, 41, b)
    return render(b)

SEP134 = "-" * 134

def sep134_line() -> str:
    return " " + SEP134

def newpage_detail(branch, brabbr, catgy, rdate, pagecnt, lines_out) -> int:
    """
    Emit page header for detail reports (OUTWOFF / PRINT).
    Returns reset LINECNT = 6.
    """
    b = make_buf()
    place(f"PROGRAM-ID : LNICDW10 - BRANCH : {branch:3d}", 1, b)
    place(f"({str(brabbr):<3})", 38, b)
    place("P U B L I C   B A N K   B E R H A D", 45, b)
    place(f"PAGE NO.: {pagecnt}", 110, b)
    wr_to(lines_out, render(b), ASA_PAGE)

    b = make_buf()
    place("WRITTEN OFF A/C FOR HP(983,993,678,679,698,699) ", 40, b)
    place(f"{str(catgy):<10}"[:10], 89, b)
    place(f" AS AT {rdate}", 99, b)
    wr_to(lines_out, render(b))

    wr_to(lines_out, " ")

    # Header line 1
    b = make_buf()
    place("NOTE                           ISSUE",      12, b)
    place("WRITTEN   SPECIFIC  INT.IN     TOT.WOFF",  71, b)
    place("LASTTRAN TRN  AKPK",                       115, b)
    wr_to(lines_out, render(b))

    # Header line 2
    b = make_buf()
    place("ACCTNO",      1,   b)
    place("NUM   NAME",  12,  b)
    place("DATE",        44,  b)
    place("DAYS    BALANCE",  52,  b)
    place("-OFF DT   ALLOW(A)", 71, b)
    place("SUSP.(B)   (A+B) / CAP  DATE     CDE   TAG", 91, b)
    wr_to(lines_out, render(b))

    wr_to(lines_out, sep134_line())
    return 6


# ─────────────────────────────────────────────
# %MACRO PRNRPT – parameterised report renderer
# Shared by OUTWOFF (all branches) and PRINT (non-CAC only)
# ─────────────────────────────────────────────
def render_detail_report(data_df: pl.DataFrame, rdate: str) -> list[str]:
    """
    Render the detail report equivalent to the %PRNRPT macro.
    Returns list of ASA output lines.
    """
    lines_out: list[str] = []

    TOTBRBAL = 0.0
    TOTBRSA  = 0.0
    TOTBRIIS = 0.0
    TOTBRWOF = 0.0
    TOTBRAC  = 0

    BRBAL   = 0.0
    BRSA    = 0.0
    BRIIS   = 0.0
    BRWOFF  = 0.0
    BRAC    = 0
    LINECNT = 6
    PAGECNT = 0

    prev_branch = None
    prev_catgy  = None

    rows   = data_df.to_dicts()
    n_rows = len(rows)

    for idx, row in enumerate(rows):
        branch   = row.get("BRANCH")
        catgy    = row.get("CATGY")    or ""
        acctno   = row.get("ACCTNO")   or ""
        noteno   = row.get("NOTENO")   or ""
        name     = row.get("NAME")     or ""
        issdte   = row.get("ISSDTE")
        daydiff  = row.get("DAYDIFF")
        balance  = row.get("BALANCE")  or 0.0
        woffdt   = row.get("WOFFDT")
        spallow  = row.get("SPALLOW")  or 0.0
        iis      = row.get("IIS")      or 0.0
        totwoff  = row.get("TOTWOFF")  or 0.0
        lastran  = row.get("LASTRAN")
        lsttrncd = row.get("LSTTRNCD") or ""
        modeldes = row.get("MODELDES") or ""
        brabbr   = row.get("BRABBR")   or ""

        first_branch = (branch != prev_branch)
        first_catgy  = (catgy  != prev_catgy) or first_branch

        is_last  = (idx == n_rows - 1)
        next_row = rows[idx + 1] if not is_last else None
        last_catgy   = is_last or (next_row["CATGY"]  != catgy  or
                                   next_row["BRANCH"] != branch)
        last_branch  = is_last or (next_row["BRANCH"] != branch)

        # ── FIRST.BRANCH ──────────────────────
        if first_branch:
            PAGECNT = 0
            BRBAL   = 0.0
            BRSA    = 0.0
            BRIIS   = 0.0
            BRWOFF  = 0.0
            BRAC    = 0
            PAGECNT += 1
            LINECNT  = newpage_detail(branch, brabbr, catgy, rdate, PAGECNT, lines_out)

        # ── FIRST.CATGY (not FIRST.BRANCH) ────
        elif first_catgy:
            PAGECNT = 0
            BRBAL   = 0.0
            BRSA    = 0.0
            BRIIS   = 0.0
            BRWOFF  = 0.0
            BRAC    = 0
            PAGECNT += 1
            LINECNT  = newpage_detail(branch, brabbr, catgy, rdate, PAGECNT, lines_out)

        # ── Detail line ───────────────────────
        # PUT @1 ACCTNO @12 NOTENO @18 NAME $24.
        #     @43 ISSDTE DDMMYY8. @52 DAYDIFF 5.
        #     @58 BALANCE 12.2 @71 WOFFDT @80 SPALLOW 10.2
        #     @91 IIS 10.2 @102 TOTWOFF 12.2
        #     @115 LASTRAN DDMMYY8. @124 LSTTRNCD $3. @128 MODELDES $6.
        b = make_buf()
        place(str(acctno),                    1,   b)
        place(str(noteno),                    12,  b)
        place(f"{str(name):<24}"[:24],        18,  b)
        place(fmt_date_ddmmyy(issdte),        43,  b)
        place(f"{int(daydiff or 0):>5}",      52,  b)
        place(fmt_plain(balance, 12, 2),      58,  b)
        place(str(woffdt or ""),              71,  b)
        place(fmt_plain(spallow, 10, 2),      80,  b)
        place(fmt_plain(iis, 10, 2),          91,  b)
        place(fmt_plain(totwoff, 12, 2),      102, b)
        place(fmt_date_ddmmyy(lastran),       115, b)
        place(f"{str(lsttrncd):<3}"[:3],      124, b)
        place(f"{str(modeldes):<6}"[:6],      128, b)
        wr_to(lines_out, render(b))

        LINECNT  += 1
        BRBAL    += balance
        BRSA     += spallow
        BRIIS    += iis
        BRWOFF   += totwoff
        BRAC     += 1
        TOTBRBAL += balance
        TOTBRSA  += spallow
        TOTBRIIS += iis
        TOTBRWOF += totwoff
        TOTBRAC  += 1

        if LINECNT > PAGE_LINES:
            PAGECNT += 1
            LINECNT  = newpage_detail(branch, brabbr, catgy, rdate, PAGECNT, lines_out)

        # ── LAST.CATGY ────────────────────────
        if last_catgy:
            wr_to(lines_out, sep91_line())

            b = make_buf()
            place("CATEGORY TOTAL",           1,   b)
            place("NO OF A/C : ",             29,  b)
            place(f"{BRAC:>8}",              43,  b)
            place(fmt_plain(BRBAL,  12, 2),   59,  b)
            place(fmt_plain(BRSA,   10, 2),   82,  b)
            place(fmt_plain(BRIIS,  10, 2),   93,  b)
            place(fmt_plain(BRWOFF, 12, 2),   107, b)
            wr_to(lines_out, render(b))

            wr_to(lines_out, sep91_line())
            wr_to(lines_out, " ")

        # ── LAST.BRANCH ───────────────────────
        if last_branch:
            b = make_buf()
            place("BRANCH TOTAL",              1,   b)
            place("NO OF A/C : ",              29,  b)
            place(f"{TOTBRAC:>10}",           41,  b)
            place(fmt_plain(TOTBRBAL, 12, 2),  59,  b)
            place(fmt_plain(TOTBRSA,  10, 2),  82,  b)
            place(fmt_plain(TOTBRIIS, 10, 2),  93,  b)
            place(fmt_plain(TOTBRWOF, 12, 2),  107, b)
            wr_to(lines_out, render(b))

            wr_to(lines_out, sep91_line())
            wr_to(lines_out, " ")

            TOTBRBAL = 0.0
            TOTBRSA  = 0.0
            TOTBRIIS = 0.0
            TOTBRWOF = 0.0
            TOTBRAC  = 0

        prev_branch = branch
        prev_catgy  = catgy

    return lines_out


# ─────────────────────────────────────────────
# OUTPUT FOR RPS – all branches
# PROC SORT BY BRANCH CATGY DESCENDING BALANCE
# ─────────────────────────────────────────────
loan2_df = loan1_df.sort(
    ["BRANCH", "CATGY", "BALANCE"],
    descending=[False, False, True]
)

outwoff_lines = render_detail_report(loan2_df, RDATE)

with open(OUTWOFF_PATH, "w", encoding="utf-8") as f:
    for line in outwoff_lines:
        f.write(line + "\n")

print(f"Detail report (all branches) written to : {OUTWOFF_PATH}")

# ─────────────────────────────────────────────
# DETAIL REPORT FOR NON CAC BRANCH
# WHERE SUBSTR(CAC,1,3) = 'NON'
# ─────────────────────────────────────────────
noncac_df = loan1_df.filter(
    pl.col("CAC").str.slice(0, 3) == "NON"
).sort(
    ["BRANCH", "CATGY", "BALANCE"],
    descending=[False, False, True]
)

print_lines = render_detail_report(noncac_df, RDATE)

with open(PRINT_PATH, "w", encoding="utf-8") as f:
    for line in print_lines:
        f.write(line + "\n")

print(f"Detail report (non-CAC branches) written to : {PRINT_PATH}")

# ─────────────────────────────────────────────
# SUMMARY REPORT  (PROC TABULATE equivalent)
# BY CAC
# CLASS BRABBR CATGY
# VAR BALANCE
# TABLE BRABBR ALL='TOTAL',
#       (CATGY ALL='TOTAL') * (BALANCE) * (N SUM)
# BOX='BRANCH' RTS=7
# ─────────────────────────────────────────────
loan2_sum_df = loan1_df.sort("CAC")

summary_lines: list[str] = []

def wr_sum(text: str = "", asa: str = ASA_NORM):
    summary_lines.append(asa + text)

# Collect unique CAC values preserving order
cac_values = loan2_sum_df["CAC"].unique(maintain_order=True).to_list()

for cac_val in cac_values:
    cac_data = loan2_sum_df.filter(pl.col("CAC") == cac_val)

    # Titles
    wr_sum("REPORT NO : LNICDW10", ASA_PAGE)
    wr_sum("PUBLIC BANK BERHAD")
    wr_sum(
        "SUMMARY ON WRITTEN OFF A/C FOR HP(983,993,678,679,698,699)"
        f" AS AT {RDATE}"
    )
    wr_sum(" ")

    # Collect all CATGY and BRABBR values
    catgy_vals  = sorted(cac_data["CATGY"].unique().to_list())
    brabbr_vals = sorted(cac_data["BRABBR"].unique().to_list())

    # Build aggregation: group by BRABBR x CATGY
    agg_df = (
        cac_data
        .group_by(["BRABBR", "CATGY"])
        .agg([
            pl.col("BALANCE").count().alias("N"),
            pl.col("BALANCE").sum().alias("SUM"),
        ])
    )

    # Build lookup dict: (brabbr, catgy) → (n, sum)
    cell: dict = {}
    for r in agg_df.iter_rows(named=True):
        cell[(r["BRABBR"], r["CATGY"])] = (r["N"], r["SUM"])

    # Column width
    COL_W  = 22    # each CATGY sub-block (N=6 + SUM=15 + sep)
    RTS    = 7     # row title space (BOX='BRANCH')
    N_W    = 6
    SUM_W  = 15

    # ── Header ────────────────────────────────
    # Row 1: CATGY names + TOTAL
    col_headers = catgy_vals + ["TOTAL"]
    header_row1 = f"{'BRANCH':<{RTS}}"
    for ch in col_headers:
        header_row1 += f" {ch:^{COL_W - 1}}"
    wr_sum(header_row1)

    # Row 2: NO.OF A/C | AMOUNT(RM) sub-headers
    header_row2 = " " * RTS
    for _ in col_headers:
        header_row2 += f" {'NO.OF A/C':>{N_W}} {'AMOUNT(RM)':>{SUM_W - 1}}"
    wr_sum(header_row2)

    sep_hdr = "-" * (RTS + len(col_headers) * COL_W)
    wr_sum(sep_hdr)

    # ── Data rows (per BRABBR) ────────────────
    total_n:   dict[str, int]   = defaultdict(int)
    total_sum: dict[str, float] = defaultdict(float)
    grand_n   = 0
    grand_sum = 0.0

    for brabbr in brabbr_vals:
        row_n   = 0
        row_sum = 0.0
        data_row = f"{str(brabbr):<{RTS}}"
        for catgy in catgy_vals:
            n, s = cell.get((brabbr, catgy), (0, 0.0))
            data_row += f" {fmt_comma(n, N_W)} {fmt_comma(s, SUM_W - 1, 2)}"
            row_n    += n
            row_sum  += s
            total_n[catgy]   += n
            total_sum[catgy] += s
        # TOTAL column
        data_row += f" {fmt_comma(row_n, N_W)} {fmt_comma(row_sum, SUM_W - 1, 2)}"
        grand_n   += row_n
        grand_sum += row_sum
        wr_sum(data_row)

    wr_sum(sep_hdr)

    # ── TOTAL row ─────────────────────────────
    total_row = f"{'TOTAL':<{RTS}}"
    for catgy in catgy_vals:
        total_row += f" {fmt_comma(total_n[catgy], N_W)} {fmt_comma(total_sum[catgy], SUM_W - 1, 2)}"
    total_row += f" {fmt_comma(grand_n, N_W)} {fmt_comma(grand_sum, SUM_W - 1, 2)}"
    wr_sum(total_row)

    wr_sum(sep_hdr)
    wr_sum(" ")

with open(SUMMARY_PATH, "w", encoding="utf-8") as f:
    for line in summary_lines:
        f.write(line + "\n")

print(f"Summary report written to : {SUMMARY_PATH}")

# PROC DATASETS LIB=WORK NOLIST; DELETE LOAN1 LOAN2; RUN;
# (DataFrames go out of scope; no explicit cleanup needed in Python)

print(f"Total records processed : {loan1_df.height}")
