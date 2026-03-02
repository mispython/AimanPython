# !/usr/bin/env python3
"""
PROGRAM  : LNICDN10.py
FUNCTION : DETAILS FOR BORSTAT F/I/R & (S & NOTENO=98010) HP ONLY
           (A MONTHLY REPORT)
MODIFY (ESMR) : 2006-1048
"""

import duckdb
import polars as pl
import os
from datetime import date, datetime

from PBBLNFMT import (
    HP_ALL,
    AITAB,
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
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

BRHFILE_PATH = os.path.join(BASE_DIR, "brhfile", "BRHFILE.txt")
OUTHP_PATH   = os.path.join(OUTPUT_DIR, "X_LNICDN10.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

con = duckdb.connect()

# ─────────────────────────────────────────────
# &HPD macro – HP product list
# In the original SAS, &HPD is defined via
# %LET HPD = ... in PBBLNFMT.  We use HP_ALL
# from PBBLNFMT.py as the equivalent set.
# ─────────────────────────────────────────────
HPD = set(HP_ALL)

# ─────────────────────────────────────────────
# RDATE macro – must be supplied externally;
# defaulting to today when not set.
# ─────────────────────────────────────────────
RDATE = os.environ.get("RDATE", date.today().strftime("%d/%m/%Y"))

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
    pl.DataFrame(brhdata_rows)
    if brhdata_rows
    else pl.DataFrame(schema={"BRANCH": pl.Int64, "BRHCODE": pl.Utf8})
)

# ─────────────────────────────────────────────
# LOANTEMP  –  BNM.LOANTEMP
# ─────────────────────────────────────────────
loantemp_path = os.path.join(BNM_DIR, "LOANTEMP.parquet")
loantemp_df   = con.execute(f"SELECT * FROM '{loantemp_path}'").pl()

# CENSUS9 = SUBSTR(PUT(CENSUS,8.2),7,1)
# PUT(x,8.2) formats x as a fixed 8-char field with 2 decimal places;
# character position 7 (1-based) = index 6 (0-based).
def census9_val(census) -> str:
    if census is None:
        return " "
    try:
        s = f"{float(census):8.2f}"   # 8-wide, 2 decimal places
        return s[6] if len(s) >= 7 else " "
    except (TypeError, ValueError):
        return " "

census9_list = [census9_val(v) for v in loantemp_df["CENSUS"].to_list()]
loantemp_df  = loantemp_df.with_columns(pl.Series("CENSUS9", census9_list))

# ─────────────────────────────────────────────
# Build LOANTEM2 – filter and assign CAT/TYPE
# ─────────────────────────────────────────────
output_rows = []

for row in loantemp_df.iter_rows(named=True):
    arrear   = row.get("ARREAR")  or 0
    borstat  = row.get("BORSTAT") or ""
    product  = row.get("PRODUCT")
    census9  = row.get("CENSUS9") or " "
    noteno   = row.get("NOTENO")  or 0

    in_hpd = product in HPD

    if arrear > 4 or borstat in ("R", "I", "F") or census9 == "9":
        if in_hpd and borstat == "F":
            r         = dict(row)
            r["CAT"]  = "A"
            r["TYPE"] = "(DEFICIT)"
            output_rows.append(r)

        if in_hpd and borstat == "I":
            r         = dict(row)
            r["CAT"]  = "B"
            r["TYPE"] = "(IRREGULAR)"
            output_rows.append(r)

        if in_hpd and borstat == "R":
            r         = dict(row)
            r["CAT"]  = "C"
            r["TYPE"] = "(REPOSSESSED)"
            output_rows.append(r)

        # /*
        # IF PRODUCT IN &HPD AND BORSTAT = 'T' THEN DO;
        #    CAT  = 'D';
        #    TYPE = '(TOTAL LOSS INSURANCE CLAMIN)';
        #    OUTPUT;
        # END;
        # */

    if in_hpd and borstat == "S" and noteno >= 98010:
        r         = dict(row)
        r["CAT"]  = "E"
        r["TYPE"] = "RESTRUCTURED"
        output_rows.append(r)

loantem2_df = (
    pl.DataFrame(output_rows)
    if output_rows
    else pl.DataFrame(schema=loantemp_df.schema | {"CAT": pl.Utf8, "TYPE": pl.Utf8})
)

# ─────────────────────────────────────────────
# Merge LOANTEM2 ← BRHDATA  (left join by BRANCH)
# ─────────────────────────────────────────────
loantem2_df = loantem2_df.sort("BRANCH")
loantem2_df = loantem2_df.join(brhdata_df, on="BRANCH", how="left")

# ─────────────────────────────────────────────
# Merge with LOAN.LNNOTE
#   KEEP=ACCTNO NOTENO ISSUEDT ORGBAL CURBAL
# ─────────────────────────────────────────────
lnnote_path = os.path.join(LOAN_DIR, "LNNOTE.parquet")
loan_df = con.execute(f"""
    SELECT ACCTNO, NOTENO, ISSUEDT, ORGBAL, CURBAL
    FROM '{lnnote_path}'
""").pl()

loantem2_df = loantem2_df.join(loan_df, on=["ACCTNO", "NOTENO"], how="left")

# ISTLPAID = ORGBAL - CURBAL
loantem2_df = loantem2_df.with_columns(
    (pl.col("ORGBAL").fill_null(0.0) - pl.col("CURBAL").fill_null(0.0))
    .alias("ISTLPAID")
)

# ISSDTE = INPUT(SUBSTR(PUT(ISSUEDT,Z11.),1,8),MMDDYY8.)
# ISSUEDT is stored as a numeric MMDDYYYY integer (Z11. → zero-pad to 11 digits,
# take first 8 chars = MMDDYYYY, then parse as MMDDYY8.)
def parse_issdte(issuedt) -> date | None:
    if issuedt is None:
        return None
    try:
        s = f"{int(issuedt):011d}"[:8]   # first 8 chars of zero-padded value
        return datetime.strptime(s, "%m%d%Y").date()
    except (ValueError, TypeError):
        return None

issdte_list = [parse_issdte(v) for v in loantem2_df["ISSUEDT"].to_list()]
loantem2_df = loantem2_df.with_columns(pl.Series("ISSDTE", issdte_list, dtype=pl.Date))

# CAC  = PUT(BRANCH, CACNAME.)
# BRABBR = PUT(BRANCH, BRCHCD.)
cac_list   = [format_cacname(int(b)) if b is not None else "" for b in loantem2_df["BRANCH"].to_list()]
brabbr_list = [format_brchcd(int(b))  if b is not None else "" for b in loantem2_df["BRANCH"].to_list()]
loantem2_df = loantem2_df.with_columns([
    pl.Series("CAC",    cac_list),
    pl.Series("BRABBR", brabbr_list),
])

# Final sort: BY BRANCH CAT ACCTNO
loantem2_df = loantem2_df.sort(["BRANCH", "CAT", "ACCTNO"])

# ─────────────────────────────────────────────
# Report helpers
# ─────────────────────────────────────────────
PAGE_LINES = 56          # LINECNT > 56 triggers new page
LINE_WIDTH = 120

ASA_PAGE  = "1"
ASA_NORM  = " "

lines_out: list[str] = []

def wr(text: str = "", asa: str = ASA_NORM):
    lines_out.append(asa + text)

def fmt_comma(val, width: int, decimals: int = 0) -> str:
    """Right-justify comma-formatted number."""
    if val is None:
        return " " * width
    try:
        if decimals:
            s = f"{float(val):,.{decimals}f}"
        else:
            s = f"{int(val):,}"
        return f"{s:>{width}}"[:width]
    except (TypeError, ValueError):
        return " " * width

def fmt_ddmmyy(dt) -> str:
    """Format date as DD/MM/YY (DDMMYY8.)."""
    if dt is None:
        return " " * 8
    try:
        if isinstance(dt, (int, float)):
            dt = date.fromordinal(int(dt))
        return dt.strftime("%d/%m/%y")
    except Exception:
        return " " * 8

def make_buf(width: int = LINE_WIDTH) -> list:
    return [" "] * width

def place(text: str, col: int, buf: list):
    for i, ch in enumerate(str(text)):
        pos = col - 1 + i
        if 0 <= pos < len(buf):
            buf[pos] = ch

def render(buf: list) -> str:
    return "".join(buf)

SEP_LINE = "-" * 40 + " " * 40 + "-" * 40

def newpage(branch, brabbr, page_type, rdate, pagecnt) -> int:
    """Emit page header; return reset LINECNT=7."""
    b = make_buf()
    place(f"PUBLIC   BANK    BERHAD - BRANCH : {branch:3d}", 1,  b)
    place(f"({brabbr:<3})",                                   40, b)
    place(f"PAGE NO.: {pagecnt}",                             110, b)
    wr(render(b), ASA_PAGE)

    b = make_buf()
    place("DETAIL OF ACCOUNTS BY BRANCH - ", 1, b)
    place(f"{page_type:<23} AS AT {rdate}", 32, b)
    wr(render(b))

    wr("PROGRAM-ID : LNICDN10")
    wr(" ")

    b = make_buf()
    place("NOTE",           13, b)
    place("  ISSUE",        51, b)
    place("  BILL",         60, b)
    place("DAYS IN",        70, b)
    wr(render(b))

    b = make_buf()
    place("ACCTNO",   1,  b)
    place("  NO",     13, b)
    place("NAME",     18, b)
    place("PRODUCT",  43, b)
    place("  DATE",   51, b)
    place("  DATE",   60, b)
    place("ARREARS",  70, b)
    place("BALANCE",  87, b)
    place("INSTALMENT PAID", 99, b)
    wr(render(b))

    wr("-" * 40 + "-" * 40 + "-" * 40)
    return 7

# ─────────────────────────────────────────────
# Main report loop  (DATA _NULL_ equivalent)
# ─────────────────────────────────────────────
TOTBRBAL = 0.0
TOTBRPD  = 0.0
TOTBRAC  = 0

BRBAL   = 0.0
BRPD    = 0.0
BRAC    = 0
LINECNT = 7
PAGECNT = 0

prev_branch = None
prev_cat    = None

rows = loantem2_df.to_dicts()
n_rows = len(rows)

for idx, row in enumerate(rows):
    branch  = row.get("BRANCH")
    cat     = row.get("CAT")    or ""
    rtype   = row.get("TYPE")   or ""
    acctno  = row.get("ACCTNO") or ""
    noteno  = row.get("NOTENO") or ""
    name    = row.get("NAME")   or ""
    product = row.get("PRODUCT")
    issdte  = row.get("ISSDTE")
    bldate  = row.get("BLDATE")
    daydiff = row.get("DAYDIFF")
    balance = row.get("BALANCE") or 0.0
    istlpaid = row.get("ISTLPAID") or 0.0
    brabbr   = row.get("BRABBR") or ""

    first_branch = (branch != prev_branch)
    first_cat    = (cat != prev_cat) or first_branch

    is_last = (idx == n_rows - 1)
    next_row = rows[idx + 1] if not is_last else None
    last_cat    = is_last or (next_row["CAT"] != cat or next_row["BRANCH"] != branch)
    last_branch = is_last or (next_row["BRANCH"] != branch)

    # ── FIRST.BRANCH ──────────────────────────
    if first_branch:
        PAGECNT = 0
        BRBAL   = 0.0
        BRPD    = 0.0
        BRAC    = 0
        PAGECNT += 1
        LINECNT  = newpage(branch, brabbr, rtype, RDATE, PAGECNT)

    # ── FIRST.CAT (but not FIRST.BRANCH – already paged) ──
    elif first_cat:
        PAGECNT = 0
        BRBAL   = 0.0
        BRPD    = 0.0
        BRAC    = 0
        PAGECNT += 1
        LINECNT  = newpage(branch, brabbr, rtype, RDATE, PAGECNT)

    # ── Detail line ───────────────────────────
    b = make_buf()
    place(str(acctno),                    1,   b)
    place(str(noteno),                    12,  b)
    place(f"{str(name):<24}"[:24],        18,  b)
    place(str(product) if product else "", 45, b)
    place(fmt_ddmmyy(issdte),             51,  b)
    place(fmt_ddmmyy(bldate),             60,  b)
    place(fmt_comma(daydiff, 5),          70,  b)
    place(fmt_comma(balance, 14, 2),      80,  b)
    place(fmt_comma(istlpaid, 14, 2),     100, b)
    wr(render(b))

    LINECNT  += 1
    BRBAL    += balance
    BRPD     += istlpaid
    BRAC     += 1
    TOTBRBAL += balance
    TOTBRPD  += istlpaid
    TOTBRAC  += 1

    if LINECNT > PAGE_LINES:
        PAGECNT += 1
        LINECNT  = newpage(branch, brabbr, rtype, RDATE, PAGECNT)

    # ── LAST.CAT ──────────────────────────────
    if last_cat:
        wr("-" * 40 + "-" * 40 + "-" * 40)

        b = make_buf()
        place("CATEGORY TOTAL",                  1,  b)
        place("NO OF A/C : ",                    29, b)
        place(fmt_comma(BRAC, 10),               38, b)
        place(fmt_comma(BRBAL, 14, 2),           80, b)
        place(fmt_comma(BRPD, 14, 2),            100, b)
        wr(render(b))

        wr("-" * 40 + "-" * 40 + "-" * 40)

        BRBAL = 0.0
        BRPD  = 0.0
        BRAC  = 0

    # ── LAST.BRANCH ───────────────────────────
    if last_branch:
        wr(" ")

        b = make_buf()
        place("BRANCH TOTAL",                    1,  b)
        place("NO OF A/C : ",                    29, b)
        place(fmt_comma(TOTBRAC, 10),            38, b)
        place(fmt_comma(TOTBRBAL, 14, 2),        80, b)
        place(fmt_comma(TOTBRPD, 14, 2),         100, b)
        wr(render(b))

        wr("=" * 40 + "=" * 40 + "=" * 40)

        TOTBRBAL = 0.0
        TOTBRPD  = 0.0
        TOTBRAC  = 0

    prev_branch = branch
    prev_cat    = cat

# ─────────────────────────────────────────────
# Write OUTHP output file
# ─────────────────────────────────────────────
with open(OUTHP_PATH, "w", encoding="utf-8") as f:
    for line in lines_out:
        f.write(line + "\n")

print(f"Report written to : {OUTHP_PATH}")
print(f"Total records     : {n_rows}")
