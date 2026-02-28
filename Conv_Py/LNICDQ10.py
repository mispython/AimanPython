# !/usr/bin/env python3
"""
PROGRAM  : LNICDQ10.py
DATE     : 31.07.01
FUNCTION : DETAILS FOR NPL ACCOUNTS FOR CCD PFB.
           (A MONTHLY REPORT)
MODIFY   : 22 FEB 2006 ESMR 2005-610
"""

import duckdb
import polars as pl
import os
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

from PBBLNFMT import (
    HP_ALL,
    format_arrclass,
)

# ─────────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────────
BASE_DIR   = r"C:/data"
BNM_DIR    = os.path.join(BASE_DIR, "bnm")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

BRHFILE_PATH = os.path.join(BASE_DIR, "brhfile", "BRHFILE.txt")
OUTFILE_PATH = os.path.join(OUTPUT_DIR, "X_LNICDQ10.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

con = duckdb.connect()

# ─────────────────────────────────────────────
# &HPD macro – HP product list from PBBLNFMT
# ─────────────────────────────────────────────
HPD = set(HP_ALL)

# ─────────────────────────────────────────────
# REPTDATE – derive macro variables
# BNM.REPTDATE
# ─────────────────────────────────────────────
reptdate_path = os.path.join(BNM_DIR, "REPTDATE.parquet")
reptdate_df   = con.execute(f"SELECT REPTDATE FROM '{reptdate_path}'").pl()
reptdate_val  = reptdate_df["REPTDATE"][0]

if isinstance(reptdate_val, int):
    reptdate_val = date.fromordinal(reptdate_val)

# Previous month calculation
if reptdate_val.month == 1:
    pmth  = 12
    pyear = reptdate_val.year - 1
else:
    pmth  = reptdate_val.month - 1
    pyear = reptdate_val.year

# PDATE = MDY(PMTH,1,PYEAR)
pdate = date(pyear, pmth, 1)

PREPTDTE = pdate                                         # as date object for comparison
RDATE    = reptdate_val.strftime("%d/%m/%y")             # DDMMYY8.
REPTYEAR = str(reptdate_val.year)
REPTMON  = f"{reptdate_val.month:02d}"
REPTDAY  = f"{reptdate_val.day:02d}"

# ─────────────────────────────────────────────
# BNM.LOANTEMP – filter and sort BY BRANCH
#   WHERE BALANCE > 0 AND BORSTAT NE 'Z' AND PRODUCT IN &HPD
# ─────────────────────────────────────────────
loantemp_path = os.path.join(BNM_DIR, "LOANTEMP.parquet")
lntemp_df = con.execute(f"SELECT * FROM '{loantemp_path}'").pl()

lntemp_df = (
    lntemp_df
    .filter(
        (pl.col("BALANCE") > 0) &
        (pl.col("BORSTAT") != "Z") &
        (pl.col("PRODUCT").is_in(list(HPD)))
    )
    .sort("BRANCH")
)

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

# Merge LNTEMP ← BRHDATA (left join by BRANCH)
lntemp_df = lntemp_df.join(brhdata_df, on="BRANCH", how="left")

# ─────────────────────────────────────────────
# LOAN – filter for NPL conditions
#   ARREAR2 GE 3 OR BORSTAT IN (R,I,T,F,Y)
#   OR (ISSDTE GE &PREPTDTE AND DAYDIFF >= 8)
# ─────────────────────────────────────────────
def parse_issdte(val) -> date | None:
    """Parse ISSDTE stored as numeric MMDDYYYY or date."""
    if val is None:
        return None
    if isinstance(val, date):
        return val
    try:
        s = f"{int(val):011d}"[:8]
        return datetime.strptime(s, "%m%d%Y").date()
    except Exception:
        return None

issdte_list = [parse_issdte(v) for v in lntemp_df["ISSDTE"].to_list()]
lntemp_df   = lntemp_df.with_columns(pl.Series("ISSDTE_D", issdte_list, dtype=pl.Date))

loan_rows = []
for row in lntemp_df.iter_rows(named=True):
    arrear2  = row.get("ARREAR2") or 0
    borstat  = row.get("BORSTAT") or ""
    daydiff  = row.get("DAYDIFF") or 0
    issdte_d = row.get("ISSDTE_D")

    npl_condition   = (arrear2 >= 3 or borstat in ("R", "I", "T", "F", "Y"))
    new_ac_cond     = (issdte_d is not None and issdte_d >= PREPTDTE and daydiff >= 8)

    if npl_condition or new_ac_cond:
        loan_rows.append(row)

loan_df = (
    pl.DataFrame(loan_rows)
    if loan_rows
    else lntemp_df.clear()
)

# ─────────────────────────────────────────────
# LOAN1 – assign CAT / TYPE
# ─────────────────────────────────────────────
output_rows = []

for row in loan_df.iter_rows(named=True):
    borstat = row.get("BORSTAT") or ""
    arrear2 = row.get("ARREAR2") or 0
    product = row.get("PRODUCT")

    # IF BORSTAT = 'F' THEN ARREAR2 = 15
    eff_arrear2 = 15 if borstat == "F" else arrear2
    arrears     = format_arrclass(eff_arrear2)

    # BLDATE2 = PUT(BLDATE, MMDDYY8.)
    bldate_raw = row.get("BLDATE")
    try:
        if isinstance(bldate_raw, (int, float)):
            bldate2 = date.fromordinal(int(bldate_raw)).strftime("%m/%d/%y")
        elif isinstance(bldate_raw, date):
            bldate2 = bldate_raw.strftime("%m/%d/%y")
        else:
            bldate2 = str(bldate_raw or "")
    except Exception:
        bldate2 = ""

    # ISSDTE formatted DDMMYY8.
    issdte_d = row.get("ISSDTE_D")
    issdte_fmt = issdte_d.strftime("%d/%m/%y") if issdte_d else "        "

    def emit(cat, rtype):
        r               = dict(row)
        r["ARREAR2"]    = eff_arrear2
        r["ARREARS"]    = arrears
        r["BLDATE2"]    = bldate2
        r["ISSDTE_FMT"] = issdte_fmt
        r["CAT"]        = cat
        r["TYPE"]       = rtype
        output_rows.append(r)

    if product in (380, 381, 700, 705, 720, 725):
        emit("A", "HP DIRECT(CONV) ")

    if product in (380, 381):
        emit("B", "HP (380,381) ")

    if product in (128, 130, 131, 132):
        emit("C", "AITAB ")

loan1_df = (
    pl.DataFrame(output_rows)
    if output_rows
    else pl.DataFrame(schema=loan_df.schema | {
        "ARREAR2":    pl.Int64,
        "ARREARS":    pl.Utf8,
        "BLDATE2":    pl.Utf8,
        "ISSDTE_FMT": pl.Utf8,
        "CAT":        pl.Utf8,
        "TYPE":       pl.Utf8,
    })
)

# Re-merge LOAN1 ← BRHDATA by BRANCH (second merge in SAS)
loan1_df = (
    loan1_df
    .drop([c for c in ["BRHCODE"] if c in loan1_df.columns])
    .join(brhdata_df, on="BRANCH", how="left")
)

# Final sort: BY CAT BRANCH ARREAR2 DAYDIFF
loan1_df = loan1_df.sort(["CAT", "BRANCH", "ARREAR2", "DAYDIFF"])

# ─────────────────────────────────────────────
# Report helpers
# ─────────────────────────────────────────────
PAGE_LINES = 56
LINE_WIDTH  = 140

ASA_PAGE = "1"
ASA_NORM = " "

lines_out: list[str] = []

def wr(text: str = "", asa: str = ASA_NORM):
    lines_out.append(asa + text)

def fmt_comma(val, width: int, decimals: int = 0) -> str:
    if val is None:
        return " " * width
    try:
        s = f"{float(val):,.{decimals}f}" if decimals else f"{int(val):,}"
        return f"{s:>{width}}"[:width]
    except (TypeError, ValueError):
        return " " * width

def fmt_int(val, width: int) -> str:
    if val is None:
        return " " * width
    try:
        return f"{int(val):>{width}}"[:width]
    except (TypeError, ValueError):
        return " " * width

def make_buf(width: int = LINE_WIDTH) -> list:
    return [" "] * width

def place(text: str, col: int, buf: list):
    for i, ch in enumerate(str(text)):
        pos = col - 1 + i
        if 0 <= pos < len(buf):
            buf[pos] = ch

def render(buf: list) -> str:
    return "".join(buf)

SEP40 = "-" * 40
SEP10 = "-" * 10

def sep_line() -> str:
    b = make_buf()
    place(SEP40, 41, b)
    place(SEP40, 81, b)
    place(SEP10, 121, b)
    return render(b)

def newpage(branch, rtype, rdate, pagecnt) -> int:
    """Emit page header. Returns reset LINECNT = 6."""
    b = make_buf()
    place(f"PROGRAM-ID : LNICDQ10 - BRANCH : {branch:3d}", 1, b)
    place("P U B L I C   I S L A M I C   B A N K   B E R H A D", 43, b)
    place(f"PAGE NO.: {pagecnt}", 118, b)
    wr(render(b), ASA_PAGE)

    b = make_buf()
    place(f"{str(rtype):<38}"[:38], 39, b)
    place("2 MTHS & ABOVE AND NEW A/C WITH ARREAR ", 39 + len(str(rtype).rstrip()), b)
    place(f"AS AT {rdate}", 39 + len(str(rtype).rstrip()) + 39, b)
    wr(render(b))

    wr(" ")

    # Column headers line 1
    b = make_buf()
    place("BRH",       1,   b)
    place("ACCTNO",    5,   b)
    place("NAME",      16,  b)
    place("NOTENO",    40,  b)
    place("PRODUCT",   50,  b)
    place("BORSTAT",   59,  b)
    place("ISSUE DT",  68,  b)
    place("DAYS",      78,  b)
    place("ARREARS",   84,  b)
    place("BALANCE",   108, b)
    place("NO ISTL",   116, b)
    place("DELQ",      127, b)
    wr(render(b))

    # Column headers line 2
    b = make_buf()
    place(" ",          1,   b)
    place("LST TR DT",  5,   b)
    place("MAT. DATE",  16,  b)
    place("LST TR AMT", 36,  b)
    place("ISTL AMT",   49,  b)
    place("COLLATERAL DESCRIPTION", 69, b)
    place("   PAID",    116, b)
    place("CODE",       127, b)
    wr(render(b))

    # Column headers line 3
    b = make_buf()
    place("CAPCLOSE", 35, b)
    wr(render(b))

    wr(sep_line())
    return 6


# ─────────────────────────────────────────────
# Main report loop  (DATA _NULL_ equivalent)
# ─────────────────────────────────────────────
TOTAL    = 0.0
TOTALCAP = 0.0
BRHAMT   = 0.0
BRHCAP   = 0.0
BRHCAPB  = 0.0
BRHARR   = 0.0
LINECNT  = 6
PAGECNT  = 0

prev_cat     = None
prev_branch  = None
prev_arrear2 = None

rows   = loan1_df.to_dicts()
n_rows = len(rows)

for idx, row in enumerate(rows):
    cat       = row.get("CAT")       or ""
    branch    = row.get("BRANCH")
    arrear2   = row.get("ARREAR2")   or 0
    brhcode   = row.get("BRHCODE")   or ""
    acctno    = row.get("ACCTNO")    or ""
    name      = row.get("NAME")      or ""
    noteno    = row.get("NOTENO")    or ""
    product   = row.get("PRODUCT")
    borstat   = row.get("BORSTAT")   or ""
    issdte_f  = row.get("ISSDTE_FMT") or ""
    daydiff   = row.get("DAYDIFF")
    arrears   = row.get("ARREARS")   or ""
    balance   = row.get("BALANCE")   or 0.0
    noistlpd  = row.get("NOISTLPD")
    delqcd    = row.get("DELQCD")    or ""
    lastran   = row.get("LASTRAN")
    maturdt   = row.get("MATURDT")
    lsttrnam  = row.get("LSTTRNAM")
    payamt    = row.get("PAYAMT")
    colldesc  = row.get("COLLDESC")  or ""
    cap       = row.get("CAP")       or 0.0
    rtype     = row.get("TYPE")      or ""

    first_cat     = (cat     != prev_cat)
    first_branch  = (branch  != prev_branch)  or first_cat
    first_arrear2 = (arrear2 != prev_arrear2) or first_branch

    is_last   = (idx == n_rows - 1)
    next_row  = rows[idx + 1] if not is_last else None
    last_arrear2 = is_last or (next_row["ARREAR2"] != arrear2 or
                               next_row["BRANCH"]  != branch  or
                               next_row["CAT"]     != cat)
    last_branch  = is_last or (next_row["BRANCH"] != branch or
                               next_row["CAT"]    != cat)
    last_cat     = is_last or (next_row["CAT"] != cat)

    # ── FIRST.CAT ─────────────────────────────
    if first_cat:
        PAGECNT  = 0
        BRHARR   = 0.0
        BRHAMT   = 0.0
        BRHCAP   = 0.0
        BRHCAPB  = 0.0
        PAGECNT += 1
        LINECNT  = newpage(branch, rtype, RDATE, PAGECNT)

    # ── FIRST.BRANCH (but not FIRST.CAT) ──────
    elif first_branch:
        PAGECNT  = 0
        BRHAMT   = 0.0
        BRHCAPB  = 0.0
        PAGECNT += 1
        LINECNT  = newpage(branch, rtype, RDATE, PAGECNT)

    # ── FIRST.ARREAR2 ─────────────────────────
    if first_arrear2:
        BRHARR = 0.0
        BRHCAP = 0.0

    # ── Helper: format date fields ─────────────
    def fmt_date_ddmmyy(val) -> str:
        if val is None:
            return "        "
        if isinstance(val, date):
            return val.strftime("%d/%m/%y")
        try:
            return date.fromordinal(int(val)).strftime("%d/%m/%y")
        except Exception:
            return "        "

    # ── Detail line 1 ─────────────────────────
    #   @1 BRHCODE @5 ACCTNO @16 NAME @41 NOTENO @54 PRODUCT
    #   @59 BORSTAT @68 ISSDTE DDMMYY8. @79 DAYDIFF @84 ARREARS
    #   @98 BALANCE COMMA17.2 @116 NOISTLPD COMMA7. @127 DELQCD $5.
    b = make_buf()
    place(f"{str(brhcode):<3}"[:3],     1,   b)
    place(str(acctno),                  5,   b)
    place(f"{str(name):<25}"[:25],      16,  b)
    place(str(noteno),                  41,  b)
    place(str(product) if product else "", 54, b)
    place(str(borstat),                 59,  b)
    place(issdte_f,                     68,  b)
    place(fmt_int(daydiff, 5),          79,  b)
    place(f"{str(arrears):<14}"[:14],   84,  b)
    place(fmt_comma(balance, 17, 2),    98,  b)
    place(fmt_comma(noistlpd, 7),       116, b)
    place(f"{str(delqcd):<5}"[:5],      127, b)
    wr(render(b))

    # ── Detail line 2 ─────────────────────────
    #   @5 LASTRAN DDMMYY8. @16 MATURDT DDMMYY8.
    #   @29 LSTTRNAM COMMA17.2 @46 PAYAMT COMMA11.2
    #   @59 COLLDESC
    b = make_buf()
    place(fmt_date_ddmmyy(lastran),     5,  b)
    place(fmt_date_ddmmyy(maturdt),     16, b)
    place(fmt_comma(lsttrnam, 17, 2),   29, b)
    place(fmt_comma(payamt, 11, 2),     46, b)
    place(str(colldesc),                59, b)
    wr(render(b))

    # ── Detail line 3 ─────────────────────────
    #   @35 CAP COMMA16.2
    b = make_buf()
    place(fmt_comma(cap, 16, 2), 35, b)
    wr(render(b))

    # ── Blank spacer line ─────────────────────
    wr(" ")

    LINECNT  += 4
    BRHARR   += balance
    BRHAMT   += balance
    BRHCAP   += cap
    BRHCAPB  += cap
    TOTALCAP += cap
    TOTAL    += balance

    if LINECNT > PAGE_LINES:
        PAGECNT += 1
        LINECNT  = newpage(branch, rtype, RDATE, PAGECNT)

    # ── LAST.ARREAR2 ──────────────────────────
    if last_arrear2:
        wr(sep_line())

        b = make_buf()
        place("SUBTOTAL",              5,   b)
        place(fmt_comma(BRHCAP, 16, 2), 35,  b)
        place(fmt_comma(BRHARR, 17, 2), 100, b)
        wr(render(b))

        wr(sep_line())
        wr(" ")
        LINECNT += 4

    if LINECNT > PAGE_LINES:
        PAGECNT += 1
        LINECNT  = newpage(branch, rtype, RDATE, PAGECNT)

    # ── LAST.BRANCH ───────────────────────────
    if last_branch:
        wr(sep_line())

        b = make_buf()
        place("BRANCH TOTAL",           5,   b)
        place(fmt_comma(BRHCAPB, 16, 2), 35,  b)
        place(fmt_comma(BRHAMT, 17, 2),  100, b)
        wr(render(b))

        wr(sep_line())
        wr(" ")

    # ── LAST.CAT ──────────────────────────────
    if last_cat:
        wr(sep_line())

        b = make_buf()
        place("GRAND TOTAL",             5,   b)
        place(fmt_comma(TOTALCAP, 16, 2), 35,  b)
        place(fmt_comma(TOTAL, 17, 2),    100, b)
        wr(render(b))

        wr(sep_line())
        wr(" ")
        TOTAL    = 0.0
        TOTALCAP = 0.0

    prev_cat     = cat
    prev_branch  = branch
    prev_arrear2 = arrear2

# ─────────────────────────────────────────────
# Write OUTFILE output
# ─────────────────────────────────────────────
with open(OUTFILE_PATH, "w", encoding="utf-8") as f:
    for line in lines_out:
        f.write(line + "\n")

# PROC DATASETS LIB=WORK NOLIST; DELETE LOAN1; RUN;
# (loan1_df goes out of scope; no explicit cleanup needed in Python)

print(f"Report written to : {OUTFILE_PATH}")
print(f"Total records     : {n_rows}")
