# !/usr/bin/env python3
"""
PROGRAM  : LNICDR10.py
DATE     : 01.07.98
FUNCTION : DETAILS FOR NPL ACCOUNTS FOR CCD PFB.
           (A MONTHLY REPORT)
MODIFIED (ESMR) : 2006-1048
"""

import duckdb
import polars as pl
import os
from datetime import date, datetime

from PFBLNFMT import format_arrclass

# ─────────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────────
BASE_DIR   = r"C:/data"
BNM_DIR    = os.path.join(BASE_DIR, "bnm")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

BRHFILE_PATH = os.path.join(BASE_DIR, "brhfile", "BRHFILE.txt")
OUTFILE_PATH = os.path.join(OUTPUT_DIR, "X_LNICDR10.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# RDATE macro – supplied via environment variable or defaults to today
RDATE = os.environ.get("RDATE", date.today().strftime("%d/%m/%Y"))

con = duckdb.connect()

# ─────────────────────────────────────────────
# BNM.LOANTEMP – sorted BY BRANCH ARREAR2
# ─────────────────────────────────────────────
loantemp_path = os.path.join(BNM_DIR, "LOANTEMP.parquet")
loan_df = con.execute(f"""
    SELECT * FROM '{loantemp_path}'
    ORDER BY BRANCH, ARREAR2
""").pl()

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
                branch  = int(line[1:4].strip())  # @2, width 3
                brhcode = line[5:8]                # @6, width 3
                brhdata_rows.append({"BRANCH": branch, "BRHCODE": brhcode})
            except (ValueError, IndexError):
                continue

brhdata_df = (
    pl.DataFrame(brhdata_rows)
    if brhdata_rows
    else pl.DataFrame(schema={"BRANCH": pl.Int64, "BRHCODE": pl.Utf8})
)

# ─────────────────────────────────────────────
# Product range helpers
# ─────────────────────────────────────────────
def in_range(product, lo, hi) -> bool:
    return product is not None and lo <= product <= hi

# ─────────────────────────────────────────────
# LOAN1 – filter and assign CAT / TYPE
#
# THIS PART IS FOR HP DIRECT, IE PRODUCT CODE 700 & 705
# THIS PART IS FOR HOUSING LOAN AND FIXED LOAN PRODUCT CODE
#   (HOUSING/LOAN = 200 TO 299), &
#   (FIXED/LOAN = 300 TO 499, 504 TO 550, 555 TO 599, 900 TO 980)
# ─────────────────────────────────────────────
output_rows = []

for row in loan_df.iter_rows(named=True):
    balance  = row.get("BALANCE")  or 0.0
    borstat  = row.get("BORSTAT")  or ""
    arrear2  = row.get("ARREAR2")  or 0
    product  = row.get("PRODUCT")

    # Pre-filter
    if not (balance > 0 and borstat != "Z" and
            (arrear2 > 0 or borstat in ("R", "I", "F"))):
        continue

    # Product eligibility check
    if not (
        product in (110, 115) or
        product in (700, 705) or
        in_range(product, 200, 299) or
        in_range(product, 300, 499) or
        in_range(product, 504, 550) or
        in_range(product, 555, 599) or
        in_range(product, 900, 980) or
        product == 126 or
        product in (100, 101, 102, 105, 106, 120, 121, 127)
    ):
        continue

    # Compute derived fields shared across all output rows for this record
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

    def emit(cat, rtype):
        r               = dict(row)
        r["ARREAR2"]    = eff_arrear2
        r["ARREARS"]    = arrears
        r["BLDATE2"]    = bldate2
        r["CAT"]        = cat
        r["TYPE"]       = rtype
        output_rows.append(r)

    # /*
    # IF (PRODUCT = 700 OR PRODUCT = 705) AND ARREAR2 GT 6 THEN DO;
    #    CAT  = 'A';
    #    TYPE = 'OUTSTANDING LOANS CLASSIFIED AS NPL(HPD-C) AS AT : ';
    #    OUTPUT;
    # END;
    # */

    # /*
    # IF (PRODUCT = 110 OR PRODUCT = 115) AND ARREAR2 GT 6 THEN DO;
    #    CAT  = 'B';
    #    TYPE = 'OUTSTANDING LOANS CLASSIFIED AS NPL(AITAB) AS AT : ';
    #    OUTPUT;
    # END;
    # */

    if in_range(product, 200, 299) and arrear2 > 2:
        emit("D", "HOUSING LOAN (CONV) - 2 MTHS AND ABOVE AS AT : ")

    if ((in_range(product, 300, 499) or
         in_range(product, 504, 550) or
         in_range(product, 555, 599) or
         in_range(product, 900, 980)) and arrear2 > 2):
        emit("E", "FIXED LOAN (CONV) - 2 MTHS AND ABOVE AS AT : ")

    # /*
    # IF PRODUCT = 310 AND ARREAR2 GT 6 THEN DO;
    #    CAT  = 'F';
    #    TYPE = 'OUTSTANDING LOANS CLASSIFIED AS NPL(USL) AS AT : ';
    #    OUTPUT;
    # END;
    # */

    # /*
    # IF PRODUCT = 315 AND ARREAR2 GT 6 THEN DO;
    #    CAT  = 'G';
    #    TYPE = 'OUTSTANDING LOANS CLASSIFIED AS NPL(QCL/RSL) AS AT : ';
    #    OUTPUT;
    # END;
    # */

    if product == 330 and arrear2 > 6:
        emit("H", "OUTSTANDING LOANS CLASSIFIED AS NPL(PSIF/PSIL) AS AT : ")

    if product in (504, 505, 509):
        emit("I", "NEW PRINCIPAL GUARANTEE SCHEME (NPGS) AS AT : ")

    if product == 510:
        emit("J", "TABUNG USAHAWAN KECIL (TUK) AS AT : ")

    # /*
    # IF ((PRODUCT GE 200 AND PRODUCT LE 299) OR ...
    #        (PRODUCT GE 900 AND PRODUCT LE 980)) AND ARREAR2 GT 6 THEN DO;
    #    CAT  = 'K';
    #    TYPE = 'OUTSTANDING LOANS CLASSIFIED AS NPL(HL&FL) AS AT : ';
    #    OUTPUT;
    # END;
    # */

    if product in (100, 101, 102, 112, 113, 114, 116, 117, 118):
        emit("L", "OUTSTANDING ABBA HSE LOAN CLASSIFY AS NPL AS AT : ")

    if product in (105, 106, 120, 121, 126, 127):
        emit("M", "OUTSTANDING ABBA UNIT TRUST CLASSIFY AS NPL AS AT : ")

loan1_df = (
    pl.DataFrame(output_rows)
    if output_rows
    else pl.DataFrame(schema=loan_df.schema | {
        "ARREAR2":  pl.Int64,
        "ARREARS":  pl.Utf8,
        "BLDATE2":  pl.Utf8,
        "CAT":      pl.Utf8,
        "TYPE":     pl.Utf8,
    })
)

# ─────────────────────────────────────────────
# Merge LOAN1 ← BRHDATA  (left join by BRANCH)
# ─────────────────────────────────────────────
loan1_df = loan1_df.join(brhdata_df, on="BRANCH", how="left")

# Final sort: BY CAT BRANCH ARREAR2
loan1_df = loan1_df.sort(["CAT", "BRANCH", "ARREAR2"])

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

def make_buf(width: int = LINE_WIDTH) -> list:
    return [" "] * width

def place(text: str, col: int, buf: list):
    for i, ch in enumerate(str(text)):
        pos = col - 1 + i
        if 0 <= pos < len(buf):
            buf[pos] = ch

def render(buf: list) -> str:
    return "".join(buf)

SEP40  = "-" * 40
SEP10  = "-" * 10
EQ40   = "=" * 40

def sep_line() -> str:
    b = make_buf()
    place(SEP40, 41, b)
    place(SEP40, 81, b)
    place(SEP10, 121, b)
    return render(b)

def newpage(branch, rtype, rdate, pagecnt) -> int:
    """Emit page header. Returns reset LINECNT = 5."""
    b = make_buf()
    place(f"PROGRAM-ID : LNICDR10 - BRANCH : {branch:3d}", 1, b)
    place("P U B L I C   F I N A N C E   B E R H A D",    43, b)
    place(f"PAGE NO.: {pagecnt}",                          118, b)
    wr(render(b), ASA_PAGE)

    b = make_buf()
    place(f"{str(rtype):<53}"[:53], 41, b)
    place(rdate,                    94, b)
    wr(render(b))

    wr(" ")

    b = make_buf()
    place("BRH",       1,   b)
    place("ACCTNO",    5,   b)
    place("NAME",      16,  b)
    place("NOTENO",    41,  b)
    place("PRODUCT",   49,  b)
    place("BORSTATS",  58,  b)
    place("BILL DATE", 68,  b)
    place("DAYS",      78,  b)
    place("ARREARS",   84,  b)
    place("BALANCE",   110, b)
    wr(render(b))

    wr(sep_line())
    return 5


# ─────────────────────────────────────────────
# Main report loop  (DATA _NULL_ equivalent)
# ─────────────────────────────────────────────
TOTAL   = 0.0
BRHAMT  = 0.0
BRHARR  = 0.0
LINECNT = 5
PAGECNT = 0

prev_cat    = None
prev_branch = None
prev_arrear2 = None

rows   = loan1_df.to_dicts()
n_rows = len(rows)

for idx, row in enumerate(rows):
    cat      = row.get("CAT")      or ""
    branch   = row.get("BRANCH")
    arrear2  = row.get("ARREAR2")  or 0
    brhcode  = row.get("BRHCODE")  or ""
    acctno   = row.get("ACCTNO")   or ""
    name     = row.get("NAME")     or ""
    noteno   = row.get("NOTENO")   or ""
    product  = row.get("PRODUCT")
    borstat  = row.get("BORSTAT")  or ""
    bldate2  = row.get("BLDATE2")  or ""
    daydiff  = row.get("DAYDIFF")
    arrears  = row.get("ARREARS")  or ""
    balance  = row.get("BALANCE")  or 0.0
    rtype    = row.get("TYPE")     or ""

    first_cat     = (cat     != prev_cat)
    first_branch  = (branch  != prev_branch)  or first_cat
    first_arrear2 = (arrear2 != prev_arrear2) or first_branch

    is_last  = (idx == n_rows - 1)
    next_row = rows[idx + 1] if not is_last else None
    last_arrear2 = is_last or (next_row["ARREAR2"] != arrear2 or
                               next_row["BRANCH"]  != branch  or
                               next_row["CAT"]     != cat)
    last_branch  = is_last or (next_row["BRANCH"] != branch or
                               next_row["CAT"]    != cat)
    last_cat     = is_last or (next_row["CAT"] != cat)

    # ── FIRST.CAT ─────────────────────────────
    if first_cat:
        PAGECNT = 0
        BRHARR  = 0.0
        BRHAMT  = 0.0
        PAGECNT += 1
        LINECNT  = newpage(branch, rtype, RDATE, PAGECNT)

    # ── FIRST.BRANCH (but not FIRST.CAT) ──────
    elif first_branch:
        PAGECNT = 0
        BRHAMT  = 0.0
        PAGECNT += 1
        LINECNT  = newpage(branch, rtype, RDATE, PAGECNT)

    # ── FIRST.ARREAR2 ─────────────────────────
    if first_arrear2:
        BRHARR = 0.0

    # ── Detail line ───────────────────────────
    b = make_buf()
    place(f"{str(brhcode):<3}"[:3],     1,   b)
    place(str(acctno),                  5,   b)
    place(f"{str(name):<25}"[:25],      16,  b)
    place(str(noteno),                  41,  b)
    place(str(product) if product else "", 49, b)
    place(str(borstat),                 58,  b)
    place(str(bldate2),                 68,  b)
    place(fmt_comma(daydiff, 6),        78,  b)
    place(f"{str(arrears):<14}"[:14],   84,  b)
    place(fmt_comma(balance, 17, 2),    100, b)
    wr(render(b))

    LINECNT += 1
    BRHARR  += balance
    BRHAMT  += balance
    TOTAL   += balance

    if LINECNT > PAGE_LINES:
        PAGECNT += 1
        LINECNT  = newpage(branch, rtype, RDATE, PAGECNT)

    # ── LAST.ARREAR2 ──────────────────────────
    if last_arrear2:
        wr(sep_line())

        b = make_buf()
        place("SUBTOTAL",                  5,   b)
        place(fmt_comma(BRHARR, 17, 2),    100, b)
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
        place("BRANCH TOTAL",             5,   b)
        place(fmt_comma(BRHAMT, 17, 2),   100, b)
        wr(render(b))

        wr(sep_line())
        wr(" ")

    # ── LAST.CAT ──────────────────────────────
    if last_cat:
        wr(sep_line())

        b = make_buf()
        place("GRAND TOTAL",              5,   b)
        place(fmt_comma(TOTAL, 17, 2),    100, b)
        wr(render(b))

        wr(sep_line())
        wr(" ")
        TOTAL = 0.0

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
