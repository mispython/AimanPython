# !/usr/bin/env python3
"""
Program: EIIFTXT4
Purpose: ESMR 2013-705 (SEND FILE TO LNS) - 2ND EXTRACTION CAP
         Merge WOFFTXT with ICAP data, write fixed-width output file,
            and produce a listing report of accounts for bad debt writing-off.
         Source: NPL (PIBB) library – uses ICAP instead of CAP
"""

import duckdb
import polars as pl
import os
from datetime import date

# ─────────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────────
BASE_DIR    = r"C:\data"
LOAN_DIR    = os.path.join(BASE_DIR, "loan")
NPL_DIR     = os.path.join(BASE_DIR, "npl")        # SAP.PIBB.NPL.HP.SASDATA
OUTPUT_DIR  = os.path.join(BASE_DIR, "output")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# SAP.PIBB.FTPLNS.WOFFTXT  (WOFFTEXT DD)
WOFFTEXT_PATH = os.path.join(OUTPUT_DIR, "EIIFTXT4_wofftxt.txt")
# PROC PRINT report output
REPORT_PATH   = os.path.join(OUTPUT_DIR, "EIIFTXT4_report.txt")

con = duckdb.connect()

# ─────────────────────────────────────────────
# REPTDATE – derive macro variables
# ─────────────────────────────────────────────
reptdate_df = con.execute(
    f"SELECT REPTDATE FROM read_parquet('{LOAN_DIR}/reptdate.parquet') LIMIT 1"
).pl()

reptdate_val: date = reptdate_df["REPTDATE"][0]
day_val   = reptdate_val.day
month_val = reptdate_val.month
year_val  = reptdate_val.year

# WK / WK1 selection
if   day_val ==  8:  wk, wk1 = "1", "4"
elif day_val == 15:  wk, wk1 = "2", "1"
elif day_val == 22:  wk, wk1 = "3", "2"
else:                wk, wk1 = "4", "3"

mm  = month_val
mm1 = mm - 1 if mm > 1 else 12

NOWK     = wk
NOWKS    = "4"
NOWK1    = wk1
REPTMON  = f"{mm:02d}"
REPTMON1 = f"{mm1:02d}"
REPTYEAR = f"{year_val % 100:02d}"
RDATE    = f"{day_val:02d}/{month_val:02d}/{year_val % 100:02d}"

# ─────────────────────────────────────────────
# Load NPL.WOFFTXT
# Dependency: upstream process produces npl/wofftxt.parquet
# ─────────────────────────────────────────────
wofftxt_parquet = os.path.join(NPL_DIR, "wofftxt.parquet")
wofftxt_df = con.execute(
    f"SELECT * FROM read_parquet('{wofftxt_parquet}')"
).pl().sort(["ACCTNO", "NOTENO"])

# ─────────────────────────────────────────────
# Load NPL.ICAP{REPTMON}{REPTYEAR} – deduplicate by ACCTNO+NOTENO
# Dependency: upstream PIBB CAP process → produces npl/icap{REPTMON}{REPTYEAR}.parquet
# ─────────────────────────────────────────────
icap_parquet = os.path.join(NPL_DIR, f"icap{REPTMON}{REPTYEAR}.parquet")
icap_df = (
    con.execute(
        f"SELECT ACCTNO, NOTENO, CAP FROM read_parquet('{icap_parquet}')"
    ).pl()
    .unique(subset=["ACCTNO", "NOTENO"], keep="first")
    .sort(["ACCTNO", "NOTENO"])
)

# ─────────────────────────────────────────────
# Merge: WOFFTXT (left) + ICAP, DROP CAP from WOFFTXT before merge
# IF BORSTAT = 'A' THEN CAP = TOTAL
# ─────────────────────────────────────────────
# Drop existing CAP column from WOFFTXT if present
if "CAP" in wofftxt_df.columns:
    wofftxt_df = wofftxt_df.drop("CAP")

text_df = (
    wofftxt_df
    .join(icap_df, on=["ACCTNO", "NOTENO"], how="left")
    .with_columns([
        pl.when(pl.col("BORSTAT") == "A")
          .then(pl.col("TOTAL"))
          .otherwise(pl.col("CAP"))
          .alias("CAP"),
    ])
)

# ─────────────────────────────────────────────
# Fixed-width output helpers
# OPTIONS MISSING=' '  → missing values rendered as spaces
# ─────────────────────────────────────────────
def _str(val, width: int) -> str:
    """Left-justified string, space-padded, missing → spaces."""
    if val is None:
        return " " * width
    return str(val)[:width].ljust(width)


def _num(val, width: int, dec: int = 0) -> str:
    """Right-justified numeric. Missing → spaces."""
    if val is None:
        return " " * width
    try:
        if dec:
            return f"{float(val):{width}.{dec}f}"
        else:
            return f"{int(val):{width}d}"
    except Exception:
        return " " * width


def _date_mmddyy10(val) -> str:
    """Format date as MM/DD/YYYY (MMDDYY10.)."""
    if val is None:
        return " " * 10
    try:
        if isinstance(val, str):
            return val[:10].ljust(10)
        d = val
        return f"{d.month:02d}/{d.day:02d}/{d.year:04d}"
    except Exception:
        return " " * 10


def build_woff_record(row: dict) -> str:
    """
    Build an 800-character fixed-width record matching the SAS PUT layout.
    Field positions are 1-indexed; record width = 800 chars.
    """
    rec = [" "] * 800

    def place(col1: int, text: str) -> None:
        idx = col1 - 1
        for i, ch in enumerate(text):
            pos = idx + i
            if pos < len(rec):
                rec[pos] = ch

    place(1,   _str(row.get("BRANCH"),      7))    # @1   BRANCH   $7.
    place(9,   _str(row.get("NAME"),        40))    # @9   NAME     $40.
    place(50,  _num(row.get("ACCTNO"),      10))    # @50  ACCTNO   10.
    place(61,  _num(row.get("NOTENO"),       5))    # @61  NOTENO   5.
    place(67,  _str(row.get("BORSTAT"),      1))    # @67  BORSTAT  $1.
    place(69,  _num(row.get("IIS"),         16, 2)) # @69  IIS      16.2
    place(85,  _num(row.get("OI"),          16, 2)) # @85  OI       16.2
    place(101, _num(row.get("TOTIIS"),      16, 2)) # @101 TOTIIS   16.2
    place(117, _num(row.get("SP"),          16, 2)) # @117 SP       16.2
    place(133, _num(row.get("TOTAL"),       16, 2)) # @133 TOTAL    16.2
    place(149, _num(row.get("CURBAL"),      16, 2)) # @149 CURBAL   16.2
    place(165, _num(row.get("PREVBAL"),     16, 2)) # @165 PREVBAL  16.2
    place(181, _num(row.get("PAYMENT"),     16, 2)) # @181 PAYMENT  16.2
    place(197, _num(row.get("ECSRRSRV"),    16, 2)) # @197 ECSRRSRV 16.2
    place(213, _num(row.get("POSTAMT"),     16, 2)) # @213 POSTAMT  16.2
    place(229, _num(row.get("OTHERAMT"),    16, 2)) # @229 OTHERAMT 16.2
    place(245, _str(row.get("MATDATE"),     10))    # @245 MATDATE  $10.
    place(255, _num(row.get("LOANTYPE"),     3))    # @255 LOANTYPE 3.
    place(258, _num(row.get("INTAMT"),      16, 2)) # @258 INTAMT   16.2
    place(274, _str(row.get("POSTNTRN"),     1))    # @274 POSTNTRN $1.
    place(278, _num(row.get("MARKETVL"),    16, 2)) # @278 MARKETVL 16.2
    place(294, _num(row.get("INTEARN4"),    16, 2)) # @294 INTEARN4 16.2
    place(310, _num(row.get("DAYS"),         6))    # @310 DAYS      6.
    place(317, _num(row.get("CUSTCODE"),     3))    # @317 CUSTCODE  3.
    place(321, _str(row.get("RIND"),         1))    # @321 RIND     $1.
    place(322, _num(row.get("OIFEEAMT"),    16, 2)) # @322 OIFEEAMT 16.2
    place(339, _str(row.get("LASTTRA1"),    10))    # @339 LASTTRA1 $10.
    place(350, _num(row.get("LSTTRNCD"),     3))    # @350 LSTTRNCD 3.
    place(354, _num(row.get("MTHPDUE"),      3))    # @354 MTHPDUE  3.
    place(357, _num(row.get("BALANCE"),     16, 2)) # @357 BALANCE  16.2
    place(374, _num(row.get("CAP"),         16, 2)) # @374 CAP      16.2
    place(391, _num(row.get("LATECHG"),     16, 2)) # @391 LATECHG  16.2
    place(408, _str(row.get("GUAREND"),     20))    # @408 GUAREND  $20.
    place(429, _str(row.get("GUARNAM1"),    40))    # @429 GUARNAM1 $40.
    place(470, _str(row.get("GUARNAM2"),    40))    # @470 GUARNAM2 $40.
    place(511, _date_mmddyy10(row.get("ISSXDTE")))  # @511 ISSXDTE  MMDDYY10.
    place(522, _num(row.get("NETPROC"),     16, 2)) # @522 NETPROC  16.2
    place(539, _str(row.get("COLLDESC"),    70))    # @539 COLLDESC $70.
    place(610, _num(row.get("COLLYEAR"),     4))    # @610 COLLYEAR 4.
    place(615, _num(row.get("BILPAID"),      3))    # @615 BILPAID  3.
    place(619, _str(row.get("CRRGRADE"),     5))    # @619 CRRGRADE $5.
    place(625, _num(row.get("MARGINFI"),    16, 2)) # @625 MARGINFI 16.2
    place(642, _num(row.get("NOTETERM"),     3))    # @642 NOTETERM 3.
    place(646, _num(row.get("PAYAMT"),      16, 2)) # @646 PAYAMT   16.2
    place(663, _date_mmddyy10(row.get("DOBMNI")))   # @663 DOBMNI   MMDDYY10.
    place(674, _str(row.get("ECSRIND"),      1))    # @674 ECSRIND  $1.
    place(677, _str(row.get("DELQCD"),       2))    # @677 DELQCD   $2.
    place(680, _str(row.get("DELQDES"),     30))    # @680 DELQDES  $30.
    place(711, _str(row.get("BIZTYPE"),      1))    # @711 BIZTYPE  $1.
    place(713, _str(row.get("OCCUPAT"),      3))    # @713 OCCUPAT  $3.
    place(717, _str(row.get("OCCUPDES"),    25))    # @717 OCCUPDES $25.
    place(743, _str(row.get("BGC"),          2))    # @743 BGC      $2.
    place(746, _str(row.get("BGCDES"),      20))    # @746 BGCDES   $20.
    place(767, _str(row.get("PAY75PCT"),     1))    # @767 PAY75PCT $1.
    place(769, _str(row.get("NACODATE"),    10))    # @769 NACODATE $10.
    place(780, _str(row.get("CP"),           1))    # @780 CP       $1.
    place(782, _str(row.get("MODELDES"),     6))    # @782 MODELDES    $6.
    place(789, _str(row.get("AKPK_STATUS"),  9))    # @789 AKPK_STATUS $9.

    return "".join(rec)


# ─────────────────────────────────────────────
# Write WOFFTEXT fixed-width output file
# DELETE step: remove stale file first
# ─────────────────────────────────────────────
if os.path.exists(WOFFTEXT_PATH):
    os.remove(WOFFTEXT_PATH)

woff_rows = []
with open(WOFFTEXT_PATH, "w", encoding="utf-8") as fout:
    for row in text_df.iter_rows(named=True):
        rec = build_woff_record(row)
        fout.write(rec + "\n")
        woff_rows.append(row)

woff_df = pl.DataFrame(woff_rows) if woff_rows else text_df.clone()

# ─────────────────────────────────────────────
# PROC PRINT equivalent – listing report
# VAR BRANCH NAME ACCTNO NOTENO BORSTAT IIS OI TOTIIS SP TOTAL CURBAL
#     PREVBAL PAYMENT MATDATE LOANTYPE INTAMT POSTNTRN
#     MARKETVL INTEARN4 DAYS LASTTRA1 LSTTRNCD MTHPDUE BALANCE
# SUM IIS OI TOTIIS SP TOTAL CURBAL PREVBAL PAYMENT BALANCE
# TITLE1 'LISTING OF ACCOUNTS FOR BAD DEBT WRITING-OFF EXERCISE'
# TITLE2 'AS AT ' "&RDATE"
# ─────────────────────────────────────────────
PRINT_VARS = [
    "BRANCH", "NAME", "ACCTNO", "NOTENO", "BORSTAT",
    "IIS", "OI", "TOTIIS", "SP", "TOTAL", "CURBAL",
    "PREVBAL", "PAYMENT", "MATDATE", "LOANTYPE", "INTAMT", "POSTNTRN",
    "MARKETVL", "INTEARN4", "DAYS", "LASTTRA1", "LSTTRNCD", "MTHPDUE", "BALANCE",
]
SUM_VARS = ["IIS", "OI", "TOTIIS", "SP", "TOTAL", "CURBAL", "PREVBAL", "PAYMENT", "BALANCE"]

PAGE_LEN  = 60
PAGE_WIDTH = 200

COL_WIDTHS = {
    "BRANCH": 7, "NAME": 20, "ACCTNO": 10, "NOTENO": 5, "BORSTAT": 7,
    "IIS": 14, "OI": 14, "TOTIIS": 14, "SP": 14, "TOTAL": 14,
    "CURBAL": 14, "PREVBAL": 14, "PAYMENT": 14, "MATDATE": 10,
    "LOANTYPE": 8, "INTAMT": 14, "POSTNTRN": 9, "MARKETVL": 14,
    "INTEARN4": 14, "DAYS": 6, "LASTTRA1": 10, "LSTTRNCD": 8,
    "MTHPDUE": 7, "BALANCE": 14,
}


def fmt_report_val(val, col: str) -> str:
    w = COL_WIDTHS.get(col, 10)
    if val is None:
        return " " * w
    if col in SUM_VARS or col in ("DAYS", "LOANTYPE", "LSTTRNCD", "MTHPDUE"):
        try:
            if col in SUM_VARS or col in ("MARKETVL", "INTEARN4", "INTAMT"):
                return f"{float(val):>{w},.2f}"
            return f"{float(val):>{w},.0f}"
        except Exception:
            return str(val)[:w].rjust(w)
    return str(val)[:w].ljust(w)


output_lines: list[str] = []
line_count = 0


def emit(cc: str, content: str = "") -> None:
    global line_count
    output_lines.append(cc + content)
    line_count += 1
    if line_count >= PAGE_LEN:
        line_count = 0


def emit_page_header() -> None:
    global line_count
    emit("1", "LISTING OF ACCOUNTS FOR BAD DEBT WRITING-OFF EXERCISE")
    emit(" ", f"AS AT {RDATE}")
    emit(" ", "")
    hdr = "  ".join(c.ljust(COL_WIDTHS.get(c, 10)) for c in PRINT_VARS)
    emit(" ", hdr)
    emit(" ", "-" * len(hdr))
    line_count = 5


emit_page_header()

sums = {v: 0.0 for v in SUM_VARS}

for row in woff_df.iter_rows(named=True):
    if line_count >= PAGE_LEN - 2:
        emit_page_header()

    parts = []
    for col in PRINT_VARS:
        val = row.get(col)
        parts.append(fmt_report_val(val, col))
    emit(" ", "  ".join(parts))

    for sv in SUM_VARS:
        v = row.get(sv)
        if v is not None:
            try:
                sums[sv] += float(v)
            except Exception:
                pass

# SUM row
sep = "-" * sum(COL_WIDTHS.get(c, 10) + 2 for c in PRINT_VARS)
emit(" ", sep)
sum_parts = []
for col in PRINT_VARS:
    w = COL_WIDTHS.get(col, 10)
    if col in SUM_VARS:
        sum_parts.append(f"{sums[col]:>{w},.2f}")
    else:
        sum_parts.append(" " * w)
emit(" ", "  ".join(sum_parts))

with open(REPORT_PATH, "w", encoding="utf-8") as frpt:
    for line in output_lines:
        frpt.write(line + "\n")

print(f"WOFFTEXT written : {WOFFTEXT_PATH}")
print(f"Report written   : {REPORT_PATH}")
print(f"Records written  : {woff_df.height}")
