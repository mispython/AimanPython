# !/usr/bin/env python3
"""
Program  : EIDAIABL.py
Purpose  : Capturing of transaction data for computation of commission from
           AIA for telemarketing product.
ESMR     : 2013-1756
"""

from pathlib import Path
from datetime import date
import duckdb
import polars as pl

# ---------- paths ----------
BASE_IN  = Path("input_parquet")       # expects: AIAIN.parquet, AIAOUT.parquet
BASE_OUT = Path("output_parquet")      # writes AIA/TRANBAIA<MM><YY>.parquet + AIAFILE_<MM><YY>.txt
BASE_OUT.mkdir(parents=True, exist_ok=True)

AIAIN_PATH  = BASE_IN / "AIAIN.parquet"
AIAOUT_PATH = BASE_IN / "AIAOUT.parquet"

MONTHLY_DIR = BASE_OUT / "AIA"
MONTHLY_DIR.mkdir(parents=True, exist_ok=True)

# ---------- 1) REPTDATE & "macro" values ----------
TODAY = date.today()
dd = TODAY.day

if dd == 8:
    SDD, WK, WK1 = 1,  "1", "4"
elif dd == 15:
    SDD, WK, WK1 = 9,  "2", "1"
elif dd == 22:
    SDD, WK, WK1 = 16, "3", "2"
else:
    SDD, WK, WK1 = 23, "4", "3"

REPTYEAR = f"{TODAY.year % 100:02d}"   # SAS YEAR2.
REPTMON  = f"{TODAY.month:02d}"        # SAS Z2.
REPTDAY  = f"{TODAY.day:02d}"          # SAS Z2.
NOWK     = WK
REPTDATE = TODAY                       # store as a true date

# ---------- 2) AIA2PBB (DROP=IND; filter IND!='FF') ----------
# SAS INFILE AIAIN FIRSTOBS=2 LRECL=150 ...
# FIRSTOBS=2 means the first row (header/label row) is skipped.
# Columns read from AIAIN: IND, NEWIC, POLICYNO, BLFREQ
AIA2PBB = duckdb.execute(f"""
    SELECT NEWIC, POLICYNO, BLFREQ
    FROM read_parquet('{AIAIN_PATH}')
    WHERE IND != 'FF'
    OFFSET 1
""").pl()

# ---------- 3) PBB2AIA (retain/forward-fill BLDATE; drop IND in ('PB','FF')) ----------
# SAS INFILE AIAOUT LRECL=150 TRUNCOVER
# _N_=1: reads only IND ($2.) at col 1 and BLDATE (6.) at col 20 — header row only.
# _N_>1: reads IND, ACCTNO, MODALPREM, NEWIC, POLICYNO, BLSTAT, INSTID; BLDATE is RETAINED.
# The first row of the parquet is the header row carrying BLDATE; detail rows follow.
# Row 0 (header) is excluded from detail output; BLDATE is forward-filled into detail rows.
PBB2AIA_raw = duckdb.execute(f"""
    SELECT *
    FROM read_parquet('{AIAOUT_PATH}')
    OFFSET 1
""").pl()

PBB2AIA = (
    PBB2AIA_raw
    .with_columns(pl.col("BLDATE").forward_fill())
    .filter(~pl.col("IND").is_in(["PB", "FF"]))
    .drop("IND")
)

# ---------- 4) Sort & MERGE BY POLICYNO NEWIC; keep left (IF A) ----------
PBB2AIA = PBB2AIA.sort(["POLICYNO", "NEWIC"])
AIA2PBB = AIA2PBB.sort(["POLICYNO", "NEWIC"])

COMBINE = (
    PBB2AIA.join(AIA2PBB, on=["POLICYNO", "NEWIC"], how="left")
    .with_columns(pl.lit(REPTDATE).alias("DATE"))
)

# ---------- 5) Monthly append to AIA/TRANBAIA<MM><YY>.parquet (PROC APPEND logic) ----------
MONTHLY_NAME = f"TRANBAIA{REPTMON}{REPTYEAR}.parquet"
MONTHLY_PATH = MONTHLY_DIR / MONTHLY_NAME

if MONTHLY_PATH.exists():
    base = pl.read_parquet(MONTHLY_PATH)
    # SAS: delete rows for the same DATE then append (idempotent re-run)
    base = base.filter(pl.col("DATE") != REPTDATE)
    out = pl.concat([base, COMBINE], how="vertical_relaxed")
else:
    out = COMBINE

out.write_parquet(MONTHLY_PATH)

# ---------- 6) TRANBAIA — SAS retained running sums (TOTAL+MODALPREM; TOTCN+CN) ----------
# SAS: CN=1; TOTAL+MODALPREM; TOTCN+CN;
# These are retained accumulator variables — each row holds the cumulative sum up to
# that point. PROC SORT BY DESCENDING TOTCN then puts the last row (highest TOTCN) first,
# effectively making the grand totals available in the first row for the _N_=1 header block.
TRANBAIA = (
    pl.read_parquet(MONTHLY_PATH)
    .with_columns(pl.lit(1).alias("CN"))
    .with_columns([
        pl.col("MODALPREM").cum_sum().alias("TOTAL"),
        pl.col("CN").cum_sum().alias("TOTCN"),
    ])
)

# PROC SORT DATA=TRANBAIA; BY DESCENDING TOTCN;
# Since TOTCN is a sequential counter (1, 2, 3, ...), descending sort puts the last row
# (with grand totals) first, which is what _N_=1 reads for header printing.
TRANBAIA = TRANBAIA.sort("TOTCN", descending=True)

# Grand totals are now in row 0 (the former last row after sort reversal)
TOTAL_value = TRANBAIA["TOTAL"][0]
TOTCN_value = TRANBAIA["TOTCN"][0]

# Optional: persist enriched table
TRANBAIA_OUT = MONTHLY_DIR / f"TRANBAIA{REPTMON}{REPTYEAR}_with_totals.parquet"
TRANBAIA.write_parquet(TRANBAIA_OUT)

# ---------- 7) Fixed-width text report (SAS DATA _NULL_ FILE AIAFILE PUT ...) ----------
# Output is a report — ASA carriage control characters are included.
# Default page length: 60 lines per page. ASA control chars are in column 1:
#   ' ' = single space (advance 1 line before printing)
#   '0' = double space (advance 2 lines before printing)
#   '1' = advance to new page
#   '+' = no advance (overprint)
REPORT_PATH = MONTHLY_DIR / f"AIAFILE_{REPTMON}{REPTYEAR}.txt"

# ASA carriage control constants
ASA_SINGLE = " "   # advance 1 line
ASA_DOUBLE = "0"   # advance 2 lines (blank line effect)
ASA_PAGE   = "1"   # advance to new page

LINE_LEN = 150     # LRECL from JCL DCB
PAGE_LEN = 60      # default page length (lines per page)

def _asa_line(asa_char: str, content: str = "", lrecl: int = 150) -> str:
    """Prepend ASA carriage control character; pad/truncate to lrecl+1 total."""
    line = asa_char + f"{content:<{lrecl}}"
    return line[:lrecl + 1]

def _put(buf: list, pos1: int, text: str):
    """Place text at 1-based column 'pos1' into a mutable list buffer (no ASA prefix)."""
    i = pos1 - 1
    for j, ch in enumerate(text):
        need = i + j + 1 - len(buf)
        if need > 0:
            buf.extend([" "] * need)
        buf[i + j] = ch

def _fmt_left(s, width: int) -> str:
    s = "" if s is None else str(s)
    return f"{s:<{width}}"[:width]

def _fmt_best12(val) -> str:
    """SAS BEST12. — right-justified in 12 chars, best numeric representation.
       Preserves significant digits without forcing decimal places.
    """
    if val is None:
        return " " * 12
    try:
        f = float(val)
        # Use integer repr if value is whole, otherwise use up to 2 decimal places
        # to match SAS BEST12. behaviour for currency-style values
        if f == int(f):
            s = str(int(f))
        else:
            s = f"{f:.2f}"
        return s.rjust(12)[:12]
    except Exception:
        return " " * 12

def _fmt_right_num(val, width: int, decimals: int = 0) -> str:
    """Format numeric value right-justified with fixed decimal places."""
    if val is None:
        return " " * width
    try:
        if decimals == 0:
            return f"{int(round(float(val))):>{width}d}"[-width:]
        return f"{float(val):>{width}.{decimals}f}"[-width:]
    except Exception:
        return " " * width

def _fmt_6(val) -> str:
    """SAS format '6.' on a numeric (SAS date integer) — right-justified integer in 6 chars.
       SAS stores dates as days since 1960-01-01; PUT(BLDATE, 6.) prints the raw integer.
       If the parquet already stores BLDATE as a Python date or string, convert accordingly.
    """
    if val is None:
        return " " * 6
    # If stored as Python date object, convert to SAS date integer (days since 1960-01-01)
    if isinstance(val, date):
        sas_origin = date(1960, 1, 1)
        sas_int = (val - sas_origin).days
        return f"{sas_int:>6d}"[:6]
    # If already an integer or numeric string
    try:
        return f"{int(val):>6d}"[:6]
    except Exception:
        return str(val)[:6].rjust(6)

def _write_line(f, asa: str, content: str = "", lrecl: int = LINE_LEN) -> None:
    """Write one ASA-prefixed fixed-width line to the open file handle."""
    f.write(_asa_line(asa, content, lrecl) + "\n")

# ---------- Write report ----------
with REPORT_PATH.open("w", encoding="utf-8", newline="\n") as f:

    line_count = 0

    # IF _N_=1 THEN DO — header block (first row of TRANBAIA after sort)
    # PUT @001 ' ';
    _write_line(f, ASA_SINGLE, "")
    line_count += 1

    # PUT @001 'TOTAL PREMIUM (RM) : ' @022 TOTAL;
    # TOTAL is printed with SAS default BEST12. format
    buf = [" "] * LINE_LEN
    _put(buf, 1,  "TOTAL PREMIUM (RM) : ")
    _put(buf, 22, _fmt_best12(TOTAL_value))
    _write_line(f, ASA_SINGLE, "".join(buf).rstrip())
    line_count += 1

    # PUT @001 ' ';
    _write_line(f, ASA_SINGLE, "")
    line_count += 1

    # PUT @001 'TOTAL NUMBER OF TRANSACTION : ' @031 TOTCN;
    # TOTCN printed with SAS default BEST12. format
    buf = [" "] * LINE_LEN
    _put(buf, 1,  "TOTAL NUMBER OF TRANSACTION : ")
    _put(buf, 31, _fmt_best12(TOTCN_value))
    _write_line(f, ASA_SINGLE, "".join(buf).rstrip())
    line_count += 1

    # PUT @001 ' ';
    _write_line(f, ASA_SINGLE, "")
    line_count += 1

    # Column headings at fixed positions
    buf = [" "] * LINE_LEN
    _put(buf, 1,  "BLDATE")
    _put(buf, 8,  "ACCTNO")
    _put(buf, 32, "MODALPREM")
    _put(buf, 42, "NEWIC")
    _put(buf, 55, "POLICYNO")
    _put(buf, 76, "BLSTAT")
    _put(buf, 84, "INSTID")
    _put(buf, 100, "BLFREQ")
    _write_line(f, ASA_SINGLE, "".join(buf).rstrip())
    line_count += 1

    # Detail lines
    # SAS PUT positions/widths:
    # @001 BLDATE 6.
    # @008 ACCTNO $20.
    # @029 MODALPREM 12.2
    # @042 NEWIC $12.
    # @055 POLICYNO $20.
    # @076 BLSTAT $2.
    # @084 INSTID $15.
    # @100 BLFREQ $1.
    for row in TRANBAIA.iter_rows(named=True):
        if line_count >= PAGE_LEN:
            # Issue a page-advance ASA character on the next line
            line_count = 0
            asa = ASA_PAGE
        else:
            asa = ASA_SINGLE

        buf = [" "] * LINE_LEN
        _put(buf, 1,   _fmt_6(row.get("BLDATE")))
        _put(buf, 8,   _fmt_left(row.get("ACCTNO"),   20))
        _put(buf, 29,  _fmt_right_num(row.get("MODALPREM"), 12, 2))
        _put(buf, 42,  _fmt_left(row.get("NEWIC"),    12))
        _put(buf, 55,  _fmt_left(row.get("POLICYNO"), 20))
        _put(buf, 76,  _fmt_left(row.get("BLSTAT"),   2))
        _put(buf, 84,  _fmt_left(row.get("INSTID"),   15))
        _put(buf, 100, _fmt_left(row.get("BLFREQ"),   1))
        _write_line(f, asa, "".join(buf).rstrip())
        line_count += 1

print(f"[OK] Monthly parquet  : {MONTHLY_PATH}")
print(f"[OK] Totals parquet   : {TRANBAIA_OUT}")
print(f"[OK] Text report      : {REPORT_PATH}")
