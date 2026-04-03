#!/usr/bin/env python3
"""
Program  : EIBDLNFL.py
Purpose  : Daily append of loan migration total-payment information
           (TOT_MIGR field) into the rolling LNMIG.TOTPAY dataset.
           Deduplicates by (ACCTNO, NOTENO), keeping the latest DATE.
ESMR     : 2012-3139 (DROP), 2013-396
"""

from pathlib import Path
from datetime import date, timedelta
import polars as pl

# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------
BASE_INPUT_PATH  = Path("INPUT")
BASE_OUTPUT_PATH = Path("OUTPUT")
BASE_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# SAS libs → folders
LNFILE_DIR = BASE_INPUT_PATH  / "LNFILE"   # RBP2.B033.LN4SCFIL(0)  → LN4SCFIL.txt
BNM1_DIR   = BASE_INPUT_PATH  / "BNM1"     # SAP.PBB.MNILN.DAILY(0) → REPTDATE.parquet
LNMIG_DIR  = BASE_OUTPUT_PATH / "LNMIG"    # SAP.PBB.LNMIG           → TOTPAY.parquet
LNMIG_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------------
# SAS date helper  (days since 1960-01-01 → Python date)
# --------------------------------------------------------------------------
_SAS_EPOCH = date(1960, 1, 1)

def sas_to_py(n: int) -> date:
    return _SAS_EPOCH + timedelta(days=int(n))

# =============================================================================
# DATA REPTDATE (KEEP=REPTDATE);
#   SET BNM1.REPTDATE;
#   CALL SYMPUT('REPTYEAR', PUT(REPTDATE, YEAR2.));
#   CALL SYMPUT('REPTMON',  PUT(MONTH(REPTDATE), Z2.));
#   CALL SYMPUT('REPTDAY',  PUT(DAY(REPTDATE),   Z2.));
#   CALL SYMPUT('RDATE',    PUT(REPTDATE, DDMMYY8.));
#   CALL SYMPUT('DATE',     PUT(REPTDATE, Z5.));
# =============================================================================
REPTDATE_val = int(
    pl.read_parquet(BNM1_DIR / "REPTDATE.parquet")
    .select(pl.col("REPTDATE").first())
    .item()
)

_dt      = sas_to_py(REPTDATE_val)
REPTYEAR = f"{_dt.year  % 100:02d}"          # PUT(REPTDATE, YEAR2.)
REPTMON  = f"{_dt.month     :02d}"            # PUT(MONTH(REPTDATE), Z2.)
REPTDAY  = f"{_dt.day       :02d}"            # PUT(DAY(REPTDATE),   Z2.)
RDATE    = _dt.strftime("%d/%m/%y")           # PUT(REPTDATE, DDMMYY8.) = 'DD/MM/YY'
DATE_MACRO = f"{REPTDATE_val:05d}"            # PUT(REPTDATE, Z5.) — string form
# DATE column value is the integer REPTDATE_val (SAS resolves &DATE as a
# numeric literal in the DATA step: DATE=&DATE → DATE=21000, not '21000')

# =============================================================================
# DATA TOTPAY;
#   INFILE LNFILE;
#   INPUT @001 ACCTNO   11.
#         @015 NOTENO    5.
#         @080 TOT_MIGR 15.2
#         ;
#   DATE = &DATE;
#
# LNFILE DD (RBP2.B033.LN4SCFIL(0)) is a mainframe flat file.
# All @pos values are 1-based; Python slice = [pos-1 : pos-1+width].
# Informat 15.2: raw integer text at that position, divided by 100.
# Minimum record length: @080 + 15 - 1 = 94 bytes.
# =============================================================================
ln_lines = (LNFILE_DIR / "LN4SCFIL.txt").read_bytes().decode("latin-1").splitlines()

def _int(s: str):
    """Parse a right-justified integer field. Returns None if blank/non-numeric."""
    s = s.strip()
    return int(s) if s.lstrip("-").isdigit() else None

def _dec(s: str, decimals: int):
    """
    Parse a right-justified numeric field with implied decimal places.
    SAS informat n.d: raw integer text divided by 10**d.
    e.g. '   123456' with decimals=2 → 1234.56
    """
    s = s.strip()
    if not s or not s.lstrip("-").isdigit():
        return None
    return int(s) / (10 ** decimals)

records = []
for line in ln_lines:
    rec = line.ljust(94)        # pad to minimum width; avoids IndexError

    records.append({
        "ACCTNO":   _int(rec[0:11]),        # @001  11.   py[0:11]
        "NOTENO":   _int(rec[14:19]),        # @015   5.   py[14:19]
        "TOT_MIGR": _dec(rec[79:94], 2),    # @080  15.2  py[79:94]
        "DATE":     REPTDATE_val,            # DATE = &DATE (SAS numeric integer)
    })

TOTPAY_DAY = pl.DataFrame(
    records,
    schema={
        "ACCTNO":   pl.Int64,
        "NOTENO":   pl.Int64,
        "TOT_MIGR": pl.Float64,
        "DATE":     pl.Int64,
    },
)

# =============================================================================
# PROC APPEND DATA=TOTPAY BASE=LNMIG.TOTPAY FORCE;
# =============================================================================
base_fp = LNMIG_DIR / "TOTPAY.parquet"

BASE = pl.read_parquet(base_fp) if base_fp.exists() else None

# FORCE-like union: diagonal_relaxed handles any schema differences
COMBINED = (
    TOTPAY_DAY if BASE is None
    else pl.concat([BASE, TOTPAY_DAY], how="diagonal_relaxed")
)

# =============================================================================
# PROC SORT DATA=LNMIG.TOTPAY; BY ACCTNO NOTENO DESCENDING DATE;
# PROC SORT DATA=LNMIG.TOTPAY NODUPKEY; BY ACCTNO NOTENO;
# =============================================================================
COMBINED = (
    COMBINED
    .sort(["ACCTNO", "NOTENO", "DATE"], descending=[False, False, True])
    .unique(subset=["ACCTNO", "NOTENO"], keep="first")
)

COMBINED.write_parquet(base_fp)

# --------------------------------------------------------------------------
# Summary
# --------------------------------------------------------------------------
print(
    f"EIBDLNFL OK | +={TOTPAY_DAY.height} rows | "
    f"total={COMBINED.height} | REPTDATE={REPTDATE_val} ({RDATE})"
)
