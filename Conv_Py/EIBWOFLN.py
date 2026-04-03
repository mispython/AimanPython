#!/usr/bin/env python3
"""
Program  : EIBWOFLN.py
Purpose  : Read NPL write-off movement flat files and split into PBB and
           PIBB datasets based on Cost Centre (COSTCTR 3000-4999 = PIBB).
           Produces four Parquet outputs:
             LNWOF/LNWOF.parquet   (PBB  write-off summary)
             ILNWOF/ILNWOF.parquet (PIBB write-off summary)
             WOMV/WOMOVE.parquet   (PBB  write-off movement)
             IWOMV/IWOMOVE.parquet (PIBB write-off movement)
"""

from pathlib import Path
from datetime import date
import polars as pl

# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------
BASE_FLAT = Path("input_flat")      # fixed-width .txt flat file inputs
BASE_OUT  = Path("output_parquet")  # Parquet output root

LN01_TXT = BASE_FLAT / "LN01" / "LN01.txt"
LN02_TXT = BASE_FLAT / "LN02" / "LN02.txt"

LNWOF_PATH  = BASE_OUT / "LNWOF"  / "LNWOF.parquet"
ILNWOF_PATH = BASE_OUT / "ILNWOF" / "ILNWOF.parquet"
WOMV_PATH   = BASE_OUT / "WOMV"   / "WOMOVE.parquet"
IWOMV_PATH  = BASE_OUT / "IWOMV"  / "IWOMOVE.parquet"

for p in (LNWOF_PATH, ILNWOF_PATH, WOMV_PATH, IWOMV_PATH):
    p.parent.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def _int(s: str):
    """Parse a right-justified integer field.  Returns None if blank/non-numeric."""
    s = s.strip()
    return int(s) if s.lstrip("-").isdigit() else None

def _dec(s: str, decimals: int):
    """
    Parse a right-justified numeric field with implied decimal places.
    SAS informat n.d: raw integer text divided by 10**d.
    e.g. '    123456789012345' with decimals=2 → 1234567890123.45
    Returns None if blank or non-numeric.
    """
    s = s.strip()
    if not s or not s.lstrip("-").isdigit():
        return None
    return int(s) / (10 ** decimals)


# --------------------------------------------------------------------------
# DATA LNWOF.LNWOF  ILNWOF.ILNWOF;
#   INFILE LN01;
#   INPUT @002  ACCTNO         10.    /* ACCOUNT NUMBER       */
#         @013  NOTENO          5.    /* NOTE NUMBER          */
#         @019  PRODUCT         3.    /* PRODUCT CODE         */
#         @023  CENSUS_TRT      6.2   /* CENSUS TRACT         */
#         @031  PAYMENT        15.2   /* TOTAL PAYMENT        */
#         @050  WRITE_DOWN_BAL 15.2   /* WRITE DOWN BALANCE   */
#         @067  NBDR           15.2   /* NET BAD DEBT RECOVER */
#         @084  RC             15.2   /* TOTAL RECOVERY COST  */
#         @101  NAI            15.2   /* NON ACCRUAL INTEREST */
#         @118  ORICODE         3.    /* ORI PRODUCT CODE     */
#         @121  IISR           15.2   /* TOTAL IISR           */
#         @140  COSTCTR         4.    /* COST CENTER          */
#         @145  REFNOTENO       5.    /* REFERENCE OLD NOTENO */
#         ;
#   IF (3000<=COSTCTR<=4999) THEN OUTPUT ILNWOF.ILNWOF;
#   ELSE                          OUTPUT LNWOF.LNWOF;
#
# All @pos values are 1-based; Python slice = [pos-1 : pos-1+width].
# Minimum record length: @145 + 5 - 1 = 149 bytes.
# --------------------------------------------------------------------------
ln01_lines = LN01_TXT.read_bytes().decode("latin-1").splitlines()

lnwof_rows  = []
ilnwof_rows = []

for line in ln01_lines:
    rec = line.ljust(149)   # pad to minimum width; avoids IndexError

    costctr = _int(rec[139:143])     # @140  4.

    row = {
        "ACCTNO":         _int(rec[1:11]),          # @002  10.
        "NOTENO":         _int(rec[12:17]),          # @013   5.
        "PRODUCT":        _int(rec[18:21]),          # @019   3.
        "CENSUS_TRT":     _dec(rec[22:28],  2),      # @023   6.2
        "PAYMENT":        _dec(rec[30:45],  2),      # @031  15.2
        "WRITE_DOWN_BAL": _dec(rec[49:64],  2),      # @050  15.2
        "NBDR":           _dec(rec[66:81],  2),      # @067  15.2
        "RC":             _dec(rec[83:98],  2),      # @084  15.2
        "NAI":            _dec(rec[100:115], 2),     # @101  15.2
        "ORICODE":        _int(rec[117:120]),         # @118   3.
        "IISR":           _dec(rec[120:135], 2),     # @121  15.2
        "COSTCTR":        costctr,                    # @140   4.
        "REFNOTENO":      _int(rec[144:149]),         # @145   5.
    }

    # IF (3000<=COSTCTR<=4999) THEN OUTPUT ILNWOF.ILNWOF;
    # ELSE                          OUTPUT LNWOF.LNWOF;
    if costctr is not None and 3000 <= costctr <= 4999:
        ilnwof_rows.append(row)
    else:
        lnwof_rows.append(row)

_LN01_SCHEMA = {
    "ACCTNO":         pl.Int64,
    "NOTENO":         pl.Int64,
    "PRODUCT":        pl.Int64,
    "CENSUS_TRT":     pl.Float64,
    "PAYMENT":        pl.Float64,
    "WRITE_DOWN_BAL": pl.Float64,
    "NBDR":           pl.Float64,
    "RC":             pl.Float64,
    "NAI":            pl.Float64,
    "ORICODE":        pl.Int64,
    "IISR":           pl.Float64,
    "COSTCTR":        pl.Int64,
    "REFNOTENO":      pl.Int64,
}

LNWOF  = pl.DataFrame(lnwof_rows,  schema=_LN01_SCHEMA)
ILNWOF = pl.DataFrame(ilnwof_rows, schema=_LN01_SCHEMA)

LNWOF.write_parquet(LNWOF_PATH)
ILNWOF.write_parquet(ILNWOF_PATH)


# --------------------------------------------------------------------------
# DATA WOMV.WOMOVE  IWOMV.IWOMOVE;
#   INFILE LN02;
#   INPUT @002  ACCTNO        10.
#         @015  NOTENO         5.
#         @030  PRODUCT        3.
#         @043  ORIPRODUCT     6.2  /* CENSUS TRACT             */
#         @055  PAYMENT       15.2  /* PAYMENT RECEIVED         */
#         @072  WDB_BFR_PAY   15.2  /* WDB BFR PAYMENT RECEIVED */
#         @089  WDB_AFT_PAY   15.2  /* WDB AFT PAYMENT RECEIVED */
#         @106  PAY_WDB       15.2  /* PAYMENT TO WDB           */
#         @130  BDR_BFR_PAY   15.2  /* BDR BFR PAYMENT RECEIVED */
#         @155  PAY_BDR       15.2  /* NET BDR                  */
#         @175  BDR_AFT_PAY   15.2  /* BDR AFT PAYMENT RECEIVED */
#         @195  RC            15.2  /* TOTAL RECOVERY COST      */
#         @215  NAI           15.2  /* NON-ACCRUAL INTEREST     */
#         @230  TRYR           4.   /* TRANSACTION DATE (YEAR)  */
#         @234  TRMM           2.   /* TRANSACTION DATE (MONTH) */
#         @236  TRDD           2.   /* TRANSACTION DATE (DAY)   */
#         @238  COSTCTR        4.   /* COST CENTER              */
#         ;
#   TRANDATE = MDY(TRMM,TRDD,TRYR);
#   DROP TRYR TRMM TRDD;
#   IF (3000<=COSTCTR<=4999) THEN OUTPUT IWOMV.IWOMOVE;
#   ELSE                          OUTPUT WOMV.WOMOVE;
#
# All @pos values are 1-based; Python slice = [pos-1 : pos-1+width].
# Minimum record length: @238 + 4 - 1 = 241 bytes.
# --------------------------------------------------------------------------
ln02_lines = LN02_TXT.read_bytes().decode("latin-1").splitlines()

womv_rows  = []
iwomv_rows = []

for line in ln02_lines:
    rec = line.ljust(241)   # pad to minimum width; avoids IndexError

    costctr = _int(rec[237:241])     # @238  4.

    # TRANDATE = MDY(TRMM, TRDD, TRYR)
    tryr = _int(rec[229:233])        # @230  4.
    trmm = _int(rec[233:235])        # @234  2.
    trdd = _int(rec[235:237])        # @236  2.
    try:
        trandate = date(tryr, trmm, trdd) if (tryr and trmm and trdd) else None
    except (ValueError, TypeError):
        trandate = None

    row = {
        "ACCTNO":       _int(rec[1:11]),         # @002  10.
        "NOTENO":       _int(rec[14:19]),         # @015   5.
        "PRODUCT":      _int(rec[29:32]),         # @030   3.
        "ORIPRODUCT":   _dec(rec[42:48],   2),    # @043   6.2  /* CENSUS TRACT */
        "PAYMENT":      _dec(rec[54:69],   2),    # @055  15.2
        "WDB_BFR_PAY":  _dec(rec[71:86],   2),   # @072  15.2
        "WDB_AFT_PAY":  _dec(rec[88:103],  2),   # @089  15.2
        "PAY_WDB":      _dec(rec[105:120], 2),   # @106  15.2
        "BDR_BFR_PAY":  _dec(rec[129:144], 2),  # @130  15.2
        "PAY_BDR":      _dec(rec[154:169], 2),  # @155  15.2
        "BDR_AFT_PAY":  _dec(rec[174:189], 2),  # @175  15.2
        "RC":           _dec(rec[194:209], 2),   # @195  15.2
        "NAI":          _dec(rec[214:229], 2),   # @215  15.2
        # TRYR, TRMM, TRDD are DROPped after TRANDATE is built
        "TRANDATE":     trandate,                 # MDY(TRMM,TRDD,TRYR)
        "COSTCTR":      costctr,                  # @238   4.
    }

    # IF (3000<=COSTCTR<=4999) THEN OUTPUT IWOMV.IWOMOVE;
    # ELSE                          OUTPUT WOMV.WOMOVE;
    if costctr is not None and 3000 <= costctr <= 4999:
        iwomv_rows.append(row)
    else:
        womv_rows.append(row)

_LN02_SCHEMA = {
    "ACCTNO":      pl.Int64,
    "NOTENO":      pl.Int64,
    "PRODUCT":     pl.Int64,
    "ORIPRODUCT":  pl.Float64,
    "PAYMENT":     pl.Float64,
    "WDB_BFR_PAY": pl.Float64,
    "WDB_AFT_PAY": pl.Float64,
    "PAY_WDB":     pl.Float64,
    "BDR_BFR_PAY": pl.Float64,
    "PAY_BDR":     pl.Float64,
    "BDR_AFT_PAY": pl.Float64,
    "RC":          pl.Float64,
    "NAI":         pl.Float64,
    "TRANDATE":    pl.Date,
    "COSTCTR":     pl.Int64,
}

WOMV  = pl.DataFrame(womv_rows,  schema=_LN02_SCHEMA)
IWOMV = pl.DataFrame(iwomv_rows, schema=_LN02_SCHEMA)

WOMV.write_parquet(WOMV_PATH)
IWOMV.write_parquet(IWOMV_PATH)

print("Done:")
print(" -", LNWOF_PATH)
print(" -", ILNWOF_PATH)
print(" -", WOMV_PATH)
print(" -", IWOMV_PATH)
