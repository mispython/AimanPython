#!/usr/bin/env python3
"""
Program  : EIBWCTCS.py
Purpose  : WEEKLY CTCS (Cheque Transaction Confirmation System) FILE ACCUMULATION
           To run on weekly basis: 1st, 9th, 16th, 23rd.
           ESMR: 2011-1379 / EJS: A2011-8660 / 2014-2484
           Reads a CTCS fixed-width text file, parses transactions,
           then appends/rebuilds the monthly CTCS parquet accumulator.
           Logic:
             - If FILEDAY == '08' (first week): overwrite the monthly store.
             - Otherwise: remove any rows already dated &RDATE, then prepend
               the new records (mimicking the SAS SET CTCS CTCS.CTCS&REPTMON).
"""

# ============================================================================
# IMPORTS
# ============================================================================
import os
import sys
from datetime import date, timedelta

import duckdb
import polars as pl

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
INPUT_DIR  = os.environ.get("INPUT_DIR",  "input")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "output")

# LOAN.REPTDATE parquet (contains single REPTDATE row)
REPTDATE_PQ  = os.path.join(INPUT_DIR, "REPTDATE.parquet")

# SAP.PBB.EPCU.CTCS.TXT(0) -- the weekly flat-file input
TXTFILE_PATH = os.environ.get("TXTFILE_PATH",
               os.path.join(INPUT_DIR, "CTCS_TXT.txt"))

# CTCS.CTCS&REPTMON -- monthly accumulator (parquet)
# The output path is determined at runtime once REPTMON is known.

os.makedirs(OUTPUT_DIR, exist_ok=True)

con = duckdb.connect()


def _load(path: str) -> pl.DataFrame:
    return con.execute(f"SELECT * FROM read_parquet('{path}')").pl()

# ============================================================================
# DERIVE MACRO VARIABLES FROM LOAN.REPTDATE
# ============================================================================
_rd_df = _load(REPTDATE_PQ)
_reptdate_val: date = _rd_df["REPTDATE"][0]

REPTDAY   = f"{_reptdate_val.day:02d}"
REPTMON   = f"{_reptdate_val.month:02d}"
REPTYEAR  = str(_reptdate_val.year)[-2:]
# &RDATE   = PUT(REPTDATE, Z5.) -- zero-padded 5-digit SAS date number
# Reproduced as DDMMYY8. string for comparison (used as &REPTDATE too)
RDATE_STR = _reptdate_val.strftime("%d/%m/%y")    # DDMMYY8. format

# Monthly accumulator path
CTCS_MON_PQ = os.path.join(OUTPUT_DIR, f"CTCS{REPTMON}.parquet")

# ============================================================================
# READ HEADER ROW OF TXTFILE TO DERIVE FILEDATE
# DATA _NULL_: INFILE TXTFILE OBS=1;
#   INPUT @003 YY 4. @007 MM 2. @009 DD 2.;
#   TDATE = MDY(MM,DD,YY) - 1;
# ============================================================================
with open(TXTFILE_PATH, "r") as _f:
    _header = _f.readline()

# Positions are 1-based in SAS: @003 = Python index [2:6], @007=[6:8], @009=[8:10]
_yy = int(_header[2:6])
_mm = int(_header[6:8])
_dd = int(_header[8:10])
_file_raw_date = date(_yy, _mm, _dd)
_tdate = _file_raw_date - timedelta(days=1)

FILEDAY    = f"{_tdate.day:02d}"
FILEMON    = f"{_tdate.month:02d}"
FILEYEAR   = str(_tdate.year)[-2:]
FILEDATE   = _tdate.strftime("%d/%m/%y")    # DDMMYY8.

# ============================================================================
# DATA _NULL_ -- validation: FILEDATE must equal REPTDATE
# ============================================================================
if FILEDATE != RDATE_STR:
    print(f"THE SAP.PBB.EPCU.CTCS IS NOT DATED {RDATE_STR}")
    sys.exit(77)

# ============================================================================
# DATA CTCS -- parse transaction rows from TXTFILE (skip header, skip TOTAL)
# SAS layout (1-based column positions):
#   @001 CHECK   $5.
#   @009 TRANDT  YYMMDD8.   -> positions [8:16]
#   @018 ACCTNO  10.        -> [17:27]
#   @029 NOTENO   5.        -> [28:33]
#   @035 CHEQNO   6.        -> [34:40]
#   @042 TRANAMT 10.2       -> [41:51]  (implied 2 decimal places)
#   @053 IND     $2.        -> [52:54]
#   @056 PRODIND $1.        -> [55:56]
#   @058 TRANCD  $2.        -> [57:59]
# REPTDATE = &RDATE (assigned as the SAS date integer; stored as date here)
# ============================================================================
_records: list[dict] = []

with open(TXTFILE_PATH, "r") as _f:
    next(_f)   # skip header line (OBS=1 in SAS is read separately above)
    for _line in _f:
        # CHECK is columns 1-5 (0-based: 0:5)
        _check = _line[0:5].strip()
        if _check == "TOTAL":
            continue
        # Parse fixed-width fields
        try:
            _trandt_str = _line[8:16].strip()   # YYMMDD8.: YYYYMMDD
            _trandt = date(
                int(_trandt_str[0:4]),
                int(_trandt_str[4:6]),
                int(_trandt_str[6:8]),
            ) if len(_trandt_str) == 8 else None

            _acctno  = int(_line[17:27].strip()) if _line[17:27].strip() else None
            _noteno  = int(_line[28:33].strip()) if _line[28:33].strip() else None
            _cheqno  = int(_line[34:40].strip()) if _line[34:40].strip() else None
            # TRANAMT 10.2 = 8 integer digits + 2 decimal digits (no decimal point)
            _tranamt_raw = _line[41:51].strip()
            _tranamt = float(_tranamt_raw) / 100.0 if _tranamt_raw else None
            _ind     = _line[52:54].strip() or None
            _prodind = _line[55:56].strip() or None
            _trancd  = _line[57:59].strip() or None
        except (ValueError, IndexError):
            continue

        _records.append({
            "TRANDT":   _trandt,
            "ACCTNO":   _acctno,
            "NOTENO":   _noteno,
            "CHEQNO":   _cheqno,
            "TRANAMT":  _tranamt,
            "IND":      _ind,
            "PRODIND":  _prodind,
            "TRANCD":   _trancd,
            "REPTDATE": _reptdate_val,
        })

ctcs_new = pl.DataFrame(_records) if _records else pl.DataFrame(
    schema={
        "TRANDT":   pl.Date,
        "ACCTNO":   pl.Int64,
        "NOTENO":   pl.Int64,
        "CHEQNO":   pl.Int64,
        "TRANAMT":  pl.Float64,
        "IND":      pl.String,
        "PRODIND":  pl.String,
        "TRANCD":   pl.String,
        "REPTDATE": pl.Date,
    }
)

# ============================================================================
# %MACRO APPEND logic
# %IF "&FILEDAY" EQ "08" %THEN %DO;   -- first week: overwrite
#   DATA CTCS.CTCS&REPTMON; SET CTCS; RUN;
# %ELSE %DO;
#   DATA CTCS.CTCS&REPTMON;
#     SET CTCS.CTCS&REPTMON;
#     IF REPTDATE EQ &RDATE THEN DELETE;   <- remove today's rows if any
#   RUN;
#   DATA CTCS.CTCS&REPTMON;               <- prepend new rows, then existing
#     SET CTCS CTCS.CTCS&REPTMON;
#   RUN;
# %END;
# ============================================================================
if FILEDAY == "08":
    # First week: initialise the monthly accumulator with today's data only
    ctcs_out = ctcs_new
else:
    # Subsequent weeks: load existing accumulator, drop rows for today,
    # then prepend the new rows
    if os.path.exists(CTCS_MON_PQ):
        ctcs_existing = (
            _load(CTCS_MON_PQ)
            .filter(pl.col("REPTDATE") != _reptdate_val)
        )
        # SET CTCS CTCS.CTCS&REPTMON -> new rows first, then existing
        ctcs_out = pl.concat([ctcs_new, ctcs_existing], how="diagonal")
    else:
        # Monthly accumulator does not yet exist; start fresh
        ctcs_out = ctcs_new

# Write monthly accumulator
ctcs_out.write_parquet(CTCS_MON_PQ)

print(f"CTCS accumulator written: {CTCS_MON_PQ}")
print(f"  New rows this run : {ctcs_new.height}")
print(f"  Total rows stored : {ctcs_out.height}")
print(f"  Report date       : {RDATE_STR}")
print(f"  File date         : {FILEDATE}")

con.close()
