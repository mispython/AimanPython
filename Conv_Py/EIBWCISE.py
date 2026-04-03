# !/usr/bin/env python3
"""
Program  : EIBWCISE.py
Purpose  : Extract Directors' Info from CIS flat file into MNICS library
Date     : 26.03.99
"""

from pathlib import Path
from datetime import date
import polars as pl

# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------
BASE_FLAT = Path("input_flat")           # fixed-width .txt flat file inputs
BASE_OUT  = Path("output_parquet")       # Parquet output root

CIS_TXT   = BASE_FLAT / "CIS" / "CIS.txt"
MNICS_DIR = BASE_OUT  / "MNICS"

MNICS_DIR.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------------------------------
# Helper: write a Polars DataFrame as a Parquet file under MNICS library
# --------------------------------------------------------------------------
def write_mnics(df: pl.DataFrame, name: str) -> None:
    df.write_parquet(MNICS_DIR / f"{name}.parquet")


# --------------------------------------------------------------------------
# DATA MNICS.REPTDATE;
#    REPTDATE = TODAY();
# --------------------------------------------------------------------------
MNICS_REPTDATE = pl.DataFrame({"REPTDATE": [date.today()]})
write_mnics(MNICS_REPTDATE, "REPTDATE")


# --------------------------------------------------------------------------
# Helper: 1-based fixed-width slice, mirroring SAS @pos $width. / width.
#   SAS column pointer @pos is 1-based; Python str.slice() is 0-based.
# --------------------------------------------------------------------------
def fw_slice(col: pl.Expr, pos1: int, width: int) -> pl.Expr:
    return col.str.slice(pos1 - 1, width)


# --------------------------------------------------------------------------
# DATA CIS;
#   DROP NUM ACCTN;
#   RETAIN NUM '0123456789';    <- lookup string for VERIFY(), not a column
#   INFILE CIS;
#   INPUT @2   ACCTN    $10. @;
#   IF VERIFY(ACCTN,NUM) ^= 0 THEN DELETE;
#   ACCTNO = INPUT(ACCTN,10.);
#   INPUT @21  NAME     $40.
#         @61  PP_ALIAS $20.
#         @81  SIC       5.
#         @86  SEX      $1.
#         @87  DIRNIC   $20.
#         @107 DIROIC    9.
#         @116 DIRNAME  $40.;
#
# Read the CIS mainframe flat file (fixed-width .txt).
# Each line is one record; columns are addressed by 1-based byte positions.
# --------------------------------------------------------------------------
raw_lines = CIS_TXT.read_bytes().decode("latin-1").splitlines()

records = []
for line in raw_lines:
    # Pad to at least 155 chars to avoid IndexError on short trailing records
    rec = line.ljust(155)

    # @2 ACCTN $10.  (positions 2-11, 0-based: 1-10)
    acctn = rec[1:11]

    # SAS: IF VERIFY(ACCTN,'0123456789') ^= 0 THEN DELETE;
    # VERIFY returns nonzero if any char in ACCTN is NOT in '0123456789'.
    # $10. always delivers exactly 10 chars; spaces → not a digit → deleted.
    if not acctn.isdigit():
        continue

    # ACCTNO = INPUT(ACCTN,10.);
    acctno = int(acctn)

    # Remaining INPUT statement fields
    name     = rec[20:60].rstrip()         # @21 NAME     $40.
    pp_alias = rec[60:80].rstrip()         # @61 PP_ALIAS $20.
    sic_raw  = rec[80:85].strip()          # @81 SIC       5.
    sex      = rec[85:86]                  # @86 SEX      $1.
    dirnic   = rec[86:106].rstrip()        # @87 DIRNIC   $20.
    diroic_r = rec[106:115].strip()        # @107 DIROIC   9.
    dirname  = rec[115:155].rstrip()       # @116 DIRNAME  $40.

    sic    = int(sic_raw)    if sic_raw.lstrip("-").isdigit()    else None
    diroic = int(diroic_r)   if diroic_r.lstrip("-").isdigit()   else None

    records.append({
        "ACCTNO":   acctno,
        "NAME":     name,
        "PP_ALIAS": pp_alias,
        "SIC":      sic,
        "SEX":      sex,
        "DIRNIC":   dirnic,
        "DIROIC":   diroic,
        "DIRNAME":  dirname,
    })

CIS_DF = pl.DataFrame(
    records,
    schema={
        "ACCTNO":   pl.Int64,
        "NAME":     pl.Utf8,
        "PP_ALIAS": pl.Utf8,
        "SIC":      pl.Int64,
        "SEX":      pl.Utf8,
        "DIRNIC":   pl.Utf8,
        "DIROIC":   pl.Int64,
        "DIRNAME":  pl.Utf8,
    },
)

# --------------------------------------------------------------------------
# PROC SORT DATA=CIS OUT=MNICS.CIS;
#    BY ACCTNO;
# --------------------------------------------------------------------------
MNICS_CIS = CIS_DF.sort("ACCTNO")
write_mnics(MNICS_CIS, "CIS")
