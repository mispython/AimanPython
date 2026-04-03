#!/usr/bin/env python3
"""
Program  : EIBDLNAP.py
Purpose  : Append daily LOAN snapshot into monthly rolling dataset
           APP.LOAN<MM> for use by EIMDISRP.
           On the 1st of month: replace monthly file with today's snapshot.
           On all other days:   remove today's rows from the base, append
           today's snapshot, sort DESC DATE, deduplicate by ACCTNO/NOTENO.
"""

from __future__ import annotations
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
SASD_DIR = BASE_INPUT_PATH  / "SASD"   # SAP.PBB.SASDATA.DAILY(0)
APP_DIR  = BASE_OUTPUT_PATH / "APP"    # SAP.PBB.STORE.SASDATA
APP_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------------
# SAS date helper  (days since 1960-01-01 → Python date)
# --------------------------------------------------------------------------
_SAS_EPOCH = date(1960, 1, 1)

def sas_to_py(n: int) -> date:
    return _SAS_EPOCH + timedelta(days=int(n))

# =============================================================================
# DATA REPTDATE;
#   SET SASD.REPTDATE;
#   CALL SYMPUT('REPTYEAR', PUT(REPTDATE, YEAR2.));
#   CALL SYMPUT('REPTMON',  PUT(MONTH(REPTDATE), Z2.));
#   CALL SYMPUT('REPTDAY',  PUT(DAY(REPTDATE),   Z2.));
#   CALL SYMPUT('REPTDATE', REPTDATE);
#   CALL SYMPUT('NOWK',     PUT('4',$1.));
# =============================================================================
REPTDATE_val = int(
    pl.read_parquet(SASD_DIR / "REPTDATE.parquet")
    .select(pl.col("REPTDATE").first())
    .item()
)

_dt      = sas_to_py(REPTDATE_val)
REPTYEAR = f"{_dt.year  % 100:02d}"
REPTMON  = f"{_dt.month     :02d}"
REPTDAY  = f"{_dt.day       :02d}"
REPTDATE = REPTDATE_val          # keep as SAS numeric integer
NOWK     = "4"                   # not used downstream (mirrors SAS &NOWK)

# =============================================================================
# %MACRO APPEND
# =============================================================================

KEEP_COLS = [
    "ACCTNO", "NOTENO", "FISSPURP", "PRODUCT", "BALANCE", "CENSUS",
    "DNBFISME", "PRODCD", "CUSTCD", "AMTIND", "SECTORCD", "BRANCH",
    "ACCTYPE", "CCY", "FORATE",
]

# Resolve file paths
base_fp = APP_DIR  / f"LOAN{REPTMON}.parquet"
day_fp  = SASD_DIR / f"LOAN{REPTMON}{REPTDAY}.parquet"

# Load daily snapshot once; select only the KEEP columns that are present,
# then unconditionally add DATE = &REPTDATE (SAS numeric).
_day_raw = pl.read_parquet(day_fp)
DAILY = (
    _day_raw
    .select([c for c in KEEP_COLS if c in _day_raw.columns])
    .with_columns(pl.lit(REPTDATE).alias("DATE"))
)

if REPTDAY == "01":
    # ------------------------------------------------------------------
    # %IF "&REPTDAY" EQ "01" — first day of month:
    #   DATA APP.LOAN&REPTMON;
    #     SET SASD.LOAN&REPTMON&REPTDAY (KEEP=...);
    #     DATE = &REPTDATE;
    # Overwrite monthly dataset with today's snapshot only.
    # ------------------------------------------------------------------
    DAILY.write_parquet(base_fp)
    total_rows = DAILY.height
    added_rows = DAILY.height

else:
    # ------------------------------------------------------------------
    # %ELSE — mid-month day:
    #   1. Remove rows where DATE = &REPTDATE from the base.
    #   2. Append today's DAILY snapshot.
    #   3. PROC SORT BY ACCTNO NOTENO DESCENDING DATE.
    #   4. PROC SORT NODUPKEY BY ACCTNO NOTENO  (keep latest DATE).
    # ------------------------------------------------------------------
    BASE = None
    if base_fp.exists():
        BASE = pl.read_parquet(base_fp)
        if "DATE" in BASE.columns:
            BASE = BASE.filter(pl.col("DATE") != REPTDATE)

    # DATA LOAN2; SET APP.LOAN&REPTMON LOAN;
    LOAN2 = DAILY if BASE is None else pl.concat([BASE, DAILY], how="diagonal_relaxed")

    # PROC SORT DATA=LOAN2; BY ACCTNO NOTENO DESCENDING DATE;
    LOAN2 = LOAN2.sort(["ACCTNO", "NOTENO", "DATE"], descending=[False, False, True])

    # PROC SORT DATA=LOAN2 NODUPKEY; BY ACCTNO NOTENO;
    LOAN2 = LOAN2.unique(subset=["ACCTNO", "NOTENO"], keep="first")

    # DATA APP.LOAN&REPTMON; SET LOAN2;
    LOAN2.write_parquet(base_fp)
    total_rows = LOAN2.height
    added_rows = DAILY.height

# --------------------------------------------------------------------------
# Summary
# --------------------------------------------------------------------------
print(
    f"EIBDLNAP OK | LOAN{REPTMON} +={added_rows} rows | "
    f"total={total_rows} | REPTDATE={REPTDATE} REPTDAY={REPTDAY}"
)
