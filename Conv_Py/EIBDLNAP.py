from __future__ import annotations
import polars as pl
from pathlib import Path
from datetime import date

# ----------------------------
# BASE PATHS (adjust as needed)
# ----------------------------
BASE_INPUT_PATH  = Path("INPUT")
BASE_OUTPUT_PATH = Path("OUTPUT")
BASE_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# SAS libs → folders
SASD_DIR = BASE_INPUT_PATH  / "SASD"   # SAP.PBB.SASDATA.DAILY(0)
APP_DIR  = BASE_OUTPUT_PATH / "APP"    # SAP.PBB.STORE.SASDATA
APP_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------
# SAS date (days since 1960-01-01)
# --------------------------------
SAS_EPOCH = date(1960, 1, 1)
def sas_to_py(n: int) -> date:
    return SAS_EPOCH.fromordinal(SAS_EPOCH.toordinal() + int(n))

# ==============================================================
# OPTIONS ... (no-ops)
# ==============================================================

# ==============================================================
# DATA REPTDATE;  -> macros REPTYEAR REPTMON REPTDAY REPTDATE NOWK='4'
# ==============================================================
rept_fp = SASD_DIR / "REPTDATE.parquet"
REPTDATE_val = int(pl.read_parquet(rept_fp).select(pl.col("REPTDATE").first()).item())

_dt = sas_to_py(REPTDATE_val)
REPTYEAR = f"{_dt.year % 100:02d}"
REPTMON  = f"{_dt.month:02d}"
REPTDAY  = f"{_dt.day:02d}"
REPTDATE = REPTDATE_val  # keep SAS numeric
NOWK     = "4"           # not used later (as in SAS)

# ==============================================================
# %MACRO APPEND; ... %APPEND;
#   If REPTDAY="01": create fresh APP.LOAN&REPTMON from daily
#   Else: delete same DATE in base, append daily, sort, dedup by keys
# ==============================================================

# filenames
base_fp = APP_DIR / f"LOAN{REPTMON}.parquet"
day_fp  = SASD_DIR / f"LOAN{REPTMON}{REPTDAY}.parquet"

KEEP_COLS = ["ACCTNO","NOTENO","FISSPURP","PRODUCT","BALANCE","CENSUS","DNBFISME",
             "PRODCD","CUSTCD","AMTIND","SECTORCD","BRANCH","ACCTYPE","CCY","FORATE"]

# load daily + add DATE=&REPTDATE (SAS numeric)
DAILY = pl.read_parquet(day_fp).select([c for c in KEEP_COLS if c in pl.read_parquet(day_fp).columns])
if "DATE" not in DAILY.columns:
    DAILY = DAILY.with_columns(pl.lit(REPTDATE).alias("DATE"))
else:
    DAILY = DAILY.with_columns(pl.lit(REPTDATE).alias("DATE"))

if REPTDAY == "01":
    # FIRST DAY OF MONTH → overwrite month dataset with just today's rows
    DAILY.write_parquet(base_fp)
    total_rows = DAILY.height
    added_rows = DAILY.height
else:
    # load base (if exists) and drop rows with same DATE
    BASE = None
    if base_fp.exists():
        BASE = pl.read_parquet(base_fp)
        if "DATE" in BASE.columns:
            BASE = BASE.filter(pl.col("DATE") != REPTDATE)

    # stack BASE + DAILY (schema-union like FORCE)
    LOAN2 = DAILY if BASE is None else pl.concat([BASE, DAILY], how="diagonal_relaxed")

    # sort BY ACCTNO NOTENO DESCENDING DATE
    LOAN2 = LOAN2.sort(by=["ACCTNO","NOTENO","DATE"], descending=[False, False, True])

    # NODUPKEY BY ACCTNO NOTENO (keep first -> latest DATE)
    LOAN2 = LOAN2.unique(subset=["ACCTNO","NOTENO"], keep="first")

    # write back
    LOAN2.write_parquet(base_fp)
    total_rows = LOAN2.height
    added_rows = DAILY.height

# --- short summary ---
print(f"EIBDLNAP OK | LOAN{REPTMON} +={added_rows} rows | total={total_rows} | REPTDATE={REPTDATE} REPTDAY={REPTDAY}")
