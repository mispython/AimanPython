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
LNFILE_DIR = BASE_INPUT_PATH  / "LNFILE"   # RBP2.B033.LN4SCFIL(0) -> LN4SCFIL.parquet
BNM1_DIR   = BASE_INPUT_PATH  / "BNM1"     # SAP.PBB.MNILN.DAILY(0) -> REPTDATE.parquet
LNMIG_DIR  = BASE_OUTPUT_PATH / "LNMIG"    # SAP.PBB.LNMIG         -> TOTPAY.parquet
LNMIG_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------
# SAS date helpers (days since 1960)
# --------------------------------
SAS_EPOCH = date(1960, 1, 1)
def sas_to_py(n: int) -> date:
    return SAS_EPOCH.fromordinal(SAS_EPOCH.toordinal() + int(n))

# ==============================================================
# DATA REPTDATE (KEEP=REPTDATE)  → macros
#   REPTYEAR, REPTMON, REPTDAY, RDATE (DDMMYY8.), DATE=PUT(REPTDATE,Z5.)
# ==============================================================
rept_fp = BNM1_DIR / "REPTDATE.parquet"
REPTDATE_val = int(pl.read_parquet(rept_fp).select(pl.col("REPTDATE").first()).item())

_dt = sas_to_py(REPTDATE_val)
REPTYEAR = f"{_dt.year%100:02d}"
REPTMON  = f"{_dt.month:02d}"
REPTDAY  = f"{_dt.day:02d}"
RDATE    = _dt.strftime("%d/%m/%y")
DATE_MACRO = f"{REPTDATE_val:05d}"  # PUT(REPTDATE,Z5.) -> digit string; numeric equivalent is REPTDATE_val

# ==============================================================
# DATA TOTPAY;
#   INFILE LNFILE; INPUT ACCTNO NOTENO TOT_MIGR; DATE=&DATE;
#   (Here LN4SCFIL.parquet must already have ACCTNO, NOTENO, TOT_MIGR)
# ==============================================================
ln_src = LNFILE_DIR / "LN4SCFIL.parquet"
TOTPAY_DAY = pl.read_parquet(ln_src).select(
    [c for c in ["ACCTNO","NOTENO","TOT_MIGR"] if c in pl.read_parquet(ln_src).columns]
)
# Add DATE as numeric SAS date (use REPTDATE_val; same as &DATE resolving to digits)
TOTPAY_DAY = TOTPAY_DAY.with_columns(pl.lit(REPTDATE_val).alias("DATE"))

# ==============================================================
# PROC APPEND DATA=TOTPAY BASE=LNMIG.TOTPAY FORCE;
# Then sort BY ACCTNO NOTENO DESCENDING DATE
# Then NODUPKEY BY ACCTNO NOTENO (keep latest DATE)
# ==============================================================
base_fp = LNMIG_DIR / "TOTPAY.parquet"

BASE = None
if base_fp.exists():
    BASE = pl.read_parquet(base_fp)

# FORCE-like union
COMBINED = TOTPAY_DAY if BASE is None else pl.concat([BASE, TOTPAY_DAY], how="diagonal_relaxed")

# Sort & dedup
COMBINED = COMBINED.sort(by=["ACCTNO","NOTENO","DATE"], descending=[False, False, True])
COMBINED = COMBINED.unique(subset=["ACCTNO","NOTENO"], keep="first")

# Write back
COMBINED.write_parquet(base_fp)

# --- short summary ---
print(f"EIBDLNFL OK | +={TOTPAY_DAY.height} rows | total={COMBINED.height} | REPTDATE={REPTDATE_val} ({RDATE})")
