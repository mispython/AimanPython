from __future__ import annotations
from pathlib import Path
from datetime import date
import polars as pl

# ===============================================
# Configurable I/O roots
# ===============================================
BASE_IN  = Path("input_parquet")
BASE_OUT = Path("output_parquet")
(BASE_OUT / "LOAN").mkdir(parents=True, exist_ok=True)

# ===============================================
# Helpers (SAS date compatibility)
# ===============================================
SAS_EPOCH = date(1960, 1, 1)

def _sas_days(d: date) -> int:
    return (d - SAS_EPOCH).days

# ===============================================
# Load REPTDATE and derive SAS-like macros
# - Expects input_parquet/LN/REPTDATE.parquet with column REPTDATE (date/datetime)
# ===============================================
T_REPTDATE = pl.read_parquet(BASE_IN / "LN" / "REPTDATE.parquet")
REPTDATE = T_REPTDATE.select(pl.col("REPTDATE").cast(pl.Date)).to_series().item()

REPTYEAR = f"{REPTDATE.year % 100:02d}"
REPTMON  = f"{REPTDATE.month:02d}"
REPTDAY  = f"{REPTDATE.day:02d}"
RDATE    = f"{_sas_days(REPTDATE):05d}"  # SAS PUT(date, Z5.) equivalent

# ===============================================
# Load HP list (replacement for %INC PGM(PBBLNFMT) providing &HP)
# - Default path: input_parquet/FORMATS/HP.parquet with column LOANTYPE (int)
# ===============================================
HP_FMT_PATH = BASE_IN / "FORMATS" / "HP.parquet"
if HP_FMT_PATH.exists():
    HP_SET = set(pl.read_parquet(HP_FMT_PATH).select("LOANTYPE").to_series().to_list())
else:
    # Fallback: empty set—adjust if you want a default list
    HP_SET = set()

# ===============================================
# Build LNFILE from ACCTFILE (already parsed from fixed-width into Parquet)
# - Expects input_parquet/ACCTFILE.parquet with the listed columns
# ===============================================
ACCT = pl.read_parquet(BASE_IN / "ACCTFILE.parquet")

required_acct_cols = [
    "ACCTNO","NOTENO","LOANTYPE","CURBAL","APPVALUE","ORGBAL","NETPROC",
    "TOTFEE","REBATEX","INTEARN4X","PAYAMT","FEETOTAL","FEETOT2","ECSRRSRV","OVERFEE",
]
missing = [c for c in required_acct_cols if c not in ACCT.columns]
if missing:
    raise ValueError(f"ACCTFILE missing columns: {missing}")

LNFILE = (
    ACCT
    .select([pl.col(c) for c in required_acct_cols])
    .with_columns([
        pl.lit(int(RDATE)).alias("REPTDATE")  # keep as SAS numeric date (days since 1960)
    ])
    .filter(pl.col("LOANTYPE").is_in(list(HP_SET)))
)

# ===============================================
# Build OI from OITXT (already parsed from fixed-width into Parquet)
# - Expects input_parquet/OITXT.parquet with columns used below
# ===============================================
OITXT = pl.read_parquet(BASE_IN / "OITXT.parquet")

required_oi_cols = [
    "ACCTNO","NOTENO","YY","MM","DD","OVERINT","PAYOFF","TOTNPAID","TOTBILL",
    "BILLPAY","BILLCNT","BILLSIGN","REBATE","INTEARN4",
]
missing_oi = [c for c in required_oi_cols if c not in OITXT.columns]
if missing_oi:
    raise ValueError(f"OITXT missing columns: {missing_oi}")

OI = (
    OITXT
    .with_columns([
        pl.when(pl.col("BILLSIGN") == pl.lit("-")).then(pl.lit(0)).otherwise(pl.col("BILLPAY")).alias("BILLPAY"),
        pl.col("YY").cast(pl.Int32).alias("YY"),
        pl.col("MM").cast(pl.Int32).alias("MM"),
        pl.col("DD").cast(pl.Int32).alias("DD"),
    ])
    .with_columns([
        pl.when(
            (pl.col("YY").is_not_null()) & (pl.col("MM").is_not_null()) & (pl.col("DD").is_not_null())
        ).then(
            pl.date(pl.col("YY"), pl.col("MM"), pl.col("DD"))
        ).otherwise(
            None
        ).alias("_REPTDATE_DT")
    ])
    .with_columns([
        (pl.col("_REPTDATE_DT").cast(pl.Date)).map_elements(
            lambda d: None if d is None else _sas_days(d), return_dtype=pl.Int64
        ).alias("REPTDATE")
    ])
    .drop(["YY","MM","DD","_REPTDATE_DT"])
    .select([
        "ACCTNO","NOTENO","OVERINT","PAYOFF","TOTNPAID","TOTBILL",
        "BILLPAY","BILLCNT","BILLSIGN","REBATE","INTEARN4","REPTDATE"
    ])
)

# ===============================================
# Merge (BY ACCTNO NOTENO REPTDATE; keep LNFILE only)
# ===============================================
LNFILE = LNFILE.join(OI, on=["ACCTNO","NOTENO","REPTDATE"], how="left")

# ===============================================
# ACCUM macro behavior → monthly rolling Parquet LOAN/REPOLN<REPTMON>.parquet
# ===============================================
monthly_path = BASE_OUT / "LOAN" / f"REPOLN{REPTMON}.parquet"

if REPTDAY == "01":
    # Replace monthly file with current LNFILE
    LNFILE.write_parquet(monthly_path)
else:
    if monthly_path.exists():
        EXIST = pl.read_parquet(monthly_path)
        EXIST = EXIST.filter(pl.col("REPTDATE") != int(RDATE))  # delete rows for today's RDATE
        OUT = pl.concat([EXIST, LNFILE], how="vertical_relaxed")
    else:
        OUT = LNFILE
    OUT.write_parquet(monthly_path)

print(f"Written: {monthly_path}")
