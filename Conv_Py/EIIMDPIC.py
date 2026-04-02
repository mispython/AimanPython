
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import duckdb
import pyarrow.parquet as pq  # use for robust REPTDATE read

BASE_INPUT_PATH = Path("inputdata") # Folder for source files
BASE_OUTPUT_PATH = Path("ECPOUT")   # Folder for output files
BASE_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

ICLRGNEW_DATA_PATH = BASE_OUTPUT_PATH / "ICLRGNEW"

# File paths
REPTDATE_LOAN = BASE_INPUT_PATH / "LOAN.REPTDATE"
RPTFILE2 = BASE_INPUT_PATH / "RPTFILE2"
RPTFILE  = BASE_INPUT_PATH / "RPTFILE"
BRHCODE  = BASE_INPUT_PATH / "BRHCODE"

# ---- REPTDATE via PyArrow (handles date/datetime/string) ----
t = pq.read_table(REPTDATE_LOAN)
col = t.column("REPTDATE")[0].as_py()
if isinstance(col, datetime):
    REPTDATE = col
elif hasattr(col, "isoformat"):  # date
    REPTDATE = datetime.combine(col, datetime.min.time())
else:
    REPTDATE = datetime.strptime(str(col)[:10], "%Y-%m-%d")

day   = REPTDATE.day
month = REPTDATE.month
year  = REPTDATE.year

if 1 <= day <= 8:
    NOWK = "1"
elif 9 <= day <= 15:
    NOWK = "2"
elif 16 <= day <= 22:
    NOWK = "3"
else:
    NOWK = "4"

REPTYEAR = str(year)[-2:]
REPTMON  = f"{month:02d}"
REPTDAY  = f"{day:02d}"
RDATE    = REPTDATE.strftime("%Y%m%d")

print(REPTDATE)

# ---- Inputs are Parquet ----
ICLRGORI = pl.read_parquet(RPTFILE2).select([
    "BNKTYPE","BNKCODE","YY","MM","DD","CHKNUM","PAYBANK","MICRPAY",
    "ACCTNO","TRXCODE","AMOUNT","PREBANK","MICRPRE","CHKTYPE","BRCODE","UICCODE"
])

ICLRGORI = ICLRGORI.with_columns([
    pl.col("YY").cast(pl.Int32),
    pl.col("MM").cast(pl.Int32),
    pl.col("DD").cast(pl.Int32),
    pl.date(pl.col("YY"), pl.col("MM"), pl.col("DD")).alias("CLRGDT")
]).sort("MICRPAY")

BR = pl.read_parquet(BRHCODE).select(["MICRPAY","BRANCH"]).sort("MICRPAY")

# unify join key dtype
ICLRGORI = ICLRGORI.with_columns(pl.col("MICRPAY").cast(pl.Utf8))
BR       = BR.with_columns(pl.col("MICRPAY").cast(pl.Utf8))

ICLRG1 = ICLRGORI.join(BR, on="MICRPAY", how="inner")

ICLRGD = ICLRG1.select([
    "BNKTYPE","BNKCODE","CLRGDT","CHKNUM","PAYBANK","CHKTYPE",
    "MICRPAY","ACCTNO","AMOUNT","TRXCODE","PREBANK","MICRPRE","BRCODE",
    "UICCODE","BRANCH"
]).sort("UICCODE")

ICLRGD_out = BASE_OUTPUT_PATH / f"ICLRGA{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
ICLRGD.write_parquet(ICLRGD_out)

ICLRGUIC = pl.read_parquet(RPTFILE).select([
    "RCRDTYPE","CHKNUM","MICRPAY","ACCTNO","TRXCODE","TRXAMT","MICR",
    "REJECT","TRXIND","TRXTYPE","YY2","MM2","DD2","UICCODE"
]).filter(pl.col("RCRDTYPE") == 2).with_columns([
    pl.col("YY2").cast(pl.Int32),
    pl.col("MM2").cast(pl.Int32),
    pl.col("DD2").cast(pl.Int32),
    pl.date(pl.col("YY2"), pl.col("MM2"), pl.col("DD2")).alias("DRDATE")
]).sort("UICCODE")

# unify join key
ICLRGD   = ICLRGD.with_columns(pl.col("UICCODE").cast(pl.Utf8))
ICLRGUIC = ICLRGUIC.with_columns(pl.col("UICCODE").cast(pl.Utf8))

ICLRG5 = ICLRGD.join(ICLRGUIC, on="UICCODE", how="inner").filter(pl.col("UICCODE").is_not_null())

ICLRGNEW = ICLRG5.select([
    "BNKTYPE","CLRGDT","MICRPRE","ACCTNO","TRXCODE","AMOUNT","MICRPAY",
    "REJECT","TRXIND","TRXTYPE","DRDATE","UICCODE","BNKCODE","CHKNUM",
    "PAYBANK","PREBANK","CHKTYPE","BRANCH"
])

# outputs -> Parquet only
target_name = BASE_OUTPUT_PATH / f"IICLRG{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
ICLRGNEW.write_parquet(target_name)

# optional temp copy
ICLRGNEW_IICLRFTP = BASE_OUTPUT_PATH / f"ICLRGNEW_IICLRFTP{REPTMON}{NOWK}{REPTYEAR}.parquet"
ICLRGNEW.write_parquet(ICLRGNEW_IICLRFTP)

# ---- quick sanity check using DuckDB (uses the DataFrame) ----
con = duckdb.connect()
con.register("ICLRGNEW", ICLRGNEW.to_arrow())  # register as Arrow table
rowcount = con.execute("select count(*) from ICLRGNEW").fetchone()[0]
print("ICLRGNEW rows:", rowcount)
con.close()
