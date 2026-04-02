import polars as pl
from datetime import datetime, timedelta
from pathlib import Path

BASE_INPUT_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/Outsource/input") # Folder for source files
BASE_OUTPUT_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output") # Folder for output files
BASE_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

ICLRGNEW_DATA_PATH = BASE_OUTPUT_PATH / "ICLRGNEW"

# File paths
REPTDATE_LOAN = BASE_INPUT_PATH / "LOAN.REPTDATE"
RPTFILE2 = BASE_INPUT_PATH / "RPTFILE2"
RPTFILE = BASE_INPUT_PATH / "RPTFILE"
BRHCODE = BASE_INPUT_PATH / "BRHCODE"

REPTDATE_df = pl.read_csv(REPTDATE_LOAN)
REPTDATE_value = REPTDATE_df["REPTDATE"][0]
REPTDATE = datetime.strptime(REPTDATE_value, "%Y-%m-%d")

day = REPTDATE.day
month = REPTDATE.month
year = REPTDATE.year

if 1 <= day <= 8:
    NOWK = "1"
elif 9 <= day <= 15:
    NOWK = "2"
elif 16 <= day <= 22:
    NOWK = "3"
else:
    NOWK = "4"

REPTYEAR = str(year)[-2:]
REPTMON = f"{month:02d}" 
REPTDAY = f"{day:02d}" 
RDATE = REPTDATE.strftime("%Y%m%d") 

print(REPTDATE)

ICLRGORI = pl.read_csv(
    RPTFILE2,
    has_header=False,
    columns=[
        "BNKTYPE", "BNKCODE", "YY", "MM ", "DD ", "CHKNUM", "PAYBANK", "MICRPAY", 
        "ACCTNO", "TRXCODE", "AMOUNT", "PREBANK", "MICRPRE", "CHKTYPE", "BRCODE", "UICCODE"
    ],
    dtypes={
        "YY": pl.Int32,
        "MM": pl.Int32,  
        "DD": pl.Int32
    }    
)

ICLRGORI = ICLRGORI.with_columns([
    pl.date(pl.col("YY"), pl.col("MM"), pl.col("DD")).alias("CLRGDT")
]).sort("MICRPAY")

BR = pl.read_csv(
    BRHCODE,
    has_header=False,
    columns=[
        "MICRPAY", "BRANCH"
    ]   
)

#BR = BR.filter((pl.col("BRANCH") >= 1) & (pl.col("BRANCH") <= 500))
BR = BR.sort("MICRPAY")

ICLRG1 = ICLRGORI.join(BR, on="MICRPAY", how="inner")

ICLRGD = ICLRG1.select(["BNKTYPE", "BNKCODE", "CLRGDT", "CHKNUM", "PAYBANK", "CHKTYPE"                          
    "MICRPAY", "ACCTNO", "AMOUNT", "TRXCODE", "PREBANK", "MICRPRE", "BRCODE"                   
    "UICCODE", "BRANCH"
])

ICLRGD = ICLRGD.sort("UICCODE")

ICLRGD_out = BASE_OUTPUT_PATH / f"ICLRGA{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
ICLRGD.write_parquet(ICLRGD_out)

ICLRGUIC = pl.read_csv(
    RPTFILE,
    has_header=False,
    columns=[
        "RCRDTYPE", "CHKNUM", "MICRPAY", "ACCTNO", "TRXCODE", "TRXAMT", "MICR", 
        "REJECT", "TRXIND", "TRXTYPE", "YY2", "MM2", "DD2", "UICCODE"      
    ],
    dtypes={
        "YY2": pl.Int32,
        "MM2": pl.Int32,  
        "DD2": pl.Int32
    }  
)
ICLRGUIC = ICLRGUIC.filter(pl.col("RCRDTYPE")==2)
ICLRGUIC = ICLRGUIC.with_columns([
    pl.date(pl.col("YY2"), pl.col("MM2"), pl.col("DD2")).alias("DRDATE")
]).sort("UICCODE")

ICLRGS = ICLRGD.join(ICLRGUIC, on="UICCODE", how = "left"
).filter(pl.col("UICCODE").is_not_null())

ICLRGNEW = ICLRGS.select([
    "BNKTYPE", "CLRGDT", "MICRPRE", "ACCTNO", "TRXCODE", "AMOUNT", "MICRPAY",                   
    "REJECT", "TRXIND", "TRXTYPE", "DRDATE", "UICCODE", "BNKCODE", "CHKNUM", 
    "PAYBANK", "PREBANK", "CHKTYPE", "BRANCH"
])

target_name = BASE_OUTPUT_PATH / f"ICLRG{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
ICLRGNEW.write_parquet(target_name)

ICLRGNEW_ICLRG = ICLRGNEW

ICLRGNEW_ICLRG.write_parquet(f"{target_name}.parquet")

# Save TEMP copy
ICLRGNEW_ICLRGFTP = f"ICLRGNEW_ICLRGFTP{REPTMON}{NOWK}{REPTYEAR}"
ICLRGNEW_ICLRG.write.parquet(f"{ICLRGNEW_ICLRGFTP}.parquet")

# Export
ICLRGNEW_ICLRG.write.csv("TRANFILE.csv.gz")
