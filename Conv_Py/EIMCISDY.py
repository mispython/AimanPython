from pathlib import Path
import polars as pl

# Paths (adjust if needed)
#BASE_IN  = Path("input_parquet")
BASE_OUT = Path("output_parquet"); (BASE_OUT / "MONTH").mkdir(parents=True, exist_ok=True)

# Parquet inputs representing SAS sources
CUST_CUSTDLY_PATH = Path("/host/cis/parquet/CIS_CUST_DAILY/year=2025/month=9/day=17/data_0.parquet")   # has ACCTNOC CUSTNO CUSTNAME DOBDOR ALIAS ...
CISTAXID_PATH     = Path("/host/cis/parquet/CCRIS_TAXID_GDG/year=2025/month=9/day=10/data_0.parquet")            # has CUSTNO, RHOLD_IND

# 1) PROC SORT DATA=CUST.CUSTDLY OUT=CUSTDLY; BY CUSTNO;
CUSTDLY = (
    pl.read_parquet(CUST_CUSTDLY_PATH)
    .sort("CUSTNO")  # sort to emulate SAS BY behavior
)

# 2) DATA CISTAXID ... INPUT ... ;  (Assume already materialized as Parquet with the same column names)
CISTAXID = pl.read_parquet(CISTAXID_PATH)

# 3) PROC SORT NODUPKEY BY CUSTNO;  (keep first by sorted order)
CISTAXID = (
    CISTAXID
    .sort("CUSTNO")
    .unique(subset=["CUSTNO"], keep="first")
)

# 4) MERGE BY CUSTNO; IF A;  -> left join
CUSTDLY_MERGED = (
    CUSTDLY.join(
        CISTAXID.select(["CUSTNO", "RHOLD_IND"]),
        on="CUSTNO",
        how="left"
    )
    .select(["ACCTNOC", "CUSTNO", "RHOLD_IND", "CUSTNAME", "DOBDOR", "ALIAS"])
)

# 5) Write Parquet instead of CPORT/FTP
OUT_PATH = BASE_OUT / "MONTH" / "CUSTDLY.parquet"
CUSTDLY_MERGED.write_parquet(OUT_PATH)
