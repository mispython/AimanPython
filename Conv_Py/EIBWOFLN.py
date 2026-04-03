from __future__ import annotations
from pathlib import Path
import polars as pl
import duckdb               # required by your stack
import pyarrow.parquet as pq  # required by your stack

# =============================================================================
# Paths (adjust to your environment only here)
# =============================================================================
BASE_INPUT  = Path("Data_Warehouse/MIS/Job/WOF/input")
BASE_OUTPUT = Path("Data_Warehouse/MIS/Job/WOF/output")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

# Parquet inputs equivalent to the JCL DD names:
#   LN01 -> RBP2.B033.WDWO.MIS
#   LN02 -> RBP2.B033.LN.WO.MISFIL2(0)
LN01_PATH = BASE_INPUT / "LN01.parquet"
LN02_PATH = BASE_INPUT / "LN02.parquet"

# Output locations mirroring SAS libraries/datasets
LNWOF_PATH = BASE_OUTPUT / "LNWOF" / "LNWOF.parquet"
ILNWOF_PATH = BASE_OUTPUT / "ILNWOF" / "ILNWOF.parquet"
WOMV_PATH  = BASE_OUTPUT / "WOMV"  / "WOMOVE.parquet"
IWOMV_PATH = BASE_OUTPUT / "IWOMV" / "IWOMOVE.parquet"
LNWOF_PATH.parent.mkdir(parents=True, exist_ok=True)
ILNWOF_PATH.parent.mkdir(parents=True, exist_ok=True)
WOMV_PATH.parent.mkdir(parents=True, exist_ok=True)
IWOMV_PATH.parent.mkdir(parents=True, exist_ok=True)

# =============================================================================
# Input 1: LN01 -> split into LNWOF and ILNWOF by COSTCTR (3000..4999 inclusive)
# =============================================================================
# Required LN01 columns based on SAS INPUT layout:
# ACCTNO, NOTENO, PRODUCT, CENSUS_TRT, PAYMENT, WRITE_DOWN_BAL, NBDR, RC,
# NAI, ORICODE, IISR, COSTCTR, REFNOTENO
LN01 = pl.read_parquet(LN01_PATH)

req_ln01 = [
    "ACCTNO","NOTENO","PRODUCT","CENSUS_TRT","PAYMENT","WRITE_DOWN_BAL",
    "NBDR","RC","NAI","ORICODE","IISR","COSTCTR","REFNOTENO"
]
missing1 = [c for c in req_ln01 if c not in LN01.columns]
if missing1:
    # Create missing columns as nulls to preserve schema/logic
    LN01 = LN01.with_columns([pl.lit(None).alias(c) for c in missing1])

# Split by COSTCTR range (inclusive)
in_mis = (pl.col("COSTCTR") >= 3000) & (pl.col("COSTCTR") <= 4999)
ILNWOF = LN01.filter(in_mis)
LNWOF  = LN01.filter(~in_mis)

# Write outputs
ILNWOF.write_parquet(ILNWOF_PATH)
LNWOF.write_parquet(LNWOF_PATH)

# =============================================================================
# Input 2: LN02 -> build TRANDATE = MDY(TRMM,TRDD,TRYR), drop TRYR/TRMM/TRDD
#          then split into IWOMV (3000..4999) and WOMV (else)
# =============================================================================
# Required LN02 columns based on SAS INPUT layout:
# ACCTNO, NOTENO, PRODUCT, ORIPRODUCT, PAYMENT, WDB_BFR_PAY, WDB_AFT_PAY,
# PAY_WDB, BDR_BFR_PAY, PAY_BDR, BDR_AFT_PAY, RC, NAI, TRYR, TRMM, TRDD, COSTCTR
LN02 = pl.read_parquet(LN02_PATH)

req_ln02 = [
    "ACCTNO","NOTENO","PRODUCT","ORIPRODUCT","PAYMENT","WDB_BFR_PAY","WDB_AFT_PAY",
    "PAY_WDB","BDR_BFR_PAY","PAY_BDR","BDR_AFT_PAY","RC","NAI",
    "TRYR","TRMM","TRDD","COSTCTR"
]
missing2 = [c for c in req_ln02 if c not in LN02.columns]
if missing2:
    LN02 = LN02.with_columns([pl.lit(None).alias(c) for c in missing2])

# TRANDATE = MDY(TRMM,TRDD,TRYR) ; keep as Date to match SAS date type semantics
LN02 = LN02.with_columns(
    pl.datetime(
        pl.col("TRYR").cast(pl.Int32, strict=False),
        pl.col("TRMM").cast(pl.Int32, strict=False),
        pl.col("TRDD").cast(pl.Int32, strict=False),
    ).cast(pl.Date).alias("TRANDATE")
).drop(["TRYR","TRMM","TRDD"])

# Split by COSTCTR (inclusive)
IWOMV = LN02.filter(in_mis)    # reuse the same predicate (defined above)
WOMV  = LN02.filter(~in_mis)

# Write outputs
IWOMV.write_parquet(IWOMV_PATH)
WOMV.write_parquet(WOMV_PATH)

print("DONE:")
print(" -", ILNWOF_PATH)
print(" -", LNWOF_PATH)
print(" -", IWOMV_PATH)
print(" -", WOMV_PATH)
