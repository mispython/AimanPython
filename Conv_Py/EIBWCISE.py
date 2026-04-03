from pathlib import Path
from datetime import date
import polars as pl

# ---------- paths ----------
BASE_IN  = Path("input_parquet")
BASE_OUT = Path("output_parquet")
BASE_OUT.mkdir(parents=True, exist_ok=True)

def write_tbl(df: pl.DataFrame, lib: str, name: str):
    outdir = BASE_OUT / lib
    outdir.mkdir(parents=True, exist_ok=True)
    df.write_parquet(outdir / f"{name}.parquet")

# MNICS.REPTDATE
MNICS_REPTDATE = pl.DataFrame({"REPTDATE": [date.today()]})
write_tbl(MNICS_REPTDATE, "MNICS", "REPTDATE")

# helper: 1-based slice like SAS column pointers
def fw_slice(col: pl.Expr, pos1: int, width: int) -> pl.Expr:
    return col.str.slice(pos1 - 1, width)

# Read the Parquet that replaces `INFILE CIS;`
# Assumed schema: one column RAW (Utf8), each row is one fixed-width record
CIS_RAW = pl.read_parquet(BASE_IN / "CIS" / "CIS.parquet")

# Build columns per SAS positions
t = (
    CIS_RAW
    .with_columns([
        fw_slice(pl.col("RAW"), 2, 10).alias("ACCTN"),                      # @2 $10.
        fw_slice(pl.col("RAW"), 21, 40).str.rstrip().alias("NAME"),         # @21 $40.
        fw_slice(pl.col("RAW"), 61, 20).str.rstrip().alias("PP_ALIAS"),     # @61 $20.
        fw_slice(pl.col("RAW"), 81, 5).str.strip_chars().cast(pl.Int64, strict=False).alias("SIC"),   # @81 5.
        fw_slice(pl.col("RAW"), 86, 1).alias("SEX"),                        # @86 $1.
        fw_slice(pl.col("RAW"), 87, 20).str.rstrip().alias("DIRNIC"),       # @87 $20.
        fw_slice(pl.col("RAW"), 107, 9).str.strip_chars().cast(pl.Int64, strict=False).alias("DIROIC"), # @107 9.
        fw_slice(pl.col("RAW"), 116, 40).str.rstrip().alias("DIRNAME"),     # @116 $40.
    ])
    # SAS: IF VERIFY(ACCTN, '0123456789') ^= 0 THEN DELETE;
    # -> keep rows where ACCTN is all digits (no spaces)
    .filter(pl.col("ACCTN").str.match(r"^\d{1,10}$"))
    # ACCTNO = INPUT(ACCTN,10.);
    .with_columns(pl.col("ACCTN").cast(pl.Int64, strict=False).alias("ACCTNO"))
    # drop ACCTN, NUM (NUM was only a retained string in SAS; not materialized here)
    .drop("ACCTN")
)

# PROC SORT BY ACCTNO → output MNICS.CIS
MNICS_CIS = t.sort("ACCTNO")
write_tbl(MNICS_CIS, "MNICS", "CIS")
