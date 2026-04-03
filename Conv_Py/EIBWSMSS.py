from pathlib import Path
import polars as pl

# ---------- I/O paths ----------
BASE_IN  = Path("input_parquet")
BASE_OUT = Path("output_parquet")
(BASE_OUT / "SIGNA").mkdir(parents=True, exist_ok=True)

def write_tbl(df: pl.DataFrame, lib: str, name: str):
    outdir = BASE_OUT / lib
    outdir.mkdir(parents=True, exist_ok=True)
    df.write_parquet(outdir / f"{name}.parquet")

# helper: 1-based slice like SAS @pos
def fw_slice(expr: pl.Expr, pos1: int, width: int) -> pl.Expr:
    return expr.str.slice(pos1 - 1, width)

# read SMSTXT (fixed-width lines), provided as Parquet with a single 'RAW' column
SMSTXT_RAW = pl.read_parquet(BASE_IN / "SMSTXT" / "SMSTXT.parquet")

# allowed modes exactly as in SAS (already uppercase)
_ALLOWED = {
    'ANY 1 OF 1 TO SIGN',
    'ANY 1 OF 2 TO SIGN',
    'ANY 1 OF 3 TO SIGN',
    'ANY 1 OF 4 TO SIGN',
    'ANY 2 OF 2 TO SIGN',
    'ANY 2 OF 3 TO SIGN',
    'ANY 2 OF 4 TO SIGN',
    'ANY 3 OF 3 TO SIGN',
    'ANY 3 OF 4 TO SIGN',
    'ANY 4 OF 4 TO SIGN',
    ' '  # SAS treats a blank; we emulate by checking trimmed length == 0 below
}

# build columns per SAS INPUT pointers
SIGNA_SMSACC = (
    SMSTXT_RAW.with_columns([
        # @001 10.  (numeric)
        fw_slice(pl.col("RAW"), 1, 10).str.strip_chars().cast(pl.Int64, strict=False).alias("ACCTNO"),
        # @011 $1.
        fw_slice(pl.col("RAW"), 11, 1).alias("ESIGNATURE"),
        # @012 $UPCASE20.  -> take 20 chars and uppercase
        fw_slice(pl.col("RAW"), 12, 20).str.to_uppercase().alias("CONDIMODE"),
    ])
    # mirror SAS: if CONDIMODE NOT IN (list OR blank) -> 'OTHERS'
    .with_columns(
        pl.when(
            # treat "blank" like SAS: if trimmed length == 0, allow it
            (pl.col("CONDIMODE").str.strip_chars().str.len_chars() == 0)
            | (pl.col("CONDIMODE").is_in(list(_ALLOWED)))
        )
        .then(pl.col("CONDIMODE"))
        .otherwise(pl.lit("OTHERS"))
        .alias("CONDIMODE")
    )
)

# write dataset equivalent to SIGNA.SMSACC
#write_tbl(SIGNA_SMSACC, "SIGNA", "SMSACC")

SIGNA_SMSACC_NODUP = (
    SIGNA_SMSACC
    .sort("ACCTNO")                           # PROC SORT BY ACCTNO
    .unique(subset=["ACCTNO"], keep="first")  # NODUPKEY: keep first in BY group
)
write_tbl(SIGNA_SMSACC_NODUP, "SIGNA", "SMSACC")
