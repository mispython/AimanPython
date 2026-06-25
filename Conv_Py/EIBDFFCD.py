from pathlib import Path
import polars as pl

IN  = Path("parquet_input")
OUT = Path("parquet_output")
OUT.mkdir(parents=True, exist_ok=True)

# Inputs
FD = pl.concat([pl.read_parquet(f) for f in sorted((IN / "MNIFD" / "DAILY").glob("*.parquet"))])
FOFMT = pl.read_parquet(IN / "FCYFD" / "FOFMT.parquet")  # CNTLOUT from LIB=FCYFD

# Formats: $FORATE. and USD rate
FORATE = (FOFMT.filter(pl.col("FMTNAME")=="$FORATE")
               .select(pl.col("START").alias("CURCODE"),
                       pl.col("LABEL").cast(pl.Float64).alias("RATE")))
USD_RATE = FORATE.filter(pl.col("CURCODE")=="USD").select("RATE").to_series()[0]

# PROC SORT BY ACCTNO CDNO
FD = FD.sort(["ACCTNO","CDNO"])

# DATA FD.FD logic
FD = (FD.join(FORATE, on="CURCODE", how="left")
        .with_columns([
            pl.when(pl.col("CURCODE")!="MYR").then(pl.col("CURBAL")).otherwise(pl.lit(None)).alias("FORBAL"),
            pl.when(pl.col("CURCODE")!="MYR").then((pl.col("CURBAL")*pl.col("RATE")).round(2)).otherwise(pl.col("CURBAL")).alias("CURBAL")
        ])
        .drop("RATE")
        .with_columns((pl.col("CURBAL")/pl.lit(USD_RATE)).alias("CURBALUS"))
)

# Write FD.FD
out_fd = OUT / "FD" / "FD.parquet"
out_fd.parent.mkdir(parents=True, exist_ok=True)
FD.write_parquet(out_fd)

# PROC PRINT WHERE CURCODE NE 'MYR' (optional preview)
# print(FD.filter(pl.col("CURCODE")!="MYR").head(100))
