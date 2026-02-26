# !/usr/bin/env python3
"""
Program Name : KALMLIQ4
Purpose      : Filter and transform K3TBL data for liquidity reporting,
                assigning ITEM codes based on customer type (CTYPE format).
"""

import duckdb
import polars as pl
from datetime import date

# ---------------------------------------------------------------------------
# Path configuration
# ---------------------------------------------------------------------------
INPUT_PARQUET   = "data/K3TBL_{REPTMON}{NOWK}.parquet"   # resolved at runtime
CTYPE_PARQUET   = "data/CTYPE_FORMAT.parquet"             # lookup: UTCTP -> CUST code
OUTPUT_FILE     = "output/K3TBL3.txt"

# Runtime parameters – set these before execution
REPTMON = "202401"   # e.g. '202401'
NOWK    = "1"        # e.g. '1'
REPTDATE = date(2024, 1, 31)  # reporting date threshold

# ---------------------------------------------------------------------------
# Macro variable equivalents
# ---------------------------------------------------------------------------
IREP = ('01', '02', '11', '12', '81')
NREP = ('13', '17', '20', '60', '71', '72', '74', '76', '79', '85')

# ---------------------------------------------------------------------------
# Resolve actual input path
# ---------------------------------------------------------------------------
input_path = INPUT_PARQUET.format(REPTMON=REPTMON, NOWK=NOWK)

# ---------------------------------------------------------------------------
# Load source data via DuckDB
# ---------------------------------------------------------------------------
con = duckdb.connect()

df = con.execute(f"""
    SELECT *
    FROM read_parquet('{input_path}')
    WHERE UTREF = 'RRS'
      AND UTSTY = 'MGS'
      AND UTDLP = 'MSS'
""").pl()

# ---------------------------------------------------------------------------
# Load CTYPE format lookup (UTCTP -> customer type code string)
# CTYPE format: maps UTCTP value to a 2-char customer category code
# ---------------------------------------------------------------------------
ctype_df = con.execute(f"""
    SELECT START AS UTCTP, LABEL AS CUST
    FROM read_parquet('{CTYPE_PARQUET}')
""").pl()

con.close()

# ---------------------------------------------------------------------------
# Apply REPTDATE filter  (IF ISSDT > REPTDATE THEN DELETE)
# ---------------------------------------------------------------------------
reptdate_val = REPTDATE
df = df.filter(pl.col("ISSDT") <= pl.lit(reptdate_val))

# ---------------------------------------------------------------------------
# Initialise amount fields
# ---------------------------------------------------------------------------
df = df.with_columns([
    pl.lit(0).cast(pl.Float64).alias("AMTUSD"),
    pl.lit(0).cast(pl.Float64).alias("AMTSGD"),
])

# AMOUNT = (UTPCP * UTFCV) * 0.01
# AMOUNT = SUM(AMOUNT, UTAICT)   /* SALES PROCEEDS */
df = df.with_columns([
    (pl.col("UTPCP") * pl.col("UTFCV") * 0.01 + pl.col("UTAICT")).alias("AMOUNT"),
])

# ---------------------------------------------------------------------------
# Apply CTYPE format lookup  (PUT(UTCTP,$CTYPE.) -> CUST)
# ---------------------------------------------------------------------------
df = df.join(ctype_df, on="UTCTP", how="left")
# Where no match found, default CUST to empty string (mirrors SAS $CTYPE. miss)
df = df.with_columns([
    pl.col("CUST").fill_null("").alias("CUST"),
])

# ---------------------------------------------------------------------------
# Derive MATDT from UTIDT (IF UTIDT NE ' ' THEN MATDT = INPUT(UTIDT,YYMMDD10.))
# UTIDT is stored as a string 'YYYY-MM-DD' or similar; parse to date.
# ---------------------------------------------------------------------------
df = df.with_columns([
    pl.when(pl.col("UTIDT").is_not_null() & (pl.col("UTIDT").str.strip_chars() != ""))
      .then(pl.col("UTIDT").str.to_date("%Y-%m-%d", strict=False))
      .otherwise(None)
      .alias("MATDT"),
])

# ---------------------------------------------------------------------------
# Assign ITEM based on CUST category
# IF CUST IN NREP THEN ITEM='830'; ELSE
# IF CUST IN IREP THEN ITEM='820';
# ---------------------------------------------------------------------------
df = df.with_columns([
    pl.when(pl.col("CUST").is_in(list(NREP))).then(pl.lit("830"))
      .when(pl.col("CUST").is_in(list(IREP))).then(pl.lit("820"))
      .otherwise(None)
      .alias("ITEM"),
])

# ---------------------------------------------------------------------------
# PART = '95'
# ---------------------------------------------------------------------------
df = df.with_columns([
    pl.lit("95").alias("PART"),
])

# ---------------------------------------------------------------------------
# IF CUST NE '  '  (exclude blank / two-space customer codes)
# ---------------------------------------------------------------------------
df = df.filter(pl.col("CUST").str.strip_chars() != "")

# ---------------------------------------------------------------------------
# Column ordering to mirror SAS dataset column sequence
# Bring derived columns to front; preserve remaining source columns.
# ---------------------------------------------------------------------------
derived_cols = ["PART", "MATDT", "AMTUSD", "AMTSGD", "AMOUNT", "CUST", "ITEM"]
source_cols  = [c for c in df.columns if c not in derived_cols]
df = df.select(derived_cols + source_cols)

# ---------------------------------------------------------------------------
# Format MATDT as YYMMDD8. (YY/MM/DD -> 'YYMMDD' 8-char, e.g. '24/01/31')
# SAS YYMMDD8. format produces: YY/MM/DD
# ---------------------------------------------------------------------------
df = df.with_columns([
    pl.col("MATDT").dt.strftime("%y/%m/%d").alias("MATDT"),
])

# ---------------------------------------------------------------------------
# Write output as fixed-format text file
# ---------------------------------------------------------------------------
df.write_csv(OUTPUT_FILE, separator="|", null_value="")

print(f"K3TBL3 written to {OUTPUT_FILE}  ({len(df)} rows)")
