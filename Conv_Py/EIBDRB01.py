#!/usr/bin/env python3
"""
Program : EIBDRB01.py
Purpose : Daily Total Outstanding Balance/Account on FCY FD & Foreign
          Companies. Accumulates a running monthly store (RB01DP<mon>)
          from the DEPO (FCY FD) and WALK (Walker detail) daily feeds,
          then re-emits the full month-to-date listing on every run.

Original JCL/DD mapping:
  PGM   : SAP.BNM.PROGRAM   - session-level DD, never referenced by a
          %INCLUDE/CALL in the visible SAS body, so no functional import
          is created for it here.
  DEPO  : SAP.PBB.MNITB.DAILY(0) -> DEPO.FD  -> fcyfd<yy><mm><dd>.sas7bdat
  WALK  : SAP.PBB.DAILY.WALKER(0)-> WALK.WKDTL&REPTYY&REPTMON&REPTDAY
                                 -> wkdtl<yy><mm><dd>.sas7bdat
  MISFD : SAP.PBB.FCYFD      - declared DD, never referenced in the SAS
          body (no SET/MERGE uses it), so it is not read here either.
  STORE : SAP.PBB.DP.SASDATA -> RB01DP<mm>.parquet (running monthly cache)
  SASLIST: DCB=(LRECL=300,RECFM=FB,...) - RECFM=FB (NOT FBA), so this
          report carries NO ASA carriage-control character. Output is a
          plain fixed-width (300-byte) sequential listing.
"""

import gc
import re
from datetime import date
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

INPUT_DEPO_DIR = BASE_DIR / "input" / "prod" / "deposit_fcy_d"
INPUT_WALK_DIR = BASE_DIR / "input" / "prod" / "walker"

CACHE_DIR = BASE_DIR / "cache" / "EIBDRB01"
STORE_DIR = BASE_DIR / "store" / "EIBDRB01"
OUTPUT_DIR = BASE_DIR / "output" / "EIBDRBDP"

CACHE_DIR.mkdir(parents=True, exist_ok=True)
STORE_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# REPORT CONSTANTS
# ============================================================================
LRECL = 300

# Order matches the original PUT statement column sequence exactly.
CURRENCY_CODES = [
    "USD", "NZD", "AUD", "GBP", "HKD", "SGD",
    "EUR", "JPY", "CAD", "CNY", "CHF", "THB",
]
# @ column (1-based) for each currency in the detail/header lines.
CURRENCY_COLS = [15, 30, 45, 60, 75, 90, 105, 120, 135, 150, 165, 180]

# ============================================================================
# STEP 1: REPORT DATE  (DATA REPTDATE; SET DEPO.REPTDATE;)
# No reptdate.parquet exists — derive everything from REPTDATE.py.
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values(year_format="%Y")
REPTDATE = reptdate_values.reptdate            # date object
REPTYEAR = reptdate_values.reptyear            # 4-digit year, e.g. "2026"
REPTYY   = REPTDATE.strftime("%y")             # 2-digit year, e.g. "26"
REPTMON  = reptdate_values.reptmon             # zero-padded month
REPTDAY  = reptdate_values.reptday             # zero-padded day
RDATE    = REPTDATE.strftime("%d/%m/%y")       # DDMMYY8. equivalent

REPTYEAR_INT = REPTDATE.year
REPTMON_INT  = REPTDATE.month

print(f"  Report date : {RDATE}")
print(f"  Year/Mon/Day: {REPTYEAR}/{REPTMON}/{REPTDAY}")

# ============================================================================
# STEP 2: RESOLVE INPUT FILE NAMES
# The daily filenames are fully determined by REPTDATE, so they are built
# directly rather than via input_date.get_latest_file(). input_date.py's
# generic patterns are tuned for mmwwyy/ddmmyy/mmddyy orderings and would
# mis-parse a yymmdd filename (and could not reliably tell the daily
# "fcyfd260722" form apart from the weekly "fcyfd07326" form), so a direct,
# regex-validated construction is used instead to guarantee the correct
# daily file is selected.
# ============================================================================
print("\nStep 2: Resolving DEPO (FCY FD) / WALK (Walker) file names...")

FCY_FILE = INPUT_DEPO_DIR / f"fcyfd{REPTYY}{REPTMON}{REPTDAY}.sas7bdat"         # /dwh/dp_fcy (daily)
WK_FILE  = INPUT_WALK_DIR / f"wkdtl{REPTYY}{REPTMON}{REPTDAY}.sas7bdat"         # /dwh/dwh_m/tempsource/walker (daily)

if not re.fullmatch(r"fcyfd\d{6}\.sas7bdat", FCY_FILE.name):
    raise ValueError(f"FCY filename does not match daily yymmdd pattern: {FCY_FILE.name}")
if not FCY_FILE.exists():
    raise FileNotFoundError(f"FCY FD file not found: {FCY_FILE}")
if not WK_FILE.exists():
    raise FileNotFoundError(f"WALK detail file not found: {WK_FILE}")

print(f"  FCY (DEPO.FD)     : {FCY_FILE.name}")
print(f"  WK  (WALK.WKDTL)  : {WK_FILE.name}")

# ============================================================================
# HELPER: CACHE STAMP / SAS -> PARQUET STREAMING CONVERSION
# Mirrors the pattern established in EIBDLN1M.py.
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


def sas_to_parquet(sas_path: Path, cache_path: Path, tag: str, chunk_rows: int = 500_000) -> None:
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0

    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=chunk_rows)
    for chunk in reader:
        table = pa.Table.from_pandas(chunk, preserve_index=False)

        if schema is None:
            schema = table.schema
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")
        else:
            cast_arrays = []
            for field in schema:
                col = table.column(field.name)
                if col.type != field.type:
                    try:
                        col = col.cast(field.type, safe=False)
                    except Exception as e:
                        print(f"  [{tag}] WARNING: cannot cast '{field.name}' "
                              f"from {col.type} to {field.type}: {e} - filling nulls")
                        col = pa.nulls(len(col), type=field.type)
                cast_arrays.append(col)
            table = pa.Table.from_arrays(cast_arrays, schema=schema)

        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer:
        writer.close()
    print(f"  [{tag}] Done - {total:,} rows cached.")


# ============================================================================
# STEP 3: CACHE SAS FILES TO PARQUET
# ============================================================================
print("\nStep 3: Caching SAS files to Parquet (if needed)...")

FCY_CACHE = CACHE_DIR / f"{FCY_FILE.stem}.parquet"
WK_CACHE  = CACHE_DIR / f"{WK_FILE.stem}.parquet"

if not _cache_is_fresh(FCY_FILE, FCY_CACHE):
    sas_to_parquet(FCY_FILE, FCY_CACHE, "FCY")
else:
    print("  [FCY] Cache fresh - skipping conversion.")

if not _cache_is_fresh(WK_FILE, WK_CACHE):
    sas_to_parquet(WK_FILE, WK_CACHE, "WK")
else:
    print("  [WK ] Cache fresh - skipping conversion.")

# ============================================================================
# HELPER: packed-date decode
# Replicates: IF x NOT IN (0,.) THEN y = INPUT(SUBSTR(PUT(x,Z11.),1,8),MMDDYY8.);
#             ELSE y = 0;
# YEARCUTOFF=1950 two-digit year rule applied (yy >= 50 -> 19xx, else 20xx).
# ============================================================================
def _decode_packed_date(value) -> Optional[date]:
    if value is None:
        return None
    try:
        ival = int(value)
    except (TypeError, ValueError):
        return None
    if ival == 0:
        return None

    z11 = f"{ival:011d}"
    field6 = z11[:6]           # MMDDYY8. reads MM DD YY from the 8-char field
    mm = int(field6[0:2])
    dd = int(field6[2:4])
    yy = int(field6[4:6])
    year = 1900 + yy if yy >= 50 else 2000 + yy

    try:
        return date(year, mm, dd)
    except ValueError:
        return None


# ============================================================================
# STEP 4: BUILD FCY  (DATA FCY; SET DEPO.FD; WHERE CURCODE NE 'MYR' AND CURBAL GE 0;)
# ============================================================================
print("\nStep 4: Building FCY (FD) dataset...")

con = duckdb.connect(database=":memory:")
fcy_raw = con.execute(f"""
    SELECT
        CAST(CURCODE  AS VARCHAR) AS CURCODE,
        CAST(CURBAL   AS DOUBLE)  AS CURBAL,
        CAST(OPENDT   AS DOUBLE)  AS OPENDT,
        CAST(CLOSEDT  AS DOUBLE)  AS CLOSEDT,
        CAST(OPENIND  AS VARCHAR) AS OPENIND
    FROM read_parquet('{FCY_CACHE}')
    WHERE TRIM(CURCODE) != 'MYR' AND CURBAL >= 0
""").pl()
con.close()

fcy = fcy_raw.with_columns([
    (pl.col("CURBAL") / 1000.0).alias("CURBAL"),
    pl.col("OPENDT").map_elements(_decode_packed_date, return_dtype=pl.Date).alias("OPENDT_D"),
    pl.col("CLOSEDT").map_elements(_decode_packed_date, return_dtype=pl.Date).alias("CLOSEDT_D"),
])

fcy = fcy.with_columns([
    pl.when(
        (pl.col("OPENDT_D").dt.year() == REPTYEAR_INT)
        & (pl.col("OPENDT_D").dt.month() == REPTMON_INT)
        & (pl.col("CLOSEDT_D").dt.year() == pl.col("OPENDT_D").dt.year())
        & (pl.col("CLOSEDT_D").dt.month() == pl.col("OPENDT_D").dt.month())
    ).then(1).otherwise(0).alias("OPCLMH"),
    pl.when(~pl.col("OPENIND").str.strip_chars().is_in(["B", "C", "P"]))
      .then(1).otherwise(0).alias("OSACCT"),
])

fcy = fcy.with_columns(
    pl.when((pl.col("OPCLMH") != 1) & (pl.col("CURBAL") > 0))
      .then(1).otherwise(0).alias("NOACCT")
)

fcy = fcy.with_columns([
    pl.lit(3).alias("ID"),
    pl.lit(REPTDATE).alias("REPTDATE"),
]).select(["REPTDATE", "ID", "CURCODE", "CURBAL", "NOACCT", "OSACCT"])

print(f"  FCY rows: {len(fcy):,}")

# ============================================================================
# STEP 5: BUILD WK  (DATA WK; SET WALK.WKDTL...;)
# ============================================================================
print("\nStep 5: Building WK (Walker) dataset...")

con = duckdb.connect(database=":memory:")
wk_raw = con.execute(f"""
    SELECT
        CAST(CURR   AS VARCHAR) AS CURR,
        CAST(CURBAL AS DOUBLE)  AS CURBAL
    FROM read_parquet('{WK_CACHE}')
""").pl()
con.close()

wk = wk_raw.with_columns([
    (pl.col("CURBAL") / 1000.0 * -1).alias("CURBAL"),
    pl.col("CURR").str.slice(3, 3).alias("CURCODE"),
    pl.lit(2).alias("ID"),
    pl.lit(REPTDATE).alias("REPTDATE"),
    pl.lit(0).alias("NOACCT"),
    pl.lit(0).alias("OSACCT"),
]).select(["REPTDATE", "ID", "CURCODE", "CURBAL", "NOACCT", "OSACCT"])

print(f"  WK rows: {len(wk):,}")

# ============================================================================
# STEP 6: COMBINE, SUMMARISE, DUPLICATE TO ID=1, RE-SUMMARISE, PIVOT
# DATA FCY; SET FCY WK; -> PROC SORT/SUMMARY BY REPTDATE ID CURCODE SUM=
# -> CURBAL = ROUND(CURBAL/1000,.001); OUTPUT; ID=1; OUTPUT;
# -> PROC SUMMARY BY REPTDATE ID (TOTAL) / BY REPTDATE ID CURCODE (currency)
# -> PROC TRANSPOSE + MERGE FD TOTAL
# ============================================================================
print("\nStep 6: Aggregating and building RBDP...")

combined = pl.concat([fcy, wk], how="vertical")

con = duckdb.connect(database=":memory:")
con.register("combined", combined)

summed = con.execute("""
    SELECT REPTDATE, ID, CURCODE,
           SUM(CURBAL)  AS CURBAL,
           SUM(NOACCT)  AS NOACCT,
           SUM(OSACCT)  AS OSACCT
    FROM combined
    GROUP BY REPTDATE, ID, CURCODE
""").pl()
con.close()

# Millions conversion + duplicate every record under ID=1 (combined A+B total).
summed = summed.with_columns(
    (pl.col("CURBAL") / 1000.0).round(3).alias("CURBAL")
)
doubled = pl.concat([summed, summed.with_columns(pl.lit(1).alias("ID"))], how="vertical")

con = duckdb.connect(database=":memory:")
con.register("doubled", doubled)

total = con.execute("""
    SELECT REPTDATE, ID,
           SUM(CURBAL) AS TOTFCYFD,
           SUM(NOACCT) AS NOACCT,
           SUM(OSACCT) AS OSACCT
    FROM doubled
    GROUP BY REPTDATE, ID
""").pl()

# Currency breakdown replicates PROC TRANSPOSE without needing a generic
# pivot: only the twelve currencies the original report names are broken
# out (any other currency code still feeds into TOTFCYFD above).
currency_case = ",\n           ".join(
    f"SUM(CASE WHEN CURCODE = '{code}' THEN CURBAL END) AS {code}"
    for code in CURRENCY_CODES
)
currency_wide = con.execute(f"""
    SELECT REPTDATE, ID,
           {currency_case}
    FROM doubled
    GROUP BY REPTDATE, ID
""").pl()
con.close()

rbdp = total.join(currency_wide, on=["REPTDATE", "ID"], how="inner")
rbdp = rbdp.with_columns([
    pl.col(code).fill_null(0.0) for code in CURRENCY_CODES
])

del fcy, wk, combined, summed, doubled, total, currency_wide
gc.collect()

print(f"  RBDP rows: {len(rbdp):,}")

# ============================================================================
# STEP 7: APPEND TO MONTHLY STORE  (%MACRO APPEND)
# Day 01 -> overwrite the monthly store with today's rows.
# Otherwise -> prepend today's rows, drop duplicate REPTDATE+ID keeping the
#              new (first) occurrence, then sort BY REPTDATE ID.
# ============================================================================
print("\nStep 7: Updating monthly store (RB01DP)...")

STORE_FILE = STORE_DIR / f"RB01DP{REPTMON}.parquet"         # Generated on 1st of the month (parquet file)

if REPTDAY == "01":
    store_df = rbdp
else:
    if STORE_FILE.exists():
        existing_df = pl.read_parquet(STORE_FILE)
        store_df = pl.concat([rbdp, existing_df], how="vertical_relaxed")
    else:
        store_df = rbdp
    store_df = store_df.unique(subset=["REPTDATE", "ID"], keep="first", maintain_order=True)

store_df = store_df.sort(["REPTDATE", "ID"])
store_df.write_parquet(STORE_FILE)

print(f"  Store file : {STORE_FILE}")
print(f"  Store rows : {len(store_df):,}")

del rbdp
gc.collect()

# ============================================================================
# STEP 8: GENERATE REPORT  (DATA _NULL_; SET STORE.RB01DP&REPTMON; FILE SASLIST;)
# DCB is RECFM=FB (not FBA) -> no ASA carriage-control character; this is a
# plain fixed-width (LRECL=300) sequential listing with no page-overflow
# logic (no HEADER=NEWPAGE / _PAGE_ in the source), so PAGE_SIZE pagination
# does not apply here.
# ============================================================================
print("\nStep 8: Generating report...")

OUTPUT_FILE = OUTPUT_DIR / f"EIBDRB01_{REPTMON}.txt"


def _comma(value, width: int, decimals: int = 0) -> str:
    """COMMA<width>.<decimals> format. OPTIONS MISSING=0 -> missing prints as 0."""
    v = 0.0 if value is None else float(value)
    if decimals > 0:
        s = f"{v:,.{decimals}f}"
    else:
        s = f"{v:,.0f}"
    return s.ljust(width)


def _place(buf: list, col: int, text: str) -> None:
    """Write *text* left-to-right starting at 1-based column *col*."""
    start = col - 1
    end = start + len(text)
    if end > len(buf):
        text = text[: len(buf) - start]
        end = len(buf)
    buf[start:end] = list(text)


def _new_line() -> list:
    return [" "] * LRECL


def _build_header_lines() -> list:
    lines = []

    l = _new_line(); _place(l, 1, "REPORT ID : EIBDRB01"); lines.append(l)
    l = _new_line(); _place(l, 1, "DAILY TOTAL OUTSTANDING BALANCE/ACCOUNT ON FCY FD & FOREIGN COMPANIES"); lines.append(l)
    l = _new_line(); _place(l, 1, f"AS AT {RDATE}"); lines.append(l)
    l = _new_line(); lines.append(l)  # blank line

    l = _new_line()
    _place(l, 2, "DATE")
    _place(l, 60, "OUTSTANDING AMOUNT (RM'MIL) (3-decimal)")
    lines.append(l)

    l = _new_line(); _place(l, 213, "TOTAL NO OF O/S ACCT"); lines.append(l)

    l = _new_line()
    for code, col in zip(CURRENCY_CODES, CURRENCY_COLS):
        _place(l, col, code)
    _place(l, 195, "TOT AMT O/S RM")
    _place(l, 213, "NO OF A/C")
    _place(l, 227, "NO OF A/C")
    lines.append(l)

    l = _new_line()
    _place(l, 2, "(A)+(B)")
    for col in CURRENCY_COLS:
        _place(l, col, "TOTAL")
    _place(l, 195, "TOTAL")
    _place(l, 213, "(EXCL")
    _place(l, 227, "(INCL")
    lines.append(l)

    l = _new_line()
    _place(l, 4, "(A)")
    for col in CURRENCY_COLS:
        _place(l, col, "FRGN CO.")
    _place(l, 195, "FRGN CO.")
    _place(l, 213, "ZERO")
    _place(l, 227, "ZERO")
    lines.append(l)

    l = _new_line()
    _place(l, 4, "(B)")
    for col in CURRENCY_COLS:
        _place(l, col, "FCY FD")
    _place(l, 195, "FCY FD")
    _place(l, 213, "BALANCE)")
    _place(l, 227, "BALANCE)")
    lines.append(l)

    l = _new_line(); _place(l, 1, "-" * 275); lines.append(l)

    return lines


output_lines: list = _build_header_lines()

current_rpdate = None
for row in store_df.iter_rows(named=True):
    if row["ID"] == 1:
        current_rpdate = row["REPTDATE"].strftime("%d/%m/%y")

    l = _new_line()
    _place(l, 2, f"{(current_rpdate or ''):<8s}")
    for code, col in zip(CURRENCY_CODES, CURRENCY_COLS):
        _place(l, col, _comma(row.get(code), 16, 3))
    _place(l, 195, _comma(row.get("TOTFCYFD"), 16, 3))
    _place(l, 213, _comma(row.get("NOACCT"), 10, 0))
    _place(l, 227, _comma(row.get("OSACCT"), 10, 0))
    output_lines.append(l)

    if row["ID"] == 3:
        l = _new_line()
        _place(l, 1, "-" * 275)
        output_lines.append(l)

with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for line in output_lines:
        fh.write("".join(line) + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(output_lines):,}")

del store_df
gc.collect()

print("\nEIBDRB01 complete.\n")
