#!/usr/bin/env python3
"""
Program : EIBDDCMG.py
Purpose : Store Daily Position for EBANK/WALKER/DEPOSIT/EQUATION
          (Bank's Total Deposits - RM'000, gold GIA balance, NID/PMMD
          proceeds, and FX rates), appended into a running monthly
          DCMG dataset and printed as a month-to-date listing.
"""

import gc
import csv
import math
import re
from pathlib import Path
from datetime import date
from typing import Optional

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values
from input_date import get_latest_file
from output_date import build_output_file

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

# INPUT_DIR  = Path("/dwh")                               # RNID, IRNID, UTFX, IUTFX (.sas7bdat)
HOST_DIR    = Path("/stgsrcsys/host/uat/AII")           # DPGIA, EQ, IEQ (.txt), EFORATE (.sas7bdat)
CACHE_DIR  = BASE_DIR / "input" / "cache" / "EIBDDCMG"  # Parquet cache for .sas7bdat sources
MASTER_DIR = BASE_DIR / "master" / "EIBDDCMG"           # Persisted monthly DCMG master (replaces the SAS permanent library member MIS.DCMG&REPTMON)
OUTPUT_DIR = BASE_DIR / "output" / "EIBDDCMG"           # Printed month-to-date listing

# for _d in (INPUT_DIR, HOST_DIR, CACHE_DIR, MASTER_DIR, OUTPUT_DIR):
for _d in (HOST_DIR, CACHE_DIR, MASTER_DIR, OUTPUT_DIR):
    _d.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS = 500_000

# ============================================================================
# REPORT PAGE CONFIGURATION
# ============================================================================
PAGE_SIZE    = 60      # OPTIONS LINESIZE=140 (no PAGESIZE override -> default 60)
HEADER_LINES = 11       # 5 title lines + 1 blank + 4-line column header + 1 rule line

# ============================================================================
# STEP 1: REPORT DATE  (no reptdate.parquet -- derive from REPTDATE.py)
#         OPTIONS YEARCUTOFF=1960 in the SAS source; REPTDATE.py's own
#         two-digit-year handling (YEARCUTOFF=1950 per project convention)
#         is used here since no reptdate.parquet / DEPO library exists.
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values()
reptdate  = reptdate_values.reptdate
REPTYEAR  = reptdate_values.reptyear          # 2-digit year (YEAR2.)
REPTMON   = reptdate_values.reptmon           # Z2. month
REPTDAY   = reptdate_values.reptday           # Z2. day

RPDATE       = reptdate.strftime("%d/%m/%y")   # DDMMYY8.  (TITLE5)
RDATE_DDMMYY10 = reptdate.strftime("%d/%m/%Y") # DDMMYY10. (PROC REPORT GROUP display)

# NOTE: The original SAS program also derives REPTYY (PUT(REPTDATE,YEAR4.))
# and NOWK (day-of-month bucket via SELECT/WHEN 8/15/22/OTHERWISE) through
# CALL SYMPUT. Neither macro variable is referenced anywhere later in the
# program body, so both are dead values in the original code and are not
# reproduced here.

print(f"  Report date : {RDATE_DDMMYY10}  (RPDATE={RPDATE})")
print(f"  REPTMON={REPTMON}  REPTDAY={REPTDAY}  REPTYEAR={REPTYEAR}")

# ============================================================================
# HELPER: CACHE STAMP  (skip re-conversion if .sas7bdat hasn't changed)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and sas_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )

# ============================================================================
# HELPER: STREAM .sas7bdat -> PARQUET  (same chunked pattern as EIBDLN1M.py)
# ============================================================================
def _sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total  = 0

    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
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
                              f"from {col.type} to {field.type}: {e} -- filling nulls")
                        col = pa.nulls(len(col), type=field.type)
                cast_arrays.append(col)
            table = pa.Table.from_arrays(cast_arrays, schema=schema)
        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer:
        writer.close()
    print(f"  [{tag}] Done -- {total:,} rows cached.")


def _ensure_cache(sas_path: Path, cache_path: Path, tag: str) -> None:
    if _cache_is_fresh(sas_path, cache_path):
        print(f"  [{tag}] Cache fresh -- skipping conversion.")
    else:
        _sas_to_parquet(sas_path, cache_path, tag)

# ============================================================================
# HELPER: NUMERIC PARSING
# ============================================================================
def _parse_comma_numeric(text) -> Optional[float]:
    """Parse a COMMA-formatted numeric text field (commas + explicit decimal)."""
    if text is None:
        return None
    s = str(text).strip().replace(",", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_w_d_numeric(text: str, decimals: int) -> Optional[float]:
    """
    Parse a plain SAS numeric informat field (e.g. 16.2): if the raw text
    already contains a decimal point, use it as-is; otherwise the informat
    implies `decimals` decimal places (SAS w.d rule), so divide accordingly.
    """
    s = text.strip()
    if not s:
        return None
    try:
        if "." in s:
            return float(s)
        return int(s) / (10 ** decimals)
    except ValueError:
        return None


def _parse_ddmmyyyy(text: str) -> Optional[date]:
    """
    Parse a DD/MM/YYYY (or similarly single-char separated) 10-char date
    field, matching SUBSTR(DTE,1,2)=day, SUBSTR(DTE,4,2)=month,
    SUBSTR(DTE,7,4)=year.
    """
    s = text.strip()
    if len(s) < 10:
        return None
    day, month, year = s[0:2], s[3:5], s[6:10]
    try:
        return date(int(year), int(month), int(day))
    except (ValueError, TypeError):
        return None


def _sas_round(value, round_to: int = 1000):
    """SAS ROUND(): round half away from zero to the nearest `round_to`."""
    if value is None:
        return None
    scaled = value / round_to
    rounded = math.floor(scaled + 0.5) if scaled >= 0 else math.ceil(scaled - 0.5)
    return rounded * round_to


def _thousands(value):
    """INT(ROUND(value,1000)/1000) -- value expressed in RM'000, truncated."""
    r = _sas_round(value, 1000)
    if r is None:
        return None
    return int(r / 1000)


def _sas_sum(*values):
    """SAS SUM(): ignores missing arguments; returns None only if all missing."""
    present = [v for v in values if v is not None]
    if not present:
        return None
    return sum(present)


def _fmt_comma(value, width: int, decimals: int = 0) -> str:
    """
    COMMAw.d display. OPTIONS MISSING=0 means a missing numeric value is
    displayed as a plain '0' rather than blank.
    """
    if value is None:
        return "0".rjust(width)
    if decimals > 0:
        s = f"{value:,.{decimals}f}"
    else:
        s = f"{int(round(value)):,}"
    return s.rjust(width)


def _fmt_num(value, width: int, decimals: int = 2) -> str:
    """Plain w.d numeric display (SELLRATE/BUYRATE, format 14.2)."""
    if value is None:
        return "0".rjust(width)
    return f"{value:,.{decimals}f}".rjust(width)

# ============================================================================
# STEP 2: UTMS  (DATA UTMS; INFILE EQ ...)
# EQ is a '|'-delimited text file (GDG "(0)" = latest generation), so the
# newest available file is resolved via input_date.get_latest_file rather
# than a REPTDATE-derived name.
# ============================================================================
print("\nStep 2: Reading UTMS (EQ)...")

EQ_FILE = HOST_DIR / "PBB_EQNID.TXT"

def _read_pipe_delimited(path: Path) -> list[list[str]]:
    with open(path, "r", encoding="latin1", newline="") as fh:
        return list(csv.reader(fh, delimiter="|"))

_eq_rows = _read_pipe_delimited(EQ_FILE)

utms_rows = []
for fields in _eq_rows[1:]:                      # FIRSTOBS=2
    proceed = _parse_comma_numeric(fields[2]) if len(fields) > 2 else None
    utms_rows.append({"REPTDATE": reptdate, "PROCEED": proceed})

utms = (
    pl.DataFrame(utms_rows, schema={"REPTDATE": pl.Date, "PROCEED": pl.Float64})
    .group_by("REPTDATE")
    .agg(pl.col("PROCEED").sum())
)
print(f"  UTMS rows: {len(utms):,}")

# ============================================================================
# STEP 3: INONFI + IFI -> IUTMS  (DATA INONFI / IFI / IUTMS)
# IEQ is the Islamic counterpart of EQ, same GDG "(0)" latest-generation
# convention.
# ============================================================================
print("\nStep 3: Reading IUTMS (IEQ)...")

IEQ_FILE = HOST_DIR / "PIBB_EQNID.TXT"
_ieq_rows = _read_pipe_delimited(IEQ_FILE)

# INONFI: FIRSTOBS=2 OBS=2 -> only the single second physical line
inonfi_rows = []
if len(_ieq_rows) > 1:
    fields = _ieq_rows[1]
    iproceed = _parse_comma_numeric(fields[2]) if len(fields) > 2 else None
    inonfi_rows.append({"REPTDATE": reptdate, "IPROCEED": iproceed, "IPROCEFI": None})

# IFI: FIRSTOBS=3 -> every line from the third onward
ifi_rows = []
for fields in _ieq_rows[2:]:
    iprocefi = _parse_comma_numeric(fields[2]) if len(fields) > 2 else None
    ifi_rows.append({"REPTDATE": reptdate, "IPROCEED": None, "IPROCEFI": iprocefi})

iutms_schema = {"REPTDATE": pl.Date, "IPROCEED": pl.Float64, "IPROCEFI": pl.Float64}
iutms = pl.concat(
    [
        pl.DataFrame(inonfi_rows, schema=iutms_schema),
        pl.DataFrame(ifi_rows, schema=iutms_schema),
    ],
    how="vertical",
).group_by("REPTDATE").agg(
    pl.col("IPROCEED").sum(),
    pl.col("IPROCEFI").sum(),
)
print(f"  IUTMS rows: {len(iutms):,}")

# ============================================================================
# STEP 4: EQUTMS = MERGE UTMS IUTMS BY REPTDATE
# ============================================================================
equtms = utms.join(iutms, on="REPTDATE", how="full", coalesce=True)

# ============================================================================
# STEP 5: PBGIA  (DATA PBGIA; INFILE DPGIA ...)
# Fixed-format report scrape: each date block begins with a date line
# (CHECK1/DTE at column 2, width 10), followed by a run of balance lines
# (CHECK2/CLOSBAL at column 95, width 16.2). The date line resets the
# in-block counter N to 1; each subsequent balance line increments N.
# Only the SECOND balance line following a date line (N=3) is kept, using
# CLOSBAL from that line and REPTDATE carried over from the date line.
# FIRSTOBS=8 skips the first 7 physical lines (report banner).
# DPGIA is a GDG "(0)" latest-generation text report, resolved the same
# way as EQ/IEQ.
# ============================================================================
print("\nStep 5: Parsing PBGIA (DPGIA)...")

DPGIA_FILE = HOST_DIR / "DPGIA.TXT"

def _parse_pbgia(path: Path) -> pl.DataFrame:
    with open(path, "r", encoding="latin1") as fh:
        all_lines = fh.readlines()

    n = 0
    block_reptdate = None
    closbal = None
    records = []

    for raw in all_lines[7:]:                      # FIRSTOBS=8
        line = raw.rstrip("\r\n")
        if len(line) < 110:
            continue

        check1_txt = line[1:11]     # @002 CHECK1 DDMMYY10.
        check2_txt = line[94:110]   # @095 CHECK2 16.2

        check1 = _parse_ddmmyyyy(check1_txt)
        check2 = _parse_w_d_numeric(check2_txt, 2)

        if check1 is not None:
            n = 1
        elif check2 is not None:
            n += 1

        if check2 is None:          # IF CHECK2 EQ . THEN DELETE
            continue

        if n == 1:
            dte_txt = line[1:11]    # @002 DTE $10.
            block_reptdate = _parse_ddmmyyyy(dte_txt)
        else:
            closbal = check2         # @095 CLOSBAL 16.2 (same field re-read)

        if n == 3:
            records.append({"REPTDATE": block_reptdate, "CLOSBAL": closbal})

    return pl.DataFrame(records, schema={"REPTDATE": pl.Date, "CLOSBAL": pl.Float64})

pbgia = _parse_pbgia(DPGIA_FILE)
print(f"  PBGIA rows (all dates in report): {len(pbgia):,}")

# Original SAS keeps every date in PBGIA and relies on the final
# "IF REPTDATE=&RDATE;" filter on the merged DCMG dataset to reduce to
# today's row. Filtering here first is equivalent and avoids carrying
# unrelated historical rows through the merge.
pbgia_today = pbgia.filter(pl.col("REPTDATE") == reptdate)

# ============================================================================
# STEP 6: UTFX / IUTFX  (DEQ.UTFX&REPTYEAR&REPTMON&REPTDAY / IDEQ.IUTFX...)
# Deterministic filename fully derived from REPTDATE (matches the SAS
# macro substitution exactly) -- input_date.get_latest_file does not apply.
# ============================================================================
print("\nStep 6: Building UTFX / IUTFX (DEQ / IDEQ)...")

# UTFX_SAS    = INPUT_DIR / "eq_d" / f"utfx{REPTYEAR}{REPTMON}{REPTDAY}.sas7bdat"
# IUTFX_SAS   = INPUT_DIR / "ieq_d" / f"iutfx{REPTYEAR}{REPTMON}{REPTDAY}.sas7bdat"

# UTFX_SAS    = get_latest_file(INPUT_DIR / "eq_d", prefix="utfx")
# IUTFX_SAS   = get_latest_file(INPUT_DIR / "ieq_d", prefix="iutfx")

UTFX_SAS    = get_latest_file(HOST_DIR / "eq_d", prefix="utfx")
IUTFX_SAS   = get_latest_file(HOST_DIR / "ieq_d", prefix="iutfx")

UTFX_CACHE  = CACHE_DIR / f"utfx{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
IUTFX_CACHE = CACHE_DIR / f"iutfx{REPTYEAR}{REPTMON}{REPTDAY}.parquet"

_ensure_cache(UTFX_SAS, UTFX_CACHE, "UTFX")
_ensure_cache(IUTFX_SAS, IUTFX_CACHE, "IUTFX")

con = duckdb.connect(database=":memory:")
utfx = con.execute(f"""
    WITH tagged AS (
        SELECT
            CAST(AMTPAY AS DOUBLE) AS AMTPAY,
            CASE WHEN CUSTFISS NOT IN ('02','03','07','10','12','81','82','83','84','01')
                 THEN AMTPAY END AS TOTCPMMD,
            CASE WHEN CUSTFISS IN     ('02','03','07','10','12','81','82','83','84','01')
                 THEN AMTPAY END AS TOTCDFI
        FROM read_parquet('{UTFX_CACHE}')
        WHERE DEALTYPE IN ('BCD','BCQ')
    )
    SELECT SUM(TOTCPMMD) AS TOTCPMMD, SUM(TOTCDFI) AS TOTCDFI
    FROM tagged
""").pl()
con.close()
utfx = utfx.with_columns(pl.lit(reptdate).alias("REPTDATE"))
print(f"  UTFX: TOTCPMMD={utfx['TOTCPMMD'][0]}  TOTCDFI={utfx['TOTCDFI'][0]}")

con = duckdb.connect(database=":memory:")
iutfx = con.execute(f"""
    WITH tagged AS (
        SELECT
            CAST(AMTPAY AS DOUBLE) AS AMTPAY,
            CASE WHEN CUSTFISS NOT IN ('02','03','07','10','12','81','82','83','84','01')
                 THEN AMTPAY END AS TOTIPMMD,
            CASE WHEN CUSTFISS IN     ('02','03','07','10','12','81','82','83','84','01')
                 THEN AMTPAY END AS TOTIDFI
        FROM read_parquet('{IUTFX_CACHE}')
        WHERE DEALTYPE IN ('BCS','BCT','BCW','BQD')
    )
    SELECT SUM(TOTIPMMD) AS TOTIPMMD, SUM(TOTIDFI) AS TOTIDFI
    FROM tagged
""").pl()
con.close()
iutfx = iutfx.with_columns(pl.lit(reptdate).alias("REPTDATE"))
print(f"  IUTFX: TOTIPMMD={iutfx['TOTIPMMD'][0]}  TOTIDFI={iutfx['TOTIDFI'][0]}")

# ============================================================================
# STEP 7: ERATE  (PROC SORT DATA=MIS.EFORATE&REPTMON OUT=ERATE BY REPTDATE)
# EFORATE is a monthly rate table (one row per day of the month); the
# filename carries only the 2-digit month, matching the SAS macro
# reference MIS.EFORATE&REPTMON.
# ============================================================================
print("\nStep 7: Reading ERATE (EFORATE)...")

EFORATE_SAS   = HOST_DIR / "SASDATA_EGOLD" / f"eforate{REPTMON}.sas7bdat"
EFORATE_CACHE = CACHE_DIR / f"eforate{REPTMON}.parquet"
_ensure_cache(EFORATE_SAS, EFORATE_CACHE, "EFORATE")

con = duckdb.connect(database=":memory:")
erate = con.execute(f"""
    SELECT
        DATE '1960-01-01' + CAST(REPTDATE AS INTEGER) AS REPTDATE,
        CAST(SELLRATE AS DOUBLE) AS SELLRATE,
        CAST(BUYRATE  AS DOUBLE) AS BUYRATE
    FROM read_parquet('{EFORATE_CACHE}')
""").pl()
con.close()
print(f"  ERATE rows (whole month): {len(erate):,}")

# As with PBGIA, filter to today's REPTDATE up front rather than merging
# the whole month and filtering afterwards.
erate_today = erate.filter(pl.col("REPTDATE") == reptdate)

# ============================================================================
# STEP 8: RNID / IRNID  (RNID.RNID&REPTDAY / IRNID.RNID&REPTDAY)
# Deterministic filenames fully derived from REPTDATE, per project
# convention: rnid{YY}{MM}{DD}.sas7bdat / irnid{YY}{MM}{DD}.sas7bdat.
# ============================================================================
print("\nStep 8: Building RNID / IRNID...")

# RNID_SAS    = INPUT_DIR / "rnid" / f"rnid{REPTYEAR}{REPTMON}{REPTDAY}.sas7bdat"
# IRNID_SAS   = INPUT_DIR / "irnid" / f"irnid{REPTYEAR}{REPTMON}{REPTDAY}.sas7bdat"

# RNID_SAS    = get_latest_file(INPUT_DIR / "rnid", prefix="rnid")
# IRNID_SAS   = get_latest_file(INPUT_DIR / "irnid", prefix="irnid")

RNID_SAS    = get_latest_file(HOST_DIR / "rnid", prefix="rnid")
IRNID_SAS   = get_latest_file(HOST_DIR / "irnid", prefix="irnid")

RNID_CACHE  = CACHE_DIR / f"rnid{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
IRNID_CACHE = CACHE_DIR / f"irnid{REPTYEAR}{REPTMON}{REPTDAY}.parquet"

_ensure_cache(RNID_SAS, RNID_CACHE, "RNID")
_ensure_cache(IRNID_SAS, IRNID_CACHE, "IRNID")

con = duckdb.connect(database=":memory:")
rnid = con.execute(f"""
    WITH tagged AS (
        SELECT
            ABS(CAST(CURBAL AS DOUBLE)) AS CURBAL,
            CASE WHEN CUSTCODE IN (77,78) THEN ABS(CAST(CURBAL AS DOUBLE)) END AS TOTINDV,
            CASE WHEN CUSTCODE NOT IN (77,78) THEN ABS(CAST(CURBAL AS DOUBLE)) END AS TOTNIND
        FROM read_parquet('{RNID_CACHE}')
        WHERE PRODUCT = 320
          AND NIDSTAT = 'N'
          AND CDSTAT  = 'A'
    )
    SELECT SUM(TOTINDV) AS TOTINDV, SUM(TOTNIND) AS TOTNIND
    FROM tagged
""").pl()
con.close()
rnid = rnid.with_columns(pl.lit(reptdate).alias("REPTDATE"))

con = duckdb.connect(database=":memory:")
irnid = con.execute(f"""
    WITH tagged AS (
        SELECT
            CASE WHEN CUSTCODE IN (77,78) THEN ABS(CAST(CURBAL AS DOUBLE)) END AS TOIINDV,
            CASE WHEN CUSTCODE NOT IN (77,78) THEN ABS(CAST(CURBAL AS DOUBLE)) END AS TOININD
        FROM read_parquet('{IRNID_CACHE}')
        WHERE PRODUCT = 321
          AND NIDSTAT = 'N'
          AND CDSTAT  = 'A'
    )
    SELECT SUM(TOIINDV) AS TOIINDV, SUM(TOININD) AS TOININD
    FROM tagged
""").pl()
con.close()
irnid = irnid.with_columns(pl.lit(reptdate).alias("REPTDATE"))

print(f"  RNID : TOTINDV={rnid['TOTINDV'][0]}  TOTNIND={rnid['TOTNIND'][0]}")
print(f"  IRNID: TOIINDV={irnid['TOIINDV'][0]}  TOININD={irnid['TOININD'][0]}")

# ============================================================================
# STEP 9: DCMG  (MERGE EQUTMS PBGIA ERATE RNID IRNID UTFX IUTFX BY REPTDATE;
#                IF REPTDATE=&RDATE; ... rounding/sum derivations)
# All pieces above are already reduced to (at most) today's REPTDATE row,
# so a sequence of left joins on a single-row REPTDATE anchor reproduces
# the merge + filter without carrying unrelated rows through, and without
# needing a PROC SORT (single row -> sort is a no-op).
# ============================================================================
print("\nStep 9: Building today's DCMG row...")

def _left_join(base_df: pl.DataFrame, other_df: pl.DataFrame) -> pl.DataFrame:
    if other_df.is_empty():
        return base_df
    return base_df.join(other_df, on="REPTDATE", how="left")

dcmg_today = pl.DataFrame({"REPTDATE": [reptdate]}, schema={"REPTDATE": pl.Date})
for piece in (equtms, pbgia_today, erate_today, rnid, irnid, utfx, iutfx):
    dcmg_today = _left_join(dcmg_today, piece)

row = dcmg_today.row(0, named=True)

proceed   = _thousands(row.get("PROCEED"))
iproceed  = _thousands(row.get("IPROCEED"))
totprocd  = _sas_sum(proceed, iproceed)
iprocefi  = _thousands(row.get("IPROCEFI"))
closbal   = _thousands(row.get("CLOSBAL"))
totindv   = _thousands(row.get("TOTINDV"))
totnind   = _thousands(row.get("TOTNIND"))
totrnid   = _sas_sum(totindv, totnind)
toiindv   = _thousands(row.get("TOIINDV"))
toinind   = _thousands(row.get("TOININD"))
toirnid   = _sas_sum(toiindv, toinind)
totanid   = _sas_sum(totrnid, toirnid)
totcpmmd  = _thousands(row.get("TOTCPMMD"))
totipmmd  = _thousands(row.get("TOTIPMMD"))
totnpmmd  = _sas_sum(totcpmmd, totipmmd)
totcdfi   = _thousands(row.get("TOTCDFI"))
totidfi   = _thousands(row.get("TOTIDFI"))
totndfi   = _sas_sum(totcdfi, totidfi)

dcmg_today = pl.DataFrame([{
    "REPTDATE": reptdate,
    "PROCEED":  proceed,
    "IPROCEED": iproceed,
    "TOTPROCD": totprocd,
    "IPROCEFI": iprocefi,
    "TOTINDV":  totindv,
    "TOTNIND":  totnind,
    "TOTRNID":  totrnid,
    "TOIINDV":  toiindv,
    "TOININD":  toinind,
    "TOIRNID":  toirnid,
    "TOTANID":  totanid,
    "TOTCPMMD": totcpmmd,
    "TOTIPMMD": totipmmd,
    "TOTNPMMD": totnpmmd,
    "TOTCDFI":  totcdfi,
    "TOTIDFI":  totidfi,
    "TOTNDFI":  totndfi,
    "CLOSBAL":  closbal,
    "SELLRATE": row.get("SELLRATE"),
    "BUYRATE":  row.get("BUYRATE"),
}])

print("  Today's DCMG row built.")

# ============================================================================
# STEP 10: %APPEND  (persist into the running monthly DCMG master)
# SAS behaviour: on REPTDAY='01' the monthly dataset restarts fresh
# (SET DCMG only); otherwise SET DCMG MIS.DCMG&REPTMON, sorted and
# de-duplicated NODUPKEY BY REPTDATE (today's freshly computed row wins
# over any pre-existing row for the same date, since it is placed first
# in the stacked order before the sort).
#
# There is no persistent reptdate-based parquet history yet in production
# (only .sas7bdat monthly masters from the legacy SAS runs), so this
# program bootstraps from the legacy .sas7bdat the first time it is asked
# to append to a month that has no parquet master yet, and writes/reads
# the parquet master on every run after that.
# ============================================================================
print("\nStep 10: Appending to monthly DCMG master...")

DCMG_PARQUET    = MASTER_DIR / f"dcmg{REPTMON}.parquet"
DCMG_SAS_LEGACY = MASTER_DIR / f"dcmg{REPTMON}.sas7bdat"

DCMG_SCHEMA = dcmg_today.schema

if REPTDAY == "01":
    dcmg_monthly = dcmg_today
else:
    if DCMG_PARQUET.exists():
        existing = pl.read_parquet(DCMG_PARQUET)
    elif DCMG_SAS_LEGACY.exists():
        existing = pl.from_pandas(
            pd.read_sas(DCMG_SAS_LEGACY, encoding="latin1")
        ).with_columns(pl.col("REPTDATE").cast(pl.Date))
    else:
        existing = pl.DataFrame(schema=DCMG_SCHEMA)

    combined = pl.concat([dcmg_today, existing], how="diagonal_relaxed")
    dcmg_monthly = (
        combined
        .unique(subset=["REPTDATE"], keep="first", maintain_order=True)
        .sort("REPTDATE")
    )

dcmg_monthly.write_parquet(DCMG_PARQUET)
print(f"  Monthly master rows: {len(dcmg_monthly):,}")
print(f"  Master file        : {DCMG_PARQUET}")

# ============================================================================
# STEP 11: REPORT GENERATION  (PROC REPORT ... SPLIT='*', ASA carriage
# control, LINESIZE=140, default PAGESIZE=60, NOCENTER/NODATE/NONUMBER)
# ============================================================================
print("\nStep 11: Generating report...")

REPORT_FILE = build_output_file(OUTPUT_DIR, "EIBDDCMG").with_suffix(".txt")

REPORT_COLUMNS = [
    {"var": "REPTDATE", "header": ["DATE"],                                          "width": 10, "type": "date"},
    {"var": "PROCEED",  "header": ["NID-C", "(NON-FI)", "(PROCEEDS)"],                "width": 15, "type": "comma"},
    {"var": "IPROCEED", "header": ["NID-I", "(NON-FI)", "(PROCEEDS)"],                "width": 15, "type": "comma"},
    {"var": "TOTPROCD", "header": ["TOTAL", "NIDS", "(CONV + ISL)"],                  "width": 15, "type": "comma"},
    {"var": "IPROCEFI", "header": ["NID-I(FI)", "(PBB ONLY)", "(PROCEEDS)"],          "width": 15, "type": "comma"},
    {"var": "TOTINDV",  "header": ["INDV", "RETAIL", "NID", "(CONV)"],                "width": 15, "type": "comma"},
    {"var": "TOTNIND",  "header": ["NON-INDV", "RETAIL", "NID", "(CONV)"],            "width": 15, "type": "comma"},
    {"var": "TOTRNID",  "header": ["TOTAL", "RETAIL", "NID", "(CONV)"],               "width": 15, "type": "comma"},
    {"var": "TOIINDV",  "header": ["INDV", "RETAIL", "NIDC", "(ISL)"],                "width": 15, "type": "comma"},
    {"var": "TOININD",  "header": ["NON-INDV", "RETAIL", "NIDC", "(ISL)"],            "width": 15, "type": "comma"},
    {"var": "TOIRNID",  "header": ["TOTAL", "RETAIL", "NIDC", "(ISL)"],               "width": 15, "type": "comma"},
    {"var": "TOTANID",  "header": ["TOTAL", "RETAIL", "NID & NIDC", "(CONV + ISL)"],  "width": 15, "type": "comma"},
    {"var": "TOTCPMMD", "header": ["PMMD &", "PQMMD", "(CONV)"],                      "width": 15, "type": "comma"},
    {"var": "TOTIPMMD", "header": ["PMMD", "(ISL)"],                                  "width": 15, "type": "comma"},
    {"var": "TOTNPMMD", "header": ["TOTAL", "PMMD", "(CONV + ISL)"],                  "width": 15, "type": "comma"},
    {"var": "TOTCDFI",  "header": ["DFI & FI", "PMMD &", "PQMMD", "(CONV)"],          "width": 15, "type": "comma"},
    {"var": "TOTIDFI",  "header": ["DFI & FI", "PMMD", "(ISL)"],                      "width": 15, "type": "comma"},
    {"var": "TOTNDFI",  "header": ["TOTAL", "DFI & FI", "PMMD", "(CONV + ISL)"],      "width": 15, "type": "comma"},
    {"var": "CLOSBAL",  "header": ["GIA", "BALANCE", "IN RM", "(SPOTRATE)"],          "width": 15, "type": "comma"},
    {"var": "SELLRATE", "header": ["GIA", "CLSG PRICE", "RM/G", "(SELLING)"],         "width": 14, "type": "num2"},
    {"var": "BUYRATE",  "header": ["GIA", "CLSG PRICE", "RM/G", "(BUYING)"],          "width": 14, "type": "num2"},
]
COLUMN_GAP = 1
HEADER_DEPTH = max(len(c["header"]) for c in REPORT_COLUMNS)


def _build_title_block() -> list[str]:
    """5 titles (NOCENTER -> left justified) preceding the column headers."""
    return [
        "REPORT ID : DMMISR1G",
        "PUBLIC BANK BERHAD",
        "SALES ADMINISTRATION & SUPPORT",
        "BANK'S TOTAL DEPOSITS (RM '000)",
        f"AS AT {RPDATE}",
    ]


def _build_column_header_block() -> list[str]:
    """SPLIT='*' multi-line headers, bottom-aligned against the rule line."""
    lines = []
    for depth in range(HEADER_DEPTH):
        parts = []
        for col in REPORT_COLUMNS:
            header_parts = col["header"]
            pad = HEADER_DEPTH - len(header_parts)
            idx = depth - pad
            text = header_parts[idx] if idx >= 0 else ""
            parts.append(text.center(col["width"]))
        lines.append((" " * COLUMN_GAP).join(parts))
    rule_width = sum(c["width"] for c in REPORT_COLUMNS) + COLUMN_GAP * (len(REPORT_COLUMNS) - 1)
    lines.append("-" * rule_width)
    return lines


def _build_page_header() -> list[str]:
    """Full page-top block: titles, blank separator, column headers, rule."""
    block = list(_build_title_block())
    block.append("")
    block.extend(_build_column_header_block())
    return block


def _format_value(col: dict, value) -> str:
    if col["type"] == "date":
        return value.strftime("%d/%m/%Y").rjust(col["width"]) if value else "0".rjust(col["width"])
    if col["type"] == "comma":
        return _fmt_comma(value, col["width"], 0)
    if col["type"] == "num2":
        return _fmt_num(value, col["width"], 2)
    return str(value or "").rjust(col["width"])


def _build_detail_line(row: dict) -> str:
    parts = [_format_value(col, row.get(col["var"])) for col in REPORT_COLUMNS]
    return (" " * COLUMN_GAP).join(parts)


output_lines: list[str] = []
lines_on_page = 0

for row in dcmg_monthly.sort("REPTDATE").iter_rows(named=True):
    if lines_on_page == 0 or lines_on_page >= PAGE_SIZE:
        page_header = _build_page_header()
        output_lines.append("1" + page_header[0])       # '1' = new page
        for hl in page_header[1:]:
            output_lines.append(" " + hl)
        lines_on_page = HEADER_LINES

    output_lines.append(" " + _build_detail_line(row))    # ' ' = single space
    lines_on_page += 1

with open(REPORT_FILE, "w", encoding="latin1") as fh:
    for ln in output_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {REPORT_FILE}")
print(f"  Total lines    : {len(output_lines):,}\n")

print("=" * 100)
print("DCMG monthly master (month-to-date):")
print(dcmg_monthly)
print("=" * 100)

del dcmg_today, dcmg_monthly
gc.collect()

# ============================================================================
# JCL/HOUSEKEEPING STEPS RETAINED AS COMMENTS (no Python equivalent needed)
# ============================================================================
# //DELETE   EXEC PGM=IEFBR14
# //DD1      DD DSN=SAP.PBB.DMMISR1G.DAILY,DISP=(MOD,DELETE,DELETE),...
#   -> mainframe dataset-delete housekeeping step; not applicable when the
#      monthly master is a parquet file managed by this program.
#
# //*RUNSFTP  EXEC COZBATCH   (fully commented alternate SFTP block, kept
# //*         disabled in the original JCL)
# //*CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(CSASSFTP)
# //*           DD *
# //*lzopts servercp=$servercp,notrim,overflow=trunc,mode=text
# //*lzopts linerule=$lr
# //*cd TextFile
# //*cd RetailBank
# //*put //SAP.PBB.DMMISR1G.DAILY  DMMISR1G@%OMM.%OYY..TXT
# //*EOB
#
# //RUNSFTP  EXEC COZBATCH   (active FTP-to-DRR step; file transfer is an
#                             operational/JCL concern, out of scope for
#                             this Python conversion)
# //CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(DRR#SFTP)
# //           DD *
# lzopts servercp=$servercp,notrim,overflow=trunc,mode=text
# lzopts linerule=$lr
# //           DD DISP=SHR,DSN=&&FTPPUT
# //           DD *
# put //SAP.PBB.DMMISR1G.DAILY  DMMISR1G@%OMM.%OYY..TXT
# EOB

print("\nEIBDDCMG complete.")
