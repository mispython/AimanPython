#!/usr/bin/env python3
"""
Program : EIBMNLFE.py
Purpose : National Loans/Deposit Behavioural Trend Report
          Computes weekly/monthly/quarterly/half-yearly/yearly percentage
          movement bands per deposit product (RM & Foreign Currency,
          Fixed/Savings/Current/CASA), re-derives BNM product coding,
          and produces the BEHAVIORAL TABLE / HIGH-LOW / MAXMIN reports.
"""

import os
import gc
import re
import math
import shutil
import duckdb
import polars as pl
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import date, timedelta

from REPTDATE import get_reptdate_values
from input_date import get_latest_file
# from output_date import build_output_file  # not used - output filename has no date component

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

# INPUT_NOTE_DIR      = BASE_DIR / "input" / "prod" / "EIBMNLFE"          # note
# INPUT_GLRMFXP2_DIR  = BASE_DIR / "input" / "prod" / "EIBMNLFE"          # glrmfxp2

STG_DIR             = Path("/stgsrcsys/host/uat/AII/EIBMNLFE")
INPUT_NOTE_DIR      = STG_DIR  / "STORE1"           # note
INPUT_GLRMFXP2_DIR  = STG_DIR  / "STOREGL"          # glrmfxp2

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBMNLFE"

# Persistent library replicating SAS permanent libraries (BASE/STORE/FINAL).
LIB_ROOT   = BASE_DIR / "input" / "lib" / "EIBMNLFE"
BASE_LIB   = LIB_ROOT / "base"     # BASE.<PROD> - true daily/milestone accumulator
STORE_LIB  = LIB_ROOT / "store"    # STORE.* - snapshot of this run's working sets
FINAL_LIB  = LIB_ROOT / "final"    # FINAL.* - snapshot of this run's report sets

OUTPUT_DIR = BASE_DIR / "output" / "EIBMNLFE"

for _d in (INPUT_NOTE_DIR, INPUT_GLRMFXP2_DIR, CACHE_DIR,
           BASE_LIB, STORE_LIB, FINAL_LIB, OUTPUT_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# Output filename carries no date component (GDG "+1" generation) - fixed name.
OUTPUT_FILE = OUTPUT_DIR / "NLFBEHAVE.txt"

# ============================================================================
# REPORT PAGE CONFIGURATION
# ============================================================================
PAGE_SIZE    = 60   # lines per page (default, not otherwise specified)
ROW_LIMIT    = int(os.environ.get("ROW_LIMIT", 0))
CHUNK_ROWS   = 500_000

# ============================================================================
# PRODUCT CONSTANTS
# ============================================================================
# BNM product code -> internal DESC (first stage: note/GL processing)
BNMCODE_TO_DESC = {
    "9531108": "INDRMFD",
    "9531109": "NONRMFD",
    "9531208": "INDRMSA",
    "9531209": "NONRMSA",
    "9531308": "INDRMDD",
    "9531309": "NONRMDD",
    "9631108": "INDFXFD",
    "9631109": "NONFXFD",
    "9631308": "INDFXCA",
    "9631309": "NONFXCA",
}

# DESC -> (ITEM, ITEM4) for the first ITEM assignment stage
ITEM_MAP_STAGE1 = {
    "INDRMFD": ("A1.15", "- FIXED  "),
    "NONRMFD": ("A1.12", "- FIXED  "),
    "INDRMSA": ("A1.16", "- SAVINGS"),
    "NONRMSA": ("A1.13", "- SAVINGS"),
    "INDRMDD": ("A1.17", "- CURRENT"),
    "NONRMDD": ("A1.14", "- CURRENT"),
    "INDFXFD": ("B1.15", "- FIXED  "),
    "NONFXFD": ("B1.12", "- FIXED  "),
    "INDFXCA": ("B1.17", "- CURRENT"),
    "NONFXCA": ("B1.14", "- CURRENT"),
}

# DESC -> re-derived BNM code used for the second (BEHAVENOTE) coding stage
DESC_TO_BNMCODE2 = {
    "INDRMFD": "9331108",
    "NONRMFD": "9331109",
    "INDRMSA": "9331208",
    "NONRMSA": "9331209",
    "INDRMDD": "9331308",
    "NONRMDD": "9331309",
    "INDFXFD": "9631108",
    "NONFXFD": "9631109",
    "INDFXCA": "9631308",
    "NONFXCA": "9631309",
}

# Second ITEM assignment stage, keyed by the re-derived BNM code
ITEM_MAP_STAGE2 = {
    "9331108": ("A1.15", "- FIXED  "),
    "9331109": ("A1.12", "- FIXED  "),
    "9331208": ("A1.16", "- SAVINGS"),
    "9331209": ("A1.13", "- SAVINGS"),
    "9331308": ("A1.17", "- CURRENT"),
    "9331309": ("A1.14", "- CURRENT"),
    "9631108": ("B1.15", "- FIXED  "),
    "9631109": ("B1.12", "- FIXED  "),
    "9631308": ("B1.17", "- CURRENT"),
    "9631309": ("B1.14", "- CURRENT"),
}

# Products processed by MISAPPD / REPORT, in original invocation order
PRODUCTS = [
    "INDRMDD", "INDRMFD", "INDRMSA", "INDFXCA", "INDFXFD",
    "NONRMDD", "NONRMFD", "NONRMSA", "NONFXCA", "NONFXFD",
]

# ============================================================================
# STEP 1: REPORT DATE  (no reptdate.parquet — derive from REPTDATE.py)
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values(year_format="%Y")
REPTDATE  = reptdate_values.reptdate          # date used as "REPTDATE" macro var
REPTYEAR  = reptdate_values.reptyear          # 4-digit year (YEAR4.)
REPTMON   = reptdate_values.reptmon           # Z2.
REPTDAY   = reptdate_values.reptday           # Z2.
DATEX     = REPTDATE.strftime("%d/%m/%y")     # DDMMYY8.
RDATE     = DATEX                             # same DDMMYY8. value
WKLYDATE  = REPTDATE

# LAST = day-of-month of (last day of previous month), relative to TODAY(),
# not REPTDATE - mirrors the original SAS logic exactly.
_today = date.today()
_first_of_month = _today.replace(day=1)
_last_day_prev_month = _first_of_month - timedelta(days=1)
LAST_DAY_STR = _last_day_prev_month.strftime("%d")

if REPTDAY in ("08", "15", "22"):
    INSERT = "Y"
elif REPTDAY == LAST_DAY_STR and _today.day < 8:
    INSERT = "Y"
else:
    # Original SAS carries INSERT over from DEPOSIT.REPTDATE when neither
    # condition is met. No such upstream dataset is available, so INSERT
    # defaults to blank (no milestone insert) per project convention.
    INSERT = ""

print(f"  REPTDATE     : {REPTDATE}  (DATEX/RDATE = {RDATE})")
print(f"  REPTYEAR/MON/DAY : {REPTYEAR}/{REPTMON}/{REPTDAY}")
print(f"  LAST (prev month end day) : {LAST_DAY_STR}")
print(f"  INSERT flag  : '{INSERT}'")

# ============================================================================
# HELPERS: CACHE STAMP + STREAM .sas7bdat -> PARQUET  (per EIBDLN1M.py pattern)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


def sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    """Convert a .sas7bdat to Parquet in streaming chunks (schema-locked)."""
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0
    rows_read = 0

    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
    for chunk in reader:
        if ROW_LIMIT and rows_read >= ROW_LIMIT:
            break
        if ROW_LIMIT:
            chunk = chunk.iloc[: ROW_LIMIT - rows_read]
        rows_read += len(chunk)

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
                              f"{col.type}->{field.type}: {e} - nulling")
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
# STEP 2: RESOLVE + CACHE INPUT FILES  (NOTE, GLRMFXP2 - exact date known)
# ============================================================================
print("\nStep 2: Resolving NOTE / GLRMFXP2 input files...")

# _note_prefix = f"note{REPTYEAR}{REPTMON}{REPTDAY}"
# _gl_prefix   = f"glrmfxp2{REPTYEAR}{REPTMON}{REPTDAY}"

_note_prefix = "note"
_gl_prefix   = "glrmfxp2"

note_path = get_latest_file(INPUT_NOTE_DIR, prefix=_note_prefix)
gl_path   = get_latest_file(INPUT_GLRMFXP2_DIR, prefix=_gl_prefix)

print(f"  NOTE      : {note_path.name}")
print(f"  GLRMFXP2  : {gl_path.name}")

NOTE_CACHE = CACHE_DIR / f"{note_path.stem}.parquet"
GL_CACHE   = CACHE_DIR / f"{gl_path.stem}.parquet"

if not _cache_is_fresh(note_path, NOTE_CACHE):
    sas_to_parquet(note_path, NOTE_CACHE, "NOTE")
else:
    print("  [NOTE    ] Cache fresh - skipping conversion.")

if not _cache_is_fresh(gl_path, GL_CACHE):
    sas_to_parquet(gl_path, GL_CACHE, "GLRMFXP2")
else:
    print("  [GLRMFXP2] Cache fresh - skipping conversion.")

# ============================================================================
# STEP 3: DATA DEPOSIT  (BNM code -> DESC, amount rounded to thousands)
# ============================================================================
print("\nStep 3: Building DEPOSIT (NOTE derivation)...")

con = duckdb.connect(database=":memory:")

_case_when = "\n".join(
    f"            WHEN '{code}' THEN '{desc}'" for code, desc in BNMCODE_TO_DESC.items()
)

deposit_pd = con.execute(f"""
    SELECT
        SUBSTR(CAST(BNMCODE AS VARCHAR), 1, 7) AS PROD,
        SUBSTR(CAST(BNMCODE AS VARCHAR), 6, 2) AS INDNON,
        CASE SUBSTR(CAST(BNMCODE AS VARCHAR), 1, 7)
{_case_when}
            ELSE NULL
        END AS DESC,
        ROUND(CAST(AMOUNT AS DOUBLE) / 1000.0, 0) AS AMOUNT
    FROM read_parquet('{NOTE_CACHE}')
""").pl()

con.close()
gc.collect()
print(f"  DEPOSIT rows: {len(deposit_pd):,}")

# ============================================================================
# STEP 4: PROC TRANSPOSE  ->  STORE.DEP  (WEEK/MONTH/QTR/HALFYR/YEAR/LAST)
# ============================================================================
print("\nStep 4: Transposing to WEEK/MONTH/QTR/HALFYR/YEAR/LAST columns...")

_transpose_cols = ["WEEK", "MONTH", "QTR", "HALFYR", "YEAR", "LAST"]

grouped = (
    deposit_pd
    .filter(pl.col("DESC").is_not_null())
    .group_by(["PROD", "DESC"], maintain_order=True)
    .agg(pl.col("AMOUNT"))
)

_records = []
for row in grouped.iter_rows(named=True):
    amounts = row["AMOUNT"]
    rec = {"PROD": row["PROD"], "DESC": row["DESC"]}
    for i, cname in enumerate(_transpose_cols):
        rec[cname] = amounts[i] if i < len(amounts) else None
    _records.append(rec)

store_dep = pl.DataFrame(
    _records,
    schema={"PROD": pl.Utf8, "DESC": pl.Utf8, **{c: pl.Float64 for c in _transpose_cols}},
) if _records else pl.DataFrame(
    schema={"PROD": pl.Utf8, "DESC": pl.Utf8, **{c: pl.Float64 for c in _transpose_cols}}
)

print(f"  STORE.DEP rows: {len(store_dep):,}")

# ============================================================================
# STEP 5: BUILD DEPRMP2 / DEPFXP2  (ITEM assignment, negate, split RM/FX)
# ============================================================================
print("\nStep 5: Building DEPRMP2 / DEPFXP2...")

# def _item_expr(mapping: dict, key_col: str):
#     return (
#         pl.col(key_col).map_dict({k: v[0] for k, v in mapping.items()}, default=None).alias("ITEM"),
#         pl.col(key_col).map_dict({k: v[1] for k, v in mapping.items()}, default=None).alias("ITEM4"),
#     )

def _item_expr(mapping: dict, key_col: str):
    item_dict   = {k: v[0] for k, v in mapping.items()}
    item4_dict  = {k: v[1] for k, v in mapping.items()}
    return (
        pl.col(key_col).replace_strict(item_dict, default=None).alias("ITEM"),
        pl.col(key_col).replace_strict(item4_dict, default=None).alias("ITEM4"),
    )

item_expr, item4_expr = _item_expr(ITEM_MAP_STAGE1, "DESC")

store_dep2 = store_dep.with_columns([item_expr, item4_expr]).with_columns(
    pl.col("PROD").str.slice(5, 2).alias("INDNON")
)

_balance_expr = sum(pl.col(c).fill_null(0.0) for c in _transpose_cols)
store_dep2 = store_dep2.with_columns(_balance_expr.alias("BALANCE"))

# Negate WEEK/MONTH/QTR/HALFYR/YEAR/LAST/BALANCE
_neg_cols = _transpose_cols + ["BALANCE"]
store_dep2 = store_dep2.with_columns(
    [(-pl.col(c).fill_null(0.0)).alias(c) for c in _neg_cols]
).with_columns([
    pl.lit(DATEX).alias("DATEX"),
    pl.lit(REPTDATE).alias("DATE"),
])

deprmp2 = store_dep2.filter(pl.col("PROD").str.slice(0, 2) == "95")
depfxp2 = store_dep2.filter(pl.col("PROD").str.slice(0, 2) == "96")

print(f"  DEPRMP2 rows: {len(deprmp2):,} | DEPFXP2 rows: {len(depfxp2):,}")

# ============================================================================
# STEP 6: MERGE GLRMFXP2 INTO DEPFXP2  (BY ITEM, sum WEEK/LAST/BALANCE)
# ============================================================================
print("\nStep 6: Merging GLRMFXP2 into DEPFXP2 (by ITEM)...")

# PROC SORT BY ITEM steps omitted: correctness here depends only on the join
# key, not on physical ordering, so the SAS sorts are unnecessary overhead
# in a DataFrame-join based implementation.
con = duckdb.connect(database=":memory:")
gl_pl = con.execute(f"""
    SELECT
        CAST(ITEM AS VARCHAR)   AS ITEM,
        CAST(WEEK AS DOUBLE)    AS WEEK1,
        CAST(LAST AS DOUBLE)    AS LAST1,
        CAST(BALANCE AS DOUBLE) AS BALANCE1
    FROM read_parquet('{GL_CACHE}')
""").pl()
con.close()
gc.collect()

depfxp2 = depfxp2.join(gl_pl, on="ITEM", how="full", coalesce=True)
depfxp2 = depfxp2.with_columns([
    (pl.col("WEEK").fill_null(0.0) + pl.col("WEEK1").fill_null(0.0)).alias("WEEK"),
    (pl.col("LAST").fill_null(0.0) + pl.col("LAST1").fill_null(0.0)).alias("LAST"),
    (pl.col("BALANCE").fill_null(0.0) + pl.col("BALANCE1").fill_null(0.0)).alias("BALANCE"),
]).drop(["WEEK1", "LAST1", "BALANCE1"])

print(f"  DEPFXP2 rows after GL merge: {len(depfxp2):,}")

# ============================================================================
# STEP 7: BASE.DEPOSIT  (concat DEPRMP2 + DEPFXP2)
# PROC SORT BY DESCENDING INDNON on each input omitted: subsequent access is
# by WHERE DATE=/DESC= filters only, so physical ordering is not required.
# ============================================================================
print("\nStep 7: Building BASE.DEPOSIT (concat DEPRMP2 + DEPFXP2)...")

_common_cols = ["PROD", "DESC", "WEEK", "MONTH", "QTR", "HALFYR", "YEAR",
                "LAST", "ITEM", "ITEM4", "INDNON", "BALANCE", "DATEX", "DATE"]

base_deposit = pl.concat(
    [deprmp2.select(_common_cols), depfxp2.select(_common_cols)],
    how="vertical_relaxed",
)

(STORE_LIB / "deprmp2.parquet").write_bytes(b"") if False else None
deprmp2.write_parquet(STORE_LIB / "deprmp2.parquet")
depfxp2.write_parquet(STORE_LIB / "depfxp2.parquet")
base_deposit.write_parquet(BASE_LIB / "deposit.parquet")

print(f"  BASE.DEPOSIT rows: {len(base_deposit):,}")

# ============================================================================
# HELPER: MISAPPD  (per-product accumulator update / STORE.<PROD> build)
# ============================================================================
def misappd(prod: str) -> pl.DataFrame:
    """
    Extract today's record for `prod` from BASE.DEPOSIT, then either append
    it to the persistent BASE.<PROD> history file (milestone/INSERT day) or
    build a transient STORE.<PROD> view without touching the persistent
    history (non-milestone day). Returns STORE.<PROD> (full history up to
    and including today, sorted by DATE).
    """
    base_path = BASE_LIB / f"{prod.lower()}.parquet"

    today_rec = (
        base_deposit
        .filter((pl.col("DATE") == REPTDATE) & (pl.col("DESC") == prod))
        .select(["DATEX", "DATE", "BALANCE"])
        .with_columns((-pl.col("BALANCE")).alias("BALANCE"))
    )

    if base_path.exists():
        base_hist = pl.read_parquet(base_path)
    else:
        base_hist = pl.DataFrame(
            schema={"DATEX": pl.Utf8, "DATE": pl.Date, "BALANCE": pl.Float64}
        )

    if INSERT == "Y":
        # Idempotent re-run: drop any existing record for today, then append.
        base_hist = base_hist.filter(pl.col("DATE") != REPTDATE).sort("DATE")
        base_hist = pl.concat([base_hist, today_rec], how="vertical_relaxed")
        base_hist.write_parquet(base_path)
        store_df = base_hist.filter(pl.col("DATE") <= REPTDATE).sort("DATE")
    else:
        store_df = pl.concat([base_hist, today_rec], how="vertical_relaxed")
        store_df = store_df.filter(pl.col("DATE") <= REPTDATE).sort("DATE")

    store_df.write_parquet(STORE_LIB / f"{prod.lower()}.parquet")
    return store_df


# ============================================================================
# HELPERS FOR %REPORT
# ============================================================================
def _nth(df: pl.DataFrame, n: int):
    """1-based row access mirroring SAS _N_=n. Returns None if out of range."""
    if n < 1 or n > df.height:
        return None
    return df.row(n - 1, named=True)


def _qtr_month(month: int, offset: int) -> int:
    m = month - offset
    return 12 + m if m <= 0 else m


def _process(store_df: pl.DataFrame, weekstdt: date, weekend: date, lastdt: date):
    """Mirrors %PROCESS: MIN/MAX balance in window, % change vs LASTDT balance."""
    window = store_df.filter(
        (pl.col("DATE") >= weekstdt) & (pl.col("DATE") <= weekend)
    )
    if window.height == 0:
        return None

    last_rows = store_df.filter(pl.col("DATE") == lastdt)
    if last_rows.height == 0:
        return None
    last_balance = last_rows["BALANCE"][0]
    if last_balance in (None, 0):
        return None

    min_row = window.sort("BALANCE").row(0, named=True)
    max_row = window.sort("BALANCE", descending=True).row(0, named=True)

    if max_row["DATE"] > min_row["DATE"]:
        pct = round(((max_row["BALANCE"] - min_row["BALANCE"]) / last_balance) * 100, 2)
    else:
        pct = round(((min_row["BALANCE"] - max_row["BALANCE"]) / last_balance) * 100, 2)

    return {"DATE": weekend, "PCTAGE": pct}


def _fmt_comma(value, width: int, decimals: int = 2) -> str:
    if value is None:
        return " " * width
    try:
        v = float(value)
    except (TypeError, ValueError):
        return " " * width
    s = f"{v:,.{decimals}f}" if decimals > 0 else f"{int(round(v)):,}"
    return s.rjust(width)


def _print_table(title_lines, columns, rows, page_size=PAGE_SIZE):
    """
    Approximate PROC PRINT rendering with ASA carriage control.
    columns: list of (header, width, align) tuples; rows: list of str tuples.
    """
    lines = []

    def _header_block():
        hb = []
        for i, t in enumerate(title_lines):
            hb.append(("1" if i == 0 else " ") + t)
        hdr_line = "OBS".rjust(5) + " " + " ".join(
            (h.ljust(w) if align == "left" else h.rjust(w))
            for h, w, align in columns
        )
        hb.append(" " + hdr_line)
        hb.append(" " + "-" * len(hdr_line))
        return hb

    header_block = _header_block()
    lines.extend(header_block)
    on_page = len(header_block)

    for idx, row in enumerate(rows, start=1):
        if on_page >= page_size:
            hb = _header_block()
            lines.extend(hb)
            on_page = len(hb)
        data_line = str(idx).rjust(5) + " " + " ".join(
            (val.ljust(w) if align == "left" else val.rjust(w))
            for (h, w, align), val in zip(columns, row)
        )
        lines.append(" " + data_line)
        on_page += 1

    return lines


# ============================================================================
# %REPORT  (per-product WEEKLY/MONTHLY/QTRLY/HALFYRLY/YEARLY -> BEHAVE/HIGHLOW/MAXMIN)
# ============================================================================
def report(prod: str, store_df: pl.DataFrame):
    report_lines = []

    if store_df.height == 0:
        print(f"  [{prod}] No history available - skipping report.")
        return report_lines, None

    count = store_df.height
    acount  = count - 49
    wklyday = WKLYDATE.day
    if wklyday <= 8:
        wcount = count - 45
    elif wklyday <= 15:
        wcount = count - 46
    else:
        wcount = count - 47
    mcount = count - 44
    qcount = count - 36
    hcount = count - 24
    ycount = count

    qtr1 = _qtr_month(REPTDATE.month, 3)
    qtr2 = _qtr_month(REPTDATE.month, 6)
    qtr3 = _qtr_month(REPTDATE.month, 9)

    # --- WEEKLY --------------------------------------------------------
    weekly_rows = []
    for i in range(max(1, wcount), count + 1):  # clamp: short-history safety
        row = _nth(store_df, i)
        if row is None:
            continue
        d = row["DATE"]
        if d.day <= 8:
            weekstdt = d.replace(day=1) - timedelta(days=1)
        elif d.day <= 15:
            weekstdt = d.replace(day=8)
        elif d.day <= 22:
            weekstdt = d.replace(day=15)
        else:
            weekstdt = d.replace(day=22)
        # NOTE: original SAS CALL SYMPUT('LASTDT',WEEKSTDT) overrides the
        # separately-computed LASTDT variable with WEEKSTDT - replicated here
        # verbatim (faithful to the source, including this quirk).
        lastdt = weekstdt
        res = _process(store_df, weekstdt, d, lastdt)
        if res:
            weekly_rows.append(res)

    # --- MONTHLY ---------------------------------------------------------
    monthly_rows = []
    for i in range(max(1, mcount), count + 1):
        row = _nth(store_df, i)
        if row is None:
            continue
        d = row["DATE"]
        weekstdt = d.replace(day=1)
        lastdt = d.replace(day=1) - timedelta(days=1)
        res = _process(store_df, weekstdt, d, lastdt)
        if res:
            monthly_rows.append(res)
    if monthly_rows:
        last_date = monthly_rows[-1]["DATE"]
        monthly_rows = [
            r for r in monthly_rows
            if r["DATE"].day not in (8, 15, 22) or r["DATE"] == last_date
        ]

    # --- QTRLY -------------------------------------------------------------
    qtrly_rows = []
    for i in range(max(1, qcount), count + 1):
        row = _nth(store_df, i)
        if row is None:
            continue
        d = row["DATE"]
        setmthx = d.month - 2
        year = d.year
        if setmthx <= 0:
            setmth = 12 + setmthx
            year -= 1
        else:
            setmth = setmthx
        weekstdt = date(year, setmth, 1)
        lastdt = weekstdt - timedelta(days=1)
        res = _process(store_df, weekstdt, d, lastdt)
        if res:
            qtrly_rows.append(res)
    if qtrly_rows:
        last_date = qtrly_rows[-1]["DATE"]
        qtrly_rows = [
            r for r in qtrly_rows
            if r["DATE"] == last_date
            or (r["DATE"].day not in (8, 15, 22) and r["DATE"].month in (qtr1, qtr2, qtr3))
        ]

    # --- HALFYRLY ------------------------------------------------------
    halfyrly_rows = []
    for i in range(max(1, hcount), count + 1):
        row = _nth(store_df, i)
        if row is None:
            continue
        d = row["DATE"]
        setmthx = d.month - 5
        year = d.year
        if setmthx <= 0:
            setmth = 12 + setmthx
            year -= 1
        else:
            setmth = setmthx
        weekstdt = date(year, setmth, 1)
        lastdt = weekstdt - timedelta(days=1)
        res = _process(store_df, weekstdt, d, lastdt)
        if res:
            halfyrly_rows.append(res)
    if halfyrly_rows:
        last_date = halfyrly_rows[-1]["DATE"]
        halfyrly_rows = [
            r for r in halfyrly_rows
            if r["DATE"] == last_date
            or (r["DATE"].day not in (8, 15, 22) and r["DATE"].month == qtr2)
        ]

    # --- YEARLY --------------------------------------------------------
    yearly_rows = []
    for i in range(max(1, ycount), count + 1):
        row = _nth(store_df, i)
        if row is None:
            continue
        d = row["DATE"]
        setmthx = d.month - 11
        year = d.year
        if setmthx <= 0:
            setmth = 12 + setmthx
            year -= 1
        else:
            setmth = setmthx
        weekstdt = date(year, setmth, 1)
        lastdt = weekstdt - timedelta(days=1)
        res = _process(store_df, weekstdt, d, lastdt)
        if res:
            yearly_rows.append(res)
    if yearly_rows:
        yearly_rows = yearly_rows[-1:]   # IF LAST.STATUS; -> keep only last

    # --- BEHAVE base: tail window (rows with _N_ > ACOUNT) --------------
    behave_base = store_df.tail(max(0, count - acount)) if acount >= 0 else store_df
    outstand = float(store_df.row(-1, named=True)["BALANCE"] or 0.0)

    def _series_dict(rows, val_name):
        return {r["DATE"]: r["PCTAGE"] for r in rows} if rows else {}

    w_map = _series_dict(weekly_rows, "WPCTAGE")
    m_map = _series_dict(monthly_rows, "MPCTAGE")
    q_map = _series_dict(qtrly_rows, "QPCTAGE")
    h_map = _series_dict(halfyrly_rows, "HPCTAGE")
    y_map = _series_dict(yearly_rows, "YPCTAGE")

    behave_rows = []
    for r in behave_base.iter_rows(named=True):
        d = r["DATE"]
        behave_rows.append({
            "DATE": d,
            "WPCTAGE": w_map.get(d),
            "MPCTAGE": m_map.get(d),
            "QPCTAGE": q_map.get(d),
            "BALANCE": r["BALANCE"],
            "HPCTAGE": h_map.get(d),
            "YPCTAGE": y_map.get(d),
        })

    # --- Print BEHAVIORAL TABLE ------------------------------------------
    columns = [
        ("DATE", 10, "right"), ("WPCTAGE", 10, "right"), ("MPCTAGE", 10, "right"),
        ("QPCTAGE", 10, "right"), ("BALANCE", 15, "right"),
        ("HPCTAGE", 10, "right"), ("YPCTAGE", 10, "right"),
    ]
    rows_fmt = []
    for r in behave_rows:
        rows_fmt.append((
            r["DATE"].strftime("%d/%m/%y"),
            "" if r["WPCTAGE"] is None else f"{r['WPCTAGE']:.2f}",
            "" if r["MPCTAGE"] is None else f"{r['MPCTAGE']:.2f}",
            "" if r["QPCTAGE"] is None else f"{r['QPCTAGE']:.2f}",
            _fmt_comma(r["BALANCE"], 15, 2).strip(),
            "" if r["HPCTAGE"] is None else f"{r['HPCTAGE']:.2f}",
            "" if r["YPCTAGE"] is None else f"{r['YPCTAGE']:.2f}",
        ))
    report_lines += _print_table(
        [f"{prod} BEHAVIORAL TABLE {RDATE}"], columns, rows_fmt
    )

    # --- HIGHLOW -----------------------------------------------------------
    def _min_max(values):
        vals = [v for v in values if v is not None]
        if not vals:
            return None, None
        return min(vals), max(vals)

    lowest, highest = {}, {}
    for label, series in (("WEEK", w_map), ("MONTH", m_map), ("QTR", q_map),
                           ("HALFYR", h_map), ("YEAR", y_map)):
        lo, hi = _min_max(series.values())
        lowest[label] = lo if lo is not None else 0.0
        highest[label] = hi if hi is not None else 0.0

    # Monotonic adjustment + cap at +/-100
    if highest["WEEK"] > highest["MONTH"]:
        highest["MONTH"] = highest["WEEK"]
    if highest["MONTH"] > highest["QTR"]:
        highest["QTR"] = highest["MONTH"]
    if highest["QTR"] > highest["HALFYR"]:
        highest["HALFYR"] = highest["QTR"]
    if highest["HALFYR"] > highest["YEAR"]:
        highest["YEAR"] = highest["HALFYR"]
    for k in highest:
        if highest[k] > 100:
            highest[k] = 100.0

    if lowest["WEEK"] < lowest["MONTH"]:
        lowest["MONTH"] = lowest["WEEK"]
    if lowest["MONTH"] < lowest["QTR"]:
        lowest["QTR"] = lowest["MONTH"]
    if lowest["QTR"] < lowest["HALFYR"]:
        lowest["HALFYR"] = lowest["QTR"]
    if lowest["HALFYR"] < lowest["YEAR"]:
        lowest["YEAR"] = lowest["HALFYR"]
    for k in lowest:
        if lowest[k] < -100:
            lowest[k] = -100.0

    highlow_columns = [
        ("STATUS", 10, "left"), ("WEEK", 10, "right"), ("MONTH", 10, "right"),
        ("QTR", 10, "right"), ("HALFYR", 10, "right"), ("YEAR", 10, "right"),
    ]
    highlow_rows = [
        ("HIGHEST",
         f"{highest['WEEK']:.2f}", f"{highest['MONTH']:.2f}", f"{highest['QTR']:.2f}",
         f"{highest['HALFYR']:.2f}", f"{highest['YEAR']:.2f}"),
        ("LOWEST",
         f"{lowest['WEEK']:.2f}", f"{lowest['MONTH']:.2f}", f"{lowest['QTR']:.2f}",
         f"{lowest['HALFYR']:.2f}", f"{lowest['YEAR']:.2f}"),
    ]
    report_lines += _print_table([f"{prod} BEHAVIORAL TABLE {RDATE}"], highlow_columns, highlow_rows)

    # --- MAXMIN --------------------------------------------------------
    maxmin_pct = {
        k: max(abs(lowest[k]), abs(highest[k])) for k in ("WEEK", "MONTH", "QTR", "HALFYR", "YEAR")
    }

    week_amt   = round(maxmin_pct["WEEK"] * outstand / 100, 1)
    month_amt  = round((maxmin_pct["MONTH"] * outstand / 100) - week_amt, 1)
    qtr_amt    = round((maxmin_pct["QTR"] * outstand / 100) - (week_amt + month_amt))
    halfyr_amt = round((maxmin_pct["HALFYR"] * outstand / 100) - (week_amt + month_amt + qtr_amt))
    year_amt   = round((maxmin_pct["YEAR"] * outstand / 100) - (week_amt + month_amt + qtr_amt + halfyr_amt))
    last_amt   = round(outstand - (week_amt + month_amt + qtr_amt + halfyr_amt + year_amt))
    total_amt  = round(outstand)

    week_amt   = max(week_amt, 0)
    month_amt  = max(month_amt, 0)
    qtr_amt    = max(qtr_amt, 0)
    halfyr_amt = max(halfyr_amt, 0)
    year_amt   = max(year_amt, 0)

    maxmin_columns = [
        ("DESC", 10, "left"), ("WEEK", 15, "right"), ("MONTH", 15, "right"),
        ("QTR", 15, "right"), ("HALFYR", 15, "right"), ("YEAR", 15, "right"),
        ("LAST", 15, "right"), ("TOTAL", 15, "right"),
    ]
    maxmin_row = (
        prod, _fmt_comma(week_amt, 15, 0).strip(), _fmt_comma(month_amt, 15, 0).strip(),
        _fmt_comma(qtr_amt, 15, 0).strip(), _fmt_comma(halfyr_amt, 15, 0).strip(),
        _fmt_comma(year_amt, 15, 0).strip(), _fmt_comma(last_amt, 15, 0).strip(),
        _fmt_comma(total_amt, 15, 0).strip(),
    )
    report_lines += _print_table(
        [f"{prod} BEHAVIORAL FIGURE TO BE REPORTED {RDATE}"], maxmin_columns, [maxmin_row]
    )

    maxmin_dict = {
        "DESC": prod, "WEEK": week_amt, "MONTH": month_amt, "QTR": qtr_amt,
        "HALFYR": halfyr_amt, "YEAR": year_amt, "LAST": last_amt, "TOTAL": total_amt,
    }
    return report_lines, maxmin_dict


# ============================================================================
# STEP 8: MISAPPD + REPORT FOR EACH PRODUCT
# ============================================================================
print("\nStep 8: Running MISAPPD + REPORT per product...")

all_report_lines = []
maxmin_results = []

for prod in PRODUCTS:
    print(f"\n  Processing product: {prod}")
    store_df = misappd(prod)
    lines, maxmin_dict = report(prod, store_df)
    all_report_lines += lines
    if maxmin_dict:
        maxmin_results.append(maxmin_dict)

# ============================================================================
# STEP 9: STORE.BEHAVENOTE  (re-derive BNM code, second ITEM stage, DEPRMP1/DEPFXP1)
# ============================================================================
print("\nStep 9: Building STORE.BEHAVENOTE / DEPRMP1 / DEPFXP1...")

behavenote = pl.DataFrame(maxmin_results) if maxmin_results else pl.DataFrame(
    schema={"DESC": pl.Utf8, "WEEK": pl.Float64, "MONTH": pl.Float64, "QTR": pl.Float64,
            "HALFYR": pl.Float64, "YEAR": pl.Float64, "LAST": pl.Float64, "TOTAL": pl.Float64}
)

behavenote = behavenote.with_columns(
    pl.col("DESC").replace_strict(DESC_TO_BNMCODE2, default=None).alias("PROD")
).with_columns(
    pl.col("PROD").str.slice(5, 2).alias("INDNON")
)

item2_expr, item4b_expr = _item_expr(ITEM_MAP_STAGE2, "PROD")
behavenote = behavenote.with_columns([item2_expr, item4b_expr])

behavenote = behavenote.with_columns(
    sum(pl.col(c).fill_null(0.0) for c in ["WEEK", "MONTH", "QTR", "HALFYR", "YEAR", "LAST"]).alias("BALANCE_SUM")
)

_neg_cols2 = ["WEEK", "MONTH", "QTR", "HALFYR", "YEAR", "LAST", "BALANCE_SUM"]
behavenote = behavenote.with_columns(
    [(-pl.col(c).fill_null(0.0)).alias(c) for c in _neg_cols2]
).rename({"BALANCE_SUM": "BALANCE"})

deprmp1 = behavenote.filter(pl.col("PROD").str.slice(0, 2) == "93")
depfxp1 = behavenote.filter(pl.col("PROD").str.slice(0, 2) == "96")

behavenote.write_parquet(STORE_LIB / "behavenote.parquet")
deprmp1.write_parquet(STORE_LIB / "deprmp1.parquet")
depfxp1.write_parquet(STORE_LIB / "depfxp1.parquet")

# ============================================================================
# STEP 10: FINAL REPORT DATASET  (OPTIONS MISSING=0; - computed, not printed)
# ============================================================================
print("\nStep 10: Building final REPORT dataset (DEPRMP1-based, not printed)...")

report_ds = deprmp1.with_columns([
    pl.lit("DEPOSIT :").alias("ITEM2"),
    pl.when(pl.col("INDNON") == "08").then(pl.lit("INDIVIDUALS    "))
      .when(pl.col("INDNON") == "09").then(pl.lit("NON-INDIVUDUALS"))
      .otherwise(None).alias("ITEM3"),
]).with_columns(
    [(-pl.col(c).fill_null(0.0)).alias(c) for c in
     ["BALANCE", "WEEK", "MONTH", "QTR", "HALFYR", "YEAR", "LAST"]]
)
report_ds.write_parquet(STORE_LIB / "report.parquet")

# ============================================================================
# STEP 11: WRITE OUTPUT  (ASA carriage control, LRECL=250)
# ============================================================================
print("\nStep 11: Writing SASLIST output...")

with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in all_report_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(all_report_lines):,}")

print("\n--- Report preview (terminal echo) ---")
for ln in all_report_lines[:80]:
    print(ln)
if len(all_report_lines) > 80:
    print(f"... ({len(all_report_lines) - 80} more lines)")

gc.collect()
print("\nEIBMNLFE complete.")
