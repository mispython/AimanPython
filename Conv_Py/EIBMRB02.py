#!/usr/bin/env python3
"""
Program : EIBMRB02.py
Purpose : Monthly Savings / Current / Fixed Deposit (RM) outstanding amount by branch,
          PBB and PIBB (Report ID EIBMRB02).

Dependency:
    %INC PGM(PBBELF) -> from PBBELF import format_brchcd   (BRABBR=PUT(BRANCH,BRCHCD.))

Physical inputs (each cached to Parquet independently):
    DEPO.SAVING / IDEPO.SAVING   (SAP.PBB.MNITB / SAP.PIBB.MNITB)
    DEPO.CURRENT / IDEPO.CURRENT
    DEPO.FD / IDEPO.FD
    All file names are fixed (no date token), so input_date.py is not used.
    DEPO.REPTDATE is not read; the report date comes from REPTDATE.py.

Output:
    //SASLIST DD SAP.PBB.EIBMRB02 (LRECL=133, RECFM=FB) -> plain text, no ASA byte.
"""

import calendar
import gc
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from PBBELF import format_brchcd
from REPTDATE import get_reptdate_values

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR = Path("/stgsrcsys/host/uat/AII")

INPUT_DEPO_SAVING_FILE   = STG_DIR / "MNITB" / "PBB"  / "saving.sas7bdat"       # DEPO.SAVING
INPUT_IDEPO_SAVING_FILE  = STG_DIR / "MNITB" / "PIBB" / "saving.sas7bdat"       # IDEPO.SAVING
INPUT_DEPO_CURRENT_FILE  = STG_DIR / "MNITB" / "PBB"  / "current.sas7bdat"      # DEPO.CURRENT
INPUT_IDEPO_CURRENT_FILE = STG_DIR / "MNITB" / "PIBB" / "current.sas7bdat"      # IDEPO.CURRENT
INPUT_DEPO_FD_FILE       = STG_DIR / "MNITB" / "PBB"  / "fd.sas7bdat"           # DEPO.FD
INPUT_IDEPO_FD_FILE      = STG_DIR / "MNITB" / "PIBB" / "fd.sas7bdat"           # IDEPO.FD

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBMRBDP"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = BASE_DIR / "output" / "EIBMRBDP"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "EIBMRB02.txt"                # //SASLIST DD SAP.PBB.EIBMRB02

CHUNK_ROWS = 500_000
LRECL = 133                 # //SASLIST LRECL=133
MISSING_CHAR = "0"          # OPTIONS MISSING=0


# ============================================================================
# STEP 1: REPORT DATE
# ============================================================================
def derive_report_context() -> dict:
    """Macro-variable equivalents of the REPTDATE step."""
    reptdate = get_reptdate_values(year_format="%Y").reptdate
    return {
        "reptdate": reptdate,
        "reptyear": reptdate.strftime("%Y"),
        "reptmon": reptdate.strftime("%m"),
        "rdate": reptdate.strftime("%d/%m/%y"),
        "reptmth": calendar.month_name[reptdate.month].upper(),
    }


print("Step 1: Deriving report date...")
CTX = derive_report_context()
REPTYEAR_I = int(CTX["reptyear"])
REPTMON_I = int(CTX["reptmon"])
print(f"  RDATE: {CTX['rdate']}")


# ============================================================================
# HELPERS
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return cache_path.exists() and cache_path.stat().st_mtime >= sas_path.stat().st_mtime


def _sas_type_to_arrow(dtype) -> pa.DataType:
    if dtype == "object":
        return pa.string()
    if pd.api.types.is_integer_dtype(dtype):
        return pa.int64()
    if pd.api.types.is_float_dtype(dtype):
        return pa.float64()
    return pa.from_numpy_dtype(dtype)


def _sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0
    for chunk in pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS):
        if schema is None:
            schema = pa.schema([pa.field(c, _sas_type_to_arrow(t)) for c, t in chunk.dtypes.items()])
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")
        table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)
        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()
    if writer:
        writer.close()
    print(f"  [{tag}] Done - {total:,} rows cached.")


def _load_cached(sas_path: Path, tag: str) -> Path:
    cache_path = CACHE_DIR / f"{tag.replace('.', '_')}.parquet"   # e.g. DEPO_SAVING.parquet / IDEPO_SAVING.parquet
    if _cache_is_fresh(sas_path, cache_path):
        print(f"  [{tag}] Cache fresh - skipping conversion.")
    else:
        _sas_to_parquet(sas_path, cache_path, tag)
    return cache_path


def _read_pq(cache_path: Path, columns: str, where: str) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    try:
        return con.execute(
            f"SELECT {columns} FROM read_parquet('{cache_path.as_posix()}') WHERE {where}"
        ).pl()
    finally:
        con.close()


def _packed_mmddyy(col: str) -> pl.Expr:
    """INPUT(SUBSTR(PUT(x,Z11.),1,8),MMDDYY8.); 0 / missing -> null."""
    return (
        pl.col(col).cast(pl.Float64).cast(pl.Int64).cast(pl.Utf8)
        .str.zfill(11).str.slice(0, 8).str.strptime(pl.Date, "%m%d%Y", strict=False)
    )


def _in_report_month(col: str) -> pl.Expr:
    return (pl.col(col).dt.year() == REPTYEAR_I) & (pl.col(col).dt.month() == REPTMON_I)


def _put(col: int, *items: str) -> str:
    """PUT @col item1 item2 ... (items concatenated, no separator)."""
    return " " * (col - 1) + "".join(items)


def _best(value) -> str:
    if value is None:
        return MISSING_CHAR
    v = float(value)
    return str(int(v)) if v == int(v) else format(v, ".12g")


def _comma_text(value) -> str:
    """COMMA16. with leading blanks trimmed (list-style PUT). Rounds the exact double, as SAS does."""
    if value is None:
        return MISSING_CHAR
    return f"{Decimal(float(value)).quantize(Decimal(1), rounding=ROUND_HALF_UP):,.0f}"


def _put_wrapped(start_col: int, parts: list) -> list:
    """List-style PUT @start_col: each ('var', text) is written followed by one blank;
    each ('lit', text) is written as is. Anything that does not fit in LRECL starts a new line at column 1."""
    lines = []
    buf = " " * (start_col - 1)
    for kind, text in parts:
        if len(buf) + len(text) > LRECL:
            lines.append(buf)
            buf = ""
        buf += text + (" " if kind == "var" else "")
    lines.append(buf)
    return lines


# ============================================================================
# STEP 2: CACHE INPUTS
# ============================================================================
print("\nStep 2: Caching input SAS datasets to Parquet...")
DEPO_SAVING_CACHE   = _load_cached(INPUT_DEPO_SAVING_FILE, "DEPO.SAVING")
IDEPO_SAVING_CACHE  = _load_cached(INPUT_IDEPO_SAVING_FILE, "IDEPO.SAVING")
DEPO_CURRENT_CACHE  = _load_cached(INPUT_DEPO_CURRENT_FILE, "DEPO.CURRENT")
IDEPO_CURRENT_CACHE = _load_cached(INPUT_IDEPO_CURRENT_FILE, "IDEPO.CURRENT")
DEPO_FD_CACHE       = _load_cached(INPUT_DEPO_FD_FILE, "DEPO.FD")
IDEPO_FD_CACHE      = _load_cached(INPUT_IDEPO_FD_FILE, "IDEPO.FD")

# ============================================================================
# STEP 3: BUILD DEPOSIT  (SAVING / ISAVING / CURRENT / ICURRENT / FD / IFD)
# ============================================================================
print("\nStep 3: Building DEPOSIT...")

_PBB_CURRENT_EXCL = (
    "104,105,107,126,127,128,129,130,131,132,133,134,135,136,139,140,141,142,143,144,145,146,"
    "147,148,149,167,171,172,173,549,550,68,69,79"
)
_COLS = "CAST(BRANCH AS BIGINT) AS BRANCH, CAST(CURBAL AS DOUBLE) AS CURBAL, OPENDT, CLOSEDT"

# (cache, entity, extra WHERE, ATYPE, BTYPE). WHERE CURBAL GT 0 (DATA DEPOSIT) is applied on every source.
# NOT IN uses COALESCE(...,-1) so a missing PRODUCT passes, as in SAS.
_SOURCES = [
    (DEPO_CURRENT_CACHE, "PBB",
     f"TRIM(CURCODE) = 'MYR' AND COALESCE(PRODUCT,-1) NOT IN ({_PBB_CURRENT_EXCL}) "
     "AND NOT (COALESCE(ACCTNO,0) BETWEEN 3590000000 AND 3599999999)", 2, 1),
    (IDEPO_CURRENT_CACHE, "PIBB",
     "COALESCE(PRODUCT,-1) NOT IN (66,67,149,80) AND TRIM(CURCODE) = 'MYR' "
     "AND NOT (COALESCE(ACCTNO,0) BETWEEN 3790000000 AND 3799999999)", 2, 2),
    (DEPO_SAVING_CACHE, "PBB", "COALESCE(PRODUCT,-1) NOT IN (297,298,480,481,482,150,151,152,177,181)", 1, 1),
    (IDEPO_SAVING_CACHE, "PIBB", "COALESCE(PRODUCT,-1) NOT IN (298)", 1, 2),
    (DEPO_FD_CACHE, "PBB", "TRIM(CURCODE) = 'MYR' AND COALESCE(PRODUCT,-1) NOT IN (395)", 3, 1),
    (IDEPO_FD_CACHE, "PIBB", "TRIM(CURCODE) = 'MYR' AND COALESCE(PRODUCT,-1) NOT IN (396)", 3, 2),
]


def _load_source(cache: Path, label: str, extra: str, atype: int, btype: int) -> pl.DataFrame:
    # label ("PBB"/"PIBB") is informational only; the entity is determined by which file is read
    where = f"CURBAL > 0 AND {extra}"
    return _read_pq(cache, _COLS, where).with_columns(
        pl.lit(atype, dtype=pl.Int8).alias("ATYPE"),
        pl.lit(btype, dtype=pl.Int8).alias("BTYPE"),
    )


deposit = pl.concat([_load_source(*src) for src in _SOURCES])
deposit = deposit.with_columns(_packed_mmddyy("OPENDT").alias("OPENDT"), _packed_mmddyy("CLOSEDT").alias("CLOSEDT"))

# IF opened in report month AND closed in the same year/month as opened THEN DELETE;
same_month = (pl.col("CLOSEDT").dt.year() == pl.col("OPENDT").dt.year()) & (
    pl.col("CLOSEDT").dt.month() == pl.col("OPENDT").dt.month()
)
deposit = deposit.filter(~((_in_report_month("OPENDT") & same_month).fill_null(False)))
print(f"  DEPOSIT rows: {len(deposit):,}")

# ============================================================================
# STEP 4: PROC SUMMARY + TEMP (per-branch pivot) + TOTAL
# ============================================================================
print("\nStep 4: Summarising by branch...")


def _abbr(branch: int) -> str:
    """BRABBR=PUT(BRANCH,BRCHCD.): a branch with no mapping in the format prints as its own number (e.g. 998)."""
    text = format_brchcd(branch)
    text = str(text).strip() if text is not None else ""
    return text or str(branch)


# CLASS BRANCH: observations with a missing BRANCH are dropped.
dep = deposit.filter(pl.col("BRANCH").is_not_null())
branch_codes = np.sort(dep["BRANCH"].unique().to_numpy())
branch_abbr = {int(b): _abbr(int(b)) for b in branch_codes}

_PIVOT = {"SA": (1, 1), "ISA": (1, 2), "CA": (2, 1), "ICA": (2, 2), "FD": (3, 1), "IFD": (3, 2)}

slot = np.searchsorted(branch_codes, dep["BRANCH"].to_numpy())
curbal = dep["CURBAL"].to_numpy()
atype = dep["ATYPE"].to_numpy()
btype = dep["BTYPE"].to_numpy()

pivot = {}
for name, (a, b) in _PIVOT.items():
    mask = (atype == a) & (btype == b)
    acc = np.zeros(len(branch_codes), dtype=np.float64)
    np.add.at(acc, slot[mask], curbal[mask])      # sequential accumulation in input order, like PROC SUMMARY
    pivot[name] = acc

temp = pl.DataFrame({"BRANCH": branch_codes, **pivot}).with_columns(
    (pl.col("SA") + pl.col("ISA")).alias("TSA"),
    (pl.col("CA") + pl.col("ICA")).alias("TCA"),
    (pl.col("FD") + pl.col("IFD")).alias("TFD"),
)

# ============================================================================
# STEP 5: WRITE REPORT
# ============================================================================
_VALUE_COLS = ["SA", "ISA", "TSA", "CA", "ICA", "TCA", "FD", "IFD", "TFD"]


def _value_parts(rec: dict) -> list:
    parts = []
    for i, c in enumerate(_VALUE_COLS):
        if i:
            parts.append(("lit", ";"))
        parts.append(("var", _comma_text(rec[c])))
    return parts


report_lines = []
if not temp.is_empty():
    report_lines += [
        _put(1, "REPORT ID : EIBMRB02"),
        _put(1, "MONTHLY SAVINGS/CURRENT/FIXED DEPOSIT ACCOUNT OUTSTANDING", " AMOUNT"),
        _put(1, "AS AT ", CTX["rdate"]),
        _put(1, " "),
        _put(2, "BRANCH;BRANCH;;SAVING(RM) ;;;CURRENT(RM);;;FIXED DEPOSIT(RM)"),
        _put(2, "CODE;ABBR;PBB;PIBB;TOTAL;PBB;PIBB;TOTAL;PBB;PIBB;TOTAL"),
    ]
    for rec in temp.iter_rows(named=True):
        report_lines += _put_wrapped(
            2,
            [("var", _best(rec["BRANCH"])), ("lit", ";"), ("var", branch_abbr[int(rec["BRANCH"])]), ("lit", ";")]
            + _value_parts(rec),
        )
    # TOTAL (PROC SUMMARY without NWAY: first observation is the grand total), summed sequentially
    grand = {c: float(np.cumsum(temp[c].to_numpy())[-1]) for c in _VALUE_COLS}
    report_lines += _put_wrapped(2, [("lit", "TOTAL;;")] + _value_parts(grand))

with open(OUTPUT_FILE, "w", encoding="latin1", newline="\n") as fh:
    for ln in report_lines:
        fh.write(ln[:LRECL].ljust(LRECL) + "\n")        # RECFM=FB, LRECL=133

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(report_lines):,}")
# print("\n".join(ln.rstrip() for ln in report_lines))
print("\nEIBMRB02 complete.")
