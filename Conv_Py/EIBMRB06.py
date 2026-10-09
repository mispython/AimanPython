#!/usr/bin/env python3
"""
Program : EIBMRB06.py
Purpose : Listing of new FCY FD opened for the month (Report ID EIBMRB06).

Dependency:
    %INC PGM(PBBELF) -> from PBBELF import format_brchcd   (BRABBR=PUT(BRANCH,BRCHCD.))

Physical inputs (each cached to Parquet independently):
    MISFD.FCYFD&REPTMON&NOWK&REPTYEAR2 (SAP.PBB.FCFD) - deterministic name from report-date tokens,
                                                        so input_date.get_latest_file() is not used
    CIS.DEPOSIT (SAP.PBB.CRM.CISBEXT)

Output:
    //SASLIST DD SAP.PBB.EIBMRB06 (LRECL=155, RECFM=FB) -> plain text, no ASA byte.
"""

import calendar
import gc
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import duckdb
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
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

INPUT_MISFD_DIR        = STG_DIR / "EIBMRBDP"                                      # //MISFD DD SAP.PBB.FCFD
INPUT_CIS_DEPOSIT_FILE = STG_DIR / "CIS" / "CRM_CISBEXT" / "deposit.sas7bdat"      # //CIS DD SAP.PBB.CRM.CISBEXT

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBMRBDP"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = BASE_DIR / "output" / "EIBMRBDP"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "EIBMRB06.txt"                 # //SASLIST DD SAP.PBB.EIBMRB06

CHUNK_ROWS   = 500_000
MISSING_CHAR = "."          # default MISSING option
LRECL        = 155          # //SASLIST DD LRECL=155 (records padded with blanks)


# ============================================================================
# STEP 1: REPORT DATE
# ============================================================================
def derive_report_context() -> dict:
    """Macro-variable equivalents of the REPTDATE step (NOWK: exact day 8/15/22, else 4)."""

    # reptdate = get_reptdate_values(year_format="%Y").reptdate

    # DEBUG - UAT override
    reptdate = date(2026, 9, 30)
    
    return {
        "reptdate": reptdate,
        "nowk": {8: "1", 15: "2", 22: "3"}.get(reptdate.day, "4"),
        "reptyear": reptdate.strftime("%Y"),
        "reptyear2": reptdate.strftime("%y"),
        "reptmon": reptdate.strftime("%m"),
        "rdate": reptdate.strftime("%d/%m/%y"),
        "reptmth": calendar.month_name[reptdate.month].upper(),
    }


print("Step 1: Deriving report date...")
CTX = derive_report_context()
REPTYEAR_I = int(CTX["reptyear"])
REPTMON_I = int(CTX["reptmon"])
print(f"  RDATE: {CTX['rdate']}  NOWK: {CTX['nowk']}")

# INPUT_MISFD_FILE = INPUT_MISFD_DIR / f"fcyfd{CTX['reptmon']}{CTX['nowk']}{CTX['reptyear2']}.sas7bdat"
INPUT_MISFD_FILE = INPUT_MISFD_DIR / f"fcyfd09426.sas7bdat"


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
    # Cache name = immediate parent folder + file stem, e.g. CISBEXT_DP_deposit.parquet,
    # so datasets with the same file name in different subfolders never share a cache.
    cache_path = CACHE_DIR / f"{sas_path.parent.name}_{sas_path.stem}.parquet"
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


def _sas_date(df: pl.DataFrame, col: str) -> pl.Expr:
    """SAS date value (days since 1960-01-01, or already a date/datetime) -> Date."""
    dtype = df.schema[col]
    if dtype == pl.Date:
        return pl.col(col)
    if isinstance(dtype, pl.Datetime):
        return pl.col(col).dt.date()
    days = pl.duration(days=pl.col(col).cast(pl.Float64).cast(pl.Int64))
    return (pl.lit(date(1960, 1, 1)) + days).cast(pl.Date)


def _in_report_month(col: str) -> pl.Expr:
    return (pl.col(col).dt.year() == REPTYEAR_I) & (pl.col(col).dt.month() == REPTMON_I)


def _line(*fields: tuple) -> str:
    """PUT @col text @col text ... (column-pointer output)."""
    line = ""
    for col, text in fields:
        line = line.ljust(col - 1)
        line = line[: col - 1] + text + line[col - 1 + len(text):]
    return line


def _put(col: int, *items: str) -> str:
    return " " * (col - 1) + "".join(items)


def _best(value) -> str:
    if value is None:
        return MISSING_CHAR
    v = float(value)
    return str(int(v)) if v == int(v) else format(v, ".12g")


def _comma(value, width: int, decimals: int = 0) -> str:
    if value is None:
        return MISSING_CHAR.rjust(width)
    quant = Decimal(1).scaleb(-decimals)
    text = f"{Decimal(repr(float(value))).quantize(quant, rounding=ROUND_HALF_UP):,.{decimals}f}"
    return text.rjust(width)


# ============================================================================
# STEP 2: CACHE INPUTS
# ============================================================================
print("\nStep 2: Caching input SAS datasets to Parquet...")
MISFD_CACHE = _load_cached(INPUT_MISFD_FILE, "MISFD.FCYFD")
CIS_CACHE = _load_cached(INPUT_CIS_DEPOSIT_FILE, "CIS.DEPOSIT")

# ============================================================================
# STEP 3: DATA CISFD / DATA FCYFD / MERGE
# ============================================================================
print("\nStep 3: Building FCYFD...")

# NAME is in the SAS KEEP list but is never printed (CUSTNAME from CIS is), so it is not loaded.
fcyfd = _read_pq(
    MISFD_CACHE,
    "CAST(BRANCH AS BIGINT) AS BRANCH, CAST(ACCTNO AS BIGINT) AS ACCTNO, "
    "CAST(CURBAL AS DOUBLE) AS CURBAL, OPENDT, CLOSEDT, TRIM(CURCODE) AS CURCODE",
    "COALESCE(CUSTCODE,-1) NOT IN (77,78,95,96)",
)
# OPENDT / CLOSEDT are real SAS dates in the FCYFD MIS dataset (no MMDDYY conversion in the SAS source).
fcyfd = fcyfd.with_columns(_sas_date(fcyfd, "OPENDT").alias("OPENDT"), _sas_date(fcyfd, "CLOSEDT").alias("CLOSEDT"))
fcyfd = fcyfd.filter(
    _in_report_month("OPENDT").fill_null(False) & ~_in_report_month("CLOSEDT").fill_null(False)
)

# DATA CISFD: KEEP ACCTNO CUSTNAME. SAS MERGE (last wins) -> keep last row per ACCTNO.
cisfd = _read_pq(CIS_CACHE, "CAST(ACCTNO AS BIGINT) AS ACCTNO, CUSTNAME", "TRUE").unique(
    subset="ACCTNO", keep="last", maintain_order=True
)

# MERGE FCYFD(IN=A) CISFD; BY ACCTNO; IF A;  then PROC SORT BY BRANCH ACCTNO.
fcyfd = fcyfd.join(cisfd, on="ACCTNO", how="left").sort(["BRANCH", "ACCTNO"], maintain_order=True)
print(f"  FCYFD rows: {len(fcyfd):,}")

# ============================================================================
# STEP 4: WRITE LISTING
# ============================================================================
branch_abbr = {int(b): format_brchcd(int(b)) for b in fcyfd["BRANCH"].drop_nulls().unique().to_list()}

report_lines = []
if not fcyfd.is_empty():
    report_lines += [
        _put(1, " "),
        _put(1, "REPORT ID : EIBMRB06"),
        _put(1, "LISTING OF NEW FCY FD OPENED FOR THE MONTH OF ", CTX["reptmth"], " OF YEAR ", CTX["reptyear"]),
        _put(1, "AS AT ", CTX["rdate"]),
        _put(1, " "),
        _line((3, "BRANCH CODE"), (18, "BRANCH ABBR"), (34, "ACCOUNT NAME"), (77, "ACCOUNT NO"),
              (92, "CURRENT BALANCE (RM)"), (116, "DATE ACCOUNT OPENED"), (140, "CURRENCY CODE")),
        _put(3, "-" * 150),
    ]
    for r in fcyfd.iter_rows(named=True):
        report_lines.append(
            _line(
                (3, _best(r["BRANCH"]).rjust(6)),
                (22, branch_abbr.get(r["BRANCH"], "")),
                (34, r["CUSTNAME"] or ""),
                (77, _best(r["ACCTNO"])),
                (97, _comma(r["CURBAL"], 14, 2)),
                (122, r["OPENDT"].strftime("%d/%m/%y")),
                (145, r["CURCODE"] or ""),
            )
        )

with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in report_lines:
        fh.write(ln.ljust(LRECL) + "\n")        # RECFM=FB, LRECL=155: pad with blanks, no ASA byte

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(report_lines):,}")
print("\nEIBMRB06 complete.")
