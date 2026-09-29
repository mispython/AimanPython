#!/usr/bin/env python3
"""
Program : EIBMRB05.py
Purpose : Month-end report by branch for FCY FD and FCY CA (PBB & PIBB) (Report ID EIBMRB05).

Dependency:
    %INC PGM(PBBDPFMT,PBBELF):
      from PBBELF    import format_brchcd      (BRABBR=PUT(BRANCH,BRCHCD.))
      from PBBDPFMT  import fdcustcd_format    (CUSTCD=PUT(CUSTCODE,FDCUSTCD.))
                            ddcustcd_format    (CUSTCD=PUT(CUSTCODE,DDCUSTCD.))
    PROC FORMAT FCYTYPE (1='FCY FD', 2='FCY CA') is defined in the SAS source but never
    referenced, so it has no Python equivalent.

Physical inputs (each cached to Parquet independently):
    MISFD.FCYFD&REPTMON&NOWK&REPTYEAR2 (SAP.PBB.FCFD) - deterministic name from report-date tokens,
                                                        so input_date.get_latest_file() is not used
    DEPO.CURRENT  (SAP.PBB.MNITB)   -> ENTITY_CD 'PBB'
    IDEPO.CURRENT (SAP.PIBB.MNITB)  -> ENTITY_CD 'PIBB'

Output:
    //SASLIST DD SAP.PBB.EIBMRB05 (LRECL=133, RECFM=FB) -> plain text, no ASA byte.
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

from PBBDPFMT import ddcustcd_format, fdcustcd_format
from PBBELF import format_brchcd
from REPTDATE import get_reptdate_values

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR = Path("/stgsrcsys/host/uat/AII")

INPUT_MISFD_DIR = STG_DIR / "sasdata" / "pbb_fcfd"                                     # //MISFD DD SAP.PBB.FCFD
INPUT_DEPO_CURRENT_FILE = STG_DIR / "sasdata" / "intg_dp_acct_current_d19.sas7bdat"    # DEPO.CURRENT
INPUT_IDEPO_CURRENT_FILE = STG_DIR / "sasdata" / "intg_dp_acct_current_d19.sas7bdat"   # IDEPO.CURRENT

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBMRBDP"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = BASE_DIR / "output" / "EIBMRBDP"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "EIBMRB05.txt"                 # //SASLIST DD SAP.PBB.EIBMRB05

CHUNK_ROWS = 500_000
MISSING_CHAR = "."          # default MISSING option


# ============================================================================
# STEP 1: REPORT DATE
# ============================================================================
def derive_report_context() -> dict:
    """Macro-variable equivalents of the REPTDATE step (NOWK: exact day 8/15/22, else 4)."""
    reptdate = get_reptdate_values(year_format="%Y").reptdate
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

INPUT_MISFD_FILE = INPUT_MISFD_DIR / f"fcyfd{CTX['reptmon']}{CTX['nowk']}{CTX['reptyear2']}.sas7bdat"


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
    cache_path = CACHE_DIR / f"{sas_path.stem}.parquet"
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


def _packed_mmddyy(col: str) -> pl.Expr:
    """INPUT(SUBSTR(PUT(x,Z11.),1,8),MMDDYY8.); 0 / missing -> null."""
    return (
        pl.col(col).cast(pl.Float64).cast(pl.Int64).cast(pl.Utf8)
        .str.zfill(11).str.slice(0, 8).str.strptime(pl.Date, "%m%d%Y", strict=False)
    )


def _in_report_month(col: str) -> pl.Expr:
    return (pl.col(col).dt.year() == REPTYEAR_I) & (pl.col(col).dt.month() == REPTMON_I)


def _put(col: int, *items: str) -> str:
    return " " * (col - 1) + "".join(items)


def _comma(value, width: int, decimals: int = 0) -> str:
    if value is None:
        return MISSING_CHAR.rjust(width)
    quant = Decimal(1).scaleb(-decimals)
    text = f"{Decimal(repr(float(value))).quantize(quant, rounding=ROUND_HALF_UP):,.{decimals}f}"
    return text.rjust(width)


def _int_w(value, width: int) -> str:
    """Numeric with format w. (integer, right-aligned)."""
    return str(int(round(value or 0))).rjust(width)


def _int_list(values) -> str:
    return ", ".join(str(v) for v in values)


# ============================================================================
# STEP 2: CACHE INPUTS
# ============================================================================
print("\nStep 2: Caching input SAS datasets to Parquet...")
MISFD_CACHE = _load_cached(INPUT_MISFD_FILE, "MISFD.FCYFD")
DEPO_CURRENT_CACHE = _load_cached(INPUT_DEPO_CURRENT_FILE, "DEPO.CURRENT")
IDEPO_CURRENT_CACHE = _load_cached(INPUT_IDEPO_CURRENT_FILE, "IDEPO.CURRENT")

# ============================================================================
# STEP 3: DATA FCYFD / DATA FCYCA / DATA FCY
# ============================================================================
print("\nStep 3: Building FCY (FCY FD + FCY CA)...")

_CUST_EXCLUDE = {2, 3, 7, 10, 12, 81, 82, 83, 84}
_FCYFD_PRODUCTS = tuple(range(350, 363))
_FCYCA_PRODUCTS = (
    tuple(range(400, 412)) + tuple(range(420, 432)) + (440, 441, 442, 443, 444, 450, 451, 452, 453, 454, 432, 433, 434)
)
_COLS = "CAST(BRANCH AS BIGINT) AS BRANCH, CAST(CURBAL AS DOUBLE) AS CURBAL, CUSTCODE, OPENDT"
_OPENIND_OK = "COALESCE(TRIM(OPENIND), '') NOT IN ('B','C','P')"      # IF OPENIND NOT IN ('B','C','P');


def _drop_excluded_customers(df: pl.DataFrame, fmt) -> pl.DataFrame:
    """IF CUSTCD NOT IN (02,03,07,10,12,81,82,83,84); CUSTCD=PUT(CUSTCODE,<fmt>.) is character,
    compared numerically by SAS."""
    excluded = [
        c for c in df["CUSTCODE"].drop_nulls().unique().to_list() if int(fmt(int(c))) in _CUST_EXCLUDE
    ]
    return df.filter(~pl.col("CUSTCODE").is_in(excluded).fill_null(False))


fcyfd = _read_pq(
    MISFD_CACHE, f"{_COLS}, 1 AS FTYPE",
    f"PRODUCT IN ({_int_list(_FCYFD_PRODUCTS)}) AND {_OPENIND_OK}",
)
fcyfd = _drop_excluded_customers(fcyfd, fdcustcd_format)
fcyfd = fcyfd.with_columns(_sas_date(fcyfd, "OPENDT").alias("OPENDT"))       # real SAS date here

_ca_where = (
    "COALESCE(TRIM(CURCODE), '') <> 'MYR' "
    f"AND PRODUCT IN ({_int_list(_FCYCA_PRODUCTS)}) AND {_OPENIND_OK}"
)
fcyca = pl.concat(
    [
        _read_pq(cache, f"{_COLS}, 2 AS FTYPE", f"TRIM(ENTITY_CD) = '{entity}' AND {_ca_where}")
        for cache, entity in ((DEPO_CURRENT_CACHE, "PBB"), (IDEPO_CURRENT_CACHE, "PIBB"))
    ]
)
fcyca = _drop_excluded_customers(fcyca, ddcustcd_format)
fcyca = fcyca.with_columns(_packed_mmddyy("OPENDT").alias("OPENDT"))          # packed MMDDYYYY here

fcy = pl.concat([fcyfd.drop("CUSTCODE"), fcyca.drop("CUSTCODE")]).with_columns(
    pl.when(_in_report_month("OPENDT")).then(1.0).otherwise(None).alias("ACNEW")
)
print(f"  FCY rows: {len(fcy):,}")

# ============================================================================
# STEP 4: PROC SUMMARY (BRANCH BRABBR FTYPE) + TEMP + TOTAL
# ============================================================================
print("\nStep 4: Summarising by branch...")

# CLASS BRANCH BRABBR FTYPE: observations with missing BRANCH or blank BRABBR are dropped.
branch_abbr = {int(b): format_brchcd(int(b)) for b in fcy["BRANCH"].drop_nulls().unique().to_list()}
valid_branches = [b for b, a in branch_abbr.items() if a]

ffcy = (
    fcy.filter(pl.col("BRANCH").is_in(valid_branches))
    .group_by(["BRANCH", "FTYPE"])
    .agg(pl.col("CURBAL").sum(), pl.col("ACNEW").sum(), pl.len().alias("FREQ"))
)


def _branch_measure(ftype: int, col: str) -> pl.Expr:
    return pl.col(col).filter(pl.col("FTYPE") == ftype).sum()


temp = (
    ffcy.group_by("BRANCH")
    .agg(
        _branch_measure(1, "CURBAL").alias("FD"),
        _branch_measure(1, "FREQ").alias("CFD"),
        _branch_measure(1, "ACNEW").alias("NFD"),
        _branch_measure(2, "CURBAL").alias("CA"),
        _branch_measure(2, "FREQ").alias("CCA"),
        _branch_measure(2, "ACNEW").alias("NCA"),
    )
    .with_columns((pl.col("FD") + pl.col("CA")).alias("TA"))
    .sort("BRANCH")
)

# ============================================================================
# STEP 5: WRITE REPORT
# ============================================================================
def _measures(rec: dict) -> str:
    """FD CFD NFD CA CCA NCA TA with FORMAT CFD NFD CCA NCA 10.; FD CA TA COMMA20.2;"""
    return ";".join(
        [
            _comma(rec["FD"], 20, 2), _int_w(rec["CFD"], 10), _int_w(rec["NFD"], 10),
            _comma(rec["CA"], 20, 2), _int_w(rec["CCA"], 10), _int_w(rec["NCA"], 10),
            _comma(rec["TA"], 20, 2),
        ]
    )


report_lines = []
if not temp.is_empty():
    report_lines += [
        _put(1, "REPORT ID : EIBMRB05"),
        _put(1, "MONTH END REPORT BY BRANCH FOR FCY FD AND FCY CA ", "(PBB & PIBB)"),
        _put(1, "AS AT ", CTX["rdate"]),
        _put(1, " "),
        _put(2, "BRANCH CODE;BRANCH ABBR;;FCY FD;;;FCY CA;;;"),
        _put(2, ";;;NO OF O/S;NO OF NEW;;NO OF O/S;NO OF NEW;;"),
        _put(2, ";;CURRENT BALANCE;ACCOUNT;ACC OPENED;CURRENT BALANCE;ACCOUNT;ACC OPENED;TOT AMT O/S RM;"),
    ]
    for rec in temp.iter_rows(named=True):
        report_lines.append(
            _put(2, str(int(rec["BRANCH"])), ";", branch_abbr[int(rec["BRANCH"])], ";", _measures(rec), ";")
        )
    # DATA _NULL_; SET TOTAL; IF _N_=1 -> grand total (PROC SUMMARY without NWAY, first observation)
    grand = {c: temp[c].sum() for c in ("FD", "CFD", "NFD", "CA", "CCA", "NCA", "TA")}
    report_lines.append(_put(2, "TOTAL", ";", ";", _measures(grand), ";"))

with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in report_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(report_lines):,}")
print("\nEIBMRB05 complete.")
