#!/usr/bin/env python3
"""
Program : EIBMRB08.py
Purpose : Monthly FD receipts withdrawals by account balance, RM and FCY (Report ID EIBDRB08 as printed).

Dependency:
    %INC PGM(PBBELF): the SAS source calls BRABBR=PUT(BRANCH,BRCHCD.), but BRABBR is not in the KEEP
    list of FDRAW and is never printed, so it has no effect on any output. format_brchcd is
    therefore intentionally NOT imported.

Physical inputs (each cached to Parquet independently):
    MIS.FDWDRW&REPTMON (SAP.PBB.DP.SASDATA) - deterministic name from report-date tokens,
                                              so input_date.get_latest_file() is not used
    DEPOSIT.REPTDATE is not read; the report date comes from REPTDATE.py.

Outputs (fixed names, no date token, so output_date.py is not used):
    //RMWDRAW  DD SAP.PBB.EIBMRB8A (LRECL=500, RECFM=FB)  RM report
    //FCYWDRAW DD SAP.PBB.EIBMRB8B (LRECL=500, RECFM=FB)  FCY report
    RECFM=FB -> plain text, no ASA byte. Each file starts empty (DISP=NEW) and is appended (FILE ... MOD).

Notes on preserved SAS behaviour:
    * PROC SORT BY TRANAMT and the per-band sort do not change any aggregated figure, so they are omitted.
    * The IF-chain on CURBAL leaves gaps (e.g. 5000<x<5001, 10000000<x<=10000001); rows in a gap are
      not counted, exactly as in SAS. Rows with a missing CURBAL match no band and are excluded.
    * A1..A16 (amounts by reason) are computed in SAS but never printed, so they are not built.
"""

import calendar
import gc
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

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
STG_DIR = Path("/stgsrcsys/host/uat/AII")

INPUT_MIS_DIR = STG_DIR / "sasdata" / "dp_sasdata"        # //MIS DD SAP.PBB.DP.SASDATA

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBMRBDP"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = BASE_DIR / "output" / "EIBMRBDP"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_RMWDRAW_FILE = OUTPUT_DIR / "EIBMRB8A.txt"         # //RMWDRAW  DD SAP.PBB.EIBMRB8A
OUTPUT_FCYWDRAW_FILE = OUTPUT_DIR / "EIBMRB8B.txt"        # //FCYWDRAW DD SAP.PBB.EIBMRB8B

CHUNK_ROWS = 500_000
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
print(f"  RDATE: {CTX['rdate']}")

INPUT_FDWDRW_FILE = INPUT_MIS_DIR / f"fdwdrw{CTX['reptmon']}.sas7bdat"      # MIS.FDWDRW&REPTMON


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
# PROC FORMAT EQUIVALENTS (FDRNGE / $FDVALUE) - band logic follows the IF-chain in %MAINPROC
# ============================================================================
_FDVALUE = {
    "01": " 5K & BELOW    ", "02": " >  5K TO 10K  ", "03": " > 10K TO 15K  ", "04": " > 15K TO 20K  ",
    "05": " > 20K TO 25K  ", "06": " > 25K TO 30K  ", "07": " > 30K TO 35K  ", "08": " > 35K TO 40K  ",
    "09": " > 40K TO 45K  ", "10": " > 45K TO 50K  ", "11": " > 50K TO 55K  ", "12": " > 55K TO 60K  ",
    "13": " > 60K TO 65K  ", "14": " > 65K TO 70K  ", "15": " > 70K TO 75K  ", "16": " > 75K TO 80K  ",
    "17": " > 80K TO 85K  ", "18": " > 85K TO 90K  ", "19": " > 90K TO 95K  ", "20": " > 95K TO 100K ",
    "21": " > 100K TO 150K", "22": " > 150K TO 250K", "23": " > 250K TO 500K", "24": " > 500K TO 1M  ",
    "25": " > 1M TO 2M    ", "26": " > 2M TO 3M    ", "27": " > 3M TO 4M    ", "28": " > 4M TO 5M    ",
    "29": " > 5M TO 10M   ", "30": " > 10M         ",
}
_BAND_RANGES = [
    ("02", 5001, 10000), ("03", 10001, 15000), ("04", 15001, 20000), ("05", 20001, 25000),
    ("06", 25001, 30000), ("07", 30001, 35000), ("08", 35001, 40000), ("09", 40001, 45000),
    ("10", 45001, 50000), ("11", 50001, 55000), ("12", 55001, 60000), ("13", 60001, 65000),
    ("14", 65001, 70000), ("15", 70001, 75000), ("16", 75001, 80000), ("17", 80001, 85000),
    ("18", 85001, 90000), ("19", 90001, 95000), ("20", 95001, 100000), ("21", 100001, 150000),
    ("22", 150001, 250000), ("23", 250001, 500000), ("24", 500001, 1000000), ("25", 1000001, 2000000),
    ("26", 2000001, 3000000), ("27", 3000001, 4000000), ("28", 4000001, 5000000),
    ("29", 5000001, 10000000),
]
_NREASON = 16


def _band_expr() -> pl.Expr:
    chain = pl.when(pl.col("CURBAL") <= 5000).then(pl.lit("01"))
    for code, low, high in _BAND_RANGES:
        chain = chain.when(pl.col("CURBAL").is_between(low, high)).then(pl.lit(code))
    return chain.when(pl.col("CURBAL") > 10000001).then(pl.lit("30")).otherwise(pl.lit(None, dtype=pl.Utf8))


# ============================================================================
# STEP 2: CACHE INPUT + DATA WDRAW
# ============================================================================
print("\nStep 2: Caching input SAS datasets to Parquet...")
FDWDRW_CACHE = _load_cached(INPUT_FDWDRW_FILE, "MIS.FDWDRW")

print("\nStep 3: Loading WDRAW...")
wdraw = _read_pq(
    FDWDRW_CACHE,
    "CAST(CURBAL AS DOUBLE) AS CURBAL, CAST(TRANAMT AS DOUBLE) AS TRANAMT, "
    "COALESCE(TRIM(RSONCODE), '') AS RSONCODE, CAST(PRODCD AS BIGINT) AS PRODCD, "
    "COALESCE(TRIM(CURCODE), '') AS CURCODE, COALESCE(TRIM(ACCTTYPE), '') AS ACCTTYPE, "
    "CAST(CUSTCODE AS BIGINT) AS CUSTCODE",
    "TRUE",
)
print(f"  WDRAW rows: {len(wdraw):,}")


# ============================================================================
# STEP 4: %RMGROUP / %FCYGROUP  (WDRAW1..WDRAW4)
# ============================================================================
def _split_groups(df: pl.DataFrame, rm: bool) -> dict:
    if rm:
        # IF CURCODE EQ 'MYR' THEN DO; IF PRODCD NOT IN (394,393); -> a missing PRODCD passes
        base = df.filter((pl.col("CURCODE") == "MYR") & ~pl.col("PRODCD").is_in([394, 393]).fill_null(False))
    else:
        base = df.filter(pl.col("CURCODE") != "MYR")
    indiv = pl.col("CUSTCODE").is_in([77, 78, 95, 96]).fill_null(False)
    is_c = pl.col("ACCTTYPE") == "C"
    is_i = pl.col("ACCTTYPE") == "I"
    return {
        1: base.filter(is_c & indiv),
        2: base.filter(is_i & indiv),
        3: base.filter(is_c & ~indiv),
        4: base.filter(is_i & ~indiv),
    }


# ============================================================================
# STEP 5: %MAINPROC  (FDRAW&J and TOTAL&J)
# ============================================================================
def _band_summary(df: pl.DataFrame) -> pl.DataFrame:
    """One row per balance band: TC (all receipts), TA (amount), C1..C16 (receipts per reason code)."""
    banded = df.with_columns(_band_expr().alias("FDVAL")).filter(pl.col("FDVAL").is_not_null())
    aggs = [pl.len().alias("TC"), pl.col("TRANAMT").sum().alias("TA")]
    aggs += [(pl.col("RSONCODE") == f"W{i:02d}").sum().alias(f"C{i}") for i in range(1, _NREASON + 1)]
    return banded.group_by("FDVAL").agg(aggs).sort("FDVAL")


# ============================================================================
# STEP 6: %REPORT
# ============================================================================
_HDR = {1: "(I) PBB", 2: "(II) PIBB", 3: "(I) PBB", 4: "(II) PIBB"}
_C_COLS = [f"C{i}" for i in range(1, _NREASON + 1)]


def _intro_lines(j: int, rtitle: str) -> list:
    if j == 1:
        return [
            _put(1, "REPORT ID : EIBDRB08"),
            _put(1, "TITLE : MONTHLY REPORT ON ", rtitle, " ", "BY ACCOUNT ", "BALANCE"),
            _put(1, "REPORTING DATE : ", CTX["rdate"]),
            _put(1, " "),
            _put(1, "(A) INDIVIDUAL CUSTOMER (CUSTOMER CODE: 77,78,95 AND 96)"),
        ]
    if j == 3:
        return [_put(1, " "), _put(1, "(B) NON-INDIVIDUAL CUSTOMER")]
    return []


def _table_header(j: int) -> list:
    return [
        _put(1, " "),
        _put(4, _HDR[j]),
        _put(1, " "),
        _put(4, "ACCOUNT BALANCE AS ", ";", "WITHDRAWAL", ";;", "BREAKDOWN BY REASON CODE"),
        _put(4, "MONTH-END FOR TOTAL;NO. OF  ;AMOUNT;" + ";".join(f"W{i:02d}" for i in range(1, _NREASON + 1))),
        _put(4, "RECEIPT (BY RANGE);RECEIPTS; (RM)"),
    ]


def _section_lines(rtitle: str, groups: dict) -> list:
    lines = []
    for j in (1, 2, 3, 4):
        lines += _intro_lines(j, rtitle)
        summary = _band_summary(groups[j])
        if summary.is_empty():
            continue
        lines += _table_header(j)
        for rec in summary.iter_rows(named=True):
            counts = ";".join(_best(rec[c]) for c in _C_COLS)
            lines.append(
                _put(4, _FDVALUE[rec["FDVAL"]], ";", _best(rec["TC"]), ";", _comma(rec["TA"], 16), ";", counts)
            )
        totals = ";".join(_best(summary[c].sum()) for c in _C_COLS)
        lines.append(
            _put(4, "TOTAL", ";", _best(summary["TC"].sum()), ";", _comma(summary["TA"].sum(), 16), ";", totals)
        )
    return lines


print("\nStep 4: Rendering reports...")

# RM: RTITLE = PUT('FD RECEIPTS WITHDRAWALS (PBB&PIBB)',$34.) - single-quoted, so &PIBB stays literal.
rm_lines = _section_lines("FD RECEIPTS WITHDRAWALS (PBB&PIBB)", _split_groups(wdraw, rm=True))
# FCY: RTITLE = PUT('FCY FD RECEIPTS WITHDRAWALS',$27.)
fcy_lines = _section_lines("FCY FD RECEIPTS WITHDRAWALS", _split_groups(wdraw, rm=False))

for out_file, lines in ((OUTPUT_RMWDRAW_FILE, rm_lines), (OUTPUT_FCYWDRAW_FILE, fcy_lines)):
    with open(out_file, "w", encoding="latin1") as fh:
        for ln in lines:
            fh.write(ln + "\n")
    print(f"  Output written : {out_file}  ({len(lines):,} lines)")

print("\nEIBMRB08 complete.")
