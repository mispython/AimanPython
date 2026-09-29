#!/usr/bin/env python3
"""
Program : EIBMRB09.py
Purpose : Monthly report on reasons for RM FD withdrawals by receipts, broken down by product type
          (Report ID EIBMRB09).

Dependency:
    %INC PGM(PBBELF): the SAS source calls BRABBR=PUT(BRANCH,BRCHCD.), but BRABBR is not in the KEEP
    list of FDRAW and is never printed, so it has no effect on any output. format_brchcd is
    therefore intentionally NOT imported.

Physical inputs (each cached to Parquet independently):
    MIS.FDWDRW&REPTMON (SAP.PBB.DP.SASDATA) - deterministic name from report-date tokens,
                                              so input_date.get_latest_file() is not used
    DEPOSIT.REPTDATE is not read; the report date comes from REPTDATE.py.

Output:
    //SASLIST DD SAP.PBB.EIBMRB09 (LRECL=500, RECFM=FB) -> plain text, no ASA byte.
    Fixed name, no date token, so output_date.py is not used.

Preserved SAS behaviour:
    * C0..C9/A0..A9 are RETAINed and only reset when the first row of a group has RSONCODE W01-W16, so a
      group with any other RSONCODE is output with the previous group's figures.
    * The TOTAL line prints RA5 RC5 RA6 RC6 RA7 RC7 RA8 RC8 RA9 RC8 (swapped order and RC8 twice) exactly
      as written in the SAS source.
    * The "% COMPOSITION" line uses format 3. (whole numbers); a zero divisor gives missing (printed 0).
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
OUTPUT_FILE = OUTPUT_DIR / "EIBMRB09.txt"                 # //SASLIST DD SAP.PBB.EIBMRB09

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


def _int3(value) -> str:
    """Format 3. (round half away from zero, right-aligned); missing -> MISSING=0 character."""
    if value is None:
        return MISSING_CHAR.rjust(3)
    return str(int(Decimal(repr(float(value))).quantize(Decimal(1), rounding=ROUND_HALF_UP))).rjust(3)


def _pct(part, whole):
    """(RCi/GTC)*100; division by zero -> missing."""
    return None if not whole else part / whole * 100


# ============================================================================
# STEP 2: CACHE INPUT + DATA WDRAW
# ============================================================================
print("\nStep 2: Caching input SAS datasets to Parquet...")
FDWDRW_CACHE = _load_cached(INPUT_FDWDRW_FILE, "MIS.FDWDRW")

print("\nStep 3: Loading WDRAW...")
wdraw = _read_pq(
    FDWDRW_CACHE,
    "CAST(TRANAMT AS DOUBLE) AS TRANAMT, COALESCE(TRIM(RSONCODE), '') AS RSONCODE, "
    "CAST(PRODCD AS BIGINT) AS PRODCD, CAST(CUSTCODE AS BIGINT) AS CUSTCODE",
    "COALESCE(TRIM(CURCODE), '') = 'MYR' AND COALESCE(PRODCD, -1) NOT IN (394, 393)",
)
print(f"  WDRAW rows (MYR, PRODCD not 393/394): {len(wdraw):,}")

# %RMGROUP: individual customers -> WDRAW1, all others -> WDRAW2
_INDIV = pl.col("CUSTCODE").is_in([77, 78, 95, 96]).fill_null(False)
GROUPS = {1: wdraw.filter(_INDIV), 2: wdraw.filter(~_INDIV)}

# ============================================================================
# STEP 4: %MAINPROC  (FDRAW&J with RETAINed counters, TOTAL&J with percentages)
# ============================================================================
_PROD_INDEX = {300: 0, 301: 1, 302: 2, 303: 3, 315: 4, 316: 5, 311: 6, 312: 7, 313: 8, 314: 9}
_PRINT_ORDER = (0, 1, 3, 2, 4, 5, 6, 7, 8, 9)      # PLUS FD, FD LIFE, GOLDEN 50, MGIA-I, ISTISMAR, ...
_VALID_REASONS = {f"W{i:02d}" for i in range(1, 17)}


def _build_fdraw(df: pl.DataFrame) -> list:
    """DATA FDRAW&J: one row per RSONCODE, counters RETAINed across groups."""
    if df.is_empty():
        return []
    agg = df.group_by(["RSONCODE", "PRODCD"]).agg(pl.len().alias("N"), pl.col("TRANAMT").sum().alias("AMT"))
    by_code = {}
    for rec in agg.iter_rows(named=True):
        by_code.setdefault(rec["RSONCODE"], []).append(rec)

    cnt, amt, rows = [0] * 10, [0.0] * 10, []
    for code in sorted(by_code):
        if code in _VALID_REASONS:                       # IF FIRST.RSONCODE THEN reset (valid codes only)
            cnt, amt = [0] * 10, [0.0] * 10
            for rec in by_code[code]:
                idx = _PROD_INDEX.get(rec["PRODCD"])
                if idx is not None:
                    cnt[idx] += rec["N"]
                    amt[idx] += rec["AMT"] or 0.0
        rows.append({"code": code, "C": list(cnt), "A": list(amt), "TC": sum(cnt), "TA": sum(amt)})
    return rows


def _build_total(rows: list) -> dict:
    """PROC SUMMARY (grand total) + DATA TOTAL&J percentages."""
    rc = [sum(r["C"][i] for r in rows) for i in range(10)]
    ra = [sum(r["A"][i] for r in rows) for i in range(10)]
    gtc = sum(r["TC"] for r in rows)
    gta = sum(r["TA"] for r in rows)
    pc = [_pct(rc[i], gtc) for i in range(10)]
    pa_ = [_pct(ra[i], gta) for i in range(10)]
    return {
        "RC": rc, "RA": ra, "GTC": gtc, "GTA": gta, "PC": pc, "PA": pa_,
        "PGTC": sum(v for v in pc if v is not None),
        "PGTA": sum(v for v in pa_ if v is not None),
    }


# ============================================================================
# STEP 5: %REPORT
# ============================================================================
def _intro_lines(j: int) -> list:
    if j == 1:
        return [
            _put(1, "REPORT ID : EIBMRB09"),
            _put(1, "TITLE : MONTHLY REPORT ON REASON FOR FD WITHDRAWALS ", "BY RECEIPTS"),
            _put(9, "- BREAKDOWN BY PRODUCT TYPES"),
            _put(1, "REPORTING DATE : ", CTX["rdate"]),
            _put(1, " "),
            _put(1, "(A) INDIVIDUAL CUSTOMER (CUSTOMER CODE: 77,78,95 AND 96)"),
        ]
    return [_put(1, " "), _put(1, "(B) NON-INDIVIDUAL CUSTOMER")]


_PRODUCT_NAMES = [
    "PLUS FD", "FD LIFE", "PB GOLDEN 50 PLUS", "MGIA-I", "ISTISMAR",
    "TERM DEPOSIT-I", "EFD NON STAFF", "EFD STAFF", "ETDI NON STAFF", "ETDI STAFF",
]
_PRODUCT_CODES = [300, 301, 303, 302, 315, 316, 311, 312, 313, 314]


def _table_header() -> list:
    return [
        _put(1, " "),
        _put(4, "TYPES OF REASON", ";", "NO. OF RECEIPTS WITHDRAWN FOR THE MONTH OF ",
             CTX["reptmth"] + " ", CTX["reptyear"], ";" * 8, "TOTAL BY REASONS"),
        _put(4, ";" + ";;".join(_PRODUCT_NAMES)),
        _put(4, ";" + ";;".join(f"PRODUCT CODE: {c}" for c in _PRODUCT_CODES)),
        _put(4, ";" + ";".join(["NO.;RM"] * 11)),
    ]


def _row_line(row: dict) -> str:
    cells = []
    for i in _PRINT_ORDER:
        cells += [_best(row["C"][i]), _comma(row["A"][i], 16)]
    return _put(4, row["code"], ";", ";".join(cells), ";", _best(row["TC"]), ";", _comma(row["TA"], 16))


def _total_lines(t: dict) -> list:
    rc, ra, pc, pa_ = t["RC"], t["RA"], t["PC"], t["PA"]
    # Sequence exactly as coded in the SAS source (RA5 RC5 ... RA9 RC8 - swapped, RC8 repeated).
    total_cells = [
        _best(rc[0]), _comma(ra[0], 16), _best(rc[1]), _comma(ra[1], 16), _best(rc[3]), _comma(ra[3], 16),
        _best(rc[2]), _comma(ra[2], 16), _best(rc[4]), _comma(ra[4], 16),
        _comma(ra[5], 16), _best(rc[5]), _comma(ra[6], 16), _best(rc[6]), _comma(ra[7], 16),
        _best(rc[7]), _comma(ra[8], 16), _best(rc[8]), _comma(ra[9], 16), _best(rc[8]),
        _best(t["GTC"]), _comma(t["GTA"], 16),
    ]
    pct_cells = []
    for i in _PRINT_ORDER:
        pct_cells += [_int3(pc[i]), _int3(pa_[i])]
    pct_cells += [_int3(t["PGTC"]), _int3(t["PGTA"])]
    return [
        _put(4, "TOTAL", ";", ";".join(total_cells)),
        _put(4, "% COMPOSITION", ";", ";".join(pct_cells)),
    ]


print("\nStep 4: Rendering report...")
report_lines = []
for j in (1, 2):
    report_lines += _intro_lines(j)
    fdraw = _build_fdraw(GROUPS[j])
    if fdraw:
        report_lines += _table_header()
        report_lines += [_row_line(r) for r in fdraw]
        report_lines += _total_lines(_build_total(fdraw))

with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in report_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(report_lines):,}")
print("\nEIBMRB09 complete.")
