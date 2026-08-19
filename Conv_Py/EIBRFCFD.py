#!/usr/bin/env python3
"""
Program : EIBRFCFD.py
Purpose : Foreign Currency Fixed Deposit (FCY FD) Average/Current Balance
          Extract — builds a CURCODE x BRANCH grid of month-to-date average
          balances (by tenure bucket) and latest current balance, written
          as a semicolon-delimited text extract (FCYFDA...TXT).

============================================================================
PHYSICAL INPUT DATASETS USED BY THIS PROGRAM (all .sas7bdat, cached to
Parquet on first read per EIBDLN1M.py's chunked-conversion pattern)
============================================================================
1. FD.REPTDATE  (SAS libref FD -> SAP.PBB.MNIFD(0))
   NOT read as a physical file in this conversion. Original SAS derives
   REPTMON/REPTDAY/REPTYRA/XDATE/SDATE from this dataset's REPTDATE field.
   Replaced entirely by REPTDATE.py's get_reptdate_values() per project
   convention (no reptdate.parquet exists).

2. FDM.FCY&REPTMON  (SAS libref FDM -> SAP.PBB.FCYSTAT)
   File     : fcy<REPTMON>.sas7bdat   (REPTMON = 2-digit month, deterministic
              from the report date — NOT resolved via input_date.get_latest_file,
              since the filename is fully predictable from REPTMON alone)
   Path     : INPUT_FDM_FCY_DIR
   Columns  : ACCTNO, CDNO, REPTDATE, CURCODE, BRANCH, TENURE, CURBAL
   Used in  : building FCY (all records in current month-to-date window) and
              FCY01 (subset with CURBAL > 0, deduplicated to latest record
              per ACCTNO+CDNO)

------------------------------------------------------------------------
NON-FILE INPUT: PBBDPFMT FORMAT LIBRARY
------------------------------------------------------------------------
%INC PGM(PBBDPFMT); loads the PBBDPFMT format library into the SAS session.
No PUT(var, <PBBDPFMT-format>.) call (format_caprod, format_fdprod,
format_sadenom, etc.) appears anywhere in this program body, so no format
function from PBBDPFMT.py is imported here — the include is session-level
boilerplate only.

============================================================================
OUTPUT
============================================================================
FDFA -> SAP.PBB.FCY.TEXT.A
   Semicolon-delimited text extract (no ASA carriage control — this is a
   flat data extract, not a printed report). Header row + one detail row
   per CURCODE/BRANCH combination + a SUBT subtotal row (and blank line)
   per CURCODE group.
   Path: OUTPUT_DIR / FCYFDA_<ddmmyy>.txt  (via output_date.build_output_file)

Secondary terminal-only output: PROC SUMMARY CLASS CURCODE + PROC PRINT of
FYY — this never wrote to a physical file in the original SAS (no OUT= file
association beyond the SAS listing), so it is reproduced as a terminal
summary table only.
"""

import gc
from pathlib import Path
from datetime import date

import duckdb
import polars as pl
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values
# from output_date import build_output_file
# from input_date import get_latest_file
# --- NOT used: FCY filename is fully deterministic from REPTMON, so no
#     latest-file directory scan is required (see module docstring).

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

INPUT_FDM_FCY_DIR = STG_DIR / "EIBRFCFD"                      # fcy<REPTMON>.sas7bdat

CACHE_DIR  = BASE_DIR / "input" / "cache" / "EIBRFCFD"
OUTPUT_DIR = BASE_DIR / "output" / "EIBRFCFD"

CACHE_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# STEP 1: REPORT DATE  (no reptdate.parquet — derive from REPTDATE.py)
# DATA REPTDATE; SET FD.REPTDATE; DD=DAY(REPTDATE); SDATE=(REPTDATE-DD)+1;
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values()
reptdate = reptdate_values.reptdate          # SAS: REPTDATE
REPTMON  = reptdate_values.reptmon           # PUT(MONTH(REPTDATE),Z2.)
REPTDAY  = reptdate_values.reptday           # PUT(DAY(REPTDATE),Z2.)
REPTYRA  = str(reptdate.year)                # PUT(YEAR(REPTDATE),Z2.) -> Z2. does
                                              # not truncate a 4-digit year; unused
                                              # elsewhere in the SAS program body.

DAYS = int(REPTDAY)                          # DAYS=&REPTDAY (divisor for averages)

SDATE = reptdate.replace(day=1)              # (REPTDATE-DD)+1 -> first day of month
XDATE = reptdate                             # REPTDATE itself (upper bound of filter)
RDATE = reptdate.strftime("%y%m%d")

# SDATE = date(2026, 7, 1)
# XDATE = date(2026, 7, 31)
# RDATE = reptdate.strftime("%y%m%d")

print(f"  Report date  : {reptdate.isoformat()}")
print(f"  REPTMON/DAY  : {REPTMON}/{REPTDAY}  (year {REPTYRA})")
print(f"  SDATE..XDATE : {SDATE.isoformat()} .. {XDATE.isoformat()}")

# OUTPUT_FILE = Path(str(build_output_file(OUTPUT_DIR, "FCYFDA", "ddmmyy")) + ".txt")
OUTPUT_FILE = OUTPUT_DIR / f"FCYFDA_{RDATE}.txt"
print(f"  Output file  : {OUTPUT_FILE.name}")

# ============================================================================
# STEP 2: RESOLVE FCY MONTHLY INPUT FILE  (deterministic from REPTMON)
# ============================================================================
fcy_sas_path = INPUT_FDM_FCY_DIR / f"fcy{REPTMON}.sas7bdat"
# fcy_sas_path = INPUT_FDM_FCY_DIR / f"fcy07.sas7bdat"
print(f"\nStep 2: FCY input file -> {fcy_sas_path}")

# ============================================================================
# HELPER: CACHE STAMP + STREAM .sas7bdat -> PARQUET
# (same freshness-check / ParquetWriter pattern as EIBDLN1M.py)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


def _sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    df = pd.read_sas(sas_path, encoding="latin1")
    table = pa.Table.from_pandas(df, preserve_index=False)
    writer = pq.ParquetWriter(cache_path, table.schema, compression="snappy")
    writer.write_table(table)
    writer.close()
    print(f"  [{tag}] Done — {len(df):,} rows cached.")


def _load_cached(sas_path: Path, tag: str) -> Path:
    cache_path = CACHE_DIR / f"{sas_path.stem}.parquet"
    if _cache_is_fresh(sas_path, cache_path):
        print(f"  [{tag}] Cache fresh — skipping conversion.")
    else:
        _sas_to_parquet(sas_path, cache_path, tag)
    return cache_path


# ============================================================================
# STEP 3: CACHE FCY MONTHLY FILE TO PARQUET
# ============================================================================
print("\nStep 3: Caching FCY monthly file to Parquet (if needed)...")
fcy_cache = _load_cached(fcy_sas_path, "FDM_FCY")

# ============================================================================
# STEP 4: BUILD FDDUM  (currency x branch skeleton, zero balances)
# DATA FDDUM; DO I=002 TO 280; ... CURCODE='USD'/'AUD'/.../'CNY'; OUTPUT; END;
# DATA FDDUM; SET FDDUM; IF BRANCH IN (...) THEN DELETE;
# ============================================================================
print("\nStep 4: Building FDDUM skeleton (currency x branch, zero balances)...")

_FDDUM_CURRENCIES = ["USD", "AUD", "JPY", "EUR", "GBP", "SGD", "HKD", "NZD", "CAD", "CNY"]
_FDDUM_EXCLUDED_BRANCHES = {
    12, 82, 84, 98, 99, 100, 119, 132, 134, 166,
    181, 182, 188, 212, 213, 214, 215, 218, 223,
    227, 229, 236, 246, 255,
}

_fddum_rows = [
    {
        "CURCODE": cur, "BRANCH": br,
        "AVGB01": 0.0, "AVGB03": 0.0, "AVGB06": 0.0, "AVGB12": 0.0,
        "AVGBAL": 0.0, "CURBAL": 0.0,
    }
    for cur in _FDDUM_CURRENCIES
    for br in range(2, 281)
    if br not in _FDDUM_EXCLUDED_BRANCHES
]
fddum = pl.DataFrame(_fddum_rows)
print(f"  FDDUM rows: {len(fddum):,}")

# ============================================================================
# STEP 5: BUILD FCY / FCY01  (filter to current month-to-date window)
# DATA FCY FCY01; SET FDM.FCY&REPTMON; IF (&SDATE<=REPTDATE<=&XDATE);
#   OUTPUT FCY; IF CURBAL > 0.00 THEN OUTPUT FCY01;
# ============================================================================
print("\nStep 5: Reading and filtering FCY monthly data...")

SAS_EPOCH = date(1960, 1, 1)
sas_sdate = (SDATE - SAS_EPOCH).days   # integer SAS date for first of month
sas_xdate = (XDATE - SAS_EPOCH).days   # integer SAS date for report date

con = duckdb.connect(database=":memory:")
# fcy_filtered = con.execute(f"""
#     SELECT
#         CAST(ACCTNO   AS VARCHAR) AS ACCTNO,
#         CAST(CDNO     AS VARCHAR) AS CDNO,
#         CAST(REPTDATE AS DATE)    AS REPTDATE,
#         CAST(CURCODE  AS VARCHAR) AS CURCODE,
#         CAST(BRANCH   AS INTEGER) AS BRANCH,
#         CAST(TENURE   AS VARCHAR) AS TENURE,
#         CAST(CURBAL   AS DOUBLE)  AS CURBAL
#     FROM read_parquet('{fcy_cache.as_posix()}')
#     WHERE CAST(REPTDATE AS DATE) BETWEEN DATE '{SDATE.isoformat()}'
#                                       AND DATE '{XDATE.isoformat()}'
# """).pl()

fcy_filtered = con.execute(f"""
    SELECT
        CAST(ACCTNO   AS VARCHAR) AS ACCTNO,
        CAST(CDNO     AS VARCHAR) AS CDNO,
        REPTDATE,
        CAST(CURCODE  AS VARCHAR) AS CURCODE,
        CAST(BRANCH   AS INTEGER) AS BRANCH,
        CAST(TENURE   AS VARCHAR) AS TENURE,
        CAST(CURBAL   AS DOUBLE)  AS CURBAL
    FROM read_parquet('{fcy_cache.as_posix()}')
    WHERE REPTDATE BETWEEN {sas_sdate} AND {sas_xdate}
""").pl()
con.close()
gc.collect()

fcy01 = fcy_filtered.filter(pl.col("CURBAL") > 0.0)
print(f"  FCY rows   : {len(fcy_filtered):,}")
print(f"  FCY01 rows : {len(fcy01):,}")

# ============================================================================
# STEP 6: DEDUPLICATE FCY01  (latest REPTDATE per ACCTNO+CDNO)
# PROC SORT DATA=FCY01; BY ACCTNO CDNO DESCENDING REPTDATE;
# PROC SORT DATA=FCY01 OUT=FCY01 NODUPKEY; BY ACCTNO CDNO;
# ============================================================================
print("\nStep 6: Deduplicating FCY01 (latest REPTDATE per ACCTNO+CDNO)...")

fcy01_dedup = (
    fcy01.sort(["ACCTNO", "CDNO", "REPTDATE"], descending=[False, False, True])
         .unique(subset=["ACCTNO", "CDNO"], keep="first")
)
print(f"  FCY01 deduped rows: {len(fcy01_dedup):,}")

# ============================================================================
# STEP 7: SUMMARIZE CURRENT BALANCE  (FCY01 -> CURCODE+BRANCH)
# PROC SUMMARY DATA=FCY01 NWAY; CLASS CURCODE BRANCH; VAR CURBAL;
# OUTPUT OUT=FCY01(DROP=_TYPE_) SUM=;
# ============================================================================
curbal_df = (
    fcy01_dedup.group_by(["CURCODE", "BRANCH"])
    .agg(pl.col("CURBAL").sum().alias("CURBAL"))
)

# ============================================================================
# STEP 8: SUMMARIZE AVERAGE BALANCE  (FCY -> CURCODE+BRANCH+TENURE, then
#         collapse TENURE into AVGB01/03/06/12 buckets and re-summarize)
# PROC SUMMARY DATA=FCY NWAY; CLASS CURCODE BRANCH TENURE; VAR CURBAL;
# OUTPUT OUT=FCY(DROP=_TYPE_) SUM=;
# DATA FCY; SET FCY; DAYS=&REPTDAY;
#   AVGB01=0; AVGB03=0; AVGB06=0; AVGB12=0;
#   IF TENURE='01-MONTH' THEN AVGB01=CURBAL/DAYS;  (etc.)
#   AVGBAL=SUM(AVGB01,AVGB03,AVGB06,AVGB12);
# PROC SUMMARY DATA=FCY NWAY; CLASS CURCODE BRANCH;
# VAR AVGB01 AVGB03 AVGB06 AVGB12 AVGBAL; OUTPUT OUT=FCY(DROP=_TYPE_) SUM=;
# ============================================================================
print("\nStep 7/8: Building average-balance summary by tenure bucket...")

avg_by_tenure = (
    fcy_filtered.group_by(["CURCODE", "BRANCH", "TENURE"])
    .agg(pl.col("CURBAL").sum().alias("CURBAL"))
)

avg_by_tenure = avg_by_tenure.with_columns([
    pl.when(pl.col("TENURE") == "01-MONTH").then(pl.col("CURBAL") / DAYS).otherwise(0.0).alias("AVGB01"),
    pl.when(pl.col("TENURE") == "03-MONTH").then(pl.col("CURBAL") / DAYS).otherwise(0.0).alias("AVGB03"),
    pl.when(pl.col("TENURE") == "06-MONTH").then(pl.col("CURBAL") / DAYS).otherwise(0.0).alias("AVGB06"),
    pl.when(pl.col("TENURE") == "12-MONTH").then(pl.col("CURBAL") / DAYS).otherwise(0.0).alias("AVGB12"),
])
avg_by_tenure = avg_by_tenure.with_columns(
    (pl.col("AVGB01") + pl.col("AVGB03") + pl.col("AVGB06") + pl.col("AVGB12")).alias("AVGBAL")
)

avg_df = (
    avg_by_tenure.group_by(["CURCODE", "BRANCH"])
    .agg([
        pl.col("AVGB01").sum().alias("AVGB01"),
        pl.col("AVGB03").sum().alias("AVGB03"),
        pl.col("AVGB06").sum().alias("AVGB06"),
        pl.col("AVGB12").sum().alias("AVGB12"),
        pl.col("AVGBAL").sum().alias("AVGBAL"),
    ])
)

del fcy_filtered, fcy01, fcy01_dedup, avg_by_tenure
gc.collect()

# ============================================================================
# STEP 9: MERGE AVG + CURBAL  (SAS: MERGE FCY FCY01; BY CURCODE BRANCH;)
# Plain key-union merge — AVGB* only present if key was in avg_df, CURBAL
# only present if key was in curbal_df.
# ============================================================================
print("\nStep 9: Merging average-balance and current-balance summaries...")

merged1 = avg_df.join(curbal_df, on=["CURCODE", "BRANCH"], how="full", coalesce=True)
merged1 = merged1.with_columns(pl.lit(True).alias("_IN_M1"))

del avg_df, curbal_df
gc.collect()

# ============================================================================
# STEP 10: MERGE FDDUM + MERGED1  (SAS: MERGE FDDUM FCY; BY CURCODE BRANCH;)
# Last-dataset-wins BY KEY PRESENCE: for any key that exists in merged1, ALL
# of merged1's fields for that key win — even if a specific field is NULL —
# overriding FDDUM's zero defaults. Only keys absent from merged1 keep
# FDDUM's zero skeleton values.
# ============================================================================
print("\nStep 10: Merging skeleton (FDDUM) with actual data (last-dataset-wins)...")

_VALUE_COLS = ["AVGB01", "AVGB03", "AVGB06", "AVGB12", "AVGBAL", "CURBAL"]

final = fddum.join(merged1, on=["CURCODE", "BRANCH"], how="full", suffix="_m", coalesce=True)

for col in _VALUE_COLS:
    final = final.with_columns(
        pl.when(pl.col("_IN_M1").is_not_null())
        .then(pl.col(f"{col}_m"))
        .otherwise(pl.col(col))
        .alias(col)
    )

final = final.select(["CURCODE", "BRANCH"] + _VALUE_COLS)

# PROC SORT (implicit ordering carried through by the BY CURCODE BRANCH merges)
final = final.sort(["CURCODE", "BRANCH"])

del fddum, merged1
gc.collect()
print(f"  Final grid rows: {len(final):,}")

# ============================================================================
# STEP 11: RENDER FDFA OUTPUT  (semicolon-delimited text extract)
# DATA FCY; SET FCY; BY CURCODE;
#   IF FIRST.CURCODE THEN DO; SUB01=0; ... END;
#   SUB01+AVGB01; ... AVGB+AVGBAL; CURB+CURBAL;
#   FILE FDFA; IF _N_=1 THEN header; PUT detail; IF LAST.CURCODE THEN subtotal;
# ============================================================================
print("\nStep 11: Rendering FDFA extract...")


def _fmt_comma(value, width: int, decimals: int = 2) -> str:
    """SAS COMMAw.d equivalent, right-justified. Missing -> blank (project convention)."""
    if value is None:
        return " " * width
    try:
        v = float(value)
    except (TypeError, ValueError):
        return " " * width
    s = f"{v:,.{decimals}f}"
    return s.rjust(width)[-width:] if len(s) > width else s.rjust(width)


def _put_buf(buf: list, col: int, text: str) -> None:
    """SAS @col PUT text equivalent (col is 1-based)."""
    start = col - 1
    for i, ch in enumerate(str(text)):
        if 0 <= start + i < len(buf):
            buf[start + i] = ch


def _build_header_line() -> str:
    # PUT @001 'CURCODE;BRANCH;01-MONTH;03-MONTH;06-MONTH;12-MONTH;'
    #     @052 'TOTAL AVG BAL;TODATE BALANCE';
    return "CURCODE;BRANCH;01-MONTH;03-MONTH;06-MONTH;12-MONTH;TOTAL AVG BAL;TODATE BALANCE"


def _build_detail_line(row: dict) -> str:
    # PUT @002 CURCODE $3. ';' @007 BRANCH Z3. ';' @012 AVGB01 COMMA15.2 ';'
    #     @029 AVGB03 COMMA15.2 ';' @046 AVGB06 COMMA15.2 ';'
    #     @063 AVGB12 COMMA15.2 ';' @080 AVGBAL COMMA20.2 ';'
    #     @102 CURBAL COMMA15.2;
    buf = [" "] * 120
    _put_buf(buf, 2, f"{str(row['CURCODE'] or ''):<3s}")
    _put_buf(buf, 5, ";")
    _put_buf(buf, 7, f"{int(row['BRANCH'] or 0):03d}")
    _put_buf(buf, 10, ";")
    _put_buf(buf, 12, _fmt_comma(row["AVGB01"], 15, 2))
    _put_buf(buf, 27, ";")
    _put_buf(buf, 29, _fmt_comma(row["AVGB03"], 15, 2))
    _put_buf(buf, 44, ";")
    _put_buf(buf, 46, _fmt_comma(row["AVGB06"], 15, 2))
    _put_buf(buf, 61, ";")
    _put_buf(buf, 63, _fmt_comma(row["AVGB12"], 15, 2))
    _put_buf(buf, 78, ";")
    _put_buf(buf, 80, _fmt_comma(row["AVGBAL"], 20, 2))
    _put_buf(buf, 100, ";")
    _put_buf(buf, 102, _fmt_comma(row["CURBAL"], 15, 2))
    return "".join(buf).rstrip()


def _build_subtotal_line(sub01, sub03, sub06, sub12, avgb_acc, curb_acc) -> str:
    # PUT @002 'SUBT;;' SUB01 COMMA18.2 ';' SUB03 COMMA18.2 ';'
    #     SUB06 COMMA18.2 ';' SUB12 COMMA18.2 ';' AVGB COMMA18.2 ';'
    #     CURB COMMA18.2;
    line = (
        " " + "SUBT;;"
        + _fmt_comma(sub01, 18, 2) + ";"
        + _fmt_comma(sub03, 18, 2) + ";"
        + _fmt_comma(sub06, 18, 2) + ";"
        + _fmt_comma(sub12, 18, 2) + ";"
        + _fmt_comma(avgb_acc, 18, 2) + ";"
        + _fmt_comma(curb_acc, 18, 2)
    )
    return line.rstrip()


output_lines: list[str] = [_build_header_line()]

current_curcode = None
sub01 = sub03 = sub06 = sub12 = avgb_acc = curb_acc = 0.0

for row in final.iter_rows(named=True):
    curcode = row["CURCODE"]

    # FIRST.CURCODE -> reset accumulators
    if curcode != current_curcode:
        if current_curcode is not None:
            # LAST.CURCODE (of previous group) -> subtotal + blank line
            output_lines.append(
                _build_subtotal_line(sub01, sub03, sub06, sub12, avgb_acc, curb_acc)
            )
            output_lines.append("")   # PUT @002 '  ';  (blank line)
        current_curcode = curcode
        sub01 = sub03 = sub06 = sub12 = avgb_acc = curb_acc = 0.0

    # SUM statement semantics: missing values contribute 0 to the accumulator
    sub01 += row["AVGB01"] or 0.0
    sub03 += row["AVGB03"] or 0.0
    sub06 += row["AVGB06"] or 0.0
    sub12 += row["AVGB12"] or 0.0
    avgb_acc += row["AVGBAL"] or 0.0
    curb_acc += row["CURBAL"] or 0.0

    output_lines.append(_build_detail_line(row))

# LAST.CURCODE for the final group
if current_curcode is not None:
    output_lines.append(_build_subtotal_line(sub01, sub03, sub06, sub12, avgb_acc, curb_acc))
    output_lines.append("")

# ============================================================================
# WRITE OUTPUT
# ============================================================================
with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in output_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(output_lines):,}")

# print("\n--- FDFA extract content ---")
# for ln in output_lines:
#     print(ln)

# ============================================================================
# STEP 12: TERMINAL-ONLY SUMMARY  (PROC SUMMARY CLASS CURCODE; PROC PRINT;)
# No physical output dataset in the original SAS — SAS listing output only.
# ============================================================================
print("\nStep 12: Currency summary (FYY) — terminal listing only...")

fyy = (
    final.group_by("CURCODE")
    .agg([
        pl.col("AVGB01").sum().alias("AVGB01"),
        pl.col("AVGB03").sum().alias("AVGB03"),
        pl.col("AVGB06").sum().alias("AVGB06"),
        pl.col("AVGB12").sum().alias("AVGB12"),
        pl.col("AVGBAL").sum().alias("AVGBAL"),
        pl.col("CURBAL").sum().alias("CURBAL"),
    ])
    .sort("CURCODE")
)

print(f"\n{'CURCODE':<8}{'AVGB01':>18}{'AVGB03':>18}{'AVGB06':>18}"
      f"{'AVGB12':>18}{'AVGBAL':>22}{'CURBAL':>22}")
for row in fyy.iter_rows(named=True):
    print(
        f"{row['CURCODE']:<8}"
        f"{row['AVGB01']:>18,.2f}"
        f"{row['AVGB03']:>18,.2f}"
        f"{row['AVGB06']:>18,.2f}"
        f"{row['AVGB12']:>18,.2f}"
        f"{row['AVGBAL']:>22,.2f}"
        f"{row['CURBAL']:>22,.2f}"
    )

del final, fyy
gc.collect()

print("\nEIBRFCFD complete.")
