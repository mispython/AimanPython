"""
Program : EIMISR01.py
Purpose : Monthly report on savings deposits' outstanding balance
          (Report ID: DMMISR01) and accounts breakdown by branch,
          for PBB (conventional) & PIBB (Islamic) combined.

Notes on conversion:
  - DYPOSXBR&REPTMON (.sas7bdat) is resolved via
    input_date.get_latest_file(INPUT_DIR, prefix="DYPOSXBR"), which picks
    the latest file whose embedded date matches the recognised filename
    patterns, rather than being built directly from REPTMON.
  - DYPOSXBR is cached to Parquet once (chunked stream + freshness check,
    same pattern as EIBDLN1M.py) and read from the cache via DuckDB
    thereafter.
  - LIBNAME MIS "SAP.PBB.MIS.D&REPTYEAR" is not applicable: there is no
    separate MIS_DIR input in this environment; DYPOSXBR is read directly
    from the input/cache directories below.
  - //ISR01 DD ... DCB=(...,RECFM=FB,...) -> RECFM=FB (not FBA), meaning
    the original output carries NO ASA carriage-control characters.
    This report is therefore written as plain semicolon-delimited,
    fixed-column text, with no leading carriage-control byte.
  - The JCL steps (ICEGENER SFTP staging, RUNSFTP to DRR) are batch/file-
    transfer infrastructure, not data-processing logic, and are kept as
    comments only; the SFTP target filename uses an undefined macro
    variable (&ACC) that never gets a value anywhere in the visible
    source, so it cannot be faithfully reproduced.
"""

from __future__ import annotations

import gc
from pathlib import Path
from datetime import date, timedelta

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values
from input_date import get_latest_file

# ============================================================
# PATH CONFIGURATION
# ============================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

INPUT_DIR = BASE_DIR / "input" / "prod" / "deposit"
CACHE_DIR = BASE_DIR / "input" / "cache" / "EIMISR01"
OUTPUT_DIR = BASE_DIR / "output" / "EIMISR01"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# BRHFILE: static-name flat file (no date component) -> direct path reference
BRHFILE = Path("/sasdata/rawdata/lookup") / "LKP_BRANCH"

# //ISR01 DD DSN=SAP.PBB.EIMISR01.TEXT ... DCB=(LRECL=1000,RECFM=FB,...)
ISR01_OUTPUT = OUTPUT_DIR / "SA_BAL&ACC.txt"
LRECL = 1000

# Chunked .sas7bdat -> Parquet streaming
CHUNK_ROWS = 500_000

# ============================================================
# REPORT DATE DERIVATION (REPTDATE.py)
# ============================================================
_reptdate_values = get_reptdate_values(year_format="%Y")

REPTYEAR    = _reptdate_values.reptyear                         # YEAR4.  e.g. "2026"
REPTYR      = _reptdate_values.reptdate.strftime("%y")          # YEAR2.  e.g. "26"
REPTMON     = _reptdate_values.reptmon                          # Z2.     zero-padded month
REPTDAY     = _reptdate_values.reptday                          # Z2.     zero-padded day
RDATE: date = _reptdate_values.reptdate                         # actual report date (used for filtering)
REPTDT      = _reptdate_values.reptdate.strftime("%d/%m/%Y")    # DDMMYY10. equivalent, e.g. "09/07/2026"

# ============================================================
# STEP: RESOLVE DYPOSXBR INPUT FILE  (input_date.get_latest_file)
# DYPOSXBR&REPTMON.sas7bdat -> latest file matching prefix
# ============================================================
DYPOSXBR_SAS = get_latest_file(INPUT_DIR, prefix="DYPOSXBR")    # e.g. DYPOSXBR06.sas7bdat
DYPOSXBR_CACHE = CACHE_DIR / f"{DYPOSXBR_SAS.stem}.parquet"


# ============================================================
# STEP: DERIVE RDATE FROM THE ACTUAL DYPOSXBR CACHE
# The monthly REPTDATE stamped in this table (e.g. 2026-06-01 for
# DYPOSXBR06) does not follow the daily "yesterday" convention in
# REPTDATE.py — it reflects whatever date MNITB.REPTDATE held when
# production populated this snapshot. Rather than assume a formula,
# pull the actual date present in the resolved file.
# ============================================================
def get_actual_reptdate(dyposxbr_cache: Path) -> date:
    con = duckdb.connect(database=":memory:")
    try:
        result = con.execute(f"""
            SELECT CAST(MAX(REPTDATE) AS DOUBLE) AS max_reptdate
            FROM read_parquet('{dyposxbr_cache.as_posix()}')
        """).fetchone()
    finally:
        con.close()

    sas_epoch = date(1960, 1, 1)
    return sas_epoch + timedelta(days=int(result[0]))


# ============================================================
# HELPER: CACHE STAMP  (skip re-conversion if .sas7bdat hasn't changed)
# ============================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    """Return True when the Parquet cache is newer than the source SAS file."""
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


# ============================================================
# HELPER: STREAM .sas7bdat -> PARQUET  (memory-efficient chunked conversion)
# ============================================================
def sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    """Convert a .sas7bdat to Parquet in streaming chunks."""
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0

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
                        print(
                            f"  [{tag}] WARNING: Cannot cast '{field.name}' "
                            f"from {col.type} to {field.type}: {e} — filling nulls"
                        )
                        col = pa.nulls(len(col), type=field.type)
                cast_arrays.append(col)
            table = pa.Table.from_arrays(cast_arrays, schema=schema)

        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer:
        writer.close()
    print(f"  [{tag}] Done — {total:,} rows cached.")


def ensure_dyposxbr_cache() -> None:
    if not _cache_is_fresh(DYPOSXBR_SAS, DYPOSXBR_CACHE):
        sas_to_parquet(DYPOSXBR_SAS, DYPOSXBR_CACHE, "DYPOSXBR")
    else:
        print(f"  [DYPOSXBR] Cache fresh — skipping conversion.")


# ============================================================
# FIXED-WIDTH FIELD / DELIMITER LINE BUILDER
# ============================================================
def build_line(placements: list[tuple[int, str]], length: int = LRECL) -> str:
    """Lay text fragments onto a space-filled line at 1-based SAS columns."""
    line = [" "] * length
    for col, text in placements:
        start = col - 1
        end = start + len(text)
        line[start:end] = list(text)
    return "".join(line)


def fmt_comma(value, width: int, decimals: int) -> str:
    """COMMAw.d equivalent: thousands-separated, right-justified."""
    if value is None:
        value = 0
    return f"{value:,.{decimals}f}".rjust(width)


def fmt_num(value: int, width: int) -> str:
    """w. numeric equivalent: right-justified, no leading zeros."""
    return str(value).rjust(width)


def fmt_zero(value: int, width: int) -> str:
    """Zw. equivalent: zero-padded, right-justified."""
    return str(value).zfill(width)


# ============================================================
# DATA BRHDATA
#   INFILE BRHFILE;
#   INPUT @002 BRANCH  3.
#         @006 BRABBR $3.
#         @050 STATUS $1.
#         ;
#   IF BRANCH IN (101,279) THEN STATUS='C';
# ============================================================
def read_branch_lookup(path: Path) -> pl.DataFrame:
    records = []
    with path.open("r", encoding="latin1") as fh:
        for raw_line in fh:
            line = raw_line.rstrip("\n")
            branch = int(line[1:4])
            brabbr = line[5:8].strip()
            status = line[49:50].strip()
            if branch in (101, 279):
                status = "C"
            records.append({"BRANCH": branch, "BRABBR": brabbr, "STATUS": status})

    # PROC SORT DATA=BRHDATA; BY BRANCH BRABBR; -> removed. The merge below
    # is implemented as a hash join on BRANCH, which does not require either
    # side to be pre-sorted.
    return pl.DataFrame(records)


# ============================================================
# PROC SORT DATA=MIS.DYPOSXBR&REPTMON OUT=SABAL;
#    BY BRANCH;
#    WHERE REPTDATE = &RDATE;
# NOTE: SAS stores dates as "days since 1960-01-01" (numeric, not a DATE type).
# pd.read_sas() -> Parquet keeps REPTDATE as DOUBLE, so we must compare
# against the equivalent SAS numeric day value instead of a SQL DATE literal.
# ============================================================
_SAS_EPOCH = date(1960, 1, 1)


def _to_sas_date_value(d: date) -> int:
    """Convert a Python date to SAS's numeric date (days since 1960-01-01)."""
    return (d - _SAS_EPOCH).days


def read_sabal(dyposxbr_cache: Path, reptdate_value: date) -> pl.DataFrame:
    sas_date_num = _to_sas_date_value(reptdate_value)
    con = duckdb.connect(database=":memory:")
    try:
        query = f"""
            SELECT
                CAST(BRANCH AS BIGINT) AS BRANCH,
                TOTSAVG, TOTSAVGI, SACNT, ISACNT
            FROM read_parquet('{dyposxbr_cache.as_posix()}')
            WHERE CAST(REPTDATE AS DOUBLE) = {sas_date_num}
            ORDER BY BRANCH
        """
        return con.execute(query).pl()
    finally:
        con.close()


# ============================================================
# DATA ALLSA SABAL EXCEP;
#    MERGE SABAL(IN=A) BRHDATA;
#    BY BRANCH;
#    IF A;
#    IF TOTSAVG  = . THEN TOTSAVG  = 0;
#    IF TOTSAVGI = . THEN TOTSAVGI = 0;
#    TOTSA   = SUM(TOTSAVG,TOTSAVGI);
#    TOTSAML = INT(ROUND(TOTSA/1000));
#    TOTCNT  = SUM(SACNT,ISACNT);
#    OUTPUT ALLSA;
#    IF STATUS = 'O' THEN OUTPUT SABAL;
#    ELSE                 OUTPUT EXCEP;
# ============================================================
def build_allsa_sabal_excep(
    sabal: pl.DataFrame, brhdata: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    # MERGE ... IF A  ->  keep every SABAL row, left-join branch attributes
    merged = sabal.join(brhdata, on="BRANCH", how="left")

    merged = merged.with_columns(
        [
            pl.col("TOTSAVG").fill_null(0.0),
            pl.col("TOTSAVGI").fill_null(0.0),
            pl.col("SACNT").fill_null(0),
            pl.col("ISACNT").fill_null(0),
            pl.col("BRABBR").fill_null(""),
            pl.col("STATUS").fill_null(""),
        ]
    )

    merged = merged.with_columns(
        [
            (pl.col("TOTSAVG") + pl.col("TOTSAVGI")).alias("TOTSA"),
            (pl.col("SACNT") + pl.col("ISACNT")).alias("TOTCNT"),
        ]
    )

    # TOTSAML = INT(ROUND(TOTSA/1000))  -- SAS ROUND is round-half-away-from-zero
    merged = merged.with_columns(
        pl.when(pl.col("TOTSA") >= 0)
        .then(((pl.col("TOTSA") / 1000) + 0.5).floor())
        .otherwise(-(((-pl.col("TOTSA") / 1000) + 0.5).floor()))
        .cast(pl.Int64)
        .alias("TOTSAML")
    )

    # Row order in the report follows BRANCH ascending (the original sorted
    # merge order) -> a single sort here is required for correct report
    # layout, unlike the input-side sorts which were purely mechanical.
    merged = merged.sort("BRANCH")

    allsa = merged
    sabal_open = merged.filter(pl.col("STATUS") == "O")
    excep = merged.filter(pl.col("STATUS") != "O")

    return allsa, sabal_open, excep


def build_detail_line(seq: int, row: dict) -> str:
    return build_line(
        [
            (1, fmt_num(seq, 3)),
            (4, ";"),
            (5, str(row.get("BRABBR") or "").ljust(3)[:3]),
            (17, ";"),
            (18, fmt_zero(row["BRANCH"], 3)),
            (29, ";"),
            (30, fmt_comma(row["TOTSAVG"], 20, 2)),
            (50, ";"),
            (51, fmt_comma(row["TOTSAVGI"], 20, 2)),
            (71, ";"),
            (72, fmt_comma(row["TOTSA"], 20, 2)),
            (92, ";"),
            (93, fmt_comma(row["TOTSAML"], 20, 0)),
            (113, ";"),
            (114, fmt_comma(row["SACNT"], 20, 0)),
            (134, ";"),
            (135, fmt_comma(row["ISACNT"], 20, 0)),
            (155, ";"),
            (156, fmt_comma(row["TOTCNT"], 20, 0)),
            (176, ";"),
        ]
    )


def _sum_columns(df: pl.DataFrame) -> dict:
    cols = ["TOTSAVG", "TOTSAVGI", "TOTSA", "TOTSAML", "SACNT", "ISACNT", "TOTCNT"]
    if df.height == 0:
        return {c: 0 for c in cols}
    return df.select([pl.col(c).sum() for c in cols]).row(0, named=True)


def build_total_line(label: str, df: pl.DataFrame) -> str:
    sums = _sum_columns(df)
    return build_line(
        [
            (4, ";"),
            (5, label),
            (17, ";"),
            (29, ";"),
            (30, fmt_comma(sums["TOTSAVG"], 20, 2)),
            (50, ";"),
            (51, fmt_comma(sums["TOTSAVGI"], 20, 2)),
            (71, ";"),
            (72, fmt_comma(sums["TOTSA"], 20, 2)),
            (92, ";"),
            (93, fmt_comma(sums["TOTSAML"], 20, 0)),
            (113, ";"),
            (114, fmt_comma(sums["SACNT"], 20, 0)),
            (134, ";"),
            (135, fmt_comma(sums["ISACNT"], 20, 0)),
            (155, ";"),
            (156, fmt_comma(sums["TOTCNT"], 20, 0)),
            (176, ";"),
        ]
    )


# ============================================================
# REPORT WRITER
#   (RECFM=FB on //ISR01 -> no ASA carriage control characters)
# ============================================================
def write_report(
    output_path: Path,
    reptdt: str,
    sabal_open: pl.DataFrame,
    excep: pl.DataFrame,
    allsa: pl.DataFrame,
) -> None:
    lines: list[str] = []

    # ---- Title block (plain text lines, column 1) ----
    lines.append(build_line([(1, "REPORT ID : EIMISR01")]))
    lines.append(
        build_line(
            [
                (
                    1,
                    "MONTHLY SAVINGS DEPOSITS OUTSTANDING BALANCE & "
                    f"ACCOUNTS BY BRANCH @ {reptdt}",
                )
            ]
        )
    )
    lines.append(
        build_line(
            [(1, "RETAIL FINANCIAL SERVICES - SALES ADMINISTRATION & SUPPORT")]
        )
    )
    lines.append(build_line([(1, " ")]))

    # ---- Table header (4 rows) ----
    lines.append(
        build_line(
            [
                (4, ";"), (17, ";"), (29, ";"), (50, ";"),
                (51, "OUTSTANDING BALANCE"), (71, ";"), (92, ";"),
                (113, ";"), (134, ";"),
                (135, "NO. OF OUSTANDING ACCOUNTS"), (175, ";;"),
            ]
        )
    )
    lines.append(
        build_line(
            [
                (4, ";"), (17, ";"), (29, ";"), (50, ";"),
                (51, "(RM)"), (71, ";"), (92, ";"), (113, ";"), (134, ";"),
                (135, "(ALL BALANCES)"), (155, ";"), (176, ";"),
            ]
        )
    )
    lines.append(
        build_line(
            [
                (2, "NO"), (4, ";"),
                (5, "BRANCH ABBV."), (17, ";"),
                (18, "BRANCH CODE"), (29, ";"),
                (30, "PBB"), (50, ";"),
                (51, "PIBB"), (71, ";"),
                (72, "TOTAL"), (92, ";"),
                (93, "TOTAL (RM '000)"), (113, ";"),
                (114, "PBB"), (134, ";"),
                (135, "PIBB"), (155, ";"),
                (156, "TOTAL"), (176, ";"),
            ]
        )
    )
    lines.append(
        build_line(
            [
                (4, ";"), (17, ";"),
                (30, "(A)"), (50, ";"),
                (51, "(B)"), (71, ";"),
                (72, "(C)=(A)+(B)"), (92, ";"), (113, ";"),
                (114, "(D)"), (134, ";"),
                (135, "(E)"), (155, ";"),
                (156, "(F)=(D)+(E)"), (176, ";"),
            ]
        )
    )

    # ---- SABAL detail rows (STATUS = 'O') + TOTAL (A) ----
    for idx, row in enumerate(sabal_open.iter_rows(named=True), start=1):
        lines.append(build_detail_line(idx, row))
    lines.append(build_total_line("TOTAL (A)", sabal_open))

    # ---- Exceptional report (STATUS <> 'O', includes unmatched branches) ----
    if excep.height == 0:
        # SAS: IF TRN=0 branch — header + label-only "TOTAL (B)" line, no
        # detail rows and no numeric fields at all.
        lines.append(build_line([(4, ";"), (5, "EXCEPTIONAL REPORT")]))
        lines.append(build_line([(4, ";"), (5, "(CLOSED/MERGED BRANCHES)")]))
        lines.append(build_line([(4, ";"), (5, "TOTAL (B)")]))
    else:
        lines.append(build_line([(4, ";"), (5, "EXCEPTIONAL REPORT")]))
        lines.append(build_line([(4, ";"), (5, "(CLOSED/MERGED BRANCHES)")]))
        for idx, row in enumerate(excep.iter_rows(named=True), start=1):
            lines.append(build_detail_line(idx, row))
        lines.append(build_total_line("TOTAL (B)", excep))

    # ---- Grand total (A)+(B): PROC SUMMARY DATA=ALLSA NWAY; ----
    grand = _sum_columns(allsa)
    lines.append(
        build_line(
            [
                (4, ";"),
                (5, "GRAND TOTAL (A)+(B)"),
                (28, ";;"),
                (30, fmt_comma(grand["TOTSAVG"], 20, 2)),
                (50, ";"),
                (51, fmt_comma(grand["TOTSAVGI"], 20, 2)),
                (71, ";"),
                (72, fmt_comma(grand["TOTSA"], 20, 2)),
                (92, ";"),
                (93, fmt_comma(grand["TOTSAML"], 20, 0)),
                (113, ";"),
                (114, fmt_comma(grand["SACNT"], 20, 0)),
                (134, ";"),
                (135, fmt_comma(grand["ISACNT"], 20, 0)),
                (155, ";"),
                (156, fmt_comma(grand["TOTCNT"], 20, 0)),
                (176, ";"),
            ]
        )
    )

    # //DELETE EXEC PGM=IEFBR14 (delete-before-create) -> handled by "w" mode
    with output_path.open("w", encoding="latin1", newline="\n") as fh:
        fh.write("\n".join(lines) + "\n")

    print(f"[EIMISR01] Report written to : {output_path}")
    print(f"[EIMISR01] Total lines written: {len(lines)}")
    print(f"[EIMISR01] SABAL (open) rows  : {sabal_open.height}")
    print(f"[EIMISR01] EXCEP rows         : {excep.height}")
    print(f"[EIMISR01] Grand total balance: {fmt_comma(grand['TOTSA'], 20, 2).strip()}")


# ============================================================
# NOTE - FTP / JCL infrastructure steps (out of scope for conversion)
# ============================================================
# //STEP01 EXEC PGM=ICEGENER  (stage SFTP command file to DRR)
# //RUNSFTP EXEC COZBATCH     (SFTP host file to Data Report Repository)
# DATA _NULL_;
#    FILE SFTP01;
#    PUT @1 "put //SAP.PBB.EIMISR01.TEXT  SA_BAL&ACC@&REPTMON&REPTYR..TXT";
# RUN;
# NOTE: &ACC is referenced but never assigned anywhere in the visible SAS
# source (no CALL SYMPUT / %LET for ACC), so the exact SFTP target filename
# cannot be reproduced faithfully. This step is pure file-transfer batch
# control, not report data logic, and is left commented as a placeholder.


def main() -> None:
    ensure_dyposxbr_cache()

    actual_rdate = get_actual_reptdate(DYPOSXBR_CACHE)
    reptdt_display = actual_rdate.strftime("%d/%m/%Y")

    print(f"Report date (RDATE) : {actual_rdate}")
    print(f"REPTMON / REPTYEAR  : {REPTMON} / {REPTYEAR}")
    print(f"DYPOSXBR input file : {DYPOSXBR_SAS.name}")

    brhdata = read_branch_lookup(BRHFILE)
    sabal = read_sabal(DYPOSXBR_CACHE, actual_rdate)

    allsa, sabal_open, excep = build_allsa_sabal_excep(sabal, brhdata)

    write_report(ISR01_OUTPUT, reptdt_display, sabal_open, excep, allsa)

    del allsa, sabal_open, excep, sabal, brhdata
    gc.collect()


if __name__ == "__main__":
    main()
