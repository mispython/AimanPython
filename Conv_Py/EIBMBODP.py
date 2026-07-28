#!/usr/bin/env python3
"""
Program : EIBMBODP.py
Purpose : BOD Papers Report — Original Maturity & Remaining Maturity
          (PROC PRINT listing of BODOM&REPTMON / BODRM&REPTMON, RECFM=FB LRECL=80)
"""

import os
import gc
import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from REPTDATE import get_reptdate_values
# from input_date import get_latest_file
# NOTE: BODOM/BODRM filenames are deterministic on REPTMON (SAS: MISB.BODOM&REPTMON,
# MISB.BODRM&REPTMON), not resolved by scanning a directory for the latest dated
# file, so input_date.get_latest_file() is not used — the path is built directly
# from REPTMON, matching the SAS macro-variable substitution.
# from output_date import build_output_file
# NOTE: The SASLIST output dataset (SAP.PBB.BOD.PAPERS, FTP'd out as BODPPR.TXT)
# carries a fixed filename with no date component, so output_date.build_output_file()
# is not used.

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR = Path("/stgsrcsys/host/uat/AII/MISB")

# INPUT_DIR = BASE_DIR / "input" / "prod" / "EIBMBODP"
CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBMBODP"
OUTPUT_DIR = BASE_DIR / "output" / "EIBMBODP"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Fixed output filename — no date component (SASLIST -> BODPPR.TXT via FTP)
OUTPUT_FILE = OUTPUT_DIR / "BODPPR.TXT"

# ============================================================================
# CHUNK SIZE FOR STREAMING LARGE .sas7bdat FILES
# ============================================================================
CHUNK_ROWS = 500_000
ROW_LIMIT  = int(os.environ.get("ROW_LIMIT", 0))   # 0 = no limit (test mode via env)

# ============================================================================
# REPORT CONFIGURATION  (OPTIONS LINESIZE=250 NOCENTER; DCB RECFM=FB LRECL=80)
# RECFM=FB (not FBA) -> physical output carries no ASA carriage-control byte.
# ============================================================================
LRECL      = 80
PAGE_SIZE  = 60   # SAS default lines-per-page (no PAGESIZE= specified)

# ============================================================================
# STEP 1: REPORT DATE  (no reptdate.parquet — derive from REPTDATE.py)
# DATA REPTDATE; SET MNI.REPTDATE;
#   CALL SYMPUT('REPTMON',PUT(MONTH(REPTDATE),Z2.));
#   CALL SYMPUT('REPTDAY',PUT(DAY(REPTDATE), Z2.));
#   CALL SYMPUT('RDATE',PUT(REPTDATE,DDMMYY8.));
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values()
reptdate = reptdate_values.reptdate
REPTMON  = reptdate_values.reptmon
REPTDAY  = reptdate_values.reptday
RDATE    = reptdate.strftime("%d/%m/%y")

print(f"  Report date : {RDATE}")
print(f"  REPTMON     : {REPTMON}")
print(f"  REPTDAY     : {REPTDAY}")
print(f"  Output file : {OUTPUT_FILE.name}")

# ============================================================================
# STEP 2: RESOLVE INPUT FILE NAMES  (fixed on REPTMON, no latest-file search)
# ============================================================================
print("\nStep 2: Resolving BODOM / BODRM file names...")

bodom_path = STG_DIR / f"bodom06.sas7bdat"
bodrm_path = STG_DIR / f"bodrm06.sas7bdat"

for _p in (bodom_path, bodrm_path):
    if not _p.exists():
        raise FileNotFoundError(f"Required input file not found: {_p}")

print(f"  BODOM : {bodom_path.name}")
print(f"  BODRM : {bodrm_path.name}")

# ============================================================================
# HELPER: CACHE STAMP  (skip re-conversion if .sas7bdat hasn't changed)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    """Return True when the Parquet cache is newer than the source SAS file."""
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )

# ============================================================================
# HELPER: STREAM .sas7bdat -> PARQUET  (memory-efficient chunked conversion)
# ============================================================================
def sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    """Convert a .sas7bdat to Parquet in streaming chunks."""
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer  = None
    schema  = None
    total   = 0
    rows_read = 0

    reader = pd.read_sas(
        sas_path,
        encoding="latin1",
        chunksize=CHUNK_ROWS,
    )
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
                        print(f"  [{tag}] WARNING: Cannot cast '{field.name}' "
                              f"from {col.type} to {field.type}: {e} — filling nulls")
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

# ============================================================================
# STEP 3: CACHE SAS FILES TO PARQUET
# ============================================================================
print("\nStep 3: Caching SAS files to Parquet (if needed)...")

BODOM_CACHE = CACHE_DIR / f"{bodom_path.stem}.parquet"
BODRM_CACHE = CACHE_DIR / f"{bodrm_path.stem}.parquet"

if not _cache_is_fresh(bodom_path, BODOM_CACHE):
    sas_to_parquet(bodom_path, BODOM_CACHE, "BODOM")
else:
    print("  [BODOM] Cache fresh — skipping conversion.")

if not _cache_is_fresh(bodrm_path, BODRM_CACHE):
    sas_to_parquet(bodrm_path, BODRM_CACHE, "BODRM")
else:
    print("  [BODRM] Cache fresh — skipping conversion.")

# ============================================================================
# STEP 4: READ BODOM / BODRM
# DATA BODOM; SET MISB.BODOM&REPTMON;   DATA BODRM; SET MISB.BODRM&REPTMON;
# VAR OM AMOUNT / VAR RM AMOUNT keep only the printed columns (plus implicit OBS)
# ============================================================================
print("\nStep 4: Reading BODOM / BODRM datasets...")

con = duckdb.connect(database=":memory:")

bodom = con.execute(f"""
    SELECT
        CAST(OM     AS VARCHAR) AS OM,
        CAST(AMOUNT AS DOUBLE)  AS AMOUNT
    FROM read_parquet('{BODOM_CACHE}')
""").pl()

bodrm = con.execute(f"""
    SELECT
        CAST(RM     AS VARCHAR) AS RM,
        CAST(AMOUNT AS DOUBLE)  AS AMOUNT
    FROM read_parquet('{BODRM_CACHE}')
""").pl()

con.close()
gc.collect()

print(f"  BODOM rows: {len(bodom):,}")
print(f"  BODRM rows: {len(bodrm):,}")

# ============================================================================
# STEP 5: GENERATE REPORT  (PROC PRINT listing, RECFM=FB -> no ASA control byte)
# ============================================================================
print("\nStep 5: Generating report...")

def _fmt_num(value, width: int, decimals: int = 2) -> str:
    """Format number right-justified to *width*, matching PROC PRINT numeric display."""
    if value is None:
        return " " * width
    try:
        v = float(value)
    except (TypeError, ValueError):
        return " " * width
    s = f"{v:,.{decimals}f}"
    return s.rjust(width)


def _print_section(df: pl.DataFrame, label_col: str, title: str) -> list[str]:
    """
    Build a PROC PRINT-style listing for one dataset.
    PROC PRINT DATA=<ds>; VAR <label_col> AMOUNT; SUM AMOUNT;
    Includes the default OBS column, VAR-ordered columns, and a SUM total row.
    """
    lines: list[str] = []

    n_rows = len(df)
    obs_width = max(len(str(n_rows)), 3)

    label_values  = df[label_col].to_list()
    amount_values = df["AMOUNT"].to_list()

    label_width = max(
        len(label_col),
        max((len(str(v)) for v in label_values if v is not None), default=0),
    )
    amount_width = max(
        len("AMOUNT"),
        max((len(f"{v:,.2f}") for v in amount_values if v is not None), default=0) + 1,
    )

    obs_hdr    = "Obs".rjust(obs_width)
    label_hdr  = label_col.ljust(label_width)
    amount_hdr = "AMOUNT".rjust(amount_width)

    # TITLE1 (NOCENTER -> left justified), truncated/padded to LRECL on write
    lines.append(title)
    lines.append("")
    lines.append(f"{obs_hdr}    {label_hdr}    {amount_hdr}")
    lines.append("")

    total = 0.0
    for idx, row in enumerate(df.iter_rows(named=True), start=1):
        obs_str    = str(idx).rjust(obs_width)
        label_str  = str(row[label_col] or "").ljust(label_width)
        amount_val = row["AMOUNT"] or 0.0
        amount_str = _fmt_num(amount_val, amount_width, 2)
        total += amount_val
        lines.append(f"{obs_str}    {label_str}    {amount_str}")

    # SUM AMOUNT; -> dash rule then grand total, aligned under the AMOUNT column
    indent     = " " * (obs_width + 4 + label_width + 4)
    dash_line  = indent + "-" * amount_width
    total_line = indent + _fmt_num(total, amount_width, 2)
    lines.append(dash_line)
    lines.append(total_line)

    return lines


output_lines: list[str] = []

output_lines.extend(
    _print_section(bodom, "OM", f"BOD PAPERS (ORIGINAL MATURITY)  {RDATE}")
)
output_lines.append("")
output_lines.extend(
    _print_section(bodrm, "RM", f"BOD PAPERS (REMAINING MATURITY)  {RDATE}")
)

# ============================================================================
# WRITE OUTPUT  (fixed record length 80, no ASA control byte — RECFM=FB)
# ============================================================================
with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in output_lines:
        fh.write(ln[:LRECL].ljust(LRECL) + "\n")

# ============================================================================
# FTP TRANSFER STEP  (out of scope for this program — infrastructure only)
# //RUNSFTP EXEC COZBATCH; put SAP.PBB.BOD.PAPERS -> BODPPR.TXT to DRR repository
# ============================================================================

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(output_lines):,}")

print("\nStep 6: Report contents:")
for ln in output_lines:
    print(ln)

del bodom, bodrm
gc.collect()

print("\nEIBMBODP complete.")
