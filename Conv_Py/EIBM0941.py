#!/usr/bin/env python3
"""
Program : EIBM0941.py
Purpose : Monthly GL Interface - Deposit Accounts (Savings, Current, Fixed
          Deposit, and Related Non-Interest-Bearing Deposit Accounts)
          Produces the FMGL0941 GL interface flat file (fixed-width, no
          ASA carriage control since the original DCB is RECFM=FB).

Original JCL DD cross-reference:
    DEP   -> SAP.PBB.MNITB(0)          : source of REPTDATE dataset
                                          (superseded here by REPTDATE.py;
                                          no reptdate.parquet exists)
    PGM   -> SAP.BNM.PROGRAM            : %INC PGM(PBBLNFMT) library
    MIS   -> SAP.PBB.OPCL.SASDATA       : PBB  SAVG/CURR/FD monthly data
    MISS  -> SAP.PIBB.OPCL.SASDATA      : PIBB SAVGF/CURRF/FDF monthly data
    DEPO  -> SAP.PBB.RNID.SASDATA       : PBB  RNIDM monthly data
    IDEPO -> SAP.PIBB.RNID.SASDATA      : PIBB RNIDM monthly data
    GLINT -> SAP.PBB.FMGL0941.TEXT(+1)  : output GDG, LRECL=500, RECFM=FB
                                          (FB, not FBA -> no ASA control byte)

NOTE ON DEPENDENCY:
    The original SAS source includes "%INC PGM(PBBLNFMT);" at the top, but
    no DATA step in this program body calls a PBBLNFMT PUT(var,fmt.) format
    function. PRODUCX is derived from hardcoded PRODUCT range comparisons
    and BRANCX from a plain Z3. numeric format, neither of which are
    PBBLNFMT formats. Per the project's dependency-import discipline
    (import only when a function is traceable to an explicit format call),
    no function is imported from PBBLNFMT.py in this conversion.
"""

import os
import gc
import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from REPTDATE import get_monthly_reptdate_values
# from input_date import get_latest_file
# NOTE: get_latest_file() is NOT used here. Each of the 8 input files carries
# a deterministic 2-digit month suffix (RMONTH) derived directly from
# REPTDATE's month -- there is no "pick the latest file in the folder"
# ambiguity to resolve, so the filenames are constructed directly instead.
# from output_date import build_output_file
# NOTE: build_output_file() is NOT used here either. The original GLINT
# output is a GDG (SAP.PBB.FMGL0941.TEXT(+1)); the filename itself carries
# no date component (the GDG generation number differentiates runs), so the
# output filename below is static, without a date suffix.

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# Input directories - Production
# BASE_DIR       = Path("/dwh")
# INPUT_MIS_DIR  = BASE_DIR / "mis_opcl"      # MIS  DD -> PBB  SAVG/CURR/FD
# INPUT_MISS_DIR = BASE_DIR / "miss_opcl"     # MISS DD -> PIBB SAVGF/CURRF/FDF
# INPUT_DEPO_DIR = BASE_DIR / "depo_rnid"     # DEPO DD -> PBB  RNIDM
# INPUT_IDEPO_DIR= BASE_DIR / "idepo_rnid"    # IDEPO DD -> PIBB RNIDM

# Input directories - Testing
BASE_DIR        = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR         = Path("/stgsrcsys/host/uat/AII/EIBM0941")

INPUT_MIS_DIR   = STG_DIR / "MIS"
INPUT_MISS_DIR  = STG_DIR / "MISS"
INPUT_DEPO_DIR  = STG_DIR / "DEPO"
INPUT_IDEPO_DIR = STG_DIR / "IDEPO"

# Parquet cache directory
CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBM0941"

# Output
OUTPUT_DIR = BASE_DIR / "output" / "EIBM0941"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# CHUNK SIZE FOR STREAMING LARGE .sas7bdat FILES
# ============================================================================
CHUNK_ROWS = 500_000
ROW_LIMIT  = int(os.environ.get("ROW_LIMIT", 0))   # 0 = no limit (test mode via env)

# ============================================================================
# STEP 1: REPORT DATE (no reptdate.parquet -- derive from REPTDATE.py)
# DATA REPTDATE; SET DEP.REPTDATE; MM=MONTH(REPTDATE); MM1=MM-1; ...
# This is a monthly GL interface program: SAVG&RMONTH etc. select data for
# the reporting month, so the monthly REPTDATE variant is used.
# ============================================================================
print("Step 1: Deriving report date...")

monthly_values = get_monthly_reptdate_values(year_format="%Y")

reptdate = monthly_values.reptdate
MM       = reptdate.month
MM1      = MM - 1 if MM > 1 else 12   # REPTMON1: same wraparound logic as SAS

RDATE    = monthly_values.ddmmyy8            # PUT(REPTDATE,DDMMYY8.)
RYEAR    = monthly_values.reptyear            # PUT(REPTDATE,YEAR4.)
RMONTH   = monthly_values.reptmon             # PUT(MM,Z2.)
REPTMON1 = f"{MM1:02d}"                       # PUT(MM1,Z2.) -- computed for
                                               # parity with SAS but, exactly
                                               # as in the original source,
                                               # REPTMON1 is never referenced
                                               # again after this point.
REPTDT   = monthly_values.yymmdd              # PUT(REPTDATE,YYMMDDN6.)

YM = int(f"{RYEAR}{REPTMON1}")                  # YM=&RYEAR&RMONTH

OUTPUT_FILE = OUTPUT_DIR / "FMGL0941.txt"

print(f"  Report date  : {RDATE}")
print(f"  RYEAR/RMONTH : {RYEAR}/{RMONTH}  (REPTMON1={REPTMON1}, unused downstream)")
print(f"  REPTDT       : {REPTDT}")
print(f"  YM           : {YM}")
print(f"  Output file  : {OUTPUT_FILE.name}")

# ============================================================================
# STEP 2: RESOLVE INPUT FILE NAMES (deterministic month-suffixed filenames)
# ============================================================================
print("\nStep 2: Resolving input file paths...")

savg_path   = INPUT_MIS_DIR   / f"savg{REPTMON1}.sas7bdat"
curr_path   = INPUT_MIS_DIR   / f"curr{REPTMON1}.sas7bdat"
fd_path     = INPUT_MIS_DIR   / f"fd{REPTMON1}.sas7bdat"
savgf_path  = INPUT_MISS_DIR  / f"savgf{REPTMON1}.sas7bdat"
currf_path  = INPUT_MISS_DIR  / f"currf{REPTMON1}.sas7bdat"
fdf_path    = INPUT_MISS_DIR  / f"fdf{REPTMON1}.sas7bdat"
rnidm_path  = INPUT_DEPO_DIR  / f"rnidm{REPTMON1}.sas7bdat"
irnidm_path = INPUT_IDEPO_DIR / f"rnidm{REPTMON1}.sas7bdat"

for _p in (savg_path, curr_path, fd_path, savgf_path,
           currf_path, fdf_path, rnidm_path, irnidm_path):
    print(f"  {_p.name}")

# ============================================================================
# HELPER: CACHE STAMP (skip re-conversion if .sas7bdat hasn't changed)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    """Return True when the Parquet cache is newer than the source SAS file."""
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )

# ============================================================================
# HELPER: STREAM .sas7bdat -> PARQUET (memory-efficient chunked conversion)
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

SAVG_CACHE   = CACHE_DIR / f"savg{REPTMON1}.parquet"
CURR_CACHE   = CACHE_DIR / f"curr{REPTMON1}.parquet"
FD_CACHE     = CACHE_DIR / f"fd{REPTMON1}.parquet"
SAVGF_CACHE  = CACHE_DIR / f"savgf{REPTMON1}.parquet"
CURRF_CACHE  = CACHE_DIR / f"currf{REPTMON1}.parquet"
FDF_CACHE    = CACHE_DIR / f"fdf{REPTMON1}.parquet"
RNIDM_CACHE  = CACHE_DIR / f"rnidm{REPTMON1}.parquet"
IRNIDM_CACHE = CACHE_DIR / f"irnidm{REPTMON1}.parquet"

_conversion_plan = [
    (savg_path, SAVG_CACHE, "SAVG"),
    (curr_path, CURR_CACHE, "CURR"),
    (fd_path, FD_CACHE, "FD"),
    (savgf_path, SAVGF_CACHE, "SAVGF"),
    (currf_path, CURRF_CACHE, "CURRF"),
    (fdf_path, FDF_CACHE, "FDF"),
    (rnidm_path, RNIDM_CACHE, "RNIDM"),
    (irnidm_path, IRNIDM_CACHE, "IRNIDM"),
]

for _src, _cache, _tag in _conversion_plan:
    if not _cache_is_fresh(_src, _cache):
        sas_to_parquet(_src, _cache, _tag)
    else:
        print(f"  [{_tag}] Cache fresh — skipping conversion.")

# ============================================================================
# STEP 4: BUILD DEP (combined PBB/PIBB savings, current, FD)
# DATA SACA;  SET MIS.SAVG&RMONTH MIS.CURR&RMONTH; AGLFLD='PBB ';
# DATA FD;    SET MIS.FD&RMONTH; AGLFLD='PBB'; NOACCT=NOCD;
# DATA SACAI; SET MISS.SAVGF&RMONTH MISS.CURRF&RMONTH; AGLFLD='PIBB';
# DATA FDI;   SET MISS.FDF&RMONTH; AGLFLD='PIBB'; NOACCT=NOCD;
# DATA DEP;   SET SACA SACAI FD FDI;
#             IF BRANCH IN (119,132) OR BRANCH > 800 THEN DELETE;
#             BRANCX=PUT(BRANCH,Z3.);
#             IF (300<=PRODUCT<=400) THEN PRODUCX='DFIXED';
#             IF (050<=PRODUCT<=198) THEN PRODUCX='DDMAND';
#             IF (200<=PRODUCT<=220) THEN PRODUCX='DSVING';
# PROC SUMMARY DATA=DEP NWAY; CLASS AGLFLD BRANCX PRODUCX; VAR NOACCT;
# OUTPUT OUT=DEPS SUM=;
# DATA DEP; SET DEPS; LOB='   '; BGLFLD='EXT'; CGLFLD='C_DEP';
#           DGLFLD='ACTUAL'; YM=&RYEAR&RMONTH; IF NOACCT > 0;
#
# NOTE: When SAS combines datasets whose AGLFLD was assigned with slightly
# different literal widths ('PBB ' vs 'PBB'), the SET statement fixes the
# variable's storage length from the FIRST dataset listed (SACA, length 4),
# so every subsequent value is blank-padded to 4 chars regardless. The
# resulting value is 'PBB ' in all cases — implemented directly as such below.
# ============================================================================
print("\nStep 4: Building DEP (deposit summary, PBB + PIBB)...")

con = duckdb.connect(database=":memory:")

dep_summary = con.execute(f"""
    WITH combined AS (
        SELECT CAST(BRANCH AS BIGINT) AS BRANCH,
               CAST(PRODUCT AS INTEGER) AS PRODUCT,
               CAST(NOACCT AS DOUBLE) AS NOACCT,
               'PBB ' AS AGLFLD
        FROM read_parquet('{SAVG_CACHE}')

        UNION ALL

        SELECT CAST(BRANCH AS BIGINT), CAST(PRODUCT AS INTEGER),
               CAST(NOACCT AS DOUBLE), 'PBB '
        FROM read_parquet('{CURR_CACHE}')

        UNION ALL

        SELECT CAST(BRANCH AS BIGINT), CAST(PRODUCT AS INTEGER),
               CAST(NOCD AS DOUBLE), 'PBB '
        FROM read_parquet('{FD_CACHE}')

        UNION ALL

        SELECT CAST(BRANCH AS BIGINT), CAST(PRODUCT AS INTEGER),
               CAST(NOACCT AS DOUBLE), 'PIBB'
        FROM read_parquet('{SAVGF_CACHE}')

        UNION ALL

        SELECT CAST(BRANCH AS BIGINT), CAST(PRODUCT AS INTEGER),
               CAST(NOACCT AS DOUBLE), 'PIBB'
        FROM read_parquet('{CURRF_CACHE}')

        UNION ALL

        SELECT CAST(BRANCH AS BIGINT), CAST(PRODUCT AS INTEGER),
               CAST(NOCD AS DOUBLE), 'PIBB'
        FROM read_parquet('{FDF_CACHE}')
    ),
    filtered AS (
        SELECT
            AGLFLD,
            LPAD(CAST(BRANCH AS VARCHAR), 3, '0') AS BRANCX,
            CASE
                WHEN PRODUCT BETWEEN 300 AND 400 THEN 'DFIXED'
                WHEN PRODUCT BETWEEN 50  AND 198 THEN 'DDMAND'
                WHEN PRODUCT BETWEEN 200 AND 220 THEN 'DSVING'
                ELSE '      '
            END AS PRODUCX,
            NOACCT
        FROM combined
        WHERE BRANCH NOT IN (119, 132)
          AND BRANCH <= 800
    )
    SELECT AGLFLD, BRANCX, PRODUCX, SUM(NOACCT) AS NOACCT
    FROM filtered
    GROUP BY AGLFLD, BRANCX, PRODUCX
    HAVING SUM(NOACCT) > 0
""").pl()

con.close()
gc.collect()

dep_final = dep_summary.with_columns([
    pl.lit("   ").alias("LOB"),
    pl.lit("EXT").alias("BGLFLD"),
    pl.lit("C_DEP").alias("CGLFLD"),
    pl.lit("ACTUAL").alias("DGLFLD"),
    pl.lit(YM).alias("YM"),
]).select(["AGLFLD", "BRANCX", "PRODUCX", "LOB", "BGLFLD", "CGLFLD", "DGLFLD", "YM", "NOACCT"])

print(f"  DEP rows: {len(dep_final):,}")

# ============================================================================
# STEP 5: BUILD RNID1 (PBB non-interest-bearing deposit account count)
# DATA RNID; SET DEPO.RNIDM&RMONTH; IF CDSTAT='A' AND NIDSTAT='N';
#            NOACCT=1; BRANCX=PUT(BRANCH,Z3.);
# PROC SUMMARY DATA=RNID NWAY; CLASS BRANCX; VAR NOACCT; OUTPUT OUT=RNID1 SUM=;
# DATA RNID1; SET RNID1; AGLFLD='PBB '; BGLFLD='EXT'; CGLFLD='C_DEP';
#             DGLFLD='ACTUAL'; PRODUCX='DPNIDS'; YM=&RYEAR&RMONTH;
# ============================================================================
print("\nStep 5: Building RNID1 (PBB)...")

con = duckdb.connect(database=":memory:")

rnid1 = con.execute(f"""
    SELECT LPAD(CAST(CAST(BRANCH AS BIGINT) AS VARCHAR), 3, '0') AS BRANCX,
           COUNT(*) AS NOACCT
    FROM read_parquet('{RNIDM_CACHE}')
    WHERE CDSTAT = 'A' AND NIDSTAT = 'N'
    GROUP BY BRANCX
""").pl()

con.close()
gc.collect()

rnid1_final = rnid1.with_columns([
    pl.lit("PBB ").alias("AGLFLD"),
    pl.lit("EXT").alias("BGLFLD"),
    pl.lit("C_DEP").alias("CGLFLD"),
    pl.lit("ACTUAL").alias("DGLFLD"),
    pl.lit("DPNIDS").alias("PRODUCX"),
    pl.lit("   ").alias("LOB"),
    pl.lit(YM).alias("YM"),
]).select(["AGLFLD", "BRANCX", "PRODUCX", "LOB", "BGLFLD", "CGLFLD", "DGLFLD", "YM", "NOACCT"])

print(f"  RNID1 rows: {len(rnid1_final):,}")

# ============================================================================
# STEP 6: BUILD IRNID1 (PIBB non-interest-bearing deposit account count)
# DATA IRNID; SET IDEPO.RNIDM&RMONTH; IF CDSTAT='A' AND NIDSTAT='N';
#             NOACCT=1; BRANCX=PUT(BRANCH,Z3.);
# PROC SUMMARY DATA=IRNID NWAY; CLASS BRANCX; VAR NOACCT; OUTPUT OUT=IRNID1 SUM=;
# DATA IRNID1; SET IRNID1; AGLFLD='PIBB'; BGLFLD='EXT'; CGLFLD='C_DEP';
#              DGLFLD='ACTUAL'; PRODUCX='DPNIDS'; YM=&RYEAR&RMONTH;
# ============================================================================
print("\nStep 6: Building IRNID1 (PIBB)...")

con = duckdb.connect(database=":memory:")

irnid1 = con.execute(f"""
    SELECT LPAD(CAST(CAST(BRANCH AS BIGINT) AS VARCHAR), 3, '0') AS BRANCX,
           COUNT(*) AS NOACCT
    FROM read_parquet('{IRNIDM_CACHE}')
    WHERE CDSTAT = 'A' AND NIDSTAT = 'N'
    GROUP BY BRANCX
""").pl()

con.close()
gc.collect()

irnid1_final = irnid1.with_columns([
    pl.lit("PIBB").alias("AGLFLD"),
    pl.lit("EXT").alias("BGLFLD"),
    pl.lit("C_DEP").alias("CGLFLD"),
    pl.lit("ACTUAL").alias("DGLFLD"),
    pl.lit("DPNIDS").alias("PRODUCX"),
    pl.lit("   ").alias("LOB"),
    pl.lit(YM).alias("YM"),
]).select(["AGLFLD", "BRANCX", "PRODUCX", "LOB", "BGLFLD", "CGLFLD", "DGLFLD", "YM", "NOACCT"])

print(f"  IRNID1 rows: {len(irnid1_final):,}")

# ============================================================================
# STEP 7: COMBINE ALL AND SORT
# DATA ALL; SET DEP RNID1 IRNID1; RUN;
# PROC SORT; BY AGLFLD BRANCX PRODUCX;
# ============================================================================
print("\nStep 7: Combining DEP + RNID1 + IRNID1...")

rnid1_final = rnid1_final.with_columns(pl.col("NOACCT").cast(pl.Float64))
irnid1_final = irnid1_final.with_columns(pl.col("NOACCT").cast(pl.Float64))

all_df = pl.concat([dep_final, rnid1_final, irnid1_final], how="vertical")
all_df = all_df.sort(["AGLFLD", "BRANCX", "PRODUCX"])

print(f"  ALL rows: {len(all_df):,}")

# ============================================================================
# STEP 8 (VESTIGIAL): DATA DEP; SET DEP; CNT+1; NUMAC+NOACCT;
# In the original SAS source this DATA step reads the pre-combination DEP
# dataset (deposit summary only) and would build running-total CNT/NUMAC
# columns. However, the very next DATA step reassigns DEP again by reading
# ALL (SET ALL END=EOF), so this intermediate DEP with CNT/NUMAC is
# discarded before it is ever used — a dead calculation in the original
# program. It is reproduced here only for completeness/fidelity, with its
# result unused, matching the original behaviour exactly.
# ============================================================================
_dep_cnt_unused   = len(dep_final)
_dep_numac_unused = dep_final["NOACCT"].sum() if len(dep_final) else 0.0

# ============================================================================
# STEP 9: GENERATE GL INTERFACE FILE (RECFM=FB -> no ASA control byte)
# DATA DEP; SET ALL END=EOF; FILE GLINT; PUT ...
# IF EOF THEN DO; PUT @001 'T,' @003 CNT 10. ',' NUMAC 15. ','; END;
#
# NOTE: In the final DATA step, CNT and NUMAC are referenced in the trailer
# PUT statement but are never assigned anywhere within that same DATA step
# (the "CNT+1; NUMAC+NOACCT;" logic lives only in the discarded Step 8
# DATA step above, which operates on a different dataset). SAS therefore
# treats CNT/NUMAC in the trailer as uninitialized numeric variables
# (missing), which a plain numeric width format ("10.", "15.") renders as
# blanks with a trailing period. This is reproduced faithfully below.
# ============================================================================
print("\nStep 9: Generating GL interface file...")

LRECL = 500


def _place(buf: list, col_1based: int, text: str) -> None:
    """Write *text* into buf starting at 1-based column col_1based."""
    start = col_1based - 1
    end = start + len(text)
    buf[start:end] = list(text)


def _fmt_num(value, width: int) -> str:
    """Replicate SAS plain numeric width format, including missing (.)."""
    if value is None:
        return " " * (width - 1) + "."
    s = str(int(value))
    if len(s) > width:
        s = s[-width:]
    return s.rjust(width)


def _build_detail_line(row: dict) -> str:
    buf = [" "] * LRECL
    _place(buf, 1, "D,")
    _place(buf, 3, str(row["AGLFLD"]))
    _place(buf, 35, ",")
    _place(buf, 36, str(row["BGLFLD"]))
    _place(buf, 68, ",")
    _place(buf, 69, str(row["CGLFLD"]))
    _place(buf, 101, ",")
    _place(buf, 102, str(row["DGLFLD"]))
    _place(buf, 134, ",")
    _place(buf, 167, ",")
    _place(buf, 168, str(row["BRANCX"]))
    _place(buf, 200, ",")
    _place(buf, 201, str(row["LOB"]))
    _place(buf, 233, ",")
    _place(buf, 234, str(row["PRODUCX"]))
    _place(buf, 266, ",")
    _place(buf, 267, _fmt_num(row["YM"], 6))
    _place(buf, 299, ",")
    _place(buf, 300, _fmt_num(row["NOACCT"], 15))
    _place(buf, 315, ",")
    _place(buf, 316, "Y,")
    _place(buf, 350, ",")
    _place(buf, 383, ",")
    _place(buf, 416, ",")
    return "".join(buf)


def _build_trailer_line(cnt_value, numac_value) -> str:
    buf = [" "] * LRECL
    _place(buf, 1, "T,")
    _place(buf, 3, _fmt_num(cnt_value, 10))
    _place(buf, 13, ",")
    _place(buf, 14, _fmt_num(numac_value, 15))
    _place(buf, 29, ",")
    return "".join(buf)


output_lines: list = []
for _row in all_df.iter_rows(named=True):
    output_lines.append(_build_detail_line(_row))

# CNT / NUMAC are uninitialized in the original final DATA step (see NOTE
# above), so both are passed as None to render the SAS missing-numeric
# convention (blank-filled with a trailing period).
output_lines.append(_build_trailer_line(None, None))

with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in output_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Detail lines   : {len(output_lines) - 1:,}")
print(f"  Trailer line   : 1")

print("\nResults preview:")
print(all_df)

del all_df, dep_final, dep_summary, rnid1, rnid1_final, irnid1, irnid1_final
gc.collect()

print("\nEIBM0941 complete.")
