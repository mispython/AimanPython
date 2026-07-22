#!/usr/bin/env python3
"""
Program : EIBDSDFT.py
Purpose : Special Deposit Facility (SDF) RM Account - Daily Extraction
          Generates daily extraction on Product Type 20 & 21 from Transaction
          History of M&I Deposit System (PBB + PIBB), enriched with customer
          name and transaction descriptions, and appended to a month-to-date
          cumulative file for reporting.
"""

import gc
import re
import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import date as _date_cls, timedelta

from REPTDATE import get_reptdate_values
from input_date import get_latest_file
# output_date.py NOT used: the original output DD (SAP.PBB.SDF.DAILY) is a
# GDG-catalogued dataset with no date component embedded in its name in SAS;
# the Python output filename is instead date-stamped directly from REPTDATE
# below (per requirement), so output_date.build_output_file's naming pattern
# is not needed.

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

# Daily deposit "current" extract files (DEPO / IDEPO, generation (0))
# and their "previous" generation (-1), resolved via input_date.get_latest_file
# and date-minus-one-day matching (same technique as EIBDLN1M.py LOAN/LOANX).
INPUT_DEPO_DIR = BASE_DIR / "input" / "prod" / "deposit_d"

# CIS.DEPOSIT — same physical customer-info deposit file referenced by
# EIBDLN1M.py (CISDP_deposit.sas7bdat); fixed filename, no date pattern.
INPUT_CIS_FILE = BASE_DIR / "input" / "prod" / "cis" / "CIS_deposit260720.sas7bdat"

# CRM.DPBTRAN&REPTYEAR&REPTMON&NOWK — weekly transaction-history extract.
# Its name is fully determined by REPTYEAR/REPTMON/NOWK, so it is built
# directly (no need to "search" for the latest file).
INPUT_DPBTRAN_DIR = BASE_DIR / "input" / "prod" / "cis"

# DETICA.TRANCODE — AML/Detica transaction-code lookup; fixed filename.
INPUT_TRANCODE_FILE = BASE_DIR / "input" / "prod" / "detica" / "trancode.sas7bdat"

# Parquet cache directory (kept across runs; freshness-checked)
CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBDSDFT"

# Output directory
OUTPUT_DIR = BASE_DIR / "output" / "EIBDSDFT"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# CHUNK SIZE FOR STREAMING LARGE .sas7bdat FILES
# ============================================================================
CHUNK_ROWS = 500_000

# ============================================================================
# OUTPUT RECORD LAYOUT  (DCB=(LRECL=1000,RECFM=FB) — NOT ASA; DLM='05'X)
# ============================================================================
LRECL = 1000
# DLM = "\x05"
DLM = "\t"

# ============================================================================
# STEP 1: REPORT DATE  (DATA REPTDATE; no reptdate.parquet — REPTDATE.py)
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values()
reptdate = reptdate_values.reptdate            # yesterday (SAS: TODAY()-1)

REPTDAY  = reptdate_values.reptday
REPTMON  = reptdate_values.reptmon
REPTYEAR = reptdate_values.reptyear
NOWK     = reptdate_values.nowk.zfill(2)        # SAS WEEK is $2. ('01'..'04')
RDATE    = reptdate.strftime("%d/%m/%Y")        # SAS PUT(REPTDATE,DDMMYY10.)

# Daily timestamp component used to date-stamp the output filename below.
RPTDT_STAMP = reptdate.strftime("%Y%m%d")       # e.g. 20260720

print(f"  Report date  : {RDATE}")
print(f"  REPTYEAR/MON/DAY/NOWK : {REPTYEAR}/{REPTMON}/{REPTDAY}/{NOWK}")

# Output filename — daily timestamped (one output file produced per run/day)
OUTPUT_FILE = OUTPUT_DIR / f"SDF_{RPTDT_STAMP}.txt"

print(f"  Output file  : {OUTPUT_FILE.name}")

# ============================================================================
# NOTE: JCL DELETE step (DD01 delete of SAP.PBB.SDF.DAILY) has no Python
# equivalent — the output file below is simply (re)written each run.
# ============================================================================

# ============================================================================
# STEP 2: RESOLVE INPUT FILE NAMES
# ============================================================================
print("\nStep 2: Resolving DEPO / IDEPO current & previous file names...")

pbb_depo_path  = get_latest_file(INPUT_DEPO_DIR, prefix="ca")
pibb_depo_path = get_latest_file(INPUT_DEPO_DIR, prefix="ica")


def _resolve_prev_day_file(current_path: Path, prefix: str, directory: Path) -> Path:
    """Find the file one calendar day earlier than *current_path*'s date."""
    match = re.search(rf"{prefix}(\d{{6}})", current_path.stem, re.IGNORECASE)
    if not match:
        raise ValueError(f"Cannot parse date from filename: {current_path.name}")
    date_str = match.group(1)                    # yymmdd
    yy, mm, dd = int(date_str[0:2]), int(date_str[2:4]), int(date_str[4:6])
    cur_date  = _date_cls(2000 + yy, mm, dd)
    prev_date = cur_date - timedelta(days=1)
    prev_stem = f"{prefix}{prev_date.strftime('%y%m%d')}"

    candidates = list(directory.glob(f"{prev_stem}*"))
    if not candidates:
        raise FileNotFoundError(
            f"Previous-generation file not found in {directory} "
            f"(expected prefix '{prev_stem}')"
        )
    return candidates[0]


pbb_depop_path  = _resolve_prev_day_file(pbb_depo_path, "ca", INPUT_DEPO_DIR)
pibb_depop_path = _resolve_prev_day_file(pibb_depo_path, "ica", INPUT_DEPO_DIR)

# DPBTRAN_FILE = INPUT_DPBTRAN_DIR / f"dpbtran{REPTYEAR}{REPTMON}{NOWK}.sas7bdat"
DPBTRAN_FILE = get_latest_file(INPUT_DPBTRAN_DIR, prefix="dpbtran")

print(f"  DEPO   : {pbb_depo_path.name}")
print(f"  DEPOP  : {pbb_depop_path.name}")
print(f"  IDEPO  : {pibb_depo_path.name}")
print(f"  IDEPOP : {pibb_depop_path.name}")
print(f"  DPBTRAN: {DPBTRAN_FILE.name}")

# ============================================================================
# HELPERS: CACHE STAMP + STREAMING SAS -> PARQUET  (from EIBDLN1M.py pattern)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    """Return True when the Parquet cache is newer than the source SAS file."""
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


def sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    """Convert a .sas7bdat to Parquet in streaming chunks (memory-efficient)."""
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0

    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
    for chunk in reader:
        table = pa.Table.from_pandas(chunk, preserve_index=False)

        if schema is None:
            # Lock schema on first chunk
            schema = table.schema
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")
        else:
            # Cast subsequent chunks to match the locked schema
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


def _ensure_cache(sas_path: Path, cache_path: Path, tag: str) -> None:
    """Convert *sas_path* to Parquet only if the cache is stale or missing."""
    if not _cache_is_fresh(sas_path, cache_path):
        sas_to_parquet(sas_path, cache_path, tag)
    else:
        print(f"  [{tag}] Cache fresh — skipping conversion.")


# ============================================================================
# STEP 3: CACHE ALL .sas7bdat SOURCE FILES TO PARQUET (if needed)
# All 7 raw inputs (DEPO, IDEPO, DEPOP, IDEPOP, CIS, DPBTRAN, TRANCODE) are
# .sas7bdat on disk; every downstream read in this program operates on the
# cached Parquet copies only, per the EIBDLN1M.py streaming-cache pattern.
# ============================================================================
print("\nStep 3: Caching SAS files to Parquet (if needed)...")

PBB_DEPO_CACHE   = CACHE_DIR / f"{pbb_depo_path.stem}.parquet"
PBB_DEPOP_CACHE  = CACHE_DIR / f"{pbb_depop_path.stem}.parquet"
PIBB_DEPO_CACHE  = CACHE_DIR / f"{pibb_depo_path.stem}.parquet"
PIBB_DEPOP_CACHE = CACHE_DIR / f"{pibb_depop_path.stem}.parquet"
CIS_CACHE        = CACHE_DIR / "cisdp_deposit.parquet"
DPBTRAN_CACHE    = CACHE_DIR / f"{DPBTRAN_FILE.stem}.parquet"
TRANCODE_CACHE   = CACHE_DIR / "detica_trancode.parquet"

_ensure_cache(pbb_depo_path, PBB_DEPO_CACHE, "DEPO")
_ensure_cache(pbb_depop_path, PBB_DEPOP_CACHE, "DEPOP")
_ensure_cache(pibb_depo_path, PIBB_DEPO_CACHE, "IDEPO")
_ensure_cache(pibb_depop_path, PIBB_DEPOP_CACHE, "IDEPOP")
_ensure_cache(INPUT_CIS_FILE, CIS_CACHE, "CIS")
_ensure_cache(DPBTRAN_FILE, DPBTRAN_CACHE, "DPBTRAN")
_ensure_cache(INPUT_TRANCODE_FILE, TRANCODE_CACHE, "TRANCODE")

# ============================================================================
# DEBUG: Inspect DPBTRAN parquet schema — confirm REPTDATE's actual type
# before it's used in Step 7. Remove once REPTDATE's column type is
# confirmed stable across cache rebuilds.
# ============================================================================
con = duckdb.connect(database=":memory:")
print("\n[DEBUG] DPBTRAN_CACHE schema:")
with pl.Config(tbl_rows=-1, tbl_cols=-1):
    print(con.execute(f"DESCRIBE SELECT * FROM read_parquet('{DPBTRAN_CACHE}')").pl())
con.close()

# ============================================================================
# STEP 4: BUILD SDF  (current-period deposit, product 20/21)
# DATA SDF; SET DEPO.CURRENT IDEPO.CURRENT; WHERE PRODUCT IN (20,21);
#   REPTDATE = &REPTDT; KEEP ACCTNO BRANCH OPENDT PRODUCT OPENIND CURBAL REPTDATE;
# All reads below use read_parquet() on the cached copies — never the raw
# .sas7bdat files directly.
# ============================================================================
print("\nStep 4: Building SDF (current period, PRODUCT 20/21)...")

con = duckdb.connect(database=":memory:")
sdf = con.execute(f"""
    WITH combined AS (
        SELECT
            CAST(ACCTNO  AS BIGINT)  AS ACCTNO,
            CAST(BRANCH  AS INTEGER) AS BRANCH,
            CAST(PRODUCT AS INTEGER) AS PRODUCT,
            CAST(CURBAL  AS DOUBLE)  AS CURBAL
        FROM read_parquet('{PBB_DEPO_CACHE}')

        UNION ALL

        SELECT
            CAST(ACCTNO  AS BIGINT)  AS ACCTNO,
            CAST(BRANCH  AS INTEGER) AS BRANCH,
            CAST(PRODUCT AS INTEGER) AS PRODUCT,
            CAST(CURBAL  AS DOUBLE)  AS CURBAL
        FROM read_parquet('{PIBB_DEPO_CACHE}')
    )
    SELECT
        ACCTNO, BRANCH, PRODUCT, CURBAL,
        DATE '{reptdate.isoformat()}' AS REPTDATE
    FROM combined
    WHERE PRODUCT IN (20, 21)
""").pl()
con.close()
print(f"  SDF rows: {len(sdf):,}")

# ============================================================================
# STEP 5: BUILD SDFPREV  (previous-period deposit, no PRODUCT filter)
# DATA SDFPREV(RENAME=(CURBAL=PCURBAL)); SET DEPOP.CURRENT IDEPOP.CURRENT;
#   KEEP ACCTNO CURBAL;
# ============================================================================
print("\nStep 5: Building SDFPREV (previous period)...")

con = duckdb.connect(database=":memory:")
sdfprev = con.execute(f"""
    SELECT CAST(ACCTNO AS BIGINT) AS ACCTNO, CAST(CURBAL AS DOUBLE) AS PCURBAL
    FROM read_parquet('{PBB_DEPOP_CACHE}')
    UNION ALL
    SELECT CAST(ACCTNO AS BIGINT) AS ACCTNO, CAST(CURBAL AS DOUBLE) AS PCURBAL
    FROM read_parquet('{PIBB_DEPOP_CACHE}')
""").pl()
con.close()
print(f"  SDFPREV rows: {len(sdfprev):,}")

# ============================================================================
# STEP 6: BUILD CIS  (customer name lookup, SECCUST='901')
# PROC SORT DATA=CIS.DEPOSIT(KEEP=ACCTNO CUSTNAME SECCUST) OUT=CIS
#   WHERE SECCUST='901'; BY ACCTNO;
# ============================================================================
print("\nStep 6: Building CIS (customer name, SECCUST=901)...")

con = duckdb.connect(database=":memory:")
cis = con.execute(f"""
    SELECT
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(CUSTNAM1 AS VARCHAR) AS CUSTNAME
    FROM read_parquet('{CIS_CACHE}')
    WHERE CAST(SECCUST AS VARCHAR) = '901'
""").pl()
con.close()
print(f"  CIS rows: {len(cis):,}")

# ============================================================================
# STEP 7: BUILD DPBTRAN  (day's transaction lines, ordered ACCTNO/TIMECTRL)
# PROC SORT DATA=CRM.DPBTRAN&REPTYEAR&REPTMON&NOWK
#   OUT=DPBTRAN(KEEP=ACCTNO TRANCODE TRANAMT TIMECTRL)
#   WHERE REPTDATE = &REPTDT; BY ACCTNO TIMECTRL;
# ============================================================================
print("\nStep 7: Building DPBTRAN (today's transactions)...")

# con = duckdb.connect(database=":memory:")
# dpbtran = con.execute(f"""
#     SELECT
#         CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
#         CAST(TRANCODE AS BIGINT)  AS TRANCODE,
#         CAST(TRANAMT  AS DOUBLE)  AS TRANAMT,
#         CAST(TIMECTRL AS BIGINT)  AS TIMECTRL
#     FROM read_parquet('{DPBTRAN_CACHE}')
#     WHERE CAST(REPTDATE AS DATE) = DATE '{reptdate.isoformat()}'
#     ORDER BY ACCTNO, TIMECTRL
# """).pl()
# con.close()

con = duckdb.connect(database=":memory:")
dpbtran = con.execute(f"""
    SELECT
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(TRANCODE AS BIGINT)  AS TRANCODE,
        CAST(TRANAMT  AS DOUBLE)  AS TRANAMT,
        CAST(TIMECTRL AS VARCHAR) AS TIMECTRL
    FROM read_parquet('{DPBTRAN_CACHE}')
    -- REPTDATE is cached as a raw SAS numeric date (DOUBLE = days since
    -- 1960-01-01), not a native DATE/TIMESTAMP. DuckDB has no direct
    -- DOUBLE -> DATE cast, so reconstruct it via the SAS epoch instead.
    WHERE (DATE '1960-01-01' + CAST(ROUND(REPTDATE) AS INTEGER))
          = DATE '{reptdate.isoformat()}'
    ORDER BY ACCTNO, TIMECTRL
""").pl()
con.close()
print(f"  DPBTRAN rows: {len(dpbtran):,}")

# ============================================================================
# STEP 8: BUILD TRANCD  (latest TXN_CODE description/CRDR, ACCTCODE='DP')
# PROC SORT DATA=DETICA.TRANCODE
#   OUT=TRANCD(KEEP=TXN_CODE TXN_DESC CRDR EFFDATETIME)
#   BY TXN_CODE DESCENDING EFFDATETIME; WHERE ACCTCODE = 'DP';
# PROC SORT NODUPKEY; BY TXN_CODE;
# ============================================================================
print("\nStep 8: Building TRANCD (transaction-code lookup)...")

# con = duckdb.connect(database=":memory:")
# trancd = con.execute(f"""
#     WITH filtered AS (
#         SELECT
#             CAST(TXN_CODE     AS BIGINT)  AS TXN_CODE,
#             CAST(TXN_DESC     AS VARCHAR) AS TXN_DESC,
#             CAST(CRDR         AS VARCHAR) AS CRDR,
#             CAST(EFFDATETIME  AS TIMESTAMP) AS EFFDATETIME
#         FROM read_parquet('{TRANCODE_CACHE}')
#         WHERE CAST(ACCTCODE AS VARCHAR) = 'DP'
#     ),
#     ranked AS (
#         SELECT *,
#                ROW_NUMBER() OVER (
#                    PARTITION BY TXN_CODE
#                    ORDER BY EFFDATETIME DESC
#                ) AS rn
#         FROM filtered
#     )
#     SELECT TXN_CODE, TXN_DESC, CRDR
#     FROM ranked
#     WHERE rn = 1
# """).pl()
# con.close()

con = duckdb.connect(database=":memory:")
trancd = con.execute(f"""
    WITH filtered AS (
        SELECT
            CAST(TXN_CODE     AS BIGINT)  AS TXN_CODE,
            CAST(TXN_DESC     AS VARCHAR) AS TXN_DESC,
            CAST(CRDR         AS VARCHAR) AS CRDR,
            TIMESTAMP '1960-01-01 00:00:00'
                + to_seconds(CAST(ROUND(EFFDATETIME) AS BIGINT)) AS EFFDATETIME
        FROM read_parquet('{TRANCODE_CACHE}')
        WHERE CAST(ACCTCODE AS VARCHAR) = 'DP'
    ),
    ranked AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY TXN_CODE
                   ORDER BY EFFDATETIME DESC
               ) AS rn
        FROM filtered
    )
    SELECT TXN_CODE, TXN_DESC, CRDR
    FROM ranked
    WHERE rn = 1
""").pl()
con.close()

# DATA TRXFMT: override description for special codes 828 / 629.
trancd = trancd.with_columns(
    pl.when(pl.col("TXN_CODE") == 828).then(pl.lit("MISC DR"))
      .when(pl.col("TXN_CODE") == 629).then(pl.lit("MISC CR"))
      .otherwise(pl.col("TXN_DESC"))
      .alias("TXN_DESC")
)
print(f"  TRANCD rows: {len(trancd):,}")

# ============================================================================
# STEP 9: MERGE  SDF(IN=A) SDFPREV CIS DPBTRAN; BY ACCTNO; IF A;
# SDF/SDFPREV/CIS are one-row-per-ACCTNO; DPBTRAN can be many-rows-per-ACCTNO.
# SAS's many-to-one MERGE broadcasts the "one" side's values across every
# DPBTRAN row for that ACCTNO (and still emits one row even with no
# DPBTRAN match) — replicated here as sequential LEFT JOINs, base = SDF.
# ============================================================================
print("\nStep 9: Merging SDF + SDFPREV + CIS + DPBTRAN...")

merged = (
    sdf
    .join(sdfprev, on="ACCTNO", how="left")
    .join(cis, on="ACCTNO", how="left")
    .join(dpbtran, on="ACCTNO", how="left")
    .sort(["ACCTNO", "TIMECTRL"], nulls_last=True, maintain_order=True)
)

# ============================================================================
# STEP 10: DERIVE TRANDESC / CRDR (format lookup), DEBIT/CREDIT, fill missing
# TRANDESC = PUT(TRANCODE,TRXDESC.); CRDR = PUT(TRANCODE,TRXCRDR.);
# IF CRDR='D' THEN DEBIT=-TRANAMT; ELSE CREDIT=TRANAMT;
# IF PCURBAL=. THEN PCURBAL=0.00;  (same for DEBIT/CREDIT/CURBAL)
# ============================================================================
print("\nStep 10: Deriving TRANDESC/CRDR and DEBIT/CREDIT...")

merged = merged.join(
    trancd.rename({"TXN_CODE": "TRANCODE", "TXN_DESC": "TRANDESC"}),
    on="TRANCODE",
    how="left",
)

merged = merged.with_columns(
    pl.when(pl.col("CRDR") == "D")
      .then(-pl.col("TRANAMT"))
      .otherwise(None)
      .alias("DEBIT"),
    pl.when(pl.col("CRDR") == "D")
      .then(None)
      .otherwise(pl.col("TRANAMT"))
      .alias("CREDIT"),
)

merged = merged.with_columns(
    pl.col("PCURBAL").fill_null(0.0),
    pl.col("DEBIT").fill_null(0.0),
    pl.col("CREDIT").fill_null(0.0),
    pl.col("CURBAL").fill_null(0.0),
)

print(f"  Merged rows: {len(merged):,}")

# ============================================================================
# STEP 11: PERSIST DAY SNAPSHOT + APPEND TO MONTH-TO-DATE CACHE
# DATA SDFD.SDF&REPTDAY SDF; ... (day snapshot)
# %MACRO APPEND; ... DATA SDFD.SDFALL&REPTMON; ... PROC SORT BY REPTDATE ACCTNO;
# SDFD is a self-derived permanent library in SAS (not a raw external input):
# it is seeded/rolled-forward entirely from this program's own daily output,
# so the Parquet equivalents below (DAY_CACHE_FILE / MONTH_CACHE_FILE) are
# both read from and written to by this same program run over run.
# ============================================================================
print("\nStep 11: Persisting day snapshot and month-to-date accumulation...")

OUTPUT_COLS = [
    "REPTDATE", "BRANCH", "ACCTNO", "CUSTNAME", "PRODUCT",
    "PCURBAL", "TRANCODE", "TRANDESC", "DEBIT", "CREDIT", "CURBAL",
]
day_snapshot = merged.select(OUTPUT_COLS)

DAY_CACHE_FILE   = CACHE_DIR / f"sdf_day_{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
MONTH_CACHE_FILE = CACHE_DIR / f"sdfall_{REPTYEAR}{REPTMON}.parquet"

day_snapshot.write_parquet(DAY_CACHE_FILE)

if REPTDAY == "01":
    # First day of month — start a fresh month-to-date file.
    sdfall = day_snapshot
else:
    if MONTH_CACHE_FILE.exists():
        existing = pl.read_parquet(MONTH_CACHE_FILE)
        # Remove any stale entry for today's REPTDATE (supports re-run).
        existing = existing.filter(pl.col("REPTDATE") != reptdate)
    else:
        existing = day_snapshot.clear()
    sdfall = pl.concat([existing, day_snapshot], how="vertical")

sdfall = sdfall.sort(["REPTDATE", "ACCTNO"], maintain_order=True)
sdfall.write_parquet(MONTH_CACHE_FILE)

print(f"  Day cache    : {DAY_CACHE_FILE.name}")
print(f"  Month cache  : {MONTH_CACHE_FILE.name} ({len(sdfall):,} rows)")

del merged, day_snapshot
gc.collect()

# ============================================================================
# STEP 12: GENERATE OUTPUT FILE
# DATA _NULL_; SET SDFD.SDFALL&REPTMON; WHERE REPTDATE <= &REPTDT;
# FORMAT REPTDATE DDMMYY10. PCURBAL DEBIT CREDIT CURBAL COMMA16.2;
# DLM='05'X; FILE SDFFL;  (RECFM=FB, LRECL=1000 — no ASA carriage control)
# ============================================================================
print("\nStep 12: Generating output file...")

report_df = sdfall.filter(pl.col("REPTDATE") <= reptdate)


def _fmt_comma16(value) -> str:
    """COMMA16.2 — right-justified, width 16, thousands separator, 2 dp."""
    if value is None:
        return " " * 16
    return f"{float(value):,.2f}".rjust(16)


def _fmt_num12(value) -> str:
    """Default numeric BEST12. — right-justified, width 12, no format."""
    if value is None:
        return " " * 12
    return f"{int(round(float(value)))}".rjust(12)


def _fmt_char(value, width: int) -> str:
    """Character variable — left-justified, padded/truncated to *width*."""
    return f"{str(value or '')[:width]:<{width}s}"


def _pad_record(line: str) -> str:
    return line[:LRECL].ljust(LRECL)


output_lines: list[str] = []

# Title line + blank line + delimited column-header line (IF _N_=1 block)
output_lines.append(_pad_record(f"Special Deposit Facility (SDF) RM Account as at {RDATE}"))
output_lines.append(_pad_record(""))
header_fields = [
    "Date", "Branch", "SDF Account Number", "Customer Name", "Product",
    "Opening Balance", "Transaction Code", "Description", "Debit",
    "Credit", "Outstanding Balance",
]
output_lines.append(_pad_record(DLM + DLM.join(header_fields) + DLM))

for row in report_df.iter_rows(named=True):
    fields = [
        row["REPTDATE"].strftime("%d/%m/%Y"),
        _fmt_num12(row["BRANCH"]),
        _fmt_num12(row["ACCTNO"]),
        _fmt_char(row["CUSTNAME"], 40),
        _fmt_num12(row["PRODUCT"]),
        _fmt_comma16(row["PCURBAL"]),
        _fmt_num12(row["TRANCODE"]),
        _fmt_char(row["TRANDESC"], 40),
        _fmt_comma16(row["DEBIT"]),
        _fmt_comma16(row["CREDIT"]),
        _fmt_comma16(row["CURBAL"]),
    ]
    output_lines.append(_pad_record(DLM + DLM.join(fields) + DLM))

with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in output_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(output_lines):,}")
print("\n  --- Output preview ---")
for ln in output_lines[:20]:
    print(ln.rstrip())
if len(output_lines) > 20:
    print(f"  ... ({len(output_lines) - 20} more lines)")

del sdfall, report_df
gc.collect()

print("\nEIBDSDFT complete.")
