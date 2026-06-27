#!/usr/bin/env python3
"""
Program : EIBDLN1M.py
Purpose : Daily Movement in Bank's Loans/OD Accounts Report
          Net Increased/(Decreased) of RM1 Million & Above Per Customer
"""

import os
import gc
import re
import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import timedelta

from REPTDATE import get_reptdate_values
from input_date import get_latest_file

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# Input directories - Production
# BASE_DIR    = Path("/dwh")

# INPUT_LOAN_DIR    = BASE_DIR / "lnd_ln"
# INPUT_CISLN_DIR   = Path("/stgsrcsys/host/uat") / "CISLN_loan.sas7bdat"
# INPUT_CISDP_DIR   = Path("/stgsrcsys/host/uat") / "CISDP_deposit.sas7bdat"
# INPUT_BRANCH_FILE = Path("/sasdata/rawdata/lookup") / "LKP_BRANCH"

# Input directories - Testing
BASE_DIR    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

INPUT_LOAN_DIR    = BASE_DIR / "input" / "prod" / "EIBDLN1M"
INPUT_CISLN_DIR   = Path("/stgsrcsys/host/uat") / "CISLN_loan.sas7bdat"
INPUT_CISDP_DIR   = Path("/stgsrcsys/host/uat") / "CISDP_deposit.sas7bdat"
INPUT_BRANCH_FILE = Path("/sasdata/rawdata/lookup") / "LKP_BRANCH"

# Parquet cache directory (temporary intermediates — cleared after use)
CACHE_DIR = BASE_DIR / "input" / "prod" / "EIBDLN1M"

# Output
OUTPUT_DIR  = BASE_DIR / "output" / "EIBDLN1M"
OUTPUT_FILE = OUTPUT_DIR / "EIBDLN1M_report.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# CHUNK SIZE FOR STREAMING LARGE .sas7bdat FILES
# ============================================================================
CHUNK_ROWS   = 500_000
ROW_LIMIT    = int(os.environ.get("ROW_LIMIT", 0))   # 0 = no limit (test mode via env)

# ============================================================================
# REPORT PAGE CONFIGURATION
# ============================================================================
PAGE_SIZE      = 60    # lines per page (SAS default)
HEADER_LINES   = 10    # header block occupies 10 lines

# ============================================================================
# STEP 1: REPORT DATE  (no reptdate.parquet — derive from REPTDATE.py)
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values()
reptdate  = reptdate_values.reptdate                     # yesterday
reptpdat  = reptdate - timedelta(days=1)                 # day before yesterday

RDATE     = reptdate.strftime("%d/%m/%y")
REPTDAY   = reptdate.strftime("%d")
REPTMON   = reptdate.strftime("%m")
REPTYEAR  = reptdate.strftime("%y")
REPTPDAY  = reptpdat.strftime("%d")
REPTPMON  = reptpdat.strftime("%m")
REPTPYEA  = reptpdat.strftime("%y")

print(f"  Report date  : {RDATE}")
print(f"  Current  : {REPTMON}/{REPTDAY}/{REPTYEAR}")
print(f"  Previous : {REPTPMON}/{REPTPDAY}/{REPTPYEA}")

# ============================================================================
# STEP 2: RESOLVE INPUT FILE NAMES  (LOAN = latest, LOANX = day before)
# ============================================================================
print("\nStep 2: Resolving LOAN / LOANX file names...")

# get_latest_file returns the newest file whose name encodes a date
loan_path  = get_latest_file(INPUT_LOAN_DIR, prefix="ln")

# Derive the date embedded in the LOAN filename, then find the file whose
# encoded date is exactly one calendar day earlier.
_ln_date_match = re.search(r"ln(\d{6})", loan_path.stem, re.IGNORECASE)
if not _ln_date_match:
    raise ValueError(f"Cannot parse date from LOAN filename: {loan_path.name}")

_ln_date_str  = _ln_date_match.group(1)          # e.g. "260625"  (yymmdd)
_ln_year      = 2000 + int(_ln_date_str[0:2])
_ln_month     = int(_ln_date_str[2:4])
_ln_day       = int(_ln_date_str[4:6])

from datetime import date as _date_cls
_loan_date  = _date_cls(_ln_year, _ln_month, _ln_day)
_loanx_date = _loan_date - timedelta(days=1)
_loanx_stem = f"ln{_loanx_date.strftime('%y%m%d')}"

loanx_candidates = list(INPUT_LOAN_DIR.glob(f"{_loanx_stem}*"))
if not loanx_candidates:
    raise FileNotFoundError(
        f"LOANX file not found in {INPUT_LOAN_DIR} "
        f"(expected prefix '{_loanx_stem}')"
    )
loanx_path = loanx_candidates[0]

print(f"  LOAN  : {loan_path.name}")
print(f"  LOANX : {loanx_path.name}")

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
# HELPER: STREAM .sas7bdat → PARQUET  (memory-efficient chunked conversion)
# ============================================================================
def sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    """Convert a large .sas7bdat to Parquet in streaming chunks."""
    print(f"  [{tag}] Converting {sas_path.name} → {cache_path.name} ...")
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

        # table = pa.Table.from_pandas(chunk, preserve_index=False)

        chunk = chunk.convert_dtypes()

        table = pa.Table.from_pandas(
            chunk,
            preserve_index=False,
            safe=False
        )

        if writer is None:
            schema = table.schema
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")
        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer:
        writer.close()
    print(f"  [{tag}] Done — {total:,} rows cached.")

# ============================================================================
# STEP 3: CACHE LARGE SAS FILES TO PARQUET
# ============================================================================
print("\nStep 3: Caching large SAS files to Parquet (if needed)...")

LOAN_CACHE  = CACHE_DIR / f"{loan_path.stem}.parquet"
LOANX_CACHE = CACHE_DIR / f"{loanx_path.stem}.parquet"
CISLN_CACHE = CACHE_DIR / "cisln.parquet"
CISDP_CACHE = CACHE_DIR / "cisdp.parquet"

# LOAN  (~2 GB)
if not _cache_is_fresh(loan_path, LOAN_CACHE):
    sas_to_parquet(loan_path, LOAN_CACHE, "LOAN")
else:
    print(f"  [LOAN ] Cache fresh — skipping conversion.")

# LOANX (~2 GB)
if not _cache_is_fresh(loanx_path, LOANX_CACHE):
    sas_to_parquet(loanx_path, LOANX_CACHE, "LOANX")
else:
    print(f"  [LOANX] Cache fresh — skipping conversion.")

# CISLN (~14.2 GB) — fixed filename, no date pattern
if not _cache_is_fresh(INPUT_CISLN_DIR , CISLN_CACHE):
    sas_to_parquet(INPUT_CISLN_DIR , CISLN_CACHE, "CISLN")
else:
    print(f"  [CISLN] Cache fresh — skipping conversion.")

# CISDP (~1.2 GB) — fixed filename, no date pattern
if not _cache_is_fresh(INPUT_CISDP_DIR , CISDP_CACHE):
    sas_to_parquet(INPUT_CISDP_DIR , CISDP_CACHE, "CISDP")
else:
    print(f"  [CISDP] Cache fresh — skipping conversion.")

# # Release file-path objects no longer needed
# del cisln_path, cisdp_path
# gc.collect()

# ============================================================================
# STEP 4: BUILD CISNM  (customer-name lookup)
# DATA CISNM;
#   KEEP ACCTNO CUSTNAME SECCUST;
#   SET CISDP.DEPOSIT CISLN.LOAN;
#   CUSTNAME=CUSTNAM1;
# PROC SORT NODUPKEY; BY ACCTNO;  (keep first after sort BY ACCTNO SECCUST CUSTNAME)
# ============================================================================
print("\nStep 4: Building CISNM (customer name lookup)...")

con = duckdb.connect(database=":memory:")

cisnm = con.execute(f"""
    WITH combined AS (
        SELECT
            CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
            CAST(CUSTNAM1 AS VARCHAR) AS CUSTNAME,
            CAST(SECCUST  AS VARCHAR) AS SECCUST
        FROM read_parquet('{CISDP_CACHE}')

        UNION ALL

        SELECT
            CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
            CAST(CUSTNAM1 AS VARCHAR) AS CUSTNAME,
            CAST(SECCUST  AS VARCHAR) AS SECCUST
        FROM read_parquet('{CISLN_CACHE}')
    ),
    ranked AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY ACCTNO
                   ORDER BY SECCUST, CUSTNAME
               ) AS rn
        FROM combined
    )
    SELECT ACCTNO, CUSTNAME, SECCUST
    FROM ranked
    WHERE rn = 1
""").pl()

con.close()
gc.collect()

print(f"  CISNM rows: {len(cisnm):,}")

# ============================================================================
# STEP 5: BUILD SASC  (current-period loan summary)
# DATA SASC; SET LOAN.LOAN&REPTMON&REPTDAY; ...
# Accumulate ACCBAL / LIMTBAL per BRANCH+ACCTNO (SAS LAST.ACCTNO pattern)
# ============================================================================
print("\nStep 5: Building SASC (current period)...")

con = duckdb.connect(database=":memory:")

sasc = con.execute(f"""
    WITH base AS (
        SELECT
            CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
            CAST(BRANCH   AS INTEGER) AS BRANCH,
            'C'                       AS TDI,
            CAST(APPRLIMT AS DOUBLE)  AS APPRLIMT,
            CAST(APPRLIM2 AS DOUBLE)  AS APPRLIM2,
            CAST(BALANCE  AS DOUBLE)  AS BALANCE,
            CAST(ACCTYPE  AS VARCHAR) AS ACCTYPE,
            CAST(PRODUCT  AS INTEGER) AS PRODUCT
        FROM read_parquet('{LOAN_CACHE}')
    ),
    categorised AS (
        SELECT *,
            CASE
                WHEN ACCTYPE = 'OD' AND PRODUCT NOT IN (107,173) THEN 'OD'
                WHEN ACCTYPE = 'LN' AND PRODUCT IN
                     (302,350,364,365,506,902,903,910,925,951)     THEN 'RC'
                WHEN ACCTYPE = 'LN' AND PRODUCT IN
                     (128,130,131,132,380,381,700,705,720,725)     THEN 'HP'
                WHEN ACCTYPE = 'LN'                                THEN 'TL'
                ELSE NULL
            END AS CATG
        FROM base
        -- OD with PRODUCT IN (107,173) → excluded (SAS: DELETE)
        WHERE NOT (ACCTYPE = 'OD' AND PRODUCT IN (107,173))
    ),
    filtered AS (
        SELECT ACCTNO, BRANCH, TDI, APPRLIMT, APPRLIM2, BALANCE, CATG
        FROM categorised
        WHERE CATG IS NOT NULL
    )
    -- SAS LAST.ACCTNO accumulation within BRANCH+ACCTNO
    SELECT
        ACCTNO,
        BRANCH,
        TDI,
        MAX(APPRLIMT)   AS APPRLIMT,
        MAX(APPRLIM2)   AS APPRLIM2,
        ANY_VALUE(CATG) AS CATG,
        SUM(BALANCE)    AS ACCBAL,
        SUM(APPRLIMT)   AS LIMTBAL
    FROM filtered
    GROUP BY ACCTNO, BRANCH, TDI, CATG
""").pl()

con.close()
gc.collect()
print(f"  SASC rows: {len(sasc):,}")

# ============================================================================
# STEP 6: BUILD SASP  (previous-period loan summary)
# DATA SASP; SET LOANX.LOAN&REPTPMON&REPTPDAY; ...
# ============================================================================
print("\nStep 6: Building SASP (previous period)...")

con = duckdb.connect(database=":memory:")

sasp = con.execute(f"""
    WITH base AS (
        SELECT
            CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
            CAST(BRANCH   AS INTEGER) AS BRANCH,
            'P'                       AS TDI,
            CAST(APPRLIMT AS DOUBLE)  AS APPRLIMT,
            CAST(BALANCE  AS DOUBLE)  AS BALANCE
        FROM read_parquet('{LOANX_CACHE}')
    ),
    categorised AS (
        SELECT
            ACCTNO, BRANCH, TDI, APPRLIMT, BALANCE,
            CASE
                WHEN CAST((SELECT ACCTYPE FROM read_parquet('{LOANX_CACHE}') l
                            WHERE l.ACCTNO = base.ACCTNO LIMIT 1) AS VARCHAR) = 'OD'
                     THEN 'OD'   -- placeholder; resolved below
                ELSE NULL
            END AS CATG
        FROM base
    )
    SELECT 1   -- dummy; see note below
""").pl()
con.close()

# NOTE: The sub-select approach above would be very slow on 2 GB.
# Use a single efficient pass instead:
con = duckdb.connect(database=":memory:")

sasp = con.execute(f"""
    WITH base AS (
        SELECT
            CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
            CAST(BRANCH   AS INTEGER) AS BRANCH,
            'P'                       AS TDI,
            CAST(APPRLIMT AS DOUBLE)  AS APPRLIMT,
            CAST(BALANCE  AS DOUBLE)  AS BALANCE,
            CAST(ACCTYPE  AS VARCHAR) AS ACCTYPE,
            CAST(PRODUCT  AS INTEGER) AS PRODUCT
        FROM read_parquet('{LOANX_CACHE}')
    ),
    categorised AS (
        SELECT *,
            CASE
                WHEN ACCTYPE = 'OD' AND PRODUCT NOT IN (107,173) THEN 'OD'
                WHEN ACCTYPE = 'LN' AND PRODUCT IN
                     (302,350,364,365,506,902,903,910,925,951)     THEN 'RC'
                WHEN ACCTYPE = 'LN' AND PRODUCT IN
                     (128,130,131,132,380,381,700,705,720,725)     THEN 'HP'
                WHEN ACCTYPE = 'LN'                                THEN 'TL'
                ELSE NULL
            END AS CATG
        FROM base
        WHERE NOT (ACCTYPE = 'OD' AND PRODUCT IN (107,173))
    ),
    filtered AS (
        SELECT ACCTNO, BRANCH, TDI, APPRLIMT, BALANCE, CATG
        FROM categorised
        WHERE CATG IS NOT NULL
    )
    SELECT
        ACCTNO,
        BRANCH,
        TDI,
        ANY_VALUE(CATG) AS CATG,
        SUM(BALANCE)    AS PACCBAL,
        SUM(APPRLIMT)   AS PLIMBAL
    FROM filtered
    GROUP BY ACCTNO, BRANCH, TDI, CATG
""").pl()

con.close()
gc.collect()
print(f"  SASP rows: {len(sasp):,}")

# ============================================================================
# STEP 7: MERGE SASP + SASC  (SAS last-dataset-wins = SASC wins)
# DATA SASC; MERGE SASP SASC; BY BRANCH ACCTNO;
# IF TDI='P' THEN LIMTBAL=PLIMBAL;
# MOVEAMTS = ACCBAL - PACCBAL;  IF MOVEAMT >= 1000000;
# ============================================================================
print("\nStep 7: Merging SASP + SASC and calculating movements...")

# SAS MERGE SASP SASC — SASC columns overwrite SASP columns when both present.
# Columns that only exist in SASP (PACCBAL, PLIMBAL) are always retained.
# TDI from SASC ('C') overwrites SASP ('P') when there is a match;
# when there is NO SASC row (left-only), TDI remains 'P'.
sasc_pd = sasc.to_pandas()
sasp_pd = sasp.to_pandas()

merged = pd.merge(
    sasp_pd[["ACCTNO", "BRANCH", "CATG", "PACCBAL", "PLIMBAL", "TDI"]].rename(
        columns={"TDI": "TDI_P", "CATG": "CATG_P"}
    ),
    sasc_pd[["ACCTNO", "BRANCH", "CATG", "ACCBAL", "LIMTBAL", "APPRLIMT",
              "APPRLIM2", "TDI"]].rename(
        columns={"TDI": "TDI_C", "CATG": "CATG_C"}
    ),
    on=["BRANCH", "ACCTNO"],
    how="outer",
)

# Replicate SAS last-dataset-wins for overlapping fields
merged["ACCBAL"]   = merged["ACCBAL"].fillna(0.0)
merged["PACCBAL"]  = merged["PACCBAL"].fillna(0.0)
merged["LIMTBAL"]  = merged["LIMTBAL"].fillna(0.0)
merged["PLIMBAL"]  = merged["PLIMBAL"].fillna(0.0)

# CATG: SASC wins when present
merged["CATG"]     = merged["CATG_C"].where(
    merged["CATG_C"].notna(), merged["CATG_P"]
)

# TDI: SASC ('C') wins when present; otherwise SASP ('P')
merged["TDI"]      = merged["TDI_C"].where(
    merged["TDI_C"].notna(), merged["TDI_P"]
)

# IF TDI='P' THEN LIMTBAL=PLIMBAL  (record existed only in previous period)
merged.loc[merged["TDI"] == "P", "LIMTBAL"] = merged.loc[
    merged["TDI"] == "P", "PLIMBAL"
]

merged["MOVEAMTS"] = merged["ACCBAL"] - merged["PACCBAL"]
merged["MOVEAMT"]  = merged["MOVEAMTS"].abs()

# IF MOVEAMT >= 1000000
merged = merged[merged["MOVEAMT"] >= 1_000_000].copy()

# Drop helper columns
merged.drop(columns=["TDI_P", "TDI_C", "CATG_P", "CATG_C"], inplace=True)

del sasc, sasp, sasc_pd, sasp_pd
gc.collect()
print(f"  Records with movement >= RM1M: {len(merged):,}")

# ============================================================================
# STEP 8: READ BRANCH FILE  (fixed-width flat file)
# INPUT @001 BANK $1.  @002 BRANCH 3.  @006 BRNAME $3.
# ============================================================================
print("\nStep 8: Reading branch flat file...")

branch_rows = []
with open(INPUT_BRANCH_FILE, "rb") as fh:
    for raw in fh:
        line = raw.rstrip(b"\r\n")
        if len(line) < 8:
            continue
        bank   = line[0:1].decode("latin1").strip()
        branch = int(line[1:4].decode("latin1").strip() or 0)
        brname = line[5:8].decode("latin1")
        branch_rows.append({"BRANCH": branch, "BANK": bank, "BRNAME": brname})

branch_df = pl.DataFrame(branch_rows)
print(f"  Branch rows: {len(branch_df):,}")

# ============================================================================
# STEP 9: MERGE WITH BRANCH  (MERGE BRANCH SASC(IN=A); BY BRANCH; IF A)
# Only keep rows that exist in SASC (the movement dataset).
# ============================================================================
print("\nStep 9: Merging with branch data...")

merged_pl = pl.from_pandas(merged)

final = merged_pl.join(branch_df, on="BRANCH", how="left")

del merged, merged_pl, branch_df
gc.collect()

# ============================================================================
# STEP 10: MERGE WITH CISNM  (MERGE CISNM SASC(IN=A); BY ACCTNO; IF A)
# ============================================================================
print("\nStep 10: Merging with customer name data...")

final = final.join(cisnm, on="ACCTNO", how="left")

del cisnm
gc.collect()

# ============================================================================
# Build CATEGORY label
# IF CATG='OD' THEN CATEGORY='OVERDRAFT       '; ...
# IF CATG NE '  ';
# ============================================================================
catg_map = {
    "OD": "OVERDRAFT       ",
    "TL": "TERM LOAN       ",
    "HP": "HIRE PURCHASE   ",
    "RC": "REVOLVING CREDIT",
}

final = final.with_columns(
    pl.col("CATG").replace(catg_map, default=None).alias("CATEGORY")
).filter(pl.col("CATG").is_not_null())

# PROC SORT; BY CATEGORY BRANCH ACCTNO;
final = final.sort(["CATEGORY", "BRANCH", "ACCTNO"])

print(f"  Final report rows: {len(final):,}")

# ============================================================================
# STEP 11: GENERATE REPORT  (ASA carriage control, LRECL=133)
# SAS FILE PRINT with HEADER= label implies ASA carriage control.
# '1' = new page, ' ' = single space, '0' = double space.
# ============================================================================
print("\nStep 11: Generating report...")

def _fmt_comma(value, width: int, decimals: int = 0) -> str:
    """Format number with comma separators, right-justified to *width*."""
    if value is None:
        return " " * width
    try:
        v = float(value)
    except (TypeError, ValueError):
        return " " * width
    if decimals > 0:
        s = f"{v:,.{decimals}f}"
    else:
        s = f"{int(round(v)):,}"
    return s.rjust(width)


def _build_header(category: str) -> list[str]:
    """
    Build the NEWPAGE header block (ASA control char fused to each line).
    Mirrors the SAS HEADER=NEWPAGE label exactly.
    """
    rdate_str = RDATE
    lines = []

    # '1' triggers new page (page eject before first data line on each page)
    lines.append(f"1 PUBLIC BANK BERHAD - RETAIL BANKING DIVISION")
    lines.append(f"  REPORT TITLE : EIBDLN1M")
    lines.append(f"  DAILY MOVEMENT IN BANK'S LOANS/OD ACCOUNTS @ {rdate_str}")
    lines.append(f"  NET INCREASED/(DECREASED) OF RM1 MILLION & ABOVE PER CUSTOMER")
    lines.append(f"  *")
    lines.append(f"  {category:<16s}")
    lines.append(f"  {'-' * 131}")
    lines.append(f"  BRH  BRH")
    lines.append(
        f"  CODE ABBR CUSTOMER NAME"
        + " " * 30
        + "ACCOUNT NO.   APPROVE LIMIT  CURRENT BALANCE"
        + "   PREVIOUS BAL    NET (INC/DEC)"
    )
    lines.append(f"  {'-' * 131}")
    return lines          # 10 lines


rows_iter   = final.iter_rows(named=True)
lines_on_page  = 0
current_cat    = None
first_data_row = True   # for '0' ASA on very first detail line per category

output_lines: list[str] = []

for row in rows_iter:
    category = row["CATEGORY"] or ""

    # New category → SAS PUT _PAGE_  ⟹  header with '1' prefix
    if category != current_cat:
        header_block = _build_header(category)
        # First line carries the '1' (page eject), rest carry ' '
        output_lines.append(header_block[0])          # '1...'
        for hl in header_block[1:]:
            output_lines.append(" " + hl[1:] if hl.startswith(" ") else " " + hl)
        current_cat    = category
        lines_on_page  = HEADER_LINES
        first_data_row = True

    # Mid-page overflow (would exceed PAGE_SIZE → new header)
    elif lines_on_page >= PAGE_SIZE:
        header_block = _build_header(category)
        output_lines.append(header_block[0])
        for hl in header_block[1:]:
            output_lines.append(" " + hl[1:] if hl.startswith(" ") else " " + hl)
        lines_on_page  = HEADER_LINES
        first_data_row = True

    # ── detail line ──────────────────────────────────────────────────────────
    # SAS PUT @002 BRANCH Z3. @007 BRNAME $3. @012 CUSTNAME $40. ...
    # Columns are 1-based in SAS; we build the full 133-char buffer.
    buf = [" "] * 133     # position 1 = index 0 (ASA char)

    asa = "0" if first_data_row else " "
    buf[0] = asa
    first_data_row = False

    # @002 BRANCH Z3.  (positions 2-4, 0-based index 1-3)
    branch_str = f"{int(row['BRANCH'] or 0):03d}"
    buf[1:4] = list(branch_str)

    # @007 BRNAME $3.  (positions 7-9, 0-based 6-8)
    brname_str = str(row.get("BRNAME") or "")[:3]
    buf[6:9] = list(f"{brname_str:<3s}")

    # @012 CUSTNAME $40.  (positions 12-51, 0-based 11-50)
    custname_str = str(row.get("CUSTNAME") or "")[:40]
    buf[11:51] = list(f"{custname_str:<40s}")

    # @054 ACCTNO 10.  (positions 54-63, 0-based 53-62, right-justified)
    acctno_str = f"{int(row['ACCTNO'] or 0):>10d}"
    buf[53:63] = list(acctno_str)

    # @066 LIMTBAL COMMA15.0  (positions 66-80, 0-based 65-79)
    limtbal_str = _fmt_comma(row.get("LIMTBAL"), 15, 0)
    buf[65:80] = list(limtbal_str)

    # @083 ACCBAL COMMA15.2  (positions 83-97, 0-based 82-96)
    accbal_str = _fmt_comma(row.get("ACCBAL"), 15, 2)
    buf[82:97] = list(accbal_str)

    # @100 PACCBAL COMMA15.2  (positions 100-114, 0-based 99-113)
    paccbal_str = _fmt_comma(row.get("PACCBAL"), 15, 2)
    buf[99:114] = list(paccbal_str)

    # @117 MOVEAMTS COMMA15.2  (positions 117-131, 0-based 116-130)
    moveamts_str = _fmt_comma(row.get("MOVEAMTS"), 15, 2)
    buf[116:131] = list(moveamts_str)

    output_lines.append("".join(buf))
    lines_on_page += 1

# ============================================================================
# WRITE OUTPUT
# ============================================================================
with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in output_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(output_lines):,}")

# ============================================================================
# STEP 12: CLEAN UP CACHE  (free disk space; re-created on next run if stale)
# ============================================================================
# Cache files are intentionally kept across runs so that a second execution
# on the same day skips the expensive SAS→Parquet conversion step.
# Remove them manually or let the freshness check handle eviction.

del final
gc.collect()

print("\nEIBDLN1M complete.")
