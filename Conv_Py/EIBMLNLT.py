#!/usr/bin/env python3
"""
Program : EIBMLNLT.py
Purpose : Generate Loan Listing Reports for PBB (Public Bank Berhad) and PIBB
          (Public Islamic Bank Berhad)
          - Report 1: Loan Listing by FISS Purpose Code (all custcodes)
          - Report 2: Loan Listing by Construction (Sector 5001-5999) and Real
            ESTATE (8310) for Non-Individual Customers
          Outputs text data file (LNLISD/LNLISX) and RPS report file
          (LNLIST/LNLISR)
"""

import os
import gc
import math
from pathlib import Path
from datetime import date

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values
from input_date import get_latest_file
# from output_date import build_output_file
# NOTE: build_output_file() is NOT used in this program. The original SAS
# DSNs (SAP.BANK.LOANLIST.TEXT/RPS, SAP.PIBB.LOANLIST.TEXT/RPS) carry no
# date component in their fileNAMEs, so all output files here use fixed
# static fileNAMEs instead.

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

# ------------------------------------------------------------------
# Input directories / files
# ------------------------------------------------------------------
# LOAN carries a date suffix in its fileNAME (format mmwyy, e.g. mm=month,
# w=week digit derived from SAS SELECT(DAY(REPTDATE)), yy=2-digit year) ->
# resolved via input_date.get_latest_file(). PIBB fileNAMEs use the "i"
# prefix convention (ln -> iln).
INPUT_DIR      = BASE_DIR / "input" / "prod"
PBB_LOAN_DIR   = INPUT_DIR / "loan"
PIBB_LOAN_DIR  = INPUT_DIR / "loan"

# LNNOTE / LNCOMM carry no date component in their fileNAMEs -> static paths
PBB_LNNOTE_FILE  = STG_DIR / "PBB_lnnote.sas7bdat"
PBB_LNCOMM_FILE  = STG_DIR / "PBB_lncomm.sas7bdat"
PIBB_LNNOTE_FILE = STG_DIR / "PIBB_lnnote.sas7bdat"
PIBB_LNCOMM_FILE = STG_DIR / "PIBB_lncomm.sas7bdat"

# ------------------------------------------------------------------
# Parquet cache directories (chunked SAS -> Parquet, freshness-checked)
# ------------------------------------------------------------------
CACHE_DIR      = BASE_DIR / "input" / "cache" / "EIBMLNLT"
PBB_CACHE_DIR  = CACHE_DIR / "PBB"
PIBB_CACHE_DIR = CACHE_DIR / "PIBB"

# ------------------------------------------------------------------
# Output directories / files (fixed fileNAMEs -- no date suffix)
# ------------------------------------------------------------------
OUTPUT_DIR      = BASE_DIR / "output" / "EIBMLNLT"
PBB_OUTPUT_DIR  = OUTPUT_DIR / "PBB"
PIBB_OUTPUT_DIR = OUTPUT_DIR / "PIBB"

for _d in (PBB_LOAN_DIR, PIBB_LOAN_DIR, PBB_CACHE_DIR, PIBB_CACHE_DIR,
           PBB_OUTPUT_DIR, PIBB_OUTPUT_DIR):
    _d.mkdir(parents=True, exist_ok=True)

PBB_LNLISD_TXT = PBB_OUTPUT_DIR / "loanlist_text.txt"   # SAP.BANK.LOANLIST.TEXT (80 char)
PBB_LNLIST_TXT = PBB_OUTPUT_DIR / "loanlist_rps.txt"    # SAP.BANK.LOANLIST.RPS  (136 char)

PIBB_LNLISX_TXT = PIBB_OUTPUT_DIR / "loanlist_text.txt"   # SAP.PIBB.LOANLIST.TEXT (80 char)
PIBB_LNLISR_TXT = PIBB_OUTPUT_DIR / "loanlist_rps.txt"    # SAP.PIBB.LOANLIST.RPS  (136 char)

# ============================================================================
# CHUNK SIZE FOR STREAMING LARGE .sas7bdat FILES
# ============================================================================
CHUNK_ROWS = 500_000
ROW_LIMIT  = int(os.environ.get("ROW_LIMIT", 0))   # 0 = no limit (test mode via env)

# =============================================================================
# FORMAT HELPERS
# =============================================================================

BANK_FMT = {33: 'PBB', 134: 'PFB'}
REPORT_LINE_PREFIX = 'P001'
AMOUNT_SINGLE_SEPARATOR = '---------------'
AMOUNT_DOUBLE_SEPARATOR = '==============='
PAGE_LINE_THRESHOLD = 55


def is_nan_float(value):
    """Return True only for float NaN values."""
    return isinstance(value, float) and math.isnan(value)

def fmt_BANKNO(BANKNO):
    """PROC FORMAT BANKFMT"""
    return BANK_FMT.get(BANKNO, str(BANKNO) if BANKNO is not None else '')

def fmt_comma15_2(val):
    """Format numeric as COMMA15.2 right-justified in 15 chars"""
    if val is None or is_nan_float(val):
        return ' ' * 15
    formatted = f"{val:,.2f}"
    return formatted.rjust(15)

def fmt_comma5_2(val):
    """Format numeric as COMMA5.2 right-justified in 5 chars"""
    if val is None or is_nan_float(val):
        return ' ' * 5
    formatted = f"{val:,.2f}"
    return formatted.rjust(5)

def fmt_z3(val):
    """Format numeric as Z3. (zero-padded 3 digits)"""
    if val is None:
        return '000'
    return str(int(val)).zfill(3)

def place_at(line_list, col, text):
    """
    Place text into line_list (1-indexed column).
    line_list is a mutable list of characters.
    """
    col0 = col - 1
    for i, ch in enumerate(text):
        pos = col0 + i
        if pos < len(line_list):
            line_list[pos] = ch
        else:
            while len(line_list) <= pos:
                line_list.append(' ')
            line_list[pos] = ch

def make_line(width=136):
    return [' '] * width

def finalize_line(line_list, width=136):
    s = ''.join(line_list)
    return s.ljust(width)[:width]

# =============================================================================
# REPORT DATE DERIVATION  (no reptdate.parquet -- derive from REPTDATE.py)
# =============================================================================

def _compute_nowk(reptdate_val: date) -> str:
    """
    Replicates SAS: SELECT(DAY(REPTDATE));
        WHEN (8)  DO; WK = '1'; END;
        WHEN(15)  DO; WK = '2'; END;
        WHEN(22)  DO; WK = '3'; END;
        OTHERWISE DO; WK = '4'; END;
    END;
    This exact day-selection logic (not REPTDATE.py's own range-based NOWK)
    is preserved deliberately, since this program's run schedule is keyed to
    those specific SAS SELECT BRANCHes.
    """
    day = reptdate_val.day
    if day == 8:
        return '1'
    if day == 15:
        return '2'
    if day == 22:
        return '3'
    return '4'


def get_report_vars():
    """
    Derive NOWK, RDATE, REPTMON, REPTYEAR using REPTDATE.py's report date
    instead of reading a (non-existent) reptdate.parquet.
    """
    reptdate_values = get_reptdate_values()
    reptdate = reptdate_values.reptdate

    wk       = _compute_nowk(reptdate)
    rdate    = reptdate.strftime('%d/%m/%y')
    reptmon  = reptdate.strftime('%m')
    reptyear = reptdate.strftime('%Y')

    return wk, rdate, reptmon, reptyear

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
    print(f"  [{tag}] Converting {sas_path.NAME} -> {cache_path.NAME} ...")
    writer = None
    schema = None
    total = 0
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
                col = table.column(field.NAME)
                if col.type != field.type:
                    try:
                        col = col.cast(field.type, safe=False)
                    except Exception as e:
                        print(f"  [{tag}] WARNING: Cannot cast '{field.NAME}' "
                              f"from {col.type} to {field.type}: {e} - filling nulls")
                        col = pa.nulls(len(col), type=field.type)
                cast_arrays.append(col)
            table = pa.Table.from_arrays(cast_arrays, schema=schema)

        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer:
        writer.close()
    print(f"  [{tag}] Done - {total:,} rows cached.")


def ensure_parquet_cache(sas_path: Path, cache_path: Path, tag: str) -> None:
    """Convert sas_path -> cache_path only when the cache is stale/missing."""
    if not _cache_is_fresh(sas_path, cache_path):
        sas_to_parquet(sas_path, cache_path, tag)
    else:
        print(f"  [{tag}] Cache fresh - skipping conversion.")

# =============================================================================
# DATA PREPARATION (shared logic for both PBB and PIBB)
# =============================================================================

def cache_inputs_to_parquet(loan_dir: Path, loan_prefix: str, lnnote_file: Path,
                             lncomm_file: Path, cache_dir: Path, tag: str):
    """
    STAGE 1 — Resolve the latest LOAN file and convert all three SAS inputs
    (LOAN / LNNOTE / LNCOMM) to Parquet, with freshness checks, before any
    report data is built. No merging/sorting happens here.
    Returns: wk, rdate, reptmon, reptyear, loan_cache, lnnote_cache, lncomm_cache
    """
    wk, rdate, reptmon, reptyear = get_report_vars()

    print(f"\n[{tag}] Report date derived: {rdate} (REPTMON={reptmon}, NOWK={wk})")

    print(f"[{tag}] Resolving latest LOAN file (suffix format mmwyy)...")
    loan_path = get_latest_file(loan_dir, prefix=loan_prefix)
    print(f"  [{tag}] LOAN file selected: {loan_path.name}")

    loan_cache   = cache_dir / f"{loan_path.stem}.parquet"
    lnnote_cache = cache_dir / "lnnote.parquet"
    lncomm_cache = cache_dir / "lncomm.parquet"

    print(f"[{tag}] Caching SAS inputs to Parquet (if needed)...")
    ensure_parquet_cache(loan_path, loan_cache, f"{tag}-LOAN")
    ensure_parquet_cache(lnnote_file, lnnote_cache, f"{tag}-LNNOTE")
    ensure_parquet_cache(lncomm_file, lncomm_cache, f"{tag}-LNCOMM")

    return wk, rdate, reptmon, reptyear, loan_cache, lnnote_cache, lncomm_cache


def build_lnnote_datasets(loan_cache: Path, lnnote_cache: Path,
                           lncomm_cache: Path, tag: str):
    """
    STAGE 2 — Read the already-cached Parquet files and replicate:
      4. PROC SORT LNNOTE (KEEP=ACCTNO NOTENO BANKNO STATE) BY ACCTNO NOTENO
      5. PROC SORT LOAN BY ACCTNO NOTENO
      6. MERGE LOAN + LNNOTE by ACCTNO NOTENO, keep IF ACCTYPE='LN' -> LNOTE
      7. PROC SORT LNOTE BY ACCTNO COMMNO
      8. PROC SORT LNCOMM BY ACCTNO COMMNO
      9. MERGE LNOTE + LNCOMM by ACCTNO COMMNO, keep if A -> NOTE1
     10. NOTE2 = NOTE1 filtered by CUSTCD and SECTORCD rules
     11. PROC SORT NOTE1 -> LNNOTE1 BY BRANCH FISSPURP CUSTCD ACCTNO
     12. PROC SORT NOTE2 -> LNNOTE2 BY BRANCH SECTORCD CUSTCD ACCTNO
    Returns: lnnote1 (polars df), lnnote2 (polars df)
    """
    con = duckdb.connect()

    # LNNOTE: KEEP=ACCTNO NOTENO BANKNO STATE, sorted BY ACCTNO NOTENO
    lnnote_df = con.execute(f"""
        SELECT ACCTNO, NOTENO, BANKNO, STATE
        FROM read_parquet('{lnnote_cache}')
        ORDER BY ACCTNO, NOTENO
    """).pl()

    # LOAN sorted BY ACCTNO NOTENO
    loan_df = con.execute(f"""
        SELECT *
        FROM read_parquet('{loan_cache}')
        ORDER BY ACCTNO, NOTENO
    """).pl()

    con.close()

    # MERGE LOAN + LNNOTE by ACCTNO NOTENO, IF ACCTYPE='LN'
    merged = loan_df.join(lnnote_df, on=['ACCTNO', 'NOTENO'], how='left', suffix='_note')

    if 'BANKNO_note' in merged.columns:
        merged = merged.with_columns([
            pl.when(pl.col('BANKNO_note').is_not_null()).then(pl.col('BANKNO_note')).otherwise(pl.col('BANKNO')).alias('BANKNO')
        ]).drop('BANKNO_note')
    if 'STATE_note' in merged.columns:
        merged = merged.with_columns([
            pl.when(pl.col('STATE_note').is_not_null()).then(pl.col('STATE_note')).otherwise(pl.col('STATE')).alias('STATE')
        ]).drop('STATE_note')

    lnote = merged.filter(pl.col('ACCTYPE') == 'LN').select([
        'BANKNO', 'BRANCH', 'ACCTNO', 'NOTENO', 'NAME', 'BALANCE',
        'SECTORCD', 'CUSTCD', 'INTRATE', 'NTBRCH', 'COMMNO', 'LIABCODE',
        'APPRLIMT', 'FISSPURP', 'STATE'
    ]).sort(['ACCTNO', 'COMMNO'])

    del merged, loan_df, lnnote_df
    gc.collect()

    con2 = duckdb.connect()
    lncomm = con2.execute(f"""
        SELECT * FROM read_parquet('{lncomm_cache}')
        ORDER BY ACCTNO, COMMNO
    """).pl()
    con2.close()

    note1 = lnote.join(lncomm, on=['ACCTNO', 'COMMNO'], how='left', suffix='_comm')

    for col in ['BANKNO', 'BRANCH', 'NOTENO', 'NAME', 'BALANCE', 'SECTORCD', 'CUSTCD',
                'INTRATE', 'NTBRCH', 'LIABCODE', 'APPRLIMT', 'FISSPURP', 'STATE']:
        comm_col = f"{col}_comm"
        if comm_col in note1.columns:
            note1 = note1.with_columns(
                pl.when(pl.col(comm_col).is_not_null()).then(pl.col(comm_col)).otherwise(pl.col(col)).alias(col)
            ).drop(comm_col)

    note1_cols = ['BANKNO', 'BRANCH', 'ACCTNO', 'NOTENO', 'NAME', 'APPRLIMT', 'BALANCE',
                  'SECTORCD', 'CUSTCD', 'STATE', 'INTRATE', 'NTBRCH', 'COMMNO', 'LIABCODE',
                  'CCOLLTRL', 'FISSPURP']
    note1 = note1.select([c for c in note1_cols if c in note1.columns])

    del lnote, lncomm
    gc.collect()

    note2 = note1.filter(
        (~pl.col('CUSTCD').cast(pl.Utf8).is_in(['77', '78', '95', '96'])) &
        (
            pl.col('SECTORCD').cast(pl.Utf8).str.starts_with('5') |
            (pl.col('SECTORCD').cast(pl.Utf8) == '8310')
        )
    )

    lnnote1 = note1.sort(['BRANCH', 'FISSPURP', 'CUSTCD', 'ACCTNO'])
    lnnote2 = note2.sort(['BRANCH', 'SECTORCD', 'CUSTCD', 'ACCTNO'])

    del note1, note2
    gc.collect()

    return lnnote1, lnnote2

# =============================================================================
# WRITE LNLISD / LNLISX  (text data file, 80-char fixed)
# =============================================================================

def write_lnlisd(lnnote1: pl.DataFrame, output_path: Path):
    """
    Write the text data file equivalent to FILE LNLISD/LNLISX.
    Only rows where BALANCE>0 OR APPRLIMT>0.
    PUT @001 BRANCH 3. @004 ACCTNO 10. @014 NOTENO 5.
        @020 APPRLIMT COMMA15.2 @040 BALANCE COMMA15.2
    Fixed record length 80.
    """
    filtered = lnnote1.filter(
        (pl.col('BALANCE') > 0) | (pl.col('APPRLIMT') > 0)
    )

    lines = []
    for row in filtered.iter_rows(NAMEd=True):
        line = [' '] * 80

        BRANCH = str(int(row['BRANCH'])).rjust(3)[:3] if row['BRANCH'] is not None else '   '
        ACCTNO = str(int(row['ACCTNO'])).rjust(10)[:10] if row['ACCTNO'] is not None else ' ' * 10
        NOTENO = str(int(row['NOTENO'])).rjust(5)[:5] if row['NOTENO'] is not None else '     '

        bal  = row['BALANCE'] if row['BALANCE'] is not None else 0.0
        appr = row['APPRLIMT'] if row['APPRLIMT'] is not None else 0.0

        APPRLIMT_str = fmt_comma15_2(appr)
        BALANCE_str  = fmt_comma15_2(bal)

        place_at(line, 1, BRANCH)
        place_at(line, 4, ACCTNO)
        place_at(line, 14, NOTENO)
        place_at(line, 20, APPRLIMT_str)
        place_at(line, 40, BALANCE_str)

        lines.append(finalize_line(line, 80))

    with open(output_path, 'w', encoding='ascii', errors='replace') as f:
        for ln in lines:
            f.write(ln + '\n')

# =============================================================================
# RPS REPORT GENERATION HELPERS
# =============================================================================

def make_brno(BRANCH):
    """Compute BRNO from BRANCH number (matching SAS SELECT logic)."""
    b = int(BRANCH)
    if b < 10:
        return f"BR0{b}"
    if b < 100:
        return f"BR{b}"
    return f"B{b}"

def write_newpage_fiss(f, pagecnt, BRANCH, BANKNO, rdate, is_first_BRANCH, pbb_title, line_width=136):
    """Write a new page header for FISS Purpose report. Returns updated pagecnt."""
    pagecnt += 1

    line = make_line(line_width)
    place_at(line, 1, 'E255')
    f.write(finalize_line(line, line_width) + '\n')

    if is_first_BRANCH:
        brno = make_brno(BRANCH)
        line = make_line(line_width)
        place_at(line, 1, 'P000PBBEDPPBBEDP')
        place_at(line, 133, brno)
        f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1, 'P000REPORT NO :  LOANLIST')
    place_at(line, 44, pbb_title)
    place_at(line, 122, f'PAGE NO : {pagecnt}')
    f.write(finalize_line(line, line_width) + '\n')

    BANKNO_fmt = fmt_BANKNO(BANKNO)
    BRANCH_z3  = fmt_z3(BRANCH)
    line = make_line(line_width)
    place_at(line, 1, 'P001BRANCH    :  ')
    place_at(line, 18, BANKNO_fmt)
    place_at(line, 22, BRANCH_z3)
    place_at(line, 31, f'LOAN LISTING BY FISS PURPOSE CODE (FOR ALL CUSTCODES) {rdate}')
    f.write(finalize_line(line, line_width) + '\n')

    for _ in range(2):
        line = make_line(line_width)
        place_at(line, 1, REPORT_LINE_PREFIX)
        f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    place_at(line, 62, 'APPROVED')
    place_at(line, 76, 'OUTSTANDING')
    place_at(line, 88, 'FISS PURPOSE')
    place_at(line, 101, 'SECTOR')
    place_at(line, 108, 'CUST')
    place_at(line, 114, 'STATE')
    place_at(line, 120, 'INT')
    place_at(line, 126, 'COLL')
    place_at(line, 131, 'COLL')
    f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    place_at(line, 10, 'A/C NO.')
    place_at(line, 19, 'NOTE NO.')
    place_at(line, 29, 'NAME OF CUSTOMER')
    place_at(line, 65, 'LIMIT')
    place_at(line, 80, 'BALANCE')
    place_at(line, 90, 'CODE')
    place_at(line, 101, 'CODE')
    place_at(line, 108, 'CODE')
    place_at(line, 114, 'CODE')
    place_at(line, 120, 'RATE')
    place_at(line, 126, 'NOTE')
    place_at(line, 131, 'COMM')
    f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    place_at(line, 5, '-' * 40)
    place_at(line, 45, '-' * 40)
    place_at(line, 85, '-' * 40)
    place_at(line, 125, '-' * 10)
    f.write(finalize_line(line, line_width) + '\n')

    for _ in range(2):
        line = make_line(line_width)
        place_at(line, 1, REPORT_LINE_PREFIX)
        f.write(finalize_line(line, line_width) + '\n')

    return pagecnt

def write_newpage_sector(f, pagecnt, BRANCH, BANKNO, rdate, is_first_BRANCH, pbb_title, line_width=136):
    """Write a new page header for Sector/Construction report. Returns updated pagecnt."""
    pagecnt += 1

    line = make_line(line_width)
    place_at(line, 1, 'E255')
    f.write(finalize_line(line, line_width) + '\n')

    if is_first_BRANCH:
        brno = make_brno(BRANCH)
        line = make_line(line_width)
        place_at(line, 1, 'P000PBBEDPPBBEDP')
        place_at(line, 133, brno)
        f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1, 'P000REPORT NO :  LOANLIST')
    place_at(line, 44, pbb_title)
    place_at(line, 122, f'PAGE NO : {pagecnt}')
    f.write(finalize_line(line, line_width) + '\n')

    BANKNO_fmt = fmt_BANKNO(BANKNO)
    BRANCH_z3  = fmt_z3(BRANCH)
    line = make_line(line_width)
    place_at(line, 1, 'P001BRANCH    :  ')
    place_at(line, 18, BANKNO_fmt)
    place_at(line, 22, BRANCH_z3)
    place_at(line, 36, 'LOAN LISTING BY CONSTRUCTION (SECTCODE 5001-5999) AND')
    f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 30, f'REAL ESTATE (SECTCODE 8310) FOR NON-INDI. CUSTOMER FOR {rdate}')
    f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    place_at(line, 62, 'APPROVED')
    place_at(line, 76, 'OUTSTANDING')
    place_at(line, 88, 'FISS PURPOSE')
    place_at(line, 101, 'SECTOR')
    place_at(line, 108, 'CUST')
    place_at(line, 114, 'STATE')
    place_at(line, 120, 'INT')
    place_at(line, 126, 'COLL')
    place_at(line, 131, 'COLL')
    f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    place_at(line, 10, 'A/C NO.')
    place_at(line, 19, 'NOTE NO.')
    place_at(line, 29, 'NAME OF CUSTOMER')
    place_at(line, 65, 'LIMIT')
    place_at(line, 80, 'BALANCE')
    place_at(line, 90, 'CODE')
    place_at(line, 101, 'CODE')
    place_at(line, 108, 'CODE')
    place_at(line, 114, 'CODE')
    place_at(line, 120, 'RATE')
    place_at(line, 126, 'NOTE')
    place_at(line, 131, 'COMM')
    f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    place_at(line, 5, '-' * 40)
    place_at(line, 45, '-' * 40)
    place_at(line, 85, '-' * 40)
    place_at(line, 125, '-' * 10)
    f.write(finalize_line(line, line_width) + '\n')

    for _ in range(2):
        line = make_line(line_width)
        place_at(line, 1, REPORT_LINE_PREFIX)
        f.write(finalize_line(line, line_width) + '\n')

    return pagecnt

def write_data_row(f, row, line_width=136):
    """Write a single data detail row to the RPS report file."""
    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)

    ACCTNO = str(int(row['ACCTNO'])).rjust(12) if row['ACCTNO'] is not None else ' ' * 12
    NOTENO = str(int(row['NOTENO'])).rjust(8) if row['NOTENO'] is not None else ' ' * 8
    NAME   = str(row['NAME'])[:26] if row['NAME'] is not None else ''

    bal  = row['BALANCE']  if row['BALANCE']  is not None else 0.0
    appr = row['APPRLIMT'] if row['APPRLIMT'] is not None else 0.0

    FISSPURP = str(row['FISSPURP'])[:4].rjust(4) if row['FISSPURP'] is not None else '    '
    SECTORCD = str(row['SECTORCD'])[:4].rjust(4) if row['SECTORCD'] is not None else '    '
    CUSTCD   = str(row['CUSTCD']).rjust(4)[:4]   if row['CUSTCD']   is not None else '    '
    STATE    = str(row['STATE'])[:3].rjust(3)    if row['STATE']    is not None else '   '
    LIABCODE = str(row['LIABCODE']) if row['LIABCODE'] is not None else ''
    CCOLLTRL = str(row['CCOLLTRL']) if row['CCOLLTRL'] is not None else ''

    place_at(line, 5, ACCTNO)
    place_at(line, 19, NOTENO)
    place_at(line, 29, NAME)
    place_at(line, 55, fmt_comma15_2(appr))
    place_at(line, 72, fmt_comma15_2(bal))
    place_at(line, 90, FISSPURP)
    place_at(line, 101, SECTORCD)
    place_at(line, 108, CUSTCD)
    place_at(line, 114, STATE)
    place_at(line, 120, fmt_comma5_2(row['INTRATE']))
    place_at(line, 126, LIABCODE)
    place_at(line, 131, CCOLLTRL)

    f.write(finalize_line(line, line_width) + '\n')

def write_blank_p001(f, line_width=136):
    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    f.write(finalize_line(line, line_width) + '\n')

def write_subtotal_fiss(f, FISSPURP, bnmamt, line_width=136):
    """Write subtotal lines for FISS Purpose."""
    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    place_at(line, 72, AMOUNT_SINGLE_SEPARATOR)
    f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    fisp_str = str(FISSPURP) if FISSPURP is not None else ''
    place_at(line, 37, f'SUBTOTAL FOR FISS PURPOSE {fisp_str}')
    place_at(line, 72, fmt_comma15_2(bnmamt))
    f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    place_at(line, 72, AMOUNT_DOUBLE_SEPARATOR)
    f.write(finalize_line(line, line_width) + '\n')

    write_blank_p001(f, line_width)

def write_subtotal_sector(f, SECTORCD, bnmamt, line_width=136):
    """Write subtotal lines for Sector Code."""
    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    place_at(line, 72, AMOUNT_SINGLE_SEPARATOR)
    f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    sect_str = str(SECTORCD) if SECTORCD is not None else ''
    place_at(line, 37, f'SUBTOTAL FOR SECTOR {sect_str}')
    place_at(line, 72, fmt_comma15_2(bnmamt))
    f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    place_at(line, 72, AMOUNT_DOUBLE_SEPARATOR)
    f.write(finalize_line(line, line_width) + '\n')

    write_blank_p001(f, line_width)

def write_grand_total(f, brchamt, line_width=136):
    """Write grand total lines for BRANCH."""
    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    place_at(line, 72, AMOUNT_SINGLE_SEPARATOR)
    f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    place_at(line, 37, 'GRAND TOTAL FOR BRANCH')
    place_at(line, 72, fmt_comma15_2(brchamt))
    f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    place_at(line, 72, AMOUNT_DOUBLE_SEPARATOR)
    f.write(finalize_line(line, line_width) + '\n')

    write_blank_p001(f, line_width)

# =============================================================================
# GROUPED REPORT WRITER — DECOMPOSED HELPERS
# (Kept flat / delegated to module-level helpers to satisfy SonarQube
#  Cognitive Complexity <= 15.)
# =============================================================================

def _derive_group_status(current_row, next_row, previous_BRANCH, previous_group, group_field):
    """Determine boundary flags for the current row relative to its neighbours."""
    BRANCH      = current_row['BRANCH']
    group_value = current_row[group_field]

    is_first_BRANCH = BRANCH != previous_BRANCH
    is_first_group  = is_first_BRANCH or (group_value != previous_group)
    is_last_BRANCH  = (next_row is None) or (next_row['BRANCH'] != BRANCH)
    is_last_group   = (
        (next_row is None)
        or (next_row[group_field] != group_value)
        or (next_row['BRANCH'] != BRANCH)
    )
    return BRANCH, group_value, is_first_BRANCH, is_first_group, is_last_BRANCH, is_last_group


def _start_new_page(f, pagecnt, BRANCH, BANKNO, rdate, is_first_page,
                    pbb_title, line_width, newpage_writer):
    """Call the appropriate newpage_writer and return updated (pagecnt, linecnt)."""
    pagecnt = newpage_writer(
        f, pagecnt, BRANCH, BANKNO, rdate, is_first_page, pbb_title, line_width
    )
    return pagecnt, 11


def _handle_BRANCH_open(f, BRANCH, BANKNO, rdate,
                        pbb_title, line_width, newpage_writer):
    """
    Actions taken when entering a new BRANCH: reset page/BRANCH accumulator,
    emit the first page header for this BRANCH. Returns (pagecnt, linecnt, brchamt).
    """
    brchamt = 0.0
    pagecnt, linecnt = _start_new_page(
        f, 0, BRANCH, BANKNO, rdate, True,
        pbb_title, line_width, newpage_writer
    )
    return pagecnt, linecnt, brchamt


def _handle_page_overflow(f, pagecnt, linecnt, BRANCH, BANKNO, rdate,
                          pbb_title, line_width, newpage_writer):
    """Emit a new page header when the current page is full."""
    if linecnt > PAGE_LINE_THRESHOLD:
        pagecnt, linecnt = _start_new_page(
            f, pagecnt, BRANCH, BANKNO, rdate, False,
            pbb_title, line_width, newpage_writer
        )
    return pagecnt, linecnt


def _handle_group_close(f, pagecnt, linecnt, group_value, bnmamt,
                        BRANCH, BANKNO, rdate, pbb_title, line_width,
                        newpage_writer, subtotal_writer):
    """Emit the group subtotal (4 lines), then check for page overflow."""
    subtotal_writer(f, group_value, bnmamt, line_width)
    linecnt += 4
    pagecnt, linecnt = _handle_page_overflow(
        f, pagecnt, linecnt, BRANCH, BANKNO, rdate,
        pbb_title, line_width, newpage_writer
    )
    return pagecnt, linecnt


def _handle_BRANCH_close(f, pagecnt, linecnt, brchamt,
                         BRANCH, BANKNO, rdate, pbb_title, line_width,
                         newpage_writer):
    """Emit the BRANCH grand total (4 lines), then check for page overflow."""
    write_grand_total(f, brchamt, line_width)
    linecnt += 4
    pagecnt, linecnt = _handle_page_overflow(
        f, pagecnt, linecnt, BRANCH, BANKNO, rdate,
        pbb_title, line_width, newpage_writer
    )
    return pagecnt, linecnt

# =============================================================================
# WRITE RPS REPORT — SHARED GROUPED WRITER
# =============================================================================

def _write_lnlist_grouped(rows, rdate, output_path, pbb_title, mode, line_width,
                          group_field, newpage_writer, subtotal_writer):
    """Shared writer for grouped RPS output (FISS purpose / Sector code variants)."""
    n           = len(rows)
    linecnt     = 0
    pagecnt     = 0
    brchamt     = 0.0
    bnmamt      = 0.0
    prev_BRANCH = None
    prev_group  = None

    with open(output_path, mode, encoding='ascii', errors='replace') as f:
        for idx, row in enumerate(rows):
            BALANCE  = row['BALANCE'] if row['BALANCE'] is not None else 0.0
            next_row = rows[idx + 1] if idx + 1 < n else None
            BANKNO   = row['BANKNO']

            (
                BRANCH,
                group_value,
                is_first_BRANCH,
                is_first_group,
                is_last_BRANCH,
                is_last_group,
            ) = _derive_group_status(row, next_row, prev_BRANCH, prev_group, group_field)

            if is_first_BRANCH:
                pagecnt, linecnt, brchamt = _handle_BRANCH_open(
                    f, BRANCH, BANKNO, rdate,
                    pbb_title, line_width, newpage_writer
                )

            if is_first_group:
                bnmamt = 0.0

            brchamt += BALANCE
            bnmamt  += BALANCE

            write_data_row(f, row, line_width)
            linecnt += 1

            pagecnt, linecnt = _handle_page_overflow(
                f, pagecnt, linecnt, BRANCH, BANKNO, rdate,
                pbb_title, line_width, newpage_writer
            )

            if is_last_group:
                pagecnt, linecnt = _handle_group_close(
                    f, pagecnt, linecnt, group_value, bnmamt,
                    BRANCH, BANKNO, rdate, pbb_title, line_width,
                    newpage_writer, subtotal_writer
                )

            if is_last_BRANCH:
                pagecnt, linecnt = _handle_BRANCH_close(
                    f, pagecnt, linecnt, brchamt,
                    BRANCH, BANKNO, rdate, pbb_title, line_width,
                    newpage_writer
                )

            prev_BRANCH = BRANCH
            prev_group  = group_value

# =============================================================================
# WRITE RPS REPORT - FISS PURPOSE (LNNOTE1)
# =============================================================================

def write_lnlist_fiss(lnnote1: pl.DataFrame, rdate: str, output_path: Path,
                      pbb_title: str, mode: str = 'w', line_width: int = 136):
    """
    Write LNLIST RPS report grouped by BRANCH / FISSPURP / CUSTCD / ACCTNO.
    Equivalent to the first DATA _NULL_ block.
    """
    _write_lnlist_grouped(
        rows=lnnote1.to_dicts(),
        rdate=rdate,
        output_path=output_path,
        pbb_title=pbb_title,
        mode=mode,
        line_width=line_width,
        group_field='FISSPURP',
        newpage_writer=write_newpage_fiss,
        subtotal_writer=write_subtotal_fiss
    )

# =============================================================================
# WRITE RPS REPORT - SECTOR CODE (LNNOTE2)
# =============================================================================

def write_lnlist_sector(lnnote2: pl.DataFrame, rdate: str, output_path: Path,
                        pbb_title: str, mode: str = 'a', line_width: int = 136):
    """
    Write LNLIST RPS report grouped by BRANCH / SECTORCD / CUSTCD / ACCTNO.
    Equivalent to the second DATA _NULL_ block (FILE LNLIST MOD).
    """
    _write_lnlist_grouped(
        rows=lnnote2.to_dicts(),
        rdate=rdate,
        output_path=output_path,
        pbb_title=pbb_title,
        mode=mode,
        line_width=line_width,
        group_field='SECTORCD',
        newpage_writer=write_newpage_sector,
        subtotal_writer=write_subtotal_sector
    )

# =============================================================================
# MAIN: PBB SECTION
# =============================================================================

# def run_pbb():
#     """
#     Process PBB (Public Bank Berhad) section.
#     Equivalent to the first EIBMLNLT SAS execution block.
#     """
#     print("Processing PBB section...")

#     _, rdate, _, _, lnnote1, lnnote2 = prepare_data(
#         PBB_LOAN_DIR, "ln",
#         PBB_LNNOTE_FILE, PBB_LNCOMM_FILE,
#         PBB_CACHE_DIR, "PBB"
#     )

#     write_lnlisd(lnnote1, PBB_LNLISD_TXT)
#     print(f"  Written: {PBB_LNLISD_TXT}")

#     pbb_title = 'P U B L I C   B A N K   B E R H A D'
#     write_lnlist_fiss(lnnote1, rdate, PBB_LNLIST_TXT, pbb_title, mode='w')
#     print(f"  Written FISS section: {PBB_LNLIST_TXT}")

#     write_lnlist_sector(lnnote2, rdate, PBB_LNLIST_TXT, pbb_title, mode='a')
#     print(f"  Appended Sector section: {PBB_LNLIST_TXT}")

#     # PROC DATASETS LIB=WORK NOLIST; DELETE NOTE1 LNNOTE1; (in-memory, no-op in Python)

#     # //STEP01 EXEC PGM=IEFBR14 - Delete RBP2.B033.LOANLIS*.RPS datasets (no-op in Python)
#     # //STEP02 EXEC PGM=SPLIB136 - Insert control characters for RPS, split by region (no-op in Python)
#     # //INFIL1 DD DSN=RMDS.OPC.BANKCODE(KLREGION) - KL Region bank codes (no-op in Python)
#     # //INFIL2 DD DSN=RMDS.OPC.BANKCODE(BGREGION) - BG Region bank codes (no-op in Python)
#     # //INFIL3 DD DSN=RMDS.OPC.BANKCODE(JBREGION) - JB Region bank codes (no-op in Python)
#     # //INFIL4 DD DSN=RMDS.OPC.BANKCODE(SBREGION) - SB Region bank codes (no-op in Python)
#     # //INFIL5 DD DSN=RMDS.OPC.BANKCODE(PPREGION) - PP Region bank codes (no-op in Python)
#     # //INFIL6 DD DSN=RMDS.OPC.BANKCODE(TTREGION) - TT Region bank codes (no-op in Python)

#     del lnnote1, lnnote2
#     gc.collect()
#     print("PBB section complete.")

def run_pbb(rdate: str, loan_cache: Path, lnnote_cache: Path, lncomm_cache: Path):
    """
    Process PBB (Public Bank Berhad) section.
    Assumes inputs are already cached to Parquet (see cache_inputs_to_parquet()).
    """
    print("Processing PBB section...")

    lnnote1, lnnote2 = build_lnnote_datasets(loan_cache, lnnote_cache, lncomm_cache, "PBB")

    write_lnlisd(lnnote1, PBB_LNLISD_TXT)
    print(f"  Written: {PBB_LNLISD_TXT}")

    pbb_title = 'P U B L I C   B A N K   B E R H A D'
    write_lnlist_fiss(lnnote1, rdate, PBB_LNLIST_TXT, pbb_title, mode='w')
    print(f"  Written FISS section: {PBB_LNLIST_TXT}")

    write_lnlist_sector(lnnote2, rdate, PBB_LNLIST_TXT, pbb_title, mode='a')
    print(f"  Appended Sector section: {PBB_LNLIST_TXT}")

    del lnnote1, lnnote2
    gc.collect()
    print("PBB section complete.")

# =============================================================================
# MAIN: PIBB SECTION
# =============================================================================

# def run_pibb():
#     """
#     Process PIBB (Public Islamic Bank Berhad) section.
#     Equivalent to the second EIBMLNLT SAS execution block (FOR PIBB).
#     """
#     print("Processing PIBB section...")

#     _, rdate, _, _, lnnote1, lnnote2 = prepare_data(
#         PIBB_LOAN_DIR, "iln",
#         PIBB_LNNOTE_FILE, PIBB_LNCOMM_FILE,
#         PIBB_CACHE_DIR, "PIBB"
#     )

#     write_lnlisd(lnnote1, PIBB_LNLISX_TXT)
#     print(f"  Written: {PIBB_LNLISX_TXT}")

#     pibb_title = 'P U B L I C   I S L A M I C   B A N K   B E R H A D'
#     write_lnlist_fiss(lnnote1, rdate, PIBB_LNLISR_TXT, pibb_title, mode='w')
#     print(f"  Written FISS section: {PIBB_LNLISR_TXT}")

#     write_lnlist_sector(lnnote2, rdate, PIBB_LNLISR_TXT, pibb_title, mode='a')
#     print(f"  Appended Sector section: {PIBB_LNLISR_TXT}")

#     # PROC DATASETS LIB=WORK NOLIST; DELETE NOTE1 LNNOTE1; (in-memory, no-op in Python)

#     # //STEP01 EXEC PGM=IEFBR14 - Delete RBP2.B051.LOANLIS*.RPS datasets (no-op in Python)
#     # //STEP02 EXEC PGM=SPLIB136 - Insert control characters for RPS, split by region (no-op in Python)
#     # //INFIL1 DD DSN=RMDS.OPC.BANKCODE(KLREGION) - KL Region bank codes (no-op in Python)
#     # //INFIL2 DD DSN=RMDS.OPC.BANKCODE(BGREGION) - BG Region bank codes (no-op in Python)
#     # //INFIL3 DD DSN=RMDS.OPC.BANKCODE(JBREGION) - JB Region bank codes (no-op in Python)
#     # //INFIL4 DD DSN=RMDS.OPC.BANKCODE(SBREGION) - SB Region bank codes (no-op in Python)
#     # //INFIL5 DD DSN=RMDS.OPC.BANKCODE(PPREGION) - PP Region bank codes (no-op in Python)
#     # //INFIL6 DD DSN=RMDS.OPC.BANKCODE(TTREGION) - TT Region bank codes (no-op in Python)

#     del lnnote1, lnnote2
#     gc.collect()
#     print("PIBB section complete.")

def run_pibb(rdate: str, loan_cache: Path, lnnote_cache: Path, lncomm_cache: Path):
    """
    Process PIBB (Public Islamic Bank Berhad) section.
    Assumes inputs are already cached to Parquet (see cache_inputs_to_parquet()).
    """
    print("Processing PIBB section...")

    lnnote1, lnnote2 = build_lnnote_datasets(loan_cache, lnnote_cache, lncomm_cache, "PIBB")

    write_lnlisd(lnnote1, PIBB_LNLISX_TXT)
    print(f"  Written: {PIBB_LNLISX_TXT}")

    pibb_title = 'P U B L I C   I S L A M I C   B A N K   B E R H A D'
    write_lnlist_fiss(lnnote1, rdate, PIBB_LNLISR_TXT, pibb_title, mode='w')
    print(f"  Written FISS section: {PIBB_LNLISR_TXT}")

    write_lnlist_sector(lnnote2, rdate, PIBB_LNLISR_TXT, pibb_title, mode='a')
    print(f"  Appended Sector section: {PIBB_LNLISR_TXT}")

    del lnnote1, lnnote2
    gc.collect()
    print("PIBB section complete.")

# =============================================================================
# ENTRY POINT
# =============================================================================

# if __name__ == '__main__':
#     run_pbb()
#     run_pibb()

if __name__ == '__main__':
    print("=== STAGE 1: Converting all SAS inputs to Parquet cache ===")

    pbb_wk, pbb_rdate, _, _, pbb_loan_cache, pbb_lnnote_cache, pbb_lncomm_cache = \
        cache_inputs_to_parquet(
            PBB_LOAN_DIR, "ln",
            PBB_LNNOTE_FILE, PBB_LNCOMM_FILE,
            PBB_CACHE_DIR, "PBB"
        )

    pibb_wk, pibb_rdate, _, _, pibb_loan_cache, pibb_lnnote_cache, pibb_lncomm_cache = \
        cache_inputs_to_parquet(
            PIBB_LOAN_DIR, "iln",
            PIBB_LNNOTE_FILE, PIBB_LNCOMM_FILE,
            PIBB_CACHE_DIR, "PIBB"
        )

    print("\n=== STAGE 2: Building reports from cached Parquet ===")

    run_pbb(pbb_rdate, pbb_loan_cache, pbb_lnnote_cache, pbb_lncomm_cache)
    run_pibb(pibb_rdate, pibb_loan_cache, pibb_lnnote_cache, pibb_lncomm_cache)
