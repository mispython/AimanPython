#!/usr/bin/env python3
"""
Program : EIBMLNLT.py
Purpose : Generate Loan Listing Reports for PBB (Public Bank Berhad) and PIBB
          (Public Islamic Bank Berhad)
          - Report 1: Loan Listing by FISS Purpose Code (all custcodes)
          - Report 2: Loan Listing by Construction (Sector 5001-5999) and Real
            Estate (8310) for Non-Individual Customers
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
# date component in their filenames, so all output files here use fixed
# static filenames instead.

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

# ------------------------------------------------------------------
# Input directories / files
# ------------------------------------------------------------------
# LOAN carries a date suffix in its filename (format mmwyy, e.g. mm=month,
# w=week digit derived from SAS SELECT(DAY(REPTDATE)), yy=2-digit year) ->
# resolved via input_date.get_latest_file(). PIBB filenames use the "i"
# prefix convention (ln -> iln).
INPUT_DIR      = BASE_DIR / "input" / "prod"
PBB_LOAN_DIR   = INPUT_DIR / "loan"
PIBB_LOAN_DIR  = INPUT_DIR / "loan"

# LNNOTE / LNCOMM carry no date component in their filenames -> static paths
PBB_LNNOTE_FILE  = STG_DIR / "PBB_lnnote.sas7bdat"
PBB_LNCOMM_FILE  = STG_DIR / "PBB_lncomm.sas7bdat"
PIBB_LNNOTE_FILE = STG_DIR / "PIBB_lnnote.sas7bdat"
PIBB_LNCOMM_FILE = STG_DIR / "PIBB_lncomm .sas7bdat"

# ------------------------------------------------------------------
# Parquet cache directories (chunked SAS -> Parquet, freshness-checked)
# ------------------------------------------------------------------
CACHE_DIR      = BASE_DIR / "input" / "cache" / "EIBMLNLT"
PBB_CACHE_DIR  = CACHE_DIR / "PBB"
PIBB_CACHE_DIR = CACHE_DIR / "PIBB"

# ------------------------------------------------------------------
# Output directories / files (fixed filenames -- no date suffix)
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

def fmt_bankno(bankno):
    """PROC FORMAT BANKFMT"""
    return BANK_FMT.get(bankno, str(bankno) if bankno is not None else '')

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
    those specific SAS SELECT branches.
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
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
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
                col = table.column(field.name)
                if col.type != field.type:
                    try:
                        col = col.cast(field.type, safe=False)
                    except Exception as e:
                        print(f"  [{tag}] WARNING: Cannot cast '{field.name}' "
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

def prepare_data(loan_dir: Path, loan_prefix: str, lnnote_file: Path,
                  lncomm_file: Path, cache_dir: Path, tag: str):
    """
    Replicate the SAS data preparation steps:
      1. Determine report variables from REPTDATE.py
      2. Resolve latest LOAN&REPTMON&NOWK-equivalent file via input_date.py
      3. Cache LOAN / LNNOTE / LNCOMM .sas7bdat inputs to Parquet
      4. PROC SORT LNNOTE (KEEP=ACCTNO NOTENO BANKNO STATE) BY ACCTNO NOTENO
      5. PROC SORT LOAN BY ACCTNO NOTENO
      6. MERGE LOAN + LNNOTE by ACCTNO NOTENO, keep IF ACCTYPE='LN' -> LNOTE
      7. PROC SORT LNOTE BY ACCTNO COMMNO
      8. PROC SORT LNCOMM BY ACCTNO COMMNO
      9. MERGE LNOTE + LNCOMM by ACCTNO COMMNO, keep if A -> NOTE1
     10. NOTE2 = NOTE1 filtered by CUSTCD and SECTORCD rules
     11. PROC SORT NOTE1 -> LNNOTE1 BY BRANCH FISSPURP CUSTCD ACCTNO
     12. PROC SORT NOTE2 -> LNNOTE2 BY BRANCH SECTORCD CUSTCD ACCTNO
    Returns: wk, rdate, reptmon, reptyear, lnnote1 (polars df), lnnote2 (polars df)
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

    con = duckdb.connect()

    # LNNOTE: KEEP=ACCTNO NOTENO BANKNO STATE, sorted BY ACCTNO NOTENO
    lnnote_df = con.execute(f"""
        SELECT acctno, noteno, bankno, state
        FROM read_parquet('{lnnote_cache}')
        ORDER BY acctno, noteno
    """).pl()

    # LOAN sorted BY ACCTNO NOTENO
    loan_df = con.execute(f"""
        SELECT *
        FROM read_parquet('{loan_cache}')
        ORDER BY acctno, noteno
    """).pl()

    con.close()

    # MERGE LOAN + LNNOTE by ACCTNO NOTENO, IF ACCTYPE='LN'
    # SAS MERGE: later dataset (LNNOTE) overwrites matching vars (BANKNO, STATE)
    merged = loan_df.join(lnnote_df, on=['acctno', 'noteno'], how='left', suffix='_note')

    if 'bankno_note' in merged.columns:
        merged = merged.with_columns([
            pl.when(pl.col('bankno_note').is_not_null()).then(pl.col('bankno_note')).otherwise(pl.col('bankno')).alias('bankno')
        ]).drop('bankno_note')
    if 'state_note' in merged.columns:
        merged = merged.with_columns([
            pl.when(pl.col('state_note').is_not_null()).then(pl.col('state_note')).otherwise(pl.col('state')).alias('state')
        ]).drop('state_note')

    lnote = merged.filter(pl.col('acctype') == 'LN').select([
        'bankno', 'branch', 'acctno', 'noteno', 'name', 'balance',
        'sectorcd', 'custcd', 'intrate', 'ntbrch', 'commno', 'liabcode',
        'apprlimt', 'fisspurp', 'state'
    ]).sort(['acctno', 'commno'])

    del merged, loan_df, lnnote_df
    gc.collect()

    # LNCOMM sorted BY ACCTNO COMMNO
    con2 = duckdb.connect()
    lncomm = con2.execute(f"""
        SELECT * FROM read_parquet('{lncomm_cache}')
        ORDER BY acctno, commno
    """).pl()
    con2.close()

    # MERGE LNOTE + LNCOMM by ACCTNO COMMNO, keep IF A (keep all from lnote)
    note1 = lnote.join(lncomm, on=['acctno', 'commno'], how='left', suffix='_comm')

    for col in ['bankno', 'branch', 'noteno', 'name', 'balance', 'sectorcd', 'custcd',
                'intrate', 'ntbrch', 'liabcode', 'apprlimt', 'fisspurp', 'state']:
        comm_col = f"{col}_comm"
        if comm_col in note1.columns:
            note1 = note1.with_columns(
                pl.when(pl.col(comm_col).is_not_null()).then(pl.col(comm_col)).otherwise(pl.col(col)).alias(col)
            ).drop(comm_col)

    note1_cols = ['bankno', 'branch', 'acctno', 'noteno', 'name', 'apprlimt', 'balance',
                  'sectorcd', 'custcd', 'state', 'intrate', 'ntbrch', 'commno', 'liabcode',
                  'ccolltrl', 'fisspurp']
    note1 = note1.select([c for c in note1_cols if c in note1.columns])

    del lnote, lncomm
    gc.collect()

    # NOTE2: CUSTCD NOT IN ('77','78','95','96') AND (SECTORCD starts with '5' OR SECTORCD='8310')
    note2 = note1.filter(
        (~pl.col('custcd').cast(pl.Utf8).is_in(['77', '78', '95', '96'])) &
        (
            pl.col('sectorcd').cast(pl.Utf8).str.starts_with('5') |
            (pl.col('sectorcd').cast(pl.Utf8) == '8310')
        )
    )

    # LNNOTE1: NOTE1 sorted BY BRANCH FISSPURP CUSTCD ACCTNO
    lnnote1 = note1.sort(['branch', 'fisspurp', 'custcd', 'acctno'])

    # LNNOTE2: NOTE2 sorted BY BRANCH SECTORCD CUSTCD ACCTNO
    lnnote2 = note2.sort(['branch', 'sectorcd', 'custcd', 'acctno'])

    del note1, note2
    gc.collect()

    return wk, rdate, reptmon, reptyear, lnnote1, lnnote2

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
        (pl.col('balance') > 0) | (pl.col('apprlimt') > 0)
    )

    lines = []
    for row in filtered.iter_rows(named=True):
        line = [' '] * 80

        branch = str(int(row['branch'])).rjust(3)[:3] if row['branch'] is not None else '   '
        acctno = str(int(row['acctno'])).rjust(10)[:10] if row['acctno'] is not None else ' ' * 10
        noteno = str(int(row['noteno'])).rjust(5)[:5] if row['noteno'] is not None else '     '

        bal  = row['balance'] if row['balance'] is not None else 0.0
        appr = row['apprlimt'] if row['apprlimt'] is not None else 0.0

        apprlimt_str = fmt_comma15_2(appr)
        balance_str  = fmt_comma15_2(bal)

        place_at(line, 1, branch)
        place_at(line, 4, acctno)
        place_at(line, 14, noteno)
        place_at(line, 20, apprlimt_str)
        place_at(line, 40, balance_str)

        lines.append(finalize_line(line, 80))

    with open(output_path, 'w', encoding='ascii', errors='replace') as f:
        for ln in lines:
            f.write(ln + '\n')

# =============================================================================
# RPS REPORT GENERATION HELPERS
# =============================================================================

def make_brno(branch):
    """Compute BRNO from branch number (matching SAS SELECT logic)."""
    b = int(branch)
    if b < 10:
        return f"BR0{b}"
    if b < 100:
        return f"BR{b}"
    return f"B{b}"

def write_newpage_fiss(f, pagecnt, branch, bankno, rdate, is_first_branch, pbb_title, line_width=136):
    """Write a new page header for FISS Purpose report. Returns updated pagecnt."""
    pagecnt += 1

    line = make_line(line_width)
    place_at(line, 1, 'E255')
    f.write(finalize_line(line, line_width) + '\n')

    if is_first_branch:
        brno = make_brno(branch)
        line = make_line(line_width)
        place_at(line, 1, 'P000PBBEDPPBBEDP')
        place_at(line, 133, brno)
        f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1, 'P000REPORT NO :  LOANLIST')
    place_at(line, 44, pbb_title)
    place_at(line, 122, f'PAGE NO : {pagecnt}')
    f.write(finalize_line(line, line_width) + '\n')

    bankno_fmt = fmt_bankno(bankno)
    branch_z3  = fmt_z3(branch)
    line = make_line(line_width)
    place_at(line, 1, 'P001BRANCH    :  ')
    place_at(line, 18, bankno_fmt)
    place_at(line, 22, branch_z3)
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

def write_newpage_sector(f, pagecnt, branch, bankno, rdate, is_first_branch, pbb_title, line_width=136):
    """Write a new page header for Sector/Construction report. Returns updated pagecnt."""
    pagecnt += 1

    line = make_line(line_width)
    place_at(line, 1, 'E255')
    f.write(finalize_line(line, line_width) + '\n')

    if is_first_branch:
        brno = make_brno(branch)
        line = make_line(line_width)
        place_at(line, 1, 'P000PBBEDPPBBEDP')
        place_at(line, 133, brno)
        f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1, 'P000REPORT NO :  LOANLIST')
    place_at(line, 44, pbb_title)
    place_at(line, 122, f'PAGE NO : {pagecnt}')
    f.write(finalize_line(line, line_width) + '\n')

    bankno_fmt = fmt_bankno(bankno)
    branch_z3  = fmt_z3(branch)
    line = make_line(line_width)
    place_at(line, 1, 'P001BRANCH    :  ')
    place_at(line, 18, bankno_fmt)
    place_at(line, 22, branch_z3)
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

    acctno = str(int(row['acctno'])).rjust(12) if row['acctno'] is not None else ' ' * 12
    noteno = str(int(row['noteno'])).rjust(8) if row['noteno'] is not None else ' ' * 8
    name   = str(row['name'])[:26] if row['name'] is not None else ''

    bal  = row['balance']  if row['balance']  is not None else 0.0
    appr = row['apprlimt'] if row['apprlimt'] is not None else 0.0

    fisspurp = str(row['fisspurp'])[:4].rjust(4) if row['fisspurp'] is not None else '    '
    sectorcd = str(row['sectorcd'])[:4].rjust(4) if row['sectorcd'] is not None else '    '
    custcd   = str(row['custcd']).rjust(4)[:4]   if row['custcd']   is not None else '    '
    state    = str(row['state'])[:3].rjust(3)    if row['state']    is not None else '   '
    liabcode = str(row['liabcode']) if row['liabcode'] is not None else ''
    ccolltrl = str(row['ccolltrl']) if row['ccolltrl'] is not None else ''

    place_at(line, 5, acctno)
    place_at(line, 19, noteno)
    place_at(line, 29, name)
    place_at(line, 55, fmt_comma15_2(appr))
    place_at(line, 72, fmt_comma15_2(bal))
    place_at(line, 90, fisspurp)
    place_at(line, 101, sectorcd)
    place_at(line, 108, custcd)
    place_at(line, 114, state)
    place_at(line, 120, fmt_comma5_2(row['intrate']))
    place_at(line, 126, liabcode)
    place_at(line, 131, ccolltrl)

    f.write(finalize_line(line, line_width) + '\n')

def write_blank_p001(f, line_width=136):
    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    f.write(finalize_line(line, line_width) + '\n')

def write_subtotal_fiss(f, fisspurp, bnmamt, line_width=136):
    """Write subtotal lines for FISS Purpose."""
    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    place_at(line, 72, AMOUNT_SINGLE_SEPARATOR)
    f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    fisp_str = str(fisspurp) if fisspurp is not None else ''
    place_at(line, 37, f'SUBTOTAL FOR FISS PURPOSE {fisp_str}')
    place_at(line, 72, fmt_comma15_2(bnmamt))
    f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    place_at(line, 72, AMOUNT_DOUBLE_SEPARATOR)
    f.write(finalize_line(line, line_width) + '\n')

    write_blank_p001(f, line_width)

def write_subtotal_sector(f, sectorcd, bnmamt, line_width=136):
    """Write subtotal lines for Sector Code."""
    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    place_at(line, 72, AMOUNT_SINGLE_SEPARATOR)
    f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    sect_str = str(sectorcd) if sectorcd is not None else ''
    place_at(line, 37, f'SUBTOTAL FOR SECTOR {sect_str}')
    place_at(line, 72, fmt_comma15_2(bnmamt))
    f.write(finalize_line(line, line_width) + '\n')

    line = make_line(line_width)
    place_at(line, 1, REPORT_LINE_PREFIX)
    place_at(line, 72, AMOUNT_DOUBLE_SEPARATOR)
    f.write(finalize_line(line, line_width) + '\n')

    write_blank_p001(f, line_width)

def write_grand_total(f, brchamt, line_width=136):
    """Write grand total lines for branch."""
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

def _derive_group_status(current_row, next_row, previous_branch, previous_group, group_field):
    """Determine boundary flags for the current row relative to its neighbours."""
    branch      = current_row['branch']
    group_value = current_row[group_field]

    is_first_branch = branch != previous_branch
    is_first_group  = is_first_branch or (group_value != previous_group)
    is_last_branch  = (next_row is None) or (next_row['branch'] != branch)
    is_last_group   = (
        (next_row is None)
        or (next_row[group_field] != group_value)
        or (next_row['branch'] != branch)
    )
    return branch, group_value, is_first_branch, is_first_group, is_last_branch, is_last_group


def _start_new_page(f, pagecnt, branch, bankno, rdate, is_first_page,
                    pbb_title, line_width, newpage_writer):
    """Call the appropriate newpage_writer and return updated (pagecnt, linecnt)."""
    pagecnt = newpage_writer(
        f, pagecnt, branch, bankno, rdate, is_first_page, pbb_title, line_width
    )
    return pagecnt, 11


def _handle_branch_open(f, branch, bankno, rdate,
                        pbb_title, line_width, newpage_writer):
    """
    Actions taken when entering a new branch: reset page/branch accumulator,
    emit the first page header for this branch. Returns (pagecnt, linecnt, brchamt).
    """
    brchamt = 0.0
    pagecnt, linecnt = _start_new_page(
        f, 0, branch, bankno, rdate, True,
        pbb_title, line_width, newpage_writer
    )
    return pagecnt, linecnt, brchamt


def _handle_page_overflow(f, pagecnt, linecnt, branch, bankno, rdate,
                          pbb_title, line_width, newpage_writer):
    """Emit a new page header when the current page is full."""
    if linecnt > PAGE_LINE_THRESHOLD:
        pagecnt, linecnt = _start_new_page(
            f, pagecnt, branch, bankno, rdate, False,
            pbb_title, line_width, newpage_writer
        )
    return pagecnt, linecnt


def _handle_group_close(f, pagecnt, linecnt, group_value, bnmamt,
                        branch, bankno, rdate, pbb_title, line_width,
                        newpage_writer, subtotal_writer):
    """Emit the group subtotal (4 lines), then check for page overflow."""
    subtotal_writer(f, group_value, bnmamt, line_width)
    linecnt += 4
    pagecnt, linecnt = _handle_page_overflow(
        f, pagecnt, linecnt, branch, bankno, rdate,
        pbb_title, line_width, newpage_writer
    )
    return pagecnt, linecnt


def _handle_branch_close(f, pagecnt, linecnt, brchamt,
                         branch, bankno, rdate, pbb_title, line_width,
                         newpage_writer):
    """Emit the branch grand total (4 lines), then check for page overflow."""
    write_grand_total(f, brchamt, line_width)
    linecnt += 4
    pagecnt, linecnt = _handle_page_overflow(
        f, pagecnt, linecnt, branch, bankno, rdate,
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
    prev_branch = None
    prev_group  = None

    with open(output_path, mode, encoding='ascii', errors='replace') as f:
        for idx, row in enumerate(rows):
            balance  = row['balance'] if row['balance'] is not None else 0.0
            next_row = rows[idx + 1] if idx + 1 < n else None
            bankno   = row['bankno']

            (
                branch,
                group_value,
                is_first_branch,
                is_first_group,
                is_last_branch,
                is_last_group,
            ) = _derive_group_status(row, next_row, prev_branch, prev_group, group_field)

            if is_first_branch:
                pagecnt, linecnt, brchamt = _handle_branch_open(
                    f, branch, bankno, rdate,
                    pbb_title, line_width, newpage_writer
                )

            if is_first_group:
                bnmamt = 0.0

            brchamt += balance
            bnmamt  += balance

            write_data_row(f, row, line_width)
            linecnt += 1

            pagecnt, linecnt = _handle_page_overflow(
                f, pagecnt, linecnt, branch, bankno, rdate,
                pbb_title, line_width, newpage_writer
            )

            if is_last_group:
                pagecnt, linecnt = _handle_group_close(
                    f, pagecnt, linecnt, group_value, bnmamt,
                    branch, bankno, rdate, pbb_title, line_width,
                    newpage_writer, subtotal_writer
                )

            if is_last_branch:
                pagecnt, linecnt = _handle_branch_close(
                    f, pagecnt, linecnt, brchamt,
                    branch, bankno, rdate, pbb_title, line_width,
                    newpage_writer
                )

            prev_branch = branch
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
        group_field='fisspurp',
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
        group_field='sectorcd',
        newpage_writer=write_newpage_sector,
        subtotal_writer=write_subtotal_sector
    )

# =============================================================================
# MAIN: PBB SECTION
# =============================================================================

def run_pbb():
    """
    Process PBB (Public Bank Berhad) section.
    Equivalent to the first EIBMLNLT SAS execution block.
    """
    print("Processing PBB section...")

    _, rdate, _, _, lnnote1, lnnote2 = prepare_data(
        PBB_LOAN_DIR, "ln",
        PBB_LNNOTE_FILE, PBB_LNCOMM_FILE,
        PBB_CACHE_DIR, "PBB"
    )

    write_lnlisd(lnnote1, PBB_LNLISD_TXT)
    print(f"  Written: {PBB_LNLISD_TXT}")

    pbb_title = 'P U B L I C   B A N K   B E R H A D'
    write_lnlist_fiss(lnnote1, rdate, PBB_LNLIST_TXT, pbb_title, mode='w')
    print(f"  Written FISS section: {PBB_LNLIST_TXT}")

    write_lnlist_sector(lnnote2, rdate, PBB_LNLIST_TXT, pbb_title, mode='a')
    print(f"  Appended Sector section: {PBB_LNLIST_TXT}")

    # PROC DATASETS LIB=WORK NOLIST; DELETE NOTE1 LNNOTE1; (in-memory, no-op in Python)

    # //STEP01 EXEC PGM=IEFBR14 - Delete RBP2.B033.LOANLIS*.RPS datasets (no-op in Python)
    # //STEP02 EXEC PGM=SPLIB136 - Insert control characters for RPS, split by region (no-op in Python)
    # //INFIL1 DD DSN=RMDS.OPC.BANKCODE(KLREGION) - KL Region bank codes (no-op in Python)
    # //INFIL2 DD DSN=RMDS.OPC.BANKCODE(BGREGION) - BG Region bank codes (no-op in Python)
    # //INFIL3 DD DSN=RMDS.OPC.BANKCODE(JBREGION) - JB Region bank codes (no-op in Python)
    # //INFIL4 DD DSN=RMDS.OPC.BANKCODE(SBREGION) - SB Region bank codes (no-op in Python)
    # //INFIL5 DD DSN=RMDS.OPC.BANKCODE(PPREGION) - PP Region bank codes (no-op in Python)
    # //INFIL6 DD DSN=RMDS.OPC.BANKCODE(TTREGION) - TT Region bank codes (no-op in Python)

    del lnnote1, lnnote2
    gc.collect()
    print("PBB section complete.")

# =============================================================================
# MAIN: PIBB SECTION
# =============================================================================

def run_pibb():
    """
    Process PIBB (Public Islamic Bank Berhad) section.
    Equivalent to the second EIBMLNLT SAS execution block (FOR PIBB).
    """
    print("Processing PIBB section...")

    _, rdate, _, _, lnnote1, lnnote2 = prepare_data(
        PIBB_LOAN_DIR, "iln",
        PIBB_LNNOTE_FILE, PIBB_LNCOMM_FILE,
        PIBB_CACHE_DIR, "PIBB"
    )

    write_lnlisd(lnnote1, PIBB_LNLISX_TXT)
    print(f"  Written: {PIBB_LNLISX_TXT}")

    pibb_title = 'P U B L I C   I S L A M I C   B A N K   B E R H A D'
    write_lnlist_fiss(lnnote1, rdate, PIBB_LNLISR_TXT, pibb_title, mode='w')
    print(f"  Written FISS section: {PIBB_LNLISR_TXT}")

    write_lnlist_sector(lnnote2, rdate, PIBB_LNLISR_TXT, pibb_title, mode='a')
    print(f"  Appended Sector section: {PIBB_LNLISR_TXT}")

    # PROC DATASETS LIB=WORK NOLIST; DELETE NOTE1 LNNOTE1; (in-memory, no-op in Python)

    # //STEP01 EXEC PGM=IEFBR14 - Delete RBP2.B051.LOANLIS*.RPS datasets (no-op in Python)
    # //STEP02 EXEC PGM=SPLIB136 - Insert control characters for RPS, split by region (no-op in Python)
    # //INFIL1 DD DSN=RMDS.OPC.BANKCODE(KLREGION) - KL Region bank codes (no-op in Python)
    # //INFIL2 DD DSN=RMDS.OPC.BANKCODE(BGREGION) - BG Region bank codes (no-op in Python)
    # //INFIL3 DD DSN=RMDS.OPC.BANKCODE(JBREGION) - JB Region bank codes (no-op in Python)
    # //INFIL4 DD DSN=RMDS.OPC.BANKCODE(SBREGION) - SB Region bank codes (no-op in Python)
    # //INFIL5 DD DSN=RMDS.OPC.BANKCODE(PPREGION) - PP Region bank codes (no-op in Python)
    # //INFIL6 DD DSN=RMDS.OPC.BANKCODE(TTREGION) - TT Region bank codes (no-op in Python)

    del lnnote1, lnnote2
    gc.collect()
    print("PIBB section complete.")

# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == '__main__':
    run_pbb()
    run_pibb()
