#!/usr/bin/env python3
"""
File Name   : EIBXLNLC.py
Description : Loan data preparation - merges LNNOTE, LNCOMM, and LOAN datasets
              to produce NOTE1 (all loans by FISSPURP) and NOTE2 (construction/
              real-estate loans for non-individual customers) for both PBB and
              PIBB. Runs at the same frequency as EIBXODLC.py (right after it
              in scheduling):
                - 16th of month -> report date = 15th  (NOWK='2')
                - 1st of month  -> report date = last day of prior month (NOWK='4')
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import os
import pandas as pd
import polars as pl
import gc

from REPTDATE import get_reptdate_values
from input_date import get_latest_file


# =============================================================================
# PATH CONFIGURATION
# =============================================================================
# # Production Path
# BASE_DIR = Path("/dwh")

BASE_DIR  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
INPUT_DIR = BASE_DIR / "input/prod" / "EIBXLNLC"

OUTPUT_DIR = BASE_DIR / "output" / "EIBXLNLC"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------------------------------
# Inputs:
#   1. enrh_ln_note.sas7bdat  - shared LNNOTE file for both PBB and PIBB;
#                                split by ENTITY_CD: 'PIBB' -> PIBB, else -> PBB
#                                KEEP=ACCTNO NOTENO BANKNO STATE ENTITY_CD
#                                (SAP.<ENTITY>.MNILN(0) - LNNOTE)
#   2. enrh_ln_comm.sas7bdat  - shared LNCOMM file for both PBB and PIBB
#                                KEEP=ACCTNO COMMNO CCOLLTRL
#                                (SAP.<ENTITY>.MNILN(0) - LNCOMM)
#   3. lnXXXXX.sas7bdat       - PBB loan extract  (SAP.PBB.SASDATA)
#   4. ilnXXXXX.sas7bdat      - PIBB loan extract (SAP.PIBB.SASDATA)
# ----------------------------------------------------------------------------
LNNOTE_PATH : Path = INPUT_DIR / "enrh_ln_note.sas7bdat"
LNCOMM_PATH : Path = INPUT_DIR / "enrh_ln_comm.sas7bdat"

PBB_CONFIG: Dict[str, Path] = {
    "loan_dir"  : get_latest_file(BASE_DIR / "input/prod/EIBXODLC", "ln"),    # e.g. ln05126.sas7bdat
    "output_dir": OUTPUT_DIR / "PBB",
}

PIBB_CONFIG: Dict[str, Path] = {
    "loan_dir"  : get_latest_file(BASE_DIR / "input/prod/EIBXODLC", "iln"),   # e.g. iln05126.sas7bdat
    "output_dir": OUTPUT_DIR / "PIBB",
}


# =============================================================================
# PROC FORMAT (informational - not used in output columns)
# =============================================================================
# PROC FORMAT;
#    VALUE BANKFMT 33='PBB'
#                 134='PFB';
# RUN;
BANKFMT = {33: 'PBB', 134: 'PFB'}


# =============================================================================
# REPORT DATE DERIVATION
# =============================================================================
# DATA _NULL_;
#    SET LOAN.REPTDATE;
#    SELECT(DAY(REPTDATE)) ... CALL SYMPUT('NOWK', ...) CALL SYMPUT('RDATE', ...)
#    CALL SYMPUT('REPTMON', ...) CALL SYMPUT('REPTYEAR', ...)
# RUN;
#
# This program does not read its own REPTDATE source. It follows the same
# biweekly schedule/derivation as EIBXODLC.py (runs immediately after it), so
# REPTMON / NOWK are obtained from REPTDATE.get_reptdate_values(). RDATE
# (DDMMYY8.) and REPTYEAR are not consumed downstream in this program (only
# REPTMON feeds output file naming), so they are not carried forward.


def _get_row_limit() -> Optional[int]:
    """
    Return an optional per-file row limit for fast testing.

    Set EIBXLNLC_ROW_LIMIT to a positive integer to read only that many rows
    from each SAS input. Leave it unset or set it to 0 for full production runs.

    e.g.     value = os.environ.get("EIBXLNLC_ROW_LIMIT", "1000").strip()   -> For 1000 dataset rows testing
    """
    value = os.environ.get("EIBXLNLC_ROW_LIMIT", "").strip()
    if not value:
        return None

    try:
        row_limit = int(value)
    except ValueError as exc:
        raise ValueError("EIBXLNLC_ROW_LIMIT must be a positive integer or 0") from exc

    return row_limit if row_limit > 0 else None


def _read_sas7bdat(path: Path, row_limit: Optional[int] = None) -> pl.DataFrame:
    """
    SAS → Parquet caching reader (biweekly-safe, memory-safe).

    - Converts SAS in chunks if needed
    - Stores Parquet in cache folder
    - Reuses Parquet if SAS not updated
    """

    # ------------------------------------------------------------------
    # Cache folder (keeps things clean)
    # ------------------------------------------------------------------
    cache_dir = path.parent / "parquet_cache_v5" / path.stem
    cache_dir.mkdir(parents=True, exist_ok=True)

    parquet_files = list(cache_dir.glob("*.parquet"))

    # ------------------------------------------------------------------
    # Check if cache is valid (IMPORTANT: biweekly-safe logic)
    # ------------------------------------------------------------------
    cache_valid = (
        len(parquet_files) > 0
        and max(f.stat().st_mtime for f in parquet_files)
            >= path.stat().st_mtime
    )

    # ------------------------------------------------------------------
    # CASE 1: USE CACHE (FAST PATH)
    # ------------------------------------------------------------------
    if cache_valid and row_limit is None:
        print(f"[CACHE HIT] Reading Parquet: {path.stem}")
        return pl.scan_parquet(str(cache_dir / "*.parquet"))

    # ------------------------------------------------------------------
    # CASE 2: TEST MODE (LIMIT ROWS)
    # ------------------------------------------------------------------
    if row_limit:
        print(f"[TEST MODE] Reading SAS: {path.name}")

        reader = pd.read_sas(
            str(path),
            encoding="latin1",
            chunksize=row_limit
        )

        try:
            pdf = next(reader)
        except StopIteration:
            pdf = pd.DataFrame()

        pdf.columns = [c.upper() for c in pdf.columns]
        return pl.from_pandas(pdf)

    # ------------------------------------------------------------------
    # CASE 3: FULL CONVERSION (SAS → PARQUET PARTITIONED)
    # ------------------------------------------------------------------
    print(f"\n[CONVERT] SAS → Parquet (chunked): {path.name}")

    reader = pd.read_sas(
        str(path),
        encoding="latin1",
        chunksize=500_000  # safe for 7GB file
    )

    print(f"[CONVERT] Starting chunked read for: {path.name}")

    for i, chunk in enumerate(reader):
        if chunk is None or chunk.empty:
            continue

        print(f"[CHUNK {i}] Reading chunk {i} ...")

        chunk.columns = [c.upper() for c in chunk.columns]

        df = pl.from_pandas(chunk)

        # Force consistent schema across all chunks
        df = df.with_columns([
            pl.col(c).cast(pl.Utf8, strict=False)
            for c in df.columns
        ])

        out_file = cache_dir / f"part-{i:05d}.parquet"
        df.write_parquet(out_file, compression="zstd")

        print(f"[WRITE] {out_file} ({len(df):,} rows)")

    print(f"[DONE] Cache created at: {cache_dir}")

    # ------------------------------------------------------------------
    # FINAL READ (as one logical dataset)
    # ------------------------------------------------------------------
    return pl.scan_parquet(str(cache_dir / "*.parquet")).collect()


def _read_lnnote_shared(lnnote_path: Path, row_limit: Optional[int] = None) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Read the shared enrh_ln_note.sas7bdat and split into PBB and PIBB subsets.

    SAS original:
      PROC SORT DATA=LNNOTE.LNNOTE (KEEP=ACCTNO NOTENO BANKNO STATE)
         OUT=LNNOTE;
         BY ACCTNO NOTENO;

    The shared file carries an ENTITY_CD column:
      - ENTITY_CD == 'PIBB'  -> PIBB dataset
      - ENTITY_CD != 'PIBB'  -> PBB dataset

    Only ACCTNO, NOTENO, BANKNO, STATE are retained (matching the SAS KEEP=).
    """
    raw = _read_sas7bdat(lnnote_path, row_limit=row_limit)
    expr = raw.select(["ACCTNO", "NOTENO", "BANKNO", "STATE", "ENTITY_CD"])
    df = expr.collect() if isinstance(expr, pl.LazyFrame) else expr

    df = (
        df
        .drop_nulls(["ACCTNO", "NOTENO"])
        .unique(subset=["ACCTNO", "NOTENO"])
        .with_columns([
            pl.col("ACCTNO").cast(pl.Float64).cast(pl.Int64),
            pl.col("NOTENO").cast(pl.Float64).cast(pl.Int64),
            pl.col("ENTITY_CD").cast(pl.Utf8),
        ])
    )

    keep_cols = ["ACCTNO", "NOTENO", "BANKNO", "STATE"]

    pibb_df = df.filter(pl.col("ENTITY_CD") == "PIBB").select(keep_cols)
    pbb_df  = df.filter(pl.col("ENTITY_CD") != "PIBB").select(keep_cols)

    return pbb_df, pibb_df


def _read_lncomm(lncomm_path: Path, row_limit: Optional[int] = None) -> pl.DataFrame:
    """
    PROC SORT DATA=LNNOTE.LNCOMM OUT=LNCOMM;
    BY ACCTNO COMMNO;
    """
    raw = _read_sas7bdat(lncomm_path, row_limit=row_limit)
    expr = raw.select(["ACCTNO", "COMMNO", "CCOLLTRL"])
    df = expr.collect() if isinstance(expr, pl.LazyFrame) else expr
    return (
        df
        .unique(subset=["ACCTNO", "COMMNO"])
        .with_columns([
            pl.col("ACCTNO").cast(pl.Float64).cast(pl.Int64),
            pl.col("COMMNO").cast(pl.Float64).cast(pl.Int64),
        ])
    )


def _read_loan(loan_path: Path, row_limit: Optional[int] = None) -> pl.DataFrame:
    """
    PROC SORT DATA=LOAN.LOAN&REPTMON&NOWK OUT=LOAN;
    BY ACCTNO NOTENO;
    """
    raw = _read_sas7bdat(loan_path, row_limit=row_limit)
    rename_map = {"SECTOR": "SECTORCD", "CUSTCODE": "CUSTCD"}

    if isinstance(raw, pl.LazyFrame):
        existing = raw.collect_schema().names()
        actual_rename = {k: v for k, v in rename_map.items() if k in existing}
        expr = raw.rename(actual_rename).select([
            "ACCTNO", "NOTENO", "COMMNO", "BRANCH", "BALANCE", "SECTORCD",
            "CUSTCD", "INTRATE", "APPRLIMT", "FISSPURP", "LIABCODE"
        ])
        df = expr.collect()
    else:
        actual_rename = {k: v for k, v in rename_map.items() if k in raw.columns}
        df = raw.rename(actual_rename).select([
            "ACCTNO", "NOTENO", "COMMNO", "BRANCH", "BALANCE", "SECTORCD",
            "CUSTCD", "INTRATE", "APPRLIMT", "FISSPURP", "LIABCODE"
        ])

    return df.with_columns([
        pl.col("ACCTNO").cast(pl.Float64).cast(pl.Int64),
        pl.col("NOTENO").cast(pl.Float64).cast(pl.Int64),
        pl.col("CUSTCD").cast(pl.Float64).cast(pl.Int64).cast(pl.Utf8),
        pl.col("SECTORCD").cast(pl.Utf8),
        pl.col("COMMNO").cast(pl.Float64).cast(pl.Int64),
    ])


# =============================================================================
# CORE PROCESSING
# =============================================================================
def process_bank(
    bank_name: str,
    config: Dict[str, Path],
    lnnote_df: pl.DataFrame,
    lncomm_df: pl.DataFrame,
    reptmon: str,
    row_limit: Optional[int] = None,
) -> None:
    """
    Process loan-list preparation for a single bank entity (PBB or PIBB).
    """
    loan_path  = config["loan_dir"]
    output_dir = config["output_dir"]

    if not loan_path.exists():
        raise FileNotFoundError(f"[{bank_name}] Missing LOAN file: {loan_path}")

    loan_df = _read_loan(loan_path, row_limit=row_limit)

    # ------------------------------------------------------------------
    # SAS original does not explicitly filter CUSTCD here at LOAN level;
    # the CUSTCD filter is applied only in the NOTE2 step.
    # Filter ACCTYPE = 'LN' is implicit via the LOAN dataset used.
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # DATA LNOTE: MERGE LOAN(IN=A) LNNOTE(IN=B); BY ACCTNO NOTENO; IF ACCTYPE = 'LN'
    # KEEP: BANKNO BRANCH ACCTNO NOTENO NAME BALANCE SECTORCD CUSTCD
    #       INTRATE NTBRCH COMMNO LIABCODE APPRLIMT FISSPURP STATE
    #
    # Note: NAME and NTBRCH are not present in LNNOTE (KEEP=ACCTNO NOTENO BANKNO STATE
    # in original SAS). They originate from the LOAN dataset itself. LIABCODE comes
    # from LOAN; the SAS merge brings BANKNO and STATE from LNNOTE side.
    # ------------------------------------------------------------------
    lnote_df = loan_df.join(
        lnnote_df,
        on=["ACCTNO", "NOTENO"],
        how="left"
    )

    keep_lnote = [
        "BANKNO", "BRANCH", "ACCTNO", "NOTENO", "NAME", "BALANCE",
        "SECTORCD", "CUSTCD", "INTRATE", "NTBRCH", "COMMNO", "LIABCODE",
        "APPRLIMT", "FISSPURP", "STATE",
    ]
    # Retain only columns that exist after the join
    lnote_df = lnote_df.select([c for c in keep_lnote if c in lnote_df.columns])

    lnote_df = lnote_df.with_columns(
        pl.col("COMMNO").cast(pl.Float64).cast(pl.Int64)
    )

    # ------------------------------------------------------------------
    # DATA NOTE1: MERGE LNOTE(IN=A) LNCOMM(IN=B); BY ACCTNO COMMNO; IF A
    # KEEP: BANKNO BRANCH ACCTNO NOTENO NAME APPRLIMT BALANCE SECTORCD
    #       CUSTCD STATE INTRATE NTBRCH COMMNO LIABCODE CCOLLTRL FISSPURP
    # ------------------------------------------------------------------
    note1_df = lnote_df.join(
        lncomm_df,
        on=["ACCTNO", "COMMNO"],
        how="left",
        suffix="_COMM"
    )

    keep_note1 = [
        "BANKNO", "BRANCH", "ACCTNO", "NOTENO", "NAME", "APPRLIMT", "BALANCE",
        "SECTORCD", "CUSTCD", "STATE", "INTRATE", "NTBRCH", "COMMNO",
        "LIABCODE", "CCOLLTRL", "FISSPURP",
    ]
    note1_df = note1_df.drop([c for c in note1_df.columns if c.endswith("_COMM")])
    note1_df = note1_df.select([c for c in keep_note1 if c in note1_df.columns])

    # ------------------------------------------------------------------
    # DATA NOTE2: SET NOTE1
    # IF CUSTCD NOT IN ('77','78','95','96') AND
    #    (SUBSTR(SECTORCD,1,1) = '5' OR SECTORCD = '8310') THEN OUTPUT
    # ------------------------------------------------------------------
    sector = pl.col("SECTORCD").cast(pl.Utf8)
    note2_df = note1_df.filter(
        (~pl.col("CUSTCD").cast(pl.Utf8).is_in(["77", "78", "95", "96"]))
        & ((sector.str.slice(0, 1) == "5") | (sector == "8310"))
    )

    del loan_df
    del lnote_df
    gc.collect()

    # PROC DATASETS LIB=WORK NOLIST; DELETE LNOTE LNCOMM (implicit - not needed in Python)

    # ------------------------------------------------------------------
    # PROC SORT DATA=NOTE1 OUT=LNLC(I).NOTE1&REPTMON; BY BRANCH FISSPURP CUSTCD ACCTNO
    # PROC SORT DATA=NOTE2 OUT=LNLC(I).NOTE2&REPTMON; BY BRANCH SECTORCD CUSTCD ACCTNO
    # ------------------------------------------------------------------
    output_dir.mkdir(parents=True, exist_ok=True)

    prefix = "LNLC" if bank_name == "PBB" else "LNLCI"

    note1_tmp = output_dir / f"{prefix}_NOTE1_{reptmon}_tmp.parquet"
    note2_tmp = output_dir / f"{prefix}_NOTE2_{reptmon}_tmp.parquet"

    # STEP 1: write UNSORTED (low memory)
    note1_df.write_parquet(note1_tmp)
    note2_df.write_parquet(note2_tmp)

    # STEP 2: lazy sort (disk-based, not RAM-heavy)
    note1_sorted = (
        pl.scan_parquet(note1_tmp)
        .sort(["BRANCH", "FISSPURP", "CUSTCD", "ACCTNO"])
        .collect()
    )

    note2_sorted = (
        pl.scan_parquet(note2_tmp)
        .sort(["BRANCH", "SECTORCD", "CUSTCD", "ACCTNO"])
        .collect()
    )

    # STEP 3: final output
    note1_out = output_dir / f"{prefix}_NOTE1_{reptmon}.parquet"
    note2_out = output_dir / f"{prefix}_NOTE2_{reptmon}.parquet"

    note1_sorted.write_parquet(note1_out)
    note2_sorted.write_parquet(note2_out)

    # ------------------------------------------------------------------
    # Terminal summary
    # ------------------------------------------------------------------
    print(f"\n[{bank_name}] REPTMON={reptmon}")
    print(f"[{bank_name}] NOTE1 rows : {len(note1_sorted):,}")
    print(f"[{bank_name}] NOTE2 rows : {len(note2_sorted):,}")
    print(f"[{bank_name}] Output -> {note1_out}")
    print(f"[{bank_name}] Output -> {note2_out}")
    print(note1_sorted.head())
    print(note2_sorted.head())


# =============================================================================
# MAIN
# =============================================================================
def main() -> None:
    rv = get_reptdate_values()
    reptmon = rv.reptmon   # zero-padded month e.g. '05'
    nowk    = rv.nowk      # week bucket       e.g. '2' or '4'

    row_limit = _get_row_limit()

    print(f"Report Date : {rv.reptdate}  (REPTMON={reptmon}, NOWK={nowk})")
    if row_limit:
        print(f"Test mode: reading at most {row_limit:,} rows from each SAS input")

    if not LNNOTE_PATH.exists():
        raise FileNotFoundError(f"Missing shared LNNOTE file: {LNNOTE_PATH}")
    if not LNCOMM_PATH.exists():
        raise FileNotFoundError(f"Missing shared LNCOMM file: {LNCOMM_PATH}")

    # Read shared LNNOTE once and split into PBB / PIBB subsets
    pbb_lnnote_df, pibb_lnnote_df = _read_lnnote_shared(LNNOTE_PATH, row_limit=row_limit)

    # Read shared LNCOMM once (same file for both banks)
    shared_lncomm_df = _read_lncomm(LNCOMM_PATH, row_limit=row_limit)

    # PBB
    process_bank(
        "PBB", PBB_CONFIG,
        lnnote_df=pbb_lnnote_df,
        lncomm_df=shared_lncomm_df,
        reptmon=reptmon,
        row_limit=row_limit,
    )

    # PIBB
    process_bank(
        "PIBB", PIBB_CONFIG,
        lnnote_df=pibb_lnnote_df,
        lncomm_df=shared_lncomm_df,
        reptmon=reptmon,
        row_limit=row_limit,
    )


if __name__ == "__main__":
    main()
