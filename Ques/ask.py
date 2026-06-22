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
# 3 inputs per bank entity, all .sas7bdat:
#   1. LNNOTE - KEEP=ACCTNO NOTENO BANKNO STATE  (SAP.<ENTITY>.MNILN(0) - LNNOTE)
#   2. LNCOMM - ACCTNO COMMNO CCOLLTRL           (SAP.<ENTITY>.MNILN(0) - LNCOMM)
#   3. LOAN / ILOAN - loan extract (SAP.PBB.SASDATA / SAP.PIBB.SASDATA)
# ----------------------------------------------------------------------------
PBB_CONFIG: Dict[str, Path] = {
    "lnnote"    : INPUT_DIR / "lnnote_pbb.sas7bdat",
    "lncomm"    : INPUT_DIR / "enrh_ln_comm.sas7bdat",
    "loan_dir"  : get_latest_file(BASE_DIR / "input/prod/EIBXODLC", "ln"),    # e.g. ln05126.sas7bdat
    "output_dir": OUTPUT_DIR / "PBB",
}

PIBB_CONFIG: Dict[str, Path] = {
    "lnnote"    : INPUT_DIR / "lnnote_pibb.sas7bdat",
    "lncomm"    : INPUT_DIR / "enrh_ln_comm.sas7bdat",
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
    value = os.environ.get("EIBXLNLC_ROW_LIMIT", "1000").strip()
    if not value:
        return None

    try:
        row_limit = int(value)
    except ValueError as exc:
        raise ValueError("EIBXLNLC_ROW_LIMIT must be a positive integer or 0") from exc

    return row_limit if row_limit > 0 else None


# def _read_sas7bdat(path: Path, row_limit: Optional[int] = None) -> pl.DataFrame:
#     """Read a .sas7bdat file via pandas and convert to Polars with uppercased columns."""
#     if row_limit:
#         reader = pd.read_sas(str(path), encoding="latin1", chunksize=row_limit)
#         try:
#             pdf = next(reader)
#         except StopIteration:
#             pdf = pd.DataFrame()
#     else:
#         pdf = pd.read_sas(str(path), encoding="latin1")

#     pdf.columns = [c.upper() for c in pdf.columns]
#     return pl.from_pandas(pdf)


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
    cache_dir = path.parent / "parquet_cache_v2" / path.stem
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
        return pl.scan_parquet(str(cache_dir / "*.parquet")).collect()

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

    for i, chunk in enumerate(reader):
        if chunk is None or chunk.empty:
            continue

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


def _read_lnnote(lnnote_path: Path, row_limit: Optional[int] = None) -> pl.DataFrame:
    """
    PROC SORT DATA=LNNOTE.LNNOTE (KEEP=ACCTNO NOTENO BANKNO STATE)
       OUT=LNNOTE; BY ACCTNO NOTENO;
    """
    return (
        _read_sas7bdat(lnnote_path, row_limit=row_limit)
        .select(["ACCTNO", "NOTENO", "BANKNO", "STATE", "NAME", "NTBRCH", "COMMNO"])
        .with_columns([
            pl.col("ACCTNO").cast(pl.Float64).cast(pl.Int64),
            pl.col("NOTENO").cast(pl.Float64).cast(pl.Int64),
        ])
    )


def _read_lncomm(lncomm_path: Path, row_limit: Optional[int] = None) -> pl.DataFrame:
    """
    PROC SORT DATA=LNNOTE.LNCOMM OUT=LNCOMM; BY ACCTNO COMMNO;
    """
    return (
        _read_sas7bdat(lncomm_path, row_limit=row_limit)
        .select(["ACCTNO", "COMMNO", "CCOLLTRL"])
        .with_columns([
            pl.col("ACCTNO").cast(pl.Float64).cast(pl.Int64),
            pl.col("COMMNO").cast(pl.Float64).cast(pl.Int64),
        ])
    )


def _read_loan(loan_path: Path, row_limit: Optional[int] = None) -> pl.DataFrame:
    """
    PROC SORT DATA=LOAN.LOAN&REPTMON&NOWK OUT=LOAN; BY ACCTNO NOTENO;
    Original LOAN columns 'SECTOR' / 'CUSTCODE' renamed to 'SECTORCD' / 'CUSTCD'.
    """
    return (
        _read_sas7bdat(loan_path, row_limit=row_limit)
        .rename({"SECTOR": "SECTORCD", "CUSTCODE": "CUSTCD", "STATECD":"STATE"})
        .with_columns([
            pl.col("ACCTNO").cast(pl.Float64).cast(pl.Int64),
            pl.col("NOTENO").cast(pl.Float64).cast(pl.Int64),
            pl.col("CUSTCD").cast(pl.Float64).cast(pl.Int64).cast(pl.Utf8),
            pl.col("SECTORCD").cast(pl.Utf8),
        ])
    )


# =============================================================================
# CORE PROCESSING
# =============================================================================
def process_bank(
    bank_name: str,
    config: Dict[str, Path],
    reptmon: str,
    lncomm_df: Optional[pl.DataFrame] = None,
    row_limit: Optional[int] = None,
) -> None:
    """
    Process loan-list preparation for a single bank entity (PBB or PIBB).
    """
    lnnote_path = config["lnnote"]
    lncomm_path = config["lncomm"]
    loan_path   = config["loan_dir"]
    output_dir  = config["output_dir"]

    if not lnnote_path.exists():
        raise FileNotFoundError(f"[{bank_name}] Missing LNNOTE file: {lnnote_path}")
    if not lncomm_path.exists():
        raise FileNotFoundError(f"[{bank_name}] Missing LNCOMM file: {lncomm_path}")
    if not loan_path.exists():
        raise FileNotFoundError(f"[{bank_name}] Missing LOAN file  : {loan_path}")

    lnnote_df = _read_lnnote(lnnote_path, row_limit=row_limit)
    if lncomm_df is None:
        lncomm_df = _read_lncomm(lncomm_path, row_limit=row_limit)
    loan_df   = _read_loan(loan_path, row_limit=row_limit)

    # ------------------------------
    # Fix JOIN Memory
    # ------------------------------
    lnnote_df = lnnote_df.select([
        "ACCTNO", "NOTENO", "BANKNO", "STATE", "NAME", "NTBRCH", "COMMNO"
    ])

    lncomm_df = lncomm_df.select([
        "ACCTNO", "COMMNO", "CCOLLTRL"
    ])

    loan_df = loan_df.select([
        "ACCTNO", "NOTENO", "BRANCH", "BALANCE", "SECTORCD", "CUSTCD",
        "INTRATE", "APPRLIMT", "FISSPURP", "STATE"
    ])

    # ------------------------------------------------------------------
    # DATA LNOTE: MERGE LOAN(IN=A) LNNOTE(IN=B); BY ACCTNO NOTENO
    # IF ACCTYPE = 'LN'
    # KEEP: BANKNO BRANCH ACCTNO NOTENO NAME BALANCE SECTORCD CUSTCD
    #       INTRATE NTBRCH COMMNO LIABCODE APPRLIMT FISSPURP STATE
    # ------------------------------------------------------------------
    lnote_df = loan_df.join(lnnote_df, on=["ACCTNO", "NOTENO"], how="left", suffix="_NOTE")
    # lnote_df = lnote_df.filter(pl.col("ACCTYPE") == "LN")

    keep_lnote = [
        "BANKNO", "BRANCH", "ACCTNO", "NOTENO", "NAME", "BALANCE",
        "SECTORCD", "CUSTCD", "INTRATE", "NTBRCH", "COMMNO", "LIABCODE",
        "APPRLIMT", "FISSPURP", "STATE",
    ]
    # BANKNO / STATE are only present on the LNNOTE side; prefer that side
    # if LOAN does not already carry them.
    for col in keep_lnote:
        note_col = col + "_NOTE"
        if note_col in lnote_df.columns and col not in lnote_df.columns:
            lnote_df = lnote_df.rename({note_col: col})
    lnote_df = lnote_df.drop([c for c in lnote_df.columns if c.endswith("_NOTE")])
    lnote_df = lnote_df.select([c for c in keep_lnote if c in lnote_df.columns])

    # SAS sorted before merging; Polars hash joins do not require pre-sorting.
    # Avoiding this large intermediate sort saves significant time and memory.
    lnote_df = lnote_df.with_columns(
        pl.col("COMMNO").cast(pl.Float64).cast(pl.Int64)
    )

    # ------------------------------------------------------------------
    # DATA NOTE1: MERGE LNOTE(IN=A) LNCOMM(IN=B); BY ACCTNO COMMNO; IF A
    # KEEP: BANKNO BRANCH ACCTNO NOTENO NAME APPRLIMT BALANCE SECTORCD
    #       CUSTCD STATE INTRATE NTBRCH COMMNO LIABCODE CCOLLTRL FISSPURP
    # ------------------------------------------------------------------
    note1_df = lnote_df.join(lncomm_df, on=["ACCTNO", "COMMNO"], how="left", suffix="_COMM")

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
    del lnnote_df
    del lncomm_df
    del lnote_df
    gc.collect()

    # PROC DATASETS LIB=WORK NOLIST; DELETE LNOTE LNCOMM (implicit - not needed in Python)

    # ------------------------------------------------------------------
    # PROC SORT DATA=NOTE1 OUT=LNLC(I).NOTE1&REPTMON; BY BRANCH FISSPURP CUSTCD ACCTNO
    # PROC SORT DATA=NOTE2 OUT=LNLC(I).NOTE2&REPTMON; BY BRANCH SECTORCD CUSTCD ACCTNO
    # ------------------------------------------------------------------
    output_dir.mkdir(parents=True, exist_ok=True)

    # note1_sorted = note1_df.sort(["BRANCH", "FISSPURP", "CUSTCD", "ACCTNO"])
    # note2_sorted = note2_df.sort(["BRANCH", "SECTORCD", "CUSTCD", "ACCTNO"])

    # prefix = "LNLC" if bank_name == "PBB" else "LNLCI"
    # note1_out = output_dir / f"{prefix}_NOTE1_{reptmon}.parquet"
    # note2_out = output_dir / f"{prefix}_NOTE2_{reptmon}.parquet"

    # note1_sorted.write_parquet(note1_out)
    # note2_sorted.write_parquet(note2_out)

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
    print(f"[{bank_name}] LOAN rows  : {len(loan_df):,}")
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

    shared_lncomm_df: Optional[pl.DataFrame] = None
    if PBB_CONFIG["lncomm"] == PIBB_CONFIG["lncomm"]:
        lncomm_path = PBB_CONFIG["lncomm"]
        if not lncomm_path.exists():
            raise FileNotFoundError(f"Missing shared LNCOMM file: {lncomm_path}")
        shared_lncomm_df = _read_lncomm(lncomm_path, row_limit=row_limit)

    # PBB
    process_bank("PBB", PBB_CONFIG, reptmon, lncomm_df=shared_lncomm_df, row_limit=row_limit)

    # PIBB
    process_bank("PIBB", PIBB_CONFIG, reptmon, lncomm_df=shared_lncomm_df, row_limit=row_limit)


if __name__ == "__main__":
    main()
