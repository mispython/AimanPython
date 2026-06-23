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

Column renames already applied at source (parquet creation):
    LOAN : SECTOR   -> SECTORCD  |  CUSTCODE -> CUSTCD  |  STATECD -> STATE
    LNCOMM: CMBRCH  -> BRANCH    |  CSECTOR  -> SECTORCD|  CSTATE  -> STATE

SAS MERGE last-dataset-wins semantics replicated by explicit column coalescing:
  Step 1: MERGE LOAN(IN=A) LNNOTE(IN=B) BY ACCTNO NOTENO
          LNNOTE was sorted with KEEP=ACCTNO NOTENO BANKNO STATE, so the work
          dataset entering the merge has ONLY those 4 columns.
          -> LNNOTE wins (last-dataset) for shared column: STATE only
          -> LNNOTE provides exclusively: BANKNO
          -> LOAN provides all other columns: BRANCH, BALANCE, SECTORCD,
             CUSTCD, INTRATE, COMMNO, LIABCODE, APPRLIMT, FISSPURP
          -> NAME, NTBRCH: in LNOTE KEEP list but absent from both
             LOAN and LNNOTE(KEEP=4); will be null unless LOAN carries them
          -> IF ACCTYPE = 'LN'  (parquet pre-filtered; effectively a no-op)

  Step 2: MERGE LNOTE(IN=A) LNCOMM(IN=B) BY ACCTNO COMMNO
          LNCOMM has no KEEP restriction, all columns flow into merge.
          -> LNCOMM wins (last-dataset) for shared columns:
             BANKNO, BRANCH (CMBRCH), SECTORCD (CSECTOR), STATE (CSTATE)
          -> CCOLLTRL comes exclusively from LNCOMM
          -> IF A  (left join; LNOTE rows always kept)

  NOTE1 = result of Step 2 (all records, KEEP list applied)
  NOTE2 = NOTE1 filtered:
          CUSTCD NOT IN ('77','78','95','96')
          AND (SUBSTR(SECTORCD,1,1)='5' OR SECTORCD='8310')
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import os
import gc

import pandas as pd
import polars as pl
import duckdb

from REPTDATE import get_reptdate_values
from input_date import get_latest_file


# =============================================================================
# PATH CONFIGURATION
# =============================================================================
# # Production Path
# BASE_DIR = Path("/dwh")

BASE_DIR   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
INPUT_DIR  = BASE_DIR / "input/prod" / "EIBXLNLC"
OUTPUT_DIR = BASE_DIR / "output" / "EIBXLNLC"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------------------------------
# Inputs (all .parquet, columns already renamed per spec above):
#
#   PBB
#     lnnote  : ACCTNO NOTENO BANKNO STATE NAME NTBRCH COMMNO LIABCODE
#     lncomm  : ACCTNO COMMNO CCOLLTRL BRANCH SECTORCD STATE
#     loan    : ACCTNO NOTENO COMMNO BRANCH BALANCE SECTORCD CUSTCD INTRATE
#                APPRLIMT FISSPURP LIABCODE  (ACCTYPE pre-filtered to 'LN')
#
#   PIBB - same structure, different files
# ----------------------------------------------------------------------------
PBB_CONFIG: Dict[str, Path] = {
    "lnnote"    : INPUT_DIR / "lnnote_pbb.parquet",
    "lncomm"    : INPUT_DIR / "lncomm_pbb.parquet",
    "loan"      : get_latest_file(BASE_DIR / "input/prod/EIBXODLC", "ln"),
    "output_dir": OUTPUT_DIR / "PBB",
}

PIBB_CONFIG: Dict[str, Path] = {
    "lnnote"    : INPUT_DIR / "lnnote_pibb.parquet",
    "lncomm"    : INPUT_DIR / "lncomm_pibb.parquet",
    "loan"      : get_latest_file(BASE_DIR / "input/prod/EIBXODLC", "iln"),
    "output_dir": OUTPUT_DIR / "PIBB",
}


# =============================================================================
# PROC FORMAT (informational only - BANKNO not kept in final output)
# =============================================================================
# PROC FORMAT;
#    VALUE BANKFMT 33='PBB'
#                 134='PFB';
# RUN;
BANKFMT: Dict[int, str] = {33: "PBB", 134: "PFB"}


# =============================================================================
# REPORT DATE DERIVATION
# =============================================================================
# DATA _NULL_;
#    SET LOAN.REPTDATE;
#    SELECT(DAY(REPTDATE)) ...
#    CALL SYMPUT('NOWK',   PUT(WK,$1.));
#    CALL SYMPUT('RDATE',  PUT(REPTDATE, DDMMYY8.));
#    CALL SYMPUT('REPTMON',PUT(MONTH(REPTDATE), Z2.));
#    CALL SYMPUT('REPTYEAR',PUT(REPTDATE, YEAR4.));
# RUN;
#
# REPTMON / NOWK are obtained from REPTDATE.get_reptdate_values().
# RDATE and REPTYEAR are not consumed downstream in this program.


# =============================================================================
# ENVIRONMENT / TEST MODE
# =============================================================================
def _get_row_limit() -> Optional[int]:
    """
    Return an optional per-file row limit for fast testing.

    Set EIBXLNLC_ROW_LIMIT to a positive integer to read only that many rows
    from each input. Leave it unset or set it to 0 for full production runs.
    """
    value = os.environ.get("EIBXLNLC_ROW_LIMIT", "").strip()
    if not value:
        return None
    try:
        row_limit = int(value)
    except ValueError as exc:
        raise ValueError("EIBXLNLC_ROW_LIMIT must be a positive integer or 0") from exc
    return row_limit if row_limit > 0 else None


# =============================================================================
# PARQUET READERS  (DuckDB → Polars)
# =============================================================================
def _read_parquet(path: Path, columns: list[str], row_limit: Optional[int] = None) -> pl.DataFrame:
    """Read selected columns from a parquet file via DuckDB."""
    cols_sql = ", ".join(f'"{c}"' for c in columns)
    limit_clause = f"LIMIT {row_limit}" if row_limit else ""
    con = duckdb.connect()
    df = con.execute(
        f"SELECT {cols_sql} FROM read_parquet('{path}') {limit_clause}"
    ).pl()
    con.close()
    return df


def _read_lnnote(path: Path, row_limit: Optional[int] = None) -> pl.DataFrame:
    """
    PROC SORT DATA=LNNOTE.LNNOTE (KEEP=ACCTNO NOTENO BANKNO STATE) OUT=LNNOTE;
    The KEEP dataset option on PROC SORT restricts the output work dataset to
    exactly 4 columns: ACCTNO, NOTENO, BANKNO, STATE.
    All other LNNOTE columns (NAME, NTBRCH, BALANCE, SECTORCD, etc.) are
    stripped before the merge and therefore come from LOAN, not LNNOTE.
    In Merge 1, LNNOTE wins only for STATE (shared); BANKNO is LNNOTE-only.
    """
    cols = ["ACCTNO", "NOTENO", "BANKNO", "STATE"]
    df = _read_parquet(path, cols, row_limit)
    return df.with_columns([
        pl.col("ACCTNO").cast(pl.Float64).cast(pl.Int64),
        pl.col("NOTENO").cast(pl.Float64).cast(pl.Int64),
        pl.col("BANKNO").cast(pl.Float64).cast(pl.Int64).cast(pl.Utf8),
        pl.col("STATE").cast(pl.Utf8),
    ])


def _read_lncomm(path: Path, row_limit: Optional[int] = None) -> pl.DataFrame:
    """
    PROC SORT DATA=LNNOTE.LNCOMM OUT=LNCOMM; BY ACCTNO COMMNO
    No KEEP restriction — all columns flow into Merge 2.
    Columns (parquet already renamed):
      ACCTNO COMMNO BANKNO CCOLLTRL BRANCH(CMBRCH) SECTORCD(CSECTOR) STATE(CSTATE)
    LNCOMM wins (last-dataset) in Merge 2 for: BANKNO, BRANCH, SECTORCD, STATE
    """
    cols = ["ACCTNO", "COMMNO", "BANKNO", "CCOLLTRL", "BRANCH", "SECTORCD", "STATE"]
    df = _read_parquet(path, cols, row_limit)
    return df.with_columns([
        pl.col("ACCTNO").cast(pl.Float64).cast(pl.Int64),
        pl.col("COMMNO").cast(pl.Float64).cast(pl.Int64),
        pl.col("BANKNO").cast(pl.Float64).cast(pl.Int64).cast(pl.Utf8),
        pl.col("CCOLLTRL").cast(pl.Utf8),
        pl.col("BRANCH").cast(pl.Float64).cast(pl.Int64).cast(pl.Utf8),
        pl.col("SECTORCD").cast(pl.Utf8),
        pl.col("STATE").cast(pl.Utf8),
    ])


def _read_loan(path: Path, row_limit: Optional[int] = None) -> pl.DataFrame:
    """
    PROC SORT DATA=LOAN.LOAN&REPTMON&NOWK OUT=LOAN; BY ACCTNO NOTENO
    Columns (parquet already renamed):
      ACCTNO NOTENO COMMNO BRANCH BALANCE SECTORCD CUSTCD INTRATE
      APPRLIMT FISSPURP LIABCODE
    NOTE: ACCTYPE='LN' filter is pre-applied during parquet creation.
          If ACCTYPE is still present, it is dropped here as in SAS KEEP list.
    """
    cols = [
        "ACCTNO", "NOTENO", "COMMNO", "BRANCH", "BALANCE",
        "SECTORCD", "CUSTCD", "INTRATE", "APPRLIMT", "FISSPURP", "LIABCODE",
    ]
    df = _read_parquet(path, cols, row_limit)
    return df.with_columns([
        pl.col("ACCTNO").cast(pl.Float64).cast(pl.Int64),
        pl.col("NOTENO").cast(pl.Float64).cast(pl.Int64),
        pl.col("COMMNO").cast(pl.Float64).cast(pl.Int64),
        pl.col("BRANCH").cast(pl.Float64).cast(pl.Int64).cast(pl.Utf8),
        pl.col("BALANCE").cast(pl.Float64),
        pl.col("SECTORCD").cast(pl.Utf8),
        # CUSTCD: SAS numeric -> float -> int -> string  (avoids "78.0" artefacts)
        pl.col("CUSTCD").cast(pl.Float64).cast(pl.Int64).cast(pl.Utf8),
        pl.col("INTRATE").cast(pl.Float64),
        pl.col("APPRLIMT").cast(pl.Float64),
        pl.col("FISSPURP").cast(pl.Utf8),
        pl.col("LIABCODE").cast(pl.Utf8),
    ])


# =============================================================================
# CORE PROCESSING
# =============================================================================
def process_bank(
    bank_name: str,
    config: Dict[str, Path],
    reptmon: str,
    row_limit: Optional[int] = None,
) -> None:
    """
    Process loan-list preparation for a single bank entity (PBB or PIBB).

    Mirrors the SAS logic exactly:
      1. MERGE LOAN(IN=A) LNNOTE(IN=B) BY ACCTNO NOTENO  -> LNOTE (IF ACCTYPE='LN')
      2. SORT LNOTE BY ACCTNO COMMNO
      3. SORT LNCOMM BY ACCTNO COMMNO
      4. MERGE LNOTE(IN=A) LNCOMM(IN=B) BY ACCTNO COMMNO -> NOTE1 (IF A)
      5. NOTE2 = NOTE1 filtered by CUSTCD / SECTORCD
      6. PROC SORT NOTE1 -> output  BY BRANCH FISSPURP CUSTCD ACCTNO
      7. PROC SORT NOTE2 -> output  BY BRANCH SECTORCD CUSTCD ACCTNO
    """
    lnnote_path = config["lnnote"]
    lncomm_path = config["lncomm"]
    loan_path   = config["loan"]
    output_dir  = config["output_dir"]

    for label, p in [("LNNOTE", lnnote_path), ("LNCOMM", lncomm_path), ("LOAN", loan_path)]:
        if not p.exists():
            raise FileNotFoundError(f"[{bank_name}] Missing {label} file: {p}")

    print(f"\n[{bank_name}] Reading inputs ...")
    lnnote_df = _read_lnnote(lnnote_path, row_limit)
    lncomm_df = _read_lncomm(lncomm_path, row_limit)
    loan_df   = _read_loan(loan_path, row_limit)

    print(f"[{bank_name}] LOAN rows  : {len(loan_df):,}")
    print(f"[{bank_name}] LNNOTE rows: {len(lnnote_df):,}")
    print(f"[{bank_name}] LNCOMM rows: {len(lncomm_df):,}")

    # ------------------------------------------------------------------
    # STEP 1 — DATA LNOTE:
    #   MERGE LOAN(IN=A) LNNOTE(IN=B); BY ACCTNO NOTENO; IF ACCTYPE='LN'
    #   KEEP: BANKNO BRANCH ACCTNO NOTENO NAME BALANCE SECTORCD CUSTCD
    #         INTRATE NTBRCH COMMNO LIABCODE APPRLIMT FISSPURP STATE
    #
    # Left join on (ACCTNO, NOTENO): LOAN drives (IN=A).
    # LNNOTE(KEEP=4) has only: ACCTNO NOTENO BANKNO STATE
    #   -> STATE  : shared, LNNOTE wins (last-dataset rule)
    #   -> BANKNO : LNNOTE-only (not on LOAN)
    # All other columns come exclusively from LOAN:
    #   BRANCH, BALANCE, SECTORCD, CUSTCD, INTRATE, COMMNO, LIABCODE,
    #   APPRLIMT, FISSPURP
    # NAME and NTBRCH: present in LNOTE KEEP list but not in either source
    #   after the KEEP restriction. They will be null unless LOAN carries them.
    # ------------------------------------------------------------------
    lnote_df = loan_df.join(
        lnnote_df.rename({"STATE": "STATE_NOTE"}),
        on=["ACCTNO", "NOTENO"],
        how="left",
    )

    # STATE -> LNNOTE wins
    lnote_df = lnote_df.with_columns(
        pl.when(pl.col("STATE_NOTE").is_not_null())
          .then(pl.col("STATE_NOTE"))
          .otherwise(pl.col("STATE"))
          .alias("STATE")
    ).drop("STATE_NOTE")

    # KEEP list for LNOTE
    keep_lnote = [
        "BANKNO", "BRANCH", "ACCTNO", "NOTENO", "NAME", "BALANCE",
        "SECTORCD", "CUSTCD", "INTRATE", "NTBRCH", "COMMNO", "LIABCODE",
        "APPRLIMT", "FISSPURP", "STATE",
    ]
    lnote_df = lnote_df.select([c for c in keep_lnote if c in lnote_df.columns])

    print(f"[{bank_name}] LNOTE rows : {len(lnote_df):,}")

    # ------------------------------------------------------------------
    # STEP 2 — PROC SORT LNOTE BY ACCTNO COMMNO  (implicit; join handles it)
    # STEP 3 — PROC SORT LNCOMM BY ACCTNO COMMNO (implicit)
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # STEP 4 — DATA NOTE1:
    #   MERGE LNOTE(IN=A) LNCOMM(IN=B); BY ACCTNO COMMNO; IF A
    #   KEEP: BANKNO BRANCH ACCTNO NOTENO NAME APPRLIMT BALANCE
    #         SECTORCD CUSTCD STATE INTRATE NTBRCH COMMNO LIABCODE
    #         CCOLLTRL FISSPURP
    #
    # Left join on (ACCTNO, COMMNO): LNOTE drives (IF A).
    # LNCOMM wins for columns it carries:
    #   BRANCH (CMBRCH), SECTORCD (CSECTOR), STATE (CSTATE)
    # CCOLLTRL comes exclusively from LNCOMM.
    # ------------------------------------------------------------------
    note1_df = lnote_df.join(
        lncomm_df.rename({
            "BRANCH"  : "BRANCH_COMM",
            "SECTORCD": "SECTORCD_COMM",
            "STATE"   : "STATE_COMM",
        }),
        on=["ACCTNO", "COMMNO"],
        how="left",
    )

    # LNCOMM wins: overwrite LNOTE values where LNCOMM provides non-null data
    note1_df = note1_df.with_columns([
        # BRANCH -> LNCOMM wins
        pl.when(pl.col("BRANCH_COMM").is_not_null())
          .then(pl.col("BRANCH_COMM"))
          .otherwise(pl.col("BRANCH"))
          .alias("BRANCH"),
        # SECTORCD -> LNCOMM wins
        pl.when(pl.col("SECTORCD_COMM").is_not_null())
          .then(pl.col("SECTORCD_COMM"))
          .otherwise(pl.col("SECTORCD"))
          .alias("SECTORCD"),
        # STATE -> LNCOMM wins
        pl.when(pl.col("STATE_COMM").is_not_null())
          .then(pl.col("STATE_COMM"))
          .otherwise(pl.col("STATE"))
          .alias("STATE"),
    ]).drop(["BRANCH_COMM", "SECTORCD_COMM", "STATE_COMM"])

    # KEEP list for NOTE1
    keep_note1 = [
        "BANKNO", "BRANCH", "ACCTNO", "NOTENO", "NAME", "APPRLIMT", "BALANCE",
        "SECTORCD", "CUSTCD", "STATE", "INTRATE", "NTBRCH", "COMMNO",
        "LIABCODE", "CCOLLTRL", "FISSPURP",
    ]
    note1_df = note1_df.select([c for c in keep_note1 if c in note1_df.columns])

    print(f"[{bank_name}] NOTE1 rows : {len(note1_df):,}")

    # ------------------------------------------------------------------
    # STEP 5 — DATA NOTE2:
    #   SET NOTE1;
    #   IF CUSTCD NOT IN ('77','78','95','96') AND
    #      (SUBSTR(SECTORCD,1,1)='5' OR SECTORCD='8310') THEN OUTPUT;
    # ------------------------------------------------------------------
    sector_col = pl.col("SECTORCD").cast(pl.Utf8)
    note2_df = note1_df.filter(
        (~pl.col("CUSTCD").cast(pl.Utf8).is_in(["77", "78", "95", "96"]))
        & (
            (sector_col.str.slice(0, 1) == "5")
            | (sector_col == "8310")
        )
    )

    print(f"[{bank_name}] NOTE2 rows : {len(note2_df):,}")

    # Free intermediates
    del loan_df, lnnote_df, lncomm_df, lnote_df
    gc.collect()

    # PROC DATASETS LIB=WORK NOLIST; DELETE LNOTE LNCOMM; (implicit in Python)

    # ------------------------------------------------------------------
    # STEP 6 — PROC SORT DATA=NOTE1 OUT=LNLC(I).NOTE1&REPTMON
    #              BY BRANCH FISSPURP CUSTCD ACCTNO
    # STEP 7 — PROC SORT DATA=NOTE2 OUT=LNLC(I).NOTE2&REPTMON
    #              BY BRANCH SECTORCD CUSTCD ACCTNO
    # ------------------------------------------------------------------
    output_dir.mkdir(parents=True, exist_ok=True)

    prefix = "LNLC" if bank_name == "PBB" else "LNLCI"

    note1_out = output_dir / f"{prefix}_NOTE1_{reptmon}.parquet"
    note2_out = output_dir / f"{prefix}_NOTE2_{reptmon}.parquet"

    # Disk-friendly: write unsorted first, lazy-sort, write final
    note1_tmp = output_dir / f"{prefix}_NOTE1_{reptmon}_tmp.parquet"
    note2_tmp = output_dir / f"{prefix}_NOTE2_{reptmon}_tmp.parquet"

    note1_df.write_parquet(note1_tmp)
    note2_df.write_parquet(note2_tmp)

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

    note1_sorted.write_parquet(note1_out)
    note2_sorted.write_parquet(note2_out)

    # Clean up temp files
    note1_tmp.unlink(missing_ok=True)
    note2_tmp.unlink(missing_ok=True)

    # ------------------------------------------------------------------
    # Terminal summary
    # ------------------------------------------------------------------
    print(f"\n[{bank_name}] REPTMON={reptmon}")
    print(f"[{bank_name}] NOTE1 sorted rows : {len(note1_sorted):,}")
    print(f"[{bank_name}] NOTE2 sorted rows : {len(note2_sorted):,}")
    print(f"[{bank_name}] Output -> {note1_out}")
    print(f"[{bank_name}] Output -> {note2_out}")
    print(f"\n[{bank_name}] NOTE1 head:")
    print(note1_sorted.head())
    print(f"\n[{bank_name}] NOTE2 head:")
    print(note2_sorted.head())


# =============================================================================
# MAIN
# =============================================================================
def main() -> None:
    rv      = get_reptdate_values()
    reptmon = rv.reptmon   # zero-padded month e.g. '05'
    nowk    = rv.nowk      # week bucket       e.g. '2' or '4'

    row_limit = _get_row_limit()

    print(f"Report Date : {rv.reptdate}  (REPTMON={reptmon}, NOWK={nowk})")
    if row_limit:
        print(f"Test mode: reading at most {row_limit:,} rows from each input")

    # PBB
    process_bank("PBB",  PBB_CONFIG,  reptmon, row_limit=row_limit)

    # PIBB
    process_bank("PIBB", PIBB_CONFIG, reptmon, row_limit=row_limit)


if __name__ == "__main__":
    main()
