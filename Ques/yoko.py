#!/usr/bin/env python3
"""
EIBXLNLC.py
Loan data preparation - merges LNNOTE, LNCOMM, and LOAN datasets
to produce NOTE1 (all loans by FISSPURP) and NOTE2 (construction/
real-estate loans for non-individual customers) for both PBB and PIBB.

Schedule:
  - 16th of month -> report date = 15th  (NOWK='2')
  - 1st of month  -> report date = last day of prior month (NOWK='4')
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional
import os
import pandas as pd
import polars as pl
import duckdb
import gc

from REPTDATE import get_reptdate_values
from input_date import get_latest_file

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
INPUT_DIR  = BASE_DIR / "input/prod" / "EIBXLNLC"
OUTPUT_DIR = BASE_DIR / "output" / "EIBXLNLC"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------------------------------
# LNNOTE  : DSN=SAP.PBB.MNILN(0) / SAP.PIBB.MNILN(0)
#           KEEP=ACCTNO NOTENO BANKNO STATE
# LOAN    : DSN=SAP.PBB.SASDATA / SAP.PIBB.SASDATA  (filename = LOAN&REPTMON&NOWK)
# LNCOMM  : DSN=LNNOTE.LNCOMM   (shared by both PBB and PIBB)
# OUTPUT  : LNLC  = SAP.PBB.LOANLIST.SASDATA  -> NOTE1&REPTMON, NOTE2&REPTMON
#           LNLCI = SAP.PIBB.LOANLIST.SASDATA -> NOTE1&REPTMON, NOTE2&REPTMON
# ----------------------------------------------------------------------------
PBB_CONFIG = {
    "lnnote"    : INPUT_DIR / "lnnote_pbb.sas7bdat",
    "lncomm"    : INPUT_DIR / "enrh_ln_comm.sas7bdat",
    "loan_path" : get_latest_file(BASE_DIR / "input/prod/EIBXODLC", "ln"),
    "output_dir": OUTPUT_DIR / "PBB",
    "prefix"    : "LNLC",
}

PIBB_CONFIG = {
    "lnnote"    : INPUT_DIR / "lnnote_pibb.sas7bdat",
    "lncomm"    : INPUT_DIR / "enrh_ln_comm.sas7bdat",
    "loan_path" : get_latest_file(BASE_DIR / "input/prod/EIBXODLC", "iln"),
    "output_dir": OUTPUT_DIR / "PIBB",
    "prefix"    : "LNLCI",
}

# =============================================================================
# PROC FORMAT (informational)
# =============================================================================
# PROC FORMAT;
#    VALUE BANKFMT 33='PBB'
#                 134='PFB';
# RUN;
BANKFMT = {33: "PBB", 134: "PFB"}


# =============================================================================
# ROW LIMIT HELPER (for testing)
# =============================================================================
def _get_row_limit() -> Optional[int]:
    value = os.environ.get("EIBXLNLC_ROW_LIMIT", "").strip()
    if not value:
        return None
    try:
        n = int(value)
    except ValueError as exc:
        raise ValueError("EIBXLNLC_ROW_LIMIT must be a positive integer or 0") from exc
    return n if n > 0 else None


# =============================================================================
# SAS → PARQUET CACHING READER
# =============================================================================
def _read_sas7bdat(path: Path, row_limit: Optional[int] = None) -> pl.LazyFrame | pl.DataFrame:
    """
    Converts a .sas7bdat file to partitioned Parquet (cached).
    Returns a LazyFrame (full run) or DataFrame (test mode).
    """
    cache_dir = path.parent / "parquet_cache_v5" / path.stem
    cache_dir.mkdir(parents=True, exist_ok=True)

    parquet_files = list(cache_dir.glob("*.parquet"))
    cache_valid = (
        len(parquet_files) > 0
        and max(f.stat().st_mtime for f in parquet_files) >= path.stat().st_mtime
    )

    # CASE 1: cache hit
    if cache_valid and row_limit is None:
        print(f"[CACHE HIT] {path.stem}")
        return pl.scan_parquet(str(cache_dir / "*.parquet"))

    # CASE 2: test / row-limited read
    if row_limit:
        print(f"[TEST MODE] {path.name} (limit={row_limit})")
        reader = pd.read_sas(str(path), encoding="latin1", chunksize=row_limit)
        try:
            pdf = next(reader)
        except StopIteration:
            pdf = pd.DataFrame()
        pdf.columns = [c.upper() for c in pdf.columns]
        return pl.from_pandas(pdf)

    # CASE 3: full conversion
    print(f"[CONVERT] {path.name} → Parquet (chunked)")
    reader = pd.read_sas(str(path), encoding="latin1", chunksize=500_000)
    for i, chunk in enumerate(reader):
        if chunk is None or chunk.empty:
            continue
        chunk.columns = [c.upper() for c in chunk.columns]
        df = pl.from_pandas(chunk).with_columns([
            pl.col(c).cast(pl.Utf8, strict=False) for c in pl.from_pandas(chunk).columns
        ])
        df.write_parquet(cache_dir / f"part-{i:05d}.parquet", compression="zstd")
        print(f"  chunk {i}: {len(df):,} rows")
    print(f"[DONE] Cache: {cache_dir}")
    return pl.scan_parquet(str(cache_dir / "*.parquet"))


# =============================================================================
# DATASET READERS — column names kept exactly as they appear in the SAS source
# =============================================================================

def _read_lnnote(path: Path, row_limit: Optional[int] = None) -> pl.DataFrame:
    """
    PROC SORT DATA=LNNOTE.LNNOTE (KEEP=ACCTNO NOTENO BANKNO STATE)
       OUT=LNNOTE;
       BY ACCTNO NOTENO;

    SAS KEEP restricts to exactly 4 columns.
    """
    raw = _read_sas7bdat(path, row_limit=row_limit)
    df  = raw.collect() if isinstance(raw, pl.LazyFrame) else raw

    keep = [c for c in ["ACCTNO", "NOTENO", "BANKNO", "STATE"] if c in df.columns]
    df = df.select(keep)

    return df.with_columns([
        pl.col("ACCTNO").cast(pl.Float64).cast(pl.Int64),
        pl.col("NOTENO").cast(pl.Float64).cast(pl.Int64),
    ])


def _read_lncomm(path: Path, row_limit: Optional[int] = None) -> pl.DataFrame:
    """
    PROC SORT DATA=LNNOTE.LNCOMM OUT=LNCOMM;
       BY ACCTNO COMMNO;

    No KEEP= in SAS, so all columns are available.
    Only ACCTNO, COMMNO, and CCOLLTRL are referenced in NOTE1 KEEP=.
    """
    raw = _read_sas7bdat(path, row_limit=row_limit)
    df  = raw.collect() if isinstance(raw, pl.LazyFrame) else raw

    keep = [c for c in ["ACCTNO", "COMMNO", "CCOLLTRL"] if c in df.columns]
    df = df.select(keep)

    return df.with_columns([
        pl.col("ACCTNO").cast(pl.Float64).cast(pl.Int64),
        pl.col("COMMNO").cast(pl.Float64).cast(pl.Int64),
    ])


def _read_loan(path: Path, row_limit: Optional[int] = None) -> pl.DataFrame:
    """
    PROC SORT DATA=LOAN.LOAN&REPTMON&NOWK OUT=LOAN;
       BY ACCTNO NOTENO;

    All columns needed for LNOTE KEEP= plus ACCTYPE filter.
    Column name aliases (SAS source name -> canonical):
      SECTOR   -> SECTORCD
      CUSTCODE -> CUSTCD
      STATECD  -> STATE   (if present; original name may vary)
    """
    raw = _read_sas7bdat(path, row_limit=row_limit)
    df  = raw.collect() if isinstance(raw, pl.LazyFrame) else raw

    # Normalise column names that differ between source systems
    rename_map = {}
    if "SECTOR"   in df.columns and "SECTORCD" not in df.columns:
        rename_map["SECTOR"]   = "SECTORCD"
    if "CUSTCODE" in df.columns and "CUSTCD"   not in df.columns:
        rename_map["CUSTCODE"] = "CUSTCD"
    if "STATECD"  in df.columns and "STATE"    not in df.columns:
        rename_map["STATECD"]  = "STATE"
    if rename_map:
        df = df.rename(rename_map)

    want = ["ACCTNO", "NOTENO", "BRANCH", "BALANCE", "SECTORCD", "CUSTCD",
            "INTRATE", "COMMNO", "LIABCODE", "APPRLIMT", "FISSPURP", "STATE",
            "ACCTYPE", "NAME", "NTBRCH"]
    df = df.select([c for c in want if c in df.columns])

    return df.with_columns([
        pl.col("ACCTNO").cast(pl.Float64).cast(pl.Int64),
        pl.col("NOTENO").cast(pl.Float64).cast(pl.Int64),
        pl.col("COMMNO").cast(pl.Float64).cast(pl.Int64),
        pl.col("CUSTCD").cast(pl.Utf8),
        pl.col("SECTORCD").cast(pl.Utf8),
    ])


# =============================================================================
# CORE PROCESSING — mirrors SAS DATA steps exactly
# =============================================================================
def process_bank(
    bank_name : str,
    config    : dict,
    reptmon   : str,
    lncomm_df : Optional[pl.DataFrame] = None,
    row_limit : Optional[int] = None,
) -> None:
    """
    Replicates the SAS batch exactly for one bank entity (PBB or PIBB).

    SAS flow:
    ─────────
    PROC SORT LNNOTE  KEEP=ACCTNO NOTENO BANKNO STATE   BY ACCTNO NOTENO
    PROC SORT LOAN                                       BY ACCTNO NOTENO
    DATA LNOTE  = MERGE LOAN(A) LNNOTE(B) BY ACCTNO NOTENO; IF ACCTYPE='LN'
                  KEEP: BANKNO BRANCH ACCTNO NOTENO NAME BALANCE SECTORCD CUSTCD
                        INTRATE NTBRCH COMMNO LIABCODE APPRLIMT FISSPURP STATE
                  (LNNOTE is dataset B → its BANKNO and STATE overwrite LOAN's)

    PROC SORT LNCOMM                                     BY ACCTNO COMMNO
    DATA NOTE1  = MERGE LNOTE(A) LNCOMM(B) BY ACCTNO COMMNO; IF A
                  KEEP: BANKNO BRANCH ACCTNO NOTENO NAME APPRLIMT BALANCE
                        SECTORCD CUSTCD STATE INTRATE NTBRCH COMMNO LIABCODE
                        CCOLLTRL FISSPURP
                  (LNCOMM is dataset B → its CCOLLTRL is added;
                   LNCOMM has no other cols that clash with LNOTE's KEEP list
                   because LNCOMM KEEP is only ACCTNO COMMNO CCOLLTRL)

    DATA NOTE2  = SET NOTE1
                  IF CUSTCD NOT IN ('77','78','95','96')
                     AND (SUBSTR(SECTORCD,1,1)='5' OR SECTORCD='8310')

    PROC SORT NOTE1 OUT=LNLC(I).NOTE1&REPTMON  BY BRANCH FISSPURP CUSTCD ACCTNO
    PROC SORT NOTE2 OUT=LNLC(I).NOTE2&REPTMON  BY BRANCH SECTORCD CUSTCD ACCTNO
    """

    lnnote_path = config["lnnote"]
    lncomm_path = config["lncomm"]
    loan_path   = config["loan_path"]
    output_dir  = Path(config["output_dir"])
    prefix      = config["prefix"]

    for label, p in [("LNNOTE", lnnote_path), ("LNCOMM", lncomm_path), ("LOAN", loan_path)]:
        if not Path(p).exists():
            raise FileNotFoundError(f"[{bank_name}] Missing {label}: {p}")

    # ── read inputs ──────────────────────────────────────────────────────────
    lnnote_df = _read_lnnote(lnnote_path, row_limit=row_limit)
    if lncomm_df is None:
        lncomm_df = _read_lncomm(lncomm_path, row_limit=row_limit)
    loan_df = _read_loan(loan_path, row_limit=row_limit)

    print(f"[{bank_name}] LOAN   rows : {len(loan_df):,}")
    print(f"[{bank_name}] LNNOTE rows : {len(lnnote_df):,}")
    print(f"[{bank_name}] LNCOMM rows : {len(lncomm_df):,}")

    # ── DATA LNOTE ───────────────────────────────────────────────────────────
    # MERGE LOAN(IN=A) LNNOTE(IN=B) BY ACCTNO NOTENO; IF ACCTYPE='LN'
    #
    # LNNOTE KEEP = ACCTNO NOTENO BANKNO STATE only.
    # Shared cols with LNOTE's target KEEP list: BANKNO, STATE.
    # LNNOTE(B) wins on BANKNO and STATE (last-dataset rule).
    # All other cols in LNOTE KEEP come exclusively from LOAN.
    #
    # "IF ACCTYPE='LN'" → only LOAN rows where ACCTYPE='LN' are kept.
    # This is an inner-side filter: rows in LOAN not matching are dropped;
    # rows in LOAN with ACCTYPE='LN' but no LNNOTE match are still kept
    # (left join semantics, B provides BANKNO/STATE or leaves them null).

    if "ACCTYPE" in loan_df.columns:
        loan_ln = loan_df.filter(pl.col("ACCTYPE") == "LN")
    else:
        loan_ln = loan_df  # ACCTYPE absent — treat all rows as LN

    # Left join: LOAN is driving (A), LNNOTE enriches BANKNO + STATE
    lnote_df = loan_ln.join(lnnote_df, on=["ACCTNO", "NOTENO"], how="left", suffix="_B")

    # LNNOTE(B) wins: overwrite LOAN's BANKNO and STATE when LNNOTE has a value
    for col in ["BANKNO", "STATE"]:
        b_col = col + "_B"
        if b_col in lnote_df.columns:
            lnote_df = lnote_df.with_columns(
                pl.when(pl.col(b_col).is_not_null())
                  .then(pl.col(b_col))
                  .otherwise(pl.col(col) if col in lnote_df.columns else pl.lit(None))
                  .alias(col)
            ).drop(b_col)

    keep_lnote = ["BANKNO", "BRANCH", "ACCTNO", "NOTENO", "NAME", "BALANCE",
                  "SECTORCD", "CUSTCD", "INTRATE", "NTBRCH", "COMMNO",
                  "LIABCODE", "APPRLIMT", "FISSPURP", "STATE"]
    lnote_df = lnote_df.select([c for c in keep_lnote if c in lnote_df.columns])

    del loan_df, loan_ln, lnnote_df
    gc.collect()

    print(f"[{bank_name}] LNOTE  rows : {len(lnote_df):,}")

    # ── DATA NOTE1 ───────────────────────────────────────────────────────────
    # MERGE LNOTE(IN=A) LNCOMM(IN=B) BY ACCTNO COMMNO; IF A
    #
    # LNCOMM KEEP (after _read_lncomm) = ACCTNO COMMNO CCOLLTRL only.
    # No column clash with LNOTE — LNCOMM only adds CCOLLTRL.
    # IF A → all LNOTE rows kept; unmatched LNCOMM rows discarded.
    # No CUSTCD filter here — all customer codes pass through to NOTE1.

    note1_df = lnote_df.join(lncomm_df, on=["ACCTNO", "COMMNO"], how="left", suffix="_B")
    # Drop any accidental _B duplicates (should be none given LNCOMM's 3-col schema)
    note1_df = note1_df.drop([c for c in note1_df.columns if c.endswith("_B")])

    keep_note1 = ["BANKNO", "BRANCH", "ACCTNO", "NOTENO", "NAME", "APPRLIMT",
                  "BALANCE", "SECTORCD", "CUSTCD", "STATE", "INTRATE", "NTBRCH",
                  "COMMNO", "LIABCODE", "CCOLLTRL", "FISSPURP"]
    note1_df = note1_df.select([c for c in keep_note1 if c in note1_df.columns])

    del lnote_df, lncomm_df
    gc.collect()

    print(f"[{bank_name}] NOTE1  rows : {len(note1_df):,}")

    # ── DATA NOTE2 ───────────────────────────────────────────────────────────
    # SET NOTE1;
    # IF CUSTCD NOT IN ('77','78','95','96') AND
    #    (SUBSTR(SECTORCD,1,1) = '5' OR SECTORCD = '8310') THEN OUTPUT;

    sector = pl.col("SECTORCD").cast(pl.Utf8)
    note2_df = note1_df.filter(
        (~pl.col("CUSTCD").cast(pl.Utf8).is_in(["77", "78", "95", "96"]))
        & ((sector.str.slice(0, 1) == "5") | (sector == "8310"))
    )

    print(f"[{bank_name}] NOTE2  rows : {len(note2_df):,}")

    # ── PROC DATASETS LIB=WORK NOLIST; DELETE LNOTE LNCOMM; ─────────────────
    # (handled by del statements above)

    # ── PROC SORT + write outputs ─────────────────────────────────────────────
    # PROC SORT DATA=NOTE1 OUT=LNLC(I).NOTE1&REPTMON; BY BRANCH FISSPURP CUSTCD ACCTNO
    # PROC SORT DATA=NOTE2 OUT=LNLC(I).NOTE2&REPTMON; BY BRANCH SECTORCD CUSTCD ACCTNO

    output_dir.mkdir(parents=True, exist_ok=True)

    note1_out = output_dir / f"{prefix}_NOTE1_{reptmon}.parquet"
    note2_out = output_dir / f"{prefix}_NOTE2_{reptmon}.parquet"

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

    note1_tmp.unlink(missing_ok=True)
    note2_tmp.unlink(missing_ok=True)

    print(f"\n[{bank_name}] REPTMON = {reptmon}")
    print(f"[{bank_name}] NOTE1 rows : {len(note1_sorted):,}  -> {note1_out}")
    print(f"[{bank_name}] NOTE2 rows : {len(note2_sorted):,}  -> {note2_out}")
    print(f"\n[{bank_name}] NOTE1 sample:\n{note1_sorted.head()}")
    print(f"\n[{bank_name}] NOTE2 sample:\n{note2_sorted.head()}")


# =============================================================================
# MAIN
# =============================================================================
def main() -> None:
    rv      = get_reptdate_values()
    reptmon = rv.reptmon   # zero-padded month, e.g. '05'
    nowk    = rv.nowk      # week bucket,         e.g. '2' or '4'

    row_limit = _get_row_limit()

    print(f"Report Date : {rv.reptdate}  (REPTMON={reptmon}, NOWK={nowk})")
    if row_limit:
        print(f"[TEST MODE] row limit = {row_limit:,} per file")

    # LNCOMM is the same physical file for both PBB and PIBB
    shared_lncomm: Optional[pl.DataFrame] = None
    if PBB_CONFIG["lncomm"] == PIBB_CONFIG["lncomm"]:
        lncomm_path = Path(PBB_CONFIG["lncomm"])
        if not lncomm_path.exists():
            raise FileNotFoundError(f"Missing shared LNCOMM: {lncomm_path}")
        shared_lncomm = _read_lncomm(lncomm_path, row_limit=row_limit)

    process_bank("PBB",  PBB_CONFIG,  reptmon, lncomm_df=shared_lncomm, row_limit=row_limit)
    process_bank("PIBB", PIBB_CONFIG, reptmon, lncomm_df=shared_lncomm, row_limit=row_limit)


if __name__ == "__main__":
    main()
