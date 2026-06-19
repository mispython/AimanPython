#!/usr/bin/env python3
"""
File Name   : EIBXLNLC.py
Description : Loan data preparation - merges LNNOTE (combined LNNOTE+LNCOMM
              source) and LOAN datasets to produce NOTE1 (all loans by
              FISSPURP) and NOTE2 (construction/real-estate loans for
              non-individual customers) for both PBB and PIBB.
              Runs at the same frequency as EIBXODLC.py (right after it in
              scheduling):
                - 16th of month -> report date = 15th  (NOWK='2')
                - 1st of month  -> report date = last day of prior month (NOWK='4')
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple

import pandas as pd
import polars as pl

from REPTDATE import get_reptdate_values
from input_date import get_latest_file


# =============================================================================
# PATH CONFIGURATION
# =============================================================================
# # Production Path
# BASE_DIR = Path("/dwh")

BASE_DIR  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS2")
INPUT_DIR = BASE_DIR / "input/prod" / "EIBXLNLC"

OUTPUT_DIR = BASE_DIR / "output" / "EIBXLNLC"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------------------------------
# Only 2 physical inputs for this program:
#   1. LNNOTE  - SHARED single source for both PBB and PIBB. In the original
#                JCL, the "LNNOTE" DD pointed to library SAP.<ENTITY>.MNILN(0),
#                which housed BOTH the LNNOTE dataset (ACCTNO NOTENO BANKNO
#                STATE) and the LNCOMM dataset (ACCTNO COMMNO CCOLLTRL).
#                Migrated as one combined parquet/sas7bdat source carrying an
#                ENTITY_CD column to distinguish PBB vs PIBB rows.
#   2. LOAN / ILOAN - bank-specific loan extract (PBB=LOAN, PIBB=ILOAN),
#                equivalent to SAP.PBB.SASDATA / SAP.PIBB.SASDATA.
#
# NOTE: File-prefix pattern for the shared LNNOTE source ("nt") is assumed
# (no example filename was provided for this input); adjust the prefix below
# if the actual naming convention differs (e.g. nt05226.sas7bdat).
# ----------------------------------------------------------------------------
LNNOTE_PATH = get_latest_file(INPUT_DIR, "nt")    # e.g. nt05226.sas7bdat (shared, ENTITY_CD-tagged)

PBB_CONFIG: Dict[str, Path] = {
    "loan_dir":   get_latest_file(INPUT_DIR, "ln"),   # e.g. ln05126.sas7bdat
    "output_dir": OUTPUT_DIR / "PBB",
}

PIBB_CONFIG: Dict[str, Path] = {
    "loan_dir":   get_latest_file(INPUT_DIR, "iln"),  # e.g. iln05126.sas7bdat
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
# This program no longer reads its own REPTDATE source. It follows the exact
# same biweekly schedule/derivation as EIBXODLC.py (it runs immediately after
# it), so REPTMON / NOWK are obtained from REPTDATE.get_reptdate_values().
# RDATE (DDMMYY8.) and REPTYEAR are not consumed anywhere downstream in this
# program (only REPTMON/NOWK feed the dataset/file naming), so they are not
# carried forward.


def _read_sas7bdat(path: Path) -> pl.DataFrame:
    """Read a .sas7bdat file via pandas and convert to Polars with uppercased columns."""
    pdf = pd.read_sas(str(path), encoding="latin1")
    pdf.columns = [c.upper() for c in pdf.columns]
    return pl.from_pandas(pdf)


def _split_lnnote_by_entity(lnnote_df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Split the shared LNNOTE source into PBB / PIBB subsets.
    ENTITY_CD = 'PIBB' -> PIBB data; ENTITY_CD != 'PIBB' -> PBB data.
    """
    entity = pl.col("ENTITY_CD").cast(pl.Utf8)
    pibb_df = lnnote_df.filter(entity == "PIBB")
    pbb_df  = lnnote_df.filter(entity != "PIBB")
    return pbb_df, pibb_df


def _derive_lnnote_and_lncomm(
    entity_lnnote_df: pl.DataFrame,
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    From the combined LNNOTE source (for one entity), reconstruct the two
    original SAS datasets it represented:
      - LNNOTE dataset : KEEP=ACCTNO NOTENO BANKNO STATE  (BY ACCTNO NOTENO)
      - LNCOMM dataset : ACCTNO COMMNO CCOLLTRL            (BY ACCTNO COMMNO)
    """
    # PROC SORT DATA=LNNOTE.LNNOTE (KEEP=ACCTNO NOTENO BANKNO STATE)
    #    OUT=LNNOTE; BY ACCTNO NOTENO;
    lnnote_df = (
        entity_lnnote_df
        .filter(pl.col("NOTENO").is_not_null())
        .select(["ACCTNO", "NOTENO", "BANKNO", "STATE"])
        .with_columns([
            pl.col("ACCTNO").cast(pl.Float64).cast(pl.Int64),
            pl.col("NOTENO").cast(pl.Float64).cast(pl.Int64),
        ])
        .sort(["ACCTNO", "NOTENO"])
    )

    # PROC SORT DATA=LNNOTE.LNCOMM OUT=LNCOMM; BY ACCTNO COMMNO;
    lncomm_df = (
        entity_lnnote_df
        .filter(pl.col("COMMNO").is_not_null())
        .select(["ACCTNO", "COMMNO", "CCOLLTRL"])
        .with_columns([
            pl.col("ACCTNO").cast(pl.Float64).cast(pl.Int64),
            pl.col("COMMNO").cast(pl.Float64).cast(pl.Int64),
        ])
        .sort(["ACCTNO", "COMMNO"])
    )

    return lnnote_df, lncomm_df


# =============================================================================
# CORE PROCESSING
# =============================================================================
def process_bank(
    bank_name: str,
    entity_lnnote_df: pl.DataFrame,
    loan_path: Path,
    output_dir: Path,
    reptmon: str,
) -> None:
    """
    Process loan-list preparation for a single bank entity (PBB or PIBB).
    """
    if not loan_path.exists():
        raise FileNotFoundError(f"[{bank_name}] Missing loan file: {loan_path}")

    lnnote_df, lncomm_df = _derive_lnnote_and_lncomm(entity_lnnote_df)

    # ------------------------------------------------------------------
    # PROC SORT DATA=LOAN.LOAN&REPTMON&NOWK OUT=LOAN; BY ACCTNO NOTENO
    # ------------------------------------------------------------------
    loan_df = (
        _read_sas7bdat(loan_path)
        .with_columns([
            pl.col("ACCTNO").cast(pl.Float64).cast(pl.Int64),
            pl.col("NOTENO").cast(pl.Float64).cast(pl.Int64),
            pl.col("CUSTCD").cast(pl.Float64).cast(pl.Int64).cast(pl.Utf8),
            pl.col("SECTORCD").cast(pl.Utf8),
        ])
        .sort(["ACCTNO", "NOTENO"])
    )

    # ------------------------------------------------------------------
    # DATA LNOTE: MERGE LOAN(IN=A) LNNOTE(IN=B); BY ACCTNO NOTENO
    # IF ACCTYPE = 'LN'
    # KEEP: BANKNO BRANCH ACCTNO NOTENO NAME BALANCE SECTORCD CUSTCD
    #       INTRATE NTBRCH COMMNO LIABCODE APPRLIMT FISSPURP STATE
    # ------------------------------------------------------------------
    lnote_df = loan_df.join(lnnote_df, on=["ACCTNO", "NOTENO"], how="left", suffix="_NOTE")
    lnote_df = lnote_df.filter(pl.col("ACCTYPE") == "LN")

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

    # PROC SORT DATA=LNOTE; BY ACCTNO COMMNO
    lnote_df = lnote_df.with_columns(
        pl.col("COMMNO").cast(pl.Float64).cast(pl.Int64)
    ).sort(["ACCTNO", "COMMNO"])

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
        & (sector.str.slice(0, 1) == "5") | (sector == "8310")
    )

    # PROC DATASETS LIB=WORK NOLIST; DELETE LNOTE LNCOMM (implicit - not needed in Python)

    # ------------------------------------------------------------------
    # PROC SORT DATA=NOTE1 OUT=LNLC(I).NOTE1&REPTMON; BY BRANCH FISSPURP CUSTCD ACCTNO
    # PROC SORT DATA=NOTE2 OUT=LNLC(I).NOTE2&REPTMON; BY BRANCH SECTORCD CUSTCD ACCTNO
    # ------------------------------------------------------------------
    output_dir.mkdir(parents=True, exist_ok=True)

    note1_sorted = note1_df.sort(["BRANCH", "FISSPURP", "CUSTCD", "ACCTNO"])
    note2_sorted = note2_df.sort(["BRANCH", "SECTORCD", "CUSTCD", "ACCTNO"])

    prefix = "LNLC" if bank_name == "PBB" else "LNLCI"
    note1_out = output_dir / f"{prefix}_NOTE1_{reptmon}.parquet"
    note2_out = output_dir / f"{prefix}_NOTE2_{reptmon}.parquet"

    note1_sorted.write_parquet(note1_out)
    note2_sorted.write_parquet(note2_out)

    # ------------------------------------------------------------------
    # Terminal summary
    # ------------------------------------------------------------------
    print(f"\n[{bank_name}] REPTMON={reptmon}")
    print(f"[{bank_name}] LOAN rows     : {len(loan_df):,}")
    print(f"[{bank_name}] NOTE1 rows    : {len(note1_sorted):,}")
    print(f"[{bank_name}] NOTE2 rows    : {len(note2_sorted):,}")
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

    print(f"Report Date : {rv.reptdate}  (REPTMON={reptmon}, NOWK={nowk})")

    lnnote_all_df = _read_sas7bdat(LNNOTE_PATH)
    pbb_lnnote_df, pibb_lnnote_df = _split_lnnote_by_entity(lnnote_all_df)

    # PBB
    process_bank("PBB", pbb_lnnote_df, PBB_CONFIG["loan_dir"], PBB_CONFIG["output_dir"], reptmon)

    # ******************************************************
    # FOR PIBB
    # ******************************************************
    process_bank("PIBB", pibb_lnnote_df, PIBB_CONFIG["loan_dir"], PIBB_CONFIG["output_dir"], reptmon)


if __name__ == "__main__":
    main()
