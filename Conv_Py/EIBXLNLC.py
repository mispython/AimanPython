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
from typing import Dict

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
# 3 inputs per bank entity, all .sas7bdat:
#   1. LNNOTE - KEEP=ACCTNO NOTENO BANKNO STATE  (SAP.<ENTITY>.MNILN(0) - LNNOTE)
#   2. LNCOMM - ACCTNO COMMNO CCOLLTRL            (SAP.<ENTITY>.MNILN(0) - LNCOMM)
#   3. LOAN / ILOAN - loan extract (SAP.PBB.SASDATA / SAP.PIBB.SASDATA)
#
# NOTE: No example filenames were provided for LNNOTE/LNCOMM. Prefixes below
# follow the same PBB/PIBB pairing convention already used for LOAN/ILOAN in
# EIBXODLC.py (plain prefix for PBB, 'i'-prefixed for PIBB). Adjust if the
# actual naming convention differs.
# ----------------------------------------------------------------------------
PBB_CONFIG: Dict[str, Path] = {
    "lnnote":     get_latest_file(INPUT_DIR, "nt"),    # e.g. nt05226.sas7bdat
    "lncomm":     get_latest_file(INPUT_DIR, "cm"),    # e.g. cm05226.sas7bdat
    "loan_dir":   get_latest_file(INPUT_DIR, "ln"),    # e.g. ln05126.sas7bdat
    "output_dir": OUTPUT_DIR / "PBB",
}

PIBB_CONFIG: Dict[str, Path] = {
    "lnnote":     get_latest_file(INPUT_DIR, "int"),   # e.g. int05226.sas7bdat
    "lncomm":     get_latest_file(INPUT_DIR, "icm"),   # e.g. icm05226.sas7bdat
    "loan_dir":   get_latest_file(INPUT_DIR, "iln"),   # e.g. iln05126.sas7bdat
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


def _read_sas7bdat(path: Path) -> pl.DataFrame:
    """Read a .sas7bdat file via pandas and convert to Polars with uppercased columns."""
    pdf = pd.read_sas(str(path), encoding="latin1")
    pdf.columns = [c.upper() for c in pdf.columns]
    return pl.from_pandas(pdf)


def _read_lnnote(lnnote_path: Path) -> pl.DataFrame:
    """
    PROC SORT DATA=LNNOTE.LNNOTE (KEEP=ACCTNO NOTENO BANKNO STATE)
       OUT=LNNOTE; BY ACCTNO NOTENO;
    """
    return (
        _read_sas7bdat(lnnote_path)
        .select(["ACCTNO", "NOTENO", "BANKNO", "STATE"])
        .with_columns([
            pl.col("ACCTNO").cast(pl.Float64).cast(pl.Int64),
            pl.col("NOTENO").cast(pl.Float64).cast(pl.Int64),
        ])
        .sort(["ACCTNO", "NOTENO"])
    )


def _read_lncomm(lncomm_path: Path) -> pl.DataFrame:
    """
    PROC SORT DATA=LNNOTE.LNCOMM OUT=LNCOMM; BY ACCTNO COMMNO;
    """
    return (
        _read_sas7bdat(lncomm_path)
        .select(["ACCTNO", "COMMNO", "CCOLLTRL"])
        .with_columns([
            pl.col("ACCTNO").cast(pl.Float64).cast(pl.Int64),
            pl.col("COMMNO").cast(pl.Float64).cast(pl.Int64),
        ])
        .sort(["ACCTNO", "COMMNO"])
    )


def _read_loan(loan_path: Path) -> pl.DataFrame:
    """
    PROC SORT DATA=LOAN.LOAN&REPTMON&NOWK OUT=LOAN; BY ACCTNO NOTENO;
    Original LOAN columns 'SECTOR' / 'CUSTCODE' renamed to 'SECTORCD' / 'CUSTCD'.
    """
    return (
        _read_sas7bdat(loan_path)
        .rename({"SECTOR": "SECTORCD", "CUSTCODE": "CUSTCD"})
        .with_columns([
            pl.col("ACCTNO").cast(pl.Float64).cast(pl.Int64),
            pl.col("NOTENO").cast(pl.Float64).cast(pl.Int64),
            pl.col("CUSTCD").cast(pl.Float64).cast(pl.Int64).cast(pl.Utf8),
            pl.col("SECTORCD").cast(pl.Utf8),
        ])
        .sort(["ACCTNO", "NOTENO"])
    )


# =============================================================================
# CORE PROCESSING
# =============================================================================
def process_bank(
    bank_name: str,
    config: Dict[str, Path],
    reptmon: str,
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

    lnnote_df = _read_lnnote(lnnote_path)
    lncomm_df = _read_lncomm(lncomm_path)
    loan_df   = _read_loan(loan_path)

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
        & ((sector.str.slice(0, 1) == "5") | (sector == "8310"))
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

    print(f"Report Date : {rv.reptdate}  (REPTMON={reptmon}, NOWK={nowk})")

    # PBB
    process_bank("PBB", PBB_CONFIG, reptmon)

    # ******************************************************
    # FOR PIBB
    # ******************************************************
    process_bank("PIBB", PIBB_CONFIG, reptmon)


if __name__ == "__main__":
    main()
