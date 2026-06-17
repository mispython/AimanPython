#!/usr/bin/env python3
"""
File Name   : EIBXODLC.py
Description : Overdraft Loan Classification outputs for PBB and PIBB.
              Runs biweekly:
                - 16th of month  → report date = 15th  (NOWK='2')
                - 1st of month   → report date = last day of prior month (NOWK='4')
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

# OUTPUT_DIR = Path("/host/mis/output/report") / "EIBXODLC"
# OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# PBB_CONFIG: Dict[str, Path] = {
#     "deposit_current" : get_latest_file(BASE_DIR / "dp_ca", "ca"),       # File name example - ca05226.sas7bdat
#     "loan_dir"        : get_latest_file(BASE_DIR / "ln_ln", "ln"),       # File name example - ln05126.sas7bdat
#     "limit_dir"       : get_latest_file(BASE_DIR / "dp_lm", "lm"),       # File name example - lm05226.sas7bdat
#     "output_dir"      : OUTPUT_DIR / "PBB",
# }

# PIBB_CONFIG: Dict[str, Path] = {
#     "deposit_current" : get_latest_file(BASE_DIR / "idp_ca", "ica"),     # File name example - ica05226.sas7bdat
#     "loan_dir"        : get_latest_file(BASE_DIR / "iln_ln", "iln"),     # File name example - iln05126.sas7bdat
#     "limit_dir"       : get_latest_file(BASE_DIR / "idp_lm", "ilm"),     # File name example - ilm05226.sas7bdat
#     "output_dir"      : OUTPUT_DIR / "PIBB",
# }

# Testing Path
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS2")
INPUT_DIR  = BASE_DIR / "input/prod" / "EIBXODLC"
OUTPUT_DIR = BASE_DIR / "output" / "EIBXODLC"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PBB_CONFIG: Dict[str, Path] = {
    "deposit_current" : get_latest_file(INPUT_DIR, "ca"),       # File name example - ca05226.sas7bdat
    "loan_dir"        : get_latest_file(INPUT_DIR, "ln"),       # File name example - ln05126.sas7bdat
    "limit_dir"       : get_latest_file(INPUT_DIR, "lm"),       # File name example - lm05226.sas7bdat
    "output_dir"      : OUTPUT_DIR / "PBB",
}

PIBB_CONFIG: Dict[str, Path] = {
    "deposit_current" : get_latest_file(INPUT_DIR, "ica"),     # File name example - ica05226.sas7bdat
    "loan_dir"        : get_latest_file(INPUT_DIR, "iln"),     # File name example - iln05126.sas7bdat
    "limit_dir"       : get_latest_file(INPUT_DIR, "ilm"),     # File name example - ilm05226.sas7bdat
    "output_dir"      : OUTPUT_DIR / "PIBB",
}


# =============================================================================
# PROC FORMAT (informational – not used in output columns)
# =============================================================================
# PROC FORMAT;
#    VALUE BANKFMT 33='PBB'
#                 134='PFB';
# RUN;


# =============================================================================
# REPORT DATE DERIVATION
# =============================================================================
# Biweekly run schedule:
#   Run on the 16th  → reptdate = 15th  → NOWK = '2'
#   Run on the  1st  → reptdate = last day of prior month → NOWK = '4'
#
# get_reptdate_values() computes reptdate = today() - 1 day, then assigns NOWK:
#   day 1-8   → NOWK='1'
#   day 9-15  → NOWK='2'
#   day 16-22 → NOWK='3'
#   day 23+   → NOWK='4'
#
# When run on the 16th  → reptdate=15th → NOWK='2'  ✓
# When run on the  1st  → reptdate=last day of prior month (28/29/30/31) → NOWK='4'  ✓


# Production USE
def _read_sas7bdat(path: Path) -> pl.DataFrame:
    """Read a .sas7bdat file via pandas and convert to Polars with uppercased columns."""
    pdf = pd.read_sas(str(path), encoding="latin1")
    pdf.columns = [c.upper() for c in pdf.columns]
    return pl.from_pandas(pdf)


# # Testing USE
# def _read_sas7bdat(path: Path, limit: int | None = None) -> pl.DataFrame:
#     """Read SAS file and optionally limit rows for testing."""

#     if not path.exists():
#         raise FileNotFoundError(f"Missing required input file: {path}")

#     # Read SAS file
#     df = pd.read_sas(
#         path,
#         format="sas7bdat",
#         encoding="latin1",
#     )

#     # Standardise column names (SAS -> Python consistency)
#     df.columns = [str(c).upper().strip() for c in df.columns]

#     # Testing mode (limit rows)
#     if limit is not None:
#         df = df.head(limit)

#     return pl.from_pandas(df)


# def _build_loan_path(loan_dir: Path, reptmon: str, nowk: str) -> Path:
#     return loan_dir / f"loan{reptmon}{nowk}.sas7bdat"


# =============================================================================
# LIMIT SLOT PIVOT (LMTCOLL → COLL1..COLL5)
# =============================================================================

def _build_coll_slots(limit_df: pl.DataFrame) -> pl.DataFrame:
    """Pivot LMTCOLL from limit file into COLL1..COLL5 per ACCTNO.

    Replicates the RCNT / ODS1..ODS5 / ODMERG pattern from EIBMODLM:
      - Filter: APPRLIMT > 1 AND LMTTYPE IN ('Y', 'A')
      - Stable-sort by ACCTNO preserving original file order
      - Assign RCNT (1-based row number within each ACCTNO group, capped at 5)
      - Pivot LMTCOLL for each RCNT slot into COLL1..COLL5 columns
    """
    pdf = limit_df.to_pandas()

    # Preserve original file order for stable intra-group ordering (mirrors SAS PROC SORT stable sort)
    pdf["_ROW_NUM"] = range(len(pdf))

    # Filter: APPRLIMT > 1 AND LMTTYPE IN ('Y', 'A')
    pdf = pdf[
        (pdf["APPRLIMT"] > 1) &
        (pdf["LMTTYPE"].isin(["Y", "A"]))
    ].copy()

    # Sort by ACCTNO then original row position (stable, mirrors SAS PROC SORT)
    pdf = pdf.sort_values(["ACCTNO", "_ROW_NUM"], kind="stable").reset_index(drop=True)

    # Assign RCNT: 1-based row counter within each ACCTNO group
    pdf["RCNT"] = pdf.groupby("ACCTNO", sort=False).cumcount() + 1

    # Keep only slots 1–5
    pdf = pdf[pdf["RCNT"] <= 5].copy()

    # Pivot LMTCOLL into COLL1..COLL5 per ACCTNO
    pivot = (
        pdf[["ACCTNO", "RCNT", "LMTCOLL"]]
        .copy()
        .assign(col_name=lambda d: "COLL" + d["RCNT"].astype(str))
        .pivot_table(index="ACCTNO", columns="col_name", values="LMTCOLL", aggfunc="first")
        .reset_index()
    )
    pivot.columns.name = None

    # Ensure all COLL1..COLL5 columns exist even if some slots are absent in data
    for n in range(1, 6):
        col = f"COLL{n}"
        if col not in pivot.columns:
            pivot[col] = ""
        else:
            pivot[col] = pivot[col].fillna("")

    return pl.from_pandas(pivot[["ACCTNO", "COLL1", "COLL2", "COLL3", "COLL4", "COLL5"]])


# =============================================================================
# CORE PROCESSING
# =============================================================================
def process_bank(
    bank_name: str,
    config: Dict[str, Path],
    reptmon: str,
    nowk: str,
) -> None:
    """
    Process overdraft classification for a single bank entity (PBB or PIBB).

    SAS pipeline:
      1. PROC SORT LOAN dataset → keep ACCTNO, SECTORCD, FISSPURP
      2. DATA ODRAFT  – filter DEPOSIT.CURRENT: CURBAL < 0 AND CUSTCODE NE 81
      3. DATA ODRAFT1 – inner join ODRAFT + LOAN on ACCTNO
      4. DATA ODRAFT2 – filter ODRAFT1: NON-INDIVIDUAL & (SECTORCD starts '5' OR = '8310')
      5. PROC SORT ODRAFT1 → output ODRAF1{reptmon}  by BRANCH FISSPURP ACCTNO
      6. PROC SORT ODRAFT2 → output ODRAF2{reptmon}  by BRANCH SECTORCD ACCTNO
      7. Join COLL1..COLL5 from limit file (pivoted from LMTCOLL via RCNT slots)
    """

    deposit_path = config["deposit_current"]
    loan_dir     = config["loan_dir"]
    limit_dir    = config["limit_dir"]
    output_dir   = config["output_dir"]

    if not deposit_path.exists():
        raise FileNotFoundError(f"[{bank_name}] Missing deposit file : {deposit_path}")
    if not loan_dir.exists():
        raise FileNotFoundError(f"[{bank_name}] Missing loan file    : {loan_dir}")
    if not limit_dir.exists():
        raise FileNotFoundError(f"[{bank_name}] Missing limit file   : {limit_dir}")

    # ------------------------------------------------------------------
    # Load inputs
    # DEPOSIT.CURRENT  → SAP.PBB.MNITB(0)  or  SAP.PIBB.MNITB(0)
    # LOAN dataset     → SAP.PBB.SASDATA   or  SAP.PIBB.SASDATA
    # LIMIT dataset    → overdraft limit file (lm / ilm)
    # ------------------------------------------------------------------
    # Production USE
    deposit_df = (
        _read_sas7bdat(deposit_path)
        .rename({"CUSTCODE": "CUSTCD"})
        .with_columns(
            pl.col("CUSTCD").cast(pl.Float64).cast(pl.Int64).cast(pl.Utf8)
        )
    )

    loan_df = (
        _read_sas7bdat(loan_dir)
        .rename({"SECTOR": "SECTORCD"})
        .with_columns(
            pl.col("SECTORCD").cast(pl.Utf8)
        )
        .select(["ACCTNO", "SECTORCD", "FISSPURP"])
    )

    limit_df = _read_sas7bdat(limit_dir)

    # # Testing USE
    # deposit_df = _read_sas7bdat(deposit_path, limit=1000)

    # loan_df = (
    #     _read_sas7bdat(loan_dir, limit=1000)
    #     .select(["ACCTNO", "SECTORCD", "FISSPURP"])
    # )

    # limit_df = _read_sas7bdat(limit_dir, limit=1000)

    # ------------------------------------------------------------------
    # Build COLL1..COLL5 lookup from limit file
    # Pivot LMTCOLL per RCNT slot into wide format keyed on ACCTNO
    # ------------------------------------------------------------------
    coll_slots = _build_coll_slots(limit_df)

    # ------------------------------------------------------------------
    # DATA ODRAFT
    # SET DEPOSIT.CURRENT;
    # IF CURBAL < 0 AND CUSTCODE NE 81;
    # BALANCE = (-1)*CURBAL;
    # ------------------------------------------------------------------
    odraft = deposit_df.filter(
        (pl.col("CURBAL") < 0) &
        (pl.col("CUSTCD") != "81")
    ).with_columns(
        (pl.col("CURBAL") * -1).alias("BALANCE")
    )

    # ------------------------------------------------------------------
    # DATA ODRAFT1
    # MERGE ODRAFT(IN=A) LOAN(IN=B); BY ACCTNO; IF A AND B;
    # (inner join)
    # ------------------------------------------------------------------
    odraft1 = odraft.join(loan_df, on="ACCTNO", how="inner")

    # ------------------------------------------------------------------
    # Join COLL1..COLL5 onto ODRAFT1 (left join — unmatched accounts get blank COLLn)
    # ------------------------------------------------------------------
    odraft1 = odraft1.join(coll_slots, on="ACCTNO", how="left").with_columns([
        pl.col("COLL1").fill_null(""),
        pl.col("COLL2").fill_null(""),
        pl.col("COLL3").fill_null(""),
        pl.col("COLL4").fill_null(""),
        pl.col("COLL5").fill_null(""),
    ])

    # ------------------------------------------------------------------
    # DATA ODRAFT2
    # IF CUSTCD NOT IN ('77','78','95','96') AND
    #    (SUBSTR(SECTORCD,1,1) = '5' OR SECTORCD = '8310')
    # ------------------------------------------------------------------
    sector = pl.col("SECTORCD").cast(pl.Utf8)
    odraft2 = odraft1.filter(
        (~pl.col("CUSTCD").is_in(["77", "78", "95", "96"]))
        & (sector.str.starts_with("5") | (sector == "8310"))
    )

    # ------------------------------------------------------------------
    # Output
    # PBB  : ODLC.ODRAF1{reptmon}  / ODLC.ODRAF2{reptmon}
    # PIBB : ODLCI.ODRAF1{reptmon} / ODLCI.ODRAF2{reptmon}
    # ------------------------------------------------------------------
    output_dir.mkdir(parents=True, exist_ok=True)

    if bank_name == "PBB":
        out1_name = f"ODLC_OVERDRAFT1_{reptmon}.parquet"
        out2_name = f"ODLC_OVERDRAFT2_{reptmon}.parquet"
    else:  # PIBB
        out1_name = f"ODLCI_OVERDRAFT1_{reptmon}.parquet"
        out2_name = f"ODLCI_OVERDRAFT2_{reptmon}.parquet"

    out1_path = output_dir / out1_name
    out2_path = output_dir / out2_name

    # PROC SORT ODRAFT1 OUT=ODLC.ODRAF1{reptmon} BY BRANCH FISSPURP ACCTNO
    odraft1.sort(["BRANCH", "FISSPURP", "ACCTNO"]).write_parquet(out1_path)

    # PROC SORT ODRAFT2 OUT=ODLC.ODRAF2{reptmon} BY BRANCH SECTORCD ACCTNO
    odraft2.sort(["BRANCH", "SECTORCD", "ACCTNO"]).write_parquet(out2_path)

    # ------------------------------------------------------------------
    # Terminal summary
    # ------------------------------------------------------------------
    print(f"\n[{bank_name}] REPTMON={reptmon}  NOWK={nowk}")
    print(f"[{bank_name}] ODRAFT rows          : {len(odraft):,}")
    print(f"[{bank_name}] ODRAFT1 (inner join) : {len(odraft1):,}")
    print(f"[{bank_name}] ODRAFT2 (sector flt) : {len(odraft2):,}")
    print(f"[{bank_name}] COLL slots matched   : {odraft1.filter(pl.col('COLL1') != '').height:,} / {len(odraft1):,}")
    print(f"[{bank_name}] Output → {out1_path}")
    print(f"[{bank_name}] Output → {out2_path}")


# =============================================================================
# MAIN
# =============================================================================
def main() -> None:
    rv = get_reptdate_values()

    reptmon = rv.reptmon   # zero-padded month  e.g. '05'
    nowk    = rv.nowk      # week bucket        e.g. '2' or '4'

    print(f"Report Date : {rv.reptdate}  (REPTMON={reptmon}, NOWK={nowk})")

    # PBB
    process_bank("PBB",  PBB_CONFIG,  reptmon, nowk)

    # ******************************************************
    # FOR PIBB
    # ******************************************************
    process_bank("PIBB", PIBB_CONFIG, reptmon, nowk)


if __name__ == "__main__":
    main()
