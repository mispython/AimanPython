#!/usr/bin/env python3
"""
File Name   : EIBXODLC
Description : Overdraft Loan Classification outputs for PBB and PIBB.
              Runs biweekly:
                - 16th of month  → report date = 15th  (NOWK='2')
                - 1st of month   → report date = last day of prior month (NOWK='4')
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict

import duckdb
import polars as pl

from REPTDATE import get_reptdate_values


# =============================================================================
# PATH CONFIGURATION
# =============================================================================
SCRIPT_DIR = Path(__file__).resolve().parent

BASE_INPUT_DIR  = SCRIPT_DIR / "input"  / "EIBXODLC"
BASE_OUTPUT_DIR = SCRIPT_DIR / "output" / "EIBXODLC"

PBB_CONFIG: Dict[str, Path] = {
    "deposit_current" : BASE_INPUT_DIR / "pbb" / "deposit_current.parquet",
    "loan_dir"        : BASE_INPUT_DIR / "pbb" / "loan",
    "output_dir"      : BASE_OUTPUT_DIR / "pbb",
}

PIBB_CONFIG: Dict[str, Path] = {
    "deposit_current" : BASE_INPUT_DIR / "pibb" / "deposit_current.parquet",
    "loan_dir"        : BASE_INPUT_DIR / "pibb" / "loan",
    "output_dir"      : BASE_OUTPUT_DIR / "pibb",
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


def _read_sas7bdat(path: Path) -> pl.DataFrame:
    """Read a .sas7bdat file via pandas and convert to Polars with uppercased columns."""
    pdf = pd.read_sas(str(path), encoding="latin1")
    pdf.columns = [c.upper() for c in pdf.columns]
    return pl.from_pandas(pdf)


def _build_loan_path(loan_dir: Path, reptmon: str, nowk: str) -> Path:
    return loan_dir / f"loan{reptmon}{nowk}.sas7bdat"


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
    """

    deposit_path = config["deposit_current"]
    loan_dir     = config["loan_dir"]
    output_dir   = config["output_dir"]
    loan_path    = _build_loan_path(loan_dir, reptmon, nowk)

    if not deposit_path.exists():
        raise FileNotFoundError(f"[{bank_name}] Missing deposit file : {deposit_path}")
    if not loan_path.exists():
        raise FileNotFoundError(f"[{bank_name}] Missing loan file    : {loan_path}")

    # ------------------------------------------------------------------
    # Load inputs
    # DEPOSIT.CURRENT  → SAP.PBB.MNITB(0)  or  SAP.PIBB.MNITB(0)
    # LOAN dataset     → SAP.PBB.SASDATA   or  SAP.PIBB.SASDATA
    # ------------------------------------------------------------------
    deposit_df = _read_sas7bdat(deposit_path)

    loan_df = (
        _read_sas7bdat(loan_path)
        .select(["ACCTNO", "SECTORCD", "FISSPURP"])
    )

    # ------------------------------------------------------------------
    # DATA ODRAFT
    # SET DEPOSIT.CURRENT;
    # IF CURBAL < 0 AND CUSTCODE NE 81;
    # BALANCE = (-1)*CURBAL;
    # ------------------------------------------------------------------
    odraft = deposit_df.filter(
        (pl.col("CURBAL") < 0) & (pl.col("CUSTCODE") != 81)
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
