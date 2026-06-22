#!/usr/bin/env python3
"""
EIBXLNLC - Lazy Streaming Version (No RAM spike, no tmp parquet)
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import os
import pandas as pd
import polars as pl

from REPTDATE import get_reptdate_values
from input_date import get_latest_file


# =============================================================================
# PATH CONFIG
# =============================================================================
BASE_DIR  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
INPUT_DIR = BASE_DIR / "input/prod" / "EIBXLNLC"
OUTPUT_DIR = BASE_DIR / "output" / "EIBXLNLC"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


PBB_CONFIG: Dict[str, Path] = {
    "lnnote": INPUT_DIR / "lnnote_pbb.sas7bdat",
    "lncomm": INPUT_DIR / "enrh_ln_comm.sas7bdat",
    "loan_dir": get_latest_file(BASE_DIR / "input/prod/EIBXODLC", "ln"),
    "output_dir": OUTPUT_DIR / "PBB",
}

PIBB_CONFIG: Dict[str, Path] = {
    "lnnote": INPUT_DIR / "lnnote_pibb.sas7bdat",
    "lncomm": INPUT_DIR / "enrh_ln_comm.sas7bdat",
    "loan_dir": get_latest_file(BASE_DIR / "input/prod/EIBXODLC", "iln"),
    "output_dir": OUTPUT_DIR / "PIBB",
}


# =============================================================================
# LAZY SAS READER
# =============================================================================
def _read_sas_lazy(path: Path) -> pl.LazyFrame:
    if not path.exists():
        raise FileNotFoundError(path)

    print(f"[READ LAZY] {path.name}")

    pdf = pd.read_sas(str(path), encoding="latin1")
    pdf.columns = [c.upper() for c in pdf.columns]

    return pl.from_pandas(pdf).lazy()


# =============================================================================
# PROCESS BANK (LAZY PIPELINE)
# =============================================================================
def process_bank(bank_name: str, config: Dict[str, Path], reptmon: str) -> None:

    lnnote_path = config["lnnote"]
    lncomm_path = config["lncomm"]
    loan_path   = config["loan_dir"]
    output_dir  = config["output_dir"]

    # ----------------------------
    # LOAD (LAZY)
    # ----------------------------
    lnnote_lf = _read_sas_lazy(lnnote_path)
    lncomm_lf = _read_sas_lazy(lncomm_path)
    loan_lf   = _read_sas_lazy(loan_path)

    # ----------------------------
    # COLUMN TRIM EARLY
    # ----------------------------
    lnnote_lf = lnnote_lf.select([
        "ACCTNO", "NOTENO", "BANKNO", "STATE", "NAME", "NTBRCH", "COMMNO"
    ])

    lncomm_lf = lncomm_lf.select([
        "ACCTNO", "COMMNO", "CCOLLTRL"
    ])

    loan_lf = loan_lf.select([
        "ACCTNO", "NOTENO", "BRANCH", "BALANCE", "SECTORCD", "CUSTCD",
        "INTRATE", "APPRLIMT", "FISSPURP", "STATE"
    ])

    # ----------------------------
    # LAZY JOINS
    # ----------------------------
    lnote_lf = loan_lf.join(
        lnnote_lf,
        on=["ACCTNO", "NOTENO"],
        how="left"
    )

    note1_lf = lnote_lf.join(
        lncomm_lf,
        on=["ACCTNO", "COMMNO"],
        how="left"
    )

    # ----------------------------
    # NOTE2 FILTER
    # ----------------------------
    note2_lf = note1_lf.filter(
        (~pl.col("CUSTCD").cast(pl.Utf8).is_in(["77", "78", "95", "96"]))
        & (
            (pl.col("SECTORCD").cast(pl.Utf8).str.slice(0, 1) == "5")
            | (pl.col("SECTORCD") == "8310")
        )
    )

    # ----------------------------
    # LAZY SORT (NO RAM SPIKE)
    # ----------------------------
    note1_lf = note1_lf.sort(
        ["BRANCH", "FISSPURP", "CUSTCD", "ACCTNO"]
    )

    note2_lf = note2_lf.sort(
        ["BRANCH", "SECTORCD", "CUSTCD", "ACCTNO"]
    )

    # ----------------------------
    # OUTPUT
    # ----------------------------
    prefix = "LNLC" if bank_name == "PBB" else "LNLCI"

    note1_out = output_dir / f"{prefix}_NOTE1_{reptmon}.parquet"
    note2_out = output_dir / f"{prefix}_NOTE2_{reptmon}.parquet"

    note1_lf.sink_parquet(note1_out)
    note2_lf.sink_parquet(note2_out)

    print(f"\n[{bank_name}] DONE")
    print(f"NOTE1 -> {note1_out}")
    print(f"NOTE2 -> {note2_out}")


# =============================================================================
# MAIN
# =============================================================================
def main() -> None:
    rv = get_reptdate_values()
    reptmon = rv.reptmon

    print(f"REPORT MONTH: {reptmon}")

    process_bank("PBB", PBB_CONFIG, reptmon)
    process_bank("PIBB", PIBB_CONFIG, reptmon)


if __name__ == "__main__":
    main()
