#!/usr/bin/env python3
"""
Program: KALMSTOR
Purpose: Store output for total commitments & contingencies and monthly purchases & sales.

SAS source behavior:
1) DEPOBACK.K1TBL&REPTMON = K1DTL + K1MDTL, dropping D1-D12, RD1-RD12, MD1-MD12,
   RPYR, RPMTH, RPDAY.
2) DEPOBACK.K2TBL&REPTMON = K2DTL.
3) DEPOBACK.K3TBL&REPTMON = K3DTL.
4) If AMTIND = "'D'", DEPOBACK.DCITBL&REPTMON = DCIDTL + DCIMDTL.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import polars as pl

# -------------------------
# Path and runtime setup
# -------------------------
INPUT_DIR = Path(os.getenv("INPUT_DIR", "input"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "output"))
DEPOBACK_DIR = OUTPUT_DIR / "depoback"

INPUT_DIR.mkdir(parents=True, exist_ok=True)
DEPOBACK_DIR.mkdir(parents=True, exist_ok=True)

REPTMON = os.getenv("REPTMON", "")
AMTIND = os.getenv("AMTIND", "")

CONN = duckdb.connect()


def _read_parquet_required(dataset_name: str) -> pl.DataFrame:
    """Read <INPUT_DIR>/<dataset_name>.parquet as a Polars DataFrame."""
    parquet_path = INPUT_DIR / f"{dataset_name}.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(f"Required input file not found: {parquet_path}")
    return CONN.execute("SELECT * FROM read_parquet(?)", [str(parquet_path)]).pl()


def _write_parquet(df: pl.DataFrame, target_table_name: str) -> Path:
    """Write DataFrame to DEPOBACK as <target_table_name>.parquet."""
    output_path = DEPOBACK_DIR / f"{target_table_name}.parquet"
    df.write_parquet(output_path)
    return output_path


def _concat_tables(table_1: str, table_2: str) -> pl.DataFrame:
    """SAS SET equivalent: stack table_1 then table_2."""
    df1 = _read_parquet_required(table_1)
    df2 = _read_parquet_required(table_2)
    return pl.concat([df1, df2], how="diagonal_relaxed")


def main() -> None:
    # DATA DEPOBACK.K1TBL&REPTMON; SET K1DTL K1MDTL; DROP ...; RUN;
    k1tbl = _concat_tables("K1DTL", "K1MDTL")

    drop_columns = [
        *(f"D{i}" for i in range(1, 13)),
        *(f"RD{i}" for i in range(1, 13)),
        *(f"MD{i}" for i in range(1, 13)),
        "RPYR",
        "RPMTH",
        "RPDAY",
    ]
    existing_drop_columns = [col for col in drop_columns if col in k1tbl.columns]
    if existing_drop_columns:
        k1tbl = k1tbl.drop(existing_drop_columns)

    k1_output = _write_parquet(k1tbl, f"K1TBL{REPTMON}")

    # DATA DEPOBACK.K2TBL&REPTMON; SET K2DTL; RUN;
    k2tbl = _read_parquet_required("K2DTL")
    k2_output = _write_parquet(k2tbl, f"K2TBL{REPTMON}")

    # DATA DEPOBACK.K3TBL&REPTMON; SET K3DTL; RUN;
    k3tbl = _read_parquet_required("K3DTL")
    k3_output = _write_parquet(k3tbl, f"K3TBL{REPTMON}")

    # %IF "&AMTIND" = "'D'" %THEN %DO; DATA DEPOBACK.DCITBL&REPTMON; SET ...; RUN; %END;
    dci_output = None
    if AMTIND == "'D'":
        dcitbl = _concat_tables("DCIDTL", "DCIMDTL")
        dci_output = _write_parquet(dcitbl, f"DCITBL{REPTMON}")

    print("KALMSTOR conversion completed.")
    print(f"Created: {k1_output}")
    print(f"Created: {k2_output}")
    print(f"Created: {k3_output}")
    if dci_output is not None:
        print(f"Created: {dci_output}")
    else:
        print("Skipped DCITBL output because AMTIND != \"'D'\".")


if __name__ == "__main__":
    try:
        main()
    finally:
        CONN.close()
