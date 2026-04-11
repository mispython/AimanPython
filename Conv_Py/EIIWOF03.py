#!/usr/bin/env python3
"""
Program: EIIWOF03.py
Purpose: Convert JCL wrapper logic for EIFMNP02 into Python.

Original intent:
- Select LOAN input source by OPC day-of-month condition.
- Execute EIFMNP02 using selected LOAN and NPL6 WOFF libraries.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
import argparse
import duckdb

# -----------------------------------------------------------------------------
# Path setup (defined early as requested)
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
LOAN_DIR = DATA_DIR / "LOAN"
NPL6_DIR = DATA_DIR / "NPL6"

# Input parquet paths (already converted from source files)
INPUT_LOAN_MONTHLY_PARQUET = LOAN_DIR / "MNILN.parquet"
INPUT_LOAN_DAILY_PARQUET = LOAN_DIR / "MNILN.DAILY.parquet"
INPUT_NPL6_WOFF_PARQUET = NPL6_DIR / "WOFF.parquet"

# Standardized EIFMNP02 input path aliases
EIFMNP02_LOAN_LNNOTE_PARQUET = BASE_DIR / "LOAN.LNNOTE.parquet"
EIFMNP02_NPL6_NPLOBAL_PARQUET = BASE_DIR / "NPL6.NPLOBAL.parquet"


@dataclass(frozen=True)
class RunContext:
    process_date: date
    selected_loan_parquet: Path


def resolve_process_date(raw_date: str | None) -> date:
    if raw_date:
        return datetime.strptime(raw_date, "%Y-%m-%d").date()
    return datetime.utcnow().date()


def select_loan_input(process_date: date) -> Path:
    """
    Mirrors OPC include conditions from JCL:
      day in 01..07 -> SAP.PIBB.MNILN(0)
      else          -> SAP.PIBB.MNILN.DAILY(0)
    """
    return INPUT_LOAN_MONTHLY_PARQUET if 1 <= process_date.day <= 7 else INPUT_LOAN_DAILY_PARQUET


def ensure_runtime_inputs(ctx: RunContext) -> None:
    """
    Materialize dependency input files expected by EIFMNP02.
    Uses DuckDB for parquet processing as requested.
    """
    EIFMNP02_LOAN_LNNOTE_PARQUET.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        con.execute(
            "COPY (SELECT * FROM read_parquet(?)) TO ? (FORMAT PARQUET)",
            [str(ctx.selected_loan_parquet), str(EIFMNP02_LOAN_LNNOTE_PARQUET)],
        )
        con.execute(
            "COPY (SELECT * FROM read_parquet(?)) TO ? (FORMAT PARQUET)",
            [str(INPUT_NPL6_WOFF_PARQUET), str(EIFMNP02_NPL6_NPLOBAL_PARQUET)],
        )
    finally:
        con.close()


def run_eifmnp02() -> None:
    # Placeholder dependency reference requested by migration rules.
    # Original JCL dependency:
    # //SYSIN DD DSN=SAP.BNM.PROGRAM(EIFMNP02),DISP=SHR
    from EIFMNP02 import main as eifmnp02_main

    eifmnp02_main()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="EIIWOF03 migration runner")
    parser.add_argument(
        "--process-date",
        help="Processing date in YYYY-MM-DD (default: UTC today)",
    )
    parser.add_argument(
        "--prepare-only",
        action="store_true",
        help="Prepare runtime parquet aliases without executing EIFMNP02",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    process_date = resolve_process_date(args.process_date)
    selected_loan_parquet = select_loan_input(process_date)

    ctx = RunContext(process_date=process_date, selected_loan_parquet=selected_loan_parquet)

    ensure_runtime_inputs(ctx)

    if not args.prepare_only:
        run_eifmnp02()


if __name__ == "__main__":
    main()
