#!/usr/bin/env python3
"""
File Name: EIBXODLC
Overdraft Loan Classification outputs for PBB and PIBB.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict

import duckdb
import polars as pl


# =============================================================================
# PATH CONFIGURATION
# =============================================================================
# BASE_INPUT_DIR = Path("input/EIBXODLC")
# BASE_OUTPUT_DIR = Path("/mnt/user-data/outputs/EIBXODLC")

SCRIPT_DIR = Path(__file__).resolve().parent
BASE_INPUT_DIR = SCRIPT_DIR / "input" / "EIBXODLC"
BASE_OUTPUT_DIR = SCRIPT_DIR / "outputs" / "EIBXODLC"

PBB_CONFIG: Dict[str, Path] = {
    "reptdate": BASE_INPUT_DIR / "pbb" / "deposit_reptdate.parquet",
    "deposit_current": BASE_INPUT_DIR / "pbb" / "deposit_current.parquet",
    "loan_dir": BASE_INPUT_DIR / "pbb" / "loan",
    "output_dir": BASE_OUTPUT_DIR / "pbb",
}

PIBB_CONFIG: Dict[str, Path] = {
    "reptdate": BASE_INPUT_DIR / "pibb" / "deposit_reptdate.parquet",
    "deposit_current": BASE_INPUT_DIR / "pibb" / "deposit_current.parquet",
    "loan_dir": BASE_INPUT_DIR / "pibb" / "loan",
    "output_dir": BASE_OUTPUT_DIR / "pibb",
}


# =============================================================================
# DATA STRUCTURES
# =============================================================================
@dataclass(frozen=True)
class MacroVars:
    reptdate: date
    nowk: str
    reptmon: str


# =============================================================================
# UTILITIES
# =============================================================================
SAS_EPOCH = date(1960, 1, 1)


def normalize_reptdate(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)):
        numeric = int(value)
        if numeric > 10_000_000:
            text = f"{numeric:08d}"
            return date(int(text[0:4]), int(text[4:6]), int(text[6:8]))
        return SAS_EPOCH + timedelta(days=numeric)
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit() and len(text) == 8:
            return date(int(text[0:4]), int(text[4:6]), int(text[6:8]))
        return datetime.fromisoformat(text).date()
    raise TypeError(f"Unsupported REPTDATE value: {value!r}")


def derive_macro_vars(reptdate: date) -> MacroVars:
    day = reptdate.day
    if day == 8:
        nowk = "1"
    elif day == 15:
        nowk = "2"
    elif day == 22:
        nowk = "3"
    else:
        nowk = "4"
    reptmon = f"{reptdate.month:02d}"
    return MacroVars(reptdate=reptdate, nowk=nowk, reptmon=reptmon)


def read_reptdate(con: duckdb.DuckDBPyConnection, reptdate_path: Path) -> MacroVars:

    print("Looking for:", reptdate_path.resolve())
    print("Exists?", reptdate_path.exists(), "\n")

    result = con.execute(
        "SELECT REPTDATE FROM read_parquet(?) LIMIT 1",
        [str(reptdate_path)],
    ).fetchone()
    if not result:
        raise ValueError(f"REPTDATE file is empty: {reptdate_path}")
    reptdate = normalize_reptdate(result[0])
    return derive_macro_vars(reptdate)


def load_parquet(con: duckdb.DuckDBPyConnection, path: Path) -> pl.DataFrame:
    return con.execute("SELECT * FROM read_parquet(?)", [str(path)]).pl()


def build_loan_path(loan_dir: Path, reptmon: str, nowk: str) -> Path:
    return loan_dir / f"loan{reptmon}{nowk}.parquet"


def process_bank(bank_name: str, config: Dict[str, Path]) -> None:
    con = duckdb.connect()

    reptdate_path = config["reptdate"]
    deposit_path = config["deposit_current"]
    loan_dir = config["loan_dir"]
    output_dir = config["output_dir"]

    macros = read_reptdate(con, reptdate_path)
    loan_path = build_loan_path(loan_dir, macros.reptmon, macros.nowk)

    if not deposit_path.exists():
        raise FileNotFoundError(f"Missing deposit file: {deposit_path}")
    if not loan_path.exists():
        raise FileNotFoundError(f"Missing loan file: {loan_path}")

    deposit = load_parquet(con, deposit_path)
    loan = load_parquet(con, loan_path).select(["ACCTNO", "SECTORCD", "FISSPURP"])

    odraft = (
        deposit.filter((pl.col("CURBAL") < 0) & (pl.col("CUSTCODE") != 81))
        .with_columns((pl.col("CURBAL") * -1).alias("BALANCE"))
    )

    odraft1 = odraft.join(loan, on="ACCTNO", how="inner")

    sector_str = pl.col("SECTORCD").cast(pl.Utf8)
    odraft2 = odraft1.filter(
        (~pl.col("CUSTCD").is_in(["77", "78", "95", "96"]))
        & (sector_str.str.starts_with("5") | (sector_str == "8310"))
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    odraft1_sorted = odraft1.sort(["BRANCH", "FISSPURP", "ACCTNO"])
    odraft2_sorted = odraft2.sort(["BRANCH", "SECTORCD", "ACCTNO"])

    output_odraf1 = output_dir / f"ODRAF1{macros.reptmon}.parquet"
    output_odraf2 = output_dir / f"ODRAF2{macros.reptmon}.parquet"

    odraft1_sorted.write_parquet(output_odraf1)
    odraft2_sorted.write_parquet(output_odraf2)

    print(f"[{bank_name}] REPTDATE: {macros.reptdate} NOWK={macros.nowk} REPTMON={macros.reptmon}")
    print(f"[{bank_name}] Output: {output_odraf1}")
    print(f"[{bank_name}] Output: {output_odraf2}")


def main() -> None:
    process_bank("PBB", PBB_CONFIG)
    process_bank("PIBB", PIBB_CONFIG)


if __name__ == "__main__":
    main()
