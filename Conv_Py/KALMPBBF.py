#!/usr/bin/env python3
"""
Program: KALMPBBF

This program recreates:
1) SAS DATA step KALM with BNMCODE values from CARDS.
2) PROC FORMAT definitions ORIMAT and KREMMTH.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import polars as pl

# ---------------------------------------------------------------------------
# Path setup (defined early per requirements)
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"

INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

KALM_PARQUET_PATH = OUTPUT_DIR / "KALM.parquet"

# ---------------------------------------------------------------------------
# DATA KALM; INPUT BNMCODE $1-14; CARDS;
# ---------------------------------------------------------------------------
BNMCODE_VALUES: list[str] = [
    "5721000000000Y",
    "5722000000000Y",
    "5723000000000Y",
    "5729900000000Y",
    "5751000000000Y",
    "5752000000000Y",
    "5753000000000Y",
    "5759900000000Y",
    "5761000000000Y",
    "5762000000000Y",
    "5763000000000Y",
    "5771000000000Y",
    "5772000000000Y",
    "5773000000000Y",
    "5779900000000Y",
    "6850101000000Y",
    "6850102000000Y",
    "6850103000000Y",
    "6850112000000Y",
    "6850115000000Y",
    "6850181000000Y",
    "6850185000000Y",
    "6850201000000Y",
    "6850202000000Y",
    "6850203000000Y",
    "6850212000000Y",
    "6850215000000Y",
    "6850281000000Y",
    "6850285000000Y",
    "6850301000000Y",
    "6850302000000Y",
    "6850303000000Y",
    "6850312000000Y",
    "6850315000000Y",
    "6850381000000Y",
    "6850385000000Y",
    "6850401000000Y",
    "6850402000000Y",
    "6850403000000Y",
    "6850412000000Y",
    "6850415000000Y",
    "6850481000000Y",
    "6850485000000Y",
    "7850101000000Y",
    "7850102000000Y",
    "7850103000000Y",
    "7850112000000Y",
    "7850115000000Y",
    "7850181000000Y",
    "7850185000000Y",
    "7850201000000Y",
    "7850202000000Y",
    "7850203000000Y",
    "7850212000000Y",
    "7850215000000Y",
    "7850281000000Y",
    "7850285000000Y",
    "7850301000000Y",
    "7850302000000Y",
    "7850303000000Y",
    "7850312000000Y",
    "7850315000000Y",
    "7850381000000Y",
    "7850385000000Y",
    "7850401000000Y",
    "7850402000000Y",
    "7850403000000Y",
    "7850412000000Y",
    "7850415000000Y",
    "7850481000000Y",
    "7850485000000Y",
]


def create_kalm_dataset(output_path: Path = KALM_PARQUET_PATH) -> pl.DataFrame:
    """Create DATA KALM equivalent and persist it as parquet."""
    kalm_df = pl.DataFrame({"BNMCODE": BNMCODE_VALUES})
    kalm_df.write_parquet(output_path)
    return kalm_df


# ---------------------------------------------------------------------------
# PROC FORMAT equivalents
# ---------------------------------------------------------------------------
def format_orimat(value: float | int) -> str:
    """SAS ORIMAT format."""
    x = float(value)
    if x == 1:
        return "12"
    if x == 2:
        return "13"
    if x == 3:
        return "14"
    if 4 <= x <= 6:
        return "15"
    if 7 <= x <= 9:
        return "16"
    if 10 <= x <= 12:
        return "17"
    if x >= 13:
        return "20"
    return ""


def format_kremmth(value: float | int) -> str:
    """SAS KREMMTH format.

    The boundary checks preserve SAS declaration order for shared endpoints.
    """
    x = float(value)
    if x <= 0:
        return "51"
    if 0 <= x <= 1:
        return "52"
    if 1 <= x <= 2:
        return "53"
    if 2 <= x <= 3:
        return "54"
    if 3 <= x <= 4:
        return "81"
    if 4 <= x <= 5:
        return "82"
    if 5 <= x <= 6:
        return "83"
    if 6 <= x <= 7:
        return "84"
    if 7 <= x <= 8:
        return "85"
    if 8 <= x <= 9:
        return "86"
    if 9 <= x <= 10:
        return "87"
    if 10 <= x <= 11:
        return "88"
    if 11 <= x <= 12:
        return "89"
    return "60"


def apply_orimat(values: Iterable[float | int]) -> list[str]:
    """Vector helper for ORIMAT format."""
    return [format_orimat(v) for v in values]


def apply_kremmth(values: Iterable[float | int]) -> list[str]:
    """Vector helper for KREMMTH format."""
    return [format_kremmth(v) for v in values]


if __name__ == "__main__":
    kalm = create_kalm_dataset()
    print(f"Created {KALM_PARQUET_PATH}")
    print(f"Rows written: {kalm.height}")
