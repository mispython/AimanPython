#!/usr/bin/env python3
"""
Program: EIAWOF14
Purpose: Select WIIS/WSP2/WAQ from NPL and append WOFF transactions into WOFFTOT.

SAS parity implemented:
1) Copy NPL.WIIS, NPL.WSP2, NPL.WAQ -> NPL1 library.
2) Read fixed-width WMIS file and build WOFF dataset.
3) Apply SAS filtering logic and SPWOFF overwrite.
4) Append into NPL1.WOFFTOT and de-duplicate by ACCTNO/NOTENO.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable

import duckdb
import polars as pl


# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_PATH = Path("./data")
INPUT_PATH = BASE_PATH / "input"
OUTPUT_PATH = BASE_PATH / "output"

NPL_INPUT_PATH = INPUT_PATH / "npl"
NPL1_OUTPUT_PATH = OUTPUT_PATH / "npl1"
WMIS_INPUT_PATH = INPUT_PATH / "wmis" / "RBP2.B033.LN.WRI2.OFF.MIS.txt"

WIIS_INPUT = NPL_INPUT_PATH / "WIIS.parquet"
WSP2_INPUT = NPL_INPUT_PATH / "WSP2.parquet"
WAQ_INPUT = NPL_INPUT_PATH / "WAQ.parquet"
WOFFTOT_OUTPUT = NPL1_OUTPUT_PATH / "WOFFTOT.parquet"

NPL1_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class FieldSpec:
    name: str
    start: int  # 1-based, inclusive (SAS column pointer semantics)
    width: int


WMIS_LAYOUT: tuple[FieldSpec, ...] = (
    FieldSpec("ACCTNO", 142, 10),
    FieldSpec("NOTENO", 152, 5),
    FieldSpec("IISWOFF", 162, 16),
    FieldSpec("SPWOFF", 178, 16),
    FieldSpec("DDWOFF", 210, 2),
    FieldSpec("MMWOFF", 213, 2),
    FieldSpec("YYWOFF", 216, 4),
    FieldSpec("CAPBAL", 220, 16),
    FieldSpec("COSTCTR", 236, 4),
)


def copy_base_tables() -> None:
    """Copy WIIS/WSP2/WAQ from NPL input to NPL1 output using DuckDB parquet scan."""
    conn = duckdb.connect()
    copies = {
        WIIS_INPUT: NPL1_OUTPUT_PATH / "WIIS.parquet",
        WSP2_INPUT: NPL1_OUTPUT_PATH / "WSP2.parquet",
        WAQ_INPUT: NPL1_OUTPUT_PATH / "WAQ.parquet",
    }

    for src, dst in copies.items():
        if not src.exists():
            raise FileNotFoundError(f"Required input parquet not found: {src}")
        conn.execute(
            """
            COPY (
                SELECT *
                FROM read_parquet(?)
            ) TO ? (FORMAT PARQUET)
            """,
            [str(src), str(dst)],
        )


def _slice_text(line: str, start: int, width: int) -> str:
    """Return fixed-width field by 1-based start and width."""
    zero_based = start - 1
    return line[zero_based : zero_based + width]


def _to_int(value: str) -> int | None:
    value = value.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _to_decimal(value: str) -> Decimal | None:
    value = value.strip()
    if not value:
        return None
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError):
        return None


def parse_wmis_records(lines: Iterable[str]) -> list[dict]:
    """Parse fixed-width WMIS lines into SAS-equivalent WOFF rows."""
    records: list[dict] = []

    for line in lines:
        parsed = {spec.name: _slice_text(line, spec.start, spec.width) for spec in WMIS_LAYOUT}

        acctno = _to_int(parsed["ACCTNO"])
        noteno = _to_int(parsed["NOTENO"])
        iiswoff = _to_decimal(parsed["IISWOFF"])
        ddwoff = _to_int(parsed["DDWOFF"])
        mmwoff = _to_int(parsed["MMWOFF"])
        yywoff = _to_int(parsed["YYWOFF"])
        capbal = _to_decimal(parsed["CAPBAL"])
        costctr = _to_int(parsed["COSTCTR"])

        if costctr is None:
            continue

        if (3000 <= costctr <= 3999) or costctr in {4043, 4048}:
            continue

        woffdt = f"{(mmwoff or 0):02d}/{(yywoff or 0):04d}"

        records.append(
            {
                "ACCTNO": acctno,
                "NOTENO": noteno,
                "IISWOFF": iiswoff,
                "SPWOFF": capbal,  # SAS: SPWOFF=CAPBAL;
                "WOFFDT": woffdt,
                "CAPBAL": capbal,
            }
        )

    return records


def build_woff_from_wmis() -> pl.DataFrame:
    """Create WOFF dataframe from raw WMIS fixed-width input."""
    if not WMIS_INPUT_PATH.exists():
        raise FileNotFoundError(f"Required WMIS input not found: {WMIS_INPUT_PATH}")

    with WMIS_INPUT_PATH.open("r", encoding="latin-1") as f:
        records = parse_wmis_records(f)

    if not records:
        return pl.DataFrame(
            schema={
                "ACCTNO": pl.Int64,
                "NOTENO": pl.Int64,
                "IISWOFF": pl.Decimal(18, 2),
                "SPWOFF": pl.Decimal(18, 2),
                "WOFFDT": pl.Utf8,
                "CAPBAL": pl.Decimal(18, 2),
            }
        )

    return pl.DataFrame(records).with_columns(
        pl.col("IISWOFF").cast(pl.Decimal(18, 2)),
        pl.col("SPWOFF").cast(pl.Decimal(18, 2)),
        pl.col("CAPBAL").cast(pl.Decimal(18, 2)),
    )


def append_and_deduplicate_wofftot(woff: pl.DataFrame) -> pl.DataFrame:
    """Append WOFF to WOFFTOT then deduplicate on ACCTNO/NOTENO."""
    if WOFFTOT_OUTPUT.exists():
        existing = pl.read_parquet(WOFFTOT_OUTPUT)
        combined = pl.concat([existing, woff], how="diagonal_relaxed")
    else:
        combined = woff

    deduped = (
        combined
        .sort(["ACCTNO", "NOTENO"], maintain_order=True)
        .unique(subset=["ACCTNO", "NOTENO"], keep="first", maintain_order=True)
    )

    deduped.write_parquet(WOFFTOT_OUTPUT)
    return deduped


def main() -> None:
    copy_base_tables()
    woff = build_woff_from_wmis()
    append_and_deduplicate_wofftot(woff)


if __name__ == "__main__":
    main()
