#!/usr/bin/env python3
"""
Program: PBBELP
Function: Compute eligible liabilities (EL) and append EL for SRR into ALW output.

Assumptions:
- Input datasets are parquet files with column names aligned to SAS datasets.
- DAYA is processed, consistent with `%EL(DAYA)` in the SAS source.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Iterable, List

import duckdb
import polars as pl

# ============================================================================
# PATH / ENVIRONMENT SETUP
# ============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

REPTMON = os.getenv("REPTMON", "")
NOWK = os.getenv("NOWK", "")
TARGET_DAY = os.getenv("TARGET_DAY", "DAYA")

MELW_FILE = Path(os.getenv("MELW_FILE", str(INPUT_DIR / f"MELW{REPTMON}{NOWK}.parquet")))
WELW_FILE = Path(os.getenv("WELW_FILE", str(INPUT_DIR / f"WELW{REPTMON}{NOWK}.parquet")))
ALW_FILE = Path(os.getenv("ALW_FILE", str(INPUT_DIR / f"ALW{REPTMON}{NOWK}.parquet")))
EL_FORMAT_SOURCE = Path(os.getenv("EL_FORMAT_SOURCE", str(BASE_DIR / "PBBELF")))

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ALW_OUTPUT_FILE = Path(os.getenv("ALW_OUTPUT_FILE", str(OUTPUT_DIR / f"ALW{REPTMON}{NOWK}.parquet")))

# ============================================================================
# CONSTANTS
# ============================================================================
DAY_TO_TOTAL_EL_CODE: Dict[str, str] = {
    "DAYA": "4011100000000Y",
    "DAYB": "4011200000000Y",
    "DAYC": "4011300000000Y",
    "DAYD": "4011400000000Y",
    "DAYE": "4011500000000Y",
    "DAYF": "4011600000000Y",
    "DAYG": "4011700000000Y",
    "DAYH": "4011800000000Y",
}

DAY_TO_ELSRR_CODE: Dict[str, str] = {
    "DAYA": "4013100000000Y",
    "DAYB": "4013200000000Y",
    "DAYC": "4013300000000Y",
    "DAYD": "4013400000000Y",
    "DAYE": "4013500000000Y",
    "DAYF": "4013600000000Y",
    "DAYG": "4013700000000Y",
    "DAYH": "4013800000000Y",
}

DAY_TO_CAGAMAS_CODES: Dict[str, List[str]] = {
    "DAYA": ["4014100000000Y", "4015100000000Y"],
    "DAYB": ["4014200000000Y", "4015200000000Y"],
    "DAYC": ["4014300000000Y", "4015300000000Y"],
    "DAYD": ["4014400000000Y", "4015400000000Y"],
    "DAYE": ["4014500000000Y", "4015500000000Y"],
    "DAYF": ["4014600000000Y", "4015600000000Y"],
    "DAYG": ["4014700000000Y", "4015700000000Y"],
    "DAYH": ["4014800000000Y", "4015800000000Y"],
}


# ============================================================================
# HELPERS
# ============================================================================
def parse_el_format_from_sas(sas_file: Path) -> pl.DataFrame:
    """Parse EL format lines from the `CARDS;` section of X_PBBELF."""
    text = sas_file.read_text(encoding="utf-8", errors="ignore")
    lines = text.splitlines()

    in_cards = False
    records: List[Dict[str, str]] = []
    for raw in lines:
        line = raw.rstrip("\n")
        if not in_cards:
            if line.strip().upper() == "CARDS;":
                in_cards = True
            continue

        if line.strip() == ";":
            break
        if not line.strip():
            continue

        # INPUT BNMCODE $ 1-14 SIGN $ 16 FMTNAME $ 18-21 ...
        # We only need BNMCODE, SIGN, FMTNAME for this program.
        bnmcode = line[0:14].strip()
        sign = line[15:16].strip()
        fmtname = line[17:21].strip()

        if bnmcode and sign and fmtname:
            records.append({"BNMCODE": bnmcode, "SIGN": sign, "FMTNAME": fmtname})

    return pl.DataFrame(records).unique(subset=["BNMCODE", "SIGN", "FMTNAME"])


def require_columns(df: pl.DataFrame, required: Iterable[str], name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")


# ============================================================================
# CORE LOGIC
# ============================================================================
def build_elw(melw_df: pl.DataFrame, welw_df: pl.DataFrame) -> pl.DataFrame:
    """Replicate MELW/WELW summarization and ELW combination logic from SAS."""

    melw_agg = (
        melw_df.group_by(["BNMCODE", "ELDAY"]).agg(pl.col("AMOUNT").sum())
        .with_columns(
            pl.when(pl.col("BNMCODE").str.slice(0, 7) == "4939980")
            .then(pl.lit("4929980000000Y"))
            .otherwise(pl.col("BNMCODE"))
            .alias("BNMCODE")
        )
        .group_by(["BNMCODE", "ELDAY"]).agg(pl.col("AMOUNT").sum())
    )

    welw_agg = welw_df.group_by(["BNMCODE", "ELDAY"]).agg(pl.col("AMOUNT").sum())

    con = duckdb.connect()
    con.register("welw", welw_agg.to_arrow())
    con.register("melw", melw_agg.to_arrow())

    query = """
        SELECT
            COALESCE(w.BNMCODE, m.BNMCODE) AS BNMCODE,
            COALESCE(w.ELDAY, m.ELDAY) AS ELDAY,
            CASE
                WHEN w.AMOUNT IS NOT NULL AND m.AMOUNT IS NOT NULL THEN
                    CASE
                        WHEN SUBSTR(COALESCE(w.BNMCODE, m.BNMCODE), 1, 7) IN ('4911080', '4929980')
                            THEN w.AMOUNT + m.AMOUNT
                        ELSE w.AMOUNT
                    END
                WHEN w.AMOUNT IS NOT NULL THEN w.AMOUNT
                ELSE m.AMOUNT
            END AS AMOUNT
        FROM welw w
        FULL OUTER JOIN melw m
          ON w.BNMCODE = m.BNMCODE
         AND w.ELDAY = m.ELDAY
    """
    out = pl.from_arrow(con.execute(query).fetch_arrow_table())
    con.close()
    return out


def compute_total_el(elw_df: pl.DataFrame, el_format_df: pl.DataFrame, day: str) -> pl.DataFrame:
    """Compute TOTALEL for a single ELDAY."""
    day_df = elw_df.filter(pl.col("ELDAY") == day)

    joined = day_df.join(el_format_df, on="BNMCODE", how="inner")

    if joined.is_empty():
        total_amount = 0.0
    else:
        signed = joined.with_columns(
            pl.when(pl.col("SIGN") == "+")
            .then(pl.col("AMOUNT"))
            .otherwise(-pl.col("AMOUNT"))
            .alias("SIGNED_AMOUNT")
        )

        sums = (
            signed.group_by("FMTNAME")
            .agg(pl.col("SIGNED_AMOUNT").sum().alias("SUM_AMOUNT"))
            .to_dicts()
        )
        sums_map = {row["FMTNAME"]: float(row["SUM_AMOUNT"]) for row in sums}

        rmel = sums_map.get("RMEL", 0.0)
        fxel = sums_map.get("FXEL", 0.0)
        rmea = sums_map.get("RMEA", 0.0)
        fxea = sums_map.get("FXEA", 0.0)
        total_amount = rmel + fxel - rmea - min(fxel, fxea)

    total_itcode = DAY_TO_TOTAL_EL_CODE[day]
    return pl.DataFrame({"ITCODE": [total_itcode], "AMOUNT": [total_amount]})


def compute_elsrr(total_el_df: pl.DataFrame, alw_df: pl.DataFrame, day: str) -> pl.DataFrame:
    """Compute ELSRR amount and code for the target day."""
    cagamas_codes = DAY_TO_CAGAMAS_CODES[day]
    cagamas_amount = (
        alw_df.filter(pl.col("ITCODE").is_in(cagamas_codes))
        .select(pl.col("AMOUNT").sum().alias("CAGAMT"))
        .item()
    )
    cagamas_amount = float(cagamas_amount) if cagamas_amount is not None else 0.0

    total_el_amount = float(total_el_df.select("AMOUNT").item())
    elsrr_amount = total_el_amount + cagamas_amount

    return pl.DataFrame(
        {
            "ITCODE": [DAY_TO_ELSRR_CODE[day]],
            "AMOUNT": [elsrr_amount],
        }
    )


def main() -> None:
    if TARGET_DAY not in DAY_TO_TOTAL_EL_CODE:
        raise ValueError(f"Unsupported TARGET_DAY={TARGET_DAY}. Expected one of {sorted(DAY_TO_TOTAL_EL_CODE)}")

    melw_df = pl.read_parquet(MELW_FILE)
    welw_df = pl.read_parquet(WELW_FILE)
    alw_df = pl.read_parquet(ALW_FILE)
    el_format_df = parse_el_format_from_sas(EL_FORMAT_SOURCE)

    require_columns(melw_df, ["BNMCODE", "ELDAY", "AMOUNT"], "MELW")
    require_columns(welw_df, ["BNMCODE", "ELDAY", "AMOUNT"], "WELW")
    require_columns(alw_df, ["ITCODE", "AMOUNT"], "ALW")
    require_columns(el_format_df, ["BNMCODE", "SIGN", "FMTNAME"], "EL format")

    elw_df = build_elw(melw_df, welw_df)
    total_el_df = compute_total_el(elw_df, el_format_df, TARGET_DAY)
    elsrr_df = compute_elsrr(total_el_df, alw_df, TARGET_DAY)

    alw_updated = pl.concat([alw_df.select(["ITCODE", "AMOUNT"]), elsrr_df], how="vertical_relaxed")
    alw_updated.write_parquet(ALW_OUTPUT_FILE)

    print(f"ELW rows: {elw_df.height}")
    print(f"TOTALEL ({TARGET_DAY}) amount: {total_el_df.select('AMOUNT').item()}")
    print(f"ELSRR ({TARGET_DAY}) appended code: {elsrr_df.select('ITCODE').item()}")
    print(f"Output written: {ALW_OUTPUT_FILE}")


if __name__ == "__main__":
    main()
