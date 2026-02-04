"""
File Name: EIBDFACT
Daily PBIF for NLF
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
import calendar
import sys

import duckdb
import polars as pl


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/path/to")
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"

REPTDATE_PATH = INPUT_DIR / "REPTDATE.parquet"
PBIF_PATH = INPUT_DIR / "PBIF.parquet"

PBIF_OUTPUT_PATH = OUTPUT_DIR / "PBIF.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# DUCKDB SESSION
# ============================================================================
CON = duckdb.connect()


# ============================================================================
# DATE HELPERS
# ============================================================================

def to_date(value) -> date | None:
    """Convert parquet values to Python date, supporting SAS numeric dates."""
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, (int, float)):
        if value <= 0:
            return None
        return date(1960, 1, 1) + timedelta(days=int(value))
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def days_in_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


# ============================================================================
# FORMAT DEFINITIONS
# ============================================================================

def get_rem_format(remmth: float) -> str:
    """Map remaining months to SAS REMFMT code."""
    if remmth <= 0.1:
        return "01"
    if remmth <= 1:
        return "02"
    if remmth <= 3:
        return "03"
    if remmth <= 6:
        return "04"
    if remmth <= 12:
        return "05"
    return "06"


def calculate_remaining_months(matdt: date, reptdate: date) -> float:
    """Calculate remaining months per SAS REMMTH macro."""
    rpyr = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day
    rpdays = days_in_month(rpyr, rpmth)

    mdyr = matdt.year
    mdmth = matdt.month
    mdday = matdt.day

    if mdday > rpdays:
        mdday = rpdays

    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday

    return remy * 12 + remm + remd / rpdays


# ============================================================================
# READ REPTDATE
# ============================================================================

def read_reptdate() -> dict:
    """Read REPTDATE and derive macro variables."""
    if not REPTDATE_PATH.exists():
        raise FileNotFoundError(f"REPTDATE file not found: {REPTDATE_PATH}")

    df = CON.execute(
        "SELECT REPTDATE FROM read_parquet(?)",
        [str(REPTDATE_PATH)],
    ).pl()

    if df.is_empty():
        raise ValueError("REPTDATE file is empty")

    reptdate = to_date(df[0, 0])
    if reptdate is None:
        raise ValueError("REPTDATE is missing or invalid")

    day = reptdate.day
    if day == 8:
        nowk = "1"
    elif day == 15:
        nowk = "2"
    elif day == 22:
        nowk = "3"
    else:
        nowk = "4"

    reptq = "Y" if (reptdate + timedelta(days=1)).day == 1 else "N"

    return {
        "REPTDATE": reptdate,
        "NOWK": nowk,
        "REPTQ": reptq,
        "REPTYR": reptdate.year,
        "REPTMON": reptdate.month,
        "REPTDAY": reptdate.day,
    }


# ============================================================================
# PROCESS PBIF
# ============================================================================

def process_pbif(macro_vars: dict) -> pl.DataFrame:
    """Process PBIF data and output BNMCODE/AMOUNT records."""
    if not PBIF_PATH.exists():
        raise FileNotFoundError(f"PBIF file not found: {PBIF_PATH}")

    df = CON.execute(
        "SELECT MATDTE, CUSTCX, BALANCE FROM read_parquet(?)",
        [str(PBIF_PATH)],
    ).pl()

    reptdate = macro_vars["REPTDATE"]

    df = df.with_columns(
        matdte=pl.col("MATDTE").map_elements(to_date, return_dtype=pl.Date),
        custcx=pl.col("CUSTCX").cast(pl.Utf8),
        balance=pl.col("BALANCE").cast(pl.Float64),
    )

    days = (pl.col("matdte") - pl.lit(reptdate)).dt.days()
    remmth_calc = pl.col("matdte").map_elements(
        lambda d: calculate_remaining_months(d, reptdate) if d else None,
        return_dtype=pl.Float64,
    )

    remmth = (
        pl.when(days.is_null() | (days < 8))
        .then(0.1)
        .otherwise(remmth_calc)
    )

    cust = pl.when(pl.col("custcx").is_in(["77", "78", "95", "96"]))
    cust = cust.then("08").otherwise("09")

    remfmt = remmth.map_elements(get_rem_format, return_dtype=pl.Utf8)

    base_fields = {
        "AMOUNT": pl.col("balance"),
    }

    df_95211 = df.select(
        pl.concat_str([pl.lit("95211"), cust, remfmt, pl.lit("0000Y")]).alias("BNMCODE"),
        **base_fields,
    )
    df_93211 = df.select(
        pl.concat_str([pl.lit("93211"), cust, remfmt, pl.lit("0000Y")]).alias("BNMCODE"),
        **base_fields,
    )

    return pl.concat([df_95211, df_93211])


def aggregate_output(df_output: pl.DataFrame) -> pl.DataFrame:
    """Aggregate amounts by BNMCODE (PROC SUMMARY NWAY)."""
    if df_output.is_empty():
        return df_output

    return df_output.group_by("BNMCODE").agg(pl.col("AMOUNT").sum())


def write_output_txt(df_agg: pl.DataFrame) -> None:
    """Write BNMCODE/AMOUNT output to text file."""
    with open(PBIF_OUTPUT_PATH, "w", encoding="utf-8") as file_handle:
        for row in df_agg.iter_rows(named=True):
            file_handle.write(f"{row['BNMCODE']}|{row['AMOUNT']:.2f}\n")


def main() -> int:
    macro_vars = read_reptdate()
    df_output = process_pbif(macro_vars)
    df_agg = aggregate_output(df_output)
    write_output_txt(df_agg)
    print(f"Wrote PBIF output to {PBIF_OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
