#!/usr/bin/env python3
"""
File Name: EIBMNLF2

Assumptions implemented from requirements:
- All inputs are parquet files with SAS column names.
- Output datasets are text files (no packed-decimal/binary fields required).
- DuckDB is used for parquet processing and Polars for transformations.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import polars as pl


# =============================================================================
# PATH SETUP (DEFINED EARLY)
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "data" / "input"
OUTPUT_DIR = BASE_DIR / "data" / "output"

LOAN_REPTDATE_PATH = INPUT_DIR / "loan" / "REPTDATE.parquet"
WALK_PATH = INPUT_DIR / "WALK.parquet"
NOTE_PATH = INPUT_DIR / "NOTE.parquet"
CALC_PATH = INPUT_DIR / "CALC.parquet"
PBIF_PATH = INPUT_DIR / "PBIF.parquet"
BT_PATH = INPUT_DIR / "BT.parquet"

GLSET_PATH = OUTPUT_DIR / "GLSET.txt"
FINAL_PATH = OUTPUT_DIR / "FINAL.txt"
FINALSUM_PATH = OUTPUT_DIR / "FINALSUM.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# DATE/PARAMETER HELPERS
# =============================================================================
SAS_EPOCH = date(1960, 1, 1)


@dataclass
class RuntimeParams:
    reptdate: date
    nowk: str
    rdate: str
    rdat1: int
    reptmon: str
    reptday: str


def to_date(value: object) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return datetime.fromisoformat(value).date()
    if isinstance(value, int):
        return SAS_EPOCH + timedelta(days=value)
    raise TypeError(f"Unsupported REPTDATE value: {value!r}")


def to_sas_date(value: date) -> int:
    return (value - SAS_EPOCH).days


def read_runtime_params(con: duckdb.DuckDBPyConnection) -> RuntimeParams:
    row = con.execute(
        "SELECT REPTDATE FROM read_parquet(?) LIMIT 1",
        [str(LOAN_REPTDATE_PATH)],
    ).fetchone()
    if row is None:
        raise ValueError("REPTDATE parquet is empty.")

    reptdate = to_date(row[0])
    day = reptdate.day

    if day == 8:
        wk = "1"
    elif day == 15:
        wk = "2"
    elif day == 22:
        wk = "3"
    else:
        wk = "0"

    next_month_first = (
        date(reptdate.year + 1, 1, 1)
        if reptdate.month == 12
        else date(reptdate.year, reptdate.month + 1, 1)
    )
    prev_day = next_month_first - timedelta(days=1)
    if reptdate == prev_day:
        wk = "4"

    return RuntimeParams(
        reptdate=reptdate,
        nowk=wk,
        rdate=reptdate.strftime("%d%m%Y"),
        rdat1=to_sas_date(reptdate),
        reptmon=f"{reptdate.month:02d}",
        reptday=f"{reptdate.day:02d}",
    )


# =============================================================================
# CORE TRANSFORMATIONS
# =============================================================================
def load_walk_for_reptdate(con: duckdb.DuckDBPyConnection, rdat1: int) -> pl.DataFrame:
    walk_df = con.execute("SELECT DESC, DD, MM, YY, AMOUNT FROM read_parquet(?)", [str(WALK_PATH)]).pl()

    if walk_df.is_empty():
        return pl.DataFrame(schema={"DESC": pl.Utf8, "AMOUNT": pl.Float64})

    walk_df = walk_df.with_columns(
        pl.date(
            pl.when(pl.col("YY") < 50)
            .then(pl.col("YY") + 2000)
            .otherwise(pl.col("YY") + 1900)
            .cast(pl.Int32),
            pl.col("MM").cast(pl.Int8),
            pl.col("DD").cast(pl.Int8),
        ).alias("_DATE")
    ).with_columns(((pl.col("_DATE") - pl.lit(SAS_EPOCH)).dt.total_days()).alias("REPTDATE_SAS"))

    return (
        walk_df.filter(pl.col("REPTDATE_SAS") == rdat1)
        .select(pl.col("DESC").cast(pl.Utf8), pl.col("AMOUNT").cast(pl.Float64))
    )


def make_defaults(prefix_a: str, prefix_b: str) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for n in range(1, 7):
        rows.append({"BNMCODE": f"{prefix_a}{n}0000Y", "AMOUNT": 0.0})
        rows.append({"BNMCODE": f"{prefix_b}{n}0000Y", "AMOUNT": 0.0})
    return pl.DataFrame(rows)


def summarize_with_defaults(
    walk_df: pl.DataFrame,
    desc_values: list[str],
    defaults: pl.DataFrame,
    mapped_codes: list[str],
) -> pl.DataFrame:
    amount = float(
        walk_df.filter(pl.col("DESC").is_in(desc_values))
        .select(pl.col("AMOUNT").sum().fill_null(0.0).alias("S"))["S"][0]
    )

    mapped = pl.DataFrame({"BNMCODE": mapped_codes, "AMOUNT": [amount, amount]})

    return (
        defaults.join(mapped, on="BNMCODE", how="left", suffix="_NEW")
        .with_columns(pl.coalesce([pl.col("AMOUNT_NEW"), pl.col("AMOUNT")]).alias("AMOUNT"))
        .select("BNMCODE", "AMOUNT")
    )


def summarize_direct(walk_df: pl.DataFrame, desc_values: list[str], mapped_codes: list[str]) -> pl.DataFrame:
    amount = float(
        walk_df.filter(pl.col("DESC").is_in(desc_values))
        .select(pl.col("AMOUNT").sum().fill_null(0.0).alias("S"))["S"][0]
    )
    return pl.DataFrame({"BNMCODE": mapped_codes, "AMOUNT": [amount, amount]})


def write_dataset_txt(path: Path, df: pl.DataFrame) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in df.iter_rows(named=True):
            handle.write(f"{row['BNMCODE']:<12}{float(row['AMOUNT']):>20.2f}\n")


def main() -> None:
    con = duckdb.connect()

    params = read_runtime_params(con)
    walk_df = load_walk_for_reptdate(con, params.rdat1)

    fs = summarize_with_defaults(
        walk_df,
        ["ML34170"],
        make_defaults("93219090", "95219090"),
        ["9321909050000Y", "9521909050000Y"],
    )
    hp = summarize_with_defaults(
        walk_df,
        ["ML34111"],
        make_defaults("93211090", "95211090"),
        ["9321109060000Y", "9521109060000Y"],
    )
    cash = summarize_direct(walk_df, ["39110"], ["9322100010000Y", "9522100010000Y"])
    srr = summarize_with_defaults(
        walk_df,
        ["32110"],
        make_defaults("93222000", "95222000"),
        ["9322200060000Y", "9522200060000Y"],
    )
    oth = summarize_direct(walk_df, ["F137010SH"], ["9322900010000Y", "9522900010000Y"])
    fcyfl = summarize_with_defaults(
        walk_df,
        ["F13460081BCB", "F13460064FLB"],
        make_defaults("94211090", "96211090"),
        ["9421109060000Y", "9621109060000Y"],
    )
    fcyrc = summarize_with_defaults(
        walk_df,
        ["F134600RC"],
        make_defaults("94212090", "96212090"),
        ["9421209060000Y", "9621209060000Y"],
    )
    fcycash = summarize_with_defaults(
        walk_df,
        ["F139610FXNC"],
        make_defaults("94221000", "96221000"),
        ["9422100010000Y", "9622100010000Y"],
    )

    glset = pl.concat([fs, hp, cash, srr, oth, fcyfl, fcycash, fcyrc], how="vertical")

    note = con.execute("SELECT BNMCODE, AMOUNT FROM read_parquet(?)", [str(NOTE_PATH)]).pl()
    calc = con.execute("SELECT BNMCODE, AMOUNT FROM read_parquet(?)", [str(CALC_PATH)]).pl()
    pbif = con.execute("SELECT BNMCODE, AMOUNT FROM read_parquet(?)", [str(PBIF_PATH)]).pl()
    bt = con.execute("SELECT BNMCODE, AMOUNT FROM read_parquet(?)", [str(BT_PATH)]).pl()

    final = pl.concat([note, calc, pbif, bt, glset], how="vertical").with_columns(
        pl.col("BNMCODE").cast(pl.Utf8),
        pl.col("AMOUNT").cast(pl.Float64),
    )

    finalsum = final.group_by("BNMCODE").agg(pl.col("AMOUNT").sum().alias("AMOUNT"))

    write_dataset_txt(GLSET_PATH, glset)
    write_dataset_txt(FINAL_PATH, final)
    write_dataset_txt(FINALSUM_PATH, finalsum)

    print(f"REPTDATE: {params.reptdate.isoformat()} (NOWK={params.nowk}, RDAT1={params.rdat1})")
    print(f"GLSET records: {glset.height}")
    print(f"FINAL records: {final.height}")
    print(f"FINALSUM records: {finalsum.height}")
    print(f"Output written: {GLSET_PATH}, {FINAL_PATH}, {FINALSUM_PATH}")


if __name__ == "__main__":
    main()
