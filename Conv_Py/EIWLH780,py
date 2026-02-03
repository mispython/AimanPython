#!/usr/bin/env python3
"""
File Name: EIWLH780
Append transaction history for TRANCODE 780 and update discharge history.
"""

from __future__ import annotations

import calendar
from datetime import date, datetime
from pathlib import Path

import duckdb
import polars as pl


# ============================================================================
# Configuration and Path Setup
# ============================================================================

INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPTDATE_FILE = INPUT_DIR / "reptdate.parquet"
LNHIST_FILE = INPUT_DIR / "lnhist.parquet"

HIST_LNHIST780_FILE = INPUT_DIR / "hist_lnhist780.parquet"
H310_LNHIST310_FILE = INPUT_DIR / "h310_lnhist310.parquet"
H310_TZERO310_FILE = INPUT_DIR / "h310_tzero310.parquet"
H750_FIRST750_FILE = INPUT_DIR / "h750_first750.parquet"

HIST_LNHIST780_OUT = OUTPUT_DIR / "hist_lnhist780.parquet"
H310_LNHIST310_OUT = OUTPUT_DIR / "h310_lnhist310.parquet"
H310_TZERO310_OUT = OUTPUT_DIR / "h310_tzero310.parquet"
H750_FIRST750_OUT = OUTPUT_DIR / "h750_first750.parquet"


# ============================================================================
# Utility Functions
# ============================================================================


def read_parquet(path: Path) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame()
    with duckdb.connect() as con:
        return con.execute(f"SELECT * FROM read_parquet('{path.as_posix()}')").pl()


def write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def ensure_date_column(df: pl.DataFrame, column: str) -> pl.DataFrame:
    if column not in df.columns:
        return df
    return df.with_columns(pl.col(column).cast(pl.Date, strict=False))


def safe_concat(dfs: list[pl.DataFrame]) -> pl.DataFrame:
    non_empty = [df for df in dfs if not df.is_empty() or df.columns]
    if not non_empty:
        return pl.DataFrame()
    return pl.concat(non_empty, how="diagonal")


def nodupkey(df: pl.DataFrame, keys: list[str], sort_by: list[str] | None = None,
             descending: list[bool] | None = None) -> pl.DataFrame:
    if df.is_empty():
        return df
    if sort_by:
        df = df.sort(sort_by, descending=descending)
    return df.unique(subset=keys, keep="first")


def ensure_columns(df: pl.DataFrame, columns: dict[str, pl.Expr]) -> pl.DataFrame:
    missing = [name for name in columns if name not in df.columns]
    if not missing:
        return df
    return df.with_columns([columns[name].alias(name) for name in missing])


# ============================================================================
# Date Processing
# ============================================================================


def get_report_dates() -> dict[str, date]:
    reptdate_df = read_parquet(REPTDATE_FILE)
    if reptdate_df.is_empty():
        raise ValueError("REPTDATE file is empty or missing.")

    reptdate_val = reptdate_df.select("REPTDATE").item(0, 0)
    if isinstance(reptdate_val, str):
        reptdate_val = datetime.strptime(reptdate_val, "%Y-%m-%d").date()
    elif isinstance(reptdate_val, datetime):
        reptdate_val = reptdate_val.date()

    mth_d1 = date(reptdate_val.year, reptdate_val.month, 1)
    mthend = date(
        reptdate_val.year,
        reptdate_val.month,
        calendar.monthrange(reptdate_val.year, reptdate_val.month)[1],
    )

    return {
        "REPTDATE": reptdate_val,
        "MTH_D1": mth_d1,
        "MTHEND": mthend,
        "REPTMON": f"{reptdate_val.month:02d}",
    }


# ============================================================================
# Main Processing
# ============================================================================


def main() -> None:
    dates = get_report_dates()
    mth_d1 = dates["MTH_D1"]

    lnhist = read_parquet(LNHIST_FILE)
    if not lnhist.is_empty():
        lnhist = ensure_date_column(lnhist, "POSTDATE")

    histnew = lnhist.filter(pl.col("TRANCODE") == 780) if "TRANCODE" in lnhist.columns else pl.DataFrame()
    hist310 = lnhist.filter(pl.col("TRANCODE") == 310) if "TRANCODE" in lnhist.columns else pl.DataFrame()
    hist750 = (
        lnhist.filter(pl.col("TRANCODE") == 750).select(["ACCTNO", "NOTENO"])
        if {"TRANCODE", "ACCTNO", "NOTENO"}.issubset(lnhist.columns)
        else pl.DataFrame()
    )

    hist_lnhist780 = read_parquet(HIST_LNHIST780_FILE)
    if not hist_lnhist780.is_empty():
        hist_lnhist780 = ensure_date_column(hist_lnhist780, "POSTDATE")
        hist_lnhist780 = hist_lnhist780.filter(
            (pl.col("POSTDATE") < mth_d1) | pl.col("POSTDATE").is_null()
        )
    hist_lnhist780 = safe_concat([hist_lnhist780, histnew])
    write_parquet(hist_lnhist780, HIST_LNHIST780_OUT)

    h310_lnhist310 = read_parquet(H310_LNHIST310_FILE)
    if not h310_lnhist310.is_empty():
        h310_lnhist310 = ensure_date_column(h310_lnhist310, "POSTDATE")
        h310_lnhist310 = h310_lnhist310.filter(
            (pl.col("POSTDATE") < mth_d1) | pl.col("POSTDATE").is_null()
        )

    tzero310 = pl.DataFrame()
    if "TOTAMT" in hist310.columns and {"ACCTNO", "NOTENO"}.issubset(hist310.columns):
        tzero310 = hist310.filter(pl.col("TOTAMT") == 0).select(["ACCTNO", "NOTENO"])
        tzero310 = tzero310.with_columns(
            [
                pl.lit("Y").alias("FRTDIDTIND"),
                pl.lit(None).alias("POSTDATE1"),
            ]
        )

    h310_tzero310 = read_parquet(H310_TZERO310_FILE)
    h310_tzero310 = safe_concat([tzero310, h310_tzero310])
    if not h310_tzero310.is_empty():
        h310_tzero310 = h310_tzero310.with_columns(
            pl.col("FRTDIDTIND").fill_null("").alias("_FRTDIDTIND_SORT")
        )
        h310_tzero310 = nodupkey(
            h310_tzero310,
            keys=["ACCTNO", "NOTENO"],
            sort_by=["ACCTNO", "NOTENO", "_FRTDIDTIND_SORT"],
            descending=[False, False, True],
        ).drop("_FRTDIDTIND_SORT")
    h310_tzero310 = ensure_columns(
        h310_tzero310, {"ACCTNO": pl.lit(None), "NOTENO": pl.lit(None)}
    )
    write_parquet(h310_tzero310, H310_TZERO310_OUT)

    h750_first750 = read_parquet(H750_FIRST750_FILE)
    h750_first750 = safe_concat([hist750, h750_first750])
    if not h750_first750.is_empty():
        h750_first750 = nodupkey(
            h750_first750,
            keys=["ACCTNO", "NOTENO"],
            sort_by=["ACCTNO", "NOTENO"],
            descending=[False, False],
        )
    h750_first750 = ensure_columns(
        h750_first750, {"ACCTNO": pl.lit(None), "NOTENO": pl.lit(None)}
    )
    write_parquet(h750_first750, H750_FIRST750_OUT)

    if not hist310.is_empty():
        postdate_expr = pl.col("POSTDATE") if "POSTDATE" in hist310.columns else pl.lit(None)
        hist310 = hist310.with_columns(
            [
                pl.lit("Y").alias("FRTDIDTIND"),
                postdate_expr.alias("POSTDATE1"),
            ]
        )

    lnhist310 = safe_concat([h310_lnhist310, hist310])
    if not lnhist310.is_empty():
        if "EFFDATE" in lnhist310.columns:
            lnhist310 = ensure_date_column(lnhist310, "EFFDATE")
            lnhist310 = lnhist310.with_columns(
                pl.col("EFFDATE").fill_null(date(1, 1, 1)).alias("_EFFDATE_SORT")
            )
            lnhist310 = lnhist310.sort(
                ["ACCTNO", "NOTENO", "_EFFDATE_SORT"],
                descending=[False, False, True],
            ).drop("_EFFDATE_SORT")
        else:
            lnhist310 = lnhist310.sort(["ACCTNO", "NOTENO"], descending=[False, False])

        h310_lnhist310 = nodupkey(
            lnhist310,
            keys=["ACCTNO", "NOTENO"],
        )
    else:
        h310_lnhist310 = lnhist310

    if not h310_lnhist310.is_empty():
        b_ind = h750_first750.with_columns(pl.lit(True).alias("_B_EXISTS"))
        c_ind = h310_tzero310.with_columns(pl.lit(True).alias("_C_EXISTS"))

        merged = h310_lnhist310.join(b_ind, on=["ACCTNO", "NOTENO"], how="left")
        merged = merged.join(c_ind, on=["ACCTNO", "NOTENO"], how="left", suffix="_C")

        merged = ensure_columns(
            merged,
            {
                "FRTDIDTIND": pl.lit(None),
                "POSTDATE1": pl.lit(None),
                "FRTDISCODE": pl.lit(None),
                "LSTDISCODE": pl.lit(None),
            },
        )

        if "FRTDIDTIND_C" in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col("_C_EXISTS").is_not_null())
                .then(pl.col("FRTDIDTIND_C"))
                .otherwise(pl.col("FRTDIDTIND"))
                .alias("FRTDIDTIND")
            )
        if "POSTDATE1_C" in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col("_C_EXISTS").is_not_null())
                .then(pl.col("POSTDATE1_C"))
                .otherwise(pl.col("POSTDATE1"))
                .alias("POSTDATE1")
            )

        merged = merged.with_columns(
            [
                pl.col("FRTDISCODE").cast(pl.Utf8, strict=False),
                pl.col("LSTDISCODE").cast(pl.Utf8, strict=False),
            ]
        )
        merged = merged.with_columns(
            [
                pl.col("FRTDISCODE").fill_null("310").alias("FRTDISCODE"),
                pl.col("LSTDISCODE").fill_null("310").alias("LSTDISCODE"),
            ]
        )
        merged = merged.with_columns(
            [
                pl.when(pl.col("_C_EXISTS").is_not_null())
                .then(pl.lit("311"))
                .otherwise(pl.col("FRTDISCODE"))
                .alias("FRTDISCODE"),
                pl.when(pl.col("_C_EXISTS").is_not_null())
                .then(pl.lit("311"))
                .otherwise(pl.col("LSTDISCODE"))
                .alias("LSTDISCODE"),
            ]
        )
        merged = merged.with_columns(
            pl.when(pl.col("_B_EXISTS").is_not_null() & (pl.col("LSTDISCODE") != "750"))
            .then(pl.lit("750"))
            .otherwise(pl.col("LSTDISCODE"))
            .alias("LSTDISCODE")
        )

        h310_lnhist310 = merged.drop(
            [col for col in merged.columns if col.endswith("_C") or col in {"_B_EXISTS", "_C_EXISTS"}]
        )

    write_parquet(h310_lnhist310, H310_LNHIST310_OUT)


if __name__ == "__main__":
    main()
