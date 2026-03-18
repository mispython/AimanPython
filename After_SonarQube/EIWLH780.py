#!/usr/bin/env python3
"""
File Name: EIWLH780.py
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


def split_lnhist_transactions(lnhist: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Split loan history into the transaction sets needed by downstream outputs."""
    if lnhist.is_empty() or "TRANCODE" not in lnhist.columns:
        return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()

    histnew = lnhist.filter(pl.col("TRANCODE") == 780)
    hist310 = lnhist.filter(pl.col("TRANCODE") == 310)

    if {"ACCTNO", "NOTENO"}.issubset(lnhist.columns):
        hist750 = lnhist.filter(pl.col("TRANCODE") == 750).select(["ACCTNO", "NOTENO"])
    else:
        hist750 = pl.DataFrame()

    return histnew, hist310, hist750


def merge_month_filtered_history(path: Path, new_rows: pl.DataFrame, month_start: date) -> pl.DataFrame:
    """Keep prior-month history rows and append the current run's rows."""
    history = read_parquet(path)
    if not history.is_empty():
        history = ensure_date_column(history, "POSTDATE")
        history = history.filter((pl.col("POSTDATE") < month_start) | pl.col("POSTDATE").is_null())
    return safe_concat([history, new_rows])


def build_tzero310_history(hist310: pl.DataFrame) -> pl.DataFrame:
    """Build the zero-amount 310 history with one row kept per account/note pair."""
    tzero310 = pl.DataFrame()
    if "TOTAMT" in hist310.columns and {"ACCTNO", "NOTENO"}.issubset(hist310.columns):
        tzero310 = hist310.filter(pl.col("TOTAMT") == 0).select(["ACCTNO", "NOTENO"])
        tzero310 = tzero310.with_columns([pl.lit("Y").alias("FRTDIDTIND"), pl.lit(None).alias("POSTDATE1")])

        history = safe_concat([tzero310, read_parquet(H310_TZERO310_FILE)])
        if history.is_empty():
            return ensure_columns(history, {"ACCTNO": pl.lit(None), "NOTENO": pl.lit(None)})

        history = history.with_columns(pl.col("FRTDIDTIND").fill_null("").alias("_FRTDIDTIND_SORT"))
        history = nodupkey(
            history,
            keys=["ACCTNO", "NOTENO"],
            sort_by=["ACCTNO", "NOTENO", "_FRTDIDTIND_SORT"],
            descending=[False, False, True],
        ).drop("_FRTDIDTIND_SORT")
        return ensure_columns(history, {"ACCTNO": pl.lit(None), "NOTENO": pl.lit(None)})


def build_first750_history(hist750: pl.DataFrame) -> pl.DataFrame:
    """Build the first 750 transaction history with duplicate keys removed."""
    history = safe_concat([hist750, read_parquet(H750_FIRST750_FILE)])
    if not history.is_empty():
        history = nodupkey(
            history,
        keys=["ACCTNO", "NOTENO"],
        sort_by=["ACCTNO", "NOTENO"],
        descending=[False, False],
    )
    return ensure_columns(history, {"ACCTNO": pl.lit(None), "NOTENO": pl.lit(None)})


def prepare_hist310(hist310: pl.DataFrame) -> pl.DataFrame:
    """Add the discharge indicator fields expected in 310 history rows."""
    if hist310.is_empty():
        return hist310

    postdate_expr = pl.col("POSTDATE") if "POSTDATE" in hist310.columns else pl.lit(None)
    return hist310.with_columns([pl.lit("Y").alias("FRTDIDTIND"), postdate_expr.alias("POSTDATE1")])


def build_h310_lnhist310(month_start: date, hist310: pl.DataFrame) -> pl.DataFrame:
    """Merge current 310 transactions with historical rows and keep the latest per key."""
    history = merge_month_filtered_history(H310_LNHIST310_FILE, pl.DataFrame(), month_start)
    combined = safe_concat([history, prepare_hist310(hist310)])
    if combined.is_empty():
        return combined

    if "EFFDATE" in combined.columns:
        combined = ensure_date_column(combined, "EFFDATE")
        combined = combined.with_columns(pl.col("EFFDATE").fill_null(date(1, 1, 1)).alias("_EFFDATE_SORT"))
        combined = combined.sort(["ACCTNO", "NOTENO", "_EFFDATE_SORT"], descending=[False, False, True]).drop(
            "_EFFDATE_SORT")
    else:
        combined = combined.sort(["ACCTNO", "NOTENO"], descending=[False, False])

    return nodupkey(combined, keys=["ACCTNO", "NOTENO"])


def update_discharge_history(
        h310_lnhist310: pl.DataFrame,
        h750_first750: pl.DataFrame,
        h310_tzero310: pl.DataFrame,
) -> pl.DataFrame:
    """Apply first-discharge and last-discharge updates to the 310 history output."""
    if h310_lnhist310.is_empty():
        return h310_lnhist310

    merged = h310_lnhist310.join(
        h750_first750.with_columns(pl.lit(True).alias("_B_EXISTS")),
        on=["ACCTNO", "NOTENO"],
        how="left",
    )
    merged = merged.join(
        h310_tzero310.with_columns(pl.lit(True).alias("_C_EXISTS")),
        on=["ACCTNO", "NOTENO"],
        how="left",
        suffix="_C",
    )

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
            pl.when(pl.col("_C_EXISTS").is_not_null()).then(pl.col("FRTDIDTIND_C")).otherwise(
                pl.col("FRTDIDTIND")).alias("FRTDIDTIND")
        )
    if "POSTDATE1_C" in merged.columns:
        merged = merged.with_columns(
            pl.when(pl.col("_C_EXISTS").is_not_null()).then(pl.col("POSTDATE1_C")).otherwise(pl.col("POSTDATE1")).alias(
                "POSTDATE1")
        )

    merged = merged.with_columns([
        pl.col("FRTDISCODE").cast(pl.Utf8, strict=False),
        pl.col("LSTDISCODE").cast(pl.Utf8, strict=False),
    ])
    merged = merged.with_columns([
        pl.col("FRTDISCODE").fill_null("310").alias("FRTDISCODE"),
        pl.col("LSTDISCODE").fill_null("310").alias("LSTDISCODE"),
    ])
    merged = merged.with_columns([
        pl.when(pl.col("_C_EXISTS").is_not_null()).then(pl.lit("311")).otherwise(pl.col("FRTDISCODE")).alias(
            "FRTDISCODE"),
        pl.when(pl.col("_C_EXISTS").is_not_null()).then(pl.lit("311")).otherwise(pl.col("LSTDISCODE")).alias(
            "LSTDISCODE"),
    ])
    merged = merged.with_columns(
        pl.when(pl.col("_B_EXISTS").is_not_null() & (pl.col("LSTDISCODE") != "750")).then(pl.lit("750")).otherwise(
            pl.col("LSTDISCODE")).alias("LSTDISCODE")
    )

    return merged.drop([col for col in merged.columns if col.endswith("_C") or col in {"_B_EXISTS", "_C_EXISTS"}])


# ============================================================================
# Main Processing
# ============================================================================

def main() -> None:
    dates = get_report_dates()
    month_start = dates["MTH_D1"]

    lnhist = read_parquet(LNHIST_FILE)
    if not lnhist.is_empty():
        lnhist = ensure_date_column(lnhist, "POSTDATE")

    histnew, hist310, hist750 = split_lnhist_transactions(lnhist)

    hist_lnhist780 = merge_month_filtered_history(HIST_LNHIST780_FILE, histnew, month_start)
    write_parquet(hist_lnhist780, HIST_LNHIST780_OUT)

    h310_tzero310 = build_tzero310_history(hist310)
    write_parquet(h310_tzero310, H310_TZERO310_OUT)

    h750_first750 = build_first750_history(hist750)
    write_parquet(h750_first750, H750_FIRST750_OUT)

    h310_lnhist310 = build_h310_lnhist310(month_start, hist310)
    h310_lnhist310 = update_discharge_history(h310_lnhist310, h750_first750, h310_tzero310)
    write_parquet(h310_lnhist310, H310_LNHIST310_OUT)


if __name__ == "__main__":
    main()
