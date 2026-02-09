#!/usr/bin/env python3
"""
File Name: NPLOBAL6
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl


# =============================================================================
# PATH SETUP (DEFINED EARLY AS REQUESTED)
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"

NPLX_IIS_FILE = INPUT_DIR / "nplx_iis.parquet"
NPLX_SP1_FILE = INPUT_DIR / "nplx_sp1.parquet"
NPLX_SP2_FILE = INPUT_DIR / "nplx_sp2.parquet"
NPLX_AQ_FILE = INPUT_DIR / "nplx_aq.parquet"

NPL_NPLOBAL_FILE = OUTPUT_DIR / "npl_nplobal.txt"
NPL_WAQ_FILE = OUTPUT_DIR / "npl_waq.txt"
NPL_WIIS_FILE = OUTPUT_DIR / "npl_wiis.txt"
NPL_WSP2_FILE = OUTPUT_DIR / "npl_wsp2.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def read_parquet(con: duckdb.DuckDBPyConnection, path: Path, columns: list[str]) -> pl.DataFrame:
    """Read parquet using DuckDB and ensure required columns exist."""
    if not path.exists():
        return pl.DataFrame({col: [] for col in columns})

    df = con.execute("SELECT * FROM read_parquet(?)", [str(path)]).pl()
    for col in columns:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))
    return df.select(columns)


def write_pipe_delimited(df: pl.DataFrame, path: Path) -> None:
    """Write a pipe-delimited text file with headers."""
    df.write_csv(path, separator="|", include_header=True, null_value="")


# =============================================================================
# MAIN PROCESSING
# =============================================================================
def main() -> None:
    con = duckdb.connect()

    iis_cols = [
        "BRANCH",
        "NTBRCH",
        "ACCTNO",
        "NOTENO",
        "NAME",
        "BORSTAT",
        "DAYS",
        "IIS",
        "OI",
        "UHC",
        "SP",
    ]
    sp_cols = ["ACCTNO", "NOTENO", "SP"]
    aq_cols = ["ACCTNO", "NOTENO", "NPL", "CURBAL"]

    iis_df = read_parquet(con, NPLX_IIS_FILE, iis_cols)
    sp2_df = read_parquet(con, NPLX_SP2_FILE, sp_cols)
    sp1_df = read_parquet(con, NPLX_SP1_FILE, sp_cols)
    aq_df = read_parquet(con, NPLX_AQ_FILE, aq_cols)

    nplobal_base = (
        iis_df.join(sp2_df, on=["ACCTNO", "NOTENO"], how="full")
        .with_columns(pl.lit(3).cast(pl.Int64).alias("LOANSTAT"))
        .rename(
            {
                "IIS": "IISP",
                "OI": "OIP",
                "UHC": "UHCP",
                "SP": "SPP2",
            }
        )
    )

    sp1_df = sp1_df.rename({"SP": "SPP1"})
    sp2_df = sp2_df.rename({"SP": "SPP2"})

    aq_df = (
        aq_df.with_columns(
            [
                pl.col("NPL").fill_null(0.0),
                pl.col("CURBAL").fill_null(0.0),
            ]
        )
        .rename({"NPL": "NETBALP", "CURBAL": "CURBALP"})
        .select(["ACCTNO", "NOTENO", "NETBALP", "CURBALP"])
        .unique(subset=["ACCTNO", "NOTENO"], keep="first")

    )

    all_df = (
        sp1_df.join(sp2_df, on=["ACCTNO", "NOTENO"], how="full")
        # .join(aq_df, on=["ACCTNO", "NOTENO"], how="full")
        .join(aq_df, on=["ACCTNO", "NOTENO"], how="left")
        .with_columns(
            [
                pl.col("SPP1").fill_null(0.0),
                pl.col("NETBALP").fill_null(0.0),
                pl.col("CURBALP").fill_null(0.0),
                pl.lit(700).cast(pl.Int64).alias("LOANTYPE"),
            ]
        )
        # .drop("SPP2")
    )

    nplobal_final = (
        nplobal_base.join(all_df, on=["ACCTNO", "NOTENO"], how="left")
        .with_columns(pl.lit("Y").alias("EXIST"))
        .filter((pl.col("NTBRCH") > 0) & (pl.col("LOANTYPE") > 0))
        .sort("ACCTNO")
        .unique(subset=["ACCTNO"], keep="first")
    )

    output_columns = [
        "BRANCH",
        "NTBRCH",
        "ACCTNO",
        "NOTENO",
        "NAME",
        "BORSTAT",
        "DAYS",
        "IISP",
        "OIP",
        "LOANSTAT",
        "UHCP",
        "SPP2",
        "SPP1",
        "NETBALP",
        "CURBALP",
        "LOANTYPE",
        "EXIST",
    ]
    for col in output_columns:
        if col not in nplobal_final.columns:
            nplobal_final = nplobal_final.with_columns(pl.lit(None).alias(col))

    write_pipe_delimited(nplobal_final.select(output_columns), NPL_NPLOBAL_FILE)

    empty_df = pl.DataFrame()
    write_pipe_delimited(empty_df, NPL_WAQ_FILE)
    write_pipe_delimited(empty_df, NPL_WIIS_FILE)
    write_pipe_delimited(empty_df, NPL_WSP2_FILE)


if __name__ == "__main__":
    main()
