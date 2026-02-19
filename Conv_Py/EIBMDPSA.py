# !/usr/bin/env python3
"""
Program: EIBMDPSA
Purpose: Savings account opened/closed monthly report converted from SAS.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb
import polars as pl


# =============================================================================
# Path and Runtime Setup
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
MIS_DIR = OUTPUT_DIR / "mis"
REPORT_DIR = OUTPUT_DIR / "report"

REPTDATE_FILE = INPUT_DIR / "reptdate.parquet"
SAVING_FILE = INPUT_DIR / "saving.parquet"

MIS_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)

PAGE_LENGTH = 60
ASA_SPACE = " "
ASA_DOUBLE = "0"
ASA_NEWPAGE = "1"


@dataclass(frozen=True)
class MacroVars:
    rdate: str
    ryear: str
    rmonth: str
    reptmon: str
    reptmon1: str
    rday: str


def load_macro_vars() -> MacroVars:
    """Equivalent of DATA REPTDATE and CALL SYMPUT macro setup."""
    rept_df = pl.read_parquet(REPTDATE_FILE)
    if rept_df.is_empty():
        raise ValueError("reptdate.parquet has no rows")

    reptdate = rept_df.select(pl.col("REPTDATE").cast(pl.Date)).item(0, 0)
    if not isinstance(reptdate, date):
        reptdate = date.fromisoformat(str(reptdate))

    mm = reptdate.month
    mm1 = 12 if mm - 1 == 0 else mm - 1

    return MacroVars(
        rdate=reptdate.strftime("%d/%m/%y"),
        ryear=f"{reptdate.year:04d}",
        rmonth=f"{reptdate.month:02d}",
        reptmon=f"{mm:02d}",
        reptmon1=f"{mm1:02d}",
        rday=f"{reptdate.day:02d}",
    )


def build_savg() -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build SAVG and SAO exactly per SAS filtering logic."""
    con = duckdb.connect()
    query = f"""
        WITH base AS (
            SELECT
                CAST(BRANCH AS INTEGER) AS BRANCH,
                CAST(PRODUCT AS INTEGER) AS PRODUCT,
                CAST(OPENMH AS DOUBLE) AS OPENMH,
                CAST(CLOSEMH AS DOUBLE) AS CLOSEMH,
                CAST(CURBAL AS DOUBLE) AS CURBAL,
                CAST(OPENIND AS VARCHAR) AS OPENIND
            FROM read_parquet('{SAVING_FILE.as_posix()}')
            WHERE ((PRODUCT BETWEEN 200 AND 206) OR PRODUCT = 208 OR (PRODUCT BETWEEN 212 AND 215))
              AND BRANCH <> 227
        ), recoded AS (
            SELECT
                CASE WHEN BRANCH = 250 THEN 92 ELSE BRANCH END AS BRANCH,
                PRODUCT,
                OPENMH,
                CLOSEMH,
                CURBAL,
                CASE WHEN OPENIND = 'Z' THEN 'O' ELSE OPENIND END AS OPENIND
            FROM base
            WHERE NOT (OPENMH = 1 AND CLOSEMH = 1)
        ), subset_if AS (
            SELECT *
            FROM recoded
            WHERE (OPENIND = 'O') OR (OPENIND IN ('B','C','P') AND CLOSEMH = 1)
        ), final_savg AS (
            SELECT
                BRANCH,
                PRODUCT,
                OPENMH,
                CASE WHEN OPENIND IN ('B','C','P') THEN CLOSEMH ELSE 0 END AS CLOSEMH,
                CURBAL,
                CASE WHEN OPENIND IN ('B','C','P') THEN 0 ELSE 1 END AS NOACCT,
                CASE WHEN OPENIND = 'B' THEN CLOSEMH ELSE 0 END AS BCLOSE,
                CASE WHEN OPENIND = 'C' THEN CLOSEMH ELSE 0 END AS CCLOSE
            FROM subset_if
        )
        SELECT * FROM final_savg
    """
    savg_detail = con.execute(query).pl()

    sao = (
        savg_detail
        .filter(pl.col("OPENMH") == 1)
        .group_by("BRANCH")
        .agg([
            pl.sum("OPENMH").alias("OPENMH"),
            pl.sum("CURBAL").alias("CURBAL"),
        ])
        .sort("BRANCH")
    )

    savg = (
        savg_detail
        .group_by(["BRANCH", "PRODUCT"])
        .agg([
            pl.sum("OPENMH").alias("OPENMH"),
            pl.sum("CURBAL").alias("CURBAL"),
            pl.sum("NOACCT").alias("NOACCT"),
            pl.sum("CLOSEMH").alias("CLOSEMH"),
            pl.sum("BCLOSE").alias("BCLOSE"),
            pl.sum("CCLOSE").alias("CCLOSE"),
        ])
        .sort(["BRANCH", "PRODUCT"])
    )
    con.close()
    return savg, sao


def apply_process_macro(savg: pl.DataFrame, macro: MacroVars) -> pl.DataFrame:
    """Equivalent of %PROCESS macro."""
    if macro.reptmon > "01":
        prior_file = MIS_DIR / f"savg1{macro.reptmon1}.parquet"
        if prior_file.exists():
            savp = pl.read_parquet(prior_file)
            savp = (
                savp
                .select([
                    pl.col("BRANCH").cast(pl.Int64),
                    pl.col("PRODUCT").cast(pl.Int64),
                    pl.col("OPENCUM").cast(pl.Float64).alias("OPENCUX"),
                    pl.col("CLOSECUM").cast(pl.Float64).alias("CLOSECUX"),
                ])
                .with_columns(
                    pl.when(pl.col("BRANCH") == 227)
                    .then(pl.lit(81))
                    .when(pl.col("BRANCH") == 250)
                    .then(pl.lit(92))
                    .otherwise(pl.col("BRANCH"))
                    .alias("BRANCH")
                )
                .group_by(["BRANCH", "PRODUCT"])
                .agg([
                    pl.sum("OPENCUX").alias("OPENCUX"),
                    pl.sum("CLOSECUX").alias("CLOSECUX"),
                ])
            )
        else:
            savp = pl.DataFrame(
                schema={
                    "BRANCH": pl.Int64,
                    "PRODUCT": pl.Int64,
                    "OPENCUX": pl.Float64,
                    "CLOSECUX": pl.Float64,
                }
            )

        merged = savp.join(savg, on=["BRANCH", "PRODUCT"], how="full", coalesce=True)
        for col in ["OPENCUX", "CLOSECUX", "OPENMH", "CLOSEMH", "BCLOSE", "CCLOSE", "NOACCT", "CURBAL"]:
            if col not in merged.columns:
                merged = merged.with_columns(pl.lit(0.0).alias(col))

        result = (
            merged
            .with_columns([
                pl.col("OPENCUX").fill_null(0.0),
                pl.col("CLOSECUX").fill_null(0.0),
                pl.col("OPENMH").fill_null(0.0),
                pl.col("CLOSEMH").fill_null(0.0),
                pl.col("BCLOSE").fill_null(0.0),
                pl.col("CCLOSE").fill_null(0.0),
                pl.col("NOACCT").fill_null(0.0),
                pl.col("CURBAL").fill_null(0.0),
            ])
            .with_columns([
                (pl.col("OPENMH") + pl.col("OPENCUX")).alias("OPENCUM"),
                (pl.col("CLOSEMH") + pl.col("CLOSECUX")).alias("CLOSECUM"),
            ])
            .with_columns([
                (pl.col("OPENMH") - pl.col("CLOSEMH")).alias("NETCHGMH"),
                (pl.col("OPENCUM") - pl.col("CLOSECUM")).alias("NETCHGYR"),
            ])
        )
    else:
        result = (
            savg
            .with_columns([
                pl.col("OPENMH").alias("OPENCUM"),
                pl.col("CLOSEMH").alias("CLOSECUM"),
                (pl.col("OPENMH") - pl.col("CLOSEMH")).alias("NETCHGMH"),
                (pl.col("OPENMH") - pl.col("CLOSEMH")).alias("NETCHGYR"),
            ])
        )

    return result.sort(["BRANCH", "PRODUCT"])


def format_int_like(value: float) -> str:
    return f"{int(round(float(value))):,}"


def format_money(value: float) -> str:
    return f"{float(value):,.2f}"


def write_report(savg: pl.DataFrame, macro: MacroVars) -> None:
    """PROC TABULATE-style report output with ASA carriage control."""
    report_file = REPORT_DIR / f"eibmdpsa_{macro.reptmon}.txt"

    branch_summary = (
        savg
        .group_by("BRANCH")
        .agg([
            pl.sum("OPENMH").alias("OPENMH"),
            pl.sum("OPENCUM").alias("OPENCUM"),
            pl.sum("CLOSEMH").alias("CLOSEMH"),
            pl.sum("BCLOSE").alias("BCLOSE"),
            pl.sum("CCLOSE").alias("CCLOSE"),
            pl.sum("CLOSECUM").alias("CLOSECUM"),
            pl.sum("NOACCT").alias("NOACCT"),
            pl.sum("CURBAL").alias("CURBAL"),
            pl.sum("NETCHGMH").alias("NETCHGMH"),
            pl.sum("NETCHGYR").alias("NETCHGYR"),
        ])
        .sort("BRANCH")
    )

    totals = branch_summary.select([
        pl.sum("OPENMH").alias("OPENMH"),
        pl.sum("OPENCUM").alias("OPENCUM"),
        pl.sum("CLOSEMH").alias("CLOSEMH"),
        pl.sum("BCLOSE").alias("BCLOSE"),
        pl.sum("CCLOSE").alias("CCLOSE"),
        pl.sum("CLOSECUM").alias("CLOSECUM"),
        pl.sum("NOACCT").alias("NOACCT"),
        pl.sum("CURBAL").alias("CURBAL"),
        pl.sum("NETCHGMH").alias("NETCHGMH"),
        pl.sum("NETCHGYR").alias("NETCHGYR"),
    ])

    headers = [
        "BRANCH", "CURRENT MONTH OPENED", "CUMULATIVE OPENED", "CURRENT MONTH CLOSED",
        "CLOSED BY BANK", "CLOSED BY CUSTOMER", "CUMULATIVE CLOSED", "NO.OF ACCTS",
        "TOTAL (RM) O/S", "NET CHANGE MONTH", "NET CHANGE YTD",
    ]

    def row_line(label: str, row: dict[str, float]) -> str:
        return (
            f"{ASA_SPACE}{label:>6} "
            f"{format_int_like(row['OPENMH']):>20} "
            f"{format_int_like(row['OPENCUM']):>18} "
            f"{format_int_like(row['CLOSEMH']):>20} "
            f"{format_int_like(row['BCLOSE']):>16} "
            f"{format_int_like(row['CCLOSE']):>20} "
            f"{format_int_like(row['CLOSECUM']):>18} "
            f"{format_int_like(row['NOACCT']):>12} "
            f"{format_money(row['CURBAL']):>18} "
            f"{format_int_like(row['NETCHGMH']):>18} "
            f"{format_int_like(row['NETCHGYR']):>14}\n"
        )

    with report_file.open("w", encoding="utf-8", newline="") as f:
        line_count = 0

        def write(asa: str, text: str = "") -> None:
            nonlocal line_count
            f.write(f"{asa}{text}\n")
            line_count += 1

        write(ASA_NEWPAGE, "PUBLIC BANK BERHAD")
        write(ASA_SPACE, f"SAVINGS ACCOUNT OPENED/CLOSED FOR THE MONTH AS AT {macro.rdate}")
        write(ASA_SPACE)
        write(ASA_SPACE, " ".join(f"{h:>20}" for h in headers))
        write(ASA_SPACE, "-" * 210)

        for rec in branch_summary.iter_rows(named=True):
            if line_count >= PAGE_LENGTH:
                line_count = 0
                write(ASA_NEWPAGE, "PUBLIC BANK BERHAD")
                write(ASA_SPACE, f"SAVINGS ACCOUNT OPENED/CLOSED FOR THE MONTH AS AT {macro.rdate}")
                write(ASA_SPACE)
                write(ASA_SPACE, " ".join(f"{h:>20}" for h in headers))
                write(ASA_SPACE, "-" * 210)
            write(ASA_SPACE, row_line(f"{int(rec['BRANCH']):03d}", rec).rstrip("\n"))

        total_row = totals.row(0, named=True)
        write(ASA_DOUBLE, "-" * 210)
        write(ASA_SPACE, row_line("TOTAL", total_row).rstrip("\n"))


def main() -> None:
    # OPTIONS YEARCUTOFF=1950 SORTDEV=3390 NONUMBER NODATE NOCENTER;
    # Original SAS conversion starts here.

    macro = load_macro_vars()
    savg, sao = build_savg()
    savg = apply_process_macro(savg, macro)

    # DATA MIS.SAO&REPTMON; / DATA MIS.SAVG1&REPTMON;
    sao.write_parquet(MIS_DIR / f"sao{macro.reptmon}.parquet")
    savg.write_parquet(MIS_DIR / f"savg1{macro.reptmon}.parquet")

    # ------------------------------------------------
    # PRINT REPORT TO TEXT FILE
    # ------------------------------------------------
    write_report(savg, macro)


if __name__ == "__main__":
    main()
