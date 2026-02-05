#!/usr/bin/env python3
"""
File Name: X_EIBMNLF3

Assumptions from requirements:
- Input datasets are parquet files with SAS-compatible column names.
- Report output is text and includes ASA carriage-control characters.
- Default report page length is 60 lines.
- DuckDB is used to read parquet and Polars handles transformations.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

# =============================================================================
# PATH CONFIGURATION (DEFINED EARLY)
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "data" / "input"
OUTPUT_DIR = BASE_DIR / "data" / "output"

FINALSUM_PATH = INPUT_DIR / "STORE_FINALSUM.parquet"
NLF_REPORT_PATH = OUTPUT_DIR / "SAP_PBB_NLF_TEXT.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# CONSTANTS
# =============================================================================
ASA_NEW_PAGE = "1"
ASA_SINGLE_SPACE = " "
DEFAULT_PAGE_LENGTH = 60

HEADER_FIELDS = [
    "    ",
    "    ",
    "    ",
    "    ",
    "UP TO 1 WK ",
    ">1 WK - 1 MTH",
    ">1 MTH - 3 MTHS",
    ">3 MTHS - 6 MTHS",
    ">6 MTHS - 1 YR",
    ">1 YEAR",
    "TOTAL",
]

PRODUCT_TO_DESC = {
    "9321109": "NONRMFL",
    "9521109": "NONRMFL",
    "9321209": "NONRMRC",
    "9521209": "NONRMRC",
    "9321309": "NONRMOD",
    "9521309": "NONRMOD",
    "9321909": "NONRMOT",
    "9521909": "NONRMOT",
    "9321408": "INDRMHL",
    "9521408": "INDRMHL",
    "9321508": "INDRMCC",
    "9521508": "INDRMCC",
    "9321308": "INDRMOD",
    "9521308": "INDRMOD",
    "9321908": "INDRMOT",
    "9521908": "INDRMOT",
    "9322100": "MISRMCH",
    "9522100": "MISRMCH",
    "9322200": "MISRMSR",
    "9522200": "MISRMSR",
    "9322900": "MISRMOT",
    "9522900": "MISRMOT",
    "9421109": "NONFXFL",
    "9621109": "NONFXFL",
    "9421209": "NONFXRC",
    "9621209": "NONFXRC",
    "9421309": "NONFXOD",
    "9621309": "NONFXOD",
    "9421909": "NONFXOT",
    "9621909": "NONFXOT",
    "9421408": "INDFXHL",
    "9621408": "INDFXHL",
    "9421508": "INDFXCC",
    "9621508": "INDFXCC",
    "9421308": "INDFXOD",
    "9621308": "INDFXOD",
    "9421908": "INDFXOT",
    "9621908": "INDFXOT",
    "9422100": "MISFXCH",
    "9622100": "MISFXCH",
    "9422200": "MISFXSR",
    "9622200": "MISFXSR",
    "9422900": "MISFXOT",
    "9622900": "MISFXOT",
}

PRODUCT_TO_ITEM_AND_TEXT = {
    "9321109": ("A1.01", "- FIXED TERM LOANS  "),
    "9521109": ("A1.01", "- FIXED TERM LOANS  "),
    "9321209": ("A1.02", "- REVOLVING LOANS  "),
    "9521209": ("A1.02", "- REVOLVING LOANS  "),
    "9321309": ("A1.03", "- OVERDRAFTS"),
    "9521309": ("A1.03", "- OVERDRAFTS"),
    "9321909": ("A1.04", "- OTHERS"),
    "9521909": ("A1.04", "- OTHERS"),
    "9321408": ("A1.05", "- HOUSING LOANS"),
    "9521408": ("A1.05", "- HOUSING LOANS"),
    "9321508": ("A1.06", "- CREDIT CARDS"),
    "9521508": ("A1.06", "- CREDIT CARDS"),
    "9321308": ("A1.07", "- OVERDRAFTS"),
    "9521308": ("A1.07", "- OVERDRAFTS"),
    "9321908": ("A1.08", "- OTHERS"),
    "9521908": ("A1.08", "- OTHERS"),
    "9322100": ("A1.09", "- CASH HOLDINGS"),
    "9522100": ("A1.09", "- CASH HOLDINGS"),
    "9322200": ("A1.10", "- SRR"),
    "9522200": ("A1.10", "- SRR"),
    "9322900": ("A1.11", "- OTHER ASSETS"),
    "9522900": ("A1.11", "- OTHER ASSETS"),
    "9421109": ("B1.01", "- FIXED TERM LOANS  "),
    "9621109": ("B1.01", "- FIXED TERM LOANS  "),
    "9421209": ("B1.02", "- REVOLVING LOANS  "),
    "9621209": ("B1.02", "- REVOLVING LOANS  "),
    "9421309": ("B1.03", "- OVERDRAFTS"),
    "9621309": ("B1.03", "- OVERDRAFTS"),
    "9421909": ("B1.04", "- OTHERS"),
    "9621909": ("B1.04", "- OTHERS"),
    "9421408": ("B1.05", "- HOUSING LOANS"),
    "9621408": ("B1.05", "- HOUSING LOANS"),
    "9421508": ("B1.06", "- CREDIT CARDS"),
    "9621508": ("B1.06", "- CREDIT CARDS"),
    "9421308": ("B1.07", "- OVERDRAFTS"),
    "9621308": ("B1.07", "- OVERDRAFTS"),
    "9421908": ("B1.08", "- OTHERS"),
    "9621908": ("B1.08", "- OTHERS"),
    "9422100": ("B1.09", "- CASH HOLDINGS"),
    "9622100": ("B1.09", "- CASH HOLDINGS"),
    "9422200": ("B1.10", "- SRR"),
    "9622200": ("B1.10", "- SRR"),
    "9422900": ("B1.11", "- OTHER ASSETS"),
    "9622900": ("B1.11", "- OTHER ASSETS"),
}


def format_sas_number(value: float | int | None) -> str:
    """Mimic SAS default numeric rendering used by PUT in this report."""
    if value is None:
        return "0"
    numeric = float(value)
    if abs(numeric - round(numeric)) < 1e-9:
        return str(int(round(numeric)))
    return f"{numeric:.12g}"


def build_store_table(finalsum_df: pl.DataFrame) -> pl.DataFrame:
    loan = (
        finalsum_df.with_columns(
            pl.col("BNMCODE").cast(pl.Utf8),
            pl.col("BNMCODE").str.slice(0, 7).alias("PROD"),
            pl.col("BNMCODE").str.slice(5, 2).alias("INDNON"),
            (pl.col("AMOUNT").cast(pl.Float64) / 1000.0).round(0).alias("AMOUNT"),
        )
        .with_columns(pl.col("PROD").replace(PRODUCT_TO_DESC, default=None).alias("DESC"))
    )

    transposed = (
        loan.with_columns(pl.len().over(["PROD", "DESC"]).cum_count().alias("_SEQ"))
        .pivot(index=["PROD", "DESC"], on="_SEQ", values="AMOUNT", aggregate_function="first")
        .rename(
            {
                "1": "WEEK",
                "2": "MONTH",
                "3": "QTR",
                "4": "HALFYR",
                "5": "YEAR",
                "6": "LAST",
                1: "WEEK",
                2: "MONTH",
                3: "QTR",
                4: "HALFYR",
                5: "YEAR",
                6: "LAST",
            }
        )
        .with_columns(
            pl.col("PROD").str.slice(5, 2).alias("INDNON"),
            pl.col("PROD").replace({k: v[0] for k, v in PRODUCT_TO_ITEM_AND_TEXT.items()}, default=None).alias("ITEM"),
            pl.col("PROD").replace({k: v[1] for k, v in PRODUCT_TO_ITEM_AND_TEXT.items()}, default=None).alias("ITEM4"),
        )
        .with_columns(
            pl.sum_horizontal(
                pl.col("WEEK").fill_null(0),
                pl.col("MONTH").fill_null(0),
                pl.col("QTR").fill_null(0),
                pl.col("HALFYR").fill_null(0),
                pl.col("YEAR").fill_null(0),
                pl.col("LAST").fill_null(0),
            ).alias("BALANCE")
        )
    )

    return transposed


def build_report_slice(store_df: pl.DataFrame, product_prefix: str) -> pl.DataFrame:
    return (
        store_df.filter(
            pl.col("PROD").str.starts_with(product_prefix)
            & pl.col("ITEM").is_not_null()
            & (pl.col("ITEM") != " ")
        )
        .sort("INDNON", descending=True)
        .with_columns(
            pl.lit("LOAN :").alias("ITEM2"),
            pl.when(pl.col("INDNON") == "08")
            .then(pl.lit("INDIVIDUALS    "))
            .when(pl.col("INDNON") == "09")
            .then(pl.lit("NON-INDIVIDUALS"))
            .otherwise(pl.lit(""))
            .alias("ITEM3"),
        )
    )


def write_section(handle, title: str, section_df: pl.DataFrame, page_length: int = DEFAULT_PAGE_LENGTH) -> None:
    lines_on_page = 0

    def write_line(asa_char: str, text: str) -> None:
        nonlocal lines_on_page
        handle.write(f"{asa_char}{text}\n")
        lines_on_page += 1

    def write_header() -> None:
        nonlocal lines_on_page
        write_line(ASA_NEW_PAGE, " ")
        write_line(ASA_SINGLE_SPACE, title)
        write_line(ASA_SINGLE_SPACE, " ")
        write_line(ASA_SINGLE_SPACE, "INFLOW(ASSETS)")
        write_line(ASA_SINGLE_SPACE, "ON BALANCE SHEET")
        write_line(ASA_SINGLE_SPACE, ";".join(HEADER_FIELDS) + ";")

    write_header()

    for row in section_df.iter_rows(named=True):
        if lines_on_page >= page_length:
            lines_on_page = 0
            write_header()

        values = [
            row.get("ITEM") or "",
            row.get("ITEM2") or "",
            row.get("ITEM3") or "",
            row.get("ITEM4") or "",
            format_sas_number(row.get("WEEK")),
            format_sas_number(row.get("MONTH")),
            format_sas_number(row.get("QTR")),
            format_sas_number(row.get("HALFYR")),
            format_sas_number(row.get("YEAR")),
            format_sas_number(row.get("LAST")),
            format_sas_number(row.get("BALANCE")),
        ]
        write_line(ASA_SINGLE_SPACE, ";".join(values) + ";")


def main() -> None:
    connection = duckdb.connect()
    finalsum_df = connection.execute(
        "SELECT BNMCODE, AMOUNT FROM read_parquet(?)",
        [str(FINALSUM_PATH)],
    ).pl()
    connection.close()

    if finalsum_df.is_empty():
        raise ValueError(f"Input parquet is empty: {FINALSUM_PATH}")

    store_df = build_store_table(finalsum_df)

    report_sections = [
        (
            "BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE - RINGGIT PART 1-RM",
            build_report_slice(store_df, "93"),
        ),
        (
            "BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE - RINGGIT PART 2-RM",
            build_report_slice(store_df, "95"),
        ),
        (
            "BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE - FCY PART 1-FX",
            build_report_slice(store_df, "94"),
        ),
        (
            "BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE - FCY PART 2-FX",
            build_report_slice(store_df, "96"),
        ),
    ]

    with NLF_REPORT_PATH.open("w", encoding="utf-8") as output_handle:
        for title, section_df in report_sections:
            write_section(output_handle, title, section_df)

    print(f"Input : {FINALSUM_PATH}")
    print(f"Output: {NLF_REPORT_PATH}")
    for title, section_df in report_sections:
        print(f"{title}: {section_df.height} rows")


if __name__ == "__main__":
    main()
