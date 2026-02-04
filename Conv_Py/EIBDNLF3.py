#!/usr/bin/env python3
"""
File Name: EIBDNLF3
BNM Liquidity Framework - NLF Report Generation
Generates behavioural maturity profile report for conventional loans.
"""

from pathlib import Path

import duckdb
import polars as pl


# ============================================================================
# CONFIGURATION AND PATHS
# ============================================================================
# INPUT_DIR = Path("/path/to/input")
# OUTPUT_DIR = Path("/path/to/output")

BASE_DIR = Path(__file__).resolve().parent

INPUT_DIR = BASE_DIR / "data" / "input"
OUTPUT_DIR = BASE_DIR / "data" / "output"

FINALSUM_PATH = OUTPUT_DIR / "FINALSUM.parquet"
NLF_REPORT_PATH = OUTPUT_DIR / "NLF_REPORT.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# CONSTANTS AND MAPPINGS
# ============================================================================

ASA_NEW_PAGE = "1"
ASA_SPACE = " "
PAGE_LENGTH = 60

DESC_MAP = {
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

ITEM_MAP = {
    "9321109": "A1.01",
    "9521109": "A1.01",
    "9321209": "A1.02",
    "9521209": "A1.02",
    "9321309": "A1.03",
    "9521309": "A1.03",
    "9321909": "A1.04",
    "9521909": "A1.04",
    "9321408": "A1.05",
    "9521408": "A1.05",
    "9321508": "A1.06",
    "9521508": "A1.06",
    "9321308": "A1.07",
    "9521308": "A1.07",
    "9321908": "A1.08",
    "9521908": "A1.08",
    "9322100": "A1.09",
    "9522100": "A1.09",
    "9322200": "A1.10",
    "9522200": "A1.10",
    "9322900": "A1.11",
    "9522900": "A1.11",
    "9421109": "B1.01",
    "9621109": "B1.01",
    "9421209": "B1.02",
    "9621209": "B1.02",
    "9421309": "B1.03",
    "9621309": "B1.03",
    "9421909": "B1.04",
    "9621909": "B1.04",
    "9421408": "B1.05",
    "9621408": "B1.05",
    "9421508": "B1.06",
    "9621508": "B1.06",
    "9421308": "B1.07",
    "9621308": "B1.07",
    "9421908": "B1.08",
    "9621908": "B1.08",
    "9422100": "B1.09",
    "9622100": "B1.09",
    "9422200": "B1.10",
    "9622200": "B1.10",
    "9422900": "B1.11",
    "9622900": "B1.11",
}

ITEM4_MAP = {
    "9321109": "- FIXED TERM LOANS  ",
    "9521109": "- FIXED TERM LOANS  ",
    "9321209": "- REVOLVING LOANS  ",
    "9521209": "- REVOLVING LOANS  ",
    "9321309": "- OVERDRAFTS",
    "9521309": "- OVERDRAFTS",
    "9321909": "- OTHERS",
    "9521909": "- OTHERS",
    "9321408": "- HOUSING LOANS",
    "9521408": "- HOUSING LOANS",
    "9321508": "- CREDIT CARDS",
    "9521508": "- CREDIT CARDS",
    "9321308": "- OVERDRAFTS",
    "9521308": "- OVERDRAFTS",
    "9321908": "- OTHERS",
    "9521908": "- OTHERS",
    "9322100": "- CASH HOLDINGS",
    "9522100": "- CASH HOLDINGS",
    "9322200": "- SRR",
    "9522200": "- SRR",
    "9322900": "- OTHER ASSETS",
    "9522900": "- OTHER ASSETS",
    "9421109": "- FIXED TERM LOANS  ",
    "9621109": "- FIXED TERM LOANS  ",
    "9421209": "- REVOLVING LOANS  ",
    "9621209": "- REVOLVING LOANS  ",
    "9421309": "- OVERDRAFTS",
    "9621309": "- OVERDRAFTS",
    "9421909": "- OTHERS",
    "9621909": "- OTHERS",
    "9421408": "- HOUSING LOANS",
    "9621408": "- HOUSING LOANS",
    "9421508": "- CREDIT CARDS",
    "9621508": "- CREDIT CARDS",
    "9421308": "- OVERDRAFTS",
    "9621308": "- OVERDRAFTS",
    "9421908": "- OTHERS",
    "9621908": "- OTHERS",
    "9422100": "- CASH HOLDINGS",
    "9622100": "- CASH HOLDINGS",
    "9422200": "- SRR",
    "9622200": "- SRR",
    "9422900": "- OTHER ASSETS",
    "9622900": "- OTHER ASSETS",
}

HEADER_COLUMNS = [
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


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def format_number(value):
    if value is None:
        return "0"
    if isinstance(value, float):
        if value != value:
            return "0"
        if abs(value - round(value)) < 1e-9:
            return str(int(round(value)))
    return str(value)


def write_report_section(file_handle, title, data_df):
    line_count = 0

    def write_line(asa_char, content):
        nonlocal line_count
        file_handle.write(f"{asa_char}{content}\n")
        line_count += 1

    def write_header():
        nonlocal line_count
        header_line = ";".join(HEADER_COLUMNS) + ";"
        write_line(ASA_NEW_PAGE, " ")
        write_line(ASA_SPACE, title)
        write_line(ASA_SPACE, " ")
        write_line(ASA_SPACE, "INFLOW(ASSETS)")
        write_line(ASA_SPACE, "ON BALANCE SHEET")
        write_line(ASA_SPACE, header_line)
        line_count = 6

    write_header()

    for row in data_df.iter_rows(named=True):
        if line_count >= PAGE_LENGTH:
            write_header()

        values = [
            row.get("ITEM", "") or "",
            row.get("ITEM2", "") or "",
            row.get("ITEM3", "") or "",
            row.get("ITEM4", "") or "",
            format_number(row.get("WEEK")),
            format_number(row.get("MONTH")),
            format_number(row.get("QTR")),
            format_number(row.get("HALFYR")),
            format_number(row.get("YEAR")),
            format_number(row.get("LAST")),
            format_number(row.get("BALANCE")),
        ]
        line = ";".join(values) + ";"
        write_line(ASA_SPACE, line)


def build_report(df, prefix):
    return (
        df.filter(
            (pl.col("PROD").str.starts_with(prefix))
            & (pl.col("ITEM").is_not_null())
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


# ============================================================================
# MAIN PROCESSING
# ============================================================================

print("Step 1: Loading FINALSUM data...")
con = duckdb.connect()
finalsum = con.execute(
    "SELECT * FROM read_parquet(?)",
    [str(FINALSUM_PATH)],
).pl()
con.close()

if len(finalsum) == 0:
    raise RuntimeError("FINALSUM is empty; cannot generate report.")

loan = finalsum.with_columns(
    pl.col("BNMCODE").cast(pl.Utf8),
    pl.col("BNMCODE").str.slice(0, 7).alias("PROD"),
    pl.col("BNMCODE").str.slice(5, 2).alias("INDNON"),
    (pl.col("AMOUNT") / 1000).round(0).alias("AMOUNT"),
).with_columns(
    pl.col("PROD").map_dict(DESC_MAP, default=None).alias("DESC"),
)

loan = loan.with_columns(
    pl.cum_count().over(["PROD", "DESC"]).alias("SEQ")
)

loan_transposed = loan.pivot(
    index=["PROD", "DESC"],
    columns="SEQ",
    values="AMOUNT",
    aggregate_function="first",
)

rename_map = {
    1: "WEEK",
    2: "MONTH",
    3: "QTR",
    4: "HALFYR",
    5: "YEAR",
    6: "LAST",
    "1": "WEEK",
    "2": "MONTH",
    "3": "QTR",
    "4": "HALFYR",
    "5": "YEAR",
    "6": "LAST",
}

loan_transposed = loan_transposed.rename(
    {col: rename_map.get(col, col) for col in loan_transposed.columns}
)

store = loan_transposed.with_columns(
    pl.col("PROD").str.slice(5, 2).alias("INDNON"),
    pl.col("PROD").map_dict(ITEM_MAP, default=None).alias("ITEM"),
    pl.col("PROD").map_dict(ITEM4_MAP, default=None).alias("ITEM4"),
    pl.sum_horizontal(
        [
            pl.col("WEEK").fill_null(0),
            pl.col("MONTH").fill_null(0),
            pl.col("QTR").fill_null(0),
            pl.col("HALFYR").fill_null(0),
            pl.col("YEAR").fill_null(0),
            pl.col("LAST").fill_null(0),
        ]
    ).alias("BALANCE"),
)

report_part1 = build_report(store, "93")
report_part2 = build_report(store, "95")
report_fcy_part1 = build_report(store, "94")
report_fcy_part2 = build_report(store, "96")

print("Step 2: Writing NLF report...")
with open(NLF_REPORT_PATH, "w") as report_file:
    write_report_section(
        report_file,
        "BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE - RINGGIT PART 1-RM",
        report_part1,
    )
    write_report_section(
        report_file,
        "BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE - RINGGIT PART 2-RM",
        report_part2,
    )
    write_report_section(
        report_file,
        "BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE - FCY PART 1-FX",
        report_fcy_part1,
    )
    write_report_section(
        report_file,
        "BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE - FCY PART 2-FX",
        report_fcy_part2,
    )

print(f"Report written to: {NLF_REPORT_PATH}")
