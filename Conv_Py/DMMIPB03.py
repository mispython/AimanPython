#!/usr/bin/env python3
"""
Program : DMMIPB03
Purpose : Profile on PB Bright Star Savings Accounts
"""

import os
from datetime import datetime
from typing import Optional

import duckdb
import polars as pl

from PBBDPFMT import SAProductFormat
from PBMISFMT import format_sadprg

# OPTIONS NOCENTER NODATE NONUMBER MISSING=0;
# %INC PGM(PBBDPFMT,PBMISFMT);

# =============================================================================
# PATH SETUP
# =============================================================================
BASE_DIR = os.environ.get("BASE_DIR", "/data")
DEPOSIT_DIR = os.path.join(BASE_DIR, "deposit")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

REPTDATE_FILE = os.path.join(DEPOSIT_DIR, "REPTDATE.parquet")
SAVING_FILE = os.path.join(DEPOSIT_DIR, "SAVING.parquet")
REPORT_FILE = os.path.join(OUTPUT_DIR, "DMMIPB03.txt")

ASA_NEWPAGE = "1"
ASA_NEWLINE = " "
PAGE_LINES = 60

REPORT_COL_RANGE = 40
REPORT_COL_COUNT = 12
REPORT_COL_AMOUNT = 20

os.makedirs(OUTPUT_DIR, exist_ok=True)


# Placeholder dependency reference from PBBDPFMT (SAS format catalog):
# VALUE SACUSTCD ... OTHER='78';
# VALUE STATECD  ...
def format_sacustcd(custcode: Optional[int]) -> str:
    """Fallback SACUSTCD mapping placeholder."""
    if custcode is None:
        return "78"
    return f"{int(custcode):02d}"[-2:]


def format_statecd(branch: Optional[int]) -> str:
    """Fallback STATECD mapping placeholder."""
    if branch is None:
        return " "
    # Full mapping is defined in Ori_SAS/PBBDPFMT VALUE STATECD.
    return " "


def parse_report_date(value: object) -> datetime:
    """Parse DEPOSIT.REPTDATE into datetime."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime.strptime(str(int(value))[:8], "%Y%m%d")
    return datetime.strptime(str(value)[:10], "%Y-%m-%d")


def derive_report_context() -> dict[str, object]:
    """Read REPTDATE and derive SAS macro-equivalent values."""
    con = duckdb.connect()
    row = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{REPTDATE_FILE}') LIMIT 1"
    ).fetchone()
    con.close()

    if row is None:
        raise ValueError("DEPOSIT.REPTDATE is empty.")

    reptdate = parse_report_date(row[0])
    return {
        "reptmon": reptdate.strftime("%m"),
        "reptyear": int(reptdate.strftime("%Y")),
        "rdate": reptdate.strftime("%d/%m/%Y"),
        "zdate": int(reptdate.strftime("%j")),
    }


def build_pb03() -> pl.DataFrame:
    """
    Build PB03 equivalent dataset from DEPOSIT.SAVING.

    SAS logic:
    - PRODUCT = 208
    - OPENIND not in ('B','C','P','Z')
    - CURBAL ne 0
    """
    con = duckdb.connect()
    saving_df = con.execute(
        f"SELECT * FROM read_parquet('{SAVING_FILE}')"
    ).pl()
    con.close()

    filtered_df = saving_df.filter(
        (pl.col("PRODUCT") == 208)
        & (~pl.col("OPENIND").is_in(["B", "C", "P", "Z"]))
        & (pl.col("CURBAL") != 0)
    )

    # IF OPENDT NE 0 THEN OPENDATE=INPUT(SUBSTR(PUT(OPENDT,Z11.),1,8),MMDDYY8.);
    # IF (BDATE NE 0) AND (BDATE NE .) THEN ... AGE=(&REPTYEAR-BYEAR);
    transformed_df = filtered_df.with_columns(
        [
            pl.col("CUSTCODE")
            .map_elements(format_sacustcd, return_dtype=pl.Utf8)
            .alias("CUSTCD"),
            pl.col("BRANCH")
            .map_elements(format_statecd, return_dtype=pl.Utf8)
            .alias("STATECD"),
            pl.col("PRODUCT")
            .map_elements(SAProductFormat.format, return_dtype=pl.Utf8)
            .alias("PRODCD"),
            pl.lit("D").alias("AMTIND"),
            pl.col("CURBAL")
            .map_elements(format_sadprg, return_dtype=pl.Utf8)
            .alias("DEPRANGE"),
            pl.lit(1).alias("NOACCT"),
            pl.col("BDATE").alias("AGE"),
        ]
    )

    return transformed_df.select(["PRODUCT", "DEPRANGE", "NOACCT", "CURBAL"])


def fmt_comma12(value: float) -> str:
    """SAS-like COMMA12. formatting."""
    return f"{value:,.0f}".rjust(REPORT_COL_COUNT)


def fmt_dollar20_2(value: float) -> str:
    """SAS-like DOLLAR20.2 formatting."""
    return f"${value:,.2f}".rjust(REPORT_COL_AMOUNT)


def render_data_line(label: str, noacct: float, amount: float) -> str:
    """Render one report data row."""
    return (
        f"{ASA_NEWLINE}{label[:REPORT_COL_RANGE]:<{REPORT_COL_RANGE}}"
        f"{fmt_comma12(float(noacct))}"
        f"{fmt_dollar20_2(float(amount))}"
    )


def render_report(pb03_df: pl.DataFrame, context: dict[str, object]) -> str:
    """Render tabulated report output with ASA carriage-control."""
    by_range = pb03_df.group_by(["PRODUCT", "DEPRANGE"]).agg(
        [
            pl.col("NOACCT").sum().alias("NOACCT_SUM"),
            pl.col("CURBAL").sum().alias("CURBAL_SUM"),
        ]
    )

    product_totals = pb03_df.group_by("PRODUCT").agg(
        [
            pl.col("NOACCT").sum().alias("NOACCT_SUM"),
            pl.col("CURBAL").sum().alias("CURBAL_SUM"),
        ]
    )

    grand_total = pb03_df.select(
        [
            pl.col("NOACCT").sum().alias("NOACCT_SUM"),
            pl.col("CURBAL").sum().alias("CURBAL_SUM"),
        ]
    ).row(0)

    lines: list[str] = [
        f"{ASA_NEWPAGE}PUBLIC BANK BERHAD   REPORT=DMMIPB03",
        f"{ASA_NEWLINE}FOR PRODUCT DEVELOPMENT & MARKETING",
        (
            f"{ASA_NEWLINE}PROFILE ON PB BRIGHT STAR SAVINGS "
            f"ACCOUNTS AS AT {context['rdate']}"
        ),
        f"{ASA_NEWLINE}",
        f"{ASA_NEWLINE}PB BRIGHT STAR",
        (
            f"{ASA_NEWLINE}{'DEPOSIT RANGE':<{REPORT_COL_RANGE}}"
            f"{'NO OF A/C':>{REPORT_COL_COUNT}}"
            f"{'AMOUNT':>{REPORT_COL_AMOUNT}}"
        ),
    ]

    line_count = len(lines)
    for row in by_range.sort(["PRODUCT", "DEPRANGE"]).iter_rows(named=True):
        if line_count >= PAGE_LINES:
            lines.append(f"{ASA_NEWPAGE}")
            line_count = 1

        lines.append(
            render_data_line(
                str(row["DEPRANGE"]),
                float(row["NOACCT_SUM"]),
                float(row["CURBAL_SUM"]),
            )
        )
        line_count += 1

    lines.append(
        render_data_line(
            "TOTAL",
            float(grand_total[0] or 0),
            float(grand_total[1] or 0),
        )
    )

    for row in product_totals.sort("PRODUCT").iter_rows(named=True):
        label = f"BANK TOTAL (PRODUCT {int(row['PRODUCT'])})"
        lines.append(
            render_data_line(
                label,
                float(row["NOACCT_SUM"]),
                float(row["CURBAL_SUM"]),
            )
        )

    lines.append(
        render_data_line(
            "BANK TOTAL",
            float(grand_total[0] or 0),
            float(grand_total[1] or 0),
        )
    )

    return "\n".join(lines) + "\n"


def main() -> None:
    context = derive_report_context()
    pb03_df = build_pb03()
    report_text = render_report(pb03_df, context)

    with open(REPORT_FILE, "w", encoding="utf-8", newline="\n") as file_obj:
        file_obj.write(report_text)


if __name__ == "__main__":
    main()
