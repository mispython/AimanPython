#!/usr/bin/env python3
"""
Program: EIBMCLAS
Purpose: Monthly Report on More Plan Loans - Purpose Others
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb
import polars as pl
from PBBLNFMT import MOREISLM_PRODUCTS, MOREPLAN_PRODUCTS

# =============================================================================
# PATH SETUP
# =============================================================================
BASE_DIR = Path(os.getenv("EIBMCLAS_BASE_DIR", "."))
INPUT_DIR = BASE_DIR / "input" / "EIBMCLAS"
OUTPUT_DIR = BASE_DIR / "output" / "EIBMCLAS"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPTDATE_PARQUET = Path(os.getenv("EIBMCLAS_REPTDATE", INPUT_DIR / "loan_reptdate.parquet"))
LNNOTE_PARQUET = Path(os.getenv("EIBMCLAS_LNNOTE", INPUT_DIR / "loan_lnnote.parquet"))
BNM_LOAN_TEMPLATE = os.getenv(
    "EIBMCLAS_BNM_LOAN_TEMPLATE",
    str(INPUT_DIR / "bnm_loan{reptmon}{nowk}.parquet"),
)
REPORT_OUTPUT = Path(os.getenv("EIBMCLAS_REPORT", OUTPUT_DIR / "EIBMCLAS.txt"))

# SAS include equivalent:
# %INC PGM(PBBLNFMT);
# Resolved using converted constants in PBBLNFMT.py.
def _parse_product_list(raw_value: str, default_values: tuple[int, ...]) -> tuple[int, ...]:
    if not raw_value.strip():
        return default_values
    return tuple(int(item.strip()) for item in raw_value.split(",") if item.strip())


# Runtime list configuration (SAS %LET defaults from PBBLNFMT, env-overridable)
# %LET MOREPLAN=(116,119,234,235,236,242);
# %LET MOREISLM=(116,119);
MOREPLAN = _parse_product_list(os.getenv("EIBMCLAS_MOREPLAN", ""), MOREPLAN_PRODUCTS)
MOREISLM = _parse_product_list(os.getenv("EIBMCLAS_MOREISLM", ""), MOREISLM_PRODUCTS)
SECTORS = ("0311", "0312", "0313", "0314", "0315", "0316")

PAGE_LENGTH = 60
LINE_SIZE = 132


@dataclass(frozen=True)
class ReportContext:
    reptdate: date
    reptmon: str
    nowk: str
    reptdt: str


class AsaReportWriter:
    def __init__(self, output_path: Path, page_length: int = 60) -> None:
        self.output_path = output_path
        self.page_length = page_length
        self.current_line = 0
        self.file = output_path.open("w", encoding="utf-8", newline="")

    def close(self) -> None:
        self.file.close()

    def _write_asa_line(self, control: str, text: str = "") -> None:
        self.file.write(f"{control}{text}\n")

    def start_page(self) -> None:
        self._write_asa_line("1", "")
        self.current_line = 1

    def write_line(self, text: str = "") -> None:
        if self.current_line == 0 or self.current_line >= self.page_length:
            self.start_page()
        self._write_asa_line(" ", text[:LINE_SIZE])
        self.current_line += 1


def parse_sas_date(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value[:10]).date()
    raise ValueError(f"Unsupported REPTDATE value: {value!r}")


def get_report_context(con: duckdb.DuckDBPyConnection) -> ReportContext:
    reptdate_df = con.execute(
        "SELECT REPTDATE FROM read_parquet(?) ORDER BY REPTDATE LIMIT 1",
        [str(REPTDATE_PARQUET)],
    ).pl()
    if reptdate_df.height == 0:
        raise ValueError("REPTDATE input is empty.")

    reptdate = parse_sas_date(reptdate_df.item(0, "REPTDATE"))
    day = reptdate.day
    if 1 <= day <= 8:
        nowk = "1"
    elif 9 <= day <= 15:
        nowk = "2"
    elif 16 <= day <= 22:
        nowk = "3"
    else:
        nowk = "4"

    reptmon = f"{reptdate.month:02d}"
    reptdt = reptdate.strftime("%d/%m/%y")
    return ReportContext(reptdate=reptdate, reptmon=reptmon, nowk=nowk, reptdt=reptdt)


def build_base_loan(con: duckdb.DuckDBPyConnection, ctx: ReportContext) -> pl.DataFrame:
    weekly_loan_path = BNM_LOAN_TEMPLATE.format(reptmon=ctx.reptmon, nowk=ctx.nowk)

    con.execute("CREATE OR REPLACE TEMP TABLE lnnote_raw AS SELECT * FROM read_parquet(?)", [str(LNNOTE_PARQUET)])
    con.execute("CREATE OR REPLACE TEMP TABLE weekly_loan_raw AS SELECT * FROM read_parquet(?)", [weekly_loan_path])

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE lnnote AS
        SELECT *
        FROM lnnote_raw
        WHERE LOANTYPE IN ({moreplan})
          AND SECTOR IN ({sectors})
        """.format(
            moreplan=",".join(["?"] * len(MOREPLAN)),
            sectors=",".join(["?"] * len(SECTORS)),
        ),
        [*MOREPLAN, *SECTORS],
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE loan_src AS
        SELECT *
        FROM weekly_loan_raw
        WHERE PRODUCT IN ({moreplan})
        """.format(moreplan=",".join(["?"] * len(MOREPLAN))),
        [*MOREPLAN],
    )

    loan_df = con.execute(
        """
        SELECT
            lnnote.ACCTNO,
            lnnote.NOTENO,
            loan_src.BRANCH,
            loan_src.PRODUCT,
            loan_src.BALANCE,
            loan_src.APPORMT,
            loan_src.BALANCE * loan_src.APPORMT AS RECLASS
        FROM lnnote
        INNER JOIN loan_src
          ON lnnote.ACCTNO = loan_src.ACCTNO
         AND lnnote.NOTENO = loan_src.NOTENO
        """
    ).pl()

    return loan_df


def summarize_by_branch(df: pl.DataFrame) -> pl.DataFrame:
    if df.height == 0:
        return pl.DataFrame({"BRANCH": [], "NOACCT": [], "BALANCE": [], "RECLASS": []})

    return (
        df.group_by("BRANCH")
        .agg(
            [
                pl.len().alias("NOACCT"),
                pl.col("BALANCE").sum().alias("BALANCE"),
                pl.col("RECLASS").sum().alias("RECLASS"),
            ]
        )
        .sort("BRANCH")
    )


def fmt_money(value: float) -> str:
    return f"{value:15,.2f}"


def write_report_section(
    writer: AsaReportWriter,
    summary_df: pl.DataFrame,
    lntype: str,
    reptdt: str,
) -> None:
    writer.write_line("REPORT ID : EIBMCLAS")
    writer.write_line(f"MONTHLY REPORT ON {lntype} (PURPOSE OTHER) - {reptdt}")
    writer.write_line("")
    writer.write_line(f"{'BRANCH':<6} {'BALANCE':>15} {'AMOUNT TO RECLASS TO 100% RISK':>31}")
    writer.write_line("-" * 60)

    total_accounts = 0
    total_balance = 0.0
    total_reclass = 0.0

    for row in summary_df.iter_rows(named=True):
        branch = str(row["BRANCH"])[:6]
        balance = float(row["BALANCE"] or 0)
        reclass = float(row["RECLASS"] or 0)
        noacct = int(row["NOACCT"] or 0)

        total_accounts += noacct
        total_balance += balance
        total_reclass += reclass

        writer.write_line(f"{branch:<6} {fmt_money(balance)} {fmt_money(reclass):>31}")

    writer.write_line("=" * 60)
    writer.write_line(f"TOTAL  {fmt_money(total_balance)} {fmt_money(total_reclass):>31}")
    writer.write_line(f"NOACCT: {total_accounts}")
    writer.write_line("")


def run() -> None:
    con = duckdb.connect(database=":memory:")
    try:
        ctx = get_report_context(con)
        base_loan_df = build_base_loan(con, ctx)

        report1 = summarize_by_branch(base_loan_df)

        if MOREISLM:
            report2_source = base_loan_df.filter(pl.col("PRODUCT").is_in(MOREISLM))
        else:
            report2_source = pl.DataFrame(schema=base_loan_df.schema)
        report2 = summarize_by_branch(report2_source)

        writer = AsaReportWriter(REPORT_OUTPUT, page_length=PAGE_LENGTH)
        try:
            write_report_section(writer, report1, "MORE PLAN LOANS", ctx.reptdt)
            write_report_section(writer, report2, "ISLAMIC MORE PLAN LOANS", ctx.reptdt)
        finally:
            writer.close()
    finally:
        con.close()


if __name__ == "__main__":
    run()
