#!/usr/bin/env python3
"""
Program: EIFMNP11
Purpose: TOP 20 NPL accounts with IIS or SP closing > 0.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb
import polars as pl

# ---------------------------------------------------------------------------
# Path configuration (define early)
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_REPTDATE_PARQUET = BASE_DIR / "NPL.REPTDATE.parquet"
INPUT_IIS_PARQUET = BASE_DIR / "NPL.IIS.parquet"
INPUT_SP_PARQUET = BASE_DIR / "NPL.SP.parquet"

OUTPUT_TOP20_TXT = BASE_DIR / "EIFMNP11_TOP20_REPORT.txt"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PAGE_LENGTH = 60
DETAIL_LINES_PER_PAGE = 49  # reserve space for header + footer blocks

IIS_KEEP_COLUMNS = [
    "BRANCH",
    "RISK",
    "ACCTNO",
    "NAME",
    "DAYS",
    "BORSTAT",
    "CURBAL",
    "UHC",
    "NETBAL",
    "OI",
    "IIS",
]


def _to_python_date(value: object) -> date:
    """Convert inbound REPTDATE cell value to python date."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value).date()
    raise TypeError(f"Unsupported REPTDATE type: {type(value)!r}")


def _worddatx18(value: date) -> str:
    """SAS WORDDATX18-like date format, e.g. June 30, 1999."""
    return f"{value.strftime('%B')} {value.day}, {value.year}"


def _fmt_money(value: float | int | None) -> str:
    """SAS COMMA14.2-style numeric rendering for report values."""
    if value is None:
        return "0.00"
    return f"{float(value):,.2f}"


def build_top20_dataframe(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Reproduce SAS data steps/sorts to build TOP20 dataset."""
    top20 = con.execute(
        f"""
        WITH iis AS (
            SELECT {', '.join(IIS_KEEP_COLUMNS)}
            FROM read_parquet('{INPUT_IIS_PARQUET.as_posix()}')
        ),
        sp AS (
            SELECT ACCTNO, SP
            FROM read_parquet('{INPUT_SP_PARQUET.as_posix()}')
        ),
        merged AS (
            SELECT
                iis.BRANCH,
                iis.RISK,
                iis.ACCTNO,
                iis.NAME,
                iis.DAYS,
                iis.BORSTAT,
                iis.CURBAL,
                iis.UHC,
                iis.OI,
                (COALESCE(iis.NETBAL, 0) + COALESCE(iis.OI, 0)) AS NETBAL,
                iis.IIS,
                sp.SP
            FROM iis
            LEFT JOIN sp ON iis.ACCTNO = sp.ACCTNO
        )
        SELECT BRANCH, RISK, ACCTNO, NAME, DAYS, BORSTAT, CURBAL, UHC, OI, NETBAL
        FROM merged
        WHERE COALESCE(IIS, 0) > 0 OR COALESCE(SP, 0) > 0
        ORDER BY NETBAL DESC
        LIMIT 20
        """
    ).pl()

    return top20.select([
        "BRANCH",
        "RISK",
        "ACCTNO",
        "NAME",
        "DAYS",
        "BORSTAT",
        "CURBAL",
        "UHC",
        "OI",
        "NETBAL",
    ])


def write_report(top20: pl.DataFrame, report_date_text: str, output_path: Path) -> None:
    """Write REPORT output as text with ASA carriage control characters."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    columns = [
        ("BRANCH", 8, "<"),
        ("RISK", 6, "<"),
        ("MNI ACCOUNT NO", 16, "<"),
        ("NAME", 24, "<"),
        ("NO OF DAYS PAST DUE", 19, ">"),
        ("BORROWER'S STATUS", 18, "<"),
        ("CURRENT BALANCE", 16, ">"),
        ("UNEARNED HIRING CHARGES", 24, ">"),
        ("OI", 14, ">"),
        ("NET BAL + OI", 16, ">"),
    ]

    def line_from_values(values: list[str]) -> str:
        out = []
        for (title, width, align), value in zip(columns, values, strict=True):
            if align == ">":
                out.append(f"{value:>{width}}")
            else:
                out.append(f"{value:<{width}}")
        return " ".join(out)

    header_titles = line_from_values([c[0] for c in columns])
    separator = "-" * len(header_titles)

    totals = {
        "CURBAL": float(top20["CURBAL"].fill_null(0).sum() or 0),
        "UHC": float(top20["UHC"].fill_null(0).sum() or 0),
        "OI": float(top20["OI"].fill_null(0).sum() or 0),
        "NETBAL": float(top20["NETBAL"].fill_null(0).sum() or 0),
    }

    with output_path.open("w", encoding="utf-8", newline="") as f:
        page = 1
        printed_on_page = 0

        def write_page_header() -> None:
            nonlocal printed_on_page
            carriage = "1" if page == 1 else "0"
            f.write(f"{carriage}PUBLIC FINANCE BERHAD\n")
            f.write(f" TOP 20 NON PERFORMING ACCOUNTS BASED ON NETBAL + OI {report_date_text}\n")
            f.write(" \n")
            f.write(f" {header_titles}\n")
            f.write(f" {separator}\n")
            printed_on_page = 5

        write_page_header()

        for row in top20.iter_rows(named=True):
            if printed_on_page >= DETAIL_LINES_PER_PAGE:
                page += 1
                write_page_header()

            values = [
                str(row["BRANCH"] or "")[:8],
                str(row["RISK"] or "")[:6],
                str(row["ACCTNO"] or "")[:16],
                str(row["NAME"] or "")[:24],
                str(int(row["DAYS"] or 0)),
                str(row["BORSTAT"] or "")[:18],
                _fmt_money(row["CURBAL"]),
                _fmt_money(row["UHC"]),
                _fmt_money(row["OI"]),
                _fmt_money(row["NETBAL"]),
            ]
            f.write(f" {line_from_values(values)}\n")
            printed_on_page += 1

        if printed_on_page >= PAGE_LENGTH - 2:
            page += 1
            write_page_header()

        total_values = [
            "",
            "",
            "",
            "TOTAL",
            "",
            "",
            _fmt_money(totals["CURBAL"]),
            _fmt_money(totals["UHC"]),
            _fmt_money(totals["OI"]),
            _fmt_money(totals["NETBAL"]),
        ]
        f.write(f" {separator}\n")
        f.write(f" {line_from_values(total_values)}\n")


def main() -> None:
    con = duckdb.connect()

    reptdate_value = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{INPUT_REPTDATE_PARQUET.as_posix()}') LIMIT 1"
    ).fetchone()
    if reptdate_value is None:
        raise ValueError("NPL.REPTDATE.parquet does not contain any rows.")

    reptdate = _to_python_date(reptdate_value[0])
    rdate_text = _worddatx18(reptdate)

    top20 = build_top20_dataframe(con)
    write_report(top20, rdate_text, OUTPUT_TOP20_TXT)


if __name__ == "__main__":
    main()
