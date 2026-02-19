#!/usr/bin/env python3
"""
Program: EIBWIS01
Purpose: Produces PROFILE ON ISLAMIC(PERSONAL/JOINT) MUDHARABAH BESTARI SAVINGS ACCOUNT report
         from monthly AWSC parquet input and writes ASA carriage-control text output.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
import importlib
import duckdb
import polars as pl

# %INC PGM(PBMISFMT);   # Placeholder for SAS include dependency.
from PBMISFMT import format_brchcd
# %INC PGM(PBBDPFMT);   # Placeholder for SAS include dependency.
from PBBDPFMT import *

# ============================================================================
# Configuration and path setup
# ============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DEPOSIT_DIR = INPUT_DIR / "deposit"
MIS_DIR = INPUT_DIR / "mis"

REPTDATE_FILE = DEPOSIT_DIR / "reptdate.parquet"
REPORT_FILE = OUTPUT_DIR / "EIBWIS01.txt"

# OPTIONS YEARCUTOFF=1930 NOCENTER;
AGELIMIT = 12
MAXAGE = 18
AGEBELOW = 11
PAGE_LENGTH = 60

RACE_DESC = {
    "1": "MALAY",
    "2": "CHINESE",
    "3": "INDIAN",
}


def _load_sas_dependencies() -> tuple[object, object]:
    """Load required SAS include dependency equivalents."""
    pbmisfmt = importlib.import_module("Conv_Py.PBMISFMT")
    pbbdpfmt = importlib.import_module("Conv_Py.PBBDPFMT")
    return pbmisfmt, pbbdpfmt

ISARANGD = {
    500: " 1. UP TO RM   500",
    2000: " 2. UP TO RM  2000",
    5000: " 3. UP TO RM  5000",
    10000: " 4. UP TO RM 10000",
    30000: " 5. UP TO RM 30000",
    50000: " 6. UP TO RM 50000",
    75000: " 7. UP TO RM 75000",
    75001: " 8. ABOVE RM 75000",
}


def _fmt_comma(value: float, decimals: int = 2, width: int = 0) -> str:
    text = f"{value:,.{decimals}f}" if decimals > 0 else f"{value:,.0f}"
    return text.rjust(width) if width else text


def _fmt_hundred(value: float) -> str:
    return _fmt_comma(value, decimals=2, width=13)


def _to_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y%m%d"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
    raise ValueError(f"Unable to parse REPTDATE: {value!r}")


def _macro_vars(reptdate_value) -> dict[str, str | int | date]:
    reptdate_obj = _to_date(reptdate_value)
    reptyear = f"{reptdate_obj.year:04d}"
    reptmon = f"{reptdate_obj.month:02d}"
    reptday = reptdate_obj.day
    rdate = reptdate_obj.strftime("%d/%m/%Y")
    # Support both numeric key styles seen in migrated jobs.
    edate_5 = int(reptdate_obj.strftime("%y%m%d"))
    return {
        "REPTYEAR": reptyear,
        "REPTMON": reptmon,
        "REPTDAY": reptday,
        "RDATE": rdate,
        "EDATE": edate_5,
        "REPTDATE_OBJ": reptdate_obj,
    }


def _load_reptdate() -> dict[str, str | int | date]:
    if not REPTDATE_FILE.exists():
        raise FileNotFoundError(f"Missing REPTDATE input: {REPTDATE_FILE}")
    reptdate_df = pl.read_parquet(REPTDATE_FILE)
    if reptdate_df.is_empty():
        raise ValueError("REPTDATE parquet is empty.")
    return _macro_vars(reptdate_df[0, "REPTDATE"])


def _load_rept3(macros: dict[str, str | int | date]) -> pl.DataFrame:
    reptmon = macros["REPTMON"]
    reptday = int(macros["REPTDAY"])
    edate = int(macros["EDATE"])

    awsc_file = MIS_DIR / f"awsc{reptmon}.parquet"
    if not awsc_file.exists():
        raise FileNotFoundError(f"Missing AWSC input: {awsc_file}")

    query = f"""
        SELECT
            *,
            NOACCT / {reptday} AS NOACCT,
            ACCYTD / {reptday} AS ACCYTD,
            CURBAL / {reptday} AS AVGAMT,
            CASE WHEN REPTDATE < {edate} THEN 0.00 ELSE CURBAL END AS CURBAL,
            CASE
                WHEN RANGE = 500 THEN ' 1. UP TO RM   500'
                WHEN RANGE = 2000 THEN ' 2. UP TO RM  2000'
                WHEN RANGE = 5000 THEN ' 3. UP TO RM  5000'
                WHEN RANGE = 10000 THEN ' 4. UP TO RM 10000'
                WHEN RANGE = 30000 THEN ' 5. UP TO RM 30000'
                WHEN RANGE = 50000 THEN ' 6. UP TO RM 50000'
                WHEN RANGE = 75000 THEN ' 7. UP TO RM 75000'
                WHEN RANGE = 75001 THEN ' 8. ABOVE RM 75000'
                ELSE 'UNKNOWN'
            END AS DEPRANGE
        FROM read_parquet('{awsc_file.as_posix()}')
        WHERE REPTDATE <= {edate}
    """

    con = duckdb.connect()
    try:
        return con.execute(query).pl()
    finally:
        con.close()


def _build_report_lines(
    rept3: pl.DataFrame,
    macros: dict[str, str | int | date],
    pbmisfmt: object,
    pbbdpfmt: object,
) -> list[str]:
    title_lines = [
        "REPORT ID : EIBWIS01",
        "PUBLIC ISLAMIC BANK BERHAD",
        "FOR ISLAMIC BANKING DEPARTMENT",
        "PROFILE ON ISLAMIC(PERSONAL/JOINT)MUDHARABAH BESTARI SAVINGS ACCOUNT",
        f"REPORT AS AT {macros['RDATE']}",
    ]

    totals = rept3.select(
        pl.sum("NOACCT").alias("NOACCT"),
        pl.sum("ACCYTD").alias("ACCYTD"),
        pl.sum("CURBAL").alias("CURBAL"),
        pl.sum("AVGAMT").alias("AVGAMT"),
    ).to_dicts()[0]

    _ = pbbdpfmt

    race_matrix = (
        rept3.with_columns(pl.col("RACE").cast(pl.Utf8))
        .group_by(["DEPRANGE", "RACE"])
        .agg(pl.sum("NOACCT").alias("RACE_NOACCT"))
        .pivot(values="RACE_NOACCT", index="DEPRANGE", on="RACE")
        .fill_null(0)
    )

    summary = (
        rept3.group_by("DEPRANGE")
        .agg(
            pl.sum("NOACCT").alias("NOACCT"),
            pl.sum("ACCYTD").alias("ACCYTD"),
            pl.sum("CURBAL").alias("CURBAL"),
            pl.sum("AVGAMT").alias("AVGAMT"),
        )
        .join(race_matrix, on="DEPRANGE", how="left")
        .fill_null(0)
        .sort("DEPRANGE")
    )

    header = (
        "DEPOSIT RANGE                 "
        "NO OF A/C     OPEN YTD      O/S BALANCE    AVERAGE BAL    MALAY    CHINESE     INDIAN     OTHERS"
    )

    lines: list[str] = []
    lines.extend(title_lines)
    lines.append("")
    lines.append(header)
    lines.append("-" * len(header))

    for row in summary.iter_rows(named=True):
        noacct = float(row.get("NOACCT", 0.0))
        accytd = float(row.get("ACCYTD", 0.0))
        curbal = float(row.get("CURBAL", 0.0))
        avgamt = float(row.get("AVGAMT", 0.0))
        malay = 0.0
        chinese = 0.0
        indian = 0.0
        others = 0.0
        for race_code in ("0", "1", "2", "3", "4"):
            race_count = float(row.get(race_code, 0.0))
            race_label = pbmisfmt.format_race(race_code) if hasattr(pbmisfmt, "format_race") else RACE_DESC.get(race_code, "OTHERS")
            if race_label == "MALAY":
                malay += race_count
            elif race_label == "CHINESE":
                chinese += race_count
            elif race_label == "INDIAN":
                indian += race_count
            else:
                others += race_count

        lines.append(
            f"{str(row['DEPRANGE'])[:30]:30} "
            f"{_fmt_comma(noacct, 0, 11)} "
            f"{_fmt_comma(accytd, 0, 11)} "
            f"{_fmt_comma(curbal, 2, 14)} "
            f"{_fmt_comma(avgamt, 2, 14)} "
            f"{_fmt_comma(malay, 0, 8)} "
            f"{_fmt_comma(chinese, 0, 10)} "
            f"{_fmt_comma(indian, 0, 10)} "
            f"{_fmt_comma(others, 0, 10)}"
        )

    total_noacct = float(totals["NOACCT"]) if totals["NOACCT"] is not None else 0.0
    total_accytd = float(totals["ACCYTD"]) if totals["ACCYTD"] is not None else 0.0
    total_curbal = float(totals["CURBAL"]) if totals["CURBAL"] is not None else 0.0
    total_avgamt = float(totals["AVGAMT"]) if totals["AVGAMT"] is not None else 0.0

    lines.append("-" * len(header))
    lines.append(
        f"{'TOTAL':30} "
        f"{_fmt_comma(total_noacct, 0, 11)} "
        f"{_fmt_comma(total_accytd, 0, 11)} "
        f"{_fmt_comma(total_curbal, 2, 14)} "
        f"{_fmt_comma(total_avgamt, 2, 14)}"
    )
    lines.append("")
    lines.append("PERCENTAGE OF TOTAL")

    for row in summary.iter_rows(named=True):
        noacct_pct = 0.0 if total_noacct == 0 else float(row["NOACCT"]) / total_noacct * 100
        accytd_pct = 0.0 if total_accytd == 0 else float(row["ACCYTD"]) / total_accytd * 100
        curbal_pct = 0.0 if total_curbal == 0 else float(row["CURBAL"]) / total_curbal * 100
        avgamt_pct = 0.0 if total_avgamt == 0 else float(row["AVGAMT"]) / total_avgamt * 100
        avg_amt_acct = 0.0 if float(row["NOACCT"]) == 0 else float(row["CURBAL"]) / float(row["NOACCT"])
        avg_bal_acct = 0.0 if float(row["NOACCT"]) == 0 else float(row["AVGAMT"]) / float(row["NOACCT"])

        lines.append(
            f"{str(row['DEPRANGE'])[:30]:30} "
            f"NO% {_fmt_comma(noacct_pct, 2, 6)} "
            f"OP% {_fmt_comma(accytd_pct, 2, 6)} "
            f"OS% {_fmt_comma(curbal_pct, 2, 6)} "
            f"AV% {_fmt_comma(avgamt_pct, 2, 6)} "
            f"AVG AMT/AC {_fmt_hundred(avg_amt_acct)} "
            f"AVG BAL {_fmt_hundred(avg_bal_acct)}"
        )

    return lines


def _write_asa_report(lines: list[str], report_path: Path, page_length: int = PAGE_LENGTH) -> None:
    output_lines: list[str] = []
    line_no = 0

    for idx, line in enumerate(lines):
        if line_no == 0:
            cc = "1" if idx == 0 or (idx > 0 and lines[idx - 1] == "\f") else "1"
        else:
            cc = " "

        output_lines.append(f"{cc}{line}")
        line_no += 1

        if line_no >= page_length:
            line_no = 0

    report_path.write_text("\n".join(output_lines) + "\n", encoding="utf-8")


def main() -> None:
    pbmisfmt, pbbdpfmt = _load_sas_dependencies()
    macros = _load_reptdate()
    rept3 = _load_rept3(macros)
    report_lines = _build_report_lines(rept3, macros, pbmisfmt, pbbdpfmt)
    _write_asa_report(report_lines, REPORT_FILE, PAGE_LENGTH)
    print(f"Report generated: {REPORT_FILE}")


if __name__ == "__main__":
    main()
