#!/usr/bin/env python3
"""
File Name: EIIBTLIQ
Report: Breakdown by Maturity Profile (Trade Bills) with Undrawn Portion
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

import duckdb
import polars as pl


# =============================================================================
# PATH CONFIGURATION
# =============================================================================

BASE_INPUT_DIR = Path("input")
BASE_OUTPUT_DIR = Path("output")
BASE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPTDATE_FILE = BASE_INPUT_DIR / "reptdate.parquet"
IBTRAD_TEMPLATE = BASE_INPUT_DIR / "ibtrad_{reptmon}{nowk}.parquet"
IBTMAST_TEMPLATE = BASE_INPUT_DIR / "ibtmast_{reptmon}{nowk}.parquet"

OUTPUT_REPORT = BASE_OUTPUT_DIR / "EIIBTLIQ.txt"


# =============================================================================
# DATA STRUCTURES
# =============================================================================


@dataclass(frozen=True)
class ReportDates:
    reptdate: date
    reptmon: str
    nowk: str
    rdate_str: str


# =============================================================================
# DATE AND FORMAT HELPERS
# =============================================================================


def compute_week(day: int) -> str:
    if 1 <= day <= 8:
        return "1"
    if 9 <= day <= 15:
        return "2"
    if 16 <= day <= 22:
        return "3"
    return "4"


def is_leap_year(year: int) -> bool:
    return year % 4 == 0


def days_in_month(year: int) -> list[int]:
    feb_days = 29 if is_leap_year(year) else 28
    return [31, feb_days, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


def to_date(value) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value)
    if isinstance(value, (int, float)):
        if value <= 0:
            return None
        base = date(1960, 1, 1)
        return base + timedelta(days=int(value))
    return None


def remfmt(remmth: float) -> str:
    if remmth <= 0.1:
        return "01"
    if remmth <= 1:
        return "02"
    if remmth <= 3:
        return "03"
    if remmth <= 6:
        return "04"
    if remmth <= 12:
        return "05"
    return "06"


def load_report_dates() -> ReportDates:
    if not REPTDATE_FILE.exists():
        raise FileNotFoundError(f"Missing REPTDATE file: {REPTDATE_FILE}")

    df = pl.read_parquet(REPTDATE_FILE)
    if df.is_empty():
        raise ValueError("REPTDATE parquet is empty.")

    reptdate = to_date(df["REPTDATE"][0])
    if reptdate is None:
        raise ValueError("Invalid REPTDATE value.")

    reptmon = f"{reptdate.month:02d}"
    nowk = compute_week(reptdate.day)
    rdate_str = f"{reptdate.day:02d}/{reptdate.month:02d}/{str(reptdate.year)[-2:]}"
    return ReportDates(
        reptdate=reptdate,
        reptmon=reptmon,
        nowk=nowk,
        rdate_str=rdate_str,
    )


# =============================================================================
# BUSINESS LOGIC
# =============================================================================


def nxtbldt(
    bldate: date,
    issdte: date,
    payfreq: str,
    freq: int,
) -> date:
    if payfreq == "6":
        dd = bldate.day + 14
        mm = bldate.month
        yy = bldate.year
        month_days = days_in_month(yy)
        if dd > month_days[mm - 1]:
            dd -= month_days[mm - 1]
            mm += 1
            if mm > 12:
                mm -= 12
                yy += 1
    else:
        dd = issdte.day
        mm = bldate.month + freq
        yy = bldate.year
        if mm > 12:
            mm -= 12
            yy += 1

    month_days = days_in_month(yy)
    if dd > month_days[mm - 1]:
        dd = month_days[mm - 1]
    return date(yy, mm, dd)


def remmth(
    matdt: date,
    rpy: int,
    rpmth: int,
    rpday: int,
    rpd_days: list[int],
) -> float:
    mdyr = matdt.year
    mdmth = matdt.month
    mdday = matdt.day
    if mdday > rpd_days[rpmth - 1]:
        mdday = rpd_days[rpmth - 1]
    remy = mdyr - rpy
    remm = mdmth - rpmth
    remd = mdday - rpday
    return remy * 12 + remm + remd / rpd_days[rpmth - 1]


def build_note_summary(df: pl.DataFrame, reptdate: date) -> pl.DataFrame:
    rpy = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day
    rpd_days = days_in_month(rpy)

    rows = []

    for row in df.iter_rows(named=True):
        prodcd = str(row.get("PRODCD") or "").strip()
        product = row.get("PRODUCT")
        if not (prodcd.startswith("34") or product in (225, 226)):
            continue

        custcd = str(row.get("CUSTCD") or "").strip()
        cust = "08" if custcd in {"77", "78", "95", "96"} else "09"
        prod = "BT"
        if custcd in {"77", "78", "95", "96"}:
            item = "214" if prod == "HL" else "219"
        else:
            if prod == "FL":
                item = "211"
            elif prod == "RC":
                item = "212"
            else:
                item = "219"

        bldate = to_date(row.get("BLDATE"))
        issdte = to_date(row.get("ISSDTE"))
        exprdate = to_date(row.get("EXPRDATE"))
        balance = float(row.get("BALANCE") or 0)
        payamt = float(row.get("PAYAMT") or 0)

        days = 0
        if bldate is not None:
            days = (reptdate - bldate).days

        if exprdate is not None and (exprdate - reptdate).days < 8:
            remmth_value = 0.1
        else:
            payfreq = "3"
            freq = 6
            if product in (350, 910, 925):
                if exprdate is not None:
                    bldate = exprdate
            elif bldate is None:
                if issdte is None:
                    continue
                bldate = issdte
                while bldate <= reptdate:
                    bldate = nxtbldt(bldate, issdte, payfreq, freq)

            if payamt < 0:
                payamt = 0
            if exprdate is not None and (bldate > exprdate or balance <= payamt):
                bldate = exprdate

            remmth_value = 0.1
            while exprdate is not None and bldate <= exprdate:
                matdt = bldate
                remmth_value = remmth(matdt, rpy, rpmth, rpday, rpd_days)
                if remmth_value > 12 or bldate == exprdate:
                    break

                rows.append(
                    {
                        "BNMCODE": f"95{item}{cust}{remfmt(remmth_value)}0000Y",
                        "AMOUNT": payamt,
                    }
                )

                remmth_for_93 = 13 if days > 89 else remmth_value
                rows.append(
                    {
                        "BNMCODE": f"93{item}{cust}{remfmt(remmth_for_93)}0000Y",
                        "AMOUNT": payamt,
                    }
                )

                balance -= payamt
                bldate = nxtbldt(bldate, issdte, payfreq, freq)
                if bldate > exprdate or balance <= payamt:
                    bldate = exprdate

        rows.append(
            {
                "BNMCODE": f"95{item}{cust}{remfmt(remmth_value)}0000Y",
                "AMOUNT": balance,
            }
        )

        remmth_for_93 = 13 if days > 89 else remmth_value
        rows.append(
            {
                "BNMCODE": f"93{item}{cust}{remfmt(remmth_for_93)}0000Y",
                "AMOUNT": balance,
            }
        )

    if not rows:
        return pl.DataFrame({"BNMCODE": [], "AMOUNT": []})

    note_df = pl.DataFrame(rows)
    return note_df.group_by("BNMCODE").agg(pl.col("AMOUNT").sum())


def build_undrawn_summary(df: pl.DataFrame, reptdate: date) -> pl.DataFrame:
    rpy = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day
    rpd_days = days_in_month(rpy)

    rows = []
    for row in df.iter_rows(named=True):
        subacct = str(row.get("SUBACCT") or "").strip()
        custcd = str(row.get("CUSTCD") or "").strip()
        dcurbal = row.get("DCURBAL")
        if subacct != "OV" or not custcd or dcurbal is None:
            continue

        exprdate = to_date(row.get("EXPRDATE"))
        if exprdate is None:
            continue

        matdt = exprdate
        if (matdt - reptdate).days < 8:
            remmth_value = 0.1
        else:
            remmth_value = remmth(matdt, rpy, rpmth, rpday, rpd_days)

        amount = float(row.get("DUNDRAWN") or 0)
        rows.append(
            {
                "BNMCODE": f"95{'429'}00{remfmt(remmth_value)}0000Y",
                "AMOUNT": amount,
            }
        )

    if not rows:
        return pl.DataFrame({"BNMCODE": [], "AMOUNT": []})

    unote_df = pl.DataFrame(rows)
    return unote_df.group_by("BNMCODE").agg(pl.col("AMOUNT").sum())


# =============================================================================
# REPORT GENERATION (ASA CARRIAGE CONTROL)
# =============================================================================


def write_report_section(
    handle,
    title: str | None,
    summary_df: pl.DataFrame,
    page_length: int = 60,
    start_new_page: bool = False,
) -> int:
    line_count = 0

    def emit_line(text: str, new_page: bool = False) -> None:
        nonlocal line_count
        control = " "
        if new_page or line_count >= page_length:
            control = "1"
            line_count = 0
        handle.write(f"{control}{text}\n")
        line_count += 1

    if start_new_page:
        emit_line("", new_page=True)

    if title:
        emit_line(title, new_page=not start_new_page)
    else:
        emit_line("", new_page=not start_new_page)

    emit_line("BNMCODE".ljust(20) + "AMOUNT".rjust(20))
    emit_line("-" * 40)

    if summary_df.is_empty():
        emit_line("NO DATA")
        emit_line("-" * 40)
        emit_line("TOTAL".ljust(20) + f"{0:>20,.2f}")
        return line_count

    sorted_df = summary_df.sort("BNMCODE")
    for row in sorted_df.iter_rows(named=True):
        bnmcode = str(row["BNMCODE"])
        amount = float(row["AMOUNT"])
        emit_line(f"{bnmcode:<20}{amount:>20,.2f}")

    total_amount = sorted_df.select(pl.col("AMOUNT").sum()).item(0, 0)
    emit_line("-" * 40)
    emit_line(f"{'TOTAL':<20}{total_amount:>20,.2f}")
    return line_count


def write_report(
    note_summary: pl.DataFrame,
    undrawn_summary: pl.DataFrame,
    report_dates: ReportDates,
) -> None:
    OUTPUT_REPORT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_REPORT, "w") as handle:
        write_report_section(
            handle,
            title=None,
            summary_df=note_summary,
            start_new_page=True,
        )

        title = f"UNDRAWN PORTION FOR TRADE BILLS AS AT {report_dates.rdate_str}"
        write_report_section(
            handle,
            title=title,
            summary_df=undrawn_summary,
            start_new_page=True,
        )


# =============================================================================
# MAIN EXECUTION
# =============================================================================


def load_parquet(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing input parquet: {path}")
    con = duckdb.connect()
    return con.execute(f"SELECT * FROM '{path.as_posix()}'").pl()


def main() -> None:
    report_dates = load_report_dates()

    ibtrad_file = IBTRAD_TEMPLATE.with_name(
        IBTRAD_TEMPLATE.name.format(
            reptmon=report_dates.reptmon,
            nowk=report_dates.nowk,
        )
    )
    ibtmast_file = IBTMAST_TEMPLATE.with_name(
        IBTMAST_TEMPLATE.name.format(
            reptmon=report_dates.reptmon,
            nowk=report_dates.nowk,
        )
    )

    ibtrad_df = load_parquet(ibtrad_file)
    ibtmast_df = load_parquet(ibtmast_file)

    note_summary = build_note_summary(ibtrad_df, report_dates.reptdate)
    undrawn_summary = build_undrawn_summary(ibtmast_df, report_dates.reptdate)

    write_report(note_summary, undrawn_summary, report_dates)


if __name__ == "__main__":
    main()
