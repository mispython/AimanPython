#!/usr/bin/env python3
"""
Program : DMMIPB06
Purpose : Profile on PB Bright Star Savings Accounts (PRODUCT=208)

Original SAS dependency placeholders:
# %INC PGM(PBBDPFMT,PBMISFMT);
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

import duckdb
import polars as pl

from PBBDPFMT import SAProductFormat
from PBMISFMT import format_race

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR = os.environ.get("BASE_DIR", "/data")
DEPOSIT_DIR = os.path.join(BASE_DIR, "deposit")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

REPTDATE_FILE = os.path.join(DEPOSIT_DIR, "REPTDATE.parquet")
SAVING_FILE = os.path.join(DEPOSIT_DIR, "SAVING.parquet")
REPORT_FILE = os.path.join(OUTPUT_DIR, "DMMIPB06.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =============================================================================
# REPORT CONFIGURATION
# =============================================================================
ASA_PAGE = "1"
ASA_LINE = " "
PAGE_LENGTH = 60  # default when not specified in SAS

PROFPBS_BANDS = [
    (float("-inf"), 500.0, " 1)BELOW RM     500.00"),
    (500.0, 1000.0, " 2)UP TO RM   1,000.00"),
    (1000.0, 2000.0, " 3)UP TO RM   2,000.00"),
    (2000.0, 5000.0, " 4)UP TO RM   5,000.00"),
    (5000.0, 10000.0, " 5)UP TO RM  10,000.00"),
    (10000.0, 20000.0, " 6)UP TO RM  20,000.00"),
    (20000.0, 30000.0, " 7)UP TO RM  30,000.00"),
    (30000.0, 50000.0, " 8)UP TO RM  50,000.00"),
    (50000.0, 75000.0, " 9)UP TO RM  75,000.00"),
    (75000.0, 100000.0, "10)UP TO RM 100,000.00"),
    (100000.0, float("inf"), "11)ABOVE RM 100,000.00"),
]

DEPRANGE_ORDER = [label for _, _, label in PROFPBS_BANDS]
DEPRANGE_RANK = {label: idx for idx, label in enumerate(DEPRANGE_ORDER)}

AGE_GROUPS = [
    " BELOW 7 YEAYS",
    " 7 TO BELOW 12",
    "12 TO BELOW 18",
    " ABOVE 18     ",
]
RACE_GROUPS = ["MALAY", "CHINESE", "INDIAN", "OTHERS"]

SDRANGE_STEPS = [
    5, 10, 50, 100, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500,
    5000, 6000, 7000, 8000, 9000, 10000, 15000, 20000, 25000, 30000, 35000,
    40000, 45000, 50000, 55000, 60000, 65000, 70000, 75000, 80000, 85000,
    90000, 95000, 100000, 150000, 200000, 300000,
]


@dataclass(frozen=True)
class ReportContext:
    reptmon: str
    reptyear: int
    rdate: str
    zdate: int


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def parse_any_date(value) -> Optional[date]:
    """Parse common date representations seen in converted parquet inputs."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    if isinstance(value, (int, float)):
        ivalue = int(value)
        svalue = str(ivalue)

        if len(svalue) == 8:
            # SAS source uses MMDDYY8. conversion for OPENDT/BDATE.
            try:
                return date(int(svalue[4:8]), int(svalue[:2]), int(svalue[2:4]))
            except ValueError:
                pass
            # Conservative fallback for YYYYMMDD style values.
            try:
                return date(int(svalue[:4]), int(svalue[4:6]), int(svalue[6:8]))
            except ValueError:
                pass

        # SAS numeric date serial fallback.
        try:
            return date(1960, 1, 1) + timedelta(days=ivalue)
        except OverflowError:
            return None

    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def invalue_sdrange(curbal: Optional[float]) -> Optional[int]:
    """Equivalent of SAS INVALUE SDRANGE for the ranges used by this program."""
    if curbal is None:
        return None
    amount = float(curbal)
    for step in SDRANGE_STEPS:
        if amount <= step:
            return step
    return SDRANGE_STEPS[-1]


def format_profpbs(range_value: Optional[int]) -> str:
    """Equivalent of PROC FORMAT VALUE PROFPBS."""
    if range_value is None:
        return ""
    numeric = float(range_value)
    for lower, upper, label in PROFPBS_BANDS:
        if lower <= numeric <= upper:
            return label
    return ""


def format_age_group(age: int) -> str:
    """Equivalent of PROC FORMAT VALUE AGEGP."""
    if age < 7:
        return " BELOW 7 YEAYS"
    if age < 12:
        return " 7 TO BELOW 12"
    if age < 18:
        return "12 TO BELOW 18"
    return " ABOVE 18     "


def pct(part: float, total: float) -> float:
    return 0.0 if total == 0 else (part / total) * 100.0


def load_reptdate_context() -> ReportContext:
    """Build macro-variable equivalent context from DEPOSIT.REPTDATE."""
    con = duckdb.connect()
    row = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{REPTDATE_FILE}') LIMIT 1"
    ).fetchone()
    con.close()

    if row is None:
        raise ValueError("REPTDATE input is empty.")

    reptdate = parse_any_date(row[0])
    if reptdate is None:
        raise ValueError("Unable to parse REPTDATE value.")

    return ReportContext(
        reptmon=reptdate.strftime("%m"),
        reptyear=int(reptdate.strftime("%Y")),
        rdate=reptdate.strftime("%d/%m/%Y"),
        zdate=int(reptdate.strftime("%j")),
    )


def calculate_age(bdate_value, reptyear: int) -> int:
    bdate = parse_any_date(bdate_value)
    if bdate is None:
        return 0
    return max(0, reptyear - bdate.year)


def build_pb06_dataset(ctx: ReportContext) -> pl.DataFrame:
    """Build PB06 dataset from SAVING input with SAS filter/derivation rules."""
    con = duckdb.connect()
    saving = con.execute(f"SELECT * FROM read_parquet('{SAVING_FILE}')").pl()
    con.close()

    savg = saving.filter(
        (pl.col("PRODUCT") == 208)
        & (~pl.col("OPENIND").cast(pl.Utf8).is_in(["B", "C", "P", "Z"]))
        & (pl.col("CURBAL") != 0)
    )

    pb06 = savg.with_columns(
        [
            pl.col("CUSTCODE")
            .map_elements(lambda v: str(v) if v is not None else "", return_dtype=pl.Utf8)
            .alias("CUSTCD"),
            pl.col("BRANCH")
            .map_elements(lambda v: str(v) if v is not None else "", return_dtype=pl.Utf8)
            .alias("STATECD"),
            pl.col("PRODUCT")
            .map_elements(
                lambda v: SAProductFormat.format(int(v)) if v is not None else "",
                return_dtype=pl.Utf8,
            )
            .alias("PRODCD"),
            pl.lit("D").alias("AMTIND"),
            pl.col("CURBAL")
            .map_elements(invalue_sdrange, return_dtype=pl.Int64)
            .alias("RANGE"),
            pl.col("BDATE")
            .map_elements(lambda v: calculate_age(v, ctx.reptyear), return_dtype=pl.Int64)
            .alias("AGE"),
            pl.col("RACE")
            .map_elements(
                lambda v: format_race(str(v) if v is not None else None),
                return_dtype=pl.Utf8,
            )
            .alias("RACE_DESC"),
            pl.lit(1).alias("NOACCT"),
        ]
    ).with_columns(
        [
            pl.col("RANGE")
            .map_elements(format_profpbs, return_dtype=pl.Utf8)
            .alias("DEPRANGE"),
            pl.col("AGE")
            .map_elements(format_age_group, return_dtype=pl.Utf8)
            .alias("AGE_GROUP"),
        ]
    )

    return pb06


def summarize_for_report(pb06: pl.DataFrame) -> pl.DataFrame:
    """Build PROC TABULATE-like summary table for output rendering."""
    base = pb06.group_by("DEPRANGE").agg(
        [
            pl.col("NOACCT").sum().alias("NOACCT_SUM"),
            pl.col("ACCYTD").sum().alias("ACCYTD_SUM"),
            pl.col("CURBAL").sum().alias("CURBAL_SUM"),
        ]
    )

    age_pivot = (
        pb06.group_by(["DEPRANGE", "AGE_GROUP"])
        .agg(pl.col("NOACCT").sum().alias("CNT"))
        .pivot(on="AGE_GROUP", index="DEPRANGE", values="CNT")
    )
    age_rename = {c: f"AGE_{c}" for c in age_pivot.columns if c != "DEPRANGE"}
    age_pivot = age_pivot.rename(age_rename)
    for age in AGE_GROUPS:
        col = f"AGE_{age}"
        if col not in age_pivot.columns:
            age_pivot = age_pivot.with_columns(pl.lit(0).alias(col))

    race_pivot = (
        pb06.group_by(["DEPRANGE", "RACE_DESC"])
        .agg(pl.col("NOACCT").sum().alias("CNT"))
        .pivot(on="RACE_DESC", index="DEPRANGE", values="CNT")
    )
    race_rename = {c: f"RACE_{c}" for c in race_pivot.columns if c != "DEPRANGE"}
    race_pivot = race_pivot.rename(race_rename)
    for race in RACE_GROUPS:
        col = f"RACE_{race}"
        if col not in race_pivot.columns:
            race_pivot = race_pivot.with_columns(pl.lit(0).alias(col))

    summary = (
        base.join(age_pivot, on="DEPRANGE", how="left")
        .join(race_pivot, on="DEPRANGE", how="left")
        .fill_null(0)
    )

    # Ensure all deposit ranges exist and preserve SAS format-order sequence.
    all_ranges = pl.DataFrame({"DEPRANGE": DEPRANGE_ORDER})
    summary = (
        all_ranges.join(summary, on="DEPRANGE", how="left")
        .fill_null(0)
        .with_columns(
            pl.col("DEPRANGE")
            .map_elements(lambda x: DEPRANGE_RANK.get(x, 999), return_dtype=pl.Int64)
            .alias("_RANK")
        )
        .sort("_RANK")
        .drop("_RANK")
    )

    return summary


def write_report(summary: pl.DataFrame, ctx: ReportContext) -> None:
    """Write output report with ASA carriage-control characters."""
    total_noacct = float(summary["NOACCT_SUM"].sum())
    total_accytd = float(summary["ACCYTD_SUM"].sum())
    total_curbal = float(summary["CURBAL_SUM"].sum())

    lines: list[str] = [
        f"{ASA_PAGE}PUBLIC BANK BERHAD   REPORT=DMMIPB06",
        f"{ASA_LINE}FOR PRODUCT DEVELOPMENT & MARKETING",
        f"{ASA_LINE}PROFILE ON PB BRIGHT STAR SAVINGS ACCOUNTS AS AT {ctx.rdate}",
        f"{ASA_LINE}",
        f"{ASA_LINE}{'DEPOSIT RANGE':<25}{'NO OF A/C':>12}{'%':>8}"
        f"{'OPEN YTD':>12}{'%':>8}{'AMOUNT':>20}{'%':>8}{'AVG AMT/ACCOUNT':>18}",
    ]

    for row in summary.iter_rows(named=True):
        noacct = float(row["NOACCT_SUM"])
        accytd = float(row["ACCYTD_SUM"])
        curbal = float(row["CURBAL_SUM"])
        avg_amt = curbal / noacct if noacct else 0.0

        lines.append(
            f"{ASA_LINE}{row['DEPRANGE']:<25}"
            f"{noacct:>12,.0f}{pct(noacct, total_noacct):>8.2f}"
            f"{accytd:>12,.0f}{pct(accytd, total_accytd):>8.2f}"
            f"{curbal:>20,.2f}{pct(curbal, total_curbal):>8.2f}{avg_amt:>18.2f}"
        )

    lines.extend(
        [
            f"{ASA_LINE}{'TOTAL':<25}{total_noacct:>12,.0f}{100.0:>8.2f}"
            f"{total_accytd:>12,.0f}{100.0:>8.2f}{total_curbal:>20,.2f}{100.0:>8.2f}"
            f"{(total_curbal / total_noacct if total_noacct else 0.0):>18.2f}",
            f"{ASA_LINE}",
            f"{ASA_LINE}AGE BREAKDOWN (NO OF A/C)",
        ]
    )

    for age in AGE_GROUPS:
        lines.append(
            f"{ASA_LINE}{age:<25}{int(summary[f'AGE_{age}'].sum()):>12,}"
        )

    lines.append(f"{ASA_LINE}RACE BREAKDOWN (NO OF A/C)")
    for race in RACE_GROUPS:
        lines.append(
            f"{ASA_LINE}{race:<25}{int(summary[f'RACE_{race}'].sum()):>12,}"
        )

    with open(REPORT_FILE, "w", encoding="utf-8") as handle:
        for line in lines:
            handle.write(line + "\n")


def main() -> None:
    context = load_reptdate_context()
    pb06 = build_pb06_dataset(context)
    summary = summarize_for_report(pb06)
    write_report(summary, context)
    print(f"Generated {REPORT_FILE}")


if __name__ == "__main__":
    main()
