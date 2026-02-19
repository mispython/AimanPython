#!/usr/bin/env python3
"""
Program: DIIMISC7
Purpose: Generate PIBB AL-MUDHARABAH FD ACCOUNT report for OLD and NEW segments.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import duckdb
import polars as pl

from PBMISFMT import format_brchcd

# -----------------------------------------------------------------------------
# Path setup (defined early per migration requirement)
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPTDATE_PARQUET = INPUT_DIR / "reptdate.parquet"  # DEPOSIT.REPTDATE
DYIBUS_PATTERN = "dyibus{reptmon}.parquet"          # MIS.DYIBUS&REPTMON
REPORT_FILE = OUTPUT_DIR / "diimisc7_report.txt"    # SAS LISTING output

PAGE_LENGTH = 60
LINE_WIDTH = 132

# %INC PGM(PBMISFMT);
# Dependency reference retained in migration:
# - Conv_Py/PBMISFMT.py

INTPLAN_TO_MTH = {
    340: 1, 448: 1, 660: 1, 720: 1,
    352: 2, 449: 2, 661: 2, 721: 2,
    341: 3, 450: 3, 662: 3, 722: 3,
    353: 4, 451: 4, 663: 4, 723: 4,
    354: 5, 452: 5, 664: 5, 724: 5,
    342: 6, 453: 6, 665: 6, 725: 6,
    355: 7, 454: 7, 666: 7, 726: 7,
    356: 8, 455: 8, 667: 8, 727: 8,
    343: 9, 456: 9, 668: 9, 728: 9,
    357: 10, 457: 10, 669: 10, 729: 10,
    358: 11, 458: 11, 670: 11, 730: 11,
    344: 12, 459: 12, 671: 12, 731: 12,
    588: 13, 461: 13, 672: 13, 732: 13,
    589: 14, 462: 14, 673: 14, 733: 14,
    345: 15, 463: 15, 674: 15, 734: 15,
    590: 16, 675: 16,
    591: 17, 676: 17,
    346: 18, 464: 18, 677: 18, 735: 18,
    592: 19, 678: 19,
    593: 20, 679: 20,
    347: 21, 465: 21, 680: 21, 736: 21,
    594: 22, 681: 22,
    595: 23, 682: 23,
    348: 24, 466: 24, 683: 24, 737: 24,
    596: 25, 684: 25,
    597: 26, 685: 26,
    359: 27, 686: 27,
    598: 28, 687: 28,
    599: 29, 688: 29,
    540: 30, 580: 30, 689: 30,
    690: 31,
    691: 32,
    541: 33, 581: 33, 692: 33,
    693: 34,
    694: 35,
    349: 36, 467: 36, 695: 36, 738: 36,
    696: 37,
    697: 38,
    542: 39, 582: 39, 698: 39,
    699: 40,
    700: 41,
    543: 42, 583: 42, 701: 42,
    702: 43,
    703: 44,
    544: 45, 584: 45, 704: 45,
    705: 46,
    706: 47,
    350: 48, 468: 48, 707: 48, 739: 48,
    708: 49,
    709: 50,
    545: 51, 585: 51, 710: 51,
    711: 52,
    712: 53,
    546: 54, 586: 54, 713: 54,
    714: 55,
    715: 56,
    547: 57, 587: 57, 716: 57,
    717: 58,
    718: 59,
    351: 60, 719: 60, 740: 60,
}


@dataclass(frozen=True)
class ReportContext:
    reptdate: date
    reptmon: str
    reptfq: str
    rdate_display: str
    sdate: date
    edate: date


def _as_date(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def load_context() -> ReportContext:
    df = pl.read_parquet(REPTDATE_PARQUET)
    if df.height == 0:
        raise ValueError("reptdate.parquet is empty")

    reptdate = _as_date(df[0, "REPTDATE"])
    reptfq = str(df[0, "REPTFQ"]) if "REPTFQ" in df.columns else "Y"

    return ReportContext(
        reptdate=reptdate,
        reptmon=f"{reptdate.month:02d}",
        reptfq=reptfq,
        rdate_display=reptdate.strftime("%d%m%y"),
        sdate=reptdate.replace(day=1),
        edate=reptdate,
    )


def resolve_dyibus_file(reptmon: str) -> Path:
    candidates = [
        INPUT_DIR / DYIBUS_PATTERN.format(reptmon=reptmon),
        INPUT_DIR / DYIBUS_PATTERN.upper().format(reptmon=reptmon),
    ]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError(f"Unable to locate DYIBUS parquet for month {reptmon}: {candidates}")


def build_base_dataframe(ctx: ReportContext) -> tuple[pl.DataFrame, pl.DataFrame]:
    dyibus_file = resolve_dyibus_file(ctx.reptmon)

    con = duckdb.connect()
    try:
        df = con.execute(
            """
            SELECT BRANCH, INTPLAN, REPTDATE, FDI, FDINO, FDINO2, CUSTCD
            FROM read_parquet(?)
            """,
            [str(dyibus_file)],
        ).pl()
    finally:
        con.close()

    df = df.with_columns([
        pl.col("REPTDATE").map_elements(_as_date, return_dtype=pl.Date),
        pl.col("INTPLAN").cast(pl.Int64),
        pl.col("BRANCH").cast(pl.Int64),
    ])

    mth_expr = pl.col("INTPLAN").replace(INTPLAN_TO_MTH, default=0)
    branch_code_expr = pl.col("BRANCH").map_elements(format_brchcd, return_dtype=pl.String)

    prepared = df.with_columns([
        mth_expr.alias("MTH"),
        branch_code_expr.alias("BRCHCD"),
        (pl.format("{:03d}/{}", pl.col("BRANCH"), branch_code_expr)).alias("BRCH"),
    ])

    dyibua = prepared.filter(pl.col("REPTDATE") == pl.lit(ctx.edate))
    dyibua1 = prepared.filter((pl.col("REPTDATE") >= pl.lit(ctx.sdate)) & (pl.col("REPTDATE") <= pl.lit(ctx.edate)))
    return dyibua, dyibua1


def agg_todate(df: pl.DataFrame, include_custcd: bool) -> pl.DataFrame:
    keys = ["BRCH", "MTH"] + (["CUSTCD"] if include_custcd else [])
    return df.group_by(keys).agg([
        pl.col("FDI").sum().alias("FDI"),
        pl.col("FDINO").sum().alias("FDINO"),
        pl.col("FDINO2").sum().alias("FDINO2"),
    ])


def agg_avg(df: pl.DataFrame, include_custcd: bool, days: int) -> pl.DataFrame:
    agged = agg_todate(df, include_custcd)
    if agged.height == 0:
        return agged
    return agged.with_columns([
        (pl.col("FDI") / days).alias("FDI"),
        (pl.col("FDINO") / days).alias("FDINO"),
        (pl.col("FDINO2") / days).alias("FDINO2"),
    ])


def mth_label(m: int) -> str:
    return f"{m} MONTH" if m == 1 else f"{m} MONTHS"


def cust_label(c: int) -> str:
    return "RETAIL" if c in (77, 78, 95, 96) else "CORPORATE"


def render_section(title: str, subtitle4: str, df: pl.DataFrame, include_custcd: bool) -> list[str]:
    lines: list[str] = [
        "1" + "",
        " " + title.center(LINE_WIDTH - 1),
        " " + "PUBLIC ISLAMIC BANK BERHAD - IBU".center(LINE_WIDTH - 1),
        " " + "NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION".center(LINE_WIDTH - 1),
        " " + subtitle4.center(LINE_WIDTH - 1),
        "0" + "",
        " " + ("CUSTCD  " if include_custcd else "") + "BRANCH NO/CODE  MONTH      NO OF A/C    NO OF RECEIPT           AMOUNT",
        " " + "-" * (LINE_WIDTH - 1),
    ]

    sort_cols = (["CUSTCD"] if include_custcd else []) + ["BRCH", "MTH"]
    data = df.sort(sort_cols)

    for row in data.iter_rows(named=True):
        cust = f"{cust_label(int(row['CUSTCD'])):<9}" if include_custcd else ""
        lines.append(
            " "
            + f"{cust}{str(row['BRCH']):<15}{mth_label(int(row['MTH'])):<10}"
            + f"{row['FDINO']:>12,.0f}{row['FDINO2']:>16,.0f}{row['FDI']:>18,.2f}"
        )

    total = df.select([
        pl.col("FDI").sum().alias("FDI"),
        pl.col("FDINO").sum().alias("FDINO"),
        pl.col("FDINO2").sum().alias("FDINO2"),
    ]).row(0, named=True)
    lines.extend([
        " " + "-" * (LINE_WIDTH - 1),
        " " + f"{'TOTAL':<34}{total['FDINO']:>12,.0f}{total['FDINO2']:>16,.0f}{total['FDI']:>18,.2f}",
    ])
    return lines


def enforce_asa_paging(lines: Iterable[str]) -> list[str]:
    result: list[str] = []
    line_no = 0
    for raw in lines:
        if raw.startswith("1"):
            line_no = 1
            result.append(raw)
            continue
        if line_no >= PAGE_LENGTH:
            result.append("1")
            line_no = 1
        else:
            line_no += 1
        if not raw or raw[0] not in {" ", "0", "1", "-", "+"}:
            result.append(" " + raw)
        else:
            result.append(raw)
    return result


def main() -> None:
    ctx = load_context()

    # %MACRO CHKRPTDT; IF "&REPTFQ" EQ "Y" THEN %DIBMIS02;
    if ctx.reptfq != "Y":
        REPORT_FILE.write_text("", encoding="utf-8")
        return

    dyibua, dyibua1 = build_base_dataframe(ctx)

    old_todate = agg_todate(dyibua, include_custcd=False)
    new_todate = agg_todate(dyibua, include_custcd=True)

    avg_old = agg_avg(dyibua1, include_custcd=False, days=ctx.edate.day) if dyibua1.height > 0 else pl.DataFrame()
    avg_new = agg_avg(dyibua1, include_custcd=True, days=ctx.edate.day) if dyibua1.height > 0 else pl.DataFrame()

    subtitle4 = f"(19-MAR-10 TO 15-JUN-10) AS AT {ctx.rdate_display}"
    report_lines: list[str] = []
    report_lines.extend(render_section("REPORT ID : DIIMISC7 (OLD)", subtitle4, old_todate, include_custcd=False))
    if avg_old.height > 0:
        report_lines.extend(render_section("REPORT ID : DIIMISC7 (OLD) - DAILY AVERAGE", subtitle4, avg_old, include_custcd=False))
    report_lines.extend(render_section("REPORT ID : DIIMISC7 (NEW)", subtitle4, new_todate, include_custcd=True))
    if avg_new.height > 0:
        report_lines.extend(render_section("REPORT ID : DIIMISC7 (NEW) - DAILY AVERAGE", subtitle4, avg_new, include_custcd=True))

    REPORT_FILE.write_text("\n".join(enforce_asa_paging(report_lines)) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
