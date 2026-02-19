#!/usr/bin/env python3
"""
Program : DIIMISC6
Purpose : REPORT OF NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT PERIOD (16-SEP-08 UNTIL 15-MAR-09)
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import duckdb
import polars as pl

from PBMISFMT import format_brchcd

# *;
# OPTIONS YEARCUTOFF=1950 NOCENTER NODATE MISSING=0 LINESIZE=132;

# =============================================================================
# PATH SETUP (defined early, as requested)
# =============================================================================
BASE_DIR = Path("/data")
INPUT_DIR = BASE_DIR / "input"
DEPOSIT_DIR = INPUT_DIR / "deposit"
MIS_DIR = INPUT_DIR / "mis"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPTDATE_PARQUET = DEPOSIT_DIR / "REPTDATE.parquet"
OUTPUT_REPORT_TXT = OUTPUT_DIR / "DIBMISC6_REPORT.txt"

LINESIZE = 132
PAGE_LENGTH = 60

# %INC PGM(PBMISFMT);
# Dependency reference placeholder:
# from Conv_Py.PBMISFMT import format_brchcd

FDFMT = {i: f"{i} MONTH" if i == 1 else f"{i} MONTHS" for i in range(1, 61)}
CDFMT_RETAIL = {77, 78, 95, 96}

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


def asa_paginate(lines: Iterable[str], page_length: int = PAGE_LENGTH) -> list[str]:
    out: list[str] = []
    line_no = 0
    for raw_line in lines:
        if line_no % page_length == 0:
            cc = "1" if line_no == 0 else "1"
        else:
            cc = " "
        out.append(f"{cc}{raw_line[:LINESIZE - 1]:<{LINESIZE - 1}}")
        line_no += 1
    return out


def fmt_num(value: float | int, decimals: int = 0, width: int = 14) -> str:
    if decimals == 0:
        return f"{value:>{width},.0f}"
    return f"{value:>{width},.{decimals}f}"


def format_rept_date_ddmmyy8(d: date) -> str:
    return d.strftime("%d%m%y")


def load_reptdate() -> tuple[date, str, str]:
    con = duckdb.connect()
    row = con.execute(
        """
        SELECT REPTDATE
        FROM read_parquet(?)
        LIMIT 1
        """,
        [str(REPTDATE_PARQUET)],
    ).fetchone()
    if row is None:
        raise ValueError("REPTDATE source is empty")

    rpt = row[0]
    if isinstance(rpt, datetime):
        rpt_date = rpt.date()
    elif isinstance(rpt, date):
        rpt_date = rpt
    else:
        rpt_date = datetime.strptime(str(rpt), "%Y-%m-%d").date()

    reptmon = f"{rpt_date.month:02d}"
    rdate = format_rept_date_ddmmyy8(rpt_date)
    return rpt_date, reptmon, rdate


def prepare_source(reptmon: str, sdate: date, edate: date) -> tuple[pl.DataFrame, pl.DataFrame]:
    src_file = MIS_DIR / f"DYIBUZ{reptmon}.parquet"
    con = duckdb.connect()
    df = pl.from_arrow(con.execute("SELECT * FROM read_parquet(?)", [str(src_file)]).arrow())

    df = df.with_columns([
        pl.col("BRANCH").map_elements(format_brchcd, return_dtype=pl.String).alias("BRCHCD"),
        (pl.col("BRANCH").cast(pl.Int64).cast(pl.String).str.zfill(3) + pl.lit("/") + pl.col("BRCHCD")).alias("BRCH"),
        pl.col("INTPLAN").replace(INTPLAN_TO_MTH, default=0).cast(pl.Int64).alias("MTH"),
        pl.when(pl.col("CUSTCD").is_in(list(CDFMT_RETAIL))).then(pl.lit("RETAIL")).otherwise(pl.lit("CORPORATE")).alias("CUSTSEG"),
    ])

    edate_df = df.filter(pl.col("REPTDATE").cast(pl.Date) == pl.lit(edate))
    range_df = df.filter((pl.col("REPTDATE").cast(pl.Date) >= pl.lit(sdate)) & (pl.col("REPTDATE").cast(pl.Date) <= pl.lit(edate)))
    return edate_df, range_df


def summarize(df: pl.DataFrame, keys: list[str], days_divisor: int | None = None) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame(schema={k: pl.String if k in ("BRCH", "CUSTSEG") else pl.Int64 for k in keys} | {"FDI": pl.Float64, "FDINO": pl.Float64, "FDINO2": pl.Float64})

    out = df.group_by(keys).agg([
        pl.col("FDI").sum().alias("FDI"),
        pl.col("FDINO").sum().alias("FDINO"),
        pl.col("FDINO2").sum().alias("FDINO2"),
    ])
    if days_divisor and days_divisor > 0:
        out = out.with_columns([
            (pl.col("FDI") / days_divisor).alias("FDI"),
            (pl.col("FDINO") / days_divisor).alias("FDINO"),
            (pl.col("FDINO2") / days_divisor).alias("FDINO2"),
        ])
    return out.sort(keys)


def render_section(title_lines: list[str], data: pl.DataFrame, include_custseg: bool, metric_label: str) -> list[str]:
    lines: list[str] = []
    lines.extend(title_lines)
    lines.append("")
    if include_custseg:
        lines.append(f"{'SEGMENT':<10} {'BRANCH NO/CODE':<16} {'MONTH':<10} {'NO OF A/C':>12} {'NO OF RECEIPT':>14} {'AMOUNT':>18}")
        sort_cols = ["CUSTSEG", "BRCH", "MTH"]
    else:
        lines.append(f"{'BRANCH NO/CODE':<16} {'MONTH':<10} {'NO OF A/C':>12} {'NO OF RECEIPT':>14} {'AMOUNT':>18}")
        sort_cols = ["BRCH", "MTH"]

    if data.is_empty():
        lines.append(f"{'':<16} NO DATA AVAILABLE")
        lines.append("")
        return lines

    data = data.sort(sort_cols)
    total_fdino = float(data["FDINO"].sum())
    total_fdino2 = float(data["FDINO2"].sum())
    total_fdi = float(data["FDI"].sum())

    for row in data.iter_rows(named=True):
        mth_lbl = FDFMT.get(int(row["MTH"]), "")
        if include_custseg:
            lines.append(
                f"{row['CUSTSEG']:<10} {row['BRCH']:<16} {mth_lbl:<10} "
                f"{fmt_num(row['FDINO'], 0, 12)} {fmt_num(row['FDINO2'], 0, 14)} {fmt_num(row['FDI'], 2, 18)}"
            )
        else:
            lines.append(
                f"{row['BRCH']:<16} {mth_lbl:<10} "
                f"{fmt_num(row['FDINO'], 0, 12)} {fmt_num(row['FDINO2'], 0, 14)} {fmt_num(row['FDI'], 2, 18)}"
            )

    lines.append("-" * 90)
    lines.append(
        f"{'TOTAL':<28} {fmt_num(total_fdino, 0, 12)} {fmt_num(total_fdino2, 0, 14)} {fmt_num(total_fdi, 2, 18)}"
    )
    lines.append(f"{metric_label}")
    lines.append("")
    return lines


def main() -> None:
    rptdate, reptmon, rdate = load_reptdate()

    # DATA _NULL_ equivalent for reporting period
    sdate = date(rptdate.year, rptdate.month, 1)
    edate = rptdate

    dyibua, dyibua1 = prepare_source(reptmon=reptmon, sdate=sdate, edate=edate)

    old_td = summarize(dyibua, keys=["BRCH", "MTH"])
    old_avg = summarize(dyibua1, keys=["BRCH", "MTH"], days_divisor=edate.day) if dyibua1.height > 0 else pl.DataFrame()

    new_td = summarize(dyibua, keys=["CUSTSEG", "BRCH", "MTH"])
    new_avg = summarize(dyibua1, keys=["CUSTSEG", "BRCH", "MTH"], days_divisor=edate.day) if dyibua1.height > 0 else pl.DataFrame()

    report_lines: list[str] = []

    report_lines.extend(render_section(
        [
            "REPORT ID : DIBMISC6 (OLD)",
            "PUBLIC ISLAMIC BANK BERHAD - IBU",
            "NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION",
            f"(16-SEP-08 UNTIL 15-MAR-09) AS AT {rdate}",
        ],
        old_td,
        include_custseg=False,
        metric_label="TODATE BALANCE",
    ))

    if dyibua1.height > 0:
        report_lines.extend(render_section(
            [
                "REPORT ID : DIBMISC6 (OLD)",
                "PUBLIC ISLAMIC BANK BERHAD - IBU",
                "NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION",
                f"(16-SEP-08 UNTIL 15-MAR-09) AS AT {rdate}",
            ],
            old_avg,
            include_custseg=False,
            metric_label="DAILY AVERAGE",
        ))

    report_lines.extend(render_section(
        [
            "REPORT ID : DIBMISC6 (NEW)",
            "PUBLIC ISLAMIC BANK BERHAD - IBU",
            "NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION",
            f"(16-SEP-08 UNTIL 15-MAR-09) AS AT {rdate}",
        ],
        new_td,
        include_custseg=True,
        metric_label="TODATE BALANCE",
    ))

    if dyibua1.height > 0:
        report_lines.extend(render_section(
            [
                "REPORT ID : DIBMISC6 (NEW)",
                "PUBLIC ISLAMIC BANK BERHAD - IBU",
                "NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION",
                f"(16-SEP-08 UNTIL 15-MAR-09) AS AT {rdate}",
            ],
            new_avg,
            include_custseg=True,
            metric_label="DAILY AVERAGE",
        ))

    asa_lines = asa_paginate(report_lines, page_length=PAGE_LENGTH)
    OUTPUT_REPORT_TXT.write_text("\n".join(asa_lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
