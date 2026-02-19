# !/usr/bin/env python3
"""
Program : DIIMISC1
Purpose : PBB OLD AL-MUDHARABAH FD ACCOUNT PRIOR PRIVATISATION
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import duckdb
import polars as pl

from PBMISFMT import format_brchcd

# *+--------------------------------------------------------------+
#  |  PROGRAM : DIIMISC1                                          |
#  |  DATE    : 31.03.09                                          |
#  |  REPORT  : PBB OLD AL-MUDHARABAH FD ACCOUNT                  |
#  |            PRIOR PRIVATISATION                               |
#  +--------------------------------------------------------------+

# ---------------------------------------------------------------------------
# PATH SETUP (early, explicit, centralized)
# ---------------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIRS = {
    "DEPOSIT": ROOT_DIR / "data" / "DEPOSIT",
    "MIS": ROOT_DIR / "data" / "MIS",
}
OUTPUT_DIR = ROOT_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
REPORT_PATH = OUTPUT_DIR / "DIIMISC1_report.txt"

PARQUET_FILES = {
    "REPTDATE": DATA_DIRS["DEPOSIT"] / "REPTDATE.parquet",
}

# If your runtime uses different storage roots, update only DATA_DIRS / PARQUET_FILES.

# %INC PGM(PBMISFMT);
# Placeholder dependency: PBMISFMT.py

INTPLAN_TO_MTH: dict[int, int] = {
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


def month_label(m: int) -> str:
    return f"{m} MONTH" if m == 1 else f"{m} MONTHS"


def custcd_label(v: int) -> str:
    return "RETAIL" if v in (77, 78, 95, 96) else "CORPORATE"


def asa_line(text: str, cc: str = " ") -> str:
    return f"{cc}{text.rstrip()}\n"


@dataclass
class ReportContext:
    reptdate: date
    reptmon: str
    rdate_text: str
    sdate: date
    edate: date


def read_context(con: duckdb.DuckDBPyConnection) -> ReportContext:
    rept_df = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{PARQUET_FILES['REPTDATE'].as_posix()}') ORDER BY REPTDATE DESC LIMIT 1"
    ).pl()
    if rept_df.height == 0:
        raise ValueError("REPTDATE source is empty.")
    rpt = rept_df.item(0, 0)
    if isinstance(rpt, datetime):
        rpt_date = rpt.date()
    elif isinstance(rpt, date):
        rpt_date = rpt
    else:
        rpt_date = datetime.strptime(str(rpt), "%Y-%m-%d").date()

    return ReportContext(
        reptdate=rpt_date,
        reptmon=f"{rpt_date.month:02d}",
        rdate_text=rpt_date.strftime("%d%m%y"),
        sdate=rpt_date.replace(day=1),
        edate=rpt_date,
    )


def build_base_df(con: duckdb.DuckDBPyConnection, ctx: ReportContext) -> pl.DataFrame:
    source = DATA_DIRS["MIS"] / f"DYIBUB{ctx.reptmon}.parquet"
    df = con.execute(f"SELECT * FROM read_parquet('{source.as_posix()}')").pl()

    df = df.with_columns([
        pl.col("REPTDATE").cast(pl.Date),
        pl.col("BRANCH").cast(pl.Int64),
        pl.col("INTPLAN").cast(pl.Int64),
        pl.col("FDI").cast(pl.Float64),
        pl.col("FDINO").cast(pl.Float64),
        pl.col("FDINO2").cast(pl.Float64),
        pl.col("CUSTCD").cast(pl.Int64),
    ])

    df = df.with_columns([
        pl.col("INTPLAN").replace(INTPLAN_TO_MTH, default=0).alias("MTH"),
        pl.col("BRANCH").map_elements(format_brchcd, return_dtype=pl.String).alias("BRCHCD"),
    ]).with_columns([
        pl.format("{0:03d}/{1}", pl.col("BRANCH"), pl.col("BRCHCD")).alias("BRCH")
    ])
    return df


def summarize(df: pl.DataFrame, by: list[str]) -> pl.DataFrame:
    if df.height == 0:
        return pl.DataFrame(schema={**{c: pl.Utf8 for c in by}, "FDI": pl.Float64, "FDINO": pl.Float64, "FDINO2": pl.Float64})
    return (
        df.group_by(by)
        .agg([
            pl.col("FDI").sum(),
            pl.col("FDINO").sum(),
            pl.col("FDINO2").sum(),
        ])
        .sort(by)
    )


def emit_section(lines: list[str], title1: str, title2: str, title3: str, title4: str, rows: Iterable[str], new_page: bool) -> None:
    cc = "1" if new_page else " "
    lines.append(asa_line(title1, cc))
    lines.append(asa_line(title2))
    lines.append(asa_line(title3))
    lines.append(asa_line(title4))
    lines.append(asa_line("-" * 131))
    lines.append(asa_line("CATEGORY   BRANCH       TENOR       NO OF A/C  NO OF RECEIPT          AMOUNT"))
    lines.append(asa_line("-" * 131))
    lines.extend(rows)
    lines.append(asa_line(""))


def render_rows(df: pl.DataFrame, include_cust: bool) -> list[str]:
    out: list[str] = []
    for r in df.iter_rows(named=True):
        cat = custcd_label(int(r["CUSTCD"])) if include_cust else "OLD"
        out.append(
            asa_line(
                f"{cat:<10} {str(r['BRCH']):<11} {month_label(int(r['MTH'])):<10} "
                f"{r['FDINO']:>10,.0f} {r['FDINO2']:>14,.0f} {r['FDI']:>15,.2f}"
            )
        )
    if df.height > 0:
        out.append(
            asa_line(
                f"{'TOTAL':<10} {'ALL':<11} {'ALL':<10} "
                f"{df['FDINO'].sum():>10,.0f} {df['FDINO2'].sum():>14,.0f} {df['FDI'].sum():>15,.2f}"
            )
        )
    return out


def main() -> None:
    con = duckdb.connect()
    ctx = read_context(con)

    # %IF "&REPTFQ" EQ "Y" %THEN ...;  (from SAS CHKRPTDT)
    reptfq = "Y"
    if reptfq != "Y":
        return

    base_df = build_base_df(con, ctx)
    dyibua = base_df.filter(pl.col("REPTDATE") == pl.lit(ctx.edate))
    dyibua1 = base_df.filter((pl.col("REPTDATE") >= pl.lit(ctx.sdate)) & (pl.col("REPTDATE") <= pl.lit(ctx.edate)))

    td_old = summarize(dyibua.select(["BRCH", "MTH", "FDI", "FDINO", "FDINO2"]), ["BRCH", "MTH"])
    td_new = summarize(dyibua.select(["CUSTCD", "BRCH", "MTH", "FDI", "FDINO", "FDINO2"]), ["CUSTCD", "BRCH", "MTH"])

    n = dyibua1.height
    avg_old = pl.DataFrame()
    avg_new = pl.DataFrame()
    if n != 0:
        days = ctx.edate.day
        avg_old = summarize(dyibua1.select(["BRCH", "MTH", "FDI", "FDINO", "FDINO2"]), ["BRCH", "MTH"]).with_columns(
            [(pl.col(c) / days).alias(c) for c in ["FDI", "FDINO", "FDINO2"]]
        )
        avg_new = summarize(dyibua1.select(["CUSTCD", "BRCH", "MTH", "FDI", "FDINO", "FDINO2"]), ["CUSTCD", "BRCH", "MTH"]).with_columns(
            [(pl.col(c) / days).alias(c) for c in ["FDI", "FDINO", "FDINO2"]]
        )

    lines: list[str] = []
    emit_section(
        lines,
        "REPORT ID : DIIMISC1 (OLD)",
        "PUBLIC ISLAMIC BANK BERHAD - IBU",
        "PBB OLD AL-MUDHARABAH FD ACCOUNT PRIOR PRIVATISATION",
        f"AS AT {ctx.rdate_text}",
        render_rows(td_old, include_cust=False),
        new_page=True,
    )

    if avg_old.height > 0:
        emit_section(
            lines,
            "REPORT ID : DIIMISC1 (OLD) - DAILY AVERAGE",
            "PUBLIC ISLAMIC BANK BERHAD - IBU",
            "PBB OLD AL-MUDHARABAH FD ACCOUNT PRIOR PRIVATISATION",
            f"AS AT {ctx.rdate_text}",
            render_rows(avg_old, include_cust=False),
            new_page=False,
        )

    emit_section(
        lines,
        "REPORT ID : DIIMISC1 (NEW)",
        "PUBLIC ISLAMIC BANK BERHAD - IBU",
        "PBB OLD AL-MUDHARABAH FD ACCOUNT PRIOR PRIVATISATION",
        f"AS AT {ctx.rdate_text}",
        render_rows(td_new, include_cust=True),
        new_page=True,
    )

    if avg_new.height > 0:
        emit_section(
            lines,
            "REPORT ID : DIIMISC1 (NEW) - DAILY AVERAGE",
            "PUBLIC ISLAMIC BANK BERHAD - IBU",
            "PBB OLD AL-MUDHARABAH FD ACCOUNT PRIOR PRIVATISATION",
            f"AS AT {ctx.rdate_text}",
            render_rows(avg_new, include_cust=True),
            new_page=False,
        )

    REPORT_PATH.write_text("".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()

