#!/usr/bin/env python3
"""
Program: DIIMISC5
Purpose: NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT PERIOD 16-MAR-09 UNTIL 18-MAR-10.
"""

# +--------------------------------------------------------------+
# |  PROGRAM : DIIMISC5                                          |
# |  DATE    : 31.03.09                                          |
# |  SMR     : 2009-00000416                                     |
# |  REPORT  : NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT            |
# |            PERIOD 16-MAR-09 UNTIL 18-MAR-10                  |
# +--------------------------------------------------------------+
# %INC PGM(PBMISFMT);

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import duckdb
import polars as pl

from PBMISFMT import format_brchcd


# ==============================
# PATH SETUP (EARLY)
# ==============================
BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
DEPOSIT_DIR = INPUT_DIR / "deposit"
MIS_DIR = INPUT_DIR / "mis"

REPTDATE_PARQUET = DEPOSIT_DIR / "REPTDATE.parquet"
OUTPUT_REPORT = OUTPUT_DIR / "DIIMISC5.txt"

ASA_SPACE = " "
ASA_NEW_PAGE = "1"
LINES_PER_PAGE = 60

RETAIL_CODES = {77, 78, 95, 96}
FDFMT = {i: f"{i} MONTH" + ("" if i == 1 else "S") for i in range(1, 61)}

INTPLAN_TO_MTH: dict[int, int] = {}
for month, plans in {
    1: (340, 448, 660, 720), 2: (352, 449, 661, 721), 3: (341, 450, 662, 722),
    4: (353, 451, 663, 723), 5: (354, 452, 664, 724), 6: (342, 453, 665, 725),
    7: (355, 454, 666, 726), 8: (356, 455, 667, 727), 9: (343, 456, 668, 728),
    10: (357, 457, 669, 729), 11: (358, 458, 670, 730), 12: (344, 459, 671, 731),
    13: (588, 461, 672, 732), 14: (589, 462, 673, 733), 15: (345, 463, 674, 734),
    16: (590, 675), 17: (591, 676), 18: (346, 464, 677, 735), 19: (592, 678),
    20: (593, 679), 21: (347, 465, 680, 736), 22: (594, 681), 23: (595, 682),
    24: (348, 466, 683, 737), 25: (596, 684), 26: (597, 685), 27: (359, 686),
    28: (598, 687), 29: (599, 688), 30: (540, 580, 689), 31: (690,), 32: (691,),
    33: (541, 581, 692), 34: (693,), 35: (694,), 36: (349, 467, 695, 738),
    37: (696,), 38: (697,), 39: (542, 582, 698), 40: (699,), 41: (700,),
    42: (543, 583, 701), 43: (702,), 44: (703,), 45: (544, 584, 704), 46: (705,),
    47: (706,), 48: (350, 468, 707, 739), 49: (708,), 50: (709,), 51: (545, 585, 710),
    52: (711,), 53: (712,), 54: (546, 586, 713), 55: (714,), 56: (715,),
    57: (547, 587, 716), 58: (717,), 59: (718,), 60: (351, 719, 740),
}.items():
    for plan in plans:
        INTPLAN_TO_MTH[plan] = month


@dataclass
class ReportContext:
    reptdate: date
    rdate_ddmmyy: str
    sdate: date
    edate: date
    reptmon: str


def _fmt_num(value: float, decimals: int = 2, width: int = 14) -> str:
    return f"{value:>{width},.{decimals}f}"


def _fmt_intlike(value: float, width: int = 12) -> str:
    return f"{value:>{width},.0f}"


def _detect_reptdate() -> ReportContext:
    con = duckdb.connect()
    df = con.execute(
        "SELECT REPTDATE FROM read_parquet(?) ORDER BY REPTDATE DESC LIMIT 1",
        [str(REPTDATE_PARQUET)],
    ).pl()
    if df.height == 0:
        raise ValueError(f"No REPTDATE rows in {REPTDATE_PARQUET}")

    rept_dt = df.item(0, 0)
    if isinstance(rept_dt, datetime):
        rept_dt = rept_dt.date()
    if not isinstance(rept_dt, date):
        rept_dt = datetime.strptime(str(rept_dt)[:10], "%Y-%m-%d").date()

    sdate = rept_dt.replace(day=1)
    return ReportContext(
        reptdate=rept_dt,
        rdate_ddmmyy=rept_dt.strftime("%d%m%y"),
        sdate=sdate,
        edate=rept_dt,
        reptmon=f"{rept_dt.month:02d}",
    )


def _load_mis(ctx: ReportContext) -> tuple[pl.DataFrame, pl.DataFrame]:
    mis_path = MIS_DIR / f"DYIBUX{ctx.reptmon}.parquet"
    if not mis_path.exists():
        raise FileNotFoundError(f"Missing monthly MIS parquet: {mis_path}")

    con = duckdb.connect()
    src = con.execute("SELECT * FROM read_parquet(?)", [str(mis_path)]).pl()

    work = src.with_columns(
        pl.col("BRANCH").cast(pl.Int64).map_elements(format_brchcd, return_dtype=pl.Utf8).alias("BRCHCD"),
        pl.col("BRANCH").cast(pl.Int64).map_elements(lambda x: f"{x:03d}", return_dtype=pl.Utf8).alias("BR3"),
        pl.col("INTPLAN").cast(pl.Int64).map_elements(lambda x: INTPLAN_TO_MTH.get(x, 0), return_dtype=pl.Int64).alias("MTH"),
        pl.col("REPTDATE").cast(pl.Date),
    ).with_columns(
        (pl.col("BR3") + pl.lit("/") + pl.col("BRCHCD")).alias("BRCH")
    )

    dyibua = work.filter(pl.col("REPTDATE") == pl.lit(ctx.edate))
    dyibua1 = work.filter((pl.col("REPTDATE") >= pl.lit(ctx.sdate)) & (pl.col("REPTDATE") <= pl.lit(ctx.edate)))
    return dyibua, dyibua1


def _summarize(df: pl.DataFrame, keys: list[str]) -> pl.DataFrame:
    if df.height == 0:
        return pl.DataFrame(schema={**{k: pl.Utf8 for k in keys}, "MTH": pl.Int64, "FDI": pl.Float64, "FDINO": pl.Float64, "FDINO2": pl.Float64})
    return df.group_by(keys + ["MTH"]).agg(
        pl.col("FDI").sum().alias("FDI"),
        pl.col("FDINO").sum().alias("FDINO"),
        pl.col("FDINO2").sum().alias("FDINO2"),
    )


def _write_lines_with_paging(lines: Iterable[str], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="\n") as f:
        line_count = 0
        for line in lines:
            cc = ASA_SPACE
            if line_count == 0 or line_count >= LINES_PER_PAGE:
                cc = ASA_NEW_PAGE
                line_count = 0
            f.write(f"{cc}{line}\n")
            line_count += 1


def _make_report_block(df: pl.DataFrame, title1: str, title4: str, by_custcd: bool = False, days: int = 1) -> list[str]:
    lines: list[str] = [
        title1,
        "PUBLIC ISLAMIC BANK BERHAD - IBU",
        "NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION",
        title4,
        "",
    ]

    keys = ["BRCH"] if not by_custcd else ["CUSTCD", "BRCH"]
    sdf = _summarize(df, keys)

    if days > 1:
        sdf = sdf.with_columns((pl.col("FDI") / days).alias("FDI"), (pl.col("FDINO") / days).alias("FDINO"), (pl.col("FDINO2") / days).alias("FDINO2"))

    if sdf.height == 0:
        lines.append("NO DATA AVAILABLE")
        lines.append("")
        return lines

    month_cols = list(range(1, 61))
    header = f"{'BRANCH NO/CODE':<14} {'NO OF A/C':>12} {'NO OF RECEIPT':>14} " + " ".join(f"{m:02d}" for m in month_cols) + " TOTAL"
    lines.append(header)
    lines.append("-" * len(header))

    if by_custcd:
        for custcd in sorted(sdf["CUSTCD"].unique().to_list()):
            cust_df = sdf.filter(pl.col("CUSTCD") == custcd)
            cust_lbl = "RETAIL" if int(custcd) in RETAIL_CODES else "CORPORATE"
            lines.append(f"CUSTOMER TYPE {custcd} ({cust_lbl})")
            lines.extend(_render_branch_lines(cust_df, include_grand_total=True))
            lines.append("")
    else:
        lines.extend(_render_branch_lines(sdf, include_grand_total=True))
        lines.append("")

    return lines


def _render_branch_lines(df: pl.DataFrame, include_grand_total: bool) -> list[str]:
    lines: list[str] = []
    branches = sorted(df["BRCH"].unique().to_list())
    for brch in branches:
        bdf = df.filter(pl.col("BRCH") == brch)
        fdino = bdf["FDINO"].sum()
        fdino2 = bdf["FDINO2"].sum()
        mvals = {int(r[0]): float(r[1]) for r in bdf.select(["MTH", "FDI"]).iter_rows()}
        total_fdi = sum(mvals.values())
        month_part = " ".join(_fmt_num(mvals.get(m, 0.0), 2, 8) for m in range(1, 61))
        lines.append(f"{brch:<14} {_fmt_intlike(fdino)} {_fmt_intlike(fdino2, 14)} {month_part} {_fmt_num(total_fdi, 2, 12)}")

    if include_grand_total:
        fdino = df["FDINO"].sum()
        fdino2 = df["FDINO2"].sum()
        mvals = {m: 0.0 for m in range(1, 61)}
        for m, fdi in df.select(["MTH", "FDI"]).iter_rows():
            mvals[int(m)] += float(fdi)
        total_fdi = sum(mvals.values())
        month_part = " ".join(_fmt_num(mvals.get(m, 0.0), 2, 8) for m in range(1, 61))
        lines.append(f"{'TOTAL':<14} {_fmt_intlike(fdino)} {_fmt_intlike(fdino2, 14)} {month_part} {_fmt_num(total_fdi, 2, 12)}")
    return lines


def main() -> None:
    ctx = _detect_reptdate()
    dyibua, dyibua1 = _load_mis(ctx)

    title4 = f"(16-MAR-09 UNTIL 18-MAR-10) AS AT {ctx.rdate_ddmmyy}"

    report_lines: list[str] = []
    report_lines.extend(_make_report_block(dyibua, "REPORT ID : DIIMISC5 (OLD)", title4, by_custcd=False, days=1))

    n = dyibua1.height
    if n != 0:
        report_lines.extend(_make_report_block(dyibua1, "REPORT ID : DIIMISC5 (OLD) - DAILY AVERAGE", title4, by_custcd=False, days=ctx.edate.day))

    report_lines.extend(_make_report_block(dyibua, "REPORT ID : DIIMISC5 (NEW)", title4, by_custcd=True, days=1))

    if n != 0:
        report_lines.extend(_make_report_block(dyibua1, "REPORT ID : DIIMISC5 (NEW) - DAILY AVERAGE", title4, by_custcd=True, days=ctx.edate.day))

    _write_lines_with_paging(report_lines, OUTPUT_REPORT)


if __name__ == "__main__":
    main()
