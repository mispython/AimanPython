# !/usr/bin/env python3
"""
Program : DIIMISC3
Purpose : New PBB & PFB Al-Mudharabah FD account after privatisation
            (16 Apr 2006 onwards) for profit sharing ratio calculation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb
import polars as pl

from PBMISFMT import format_brchcd

# ---------------------------------------------------------------------------
# Path setup (defined early)
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_DIR = BASE_DIR / "input"
DEPOSIT_DIR = INPUT_DIR / "deposit"
MIS_DIR = INPUT_DIR / "mis"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
REPORT_FILE = OUTPUT_DIR / "DIIMISC3_report.txt"

REPTDATE_PARQUET = DEPOSIT_DIR / "REPTDATE.parquet"
DYIBUN_FILE_TEMPLATE = "DYIBUN{reptmon}.parquet"

# %INC PGM(PBMISFMT);
# Dependency reference kept from SAS conversion.

# PROC FORMAT VALUE CDFMT 77,78,95,96='RETAIL' OTHER='CORPORATE';
RETAIL_CODES = {77, 78, 95, 96}

# SELECT (INTPLAN) ... WHEN(...) MTH=..; OTHERWISE MTH=0;
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


@dataclass
class ReportWriter:
    path: Path
    page_len: int = 60

    def __post_init__(self) -> None:
        self._lines: list[str] = []
        self._line_no = 0

    def write(self, text: str = "", ctl: str = " ") -> None:
        if ctl == "1":
            self._line_no = 0
        self._lines.append(f"{ctl}{text}")
        self._line_no += 1

    def flush(self) -> None:
        self.path.write_text("\n".join(self._lines) + "\n", encoding="utf-8")


def _to_date(value: object) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return datetime.fromisoformat(value).date()
    raise ValueError(f"Unsupported date value: {value!r}")


def _read_parquet(path: Path) -> pl.DataFrame:
    return duckdb.sql(f"SELECT * FROM read_parquet('{path.as_posix()}')").pl()


def _fmt_mth(mth: int) -> str:
    return f"{mth} MONTH" if mth == 1 else f"{mth} MONTHS"


def _fmt_custcd(custcd: int | None) -> str:
    if custcd is None:
        return ""
    return "RETAIL" if int(custcd) in RETAIL_CODES else "CORPORATE"


def _print_summary(
    wr: ReportWriter,
    data: pl.DataFrame,
    title: str,
    rpt_date: date,
    include_cust: bool,
    metric_label: str,
) -> None:
    group_cols = ["BRCH", "MTH"] + (["CUSTCD"] if include_cust else [])
    agg = (
        data.group_by(group_cols)
        .agg(pl.sum("FDI"), pl.sum("FDINO"), pl.sum("FDINO2"))
        .sort(group_cols)
    )

    wr.write(f"REPORT ID : {title}", ctl="1")
    wr.write("PUBLIC ISLAMIC BANK BERHAD - IBU")
    wr.write("PBB AL-MUDHARABAH FD A/C AFTER PRIVATISATION")
    wr.write(f"(16-APR-06 UNTIL 15-SEP-08) AS AT {rpt_date.strftime('%d%m%Y')}")
    wr.write("")
    wr.write(f"{metric_label}")
    if include_cust:
        wr.write("CUST TYPE  BRANCH      TENURE      NO OF A/C   NO OF RECEIPT          AMOUNT")
    else:
        wr.write("BRANCH      TENURE      NO OF A/C   NO OF RECEIPT          AMOUNT")

    for rec in agg.iter_rows(named=True):
        brch = str(rec["BRCH"])
        mth = int(rec["MTH"]) if rec["MTH"] is not None else 0
        fdino = int(rec["FDINO"] or 0)
        fdino2 = int(rec["FDINO2"] or 0)
        fdi = float(rec["FDI"] or 0.0)
        if include_cust:
            cust_lbl = _fmt_custcd(rec.get("CUSTCD"))
            wr.write(
                f"{cust_lbl:<10} {brch:<10} {_fmt_mth(mth):<10} {fdino:>10,} {fdino2:>14,} {fdi:>16,.2f}"
            )
        else:
            wr.write(f"{brch:<10} {_fmt_mth(mth):<10} {fdino:>10,} {fdino2:>14,} {fdi:>16,.2f}")

    total_fdino = int(agg["FDINO"].sum() or 0)
    total_fdino2 = int(agg["FDINO2"].sum() or 0)
    total_fdi = float(agg["FDI"].sum() or 0.0)
    wr.write("-" * 78)
    wr.write(f"TOTAL{'':<27} {total_fdino:>10,} {total_fdino2:>14,} {total_fdi:>16,.2f}")


def main() -> None:
    reptdate_df = _read_parquet(REPTDATE_PARQUET)
    if reptdate_df.is_empty():
        raise ValueError("DEPOSIT.REPTDATE parquet has no rows.")

    reptdate = _to_date(reptdate_df[-1, "REPTDATE"])
    reptfq = "Y"

    # %IF "&REPTFQ" EQ "Y" %THEN %DO; ...
    if reptfq != "Y":
        return

    reptmon = f"{reptdate.month:02d}"
    sdate = reptdate.replace(day=1)
    edate = reptdate

    dyibun_path = MIS_DIR / DYIBUN_FILE_TEMPLATE.format(reptmon=reptmon)
    dyibun = _read_parquet(dyibun_path)

    dyibun = dyibun.with_columns(
        pl.col("BRANCH").map_elements(lambda x: format_brchcd(int(x) if x is not None else None), return_dtype=pl.String).alias("BRCHCD"),
        pl.col("INTPLAN").map_elements(lambda x: INTPLAN_TO_MTH.get(int(x), 0), return_dtype=pl.Int64).alias("MTH"),
        pl.col("REPTDATE").cast(pl.Date),
    ).with_columns(
        (pl.col("BRANCH").cast(pl.Int64).cast(pl.String).str.zfill(3) + pl.lit("/") + pl.col("BRCHCD")).alias("BRCH")
    )

    dyibua = dyibun.filter(pl.col("REPTDATE") == pl.lit(edate))
    dyibua1 = dyibun.filter((pl.col("REPTDATE") >= pl.lit(sdate)) & (pl.col("REPTDATE") <= pl.lit(edate)))

    wr = ReportWriter(REPORT_FILE)

    _print_summary(wr, dyibua, "DIIMISC3 (OLD)", reptdate, include_cust=False, metric_label="TODATE BALANCE")

    n = dyibua1.height
    if n != 0:
        days = edate.day
        dyibua1_avg_old = dyibua1.with_columns(
            (pl.col("FDI") / days).alias("FDI"),
            (pl.col("FDINO") / days).alias("FDINO"),
            (pl.col("FDINO2") / days).alias("FDINO2"),
        )
        _print_summary(
            wr,
            dyibua1_avg_old,
            "DIIMISC3 (OLD)",
            reptdate,
            include_cust=False,
            metric_label="DAILY AVERAGE",
        )

    _print_summary(wr, dyibua, "DIIMISC3 (NEW)", reptdate, include_cust=True, metric_label="TODATE BALANCE")

    if n != 0:
        days = edate.day
        dyibua1_avg_new = dyibua1.with_columns(
            (pl.col("FDI") / days).alias("FDI"),
            (pl.col("FDINO") / days).alias("FDINO"),
            (pl.col("FDINO2") / days).alias("FDINO2"),
        )
        _print_summary(
            wr,
            dyibua1_avg_new,
            "DIIMISC3 (NEW)",
            reptdate,
            include_cust=True,
            metric_label="DAILY AVERAGE",
        )

    wr.flush()


if __name__ == "__main__":
    main()
