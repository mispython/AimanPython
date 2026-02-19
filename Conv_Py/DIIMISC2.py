# !/usr/bin/env python3
"""
Program : DIIMISC2
Purpose : NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
          (04-SEP-04 UNTIL 15-APR-06)
"""

import os
from datetime import date, datetime, timedelta

import duckdb
import polars as pl

from PBMISFMT import format_brchcd

# %INC PGM(PBMISFMT);  # SAS dependency retained as comment placeholder.

# =============================================================================
# PATH SETUP
# =============================================================================
BASE_DIR = "/data"
DEPOSIT_DIR = os.path.join(BASE_DIR, "deposit")
MIS_DIR = os.path.join(BASE_DIR, "mis")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

REPTDATE_PATH = os.path.join(DEPOSIT_DIR, "REPTDATE.parquet")
OUTPUT_REPORT = os.path.join(OUTPUT_DIR, "DIIMISC2.REPT.txt")
PAGE_LENGTH = 60

# =============================================================================
# SAS FORMAT EQUIVALENTS
# =============================================================================
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


class AsaReportWriter:
    def __init__(self, path: str, page_length: int = 60):
        self.path = path
        self.page_length = page_length
        self.lines: list[tuple[str, str]] = []

    def put(self, cc: str, text: str = "") -> None:
        self.lines.append((cc, text))

    def save(self) -> None:
        line_in_page = 0
        with open(self.path, "w", encoding="utf-8") as f:
            for cc, text in self.lines:
                if cc == "1" and line_in_page > 0:
                    while line_in_page < self.page_length:
                        f.write(" \n")
                        line_in_page += 1
                    line_in_page = 0
                f.write(f"{cc}{text}\n")
                line_in_page += 1
                if line_in_page >= self.page_length:
                    line_in_page = 0


def to_date(value) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, (int, float)):
        return date(1960, 1, 1) + timedelta(days=int(value))
    return datetime.fromisoformat(str(value)).date()


def format_num(v: float, dec: int = 2, w: int = 14) -> str:
    return f"{(v or 0):>{w},.{dec}f}" if dec > 0 else f"{int(v or 0):>{w},}"


def prepare_data() -> tuple[pl.DataFrame, pl.DataFrame, str, int]:
    con = duckdb.connect()

    rept_row = con.execute(f"SELECT REPTDATE FROM read_parquet('{REPTDATE_PATH}') LIMIT 1").fetchone()
    reptdate = to_date(rept_row[0])
    reptmon = f"{reptdate.month:02d}"
    rdate = reptdate.strftime("%d%m%y")
    sdate = reptdate.replace(day=1)
    edate = reptdate

    dyibua_path = os.path.join(MIS_DIR, f"DYIBUA{reptmon}.parquet")
    df = con.execute(f"SELECT * FROM read_parquet('{dyibua_path}')").pl()

    df = df.with_columns([
        pl.col("BRANCH").map_elements(format_brchcd, return_dtype=pl.String).alias("BRCHCD"),
        pl.col("BRANCH").cast(pl.Int64).map_elements(lambda x: f"{x:03d}", return_dtype=pl.String).alias("BRANCH3"),
        pl.col("INTPLAN").cast(pl.Int64).map_elements(lambda x: INTPLAN_TO_MTH.get(x, 0), return_dtype=pl.Int64).alias("MTH"),
        pl.col("CUSTCD").cast(pl.Int64).map_elements(lambda x: "RETAIL" if x in CDFMT_RETAIL else "CORPORATE", return_dtype=pl.String).alias("CUSTTXT"),
        pl.col("REPTDATE").cast(pl.Date),
    ]).with_columns([
        (pl.col("BRANCH3") + pl.lit("/") + pl.col("BRCHCD")).alias("BRCH")
    ])

    dyibua = df.filter(pl.col("REPTDATE") == pl.lit(edate))
    dyibua1 = df.filter((pl.col("REPTDATE") >= pl.lit(sdate)) & (pl.col("REPTDATE") <= pl.lit(edate)))

    return dyibua, dyibua1, rdate, edate.day


def summarize(df: pl.DataFrame, keys: list[str], days: int | None = None) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame(schema={k: pl.String for k in keys} | {"FDI": pl.Float64, "FDINO": pl.Float64, "FDINO2": pl.Float64})
    out = df.group_by(keys).agg([
        pl.col("FDI").sum().alias("FDI"),
        pl.col("FDINO").sum().alias("FDINO"),
        pl.col("FDINO2").sum().alias("FDINO2"),
    ])
    if days:
        out = out.with_columns([
            (pl.col("FDI") / days).alias("FDI"),
            (pl.col("FDINO") / days).alias("FDINO"),
            (pl.col("FDINO2") / days).alias("FDINO2"),
        ])
    return out.sort(keys)


def write_section(rpt: AsaReportWriter, title: str, subtitle: str, data: pl.DataFrame, include_cust: bool = False) -> None:
    rpt.put("1", title)
    rpt.put(" ", "PUBLIC ISLAMIC BANK BERHAD - IBU")
    rpt.put(" ", "NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION")
    rpt.put(" ", subtitle)
    rpt.put(" ", "")

    hdr = "BRANCH NO/CODE           TENOR      NO OF A/C   NO OF RECEIPT              AMOUNT"
    if include_cust:
        hdr = "CUSTOMER    " + hdr
    rpt.put(" ", hdr)
    rpt.put(" ", "-" * len(hdr))

    total_fdi = total_fdino = total_fdino2 = 0.0
    for row in data.iter_rows(named=True):
        mth_txt = FDFMT.get(int(row.get("MTH", 0)), "")
        line = (
            f"{row.get('BRCH', ''):<22}"
            f"{mth_txt:<12}"
            f"{format_num(row.get('FDINO', 0), dec=0, w=12)}"
            f"{format_num(row.get('FDINO2', 0), dec=0, w=16)}"
            f"{format_num(row.get('FDI', 0), dec=2, w=20)}"
        )
        if include_cust:
            line = f"{row.get('CUSTTXT', ''):<12}" + line
        rpt.put(" ", line)
        total_fdi += float(row.get("FDI", 0) or 0)
        total_fdino += float(row.get("FDINO", 0) or 0)
        total_fdino2 += float(row.get("FDINO2", 0) or 0)

    rpt.put(" ", "-" * len(hdr))
    ttl = (
        f"{'TOTAL':<34}"
        f"{format_num(total_fdino, dec=0, w=12)}"
        f"{format_num(total_fdino2, dec=0, w=16)}"
        f"{format_num(total_fdi, dec=2, w=20)}"
    )
    if include_cust:
        ttl = f"{'':12}" + ttl
    rpt.put(" ", ttl)


def main() -> None:
    # %MACRO CHKRPTDT; %IF "&REPTFQ" EQ "Y" %THEN %DO;
    reptfq = "Y"
    if reptfq != "Y":
        return

    dyibua, dyibua1, rdate, days = prepare_data()

    rpt = AsaReportWriter(OUTPUT_REPORT, PAGE_LENGTH)

    # REPORT ID : DIIMISC2 (OLD)
    old_todate = summarize(dyibua, ["BRCH", "MTH"])
    write_section(rpt, "REPORT ID : DIIMISC2 (OLD)", f"(04-SEP-04 UNTIL 15-APR-06) AS AT {rdate}", old_todate, include_cust=False)

    if dyibua1.height > 0:
        old_avg = summarize(dyibua1, ["BRCH", "MTH"], days=days)
        write_section(rpt, "REPORT ID : DIIMISC2 (OLD)", f"DAILY AVERAGE AS AT {rdate}", old_avg, include_cust=False)

    # REPORT ID : DIIMISC2 (NEW)
    new_todate = summarize(dyibua, ["CUSTTXT", "BRCH", "MTH"])
    write_section(rpt, "REPORT ID : DIIMISC2 (NEW)", f"(04-SEP-04 UNTIL 15-APR-06) AS AT {rdate}", new_todate, include_cust=True)

    if dyibua1.height > 0:
        new_avg = summarize(dyibua1, ["CUSTTXT", "BRCH", "MTH"], days=days)
        write_section(rpt, "REPORT ID : DIIMISC2 (NEW)", f"DAILY AVERAGE AS AT {rdate}", new_avg, include_cust=True)

    rpt.save()


if __name__ == "__main__":
    main()
