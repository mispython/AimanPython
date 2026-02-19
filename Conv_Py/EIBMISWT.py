#!/usr/bin/env python3
"""
Program: EIBMISWT
"""

from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path

import duckdb
import polars as pl

# %INC PGM(PBBLNFMT);
# Functional dependency: reuse shared converted format helpers.
from PBBLNFMT import format_lnprod

# -----------------------------------------------------------------------------
# Path setup (defined early)
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPTDATE_FILE = Path(os.getenv("REPTDATE_FILE", str(INPUT_DIR / "LOAN_REPTDATE.parquet")))
NPL_TOTIIS_FILE_TEMPLATE = os.getenv("NPL_TOTIIS_FILE_TEMPLATE", str(INPUT_DIR / "NPL_TOTIIS{reptmon}.parquet"))
SASDATA_LOAN_FILE_TEMPLATE = os.getenv("SASDATA_LOAN_FILE_TEMPLATE", str(INPUT_DIR / "SASDATA_LOAN{reptmon}{nowk}.parquet"))
CRM_ISLM_FILE_TEMPLATE = os.getenv("CRM_ISLM_FILE_TEMPLATE", str(INPUT_DIR / "CRM_ISLM{reptmon}.parquet"))

OUT_REPTDATE = Path(os.getenv("OUT_REPTDATE", str(OUTPUT_DIR / "REPTDATE.parquet")))
OUT_IIS = Path(os.getenv("OUT_IIS", str(OUTPUT_DIR / "IIS.parquet")))
OUT_LOAN = Path(os.getenv("OUT_LOAN", str(OUTPUT_DIR / "LOAN.parquet")))
OUT_ISLM = Path(os.getenv("OUT_ISLM", str(OUTPUT_DIR / "ISLM.parquet")))
OUT_ISLM1_DAY = Path(os.getenv("OUT_ISLM1_DAY", str(OUTPUT_DIR / "ISLM1_DAY.parquet")))
OUT_ISLM1_MON = Path(os.getenv("OUT_ISLM1_MON", str(OUTPUT_DIR / "ISLM1_MONTH.parquet")))
OUT_REPORT = Path(os.getenv("OUT_REPORT", str(OUTPUT_DIR / "EIBMISWT_report.txt")))

PAGE_LENGTH = 60


# -----------------------------------------------------------------------------
# PROC FORMAT VALUE SLNPRDF
# -----------------------------------------------------------------------------
def slnprdf(value: int | None) -> str:
    if value is None:
        return "OTHERS"
    if 110 <= value <= 116:
        return "ISLAMIC HOUSING"
    if value in {120, 127, 129, 137, 138, 193}:
        return "ISLAMIC TERM"
    if value in {135, 136}:
        return "ISLAMIC PERSONAL"
    if value in {194, 195, 196}:
        return "ISLAMIC CON. DURABLE"
    if value == 170:
        return "ISLAMIC FUND FOR FOOD"
    return "OTHERS"


def _parse_reptdate(v: object) -> date:
    if isinstance(v, date):
        return v
    if isinstance(v, datetime):
        return v.date()
    return datetime.fromisoformat(str(v)).date()


def _asa_write(lines: list[str], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="\n") as f:
        row_no = 0
        for line in lines:
            ctrl = "1" if row_no % PAGE_LENGTH == 0 else " "
            f.write(f"{ctrl}{line}\n")
            row_no += 1


def _fmt_amt(v: float | None, width: int = 17, decimals: int = 2) -> str:
    val = 0.0 if v is None else float(v)
    return f"{val:>{width},.{decimals}f}"


def main() -> None:
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4")

    reptdate_df = pl.from_arrow(
        con.execute(f"SELECT * FROM read_parquet('{REPTDATE_FILE.as_posix()}')").fetch_arrow_table()
    )
    if reptdate_df.height == 0:
        raise ValueError("REPTDATE source is empty.")

    rept_col = reptdate_df.get_column("REPTDATE")
    reptdate = max(_parse_reptdate(v) for v in rept_col.to_list())

    day = reptdate.day
    if day == 8:
        sdd, nowk, nowk1 = 1, "1", "4"
    elif day == 15:
        sdd, nowk, nowk1 = 9, "2", "1"
    elif day == 22:
        sdd, nowk, nowk1 = 16, "3", "2"
    else:
        sdd, nowk, nowk1 = 23, "4", "3"

    reptmon = f"{reptdate.month:02d}"
    mm1 = reptdate.month - 1 if nowk == "1" else reptdate.month
    if mm1 == 0:
        mm1 = 12
    reptmon1 = f"{mm1:02d}"
    reptyear = f"{reptdate.year:04d}"
    reptday = f"{reptdate.day:02d}"
    rdate = reptdate.strftime("%d%m%y")
    _ = (sdd, nowk1, reptmon1, reptyear, reptday)  # retained macro equivalents

    reptdate_out = pl.DataFrame({"REPTDATE": [reptdate]})
    reptdate_out.write_parquet(OUT_REPTDATE)

    totiis_file = Path(NPL_TOTIIS_FILE_TEMPLATE.format(reptmon=reptmon, nowk=nowk))
    sasloan_file = Path(SASDATA_LOAN_FILE_TEMPLATE.format(reptmon=reptmon, nowk=nowk))
    islm_file = Path(CRM_ISLM_FILE_TEMPLATE.format(reptmon=reptmon, nowk=nowk))

    iis = pl.from_arrow(con.execute(f"SELECT * FROM read_parquet('{totiis_file.as_posix()}')").fetch_arrow_table())
    iis = (
        iis.with_columns(pl.col("LOANTYPE").map_elements(slnprdf, return_dtype=pl.Utf8).alias("PRODTYPE"))
        .filter(pl.col("PRODTYPE") != "OTHERS")
        .select(["ACCTNO", "NOTENO", "TOTIIS", "PRODTYPE"])
    )
    iis.write_parquet(OUT_IIS)

    loan = pl.from_arrow(con.execute(f"SELECT * FROM read_parquet('{sasloan_file.as_posix()}')").fetch_arrow_table())
    cutoff = date(2004, 9, 4)

    loan = loan.with_columns([
        pl.when(pl.col("PRODCD").is_null() | (pl.col("PRODCD") == ""))
        .then(pl.col("PRODUCT").map_elements(lambda x: format_lnprod(int(x)) if x is not None else "", return_dtype=pl.Utf8))
        .otherwise(pl.col("PRODCD").cast(pl.Utf8))
        .alias("PRODCD")
    ])

    loan = loan.filter(
        (pl.col("PRODCD") != "N")
        & (pl.col("AMTIND") == "I")
        & (pl.col("ISSDTE") < pl.lit(cutoff))
        & (pl.col("ACCTNO") < 8_000_000_000)
    )

    loan = loan.with_columns([
        pl.when(pl.col("ACCTYPE") == "LN")
        .then(pl.col("PRODUCT").map_elements(slnprdf, return_dtype=pl.Utf8))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
        .alias("PRODTYPE"),
        pl.when((pl.col("PRODCD") == "34180") & (pl.col("AMTIND") == "I"))
        .then((-1) * pl.col("CURBAL"))
        .otherwise(pl.col("CURBAL"))
        .alias("CURBAL"),
        pl.when(pl.col("PRODUCT") == 193)
        .then(pl.coalesce([pl.col("CURBAL"), pl.lit(0.0)]) + pl.coalesce([pl.col("ACCRUAL"), pl.lit(0.0)]))
        .otherwise(pl.col("CURBAL"))
        .alias("CURBAL"),
        pl.when(pl.col("RISKRTE").is_in([1, 2, 3, 4])).then(pl.col("BALANCE")).otherwise(pl.lit(None)).alias("NPLBAL"),
    ])

    loan = loan.with_columns([
        pl.when((pl.col("PRODCD") == "34180") & (pl.col("AMTIND") == "I"))
        .then(pl.lit("ISLAMIC OD"))
        .otherwise(pl.col("PRODTYPE"))
        .alias("PRODTYPE"),
        pl.when((pl.col("PRODTYPE").is_null()) | (pl.col("PRODTYPE") == ""))
        .then(pl.col("PRODUCT").cast(pl.Utf8))
        .otherwise(pl.col("PRODTYPE"))
        .alias("PRODTYPE"),
    ])

    loan = loan.select(["ACCTNO", "NOTENO", "CURBAL", "PRODTYPE", "NPLBAL"])
    loan_merged = loan.join(iis, on=["ACCTNO", "NOTENO"], how="left")
    loan_merged.write_parquet(OUT_LOAN)

    # TITLE1 'TOTAL INTEREST IN SUSPENSE TAGGED WITH LOANS AS AT ' &RDATE;
    tab1 = (
        loan_merged.group_by("PRODTYPE")
        .agg([
            pl.sum("CURBAL").alias("CURBAL"),
            pl.sum("TOTIIS").alias("TOTIIS"),
            pl.sum("NPLBAL").alias("NPLBAL"),
        ])
        .sort("PRODTYPE")
    )

    islm = pl.from_arrow(con.execute(f"SELECT * FROM read_parquet('{islm_file.as_posix()}')").fetch_arrow_table())
    islm = (
        islm.with_columns(pl.col("LOANTYPE").map_elements(slnprdf, return_dtype=pl.Utf8).alias("PRODTYPE"))
        .filter(pl.col("PRODTYPE") != "OTHERS")
        .filter((pl.col("REPTDATE") > pl.lit(date(2004, 9, 3))) & (pl.col("REPTDATE") <= pl.lit(reptdate)))
        .unique(subset=["LOANTYPE", "REPTDATE"], keep="first")
        .sort(["PRODTYPE", "REPTDATE"])
    )
    islm.write_parquet(OUT_ISLM)

    islm1_day = (
        islm.group_by(["PRODTYPE", "REPTDATE"])
        .agg([
            pl.sum("CURBAL").alias("CURBAL"),
            pl.sum("YIELD").alias("YIELD"),
        ])
        .with_columns([
            pl.when(pl.col("CURBAL") != 0).then(pl.col("YIELD") / pl.col("CURBAL")).otherwise(0.0).alias("RATE"),
            pl.col("REPTDATE").dt.day().alias("DAY"),
        ])
        .sort(["PRODTYPE", "REPTDATE"])
    )
    islm1_day.write_parquet(OUT_ISLM1_DAY)

    islm1_mon = (
        islm.group_by(["PRODTYPE"])
        .agg([
            pl.sum("CURBAL").alias("CURBAL"),
            pl.sum("YIELD").alias("YIELD"),
        ])
        .with_columns([
            pl.when(pl.col("CURBAL") != 0).then(pl.col("YIELD") / pl.col("CURBAL")).otherwise(0.0).alias("RATE"),
            pl.lit(reptdate.day).alias("DAY"),
            (pl.col("CURBAL") / pl.lit(max(reptdate.day, 1))).alias("AVGAMT"),
        ])
        .sort("PRODTYPE")
    )
    islm1_mon.write_parquet(OUT_ISLM1_MON)

    report_lines: list[str] = []
    report_lines.append(f"TOTAL INTEREST IN SUSPENSE TAGGED WITH LOANS AS AT {rdate}")
    report_lines.append("PRODUCT TYPE                 BALANCE                IIS                NPL")
    report_lines.append("-" * 79)
    for row in tab1.iter_rows(named=True):
        report_lines.append(
            f"{str(row['PRODTYPE']):<25}"
            f"{_fmt_amt(row['CURBAL'], 18, 2)}"
            f"{_fmt_amt(row['TOTIIS'], 19, 2)}"
            f"{_fmt_amt(row['NPLBAL'], 19, 2)}"
        )

    report_lines.append("")
    report_lines.append(f"PBB ISLAMIC LOANS PRIOR TO PRIVATISATION AS AT {rdate}")
    report_lines.append("PRODUCT TYPE              DAY      O/S BALANCE (RM)      RATE")
    report_lines.append("-" * 79)
    for row in islm1_day.iter_rows(named=True):
        report_lines.append(
            f"{str(row['PRODTYPE']):<25}{int(row['DAY']):>4}"
            f"{_fmt_amt(row['CURBAL'], 24, 2)}"
            f"{float(row['RATE']):>10.2f}"
        )

    report_lines.append("")
    report_lines.append("PRODUCT TYPE              AVERAGE BALANCE FOR THE MONTH      WEIGHTED AVERAGE RATE")
    report_lines.append("-" * 92)
    for row in islm1_mon.iter_rows(named=True):
        report_lines.append(
            f"{str(row['PRODTYPE']):<25}"
            f"{_fmt_amt(row['AVGAMT'], 30, 2)}"
            f"{float(row['RATE']):>27.6f}"
        )

    _asa_write(report_lines, OUT_REPORT)
    con.close()


if __name__ == "__main__":
    main()
