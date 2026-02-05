#!/usr/bin/env python3
"""
File Name: EIBMNLF1
Inputs are expected as Parquet datasets with SAS column names.
Outputs:
- CALC.txt: final BNMCODE/AMOUNT output equivalent to BNM.CALC
- *_REPORT.txt: PROC TABULATE-style text reports with ASA carriage controls
- BASE_*.parquet and STORE_*.parquet: persisted product histories
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict

import duckdb
import polars as pl


# =============================================================================
# PATH SETUP (DEFINED EARLY AS REQUESTED)
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "data" / "input"
OUTPUT_DIR = BASE_DIR / "data" / "output"
STATE_DIR = BASE_DIR / "data" / "state"

REPTDATE_PATH = INPUT_DIR / "REPTDATE.parquet"
TABLE_PATH = INPUT_DIR / "TABLE.parquet"
WALK_PATH = INPUT_DIR / "WALK.parquet"
NOTE_PATH = INPUT_DIR / "NOTE.parquet"
LOAN_TEMPLATE = INPUT_DIR / "LOAN{reptmon}{nowk}.parquet"

BASE_TEMPLATE = STATE_DIR / "BASE_{prod}.parquet"
STORE_TEMPLATE = STATE_DIR / "STORE_{prod}.parquet"
CALC_TXT_PATH = OUTPUT_DIR / "CALC.txt"
REPORT_TEMPLATE = OUTPUT_DIR / "{prod}_REPORT.txt"

for folder in (OUTPUT_DIR, STATE_DIR):
    folder.mkdir(parents=True, exist_ok=True)


# =============================================================================
# FORMATS / BUSINESS RULES
# =============================================================================
def remfmt(value: float) -> str:
    if value <= 0.1:
        return "01"
    if value <= 1:
        return "02"
    if value <= 3:
        return "03"
    if value <= 6:
        return "04"
    if value <= 12:
        return "05"
    return "06"


def remfmt_label(value: float) -> str:
    if value <= 0.255:
        return "UP TO 1 WK"
    if value <= 1:
        return ">1 WK - 1 MTH"
    if value <= 3:
        return ">1 MTH - 3 MTHS"
    if value <= 6:
        return ">3 - 6 MTHS"
    if value <= 12:
        return ">6 MTHS - 1 YR"
    return "> 1 YEAR"


@dataclass
class Params:
    reptdate: date
    nowk: str
    insert: str
    rdate: str
    rdat1: int
    reptmon: str
    reptday: str


def _to_date(value) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return datetime.fromisoformat(value).date()
    if isinstance(value, int):
        return date(1960, 1, 1) + timedelta(days=value)
    raise TypeError(f"Unsupported date value: {value!r}")


def date_to_sas(d: date) -> int:
    return (d - date(1960, 1, 1)).days


def read_params(con: duckdb.DuckDBPyConnection) -> Params:
    rept_raw = con.execute(
        "SELECT REPTDATE FROM read_parquet(?) LIMIT 1", [str(REPTDATE_PATH)]
    ).fetchone()
    if rept_raw is None:
        raise ValueError("REPTDATE input is empty")

    reptdate = _to_date(rept_raw[0])
    day = reptdate.day
    nowk = "1" if day == 8 else "2" if day == 15 else "3" if day == 22 else "4"

    today = date.today()
    first_day_current_month = date(today.year, today.month, 1)
    last_prev_month = first_day_current_month - timedelta(days=1)
    last = f"{last_prev_month.day:02d}"
    reptday = f"{day:02d}"

    insert = "N"
    if reptday in {"08", "15", "22"}:
        insert = "Y"
    elif reptday == last and today.day < 8:
        insert = "Y"

    return Params(
        reptdate=reptdate,
        nowk=nowk,
        insert=insert,
        rdate=reptdate.strftime("%d%m%Y"),
        rdat1=date_to_sas(reptdate),
        reptmon=f"{reptdate.month:02d}",
        reptday=reptday,
    )


def load_parquet(con: duckdb.DuckDBPyConnection, path: Path) -> pl.DataFrame:
    return con.execute("SELECT * FROM read_parquet(?)", [str(path)]).pl()


def write_report_with_asa(prod: str, table_df: pl.DataFrame) -> None:
    report_path = Path(str(REPORT_TEMPLATE).format(prod=prod))
    summary = (
        table_df.group_by("REMMTH")
        .agg(pl.col("AMOUNT").sum().alias("AMOUNT"))
        .sort("REMMTH")
    )

    lines = [
        f"1BREAKDOWN BY PURE CONTRACTUAL MATURITY PROFILE - {prod}",
        " CORE (NON-TRADING) BANKING ACTIVITIES",
        "  REMMTH LABEL                    AMOUNT",
        "  -----------------------------------------------",
    ]

    total = 0.0
    for row in summary.iter_rows(named=True):
        amount = float(row["AMOUNT"] or 0.0)
        total += amount
        lines.append(f"  {row['REMMTH']:>5} {remfmt_label(float(row['REMMTH'])):<16} {amount:>20,.2f}")
    lines.append("  -----------------------------------------------")
    lines.append(f"  TOTAL                          {total:>20,.2f}")

    page_len = 60
    with report_path.open("w", encoding="utf-8") as f:
        for i, line in enumerate(lines, start=1):
            f.write(line + "\n")
            if i % page_len == 0 and i != len(lines):
                f.write("1CONTINUED\n")


def upsert_store_and_calculate(
    prod: str,
    newrec: pl.DataFrame,
    table_df: pl.DataFrame,
    params: Params,
) -> pl.DataFrame:
    base_path = Path(str(BASE_TEMPLATE).format(prod=prod))
    store_path = Path(str(STORE_TEMPLATE).format(prod=prod))

    if base_path.exists():
        base_df = pl.read_parquet(base_path)
    else:
        base_df = pl.DataFrame({"REPTDATE": [], "AMOUNT": []}, schema={"REPTDATE": pl.Int32, "AMOUNT": pl.Float64})

    if params.insert == "Y":
        base_df = base_df.filter(pl.col("REPTDATE") != params.rdat1)
        base_df = pl.concat([base_df, newrec], how="vertical")
        base_df.write_parquet(base_path)
        store_df = base_df
    else:
        store_df = pl.concat([base_df, newrec], how="vertical")

    store_df = store_df.filter(pl.col("REPTDATE") <= params.rdat1)
    store_df.write_parquet(store_path)

    week48 = store_df.sort("REPTDATE", descending=True).head(48)
    curbal = float(week48["AMOUNT"][0]) if week48.height else 0.0
    minbal = float(week48["AMOUNT"].min()) if week48.height else 0.0

    calculated = table_df.with_columns(
        pl.when(pl.col("REMMTH") < 12)
        .then((pl.lit(curbal) - pl.lit(minbal)) / 5)
        .otherwise(pl.lit(minbal))
        .alias("AMOUNT")
    )

    write_report_with_asa(prod, calculated)
    return calculated


def process_pbcard(con: duckdb.DuckDBPyConnection, params: Params, table_df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    walk_df = load_parquet(con, WALK_PATH)

    if "REPTDATE" in walk_df.columns:
        card_rows = walk_df.filter((pl.col("DESC") == "34200") & (pl.col("REPTDATE") == params.rdat1))
    else:
        card_rows = walk_df.filter(pl.col("DESC") == "34200").with_columns(
            pl.date((pl.col("YY") + 2000).cast(pl.Int32), pl.col("MM").cast(pl.Int8), pl.col("DD").cast(pl.Int8)).alias("D")
        ).with_columns((pl.col("D") - pl.lit(date(1960, 1, 1))).dt.total_days().alias("REPTDATE")).filter(pl.col("REPTDATE") == params.rdat1)

    cardamt = float(card_rows["AMOUNT"][0]) if card_rows.height else 0.0
    newrec = pl.DataFrame({"REPTDATE": [params.rdat1], "AMOUNT": [cardamt]})
    table_calc = upsert_store_and_calculate("CARD", newrec, table_df, params)

    pbcard = table_calc.with_columns(
        (pl.lit("9321508") + pl.col("REMMTH").map_elements(remfmt, return_dtype=pl.Utf8) + pl.lit("0000Y")).alias("BNMCODE")
    ).select("BNMCODE", "AMOUNT")

    basket_pct = {1: 0.15, 2: 0.69, 3: 0.01, 4: 0.03, 5: 0.07, 6: 0.05}
    pbcard2 = table_df.filter(pl.col("BASKET").is_in(list(basket_pct.keys()))).with_columns([
        (pl.lit("9521508") + pl.col("REMMTH").map_elements(remfmt, return_dtype=pl.Utf8) + pl.lit("0000Y")).alias("BNMCODE"),
        pl.col("BASKET").map_elements(lambda x: cardamt * basket_pct[int(x)], return_dtype=pl.Float64).alias("AMOUNT"),
    ]).select("BNMCODE", "AMOUNT")

    return pbcard, pbcard2


def process_overdraft(params: Params, table_df: pl.DataFrame, note_df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    note_df = note_df.with_columns(pl.col("BNMCODE").str.slice(0, 7).alias("BNMCODE1"))

    odcorp_amount = float(note_df.filter(pl.col("BNMCODE1") == "9521309")["AMOUNT"].sum() or 0.0)
    odind_amount = float(note_df.filter(pl.col("BNMCODE1") == "9521308")["AMOUNT"].sum() or 0.0)

    odcorp_table = upsert_store_and_calculate(
        "ODCORP", pl.DataFrame({"REPTDATE": [params.rdat1], "AMOUNT": [odcorp_amount]}), table_df, params
    )
    odind_table = upsert_store_and_calculate(
        "ODIND", pl.DataFrame({"REPTDATE": [params.rdat1], "AMOUNT": [odind_amount]}), table_df, params
    )

    odcorp = odcorp_table.with_columns(
        (pl.lit("9321309") + pl.col("REMMTH").map_elements(remfmt, return_dtype=pl.Utf8) + pl.lit("0000Y")).alias("BNMCODE")
    ).select("BNMCODE", "AMOUNT")

    odind = odind_table.with_columns(
        (pl.lit("9321308") + pl.col("REMMTH").map_elements(remfmt, return_dtype=pl.Utf8) + pl.lit("0000Y")).alias("BNMCODE")
    ).select("BNMCODE", "AMOUNT")

    return odcorp, odind


def process_smc(con: duckdb.DuckDBPyConnection, params: Params, table_df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    loan_path = Path(str(LOAN_TEMPLATE).format(reptmon=params.reptmon, nowk=params.nowk))
    loan_df = load_parquet(con, loan_path)

    smc = (
        loan_df
        .filter(~pl.col("PAIDIND").is_in(["P", "C"]))
        .filter((pl.col("PRODCD").str.slice(0, 2) == "34") | (pl.col("PRODUCT").is_in([225, 226])))
        .filter(pl.col("ACCTYPE") == "OD")
        .filter(~pl.col("PRODUCT").is_in([151, 152, 181]))
        .with_columns([
            pl.when(pl.col("CUSTCD").is_in(["77", "78", "95", "96"]))
            .then(pl.lit("08"))
            .otherwise(pl.lit("09"))
            .alias("CUST"),
            pl.lit(0.1).alias("REMMTH"),
            pl.col("BALANCE").cast(pl.Float64).alias("AMOUNT"),
        ])
        .filter(pl.col("PRODCD") == "34240")
        .with_columns(
            (pl.lit("95219") + pl.col("CUST") + pl.col("REMMTH").map_elements(remfmt, return_dtype=pl.Utf8) + pl.lit("0000Y")).alias("BNMCODE")
        )
    )

    smccorp_amount = float(smc.filter(pl.col("BNMCODE").str.slice(0, 7) == "9521909")["AMOUNT"].sum() or 0.0)
    smcind_amount = float(smc.filter(pl.col("BNMCODE").str.slice(0, 7) == "9521908")["AMOUNT"].sum() or 0.0)

    smccorp_table = upsert_store_and_calculate(
        "SMCCORP", pl.DataFrame({"REPTDATE": [params.rdat1], "AMOUNT": [smccorp_amount]}), table_df, params
    )
    smcind_table = upsert_store_and_calculate(
        "SMCIND", pl.DataFrame({"REPTDATE": [params.rdat1], "AMOUNT": [smcind_amount]}), table_df, params
    )

    smccorp = smccorp_table.with_columns(
        (pl.lit("9321909") + pl.col("REMMTH").map_elements(remfmt, return_dtype=pl.Utf8) + pl.lit("0000Y")).alias("BNMCODE")
    ).select("BNMCODE", "AMOUNT")

    smcind = smcind_table.with_columns(
        (pl.lit("9321908") + pl.col("REMMTH").map_elements(remfmt, return_dtype=pl.Utf8) + pl.lit("0000Y")).alias("BNMCODE")
    ).select("BNMCODE", "AMOUNT")

    return smccorp, smcind


def write_calc_txt(df: pl.DataFrame, path: Path) -> None:
    # Text output is sufficient (no packed-decimal fields required by this job).
    with path.open("w", encoding="utf-8") as f:
        for row in df.iter_rows(named=True):
            f.write(f"{row['BNMCODE']:<14}{float(row['AMOUNT']):>20.2f}\n")


def main() -> int:
    print("BASE_DIR     =", BASE_DIR)
    print("INPUT_DIR    =", INPUT_DIR)
    print("REPTDATE     =", REPTDATE_PATH)
    print("Exists?      =", REPTDATE_PATH.exists(), "\n")

    con = duckdb.connect()
    params = read_params(con)

    table_df = load_parquet(con, TABLE_PATH)
    note_df = load_parquet(con, NOTE_PATH)

    pbcard, pbcard2 = process_pbcard(con, params, table_df)
    odcorp, odind = process_overdraft(params, table_df, note_df)
    smccorp, smcind = process_smc(con, params, table_df)

    calc = pl.concat([pbcard, pbcard2, odcorp, odind, smccorp, smcind], how="vertical")
    write_calc_txt(calc, CALC_TXT_PATH)

    print(f"Generated: {CALC_TXT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
