#!/usr/bin/env python3
"""
File Name: EIBM0930
Report: Loans Matching and GL Interface Output
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb
import polars as pl


# =============================================================================
# Configuration and Path Setup
# =============================================================================

BASE_INPUT_DIR = Path("input")
BASE_OUTPUT_DIR = Path("output")
BASE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPTDATE_FILE = BASE_INPUT_DIR / "reptdate.parquet"
ODFMT_FILE = BASE_INPUT_DIR / "odfmt.parquet"
LNFMT_FILE = BASE_INPUT_DIR / "lnfmt.parquet"
DRFMT_FILE = BASE_INPUT_DIR / "drfmt.parquet"

BNM_LOAN_TEMPLATE = BASE_INPUT_DIR / "loan_{reptmon}{nowk}.parquet"
BNMI_LOAN_TEMPLATE = BASE_INPUT_DIR / "loan_i_{reptmon}{nowk}.parquet"
BTBNM_BTRAD_TEMPLATE = BASE_INPUT_DIR / "btrad_{reptmon}{nowk}.parquet"
IBTNM_IBTRAD_TEMPLATE = BASE_INPUT_DIR / "ibtrad_{reptmon}{nowk}.parquet"

GLINT_OUTPUT = BASE_OUTPUT_DIR / "fmgl0930.txt"
XMATCH_OUTPUT = BASE_OUTPUT_DIR / "xmatch.txt"
IMATCH_OUTPUT = BASE_OUTPUT_DIR / "imatch.txt"


# =============================================================================
# Helper Structures
# =============================================================================


@dataclass(frozen=True)
class ReportDates:
    reptdate: date
    reptmon: str
    nowk: str
    rdate_str: str
    rmonth: str
    ryear: str


# =============================================================================
# Helper Functions
# =============================================================================


def compute_week(day: int) -> str:
    if 1 <= day <= 8:
        return "1"
    if 9 <= day <= 15:
        return "2"
    if 16 <= day <= 22:
        return "3"
    return "4"


def load_report_dates() -> ReportDates:
    if not REPTDATE_FILE.exists():
        raise FileNotFoundError(f"Missing REPTDATE file: {REPTDATE_FILE}")

    df = pl.read_parquet(REPTDATE_FILE)
    if df.is_empty():
        raise ValueError("REPTDATE parquet is empty.")
    reptdate = df["REPTDATE"][0]
    if isinstance(reptdate, str):
        reptdate = date.fromisoformat(reptdate)
    reptmon = f"{reptdate.month:02d}"
    nowk = compute_week(reptdate.day)
    rdate_str = f"{reptdate.day:02d}/{reptdate.month:02d}/{str(reptdate.year)[-2:]}"
    rmonth = f"{reptdate.month:02d}"
    ryear = f"{reptdate.year:04d}"
    return ReportDates(
        reptdate=reptdate,
        reptmon=reptmon,
        nowk=nowk,
        rdate_str=rdate_str,
        rmonth=rmonth,
        ryear=ryear,
    )


def load_plob() -> pl.DataFrame:
    odfmt = pl.read_parquet(ODFMT_FILE).with_columns(pl.lit("OD").alias("ACCTYPE"))
    lnfmt = pl.read_parquet(LNFMT_FILE).with_columns(pl.lit("LN").alias("ACCTYPE"))
    plob = pl.concat([odfmt, lnfmt], how="vertical")
    return plob.unique(subset=["ACCTYPE", "PRODUCT"], keep="first")


def load_drfmt() -> pl.DataFrame:
    drfmt = pl.read_parquet(DRFMT_FILE)
    return (
        drfmt.select(["BRANCH1", "FMLOB"])
        .with_columns(
            pl.col("BRANCH1")
            .cast(pl.Utf8)
            .str.strip_chars()
            .cast(pl.Int64, strict=False)
            .alias("BRANCH1")
        )
        .drop_nulls(["BRANCH1"])
    )


def round_balance(expr: pl.Expr) -> pl.Expr:
    return expr.round(2)


def filter_loan_data(df: pl.DataFrame, reptdate: date, paidind_filter: pl.Expr) -> pl.DataFrame:
    df = df.with_columns(
        round_balance(pl.col("BALANCE")).alias("BALX"),
        pl.lit(" ").alias("XIND"),
    )
    return (
        df.filter(pl.col("BALANCE") != -0.0)
        .with_columns(
            pl.when(pl.col("BALX") == 0)
            .then(pl.lit("Y"))
            .otherwise(pl.lit(" "))
            .alias("XIND")
        )
        .filter(pl.col("XIND") != "Y")
        .filter(paidind_filter)
        .filter(pl.col("PRODCD").cast(pl.Utf8).str.starts_with("34"))
        .filter(pl.col("APPRDATE").cast(pl.Date) <= pl.lit(reptdate))
        .with_columns(pl.lit(1).alias("NOACCT"))
    )


def filter_loan_data_pi(df: pl.DataFrame, reptdate: date, paidind_filter: pl.Expr) -> pl.DataFrame:
    df = df.with_columns(
        round_balance(pl.col("BALANCE")).alias("BALX"),
        pl.lit(" ").alias("XIND"),
    )
    return (
        df.filter(pl.col("BALANCE") != -0.0)
        .with_columns(
            pl.when(pl.col("BALX") == 0)
            .then(pl.lit("Y"))
            .otherwise(pl.lit(" "))
            .alias("XIND")
        )
        .filter(pl.col("XIND") != "Y")
        .filter(paidind_filter)
        .filter(
            pl.col("PRODCD").cast(pl.Utf8).str.starts_with("34")
            | (pl.col("PRODCD") == "54120")
        )
        .filter(pl.col("APPRDATE").cast(pl.Date) <= pl.lit(reptdate))
        .with_columns(pl.lit(1).alias("NOACCT"))
    )


def compute_unmatched(alldata: pl.DataFrame, plob: pl.DataFrame) -> pl.DataFrame:
    return (
        alldata.join(plob.select(["ACCTYPE", "PRODUCT"]), on=["ACCTYPE", "PRODUCT"], how="anti")
        .unique(subset=["PRODUCT"], keep="first")
        .sort("PRODUCT")
    )


def map_pccc_lob(df: pl.DataFrame, drfmt: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(pl.col("BRANCH").alias("BRANCH1"))
    mapped = df.join(drfmt, on="BRANCH1", how="left")
    return mapped.with_columns(pl.col("FMLOB").alias("LOB")).drop("FMLOB")


def write_match_report(rows: pl.DataFrame, output_path: Path) -> None:
    with output_path.open("w", encoding="utf-8") as handle:
        if rows.is_empty():
            handle.write("DATA SUCCESSFULLY MATCHED\n")
            return
        for row in rows.iter_rows(named=True):
            product = f"{int(row['PRODUCT']):>3}"
            producx = str(row.get("PRODUCX", ""))[:7].ljust(7)
            lob = str(row.get("LOB", ""))[:4].ljust(4)
            line = list(" " * 28)
            line[0:3] = product
            line[12:19] = producx
            line[24:28] = lob
            handle.write("".join(line).rstrip() + "\n")


def format_branch(branch: int) -> str:
    if 800 <= branch <= 899:
        return f"H{branch:03d}"
    return f"A{branch:03d}"


def place_value(line: list[str], pos: int, value: str) -> None:
    start = pos - 1
    for idx, char in enumerate(value):
        if start + idx < len(line):
            line[start + idx] = char


def build_glint_line(row: dict, ym: int) -> str:
    line = list(" " * 416)
    place_value(line, 1, "D,")
    place_value(line, 3, str(row["AGLFLD"]).ljust(32))
    place_value(line, 35, ",")
    place_value(line, 36, "EXT".ljust(32))
    place_value(line, 68, ",")
    place_value(line, 69, str(row["CGLFLD"]).ljust(32))
    place_value(line, 101, ",")
    place_value(line, 102, "ACTUAL".ljust(32))
    place_value(line, 134, ",")
    place_value(line, 135, "MYR".ljust(32))
    place_value(line, 167, ",")
    place_value(line, 168, str(row["BRANCX"]).ljust(32))
    place_value(line, 200, ",")
    place_value(line, 201, str(row["LOB"]).ljust(32))
    place_value(line, 233, ",")
    place_value(line, 234, str(row["PRODUCX"]).ljust(32))
    place_value(line, 266, ",")
    place_value(line, 267, f"{ym:>6}")
    place_value(line, 299, ",")
    place_value(line, 300, f"{int(row['NOACCT']):>15}")
    place_value(line, 315, ",")
    place_value(line, 316, "Y,")
    place_value(line, 318, " ")
    place_value(line, 350, ",")
    place_value(line, 383, ",")
    place_value(line, 416, ",")
    return "".join(line).rstrip() + "\n"


def build_glint_trailer(cnt: int, numac: int) -> str:
    line = list(" ") * 80
    place_value(line, 1, "T,")
    place_value(line, 3, f"{cnt:>10}")
    place_value(line, 13, ",")
    place_value(line, 14, f"{numac:>15}")
    place_value(line, 29, ",")
    return "".join(line).rstrip() + "\n"


# =============================================================================
# Main Processing
# =============================================================================


def main() -> None:
    con = duckdb.connect(database=":memory:")
    dates = load_report_dates()
    plob = load_plob()
    drfmt = load_drfmt()

    loan_file = BNM_LOAN_TEMPLATE.with_name(
        BNM_LOAN_TEMPLATE.name.format(reptmon=dates.reptmon, nowk=dates.nowk)
    )
    loan_df = con.execute(f"SELECT * FROM read_parquet('{loan_file}')").pl()
    alm = filter_loan_data(
        loan_df,
        dates.reptdate,
        ~pl.col("PAIDIND").is_in(["P", "C"]),
    ).with_columns(pl.lit("PBB ").alias("AGLFLD"))

    plobx = compute_unmatched(alm, plob)

    alm1 = alm.filter(pl.col("LOB") == "PCCC")
    alm2 = alm.filter(pl.col("LOB") != "PCCC")
    alm1 = map_pccc_lob(alm1, drfmt)
    alm = pl.concat([alm1, alm2], how="vertical")

    write_match_report(plobx, XMATCH_OUTPUT)

    alms = (
        alm.group_by(["AGLFLD", "BRANCH", "PRODUCX", "LOB"], maintain_order=False)
        .agg(pl.col("NOACCT").sum())
        .rename({"NOACCT": "NOACCT"})
    )

    loan_i_file = BNMI_LOAN_TEMPLATE.with_name(
        BNMI_LOAN_TEMPLATE.name.format(reptmon=dates.reptmon, nowk=dates.nowk)
    )
    loan_i_df = con.execute(f"SELECT * FROM read_parquet('{loan_i_file}')").pl()
    almi = filter_loan_data_pi(
        loan_i_df,
        dates.reptdate,
        pl.col("PAIDIND") != "P",
    ).with_columns(pl.lit("PIBB ").alias("AGLFLD"))

    plobi = compute_unmatched(almi, plob)

    almi1 = almi.filter(pl.col("LOB") == "PCCC")
    almi2 = almi.filter(pl.col("LOB") != "PCCC")
    almi1 = map_pccc_lob(almi1, drfmt)
    almi = pl.concat([almi1, almi2], how="vertical")

    write_match_report(plobi, IMATCH_OUTPUT)

    almsi = (
        almi.group_by(["AGLFLD", "BRANCH", "PRODUCX", "LOB"], maintain_order=False)
        .agg(pl.col("NOACCT").sum())
        .rename({"NOACCT": "NOACCT"})
    )

    btrad_file = BTBNM_BTRAD_TEMPLATE.with_name(
        BTBNM_BTRAD_TEMPLATE.name.format(reptmon=dates.reptmon, nowk=dates.nowk)
    )
    btrad_df = con.execute(f"SELECT * FROM read_parquet('{btrad_file}')").pl()
    btrad_df = btrad_df.filter(
        (pl.col("DIRCTIND") == "D")
        & (pl.col("CUSTCD").cast(pl.Utf8).str.strip_chars() != "")
        & (pl.col("APPRLIMT") > 0)
        & (pl.col("BALANCE").round(2) != 0)
    )
    btrad_df = (
        btrad_df.sort(["ACCTNO", "APPRLIMT"], descending=[False, True])
        .unique(subset=["ACCTNO"], keep="first")
    )

    ibtrad_file = IBTNM_IBTRAD_TEMPLATE.with_name(
        IBTNM_IBTRAD_TEMPLATE.name.format(reptmon=dates.reptmon, nowk=dates.nowk)
    )
    ibtrad_df = con.execute(f"SELECT * FROM read_parquet('{ibtrad_file}')").pl()
    ibtrad_df = ibtrad_df.with_columns(
        pl.lit("IBTRAD").alias("IND"),
        pl.when(pl.col("BRANCH") >= 3000)
        .then(pl.col("BRANCH") - 3000)
        .otherwise(pl.col("BRANCH"))
        .alias("BRANCH"),
    )
    ibtrad_df = ibtrad_df.filter(
        (pl.col("DIRCTIND") == "D")
        & (pl.col("CUSTCD").cast(pl.Utf8).str.strip_chars() != "")
        & (pl.col("APPRLIMT") > 0)
        & (pl.col("BALANCE").round(2) != 0)
    )
    ibtrad_df = (
        ibtrad_df.sort(["ACCTNO", "APPRLIMT"], descending=[False, True])
        .unique(subset=["ACCTNO"], keep="first")
    )

    btrad_all = pl.concat([ibtrad_df, btrad_df], how="vertical")
    btrad_all = btrad_all.with_columns(
        pl.when(pl.col("IND") == "IBTRAD")
        .then(pl.lit("PIBB"))
        .otherwise(pl.lit("PBB"))
        .alias("AGLFLD"),
        pl.lit(1).alias("NOACCT"),
        pl.lit("LTF_OT  ").alias("PRODUCX"),
        pl.when(pl.col("RETAILID") == "C")
        .then(pl.lit("CORP    "))
        .otherwise(pl.lit("RETL        "))
        .alias("LOB"),
    )

    btalm = (
        btrad_all.group_by(["AGLFLD", "BRANCH", "PRODUCX", "LOB"], maintain_order=False)
        .agg(pl.col("NOACCT").sum())
    )

    fm = pl.concat([alms, almsi, btalm], how="vertical")
    fm = fm.with_columns(
        pl.col("BRANCH").cast(pl.Int64, strict=False).fill_null(0).alias("BRANCH"),
        pl.col("BRANCH").cast(pl.Int64, strict=False).fill_null(0).alias("BRANCH_NUM"),
    )
    fm = fm.with_columns(
        pl.col("BRANCH_NUM")
        .map_elements(format_branch, return_dtype=pl.Utf8)
        .alias("BRANCX"),
        pl.lit("EXT").alias("BGLFLD"),
        pl.lit("C_LNS").alias("CGLFLD"),
        pl.lit("ACTUAL").alias("DGLFLD"),
    )
    ym = int(f"{dates.ryear}{dates.rmonth}")
    fm = fm.sort(["AGLFLD", "BRANCX", "PRODUCX"])

    fm = fm.with_columns(
        pl.when(pl.col("BRANCX").str.starts_with("A"))
        .then(pl.col("BRANCX").str.slice(1, 3))
        .otherwise(pl.col("BRANCX"))
        .alias("BRANCX")
    ).with_columns(
        pl.when(pl.col("BRANCX").str.starts_with("H"))
        .then(pl.lit("HPOP"))
        .otherwise(pl.col("LOB"))
        .alias("LOB")
    )

    cnt = 0
    numac = 0
    with GLINT_OUTPUT.open("w", encoding="utf-8") as handle:
        for row in fm.iter_rows(named=True):
            cnt += 1
            numac += int(row["NOACCT"])
            handle.write(build_glint_line(row, ym))
        handle.write(build_glint_trailer(cnt, numac))


if __name__ == "__main__":
    main()
