#!/usr/bin/env python3
"""
File Name: EIBMRT20
Report: TOP 20 LARGEST FD CORPORATE CUSTOMER BY INTEREST BAND
"""

import duckdb
import polars as pl
from pathlib import Path
import sys


# ============================================================================
# Configuration and Path Setup
# ============================================================================

INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPTDATE_FILE = INPUT_DIR / "reptdate.parquet"
CISFD_FILE = INPUT_DIR / "cisfd_deposit.parquet"
FD_FILE = INPUT_DIR / "deposit_fd.parquet"

OUTPUT_FILE = OUTPUT_DIR / "tp20rate_month.txt"

PAGE_LENGTH = 60
ASA_NEW_PAGE = "1"
ASA_SPACE = " "


# ============================================================================
# Helper Functions
# ============================================================================

def format_comma(value: float, width: int = 20) -> str:
    if value is None:
        value = 0
    return f"{value:{width},.0f}"


def format_pct(value: float, width: int = 8) -> str:
    if value is None:
        value = 0.0
    return f"{value:{width}.2f}"


def ratecode_expr(rate_col: pl.Expr) -> pl.Expr:
    return (
        pl.when(rate_col.is_null()).then(1)
        .when(rate_col <= 2.75).then(1)
        .when(rate_col <= 2.80).then(2)
        .when(rate_col <= 2.85).then(3)
        .when(rate_col <= 2.90).then(4)
        .when(rate_col <= 2.95).then(5)
        .when(rate_col <= 3.00).then(6)
        .when(rate_col <= 3.05).then(7)
        .when(rate_col <= 3.10).then(8)
        .when(rate_col <= 3.15).then(9)
        .when(rate_col <= 3.20).then(10)
        .when(rate_col <= 3.25).then(11)
        .when(rate_col <= 3.30).then(12)
        .when(rate_col <= 3.35).then(13)
        .when(rate_col <= 3.40).then(14)
        .when(rate_col <= 3.45).then(15)
        .when(rate_col <= 3.50).then(16)
        .when(rate_col <= 3.55).then(17)
        .when(rate_col <= 3.60).then(18)
        .when(rate_col <= 3.65).then(19)
        .when(rate_col <= 3.70).then(20)
        .when(rate_col <= 3.75).then(21)
        .when(rate_col <= 3.80).then(22)
        .when(rate_col <= 3.85).then(23)
        .when(rate_col <= 3.90).then(24)
        .when(rate_col <= 3.95).then(25)
        .when(rate_col <= 4.00).then(26)
        .otherwise(27)
    )


def process_reptdate() -> dict:
    df = pl.read_parquet(REPTDATE_FILE)
    if df.is_empty():
        raise ValueError("REPTDATE file is empty.")
    reptdate = df["REPTDATE"][0]
    rdate = f"{reptdate.day:02d}/{reptdate.month:02d}/{str(reptdate.year)[-2:]}"
    return {
        "REPTYEAR": f"{reptdate.year:04d}",
        "REPTMON": f"{reptdate.month:02d}",
        "REPTDAY": f"{reptdate.day:02d}",
        "RDATE": rdate,
    }


def load_cisfd(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    query = f"""
        SELECT *
        FROM read_parquet('{CISFD_FILE}')
        WHERE SECCUST = '901'
          AND (
                (ACCTNO BETWEEN 1000000000 AND 1999999999)
             OR (ACCTNO BETWEEN 7000000000 AND 7999999999)
             OR (ACCTNO BETWEEN 4000000000 AND 4999999999)
             OR (ACCTNO BETWEEN 6000000000 AND 6999999999)
          )
    """
    cisfd_df = con.execute(query).pl()
    cisfd_df = cisfd_df.with_columns(
        pl.when(
            pl.col("NEWIC").is_not_null()
            & (pl.col("NEWIC").cast(pl.Utf8).str.strip_chars() != "")
        )
        .then(pl.col("NEWIC"))
        .otherwise(pl.col("CUSTNO"))
        .alias("ICNO")
    )
    return cisfd_df.select(
        ["CUSTNO", "ACCTNO", "CUSTNAME", "ICNO", "NEWIC", "OLDIC", "INDORG"]
    )


def load_fd(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    fd_df = con.execute(f"SELECT * FROM read_parquet('{FD_FILE}')").pl()
    fd_df = fd_df.filter(pl.col("CURBAL") > 0)
    keep_cols = ["ACCTNO", "CDNO", "CURBAL", "RATE", "PURPOSE", "CUSTCD"]
    if "NAME" in fd_df.columns:
        keep_cols.append("NAME")
    return fd_df.select(keep_cols)


def build_top_customers(fd_df: pl.DataFrame) -> pl.DataFrame:
    filtered = fd_df.filter(
        (pl.col("ICNO").is_not_null())
        & (pl.col("ICNO").cast(pl.Utf8).str.strip_chars() != "")
    )
    topfd = (
        filtered.group_by(["ICNO", "CUSTNAME"])
        .agg(pl.col("CURBAL").sum().alias("FDBAL"))
        .sort("FDBAL", descending=True)
        .head(20)
    )
    return topfd


def build_rate_table(fd_df: pl.DataFrame) -> pl.DataFrame:
    rate_df = (
        fd_df.with_columns(
            ratecode_expr(pl.col("RATE")).alias("RATECODE_INT")
        )
        .with_columns(
            pl.col("RATECODE_INT").cast(pl.Utf8).str.zfill(2).alias("RATECODE")
        )
        .group_by(["ICNO", "CUSTNAME", "RATECODE"])
        .agg(pl.col("CURBAL").sum().alias("CURBAL"))
        .pivot(values="CURBAL", index=["ICNO", "CUSTNAME"], columns="RATECODE")
        .fill_null(0)
    )
    for idx in range(1, 28):
        col_name = f"RATECD{idx:02d}"
        pivot_col = f"{idx:02d}"
        if pivot_col in rate_df.columns:
            rate_df = rate_df.rename({pivot_col: col_name})
        else:
            rate_df = rate_df.with_columns(pl.lit(0).alias(col_name))
    rate_cols = [f"RATECD{idx:02d}" for idx in range(1, 28)]
    rate_df = rate_df.with_columns(pl.sum_horizontal(rate_cols).alias("TOTRCD"))
    return rate_df.select(["ICNO", "CUSTNAME", "TOTRCD"] + rate_cols)


def write_header(f, rdate: str, new_page: bool) -> int:
    if new_page:
        f.write(f"{ASA_NEW_PAGE}REPORT ID : EIBMRT20\n")
    else:
        f.write(f"{ASA_NEW_PAGE}REPORT ID : EIBMRT20\n")
    f.write(
        f"{ASA_SPACE}TOP 20 LARGEST FD CORPORATE CUSTOMER BY INTEREST BAND AS AT {rdate}\n"
    )
    f.write(f"{ASA_SPACE}\n")
    header_fields = [
        "OBS",
        "DEPOSITOR",
        "BALANCE (RM)",
        "%",
        "<=2.75",
        "<=2.80",
        "<=2.85",
        "<=2.90",
        "<=2.95",
        "<=3.00",
        "<=3.05",
        "<=3.10",
        "<=3.15",
        "<=3.20",
        "<=3.25",
        "<=3.30",
        "<=3.35",
        "<=3.40",
        "<=3.45",
        "<=3.50",
        "<=3.55",
        "<=3.60",
        "<=3.65",
        "<=3.70",
        "<=3.75",
        "<=3.80",
        "<=3.85",
        "<=3.90",
        "<=3.95",
        "<=4.00",
        "> 4.00",
    ]
    f.write(f"{ASA_SPACE}{';'.join(header_fields)};\n")
    return 4


def write_report(rate_df: pl.DataFrame, rdate: str, totals: dict) -> None:
    rate_cols = [f"RATECD{idx:02d}" for idx in range(1, 28)]
    total_fdcd = totals["total_fdcd"]
    total_rate = totals["total_rate"]
    pct_rate = totals["pct_rate"]
    total_pct = totals["total_pct"]

    with OUTPUT_FILE.open("w", encoding="utf-8", newline="") as f:
        line_count = write_header(f, rdate, new_page=True)
        for idx, row in enumerate(rate_df.iter_rows(named=True), start=1):
            if line_count >= PAGE_LENGTH:
                line_count = write_header(f, rdate, new_page=True)
            custname = (row.get("CUSTNAME") or "").strip()
            totrcd = row.get("TOTRCD", 0)
            pctr = (totrcd / total_fdcd * 100) if total_fdcd else 0.0
            fields = [
                str(idx),
                custname,
                format_comma(totrcd),
                format_pct(pctr),
            ]
            for col in rate_cols:
                fields.append(format_comma(row.get(col, 0)))
            f.write(f"{ASA_SPACE}{';'.join(fields)};\n")
            line_count += 1

        if line_count >= PAGE_LENGTH:
            line_count = write_header(f, rdate, new_page=True)
        total_fields = [
            "",
            "TOTAL",
            format_comma(total_fdcd),
            format_pct(total_pct),
        ]
        total_fields.extend(format_comma(total_rate[idx]) for idx in range(1, 28))
        f.write(f"{ASA_SPACE}{';'.join(total_fields)};\n")
        line_count += 1

        if line_count >= PAGE_LENGTH:
            line_count = write_header(f, rdate, new_page=True)
        pct_fields = [
            "",
            "% COMPOSITION",
            format_pct(total_pct),
            "",
        ]
        pct_fields.extend(format_pct(pct_rate[idx]) for idx in range(1, 28))
        f.write(f"{ASA_SPACE}{';'.join(pct_fields)};\n")


def main() -> None:
    if not REPTDATE_FILE.exists():
        raise FileNotFoundError(f"Missing file: {REPTDATE_FILE}")
    if not CISFD_FILE.exists():
        raise FileNotFoundError(f"Missing file: {CISFD_FILE}")
    if not FD_FILE.exists():
        raise FileNotFoundError(f"Missing file: {FD_FILE}")

    macro_vars = process_reptdate()

    con = duckdb.connect()
    try:
        cisfd_df = load_cisfd(con)
        fd_df = load_fd(con)
    finally:
        con.close()

    fd_df = fd_df.join(cisfd_df, on="ACCTNO", how="left")
    if "NAME" in fd_df.columns:
        fd_df = fd_df.with_columns(
            pl.when(
                pl.col("CUSTNAME").is_null()
                | (pl.col("CUSTNAME").cast(pl.Utf8).str.strip_chars() == "")
            )
            .then(pl.col("NAME"))
            .otherwise(pl.col("CUSTNAME"))
            .alias("CUSTNAME")
        )

    fd_df = fd_df.filter(
        (pl.col("PURPOSE").cast(pl.Utf8) != "2")
        & (~pl.col("CUSTCD").is_in([77, 78, 95, 96]))
        & (pl.col("INDORG") == "O")
    )

    topfd = build_top_customers(fd_df)
    fd_df = fd_df.join(topfd, on=["ICNO", "CUSTNAME"], how="inner")

    rate_df = build_rate_table(fd_df).sort("TOTRCD", descending=True)

    total_fdcd = rate_df["TOTRCD"].sum()
    total_rate = {
        idx: rate_df.get_column(f"RATECD{idx:02d}").sum() for idx in range(1, 28)
    }
    pct_rate = {
        idx: (total_rate[idx] / total_fdcd * 100) if total_fdcd else 0.0
        for idx in range(1, 28)
    }
    total_pct = sum(pct_rate.values()) if total_fdcd else 0.0

    write_report(
        rate_df,
        macro_vars["RDATE"],
        {
            "total_fdcd": total_fdcd,
            "total_rate": total_rate,
            "pct_rate": pct_rate,
            "total_pct": total_pct,
        },
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
