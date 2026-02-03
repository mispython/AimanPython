#!/usr/bin/env python3
"""
File Name: EIBMMV20
Report: TOP 20 MOVEMENT FD CORPORATE CUSTOMER BY INTEREST BAND
Outputs a semicolon-delimited report with ASA carriage control characters.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl


# ============================================================================
# Configuration and Path Setup
# ============================================================================

INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input files
REPTDATE_FILE = INPUT_DIR / "reptdate.parquet"
CISFD_DEPOSIT_FILE = INPUT_DIR / "cisfd_deposit.parquet"
FDCURR_FD_FILE = INPUT_DIR / "fdcurr_fd.parquet"
FDPREV_FD_FILE = INPUT_DIR / "fdprev_fd.parquet"

# Output file (report with ASA carriage control)
OUTPUT_REPORT = OUTPUT_DIR / "eibmmv20_report.txt"

# Report constants
PAGE_LENGTH = 60
ASA_NEW_PAGE = "1"
ASA_SPACE = " "


# ============================================================================
# Helper Functions
# ============================================================================


def format_comma20(value: float | int | None) -> str:
    if value is None:
        return " " * 20
    return f"{value:20,.0f}"


def format_pct(value: float | int | None) -> str:
    if value is None:
        return f"{0:8.2f}"
    return f"{value:8.2f}"


def rate_code(rate: float | None) -> int:
    if rate is None:
        return 27
    thresholds = [
        2.75,
        2.80,
        2.85,
        2.90,
        2.95,
        3.00,
        3.05,
        3.10,
        3.15,
        3.20,
        3.25,
        3.30,
        3.35,
        3.40,
        3.45,
        3.50,
        3.55,
        3.60,
        3.65,
        3.70,
        3.75,
        3.80,
        3.85,
        3.90,
        3.95,
        4.00,
    ]
    for idx, threshold in enumerate(thresholds, start=1):
        if rate <= threshold:
            return idx
    return 27


def process_reptdate() -> dict[str, str]:
    if not REPTDATE_FILE.exists():
        raise FileNotFoundError(f"Missing REPTDATE file: {REPTDATE_FILE}")

    df = pl.read_parquet(REPTDATE_FILE)
    if len(df) == 0:
        raise ValueError("REPTDATE file is empty")

    reptdate = df["REPTDATE"][0]
    day = reptdate.day

    if day == 8:
        nowk = "1"
    elif day == 15:
        nowk = "2"
    elif day == 22:
        nowk = "3"
    else:
        nowk = "4"

    return {
        "NOWK": nowk,
        "REPTYEAR": f"{reptdate.year}",
        "REPTMON": f"{reptdate.month:02d}",
        "REPTDAY": f"{day:02d}",
        "RDATE": reptdate.strftime("%d/%m/%y"),
    }


def load_cisfd() -> pl.DataFrame:
    if not CISFD_DEPOSIT_FILE.exists():
        raise FileNotFoundError(f"Missing CISFD deposit file: {CISFD_DEPOSIT_FILE}")

    con = duckdb.connect()
    df = con.execute(
        """
        SELECT
            CUSTNO,
            ACCTNO,
            CUSTNAME,
            NEWIC,
            OLDIC,
            INDORG,
            SECCUST
        FROM read_parquet(?)
        WHERE SECCUST = '901'
          AND (
              (ACCTNO BETWEEN 1000000000 AND 1999999999) OR
              (ACCTNO BETWEEN 7000000000 AND 7999999999) OR
              (ACCTNO BETWEEN 4000000000 AND 4999999999) OR
              (ACCTNO BETWEEN 6000000000 AND 6999999999)
          )
        """,
        [str(CISFD_DEPOSIT_FILE)],
    ).pl()
    con.close()

    df = df.with_columns(
        pl.when(pl.col("NEWIC").is_not_null() & (pl.col("NEWIC").str.strip_chars() != ""))
        .then(pl.col("NEWIC"))
        .otherwise(pl.col("CUSTNO"))
        .alias("ICNO")
    )
    return df.select(["CUSTNO", "ACCTNO", "CUSTNAME", "ICNO", "NEWIC", "OLDIC", "INDORG"])


def load_fdcurr() -> pl.DataFrame:
    if not FDCURR_FD_FILE.exists():
        raise FileNotFoundError(f"Missing FDCURR file: {FDCURR_FD_FILE}")

    con = duckdb.connect()
    df = con.execute(
        """
        SELECT ACCTNO, CDNO, CURBAL, RATE, PURPOSE, CUSTCD
        FROM read_parquet(?)
        WHERE CURBAL > 0
        """,
        [str(FDCURR_FD_FILE)],
    ).pl()
    con.close()
    return df


def load_fdprev() -> pl.DataFrame:
    if not FDPREV_FD_FILE.exists():
        raise FileNotFoundError(f"Missing FDPREV file: {FDPREV_FD_FILE}")

    con = duckdb.connect()
    df = con.execute(
        """
        SELECT ACCTNO, CDNO, CURBAL, PURPOSE, CUSTCD
        FROM read_parquet(?)
        WHERE CURBAL > 0
        """,
        [str(FDPREV_FD_FILE)],
    ).pl()
    con.close()
    return df.rename({"CURBAL": "PREVBAL"})


def apply_common_filters(df: pl.DataFrame) -> pl.DataFrame:
    custcd_col = pl.col("CUSTCD").cast(pl.Int64, strict=False)
    purpose_col = pl.col("PURPOSE").cast(pl.Utf8)
    return df.filter(
        (purpose_col != "2")
        & (~custcd_col.is_in([77, 78, 95, 96]))
        & (pl.col("INDORG") == "O")
    )


def summarize_balances(df: pl.DataFrame, value_column: str) -> pl.DataFrame:
    return (
        df.filter(pl.col("ICNO").is_not_null() & (pl.col("ICNO").str.strip_chars() != ""))
        .group_by(["ICNO", "CUSTNAME"])
        .agg(pl.col(value_column).sum().alias(value_column))
    )


def build_rate_pivot(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        pl.col("RATE").map_elements(rate_code, return_dtype=pl.Int64).alias("RATE_CODE")
    )
    rate_summary = (
        df.group_by(["ICNO", "CUSTNAME", "RATE_CODE"])
        .agg(pl.col("CURBAL").sum().alias("RATE_SUM"))
    )
    pivot = rate_summary.pivot(
        index=["ICNO", "CUSTNAME"],
        columns="RATE_CODE",
        values="RATE_SUM",
    ).fill_null(0)

    for i in range(1, 28):
        col = str(i)
        if col not in pivot.columns:
            pivot = pivot.with_columns(pl.lit(0).alias(col))

    ordered_cols = ["ICNO", "CUSTNAME"] + [str(i) for i in range(1, 28)]
    pivot = pivot.select(ordered_cols)
    pivot = pivot.rename({str(i): f"RATECD{i:02d}" for i in range(1, 28)})
    pivot = pivot.with_columns(
        pl.sum_horizontal([pl.col(f"RATECD{i:02d}") for i in range(1, 28)]).alias("TOTRCD")
    )
    return pivot


def write_report(df: pl.DataFrame, totals: dict[str, str], rdate: str) -> None:
    headers = [
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

    line_count = 0

    def write_line(handle, asa: str, text: str, indent: int = 0) -> None:
        nonlocal line_count
        if line_count >= PAGE_LENGTH:
            handle.write(f"{ASA_NEW_PAGE}\n")
            line_count = 0
        handle.write(f"{asa}{' ' * indent}{text}\n")
        line_count += 1

    with OUTPUT_REPORT.open("w", encoding="utf-8") as handle:
        write_line(handle, ASA_NEW_PAGE, "REPORT ID : EIBMMV20")
        write_line(
            handle,
            ASA_SPACE,
            f"TOP 20 MOVEMENT FD CORPORATE CUSTOMER BY INTEREST BAND AT AT {rdate}",
        )
        write_line(handle, ASA_SPACE, " ")
        write_line(handle, ASA_SPACE, ";".join(headers), indent=1)

        for idx, row in enumerate(df.iter_rows(named=True), start=1):
            pct = row["TOTRCD"] / totals["NUMFDCD"] * 100 if totals["NUMFDCD"] else 0
            custname = row["CUSTNAME"] if row["CUSTNAME"] is not None else ""
            values = [
                str(idx),
                custname,
                format_comma20(row["TOTRCD"]),
                format_pct(pct),
            ] + [format_comma20(row[f"RATECD{i:02d}"]) for i in range(1, 28)]
            write_line(handle, ASA_SPACE, ";".join(values), indent=1)

        total_values = [
            "",
            "TOTAL",
            totals["TOTFDCD"],
            totals["PCTRCD"],
        ] + [totals[f"TOTRCD{i:02d}"] for i in range(1, 28)]
        write_line(handle, ASA_SPACE, ";".join(total_values), indent=1)

        pct_values = [
            "",
            "% COMPOSITION",
            totals["PCTRCD"],
            "",
        ] + [totals[f"PCTRCD{i:02d}"] for i in range(1, 28)]
        write_line(handle, ASA_SPACE, ";".join(pct_values), indent=1)


# ============================================================================
# Main Workflow
# ============================================================================

def main() -> None:
    rept = process_reptdate()

    cisfd = load_cisfd()
    fdcurr = load_fdcurr()
    fdprev = load_fdprev()

    fdcurr = fdcurr.join(cisfd, on="ACCTNO", how="left")
    fdprev = fdprev.join(cisfd, on="ACCTNO", how="left")

    if "NAME" in fdcurr.columns:
        fdcurr = fdcurr.with_columns(
            pl.when(pl.col("CUSTNAME").str.strip_chars() == "")
            .then(pl.col("NAME"))
            .otherwise(pl.col("CUSTNAME"))
            .alias("CUSTNAME")
        )
    if "NAME" in fdprev.columns:
        fdprev = fdprev.with_columns(
            pl.when(pl.col("CUSTNAME").str.strip_chars() == "")
            .then(pl.col("NAME"))
            .otherwise(pl.col("CUSTNAME"))
            .alias("CUSTNAME")
        )

    fdcurr = apply_common_filters(fdcurr)
    fdprev = apply_common_filters(fdprev)

    fdcurr_sum = summarize_balances(fdcurr, "CURBAL")
    fdprev_sum = summarize_balances(fdprev, "PREVBAL")

    movement = fdprev_sum.join(fdcurr_sum, on=["ICNO", "CUSTNAME"], how="outer")
    movement = movement.with_columns(
        pl.col("PREVBAL").fill_null(0),
        pl.col("CURBAL").fill_null(0),
    )
    movement = movement.with_columns(
        pl.when(pl.col("CURBAL") > pl.col("PREVBAL"))
        .then(pl.col("CURBAL") - pl.col("PREVBAL"))
        .otherwise(0)
        .alias("MOVEMENT")
    )

    top20 = (
        movement.sort("MOVEMENT", descending=True)
        .head(20)
        .select(["ICNO", "CUSTNAME"])
    )

    fdmvmt = fdcurr.join(top20, on=["ICNO", "CUSTNAME"], how="inner")

    pivot = build_rate_pivot(fdmvmt)
    pivot = pivot.sort("TOTRCD", descending=True)

    totfdcd = pivot["TOTRCD"].sum()
    totrate = {f"TOTRCD{i:02d}": pivot[f"RATECD{i:02d}"].sum() for i in range(1, 28)}

    totals = {
        "TOTFDCD": format_comma20(totfdcd),
        "NUMFDCD": float(totfdcd),
    }

    pct_values = {}
    pctr_total = 0.0
    for i in range(1, 28):
        rate_total = totrate[f"TOTRCD{i:02d}"]
        pct = (rate_total / totfdcd) * 100 if totfdcd else 0.0
        pctr_total += pct
        totals[f"TOTRCD{i:02d}"] = format_comma20(rate_total)
        pct_values[f"PCTRCD{i:02d}"] = format_pct(pct)

    totals["PCTRCD"] = format_pct(pctr_total)
    totals.update(pct_values)

    write_report(pivot, totals, rept["RDATE"])


if __name__ == "__main__":
    main()
