# !/usr/bin/env python3
"""
Program: EIBMDPFQ
Purpose: Convert SAS program EIBMDPFQ to Python.

Generates FD monthly opened/closed summary and PROC TABULATE-style report with ASA carriage control characters.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import polars as pl


# =============================================================================
# PATH SETUP (defined early as requested)
# =============================================================================
BASE_DIR = Path(".")
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR
OUTPUT_DIR = DATA_DIR / "output"
MIS_DIR = DATA_DIR / "MIS"

REPTDATE_PATH = INPUT_DIR / "DEPOSIT" / "REPTDATE.parquet"
FD_FD_PATH = INPUT_DIR / "FD" / "FD.parquet"
DEPOSIT_FD_PATH = INPUT_DIR / "DEPOSIT" / "FD.parquet"

REPORT_PATH = OUTPUT_DIR / "EIBMDPFQ_REPORT.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
MIS_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# REPORT / FORMAT CONSTANTS
# =============================================================================
ASA_NEW_PAGE = "1"
ASA_SPACE = " "
PAGE_LENGTH = 60


def sas_num_to_datetime(value: object) -> datetime:
    """Convert SAS date numeric (days since 1960-01-01) to datetime."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime(1960, 1, 1) + timedelta(days=int(value))
    # fall back to duckdb/iso parsing if string
    return datetime.fromisoformat(str(value))


def fmt_comma10(value: object) -> str:
    return f"{int(value if value is not None else 0):>10,}"


def fmt_comma18_2(value: object) -> str:
    return f"{float(value if value is not None else 0):>18,.2f}"


#;
# DATA REPTDATE;
#    SET DEPOSIT.REPTDATE;
#    ... CALL SYMPUT(...)
# RUN;
con = duckdb.connect()
rept_raw = con.execute(f"SELECT REPTDATE FROM '{REPTDATE_PATH}' LIMIT 1").fetchone()[0]
rept_dt = sas_num_to_datetime(rept_raw)

mm = rept_dt.month
mm1 = mm - 1
if mm1 == 0:
    mm1 = 12

RDATE = rept_dt.strftime("%d%m%Y")
RYEAR = rept_dt.strftime("%Y")
RMONTH = rept_dt.strftime("%m")
REPTMON = f"{mm:02d}"
REPTMON1 = f"{mm1:02d}"
RDAY = rept_dt.strftime("%d")

FDO_OUT_PATH = MIS_DIR / f"FDO{REPTMON}.parquet"
FD1_OUT_PATH = MIS_DIR / f"FD1{REPTMON}.parquet"
FD1_PREV_PATH = MIS_DIR / f"FD1{REPTMON1}.parquet"


#;
# *------------------------------------------------*
# *  AMOUNT OUTSTANDING (EXCLUDE -IVE BALANCES)    *
# *------------------------------------------------*;
# PROC SORT DATA=FD.FD OUT=FDC; BY ACCTNO;
# WHERE OPENIND IN ('O','D');
# DATA FDC; ... FIRST./LAST. ACCTNO logic ...
# PROC SORT; BY ACCTNO;
fdc = con.execute(
    f"""
    SELECT ACCTNO, COUNT(*)::BIGINT AS NOCD
    FROM '{FD_FD_PATH}'
    WHERE OPENIND IN ('O','D')
    GROUP BY ACCTNO
    """
).pl()


#;
# DATA FD;
#   SET DEPOSIT.FD;
#   IF BRANCH=227 THEN DELETE;
#   IF BRANCH=250 THEN BRANCH=092;
#   IF OPENMH=1 AND CLOSEMH=1 THEN DELETE;
#   IF (300<=PRODUCT<=303);
#   IF OPENIND='Z' THEN OPENIND='O';
#   IF OPENIND='O' OR OPENIND IN ('B','C','P') AND CLOSEMH=1;
#   ... NOACCT/open-close normalization ...
fd = con.execute(f"SELECT * FROM '{DEPOSIT_FD_PATH}'").pl()

fd = (
    fd.filter(pl.col("BRANCH") != 227)
    .with_columns(
        pl.when(pl.col("BRANCH") == 250)
        .then(pl.lit(92))
        .otherwise(pl.col("BRANCH"))
        .alias("BRANCH")
    )
    .filter(~((pl.col("OPENMH") == 1) & (pl.col("CLOSEMH") == 1)))
    .filter((pl.col("PRODUCT") >= 300) & (pl.col("PRODUCT") <= 303))
    .with_columns(
        pl.when(pl.col("OPENIND") == "Z")
        .then(pl.lit("O"))
        .otherwise(pl.col("OPENIND"))
        .alias("OPENIND")
    )
    .filter(
        (pl.col("OPENIND") == "O")
        | (pl.col("OPENIND").is_in(["B", "C", "P"]) & (pl.col("CLOSEMH") == 1))
    )
    .with_columns(
        pl.lit(0).alias("NOACCT"),
        pl.when(pl.col("OPENIND").is_in(["B", "C", "P"]))
        .then(pl.lit(0))
        .otherwise(pl.col("OPENMH"))
        .alias("OPENMH"),
        pl.when(pl.col("OPENIND").is_in(["B", "C", "P"]))
        .then(pl.col("CLOSEMH"))
        .otherwise(pl.lit(0))
        .alias("CLOSEMH"),
        pl.when(pl.col("OPENIND").is_in(["B", "C", "P"]))
        .then(pl.lit(0))
        .otherwise(pl.lit(1))
        .alias("NOACCT"),
    )
)

#;
# DATA FDO; SET FD; IF OPENMH=1;
fdo = fd.filter(pl.col("OPENMH") == 1)

#;
# PROC SUMMARY DATA=FDO NWAY;
# CLASS BRANCH;
# VAR OPENMH CURBAL;
# OUTPUT OUT=MIS.FDO&REPTMON ... SUM=;
fdo_summary = (
    fdo.group_by("BRANCH")
    .agg(
        pl.col("OPENMH").sum().alias("OPENMH"),
        pl.col("CURBAL").sum().alias("CURBAL"),
    )
    .sort("BRANCH")
)
fdo_summary.write_parquet(FDO_OUT_PATH)

#;
# DATA FD;
#   MERGE FDC FD(IN=A); BY ACCTNO;
#   IF A;
#   IF NOCD=. THEN NOCD=0;
fd_joined = fd.join(fdc, on="ACCTNO", how="left").with_columns(
    pl.col("NOCD").fill_null(0).alias("NOCD")
)

#;
# PROC SUMMARY DATA=FD NWAY;
# CLASS BRANCH PRODUCT;
# VAR OPENMH CLOSEMH NOACCT NOCD CURBAL;
# OUTPUT OUT=FD ... SUM=;
fd_summary = (
    fd_joined.group_by(["BRANCH", "PRODUCT"])
    .agg(
        pl.col("OPENMH").sum().alias("OPENMH"),
        pl.col("CLOSEMH").sum().alias("CLOSEMH"),
        pl.col("NOACCT").sum().alias("NOACCT"),
        pl.col("NOCD").sum().alias("NOCD"),
        pl.col("CURBAL").sum().alias("CURBAL"),
    )
    .sort(["BRANCH", "PRODUCT"])
)

#;
# %MACRO PROCESS;
#    %IF &REPTMON > 01 %THEN %DO;
if REPTMON > "01" and FD1_PREV_PATH.exists():
    fdp_prev = pl.read_parquet(FD1_PREV_PATH)
    fdp_prev = (
        fdp_prev.with_columns(
            pl.col("OPENCUM").alias("OPENCUX"),
            pl.col("CLOSECUM").alias("CLOSECUX"),
            pl.when(pl.col("BRANCH") == 250)
            .then(pl.lit(92))
            .otherwise(pl.col("BRANCH"))
            .alias("BRANCH"),
        )
        .group_by(["BRANCH", "PRODUCT"])
        .agg(
            pl.col("OPENCUX").sum().alias("OPENCUX"),
            pl.col("CLOSECUX").sum().alias("CLOSECUX"),
        )
    )

    fd_merged = fd_summary.join(fdp_prev, on=["BRANCH", "PRODUCT"], how="full", suffix="_right")
    drop_cols = [c for c in ["BRANCH_right", "PRODUCT_right"] if c in fd_merged.columns]

    fd_final = (
        fd_merged.with_columns(
            pl.coalesce([pl.col("BRANCH"), pl.col("BRANCH_right")]).alias("BRANCH"),
            pl.coalesce([pl.col("PRODUCT"), pl.col("PRODUCT_right")]).alias("PRODUCT"),
        )
        .drop(drop_cols)
        .with_columns(
            pl.col("CLOSECUX").fill_null(0).alias("CLOSECUX"),
            pl.col("OPENCUX").fill_null(0).alias("OPENCUX"),
            pl.col("OPENMH").fill_null(0).alias("OPENMH"),
            pl.col("CLOSEMH").fill_null(0).alias("CLOSEMH"),
            pl.col("NOACCT").fill_null(0).alias("NOACCT"),
            pl.col("CURBAL").fill_null(0).alias("CURBAL"),
            pl.col("NOCD").fill_null(0).alias("NOCD"),
        )
        .with_columns(
            (pl.col("OPENMH") + pl.col("OPENCUX")).alias("OPENCUM"),
            (pl.col("CLOSEMH") + pl.col("CLOSECUX")).alias("CLOSECUM"),
            (pl.col("OPENMH") - pl.col("CLOSEMH")).alias("NETCHGMH"),
        )
        .with_columns((pl.col("OPENCUM") - pl.col("CLOSECUM")).alias("NETCHGYR"))
        .select(
            [
                "BRANCH",
                "PRODUCT",
                "OPENMH",
                "CLOSEMH",
                "NOACCT",
                "NOCD",
                "CURBAL",
                "OPENCUM",
                "CLOSECUM",
                "NETCHGMH",
                "NETCHGYR",
            ]
        )
        .sort(["BRANCH", "PRODUCT"])
    )
else:
    # %ELSE %DO;
    fd_final = (
        fd_summary.with_columns(
            pl.col("OPENMH").alias("OPENCUM"),
            pl.col("CLOSEMH").alias("CLOSECUM"),
            (pl.col("OPENMH") - pl.col("CLOSEMH")).alias("NETCHGMH"),
        )
        .with_columns((pl.col("OPENCUM") - pl.col("CLOSECUM")).alias("NETCHGYR"))
        .with_columns(
            pl.when(pl.col("BRANCH") == 250)
            .then(pl.lit(92))
            .otherwise(pl.col("BRANCH"))
            .alias("BRANCH")
        )
        .sort(["BRANCH", "PRODUCT"])
    )

# DATA MIS.FD1&REPTMON; SET FD;
fd_final.write_parquet(FD1_OUT_PATH)


#;
# *------------------------------------------------*
# *  PRINT REPORT TO TEXT FILE                     *
# *------------------------------------------------*;
# TITLE1 'PUBLIC BANK BERHAD';
# TITLE2 'FD ACCOUNT OPENED/CLOSED FOR THE MONTH AS AT' &RDATE;
# PROC TABULATE DATA=FD FORMAT=COMMA10. MISSING NOSEPS FORMCHAR='           ';
report_data = (
    fd_final.group_by("BRANCH")
    .agg(
        pl.col("OPENMH").sum().alias("OPENMH"),
        pl.col("OPENCUM").sum().alias("OPENCUM"),
        pl.col("CLOSEMH").sum().alias("CLOSEMH"),
        pl.col("CLOSECUM").sum().alias("CLOSECUM"),
        pl.col("NOACCT").sum().alias("NOACCT"),
        pl.col("NOCD").sum().alias("NOCD"),
        pl.col("CURBAL").sum().alias("CURBAL"),
        pl.col("NETCHGMH").sum().alias("NETCHGMH"),
        pl.col("NETCHGYR").sum().alias("NETCHGYR"),
    )
    .sort("BRANCH")
)

totals = report_data.select(
    [
        pl.col("OPENMH").sum().alias("OPENMH"),
        pl.col("OPENCUM").sum().alias("OPENCUM"),
        pl.col("CLOSEMH").sum().alias("CLOSEMH"),
        pl.col("CLOSECUM").sum().alias("CLOSECUM"),
        pl.col("NOACCT").sum().alias("NOACCT"),
        pl.col("NOCD").sum().alias("NOCD"),
        pl.col("CURBAL").sum().alias("CURBAL"),
        pl.col("NETCHGMH").sum().alias("NETCHGMH"),
        pl.col("NETCHGYR").sum().alias("NETCHGYR"),
    ]
)

RTS = 10
columns = [
    (["CURRENT MONTH", "OPENED"], 10),
    (["CUMULATIVE", "OPENED"], 10),
    (["CURRENT MONTH", "CLOSED"], 10),
    (["CUMULATIVE", "CLOSED"], 10),
    (["NO.OF", "ACCTS"], 10),
    (["NO.OF", "CDS"], 10),
    (["TOTAL (RM)", "O/S"], 18),
    (["NET CHANGE FOR", "THE MONTH"], 10),
    (["NET CHANGE YEAR", "TO DATE"], 10),
]
max_label_lines = max(len(labels) for labels, _ in columns)


with REPORT_PATH.open("w", encoding="utf-8") as out:
    out.write(f"{ASA_NEW_PAGE}PUBLIC BANK BERHAD\n")
    out.write(f"{ASA_SPACE}FD ACCOUNT OPENED/CLOSED FOR THE MONTH AS AT {RDATE}\n")

    for line_idx in range(max_label_lines):
        line = "BRANCH".rjust(RTS) if line_idx == 0 else (" " * RTS)
        for labels, width in columns:
            padded = [""] * (max_label_lines - len(labels)) + labels
            line += " " + padded[line_idx].rjust(width)
        out.write(f"{ASA_SPACE}{line}\n")

    for row in report_data.iter_rows(named=True):
        line = str(int(row["BRANCH"])) if row["BRANCH"] is not None else ""
        line = line.rjust(RTS)
        values = [
            row["OPENMH"],
            row["OPENCUM"],
            row["CLOSEMH"],
            row["CLOSECUM"],
            row["NOACCT"],
            row["NOCD"],
            row["CURBAL"],
            row["NETCHGMH"],
            row["NETCHGYR"],
        ]
        for idx, value in enumerate(values):
            line += " " + (fmt_comma18_2(value) if idx == 6 else fmt_comma10(value))
        out.write(f"{ASA_SPACE}{line}\n")

    t = totals.row(0, named=True)
    line = "TOTAL".rjust(RTS)
    total_vals = [
        t["OPENMH"],
        t["OPENCUM"],
        t["CLOSEMH"],
        t["CLOSECUM"],
        t["NOACCT"],
        t["NOCD"],
        t["CURBAL"],
        t["NETCHGMH"],
        t["NETCHGYR"],
    ]
    for idx, value in enumerate(total_vals):
        line += " " + (fmt_comma18_2(value) if idx == 6 else fmt_comma10(value))
    out.write(f"{ASA_SPACE}{line}\n")

print(f"Completed EIBMDPFQ conversion. Output report: {REPORT_PATH}")
