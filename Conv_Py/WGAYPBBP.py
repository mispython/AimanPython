#!/usr/bin/env python3
"""
Program: WGAYPBBP

Assumptions:
- All SAS input datasets are already available as parquet files.
- Column names in parquet files follow the SAS program specifications.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import duckdb
import polars as pl

# =============================================================================
# CONFIGURATION (defined early as requested)
# =============================================================================
REPTMON = "202401"
NOWK = "01"
RDATE = "31/01/2024"
SDESC = "RGAC"
PAGE_LENGTH = 60

BASE_PATH = Path(__file__).resolve().parent
DATA_PATH = BASE_PATH / "data"
GAY_PATH = DATA_PATH / "gay"
GAY2_PATH = DATA_PATH / "gay2"
BNM_PATH = DATA_PATH / "bnm"
OUTPUT_PATH = BASE_PATH / "output"

OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
BNM_PATH.mkdir(parents=True, exist_ok=True)

GAY_OUT_PARQUET = BNM_PATH / f"gay{REPTMON}{NOWK}.parquet"
CAY_OUT_PARQUET = BNM_PATH / f"cay{REPTMON}{NOWK}.parquet"
RGAC_VARIANCE_REPORT = OUTPUT_PATH / f"wgaypbbp_variance_{REPTMON}{NOWK}.txt"
RGAC_SUMMARY_REPORT = OUTPUT_PATH / f"wgaypbbp_summary_{REPTMON}{NOWK}.txt"


WGAY_ROWS: list[tuple[str, str]] = [
    ("C-53912", "3912000000000G"), ("C-39110", "3911000000000G"),
    ("C-39610", "3961000000000G"), ("C-32110", "3211001000000G"),
    ("C-32120", "3212001000000G"), ("C-37200", "3720000000000G"),
    ("C-37250", "3725000000000G"), ("C-30309", "3030900000000G"),
    ("C-32140", "3214013000000G"), ("C-33140", "3314013000000G"),
    ("C-37500", "3750000000000G"), ("C-37030", "3703000000000G"),
    ("C-37040", "3704000000000G"), ("C-30325", "3032500000000G"),
    ("C-30327", "3032700000000G"), ("C-30331", "3033100000000G"),
    ("C-30333", "3033300000000G"), ("C-30339", "3033900000000G"),
    ("C-30342", "3034200000000G"), ("C-37070", "3707000000000G"),
    ("C-30355", "3035500000000G"), ("C-30357", "3035700000000G"),
    ("C-30359", "3035900000000G"), ("C-30369", "3036900000000G"),
    ("C-41101", "4110100000000G"), ("C-41102", "4110200000000G"),
    ("C-41103", "4110300000000G"), ("C-41104", "4110400000000G"),
    ("C-41105", "4110500000000G"), ("C-41107", "4110700000000G"),
    ("C-49160", "4916000000000G"), ("C-54120", "5412000000000G"),
    ("C-34152", "3415200000000G"), ("C-38250", "3825000000000G"),
    ("C-00000", "3000901000000G"), ("C-00001", "3000971000000G"),
    ("C-38200", "3820000000000G"), ("C-37270", "3727000000000G"),
    ("C-38270", "3827000000000G"), ("C-37280", "3728000000000G"),
    ("C-38280", "3828000000000G"), ("C-11111", "3000913000000G"),
    ("C-11112", "3000917000000G"), ("C-38500", "3850000000000G"),
    ("C-38030", "3803000000000G"), ("C-38040", "3804000000000G"),
    ("C-30311", "3031110000000G"), ("C-22222", "3000910000000G"),
]


@dataclass
class CycleInputs:
    rgr115: Path
    rgr913: Path
    rggl4000: Path
    rggl4100: Path
    rgr901: Path


def wgay_format_df() -> pl.DataFrame:
    return pl.DataFrame(
        {"START": [r[0] for r in WGAY_ROWS], "LABEL": [r[1] for r in WGAY_ROWS]}
    )


def compute_cycle_balances(conn: duckdb.DuckDBPyConnection, inp: CycleInputs, prefix: str) -> pl.DataFrame:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {prefix}_rgr115 AS
        SELECT SET_ID, USERCD
        FROM (
            SELECT SET_ID, USERCD,
                   ROW_NUMBER() OVER (PARTITION BY SET_ID ORDER BY USERCD) AS rn
            FROM read_parquet('{inp.rgr115.as_posix()}')
        )
        WHERE rn = 1
        """
    )

    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {prefix}_rgr913 AS
        SELECT t1.SET_ID, t1.USERCD, t2.ACCT_NO
        FROM {prefix}_rgr115 t1
        JOIN read_parquet('{inp.rgr913.as_posix()}') t2
          ON t1.SET_ID = t2.SET_ID
        """
    )

    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {prefix}_gaygl AS
        SELECT t1.SET_ID,
               t1.USERCD,
               SUBSTR(t2.ACCT_NO, 8, 5) AS ACK,
               SUM(t2.GLAMT) AS GLAMT
        FROM {prefix}_rgr913 t1
        JOIN read_parquet('{inp.rggl4000.as_posix()}') t2
          ON t1.ACCT_NO = t2.ACCT_NO
        WHERE t2.ACKIND = 'N'
        GROUP BY 1, 2, 3
        """
    )

    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {prefix}_rggl4100 AS
        SELECT ACCT_NO, ACKIND, SUM(JEAMT) AS JEAMT
        FROM read_parquet('{inp.rggl4100.as_posix()}')
        GROUP BY 1, 2
        """
    )

    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {prefix}_gayje AS
        SELECT t1.SET_ID,
               t1.USERCD,
               SUBSTR(t2.ACCT_NO, 8, 5) AS ACK,
               SUM(t2.JEAMT) AS JEAMT
        FROM {prefix}_rgr913 t1
        JOIN {prefix}_rggl4100 t2
          ON t1.ACCT_NO = t2.ACCT_NO
        WHERE t2.ACKIND = 'N'
        GROUP BY 1, 2, 3
        """
    )

    return conn.sql(
        f"""
        SELECT
            COALESCE(g.SET_ID, j.SET_ID) AS SET_ID,
            COALESCE(g.USERCD, j.USERCD) AS USERCD,
            COALESCE(g.ACK, j.ACK) AS ACK,
            CASE
                WHEN COALESCE(g.USERCD, j.USERCD) = '6666' THEN
                    CASE WHEN COALESCE(g.GLAMT, 0) + COALESCE(j.JEAMT, 0) < 0 THEN 0
                         ELSE COALESCE(g.GLAMT, 0) + COALESCE(j.JEAMT, 0) END
                WHEN COALESCE(g.USERCD, j.USERCD) = '7777' THEN
                    CASE WHEN COALESCE(g.GLAMT, 0) + COALESCE(j.JEAMT, 0) > 0 THEN 0
                         ELSE COALESCE(g.GLAMT, 0) + COALESCE(j.JEAMT, 0) END
                WHEN COALESCE(g.USERCD, j.USERCD) = '8888' THEN
                    CASE WHEN COALESCE(g.GLAMT, 0) + COALESCE(j.JEAMT, 0) > 0 THEN 0
                         ELSE -(COALESCE(g.GLAMT, 0) + COALESCE(j.JEAMT, 0)) END
                WHEN COALESCE(g.USERCD, j.USERCD) = '9999' THEN
                    -(COALESCE(g.GLAMT, 0) + COALESCE(j.JEAMT, 0))
                ELSE COALESCE(g.GLAMT, 0) + COALESCE(j.JEAMT, 0)
            END AS BAL
        FROM {prefix}_gaygl g
        FULL OUTER JOIN {prefix}_gayje j
          ON g.SET_ID = j.SET_ID
         AND g.USERCD = j.USERCD
         AND g.ACK = j.ACK
        ORDER BY 1, 2, 3
        """
    ).pl()


def build_gay_and_cay(curr_bal: pl.DataFrame, wgay_df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    set_amount = curr_bal.group_by("SET_ID").agg(pl.col("BAL").sum().alias("AMOUNT"))

    gay = (
        set_amount
        .join(wgay_df.rename({"START": "SET_ID", "LABEL": "BNMCODE"}), on="SET_ID", how="left")
        .group_by("BNMCODE")
        .agg(pl.col("AMOUNT").sum().alias("AMOUNT"))
        .rename({"BNMCODE": "ITCODE"})
    )

    all_itcodes = wgay_df.select(pl.col("LABEL").alias("ITCODE")).unique()
    gay_complete = (
        all_itcodes.join(gay, on="ITCODE", how="left")
        .with_columns(
            pl.lit("E").alias("FLAG"),
            pl.col("AMOUNT").fill_null(0.0),
        )
        .select(["ITCODE", "FLAG", "AMOUNT"])
        .sort("ITCODE")
    )
    return gay_complete, gay_complete.clone()


def format_number(value: float, width: int = 18, decimals: int = 2) -> str:
    return f"{value:>{width},.{decimals}f}"


def _variance_header(page_no: int) -> list[str]:
    return [
        "1" + f"PROGRAM-ID : WGAYPBBP{'':<20}{SDESC:>35}PAGE NO.: {page_no}",
        " " + f"{'':<38}VARIANCE REPORT FOR RGAC FOR {RDATE}",
        " ",
        " " + "SET ID       USERCD   ACK     DESCRIPTION                            CURRENT BALANCE   PREVIOUS BALANCE            VARIANCE",
        " " + "----------------------------------------" + "----------------------------------------" + "----------------------------------------" + "------------",
        " ",
    ]


def write_variance_report(totv: pl.DataFrame, path: Path, page_len: int = PAGE_LENGTH) -> None:
    lines: list[str] = []
    page_no = 0
    line_in_page = page_len

    for set_id, group in totv.group_by("SET_ID", maintain_order=True):
        if line_in_page >= page_len:
            page_no += 1
            hdr = _variance_header(page_no)
            lines.extend(hdr)
            line_in_page = len(hdr)

        setamt1 = setamt2 = setamt3 = 0.0
        for row in group.sort(["USERCD", "ACK"]).iter_rows(named=True):
            variance = float(row["CURBAL"] or 0.0) - float(row["PREVBAL"] or 0.0)
            setamt1 += float(row["CURBAL"] or 0.0)
            setamt2 += float(row["PREVBAL"] or 0.0)
            setamt3 += variance
            desc = (row.get("DESC") or "")[:30]
            line = (
                " "
                + f"{str(row['SET_ID']):<12}{str(row['USERCD']):<8}{str(row['ACK']):<8}"
                + f"{desc:<30}"
                + f"{format_number(float(row['CURBAL'] or 0.0), 18)}"
                + f"{format_number(float(row['PREVBAL'] or 0.0), 18)}"
                + f"{format_number(variance, 18)}"
            )
            lines.append(line)
            line_in_page += 1

        lines.append(" " + f"{'':<58}{'-'*39}{'-'*21}")
        lines.append(
            " "
            + f"{'':<58}{format_number(setamt1,18)}{format_number(setamt2,18)}{format_number(setamt3,18)}"
        )
        lines.append(" " + f"{'':<58}{'='*39}{'='*21}")
        lines.extend([" ", " ", " "])
        line_in_page += 6

    path.write_text("\n".join(lines), encoding="utf-8")


def write_summary_report(gay: pl.DataFrame, path: Path) -> None:
    lines = [
        "1PUBLIC BANK BERHAD",
        " REPORT ON GLOBAL ASSETS AND CAPITAL",
        f" REPORT DATE : {RDATE}",
        " ",
        " ITCODE                AMOUNT",
    ]
    for row in gay.iter_rows(named=True):
        lines.append(f" {row['ITCODE']:<20}{row['AMOUNT']:>25,.2f}")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    wgay_df = wgay_format_df()

    curr = CycleInputs(
        rgr115=GAY_PATH / "RGR115.parquet",
        rgr913=GAY_PATH / "RGR913.parquet",
        rggl4000=GAY_PATH / "RGGL4000.parquet",
        rggl4100=GAY_PATH / "RGGL4100.parquet",
        rgr901=GAY_PATH / "RGR901.parquet",
    )
    prev = CycleInputs(
        rgr115=GAY2_PATH / "RGR115.parquet",
        rgr913=GAY2_PATH / "RGR913.parquet",
        rggl4000=GAY2_PATH / "RGGL4000.parquet",
        rggl4100=GAY2_PATH / "RGGL4100.parquet",
        rgr901=GAY2_PATH / "RGR901.parquet",
    )

    conn = duckdb.connect()

    curr_bal = compute_cycle_balances(conn, curr, "c")
    prev_bal = compute_cycle_balances(conn, prev, "p")

    gay, cay = build_gay_and_cay(curr_bal, wgay_df)
    gay.write_parquet(GAY_OUT_PARQUET)
    cay.write_parquet(CAY_OUT_PARQUET)

    rgr901_curr = pl.read_parquet(curr.rgr901).rename({"SEG_VAL": "ACK", "SEG_DESC": "DESC"}).unique("ACK")
    rgr901_prev = pl.read_parquet(prev.rgr901).rename({"SEG_VAL": "ACK", "SEG_DESC": "DESC"}).unique("ACK")

    currgay = curr_bal.join(rgr901_curr.select(["ACK", "DESC"]), on="ACK", how="left")
    prevgay = prev_bal.join(rgr901_prev.select(["ACK", "DESC"]), on="ACK", how="left")

    totv = (
        currgay.join(prevgay, on=["SET_ID", "USERCD", "ACK"], how="full", suffix="_P")
        .with_columns([
            pl.coalesce([pl.col("SET_ID"), pl.col("SET_ID_P")]).alias("SET_ID_OUT"),
            pl.coalesce([pl.col("USERCD"), pl.col("USERCD_P")]).alias("USERCD_OUT"),
            pl.coalesce([pl.col("ACK"), pl.col("ACK_P")]).alias("ACK_OUT"),
            pl.coalesce([pl.col("DESC"), pl.col("DESC_P")]).alias("DESC_OUT"),
            pl.col("BAL").fill_null(0.0).alias("CURBAL"),
            pl.col("BAL_P").fill_null(0.0).alias("PREVBAL"),
        ])
        .select([
            pl.col("SET_ID_OUT").alias("SET_ID"),
            pl.col("USERCD_OUT").alias("USERCD"),
            pl.col("ACK_OUT").alias("ACK"),
            pl.col("DESC_OUT").alias("DESC"),
            "CURBAL",
            "PREVBAL",
        ])
        .sort(["SET_ID", "USERCD", "ACK"])
    )

    write_variance_report(totv, RGAC_VARIANCE_REPORT)
    write_summary_report(gay, RGAC_SUMMARY_REPORT)

    print(f"Wrote {GAY_OUT_PARQUET}")
    print(f"Wrote {CAY_OUT_PARQUET}")
    print(f"Wrote {RGAC_SUMMARY_REPORT}")
    print(f"Wrote {RGAC_VARIANCE_REPORT}")


if __name__ == "__main__":
    main()
