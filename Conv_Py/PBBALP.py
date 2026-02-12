#!/usr/bin/env python3
"""
Program: PBBALP
Purpose: Reproduce SAS PBBALP (Assets and Liabilities listing) from parquet inputs.

Key behavior mirrored from SAS:
- Build AL & REPTMON from ALW & REPTMON & NOWK (ITCODE, AMOUNT).
- Deduplicate ALW and AL definition datasets by key (NODUPKEYS semantics).
- Merge ALW amounts with AL definitions by ITCODE, defaulting missing AMOUNT to 0.
- Derive TYPE: 'A' for ITCODE starting with '3', else 'L'.
- Output sorted result and PROC PRINT-style report with ASA carriage control chars.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import polars as pl

# =============================================================================
# PATH / ENVIRONMENT SETUP
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

REPTMON = os.getenv("REPTMON", "")
NOWK = os.getenv("NOWK", "")
SDESC = os.getenv("SDESC", "")
RDATE = os.getenv("RDATE", "")

ALW_FILE = Path(os.getenv("ALW_FILE", str(INPUT_DIR / f"ALW{REPTMON}{NOWK}.parquet")))
AL_FILE = Path(os.getenv("AL_FILE", str(INPUT_DIR / "AL.parquet")))
AL_SOURCE_SAS = Path(os.getenv("AL_SOURCE_SAS", str("X_PBBALF")))

AL_OUTPUT_FILE = Path(os.getenv("AL_OUTPUT_FILE", str(OUTPUT_DIR / f"AL{REPTMON}.parquet")))
REPORT_FILE = Path(os.getenv("REPORT_FILE", str(OUTPUT_DIR / f"PBBALP_{REPTMON}.txt")))

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# REPORT CONSTANTS
# =============================================================================
ASA_NEW_PAGE = "1"
ASA_SPACE = " "
PAGE_LENGTH = 60
TYPE_LABELS = {"A": "ASSETS", "L": "CAPITAL AND LIABILITIES"}


def parse_al_from_sas_cards(sas_file: Path) -> pl.DataFrame:
    """Parse BNMCODE and DESC from X_PBBALF CARDS section (fixed positions)."""
    text = sas_file.read_text(encoding="utf-8", errors="ignore")
    lines = text.splitlines()

    in_cards = False
    rows: list[dict[str, str]] = []
    for line in lines:
        if not in_cards:
            if line.strip().upper() == "CARDS;":
                in_cards = True
            continue

        if line.strip() == ";":
            break
        if not line.strip() or line.strip().startswith("/"):
            continue

        bnmcode = line[0:14].strip()
        desc = line[24:68].strip()
        if bnmcode:
            rows.append({"BNMCODE": bnmcode, "DESC": desc})

    return pl.DataFrame(rows)


def read_al_definition() -> pl.DataFrame:
    """Load AL definition from parquet if available, else parse SAS source."""
    if AL_FILE.exists():
        al_df = pl.read_parquet(AL_FILE)
        needed = {"BNMCODE", "DESC"}
        missing = needed - set(al_df.columns)
        if missing:
            raise ValueError(f"AL parquet missing columns: {sorted(missing)}")
        return al_df.select(["BNMCODE", "DESC"])

    if not AL_SOURCE_SAS.exists():
        raise FileNotFoundError(
            "Neither AL parquet nor SAS include source found. "
            f"Checked: {AL_FILE} and {AL_SOURCE_SAS}"
        )

    return parse_al_from_sas_cards(AL_SOURCE_SAS)


def build_al_report_data(alw_df: pl.DataFrame, al_def_df: pl.DataFrame) -> pl.DataFrame:
    """Apply SAS data prep logic and return final AL&REPTMON output."""
    con = duckdb.connect()
    con.register("alw_src", alw_df.to_arrow())
    con.register("al_src", al_def_df.to_arrow())

    query = """
        WITH alw_dedup AS (
            SELECT ITCODE, AMOUNT
            FROM (
                SELECT ITCODE, AMOUNT,
                       ROW_NUMBER() OVER (PARTITION BY ITCODE ORDER BY ITCODE) AS rn
                FROM alw_src
            )
            WHERE rn = 1
        ),
        al_dedup AS (
            SELECT BNMCODE, DESC
            FROM (
                SELECT BNMCODE, DESC,
                       ROW_NUMBER() OVER (PARTITION BY BNMCODE ORDER BY BNMCODE) AS rn
                FROM al_src
            )
            WHERE rn = 1
        )
        SELECT
            d.BNMCODE AS ITCODE,
            d.DESC AS DESC,
            COALESCE(a.AMOUNT, 0) AS AMOUNT,
            CASE WHEN SUBSTR(d.BNMCODE, 1, 1) = '3' THEN 'A' ELSE 'L' END AS TYPE
        FROM al_dedup d
        LEFT JOIN alw_dedup a
          ON a.ITCODE = d.BNMCODE
        ORDER BY TYPE, ITCODE
    """

    out_df = pl.from_arrow(con.execute(query).fetch_arrow_table())
    con.close()
    return out_df


def fmt_amount(value: float | int | None) -> str:
    amount = 0.0 if value is None else float(value)
    return f"{amount:22,.2f}"


def write_report(df: pl.DataFrame, output_file: Path) -> None:
    """Generate PROC PRINT-style listing with ASA carriage control chars."""
    title_lines = [
        SDESC,
        "LISTING OF ASSETS AND LIABILITIES ITEMS FROM",
        "REPORT ON DOMESTIC ASSETS AND LIABILITIES PART I AND II",
        f"REPORT DATE : {RDATE}",
        "",
    ]

    col_header_1 = f"{'BNM CODE':<14} {'DESCRIPTION':<44} {'AMOUNT':>22}"
    col_header_2 = f"{'':<14} {'':<44} {'(SUM)':>22}"

    with output_file.open("w", encoding="utf-8") as handle:
        for group_index, group in enumerate(df.partition_by("TYPE", maintain_order=True)):
            type_code = group["TYPE"][0]
            type_label = TYPE_LABELS.get(type_code, type_code)

            line_count = 0

            def write_line(text: str, new_page: bool = False) -> None:
                nonlocal line_count
                if new_page:
                    handle.write(f"{ASA_NEW_PAGE}{text}\n")
                    line_count = 1
                else:
                    handle.write(f"{ASA_SPACE}{text}\n")
                    line_count += 1

            def write_page_header(force_new_page: bool) -> None:
                write_line(title_lines[0], new_page=force_new_page)
                for t in title_lines[1:]:
                    write_line(t)
                write_line(f"TYPE={type_label}")
                write_line("")
                write_line(col_header_1)
                write_line(col_header_2)
                write_line("-" * len(col_header_1))

            write_page_header(force_new_page=True if group_index == 0 else True)

            for row in group.iter_rows(named=True):
                if line_count >= PAGE_LENGTH:
                    write_page_header(force_new_page=True)
                detail_line = f"{row['ITCODE']:<14} {str(row['DESC'] or '')[:44]:<44} {fmt_amount(row['AMOUNT'])}"
                write_line(detail_line)

            sum_amount = group.select(pl.col("AMOUNT").sum()).item()
            if line_count >= PAGE_LENGTH:
                write_page_header(force_new_page=True)
            write_line("-" * len(col_header_1))
            write_line(f"{'SUM':<59} {fmt_amount(sum_amount)}")


def main() -> None:
    alw_df = pl.read_parquet(ALW_FILE)
    required_alw = {"ITCODE", "AMOUNT"}
    missing_alw = required_alw - set(alw_df.columns)
    if missing_alw:
        raise ValueError(f"ALW parquet missing columns: {sorted(missing_alw)}")

    al_def_df = read_al_definition()
    output_df = build_al_report_data(alw_df.select(["ITCODE", "AMOUNT"]), al_def_df)

    output_df.write_parquet(AL_OUTPUT_FILE)
    write_report(output_df, REPORT_FILE)

    print(f"Output dataset written: {AL_OUTPUT_FILE}")
    print(f"Report written: {REPORT_FILE}")
    print(f"Rows written: {output_df.height}")


if __name__ == "__main__":
    main()
