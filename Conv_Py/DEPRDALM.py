#!/usr/bin/env python3
"""
Program : DEPRDALM.py
Purpose : Report on Islamic RDAL (M&I) - Deposits
          Reads weekly (DALW/FALW) and monthly (DALM/FALM) BNM deposit data,
          filters for Islamic indicator ('I'), summarises by BNMCODE & AMTIND,
          then prints two reports (Part I = weekly, Part II = monthly).
"""

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

import sys
from pathlib import Path

import duckdb
import polars as pl

# Input parquet directory (BNM library equivalent)
BNM_DIR     = Path("data/BNM")

# Output report file
OUTPUT_DIR  = Path("output")
OUTPUT_FILE = OUTPUT_DIR / "DEPRDALM.txt"

# Runtime parameters – set these before execution
REPTMON = "202412"   # Reporting month  (e.g. "202412")
NOWK    = "01"       # Week suffix      (e.g. "01")
RDATE   = "31DEC2024"

# Page length for ASA carriage-control report
PAGE_LINES = 60

# ============================================================================
# HELPERS
# ============================================================================

def _parquet(name: str) -> Path:
    """Return full path to a BNM parquet file."""
    return BNM_DIR / f"{name}{REPTMON}{NOWK}.parquet"


def _load_and_filter(paths: list[Path]) -> pl.DataFrame:
    """
    Load one or more parquet files, concatenate, and filter AMTIND == 'I'.
    Uses DuckDB for efficient parquet reading.
    """
    frames = []
    con = duckdb.connect()
    for p in paths:
        sql = f"SELECT * FROM read_parquet('{p}') WHERE AMTIND = 'I'"
        frames.append(con.execute(sql).pl())
    con.close()
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames)


def _summarise(df: pl.DataFrame, group_cols: list[str]) -> pl.DataFrame:
    """Group by specified columns and sum AMOUNT."""
    if df.is_empty():
        return df
    return (
        df.group_by(group_cols)
          .agg(pl.col("AMOUNT").sum())
          .sort(group_cols)
    )


def _format_amount(value: float) -> str:
    """Format amount as COMMA25.2 (right-aligned in 25 chars with 2 decimal places)."""
    formatted = f"{value:,.2f}"
    return formatted.rjust(25)


def _build_report(df: pl.DataFrame, title1: str, title2: str, title3: str,
                  page_lines: int = PAGE_LINES) -> str:
    """
    Build a text report with ASA carriage-control characters.

    ASA codes used:
        '1' = skip to new page (first line of page)
        ' ' = single space (advance one line before printing)
    """
    lines = []

    def new_page(line_text: str) -> str:
        return "1" + line_text

    def body_line(line_text: str) -> str:
        return " " + line_text

    # --- Page header ---
    lines.append(new_page(title1))
    lines.append(body_line(title2))
    lines.append(body_line(title3))
    lines.append(body_line(""))

    # --- Column header ---
    header = f"{'OBS':<6} {'BNMCODE':<10} {'AMTIND':<8} {'AMOUNT':>25}"
    lines.append(body_line(header))
    lines.append(body_line("-" * len(header)))

    # --- Data rows ---
    current_line = 6   # already used: 3 titles + blank + header + separator
    obs = 1
    page_num = 1

    for row in df.iter_rows(named=True):
        if current_line >= page_lines:
            # New page: reprint header block
            lines.append(new_page(title1))
            lines.append(body_line(title2))
            lines.append(body_line(title3))
            lines.append(body_line(""))
            lines.append(body_line(header))
            lines.append(body_line("-" * len(header)))
            current_line = 6
            page_num += 1

        bnmcode = str(row.get("BNMCODE", "")).strip()
        amtind  = str(row.get("AMTIND",  "")).strip()
        amount  = row.get("AMOUNT", 0.0) or 0.0

        data_line = f"{obs:<6} {bnmcode:<10} {amtind:<8} {_format_amount(amount)}"
        lines.append(body_line(data_line))
        current_line += 1
        obs += 1

    return "\n".join(lines) + "\n"


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # -------------------------------------------------------------------------
    # RDAL1 : Weekly deposit data (DALW + FALW), AMTIND = 'I'
    # Summarise by BNMCODE, AMTIND
    # -------------------------------------------------------------------------
    rdal1_raw = _load_and_filter([
        _parquet("DALW"),
        _parquet("FALW"),
    ])
    rdal1 = _summarise(rdal1_raw, ["BNMCODE", "AMTIND"])

    # -------------------------------------------------------------------------
    # RDAL2 : Monthly deposit data (DALM + FALM), AMTIND = 'I'
    # Summarise by BNMCODE, AMTIND
    # -------------------------------------------------------------------------
    rdal2_raw = _load_and_filter([
        _parquet("DALM"),
        _parquet("FALM"),
    ])
    rdal2 = _summarise(rdal2_raw, ["BNMCODE", "AMTIND"])

    # -------------------------------------------------------------------------
    # Build reports
    # -------------------------------------------------------------------------
    report_part1 = _build_report(
        df     = rdal1,
        title1 = "PUBLIC BANK BERHAD",
        title2 = "REPORT ON ISLAMIC RDAL (M&I) PART I  ",
        title3 = f"REPORT DATE : {RDATE}",
    )

    report_part2 = _build_report(
        df     = rdal2,
        title1 = "PUBLIC BANK BERHAD",
        title2 = "REPORT ON ISLAMIC RDAL (M&I) PART II ",
        title3 = f"REPORT DATE : {RDATE}",
    )

    # -------------------------------------------------------------------------
    # Write output file
    # -------------------------------------------------------------------------
    with open(OUTPUT_FILE, "w", encoding="utf-8") as fh:
        fh.write(report_part1)
        fh.write(report_part2)

    print(f"Report written to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
