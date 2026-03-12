#!/usr/bin/env python3
"""
Program : LOANRDAL.py
Purpose : Report on Islamic RDAL (Loans)
          Reads weekly (LALW) and monthly (LALM) BNM loan data,
          filters for Islamic indicator ('I'), summarises by BNMCODE,
          then prints two reports (Part I = weekly, Part II = monthly).
"""

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

from pathlib import Path

import duckdb
import polars as pl

# Input parquet directory (BNM library equivalent)
BNM_DIR     = Path("data/BNM")

# Output report file
OUTPUT_DIR  = Path("output")
OUTPUT_FILE = OUTPUT_DIR / "LOANRDAL.txt"

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


def _load_and_filter(path: Path) -> pl.DataFrame:
    """
    Load a single parquet file and filter AMTIND == 'I'.
    Uses DuckDB for efficient parquet reading.
    """
    con = duckdb.connect()
    sql = f"SELECT * FROM read_parquet('{path}') WHERE AMTIND = 'I'"
    df  = con.execute(sql).pl()
    con.close()
    return df


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
                  title4: str = "", page_lines: int = PAGE_LINES) -> str:
    """
    Build a text report with ASA carriage-control characters.

    ASA codes used:
        '1' = skip to new page (first line of page)
        ' ' = single space (advance one line before printing)

    Note: TITLE4 is blank in the original SAS (TITLE4;) – included for
    structural fidelity and kept as empty by default.
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
    lines.append(body_line(title4))   # blank TITLE4
    lines.append(body_line(""))

    # --- Column header ---
    # RDAL1 is summarised by BNMCODE only (no AMTIND in CLASS for RDAL1)
    has_amtind = "AMTIND" in df.columns
    if has_amtind:
        header = f"{'OBS':<6} {'BNMCODE':<10} {'AMTIND':<8} {'AMOUNT':>25}"
    else:
        header = f"{'OBS':<6} {'BNMCODE':<10} {'AMOUNT':>25}"

    lines.append(body_line(header))
    lines.append(body_line("-" * len(header)))

    # --- Data rows ---
    current_line = 7   # titles(4) + blank + header + separator
    obs = 1

    for row in df.iter_rows(named=True):
        if current_line >= page_lines:
            # New page: reprint header block
            lines.append(new_page(title1))
            lines.append(body_line(title2))
            lines.append(body_line(title3))
            lines.append(body_line(title4))
            lines.append(body_line(""))
            lines.append(body_line(header))
            lines.append(body_line("-" * len(header)))
            current_line = 7

        bnmcode = str(row.get("BNMCODE", "")).strip()
        amount  = row.get("AMOUNT", 0.0) or 0.0

        if has_amtind:
            amtind    = str(row.get("AMTIND", "")).strip()
            data_line = f"{obs:<6} {bnmcode:<10} {amtind:<8} {_format_amount(amount)}"
        else:
            data_line = f"{obs:<6} {bnmcode:<10} {_format_amount(amount)}"

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
    # RDAL1 : Weekly loan data (LALW), AMTIND = 'I'
    # Summarise by BNMCODE only (CLASS BNMCODE; – no AMTIND in PROC SUMMARY)
    # -------------------------------------------------------------------------
    rdal1_raw = _load_and_filter(_parquet("LALW"))
    rdal1     = _summarise(rdal1_raw, ["BNMCODE"])

    # -------------------------------------------------------------------------
    # RDAL2 : Monthly loan data (LALM), AMTIND = 'I'
    # No PROC SUMMARY in the original for RDAL2 – printed as-is after filter.
    # -------------------------------------------------------------------------
    rdal2 = _load_and_filter(_parquet("LALM"))

    # -------------------------------------------------------------------------
    # Build reports
    # -------------------------------------------------------------------------
    report_part1 = _build_report(
        df     = rdal1,
        title1 = "PUBLIC BANK  BERHAD",
        title2 = "REPORT ON ISLAMIC RDAL (LOANS) PART I  ",
        title3 = f"REPORT DATE : {RDATE}",
        title4 = "",
    )

    report_part2 = _build_report(
        df     = rdal2,
        title1 = "PUBLIC BANK BERHAD",
        title2 = "REPORT ON ISLAMIC RDAL (LOANS) PART II ",
        title3 = f"REPORT DATE : {RDATE}",
        title4 = "",
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
