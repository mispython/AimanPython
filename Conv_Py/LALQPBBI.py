#!/usr/bin/env python3
"""
Program : LALQPBBI.py (ISLAMIC)
Report  : ISLAMIC DOMESTIC ASSETS AND LIABILITIES PART III - M&I LOAN

Reads the pre-built BNM.LALQ dataset, filters to Islamic indicator (AMTIND='I'),
and produces a PROC PRINT equivalent report with ASA carriage control characters.

No transformation logic — this is a pure reporting program over an existing dataset.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
BASE_DIR   = Path(__file__).resolve().parent
INPUT_DIR  = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Macro variable equivalents
# &REPTMON — reporting month string e.g. "200509"
# &NOWK    — week suffix string    e.g. ""
# &RDATE   — reporting date string e.g. "30/09/05"
REPTMON: str = "200509"
NOWK:    str = ""
RDATE:   str = "30/09/05"

# Input parquet: BNM.LALQ&REPTMON&NOWK
LALQ_PARQUET  = INPUT_DIR  / f"LALQ{REPTMON}{NOWK}.parquet"
REPORT_OUTPUT = OUTPUT_DIR / f"LALQPBBI_{REPTMON}{NOWK}.txt"

# Page layout for ASA report
PAGE_LENGTH = 60


# ---------------------------------------------------------------------------
# Helper — parse reporting date string
# ---------------------------------------------------------------------------
def parse_rdate(rdate_str: str) -> date:
    """Parse &RDATE in DDMMYY8. or DD/MM/YY format to Python date."""
    from datetime import datetime
    for fmt in ("%d/%m/%y", "%d/%m/%Y", "%d%m%y", "%d%m%Y"):
        try:
            return datetime.strptime(rdate_str.strip(), fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Cannot parse RDATE: '{rdate_str}'")


# ---------------------------------------------------------------------------
# Report writer — PROC PRINT equivalent with ASA carriage control
# ---------------------------------------------------------------------------
def write_report(df: pl.DataFrame, reptdate: date, output_path: Path) -> None:
    """
    OPTIONS NOCENTER NODATE NONUMBER;
    TITLE1 'PUBLIC BANK BERHAD';
    TITLE2 'ISLAMIC DOMESTIC ASSETS AND LIABILITIES PART III - M&I LOAN';
    TITLE3 'REPORT DATE : ' &RDATE;
    PROC PRINT DATA=BNM.LALQ&REPTMON&NOWK;
    WHERE AMTIND='I';
    FORMAT AMOUNT COMMA25.2;

    ASA carriage control: '1' = new page, ' ' = single space.
    Page length: 60 lines.
    """
    title1 = "PUBLIC BANK BERHAD"
    title2 = "ISLAMIC DOMESTIC ASSETS AND LIABILITIES PART III - M&I LOAN"
    title3 = f"REPORT DATE : {reptdate.strftime('%d/%m/%y')}"

    # Determine printable columns (all except AMTIND filter col, keep in output)
    # SAS PROC PRINT prints all variables; AMOUNT uses COMMA25.2
    # We'll render all columns present in the DataFrame
    cols = df.columns

    col_widths: dict[str, int] = {}
    for c in cols:
        col_widths[c] = max(len(c), 12)
    if "AMOUNT" in col_widths:
        col_widths["AMOUNT"] = 25

    header_sep = " " + "  ".join("-" * col_widths[c] for c in cols)
    col_header = " " + "  ".join(c.ljust(col_widths[c]) for c in cols)

    header_lines = [
        f" {title1}",
        f" {title2}",
        f" {title3}",
        " ",
        col_header,
        header_sep,
    ]

    HEADER_ROWS = len(header_lines)
    DATA_ROWS_PER_PAGE = PAGE_LENGTH - HEADER_ROWS

    lines: list[str] = []
    for row in df.iter_rows(named=True):
        parts = []
        for c in cols:
            val = row.get(c)
            w   = col_widths[c]
            if c == "AMOUNT":
                # FORMAT AMOUNT COMMA25.2
                try:
                    formatted = f"{float(val):>{w},.2f}"
                except (TypeError, ValueError):
                    formatted = "".rjust(w)
            else:
                formatted = str(val or "").ljust(w)
            parts.append(formatted)
        lines.append(" " + "  ".join(parts))

    with open(output_path, "w", encoding="ascii", errors="replace") as f:
        i = 0
        total = len(lines)

        while True:
            # Write page header; first line gets ASA '1' (form feed / new page)
            for h_idx, hline in enumerate(header_lines):
                asa = "1" if h_idx == 0 else " "
                f.write(asa + hline + "\n")

            # Write data rows for this page
            page_slice = lines[i:i + DATA_ROWS_PER_PAGE]
            for dline in page_slice:
                f.write(" " + dline + "\n")

            i += DATA_ROWS_PER_PAGE
            if i >= total:
                break


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    reptdate = parse_rdate(RDATE)

    # Read BNM.LALQ dataset
    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{LALQ_PARQUET}')").pl()
    con.close()

    # WHERE AMTIND='I'
    if "AMTIND" in df.columns:
        df = df.filter(pl.col("AMTIND") == "I")

    # PROC PRINT report
    write_report(df, reptdate, REPORT_OUTPUT)
    print(f"Report : {REPORT_OUTPUT}  ({df.height} rows)")


if __name__ == "__main__":
    main()
