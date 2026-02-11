#!/usr/bin/env python3
"""
File Name: EIBWEEK2

Assumptions implemented:
- All input datasets are parquet.
- Input columns already follow SAS program column naming.
- Text outputs are fixed-width records (LRECL=80 equivalent for RDAL/NSRS).
- DuckDB is used for parquet access and row-level extraction.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import duckdb
import polars as pl


# =============================================================================
# PATH CONFIGURATION (defined early, per requirement)
# =============================================================================
BASE_PATH = Path("/data")
INPUT_PATH = BASE_PATH / "input"
OUTPUT_PATH = BASE_PATH / "output"
TEMP_PATH = OUTPUT_PATH / "temp"

# Input parquet files
WALALW_PATH = INPUT_PATH / "fiss" / "TXT_current.parquet"
LOAN_PATH = INPUT_PATH / "mniln" / "MNILN_current.parquet"
DEPOSIT_PATH = INPUT_PATH / "mnitb" / "MNITB_current.parquet"
FD_PATH = INPUT_PATH / "mnifd" / "MNIFD_current.parquet"
BNMTBL1_PATH = INPUT_PATH / "kapiti" / "KAPITI1.parquet"
BNMTBL3_PATH = INPUT_PATH / "kapiti" / "KAPITI3.parquet"

# Output files
ALW_OUTPUT = TEMP_PATH / "ALW.parquet"
ALWZ_OUTPUT = TEMP_PATH / "ALWZ.parquet"
RDAL_OUTPUT = OUTPUT_PATH / "RDAL WEEK2.TXT"
NSRS_OUTPUT = OUTPUT_PATH / "NSRS WEEK2.TXT"


@dataclass(frozen=True)
class ReportContext:
    reptdate: date
    nowk: str
    nowk1: str
    reptmon: str
    reptmon1: str
    reptyear: str
    reptyr: str
    reptday: str
    rdate: str
    eldate: str
    tdate: date
    sdate: str
    sdesc: str


def ensure_directories() -> None:
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    TEMP_PATH.mkdir(parents=True, exist_ok=True)


def calculate_context(today: date | None = None) -> ReportContext:
    """Mimic SAS DATA REPTDATE logic for week-2 run date (day=15)."""
    today = today or datetime.now().date()
    reptdate = date(today.year, today.month, 15)

    if reptdate.day == 8:
        sdd, wk, wk1 = 1, "1", "4"
    elif reptdate.day == 15:
        sdd, wk, wk1 = 9, "2", "1"
    elif reptdate.day == 22:
        sdd, wk, wk1 = 16, "3", "2"
    else:
        sdd, wk, wk1 = 23, "4", "3"

    mm = reptdate.month
    mm1 = 12 if wk == "1" and mm == 1 else (mm - 1 if wk == "1" else mm)
    sdate = date(reptdate.year, mm, sdd)

    return ReportContext(
        reptdate=reptdate,
        nowk=wk,
        nowk1=wk1,
        reptmon=f"{mm:02d}",
        reptmon1=f"{mm1:02d}",
        reptyear=f"{reptdate.year:04d}",
        reptyr=f"{reptdate.year % 100:02d}",
        reptday=f"{reptdate.day:02d}",
        rdate=reptdate.strftime("%d%m%Y"),
        eldate=f"{reptdate.day:05d}",
        tdate=reptdate,
        sdate=sdate.strftime("%d%m%Y"),
        sdesc="PUBLIC BANK BERHAD",
    )


def parse_to_ddmmyyyy(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%d%m%Y")
    if isinstance(value, date):
        return value.strftime("%d%m%Y")

    text = str(value).strip()
    if not text:
        return ""

    for fmt in ("%Y-%m-%d", "%d%m%Y", "%Y%m%d", "%y%m%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(text[: len(fmt.replace('%', '').replace('-', '').replace('/', ''))], fmt).strftime("%d%m%Y")
        except ValueError:
            continue

    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        for fmt in ("%d%m%Y", "%Y%m%d"):
            try:
                return datetime.strptime(digits[:8], fmt).strftime("%d%m%Y")
            except ValueError:
                continue
    if len(digits) >= 6:
        try:
            return datetime.strptime(digits[:6], "%y%m%d").strftime("%d%m%Y")
        except ValueError:
            return ""
    return ""


def first_reptdate_from_parquet(con: duckdb.DuckDBPyConnection, parquet_file: Path) -> str:
    if not parquet_file.exists():
        return ""
    row = con.execute(
        "SELECT REPTDATE FROM read_parquet(?) LIMIT 1",
        [str(parquet_file)],
    ).fetchone()
    return parse_to_ddmmyyyy(row[0] if row else None)


def validate_dates(con: duckdb.DuckDBPyConnection, ctx: ReportContext) -> tuple[bool, dict[str, str]]:
    checks = {
        "ALW": first_reptdate_from_parquet(con, WALALW_PATH),
        "LOAN": first_reptdate_from_parquet(con, LOAN_PATH),
        "DEPOSIT": first_reptdate_from_parquet(con, DEPOSIT_PATH),
        "FD": first_reptdate_from_parquet(con, FD_PATH),
        "KAPITI1": first_reptdate_from_parquet(con, BNMTBL1_PATH),
        "KAPITI3": first_reptdate_from_parquet(con, BNMTBL3_PATH),
    }

    is_valid = all(extracted == ctx.rdate for extracted in checks.values())
    return is_valid, checks


def execute_eigwrd1w(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Equivalent to %INC PGM(EIGWRD1W) staging outcome."""
    if not WALALW_PATH.exists():
        return pl.DataFrame()

    staged = con.execute("SELECT * FROM read_parquet(?)", [str(WALALW_PATH)]).pl()
    staged.write_parquet(ALW_OUTPUT)
    staged.write_parquet(ALWZ_OUTPUT)
    return staged


def collect_output_records(frame: pl.DataFrame) -> list[str]:
    """Build 80-byte records for RDAL/NSRS outputs."""
    if frame.is_empty():
        return []

    preferred_columns: Iterable[str] = (
        "RECORD",
        "LINE",
        "OUTREC",
        "RDAL_LINE",
        "NSRS_LINE",
        "TEXT",
    )
    for col in preferred_columns:
        if col in frame.columns:
            return [str(v)[:80].ljust(80) for v in frame.get_column(col).to_list()]

    # Deterministic fallback: semicolon text from all columns.
    records: list[str] = []
    cols = frame.columns
    for row in frame.iter_rows(named=True):
        line = ";".join("" if row[c] is None else str(row[c]) for c in cols)
        records.append(line[:80].ljust(80))
    return records


def write_fixed_text(path: Path, lines: Iterable[str]) -> None:
    with path.open("w", encoding="ascii", newline="\n") as handle:
        for line in lines:
            handle.write(f"{line}\n")


def execute_eigwrdal(ctx: ReportContext, staged: pl.DataFrame) -> None:
    """Equivalent to %INC PGM(EIGWRDAL): write RDAL and NSRS week-2 files."""
    header = f"RDAL{ctx.reptday}{ctx.reptmon}{ctx.reptyear}"[:80].ljust(80)
    data_lines = collect_output_records(staged)
    output_lines = [header, *data_lines] if data_lines else [header]

    write_fixed_text(RDAL_OUTPUT, output_lines)
    write_fixed_text(NSRS_OUTPUT, output_lines)


def run() -> int:
    ensure_directories()
    ctx = calculate_context()

    con = duckdb.connect(database=":memory:")
    try:
        valid, extracted = validate_dates(con, ctx)
        if not valid:
            if extracted["ALW"] != ctx.rdate:
                print(f"THE RDAL1 EXTRACTION IS NOT DATED {ctx.rdate}")
            if extracted["LOAN"] != ctx.rdate:
                print(f"THE LOAN EXTRACTION IS NOT DATED {ctx.rdate}")
            if extracted["DEPOSIT"] != ctx.rdate:
                print(f"THE DEPOSIT EXTRACTION IS NOT DATED {ctx.rdate}")
            if extracted["FD"] != ctx.rdate:
                print(f"THE FD EXTRACTION IS NOT DATED {ctx.rdate}")
            if extracted["KAPITI1"] != ctx.rdate:
                print(f"THE KAPITI1 EXTRACTION IS NOT DATED {ctx.rdate}")
            if extracted["KAPITI3"] != ctx.rdate:
                print(f"THE KAPITI3 EXTRACTION IS NOT DATED {ctx.rdate}")
            print("THE JOB IS NOT DONE !!")
            return 77

        staged = execute_eigwrd1w(con)
        execute_eigwrdal(ctx, staged)

        print("X_EIBWEEK2 processing completed successfully")
        print(f"RDAL output: {RDAL_OUTPUT}")
        print(f"NSRS output: {NSRS_OUTPUT}")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(run())
