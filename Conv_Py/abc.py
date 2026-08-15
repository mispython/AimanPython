#!/usr/bin/env python3
"""Standalone Parquet file viewer.

Put your Parquet file path in PARQUET_FILE_PATH below, then run:

    python parquet_viewer.py

The program reads that .parquet file and displays every row and every column.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path
from typing import Iterable

# ---------------------------------------------------------------------------
# PUT YOUR PARQUET FILE PATH HERE
# ---------------------------------------------------------------------------
# Examples:
#   PARQUET_FILE_PATH = r"C:\Users\you\Downloads\report.parquet"
#   PARQUET_FILE_PATH = "/home/you/downloads/report.parquet"
#   PARQUET_FILE_PATH = "./report.parquet"
# PARQUET_FILE_PATH = Path("/stgsrcsys/host/uat/AII/STG_DP_DPTRBLGS.parquet/part.0.parquet")
PARQUET_FILE_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDEPDP/BNMCC/DPCC06.parquet")

# None means display the whole file. Set to a number such as 100 if you only
# want to display the first 10 rows.
ROWS_TO_DISPLAY: int | 10 = 10


class ParquetViewerError(Exception):
    """Raised when the Parquet viewer cannot complete the requested action."""


def _load_pandas():
    """Import pandas lazily so help text still works without dependencies."""
    try:
        import pandas as pd
    except ImportError as exc:
        raise ParquetViewerError(
            "Missing dependency: pandas. Install pandas and a Parquet engine, "
            "for example: python -m pip install pandas pyarrow"
        ) from exc
    return pd


def _read_parquet(path: Path, columns: list[str] | None):
    pd = _load_pandas()
    try:
        return pd.read_parquet(path, columns=columns)
    except ImportError as exc:
        raise ParquetViewerError(
            "Missing Parquet engine. Install pyarrow or fastparquet, for example: "
            "python -m pip install pyarrow"
        ) from exc
    except Exception as exc:  # pandas raises several typed exceptions here.
        raise ParquetViewerError(f"Unable to read '{path}': {exc}") from exc


def _terminal_width() -> int:
    return shutil.get_terminal_size(fallback=(120, 24)).columns


def _validate_file(path: Path) -> None:
    if str(path) == "PUT_YOUR_PARQUET_FILE_PATH_HERE.parquet":
        raise ParquetViewerError(
            "Open parquet_viewer.py and replace PARQUET_FILE_PATH with your real .parquet file path."
        )
    if not path.exists():
        raise ParquetViewerError(f"File does not exist: {path}")
    if not path.is_file():
        raise ParquetViewerError(f"Path is not a file: {path}")


def _parse_columns(columns: str | None) -> list[str] | None:
    if not columns:
        return None
    parsed = [column.strip() for column in columns.split(",") if column.strip()]
    if not parsed:
        raise ParquetViewerError("--columns must include at least one column name")
    return parsed


def _print_basic_info(path: Path, dataframe) -> None:
    print(f"File: {path}")
    print(f"Rows: {len(dataframe):,}")
    print(f"Columns: {len(dataframe.columns):,}")
    print()


def _print_columns(columns: Iterable[str]) -> None:
    print("Columns:")
    for column in columns:
        print(f"  - {column}")
    print()


def _print_schema(dataframe) -> None:
    print("Schema:")
    for column, dtype in dataframe.dtypes.items():
        print(f"  {column}: {dtype}")
    print()


def _print_stats(dataframe) -> None:
    print("Statistics:")
    if dataframe.empty:
        print("  No rows to summarize.")
    else:
        print(dataframe.describe(include="all").transpose().to_string())
    print()


def _print_table(dataframe, rows: int | None) -> None:
    if rows is not None and rows < 0:
        raise ParquetViewerError("--rows must be 0 or greater")
    if rows == 0:
        return

    display_dataframe = dataframe if rows is None else dataframe.head(rows)
    if display_dataframe.empty:
        print("No rows to display.")
        return

    if rows is None:
        print(f"All {len(display_dataframe):,} row(s):")
    else:
        print(f"First {len(display_dataframe):,} row(s):")

    with _load_pandas().option_context(
        "display.max_rows",
        None,
        "display.max_columns",
        None,
        "display.max_colwidth",
        None,
        "display.width",
        _terminal_width(),
    ):
        print(display_dataframe.to_string(index=False, max_rows=None, max_cols=None, max_colwidth=None))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Display the .parquet file path configured inside parquet_viewer.py.",
        epilog=(
            "Standalone use:\n"
            "  1. Open parquet_viewer.py.\n"
            "  2. Replace PARQUET_FILE_PATH with your .parquet file path.\n"
            "  3. Run: python parquet_viewer.py\n\n"
            "Optional override:\n"
            "  python parquet_viewer.py /path/to/data.parquet"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "file",
        nargs="?",
        metavar="PARQUET_FILE",
        type=Path,
        help="Optional override. If omitted, the program uses PARQUET_FILE_PATH inside this file.",
    )
    parser.add_argument(
        "-n",
        "--rows",
        type=int,
        default=ROWS_TO_DISPLAY,
        help="Number of rows to display. Default displays the whole file. Use 0 for metadata only.",
    )
    parser.add_argument(
        "-c",
        "--columns",
        help="Comma-separated list of columns to load and display",
    )
    parser.add_argument("--schema", action="store_true", help="Display column names and data types")
    parser.add_argument("--list-columns", action="store_true", help="Display column names")
    parser.add_argument("--stats", action="store_true", help="Display summary statistics")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    parquet_file = args.file or Path(PARQUET_FILE_PATH)

    try:
        _validate_file(parquet_file)
        columns = _parse_columns(args.columns)
        dataframe = _read_parquet(parquet_file, columns)
        _print_basic_info(parquet_file, dataframe)

        if args.list_columns:
            _print_columns(dataframe.columns)
        if args.schema:
            _print_schema(dataframe)
        if args.stats:
            _print_stats(dataframe)

        _print_table(dataframe, args.rows)
    except ParquetViewerError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
