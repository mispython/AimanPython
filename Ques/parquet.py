#!/usr/bin/env python3
"""Display Parquet files from the command line.

This utility prints a preview of a .parquet file as a table and can also show
metadata such as columns, schema, row count, and column statistics. It uses
pandas with a Parquet engine such as pyarrow or fastparquet.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path
from typing import Iterable


DEFAULT_ROWS = 20


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


def _print_table(dataframe, rows: int) -> None:
    if rows < 0:
        raise ParquetViewerError("--rows must be 0 or greater")
    if rows == 0:
        return

    preview = dataframe.head(rows)
    if preview.empty:
        print("No rows to display.")
        return

    print(f"First {len(preview):,} row(s):")
    print(
        preview.to_string(
            index=False,
            max_cols=None,
            max_colwidth=40,
            line_width=_terminal_width(),
        )
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Display a .parquet file as a readable command-line table.",
        epilog=(
            "Examples:\n"
            "  python parquet_viewer.py data.parquet\n"
            "  python parquet_viewer.py data.parquet --rows 50 --schema\n"
            "  python parquet_viewer.py data.parquet --columns name,amount,date --stats"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("file", type=Path, help="Path to the .parquet file to display")
    parser.add_argument(
        "-n",
        "--rows",
        type=int,
        default=DEFAULT_ROWS,
        help=f"Number of rows to display (default: {DEFAULT_ROWS}; use 0 for metadata only)",
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

    try:
        _validate_file(args.file)
        columns = _parse_columns(args.columns)
        dataframe = _read_parquet(args.file, columns)
        _print_basic_info(args.file, dataframe)

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
