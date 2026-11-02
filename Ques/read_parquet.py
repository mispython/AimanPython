#!/usr/bin/env python3
"""
inspect_parquet.py

Read a Parquet file (or a folder of *.parquet files) and print:
  - file metadata (rows, columns, row groups, size)
  - every column's name, Arrow type, pandas dtype, nullability
  - the first N sample rows

The input path is hardcoded in INPUT_PATH below. You can still override it
from the command line if you want:

    python inspect_parquet.py
    python inspect_parquet.py other.parquet
    python inspect_parquet.py --rows 10 --columns id,price

Requirements: pip install pyarrow pandas
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    import pyarrow.parquet as pq
except ImportError:
    sys.exit("pyarrow is required:  pip install pyarrow")

try:
    import pandas as pd
except ImportError:
    pd = None  # fall back to a plain dict dump


# =========================================================================== #
#  CONFIGURATION — edit these
# =========================================================================== #
INPUT_PATH      = r"C:\data\sales.parquet"   # <- your file OR a folder of *.parquet
DEFAULT_ROWS    = 5                          # sample rows to show (0 = none)
DEFAULT_COLUMNS = None                       # e.g. ["id", "price"], or None = all
# =========================================================================== #


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def human_bytes(n: int) -> str:
    size = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{int(size)} B" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{n} B"


def pandas_dtype(arrow_type) -> str:
    """Best-effort mapping of a pyarrow type to a pandas/numpy dtype string."""
    try:
        return str(arrow_type.to_pandas_dtype())
    except Exception:            # nested / dictionary / unsupported types
        return "-"


# --------------------------------------------------------------------------- #
# inspection
# --------------------------------------------------------------------------- #
def inspect(path: Path, nrows: int, columns: list[str] | None) -> None:
    pf = pq.ParquetFile(path)          # reads only the footer — fast
    md = pf.metadata
    schema = pf.schema_arrow

    all_names = [f.name for f in schema]
    if columns:
        missing = [c for c in columns if c not in all_names]
        if missing:
            sys.exit(f"{path}: unknown column(s): {', '.join(missing)}")
        fields = [f for f in schema if f.name in columns]
    else:
        fields = list(schema)

    # ---- header ---------------------------------------------------------- #
    print("=" * 78)
    print(f"File        : {path}")
    print(f"Rows        : {md.num_rows:,}")
    print(f"Columns     : {md.num_columns}")
    print(f"Row groups  : {md.num_row_groups}")
    print(f"Created by  : {md.created_by or 'unknown'}")
    print(f"Size        : {human_bytes(path.stat().st_size)}")
    print("=" * 78)

    # ---- schema table ---------------------------------------------------- #
    names  = [f.name for f in fields]
    types  = [str(f.type) for f in fields]
    ptypes = [pandas_dtype(f.type) for f in fields]

    nw = max([len("column")]      + [len(n) for n in names])
    tw = max([len("arrow type")]  + [len(t) for t in types])
    pw = max([len("pandas dtype")] + [len(p) for p in ptypes])

    print("\nSchema")
    print("-" * 78)
    print(f"{'column':<{nw}}  {'arrow type':<{tw}}  {'pandas dtype':<{pw}}  nullable")
    print(f"{'-'*nw}  {'-'*tw}  {'-'*pw}  {'-'*8}")
    for f, t, p in zip(fields, types, ptypes):
        print(f"{f.name:<{nw}}  {t:<{tw}}  {p:<{pw}}  {f.nullable}")

    # ---- sample rows ----------------------------------------------------- #
    if nrows > 0 and md.num_rows > 0:
        shown = min(nrows, md.num_rows)
        print(f"\nFirst {shown} row(s)")
        print("-" * 78)

        reader = pf.iter_batches(batch_size=shown, columns=columns)
        batch = next(iter(reader), None)

        if batch is None or batch.num_rows == 0:
            print("(no rows)")
        elif pd is not None:
            df = batch.to_pandas()
            print(df.to_string(index=False, max_colwidth=40))
        else:                                   # no pandas -> plain dict dump
            for k, v in batch.to_pydict().items():
                print(f"{k}: {v}")

    print()


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(
        description="Show columns, data types and sample rows of a Parquet file."
    )
    ap.add_argument(
        "path",
        nargs="?",
        default=INPUT_PATH,
        help=f"parquet file or directory (default: {INPUT_PATH})",
    )
    ap.add_argument(
        "-n", "--rows",
        type=int,
        default=DEFAULT_ROWS,
        help=f"number of sample rows to print (default: {DEFAULT_ROWS}, 0 = none)",
    )
    ap.add_argument(
        "-c", "--columns",
        default=",".join(DEFAULT_COLUMNS) if DEFAULT_COLUMNS else None,
        help="comma-separated list of columns to inspect",
    )
    args = ap.parse_args(argv)

    p = Path(args.path)
    if not p.exists():
        sys.exit(f"Not found: {p}")

    files = sorted(p.glob("*.parquet")) if p.is_dir() else [p]
    if not files:
        sys.exit(f"No .parquet files found in {p}")

    cols = [c.strip() for c in args.columns.split(",") if c.strip()] if args.columns else None

    for i, f in enumerate(files):
        if i:
            print()
        try:
            inspect(f, args.rows, cols)
        except Exception as e:                  # corrupt file, wrong format, …
            print(f"{f}: ERROR -> {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
