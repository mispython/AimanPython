"""
Program Name : COMPARE_PARQUET_SAS.py
Purpose      : Compare a Python-generated Parquet dataset against the
               original SAS .sas7bdat dataset to verify conversion
               correctness (row counts, column values, mismatches).
Library      : datacompy (pandas-based dataframe comparison)

Usage:
    Edit the CONFIG section below, then run:
        python compare_parquet_sas.py
"""

from pathlib import Path
import sys

import pandas as pd
import polars as pl
import datacompy

# =====================================================================
# CONFIG - edit these paths/settings for each comparison run
# =====================================================================
BASE_DIR = Path(r"C:\Users\aiman\Desktop\SAS_Python_Migration")

# Path to the SAS dataset (ground truth / expected output)
SAS_FILE = BASE_DIR / "SAS_OUTPUT" / "example.sas7bdat"

# Path to the Python-generated Parquet dataset (actual output)
PARQUET_FILE = BASE_DIR / "PYTHON_OUTPUT" / "example.parquet"

# Directory where the comparison report will be written
REPORT_DIR = BASE_DIR / "COMPARE_REPORTS"
REPORT_FILE = REPORT_DIR / f"{PARQUET_FILE.stem}_vs_{SAS_FILE.stem}_report.txt"

# Column(s) that uniquely identify a row in both datasets.
# This MUST be set correctly for a meaningful comparison.
JOIN_COLUMNS = ["ACCTNO"]

# Tolerances for numeric comparison (handles float rounding differences)
ABS_TOL = 0.0001
REL_TOL = 0

# If True, column names are upper-cased and stripped before comparing
# (SAS columns are typically uppercase; Polars/Parquet columns may differ).
NORMALIZE_COLUMN_NAMES = True


# =====================================================================
# FUNCTIONS
# =====================================================================
def load_sas_dataset(path: Path) -> pd.DataFrame:
    """Read a SAS .sas7bdat file into a pandas DataFrame."""
    if not path.exists():
        raise FileNotFoundError(f"SAS file not found: {path}")
    df = pd.read_sas(path, format="sas7bdat", encoding="latin1")
    return df


def load_parquet_dataset(path: Path) -> pd.DataFrame:
    """Read a Parquet file into a pandas DataFrame (via Polars)."""
    if not path.exists():
        raise FileNotFoundError(f"Parquet file not found: {path}")
    df_pl = pl.read_parquet(path)
    return df_pl.to_pandas()


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Uppercase and strip column names for consistent comparison."""
    df = df.copy()
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df


def decode_byte_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Decode any bytes-typed object columns (common with pandas.read_sas)."""
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == object:
            sample = df[col].dropna()
            if len(sample) > 0 and isinstance(sample.iloc[0], bytes):
                df[col] = df[col].apply(
                    lambda x: x.decode("latin1").strip() if isinstance(x, bytes) else x
                )
            elif len(sample) > 0 and isinstance(sample.iloc[0], str):
                df[col] = df[col].str.strip()
    return df


def run_comparison(df_sas: pd.DataFrame, df_parquet: pd.DataFrame) -> datacompy.Compare:
    """Run datacompy comparison between SAS (expected) and Parquet (actual)."""
    compare = datacompy.Compare(
        df1=df_sas,
        df2=df_parquet,
        join_columns=JOIN_COLUMNS,
        abs_tol=ABS_TOL,
        rel_tol=REL_TOL,
        df1_name="SAS",
        df2_name="PARQUET",
    )
    return compare


def write_report(compare: datacompy.Compare, report_path: Path) -> None:
    """Write the full datacompy report to a text file."""
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_text = compare.report()
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_text)


# =====================================================================
# MAIN
# =====================================================================
def main():
    print(f"SAS file      : {SAS_FILE}")
    print(f"Parquet file  : {PARQUET_FILE}")
    print(f"Join column(s): {JOIN_COLUMNS}")
    print("-" * 70)

    try:
        df_sas = load_sas_dataset(SAS_FILE)
        df_parquet = load_parquet_dataset(PARQUET_FILE)
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    if NORMALIZE_COLUMN_NAMES:
        df_sas = normalize_columns(df_sas)
        df_parquet = normalize_columns(df_parquet)

    df_sas = decode_byte_strings(df_sas)
    df_parquet = decode_byte_strings(df_parquet)

    print(f"SAS rows      : {len(df_sas):,}  | columns: {len(df_sas.columns)}")
    print(f"Parquet rows  : {len(df_parquet):,}  | columns: {len(df_parquet.columns)}")
    print("-" * 70)

    missing_join_cols = [c for c in JOIN_COLUMNS if c not in df_sas.columns or c not in df_parquet.columns]
    if missing_join_cols:
        print(f"ERROR: Join column(s) not found in both datasets: {missing_join_cols}")
        print(f"SAS columns     : {sorted(df_sas.columns.tolist())}")
        print(f"Parquet columns : {sorted(df_parquet.columns.tolist())}")
        sys.exit(1)

    compare = run_comparison(df_sas, df_parquet)

    write_report(compare, REPORT_FILE)

    # Print full report to terminal
    print(compare.report())

    # Print quick summary
    print("=" * 70)
    print("QUICK SUMMARY")
    print("=" * 70)
    print(f"Match (rows + columns identical) : {compare.matches()}")
    print(f"Rows only in SAS                 : {len(compare.df1_unq_rows):,}")
    print(f"Rows only in PARQUET              : {len(compare.df2_unq_rows):,}")
    print(f"Columns compared                  : {len(compare.intersect_columns())}")
    print(f"Columns with mismatches           : {len(compare.column_stats) - sum(1 for cs in compare.column_stats if cs['unequal_cnt'] == 0)}")
    print(f"Report written to                 : {REPORT_FILE}")


if __name__ == "__main__":
    main()
