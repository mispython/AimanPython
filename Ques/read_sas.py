# sas_peek.py
from pathlib import Path
import pandas as pd

# ============================================================
# HARDCODE YOUR FILE PATH HERE
# ============================================================
SAS_FILE = r"C:\Users\you\Downloads\mydata.sas7bdat"
# ============================================================

PREVIEW_ROWS = 10
USE_PARQUET_CACHE = True
OVERWRITE_CACHE = False


def read_sas_file(path: Path):
    """Read .sas7bdat, preferring pyreadstat (handles more encodings/compression)."""
    try:
        import pyreadstat
        print(f"Reading with pyreadstat: {path}")
        df, meta = pyreadstat.read_sas7bdat(str(path))
        return df, meta
    except ImportError:
        print("pyreadstat not installed – using pandas.read_sas (slower, "
              "may fail on some encodings/compressed files)")
        df = pd.read_sas(path, format="sas7bdat", encoding="utf-8")
        return df, None


def main():
    path = Path(SAS_FILE)
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    parquet_path = path.with_suffix(".parquet")
    use_cache = USE_PARQUET_CACHE and parquet_path.exists() and not OVERWRITE_CACHE

    meta = None
    if use_cache:
        print(f"Loading from Parquet cache: {parquet_path}")
        df = pd.read_parquet(parquet_path)
    else:
        df, meta = read_sas_file(path)
        if USE_PARQUET_CACHE:
            try:
                df.to_parquet(parquet_path, index=False)
                print(f"Cached to Parquet: {parquet_path}")
            except Exception as e:
                print(f"⚠ Could not write Parquet ({e}). "
                      "Install pyarrow: pip install pyarrow")

    print(f"\nRows: {len(df):,}   |   Columns: {df.shape[1]}")

    # ---- Schema table ----
    print("\n=== Schema ===")
    schema = pd.DataFrame({
        "column":   list(df.columns),
        "dtype":    [str(t) for t in df.dtypes],
        "non_null": df.notna().sum().to_numpy(),
        "nulls":    df.isna().sum().to_numpy(),
    })
    if meta is not None:
        schema["sas_label"] = [meta.column_names_to_labels.get(c, "") for c in df.columns]
        schema["sas_type"]  = [meta.original_variable_types.get(c, "") for c in df.columns]
    print(schema.to_string(index=False))

    # ---- Preview rows ----
    print(f"\n=== First {PREVIEW_ROWS} rows ===")
    with pd.option_context(
        "display.max_columns", None,
        "display.width", None,
        "display.max_colwidth", 40,
    ):
        print(df.head(PREVIEW_ROWS).to_string())


if __name__ == "__main__":
    main()
