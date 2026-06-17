import polars as pl
import sys
from pathlib import Path

def display_parquet(file_path: str):
    path = Path(file_path)

    if not path.exists():
        print(f"File not found: {file_path}")
        return

    df = pl.read_parquet(path)

    print("\n=== Parquet File Content ===\n")
    print(df)

    print("\n=== Schema ===")
    print(df.schema)

    print("\n=== Shape ===")
    print(df.shape)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python read_parquet.py <file.parquet>")
    else:
        display_parquet(sys.argv[1])
