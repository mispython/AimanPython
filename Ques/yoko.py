from pathlib import Path
import pandas as pd

path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBXODLC/ln06226.sas7bdat")
cache_dir = path.parent / "parquet_cache" / path.stem
parquet_files = list(cache_dir.glob("*.parquet"))

print(f"Cache dir   : {cache_dir}")
print(f"Cache exists: {cache_dir.exists()}")
print(f"Parquet files: {len(parquet_files)}")
if parquet_files:
    newest_cache = max(f.stat().st_mtime for f in parquet_files)
    sas_mtime    = path.stat().st_mtime
    print(f"Cache mtime : {newest_cache}")
    print(f"SAS mtime   : {sas_mtime}")
    print(f"Cache valid : {newest_cache >= sas_mtime}")
print(f"SAS file size: {path.stat().st_size / 1e9:.2f} GB")
