#!/usr/bin/env python3
"""
Program  : EIIWOF10.py
Purpose  : Merge new hardcode data with existing hardcode data.
           Replicates the inline SAS code from the EIIWOF10 JCL job step.

           Merges staging (T-prefix) datasets into working (W-prefix) datasets
           within the NPL HP SASDATA WOFF library, then sorts each by ACCTNO:
             NPL.WSP2 <- NPLA.WSP2 + NPL.TSP2  (SET append, sort by ACCTNO)
             NPL.WIIS <- NPLA.WIIS + NPL.TIIS  (SET append, sort by ACCTNO)
             NPL.WAQ  <- NPLA.WAQ  + NPL.TAQ   (SET append, sort by ACCTNO)

           Library mapping:
             NPL  = SAP.PIBB.NPL.HP.SASDATA.WOFF  (write target, DISP=OLD)
             NPLA = SAP.PIBB.NPL.HP.SASDATA        (read-only source, DISP=SHR)
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
from pathlib import Path

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
import duckdb
import polars as pl

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR  = Path(".")
NPL_DIR   = BASE_DIR / "data" / "pibb" / "npl_woff"    # NPL:  SAP.PIBB.NPL.HP.SASDATA.WOFF
NPLA_DIR  = BASE_DIR / "data" / "pibb" / "npl_sasdata"  # NPLA: SAP.PIBB.NPL.HP.SASDATA

NPL_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# HELPER: read parquet safely (returns empty DataFrame if file missing)
# ============================================================================
def _read(path: Path) -> pl.DataFrame:
    """Read a parquet file; return empty DataFrame if not found."""
    if not path.exists():
        return pl.DataFrame()
    con = duckdb.connect()
    return con.execute(f"SELECT * FROM read_parquet('{path}')").pl()

# ============================================================================
# HELPER: merge two datasets, sort by ACCTNO, write back to NPL
# Replicates:
#   DATA NPL.<target>; SET NPLA.<base> NPL.<staging>; RUN;
#   PROC SORT; BY ACCTNO;
# ============================================================================
def _merge_and_sort(
    base_file:    Path,
    staging_file: Path,
    target_file:  Path,
    sort_key:     str = "ACCTNO",
) -> None:
    """
    Append staging dataset to base dataset, sort by sort_key,
    write result to target_file.
    """
    base    = _read(base_file)
    staging = _read(staging_file)

    if base.is_empty() and staging.is_empty():
        pl.DataFrame().write_parquet(str(target_file))
        return

    merged = pl.concat([base, staging], how="diagonal")

    # PROC SORT; BY ACCTNO;
    if sort_key in merged.columns:
        merged = merged.sort(sort_key)

    merged.write_parquet(str(target_file))
    print(f"Written {len(merged):,} rows -> {target_file.name}")

# ============================================================================
# MAIN
# ============================================================================
def main():
    # ---- SP section ----
    # DATA NPL.WSP2; SET NPLA.WSP2 NPL.TSP2; RUN;
    # PROC SORT; BY ACCTNO;
    print("Merging WSP2...")
    _merge_and_sort(
        base_file    = NPLA_DIR / "wsp2.parquet",
        staging_file = NPL_DIR  / "tsp2.parquet",
        target_file  = NPL_DIR  / "wsp2.parquet",
    )

    # DATA NPL.WIIS; SET NPLA.WIIS NPL.TIIS; RUN;
    # PROC SORT; BY ACCTNO;
    print("Merging WIIS...")
    _merge_and_sort(
        base_file    = NPLA_DIR / "wiis.parquet",
        staging_file = NPL_DIR  / "tiis.parquet",
        target_file  = NPL_DIR  / "wiis.parquet",
    )

    # DATA NPL.WAQ; SET NPLA.WAQ NPL.TAQ; RUN;
    # PROC SORT; BY ACCTNO;
    print("Merging WAQ...")
    _merge_and_sort(
        base_file    = NPLA_DIR / "waq.parquet",
        staging_file = NPL_DIR  / "taq.parquet",
        target_file  = NPL_DIR  / "waq.parquet",
    )

    print("EIAWOF10 completed successfully.")


if __name__ == '__main__':
    main()
