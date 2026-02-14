# !/usr/bin/env python3
"""
Program: EIAWOF10
Purpose: MERGE THE NEW HARDCODE DATA WITH EXISTING HARDCODE DATA

This program merges newly created temporary datasets (TIIS, TAQ, TSP2)
with existing NPL datasets (WIIS, WAQ, WSP2) and sorts them by ACCTNO.
"""

import polars as pl
from pathlib import Path

# Setup paths - Input files
INPUT_NPLA_WSP2 = "SAP.PBB.NPL.HP.SASDATA.WSP2.parquet"
INPUT_NPL_TSP2 = "SAP.PBB.NPL.HP.SASDATA.WOFF.TSP2.parquet"
INPUT_NPLA_WIIS = "SAP.PBB.NPL.HP.SASDATA.WIIS.parquet"
INPUT_NPL_TIIS = "SAP.PBB.NPL.HP.SASDATA.WOFF.TIIS.parquet"
INPUT_NPLA_WAQ = "SAP.PBB.NPL.HP.SASDATA.WAQ.parquet"
INPUT_NPL_TAQ = "SAP.PBB.NPL.HP.SASDATA.WOFF.TAQ.parquet"

# Setup paths - Output files
OUTPUT_NPL_WSP2 = "SAP.PBB.NPL.HP.SASDATA.WOFF.WSP2.parquet"
OUTPUT_NPL_WIIS = "SAP.PBB.NPL.HP.SASDATA.WOFF.WIIS.parquet"
OUTPUT_NPL_WAQ = "SAP.PBB.NPL.HP.SASDATA.WOFF.WAQ.parquet"


def main():
    """
    Main processing function for EIAWOF10
    """
    print("EIAWOF10 - Merge Hardcode Data")
    print("=" * 70)
    print("Merging new hardcode data with existing hardcode data")
    print("=" * 70)

    # Process WSP2 - Specific Provision
    print("\nProcessing WSP2 (Specific Provision)...")

    # Check if files exist
    npla_wsp2_path = Path(INPUT_NPLA_WSP2)
    npl_tsp2_path = Path(INPUT_NPL_TSP2)

    if not npla_wsp2_path.exists():
        print(f"  Warning: {INPUT_NPLA_WSP2} not found")
        df_npla_wsp2 = pl.DataFrame()
    else:
        df_npla_wsp2 = pl.read_parquet(INPUT_NPLA_WSP2)
        print(f"  NPLA.WSP2 records: {len(df_npla_wsp2):,}")

    if not npl_tsp2_path.exists():
        print(f"  Warning: {INPUT_NPL_TSP2} not found")
        df_npl_tsp2 = pl.DataFrame()
    else:
        df_npl_tsp2 = pl.read_parquet(INPUT_NPL_TSP2)
        print(f"  NPL.TSP2 records: {len(df_npl_tsp2):,}")

    # Concatenate and sort by ACCTNO
    if len(df_npla_wsp2) > 0 or len(df_npl_tsp2) > 0:
        df_wsp2 = pl.concat([df_npla_wsp2, df_npl_tsp2])
        df_wsp2 = df_wsp2.sort("ACCTNO")
        df_wsp2.write_parquet(OUTPUT_NPL_WSP2)
        print(f"  NPL.WSP2 merged records: {len(df_wsp2):,}")
        print(f"  Written to: {OUTPUT_NPL_WSP2}")
    else:
        print("  No records to merge for WSP2")

    # Process WIIS - Interest in Suspense
    print("\nProcessing WIIS (Interest in Suspense)...")

    npla_wiis_path = Path(INPUT_NPLA_WIIS)
    npl_tiis_path = Path(INPUT_NPL_TIIS)

    if not npla_wiis_path.exists():
        print(f"  Warning: {INPUT_NPLA_WIIS} not found")
        df_npla_wiis = pl.DataFrame()
    else:
        df_npla_wiis = pl.read_parquet(INPUT_NPLA_WIIS)
        print(f"  NPLA.WIIS records: {len(df_npla_wiis):,}")

    if not npl_tiis_path.exists():
        print(f"  Warning: {INPUT_NPL_TIIS} not found")
        df_npl_tiis = pl.DataFrame()
    else:
        df_npl_tiis = pl.read_parquet(INPUT_NPL_TIIS)
        print(f"  NPL.TIIS records: {len(df_npl_tiis):,}")

    # Concatenate and sort by ACCTNO
    if len(df_npla_wiis) > 0 or len(df_npl_tiis) > 0:
        df_wiis = pl.concat([df_npla_wiis, df_npl_tiis])
        df_wiis = df_wiis.sort("ACCTNO")
        df_wiis.write_parquet(OUTPUT_NPL_WIIS)
        print(f"  NPL.WIIS merged records: {len(df_wiis):,}")
        print(f"  Written to: {OUTPUT_NPL_WIIS}")
    else:
        print("  No records to merge for WIIS")

    # Process WAQ - Asset Quality
    print("\nProcessing WAQ (Asset Quality)...")

    npla_waq_path = Path(INPUT_NPLA_WAQ)
    npl_taq_path = Path(INPUT_NPL_TAQ)

    if not npla_waq_path.exists():
        print(f"  Warning: {INPUT_NPLA_WAQ} not found")
        df_npla_waq = pl.DataFrame()
    else:
        df_npla_waq = pl.read_parquet(INPUT_NPLA_WAQ)
        print(f"  NPLA.WAQ records: {len(df_npla_waq):,}")

    if not npl_taq_path.exists():
        print(f"  Warning: {INPUT_NPL_TAQ} not found")
        df_npl_taq = pl.DataFrame()
    else:
        df_npl_taq = pl.read_parquet(INPUT_NPL_TAQ)
        print(f"  NPL.TAQ records: {len(df_npl_taq):,}")

    # Concatenate and sort by ACCTNO
    if len(df_npla_waq) > 0 or len(df_npl_taq) > 0:
        df_waq = pl.concat([df_npla_waq, df_npl_taq])
        df_waq = df_waq.sort("ACCTNO")
        df_waq.write_parquet(OUTPUT_NPL_WAQ)
        print(f"  NPL.WAQ merged records: {len(df_waq):,}")
        print(f"  Written to: {OUTPUT_NPL_WAQ}")
    else:
        print("  No records to merge for WAQ")

    # Display summary
    print("\n" + "=" * 70)
    print("PROCESSING SUMMARY")
    print("=" * 70)
    print("\nMerged Datasets:")
    print(f"  WSP2 (Specific Provision): {len(df_wsp2) if 'df_wsp2' in locals() else 0:,} records")
    print(f"  WIIS (Interest in Suspense): {len(df_wiis) if 'df_wiis' in locals() else 0:,} records")
    print(f"  WAQ (Asset Quality): {len(df_waq) if 'df_waq' in locals() else 0:,} records")

    print("\nOutput Files:")
    print(f"  {OUTPUT_NPL_WSP2}")
    print(f"  {OUTPUT_NPL_WIIS}")
    print(f"  {OUTPUT_NPL_WAQ}")

    print("=" * 70)
    print("\nProcessing complete.")
    print("All hardcode data has been merged and sorted by ACCTNO.")


if __name__ == "__main__":
    main()
