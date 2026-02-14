# !/usr/bin/env python3
"""
Program: EIAWOF09
Purpose: COPY AND RENAME ALL RELATED FIELDS FROM EXISTING NPL
         IE. IIS, AQ & SP2 FOR HARDCODING OF WRITTEN ACCOUNTS
         RUN ONLY ON WRITE OFF ACCOUNTS

This program creates temporary datasets (TIIS, TAQ, TSP2) by copying
    and renaming fields from existing NPL datasets for accounts that are being written off.
"""

import polars as pl
from datetime import datetime
from pathlib import Path

# Setup paths
INPUT_NPL_REPTDATE = "SAP.PBB.NPL.HP.SASDATA.REPTDATE.parquet"
INPUT_NPLA_IIS = "SAP.PBB.NPL.HP.SASDATA.IIS.parquet"
INPUT_NPLA_AQ = "SAP.PBB.NPL.HP.SASDATA.AQ.parquet"
INPUT_NPLA_SP2 = "SAP.PBB.NPL.HP.SASDATA.SP2.parquet"
INPUT_NPL_WOFFHP = "SAP.PBB.NPL.HP.SASDATA.WOFF.WOFFHP.parquet"

# Output paths
OUTPUT_NPL_TIIS = "SAP.PBB.NPL.HP.SASDATA.WOFF.TIIS.parquet"
OUTPUT_NPL_TAQ = "SAP.PBB.NPL.HP.SASDATA.WOFF.TAQ.parquet"
OUTPUT_NPL_TSP2 = "SAP.PBB.NPL.HP.SASDATA.WOFF.TSP2.parquet"

# Report outputs
OUTPUT_REPORT_TIIS = "NPL_TIIS_REPORT.txt"
OUTPUT_REPORT_TAQ = "NPL_TAQ_REPORT.txt"
OUTPUT_REPORT_TSP2 = "NPL_TSP2_REPORT.txt"


def safe_float(value):
    """Safely convert value to float, returning None if invalid"""
    if value is None:
        return None
    try:
        return float(value)
    except:
        return None


def write_report_with_asa(df, output_file, title, page_length=60):
    """
    Write a report with ASA carriage control characters
    PROC PRINT equivalent
    """
    with open(output_file, 'w') as f:
        lines_on_page = 0
        page_num = 1

        def write_header():
            nonlocal lines_on_page
            f.write('1')
            f.write(f"{title:^132}\n")
            f.write(' ')
            f.write(f"{'Page ' + str(page_num):>125}\n")
            f.write(' \n')

            # Write column headers
            if len(df) > 0:
                cols = df.columns
                header = ''.join([f'{col:>15}' for col in cols[:8]])
                f.write(' ')
                f.write(header + '\n')
                f.write(' ')
                f.write('-' * min(len(cols) * 15, 132) + '\n')

            lines_on_page = 5

        write_header()

        if len(df) > 0:
            for idx, row_dict in enumerate(df.iter_rows(named=True), start=1):
                if lines_on_page >= page_length - 2:
                    page_num += 1
                    write_header()

                f.write(' ')
                values = []
                for col in df.columns[:8]:
                    val = row_dict.get(col)
                    if val is None:
                        values.append(f"{'':>15}")
                    elif isinstance(val, (int, float)):
                        values.append(f"{val:>15.2f}" if isinstance(val, float) else f"{val:>15}")
                    else:
                        values.append(f"{str(val):>15}")

                f.write(''.join(values) + '\n')
                lines_on_page += 1
        else:
            f.write(' ')
            f.write("No observations\n")

        f.write(' ')
        f.write('=' * 132 + '\n')


def main():
    """
    Main processing function for EIAWOF09
    """
    print("EIAWOF09 - Copy NPL Fields for Written Off Accounts")
    print("=" * 70)

    # Read REPTDATE
    print("\nReading REPTDATE...")
    df_reptdate = pl.read_parquet(INPUT_NPL_REPTDATE)
    reptdate = df_reptdate['REPTDATE'][0]

    print(f"Reporting Date: {reptdate}")
    print("=" * 70)

    # Read input files and sort by ACCTNO
    print("\nReading input files...")
    df_iis = pl.read_parquet(INPUT_NPLA_IIS).sort("ACCTNO")
    df_aq = pl.read_parquet(INPUT_NPLA_AQ).sort("ACCTNO")
    df_sp2 = pl.read_parquet(INPUT_NPLA_SP2).sort("ACCTNO")
    df_woffhp = pl.read_parquet(INPUT_NPL_WOFFHP).sort("ACCTNO")

    print(f"IIS records: {len(df_iis):,}")
    print(f"AQ records: {len(df_aq):,}")
    print(f"SP2 records: {len(df_sp2):,}")
    print(f"WOFFHP records: {len(df_woffhp):,}")

    # Process TIIS - Interest in Suspense
    print("\nProcessing TIIS (Interest in Suspense)...")
    df_tiis = df_iis.join(
        df_woffhp.select(["ACCTNO"]).with_columns(pl.lit(True).alias("_in_woffhp")),
        on="ACCTNO",
        how="inner"
    )

    # Create TIIS with renamed fields
    df_tiis = df_tiis.select([
        pl.col("ACCTNO"),
        pl.col("NOTENO"),
        pl.col("SUSPEND").alias("WSUSPEND"),
        pl.col("RECOVER").round(2).alias("WRECOVER"),
        pl.col("RECC").alias("WRECC"),
        pl.col("OISUSP").alias("WOISUSP"),
        pl.col("OIRECV").alias("WOIRECV"),
        pl.col("OIRECC").alias("WOIRECC"),
        pl.lit(reptdate).alias("WDATE")
    ])

    print(f"TIIS records created: {len(df_tiis):,}")

    # Process TAQ - Asset Quality
    print("\nProcessing TAQ (Asset Quality)...")
    df_taq = df_aq.join(
        df_woffhp.select(["ACCTNO"]).with_columns(pl.lit(True).alias("_in_woffhp")),
        on="ACCTNO",
        how="inner"
    )

    # Create TAQ with renamed fields
    df_taq = df_taq.select([
        pl.col("ACCTNO"),
        pl.col("NOTENO"),
        pl.col("NEWNPL").alias("WNEWNPL"),
        pl.col("ACCRINT").alias("WACCRINT"),
        pl.col("RECOVER").alias("WRECOVER"),
        pl.lit(reptdate).alias("WDATE")
    ])

    print(f"TAQ records created: {len(df_taq):,}")

    # Process TSP2 - Specific Provision
    print("\nProcessing TSP2 (Specific Provision)...")
    df_tsp2 = df_sp2.join(
        df_woffhp.select(["ACCTNO"]).with_columns(pl.lit(True).alias("_in_woffhp")),
        on="ACCTNO",
        how="inner"
    )

    # Create TSP2 with renamed fields
    df_tsp2 = df_tsp2.select([
        pl.col("ACCTNO"),
        pl.col("NOTENO"),
        pl.col("SPPL").alias("WSPPL"),
        pl.col("RECOVER").alias("WRECOVER"),
        pl.lit(reptdate).alias("WDATE")
    ])

    print(f"TSP2 records created: {len(df_tsp2):,}")

    # Write output files
    print("\nWriting output files...")
    df_tiis.write_parquet(OUTPUT_NPL_TIIS)
    print(f"  {OUTPUT_NPL_TIIS} ({len(df_tiis):,} records)")

    df_taq.write_parquet(OUTPUT_NPL_TAQ)
    print(f"  {OUTPUT_NPL_TAQ} ({len(df_taq):,} records)")

    df_tsp2.write_parquet(OUTPUT_NPL_TSP2)
    print(f"  {OUTPUT_NPL_TSP2} ({len(df_tsp2):,} records)")

    # Generate reports (PROC PRINT equivalents)
    print("\nGenerating reports...")

    write_report_with_asa(
        df_tiis,
        OUTPUT_REPORT_TIIS,
        "NPL.TIIS - Interest in Suspense for Written Off Accounts"
    )
    print(f"  {OUTPUT_REPORT_TIIS}")

    write_report_with_asa(
        df_taq,
        OUTPUT_REPORT_TAQ,
        "NPL.TAQ - Asset Quality for Written Off Accounts"
    )
    print(f"  {OUTPUT_REPORT_TAQ}")

    write_report_with_asa(
        df_tsp2,
        OUTPUT_REPORT_TSP2,
        "NPL.TSP2 - Specific Provision for Written Off Accounts"
    )
    print(f"  {OUTPUT_REPORT_TSP2}")

    # Display summary
    print("\n" + "=" * 70)
    print("PROCESSING SUMMARY")
    print("=" * 70)
    print(f"Reporting Date: {reptdate}")
    print(f"\nTIIS (Interest in Suspense):")
    print(f"  Records: {len(df_tiis):,}")
    if len(df_tiis) > 0:
        print(f"  Total WSUSPEND: {df_tiis['WSUSPEND'].sum():,.2f}")
        print(f"  Total WRECOVER: {df_tiis['WRECOVER'].sum():,.2f}")

    print(f"\nTAQ (Asset Quality):")
    print(f"  Records: {len(df_taq):,}")
    if len(df_taq) > 0:
        print(f"  Total WNEWNPL: {df_taq['WNEWNPL'].sum():,.2f}")
        print(f"  Total WACCRINT: {df_taq['WACCRINT'].sum():,.2f}")
        print(f"  Total WRECOVER: {df_taq['WRECOVER'].sum():,.2f}")

    print(f"\nTSP2 (Specific Provision):")
    print(f"  Records: {len(df_tsp2):,}")
    if len(df_tsp2) > 0:
        print(f"  Total WSPPL: {df_tsp2['WSPPL'].sum():,.2f}")
        print(f"  Total WRECOVER: {df_tsp2['WRECOVER'].sum():,.2f}")

    print("=" * 70)
    print("\nProcessing complete.")


if __name__ == "__main__":
    main()
