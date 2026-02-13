#!/usr/bin/env python3
"""
Program: EIBRDL2A
Report : RDAL PART II

This program combines monthly datasets (DALM, FALM, KALM, NALM, LALM),
    merges them with weekly data (ALWKM), and produces consolidated RDAL Part II output.
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import polars as pl
import pyarrow.parquet as pq


class PathConfig:
    """Path configuration for EIBRDL2A"""

    def __init__(self):
        base_path = Path(os.getenv('BNM_DATA_PATH', '/data'))
        output_path = Path(os.getenv('BNM_OUTPUT_PATH', '/data/output'))

        # BNM library
        self.bnm_lib = output_path / "BNM"
        self.bnm_lib.mkdir(parents=True, exist_ok=True)

        # Macro variables from environment
        self.reptmon = os.getenv('REPTMON', '01')
        self.nowk = os.getenv('NOWK', '1')

    def get_input_path(self, dataset_name: str) -> Path:
        """Get input path for a dataset"""
        return self.bnm_lib / f"{dataset_name}.parquet"

    def get_output_path(self, dataset_name: str) -> Path:
        """Get output path for a dataset"""
        return self.bnm_lib / f"{dataset_name}.parquet"


class EIBRDL2AProcessor:
    """Main processor for EIBRDL2A"""

    def __init__(self, paths: PathConfig):
        self.paths = paths

    def combine_monthly_datasets(self) -> pl.DataFrame:
        """Combine DALM, FALM, KALM, NALM, LALM datasets"""
        print("Combining monthly datasets...")

        datasets = []
        dataset_names = [
            f"DALM{self.paths.reptmon}{self.paths.nowk}",
            f"FALM{self.paths.reptmon}{self.paths.nowk}",
            f"KALM{self.paths.reptmon}{self.paths.nowk}",
            f"NALM{self.paths.reptmon}{self.paths.nowk}",
            f"LALM{self.paths.reptmon}{self.paths.nowk}"
        ]

        for dataset_name in dataset_names:
            file_path = self.paths.get_input_path(dataset_name)
            if file_path.exists():
                df = pl.read_parquet(file_path)
                datasets.append(df)
                print(f"  Loaded {dataset_name}: {len(df)} records")
            else:
                print(f"  Warning: {dataset_name} not found, skipping")

        if not datasets:
            return pl.DataFrame({'BNMCODE': [], 'AMTIND': [], 'AMOUNT': []})

        # Combine all datasets
        combined = pl.concat(datasets, how='diagonal')
        print(f"  Combined total: {len(combined)} records")

        return combined

    def summarize_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Summarize data by BNMCODE and AMTIND"""
        print("Summarizing monthly data...")

        if len(df) == 0:
            return df

        # Summarize by BNMCODE and AMTIND
        df_summary = df.group_by(['BNMCODE', 'AMTIND']).agg([
            pl.col('AMOUNT').sum().alias('AMOUNT')
        ])

        print(f"  Summary: {len(df_summary)} records")
        return df_summary

    def create_almkm_temp(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create temporary ALMKM dataset"""
        print("Creating temporary ALMKM dataset...")

        if len(df) == 0:
            return pl.DataFrame({'ITCODE': [], 'AMTIND': [], 'AMOUNT': []})

        # Sort by BNMCODE and AMTIND
        df = df.sort(['BNMCODE', 'AMTIND'])

        # Rename BNMCODE to ITCODE and AMOUNT to OTHAMT, then back to AMOUNT
        df = df.rename({'BNMCODE': 'ITCODE'})

        # Select final columns
        df = df.select(['ITCODE', 'AMTIND', 'AMOUNT'])

        print(f"  ALMKM temp: {len(df)} records")
        return df

    def merge_with_weekly(self, df_almkm: pl.DataFrame, df_alwkm: pl.DataFrame) -> pl.DataFrame:
        """Merge ALMKM with ALWKM, keeping only items not in ALWKM"""
        print("Merging monthly with weekly data...")

        if len(df_almkm) == 0:
            print("  No ALMKM data to merge")
            return pl.DataFrame({'ITCODE': [], 'AMTIND': [], 'AMOUNT': []})

        if len(df_alwkm) == 0:
            print("  No ALWKM data, using all ALMKM records")
            # Filter out code 37
            df_filtered = df_almkm.filter(
                ~pl.col('ITCODE').str.slice(0, 2).eq('37')
            )
            print(f"  Filtered ALMKM: {len(df_filtered)} records")
            return df_filtered

        # Perform left anti-join: keep records from ALMKM that are NOT in ALWKM
        # SAS: IF B AND NOT A means keep records from ALMKM (B) that don't match ALWKM (A)
        df_merged = df_almkm.join(
            df_alwkm.select(['ITCODE', 'AMTIND']).with_columns(pl.lit(True).alias('_in_alwkm')),
            on=['ITCODE', 'AMTIND'],
            how='left'
        )

        # Keep only records not in ALWKM
        df_merged = df_merged.filter(pl.col('_in_alwkm').is_null())

        # Filter out code 37
        df_merged = df_merged.filter(
            ~pl.col('ITCODE').str.slice(0, 2).eq('37')
        )

        # Select final columns
        df_merged = df_merged.select(['ITCODE', 'AMTIND', 'AMOUNT'])

        print(f"  Merged ALMKM (monthly only): {len(df_merged)} records")
        return df_merged

    def append_to_weekly(self, df_alwkm: pl.DataFrame, df_monthly_only: pl.DataFrame) -> pl.DataFrame:
        """Append monthly-only items to weekly dataset"""
        print("Appending monthly items to weekly dataset...")

        if len(df_monthly_only) == 0:
            print("  No monthly items to append")
            return df_alwkm

        # Append monthly items to weekly
        df_final = pl.concat([df_alwkm, df_monthly_only], how='diagonal')

        print(f"  Final combined: {len(df_final)} records")
        return df_final

    def run(self):
        """Execute full processing"""
        print("=" * 80)
        print("EIBRDL2A - RDAL Part II Processing")
        print("=" * 80)

        # Step 1: Combine monthly datasets
        df_combined = self.combine_monthly_datasets()

        # Step 2: Summarize
        df_summary = self.summarize_data(df_combined)

        # Step 3: Create temporary ALMKM
        df_almkm_temp = self.create_almkm_temp(df_summary)

        # Step 4: Read ALWKM
        alwkm_file = self.paths.get_input_path(f"ALWKM{self.paths.reptmon}{self.paths.nowk}")
        if alwkm_file.exists():
            df_alwkm = pl.read_parquet(alwkm_file)
            # Sort ALWKM by ITCODE and AMTIND
            df_alwkm = df_alwkm.sort(['ITCODE', 'AMTIND'])
            print(f"  Loaded ALWKM: {len(df_alwkm)} records")
        else:
            print(f"  Warning: ALWKM not found")
            df_alwkm = pl.DataFrame({'ITCODE': [], 'AMTIND': [], 'AMOUNT': []})

        # Step 5: Merge ALMKM with ALWKM (keep monthly items not in weekly)
        df_monthly_only = self.merge_with_weekly(df_almkm_temp, df_alwkm)

        # Step 6: Append monthly items to weekly
        df_final = self.append_to_weekly(df_alwkm, df_monthly_only)

        # Step 7: Write final ALWKM output
        alwkm_output = self.paths.get_output_path(f"ALWKM{self.paths.reptmon}{self.paths.nowk}")
        df_final.write_parquet(alwkm_output)
        print(f"\nWritten final ALWKM to {alwkm_output}")

        # Display summary
        print("\n" + "=" * 80)
        print("Processing Summary:")
        print("=" * 80)
        print(f"Month: {self.paths.reptmon}")
        print(f"Week: {self.paths.nowk}")
        print(f"Combined Monthly Datasets: {len(df_combined)}")
        print(f"Summarized Monthly: {len(df_summary)}")
        print(f"Weekly (ALWKM): {len(df_alwkm)}")
        print(f"Monthly-Only Items: {len(df_monthly_only)}")
        print(f"Final ALWKM: {len(df_final)}")

        if len(df_final) > 0:
            total_amount = df_final['AMOUNT'].sum()
            print(f"Total Amount: {total_amount:,.2f}")

            # Show sample records
            print("\nSample Final Records (first 20):")
            print(df_final.sort(['ITCODE', 'AMTIND']).head(20))

        print("=" * 80)
        print("EIBRDL2A processing completed successfully")
        print("=" * 80)


def main():
    """Main entry point"""
    try:
        paths = PathConfig()
        processor = EIBRDL2AProcessor(paths)
        processor.run()
        return 0
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
