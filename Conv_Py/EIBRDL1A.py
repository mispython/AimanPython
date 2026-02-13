#!/usr/bin/env python3
"""
Program     : EIBRDL1A
Report      : RDAL PART IA
Description : RDAL for KAPITI & MNI

This program consolidates weekly data from DALW, FALW, LALW, and KALW datasets,
    applies special mapping rules, and filters for weekly vs monthly items.
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import polars as pl
import pyarrow.parquet as pq
from typing import List, Set


class PathConfig:
    """Path configuration for EIBRDL1A"""

    def __init__(self):
        base_path = Path(os.getenv('BNM_DATA_PATH', '/data'))
        output_path = Path(os.getenv('BNM_OUTPUT_PATH', '/data/output'))

        # BNM library
        self.bnm_lib = output_path / "BNM"
        self.bnm_lib.mkdir(parents=True, exist_ok=True)

        # Macro variables from environment
        self.reptmon = os.getenv('REPTMON', '01')
        self.nowk = os.getenv('NOWK', '1')

        # Define AL100, AL500, AL600 lists
        self.al100 = ['42110', '42120', '42130', '42131', '42132', '42150', '42160',
                      '42170', '42180', '42199', '42510', '42520', '42599', '42190']
        self.al500 = ['42510', '42520', '42599']
        self.al600 = ['42610', '42620', '42630', '42631', '42632', '42660', '42699']

    def get_input_path(self, dataset_name: str) -> Path:
        """Get input path for a dataset"""
        return self.bnm_lib / f"{dataset_name}.parquet"

    def get_output_path(self, dataset_name: str) -> Path:
        """Get output path for a dataset"""
        return self.bnm_lib / f"{dataset_name}.parquet"


class EIBRDL1AProcessor:
    """Main processor for EIBRDL1A"""

    def __init__(self, paths: PathConfig):
        self.paths = paths

    def combine_weekly_datasets(self) -> pl.DataFrame:
        """Combine DALW, FALW, LALW, KALW datasets"""
        print("Combining weekly datasets...")

        datasets = []
        dataset_names = [
            f"DALW{self.paths.reptmon}{self.paths.nowk}",
            f"FALW{self.paths.reptmon}{self.paths.nowk}",
            f"LALW{self.paths.reptmon}{self.paths.nowk}",
            f"KALW{self.paths.reptmon}{self.paths.nowk}"
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
        print("Summarizing data...")

        if len(df) == 0:
            return df

        # Summarize by BNMCODE and AMTIND
        df_summary = df.group_by(['BNMCODE', 'AMTIND']).agg([
            pl.col('AMOUNT').sum().alias('AMOUNT')
        ])

        print(f"  Summary: {len(df_summary)} records")
        return df_summary

    def apply_special_mappings(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply special BNM code mappings"""
        print("Applying special mappings...")

        if len(df) == 0:
            return df

        results = []

        for row in df.iter_rows(named=True):
            bnmcode = row['BNMCODE']
            amtind = row['AMTIND']
            amount = row['AMOUNT']

            # Always output original record
            results.append({
                'BNMCODE': bnmcode,
                'AMTIND': amtind,
                'AMOUNT': amount
            })

            # Get code parts
            code_5 = bnmcode[:5] if len(bnmcode) >= 5 else ''
            code_6_7 = bnmcode[5:7] if len(bnmcode) >= 7 else ''

            # AL100 mappings (codes 57, 75)
            if code_5 in self.paths.al100 and code_6_7 in ['57', '75']:
                if code_6_7 == '75':
                    results.append({
                        'BNMCODE': '4210075000000Y',
                        'AMTIND': amtind,
                        'AMOUNT': amount
                    })
                if code_6_7 == '57':
                    results.append({
                        'BNMCODE': '4210057000000Y',
                        'AMTIND': amtind,
                        'AMOUNT': amount
                    })

            # AL500 mappings (codes 01, 71)
            # Note: Original SAS checks for '01' and '71' but outputs '75' and '57'
            # This appears to be a bug in the original code, but we'll preserve it
            if code_5 in self.paths.al500 and code_6_7 in ['01', '71']:
                if code_6_7 == '75':  # This condition will never be true given the check above
                    results.append({
                        'BNMCODE': '4250001000000Y',
                        'AMTIND': amtind,
                        'AMOUNT': amount
                    })
                if code_6_7 == '57':  # This condition will never be true given the check above
                    results.append({
                        'BNMCODE': '4250071000000Y',
                        'AMTIND': amtind,
                        'AMOUNT': amount
                    })

            # AL600 mappings (codes 57, 75)
            if code_5 in self.paths.al600 and code_6_7 in ['57', '75']:
                if code_6_7 == '75':
                    results.append({
                        'BNMCODE': '4260075000000Y',
                        'AMTIND': amtind,
                        'AMOUNT': amount
                    })
                if code_6_7 == '57':
                    results.append({
                        'BNMCODE': '4260057000000Y',
                        'AMTIND': amtind,
                        'AMOUNT': amount
                    })

        df_result = pl.DataFrame(results)
        print(f"  After mappings: {len(df_result)} records")
        return df_result

    def filter_and_split(self, df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Filter and split data into ALWKM and ALW datasets"""
        print("Filtering and splitting data...")

        if len(df) == 0:
            empty_df = pl.DataFrame({'ITCODE': [], 'AMTIND': [], 'AMOUNT': []})
            return empty_df, empty_df

        # Rename BNMCODE to ITCODE and AMOUNT to OTHAMT for processing
        df = df.rename({'BNMCODE': 'ITCODE', 'AMOUNT': 'OTHAMT'})

        # Determine WEEKLY flag
        weekly = 'Y' if self.paths.nowk != '4' else 'N'

        alwkm_records = []
        alw_records = []

        for row in df.iter_rows(named=True):
            itcode = row['ITCODE']
            amtind = row['AMTIND']
            othamt = row['OTHAMT']

            # Get code parts
            code_2 = itcode[:2] if len(itcode) >= 2 else ''
            code_3 = itcode[:3] if len(itcode) >= 3 else ''
            code_4 = itcode[:4] if len(itcode) >= 4 else ''
            code_7 = itcode[:7] if len(itcode) >= 7 else ''

            # Check exclusion criteria for monthly items
            is_monthly_excluded = (
                    code_2 in ['35', '36', '37', '59'] or
                    code_3 in ['821', '411', '391', '392', '491', '492']
            )

            if not is_monthly_excluded:
                # Filter out monthly items - process normally
                final_amtind = ' ' if code_2 == '30' else amtind
                alwkm_records.append({
                    'ITCODE': itcode,
                    'AMTIND': final_amtind,
                    'AMOUNT': othamt
                })
            else:
                # Handle excluded items with special logic
                current_weekly = weekly

                # Code 37: Output if WEEKLY='Y'
                if code_2 == '37' and current_weekly == 'Y':
                    alwkm_records.append({
                        'ITCODE': itcode,
                        'AMTIND': amtind,
                        'AMOUNT': othamt
                    })

                # Codes 391, 392
                if code_3 in ['391', '392']:
                    alw_records.append({
                        'ITCODE': itcode,
                        'AMTIND': amtind,
                        'AMOUNT': othamt
                    })

                    # Output 3911 codes to ALWKM
                    if code_4 == '3911':
                        alwkm_records.append({
                            'ITCODE': itcode,
                            'AMTIND': amtind,
                            'AMOUNT': othamt
                        })

                    # Output consolidated 3910000000000Y
                    if current_weekly == 'Y':
                        alwkm_records.append({
                            'ITCODE': '3910000000000Y',
                            'AMTIND': amtind,
                            'AMOUNT': othamt
                        })

                # Codes 4110, 4111
                if code_4 in ['4110', '4111']:
                    # Output consolidated 4100000000000Y
                    if current_weekly == 'Y':
                        alwkm_records.append({
                            'ITCODE': '4100000000000Y',
                            'AMTIND': amtind,
                            'AMOUNT': othamt
                        })

                # Codes 491, 492
                if code_3 in ['491', '492']:
                    alw_records.append({
                        'ITCODE': itcode,
                        'AMTIND': amtind,
                        'AMOUNT': othamt
                    })

                    # Output specific 491/492 codes to ALWKM
                    if code_7 in ['4911050', '4911080', '4912050', '4912080',
                                  '4929950', '4929980', '4929000']:
                        alwkm_records.append({
                            'ITCODE': itcode,
                            'AMTIND': amtind,
                            'AMOUNT': othamt
                        })

                    # Output consolidated 4910000000000Y
                    if current_weekly == 'Y':
                        alwkm_records.append({
                            'ITCODE': '4910000000000Y',
                            'AMTIND': amtind,
                            'AMOUNT': othamt
                        })

        # Create DataFrames
        if alwkm_records:
            df_alwkm = pl.DataFrame(alwkm_records)
        else:
            df_alwkm = pl.DataFrame({'ITCODE': [], 'AMTIND': [], 'AMOUNT': []})

        if alw_records:
            df_alw = pl.DataFrame(alw_records)
        else:
            df_alw = pl.DataFrame({'ITCODE': [], 'AMTIND': [], 'AMOUNT': []})

        print(f"  ALWKM records: {len(df_alwkm)}")
        print(f"  ALW records: {len(df_alw)}")

        return df_alwkm, df_alw

    def run(self):
        """Execute full processing"""
        print("=" * 80)
        print("EIBRDL1A - RDAL Part IA Processing")
        print("=" * 80)

        # Combine weekly datasets
        df_combined = self.combine_weekly_datasets()

        if len(df_combined) == 0:
            print("No data to process")
            return

        # Summarize
        df_summary = self.summarize_data(df_combined)

        # Apply special mappings
        df_mapped = self.apply_special_mappings(df_summary)

        # Sort by BNMCODE and AMTIND
        df_mapped = df_mapped.sort(['BNMCODE', 'AMTIND'])

        # Filter and split
        df_alwkm, df_alw = self.filter_and_split(df_mapped)

        # Write ALWKM output
        alwkm_file = self.paths.get_output_path(f"ALWKM{self.paths.reptmon}{self.paths.nowk}")
        df_alwkm.write_parquet(alwkm_file)
        print(f"\nWritten ALWKM to {alwkm_file}")

        # Display summary
        print("\n" + "=" * 80)
        print("Processing Summary:")
        print("=" * 80)
        print(f"Week: {self.paths.nowk}")
        print(f"Month: {self.paths.reptmon}")
        print(f"ALWKM Records: {len(df_alwkm)}")
        print(f"ALW Records: {len(df_alw)}")

        if len(df_alwkm) > 0:
            total_amount = df_alwkm['AMOUNT'].sum()
            print(f"Total ALWKM Amount: {total_amount:,.2f}")

            # Show sample records
            print("\nSample ALWKM Records (first 20):")
            print(df_alwkm.head(20))

        print("=" * 80)
        print("EIBRDL1A processing completed successfully")
        print("=" * 80)


def main():
    """Main entry point"""
    try:
        paths = PathConfig()
        processor = EIBRDL1AProcessor(paths)
        processor.run()
        return 0
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
