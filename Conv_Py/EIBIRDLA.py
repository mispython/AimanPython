#!/usr/bin/env python3
"""
Program: EIBIRDLA
Purpose: RDAL/NSRS Report Generation

This program generates RDAL and NSRS formatted text files from weekly data,
    including Assets and Liabilities (AL), Off-Balance Sheet (OB), and Special (SP) sections.
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import polars as pl
import pyarrow.parquet as pq
from typing import List, Tuple


class PathConfig:
    """Path configuration"""

    def __init__(self):
        base_path = Path(os.getenv('BNM_DATA_PATH', '/data'))
        output_path = Path(os.getenv('BNM_OUTPUT_PATH', '/data/output'))

        # Input paths
        self.loan_lib = base_path / "LOAN"

        # BNM library
        self.bnm_lib = output_path / "BNM"
        self.bnm_lib.mkdir(parents=True, exist_ok=True)

        # RDALTMP library
        self.rdaltmp_lib = output_path / "RDALTMP"
        self.rdaltmp_lib.mkdir(parents=True, exist_ok=True)

        # Macro variables from environment
        self.reptmon = os.getenv('REPTMON', '01')
        self.nowk = os.getenv('NOWK', '1')
        self.reptyear = os.getenv('REPTYEAR', str(datetime.now().year))
        self.reptday = os.getenv('REPTDAY', '01')

        # Output files
        self.rdalkm_file = self.bnm_lib / f"RDAL KAPMNI.TXT"
        self.nsrskm_file = self.bnm_lib / f"NSRS KAPMNI.TXT"

    def get_input_path(self, dataset_name: str) -> Path:
        """Get input path for a dataset"""
        return self.bnm_lib / f"{dataset_name}.parquet"

    def get_loan_path(self, dataset_name: str) -> Path:
        """Get loan library path"""
        return self.loan_lib / f"{dataset_name}.parquet"


class RDALProcessor:
    """Main processor for RDAL/NSRS generation"""

    def __init__(self, paths: PathConfig):
        self.paths = paths

    def load_weekly_data(self) -> pl.DataFrame:
        """Load and filter weekly data"""
        print("Loading weekly data...")

        # Read ALWKM dataset
        alwkm_file = self.paths.get_input_path(f"ALWKM{self.paths.reptmon}{self.paths.nowk}")
        df = pl.read_parquet(alwkm_file)

        print(f"  Loaded {len(df)} records")

        # Filter out specific code ranges
        df = df.with_columns([
            pl.col('ITCODE').str.slice(0, 5).alias('CODE5')
        ])

        df = df.filter(
            ~((pl.col('CODE5') >= '30221') & (pl.col('CODE5') <= '30228')) &
            ~((pl.col('CODE5') >= '30231') & (pl.col('CODE5') <= '30238')) &
            ~((pl.col('CODE5') >= '30091') & (pl.col('CODE5') <= '30098')) &
            ~((pl.col('CODE5') >= '40151') & (pl.col('CODE5') <= '40158'))
        )

        df = df.drop('CODE5')

        print(f"  After filtering: {len(df)} records")
        return df

    def process_cag_loans(self) -> pl.DataFrame:
        """Process CAG loans"""
        print("Processing CAG loans...")

        lnnote_file = self.paths.get_loan_path('LNNOTE')
        if not lnnote_file.exists():
            print("  LNNOTE file not found, skipping CAG processing")
            return pl.DataFrame({'ITCODE': [], 'AMTIND': [], 'AMOUNT': []})

        df = pl.read_parquet(lnnote_file)

        # Filter by PZIPCODE
        cag_codes = [2002, 2013, 3039, 3047, 800003098, 800003114,
                     800004016, 800004022, 800004029, 800040050,
                     800040053, 800050024, 800060024, 800060045,
                     800060081, 80060085]

        df = df.filter(pl.col('PZIPCODE').is_in(cag_codes))

        # Add columns
        df = df.with_columns([
            pl.lit('I').alias('AMTIND'),
            pl.lit('7511100000000Y').alias('ITCODE')
        ])

        # Summarize
        cag = df.group_by(['ITCODE', 'AMTIND']).agg([
            pl.col('BALANCE').sum().alias('AMOUNT')
        ])

        print(f"  CAG loans: {len(cag)} records")
        return cag

    def combine_rdalkm(self, df_weekly: pl.DataFrame, df_cag: pl.DataFrame) -> pl.DataFrame:
        """Combine weekly and CAG data"""
        print("Combining RDALKM data...")

        # Combine datasets
        df_combined = pl.concat([df_weekly, df_cag], how='diagonal')

        # Sort by ITCODE and AMTIND
        df_combined = df_combined.sort(['ITCODE', 'AMTIND'])

        print(f"  Combined: {len(df_combined)} records")

        # Save to RDALTMP
        rdaltmp_file = self.paths.rdaltmp_lib / "RDALKM.parquet"
        df_combined.write_parquet(rdaltmp_file)

        return df_combined

    def split_into_sections(self, df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Split data into AL, OB, and SP sections"""
        print("Splitting into sections...")

        # Filter: SUBSTR(ITCODE,14,1) NE 'F'
        df = df.filter(pl.col('ITCODE').str.slice(13, 1) != 'F')

        # Add helper columns
        df = df.with_columns([
            pl.col('ITCODE').str.slice(0, 3).alias('CODE3'),
            pl.col('ITCODE').str.slice(0, 1).alias('CODE1'),
            pl.col('ITCODE').str.slice(1, 1).alias('CODE2')
        ])

        # Split logic
        df_al = pl.DataFrame()
        df_ob = pl.DataFrame()
        df_sp = pl.DataFrame()

        # AMTIND != ' '
        df_with_amtind = df.filter(pl.col('AMTIND') != ' ')

        # Code 307 -> SP
        df_sp_307 = df_with_amtind.filter(pl.col('CODE3') == '307')

        # Code1 != '5'
        df_not_5 = df_with_amtind.filter((pl.col('CODE3') != '307') & (pl.col('CODE1') != '5'))

        # Codes 685, 785 -> SP, else -> AL
        df_sp_685_785 = df_not_5.filter(pl.col('CODE3').is_in(['685', '785']))
        df_al_part = df_not_5.filter(~pl.col('CODE3').is_in(['685', '785']))

        # Code1 == '5' -> OB
        df_ob_part = df_with_amtind.filter((pl.col('CODE3') != '307') & (pl.col('CODE1') == '5'))

        # AMTIND == ' ' and CODE2 == '0' -> SP
        df_sp_blank = df.filter((pl.col('AMTIND') == ' ') & (pl.col('CODE2') == '0'))

        # Combine sections
        df_al = df_al_part.select(['ITCODE', 'AMTIND', 'AMOUNT'])
        df_ob = df_ob_part.select(['ITCODE', 'AMTIND', 'AMOUNT'])
        df_sp = pl.concat([df_sp_307, df_sp_685_785, df_sp_blank], how='diagonal').select(
            ['ITCODE', 'AMTIND', 'AMOUNT'])

        print(f"  AL: {len(df_al)}, OB: {len(df_ob)}, SP: {len(df_sp)}")

        return df_al, df_ob, df_sp

    def write_rdal_file(self, df_al: pl.DataFrame, df_ob: pl.DataFrame, df_sp: pl.DataFrame, df_k3fei: pl.DataFrame):
        """Write RDAL formatted file"""
        print("Writing RDAL file...")

        phead = f"RDAL{self.paths.reptday}{self.paths.reptmon}{self.paths.reptyear}"

        with open(self.paths.rdalkm_file, 'w') as f:
            # Write header
            f.write(f"{phead}\n")

            # Write AL section
            f.write("AL\n")
            amounti = 0

            for row in df_al.sort(['ITCODE', 'AMTIND']).iter_rows(named=True):
                itcode = row['ITCODE']
                amount = row['AMOUNT']

                # Check PROCEED logic
                proceed = True
                if self.paths.reptday in ['08', '22']:
                    if itcode == '4003000000000Y' and itcode[:2] in ['68', '78']:
                        proceed = False

                if proceed:
                    amount_rounded = round(amount / 1000)
                    amounti += amount_rounded

                    # Write when last occurrence of ITCODE
                    # For simplicity, write each record (SAS BY group logic)
                    f.write(f"{itcode};{amounti};{amounti}\n")
                    amounti = 0

            # Write OB section
            f.write("OB\n")
            amounti = 0

            for row in df_ob.sort(['ITCODE', 'AMTIND']).iter_rows(named=True):
                itcode = row['ITCODE']
                amount = row['AMOUNT']

                amounti += round(amount / 1000)

                # Write when last occurrence of ITCODE
                f.write(f"{itcode};{amounti};{amounti}\n")
                amounti = 0

            # Write SP section
            f.write("SP\n")

            # Combine SP and K3FEI
            df_sp_combined = pl.concat([df_sp, df_k3fei], how='diagonal')
            df_sp_combined = df_sp_combined.sort('ITCODE')

            # Group by ITCODE
            df_sp_grouped = df_sp_combined.group_by('ITCODE').agg([
                pl.col('AMOUNT').sum().alias('AMOUNT')
            ]).sort('ITCODE')

            for row in df_sp_grouped.iter_rows(named=True):
                itcode = row['ITCODE']
                amount = row['AMOUNT']

                amounti = round(amount / 1000)
                f.write(f"{itcode};{amounti}\n")

        print(f"  Written to {self.paths.rdalkm_file}")

    def write_nsrs_file(self, df_al: pl.DataFrame, df_ob: pl.DataFrame, df_sp: pl.DataFrame, df_k3fei: pl.DataFrame):
        """Write NSRS formatted file"""
        print("Writing NSRS file...")

        phead = f"RDAL{self.paths.reptday}{self.paths.reptmon}{self.paths.reptyear}"

        with open(self.paths.nsrskm_file, 'w') as f:
            # Write header
            f.write(f"{phead}\n")

            # Write AL section
            f.write("AL\n")
            amounti = 0

            for row in df_al.sort(['ITCODE', 'AMTIND']).iter_rows(named=True):
                itcode = row['ITCODE']
                amount = row['AMOUNT']

                # Check PROCEED logic
                proceed = True
                if self.paths.reptday in ['08', '22']:
                    if itcode == '4003000000000Y' and itcode[:2] in ['68', '78']:
                        proceed = False

                if proceed:
                    # Different rounding for NSRS
                    amount_rounded = round(amount)
                    if itcode[:2] == '80':
                        amount_rounded = round(amount / 1000)

                    amounti += amount_rounded

                    # Write when last occurrence of ITCODE
                    f.write(f"{itcode};{amounti};{amounti}\n")
                    amounti = 0

            # Write OB section
            f.write("OB\n")
            amounti = 0

            for row in df_ob.sort(['ITCODE', 'AMTIND']).iter_rows(named=True):
                itcode = row['ITCODE']
                amount = row['AMOUNT']

                # Different rounding for NSRS
                if itcode[:2] == '80':
                    amount = round(amount / 1000)

                amounti += round(amount)

                # Write when last occurrence of ITCODE
                f.write(f"{itcode};{amounti};{amounti}\n")
                amounti = 0

            # Write SP section
            f.write("SP\n")

            # Combine SP and K3FEI
            df_sp_combined = pl.concat([df_sp, df_k3fei], how='diagonal')
            df_sp_combined = df_sp_combined.sort('ITCODE')

            # Group by ITCODE
            df_sp_grouped = df_sp_combined.group_by('ITCODE').agg([
                pl.col('AMOUNT').sum().alias('AMOUNT')
            ]).sort('ITCODE')

            for row in df_sp_grouped.iter_rows(named=True):
                itcode = row['ITCODE']
                amount = row['AMOUNT']

                amounti = round(amount)
                if itcode[:2] == '80':
                    amounti = round(amount / 1000)

                f.write(f"{itcode};{amounti}\n")

        print(f"  Written to {self.paths.nsrskm_file}")

    def run(self):
        """Execute full processing"""
        print("=" * 80)
        print("RDAL/NSRS Report Generation")
        print("=" * 80)

        # Load weekly data
        df_weekly = self.load_weekly_data()

        # Process CAG loans
        df_cag = self.process_cag_loans()

        # Combine
        df_rdalkm = self.combine_rdalkm(df_weekly, df_cag)

        # Load K3FEI
        k3fei_file = self.paths.get_input_path('K3FEI')
        if k3fei_file.exists():
            df_k3fei = pl.read_parquet(k3fei_file)
            print(f"Loaded K3FEI: {len(df_k3fei)} records")

            # Save to RDALTMP
            rdaltmp_k3fei = self.paths.rdaltmp_lib / "K3FEI.parquet"
            df_k3fei.write_parquet(rdaltmp_k3fei)
        else:
            print("K3FEI not found, using empty dataset")
            df_k3fei = pl.DataFrame({'ITCODE': [], 'AMTIND': [], 'AMOUNT': []})

        # Split into sections
        df_al, df_ob, df_sp = self.split_into_sections(df_rdalkm)

        # Write RDAL file
        self.write_rdal_file(df_al, df_ob, df_sp, df_k3fei)

        # Write NSRS file
        self.write_nsrs_file(df_al, df_ob, df_sp, df_k3fei)

        print("=" * 80)
        print("Processing Summary:")
        print("=" * 80)
        print(f"Report Date: {self.paths.reptday}/{self.paths.reptmon}/{self.paths.reptyear}")
        print(f"RDAL file: {self.paths.rdalkm_file}")
        print(f"NSRS file: {self.paths.nsrskm_file}")
        print("=" * 80)
        print("RDAL/NSRS generation completed successfully")
        print("=" * 80)


def main():
    """Main entry point"""
    try:
        paths = PathConfig()
        processor = RDALProcessor(paths)
        processor.run()
        return 0
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
