#!/usr/bin/env python3
"""
Program: EIBPTH1A
Purpose: PBB RDAL/NSRS Report Generation

- This program generates RDAL and NSRS formatted text files for Public Bank Berhad,
    including Assets and Liabilities (AL), Off-Balance Sheet (OB), and Special (SP) sections.
Supports D (Domestic), I (Islamic), and F (Foreign) amount indicators.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import polars as pl
import pyarrow.parquet as pq
from typing import Dict, Tuple


class PathConfig:
    """Path configuration"""

    def __init__(self):
        base_path = Path(os.getenv('BNM_DATA_PATH', '/data'))
        output_path = Path(os.getenv('BNM_OUTPUT_PATH', '/data/output'))

        # Input paths
        self.loan_lib = base_path / "LOAN"
        self.pbcs_lib = base_path / "PBCS"

        # BNM library
        self.bnm_lib = output_path / "BNM"
        self.bnm_lib.mkdir(parents=True, exist_ok=True)

        # Calculate reporting date
        self.calculate_dates()

        # Output files
        self.rdalkm_file = output_path / "SAP.PBB.KAPMNI.RDAL.PBCS.txt"
        self.nsrskm_file = output_path / "SAP.PBB.NSRS.KAPMNI.RDAL.PBCS.txt"
        self.sftp01_file = output_path / "FTPPUT.txt"

    def calculate_dates(self):
        """Calculate reporting dates - last day of previous month"""
        today = datetime.now()
        first_of_month = datetime(today.year, today.month, 1)
        self.reptdate = first_of_month - timedelta(days=1)

        day = self.reptdate.day

        # Determine week and SDD
        if day == 8:
            self.sdd = 1
            self.wk = '1'
            self.wk1 = '4'
            self.wk2 = None
            self.wk3 = None
        elif day == 15:
            self.sdd = 9
            self.wk = '2'
            self.wk1 = '1'
            self.wk2 = None
            self.wk3 = None
        elif day == 22:
            self.sdd = 16
            self.wk = '3'
            self.wk1 = '2'
            self.wk2 = None
            self.wk3 = None
        else:
            self.sdd = 23
            self.wk = '4'
            self.wk1 = '3'
            self.wk2 = '2'
            self.wk3 = '1'

        self.mm = self.reptdate.month

        if self.wk == '1':
            self.mm1 = self.mm - 1
            if self.mm1 == 0:
                self.mm1 = 12
        else:
            self.mm1 = self.mm

        self.mm2 = self.mm - 1
        if self.mm2 == 0:
            self.mm2 = 12

        self.sdate = datetime(self.reptdate.year, self.mm, self.sdd)
        self.sdesc = 'PUBLIC BANK BERHAD'

        # Format strings
        self.nowk = self.wk
        self.reptmon = f"{self.mm:02d}"
        self.reptmon1 = f"{self.mm1:02d}"
        self.reptmon2 = f"{self.mm2:02d}"
        self.reptyear = f"{self.reptdate.year}"
        self.reptyr = f"{self.reptdate.year % 100:02d}"
        self.reptday = f"{self.reptdate.day:02d}"
        self.rdate = self.reptdate.strftime('%d%m%Y')
        self.fdate = self.reptdate.strftime('%d%m%y')
        self.tdate = self.reptdate

    def get_input_path(self, dataset_name: str) -> Path:
        """Get input path for a dataset"""
        return self.bnm_lib / f"{dataset_name}.parquet"

    def get_pbcs_path(self, dataset_name: str) -> Path:
        """Get PBCS library path"""
        return self.pbcs_lib / f"{dataset_name}.parquet"

    def get_loan_path(self, dataset_name: str) -> Path:
        """Get loan library path"""
        return self.loan_lib / f"{dataset_name}.parquet"


class PBBRDALProcessor:
    """Main processor for PBB RDAL/NSRS generation"""

    def __init__(self, paths: PathConfig):
        self.paths = paths

    def load_weekly_data(self) -> pl.DataFrame:
        """Load and combine weekly data from ALWKM and PBCS"""
        print("Loading weekly data...")

        datasets = []

        # Read ALWKM dataset
        alwkm_file = self.paths.get_input_path(f"ALWKM{self.paths.reptmon}{self.paths.nowk}")
        if alwkm_file.exists():
            df_alwkm = pl.read_parquet(alwkm_file)
            datasets.append(df_alwkm)
            print(f"  Loaded ALWKM: {len(df_alwkm)} records")

        # Read PBCS CCLW dataset
        cclw_file = self.paths.get_pbcs_path(f"CCLW{self.paths.reptmon}{self.paths.nowk}")
        if cclw_file.exists():
            df_cclw = pl.read_parquet(cclw_file)
            datasets.append(df_cclw)
            print(f"  Loaded CCLW: {len(df_cclw)} records")

        if not datasets:
            return pl.DataFrame({'ITCODE': [], 'AMTIND': [], 'AMOUNT': []})

        df = pl.concat(datasets, how='diagonal')

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

        # Note: PRODCD and AMTIND would need LNPROD and LNDENOM format mappings
        # For now, using placeholder logic
        df = df.with_columns([
            pl.lit('I').alias('AMTIND'),  # Placeholder - should use LNDENOM format
            pl.lit('7511100000000Y').alias('ITCODE')
        ])

        # Summarize
        cag = df.group_by(['ITCODE', 'AMTIND']).agg([
            pl.col('BALANCE').sum().alias('AMOUNT')
        ])

        print(f"  CAG loans: {len(cag)} records")
        return cag

    def split_into_sections(self, df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Split data into AL, OB, and SP sections"""
        print("Splitting into sections...")

        # Filter: SUBSTR(ITCODE,14,1) NOT IN ('F','#')
        df = df.filter(~pl.col('ITCODE').str.slice(13, 1).is_in(['F', '#']))

        # Add helper columns
        df = df.with_columns([
            pl.col('ITCODE').str.slice(0, 3).alias('CODE3'),
            pl.col('ITCODE').str.slice(0, 1).alias('CODE1'),
            pl.col('ITCODE').str.slice(1, 1).alias('CODE2')
        ])

        # Split logic
        df_with_amtind = df.filter(pl.col('AMTIND') != ' ')

        df_sp_307 = df_with_amtind.filter(pl.col('CODE3') == '307')
        df_not_5 = df_with_amtind.filter((pl.col('CODE3') != '307') & (pl.col('CODE1') != '5'))
        df_sp_685_785 = df_not_5.filter(pl.col('CODE3').is_in(['685', '785']))
        df_al_part = df_not_5.filter(~pl.col('CODE3').is_in(['685', '785']))
        df_ob_part = df_with_amtind.filter((pl.col('CODE3') != '307') & (pl.col('CODE1') == '5'))
        df_sp_blank = df.filter((pl.col('AMTIND') == ' ') & (pl.col('CODE2') == '0'))

        df_al = df_al_part.select(['ITCODE', 'AMTIND', 'AMOUNT'])
        df_ob = df_ob_part.select(['ITCODE', 'AMTIND', 'AMOUNT'])
        df_sp = pl.concat([df_sp_307, df_sp_685_785, df_sp_blank], how='diagonal').select(
            ['ITCODE', 'AMTIND', 'AMOUNT'])

        print(f"  AL: {len(df_al)}, OB: {len(df_ob)}, SP: {len(df_sp)}")

        return df_al, df_ob, df_sp

    def process_hash_codes(self, df: pl.DataFrame) -> pl.DataFrame:
        """Process codes with # - replace with Y and negate amount"""
        print("Processing hash codes...")

        df = df.with_columns([
            pl.when(pl.col('ITCODE').str.slice(13, 1) == '#')
            .then(pl.col('ITCODE').str.slice(0, 13) + 'Y')
            .otherwise(pl.col('ITCODE'))
            .alias('ITCODE'),

            pl.when(pl.col('ITCODE').str.slice(13, 1) == '#')
            .then(pl.col('AMOUNT') * -1)
            .otherwise(pl.col('AMOUNT'))
            .alias('AMOUNT')
        ])

        # Summarize after hash processing
        df = df.group_by(['ITCODE', 'AMTIND']).agg([
            pl.col('AMOUNT').sum().alias('AMOUNT')
        ])

        print(f"  After hash processing: {len(df)} records")
        return df

    def write_rdal_file(self, df_al: pl.DataFrame, df_ob: pl.DataFrame, df_sp: pl.DataFrame,
                        df_k3fei: pl.DataFrame, df_kapx: pl.DataFrame):
        """Write RDAL formatted file"""
        print("Writing RDAL file...")

        phead = f"RDAL{self.paths.reptday}{self.paths.reptmon}{self.paths.reptyear}"

        with open(self.paths.rdalkm_file, 'w') as f:
            # Write header
            f.write(f"{phead}\n")

            # Write AL section
            f.write("AL\n")
            current_itcode = None
            amountd = amounti = amountf = 0

            for row in df_al.sort(['ITCODE', 'AMTIND']).iter_rows(named=True):
                itcode = row['ITCODE']
                amtind = row['AMTIND']
                amount = row['AMOUNT']

                # Check PROCEED logic
                proceed = True
                if self.paths.reptday in ['08', '22']:
                    if itcode == '4003000000000Y' and itcode[:2] in ['68', '78']:
                        proceed = False

                if proceed:
                    if current_itcode is None:
                        current_itcode = itcode

                    if itcode != current_itcode:
                        # Write previous ITCODE
                        total = amountd + amounti + amountf
                        f.write(f"{current_itcode};{total};{amounti};{amountf}\n")
                        current_itcode = itcode
                        amountd = amounti = amountf = 0

                    amount_rounded = round(amount / 1000)
                    if amtind == 'D':
                        amountd += amount_rounded
                    elif amtind == 'I':
                        amounti += amount_rounded
                    elif amtind == 'F':
                        amountf += amount_rounded

            # Write last ITCODE
            if current_itcode is not None:
                total = amountd + amounti + amountf
                f.write(f"{current_itcode};{total};{amounti};{amountf}\n")

            # Write OB section
            f.write("OB\n")
            current_itcode = None
            amountd = amounti = amountf = 0

            for row in df_ob.sort(['ITCODE', 'AMTIND']).iter_rows(named=True):
                itcode = row['ITCODE']
                amtind = row['AMTIND']
                amount = row['AMOUNT']

                if current_itcode is None:
                    current_itcode = itcode

                if itcode != current_itcode:
                    total = amountd + amounti + amountf
                    f.write(f"{current_itcode};{total};{amounti};{amountf}\n")
                    current_itcode = itcode
                    amountd = amounti = amountf = 0

                amount_rounded = round(amount / 1000)
                if amtind == 'D':
                    amountd += amount_rounded
                elif amtind == 'I':
                    amounti += amount_rounded
                elif amtind == 'F':
                    amountf += amount_rounded

            if current_itcode is not None:
                total = amountd + amounti + amountf
                f.write(f"{current_itcode};{total};{amounti};{amountf}\n")

            # Write SP section
            f.write("SP\n")

            # Combine SP, K3FEI, and KAPX
            df_sp_combined = pl.concat([df_sp, df_k3fei, df_kapx], how='diagonal')
            df_sp_combined = df_sp_combined.sort('ITCODE')

            current_itcode = None
            amountd = amountf = 0

            for row in df_sp_combined.iter_rows(named=True):
                itcode = row['ITCODE']
                amtind = row.get('AMTIND', 'D')
                amount = row['AMOUNT']

                if current_itcode is None:
                    current_itcode = itcode

                if itcode != current_itcode:
                    total = amountd + amountf
                    f.write(f"{current_itcode};{total};{amountf}\n")
                    current_itcode = itcode
                    amountd = amountf = 0

                amount_rounded = round(amount / 1000)
                if amtind == 'D':
                    amountd += amount_rounded
                elif amtind == 'F':
                    amountf += amount_rounded

            if current_itcode is not None:
                total = amountd + amountf
                f.write(f"{current_itcode};{total};{amountf}\n")

        print(f"  Written to {self.paths.rdalkm_file}")

    def write_nsrs_file(self, df_al: pl.DataFrame, df_ob: pl.DataFrame, df_sp: pl.DataFrame,
                        df_k3fei: pl.DataFrame, df_kapx: pl.DataFrame):
        """Write NSRS formatted file"""
        print("Writing NSRS file...")

        phead = f"RDAL{self.paths.reptday}{self.paths.reptmon}{self.paths.reptyear}"

        with open(self.paths.nsrskm_file, 'w') as f:
            # Write header
            f.write(f"{phead}\n")

            # Write AL section
            f.write("AL\n")
            current_itcode = None
            amountd = amounti = amountf = 0

            for row in df_al.sort(['ITCODE', 'AMTIND']).iter_rows(named=True):
                itcode = row['ITCODE']
                amtind = row['AMTIND']
                amount = row['AMOUNT']

                proceed = True
                if self.paths.reptday in ['08', '22']:
                    if itcode == '4003000000000Y' and itcode[:2] in ['68', '78']:
                        proceed = False

                if proceed:
                    if current_itcode is None:
                        current_itcode = itcode

                    if itcode != current_itcode:
                        total = amountd + amounti + amountf
                        f.write(f"{current_itcode};{total};{amounti};{amountf}\n")
                        current_itcode = itcode
                        amountd = amounti = amountf = 0

                    amount_rounded = round(amount)
                    if itcode[:2] == '80':
                        amount_rounded = round(amount / 1000)

                    if amtind == 'D':
                        amountd += amount_rounded
                    elif amtind == 'I':
                        amounti += amount_rounded
                    elif amtind == 'F':
                        amountf += amount_rounded

            if current_itcode is not None:
                total = amountd + amounti + amountf
                f.write(f"{current_itcode};{total};{amounti};{amountf}\n")

            # Write OB section
            f.write("OB\n")
            current_itcode = None
            amountd = amounti = amountf = 0

            for row in df_ob.sort(['ITCODE', 'AMTIND']).iter_rows(named=True):
                itcode = row['ITCODE']
                amtind = row['AMTIND']
                amount = row['AMOUNT']

                if current_itcode is None:
                    current_itcode = itcode

                if itcode != current_itcode:
                    total = amountd + amounti + amountf
                    f.write(f"{current_itcode};{total};{amounti};{amountf}\n")
                    current_itcode = itcode
                    amountd = amounti = amountf = 0

                if itcode[:2] == '80':
                    amount = round(amount / 1000)

                amount_rounded = round(amount)
                if amtind == 'D':
                    amountd += amount_rounded
                elif amtind == 'I':
                    amounti += amount_rounded
                elif amtind == 'F':
                    amountf += amount_rounded

            if current_itcode is not None:
                total = amountd + amounti + amountf
                f.write(f"{current_itcode};{total};{amounti};{amountf}\n")

            # Write SP section
            f.write("SP\n")

            df_sp_combined = pl.concat([df_sp, df_k3fei, df_kapx], how='diagonal')
            df_sp_combined = df_sp_combined.sort('ITCODE')

            current_itcode = None
            amountd = amountf = 0

            for row in df_sp_combined.iter_rows(named=True):
                itcode = row['ITCODE']
                amtind = row.get('AMTIND', 'D')
                amount = row['AMOUNT']

                if current_itcode is None:
                    current_itcode = itcode

                if itcode != current_itcode:
                    total = amountd + amountf
                    if current_itcode[:2] == '80':
                        total = round(total / 1000)
                    f.write(f"{current_itcode};{total};{amountf}\n")
                    current_itcode = itcode
                    amountd = amountf = 0

                amount_rounded = round(amount)
                if amtind == 'D':
                    amountd += amount_rounded
                elif amtind == 'F':
                    amountf += amount_rounded

            if current_itcode is not None:
                total = amountd + amountf
                if current_itcode[:2] == '80':
                    total = round(total / 1000)
                f.write(f"{current_itcode};{total};{amountf}\n")

        print(f"  Written to {self.paths.nsrskm_file}")

    def generate_sftp_commands(self):
        """Generate SFTP commands file"""
        print("Generating SFTP commands...")

        with open(self.paths.sftp01_file, 'w') as f:
            f.write(f'PUT //SAP.PBB.NSRS.KAPMNI.RDAL.PBCS kapmni_EAB_PBCS_{self.paths.fdate}.txt\n')

        print(f"  Written to {self.paths.sftp01_file}")

    def run(self):
        """Execute full processing"""
        print("=" * 80)
        print("EIBPTH1A - PBB RDAL/NSRS Report Generation")
        print("=" * 80)
        print(f"Bank: {self.paths.sdesc}")
        print(f"Report Date: {self.paths.rdate}")
        print(f"Week: {self.paths.nowk}")
        print("=" * 80)

        # Load weekly data
        df_weekly = self.load_weekly_data()

        # Process CAG loans
        df_cag = self.process_cag_loans()

        # Combine
        df_rdalkm = pl.concat([df_weekly, df_cag], how='diagonal')
        df_rdalkm = df_rdalkm.sort(['ITCODE', 'AMTIND'])

        # Split into initial sections
        df_al_init, df_ob_init, df_sp_init = self.split_into_sections(df_rdalkm)

        # Load K3FEI and KAPX
        k3fei_file = self.paths.get_input_path('K3FEI')
        df_k3fei = pl.read_parquet(k3fei_file) if k3fei_file.exists() else pl.DataFrame(
            {'ITCODE': [], 'AMTIND': [], 'AMOUNT': []})

        kapx_file = self.paths.get_input_path('KAPX')
        df_kapx = pl.read_parquet(kapx_file) if kapx_file.exists() else pl.DataFrame(
            {'ITCODE': [], 'AMTIND': [], 'AMOUNT': []})

        # Write RDAL file
        self.write_rdal_file(df_al_init, df_ob_init, df_sp_init, df_k3fei, df_kapx)

        # Process hash codes for NSRS
        df_rdalkm_hash = self.process_hash_codes(df_rdalkm)
        df_al_final, df_ob_final, df_sp_final = self.split_into_sections(df_rdalkm_hash)

        # Write NSRS file
        self.write_nsrs_file(df_al_final, df_ob_final, df_sp_final, df_k3fei, df_kapx)

        # Generate SFTP commands
        self.generate_sftp_commands()

        print("=" * 80)
        print("EIBPTH1A processing completed successfully")
        print("=" * 80)


def main():
    """Main entry point"""
    try:
        paths = PathConfig()
        processor = PBBRDALProcessor(paths)
        processor.run()
        return 0
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
