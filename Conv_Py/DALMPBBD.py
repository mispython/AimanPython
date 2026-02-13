# !/usr/bin/env python3
"""
Program           : DALMPBBD
Invoke by program : PBBMTH1/PBBQTR1
Purpose           : PBB deposit manipulation extracted from SAP.PBB.MNITB
                    Manipulates extracted saving and current accounts database.
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import duckdb
import polars as pl
import pyarrow.parquet as pq
from typing import Dict, Optional

# Import format mappings from PBBDPFMT
try:
    from PBBDPFMT import get_format_mappings, ACE_PRODUCTS
except ImportError:
    print("Warning: PBBDPFMT module not found. Using default mappings.")
    ACE_PRODUCTS = []


    def get_format_mappings():
        return {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}


class PathConfig:
    """Path configuration for DALMPBBD"""

    def __init__(self):
        base_path = Path(os.getenv('BNM_DATA_PATH', '/data'))
        output_path = Path(os.getenv('BNM_OUTPUT_PATH', '/data/output'))

        # Input files
        self.deposit_saving = base_path / "DEPOSIT.SAVING.parquet"
        self.deposit_current = base_path / "DEPOSIT.CURRENT.parquet"
        self.cisdp = base_path / "CISDP.parquet"

        # Output directory (BNM library)
        self.bnm_lib = output_path / "BNM"
        self.bnm_lib.mkdir(parents=True, exist_ok=True)

        # Macro variables from environment
        self.reptmon = os.getenv('REPTMON', '01')
        self.nowk = os.getenv('NOWK', '1')
        self.reptyear = int(os.getenv('REPTYEAR', str(datetime.now().year)))
        self.reptday = int(os.getenv('REPTDAY', '01'))

    def get_output_path(self, dataset_name: str) -> Path:
        """Get output path for a dataset"""
        return self.bnm_lib / f"{dataset_name}.parquet"


class FormatMapper:
    """Handle SAS format mappings"""

    def __init__(self):
        (self.sacustcd_map, self.statecd_map, self.saprod_map, self.sadenom_map,
         self.race_map, self.sdrange_map, self.s2range_map, self.ddcustcd_map,
         self.caprod_map, self.cadenom_map, self.ddrange_map,
         self.sector_map, self.ace_products) = get_format_mappings()

        if not self.ace_products:
            self.ace_products = ACE_PRODUCTS

    def apply_sacustcd(self, custcode):
        """Apply SACUSTCD format"""
        return self.sacustcd_map.get(custcode, '00')

    def apply_statecd(self, branch):
        """Apply STATECD format"""
        return self.statecd_map.get(branch, 'X')

    def apply_saprod(self, product):
        """Apply SAPROD format"""
        return self.saprod_map.get(product, 'N')

    def apply_sadenom(self, product):
        """Apply SADENOM format"""
        return self.sadenom_map.get(product, 'X')

    def apply_race(self, race):
        """Apply RACE format"""
        return self.race_map.get(str(race), '9')

    def apply_sdrange(self, curbal):
        """Apply SDRANGE format - savings deposit range"""
        if curbal < 100:
            return 1
        elif curbal < 500:
            return 2
        elif curbal < 1000:
            return 3
        elif curbal < 5000:
            return 4
        elif curbal < 10000:
            return 5
        elif curbal < 50000:
            return 6
        elif curbal < 100000:
            return 7
        elif curbal < 500000:
            return 8
        else:
            return 9

    def apply_s2range(self, curbal):
        """Apply S2RANGE format - alternate savings range"""
        if curbal < 1000:
            return 1
        elif curbal < 5000:
            return 2
        elif curbal < 10000:
            return 3
        elif curbal < 50000:
            return 4
        elif curbal < 100000:
            return 5
        elif curbal < 500000:
            return 6
        else:
            return 7

    def apply_ddcustcd(self, custcode):
        """Apply DDCUSTCD format"""
        return self.ddcustcd_map.get(custcode, '00')

    def apply_caprod(self, product):
        """Apply CAPROD format"""
        return self.caprod_map.get(product, 'N')

    def apply_cadenom(self, product):
        """Apply CADENOM format"""
        return self.cadenom_map.get(product, 'X')

    def apply_ddrange(self, amount):
        """Apply DDRANGE format - demand deposit range"""
        if amount < 100:
            return 1
        elif amount < 500:
            return 2
        elif amount < 1000:
            return 3
        elif amount < 5000:
            return 4
        elif amount < 10000:
            return 5
        elif amount < 50000:
            return 6
        elif amount < 100000:
            return 7
        elif amount < 500000:
            return 8
        else:
            return 9


class DepositProcessor:
    """Main processor for deposit data"""

    def __init__(self, paths: PathConfig):
        self.paths = paths
        self.mapper = FormatMapper()
        self.conn = duckdb.connect(':memory:')

        # Constants
        self.AGELIMIT = 12
        self.MAXAGE = 18
        self.AGEBELOW = 11

    def calculate_age(self, bdate, reptyear, reptmon, reptday):
        """Calculate age with SAS logic"""
        if bdate is None or bdate == 0:
            return 0

        try:
            # Convert MMDDYYYY format to date
            bdate_str = str(int(bdate)).zfill(8)
            bmonth = int(bdate_str[0:2])
            bday = int(bdate_str[2:4])
            byear = int(bdate_str[4:8])

            age = reptyear - byear

            # Adjust age based on month/day comparison
            if age == self.AGELIMIT:
                if (bmonth == reptmon and bday > reptday) or bmonth > reptmon:
                    age = self.AGEBELOW
            elif age == self.MAXAGE:
                if (bmonth == reptmon and bday > reptday) or bmonth > reptmon:
                    age = self.AGELIMIT
            elif age > self.MAXAGE:
                age = self.MAXAGE
            elif age < self.AGELIMIT:
                age = self.AGEBELOW
            else:
                age = self.AGELIMIT

            return age
        except:
            return 0

    def convert_opendt(self, opendt):
        """Convert OPENDT to date format"""
        if opendt is None or opendt == 0:
            return 0
        try:
            opendt_str = str(int(opendt)).zfill(8)
            return int(opendt_str)
        except:
            return 0

    def process_saving_accounts(self):
        """Process saving accounts - creates SAVG dataset"""
        print(f"Processing saving accounts -> SAVG{self.paths.reptmon}{self.paths.nowk}")

        # Read saving accounts
        df = pl.read_parquet(self.paths.deposit_saving)

        # Filter: OPENIND NOT IN ('B','C','P') AND CURBAL >= 0
        df = df.filter(
            (~pl.col('OPENIND').is_in(['B', 'C', 'P'])) &
            (pl.col('CURBAL') >= 0)
        )

        # Apply format mappings
        df = df.with_columns([
            pl.col('CUSTCODE').map_elements(self.mapper.apply_sacustcd, return_dtype=pl.Utf8).alias('CUSTCD'),
            pl.col('BRANCH').map_elements(self.mapper.apply_statecd, return_dtype=pl.Utf8).alias('STATECD'),
            pl.col('PRODUCT').map_elements(self.mapper.apply_saprod, return_dtype=pl.Utf8).alias('PRODCD'),
            pl.col('PRODUCT').map_elements(self.mapper.apply_sadenom, return_dtype=pl.Utf8).alias('AMTIND'),
            pl.col('CURBAL').map_elements(self.mapper.apply_sdrange, return_dtype=pl.Int64).alias('RANGE'),
            pl.col('CURBAL').map_elements(self.mapper.apply_s2range, return_dtype=pl.Int64).alias('R2NGE'),
            pl.col('RACE').cast(pl.Utf8).map_elements(self.mapper.apply_race, return_dtype=pl.Utf8).alias('RACE'),
            pl.col('OPENDT').map_elements(self.convert_opendt, return_dtype=pl.Int64).alias('OPENDATE'),
        ])

        # Calculate AGE
        df = df.with_columns([
            pl.struct(['BDATE']).map_elements(
                lambda x: self.calculate_age(
                    x['BDATE'],
                    self.paths.reptyear,
                    int(self.paths.reptmon),
                    self.paths.reptday
                ),
                return_dtype=pl.Int64
            ).alias('AGE')
        ])

        # Select final columns
        savg_cols = [
            'BRANCH', 'DEPTYPE', 'PRODUCT', 'PRODCD', 'CUSTCD', 'STATECD', 'AGE',
            'RACE', 'INTPAYBL', 'CURBAL', 'OPENMH', 'CLOSEMH', 'RANGE', 'BDATE',
            'NAME', 'ACCTNO', 'LASTTRAN', 'ACCYTD', 'AMTIND', 'LEDGBAL', 'SCHIND',
            'BANKNO', 'OPENDATE', 'COSTCTR', 'R2NGE'
        ]

        df = df.select([col for col in savg_cols if col in df.columns])

        # Write output
        output_file = self.paths.get_output_path(f"SAVG{self.paths.reptmon}{self.paths.nowk}")
        df.write_parquet(output_file)
        print(f"  Written {len(df)} records to {output_file}")

        return df

    def process_current_accounts(self):
        """Process current accounts - creates CURN and FCY datasets"""
        print(
            f"Processing current accounts -> CURN{self.paths.reptmon}{self.paths.nowk}, FCY{self.paths.reptmon}{self.paths.nowk}")

        # Read current accounts
        df = pl.read_parquet(self.paths.deposit_current)

        # Filter: OPENIND NOT IN ('B','C','P') AND CURBAL >= 0
        df = df.filter(
            (~pl.col('OPENIND').is_in(['B', 'C', 'P'])) &
            (pl.col('CURBAL') >= 0)
        )

        # Apply format mappings
        df = df.with_columns([
            pl.col('BRANCH').map_elements(self.mapper.apply_statecd, return_dtype=pl.Utf8).alias('STATECD'),
            pl.col('PRODUCT').map_elements(self.mapper.apply_caprod, return_dtype=pl.Utf8).alias('PRODCD'),
            pl.col('PRODUCT').map_elements(self.mapper.apply_cadenom, return_dtype=pl.Utf8).alias('AMTIND'),
            pl.col('RACE').cast(pl.Utf8).map_elements(self.mapper.apply_race, return_dtype=pl.Utf8).alias('RACE'),
            pl.col('CURBAL').map_elements(self.mapper.apply_ddrange, return_dtype=pl.Int64).alias('RANGE'),
            pl.col('AVGAMT').map_elements(self.mapper.apply_ddrange, return_dtype=pl.Int64).alias('AVGRNGE'),
            pl.lit(0).alias('CABAL'),
            pl.lit(0).alias('SABAL'),
        ])

        # Handle CUSTCD with special logic for VOSTRO accounts
        df = df.with_columns([
            pl.when(pl.col('PRODUCT') == 104)
            .then(pl.lit('02'))
            .when(pl.col('PRODUCT') == 105)
            .then(pl.lit('81'))
            .otherwise(pl.col('CUSTCODE').map_elements(self.mapper.apply_ddcustcd, return_dtype=pl.Utf8))
            .alias('CUSTCD')
        ])

        # Split into ACE products, FCY products, and regular CURN
        ace_products = self.mapper.ace_products if self.mapper.ace_products else []

        # ACE products
        if ace_products:
            df_ace = df.filter(pl.col('PRODUCT').is_in(ace_products))
            df_ace = df_ace.with_columns([
                pl.lit(0).alias('INTPAYBL'),
                pl.col('PRODUCT').map_elements(self.mapper.apply_caprod, return_dtype=pl.Utf8).alias('PRODCD'),
                pl.col('PRODUCT').map_elements(self.mapper.apply_cadenom, return_dtype=pl.Utf8).alias('AMTIND'),
                pl.col('CURBAL').map_elements(self.mapper.apply_ddrange, return_dtype=pl.Int64).alias('RANGE'),
                pl.col('AVGAMT').map_elements(self.mapper.apply_ddrange, return_dtype=pl.Int64).alias('AVGRNGE'),
            ])
        else:
            df_ace = pl.DataFrame()

        # FCY products (400-444 or 450-454)
        df_fcy = df.filter(
            ((pl.col('PRODUCT') >= 400) & (pl.col('PRODUCT') <= 444)) |
            ((pl.col('PRODUCT') >= 450) & (pl.col('PRODUCT') <= 454))
        )

        # Adjust SECTOR for FCY
        df_fcy = df_fcy.with_columns([
            pl.when(pl.col('CUSTCD').is_in(['77', '78', '95']))
            .then(
                pl.when(pl.col('SECTOR').is_in([4, 5]))
                .then(pl.lit(1))
                .when(~pl.col('SECTOR').is_in([1, 2, 3, 4, 5]))
                .then(pl.lit(1))
                .otherwise(pl.col('SECTOR'))
            )
            .otherwise(
                pl.when(pl.col('SECTOR').is_in([1, 2, 3]))
                .then(pl.lit(4))
                .when(~pl.col('SECTOR').is_in([1, 2, 3, 4, 5]))
                .then(pl.lit(4))
                .otherwise(pl.col('SECTOR'))
            )
            .alias('SECTOR')
        ])

        # Regular CURN (excluding ACE and FCY)
        excluded_products = set(ace_products) if ace_products else set()
        df_curn = df.filter(
            ~pl.col('PRODUCT').is_in(ace_products) &
            ~(((pl.col('PRODUCT') >= 400) & (pl.col('PRODUCT') <= 444)) |
              ((pl.col('PRODUCT') >= 450) & (pl.col('PRODUCT') <= 454)))
        )

        # Combine ACE into CURN
        if len(df_ace) > 0:
            df_curn = pl.concat([df_curn, df_ace], how='diagonal')

        # Merge FCY with CISDP customer numbers
        if self.paths.cisdp.exists():
            df_cisdp = pl.read_parquet(self.paths.cisdp)
            df_cisdp = df_cisdp.select(['ACCTNO', 'CUSTNO']).unique(subset=['ACCTNO'])
            df_fcy = df_fcy.join(df_cisdp, on='ACCTNO', how='left')

        # Append FCY to CURN (drop CUSTNO column)
        if len(df_fcy) > 0:
            df_fcy_for_curn = df_fcy.drop('CUSTNO') if 'CUSTNO' in df_fcy.columns else df_fcy
            df_curn = pl.concat([df_curn, df_fcy_for_curn], how='diagonal')

        # Select final columns for CURN
        curn_cols = [
            'BRANCH', 'DEPTYPE', 'PRODUCT', 'PRODCD', 'CUSTCD', 'STATECD', 'CUSTNO',
            'RACE', 'INTPAYBL', 'CURBAL', 'OPENMH', 'CLOSEMH', 'RANGE', 'AVGAMT',
            'AVGRNGE', 'PURPOSE', 'SABAL', 'CABAL', 'AGE', 'NAME', 'ACCTNO', 'AMTIND',
            'LASTTRAN', 'ACCYTD', 'LEDGBAL', 'ODINTACC', 'COSTCTR', 'SECTOR'
        ]

        df_curn = df_curn.select([col for col in curn_cols if col in df_curn.columns])

        # Write output
        output_file = self.paths.get_output_path(f"CURN{self.paths.reptmon}{self.paths.nowk}")
        df_curn.write_parquet(output_file)
        print(f"  Written {len(df_curn)} records to {output_file}")

        return df_curn

    def summarize_for_rdal(self, df_savg, df_curn):
        """Summarize datasets at branch level for RDAL"""
        print(f"Summarizing for RDAL -> DEPT{self.paths.reptmon}{self.paths.nowk}")

        # Summary for SAVG
        dept_savg = df_savg.group_by(['BRANCH', 'STATECD', 'PRODCD', 'CUSTCD', 'AMTIND']).agg([
            pl.col('CURBAL').sum().alias('CURBAL'),
            pl.col('INTPAYBL').sum().alias('INTPAYBL'),
        ])

        # Summary for CURN (with SECTOR and MISSING option)
        dept_curn = df_curn.group_by(['BRANCH', 'STATECD', 'PRODCD', 'CUSTCD', 'SECTOR', 'AMTIND']).agg([
            pl.col('CURBAL').sum().alias('CURBAL'),
            pl.col('INTPAYBL').sum().alias('INTPAYBL'),
        ])

        # Combine (PROC APPEND with FORCE)
        dept_combined = pl.concat([dept_savg, dept_curn], how='diagonal')

        # Write output
        output_file = self.paths.get_output_path(f"DEPT{self.paths.reptmon}{self.paths.nowk}")
        dept_combined.write_parquet(output_file)
        print(f"  Written {len(dept_combined)} records to {output_file}")

    def process_save293(self):
        """Process SAVE293 summary"""
        print("Processing SAVE293 summary")

        # Read saving accounts
        df = pl.read_parquet(self.paths.deposit_saving)

        # Filter: OPENIND NOT IN ('B','C','P')
        df = df.filter(~pl.col('OPENIND').is_in(['B', 'C', 'P']))

        # Apply format mappings
        df = df.with_columns([
            pl.col('CUSTCODE').map_elements(self.mapper.apply_sacustcd, return_dtype=pl.Utf8).alias('CUSTCD'),
            pl.col('BRANCH').map_elements(self.mapper.apply_statecd, return_dtype=pl.Utf8).alias('STATECD'),
            pl.col('PRODUCT').map_elements(self.mapper.apply_saprod, return_dtype=pl.Utf8).alias('PRODCD'),
            pl.col('PRODUCT').map_elements(self.mapper.apply_sadenom, return_dtype=pl.Utf8).alias('AMTIND'),
            pl.lit(1).alias('OPENMH'),
        ])

        # Delete if PRODCD='N'
        df = df.filter(pl.col('PRODCD') != 'N')

        # Summary
        save293 = df.group_by(['BRANCH', 'STATECD', 'PRODCD', 'CUSTCD', 'AMTIND']).agg([
            pl.col('OPENMH').sum().alias('OPENMH'),
        ])

        return save293

    def process_curr293(self):
        """Process CURR293 summary"""
        print("Processing CURR293 summary")

        # Read current accounts
        df = pl.read_parquet(self.paths.deposit_current)

        # Filter: OPENIND NOT IN ('B','C','P') and BRANCH <= 900
        df = df.filter(
            (~pl.col('OPENIND').is_in(['B', 'C', 'P'])) &
            (pl.col('BRANCH') <= 900)
        )

        # Apply format mappings
        df = df.with_columns([
            pl.col('BRANCH').map_elements(self.mapper.apply_statecd, return_dtype=pl.Utf8).alias('STATECD'),
            pl.col('PRODUCT').map_elements(self.mapper.apply_caprod, return_dtype=pl.Utf8).alias('PRODCD'),
            pl.col('PRODUCT').map_elements(self.mapper.apply_cadenom, return_dtype=pl.Utf8).alias('AMTIND'),
            pl.lit('00').alias('CUSTCD'),
            pl.lit(1).alias('OPENMH'),
        ])

        # Special mapping for certain products
        df = df.with_columns([
            pl.when(pl.col('PRODUCT').is_in([160, 161, 162, 163, 164, 165, 166, 182]))
            .then(pl.lit('42310'))
            .otherwise(pl.lit('42110'))
            .alias('PRODCD'),

            pl.when(pl.col('PRODUCT').is_in([160, 161, 162, 163, 164, 165, 166, 182]))
            .then(pl.lit('I'))
            .otherwise(pl.lit('D'))
            .alias('AMTIND'),
        ])

        # Delete if PRODCD='N'
        df = df.filter(pl.col('PRODCD') != 'N')

        # Summary
        curr293 = df.group_by(['BRANCH', 'STATECD', 'PRODCD', 'CUSTCD', 'AMTIND']).agg([
            pl.col('OPENMH').sum().alias('OPENMH'),
        ])

        return curr293

    def run(self):
        """Execute full processing"""
        print("=" * 80)
        print("DALMPBBD - Deposit Manipulation Processing")
        print("=" * 80)

        # Process main datasets
        df_savg = self.process_saving_accounts()
        df_curn = self.process_current_accounts()

        # Summarize for RDAL
        self.summarize_for_rdal(df_savg, df_curn)

        # Process 293 summaries
        save293 = self.process_save293()
        curr293 = self.process_curr293()

        print("=" * 80)
        print("DALMPBBD processing completed successfully")
        print("=" * 80)


def main():
    """Main entry point"""
    try:
        paths = PathConfig()
        processor = DepositProcessor(paths)
        processor.run()
        return 0
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
