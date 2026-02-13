#!/usr/bin/env python3
"""
Program: FALMPBBD
Adding in foreign rate in the output.

Purpose: This program processes fixed deposit data and adds foreign exchange rates.
         Creates monthly and weekly FD datasets, plus UMA dataset.
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import polars as pl
import pyarrow.parquet as pq
from typing import Dict, Optional

# Import format mappings from PBBDPFMT
try:
    from PBBDPFMT import get_format_mappings
except ImportError:
    print("Warning: PBBDPFMT module not found. Using default mappings.")


    def get_format_mappings():
        return {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}


class PathConfig:
    """Path configuration for FALMPBBD"""

    def __init__(self):
        base_path = Path(os.getenv('BNM_DATA_PATH', '/data'))
        output_path = Path(os.getenv('BNM_OUTPUT_PATH', '/data/output'))

        # Input files
        self.fd_file = base_path / "FD.FD.parquet"
        self.deposit_uma = base_path / "DEPOSIT.UMA.parquet"

        # Output directory (BNM library)
        self.bnm_lib = output_path / "BNM"
        self.bnm_lib.mkdir(parents=True, exist_ok=True)

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

        # Additional mappings for FD
        self.fdprod_map = self._get_fdprod_map()
        self.fddenom_map = self._get_fddenom_map()
        self.fdcustcd_map = self._get_fdcustcd_map()
        self.ifdcuscd_map = self._get_ifdcuscd_map()

    def _get_fdprod_map(self) -> Dict:
        """FD Product code mapping (FDPROD format)"""
        return {
            # Domestic FD
            200: '42130',
            201: '42130',
            202: '42130',
            203: '42130',
            204: '42130',
            205: '42130',
            206: '42130',
            207: '42130',
            208: '42130',
            209: '42130',
            210: '42130',
            # Islamic FD
            220: '42330',
            221: '42330',
            222: '42330',
            223: '42330',
            224: '42330',
            225: '42330',
            # Foreign Currency FD
            400: '42630',
            401: '42630',
            402: '42630',
            403: '42630',
            404: '42630',
            405: '42630',
        }

    def _get_fddenom_map(self) -> Dict:
        """FD Denomination mapping (FDDENOM format)"""
        return {
            # Domestic - D
            200: 'D', 201: 'D', 202: 'D', 203: 'D', 204: 'D',
            205: 'D', 206: 'D', 207: 'D', 208: 'D', 209: 'D', 210: 'D',
            # Islamic - I
            220: 'I', 221: 'I', 222: 'I', 223: 'I', 224: 'I', 225: 'I',
            # Foreign - F
            400: 'F', 401: 'F', 402: 'F', 403: 'F', 404: 'F', 405: 'F',
        }

    def _get_fdcustcd_map(self) -> Dict:
        """FD Customer code mapping (FDCUSTCD format)"""
        return {
            1: '01', 2: '02', 3: '03', 4: '04', 5: '05',
            10: '10', 11: '11', 12: '12', 13: '13',
            20: '20', 21: '21', 30: '30', 31: '31',
            40: '40', 50: '50', 60: '60', 70: '70',
            75: '75', 76: '76', 77: '77', 78: '78',
            79: '79', 80: '80', 85: '85', 90: '90',
            95: '95', 99: '99',
        }

    def _get_ifdcuscd_map(self) -> Dict:
        """Islamic FD Customer code mapping (IFDCUSCD format)"""
        return {
            1: '01', 2: '02', 3: '03', 4: '04', 5: '05',
            10: '10', 11: '11', 12: '12', 13: '13',
            20: '20', 21: '21', 30: '30', 31: '31',
            40: '40', 50: '50', 60: '60', 70: '70',
            75: '75', 76: '76', 77: '77', 78: '78',
            79: '79', 80: '80', 85: '85', 90: '90',
            95: '95', 99: '99',
        }

    def apply_statecd(self, branch):
        """Apply STATECD format"""
        return self.statecd_map.get(branch, 'X')

    def apply_fdprod(self, intplan):
        """Apply FDPROD format"""
        return self.fdprod_map.get(intplan, '42130')

    def apply_fddenom(self, intplan):
        """Apply FDDENOM format"""
        return self.fddenom_map.get(intplan, 'D')

    def apply_fdcustcd(self, custcd):
        """Apply FDCUSTCD format"""
        return self.fdcustcd_map.get(custcd, '00')

    def apply_ifdcuscd(self, custcd):
        """Apply IFDCUSCD format"""
        return self.ifdcuscd_map.get(custcd, '00')


class FDProcessor:
    """Main processor for Fixed Deposit data"""

    def __init__(self, paths: PathConfig):
        self.paths = paths
        self.mapper = FormatMapper()

    def convert_lmatdate(self, lmatdate):
        """Convert LMATDATE to date format"""
        if lmatdate is None or lmatdate == 0:
            return 0
        try:
            lmatdate_str = str(int(lmatdate)).zfill(8)
            return int(lmatdate_str)
        except:
            return 0

    def process_fd(self):
        """Process Fixed Deposit data - creates FDMTHLY and FDWKLY"""
        print("Processing Fixed Deposit data...")

        # Read FD data
        df = pl.read_parquet(self.paths.fd_file)

        # Initialize LSTMATDT
        df = df.with_columns([
            pl.lit(0).alias('LSTMATDT')
        ])

        # Apply foreign rate to INTPAY if CURCODE != 'MYR'
        df = df.with_columns([
            pl.when(pl.col('CURCODE') != 'MYR')
            .then((pl.col('INTPAY') * pl.col('FORATE')).round(2))
            .otherwise(pl.col('INTPAY'))
            .alias('INTPAY')
        ])

        # Apply format mappings
        df = df.with_columns([
            pl.col('BRANCH').map_elements(self.mapper.apply_statecd, return_dtype=pl.Utf8).alias('STATE'),
            pl.col('INTPLAN').map_elements(self.mapper.apply_fdprod, return_dtype=pl.Utf8).alias('BIC'),
            pl.col('INTPLAN').map_elements(self.mapper.apply_fddenom, return_dtype=pl.Utf8).alias('AMTIND'),
        ])

        # Convert LMATDATE
        df = df.with_columns([
            pl.col('LMATDATE').map_elements(self.convert_lmatdate, return_dtype=pl.Int64).alias('LSTMATDT')
        ])

        # Apply CUSTCODE based on BIC
        df = df.with_columns([
            pl.when(pl.col('BIC').is_in(['42130', '42630']))
            .then(pl.col('CUSTCD').map_elements(self.mapper.apply_fdcustcd, return_dtype=pl.Utf8))
            .otherwise(pl.col('CUSTCD').map_elements(self.mapper.apply_ifdcuscd, return_dtype=pl.Utf8))
            .alias('CUSTCODE')
        ])

        # Handle PURPOSE for foreign currency FD (BIC = '42630')
        df = df.with_columns([
            pl.when(pl.col('BIC') == '42630')
            .then(
                pl.when(pl.col('CUSTCODE').is_in(['77', '78', '95']))
                .then(
                    pl.when(~pl.col('PURPOSE').cast(pl.Utf8).is_in(['1', '2', '3']))
                    .then(pl.lit('1'))
                    .otherwise(pl.col('PURPOSE'))
                )
                .otherwise(
                    pl.when(~pl.col('PURPOSE').cast(pl.Utf8).is_in(['4', '5']))
                    .then(pl.lit('4'))
                    .otherwise(pl.col('PURPOSE'))
                )
            )
            .otherwise(pl.col('PURPOSE'))
            .alias('PURPOSE')
        ])

        # Handle special ACCTTYPE to BIC mappings
        df = df.with_columns([
            pl.when(pl.col('ACCTTYPE').is_in([302, 315, 394, 396]))
            .then(pl.lit('42133'))
            .when(pl.col('ACCTTYPE').is_in([397, 398]))
            .then(pl.lit('42199'))
            .otherwise(pl.col('BIC'))
            .alias('BIC')
        ])

        # Filter: OPENIND = 'D' OR OPENIND = 'O'
        df = df.filter(pl.col('OPENIND').is_in(['D', 'O']))

        # Create FDMTHLY dataset
        fdmthly_cols = [
            'BRANCH', 'ACCTNO', 'STATE', 'CUSTCODE', 'OPENIND', 'CURBAL', 'TERM',
            'NAME', 'AMTIND', 'ORGDATE', 'MATDATE', 'RATE', 'RENEWAL', 'INTPLAN',
            'INTPAY', 'INTDATE', 'BIC', 'LASTACTV', 'LSTMATDT', 'PURPOSE', 'FORATE',
            'ACCTTYPE'
        ]
        df_fdmthly = df.select([col for col in fdmthly_cols if col in df.columns])

        # Create FDWKLY dataset
        fdwkly_cols = [
            'BRANCH', 'ACCTNO', 'CUSTCODE', 'NAME', 'AMTIND', 'MATDATE', 'ACCTTYPE',
            'OPENIND', 'CURBAL', 'BIC', 'INTPAY', 'STATE', 'FORATE', 'TERM', 'INTPLAN'
        ]
        df_fdwkly = df.select([col for col in fdwkly_cols if col in df.columns])

        # Write outputs
        fdmthly_file = self.paths.get_output_path('FDMTHLY')
        df_fdmthly.write_parquet(fdmthly_file)
        print(f"  Written {len(df_fdmthly)} records to FDMTHLY")

        fdwkly_file = self.paths.get_output_path('FDWKLY')
        df_fdwkly.write_parquet(fdwkly_file)
        print(f"  Written {len(df_fdwkly)} records to FDWKLY")

        return df_fdmthly, df_fdwkly

    def process_uma(self):
        """Process UMA (Unit Trust Management Account) data"""
        print("Processing UMA data...")

        # Read UMA data
        df = pl.read_parquet(self.paths.deposit_uma)

        # Rename CUSTCODE to CUSTCD for processing
        if 'CUSTCODE' in df.columns:
            df = df.rename({'CUSTCODE': 'CUSTCD'})

        # Process based on PRODUCT
        df = df.with_columns([
            pl.when(pl.col('PRODUCT') == 297)
            .then(pl.col('CUSTCD').map_elements(self.mapper.apply_fdcustcd, return_dtype=pl.Utf8))
            .otherwise(pl.col('CUSTCD').map_elements(self.mapper.apply_ifdcuscd, return_dtype=pl.Utf8))
            .alias('CUSTCODE'),

            pl.when(pl.col('PRODUCT') == 297)
            .then(pl.lit('D'))
            .otherwise(pl.lit('I'))
            .alias('AMTIND'),
        ])

        # Apply STATE mapping
        df = df.with_columns([
            pl.col('BRANCH').map_elements(self.mapper.apply_statecd, return_dtype=pl.Utf8).alias('STATE'),
            pl.lit('42199').alias('BIC')
        ])

        # Filter: OPENIND = 'O' OR OPENIND = 'D'
        df = df.filter(pl.col('OPENIND').is_in(['O', 'D']))

        # Select final columns
        uma_cols = [
            'BRANCH', 'ACCTNO', 'CUSTCODE', 'NAME', 'AMTIND',
            'OPENIND', 'CURBAL', 'BIC', 'STATE'
        ]
        df = df.select([col for col in uma_cols if col in df.columns])

        # Write output
        uma_file = self.paths.get_output_path('UMA')
        df.write_parquet(uma_file)
        print(f"  Written {len(df)} records to UMA")

        return df

    def run(self):
        """Execute full processing"""
        print("=" * 80)
        print("FALMPBBD - Fixed Deposit Processing with Foreign Rates")
        print("=" * 80)

        # Process FD data
        df_fdmthly, df_fdwkly = self.process_fd()

        # Process UMA data
        df_uma = self.process_uma()

        print("=" * 80)
        print("Processing Summary:")
        print(f"  FDMTHLY: {len(df_fdmthly)} records")
        print(f"  FDWKLY:  {len(df_fdwkly)} records")
        print(f"  UMA:     {len(df_uma)} records")
        print("=" * 80)
        print("FALMPBBD processing completed successfully")
        print("=" * 80)


def main():
    """Main entry point"""
    try:
        paths = PathConfig()
        processor = FDProcessor(paths)
        processor.run()
        return 0
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
