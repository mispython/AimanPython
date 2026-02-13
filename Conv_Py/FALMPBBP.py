#!/usr/bin/env python3
"""
Program: FALMPBBP
Purpose: To process FDMTHLY to produce BIC items for RDAL II for PBB.
         This program processes fixed deposit monthly data and creates consolidated BNM codes for regulatory reporting.
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import polars as pl
import pyarrow.parquet as pq
from typing import List, Dict


class PathConfig:
    """Path configuration for FALMPBBP"""

    def __init__(self):
        base_path = Path(os.getenv('BNM_DATA_PATH', '/data'))
        output_path = Path(os.getenv('BNM_OUTPUT_PATH', '/data/output'))

        # BNM library
        self.bnm_lib = output_path / "BNM"
        self.bnm_lib.mkdir(parents=True, exist_ok=True)

        # Macro variables from environment
        self.reptmon = os.getenv('REPTMON', '01')
        self.nowk = os.getenv('NOWK', '1')
        self.rdate = os.getenv('RDATE', datetime.now().strftime('%d%m%Y'))

    def get_input_path(self, dataset_name: str) -> Path:
        """Get input path for a dataset"""
        return self.bnm_lib / f"{dataset_name}.parquet"

    def get_output_path(self, dataset_name: str) -> Path:
        """Get output path for a dataset"""
        return self.bnm_lib / f"{dataset_name}.parquet"


class BNMCodeMapper:
    """Handle BNM code mapping logic for Fixed Deposits"""

    @staticmethod
    def map_fd_customer_code(bic: str, custcode: str, state: str) -> List[str]:
        """Map customer code to BNM code for FD"""
        codes = []

        if custcode in ['01', '79', '07', '17', '57', '75']:
            if custcode == '01':
                # Special handling for custcode '01'
                # If BIC = '42130' AND STATE = 'B', change STATE to 'W'
                if bic == '42130' and state == 'B':
                    state = 'W'
                codes.append(f'{bic}{custcode}000000{state}')
            else:
                codes.append(f'{bic}{custcode}000000{state}')
        elif custcode in ['10', '02', '03', '11', '12']:
            codes.append(f'{bic}10000000{state}')
        elif custcode in ['20', '13', '30', '31', '04', '05', '06',
                          '32', '33', '34', '35', '36', '37', '38', '39', '40', '45']:
            codes.append(f'{bic}20000000{state}')
        elif custcode in ['60', '61', '62', '63', '64', '65', '66', '67', '68', '69',
                          '41', '42', '43', '44', '46', '47', '48', '49', '51',
                          '52', '53', '54', '59']:
            codes.append(f'{bic}60000000{state}')
        elif custcode in ['70', '71', '72', '73', '74']:
            codes.append(f'{bic}70000000{state}')
        elif custcode in ['76', '77', '78']:
            codes.append(f'{bic}76000000{state}')
        elif custcode in ['80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '90',
                          '91', '92', '95', '96', '98', '99']:
            codes.append(f'{bic}80000000{state}')

        return codes

    @staticmethod
    def map_fcy_fd_purpose(custcode: str, purpose: str) -> List[str]:
        """Map FCY FD by customer code and purpose"""
        codes = []

        if custcode in ['76', '77', '78']:
            if purpose in ['1', '2']:
                codes.append('4260076009997Y')
            elif purpose == '3':
                codes.append('4260076009999Y')
        elif custcode in ['95', '96']:
            if purpose in ['1', '2']:
                codes.append('4260095009997Y')
            elif purpose == '3':
                codes.append('4260095009999Y')
        elif custcode in ['80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '90',
                          '91', '92', '93', '94', '97', '98', '99']:
            if purpose == '4':
                codes.append('4260086009998Y')
            elif purpose == '5':
                codes.append('4260086009999Y')
        else:
            # Otherwise
            if purpose == '4':
                codes.append('4260060009998Y')
            elif purpose == '5':
                codes.append('4260060009999Y')

        return codes


class FALMPBBPProcessor:
    """Main processor for FALMPBBP"""

    def __init__(self, paths: PathConfig):
        self.paths = paths
        self.mapper = BNMCodeMapper()

    def process_fd_by_customer(self) -> pl.DataFrame:
        """Process RM FD accepted by customer code and state"""
        print("Processing RM FD by customer code and state...")

        # Read FDMTHLY dataset
        fdmthly_file = self.paths.get_input_path('FDMTHLY')
        df = pl.read_parquet(fdmthly_file)

        # Filter: BIC IN ('42130','42132','42133','42199') AND ACCTTYPE NOT IN (397,398)
        df = df.filter(
            pl.col('BIC').is_in(['42130', '42132', '42133', '42199']) &
            ~pl.col('ACCTTYPE').is_in([397, 398])
        )

        # Summarize by BIC, STATE, CUSTCODE, AMTIND
        alm = df.group_by(['BIC', 'STATE', 'CUSTCODE', 'AMTIND']).agg([
            pl.col('CURBAL').sum().alias('AMOUNT')
        ])

        # Map to BNM codes
        results = []
        for row in alm.iter_rows(named=True):
            bic = row['BIC']
            custcode = row['CUSTCODE']
            state = row['STATE']
            amtind = row['AMTIND']
            amount = row['AMOUNT']

            bnmcodes = self.mapper.map_fd_customer_code(bic, custcode, state)
            for bnmcode in bnmcodes:
                results.append({
                    'BNMCODE': bnmcode,
                    'AMTIND': amtind,
                    'AMOUNT': amount
                })

        if results:
            df_result = pl.DataFrame(results)
            # Summarize by BNMCODE and AMTIND
            df_result = df_result.group_by(['BNMCODE', 'AMTIND']).agg([
                pl.col('AMOUNT').sum().alias('AMOUNT')
            ])
            return df_result
        else:
            return pl.DataFrame({'BNMCODE': [], 'AMTIND': [], 'AMOUNT': []})

    def process_fd_account_count(self) -> pl.DataFrame:
        """Process number of FD accounts by state"""
        print("Processing number of FD accounts by state...")

        # Read FDMTHLY dataset
        fdmthly_file = self.paths.get_input_path('FDMTHLY')
        df = pl.read_parquet(fdmthly_file)

        # Summarize by BIC, STATE, AMTIND
        alm = df.group_by(['BIC', 'STATE', 'AMTIND']).agg([
            pl.count().alias('_FREQ_')
        ])

        # Create BNMCODE based on BIC
        results = []
        for row in alm.iter_rows(named=True):
            bic = row['BIC']
            state = row['STATE']
            amtind = row['AMTIND']
            freq = row['_FREQ_']

            amount = freq * 1000

            # Only process BIC='42133'
            if bic == '42133':
                results.append({
                    'BNMCODE': '8023300000000Y',
                    'AMTIND': amtind,
                    'AMOUNT': amount
                })

        if results:
            df_result = pl.DataFrame(results)
            # Summarize by BNMCODE and AMTIND
            df_result = df_result.group_by(['BNMCODE', 'AMTIND']).agg([
                pl.col('AMOUNT').sum().alias('AMOUNT')
            ])
            return df_result
        else:
            return pl.DataFrame({'BNMCODE': [], 'AMTIND': [], 'AMOUNT': []})

    def process_fcy_fd_by_purpose(self) -> pl.DataFrame:
        """Process FX FD FCY deposit accepted by purpose"""
        print("Processing FX FD FCY by purpose...")

        # Read FDMTHLY dataset
        fdmthly_file = self.paths.get_input_path('FDMTHLY')
        df = pl.read_parquet(fdmthly_file)

        # Filter: BIC = '42630'
        df = df.filter(pl.col('BIC') == '42630')

        # Summarize by BIC, CUSTCODE, PURPOSE, AMTIND
        alm = df.group_by(['BIC', 'CUSTCODE', 'PURPOSE', 'AMTIND']).agg([
            pl.col('CURBAL').sum().alias('AMOUNT')
        ])

        # Map to BNM codes
        results = []
        for row in alm.iter_rows(named=True):
            custcode = row['CUSTCODE']
            purpose = str(row['PURPOSE'])
            amtind = row['AMTIND']
            amount = row['AMOUNT']

            bnmcodes = self.mapper.map_fcy_fd_purpose(custcode, purpose)
            for bnmcode in bnmcodes:
                results.append({
                    'BNMCODE': bnmcode,
                    'AMTIND': amtind,
                    'AMOUNT': amount
                })

        if results:
            df_result = pl.DataFrame(results)
            # Summarize by BNMCODE and AMTIND
            df_result = df_result.group_by(['BNMCODE', 'AMTIND']).agg([
                pl.col('AMOUNT').sum().alias('AMOUNT')
            ])
            return df_result
        else:
            return pl.DataFrame({'BNMCODE': [], 'AMTIND': [], 'AMOUNT': []})

    def consolidate_final(self, *dataframes: pl.DataFrame) -> pl.DataFrame:
        """Consolidate all dataframes"""
        print("Consolidating final data...")

        # Combine all dataframes
        combined = pl.concat([df for df in dataframes if len(df) > 0], how='diagonal')

        if len(combined) == 0:
            return pl.DataFrame({'BNMCODE': [], 'AMTIND': [], 'AMOUNT': []})

        return combined

    def run(self):
        """Execute full processing"""
        print("=" * 80)
        print("FALMPBBP - FD Monthly BIC Processing for RDAL II")
        print("=" * 80)

        # Delete existing FALM dataset
        falm_file = self.paths.get_output_path(f"FALM{self.paths.reptmon}{self.paths.nowk}")
        if falm_file.exists():
            falm_file.unlink()
            print(f"Deleted existing {falm_file.name}")

        # Process FD by customer
        df_customer = self.process_fd_by_customer()
        print(f"  FD by customer: {len(df_customer)} records")

        # Process FD account counts
        df_counts = self.process_fd_account_count()
        print(f"  FD account counts: {len(df_counts)} records")

        # Process FCY FD by purpose
        df_fcy = self.process_fcy_fd_by_purpose()
        print(f"  FCY FD by purpose: {len(df_fcy)} records")

        # Consolidate all
        df_final = self.consolidate_final(df_customer, df_counts, df_fcy)
        print(f"  Final consolidated: {len(df_final)} records")

        # Write final dataset
        df_final.write_parquet(falm_file)
        print(f"  Written to {falm_file}")

        # Display summary
        print("\n" + "=" * 80)
        print("Processing Summary:")
        print("=" * 80)
        print(f"Report Date: {self.paths.rdate}")
        print(f"Total Records: {len(df_final)}")

        if len(df_final) > 0:
            total_amount = df_final['AMOUNT'].sum()
            print(f"Total Amount: {total_amount:,.2f}")

            # Show sample records
            print("\nSample Records (first 20):")
            print(df_final.sort(['BNMCODE', 'AMTIND']).head(20))

        print("=" * 80)
        print("FALMPBBP processing completed successfully")
        print("=" * 80)


def main():
    """Main entry point"""
    try:
        paths = PathConfig()
        processor = FALMPBBPProcessor(paths)
        processor.run()
        return 0
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
