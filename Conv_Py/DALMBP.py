# !/usr/bin/env python3
"""
Program  : DALMBP
Report on domestic assets and liabilities - Part II
Weekly BNM Processing

Purpose: To Process current and saving deposits, creating consolidated
            reports by customer type and state code for BNM regulatory reporting.
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import polars as pl
import pyarrow.parquet as pq
from typing import List, Dict


class PathConfig:
    """Path configuration for DALMBP"""

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

        # CURX macro - current account product exclusions
        self.curx = self.get_curx_products()

    def get_curx_products(self) -> List[int]:
        """Get CURX product list from environment or use default"""
        curx_str = os.getenv('CURX', '')
        if curx_str:
            return [int(x.strip()) for x in curx_str.split(',')]
        # Default CURX products (typical current account products)
        return list(range(100, 200))  # Products 100-199

    def get_input_path(self, dataset_name: str) -> Path:
        """Get input path for a dataset"""
        return self.bnm_lib / f"{dataset_name}.parquet"

    def get_output_path(self, dataset_name: str) -> Path:
        """Get output path for a dataset"""
        return self.bnm_lib / f"{dataset_name}.parquet"


class BNMCodeMapper:
    """Handle BNM code mapping logic"""

    @staticmethod
    def map_demand_deposit_code(custcd: str, statecd: str) -> List[str]:
        """Map customer code to BNM code for demand deposits (42110)"""
        codes = []

        if custcd in ['79', '07', '17', '57', '75']:
            codes.append(f'42110{custcd}000000{statecd}')
        elif custcd in ['10', '02', '03', '11', '12']:
            codes.append(f'4211010000000{statecd}')
        elif custcd in ['20', '13', '30', '31', '04', '05', '06',
                        '32', '33', '34', '35', '36', '37', '38', '39', '40']:
            codes.append(f'4211020000000{statecd}')
        elif custcd in ['60', '61', '62', '63', '64', '65', '45',
                        '41', '42', '43', '44', '46', '47', '48', '49', '51',
                        '52', '53', '54', '59']:
            codes.append(f'4211060000000{statecd}')
        elif custcd in ['70', '71', '72', '73', '74']:
            codes.append(f'4211070000000{statecd}')
        elif custcd in ['76', '77', '78']:
            codes.append(f'4211076000000{statecd}')
        elif custcd in ['80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '90',
                        '91', '92', '95', '96', '98', '99']:
            codes.append(f'4211080000000{statecd}')

        return codes

    @staticmethod
    def map_saving_deposit_code(custcd: str, statecd: str) -> List[str]:
        """Map customer code to BNM code for saving deposits (42120)"""
        codes = []

        if custcd in ['20', '13', '30', '31', '45',
                      '04', '05', '06', '32', '33', '34', '35',
                      '36', '37', '38', '39', '40']:
            codes.append(f'4212020000000{statecd}')
        elif custcd in ['76', '77', '78']:
            codes.append(f'4212076000000{statecd}')
        elif custcd in ['79', '07', '17', '57', '75']:
            codes.append(f'42120{custcd}000000{statecd}')
        elif custcd in ['80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '90',
                        '91', '92', '95', '96', '98', '99']:
            codes.append(f'4212080000000{statecd}')
            if custcd in ['86', '87', '88', '89']:
                codes.append(f'4212086000000{statecd}')

        return codes


class ReportGenerator:
    """Generate ASA formatted report"""

    def __init__(self, rdate: str):
        self.rdate = rdate
        self.page_length = 60
        self.line_count = 0
        self.page_count = 0

    def format_asa_line(self, control_char: str, line: str) -> str:
        """Format line with ASA carriage control character"""
        return f"{control_char}{line}\n"

    def generate_header(self) -> List[str]:
        """Generate report header"""
        self.page_count += 1
        self.line_count = 0

        lines = []
        lines.append(self.format_asa_line('1', ' ' * 132))  # Form feed
        lines.append(
            self.format_asa_line(' ', 'REPORT ON DOMESTIC ASSETS AND LIABILITIES PART II-M&I DEPOSIT'.center(132)))
        lines.append(self.format_asa_line(' ', f'REPORT DATE : {self.rdate}'.center(132)))
        lines.append(self.format_asa_line(' ', ' ' * 132))
        lines.append(self.format_asa_line(' ',
                                          f"{'BNMCODE':<14} {'AMTIND':<6} {'AMOUNT':>25}"))
        lines.append(self.format_asa_line(' ', '-' * 132))

        self.line_count = 6
        return lines

    def format_amount(self, amount: float) -> str:
        """Format amount with comma separator"""
        return f"{amount:,.2f}"

    def generate_report(self, df: pl.DataFrame, output_file: Path):
        """Generate formatted report with ASA control characters"""
        lines = []
        lines.extend(self.generate_header())

        # Sort by BNMCODE and AMTIND
        df = df.sort(['BNMCODE', 'AMTIND'])

        for row in df.iter_rows(named=True):
            if self.line_count >= self.page_length - 2:
                lines.extend(self.generate_header())

            bnmcode = row['BNMCODE']
            amtind = row.get('AMTIND', ' ')
            amount = row['AMOUNT']

            line = f"{bnmcode:<14} {amtind:<6} {self.format_amount(amount):>25}"
            lines.append(self.format_asa_line(' ', line))
            self.line_count += 1

        # Write to file
        with open(output_file, 'w') as f:
            f.writelines(lines)


class DALMBPProcessor:
    """Main processor for DALMBP report"""

    def __init__(self, paths: PathConfig):
        self.paths = paths
        self.mapper = BNMCodeMapper()

    def process_demand_deposits(self) -> pl.DataFrame:
        """Process RM Demand Deposit by customer and state code"""
        print("Processing RM Demand Deposits...")

        # Read CURN dataset
        curn_file = self.paths.get_input_path(f"CURN{self.paths.reptmon}{self.paths.nowk}")
        df = pl.read_parquet(curn_file)

        # Filter: PRODUCT IN &CURX and exclude 63, 163
        df = df.filter(
            pl.col('PRODUCT').is_in(self.paths.curx) &
            ~pl.col('PRODUCT').is_in([63, 163])
        )

        # Summarize by PRODCD, CUSTCD, STATECD, AMTIND
        alm = df.group_by(['PRODCD', 'CUSTCD', 'STATECD', 'AMTIND']).agg([
            pl.col('CURBAL').sum().alias('AMOUNT')
        ])

        # Map to BNM codes
        results = []
        for row in alm.iter_rows(named=True):
            custcd = row['CUSTCD']
            statecd = row['STATECD']
            amtind = row['AMTIND']
            amount = row['AMOUNT']

            bnmcodes = self.mapper.map_demand_deposit_code(custcd, statecd)
            for bnmcode in bnmcodes:
                results.append({
                    'BNMCODE': bnmcode,
                    'AMTIND': amtind,
                    'AMOUNT': amount
                })

        if results:
            return pl.DataFrame(results)
        else:
            return pl.DataFrame({'BNMCODE': [], 'AMTIND': [], 'AMOUNT': []})

    def process_saving_deposits(self) -> pl.DataFrame:
        """Process RM Saving Deposit by customer and state code"""
        print("Processing RM Saving Deposits...")

        # Read DEPT dataset
        dept_file = self.paths.get_input_path(f"DEPT{self.paths.reptmon}{self.paths.nowk}")
        df = pl.read_parquet(dept_file)

        # Filter: PRODCD IN ('42120','42320')
        df = df.filter(pl.col('PRODCD').is_in(['42120', '42320']))

        # Summarize by PRODCD, CUSTCD, STATECD, AMTIND
        alm = df.group_by(['PRODCD', 'CUSTCD', 'STATECD', 'AMTIND']).agg([
            pl.col('CURBAL').sum().alias('AMOUNT')
        ])

        # Map to BNM codes
        results = []
        for row in alm.iter_rows(named=True):
            custcd = row['CUSTCD']
            statecd = row['STATECD']
            amtind = row['AMTIND']
            amount = row['AMOUNT']

            bnmcodes = self.mapper.map_saving_deposit_code(custcd, statecd)
            for bnmcode in bnmcodes:
                results.append({
                    'BNMCODE': bnmcode,
                    'AMTIND': amtind,
                    'AMOUNT': amount
                })

        if results:
            return pl.DataFrame(results)
        else:
            return pl.DataFrame({'BNMCODE': [], 'AMTIND': [], 'AMOUNT': []})

    def consolidate_final(self, *dataframes: pl.DataFrame) -> pl.DataFrame:
        """Final consolidation of all data"""
        print("Consolidating final data...")

        # Combine all dataframes
        combined = pl.concat([df for df in dataframes if len(df) > 0], how='diagonal')

        if len(combined) == 0:
            return pl.DataFrame({'BNMCODE': [], 'AMTIND': [], 'AMOUNT': []})

        # Summarize by BNMCODE and AMTIND
        final = combined.group_by(['BNMCODE', 'AMTIND']).agg([
            pl.col('AMOUNT').sum().alias('AMOUNT')
        ])

        return final

    def run(self):
        """Execute full processing"""
        print("=" * 80)
        print("DALMBP - Domestic Assets and Liabilities Report Part II")
        print("=" * 80)

        # Delete existing DALM dataset
        dalm_file = self.paths.get_output_path(f"DALM{self.paths.reptmon}{self.paths.nowk}")
        if dalm_file.exists():
            dalm_file.unlink()
            print(f"Deleted existing {dalm_file.name}")

        # Process demand deposits
        df_demand = self.process_demand_deposits()
        print(f"  Demand deposits: {len(df_demand)} records")

        # Process saving deposits
        df_saving = self.process_saving_deposits()
        print(f"  Saving deposits: {len(df_saving)} records")

        # Consolidate
        df_final = self.consolidate_final(df_demand, df_saving)
        print(f"  Final consolidated: {len(df_final)} records")

        # Write final dataset
        df_final.write_parquet(dalm_file)
        print(f"  Written to {dalm_file}")

        # Generate report
        report_file = self.paths.bnm_lib / f"DALMBP_{self.paths.reptmon}{self.paths.nowk}_report.txt"
        report_gen = ReportGenerator(self.paths.rdate)
        report_gen.generate_report(df_final, report_file)
        print(f"  Report written to {report_file}")

        # Display summary
        print("\n" + "=" * 80)
        print("Report Summary:")
        print("=" * 80)
        print(f"Report Date: {self.paths.rdate}")
        print(f"Total Records: {len(df_final)}")

        if len(df_final) > 0:
            total_amount = df_final['AMOUNT'].sum()
            print(f"Total Amount: {total_amount:,.2f}")

            # Show sample records
            print("\nSample Records (first 10):")
            print(df_final.head(10))

        print("=" * 80)
        print("DALMBP processing completed successfully")
        print("=" * 80)


def main():
    """Main entry point"""
    try:
        paths = PathConfig()
        processor = DALMBPProcessor(paths)
        processor.run()
        return 0
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
