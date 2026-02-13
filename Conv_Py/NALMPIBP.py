# !/usr/bin/env python3
"""
Program : NALMPIBP
Report  : RDAL Part I (NSRS KAPITI ITEMS)
          RDIR Part I (NSRS KAPITI ITEMS)

This program processes KAPITI table 1 data to generate BNM codes for domestic assets and liabilities reporting.
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import polars as pl
import pyarrow.parquet as pq
from typing import Optional, List, Dict


class PathConfig:
    """Path configuration for NALMPIBP"""

    def __init__(self):
        base_path = Path(os.getenv('BNM_DATA_PATH', '/data'))
        output_path = Path(os.getenv('BNM_OUTPUT_PATH', '/data/output'))

        # BNMK library (KAPITI data)
        self.bnmk_lib = base_path / "BNMK"

        # BNM library (output)
        self.bnm_lib = output_path / "BNM"
        self.bnm_lib.mkdir(parents=True, exist_ok=True)

        # Macro variables from environment
        self.reptmon = os.getenv('REPTMON', '01')
        self.nowk = os.getenv('NOWK', '1')
        self.rdate = os.getenv('RDATE', datetime.now().strftime('%d%m%Y'))
        self.sdesc = os.getenv('SDESC', 'PUBLIC ISLAMIC BANK BERHAD')

        # Parse reporting date
        self.reptdate = self.parse_rdate()

    def parse_rdate(self) -> datetime:
        """Parse RDATE string to datetime"""
        try:
            return datetime.strptime(self.rdate, '%d%m%Y')
        except:
            return datetime.now()

    def get_input_path(self, dataset_name: str) -> Path:
        """Get input path for a dataset"""
        return self.bnmk_lib / f"{dataset_name}.parquet"

    def get_output_path(self, dataset_name: str) -> Path:
        """Get output path for a dataset"""
        return self.bnm_lib / f"{dataset_name}.parquet"


class DateCalculator:
    """Calculate remaining months and handle date operations"""

    def __init__(self, reptdate: datetime):
        self.reptdate = reptdate
        self.rpyr = reptdate.year
        self.rpmth = reptdate.month
        self.rpday = reptdate.day

        # Days in each month
        self.days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

        # Adjust February for leap year
        if self.is_leap_year(self.rpyr):
            self.days_in_month[1] = 29

    @staticmethod
    def is_leap_year(year: int) -> bool:
        """Check if year is a leap year"""
        return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)

    def calculate_remaining_months(self, gwmdt: datetime) -> Optional[float]:
        """Calculate remaining months from reptdate to gwmdt"""
        if gwmdt is None:
            return None

        mdyr = gwmdt.year
        mdmth = gwmdt.month
        mdday = gwmdt.day

        # Adjust February for maturity date year
        md_days = self.days_in_month.copy()
        if self.is_leap_year(mdyr):
            md_days[1] = 29

        # Get days in reporting month
        rpdays = self.days_in_month[self.rpmth - 1]

        # Adjust mdday if it exceeds days in reporting month
        if mdday > rpdays:
            mdday = rpdays

        # Calculate remaining months
        remy = mdyr - self.rpyr
        remm = mdmth - self.rpmth
        remd = mdday - self.rpday

        remmth = remy * 12 + remm + remd / rpdays

        return remmth


class BNMCodeMapper:
    """Map KAPITI data to BNM codes"""

    @staticmethod
    def map_dp32000(gwctp: str) -> Optional[str]:
        """Map DP32000 codes (Deposits > 3 months)"""
        mapping = {
            'BC': '3250001000000F', 'BP': '3250001000000F',
            'BB': '3250002000000F',
            'BI': '3250003000000F',
            'BM': '3250012000000F',
            'BG': '3250017000000F',
            'AD': '3250020000000F', 'BF': '3250020000000F', 'BH': '3250020000000F',
            'BN': '3250020000000F', 'BR': '3250020000000F', 'BS': '3250020000000F',
            'BT': '3250020000000F', 'BU': '3250020000000F', 'BV': '3250020000000F',
            'BZ': '3250020000000F',
            'AC': '3250006000000F', 'CA': '3250006000000F', 'CB': '3250006000000F',
            'CC': '3250006000000F', 'CD': '3250006000000F', 'CF': '3250006000000F',
            'CG': '3250006000000F', 'DD': '3250006000000F',
            'DA': '3250007000000F', 'DB': '3250007000000F', 'DC': '3250007000000F',
            'EA': '3250007600000F', 'EC': '3250007600000F', 'EJ': '3250007600000F',
            'FA': '3250007900000F',
            'BA': '3250008100000F', 'BE': '3250008100000F',
            'CE': '3250008500000F', 'EB': '3250008500000F', 'GA': '3250008500000F',
        }
        return mapping.get(gwctp)

    @staticmethod
    def map_dp32800(gwctp: str) -> Optional[str]:
        """Map DP32800 codes (Second mapping for deposits)"""
        mapping = {
            'BB': '3280002000000F',
            'BI': '3280003000000F',
            'BM': '3280012000000F',
            'AD': '3280020000000F', 'BF': '3280020000000F', 'BH': '3280020000000F',
            'BN': '3280020000000F', 'BR': '3280020000000F', 'BS': '3280020000000F',
            'BT': '3280020000000F', 'BU': '3280020000000F', 'BV': '3280020000000F',
            'BZ': '3280020000000F',
            'BA': '3280008100000F', 'BE': '3280008100000F',
        }
        return mapping.get(gwctp)

    @staticmethod
    def map_af33140_myr(gwctp: str) -> Optional[str]:
        """Map AF33140 codes (MYR acceptances/financing)"""
        mapping = {
            'BC': '3314001100000F', 'BP': '3314001100000F',
            'BB': '3314002100000F',
            'BI': '3314003100000F',
            'BM': '3314012100000F',
            'BG': '3314017100000F',
            'AD': '3314020100000F', 'BF': '3314020100000F', 'BH': '3314020100000F',
            'BN': '3314020100000F', 'BR': '3314020100000F', 'BS': '3314020100000F',
            'BT': '3314020100000F', 'BU': '3314020100000F', 'BV': '3314020100000F',
            'BZ': '3314020100000F',
            'BA': '3314081100000F', 'BE': '3314081100000F',
        }
        return mapping.get(gwctp)

    @staticmethod
    def map_af33640_fcy(gwctp: str) -> Optional[str]:
        """Map AF33640 codes (FCY acceptances/financing)"""
        mapping = {
            'BC': '3364001100000F', 'BP': '3364001100000F',
            'BB': '3364002100000F',
            'BI': '3364003100000F',
            'BJ': '3364007100000F',
            'BM': '3364012100000F',
            'BA': '3364081100000F', 'BE': '3364081100000F',
        }
        return mapping.get(gwctp)


class ReportGenerator:
    """Generate ASA formatted report"""

    def __init__(self, sdesc: str, rdate: str):
        self.sdesc = sdesc
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
        lines.append(self.format_asa_line('1', ' ' * 132))
        lines.append(self.format_asa_line(' ', self.sdesc.center(132)))
        lines.append(self.format_asa_line(' ', 'REPORT ON DOMESTIC ASSETS AND LIABILITIES PART I - KAPITI'.center(132)))
        lines.append(self.format_asa_line(' ', f'REPORT DATE : {self.rdate}'.center(132)))
        lines.append(self.format_asa_line(' ', ' ' * 132))
        lines.append(self.format_asa_line(' ',
                                          f"{'BNMCODE':<14} {'AMTIND':<6} {'AMOUNT':>30}"))
        lines.append(self.format_asa_line(' ', '-' * 132))

        self.line_count = 7
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

            line = f"{bnmcode:<14} {amtind:<6} {self.format_amount(amount):>30}"
            lines.append(self.format_asa_line(' ', line))
            self.line_count += 1

        # Write to file
        with open(output_file, 'w') as f:
            f.writelines(lines)


class NALMPIBPProcessor:
    """Main processor for NALMPIBP"""

    def __init__(self, paths: PathConfig):
        self.paths = paths
        self.mapper = BNMCodeMapper()
        self.date_calc = DateCalculator(paths.reptdate)
        self.amtind = 'I'

    def process_k1table(self) -> pl.DataFrame:
        """Process K1TBL data"""
        print("Processing K1TBL data...")

        # Read K1TBL dataset
        k1tbl_file = self.paths.get_input_path(f"K1TBL{self.paths.reptmon}{self.paths.nowk}")
        df = pl.read_parquet(k1tbl_file)

        # Rename GWBALC to AMOUNT
        if 'GWBALC' in df.columns:
            df = df.rename({'GWBALC': 'AMOUNT'})

        # Extract BRANCH from GWAB (first 4 characters)
        df = df.with_columns([
            pl.col('GWAB').str.slice(0, 4).cast(pl.Int64).alias('BRANCH')
        ])

        # Initialize BNMCODE and AMTIND
        df = df.with_columns([
            pl.lit('').alias('BNMCODE'),
            pl.lit(self.amtind).alias('AMTIND')
        ])

        results = []

        for row in df.iter_rows(named=True):
            gwccy = row.get('GWCCY', '')
            gwmvt = row.get('GWMVT', '')
            gwmvts = row.get('GWMVTS', '')
            gwdlp = row.get('GWDLP', '')
            gwact = row.get('GWACT', '')
            gwctp = row.get('GWCTP', '')
            gwmdt = row.get('GWMDT')
            amount = row.get('AMOUNT', 0)
            branch = row.get('BRANCH', 0)

            # Convert gwmdt to datetime if needed
            if gwmdt is not None and not isinstance(gwmdt, datetime):
                try:
                    if isinstance(gwmdt, (int, float)):
                        # Assume days since epoch or similar
                        gwmdt = None
                    elif isinstance(gwmdt, str):
                        gwmdt = datetime.strptime(gwmdt, '%Y-%m-%d')
                except:
                    gwmdt = None

            # Process DP32000 section
            if (gwccy == 'MYR' and gwmvt == 'P' and gwmvts == 'M' and
                    (gwdlp in ['FDA', 'FDB', 'FDL', 'FDS', 'LC', 'LO'] or
                     (len(gwdlp) >= 3 and gwdlp[1:3] in ['XI', 'XT']))):

                # Check for XI or XT
                if len(gwdlp) >= 3 and gwdlp[1:3] in ['XI', 'XT']:
                    if gwmdt is not None:
                        remmth = self.date_calc.calculate_remaining_months(gwmdt)

                        if remmth is not None and gwccy == 'MYR' and remmth > 3:
                            # Map DP32000
                            bnmcode = self.mapper.map_dp32000(gwctp)
                            if bnmcode:
                                results.append({
                                    'BNMCODE': bnmcode,
                                    'AMTIND': self.amtind,
                                    'AMOUNT': amount
                                })

                            # Map DP32800
                            bnmcode = self.mapper.map_dp32800(gwctp)
                            if bnmcode:
                                results.append({
                                    'BNMCODE': bnmcode,
                                    'AMTIND': self.amtind,
                                    'AMOUNT': amount
                                })

            # Process AF33000 section
            if (gwdlp in ['', 'LO', 'LS', 'LF', 'LOW', 'LSW', 'LSC', 'LOC', 'LOI', 'LSI'] or
                    gwact == 'CN'):

                if gwdlp in ['LOW', 'LSW', 'LSC', 'LOC', 'LOI', 'LSI']:
                    # MYR acceptances/financing
                    if gwccy == 'MYR' and gwmvt == 'P' and gwmvts == 'M':
                        bnmcode = self.mapper.map_af33140_myr(gwctp)
                        if bnmcode:
                            results.append({
                                'BNMCODE': bnmcode,
                                'AMTIND': self.amtind,
                                'AMOUNT': amount
                            })

                    # FCY acceptances/financing
                    if gwccy != 'MYR' and gwmvt == 'P' and gwmvts == 'M':
                        bnmcode = self.mapper.map_af33640_fcy(gwctp)
                        if bnmcode:
                            results.append({
                                'BNMCODE': bnmcode,
                                'AMTIND': self.amtind,
                                'AMOUNT': amount
                            })

        if results:
            return pl.DataFrame(results)
        else:
            return pl.DataFrame({'BNMCODE': [], 'AMTIND': [], 'AMOUNT': []})

    def consolidate_final(self, df: pl.DataFrame) -> pl.DataFrame:
        """Final consolidation - summarize and take absolute values"""
        print("Consolidating final data...")

        if len(df) == 0:
            return df

        # Summarize by BNMCODE and AMTIND
        df_final = df.group_by(['BNMCODE', 'AMTIND']).agg([
            pl.col('AMOUNT').sum().alias('AMOUNT')
        ])

        # Take absolute value
        df_final = df_final.with_columns([
            pl.col('AMOUNT').abs().alias('AMOUNT')
        ])

        return df_final

    def run(self):
        """Execute full processing"""
        print("=" * 80)
        print("NALMPIBP - NSRS KAPITI Items for RDAL/RDIR Part I")
        print("=" * 80)

        # Delete existing NALM dataset
        nalm_file = self.paths.get_output_path(f"NALM{self.paths.reptmon}{self.paths.nowk}")
        if nalm_file.exists():
            nalm_file.unlink()
            print(f"Deleted existing {nalm_file.name}")

        # Process K1 table
        df_k1 = self.process_k1table()
        print(f"  K1 table processed: {len(df_k1)} records")

        # Consolidate
        df_final = self.consolidate_final(df_k1)
        print(f"  Final consolidated: {len(df_final)} records")

        # Write final dataset
        df_final.write_parquet(nalm_file)
        print(f"  Written to {nalm_file}")

        # Generate report
        report_file = self.paths.bnm_lib / f"NALMPIBP_{self.paths.reptmon}{self.paths.nowk}_report.txt"
        report_gen = ReportGenerator(self.paths.sdesc, self.paths.rdate)
        report_gen.generate_report(df_final, report_file)
        print(f"  Report written to {report_file}")

        # Display summary
        print("\n" + "=" * 80)
        print("Processing Summary:")
        print("=" * 80)
        print(f"Bank: {self.paths.sdesc}")
        print(f"Report Date: {self.paths.rdate}")
        print(f"Total Records: {len(df_final)}")

        if len(df_final) > 0:
            total_amount = df_final['AMOUNT'].sum()
            print(f"Total Amount: {total_amount:,.2f}")

            # Show sample records
            print("\nSample Records (first 20):")
            print(df_final.sort(['BNMCODE', 'AMTIND']).head(20))

        print("=" * 80)
        print("NALMPIBP processing completed successfully")
        print("=" * 80)


def main():
    """Main entry point"""
    try:
        paths = PathConfig()
        processor = NALMPIBPProcessor(paths)
        processor.run()
        return 0
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
