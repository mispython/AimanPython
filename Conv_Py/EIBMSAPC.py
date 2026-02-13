#!/usr/bin/env python3
"""
Program: EIBMSAPC
Purpose: KAPX Processing - BNM Table X Securities Processing

This program reads BNMTBLX text file containing securities transactions,
    calculates remaining months to maturity, and maps them to BNM codes.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import polars as pl
import pyarrow.parquet as pq
from typing import List, Dict


class PathConfig:
    """Path configuration"""

    def __init__(self):
        base_path = Path(os.getenv('BNM_DATA_PATH', '/data'))
        output_path = Path(os.getenv('BNM_OUTPUT_PATH', '/data/output'))

        # Input file
        self.bnmtblx_file = base_path / "SAP.PBB.KAPITIX.TXT.0.txt"

        # BNM library
        self.bnm_lib = output_path / "BNM"
        self.bnm_lib.mkdir(parents=True, exist_ok=True)

        # Calculate reporting date
        self.calculate_dates()

    def calculate_dates(self):
        """Calculate reporting dates - last day of previous month"""
        today = datetime.now()
        first_of_month = datetime(today.year, today.month, 1)
        self.reptdate = first_of_month - timedelta(days=1)

        day = self.reptdate.day

        if day == 8:
            self.sdd = 1
            self.wk = '1'
            self.wk1 = '4'
        elif day == 15:
            self.sdd = 9
            self.wk = '2'
            self.wk1 = '1'
        elif day == 22:
            self.sdd = 16
            self.wk = '3'
            self.wk1 = '2'
        else:
            self.sdd = 23
            self.wk = '4'
            self.wk1 = '3'

        self.mm = self.reptdate.month
        self.sdesc = 'PUBLIC BANK BERHAD'
        self.reptmon = f"{self.mm:02d}"
        self.reptyear = f"{self.reptdate.year}"
        self.reptday = f"{self.reptdate.day:02d}"
        self.rdate = self.reptdate.strftime('%d%m%Y')

    def get_output_path(self, dataset_name: str) -> Path:
        """Get output path for a dataset"""
        return self.bnm_lib / f"{dataset_name}.parquet"


class DateCalculator:
    """Calculate remaining months to maturity"""

    def __init__(self, reptdate: datetime):
        self.reptdate = reptdate
        self.rpyr = reptdate.year
        self.rpmth = reptdate.month
        self.rpday = reptdate.day

        # Days in each month
        self.days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

        # Adjust February for leap year in reporting year
        if self.is_leap_year(self.rpyr):
            self.days_in_month[1] = 29

    @staticmethod
    def is_leap_year(year: int) -> bool:
        """Check if year is a leap year"""
        return year % 4 == 0

    def calculate_remaining_months(self, utmdt: datetime) -> float:
        """Calculate remaining months from reptdate to maturity date"""
        mdyr = utmdt.year
        mdmth = utmdt.month
        mdday = utmdt.day

        # Adjust February for maturity year
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

    def get_origdate_category(self, remmth: float) -> str:
        """Get origdate category based on remaining months"""
        if remmth < 12:
            return '50'
        else:
            return '60'


class KAPXProcessor:
    """Main processor for KAPX data"""

    def __init__(self, paths: PathConfig):
        self.paths = paths
        self.date_calc = DateCalculator(paths.reptdate)

    def read_bnmtblx(self) -> pl.DataFrame:
        """Read BNMTBLX text file with fixed-width format"""
        print("Reading BNMTBLX file...")

        if not self.paths.bnmtblx_file.exists():
            print(f"  Warning: {self.paths.bnmtblx_file} not found")
            return pl.DataFrame()

        # Read file with fixed-width columns (skip first row - header)
        with open(self.paths.bnmtblx_file, 'r') as f:
            lines = f.readlines()[1:]  # FIRSTOBS=2

        records = []
        for line in lines:
            if len(line) < 101:
                continue

            record = {
                'UTDLP': line[0:3].strip(),
                'UTDLR': line[3:16].strip(),
                'UTSTY': line[16:19].strip(),
                'UTCUS': line[19:25].strip(),
                'UTCLC': line[25:28].strip(),
                'GFCTP': line[28:30].strip(),
                'GFCNAL': line[30:32].strip(),
                'SVTLX': line[32:52].strip(),
                'UTOSD': line[52:62].strip(),
                'UTTRD': line[62:72].strip(),
                'UTMDD': line[72:75].strip(),
                'UTMMM': line[75:78].strip(),
                'UTMYY': line[78:83].strip(),
                'UTFCV': line[82:101].strip(),
                'UTBFCY': line[100:103].strip() if len(line) > 100 else ''
            }
            records.append(record)

        if not records:
            return pl.DataFrame()

        df = pl.DataFrame(records)

        # Parse UTFCV as float
        df = df.with_columns([
            pl.col('UTFCV').cast(pl.Float64)
        ])

        # Create UTMDT from components
        df = df.with_columns([
            pl.struct(['UTMDD', 'UTMMM', 'UTMYY']).map_elements(
                lambda x: self.parse_maturity_date(x['UTMMM'], x['UTMDD'], x['UTMYY']),
                return_dtype=pl.Date
            ).alias('UTMDT')
        ])

        # Filter out FT deals
        df = df.filter(~pl.col('UTDLP').str.slice(0, 2).eq('FT'))

        print(f"  Loaded {len(df)} records")
        return df

    def parse_maturity_date(self, mm: str, dd: str, yyyy: str) -> datetime:
        """Parse maturity date from components"""
        try:
            month = int(mm)
            day = int(dd)
            year = int(yyyy)
            return datetime(year, month, day)
        except:
            return None

    def create_dummy_codes(self) -> pl.DataFrame:
        """Create dummy BNM codes for all possible combinations"""
        print("Creating dummy BNM codes...")

        records = []
        for h in [6, 7]:
            for i in [0, 3, 4, 10, 21, 22, 23, 50, 70, 90]:
                for j in [0, 50, 60]:
                    bnmcode = f"{h}87{i:02d}80{j:02d}0000Y"
                    records.append({'BNMCODE': bnmcode})

        df = pl.DataFrame(records).sort('BNMCODE')
        print(f"  Created {len(df)} dummy codes")
        return df

    def split_purchase_sale(self, df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Split data into purchase and sale transactions"""
        print("Splitting purchase and sale...")

        # Extract transaction type from position 3 of UTDLP
        df = df.with_columns([
            pl.col('UTDLP').str.slice(2, 1).alias('TYPEPRSL')
        ])

        # Calculate remaining months
        df = df.with_columns([
            pl.col('UTMDT').map_elements(
                lambda x: self.date_calc.calculate_remaining_months(x) if x else 0,
                return_dtype=pl.Float64
            ).alias('REMMTH')
        ])

        df_purchase = df.filter(pl.col('TYPEPRSL') == 'P')
        df_sale = df.filter(pl.col('TYPEPRSL') == 'S')

        print(f"  Purchase: {len(df_purchase)}, Sale: {len(df_sale)}")
        return df_purchase, df_sale

    def map_purchase_codes(self, df: pl.DataFrame) -> pl.DataFrame:
        """Map purchase transactions to BNM codes"""
        print("Mapping purchase codes...")

        results = []
        for row in df.iter_rows(named=True):
            utsty = row['UTSTY']
            amount = row['UTFCV']
            remmth = row['REMMTH']

            origdate = self.date_calc.get_origdate_category(remmth)
            bnmcode = None

            if utsty in ['SSD', 'SLD', 'SDC', 'LDC', 'SZD', 'SFD']:
                bnmcode = f'6870380{origdate}0000Y'
            elif utsty in ['PBA', 'SBA']:
                bnmcode = f'6870480{origdate}0000Y'
            elif utsty in ['DBD', 'DBZ', 'MTN', 'PNB']:
                bnmcode = f'6871080{origdate}0000Y'
            elif utsty == 'MGS':
                bnmcode = f'6872180{origdate}0000Y'
            elif utsty == 'MTB':
                bnmcode = f'6872280{origdate}0000Y'
            elif utsty == 'MGI':
                bnmcode = f'6872380{origdate}0000Y'
            elif utsty in ['CB1', 'CNT']:
                bnmcode = f'6875080{origdate}0000Y'
            elif utsty in ['ISB', 'IDS', 'IBZ', 'KHA', 'SAC', 'SCM', 'SCD', 'SMC', 'ITB', 'BMC']:
                bnmcode = f'6877080{origdate}0000Y'
            elif utsty in ['BMN', 'BMF']:
                bnmcode = f'6879080{origdate}0000Y'

            if bnmcode:
                results.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

        if results:
            df_result = pl.DataFrame(results)
            print(f"  Mapped {len(df_result)} purchase codes")
            return df_result
        else:
            return pl.DataFrame({'BNMCODE': [], 'AMOUNT': []})

    def map_sale_codes(self, df: pl.DataFrame) -> pl.DataFrame:
        """Map sale transactions to BNM codes"""
        print("Mapping sale codes...")

        results = []
        for row in df.iter_rows(named=True):
            utsty = row['UTSTY']
            amount = row['UTFCV']
            remmth = row['REMMTH']

            origdate = self.date_calc.get_origdate_category(remmth)
            bnmcode = None

            if utsty in ['SSD', 'SLD', 'SDC', 'LDC', 'SZD', 'SFD']:
                bnmcode = f'7870380{origdate}0000Y'
            elif utsty in ['PBA', 'SBA']:
                bnmcode = f'7870480{origdate}0000Y'
            elif utsty in ['DBD', 'DBZ', 'MTN', 'PNB']:
                bnmcode = f'7871080{origdate}0000Y'
            elif utsty == 'MGS':
                bnmcode = f'7872180{origdate}0000Y'
            elif utsty == 'MTB':
                bnmcode = f'7872280{origdate}0000Y'
            elif utsty == 'MGI':
                bnmcode = f'7872380{origdate}0000Y'
            elif utsty in ['CB1', 'CNT']:
                bnmcode = f'7875080{origdate}0000Y'
            elif utsty in ['ISB', 'IDS', 'IBZ', 'KHA', 'SAC', 'SCM', 'SCD', 'SMC', 'ITB', 'BMC']:
                bnmcode = f'7877080{origdate}0000Y'
            elif utsty in ['BMN', 'BMF']:
                bnmcode = f'7879080{origdate}0000Y'

            if bnmcode:
                results.append({'BNMCODE': bnmcode, 'AMOUNT': amount})

        if results:
            df_result = pl.DataFrame(results)
            print(f"  Mapped {len(df_result)} sale codes")
            return df_result
        else:
            return pl.DataFrame({'BNMCODE': [], 'AMOUNT': []})

    def create_aggregates(self, df_merged: pl.DataFrame) -> pl.DataFrame:
        """Create aggregate codes at different levels"""
        print("Creating aggregate codes...")

        # Add helper columns
        df_merged = df_merged.with_columns([
            pl.col('BNMCODE').str.slice(0, 7).alias('PREX'),
            pl.col('BNMCODE').str.slice(0, 2).alias('CATX'),
            pl.col('BNMCODE').str.slice(7, 2).alias('MATX')
        ])

        # Aggregate by PREX (first 7 chars)
        df_prex = df_merged.group_by('PREX').agg([
            pl.col('AMOUNT').sum().alias('AMOUNT')
        ]).with_columns([
            (pl.col('PREX') + '000000Y').alias('BNMCODE')
        ]).select(['BNMCODE', 'AMOUNT'])

        # Aggregate by CATX (first 2 chars)
        df_catx = df_merged.group_by('CATX').agg([
            pl.col('AMOUNT').sum().alias('AMOUNT')
        ]).with_columns([
            (pl.col('CATX') + '70080000000Y').alias('BNMCODE')
        ]).select(['BNMCODE', 'AMOUNT'])

        # Aggregate by CATX + MATX
        df_matx = df_merged.group_by(['CATX', 'MATX']).agg([
            pl.col('AMOUNT').sum().alias('AMOUNT')
        ]).filter(
            pl.col('MATX') != '00'
        ).with_columns([
            (pl.col('CATX') + '70080' + pl.col('MATX') + '0000Y').alias('BNMCODE')
        ]).select(['BNMCODE', 'AMOUNT'])

        print(f"  PREX aggregates: {len(df_prex)}")
        print(f"  CATX aggregates: {len(df_catx)}")
        print(f"  MATX aggregates: {len(df_matx)}")

        return df_prex, df_catx, df_matx

    def run(self) -> pl.DataFrame:
        """Execute full processing"""
        print("=" * 80)
        print("KAPX Processing - BNM Table X Securities")
        print("=" * 80)
        print(f"Report Date: {self.paths.rdate}")
        print("=" * 80)

        # Read BNMTBLX
        df_raw = self.read_bnmtblx()

        if len(df_raw) == 0:
            print("No data to process")
            return pl.DataFrame({'ITCODE': [], 'AMTIND': [], 'AMOUNT': []})

        # Create dummy codes
        df_dummy = self.create_dummy_codes()

        # Split purchase and sale
        df_purchase, df_sale = self.split_purchase_sale(df_raw)

        # Map to BNM codes
        df_purcode = self.map_purchase_codes(df_purchase)
        df_salcode = self.map_sale_codes(df_sale)

        # Combine purchase and sale
        df_salpur = pl.concat([df_purcode, df_salcode], how='diagonal')

        if len(df_salpur) > 0:
            df_salpur = df_salpur.group_by('BNMCODE').agg([
                pl.col('AMOUNT').sum().alias('AMOUNT')
            ]).sort('BNMCODE')

        # Merge with dummy codes (fill missing with 0)
        df_merged = df_dummy.join(df_salpur, on='BNMCODE', how='left')
        df_merged = df_merged.with_columns([
            pl.col('AMOUNT').fill_null(0.0)
        ])

        # Summarize by BNMCODE
        df_mergx = df_merged.group_by('BNMCODE').agg([
            pl.col('AMOUNT').sum().alias('AMOUNT')
        ]).sort('BNMCODE')

        # Create aggregates
        df_prex, df_catx, df_matx = self.create_aggregates(df_merged)

        # Combine all
        df_kapx = pl.concat([df_mergx, df_prex, df_catx, df_matx], how='diagonal')

        # Add AMTIND and ITCODE
        df_kapx = df_kapx.with_columns([
            pl.lit('D').alias('AMTIND'),
            pl.col('BNMCODE').alias('ITCODE')
        ])

        # Final summary
        df_kapx = df_kapx.group_by(['ITCODE', 'AMTIND']).agg([
            pl.col('AMOUNT').sum().alias('AMOUNT')
        ])

        # Write output
        output_file = self.paths.get_output_path('KAPX')
        df_kapx.write_parquet(output_file)
        print(f"\nWritten to {output_file}")

        # Display summary
        print("\n" + "=" * 80)
        print("Processing Summary:")
        print("=" * 80)
        print(f"Total Records: {len(df_kapx)}")

        if len(df_kapx) > 0:
            total_amount = df_kapx['AMOUNT'].sum()
            print(f"Total Amount: {total_amount:,.2f}")

            print("\nSample Records (first 20):")
            print(df_kapx.sort(['ITCODE', 'AMTIND']).head(20))

        print("=" * 80)
        print("KAPX processing completed successfully")
        print("=" * 80)

        return df_kapx


def main():
    """Main entry point"""
    try:
        paths = PathConfig()
        processor = KAPXProcessor(paths)
        processor.run()
        return 0
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
