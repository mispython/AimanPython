#!/usr/bin/env python3
"""
Program: KALMLIIE
K3TBLFE Processing - KAPITI Table 3 Foreign Exchange

This program processes KAPITI Table 3 data for foreign exchange securities,
    mapping them to BNM codes based on security type, maturity, and transaction type.
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import polars as pl
import pyarrow.parquet as pq
from typing import Optional


class PathConfig:
    """Path configuration for K3TBLFE processing"""

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
        self.reptyear = int(os.getenv('REPTYEAR', str(datetime.now().year)))

        # Parse reporting date
        self.reptdate = self.parse_rdate()

    def parse_rdate(self) -> datetime:
        """Parse RDATE string to datetime"""
        rdate_str = os.getenv('RDATE', datetime.now().strftime('%d%m%Y'))
        try:
            return datetime.strptime(rdate_str, '%d%m%Y')
        except:
            return datetime.now()

    def get_input_path(self, dataset_name: str) -> Path:
        """Get input path for a dataset"""
        return self.bnmk_lib / f"{dataset_name}.parquet"

    def get_output_path(self, dataset_name: str) -> Path:
        """Get output path for a dataset"""
        return self.bnm_lib / f"{dataset_name}.parquet"


class K3TBLFEProcessor:
    """Main processor for K3TBLFE data"""

    def __init__(self, paths: PathConfig):
        self.paths = paths
        self.amtind = 'I'

    def parse_date(self, date_str: str, format_type: str = 'YYMMDD10') -> Optional[datetime]:
        """Parse date string to datetime"""
        if not date_str or date_str.strip() == '':
            return None

        try:
            # YYMMDD10 format: YYYY-MM-DD
            if format_type == 'YYMMDD10':
                return datetime.strptime(date_str, '%Y-%m-%d')
            return None
        except:
            return None

    def calculate_remmth(self, days: int) -> str:
        """Calculate remaining months category based on days to maturity"""
        if days <= 365:
            return '05'
        else:
            return '06'

    def map_bnmcode(self, utdlp: str, utsty: str, islm: str, remmth: str) -> Optional[str]:
        """Map UTDLP, UTSTY, ISLM to BNMCODE"""
        bnmcode = None

        # Purchase transactions (MOP, MSP, IUP, IOP, ISP)
        if utdlp in ['MOP', 'MSP', 'IUP', 'IOP', 'ISP']:
            if utsty in ['SSD', 'SLD', 'SFD', 'SZD']:
                bnmcode = f'687038{remmth}00000Y'
            elif utsty == 'SBA' and remmth == '05':
                bnmcode = '6870480500000Y'
            elif utsty == 'FCF':
                bnmcode = f'687108{remmth}00000Y'
            elif utsty == 'MGS':
                bnmcode = f'687218{remmth}00000Y'
            elif utsty == 'MTB' and remmth == '05':
                bnmcode = '6872280500000Y'
            elif utsty == 'MGI':
                bnmcode = f'687238{remmth}00000Y'
            elif utsty in ['CB1', 'CB2', 'PNB', 'SMC']:
                bnmcode = f'687508{remmth}00000Y'
            elif utsty == 'SAC' and islm == 'N':
                bnmcode = f'687508{remmth}00000Y'
            elif utsty in ['SMC', 'KHA', 'DMB', 'DHB']:
                bnmcode = f'687708{remmth}00000Y'
            elif utsty == 'SAC' and islm == 'Y':
                bnmcode = f'687708{remmth}00000Y'
            elif utsty in ['BNB', 'BNN'] and remmth == '05':
                bnmcode = '6879080500000Y'
            elif utsty in ['BMN', 'BMC', 'BMF'] and remmth == '05':
                bnmcode = '6879080500000Y'

        # Sale transactions (MOS, MSS, IUS, IOS, ISS)
        elif utdlp in ['MOS', 'MSS', 'IUS', 'IOS', 'ISS']:
            if utsty in ['SSD', 'SLD', 'SFD', 'SZD']:
                bnmcode = f'787038{remmth}00000Y'
            elif utsty == 'SBA' and remmth == '05':
                bnmcode = '7870480500000Y'
            elif utsty == 'FCF':
                bnmcode = f'787108{remmth}00000Y'
            elif utsty == 'MGS':
                bnmcode = f'787218{remmth}00000Y'
            elif utsty == 'MTB' and remmth == '05':
                bnmcode = '7872280500000Y'
            elif utsty == 'MGI':
                bnmcode = f'787238{remmth}00000Y'
            elif utsty in ['CB1', 'CB2', 'PNB', 'SMC']:
                bnmcode = f'787508{remmth}00000Y'
            elif utsty == 'SAC' and islm == 'N':
                bnmcode = f'787508{remmth}00000Y'
            elif utsty in ['SMC', 'KHA', 'DMB', 'DHB']:
                bnmcode = f'787708{remmth}00000Y'
            elif utsty == 'SAC' and islm == 'Y':
                bnmcode = f'787708{remmth}00000Y'
            elif utsty in ['BNB', 'BNN'] and remmth == '05':
                bnmcode = '7879080500000Y'
            elif utsty in ['BMN', 'BMC', 'BMF'] and remmth == '05':
                bnmcode = '7879080500000Y'

        return bnmcode

    def process_k3table(self) -> pl.DataFrame:
        """Process K3TBL data"""
        print("Processing K3TBL data...")

        # Read K3TBL dataset
        k3tbl_file = self.paths.get_input_path(f"K3TBL{self.paths.reptmon}{self.paths.nowk}")
        df = pl.read_parquet(k3tbl_file)

        print(f"  Loaded {len(df)} records")

        # Filter: UTCTP IN ('BW','BA','BE','EB','CE','GA')
        df = df.filter(pl.col('UTCTP').is_in(['BW', 'BA', 'BE', 'EB', 'CE', 'GA']))
        print(f"  After UTCTP filter: {len(df)} records")

        # Add AMTIND
        df = df.with_columns([
            pl.lit(self.amtind).alias('AMTIND')
        ])

        results = []

        for row in df.iter_rows(named=True):
            utctp = row.get('UTCTP', '')
            utdlp = row.get('UTDLP', '')
            utsty = row.get('UTSTY', '')
            islm = row.get('ISLM', '')
            utmdt = row.get('UTMDT', '')
            utosd = row.get('UTOSD', '')
            utamts = row.get('UTAMTS', 0)

            # Parse maturity date
            matdt = self.parse_date(utmdt, 'YYMMDD10')
            if matdt is None:
                continue

            # Parse issue date (only if UTOSD is not empty)
            if utosd and utosd.strip() != '':
                issdt = self.parse_date(utosd, 'YYMMDD10')
            else:
                continue

            if issdt is None:
                continue

            # Extract month and year from issue date
            trmm = issdt.month
            tryy = issdt.year

            # Filter: TRMM=&REPTMON AND TRYY=&REPTYEAR
            if trmm != int(self.paths.reptmon) or tryy != self.paths.reptyear:
                continue

            # Calculate days to maturity
            days = (matdt - self.paths.reptdate).days

            # Calculate remaining months category
            remmth = self.calculate_remmth(days)

            # Map to BNMCODE
            bnmcode = self.map_bnmcode(utdlp, utsty, islm, remmth)

            if bnmcode:
                results.append({
                    'ITCODE': bnmcode,
                    'AMTIND': self.amtind,
                    'AMOUNT': utamts
                })

        if results:
            df_result = pl.DataFrame(results)
            print(f"  Processed: {len(df_result)} records")
            return df_result
        else:
            return pl.DataFrame({'ITCODE': [], 'AMTIND': [], 'AMOUNT': []})

    def summarize_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Summarize data by ITCODE and AMTIND"""
        print("Summarizing data...")

        if len(df) == 0:
            return df

        # Summarize by ITCODE and AMTIND
        df_summary = df.group_by(['ITCODE', 'AMTIND']).agg([
            pl.col('AMOUNT').sum().alias('AMOUNT')
        ])

        print(f"  Summary: {len(df_summary)} records")
        return df_summary

    def run(self) -> pl.DataFrame:
        """Execute full processing and return result"""
        print("=" * 80)
        print("K3TBLFE - KAPITI Table 3 Foreign Exchange Processing")
        print("=" * 80)

        # Process K3 table
        df_processed = self.process_k3table()

        # Summarize
        df_summary = self.summarize_data(df_processed)

        # Write output
        output_file = self.paths.get_output_path('K3FEI')
        df_summary.write_parquet(output_file)
        print(f"\nWritten to {output_file}")

        # Display summary
        print("\n" + "=" * 80)
        print("Processing Summary:")
        print("=" * 80)
        print(f"Month: {self.paths.reptmon}")
        print(f"Year: {self.paths.reptyear}")
        print(f"Week: {self.paths.nowk}")
        print(f"Total Records: {len(df_summary)}")

        if len(df_summary) > 0:
            total_amount = df_summary['AMOUNT'].sum()
            print(f"Total Amount: {total_amount:,.2f}")

            # Show sample records
            print("\nSample Records (first 20):")
            print(df_summary.sort(['ITCODE', 'AMTIND']).head(20))

        print("=" * 80)
        print("K3TBLFE processing completed successfully")
        print("=" * 80)

        return df_summary


def main():
    """Main entry point"""
    try:
        paths = PathConfig()
        processor = K3TBLFEProcessor(paths)
        processor.run()
        return 0
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
