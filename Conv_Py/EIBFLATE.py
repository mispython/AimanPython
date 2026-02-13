#!/usr/bin/env python3
"""
Program: EIBFLATE
Purpose: Loan Late Payment Data Update Processing

This program updates loan note data (LNNOTE) with late payment information from multiple sources
    including MIS files, HP files, RNR reclass files, and AKPK nursing data.
Only runs on week 4 (month-end).
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import polars as pl
import pyarrow.parquet as pq


class PathConfig:
    """Path configuration for EIBFLATE"""

    def __init__(self):
        base_path = Path(os.getenv('BNM_DATA_PATH', '/data'))
        output_path = Path(os.getenv('BNM_OUTPUT_PATH', '/data/output'))

        # Input directories
        self.rbp2_lib = base_path / "RBP2.B033"
        self.loan_lib = base_path / "LOAN"
        self.sap_pbb = base_path / "SAP.PBB"
        self.sap_pibb = base_path / "SAP.PIBB"

        # Output directory
        self.fixloan_lib = output_path / "SAP.LOAN.FIX.LATE"
        self.fixloan_lib.mkdir(parents=True, exist_ok=True)

        # Calculate dates and week
        self.calculate_dates()

    def calculate_dates(self):
        """Calculate reporting dates and week"""
        # Read REPTDATE from LOAN library
        reptdate_file = self.loan_lib / "REPTDATE.parquet"
        if reptdate_file.exists():
            df = pl.read_parquet(reptdate_file)
            self.reptdate = df['REPTDATE'][0]
            if isinstance(self.reptdate, str):
                self.reptdate = datetime.strptime(self.reptdate, '%Y-%m-%d')
        else:
            # Default to last day of previous month
            today = datetime.now()
            first_of_month = datetime(today.year, today.month, 1)
            self.reptdate = first_of_month - timedelta(days=1)

        day = self.reptdate.day

        # Determine week
        if 1 <= day <= 8:
            self.nowk = '1'
        elif 9 <= day <= 15:
            self.nowk = '2'
        elif 16 <= day <= 22:
            self.nowk = '3'
        else:
            self.nowk = '4'

        self.reptmon = f"{self.reptdate.month:02d}"
        self.reptyr = f"{self.reptdate.year:04d}"

    def get_input_path(self, filename: str) -> Path:
        """Get input file path"""
        return self.rbp2_lib / filename

    def get_loan_path(self, filename: str) -> Path:
        """Get loan library path"""
        return self.loan_lib / filename

    def get_output_path(self, dataset_name: str) -> Path:
        """Get output path"""
        return self.fixloan_lib / f"{dataset_name}.parquet"


class LoanUpdateProcessor:
    """Main processor for loan late payment updates"""

    def __init__(self, paths: PathConfig):
        self.paths = paths

    def parse_packed_decimal(self, value: bytes, length: int) -> int:
        """Parse packed decimal format (PD)"""
        # Simplified packed decimal parser - assumes positive numbers
        if not value:
            return 0
        try:
            # For PD6 and PD3, convert appropriately
            return int(value)
        except:
            return 0

    def parse_date_ddmmyy8(self, date_str: str) -> datetime:
        """Parse DDMMYY8 format date"""
        try:
            return datetime.strptime(date_str, '%d%m%Y')
        except:
            return None

    def parse_date_yymmdd8(self, date_int: int) -> datetime:
        """Parse YYMMDD8 format date from integer"""
        try:
            date_str = str(date_int).zfill(8)
            return datetime.strptime(date_str, '%Y%m%d')
        except:
            return None

    def read_lnstat_file(self) -> pl.DataFrame:
        """Read loan status file (LNSTATFL)"""
        print("Reading LNSTATFL...")

        file_path = self.paths.get_input_path("INTRADAY.NPL.BTCARD.parquet")
        if not file_path.exists():
            print(f"  Warning: {file_path} not found")
            return pl.DataFrame({'ACCTNO': [], 'NOTENO': [], 'LOANSTAT': []})

        df = pl.read_parquet(file_path)

        # Select relevant columns (positions 8, 19, 80)
        df = df.select(['ACCTNO', 'NOTENO', 'LOANSTAT'])

        print(f"  Loaded {len(df)} records")
        return df

    def read_misfile(self) -> pl.DataFrame:
        """Read MIS file"""
        print("Reading MISFILE...")

        file_path = self.paths.get_input_path("LNINTR.NPL.MISFILE.parquet")
        if not file_path.exists():
            print(f"  Warning: {file_path} not found")
            return pl.DataFrame({'ACCTNO': [], 'NOTENO': [], 'RISKRATE': [], 'BILDUE1': [], 'BLDATE': []})

        df = pl.read_parquet(file_path)

        # Select relevant columns
        df = df.select(['ACCTNO', 'NOTENO', 'RISKRATE', 'BILDUE1'])

        # Convert BILDUE1 to BLDATE
        df = df.with_columns([
            pl.when(pl.col('BILDUE1') > 0)
            .then(
                pl.col('BILDUE1').cast(pl.Utf8).str.zfill(11).str.slice(0, 8)
                .str.to_date('%Y%m%d', strict=False)
            )
            .otherwise(None)
            .alias('BLDATE')
        ])

        print(f"  Loaded {len(df)} records")
        return df

    def read_hpfile(self) -> pl.DataFrame:
        """Read HP file"""
        print("Reading HPFILE...")

        file_path = self.paths.get_input_path("INTRADAY.NPL.FILE.HP.parquet")
        if not file_path.exists():
            print(f"  Warning: {file_path} not found")
            return pl.DataFrame({'ACCTNO': [], 'NOTENO': [], 'BILDUE1': [], 'BLDATE': []})

        df = pl.read_parquet(file_path)

        # Select relevant columns
        df = df.select(['ACCTNO', 'NOTENO', 'BILDUE1'])

        # Convert BILDUE1 to BLDATE
        df = df.with_columns([
            pl.when(pl.col('BILDUE1') > 0)
            .then(
                pl.col('BILDUE1').cast(pl.Utf8).str.zfill(11).str.slice(0, 8)
                .str.to_date('%Y%m%d', strict=False)
            )
            .otherwise(None)
            .alias('BLDATE')
        ])

        print(f"  Loaded {len(df)} records")
        return df

    def read_rnr_file(self) -> pl.DataFrame:
        """Read RNR reclass file"""
        print("Reading RNRFILE...")

        file_path = self.paths.get_input_path("LNRNRMIS.RECLASS.0.parquet")
        if not file_path.exists():
            print(f"  Warning: {file_path} not found")
            return pl.DataFrame()

        df = pl.read_parquet(file_path)

        # Select and process columns
        cols_to_keep = ['RRCYCLE', 'ACCTNO', 'NOTENO', 'BILDUE1', 'LOANSTAT',
                        'NUMCPNS', 'FCLOSUREDT', 'BILLCNT', 'ASSMDATE', 'RSN',
                        'RR_IL_RECLASS_DT']

        df = df.select([col for col in cols_to_keep if col in df.columns])

        # Convert BILDUE1 to BLDATE
        if 'BILDUE1' in df.columns:
            df = df.with_columns([
                pl.when(pl.col('BILDUE1') > 0)
                .then(
                    pl.col('BILDUE1').cast(pl.Utf8).str.zfill(11).str.slice(0, 8)
                    .str.to_date('%Y%m%d', strict=False)
                )
                .otherwise(None)
                .alias('BLDATE')
            ])

        # Remove duplicates
        df = df.unique(subset=['ACCTNO', 'NOTENO'], keep='first')

        print(f"  Loaded {len(df)} records")
        return df

    def read_rnr_mor(self) -> pl.DataFrame:
        """Read RNR MOR file"""
        print("Reading RNR MOR...")

        file_path = self.paths.get_input_path("LNSASMOR.OUTMIS.parquet")
        if not file_path.exists():
            print(f"  Warning: {file_path} not found")
            return pl.DataFrame()

        df = pl.read_parquet(file_path)

        cols_to_keep = ['ACCTNO', 'NOTENO', 'MORDAYARR', 'MO_INSTL_ARR']
        df = df.select([col for col in cols_to_keep if col in df.columns])

        # Remove duplicates
        df = df.unique(subset=['ACCTNO', 'NOTENO'], keep='first')

        print(f"  Loaded {len(df)} records")
        return df

    def read_rnr_cov(self) -> pl.DataFrame:
        """Read RNR coverage file"""
        print("Reading RNR COV...")

        file_path = self.paths.get_input_path("LNMORCOV.OUTMIS.parquet")
        if not file_path.exists():
            print(f"  Warning: {file_path} not found")
            return pl.DataFrame()

        df = pl.read_parquet(file_path)

        cols_to_keep = ['ACCTNO', 'NOTENO', 'MORDAYARR', 'MO_INSTL_ARR', 'LOANSTAT']
        df = df.select([col for col in cols_to_keep if col in df.columns])

        # Remove duplicates
        df = df.unique(subset=['ACCTNO', 'NOTENO'], keep='first')

        print(f"  Loaded {len(df)} records")
        return df

    def read_rnr_hp(self) -> pl.DataFrame:
        """Read RNR HP file"""
        print("Reading RNR HP...")

        file_path = self.paths.get_input_path("LNRNRMIS.RECLASS.HP.0.parquet")
        if not file_path.exists():
            print(f"  Warning: {file_path} not found")
            return pl.DataFrame()

        df = pl.read_parquet(file_path)

        cols_to_keep = ['RRCYCLE', 'ACCTNO', 'NOTENO', 'USER5', 'NUMCPNS',
                        'FCLOSUREDT', 'CPNSTDTE']
        df = df.select([col for col in cols_to_keep if col in df.columns])

        # Remove duplicates
        df = df.unique(subset=['ACCTNO', 'NOTENO'], keep='first')

        print(f"  Loaded {len(df)} records")
        return df

    def read_rnr_hp_mor(self) -> pl.DataFrame:
        """Read RNR HP MOR file"""
        print("Reading RNR HP MOR...")

        file_path = self.paths.get_input_path("LNSASMOR.OUTMIS.HP.parquet")
        if not file_path.exists():
            print(f"  Warning: {file_path} not found")
            return pl.DataFrame()

        df = pl.read_parquet(file_path)

        cols_to_keep = ['ACCTNO', 'NOTENO', 'MORDAYARR']
        df = df.select([col for col in cols_to_keep if col in df.columns])

        # Remove duplicates
        df = df.unique(subset=['ACCTNO', 'NOTENO'], keep='first')

        print(f"  Loaded {len(df)} records")
        return df

    def read_akpk(self) -> pl.DataFrame:
        """Read AKPK nursing file"""
        print("Reading AKPK...")

        # Two files to combine
        file_paths = [
            self.paths.get_input_path("LNLNNRID.MTHEND.0.parquet"),
            self.paths.get_input_path("LNHPNRID.MTHEND.0.parquet")
        ]

        dfs = []
        for file_path in file_paths:
            if file_path.exists():
                df = pl.read_parquet(file_path)
                dfs.append(df)

        if not dfs:
            print(f"  Warning: AKPK files not found")
            return pl.DataFrame()

        df = pl.concat(dfs, how='diagonal')

        cols_to_keep = ['ACCTNO', 'NOTENO', 'NURS_TAG', 'NUR_STARTDT',
                        'NURS_TAGDT', 'NURS_ENDDT', 'NURS_COUNTER']
        df = df.select([col for col in cols_to_keep if col in df.columns])

        # Remove duplicates
        df = df.unique(subset=['ACCTNO', 'NOTENO'], keep='first')

        print(f"  Loaded {len(df)} records")
        return df

    def update_loan_data(self, loan_file: Path, output_file: Path):
        """Update loan note data with all sources"""
        print(f"\nUpdating {loan_file.name}...")

        # Read base LNNOTE
        df_loan = pl.read_parquet(loan_file)
        df_loan = df_loan.sort(['ACCTNO', 'NOTENO'])
        print(f"  Base records: {len(df_loan)}")

        # Read all update sources
        df_misfile = self.read_misfile()
        df_lnstat = self.read_lnstat_file()

        # Merge MISFILE and LNSTAT
        df_loan = df_loan.join(df_misfile, on=['ACCTNO', 'NOTENO'], how='left', suffix='_mis')
        df_loan = df_loan.join(df_lnstat, on=['ACCTNO', 'NOTENO'], how='left', suffix='_stat')

        # Update columns from MISFILE
        if 'RISKRATE_mis' in df_loan.columns:
            df_loan = df_loan.with_columns([
                pl.coalesce('RISKRATE_mis', 'RISKRATE').alias('RISKRATE')
            ])
        if 'BLDATE_mis' in df_loan.columns:
            df_loan = df_loan.with_columns([
                pl.coalesce('BLDATE_mis', 'BLDATE').alias('BLDATE')
            ])
        if 'LOANSTAT_stat' in df_loan.columns:
            df_loan = df_loan.with_columns([
                pl.coalesce('LOANSTAT_stat', 'LOANSTAT').alias('LOANSTAT')
            ])

        # Read HP file
        df_hpfile = self.read_hpfile()

        # Merge HPFILE
        df_loan = df_loan.join(df_hpfile, on=['ACCTNO', 'NOTENO'], how='left', suffix='_hp')
        if 'BLDATE_hp' in df_loan.columns:
            df_loan = df_loan.with_columns([
                pl.coalesce('BLDATE_hp', 'BLDATE').alias('BLDATE')
            ])

        # Read RNR files
        df_rnr = self.read_rnr_file()
        df_rnrhp = self.read_rnr_hp()
        df_rnrmor = self.read_rnr_mor()
        df_rnrhpmor = self.read_rnr_hp_mor()
        df_akpk = self.read_akpk()
        df_rnrcov = self.read_rnr_cov()

        # Merge all RNR sources
        for df_source, suffix in [
            (df_rnr, '_rnr'),
            (df_rnrhp, '_rnrhp'),
            (df_rnrmor, '_rnrmor'),
            (df_rnrhpmor, '_rnrhpmor'),
            (df_akpk, '_akpk'),
            (df_rnrcov, '_rnrcov')
        ]:
            if len(df_source) > 0:
                df_loan = df_loan.join(df_source, on=['ACCTNO', 'NOTENO'], how='left', suffix=suffix)

        # Apply USER5 logic: IF USER5 NE 'N' THEN OLDNOTEDAYARR = 0
        if 'USER5_rnrhp' in df_loan.columns and 'OLDNOTEDAYARR' in df_loan.columns:
            df_loan = df_loan.with_columns([
                pl.when(pl.col('USER5_rnrhp') != 'N')
                .then(pl.lit(0))
                .otherwise(pl.col('OLDNOTEDAYARR'))
                .alias('OLDNOTEDAYARR')
            ])

        # Drop temporary columns
        cols_to_drop = [col for col in df_loan.columns if any(
            col.endswith(suf) for suf in ['_mis', '_stat', '_hp', '_rnr', '_rnrhp',
                                          '_rnrmor', '_rnrhpmor', '_akpk', '_rnrcov']
        )]
        if cols_to_drop:
            df_loan = df_loan.drop(cols_to_drop)

        # Write updated LNNOTE
        df_loan.write_parquet(output_file)
        print(f"  Written {len(df_loan)} records to {output_file}")

    def save_backup_files(self):
        """Save backup copies of source files"""
        print("\nSaving backup files...")

        backup_datasets = [
            (self.read_misfile(), f"MISFILE{self.paths.reptmon}{self.paths.reptyr}"),
            (self.read_lnstat_file(), f"LNSTAT{self.paths.reptmon}{self.paths.reptyr}"),
            (self.read_hpfile(), f"HPFILE{self.paths.reptmon}{self.paths.reptyr}"),
            (self.read_rnr_file(), f"RNRFILE{self.paths.reptmon}{self.paths.reptyr}"),
            (self.read_rnr_mor(), f"MORLNFL{self.paths.reptmon}{self.paths.reptyr}"),
            (self.read_rnr_cov(), f"RNRCOV{self.paths.reptmon}{self.paths.reptyr}"),
            (self.read_rnr_hp(), f"RNRHPFL{self.paths.reptmon}{self.paths.reptyr}"),
            (self.read_rnr_hp_mor(), f"MORHPFL{self.paths.reptmon}{self.paths.reptyr}"),
            (self.read_akpk(), f"AKPKFL{self.paths.reptmon}{self.paths.reptyr}")
        ]

        for df, dataset_name in backup_datasets:
            if len(df) > 0:
                output_file = self.paths.get_output_path(dataset_name)
                df.write_parquet(output_file)
                print(f"  Saved {dataset_name}")

    def run(self):
        """Execute full processing"""
        print("=" * 80)
        print("EIBFLATE - Loan Late Payment Data Update")
        print("=" * 80)
        print(f"Report Date: {self.paths.reptdate.strftime('%Y-%m-%d')}")
        print(f"Week: {self.paths.nowk}")
        print(f"Month: {self.paths.reptmon}")
        print(f"Year: {self.paths.reptyr}")
        print("=" * 80)

        # Only run on week 4 (month-end)
        if self.paths.nowk != '4':
            print(f"\nSkipping - only runs on week 4 (current week: {self.paths.nowk})")
            print("=" * 80)
            return

        print("\nProcessing month-end updates...")

        # Update PBB LOAN
        pbb_loan_file = self.paths.sap_pbb / "MNILN.0.parquet"
        pbb_output_file = self.paths.loan_lib / "LNNOTE.parquet"
        if pbb_loan_file.exists():
            self.update_loan_data(pbb_loan_file, pbb_output_file)

        # Update PIBB ILOAN
        pibb_loan_file = self.paths.sap_pibb / "MNILN.0.parquet"
        pibb_output_file = self.paths.loan_lib / "ILNNOTE.parquet"
        if pibb_loan_file.exists():
            self.update_loan_data(pibb_loan_file, pibb_output_file)

        # Save backup files
        self.save_backup_files()

        print("\n" + "=" * 80)
        print("EIBFLATE processing completed successfully")
        print("=" * 80)


def main():
    """Main entry point"""
    try:
        paths = PathConfig()
        processor = LoanUpdateProcessor(paths)
        processor.run()
        return 0
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
