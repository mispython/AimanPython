# !/usr/bin/env python3
"""
Program: EIIMTH1A
BNM Reporting Data Processing

Purpose: Processes banking data for BNM regulatory reporting.
         Handles deposits, loans, fixed deposits, and generates various regulatory reports.
"""

import os
import sys
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from dateutil.relativedelta import relativedelta
import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Dict, Tuple, Optional
import subprocess
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PathConfig:
    """Centralized path configuration for all input/output files"""

    def __init__(self, base_path: str = "/data"):
        self.base_path = Path(base_path)

        # Input paths
        self.loan = self.base_path / "SAP.PIBB.MNILN.0.parquet"
        self.fd = self.base_path / "SAP.PIBB.MNIFD.0.parquet"
        self.lmloan = self.base_path / "SAP.PIBB.MNILN.-4.parquet"
        self.lmte = self.base_path / "SAP.PIBB.MNILIMT.0.parquet"
        self.deposit = self.base_path / "SAP.PIBB.MNITB.0.parquet"
        self.lmdept = self.base_path / "SAP.PIBB.MNITB.-4.parquet"
        self.cisdp = self.base_path / "RBP2.B033.CCRIS.CISDEMO.DP.GDG.0.parquet"
        self.bnmtbl1 = self.base_path / "SAP.PIBB.KAPITI1.0.txt"
        self.bnmtbl2 = self.base_path / "SAP.PIBB.KAPITI2.0.txt"
        self.bnmtbl3 = self.base_path / "SAP.PIBB.KAPITI3.0.txt"
        self.lnhist = self.base_path / "SAP.PBB.LNHIST.parquet"
        self.forate = self.base_path / "SAP.PIBB.FCYCA.parquet"
        self.iwomv = self.base_path / "SAP.PIBB.WOF.WOMV.parquet"
        self.nid = self.base_path / "SAP.PIBB.RNID.SASDATA.parquet"
        self.bnmk = self.base_path / "SAP.PIBB.KAPITI.SASDATA.parquet"
        self.dispay = self.base_path / "SAP.PIBB.DISPAY.parquet"
        self.daily = self.base_path / "SAP.PIBB.DISPAY.DAILY.parquet"
        self.sasd = self.base_path / "SAP.PIBB.STORE.SASDATA.parquet"
        self.uma = self.base_path / "SAP.PIBB.UMA.DAILY.0.parquet"
        self.pgm = self.base_path / "SAP.BNM.PROGRAM"

        # Output paths
        self.output_base = self.base_path / "output"
        self.output_base.mkdir(parents=True, exist_ok=True)

        self.depoback = self.output_base / "SAP.PIBB.DEPOSIT.+1.parquet"
        self.rdalkm = self.output_base / "SAP.PIBB.KAPMNI.RDAL.txt"
        self.nsrskm = self.output_base / "SAP.PIBB.NSRS.KAPMNI.RDAL.txt"
        self.temp = self.output_base / "SAP.TEMP.dat"
        self.rdaltmp = self.output_base / "SAP.PIBB.TEMP.RDAL.dat"
        self.rdalftp = self.output_base / "SAP.PIBB.TEMP.RDAL.RDALFTP.txt"
        self.sftp01 = self.output_base / "FTPPUT.txt"

        # Working directory for BNM library
        self.bnm_lib = self.output_base / "BNM"
        self.bnm_lib.mkdir(parents=True, exist_ok=True)

        self.bnm1_lib = self.base_path / "SAP.PIBB.SASDATA"


class DateCalculator:
    """Calculate reporting dates and related parameters"""

    def __init__(self, report_date: Optional[datetime] = None):
        if report_date is None:
            # Calculate as: 01 + current month + current year - 1 day
            today = datetime.now()
            first_of_month = datetime(today.year, today.month, 1)
            self.reptdate = first_of_month - timedelta(days=1)
        else:
            self.reptdate = report_date

        self._calculate_parameters()

    def _calculate_parameters(self):
        """Calculate week, month, and date parameters based on reptdate"""
        day = self.reptdate.day

        # Determine week and SDD based on day
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

        # Calculate MM1
        if self.wk == '1':
            self.mm1 = self.mm - 1
            if self.mm1 == 0:
                self.mm1 = 12
        else:
            self.mm1 = self.mm

        # Calculate MM2
        self.mm2 = self.mm - 1
        if self.mm2 == 0:
            self.mm2 = 12

        # Calculate SDATE
        self.sdate = datetime(self.reptdate.year, self.mm, self.sdd)

        self.sdesc = 'PUBLIC ISLAMIC BANK BERHAD'

        # Format strings
        self.nowk = self.wk
        self.nowk1 = '1'
        self.nowk2 = '2'
        self.nowk3 = '3'
        self.reptmon = f"{self.mm:02d}"
        self.reptmon1 = f"{self.mm1:02d}"
        self.reptmon2 = f"{self.mm2:02d}"
        self.reptyear = f"{self.reptdate.year}"
        self.reptday = f"{self.reptdate.day:02d}"
        self.rdate = self.reptdate.strftime("%d%m%Y")
        self.tdate = self.reptdate
        self.sdate_str = self.sdate.strftime("%d%m%Y")

    def get_macro_vars(self) -> Dict[str, str]:
        """Return dictionary of macro variables for substitution"""
        return {
            'NOWK': self.nowk,
            'NOWK1': self.nowk1,
            'NOWK2': self.nowk2,
            'NOWK3': self.nowk3,
            'REPTMON': self.reptmon,
            'REPTMON1': self.reptmon1,
            'REPTMON2': self.reptmon2,
            'REPTYEAR': self.reptyear,
            'REPTDAY': self.reptday,
            'RDATE': self.rdate,
            'TDATE': str(self.tdate),
            'SDATE': self.sdate_str,
            'SDESC': self.sdesc
        }


class InputValidator:
    """Validate input file dates match expected reporting date"""

    def __init__(self, paths: PathConfig, date_calc: DateCalculator):
        self.paths = paths
        self.date_calc = date_calc
        self.expected_date = date_calc.rdate

    def get_loan_date(self) -> str:
        """Extract REPTDATE from LOAN file"""
        try:
            df = pl.read_parquet(self.paths.loan, n_rows=1)
            if 'REPTDATE' in df.columns:
                reptdate = df['REPTDATE'][0]
                if isinstance(reptdate, datetime):
                    return reptdate.strftime("%d%m%Y")
                else:
                    return str(reptdate)
        except Exception as e:
            logger.error(f"Error reading LOAN date: {e}")
            return ""
        return ""

    def get_deposit_date(self) -> str:
        """Extract REPTDATE from DEPOSIT file"""
        try:
            df = pl.read_parquet(self.paths.deposit, n_rows=1)
            if 'REPTDATE' in df.columns:
                reptdate = df['REPTDATE'][0]
                if isinstance(reptdate, datetime):
                    return reptdate.strftime("%d%m%Y")
                else:
                    return str(reptdate)
        except Exception as e:
            logger.error(f"Error reading DEPOSIT date: {e}")
            return ""
        return ""

    def get_fd_date(self) -> str:
        """Extract REPTDATE from FD file"""
        try:
            df = pl.read_parquet(self.paths.fd, n_rows=1)
            if 'REPTDATE' in df.columns:
                reptdate = df['REPTDATE'][0]
                if isinstance(reptdate, datetime):
                    return reptdate.strftime("%d%m%Y")
                else:
                    return str(reptdate)
        except Exception as e:
            logger.error(f"Error reading FD date: {e}")
            return ""
        return ""

    def get_kapiti1_date(self) -> str:
        """Extract REPTDATE from BNMTBL1 file"""
        try:
            with open(self.paths.bnmtbl1, 'r') as f:
                first_line = f.readline().strip()
                parts = first_line.split('|')
                if parts:
                    reptdate_str = parts[0]
                    reptdate = datetime.strptime(reptdate_str, "%Y%m%d")
                    return reptdate.strftime("%d%m%Y")
        except Exception as e:
            logger.error(f"Error reading KAPITI1 date: {e}")
            return ""
        return ""

    def get_kapiti2_date(self) -> str:
        """Extract REPTDATE from BNMTBL2 file"""
        try:
            with open(self.paths.bnmtbl2, 'r') as f:
                first_line = f.readline().strip()
                parts = first_line.split('|')
                if parts:
                    reptdate_str = parts[0]
                    reptdate = datetime.strptime(reptdate_str, "%Y%m%d")
                    return reptdate.strftime("%d%m%Y")
        except Exception as e:
            logger.error(f"Error reading KAPITI2 date: {e}")
            return ""
        return ""

    def get_kapiti3_date(self) -> str:
        """Extract REPTDATE from BNMTBL3 file"""
        try:
            with open(self.paths.bnmtbl3, 'r') as f:
                first_line = f.readline().strip()
                parts = first_line.split('|')
                if parts:
                    reptdate_str = parts[0]
                    reptdate = datetime.strptime(reptdate_str, "%Y%m%d")
                    return reptdate.strftime("%d%m%Y")
        except Exception as e:
            logger.error(f"Error reading KAPITI3 date: {e}")
            return ""
        return ""

    def validate_all(self) -> Tuple[bool, Dict[str, str]]:
        """Validate all input file dates"""
        dates = {
            'LOAN': self.get_loan_date(),
            'DEPOSIT': self.get_deposit_date(),
            'FD': self.get_fd_date(),
            'KAPITI1': self.get_kapiti1_date(),
            'KAPITI2': self.get_kapiti2_date(),
            'KAPITI3': self.get_kapiti3_date()
        }

        all_valid = all(date == self.expected_date for date in dates.values())

        return all_valid, dates


class DataProcessor:
    """Process BNM reporting data"""

    def __init__(self, paths: PathConfig, date_calc: DateCalculator):
        self.paths = paths
        self.date_calc = date_calc
        self.macro_vars = date_calc.get_macro_vars()
        self.conn = duckdb.connect(':memory:')

    def substitute_macros(self, text: str) -> str:
        """Replace macro variables in text (e.g., &REPTMON -> 01)"""
        result = text
        for key, value in self.macro_vars.items():
            result = result.replace(f"&{key}", value)
        return result

    def copy_datasets(self, source_lib: Path, dest_lib: Path, datasets: list):
        """Copy datasets from source to destination library"""
        dest_lib.mkdir(parents=True, exist_ok=True)

        for dataset in datasets:
            dataset_name = self.substitute_macros(dataset)
            source_file = source_lib / f"{dataset_name}.parquet"
            dest_file = dest_lib / f"{dataset_name}.parquet"

            if source_file.exists():
                shutil.copy2(source_file, dest_file)
                logger.info(f"Copied {dataset_name} from {source_lib} to {dest_lib}")
            else:
                logger.warning(f"Source file not found: {source_file}")

    def delete_datasets(self, lib: Path, datasets: list):
        """Delete datasets from library"""
        for dataset in datasets:
            dataset_name = self.substitute_macros(dataset)
            file_path = lib / f"{dataset_name}.parquet"

            if file_path.exists():
                file_path.unlink()
                logger.info(f"Deleted {dataset_name} from {lib}")
            else:
                logger.warning(f"File not found for deletion: {file_path}")

    def run_program(self, program_name: str):
        """Execute a program from the PGM library"""
        program_path = self.paths.pgm / f"{program_name}.py"

        if program_path.exists():
            logger.info(f"Executing program: {program_name}")
            try:
                # Execute the program with current environment
                result = subprocess.run(
                    [sys.executable, str(program_path)],
                    env={**os.environ, **self.macro_vars},
                    cwd=str(self.paths.output_base),
                    capture_output=True,
                    text=True
                )

                if result.returncode != 0:
                    logger.error(f"Program {program_name} failed: {result.stderr}")
                    raise RuntimeError(f"Program {program_name} failed")
                else:
                    logger.info(f"Program {program_name} completed successfully")
                    if result.stdout:
                        logger.debug(result.stdout)
            except Exception as e:
                logger.error(f"Error executing {program_name}: {e}")
                raise
        else:
            logger.warning(f"Program not found: {program_path}")

    def process_reports(self):
        """Execute the main processing sequence"""
        logger.info("Starting BNM report processing")

        # Process Domestic Assets and Liabilities reports
        self.run_program('DALMPBBD')
        self.run_program('DALWBP')
        self.run_program('DALMBP')

        # Copy datasets to DEPOBACK
        datasets_to_copy = [
            f'SAVG&REPTMON&NOWK',
            f'CURN&REPTMON&NOWK',
            f'DEPT&REPTMON&NOWK'
        ]
        self.copy_datasets(self.paths.bnm_lib, self.paths.depoback.parent, datasets_to_copy)
        self.delete_datasets(self.paths.bnm_lib, datasets_to_copy)

        # Process Fixed Deposit reports
        self.run_program('FALMPBBD')
        self.run_program('FALWPBBP')
        self.run_program('FALMPBBP')

        # Copy FD datasets
        fd_datasets = ['FDWKLY', 'FDMTHLY']
        self.copy_datasets(self.paths.bnm_lib, self.paths.depoback.parent, fd_datasets)
        self.delete_datasets(self.paths.bnm_lib, fd_datasets)

        # Copy loan datasets from BNM1 to BNM
        loan_datasets = [
            f'LOAN&REPTMON&NOWK',
            f'ULOAN&REPTMON&NOWK'
        ]
        self.copy_datasets(Path(str(self.paths.bnm1_lib)), self.paths.bnm_lib, loan_datasets)

        # Process Loan reports
        self.run_program('LALWPBBP')
        self.run_program('LALMPIBP')

        # Delete loan datasets
        self.delete_datasets(self.paths.bnm_lib, loan_datasets)

        # Process remaining reports
        self.run_program('KALWPIBP')
        self.run_program('KALMPIBP')
        self.run_program('KALMSTOR')
        self.run_program('NALMPIBP')
        self.run_program('EIBRDL1A')
        self.run_program('EIBRDL2A')
        self.run_program('KALMLIIE')
        self.run_program('EIBIRDLA')

        logger.info("BNM report processing completed")

    def cleanup(self):
        """Clean up resources"""
        self.conn.close()


class SFTPFileGenerator:
    """Generate SFTP command file for data transfer"""

    def __init__(self, paths: PathConfig):
        self.paths = paths

    def generate(self):
        """Generate SFTP command file"""
        today = datetime.now()

        with open(self.paths.sftp01, 'w') as f:
            if today.day > 2 or (today.day == 2 and today.hour > 20):
                f.write('PUT //SAP.PIBB.KAPMNI.RDAL      "RDAL KAPMNI EIR.TXT"\n')
                f.write('PUT //SAP.PIBB.NSRS.KAPMNI.RDAL "NSRS KAPMNI EIR.TXT"\n')
            else:
                f.write('PUT //SAP.PIBB.KAPMNI.RDAL      "RDAL KAPMNI.TXT"\n')
                f.write('PUT //SAP.PIBB.NSRS.KAPMNI.RDAL "NSRS KAPMNI.TXT"\n')

        logger.info(f"SFTP command file generated: {self.paths.sftp01}")


def export_to_cport(source_path: Path, dest_path: Path):
    """
    Export dataset to CPORT format (binary transport file)
    This is a simplified version - actual CPORT format would require SAS format implementation
    """
    logger.info(f"Exporting {source_path} to CPORT format at {dest_path}")

    # For now, we'll just copy the binary data
    # In a real implementation, this would need to match SAS PROC CPORT format
    if source_path.exists():
        shutil.copy2(source_path, dest_path)
        logger.info(f"CPORT export completed")
    else:
        logger.warning(f"Source file not found: {source_path}")


def main():
    """Main execution function"""
    logger.info("=" * 80)
    logger.info("EIIMTH1A - BNM Reporting Data Processing")
    logger.info("=" * 80)

    # Initialize configuration
    paths = PathConfig()
    date_calc = DateCalculator()

    logger.info(f"Report Date: {date_calc.rdate}")
    logger.info(f"Report Year: {date_calc.reptyear}")
    logger.info(f"Report Month: {date_calc.reptmon}")
    logger.info(f"Week: {date_calc.nowk}")

    # Validate input files
    validator = InputValidator(paths, date_calc)
    all_valid, dates = validator.validate_all()

    logger.info("Input file date validation:")
    for file_type, date in dates.items():
        status = "✓" if date == date_calc.rdate else "✗"
        logger.info(f"  {status} {file_type}: {date} (expected: {date_calc.rdate})")

    if not all_valid:
        logger.error("THE JOB IS NOT DONE !!")
        for file_type, date in dates.items():
            if date != date_calc.rdate:
                logger.error(f"THE {file_type} EXTRACTION IS NOT DATED {date_calc.rdate}")
        sys.exit(77)

    logger.info("All input files validated successfully")

    # Process data
    processor = DataProcessor(paths, date_calc)

    try:
        processor.process_reports()

        # Generate SFTP command file
        sftp_gen = SFTPFileGenerator(paths)
        sftp_gen.generate()

        # Export to CPORT format for FTP
        export_to_cport(paths.rdaltmp, paths.rdalftp)

        logger.info("=" * 80)
        logger.info("Processing completed successfully")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Processing failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        processor.cleanup()


if __name__ == "__main__":
    main()
