# !/usr/bin/env python3
"""
Program  : EIIBWEEK
Purpose  : PIBB Weekly BNM Reporting Orchestrator.
           - Validates that all source extractions (ALW, LOAN, DEPOSIT, FD,
             KAPITI1, KAPITI3) are dated to the current REPTDATE.
           - If all dates match, runs the full weekly BNM reporting pipeline:
               WALWPBBP, DALWPBBD, DALWBP, FALWPBBD, FALWPBBP,
               LALWPBBP, KALWPIBP, PBBRDAL1, PBBELP, PIBBRELP, EIIWRDAL.
           - Writes an SFTP command file for pushing RDAL and NSRS outputs
             to the Data Report Repository (DRR) system.
           - If dates do not match, logs mismatches and aborts.

Dependencies:
    EIGWRD1W  - early weekly initialisation step
    WALWPBBP  - Walker ALW processing for PBB
    DALWPBBD  - Deposit ALW processing (PBB, detailed)
    DALWBP    - Deposit ALW processing (BP)
    FALWPBBD  - FD ALW processing (PBB, detailed)
    FALWPBBP  - FD ALW processing (PBB)
    LALWPBBP  - Loan ALW processing (PBB)
    KALWPIBP  - Kapiti ALW processing (PIBB)
    PBBRDAL1  - BNM RDAL1 extraction
    PBBELP    - Eligible liabilities processing (PBB)
    PIBBRELP  - Branch eligible liabilities report (PIBB)
    EIIWRDAL  - Weekly RDAL/NSRS output generation (PIBB)
"""

import duckdb
import polars as pl
from pathlib import Path
import datetime
import sys

# ---------------------------------------------------------------------------
# Dependency imports
# ---------------------------------------------------------------------------
# %INC PGM(EIGWRD1W)
from EIGWRD1W import main as run_eigwrd1w

# %INC PGM(WALWPBBP)
from WALWPBBP import main as run_walwpbbp

# %INC PGM(DALWPBBD)
from DALWPBBD import main as run_dalwpbbd

# %INC PGM(DALWBP)
from DALWBP import main as run_dalwbp

# %INC PGM(FALWPBBD)
from FALWPBBD import main as run_falwpbbd

# %INC PGM(FALWPBBP)
from FALWPBBP import main as run_falwpbbp

# %INC PGM(LALWPBBP)
from LALWPBBP import main as run_lalwpbbp

# %INC PGM(KALWPIBP)
from KALWPIBP import main as run_kalwpibp

# %INC PGM(PBBRDAL1)
from PBBRDAL1 import main as run_pbbrdal1

# %INC PGM(PBBELP)
from PBBELP import main as run_pbbelp

# %INC PGM(PIBBRELP)
from PIBBRELP import main as run_pibbrelp

# %INC PGM(EIIWRDAL)
from EIIWRDAL import main as run_eiiwrdal

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR  = Path("/data/sap")

# Input source reptdate parquets
DEPOSIT_REPTDATE_PATH  = BASE_DIR / "pibb/mnitb/reptdate.parquet"        # SAP.PIBB.MNITB(0)    DEPOSIT.REPTDATE
LOAN_REPTDATE_PATH     = BASE_DIR / "pibb/mniln/reptdate.parquet"        # SAP.PIBB.MNILN(0)    LOAN.REPTDATE
FD_REPTDATE_PATH       = BASE_DIR / "pibb/mnifd/reptdate.parquet"        # SAP.PIBB.MNIFD(0)    FD.REPTDATE
ALW_REPTDATE_PATH      = BASE_DIR / "pibb/rdal1/reptdate.parquet"        # SAP.PIBB.RDAL1       ALW.REPTDATE
BNMTBL1_PATH           = BASE_DIR / "pibb/kapiti1/kapiti1.txt"           # SAP.PIBB.KAPITI1(0)  BNMTBL1 (fixed-width text)
BNMTBL3_PATH           = BASE_DIR / "pibb/kapiti3/kapiti3.txt"           # SAP.PIBB.KAPITI3(0)  BNMTBL3 (fixed-width text)

# BNM library paths
BNM_PATH   = BASE_DIR / "pibb"                                           # SAP.PIBB.D&REPTYEAR  (BNM, resolved at runtime)
BNM1_PATH  = BASE_DIR / "pibb/sasdata"                                   # SAP.PIBB.SASDATA     (BNM1)

# SFTP command output file
SFTP_OUTPUT_PATH = BASE_DIR / "pibb/sftp/ftpput.txt"

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def read_reptdate_parquet(path: Path) -> datetime.date:
    """Read first REPTDATE from a parquet file and return as datetime.date."""
    con = duckdb.connect()
    df  = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{path}') LIMIT 1"
    ).fetchdf()
    con.close()
    val = df['REPTDATE'].iloc[0]
    if hasattr(val, 'date'):
        val = val.date()
    return val


def read_reptdate_fixedwidth(path: Path) -> datetime.date:
    """
    DATA _NULL_: INFILE BNMTBL1/3 OBS=1; INPUT @1 REPTDATE YYMMDD8.;
    Read first line of fixed-width text file, parse columns 1-8 as YYMMDD8.
    YYMMDD8. = YYYYMMDD (8 chars, 4-digit year).
    """
    with open(path, 'r', encoding='utf-8') as f:
        line = f.readline().rstrip('\n')
    date_str = line[0:8]   # @1 for 8 chars
    yyyy = int(date_str[0:4])
    mm   = int(date_str[4:6])
    dd   = int(date_str[6:8])
    return datetime.date(yyyy, mm, dd)


def fmt_ddmmyy8(d: datetime.date) -> str:
    """Format date as DD/MM/YY (DDMMYY8.)."""
    return d.strftime('%d/%m/%y')


def get_week_vars(reptdate: datetime.date) -> dict:
    """
    Derive all week/month/year macro variables from REPTDATE.
    Mirrors the DATA REPTDATE step in EIIBWEEK.
    OPTIONS YEARCUTOFF=1950.
    """
    day  = reptdate.day
    mm   = reptdate.month
    yyyy = reptdate.year

    # SELECT(DAY(REPTDATE))
    if day == 8:
        sdd = 1;  wk = '1'; wk1 = '4'
    elif day == 15:
        sdd = 9;  wk = '2'; wk1 = '1'
    elif day == 22:
        sdd = 16; wk = '3'; wk1 = '2'
    else:
        sdd = 23; wk = '4'; wk1 = '3'

    # MM1: prior month if WK='1'
    if wk == '1':
        mm1 = mm - 1
        if mm1 == 0:
            mm1 = 12
    else:
        mm1 = mm

    sdate = datetime.date(yyyy, mm, sdd)

    # ELDATE = PUT(REPTDATE, Z5.) = SAS internal date integer zero-padded to 5
    sas_date = (reptdate - datetime.date(1960, 1, 1)).days
    eldate   = f"{sas_date:05d}"

    sdesc = 'PUBLIC ISLAMIC BANK BERHAD'

    return {
        'NOWK':     wk,
        'NOWK1':    wk1,
        'REPTMON':  f"{mm:02d}",
        'REPTMON1': f"{mm1:02d}",
        'REPTYEAR': str(yyyy),
        'REPTDAY':  f"{day:02d}",
        'RDATE':    fmt_ddmmyy8(reptdate),
        'ELDATE':   eldate,
        'SDATE':    fmt_ddmmyy8(sdate),
        'SDESC':    f"{sdesc:<26}",
    }


# ============================================================================
# MAIN
# ============================================================================

def main():
    # -----------------------------------------------------------------------
    # %INC PGM(EIGWRD1W) - early weekly initialisation
    # -----------------------------------------------------------------------
    run_eigwrd1w()

    # -----------------------------------------------------------------------
    # DATA REPTDATE: SET DEPOSIT.REPTDATE
    # Derive all macro variables
    # -----------------------------------------------------------------------
    deposit_reptdate = read_reptdate_parquet(DEPOSIT_REPTDATE_PATH)
    dvars            = get_week_vars(deposit_reptdate)

    nowk     = dvars['NOWK']
    reptmon  = dvars['REPTMON']
    reptyear = dvars['REPTYEAR']
    reptday  = dvars['REPTDAY']
    rdate    = dvars['RDATE']

    # LIBNAME BNM  "SAP.PIBB.D&REPTYEAR"
    # LIBNAME BNM1 "SAP.PIBB.SASDATA"
    bnm_year_path = BASE_DIR / f"pibb/d{reptyear}"
    bnm1_path     = BNM1_PATH

    # -----------------------------------------------------------------------
    # Read all source REPTDATE values for validation
    # DATA _NULL_: SET ALW.REPTDATE(OBS=1)
    # DATA _NULL_: SET LOAN.REPTDATE(OBS=1)
    # DATA _NULL_: SET DEPOSIT.REPTDATE(OBS=1)
    # DATA _NULL_: SET FD.REPTDATE(OBS=1)
    # DATA _NULL_: INFILE BNMTBL1 OBS=1; INPUT @1 REPTDATE YYMMDD8.
    # DATA _NULL_: INFILE BNMTBL3 OBS=1; INPUT @1 REPTDATE YYMMDD8.
    # -----------------------------------------------------------------------
    alw_date     = read_reptdate_parquet(ALW_REPTDATE_PATH)
    loan_date    = read_reptdate_parquet(LOAN_REPTDATE_PATH)
    deposit_date = deposit_reptdate
    fd_date      = read_reptdate_parquet(FD_REPTDATE_PATH)
    kapiti1_date = read_reptdate_fixedwidth(BNMTBL1_PATH)
    kapiti3_date = read_reptdate_fixedwidth(BNMTBL3_PATH)

    alw_str     = fmt_ddmmyy8(alw_date)
    loan_str    = fmt_ddmmyy8(loan_date)
    deposit_str = fmt_ddmmyy8(deposit_date)
    fd_str      = fmt_ddmmyy8(fd_date)
    kapiti1_str = fmt_ddmmyy8(kapiti1_date)
    kapiti3_str = fmt_ddmmyy8(kapiti3_date)

    # -----------------------------------------------------------------------
    # %MACRO PROCESS: validate all dates match RDATE
    # -----------------------------------------------------------------------
    all_match = (
        alw_str     == rdate and
        loan_str    == rdate and
        fd_str      == rdate and
        deposit_str == rdate and
        kapiti1_str == rdate and
        kapiti3_str == rdate
    )

    if all_match:
        # -------------------------------------------------------------------
        # All dates match - run full pipeline
        # -------------------------------------------------------------------

        # /*** REPORT ON DOMESTIC ASSETS AND LIABILITIES ***/
        # %INC PGM(WALWPBBP)
        run_walwpbbp()

        # PROC DATASETS LIB=WORK KILL NOLIST (implicit in Python)
        # PROC DATASETS LIB=BNM NOLIST; DELETE ALWGL&REPTMON&NOWK ALWJE&REPTMON&NOWK
        for fname in (
            bnm_year_path / f"alwgl{reptmon}{nowk}.parquet",
            bnm_year_path / f"alwje{reptmon}{nowk}.parquet",
        ):
            if fname.exists():
                fname.unlink()

        # %INC PGM(DALWPBBD)
        run_dalwpbbd()

        # %INC PGM(DALWBP)
        run_dalwbp()

        # PROC DATASETS LIB=WORK KILL NOLIST
        # PROC DATASETS LIB=BNM NOLIST; DELETE SAVG CURN DEPT
        for fname in (
            bnm_year_path / f"savg{reptmon}{nowk}.parquet",
            bnm_year_path / f"curn{reptmon}{nowk}.parquet",
            bnm_year_path / f"dept{reptmon}{nowk}.parquet",
        ):
            if fname.exists():
                fname.unlink()

        # %INC PGM(FALWPBBD)
        run_falwpbbd()

        # %INC PGM(FALWPBBP)
        run_falwpbbp()

        # PROC DATASETS LIB=WORK KILL NOLIST
        # PROC DATASETS LIB=BNM NOLIST; DELETE FDWKLY
        fdwkly_path = bnm_year_path / "fdwkly.parquet"
        if fdwkly_path.exists():
            fdwkly_path.unlink()

        # PROC COPY IN=BNM1 OUT=BNM; SELECT LOAN&REPTMON&NOWK ULOAN&REPTMON&NOWK
        # Copy LOAN and ULOAN from BNM1 (PIBB.SASDATA) to BNM (PIBB.D&REPTYEAR)
        bnm_year_path.mkdir(parents=True, exist_ok=True)
        for tbl in (f"loan{reptmon}{nowk}", f"uloan{reptmon}{nowk}"):
            src = bnm1_path  / f"{tbl}.parquet"
            dst = bnm_year_path / f"{tbl}.parquet"
            if src.exists():
                import shutil
                shutil.copy2(src, dst)

        # %INC PGM(LALWPBBP)
        run_lalwpbbp()

        # PROC DATASETS LIB=WORK KILL NOLIST
        # PROC DATASETS LIB=BNM NOLIST; DELETE LOAN&REPTMON&NOWK ULOAN&REPTMON&NOWK
        for fname in (
            bnm_year_path / f"loan{reptmon}{nowk}.parquet",
            bnm_year_path / f"uloan{reptmon}{nowk}.parquet",
        ):
            if fname.exists():
                fname.unlink()

        # %INC PGM(KALWPIBP)
        run_kalwpibp()

        # %INC PGM(PBBRDAL1)
        run_pbbrdal1()

        # %INC PGM(PBBELP)
        run_pbbelp()

        # %INC PGM(PIBBRELP)
        run_pibbrelp()

        # %INC PGM(EIIWRDAL)
        run_eiiwrdal()

    else:
        # -------------------------------------------------------------------
        # Date mismatch - log warnings and abort (ABORT 77 equivalent)
        # -------------------------------------------------------------------
        if alw_str != rdate:
            print(f"THE RDAL1 EXTRACTION IS NOT DATED {rdate}")
        if loan_str != rdate:
            print(f"THE LOAN EXTRACTION IS NOT DATED {rdate}")
        if deposit_str != rdate:
            print(f"THE DEPOSIT EXTRACTION IS NOT DATED {rdate}")
        if fd_str != rdate:
            print(f"THE FD EXTRACTION IS NOT DATED {rdate}")
        if kapiti1_str != rdate:
            print(f"THE KAPITI1 EXTRACTION IS NOT DATED {rdate}")
        print("THE JOB IS NOT DONE !!")
        sys.exit(77)

    # -----------------------------------------------------------------------
    # DATA _NULL_: FILE SFTP01
    # Write SFTP PUT commands for RDAL and NSRS outputs
    # PUT @1 "PUT //SAP.PIBB.FISS.RDAL   ""RDAL WEEK&NOWK..TXT"""
    #     /  "PUT //SAP.PIBB.NSRS.RDAL   ""NSRS WEEK&NOWK..TXT"""
    # -----------------------------------------------------------------------
    SFTP_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SFTP_OUTPUT_PATH, 'w', encoding='utf-8', newline='\n') as f:
        f.write(f'PUT //SAP.PIBB.FISS.RDAL   "RDAL WEEK{nowk}.TXT"\n')
        f.write(f'PUT //SAP.PIBB.NSRS.RDAL   "NSRS WEEK{nowk}.TXT"\n')

    print(f"SFTP commands written to: {SFTP_OUTPUT_PATH}")

    # ******************************************************************
    # * FTP HOST DATASETS TO DATA REPORT REPOSITORY SYSTEM (DRR)
    # ******************************************************************
    # Note: Actual FTP/SFTP transfer to DRR is handled externally.
    # The SFTP command file above (SFTP_OUTPUT_PATH) should be passed
    # to the SFTP client pointing to DRR server, targeting:
    #   "FD-BNM REPORTING/PIBB/BNM RPTG"


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    main()
