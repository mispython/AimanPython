#!/usr/bin/env python3
"""
Program  : EIBDWKLX.py
Purpose  : Orchestrator — run after EIBDLNEX (ESMR: 06-1428).
           Reads REPTDATE from LOAN.REPTDATE, validates loan extraction date,
           copies SME08, then invokes LALWPBBC for loan note processing.

           Run condition: LOAN.REPTDATE must equal BNM.REPTDATE (same report date).
           Aborts with exit code 77 if dates do not match.

           Dependencies:
             LALWPBBC  - Loan note manipulation program
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import sys
import logging
from datetime import date
from pathlib import Path

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
import polars as pl

# ============================================================================
# DEPENDENCY IMPORTS
# ============================================================================
from LALWPBBC import main as lalwpbbc_main

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR  = Path(".")
DATA_DIR  = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input parquet paths
LOAN_REPTDATE_PATH = DATA_DIR / "loan"  / "reptdate.parquet"   # LOAN.REPTDATE
BNM_REPTDATE_PATH  = DATA_DIR / "bnm"   / "reptdate.parquet"   # BNM.REPTDATE (output)
BNM1_SME08_PATH    = DATA_DIR / "bnm1"  / "sme08.parquet"      # BNM1.SME08
BNM_SME08_PATH     = DATA_DIR / "bnm"   / "sme08.parquet"      # BNM.SME08 (copy target)

# ============================================================================
# LOGGING
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ============================================================================
# DATA BNM.REPTDATE — derive macro variables from LOAN.REPTDATE
# ============================================================================
def load_reptdate() -> tuple[date, str, str, str, str, str, int]:
    """
    Read LOAN.REPTDATE and derive all macro variables:
      NOWK, REPTMON, REPTYEAR, RDATE, SDATE, TDATE (SAS integer date).
    Also writes BNM.REPTDATE parquet.
    Returns (reptdate, nowk, reptmon, reptyear, rdate, sdate, tdate).
    """
    df = pl.read_parquet(LOAN_REPTDATE_PATH)
    reptdate: date = df["REPTDATE"][0]

    wk       = reptdate.day
    mm       = reptdate.month
    nowk     = f"{wk:02d}"
    reptmon  = f"{mm:02d}"
    reptyear = str(reptdate.year)
    rdate    = reptdate.strftime("%d/%m/%Y")
    sdate    = rdate
    # SAS date integer: days since 01-JAN-1960
    tdate    = (reptdate - date(1960, 1, 1)).days
    tdate_z5 = f"{tdate:05d}"

    log.info("REPTDATE=%s  NOWK=%s  REPTMON=%s  REPTYEAR=%s  RDATE=%s",
             reptdate, nowk, reptmon, reptyear, rdate)

    # Write BNM.REPTDATE
    df.write_parquet(BNM_REPTDATE_PATH)
    log.info("BNM.REPTDATE written: %s", BNM_REPTDATE_PATH)

    return reptdate, nowk, reptmon, reptyear, rdate, sdate, tdate


# ============================================================================
# DATA _NULL_ — read LOAN.REPTDATE to derive &LOAN macro variable
# ============================================================================
def load_loan_date() -> str:
    """
    Read LOAN.REPTDATE to get the loan extraction date formatted as DDMMYY8.
    Mirrors: DATA _NULL_; SET LOAN.REPTDATE; CALL SYMPUT('LOAN', PUT(REPTDATE,DDMMYY8.));
    Returns formatted loan date string (DD/MM/YYYY).
    """
    df = pl.read_parquet(LOAN_REPTDATE_PATH)
    loan_date: date = df["REPTDATE"][0]
    return loan_date.strftime("%d/%m/%Y")


# ============================================================================
# %MACRO PROCESS — validate dates and invoke LALWPBBC
# ============================================================================
def process(loan: str, rdate: str, reptdate: date,
            nowk: str, reptmon: str, tdate: int, sdate: str) -> None:
    """
    Mirrors %MACRO PROCESS:
      If LOAN date == RDATE:
        - Copy BNM1.SME08 → BNM.SME08
        - Invoke LALWPBBC
        - Clean up work datasets (no-op in Python)
      Else:
        - Log mismatch and abort with exit code 77.
    """
    if loan == rdate:
        log.info("Loan extraction date matches report date (%s). Proceeding.", rdate)

        # DATA BNM.SME08; SET BNM1.SME08; RUN;
        sme08 = pl.read_parquet(BNM1_SME08_PATH)
        sme08.write_parquet(BNM_SME08_PATH)
        log.info("BNM.SME08 copied from BNM1.SME08.")

        # %INC PGM(LALWPBBC)
        lalwpbbc_main(
            reptmon=reptmon,
            nowk=nowk,
            reptdate=reptdate,
            tdate=tdate,
            rdate=rdate,
            sdate=sdate,
        )

        # PROC DATASETS LIB=WORK KILL NOLIST — work cleanup (no-op in Python)
        log.info("Work dataset cleanup skipped (in-memory processing).")
    else:
        if loan != rdate:
            log.warning("THE LOAN EXTRACTION IS NOT DATED %s", rdate)
        log.error("THE JOB IS NOT DONE !!")
        # DATA A; ABORT 77; — abort with return code 77
        sys.exit(77)


# ============================================================================
# MAIN
# ============================================================================
def main() -> None:
    log.info("EIBDWKLX started.")

    # ----------------------------------------------------------------
    # DATA BNM.REPTDATE — load and derive macro variables
    # ----------------------------------------------------------------
    reptdate, nowk, reptmon, reptyear, rdate, sdate, tdate = load_reptdate()

    # ----------------------------------------------------------------
    # DATA _NULL_ — load LOAN extraction date
    # ----------------------------------------------------------------
    loan = load_loan_date()
    log.info("LOAN extraction date: %s  |  RDATE: %s", loan, rdate)

    # ----------------------------------------------------------------
    # %PROCESS — validate and run
    # ----------------------------------------------------------------
    process(loan, rdate, reptdate, nowk, reptmon, tdate, sdate)

    log.info("EIBDWKLX completed successfully.")


if __name__ == "__main__":
    main()
