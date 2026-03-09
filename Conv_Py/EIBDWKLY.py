#!/usr/bin/env python3
"""
Program  : EIBDWKLY.py
Purpose  : Orchestrator — run after EIBDLNEX (ESMR: 06-1428).
           Reads REPTDATE from LOAN.REPTDATE and DEPOSIT.REPTDATE,
           validates both extraction dates against the report date,
           then invokes LALWPBBD and LALWEIRC sequentially.

           Run condition: both LOAN.REPTDATE and DEPOSIT.REPTDATE must equal
           BNM.REPTDATE (same report date).
           Aborts with exit code 77 if either date does not match.

           Commented-out steps preserved from original JCL:
             %INC PGM(LALBDBAL)  — disabled SMR2016-1430
             %INC PGM(LALWPBBU)  — disabled (no SMR reference)

           Dependencies:
             LALWPBBD  - Loan + OD manipulation
             LALWEIRC  - EIR adjustment merge + write-down
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
from LALWPBBD import main as lalwpbbd_main
from LALWEIRC import main as lalweirc_main
# from LALBDBAL import main as lalbdbal_main  # SMR2016-1430 — disabled
# from LALWPBBU import main as lalwpbbu_main  # disabled

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR   = Path(".")
DATA_DIR   = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input parquet paths
LOAN_REPTDATE_PATH    = DATA_DIR / "loan"    / "reptdate.parquet"   # LOAN.REPTDATE
DEPOSIT_REPTDATE_PATH = DATA_DIR / "deposit" / "reptdate.parquet"   # DEPOSIT.REPTDATE
BNM_REPTDATE_PATH     = DATA_DIR / "bnm"     / "reptdate.parquet"   # BNM.REPTDATE (output)

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
def load_reptdate() -> tuple[date, str, str, str, str, str, str, int]:
    """
    Read LOAN.REPTDATE and derive all macro variables:
      NOWK, REPTMON, REPTYEAR, REPTYR, RDATE, SDATE, TDATE (SAS integer date).
    Also writes BNM.REPTDATE parquet.
    Returns (reptdate, nowk, reptmon, reptyear, reptyr, rdate, sdate, tdate).

    Mirrors:
      DATA BNM.REPTDATE; SET LOAN.REPTDATE;
        WK=DAY(REPTDATE); MM=MONTH(REPTDATE);
        CALL SYMPUT('NOWK',PUT(WK,Z2.));
        CALL SYMPUT('REPTMON',PUT(MM,Z2.));
        CALL SYMPUT('REPTYEAR',PUT(REPTDATE,YEAR4.));
        CALL SYMPUT('REPTYR',PUT(REPTDATE,YEAR2.));
        CALL SYMPUT('RDATE',PUT(REPTDATE,DDMMYY8.));
        CALL SYMPUT('SDATE',PUT(REPTDATE,DDMMYY8.));
        CALL SYMPUT('TDATE',PUT(REPTDATE,Z5.));
    """
    df       = pl.read_parquet(LOAN_REPTDATE_PATH)
    reptdate: date = df["REPTDATE"][0]

    wk       = reptdate.day
    mm       = reptdate.month
    nowk     = f"{wk:02d}"
    reptmon  = f"{mm:02d}"
    reptyear = str(reptdate.year)
    reptyr   = reptdate.strftime("%y")         # two-digit year — YEAR2. format
    rdate    = reptdate.strftime("%d/%m/%Y")   # DDMMYY8. — DD/MM/YYYY
    sdate    = rdate
    # SAS date integer: days since 01-JAN-1960 — PUT(REPTDATE,Z5.)
    tdate    = (reptdate - date(1960, 1, 1)).days

    log.info(
        "REPTDATE=%s  NOWK=%s  REPTMON=%s  REPTYEAR=%s  REPTYR=%s  RDATE=%s",
        reptdate, nowk, reptmon, reptyear, reptyr, rdate,
    )

    # Write BNM.REPTDATE
    df.write_parquet(BNM_REPTDATE_PATH)
    log.info("BNM.REPTDATE written: %s", BNM_REPTDATE_PATH)

    return reptdate, nowk, reptmon, reptyear, reptyr, rdate, sdate, tdate


# ============================================================================
# DATA _NULL_ — read LOAN.REPTDATE to derive &LOAN macro variable
# ============================================================================
def load_loan_date() -> str:
    """
    Read LOAN.REPTDATE to get the loan extraction date formatted as DDMMYY8.
    Mirrors:
      DATA _NULL_; SET LOAN.REPTDATE;
        CALL SYMPUT('LOAN', PUT(REPTDATE,DDMMYY8.));
    """
    df        = pl.read_parquet(LOAN_REPTDATE_PATH)
    loan_date: date = df["REPTDATE"][0]
    return loan_date.strftime("%d/%m/%Y")


# ============================================================================
# DATA _NULL_ — read DEPOSIT.REPTDATE to derive &DEPOSIT macro variable
# ============================================================================
def load_deposit_date() -> str:
    """
    Read DEPOSIT.REPTDATE to get the deposit extraction date formatted as DDMMYY8.
    Mirrors:
      DATA _NULL_; SET DEPOSIT.REPTDATE;
        CALL SYMPUT('DEPOSIT', PUT(REPTDATE,DDMMYY8.));
    """
    df           = pl.read_parquet(DEPOSIT_REPTDATE_PATH)
    deposit_date: date = df["REPTDATE"][0]
    return deposit_date.strftime("%d/%m/%Y")


# ============================================================================
# %MACRO PROCESS — validate dates and invoke downstream programs
# ============================================================================
def process(loan: str, deposit: str, rdate: str,
            reptdate: date, nowk: str, reptmon: str,
            reptyr: str, tdate: int, sdate: str) -> None:
    """
    Mirrors %MACRO PROCESS:
      If LOAN == RDATE AND DEPOSIT == RDATE:
        - Invoke LALWPBBD
        - Invoke LALWEIRC
        - (LALBDBAL disabled — SMR2016-1430)
        - (LALWPBBU disabled)
        - Clean up work datasets (no-op in Python)
      Else:
        - Log mismatch message(s) and abort with exit code 77.
    """
    if loan == rdate and deposit == rdate:
        log.info(
            "Extraction dates match report date (%s). Proceeding.", rdate
        )

        # %INC PGM(LALWPBBD)
        lalwpbbd_main(
            reptmon=reptmon,
            nowk=nowk,
            reptdate=reptdate,
            sdate=sdate,
        )

        # %INC PGM(LALWEIRC)
        lalweirc_main(
            reptmon=reptmon,
            nowk=nowk,
            reptyr=reptyr,
        )

        # /* %INC PGM(LALBDBAL); SMR2016-1430 */
        # lalbdbal_main(reptmon=reptmon, nowk=nowk)

        # /* %INC PGM(LALWPBBU); */
        # lalwpbbu_main(reptmon=reptmon, nowk=nowk, tdate=tdate)

        # PROC DATASETS LIB=WORK KILL NOLIST — work cleanup (no-op in Python)
        log.info("Work dataset cleanup skipped (in-memory processing).")

    else:
        if loan != rdate:
            log.warning("THE LOAN EXTRACTION IS NOT DATED %s", rdate)
        if deposit != rdate:
            log.warning("THE DEPOSIT EXTRACTION IS NOT DATED %s", rdate)
        log.error("THE JOB IS NOT DONE !!")
        # DATA A; ABORT 77; — abort with return code 77
        sys.exit(77)


# ============================================================================
# MAIN
# ============================================================================
def main() -> None:
    log.info("EIBDWKLY started.")

    # ----------------------------------------------------------------
    # DATA BNM.REPTDATE — load and derive macro variables
    # ----------------------------------------------------------------
    reptdate, nowk, reptmon, reptyear, reptyr, rdate, sdate, tdate = load_reptdate()

    # ----------------------------------------------------------------
    # DATA _NULL_ — load LOAN extraction date
    # ----------------------------------------------------------------
    loan    = load_loan_date()
    log.info("LOAN extraction date    : %s  |  RDATE: %s", loan, rdate)

    # ----------------------------------------------------------------
    # DATA _NULL_ — load DEPOSIT extraction date
    # ----------------------------------------------------------------
    deposit = load_deposit_date()
    log.info("DEPOSIT extraction date : %s  |  RDATE: %s", deposit, rdate)

    # ----------------------------------------------------------------
    # %PROCESS — validate dates and run downstream programs
    # ----------------------------------------------------------------
    process(loan, deposit, rdate, reptdate, nowk, reptmon, reptyr, tdate, sdate)

    log.info("EIBDWKLY completed successfully.")


if __name__ == "__main__":
    main()
