#!/usr/bin/env python3
"""
Program  : EIIMMTH1.py
Purpose  : Monthly BNM RDAL/NSRS reporting orchestrator for Public Islamic Bank Berhad (PIBB).
           Equivalent to the JCL job EIIMMTH1 / SAS step EIBMMTH1.

           Execution flow:
           1.  Parse today's date to derive REPTDATE (last day of previous month),
               NOWK, REPTMON, REPTDAY, RDATE and related macro variables.
           2.  Read REPTDATE from each source dataset (ALW, GAY, LOAN, DEPOSIT, FD,
               KAPITI1/2/3) and compare against RDATE as a data-currency guard.
           3.  If all source dates match RDATE, invoke the full processing chain
               (equivalent to %PROCESS %DO block):
                   - WALWPBBP   : Report on Domestic Assets & Liabilities (Walker items)
                   - EIBRDL1B   : RDAL Part I  (Walker-only)
                   - EIBRDL2B   : RDAL Part II (monthly append)
                   - KALMLIIE   : KAPITI Table 3 FX processing
                   - EIBWRDLB   : Write RDAL Walker output file
                   - PBBRDAL1   : ALW summary Part I
                   - PBBRDAL2   : ALW summary Part II
                   - PBBELP     : Eligible Liabilities computation
                   - PIBBRELP   : Branch Eligible Liabilities report (Islamic)
                   - EIGWRDLI   : Weekly/Monthly RDAL & NSRS file writer (Islamic)
                   - PBBALP     : Assets & Liabilities listing
           4.  If any source date mismatches, log warnings and abort (exit code 77).

           Output files:
               RDAL MTH.TXT  -> SAP.PIBB.FISS.RDAL.MTH  (LRECL=80)
               NSRS MTH.TXT  -> SAP.PIBB.NSRS.RDAL.MTH  (LRECL=80)
               RDALWK        -> SAP.PIBB.WALKER.RDAL     (LRECL=80)
               ELIAB TEXT    -> SAP.PIBB.ELIAB.TEXT      (LRECL=134)

           Commented-out sections (%INC PGM(WALMPBBP), WISDPBBE, WISMPBBE,
           WGAYPBBP, ALMGL/GAYGL cleanup) are preserved as comments below,
           matching the original SAS source.

           ESMR: 2006-1346

Dependencies (all converted; called via subprocess or direct import as noted):
    EIGWRD1W  - Walker ALW extraction  (%INC PGM(EIGWRD1W))
    EIGMRGCW  - Walker GAY extraction  (%INC PGM(EIGMRGCW))
    WALWPBBP  - Domestic A&L report    (%INC PGM(WALWPBBP))
    EIBRDL1B  - RDAL Part I            (%INC PGM(EIBRDL1B))
    EIBRDL2B  - RDAL Part II           (%INC PGM(EIBRDL2B))
    KALMLIIE  - KAPITI Table 3 FX      (%INC PGM(KALMLIIE))
    EIBWRDLB  - RDAL Walker writer     (%INC PGM(EIBWRDLB))
    PBBRDAL1  - ALW summary I          (%INC PGM(PBBRDAL1))
    PBBRDAL2  - ALW summary II         (%INC PGM(PBBRDAL2))
    PBBELP    - Eligible liabilities   (%INC PGM(PBBELP))
    PIBBRELP  - Branch EL report (IB)  (%INC PGM(PIBBRELP))
    EIGWRDLI  - RDAL/NSRS writer (IB)  (%INC PGM(EIGWRDLI))
    PBBALP    - A&L listing            (%INC PGM(PBBALP))
"""

import os
import sys
import subprocess
import logging
from datetime import datetime, date, timedelta
from calendar import monthrange
from pathlib import Path

import duckdb
import polars as pl

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR     = Path(__file__).resolve().parent
DATA_DIR     = BASE_DIR / "data"
INPUT_DIR    = DATA_DIR / "input"
OUTPUT_DIR   = DATA_DIR / "output"

# Library equivalents (SAS LIBNAME / DD statement mappings)
ALW_DIR      = INPUT_DIR  / "alw"        # DD ALW   -> SAP.PIBB.RDAL1
GAY_DIR      = INPUT_DIR  / "gay"        # DD GAY   -> SAP.PBB.RGAC.TXT
LOAN_DIR     = INPUT_DIR  / "loan"       # DD LOAN  -> SAP.PIBB.MNILN(0)
FD_DIR       = INPUT_DIR  / "fd"         # DD FD    -> SAP.PIBB.MNIFD(0)
DEPOSIT_DIR  = INPUT_DIR  / "deposit"    # DD DEPOSIT -> SAP.PIBB.MNITB(0)
BNMTBL1_DIR  = INPUT_DIR  / "bnmtbl1"   # DD BNMTBL1 -> SAP.PIBB.KAPITI1(0)
BNMTBL2_DIR  = INPUT_DIR  / "bnmtbl2"   # DD BNMTBL2 -> SAP.PIBB.KAPITI2(0)
BNMTBL3_DIR  = INPUT_DIR  / "bnmtbl3"   # DD BNMTBL3 -> SAP.PIBB.KAPITI3(0)

RDAL_OUT     = OUTPUT_DIR / "RDAL_MTH.txt"     # DD RDAL   -> SAP.PIBB.FISS.RDAL.MTH
NSRS_OUT     = OUTPUT_DIR / "NSRS_MTH.txt"     # DD NSRS   -> SAP.PIBB.NSRS.RDAL.MTH
RDALWK_OUT   = OUTPUT_DIR / "WALKER_RDAL.txt"  # DD RDALWK -> SAP.PIBB.WALKER.RDAL
ELIAB_OUT    = OUTPUT_DIR / "ELIAB.txt"        # DD ELIAB  -> SAP.PIBB.ELIAB.TEXT (LRECL=134)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("EIIMMTH1")

# ============================================================================
# DATE DERIVATION
# Equivalent to the DATA REPTDATE step in the SAS program.
#
# SAS logic:
#   REPTDATE = INPUT('01'||PUT(MONTH(TODAY()),Z2.)||PUT(YEAR(TODAY()),4.), DDMMYY8.) - 1
#   i.e. first day of current month minus 1 day = last day of previous month.
#
#   WHEN (8)  -> SDD=1,  WK='1', WK1='4'
#   WHEN (15) -> SDD=9,  WK='2', WK1='1'
#   WHEN (22) -> SDD=16, WK='3', WK1='2'
#   OTHERWISE -> SDD=23, WK='4', WK1='3'
#
#   SDATE = MDY(MM, SDD, YEAR(REPTDATE))
#   SDESC = 'PUBLIC ISLAMIC BANK BERHAD'
# ============================================================================

def derive_report_date(today: date | None = None) -> dict:
    """
    Derive all SAS macro variables from today's date.
    Returns a dict with keys matching the CALL SYMPUT names in the SAS source.
    """
    if today is None:
        today = date.today()

    # REPTDATE = first day of current month - 1 = last day of previous month
    first_of_month = date(today.year, today.month, 1)
    reptdate = first_of_month - timedelta(days=1)

    day = reptdate.day
    mm  = reptdate.month

    if day == 8:
        sdd, wk, wk1 = 1, "1", "4"
    elif day == 15:
        sdd, wk, wk1 = 9, "2", "1"
    elif day == 22:
        sdd, wk, wk1 = 16, "3", "2"
    else:
        sdd, wk, wk1 = 23, "4", "3"

    # MM1: if WK='1' then previous month, else same month
    if wk == "1":
        mm1 = mm - 1 if mm > 1 else 12
    else:
        mm1 = mm

    sdate = date(reptdate.year, mm, sdd)

    return {
        "NOWK"     : wk,
        "NOWK1"    : wk1,
        "REPTMON"  : f"{mm:02d}",
        "REPTMON1" : f"{mm1:02d}",
        "REPTYEAR" : str(reptdate.year),
        "REPTDAY"  : f"{reptdate.day:02d}",
        "RDATE"    : reptdate.strftime("%d%m%Y"),       # DDMMYY8. -> DDMMYYYY
        "ELDATE"   : f"{(reptdate - date(1960, 1, 1)).days:05d}",  # SAS Z5. of SAS date
        "SDATE"    : sdate.strftime("%d%m%Y"),
        "SDESC"    : "PUBLIC ISLAMIC BANK BERHAD",
        "reptdate_obj": reptdate,
    }


# ============================================================================
# SOURCE DATE VALIDATION HELPERS
# Equivalent to the DATA _NULL_ / CALL SYMPUT blocks that read REPTDATE from
# each source and to the %IF guards inside %MACRO PROCESS.
# ============================================================================

def _read_reptdate_parquet(parquet_path: Path) -> str | None:
    """
    Read REPTDATE from a parquet file (first row only).
    Returns date formatted as DDMMYYYY, or None if file is missing.
    SAS: DATA _NULL_; SET <lib>.REPTDATE(OBS=1); CALL SYMPUT('X', PUT(REPTDATE, DDMMYY8.));
    """
    if not parquet_path.exists():
        log.warning("Parquet not found: %s", parquet_path)
        return None
    try:
        con = duckdb.connect()
        row = con.execute(
            f"SELECT REPTDATE FROM read_parquet('{parquet_path}') LIMIT 1"
        ).fetchone()
        con.close()
        if row is None:
            return None
        reptdate_val = row[0]
        # Handle integer SAS date (days since 1960-01-01) or Python date/datetime
        if isinstance(reptdate_val, (int, float)):
            d = date(1960, 1, 1) + timedelta(days=int(reptdate_val))
        elif isinstance(reptdate_val, (date, datetime)):
            d = reptdate_val if isinstance(reptdate_val, date) else reptdate_val.date()
        else:
            d = datetime.strptime(str(reptdate_val), "%Y-%m-%d").date()
        return d.strftime("%d%m%Y")
    except Exception as exc:
        log.warning("Could not read REPTDATE from %s: %s", parquet_path, exc)
        return None


def _read_reptdate_text(txt_path: Path) -> str | None:
    """
    Read REPTDATE from first line of a fixed-width text file (YYMMDD8. at position 1).
    SAS: INFILE BNMTBL1 OBS=1; INPUT @1 REPTDATE YYMMDD8.;
    """
    if not txt_path.exists():
        log.warning("Text file not found: %s", txt_path)
        return None
    try:
        with open(txt_path, "r") as fh:
            line = fh.readline()
        raw = line[:8].strip()   # YYMMDD8. -> 8 characters
        d = datetime.strptime(raw, "%Y%m%d").date()
        return d.strftime("%d%m%Y")
    except Exception as exc:
        log.warning("Could not read REPTDATE from %s: %s", txt_path, exc)
        return None


def validate_source_dates(rdate: str) -> bool:
    """
    Check that all mandatory source datasets are dated RDATE.
    Returns True only when every date matches.
    Logs a warning (equivalent to %PUT) for each mismatch.

    SAS %MACRO PROCESS date guard:
        "&ALW"="&RDATE" AND "&GAY"="&RDATE" AND "&LOAN"="&RDATE" AND
        "&DEPOSIT"="&RDATE" AND "&FD"="&RDATE" AND
        "&KAPITI1"="&RDATE" AND "&KAPITI2"="&RDATE" AND "&KAPITI3"="&RDATE"
    """
    checks = {
        "ALW"     : _read_reptdate_parquet(ALW_DIR     / "REPTDATE.parquet"),
        # ALM check commented out in original SAS: /* "&ALM"="&RDATE" AND */
        "GAY"     : _read_reptdate_parquet(GAY_DIR     / "REPTDATE.parquet"),
        "LOAN"    : _read_reptdate_parquet(LOAN_DIR    / "REPTDATE.parquet"),
        "DEPOSIT" : _read_reptdate_parquet(DEPOSIT_DIR / "REPTDATE.parquet"),
        "FD"      : _read_reptdate_parquet(FD_DIR      / "REPTDATE.parquet"),
        "KAPITI1" : _read_reptdate_text(BNMTBL1_DIR / "BNMTBL1.txt"),
        "KAPITI2" : _read_reptdate_text(BNMTBL2_DIR / "BNMTBL2.txt"),
        "KAPITI3" : _read_reptdate_text(BNMTBL3_DIR / "BNMTBL3.txt"),
    }

    all_ok = True
    for label, actual in checks.items():
        if actual != rdate:
            # %IF "&X" NE "&RDATE" %THEN %PUT THE <X> EXTRACTION IS NOT DATED &RDATE;
            log.warning("THE %s EXTRACTION IS NOT DATED %s (found: %s)", label, rdate, actual)
            all_ok = False

    return all_ok


# ============================================================================
# SUB-PROGRAM INVOCATION
# Each %INC PGM(...) in the SAS source is translated to a subprocess call of
# the corresponding converted Python program, passing macro variables via
# environment variables.  This mirrors the SAS global macro variable scope.
# ============================================================================

def _run_program(script_name: str, env: dict) -> int:
    """
    Execute a converted sub-program as a subprocess.
    Returns the process exit code.
    """
    script_path = BASE_DIR / f"{script_name}.py"
    if not script_path.exists():
        log.error("Sub-program not found: %s", script_path)
        return 1

    merged_env = {**os.environ, **{k: str(v) for k, v in env.items()}}
    log.info("Invoking %s ...", script_name)
    result = subprocess.run(
        [sys.executable, str(script_path)],
        env=merged_env,
    )
    if result.returncode != 0:
        log.error("%s exited with code %d", script_name, result.returncode)
    return result.returncode


# ============================================================================
# MAIN ORCHESTRATION  (%MACRO PROCESS)
# ============================================================================

def run_process(params: dict) -> None:
    """
    Full processing chain, executed only when all source dates match RDATE.
    Equivalent to the %IF ... %THEN %DO block inside %MACRO PROCESS.
    """
    env = {
        "NOWK"     : params["NOWK"],
        "NOWK1"    : params["NOWK1"],
        "REPTMON"  : params["REPTMON"],
        "REPTMON1" : params["REPTMON1"],
        "REPTYEAR" : params["REPTYEAR"],
        "REPTDAY"  : params["REPTDAY"],
        "RDATE"    : params["RDATE"],
        "ELDATE"   : params["ELDATE"],
        "SDATE"    : params["SDATE"],
        "SDESC"    : params["SDESC"],
        # Output file paths passed through environment so sub-programs resolve correctly
        "RDAL_OUT"  : str(RDAL_OUT),
        "NSRS_OUT"  : str(NSRS_OUT),
        "RDALWK_OUT": str(RDALWK_OUT),
        "ELIAB_OUT" : str(ELIAB_OUT),
    }

    # ------------------------------------------------------------------
    # REPORT ON DOMESTIC ASSETS AND LIABILITIES
    # ------------------------------------------------------------------

    # %INC PGM(WALWPBBP);
    _run_program("WALWPBBP", env)

    # /* %INC PGM(WALMPBBP); */  -- commented out in original SAS source

    # PROC DATASETS LIB=WORK KILL NOLIST;  -- no equivalent needed in Python

    # /* %INC PGM(WISDPBBE); PROC DATASETS LIB=WORK KILL NOLIST; RUN; */
    # /* %INC PGM(WISMPBBE); PROC DATASETS LIB=WORK KILL NOLIST; RUN; */

    # PROC DATASETS LIB=BNM NOLIST;
    #   DELETE ALWGL&REPTMON&NOWK ALWJE&REPTMON&NOWK;
    #   /* DELETE ALMGL&REPTMON&NOWK; */
    # -- Equivalent: remove intermediate BNM parquet files if they exist.
    bnm_lib = DATA_DIR / "bnm"
    for pattern in [
        f"ALWGL{params['REPTMON']}{params['NOWK']}.parquet",
        f"ALWJE{params['REPTMON']}{params['NOWK']}.parquet",
        # f"ALMGL{params['REPTMON']}{params['NOWK']}.parquet",  # commented out
    ]:
        target = bnm_lib / pattern
        if target.exists():
            target.unlink()
            log.info("Deleted intermediate file: %s", target)

    # %INC PGM(EIBRDL1B);
    _run_program("EIBRDL1B", env)

    # %INC PGM(EIBRDL2B);
    _run_program("EIBRDL2B", env)

    # %INC PGM(KALMLIIE);
    _run_program("KALMLIIE", env)

    # %INC PGM(EIBWRDLB);
    _run_program("EIBWRDLB", env)

    # %INC PGM(PBBRDAL1);
    _run_program("PBBRDAL1", env)

    # %INC PGM(PBBRDAL2);
    _run_program("PBBRDAL2", env)

    # %INC PGM(PBBELP);
    _run_program("PBBELP", env)

    # %INC PGM(PIBBRELP);
    _run_program("PIBBRELP", env)

    # %INC PGM(EIGWRDLI);
    _run_program("EIGWRDLI", env)

    # %INC PGM(PBBALP);
    _run_program("PBBALP", env)

    # ------------------------------------------------------------------
    # REPORT ON GLOBAL ASSETS AND CAPITAL  (commented out in original)
    # REPORT ON CONSOLIDATED ASSETS AND CAPITAL  (commented out in original)
    # ------------------------------------------------------------------
    # /* %INC PGM(WGAYPBBP);
    #    PROC DATASETS LIB=WORK KILL NOLIST; RUN;
    #    PROC DATASETS LIB=BNM NOLIST;
    #      DELETE GAYGL&REPTMON&NOWK GAYJE&REPTMON&NOWK;
    #    RUN; */

    log.info("EIIMMTH1 processing complete.")


# ============================================================================
# PRE-PROCESSING: %INC PGM(EIGWRD1W) and %INC PGM(EIGMRGCW)
# These run unconditionally before the date-validation guard.
# ============================================================================

def run_walker_extraction(env: dict) -> None:
    """
    %INC PGM(EIGWRD1W)  -- Walker ALW extraction
    %INC PGM(EIGMRGCW)  -- Walker GAY extraction
    Both run before the date-validation guard in the SAS source.
    """
    _run_program("EIGWRD1W", env)
    _run_program("EIGMRGCW", env)


# ============================================================================
# ENTRY POINT
# ============================================================================

def main() -> None:
    log.info("=" * 60)
    log.info("EIIMMTH1 started  (PIBB Monthly BNM RDAL/NSRS)")
    log.info("=" * 60)

    # Step 1: Derive reporting-date macro variables
    params = derive_report_date()
    log.info(
        "REPTDATE=%s  NOWK=%s  REPTMON=%s  REPTYEAR=%s  RDATE=%s",
        params["reptdate_obj"].isoformat(),
        params["NOWK"],
        params["REPTMON"],
        params["REPTYEAR"],
        params["RDATE"],
    )

    env = {k: str(v) for k, v in params.items() if k != "reptdate_obj"}

    # Step 2: Walker extractions (unconditional)
    run_walker_extraction(env)

    # Step 3: Source-date validation
    # DATA _NULL_: read REPTDATE from ALW, GAY, LOAN, DEPOSIT, FD, KAPITI1/2/3
    rdate = params["RDATE"]
    dates_ok = validate_source_dates(rdate)

    # Step 4: Conditional processing (%MACRO PROCESS)
    if dates_ok:
        run_process(params)
    else:
        # %ELSE %DO ... %PUT THE JOB IS NOT DONE !! ... ABORT 77; %END;
        log.error("THE JOB IS NOT DONE !!")
        log.error(
            "One or more source datasets are not dated %s. "
            "Processing aborted (equivalent to SAS ABORT 77).",
            rdate,
        )
        sys.exit(77)


if __name__ == "__main__":
    main()
