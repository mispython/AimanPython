# !/usr/bin/env python3
"""
Program : EIBMRPT1
Purpose : Orchestrator equivalent of JCL job EIBMRPT1.
          Executes all active MIS/BNM reporting steps in the same sequence
            as the original JCL, managing pre-execution dataset deletion,
            sequential step invocation, condition-code checking (COND=(4,LT))
            and job-level logging.

JOB     : EIBMRPT1 JOB MISEIS,EIBMRPT1,COND=(4,LT),CLASS=A,MSGCLASS=X

Active steps (in order):
  DELETE     - Pre-delete SAP.PBB.PDRRPT.COLD
  EIBMBPOS   - Branch Deposit Position (COLD/RPTS download)
  EIBMRM01   - Deposits by Time-to-Maturity for ALCO           -> EIBMRM1X.py
  EIBMRM02   - FD Ind/Non-Ind by Time-to-Maturity Part 1       -> EIBMRM2X.py
  EIBMRM03   - FD Ind/Non-Ind by Time-to-Maturity Part 2       -> EIBMRM3X.py
  EIBMRM04   - Loans & Advances by Time-to-Maturity for ALCO   -> EIBMRM4X.py
  DELETE     - Pre-delete OPCL.SAVG / OPCL.CURR / OPCL.FD / EIMALIMT.TEXT
  EIBMDPSA   - Savings accounts opened/closed                  -> EIBMDPSA.py
  EIBMDPCA   - Current accounts opened/closed                  -> EIBMDPCA.py
  EIBMDPFQ   - Fixed Deposit accounts opened/closed            -> EIBMDPFQ.py
  EIBMCLAS   - Monthly MORE PLAN loans report                  -> EIBMCLAS.py
  EIBMISL2   - Islamic products extraction (204, 215)           -> EIBMISL2.py
  EIMALIMT   - Gross loan approved limit                        -> EIMALIMT.py
  DELETE     - Pre-delete SAP.PBB.EIBMRM4X.SUMMR
  EIBMRM4X   - Repricing gap (loans by time-to-maturity ALCO)  -> EIBMRM4X.py

Commented-out steps (preserved, not executed):
  EIBMIWAR - Weighted avg rate Islamic products (discontinued 06/07/2004)
  EIBMEXCP - Exceptional reports on deposit/loan extractions
  EIBMNRCC - BNM EALIS2 NRCC report (CUSTCODE 63, 68)
  EIBMRATE - Average rate all FD products monthly
  EIBMBEXT - Balances of external account by branch
  EIMHPMAX - Max/min lending rate HP(D) (removed ESMR 2007-329)
  EIBM619L - Number of loan accounts (discontinued EMSR 2018-1682)
  EIBM619D - Number of deposit accounts (discontinued EMSR 2018-1682)
  EIBMFC01 - FCY FD time-to-maturity by remaining maturity (SMR 2005-1085)
  EIBMFC02 - FCY FD time-to-maturity by original maturity
  EIBMFC03 - FCY FD time-to-maturity by original maturity (alt)
  EIBMRM1X - Deposits by time-to-maturity ALCO softcopy
  EIBMRM2X - FD Ind/Non-Ind ALCO softcopy
  EIBMRM3X - FD Ind/Non-Ind ALCO softcopy
  RUNSFTP  - FTP datasets to PBB Datawarehouse / DRR system
"""

import os
import sys
import shutil
import logging
import subprocess
from datetime import datetime
from pathlib import Path

# =============================================================================
# PATH CONFIGURATION
# Mirrors JCL DD DSN allocations mapped to Linux filesystem equivalents.
# =============================================================================

BASE_DIR = r"/data"

# Directory where all converted Python program scripts reside
# (equivalent to SAP.BNM.PROGRAM PDS)
PGM_DIR = os.path.join(BASE_DIR, "programs")

# Input data directories
# SAP.PBB.MNITB(0)  / SAP.PBB.MNITB.MTH
DEPOSIT_DIR   = os.path.join(BASE_DIR, "deposit")
# SAP.PBB.MNIFD(0)  / SAP.PBB.MNIFD.MTH
FD_DIR        = os.path.join(BASE_DIR, "fd")
# SAP.PBB.MNILN(0)
LOAN_DIR      = os.path.join(BASE_DIR, "loan")
# SAP.PBB.SASDATA
SASDATA_DIR   = os.path.join(BASE_DIR, "sasdata")
# SAP.PBB.MNILIMT(0)
OD_DIR        = os.path.join(BASE_DIR, "od")
# SAP.PBB.CCRIS2.PROVISIO
PROV_DIR      = os.path.join(BASE_DIR, "prov")
# RBP2.B033.PBB.BRANCH
BRHFILE_DIR   = os.path.join(BASE_DIR, "brhfile")
# SAP.PBB.KAPITI5(0)
KAPITI5_DIR   = os.path.join(BASE_DIR, "kapiti5")
# SAP.PBB.OPCL.SASDATA
MIS_DIR       = os.path.join(BASE_DIR, "mis")

# Output directories / dataset equivalents
# SAP.PBB.PDRRPT.COLD  (EIBMBPOS SASLIST: RECFM=FBA, LRECL=133)
COLD_DIR      = os.path.join(BASE_DIR, "cold")
PDRRPT_COLD   = os.path.join(COLD_DIR,  "PDRRPT.COLD.txt")

# SAP.PBB.OPCL.SAVG    (EIBMDPSA SASLIST: RECFM=FB, LRECL=133)
OPCL_DIR      = os.path.join(BASE_DIR, "opcl")
OPCL_SAVG     = os.path.join(OPCL_DIR,  "OPCL.SAVG.txt")
# SAP.PBB.OPCL.CURR    (EIBMDPCA SASLIST: RECFM=FB, LRECL=133)
OPCL_CURR     = os.path.join(OPCL_DIR,  "OPCL.CURR.txt")
# SAP.PBB.OPCL.FD      (EIBMDPFQ SASLIST: RECFM=FB, LRECL=133)
OPCL_FD       = os.path.join(OPCL_DIR,  "OPCL.FD.txt")

# SAP.PBB.EIMALIMT.TEXT (EIMALIMT SASLIST: RECFM=FB, LRECL=133)
OUTPUT_DIR    = os.path.join(BASE_DIR, "output")
EIMALIMT_TEXT = os.path.join(OUTPUT_DIR, "EIMALIMT.txt")

# SAP.PBB.EIBMRM4X.SUMMR (EIBMRM4X RM4XSMMR: RECFM=FS, LRECL=27648)
RM4XSMMR_DIR  = os.path.join(BASE_DIR, "rm4xsmmr")

# Log directory
LOG_DIR = os.path.join(BASE_DIR, "logs")

# =============================================================================
# LOGGING
# Equivalent to MSGCLASS=X (system log) and //SASLOG DD SYSOUT=X on each step.
# =============================================================================

os.makedirs(LOG_DIR, exist_ok=True)
_ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = os.path.join(LOG_DIR, f"EIBMRPT1_{_ts}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("EIBMRPT1")

# =============================================================================
# JOB PARAMETERS
# COND=(4,LT) -> abort remaining steps if any step returns RC > 4.
# =============================================================================

JOB_NAME = "EIBMRPT1"
MAX_RC   = 4   # steps are skipped when highest_rc > MAX_RC

# =============================================================================
# OUTPUT ROUTING METADATA
# Equivalent to JCL //PRINTn OUTPUT CLASS=R, NAME=..., DEST=...
# In Python these are metadata records only; actual routing is performed by
# each report program writing to its designated output path.
# =============================================================================

# //PRINT1
PRINT1 = {
    "name":     "THE MANAGER",
    "room":     "26TH FLOOR",
    "building": "MENARA PBB",
    "dept":     "STATISTICS",
    "address":  ["FINANCE DIVISION", "MENARA PUBLIC BANK",
                 "146 JALAN AMPANG", "50450 KUALA LUMPUR"],
    "dest":     "S1.RMT5",
}
# //PRINT2
PRINT2 = {
    "name":     "MS YEE CHUI ENG",
    "room":     "26TH FLOOR",
    "building": "MENARA PBB",
    "dept":     "STATISTICS",
    "address":  ["FINANCE DIVISION", "MENARA PUBLIC BANK",
                 "146 JALAN AMPANG", "50450 KUALA LUMPUR"],
    "dest":     "S1.RMT5",
}
# //PRINT4
PRINT4 = {
    "name":     "THE MANAGER",
    "room":     "26TH FLOOR",
    "building": "MENARA PBB",
    "dept":     "FIN & TREA OPERATION",
    "address":  ["FINANCE DIVISION", "MENARA PUBLIC BANK",
                 "146 JALAN AMPANG", "50450 KUALA LUMPUR"],
    "dest":     "S1.RMT5",
}
# //PRINT5
PRINT5 = {
    "name":     "MR TIEW SWEE SENG",
    "room":     "28TH FLOOR",
    "building": "MENARA PBB",
    "dept":     "ALM DEPT",
    "address":  ["TREASURY DIVISION", "MENARA PUBLIC BANK",
                 "146 JALAN AMPANG", "50450 KUALA LUMPUR"],
    "dest":     "S1.RMT5",
}
# //PRINT6
PRINT6 = {
    "name":     "ATTN: CIK NORAEAN ADAM",
    "room":     "26TH FLOOR",
    "building": "MENARA PBB",
    "dept":     "SYSTEMS AND BNM REPORTING",
    "address":  ["FINANCE DIVISION", "MENARA PUBLIC BANK",
                 "146 JALAN AMPANG", "50450 KUALA LUMPUR"],
    "dest":     "S1.RMT5",
}
# //*
# //PRINT9
PRINT9 = {
    "name":     "MISS KELLY CHOY",
    "room":     "26TH FLOOR",
    "building": "MENARA PBB",
    "dept":     "FINANCE DIVISION",
    "address":  ["FINANCIAL ACCOUNTING", "MENARA PUBLIC BANK",
                 "146 JALAN AMPANG", "50450 KUALA LUMPUR"],
    "dest":     "S1.RMT5",
}
# //PRINT10
PRINT10 = {
    "name":     "EN NOR HISHAM NORDIN",
    "room":     "26TH FLOOR",
    "building": "MENARA PB",
    "dept":     "FINANCE DIVISION",
    "address":  ["MENARA PUBLIC BANK",
                 "146 JALAN AMPANG", "50450 KUALA LUMPUR"],
    "dest":     "S1.RMT5",
}
# //PRINT11
PRINT11 = {
    "name":     "MS CHIN SIM YEE",
    "room":     "26TH FLOOR",
    "building": "MENARA PB",
    "dept":     "MANAGEMENT ACCOUNTING",
    "address":  ["MENARA PUBLIC BANK",
                 "146 JALAN AMPANG", "50450 KUALA LUMPUR"],
    "dest":     "S1.RMT5",
}
# //PRINT13
PRINT13 = {
    "name":     "MR WONG WEE HONG",
    "room":     "26TH FLOOR",
    "building": "MENARA PB",
    "dept":     "FINANCE DIVISION",
    "address":  ["MANAGEMENT ACCTG", "MENARA PUBLIC BANK",
                 "146 JALAN AMPANG", "50450 KUALA LUMPUR"],
    "dest":     "S1.RMT5",
}
# //*
# //PRINT14
PRINT14 = {
    "name":     "EN SAIFUL ADZLIM ARIFF",
    "room":     "24TH FLOOR",
    "building": "MENARA PB",
    "dept":     "RISK MANAGEMENT DIVISION",
    "address":  ["MENARA PUBLIC BANK",
                 "146 JALAN AMPANG", "50450 KUALA LUMPUR"],
    "dest":     "S1.LOCAL",
}

# =============================================================================
# HELPER: IEFBR14 equivalent
# Deletes the specified file or directory paths.
# Equivalent to //DELETE EXEC PGM=IEFBR14 with DISP=(MOD,DELETE,DELETE).
# Returns 0 on success, 8 if any deletion fails.
# =============================================================================

def delete_datasets(*paths: str, step_name: str = "DELETE") -> int:
    log.info(f"[{step_name}] BEGIN  (IEFBR14 -- DELETE output datasets)")
    rc = 0
    for path in paths:
        p = Path(path)
        try:
            if p.is_file():
                p.unlink()
                log.info(f"[{step_name}]   Deleted file : {path}")
            elif p.is_dir():
                shutil.rmtree(path)
                log.info(f"[{step_name}]   Deleted dir  : {path}")
            else:
                log.info(f"[{step_name}]   Not found (skipped): {path}")
        except OSError as exc:
            log.error(f"[{step_name}]   ERROR deleting {path}: {exc}")
            rc = 8
    log.info(f"[{step_name}] END    RC={rc}")
    return rc


# =============================================================================
# HELPER: Execute a converted Python step
# Equivalent to //stepname EXEC SAS609 ... //SYSIN DD DSN=SAP.BNM.PROGRAM(pgm)
# COND=(4,LT) enforcement: caller must call check_cond() before invoking.
# =============================================================================

def run_step(step_name: str, script_name: str) -> int:
    """
    Run a converted Python program as a subprocess.

    Args:
        step_name   : JCL step name (used in log messages).
        script_name : Filename of the Python script inside PGM_DIR.

    Returns:
        Return code from the subprocess (0 = success).
    """
    script = os.path.join(PGM_DIR, script_name)

    if not os.path.isfile(script):
        log.error(f"[{step_name}] Script not found: {script}")
        return 8

    env = os.environ.copy()
    env["PYTHONPATH"] = PGM_DIR + os.pathsep + env.get("PYTHONPATH", "")

    log.info(f"[{step_name}] BEGIN  -> {script_name}")
    t_start = datetime.now()

    result = subprocess.run([sys.executable, script], env=env)

    elapsed = (datetime.now() - t_start).total_seconds()
    rc      = result.returncode
    status  = "OK" if rc == 0 else f"FAILED (RC={rc})"
    log.info(f"[{step_name}] END    {status}  elapsed={elapsed:.1f}s")
    return rc


# =============================================================================
# HELPER: COND=(4,LT) gate
# JCL COND=(4,LT) means: bypass this step IF 4 < any-previous-RC,
# i.e. proceed only while highest_rc <= 4.
# =============================================================================

def check_cond(highest_rc: int) -> bool:
    return highest_rc <= MAX_RC


# =============================================================================
# MAIN JOB LOGIC
# =============================================================================

def main() -> int:
    log.info("=" * 70)
    log.info(f"  JOB: {JOB_NAME}    CLASS=A  MSGCLASS=X  COND=({MAX_RC},LT)")
    log.info(f"  START: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"  LOG  : {LOG_FILE}")
    log.info("=" * 70)

    # Ensure output directories exist before any step runs
    for d in (COLD_DIR, OPCL_DIR, OUTPUT_DIR, RM4XSMMR_DIR):
        os.makedirs(d, exist_ok=True)

    highest_rc = 0

    # -------------------------------------------------------------------------
    # //*---------------------------------------------------------------------
    # //*  WEIGHTED AVERAGE RATE FOR ALL ISLAMIC PRODUCTS........
    # //*  - TO DISCONTINUE PRINTING REPORTS AS REQUESTED:
    # //*    MOHD FADZLI MOHD YUSOFF - DATED 06/07/2004 - RHA
    # //*---------------------------------------------------------------------
    # //*EIBMIWAR EXEC SAS609,REGION=0M,WORK='395000,390000'
    # //*RATE     DD DSN=RBP2.B033.WADIAH.RATE.SORT,DISP=SHR
    # //*FD       DD DSN=SAP.PBB.MNIFD(0),DISP=SHR
    # //*LOAN     DD DSN=SAP.PBB.MNILN(0),DISP=SHR
    # //*DEPOSIT  DD DSN=SAP.PBB.MNITB(0),DISP=SHR
    # //*BNMTBL3  DD DSN=SAP.PBB.KAPITI3(0),DISP=SHR
    # //*SASLIST  DD SYSOUT=(,),OUTPUT=(*.PRINT2)
    # //*SYSIN    DD DSN=SAP.BNM.PROGRAM(EIBMIWAR),DISP=SHR
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # //*  TO GENERATE BRANCH DEPOSIT POSITION AS AT MONTH END.
    # //*  DOWNLOAD TO RPTS AND COLD FOR BRANCH TO VIEW AND MONITOR..
    # //*  CONTACT  MIS YIP LH OF PRODUCT DEVELOPMENT AND RESEARCH FOR
    # //*  FURTHER INFO...LN6
    # //*  DELETE OUTPUT DATASETS
    # //DELETE   EXEC PGM=IEFBR14
    # //DD1      DD DSN=SAP.PBB.PDRRPT.COLD,
    # //            DISP=(MOD,DELETE,DELETE),UNIT=SYSDA,SPACE=(CYL,1,RLSE)
    # -------------------------------------------------------------------------
    if check_cond(highest_rc):
        rc = delete_datasets(PDRRPT_COLD, step_name="DELETE")
        highest_rc = max(highest_rc, rc)

    # -------------------------------------------------------------------------
    # //EIBMBPOS  EXEC SAS609,REGION=64M,WORK='350000,350000'
    # //DEPOSIT   DD DSN=SAP.PBB.MNITB(0),DISP=SHR
    # //FD        DD DSN=SAP.PBB.MNIFD(0),DISP=SHR
    # //BRHFILE   DD DSN=RBP2.B033.PBB.BRANCH,DISP=SHR
    # //KAPITI5   DD DSN=SAP.PBB.KAPITI5(0),DISP=SHR
    # //LOAN      DD DSN=SAP.PBB.MNILN(0),DISP=SHR
    # //SASDATA   DD DSN=SAP.PBB.SASDATA,DISP=SHR
    # //PGM       DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //SASLIST   DD DSN=SAP.PBB.PDRRPT.COLD,DISP=(NEW,CATLG,DELETE),
    # //             DCB=(RECFM=FBA,LRECL=133,BLKSIZE=0,DSORG=PS),
    # //             SPACE=(CYL,(3,2),RLSE),UNIT=SYSDA
    # //SASLOG    DD SYSOUT=X
    # //SYSIN     DD DSN=SAP.BNM.PROGRAM(EIBMBPOS),DISP=SHR
    # -------------------------------------------------------------------------
    if check_cond(highest_rc):
        rc = run_step("EIBMBPOS", "EIBMBPOS.py")
        highest_rc = max(highest_rc, rc)

    # -------------------------------------------------------------------------
    # //*------------------------------------------------------------------
    # //* EXCEPTIONAL REPORTS ON DEPOSIT AND LOAN EXTRACTIONS...
    # //*------------------------------------------------------------------
    # //*EIBMEXCP EXEC SAS609,REGION=6M,WORK='120000,20000'
    # //*PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //*DEPOSIT  DD DSN=SAP.PBB.MNITB(0),DISP=SHR
    # //*FD       DD DSN=SAP.PBB.MNIFD(0),DISP=SHR
    # //*LOAN     DD DSN=SAP.PBB.MNILN(0),DISP=SHR
    # //*SASDATA  DD DSN=SAP.PBB.SASDATA,DISP=SHR
    # //*SASLIST  DD SYSOUT=(,),OUTPUT=(*.PRINT1)
    # //*SYSIN    DD DSN=SAP.BNM.PROGRAM(EIBMEXCP),DISP=SHR
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # //*------------------------------------------------------------------
    # //* EIBMNRCC - BNM REPORT FOR EALIS2 NRCC - CUSTCODE 63,68
    # //* REQUEST BY NORAEAN - 16/09/09
    # //*------------------------------------------------------------------
    # //*EIBMNRCC EXEC SAS609,REGION=6M,WORK='120000,20000'
    # //*LOAN1    DD DSN=SAP.PBB.MNILN(0),DISP=SHR
    # //*LOAN2    DD DSN=SAP.PBB.MNILN(-4),DISP=SHR
    # //*TRBL     DD DSN=SAP.PBB.MNITRBL,DISP=SHR
    # //*SASLIST  DD SYSOUT=(,),OUTPUT=(*.PRINT6)
    # //*SYSIN    DD DSN=SAP.BNM.PROGRAM(EIBMNRCC),DISP=SHR
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # //*    REPORT  : DEPOSITS, BY TIME TO MATURITY FOR ALCO
    # //*              (WEIGHTED AVERAGE COST BY MATURITY PROFILE)
    # //EIBMRM01 EXEC SAS609,REGION=6M,WORK='120000,80000'
    # //PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //FD       DD DSN=SAP.PBB.MNIFD(0),DISP=SHR
    # //BNM      DD DSN=SAP.PBB.MNITB(0),DISP=SHR
    # //SASLIST  DD SYSOUT=X
    # //SYSIN    DD DSN=SAP.BNM.PROGRAM(EIBMRM01),DISP=SHR
    # -------------------------------------------------------------------------
    if check_cond(highest_rc):
        rc = run_step("EIBMRM01", "EIBMRM1X.py")
        highest_rc = max(highest_rc, rc)

    # -------------------------------------------------------------------------
    # //*    REPORT  : FD-BY INDIVIDUAL AND NON-INDIVIDUAL, BY TIME
    # //*              TO MATURITY FOR ALCO  - PART 1
    # //*              (WEIGHTED AVERAGE COST BY MATURITY PROFILE)
    # //EIBMRM02 EXEC SAS609,REGION=6M,WORK='120000,80000'
    # //PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //FD       DD DSN=SAP.PBB.MNIFD(0),DISP=SHR
    # //SASLIST  DD SYSOUT=X
    # //SYSIN    DD DSN=SAP.BNM.PROGRAM(EIBMRM02),DISP=SHR
    # -------------------------------------------------------------------------
    if check_cond(highest_rc):
        rc = run_step("EIBMRM02", "EIBMRM2X.py")
        highest_rc = max(highest_rc, rc)

    # -------------------------------------------------------------------------
    # //*    REPORT  : FD-BY INDIVIDUAL AND NON-INDIVIDUAL, BY TIME
    # //*              TO MATURITY FOR ALCO  - PART 2.
    # //*              (WEIGHTED AVERAGE COST BY MATURITY PROFILE)
    # //EIBMRM03 EXEC SAS609,REGION=6M,WORK='120000,80000'
    # //PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //FD       DD DSN=SAP.PBB.MNIFD(0),DISP=SHR
    # //SASLIST  DD SYSOUT=X
    # //SYSIN    DD DSN=SAP.BNM.PROGRAM(EIBMRM03),DISP=SHR
    # -------------------------------------------------------------------------
    if check_cond(highest_rc):
        rc = run_step("EIBMRM03", "EIBMRM3X.py")
        highest_rc = max(highest_rc, rc)

    # -------------------------------------------------------------------------
    # //*   REPORT  : LOANS & ADVANCES, BY TIME TO MATURITY FOR ALCO
    # //*             (WEIGHTED AVERAGE YIELD BY MATURITY PROFILE)
    # //EIBMRM04 EXEC SAS609,REGION=6M,WORK='120000,80000'
    # //PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //BNM      DD DSN=SAP.PBB.SASDATA,DISP=SHR
    # //SASLIST  DD SYSOUT=X
    # //SYSIN    DD DSN=SAP.BNM.PROGRAM(EIBMRM04),DISP=SHR
    # -------------------------------------------------------------------------
    if check_cond(highest_rc):
        rc = run_step("EIBMRM04", "EIBMRM4X.py")
        highest_rc = max(highest_rc, rc)

    # -------------------------------------------------------------------------
    # //*   AVERAGE RATE ON ALL FIXED DEPOSIT PRODUCTS ON MONTHLY BASIS
    # //*   IE., ON 8TH, 15TH, 22TH AND MONTH-END EXTRACTIONS.....
    # //*EIBMRATE EXEC SAS609,REGION=48M
    # //*BNM      DD DSN=SAP.PBB.SASDATA,DISP=SHR
    # //*PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //*FD       DD DSN=SAP.PBB.MNIFD(0),DISP=SHR
    # //*FD1      DD DSN=SAP.PBB.MNIFD(-1),DISP=SHR
    # //*FD2      DD DSN=SAP.PBB.MNIFD(-2),DISP=SHR
    # //*FD3      DD DSN=SAP.PBB.MNIFD(-3),DISP=SHR
    # //*SASLIST  DD SYSOUT=(,),OUTPUT=(*.PRINT4),COPIES=2
    # //*SYSIN    DD DSN=SAP.BNM.PROGRAM(EIBMRATE),DISP=SHR
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # //*   BALANCES OF EXTERNAL ACCOUNT BY BRANCH
    # //*EIBMBEXT EXEC SAS609,REGION=6M,WORK='120000,80000'
    # //*BNM      DD DSN=&&TEMP,DISP=(NEW,PASS),
    # //*            DCB=(RECFM=FS,LRECL=27648,BLKSIZE=27648),
    # //*            SPACE=(CYL,(300,50)),UNIT=SYSDA
    # //*DEPOSIT  DD DSN=SAP.PBB.MNITB(0),DISP=SHR
    # //*FD       DD DSN=SAP.PBB.MNIFD(0),DISP=SHR
    # //*BNM1     DD DSN=SAP.PBB.SASDATA,DISP=SHR
    # //*PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //*SASLIST  DD SYSOUT=(,),OUTPUT=(*.PRINT1)
    # //*SYSIN    DD DSN=SAP.BNM.PROGRAM(EIBMBEXT),DISP=SHR
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # //*   OPEN CLOSED ACCOUNT
    # //DELETE   EXEC PGM=IEFBR14
    # //DD4      DD DSN=SAP.PBB.OPCL.SAVG,DISP=(MOD,DELETE,DELETE),SPACE=(CYL,(5,1))
    # //DD5      DD DSN=SAP.PBB.OPCL.CURR,DISP=(MOD,DELETE,DELETE),SPACE=(CYL,(5,1))
    # //DD6      DD DSN=SAP.PBB.EIMALIMT.TEXT,DISP=(MOD,DELETE,DELETE),SPACE=(CYL,(5,1))
    # //DD7      DD DSN=SAP.PBB.OPCL.FD,DISP=(MOD,DELETE,DELETE),SPACE=(CYL,(5,1))
    # -------------------------------------------------------------------------
    if check_cond(highest_rc):
        rc = delete_datasets(
            OPCL_SAVG, OPCL_CURR, EIMALIMT_TEXT, OPCL_FD,
            step_name="DELETE",
        )
        highest_rc = max(highest_rc, rc)

    # -------------------------------------------------------------------------
    # //* SMR 2008-1057
    # //* ALL ACCTS (PIBB+PBB)
    # //EIBMDPSA EXEC SAS609
    # //BRHFILE  DD DSN=SAP.RBP2.B033.PBB.BRANCH,DISP=SHR
    # //DEPOSIT  DD DSN=SAP.PBB.MNITB.MTH,DISP=SHR
    # //MIS      DD DSN=SAP.PBB.OPCL.SASDATA,DISP=OLD
    # //PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //SASLIST  DD DSN=SAP.PBB.OPCL.SAVG,DISP=(NEW,CATLG,DELETE),
    # //            DCB=(RECFM=FB,LRECL=133,BLKSIZE=0,DSORG=PS),
    # //            SPACE=(CYL,(3,2),RLSE),UNIT=SYSDA
    # //SYSIN    DD DSN=SAP.BNM.PROGRAM(EIBMDPSA),DISP=SHR
    # -------------------------------------------------------------------------
    if check_cond(highest_rc):
        rc = run_step("EIBMDPSA", "EIBMDPSA.py")
        highest_rc = max(highest_rc, rc)

    # -------------------------------------------------------------------------
    # //EIBMDPCA EXEC SAS609
    # //BRHFILE  DD DSN=SAP.RBP2.B033.PBB.BRANCH,DISP=SHR
    # //DEPOSIT  DD DSN=SAP.PBB.MNITB.MTH,DISP=SHR
    # //MIS      DD DSN=SAP.PBB.OPCL.SASDATA,DISP=OLD
    # //PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //SASLIST  DD DSN=SAP.PBB.OPCL.CURR,DISP=(NEW,CATLG,DELETE),
    # //            DCB=(RECFM=FB,LRECL=133,BLKSIZE=0,DSORG=PS),
    # //            SPACE=(CYL,(3,2),RLSE),UNIT=SYSDA
    # //SYSIN    DD DSN=SAP.BNM.PROGRAM(EIBMDPCA),DISP=SHR
    # -------------------------------------------------------------------------
    if check_cond(highest_rc):
        rc = run_step("EIBMDPCA", "EIBMDPCA.py")
        highest_rc = max(highest_rc, rc)

    # -------------------------------------------------------------------------
    # //EIBMDPFQ EXEC SAS609
    # //BRHFILE  DD DSN=SAP.RBP2.B033.PBB.BRANCH,DISP=SHR
    # //DEPOSIT  DD DSN=SAP.PBB.MNITB.MTH,DISP=SHR
    # //FD       DD DSN=SAP.PBB.MNIFD.MTH,DISP=SHR
    # //MIS      DD DSN=SAP.PBB.OPCL.SASDATA,DISP=OLD
    # //PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //SASLIST  DD DSN=SAP.PBB.OPCL.FD,DISP=(NEW,CATLG,DELETE),
    # //            DCB=(RECFM=FB,LRECL=133,BLKSIZE=0,DSORG=PS),
    # //            SPACE=(CYL,(3,2),RLSE),UNIT=SYSDA
    # //SYSIN    DD DSN=SAP.BNM.PROGRAM(EIBMDPFQ),DISP=SHR
    # -------------------------------------------------------------------------
    if check_cond(highest_rc):
        rc = run_step("EIBMDPFQ", "EIBMDPFQ.py")
        highest_rc = max(highest_rc, rc)

    # -------------------------------------------------------------------------
    # //*------------------------------------------------------------------
    # //* MONTHLY REPORT ON MORE PLAN LOANS - PURPOSE OTHERS
    # //*------------------------------------------------------------------
    # //EIBMCLAS  EXEC SAS609
    # //BNM       DD DSN=SAP.PBB.SASDATA,DISP=SHR
    # //LOAN      DD DSN=SAP.PBB.MNILN(0),DISP=SHR
    # //PGM       DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //SASLIST   DD SYSOUT=X
    # //SYSIN     DD DSN=SAP.BNM.PROGRAM(EIBMCLAS),DISP=SHR
    # -------------------------------------------------------------------------
    if check_cond(highest_rc):
        rc = run_step("EIBMCLAS", "EIBMCLAS.py")
        highest_rc = max(highest_rc, rc)

    # -------------------------------------------------------------------------
    # //*------------------------------------------------------------------
    # //* EXTRACTION FOR ISLAMIC PRODUCTS (204,215)
    # //*------------------------------------------------------------------
    # //EIBMISL2  EXEC SAS609
    # //DEPOSIT   DD DSN=SAP.PBB.MNITB(0),DISP=SHR
    # //PGM       DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //SASLIST   DD SYSOUT=(,),OUTPUT=(*.PRINT9)
    # //SYSIN     DD DSN=SAP.BNM.PROGRAM(EIBMISL2),DISP=SHR
    # -------------------------------------------------------------------------
    if check_cond(highest_rc):
        rc = run_step("EIBMISL2", "EIBMISL2.py")
        highest_rc = max(highest_rc, rc)

    # -------------------------------------------------------------------------
    # //*------------------------------------------------------------------
    # //* MAX & MIN LENDING RATE BY SECTOR FOR HP (D) DISBURSED FOR THE MTH
    # //* REMOVE BY ESMR 2007-329 BY NEW JOB EIBMHPMX RUN ON 3RD MTHLY.
    # //*------------------------------------------------------------------
    # //*EIMHPMAX  EXEC SAS609
    # //*LOAN      DD DSN=SAP.PBB.SASDATA,DISP=SHR
    # //*NOTE      DD DSN=SAP.PBB.MNILN(0),DISP=SHR
    # //*PGM       DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //*SASLIST   DD SYSOUT=(,),OUTPUT=(*.PRINT10)
    # //*SYSIN     DD DSN=SAP.BNM.PROGRAM(EIMHPMAX),DISP=SHR
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # //*------------------------------------------------------------------
    # //* GROSS LOAN APPROVED LIMIT
    # //*------------------------------------------------------------------
    # //EIMALIMT EXEC SAS609
    # //LOAN     DD DSN=SAP.PBB.SASDATA,DISP=SHR
    # //BRHFILE  DD DSN=RBP2.B033.PBB.BRANCH,DISP=SHR
    # //SASLIST  DD DSN=SAP.PBB.EIMALIMT.TEXT,DISP=(NEW,CATLG,DELETE),
    # //            DCB=(RECFM=FB,LRECL=133,BLKSIZE=0,DSORG=PS),
    # //            SPACE=(CYL,(3,2),RLSE),UNIT=SYSDA
    # //SYSIN    DD DSN=SAP.BNM.PROGRAM(EIMALIMT),DISP=SHR
    # -------------------------------------------------------------------------
    if check_cond(highest_rc):
        rc = run_step("EIMALIMT", "EIMALIMT.py")
        highest_rc = max(highest_rc, rc)

    # -------------------------------------------------------------------------
    # //*------------------------------------------------------------------
    # //* NUMBER OF ACCOUNTS FOR LOANS *DISCONTINUE EMSR 2018-1682
    # //*------------------------------------------------------------------
    # //*EIBM619L EXEC SAS609
    # //*LOAN     DD DSN=SAP.PBB.SASDATA,DISP=SHR
    # //*PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //*BRHFILE  DD DSN=SAP.RBP2.B033.PBB.BRANCH,DISP=SHR
    # //*SASLIST  DD SYSOUT=(,),OUTPUT=(*.PRINT13)
    # //*SYSIN    DD DSN=SAP.BNM.PROGRAM(EIBM619L),DISP=SHR
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # //*------------------------------------------------------------------
    # //* NUMBER OF ACCOUNTS FOR DEPOSIT *DISCONTINUE EMSR 2018-1682
    # //*------------------------------------------------------------------
    # //*EIBM619D EXEC SAS609
    # //*BNM      DD DSN=SAP.PBB.MNITB(0),DISP=SHR
    # //*LOAN     DD DSN=SAP.PBB.SASDATA,DISP=SHR
    # //*PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //*BRHFILE  DD DSN=SAP.RBP2.B033.PBB.BRANCH,DISP=SHR
    # //*SASLIST  DD SYSOUT=(,),OUTPUT=(*.PRINT13)
    # //*SYSIN    DD DSN=SAP.BNM.PROGRAM(EIBM619D),DISP=SHR
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # //* TIME TO MATURITY REPORT FOR FCY FD. SMR 2005-1085.
    # //* EIBMFC01 - BY REMAINING MATURITY
    # //* 2007-690 - SOFTCOPY RPT OF EIBMFC01 INTO RISKMGMT FOLDER (RHA)
    # //*EIBMFC01 EXEC SAS609
    # //*PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //*FD       DD DSN=SAP.PBB.MNIFD(0),DISP=SHR
    # //*SASLIST  DD SYSOUT=(,),OUTPUT=(*.PRINT14)
    # //*SASLIST  DD DSN=SAP.PBB.RISKMGMT.FCYRPT(+1),
    # //*            DISP=(NEW,CATLG,DELETE),SPACE=(TRK,(1,1),RLSE),
    # //*            UNIT=SYSDA,DCB=(LRECL=132,RECFM=FBA,BLKSIZE=0,BUFNO=30)
    # //*SYSIN    DD DSN=SAP.BNM.PROGRAM(EIBMFC01),DISP=SHR
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # //* EIBMFC02 - BY ORIGINAL MATURITY
    # //*DELETE   EXEC PGM=IEFBR14
    # //*DD1      DD DSN=SAP.PBB.EIBMFC02.TXT,DISP=(MOD,DELETE,DELETE),SPACE=(CYL,(5,1))
    # //*EIBMFC02 EXEC SAS609
    # //*PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //*FD       DD DSN=SAP.PBB.MNIFD(0),DISP=SHR
    # //*SASLIST  DD DSN=SAP.PBB.EIBMFC02.TXT,DISP=(NEW,CATLG,DELETE),
    # //*            DCB=(RECFM=FB,LRECL=133,BLKSIZE=0,DSORG=PS),
    # //*            SPACE=(CYL,(3,2),RLSE),UNIT=SYSDA
    # //*SYSIN    DD DSN=SAP.BNM.PROGRAM(EIBMFC02),DISP=SHR
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # //* EIBMFC03 - BY ORIGINAL MATURITY
    # //*DELETE   EXEC PGM=IEFBR14
    # //*DD1      DD DSN=SAP.PBB.EIBMFC03.TXT,DISP=(MOD,DELETE,DELETE),SPACE=(CYL,(5,1))
    # //*EIBMFC03 EXEC SAS609
    # //*PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //*FD       DD DSN=SAP.PBB.MNIFD(0),DISP=SHR
    # //*SASLIST  DD DSN=SAP.PBB.EIBMFC03.TXT,DISP=(NEW,CATLG,DELETE),
    # //*            DCB=(RECFM=FB,LRECL=133,BLKSIZE=0,DSORG=PS),
    # //*            SPACE=(CYL,(3,2),RLSE),UNIT=SYSDA
    # //*SYSIN    DD DSN=SAP.BNM.PROGRAM(EIBMFC03),DISP=SHR
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # //* EIBMRM1X -  DEPOSITS, BY TIME TO MATURITY FOR ALCO
    # //*DELETE   EXEC PGM=IEFBR14
    # //*DD1      DD DSN=SAP.PBB.EIBMRM1X.TXT,DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(1,10))
    # //*CREATE   EXEC PGM=IEFBR14
    # //*CRT01    DD DSN=SAP.PBB.EIBMRM1X.TXT,DISP=(NEW,CATLG,DELETE),
    # //*            DCB=(RECFM=FB,LRECL=256,BLKSIZE=25600),SPACE=(CYL,(10,20),RLSE),UNIT=SYSDA
    # //*EIBMRM1X EXEC SAS609,REGION=6M,WORK='120000,80000'
    # //*PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //*FD       DD DSN=SAP.PBB.MNIFD(0),DISP=SHR
    # //*BNM      DD DSN=SAP.PBB.MNITB(0),DISP=SHR
    # //*TEMP     DD DSN=SAP.PBB.EIBMRM1X.TXT,DISP=OLD
    # //*SASLIST  DD SYSOUT=X
    # //*SYSIN    DD DSN=SAP.BNM.PROGRAM(EIBMRM1X),DISP=SHR
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # //* EIBMRM2X -  FD-BY INDIVIDUAL AND NON-INDIVIDUAL, BY TIME TO MATURITY FOR ALCO
    # //*DELETE   EXEC PGM=IEFBR14
    # //*DD1      DD DSN=SAP.PBB.EIBMRM2X.TXT,DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(1,10))
    # //*CREATE   EXEC PGM=IEFBR14
    # //*CRT01    DD DSN=SAP.PBB.EIBMRM2X.TXT,DISP=(NEW,CATLG,DELETE),
    # //*            DCB=(RECFM=FB,LRECL=256,BLKSIZE=25600),SPACE=(CYL,(10,20),RLSE),UNIT=SYSDA
    # //*EIBMRM2X EXEC SAS609,REGION=6M,WORK='120000,80000'
    # //*PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //*FD       DD DSN=SAP.PBB.MNIFD(0),DISP=SHR
    # //*TEMP     DD DSN=SAP.PBB.EIBMRM2X.TXT,DISP=OLD
    # //*SASLIST  DD SYSOUT=X
    # //*SYSIN    DD DSN=SAP.BNM.PROGRAM(EIBMRM2X),DISP=SHR
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # //* EIBMRM3X -  FD-BY INDIVIDUAL AND NON-INDIVIDUAL, BY TIME TO MATURITY FOR ALCO
    # //*DELETE   EXEC PGM=IEFBR14
    # //*DD1      DD DSN=SAP.PBB.EIBMRM3X.TXT,DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(1,10))
    # //*CREATE   EXEC PGM=IEFBR14
    # //*CRT01    DD DSN=SAP.PBB.EIBMRM3X.TXT,DISP=(NEW,CATLG,DELETE),
    # //*            DCB=(RECFM=FB,LRECL=256,BLKSIZE=25600),SPACE=(CYL,(10,20),RLSE),UNIT=SYSDA
    # //*EIBMRM3X EXEC SAS609,REGION=6M,WORK='120000,80000'
    # //*PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //*FD       DD DSN=SAP.PBB.MNIFD(0),DISP=SHR
    # //*TEMP     DD DSN=SAP.PBB.EIBMRM3X.TXT,DISP=OLD
    # //*SASLIST  DD SYSOUT=X
    # //*SYSIN    DD DSN=SAP.BNM.PROGRAM(EIBMRM3X),DISP=SHR
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # //* EIBMRM4X -  REPRICING GAP
    # //DELETE   EXEC PGM=IEFBR14
    # //DD1      DD DSN=SAP.PBB.EIBMRM4X.SUMMR,DISP=(MOD,DELETE,DELETE),SPACE=(TRK,(1,10))
    # -------------------------------------------------------------------------
    if check_cond(highest_rc):
        rc = delete_datasets(RM4XSMMR_DIR, step_name="DELETE")
        highest_rc = max(highest_rc, rc)
        # Re-create the directory after deletion so EIBMRM4X can write into it
        os.makedirs(RM4XSMMR_DIR, exist_ok=True)

    # -------------------------------------------------------------------------
    # //EIBMRM4X EXEC SAS609,WORKSPC='300,300'
    # //BNM       DD DSN=SAP.PBB.SASDATA,DISP=SHR
    # //PGM       DD DSN=SAP.BNM.PROGRAM,DISP=SHR
    # //OD        DD DSN=SAP.PBB.MNILIMT(0),DISP=SHR
    # //PROV      DD DSN=SAP.PBB.CCRIS2.PROVISIO,DISP=SHR
    # //LNNOTE    DD DSN=SAP.PBB.MNILN(0),DISP=SHR
    # //RM4XSMMR  DD DSN=SAP.PBB.EIBMRM4X.SUMMR,DISP=(NEW,CATLG,DELETE),
    # //             DCB=(RECFM=FS,LRECL=27648,BLKSIZE=27648),
    # //             SPACE=(CYL,(20,20),RLSE),UNIT=(SYSDA,5)
    # //SASLIST   DD SYSOUT=X
    # //SORTWK01  DD UNIT=SYSDA,SPACE=(CYL,(500))
    # //SORTWK02  DD UNIT=SYSDA,SPACE=(CYL,(500))
    # //SORTWK03  DD UNIT=SYSDA,SPACE=(CYL,(500))
    # //SYSIN     DD DSN=SAP.BNM.PROGRAM(EIBMRM4X),DISP=SHR
    # -------------------------------------------------------------------------
    if check_cond(highest_rc):
        rc = run_step("EIBMRM4X", "EIBMRM4X.py")
        highest_rc = max(highest_rc, rc)

    # -------------------------------------------------------------------------
    # //* FTP HOST DATASETS TO PBB DATAWAREHOUSE SERVER
    # //*RUNSFTP  EXEC COZBATCH
    # //*CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(CSASSFTP)
    # //*           DD *
    # //*lzopts servercp=$servercp,notrim,overflow=trunc,mode=text
    # //*lzopts linerule=$lr
    # //*CD TEXTFILE
    # //*CD RISKMGT
    # //*PUT //SAP.PBB.RISKMGMT.FCYRPT(+1) RISKMFCY.TXT
    # //*PUT //SAP.PBB.EIBMFC02.TXT EIBMFC02.TXT
    # //*PUT //SAP.PBB.EIBMFC03.TXT EIBMFC03.TXT
    # //*PUT //SAP.PBB.EIBMRM1X.TXT EIBMRM1X.TXT
    # //*PUT //SAP.PBB.EIBMRM2X.TXT EIBMRM2X.TXT
    # //*PUT //SAP.PBB.EIBMRM3X.TXT EIBMRM3X.TXT
    # //*EOB
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # //* FTP HOST DATASETS TO DATA REPORT REPOSITORY SYSTEM (DRR)
    # //*RUNSFTP  EXEC COZBATCH
    # //*CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(DRR#SFTP)
    # //*           DD *
    # //*lzopts servercp=$servercp,notrim,overflow=trunc,mode=text
    # //*lzopts linerule=$lr
    # //*CD "RMD-ALM/IRR"
    # //*PUT //SAP.PBB.RISKMGMT.FCYRPT(+1) RISKMFCY.TXT
    # //*PUT //SAP.PBB.EIBMFC02.TXT EIBMFC02.TXT
    # //*PUT //SAP.PBB.EIBMFC03.TXT EIBMFC03.TXT
    # //*PUT //SAP.PBB.EIBMRM1X.TXT EIBMRM1X.TXT
    # //*PUT //SAP.PBB.EIBMRM2X.TXT EIBMRM2X.TXT
    # //*PUT //SAP.PBB.EIBMRM3X.TXT EIBMRM3X.TXT
    # //*EOB
    # -------------------------------------------------------------------------

    # ==========================================================================
    # JOB COMPLETION SUMMARY
    # ==========================================================================
    log.info("=" * 70)
    log.info(f"  JOB: {JOB_NAME}   COMPLETED")
    log.info(f"  HIGHEST RC : {highest_rc}")
    log.info(f"  END        : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"  LOG FILE   : {LOG_FILE}")
    log.info("=" * 70)

    return highest_rc


# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    sys.exit(main())
