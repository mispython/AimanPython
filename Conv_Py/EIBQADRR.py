# !/usr/bin/env python3
"""
PROGRAM : EIBQADRR
PURPOSE : JOB ORCHESTRATOR FOR PB PREMIUM CLUB ADDRESS LABEL AND REPORT PROCESSING PIPELINE.
          EQUIVALENT OF THE JCL JOB EIBQADRR WHICH SEQUENCES THE EXECUTION OF:
            EIBQADR0 - PRE-PROCESS / EXTRACT RECORDS
            EIBQADR1 - NAME LABELS: NEW MEMBERS
            EIBQADR2 - NAME LABELS: AUTO MEMBERS
            EIBQADR3 - NAME LABELS: 50 PLUS
            EIBQADR4 - NAME LABELS: HOUSING LOAN
            EIBQADR5 - NAME LABELS: HIRE PURCHASE
            EIBQPBCR - LISTING OF ELIGIBLE DEPOSITORS
            EIBQPBCM - SOFT COPY MEMBER LISTING (UNICARD)

JCL OUTPUT DESTINATIONS (original):
  LABEL1  -> EIBQADR1 output  FORMS=PBCLBLB1
  LABEL2  -> EIBQADR2 output  FORMS=PBCLBLB2
  LABEL3  -> EIBQADR3 output  FORMS=PBCLBLB3
  LABEL4  -> EIBQADR4 output  FORMS=PBCLBLB4
  LABEL5  -> EIBQADR5 output  FORMS=PBCLBLB5
  LABELA  -> EIBQPBCR output  (CLASS=R listing report)
  OUTPUT  -> EIBQPBCM output  SAP.PBB.PBCLUB.NAMELIST

---------------------------------------------------------------------------

JCL header metadata (informational, from original JCL)
EIBQADRR JOB MISEIS, EIBQADRR, CLASS=A, MSGCLASS=X, NOTIFY=&SYSUID, USER=OPCC
JOBPARM S=S1M1

---------------------------------------------------------------------------

OUTPUT routing metadata (from JCL OUTPUT statements)
In the original JCL these route printed output to a physical printer
with specific label stock forms (PBCLBLB1-5). In Python the equivalent
output files are written by each sub-program to the OUTPUT_DIR defined
in their respective modules.

LABEL1 OUTPUT CLASS=P, NAME='MS.YONG POH PIN / MS HINDULEKA',
        ROOM='21ST FLOOR', BUILDING='MENARA PBB',
        DEPT='CONSUMER BANKING',
        ADDRESS=('MENARA PUBLIC BANK','146 JALAN AMPANG','50450 KUALA LUMPUR'),
        FORMS=PBCLBLB1,LINECT=90,FCB=LBL9,DEST=S1.LOCAL

LABEL2 OUTPUT CLASS=P, NAME='MS.YONG POH PIN / MS HINDULEKA',
        ROOM='21ST FLOOR', BUILDING='MENARA PBB',
        DEPT='CONSUMER BANKING',
        ADDRESS=('MENARA PUBLIC BANK','146 JALAN AMPANG','50450 KUALA LUMPUR'),
        FORMS=PBCLBLB2,LINECT=90,FCB=LBL9,DEST=S1.LOCAL

LABEL3 OUTPUT CLASS=P, NAME='MS.YONG POH PIN / MS HINDULEKA',
        ROOM='21ST FLOOR', BUILDING='MENARA PBB',
        DEPT='CONSUMER BANKING',
        ADDRESS=('MENARA PUBLIC BANK','146 JALAN AMPANG','50450 KUALA LUMPUR'),
        FORMS=PBCLBLB3,LINECT=90,FCB=LBL9,DEST=S1.LOCAL

LABEL4 OUTPUT CLASS=P, NAME='MS.YONG POH PIN / MS HINDULEKA',
        ROOM='21ST FLOOR', BUILDING='MENARA PBB',
        DEPT='CONSUMER BANKING',
        ADDRESS=('MENARA PUBLIC BANK','146 JALAN AMPANG','50450 KUALA LUMPUR'),
        FORMS=PBCLBLB4,LINECT=90,FCB=LBL9,DEST=S1.LOCAL

LABEL5 OUTPUT CLASS=P, NAME='MS.YONG POH PIN / MS HINDULEKA',
        ROOM='21ST FLOOR', BUILDING='MENARA PBB',
        DEPT='CONSUMER BANKING',
        ADDRESS=('MENARA PUBLIC BANK','146 JALAN AMPANG','50450 KUALA LUMPUR'),
        FORMS=PBCLBLB5,LINECT=90,FCB=LBL9,DEST=S1.LOCAL

LABELA OUTPUT CLASS=R, NAME='MS.YONG POH PIN / MS HINDULEKA',
        ROOM='21ST FLOOR', BUILDING='MENARA PBB',
        DEPT='CONSUMER BANKING',
        ADDRESS=('MENARA PUBLIC BANK','146 JALAN AMPANG','50450 KUALA LUMPUR'),
        DEST=S1.LOCAL
"""


import subprocess
import sys
import logging
from pathlib import Path

# ---------------------------------------------------------------------------
# Path Configuration
# ---------------------------------------------------------------------------
BASE_DIR    = Path(".")
SCRIPTS_DIR = BASE_DIR    # all sub-programs reside in the same directory
LOG_FILE    = BASE_DIR / "output" / "EIBQADRR_pipeline.log"

LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pipeline step definitions
# Each entry: (step_label, script_name, description)
# Steps that were commented out in the JCL are marked with active=False.
# ---------------------------------------------------------------------------
PIPELINE_STEPS = [
    # -----------------------------------------------------------------------
    # PRE PROCESSES TO EXTRACT RECORDS THAT MEET CRITERIA.
    # //EIBQADR0 EXEC SAS609,WORK='120000,8000',OPTIONS='COMPRESS=YES'
    # //ADDR  DD DSN=SAP.PBB.DPADDR,DISP=OLD
    # //CISS  DD DSN=SAP.PBB.CRM.CISBEXT,DISP=SHR
    # //CISC  DD DSN=SAP.PBB.CISBEXT.DP,DISP=SHR
    # //CIS   DD DSN=SAP.PBB.CISBEXT.LN,DISP=SHR
    # //DP    DD DSN=SAP.PBB.CRMDP,DISP=SHR
    # //HP    DD DSN=SAP.PBB.HPDATAWH,DISP=SHR
    # //LOAN  DD DSN=SAP.PBB.CRMLN,DISP=SHR
    # //MNITB DD DSN=SAP.PBB.MNITB(0),DISP=SHR
    # -----------------------------------------------------------------------
    {
        "label":       "EIBQADR0",
        "script":      "EIBQADR0.py",
        "description": "PRE-PROCESS: extract and filter deposit/loan/HP records",
        "active":      True,
    },

    # -----------------------------------------------------------------------
    # BATCHCARD FOR INTRADAY POSTING.
    # //*EIBQCARD EXEC SAS609,WORK='120000,8000'
    # //*CARD DD DSN=SAP.PBB.PBPREM.TEXT,DISP=(NEW,CATLG),...
    # (Commented out in original JCL)
    # -----------------------------------------------------------------------
    {
        "label":       "EIBQCARD",
        "script":      "EIBQCARD.py",
        "description": "BATCHCARD for intraday posting (commented out in JCL)",
        "active":      False,
    },

    # -----------------------------------------------------------------------
    # NAME LABELS FOR PB PREMIUM CLUB MEMBERS - HP
    # //*EIBQADR5 EXEC SAS609,WORK='120000,80000',OPTIONS='COMPRESS=YES'
    # //*SASLIST DD SYSOUT=(,),OUTPUT=(*.LABEL5)
    # (Commented out in original JCL)
    # -----------------------------------------------------------------------
    {
        "label":       "EIBQADR5",
        "script":      "EIBQADR5.py",
        "description": "NAME LABELS: Hire Purchase members -> LABEL5 (PBCLBLB5)",
        "active":      False,
    },

    # -----------------------------------------------------------------------
    # NAME LABELS FOR PB PREMIUM CLUB MEMBERS - HL
    # //*EIBQADR4 EXEC SAS609,WORK='120000,8000',OPTIONS='COMPRESS=YES'
    # //*SASLIST DD SYSOUT=(,),OUTPUT=(*.LABEL4)
    # (Commented out in original JCL)
    # -----------------------------------------------------------------------
    {
        "label":       "EIBQADR4",
        "script":      "EIBQADR4.py",
        "description": "NAME LABELS: Housing Loan members -> LABEL4 (PBCLBLB4)",
        "active":      False,
    },

    # -----------------------------------------------------------------------
    # NAME LABELS FOR PB PREMIUM CLUB MEMBERS - 50 PLUS
    # //*EIBQADR3 EXEC SAS609,WORK='120000,8000',OPTIONS='COMPRESS=YES'
    # //*SASLIST DD SYSOUT=(,),OUTPUT=(*.LABEL3)
    # (Commented out in original JCL)
    # -----------------------------------------------------------------------
    {
        "label":       "EIBQADR3",
        "script":      "EIBQADR3.py",
        "description": "NAME LABELS: 50 Plus members -> LABEL3 (PBCLBLB3)",
        "active":      False,
    },

    # -----------------------------------------------------------------------
    # NAME LABELS FOR PB PREMIUM CLUB MEMBERS - AUTO MEMB.
    # //*EIBQADR2 EXEC SAS609,WORK='120000,8000',OPTIONS='COMPRESS=YES'
    # //*SASLIST DD SYSOUT=(,),OUTPUT=(*.LABEL2)
    # (Commented out in original JCL)
    # -----------------------------------------------------------------------
    {
        "label":       "EIBQADR2",
        "script":      "EIBQADR2.py",
        "description": "NAME LABELS: Auto members -> LABEL2 (PBCLBLB2)",
        "active":      False,
    },

    # -----------------------------------------------------------------------
    # NAME LABELS FOR PB PREMIUM CLUB MEMBERS - NEW MEMBERS
    # //*EIBQADR1 EXEC SAS609,WORK='120000,80000',OPTIONS='COMPRESS=YES'
    # //*SASLIST DD SYSOUT=(,),OUTPUT=(*.LABEL1)
    # (Commented out in original JCL)
    # -----------------------------------------------------------------------
    {
        "label":       "EIBQADR1",
        "script":      "EIBQADR1.py",
        "description": "NAME LABELS: New members -> LABEL1 (PBCLBLB1)",
        "active":      False,
    },

    # -----------------------------------------------------------------------
    # LISTING OF ELIGIBLE DEPOSITORS FOR PB PREMIUM CLUB
    # //*EIBQPBCR EXEC SAS609
    # //*SASLIST DD SYSOUT=(,),OUTPUT=(*.LABELA)
    # (Commented out in original JCL)
    # -----------------------------------------------------------------------
    {
        "label":       "EIBQPBCR",
        "script":      "EIBQPBCR.py",
        "description": "LISTING: eligible depositors report -> LABELA",
        "active":      False,
    },

    # -----------------------------------------------------------------------
    # SOFT COPY PB PREMIUM CLUB MEMBER LISTING FOR UNICARD.
    # //*EIBQPBCM EXEC SAS609,WORK='100000,100000'
    # //*OUTPUT   DD DSN=SAP.PBB.PBCLUB.NAMELIST,DISP=(NEW,CATLG),...
    #             DCB=(LRECL=80,RECFM=FB,BLKSIZE=0,BUFNO=5)
    # (Commented out in original JCL)
    # -----------------------------------------------------------------------
    {
        "label":       "EIBQPBCM",
        "script":      "EIBQPBCM.py",
        "description": "SOFT COPY member listing for Unicard -> SAP.PBB.PBCLUB.NAMELIST",
        "active":      False,
    },
]


# ---------------------------------------------------------------------------
# Step runner
# ---------------------------------------------------------------------------
def run_step(step: dict) -> bool:
    """
    Execute a single pipeline step using the current Python interpreter.
    Returns True on success, False on failure.
    """
    script_path = SCRIPTS_DIR / step["script"]

    if not script_path.exists():
        log.error(f"[{step['label']}] Script not found: {script_path}")
        return False

    log.info(f"[{step['label']}] START  -- {step['description']}")
    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=False,   # allow sub-program stdout/stderr to flow through
    )

    if result.returncode == 0:
        log.info(f"[{step['label']}] COMPLETED SUCCESSFULLY (RC=0)")
        return True
    else:
        log.error(
            f"[{step['label']}] FAILED  (RC={result.returncode})"
        )
        return False


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def main():
    log.info("=" * 70)
    log.info("EIBQADRR PIPELINE START")
    log.info("=" * 70)

    overall_rc = 0

    for step in PIPELINE_STEPS:
        if not step["active"]:
            log.info(
                f"[{step['label']}] SKIPPED (inactive / commented out in JCL) "
                f"-- {step['description']}"
            )
            continue

        success = run_step(step)
        if not success:
            log.error(
                f"Pipeline aborted at step [{step['label']}]. "
                "Subsequent steps will not be executed."
            )
            overall_rc = 8   # non-zero return code mirrors JCL COND failure
            break

    log.info("=" * 70)
    if overall_rc == 0:
        log.info("EIBQADRR PIPELINE COMPLETED SUCCESSFULLY")
    else:
        log.info(f"EIBQADRR PIPELINE ENDED WITH ERRORS  (RC={overall_rc})")
    log.info("=" * 70)

    sys.exit(overall_rc)


if __name__ == "__main__":
    main()
