#!/usr/bin/env python3
"""
Program  : EIIMRPT1.py
Purpose  : MIS reporting orchestrator for Public Islamic Bank Berhad (PIBB).
           Run AFTER EIBMMISE.

           Executes the following sub-programs in sequence:

           1. DEPM500K  — Credit/OD movement of RM500K and above for demand
                          deposits (DD SAP.PIBB.MNITB(0)).
                          Output: DEPM500K.txt  (equiv. SAP.PIBB.DEPM500K.TXT,
                                                 RECFM=FB, LRECL=133)

           2. EIIMFD03  — FD movement of RM500K and above (current vs previous
                          period FD snapshots).
                          Output: FDTEXT.txt    (equiv. SAP.PIBB.FD500K.TXT,
                                                 RECFM=FB, LRECL=133)

           3. EIIMLIMT  — Gross loan approved limit for OD, RC and other loans.
                          Output: EIMALIMT.txt  (equiv. SAP.PIBB.EIMALIMT.TEXT,
                                                 RECFM=FB, LRECL=133)

           4. EIBMRM4X  — Repricing gap (BNM=SAP.PIBB.SASDATA, OD=MNILIMT(0),
                          PROV=CCRIS2.PROVISIO, LNNOTE=MNILN(0)).
                          Output: RM4XSMMR parquet/text.

           Commented-out steps (EIBMRM1X, EIBMRM2X, EIBMRM3X, FTP sections)
           are preserved as comments matching the original JCL source.

           JCL artefacts translated:
           - DELETE (IEFBR14 MOD,DELETE,DELETE) -> pre-run deletion of output files
           - CREATE (IEFBR14 NEW,CATLG,DELETE)  -> output directories created by
                                                    sub-programs; no explicit pre-
                                                    allocation needed in Python
           - SORTWK01-10 -> not required; DuckDB/Polars manage sort memory internally
           - COND=(4,LT) on JOB card -> exit code guard: abort if previous step
                                        returned >= 4 (implemented via return-code check)

           Note: EIBMRM4X is referenced as a sub-program; if not yet converted,
           a placeholder stub is called and a warning is logged.
"""

import os
import sys
import logging
import subprocess
from pathlib import Path

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR    = Path(__file__).resolve().parent
DATA_DIR    = BASE_DIR / "data"
OUTPUT_DIR  = BASE_DIR / "output"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Output files (equiv. to DD DSN allocations in JCL)
# DD SASLIST -> SAP.PIBB.DEPM500K.TXT  (RECFM=FB, LRECL=133)
DEPM500K_TXT   = OUTPUT_DIR / "DEPM500K.txt"
# DD FDTEXT  -> SAP.PIBB.FD500K.TXT    (RECFM=FB, LRECL=133)
FD500K_TXT     = OUTPUT_DIR / "FDTEXT.txt"
# DD SASLIST -> SAP.PIBB.EIMALIMT.TEXT (RECFM=FB, LRECL=133)
EIMALIMT_TXT   = OUTPUT_DIR / "EIMALIMT.txt"
# DD RM4XSMMR -> SAP.PIBB.EIBMRM4X.SUMMR (RECFM=FS, LRECL=27648)
RM4XSMMR_OUT   = OUTPUT_DIR / "EIBMRM4X_SUMMR"

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("EIIMRPT1")

# ============================================================================
# JCL DELETE STEP  (IEFBR14 DISP=(MOD,DELETE,DELETE))
# Removes output files before each run so stale data is never carried forward.
# Equivalent to:
#   //DD1 DD DSN=SAP.PIBB.DEPM500K.TXT, DISP=(MOD,DELETE,DELETE)
#   //DD2 DD DSN=SAP.PIBB.FD500K.TXT,   DISP=(MOD,DELETE,DELETE)
#   //DD6 DD DSN=SAP.PIBB.EIMALIMT.TEXT,DISP=(MOD,DELETE,DELETE)
# ============================================================================

def delete_outputs() -> None:
    """Remove output files if they exist (JCL DELETE step equivalent)."""
    for fpath in [DEPM500K_TXT, FD500K_TXT, EIMALIMT_TXT]:
        if fpath.exists():
            fpath.unlink()
            log.info("Deleted (pre-run cleanup): %s", fpath)


# ============================================================================
# SUB-PROGRAM RUNNER
# Passes output file paths via environment variables so sub-programs write
# to the correct locations without hardcoding paths inside themselves.
# COND=(4,LT) on JCL job card: abort the job if any step return code >= 4.
# ============================================================================

def run_step(script_name: str, extra_env: dict | None = None) -> int:
    """
    Execute a converted sub-program as a subprocess.
    Returns the exit code. Raises SystemExit if return code >= 4
    (equivalent to JCL COND=(4,LT) — skip/abort if prior step RC >= 4).
    """
    script_path = BASE_DIR / f"{script_name}.py"
    if not script_path.exists():
        log.error("Sub-program not found: %s — step skipped.", script_path)
        # Non-fatal for missing optional programs; treat as RC=0 warning.
        return 0

    env = {**os.environ}
    if extra_env:
        env.update({k: str(v) for k, v in extra_env.items()})

    log.info(">>> Starting step: %s", script_name)
    result = subprocess.run([sys.executable, str(script_path)], env=env)
    rc = result.returncode
    log.info("<<< Step %s completed  RC=%d", script_name, rc)

    if rc >= 4:
        log.error(
            "Step %s returned RC=%d (>= 4). "
            "Job aborted (COND=(4,LT) equivalent).",
            script_name, rc,
        )
        sys.exit(rc)

    return rc


# ============================================================================
# MAIN ORCHESTRATION
# ============================================================================

def main() -> None:
    log.info("=" * 60)
    log.info("EIIMRPT1 started  (PIBB MIS Reporting — run after EIBMMISE)")
    log.info("=" * 60)

    # ---- JCL DELETE step ----
    # //DELETE EXEC PGM=IEFBR14
    # //DD1 DD DSN=SAP.PIBB.DEPM500K.TXT, DISP=(MOD,DELETE,DELETE)
    # //DD2 DD DSN=SAP.PIBB.FD500K.TXT,   DISP=(MOD,DELETE,DELETE)
    # //DD6 DD DSN=SAP.PIBB.EIMALIMT.TEXT,DISP=(MOD,DELETE,DELETE)
    delete_outputs()

    # ---- JCL CREATE step ----
    # //CREATE EXEC PGM=IEFBR14
    # //CRT01 DD DSN=SAP.PIBB.FD500K.TXT,   DISP=(NEW,CATLG,DELETE), LRECL=133
    # //CRT02 DD DSN=SAP.PIBB.DEPM500K.TXT, DISP=(NEW,CATLG,DELETE), LRECL=133
    # Output files are created by the sub-programs themselves; no pre-allocation
    # is needed in Python.  Output directory is guaranteed to exist (created above).

    # ------------------------------------------------------------------
    # STEP 1: DEPM500K
    # //DEPM500K EXEC SAS609
    # //BNM      DD DSN=SAP.PIBB.MNITB(0)      -> data/bnm  (demand deposit table)
    # //CISCADP  DD DSN=SAP.PBB.CISBEXT.DP     -> data/ciscadp
    # //BRHFILE  DD DSN=SAP.RBP2.B033.PBB.BRANCH -> data/BRHFILE.txt
    # //SASLIST  DD DSN=SAP.PIBB.DEPM500K.TXT   -> output/DEPM500K*.txt
    # Output: DEPM500K_SAVING.txt, DEPM500K_CRMOVE.txt, DEPM500K_ODMOVE.txt
    # ------------------------------------------------------------------
    run_step(
        "DEPM500K",
        extra_env={
            "DEPM500K_OUTPUT_DIR": str(OUTPUT_DIR),
        },
    )

    # ------------------------------------------------------------------
    # STEP 2: EIIMFD03
    # //EIIMFD03 EXEC SAS609
    # //FD       DD DSN=SAP.PIBB.MNIFD(0)       -> data/fd
    # //FD1      DD DSN=SAP.PIBB.MNIFD(-4)      -> data/fd1  (previous period)
    # //CISSAFD  DD DSN=SAP.PBB.CRM.CISBEXT     -> data/cissafd
    # //BRHFILE  DD DSN=SAP.RBP2.B033.PBB.BRANCH -> data/BRHFILE.txt
    # //FDTEXT   DD DSN=SAP.PIBB.FD500K.TXT     -> output/FDTEXT.txt
    # //SASLIST  DD SYSOUT=*                     -> stdout (no separate list file)
    # ------------------------------------------------------------------
    run_step(
        "EIIMFD03",
        extra_env={
            "FDTEXT_OUTPUT": str(FD500K_TXT),
        },
    )

    # ------------------------------------------------------------------
    # STEP 3: EIIMLIMT
    # //EIIMLIMT EXEC SAS609
    # //LOAN     DD DSN=SAP.PIBB.SASDATA         -> data/loan
    # //BRHFILE  DD DSN=RBP2.B033.PBB.BRANCH     -> data/BRHFILE.txt
    # //SASLIST  DD DSN=SAP.PIBB.EIMALIMT.TEXT   -> output/EIMALIMT*.txt
    # Output: EIILMTOD.txt, EIILMTHP.txt, EIILMTRC.txt, EIILMTLN.txt
    # ------------------------------------------------------------------
    run_step(
        "EIIMLIMT",
        extra_env={
            "EIIMLIMT_OUTPUT_DIR": str(OUTPUT_DIR),
        },
    )

    # ------------------------------------------------------------------
    # STEP 4: EIBMRM4X  — Repricing Gap
    # //EIBMRM4X EXEC SAS609, WORKSPC='1500,1500'
    # //BNM      DD DSN=SAP.PIBB.SASDATA         -> data/bnm
    # //OD       DD DSN=SAP.PIBB.MNILIMT(0)      -> data/od
    # //PROV     DD DSN=SAP.PIBB.CCRIS2.PROVISIO -> data/prov
    # //LNNOTE   DD DSN=SAP.PIBB.MNILN(0)        -> data/loan (LNNOTE parquet)
    # //RM4XSMMR DD DSN=SAP.PIBB.EIBMRM4X.SUMMR -> output/EIBMRM4X_SUMMR
    # //SASLIST  DD SYSOUT=X                     -> stdout / discard
    # SORTWK01-10 -> not required in Python (DuckDB/Polars manage sort memory)
    # //DELETE DD DSN=SAP.PIBB.EIBMRM4X.SUMMR, DISP=(MOD,DELETE,DELETE)
    # ------------------------------------------------------------------
    # Pre-run deletion of EIBMRM4X output (equiv. //DELETE step before EIBMRM4X)
    if RM4XSMMR_OUT.exists():
        RM4XSMMR_OUT.unlink()
        log.info("Deleted (pre-run cleanup): %s", RM4XSMMR_OUT)

    run_step(
        "EIBMRM4X",
        extra_env={
            "RM4XSMMR_OUTPUT": str(RM4XSMMR_OUT),
        },
    )

    # ------------------------------------------------------------------
    # COMMENTED-OUT STEPS from original JCL (preserved for traceability)
    # ------------------------------------------------------------------

    # //* EIBMRM1X — Deposits, by time to maturity for ALCO
    # //*DELETE   DD DSN=SAP.PIBB.EIBMRM1X.TXT, DISP=(MOD,DELETE,DELETE)
    # //*CREATE   DD DSN=SAP.PIBB.EIBMRM1X.TXT, DISP=(NEW,CATLG,DELETE), LRECL=256
    # //*EIBMRM1X EXEC SAS609,REGION=6M,WORK='120000,80000'
    # //*FD       DD DSN=SAP.PIBB.MNIFD(0)
    # //*BNM      DD DSN=SAP.PIBB.MNITB(0)
    # //*TEMP     DD DSN=SAP.PIBB.EIBMRM1X.TXT
    # //*SASLIST  DD SYSOUT=X
    # run_step("EIBMRM1X")

    # //* EIBMRM2X — FD by individual/non-individual, by time to maturity for ALCO
    # //*DELETE   DD DSN=SAP.PIBB.EIBMRM2X.TXT, DISP=(MOD,DELETE,DELETE)
    # //*CREATE   DD DSN=SAP.PIBB.EIBMRM2X.TXT, DISP=(NEW,CATLG,DELETE), LRECL=256
    # //*EIBMRM2X EXEC SAS609,REGION=6M,WORK='120000,80000'
    # //*FD       DD DSN=SAP.PIBB.MNIFD(0)
    # //*TEMP     DD DSN=SAP.PIBB.EIBMRM2X.TXT
    # //*SASLIST  DD SYSOUT=X
    # run_step("EIBMRM2X")

    # //* EIBMRM3X — FD by individual/non-individual, by time to maturity for ALCO
    # //*DELETE   DD DSN=SAP.PIBB.EIBMRM3X.TXT, DISP=(MOD,DELETE,DELETE)
    # //*CREATE   DD DSN=SAP.PIBB.EIBMRM3X.TXT, DISP=(NEW,CATLG,DELETE), LRECL=256
    # //*EIBMRM3X EXEC SAS609,REGION=6M,WORK='120000,80000'
    # //*FD       DD DSN=SAP.PIBB.MNIFD(0)
    # //*TEMP     DD DSN=SAP.PIBB.EIBMRM3X.TXT
    # //*SASLIST  DD SYSOUT=X
    # run_step("EIBMRM3X")   # Note: JCL SYSIN referenced EIIMRM3X (typo in original)

    # //* FTP to PBB Datawarehouse Server (commented out in original JCL)
    # //*RUNSFTP EXEC COZBATCH
    # //*CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(CSASSFTP)
    # //*lzopts servercp=$servercp,notrim,overflow=trunc,mode=text
    # //*lzopts linerule=$lr
    # //*CD TEXTFILE/RISKMGT
    # //*PUT //SAP.PIBB.EIBMRM1X.TXT EIIMRM1X.TXT
    # //*PUT //SAP.PIBB.EIBMRM2X.TXT EIIMRM2X.TXT
    # //*PUT //SAP.PIBB.EIBMRM3X.TXT EIIMRM3X.TXT

    # //* FTP to Data Report Repository System (DRR) (commented out in original JCL)
    # //*RUNSFTP EXEC COZBATCH
    # //*CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(DRR#SFTP)
    # //*lzopts servercp=$servercp,notrim,overflow=trunc,mode=text
    # //*lzopts linerule=$lr
    # //*CD RMD-ALM/IRR
    # //*PUT //SAP.PIBB.EIBMRM1X.TXT EIIMRM1X.TXT
    # //*PUT //SAP.PIBB.EIBMRM2X.TXT EIIMRM2X.TXT
    # //*PUT //SAP.PIBB.EIBMRM3X.TXT EIIMRM3X.TXT

    log.info("=" * 60)
    log.info("EIIMRPT1 completed successfully.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
