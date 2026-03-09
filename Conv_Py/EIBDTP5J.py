#!/usr/bin/env python3
"""
Program  : EIBDTP5J.py
Purpose  : Daily Top 50 Depositors RM & FCY — Job orchestrator.
           Runs after EIBDDEPF.
           Executes EIBDTP50 (main processing program) then transfers
           output reports to the Data Report Repository (DRR) via SFTP.

           Output files:
             TOP50I.TXT  → INDVTP50@DD.MM.YY.TXT   (Individual depositors)
             TOP50C.TXT  → CORPTP50@DD.MM.YY.TXT   (Corporate depositors)
             TOP50S.TXT  → SUBSTP50@DD.MM.YY.TXT   (Subsidiaries)

           DRR destinations:
             FD-BNM REPORTING/PBB/TOP DEPOSITORS              → INDVTP50
             FD-BNM REPORTING/PBB/TOP DEPOSITORS/CORP DEPOSITORS → CORPTP50
             FD-BNM REPORTING/PBB/TOP DEPOSITORS/RETAIL DEPOSITORS → SUBSTP50
             CPSD/CORPTP50                                     → CORPTP50

           Dependencies:
             EIBDTP50  - Main Top 50 depositor report program
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import sys
import logging
from datetime import datetime
from pathlib import Path

# ============================================================================
# DEPENDENCY IMPORTS
# ============================================================================
from EIBDTP50 import main as eibdtp50_main

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR   = Path(".")
OUTPUT_DIR = BASE_DIR / "output"

# Output report files (produced by EIBDTP50)
FD11TEXT_PATH = OUTPUT_DIR / "TOP50I.TXT"   # SAP.PBB.TOP50I.DAILY(+1)
FD12TEXT_PATH = OUTPUT_DIR / "TOP50C.TXT"   # SAP.PBB.TOP50C.DAILY(+1)
FD2TEXT_PATH  = OUTPUT_DIR / "TOP50S.TXT"   # SAP.PBB.TOP50S.DAILY(+1)

# DRR SFTP connection parameters (populate from environment or config)
DRR_HOST = ""   # DRR server hostname
DRR_USER = ""   # DRR SFTP username
DRR_PASS = ""   # DRR SFTP password (prefer key-based auth in production)

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
# FTP / FILE TRANSFER STEP
# Mirrors //RUNSFTP EXEC COZBATCH with DRR#SFTP parmlib.
#
# Commented FTP to datawarehouse (CSASSFTP) has been retained as comments:
# //*RUNSFTP EXEC COZBATCH (CSASSFTP section — not active)
# //*cd TEXTFILE/FIN
# //*PUT TOP50I.TXT  INDVTP50@DD.MM.YY.TXT
# //*PUT TOP50C.TXT  CORPTP50@DD.MM.YY.TXT
# //*PUT TOP50S.TXT  SUBSTP50@DD.MM.YY.TXT
# //*cd ../RISKMGT   → SUBSTP50
# //*cd ../CPSD      → CORPTP50
# ============================================================================

def ftp_outputs(run_date: datetime) -> None:
    """
    Transfer output files to the Data Report Repository (DRR) via SFTP.
    Mirrors the active //RUNSFTP EXEC COZBATCH (DRR#SFTP) step.

    Remote file names follow OPC substitution pattern:
      %ODD = day (2-digit), %OMM = month (2-digit), %OYY = 2-digit year.

    DRR destinations:
      FD-BNM REPORTING/PBB/TOP DEPOSITORS              ← INDVTP50
      FD-BNM REPORTING/PBB/TOP DEPOSITORS/CORP DEPOSITORS ← CORPTP50
      FD-BNM REPORTING/PBB/TOP DEPOSITORS/RETAIL DEPOSITORS ← SUBSTP50
      CPSD/CORPTP50                                    ← CORPTP50
    """
    dd  = run_date.strftime("%d")
    mm  = run_date.strftime("%m")
    yy  = run_date.strftime("%y")

    indv_name = f"INDVTP50@{dd}.{mm}.{yy}.TXT"
    corp_name = f"CORPTP50@{dd}.{mm}.{yy}.TXT"
    subs_name = f"SUBSTP50@{dd}.{mm}.{yy}.TXT"

    transfers = [
        # (local_path, remote_dir, remote_name)
        (FD11TEXT_PATH, "FD-BNM REPORTING/PBB/TOP DEPOSITORS",                   indv_name),
        (FD12TEXT_PATH, "FD-BNM REPORTING/PBB/TOP DEPOSITORS/CORP DEPOSITORS",   corp_name),
        (FD2TEXT_PATH,  "FD-BNM REPORTING/PBB/TOP DEPOSITORS/RETAIL DEPOSITORS", subs_name),
        (FD12TEXT_PATH, "CPSD/CORPTP50",                                          corp_name),
    ]

    log.info("FTP step: transferring output files to DRR (stub — implement SFTP).")

    # Implement SFTP transfer using paramiko:
    # import paramiko
    # ssh = paramiko.SSHClient()
    # ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # ssh.connect(hostname=DRR_HOST, username=DRR_USER, password=DRR_PASS)
    # sftp = ssh.open_sftp()
    # for local_path, remote_dir, remote_name in transfers:
    #     remote_path = f"{remote_dir}/{remote_name}"
    #     sftp.put(str(local_path), remote_path)
    #     log.info("Transferred %s → %s", local_path.name, remote_path)
    # sftp.close()
    # ssh.close()

    for local_path, remote_dir, remote_name in transfers:
        log.info("[STUB] Would transfer %s → %s/%s", local_path.name, remote_dir, remote_name)


# ============================================================================
# MAIN — Job step execution (mirrors JCL job step ordering)
# COND=(4,LT) equivalent: abort on step failure (sys.exit(8)).
# ============================================================================

def main() -> None:
    log.info("EIBDTP5J started.")

    run_date = datetime.today()

    # ----------------------------------------------------------------
    # EIBDTP50 — Main Top 50 depositor extraction and report generation
    # //EIBDTP50 EXEC SAS609
    # ----------------------------------------------------------------
    try:
        eibdtp50_main()
        log.info("EIBDTP50 completed.")
    except Exception as exc:
        log.error("EIBDTP50 failed: %s", exc)
        sys.exit(8)

    # ----------------------------------------------------------------
    # RUNSFTP — Transfer reports to DRR
    # //RUNSFTP EXEC COZBATCH (active DRR#SFTP section)
    # ----------------------------------------------------------------
    try:
        ftp_outputs(run_date)
        log.info("FTP step completed.")
    except Exception as exc:
        log.error("FTP step failed: %s", exc)
        sys.exit(8)

    log.info("EIBDTP5J completed successfully.")


if __name__ == "__main__":
    main()
