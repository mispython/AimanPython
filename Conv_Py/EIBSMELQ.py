#!/usr/bin/env python3
"""
Program : EIBSMELQ.py
Remarks : JCL ORCHESTRATOR – NEW LIQUIDITY FRAMEWORK FOR SME (PBB)
          (TO RUN AS MONTH END)
          Equivalent to JCL job EIBSMELQ which:
            1. Deletes/recreates SAP.PBB.SMELN.MNILIQ
            2. Sets SDESC = 'PUBLIC BANK BERHAD' and other macro variables
            3. Calls %INC PGM(EISMELQE) to produce the SME liquidity report
            4. FTPs the output to PBB DataWarehouse server as SMEPBB.TXT
"""

import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb

# ============================================================================
# PATH SETUP
# ============================================================================

BASE_DIR    = Path(os.environ.get("BASE_DIR",    "/data"))
# PBB-specific library paths (mapped from JCL DD statements)
# //DEPOSIT  DD DSN=SAP.PBB.MNITB(0)
# //LOAN     DD DSN=SAP.PBB.MNILN(0)
# //BNM1     DD DSN=SAP.PBB.SASDATA
# //MNILQ    DD DSN=SAP.PBB.SMELN.MNILIQ
DEPOSIT_DIR = Path(os.environ.get("DEPOSIT_DIR", BASE_DIR / "PBB" / "MNITB"))
LOAN_DIR    = Path(os.environ.get("LOAN_DIR",    BASE_DIR / "PBB" / "MNILN"))
BNM1_DIR    = Path(os.environ.get("BNM1_DIR",    BASE_DIR / "PBB" / "SASDATA"))
BNM_DIR     = Path(os.environ.get("BNM_DIR",     BASE_DIR / "BNM"))
OUTPUT_DIR  = Path(os.environ.get("OUTPUT_DIR",  BASE_DIR / "output"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Output file: SAP.PBB.SMELN.MNILIQ → MNILQ_PBB.txt
MNILQ_PATH  = OUTPUT_DIR / "MNILQ_PBB.txt"

# ============================================================================
# STEP: DELETE  (IEFBR14 DISP=(MOD,DELETE,DELETE))
# Equivalent to deleting SAP.PBB.SMELN.MNILIQ before the run.
# ============================================================================

def step_delete() -> None:
    """Remove the output file if it exists (mirrors IEFBR14 DELETE step)."""
    if MNILQ_PATH.exists():
        MNILQ_PATH.unlink()
        print(f"Deleted existing output: {MNILQ_PATH}")


# ============================================================================
# READ REPTDATE FROM DEPOSIT LIBRARY
# (DATA BNM.REPTDATE; SET DEPOSIT.REPTDATE; ...)
# ============================================================================

def load_reptdate_from_deposit() -> dict:
    """
    Read REPTDATE from DEPOSIT.REPTDATE parquet (SAP.PBB.MNITB(0)) and
    derive macro variables including SDESC for PBB.
    """
    reptdate_parquet = DEPOSIT_DIR / "REPTDATE.parquet"
    conn = duckdb.connect()
    row = conn.execute(
        f"SELECT reptdate FROM read_parquet('{reptdate_parquet}') LIMIT 1"
    ).fetchone()
    conn.close()

    if row is None:
        raise RuntimeError("REPTDATE table is empty")

    reptdate: date = row[0]

    day = reptdate.day
    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'

    return {
        "reptdate":  reptdate,
        "nowk":      nowk,
        "reptyear":  str(reptdate.year),
        "reptmon":   f"{reptdate.month:02d}",
        "reptday":   f"{reptdate.day:02d}",
        "rdate":     reptdate.strftime("%d/%m/%Y"),
        # SDESC='PUBLIC BANK BERHAD' (padded to $26. in SAS → stored as-is here)
        "sdesc":     "PUBLIC BANK BERHAD",
    }


# ============================================================================
# MAIN
# ============================================================================

def main() -> int:
    # ── STEP DELETE ──────────────────────────────────────────────────────────
    step_delete()

    # ── Read REPTDATE and set macro-equivalent environment variables ──────────
    rd = load_reptdate_from_deposit()

    # Propagate as environment variables for EISMELQE (mirrors CALL SYMPUT)
    os.environ["REPTMON"]  = rd["reptmon"]
    os.environ["NOWK"]     = rd["nowk"]
    os.environ["RDATE"]    = rd["rdate"]
    os.environ["SDESC"]    = rd["sdesc"]
    os.environ["BNM1_DIR"] = str(BNM1_DIR)
    os.environ["BNM_DIR"]  = str(BNM_DIR)
    os.environ["OUTPUT_DIR"] = str(OUTPUT_DIR)

    # Override the output path env var so EISMELQE writes to PBB-specific file
    os.environ["MNILQ_FILENAME"] = MNILQ_PATH.name

    print(f"REPTDATE : {rd['reptdate']}")
    print(f"REPTMON  : {rd['reptmon']}")
    print(f"NOWK     : {rd['nowk']}")
    print(f"RDATE    : {rd['rdate']}")
    print(f"SDESC    : {rd['sdesc']}")

    # ── %INC PGM(EISMELQE) – invoke the shared processing program ───────────
    # Mirrors JCL step EIBSMELQ EXEC SAS609 ... %INC PGM(EISMELQE)
    result = subprocess.run(
        [sys.executable, "EISMELQE.py"],
        env=os.environ.copy(),
    )
    rc = result.returncode
    if rc >= 4:
        print(f"EISMELQE returned RC={rc} – aborting.", file=sys.stderr)
        return rc

    print(f"EISMELQE completed with RC={rc}")

    # ── FTP step (RUNSFTP EXEC COZBATCH) ────────────────────────────────────
    # Transfers SAP.PBB.SMELN.MNILIQ to PBB DataWarehouse server as SMEPBB.TXT
    # PUT //SAP.PBB.SMELN.MNILIQ SMEPBB.TXT
    # This step is environment-specific; implement FTP/SFTP transfer here
    # using parameterised credentials from the runtime environment.
    print("FTP step: transfer MNILQ_PBB.txt → SMEPBB.TXT (implement per site SFTP config)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
