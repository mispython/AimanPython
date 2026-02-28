# !/usr/bin/env python3
"""
Program  : EIIMNPLP.py
Purpose  : Master control / job orchestrator for the PIBB NPL monthly
            reporting pipeline. Equivalent to JCL job EIIMNPLP which:
             1. Derives REPTDATE macro variables
             2. Runs NPL detail reports (LNICDN10, LNICDQ10, LNICDW10)
             3. Runs IIS/SP1/SP2 provision reports (EIIMNPP1)
             4. Splits and routes IIS → CAC files (EIIMNPP2)
             5. Splits and routes SP1 → CAC files (EIIMNPP3)
             6. Splits and routes SP2 → CAC files (EIIMNPP4)
             7. Splits and routes ARREARS → CAC files (EIIMNPP5)
             8. Splits and routes WRITTEN-OFF HP → CAC files (EIIMNPP6)
             9. Splits and routes HP F/I/R/T & RESTRUCTURED → CAC files (EIIMNPP7)
"""

import os
import sys
import traceback
import runpy
from datetime import date, datetime

import duckdb

# ─────────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────────
BASE_DIR   = r"C:/data"
LOAN_DIR   = os.path.join(BASE_DIR, "loan")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
LOG_DIR    = os.path.join(BASE_DIR, "logs")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR,    exist_ok=True)

LOG_PATH = os.path.join(LOG_DIR, "EIIMNPLP.log")

# ─────────────────────────────────────────────
# Intermediate RPS text files
# (JCL DD names: OUTFILE, OUTWOFF, OUTHP, RPSIIS, RPSSP1, RPSSP2)
# These are produced by the SAS report programs and consumed by the
# EIIMNPP2-7 splitter programs as their INFIL01 inputs.
# ─────────────────────────────────────────────
NPL_ARREARS_RPS = os.path.join(OUTPUT_DIR, "NPL_ARREARS.rps")    # LNICDQ10 → EIIMNPP5
NPL_WOFFHP_RPS  = os.path.join(OUTPUT_DIR, "NPL_WOFFHP.rps")     # LNICDW10 → EIIMNPP6
NPL_HP_RPS      = os.path.join(OUTPUT_DIR, "NPL_HP.rps")          # LNICDN10 → EIIMNPP7
NPL_IIS_RPS     = os.path.join(OUTPUT_DIR, "NPL_IIS.rps")         # EIIMNPP1 → EIIMNPP2
NPL_SP1_RPS     = os.path.join(OUTPUT_DIR, "NPL_SP1.rps")         # EIIMNPP1 → EIIMNPP3
NPL_SP2_RPS     = os.path.join(OUTPUT_DIR, "NPL_SP2.rps")         # EIIMNPP1 → EIIMNPP4

# Final CAC-combined output files
NPL_IIS_CAC     = os.path.join(OUTPUT_DIR, "EIIMNPP2_COMBINED.txt")
NPL_SP1_CAC     = os.path.join(OUTPUT_DIR, "EIIMNPP3_COMBINED.txt")
NPL_SP2_CAC     = os.path.join(OUTPUT_DIR, "EIIMNPP4_COMBINED.txt")
NPL_ARREARS_CAC = os.path.join(OUTPUT_DIR, "EIIMNPP5_COMBINED.txt")
NPL_WOFFHP_CAC  = os.path.join(OUTPUT_DIR, "EIIMNPP6_COMBINED.txt")
NPL_HP_CAC      = os.path.join(OUTPUT_DIR, "EIIMNPP7_COMBINED.txt")

con = duckdb.connect()

# ─────────────────────────────────────────────
# Logging helper
# ─────────────────────────────────────────────
def log(msg: str):
    ts    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{ts}] {msg}"
    print(entry)
    with open(LOG_PATH, "a", encoding="utf-8") as lf:
        lf.write(entry + "\n")

# ─────────────────────────────────────────────
# Step executor
# ─────────────────────────────────────────────
def run_step(step_name: str, module_name: str) -> bool:
    log(f"START  {step_name}  ({module_name})")
    try:
        runpy.run_module(module_name, run_name="__main__", alter_sys=True)
        log(f"OK     {step_name}")
        return True
    except Exception:
        log(f"FAILED {step_name}")
        log(traceback.format_exc())
        return False

# ─────────────────────────────────────────────
# REPTDATE – derive macro variables
# Equivalent to the inline DATA REPTDATE step in the JCL SYSIN
#
# SELECT(DAY(REPTDATE)):
#   WHEN (8)  WK = '1'
#   WHEN (15) WK = '2'
#   WHEN (22) WK = '3'
#   OTHERWISE WK = '4'
# ─────────────────────────────────────────────
def derive_reptdate() -> dict:
    reptdate_path = os.path.join(LOAN_DIR, "REPTDATE.parquet")
    df = con.execute(f"SELECT REPTDATE FROM '{reptdate_path}'").pl()
    reptdate_val = df["REPTDATE"][0]
    if isinstance(reptdate_val, int):
        reptdate_val = date.fromordinal(reptdate_val)

    day = reptdate_val.day
    if day == 8:
        wk = "1"
    elif day == 15:
        wk = "2"
    elif day == 22:
        wk = "3"
    else:
        wk = "4"

    macros = {
        "NOWK":     wk,
        "RDATE":    reptdate_val.strftime("%d/%m/%y"),     # DDMMYY8.
        "REPTYEAR": str(reptdate_val.year),                # YEAR4.
        "REPTMON":  f"{reptdate_val.month:02d}",           # Z2.
        "REPTDAY":  f"{reptdate_val.day:02d}",             # Z2.
    }

    log(f"REPTDATE derived: RDATE={macros['RDATE']}  REPTMON={macros['REPTMON']}"
        f"  REPTYEAR={macros['REPTYEAR']}  NOWK={macros['NOWK']}")
    return macros

# ─────────────────────────────────────────────
# Propagate macro variables to child programs
# via environment variables so each module can
# pick them up without needing direct coupling.
# ─────────────────────────────────────────────
def export_macros(macros: dict):
    for key, val in macros.items():
        os.environ[key] = str(val)

# ─────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────
def main():
    log("=" * 70)
    log("JOB  EIIMNPLP  START")
    log("=" * 70)

    # ── Step 0: REPTDATE ──────────────────────────────────────────────
    try:
        macros = derive_reptdate()
        export_macros(macros)
    except Exception:
        log("FAILED  REPTDATE derivation")
        log(traceback.format_exc())
        sys.exit(1)

    # ── Pipeline steps ────────────────────────────────────────────────
    # Comments mirror the JCL structure exactly: active steps run;
    # commented-out steps in the JCL are preserved as commented entries
    # with their module names so they can be re-enabled when needed.

    pipeline = [

        # /* %INC PGM(LNCCD006);
        # PROC DATASETS LIB=WORK KILL NOLIST; */
        # (LNCCD006 is commented out in the original JCL – not executed)
        # ("LNCCD006", "LNCCD006"),

        # /**  LOANS IN NPL REPORT - BY CAC **/
        # %INC PGM(LNICDN10);
        ("LNICDN10",  "LNICDN10"),

        # /**  MONTHLY DETAIL LISTING FOR NPL ACCOUNTS  **/
        # /* %INC PGM(LNICDR10);
        # PROC DATASETS LIB=WORK KILL NOLIST; RUN;  */
        # (LNICDR10 is commented out in the original JCL – not executed)
        # ("LNICDR10", "LNICDR10"),

        # /* MTHLY DETAIL LISTING FOR NPL 3 MONTHS & ABOVE FOR ALL ACCT */
        # %INC PGM(LNICDQ10);
        ("LNICDQ10",  "LNICDQ10"),

        # /* MTHLY DETAIL LISTING FOR WRITTEN OFF HP 983 & 993 ACCT */
        # %INC PGM(LNICDW10);
        ("LNICDW10",  "LNICDW10"),

        # //*********************************************************************
        # //*  PROGRAM TO PRINT THE IIS, SP1, SP2 INTO A TEXT FILE FOR RPS USE
        # //*  (SMR A264)
        # //*********************************************************************
        # //EIFMNPP1 EXEC SAS609
        ("EIIMNPP1",  "EIIMNPP1"),

        # //* TO PRINT IIS INTO RPS FORMAT
        # //EIFMNPP2 EXEC SAS609
        # //INFIL01  DD DSN=SAP.PIBB.NPL.IIS.RPS
        ("EIIMNPP2",  "EIIMNPP2"),

        # //* TO PRINT SP1 INTO RPS FORMAT
        # //EIFMNPP3 EXEC SAS609
        # //INFIL01  DD DSN=SAP.PIBB.NPL.SP1.RPS
        ("EIIMNPP3",  "EIIMNPP3"),

        # //* TO PRINT SP2 INTO RPS FORMAT
        # //EIFMNPP4 EXEC SAS609
        # //INFIL01  DD DSN=SAP.PIBB.NPL.SP2.RPS
        ("EIIMNPP4",  "EIIMNPP4"),

        # //* TO PRINT ARREARS INTO RPS FORMAT
        # //EIFMNPP5 EXEC SAS609
        # //INFIL01  DD DSN=SAP.PIBB.NPL.ARREARS.RPS
        ("EIIMNPP5",  "EIIMNPP5"),

        # //* TO PRINT WRITTEN OFF HP INTO RPS FORMAT
        # //EIFMNPP6 EXEC SAS609
        # //INFIL01  DD DSN=SAP.PIBB.NPL.WOFFHP.RPS
        ("EIIMNPP6",  "EIIMNPP6"),

        # //* TO PRINT HP A/C WITH F/I/R/T & RESTRUCTURED INTO RPS FORMAT
        # //EIFMNPP7 EXEC SAS609
        # //INFIL01  DD DSN=SAP.PIBB.NPL.HP.RPS
        ("EIIMNPP7",  "EIIMNPP7"),
    ]

    failed_steps: list[str] = []

    for step_name, module_name in pipeline:
        ok = run_step(step_name, module_name)
        if not ok:
            failed_steps.append(step_name)
            log(f"ABEND  Pipeline halted after step {step_name}.")
            break

    # ── Final status ──────────────────────────────────────────────────
    log("=" * 70)
    if failed_steps:
        log(f"JOB  EIIMNPLP  ENDED WITH ERRORS – Failed: {failed_steps}")
        log("=" * 70)
        sys.exit(1)
    else:
        log("JOB  EIIMNPLP  COMPLETED SUCCESSFULLY")
        log("=" * 70)


if __name__ == "__main__":
    main()
