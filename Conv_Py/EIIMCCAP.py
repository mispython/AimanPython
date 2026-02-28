# !/usr/bin/env python3
"""
Program: EIIMCCAP.py
Purpose: Master Control / Job Control script to orchestrate the full CAP
            computation pipeline for PIBB and PIBB(STAFF) hire-purchase loans.
         Equivalent to the JCL job EIIMCCAP which sequentially executes:
           EIICAP41  → CAP report by campaign (PIBB)
           EIICAPS1  → CAP report by campaign (PIBB Staff)
           EIICAP42  → CAP movement by branch (PIBB)
           EIICAPS2  → CAP movement by branch (PIBB Staff)
           EIICAP43  → CAP movement by category (PIBB)
           EIICAPS3  → CAP movement by category (PIBB Staff)
           EIICAP44  → Interface CAP to CCRIS for all accounts
"""

import os
import sys
import traceback
from datetime import datetime

# ─────────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────────
BASE_DIR   = r"C:/data"
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
LOG_DIR    = os.path.join(BASE_DIR, "logs")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR,    exist_ok=True)

LOG_PATH = os.path.join(LOG_DIR, "EIIMCCAP.log")

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
# Step executor – runs each program module as a
# callable and captures any exception so that
# the log always records pass/fail per step.
# ─────────────────────────────────────────────
def run_step(step_name: str, module_main):
    log(f"START  {step_name}")
    try:
        module_main()
        log(f"OK     {step_name}")
        return True
    except Exception:
        log(f"FAILED {step_name}")
        log(traceback.format_exc())
        return False


# ─────────────────────────────────────────────
# Import each step as a callable.
# Every converted SAS program exposes its logic
# at module level; we wrap each in a lambda that
# re-executes the module body via runpy so that
# the module-level code is treated as main().
# Alternatively, each program can expose a
# main() function – preferred pattern shown here.
# ─────────────────────────────────────────────

def _exec_module(module_name: str):
    """Execute a peer module by name using runpy."""
    import runpy
    runpy.run_module(module_name, run_name="__main__", alter_sys=True)


# ─────────────────────────────────────────────
# Pipeline definition – mirrors JCL step order
# //* ESMR 2014-694 CHANGE THE COMPUTATION OF CAP
# ─────────────────────────────────────────────
PIPELINE = [
    # Step name       Module (program file without .py)
    ("EIICAP41",  "EIICAP41"),   # CAP report by campaign         – PIBB
    ("EIICAPS1",  "EIICAPS1"),   # CAP report by campaign         – PIBB Staff
    ("EIICAP42",  "EIICAP42"),   # CAP movement by branch         – PIBB
    ("EIICAPS2",  "EIICAPS2"),   # CAP movement by branch         – PIBB Staff
    ("EIICAP43",  "EIICAP43"),   # CAP movement by category       – PIBB
    ("EIICAPS3",  "EIICAPS3"),   # CAP movement by category       – PIBB Staff
    # INTERFACE CAP TO ECCRIS FOR ALL ACCOUNTS
    ("EIICAP44",  "EIICAP44"),   # Interface CAP to CCRIS         – PIBB + Staff
]


# ─────────────────────────────────────────────
# Main orchestrator
# ─────────────────────────────────────────────
def main():
    log("=" * 60)
    log("JOB  EIIMCCAP  START")
    log("=" * 60)

    failed_steps = []

    for step_name, module_name in PIPELINE:
        success = run_step(
            step_name,
            lambda m=module_name: _exec_module(m)
        )
        if not success:
            failed_steps.append(step_name)
            # Mirror JCL behaviour: a step failure stops dependent steps.
            # All steps in this job are sequential with dependencies, so
            # abort the pipeline on first failure (COND checking equivalent).
            log(f"ABEND  Pipeline halted after step {step_name}.")
            break

    log("=" * 60)
    if failed_steps:
        log(f"JOB  EIIMCCAP  ENDED WITH ERRORS – Failed steps: {failed_steps}")
        log("=" * 60)
        sys.exit(1)
    else:
        log("JOB  EIIMCCAP  COMPLETED SUCCESSFULLY")
        log("=" * 60)


if __name__ == "__main__":
    main()
