#!/usr/bin/env python3
"""
Program : EIBQ1241.py
Purpose : Orchestrator – computes reporting-date macro variables
            (equivalent to SAS REPTDATE DATA step + CALL SYMPUT),
            then invokes sub-programs in order:
                P124DL1B → P124DL2B → P124RDLB →
                P124DAL1 → P124DAL2 → P124DAL3 → P124RDAL

          The original JCL resolved REPTDATE as the last day of the
            previous month relative to TODAY(), derived the week number
            (WK) from that date's day-of-month, and passed all computed
            values as global macro variables to the included SAS programs.

          In Python, computed values are propagated via os.environ so
            that each sub-program can read them with os.environ.get().

Dependency: P124DL1B  – BNM.ALWWK (weekly, zero-amount)
            P124DL2B  – BNM.ALWWK monthly merge (zero-amount)
            P124RDLB  – RDALWK report (ASA carriage-control)
            P124DAL1  – BNM.ALW (weekly actuals)
            P124DAL2  – BNM.ALW monthly merge
            P124DAL3  – BNM.ALW quarterly append
            P124RDAL  – RDAL report (AL / OB / SP sections)
"""

import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

# ── Path configuration ────────────────────────────────────────────────────────
BASE_DIR   = os.environ.get("BASE_DIR", "/data")
BNM_DIR    = os.path.join(BASE_DIR, "bnm")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

# Directory that contains the sub-program .py files
PROG_DIR   = Path(__file__).resolve().parent

os.makedirs(BNM_DIR,    exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── REPTDATE computation ──────────────────────────────────────────────────────
# SAS equivalent:
#   REPTDATE = INPUT('01' || PUT(MONTH(TODAY()), Z2.) ||
#                    PUT(YEAR(TODAY()),  4.), DDMMYY8.) - 1;
# i.e. the last day of the PREVIOUS month relative to today.

def compute_reptdate(today: date = None) -> date:
    """Return the last day of the month preceding `today`."""
    if today is None:
        today = date.today()
    first_of_current = today.replace(day=1)
    return first_of_current - timedelta(days=1)


# ── Week / macro-variable derivation ─────────────────────────────────────────
# SAS SELECT(DAY(REPTDATE)):
#   WHEN (8)       SDD=1,  WK='1', WK1='4'
#   WHEN (15)      SDD=9,  WK='2', WK1='1'
#   WHEN (22)      SDD=16, WK='3', WK1='2'
#   OTHERWISE      SDD=23, WK='4', WK1='3'

def derive_macro_vars(reptdate: date) -> dict:
    """
    Derive all CALL SYMPUT macro variables from REPTDATE.

    Returns a dict with string values mirroring the SAS macro variables:
      NOWK, NOWK1, REPTMON, REPTMON1, REPTYEAR, REPTDAY, RDATE, SDATE
    """
    day = reptdate.day

    if day == 8:
        sdd, wk, wk1 = 1, "1", "4"
    elif day == 15:
        sdd, wk, wk1 = 9, "2", "1"
    elif day == 22:
        sdd, wk, wk1 = 16, "3", "2"
    else:
        sdd, wk, wk1 = 23, "4", "3"

    mm = reptdate.month

    # IF WK='1' THEN MM1=MM-1 (wrap 0→12); ELSE MM1=MM
    if wk == "1":
        mm1 = mm - 1
        if mm1 == 0:
            mm1 = 12
    else:
        mm1 = mm

    # SDATE = MDY(MM, SDD, YEAR(REPTDATE))
    sdate = date(reptdate.year, mm, sdd)

    # Format helpers
    reptmon   = f"{mm:02d}"           # Z2.  e.g. '03'
    reptmon1  = f"{mm1:02d}"          # Z2.  e.g. '02'
    reptyear  = f"{reptdate.year:04d}" # YEAR4. e.g. '2026'
    reptday   = f"{reptdate.day:02d}" # Z2.  e.g. '31'
    rdate     = reptdate.strftime("%d%m%Y")  # DDMMYY8. e.g. '31032026'
    sdate_fmt = sdate.strftime("%d%m%Y")     # DDMMYY8. e.g. '23032026'

    return {
        "NOWK":     wk,
        "NOWK1":    wk1,
        "REPTMON":  reptmon,
        "REPTMON1": reptmon1,
        "REPTYEAR": reptyear,
        "REPTDAY":  reptday,
        "RDATE":    rdate,
        "SDATE":    sdate_fmt,
    }


# ── Sub-program invocation ────────────────────────────────────────────────────
# Equivalent to %INC PGM(...) – each included SAS program is run as a
# separate Python process with the macro variables exported via os.environ.

SUB_PROGRAMS = [
    "P124DL1B.py",
    "P124DL2B.py",
    "P124RDLB.py",
    "P124DAL1.py",
    "P124DAL2.py",
    "P124DAL3.py",
    "P124RDAL.py",
]


def run_subprogram(prog_name: str, env: dict) -> None:
    """
    Execute a sub-program Python file as a child process,
    passing the computed macro variables through the environment.
    """
    prog_path = PROG_DIR / prog_name
    if not prog_path.exists():
        print(f"[WARN] Sub-program not found, skipping: {prog_path}", file=sys.stderr)
        return

    print(f"[INFO] Running {prog_name} ...")
    result = subprocess.run(
        [sys.executable, str(prog_path)],
        env=env,
        check=False,
    )
    if result.returncode != 0:
        print(
            f"[ERROR] {prog_name} exited with code {result.returncode}",
            file=sys.stderr,
        )
        sys.exit(result.returncode)
    print(f"[INFO] {prog_name} completed successfully.")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    # Compute REPTDATE (last day of previous month)
    reptdate = compute_reptdate()
    print(f"[INFO] REPTDATE = {reptdate.strftime('%d/%m/%Y')}")

    # Derive all macro variables
    macro_vars = derive_macro_vars(reptdate)
    print(f"[INFO] Macro variables: {macro_vars}")

    # Build child environment: inherit current env, overlay macro vars
    # and BASE_DIR so sub-programs resolve paths consistently.
    child_env = os.environ.copy()
    child_env["BASE_DIR"] = BASE_DIR
    child_env.update(macro_vars)

    # %INC PGM(P124DL1B); %INC PGM(P124DL2B); %INC PGM(P124RDLB);
    # %INC PGM(P124DAL1); %INC PGM(P124DAL2); %INC PGM(P124DAL3);
    # %INC PGM(P124RDAL);
    for prog in SUB_PROGRAMS:
        run_subprogram(prog, child_env)

    print("[INFO] EIBQ1241 complete.")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
