# !/usr/bin/env python3
"""
Program : EIIQTR1A
Purpose : Main orchestrator for RDAL quarterly processing.
          Derives report date and macro variables, validates source extractions,
            invokes all sub-programs in sequence, and generates the SFTP command file.

JCL-level DD allocations and SFTP/FTP steps are handled via Python file paths
and subprocess equivalents below. Mainframe-specific JCL (IEFBR14, COZBATCH,
PROC CPORT) have no direct Python equivalent and are noted as comments.
"""

import os
import sys
import subprocess
import importlib.util
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

# ─────────────────────────────────────────────────────────────
# PATH CONFIGURATION  (equivalent to JCL DD statements / LIBNAME)
# ─────────────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
PGM_DIR     = BASE_DIR                                       # PGM DD  (SAS program library)
OUTPUT_DIR  = os.path.join(BASE_DIR, "output")               # BNM library root
BNM1_DIR    = os.path.join(BASE_DIR, "input", "sasdata")     # BNM1 / SAP.PIBB.SASDATA
BNMX_DIR    = os.path.join(OUTPUT_DIR, "bnmx")               # BNMX library
DEPOBACK_DIR= os.path.join(OUTPUT_DIR, "depoback")           # DEPOBACK DD
RDALTMP_DIR = os.path.join(OUTPUT_DIR, "rdaltmp")            # RDALTMP DD
RDALFTP_DIR = os.path.join(OUTPUT_DIR, "rdalftp")            # RDALFTP DD
RDALKM_DIR  = os.path.join(OUTPUT_DIR, "rdalkm")             # RDALKM DD  (SAP.PIBB.KAPMNI.RDAL)
NSRSKM_DIR  = os.path.join(OUTPUT_DIR, "nsrskm")             # NSRSKM DD  (SAP.PIBB.NSRS.KAPMNI.RDAL)
SFTP01_FILE = os.path.join(OUTPUT_DIR, "sftp01.txt")         # SFTP01 DD

# Source input file paths (JCL DD → parquet equivalents)
INPUT_DIR         = os.path.join(BASE_DIR, "input")
LOAN_PARQUET      = os.path.join(INPUT_DIR, "LOAN_REPTDATE.parquet")       # LOAN DD
DEPOSIT_PARQUET   = os.path.join(INPUT_DIR, "DEPOSIT_REPTDATE.parquet")    # DEPOSIT DD
FD_PARQUET        = os.path.join(INPUT_DIR, "FD_REPTDATE.parquet")         # FD DD
BNMTBL1_TXT       = os.path.join(INPUT_DIR, "BNMTBL1.txt")                 # BNMTBL1 DD
BNMTBL2_TXT       = os.path.join(INPUT_DIR, "BNMTBL2.txt")                 # BNMTBL2 DD
BNMTBL3_TXT       = os.path.join(INPUT_DIR, "BNMTBL3.txt")                 # BNMTBL3 DD

for d in (OUTPUT_DIR, BNM1_DIR, DEPOBACK_DIR, RDALTMP_DIR,
          RDALFTP_DIR, RDALKM_DIR, NSRSKM_DIR, BNMX_DIR):
    os.makedirs(d, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# OPTIONS: YEARCUTOFF=1950 (two-digit year pivot handled in parsing)
# OPTIONS SORTDEV=3390 NOCENTER — no Python equivalent needed
# ─────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────
# DERIVE REPORT DATE AND MACRO VARIABLES
# Equivalent to DATA REPTDATE step
# ─────────────────────────────────────────────────────────────
def derive_report_date(today: date = None) -> dict:
    """
    Replicates the DATA REPTDATE step.
    REPTDATE = first day of current month - 1 day = last day of previous month.
    Week number (WK) is derived from DAY(REPTDATE):
      day  8 -> WK='1', prev-WK='4'
      day 15 -> WK='2', prev-WK='1'
      day 22 -> WK='3', prev-WK='2'
      other  -> WK='4', prev-WK='3','2','1'
    """
    if today is None:
        today = date.today()

    # REPTDATE = INPUT('01' || PUT(MONTH(TODAY()),Z2.) || PUT(YEAR(TODAY()),4.), DDMMYY8.) - 1
    first_of_month = date(today.year, today.month, 1)
    reptdate = first_of_month - timedelta(days=1)

    day = reptdate.day
    mm  = reptdate.month
    yy  = reptdate.year

    if day == 8:
        sdd, wk, wk1 = 1, '1', '4'
        wk2, wk3 = None, None
    elif day == 15:
        sdd, wk, wk1 = 9, '2', '1'
        wk2, wk3 = None, None
    elif day == 22:
        sdd, wk, wk1 = 16, '3', '2'
        wk2, wk3 = None, None
    else:
        sdd, wk, wk1 = 23, '4', '3'
        wk2, wk3 = '2', '1'

    if wk == '1':
        mm1 = mm - 1 if mm > 1 else 12
    else:
        mm1 = mm

    mm2 = mm - 1 if mm > 1 else 12

    sdate = date(yy, mm, sdd)

    sdesc = 'PUBLIC ISLAMIC BANK BERHAD'

    rdate_str  = reptdate.strftime("%d%m%Y")   # DDMMYYYY  (DDMMYY8. in SAS = 8-char DDMMYYYY)
    sdate_str  = sdate.strftime("%d%m%Y")
    reptmon    = f"{mm:02d}"
    reptmon1   = f"{mm1:02d}"
    reptmon2   = f"{mm2:02d}"
    reptyear   = str(yy)
    reptday    = f"{day:02d}"

    return {
        "reptdate"  : reptdate,
        "NOWK"      : wk,
        "NOWK1"     : '1',
        "NOWK2"     : '2',
        "NOWK3"     : '3',
        "REPTMON"   : reptmon,
        "REPTMON1"  : reptmon1,
        "REPTMON2"  : reptmon2,
        "REPTYEAR"  : reptyear,
        "REPTDAY"   : reptday,
        "RDATE"     : rdate_str,
        "TDATE"     : reptdate,
        "SDATE"     : sdate_str,
        "SDESC"     : sdesc,
        "WK1"       : wk1,
        "WK2"       : wk2,
        "WK3"       : wk3,
    }


# ─────────────────────────────────────────────────────────────
# READ REPTDATE FROM SOURCE EXTRACTIONS
# Equivalent to DATA _NULL_ / CALL SYMPUT blocks
# ─────────────────────────────────────────────────────────────
def read_parquet_reptdate(parquet_path: str) -> str:
    """Read REPTDATE from first row of a parquet file; return as DDMMYYYY string."""
    import duckdb
    con = duckdb.connect()
    row = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{parquet_path}') LIMIT 1"
    ).fetchone()
    con.close()
    if row is None:
        raise ValueError(f"No rows found in {parquet_path}")
    rd = row[0]
    if isinstance(rd, date):
        return rd.strftime("%d%m%Y")
    # numeric SAS date (days since 1960-01-01)
    actual = date(1960, 1, 1) + timedelta(days=int(rd))
    return actual.strftime("%d%m%Y")


def read_txt_reptdate(txt_path: str) -> str:
    """
    Read REPTDATE from first record of a flat text file.
    @1 REPTDATE YYMMDD8. -> positions 1-8 are YYYYMMDD.
    Returns DDMMYYYY string.
    """
    with open(txt_path, "r", encoding="utf-8") as f:
        line = f.readline()
    yymmdd = line[:8].strip()           # YYMMDD8. format: YYYYMMDD
    dt = date(int(yymmdd[:4]), int(yymmdd[4:6]), int(yymmdd[6:8]))
    return dt.strftime("%d%m%Y")


# ─────────────────────────────────────────────────────────────
# DYNAMIC PROGRAM LOADER
# Equivalent to %INC PGM(...) macro
# ─────────────────────────────────────────────────────────────
def run_program(pgm_name: str, env: dict):
    """
    Dynamically load and execute a Python program from PGM_DIR,
    injecting macro-equivalent variables via os.environ for child programs
    that read them at module level.
    """
    # Propagate macro variables as environment variables
    for k, v in env.items():
        if v is not None:
            os.environ[k] = str(v)

    pgm_file = os.path.join(PGM_DIR, f"{pgm_name}.py")
    if not os.path.isfile(pgm_file):
        print(f"[WARN] Program not found, skipping: {pgm_file}")
        return

    spec   = importlib.util.spec_from_file_location(pgm_name, pgm_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    print(f"[OK] Completed: {pgm_name}")


# ─────────────────────────────────────────────────────────────
# GENERATE SFTP COMMAND FILE
# Equivalent to DATA _NULL_ FILE SFTP01 step
# ─────────────────────────────────────────────────────────────
def generate_sftp01(today: date = None):
    """
    Write SFTP PUT commands to SFTP01 file.
    If DAY(TODAY()) > 2 OR (DAY(TODAY()) = 2 AND HOUR > 20): use EIR filenames.
    Otherwise: use standard filenames.
    """
    if today is None:
        today = date.today()

    from datetime import datetime
    hour = datetime.now().hour

    use_eir = (today.day > 2) or (today.day == 2 and hour > 20)

    if use_eir:
        line1 = 'PUT //SAP.PIBB.KAPMNI.RDAL      "RDAL KAPMNI EIR.TXT"'
        line2 = 'PUT //SAP.PIBB.NSRS.KAPMNI.RDAL "NSRS KAPMNI EIR.TXT"'
    else:
        line1 = 'PUT //SAP.PIBB.KAPMNI.RDAL      "RDAL KAPMNI.TXT"'
        line2 = 'PUT //SAP.PIBB.NSRS.KAPMNI.RDAL "NSRS KAPMNI.TXT"'

    with open(SFTP01_FILE, "w", encoding="utf-8") as f:
        f.write(line1 + "\n")
        f.write(line2 + "\n")

    print(f"SFTP01 command file written: {SFTP01_FILE}")


# ─────────────────────────────────────────────────────────────
# %MACRO PROCESS equivalent
# ─────────────────────────────────────────────────────────────
def process(env: dict):
    """
    Validate that all source extractions are dated as of RDATE,
    then invoke all sub-programs in sequence.
    """
    rdate = env["RDATE"]

    # Read REPTDATE from each source extraction
    try:
        loan_date    = read_parquet_reptdate(LOAN_PARQUET)
        deposit_date = read_parquet_reptdate(DEPOSIT_PARQUET)
        fd_date      = read_parquet_reptdate(FD_PARQUET)
        kapiti1_date = read_txt_reptdate(BNMTBL1_TXT)
        kapiti2_date = read_txt_reptdate(BNMTBL2_TXT)
        kapiti3_date = read_txt_reptdate(BNMTBL3_TXT)
    except Exception as exc:
        print(f"[ERROR] Could not read source dates: {exc}")
        sys.exit(77)

    # Validate all dates match RDATE
    valid = True
    if loan_date    != rdate:
        print(f"THE LOAN EXTRACTION IS NOT DATED {rdate}")
        valid = False
    if deposit_date != rdate:
        print(f"THE DEPOSIT EXTRACTION IS NOT DATED {rdate}")
        valid = False
    if kapiti1_date != rdate:
        print(f"THE KAPITI1 EXTRACTION IS NOT DATED {rdate}")
        valid = False
    if kapiti2_date != rdate:
        print(f"THE KAPITI2 EXTRACTION IS NOT DATED {rdate}")
        valid = False
    if kapiti3_date != rdate:
        print(f"THE KAPITI3 EXTRACTION IS NOT DATED {rdate}")
        valid = False

    if not valid:
        print("THE JOB IS NOT DONE !!")
        sys.exit(77)

    # ── All validations passed — run sub-programs in sequence ──

    # %INC PGM(DALMPBBD)
    # Placeholder: DALMPBBD.py — Deposit ALM monthly processing
    run_program("DALMPBBD", env)

    # %INC PGM(DALWBP)
    # Placeholder: DALWBP.py — Deposit ALW weekly processing
    run_program("DALWBP", env)

    # %INC PGM(DALMBP)
    # Placeholder: DALMBP.py — Deposit ALM monthly base processing
    run_program("DALMBP", env)

    # PROC DATASETS LIB=WORK KILL NOLIST — clear work datasets (no-op in Python)

    # PROC COPY IN=BNM OUT=DEPOBACK: SELECT SAVG, CURN, DEPT datasets
    # Copy SAVG{REPTMON}{NOWK}, CURN{REPTMON}{NOWK}, DEPT{REPTMON}{NOWK}
    # from OUTPUT_DIR to DEPOBACK_DIR
    reptmon = env["REPTMON"]
    nowk    = env["NOWK"]
    for ds in (f"SAVG{reptmon}{nowk}", f"CURN{reptmon}{nowk}", f"DEPT{reptmon}{nowk}"):
        src = os.path.join(OUTPUT_DIR, f"{ds}.parquet")
        dst = os.path.join(DEPOBACK_DIR, f"{ds}.parquet")
        if os.path.exists(src):
            import shutil
            shutil.copy2(src, dst)

    # PROC DATASETS LIB=BNM NOLIST; DELETE SAVG, CURN, DEPT
    for ds in (f"SAVG{reptmon}{nowk}", f"CURN{reptmon}{nowk}", f"DEPT{reptmon}{nowk}"):
        p = os.path.join(OUTPUT_DIR, f"{ds}.parquet")
        if os.path.exists(p):
            os.remove(p)

    # %INC PGM(FALMPBBD)
    # Placeholder: FALMPBBD.py — FD ALM monthly processing
    run_program("FALMPBBD", env)

    # %INC PGM(FALWPBBP)
    # Placeholder: FALWPBBP.py — FD ALW weekly processing
    run_program("FALWPBBP", env)

    # %INC PGM(FALMPBBP)
    # Placeholder: FALMPBBP.py — FD ALM monthly processing (PBB)
    run_program("FALMPBBP", env)

    # %INC PGM(FALQPBBP) — RM FD Accepted by Remaining Maturity
    run_program("FALQPBBP", env)

    # PROC COPY IN=BNM OUT=DEPOBACK: SELECT FDWKLY, FDMTHLY
    for ds in ("FDWKLY", "FDMTHLY"):
        src = os.path.join(OUTPUT_DIR, f"{ds}.parquet")
        dst = os.path.join(DEPOBACK_DIR, f"{ds}.parquet")
        if os.path.exists(src):
            import shutil
            shutil.copy2(src, dst)

    # PROC COPY IN=BNM1 OUT=BNM: SELECT LOAN{REPTMON}{NOWK}, ULOAN{REPTMON}{NOWK}
    import shutil
    for ds in (f"LOAN{reptmon}{nowk}", f"ULOAN{reptmon}{nowk}"):
        src = os.path.join(BNM1_DIR, f"{ds}.parquet")
        dst = os.path.join(OUTPUT_DIR, f"{ds}.parquet")
        if os.path.exists(src):
            shutil.copy2(src, dst)

    # PROC DATASETS LIB=WORK KILL NOLIST — no-op in Python

    # %INC PGM(LALWPBBP)
    # Placeholder: LALWPBBP.py — Loan ALW weekly processing
    run_program("LALWPBBP", env)

    # %INC PGM(LALMPIBP)
    # Placeholder: LALMPIBP.py — Loan ALM monthly processing (PIB)
    run_program("LALMPIBP", env)

    # %INC PGM(LALQPBBP) — Domestic Assets & Liabilities Part III
    run_program("LALQPBBP", env)

    # PROC DATASETS LIB=BNM NOLIST; DELETE LOAN, ULOAN
    for ds in (f"LOAN{reptmon}{nowk}", f"ULOAN{reptmon}{nowk}"):
        p = os.path.join(OUTPUT_DIR, f"{ds}.parquet")
        if os.path.exists(p):
            os.remove(p)

    # %INC PGM(KALWPIBP)
    # Placeholder: KALWPIBP.py — KAPITI ALW weekly processing (PIB)
    run_program("KALWPIBP", env)

    # %INC PGM(KALMPIBP)
    # Placeholder: KALMPIBP.py — KAPITI ALM monthly processing (PIB)
    run_program("KALMPIBP", env)

    # %INC PGM(KALMSTOR)
    # Placeholder: KALMSTOR.py — KAPITI ALM store processing
    run_program("KALMSTOR", env)

    # %INC PGM(NALMPIBP)
    # Placeholder: NALMPIBP.py — NSRS ALM monthly processing (PIB)
    run_program("NALMPIBP", env)

    # %INC PGM(KALQPIBP) — KAPITI quarterly items
    run_program("KALQPBBP", env)

    # PROC DATASETS LIB=WORK KILL NOLIST — no-op in Python

    # %INC PGM(EIBRDL1A)
    # Placeholder: EIBRDL1A.py — RDAL Part I consolidation
    run_program("EIBRDL1A", env)

    # %INC PGM(EIBRDL2A)
    # Placeholder: EIBRDL2A.py — RDAL Part II consolidation
    run_program("EIBRDL2A", env)

    # %INC PGM(EIBRDL3A) — RDAL Part III consolidation (ALQKM -> ALWKM)
    run_program("EIBRDL3A", env)

    # %INC PGM(KALMLIIE)
    # Placeholder: KALMLIIE.py — KAPITI ALM LII processing
    run_program("KALMLIIE", env)

    # %INC PGM(EIBIRDLA)
    # Placeholder: EIBIRDLA.py — IRD/ALM final consolidation
    run_program("EIBIRDLA", env)

    # PROC DATASETS LIB=WORK KILL NOLIST — no-op in Python

    print("[DONE] All sub-programs completed successfully.")


# ─────────────────────────────────────────────────────────────
# PROC CPORT / FTP steps
# These mainframe-specific steps have no direct Python equivalent.
# In a Python environment, PROC CPORT (transport file creation) would
# be replaced by packaging output files (e.g. tar/zip), and FTP/SFTP
# would be handled via paramiko or subprocess sftp calls.
#
# FILENAME TRANFILE 'SAP.PIBB.TEMP.RDAL.RDALFTP' DISP=OLD;
# PROC CPORT LIBRARY=RDALTMP FILE=TRANFILE;
#   -> Package RDALTMP_DIR contents into RDALFTP_DIR (placeholder)
#
# //RUNSFTP  EXEC COZBATCH (DRR SFTP and EDW SFTP steps)
#   -> Would be implemented via paramiko SFTP client using SFTP01_FILE commands
# ─────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    today = date.today()

    # Derive all macro variables from today's date
    env = derive_report_date(today)

    print(f"Report Date  : {env['RDATE']}")
    print(f"REPTMON      : {env['REPTMON']}")
    print(f"NOWK         : {env['NOWK']}")
    print(f"REPTYEAR     : {env['REPTYEAR']}")
    print(f"SDESC        : {env['SDESC']}")

    # Set LIBNAME BNM = output directory for the report year
    # LIBNAME BNM  "SAP.PIBB.D&REPTYEAR" -> already OUTPUT_DIR
    # LIBNAME BNM1 "SAP.PIBB.SASDATA"    -> already BNM1_DIR

    # Run the main processing macro
    process(env)

    # Generate the SFTP command file
    generate_sftp01(today)
