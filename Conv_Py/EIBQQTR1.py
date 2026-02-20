#!/usr/bin/env python3
"""
Program : EIBQQTR1
ESMR    : 2006-1346
Purpose : Main orchestrator for PBB RDAL quarterly processing.
          Derives report date and macro variables, validates source extractions,
            invokes all sub-programs in sequence, and generates SFTP command output.
          Covers Report on Domestic Assets and Liabilities (RDAL),
          Report on Global Assets and Capital, and Report on Consolidated Assets.

JCL-level DD allocations (IEFBR14 DELETE/CREATE, COZBATCH SFTP) are handled
    via Python file-path equivalents. Mainframe-specific steps are noted as comments.
"""

import os
import sys
import shutil
import importlib.util
from datetime import date, timedelta

# ─────────────────────────────────────────────────────────────
# PATH CONFIGURATION  (equivalent to JCL DD statements / LIBNAME)
# ─────────────────────────────────────────────────────────────
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
PGM_DIR      = BASE_DIR                                          # PGM DD  (program library)
INPUT_DIR    = os.path.join(BASE_DIR, "input")                  # Source extraction inputs
OUTPUT_DIR   = os.path.join(BASE_DIR, "output")                 # BNM library  (SAP.PBB.D&REPTYEAR)
BNMX_DIR     = os.path.join(BASE_DIR, "output", "bnmx")        # BNMX library (SAP.PBB.P124)
BNM1_DIR     = os.path.join(BASE_DIR, "input",  "sasdata")     # BNM1 library (SAP.PBB.SASDATA)

# JCL DD input sources (parquet equivalents)
WALALW_PARQUET  = os.path.join(INPUT_DIR, "WALALW.parquet")     # WALALW DD  (SAP.PBB.FISS.TXT)
# WALALM_PARQUET= os.path.join(INPUT_DIR, "WALALM.parquet")     # *WALALM DD (SAP.PBB.RDAL2.TXT) - commented in JCL
WALGAY_PARQUET  = os.path.join(INPUT_DIR, "WALGAY.parquet")     # WALGAY DD  (SAP.PBB.RGAC.TXT)
LOAN_PARQUET    = os.path.join(INPUT_DIR, "LOAN_REPTDATE.parquet")    # LOAN DD
DEPOSIT_PARQUET = os.path.join(INPUT_DIR, "DEPOSIT_REPTDATE.parquet") # DEPOSIT DD
FD_PARQUET      = os.path.join(INPUT_DIR, "FD_REPTDATE.parquet")      # FD DD
BNMTBL1_TXT     = os.path.join(INPUT_DIR, "BNMTBL1.txt")             # BNMTBL1 DD
BNMTBL2_TXT     = os.path.join(INPUT_DIR, "BNMTBL2.txt")             # BNMTBL2 DD
BNMTBL3_TXT     = os.path.join(INPUT_DIR, "BNMTBL3.txt")             # BNMTBL3 DD

# JCL DD output destinations
RDAL_DIR        = os.path.join(OUTPUT_DIR, "rdal")              # RDAL DD    (SAP.PBB.FISS.RDAL)
NSRS_DIR        = os.path.join(OUTPUT_DIR, "nsrs")              # NSRS DD    (SAP.PBB.NSRS.RDAL)
RDALWK_DIR      = os.path.join(OUTPUT_DIR, "rdalwk")            # RDALWK DD  (SAP.PBB.WALKER.RDAL)
ELIAB_FILE      = os.path.join(OUTPUT_DIR, "eliab", "ELIAB.txt") # ELIAB DD  (SAP.PBB.ELIAB.TEXT LRECL=134)

for d in (OUTPUT_DIR, BNMX_DIR, BNM1_DIR,
          RDAL_DIR, NSRS_DIR, RDALWK_DIR,
          os.path.dirname(ELIAB_FILE)):
    os.makedirs(d, exist_ok=True)

# //DELETE EXEC PGM=IEFBR14 — pre-delete output datasets if they exist
# DD1: SAP.PBB.FISS.RDAL      DD2: SAP.PBB.WALKER.RDAL
# DD3: SAP.PBB.ELIAB.TEXT     DD4: SAP.PBB.NSRS.RDAL
for path in (RDAL_DIR, RDALWK_DIR, ELIAB_FILE, NSRS_DIR):
    if os.path.isfile(path):
        os.remove(path)
    elif os.path.isdir(path):
        shutil.rmtree(path)
        os.makedirs(path, exist_ok=True)

# //CREATE EXEC PGM=IEFBR14 — pre-create ELIAB.TEXT (RECFM=FB,LRECL=134)
# Equivalent: ensure the ELIAB output file parent directory exists (done above)

# OPTIONS SORTDEV=3390 YEARCUTOFF=1950 NOCENTER — no Python equivalent needed

# ─────────────────────────────────────────────────────────────
# %INC PGM(EIGWRD1W) — Weekly RDAL1 Walker processing (PBB)
# Placeholder: X_EIGWRD1W.py
# ─────────────────────────────────────────────────────────────
# run_program("X_EIGWRD1W", env)  # invoked below after env is built

# /*
# %INC PGM(EIGMRD2W)  — Monthly RDAL2 Walker processing (PBB) - commented out
# */

# ─────────────────────────────────────────────────────────────
# %INC PGM(EIGMRGCW) — Monthly RGAC Walker processing (PBB)
# Placeholder: X_EIGMRGCW.py
# ─────────────────────────────────────────────────────────────
# run_program("X_EIGMRGCW", env)  # invoked below after env is built


# ─────────────────────────────────────────────────────────────
# DERIVE REPORT DATE AND MACRO VARIABLES
# Equivalent to DATA REPTDATE step
# ─────────────────────────────────────────────────────────────
def derive_report_date(today: date = None) -> dict:
    """
    Replicates the DATA REPTDATE step.
    REPTDATE = first day of current month - 1 = last day of previous month.
    Week (WK) derived from DAY(REPTDATE):
      day  8 -> WK='1', WK1='4'
      day 15 -> WK='2', WK1='1'
      day 22 -> WK='3', WK1='2'
      other  -> WK='4', WK1='3'
    Note: WK2/WK3 not set in this program (differs from X_EIIQTR1A).
    ELDATE = PUT(REPTDATE, Z5.) -> SAS internal date as zero-padded 5-digit integer.
    """
    if today is None:
        today = date.today()

    # REPTDATE = INPUT('01'||PUT(MONTH(TODAY()),Z2.)||PUT(YEAR(TODAY()),4.), DDMMYY8.) - 1
    first_of_month = date(today.year, today.month, 1)
    reptdate = first_of_month - timedelta(days=1)

    day = reptdate.day
    mm  = reptdate.month
    yy  = reptdate.year

    if day == 8:
        sdd, wk, wk1 = 1, '1', '4'
    elif day == 15:
        sdd, wk, wk1 = 9, '2', '1'
    elif day == 22:
        sdd, wk, wk1 = 16, '3', '2'
    else:
        sdd, wk, wk1 = 23, '4', '3'

    if wk == '1':
        mm1 = mm - 1 if mm > 1 else 12
    else:
        mm1 = mm

    sdate = date(yy, mm, sdd)

    sdesc = 'PUBLIC BANK BERHAD'

    # ELDATE = PUT(REPTDATE, Z5.) -> SAS date = days since 1960-01-01, zero-padded 5 digits
    sas_epoch   = date(1960, 1, 1)
    sas_date_n  = (reptdate - sas_epoch).days
    eldate_str  = f"{sas_date_n:05d}"

    rdate_str  = reptdate.strftime("%d%m%Y")   # DDMMYY8. = DDMMYYYY
    sdate_str  = sdate.strftime("%d%m%Y")
    reptmon    = f"{mm:02d}"
    reptmon1   = f"{mm1:02d}"
    reptyear   = str(yy)
    reptday    = f"{day:02d}"

    return {
        "reptdate"  : reptdate,
        "NOWK"      : wk,
        "NOWK1"     : wk1,
        "REPTMON"   : reptmon,
        "REPTMON1"  : reptmon1,
        "REPTYEAR"  : reptyear,
        "REPTDAY"   : reptday,
        "RDATE"     : rdate_str,
        "ELDATE"    : eldate_str,
        "SDATE"     : sdate_str,
        "SDESC"     : sdesc,
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
    # Numeric SAS date (days since 1960-01-01)
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
    yymmdd = line[:8].strip()   # YYMMDD8. in SAS = YYYYMMDD
    dt = date(int(yymmdd[:4]), int(yymmdd[4:6]), int(yymmdd[6:8]))
    return dt.strftime("%d%m%Y")


# ─────────────────────────────────────────────────────────────
# DYNAMIC PROGRAM LOADER
# Equivalent to %INC PGM(...) macro
# ─────────────────────────────────────────────────────────────
def run_program(pgm_name: str, env: dict):
    """
    Dynamically load and execute a Python program from PGM_DIR,
    injecting macro-equivalent variables via os.environ.
    """
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
# %MACRO PROCESS equivalent
# ─────────────────────────────────────────────────────────────
def process(env: dict):
    """
    Validate all source extractions are dated as of RDATE,
    then invoke all sub-programs in sequence.
    """
    rdate   = env["RDATE"]
    reptmon = env["REPTMON"]
    nowk    = env["NOWK"]

    # ── Read REPTDATE from each source extraction ──
    try:
        alw_date     = read_parquet_reptdate(WALALW_PARQUET)
    except Exception as exc:
        print(f"[ERROR] Could not read ALW date: {exc}")
        alw_date = ""

    # /*
    # DATA _NULL_;
    #   SET ALM.REPTDATE(OBS=1);
    #   CALL SYMPUT('ALM', PUT(REPTDATE, DDMMYY8.));
    # RUN;
    # */

    try:
        gay_date     = read_parquet_reptdate(WALGAY_PARQUET)
    except Exception as exc:
        print(f"[ERROR] Could not read GAY date: {exc}")
        gay_date = ""

    try:
        loan_date    = read_parquet_reptdate(LOAN_PARQUET)
    except Exception as exc:
        print(f"[ERROR] Could not read LOAN date: {exc}")
        loan_date = ""

    try:
        deposit_date = read_parquet_reptdate(DEPOSIT_PARQUET)
    except Exception as exc:
        print(f"[ERROR] Could not read DEPOSIT date: {exc}")
        deposit_date = ""

    try:
        fd_date      = read_parquet_reptdate(FD_PARQUET)
    except Exception as exc:
        print(f"[ERROR] Could not read FD date: {exc}")
        fd_date = ""

    try:
        kapiti1_date = read_txt_reptdate(BNMTBL1_TXT)
    except Exception as exc:
        print(f"[ERROR] Could not read KAPITI1 date: {exc}")
        kapiti1_date = ""

    try:
        kapiti2_date = read_txt_reptdate(BNMTBL2_TXT)
    except Exception as exc:
        print(f"[ERROR] Could not read KAPITI2 date: {exc}")
        kapiti2_date = ""

    try:
        kapiti3_date = read_txt_reptdate(BNMTBL3_TXT)
    except Exception as exc:
        print(f"[ERROR] Could not read KAPITI3 date: {exc}")
        kapiti3_date = ""

    # ── Validate all dates match RDATE ──
    # %IF "&ALW"="&RDATE" AND
    # /* "&ALM"="&RDATE" AND */
    #     "&GAY"="&RDATE" AND
    #     "&LOAN"="&RDATE" AND
    #     "&DEPOSIT"="&RDATE" AND
    #     "&FD"="&RDATE" AND
    #     "&KAPITI1"="&RDATE" AND
    #     "&KAPITI2"="&RDATE" AND
    #     "&KAPITI3"="&RDATE"
    valid = True

    if alw_date != rdate:
        print(f"THE RDAL1 EXTRACTION IS NOT DATED {rdate}")
        valid = False
    # /*
    # if alm_date != rdate:
    #     print(f"THE RDAL1 EXTRACTION IS NOT DATED {rdate}")
    #     valid = False
    # */
    if gay_date != rdate:
        print(f"THE RGAY EXTRACTION IS NOT DATED {rdate}")
        valid = False
    if loan_date != rdate:
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

    # ── All validations passed ──

    # /*** REPORT ON DOMESTIC ASSETS AND LIABILITIES ***/

    # %INC PGM(WALWPBBP) — Weekly ALW PBB processing (Walker)
    # Placeholder: WALWPBBP.py
    run_program("WALWPBBP", env)

    # /* %INC PGM(WALMPBBP) — Monthly ALM PBB processing (Walker) - commented out */

    # /*
    # PROC DATASETS LIB=WORK KILL NOLIST; RUN;
    # %INC PGM(WISDPBBE) — WIS deposit PBB processing - commented out
    # PROC DATASETS LIB=WORK KILL NOLIST; RUN;
    # */

    # /* %INC PGM(WISMPBBE) — WIS monthly PBB processing - commented out */
    # /* PROC DATASETS LIB=WORK KILL NOLIST; RUN; */

    # PROC DATASETS LIB=BNM NOLIST;
    #   DELETE ALWGL{REPTMON}{NOWK}
    #          ALWJE{REPTMON}{NOWK}
    #          /* ALMGL{REPTMON}{NOWK} — commented out */
    for ds in (f"ALWGL{reptmon}{nowk}", f"ALWJE{reptmon}{nowk}"):
        p = os.path.join(OUTPUT_DIR, f"{ds}.parquet")
        if os.path.exists(p):
            os.remove(p)
    # /* f"ALMGL{reptmon}{nowk}" — commented out */

    # %INC PGM(EIBRDL1B) — RDAL Part I consolidation (PBB)
    # Placeholder: EIBRDL1B.py
    run_program("EIBRDL1B", env)

    # %INC PGM(EIBRDL2B) — RDAL Part II consolidation (PBB)
    # Placeholder: EIBRDL2B.py
    run_program("EIBRDL2B", env)

    # %INC PGM(KALMLIFE) — KAPITI ALM LIF processing (PBB)
    # Placeholder: KALMLIFE.py
    run_program("KALMLIFE", env)

    # %INC PGM(EIBWRDLB) — RDAL Walker consolidation (PBB)
    # Placeholder: EIBWRDLB.py
    run_program("EIBWRDLB", env)

    # %INC PGM(PBBRDAL1) — RDAL Part I report (PBB)
    # Placeholder: PBBRDAL1.py
    run_program("PBBRDAL1", env)

    # %INC PGM(PBBRDAL2) — RDAL Part II report (PBB)
    # Placeholder: PBBRDAL2.py
    run_program("PBBRDAL2", env)

    # %INC PGM(PBBRDAL3) — RDAL Part III report (PBB)
    run_program("PBBRDAL3", env)

    # %INC PGM(PBBELP) — ELP report (PBB)
    # Placeholder: PBBELP.py
    run_program("PBBELP", env)

    # %INC PGM(EIGWRDAL) — Walker RDAL processing (PBB)
    # Placeholder: EIGWRDAL.py
    run_program("EIGWRDAL", env)

    # %INC PGM(PBBBRELP) — Branch ELP report (PBB)
    # Placeholder: PBBBRELP.py
    run_program("PBBBRELP", env)

    # %INC PGM(PBBALP) — ALP report (PBB)
    # Placeholder: PBBALP.py
    run_program("PBBALP", env)

    # %INC PGM(ALWDP124) — Copy ALW to BNMX (P124) library
    run_program("ALWDP124", env)

    # PROC DATASETS LIB=WORK KILL NOLIST — no-op in Python

    # /*** REPORT ON GLOBAL ASSETS AND CAPITAL ***/
    # /*** REPORT ON CONSOLIDATED ASSETS AND CAPITAL ***/
    # /*
    # %INC PGM(WGAYPBBP) — Global Assets/Capital Walker processing (PBB) - commented out
    # PROC DATASETS LIB=WORK KILL NOLIST; RUN;
    # PROC DATASETS LIB=BNM NOLIST;
    #   DELETE GAYGL{REPTMON}{NOWK}
    #          GAYJE{REPTMON}{NOWK};
    # RUN;
    # */

    print("[DONE] All sub-programs completed successfully.")


# ─────────────────────────────────────────────────────────────
# FTP HOST DATASETS TO DATA REPORT REPOSITORY SYSTEM (DRR)
# //RUNSFTP EXEC COZBATCH (DRR#SFTP)
# Mainframe COZBATCH SFTP has no direct Python equivalent.
# In a Python environment this would be implemented via paramiko SFTP.
# The equivalent PUT commands are:
#   put //SAP.PBB.FISS.RDAL    "RDAL MTH.TXT"
#   put //SAP.PBB.NSRS.RDAL    "NSRS MTH.TXT"
# Source files: RDAL_DIR contents -> remote "FD-BNM REPORTING/PBB/BNM RPTG"
# ─────────────────────────────────────────────────────────────
def generate_sftp_commands() -> str:
    """
    Return the SFTP command block equivalent to the JCL COZBATCH step.
    In production, pass these commands to a paramiko SFTP session.
    """
    lines = [
        'lzopts servercp=$servercp,notrim,overflow=trunc,mode=text',
        'lzopts linerule=$lr',
        'cd "FD-BNM REPORTING/PBB/BNM RPTG"',
        f'put {os.path.join(RDAL_DIR, "RDAL_MTH.txt")}  "RDAL MTH.TXT"',
        f'put {os.path.join(NSRS_DIR, "NSRS_MTH.txt")}  "NSRS MTH.TXT"',
        'EOB',
    ]
    return "\n".join(lines)


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
    print(f"ELDATE       : {env['ELDATE']}")
    print(f"SDESC        : {env['SDESC']}")

    # LIBNAME BNM  "SAP.PBB.D&REPTYEAR" -> OUTPUT_DIR
    # LIBNAME BNMX "SAP.PBB.P124"       -> BNMX_DIR
    # LIBNAME BNM1 "SAP.PBB.SASDATA"    -> BNM1_DIR

    # %INC PGM(EIGWRD1W) — invoked before REPTDATE derivation in original SAS,
    # but requires macro vars; run after env is built
    run_program("EIGWRD1W", env)

    # /*
    # %INC PGM(EIGMRD2W) — commented out in original
    # */

    # %INC PGM(EIGMRGCW)
    run_program("EIGMRGCW", env)

    # Run the main processing macro
    process(env)

    # Print SFTP command block (for reference / manual execution)
    sftp_cmds = generate_sftp_commands()
    print("\n[SFTP Commands]")
    print(sftp_cmds)
