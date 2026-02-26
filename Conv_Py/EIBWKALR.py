#!/usr/bin/env python3
"""
Program : EIBWKALR
Purpose : Weekly Kapiti Interest Rate (RDIR Part I) orchestrator for PBB and PIBB.
          Replicates the JCL job EIBWKALR which:
            1. Derives reporting-period macro variables from LOAN.REPTDATE and
               LOANP.REPTDATE (current and previous week datasets).
            2. Validates that all three Kapiti flat-file dates match the reporting
               date (PBB path only); aborts if mismatched.
            3. Invokes, in sequence, the six sub-programs:
               PBB  path: KALWE   -> KALWPBBS  -> KALWPBBN
               PIBB path: KALWEI  -> KALWPIBS  -> KALWPIBN

          The SAS macro variables produced here map to Python runtime parameters
          injected into each sub-module's main() function.

JCL job  : EIBWKALR  (original mainframe job name)
Date     : (derived from JCL, original SAS programs dated 1999-2015)
"""

import os
import sys
import logging
from datetime import date, datetime
from calendar import monthrange

import duckdb
import polars as pl

# Sub-module imports  (replaces %INC PGM(...) in SAS)
import KALWE
import KALWEI
import KALWPBBS
import KALWPBBN
import KALWPIBS
import KALWPIBN

# ==============================================================================
# PATH CONFIGURATION
# ==============================================================================

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR  = os.path.join(BASE_DIR, "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# PBB loan / date-reference parquet paths
# LOAN   DD DSN=SAP.PBB.MNILN(0)   -> current period REPTDATE table
# LOANP  DD DSN=SAP.PBB.MNILN(-1)  -> previous period REPTDATE table
# ---------------------------------------------------------------------------
PBB_LOAN_PARQUET  = os.path.join(INPUT_DIR, "PBB_LOAN_REPTDATE.parquet")   # LOAN.REPTDATE  (PBB)
PBB_LOANP_PARQUET = os.path.join(INPUT_DIR, "PBB_LOANP_REPTDATE.parquet")  # LOANP.REPTDATE (PBB)

# ---------------------------------------------------------------------------
# PIBB loan / date-reference parquet paths
# LOAN   DD DSN=SAP.PIBB.MNILN(0)
# LOANP  DD DSN=SAP.PIBB.MNILN(-1)
# ---------------------------------------------------------------------------
PIBB_LOAN_PARQUET  = os.path.join(INPUT_DIR, "PIBB_LOAN_REPTDATE.parquet")   # LOAN.REPTDATE  (PIBB)
PIBB_LOANP_PARQUET = os.path.join(INPUT_DIR, "PIBB_LOANP_REPTDATE.parquet")  # LOANP.REPTDATE (PIBB)

# ---------------------------------------------------------------------------
# Kapiti flat-file paths — shared for both PBB and PIBB steps
# (Each bank set uses its own physical files; paths below point to PBB variants.
#  PIBB variants are defined separately further below.)
# ---------------------------------------------------------------------------
PBB_BNMTBLW_TXT = os.path.join(INPUT_DIR, "PBB_BNMTBLW.txt")   # BNMTBLW DD DSN=SAP.PBB.KAPITIW(0)
PBB_BNMTBL1_TXT = os.path.join(INPUT_DIR, "PBB_BNMTBL1.txt")   # BNMTBL1 DD DSN=SAP.PBB.KAPITI1(0)
PBB_BNMTBL3_TXT = os.path.join(INPUT_DIR, "PBB_BNMTBL3.txt")   # BNMTBL3 DD DSN=SAP.PBB.KAPITI3(0)
PBB_DCIWTBL_TXT = os.path.join(INPUT_DIR, "PBB_DCIWTBL.txt")   # DCIWTBL DD DSN=SAP.PBB.DCIWKLY(0)

PIBB_BNMTBLW_TXT = os.path.join(INPUT_DIR, "PIBB_BNMTBLW.txt")  # BNMTBLW DD DSN=SAP.PIBB.KAPITIW(0)
PIBB_BNMTBL1_TXT = os.path.join(INPUT_DIR, "PIBB_BNMTBL1.txt")  # BNMTBL1 DD DSN=SAP.PIBB.KAPITI1(0)
PIBB_BNMTBL3_TXT = os.path.join(INPUT_DIR, "PIBB_BNMTBL3.txt")  # BNMTBL3 DD DSN=SAP.PIBB.KAPITI3(0)

# ---------------------------------------------------------------------------
# Output text files
# SASLIST  DD DSN=SAP.PBB.EIBWKALR.TEXT       -> PBB main listing
# NSRSTXT  DD DSN=SAP.PBB.EIBWKALR.NSRS.TEXT  -> PBB NSRS/RDIR report
# ---------------------------------------------------------------------------
PBB_SASLIST_TXT  = os.path.join(OUTPUT_DIR, "PBB_EIBWKALR.txt")
PBB_NSRSTXT_TXT  = os.path.join(OUTPUT_DIR, "PBB_EIBWKALR_NSRS.txt")
PIBB_SASLIST_TXT = os.path.join(OUTPUT_DIR, "PIBB_EIBWKALR.txt")
PIBB_NSRSTXT_TXT = os.path.join(OUTPUT_DIR, "PIBB_EIBWKALR_NSRS.txt")

# OPTIONS YEARCUTOFF=1950 LS=132 PS=60 NOCENTER
YEARCUTOFF = 1950

# ==============================================================================
# LOGGING
# ==============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ==============================================================================
# HELPER: SAS internal date number <-> Python date
#
# SAS stores dates as integers counting days since 1960-01-01.
# PUT(REPTDATE, Z5.) zero-pads that integer to 5 characters.
# The child programs use PDATE as a filter threshold:  IF ISSDT > &PDATE
# which SAS evaluates as a numeric date comparison.
# In Python we convert to a Python date object for direct comparison.
# ==============================================================================

SAS_EPOCH = date(1960, 1, 1)


def sas_date_to_python(sas_int: int) -> date:
    """Convert SAS internal date integer (days since 1960-01-01) to Python date."""
    from datetime import timedelta
    return SAS_EPOCH + timedelta(days=int(sas_int))


def python_date_to_sas(d: date) -> int:
    """Convert Python date to SAS internal date integer (days since 1960-01-01)."""
    return (d - SAS_EPOCH).days


# ==============================================================================
# HELPER: Derive SAS macro variables from REPTDATE
#
# SELECT(DAY(REPTDATE));
#   WHEN (8)  DO; SDD=1;  WK='1'; WK1='4'; END;
#   WHEN(15)  DO; SDD=9;  WK='2'; WK1='1'; END;
#   WHEN(22)  DO; SDD=16; WK='3'; WK1='2'; END;
#   OTHERWISE DO; SDD=23; WK='4'; WK1='3'; END;
# IF WK='1' THEN DO; MM1=MM-1; IF MM1=0 THEN MM1=12; END;
# CALL SYMPUT('NOWK',   PUT(WK,$1.));
# CALL SYMPUT('NOWK1',  PUT(WK1,$1.));
# CALL SYMPUT('REPTMON',PUT(MONTH(REPTDATE),Z2.));
# CALL SYMPUT('REPTMON1',PUT(MM1,Z2.));
# CALL SYMPUT('REPTYEAR',PUT(REPTDATE,YEAR4.));
# CALL SYMPUT('REPTDAY', PUT(DAY(REPTDATE),Z2.));
# CALL SYMPUT('RDATE',   PUT(REPTDATE,DDMMYY8.));   -> DD/MM/YY (8-char)
# CALL SYMPUT('SDESC',   PUT(SDESC,$26.));
# ==============================================================================

def derive_macro_vars(reptdate: date, sdesc: str) -> dict:
    """
    Derive all SAS macro variables from REPTDATE and SDESC.
    Returns a dict mirroring each CALL SYMPUT value.
    """
    day = reptdate.day
    mm  = reptdate.month
    yr  = reptdate.year

    # SELECT(DAY(REPTDATE))
    if day == 8:
        # WHEN(8)  DO; SDD=1;  WK='1'; WK1='4'; END;
        sdd = 1
        wk  = "1"
        wk1 = "4"
    elif day == 15:
        # WHEN(15) DO; SDD=9;  WK='2'; WK1='1'; END;
        sdd = 9
        wk  = "2"
        wk1 = "1"
    elif day == 22:
        # WHEN(22) DO; SDD=16; WK='3'; WK1='2'; END;
        sdd = 16
        wk  = "3"
        wk1 = "2"
    else:
        # OTHERWISE DO; SDD=23; WK='4'; WK1='3'; END;
        sdd = 23
        wk  = "4"
        wk1 = "3"

    # MM=MONTH(REPTDATE); MM1=MM;
    # IF WK='1' THEN DO; MM1=MM-1; IF MM1=0 THEN MM1=12; END;
    mm1 = mm
    if wk == "1":
        mm1 = mm - 1
        if mm1 == 0:
            mm1 = 12

    # Derive previous month's year: if mm1 wrapped to 12, year decrements
    yr1 = yr if mm1 != 12 or wk != "1" else yr - 1

    # CALL SYMPUT('RDATE', PUT(REPTDATE,DDMMYY8.));  -> DD/MM/YY  e.g. 31/01/23
    rdate_ddmmyy8 = reptdate.strftime("%d/%m/%y")

    # CALL SYMPUT('REPTMON', PUT(MONTH(REPTDATE),Z2.));  -> zero-padded 2-digit month
    reptmon  = f"{mm:02d}"
    reptmon1 = f"{mm1:02d}"

    # Full YYYYMM strings used to construct parquet filenames in child programs
    reptmon_yyyymm  = f"{yr}{reptmon}"
    reptmon1_yyyymm = f"{yr1}{reptmon1}"

    return {
        # Raw derived values
        "sdd":      sdd,
        "wk":       wk,
        "wk1":      wk1,
        "mm":       mm,
        "mm1":      mm1,
        # CALL SYMPUT equivalents
        "NOWK":         wk,
        "NOWK1":        wk1,
        "REPTYEAR":     str(yr),
        "REPTMON":      reptmon,
        "REPTMON1":     reptmon1,
        "REPTDAY":      f"{day:02d}",
        "RDATE":        rdate_ddmmyy8,
        "SDESC":        sdesc.ljust(26)[:26],  # PUT(SDESC,$26.)
        # Full YYYYMM for filename construction
        "REPTMON_YYYYMM":  reptmon_yyyymm,
        "REPTMON1_YYYYMM": reptmon1_yyyymm,
        # Full date for display (DD/MM/YYYY used in report titles)
        "RDATE_FULL": reptdate.strftime("%d/%m/%Y"),
        # Python date object
        "reptdate": reptdate,
    }


# ==============================================================================
# HELPER: Read REPTDATE from parquet (LOAN.REPTDATE / LOANP.REPTDATE)
#
# DATA REPTDATE (KEEP=REPTDATE); SET LOAN.REPTDATE;  ...
# DATA REPTDATX (KEEP=REPTDATE); SET LOANP.REPTDATE; ...
#   CALL SYMPUT('PDATE', PUT(REPTDATE,Z5.));
# ==============================================================================

def read_reptdate(parquet_path: str, con: duckdb.DuckDBPyConnection) -> date:
    """
    Read the single REPTDATE value from a parquet dataset.
    Expected column: REPTDATE (stored as SAS date integer or ISO date string).
    Returns a Python date object.
    """
    df = con.execute(f"SELECT REPTDATE FROM read_parquet('{parquet_path}') LIMIT 1").pl()
    if df.is_empty():
        raise ValueError(f"No REPTDATE found in: {parquet_path}")

    val = df["REPTDATE"][0]

    # Handle SAS integer date, Python date, or string
    if isinstance(val, int):
        return sas_date_to_python(val)
    if isinstance(val, date):
        return val
    # Try parsing as string
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(str(val), fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Cannot parse REPTDATE value: {val!r}")


# ==============================================================================
# HELPER: Read first-record REPTDATE from a Kapiti flat file
#
# DATA _NULL_;
#   INFILE BNMTBL1 OBS=1;
#   INPUT @1 REPTDATE YYMMDD8.;
#   CALL SYMPUT('KAPITI1', PUT(REPTDATE, DDMMYY8.));
# RUN;
#
# The first line of each Kapiti file contains REPTDATE in YYYYMMDD (YYMMDD8.)
# format. We read it and return a DD/MM/YY string to compare against RDATE.
# ==============================================================================

def read_kapiti_reptdate(filepath: str) -> str:
    """
    Read the first line of a pipe-delimited Kapiti flat file and return
    the date formatted as DD/MM/YY (matching SAS DDMMYY8. = PUT(d, DDMMYY8.)).
    Returns empty string if file is missing or unreadable.
    """
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
            first_line = fh.readline().rstrip("\r\n")
    except OSError:
        return ""

    raw = first_line.split("|")[0].strip()
    if not raw:
        return ""

    # Parse YYYYMMDD (YYMMDD8. informat with YEARCUTOFF=1950)
    for fmt in ("%Y%m%d", "%y%m%d"):
        try:
            d = datetime.strptime(raw[:8], fmt).date()
            return d.strftime("%d/%m/%y")   # DDMMYY8. = DD/MM/YY
        except ValueError:
            continue
    return ""


# ==============================================================================
# INJECT RUNTIME PARAMETERS into a sub-module's global namespace
#
# Each sub-module (KALWE, KALWPBBS, etc.) defines module-level constants such
# as REPTMON, NOWK, PDATE, SDESC, etc., which are used to build file paths and
# apply business logic.  The SAS equivalent was CALL SYMPUT + macro variable
# resolution at compile time.  Here we inject the derived values directly into
# each module's globals before calling its main() function.
# ==============================================================================

def inject_params(module, params: dict) -> None:
    """
    Overwrite module-level runtime constants with values from params dict.
    Only keys that already exist as module attributes are updated
    (avoids accidentally creating spurious globals).
    """
    for key, val in params.items():
        if hasattr(module, key):
            setattr(module, key, val)


def build_module_params(
    mvars: dict,
    pdate: date,
    base_dir: str,
    input_dir: str,
    output_dir: str,
    bnmtbl1_txt: str,
    bnmtbl3_txt: str,
    bnmtblw_txt: str,
    dciwtbl_txt: str | None = None,
) -> dict:
    """
    Build the full parameter dict to inject into child modules.
    Maps SAS macro variable names to Python values.
    """
    reptmon_yyyymm  = mvars["REPTMON_YYYYMM"]   # e.g. "202301"
    reptmon1_yyyymm = mvars["REPTMON1_YYYYMM"]  # e.g. "202212"
    nowk            = mvars["NOWK"]              # e.g. "1"
    nowk1           = mvars["NOWK1"]             # e.g. "4"
    rdate_full      = mvars["RDATE_FULL"]        # e.g. "31/01/2023"
    sdesc           = mvars["SDESC"].strip()

    params: dict = {
        # Core period identifiers
        "REPTMON":  reptmon_yyyymm,
        "NOWK":     nowk,
        "REPTMON1": reptmon1_yyyymm,
        "NOWK1":    nowk1,

        # Display / filter values
        "RDATE":  rdate_full,
        "SDESC":  sdesc,
        "PDATE":  pdate,           # Python date object used as filter threshold

        # Directory overrides so child modules write to the correct locations
        "BASE_DIR":   base_dir,
        "INPUT_DIR":  input_dir,
        "OUTPUT_DIR": output_dir,

        # Flat-file paths for extraction modules (KALWE / KALWEI)
        "BNMTBL1_TXT": bnmtbl1_txt,
        "BNMTBL3_TXT": bnmtbl3_txt,
        "BNMTBLW_TXT": bnmtblw_txt,
    }

    # DCIWTBL only used by KALWE (PBB path); not present in KALWEI (PIBB path)
    if dciwtbl_txt is not None:
        params["DCIWTBL_TXT"] = dciwtbl_txt

    # Rebuild all parquet path attributes that depend on REPTMON / NOWK
    # These mirror the path definitions at the top of each child module.
    reptmon_nowk  = f"{reptmon_yyyymm}{nowk}"
    reptmon1_nowk1 = f"{reptmon1_yyyymm}{nowk1}"

    # KALWE / KALWEI output parquets (also inputs to KALWPBBS/KALWPBBN etc.)
    params["K1TBL_OUT"]  = os.path.join(output_dir, f"K1TBL{reptmon_nowk}.parquet")
    params["K3TBL_OUT"]  = os.path.join(output_dir, f"K3TBL{reptmon_nowk}.parquet")
    params["KWTBL_OUT"]  = os.path.join(output_dir, f"KWTBL{reptmon_nowk}.parquet")
    params["DCIWTB_OUT"] = os.path.join(output_dir, f"DCIWTB{reptmon_nowk}.parquet")

    # Input parquets consumed by KALWPBBS / KALWPBBN / KALWPIBS / KALWPIBN
    params["KWTBL_PARQUET"]  = os.path.join(output_dir, f"KWTBL{reptmon_nowk}.parquet")
    params["K3TBL_PARQUET"]  = os.path.join(output_dir, f"K3TBL{reptmon_nowk}.parquet")
    params["UTRP_PARQUET"]   = os.path.join(input_dir,  f"UTRP{reptmon_nowk}.parquet")
    params["DCIWH_PARQUET"]  = os.path.join(input_dir,  f"DCI{reptmon_nowk}.parquet")

    # Previous-period reference datasets (K3TBD for KALWPBBS/KALWPIBS, K3TBE for KALWPBBN/KALWPIBN)
    params["K3TBD_PARQUET"] = os.path.join(input_dir,  f"K3TBD{reptmon1_nowk1}.parquet")
    params["K3TBE_PARQUET"] = os.path.join(input_dir,  f"K3TBE{reptmon1_nowk1}.parquet")

    # Output parquets for period-end storage
    params["K3TBD_OUT_PARQUET"]  = os.path.join(output_dir, f"K3TBD{reptmon_nowk}.parquet")
    params["K3TBE_OUT_PARQUET"]  = os.path.join(output_dir, f"K3TBE{reptmon_nowk}.parquet")
    params["K3TBLA_OUT_PARQUET"] = os.path.join(output_dir, f"K3TBLA{reptmon_nowk}.parquet")
    params["K3TBLB_OUT_PARQUET"] = os.path.join(output_dir, f"K3TBLB{reptmon_nowk}.parquet")

    # Report output text files
    params["REPORT_OUT_TXT"] = os.path.join(output_dir, f"KALWPBBS_{reptmon_nowk}_report.txt")
    params["NSRSTXT_OUT"]    = os.path.join(output_dir, f"KALWPBBN_{reptmon_nowk}_report.txt")

    # Diagnostic print files (KALWPIBN)
    params["K3TBLA_DIAG_TXT"] = os.path.join(output_dir, f"KALWPIBN_{reptmon_nowk}_K3TBLA_diag.txt")
    params["K3TBLB_DIAG_TXT"] = os.path.join(output_dir, f"KALWPIBN_{reptmon_nowk}_K3TBLB_diag.txt")

    return params


# ==============================================================================
# PBB STEP  (first SAS step in the JCL: //EIBWKALR EXEC SAS609)
#
# 1. Read PBB LOAN.REPTDATE  -> derive macro vars
# 2. Read PBB LOANP.REPTDATE -> derive PDATE
# 3. Read first-line dates from BNMTBL1, BNMTBL3, BNMTBLW flat files
# 4. %IF "&KAPITI1"="&RDATE" AND "&KAPITI3"="&RDATE" AND "&KAPITIW"="&RDATE"
#      %THEN %DO;
#        %INC PGM(KALWE);
#        %INC PGM(KALWPBBS);
#        %INC PGM(KALWPBBN);
#      %END;
#    %ELSE %DO;  ... PUT warning messages; ABORT 77;  %END;
# ==============================================================================

def run_pbb_step(con: duckdb.DuckDBPyConnection) -> bool:
    """
    Execute the PBB processing step.
    Returns True on success, False on date-validation failure (aborts processing).
    """
    log.info("=" * 70)
    log.info("PBB STEP  (//EIBWKALR EXEC SAS609)")
    log.info("=" * 70)

    # ------------------------------------------------------------------
    # DATA REPTDATE; SET LOAN.REPTDATE;  (PBB)
    # ------------------------------------------------------------------
    log.info("Reading PBB LOAN.REPTDATE from: %s", PBB_LOAN_PARQUET)
    reptdate_pbb: date = read_reptdate(PBB_LOAN_PARQUET, con)
    log.info("PBB REPTDATE = %s", reptdate_pbb)

    # SDESC='PUBLIC BANK BERHAD';
    sdesc_pbb = "PUBLIC BANK BERHAD"

    # Derive all SAS macro variables
    mvars = derive_macro_vars(reptdate_pbb, sdesc_pbb)
    rdate = mvars["RDATE"]   # DDMMYY8. format: DD/MM/YY

    log.info(
        "PBB macros: REPTMON=%s NOWK=%s REPTMON1=%s NOWK1=%s RDATE=%s",
        mvars["REPTMON_YYYYMM"], mvars["NOWK"],
        mvars["REPTMON1_YYYYMM"], mvars["NOWK1"],
        rdate,
    )

    # ------------------------------------------------------------------
    # DATA REPTDATX; SET LOANP.REPTDATE;
    #   CALL SYMPUT('PDATE', PUT(REPTDATE,Z5.));
    # ------------------------------------------------------------------
    log.info("Reading PBB LOANP.REPTDATE from: %s", PBB_LOANP_PARQUET)
    pdate_pbb: date = read_reptdate(PBB_LOANP_PARQUET, con)
    log.info("PBB PDATE = %s  (SAS int %d)", pdate_pbb, python_date_to_sas(pdate_pbb))

    # ------------------------------------------------------------------
    # DATA _NULL_; INFILE BNMTBL1 OBS=1; -> KAPITI1
    # DATA _NULL_; INFILE BNMTBL3 OBS=1; -> KAPITI3
    # DATA _NULL_; INFILE BNMTBLW OBS=1; -> KAPITIW
    # ------------------------------------------------------------------
    kapiti1 = read_kapiti_reptdate(PBB_BNMTBL1_TXT)
    kapiti3 = read_kapiti_reptdate(PBB_BNMTBL3_TXT)
    kapitiw = read_kapiti_reptdate(PBB_BNMTBLW_TXT)

    log.info("Kapiti file dates: KAPITI1=%s  KAPITI3=%s  KAPITIW=%s", kapiti1, kapiti3, kapitiw)
    log.info("Reporting date  : RDATE   =%s", rdate)

    # ------------------------------------------------------------------
    # %MACRO PROCESS;
    #   %IF "&KAPITI1"="&RDATE" AND "&KAPITI3"="&RDATE" AND "&KAPITIW"="&RDATE"
    #   %THEN %DO;
    #     %INC PGM(KALWE); %INC PGM(KALWPBBS); %INC PGM(KALWPBBN);
    #   %END;
    #   %ELSE %DO;
    #     %PUT THE KAPITI1/3/W EXTRACTION IS NOT DATED &RDATE;
    #     %PUT THE JOB IS NOT DONE !!;
    #     DATA A; ABORT 77;
    #   %END;
    # %MEND;
    # ------------------------------------------------------------------
    date_ok = (kapiti1 == rdate) and (kapiti3 == rdate) and (kapitiw == rdate)

    if not date_ok:
        # %PUT warnings + ABORT 77
        if kapiti1 != rdate:
            log.error("THE KAPITI1 EXTRACTION IS NOT DATED %s", rdate)
        if kapiti3 != rdate:
            log.error("THE KAPITI3 EXTRACTION IS NOT DATED %s", rdate)
        if kapitiw != rdate:
            log.error("THE KAPITIW EXTRACTION IS NOT DATED %s", rdate)
        log.error("THE JOB IS NOT DONE !!")
        return False   # Abort 77 equivalent

    # ------------------------------------------------------------------
    # Build runtime parameter dict for all PBB child modules
    # ------------------------------------------------------------------
    params = build_module_params(
        mvars       = mvars,
        pdate       = pdate_pbb,
        base_dir    = BASE_DIR,
        input_dir   = INPUT_DIR,
        output_dir  = OUTPUT_DIR,
        bnmtbl1_txt = PBB_BNMTBL1_TXT,
        bnmtbl3_txt = PBB_BNMTBL3_TXT,
        bnmtblw_txt = PBB_BNMTBLW_TXT,
        dciwtbl_txt = PBB_DCIWTBL_TXT,
    )

    # ------------------------------------------------------------------
    # %INC PGM(KALWE);  - Extract Kapiti Table 1, 3, W + DCI
    # ------------------------------------------------------------------
    log.info("-" * 60)
    log.info("PBB: Running KALWE ...")
    inject_params(KALWE, params)
    KALWE.main()
    log.info("PBB: KALWE complete.")

    # ------------------------------------------------------------------
    # %INC PGM(KALWPBBS);  - RDIR Part I quoted rates (PBB, summary format)
    # ------------------------------------------------------------------
    log.info("-" * 60)
    log.info("PBB: Running KALWPBBS ...")
    inject_params(KALWPBBS, params)
    KALWPBBS.main()
    log.info("PBB: KALWPBBS complete.")

    # ------------------------------------------------------------------
    # %INC PGM(KALWPBBN);  - RDIR Part I quoted rates (PBB, NSRS format)
    # ------------------------------------------------------------------
    log.info("-" * 60)
    log.info("PBB: Running KALWPBBN ...")
    inject_params(KALWPBBN, params)
    KALWPBBN.main()
    log.info("PBB: KALWPBBN complete.")

    return True


# ==============================================================================
# PIBB STEP  (second SAS step in the JCL: //EIIWKALR EXEC SAS609)
#
# 1. Read PIBB LOANP.REPTDATE -> derive PDATE  (note: REPTDATX is read FIRST here)
# 2. Read PIBB LOAN.REPTDATE  -> derive macro vars
# 3. No Kapiti date-validation check (unconditional execution)
# 4. %INC PGM(KALWEI);
#    %INC PGM(KALWPIBS);
#    %INC PGM(KALWPIBN);
# ==============================================================================

def run_pibb_step(con: duckdb.DuckDBPyConnection) -> None:
    """Execute the PIBB (Islamic) processing step."""
    log.info("=" * 70)
    log.info("PIBB STEP  (//EIIWKALR EXEC SAS609)")
    log.info("=" * 70)

    # ------------------------------------------------------------------
    # DATA REPTDATX; SET LOANP.REPTDATE;  (PIBB - read first in this step)
    #   CALL SYMPUT('PDATE', PUT(REPTDATE,Z5.));
    # ------------------------------------------------------------------
    log.info("Reading PIBB LOANP.REPTDATE from: %s", PIBB_LOANP_PARQUET)
    pdate_pibb: date = read_reptdate(PIBB_LOANP_PARQUET, con)
    log.info("PIBB PDATE = %s  (SAS int %d)", pdate_pibb, python_date_to_sas(pdate_pibb))

    # ------------------------------------------------------------------
    # DATA REPTDATE; SET LOAN.REPTDATE;  (PIBB)
    # ------------------------------------------------------------------
    log.info("Reading PIBB LOAN.REPTDATE from: %s", PIBB_LOAN_PARQUET)
    reptdate_pibb: date = read_reptdate(PIBB_LOAN_PARQUET, con)
    log.info("PIBB REPTDATE = %s", reptdate_pibb)

    # SDESC='PUBLIC ISLAMIC BANK BERHAD';
    sdesc_pibb = "PUBLIC ISLAMIC BANK BERHAD"

    # Derive all SAS macro variables
    mvars = derive_macro_vars(reptdate_pibb, sdesc_pibb)

    log.info(
        "PIBB macros: REPTMON=%s NOWK=%s REPTMON1=%s NOWK1=%s RDATE=%s",
        mvars["REPTMON_YYYYMM"], mvars["NOWK"],
        mvars["REPTMON1_YYYYMM"], mvars["NOWK1"],
        mvars["RDATE"],
    )

    # ------------------------------------------------------------------
    # Build runtime parameter dict for all PIBB child modules
    # DCIWTBL is not used in the PIBB step (no DCIWTBL DD card)
    # ------------------------------------------------------------------
    params = build_module_params(
        mvars       = mvars,
        pdate       = pdate_pibb,
        base_dir    = BASE_DIR,
        input_dir   = INPUT_DIR,
        output_dir  = OUTPUT_DIR,
        bnmtbl1_txt = PIBB_BNMTBL1_TXT,
        bnmtbl3_txt = PIBB_BNMTBL3_TXT,
        bnmtblw_txt = PIBB_BNMTBLW_TXT,
        dciwtbl_txt = None,   # No DCIWTBL DD for PIBB
    )

    # ------------------------------------------------------------------
    # %INC PGM(KALWEI);  - Extract Islamic Kapiti Table 1, 3, W
    # ------------------------------------------------------------------
    log.info("-" * 60)
    log.info("PIBB: Running KALWEI ...")
    inject_params(KALWEI, params)
    KALWEI.main()
    log.info("PIBB: KALWEI complete.")

    # ------------------------------------------------------------------
    # %INC PGM(KALWPIBS);  - RDIR Part I Islamic quoted rates (summary format)
    # ------------------------------------------------------------------
    log.info("-" * 60)
    log.info("PIBB: Running KALWPIBS ...")
    inject_params(KALWPIBS, params)
    KALWPIBS.main()
    log.info("PIBB: KALWPIBS complete.")

    # ------------------------------------------------------------------
    # %INC PGM(KALWPIBN);  - RDIR Part I Islamic quoted rates (NSRS format)
    # ------------------------------------------------------------------
    log.info("-" * 60)
    log.info("PIBB: Running KALWPIBN ...")
    inject_params(KALWPIBN, params)
    KALWPIBN.main()
    log.info("PIBB: KALWPIBN complete.")


# ==============================================================================
# MAIN
# ==============================================================================

def main() -> None:
    """
    Orchestrate the full EIBWKALR job:
      Step 1 - PBB  path: KALWE  -> KALWPBBS -> KALWPBBN
      Step 2 - PIBB path: KALWEI -> KALWPIBS -> KALWPIBN
    """
    log.info("EIBWKALR job started.")

    con = duckdb.connect()

    try:
        # ==================================================================
        # PBB STEP
        # ==================================================================
        pbb_ok = run_pbb_step(con)

        if not pbb_ok:
            # Replicates ABORT 77: log and exit with non-zero return code
            log.error("PBB step aborted due to Kapiti date mismatch. EIBWKALR terminated.")
            sys.exit(77)

        # ==================================================================
        # PIBB STEP  (Islamic)
        # Note: In the original JCL this is a separate //EIIWKALR EXEC step.
        #       There is no date-validation guard in the PIBB step; it always runs.
        # ==================================================================
        run_pibb_step(con)

    finally:
        con.close()

    log.info("EIBWKALR job completed successfully.")


if __name__ == "__main__":
    main()
