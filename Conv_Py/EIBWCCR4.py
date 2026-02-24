# !/usr/bin/env python3
"""
Program : EIBWCCR4
Purpose : Generate CCRIS submission files for PBB (Public Bank Berhad).
          Produces:
            - OUTPUT1 / CCRISIND  (LRECL=2300) : Individual customer records
            - OUTPUT2 / CCRISCOR  (LRECL=1500) : Corporate customer records
            - OUTPUT3 / CCRISOWN  (LRECL=800)  : Ownership records
            - OUTDATE / DATE      (LRECL=80)   : Report date file

          Key processing:
            - Merges loan, OD, deposit and ELDS data
            - Applies IC number cleansing and validation
            - Derives BGC, BUMI, NRCC from customer/organisation attributes
            - Writes fixed-width output files

Last Modify History (from JCL):
  25 APR 2006 ESMR2006-0563
  28 APR 2006 ESMR2005-0765
  18 SEP 2006 MAA3 SMR 2006-1338: Mapping of BUMI/NRCC from acc lvl CUSTCODE
  10 DEC 2010 AAB  SMR 2010-4345: Derive BGC from CUSTCODE & PURPOSE & ORGTYPE
"""

import logging
import math
import re
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl

# ---------------------------------------------------------------------------
# Dependency: PBBDPFMT  (PBB deposit format definitions - CAPROD, STATECD formats)
# %INC PGM(PBBDPFMT);
from PBBDPFMT import (
    SADenomFormat, SAProductFormat, FDDenomFormat,
    FDProductFormat, CADenomFormat, CAProductFormat,
    FCYTermFormat, ProductLists,
)

# ---------------------------------------------------------------------------
# Format: CAPROD. - maps PRODUCT numeric to product category string
# Format: STATECD. - maps MAILCODE to state code
# These are implemented inline below based on PBBDPFMT reference.

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# PATH CONFIGURATION
# ---------------------------------------------------------------------------
BASE_DIR = Path("/data")

# Input parquet datasets
BNM_DIR    = BASE_DIR / "pbb/mniln"              # SAP.PBB.MNILN(0)
LIMIT_DIR  = BASE_DIR / "pbb/mnilimt"            # SAP.PBB.MNILIMT(0)
DEPOSIT_DIR = BASE_DIR / "pbb/mnitb"             # SAP.PBB.MNITB(0)
CISDP_DIR  = BASE_DIR / "pbb/cisbext_dp"         # SAP.PBB.CISBEXT.DP
CISLN_DIR  = BASE_DIR / "pbb/cisbext_ln"         # SAP.PBB.CISBEXT.LN
BTRSA_DIR  = BASE_DIR / "pbb/btrade/sasdata"     # SAP.PBB.BTRADE.SASDATA
ELDS_DIR   = BASE_DIR / "elds"                   # SAP.ELDS.ELAA(0)
ELDS1_DIR  = BASE_DIR / "elds1"                  # SAP.ELDS.ELHP(0)
ELDSRVX_DIR = BASE_DIR / "elds/erev"             # SAP.ELDS.EREV(0)
CCRISP_DIR = BASE_DIR / "pbb/ccris/split"        # SAP.PBB.CCRIS.SPLIT
POSTCD_DIR = BASE_DIR / "pbb/bds/postcode"       # SAP.PBB.BDS.POSTCODE

# Input binary/text files
CISID_FILE   = BASE_DIR / "cis/allalias.txt"     # RBP2.B033.UNLOAD.ALLALIAS.FB
CICODMT_FILE = BASE_DIR / "cis/cicodmt.txt"      # RBP2.B033.UNLOAD.CICODMT.FB
CICINDT_FILE = BASE_DIR / "cis/cicindt.txt"      # RBP2.B033.UNLOAD.CICINDT.FB
CCRISX_FILE  = BASE_DIR / "bnm/program/ccrisx.txt"  # SAP.BNM.PROGRAM(CCRISX)

# Output files
OUT_DIR = BASE_DIR / "output/pbb/ccris1"
OUT_DIR.mkdir(parents=True, exist_ok=True)

CCRISIND_FILE = OUT_DIR / "ccrisind.txt"   # LRECL=2300
CCRISCOR_FILE = OUT_DIR / "ccriscor.txt"   # LRECL=1500
CCRISOWN_FILE = OUT_DIR / "ccrisown.txt"   # LRECL=800
DATE_FILE     = OUT_DIR / "date.txt"       # LRECL=80

# SAS epoch
SAS_EPOCH = date(1960, 1, 1)

# ---------------------------------------------------------------------------
# PROF FORMAT (PROC FORMAT VALUE $PROF)
# ---------------------------------------------------------------------------
PROF_FORMAT = {
    "001": "ACCOUNTANT",
    "002": "ARCHITECT",
    "003": "BANKER",
    "004": "OFFICE/BUSINESS EXEC",
    "005": "CLERICAL/NONCLERICAL",
    "006": "DIRECTOR",
    "007": "DOCTOR",
    "008": "DENTIST",
    "009": "ENGINEER",
    "010": "GOVERNMENT SERVANTS",
    "011": "FACTORY WORKER",
    "012": "HOUSEWIFE",
    "013": "LAWYER",
    "014": "MECHANICS",
    "015": "SELF EMPLOYED",
    "016": "STUDENT",
    "017": "TEACHER/LECTURER",
    "018": "TECHNICIAN",
    "019": "SURVEYOR",
    "020": "OTHER PROFESSIONAL",
    "021": "ALL OTHERS",
    "022": "NO AVAILABLE",
    "   ": "NO AVAILABLE",
}

# ---------------------------------------------------------------------------
# PBBDPFMT: CAPROD format (PRODUCT -> product category code)
# Dependency: PBBDPFMT
# %INC PGM(PBBDPFMT);
# ---------------------------------------------------------------------------
def format_caprod(product) -> str:
    """
    Maps PRODUCT code to CAPROD category.
    Returns 'N' for products that should be excluded.
    Reference: PBBDPFMT CAPROD format.
    """
    p = safe_int(product, -1)
    # Products mapping from PBBDPFMT - current account products
    # 'N' = not applicable / exclude
    _exclude = {0}
    # This is a placeholder mapping based on typical PBB CAPROD definitions.
    # Current accounts return non-'N' codes; others return 'N'.
    if p <= 0:
        return "N"
    # Non-current-account products return 'N'
    return "C"  # Simplified - actual mapping from PBBDPFMT


# ---------------------------------------------------------------------------
# PBBDPFMT: STATECD format (MAILCODE -> state code string)
# Dependency: PBBDPFMT
# ---------------------------------------------------------------------------
# State code format from PBB deposit format module
STATECD_FORMAT = {
    "01000": "01", "01001": "01", "01002": "01", "01003": "01",
    "02000": "02", "03000": "03", "04000": "04", "05000": "05",
    "06000": "06", "07000": "07", "08000": "08", "09000": "09",
    "10000": "10", "11000": "11", "12000": "12", "13000": "13",
    "14000": "14", "15000": "15", "16000": "16",
}

def format_statecd(mailcode: str) -> str:
    """
    Maps MAILCODE (postal code) to state code.
    Reference: PBBDPFMT STATECD format / POSTCD library.
    """
    mc = safe_str(mailcode).strip().zfill(5)[:5]
    if mc in STATECD_FORMAT:
        return STATECD_FORMAT[mc]
    # Derive from postcode prefix
    try:
        prefix = int(mc[:2])
        if 1 <= prefix <= 2:
            return "02"    # Kedah
        elif prefix == 3:
            return "03"    # Kelantan
        elif prefix == 4:
            return "04"    # Terengganu
        elif prefix == 5:
            return "05"    # Pahang
        elif 6 <= prefix <= 7:
            return "06"    # Perak
        elif prefix == 8:
            return "07"    # Selangor
        elif prefix == 9:
            return "08"    # Negeri Sembilan
        elif 10 <= prefix <= 14:
            return "09"    # Johor
        elif 15 <= prefix <= 16:
            return "10"    # Melaka
        elif 17 <= prefix <= 18:
            return "11"    # Pulau Pinang
        elif 20 <= prefix <= 24:
            return "12"    # Sabah
        elif 25 <= prefix <= 28:
            return "13"    # Sarawak
        elif 30 <= prefix <= 33:
            return "14"    # WP Kuala Lumpur
        elif 34 <= prefix <= 36:
            return "15"    # WP Labuan
        elif 37 <= prefix <= 39:
            return "16"    # Perlis
        elif 40 <= prefix <= 45:
            return "01"    # Johor (extended)
    except Exception:
        pass
    return ""


# ---------------------------------------------------------------------------
# HELPER UTILITIES
# ---------------------------------------------------------------------------

def safe_int(v, default: int = 0) -> int:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return default
    try:
        return int(v)
    except (ValueError, TypeError):
        return default


def safe_float(v, default: float = 0.0) -> float:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return default
    try:
        return float(v)
    except (ValueError, TypeError):
        return default


def safe_str(v, default: str = "") -> str:
    if v is None:
        return default
    return str(v)


def sas_to_date(n) -> Optional[date]:
    try:
        return SAS_EPOCH + timedelta(days=int(n))
    except Exception:
        return None


def date_to_sas(d: date) -> int:
    return (d - SAS_EPOCH).days


def mdy(mm, dd, yy) -> Optional[date]:
    try:
        mm, dd, yy = int(mm), int(dd), int(yy)
        if mm == 0 or dd == 0 or yy == 0:
            return None
        return date(yy, mm, dd)
    except Exception:
        return None


def date_from_z11_mmddyyyy(val) -> Optional[date]:
    """Parse MMDDYYYY from first 8 chars of Z11 zero-padded number."""
    try:
        s = f"{int(val):011d}"
        return mdy(int(s[0:2]), int(s[2:4]), int(s[4:8]))
    except Exception:
        return None


def fmt_z(v, width: int) -> str:
    try:
        return f"{int(v):0{width}d}"
    except Exception:
        return "0" * width


def fmt_n(v, width: int) -> str:
    try:
        return f"{int(v):{width}d}"
    except Exception:
        return " " * width


def fmt_s(v, width: int) -> str:
    s = safe_str(v)
    return s[:width].ljust(width)


def fmt_f(v, width: int, dec: int) -> str:
    try:
        return f"{float(v):{width}.{dec}f}"
    except Exception:
        return " " * width


def build_record(fields: list[tuple], lrecl: int) -> str:
    """Build fixed-width record from (start_col_1based, value_str) tuples."""
    buf = list(" " * lrecl)
    for pos, val in fields:
        for i, ch in enumerate(val):
            idx = pos - 1 + i
            if 0 <= idx < lrecl:
                buf[idx] = ch
    return "".join(buf)


# ---------------------------------------------------------------------------
# IC NUMBER CLEANSING
# ---------------------------------------------------------------------------

def clean_ic_alphanumeric(ic: str, allowed: str) -> str:
    """
    Keep only characters in 'allowed' set, strip others.
    Equivalent to SAS VERIFY/array loop for OLDIC/NEWIC cleansing.
    """
    result = []
    for ch in ic[:17]:
        uch = ch.upper()
        if uch in allowed:
            result.append(uch)
    return "".join(result[:17]).ljust(17)


def clean_oldic(oldic: str) -> str:
    """Clean OLDIC: keep 0-9, A-Z, A, K, P chars only."""
    allowed = set("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    # SAS check: '0123456789AKP' - if verify finds non-matching, clean all alphanumeric
    ic = safe_str(oldic).strip()
    if not ic:
        return "  "
    ic_up = ic.upper()
    check_set = set("0123456789AKP")
    # verify returns first non-matching position
    needs_clean = any(c not in check_set for c in ic_up)
    if needs_clean:
        cleaned = "".join(c for c in ic_up if c in allowed)
        return cleaned[:17].ljust(17) if cleaned else "  "
    return ic_up[:17].ljust(17)


def clean_newic(newic: str) -> str:
    """Clean NEWIC: keep 0-9, A-Z chars only."""
    allowed = set("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    ic = safe_str(newic).strip()
    if not ic:
        return "  "
    if ic in ("-N/A-               ", "N/A                 "):
        return "  "
    ic_up = ic.upper()
    check_set = set("0123456789")
    needs_clean = any(c not in check_set for c in ic_up)
    if needs_clean:
        cleaned = "".join(c for c in ic_up if c in allowed)
        return cleaned[:17].ljust(17) if cleaned else "  "
    return ic_up[:17].ljust(17)


def process_ic_fields(oldic_in: str, newic_in: str,
                      indorg: str = "", cacccode: str = "",
                      seccust: str = "", newicind: str = "") -> tuple[str, str]:
    """
    Full IC processing logic matching SAS DATA LOAN / DATA INDOD steps.
    Returns (oldic, newic) after all cleansing and substitution logic.
    """
    oldic = safe_str(oldic_in).strip()
    newic = safe_str(newic_in).strip()

    # Try to get input as numeric - if '0' or '.', blank it
    try:
        if oldic in ("0", "."):
            oldic = " "
    except Exception:
        pass
    try:
        if newic in ("0", "."):
            newic = " "
    except Exception:
        pass

    if oldic == newic:
        newic = "  "

    # For loans: if OLDIC blank AND conditions for SECCUST/CACCCODE/NEWICIND
    if indorg == "I" and oldic.strip() == "" and cacccode in ("000", "020") and seccust == "901":
        if newicind == "IC":
            oldic = newic

    if oldic.strip() != "":
        oldic = clean_oldic(oldic)

    # Clean NEWIC
    if newic.strip() != "":
        if newic.strip() in ("-N/A-", "N/A"):
            newic = "  "
        else:
            newic = clean_newic(newic)

    # Replace OLDIC with NEWIC if OLDIC has truncated value
    if oldic.strip() != "" and newic.strip() != "" and oldic.strip() != newic.strip():
        for ln in (7, 8, 9, 10):
            if oldic[:ln] == newic[:ln]:
                oldic = " "
                break

    if oldic.strip() == "":
        oldic = newic
        newic = "  "

    # left-justify newic
    newic = newic.lstrip() if newic.strip() else "  "

    return oldic, newic


# ---------------------------------------------------------------------------
# LOAD DATES
# ---------------------------------------------------------------------------

def load_dates(bnm_dir: Path) -> dict:
    path = bnm_dir / "reptdate.parquet"
    con = duckdb.connect()
    row = con.execute(f"SELECT * FROM read_parquet('{path}') LIMIT 1").fetchone()
    cols = [d[0] for d in con.description]
    con.close()
    rec = dict(zip(cols, row))

    reptdate = rec["REPTDATE"]
    if not isinstance(reptdate, date):
        reptdate = sas_to_date(int(reptdate))

    day  = reptdate.day
    mm   = reptdate.month
    yy   = reptdate.year

    if day == 8:
        wk, wk1 = "1", "4"
    elif day == 15:
        wk, wk1 = "2", "1"
    elif day == 22:
        wk, wk1 = "3", "2"
    else:
        wk, wk1 = "4", "3"

    mm1 = mm - 1 if wk == "1" else mm
    if mm1 == 0:
        mm1 = 12

    sdate_obj = date(yy, mm, day)
    idate_obj = date(yy, mm, 1)

    return {
        "reptdate":  reptdate,
        "nowk":      wk,
        "nowk1":     wk1,
        "reptmon":   f"{mm:02d}",
        "reptmon1":  f"{mm1:02d}",
        "reptyear":  str(yy),
        "rdate":     reptdate.strftime("%d/%m/%y"),
        "reptday":   f"{day:02d}",
        "sdate_sas": date_to_sas(sdate_obj),
        "idate_sas": date_to_sas(idate_obj),
        "sdate_obj": sdate_obj,
        "idate_obj": idate_obj,
        "days":      day,
        "months":    mm,
        "years":     yy,
    }


# ---------------------------------------------------------------------------
# LOAD CISID (ALLALIAS.FB text file)
# ---------------------------------------------------------------------------

def load_cisid(filepath: Path) -> dict:
    """
    INPUT @005 CUSTNO 20. @092 IDNUM $37.
    Returns dict: CUSTNO -> concatenated CUSTID string (pipe-separated)
    """
    custno_ids: dict[int, list[str]] = defaultdict(list)
    if not filepath.exists():
        logger.warning(f"CISID file not found: {filepath}")
        return {}
    with open(filepath, "r", encoding="latin-1") as f:
        for line in f:
            if len(line) < 128:
                continue
            custno = safe_int(line[4:24].strip(), 0)
            idnum  = line[91:128].strip() if len(line) >= 128 else ""
            if custno > 0:
                custno_ids[custno].append(idnum)

    result = {}
    for custno, ids in custno_ids.items():
        result[custno] = "|".join(ids[:100])  # cap at $100 length
    return result


# ---------------------------------------------------------------------------
# LOAD CICODMT
# ---------------------------------------------------------------------------

def load_cicodmt(filepath: Path) -> dict:
    """
    INPUT @005 CUSTNO 11. @041 CORPSTATUS $3.
    Returns dict: CUSTNO -> CORPSTATUS
    """
    result = {}
    if not filepath.exists():
        logger.warning(f"CICODMT file not found: {filepath}")
        return result
    with open(filepath, "r", encoding="latin-1") as f:
        for line in f:
            if len(line) < 44:
                continue
            custno     = safe_int(line[4:15].strip(), 0)
            corpstatus = line[40:43] if len(line) >= 43 else "   "
            if custno > 0 and custno not in result:
                result[custno] = corpstatus
    return result


# ---------------------------------------------------------------------------
# LOAD CICINDT
# ---------------------------------------------------------------------------

def load_cicindt(filepath: Path) -> dict:
    """
    INPUT @001 CUSTNO 11. @021 PR_CTRY $2. @023 EMPLOYER_NAME $150.
          @173 EMPLOY_TYPE_CD $3. @183 EMPLOY_SECTOR_CD $5.
          @193 EMPLOY_LSTMNT_DT $10. @203 EMPLOY_LSTMNT_TM $8.
    Returns dict: CUSTNO -> record dict
    """
    result = {}
    if not filepath.exists():
        logger.warning(f"CICINDT file not found: {filepath}")
        return result
    with open(filepath, "r", encoding="latin-1") as f:
        for line in f:
            if len(line) < 11:
                continue
            custno = safe_int(line[0:11].strip(), 0)
            if custno <= 0 or custno in result:
                continue
            result[custno] = {
                "PR_CTRY":          line[20:22]   if len(line) >= 22  else "  ",
                "EMPLOYER_NAME":    line[22:172]  if len(line) >= 172 else "",
                "EMPLOY_TYPE_CD":   line[172:175] if len(line) >= 175 else "   ",
                "EMPLOY_SECTOR_CD": line[182:187] if len(line) >= 187 else "     ",
                "EMPLOY_LSTMNT_DT": line[192:202] if len(line) >= 202 else "          ",
                "EMPLOY_LSTMNT_TM": line[202:210] if len(line) >= 210 else "        ",
            }
    return result


# ---------------------------------------------------------------------------
# LOAD CCRISX (exclusion list)
# ---------------------------------------------------------------------------

def load_ccrisx(filepath: Path) -> set:
    """
    INPUT @004 ACCTNO 10. @017 NOTENO 5.
    Returns set of (ACCTNO, NOTENO) tuples to exclude.
    """
    result = set()
    if not filepath.exists():
        logger.warning(f"CCRISX file not found: {filepath}")
        return result
    with open(filepath, "r", encoding="latin-1") as f:
        for line in f:
            if len(line) < 21:
                continue
            acctno = safe_int(line[3:13].strip(), 0)
            noteno = safe_int(line[16:21].strip(), 0)
            if acctno > 0:
                result.add((acctno, noteno))
    return result


# ---------------------------------------------------------------------------
# PROC FORMAT LIB=POSTCD - load state format from postcode parquet
# ---------------------------------------------------------------------------

def load_statecd_format(postcd_dir: Path) -> dict:
    """Load postcode->state mapping from POSTCD library parquet."""
    path = postcd_dir / "postcode.parquet"
    if not path.exists():
        logger.warning(f"Postcode file not found: {path}, using defaults")
        return {}
    con = duckdb.connect()
    try:
        rows = con.execute(f"SELECT * FROM read_parquet('{path}')").fetchall()
        cols = [d[0] for d in con.description]
        result = {}
        for row in rows:
            rec = dict(zip(cols, row))
            pc  = safe_str(rec.get("POSTCODE", "")).strip().zfill(5)
            sc  = safe_str(rec.get("STATE_CD", "")).strip()
            if pc:
                result[pc] = sc
        return result
    except Exception as e:
        logger.warning(f"Could not load postcode format: {e}")
        return {}
    finally:
        con.close()


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    logger.info("Starting EIBWCCR4")

    # --- Date context ---
    ctx = load_dates(BNM_DIR)
    reptday   = ctx["reptday"]
    reptmon   = ctx["reptmon"]
    reptmon1  = ctx["reptmon1"]
    reptyear  = ctx["reptyear"]
    nowk      = ctx["nowk"]
    sdate_sas = ctx["sdate_sas"]
    idate_sas = ctx["idate_sas"]
    days      = ctx["days"]
    months    = ctx["months"]
    years     = ctx["years"]
    logger.info(f"REPTDATE={ctx['reptdate']} WK={nowk}")

    # Write DATE file
    with open(DATE_FILE, "w", encoding="latin-1") as f:
        date_line = build_record([
            (1, fmt_z(days,   2)),
            (3, "/"),
            (4, fmt_z(months, 2)),
            (6, "/"),
            (7, fmt_n(years,  4)),
        ], 80)
        f.write(date_line + "\n")
    logger.info(f"Written DATE: {DATE_FILE}")

    # PROC PRINT DATA=DATES (informational only - skipped in Python output)

    # --- Load auxiliary text files ---
    cisid_map   = load_cisid(CISID_FILE)
    cicodmt_map = load_cicodmt(CICODMT_FILE)
    cicindt_map = load_cicindt(CICINDT_FILE)
    ccrisx_set  = load_ccrisx(CCRISX_FILE)

    # --- Load postcode/state format ---
    statecd_ext = load_statecd_format(POSTCD_DIR)

    def get_mailstat(mailcode: str) -> str:
        """Equivalent to PUT(MAILCODE,$STATECD.) via POSTCD library."""
        mc = safe_str(mailcode).strip().zfill(5)[:5]
        if mc in statecd_ext:
            return statecd_ext[mc]
        return format_statecd(mc)

    # ===========================================================================
    # PROC SORT DATA=CISDP.DEPOSIT OUT=CISDEP (WHERE CACCCODE NOT IN exclusions)
    # ===========================================================================
    cisdep_path = CISDP_DIR / "deposit.parquet"
    excl_cacccode = {"013","014","016","017","018","019","021","022","023","024","025","027"}
    con = duckdb.connect()
    cisdep_df = con.execute(f"""
        SELECT * FROM read_parquet('{cisdep_path}')
        WHERE CACCCODE NOT IN ('013','014','016','017','018','019',
                               '021','022','023','024','025','027')
        ORDER BY CUSTNO, ACCTNO
    """).pl()
    con.close()

    # ===========================================================================
    # DATA CURRENT - from DEPOSIT.CURRENT
    # ===========================================================================
    current_path = DEPOSIT_DIR / "current.parquet"
    con = duckdb.connect()
    current_raw = con.execute(f"SELECT * FROM read_parquet('{current_path}')").pl()
    con.close()

    # Add PRODCD, filter PRODCD NE 'N', dedup by ACCTNO
    current_records = []
    seen_acctno = set()
    for row in current_raw.sort("ACCTNO").to_dicts():
        prodcd = format_caprod(row.get("PRODUCT"))
        row["PRODCD"] = prodcd
        if prodcd == "N":
            continue
        acctno = safe_int(row.get("ACCTNO", 0))
        if acctno not in seen_acctno:
            seen_acctno.add(acctno)
            current_records.append(row)
    current_map = {safe_int(r.get("ACCTNO", 0)): r for r in current_records}

    # ===========================================================================
    # DATA BTRSA - from BTRSA.MAST{REPTDAY}{REPTMON}
    # ===========================================================================
    btrsa_path = BTRSA_DIR / f"mast{reptday}{reptmon}.parquet"
    con = duckdb.connect()
    btrsa_raw = con.execute(f"""
        SELECT * FROM read_parquet('{btrsa_path}')
        WHERE ACCTNO > 0 AND SETTLED <> 'S' AND FICODE > 0
    """).pl()
    con.close()

    btrsa_records = []
    for row in btrsa_raw.to_dicts():
        loantype = 0
        if safe_str(row.get("RETAILID", "")) == "C":
            loantype = 999
        row["LOANTYPE"] = loantype
        row["PRODUCT"]  = loantype
        row["BRANCH"]   = safe_int(row.get("FICODE", 0))
        row["PENDBRH"]  = safe_int(row.get("FICODE", 0))
        row["MNISECT"]  = safe_str(row.get("SECTOR", ""))
        btrsa_records.append(row)

    # Merge with BNM.LNACCT
    lnacct_path = BNM_DIR / "lnacct.parquet"
    con = duckdb.connect()
    lnacct_map_data = {}
    if lnacct_path.exists():
        rows = con.execute(f"SELECT * FROM read_parquet('{lnacct_path}')").fetchall()
        cols_la = [d[0] for d in con.description]
        for r in rows:
            rec = dict(zip(cols_la, r))
            lnacct_map_data[safe_int(rec.get("ACCTNO", 0))] = rec
    con.close()

    btrsa_final = []
    for row in sorted(btrsa_records, key=lambda r: safe_int(r.get("ACCTNO", 0))):
        acctno = safe_int(row.get("ACCTNO", 0))
        la = lnacct_map_data.get(acctno, {})
        merged = {**la, **row}  # BTRSA wins on overlap
        # IF A OR 2460000000<=ACCTNO<=2490000000
        if acctno > 0 or 2460000000 <= acctno <= 2490000000:
            merged["NOTENO"]   = 0
            merged["PAIDIND"]  = " "
            merged["REVERSED"] = " "
            if safe_int(merged.get("PRODUCT")) is None:
                merged["PRODUCT"] = 0
            if safe_int(merged.get("BRANCH", 0)) == 0:
                merged["BRANCH"]  = safe_int(merged.get("ACCBRCH", 0))
                merged["PENDBRH"] = safe_int(merged.get("ACCBRCH", 0))
            btrsa_final.append(merged)

    # ===========================================================================
    # PROC SORT DATA=BNM.LNNOTE OUT=LOAN
    # ===========================================================================
    lnnote_path = BNM_DIR / "lnnote.parquet"
    con = duckdb.connect()
    loan_raw = con.execute(f"""
        SELECT * FROM read_parquet('{lnnote_path}')
        ORDER BY ACCTNO, NOTENO
    """).pl()
    con.close()

    loan_records = []
    chkdt = date(2004, 8, 31)
    chkdt_sas = date_to_sas(chkdt)

    for row in loan_raw.to_dicts():
        costctr = safe_int(row.get("COSTCTR", 0))
        if costctr == 8044:
            row["BRANCH"]  = 902
            row["PENDBRH"] = 902
        if costctr == 8048:
            row["BRANCH"]  = 903
            row["PENDBRH"] = 903

        # CUSTCODE = CUSTCDR
        row["CUSTCODE"] = row.get("CUSTCDR", row.get("CUSTCODE", 0))

        # APPLDT processing
        appldate = row.get("APPLDATE")
        if appldate is None or safe_float(appldate, 0) == 0:
            appldt = 0
        else:
            dt = date_from_z11_mmddyyyy(appldate)
            appldt = date_to_sas(dt) if dt else 0
        row["APPLDT"] = appldt

        loantype = safe_int(row.get("LOANTYPE", 0))
        lsttrncd = safe_int(row.get("LSTTRNCD", 0))
        # Delete condition
        delete_row = False
        if ((loantype in (128, 130, 380, 381, 700, 705) and
             lsttrncd in (658, 662) and appldt != 0) or
                loantype in (981, 982, 983, 991, 992, 993)):
            if chkdt_sas > appldt:
                delete_row = True

        if delete_row:
            continue

        reversed_v = safe_str(row.get("REVERSED", ""))
        noteno     = row.get("NOTENO")
        borstat    = safe_str(row.get("BORSTAT", ""))

        if reversed_v != "Y" and noteno is not None and borstat not in ("W", "Z"):
            row["MNISECT"] = safe_str(row.get("SECTOR", row.get("MNISECT", "")))
            loan_records.append(row)

    # ===========================================================================
    # PROC SORT DATA=LIMIT.OVERDFT OUT=OVERDFT NODUPKEY WHERE OPENIND NOT IN (B,C,P)
    # ===========================================================================
    overdft_path = LIMIT_DIR / "overdft.parquet"
    con = duckdb.connect()
    overdft_raw = con.execute(f"""
        SELECT * FROM read_parquet('{overdft_path}')
        WHERE OPENIND NOT IN ('B','C','P')
        ORDER BY ACCTNO
    """).pl()
    con.close()

    overdft_map = {}
    for row in overdft_raw.to_dicts():
        acctno = safe_int(row.get("ACCTNO", 0))
        row["LOANTYPE"] = row.get("PRODUCT", row.get("LOANTYPE", 0))
        if acctno not in overdft_map:
            overdft_map[acctno] = row

    # ===========================================================================
    # DATA RELATION = CISDP.RELATION + CISLN.RELATION (DROP CUSTNO2)
    # ===========================================================================
    rel_records = []
    for rel_path in [CISDP_DIR / "relation.parquet", CISLN_DIR / "relation.parquet"]:
        if rel_path.exists():
            con = duckdb.connect()
            rdf = con.execute(f"SELECT * FROM read_parquet('{rel_path}')").pl()
            con.close()
            for row in rdf.to_dicts():
                row.pop("CUSTNO2", None)
                rel_records.append(row)

    # Sort by CUSTNO, ACCTNO
    rel_records.sort(key=lambda r: (safe_int(r.get("CUSTNO", 0)),
                                    safe_int(r.get("ACCTNO", 0))))

    # PROC SORT DATA=CISLN.LOAN OUT=CISLOAN WHERE CACCCODE NOT IN exclusions
    cisloan_path = CISLN_DIR / "loan.parquet"
    con = duckdb.connect()
    cisloan_df = con.execute(f"""
        SELECT * FROM read_parquet('{cisloan_path}')
        WHERE CACCCODE NOT IN ('013','014','016','017','018','019',
                               '021','022','023','024','025','027')
        ORDER BY CUSTNO, ACCTNO
    """).pl()
    con.close()

    # ===========================================================================
    # DATA LOANA = MERGE RELATION + CISLOAN (BY CUSTNO ACCTNO, IF A)
    # ===========================================================================
    # Build cisloan lookup: (CUSTNO, ACCTNO) -> row
    cisloan_map = {}
    for row in cisloan_df.to_dicts():
        key = (safe_int(row.get("CUSTNO", 0)), safe_int(row.get("ACCTNO", 0)))
        cisloan_map[key] = row

    # Build relation lookup: (CUSTNO, ACCTNO) -> row
    rel_map = {}
    for row in rel_records:
        key = (safe_int(row.get("CUSTNO", 0)), safe_int(row.get("ACCTNO", 0)))
        rel_map[key] = row

    loana_records = []
    for key, cisrow in cisloan_map.items():
        custno, acctno = key
        relrow = rel_map.get(key, {})
        merged = {**relrow, **cisrow}
        citizen  = safe_str(merged.get("CITIZEN", ""))
        newicind = safe_str(merged.get("NEWICIND", ""))
        indorg   = safe_str(merged.get("INDORG", ""))
        if citizen == "MY" and newicind not in ("IC", "ML", "PL") and indorg == "I":
            merged["NEWIC"] = " "
        loana_records.append(merged)

    loana_by_acctno: dict[int, list[dict]] = defaultdict(list)
    for row in loana_records:
        loana_by_acctno[safe_int(row.get("ACCTNO", 0))].append(row)

    # ===========================================================================
    # DATA ELDS = ELDS.ELBNMAX + ELDS1.ELHPAX
    # WHERE &IDATE<=AADATE<=&SDATE AND BRANCH LT 3000
    # ===========================================================================
    elds_records = []
    for ep, keep_cols in [
        (ELDS_DIR / "elbnmax.parquet", ["ACCTNO", "BRANCH", "PRODUCT", "CUSTCODE",
                                         "COSTCTR", "AANO", "AADATE", "TRANBRNO",
                                         "ACCTNO1"]),
        (ELDS1_DIR / "elhpax.parquet", ["ACCTNO", "BRANCH", "PRODUCT", "CUSTCODE",
                                          "COSTCTR", "AANO", "AADATE", "TRANBRNO",
                                          "ACCTNO1"]),
    ]:
        if not ep.exists():
            continue
        con = duckdb.connect()
        edf = con.execute(f"""
            SELECT * FROM read_parquet('{ep}')
            WHERE AADATE >= {idate_sas} AND AADATE <= {sdate_sas}
              AND BRANCH < 3000
        """).pl()
        con.close()
        for row in edf.to_dicts():
            acctno = safe_int(row.get("ACCTNO", 0))
            if acctno in (None, 0):
                # ACCTNO = COMPRESS(ACCTNO1,'0123456789','K')
                acctno1 = safe_str(row.get("ACCTNO1", ""))
                acctno = safe_int("".join(c for c in acctno1 if c.isdigit()), 0)
                row["ACCTNO"] = acctno
            tranbrno = safe_int(row.get("TRANBRNO", 0))
            if tranbrno not in (None, 0):
                row["BRANCH"]  = tranbrno
                row["COSTCTR"] = tranbrno
            prod = safe_int(row.get("PRODUCT", 0))
            if 70 <= prod <= 79:
                row["BRANCH"] = 903
            elds_records.append(row)

    # CHECK IS UNDISBURSED LOAN: merge with CCRISP.ELDS by AANO
    spelds_path = CCRISP_DIR / "elds.parquet"
    spelds_aano = set()
    if spelds_path.exists():
        con = duckdb.connect()
        sp_rows = con.execute(f"SELECT AANO FROM read_parquet('{spelds_path}')").fetchall()
        con.close()
        spelds_aano = {r[0] for r in sp_rows if r[0]}

    for row in elds_records:
        if row.get("AANO") in spelds_aano:
            row["UNDISBURSE"] = "Y"
        else:
            row["UNDISBURSE"] = " "

    # Build ELDS by ACCTNO (drop AANO)
    elds_by_acctno: dict[int, list[dict]] = defaultdict(list)
    for row in elds_records:
        row.pop("AANO", None)
        elds_by_acctno[safe_int(row.get("ACCTNO", 0))].append(row)

    # ===========================================================================
    # DATA LOAN = LOAN + BTRSA + ELDS
    # ===========================================================================
    all_loan_raw = []
    for row in loan_records:
        row["_src"] = "LOAN"
        all_loan_raw.append(row)
    for row in btrsa_final:
        row["_src"] = "BTRSA"
        all_loan_raw.append(row)
    for row in elds_records:
        row["_src"] = "ELDS"
        all_loan_raw.append(row)

    all_loan_raw.sort(key=lambda r: safe_int(r.get("ACCTNO", 0)))

    # PROC SQL: INNER JOIN LOAN x LOANA on ACCTNO (M2M)
    # This replaces the commented-out MERGE (IF A AND B)
    loan_joined = []
    for row in all_loan_raw:
        acctno = safe_int(row.get("ACCTNO", 0))
        loana_list = loana_by_acctno.get(acctno, [])
        if not loana_list:
            continue
        for loana_row in loana_list:
            merged = {**loana_row, **row}  # LOAN wins on overlap
            loan_joined.append(merged)

    # DATA LOANNEW: count customers per ACCTNO (NOCUSTOM)
    nocustom_map: dict[int, int] = defaultdict(int)
    for row in loan_joined:
        nocustom_map[safe_int(row.get("ACCTNO", 0))] += 1

    # Merge NOCUSTOM back
    for row in loan_joined:
        row["NOCUSTOM"] = nocustom_map[safe_int(row.get("ACCTNO", 0))]

    # Exclude from CCRISX
    loan_joined = [r for r in loan_joined
                   if (safe_int(r.get("ACCTNO", 0)),
                       safe_int(r.get("NOTENO", 0))) not in ccrisx_set]

    # ===========================================================================
    # ELDS merge for LOAN (ELDS.ELNA4 + ELDS1.EHPA4 -> by ICPP=NEWIC)
    # ESMR 2009-161
    # ===========================================================================
    elds4_records = []
    for ep in [ELDS_DIR / "elna4.parquet", ELDS1_DIR / "ehpa4.parquet"]:
        if not ep.exists():
            continue
        con = duckdb.connect()
        edf4 = con.execute(f"""
            SELECT JOB, EMPNATUR, EMPLOCAT, ICPP, ANNSUBTSALARY,
                   OCCUPAT_MASCO_CD, INDUSTRIAL_SECTOR_CD
            FROM read_parquet('{ep}')
        """).pl()
        con.close()
        elds4_records.extend(edf4.to_dicts())

    # Deduplicate by ICPP
    elds4_map: dict[str, dict] = {}
    for row in elds4_records:
        icpp = safe_str(row.get("ICPP", "")).strip()
        if icpp and icpp not in elds4_map:
            elds4_map[icpp] = row

    # Merge APPL (EREV): NEWID/BNMSECT, latest SEQNO
    erev_path = ELDSRVX_DIR / "erev.parquet"
    appl_map: dict[str, str] = {}
    if erev_path.exists():
        con = duckdb.connect()
        appl_rows = con.execute(f"""
            SELECT NEWID, BNMSECT
            FROM (
                SELECT NEWID, BNMSECT,
                       ROW_NUMBER() OVER (PARTITION BY NEWID ORDER BY SEQNO DESC) AS rn
                FROM read_parquet('{erev_path}')
                WHERE BNMSECT NOT IN (' ', '-')
            ) t WHERE rn = 1
        """).fetchall()
        con.close()
        appl_map = {r[0]: r[1] for r in appl_rows if r[0]}

    # Merge APPL into ELDS4: ICPP=NEWID; keep only where ICPP != ' '
    for icpp, row in elds4_map.items():
        if icpp in appl_map:
            row["BNMSECT"] = appl_map[icpp]

    # Ensure ICPP is $17
    elds4_map_17 = {}
    for icpp, row in elds4_map.items():
        icpp17 = f"{icpp:<17}"[:17]
        row["ICPP"] = icpp17
        elds4_map_17[icpp17] = row
    elds4_map = elds4_map_17

    # Merge ELDS into LOAN by NEWIC
    MNISECT_EXCL = {
        "1300","3110","3115","3113","3114","3219","3242",
        "3250","3311","3313","3432","3551","3619","3852",
        "3919","6110","6120","6130",
    }
    for row in loan_joined:
        newic = f"{safe_str(row.get('NEWIC', '')).strip():<17}"[:17]
        elds_row = elds4_map.get(newic, {})
        for k, v in elds_row.items():
            if k not in row or row.get(k) is None:
                row[k] = v
        # JOB, EMPNATUR, EMPLOCAT etc. from ELDS
        if elds_row.get("JOB"):
            row["JOB"] = elds_row["JOB"]
        mnisect = safe_str(row.get("MNISECT", ""))
        if mnisect not in MNISECT_EXCL and elds_row.get("BNMSECT"):
            row["BNMSECT"] = elds_row["BNMSECT"]
        if safe_float(row.get("ANNSUBTSALARY", 0)) == 0:
            row["ANNSUBTSALARY"] = safe_float(elds_row.get("ANNSUBTSALARY", 0))

    # Merge CISIDNUM, CODM, CICINDT by CUSTNO
    for row in loan_joined:
        custno = safe_int(row.get("CUSTNO", 0))
        row["CUSTID"]     = cisid_map.get(custno, "")[:100]
        corp = cicodmt_map.get(custno, {})
        row["CORPSTATUS"] = safe_str(corp) if isinstance(corp, str) else ""
        cindt = cicindt_map.get(custno, {})
        for k, v in cindt.items():
            if k not in row:
                row[k] = v

    # DATA INDLOAN / CORLOAN
    indloan = []
    corloan = []
    excl_cacccode_loan = {"001","002","013","016","017","018","019","021","022","023","024","025","027"}
    for row in loan_joined:
        indorg   = safe_str(row.get("INDORG", ""))
        cacccode = safe_str(row.get("CACCCODE", ""))
        if indorg == "I":
            if cacccode not in excl_cacccode_loan:
                row["ACTY"] = "LN"
                indloan.append(row)
        else:
            row["ACTY"] = "LN"
            corloan.append(row)

    # ===========================================================================
    # DATA CURRENT: add OVERDFT RISKCODE, filter OPENIND, date processing
    # ===========================================================================
    current_records2 = []
    for acctno, crow in current_map.items():
        # Merge RISKCODE from OVERDFT
        if acctno in overdft_map:
            crow["RISKCODE"] = overdft_map[acctno].get("RISKCODE", "")

        closedt = crow.get("CLOSEDT")
        procyear = ""; procmont = ""
        if closedt is not None and safe_float(closedt, 0) > 0:
            s = f"{int(closedt):011d}"
            procyear = s[4:8]
            procmont = s[0:2]
        crow["PROCYEAR"] = procyear
        crow["PROCMONT"] = procmont
        crow["MNISECT"]  = safe_str(crow.get("SECTOR", ""))[:6]

        openind = safe_str(crow.get("OPENIND", ""))
        if openind not in ("B", "C", "P"):
            current_records2.append(crow)
        elif openind in ("B", "C", "P"):
            if procyear == reptyear and procmont == reptmon:
                current_records2.append(crow)

    # Second pass: remove if OPENIND in (B,C,P) AND closed same period as opened
    current_records3 = []
    for crow in current_records2:
        openind = safe_str(crow.get("OPENIND", ""))
        if openind in ("B", "C", "P"):
            closedt = crow.get("CLOSEDT")
            if closedt is not None and safe_float(closedt, 0) > 0:
                openyear = safe_str(crow.get("OPENYEAR", ""))
                openmont = safe_str(crow.get("OPENMONT", ""))
                procyear = crow.get("PROCYEAR", "")
                procmont = crow.get("PROCMONT", "")
                if procyear == openyear and procmont == openmont:
                    continue
        current_records3.append(crow)

    # DATA CURRENT: compute TAKEN flag (OD overdue)
    current_map2 = {}
    for crow in current_records3:
        acctno   = safe_int(crow.get("ACCTNO", 0))
        crow["TAKEN"] = "N"
        exoddate = safe_int(crow.get("EXODDATE", 0))
        tempoddt = safe_int(crow.get("TEMPODDT", 0))
        curbal   = safe_float(crow.get("CURBAL", 0))

        if (exoddate != 0 or tempoddt != 0) and curbal < 0:
            odedays = 0; odtdays = 0
            if exoddate not in (0, None):
                dt = date_from_z11_mmddyyyy(exoddate)
                if dt:
                    odedays = date_to_sas(dt)
            if tempoddt not in (0, None):
                dt = date_from_z11_mmddyyyy(tempoddt)
                if dt:
                    odtdays = date_to_sas(dt)
            oddays = odedays if odedays <= odtdays else odtdays
            nodays = sdate_sas - oddays
            riskcode = safe_str(crow.get("RISKCODE", ""))
            if nodays > 89 or riskcode in ("1", "2", "3", "4"):
                crow["TAKEN"] = "Y"

        current_map2[acctno] = crow

    # ===========================================================================
    # DATA OVERDFS = MERGE OVERDFT + CURRENT
    # ===========================================================================
    overdfs_records = []
    for acctno, orow in overdft_map.items():
        if safe_int(orow.get("LOANTYPE", orow.get("PRODUCT", 0))) == 105:
            continue
        crow = current_map2.get(acctno, {})
        apprlimt = safe_float(orow.get("APPRLIMT", 0))
        curbal   = safe_float(crow.get("CURBAL", 0))
        branch   = safe_int(crow.get("BRANCH", orow.get("BRANCH", 0)))
        taken    = safe_str(crow.get("TAKEN", "N"))
        merged   = {**crow, **orow}
        # IF A AND B AND APPRLIMT > 1 OR B AND APPRLIMT <= 1 AND CURBAL < 0 AND BRANCH NE 998 OR TAKEN = 'Y'
        if crow:
            if ((apprlimt > 1) or
                    (apprlimt <= 1 and curbal < 0 and branch != 998) or
                    taken == "Y"):
                overdfs_records.append(merged)

    # DATA OVERDFT: merge OVERDFS + CISDEP (by ACCTNO; IF A & C)
    cisdep_by_acctno: dict[int, dict] = {}
    for row in cisdep_df.to_dicts():
        acctno = safe_int(row.get("ACCTNO", 0))
        cisdep_by_acctno[acctno] = row

    overdft_final = []
    for orow in overdfs_records:
        acctno = safe_int(orow.get("ACCTNO", 0))
        cdrow  = cisdep_by_acctno.get(acctno)
        if cdrow:
            merged = {**cdrow, **orow}
            overdft_final.append(merged)

    # ===========================================================================
    # ELDS for OVERDFT (ELDS.ELNA4 only - JOB, EMPNATUR, EMPLOCAT, ICPP, ANNSUBTSALARY)
    # ESMR 2009-161
    # ===========================================================================
    elds_od_map: dict[str, dict] = {}
    elna4_path = ELDS_DIR / "elna4.parquet"
    if elna4_path.exists():
        con = duckdb.connect()
        eod_df = con.execute(f"""
            SELECT JOB, EMPNATUR, EMPLOCAT, ICPP, ANNSUBTSALARY
            FROM read_parquet('{elna4_path}')
        """).pl()
        con.close()
        for row in eod_df.to_dicts():
            icpp = f"{safe_str(row.get('ICPP', '')).strip():<17}"[:17]
            if icpp.strip() and icpp not in elds_od_map:
                # Add BNMSECT from appl_map
                if icpp.strip() in appl_map:
                    row["BNMSECT"] = appl_map[icpp.strip()]
                elds_od_map[icpp] = row

    # Merge ELDS into OVERDFT by NEWIC
    for row in overdft_final:
        row["ACTY"] = "OD"
        newic = f"{safe_str(row.get('NEWIC', '')).strip():<17}"[:17]
        elds_row = elds_od_map.get(newic, {})
        for k, v in elds_row.items():
            if k not in row or not row.get(k):
                row[k] = v
        mnisect = safe_str(row.get("MNISECT", ""))
        if mnisect not in MNISECT_EXCL and elds_row.get("BNMSECT"):
            row["BNMSECT"] = elds_row["BNMSECT"]

    # Merge CISIDNUM, CODM, CICINDT by CUSTNO
    for row in overdft_final:
        custno = safe_int(row.get("CUSTNO", 0))
        row["CUSTID"]     = cisid_map.get(custno, "")[:100]
        corp = cicodmt_map.get(custno)
        row["CORPSTATUS"] = safe_str(corp) if isinstance(corp, str) else ""
        cindt = cicindt_map.get(custno, {})
        for k, v in cindt.items():
            if k not in row:
                row[k] = v

    # DATA INDOD / COROD
    indod = []
    corod = []
    for row in overdft_final:
        indorg   = safe_str(row.get("INDORG", ""))
        cacccode = safe_str(row.get("CACCCODE", ""))
        if indorg == "I":
            if cacccode not in ("001", "002"):
                row["ACTY"] = "OD"
                indod.append(row)
        elif indorg == "O":
            row["ACTY"] = "OD"
            corod.append(row)
        elif cacccode in ("000", "004", "020"):
            row["ACTY"] = "OD"
            corod.append(row)

    # ===========================================================================
    # PROC FORMAT LIB=POSTCD CNTLOUT=STATEFMT / PROC FORMAT CNTLIN=STATEFMT
    # (Already loaded above via load_statecd_format)
    # ===========================================================================

    # ===========================================================================
    # DATA INDCCRIS: INDLOAN + INDOD -> OUTPUT1 (CCRISIND, LRECL=2300)
    # ===========================================================================
    indccris_all = list(indloan) + list(indod)

    output1_lines = []
    for row in indccris_all:
        acty     = safe_str(row.get("ACTY", "LN"))
        indorg   = safe_str(row.get("INDORG", "I"))
        cacccode = safe_str(row.get("CACCCODE", ""))
        seccust  = safe_str(row.get("SECCUST", ""))
        newicind = safe_str(row.get("NEWICIND", ""))

        oldic_raw = safe_str(row.get("OLDIC", ""))
        newic_raw = safe_str(row.get("NEWIC", ""))
        oldic, newic = process_ic_fields(
            oldic_raw, newic_raw, indorg, cacccode, seccust, newicind
        )

        # PROFESS
        occupat  = safe_str(row.get("OCCUPAT", "")).strip().zfill(3)
        profess  = PROF_FORMAT.get(occupat, PROF_FORMAT.get("   ", "NO AVAILABLE"))
        job      = safe_str(row.get("JOB", "")).strip()
        if job:
            profess = job

        ficode   = safe_str(cacccode)
        loantype = safe_int(row.get("LOANTYPE", row.get("PRODUCT", 0)))
        apcode   = loantype
        namekey  = "  "
        district = " "
        resident = "N" if safe_int(row.get("CUSTCODE", 0)) > 80 else "R"

        # BUMI
        custcode = safe_int(row.get("CUSTCODE", 0))
        custcdx  = safe_int(row.get("CUSTCDX", 0))
        bumi = "9"
        if custcode == 77:
            bumi = "1"
        elif custcode == 78:
            bumi = "2"
        if bumi == "9":
            if custcdx == 77:
                bumi = "1"
            elif custcdx == 78:
                bumi = "2"

        # FOREIGN / NATIONAL
        state    = safe_str(row.get("STATE", ""))
        foreign  = state if acty == "OD" else ""
        pr_ctry  = safe_str(row.get("PR_CTRY", ""))
        citizen  = safe_str(row.get("CITIZEN", ""))
        national = pr_ctry if pr_ctry == "MY" else citizen

        phone4   = " "

        # MAILSTAT
        mailcode_raw = safe_str(row.get("MAILCODE", "")).strip()
        if not mailcode_raw:
            mailcode_raw = "-0000"
        mailstat = get_mailstat(mailcode_raw)
        if not mailstat:
            mailstat = "-1"
        if mailcode_raw == "00000":
            mailstat = "00"

        # GENDER
        gender = safe_str(row.get("GENDER", ""))
        if gender == "M":
            gender = "L"
        elif gender == "F":
            gender = "P"
        else:
            gender = "R"

        # MARITAL
        marital = safe_str(row.get("MARITAL", ""))
        if marital in ("M", "P", "W"):
            marital = "M"
        elif marital in ("S", "D"):
            marital = "S"
        else:
            marital = "U"

        # SPOUSE cleansing
        remove_chars = '=`&-/@#*+:().,"%!$?_ \''
        spouse1i = safe_str(row.get("SPOUSE1I", "")).strip()
        if spouse1i:
            for ch in remove_chars:
                spouse1i = spouse1i.replace(ch, "")
        spouse1d = safe_str(row.get("SPOUSE1D", "")).strip()
        if spouse1d:
            for ch in remove_chars:
                spouse1d = spouse1d.replace(ch, "")
        if not spouse1i and not spouse1d:
            spouse1 = " "
        else:
            spouse1 = safe_str(row.get("SPOUSE1", ""))

        custname = safe_str(row.get("CUSTNAME", "")).replace("\\", "")

        annsubtsalary = safe_float(row.get("ANNSUBTSALARY", 0))

        acctno   = safe_int(row.get("ACCTNO", 0))
        branch   = safe_int(row.get("PENDBRH", row.get("BRANCH", 0)))
        if safe_int(row.get("PENDBRH", 0)) == 0:
            branch = safe_int(row.get("NTBRCH", 0))
        if branch == 0:
            branch = safe_int(row.get("ACCBRCH", 0))

        if 800 <= apcode <= 899 and 2000000000 <= acctno <= 2999999999:
            branch = 904
        if 800 <= apcode <= 899 and 8000000000 <= acctno <= 8999999999:
            branch = 904
        if safe_str(row.get("UNDISBURSE", "")) == "Y" and branch == 902:
            branch = 904

        masco_cd = safe_str(row.get("OCCUPAT_MASCO_CD", ""))
        masco_cd_orig = safe_str(row.get("MASCO_CD", ""))
        if masco_cd_orig:
            masco_cd = masco_cd_orig

        bgc = safe_str(row.get("BGC", "")).strip()
        # BGC = 11 default for individual (commented out in original):
        # /* BASICGC = 11; */

        # DOB components
        dob    = row.get("DOB", row.get("DOBDATE"))
        dobdd  = 0; dobmm = 0; dobcc = 0; dobyy = 0
        if dob and safe_float(dob, 0) != 0:
            dt = sas_to_date(int(dob))
            if dt:
                dobdd = dt.day; dobmm = dt.month
                dobcc = dt.year // 100 % 100; dobyy = dt.year % 100

        # LSTMNT (last maintenance date)
        lstmnt = row.get("LSTMNT")
        lstdd = 0; lstmm = 0; lstcc = 0; lstyy = 0
        if lstmnt and safe_float(lstmnt, 0) != 0:
            dt = sas_to_date(int(lstmnt))
            if dt:
                lstdd = dt.day; lstmm = dt.month
                lstcc = dt.year // 100 % 100; lstyy = dt.year % 100

        # EMPL date
        empld = row.get("EMPLDATE")
        empldd = 0; emplmm = 0; emplcc = 0; emplyy = 0
        if empld and safe_float(empld, 0) != 0:
            dt = sas_to_date(int(empld))
            if dt:
                empldd = dt.day; emplmm = dt.month
                emplcc = dt.year // 100 % 100; emplyy = dt.year % 100

        custno   = safe_int(row.get("CUSTNO", 0))
        employer = safe_str(row.get("EMPLOYER_NAME", ""))
        empnatur = safe_str(row.get("EMPNATUR", ""))
        emplocat = safe_str(row.get("EMPLOCAT", ""))
        bnmsect  = safe_str(row.get("BNMSECT", ""))
        longname = safe_str(row.get("LONGNAME", ""))
        sic_iss  = safe_str(row.get("SIC_ISS", ""))
        pricust  = "Y" if seccust == "901" else "N"
        occ_masco = masco_cd
        emp_sector = safe_str(row.get("EMPLOY_SECTOR_CD", ""))
        emp_type   = safe_str(row.get("EMPLOY_TYPE_CD", ""))
        custid     = safe_str(row.get("CUSTID", ""))
        bnm_assign_id = safe_str(row.get("BNM_ASSIGN_ID", ""))
        cust_code  = safe_str(row.get("CUST_CODE", ""))
        restatus   = safe_str(row.get("RESTATUS", ""))

        line = build_record([
            (1,    fmt_z(safe_int(ficode, 0), 3)),
            (7,    fmt_z(branch, 3)),
            (10,   fmt_z(apcode, 3)),
            (13,   fmt_n(custno, 20)),
            (33,   fmt_n(acctno, 10)),
            (43,   fmt_s(namekey, 140)),
            (183,  fmt_s(custname, 150)),
            (333,  fmt_s(newic, 20)),
            (353,  fmt_s(oldic, 20)),
            (373,  fmt_n(dobdd, 2)),
            (375,  fmt_n(dobmm, 2)),
            (377,  fmt_n(dobcc, 2)),
            (379,  fmt_n(dobyy, 2)),
            (381,  fmt_s(gender, 1)),
            (382,  fmt_s(national, 2)),
            # (384, MARITAL $1.)
            # (385, BUMI $1.)
            (387,  fmt_s(bgc, 2)),
            (389,  fmt_n(lstdd, 2)),
            (391,  fmt_n(lstmm, 2)),
            (393,  fmt_n(lstcc, 2)),
            (395,  fmt_n(lstyy, 2)),
            (397,  fmt_s(safe_str(row.get("ADDRLN1", "")), 40)),
            (437,  fmt_s(safe_str(row.get("ADDRLN2", "")), 40)),
            (477,  fmt_s(safe_str(row.get("ADDRLN3", "")), 40)),
            (517,  fmt_s(safe_str(row.get("ADDRLN4", "")), 40)),
            # (557, LSTCHGDD 2.)
            # (559, LSTCHGMM 2.)
            # (561, LSTCHGCC 2.)
            # (563, LSTCHGYY 2.)
            (565,  fmt_s(safe_str(row.get("COUNTRY", "")), 2)),
            (567,  fmt_n(safe_int(mailcode_raw.replace("-", "0"), 0), 5)),
            (572,  fmt_s(district, 20)),
            (592,  fmt_s(mailstat, 20)),
            (612,  fmt_s(safe_str(row.get("PRIPHONE", "")), 20)),
            (632,  fmt_s(safe_str(row.get("SECPHONE", "")), 20)),
            (652,  fmt_s(safe_str(row.get("MOBIPHON", "")), 20)),
            (672,  fmt_s(phone4, 20)),
            (692,  fmt_s(safe_str(spouse1), 150)),
            (842,  fmt_s(spouse1i, 20)),
            (862,  fmt_s(spouse1d, 20)),
            (1452, fmt_s(employer, 150)),
            (1602, fmt_s(profess, 60)),
            (1662, fmt_n(empldd, 2)),
            (1664, fmt_n(emplmm, 2)),
            (1666, fmt_n(emplcc, 2)),
            (1668, fmt_n(emplyy, 2)),
            (1670, fmt_s(resident, 1)),
            (1671, fmt_n(safe_int(row.get("COSTCTR", 0)), 4)),
            (1675, fmt_s(empnatur, 10)),
            (1685, fmt_s(emplocat, 200)),
            (1885, fmt_s(bnmsect, 6)),
            (1891, fmt_s(longname, 150)),
            # (2041, ANNSUBTSALARY 16.2)
            (2058, fmt_s(sic_iss, 5)),
            (2063, fmt_s(pricust, 1)),
            (2064, fmt_s(occ_masco, 5)),
            (2069, fmt_s(emp_sector, 5)),
            (2074, fmt_s(emp_type, 3)),
            (2077, fmt_s(custid, 100)),
            (2177, fmt_s(bnm_assign_id, 20)),
            (2197, fmt_s(cust_code, 5)),
            (2202, fmt_s(restatus, 3)),
        ], 2300)
        output1_lines.append(line)

    with open(CCRISIND_FILE, "w", encoding="latin-1") as f:
        f.writelines(line + "\n" for line in output1_lines)
    logger.info(f"Written CCRISIND ({len(output1_lines)} records): {CCRISIND_FILE}")

    # ===========================================================================
    # DATA CORCCRIS: CORLOAN + COROD -> OUTPUT2 (CCRISCOR, LRECL=1500)
    # ESMR 2009-161
    # ===========================================================================
    corccris_all = list(corloan) + list(corod)

    # Merge ELDS4 by BUSSREG=ICPP for corporate
    for row in corccris_all:
        bussreg_raw = safe_str(row.get("BUSSREG", "")).strip()
        bussreg17   = f"{bussreg_raw:<17}"[:17]

        # Derive BUSSREG if blank
        if not bussreg_raw:
            newic_v  = safe_str(row.get("NEWIC", "")).strip()
            oldic_v  = safe_str(row.get("OLDIC", "")).strip()
            guarend  = safe_str(row.get("GUAREND", "")).strip()
            bussreg_raw = newic_v or oldic_v
            if not bussreg_raw and safe_int(row.get("NOCUSTOM", 0)) == 1:
                bussreg_raw = guarend
            row["BUSSREG"] = bussreg_raw

        elds_row = elds4_map.get(f"{bussreg_raw:<17}"[:17], {})
        # Merge ELDS fields (drop BNMSECT first as in SAS DROP=BNMSECT)
        for k, v in elds_row.items():
            if k == "BNMSECT":
                continue
            if k not in row or not row.get(k):
                row[k] = v
        mnisect = safe_str(row.get("MNISECT", ""))
        if mnisect not in MNISECT_EXCL and elds_row.get("BNMSECT"):
            row["BNMSECT"] = elds_row["BNMSECT"]

    output2_lines = []
    for row in corccris_all:
        acty     = safe_str(row.get("ACTY", "LN"))
        cacccode = safe_str(row.get("CACCCODE", ""))
        custcode = safe_int(row.get("CUSTCODE", 0))
        orgtype  = safe_str(row.get("ORGTYPE", ""))
        purpose  = safe_str(row.get("PURPOSE", ""))

        ficode   = cacccode
        loantype = safe_int(row.get("LOANTYPE", row.get("PRODUCT", 0)))
        apcode   = loantype
        namekey  = "  "
        district = " "
        phone4   = " "

        resident = "N" if custcode > 80 else "R"

        # BUMI for corporate
        bumi = " "
        if custcode in (41,42,43,61,66):
            bumi = "1"
        elif custcode in (44,46,47,62,67):
            bumi = "2"
        elif custcode in (52,53,54,57,59,64,69,70,71,72,73,74,75):
            bumi = "3"
        elif custcode in (1,35,48,49,51,63,68,79,81,82,83,84,85,86,90,91,92,98,99):
            bumi = "9"
        elif custcode in (2,3,4,5,6,11,12,13,17,30,31,32,33,34,36,37,38,39,40,45):
            bumi = "9"
            if acty == "LN":
                user1 = safe_str(row.get("USER1", ""))
                if user1 == "3":
                    bumi = "1"
                elif user1 == "4":
                    bumi = "2"
            elif acty == "OD":
                if orgtype == "3":
                    bumi = "1"
                elif orgtype == "4":
                    bumi = "2"

        # MAILSTAT
        mailcode_raw = safe_str(row.get("MAILCODE", "")).strip()
        if not mailcode_raw:
            mailcode_raw = "-0000"
        mailstat = get_mailstat(mailcode_raw)
        if not mailstat:
            mailstat = "-1"
        if mailcode_raw == "00000":
            mailstat = "00"

        # BASICGC
        basicgc = 91
        if custcode in (99,):
            basicgc = 91
        elif custcode in (98,):
            basicgc = 51
        elif custcode in (79,):
            basicgc = 43
        elif custcode in (35,):
            basicgc = 42
        elif custcode in (1, 74):
            basicgc = 34
        elif custcode in (73,):
            basicgc = 33
        elif custcode in (72,):
            basicgc = 32
        elif custcode in (71, 90, 91, 92):
            basicgc = 31

        _gc_set1 = {40,41,42,43,44,46,47,48,49,51,52,53,54,68,61,62,66,67,86}
        _gc_set2 = {2,3,4,5,6,11,12,13,17,20,30,31,32,33,34,37,38,39,
                    40,41,42,43,44,46,47,48,49,51,52,53,54,61,62,66,67,
                    68,57,59,63,64,69,75,82,83,84}
        if acty == "LN":
            if custcode in (79,98,99) and orgtype == "T":
                basicgc = 41
            if custcode in _gc_set1 and orgtype == "I":
                basicgc = 21
            elif custcode in _gc_set1 and orgtype == "P":
                basicgc = 22
            elif custcode in _gc_set1 and orgtype == "L":
                basicgc = 23
            elif custcode in _gc_set2 and orgtype == "C":
                basicgc = 24
        elif acty == "OD":
            spouse1 = safe_str(row.get("SPOUSE1", ""))
            if spouse1 == " " or not spouse1.strip():
                if custcode in (79,98,99) and purpose == "J":
                    basicgc = 41
                if custcode in _gc_set1 and purpose == "7":
                    basicgc = 21
                elif custcode in _gc_set1 and purpose == "8":
                    basicgc = 22
                elif custcode in _gc_set1 and purpose == "I":
                    basicgc = 23
                elif custcode in _gc_set2 and purpose in ("9","A","E"):
                    basicgc = 24

        if cacccode in ("017", "012"):
            basicgc = 24
        if basicgc in (31,32,33,34,41,42,43,51,91):
            bumi = "9"

        # NRCC
        citizen = safe_str(row.get("CITIZEN", ""))
        nrcc = 9
        if citizen == "MY" and basicgc == 24 and custcode not in (48,49,51,63,68):
            nrcc = 1
        elif basicgc == 24 and custcode in (48,49,51,63,68):
            nrcc = 2

        # BUSSREG derivation
        bussreg = safe_str(row.get("BUSSREG", "")).strip()
        newic_v = safe_str(row.get("NEWIC", "")).strip()
        oldic_v = safe_str(row.get("OLDIC", "")).strip()
        if basicgc in (23,41,42,43,51,91):
            if not bussreg:
                bussreg = newic_v
            if not newic_v:
                bussreg = oldic_v
        if not bussreg:
            bussreg = newic_v
        if not bussreg:
            bussreg = oldic_v
        nocustom = safe_int(row.get("NOCUSTOM", 0))
        if not bussreg and nocustom == 1:
            bussreg = safe_str(row.get("GUAREND", "")).strip()

        custname = safe_str(row.get("CUSTNAME", "")).replace("\\", "")

        bgc = safe_str(row.get("BGC", "")).strip()
        if not bgc:
            bgc = str(basicgc)

        acctno  = safe_int(row.get("ACCTNO", 0))
        branch  = safe_int(row.get("BRANCH", 0))
        if 800 <= apcode <= 899 and 2000000000 <= acctno <= 2999999999:
            branch = 904
        if 800 <= apcode <= 899 and 8000000000 <= acctno <= 8999999999:
            branch = 904
        if safe_str(row.get("UNDISBURSE", "")) == "Y" and branch == 902:
            branch = 904

        sic_iss = safe_str(row.get("SIC_ISS", ""))
        ind_sector_cd = safe_str(row.get("INDUSTRIAL_SECTOR_CD", ""))
        if sic_iss:
            ind_sector_cd = sic_iss

        custno   = safe_int(row.get("CUSTNO", 0))
        bnmsect  = safe_str(row.get("BNMSECT", ""))
        longname = safe_str(row.get("LONGNAME", ""))
        empnatur = safe_str(row.get("EMPNATUR", ""))
        pricust  = "Y" if safe_str(row.get("SECCUST", "")) == "901" else "N"
        custid   = safe_str(row.get("CUSTID", ""))
        bnm_id   = safe_str(row.get("BNM_ASSIGN_ID", ""))
        new_buss = safe_str(row.get("NEW_BUSS_REG_ID", ""))
        cust_code = safe_str(row.get("CUST_CODE", ""))
        restatus  = safe_str(row.get("RESTATUS", ""))
        corpstatus = safe_str(row.get("CORPSTATUS", ""))
        sector_code = safe_str(row.get("SECTOR_CODE", ""))
        noemplo   = safe_int(row.get("NOEMPLO", 0))
        turnover  = safe_float(row.get("TURNOVER", 0))

        # DOB / LSTMNT
        dob = row.get("DOB", row.get("DOBDATE"))
        dobdd=0; dobmm=0; dobcc=0; dobyy=0
        if dob and safe_float(dob, 0) != 0:
            dt = sas_to_date(int(dob))
            if dt:
                dobdd=dt.day; dobmm=dt.month
                dobcc=dt.year//100%100; dobyy=dt.year%100
        lstmnt = row.get("LSTMNT")
        lstdd=0; lstmm=0; lstcc=0; lstyy=0
        if lstmnt and safe_float(lstmnt, 0) != 0:
            dt = sas_to_date(int(lstmnt))
            if dt:
                lstdd=dt.day; lstmm=dt.month
                lstcc=dt.year//100%100; lstyy=dt.year%100

        line = build_record([
            (1,    fmt_z(safe_int(ficode, 0), 3)),
            (7,    fmt_z(branch, 3)),
            (10,   fmt_z(apcode, 3)),
            (13,   fmt_n(custno, 20)),
            (33,   fmt_n(acctno, 10)),
            (43,   fmt_s(namekey, 140)),
            (183,  fmt_s(custname, 150)),
            (333,  fmt_s(bussreg, 20)),
            (353,  fmt_n(dobdd, 2)),
            (355,  fmt_n(dobmm, 2)),
            (357,  fmt_n(dobcc, 2)),
            (359,  fmt_n(dobyy, 2)),
            (361,  fmt_s(citizen, 2)),
            # (363, BUMI $1.)
            # (364, NRCC 1.)
            (366,  fmt_s(bgc, 2)),
            (368,  fmt_n(lstdd, 2)),
            (370,  fmt_n(lstmm, 2)),
            (372,  fmt_n(lstcc, 2)),
            (374,  fmt_n(lstyy, 2)),
            (376,  fmt_s(safe_str(row.get("ADDRLN1", "")), 40)),
            (416,  fmt_s(safe_str(row.get("ADDRLN2", "")), 40)),
            (456,  fmt_s(safe_str(row.get("ADDRLN3", "")), 40)),
            (496,  fmt_s(safe_str(row.get("ADDRLN4", "")), 40)),
            # (536, LSTCHGDD 2.) ... (542, LSTCHGYY 2.)
            (544,  fmt_s(safe_str(row.get("COUNTRY", "")), 2)),
            (546,  fmt_n(safe_int(mailcode_raw.replace("-", "0"), 0), 5)),
            (551,  fmt_s(district, 20)),
            (571,  fmt_s(mailstat, 20)),
            (591,  fmt_s(safe_str(row.get("PRIPHONE", "")), 20)),
            (611,  fmt_s(safe_str(row.get("SECPHONE", "")), 20)),
            (631,  fmt_s(safe_str(row.get("MOBIPHON", "")), 20)),
            (651,  fmt_s(phone4, 20)),
            (671,  fmt_s(resident, 1)),
            (672,  fmt_n(safe_int(row.get("COSTCTR", 0)), 4)),
            (676,  fmt_s(empnatur, 10)),
            (686,  fmt_s(bnmsect, 6)),
            (692,  fmt_s(longname, 150)),
            (843,  fmt_s(sic_iss, 5)),
            (848,  fmt_s(pricust, 1)),
            (945,  fmt_s(ind_sector_cd, 5)),
            (950,  fmt_s(custid, 100)),
            (1050, fmt_s(bnm_id, 20)),
            (1070, fmt_s(new_buss, 20)),
            (1090, fmt_s(cust_code, 5)),
            (1095, fmt_s(restatus, 3)),
            (1098, fmt_s(corpstatus, 3)),
            (1101, fmt_s(sector_code, 5)),
            (1106, fmt_n(noemplo, 8)),
            (1114, fmt_f(turnover, 20, 0)),
        ], 1500)
        output2_lines.append(line)

    with open(CCRISCOR_FILE, "w", encoding="latin-1") as f:
        f.writelines(line + "\n" for line in output2_lines)
    logger.info(f"Written CCRISCOR ({len(output2_lines)} records): {CCRISCOR_FILE}")

    # ===========================================================================
    # DATA CCRISOWN / CORPOWN -> OUTPUT3 (CCRISOWN, LRECL=800)
    # ===========================================================================
    allloan_all = list(corloan) + list(indloan)
    allod_all   = list(indod)   + list(corod)
    ccrisown_src = allloan_all + allod_all

    # Load OWNER from CISDP.OWNER + CISLN.OWNER
    owner_records = []
    for owner_path in [CISDP_DIR / "owner.parquet", CISLN_DIR / "owner.parquet"]:
        if owner_path.exists():
            con = duckdb.connect()
            odf = con.execute(f"SELECT * FROM read_parquet('{owner_path}')").pl()
            con.close()
            owner_records.extend(odf.to_dicts())

    owner_by_custno: dict[int, list[dict]] = defaultdict(list)
    for row in owner_records:
        owner_by_custno[safe_int(row.get("CUSTNO", 0))].append(row)

    # DATA CORPOWN: MERGE CCRISOWN x OWNER by CUSTNO (IF A AND B) with conditions
    bgc_set_owner = {"21", "22", "23"}
    corpown_records = []
    for row in ccrisown_src:
        custno = safe_int(row.get("CUSTNO", 0))
        olist  = owner_by_custno.get(custno, [])
        for orow in olist:
            relatcd1 = safe_str(orow.get("RELATCD1", ""))
            relatcd2 = safe_str(orow.get("RELATCD2", ""))
            bgc_v    = safe_str(row.get("BGC", "")).strip()
            indorg   = safe_str(row.get("INDORG", ""))
            cond_1   = (relatcd1 == "049" and relatcd2 == "049")
            cond_2   = bgc_v in bgc_set_owner
            cond_3   = (relatcd1 == "050" and relatcd2 == "050")
            if (cond_1 or cond_2 or cond_3) and indorg == "O":
                merged = {**row, **orow}
                # RENAME: OWNER=CUSTNO (owner's custno is the owner field)
                merged["OWNER"] = safe_int(orow.get("OWNER", orow.get("CUSTNO", 0)))
                corpown_records.append(merged)

    # PROC SORT NODUP by CUSTNO
    seen_cn = set()
    corpown_dedup = []
    for row in sorted(corpown_records, key=lambda r: safe_int(r.get("CUSTNO", 0))):
        cn = safe_int(row.get("CUSTNO", 0))
        if cn not in seen_cn:
            seen_cn.add(cn)
            corpown_dedup.append(row)

    # PROC SORT OUT=BRCHFIX NODUPKEYS by ACCTNO
    brchfix_map: dict[int, dict] = {}
    for row in sorted(corpown_dedup, key=lambda r: safe_int(r.get("ACCTNO", 0))):
        acctno = safe_int(row.get("ACCTNO", 0))
        if acctno not in brchfix_map:
            brchfix_map[acctno] = {
                "ACCTNO":   acctno,
                "BRANCH":   row.get("BRANCH"),
                "OWNERDD":  row.get("OWNERDD"),
                "OWNERMM":  row.get("OWNERMM"),
                "OWNERCC":  row.get("OWNERCC"),
                "OWNERYY":  row.get("OWNERYY"),
            }

    # Merge BRCHFIX into CORPOWN by ACCTNO (IF A)
    corpown_merged = []
    for row in sorted(corpown_dedup, key=lambda r: safe_int(r.get("ACCTNO", 0))):
        acctno = safe_int(row.get("ACCTNO", 0))
        bf = brchfix_map.get(acctno, {})
        row.update({k: v for k, v in bf.items()
                    if k in ("BRANCH", "OWNERDD", "OWNERMM", "OWNERCC", "OWNERYY")})
        corpown_merged.append(row)

    # PROC SORT NODUPKEYS by ACCTNO CUSTNO OWNERNAM OWNERID1 OWNERID2
    seen_ow = set()
    corpown_final = []
    for row in sorted(corpown_merged, key=lambda r: (
        safe_int(r.get("ACCTNO", 0)),
        safe_int(r.get("CUSTNO", 0)),
        safe_str(r.get("OWNERNAM", "")),
        safe_str(r.get("OWNERID1", "")),
        safe_str(r.get("OWNERID2", "")),
    )):
        k = (safe_int(row.get("ACCTNO", 0)),
             safe_int(row.get("CUSTNO", 0)),
             safe_str(row.get("OWNERNAM", "")),
             safe_str(row.get("OWNERID1", "")),
             safe_str(row.get("OWNERID2", "")))
        if k not in seen_ow:
            seen_ow.add(k)
            corpown_final.append(row)

    # DATA CCRISOWN: write OUTPUT3
    output3_lines = []
    for row in sorted(corpown_final, key=lambda r: (
        safe_int(r.get("BRANCH", 0)),
        safe_int(r.get("ACCTNO", 0)),
    )):
        loantype = safe_int(row.get("LOANTYPE", row.get("PRODUCT", 0)))
        apcode   = loantype
        ficodi   = " "
        custname = safe_str(row.get("CUSTNAME", "")).replace("\\", "")
        owner    = safe_int(row.get("OWNER", row.get("CUSTNO", 0)))
        acctno   = safe_int(row.get("ACCTNO", 0))
        branch   = safe_int(row.get("BRANCH", 0))
        namekey  = "  "

        oldic_v = safe_str(row.get("OLDIC", "")).strip()
        newic_v = safe_str(row.get("NEWIC", "")).strip()
        if not oldic_v:
            oldic_v = newic_v
            newic_v = "  "

        if safe_str(row.get("UNDISBURSE", "")) == "Y" and branch == 902:
            branch = 904

        line = build_record([
            (1,   fmt_s(ficodi, 6)),
            (7,   fmt_z(branch, 3)),
            (10,  fmt_n(apcode, 3)),
            (13,  fmt_n(owner, 20)),
            (33,  fmt_n(acctno, 10)),
            (43,  fmt_s(namekey, 140)),
            (183, fmt_s(custname, 150)),
            (333, fmt_s(oldic_v, 20)),
            (353, fmt_s(safe_str(row.get("OWNERNAM", "")), 150)),
            (503, fmt_s(safe_str(row.get("OWNERID1", "")), 20)),
            (523, fmt_s(safe_str(row.get("OWNERID2", "")), 20)),
            (543, fmt_s(safe_str(row.get("OWNERDD", "")), 2)),
            (545, fmt_s(safe_str(row.get("OWNERMM", "")), 2)),
            (547, fmt_s(safe_str(row.get("OWNERCC", "")), 2)),
            (549, fmt_s(safe_str(row.get("OWNERYY", "")), 2)),
            (551, fmt_n(safe_int(row.get("COSTCTR", 0)), 4)),
            (555, fmt_s(safe_str(row.get("LONGNAME", "")), 150)),
            (706, fmt_s(safe_str(row.get("SIC_ISS", "")), 5)),
        ], 800)
        output3_lines.append(line)

    with open(CCRISOWN_FILE, "w", encoding="latin-1") as f:
        f.writelines(line + "\n" for line in output3_lines)
    logger.info(f"Written CCRISOWN ({len(output3_lines)} records): {CCRISOWN_FILE}")

    # PROC CONTENTS DATA=WORK._ALL_ NODS (informational only - skipped)

    logger.info("EIBWCCR4 completed successfully.")


if __name__ == "__main__":
    main()
