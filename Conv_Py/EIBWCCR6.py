# !/usr/bin/env python3
"""
Program : EIBWCCR6
Function: CCRIS Enhancement - Generate CCRIS submission files from PBB/ELDS data.
          Produces:
            - ACCTCRED  (LRECL=900)  : Account Credit master record
            - SUBACRED  (LRECL=950)  : Sub-account Credit detail
            - CREDITPO  (LRECL=400)  : Credit position
            - PROVISIO  (LRECL=400)  : Provision data (NPL accounts)
            - LEGALACT  (LRECL=100)  : Legal action
            - CREDITOD  (LRECL=210)  : Credit OD (commented out in original)
            - ACCTREJT  (LRECL=200)  : Account rejects / HP rejects

          Key differences from EIIWCCR6:
            - Source library is PBB (not PIBB)
            - RVNOTE branch mapping uses COSTCTR 8044->902, 8048->903
            - FX UNDRAWN applies to ACCTNO 2500000000-2599999999
            - FEFCL FACICODE mapping differs
            - ACCTNO 2500000000-2599999999 INTRATE clamped to [6.72, 10.22]
            - TYPEPRC logic for 2500000000-2599999999 range differs
            - LOANTYPE 600-699 special case: 678,679 -> FCONCEPT=99 (not 16)
"""

import math
import logging
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl

# ---------------------------------------------------------------------------
# Dependency: PBBLNFMT  (PBB loan format definitions)
# %INC PGM(PBBLNFMT);
# ---------------------------------------------------------------------------
from PBBLNFMT import (
    format_oddenom, format_lndenom, format_lnprod,
    format_odcustcd, format_locustcd, format_lncustcd,
    format_statecd, format_apprlimt, format_loansize,
    format_mthpass, format_lnormt, format_lnrmmt,
    format_collcd, format_riskcd, format_busind,
)

# ---------------------------------------------------------------------------
# Dependency: PFBCRFMT  (BNM taxonomy format definitions)
# %INC PGM(PFBCRFMT);
# ---------------------------------------------------------------------------
from PFBCRFMT import (
    format_faccode, format_odfaccode, format_facname,
    format_fundsch, format_fschemec, format_synd,
    format_odfundsch, format_odfacility, format_fconcept,
    format_odfconcept, format_fincept, format_cpurphp,
    format_lnprodc, format_purphp,
)

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
BNM_DIR     = BASE_DIR / "pbb/mniln"           # SAP.PBB.MNILN(0)
BNMPREV_DIR = BASE_DIR / "pbb/mniln_prev"      # SAP.PBB.MNILN(-4)
BNMD_DIR    = BASE_DIR / "pbb/mniln_daily"     # SAP.PBB.MNILN.DAILY(0)
CCRIS_DIR   = BASE_DIR / "pbb/ccris/sasdata"   # SAP.PBB.CCRIS.SASDATA
CCRISP_DIR  = BASE_DIR / "pbb/ccris/split"     # SAP.PBB.CCRIS.SPLIT
FORATE_DIR  = BASE_DIR / "pbb/fcyca"           # SAP.PBB.FCYCA
HPREJ_DIR   = BASE_DIR / "elds/hprej"          # SAP.ELDS.HPREJ.BACKDATE(0)
BNMSUM_DIR  = BASE_DIR / "pbb/elds_iss3"       # SAP.PBB.ELDS.ISS3(0)

# Input text files
WRITOFF_FILE = BASE_DIR / "bnm/program/wriofac.txt"   # SAP.BNM.PROGRAM(WRIOFAC)

# Output text files
OUT_DIR = BASE_DIR / "output/pbb/ccris2"
OUT_DIR.mkdir(parents=True, exist_ok=True)

ACCTCRED_FILE = OUT_DIR / "acctcred.txt"   # LRECL=900
SUBACRED_FILE = OUT_DIR / "subacred.txt"   # LRECL=950
CREDITPO_FILE = OUT_DIR / "creditpo.txt"   # LRECL=400
PROVISIO_FILE = OUT_DIR / "provisio.txt"   # LRECL=400
LEGALACT_FILE = OUT_DIR / "legalact.txt"   # LRECL=100
CREDITOD_FILE = OUT_DIR / "creditod.txt"   # LRECL=210  (commented out in original)
ACCTREJT_FILE = OUT_DIR / "acctrejt.txt"   # LRECL=200

# ---------------------------------------------------------------------------
# PRODUCT GROUP DEFINITIONS  (%LET macros)
# ---------------------------------------------------------------------------
HP   = {128, 130, 380, 381, 700, 705, 983, 993, 996}
PRDA = {302, 350, 364, 365, 506, 902, 903, 910, 925, 951}
PRDB = {983, 993, 996}
PRDC = {110, 112, 114, 200, 201, 209, 210, 211, 212, 225, 227, 141,
        230, 232, 237, 239, 243, 244, 246}
PRDD = {111, 113, 115, 116, 117, 118, 119, 120, 126, 127, 129, 139, 140, 170,
        180, 181, 182, 193, 204, 205, 214, 215, 219, 220, 226, 228, 231, 233,
        234, 235, 245, 247, 142, 143, 183, 532, 533, 573, 574, 248, 147, 173,
        236, 238, 240, 241, 242, 300, 301, 304, 305, 345, 359, 361, 362, 363,
        504, 505, 509, 510, 515, 516, 531, 517, 518, 519, 521, 522,
        523, 524, 525, 526, 527, 528, 529, 530, 555, 556, 559, 560, 561, 564,
        565, 566, 567, 568, 569, 570, 900, 901, 906, 907, 909, 914, 915, 950}
PRDE = {135, 136, 138, 182, 194, 196, 315, 320, 325, 330, 335, 340,
        355, 356, 357, 358, 391, 148, 174, 195}
PRDF = {110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 122, 126,
        141, 142, 143, 147, 148, 173, 174,
        127, 129, 139, 140, 170, 172, 181, 194, 195, 196}
PRDG = {6, 7, 61, 200, 201, 204, 205, 209, 210, 211, 212, 214, 215, 219, 220,
        225, 226, 300, 301, 302, 304, 305, 309, 310, 315, 320, 325, 330, 335,
        340, 345, 350, 355, 356, 357, 358, 362, 363, 364, 365, 391, 504, 505,
        506, 509, 510, 515, 516, 900, 901, 902, 903, 904, 905, 909, 914, 915,
        919, 920, 950, 951}
PRDH = {500, 517, 518, 519, 520, 521, 522, 523, 524, 525, 526, 527, 528, 529,
        530, 555, 556, 559, 560, 561, 564, 565, 566, 567, 568, 569, 570}
PRDJ = {4, 5, 6, 7, 15, 20, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 60, 61, 62, 63,
        70, 71, 72, 73, 74, 75, 76, 77, 78, 100, 108}

ALP = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 ")

# SAS epoch
SAS_EPOCH = date(1960, 1, 1)

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


def date_from_z11(val) -> Optional[date]:
    """Parse a Z11 numeric date stored as MMDDYYYY (first 8 chars of zero-padded 11-digit)."""
    try:
        s = f"{int(val):011d}"
        return mdy(int(s[0:2]), int(s[2:4]), int(s[4:8]))
    except Exception:
        return None


def ndays_format(nodays: int) -> int:
    """Convert days overdue to arrears bucket (SAS NDAYS informat)."""
    if nodays <= 0:
        return 0
    thresholds = [30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330, 365]
    for i, t in enumerate(thresholds, 1):
        if nodays <= t:
            return i
    return 24  # > 365 days: caller rounds nodays/30


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


def fmt_f(v, width: int, dec: int) -> str:
    try:
        return f"{float(v):{width}.{dec}f}"
    except Exception:
        return " " * width


def fmt_s(v, width: int) -> str:
    s = safe_str(v)
    return s[:width].ljust(width)


def fmt_ddmmyy8(dt) -> str:
    """Format date as DD/MM/YY (8 chars)."""
    if dt is None:
        return "        "
    try:
        if isinstance(dt, (int, float)):
            dt = sas_to_date(int(dt))
        return dt.strftime("%d/%m/%y")
    except Exception:
        return "        "


def build_record(fields: list[tuple]) -> str:
    """
    Build a fixed-width record.
    fields: list of (start_col_1based, value_str)
    """
    max_col = max(p + len(v) for p, v in fields)
    buf = list(" " * max_col)
    for pos, val in fields:
        for i, ch in enumerate(val):
            idx = pos - 1 + i
            if idx < max_col:
                buf[idx] = ch
    return "".join(buf)


def parse_z11_mmddyyyy(val) -> tuple[str, str, str]:
    """Return (dd, mm, yyyy) strings from Z11 MMDDYYYY numeric field."""
    if val is None or safe_float(val, 0) == 0:
        return "", "", ""
    s = f"{int(val):011d}"
    return s[2:4], s[0:2], s[4:8]


# ---------------------------------------------------------------------------
# LOAD DATES FROM BNM.REPTDATE
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

    day = reptdate.day
    mm  = reptdate.month
    yy  = reptdate.year

    if day == 8:
        sdd, wk, wk1 = 1, "1", "4"
    elif day == 15:
        sdd, wk, wk1 = 9, "2", "1"
    elif day == 22:
        sdd, wk, wk1 = 16, "3", "2"
    else:
        sdd, wk, wk1 = 23, "4", "3"

    mm1 = (mm - 1) if wk == "1" else mm
    if mm1 == 0:
        mm1 = 12

    if wk == "4":
        mm2 = mm
    else:
        mm2 = mm - 1
        if mm2 == 0:
            mm2 = 12

    sdate_obj = date(yy, mm, day)
    idate_obj = date(yy, mm, 1)
    xdate_obj = date(yy if mm > 1 else yy - 1, mm - 1 if mm > 1 else 12, 1)
    currdte   = date.today() - timedelta(days=1)

    return {
        "reptdate":  reptdate,
        "nowk":      wk,
        "nowk1":     wk1,
        "reptmon":   f"{mm:02d}",
        "reptmon1":  f"{mm1:02d}",
        "reptmon2":  f"{mm2:02d}",
        "reptyear":  str(yy),
        "reptyr2":   str(yy)[2:],
        "rdate":     reptdate.strftime("%d/%m/%y"),
        "reptday":   f"{day:02d}",
        "sdate_sas": date_to_sas(sdate_obj),
        "idate_sas": date_to_sas(idate_obj),
        "xdate_sas": date_to_sas(xdate_obj),
        "mdate_sas": date_to_sas(reptdate),
        "rdate1":    date_to_sas(reptdate),
        "rdate2":    currdte.strftime("%y%m%d"),
        "currdte":   currdte,
        "sdate_obj": sdate_obj,
        "idate_obj": idate_obj,
        "xdate_obj": xdate_obj,
    }


# ---------------------------------------------------------------------------
# LOAD FX RATES
# ---------------------------------------------------------------------------

def load_fx_rates(forate_dir: Path, rdate1: int) -> dict:
    path = forate_dir / "foratebkp.parquet"
    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT CURCODE, SPOTRATE
        FROM (
            SELECT CURCODE, SPOTRATE,
                   ROW_NUMBER() OVER (PARTITION BY CURCODE ORDER BY REPTDATE DESC) AS rn
            FROM read_parquet('{path}')
            WHERE REPTDATE <= {rdate1}
        ) t
        WHERE rn = 1
    """).fetchall()
    con.close()
    return {r[0]: float(r[1]) for r in rows}


def get_forate(fx_rates: dict, curcode: str) -> float:
    return fx_rates.get(safe_str(curcode).strip(), 1.0)


# ---------------------------------------------------------------------------
# LOAD BLR / LENDING RATES
# ---------------------------------------------------------------------------

def load_blrate(bnm_dir: Path, bnmprev_dir: Path) -> tuple[str, int, str]:
    blrate    = "0.00"
    blreff    = 0
    olrate    = "0.00"

    for parq, is_idx in [
        (bnm_dir / "lnrate.parquet", True),
        (bnmprev_dir / "lnrate.parquet", False),
    ]:
        if not parq.exists():
            continue
        con = duckdb.connect()
        rows = con.execute(
            f"SELECT CURRATE, EFFDATE FROM read_parquet('{parq}') WHERE RINDEX=1 LIMIT 1"
        ).fetchall()
        con.close()
        for row in rows:
            rate_str = f"{float(row[0]):.2f}"
            if is_idx:
                blrate = rate_str
                blreff = safe_int(row[1], 0)
            olrate = rate_str

    return blrate, blreff, olrate


# ---------------------------------------------------------------------------
# LOAD WRIOFAC
# ---------------------------------------------------------------------------

def load_wriofac(filepath: Path) -> dict:
    """
    Parse WRITOFF text file.
    INPUT @10 ACCTNO 10. @20 NOTENO 10. @74 RESTRUCT $1.
    * ESMR2013-2133: CONVERSION FROM 'R','S' TO 'C','T' *
    """
    result = {}
    if not filepath.exists():
        logger.warning(f"WRITOFF file not found: {filepath}")
        return result
    with open(filepath, "r", encoding="latin-1") as f:
        for line in f:
            if len(line) < 74:
                continue
            acctno   = safe_int(line[9:19].strip(), 0)
            noteno   = safe_int(line[19:29].strip(), 0)
            restruct = line[73:74] if len(line) >= 74 else " "
            # ESMR2013-2133: CONVERSION FROM 'R','S' TO 'C','T'
            if restruct == "R":
                restruct = "C"
            if restruct == "S":
                restruct = "T"
            acctstat = ""
            if restruct == "C":
                acctstat = "C"
            elif restruct == "T":
                acctstat = "T"
            result[(acctno, noteno)] = {"RESTRUCT": restruct, "ACCTSTAT": acctstat}
    return result


# ---------------------------------------------------------------------------
# COMPUTE TYPEPRC
# ---------------------------------------------------------------------------

def compute_typeprc(
    acctno: int,
    curcode: str,
    facgptype: str,
    pritype: str,
    product: int,
    pricing: float,
    prirate: float,
    blrate: str,
    indexcd: int,
) -> str:
    blrate_f = safe_float(blrate, 0.0)

    # PBB-specific: 2500000000-2599999999 range
    if 2500000000 <= acctno <= 2599999999:
        return "41" if curcode == "MYR" else "68"

    if facgptype == "6":
        return "80"

    if pritype == "BLR":
        if product in (166, 168):
            return "42" if pricing < blrate_f else "41"
        return "42" if prirate < blrate_f else "41"
    if pritype == "FIXED":
        return "59"
    if pritype == "BR":
        return "43"
    if pritype == "COF":
        cof_map = {900: "53", 908: "54", 912: "55", 916: "56",
                   904: "57", 920: "58", 924: "68"}
        return cof_map.get(indexcd, "59")
    if pritype == "SOR":
        return "79" if indexcd == 925 else "59"
    if pritype == "ORFR":
        return "51"
    if pritype == "SBR":
        return "44"
    if pritype == "MYOR":
        return "61"
    if pritype == "MYORi":
        return "62"
    if pritype == "SOFR":
        return "63"
    if pritype == "SONIA":
        return "64"
    if pritype == "ESTR":
        return "65"
    if pritype == "SORA":
        return "66"
    if pritype == "TONAR":
        return "67"
    if pritype == "OORFR":
        return "51"
    return "59"


# ---------------------------------------------------------------------------
# PROCESS LNACC4
# ---------------------------------------------------------------------------

def process_lnacc4(bnm_dir: Path) -> dict:
    path = bnm_dir / "lnacc4.parquet"
    con = duckdb.connect()
    df = con.execute(f"""
        SELECT ACCTNO, AANO, PRODUCT, LNTERM, MNIAPDTE, REVIEWDT,
               FIRST_DISBDT, SETTLEMNTDT, TOTACBAL, MNIAPLMT, EXPRDATE
        FROM read_parquet('{path}')
    """).pl()
    con.close()

    result = {}
    for row in df.to_dicts():
        acctno = safe_int(row.get("ACCTNO", 0))
        r = {
            "ACCT_AANO":   safe_str(row.get("AANO", "")),
            "ACCT_LNTYPE": safe_int(row.get("PRODUCT", 0)),
            "ACCT_LNTERM": safe_int(row.get("LNTERM", 0)),
            "ACCT_OUTBAL": round(safe_float(row.get("TOTACBAL", 0)) * 100),
            "ACCT_APLMT":  round(safe_float(row.get("MNIAPLMT", 0)) * 100),
            "ACCT_REVDD": "", "ACCT_REVMM": "", "ACCT_REVYY": "",
            "ACCT_1DISDD": "", "ACCT_1DISMM": "", "ACCT_1DISYY": "",
            "ACCT_SETDD": "", "ACCT_SETMM": "", "ACCT_SETYY": "",
            "ACCT_EXPDD": "", "ACCT_EXPMM": "", "ACCT_EXPYY": "",
            "ACCT_ALDD":  "", "ACCT_ALMM":  "", "ACCT_ALYY":  "",
        }
        for key, dd_f, mm_f, yy_f in [
            ("REVIEWDT",    "ACCT_REVDD",   "ACCT_REVMM",   "ACCT_REVYY"),
            ("FIRST_DISBDT","ACCT_1DISDD",  "ACCT_1DISMM",  "ACCT_1DISYY"),
            ("SETTLEMNTDT", "ACCT_SETDD",   "ACCT_SETMM",   "ACCT_SETYY"),
            ("EXPRDATE",    "ACCT_EXPDD",   "ACCT_EXPMM",   "ACCT_EXPYY"),
            ("MNIAPDTE",    "ACCT_ALDD",    "ACCT_ALMM",    "ACCT_ALYY"),
        ]:
            v = row.get(key)
            if v is not None and safe_float(v, 0) != 0:
                dt = date_from_z11(v)
                if dt:
                    r[dd_f] = f"{dt.day:02d}"
                    r[mm_f] = f"{dt.month:02d}"
                    r[yy_f] = f"{dt.year:04d}"
        result[acctno] = r
    return result


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    logger.info("Starting EIBWCCR6")

    # --- Date context ---
    ctx = load_dates(BNM_DIR)
    reptday   = ctx["reptday"]
    reptmon   = ctx["reptmon"]
    reptyear  = ctx["reptyear"]
    nowk      = ctx["nowk"]
    rdate1    = ctx["rdate1"]
    rdate2    = ctx["rdate2"]
    sdate_sas = ctx["sdate_sas"]
    idate_sas = ctx["idate_sas"]
    xdate_sas = ctx["xdate_sas"]
    logger.info(f"REPTDATE={ctx['reptdate']} WK={nowk} SDATE={ctx['sdate_obj']}")

    # --- FX rates ---
    fx_rates = load_fx_rates(FORATE_DIR, rdate1)

    # --- BLR rates ---
    blrate, blreff_sas, olrate = load_blrate(BNM_DIR, BNMPREV_DIR)
    logger.info(f"BLRATE={blrate} OLRATE={olrate} BLREFF={blreff_sas}")

    # --- WRIOFAC ---
    wriofac = load_wriofac(WRITOFF_FILE)

    # --- LNACC4 ---
    lnacc4_map = process_lnacc4(BNM_DIR)

    # --- Load CCRISP.ELDS ---
    elds_path = CCRISP_DIR / "elds.parquet"
    con = duckdb.connect()
    elds_df = con.execute(f"SELECT * FROM read_parquet('{elds_path}')").pl()
    con.close()

    # --- Load HPREJ ---
    hprej_path = HPREJ_DIR / "ehpa123.parquet"
    con = duckdb.connect()
    hprj_df = con.execute(f"""
        SELECT AANO, ICDATE, CCRIS_STAT
        FROM read_parquet('{hprej_path}')
        WHERE CCRIS_STAT = 'D'
          AND ICDATE >= {xdate_sas}
          AND ICDATE <= {sdate_sas}
    """).pl()
    con.close()

    # --- Load and process RVNOTE ---
    rvnote_path = BNMD_DIR / "rvnote.parquet"
    con = duckdb.connect()
    rvnote_raw = con.execute(f"SELECT * FROM read_parquet('{rvnote_path}')").pl()
    con.close()

    rvnote_records = []
    for row in rvnote_raw.to_dicts():
        vinno = safe_str(row.get("VINNO", ""))
        aano1 = vinno.strip()[:13]
        comp  = aano1.translate(str.maketrans("", "", "@*(#)-")).replace(" ", "")
        aano  = comp if len(comp) == 13 else ""

        branch  = safe_int(row.get("NTBRCH", 0))
        costctr = safe_int(row.get("COSTCTR", 0))
        # FOLLOWING MAPPING MUST BE IN SYNC WITH EIBWCC5C
        if costctr == 8044:
            branch = 902
        if costctr == 8048:
            branch = 903

        lasttran = row.get("LASTTRAN")
        if lasttran is None or safe_float(lasttran, 0) == 0:
            continue
        icdate_dt = date_from_z11(lasttran)
        if icdate_dt is None:
            continue
        icdate_sas2 = date_to_sas(icdate_dt)
        if icdate_sas2 < xdate_sas:
            continue

        rvnote_records.append({
            "AANO":     aano,
            "ACCTNO":   row.get("ACCTNO"),
            "NOTENO":   row.get("NOTENO"),
            "BRANCH":   branch,
            "COSTCTR":  costctr,
            "ICDATE":   icdate_sas2,
            "REVERSED": row.get("REVERSED"),
        })

    rvnote_proc = (
        pl.DataFrame(rvnote_records)
        if rvnote_records
        else pl.DataFrame(schema={
            "AANO": pl.Utf8, "ACCTNO": pl.Int64, "NOTENO": pl.Int64,
            "BRANCH": pl.Int64, "COSTCTR": pl.Int64, "ICDATE": pl.Int64,
            "REVERSED": pl.Utf8,
        })
    )

    # --- Merge HPRJ with RVNOTE: IF (A AND B) OR B ---
    if not hprj_df.is_empty() and not rvnote_proc.is_empty():
        hprj_merged = (
            rvnote_proc
            .join(hprj_df, on="AANO", how="left")
        )
    elif not rvnote_proc.is_empty():
        hprj_merged = rvnote_proc
    else:
        hprj_merged = hprj_df

    # --- Load SUMM1 ---
    summ1_path = BNMSUM_DIR / f"summ1{rdate2}.parquet"
    summ1_map: dict = {}
    if summ1_path.exists():
        con = duckdb.connect()
        try:
            summ1_df = con.execute(f"""
                SELECT *
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY AANO ORDER BY STAGE) AS rn
                    FROM read_parquet('{summ1_path}')
                    WHERE DATE <= {rdate1}
                ) t
                WHERE rn = 1
            """).pl()
            for s in summ1_df.to_dicts():
                summ1_map[safe_str(s.get("AANO", ""))] = s
        except Exception as e:
            logger.warning(f"Could not load SUMM1: {e}")
        con.close()

    # ---------------------------------------------------------------------------
    # SPLIT ELDS INTO LOAN / LNRJ
    # ---------------------------------------------------------------------------
    loan_rows: list[dict] = []
    lnrj_rows: list[dict] = []

    for row in elds_df.to_dicts():
        row["ACTY"] = "LN"
        branch   = safe_int(row.get("BRANCH", 0))
        tranbrno = safe_int(row.get("TRANBRNO", 0))
        smesize  = safe_int(row.get("SMESIZE", 0))

        if branch == 902:
            branch = 904
        row["BRANCH"] = branch
        if tranbrno not in (None, 0):
            row["BRANCH"] = tranbrno
        if smesize not in (None, 0):
            row["CUSTCODE"] = smesize

        aadate = safe_int(row.get("AADATE", 0))
        icdate = safe_int(row.get("ICDATE", 0))

        if idate_sas <= aadate <= sdate_sas:
            loan_rows.append(dict(row))
        if xdate_sas <= icdate <= sdate_sas:
            lnrj_rows.append(dict(row))

    # LNHPRJ = LNRJ + HPRJ
    lnhprj_rows: list[dict] = list(lnrj_rows)
    for row in hprj_merged.to_dicts():
        r = dict(row)
        if r.get("NOTENO") is None:
            r["NOTENO"] = 0
        lnhprj_rows.append(r)

    # ---------------------------------------------------------------------------
    # PROCESS LOAN ROWS -> acctcred_records
    # ---------------------------------------------------------------------------
    acctcred_records: list[dict] = []

    for row in loan_rows:
        acty      = safe_str(row.get("ACTY", "LN"))
        loantype  = safe_int(row.get("LOANTYPE", 0))
        product   = safe_int(row.get("PRODUCT", loantype))
        acctno    = safe_int(row.get("ACCTNO", 0))
        noteno    = safe_int(row.get("NOTENO", 0))
        branch    = safe_int(row.get("BRANCH", 0))
        ntbrch    = safe_int(row.get("NTBRCH", 0))
        accbrch   = safe_int(row.get("ACCBRCH", 0))
        costctr   = safe_int(row.get("COSTCTR", 0))
        commno    = safe_int(row.get("COMMNO", 0))
        ind       = safe_str(row.get("IND", ""))
        paidind   = safe_str(row.get("PAIDIND", ""))
        borstat   = safe_str(row.get("BORSTAT", ""))
        accstat   = safe_str(row.get("ACCSTAT", ""))
        orgtype   = safe_str(row.get("ORGTYPE", ""))
        facgptype = safe_str(row.get("FACGPTYPE", ""))
        restruct  = safe_str(row.get("RESTRUCT", ""))
        refinanc  = safe_str(row.get("REFINANC", ""))
        recourse  = safe_str(row.get("RECOURSE", ""))
        riskcode  = safe_str(row.get("RISKCODE", ""))
        riskrate  = safe_str(row.get("RISKRATE", ""))
        nplind    = safe_str(row.get("NPLIND", ""))
        odstatus  = safe_str(row.get("ODSTATUS", ""))
        gp3ind    = safe_str(row.get("GP3IND", ""))
        leg       = safe_str(row.get("LEG", ""))
        cproduct  = safe_str(row.get("CPRODUCT", ""))
        pritype   = safe_str(row.get("PRITYPE", ""))
        ntint     = safe_str(row.get("NTINT", ""))
        score     = safe_str(row.get("SCORE", ""))
        modeldes  = safe_str(row.get("MODELDES", ""))
        prodesc   = safe_str(row.get("PRODESC", ""))
        aano      = safe_str(row.get("AANO", ""))
        curcode   = safe_str(row.get("CURCODE", "MYR")) or "MYR"
        sector    = safe_str(row.get("SECTOR", ""))
        sector1   = safe_str(row.get("SECTOR1", ""))
        sectcd    = safe_str(row.get("SECTCD", ""))
        payfreq   = safe_str(row.get("PAYFREQ", ""))
        ccriscode = safe_str(row.get("CCRICODE", ""))
        openind   = safe_str(row.get("OPENIND", ""))
        dnbfisme  = safe_str(row.get("DNBFISME", ""))
        reason    = safe_str(row.get("REASON", ""))
        reconame  = safe_str(row.get("RECONAME", ""))
        reftype   = safe_str(row.get("REFTYPE", ""))
        nmref1    = safe_str(row.get("NMREF1", ""))
        nmref2    = safe_str(row.get("NMREF2", ""))
        apvnme1   = safe_str(row.get("APVNME1", ""))
        apvnme2   = safe_str(row.get("APVNME2", ""))
        newic     = safe_str(row.get("NEWIC", ""))
        collmake  = safe_str(row.get("COLLMAKE", ""))
        refin_flg     = safe_str(row.get("REFIN_FLG", ""))
        old_fi_cd     = safe_str(row.get("OLD_FI_CD", ""))
        old_mastacc   = safe_str(row.get("OLD_MASTACC", ""))
        old_subacc    = safe_str(row.get("OLD_SUBACC", ""))
        lu_add1       = safe_str(row.get("LU_ADD1", ""))
        lu_add2       = safe_str(row.get("LU_ADD2", ""))
        lu_add3       = safe_str(row.get("LU_ADD3", ""))
        lu_add4       = safe_str(row.get("LU_ADD4", ""))
        lu_town       = safe_str(row.get("LU_TOWN_CITY", ""))
        lu_postcode   = safe_str(row.get("LU_POSTCODE", ""))
        lu_state      = safe_str(row.get("LU_STATE_CD", ""))
        lu_country    = safe_str(row.get("LU_COUNTRY_CD", ""))
        lu_source     = safe_str(row.get("LU_SOURCE", ""))
        aprvdt        = safe_str(row.get("APRVDT", ""))

        custcode  = safe_int(row.get("CUSTCODE", 0))
        pzipcode  = safe_int(row.get("PZIPCODE", 0))
        indexcd   = safe_int(row.get("INDEXCD", 0))
        earnterm  = safe_int(row.get("EARNTERM", 0))
        noteterm  = safe_int(row.get("NOTETERM", 0))
        term      = safe_int(row.get("TERM", 0))
        billcnt   = safe_int(row.get("BILLCNT", 0))
        bldate    = safe_int(row.get("BLDATE", 0))
        dlvdate   = safe_int(row.get("DLVDATE", 0))
        exprdate  = safe_int(row.get("EXPRDATE", 0))

        amount     = safe_float(row.get("AMOUNT", 0))
        balance    = safe_float(row.get("BALANCE", 0))
        curbal     = safe_float(row.get("CURBAL", 0))
        appvalue   = safe_float(row.get("APPVALUE", 0))
        realisab   = safe_float(row.get("REALISAB", 0))
        cavaiamt   = safe_float(row.get("CAVAIAMT", 0))
        limtcurr   = safe_float(row.get("LIMTCURR", 0))
        apprlim2   = safe_float(row.get("APPRLIM2", 0))
        netproc    = safe_float(row.get("NETPROC", 0))
        apprlmaa   = safe_float(row.get("APPRLMAA", 0))
        lmtamt     = safe_float(row.get("LMTAMT", 0))
        odxsamt    = safe_float(row.get("ODXSAMT", 0))
        biltot     = safe_float(row.get("BILTOT", 0))
        census     = safe_float(row.get("CENSUS", 0))
        spaamt     = safe_float(row.get("SPAAMT", 0))
        intrate    = safe_float(row.get("INTRATE", 0))
        prirate    = safe_float(row.get("PRIRATE", 0))
        pricing    = safe_float(row.get("PRICING", 0))
        rate       = safe_float(row.get("RATE", 0))
        mthinstlmt = safe_float(row.get("MTHINSTLMT", 0))
        feeamt     = safe_float(row.get("FEEAMT", 0))
        rebate     = safe_float(row.get("REBATE", 0))
        intearn    = safe_float(row.get("INTEARN", 0))
        intearn4   = safe_float(row.get("INTEARN4", 0))
        intamt     = safe_float(row.get("INTAMT", 0))
        corgamt    = safe_float(row.get("CORGAMT", 0))
        iisopbal   = safe_float(row.get("IISOPBAL", 0))
        iissuamt   = safe_float(row.get("IISSUAMT", 0))
        iisbwamt   = safe_float(row.get("IISBWAMT", 0))
        totiis     = safe_float(row.get("TOTIIS", 0))
        totiisr    = safe_float(row.get("TOTIISR", 0))
        totwof     = safe_float(row.get("TOTWOF", 0))
        spopbal    = safe_float(row.get("SPOPBAL", 0))
        spwbamt    = safe_float(row.get("SPWBAMT", 0))
        spwof      = safe_float(row.get("SPWOF", 0))
        spdanah    = safe_float(row.get("SPDANAH", 0))
        sptrans    = safe_float(row.get("SPTRANS", 0))
        refin_amt  = safe_float(row.get("REFIN_AMT", 0))

        maturedt  = row.get("MATUREDT")
        dlivrydt  = row.get("DLIVRYDT")
        nxbildt   = row.get("NXBILDT")
        collyear  = row.get("COLLYEAR")
        issuedt   = row.get("ISSUEDT")
        cappdate  = row.get("CAPPDATE")
        complidt  = row.get("COMPLIDT")
        rjdate    = row.get("RJDATE")
        aadate_v  = safe_int(row.get("AADATE", 0))
        icdate_v  = safe_int(row.get("ICDATE", 0))

        # --- Initialise output fields ---
        oldbrh   = 0
        arrears  = 0
        nodays   = 0
        instalm  = 0
        undrawn  = amount
        acctstat = "O"
        ficode   = branch
        outstand = 0.0
        interdue = 0.0
        collval  = 0.0
        cagamas  = 0
        syndicat = "N"
        specialf = "00"
        fconcept_v = 0
        facility = safe_int(row.get("FACILITY", 0))
        faccode  = safe_int(row.get("FACCODE", 0))
        apcode   = 0
        classifi = ""
        liabcode = ""
        purposes = safe_str(row.get("PURPOSES", ""))
        payfreqc = safe_str(row.get("PAYFREQC", ""))
        lnty     = 0

        paydd = "00"; paymm = "00"; payyr = "0000"
        grantdd = "00"; grantmm = "00"; grantyr = "0000"
        lnmatdd = "00"; lnmatmm = "00"; lnmatyr = "0000"
        ormatdd = "00"; ormatmm = "00"; ormatyr = "0000"
        dlvrdd  = "00"; dlvrmm  = "00"; dlvryr  = "0000"
        bilduedd = "00"; bilduemm = "00"; bildueyr = "0000"
        collmm   = "00"; collyr   = "00"
        issuedd  = ""; issuemm = ""; issueyy = ""

        # Exclude specific accounts
        if ((acctno == 8027370307 and noteno == 10) or
                (acctno == 8124498008 and noteno == 10010) or
                (acctno == 8124948801 and noteno == 10)):
            continue

        # -------- OD PROCESSING --------
        if acty == "OD":
            syndicat = " "
            collval  = realisab
            purposes = ccriscode[:4] if ccriscode else ""
            if collval in (0, 0.0):
                collval = 0.0
            odxsamt  = odxsamt * 100
            lmtamt   = lmtamt  * 100
            biltot   = 0.0
            limtcurr = apprlim2 * 100
            loantype = product
            apcode   = product
            noteno   = 0
            sector   = sector1
            if product in (124, 165, 191):
                facility = 34111
            else:
                facility = 34110

            if product == 114:
                specialf   = "12"
                fconcept_v = 60
                noteterm   = 12
                payfreqc   = "20"
            if product in (160, 161, 162, 163, 164, 165, 166, 167):
                specialf   = "00"
                fconcept_v = 10
                noteterm   = 12
                payfreqc   = "20"
            else:
                specialf   = "00"
                fconcept_v = 51
                noteterm   = 12
                payfreqc   = "20"

            feeamt   = 0.0
            interdue = 0.0

            exoddate = safe_int(row.get("EXODDATE", 0))
            tempoddt = safe_int(row.get("TEMPODDT", 0))
            if (exoddate != 0 or tempoddt != 0) and curbal < 0:
                if exoddate == 0:
                    exoddate = tempoddt
                oddate = date_from_z11(exoddate)
                if oddate:
                    oddays = date_to_sas(oddate)
                    paydd = f"{oddate.day:02d}"
                    paymm = f"{oddate.month:02d}"
                    payyr = f"{oddate.year:04d}"
                    nodays = sdate_sas - oddays + 1
                    if nodays > 0:
                        arrears = ndays_format(nodays)
                        if arrears == 24:
                            arrears = round(nodays / 30)
                        instalm = arrears

            if riskcode == "1":
                classifi = "C"
            elif riskcode == "2":
                classifi = "S"
            elif riskcode == "3":
                classifi = "D"
            elif riskcode == "4":
                classifi = "B"
            elif odstatus == "AC" or (exoddate == 0 and tempoddt == 0):
                classifi = "P"

            for col_key in ["COL1", "COL2", "COL3", "COL4", "COL5"]:
                col_val = safe_str(row.get(col_key, ""))
                lb1 = col_val[0:2].strip()
                lb2 = col_val[3:5].strip() if len(col_val) >= 5 else ""
                for lb in (lb1, lb2):
                    if lb:
                        liabcode = lb
                        break
                if liabcode:
                    break

            issuedd = f"{safe_int(row.get('ODLMTDD', 0)):02d}"
            issuemm = f"{safe_int(row.get('ODLMTMM', 0)):02d}"
            issueyy = f"{safe_int(row.get('ODLMTYY', 0)):04d}"

            acctstat = "S" if openind in ("P", "C", "B") else "O"
            curbal   = curbal * -1
            outstand = curbal * 100

            # BNM TAXANOMY & CCRIS ENHANCEMENT (2012-0752)
            syndicat   = "N"
            specialf   = "00"
            fconcept_v = format_odfconcept(product)

        else:
            # -------- LOAN PROCESSING --------
            collval  = appvalue * 100
            outstand = balance  * 100
            cavaiamt = cavaiamt * 100
            lmtamt   = 0.0
            odxsamt  = 0.0
            biltot   = biltot   * 100
            census   = census   * 100
            limtcurr = apprlim2
            if limtcurr in (None, 0.0):
                limtcurr = netproc

            hp_lt  = {122, 106, 128, 130, 120, 121, 700, 705, 709, 710}
            hp_lt2 = {100, 101, 102, 103, 110, 111, 112, 113, 114, 115, 116, 117,
                      118, 120, 121, 122, 123, 124, 125, 127, 135, 136, 170, 180,
                      106, 128, 130, 700, 705, 709, 710, 194, 195, 380, 381}

            if ((acctno > 8000000000 and loantype in hp_lt) or
                    (acctno < 2999999999 and loantype in hp_lt2)):
                dd, mm, yy = parse_z11_mmddyyyy(issuedt)
                issuedd, issuemm, issueyy = dd, mm, yy
                limtcurr = netproc
                if (10010 <= noteno <= 10099) or (30010 <= noteno <= 30099):
                    if ind == "B":
                        limtcurr = corgamt + (-1 * intamt)
            else:
                use_dt = cappdate if (cappdate is not None and
                                     safe_float(cappdate, 0) != 0) else issuedt
                dd, mm, yy = parse_z11_mmddyyyy(use_dt)
                issuedd, issuemm, issueyy = dd, mm, yy

            cagamas    = 0
            syndicat   = "N"
            specialf   = "00"
            fconcept_v = 99
            # ***RESTRUCT=' ';
            # *ESMR 2013-2133: RESTRUCT MAINTAINED AS 'C' 'T' IN HOST
            #                  SAS DWH MAINTAIN AS 'R' 'S' *;

            # SPECIALF mapping
            if loantype in (124, 145):
                specialf = "00"
            if loantype == 566:
                specialf = "10"
            if loantype in (991, 994) and pzipcode == 566:
                specialf = "10"
            if loantype in (170, 564, 565):
                specialf = "11"
            if loantype in (991, 994) and pzipcode in (170, 564, 565):
                specialf = "11"
            if loantype in (559, 560, 567):
                specialf = "12"
            if loantype in (991, 994) and pzipcode in (559, 560, 567):
                specialf = "12"
            if loantype in (568, 570, 532, 533, 573, 574):
                specialf = "14"
            if loantype in (991, 994) and pzipcode in (568, 570, 573):
                specialf = "14"
            if loantype == 561:
                specialf = "16"
            if loantype in (991, 994) and pzipcode == 561:
                specialf = "16"
            if loantype in (555, 556):
                specialf = "18"
            if loantype in (991, 994) and pzipcode in (555, 556):
                specialf = "18"
            if loantype in (521, 522, 523, 528):
                specialf = "99"
            if loantype in (991, 994) and pzipcode in (521, 522, 523, 528):
                specialf = "99"

            # FCONCEPT mapping
            if loantype in (124, 145):
                fconcept_v = 10
            if loantype in PRDF:
                fconcept_v = 10
            if loantype in (981, 982, 984, 985) and pzipcode in PRDF:
                fconcept_v = 10
            if loantype == 180:
                fconcept_v = 13
            if loantype in (981, 982, 984, 985) and pzipcode == 180:
                fconcept_v = 13
            if loantype in (128, 130, 131, 132, 983):
                fconcept_v = 16
            if loantype == 193:
                fconcept_v = 18
            if loantype in (135, 136, 138, 182):
                fconcept_v = 26
            if loantype in (982, 985) and pzipcode in (135, 136):
                fconcept_v = 26
            if loantype in PRDG:
                fconcept_v = 51
            if loantype in (981, 982, 994, 995) and pzipcode in PRDG:
                fconcept_v = 51
            if loantype in (910, 925):
                fconcept_v = 52
            if loantype in (992, 995) and pzipcode in (910, 925):
                fconcept_v = 52
            if loantype in {4,5,15,20,25,26,27,28,29,30,31,32,33,34,60,62,
                            63,70,71,72,73,74,75,76,77,78,360,380,381,
                            390,700,705,993,996}:
                fconcept_v = 59
            if loantype in {227,228,230,231,232,233,234,235,236,237,
                            532,533,573,574,248,
                            238,239,240,241,242,243,359,361,531,906,907}:
                fconcept_v = 60
            if loantype in PRDH:
                fconcept_v = 99
            if loantype in (991, 992, 994, 995) and pzipcode in PRDH:
                fconcept_v = 99
            if loantype in (180, 914, 915, 919, 920, 925, 950, 951):
                syndicat = "Y"

            # INTERDUE
            if ntint != "A":
                interdue = balance + (-1 * curbal) + (-1 * feeamt)
            else:
                curbal   = curbal + intearn + (-1 * intamt)
                interdue = 0.0
                if curbal < 0:
                    curbal = 0.0

            if loantype in (700, 705, 709, 710, 752, 760):
                curbal   = curbal + (-1 * rebate) + (-1 * intearn4)
                interdue = 0.0

            instalm  = billcnt
            lnty     = 0
            acctstat = "O"
            # /*
            # IF CAVAIAMT=.  THEN UNDRAWN=0;
            #                ELSE UNDRAWN=CAVAIAMT*100;
            # */

            if paidind == "P":
                acctstat = "S"; arrears = 0; instalm = 0

            if loantype in (128, 130, 700, 705, 380, 381, 709, 710):
                lnty = 1
            if lnty == 1 and (noteno >= 98000 or borstat == "S"):
                acctstat = "T"
            if lnty == 0 and borstat == "S":
                acctstat = "T"

            if loantype in (950, 951, 915, 953):
                syndicat = "Y"
            if loantype in (248, 532, 533, 573, 574):
                noteterm = earnterm
            if loantype in (330, 302, 506, 902, 903, 951, 953) and cproduct == "170":
                noteterm = 12
            if loantype == 521:
                payfreqc = "13"
            if loantype in (124, 145, 532, 533, 573, 574, 248):
                payfreqc = "14"

            _pf14_types = {
                5, 6, 25, 15, 20, 111, 139, 140, 122, 106, 128, 130,
                120, 121, 201, 204, 205, 225, 300, 301, 307, 320, 330,
                504, 505, 506, 511, 568, 555, 556, 559, 567, 564, 565,
                570, 566, 700, 705, 709, 710, 900, 901, 910, 912,
                930, 126, 127, 359, 981, 982, 983, 991, 993,
                117, 113, 118, 115, 119, 116, 227, 228, 230, 231, 234,
                235, 236, 237, 238, 239, 240, 241, 242,
            }
            _pf14_zip = {
                300, 304, 305, 307, 310, 330, 504, 505, 506, 511, 515,
                359, 555, 556, 559, 560, 564, 565, 570, 571, 574, 575, 900,
                901, 902, 910, 912, 930,
            }
            if loantype in _pf14_types:
                payfreqc = "14"
            if loantype == 992 and pzipcode in _pf14_zip:
                payfreqc = "14"
            if loantype == 330 and safe_float(cproduct, 0) == 0:
                payfreqc = "16"
            if loantype in (302, 506, 902, 903, 951, 953, 330) and cproduct == "170":
                payfreqc = "19"
            if loantype in (569, 904, 932, 933, 950, 915):
                payfreqc = "21"

            # RESTRUCT
            if loantype in (128, 130, 131, 132, 720, 725, 380, 381, 700, 705, 709, 710):
                if noteno >= 98000:
                    restruct = "C"
                if borstat == "S":
                    restruct = "T"
            else:
                if borstat == "S":
                    restruct = "T"
                if borstat == "Q":
                    restruct = "C"

            if noteterm in (None, 0):
                noteterm = term
            if borstat == "W":
                acctstat = "P"
            elif borstat == "X":
                acctstat = "W"

            if product in (135, 136, 166, 168):
                intrate = pricing
            elif facgptype == "6":
                intrate = rate
            else:
                intrate = prirate

            # CAGAMAS
            cagamas_map = {
                "B": 20041998, "C": 20041998, "F": 25081997, "G": 30051997,
                "H": 12031999, "I": 25081999, "K":  3082000, "L": 18082000,
                "M": 18082000, "J":  8062000, "P": 25082000, "Q": 25082000,
            }
            cagamas = cagamas_map.get(refinanc, 0)
            if cagamas > 0:
                recourse = "Y"

            # NODAYS / date fields
            if bldate > 0:
                nodays = sdate_sas - bldate
            if nodays < 0:
                nodays = 0
            if bldate > 0:
                bd = sas_to_date(bldate)
                if bd:
                    paydd = f"{bd.day:02d}"
                    paymm = f"{bd.month:02d}"
                    payyr = f"{bd.year:04d}"
            if dlvdate > 0:
                dd_obj = sas_to_date(dlvdate)
                if dd_obj:
                    grantdd = f"{dd_obj.day:02d}"
                    grantmm = f"{dd_obj.month:02d}"
                    grantyr = f"{dd_obj.year:04d}"
            if exprdate > 0:
                ed = sas_to_date(exprdate)
                if ed:
                    lnmatdd = f"{ed.day:02d}"
                    lnmatmm = f"{ed.month:02d}"
                    lnmatyr = f"{ed.year:04d}"

            ormatdd, ormatmm, ormatyr = parse_z11_mmddyyyy(maturedt)
            dlvr_dd, dlvr_mm, dlvr_yr = parse_z11_mmddyyyy(dlivrydt)
            if dlvr_dd:
                dlvrdd = dlvr_dd; dlvrmm = dlvr_mm; dlvryr = dlvr_yr
            bild_dd, bild_mm, bild_yr = parse_z11_mmddyyyy(nxbildt)
            if bild_dd:
                bilduedd = bild_dd; bilduemm = bild_mm; bildueyr = bild_yr

            _staffloan = {
                128, 130, 131, 132, 380, 381, 700, 705, 750, 720, 725, 752, 760,
                4, 5, 6, 7, 15, 20, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 60, 61,
                62, 63, 70, 71, 72, 73, 74, 75, 76, 77, 78, 100, 101, 108,
                199, 500, 520, 983, 993, 996,
            }
            if collyear is not None and safe_float(collyear, 0) != 0:
                cy_s = f"{int(collyear):05d}"
                if loantype in _staffloan or orgtype == "S":
                    collyr = cy_s[3:5]
                else:
                    collmm = cy_s[1:3]
                    collyr = cy_s[3:5]

            arrears = ndays_format(nodays)
            if arrears == 24:
                arrears = round((nodays / 365) * 12)
            instalm = billcnt
            # /*
            # IF FLAG1 IN ('F')      THEN UNDRAWN=0;
            # IF FLAG1 IN ('P','I')  THEN UNDRAWN=CAVAIAMT;
            # */

            # CLASSIFI
            if riskrate in (" ", ""):
                classifi = "P"
            elif riskrate == "1":
                classifi = "C"
            elif riskrate == "2":
                classifi = "S"
            elif riskrate == "3":
                classifi = "D"
            elif riskrate == "4":
                classifi = "B"
            if nplind == "P":
                classifi = "P"

            sector = sector.replace(" ", "")
            if not sector:
                sector = sectcd.strip()

            limtcurr = limtcurr * 100
            if limtcurr in (None, 0.0):
                limtcurr = amount

            if branch == 0:
                branch = ntbrch
            if branch == 0:
                branch = accbrch
            apcode = loantype
            if facgptype == "5":
                apcode = 0
            ficode  = branch
            interdue = interdue * 100
            realisab = realisab * 100

            # PAYFREQC from PAYFREQ (applied after all loantype overrides)
            _payfreq_map = {"1": "14", "2": "15", "3": "16", "4": "17",
                            "5": "18", "6": "13", "9": "21"}
            if payfreq in _payfreq_map:
                payfreqc = _payfreq_map[payfreq]

            if loantype in (984, 985, 994, 995):
                if noteno == 10010:
                    outstand = 0; acctstat = "S"
                    arrears = 999; instalm = 999; undrawn = 0
            if loantype in (981, 982, 983, 991, 992, 993):
                outstand = 0; arrears = 999; balance = 0
                instalm  = 999; undrawn = 0
                acctstat = "W"

            # BNM TAXANOMY & CCRIS ENHANCEMENT (2011-4096)
            syndicat   = format_synd(loantype)
            specialf   = format_fundsch(loantype)
            fconcept_v = format_fconcept(loantype)

            if loantype in (412, 413, 414, 415, 466):
                facility = 34322; faccode = 34320
            if loantype in (575, 416, 467, 461):
                facility = 34391; faccode = 34391
            if loantype in (307, 464, 465):
                facility = 34391; faccode = 34392
            if loantype in (103, 104):
                facility = 34371; faccode = 34371
            if loantype in (191, 417):
                facility = 34351; faccode = 34364

            if 600 <= loantype <= 699:
                if loantype in (678, 679):
                    syndicat   = "N"
                    specialf   = "00"
                    fconcept_v = 99
                else:
                    # *SYNDICAT = PUT(FLOOR(CENSUS),SYND.);
                    # *SPECIALF = PUT(FLOOR(CENSUS),FUNDSCH.);
                    fconcept_v = format_fconcept(math.floor(safe_float(census, 0)))
                if loantype in (610, 611, 668, 669):
                    facility = 34391; faccode = 34392
                if loantype in (670, 690):
                    facility = 34351; faccode = 34364

            if accstat == "W" and balance > 10:
                acctstat = "P"
            elif accstat == "P" and balance <= 2:
                acctstat = "W"

            # /*
            # MODELR=SUBSTR(MODELDES,1,1);
            # IF MODELR IN ('S','R') THEN DO;
            #    ACCTSTAT=MODELR;
            #    RESTRUCT=MODELR;
            # END;
            # */

        # -------- COMMON POST-PROCESSING --------
        if not curcode:
            curcode = "MYR"
        if curcode != "MYR":
            fo = get_forate(fx_rates, curcode)
            mthinstlmt = mthinstlmt * fo
            # PBB: ACCTNO 2500000000-2599999999
            if 2500000000 <= acctno <= 2599999999:
                undrawn = undrawn * fo

        if undrawn < 0:
            undrawn = 0.0

        iisdanah = 0; iistrans = 0; spcharge = 0

        if score and score[0].upper() not in ALP:
            score = "   "

        issued = mdy(
            safe_int(issuemm or 0),
            safe_int(issuedd or 0),
            safe_int(issueyy or 0),
        )

        # Scale financial fields *100
        undrawn    = undrawn    * 100
        spaamt     = spaamt     * 100
        intrate    = intrate    * 100
        curbal_out = curbal     * 100
        feeamt     = feeamt     * 100
        totiis     = totiis     * 100
        totiisr    = totiisr    * 100
        totwof     = totwof     * 100
        iisopbal   = iisopbal   * 100
        iissuamt   = iissuamt   * 100
        iisbwamt   = iisbwamt   * 100
        spwbamt    = spwbamt    * 100
        spwof      = spwof      * 100
        spopbal    = spopbal    * 100
        spdanah    = spdanah    * 100
        sptrans    = sptrans    * 100
        mthinstlmt = mthinstlmt * 100

        # RESTRUCT / ACCTSTAT overrides for HP restructured
        if loantype in (128, 130, 380, 381, 700, 705, 720, 725) and noteno >= 98000:
            if issued:
                d_30sep05 = date(2005, 9, 30)
                d_19dec07 = date(2007, 12, 19)
                d_31aug08 = date(2008, 8, 31)
                d_01sep08 = date(2008, 9, 1)
                d_01oct05 = date(2005, 10, 1)
                d_18dec07 = date(2007, 12, 18)
                if issued <= d_30sep05 or d_19dec07 <= issued <= d_31aug08:
                    restruct = "T"
                elif issued >= d_01sep08 or d_01oct05 <= issued <= d_18dec07:
                    restruct = "C"
                if restruct == "C":
                    acctstat = "C"
                elif restruct == "T":
                    acctstat = "T"

        if loantype in (128, 130, 380, 381, 700, 705, 720, 725):
            if noteno < 98000:
                prefix4 = modeldes[:4] if len(modeldes) >= 4 else ""
                prefix5 = modeldes[:5] if len(modeldes) >= 5 else ""
                if prefix4 in ("CAM4", "CAM5", "CAM6", "CAM7", "CAM8", "CAM9") or \
                        prefix5 == "CAM10":
                    restruct = "C"; acctstat = "C"

        # * IF AANO NE ' ' THEN NOTENO = FACILITY;
        if 800 <= apcode <= 899 and 2000000000 <= acctno <= 2999999999:
            ficode = 904
        if 800 <= apcode <= 899 and 8000000000 <= acctno <= 8999999999:
            ficode = 904
        if 800 <= loantype <= 899:
            costctr = 904

        # AADATE formatted
        aadate_dt = sas_to_date(aadate_v)
        aadatedd = f"{aadate_dt.day:02d}"   if aadate_dt else "00"
        aadatemm = f"{aadate_dt.month:02d}" if aadate_dt else "00"
        aadateyy = f"{aadate_dt.year:04d}"  if aadate_dt else "0000"

        rec = {
            "ACCTNO": acctno, "NOTENO": noteno, "AANO": aano,
            "BRANCH": branch, "FICODE": ficode, "APCODE": apcode,
            "COMMNO": commno, "IND": ind, "PAIDIND": paidind,
            "ACCTSTAT": acctstat, "CLASSIFI": classifi, "GP3IND": gp3ind,
            "LEG": leg, "LOANTYPE": loantype, "PRODUCT": product,
            "ACTY": acty, "FACILITY": facility, "FACCODE": faccode,
            "OUTSTAND": outstand, "ARREARS": arrears, "INSTALM": instalm,
            "UNDRAWN": undrawn, "NODAYS": nodays, "BILTOT": biltot,
            "ODXSAMT": odxsamt, "LIMTCURR": limtcurr, "LMTAMT": lmtamt,
            "CURBAL": curbal_out, "INTERDUE": interdue, "FEEAMT": feeamt,
            "REALISAB": realisab, "IISOPBAL": iisopbal, "TOTIIS": totiis,
            "TOTIISR": totiisr, "TOTWOF": totwof, "IISDANAH": iisdanah,
            "IISTRANS": iistrans, "SPOPBAL": spopbal, "SPCHARGE": spcharge,
            "SPWBAMT": spwbamt, "SPWOF": spwof, "SPDANAH": spdanah,
            "SPTRANS": sptrans, "SPAAMT": spaamt, "INTRATE": intrate,
            "MTHINSTLMT": mthinstlmt, "COLLVAL": collval, "CAGAMAS": cagamas,
            "APPRLMAA": apprlmaa, "CAVAIAMT": cavaiamt, "AMOUNT": amount,
            "SYNDICAT": syndicat, "SPECIALF": specialf, "FCONCEPT": fconcept_v,
            "NOTETERM": noteterm, "PAYFREQC": payfreqc, "PURPOSES": purposes,
            "RESTRUCT": restruct, "RECOURSE": recourse, "SECTOR": sector,
            "OLDBRH": oldbrh, "SCORE": score, "COSTCTR": costctr,
            "LIABCODE": liabcode, "CUSTCODE": custcode, "CENSUS": census,
            "COLLMAKE": collmake, "MODELDES": modeldes, "ORGTYPE": orgtype,
            "PAYFREQ": payfreq, "CURCODE": curcode, "FACGPTYPE": facgptype,
            "PAYDD": paydd, "PAYMM": paymm, "PAYYR": payyr,
            "GRANTDD": grantdd, "GRANTMM": grantmm, "GRANTYR": grantyr,
            "LNMATDD": lnmatdd, "LNMATMM": lnmatmm, "LNMATYR": lnmatyr,
            "ORMATDD": ormatdd, "ORMATMM": ormatmm, "ORMATYR": ormatyr,
            "DLVRDD": dlvrdd, "DLVRMM": dlvrmm, "DLVRYR": dlvryr,
            "BILDUEDD": bilduedd, "BILDUEMM": bilduemm, "BILDUEYR": bildueyr,
            "COLLMM": collmm, "COLLYR": collyr,
            "ISSUEDD": issuedd, "ISSUEMM": issuemm, "ISSUEYY": issueyy,
            "AADATE": aadate_v, "AADATEDD": aadatedd,
            "AADATEMM": aadatemm, "AADATEYY": aadateyy,
            "ISSUED": issued, "COMPLIDT": complidt, "RJDATE": rjdate,
            "RECONAME": reconame, "REFTYPE": reftype, "NMREF1": nmref1,
            "NMREF2": nmref2, "APVNME1": apvnme1, "APVNME2": apvnme2,
            "NEWIC": newic, "DNBFISME": dnbfisme, "REASON": reason,
            "STRUPCO_3YR": "", "REFIN_FLG": refin_flg, "OLD_FI_CD": old_fi_cd,
            "OLD_MASTACC": old_mastacc, "OLD_SUBACC": old_subacc,
            "REFIN_AMT": refin_amt, "DSR": 0.0, "LN_UTILISE_LOCAT_CD": "",
            "LN_UTILISE_LOCAT_CDX": "", "PRIORITY_SECTOR": "",
            "EIR": 0.0, "CIR": 0.0, "CCPT_TAG": "",
            "LU_ADD1": lu_add1, "LU_ADD2": lu_add2,
            "LU_ADD3": lu_add3, "LU_ADD4": lu_add4,
            "LU_TOWN_CITY": lu_town, "LU_POSTCODE": lu_postcode,
            "LU_STATE_CD": lu_state, "LU_COUNTRY_CD": lu_country,
            "LU_SOURCE": lu_source,
            "PRODESC": prodesc, "APRVDT": aprvdt,
            "PRITYPE": pritype, "INDEXCD": indexcd,
            "PRIRATE": prirate, "PRICING": pricing,
            "ICDATE": icdate_v, "BLRATE_STR": blrate,
            "TYPEPRC": "", "AANO_X": "",
        }
        acctcred_records.append(rec)

    # ---------------------------------------------------------------------------
    # MERGE SUMM1 INTO ACCTCRED
    # ---------------------------------------------------------------------------
    for rec in acctcred_records:
        aano     = rec["AANO"]
        aano_x   = (aano[11:13] + aano[4:10]) if len(aano) >= 13 else ""
        rec["AANO_X"] = aano_x

        s        = summ1_map.get(safe_str(aano), {})
        rec["CIR"] = safe_float(s.get("CIR", 0)) * 100
        rec["EIR"] = safe_float(s.get("EIR", 0)) * 100

        acctno   = rec["ACCTNO"]
        prodesc  = rec.get("PRODESC", "")

        # SMR 2019-4532: FEFCL products
        if "FEFCL" in prodesc.upper():
            facicode = safe_str(s.get("FACICODE", ""))
            fefcl_map = {
                "0091000304": 201, "0091000314": 201,
                "0091000305": 202, "0091000315": 202,
                "0091000306": 204, "0091000316": 204,
                "0091000307": 203, "0091000317": 203,
                "0091000308": 205, "0091000318": 205,
                "0091000309": 206, "0091000319": 206,
            }
            if facicode in fefcl_map:
                rec["LOANTYPE"] = fefcl_map[facicode]
            rec["LN_UTILISE_LOCAT_CD"] = safe_str(s.get("LN_UTILISE_LOCAT_CD", ""))
            rec["STRUPCO_3YR"] = safe_str(s.get("STRUPCO_3YR", ""))
            rec["APRVDT"]      = safe_str(s.get("DTECOMPLETE", ""))
            rec["UNDRAWN"]     = safe_float(s.get("AMOUNT", 0)) * 100
            rec["FACILITY"]    = safe_int(s.get("FACICODE", 0))
            rec["APCODE"]      = rec["LOANTYPE"]
            rec["FACCODE"]     = rec["FACILITY"]
            rec["SYNDICAT"]    = "N"
            rec["PURPOSES"]    = safe_str(s.get("PURPOSE_LOAN", ""))
            rec["FCONCEPT"]    = safe_int(s.get("FIN_CONCEPT", 0))
            rec["NOTETERM"]    = 12
            rec["PAYFREQC"]    = "19"  # REVOLVING CREDIT
            sf = safe_str(s.get("SPECIALFUND", ""))
            rec["SPECIALF"]    = sf if sf not in ("", "0", None) else "00"

        elif 2500000000 <= acctno <= 2599999999:
            dtec = safe_str(s.get("DTECOMPLETE", ""))
            if len(dtec) >= 10:
                try:
                    aadate_new = mdy(int(dtec[3:5]), int(dtec[0:2]), int(dtec[6:10]))
                    if aadate_new:
                        rec["AADATE"] = date_to_sas(aadate_new)
                except Exception:
                    pass
            rec["FACILITY"] = 34480
            rec["FACCODE"]  = 34480
            rec["APCODE"]   = 888
            sf = safe_str(s.get("SPECIALFUND", ""))
            rec["SPECIALF"] = sf if sf not in ("", "0", None) else "00"
            rec["PURPOSES"] = safe_str(s.get("PURPOSE_LOAN", ""))
            if rec["PURPOSES"] not in ("4500", "5300"):
                rec["PURPOSES"] = "5300"
            rec["FCONCEPT"] = 99
            int_rate = safe_float(s.get("PRICING", 0))
            if int_rate <= 6.72:
                int_rate = 6.72
            elif int_rate > 10.22:
                int_rate = 10.22
            rec["INTRATE"]     = int_rate * 100
            rec["STRUPCO_3YR"] = safe_str(s.get("STRUPCO_3YR", ""))
            rec["DSR"]         = safe_float(s.get("DSRISS3", 0))

        else:
            ap = safe_float(s.get("ASSET_PURCH_AMT", 0))
            rec["SPAAMT"]              = (ap * 100) if ap else 0.0
            rec["LN_UTILISE_LOCAT_CD"] = safe_str(s.get("LN_UTILISE_LOCAT_CD", ""))
            rec["STRUPCO_3YR"]         = safe_str(s.get("STRUPCO_3YR", ""))
            rec["DSR"]                 = safe_float(s.get("DSRISS3", 0))
            cc = rec.get("CURCODE", "MYR") or "MYR"
            amt = safe_float(s.get("AMOUNT", 0))
            if cc in ("", "MYR") and amt not in (None, 0.0):
                rec["APPRLMAA"] = amt
            if rec.get("FACILITY") == 34460:
                rec["APRVDT"]   = safe_str(s.get("DTECOMPLETE", ""))
                rec["NOTETERM"] = 12
                rec["PAYFREQC"] = "18"
                if safe_str(s.get("APPTYPE", "")) == "I":
                    rec["_DELETE"] = True  # LIMIT INCREASE

        # BLRATE selection
        aadate_v2 = safe_int(rec.get("AADATE", 0))
        rec["BLRATE_STR"] = blrate if aadate_v2 >= blreff_sas else olrate

        # TYPEPRC
        rec["TYPEPRC"] = compute_typeprc(
            acctno       = acctno,
            curcode      = rec.get("CURCODE", "MYR"),
            facgptype    = rec.get("FACGPTYPE", ""),
            pritype      = rec.get("PRITYPE", ""),
            product      = safe_int(rec.get("PRODUCT", 0)),
            pricing      = safe_float(rec.get("PRICING", 0)),
            prirate      = safe_float(rec.get("PRIRATE", 0)),
            blrate       = rec.get("BLRATE_STR", blrate),
            indexcd      = safe_int(rec.get("INDEXCD", 0)),
        )

    # Remove deleted
    acctcred_records = [r for r in acctcred_records if not r.get("_DELETE")]

    # PROC SORT DATA=ACCTCRED; BY ACCTNO NOTENO AANO_X;
    acctcred_records.sort(key=lambda r: (
        safe_int(r.get("ACCTNO", 0)),
        safe_int(r.get("NOTENO", 0)),
        safe_str(r.get("AANO_X", "")),
    ))

    # ---------------------------------------------------------------------------
    # MERGE WRIOFAC
    # ---------------------------------------------------------------------------
    for rec in acctcred_records:
        key = (safe_int(rec.get("ACCTNO", 0)), safe_int(rec.get("NOTENO", 0)))
        wf  = wriofac.get(key, {})
        if wf:
            rec["RESTRUCT"] = wf.get("RESTRUCT", rec.get("RESTRUCT", ""))
            if wf.get("ACCTSTAT"):
                rec["ACCTSTAT"] = wf["ACCTSTAT"]

        # COMPLIDT
        complidt = rec.get("COMPLIDT")
        if complidt is not None and safe_float(complidt, 0) != 0:
            cd = sas_to_date(int(complidt))
            rec["COMPLIDD"] = f"{cd.day:02d}"   if cd else "00"
            rec["COMPLIMM"] = f"{cd.month:02d}" if cd else "00"
            rec["COMPLIYY"] = f"{cd.year:04d}"  if cd else "0000"
        else:
            rec["COMPLIDD"] = "00"; rec["COMPLIMM"] = "00"; rec["COMPLIYY"] = "0000"

        # AKPK override
        if rec.get("LOANTYPE", 0) in (128, 130, 380, 381, 700, 705, 720, 725):
            if rec.get("MODELDES", "") in ("EAKPK", "AKPK") and rec.get("ACCTSTAT") != "S":
                rec["ACCTSTAT"] = "K"

        # Delete if AADATE same month/year as ICDATE
        aadate_v3 = safe_int(rec.get("AADATE", 0))
        icdate_v3 = safe_int(rec.get("ICDATE", 0))
        if aadate_v3 and icdate_v3:
            aa_dt = sas_to_date(aadate_v3)
            ic_dt = sas_to_date(icdate_v3)
            if aa_dt and ic_dt and aa_dt.month == ic_dt.month and aa_dt.year == ic_dt.year:
                rec["_DELETE"] = True

        # RJDATE
        rjdate_v = rec.get("RJDATE")
        if rjdate_v is not None and safe_float(rjdate_v, 0) != 0:
            rj_dt = sas_to_date(int(rjdate_v))
            rec["RJDATEDD"] = f"{rj_dt.day:02d}"   if rj_dt else "00"
            rec["RJDATEMM"] = f"{rj_dt.month:02d}" if rj_dt else "00"
            rec["RJDATEYY"] = f"{rj_dt.year:04d}"  if rj_dt else "0000"
        else:
            rec["RJDATEDD"] = "00"; rec["RJDATEMM"] = "00"; rec["RJDATEYY"] = "0000"

        # ESMR 2025-1525: LU_SOURCE normalisation
        lu_src = safe_str(rec.get("LU_SOURCE", ""))
        if lu_src and lu_src[0] == "A":
            rec["LU_SOURCE"] = lu_src if lu_src in ("A1", "A2") else "A2"
        elif len(lu_src) >= 2 and lu_src[1] == "P":
            rec["LU_SOURCE"] = lu_src[:2]

    acctcred_records = [r for r in acctcred_records if not r.get("_DELETE")]

    # ---------------------------------------------------------------------------
    # DATA CREDIT: derive ISSUED, AADATE parts
    # DATA LNACC4 merge by ACCTNO
    # PROC SORT DATA=CREDIT; BY BRANCH ACCTNO AANO COMMNO NOTENO;
    # Dedup + filter
    # PROC SUMMARY: SUM LIMTCURR by BRANCH ACCTNO AANO
    # CREDIT1: first per BRANCH ACCTNO AANO
    # ---------------------------------------------------------------------------

    # Sort by BRANCH ACCTNO AANO COMMNO NOTENO
    acctcred_records.sort(key=lambda r: (
        safe_int(r.get("BRANCH", 0)),
        safe_int(r.get("ACCTNO", 0)),
        safe_str(r.get("AANO", "")),
        safe_int(r.get("COMMNO", 0)),
        safe_int(r.get("NOTENO", 0)),
    ))

    credit_records: list[dict] = []
    seen_keys: set = set()
    for rec in acctcred_records:
        key = (
            safe_int(rec.get("BRANCH", 0)),
            safe_int(rec.get("ACCTNO", 0)),
            safe_str(rec.get("AANO", "")),
            safe_int(rec.get("COMMNO", 0)),
            safe_int(rec.get("NOTENO", 0)),
        )
        if key not in seen_keys:
            seen_keys.add(key)
            acty   = rec.get("ACTY", "LN")
            commno = safe_int(rec.get("COMMNO", 0))
            ind    = safe_str(rec.get("IND", ""))
            aano   = safe_str(rec.get("AANO", ""))
            paidind = safe_str(rec.get("PAIDIND", ""))
            if acty == "OD":
                credit_records.append(rec)
            elif ((commno == 0 and ind == "A") or
                  (commno > 0 and ind == "B") or aano != ""):
                if paidind != "P":
                    credit_records.append(rec)

    # PROC SUMMARY: SUM LIMTCURR by BRANCH ACCTNO AANO
    limtcurr_sum: dict = defaultdict(float)
    for rec in credit_records:
        k = (
            safe_int(rec.get("BRANCH", 0)),
            safe_int(rec.get("ACCTNO", 0)),
            safe_str(rec.get("AANO", "")),
        )
        limtcurr_sum[k] += safe_float(rec.get("LIMTCURR", 0))

    # Merge LNACC4 by ACCTNO; IF CURCODE NE 'MYR' THEN FXRATE = FORATE*100000
    for rec in credit_records:
        acctno = safe_int(rec.get("ACCTNO", 0))
        lna    = lnacc4_map.get(acctno, {})
        rec.update({k: v for k, v in lna.items() if k != "ACCTNO"})
        curcode = rec.get("CURCODE", "MYR") or "MYR"
        if curcode != "MYR":
            fo = get_forate(fx_rates, curcode)
            rec["FXRATE"] = int(fo * 100000)
        else:
            rec["FXRATE"] = 0

    # %MACRO MONTHLY;
    #    %IF "&NOWK" NE "4" %THEN %DO;
    #         DATA CCRIS.COLLATER;
    #              SET CREDIT;
    #         DATA CCRIS.WRITOFF;
    #              KEEP ACCTNO NOTENO ACCTSTAT RESTRUCT;
    #              SET CREDIT;
    #              IF  ACCTSTAT='W';
    #    %END;
    # %MEND MONTHLY;
    # /*
    # %MONTHLY;
    # */

    # CREDIT1: first per BRANCH ACCTNO AANO
    credit1_records: list[dict] = []
    seen2: set = set()
    for rec in credit_records:
        k = (
            safe_int(rec.get("BRANCH", 0)),
            safe_int(rec.get("ACCTNO", 0)),
            safe_str(rec.get("AANO", "")),
        )
        if k not in seen2:
            seen2.add(k)
            credit1_records.append(rec)

    # ---------------------------------------------------------------------------
    # WRITE OUTPUT FILES
    # ---------------------------------------------------------------------------
    subacred_lines: list[str] = []
    creditpo_lines: list[str] = []
    provisio_lines: list[str] = []
    legalact_lines: list[str] = []
    acctcred_lines: list[str] = []

    # SUBACRED / CREDITPO / PROVISIO / LEGALACT written per credit_records
    for rec in credit_records:
        acctno = safe_int(rec.get("ACCTNO", 0))
        noteno = safe_int(rec.get("NOTENO", 0))
        aadate_dt4 = sas_to_date(safe_int(rec.get("AADATE", 0)))
        aadate_str4 = fmt_ddmmyy8(aadate_dt4)

        # ---- SUBACRED (LRECL=950) ----
        sub = build_record([
            (1,    fmt_n(rec.get("FICODE", 0), 9)),
            (10,   fmt_n(rec.get("APCODE", 0), 3)),
            (13,   fmt_n(acctno, 10)),
            (43,   fmt_n(noteno, 10)),
            (73,   fmt_n(rec.get("FACILITY", 0), 5)),
            (78,   fmt_s(rec.get("SYNDICAT", "N"), 1)),
            (79,   fmt_s(rec.get("SPECIALF", "00"), 2)),
            (81,   fmt_s(rec.get("PURPOSES", ""), 4)),
            (85,   fmt_n(rec.get("FCONCEPT", 0), 2)),
            (87,   fmt_z(rec.get("NOTETERM", 0), 3)),
            (90,   fmt_s(rec.get("PAYFREQC", ""), 2)),
            (92,   fmt_s(rec.get("RESTRUCT", ""), 1)),
            (93,   fmt_n(rec.get("CAGAMAS", 0), 8)),
            (101,  fmt_s(rec.get("RECOURSE", ""), 1)),
            (102,  "00000000"),
            (110,  fmt_n(rec.get("CUSTCODE", 0), 2)),
            (112,  fmt_s(rec.get("SECTOR", ""), 4)),
            (116,  fmt_n(rec.get("OLDBRH", 0), 5)),
            (121,  fmt_s(rec.get("SCORE", ""), 3)),
            (124,  fmt_n(rec.get("COSTCTR", 0), 4)),
            (128,  fmt_s(rec.get("PAYDD", "00"), 2)),
            (130,  fmt_s(rec.get("PAYMM", "00"), 2)),
            (132,  fmt_s(rec.get("PAYYR", "0000"), 4)),
            (136,  fmt_s(rec.get("GRANTDD", "00"), 2)),
            (138,  fmt_s(rec.get("GRANTMM", "00"), 2)),
            (140,  fmt_s(rec.get("GRANTYR", "0000"), 4)),
            (144,  fmt_z(int(safe_float(rec.get("CENSUS", 0))), 6)),
            (150,  fmt_s(rec.get("LNMATDD", "00"), 2)),
            (152,  fmt_s(rec.get("LNMATMM", "00"), 2)),
            (154,  fmt_s(rec.get("LNMATYR", "0000"), 4)),
            (158,  fmt_s(rec.get("ORMATDD", "00"), 2)),
            (160,  fmt_s(rec.get("ORMATMM", "00"), 2)),
            (162,  fmt_s(rec.get("ORMATYR", "0000"), 4)),
            (166,  fmt_n(int(safe_float(rec.get("CAVAIAMT", 0))), 16)),
            (182,  fmt_s(rec.get("COLLMAKE", ""), 6)),
            (188,  fmt_s(rec.get("MODELDES", ""), 6)),
            (194,  fmt_s(rec.get("DLVRDD", "00"), 2)),
            (196,  fmt_s(rec.get("DLVRMM", "00"), 2)),
            (198,  fmt_s(rec.get("DLVRYR", "0000"), 4)),
            (202,  fmt_s(rec.get("COLLMM", "00"), 2)),
            (204,  fmt_s(rec.get("COLLYR", "00"), 2)),
            (206,  fmt_s(rec.get("BILDUEDD", "00"), 2)),
            (208,  fmt_s(rec.get("BILDUEMM", "00"), 2)),
            (210,  fmt_s(rec.get("BILDUEYR", "0000"), 4)),
            (214,  fmt_s(rec.get("PAYFREQ", ""), 1)),
            (215,  fmt_s(rec.get("ORGTYPE", ""), 1)),
            (216,  fmt_s(rec.get("PAIDIND", ""), 1)),
            (217,  fmt_s(rec.get("AANO", ""), 13)),
            (231,  aadate_str4),
            (239,  fmt_s(rec.get("FACGPTYPE", ""), 1)),
            (240,  fmt_n(rec.get("FACCODE", 0), 5)),
            (245,  fmt_n(int(safe_float(rec.get("CIR", 0))), 5)),
            (250,  fmt_s(rec.get("TYPEPRC", ""), 2)),
            (252,  fmt_z(int(safe_float(rec.get("SPAAMT", 0))), 16)),
            (269,  fmt_s(rec.get("DNBFISME", ""), 1)),
            (271,  fmt_s(rec.get("RJDATEYY", "0000"), 4)),
            (275,  fmt_s(rec.get("RJDATEMM", "00"), 2)),
            (277,  fmt_s(rec.get("RJDATEDD", "00"), 2)),
            (280,  fmt_s(rec.get("REASON", ""), 294)),
            (574,  fmt_s(rec.get("STRUPCO_3YR", ""), 5)),       # 2018-1432 & 2018-1442
            (579,  fmt_s(rec.get("REFIN_FLG", ""), 3)),         # 2018-1432 & 2018-1442
            (582,  fmt_s(rec.get("OLD_FI_CD", ""), 10)),        # 2018-1432 & 2018-1442
            (592,  fmt_s(rec.get("OLD_MASTACC", ""), 30)),      # 2018-1432 & 2018-1442
            (622,  fmt_s(rec.get("OLD_SUBACC", ""), 30)),       # 2018-1432 & 2018-1442
            (652,  fmt_f(safe_float(rec.get("REFIN_AMT", 0)), 20, 2)),  # 2018-1432 & 2018-1442
            (672,  fmt_f(safe_float(rec.get("DSR", 0)), 6, 2)),         # 2018-1432 & 2018-1442
            (678,  fmt_s(rec.get("LN_UTILISE_LOCAT_CD", ""), 5)),       # 2018-1432 & 2018-1442
            (683,  fmt_s(rec.get("PRIORITY_SECTOR", ""), 2)),            # 2020-296
            (685,  fmt_s(rec.get("LN_UTILISE_LOCAT_CDX", ""), 5)),      # 2020-296
            (690,  fmt_n(int(safe_float(rec.get("EIR", 0))), 10)),      # 2020-296
            (700,  fmt_s(rec.get("CURCODE", ""), 3)),                     # 2021-2603
            (703,  fmt_s(rec.get("CCPT_TAG", ""), 5)),                   # 2023-3235
            (710,  fmt_s(rec.get("LU_ADD1", ""), 40)),                   # 2023-3931
            (750,  fmt_s(rec.get("LU_ADD2", ""), 40)),
            (790,  fmt_s(rec.get("LU_ADD3", ""), 40)),
            (830,  fmt_s(rec.get("LU_ADD4", ""), 40)),
            (870,  fmt_s(rec.get("LU_TOWN_CITY", ""), 20)),
            (890,  fmt_s(rec.get("LU_POSTCODE", ""), 5)),
            (895,  fmt_s(rec.get("LU_STATE_CD", ""), 2)),
            (897,  fmt_s(rec.get("LU_COUNTRY_CD", ""), 2)),
            (899,  fmt_s(rec.get("LU_SOURCE", ""), 5)),
        ])
        subacred_lines.append(sub.ljust(950)[:950])

        # ---- CREDITPO (LRECL=400) ----
        cp = build_record([
            (1,    fmt_n(rec.get("FICODE", 0), 9)),
            (10,   fmt_n(rec.get("APCODE", 0), 3)),
            (13,   fmt_n(acctno, 10)),
            (43,   fmt_n(noteno, 10)),
            (73,   reptday),
            (75,   reptmon),
            (77,   reptyear),
            (81,   fmt_z(int(safe_float(rec.get("OUTSTAND", 0))), 16)),
            (97,   fmt_z(safe_int(rec.get("ARREARS", 0)), 3)),
            (100,  fmt_z(safe_int(rec.get("INSTALM", 0)), 3)),
            (103,  fmt_z(int(safe_float(rec.get("UNDRAWN", 0))), 17)),
            (120,  fmt_s(rec.get("ACCTSTAT", "O"), 1)),
            (121,  fmt_z(safe_int(rec.get("NODAYS", 0)), 5)),
            (126,  fmt_n(rec.get("OLDBRH", 0), 5)),
            (131,  fmt_z(int(safe_float(rec.get("BILTOT", 0))), 17)),
            (148,  fmt_z(int(safe_float(rec.get("ODXSAMT", 0))), 17)),
            (165,  fmt_n(rec.get("COSTCTR", 0), 4)),
            (169,  fmt_s(rec.get("AANO", ""), 13)),
            (182,  fmt_n(rec.get("FACILITY", 0), 5)),
            (187,  fmt_s(rec.get("COMPLIBY", ""), 60)),
            (247,  fmt_s(rec.get("COMPLIYY", "0000"), 4)),
            (251,  fmt_s(rec.get("COMPLIMM", "00"), 2)),
            (253,  fmt_s(rec.get("COMPLIDD", "00"), 2)),
            (255,  fmt_s(rec.get("COMPLIGR", ""), 15)),
            (270,  fmt_n(rec.get("FACCODE", 0), 5)),
            (276,  fmt_n(int(safe_float(rec.get("MTHINSTLMT", 0))), 16)),
            (292,  fmt_s(rec.get("CURCODE", ""), 3)),
        ])
        creditpo_lines.append(cp.ljust(400)[:400])

        # ---- PROVISIO (LRECL=400) ----
        classifi = rec.get("CLASSIFI", "")
        lt       = safe_int(rec.get("LOANTYPE", 0))
        if (classifi in ("S", "D", "B") or rec.get("GP3IND", "") != "") and lt not in HP:
            pv = build_record([
                (1,    fmt_n(rec.get("FICODE", 0), 9)),
                (10,   fmt_n(rec.get("APCODE", 0), 3)),
                (13,   fmt_n(acctno, 10)),
                (43,   fmt_n(noteno, 10)),
                (73,   reptday),
                (75,   reptmon),
                (77,   reptyear),
                (81,   fmt_s(classifi, 1)),
                (82,   fmt_z(safe_int(rec.get("ARREARS", 0)), 3)),
                (85,   fmt_z(int(safe_float(rec.get("CURBAL", 0))), 17)),
                (102,  fmt_z(int(safe_float(rec.get("INTERDUE", 0))), 17)),
                (119,  fmt_z(int(safe_float(rec.get("FEEAMT", 0))), 16)),
                (135,  fmt_z(int(safe_float(rec.get("REALISAB", 0))), 17)),
                (152,  fmt_z(int(safe_float(rec.get("IISOPBAL", 0))), 17)),
                (169,  fmt_z(int(safe_float(rec.get("TOTIIS", 0))), 17)),
                (186,  fmt_z(int(safe_float(rec.get("TOTIISR", 0))), 17)),
                (203,  fmt_z(int(safe_float(rec.get("TOTWOF", 0))), 17)),
                (220,  fmt_z(0, 17)),  # IISDANAH
                (237,  fmt_z(0, 17)),  # IISTRANS
                (254,  fmt_z(int(safe_float(rec.get("SPOPBAL", 0))), 17)),
                (271,  fmt_z(0, 17)),  # SPCHARGE
                (288,  fmt_z(int(safe_float(rec.get("SPWBAMT", 0))), 17)),
                (305,  fmt_z(int(safe_float(rec.get("SPWOF", 0))), 17)),
                (322,  fmt_z(int(safe_float(rec.get("SPDANAH", 0))), 17)),
                (339,  fmt_z(int(safe_float(rec.get("SPTRANS", 0))), 17)),
                (356,  fmt_s(rec.get("GP3IND", ""), 1)),
                (357,  fmt_n(rec.get("OLDBRH", 0), 5)),
                (362,  fmt_n(rec.get("COSTCTR", 0), 5)),
                (367,  fmt_s(rec.get("AANO", ""), 13)),
                (380,  fmt_n(rec.get("FACILITY", 0), 5)),
                (385,  fmt_n(rec.get("FACCODE", 0), 5)),
            ])
            provisio_lines.append(pv.ljust(400)[:400])

        # ---- LEGALACT (LRECL=100) ----
        if rec.get("LEG", "") == "A":
            la = build_record([
                (1,   fmt_n(rec.get("FICODE", 0), 9)),
                (10,  fmt_n(rec.get("APCODE", 0), 3)),
                (13,  fmt_n(acctno, 10)),
                (43,  fmt_s(rec.get("DELQCD", ""), 2)),
                (45,  reptday),
                (47,  reptmon),
                (49,  reptyear),
                (53,  fmt_n(rec.get("OLDBRH", 0), 5)),
                (58,  fmt_n(rec.get("COSTCTR", 0), 4)),
                (62,  fmt_s(rec.get("AANO", ""), 13)),
                (75,  fmt_n(rec.get("FACILITY", 0), 5)),
                (80,  fmt_n(rec.get("FACCODE", 0), 5)),
            ])
            legalact_lines.append(la.ljust(100)[:100])

    # ---- ACCTCRED (LRECL=900) written from CREDIT1 x ACCTCD ----
    for rec in credit1_records:
        acctno = safe_int(rec.get("ACCTNO", 0))
        k = (
            safe_int(rec.get("BRANCH", 0)),
            acctno,
            safe_str(rec.get("AANO", "")),
        )
        limtcurr_tot = limtcurr_sum.get(k, safe_float(rec.get("LIMTCURR", 0)))
        aadate_dt5 = sas_to_date(safe_int(rec.get("AADATE", 0)))
        aadate_str5 = fmt_ddmmyy8(aadate_dt5)

        ac = build_record([
            (1,    fmt_n(rec.get("FICODE", 0), 9)),
            (10,   fmt_n(rec.get("APCODE", 0), 3)),
            (13,   fmt_n(acctno, 10)),
            (43,   fmt_s(rec.get("CURCODE", ""), 3)),
            (46,   fmt_z(int(limtcurr_tot), 24)),
            (70,   fmt_z(int(safe_float(rec.get("LIMTCURR", 0))), 16)),
            (86,   fmt_s(rec.get("ISSUEDD", ""), 2)),
            (88,   fmt_s(rec.get("ISSUEMM", ""), 2)),
            (90,   fmt_s(rec.get("ISSUEYY", ""), 4)),
            (94,   fmt_z(rec.get("OLDBRH", 0), 5)),
            (99,   fmt_z(int(safe_float(rec.get("LMTAMT", 0))), 16)),
            (115,  fmt_s(rec.get("LIABCODE", ""), 2)),
            (117,  fmt_n(rec.get("COSTCTR", 0), 4)),
            (121,  fmt_s(rec.get("AANO", ""), 13)),
            (134,  fmt_f(safe_float(rec.get("APPRLMAA", 0)), 15, 0)),
            (149,  fmt_s(rec.get("AADATEYY", ""), 4)),
            (153,  fmt_s(rec.get("AADATEMM", ""), 2)),
            (155,  fmt_s(rec.get("AADATEDD", ""), 2)),
            (157,  fmt_s(rec.get("RECONAME", ""), 60)),
            (217,  fmt_s(rec.get("REFTYPE", ""), 70)),
            (287,  fmt_s(rec.get("NMREF1", ""), 60)),
            (347,  fmt_s(rec.get("NMREF2", ""), 60)),
            (407,  fmt_s(rec.get("APVNME1", ""), 60)),
            (467,  fmt_s(rec.get("APVNME2", ""), 60)),
            (527,  fmt_n(rec.get("FACILITY", 0), 5)),
            (532,  fmt_s(rec.get("NEWIC", ""), 20)),
            (553,  aadate_str5),
            (561,  fmt_s(rec.get("FACGPTYPE", ""), 1)),
            (563,  fmt_n(rec.get("FACCODE", 0), 5)),
            (568,  fmt_s(rec.get("ACCT_AANO", ""), 13)),
            (581,  fmt_n(rec.get("ACCT_LNTYPE", 0), 9)),
            (590,  fmt_n(rec.get("ACCT_LNTERM", 0), 10)),
            (600,  fmt_z(rec.get("ACCT_OUTBAL", 0), 16)),
            (616,  fmt_z(rec.get("ACCT_APLMT", 0), 16)),
            (632,  fmt_s(rec.get("ACCT_ALDD", ""), 2)),
            (634,  fmt_s(rec.get("ACCT_ALMM", ""), 2)),
            (636,  fmt_s(rec.get("ACCT_ALYY", ""), 4)),
            (640,  fmt_s(rec.get("ACCT_REVDD", ""), 2)),
            (642,  fmt_s(rec.get("ACCT_REVMM", ""), 2)),
            (644,  fmt_s(rec.get("ACCT_REVYY", ""), 4)),
            (648,  fmt_s(rec.get("ACCT_1DISDD", ""), 2)),
            (650,  fmt_s(rec.get("ACCT_1DISMM", ""), 2)),
            (652,  fmt_s(rec.get("ACCT_1DISYY", ""), 4)),
            (656,  fmt_s(rec.get("ACCT_SETDD", ""), 2)),
            (658,  fmt_s(rec.get("ACCT_SETMM", ""), 2)),
            (660,  fmt_s(rec.get("ACCT_SETYY", ""), 4)),
            (664,  fmt_s(rec.get("ACCT_EXPDD", ""), 2)),
            (666,  fmt_s(rec.get("ACCT_EXPMM", ""), 2)),
            (668,  fmt_s(rec.get("ACCT_EXPYY", ""), 4)),
            (672,  fmt_z(safe_int(rec.get("FXRATE", 0)), 8)),
            (680,  fmt_s(rec.get("APRVDT", ""), 10)),
            (690,  fmt_z(int(safe_float(rec.get("UNDRAWN", 0))), 17)),
        ])
        acctcred_lines.append(ac.ljust(900)[:900])

    # ---- ACCTREJT (LRECL=200) ----
    acctrejt_lines: list[str] = []
    for rec in lnhprj_rows:
        icdate_v5 = safe_int(rec.get("ICDATE", 0))
        ic_dt5    = sas_to_date(icdate_v5) if icdate_v5 else None
        ar = build_record([
            (1,   fmt_n(safe_int(rec.get("BRANCH", 0)), 9)),
            (11,  fmt_n(safe_int(rec.get("ACCTNO", 0)), 10)),
            (22,  fmt_s(safe_str(rec.get("AANO", "")), 13)),
            (36,  fmt_ddmmyy8(ic_dt5)),
            (45,  fmt_n(safe_int(rec.get("NOTENO", 0)), 5)),
            (51,  fmt_s(safe_str(rec.get("CCRIS_STAT", "")), 1)),
            (53,  fmt_s(safe_str(rec.get("REVERSED", "")), 1)),
        ])
        acctrejt_lines.append(ar.ljust(200)[:200])

    # ---------------------------------------------------------------------------
    # WRITE FILES
    # ---------------------------------------------------------------------------
    with open(ACCTCRED_FILE, "w", encoding="latin-1") as f:
        f.writelines(line + "\n" for line in acctcred_lines)
    logger.info(f"Written ACCTCRED ({len(acctcred_lines)} records): {ACCTCRED_FILE}")

    with open(SUBACRED_FILE, "w", encoding="latin-1") as f:
        f.writelines(line + "\n" for line in subacred_lines)
    logger.info(f"Written SUBACRED ({len(subacred_lines)} records): {SUBACRED_FILE}")

    with open(CREDITPO_FILE, "w", encoding="latin-1") as f:
        f.writelines(line + "\n" for line in creditpo_lines)
    logger.info(f"Written CREDITPO ({len(creditpo_lines)} records): {CREDITPO_FILE}")

    with open(PROVISIO_FILE, "w", encoding="latin-1") as f:
        f.writelines(line + "\n" for line in provisio_lines)
    logger.info(f"Written PROVISIO ({len(provisio_lines)} records): {PROVISIO_FILE}")

    with open(LEGALACT_FILE, "w", encoding="latin-1") as f:
        f.writelines(line + "\n" for line in legalact_lines)
    logger.info(f"Written LEGALACT ({len(legalact_lines)} records): {LEGALACT_FILE}")

    with open(ACCTREJT_FILE, "w", encoding="latin-1") as f:
        f.writelines(line + "\n" for line in acctrejt_lines)
    logger.info(f"Written ACCTREJT ({len(acctrejt_lines)} records): {ACCTREJT_FILE}")

    # *; /*
    # DATA OD;
    #   SET ACCTCRED;
    #   IF  (3000000000<=ACCTNO<=3999999999);
    # *;
    # PROC SORT; BY ACCTNO;
    # PROC SORT DATA=CCRIS.COLLATER OUT=ODCR NODUPKEY; BY ACCTNO;
    # WHERE (3000000000<=ACCTNO<=3999999999);
    # *;
    # DATA OD;
    #   MERGE OD(IN=A) ODCR(IN=B); BY ACCTNO;
    #   IF B AND NOT A;
    #   ACCTSTAT='S';
    #   FILE CREDITOD;
    #      PUT @0001 FICODE      9.
    #          @0010 APCODE      3.
    #          @0013 ACCTNO     10.
    #          @0043 NOTENO     10.
    #          @0073 "&REPTDAY"
    #          @0075 "&REPTMON"
    #          @0077 "&REPTYEAR"
    #          @0081 OUTSTAND  Z16.
    #          @0097 ARREARS    Z3.
    #          @0100 INSTALM    Z3.
    #          @0103 UNDRAWN   Z17.
    #          @0120 ACCTSTAT   $1.
    #          @0121 NODAYS     Z5.
    #          @0126 OLDBRH      5.
    #          @0131 BILTOT    Z17.
    #          @0148 ODXSAMT   Z17.
    #          @0165 COSTCTR     4.
    #          @0169 AANO      $13.
    #          @0182 FACILITY    5.
    #          @0187 FACCODE     5.
    #          ;
    #      */

    logger.info("EIBWCCR6 completed successfully.")


if __name__ == "__main__":
    main()
