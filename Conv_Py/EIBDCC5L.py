#!/usr/bin/env python3
"""
Program : EIBDCC5L
Purpose : Generate CCRIS Daily Collateral submission files for PBB (Public Bank Berhad).
          Produces:
            - COLLATER  (LRECL=200)  : Collateral master
            - DCCMS     (LRECL=200)  : DCCMS (discharged property)
            - CPROPETY  (LRECL=1200) : Collateral Property
            - CMTORVEH  (LRECL=200)  : Motor Vehicle
            - COTHVEHI  (LRECL=200)  : Other Vehicle/Carrier
            - CPLANTMA  (LRECL=200)  : Plant and Machinery
            - CCONCESS  (LRECL=200)  : Concession & Contractual Rights
            - CFINASST  (LRECL=200)  : Other Financial Assets
            - COTHASST  (LRECL=200)  : Other Assets
            - CFINGUAR  (LRECL=500)  : Financial Guarantees

          Source: SAP.PBB.* (PBB entity)
"""

import logging
import math
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl

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
BNM_DIR   = BASE_DIR / "pbb/mniln_daily"       # SAP.PBB.MNILN.DAILY(0)
BTRD_DIR  = BASE_DIR / "pbb/btrade/sasdata_daily"  # SAP.PBB.BTRADE.SASDATA.DAILY(0)
COLL_DIR  = BASE_DIR / "pbb/mnicol_daily"      # SAP.PBB.MNICOL.DAILY(0)
CCRISP_DIR = BASE_DIR / "pbb/ccris/split"      # SAP.PBB.CCRIS.SPLIT

# Output text files
OUT_DIR = BASE_DIR / "output/pbb/ccris2/daily"
OUT_DIR.mkdir(parents=True, exist_ok=True)

COLLATER_FILE = OUT_DIR / "collater.txt"   # LRECL=200
DCCMS_FILE    = OUT_DIR / "dccms.txt"      # LRECL=200
CPROPETY_FILE = OUT_DIR / "cpropety.txt"   # LRECL=1200
CMTORVEH_FILE = OUT_DIR / "cmtorveh.txt"   # LRECL=200
COTHVEHI_FILE = OUT_DIR / "cothvehi.txt"   # LRECL=200
CPLANTMA_FILE = OUT_DIR / "cplantma.txt"   # LRECL=200
CCONCESS_FILE = OUT_DIR / "cconcess.txt"   # LRECL=200
CFINASST_FILE = OUT_DIR / "cfinasst.txt"   # LRECL=200
COTHASST_FILE = OUT_DIR / "cothasst.txt"   # LRECL=200
CFINGUAR_FILE = OUT_DIR / "cfinguar.txt"   # LRECL=500

# SAS epoch
SAS_EPOCH = date(1960, 1, 1)

# ALP character set
ALP = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 ")

# ---------------------------------------------------------------------------
# CCLASSC -> COLLATER type mappings
# ---------------------------------------------------------------------------
CCLASSC_COLLATER = [
    ({"001","006","007","014","016","024","112","113","114","115","116","117",
      "025","026","046","048","049","147","149"}, "29"),
    ({"000","011","012","013","017","018","019","021","027","028","029","030","124",
      "031","105","106"}, "70"),
    ({"002","003","041","042","043","058","059","067","068","069","070","111","123",
      "071","072","078","079","084","107"}, "90"),
    ({"004","005","127","128","129","142","143","131","132","133","134","135","136",
      "137","138","139","141"}, "30"),
    ({"032","033","034","035","036","037","038","039","040","044","050","051",
      "052","053","054","055","056","057","118","119","121","122","060","061","062"}, "10"),
    ({"065","066","075","076","082","083","093","094","095","096","097","098",
      "101","102","103","104"}, "40"),
    ({"063","064","073","074","080","081"}, "60"),
    ({"010","085","086","087","088","089","090","125","126","144","091","092"}, "50"),
    ({"009","022","023"}, "00"),
    ({"008"}, "21"),
    ({"045","047"}, "22"),
    ({"015"}, "23"),
    ({"020"}, "80"),
    ({"108","109"}, "81"),
    ({"077"}, "99"),
]

# Output routing: CCLASSC sets -> output dataset name
CCLASSC_OUTPUT = [
    ({"001","006","007","014","016","024","112","113","114","115","116","117",
      "025","026","046","048","049","147","149"}, "CFINASST"),
    ({"000","011","012","013","017","018","019","021","027","028","029","030","124",
      "031","105","106"}, "CFINGUAR"),
    ({"002","003","041","042","043","058","123","059","067","068","069","070","111",
      "071","072","078","079","084","107"}, "COTHASST"),
    ({"004","005","127","128","129","142","143","131","132","133","134","135","136",
      "137","138","139","141"}, "CMTORVEH"),
    ({"032","033","034","035","036","037","038","039","040","044","050","051",
      "118","119","121","122","052","053","054","055","056","057","060","061","062"}, "CPROPETY"),
    ({"065","066","075","076","082","083","093","094","095","096","097","098",
      "101","102","103","104"}, "COTHVEHI"),
    ({"063","064","073","074","080","081"}, "CPLANTMA"),
    ({"010","085","086","087","088","089","125","126","144","090","091","092"}, "CCONCESS"),
]

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


def date_from_z11_mmddyyyy(val) -> Optional[date]:
    """Parse MMDDYYYY from first 8 chars of Z11 zero-padded number."""
    try:
        s = f"{int(val):011d}"
        mm, dd, yyyy = int(s[0:2]), int(s[2:4]), int(s[4:8])
        if mm == 0 or dd == 0 or yyyy == 0:
            return None
        return date(yyyy, mm, dd)
    except Exception:
        return None


def date_from_ddmmyy8(val: str) -> Optional[date]:
    """Parse DDMMYYYY string (8 chars)."""
    try:
        s = str(val).strip().zfill(8)
        dd, mm, yyyy = int(s[0:2]), int(s[2:4]), int(s[4:8])
        if dd == 0 or mm == 0 or yyyy == 0:
            return None
        return date(yyyy, mm, dd)
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


def fmt_ddmmyyn(dt) -> str:
    """Format date as DDMMYYYY (8 chars, SAS DDMMYYN format)."""
    if dt is None:
        return "        "
    try:
        if isinstance(dt, (int, float)):
            dt = sas_to_date(int(dt))
        return dt.strftime("%d%m%Y")
    except Exception:
        return "        "


def build_record(fields: list[tuple], lrecl: int) -> str:
    """Build fixed-width record from (start_col_1based, value_str) tuples."""
    buf = list(" " * lrecl)
    for pos, val in fields:
        for i, ch in enumerate(val):
            idx = pos - 1 + i
            if 0 <= idx < lrecl:
                buf[idx] = ch
    return "".join(buf)


def get_collater_type(cclassc: str) -> str:
    """Map CCLASSC to COLLATER type code."""
    for cset, code in CCLASSC_COLLATER:
        if cclassc in cset:
            return code
    return "  "


def get_output_targets(cclassc: str) -> list[str]:
    """Map CCLASSC to list of output dataset names."""
    targets = []
    for cset, name in CCLASSC_OUTPUT:
        if cclassc in cset:
            targets.append(name)
    return targets


def clamp_valuation_date(vtdd: int, vtmm: int, vtyy: int,
                          reptday: int, bkmth: int, bkyyr: int, bkday: int) -> tuple[int, int, int]:
    """Apply valuation date clamping logic for weeks 1/2/3."""
    if reptday in (8, 15, 22):
        if vtmm > bkmth and vtyy >= bkyyr:
            return bkday, bkmth, bkyyr
    return vtdd, vtmm, vtyy


def verify_digits_only(s: str) -> bool:
    """Returns True if all chars in s are digits (equivalent to VERIFY=0)."""
    if not s:
        return False
    return all(c.isdigit() for c in s)


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

    if wk == "4":
        mm2 = mm
    else:
        mm2 = mm - 1
        if mm2 == 0:
            mm2 = 12

    sdate_obj = date(yy, mm, day)
    mdate_sas = date_to_sas(reptdate)
    sdate_sas = date_to_sas(sdate_obj)

    # BKDATE = MDATE - REPTDAY  (first day of current month - 1 = last day of prev month... actually MDATE - DAY)
    # In SAS: BKDATE = &MDATE - &REPTDAY means SAS date of reptdate - reptday (numeric day)
    bkdate_sas = mdate_sas - day
    bkdate_obj = sas_to_date(bkdate_sas)

    reptyear2 = str(yy)[2:]   # YEAR2 format = last 2 digits

    return {
        "reptdate":   reptdate,
        "nowk":       wk,
        "nowk1":      wk1,
        "reptmon":    f"{mm:02d}",
        "reptmon1":   f"{mm1:02d}",
        "reptmon2":   f"{mm2:02d}",
        "reptyear":   reptyear2,
        "rdate":      reptdate.strftime("%d/%m/%y"),
        "reptday":    day,
        "reptday_str": f"{day:02d}",
        "sdate_sas":  sdate_sas,
        "mdate_sas":  mdate_sas,
        "bkdate_obj": bkdate_obj,
        "bkmth":      bkdate_obj.month if bkdate_obj else mm,
        "bkyyr":      bkdate_obj.year  if bkdate_obj else yy,
        "bkday":      bkdate_obj.day   if bkdate_obj else 1,
    }


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    logger.info("Starting EIBDCC5L")

    # --- Date context ---
    ctx = load_dates(BNM_DIR)
    reptday    = ctx["reptday"]
    reptday_str = ctx["reptday_str"]
    reptmon    = ctx["reptmon"]
    reptyear   = ctx["reptyear"]
    bkmth      = ctx["bkmth"]
    bkyyr      = ctx["bkyyr"]
    bkday      = ctx["bkday"]
    mdate_sas  = ctx["mdate_sas"]
    logger.info(f"REPTDATE={ctx['reptdate']} WK={ctx['nowk']} REPTYEAR={reptyear}")

    # PROC PRINT DATA=DATES (informational only)

    # ===========================================================================
    # PROC SORT DATA=BTRD.MAST{REPTYEAR}{REPTMON}{REPTDAY}
    # OUT=BTRD (DROP=ACCTNO RENAME=(ACCTNON=ACCTNO))
    # ===========================================================================
    btrd_path = BTRD_DIR / f"mast{reptyear}{reptmon}{reptday_str}.parquet"
    con = duckdb.connect()
    btrd_df = con.execute(f"""
        SELECT *, ACCTNON AS ACCTNO_NEW
        FROM read_parquet('{btrd_path}')
        ORDER BY ACCTNON
    """).pl()
    con.close()

    # PROC SORT DATA=BNM.LNACCT OUT=BTRL
    lnacct_path = BNM_DIR / "lnacct.parquet"
    con = duckdb.connect()
    btrl_df = con.execute(f"""
        SELECT * FROM read_parquet('{lnacct_path}')
        ORDER BY ACCTNO
    """).pl()
    con.close()

    # DATA BTRL: MERGE BTRD(IN=A) BTRL(IN=B) BY ACCTNO; IF A AND ACCTNO > 0 AND SETTLED NE 'S'
    btrd_records = {safe_int(r.get("ACCTNON", 0)): r for r in btrd_df.to_dicts()}
    btrl_records = {safe_int(r.get("ACCTNO", 0)): r for r in btrl_df.to_dicts()}

    btrl_merged = []
    for acctnon, a_row in btrd_records.items():
        acctno = acctnon
        if acctno <= 0:
            continue
        settled = safe_str(a_row.get("SETTLED", ""))
        if settled == "S":
            continue
        b_row = btrl_records.get(acctno, {})
        merged = {**b_row, **a_row}
        merged["ACCTNO"]   = acctno
        merged["BRANCH"]   = safe_int(merged.get("FICODE", 0))
        loantype = 0
        if safe_str(merged.get("RETAILID", "")) == "C":
            loantype = 999
        merged["LOANTYPE"] = loantype
        btrl_merged.append(merged)

    # ===========================================================================
    # PROC SORT DATA=CCRISP.OVERDFS OUT=OVERDFS; BY ACCTNO
    # PROC SORT DATA=CCRISP.LOAN OUT=LOAN (DROP=FDACCTNO); BY ACCTNO NOTENO
    # ===========================================================================
    overdfs_path = CCRISP_DIR / "overdfs.parquet"
    loan_path    = CCRISP_DIR / "loan.parquet"

    con = duckdb.connect()
    overdfs_df = con.execute(f"""
        SELECT * FROM read_parquet('{overdfs_path}')
        ORDER BY ACCTNO
    """).pl()
    loan_df = con.execute(f"""
        SELECT * EXCLUDE (FDACCTNO) FROM read_parquet('{loan_path}')
        ORDER BY ACCTNO, NOTENO
    """).pl()
    con.close()

    # ===========================================================================
    # DATA CCOLLAT: SET COLL.COLLATER; filter EFF_DX/START_DX
    # ===========================================================================
    collater_path = COLL_DIR / "collater.parquet"
    con = duckdb.connect()
    ccollat_raw = con.execute(f"SELECT * FROM read_parquet('{collater_path}')").pl()
    con.close()

    jul2018_sas = date_to_sas(date(2018, 7, 1))
    ccollat_records = []
    for row in ccollat_raw.to_dicts():
        eff_dx   = None
        start_dx = None

        coll_eff_dt = row.get("COLLATERAL_EFF_DT")
        if coll_eff_dt is not None and safe_float(coll_eff_dt, 0) not in (0.0,):
            eff_dx = date_from_z11_mmddyyyy(coll_eff_dt)

        coll_start_dt = safe_str(row.get("COLLATERAL_START_DT", "")).strip()
        if coll_start_dt and coll_start_dt != "00000000":
            start_dx = date_from_ddmmyy8(coll_start_dt)

        # IF EFF_DX >= '01JUL2018'D & START_DX IN (.,0) THEN DELETE; *18-2830
        if eff_dx is not None and date_to_sas(eff_dx) >= jul2018_sas:
            if start_dx is None:
                continue

        row["EFF_DX"]   = date_to_sas(eff_dx)   if eff_dx   else None
        row["START_DX"] = date_to_sas(start_dx) if start_dx else None
        ccollat_records.append(row)

    # Build ccollat lookup by (ACCTNO, NOTENO) and by ACCTNO
    ccollat_by_acctno_noteno: dict[tuple, dict] = {}
    ccollat_by_acctno: dict[int, dict] = {}
    for row in sorted(ccollat_records,
                      key=lambda r: (safe_int(r.get("ACCTNO", 0)),
                                     safe_int(r.get("NOTENO", 0)))):
        acctno = safe_int(row.get("ACCTNO", 0))
        noteno = safe_int(row.get("NOTENO", 0))
        key    = (acctno, noteno)
        if key not in ccollat_by_acctno_noteno:
            ccollat_by_acctno_noteno[key] = row
        if acctno not in ccollat_by_acctno:
            ccollat_by_acctno[acctno] = row

    # ===========================================================================
    # DATA LOAN: MERGE LOAN(IN=A) CCOLLAT(IN=B); BY ACCTNO NOTENO; IF A AND B
    # ===========================================================================
    loan_final = []
    for row in loan_df.to_dicts():
        acctno = safe_int(row.get("ACCTNO", 0))
        noteno = safe_int(row.get("NOTENO", 0))
        coll   = ccollat_by_acctno_noteno.get((acctno, noteno))
        if coll:
            merged = {**coll, **row}
            loan_final.append(merged)

    # ===========================================================================
    # DATA OVERDFS: MERGE OVERDFS(IN=A) CCOLLAT(IN=B); BY ACCTNO; IF A AND B
    # IF APPRLIMT IN (.,0) AND ODSTATUS NOT IN ('NI','RI') THEN DELETE
    # ===========================================================================
    overdfs_final = []
    for row in overdfs_df.to_dicts():
        acctno  = safe_int(row.get("ACCTNO", 0))
        coll    = ccollat_by_acctno.get(acctno)
        if not coll:
            continue
        merged  = {**coll, **row}
        apprlimt = safe_float(merged.get("APPRLIMT", 0))
        odstatus = safe_str(merged.get("ODSTATUS", ""))
        if apprlimt in (None, 0.0) and odstatus not in ("NI", "RI"):
            continue
        overdfs_final.append(merged)

    # ===========================================================================
    # DATA BTRL: MERGE BTRL(IN=A) CCOLLAT(IN=B); BY ACCTNO; IF A AND B; BRANCH=FICODE
    # ===========================================================================
    btrl_final = []
    btrl_by_acctno = {safe_int(r.get("ACCTNO", 0)): r for r in btrl_merged}
    for acctno, a_row in btrl_by_acctno.items():
        coll = ccollat_by_acctno.get(acctno)
        if not coll:
            continue
        merged = {**coll, **a_row}
        merged["BRANCH"] = safe_int(merged.get("FICODE", 0))
        btrl_final.append(merged)

    # ===========================================================================
    # DATA COLLATER: SET LOAN OVERDFS BTRL; IF NOTENO=. THEN NOTENO=0
    # ===========================================================================
    collater_all = []
    for row in loan_final + overdfs_final + btrl_final:
        if row.get("NOTENO") is None:
            row["NOTENO"] = 0
        collater_all.append(row)

    # Sort by ACCTNO NOTENO
    collater_all.sort(key=lambda r: (safe_int(r.get("ACCTNO", 0)),
                                     safe_int(r.get("NOTENO", 0))))

    # ===========================================================================
    # Main processing loop - classify and route records
    # ===========================================================================
    collater_lines: list[str] = []
    dccms_lines:    list[str] = []
    cpropety_rows:  list[dict] = []
    cmtorveh_rows:  list[dict] = []
    cothvehi_rows:  list[dict] = []
    cplantma_rows:  list[dict] = []
    cconcess_rows:  list[dict] = []
    cfinasst_rows:  list[dict] = []
    cothasst_rows:  list[dict] = []
    cfinguar_rows:  list[dict] = []

    output_map = {
        "CPROPETY": cpropety_rows,
        "CMTORVEH": cmtorveh_rows,
        "COTHVEHI": cothvehi_rows,
        "CPLANTMA": cplantma_rows,
        "CCONCESS": cconcess_rows,
        "CFINASST": cfinasst_rows,
        "COTHASST": cothasst_rows,
        "CFINGUAR": cfinguar_rows,
    }

    for row in collater_all:
        purgeind = safe_str(row.get("PURGEIND", ""))
        if purgeind == "N":
            continue

        # BKDATE / BKMTH / BKYYR / BKDAY already derived from ctx
        row["BKMTH"] = bkmth
        row["BKYYR"] = bkyyr
        row["BKDAY"] = bkday

        # COLLREF = CCOLLNO
        row["COLLREF"] = row.get("CCOLLNO", row.get("COLLREF", 0))

        branch  = safe_int(row.get("BRANCH", 0))
        ntbrch  = safe_int(row.get("NTBRCH", 0))
        accbrch = safe_int(row.get("ACCBRCH", 0))
        if branch == 0:
            branch = ntbrch
        if branch == 0:
            branch = accbrch
        row["BRANCH"] = branch

        apcode  = safe_int(row.get("LOANTYPE", 0))
        row["APCODE"] = apcode
        ficode  = branch

        cclassc  = safe_str(row.get("CCLASSC", "")).strip().zfill(3)
        row["CCLASSC"] = cclassc
        collater_type = get_collater_type(cclassc)

        # CISSUER override
        cissuer = safe_str(row.get("CISSUER", ""))
        if cissuer[:3] == "KLM" or cissuer[:2] == "UT":
            collater_type = "23"

        row["COLLATER_TYPE"] = collater_type

        # COLLVAL / COLLVALX
        cdolarv  = safe_float(row.get("CDOLARV", 0))
        cdolarvx = safe_float(row.get("CDOLARVX", 0))
        collval  = cdolarv  * 100
        collvalx = cdolarvx * 100
        cguarnat = safe_str(row.get("CGUARNAT", "")).strip()
        curcode  = safe_str(row.get("CURCODE", "")).strip()
        if cguarnat != "06" and curcode in ("", "MYR", "  "):
            collvalx = 0.0
        row["COLLVAL"]  = collval
        row["COLLVALX"] = collvalx

        # DELETE if COLLATER still blank
        if collater_type.strip() == "":
            continue

        # APCODE / FICODE override for 800-899 range
        if 800 <= apcode <= 899:
            acctno = safe_int(row.get("ACCTNO", 0))
            if 2000000000 <= acctno <= 2999999999:
                ficode = 904
            if 8000000000 <= acctno <= 8999999999:
                ficode = 904
        row["FICODE"] = ficode

        # ACTUAL_SALE_VALUE / CPRFORSV / CPRESVAL defaults
        succaucpc = safe_float(row.get("SUCCAUCPC", 0))
        actual_sv = safe_float(row.get("ACTUAL_SALE_VALUE", 0))
        if succaucpc > 0:
            actual_sv = succaucpc
        if actual_sv in (None, 0.0):
            actual_sv = 0.0
        row["ACTUAL_SALE_VALUE"] = actual_sv

        cprforsv = safe_float(row.get("CPRFORSV", 0))
        if cprforsv in (None, 0.0):
            cprforsv = 0.0
        row["CPRFORSV"] = cprforsv

        cpresval = safe_float(row.get("CPRESVAL", 0))
        if cpresval in (None, 0.0):
            cpresval = 0.0
        row["CPRESVAL"] = cpresval

        # COLLATERAL_START_DT < 0 -> 0
        coll_start = safe_int(row.get("COLLATERAL_START_DT", 0))
        if coll_start < 0:
            coll_start = 0
        row["COLLATERAL_START_DT"] = coll_start

        # CINSTCL: 18-359
        cinstcl = safe_str(row.get("CINSTCL", "")).strip()
        if cinstcl in ("19", "20"):
            cinstcl = "35"
        row["CINSTCL"] = cinstcl

        # Write COLLATER record
        acctno  = safe_int(row.get("ACCTNO", 0))
        noteno  = safe_int(row.get("NOTENO", 0))
        collref = safe_int(row.get("COLLREF", 0))
        oldbrh  = safe_int(row.get("OLDBRH", 0))
        costctr = safe_int(row.get("COSTCTR", 0))
        aano    = safe_str(row.get("AANO", ""))
        facility = safe_int(row.get("FACILITY", 0))
        faccode  = safe_int(row.get("FACCODE", 0))
        cgexamtg = safe_float(row.get("CGEXAMTG", 0))
        coll_end_dt = safe_str(row.get("COLLATERAL_END_DT", "")).strip()

        cline = build_record([
            (1,   fmt_n(ficode, 9)),
            (10,  fmt_n(apcode, 3)),
            (13,  fmt_n(acctno, 10)),
            (43,  fmt_n(noteno, 10)),
            (73,  fmt_n(collref, 11)),
            (103, fmt_s(collater_type, 2)),
            (105, fmt_z(int(collval), 16)),
            (121, fmt_n(oldbrh, 5)),
            (126, fmt_n(costctr, 4)),
            (130, fmt_s(aano, 13)),
            (143, fmt_n(facility, 5)),
            (148, fmt_n(faccode, 5)),
            (154, fmt_f(cgexamtg, 15, 2)),
            (170, fmt_z(coll_start, 8)),
            (178, fmt_s(coll_end_dt, 8)),
        ], 200)
        collater_lines.append(cline)

        # Route to DCCMS if COLLATER='10' AND CPRDISDT all digits
        cprdisdt = safe_str(row.get("CPRDISDT", "")).strip()
        if collater_type == "10":
            if verify_digits_only(cprdisdt):
                # OUTPUT DCCMS (written later from dccms_rows)
                row["_DCCMS"] = True
            else:
                # IF VERIFY(CPRDISDT,'0123456789')=1 means NOT all digits -> keep for non-DCCMS
                pass

        # Route to detail output datasets
        targets = get_output_targets(cclassc)
        for t in targets:
            if t in output_map:
                output_map[t].append(dict(row))

    # Write COLLATER
    with open(COLLATER_FILE, "w", encoding="latin-1") as f:
        f.writelines(ln + "\n" for ln in collater_lines)
    logger.info(f"Written COLLATER ({len(collater_lines)} records): {COLLATER_FILE}")

    # ===========================================================================
    # DATA _NULL_: SET DCCMS; FILE DCCMS
    # ===========================================================================
    dccms_rows = [r for r in collater_all if r.get("_DCCMS")]
    with open(DCCMS_FILE, "w", encoding="latin-1") as f:
        for row in dccms_rows:
            acctno   = safe_int(row.get("ACCTNO", 0))
            noteno   = safe_int(row.get("NOTENO", 0))
            collref  = safe_int(row.get("COLLREF", 0))
            ficode   = safe_int(row.get("FICODE", 0))
            costctr  = safe_int(row.get("COSTCTR", 0))
            aano     = safe_str(row.get("AANO", ""))
            facility = safe_int(row.get("FACILITY", 0))
            faccode  = safe_int(row.get("FACCODE", 0))
            cprdisdt = safe_str(row.get("CPRDISDT", "")).strip()
            ctype    = safe_str(row.get("COLLATER_TYPE", ""))

            line = build_record([
                (1,   fmt_n(ficode, 9)),
                (13,  fmt_n(acctno, 10)),
                (43,  fmt_n(noteno, 10)),
                (73,  fmt_n(collref, 11)),
                (103, fmt_s(ctype, 2)),
                (105, fmt_n(costctr, 4)),
                (130, fmt_s(aano, 13)),
                (143, fmt_n(facility, 5)),
                (148, fmt_s(cprdisdt[:8].ljust(8), 8)),
                (156, fmt_n(faccode, 5)),
            ], 200)
            f.write(line + "\n")
    logger.info(f"Written DCCMS ({len(dccms_rows)} records): {DCCMS_FILE}")

    # ===========================================================================
    # DATA CPROPETY (FOR COLLATERAL PROPERTY FILE)
    # ===========================================================================
    # Sort by ACCTNO NOTENO DESCENDING CPRPARC1
    cpropety_rows.sort(key=lambda r: (
        safe_int(r.get("ACCTNO", 0)),
        safe_int(r.get("NOTENO", 0)),
        safe_str(r.get("CPRPARC1", "")),
    ), reverse=False)
    # Descending CPRPARC1 means reverse on that key
    cpropety_rows.sort(key=lambda r: (
        safe_int(r.get("ACCTNO", 0)),
        safe_int(r.get("NOTENO", 0)),
    ))
    # Re-sort with DESCENDING CPRPARC1
    from functools import cmp_to_key
    def cmp_cpropety(a, b):
        ka = (safe_int(a.get("ACCTNO", 0)), safe_int(a.get("NOTENO", 0)))
        kb = (safe_int(b.get("ACCTNO", 0)), safe_int(b.get("NOTENO", 0)))
        if ka != kb:
            return -1 if ka < kb else 1
        pa = safe_str(a.get("CPRPARC1", ""))
        pb = safe_str(b.get("CPRPARC1", ""))
        return 1 if pa < pb else (-1 if pa > pb else 0)  # descending
    cpropety_rows.sort(key=cmp_to_key(cmp_cpropety))

    with open(CPROPETY_FILE, "w", encoding="latin-1") as f:
        for row in cpropety_rows:
            # Scale amounts *100
            cprforsv   = safe_float(row.get("CPRFORSV", 0)) * 100
            cpresval   = safe_float(row.get("CPRESVAL", 0)) * 100
            ltabtaucpc = safe_float(row.get("LTABTAUCPC", 0)) * 100
            succaucpc  = safe_float(row.get("SUCCAUCPC", 0)) * 100
            actual_sv  = safe_float(row.get("ACTUAL_SALE_VALUE", 0))

            # Valuation date from CPRVALDT
            cprvaldt = safe_str(row.get("CPRVALDT", "")).strip()
            vtdd = safe_int(cprvaldt[0:2]) if len(cprvaldt) >= 6 else 0
            vtmm = safe_int(cprvaldt[2:4]) if len(cprvaldt) >= 6 else 0
            vtyy = safe_int(cprvaldt[4:8]) if len(cprvaldt) >= 8 else 0
            vtdd, vtmm, vtyy = clamp_valuation_date(vtdd, vtmm, vtyy, reptday, bkmth, bkyyr, bkday)

            # LANDNREA
            landarea_raw = safe_str(row.get("LANDAREA", "")).strip()
            if landarea_raw in ("0", "", "  "):
                landnrea = "0000"
            else:
                try:
                    landnrea = str(int(float(landarea_raw))).zfill(4)[:4]
                except Exception:
                    landnrea = "0000"

            # CPRPAR1C-4C from CPRPARC1-4
            def par_clean(prc_val: str) -> str:
                if not prc_val:
                    return " " * 40
                if len(prc_val) > 0 and prc_val[0].upper() not in ALP:
                    return prc_val[1:40].ljust(40)[:40]
                return prc_val[:40].ljust(40)

            cprpar1c = par_clean(safe_str(row.get("CPRPARC1", "")))
            cprpar2c = par_clean(safe_str(row.get("CPRPARC2", "")))
            cprpar3c = par_clean(safe_str(row.get("CPRPARC3", "")))
            cprpar4c = par_clean(safe_str(row.get("CPRPARC4", "")))

            # Validation of fields
            cplocat  = safe_str(row.get("CPLOCAT", " "))
            if cplocat.strip() and cplocat[0].upper() not in ALP:
                cplocat = " "
            ownocupy = safe_str(row.get("OWNOCUPY", " "))
            if ownocupy.strip() and ownocupy[0].upper() not in ALP:
                ownocupy = " "
            cposcode = safe_str(row.get("CPOSCODE", "     ")).strip().ljust(5)[:5]
            if cposcode[0:1] not in "0123456789":
                cposcode = "     "
            bltnarea = safe_str(row.get("BLTNAREA", "    ")).strip().ljust(4)[:4]
            if bltnarea[0:1] not in "0123456789":
                bltnarea = "    "
            bltnunit = safe_str(row.get("BLTNUNIT", " "))
            if bltnunit.strip() and bltnunit[0].upper() not in ALP:
                bltnunit = " "
            landunit = safe_str(row.get("LANDUNIT", " "))
            if landunit.strip() and landunit[0].upper() not in ALP:
                landunit = " "
            cpstate_raw = safe_str(row.get("CPSTATE", "  ")).ljust(2)[:2]
            if len(cpstate_raw) >= 2 and cpstate_raw[1] not in "0123456789":
                cpstate_raw = "  "
            cpstate_val = safe_int(cpstate_raw.strip(), 0)

            # ESMR 2011-4011: address fields
            cphseno  = safe_str(row.get("CPHSENO", ""))
            addrb02  = safe_str(row.get("ADDRB02", ""))
            addrb03  = safe_str(row.get("ADDRB03", ""))
            clhseno  = cphseno if cphseno.strip() else addrb02
            cljlnnm  = cprpar2c if cprpar2c.strip() else addrb03
            clbldnm  = safe_str(row.get("CPBUILNO", ""))

            # ESMR 2012-2277: ACCTSTAT
            openind = safe_str(row.get("OPENIND", ""))
            paidind = safe_str(row.get("PAIDIND", ""))
            acctstat = ""
            if openind in ("B", "C", "P") or paidind == "P":
                acctstat = "S"

            # FIREDATE processing
            firedate_raw = safe_str(row.get("FIREDATE", "")).strip().ljust(8)[:8]
            try:
                fd_int = int(firedate_raw)
                fd_str = f"{fd_int:08d}"
                frdd = fd_str[0:2]
                frmm = fd_str[2:4]
                fryy = fd_str[4:8]
            except Exception:
                frdd = "00"; frmm = "00"; fryy = "0000"
            firedatex = f"{fryy}-{frmm}-{frdd}" if fryy != "0000" else ""

            # CPOLYNUM cleansing
            cpolynum = safe_str(row.get("CPOLYNUM", ""))
            remove_chars = '=`&/\\@#*+:().,"%!$?_ \''
            for ch in remove_chars:
                cpolynum = cpolynum.replace(ch, "")

            # /* REVISED CRITERIA FOR ESMR 2013-616 ... (commented out in original) */

            ficode   = safe_int(row.get("FICODE", 0))
            apcode   = safe_int(row.get("APCODE", 0))
            acctno   = safe_int(row.get("ACCTNO", 0))
            ccollno  = safe_int(row.get("CCOLLNO", 0))
            noteno   = safe_int(row.get("NOTENO", 0))
            cinstcl  = safe_str(row.get("CINSTCL", ""))
            cprrankc = safe_str(row.get("CPRRANKC", ""))
            cprshare = safe_str(row.get("CPRSHARE", ""))
            cprlandu = safe_str(row.get("CPRLANDU", ""))
            cprpropd = safe_str(row.get("CPRPROPD", ""))
            cprvalu1 = safe_str(row.get("CPRVALU1", ""))
            cprvalu2 = safe_str(row.get("CPRVALU2", ""))
            cprvalu3 = safe_str(row.get("CPRVALU3", ""))
            cprabndt = safe_int(row.get("CPRABNDT", 0))
            name1own = safe_str(row.get("NAME1OWN", ""))
            oldbrh   = safe_int(row.get("OLDBRH", 0))
            costctr  = safe_int(row.get("COSTCTR", 0))
            aano     = safe_str(row.get("AANO", ""))
            facility = safe_int(row.get("FACILITY", 0))
            adlrefno = safe_int(row.get("ADLREFNO", 0))
            devname  = safe_str(row.get("DEVNAME", ""))
            faccode  = safe_int(row.get("FACCODE", 0))
            cprstat  = safe_str(row.get("CPRSTAT", ""))
            insurer  = safe_str(row.get("INSURER", ""))
            suminsur = safe_float(row.get("SUMINSUR", 0))
            cprparc3 = safe_str(row.get("CPRPARC3", ""))
            cprparc4 = safe_str(row.get("CPRPARC4", ""))
            ctrycode = safe_str(row.get("CTRYCODE", ""))
            aucind   = safe_str(row.get("AUCIND", ""))
            auccomdt = safe_str(row.get("AUCCOMDT", ""))
            noauc    = safe_int(row.get("NOAUC", 0))
            aucsuccind = safe_str(row.get("AUCSUCCIND", ""))
            succaucdt  = safe_str(row.get("SUCCAUCDT", ""))
            cptownn    = safe_str(row.get("CPTOWNN", ""))
            property_ind = safe_str(row.get("PROPERTY_IND", ""))
            ttlpartclr = safe_str(row.get("TTLPARTCLR", ""))
            mastownr   = safe_str(row.get("MASTOWNR", ""))
            ttleno     = safe_str(row.get("TTLENO", ""))
            ttlmukim   = safe_str(row.get("TTLMUKIM", ""))
            holdexpd   = safe_str(row.get("HOLDEXPD", ""))
            expdate    = safe_str(row.get("EXPDATE", ""))
            prjctnam   = safe_str(row.get("PRJCTNAM", ""))

            line = build_record([
                (1,    fmt_n(ficode, 9)),
                (10,   fmt_n(apcode, 3)),
                (13,   fmt_n(acctno, 10)),
                (23,   fmt_n(ccollno, 11)),
                (36,   fmt_n(noteno, 5)),
                (43,   fmt_s(cinstcl, 2)),
                (45,   fmt_s(cprrankc, 1)),
                (46,   fmt_s(cprshare, 1)),
                (47,   fmt_s(cprlandu, 2)),
                (49,   fmt_s(cprpropd, 2)),
                (51,   fmt_s(cprpar1c, 40)),
                (91,   fmt_s(cprpar2c, 40)),
                (131,  fmt_s(cprpar3c, 40)),
                (171,  fmt_s(cprpar4c[:30], 30)),
                (201,  fmt_z(int(cprforsv), 16)),
                (217,  fmt_z(int(cpresval), 16)),
                (233,  fmt_z(vtdd, 2)),
                (235,  fmt_z(vtmm, 2)),
                (237,  fmt_z(vtyy, 4)),
                (241,  fmt_s(cprvalu1, 40)),
                (281,  fmt_s(cprvalu2, 40)),
                (321,  fmt_s(cprvalu3, 20)),
                (341,  fmt_z(cprabndt, 8)),
                (349,  fmt_s(name1own, 40)),
                (389,  fmt_s(ownocupy[:1], 1)),
                (394,  fmt_s(landnrea[:4], 4)),
                (398,  fmt_z(oldbrh, 5)),
                (403,  fmt_s(cplocat[:1], 1)),
                (404,  fmt_s(cposcode, 5)),
                (409,  fmt_s(bltnarea, 4)),
                (413,  fmt_n(costctr, 4)),
                # @0417 MASTTLNO $20. / @0437 MASTTLMD $20.  -- 2016-1859 commented out
                (417,  fmt_s(ttlpartclr[:1], 1)),
                (457,  fmt_s(mastownr, 40)),
                (497,  fmt_s(ttleno, 40)),
                (537,  fmt_s(ttlmukim, 40)),
                (577,  fmt_s(holdexpd[:1], 1)),
                (578,  fmt_s(expdate[:8], 8)),
                (586,  fmt_s(devname[:40], 40)),
                (626,  fmt_s(prjctnam, 40)),
                (666,  fmt_n(cpstate_val, 2)),
                (668,  fmt_s(bltnunit[:1], 1)),
                (669,  fmt_s(landunit[:1], 1)),
                (670,  fmt_s(aano, 13)),
                (683,  fmt_n(facility, 5)),
                (688,  fmt_n(adlrefno, 25)),
                (713,  fmt_s(devname[:60], 60)),
                (773,  fmt_n(faccode, 5)),
                (779,  fmt_s(cprstat[:1], 1)),
                (788,  fmt_s(clhseno[:11], 11)),   # HOUSE NO
                (800,  fmt_s(cljlnnm[:60], 60)),   # JALAN NAME
                (860,  fmt_s(clbldnm[:60], 60)),   # BUILDING NAME
                (920,  fmt_s(insurer[:2], 2)),      # INSURER CODE
                (923,  fmt_s(cpolynum[:16], 16)),   # POLICY NUMBER
                (940,  fmt_s(firedatex[:10], 10)),  # EXPIRY DATE
                (951,  fmt_f(suminsur, 16, 0)),     # SUM INSURED
                (968,  fmt_s(cprparc3, 40)),
                (1009, fmt_s(cprparc4, 40)),        # 2016-978
                (1050, fmt_s(ctrycode[:2], 2)),     # COUNTRY CODE
                (1052, fmt_s(aucind[:1], 1)),       # AUCTION INDICATOR
                (1053, fmt_s(auccomdt[:8], 8)),     # AUCTION COMMENCEMENT DATE
                (1061, fmt_n(noauc, 2)),            # NO. OF AUCTION
                (1063, fmt_s(aucsuccind[:1], 1)),   # AUC SUCCESSFUL INDICATOR
                (1064, fmt_n(int(ltabtaucpc), 10)), # LAST ABORTED AUCTION PRICE
                (1074, fmt_s(succaucdt[:8], 8)),    # SUCCESSFUL AUC DATE
                (1082, fmt_n(int(succaucpc), 10)),  # SUCCESSFUL AUC PRICE
                (1093, fmt_s(cptownn[:20], 20)),    # PROPERTY TOWN NAME
                (1113, fmt_f(actual_sv, 16, 2)),
                (1130, fmt_s(property_ind[:1], 1)),
            ], 1200)
            f.write(line + "\n")
    logger.info(f"Written CPROPETY ({len(cpropety_rows)} records): {CPROPETY_FILE}")

    # ===========================================================================
    # DATA CMTORVEH (COLLATERAL MOTOR VEHICLE)
    # ===========================================================================
    with open(CMTORVEH_FILE, "w", encoding="latin-1") as f:
        for row in cmtorveh_rows:
            ficode   = safe_int(row.get("FICODE", 0))
            apcode   = safe_int(row.get("APCODE", 0))
            acctno   = safe_int(row.get("ACCTNO", 0))
            ccollno  = safe_int(row.get("CCOLLNO", 0))
            noteno   = safe_int(row.get("NOTENO", 0))
            cinstcl  = safe_str(row.get("CINSTCL", ""))
            chpmake  = safe_str(row.get("CHPMAKE", ""))
            chpengin = safe_str(row.get("CHPENGIN", ""))
            chpchass = safe_str(row.get("CHPCHASS", ""))
            chpvehno = safe_str(row.get("CHPVEHNO", ""))
            oldbrh   = safe_int(row.get("OLDBRH", 0))
            costctr  = safe_int(row.get("COSTCTR", 0))
            aano     = safe_str(row.get("AANO", ""))
            facility = safe_int(row.get("FACILITY", 0))
            faccode  = safe_int(row.get("FACCODE", 0))

            line = build_record([
                (1,   fmt_n(ficode, 9)),
                (10,  fmt_n(apcode, 3)),
                (13,  fmt_n(acctno, 10)),
                (23,  fmt_n(ccollno, 11)),
                (36,  fmt_n(noteno, 5)),
                (43,  fmt_s(cinstcl, 2)),
                (45,  fmt_s(chpmake, 40)),
                (85,  fmt_s(chpengin, 20)),
                (105, fmt_s(chpchass, 20)),
                (125, fmt_s(chpvehno, 12)),
                (137, fmt_n(oldbrh, 5)),
                (142, fmt_n(costctr, 4)),
                (146, fmt_s(aano, 13)),
                (159, fmt_n(facility, 5)),
                (164, fmt_n(faccode, 5)),
            ], 200)
            f.write(line + "\n")
    logger.info(f"Written CMTORVEH ({len(cmtorveh_rows)} records): {CMTORVEH_FILE}")

    # ===========================================================================
    # DATA COTHVEHI (COLLATERAL OTHER VEHICLE/CARRIER)
    # ===========================================================================
    with open(COTHVEHI_FILE, "w", encoding="latin-1") as f:
        for row in cothvehi_rows:
            covvaldt = safe_str(row.get("COVVALDT", "")).strip()
            vtdd = safe_int(covvaldt[0:2]) if len(covvaldt) >= 6 else 0
            vtmm = safe_int(covvaldt[2:4]) if len(covvaldt) >= 6 else 0
            vtyy = safe_int(covvaldt[4:8]) if len(covvaldt) >= 8 else 0
            vtdd, vtmm, vtyy = clamp_valuation_date(vtdd, vtmm, vtyy, reptday, bkmth, bkyyr, bkday)

            ficode   = safe_int(row.get("FICODE", 0))
            apcode   = safe_int(row.get("APCODE", 0))
            acctno   = safe_int(row.get("ACCTNO", 0))
            ccollno  = safe_int(row.get("CCOLLNO", 0))
            noteno   = safe_int(row.get("NOTENO", 0))
            cinstcl  = safe_str(row.get("CINSTCL", ""))
            covdescr = safe_str(row.get("COVDESCR", ""))
            oldbrh   = safe_int(row.get("OLDBRH", 0))
            costctr  = safe_int(row.get("COSTCTR", 0))
            aano     = safe_str(row.get("AANO", ""))
            facility = safe_int(row.get("FACILITY", 0))
            faccode  = safe_int(row.get("FACCODE", 0))
            actual_sv = safe_float(row.get("ACTUAL_SALE_VALUE", 0))

            line = build_record([
                (1,   fmt_n(ficode, 9)),
                (10,  fmt_n(apcode, 3)),
                (13,  fmt_n(acctno, 10)),
                (23,  fmt_n(ccollno, 11)),
                (36,  fmt_n(noteno, 5)),
                (43,  fmt_s(cinstcl, 2)),
                (45,  fmt_s(covdescr[:2], 2)),
                (47,  fmt_z(vtdd, 2)),
                (49,  fmt_z(vtmm, 2)),
                (51,  fmt_z(vtyy, 4)),
                (55,  fmt_n(oldbrh, 5)),
                (60,  fmt_n(costctr, 4)),
                (64,  fmt_s(aano, 13)),
                (77,  fmt_n(facility, 5)),
                (82,  fmt_n(faccode, 5)),
                (127, fmt_f(actual_sv, 16, 2)),
            ], 200)
            f.write(line + "\n")
    logger.info(f"Written COTHVEHI ({len(cothvehi_rows)} records): {COTHVEHI_FILE}")

    # ===========================================================================
    # DATA CPLANTMA (COLLATERAL PLANT AND MACHINERY)
    # ===========================================================================
    with open(CPLANTMA_FILE, "w", encoding="latin-1") as f:
        for row in cplantma_rows:
            cpmdescr = safe_str(row.get("CPMDESCR", ""))
            cpmdesc1 = cpmdescr[1:2] if len(cpmdescr) >= 2 else " "

            cpmvaldt = safe_str(row.get("CPMVALDT", "")).strip()
            vtdd = safe_int(cpmvaldt[0:2]) if len(cpmvaldt) >= 6 else 0
            vtmm = safe_int(cpmvaldt[2:4]) if len(cpmvaldt) >= 6 else 0
            vtyy = safe_int(cpmvaldt[4:8]) if len(cpmvaldt) >= 8 else 0
            vtdd, vtmm, vtyy = clamp_valuation_date(vtdd, vtmm, vtyy, reptday, bkmth, bkyyr, bkday)

            ficode   = safe_int(row.get("FICODE", 0))
            apcode   = safe_int(row.get("APCODE", 0))
            acctno   = safe_int(row.get("ACCTNO", 0))
            ccollno  = safe_int(row.get("CCOLLNO", 0))
            noteno   = safe_int(row.get("NOTENO", 0))
            cinstcl  = safe_str(row.get("CINSTCL", ""))
            oldbrh   = safe_int(row.get("OLDBRH", 0))
            costctr  = safe_int(row.get("COSTCTR", 0))
            aano     = safe_str(row.get("AANO", ""))
            facility = safe_int(row.get("FACILITY", 0))
            faccode  = safe_int(row.get("FACCODE", 0))
            actual_sv = safe_float(row.get("ACTUAL_SALE_VALUE", 0))
            cprforsv  = safe_float(row.get("CPRFORSV", 0))

            line = build_record([
                (1,   fmt_n(ficode, 9)),
                (10,  fmt_n(apcode, 3)),
                (13,  fmt_n(acctno, 10)),
                (23,  fmt_n(ccollno, 11)),
                (36,  fmt_n(noteno, 5)),
                (43,  fmt_s(cinstcl, 2)),
                (45,  fmt_s(cpmdesc1, 1)),
                (46,  fmt_z(vtdd, 2)),
                (48,  fmt_z(vtmm, 2)),
                (50,  fmt_z(vtyy, 4)),
                (54,  fmt_n(oldbrh, 5)),
                (59,  fmt_n(costctr, 4)),
                (63,  fmt_s(aano, 13)),
                (76,  fmt_n(facility, 5)),
                (81,  fmt_n(faccode, 5)),
                (127, fmt_f(actual_sv, 16, 2)),
                (144, fmt_f(cprforsv, 16, 2)),
            ], 200)
            f.write(line + "\n")
    logger.info(f"Written CPLANTMA ({len(cplantma_rows)} records): {CPLANTMA_FILE}")

    # ===========================================================================
    # DATA CCONCESS (COLLATERAL CONCESSION & CONTRACTUAL RIGHTS)
    # ===========================================================================
    with open(CCONCESS_FILE, "w", encoding="latin-1") as f:
        for row in cconcess_rows:
            cccdescr = safe_str(row.get("CCCDESCR", ""))
            cccdesc1 = cccdescr[1:2] if len(cccdescr) >= 2 else " "

            cccvaldt = safe_str(row.get("CCCVALDT", "")).strip()
            vtdd = safe_int(cccvaldt[0:2]) if len(cccvaldt) >= 6 else 0
            vtmm = safe_int(cccvaldt[2:4]) if len(cccvaldt) >= 6 else 0
            vtyy = safe_int(cccvaldt[4:8]) if len(cccvaldt) >= 8 else 0
            vtdd, vtmm, vtyy = clamp_valuation_date(vtdd, vtmm, vtyy, reptday, bkmth, bkyyr, bkday)

            ficode   = safe_int(row.get("FICODE", 0))
            apcode   = safe_int(row.get("APCODE", 0))
            acctno   = safe_int(row.get("ACCTNO", 0))
            ccollno  = safe_int(row.get("CCOLLNO", 0))
            noteno   = safe_int(row.get("NOTENO", 0))
            cinstcl  = safe_str(row.get("CINSTCL", ""))
            oldbrh   = safe_int(row.get("OLDBRH", 0))
            costctr  = safe_int(row.get("COSTCTR", 0))
            aano     = safe_str(row.get("AANO", ""))
            facility = safe_int(row.get("FACILITY", 0))
            faccode  = safe_int(row.get("FACCODE", 0))

            line = build_record([
                (1,  fmt_n(ficode, 9)),
                (10, fmt_n(apcode, 3)),
                (13, fmt_n(acctno, 10)),
                (23, fmt_n(ccollno, 11)),
                (36, fmt_n(noteno, 5)),
                (43, fmt_s(cinstcl, 2)),
                (45, fmt_s(cccdesc1, 1)),
                (46, fmt_z(vtdd, 2)),
                (48, fmt_z(vtmm, 2)),
                (50, fmt_z(vtyy, 4)),
                (54, fmt_n(oldbrh, 5)),
                (59, fmt_n(costctr, 4)),
                (63, fmt_s(aano, 13)),
                (76, fmt_n(facility, 5)),
                (81, fmt_n(faccode, 5)),
            ], 200)
            f.write(line + "\n")
    logger.info(f"Written CCONCESS ({len(cconcess_rows)} records): {CCONCESS_FILE}")

    # ===========================================================================
    # DATA CFINASST (COLLATERAL OTHER FINANCIAL ASSETS)
    # ===========================================================================
    with open(CFINASST_FILE, "w", encoding="latin-1") as f:
        for row in cfinasst_rows:
            cfdvaldt = safe_str(row.get("CFDVALDT", "")).strip()
            vtdd = safe_int(cfdvaldt[0:2]) if len(cfdvaldt) >= 6 else 0
            vtmm = safe_int(cfdvaldt[2:4]) if len(cfdvaldt) >= 6 else 0
            vtyy = safe_int(cfdvaldt[4:8]) if len(cfdvaldt) >= 8 else 0
            vtdd, vtmm, vtyy = clamp_valuation_date(vtdd, vtmm, vtyy, reptday, bkmth, bkyyr, bkday)

            cclassc  = safe_str(row.get("CCLASSC", "")).strip().zfill(3)
            cinstcl  = safe_str(row.get("CINSTCL", ""))
            cfddescr = safe_str(row.get("CFDDESCR", ""))
            if cclassc == "147":
                cinstcl  = "27"
                cfddescr = "21"

            ficode   = safe_int(row.get("FICODE", 0))
            apcode   = safe_int(row.get("APCODE", 0))
            acctno   = safe_int(row.get("ACCTNO", 0))
            ccollno  = safe_int(row.get("CCOLLNO", 0))
            noteno   = safe_int(row.get("NOTENO", 0))
            oldbrh   = safe_int(row.get("OLDBRH", 0))
            costctr  = safe_int(row.get("COSTCTR", 0))
            curcode  = safe_str(row.get("CURCODE", ""))
            collvalx = safe_float(row.get("COLLVALX", 0))
            fdacctno = safe_int(row.get("FDACCTNO", 0))
            fdcdno   = safe_int(row.get("FDCDNO", 0))
            aano     = safe_str(row.get("AANO", ""))
            facility = safe_int(row.get("FACILITY", 0))
            faccode  = safe_int(row.get("FACCODE", 0))
            actual_sv = safe_float(row.get("ACTUAL_SALE_VALUE", 0))
            cprforsv  = safe_float(row.get("CPRFORSV", 0))

            line = build_record([
                (1,   fmt_n(ficode, 9)),
                (10,  fmt_n(apcode, 3)),
                (13,  fmt_n(acctno, 10)),
                (23,  fmt_n(ccollno, 11)),
                (36,  fmt_n(noteno, 5)),
                (43,  fmt_s(cinstcl, 2)),
                (45,  fmt_s(cfddescr[:2], 2)),
                (47,  fmt_z(vtdd, 2)),
                (49,  fmt_z(vtmm, 2)),
                (51,  fmt_z(vtyy, 4)),
                (55,  fmt_n(oldbrh, 5)),
                (60,  fmt_n(costctr, 4)),
                (64,  fmt_s(curcode[:3], 3)),
                (67,  fmt_z(int(collvalx), 16)),
                (83,  fmt_n(fdacctno, 10)),
                (93,  fmt_n(fdcdno, 10)),
                (103, fmt_s(aano, 13)),
                (116, fmt_n(facility, 5)),
                (121, fmt_n(faccode, 5)),
                (127, fmt_f(actual_sv, 16, 2)),
                (144, fmt_f(cprforsv, 16, 2)),
            ], 200)
            f.write(line + "\n")
    logger.info(f"Written CFINASST ({len(cfinasst_rows)} records): {CFINASST_FILE}")

    # ===========================================================================
    # DATA COTHASST (COLLATERAL OTHER ASSETS)
    # ===========================================================================
    with open(COTHASST_FILE, "w", encoding="latin-1") as f:
        for row in cothasst_rows:
            cdebvald = safe_str(row.get("CDEBVALD", "")).strip()
            vtdd = safe_int(cdebvald[0:2]) if len(cdebvald) >= 6 else 0
            vtmm = safe_int(cdebvald[2:4]) if len(cdebvald) >= 6 else 0
            vtyy = safe_int(cdebvald[4:8]) if len(cdebvald) >= 8 else 0
            vtdd, vtmm, vtyy = clamp_valuation_date(vtdd, vtmm, vtyy, reptday, bkmth, bkyyr, bkday)

            cdebdesc_raw = safe_str(row.get("CDEBDESC", ""))
            cdebdesc = cdebdesc_raw[1:2] if len(cdebdesc_raw) >= 2 else " "

            ficode   = safe_int(row.get("FICODE", 0))
            apcode   = safe_int(row.get("APCODE", 0))
            acctno   = safe_int(row.get("ACCTNO", 0))
            ccollno  = safe_int(row.get("CCOLLNO", 0))
            noteno   = safe_int(row.get("NOTENO", 0))
            cinstcl  = safe_str(row.get("CINSTCL", ""))
            oldbrh   = safe_int(row.get("OLDBRH", 0))
            costctr  = safe_int(row.get("COSTCTR", 0))
            aano     = safe_str(row.get("AANO", ""))
            facility = safe_int(row.get("FACILITY", 0))
            faccode  = safe_int(row.get("FACCODE", 0))
            cpresval = safe_float(row.get("CPRESVAL", 0))
            deb_real = safe_float(row.get("DEBENTURE_REALISABLE_VALUE", 0))
            actual_sv = safe_float(row.get("ACTUAL_SALE_VALUE", 0))

            line = build_record([
                (1,   fmt_n(ficode, 9)),
                (10,  fmt_n(apcode, 3)),
                (13,  fmt_n(acctno, 10)),
                (23,  fmt_n(ccollno, 11)),
                (36,  fmt_n(noteno, 5)),
                (43,  fmt_s(cinstcl, 2)),
                (45,  fmt_s(cdebdesc, 1)),
                (46,  fmt_z(vtdd, 2)),
                (48,  fmt_z(vtmm, 2)),
                (50,  fmt_z(vtyy, 4)),
                (54,  fmt_n(oldbrh, 5)),
                (59,  fmt_n(costctr, 4)),
                (63,  fmt_s(aano, 13)),
                (76,  fmt_n(facility, 5)),
                (81,  fmt_n(faccode, 5)),
                (93,  fmt_f(cpresval, 16, 2)),
                (110, fmt_f(deb_real, 16, 2)),
                (127, fmt_f(actual_sv, 16, 2)),
            ], 200)
            f.write(line + "\n")
    logger.info(f"Written COTHASST ({len(cothasst_rows)} records): {COTHASST_FILE}")

    # ===========================================================================
    # DATA CFINGUAR (COLLATERAL FINANCIAL GUARANTEES)
    # ===========================================================================
    with open(CFINGUAR_FILE, "w", encoding="latin-1") as f:
        for row in cfinguar_rows:
            cguarnat_raw = safe_str(row.get("CGUARNAT", ""))
            cguarna1 = cguarnat_raw[1:2] if len(cguarnat_raw) >= 2 else " "

            # FORMAT CGEFFDAT1 CGEXPDAT1 DDMMYYN. (= DDMMYYYY 8-char format)
            cgeffdat = row.get("CGEFFDAT")
            cgexpdat = row.get("CGEXPDAT")
            cgeffdat1 = fmt_ddmmyyn(cgeffdat) if cgeffdat is not None else "        "
            cgexpdat1 = fmt_ddmmyyn(cgexpdat) if cgexpdat is not None else "        "

            ficode   = safe_int(row.get("FICODE", 0))
            apcode   = safe_int(row.get("APCODE", 0))
            acctno   = safe_int(row.get("ACCTNO", 0))
            ccollno  = safe_int(row.get("CCOLLNO", 0))
            noteno   = safe_int(row.get("NOTENO", 0))
            cinstcl  = safe_str(row.get("CINSTCL", ""))
            cguarnam = safe_str(row.get("CGUARNAM", ""))
            cguarid  = safe_str(row.get("CGUARID", ""))
            cguarcty = safe_str(row.get("CGUARCTY", ""))
            oldbrh   = safe_int(row.get("OLDBRH", 0))
            costctr  = safe_int(row.get("COSTCTR", 0))
            cgcgsr   = safe_int(row.get("CGCGSR", 0))
            cgcgur   = safe_int(row.get("CGCGUR", 0))
            aano     = safe_str(row.get("AANO", ""))
            facility = safe_int(row.get("FACILITY", 0))
            curcode  = safe_str(row.get("CURCODE", ""))
            collvalx = safe_float(row.get("COLLVALX", 0))
            faccode  = safe_int(row.get("FACCODE", 0))
            cguarlg  = safe_str(row.get("CGUARLG", ""))
            cgexamtg = safe_float(row.get("CGEXAMTG", 0))
            gfeers   = safe_float(row.get("GFEERS", 0))
            gfeeru   = safe_float(row.get("GFEERU", 0))
            gfamts   = safe_float(row.get("GFAMTS", 0))
            gfamtu   = safe_float(row.get("GFAMTU", 0))
            gcamts   = safe_float(row.get("GCAMTS", 0))
            gcamtu   = safe_float(row.get("GCAMTU", 0))
            guar_ent = safe_str(row.get("GUARANTOR_ENTITY_TYPE", ""))
            guar_brd = safe_str(row.get("GUARANTOR_BIRTH_REGISTER_DT", ""))
            bnmassid = safe_str(row.get("BNMASSIGNID", ""))
            custno   = safe_int(row.get("CUSTNO", 0))
            new_ssm  = safe_str(row.get("NEW_SSM_ID_NO", ""))

            line = build_record([
                (1,   fmt_n(ficode, 9)),
                (10,  fmt_n(apcode, 3)),
                (13,  fmt_n(acctno, 10)),
                (23,  fmt_n(ccollno, 11)),
                (36,  fmt_n(noteno, 5)),
                (43,  fmt_s(cinstcl, 2)),
                (45,  fmt_s(cguarna1, 1)),
                (46,  fmt_s(cguarnam[:40], 40)),
                (196, fmt_s(cguarid[:20], 20)),
                (216, fmt_s(cguarcty[:2], 2)),
                (218, fmt_n(oldbrh, 5)),
                (223, fmt_n(costctr, 4)),
                (227, fmt_n(cgcgsr, 3)),
                (230, fmt_n(cgcgur, 3)),
                (233, fmt_s(aano, 13)),
                (246, fmt_n(facility, 5)),
                (251, fmt_s(curcode[:3], 3)),
                (254, fmt_z(int(collvalx), 16)),
                (270, fmt_n(faccode, 5)),
                (275, fmt_s(cguarlg[:40], 40)),
                (315, cgeffdat1),
                (323, cgexpdat1),
                (331, fmt_f(cgexamtg, 15, 2)),
                (347, fmt_f(gfeers, 6, 2)),
                (354, fmt_f(gfeeru, 6, 2)),
                (361, fmt_f(gfamts, 15, 2)),
                (377, fmt_f(gfamtu, 15, 2)),
                (393, fmt_f(gcamts, 15, 2)),
                (409, fmt_f(gcamtu, 15, 2)),
                (427, fmt_s(guar_ent[:2], 2)),
                (430, fmt_s(guar_brd[:8], 8)),
                (439, fmt_s(bnmassid[:12], 12)),
                (451, fmt_n(custno, 11)),
                (462, fmt_s(new_ssm[:12], 12)),
            ], 500)
            f.write(line + "\n")
    logger.info(f"Written CFINGUAR ({len(cfinguar_rows)} records): {CFINGUAR_FILE}")

    # PROC CONTENTS DATA=WORK._ALL_ NODS (informational only)

    logger.info("EIBDCC5L completed successfully.")


if __name__ == "__main__":
    main()
