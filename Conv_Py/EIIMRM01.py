#!/usr/bin/env python3
"""
Program : EIIMRM01.py
Purpose : Deposits, By Time To Maturity For ALCO
          (Weighted Average Cost By Maturity Profile) - Islamic book.

Dependency:
    %INC PGM(PBBDPFMT); -> from PBBDPFMT import fdprod_format, caprod_format
    FDPROD.  is used (BNMCODE drives FCY vs RM classification / SPTF flag).
    CAPROD.  is used (BNMCODE drives the Islamic CA subset filter).
    SAPROD.  is called in the original SAS ("BNMCODE=PUT(PRODUCT,SAPROD.);")
             but the resulting BNMCODE is never referenced again anywhere in
             the SA data step -- it has zero effect on the output. It is
             therefore intentionally NOT imported/called here; PRODUCT is
             tested directly, exactly as the original logic actually does.

============================================================================
PHYSICAL INPUT DATASETS  (each cached to Parquet independently)
============================================================================
1. main_fd.sas7bdat   (JCL DD DSN=SAP.PIBB.MNITB(0)) (PBB+PIBB combined account master for FD)
   File : INPUT_MAIN_FD_FILE   -> intg_dp_acct_fd.sas7bdat
   Cols used : ACCTNO, ENTITY_CD
   Used to build the list of PIBB-only account numbers, since fd.sas7bdat
   itself is a mixed PBB/PIBB dataset with no ENTITY_CD column.

2. fd.sas7bdat        (JCL //FD  DD DSN=SAP.PIBB.MNIFD(0))
   File : INPUT_FD_FILE        -> enrh_dp_fd_cert.sas7bdat
   Cols used : ACCT_NUM, INTPLAN, CURBAL, RATE, MATDATE, OPENIND
   Used : DATA FD/TD/FDN step. Filtered to PIBB-only rows by inner-joining
          ACCT_NUM against the PIBB ACCTNO list derived from main_fd.sas7bdat.
   MATDATE format : SAS datetime-style string 'DDMONYYYY:HH:MM:SS'
                     (e.g. '25SEP2026:00:00:00'), parsed accordingly.

3. saving.sas7bdat    (JCL //BNM DD DSN=SAP.PIBB.MNITB(0), member SAVING)
   File : INPUT_SAVING_FILE    -> intg_dp_acct_saving.sas7bdat
   Cols used : PRODUCT, OPENIND, CURBAL, INTRATE, ENTITY_CD
   Used : DATA SA step. Filtered directly by ENTITY_CD = 'PIBB'.

4. current.sas7bdat   (JCL //BNM DD DSN=SAP.PIBB.MNITB(0), member CURRENT)
   File : INPUT_CURRENT_FILE   -> intg_dp_acct_current.sas7bdat
   Cols used : PRODUCT, OPENIND, CURBAL, INTRATE, ENTITY_CD
   Used : DATA CA/CAG/CAS step. Filtered directly by ENTITY_CD = 'PIBB'.

Both BNM.SAVING and BNM.CURRENT physically live under the same //BNM DD in
the JCL, but are treated here as two independent physical inputs (own path,
own cache) since they are logically distinct SAS datasets read separately.

============================================================================
OUTPUT
============================================================================
//TEMP DD DSN=SAP.PIBB.EIIMRM01.TEXT, DISP=OLD, DCB=(RECFM=FB,LRECL=256,...)
This is a fixed-name GDG-style catalogued dataset (no date token in the
name), so the Python output file uses a fixed filename, not a dated one.

RECFM=FB (NOT FBA) on the //TEMP DD means this particular report carries
NO ASA carriage-control byte (per project convention: RECFM=FBA implies
ASA control, RECFM=FB does not). The output below is therefore plain
fixed-width text; page boundaries (PAGESIZE=60, not otherwise specified in
the SAS source) are marked with a form-feed character instead of an ASA
'1' byte.
"""

import gc
from pathlib import Path
from datetime import date

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values
from PBBDPFMT import fdprod_format, caprod_format

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# BASE_DIR = Path("/sas/deposit/dwh")

BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

# INPUT_MAIN_FD_DIR = BASE_DIR / "input" / "prod" / "EIIMRM01" / "MAIN_FD"
# INPUT_FD_DIR      = BASE_DIR / "enrichment"
# INPUT_SAVING_DIR  = BASE_DIR / "integration"
# INPUT_CURRENT_DIR = BASE_DIR / "integration"

INPUT_MAIN_FD_DIR = STG_DIR / "sasdata"
INPUT_FD_DIR      = STG_DIR / "sasdata"
INPUT_SAVING_DIR  = STG_DIR / "sasdata"
INPUT_CURRENT_DIR = STG_DIR / "sasdata"

INPUT_MAIN_FD_FILE = INPUT_MAIN_FD_DIR / "intg_dp_acct_fd_d19.sas7bdat"
INPUT_FD_FILE      = INPUT_FD_DIR / "enrh_dp_fd_cert_d19.sas7bdat"
INPUT_SAVING_FILE  = INPUT_SAVING_DIR / "intg_dp_acct_saving_d19.sas7bdat"
INPUT_CURRENT_FILE = INPUT_CURRENT_DIR / "intg_dp_acct_current_d19.sas7bdat"

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIIMRPTS"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR  = BASE_DIR / "output" / "EIIMRPTS"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "EIIMRM01.txt"

CHUNK_ROWS = 500_000
PAGE_SIZE  = 60          # lines per page (not specified in SAS -> default)

# ============================================================================
# STEP 1: REPORT DATE  (no reptdate.parquet -- derive from REPTDATE.py)
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values(year_format="%Y")
reptdate = reptdate_values.reptdate

# NOWK in the original SAS is derived by exact-day matching
# (day=8/15/22 else 4), which differs from REPTDATE.py's range-based NOWK.
# NOWK is set via CALL SYMPUT but is never referenced again anywhere else
# in this program, so it has no effect on the report and is only kept here
# for documentation parity with the SAS source.
_day = reptdate.day
NOWK = "1" if _day == 8 else "2" if _day == 15 else "3" if _day == 22 else "4"

REPTYRS  = reptdate.strftime("%y")
REPTYEAR = reptdate.strftime("%Y")
REPTMON  = reptdate.strftime("%m")
REPTDAY  = reptdate.strftime("%d")
RDATE    = reptdate.strftime("%d/%m/%y")          # PUT(REPTDATE,DDMMYY8.)

RPYR, RPMTH, RPDAY = reptdate.year, reptdate.month, reptdate.day

# DCLVAR macro's RD1-RD12 array (days-per-month for the report year), used
# by %REMMTH. D1-D12 (LDAY) and MD1-MD12 (MDDAYS) are also RETAINed/declared
# by the original DCLVAR macro but are never referenced anywhere else in the
# program body -- they are dead declarations, omitted here.
RD_DAYS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
if RPYR % 4 == 0:
    RD_DAYS[1] = 29

print(f"  RDATE        : {RDATE}")
print(f"  REPTMON/DAY  : {REPTMON}/{REPTDAY}  REPTYEAR: {REPTYEAR}")
print(f"  Output file  : {OUTPUT_FILE.name}")

# ============================================================================
# PROC FORMAT EQUIVALENTS (local to this program)
# ============================================================================
_REMFMT_ORDER = [
    "       ", ">  0-1 MTH", ">  1-2 MTHS", ">  2-3 MTHS", ">  3-4 MTHS",
    ">  4-5 MTHS", ">  5-6 MTHS", ">  6-7 MTHS", ">  7-8 MTHS", ">  8-9 MTHS",
    ">  9-10 MTHS", "> 10-11 MTHS", "> 11-12 MTHS", "> 12-13 MTHS", "> 13-14 MTHS",
    "> 14-15 MTHS", "> 15-16 MTHS", "> 16-17 MTHS", "> 17-18 MTHS", "> 18-19 MTHS",
    "> 19-20 MTHS", "> 20-21 MTHS", "> 21-22 MTHS", "> 22-23 MTHS", "> 23-24 MTHS",
    ">2-3 YRS", ">3-4 YRS", ">4-5 YRS",
    " 1 MONTH", " 3 MONTHS", " 6 MONTHS", " 9 MONTHS", "12 MONTHS", "15 MONTHS",
    "ABOVE 15 MONTHS", "OVERDUE FD", "  ",
]
_REMFMT_ORDER_INDEX = {label: i for i, label in enumerate(_REMFMT_ORDER)}
_REMFMT_ORDER_INDEX["SUB-TOTAL"] = len(_REMFMT_ORDER) + 1


def remfmt_format(value):
    """PROC FORMAT VALUE REMFMT. SAS numeric missing sorts as LOW, so a
    None value (SAS missing) falls into the LOW-0 (blank) bucket. Ranges
    are checked in the same order as the SAS format definition, so an
    exact boundary value (e.g. 1.0) matches the FIRST listed range."""
    if value is None or value <= 0:
        return "       "
    if value <= 1:
        return ">  0-1 MTH"
    if value <= 2:
        return ">  1-2 MTHS"
    if value <= 3:
        return ">  2-3 MTHS"
    if value <= 4:
        return ">  3-4 MTHS"
    if value <= 5:
        return ">  4-5 MTHS"
    if value <= 6:
        return ">  5-6 MTHS"
    if value <= 7:
        return ">  6-7 MTHS"
    if value <= 8:
        return ">  7-8 MTHS"
    if value <= 9:
        return ">  8-9 MTHS"
    if value <= 10:
        return ">  9-10 MTHS"
    if value <= 11:
        return "> 10-11 MTHS"
    if value <= 12:
        return "> 11-12 MTHS"
    if value <= 13:
        return "> 12-13 MTHS"
    if value <= 14:
        return "> 13-14 MTHS"
    if value <= 15:
        return "> 14-15 MTHS"
    if value <= 16:
        return "> 15-16 MTHS"
    if value <= 17:
        return "> 16-17 MTHS"
    if value <= 18:
        return "> 17-18 MTHS"
    if value <= 19:
        return "> 18-19 MTHS"
    if value <= 20:
        return "> 19-20 MTHS"
    if value <= 21:
        return "> 20-21 MTHS"
    if value <= 22:
        return "> 21-22 MTHS"
    if value <= 23:
        return "> 22-23 MTHS"
    if value <= 24:
        return "> 23-24 MTHS"
    if value <= 36:
        return ">2-3 YRS"
    if value <= 48:
        return ">3-4 YRS"
    if value <= 60:
        return ">4-5 YRS"
    if value == 91:
        return " 1 MONTH"
    if value == 92:
        return " 3 MONTHS"
    if value == 93:
        return " 6 MONTHS"
    if value == 94:
        return " 9 MONTHS"
    if value == 95:
        return "12 MONTHS"
    if value == 96:
        return "15 MONTHS"
    if value == 97:
        return "ABOVE 15 MONTHS"
    if value == 99:
        return "OVERDUE FD"
    return "  "


def _remfmt_sort_key(label: str) -> int:
    return _REMFMT_ORDER_INDEX.get(label, len(_REMFMT_ORDER))


_SUBTTL_LABELS = {
    "A": "REMAINING MATURITY",
    "B": "OVERDUE FD",
    "C": "NEW FD FOR THE MONTH",
    "D1": "SAVING ACCOUNTS  ",
    "D2": "WADIAH SAVING A/C",
    "E1": "NORMAL CURRENT A/C",
    "E2": "WADIAH CURRENT A/C",
    "E3": "FCY CURRENT A/C",
    "E4": "OD A/C",
    "F1": "INT-BEAR. GOV.  ACCT",
    "F2": "INT-BEAR. HSING ACCT",
    "F3": "ACE < 5K            ",
    "F4": "ACE > 5K            ",
    "F5": "VOSTRO LOCAL        ",
    "F6": "VOSTRO FOREIGN      ",
    "F7": "PB SHARE LINK       ",
    "H": "PORTION FROM ACE ACC",
    "I": "SUB-TOTAL",
}


def subttl_format(code):
    if code is None:
        return ""
    return _SUBTTL_LABELS.get(code, code)


# TERMFMT is a PROC FORMAT declared locally inside this program and is a
# DIFFERENT value set from PBBDPFMT's FCYTERM format -- it must not be
# confused with / substituted by fcyterm_format().
_TERMFMT_1 = {470, 471, 476, 477, 482, 483, 488, 489, 494, 495, 548, 549, 554, 555}
_TERMFMT_3 = {472, 473, 478, 479, 484, 485, 490, 491, 496, 497, 550, 551, 556, 557}
_TERMFMT_6 = {474, 475, 480, 481, 486, 487, 492, 493, 498, 499, 552, 553, 558, 559}


def termfmt_format(intplan):
    if intplan is None:
        return None
    if intplan in _TERMFMT_1:
        return 1
    if intplan in _TERMFMT_3:
        return 3
    if intplan in _TERMFMT_6:
        return 6
    return None


def _sas_round(x: float) -> float:
    """SAS ROUND() with no scale argument: round to nearest integer,
    halves away from zero."""
    if x >= 0:
        return float(int(x + 0.5))
    return float(-int(-x + 0.5))


_MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


# def _parse_matdate(matdate) -> date:
#     """MATDATE is now sourced as a SAS datetime-style string of the form
#     'DDMONYYYY:HH:MM:SS' (e.g. '25SEP2026:00:00:00'). Only the date
#     portion (before the first ':') is relevant here; the time component
#     is always 00:00:00 and is discarded, matching the original SAS
#     MATDT derivation which only ever used the date value."""
#     date_part = str(matdate).split(":")[0].strip().upper()
#     day = int(date_part[0:2])
#     mon = _MONTH_MAP[date_part[2:5]]
#     year = int(date_part[5:9])
#     return date(year, mon, day)


def _parse_matdate(matdate) -> date:
    """

    Parse either:
      - 'DDMONYYYY:HH:MM:SS'   (SAS datetime string)
      - 'YYYY-MM-DD'           (ISO date)
      - 'YYYY-MM-DD HH:MM:SS'  (ISO with time)
    """
    s = str(matdate).strip().upper()
    
    # If the string contains '-' it's likely ISO format (YYYY-MM-DD)
    if '-' in s:
        # Split by '-' to get year, month, day
        parts = s.split('-')
        year = int(parts[0])
        mon = int(parts[1])
        # The day part might have a trailing time, e.g., "26 00:00:00"
        day_part = parts[2].split()[0]  # take only the date part
        day = int(day_part)
        return date(year, mon, day)
    
    # Otherwise assume SAS datetime format 'DDMONYYYY:HH:MM:SS'
    date_part = s.split(":")[0]           # e.g., "25SEP2026"
    day = int(date_part[0:2])
    mon = _MONTH_MAP[date_part[2:5]]      # "SEP" -> 9
    year = int(date_part[5:9])
    return date(year, mon, day)


def _remmth(matdt: date) -> float:
    """%REMMTH macro."""
    mdyr, mdmth, mdday = matdt.year, matdt.month, matdt.day
    days_in_rpmth = RD_DAYS[RPMTH - 1]
    if mdday > days_in_rpmth:
        mdday = days_in_rpmth
    remy = mdyr - RPYR
    remm = mdmth - RPMTH
    remd = mdday - RPDAY
    return remy * 12 + remm + remd / days_in_rpmth


# ============================================================================
# HELPER: CACHE STAMP + STREAM .sas7bdat -> PARQUET  (EIBDLN1M.py pattern)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


def _sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0

    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
    for chunk in reader:
        # Build schema from the first chunk's dtypes
        if schema is None:
            fields = []
            for col, dtype in chunk.dtypes.items():
                if dtype == 'object':
                    pa_type = pa.string()          # treat all object columns as string
                elif pd.api.types.is_integer_dtype(dtype):
                    pa_type = pa.int64()
                elif pd.api.types.is_float_dtype(dtype):
                    pa_type = pa.float64()
                else:
                    # fallback (e.g., datetime, bool)
                    pa_type = pa.from_numpy_dtype(dtype)
                fields.append(pa.field(col, pa_type))
            schema = pa.schema(fields)
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")

        # Convert chunk to PyArrow Table using the fixed schema
        table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)
        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer:
        writer.close()
    print(f"  [{tag}] Done - {total:,} rows cached.")


def _load_cached(sas_path: Path, tag: str) -> Path:
    cache_path = CACHE_DIR / f"{sas_path.stem}.parquet"
    if _cache_is_fresh(sas_path, cache_path):
        print(f"  [{tag}] Cache fresh - skipping conversion.")
    else:
        _sas_to_parquet(sas_path, cache_path, tag)
    return cache_path


# ============================================================================
# STEP 2: CACHE INPUT SAS FILES TO PARQUET
# ============================================================================
print("\nStep 2: Caching input SAS datasets to Parquet...")
MAIN_FD_CACHE = _load_cached(INPUT_MAIN_FD_FILE, "MAIN_FD")
FD_CACHE      = _load_cached(INPUT_FD_FILE, "FD")
SAVING_CACHE  = _load_cached(INPUT_SAVING_FILE, "SAVING")
CURRENT_CACHE = _load_cached(INPUT_CURRENT_FILE, "CURRENT")

# ============================================================================
# STEP 3: BUILD FD / TD / FDN  (DATA FD TD FDN; SET FD.FD; ...)
# ============================================================================
print("\nStep 3: Building FD / TD / FDN from FD.FD (PIBB-only via main_fd)...")

con = duckdb.connect(database=":memory:")
fd_raw = con.execute(f"""
    WITH main_fd_pibb AS (
        SELECT DISTINCT CAST(ACCTNO AS BIGINT) AS ACCTNO
        FROM read_parquet('{MAIN_FD_CACHE.as_posix()}')
        WHERE ENTITY_CD = 'PIBB'
    )
    SELECT
        CAST(f.INT_PLAN     AS INTEGER) AS INTPLAN,
        CAST(f.CURR_BAL     AS DOUBLE)  AS CURBAL,
        CAST(f.RT           AS DOUBLE)  AS RATE,
        CAST(f.MATURE_DT    AS DATE) AS MATDATE,
        CAST(f.OPEN_IND     AS VARCHAR) AS OPENIND
    FROM read_parquet('{FD_CACHE.as_posix()}') f
    INNER JOIN main_fd_pibb m
        ON CAST(f.ACCT_NUM AS BIGINT) = m.ACCTNO
""").pl()

# DEBUG
print(f"  FD Parquet schema columns:", pq.read_schema(FD_CACHE).names)
print(f"  FD raw columns: {fd_raw.columns}")
print(f"  First 3 rows of fd_raw (sample):")
print(fd_raw.head(3))

con.close()

print(f"  FD rows after PIBB account filter: {len(fd_raw):,}")

fcy_count = sum(1 for r in fd_raw.iter_rows(named=True) if fdprod_format(r['INTPLAN']) == '42630')
print(f"  FCY deposits in FD: {fcy_count}")

def _row(prodtyp, subtyp, subttl, amount, cost, remmth, remm):
    return {"PRODTYP": prodtyp, "SUBTYP": subtyp, "SUBTTL": subttl,
            "AMOUNT": amount, "COST": cost, "REMMTH": remmth, "REMM": remm}


fd_rows, td_rows, fdn_rows = [], [], []

for r in fd_raw.iter_rows(named=True):
    intplan = r["INTPLAN"]
    curbal  = r["CURBAL"]
    rate    = r["RATE"] or 0.0
    openind = r["OPENIND"]

    if curbal is None:
        continue

    bnmcode = fdprod_format(intplan)
    term = None   # RETAIN-less: reset to missing every DATA-step iteration
    if bnmcode == "42630":
        prodtyp = "FIXED DEPT(FCY)"
        term = termfmt_format(intplan)
    else:
        prodtyp = "FIXED DEPT(RM)"

    # IF OPENIND = 'O' OR OPENIND = 'D' AND CURBAL > 0  (AND binds tighter)
    if openind == "O" or (openind == "D" and curbal > 0):
        # matdt = _parse_matdate(r["MATDATE"])
        matdt = r["MATDATE"]

        if openind == "D" or matdt < reptdate:
            subtyp = "SPTF" if bnmcode == "42132" else "CONVENTIONAL"
            cost = curbal * rate
            td_rows.append(_row(prodtyp, subtyp, "B", curbal, cost, 99.0, None))
        else:
            remmth = _remmth(matdt)
            subtyp = "SPTF" if bnmcode == "42132" else "CONVENTIONAL"
            cost = curbal * rate
            remm = curbal * remmth
            fd_rows.append(_row(prodtyp, subtyp, "A", curbal, cost, remmth, remm))

            # IF ((TERM - REMMTH) < 1) THEN DO ... OUTPUT FDN; END;
            # TERM is un-RETAINed and only ever assigned for FCY deposits
            # (BNMCODE='42630'); for RM deposits TERM stays missing here.
            # SAS treats a missing numeric operand as smaller than any real
            # number, so (missing - REMMTH) < 1 is ALWAYS TRUE for RM
            # deposits -- this branch fires for every RM FD row, not only
            # ones maturing within the current month. Preserved as-is.
            fires = (term is None) or ((term - remmth) < 1)
            if fires:
                cost2 = curbal * rate
                remm2 = remmth * curbal
                remmth2 = term   # REMMTH = TERM (missing for RM deposits)
                fdn_rows.append(_row(prodtyp, subtyp, "C", curbal, cost2, remmth2, remm2))

print(f"  FD  rows: {len(fd_rows):,}   TD rows: {len(td_rows):,}   FDN rows: {len(fdn_rows):,}")

# ============================================================================
# STEP 4: BUILD SA  (DATA SA; SET BNM.SAVING; ...)
# ============================================================================
print("\nStep 4: Building SA from BNM.SAVING (PIBB only)...")

con = duckdb.connect(database=":memory:")
saving_raw = con.execute(f"""
    SELECT
        CAST(PRODUCT AS INTEGER) AS PRODUCT,
        CAST(OPENIND AS VARCHAR) AS OPENIND,
        CAST(CURBAL  AS DOUBLE)  AS CURBAL,
        CAST(INTRATE AS DOUBLE)  AS INTRATE
    FROM read_parquet('{SAVING_CACHE.as_posix()}')
    WHERE ENTITY_CD = 'PIBB'
""").pl()
con.close()

sa_rows = []
_REMMTH_SA = 0.0   # RETAIN ... REMMTH 0

for r in saving_raw.iter_rows(named=True):
    openind = r["OPENIND"]
    curbal  = r["CURBAL"]
    if curbal is None or openind in ("B", "C", "P") or curbal < 0:
        continue
    product = r["PRODUCT"]
    intrate = r["INTRATE"] or 0.0
    if product in (204, 214, 215):
        subtyp, subttl = "SPTF", "D2"
    else:
        subtyp, subttl = "CONVENTIONAL", "D1"
    cost = curbal * intrate
    remm = _REMMTH_SA * curbal
    sa_rows.append(_row("SAVINGS DEPOSIT", subtyp, subttl, curbal, cost, _REMMTH_SA, remm))

print(f"  SA rows: {len(sa_rows):,}")

# ============================================================================
# STEP 5: BUILD CA / CAG / CAS  (DATA CA CAG CAS; SET BNM.CURRENT; ...)
# ============================================================================
print("\nStep 5: Building CA / CAG / CAS from BNM.CURRENT (PIBB only)...")

con = duckdb.connect(database=":memory:")
current_raw = con.execute(f"""
    SELECT
        CAST(PRODUCT AS INTEGER) AS PRODUCT,
        CAST(OPENIND AS VARCHAR) AS OPENIND,
        CAST(CURBAL  AS DOUBLE)  AS CURBAL,
        CAST(INTRATE AS DOUBLE)  AS INTRATE
    FROM read_parquet('{CURRENT_CACHE.as_posix()}')
    WHERE ENTITY_CD = 'PIBB'
""").pl()
con.close()

ca_rows, cag_rows, cas_rows = [], [], []
_REMMTH_CA = 0.0   # RETAIN REMMTH 0

for r in current_raw.iter_rows(named=True):
    product = r["PRODUCT"]
    bnmcode = caprod_format(product)

    # IF BNMCODE='42310' OR BNMCODE='42180' OR PRODUCT=166;  (subsetting IF)
    if not (bnmcode == "42310" or bnmcode == "42180" or product == 166):
        continue

    openind = r["OPENIND"]
    curbal  = r["CURBAL"]
    if curbal is None:
        continue
    intrate = r["INTRATE"] or 0.0

    if openind not in ("B", "C", "P") and bnmcode != "N":
        if curbal > 0:
            if product in (101, 103, 161, 163):
                subtyp = "CONVENTIONAL" if product in (101, 103) else "SPTF"
                subttl = "F1" if product in (101, 161) else "F2"
                cost = curbal * intrate
                remm = curbal * _REMMTH_CA
                cag_rows.append(_row("DEMAND DEPOSIT", subtyp, subttl, curbal, cost, _REMMTH_CA, remm))
            elif product in (150, 151, 152, 181):
                subtyp = "CONVENTIONAL"
                if curbal <= 5000:
                    ca_rows.append(_row("DEMAND DEPOSIT", subtyp, "F3", curbal, 0.0, _REMMTH_CA, None))
                else:
                    curbal2 = curbal - 5000
                    cost2 = curbal2 * intrate
                    remm2 = curbal2 * _REMMTH_CA
                    cas_rows.append(_row("SAVINGS DEPOSIT", subtyp, "H", curbal2, cost2, _REMMTH_CA, remm2))
                    curbal3 = 5000.0
                    remm3 = curbal3 * _REMMTH_CA
                    ca_rows.append(_row("DEMAND DEPOSIT", subtyp, "F4", curbal3, 0.0, _REMMTH_CA, remm3))
            elif product in (60, 61, 62, 63, 64, 160, 162, 164, 165, 166, 182):
                cost = curbal * intrate
                remm = curbal * _REMMTH_CA
                ca_rows.append(_row("DEMAND DEPOSIT", "SPTF", "E2", curbal, cost, _REMMTH_CA, remm))
            elif 400 <= product <= 410:
                cost = curbal * intrate
                remm = curbal * _REMMTH_CA
                ca_rows.append(_row("DEMAND DEPOSIT", "CONVENTIONAL", "E3", curbal, cost, _REMMTH_CA, remm))
            elif product in (104, 105, 177, 189, 190, 178):
                subttl = "F5" if product == 104 else "F6" if product == 105 else "F7"
                cost = curbal * intrate
                remm = curbal * _REMMTH_CA
                ca_rows.append(_row("DEMAND DEPOSIT", "CONVENTIONAL", subttl, curbal, cost, _REMMTH_CA, remm))
            elif product not in (101, 104, 105, 107, 113, 150, 151, 152, 178, 189, 190):
                cost = curbal * intrate
                remm = curbal * _REMMTH_CA
                ca_rows.append(_row("DEMAND DEPOSIT", "CONVENTIONAL", "E1", curbal, cost, _REMMTH_CA, remm))

        if curbal <= 0:
            cost = curbal * intrate
            remm = curbal * _REMMTH_CA
            ca_rows.append(_row("DEMAND DEPOSIT", "SPTF", "E4", curbal, cost, _REMMTH_CA, remm))

print(f"  CA rows: {len(ca_rows):,}   CAG rows: {len(cag_rows):,}   CAS rows: {len(cas_rows):,}")

del fd_raw, saving_raw, current_raw
gc.collect()

# ============================================================================
# STEP 6: PROC SUMMARY (per-source) -- CLASS PRODTYP SUBTYP SUBTTL REMMTH
# ============================================================================
print("\nStep 6: Summarising TD / FD / FDN / SA / CA / CAG / CAS...")


def _group_sum(rows, key_fields, sum_fields=("AMOUNT", "COST", "REMM")):
    """PROC SUMMARY NWAY; CLASS <key_fields>; VAR <sum_fields>; SUM=;
    PROC SUMMARY's SUM statistic ignores missing values rather than
    propagating them; if every contributing value is missing the result
    stays missing (None)."""
    groups = {}
    for r in rows:
        key = tuple(r.get(f) for f in key_fields)
        g = groups.setdefault(key, {f: None for f in sum_fields})
        for f in sum_fields:
            v = r.get(f)
            if v is not None:
                g[f] = (g[f] or 0.0) + v
    out = []
    for key, sums in groups.items():
        rec = dict(zip(key_fields, key))
        rec.update(sums)
        out.append(rec)
    return out


def _proc_summary_by_bucket(rows, allow_missing: bool):
    """CLASS PRODTYP SUBTYP SUBTTL REMMTH; FORMAT REMMTH REMFMT.;
    Without the MISSING option (the default -- only TD uses MISSING), SAS
    PROC SUMMARY drops any observation with a missing CLASS value before
    summarising. For FDN this removes every RM-deposit row (TERM/REMMTH is
    always missing there), so only FCY 'NEW FD FOR THE MONTH' rows survive
    into the aggregated FD dataset -- a deliberate preservation of the
    original SAS behaviour."""
    filtered = [r for r in rows if allow_missing or r["REMMTH"] is not None]
    for r in filtered:
        r["REMMTH_BKT"] = remfmt_format(r["REMMTH"])
    grouped = _group_sum(filtered, ["PRODTYP", "SUBTYP", "SUBTTL", "REMMTH_BKT"])
    for g in grouped:
        g["REMMTH1"] = None
    return grouped


td_summary  = _proc_summary_by_bucket(td_rows,  allow_missing=True)
fd_summary  = _proc_summary_by_bucket(fd_rows,  allow_missing=False)
fdn_summary = _proc_summary_by_bucket(fdn_rows, allow_missing=False)
sa_summary  = _proc_summary_by_bucket(sa_rows,  allow_missing=False)
ca_summary  = _proc_summary_by_bucket(ca_rows,  allow_missing=False)
cag_summary = _proc_summary_by_bucket(cag_rows, allow_missing=False)
cas_summary = _proc_summary_by_bucket(cas_rows, allow_missing=False)

print(f"  FDN summary rows after MISSING-option filter: {len(fdn_summary):,} "
      f"(RM deposits excluded because TERM/REMMTH is missing there)")

# ============================================================================
# STEP 7: DATA FD; SET TD FD FDN; REMMTH1 = PUT(REMMTH,REMFMT.);
#         PROC SORT; BY PRODTYP SUBTTL SUBTYP REMMTH1;
# ============================================================================
print("\nStep 7: Combining TD+FD+FDN and computing REMMTH1...")

fd_combined = []
for src in (td_summary, fd_summary, fdn_summary):
    for r in src:
        fd_combined.append({**r, "REMMTH1": r["REMMTH_BKT"]})
fd_combined.sort(key=lambda r: (r["PRODTYP"], r["SUBTTL"], r["SUBTYP"], r["REMMTH1"]))

# ----------------------------------------------------------------------
# DATA DUMMY; ... WHERE SUBTTL IN ('A','C','E3'); (E3 never occurs among
# TD/FD/FDN records, so only 'A' and 'C' apply in practice.) For the first
# occurrence of each PRODTYP/SUBTTL/SUBTYP group, emit a placeholder for
# every month 1-60 so all maturity buckets are represented in the report
# even when no deposits fall into them.
# ----------------------------------------------------------------------
seen_groups = set()
dummy_rows = []
for r in fd_combined:
    if r["SUBTTL"] not in ("A", "C", "E3"):
        continue
    key = (r["PRODTYP"], r["SUBTTL"], r["SUBTYP"])
    if key in seen_groups:
        continue
    seen_groups.add(key)
    for m in range(1, 61):
        bkt = remfmt_format(float(m))
        dummy_rows.append({
            "PRODTYP": r["PRODTYP"], "SUBTTL": r["SUBTTL"], "SUBTYP": r["SUBTYP"],
            "REMMTH_BKT": bkt, "REMMTH1": bkt, "AMOUNT": None, "COST": None, "REMM": None,
        })

# PROC SORT DATA=DUMMY NODUPKEYS; BY PRODTYP SUBTTL SUBTYP REMMTH1;
dummy_by_key = {}
for r in dummy_rows:
    key = (r["PRODTYP"], r["SUBTTL"], r["SUBTYP"], r["REMMTH1"])
    dummy_by_key.setdefault(key, r)

# DATA FD; MERGE FD DUMMY; BY PRODTYP SUBTTL SUBTYP REMMTH1;
# DUMMY carries no AMOUNT/COST/REMM of its own for keys that already exist
# in fd_combined, so real records are never overwritten -- only genuinely
# absent buckets get added as zero-data placeholder rows.
existing_keys = {(r["PRODTYP"], r["SUBTTL"], r["SUBTYP"], r["REMMTH1"]) for r in fd_combined}
fd_final = list(fd_combined)
for key, r in dummy_by_key.items():
    if key not in existing_keys:
        fd_final.append(r)

print(f"  FD (final) rows after DUMMY padding: {len(fd_final):,}")

# ============================================================================
# STEP 8: DATA DEP; SET FD SA CA CAG CAS;
# ============================================================================
print("\nStep 8: Building DEP...")

dep_rows = []
for r in fd_final:
    dep_rows.append({
        "PRODTYP": r["PRODTYP"], "SUBTYP": r["SUBTYP"], "SUBTTL": r["SUBTTL"],
        "AMOUNT": r["AMOUNT"], "COST": r["COST"], "REMM": r["REMM"],
        "REMMTH_BKT": r["REMMTH_BKT"], "REMMTH1": r["REMMTH1"],
    })
for src in (sa_summary, ca_summary, cag_summary, cas_summary):
    for r in src:
        dep_rows.append({
            "PRODTYP": r["PRODTYP"], "SUBTYP": r["SUBTYP"], "SUBTTL": r["SUBTTL"],
            "AMOUNT": r["AMOUNT"], "COST": r["COST"], "REMM": r["REMM"],
            "REMMTH_BKT": r["REMMTH_BKT"], "REMMTH1": None,
        })

# ============================================================================
# STEP 9: DATA DEP; SET DEP;  (dangling-ELSE bug preserved exactly)
# ============================================================================
print("\nStep 9: Applying DEP transform...")

dep2_rows = []
for r in dep_rows:
    subtyp = r["SUBTYP"]
    amount = r["AMOUNT"]          # keep None (missing) distinct from 0.0
    cost   = r["COST"]
    remm   = r["REMM"]

    # IF SUBTYP IN ('SPTF','CONVENTIONAL') THEN
    #    IF AMOUNT > 0 THEN WACOST = COST / AMOUNT; ELSE WACOST = 0;
    # -- only this nested IF-ELSE is scoped to the outer condition; every
    # statement below runs UNCONDITIONALLY for every row regardless of
    # SUBTYP (a dangling-ELSE scoping artefact in the original SAS),
    # preserved here exactly as written.
    if subtyp in ("SPTF", "CONVENTIONAL"):
        if amount is not None and amount > 0:
            wacost = (cost or 0.0) / amount
        else:
            wacost = 0.0   # SAS: missing AMOUNT is not > 0 -> ELSE branch
    else:
        wacost = None

    waremm = (remm / amount) if (remm is not None and amount not in (None, 0)) else None
    amount_k = None if amount is None else _sas_round(amount / 1000)

    prodtyp = r["PRODTYP"]
    if r["SUBTTL"] in ("E1", "E2", "E3"):
        prodtyp = "CA NON-INT BEARING"
    elif r["SUBTTL"] in ("F1", "F2", "F3", "F4", "F5", "F6", "F7"):
        prodtyp = "CA INT BEARING"

    dep2_rows.append({
        "PRODTYP": prodtyp, "SUBTYP": subtyp, "SUBTTL": r["SUBTTL"],
        "AMOUNT": amount_k, "COST": cost, "REMM": remm,
        "REMMTH_BKT": r["REMMTH_BKT"], "REMMTH1": r["REMMTH1"],
        "WACOST": wacost, "WAREMM": waremm,
    })


def _wacost_waremm(amount_k, cost, remm):
    """WACOST = COST / ROUND(AMOUNT*1000); WAREMM = REMM / ROUND(AMOUNT*1000).
    Unconditional division at these aggregate levels: a missing AMOUNT
    propagates to a missing result, it is NOT special-cased to zero the way
    the row-level WACOST assignment above is."""
    if amount_k is None:
        return None, None
    denom = _sas_round(amount_k * 1000)
    wacost = (cost / denom) if (cost is not None and denom != 0) else None
    waremm = (remm / denom) if (remm is not None and denom != 0) else None
    return wacost, waremm


# ============================================================================
# STEP 10: DEPTOTAL / DEPFINAL / DEPTOTA2  (non-FD table source)
# ============================================================================
print("\nStep 10: Building DEPFINAL (non-FD PRODTYP)...")

deptotal = _group_sum(dep2_rows, ["PRODTYP", "SUBTTL", "REMMTH_BKT"])
deptotal_rows = []
for r in deptotal:
    wacost, waremm = _wacost_waremm(r["AMOUNT"], r["COST"], r["REMM"])
    deptotal_rows.append({
        "PRODTYP": r["PRODTYP"], "SUBTTL": r["SUBTTL"], "REMMTH_BKT": r["REMMTH_BKT"],
        "SUBTYP": "TOTAL", "AMOUNT": r["AMOUNT"], "COST": r["COST"], "REMM": r["REMM"],
        "WACOST": wacost, "WAREMM": waremm,
    })

depfinal_rows = [
    {k: r[k] for k in ("PRODTYP", "SUBTTL", "REMMTH_BKT", "SUBTYP", "AMOUNT", "COST", "REMM", "WACOST", "WAREMM")}
    for r in dep2_rows
] + deptotal_rows

deptota2 = _group_sum(depfinal_rows, ["PRODTYP", "SUBTYP"])
deptota2_rows = []
for r in deptota2:
    wacost, waremm = _wacost_waremm(r["AMOUNT"], r["COST"], r["REMM"])
    deptota2_rows.append({
        "PRODTYP": r["PRODTYP"], "SUBTYP": r["SUBTYP"], "SUBTTL": "I", "REMMTH_BKT": None,
        "AMOUNT": r["AMOUNT"], "COST": r["COST"], "REMM": r["REMM"],
        "WACOST": wacost, "WAREMM": waremm,
    })

depfinal_all = depfinal_rows + deptota2_rows
depfinal_all = [r for r in depfinal_all
                if r["PRODTYP"] not in ("FIXED DEPT(RM)", "FIXED DEPT(FCY)")]
print(f"  DEPFINAL rows: {len(depfinal_all):,}")

# ============================================================================
# STEP 11: DEPTOTAL(REMMTH1) / FDTOTAL / FDTOTA2  (FD-only table source)
# ============================================================================
print("\nStep 11: Building FD (FIXED DEPT RM/FCY) table...")

deptotal_r1 = _group_sum(dep2_rows, ["PRODTYP", "SUBTTL", "REMMTH1"])

fdtotal_rows = []
for r in deptotal_r1:
    if r["PRODTYP"] not in ("FIXED DEPT(RM)", "FIXED DEPT(FCY)"):
        continue
    wacost, waremm = _wacost_waremm(r["AMOUNT"], r["COST"], r["REMM"])
    fdtotal_rows.append({
        "PRODTYP": r["PRODTYP"], "SUBTTL": r["SUBTTL"], "REMMTH1": r["REMMTH1"],
        "SUBTYP": "TOTAL", "AMOUNT": r["AMOUNT"], "COST": r["COST"], "REMM": r["REMM"],
        "WACOST": wacost, "WAREMM": waremm,
    })

fdtotal_combined = [
    {k: r[k] for k in ("PRODTYP", "SUBTTL", "REMMTH1", "SUBTYP", "AMOUNT", "COST", "REMM", "WACOST", "WAREMM")}
    for r in dep2_rows if r["PRODTYP"] in ("FIXED DEPT(RM)", "FIXED DEPT(FCY)")
] + fdtotal_rows

fdtota2 = _group_sum(fdtotal_combined, ["PRODTYP", "SUBTTL", "SUBTYP"])
fdtota2_rows = []
for r in fdtota2:
    wacost, waremm = _wacost_waremm(r["AMOUNT"], r["COST"], r["REMM"])
    fdtota2_rows.append({
        "PRODTYP": r["PRODTYP"], "SUBTTL": r["SUBTTL"], "SUBTYP": r["SUBTYP"],
        "REMMTH1": "SUB-TOTAL",
        "AMOUNT": r["AMOUNT"], "COST": r["COST"], "REMM": r["REMM"],
        "WACOST": wacost, "WAREMM": waremm,
    })

fd_table_rows = fdtotal_combined + fdtota2_rows
print(f"  FD table rows: {len(fd_table_rows):,}")

# ============================================================================
# STEP 12: REPORT RENDERING  (PROC TABULATE emulation)
# BOX='DEPOSITS' RTS=65 CONDENSE  -- row-label width 65 (PRODTYP is LENGTH $16,
# SUBTTL formatted label occupies 20 chars, REMMTH/REMMTH1 bucket the rest).
# LINE_SIZE assumed 132 (SAS default) -- this bounds how many SUBTYP column
# groups (CONVENTIONAL / SPTF / TOTAL) fit side-by-side before PROC TABULATE
# horizontally wraps to a "(Continued)" segment.
# (plain fixed-width text, no ASA -- see module docstring re. RECFM=FB)
# ============================================================================
print("\nStep 12: Rendering report...")

LINE_SIZE          = 132
LABEL_WIDTH         = 65    # RTS=65
PRODTYP_WIDTH       = 16    # SAS: LENGTH PRODTYP $16
SUBTTL_LABEL_WIDTH  = 20    # widest $SUBTTL. label (e.g. 'NEW FD FOR THE MONTH')
BUCKET_COL_START    = 43    # fixed column where REMMTH/REMMTH1 bucket text begins
HEADER_ROWS         = 4     # stacked column-header lines per measure
SUBTYP_ORDER        = ["CONVENTIONAL", "SPTF", "TOTAL"]
FF                  = "\f"

# Measure column widths, matching F=COMMA12. / COMMA12.2 / COMMA5.2
_MEASURE_SPECS = {"AMOUNT": (12, 0), "WACOST": (12, 2), "WAREMM": (5, 2)}
GROUP_INNER_WIDTH = sum(w for w, _ in _MEASURE_SPECS.values()) + (len(_MEASURE_SPECS) - 1)  # 12+12+5+2 = 31
GROUP_WIDTH       = GROUP_INNER_WIDTH + 1                                                   # +1 trailing pipe = 32


def _total_width(n_groups: int) -> str:
    return LABEL_WIDTH + 2 + n_groups * GROUP_WIDTH


def _center(text: str, width: int) -> str:
    text = text[:width]
    pad = width - len(text)
    left = pad // 2
    right = pad - left
    return " " * left + text + " " * right


def _dashes(width: int) -> str:
    return "-" * width


# Stacked column-header content (fixed, since only these 3 measures ever appear).
# AMOUNT / WACOST wrap by whole words (SAS splits a too-long label word-by-word,
# bottom-anchored); WAREMM's label ("REMAINING MATURITY") is narrower than any
# single word, so SAS hyphenates mid-word -- that wrap is hardcoded verbatim.
_AMOUNT_HEADER = [_center(w, 12) for w in ("", "BAL", "OUSTANDING", "(RM'000)")]
_WACOST_HEADER = [_center(w, 12) for w in ("", "", "", "W.A. COST %")]
_WAREMM_HEADER = ["REMA-", "INING", "MATU-", "RITY "]


def _build_label(prodtyp: str, subttl_label: str, bucket_label: str) -> str:
    """Fixed-column row label: PRODTYP(1-16) + gap + SUBTTL(22-41) + gap + bucket(43-65)."""
    buf = [" "] * LABEL_WIDTH
    if prodtyp:
        for i, ch in enumerate(prodtyp[:PRODTYP_WIDTH]):
            buf[i] = ch
    if subttl_label:
        for i, ch in enumerate(subttl_label[:SUBTTL_LABEL_WIDTH]):
            buf[21 + i] = ch
    if bucket_label:
        for i, ch in enumerate(bucket_label[:LABEL_WIDTH - (BUCKET_COL_START - 1)]):
            buf[BUCKET_COL_START - 1 + i] = ch
    return "".join(buf)


def _fmt_num(value, width: int, decimals: int) -> str:
    """COMMAw.d with comma-drop-on-overflow. MISSING=0 semantics: a genuinely
    absent cell (no contributing rows -> None) renders as a bare '0', while a
    real computed zero renders fully decimal-formatted."""
    if value is None:
        return "0".rjust(width)
    v = float(value)
    if abs(v) < 0.5 * 10 ** -decimals:   # threshold for rounding to zero
        v = 0.0
    s = f"{v:,.{decimals}f}"
    if len(s) > width:
        s = f"{v:.{decimals}f}"          # drop commas if it doesn't fit
    if len(s) > width:
        s = s[-width:]                   # last-resort truncation
    return s.rjust(width)


def _merged_row(box_label: str, groups: list) -> str:
    label_cell = box_label.ljust(LABEL_WIDTH)[:LABEL_WIDTH]
    group_cells = [_center(g, GROUP_INNER_WIDTH) for g in groups]
    return "|" + label_cell + "|" + "|".join(group_cells) + "|"


def _divider1(n_groups: int) -> str:
    """Divider between the merged group-header row and the stacked sub-headers.
    Label side stays blank (spaces); '+' only appears between dash groups."""
    return "|" + " " * LABEL_WIDTH + "|" + "+".join([_dashes(GROUP_INNER_WIDTH)] * n_groups) + "|"


def _stacked_header_row(groups: list, line_idx: int) -> str:
    parts = [" " * LABEL_WIDTH]
    for _ in groups:
        parts.append(_AMOUNT_HEADER[line_idx])
        parts.append(_WACOST_HEADER[line_idx])
        parts.append(_WAREMM_HEADER[line_idx])
    return "|" + "|".join(parts) + "|"


def _divider2(n_groups: int) -> str:
    """Full divider before data rows: dashes everywhere, '+' at every boundary."""
    group_dash = "-" * 12 + "+" + "-" * 12 + "+" + "-" * 5
    return "|" + "-" * LABEL_WIDTH + "+" + "+".join([group_dash] * n_groups) + "|"


def _full_row(label: str, group_cells: list) -> str:
    parts = [label]
    for amount_s, wacost_s, waremm_s in group_cells:
        parts.append(amount_s)
        parts.append(wacost_s)
        parts.append(waremm_s)
    return "|" + "|".join(parts) + "|"


def _title_block() -> list:
    titles = [
        "PUBLIC ISLAMIC BANK BERHAD",
        f"TIME TO MATURITY AS AT {RDATE}",
        "RISK MANAGEMENT REPORT : EIIMRM01",
        "RM DENOMINATION",
        "",
    ]
    return titles


def _render_tabulate(rows: list, bucket_key: str) -> list:
    """
    Emulates: TABLE PRODTYP*SUBTTL*<bucket>, (SUBTYP)*SUM*(AMOUNT WACOST WAREMM)
              / BOX='DEPOSITS' RTS=65 CONDENSE;

    - Horizontal pagination: when the SUBTYP groups present don't all fit
      within LINE_SIZE, splits into column-group chunks separated by
      '(Continued)' (no new page, no title repeat, same rows re-rendered
      for the next chunk of columns).
    - Vertical pagination: when rows exceed PAGE_SIZE within one chunk,
      starts a new page (form feed + repeated titles + full header block).
    """
    present_groups = [g for g in SUBTYP_ORDER if any(r["SUBTYP"] == g for r in rows)]
    if not present_groups:
        return []

    cell = {}
    seen_keys = set()
    row_keys = []
    for r in rows:
        bucket = r.get(bucket_key) or ""
        key = (r["PRODTYP"], r["SUBTTL"], bucket)
        cell.setdefault(key, {})[r["SUBTYP"]] = r
        if key not in seen_keys:
            seen_keys.add(key)
            row_keys.append(key)

    row_keys.sort(key=lambda k: (k[0], k[1], _remfmt_sort_key(k[2])))

    max_groups_per_chunk = max(1, (LINE_SIZE - (LABEL_WIDTH + 2)) // GROUP_WIDTH)
    chunks = [
        present_groups[i:i + max_groups_per_chunk]
        for i in range(0, len(present_groups), max_groups_per_chunk)
    ]

    output: list = []

    for chunk_idx, groups in enumerate(chunks):
        n = len(groups)
        state = {"lines_on_page": 0}

        def _emit_page(with_titles: bool):
            block = []
            if with_titles:
                block.append(FF)
                block.extend(_title_block())
            block.append(_dashes(_total_width(n)))
            block.append(_merged_row("DEPOSITS", groups))
            block.append(_divider1(n))
            for line_idx in range(HEADER_ROWS):
                block.append(_stacked_header_row(groups, line_idx))
            block.append(_divider2(n))
            output.extend(block)
            state["lines_on_page"] = len(block)

        _emit_page(with_titles=(chunk_idx == 0))
        prev_prodtyp = prev_subttl = None

        for key in row_keys:
            prodtyp, subttl, bucket = key

            if state["lines_on_page"] >= PAGE_SIZE:
                _emit_page(with_titles=True)
                prev_prodtyp = prev_subttl = None

            show_subttl = (subttl != prev_subttl) or (prodtyp != prev_prodtyp)
            subttl_label = subttl_format(subttl) if show_subttl else ""
            prodtyp_label = prodtyp if prodtyp != prev_prodtyp else ""
            prev_prodtyp, prev_subttl = prodtyp, subttl

            label = _build_label(prodtyp_label, subttl_label, bucket)

            group_cells = []
            for g in groups:
                rec = cell[key].get(g)
                amount = rec["AMOUNT"] if rec else None
                wacost = rec["WACOST"] if rec else None
                waremm = rec["WAREMM"] if rec else None
                group_cells.append((
                    _fmt_num(amount, 12, 0),
                    _fmt_num(wacost, 12, 2),
                    _fmt_num(waremm, 5, 2),
                ))

            output.append(_full_row(label, group_cells))
            state["lines_on_page"] += 1

        output.append(_dashes(_total_width(n)))

        if chunk_idx < len(chunks) - 1:
            output.append("")
            output.append("(Continued)")
            output.append("")
            output.append("")

    return output


report_lines = []
report_lines += _render_tabulate(depfinal_all, "REMMTH_BKT")
report_lines += _render_tabulate(fd_table_rows, "REMMTH1")

# ============================================================================
# STEP 13: WRITE OUTPUT
# ============================================================================
with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in report_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(report_lines):,}")
# print("\n--- Report preview (first 40 lines) ---")
# for ln in report_lines[:40]:
#     print(ln)

print("\nEIIMRM01 complete.")
