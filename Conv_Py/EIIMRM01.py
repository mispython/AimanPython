#!/usr/bin/env python3
"""
Program : EIIMRM01.py (converted from SAS EIIMRM01, ISLAMIC)
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
PHYSICAL INPUT DATASETS  (each cached to Parquet independently, using the
same chunked sas7bdat -> Parquet -> cache pattern as EIBDLN1M.py)
============================================================================
1. FD.FD        (JCL //FD  DD DSN=SAP.PIBB.MNIFD(0))
   File : INPUT_FD_FILE        -> fd.sas7bdat
   Used : DATA FD/TD/FDN step - fixed deposit detail (INTPLAN, CURBAL,
          RATE, MATDATE, OPENIND).

2. BNM.SAVING   (JCL //BNM DD DSN=SAP.PIBB.MNITB(0), member SAVING)
   File : INPUT_SAVING_FILE    -> saving.sas7bdat
   Used : DATA SA step - savings account detail (PRODUCT, OPENIND, CURBAL,
          INTRATE).

3. BNM.CURRENT  (JCL //BNM DD DSN=SAP.PIBB.MNITB(0), member CURRENT)
   File : INPUT_CURRENT_FILE   -> current.sas7bdat
   Used : DATA CA/CAG/CAS step - current account detail (PRODUCT, OPENIND,
          CURBAL, INTRATE).

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
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

INPUT_FD_DIR      = BASE_DIR / "input" / "prod" / "EIIMRM01" / "FD"
INPUT_SAVING_DIR  = BASE_DIR / "input" / "prod" / "EIIMRM01" / "SAVING"
INPUT_CURRENT_DIR = BASE_DIR / "input" / "prod" / "EIIMRM01" / "CURRENT"

INPUT_FD_FILE      = INPUT_FD_DIR / "fd.sas7bdat"
INPUT_SAVING_FILE  = INPUT_SAVING_DIR / "saving.sas7bdat"
INPUT_CURRENT_FILE = INPUT_CURRENT_DIR / "current.sas7bdat"

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIIMRM01"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR  = BASE_DIR / "output" / "EIIMRM01"
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


def _parse_matdate(matdate) -> date:
    """MATDT = INPUT(PUT(MATDATE,Z8.),YYMMDD8.) -- MATDATE is stored as an
    8-digit YYYYMMDD integer."""
    s = f"{int(matdate):08d}"
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))


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
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        if schema is None:
            schema = table.schema
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")
        else:
            cast_arrays = []
            for field in schema:
                col = table.column(field.name)
                if col.type != field.type:
                    try:
                        col = col.cast(field.type, safe=False)
                    except Exception as e:
                        print(f"  [{tag}] WARNING: cannot cast '{field.name}' "
                              f"from {col.type} to {field.type}: {e} - filling nulls")
                        col = pa.nulls(len(col), type=field.type)
                cast_arrays.append(col)
            table = pa.Table.from_arrays(cast_arrays, schema=schema)
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
FD_CACHE      = _load_cached(INPUT_FD_FILE, "FD")
SAVING_CACHE  = _load_cached(INPUT_SAVING_FILE, "SAVING")
CURRENT_CACHE = _load_cached(INPUT_CURRENT_FILE, "CURRENT")

# ============================================================================
# STEP 3: BUILD FD / TD / FDN  (DATA FD TD FDN; SET FD.FD; ...)
# ============================================================================
print("\nStep 3: Building FD / TD / FDN from FD.FD...")

con = duckdb.connect(database=":memory:")
fd_raw = con.execute(f"""
    SELECT
        CAST(INTPLAN AS INTEGER) AS INTPLAN,
        CAST(CURBAL  AS DOUBLE)  AS CURBAL,
        CAST(RATE    AS DOUBLE)  AS RATE,
        CAST(MATDATE AS BIGINT)  AS MATDATE,
        CAST(OPENIND AS VARCHAR) AS OPENIND
    FROM read_parquet('{FD_CACHE.as_posix()}')
""").pl()
con.close()


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
        matdt = _parse_matdate(r["MATDATE"])

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
print("\nStep 4: Building SA from BNM.SAVING...")

con = duckdb.connect(database=":memory:")
saving_raw = con.execute(f"""
    SELECT
        CAST(PRODUCT AS INTEGER) AS PRODUCT,
        CAST(OPENIND AS VARCHAR) AS OPENIND,
        CAST(CURBAL  AS DOUBLE)  AS CURBAL,
        CAST(INTRATE AS DOUBLE)  AS INTRATE
    FROM read_parquet('{SAVING_CACHE.as_posix()}')
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
print("\nStep 5: Building CA / CAG / CAS from BNM.CURRENT...")

con = duckdb.connect(database=":memory:")
current_raw = con.execute(f"""
    SELECT
        CAST(PRODUCT AS INTEGER) AS PRODUCT,
        CAST(OPENIND AS VARCHAR) AS OPENIND,
        CAST(CURBAL  AS DOUBLE)  AS CURBAL,
        CAST(INTRATE AS DOUBLE)  AS INTRATE
    FROM read_parquet('{CURRENT_CACHE.as_posix()}')
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
# STEP 12: REPORT RENDERING
# (plain fixed-width text, no ASA -- see module docstring re. RECFM=FB)
# ============================================================================
print("\nStep 12: Rendering report...")

ROW_LABEL_WIDTH = 65   # RTS=65
COL_WIDTHS = {"AMOUNT": 14, "WACOST": 14, "WAREMM": 9}
MEASURE_LABELS = {
    "AMOUNT": "BAL OUSTANDING (RM'000)",
    "WACOST": "W.A. COST %",
    "WAREMM": "REMAINING MATURITY",
}
SUBTYP_ORDER = ["CONVENTIONAL", "SPTF", "TOTAL"]
FF = "\f"


def _new_buf(width: int = 200) -> list:
    return [" "] * width


def _put(buf: list, col: int, text: str) -> None:
    start = col - 1
    for i, ch in enumerate(str(text)):
        if 0 <= start + i < len(buf):
            buf[start + i] = ch


def _line(buf: list) -> str:
    return "".join(buf).rstrip()


def _fmt_comma(value, width: int, decimals: int = 0) -> str:
    """COMMAw.d format. OPTIONS MISSING=0 substitutes a bare '0' (not
    decimal-formatted) for a genuinely missing value, distinguishing a
    bucket with no underlying records from one that summed to a real zero
    (which prints '0.00' / '0')."""
    if value is None:
        return "0".rjust(width)
    v = float(value)
    s = f"{v:,.{decimals}f}"
    return s.rjust(width)


def _title_block() -> list:
    titles = [
        "PUBLIC ISLAMIC BANK BERHAD",
        f"TIME TO MATURITY AS AT {RDATE}",
        "RISK MANAGEMENT REPORT : EIIMRM01",
        "RM DENOMINATION",
        "",
    ]
    out = []
    for t in titles:
        buf = _new_buf()
        _put(buf, 1, t)
        out.append(_line(buf))
    return out


def _column_header(box_label: str) -> list:
    lines = []
    col_group_width = sum(COL_WIDTHS.values())

    buf = _new_buf()
    _put(buf, 1, box_label)
    col = ROW_LABEL_WIDTH + 1
    for st in SUBTYP_ORDER:
        _put(buf, col, st.center(col_group_width))
        col += col_group_width
    lines.append(_line(buf))

    buf = _new_buf()
    col = ROW_LABEL_WIDTH + 1
    for _ in SUBTYP_ORDER:
        _put(buf, col, MEASURE_LABELS["AMOUNT"][:COL_WIDTHS["AMOUNT"]])
        col += COL_WIDTHS["AMOUNT"]
        _put(buf, col, MEASURE_LABELS["WACOST"][:COL_WIDTHS["WACOST"]])
        col += COL_WIDTHS["WACOST"]
        _put(buf, col, MEASURE_LABELS["WAREMM"][:COL_WIDTHS["WAREMM"]])
        col += COL_WIDTHS["WAREMM"]
    lines.append(_line(buf))

    total_width = ROW_LABEL_WIDTH + len(SUBTYP_ORDER) * col_group_width
    buf = _new_buf()
    _put(buf, 1, "-" * min(total_width, 199))
    lines.append(_line(buf))
    return lines


def _render_tabulate(rows: list, bucket_key: str, box_label: str) -> list:
    cell = {}
    row_keys = set()
    for r in rows:
        key = (r["PRODTYP"], r["SUBTTL"], r.get(bucket_key) or "")
        cell.setdefault(key, {})[r["SUBTYP"]] = r
        row_keys.add(key)

    ordered_keys = sorted(row_keys, key=lambda k: (k[0], k[1], _remfmt_sort_key(k[2])))

    output = []
    lines_on_page = 0
    prev_prodtyp = prev_subttl = None

    def start_page():
        nonlocal lines_on_page, prev_prodtyp, prev_subttl
        output.append(FF)
        output.extend(_title_block())
        output.extend(_column_header(box_label))
        lines_on_page = 5 + 3
        prev_prodtyp = None
        prev_subttl = None

    start_page()

    for prodtyp, subttl, bucket in ordered_keys:
        if lines_on_page >= PAGE_SIZE:
            start_page()

        buf = _new_buf()
        if prodtyp != prev_prodtyp:
            _put(buf, 1, prodtyp[:20])
            prev_prodtyp = prodtyp
            prev_subttl = None
        if subttl != prev_subttl:
            _put(buf, 22, subttl_format(subttl)[:22])
            prev_subttl = subttl
        _put(buf, 45, bucket[:20])

        vcol = ROW_LABEL_WIDTH + 1
        for st in SUBTYP_ORDER:
            rec = cell[(prodtyp, subttl, bucket)].get(st)
            amount = rec["AMOUNT"] if rec else None
            wacost = rec["WACOST"] if rec else None
            waremm = rec["WAREMM"] if rec else None
            _put(buf, vcol, _fmt_comma(amount, COL_WIDTHS["AMOUNT"], 0))
            vcol += COL_WIDTHS["AMOUNT"]
            _put(buf, vcol, _fmt_comma(wacost, COL_WIDTHS["WACOST"], 2))
            vcol += COL_WIDTHS["WACOST"]
            _put(buf, vcol, _fmt_comma(waremm, COL_WIDTHS["WAREMM"], 2))
            vcol += COL_WIDTHS["WAREMM"]

        output.append(_line(buf))
        lines_on_page += 1

    return output


report_lines = []
report_lines += _render_tabulate(depfinal_all, "REMMTH_BKT", "DEPOSITS")
report_lines += _render_tabulate(fd_table_rows, "REMMTH1", "DEPOSITS")

# ============================================================================
# STEP 13: WRITE OUTPUT
# ============================================================================
with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in report_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(report_lines):,}")
print("\n--- Report preview (first 40 lines) ---")
for ln in report_lines[:40]:
    print(ln)

print("\nEIIMRM01 complete.")
