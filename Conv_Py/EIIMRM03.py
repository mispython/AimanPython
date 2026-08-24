#!/usr/bin/env python3
"""
Program : EIIMRM03.py
Purpose : FD - By Individual And Non-Individual, By Time To Maturity For
          ALCO (Weighted Average Cost By Maturity Profile) - Islamic book.
          Structurally the same family as EIIMRM01.py / EIIMRM02.py, but
          restricted to BNMCODE='42132' (FD MUDH / Islamic term investment
          products) only, with its own REMFMT bucket scheme (same ranges as
          EIIMRM01) and its own TYPE (INDIVIDUALS/NON-INDIVIDUALS) dimension
          (same idea as EIIMRM02, but a different CUSTCD membership set).

Dependency:
    %INC PGM(PBBDPFMT); -> from PBBDPFMT import fdprod_format
    FDPROD.  is used (BNMCODE drives FCY vs RM classification / SPTF flag,
             and also drives this program's own subsetting filter).
    CAPROD.  and SAPROD.  are NOT called anywhere in this program's SAS body
             (like EIIMRM02, EIIMRM03 has no BNM.CURRENT / BNM.SAVING data
             steps -- the JCL only carries a //FD DD). They are therefore
             intentionally NOT imported here.

============================================================================
PHYSICAL INPUT DATASETS  (each cached to Parquet independently)
============================================================================
1. main_fd.sas7bdat   (JCL DD DSN=SAP.PIBB.MNITB(0)) (PBB+PIBB combined account master for FD)
   File : INPUT_MAIN_FD_FILE   -> intg_dp_acct_fd_d19.sas7bdat
   Cols used : ACCTNO, ENTITY_CD
   Used to build the list of PIBB-only account numbers, since fd.sas7bdat
   itself is a mixed PBB/PIBB dataset with no ENTITY_CD column (same
   dependency as EIIMRM01.py / EIIMRM02.py).

2. fd.sas7bdat        (JCL //FD  DD DSN=SAP.PIBB.MNIFD(0))
   File : INPUT_FD_FILE        -> enrh_dp_fd_cert_d19.sas7bdat
   Cols used : ACCT_NUM, INT_PLAN, CURR_BAL, RT, MATURE_DT, OPEN_IND, CUST_CD
   Used  : DATA FD/TD/FDN step (SET FD.FD). Filtered to PIBB-only rows by
           inner-joining ACCT_NUM against the PIBB ACCTNO list derived from
           main_fd.sas7bdat.
   ACCT_NUM is required here purely as the join key against main_fd; it is
   NOT part of this program's own KEEP list, so it is dropped again
   immediately after the join and never enters fd_rows/td_rows/fdn_rows.

============================================================================
KEY BEHAVIOURAL NOTE -- "IF BNMCODE='42132';" SUBSETTING FILTER
============================================================================
Right after PRODTYP/TERM derivation, the SAS DATA step applies a subsetting
IF that keeps ONLY rows where BNMCODE='42132' (FD MUDH / Islamic term
investment products per PBBDPFMT.FDPROD). Because FCY classification
requires BNMCODE='42630', this filter guarantees for every surviving row:
  - PRODTYP is always 'FIXED DEPT(RM)' (never 'FIXED DEPT(FCY)')
  - TERM is always missing/None (TERM is only ever assigned in the
    BNMCODE='42630' branch, which can never be true here)
  - "IF BNMCODE='42132' THEN SUBTYP='SPTF'" always fires, so SUBTYP is
    always 'SPTF' for every row this program ever outputs (the
    'CONVENTIONAL' default is set but immediately overwritten every time)
These are preserved exactly as the SAS source computes them, not
special-cased away, to keep behavioural parity.

============================================================================
OUTPUT
============================================================================
//TEMP DD DSN=SAP.PIBB.EIIMRM03.TEXT (implied by pattern; JCL header for
this member was not supplied, but PROC PRINTTO PRINT=TEMP NEW; matches the
same //TEMP DD convention used by EIIMRM01/EIIMRM02), DISP=OLD,
DCB=(RECFM=FB,LRECL=256,...). Fixed-name GDG-style catalogued dataset (no
date token in the name), so the Python output file uses a fixed filename,
not a dated one.

RECFM=FB (NOT FBA) means NO ASA carriage-control byte (per project
convention: RECFM=FBA implies ASA control, RECFM=FB does not). Page
boundaries (PAGESIZE=60, not otherwise specified in the SAS source) are
marked with a form-feed character instead of an ASA '1' byte.
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
from PBBDPFMT import fdprod_format

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# BASE_DIR = Path("/sas/deposit/dwh")

BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

# INPUT_MAIN_FD_DIR = BASE_DIR / "input" / "prod" / "EIIMRM03" / "MAIN_FD"
# INPUT_FD_DIR      = BASE_DIR / "enrichment"
INPUT_MAIN_FD_DIR = STG_DIR / "sasdata"
INPUT_FD_DIR      = STG_DIR / "sasdata"

INPUT_MAIN_FD_FILE = INPUT_MAIN_FD_DIR / "intg_dp_acct_fd_d19.sas7bdat"
INPUT_FD_FILE      = INPUT_FD_DIR / "enrh_dp_fd_cert_d19.sas7bdat"

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIIMRPTS"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR  = BASE_DIR / "output" / "EIIMRPTS"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "EIIMRM03.txt"

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
# program body -- they are dead declarations, omitted here (same as
# EIIMRM01.py / EIIMRM02.py).
RD_DAYS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
if RPYR % 4 == 0:
    RD_DAYS[1] = 29

print(f"  RDATE        : {RDATE}")
print(f"  REPTMON/DAY  : {REPTMON}/{REPTDAY}  REPTYEAR: {REPTYEAR}")
print(f"  Output file  : {OUTPUT_FILE.name}")

# ============================================================================
# PROC FORMAT EQUIVALENTS (local to this program)
# ============================================================================
# VALUE REMFMT -- identical range set to EIIMRM01's local REMFMT (NOT
# EIIMRM02's whole-month variant). LOW-0 -> blank, then '> X-Y MTHS' ranges,
# year-range buckets, special codes 91-97/99, OTHER -> blank.
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
    """PROC FORMAT VALUE REMFMT. (local to EIIMRM03, same ranges as
    EIIMRM01). SAS numeric missing sorts as LOW, so a None value falls into
    the LOW-0 (blank) bucket. Ranges are checked in the same order as the
    SAS format definition, so an exact boundary value matches the FIRST
    listed range."""
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


# VALUE $SUBTTL. -- this program's own label set. Only 'A', 'B', 'C' are
# ever assigned by this program's DATA step (FD-only content); D-H are kept
# for parity with the SAS PROC FORMAT definition but are dead here. Note
# label 'A' reads 'REMAINING MATURITY' in THIS program, unlike EIIMRM02
# where 'A' reads 'ORIGINAL MATURITY' -- must not be confused.
_SUBTTL_LABELS = {
    "A": "REMAINING MATURITY",
    "B": "OVERDUE FD",
    "C": "NEW FD FOR THE MONTH",
    "D": "SAVING ACCOUNTS",
    "E": "NON INTEREST BEARING",
    "F": "INTEREST BEARING",
    "G": "HOUSNG DEVELOPER ACC",
    "H": "PORTION FROM ACE ACC",
}


def subttl_format(code):
    if code is None:
        return ""
    return _SUBTTL_LABELS.get(code, code)


# TERMFMT is a PROC FORMAT declared locally inside this program -- same
# value set as EIIMRM01/EIIMRM02's local TERMFMT. Kept here for structural
# parity, but it is effectively DEAD CODE in this program: the subsetting
# "IF BNMCODE='42132';" guarantees BNMCODE is never '42630', so the branch
# that calls TERMFMT. is never reached and TERM stays missing for every row.
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
# HELPER: CACHE STAMP + STREAM .sas7bdat -> PARQUET  (EIIMRM01.py pattern)
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
        if schema is None:
            fields = []
            for col, dtype in chunk.dtypes.items():
                if dtype == 'object':
                    pa_type = pa.string()
                elif pd.api.types.is_integer_dtype(dtype):
                    pa_type = pa.int64()
                elif pd.api.types.is_float_dtype(dtype):
                    pa_type = pa.float64()
                else:
                    pa_type = pa.from_numpy_dtype(dtype)
                fields.append(pa.field(col, pa_type))
            schema = pa.schema(fields)
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")

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
# STEP 2: CACHE INPUT SAS DATASETS TO PARQUET
# ============================================================================
print("\nStep 2: Caching input SAS datasets to Parquet...")
MAIN_FD_CACHE = _load_cached(INPUT_MAIN_FD_FILE, "MAIN_FD")
FD_CACHE      = _load_cached(INPUT_FD_FILE, "FD")

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
        CAST(f.MATURE_DT    AS DATE)    AS MATDATE,
        CAST(f.OPEN_IND     AS VARCHAR) AS OPENIND,
        CAST(f.CUSTOMER_CD  AS INTEGER) AS CUSTCD
    FROM read_parquet('{FD_CACHE.as_posix()}') f
    INNER JOIN main_fd_pibb m
        ON CAST(f.ACCT_NUM AS BIGINT) = m.ACCTNO
""").pl()
con.close()

print(f"  FD rows after PIBB account filter: {len(fd_raw):,}")

# # >>>>> DEBUG STARTS <<<<<
# print("  Debug: sample BNMCODE values (first 10 rows):")
# for i, r in enumerate(fd_raw.iter_rows(named=True)):
#     if i >= 10:
#         break
#     bnmcode = fdprod_format(r["INTPLAN"])
#     print(f"    intplan={r['INTPLAN']} -> bnmcode={bnmcode}")

# # Create a new column with bnmcode using map_elements
# fd_with_bnm = fd_raw.with_columns(
#     pl.col('INTPLAN').map_elements(lambda x: fdprod_format(x), return_dtype=pl.String).alias('BNMCODE')
# )
# counts = fd_with_bnm.group_by('BNMCODE').agg(pl.len())
# print("  Distinct BNMCODE counts:")
# print(counts)

# # Add BNMCODE column
# fd_with_bnm = fd_raw.with_columns(
#     pl.col('INTPLAN').map_elements(lambda x: fdprod_format(x), return_dtype=pl.String).alias('BNMCODE')
# )

# # Write to CSV (you can change the path)
# csv_path = OUTPUT_DIR / "EIIMRM03_fd_42132.csv"
# fd_with_bnm.write_csv(csv_path)
# print(f"  Saved all {len(fd_with_bnm):,} rows with BNMCODE to {csv_path}")

# # Optionally, filter for only 42132
# filt = fd_with_bnm.filter(pl.col('BNMCODE') == '42132')
# print(f"  Rows with BNMCODE='42132': {filt.height}")
# if filt.height > 0:
#     filt.write_csv("EIIMRM03_fd_42132.csv")
#     print("  Saved rows with BNMCODE='42132' to EIIMRM03_fd_42132.csv")
# # >>>>> DEBUG ENDS <<<<<


def _row(prodtyp, subtyp, subttl, type_, amount, cost, remmth, remm):
    return {"PRODTYP": prodtyp, "SUBTYP": subtyp, "SUBTTL": subttl, "TYPE": type_,
            "AMOUNT": amount, "COST": cost, "REMMTH": remmth, "REMM": remm}


fd_rows, td_rows, fdn_rows = [], [], []

for r in fd_raw.iter_rows(named=True):
    intplan = r["INTPLAN"]
    curbal  = r["CURBAL"]
    rate    = r["RATE"] or 0.0
    openind = r["OPENIND"]
    custcd  = r["CUSTCD"]

    if curbal is None:
        continue

    bnmcode = fdprod_format(intplan)
    term = None   # RETAIN-less: reset to missing every DATA-step iteration
    if bnmcode == "42630":
        prodtyp = "FIXED DEPT(FCY)"
        term = termfmt_format(intplan)
    else:
        prodtyp = "FIXED DEPT(RM)"

    # IF BNMCODE='42132';  (subsetting IF -- drop everything else)
    # Guarantees PRODTYP='FIXED DEPT(RM)' and TERM stays None for every
    # surviving row, since FCY classification requires BNMCODE='42630'.
    if bnmcode != "42132":
        continue

    # TYPE='NON-INDIVIDUALS'; IF CUSTCD IN (77,78,95,96) THEN TYPE='INDIVIDUALS';
    # Note: unlike EIIMRM02, 76 is NOT in this membership set.
    type_ = "INDIVIDUALS" if custcd in (77, 78, 95, 96) else "NON-INDIVIDUALS"

    # IF OPENIND = 'O' OR OPENIND = 'D' AND CURBAL > 0  (AND binds tighter)
    if openind == "O" or (openind == "D" and curbal > 0):
        matdt = r["MATDATE"]

        if openind == "D" or matdt < reptdate:
            # SUBTYP='CONVENTIONAL'; IF BNMCODE='42132' THEN SUBTYP='SPTF'
            # -- always fires given the subsetting IF above, so SUBTYP is
            # always 'SPTF' here. No REMM assignment in this branch (SAS
            # source has none), so REMM stays missing/None.
            subtyp = "SPTF" if bnmcode == "42132" else "CONVENTIONAL"
            remmth = 99.0
            cost = curbal * rate
            td_rows.append(_row(prodtyp, subtyp, "B", type_, curbal, cost, remmth, None))
        else:
            remmth_fd = _remmth(matdt)       # %REMMTH result -- used as-is for FD
            subtyp = "SPTF" if bnmcode == "42132" else "CONVENTIONAL"
            cost = curbal * rate
            remm_fd = curbal * remmth_fd
            fd_rows.append(_row(prodtyp, subtyp, "A", type_, curbal, cost, remmth_fd, remm_fd))

            # IF ((TERM - REMMTH) < 1) THEN DO ... OUTPUT FDN; END;
            # TERM is always None here (see subsetting-IF note above), so
            # SAS missing-arithmetic/comparison rules make this condition
            # ALWAYS TRUE -- FDN fires for every FD row, exactly the same
            # quirk documented in EIIMRM01.py/EIIMRM02.py.
            fires = (term is None) or ((term - remmth_fd) < 1)
            if fires:
                cost2 = curbal * rate
                # REMM = REMMTH * CURBAL  (uses the ORIGINAL remmth_fd,
                # i.e. the same value already used for the FD row's REMM,
                # captured BEFORE the REMMTH overwrite below).
                remm2 = remmth_fd * curbal
                # REMMTH = TERM - 0.5  (unique to EIIMRM03 -- EIIMRM01/02
                # instead just assign REMMTH = TERM). Since TERM is always
                # None here, SAS missing arithmetic keeps this missing.
                remmth2 = None if term is None else term - 0.5
                fdn_rows.append(_row(prodtyp, subtyp, "C", type_, curbal, cost2, remmth2, remm2))

print(f"  FD  rows: {len(fd_rows):,}   TD rows: {len(td_rows):,}   FDN rows: {len(fdn_rows):,}")

del fd_raw
gc.collect()

# ============================================================================
# STEP 4: PROC SUMMARY (per-source) -- CLASS TYPE PRODTYP SUBTYP SUBTTL REMMTH
# ============================================================================
print("\nStep 4: Summarising TD / FD / FDN...")


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
    """CLASS TYPE PRODTYP SUBTYP SUBTTL REMMTH; FORMAT REMMTH REMFMT.;
    Only TD's PROC SUMMARY carries MISSING (REMMTH is always 99 there
    anyway). FD's REMMTH is always the real %REMMTH-computed value (never
    missing), so every FD row survives its own (non-MISSING) PROC SUMMARY.
    FDN's REMMTH is ALWAYS None here (TERM-0.5 with TERM always None), and
    FDN's PROC SUMMARY has NO MISSING option, so every FDN row is dropped
    before summarising -- fdn_summary will always come out empty for this
    program (a stronger version of the FDN-emptying quirk noted in
    EIIMRM01.py/EIIMRM02.py, since here TERM can never be non-missing at
    all given the BNMCODE='42132' subsetting filter)."""
    filtered = [r for r in rows if allow_missing or r["REMMTH"] is not None]
    for r in filtered:
        r["REMMTH_BKT"] = remfmt_format(r["REMMTH"])
    grouped = _group_sum(filtered, ["TYPE", "PRODTYP", "SUBTYP", "SUBTTL", "REMMTH_BKT"])
    for g in grouped:
        g["REMMTH1"] = g["REMMTH_BKT"]
    return grouped


td_summary  = _proc_summary_by_bucket(td_rows,  allow_missing=True)
fd_summary  = _proc_summary_by_bucket(fd_rows,  allow_missing=False)
fdn_summary = _proc_summary_by_bucket(fdn_rows, allow_missing=False)

print(f"  TD summary rows : {len(td_summary):,}")
print(f"  FD summary rows : {len(fd_summary):,}")
print(f"  FDN summary rows: {len(fdn_summary):,} "
      f"(always 0 for this program -- REMMTH is always missing for FDN "
      f"given the BNMCODE='42132' subsetting filter, and FDN's PROC "
      f"SUMMARY has no MISSING option)")

# ============================================================================
# STEP 5: DATA DEP; SET TD FD FDN; REMMTH1 = PUT(REMMTH,REMFMT.);
#         PROC SORT; BY TYPE PRODTYP SUBTTL SUBTYP REMMTH1;
# ============================================================================
print("\nStep 5: Combining TD+FD+FDN...")

dep_combined = []
for src in (td_summary, fd_summary, fdn_summary):
    for r in src:
        dep_combined.append(dict(r))
dep_combined.sort(key=lambda r: (r["TYPE"], r["PRODTYP"], r["SUBTTL"], r["SUBTYP"], r["REMMTH1"]))

# ----------------------------------------------------------------------
# DATA DUMMY; ... WHERE SUBTTL IN ('A','C'); For the first occurrence of
# each TYPE/PRODTYP/SUBTTL/SUBTYP group, emit a placeholder for every
# month 1-60 so all maturity buckets are represented even when no
# deposits fall into them.
# ----------------------------------------------------------------------
seen_groups = set()
dummy_rows = []
for r in dep_combined:
    if r["SUBTTL"] not in ("A", "C"):
        continue
    key = (r["TYPE"], r["PRODTYP"], r["SUBTTL"], r["SUBTYP"])
    if key in seen_groups:
        continue
    seen_groups.add(key)
    for m in range(1, 61):
        bkt = remfmt_format(float(m))
        dummy_rows.append({
            "TYPE": r["TYPE"], "PRODTYP": r["PRODTYP"], "SUBTTL": r["SUBTTL"],
            "SUBTYP": r["SUBTYP"], "REMMTH_BKT": bkt, "REMMTH1": bkt,
            "AMOUNT": None, "COST": None, "REMM": None,
        })

# PROC SORT DATA=DUMMY NODUPKEYS; BY TYPE PRODTYP SUBTTL SUBTYP REMMTH1;
dummy_by_key = {}
for r in dummy_rows:
    key = (r["TYPE"], r["PRODTYP"], r["SUBTTL"], r["SUBTYP"], r["REMMTH1"])
    dummy_by_key.setdefault(key, r)

# DATA DEP; MERGE DEP DUMMY; BY TYPE PRODTYP SUBTTL SUBTYP REMMTH1;
# DUMMY carries no AMOUNT/COST/REMM of its own for keys that already exist
# in dep_combined, so real records are never overwritten -- only genuinely
# absent buckets get added as zero-data placeholder rows.
existing_keys = {
    (r["TYPE"], r["PRODTYP"], r["SUBTTL"], r["SUBTYP"], r["REMMTH1"]) for r in dep_combined
}
dep_final = list(dep_combined)
for key, r in dummy_by_key.items():
    if key not in existing_keys:
        dep_final.append(r)

print(f"  DEP rows after DUMMY padding: {len(dep_final):,}")

# ============================================================================
# STEP 6: DATA DEP; SET DEP;  (dangling-ELSE bug preserved exactly)
# IF SUBTYP IN ('SPTF','CONVENTIONAL') THEN WACOST = COST / AMOUNT;
# WAREMM = REMM / AMOUNT;
# AMOUNT = ROUND(AMOUNT/1000);
# -- only WACOST is scoped to the IF; WAREMM and the AMOUNT rounding run
# UNCONDITIONALLY for every row (dangling-ELSE scoping artefact in the
# original SAS, same as EIIMRM01/02). Neither division has a
# zero-denominator guard in the SAS source, so a zero/missing AMOUNT
# yields a missing (None) result rather than a substituted zero.
# ============================================================================
print("\nStep 6: Applying DEP transform...")

dep2_rows = []
for r in dep_final:
    subtyp = r["SUBTYP"]
    amount = r["AMOUNT"]
    cost   = r["COST"]
    remm   = r["REMM"]

    if subtyp in ("SPTF", "CONVENTIONAL"):
        wacost = (cost / amount) if (amount not in (None, 0) and cost is not None) else None
    else:
        wacost = None

    waremm = (remm / amount) if (amount not in (None, 0) and remm is not None) else None
    amount_k = None if amount is None else _sas_round(amount / 1000)

    dep2_rows.append({
        "PRODTYP": r["PRODTYP"], "SUBTYP": subtyp, "SUBTTL": r["SUBTTL"], "TYPE": r["TYPE"],
        "AMOUNT": amount_k, "COST": cost, "REMM": remm,
        "REMMTH1": r["REMMTH1"], "WACOST": wacost, "WAREMM": waremm,
    })


def _wacost_waremm(amount_k, cost, remm):
    """WACOST = COST / ROUND(AMOUNT*1000); WAREMM = REMM / ROUND(AMOUNT*1000).
    Unconditional division at the aggregate levels below: a missing AMOUNT
    propagates to a missing result, it is NOT special-cased to zero."""
    if amount_k is None:
        return None, None
    denom = _sas_round(amount_k * 1000)
    wacost = (cost / denom) if (cost is not None and denom != 0) else None
    waremm = (remm / denom) if (remm is not None and denom != 0) else None
    return wacost, waremm


# ============================================================================
# STEP 7: DEPTOTAL (SUBTYPE TOTAL) -- CLASS TYPE PRODTYP SUBTTL REMMTH1
# ============================================================================
print("\nStep 7: Building DEPTOTAL (SUBTYP='TOTAL')...")

deptotal_grouped = _group_sum(dep2_rows, ["TYPE", "PRODTYP", "SUBTTL", "REMMTH1"])
deptotal_rows = []
for r in deptotal_grouped:
    wacost, waremm = _wacost_waremm(r["AMOUNT"], r["COST"], r["REMM"])
    deptotal_rows.append({
        "PRODTYP": r["PRODTYP"], "SUBTTL": r["SUBTTL"], "TYPE": r["TYPE"],
        "REMMTH1": r["REMMTH1"], "SUBTYP": "TOTAL",
        "AMOUNT": r["AMOUNT"], "COST": r["COST"], "REMM": r["REMM"],
        "WACOST": wacost, "WAREMM": waremm,
    })

# ============================================================================
# STEP 8: DEPTOTA2 (TYPE TOTAL) -- CLASS SUBTYP PRODTYP SUBTTL REMMTH1
# ============================================================================
print("\nStep 8: Building DEPTOTA2 (TYPE='TOTAL')...")

deptota2_type_grouped = _group_sum(dep2_rows, ["SUBTYP", "PRODTYP", "SUBTTL", "REMMTH1"])
deptota2_type_rows = []
for r in deptota2_type_grouped:
    wacost, waremm = _wacost_waremm(r["AMOUNT"], r["COST"], r["REMM"])
    deptota2_type_rows.append({
        "PRODTYP": r["PRODTYP"], "SUBTTL": r["SUBTTL"], "SUBTYP": r["SUBTYP"],
        "REMMTH1": r["REMMTH1"], "TYPE": "TOTAL",
        "AMOUNT": r["AMOUNT"], "COST": r["COST"], "REMM": r["REMM"],
        "WACOST": wacost, "WAREMM": waremm,
    })

# DATA DEPFINAL; SET DEP DEPTOTAL DEPTOTA2;
depfinal_rows = (
    [{k: r[k] for k in ("PRODTYP", "SUBTTL", "SUBTYP", "TYPE", "REMMTH1",
                        "AMOUNT", "COST", "REMM", "WACOST", "WAREMM")}
     for r in dep2_rows]
    + deptotal_rows
    + deptota2_type_rows
)
print(f"  DEPFINAL rows after TOTAL levels: {len(depfinal_rows):,}")

# ============================================================================
# STEP 9: GRAND SUB-TOTAL -- CLASS PRODTYP SUBTTL SUBTYP TYPE (across REMMTH1)
# ============================================================================
print("\nStep 9: Building grand SUB-TOTAL rows...")

grand_grouped = _group_sum(depfinal_rows, ["PRODTYP", "SUBTTL", "SUBTYP", "TYPE"])
grand_rows = []
for r in grand_grouped:
    wacost, waremm = _wacost_waremm(r["AMOUNT"], r["COST"], r["REMM"])
    grand_rows.append({
        "PRODTYP": r["PRODTYP"], "SUBTTL": r["SUBTTL"], "SUBTYP": r["SUBTYP"], "TYPE": r["TYPE"],
        "REMMTH1": "SUB-TOTAL",
        "AMOUNT": r["AMOUNT"], "COST": r["COST"], "REMM": r["REMM"],
        "WACOST": wacost, "WAREMM": waremm,
    })

# DATA DEPFINAL; SET DEPFINAL DEPTOTA2; IF TYPE NE '               ';
# TYPE is always populated by this point (INDIVIDUALS / NON-INDIVIDUALS /
# TOTAL), so this filter is a no-op safeguard in practice -- preserved
# verbatim for parity with the SAS source (same as EIIMRM02.py).
depfinal_all = depfinal_rows + grand_rows
depfinal_all = [r for r in depfinal_all if r["TYPE"] not in (None, "")]
print(f"  DEPFINAL final row count: {len(depfinal_all):,}")

# ============================================================================
# STEP 10: REPORT RENDERING  (PROC TABULATE emulation)
# TABLE PRODTYP*SUBTTL*REMMTH1, (SUBTYP)*((TYPE)*SUM*(AMOUNT WACOST WAREMM))
#       / BOX='DEPOSITS' RTS=65 CONDENSE;
# Two-level column nesting: SUBTYP (outer) crossed with TYPE (inner), each
# leaf carrying the 3 measures. (plain fixed-width text, no ASA -- see
# module docstring re. RECFM=FB)
# ============================================================================
print("\nStep 10: Rendering report...")

LINE_SIZE          = 132
LABEL_WIDTH        = 65    # RTS=65
PRODTYP_WIDTH      = 16    # SAS: LENGTH PRODTYP $16
SUBTTL_LABEL_WIDTH = 20    # widest $SUBTTL. label (e.g. 'NEW FD FOR THE MONTH')
BUCKET_COL_START   = 43    # fixed column where REMMTH1 bucket text begins
HEADER_ROWS        = 4     # stacked column-header lines per measure
SUBTYP_ORDER       = ["CONVENTIONAL", "SPTF", "TOTAL"]
TYPE_ORDER         = ["INDIVIDUALS", "NON-INDIVIDUALS", "TOTAL"]
FF                 = "\f"

_MEASURE_SPECS = {"AMOUNT": (12, 0), "WACOST": (12, 2), "WAREMM": (5, 2)}
GROUP_INNER_WIDTH = sum(w for w, _ in _MEASURE_SPECS.values()) + (len(_MEASURE_SPECS) - 1)  # 31
GROUP_WIDTH       = GROUP_INNER_WIDTH + 1                                                   # 32


def _total_width(n_groups: int) -> int:
    return LABEL_WIDTH + 2 + n_groups * GROUP_WIDTH


def _center(text: str, width: int) -> str:
    text = text[:width]
    pad = width - len(text)
    left = pad // 2
    right = pad - left
    return " " * left + text + " " * right


def _dashes(width: int) -> str:
    return "-" * width


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
    if abs(v) < 0.5 * 10 ** -decimals:
        v = 0.0
    s = f"{v:,.{decimals}f}"
    if len(s) > width:
        s = f"{v:.{decimals}f}"
    if len(s) > width:
        s = s[-width:]
    return s.rjust(width)


def _title_block() -> list:
    return [
        "PUBLIC ISLAMIC BANK BERHAD",
        f"TIME TO MATURITY AS AT {RDATE}",
        "RISK MANAGEMENT REPORT : EIIMRM03",
        "RM DENOMINATION",
        "",
    ]


def _render_tabulate(rows: list) -> list:
    """
    Emulates: TABLE PRODTYP*SUBTTL*REMMTH1,
                    (SUBTYP)*((TYPE)*SUM*(AMOUNT WACOST WAREMM))
              / BOX='DEPOSITS' RTS=65 CONDENSE;

    Column hierarchy is two levels deep: each present SUBTYP spans its
    present TYPE children, and each (SUBTYP,TYPE) leaf carries the 3
    measure columns. Horizontal pagination ('(Continued)') splits leaf
    columns into chunks that fit LINE_SIZE; vertical pagination starts a
    new page (form feed + repeated titles + full header block) once a
    chunk's row count would exceed PAGE_SIZE.
    """
    present_subtyp = [s for s in SUBTYP_ORDER if any(r["SUBTYP"] == s for r in rows)]
    if not present_subtyp:
        return []

    subtyp_children = {}
    for s in present_subtyp:
        children = [t for t in TYPE_ORDER if any(r["SUBTYP"] == s and r["TYPE"] == t for r in rows)]
        subtyp_children[s] = children
    leaf_columns = [(s, t) for s in present_subtyp for t in subtyp_children[s]]

    cell = {}
    seen_keys = set()
    row_keys = []
    for r in rows:
        bucket = r.get("REMMTH1") or ""
        key = (r["PRODTYP"], r["SUBTTL"], bucket)
        cell.setdefault(key, {})[(r["SUBTYP"], r["TYPE"])] = r
        if key not in seen_keys:
            seen_keys.add(key)
            row_keys.append(key)

    # 'SUB-TOTAL' bucket sorts after every real bucket for a given group.
    row_keys.sort(key=lambda k: (
        k[0], k[1],
        (1, 0) if k[2] == "SUB-TOTAL" else (0, _remfmt_sort_key(k[2])),
    ))

    max_leaves_per_chunk = max(1, (LINE_SIZE - (LABEL_WIDTH + 2)) // GROUP_WIDTH)
    leaf_chunks = [
        leaf_columns[i:i + max_leaves_per_chunk]
        for i in range(0, len(leaf_columns), max_leaves_per_chunk)
    ]

    output: list = []

    for chunk_idx, chunk_leaves in enumerate(leaf_chunks):
        n_leaves = len(chunk_leaves)

        chunk_subtyp_spans = []
        i = 0
        while i < n_leaves:
            s = chunk_leaves[i][0]
            span = 0
            while i + span < n_leaves and chunk_leaves[i + span][0] == s:
                span += 1
            chunk_subtyp_spans.append((s, span))
            i += span

        state = {"lines_on_page": 0}

        def _emit_page(with_titles: bool):
            block = []
            if with_titles:
                block.append(FF)
                block.extend(_title_block())

            total_w = _total_width(n_leaves)
            block.append(_dashes(total_w))

            block.append(
                "|" + "DEPOSITS".ljust(LABEL_WIDTH)[:LABEL_WIDTH] + "|"
                + _center("", n_leaves * GROUP_WIDTH - 1) + "|"
            )
            block.append(
                "|" + " " * LABEL_WIDTH + "|" + _dashes(n_leaves * GROUP_WIDTH - 1) + "|"
            )

            subtyp_cells = [
                _center(s, span * GROUP_WIDTH - 1) for s, span in chunk_subtyp_spans
            ]
            block.append("|" + " " * LABEL_WIDTH + "|" + "|".join(subtyp_cells) + "|")
            block.append(
                "|" + " " * LABEL_WIDTH + "|"
                + "+".join([_dashes(span * GROUP_WIDTH - 1) for _, span in chunk_subtyp_spans])
                + "|"
            )

            type_cells = [_center(t, GROUP_INNER_WIDTH) for _, t in chunk_leaves]
            block.append("|" + " " * LABEL_WIDTH + "|" + "|".join(type_cells) + "|")
            block.append(
                "|" + " " * LABEL_WIDTH + "|" + "+".join([_dashes(GROUP_INNER_WIDTH)] * n_leaves) + "|"
            )

            for line_idx in range(HEADER_ROWS):
                parts = [" " * LABEL_WIDTH]
                for _ in chunk_leaves:
                    parts.append(_AMOUNT_HEADER[line_idx])
                    parts.append(_WACOST_HEADER[line_idx])
                    parts.append(_WAREMM_HEADER[line_idx])
                block.append("|" + "|".join(parts) + "|")

            group_dash = "-" * 12 + "+" + "-" * 12 + "+" + "-" * 5
            block.append(
                "|" + "-" * LABEL_WIDTH + "+" + "+".join([group_dash] * n_leaves) + "|"
            )

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
            for leaf in chunk_leaves:
                rec = cell[key].get(leaf)
                amount = rec["AMOUNT"] if rec else None
                wacost = rec["WACOST"] if rec else None
                waremm = rec["WAREMM"] if rec else None
                group_cells.append((
                    _fmt_num(amount, 12, 0),
                    _fmt_num(wacost, 12, 2),
                    _fmt_num(waremm, 5, 2),
                ))

            parts = [label]
            for amount_s, wacost_s, waremm_s in group_cells:
                parts.append(amount_s)
                parts.append(wacost_s)
                parts.append(waremm_s)
            output.append("|" + "|".join(parts) + "|")
            state["lines_on_page"] += 1

        output.append(_dashes(_total_width(n_leaves)))

        if chunk_idx < len(leaf_chunks) - 1:
            output.append("")
            output.append("(Continued)")
            output.append("")
            output.append("")

    return output


report_lines = _render_tabulate(depfinal_all)

# ============================================================================
# STEP 11: WRITE OUTPUT
# ============================================================================
with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in report_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(report_lines):,}")
# print("\n--- Report preview (first 40 lines) ---")
# for ln in report_lines[:40]:
#     print(ln)

print("\nEIIMRM03 complete.")
