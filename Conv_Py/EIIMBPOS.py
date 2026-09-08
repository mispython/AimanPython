#!/usr/bin/env python3
"""
Program : EIIMBPOS.py
Purpose : Branch Deposit Position Report (BR-DEP-POS) for Public Islamic
          Bank Berhad -- summarises Savings, Current, Fixed Deposit, PB
          Premium Club membership (Savings/Current/Housing Loans) and
          PB Telebanking subscriber counts/amounts by BRANCH / GROUP /
          CATG / ITEM.

============================================================================
PHYSICAL INPUT DATASETS  (each cached to Parquet independently, using the
same chunked sas7bdat -> Parquet -> cache pattern)
============================================================================
1. main_fd.sas7bdat    (JCL DD DSN=SAP.PIBB.MNITB(0)) (PBB+PIBB combined
   account master for FD)
   File : INPUT_MAIN_FD_FILE -> intg_dp_acct_fd_d19.sas7bdat
   Cols used : ACCTNO, ENTITY_CD
   Used only to build the list of PIBB-only account numbers, since
   fd.sas7bdat itself is a mixed PBB/PIBB dataset with no ENTITY_CD column.

2. fd.sas7bdat         (JCL //FD DD DSN=SAP.PIBB.MNIFD(0))
   File : INPUT_FD_FILE -> enrh_dp_fd_cert_d19.sas7bdat
   Cols used : ACCT_NUM, CD_NO, INT_PLAN, OPEN_IND, CURR_BAL, BRANCH
   Used : DATA FD(RENAME=(INTPLAN=PRODUCT)) step. Filtered to PIBB-only
          rows by inner-joining ACCT_NUM against the PIBB ACCTNO list
          derived from main_fd.sas7bdat.

3. saving.sas7bdat     (JCL //DEPOSIT DD DSN=SAP.PIBB.MNITB(0), member
   SAVING)
   File : INPUT_SAVING_FILE -> intg_dp_acct_saving_d19.sas7bdat
   Cols used : PRODUCT, OPENIND, USER3, CURBAL, BRANCH, ENTITY_CD
   Used : DATA SAVING PBSAVE step. Filtered by ENTITY_CD='PIBB' (same
          physical dataset as EIIMRM01's SAVING file).

4. current.sas7bdat    (JCL //DEPOSIT DD DSN=SAP.PIBB.MNITB(0), member
   CURRENT)
   File : INPUT_CURRENT_FILE -> intg_dp_acct_current_d19.sas7bdat
   Cols used : PRODUCT, OPENIND, USER3, CURBAL, BRANCH, ENTITY_CD
   Used : DATA CURRENT PBCURR step. Filtered by ENTITY_CD='PIBB'.

5. lnnote.sas7bdat     (JCL //LOAN DD DSN=SAP.PIBB.MNILN(0), member
   LNNOTE)
   File : INPUT_LNNOTE_FILE -> intg_ln_note_pibb_d19.sas7bdat
   Cols used : ACCTNO, NOTENO, REVERSED, PAIDIND, FLAG1, LOANTYPE,
               RISKRATE, CUSTCODE, ORGTYPE, BRANCH
   Used : PROC SORT DATA=LOAN.LNNOTE ... NODUPKEY step. This is an
          entity-specific (PIBB) physical file -- no ENTITY_CD filter
          needed (same convention as the LOAN PBB/PIBB entity-specific
          files used in the EIBMNPL suite).

6. LOAN&REPTMON&NOWK.sas7bdat (JCL //SASDATA DD DSN=SAP.PIBB.SASDATA,
   member built from &REPTMON&NOWK)
   File : INPUT_SASDATA_LOAN_FILE -> loan{REPTMON}{NOWK}.sas7bdat
   Cols used : ACCTNO, NOTENO, PRODUCT, APPRLIMT, NETPROC
   The member name is fully predictable from REPTMON/NOWK, so it is
   built deterministically here rather than via input_date.py's
   get_latest_file() (which is reserved for filenames requiring a
   directory-listing date search). NOWK is derived by exact-day
   matching (day=8/15/22/else 4), matching REPTDATE's SELECT(DAY(...))
   logic -- this differs from REPTDATE.py's own range-based NOWK, so it
   is computed locally exactly as in EIIMRM01.py.

7. kapiti5.txt         (JCL //KAPITI5 DD DSN=SAP.PBB.KAPITI5(0)) -- fixed
   width flat file, NOT converted to Parquet (kept as .txt per project
   convention for mainframe flat files).
   File : INPUT_KAPITI5_FILE
   Only BRANCHES (@209, $30) and PBBORPFB (@201, $3, a sub-field of the
   $40 REMARK1 field) are needed for the output; all other named fields
   in the original INPUT statement (CARDIN, ISSUEDT/IYEAR/IMONTH/IDAYS,
   EXPIREDT/EYEAR/EMONTH/EDAYS, CARDSTAT, SERVICEN, SERVICES,
   LASTLOG/LYEAR/LMONTH/LDAYS, ADDRESS1, ADDRESS2, LEGALID, LEGALTYP,
   CUSTNAME, REMARK1, REMARK2, ACCTNO, ACCTTYPE, PRIACIND) are read by
   the original INPUT statement but never referenced anywhere else in
   the DATA KAPITI5 step, so they are not extracted here.

DEAD / UNUSED PHYSICAL INPUTS (declared as JCL DDs, never referenced in
the SAS program body -- documented only, not converted):
    * BRHFILE DD DSN=RBP2.B033.PBB.BRANCH,DISP=SHR
      -- no INFILE BRHFILE (or any other reference) anywhere in the SAS
         source; dead input.
    * PGM DD DSN=SAP.BNM.PROGRAM,DISP=SHR
      -- no %INC PGM(...) (or any other reference) anywhere in the SAS
         source; dead input.

============================================================================
OUTPUT
============================================================================
//SASLIST DD DSN=SAP.PIBB.PDRRPT.COLD, DISP=(NEW,CATLG,DELETE),
          DCB=(RECFM=FBA,LRECL=133,BLKSIZE=0,DSORG=PS)
This is a fixed-name GDG-style catalogued dataset (no date token in the
name), so the Python output uses a fixed filename, not a dated one
(output_date.py is therefore not applicable here).

RECFM=FBA means this report DOES carry an ASA carriage-control byte (per
project convention: RECFM=FBA implies ASA control). LRECL=133 = 1 ASA
byte + 132 print columns (matches OPTIONS LS=132). PAGESIZE is
explicitly PS=65 (not the 60-line default).

The preceding //DELETE EXEC PGM=IEFBR14 step (DISP=(MOD,DELETE,DELETE)
against the same DSN) is replicated as a Path.unlink() of the output
file at program start.

Dead accumulators preserved-but-omitted: AACCNT, AFDCNT, ATOTOL, BACCNT,
BFDCNT, BTOTOL are accumulated in the original WRITE data step via "+"
sum statements but are never PUT or referenced anywhere afterwards in
the program -- they have zero effect on the report and are therefore not
computed here (same dead-code-omission convention used for EIIMRM01's
D1-D12/MD1-MD12 arrays).

AMOUNT1 (SUM=NOOFCD AMOUNT AMOUNT1 on the FDSORT PROC SUMMARY, driven by
VAR NOOFCD AMOUNT CURBAL) is likewise computed in the original SAS but
never referenced afterwards, so it is not computed here.
"""

import gc
from pathlib import Path
from datetime import date

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

INPUT_MAIN_FD_DIR   = STG_DIR / "sasdata"
INPUT_FD_DIR        = STG_DIR / "sasdata"
INPUT_SAVING_DIR    = STG_DIR / "sasdata"
INPUT_CURRENT_DIR   = STG_DIR / "sasdata"
INPUT_LNNOTE_DIR    = STG_DIR / "sasdata"
INPUT_SASDATA_LOAN_DIR = STG_DIR / "sasdata"
INPUT_KAPITI5_DIR   = STG_DIR / "flatfile"

INPUT_MAIN_FD_FILE = INPUT_MAIN_FD_DIR / "intg_dp_acct_fd_d19.sas7bdat"
INPUT_FD_FILE      = INPUT_FD_DIR / "enrh_dp_fd_cert_d19.sas7bdat"
INPUT_SAVING_FILE  = INPUT_SAVING_DIR / "intg_dp_acct_saving_d19.sas7bdat"
INPUT_CURRENT_FILE = INPUT_CURRENT_DIR / "intg_dp_acct_current_d19.sas7bdat"
INPUT_LNNOTE_FILE  = INPUT_LNNOTE_DIR / "intg_ln_note_pibb_d19.sas7bdat"
INPUT_KAPITI5_FILE = INPUT_KAPITI5_DIR / "kapiti5.txt"

# BRHFILE DD DSN=RBP2.B033.PBB.BRANCH,DISP=SHR -- declared in JCL but never
# referenced (no INFILE BRHFILE) anywhere in the SAS program body. Dead
# physical input; intentionally not converted.
#
# PGM DD DSN=SAP.BNM.PROGRAM,DISP=SHR -- declared in JCL but never used via
# %INC or any other reference in the SAS program body. Dead physical input;
# intentionally not converted.

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIIMBPOS"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR  = BASE_DIR / "output" / "EIIMBPOS"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "EIIMBPOS.txt"

CHUNK_ROWS = 500_000
PAGE_SIZE  = 65     # OPTIONS PS=65
LINE_WIDTH = 132    # OPTIONS LS=132 (RECFM=FBA LRECL=133 = 1 ASA + 132)

# ============================================================================
# STEP 0: DELETE STALE OUTPUT DATASET  (JCL //DELETE EXEC PGM=IEFBR14)
# ============================================================================
if OUTPUT_FILE.exists():
    OUTPUT_FILE.unlink()
    print(f"Step 0: Deleted stale output dataset -> {OUTPUT_FILE}")

# ============================================================================
# STEP 1: REPORT DATE  (no reptdate.parquet -- derive from REPTDATE.py)
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values(year_format="%Y")
reptdate = reptdate_values.reptdate
run_date = date.today()          # DATE = TODAY();  (actual run date, NOT reptdate)

# NOWK is derived by exact-day matching (day=8/15/22/else 4), which differs
# from REPTDATE.py's range-based NOWK. This exact-day NOWK feeds the
# SASDATA.LOAN&REPTMON&NOWK member name (unlike EIIMRM01 where NOWK was
# computed but never referenced again, here it IS used).
_day = reptdate.day
NOWK = "1" if _day == 8 else "2" if _day == 15 else "3" if _day == 22 else "4"

# REPTYRS / REPTYEAR are derived for documentation parity with the SAS
# CALL SYMPUT macros but are never referenced again anywhere else in this
# program (dead macro variables), same as in EIIMRM01.py.
REPTYRS  = reptdate.strftime("%y")
REPTYEAR = reptdate.strftime("%Y")
REPTMON  = reptdate.strftime("%m")
REPTDAY  = reptdate.strftime("%d")
RDATE    = reptdate.strftime("%d/%m/%y")     # PUT(REPTDATE,DDMMYY8.)

# SASDATA.LOAN&REPTMON&NOWK: fully predictable filename built directly from
# REPTMON/NOWK -- input_date.py's get_latest_file() is not used here since
# there is nothing to search for (the member name is deterministic).
INPUT_SASDATA_LOAN_FILE = INPUT_SASDATA_LOAN_DIR / f"loan{REPTMON}{NOWK}.sas7bdat"

print(f"  RDATE        : {RDATE}")
print(f"  REPTMON/NOWK : {REPTMON}/{NOWK}")
print(f"  SASDATA LOAN input : {INPUT_SASDATA_LOAN_FILE.name}")
print(f"  Output file  : {OUTPUT_FILE.name}")

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
# STEP 2: CACHE INPUT SAS FILES TO PARQUET
# ============================================================================
print("\nStep 2: Caching input SAS datasets to Parquet...")
MAIN_FD_CACHE       = _load_cached(INPUT_MAIN_FD_FILE, "MAIN_FD")
FD_CACHE            = _load_cached(INPUT_FD_FILE, "FD")
SAVING_CACHE        = _load_cached(INPUT_SAVING_FILE, "SAVING")
CURRENT_CACHE       = _load_cached(INPUT_CURRENT_FILE, "CURRENT")
LNNOTE_CACHE        = _load_cached(INPUT_LNNOTE_FILE, "LNNOTE")
SASDATA_LOAN_CACHE  = _load_cached(INPUT_SASDATA_LOAN_FILE, "SASDATA_LOAN")

# ============================================================================
# STEP 3: DATA SAVING PBSAVE;  SET DEPOSIT.SAVING;  (PIBB only)
# ============================================================================
print("\nStep 3: Building SAVING / PBSAVE from DEPOSIT.SAVING (PIBB)...")

con = duckdb.connect(database=":memory:")
saving_raw = con.execute(f"""
    SELECT
        CAST(PRODUCT AS INTEGER) AS PRODUCT,
        CAST(OPENIND AS VARCHAR) AS OPENIND,
        CAST(USER3   AS VARCHAR) AS USER3,
        CAST(CURBAL  AS DOUBLE)  AS CURBAL,
        CAST(BRANCH  AS INTEGER) AS BRANCH
    FROM read_parquet('{SAVING_CACHE.as_posix()}')
    WHERE ENTITY_CD = 'PIBB'
""").pl()
con.close()


def _row(branch, group, catg, item, curbal):
    return {"BRANCH": branch, "GROUP": group, "CATG": catg, "ITEM": item, "CURBAL": curbal}


def _process_saving(rows):
    saving_out, pbsave_out = [], []
    for r in rows:
        openind = r["OPENIND"]
        if openind in ("B", "C", "P", "Z"):
            continue
        product, user3, curbal, branch = r["PRODUCT"], r["USER3"], r["CURBAL"], r["BRANCH"]
        group, catg = "(1) DEPOSIT PRODUCTS", "(A) SAVINGS ACCOUNTS"

        # ELSE-IF chain: only these 7 product groups are ever OUTPUT to
        # SAVING -- any other PRODUCT value is dropped entirely.
        if product in (200, 201):
            saving_out.append(_row(branch, group, catg, "1 PLUS SAVING ACCOUNTS", curbal))
        elif product == 202:
            saving_out.append(_row(branch, group, catg, "2 YOUNG ACHIEVER S/A", curbal))
        elif product == 212:
            saving_out.append(_row(branch, group, catg, "3 WISE SAVING ACCOUNTS", curbal))
        elif product == 203:
            saving_out.append(_row(branch, group, catg, "4 50 PLUS SAVING ACCOUNTS", curbal))
        elif product == 205:
            saving_out.append(_row(branch, group, catg, "5 BASIC SAVING ACCOUNTS", curbal))
        elif product == 206:
            saving_out.append(_row(branch, group, catg, "6 BASIC 55 SAVING ACCOUNTS", curbal))
        elif product == 213:
            saving_out.append(_row(branch, group, catg, "7 PB SAVELINK ACCOUNTS", curbal))

        # Independent, unconditional check (NOT part of the ELSE chain above).
        if user3 in ("2", "4"):
            pbsave_out.append(_row(
                branch, "(2) PB PREMIUM CLUB", "(A) CLUB MEMBERS ON :",
                "2 SAVING ACCOUNTS", curbal,
            ))
    return saving_out, pbsave_out


saving_rows, pbsave_rows = _process_saving(saving_raw.iter_rows(named=True))
print(f"  SAVING rows: {len(saving_rows):,}   PBSAVE rows: {len(pbsave_rows):,}")
del saving_raw
gc.collect()

# ============================================================================
# STEP 4: DATA CURRENT PBCURR;  SET DEPOSIT.CURRENT;  (PIBB only)
# ============================================================================
print("\nStep 4: Building CURRENT / PBCURR from DEPOSIT.CURRENT (PIBB)...")

con = duckdb.connect(database=":memory:")
current_raw = con.execute(f"""
    SELECT
        CAST(PRODUCT AS INTEGER) AS PRODUCT,
        CAST(OPENIND AS VARCHAR) AS OPENIND,
        CAST(USER3   AS VARCHAR) AS USER3,
        CAST(CURBAL  AS DOUBLE)  AS CURBAL,
        CAST(BRANCH  AS INTEGER) AS BRANCH
    FROM read_parquet('{CURRENT_CACHE.as_posix()}')
    WHERE ENTITY_CD = 'PIBB'
""").pl()
con.close()


def _process_current(rows):
    current_out, pbcurr_out = [], []
    for r in rows:
        openind = r["OPENIND"]
        if openind in ("B", "C", "P"):
            continue
        product, user3, curbal, branch = r["PRODUCT"], r["USER3"], r["CURBAL"], r["BRANCH"]
        group, catg = "(1) DEPOSIT PRODUCTS", "(B) CURRENT ACCOUNTS"

        if product in (100, 102, 106, 180):
            item = "1A PLUS ACCOUNTS (CREDIT)"
            # SAS: IF CURBAL LT 0 -- SAS missing numeric sorts as LOW, so a
            # missing CURBAL also satisfies "LT 0" here; preserved as-is.
            if curbal is None or curbal < 0:
                item = "1B PLUS ACCOUNTS (DEBIT)"
            current_out.append(_row(branch, group, catg, item, curbal))
        elif product in (150, 151, 152, 181):
            current_out.append(_row(branch, group, catg, "2 ACE ACCOUNTS", curbal))
        elif product == 90:
            current_out.append(_row(branch, group, catg, "3 BASIC CURRENT ACOUNTS", curbal))

        # Two independent, unconditional IFs (NOT part of the ELSE chain
        # above -- they follow it as separate statements in the SAS source).
        if product == 91:
            current_out.append(_row(branch, group, catg, "4 BASIC 55 CURRENT ACOUNTS", curbal))
        if product in (156, 157, 158):
            current_out.append(_row(branch, group, catg, "5 PB CURRENTLINK ACOUNTS", curbal))

        # Independent PB Premium Club check.
        if product in (150, 151, 152, 100, 102, 160, 162) and user3 in ("2", "4"):
            pbcurr_out.append(_row(
                branch, "(2) PB PREMIUM CLUB", "(A) CLUB MEMBERS ON :",
                "1 CURRENT ACCOUNTS", curbal,
            ))
    return current_out, pbcurr_out


current_rows, pbcurr_rows = _process_current(current_raw.iter_rows(named=True))
print(f"  CURRENT rows: {len(current_rows):,}   PBCURR rows: {len(pbcurr_rows):,}")
del current_raw
gc.collect()

# ============================================================================
# STEP 5: DATA FD(RENAME=(INTPLAN=PRODUCT));  SET FD.FD;  (PIBB-only via
#         main_fd, same join pattern as EIIMRM01.py)
# ============================================================================
print("\nStep 5: Building FD from FD.FD (PIBB-only via main_fd)...")

_FD_GOLDEN_50 = {360, 361, 362, 363, 364, 365, 366, 460}
_FD_LIFE = {
    320, 321, 322, 323, 324, 325, 326, 327, 328, 329, 330, 331,
    420, 421, 422, 423, 424, 425, 426, 427, 428, 429, 430, 431,
}
_FD_PLUS = {
    400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413,
    414, 415, 416, 417, 418, 419,
    600, 601, 602, 603, 604, 605, 606, 607, 608, 609, 610, 611, 612, 613,
    614, 615, 616, 617, 618, 619,
    620, 621, 622, 623, 624, 625, 626, 627, 628, 629, 630, 631, 632, 633,
    634, 635, 636, 637, 638, 639,
    300, 301, 302, 303, 304, 305, 306, 307, 308, 309, 310, 311, 312, 313,
    314, 315, 316, 317, 318, 319,
    373, 374, 375, 376, 377, 378, 379, 380, 381, 382, 383, 384,
    500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512, 513,
    514, 515, 516, 517, 518, 519,
    520, 521, 522, 523, 524, 525, 526, 527, 528, 529, 530, 531, 532, 533,
    534, 535, 536, 537, 538, 539,
}

con = duckdb.connect(database=":memory:")
fd_raw = con.execute(f"""
    WITH main_fd_pibb AS (
        SELECT DISTINCT CAST(ACCTNO AS BIGINT) AS ACCTNO
        FROM read_parquet('{MAIN_FD_CACHE.as_posix()}')
        WHERE ENTITY_CD = 'PIBB'
    )
    SELECT
        CAST(f.ACCT_NUM AS BIGINT)  AS ACCTNO,
        CAST(f.CD_NO    AS INTEGER) AS CDNO,
        CAST(f.INT_PLAN AS INTEGER) AS PRODUCT,
        CAST(f.OPEN_IND AS VARCHAR) AS OPENIND,
        CAST(f.CURR_BAL AS DOUBLE)  AS CURBAL,
        CAST(f.BRANCH   AS INTEGER) AS BRANCH
    FROM read_parquet('{FD_CACHE.as_posix()}') f
    INNER JOIN main_fd_pibb m
        ON CAST(f.ACCT_NUM AS BIGINT) = m.ACCTNO
""").pl()
con.close()

print(f"  FD rows after PIBB account filter: {len(fd_raw):,}")


def _process_fd(rows):
    out = []
    for r in rows:
        if r["OPENIND"] in ("B", "C", "P"):
            continue
        intplan = r["PRODUCT"]
        if intplan in _FD_GOLDEN_50:
            item = "3 PB GOLDEN 50 PLUS"
        elif intplan in _FD_LIFE:
            item = "2 FIXED DEPOSIT LIFE"
        elif intplan in _FD_PLUS:
            item = "1 PLUS FIXED DEPOSIT"
        else:
            item = " "
        if item.strip() == "":
            continue
        out.append({
            "BRANCH": r["BRANCH"], "GROUP": "(1) DEPOSIT PRODUCTS",
            "CATG": "(C) FIXED DEPOSIT ACCOUNT", "ITEM": item,
            "ACCTNO": r["ACCTNO"], "CDNO": r["CDNO"], "CURBAL": r["CURBAL"],
        })
    return out


fd_rows = _process_fd(fd_raw.iter_rows(named=True))
print(f"  FD (item-matched) rows: {len(fd_rows):,}")
del fd_raw
gc.collect()

# ----------------------------------------------------------------------
# DATA FDSORT;  SET FD; BY BRANCH ITEM ACCTNO CDNO;
# FIRST.ACCTNO/LAST.ACCTNO accumulate CURBAL (AMOUNT) and count CDs
# (NOOFCD) per ACCTNO, outputting one row per account. Implemented as a
# direct groupby (equivalent result, avoids an unnecessary physical sort).
# ----------------------------------------------------------------------
def _fdsort_aggregate(rows):
    groups, order = {}, []
    for r in rows:
        key = (r["BRANCH"], r["ITEM"], r["ACCTNO"])
        if key not in groups:
            groups[key] = {
                "BRANCH": r["BRANCH"], "GROUP": "(1) DEPOSIT PRODUCTS",
                "CATG": "(C) FIXED DEPOSIT ACCOUNT", "ITEM": r["ITEM"],
                "AMOUNT": 0.0, "NOOFCD": 0,
            }
            order.append(key)
        g = groups[key]
        g["AMOUNT"] += r["CURBAL"] or 0.0
        g["NOOFCD"] += 1
    return [groups[k] for k in order]


fdsort_rows = _fdsort_aggregate(fd_rows)
print(f"  FDSORT (per-account) rows: {len(fdsort_rows):,}")

# ============================================================================
# STEP 6: PROC SORT DATA=LOAN.LNNOTE ... NODUPKEY  (WHERE-filtered)
# ============================================================================
print("\nStep 6: Filtering LOAN.LNNOTE...")

_LNNOTE_LOANTYPES = (
    110, 111, 112, 113, 114, 115, 116,
    200, 201, 204, 205, 209, 210, 211, 212, 214, 215,
    225, 226, 227, 228, 230, 231, 232, 233, 234,
)

con = duckdb.connect(database=":memory:")
lnnote_raw = con.execute(f"""
    SELECT ACCTNO, NOTENO, BRANCH FROM (
        SELECT
            CAST(ACCTNO AS BIGINT)    AS ACCTNO,
            CAST(NOTENO AS BIGINT)    AS NOTENO,
            CAST(BRANCH AS INTEGER)   AS BRANCH,
            ROW_NUMBER() OVER (PARTITION BY ACCTNO, NOTENO
                               ORDER BY ACCTNO, NOTENO) AS RN
        FROM read_parquet('{LNNOTE_CACHE.as_posix()}')
        WHERE COALESCE(CAST(REVERSED AS VARCHAR), '') <> 'Y'
          AND COALESCE(CAST(PAIDIND AS VARCHAR), '') <> 'P'
          AND NOTENO IS NOT NULL
          AND (
                NOTENO < 100
                OR NOTENO BETWEEN 20000 AND 29999
                OR (NOTENO BETWEEN 30000 AND 39999
                    AND COALESCE(CAST(FLAG1 AS VARCHAR), '') = 'F')
              )
          AND CAST(LOANTYPE AS INTEGER) IN {_LNNOTE_LOANTYPES}
          AND CAST(RISKRATE AS VARCHAR) NOT IN ('1','2','3','4')
          AND CAST(CUSTCODE AS INTEGER) IN (77,78,95,96)
          AND CAST(ORGTYPE AS VARCHAR) IN ('E','M','N','S')
    ) WHERE RN = 1
""").pl()
con.close()
print(f"  LNNOTE (filtered, deduped) rows: {len(lnnote_raw):,}")

# ============================================================================
# STEP 7: PROC SORT DATA=SASDATA.LOAN&REPTMON&NOWK ... NODUPKEY
# ============================================================================
print("\nStep 7: Filtering SASDATA.LOAN{REPTMON}{NOWK}...")

_SLOAN_PRODUCT_A = (200, 201, 204, 205, 209, 210, 211, 212, 214, 215,
                     225, 226, 227, 228, 230, 231, 232, 233, 234)
_SLOAN_PRODUCT_B = (110, 111, 112, 113, 114, 115, 116)

con = duckdb.connect(database=":memory:")
sloan_raw = con.execute(f"""
    SELECT ACCTNO, NOTENO FROM (
        SELECT
            CAST(ACCTNO AS BIGINT)   AS ACCTNO,
            CAST(NOTENO AS BIGINT)   AS NOTENO,
            CAST(PRODUCT AS INTEGER) AS PRODUCT,
            CAST(APPRLIMT AS DOUBLE) AS APPRLIMT,
            CAST(NETPROC AS DOUBLE)  AS NETPROC,
            ROW_NUMBER() OVER (PARTITION BY ACCTNO, NOTENO
                               ORDER BY ACCTNO, NOTENO) AS RN
        FROM read_parquet('{SASDATA_LOAN_CACHE.as_posix()}')
        WHERE (PRODUCT IN {_SLOAN_PRODUCT_A} AND APPRLIMT >= 150000)
           OR (PRODUCT IN {_SLOAN_PRODUCT_B} AND NETPROC >= 150000)
    ) WHERE RN = 1
""").pl()
con.close()
print(f"  SLOAN (filtered, deduped) rows: {len(sloan_raw):,}")

# ============================================================================
# STEP 8: DATA PBLOAN; MERGE LOAN(IN=A) SLOAN(IN=B); BY ACCTNO NOTENO;
#         IF A AND B;   (inner join)
# STEP 9: DATA PBLOANS(RENAME=(BALANCE=LEDGBAL)); SET PBLOAN; ...
#         BALANCE/LEDGBAL is never referenced downstream (AMOUNT is forced
#         to 0.00 for this ITEM in the WRITE step regardless), so the
#         rename is not carried through here.
# ============================================================================
print("\nStep 8-9: Building PBLOANS (inner join LNNOTE x SLOAN)...")

pbloan = lnnote_raw.join(sloan_raw, on=["ACCTNO", "NOTENO"], how="inner")
pbloans_rows = [
    {"BRANCH": r["BRANCH"], "GROUP": "(2) PB PREMIUM CLUB",
     "CATG": "(A) CLUB MEMBERS ON :", "ITEM": "3 HOUSING LOANS", "CURBAL": None}
    for r in pbloan.iter_rows(named=True)
]
print(f"  PBLOANS rows: {len(pbloans_rows):,}")
del lnnote_raw, sloan_raw, pbloan
gc.collect()

# ============================================================================
# STEP 10: DATA PBPREM; SET PBSAVE PBCURR PBLOANS;
# ============================================================================
print("\nStep 10: Building PBPREM...")
pbprem_rows = pbsave_rows + pbcurr_rows + pbloans_rows
print(f"  PBPREM rows: {len(pbprem_rows):,}")

# ============================================================================
# STEP 11: DATA KAPITI5;  INFILE KAPITI5;  (fixed-width flat file)
# ============================================================================
print("\nStep 11: Reading KAPITI5 flat file...")

_REMOVE_CHARS = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ ():/")


def _process_kapiti5(path: Path):
    rows = []
    with open(path, "r", encoding="latin1") as fh:
        for raw_line in fh:
            line = raw_line.rstrip("\r\n")
            if len(line) < 238:
                continue
            pbborpfb = line[200:203]              # @201 PBBORPFB $3.
            branches = line[208:238]               # @209 BRANCHES  $30.
            # BRANCI = RIGHT(COMPRESS(BRANCHES,'ABC...Z ():/'));
            compressed = "".join(
                ch for ch in branches.upper() if ch not in _REMOVE_CHARS
            )
            branci = compressed.rjust(len(branches))
            digits = branci.strip()
            if not digits.isdigit():
                continue
            branch = int(digits)                   # BRANCH = BRANCI;
            if pbborpfb == "PBB" and branch < 500:
                rows.append({
                    "BRANCH": branch, "GROUP": "(3) SERVICES",
                    "CATG": "(A) PB TELEBANKING", "ITEM": "1 SUBSCRIBERS",
                    "AMT": 0.0,
                })
    return rows


kapiti5_rows = _process_kapiti5(INPUT_KAPITI5_FILE)
print(f"  KAPITI5 (PBB, branch<500) rows: {len(kapiti5_rows):,}")

# ============================================================================
# STEP 12: PROC SUMMARY (NWAY) FOR EACH SOURCE -> _FREQ_ RENAMED TO NOACCT
# ============================================================================
print("\nStep 12: Summarising SAVING / CURRENT / FD / PBPREM / KAPITI5...")


def _group_sum_amount(rows, value_field):
    """PROC SUMMARY NWAY; CLASS BRANCH GROUP CATG ITEM; VAR <value_field>;
    OUTPUT OUT=... (RENAME=(_FREQ_=NOACCT) DROP=_TYPE_) SUM=AMOUNT;
    SUM ignores missing contributions; the result stays missing (None)
    only if every contributing observation is missing."""
    groups, order = {}, []
    for r in rows:
        key = (r["BRANCH"], r["GROUP"], r["CATG"], r["ITEM"])
        if key not in groups:
            groups[key] = {
                "BRANCH": r["BRANCH"], "GROUP": r["GROUP"], "CATG": r["CATG"],
                "ITEM": r["ITEM"], "AMOUNT": None, "NOOFCD": None, "NOACCT": 0,
            }
            order.append(key)
        g = groups[key]
        v = r.get(value_field)
        if v is not None:
            g["AMOUNT"] = (g["AMOUNT"] or 0.0) + v
        g["NOACCT"] += 1
    return [groups[k] for k in order]


def _summarize_fd(rows):
    groups, order = {}, []
    for r in rows:
        key = (r["BRANCH"], r["GROUP"], r["CATG"], r["ITEM"])
        if key not in groups:
            groups[key] = {
                "BRANCH": r["BRANCH"], "GROUP": r["GROUP"], "CATG": r["CATG"],
                "ITEM": r["ITEM"], "NOOFCD": 0.0, "AMOUNT": 0.0, "NOACCT": 0,
            }
            order.append(key)
        g = groups[key]
        g["NOOFCD"] += r["NOOFCD"] or 0.0
        g["AMOUNT"] += r["AMOUNT"] or 0.0
        g["NOACCT"] += 1
    return [groups[k] for k in order]


saving_summary  = _group_sum_amount(saving_rows, "CURBAL")
current_summary = _group_sum_amount(current_rows, "CURBAL")
fd_summary      = _summarize_fd(fdsort_rows)
pbprem_summary  = _group_sum_amount(pbprem_rows, "CURBAL")
telebank_summary = _group_sum_amount(kapiti5_rows, "AMT")

print(f"  SAVING summary: {len(saving_summary):,}  CURRENT summary: {len(current_summary):,}")
print(f"  FD summary: {len(fd_summary):,}  PBPREM summary: {len(pbprem_summary):,}")
print(f"  TELEBANK summary: {len(telebank_summary):,}")

# ============================================================================
# STEP 13: DATA PDRDATA; SET SAVING CURRENT FD PBPREM TELEBANK;
#          PROC SORT DATA=PDRDATA; BY BRANCH GROUP CATG ITEM;
# ============================================================================
print("\nStep 13: Building and sorting PDRDATA...")

pdrdata = saving_summary + current_summary + fd_summary + pbprem_summary + telebank_summary
pdrdata.sort(key=lambda r: (r["BRANCH"], r["GROUP"], r["CATG"], r["ITEM"]))
print(f"  PDRDATA rows: {len(pdrdata):,}")

# ============================================================================
# STEP 14: REPORT RENDERING  (DATA WRITE; FILE PRINT HEADER=NEWPAGE;)
# RECFM=FBA -> ASA carriage control required. PAGESIZE (PS=65) explicit.
# ============================================================================
print("\nStep 14: Rendering report...")

_BANK_TITLE = "P U B L I C   I S L A M I C   B A N K   B E R H A D"


def _new_buf(width: int = LINE_WIDTH) -> list:
    return [" "] * width


def _place(buf: list, col: int, text: str) -> None:
    """1-based SAS @col placement -> 0-based slice assignment."""
    start = col - 1
    for i, ch in enumerate(text):
        if start + i >= len(buf):
            break
        buf[start + i] = ch


def _finalize(buf: list) -> str:
    return "".join(buf)


def _fmt_int(value, width: int) -> str:
    if value is None:
        return " " * width
    s = str(int(value))
    return s[-width:] if len(s) > width else s.rjust(width)


def _fmt_comma(value, width: int, decimals: int) -> str:
    """COMMAw.d. A genuinely absent cell (no contributing rows -> None)
    prints as a blank field (no MISSING=0 option is set in the original
    SAS OPTIONS statement); a real computed zero prints fully formatted."""
    if value is None:
        return " " * width
    v = float(value)
    s = f"{v:,.{decimals}f}"
    if len(s) > width:
        s = f"{v:.{decimals}f}"
    if len(s) > width:
        s = s[-width:]
    return s.rjust(width)


def _title_line(branch) -> str:
    buf = _new_buf()
    _place(buf, 1, f"REPORT NAME : BR-DEP-POS :  BRANCH : {_fmt_int(branch, 3)}")
    _place(buf, 50, _BANK_TITLE)
    _place(buf, 110, f"DATE : {run_date.strftime('%d/%m/%y')}")
    return _finalize(buf)


def _line2() -> str:
    buf = _new_buf()
    _place(buf, 51, f"PRODUCTS POSITION AS AT : {RDATE}")
    return _finalize(buf)


def _line3() -> str:
    buf = _new_buf()
    _place(buf, 39, "NO. OF ACCOUNTS/")
    return _finalize(buf)


def _line4() -> str:
    buf = _new_buf()
    _place(buf, 6, "PRODUCT TYPES")
    _place(buf, 37, "MEMBERS/SUBSCRIBERS")
    _place(buf, 63, "NO OF RECEIPTS")
    _place(buf, 85, "AMOUNT OUTSTANDING (RM)")
    return _finalize(buf)


def _line5() -> str:
    buf = _new_buf()
    _place(buf, 6, "_____________")
    _place(buf, 37, "___________________")
    _place(buf, 63, "______________")
    _place(buf, 85, "______________________")
    return _finalize(buf)


def _group_line(group_text: str) -> str:
    buf = _new_buf()
    _place(buf, 1, group_text)
    return _finalize(buf)


def _catg_line(catg_text: str) -> str:
    buf = _new_buf()
    _place(buf, 5, catg_text)
    return _finalize(buf)


def _data_line(item, noacct, noofcd, amount) -> str:
    buf = _new_buf()
    _place(buf, 9, item)
    _place(buf, 43, _fmt_comma(noacct, 7, 0))
    _place(buf, 67, _fmt_comma(noofcd, 7, 0))
    _place(buf, 83, _fmt_comma(amount, 18, 2))
    return _finalize(buf)


report_lines: list = []       # list[(asa_char, text)]
_lines_on_page = 0


def _start_new_page(branch) -> None:
    global _lines_on_page
    report_lines.append(("1", _title_line(branch)))
    report_lines.append((" ", _line2()))
    report_lines.append((" ", _line3()))
    report_lines.append((" ", _line4()))
    report_lines.append(("+", _line5()))
    report_lines.append((" ", _finalize(_new_buf())))
    _lines_on_page = 6


def _emit(asa: str, text: str) -> None:
    global _lines_on_page
    if _lines_on_page + 1 > PAGE_SIZE:
        _start_new_page(current_branch)
    report_lines.append((asa, text))
    _lines_on_page += 1


prev_branch = prev_group = prev_catg = None
current_branch = None

for idx, r in enumerate(pdrdata):
    branch, group, catg, item = r["BRANCH"], r["GROUP"], r["CATG"], r["ITEM"]
    current_branch = branch

    first_branch = (idx == 0) or (branch != prev_branch)
    first_group = first_branch or (group != prev_group)
    first_catg = first_group or (catg != prev_catg)

    if idx == 0:
        _start_new_page(branch)                # automatic header before first line
    elif first_branch:
        _start_new_page(branch)                # PUT _PAGE_ (forced break)

    if first_group:
        _emit("0", _group_line(group))          # PUT //@001 GROUP;
    if first_catg:
        _emit("0", _catg_line(catg))            # PUT //@005 CATG;

    amount = r["AMOUNT"]
    if item.strip() == "3 HOUSING LOANS":
        amount = 0.00                           # IF ITEM='3 HOUSING LOANS' THEN AMOUNT=0.00;

    _emit(" ", _data_line(item, r["NOACCT"], r.get("NOOFCD"), amount))

    # AACCNT/AFDCNT/ATOTOL/BACCNT/BFDCNT/BTOTOL "+"-accumulators are
    # computed in the original SAS but never PUT or referenced anywhere
    # afterwards -- intentionally omitted (dead code, see module docstring).

    prev_branch, prev_group, prev_catg = branch, group, catg

print(f"  Total report lines: {len(report_lines):,}")

# ============================================================================
# STEP 15: WRITE OUTPUT  (RECFM=FBA -> leading ASA carriage-control byte)
# ============================================================================
with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for asa, text in report_lines:
        fh.write(asa + text + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(report_lines):,}")

print("\n--- Report preview (first 30 lines, ASA byte shown as first char) ---")
for asa, text in report_lines[:30]:
    print(asa + text)

print("\nEIIMBPOS complete.")
