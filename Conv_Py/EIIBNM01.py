#!/usr/bin/env python3
"""
Program : EIIBNM01.py
Purpose : All Loans / Bank Trade report for BNM/PIBB submission -- builds a
          combined loan (ALM) and bank-trade (ALMBT) book, classifies every
          facility into a PRODESC category (housing, hire purchase, OD
          corporate/retail, bank trade, etc.), then produces a family of
          PROC PRINT style summary reports (All / Retail / SME / DBE / DNBFI /
          Foreign-Entity cuts, for both loans and bank trade) plus two
          PROC TABULATE style breakdowns (by facility TYPE and by sector).

Dependency:
    %INC PGM(PBBLNFMT);  -> only PUT(SECTORCD,$FISSTYPE.) and
                             PUT(SECTORCD,$FISSGROUP.) are actually called
                             anywhere in this program's body, so only
                             format_fisstype()/format_fissgroup() are
                             imported from PBBLNFMT.py. No other PBBLNFMT
                             format (LNPROD, LNDENOM, ODPROD, LNRATE, ...) is
                             referenced anywhere in EIIBNM01, so they are
                             intentionally NOT imported here.

    //PGM DD DSN=SAP.BNM.PROGRAM is the SAS program library the %INC pulls
    from -- it carries no data and has no physical Parquet input.

============================================================================
PHYSICAL INPUT DATASETS  (each cached to Parquet independently)
============================================================================
1. isasd_loan.sas7bdat      (JCL //ISASD DD DSN=SAP.PIBB.STORE.SASDATA,
                              member LOAN&REPTMON)
   Store-book loan master for the report month. Deterministic filename
   (fully derived from REPTMON) -- built directly, no input_date scan.

2. bnm_loan_cur.sas7bdat     (JCL //BNM DD DSN=SAP.PIBB.SASDATA,
                               member LOAN&REPTMON&NOWK)
   Current-week/-month loan master. Read TWICE in the original SAS: once
   sorted BY ACCTNO NOTENO (-> MLOAN) and once, completely independently,
   sorted BY ACCTNO COMMNO (-> LOAN2) for the ALM/ALMBT build.

3. bnm_loan_prev.sas7bdat    (BNM.LOAN&REPTMON2&NOWK -- prior month)
4. bnm_lnwof_cur.sas7bdat    (BNM.LNWOF&REPTMON&NOWK  -- write-off, current)
5. bnm_lnwod_cur.sas7bdat    (BNM.LNWOD&REPTMON&NOWK  -- write-down, current)
6. bnm_lnwof_prev.sas7bdat   (BNM.LNWOF&REPTMON2&NOWK -- write-off, prior)
7. bnm_lnwod_prev.sas7bdat   (BNM.LNWOD&REPTMON2&NOWK -- write-down, prior)
   Datasets 2-7 all share the same loan-record layout (ACCTNO, NOTENO,
   FISSPURP, PRODUCT, NOTETERM, EARNTERM, BALANCE, BAL_AFT_EIR, PAIDIND,
   APPRDATE, APPRLIM2, PRODCD, CUSTCD, AMTIND, SECTORCD, ACCTYPE, BRANCH,
   DNBFISME, NOACCT, COMMNO, EIR_ADJ, RLEASAMT, CJFEE) -- the SAS source
   never KEEPs/DROPs any of these before the MERGEs, so all are assumed to
   carry the full loan-record layout.

8. btbnm_ibtrad.sas7bdat     (JCL //BTBNM DD DSN=SAP.IBT.SASDATA,
                               member IBTRAD&REPTMON&NOWK)
   Bank-trade (bill) transactions.

9. dispay_idispaymth.sas7bdat (JCL //DISPAY DD DSN=SAP.PIBB.DISPAY,
                                 member IDISPAYMTH&REPTMON)
   Monthly disbursement/repayment feed.

10. loan_lncomm.sas7bdat     (JCL //LOAN DD DSN=SAP.PIBB.MNILN(0),
                               member LNCOMM)
    Commitment-linked utilisation. No date token in the member name (a
    fixed "(0)" generation) -- fixed filename, not date-scanned.

============================================================================
OUTPUT
============================================================================
//SASLIST DD DSN=SAP.PIBB.EIIBNM01.TEXT, DISP=(NEW,CATLG,DELETE),
   DCB=(RECFM=FB,LRECL=133,BLKSIZE=0)
RECFM=FB (NOT FBA) means this report carries NO ASA carriage-control byte
(per project convention: RECFM=FBA implies ASA control, RECFM=FB does not,
exactly as established for EIIMRM01's //TEMP DD). Page breaks are marked
with a form-feed character; PAGESIZE is not specified in the SAS source so
the project default of 60 lines/page is used.
The preceding //DELETE EXEC PGM=IEFBR14 step exists only to purge/recreate
the catalogued dataset before the run -- opening OUTPUT_FILE in "w" mode
achieves the same effect, so no separate delete step is coded.

//MFRS DD DSN=SAP.PIBB.MFRS.DETAILS, DISP=OLD is an existing SAS library
that two data-extract "members" are written into:
   MFRS.MAST_BR (KEEP=ACCTNO PRODESC NOACCT)
   MFRS.ALM_CR  (KEEP=ACCTNO NOTENO PRODESC NOACCT)
Neither member name carries a date token, so both are written as fixed-name
Parquet files (not report text, since they are data extracts feeding other
downstream programs, not print output).
"""

import gc
from pathlib import Path
from datetime import date, timedelta

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_monthly_reptdate_values
from PBBLNFMT_AII import format_fisstype, format_fissgroup

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

INPUT_BTBNM_DIR  = STG_DIR / "from_dwh"   # //BTBNM  DD DSN=SAP.IBT.SASDATA
INPUT_DISPAY_DIR = STG_DIR / "from_dwh"   # //DISPAY DD DSN=SAP.PIBB.DISPAY
INPUT_ISASD_DIR  = STG_DIR / "mth_bnm"    # //ISASD  DD DSN=SAP.PIBB.STORE.SASDATA
INPUT_BNM_DIR    = STG_DIR / "mth_bnm"    # //BNM    DD DSN=SAP.PIBB.SASDATA
INPUT_LOAN_DIR   = STG_DIR / "MNILN"      # //LOAN   DD DSN=SAP.PIBB.MNILN(0)

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIIBNM01"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# OUTPUT_DIR = BASE_DIR / "output" / "EIIBNM01"
# OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
# OUTPUT_FILE = OUTPUT_DIR / "EIIBNM01.txt"

# OUTPUT_MFRS_DIR = BASE_DIR / "output" / "MFRS"     # //MFRS DD DSN=SAP.PIBB.MFRS.DETAILS
OUTPUT_MFRS_DIR = BASE_DIR / "input" / "cache" / "EIIBNM01" / "MFRS"     # //MFRS DD DSN=SAP.PIBB.MFRS.DETAILS
OUTPUT_MFRS_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_MFRS_MAST_BR_FILE = OUTPUT_MFRS_DIR / "MFRS_MAST_BR.parquet"
OUTPUT_MFRS_ALM_CR_FILE  = OUTPUT_MFRS_DIR / "MFRS_ALM_CR.parquet"

CHUNK_ROWS = 500_000
PAGE_SIZE  = 60          # lines per page -- not specified in SAS -> project default

# ============================================================================
# MACRO VARIABLE LISTS  (equivalent to SAS %LET declarations)
# ============================================================================
ODCORP = (50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
          60, 61, 62, 63, 64, 65, 70, 71, 33)
# %LET ODFISS=(0311,0312,0313,0314,0315,0316); -- unquoted numeric literals,
# so SAS drops the leading zero (0311 == 311) when substituted into the IN().
ODFISS = (311, 312, 313, 314, 315, 316)
FLCORP = (180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192,
          193, 195, 197, 199, 851, 852, 853, 854, 855, 856, 857, 858, 859,
          860, 900, 901, 902, 903, 904, 905, 906, 907, 908, 909, 910,
          914, 915, 919, 920, 925, 950, 951,
          680, 681, 682, 683, 684, 685, 686, 687, 688, 689, 690)
HLWOF  = (423, 650, 651, 664)
REWOF  = (425, 654, 655, 656, 657, 658, 659, 660, 661, 662, 663, 665, 666, 667,
          421, 671)
STFLN  = (102, 103, 104, 105, 106, 107, 108)

# ============================================================================
# STEP 1: REPORT DATE
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_monthly_reptdate_values(year_format="%Y")
reptdate = reptdate_values.reptdate     # last day of the previous month

_day = reptdate.day
# SELECT(DAY(REPTDATE)): the WHEN(8)/WHEN(15)/WHEN(22) branches are
# unreachable in practice, since REPTDATE is always the last day of the
# previous month (day 28-31) -- OTHERWISE always fires. Preserved verbatim
# for source fidelity.
if _day == 8:
    SDD, WK, WK1, WK2, WK3 = 1, "1", "4", None, None
elif _day == 15:
    SDD, WK, WK1, WK2, WK3 = 9, "2", "1", None, None
elif _day == 22:
    SDD, WK, WK1, WK2, WK3 = 16, "3", "2", None, None
else:
    SDD, WK, WK1, WK2, WK3 = 23, "4", "3", "2", "1"

MM = reptdate.month
if WK == "1":
    MM1 = MM - 1 if MM - 1 != 0 else 12
else:
    MM1 = MM
MM2 = MM - 1 if MM - 1 != 0 else 12
SDATE = date(reptdate.year, MM, SDD)   # CALL SYMPUT('SDATE',...) -- never
                                        # referenced again anywhere in the
                                        # SAS body; kept only for parity.

NOWK     = WK
REPTMON  = f"{MM:02d}"
REPTMON1 = f"{MM1:02d}"   # CALL SYMPUT'd but never referenced again -- dead.
REPTMON2 = f"{MM2:02d}"
REPTYEAR = f"{reptdate.year:04d}"
REPTDAY  = f"{reptdate.day:02d}"  # CALL SYMPUT'd but never referenced -- dead.
RDATE    = reptdate.strftime("%d/%m/%y")

ts = reptdate.strftime("%y%m%d") - timedelta(days=1)

OUTPUT_DIR = BASE_DIR / "output" / "EIIBNM01"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / f"EIIBNM01_{ts}.txt"

print(f"  REPTMON/REPTMON2/NOWK : {REPTMON}/{REPTMON2}/{NOWK}")
print(f"  REPTYEAR              : {REPTYEAR}")
print(f"  RDATE                 : {RDATE}")

# ============================================================================
# DYNAMIC PHYSICAL INPUT FILES  (deterministic -- built directly from
# REPTMON/REPTMON2/NOWK, no input_date.get_latest_file() needed)
# ============================================================================
# INPUT_BTBNM_IBTRAD_FILE   = INPUT_BTBNM_DIR  / f"btbnm_ibtrad{REPTMON}{NOWK}.sas7bdat"
# INPUT_DISPAY_FILE         = INPUT_DISPAY_DIR / f"dispay_idispaymth{REPTMON}.sas7bdat"
# INPUT_ISASD_LOAN_FILE     = INPUT_ISASD_DIR  / f"isasd_loan{REPTMON}.sas7bdat"
# INPUT_BNM_LOAN_CUR_FILE   = INPUT_BNM_DIR    / f"bnm_loan{REPTMON}{NOWK}.sas7bdat"
# INPUT_BNM_LOAN_PREV_FILE  = INPUT_BNM_DIR    / f"bnm_loan{REPTMON2}{NOWK}.sas7bdat"
# INPUT_BNM_LNWOF_CUR_FILE  = INPUT_BNM_DIR    / f"bnm_lnwof{REPTMON}{NOWK}.sas7bdat"
# INPUT_BNM_LNWOD_CUR_FILE  = INPUT_BNM_DIR    / f"bnm_lnwod{REPTMON}{NOWK}.sas7bdat"
# INPUT_BNM_LNWOF_PREV_FILE = INPUT_BNM_DIR    / f"bnm_lnwof{REPTMON2}{NOWK}.sas7bdat"
# INPUT_BNM_LNWOD_PREV_FILE = INPUT_BNM_DIR    / f"bnm_lnwod{REPTMON2}{NOWK}.sas7bdat"
# INPUT_LOAN_LNCOMM_FILE    = INPUT_LOAN_DIR   / "ilncomm.sas7bdat"   # fixed -- no date token

INPUT_BTBNM_IBTRAD_FILE   = INPUT_BTBNM_DIR  / f"ibtrad08426.sas7bdat"
INPUT_DISPAY_FILE         = INPUT_DISPAY_DIR / f"idispaymth0826.sas7bdat"
INPUT_ISASD_LOAN_FILE     = INPUT_ISASD_DIR  / f"loan08.sas7bdat"
INPUT_BNM_LOAN_CUR_FILE   = INPUT_BNM_DIR    / f"loan084.sas7bdat"
INPUT_BNM_LOAN_PREV_FILE  = INPUT_BNM_DIR    / f"loan074.sas7bdat"
INPUT_BNM_LNWOF_CUR_FILE  = INPUT_BNM_DIR    / f"bnm_lnwof084.sas7bdat"
INPUT_BNM_LNWOF_PREV_FILE = INPUT_BNM_DIR    / f"bnm_lnwof074.sas7bdat"
INPUT_BNM_LNWOD_CUR_FILE  = INPUT_BNM_DIR    / f"bnm_lnwod084.sas7bdat"
INPUT_BNM_LNWOD_PREV_FILE = INPUT_BNM_DIR    / f"bnm_lnwod074.sas7bdat"
INPUT_LOAN_LNCOMM_FILE    = INPUT_LOAN_DIR   / "ilncomm.sas7bdat"   # fixed -- no date token

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
                if dtype == "object":
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


def _read_rows(parquet_path: Path, select_sql: str) -> list:
    """Read a Parquet cache through DuckDB with explicit CASTs and return
    a plain list of dict rows -- the row-oriented representation used
    throughout this program (mirrors EIIMRM01.py's `iter_rows(named=True)`
    style) so the many SAS MERGE / conditional-logic DATA steps below can
    be transcribed directly."""
    con = duckdb.connect(database=":memory:")
    df = con.execute(
        f"SELECT {select_sql} FROM read_parquet('{parquet_path.as_posix()}')"
    ).pl()
    con.close()
    return df.to_dicts()


# ============================================================================
# STEP 2: CACHE INPUT SAS FILES TO PARQUET
# ============================================================================
print("\nStep 2: Caching input SAS datasets to Parquet...")

ISASD_LOAN_CACHE     = _load_cached(INPUT_ISASD_LOAN_FILE, "ISASD_LOAN")
BNM_LOAN_CUR_CACHE    = _load_cached(INPUT_BNM_LOAN_CUR_FILE, "BNM_LOAN_CUR")
BNM_LOAN_PREV_CACHE   = _load_cached(INPUT_BNM_LOAN_PREV_FILE, "BNM_LOAN_PREV")
BNM_LNWOF_CUR_CACHE   = _load_cached(INPUT_BNM_LNWOF_CUR_FILE, "BNM_LNWOF_CUR")
BNM_LNWOD_CUR_CACHE   = _load_cached(INPUT_BNM_LNWOD_CUR_FILE, "BNM_LNWOD_CUR")
BNM_LNWOF_PREV_CACHE  = _load_cached(INPUT_BNM_LNWOF_PREV_FILE, "BNM_LNWOF_PREV")
BNM_LNWOD_PREV_CACHE  = _load_cached(INPUT_BNM_LNWOD_PREV_FILE, "BNM_LNWOD_PREV")
BTBNM_IBTRAD_CACHE    = _load_cached(INPUT_BTBNM_IBTRAD_FILE, "BTBNM_IBTRAD")
DISPAY_CACHE          = _load_cached(INPUT_DISPAY_FILE, "DISPAY")
LNCOMM_CACHE          = _load_cached(INPUT_LOAN_LNCOMM_FILE, "LNCOMM")

# ============================================================================
# COLUMN CASTS
# ============================================================================
LOAN_SELECT = (
    "CAST(ACCTNO AS BIGINT) AS ACCTNO, CAST(NOTENO AS BIGINT) AS NOTENO, "
    "CAST(FISSPURP AS INTEGER) AS FISSPURP, CAST(PRODUCT AS INTEGER) AS PRODUCT, "
    "CAST(NOTETERM AS DOUBLE) AS NOTETERM, CAST(EARNTERM AS DOUBLE) AS EARNTERM, "
    "CAST(BALANCE AS DOUBLE) AS BALANCE, CAST(BAL_AFT_EIR AS DOUBLE) AS BAL_AFT_EIR, "
    "CAST(PAIDIND AS VARCHAR) AS PAIDIND, CAST(APPRDATE AS DOUBLE) AS APPRDATE, "
    "CAST(APPRLIM2 AS DOUBLE) AS APPRLIM2, CAST(PRODCD AS VARCHAR) AS PRODCD, "
    "CAST(CUSTCD AS VARCHAR) AS CUSTCD, CAST(AMTIND AS VARCHAR) AS AMTIND, "
    "CAST(SECTORCD AS VARCHAR) AS SECTORCD, CAST(ACCTYPE AS VARCHAR) AS ACCTYPE, "
    "CAST(BRANCH AS VARCHAR) AS BRANCH, CAST(DNBFISME AS VARCHAR) AS DNBFISME, "
    "CAST(NOACCT AS DOUBLE) AS NOACCT, CAST(COMMNO AS DOUBLE) AS COMMNO, "
    "CAST(EIR_ADJ AS DOUBLE) AS EIR_ADJ, CAST(RLEASAMT AS DOUBLE) AS RLEASAMT, "
    "CAST(CJFEE AS DOUBLE) AS CJFEE"
)

IBTRAD_SELECT = (
    "CAST(ACCTNO AS BIGINT) AS ACCTNO, CAST(SUBACCT AS DOUBLE) AS SUBACCT, "
    "CAST(DIRCTIND AS VARCHAR) AS DIRCTIND, CAST(CUSTCD AS VARCHAR) AS CUSTCD, "
    "CAST(APPRLIMT AS DOUBLE) AS APPRLIMT, CAST(RETAILID AS VARCHAR) AS RETAILID, "
    "CAST(SECTORCD AS VARCHAR) AS SECTORCD, CAST(DNBFISME AS VARCHAR) AS DNBFISME, "
    "CAST(DISBURSE AS DOUBLE) AS DISBURSE, CAST(REPAID AS DOUBLE) AS REPAID, "
    "CAST(BALANCE AS DOUBLE) AS BALANCE, CAST(FISSPURP AS INTEGER) AS FISSPURP, "
    "CAST(PRODUCT AS INTEGER) AS PRODUCT, CAST(NOTETERM AS DOUBLE) AS NOTETERM, "
    "CAST(PRODCD AS VARCHAR) AS PRODCD, CAST(AMTIND AS VARCHAR) AS AMTIND, "
    "CAST(TRANSREF AS VARCHAR) AS TRANSREF"
)

DISPAY_SELECT = (
    "CAST(ACCTNO AS BIGINT) AS ACCTNO, CAST(NOTENO AS BIGINT) AS NOTENO, "
    "CAST(DISBURSE AS DOUBLE) AS DISBURSE, CAST(REPAID AS DOUBLE) AS REPAID, "
    "CAST(FISSPURP AS INTEGER) AS FISSPURP, CAST(PRODUCT AS INTEGER) AS PRODUCT, "
    "CAST(DNBFISME AS VARCHAR) AS DNBFISME, CAST(PRODCD AS VARCHAR) AS PRODCD, "
    "CAST(CUSTCD AS VARCHAR) AS CUSTCD, CAST(AMTIND AS VARCHAR) AS AMTIND, "
    "CAST(SECTORCD AS VARCHAR) AS SECTORCD, CAST(BRANCH AS VARCHAR) AS BRANCH, "
    "CAST(ACCTYPE AS VARCHAR) AS ACCTYPE"
)

LNCOMM_SELECT = (
    "CAST(ACCTNO AS BIGINT) AS ACCTNO, CAST(COMMNO AS DOUBLE) AS COMMNO, "
    "CAST(CUSEDAMT AS DOUBLE) AS CUSEDAMT"
)

print("\nStep 3: Loading cached inputs...")
isasd_loan_rows    = _read_rows(ISASD_LOAN_CACHE, LOAN_SELECT)
bnm_loan_cur_rows   = _read_rows(BNM_LOAN_CUR_CACHE, LOAN_SELECT)
bnm_loan_prev_rows  = _read_rows(BNM_LOAN_PREV_CACHE, LOAN_SELECT)
bnm_lnwof_cur_rows  = _read_rows(BNM_LNWOF_CUR_CACHE, LOAN_SELECT)
bnm_lnwod_cur_rows  = _read_rows(BNM_LNWOD_CUR_CACHE, LOAN_SELECT)
bnm_lnwof_prev_rows = _read_rows(BNM_LNWOF_PREV_CACHE, LOAN_SELECT)
bnm_lnwod_prev_rows = _read_rows(BNM_LNWOD_PREV_CACHE, LOAN_SELECT)
ibtrad_rows         = _read_rows(BTBNM_IBTRAD_CACHE, IBTRAD_SELECT)
dispay_raw_rows     = _read_rows(DISPAY_CACHE, DISPAY_SELECT)
lncomm_rows         = _read_rows(LNCOMM_CACHE, LNCOMM_SELECT)
print(f"  ISASD LOAN rows: {len(isasd_loan_rows):,}   BNM LOAN(cur) rows: {len(bnm_loan_cur_rows):,}")
print(f"  IBTRAD rows: {len(ibtrad_rows):,}   DISPAY rows: {len(dispay_raw_rows):,}   LNCOMM rows: {len(lncomm_rows):,}")

# ============================================================================
# GENERIC SAS-STYLE HELPERS
# ============================================================================
def _sort_key(v):
    """SAS missing sorts LOW -- treat None as sorting before any real value."""
    return (0,) if v is None else (1, v)


def proc_sort(rows: list, by: list, descending: set | None = None) -> list:
    """PROC SORT ... BY <by>; -- descending is a set of column names sorted
    DESCENDING (e.g. {'APPRLIMT'} for `BY ACCTNO DESCENDING APPRLIMT`)."""
    descending = descending or set()

    def key(r):
        parts = []
        for b in by:
            k = _sort_key(r.get(b))
            if b in descending:
                # invert ordering for descending numeric/text keys
                k = tuple(-x if isinstance(x, (int, float)) else x for x in k)
            parts.append(k)
        return parts

    return sorted(rows, key=key)


def sas_merge(datasets: list, by: list) -> tuple:
    """Emulates a plain `MERGE ds1 ds2 ... dsN; BY <by>;` (no IN=). Each
    input in `datasets` must already be sorted BY `by` (as PROC SORT
    would ensure). For a BY value present in more than one dataset,
    columns from datasets listed LATER in the argument list overwrite
    those from earlier ones -- SAS's last-dataset-wins rule within a BY
    group. Returns (merged_rows, contributed_flags) where
    contributed_flags[i] is a tuple of booleans (one per input dataset)
    recording whether that dataset had an observation for this BY value
    -- this is what IF A / IF B / IF A AND B subsetting checks against."""
    indexed = []
    for rows in datasets:
        d = {}
        for r in rows:
            d[tuple(r[b] for b in by)] = r
        indexed.append(d)
    all_keys = sorted(set().union(*[d.keys() for d in indexed]), key=lambda k: [_sort_key(x) for x in k])

    merged, flags = [], []
    for key in all_keys:
        row, flag = {}, []
        for d in indexed:
            present = key in d
            flag.append(present)
            if present:
                row.update(d[key])
        merged.append(row)
        flags.append(tuple(flag))
    return merged, flags


def proc_summary(rows: list, class_cols: list, sum_cols: list, missing: bool) -> list:
    """PROC SUMMARY NWAY [MISSING]; CLASS <class_cols>; VAR <sum_cols>;
    OUTPUT OUT=... SUM=; -- when `missing` is False, observations with any
    missing CLASS value are dropped before summarising (SAS default
    behaviour without the MISSING option). SUM ignores missing values
    within a group; a group where every contributing value is missing
    stays missing (None), matching SAS's SUM statistic."""
    groups = {}
    for r in rows:
        if not missing and any(r.get(c) is None for c in class_cols):
            continue
        key = tuple(r.get(c) for c in class_cols)
        g = groups.setdefault(key, {c: None for c in sum_cols})
        for c in sum_cols:
            v = r.get(c)
            if v is not None:
                g[c] = (g[c] or 0.0) + v
    out = []
    for key, sums in groups.items():
        rec = dict(zip(class_cols, key))
        rec.update(sums)
        out.append(rec)
    return out


# ============================================================================
# STEP 4: LOAN MASTER BUILD  (DLOAN / MLOAN / LNWOF / LNWOD / PLNWOF / PLNWOD)
# ============================================================================
print("\nStep 4: Building loan master (DLOAN/MLOAN/LNWOF/LNWOD)...")

DLOAN  = proc_sort(isasd_loan_rows, ["ACCTNO", "NOTENO"])
MLOAN  = proc_sort(bnm_loan_cur_rows, ["ACCTNO", "NOTENO"])
LNWOF  = proc_sort(bnm_lnwof_cur_rows, ["ACCTNO", "NOTENO"])
LNWOD  = proc_sort(bnm_lnwod_cur_rows, ["ACCTNO", "NOTENO"])
PLNWOF = proc_sort(bnm_lnwof_prev_rows, ["ACCTNO", "NOTENO"])
PLNWOD = proc_sort(bnm_lnwod_prev_rows, ["ACCTNO", "NOTENO"])
BNM_LOAN_PREV_SORTED = proc_sort(bnm_loan_prev_rows, ["ACCTNO", "NOTENO"])

# DATA LOANDM; MERGE DLOAN(IN=A) MLOAN(IN=B); BY ACCTNO NOTENO; IF A AND NOT B;
_dm_merged, _dm_flags = sas_merge([DLOAN, MLOAN], by=["ACCTNO", "NOTENO"])
LOANDM = [r for r, (a, b) in zip(_dm_merged, _dm_flags) if a and not b]

# DATA LOAN&REPTMON&NOWK (WORK dataset -- the current-month "candidate"
# loan master, distinct from BNM.LOAN&REPTMON&NOWK read again later):
# MERGE PLNWOF PLNWOD LOANDM BNM.LOAN&REPTMON2&NOWK MLOAN LNWOF LNWOD;
loan_work, _ = sas_merge(
    [PLNWOF, PLNWOD, LOANDM, BNM_LOAN_PREV_SORTED, MLOAN, LNWOF, LNWOD],
    by=["ACCTNO", "NOTENO"],
)
# * IF ACCTYPE='OD' AND PRODUCT IN (150,151,152,181) THEN DELETE;  -- commented
#   out in the original SAS; preserved as dead/disabled logic, not applied.
print(f"  loan_work rows: {len(loan_work):,}")

# ============================================================================
# STEP 5: DISPAY BUILD  (disbursement / repayment feed, merged onto loan_work)
# ============================================================================
print("\nStep 5: Building DISPAY (disbursement/repayment)...")


def _sas_round2(x):
    return None if x is None else round(x, 2)


dispay_rows = []
for r in dispay_raw_rows:
    disburse = _sas_round2(r.get("DISBURSE"))
    repaid = _sas_round2(r.get("REPAID"))
    if (disburse or 0) > 0 or (repaid or 0) > 0:
        rr = dict(r)
        rr["DISBURSE"], rr["REPAID"] = disburse, repaid
        dispay_rows.append(rr)
DISPAY_raw = proc_sort(dispay_rows, ["ACCTNO", "NOTENO"])

# DATA DISPAY; MERGE LOAN&REPTMON&NOWK(IN=A) DISPAY(IN=B DROP=PRODCD);
#   BY ACCTNO NOTENO; IF A & B;
_dispay_wo_prodcd = [{k: v for k, v in r.items() if k != "PRODCD"} for r in DISPAY_raw]
_dp_merged, _dp_flags = sas_merge([loan_work, _dispay_wo_prodcd], by=["ACCTNO", "NOTENO"])
DISPAY_work = [r for r, (a, b) in zip(_dp_merged, _dp_flags) if a and b]
print(f"  DISPAY_work rows: {len(DISPAY_work):,}")

# ============================================================================
# STEP 6: ALL LOAN - DISBURSEMENT, REPAYMENT, O/S
# ============================================================================
print("\nStep 6: Building ALM / ALMBT (loan level)...")

# PROC SORT DATA=LOAN.LNCOMM OUT=LNCOMM(KEEP=ACCTNO COMMNO CUSEDAMT);
#   BY ACCTNO COMMNO;
LNCOMM = proc_sort(lncomm_rows, ["ACCTNO", "COMMNO"])

# PROC SORT DATA=BNM.LOAN&REPTMON&NOWK OUT=LOAN; BY ACCTNO COMMNO;
LOAN2 = proc_sort(bnm_loan_cur_rows, ["ACCTNO", "COMMNO"])
# RENAME=(BALANCE=ORIBAL BAL_AFT_EIR=BALANCE)
LOAN2_renamed = []
for r in LOAN2:
    rr = dict(r)
    rr["ORIBAL"] = rr.pop("BALANCE")
    rr["BALANCE"] = rr.pop("BAL_AFT_EIR")
    LOAN2_renamed.append(rr)

_almbase_merged, _almbase_flags = sas_merge([LOAN2_renamed, LNCOMM], by=["ACCTNO", "COMMNO"])
_almbase = [r for r, (a, b) in zip(_almbase_merged, _almbase_flags) if a]

ALM_KEEP = ["ACCTNO", "NOTENO", "FISSPURP", "PRODUCT", "NOTETERM", "EARNTERM",
            "BALANCE", "PAIDIND", "APPRDATE", "APPRLIM2", "PRODCD", "CUSTCD",
            "AMTIND", "SECTORCD", "ACCTYPE", "BRANCH", "ORIBAL", "DNBFISME",
            "NOACCT", "COMMNO"]


def _alm_almbt_row(r: dict):
    """DATA ALM ALMBT; ... one observation's worth of the big conditional
    block; returns (kept_row_or_None, is_almbt)."""
    paidind = r.get("PAIDIND")
    if paidind in ("P", "C") and r.get("EIR_ADJ") is None:
        return None, False
    oribal = r.get("ORIBAL")
    if oribal == 0.0:
        return None, False
    balx = round(oribal, 2) if oribal is not None else None
    xind = "Y" if balx in (0.0, -0.0) else " "
    if xind == "Y":
        return None, False
    prodcd = r.get("PRODCD") or ""
    if not (prodcd[:2] == "34" or prodcd == "54120"):
        return None, False

    noacct = r.get("NOACCT")
    if r.get("ACCTYPE") == "LN":
        rlease = r.get("RLEASAMT")
        cjfee = r.get("CJFEE")
        product = r.get("PRODUCT")
        commno = r.get("COMMNO") or 0
        cusedamt = r.get("CUSEDAMT") or 0
        keep_noacct = (
            (rlease not in (0.0, None) and paidind not in ("P", "C") and (oribal or 0) > 0 and cjfee != oribal)
            or (rlease in (0.0, None) and paidind not in ("P", "C") and (oribal or 0) > 0 and product is not None and 600 <= product <= 699)
            or (rlease in (0.0, None) and paidind not in ("P", "C") and (oribal or 0) > 0 and commno > 0 and cusedamt > 0)
        )
        if not keep_noacct:
            noacct = 0
    if paidind not in ("P", "C") and noacct != 0 and round(oribal, 2) not in (0.0, -0.0) and oribal != 0:
        noacct = 1

    out = {k: r.get(k) for k in ALM_KEEP}
    out["NOACCT"] = noacct
    acctno, noteno, product = r.get("ACCTNO"), r.get("NOTENO"), r.get("PRODUCT")
    is_almbt = (
        (2850000000 <= acctno <= 2859999999 and noteno is not None and 40000 <= noteno <= 49999)
        or product == 444
    )
    return out, is_almbt


ALM_rows, ALMBT_rows = [], []
for r in _almbase:
    out, is_bt = _alm_almbt_row(r)
    if out is None:
        continue
    (ALMBT_rows if is_bt else ALM_rows).append(out)

# DATA ALM; SET ALM; BY ACCTNO COMMNO; dedup UNQ counter for PRODCD in
# ('34170','34190','34690') -- zero NOACCT once more than one such row
# appears within an (ACCTNO,COMMNO) group.
ALM_rows = proc_sort(ALM_rows, ["ACCTNO", "COMMNO"])
_unq_prodcds = {"34170", "34190", "34690"}
_unq = 0
_prev_key = None
for r in ALM_rows:
    key = (r["ACCTNO"], r["COMMNO"])
    if key != _prev_key:
        _unq = 0
        _prev_key = key
    if r.get("PRODCD") in _unq_prodcds:
        _unq += (r.get("NOACCT") or 0)
        if _unq > 1:
            r["NOACCT"] = 0

# DATA ALMBT; SET ALMBT; BY ACCTNO; IF FIRST.ACCTNO THEN NOACCT=1; ELSE NOACCT=0;
ALMBT_rows = proc_sort(ALMBT_rows, ["ACCTNO"])
_prev_acct = None
for r in ALMBT_rows:
    r["NOACCT"] = 1 if r["ACCTNO"] != _prev_acct else 0
    _prev_acct = r["ACCTNO"]

# DATA ALM; SET ALM ALMBT;
ALM_all = ALM_rows + ALMBT_rows
print(f"  ALM rows: {len(ALM_rows):,}   ALMBT rows: {len(ALMBT_rows):,}")

# PROC SORT DATA=DISPAY(KEEP=...) BY ACCTNO NOTENO CUSTCD FISSPURP SECTORCD;
#   WHERE SUBSTR(PRODCD,1,2)='34' OR PRODCD='54120' OR PRODUCT IN (698,699,983);
DISPAY_KEEP = ["ACCTNO", "NOTENO", "FISSPURP", "PRODUCT", "DNBFISME", "PRODCD",
               "CUSTCD", "AMTIND", "SECTORCD", "DISBURSE", "REPAID", "BRANCH", "ACCTYPE"]
DISPAY_final = []
for r in DISPAY_work:
    prodcd = r.get("PRODCD") or ""
    if prodcd[:2] == "34" or prodcd == "54120" or r.get("PRODUCT") in (698, 699, 983):
        DISPAY_final.append({k: r.get(k) for k in DISPAY_KEEP})
DISPAY_final = proc_sort(DISPAY_final, ["ACCTNO", "NOTENO", "CUSTCD", "FISSPURP", "SECTORCD"])

# PROC SORT DATA=ALM; BY ACCTNO NOTENO;
# DATA ALM; MERGE ALM(IN=B) DISPAY(IN=A); BY ACCTNO NOTENO;
#   IF REPAID>0 THEN REPAYNO=1; IF DISBURSE>0 THEN DISBNO=1;
ALM_all = proc_sort(ALM_all, ["ACCTNO", "NOTENO"])
ALM_final, _ = sas_merge([ALM_all, DISPAY_final], by=["ACCTNO", "NOTENO"])
for r in ALM_final:
    if (r.get("REPAID") or 0) > 0:
        r["REPAYNO"] = 1
    if (r.get("DISBURSE") or 0) > 0:
        r["DISBNO"] = 1
print(f"  ALM_final rows: {len(ALM_final):,}")

# ============================================================================
# STEP 7: PRODESC CLASSIFICATION  (DATA ALM; SET ALM; ... big IF/ELSE chain)
# ============================================================================
print("\nStep 7: Classifying PRODESC...")

_HP_PRODCDS = {"34111"}
PERSONAL_LOAN_PRODUCTS = {135, 136, 138, 419, 420, 422, 424, 426, 464, 465, 468, 469, 470,
                           441, 443, 475, 477, 482, 483, 490, 491, 492, 493, 496, 497, 498,
                           652, 653, 668, 669, 672, 673, 674, 675, 693}


def _classify_prodesc(r: dict) -> str:
    acctype, prodcd, product = r.get("ACCTYPE"), r.get("PRODCD") or "", r.get("PRODUCT")
    if product in PERSONAL_LOAN_PRODUCTS:
        return "PERSONAL LOANS"
    if (acctype == "LN" and prodcd in _HP_PRODCDS) or product in (698, 699, 983):
        return "HIRE PURCHASE"
    if acctype == "LN" and prodcd == "54120":
        return "HOUSE FINANCING SOLD TO CAGAMAS"
    if (acctype == "LN" and prodcd == "34120") or product in HLWOF:
        return "HOUSING LOANS"
    if acctype == "OD" and prodcd in ("34180", "34240") and product in ODCORP:
        return "OD CORPORATE"
    if acctype == "OD" and prodcd in ("34180", "34240") and product not in ODCORP:
        return "OD RETAIL"
    if acctype == "LN" and prodcd not in ("34111", "34120", "N", "M") and product in FLCORP:
        return "OTHERS CORPORATE"
    if (acctype == "LN" and prodcd not in ("34111", "34120", "N", "M") and product not in FLCORP) or product in REWOF:
        return "OTHERS RETAIL"
    return None


for r in ALM_final:
    r["PRODESC"] = _classify_prodesc(r)
    sectorcd = r.get("SECTORCD")
    r["SECTTYPE"] = format_fisstype(sectorcd)
    r["SECTGROUP"] = format_fissgroup(sectorcd)

MEASURE_COLS = ["DISBURSE", "REPAID", "BALANCE", "DISBNO", "REPAYNO", "NOACCT"]

ALMLOAN_v1 = proc_summary(ALM_final, ["PRODESC"], MEASURE_COLS, missing=True)
ALMLOAN_v2 = [r for r in ALMLOAN_v1 if r["PRODESC"] != "HOUSE FINANCING SOLD TO CAGAMAS"]
ALMHFSC = [r for r in ALMLOAN_v1 if r["PRODESC"] == "HOUSE FINANCING SOLD TO CAGAMAS"]

# MFRS.ALM_CR is populated later (KEEP=ACCTNO NOTENO PRODESC NOACCT); collect
# the rows contributing to it as we build ALM2/ALMBTCR below.
mfrs_alm_cr_rows = []
mfrs_mast_br_rows = []

# ============================================================================
# STEP 8: BTRADE - DISBURSEMENT, REPAYMENT & O/S
# ============================================================================
print("\nStep 8: Building bank-trade (BTRAD/OVC/MAST) section...")

# *** CURRENT MTH ***
btrad_src = [r for r in ibtrad_rows if r.get("DIRCTIND") == "D" and (r.get("CUSTCD") or "").strip() not in ("", None)]
BTRAD = proc_sort(btrad_src, ["ACCTNO", "APPRLIMT"], descending={"APPRLIMT"})
BTRAD = proc_sort(BTRAD, ["ACCTNO", "CUSTCD", "RETAILID"])

BTRAD1 = proc_summary(BTRAD, ["ACCTNO", "CUSTCD", "RETAILID", "SECTORCD", "DNBFISME"],
                       ["DISBURSE", "REPAID"], missing=False)
BTRAD_bal_src = [r for r in BTRAD if (r.get("APPRLIMT") or 0) > 0]
BTRAD_bal = proc_summary(BTRAD_bal_src, ["ACCTNO", "CUSTCD", "RETAILID", "SECTORCD"],
                          ["BALANCE"], missing=False)

# DATA OVC(KEEP=ACCTNO RETAILID) MAST(KEEP=ACCTNO CUSTCD BALANCE RETAILID
#   DISBNO REPAYNO NOACCT SECTORCD DNBFISME);
#   MERGE BTRAD(IN=A) BTRAD1(IN=B); BY ACCTNO CUSTCD RETAILID SECTORCD; IF B;
# BTRAD1 also carries DNBFISME (an extra CLASS var not in this BY list);
# matched here on the 4 shared BY columns -- assumes a 1:1 relationship
# between DNBFISME and the (ACCTNO,CUSTCD,RETAILID,SECTORCD) combination,
# consistent with how the source data is populated.
_bal_by_key = {}
for r in BTRAD_bal:
    _bal_by_key[(r["ACCTNO"], r["CUSTCD"], r["RETAILID"], r["SECTORCD"])] = r

OVC_rows, MAST_rows = [], []
for r in BTRAD1:
    bal_key = (r["ACCTNO"], r["CUSTCD"], r["RETAILID"], r["SECTORCD"])
    bal_row = _bal_by_key.get(bal_key)
    balance = bal_row["BALANCE"] if bal_row else None
    disbno = 1 if (r.get("DISBURSE") or 0) > 0 else None
    repayno = 1 if (r.get("REPAID") or 0) > 0 else None
    noacct = None
    if bal_row is not None and round(balance, 2) not in (None, 0.0) and noacct != 0:
        noacct = 1
    OVC_rows.append({"ACCTNO": r["ACCTNO"], "RETAILID": r["RETAILID"]})
    MAST_rows.append({
        "ACCTNO": r["ACCTNO"], "CUSTCD": r["CUSTCD"], "BALANCE": balance,
        "RETAILID": r["RETAILID"], "DISBNO": disbno, "REPAYNO": repayno,
        "NOACCT": noacct, "SECTORCD": r["SECTORCD"], "DNBFISME": r["DNBFISME"],
    })
OVC = proc_sort(OVC_rows, ["ACCTNO"])
MAST = proc_sort(MAST_rows, ["ACCTNO"])

# PROC SORT DATA=BTBNM.IBTRAD&REPTMON&NOWK OUT=ALMBT(KEEP=...)
#   BY ACCTNO SUBACCT TRANSREF CUSTCD FISSPURP SECTORCD;
#   WHERE SUBSTR(PRODCD,1,2) EQ '34';
ALMBT_KEEP2 = ["ACCTNO", "SUBACCT", "FISSPURP", "PRODUCT", "NOTETERM", "BALANCE",
               "APPRLIM2", "PRODCD", "CUSTCD", "AMTIND", "TRANSREF", "SECTORCD",
               "DISBURSE", "REPAID", "DNBFISME"]
ALMBT_bt = [{k: r.get(k) for k in ALMBT_KEEP2} for r in ibtrad_rows if (r.get("PRODCD") or "")[:2] == "34"]
ALMBT_bt = proc_sort(ALMBT_bt, ["ACCTNO", "SUBACCT", "TRANSREF", "CUSTCD", "FISSPURP", "SECTORCD"])

# DATA ALMBT; MERGE OVC ALMBT(IN=A); BY ACCTNO; IF A;
_ovc_merged, _ovc_flags = sas_merge([OVC, ALMBT_bt], by=["ACCTNO"])
ALMBT_bt2 = [r for r, (a, b) in zip(_ovc_merged, _ovc_flags) if b]

ALMBTX = proc_summary(ALMBT_bt2, ["ACCTNO", "TRANSREF", "CUSTCD", "FISSPURP", "SECTORCD"],
                       ["BALANCE"], missing=True)

# PROC SORT DATA=ALMBT NODUPKEYS; BY ACCTNO TRANSREF CUSTCD FISSPURP SECTORCD;
_seen_bt_keys = set()
ALMBT_bt2_dedup = []
for r in proc_sort(ALMBT_bt2, ["ACCTNO", "TRANSREF", "CUSTCD", "FISSPURP", "SECTORCD"]):
    key = (r["ACCTNO"], r["TRANSREF"], r["CUSTCD"], r["FISSPURP"], r["SECTORCD"])
    if key not in _seen_bt_keys:
        _seen_bt_keys.add(key)
        ALMBT_bt2_dedup.append(r)

# DATA ALMBT; MERGE ALMBT ALMBTX; BY ACCTNO TRANSREF CUSTCD FISSPURP SECTORCD;
ALMBT_final, _ = sas_merge([ALMBT_bt2_dedup, ALMBTX], by=["ACCTNO", "TRANSREF", "CUSTCD", "FISSPURP", "SECTORCD"])

# *** SUMMARY ***  DATA ALMBT; KEEP FISSPURP DISBURSE REPAID APPRLIM2 BALANCE
#   RETAILID AMTIND CUSTCD PRODCD SECTORCD PRODUCT PRODESC SECTTYPE SECTGROUP DNBFISME;
for r in ALMBT_final:
    r["SECTTYPE"] = format_fisstype(r.get("SECTORCD"))
    r["SECTGROUP"] = format_fissgroup(r.get("SECTORCD"))
    r["PRODESC"] = "BILLS CORPORATE" if r.get("RETAILID") == "C" else "BILLS RETAIL"

# *** NO OF A/C ***  DATA MAST(...) MFRS.MAST_BR(KEEP=ACCTNO PRODESC NOACCT);
for r in MAST:
    r["SECTTYPE"] = format_fisstype(r.get("SECTORCD"))
    r["SECTGROUP"] = format_fissgroup(r.get("SECTORCD"))
    r["PRODESC"] = "BILLS CORPORATE" if r.get("RETAILID") == "C" else "BILLS RETAIL"
    acctno = int(r["ACCTNO"]) if r.get("ACCTNO") is not None else None
    mfrs_mast_br_rows.append({"ACCTNO": acctno, "PRODESC": r["PRODESC"], "NOACCT": r["NOACCT"]})

ALMBTRD = proc_summary(ALMBT_final, ["PRODESC"], MEASURE_COLS[:3], missing=True)
MASTLOAN = proc_summary(MAST, ["PRODESC"], ["DISBNO", "REPAYNO", "NOACCT"], missing=True)
ALMBTRD, _ = sas_merge([ALMBTRD], by=["PRODESC"])  # no-op single-input merge kept for symmetry with SAS
_almbtrd_by_key = {r["PRODESC"]: r for r in ALMBTRD}
for r in MASTLOAN:
    tgt = _almbtrd_by_key.setdefault(r["PRODESC"], {"PRODESC": r["PRODESC"], "DISBURSE": None, "REPAID": None, "BALANCE": None})
    tgt.update({k: r[k] for k in ("DISBNO", "REPAYNO", "NOACCT")})
ALMBTRD = list(_almbtrd_by_key.values())

# DATA ALMLOAN ALMHFSC; SET ALMLOAN; -- (the /* */ commented SET ALMBTRD
# ALMLOAN; RUN; block above is intentionally never executed in the source
# and is preserved only as a comment there; not reproduced as live code.)
print(f"  ALMBTRD rows: {len(ALMBTRD):,}")

# ============================================================================
# STEP 9: RETAIL / COMMERCIAL RETAIL BREAKDOWN  (ALM2 / ALMBTCR)
# ============================================================================
print("\nStep 9: Building retail/commercial-retail breakdown...")


def _classify_alm2(r: dict) -> tuple:
    """DATA ALM2; SET ALM; WHERE PRODESC IN ('OD RETAIL','OTHERS RETAIL');
    returns (new_prodesc, type_label) or None if the row is filtered out."""
    prodesc = r.get("PRODESC")
    if prodesc not in ("OD RETAIL", "OTHERS RETAIL"):
        return None
    if prodesc == "OD RETAIL":
        new_desc = "PURCHASE OF RESIDENTIAL PROPERTY" if r.get("FISSPURP") in ODFISS else "TOTAL COMMERCIAL RETAILS"
        return new_desc, "CASH LINE FACILITY"
    new_desc = "STAFF FINANCING" if r.get("PRODUCT") in STFLN else "TOTAL COMMERCIAL RETAILS"
    return new_desc, "FIXED FINANCING"


ALM2 = []
for r in ALM_final:
    res = _classify_alm2(r)
    if res is None:
        continue
    rr = dict(r)
    rr["PRODESC"], rr["TYPE"] = res
    ALM2.append(rr)

ALMBTCR = []
for r in ALMBTRD:
    if r.get("PRODESC") != "BILLS RETAIL":
        continue
    rr = dict(r)
    rr["PRODESC"] = "TOTAL COMMERCIAL RETAILS"
    rr["TYPE"] = "BANK TRADE"
    ALMBTCR.append(rr)

# DATA ALMLOAN2 ALM2CRF MFRS.ALM_CR(KEEP=ACCTNO NOTENO PRODESC NOACCT);
#   SET ALM2 ALMBTCR; OUTPUT ALMLOAN2; OUTPUT MFRS.ALM_CR;
#   IF PRODESC='TOTAL COMMERCIAL RETAILS' THEN ... OUTPUT ALM2CRF;
ALMLOAN2_pre, ALM2CRF_pre = [], []
for r in (ALM2 + ALMBTCR):
    ALMLOAN2_pre.append(r)
    mfrs_alm_cr_rows.append({
        "ACCTNO": int(r["ACCTNO"]) if r.get("ACCTNO") is not None else None,
        "NOTENO": int(r["NOTENO"]) if r.get("NOTENO") is not None else None,
        "PRODESC": r.get("PRODESC"), "NOACCT": r.get("NOACCT"),
    })
    if r.get("PRODESC") == "TOTAL COMMERCIAL RETAILS":
        rr = dict(r)
        custcd = r.get("CUSTCD")
        rr["PRODESC"] = "COMMERCIAL RETAIL - IND" if custcd in ("77", "78", "95", "96") else "COMMERCIAL RETAIL - NON IND"
        ALM2CRF_pre.append(rr)

ALMLOAN2_v1 = proc_summary(ALMLOAN2_pre, ["PRODESC"], MEASURE_COLS, missing=True)
ALM2CRF_v1 = proc_summary(ALM2CRF_pre, ["PRODESC"], MEASURE_COLS, missing=True)
print(f"  ALMLOAN2 rows: {len(ALMLOAN2_v1):,}   ALM2CRF rows: {len(ALM2CRF_v1):,}")

# ============================================================================
# STEP 10: SME / DBE / DNBFI / FOREIGN-ENTITY BREAKDOWN
# ============================================================================
print("\nStep 10: Building SME/DBE/DNBFI/FBE breakdown...")

DBE_CODES = {"41", "42", "43", "44", "46", "47", "48", "49", "51", "52", "53", "54"}
FBE_CODES = {"87", "88", "89"}
SME_CODES = DBE_CODES | FBE_CODES
DNBFI_CODES = {"1", "2", "3"}


def _sme_split(rows: list) -> tuple:
    sme, dbe, dnbfi, fbe = [], [], [], []
    for r in rows:
        custcd, dnb = r.get("CUSTCD"), r.get("DNBFISME")
        if custcd in SME_CODES or dnb in DNBFI_CODES:
            sme.append(r)
        if custcd in DBE_CODES:
            dbe.append(r)
        if dnb in DNBFI_CODES:
            dnbfi.append(r)
        if custcd in FBE_CODES:
            fbe.append(r)
    return sme, dbe, dnbfi, fbe


ALMSME, DBE, DNBFI, FBE = _sme_split(ALM_final)

ALMLOAN_v3 = proc_summary(ALMSME, ["PRODESC"], MEASURE_COLS, missing=True)

ALMSMEBT_pre = [r for r in ALMBT_final if r.get("CUSTCD") in SME_CODES or r.get("DNBFISME") in DNBFI_CODES]
MASTSME_pre = [r for r in MAST if r.get("CUSTCD") in SME_CODES or r.get("DNBFISME") in DNBFI_CODES]

ALMSMEBT_sum = proc_summary(ALMSMEBT_pre, ["PRODESC", "CUSTCD", "DNBFISME"], ["DISBURSE", "REPAID", "BALANCE"], missing=True)
MASTSME_sum = proc_summary(MASTSME_pre, ["PRODESC", "CUSTCD", "DNBFISME"], ["DISBNO", "REPAYNO", "NOACCT"], missing=True)

_mastsme_by_key = {(r["PRODESC"], r["CUSTCD"], r["DNBFISME"]): r for r in MASTSME_sum}
ALMSMEBT_merged = []
for r in ALMSMEBT_sum:
    key = (r["PRODESC"], r["CUSTCD"], r["DNBFISME"])
    rr = dict(r)
    m = _mastsme_by_key.get(key, {})
    rr["DISBNO"], rr["REPAYNO"], rr["NOACCT"] = m.get("DISBNO"), m.get("REPAYNO"), m.get("NOACCT")
    ALMSMEBT_merged.append(rr)

DBEBT_pre = [r for r in ALMSMEBT_merged if r.get("CUSTCD") in DBE_CODES]
DNBFIBT_pre = [r for r in ALMSMEBT_merged if r.get("DNBFISME") in DNBFI_CODES]
FBEBT_pre = [r for r in ALMSMEBT_merged if r.get("CUSTCD") in FBE_CODES]

ALMBTRD2 = proc_summary(ALMSMEBT_merged, ["PRODESC"], MEASURE_COLS, missing=True)

ALMSME2, DBE2, DNBFI2, FBE2 = _sme_split(ALM2)

ALMBTCR2, DBEBT2 = [], []
for r in ALMSMEBT_merged:
    if r.get("PRODESC") != "BILLS RETAIL":
        continue
    rr = dict(r)
    rr["PRODESC"] = "TOTAL COMMERCIAL RETAILS"
    ALMBTCR2.append(rr)
    if r.get("CUSTCD") in DBE_CODES:
        DBEBT2.append(dict(rr))

ALMLOAN2_v2 = proc_summary(ALMSME2 + ALMBTCR2, ["PRODESC"], MEASURE_COLS, missing=True)
ALMLOAN3 = proc_summary(DBE2 + DBEBT2, ["PRODESC"], MEASURE_COLS, missing=True)
DBE_v2 = proc_summary(DBE, ["PRODESC"], MEASURE_COLS, missing=True)
DNBFI_v2 = proc_summary(DNBFI, ["PRODESC"], MEASURE_COLS, missing=True)
FBE_v2 = proc_summary(FBE, ["PRODESC"], MEASURE_COLS, missing=True)
DNBFI2_v2 = proc_summary(DNBFI2, ["PRODESC"], MEASURE_COLS, missing=True)
FBE2_v2 = proc_summary(FBE2, ["PRODESC"], MEASURE_COLS, missing=True)
DBEBT_v2 = proc_summary(DBEBT_pre, ["PRODESC"], MEASURE_COLS, missing=True)
DNBFIBT_v2 = proc_summary(DNBFIBT_pre, ["PRODESC"], MEASURE_COLS, missing=True)
FBEBT_v2 = proc_summary(FBEBT_pre, ["PRODESC"], MEASURE_COLS, missing=True)
print("  SME/DBE/DNBFI/FBE summaries built.")

# ============================================================================
# STEP 11: FACILITY / SECTOR BREAKDOWN  (feeds the two PROC TABULATE reports)
# ============================================================================
print("\nStep 11: Building facility/sector breakdown...")

ALMPROD = ALM2 + ALMBTCR

MASTSEC = proc_summary(MAST, ["SECTGROUP", "SECTTYPE"], ["NOACCT"], missing=False)
ALMBTSEC_raw = proc_summary(ALMBT_final, ["SECTGROUP", "SECTTYPE"], ["BALANCE"], missing=False)
_almbtsec_by_key = {(r["SECTGROUP"], r["SECTTYPE"]): r for r in ALMBTSEC_raw}
ALMBTSEC = []
for r in MASTSEC:
    key = (r["SECTGROUP"], r["SECTTYPE"])
    rr = dict(r)
    b = _almbtsec_by_key.get(key)
    rr["BALANCE"] = b["BALANCE"] if b else None
    rr["PRODESC"] = "TOTAL COMMERCIAL RETAILS"
    ALMBTSEC.append(rr)

ALMSEC = ALM2 + ALMBTSEC
print(f"  ALMPROD rows: {len(ALMPROD):,}   ALMSEC rows: {len(ALMSEC):,}")

# ============================================================================
# STEP 12: REPORT RENDERING
# ============================================================================
print("\nStep 12: Rendering report...")

LINE_SIZE = 132
FF = "\f"


def _center(text: str, width: int) -> str:
    text = text[:width]
    pad = width - len(text)
    left = pad // 2
    return " " * left + text + " " * (pad - left)


def _fmt_amt(value, width=16, decimals=2) -> str:
    if value is None:
        return "0".rjust(width)
    s = f"{float(value):.{decimals}f}"
    return s.rjust(width) if len(s) <= width else s[-width:]


def _fmt_cnt(value, width=9) -> str:
    if value is None:
        return "0".rjust(width)
    return f"{int(value)}".rjust(width)


_TITLE1 = "PUBLIC ISLAMIC BANK BERHAD"
_TITLE3 = "REPORT ID : EIIBNM01"


class _Pager:
    """Tracks lines-on-page and emits form-feed + titles at PAGE_SIZE."""

    def __init__(self, lines: list):
        self.lines = lines
        self.on_page = 0

    def new_page(self, title2: str, header_lines: list):
        self.lines.append(FF)
        self.lines.append(_TITLE1)
        self.lines.append(title2)
        self.lines.append(_TITLE3)
        self.lines.append("")
        self.lines.extend(header_lines)
        self.on_page = 5 + len(header_lines)

    def add(self, line: str):
        if self.on_page >= PAGE_SIZE:
            self.new_page(self._title2, self._header)
        self.lines.append(line)
        self.on_page += 1

    def start(self, title2: str, header_lines: list):
        self._title2, self._header = title2, header_lines
        self.new_page(title2, header_lines)


def proc_print_prodesc(pager: _Pager, rows: list, title2: str, where=None) -> None:
    """PROC PRINT DATA=...; [WHERE ...;] SUM DISBURSE REPAID BALANCE DISBNO
    REPAYNO NOACCT; TITLE2 <title2>;"""
    data = [r for r in rows if (where is None or where(r))]
    header = [
        "OBS  PRODESC" + " " * 29 + "DISBURSE".rjust(16) + "REPAID".rjust(16)
        + "BALANCE".rjust(16) + "DISBNO".rjust(9) + "REPAYNO".rjust(9) + "NOACCT".rjust(9),
        "-" * LINE_SIZE,
    ]
    pager.start(title2, header)
    totals = {c: 0.0 for c in MEASURE_COLS}
    for i, r in enumerate(data, start=1):
        line = (
            f"{i:<5}{(r.get('PRODESC') or ''):<35}"
            f"{_fmt_amt(r.get('DISBURSE'))}{_fmt_amt(r.get('REPAID'))}{_fmt_amt(r.get('BALANCE'))}"
            f"{_fmt_cnt(r.get('DISBNO'))}{_fmt_cnt(r.get('REPAYNO'))}{_fmt_cnt(r.get('NOACCT'))}"
        )
        pager.add(line)
        for c in MEASURE_COLS:
            totals[c] += r.get(c) or 0.0
    pager.add("-" * LINE_SIZE)
    total_line = (
        f"{'':<5}{'TOTAL':<35}"
        f"{_fmt_amt(totals['DISBURSE'])}{_fmt_amt(totals['REPAID'])}{_fmt_amt(totals['BALANCE'])}"
        f"{_fmt_cnt(totals['DISBNO'])}{_fmt_cnt(totals['REPAYNO'])}{_fmt_cnt(totals['NOACCT'])}"
    )
    pager.add(total_line)


def proc_tabulate_type(pager: _Pager, rows: list, title2: str) -> None:
    """PROC TABULATE ... TABLE TYPE=' ' ALL='GRAND TOTAL', SUM=' '*(BALANCE=
    'AMOUNT' NOACCT='NO. OF ACCT'*F=10.) / BOX='FACILITY' RTS=25;"""
    data = [r for r in rows if r.get("PRODESC") == "TOTAL COMMERCIAL RETAILS" and (r.get("BALANCE") or 0) != 0]
    groups = proc_summary(data, ["TYPE"], ["BALANCE", "NOACCT"], missing=True)
    header = [_center("FACILITY", 25) + "AMOUNT".rjust(16) + "NO. OF ACCT".rjust(12), "-" * LINE_SIZE]
    pager.start(title2, header)
    grand_bal, grand_acct = 0.0, 0
    for g in groups:
        pager.add(f"{(g.get('TYPE') or ''):<25}{_fmt_amt(g.get('BALANCE'))}{_fmt_cnt(g.get('NOACCT'), 12)}")
        grand_bal += g.get("BALANCE") or 0.0
        grand_acct += int(g.get("NOACCT") or 0)
    pager.add("-" * LINE_SIZE)
    pager.add(f"{'GRAND TOTAL':<25}{_fmt_amt(grand_bal)}{_fmt_cnt(grand_acct, 12)}")


def proc_tabulate_sector(pager: _Pager, rows: list, title2: str) -> None:
    """PROC TABULATE ... TABLE SECTGROUP=' '*(SECTTYPE='' ALL='SUB-TOTAL')
    ALL='GRAND TOTAL', SUM=' '*(BALANCE='AMOUNT' NOACCT='NO. OF ACCT'*F=10.)
    / BOX='SECTFISS' RTS=25;"""
    data = [r for r in rows if r.get("PRODESC") == "TOTAL COMMERCIAL RETAILS" and (r.get("BALANCE") or 0) != 0]
    header = [_center("SECTFISS", 25) + "AMOUNT".rjust(16) + "NO. OF ACCT".rjust(12), "-" * LINE_SIZE]
    pager.start(title2, header)
    by_group = {}
    for r in data:
        by_group.setdefault(r.get("SECTGROUP"), []).append(r)
    grand_bal, grand_acct = 0.0, 0
    for group_key in sorted(by_group, key=_sort_key):
        subs = proc_summary(by_group[group_key], ["SECTTYPE"], ["BALANCE", "NOACCT"], missing=True)
        sub_bal, sub_acct = 0.0, 0
        for s in subs:
            pager.add(f"{(group_key or ''):<12}{(s.get('SECTTYPE') or ''):<13}{_fmt_amt(s.get('BALANCE'))}{_fmt_cnt(s.get('NOACCT'), 12)}")
            sub_bal += s.get("BALANCE") or 0.0
            sub_acct += int(s.get("NOACCT") or 0)
        pager.add(f"{(group_key or ''):<12}{'SUB-TOTAL':<13}{_fmt_amt(sub_bal)}{_fmt_cnt(sub_acct, 12)}")
        grand_bal += sub_bal
        grand_acct += sub_acct
    pager.add("-" * LINE_SIZE)
    pager.add(f"{'GRAND TOTAL':<25}{_fmt_amt(grand_bal)}{_fmt_cnt(grand_acct, 12)}")


report_lines: list = []
pager = _Pager(report_lines)

_T = lambda label: f'{label} AS AT {REPTMON}/{REPTYEAR}'  # noqa: E731 -- mirrors "&REPTMON/&REPTYEAR" title text

proc_print_prodesc(pager, ALMLOAN_v2, _T("ALL LOANS"), where=lambda r: r["PRODESC"] != "HOUSE FINANCING SOLD TO CAGAMAS")
proc_print_prodesc(pager, ALMHFSC, _T("ALL LOANS"), where=lambda r: r["PRODESC"] == "HOUSE FINANCING SOLD TO CAGAMAS")
proc_print_prodesc(pager, ALMLOAN2_v1, _T("RETAILS LOANS"))
proc_print_prodesc(pager, ALM2CRF_v1, _T("COMMERCIAL RETAIL LOANS"))
proc_print_prodesc(pager, ALMLOAN_v3, _T("SME LOANS"), where=lambda r: r["PRODESC"] != "HOUSE FINANCING SOLD TO CAGAMAS")
proc_print_prodesc(pager, ALMLOAN_v3, _T("SME LOANS"), where=lambda r: r["PRODESC"] == "HOUSE FINANCING SOLD TO CAGAMAS")
proc_print_prodesc(pager, ALMLOAN2_v2, _T("RETAILS SME LOANS"))
proc_print_prodesc(pager, DBE_v2, _T("OF WHICH : SME DBE"), where=lambda r: r["PRODESC"] != "HOUSE FINANCING SOLD TO CAGAMAS")
proc_print_prodesc(pager, DBE_v2, _T("OF WHICH : SME DBE"), where=lambda r: r["PRODESC"] == "HOUSE FINANCING SOLD TO CAGAMAS")
proc_print_prodesc(pager, ALMLOAN3, _T("OF WHICH : RETAILS SME DBE"))
proc_print_prodesc(pager, DNBFI_v2, _T("OF WHICH : SME DNBFI"), where=lambda r: r["PRODESC"] != "HOUSE FINANCING SOLD TO CAGAMAS")
proc_print_prodesc(pager, DNBFI_v2, _T("OF WHICH : SME DNBFI"), where=lambda r: r["PRODESC"] == "HOUSE FINANCING SOLD TO CAGAMAS")
proc_print_prodesc(pager, DNBFI2_v2, _T("OF WHICH : RETAILS SME DNBFI"))
proc_print_prodesc(pager, FBE_v2, _T("OF WHICH : SME FE"), where=lambda r: r["PRODESC"] != "HOUSE FINANCING SOLD TO CAGAMAS")
proc_print_prodesc(pager, FBE_v2, _T("OF WHICH : SME FE"), where=lambda r: r["PRODESC"] == "HOUSE FINANCING SOLD TO CAGAMAS")
proc_print_prodesc(pager, FBE2_v2, _T("OF WHICH : RETAILS SME FE"))
proc_print_prodesc(pager, ALMBTRD, _T("BANK TRADE"))
proc_print_prodesc(pager, ALMBTRD2, _T("SME BANK TRADE"))
proc_print_prodesc(pager, DBEBT_v2, _T("OF WHICH : SME DBE BANK TRADE"))
proc_print_prodesc(pager, DNBFIBT_v2, _T("OF WHICH : SME DNBFI BANK TRADE"))
proc_print_prodesc(pager, FBEBT_v2, _T("OF WHICH : SME FE BANK TRADE"))

proc_tabulate_type(pager, ALMPROD, f"TOTAL COMMERCIAL RETAIL FINANCING BY FACILITY AS AT {RDATE}")
proc_tabulate_sector(pager, ALMSEC, f"TOTAL COMMERCIAL RETAIL FINANCING BY SECTOR AS AT {RDATE}")

# ============================================================================
# STEP 13: WRITE OUTPUTS
# ============================================================================
print("\nStep 13: Writing outputs...")

with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in report_lines:
        fh.write(ln + "\n")
print(f"  SASLIST report written : {OUTPUT_FILE}  ({len(report_lines):,} lines)")

pl.DataFrame(mfrs_mast_br_rows, schema={"ACCTNO": pl.Int64, "PRODESC": pl.Utf8, "NOACCT": pl.Float64}) \
    .write_parquet(OUTPUT_MFRS_MAST_BR_FILE)
print(f"  MFRS.MAST_BR written   : {OUTPUT_MFRS_MAST_BR_FILE}  ({len(mfrs_mast_br_rows):,} rows)")

pl.DataFrame(
    mfrs_alm_cr_rows,
    schema={"ACCTNO": pl.Int64, "NOTENO": pl.Int64, "PRODESC": pl.Utf8, "NOACCT": pl.Float64},
).write_parquet(OUTPUT_MFRS_ALM_CR_FILE)
print(f"  MFRS.ALM_CR written    : {OUTPUT_MFRS_ALM_CR_FILE}  ({len(mfrs_alm_cr_rows):,} rows)")

print("\nEIIBNM01 complete.")
