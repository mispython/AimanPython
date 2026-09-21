#!/usr/bin/env python3
"""
Program : EIWBTR1C.py
Purpose : Undrawn Trade Bills By Collaterals With Remaining Maturity.

Dependency:
    %INC PGM(PBBLNFMT); -> from PBBLNFMT import format_btproda
    ($BTPRODA. format is applied to LIABCODE to derive PRODCD/BNMCODE
    throughout this program. No other PBBLNFMT format/list is referenced
    anywhere in the SAS source body, so nothing else is imported.)

============================================================================
PHYSICAL INPUT DATASETS  (each cached to Parquet independently)
============================================================================
1. btdtl<REPTYEAR><REPTMON><REPTDAY>.sas7bdat
   (JCL //BNMDAILY DD DSN=SAP.BT.SASDATA.DAILY(0))
   SAS ref  : BNMDAILY.BTDTL&REPTYEAR&REPTMON&REPTDAY
   Filename is fully deterministic from the report-date tokens, so it is
   built directly (input_date.get_latest_file() is not needed here).
   Cols used (resolved from how LOAN2's KEEP= list and DATA BTRADIX use
   them -- these fields are never sourced from BTMAST/COLLATER):
       ACCTNO, DIRCTIND, LIABCODE, OUTSTAND (renamed -> BALANCE), BRANCH,
       UNDRAWN, ISSDTE, EXPRDATE, ORIGMT, LIABCOD1, CDOLARV, TYP,
       XBALANCE.
   NOTE: The last two (TYP, XBALANCE) are raw pass-through columns on the
   daily extract itself -- they are unrelated in meaning to the derived
   TYP/XBALANCE variables computed later in DATA LOAN (different DATA
   step, different PDV; SAS allows the name reuse without conflict).

2. btmast<REPTMON><NOWK>.sas7bdat
   (JCL //BNM DD DSN=SAP.BT.SASDATA)
   SAS ref  : BNM.BTMAST&REPTMON&NOWK
   Filename is deterministic from REPTMON + NOWK (exact-day NOWK -- see
   Step 1; unlike EIIMRM01, NOWK is NOT dead here, it drives this
   filename). Filtered to SUBACCT='OV', deduplicated by ACCTNO
   (PROC SORT ... OUT=BTRADM NODUPKEYS).
   Cols used: ACCTNO, SUBACCT, ICURBAL, DBALANCE.

3. collater.sas7bdat
   (JCL //BTCOLL DD DSN=SAP.PBB.MNICOL(0))
   SAS ref  : BTCOLL.COLLATER
   Fixed-name catalogued member (no date token in the name) -> static
   physical filename, same pattern as EIIMRM01's SAVING/CURRENT inputs.
   Cols used: ACCTNO (SAS renames this to ACCTNO2 then reassigns it back
   to ACCTNO -- see note below), CCLASSC, CDOLARV.

   NOTE on "FORMAT ACCTNO $10.;": in the original SAS, this statement
   (placed before the SET) implicitly declares a brand-new CHARACTER
   ACCTNO variable, which is then populated via ACCTNO = ACCTNO2 (a
   numeric-to-character conversion). Every other ACCTNO in this program
   (BTRADIX/BTRADM/BTRADI/BTRADE) is numeric (used in numeric range
   comparisons such as 2500000000 <= ACCTNO <= 2599999999), and the
   downstream PROC SORT/MERGE BY ACCTNO requires matching types. For a
   working merge, ACCTNO is kept as a numeric (BIGINT) type throughout
   this conversion; the $10. character format itself has no further
   behavioural effect on the report and is therefore not reproduced.

============================================================================
OUTPUT
============================================================================
//EIWBTR1C DD DSN=SAP.PBB.EIWBTR1C.TEXT, DISP=(NEW,CATLG,DELETE),
           DCB=(RECFM=FB,LRECL=150,BLKSIZE=27000)
RECFM=FB (NOT FBA) -> per project convention, no ASA carriage-control
byte. Plain fixed-width text; page boundaries (PAGESIZE=60, not
specified in the SAS source -> default) are marked with a form-feed
character. LINE_SIZE is set to 150 to match LRECL=150.

Three PROC TABULATE reports are written to this single output, in order:
  1. REPORT ON COLLATERAL              (DATA=LOAN,  paged by BRANCH)
  2. REPORT ON REMAINING MATURITY      (DATA=LOAN2, FORMAT REMFMT.)
  3. REPORT ON REMAINING MATURITY      (DATA=LOAN2, FORMAT REMFMTS.)

OPTIONS MISSING=0 (SAS system option) -> a missing numeric cell prints as
a bare '0' character instead of blank; reproduced via _fmt_num13(None).

NOTE on table rendering: PROC TABULATE's exact character-level box
drawing (border/grid conventions) cannot be reproduced byte-for-byte
without a reference listing to compare against, so a structured,
bordered crosstab renderer (same approach as EIIMRM01.py's report
renderer) is used instead: BOX label, wrapped stacked column headers,
ALL='TOTAL'/'GRAND TOTAL' rows and columns, FORMAT=13.2 cells. Internal
PAGESIZE=60 re-pagination WITHIN a single crosstab block is not
implemented -- BNMCODE class-value cardinality (a handful of BT product
codes) is expected to comfortably fit on one logical page in practice.
The BRANCH PAGE dimension (report 1: one page per BRANCH plus a
GRAND TOTAL page) IS implemented, since it is a structural part of the
TABULATE table definition, not a display nicety.
"""

import gc
from pathlib import Path
from datetime import date, timedelta

import duckdb
import polars as pl
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values
from PBBLNFMT_AII import format_btproda

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

# Each physical input gets its own directory/path variable, per project
# convention, for clear traceability.
INPUT_BTDTL_DIR    = STG_DIR / "from_dwh"
INPUT_BTMAST_DIR   = STG_DIR / "from_dwh"
INPUT_COLLATER_DIR = STG_DIR / "MNICOL"

CACHE_DIR = BASE_DIR / "input" / "cache" / "BTRD"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS  = 500_000
PAGE_SIZE   = 60          # lines per page (not specified in SAS -> default)
LINE_SIZE   = 150         # matches LRECL=150 on the //EIWBTR1C DD
LABEL_WIDTH = 6           # RTS=8 (was 8)
NUM_WIDTH   = 13          # FORMAT=13.2 (plain w.d, no thousands separator)
FF = "\f"

# ============================================================================
# STEP 1: REPORT DATE
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values(year_format="%Y")
# reptdate = reptdate_values.reptdate

# DEBUG (UAT OVERRRIDE - Need to be removed before production)
reptdate = date(2026, 8, 31)

REPTYEAR = reptdate.strftime("%y")            # PUT(REPTDATE,YEAR2.)
REPTMON  = reptdate.strftime("%m")            # PUT(MONTH(REPTDATE),Z2.)
REPTDAY  = reptdate.strftime("%d")            # PUT(DAY(REPTDATE),Z2.)
RDATE    = reptdate.strftime("%d/%m/%y")      # PUT(REPTDATE,DDMMYY8.)

RPYR, RPMTH, RPDAY = reptdate.year, reptdate.month, reptdate.day

# NOWK: exact-day matching (8/15/22/else->4), diverging from REPTDATE.py's
# standard range-based NOWK. Unlike EIIMRM01 (where NOWK was dead code),
# NOWK IS live here -- it drives the BTMAST filename
# (BNM.BTMAST&REPTMON&NOWK) -- so it is computed exactly per the SAS
# SELECT(DAY(REPTDATE)) logic.
_day = reptdate.day
NOWK = "1" if _day == 8 else "2" if _day == 15 else "3" if _day == 22 else "4"

# Generate time stamp
report_date = date.today() - timedelta(days=1)
ts = report_date.strftime("%y%m%d")

OUTPUT_DIR  = BASE_DIR / "output" / "BTRD"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / f"EIWBTR1C_{ts}.txt"

print(f"  RDATE        : {RDATE}")
print(f"  REPTMON/DAY  : {REPTMON}/{REPTDAY}  REPTYEAR: {REPTYEAR}  NOWK: {NOWK}")

# ============================================================================
# INPUT FILE NAMES (deterministic, built directly from date tokens)
# ============================================================================
# INPUT_BTDTL_FILE    = INPUT_BTDTL_DIR / f"btdtl{REPTYEAR}{REPTMON}{REPTDAY}.sas7bdat"
# INPUT_BTMAST_FILE   = INPUT_BTMAST_DIR / f"btmast{REPTMON}{NOWK}.sas7bdat"
# INPUT_COLLATER_FILE = INPUT_COLLATER_DIR / "collater.sas7bdat"   # fixed name, no date token

INPUT_BTDTL_FILE    = INPUT_BTDTL_DIR / f"btdtl260831.sas7bdat"
INPUT_BTMAST_FILE   = INPUT_BTMAST_DIR / f"btmast08426.sas7bdat"
INPUT_COLLATER_FILE = INPUT_COLLATER_DIR / "collater.sas7bdat"   # fixed name, no date token

print(f"  Input BTDTL     : {INPUT_BTDTL_FILE}")
print(f"  Input BTMAST    : {INPUT_BTMAST_FILE}")
print(f"  Input COLLATER  : {INPUT_COLLATER_FILE}")
print(f"  Output file     : {OUTPUT_FILE.name}")

# %DCLVAR's RD1-RD12 (RPDAYS) array -- days-per-month for the report year.
# D1-D12 (LDAY) and MD1-MD12 (MDDAYS) are also RETAINed/declared by the
# original DCLVAR macro but are never referenced anywhere in %REMMTH's
# body (only RPDAYS(RPMTH) is used as both the day-cap and the
# denominator) -- they are dead declarations, omitted here, same as the
# equivalent dead arrays documented in EIIMRM01.py.
RD_DAYS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
if RPYR % 4 == 0:
    RD_DAYS[1] = 29

# ============================================================================
# PROC FORMAT EQUIVALENTS
# ============================================================================
TYPFMT_MAP = {
    1: "PBB OWN FIXED DEPOSIT, 30308 (0%)",
    2: "DISCOUNT HOUSE/CAGAMAS, 30314 (10%)",
    3: "FIN INST FIXED DEPOSIT/GUARANTEE, 30332 (20%)",
    4: "STATUTORY BODIES, 30336 (20%)",
    5: "FIRST CHARGE, 30342 (50%)",
    6: "SHARES/UNIT TRUSTS, 30360 (100%)",
    7: "OTHERS, 30360 (100%)",
}
TYPFMT_ORDER = [1, 2, 3, 4, 5, 6, 7]

# VALUE REMFMT: ranges are checked in the order declared; SAS treats an
# overlapping boundary (e.g. value=1 or value=12) as matching the FIRST
# listed range, and numeric missing sorts as LOW.
REMFMT_LABELS = [
    "UP TO 1 WK", ">1 WK - 1 MTH", ">1 MTH - 3 MTHS",
    ">3 - 6 MTHS", ">6 MTHS - 1 YR", ">1 YEAR",
]


def remfmt_format(value):
    """PROC FORMAT VALUE REMFMT."""
    if value is None or value <= 0.1:
        return "UP TO 1 WK"
    if value <= 1:
        return ">1 WK - 1 MTH"
    if value <= 3:
        return ">1 MTH - 3 MTHS"
    if value <= 6:
        return ">3 - 6 MTHS"
    if value <= 12:
        return ">6 MTHS - 1 YR"
    return ">1 YEAR"


REMFMTS_LABELS = ["<1 YEAR", ">1 YEAR"]


def remfmts_format(value):
    """PROC FORMAT VALUE REMFMTS. LOW-12 listed first -> boundary 12 wins
    '<1 YEAR' over the overlapping 12-HIGH range."""
    if value is None or value <= 12:
        return "<1 YEAR"
    return ">1 YEAR"


# %MACRO COLL -- TYP classification driven by LIABCODE (here, the
# collateral classification code CCLASSC, renamed to LIABCODE in BTRCOL).
_TYP_MAP = {}
for _c in ("007", "012", "013", "014", "021", "024", "048", "049"):
    _TYP_MAP[_c] = 1
for _c in ("017", "026", "029"):
    _TYP_MAP[_c] = 2
for _c in ("006", "011", "016", "030", "018", "027", "003"):
    _TYP_MAP[_c] = 3
_TYP_MAP["025"] = 4
_TYP_MAP["050"] = 5
for _c in ("015", "008", "042"):
    _TYP_MAP[_c] = 6


def typ_from_liabcode(liabcode) -> int:
    """%MACRO COLL; OTHERWISE DO; TYP=7; END;"""
    if liabcode is None:
        return 7
    return _TYP_MAP.get(str(liabcode).strip(), 7)


def _remmth(matdt: date) -> float:
    """%MACRO REMMTH; -- identical structure/formula to EIIMRM01's
    _remmth(): MDDAY is capped at RPDAYS(RPMTH) (report month's day
    count, NOT the maturity month's), and RPDAYS(RPMTH) is also the
    REMMTH denominator. MD2 (MDDAYS array leap-year override inside the
    macro) is dead code -- MDDAYS is never referenced elsewhere in the
    macro body, exactly like the equivalent dead array noted above."""
    mdyr, mdmth, mdday = matdt.year, matdt.month, matdt.day
    days_in_rpmth = RD_DAYS[RPMTH - 1]
    if mdday > days_in_rpmth:
        mdday = days_in_rpmth
    remy = mdyr - RPYR
    remm = mdmth - RPMTH
    remd = mdday - RPDAY
    return remy * 12 + remm + remd / days_in_rpmth


def _is_blank(x) -> bool:
    return x is None or str(x).strip() == ""


def _lo(x):
    """SAS numeric missing sorts as lower than any real number."""
    return float("-inf") if x is None else x


def _sas_sum(*vals):
    """SAS SUM() function: ignores missing; returns missing only if every
    argument is missing."""
    present = [v for v in vals if v is not None]
    return sum(present) if present else None


def _sas_div(a, b):
    if a is None or b is None or b == 0:
        return None
    return a / b


def _sas_mul(a, b):
    if a is None or b is None:
        return None
    return a * b


# ============================================================================
# HELPER: CACHE STAMP + STREAM .sas7bdat -> PARQUET  (EIBDLN1M.py pattern,
# same as used in EIIMRM01.py)
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


# ============================================================================
# STEP 2: CACHE INPUT SAS FILES TO PARQUET
# ============================================================================
print("\nStep 2: Caching input SAS datasets to Parquet...")
BTDTL_CACHE    = _load_cached(INPUT_BTDTL_FILE, "BTDTL")
BTMAST_CACHE   = _load_cached(INPUT_BTMAST_FILE, "BTMAST")
COLLATER_CACHE = _load_cached(INPUT_COLLATER_FILE, "COLLATER")

# ============================================================================
# STEP 3: DATA BTRADIX  (SET BNMDAILY.BTDTL...; subset + PRODCD)
# ============================================================================
print("\nStep 3: Building BTRADIX from BTDTL...")

con = duckdb.connect(database=":memory:")
btdtl_raw = con.execute(f"""
    SELECT
        CAST(t.ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(t.DIRCTIND AS VARCHAR) AS DIRCTIND,
        CAST(t.LIABCODE AS VARCHAR) AS LIABCODE,
        CAST(t.OUTSTAND AS DOUBLE)  AS BALANCE,
        CAST(t.BRANCH   AS VARCHAR) AS BRANCH,
        (DATE '1960-01-01' + CAST(t.EXPRDATE AS INTEGER)) AS EXPRDATE
    FROM read_parquet('{BTDTL_CACHE.as_posix()}') AS t
    WHERE CAST(t.ACCTNO AS BIGINT) BETWEEN 2500000000 AND 2599999999
      AND t.DIRCTIND = 'I'
      AND t.LIABCODE NOT IN ('LCE','SGE','BGE','SBE','FBE')
""").pl()
con.close()

btradix = btdtl_raw.with_columns(
    pl.col("LIABCODE").map_elements(format_btproda, return_dtype=pl.Utf8).alias("PRODCD")
)
print(f"  BTRADIX rows: {len(btradix):,}")

# ============================================================================
# STEP 4: PROC SUMMARY NWAY MISSING -> BTRADI  (CLASS BRANCH ACCTNO PRODCD)
# ============================================================================
print("\nStep 4: Summarising BTRADIX -> BTRADI (SUM BALANCE)...")

btradi = (
    btradix
    .group_by(["BRANCH", "ACCTNO", "PRODCD"])
    .agg(pl.col("BALANCE").sum().alias("BALANCE"))
)
print(f"  BTRADI rows: {len(btradi):,}")

# ============================================================================
# STEP 5: BNM.BTMAST&REPTMON&NOWK -> BTRADM  (WHERE SUBACCT='OV', NODUPKEYS)
# ============================================================================
print("\nStep 5: Building BTRADM from BTMAST (SUBACCT='OV', deduped)...")

con = duckdb.connect(database=":memory:")
btmast_raw = con.execute(f"""
    SELECT
        CAST(ACCTNO   AS BIGINT) AS ACCTNO,
        CAST(ICURBAL  AS DOUBLE) AS ICURBAL,
        CAST(DBALANCE AS DOUBLE) AS DBALANCE
    FROM read_parquet('{BTMAST_CACHE.as_posix()}')
    WHERE SUBACCT = 'OV'
""").pl()
con.close()

btradm = (
    btmast_raw.sort("ACCTNO").unique(subset=["ACCTNO"], keep="first")
)
print(f"  BTRADM rows (deduped): {len(btradm):,}")

# ============================================================================
# STEP 6: DATA BTRADE; MERGE BTRADM(IN=A) BTRADI; BY ACCTNO; IF A;
#         -> LEFT JOIN (BTRADM base, one-to-many against BTRADI naturally
#            replicates SAS's one-to-many BY-group merge behaviour)
# ============================================================================
print("\nStep 6: Building BTRADE (BTRADM LEFT JOIN BTRADI)...")

btrade = btradm.join(btradi, on="ACCTNO", how="left")
print(f"  BTRADE rows: {len(btrade):,}")

# ============================================================================
# STEP 7: DATA COLLATER  (SET BTCOLL.COLLATER; WHERE 2500000000<ACCTNO2<...)
# ============================================================================
print("\nStep 7: Building COLLATER / BTCOLL (deduped by ACCTNO)...")

con = duckdb.connect(database=":memory:")
collater_raw = con.execute(f"""
    SELECT
        CAST(t.ACCTNO  AS BIGINT)  AS ACCTNO,
        CAST(t.CCLASSC AS VARCHAR) AS LIABCODE,
        CAST(t.CDOLARV AS DOUBLE)  AS CDOLARV
    FROM read_parquet('{COLLATER_CACHE.as_posix()}') AS t
    WHERE CAST(t.ACCTNO AS BIGINT) > 2500000000
      AND CAST(t.ACCTNO AS BIGINT) < 2600000000
""").pl()
con.close()

btcoll = collater_raw.sort("ACCTNO").unique(subset=["ACCTNO"], keep="first")
print(f"  BTCOLL rows (deduped): {len(btcoll):,}")

# ============================================================================
# STEP 8: DATA BTRCOL; MERGE BTRADE(IN=A)
#              BTCOLL(KEEP=ACCTNO CCLASSC CDOLARV RENAME=(CCLASSC=LIABCODE));
#         BY ACCTNO; IF A;
# ============================================================================
print("\nStep 8: Building BTRCOL (BTRADE LEFT JOIN BTCOLL)...")

btrcol = btrade.join(btcoll, on="ACCTNO", how="left")
btrcol_sorted = btrcol.sort(["ACCTNO", "PRODCD"])
print(f"  BTRCOL rows: {len(btrcol):,}")

del btdtl_raw, btmast_raw, collater_raw, btrade, btcoll
gc.collect()

# ============================================================================
# STEP 9: DATA LOAN  (collateral-allocation waterfall, per-ACCTNO BY group)
# ============================================================================
print("\nStep 9: Building LOAN (collateral waterfall)...")


def _build_loan(btrcol_df: pl.DataFrame) -> list:
    """SET BTRCOL; BY ACCTNO PRODCD; RETAIN XCOLVAL 0; %COLL; ...
    Replicates the waterfall exactly, including the two-OUTPUT split when
    a single product's balance exceeds the account's remaining collateral
    value, and the COLVAL>0 & XCOLVAL=0 -> TYP=7 override."""
    rows = []
    prev_acctno = object()  # sentinel never equal to a real ACCTNO
    xcolval = 0.0

    for r in btrcol_df.iter_rows(named=True):
        acctno = r["ACCTNO"]
        liabcode = r["LIABCODE"]
        prodcd = r["PRODCD"]
        branch = r["BRANCH"]
        balance = r["BALANCE"]
        icurbal = r["ICURBAL"]
        dbalance = r["DBALANCE"]
        cdolarv = r["CDOLARV"]

        typ = typ_from_liabcode(liabcode)
        bnmcode = prodcd
        totbal = _sas_sum(icurbal, dbalance)
        colval = _sas_mul(_sas_div(icurbal, totbal), cdolarv)

        if acctno != prev_acctno:
            # IF FIRST.ACCTNO THEN XCOLVAL = COLVAL; a missing COLVAL here
            # is behaviourally equivalent to 0 for every "XCOLVAL > 0"
            # test below, so it is normalised to 0.0 rather than kept as
            # a true SAS missing value.
            xcolval = colval if colval is not None else 0.0
        prev_acctno = acctno

        if xcolval > 0 and _lo(balance) >= xcolval:
            vbal = balance
            rows.append({"BRANCH": branch, "BNMCODE": bnmcode, "TYP": typ, "XBALANCE": xcolval})
            rows.append({"BRANCH": branch, "BNMCODE": bnmcode, "TYP": 7, "XBALANCE": vbal - xcolval})
            xcolval = 0.0
        else:
            xbalance = balance if balance is not None else 0.0
            xcolval = xcolval - xbalance
            if xcolval < 0:
                xcolval = 0.0
            if colval is not None and colval > 0 and xcolval == 0:
                rows.append({"BRANCH": branch, "BNMCODE": bnmcode, "TYP": 7, "XBALANCE": xbalance})
            else:
                rows.append({"BRANCH": branch, "BNMCODE": bnmcode, "TYP": typ, "XBALANCE": xbalance})

    return rows


loan_rows_all = _build_loan(btrcol_sorted)

# PROC SORT DATA=LOAN; BY BRANCH; WHERE BNMCODE NE ' '; (then a further
# PROC SORT DATA=LOAN; BY ACCTNO; with no WHERE -- since PROC TABULATE
# does not require presorted data, this final re-sort by ACCTNO has no
# observable effect on the TABULATE output and is therefore not
# reproduced; only the WHERE-filter is functionally relevant.)
loan_rows = [r for r in loan_rows_all if not _is_blank(r["BNMCODE"])]
print(f"  LOAN rows (BNMCODE filtered): {len(loan_rows):,}")

# ============================================================================
# STEP 10: DATA LOAN2  (SET BTRADX, i.e. sorted BTRADIX -- independent of
#          the collateral waterfall above)
# ============================================================================
print("\nStep 10: Building LOAN2 (remaining maturity, from BTRADIX)...")


def _build_loan2(btradix_df: pl.DataFrame) -> list:
    """SET BTRADX; ... BNMCODE = PRODCD; MATDT = EXPRDATE; %REMMTH;
    IF BNMCODE = ' ' THEN DELETE;
    NOTE: the original SAS assigns REMMTH = 1 or 13 based on ORIGMT
    ('IF ORIGMT < '20' THEN REMMTH=1; ELSE REMMTH=13;') immediately
    before %REMMTH unconditionally overwrites REMMTH with the computed
    remaining-months value -- that ORIGMT-based assignment is therefore
    dead code (overwritten every row) and is not reproduced beyond this
    note."""
    rows = []
    for r in btradix_df.iter_rows(named=True):
        prodcd = r["PRODCD"]
        if _is_blank(prodcd):
            continue
        matdt = r["EXPRDATE"]
        remmth = _remmth(matdt) if matdt is not None else None
        rows.append({"BNMCODE": prodcd, "REMMTH": remmth, "BALANCE": r["BALANCE"]})
    return rows


btradx_sorted = btradix.sort("ACCTNO")
loan2_rows = _build_loan2(btradx_sorted)
print(f"  LOAN2 rows: {len(loan2_rows):,}")

del btrcol, btrcol_sorted, loan_rows_all, btradix, btradx_sorted
gc.collect()

# ============================================================================
# STEP 11: REPORT RENDERING
# ============================================================================
print("\nStep 11: Rendering reports...")


def _wrap_words(text, width):
    words = text.split()
    lines, cur = [], ""
    for w in words:
        cand = (cur + " " + w).strip() if cur else w
        if len(cand) <= width:
            cur = cand
        else:
            if cur:
                lines.append(cur); cur = ""
            while len(w) > width:
                lines.append(w[:width - 1] + "-")
                w = w[width - 1:]
            cur = w
    if cur:
        lines.append(cur)
    return lines or [""]


def _fmt_num13(value) -> str:
    """FORMAT=13.2 MISSING (OPTIONS MISSING=0): a genuinely absent cell
    (None) renders as a bare '0', a real computed value renders fully
    decimal-formatted."""
    if value is None:
        return "0".rjust(NUM_WIDTH)
    v = float(value)
    if abs(v) < 0.005:
        v = 0.0
    s = f"{v:.2f}"
    if len(s) > NUM_WIDTH:
        s = s[-NUM_WIDTH:]
    return s.rjust(NUM_WIDTH)


def _title_lines(title3, page_extra=None, page_no=None):
    line1 = "REPORT ID: EIWBTR1C"
    if page_no is not None:
        line1 = line1.ljust(150 - len(str(page_no))) + str(page_no)
    lines = [FF, line1,
             f"PUBLIC BANK BERHAD            DATE : {RDATE}",
             f" {title3}",
             ""]
    if page_extra:
        lines.append(page_extra)   # <-- no extra "" here
    return lines


def _render_crosstab(cell_sums, row_vals, col_defs, box_label):
    # Suppress columns whose cells are all 0/missing on this page
    col_defs = [
        (ck, lbl) for ck, lbl in col_defs
        if any(cell_sums.get((rv, ck)) not in (None, 0.0) for rv in row_vals)
    ]
    n_cols = len(col_defs) + 1  # +1 for TOTAL

    header_texts = [lbl for _, lbl in col_defs] + ["TOTAL"]
    wrapped = [_wrap_words(t, NUM_WIDTH) for t in header_texts]
    n_header_lines = max(len(w) for w in wrapped)
    wrapped = [w + [""] * (n_header_lines - len(w)) for w in wrapped]

    box_wrapped = _wrap_words(box_label, LABEL_WIDTH)
    box_wrapped += [""] * (n_header_lines - len(box_wrapped))
    box_wrapped = [t.ljust(LABEL_WIDTH)[:LABEL_WIDTH] for t in box_wrapped]

    total_width = LABEL_WIDTH + 2 + n_cols * (NUM_WIDTH + 1)
    lines = ["-" * total_width]
    for li in range(n_header_lines):
        parts = [box_wrapped[li]]
        for ci in range(n_cols):
            parts.append(wrapped[ci][li].center(NUM_WIDTH)[:NUM_WIDTH])
        lines.append("|" + "|".join(parts) + "|")

    sep = "|" + "-" * LABEL_WIDTH + "+"
    sep += "+".join("-" * NUM_WIDTH for _ in range(n_cols)) + "|"
    lines.append(sep)

    col_totals = [0.0] * len(col_defs); col_has = [False] * len(col_defs)
    grand = 0.0; grand_has = False
    for rv in row_vals:
        row_cells, row_total = [], None
        for ci, (ck, _lbl) in enumerate(col_defs):
            v = cell_sums.get((rv, ck))
            row_cells.append(v)
            if v is not None:
                row_total = (row_total or 0.0) + v
                col_totals[ci] += v; col_has[ci] = True
                grand += v; grand_has = True
        row_label = str(rv)[:LABEL_WIDTH].ljust(LABEL_WIDTH)
        cells_txt = [_fmt_num13(v) for v in row_cells] + [_fmt_num13(row_total)]
        lines.append("|" + row_label + "|" + "|".join(cells_txt) + "|")
    total_label = "TOTAL".ljust(LABEL_WIDTH)
    total_cells = [_fmt_num13(col_totals[i] if col_has[i] else None) for i in range(len(col_defs))]
    total_cells.append(_fmt_num13(grand if grand_has else None))
    lines.append("|" + total_label + "|" + "|".join(total_cells) + "|")
    lines.append("-" * total_width)
    return lines


def _branch_key(b):
    s = str(b)
    try:
        return (0, int(float(s)))
    except ValueError:
        return (1, s)


def render_report_collateral(loan_rows_, page_no):
    """TABLE (BRANCH ALL='GRAND TOTAL'),(BNMCODE=' ' ALL='TOTAL'),
             (TYP=' ' ALL='TOTAL')*XBALANCE=' '*SUM=' ' / BOX='BNMCODE' RTS=8;
    PAGE dimension = BRANCH (one page per branch, plus a GRAND TOTAL page
    that combines every branch)."""
    col_defs = [(t, TYPFMT_MAP[t]) for t in TYPFMT_ORDER]
    branches = sorted(
        {r["BRANCH"] for r in loan_rows_ if not _is_blank(r["BRANCH"])},
        key=_branch_key,
    )

    out = []
    for branch in branches + [None]:  # None sentinel = the ALL='GRAND TOTAL' page
        if branch is None:
            subset = loan_rows_
            page_extra = "GRAND TOTAL"
        else:
            subset = [r for r in loan_rows_ if r["BRANCH"] == branch]
            page_extra = f"BRANCH {int(float(branch))}"

        cell_sums = {}
        for r in subset:
            key = (r["BNMCODE"], r["TYP"])
            cell_sums[key] = (cell_sums.get(key) or 0.0) + (r["XBALANCE"] or 0.0)
        row_vals = sorted({r["BNMCODE"] for r in subset})

        out.extend(_title_lines(" REPORT ON COLLATERAL", page_extra, page_no))
        out.extend(_render_crosstab(cell_sums, row_vals, col_defs, "BNMCODE"))
        page_no += 1
    return out, page_no


def render_report_remaining_maturity(loan2_rows_, bucket_fn, labels, page_no, title3):
    """TABLE (BNMCODE=' ' ALL='TOTAL'),(REMMTH=' ' ALL='TOTAL')
             *BALANCE=' '*SUM=' ' / BOX='BNMCODE' RTS=8;
    Used for both the REMFMT. and REMFMTS. variants (same TITLE3 text in
    the SAS source for both)."""
    col_defs = [(lbl, lbl) for lbl in labels]
    cell_sums = {}
    for r in loan2_rows_:
        bucket = bucket_fn(r["REMMTH"])
        key = (r["BNMCODE"], bucket)
        cell_sums[key] = (cell_sums.get(key) or 0.0) + (r["BALANCE"] or 0.0)
    row_vals = sorted({r["BNMCODE"] for r in loan2_rows_})

    out = _title_lines(title3, None, page_no)
    out.extend(_render_crosstab(cell_sums, row_vals, col_defs, "BNMCODE"))
    return out, page_no + 1


report_lines = []
page_no = 1
rpt, page_no = render_report_collateral(loan_rows, page_no)
report_lines += rpt
rpt, page_no = render_report_remaining_maturity(loan2_rows, remfmt_format, REMFMT_LABELS, page_no, " REPORT ON REMAINING MATURITY")
report_lines += rpt
rpt, page_no = render_report_remaining_maturity(loan2_rows, remfmts_format, REMFMTS_LABELS, page_no, " REPORT ON REMAINING MATURITY")
report_lines += rpt

# ============================================================================
# STEP 12: WRITE OUTPUT
# ============================================================================
with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in report_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(report_lines):,}")

print("\nEIWBTR1C complete.")
