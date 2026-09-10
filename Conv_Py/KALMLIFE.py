#!/usr/bin/env python3
"""
Program : KALMLIFE.py
Purpose : Treasury Life/Islamic deal BNM code derivation and summarisation.
          Originally %INC PGM(KALMLIFE) inside EIBPTH1A -- a SAS open-code
          fragment (no own JCL) that consumes macro variables (&REPTMON,
          &NOWK, &REPTYEAR) set by the invoking program's REPTDATE step and
          produces K3FEI, which EIBPTH1A later appends into its SP dataset
          ("DATA SP; SET SP K3FEI KAPX;").

          Converted here as an independent, standalone module (per project
          convention: each SAS program -> its own Python file). Since the
          original relies entirely on inherited macro variables rather than
          computing its own REPTDATE, this module independently re-derives
          the identical REPTDATE/REPTMON/REPTYEAR/NOWK context using the same
          formula EIBPTH1A/EIBMSAPC use (REPTDATE.py's monthly variant --
          "last day of previous month" -- plus the day-based NOWK selector).
          Because that formula is a pure function of the run date, this
          reproduces the values EIBPTH1A would have passed down.

Dependency:
    REPTDATE.py -> get_monthly_reptdate_values() (REPTDATE = last day of the
        previous month, matching:
        REPTDATE = INPUT('01'||PUT(MONTH(TODAY()),Z2.)||PUT(YEAR(TODAY()),4.),
                          DDMMYY8.) - 1;)

    BNMK.K3TBL&REPTMON&NOWK  -- physical SAS dataset. The BNMK LIBNAME is not
        defined anywhere in the provided EIBPTH1A JCL/SAS source (only BNM and
        BNM1 are). FLAG-01: path below is a placeholder pending confirmation
        of the actual physical library BNMK maps to.

============================================================================
KNOWN SAS BUG -- PRESERVED VERBATIM
============================================================================
Original: "DAYS=MATDT-REPTDATE;" -- REPTDATE here is written WITHOUT the '&'
macro prefix, so SAS resolves it as a DATA-step variable, not the &REPTDATE
macro variable. REPTDATE is never SET, INFILE'd, or assigned anywhere in this
DATA step, so it is an auto-created variable that is always missing (.).
SAS numeric missing compares as -infinity, so "DAYS <= 365" (missing <= 365)
is unconditionally TRUE. Consequently REMMTH is ALWAYS '05' -- the REMMTH='06'
branch is unreachable dead code. This is preserved exactly: REMMTH is
hardcoded to '05' below, and every "AND REMMTH='05'" condition in the BNMCODE
derivation is therefore always satisfied when reached.
============================================================================
"""

import gc
from pathlib import Path
from datetime import date

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_monthly_reptdate_values

# ============================================================================
# STEP 0: REPORT-DATE / MACRO-VARIABLE CONTEXT
# (Paths below depend on REPTMON/NOWK, so this must run before path setup.)
# ============================================================================


def _derive_context() -> dict:
    monthly = get_monthly_reptdate_values(year_format="%Y")
    reptdate = monthly.reptdate  # last day of previous month

    day_of_month = reptdate.day
    # SELECT(DAY(REPTDATE)) in the invoking program: since REPTDATE is always
    # a month-end date (28-31), the WHEN(8)/WHEN(15)/WHEN(22) branches never
    # fire in practice; OTHERWISE always applies. Preserved verbatim anyway.
    if day_of_month == 8:
        wk = "1"
    elif day_of_month == 15:
        wk = "2"
    elif day_of_month == 22:
        wk = "3"
    else:
        wk = "4"

    return {
        "reptdate": reptdate,
        "reptyear": reptdate.strftime("%Y"),
        "reptmon": reptdate.strftime("%m"),
        "nowk": wk,
    }


_CTX = _derive_context()
REPTDATE = _CTX["reptdate"]
REPTYEAR = _CTX["reptyear"]
REPTMON = _CTX["reptmon"]
NOWK = _CTX["nowk"]

print("KALMLIFE: Deriving report-date context...")
print(f"  REPTDATE : {REPTDATE.isoformat()}")
print(f"  REPTMON  : {REPTMON}   NOWK : {NOWK}   REPTYEAR : {REPTYEAR}")

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

# FLAG-01: BNMK library is not defined in the supplied EIBPTH1A/EIBMSAPC
# source (only BNM = SAP.PBB.D&REPTYEAR and BNM1 = SAP.PBB.SASDATA are).
# Assumed, pending confirmation, to live alongside BNM1's physical area.
INPUT_K3TBL_DIR = BASE_DIR / "input" / "prod" / "BNMK"
INPUT_K3TBL_FILE = INPUT_K3TBL_DIR / f"k3tbl{REPTMON}{NOWK}.sas7bdat"

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBPTH1A"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS = 500_000

# ============================================================================
# HELPERS: sas7bdat -> parquet cache (EIBDLN1M.py / EIIMRM01.py pattern)
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
# STEP 1: CACHE K3TBL INPUT
# ============================================================================
print("\nKALMLIFE Step 1: Caching K3TBL...")
K3TBL_CACHE = _load_cached(INPUT_K3TBL_FILE, "K3TBL")

# ============================================================================
# HELPER: BNMCODE derivation (purchase / sale UTDLP+UTSTY branch table)
# ============================================================================


def _build_bnmcode(utdlp: str, utsty: str, islm: str, remmth: str):
    """Mirrors the two independent (non-ELSE) IF/DO blocks in the SAS
    source. Both blocks are independent IFs, not ELSE-IF, but the UTDLP
    value sets are mutually exclusive (purchase vs sale codes) so at most
    one block's conditions can ever fire for a given row."""
    bnmcode = None

    if utdlp in ("MOP", "MSP", "IUP", "IOP", "ISP"):
        if utsty in ("SSD", "SLD", "SFD", "SZD"):
            bnmcode = f"687038{remmth}00000Y"
        if utsty == "SBA" and remmth == "05":
            bnmcode = "6870480500000Y"
        if utsty == "FCF":
            bnmcode = f"687108{remmth}00000Y"
        if utsty == "MGS":
            bnmcode = f"687218{remmth}00000Y"
        if utsty == "MTB" and remmth == "05":
            bnmcode = "6872280500000Y"
        if utsty == "MGI":
            bnmcode = f"687238{remmth}00000Y"
        if utsty in ("CB1", "CB2", "PNB", "SMC"):
            bnmcode = f"687508{remmth}00000Y"
        if utsty == "SAC" and islm == "N":
            bnmcode = f"687508{remmth}00000Y"
        if utsty in ("SMC", "KHA", "DMB", "DHB"):
            bnmcode = f"687708{remmth}00000Y"
        if utsty == "SAC" and islm == "Y":
            bnmcode = f"687708{remmth}00000Y"
        if utsty in ("BNB", "BNN") and remmth == "05":
            bnmcode = "6879080500000Y"
        if utsty in ("BMN", "BMC", "BMF") and remmth == "05":
            bnmcode = "6879080500000Y"

    if utdlp in ("MOS", "MSS", "IUS", "IOS", "ISS"):
        if utsty in ("SSD", "SLD", "SFD", "SZD"):
            bnmcode = f"787038{remmth}00000Y"
        if utsty == "SBA" and remmth == "05":
            bnmcode = "7870480500000Y"
        if utsty == "FCF":
            bnmcode = f"787108{remmth}00000Y"
        if utsty == "MGS":
            bnmcode = f"787218{remmth}00000Y"
        if utsty == "MTB" and remmth == "05":
            bnmcode = "7872280500000Y"
        if utsty == "MGI":
            bnmcode = f"787238{remmth}00000Y"
        if utsty in ("CB1", "CB2", "PNB", "SMC"):
            bnmcode = f"787508{remmth}00000Y"
        if utsty == "SAC" and islm == "N":
            bnmcode = f"787508{remmth}00000Y"
        if utsty in ("SMC", "KHA", "DMB", "DHB"):
            bnmcode = f"787708{remmth}00000Y"
        if utsty == "SAC" and islm == "Y":
            bnmcode = f"787708{remmth}00000Y"
        if utsty in ("BNB", "BNN") and remmth == "05":
            bnmcode = "7879080500000Y"
        if utsty in ("BMN", "BMC", "BMF") and remmth == "05":
            bnmcode = "7879080500000Y"

    return bnmcode


def _parse_iso_date(raw) -> date:
    """INPUT(var,YYMMDD10.) equivalent -- source column is an ISO date
    string 'YYYY-MM-DD'."""
    s = str(raw).strip()
    y, m, d = s.split("-")[:3]
    return date(int(y), int(m), int(d[:2]))


# ============================================================================
# STEP 2: DATA K3TBLFE; SET BNMK.K3TBL&REPTMON&NOWK; ...
# ============================================================================
print("\nKALMLIFE Step 2: Building K3TBLFE...")

con = duckdb.connect(database=":memory:")
k3tbl_raw = con.execute(f"""
    SELECT
        CAST(UTCTP AS VARCHAR)  AS UTCTP,
        CAST(UTREF AS VARCHAR)  AS UTREF,
        CAST(UTMDT AS VARCHAR)  AS UTMDT,
        CAST(UTOSD AS VARCHAR)  AS UTOSD,
        CAST(UTAMTS AS DOUBLE) AS UTAMTS,
        CAST(UTDLP AS VARCHAR)  AS UTDLP,
        CAST(UTSTY AS VARCHAR)  AS UTSTY
    FROM read_parquet('{K3TBL_CACHE.as_posix()}')
    WHERE UTCTP IN ('BW','BA','BE','EB','CE','GA')
""").pl()
con.close()

REPTMON_INT = int(REPTMON)
REPTYEAR_INT = int(REPTYEAR)

k3tblfe_rows = []
for r in k3tbl_raw.iter_rows(named=True):
    utref = r["UTREF"] or ""
    isl1 = utref[0:1]
    isl2 = utref[3:4]
    islm = "Y" if (isl1 == "I" and isl2 != " ") else "N"

    utosd = r["UTOSD"]
    if utosd is None or str(utosd).strip() == "":
        # ISSDT never assigned -> TRMM/TRYY stay missing -> subsetting IF
        # below always drops this row (missing never equals REPTMON/REPTYEAR).
        continue
    issdt = _parse_iso_date(utosd)
    trmm, tryy = issdt.month, issdt.year
    if not (trmm == REPTMON_INT and tryy == REPTYEAR_INT):
        continue

    amount = r["UTAMTS"]
    utdlp = r["UTDLP"] or ""
    utsty = r["UTSTY"] or ""

    # DAYS=MATDT-REPTDATE bug -> REMMTH is unconditionally '05' (see module
    # docstring for the full explanation of this preserved SAS defect).
    remmth = "05"

    bnmcode = _build_bnmcode(utdlp, utsty, islm, remmth)
    if bnmcode is None:  # IF BNMCODE NE ' ';
        continue

    k3tblfe_rows.append({"ITCODE": bnmcode, "AMOUNT": amount, "AMTIND": "D"})

print(f"  K3TBLFE rows: {len(k3tblfe_rows):,}")

# ============================================================================
# STEP 3: PROC SUMMARY DATA=K3TBLFE NWAY; CLASS ITCODE AMTIND; VAR AMOUNT;
#         OUTPUT OUT=K3FEI SUM=;
# ============================================================================
print("\nKALMLIFE Step 3: Summarising into K3FEI...")

_groups: dict = {}
for r in k3tblfe_rows:
    key = (r["ITCODE"], r["AMTIND"])
    _groups[key] = (_groups.get(key, 0.0) or 0.0) + (r["AMOUNT"] or 0.0)

K3FEI = pl.DataFrame(
    [{"ITCODE": k[0], "AMTIND": k[1], "AMOUNT": v} for k, v in _groups.items()]
)

print(f"  K3FEI rows (module-level output): {K3FEI.height:,}")
print("KALMLIFE complete.")
