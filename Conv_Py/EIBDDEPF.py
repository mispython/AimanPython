#!/usr/bin/env python3
"""
Program : EIBDDEPF.py
Purpose : Bank's Total Foreign Currency Deposits (RM) - Daily Data Preparation

          Builds the daily FCY Current Account / Fixed Deposit summary row
          and appends it to the monthly cumulative dataset (Python/parquet
          equivalent of the permanent SAS dataset MIS.DYFCY&REPTMON), then
          triggers the DMMISR1F report program.

Original JCL flow (EIBDDEPF):
    DELETE   - remove prior DMMISR1F.DAILY report dataset
    SAS609   step:
        - Derive REPTDATE (MNITB.REPTDATE) / TDATE (TODAY()-1); ABORT 77
          if they differ.
        - %INC PGM(PBBDPFMT)          -> product/customer-code formats
        - Build CAFY  (FCY current-account summary: domestic + Islamic)
        - Build FDFY  (FCY fixed-deposit summary: domestic + WALK/FI)
        - MERGE CAFY FDFY BY REPTDATE  -> DYPOSN
        - Append DYPOSN to MIS.DYFCY&REPTMON (recreate if REPTDAY = '01')
        - PROC SORT NODUPKEY BY REPTDATE
        - %INC PGM(DMMISR1F)           -> print the report

NOTE ON INPUT RESTRUCTURING (per migration instructions):
    The legacy MNITB.CURRENT / MNITB.FD (and Islamic IMNITB counterparts)
    have been consolidated into single source files per side:
        dp_fcy<date>.sas7bdat   (MNITB : domestic FCY CA + FD, combined)
        idp_fcy<date>.sas7bdat  (IMNITB: Islamic  FCY CA + FD, combined)
    Records are split into CA vs FD using the ACCTNO range convention
    (3000000000-3999999999 = CA, else = FD), matching the reference
    DATA FCYCA FCYFD step supplied for this conversion. CURBAL is derived
    from CURBALRM as in that reference.

    FD ISLAMIC NOTE: The original FDFY dataset only ever read MNITB.FD
    (domestic). The DMMISR1F report has no "FD-I" (Islamic FD) columns
    anywhere in its COLUMN/DEFINE list (only FD-C = conventional). To stay
    faithful to the report's structure, the Islamic (idp_fcy) FD split is
    read/derived but intentionally NOT aggregated into the FD totals -
    only its CA portion is used. This mirrors the original program's
    scope rather than expanding it.
"""

import gc
import runpy
import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from REPTDATE import get_reptdate_values
from input_date import get_latest_file
from PBBDPFMT import FCY, ddcustcd_format, fdcustcd_format

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

INPUT_MNITB_DIR  = BASE_DIR / "input" / "prod" / "EIBDDEPF"   # dp_fcy<date>.sas7bdat
INPUT_IMNITB_DIR = BASE_DIR / "input" / "prod" / "EIBDDEPF"   # idp_fcy<date>.sas7bdat
INPUT_WALK_DIR   = BASE_DIR / "input" / "prod" / "EIBDDEPF"   # wk<date>.sas7bdat

CACHE_DIR = BASE_DIR / "input" / "prod" / "EIBDDEPF"

# Monthly cumulative dataset directory - equivalent of LIBNAME MIS
# "SAP.PBB.MIS.D&REPTYEAR" holding dataset DYFCY&REPTMON.
MONTHLY_DIR = BASE_DIR / "output" / "EIBDDEPF"

CACHE_DIR.mkdir(parents=True, exist_ok=True)
MONTHLY_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# CUSTOMER-CODE / PRODUCT CLASSIFICATION CONSTANTS
# ============================================================================
# IF CUSTCD IN (02,03,07,10,12,81,82,83,84) OR PRODUCT = 413  -> FI bucket
FI_CUSTCD_CODES = {2, 3, 7, 10, 12, 81, 82, 83, 84}

# IF CUSTCODE IN (77,78,95,96) -> individual bucket
INDIV_CUSTCODE_CODES = {77, 78, 95, 96}

# CA range for FCY products: PRODUCT IN (400:411,413,420:434,440:444,450:454)
def _in_ca_fcy_product_range(product: int) -> bool:
    return (
        400 <= product <= 411
        or product == 413
        or 420 <= product <= 434
        or 440 <= product <= 444
        or 450 <= product <= 454
    )

# ACCTNO ranges used to split the combined FCY input into CA vs FD
CA_ACCTNO_LOW  = 3_000_000_000
CA_ACCTNO_HIGH = 3_999_999_999

# ============================================================================
# STEP 1: REPORT DATE  (no reptdate.parquet — derive from REPTDATE.py)
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values(year_format="%Y")
reptdate = reptdate_values.reptdate           # yesterday (RDATE equivalent)

REPTYEAR = reptdate.strftime("%Y")            # 4-digit year (library year folder)
REPTYY   = reptdate.strftime("%y")            # 2-digit year (SYMPUT REPTYY - unused downstream)
REPTMON  = reptdate.strftime("%m")
REPTDAY  = reptdate.strftime("%d")
XDATE    = reptdate.strftime("%d/%m/%Y")      # DDMMYY8-style title date

# NOTE: In the original JCL, TDATE = TODAY()-1 is derived independently of
# REPTDATE (read from MNITB.REPTDATE) and the job aborts (ABORT 77) if the
# two differ. Both values now derive from the single REPTDATE.py source, so
# they are always equal; the historical guard is kept only as a comment:
# %IF "&TDATE" NE "&RDATE" %THEN %DO; DATA A; ABORT 77; %END;

print(f"  Report date  : {XDATE}")
print(f"  REPTMON/DAY  : {REPTMON}/{REPTDAY}   REPTYEAR: {REPTYEAR}")

MONTHLY_YEAR_DIR = MONTHLY_DIR / REPTYEAR
MONTHLY_YEAR_DIR.mkdir(parents=True, exist_ok=True)
MONTHLY_FILE = MONTHLY_YEAR_DIR / f"DYFCY{REPTMON}.parquet"

# ============================================================================
# STEP 2: RESOLVE INPUT FILES
# ============================================================================
print("\nStep 2: Resolving input files...")

mnitb_path  = get_latest_file(INPUT_MNITB_DIR, prefix="dp_fcy")
imnitb_path = get_latest_file(INPUT_IMNITB_DIR, prefix="idp_fcy")
walk_path   = get_latest_file(INPUT_WALK_DIR, prefix="wk")

print(f"  MNITB (dp_fcy) : {mnitb_path.name}")
print(f"  IMNITB(idp_fcy): {imnitb_path.name}")
print(f"  WALK  (wk)     : {walk_path.name}")

# ============================================================================
# HELPERS: CACHE FRESHNESS + STREAMED SAS -> PARQUET CONVERSION
# (pattern follows EIBDLN1M.py)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


def sas_to_parquet(sas_path: Path, cache_path: Path, tag: str,
                    chunk_rows: int = 500_000) -> None:
    """Convert a .sas7bdat file to Parquet in streaming chunks."""
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0

    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=chunk_rows)
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
                              f"{col.type}->{field.type}: {e} — filling nulls")
                        col = pa.nulls(len(col), type=field.type)
                cast_arrays.append(col)
            table = pa.Table.from_arrays(cast_arrays, schema=schema)
        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer:
        writer.close()
    print(f"  [{tag}] Done — {total:,} rows cached.")


print("\nStep 3: Caching source files to Parquet (if needed)...")

MNITB_CACHE  = CACHE_DIR / f"{mnitb_path.stem}.parquet"
IMNITB_CACHE = CACHE_DIR / f"{imnitb_path.stem}.parquet"
WALK_CACHE   = CACHE_DIR / f"{walk_path.stem}.parquet"

for src, cache, tag in (
    (mnitb_path,  MNITB_CACHE,  "MNITB"),
    (imnitb_path, IMNITB_CACHE, "IMNITB"),
    (walk_path,   WALK_CACHE,   "WALK"),
):
    if not _cache_is_fresh(src, cache):
        sas_to_parquet(src, cache, tag)
    else:
        print(f"  [{tag}] Cache fresh — skipping conversion.")

# ============================================================================
# STEP 4: BUILD CAFY  (FCY current-account summary, domestic + Islamic)
# DATA CAFY;
#   SET MNITB.CURRENT IMNITB.CURRENT(RENAME=(CURBAL=ICURBAL));
#   CUSTCD = PUT(CUSTCODE,DDCUSTCD.);
#   IF CURCODE NE 'MYR' AND PRODUCT IN (400:411,413,420:434,440:444,450:454);
#   IF CUSTCD IN (...) OR PRODUCT=413 THEN FCAFIC/FCAFII=CURBAL/ICURBAL;
#   ELSE CURBAL1/ICURBAL1 = CURBAL/ICURBAL;
#   IF PRODUCT NE 413 AND CUSTCODE IN (77,78,95,96) THEN FCAIDC/FCAIDI=...;
# ============================================================================
print("\nStep 4: Building CAFY (FCY current-account summary)...")


def _load_fcy_ca_rows(cache_path: Path, source_tag: str) -> pl.DataFrame:
    """Read the combined FCY parquet, derive CURBAL, and keep only CA-range
    rows matching the CA FCY product filter (CURCODE <> 'MYR', product range).
    """
    con = duckdb.connect(database=":memory:")
    df = con.execute(f"""
        SELECT
            CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
            CAST(CURBALRM AS DOUBLE)  AS CURBAL,
            CAST(CUSTCODE AS INTEGER) AS CUSTCODE,
            CAST(PRODUCT  AS INTEGER) AS PRODUCT,
            CAST(CURCODE  AS VARCHAR) AS CURCODE
        FROM read_parquet('{cache_path}')
        WHERE ACCTNO BETWEEN {CA_ACCTNO_LOW} AND {CA_ACCTNO_HIGH}
          AND CURCODE <> 'MYR'
    """).pl()
    con.close()

    if df.is_empty():
        return df

    # Apply the CA FCY product-range filter (row-wise; small residual set).
    df = df.filter(
        pl.col("PRODUCT").map_elements(_in_ca_fcy_product_range, return_dtype=pl.Boolean)
    )
    df = df.with_columns(pl.lit(source_tag).alias("SOURCE"))
    return df


ca_domestic = _load_fcy_ca_rows(MNITB_CACHE, "D")
ca_islamic  = _load_fcy_ca_rows(IMNITB_CACHE, "I")

# CUSTCD = PUT(CUSTCODE, DDCUSTCD.)  — applied per side using PBBDPFMT.
def _add_custcd(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df.with_columns(pl.lit(None).cast(pl.Int64).alias("CUSTCD"))
    custcd_str = [ddcustcd_format(v) for v in df["CUSTCODE"].to_list()]
    custcd_num = [int(v) for v in custcd_str]   # numeric compare, matches SAS auto-conversion
    return df.with_columns(pl.Series("CUSTCD", custcd_num))


ca_domestic = _add_custcd(ca_domestic)
ca_islamic  = _add_custcd(ca_islamic)


def _ca_sums(df: pl.DataFrame) -> dict:
    """Return (non_fi_total, fi_total, indiv_total) sums of CURBAL for one side."""
    if df.is_empty():
        return {"non_fi": 0.0, "fi": 0.0, "indiv": 0.0}

    is_fi = (
        pl.col("CUSTCD").is_in(list(FI_CUSTCD_CODES)) | (pl.col("PRODUCT") == 413)
    )
    is_indiv = (pl.col("PRODUCT") != 413) & (pl.col("CUSTCODE").is_in(list(INDIV_CUSTCODE_CODES)))

    non_fi = df.filter(~is_fi)["CURBAL"].sum() or 0.0
    fi     = df.filter(is_fi)["CURBAL"].sum() or 0.0
    indiv  = df.filter(is_indiv)["CURBAL"].sum() or 0.0
    return {"non_fi": non_fi, "fi": fi, "indiv": indiv}


ca_dom_sums = _ca_sums(ca_domestic)
ca_isl_sums = _ca_sums(ca_islamic)

TOTCAFY  = ca_dom_sums["non_fi"]   # FCY CA-C balance (excl. FI)
TOTCAFYI = ca_isl_sums["non_fi"]   # FCY CA-I balance (excl. FI)
TOFCAFIC = ca_dom_sums["fi"]       # FCY CA-C FI
TOFCAFII = ca_isl_sums["fi"]       # FCY CA-I FI
TOFCAIDC = ca_dom_sums["indiv"]    # FCY CA-C individual
TOFCAIDI = ca_isl_sums["indiv"]    # FCY CA-I individual

del ca_domestic, ca_islamic
gc.collect()

print(f"  TOTCAFY={TOTCAFY:,.2f}  TOTCAFYI={TOTCAFYI:,.2f}  "
      f"TOFCAFIC={TOFCAFIC:,.2f}  TOFCAFII={TOFCAFII:,.2f}")

# ============================================================================
# STEP 5: BUILD FDFY  (FCY fixed-deposit summary, domestic MNITB.FD + WALK)
# DATA FDFY; SET MNITB.FD; IF PRODUCT IN &FCY; CUSTCD=PUT(CUSTCODE,FDCUSTCD.);
# DATA FCFY; SET WALK...(RENAME=(CURBAL=FCURBAL)); IF PROD EQ 'DCM22110';
#            FCURBAL = -1*FCURBAL;
# DATA FDFY; SET FDFY FCFY;
#   IF CUSTCD IN (...) THEN FFDFIC=CURBAL; ELSE CURBAL1=CURBAL;
#   IF CUSTCODE IN (77,78,95,96) THEN FFDIDC=CURBAL;
# ============================================================================
print("\nStep 5: Building FDFY (FCY fixed-deposit summary)...")

con = duckdb.connect(database=":memory:")
fd_domestic = con.execute(f"""
    SELECT
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(CURBALRM AS DOUBLE)  AS CURBAL,
        CAST(CUSTCODE AS INTEGER) AS CUSTCODE,
        CAST(PRODUCT  AS INTEGER) AS PRODUCT
    FROM read_parquet('{MNITB_CACHE}')
    WHERE NOT (ACCTNO BETWEEN {CA_ACCTNO_LOW} AND {CA_ACCTNO_HIGH})
      AND PRODUCT IN ({",".join(str(p) for p in sorted(FCY))})
""").pl()
con.close()

if fd_domestic.is_empty():
    TOTFDFY = 0.0
    TOFFDFIC = 0.0
    TOFFDIDC = 0.0
else:
    custcd_str = [fdcustcd_format(v) for v in fd_domestic["CUSTCODE"].to_list()]
    custcd_num = [int(v) for v in custcd_str]
    fd_domestic = fd_domestic.with_columns(pl.Series("CUSTCD", custcd_num))

    is_fi = fd_domestic["CUSTCD"].is_in(list(FI_CUSTCD_CODES))
    is_indiv = fd_domestic["CUSTCODE"].is_in(list(INDIV_CUSTCODE_CODES))

    TOTFDFY  = fd_domestic.filter(~is_fi)["CURBAL"].sum() or 0.0
    TOFFDFIC = fd_domestic.filter(is_fi)["CURBAL"].sum() or 0.0
    TOFFDIDC = fd_domestic.filter(is_indiv)["CURBAL"].sum() or 0.0

del fd_domestic
gc.collect()

# FCFY — foreign-companies GL walk file (GL:22110), negated.
con = duckdb.connect(database=":memory:")
walk_df = con.execute(f"""
    SELECT
        CAST(PROD   AS VARCHAR) AS PROD,
        CAST(CURBAL AS DOUBLE)  AS CURBAL
    FROM read_parquet('{WALK_CACHE}')
    WHERE PROD = 'DCM22110'
""").pl()
con.close()

TOTFCFY = -1.0 * (walk_df["CURBAL"].sum() or 0.0) if not walk_df.is_empty() else 0.0

del walk_df
gc.collect()

print(f"  TOTFDFY={TOTFDFY:,.2f}  TOTFCFY={TOTFCFY:,.2f}  "
      f"TOFFDFIC={TOFFDFIC:,.2f}  TOFFDIDC={TOFFDIDC:,.2f}")

# ============================================================================
# STEP 6: MERGE CAFY + FDFY BY REPTDATE  ->  DYPOSN
# (Both sides are single-row NWAY summaries for the same REPTDATE, so the
#  "merge" collapses to combining the two dicts of computed sums.)
# ============================================================================
print("\nStep 6: Merging CAFY + FDFY -> DYPOSN and deriving report totals...")


def _r(v: float) -> float:
    """SAS ROUND(x,1) — round to the nearest whole number."""
    return round(v or 0.0)


TOTFDFY  = _r(TOTFDFY)
TOTFCFY  = _r(TOTFCFY)
TOTFYFD  = TOTFDFY + TOTFCFY
TOFFDFIC = _r(TOFFDFIC)
TOTCAFY  = _r(TOTCAFY)
TOTCAFYI = _r(TOTCAFYI)
TOTFYCA  = TOTCAFY + TOTCAFYI
TOTFCY   = TOTFYFD + TOTFYCA
TOFFDIDC = _r(TOFFDIDC)
TOFFDNDC = TOTFYFD + (-1 * TOFFDIDC)
TOFCAFIC = _r(TOFCAFIC)
TOFCAFII = _r(TOFCAFII)
TOTFCAFI = TOFCAFIC + TOFCAFII
TOTFCYFI = TOFFDFIC + TOTFCAFI
TOFCAIDC = _r(TOFCAIDC)
TOFCANDC = TOTCAFY + (-1 * TOFCAIDC)
TOFCAIDI = _r(TOFCAIDI)
TOFCANDI = TOTCAFYI + (-1 * TOFCAIDI)
TOTFCAID = TOFCAIDC + TOFCAIDI
TOTFCAND = TOFCANDC + TOFCANDI

dyposn_row = {
    "REPTDATE": reptdate,
    "TOTFDFY": TOTFDFY, "TOTFCFY": TOTFCFY, "TOTFYFD": TOTFYFD,
    "TOFFDFIC": TOFFDFIC, "TOTCAFY": TOTCAFY, "TOTCAFYI": TOTCAFYI,
    "TOTFYCA": TOTFYCA, "TOTFCY": TOTFCY, "TOFFDIDC": TOFFDIDC,
    "TOFFDNDC": TOFFDNDC, "TOFCAFIC": TOFCAFIC, "TOFCAFII": TOFCAFII,
    "TOTFCAFI": TOTFCAFI, "TOTFCYFI": TOTFCYFI, "TOFCAIDC": TOFCAIDC,
    "TOFCANDC": TOFCANDC, "TOFCAIDI": TOFCAIDI, "TOFCANDI": TOFCANDI,
    "TOTFCAID": TOTFCAID, "TOTFCAND": TOTFCAND,
}

print("  DYPOSN row:")
for k, v in dyposn_row.items():
    print(f"    {k:10s} = {v}")

# ============================================================================
# STEP 7: APPEND TO MONTHLY CUMULATIVE DATASET (MIS.DYFCY&REPTMON equivalent)
# %IF "&REPTDAY" EQ "01" %THEN DATA MIS.DYFCY&REPTMON; SET DYPOSN;
# %ELSE DATA MIS.DYFCY&REPTMON; SET DYPOSN MIS.DYFCY&REPTMON;
# PROC SORT DATA=MIS.DYFCY&REPTMON NODUPKEY; BY REPTDATE;
#
# NOTE: PROC SORT NODUPKEY keeps the first record per BY-group in sort order.
# Operationally, a same-day rerun should refresh (not discard) that day's
# figures, so on a duplicate REPTDATE this implementation keeps the LATEST
# (freshly computed) row rather than the stale existing one.
# ============================================================================
print("\nStep 7: Updating monthly cumulative dataset...")

new_row_df = pl.DataFrame([dyposn_row])

if REPTDAY == "01" or not MONTHLY_FILE.exists():
    monthly_df = new_row_df
else:
    existing_df = pl.read_parquet(MONTHLY_FILE)
    combined = pl.concat([existing_df, new_row_df], how="vertical_relaxed")
    monthly_df = (
        combined.sort("REPTDATE")
        .unique(subset=["REPTDATE"], keep="last")
        .sort("REPTDATE")
    )

monthly_df.write_parquet(MONTHLY_FILE)
print(f"  Monthly cumulative file written: {MONTHLY_FILE}")
print(f"  Rows in month-to-date dataset  : {len(monthly_df):,}")

del monthly_df, new_row_df
gc.collect()

# ============================================================================
# STEP 8: TRIGGER DMMISR1F REPORT PROGRAM  (%INC PGM(DMMISR1F))
# ============================================================================
print("\nStep 8: Invoking DMMISR1F report program...")

DMMISR1F_SCRIPT = Path(__file__).resolve().parent / "DMMISR1F.py"

try:
    runpy.run_path(str(DMMISR1F_SCRIPT), run_name="__main__")
except Exception as exc:
    # Mirrors JCL COND-check behaviour: halt on first failure.
    raise RuntimeError(f"DMMISR1F report step failed: {exc}") from exc

print("\nEIBDDEPF complete.")
