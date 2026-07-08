#!/usr/bin/env python3
"""
Program : EIMBRAMC.py
Purpose : Branch - Account Management Concept (AMC)
          COLD report for Branch (detail) + Report for Commercial Banking (summary)
          Original SAS: cold-for-branch / summary-for-HQ dual report, comparing
          current month vs previous month approved-limit / outstanding-balance
          utilisation per customer (ICNO) with combined-limit > RM1,000,000.
"""

import os
import gc
import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import date

from REPTDATE import get_reptdate_values
from input_date import get_latest_file
from output_date import build_output_file

# ============================================================================
# DEPENDENCY NOTES
# ============================================================================
# %INC PGM(PBBLNFMT); is included in the SAS SYSIN, but no PUT(var,fmt.) call
# is made anywhere in this program's body, so no format_* function from
# PBBLNFMT is imported here (per project convention: only import a dependency
# function when it is traceable to an explicit PUT(var,fmt.) call).
#
# The SAS source filters "IF PRODUCT NOT IN &HP;" where &HP is a macro list
# NOT defined anywhere inside this program's SYSIN (it must be set upstream,
# e.g. in an included macro library not provided to this conversion). The
# closest documented equivalent is PBBLNFMT.HP_ALL ("HP - ALL PRODUCTS").
# It is reproduced locally below as the best available reference; confirm
# against the true &HP macro definition before relying on this in production.
HP_PRODUCTS = (128, 130, 131, 132, 380, 381, 700, 705, 720, 725,
               983, 993, 996, 678, 679, 698, 699)

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

INPUT_LOAN_DIR    = BASE_DIR / "input" / "prod" / "loan"        # lnXXXXX.sas7bdat
INPUT_BT_DIR      = BASE_DIR / "input" / "prod" / "btrade"      # btmastXXXXX.sas7bdat
INPUT_CISLN_DIR   = BASE_DIR / "input" / "prod" / "cis" / "CISLN_loan.sas7bdat"
INPUT_CISDP_DIR   = BASE_DIR / "input" / "prod" / "cis" / "CISDP_deposit.sas7bdat"
INPUT_BRANCH_FILE = Path("/sasdata/rawdata/lookup") / "LKP_BRANCH"

CACHE_DIR     = BASE_DIR / "input" / "prod" / "EIMBRAMC" / "cache"
AMC_STORE_DIR = BASE_DIR / "input" / "prod" / "EIMBRAMC" / "amc_store"     # equivalent of SAP.PBB.AMC library

OUTPUT_DIR = BASE_DIR / "output" / "EIMBRAMC"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)
AMC_STORE_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_ROWS = 500_000
ROW_LIMIT  = int(os.environ.get("ROW_LIMIT", 0))   # 0 = no limit

PAGE_SIZE  = 60
LRECL      = 132   # data portion width (ASA control char is a separate byte)

# ============================================================================
# STEP 1: REPORT DATE  (no reptdate.parquet -- derive from REPTDATE.py, then
# replicate the SAS DATA REPTDATE step's custom WK/WK1/MM1/MM2 logic locally,
# since this program's week-bucket rule is a discrete day-of-month match
# [8/15/22/otherwise], not the generic range-based NOWK in REPTDATE.py)
# ============================================================================
print("Step 1: Deriving report date...")

# _reptdate_values = get_reptdate_values()
_reptdate_values = get_reptdate_values(run_date=date(2026, 7, 1))
reptdate: date = _reptdate_values.reptdate     # equivalent of SET LN.REPTDATE

_day = reptdate.day
if _day == 8:
    WK, WK1 = "1", "4"
elif _day == 15:
    WK, WK1 = "2", "1"
elif _day == 22:
    WK, WK1 = "3", "2"
else:
    WK, WK1 = "4", "3"

MM = reptdate.month
YY1 = reptdate.year
YY2 = reptdate.year
MM1 = MM - 1
if MM1 == 0:
    MM1 = 12
    YY1 = reptdate.year - 1
MM2 = MM1 - 1
if MM2 == 0:
    MM2 = 12
    YY2 = reptdate.year - 1

NOWK      = WK
NOWK1     = WK1                      # derived but unused later in SAS source (kept for parity)
REPTMON   = f"{MM:02d}"
REPTMON1  = f"{MM1:02d}"
REPTMON2  = f"{MM2:02d}"             # derived but unused later in SAS source (kept for parity)
REPTYR    = f"{reptdate.year:04d}"
REPTYR1   = f"{YY1:04d}"
REPTYR2   = f"{YY2:04d}"             # derived but unused later in SAS source (kept for parity)
RDATE     = reptdate.strftime("%d/%m/%y")

print(f"  REPTDATE : {reptdate}  (NOWK={NOWK})")
print(f"  REPTMON  : {REPTMON}/{REPTYR}   REPTMON1: {REPTMON1}/{REPTYR1}")
print(f"  RDATE    : {RDATE}")

# ============================================================================
# STEP 2: RESOLVE INPUT FILES
# ============================================================================
print("\nStep 2: Resolving input files...")

loan_path = get_latest_file(INPUT_LOAN_DIR, prefix="ln")
bt_path   = get_latest_file(INPUT_BT_DIR, prefix="btmast")

print(f"  LOAN : {loan_path.name}")
print(f"  BT   : {bt_path.name}")


def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


def sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    """Convert a .sas7bdat to Parquet in streaming chunks (memory-efficient)."""
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0
    rows_read = 0

    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
    for chunk in reader:
        if ROW_LIMIT and rows_read >= ROW_LIMIT:
            break
        if ROW_LIMIT:
            chunk = chunk.iloc[: ROW_LIMIT - rows_read]
        rows_read += len(chunk)

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
                              f"from {col.type} to {field.type}: {e} -- filling nulls")
                        col = pa.nulls(len(col), type=field.type)
                cast_arrays.append(col)
            table = pa.Table.from_arrays(cast_arrays, schema=schema)

        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer:
        writer.close()
    print(f"  [{tag}] Done -- {total:,} rows cached.")


# ============================================================================
# STEP 3: CACHE SOURCE FILES TO PARQUET
# ============================================================================
print("\nStep 3: Caching SAS files to Parquet (if needed)...")

LOAN_CACHE  = CACHE_DIR / f"{loan_path.stem}.parquet"
BT_CACHE    = CACHE_DIR / f"{bt_path.stem}.parquet"
CISLN_CACHE = CACHE_DIR / "cisln.parquet"
CISDP_CACHE = CACHE_DIR / "cisdp.parquet"

if not _cache_is_fresh(loan_path, LOAN_CACHE):
    sas_to_parquet(loan_path, LOAN_CACHE, "LOAN")
else:
    print("  [LOAN ] Cache fresh -- skipping conversion.")

if not _cache_is_fresh(bt_path, BT_CACHE):
    sas_to_parquet(bt_path, BT_CACHE, "BT")
else:
    print("  [BT   ] Cache fresh -- skipping conversion.")

if not _cache_is_fresh(INPUT_CISLN_DIR, CISLN_CACHE):
    sas_to_parquet(INPUT_CISLN_DIR, CISLN_CACHE, "CISLN")
else:
    print("  [CISLN] Cache fresh -- skipping conversion.")

if not _cache_is_fresh(INPUT_CISDP_DIR, CISDP_CACHE):
    sas_to_parquet(INPUT_CISDP_DIR, CISDP_CACHE, "CISDP")
else:
    print("  [CISDP] Cache fresh -- skipping conversion.")

# ============================================================================
# STEP 4: BUILD CIS  (customer name / phone lookup from CISLN + CISDP)
# DATA LNCIS(KEEP=ACCTNO ICNO CUSTNAME PRIPHONE SECPHONE);
#   SET CISLN.LOAN;  IF CACCCODE NOT IN ('017','021','028') AND SECCUST='901';
#   IF NEWIC NE ' ' THEN ICNO=NEWIC; ELSE ICNO=OLDIC;
# (DPCIS is the same logic against CISDP.DEPOSIT; CIS = LNCIS stacked DPCIS)
# ============================================================================
print("\nStep 4: Building CIS (customer name/phone lookup)...")

con = duckdb.connect(database=":memory:")

cis = con.execute(f"""
    SELECT
        CAST(ACCTNO AS BIGINT) AS ACCTNO,
        CASE WHEN TRIM(COALESCE(NEWIC, '')) <> '' THEN NEWIC ELSE OLDIC END AS ICNO,
        CUSTNAME,
        PRIPHONE,
        SECPHONE
    FROM read_parquet('{CISLN_CACHE}')
    WHERE CACCCODE NOT IN ('017','021','028') AND SECCUST = '901'

    UNION ALL

    SELECT
        CAST(ACCTNO AS BIGINT) AS ACCTNO,
        CASE WHEN TRIM(COALESCE(NEWIC, '')) <> '' THEN NEWIC ELSE OLDIC END AS ICNO,
        CUSTNAME,
        PRIPHONE,
        SECPHONE
    FROM read_parquet('{CISDP_CACHE}')
    WHERE CACCCODE NOT IN ('017','021','028') AND SECCUST = '901'
""").pl()

con.close()
gc.collect()
print(f"  CIS rows: {len(cis):,}")

# ============================================================================
# STEP 5: EXTRACT LN A/C (EXCEPT HP)
# DATA LNACC(KEEP=ACCTNO NOTENO BRANCH CURBAL BALANCE APPRLIMT APPRLIM2
#                 PRODTYPE UNDRAWN);
#   SET LN_LN.LN06426;  IF PRODUCT NOT IN &HP;
#   IF (3000000000<=ACCTNO<=3999999999) THEN PRODTYPE='OD'; ELSE PRODTYPE='FL';
#
# FLAG: ACCTYPE column does not exist in the LOAN parquet dataset.
# PRODTYPE is instead derived from the ACCTNO numeric range, per source.
# ============================================================================
print("\nStep 5: Extracting LN accounts (excluding HP)...")

# print(pl.read_parquet(LOAN_CACHE).columns)

con = duckdb.connect(database=":memory:")
_hp_list = ",".join(str(p) for p in HP_PRODUCTS)

lnacc = con.execute(f"""
    SELECT
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(NOTENO   AS BIGINT)  AS NOTENO,
        CAST(BRANCH   AS INTEGER) AS BRANCH,
        CAST(CURBAL   AS DOUBLE)  AS CURBAL,
        CAST(BALANCE  AS DOUBLE)  AS BALANCE,
        CAST(APPRLIMT AS DOUBLE)  AS APPRLIMT,
        CAST(APPRLIM2 AS DOUBLE)  AS APPRLIM2,
        CASE
            WHEN CAST(ACCTNO AS BIGINT) BETWEEN 3000000000 AND 3999999999
                THEN 'OD'
            ELSE 'FL'
        END AS PRODTYPE,
        CAST(UNDRAWN  AS DOUBLE)  AS UNDRAWN
    FROM read_parquet('{LOAN_CACHE}')
    WHERE CAST(PRODUCT AS INTEGER) NOT IN ({_hp_list})
""").pl()

con.close()
gc.collect()
print(f"  LNACC rows: {len(lnacc):,}")

# ============================================================================
# STEP 6: EXTRACT BT A/C
# DATA BTACC(KEEP=ACCT BRANCH DCURBAL DBALANCE APPRLIMT PRODTYPE DUNDRAWN);
#   SET BT.BTMAST&REPTMON&NOWK;
#   IF SUBACCT='OV' AND CUSTCD NE ' ' AND DCURBAL NE .;  PRODTYPE='TB';
#   ACCT=ACCTNO; NOTENO=0;
# DATA AMC.BTACC(RENAME=(ACCT=ACCTNO DCURBAL=CURBAL DBALANCE=BALANCE
#                        DUNDRAWN=UNDRAWN)); SET BTACC;
#
# NOTE: NOTENO is set to 0 in the source data step, but the DATA statement's
# KEEP= list does not include NOTENO, so it is not retained on BTACC. When
# BTACC is later stacked with LNACC (SET LNACC BTACC), NOTENO is therefore
# missing for every BT-sourced record -- reproduced here as a NULL column.
# ============================================================================
print("\nStep 6: Extracting BT accounts...")

# print(pl.read_parquet(BT_CACHE).columns)

con = duckdb.connect(database=":memory:")

btacc = con.execute(f"""
    SELECT
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(BRANCH   AS INTEGER) AS BRANCH,
        CAST(DCURBAL  AS DOUBLE)  AS CURBAL,
        CAST(DBALANCE AS DOUBLE)  AS BALANCE,
        CAST(APPRLIMT AS DOUBLE)  AS APPRLIMT,
        'TB'                      AS PRODTYPE,
        CAST(DUNDRAWN AS DOUBLE)  AS UNDRAWN
    FROM read_parquet('{BT_CACHE}')
    WHERE SUBACCT = 'OV'
      AND TRIM(COALESCE(CUSTCD, '')) <> ''
      AND DCURBAL IS NOT NULL
""").pl()

con.close()
gc.collect()
print(f"  BTACC rows: {len(btacc):,}")

# Permanent side-output equivalent of "DATA AMC.BTACC(RENAME=...); SET BTACC;"
btacc.write_parquet(AMC_STORE_DIR / "BTACC.parquet")

# ============================================================================
# STEP 7: COMBINE AMCACC = SET LNACC BTACC; MERGE WITH CIS (BY ACCTNO)
# ============================================================================
print("\nStep 7: Combining LNACC + BTACC, joining CIS...")

btacc = btacc.with_columns(pl.lit(None).cast(pl.Int64).alias("NOTENO"))
lnacc = lnacc.with_columns(pl.col("NOTENO").cast(pl.Int64))
# btacc = btacc.with_columns(pl.col("APPRLIM2").cast(pl.Float64) if "APPRLIM2" in btacc.columns else pl.lit(None).cast(pl.Float64).alias("APPRLIM2"))
# if "APPRLIM2" not in btacc.columns:
#     btacc = btacc.with_columns(pl.lit(None).cast(pl.Float64).alias("APPRLIM2"))

if "APPRLIM2" in btacc.columns:
    btacc = btacc.with_columns(
        pl.col("APPRLIM2").cast(pl.Float64)
    )
else:
    btacc = btacc.with_columns(
        pl.lit(None).cast(pl.Float64).alias("APPRLIM2")
    )

amcacc_raw = pl.concat([lnacc, btacc.select(lnacc.columns)], how="vertical")

# INNER JOIN AMCACC & CIS BY ACCTNO; IF (A AND B) AND ICNO NE ' '
amcacc = amcacc_raw.join(cis, on="ACCTNO", how="inner").filter(
    pl.col("ICNO").is_not_null() & (pl.col("ICNO").str.strip_chars() != "")
)
print(f"  AMCACC (post CIS join) rows: {len(amcacc):,}")

# ============================================================================
# STEP 8: FILTER TO CUSTOMERS WITH SUMMED APPRLIMT > RM1,000,000 BY BRANCH+ICNO
# ============================================================================
print("\nStep 8: Filtering customers with combined limit > RM1,000,000...")

amclimt_keys = (
    amcacc.group_by(["BRANCH", "ICNO"])
    .agg(pl.col("APPRLIMT").sum().alias("APPRLIMT"))
    .filter(pl.col("APPRLIMT") > 1_000_000)
    .select(["BRANCH", "ICNO"])
)

amcacc = amcacc.join(amclimt_keys, on=["BRANCH", "ICNO"], how="inner")
print(f"  AMCACC (post RM1M filter) rows: {len(amcacc):,}")

# ============================================================================
# STEP 9: READ BRANCH FLAT FILE (fixed-width) & LEFT-JOIN BY BRANCH
# INFILE BRHFILE LRECL=80; INPUT @2 BRANCH 3. @6 BRABBR $3.;
# ============================================================================
print("\nStep 9: Reading branch flat file and merging...")

branch_rows = []
with open(INPUT_BRANCH_FILE, "rb") as fh:
    for raw in fh:
        line = raw.rstrip(b"\r\n")
        if len(line) < 8:
            continue
        branch = int(line[1:4].decode("latin1").strip() or 0)   # @2 3.
        brabbr = line[5:8].decode("latin1")                     # @6 $3.
        branch_rows.append({"BRANCH": branch, "BRABBR": brabbr})

brhdata = pl.DataFrame(branch_rows).with_columns(pl.col("BRANCH").cast(pl.Int64))
amcacc = amcacc.with_columns(pl.col("BRANCH").cast(pl.Int64))

# DATA AMC.AMC&REPTMON&NOWK; MERGE AMCACC(IN=A) BRHDATA; BY BRANCH; IF A;
amc_current = amcacc.join(brhdata, on="BRANCH", how="left")
print(f"  AMC current-period rows: {len(amc_current):,}")

# Persist current period to the AMC store (self-referencing history library)
AMC_CURRENT_KEY  = f"AMC{REPTMON}{NOWK}"
AMC_PREVIOUS_KEY = f"AMC{REPTMON1}{NOWK}"

amc_current.write_parquet(AMC_STORE_DIR / f"{AMC_CURRENT_KEY}.parquet")
print(f"  Stored current-period snapshot: {AMC_CURRENT_KEY}.parquet")

del amcacc_raw, amcacc, amclimt_keys, lnacc, btacc, brhdata
gc.collect()

# ============================================================================
# STEP 10: CURRENT-MONTH DATASET (CAMC)
# PROC SORT ... OUT=CAMC; BY BRANCH ICNO ACCTNO NOTENO;
# IF APPRLIMT/BALANCE/UNDRAWN missing THEN 0;
# IF (APPRLIMT GT 0 AND PRODTYPE NE 'FL') THEN CPERCT=ROUND(...);
# ============================================================================
print("\nStep 10: Building CAMC (current month)...")

camc = amc_current.sort(["BRANCH", "ICNO", "ACCTNO", "NOTENO"], nulls_last=False)
camc = camc.with_columns([
    pl.col("APPRLIMT").fill_null(0.0),
    pl.col("BALANCE").fill_null(0.0),
    pl.col("UNDRAWN").fill_null(0.0),
])
camc = camc.with_columns(
    pl.when((pl.col("APPRLIMT") > 0) & (pl.col("PRODTYPE") != "FL"))
    .then(((pl.col("APPRLIMT") - pl.col("UNDRAWN")) / pl.col("APPRLIMT") * 100).round(2))
    .otherwise(None)
    .alias("CPERCT")
)
print(f"  CAMC rows: {len(camc):,}")

# ============================================================================
# STEP 11: PREVIOUS-MONTH DATASET (PAMC)  -- read from AMC store
# PROC SORT DATA=AMC.AMC&REPTMON1&NOWK OUT=PAMC
#   (RENAME=(APPRLIMT=PLIMT BALANCE=PBAL UNDRAWN=PUNDRAWN));
# ============================================================================
print("\nStep 11: Building PAMC (previous month)...")

_prev_path = AMC_STORE_DIR / f"{AMC_PREVIOUS_KEY}.parquet"
if _prev_path.exists():
    pamc_raw = pl.read_parquet(_prev_path)
# else:
#     print(f"  WARNING: previous-period snapshot {AMC_PREVIOUS_KEY}.parquet not found "
#           f"(first-ever run for this cycle) -- treating previous month as empty.")
#     pamc_raw = amc_current.clear()
else:
    raise FileNotFoundError(
        f"Previous AMC snapshot missing: "
        f"{AMC_PREVIOUS_KEY}.parquet"
    )

pamc = pamc_raw.sort(["BRANCH", "ICNO", "ACCTNO", "NOTENO"], nulls_last=False).rename({
    "APPRLIMT": "PLIMT",
    "BALANCE": "PBAL",
    "UNDRAWN": "PUNDRAWN",
})
pamc = pamc.with_columns([
    pl.col("PLIMT").fill_null(0.0),
    pl.col("PBAL").fill_null(0.0),
    pl.col("PUNDRAWN").fill_null(0.0),
])
pamc = pamc.with_columns(
    pl.when((pl.col("PLIMT") > 0) & (pl.col("PRODTYPE") != "FL"))
    .then(((pl.col("PLIMT") - pl.col("PUNDRAWN")) / pl.col("PLIMT") * 100).round(2))
    .otherwise(None)
    .alias("PPERCT")
)
print(f"  PAMC rows: {len(pamc):,}")

# ============================================================================
# STEP 12: DETAIL MERGE  (AMC = MERGE PAMC CAMC; BY BRANCH ICNO ACCTNO NOTENO)
# SAS last-dataset-wins: CAMC values win over PAMC for overlapping columns.
# ============================================================================
print("\nStep 12: Merging PAMC + CAMC for detail report...")

# camc_pd = camc.to_pandas()
# pamc_pd = pamc.to_pandas()

# _keys = ["BRANCH", "ICNO", "ACCTNO", "NOTENO"]
# _shared = ["CUSTNAME", "PRIPHONE", "SECPHONE", "PRODTYPE", "BRABBR", "CURBAL", "APPRLIM2"]

# detail_merged = pd.merge(pamc_pd, camc_pd, on=_keys, how="outer", suffixes=("_p", "_c"))

camc_pd = camc.to_pandas()
pamc_pd = pamc.to_pandas()

_keys = ["BRANCH", "ICNO", "ACCTNO", "NOTENO"]

# Reproduce SAS MERGE sequencing behaviour
pamc_pd["_SEQ"] = (
    pamc_pd.groupby(_keys)
    .cumcount()
)

camc_pd["_SEQ"] = (
    camc_pd.groupby(_keys)
    .cumcount()
)

detail_merged = pd.merge(
    pamc_pd,
    camc_pd,
    on=_keys + ["_SEQ"],
    how="outer",
    suffixes=("_p", "_c")
)

detail_merged.drop(columns=["_SEQ"], inplace=True)

_shared = [
    "CUSTNAME", 
    "PRIPHONE", 
    "SECPHONE", 
    "PRODTYPE", 
    "BRABBR", 
    "CURBAL", 
    "APPRLIM2"
]

for col in _shared:
    detail_merged[col] = (
        detail_merged[f"{col}_c"]
        .combine_first(detail_merged[f"{col}_p"])
    )
    detail_merged.drop(columns=[f"{col}_c", f"{col}_p"], inplace=True)

# For testing purposes (Excluding TB from COLDBR detail report)
detail_merged = detail_merged[
    detail_merged["PRODTYPE"] != "TB"
]

detail_merged.sort_values(["BRANCH", "CUSTNAME", "PRODTYPE"], inplace=True, kind="stable")
detail_merged.reset_index(drop=True, inplace=True)
print(f"  Detail merged rows: {len(detail_merged):,}")

# ============================================================================
# FORMATTING HELPERS
# ============================================================================

def _comma(value, width: int, decimals: int = 0) -> str:
    """SAS COMMAw.d equivalent: right-justified in *width*, missing -> '.'."""
    if value is None or pd.isna(value):
        return ".".rjust(width)
    v = float(value)
    s = f"{v:,.{decimals}f}" if decimals > 0 else f"{v:,.0f}"
    return s.rjust(width)


def _place(buf: list, col: int, text: str) -> None:
    """Write *text* into buf starting at SAS column *col* (1-based)."""
    idx = col - 1
    end = idx + len(text)
    if end > len(buf):
        buf.extend([" "] * (end - len(buf)))
    buf[idx:end] = list(text)


class ReportWriter:
    """Accumulates ASA-carriage-controlled report lines.

    A SAS PUT statement that writes only blanks (e.g. PUT @001 '     ';) does
    not produce its own visible print record; SAS folds its carriage-control
    effect into the NEXT emitted line as a double-space ('0') control
    character. .blank() records that pending fold.
    """

    def __init__(self):
        self.lines: list[str] = []
        self._pending_asa = None

    def blank(self) -> None:
        self._pending_asa = "0"

    def emit(self, buf: list, asa: str = " ") -> None:
        if self._pending_asa is not None:
            asa = self._pending_asa
            self._pending_asa = None
        line = asa + "".join(buf[:LRECL])
        self.lines.append(line)


# ============================================================================
# STEP 13: GENERATE COLDBR (detail report)
# ============================================================================
print("\nStep 13: Generating COLDBR (detail) report...")


def _coldbr_header(writer: ReportWriter, branch: int, pagecnt: int) -> int:
    buf = [" "] * LRECL
    _place(buf, 1, "PUBLIC BANK BERHAD")
    _place(buf, 119, f"PAGE NO : {pagecnt}")
    writer.emit(buf, asa="1")

    buf = [" "] * LRECL
    _place(buf, 1, "DETAIL REPORT ON CUSTOMER UNDER ACCOUNT MANAGEMENT CON")
    _place(buf, 55, f"CEPT(AMC) AS AT {RDATE}")
    writer.emit(buf)

    buf = [" "] * LRECL
    _place(buf, 1, "REPORT ID: EIMBRAMC")
    writer.emit(buf)

    writer.blank()

    buf = [" "] * LRECL
    _place(buf, 1, "BRANCH CODE= ")
    _place(buf, 14, f"{int(branch or 0):03d}")
    writer.emit(buf)

    buf = [" "] * LRECL
    _place(buf, 36, "CURRENT MONTH")
    _place(buf, 73, "PREVIOUS MONTH")
    writer.emit(buf)

    buf = [" "] * LRECL
    _place(buf, 1, "NAME(TEL/NO)/")
    _place(buf, 23, "-" * 38)
    _place(buf, 63, "-" * 38)
    writer.emit(buf)

    buf = [" "] * LRECL
    _place(buf, 1, "I/C NO")
    _place(buf, 13, "FACILITY")
    _place(buf, 23, "APP/OPER LIMIT")
    _place(buf, 41, "O/S BALANCE")
    _place(buf, 53, "UTILISED")
    _place(buf, 63, "APP/OPER LIMIT")
    _place(buf, 81, "O/S BALANCE")
    _place(buf, 93, "UTILISED")
    _place(buf, 104, "OFFICER")
    _place(buf, 116, "REMARKS")
    writer.emit(buf)

    buf = [" "] * LRECL
    _place(buf, 1, "-" * 132)
    writer.emit(buf)

    return 9

DEBUG_FILE = OUTPUT_DIR / "AMCBR_debug.txt"

def generate_coldbr(df: pd.DataFrame) -> list:
    writer = ReportWriter()
    debug = open(DEBUG_FILE, "w", encoding="utf-8")
    pagecnt = 0
    linecnt = 0
    cur_branch = None
    cur_custname = None

    bcuaplmt = bcuosbal = bpraplmt = bprosbal = 0.0
    cuaplmt = cuosbal = cuudraw = praplmt = prosbal = prudraw = 0.0

    n = len(df)
    for i in range(n):
        row = df.iloc[i]
        branch = row["BRANCH"]
        custname = row["CUSTNAME"]
        is_first_branch = branch != cur_branch
        is_first_custname = is_first_branch or (custname != cur_custname)

        nxt = df.iloc[i + 1] if i + 1 < n else None
        is_last_branch = (nxt is None) or (nxt["BRANCH"] != branch)
        is_last_custname = is_last_branch or (nxt is not None and nxt["CUSTNAME"] != custname)

        if is_first_branch:
            pagecnt += 1
            linecnt = _coldbr_header(writer, branch, pagecnt)
            # bcuaplmt = row["APPRLIMT"] or 0
            # bcuosbal = row["BALANCE"] or 0
            # bpraplmt = row["PLIMT"] or 0
            # bprosbal = row["PBAL"] or 0
            bcuaplmt = 0.0
            bcuosbal = 0.0
            bpraplmt = 0.0
            bprosbal = 0.0
            cur_branch = branch

        cuaplmt += row["APPRLIMT"] or 0
        cuosbal += row["BALANCE"] or 0
        cuudraw += row["UNDRAWN"] or 0
        praplmt += row["PLIMT"] or 0
        prosbal += row["PBAL"] or 0
        prudraw += row["PUNDRAWN"] or 0

        bcuaplmt += row["APPRLIMT"] or 0
        bcuosbal += row["BALANCE"] or 0
        bpraplmt += row["PLIMT"] or 0
        bprosbal += row["PBAL"] or 0

        if is_first_custname:
            name = str(row["CUSTNAME"] or "").rstrip()
            pri = str(row["PRIPHONE"] or "").rstrip()
            sec = str(row["SECPHONE"] or "").rstrip()
            cust = f"{name} ({pri}/{sec})"
            buf = [" "] * LRECL
            _place(buf, 1, cust)
            writer.emit(buf)
            linecnt += 1
            cur_custname = custname

        if linecnt > 55:
            pagecnt += 1
            linecnt = _coldbr_header(writer, branch, pagecnt)

        buf = [" "] * LRECL
        _place(buf, 1, str(row["ICNO"] or ""))
        _place(buf, 16, str(row["PRODTYPE"] or ""))
        _place(buf, 23, _comma(row["APPRLIMT"], 14, 2))
        _place(buf, 38, _comma(row["BALANCE"], 14, 2))
        _place(buf, 54, _comma(row["CPERCT"], 6, 2))
        _place(buf, 60, "%")
        _place(buf, 63, _comma(row["PLIMT"], 14, 2))
        _place(buf, 78, _comma(row["PBAL"], 14, 2))
        _place(buf, 94, _comma(row["PPERCT"], 6, 2))
        _place(buf, 100, "%")
        writer.emit(buf)
        linecnt += 1

        if is_last_custname:

            print(
                f"Customer TOTAL | "
                f"Branch = {branch} | "
                f"Customer = {custname} |  "
                f"linecnt = {linecnt}\n"
            )

            cupcent = round((cuaplmt - cuudraw) / cuaplmt * 100, 2) if cuaplmt > 0 else 0.0
            prpcent = round((praplmt - prudraw) / praplmt * 100, 2) if praplmt > 0 else 0.0

            buf = [" "] * LRECL
            _place(buf, 1, "-" * 132)
            writer.emit(buf)

            buf = [" "] * LRECL
            _place(buf, 1, "TOTAL: ")
            _place(buf, 23, _comma(cuaplmt, 14, 2))
            _place(buf, 38, _comma(cuosbal, 14, 2))
            _place(buf, 54, _comma(cupcent, 6, 2))
            _place(buf, 60, "%")
            _place(buf, 63, _comma(praplmt, 14, 2))
            _place(buf, 78, _comma(prosbal, 14, 2))
            _place(buf, 94, _comma(prpcent, 6, 2))
            _place(buf, 100, "%")
            writer.emit(buf)

            buf = [" "] * LRECL
            _place(buf, 1, "-" * 132)
            writer.emit(buf)
            linecnt += 3

            writer.blank()
            linecnt += 1

            cuaplmt = cuosbal = cuudraw = praplmt = prosbal = prudraw = 0.0

        if is_last_branch:

            print(
                f"BR TOTAL | "
                f"Branch = {branch} | "
                f"linecnt = {linecnt}\n"
            )

            buf = [" "] * LRECL
            _place(buf, 1, "BR TOTAL: ")
            _place(buf, 23, _comma(bcuaplmt, 14, 2))
            _place(buf, 38, _comma(bcuosbal, 14, 2))
            _place(buf, 63, _comma(bpraplmt, 14, 2))
            _place(buf, 78, _comma(bprosbal, 14, 2))
            writer.emit(buf)

            buf = [" "] * LRECL
            _place(buf, 1, "=" * 132)
            writer.emit(buf)

            bcuaplmt = bcuosbal = bpraplmt = bprosbal = 0.0
            pagecnt = 0

    debug.close()

    return writer.lines


coldbr_lines = generate_coldbr(detail_merged)
print(f"  COLDBR lines: {len(coldbr_lines):,}")

# ============================================================================
# STEP 14: BUILD HQ SUMMARY INPUTS (re-derived from CAMC / PAMC, per source)
# ============================================================================
print("\nStep 14: Building HQ summary aggregates...")

pamc_hq = pamc.to_pandas().copy()

# For testing purposes (Excluding TB from HQ summary)
pamc_hq = pamc_hq[pamc_hq["PRODTYPE"] != "TB"]

pamc_hq["PFLLIMT"] = pamc_hq.apply(lambda r: r["PLIMT"] if r["PRODTYPE"] == "FL" else None, axis=1)
pamc_hq["PUNDRAW"] = pamc_hq.apply(lambda r: 0.0 if r["PRODTYPE"] == "FL" else r["PUNDRAWN"], axis=1)
pamc_hq["PODLIMT"] = pamc_hq.apply(lambda r: r["PLIMT"] if r["PRODTYPE"] != "FL" else None, axis=1)

_p1 = pamc_hq.groupby(["BRANCH", "ICNO"], as_index=False)[["PFLLIMT", "PODLIMT", "PBAL", "PUNDRAW"]].sum(min_count=1)
pamc_hq_agg = _p1.groupby("BRANCH", as_index=False).agg(
    PFLLIMT=("PFLLIMT", "sum"),
    PODLIMT=("PODLIMT", "sum"),
    PBAL=("PBAL", "sum"),
    PUNDRAW=("PUNDRAW", "sum"),
    # PCUST=("ICNO", "count"),
    PCUST=("ICNO", "nunique"),
)

camc_hq = camc.to_pandas().copy()

# For testing purposes (Excluding TB from HQ summary)
camc_hq = camc_hq[camc_hq["PRODTYPE"] != "TB"]

camc_hq["CFLLIMT"] = camc_hq.apply(lambda r: r["APPRLIMT"] if r["PRODTYPE"] == "FL" else None, axis=1)
camc_hq["CUNDRAW"] = camc_hq.apply(lambda r: 0.0 if r["PRODTYPE"] == "FL" else r["UNDRAWN"], axis=1)
camc_hq["CODLIMT"] = camc_hq.apply(lambda r: r["APPRLIMT"] if r["PRODTYPE"] != "FL" else None, axis=1)

_c1 = camc_hq.groupby(["BRANCH", "ICNO"], as_index=False)[["CFLLIMT", "CODLIMT", "BALANCE", "CUNDRAW"]].sum(min_count=1)
camc_hq_agg = _c1.groupby("BRANCH", as_index=False).agg(
    CFLLIMT=("CFLLIMT", "sum"),
    CODLIMT=("CODLIMT", "sum"),
    BALANCE=("BALANCE", "sum"),
    CUNDRAW=("CUNDRAW", "sum"),
    # CCUST=("ICNO", "count"),
    CCUST=("ICNO", "nunique"),
)

amclimit = pd.merge(camc_hq_agg, pamc_hq_agg, on="BRANCH", how="outer")


def _pct(numer_limit, undrawn):
    if numer_limit is None or pd.isna(numer_limit) or numer_limit <= 0:
        return 0.0
    return round((numer_limit - (undrawn or 0.0)) / numer_limit * 100, 2)


amclimit["CPERCT"] = amclimit.apply(lambda r: _pct(r["CODLIMT"], r["CUNDRAW"]), axis=1)
amclimit["PPERCT"] = amclimit.apply(lambda r: _pct(r["PODLIMT"], r["PUNDRAW"]), axis=1)
amclimit.sort_values("BRANCH", inplace=True, kind="stable")
amclimit.reset_index(drop=True, inplace=True)
print(f"  AMCLIMIT (HQ) rows: {len(amclimit):,}")

# ============================================================================
# STEP 15: GENERATE COLDHQ (summary report)
# ============================================================================
print("\nStep 15: Generating COLDHQ (summary) report...")


def _thousands(value):
    if value is None or pd.isna(value):
        return None
    return round(value / 1000)


def _coldhq_header(writer: ReportWriter, pagecnt: int) -> int:
    buf = [" "] * LRECL
    _place(buf, 1, "PUBLIC BANK BERHAD")
    _place(buf, 115, f"PAGE NO : {pagecnt}")
    writer.emit(buf, asa="1")

    buf = [" "] * LRECL
    _place(buf, 1, "SUMMARY REPORT BY BRANCH ON CUSTOMER UNDER ACCO")
    _place(buf, 48, f"UNT MANAGEMENT CONCEPT(AMC) AS AT {RDATE}")
    writer.emit(buf)

    buf = [" "] * LRECL
    _place(buf, 1, "REPORT ID: EIMBRAMC")
    writer.emit(buf)

    writer.blank()

    buf = [" "] * LRECL
    _place(buf, 27, f"{REPTMON}/{REPTYR}")
    _place(buf, 79, f"{REPTMON1}/{REPTYR1}")
    writer.emit(buf)

    buf = [" "] * LRECL
    _place(buf, 5, "-" * 51)
    _place(buf, 58, "-" * 51)
    writer.emit(buf)

    buf = [" "] * LRECL
    _place(buf, 5, "NO OF")
    _place(buf, 13, "APPROVED/")
    _place(buf, 35, "TOTAL")
    _place(buf, 51, "%")
    _place(buf, 58, "NO OF")
    _place(buf, 66, "APPROVED/")
    _place(buf, 88, "TOTAL")
    _place(buf, 103, "%")
    writer.emit(buf)

    buf = [" "] * LRECL
    _place(buf, 1, "BR")
    _place(buf, 5, "CUST")
    _place(buf, 13, "OPERATIVE LTD")
    _place(buf, 35, "OUTSTANDING")
    _place(buf, 48, "UTILISED")
    _place(buf, 58, "CUST")
    _place(buf, 66, "OPERATIVE LTD")
    _place(buf, 88, "OUTSTANDING")
    _place(buf, 100, "UTILISED")
    writer.emit(buf)

    buf = [" "] * LRECL
    _place(buf, 13, "RM(`000)")
    _place(buf, 35, "BALANCE")
    _place(buf, 66, "RM(`000)")
    _place(buf, 88, "BALANCE")
    writer.emit(buf)

    buf = [" "] * LRECL
    _place(buf, 35, "(FL+OD+TB)")
    _place(buf, 48, "(OD+TB)")
    _place(buf, 88, "(FL+OD+TB)")
    _place(buf, 100, "(OD+TB)")
    writer.emit(buf)

    buf = [" "] * LRECL
    _place(buf, 13, "FL")
    _place(buf, 24, "OD+TB")
    _place(buf, 35, "RM(`000)")
    _place(buf, 66, "FL")
    _place(buf, 77, "OD+TB")
    _place(buf, 88, "RM(`000)")
    writer.emit(buf)

    buf = [" "] * LRECL
    _place(buf, 1, "-" * 107)
    writer.emit(buf)

    return 12


def generate_coldhq(df: pd.DataFrame) -> list:
    writer = ReportWriter()
    pagecnt = 1
    linecnt = _coldhq_header(writer, pagecnt)

    tccust = tpcust = 0
    tcfllimt = tcodlimt = tbalance = tcundraw = 0.0
    tpfllimt = tpodlimt = tpbal = tpundraw = 0.0

    n = len(df)
    for i in range(n):
        row = df.iloc[i]
        is_last = (i == n - 1)

        if linecnt > 55:
            pagecnt += 1
            linecnt = _coldhq_header(writer, pagecnt)

        cfllimt = _thousands(row["CFLLIMT"])
        codlimt = _thousands(row["CODLIMT"])
        balance = _thousands(row["BALANCE"])
        cundraw = _thousands(row["CUNDRAW"])
        pfllimt = _thousands(row["PFLLIMT"])
        podlimt = _thousands(row["PODLIMT"])
        pbal    = _thousands(row["PBAL"])
        pundraw = _thousands(row["PUNDRAW"])

        buf = [" "] * LRECL
        _place(buf, 1, f"{int(row['BRANCH'] or 0):03d}")
        _place(buf, 5, _comma(row["CCUST"], 7))
        _place(buf, 13, _comma(cfllimt, 10))
        _place(buf, 24, _comma(codlimt, 10))
        _place(buf, 35, _comma(balance, 11))
        _place(buf, 50, _comma(row["CPERCT"], 6, 2))
        _place(buf, 58, _comma(row["PCUST"], 7))
        _place(buf, 66, _comma(pfllimt, 10))
        _place(buf, 77, _comma(podlimt, 10))
        _place(buf, 88, _comma(pbal, 11))
        _place(buf, 102, _comma(row["PPERCT"], 6, 2))
        writer.emit(buf)
        linecnt += 1

        tccust  += int(row["CCUST"]) if pd.notna(row["CCUST"]) else 0
        tcfllimt += cfllimt or 0.0
        tcodlimt += codlimt or 0.0
        tbalance += balance or 0.0
        tcundraw += cundraw or 0.0
        tpcust  += int(row["PCUST"]) if pd.notna(row["PCUST"]) else 0
        tpfllimt += pfllimt or 0.0
        tpodlimt += podlimt or 0.0
        tpbal   += pbal or 0.0
        tpundraw += pundraw or 0.0

        if is_last:
            tcperct = _pct(tcodlimt, tcundraw)
            tppercnt = _pct(tpodlimt, tpundraw)

            buf = [" "] * LRECL
            _place(buf, 1, "-" * 107)
            writer.emit(buf)

            buf = [" "] * LRECL
            _place(buf, 1, "TOT")
            _place(buf, 5, _comma(tccust, 7))
            _place(buf, 13, _comma(tcfllimt, 10))
            _place(buf, 24, _comma(tcodlimt, 10))
            _place(buf, 35, _comma(tbalance, 11))
            _place(buf, 50, _comma(tcperct, 6, 2))
            _place(buf, 58, _comma(tpcust, 7))
            _place(buf, 66, _comma(tpfllimt, 10))
            _place(buf, 77, _comma(tpodlimt, 10))
            _place(buf, 88, _comma(tpbal, 11))
            _place(buf, 102, _comma(tppercnt, 6, 2))
            writer.emit(buf)

            buf = [" "] * LRECL
            _place(buf, 1, "=" * 107)
            writer.emit(buf)
            linecnt += 3

    return writer.lines


coldhq_lines = generate_coldhq(amclimit)
print(f"  COLDHQ lines: {len(coldhq_lines):,}")

# ============================================================================
# STEP 16: WRITE OUTPUT FILES
# ============================================================================
print("\nStep 16: Writing output files...")

coldbr_file = build_output_file(OUTPUT_DIR, "AMCBR").with_suffix(".txt")
coldhq_file = build_output_file(OUTPUT_DIR, "AMCHQ").with_suffix(".txt")

with open(coldbr_file, "w", encoding="latin1") as fh:
    for ln in coldbr_lines:
        fh.write(ln + "\n")

with open(coldhq_file, "w", encoding="latin1") as fh:
    for ln in coldhq_lines:
        fh.write(ln + "\n")

print(f"  COLDBR output : {coldbr_file}")
print(f"  COLDHQ output : {coldhq_file}")

del detail_merged, camc, pamc, amc_current
gc.collect()

print("\nEIMBRAMC complete.")
