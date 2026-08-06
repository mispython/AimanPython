#!/usr/bin/env python3
"""
Program : EIIDARTB.py
Purpose : Extract Fixed Deposits (FD) and Current Account (CA) balances for
          Amanah Raya Trustees Berhad (ARTB) on a daily basis, classify each
          fund as Public Mutual (PM) or Non-Public Mutual (NON-PM), and
          produce a semicolon-delimited summary + detailed breakdown report
          for both Public Islamic Bank Berhad (PIBB) current accounts and
          fixed deposits.

Dependencies:
    PBBDPFMT.py -> caprod_format   (PUT(PRODUCT, CAPROD.) in DATA CA)
    ARTBFMT.py  -> get_fundmne     (PUT(FNAMEX, $FUNDMNE.) in DATA BNM.CAFD)
                   get_fundtype    (PUT(PMTYP, FUNDTYPE.)  in DATA BNM.CAFD)
                   PMFUND          (%LET PMFUND=(...) used for IN-clause match)
"""

import gc
import re
from datetime import date, timedelta, datetime
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values
from PBBDPFMT import caprod_format
from ARTBFMT import get_fundmne, get_fundtype, PMFUND

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII/EIIDARTB")

# INPUT_DEPOSIT_CA  = BASE_DIR / "input" / "prod" / "EIIDARTB" / "dpd_ca"   # holds dpd_ca{yymmdd}, dpd_fd{yymmdd}
# INPUT_DEPOSIT_FD  = BASE_DIR / "input" / "prod" / "EIIDARTB" / "dpd_fd"   # holds dpd_ca{yymmdd}, dpd_fd{yymmdd}
# INPUT_FD_DIR      = BASE_DIR / "input" / "prod" / "EIIDARTB" / "dpd_fdcd" # holds dpd_fdcd{yymmdd} (detailed FD/MNIFD)
INPUT_DEPOSIT_CA  = STG_DIR
INPUT_DEPOSIT_FD  = STG_DIR
INPUT_FD_DIR      = STG_DIR
INPUT_CISDP_FILE  = STG_DIR / "CISDP" / "CISDP_deposit.sas7bdat"            # static catalogued deposit CIS extract
INPUT_CISFD_FILE  = STG_DIR / "CISFD" / "CISFD_deposit.sas7bdat"            # static catalogued deposit CIS extract
INPUT_DPCUST_FILE = STG_DIR / "DPEXTCRM_CUSTINFO.txt"                       # static fixed-width text file

# Parquet cache directory (temporary intermediates from .sas7bdat sources)
CACHE_DIR = BASE_DIR / "input" / "cache" / "EIIDARTB"

# Generate time stamp
_ts = datetime.now().strftime("%y%m%d_%H%M%S")

OUTPUT_DIR = BASE_DIR / "output" / "EIIDARTB"
OUTPUT_FILE = OUTPUT_DIR / f"ARTB_DAILY_{_ts}.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

FILE_ENCODING = "latin1"

# ============================================================================
# STEP 1: REPORT DATE  (no reptdate.parquet — derive from REPTDATE.py)
# DATA REPTDATE; SET DEPOSIT.REPTDATE; ...
# NOTE: NOWK is computed in the original SAS via CALL SYMPUT but is never
# referenced anywhere else in the program (dead macro variable). It is
# reproduced below via REPTDATE.py's bucketing (which differs slightly from
# the original's exact-day WHEN(8)/WHEN(15)/WHEN(22) logic) purely for
# traceability; it has no effect on any output in this program.
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values(year_format="%Y")
reptdate: date = reptdate_values.reptdate
RDATE     = reptdate                                    # used for RMAINDT date arithmetic
REPTYEAR  = reptdate_values.reptyear                     # YEAR4.
REPTMON   = reptdate_values.reptmon                      # Z2.
REPTDAY   = reptdate_values.reptday                      # Z2.
TDATE     = reptdate.strftime("%d/%m/%Y")                # DDMMYY10.
NOWK      = reptdate_values.nowk                         # unused downstream (see note above)

print(f"  Report date (TDATE) : {TDATE}")
print(f"  Output file         : {OUTPUT_FILE.name}")

# ============================================================================
# STEP 2: PMFUND compressed list  (equivalent of COMPRESS("&PMFUND"))
# DATA _NULL_; CALL SYMPUT('PMFUNDX',COMPRESS("&PMFUND")); RUN;
# The compressed (no-space) fund-name set is matched against FNAMEX
# (also COMPRESS(FUNDNAME)) further below.
# ============================================================================
PMFUND_COMPRESSED = {re.sub(r"\s+", "", fund) for fund in PMFUND}

# Fixed CUSTNO list used to filter the combined CA/FD dataset (DATA CAFD)
CAFD_CUSTNO_FILTER = {
    3523050, 11335374, 11880426, 3728510,
    13158067, 14368177, 14368641, 14369065,
    14387105, 14932947, 14960789, 15254645,
    15241330, 15310964, 15352797,
}

# ============================================================================
# HELPER: CACHE STAMP  (skip re-conversion if .sas7bdat hasn't changed)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


# ============================================================================
# HELPER: STREAM .sas7bdat -> PARQUET  (mirrors EIBDLN1M.py pattern)
# ============================================================================
def sas_to_parquet(sas_path: Path, cache_path: Path, tag: str, chunk_rows: int = 250_000) -> None:
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0

    reader = pd.read_sas(sas_path, encoding=FILE_ENCODING, chunksize=chunk_rows)
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
                    except Exception as exc:
                        print(f"  [{tag}] WARNING: cannot cast '{field.name}' "
                              f"from {col.type} to {field.type}: {exc} - filling nulls")
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


def ensure_cached(sas_path: Path, cache_path: Path, tag: str) -> Path:
    if not _cache_is_fresh(sas_path, cache_path):
        sas_to_parquet(sas_path, cache_path, tag)
    else:
        print(f"  [{tag}] Cache fresh - skipping conversion.")
    return cache_path


# ============================================================================
# HELPER: strict "latest dated file" resolver avoiding overlapping prefixes
# (dpd_fd vs dpd_fdcd) — uses input_date.extract_key for date ranking.
# ============================================================================
from input_date import extract_key  # noqa: E402  (import placed near use)


def get_latest_file_strict(directory: Path, name_pattern: "re.Pattern") -> Path:
    candidates = [f for f in directory.iterdir() if f.is_file() and name_pattern.match(f.name)]
    if not candidates:
        raise FileNotFoundError(f"No files matching pattern in {directory}")
    latest = max(candidates, key=lambda f: extract_key(f.name))
    print(f"  [FILE_RESOLVER] Selected latest: {latest.name}")
    return latest


# ============================================================================
# STEP 3: RESOLVE INPUT FILES
# ============================================================================
print("\nStep 3: Resolving dated input files...")

_dpd_ca_pattern   = re.compile(r"^dpd_ca\d{6}\.sas7bdat$", re.IGNORECASE)
_dpd_fd_pattern   = re.compile(r"^dpd_fd\d{6}\.sas7bdat$", re.IGNORECASE)   # excludes dpd_fdcd
_dpd_fdcd_pattern = re.compile(r"^dpd_fdcd\d{6}\.sas7bdat$", re.IGNORECASE)

dpd_ca_path   = get_latest_file_strict(INPUT_DEPOSIT_CA, _dpd_ca_pattern)     # DEPOSIT.CURRENT
dpd_fd_path   = get_latest_file_strict(INPUT_DEPOSIT_FD, _dpd_fd_pattern)     # DEPOSIT.FD
dpd_fdcd_path = get_latest_file_strict(INPUT_FD_DIR, _dpd_fdcd_pattern)       # FD.FD (MNIFD)

# ============================================================================
# STEP 4: CACHE ALL .sas7bdat SOURCES TO PARQUET
# ============================================================================
print("\nStep 4: Caching .sas7bdat sources to Parquet...")

CA_CACHE       = CACHE_DIR / f"{dpd_ca_path.stem}.parquet"
FD_CACHE       = CACHE_DIR / f"{dpd_fd_path.stem}.parquet"
FDCD_CACHE     = CACHE_DIR / f"{dpd_fdcd_path.stem}.parquet"
CISDP_CACHE    = CACHE_DIR / "cisdp_deposit.parquet"
CISFD_CACHE    = CACHE_DIR / "cisfd_deposit.parquet"

ensure_cached(dpd_ca_path, CA_CACHE, "CA")
ensure_cached(dpd_fd_path, FD_CACHE, "FD")
ensure_cached(dpd_fdcd_path, FDCD_CACHE, "FDCD")
ensure_cached(INPUT_CISDP_FILE, CISDP_CACHE, "CISDP")
ensure_cached(INPUT_CISFD_FILE, CISFD_CACHE, "CISFD")

# ============================================================================
# STEP 5: DATA CISCA  (CIS current-account customer lookup)
# IF SECCUST='901'; IF (3000000000<=ACCTNO<=3999999999);
# ICNO = NEWIC if present else OLDIC;  CISTYPE='CA'
# ============================================================================
print("\nStep 5: Building CISCA...")

con = duckdb.connect(database=":memory:")
cisca = con.execute(f"""
    SELECT
        CAST(CUSTNO   AS BIGINT)  AS CUSTNO,
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(CUSTNAME AS VARCHAR) AS CUSTNAME,
        CASE WHEN NEWIC IS NOT NULL AND TRIM(NEWIC) <> '' THEN NEWIC ELSE OLDIC END AS ICNO,
        'CA' AS CISTYPE
    FROM read_parquet('{CISDP_CACHE}')
    WHERE CAST(SECCUST AS VARCHAR) = '901'
      AND CAST(ACCTNO AS BIGINT) BETWEEN 3000000000 AND 3999999999
""").pl()
con.close()
print(f"  CISCA rows: {len(cisca):,}")

# ============================================================================
# STEP 6: DATA CA  (current account transactions)
# PRODCD = PUT(PRODUCT, CAPROD.); IF CURBAL>0 AND PRODCD NE 'N';
# ============================================================================
print("\nStep 6: Building CA...")

con = duckdb.connect(database=":memory:")
ca_raw = con.execute(f"""
    SELECT
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(PRODUCT  AS INTEGER) AS PRODUCT,
        CAST(CURBAL   AS DOUBLE)  AS CURBAL,
        CAST(INTPAYBL AS DOUBLE)  AS INTPAYBL,
        CAST(NAME     AS VARCHAR) AS NAME,
        CAST(INTRATE  AS DOUBLE)  AS INTRATE
    FROM read_parquet('{CA_CACHE}')
    WHERE CAST(CURBAL AS DOUBLE) > 0
""").pl()
con.close()

ca_raw = ca_raw.with_columns(
    pl.col("PRODUCT").map_elements(caprod_format, return_dtype=pl.Utf8).alias("PRODCD")
).filter(pl.col("PRODCD") != "N")

print(f"  CA rows (post PRODCD filter): {len(ca_raw):,}")

# ---- MERGE CA(IN=A) CISCA; BY ACCTNO; IF A; ----------------------------------
# CUSTNAME comes from CISCA when matched, else falls back to CA.NAME.
ca_pd = ca_raw.to_pandas()
cisca_pd = cisca.to_pandas()

ca_merged = pd.merge(
    ca_pd,
    cisca_pd[["ACCTNO", "CUSTNO", "CUSTNAME", "ICNO", "CISTYPE"]],
    on="ACCTNO",
    how="left",
)
ca_merged["CUSTNAME"] = ca_merged["CUSTNAME"].where(
    ca_merged["CUSTNAME"].notna() & (ca_merged["CUSTNAME"].str.strip() != ""),
    ca_merged["NAME"],
)
ca_merged["BALANCE"] = ca_merged["CURBAL"].fillna(0.0) + ca_merged["INTPAYBL"].fillna(0.0)
ca_merged["CISTYPE"] = ca_merged["CISTYPE"].fillna("CA")

CA = pl.from_pandas(ca_merged)
del ca_raw, ca_pd, cisca_pd, ca_merged
gc.collect()

# ============================================================================
# STEP 7: DATA CISFD  (CIS fixed-deposit customer lookup)
# IF SECCUST='901';
# IF (1000000000<=ACCTNO<=1999999999) OR (7000000000<=ACCTNO<=7999999999)
#    OR (4000000000<=ACCTNO<=6999999999);
# ============================================================================
print("\nStep 7: Building CISFD...")

con = duckdb.connect(database=":memory:")
cisfd = con.execute(f"""
    SELECT
        CAST(CUSTNO   AS BIGINT)  AS CUSTNO,
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(CUSTNAME AS VARCHAR) AS CUSTNAME,
        CASE WHEN NEWIC IS NOT NULL AND TRIM(NEWIC) <> '' THEN NEWIC ELSE OLDIC END AS ICNO,
        'FD' AS CISTYPE
    FROM read_parquet('{CISFD_CACHE}')
    WHERE CAST(SECCUST AS VARCHAR) = '901'
      AND (
            CAST(ACCTNO AS BIGINT) BETWEEN 1000000000 AND 1999999999
         OR CAST(ACCTNO AS BIGINT) BETWEEN 7000000000 AND 7999999999
         OR CAST(ACCTNO AS BIGINT) BETWEEN 4000000000 AND 6999999999
      )
""").pl()
con.close()
print(f"  CISFD rows: {len(cisfd):,}")

# ============================================================================
# STEP 8: DATA FD  (fixed-deposit transactions, basic dataset)
# IF CURBAL>0;
# ============================================================================
print("\nStep 8: Building FD...")

con = duckdb.connect(database=":memory:")
fd_raw = con.execute(f"""
    SELECT
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(CURBAL   AS DOUBLE)  AS CURBAL,
        CAST(INTPAYBL AS DOUBLE)  AS INTPAYBL,
        CAST(NAME     AS VARCHAR) AS NAME,
        CAST(PURPOSE  AS VARCHAR) AS PURPOSE
    FROM read_parquet('{FD_CACHE}')
    WHERE CAST(CURBAL AS DOUBLE) > 0
""").pl()
con.close()
print(f"  FD rows (pre-merge): {len(fd_raw):,}")

# ---- MERGE FD(IN=A) CISFD; BY ACCTNO; IF A AND PURPOSE NE '2'; --------------
fd_pd = fd_raw.to_pandas()
cisfd_pd = cisfd.to_pandas()

fd_merged = pd.merge(
    fd_pd,
    cisfd_pd[["ACCTNO", "CUSTNO", "CUSTNAME", "ICNO", "CISTYPE"]],
    on="ACCTNO",
    how="left",
)
fd_merged = fd_merged[fd_merged["PURPOSE"] != "2"].copy()
fd_merged["CUSTNAME"] = fd_merged["CUSTNAME"].where(
    fd_merged["CUSTNAME"].notna() & (fd_merged["CUSTNAME"].str.strip() != ""),
    fd_merged["NAME"],
)
fd_merged["BALANCE"] = fd_merged["CURBAL"].fillna(0.0) + fd_merged["INTPAYBL"].fillna(0.0)
fd_merged["CISTYPE"] = fd_merged["CISTYPE"].fillna("FD")

FD = pl.from_pandas(fd_merged)
del fd_raw, fd_pd, cisfd_pd, fd_merged
gc.collect()
print(f"  FD rows (post-merge/filter): {len(FD):,}")

# ============================================================================
# STEP 9: DATA CAFD  (stack CA + FD, filter fixed CUSTNO list)
# IF A THEN CABAL=CURBAL; ELSE FDBAL=CURBAL; IF CURBAL>0 THEN OUTPUT;
# ============================================================================
print("\nStep 9: Building CAFD (CA + FD stack, CUSTNO filter)...")

ca_stack = CA.select(["ACCTNO", "CUSTNO", "CUSTNAME", "ICNO", "CISTYPE", "CURBAL",
                       "BALANCE", "INTRATE"]).with_columns([
    pl.col("CURBAL").alias("CABAL"),
    pl.lit(None, dtype=pl.Float64).alias("FDBAL"),
])
fd_stack = FD.select(["ACCTNO", "CUSTNO", "CUSTNAME", "ICNO", "CISTYPE", "CURBAL",
                       "BALANCE"]).with_columns([
    pl.lit(None, dtype=pl.Float64).alias("CABAL"),
    pl.col("CURBAL").alias("FDBAL"),
    pl.lit(None, dtype=pl.Float64).alias("INTRATE"),
])

CAFD = pl.concat([ca_stack, fd_stack], how="diagonal")
CAFD = CAFD.filter(
    pl.col("CUSTNO").is_in(list(CAFD_CUSTNO_FILTER)) & (pl.col("CURBAL") > 0)
)
print(f"  CAFD rows: {len(CAFD):,}")

# ============================================================================
# STEP 10: DATA DEPO  (read DPCUST fixed-width text file)
# INPUT @001 ACCTNO 11.  @012 FUNDNAME $40.;  FNAMEX = COMPRESS(FUNDNAME);
# ============================================================================
print("\nStep 10: Reading DPCUST fixed-width file...")

depo_rows = []
with open(INPUT_DPCUST_FILE, "r", encoding=FILE_ENCODING) as fh:
    for raw_line in fh:
        line = raw_line.rstrip("\n").rstrip("\r")
        if not line.strip():
            continue
        padded = line.ljust(51)
        acctno_str = padded[0:11].strip()
        fundname = padded[11:51].rstrip()
        try:
            acctno = int(acctno_str)
        except ValueError:
            continue
        fnamex = re.sub(r"\s+", "", fundname)
        depo_rows.append({"ACCTNO": acctno, "FUNDNAME": fundname, "FNAMEX": fnamex})

DEPO = pl.DataFrame(depo_rows, schema={"ACCTNO": pl.Int64, "FUNDNAME": pl.Utf8, "FNAMEX": pl.Utf8})
print(f"  DEPO rows: {len(DEPO):,}")

# ============================================================================
# STEP 11: DATA BNM.CAFD
# MERGE CAFD(IN=A) DEPO; BY ACCTNO; IF A;
# IF FUNDNAME=' ' THEN FUNDNAME=CUSTNAME;
# IF FNAMEX IN &PMFUNDX THEN PMTYP=1; ELSE PMTYP=2;
# PMTYPE = PUT(PMTYP,FUNDTYPE.); FUNDMNE = PUT(FNAMEX,$FUNDMNE.);
#
# NOTE (faithful to SAS): FNAMEX is populated only from the DEPO merge; it is
# NOT recomputed from the FUNDNAME fallback below. Unmatched ACCTNO rows keep
# FNAMEX blank and therefore always classify as PMTYP=2 (NON-PM), exactly as
# the original SAS logic behaves.
# ============================================================================
print("\nStep 11: Building BNM.CAFD...")

cafd_pd = CAFD.to_pandas()
depo_pd = DEPO.to_pandas()

bnm_cafd_pd = pd.merge(cafd_pd, depo_pd, on="ACCTNO", how="left")
bnm_cafd_pd["FUNDNAME"] = bnm_cafd_pd["FUNDNAME"].where(
    bnm_cafd_pd["FUNDNAME"].notna() & (bnm_cafd_pd["FUNDNAME"].str.strip() != ""),
    bnm_cafd_pd["CUSTNAME"],
)
bnm_cafd_pd["FNAMEX"] = bnm_cafd_pd["FNAMEX"].fillna("")

bnm_cafd_pd["PMTYP"] = bnm_cafd_pd["FNAMEX"].apply(
    lambda v: 1 if v in PMFUND_COMPRESSED else 2
)
bnm_cafd_pd["PMTYPE"] = bnm_cafd_pd["PMTYP"].apply(get_fundtype)
bnm_cafd_pd["FUNDMNE"] = bnm_cafd_pd["FNAMEX"].apply(get_fundmne)

BNM_CAFD = pl.from_pandas(bnm_cafd_pd)
del cafd_pd, depo_pd, bnm_cafd_pd
gc.collect()
print(f"  BNM.CAFD rows: {len(BNM_CAFD):,}")

# ============================================================================
# STEP 12: SUMCAFD (BY PMTYPE sums) / TOTCAFD (grand total)
# ============================================================================
print("\nStep 12: Summarising CABAL/FDBAL/CURBAL by PMTYPE...")

sumcafd_pd = (
    BNM_CAFD.select(["PMTYPE", "CABAL", "FDBAL", "CURBAL"])
    .to_pandas()
    .groupby("PMTYPE", as_index=False, sort=False)
    .sum(numeric_only=True)
)
totcafd_pd = BNM_CAFD.select(["CABAL", "FDBAL", "CURBAL"]).to_pandas().sum(numeric_only=True)

# ============================================================================
# STEP 13: SPLIT ARCA / ARFD  BY CISTYPE
# ARFD keeps only ACCTNO CUSTNO CUSTNAME FUNDMNE FUNDNAME PMTYP
# ============================================================================
print("\nStep 13: Splitting ARCA / ARFD by CISTYPE...")

ARCA = BNM_CAFD.filter(pl.col("CISTYPE") == "CA").sort("ACCTNO")
ARFD_base = (
    BNM_CAFD.filter(pl.col("CISTYPE") != "CA")
    .select(["ACCTNO", "CUSTNO", "CUSTNAME", "FUNDMNE", "FUNDNAME", "PMTYP"])
    .sort("ACCTNO")
)

# ---- DATA ARCA1 ARCA2; IF PMTYP=1 THEN OUTPUT ARCA1; ELSE OUTPUT ARCA2; -----
ARCA1 = ARCA.filter(pl.col("PMTYP") == 1)
ARCA2 = ARCA.filter(pl.col("PMTYP") != 1)

sumarca1 = ARCA1.select(["CURBAL", "BALANCE"]).to_pandas().sum(numeric_only=True)
sumarca2 = ARCA2.select(["CURBAL", "BALANCE"]).to_pandas().sum(numeric_only=True)
totarca  = ARCA.select(["CURBAL", "BALANCE"]).to_pandas().sum(numeric_only=True)

# ============================================================================
# STEP 14: PROC SORT DATA=FD.FD OUT=MNIFD; BY ACCTNO;   (dpd_fdcd file)
# ============================================================================
print("\nStep 14: Loading MNIFD (FD.FD detailed dataset)...")

con = duckdb.connect(database=":memory:")
mnifd = con.execute(f"""
    SELECT
        CAST(ACCTNO  AS BIGINT)  AS ACCTNO,
        CAST(DEPODTE AS BIGINT)  AS DEPODTE,
        CAST(MATDATE AS BIGINT)  AS MATDATE,
        CAST(TERM    AS DOUBLE)  AS TERM,
        CAST(MATID   AS VARCHAR) AS MATID,
        CAST(INTPAY  AS DOUBLE)  AS INTPAY,
        CAST(CURBAL  AS DOUBLE)  AS CURBAL,
        CAST(RATE    AS DOUBLE)  AS RATE
    FROM read_parquet('{FDCD_CACHE}')
    ORDER BY ACCTNO
""").pl()
con.close()
print(f"  MNIFD rows: {len(mnifd):,}")


def _parse_depodte(raw: Optional[int]) -> Optional[date]:
    """PUT(DEPODTE,Z11.) -> SUBSTR(...,1,8) -> INPUT(...,MMDDYY8.)  (MM DD YYYY)."""
    if raw is None:
        return None
    z11 = f"{int(raw):011d}"
    mmddyyyy = z11[0:8]
    mm, dd, yyyy = int(mmddyyyy[0:2]), int(mmddyyyy[2:4]), int(mmddyyyy[4:8])
    try:
        return date(yyyy, mm, dd)
    except ValueError:
        return None


def _parse_matdate(raw: Optional[int]) -> Optional[date]:
    """PUT(MATDATE,Z8.) -> INPUT(...,YYMMDD8.)  (YYYY MM DD)."""
    if raw is None:
        return None
    z8 = f"{int(raw):08d}"
    yyyy, mm, dd = int(z8[0:4]), int(z8[4:6]), int(z8[6:8])
    try:
        return date(yyyy, mm, dd)
    except ValueError:
        return None


# ============================================================================
# STEP 15: DATA ARFD
# MERGE ARFD(IN=A) MNIFD(IN=B); BY ACCTNO; IF A AND B;
# MATID default; DEPODTE/MATDATE parse; RMAINDT = MATDATE - RDATE; BALANCE
# ============================================================================
print("\nStep 15: Building ARFD (inner join with MNIFD)...")

arfd_pd = pd.merge(
    ARFD_base.to_pandas(), mnifd.to_pandas(), on="ACCTNO", how="inner"
)

arfd_pd["MATID"] = arfd_pd["MATID"].apply(lambda v: "M" if v is None or str(v).strip() in ("", ".") else v)

_depodte_dates = arfd_pd["DEPODTE"].apply(_parse_depodte)
_matdate_dates = arfd_pd["MATDATE"].apply(_parse_matdate)

arfd_pd["DEPDTE"] = _depodte_dates.apply(lambda d: d.strftime("%d/%m/%y") if d else "")
arfd_pd["MATDTE"] = _matdate_dates.apply(lambda d: d.strftime("%d/%m/%y") if d else "")
arfd_pd["RMAINDT"] = _matdate_dates.apply(lambda d: (d - RDATE).days if d else 0)
arfd_pd["BALANCE"] = arfd_pd["CURBAL"].fillna(0.0) + arfd_pd["INTPAY"].fillna(0.0)

ARFD = pl.from_pandas(arfd_pd)
del arfd_pd, mnifd
gc.collect()
print(f"  ARFD rows: {len(ARFD):,}")

# ---- DATA ARFD1 ARFD2; PROC SORT BY FUNDNAME  (needed for FIRST./LAST.) ----
ARFD1 = ARFD.filter(pl.col("PMTYP") == 1).sort("FUNDNAME")
ARFD2 = ARFD.filter(pl.col("PMTYP") != 1).sort("FUNDNAME")

sumarfd1 = ARFD1.select(["TERM", "RMAINDT", "CURBAL", "BALANCE"]).to_pandas().sum(numeric_only=True)
sumarfd2 = ARFD2.select(["TERM", "RMAINDT", "CURBAL", "BALANCE"]).to_pandas().sum(numeric_only=True)
totarfd  = ARFD.select(["TERM", "RMAINDT", "CURBAL", "BALANCE"]).to_pandas().sum(numeric_only=True)

# ---- GTCAFD: SET TOTARCA TOTARFD; PROC SUMMARY (grand total) --------------
gtcafd = {
    "TERM":    totarfd.get("TERM", 0.0),
    "RMAINDT": totarfd.get("RMAINDT", 0.0),
    "CURBAL":  totarca.get("CURBAL", 0.0) + totarfd.get("CURBAL", 0.0),
    "BALANCE": totarca.get("BALANCE", 0.0) + totarfd.get("BALANCE", 0.0),
}

# ============================================================================
# REPORT FORMATTING HELPERS
# ============================================================================
class SasLine:
    """Simulates SAS PUT @col value; column-pointer (forward-only) semantics."""

    def __init__(self) -> None:
        self._buf: list[str] = []
        self._pos = 0

    def at(self, col: int) -> "SasLine":
        self._pos = col - 1
        if self._pos > len(self._buf):
            self._buf.extend([" "] * (self._pos - len(self._buf)))
        return self

    def put(self, text: str) -> "SasLine":
        end = self._pos + len(text)
        if end > len(self._buf):
            self._buf.extend([" "] * (end - len(self._buf)))
        for i, ch in enumerate(text):
            self._buf[self._pos + i] = ch
        self._pos = end
        return self

    def line(self) -> str:
        return "".join(self._buf)


def _fmt_comma(value, width: int = 18, decimals: int = 2) -> str:
    """COMMA18.2 style formatting, right-justified."""
    if value is None:
        return " " * width
    try:
        v = float(value)
    except (TypeError, ValueError):
        return " " * width
    return f"{v:,.{decimals}f}".rjust(width)


def _fmt_int(value, width: int = 5) -> str:
    """Plain right-justified integer (e.g. RMAINDT format 5.)."""
    if value is None:
        return " " * width
    try:
        return f"{int(round(float(value)))}".rjust(width)
    except (TypeError, ValueError):
        return " " * width


def _fmt_plain(value) -> str:
    """Default (unformatted) numeric/character conversion."""
    if value is None:
        return ""
    if isinstance(value, float) and value == int(value):
        return str(int(value))
    return str(value)


# ============================================================================
# STEP 16: BUILD REPORT  (Table 1 - Summary, Table 2 - Detailed Breakdown)
# ============================================================================
print("\nStep 16: Generating report...")

output_lines: list[str] = []

# ---- Table 1 header ----
output_lines.append(SasLine().at(1).put("P U B L I C   I S L A M I C   B A N K   B E R H A D").line())
output_lines.append(SasLine().at(1).put(f"REPORT ID : EIIDARTB @ {TDATE}").line())
output_lines.append("")
output_lines.append(SasLine().at(1).put("TABLE 1: SUMMARY TABLE FOR AMANAH RAYA GROUP").line())
output_lines.append(
    SasLine().at(1).put("PUBLIC ISLAMIC BANK (PIBB)")
    .at(30).put(";;;CURRENT ACCOUNTS")
    .at(59).put(";FIXED DEPOSITS")
    .at(90).put(";TOTAL (RM)")
    .line()
)

# ---- Table 1 detail (per PMTYPE) ----
for idx, row in enumerate(sumcafd_pd.to_dict("records"), start=1):
    output_lines.append(
        SasLine().at(1).put(f"{idx}) {row['PMTYPE']}")
        .at(30).put(";;;").put(_fmt_comma(row["CABAL"]))
        .at(59).put(";").put(_fmt_comma(row["FDBAL"]))
        .at(90).put(";").put(_fmt_comma(row["CURBAL"]))
        .line()
    )

# ---- Table 1 total ----
output_lines.append(
    SasLine().at(1).put("TOTAL")
    .at(30).put(";;;").put(_fmt_comma(totcafd_pd.get("CABAL")))
    .at(59).put(";").put(_fmt_comma(totcafd_pd.get("FDBAL")))
    .at(90).put(";").put(_fmt_comma(totcafd_pd.get("CURBAL")))
    .line()
)

# ---- Table 2 header ----
output_lines.append("")
output_lines.append(SasLine().at(1).put("TABLE 2: DETAILED BREAKDOWN LISTING FOR AMANAH RAYA GROUP").line())
output_lines.append(SasLine().at(1).put("I) CURRENT ACCOUNT").line())
output_lines.append(
    SasLine().at(1).put("CIS NO.")
    .at(15).put(";CUST. MNEMONIC")
    .at(31).put(";CUSTOMER FULL NAME")
    .at(149).put(";CURRENT BALANCE")
    .at(169).put(";INTEREST (%)")
    .at(184).put(";BALANCE (CURRENT BALANCE + ACCRUED INTEREST)")
    .line()
)


def _print_ca_section(dsn: pl.DataFrame, subtotal: pd.Series, sub_label: str, ftype: str) -> None:
    records = dsn.to_dicts()
    for i, row in enumerate(records):
        if i == 0:
            output_lines.append(SasLine().at(1).put(f"({sub_label}) {ftype} MUTUAL FUND PORTFOLIO").line())
        output_lines.append(
            SasLine().at(1).put(_fmt_plain(row.get("CUSTNO")))
            .at(15).put(";").put(_fmt_plain(row.get("FUNDMNE")))
            .at(31).put(";").put(_fmt_plain(row.get("FUNDNAME")))
            .at(149).put(";").put(_fmt_comma(row.get("CURBAL")))
            .at(169).put(";").put(_fmt_plain(row.get("INTRATE")))
            .at(184).put(";").put(_fmt_comma(row.get("BALANCE")))
            .line()
        )
    output_lines.append(SasLine().at(1).put(" ").line())
    output_lines.append(
        SasLine().at(1).put(f"SUBTOTAL ({sub_label})")
        .at(15).put(";")
        .at(31).put(";")
        .at(149).put(";").put(_fmt_comma(subtotal.get("CURBAL")))
        .at(169).put("; -")
        .at(184).put(";").put(_fmt_comma(subtotal.get("BALANCE")))
        .line()
    )
    output_lines.append(SasLine().at(1).put(" ").line())


_print_ca_section(ARCA1, sumarca1, "A", "PUBLIC")
_print_ca_section(ARCA2, sumarca2, "B", "NON-PUBLIC")

output_lines.append(
    SasLine().at(1).put("TOTAL (A)+(B)")
    .at(15).put(";")
    .at(31).put(";")
    .at(149).put(";").put(_fmt_comma(totarca.get("CURBAL")))
    .at(169).put("; -")
    .at(184).put(";").put(_fmt_comma(totarca.get("BALANCE")))
    .line()
)

# ---- Table 2, Part II header (Fixed Deposits) ----
output_lines.append(SasLine().at(1).put(" ").line())
output_lines.append(SasLine().at(1).put("II) FIXED DEPOSITS").line())
output_lines.append(
    SasLine().at(1).put("CIS NO.")
    .at(15).put(";CUST. MNEMONIC")
    .at(31).put(";CUSTOMER FULL NAME")
    .at(72).put(";TRANSACTION DATE")
    .at(90).put(";MATURITY DATE")
    .at(105).put(";ORIGINAL TENOR")
    .at(121).put(";REMAINING DAYS TO MATURITY")
    .at(149).put(";CURRENT BALANCE")
    .at(169).put(";INTEREST (%)")
    .at(184).put(";BALANCE (CURRENT BALANCE + ACCRUED INTEREST)")
    .line()
)


def _print_fd_section(dsn: pl.DataFrame, subtotal: pd.Series, sub_label: str, ftype: str) -> None:
    records = dsn.to_dicts()
    fterm = frmaindt = fcurbal = fbalance = 0.0
    prev_fundname = None

    for i, row in enumerate(records):
        fundname = row.get("FUNDNAME")
        is_first = fundname != prev_fundname
        is_last = (i == len(records) - 1) or (records[i + 1].get("FUNDNAME") != fundname)

        if is_first:
            fterm = frmaindt = fcurbal = fbalance = 0.0
        fterm += row.get("TERM") or 0.0
        frmaindt += row.get("RMAINDT") or 0.0
        fcurbal += row.get("CURBAL") or 0.0
        fbalance += row.get("BALANCE") or 0.0

        if i == 0:
            output_lines.append(SasLine().at(1).put(f"({sub_label}) {ftype} MUTUAL FUND PORTFOLIO").line())

        output_lines.append(
            SasLine().at(1).put(_fmt_plain(row.get("CUSTNO")))
            .at(15).put(";").put(_fmt_plain(row.get("FUNDMNE")))
            .at(31).put(";").put(_fmt_plain(row.get("FUNDNAME")))
            .at(72).put(";").put(_fmt_plain(row.get("DEPDTE")))
            .at(90).put(";").put(_fmt_plain(row.get("MATDTE")))
            .at(105).put(";").put(_fmt_plain(row.get("TERM")))
            .at(111).put(_fmt_plain(row.get("MATID")))
            .at(121).put(";").put(_fmt_int(row.get("RMAINDT")))
            .at(149).put(";").put(_fmt_comma(row.get("CURBAL")))
            .at(169).put(";").put(_fmt_plain(row.get("RATE")))
            .at(184).put(";").put(_fmt_comma(row.get("BALANCE")))
            .line()
        )

        if is_last:
            output_lines.append(
                SasLine().at(1).put("SUBTOTAL")
                .at(15).put(";")
                .at(31).put(";")
                .at(72).put(";")
                .at(90).put(";")
                .at(105).put(";").put(_fmt_plain(fterm)).put(" M")
                .at(121).put(";").put(_fmt_int(frmaindt))
                .at(149).put(";").put(_fmt_comma(fcurbal))
                .at(169).put("; -")
                .at(184).put(";").put(_fmt_comma(fbalance))
                .line()
            )
            output_lines.append(SasLine().at(1).put("").line())

        prev_fundname = fundname

    output_lines.append(
        SasLine().at(1).put(f"SUBTOTAL ({sub_label})")
        .at(15).put(";")
        .at(31).put(";")
        .at(72).put(";")
        .at(90).put(";")
        .at(105).put(";").put(_fmt_plain(subtotal.get("TERM"))).put(" M")
        .at(121).put(";").put(_fmt_int(subtotal.get("RMAINDT")))
        .at(149).put(";").put(_fmt_comma(subtotal.get("CURBAL")))
        .at(169).put("; -")
        .at(184).put(";").put(_fmt_comma(subtotal.get("BALANCE")))
        .line()
    )
    output_lines.append(SasLine().at(1).put("").line())


_print_fd_section(ARFD1, sumarfd1, "C", "PUBLIC")
_print_fd_section(ARFD2, sumarfd2, "D", "NON-PUBLIC")

output_lines.append(
    SasLine().at(1).put("TOTAL (C)+(D)")
    .at(15).put(";")
    .at(31).put(";")
    .at(72).put(";")
    .at(90).put(";")
    .at(105).put(";").put(_fmt_plain(totarfd.get("TERM"))).put(" M")
    .at(121).put(";").put(_fmt_int(totarfd.get("RMAINDT")))
    .at(149).put(";").put(_fmt_comma(totarfd.get("CURBAL")))
    .at(169).put("; -")
    .at(184).put(";").put(_fmt_comma(totarfd.get("BALANCE")))
    .line()
)

output_lines.append(
    SasLine().at(1).put("GRAND TOTAL (A+B+C+D)")
    .at(72).put(";;")
    .at(90).put(";")
    .at(105).put(";").put(_fmt_plain(gtcafd["TERM"])).put(" M")
    .at(121).put(";").put(_fmt_int(gtcafd["RMAINDT"]))
    .at(149).put(";").put(_fmt_comma(gtcafd["CURBAL"]))
    .at(169).put("; -")
    .at(184).put(";").put(_fmt_comma(gtcafd["BALANCE"]))
    .line()
)

# ============================================================================
# WRITE OUTPUT
# ============================================================================
with open(OUTPUT_FILE, "w", encoding=FILE_ENCODING) as fh:
    for ln in output_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(output_lines):,}")
print("\n[RESULT] Report contents:\n")
for ln in output_lines:
    print(ln)

print("\nEIIDARTB complete.")
