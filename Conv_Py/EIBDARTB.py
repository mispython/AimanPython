#!/usr/bin/env python3
"""
Program : EIBDARTB.py
Purpose : Extract Fixed Deposits (FD) and Current Account (CA) balances for
          Amanah Raya Trustees Berhad (ARTB) on a daily basis, and produce
          the ARTB summary/detail report (Table 1 - Summary, Table 2 -
          Detailed Breakdown) equivalent to the SASLIST output of the
          original SAS program.
"""

import gc
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values
from input_date import get_latest_file, extract_key, SUPPORTED_EXTENSIONS
# from output_date import build_output_file
# -- Not used: the JCL SASLIST DD writes to a fixed PDS member name
#    (SAP.PBB.ARTB.DAILY, new generation "+1") with no date component in the
#    filename itself, so output_date.build_output_file (which stamps a date
#    suffix onto the output filename) does not apply here.

from PBBDPFMT import caprod_format
# -- Only CAPROD is used (PUT(PRODUCT, CAPROD.) in the original SAS). No
#    other PBBDPFMT formats (SADENOM/SAPROD/FDDENOM/FDPROD/etc.) appear in
#    this program's SAS source, so they are intentionally not imported here.
from ARTBFMT import get_fundmne, get_fundtype, PMFUND

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII/EIBDARTB")

INPUT_DIR  = BASE_DIR / "input" / "prod" / "deposit_fcy_d"
CACHE_DIR  = BASE_DIR / "input" / "cache" / "EIBDARTB"
OUTPUT_DIR = BASE_DIR / "output" / "EIBDARTB"

INPUT_CISDP_FILE  = STG_DIR / "CISDP" / "CISDP_deposit.sas7bdat"     # CISDP.DEPOSIT
INPUT_CISFD_FILE  = STG_DIR / "CISFD" / "CISFD_deposit.sas7bdat"     # CISFD.DEPOSIT
INPUT_DPCUST_FILE = STG_DIR / "DPCUST.TXT"                 # DPCUST DD (fixed-width flat file)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# OUTPUT_FILE = OUTPUT_DIR / f"EIBDARTB.txt"
# NOTE: SASLIST DD is RECFM=VB (variable, blocked) — NOT RECFM=VBA — so the
# original report carries no ASA carriage-control byte. Lines are written
# here as plain text, matching the actual DCB in the JCL.

# ============================================================================
# STEP 1: REPORT DATE  (no reptdate.parquet — derive from REPTDATE.py)
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values(year_format="%Y")   # CALL SYMPUT('REPTYEAR',PUT(REPTDATE,YEAR4.))
reptdate = reptdate_values.reptdate
REPTYEAR = reptdate_values.reptyear
REPTMON  = reptdate_values.reptmon
REPTDAY  = reptdate_values.reptday
TDATE    = reptdate.strftime("%d/%m/%Y")     # PUT(REPTDATE,DDMMYY10.)
RDATE    = reptdate                          # numeric SAS date equivalent (date object)

print(f"  Report date : {reptdate.isoformat()}  (TDATE={TDATE})")

# ============================================================================
# STEP 2: RESOLVE LATEST DATED INPUT FILES  (yymmdd suffix)
# ============================================================================
print("\nStep 2: Resolving dated input files...")


def _get_latest_excluding(directory: Path, prefix: str, exclude_prefix: str) -> Path:
    """Same resolution rule as input_date.get_latest_file, but excludes files
    that also satisfy a more specific overlapping prefix (e.g. prefix
    'dpd_fd' would also match 'dpd_fdcd' filenames)."""
    files = [
        f for f in directory.iterdir()
        if f.is_file()
        and f.suffix.lower() in SUPPORTED_EXTENSIONS
        and f.name.startswith(prefix)
        and not f.name.startswith(exclude_prefix)
    ]
    valid_files = [f for f in files if extract_key(f.name) is not None]
    if not valid_files:
        raise FileNotFoundError(
            f"No valid files found in {directory} with prefix '{prefix}' "
            f"(excluding '{exclude_prefix}')"
        )
    latest = max(valid_files, key=lambda f: extract_key(f.name))
    print(f"[FILE_RESOLVER] Selected latest: {latest.name}")
    return latest


ca_path   = get_latest_file(INPUT_DIR, prefix="ca")                                    # DEPOSIT.CURRENT            - /dwh/dpd_ca
# fd_path   = _get_latest_excluding(INPUT_DIR, prefix="dpd_fd", exclude_prefix="dpd_fdcd")   
fd_path   = get_latest_file(INPUT_DIR, prefix="fd")                                    # DEPOSIT.FD                 - /dwh/dpd_fd
fdcd_path = get_latest_file(INPUT_DIR, prefix="fdcd")                                  # FD.FD (detailed FD master) - /dwh/dpd_fdcd

print(f"  CA   : {ca_path.name}")
print(f"  FD   : {fd_path.name}")
print(f"  FDCD : {fdcd_path.name}")

# ============================================================================
# HELPER: CACHE STAMP  (skip re-conversion if .sas7bdat hasn't changed)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


# ============================================================================
# HELPER: STREAM .sas7bdat -> PARQUET  (memory-efficient chunked conversion)
# ============================================================================
def sas_to_parquet(sas_path: Path, cache_path: Path, tag: str, chunk_rows: int = 250_000) -> None:
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total  = 0

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


# ============================================================================
# STEP 3: CACHE SAS FILES TO PARQUET
# ============================================================================
print("\nStep 3: Caching SAS files to Parquet (if needed)...")

CA_CACHE    = CACHE_DIR / f"{ca_path.stem}.parquet"
FD_CACHE    = CACHE_DIR / f"{fd_path.stem}.parquet"
FDCD_CACHE  = CACHE_DIR / f"{fdcd_path.stem}.parquet"
CISDP_CACHE = CACHE_DIR / "cisdp_deposit.parquet"
CISFD_CACHE = CACHE_DIR / "cisfd_deposit.parquet"

for _src, _cache, _tag in (
    (ca_path,           CA_CACHE,    "CA"),
    (fd_path,           FD_CACHE,    "FD"),
    (fdcd_path,         FDCD_CACHE,  "FDCD"),
    (INPUT_CISDP_FILE,  CISDP_CACHE, "CISDP"),
    (INPUT_CISFD_FILE,  CISFD_CACHE, "CISFD"),
):
    if not _cache_is_fresh(_src, _cache):
        sas_to_parquet(_src, _cache, _tag)
    else:
        print(f"  [{_tag}] Cache fresh - skipping conversion.")

# ============================================================================
# STEP 4: BUILD CISCA  (CA customer / IC lookup)
# DATA CISCA(KEEP=CUSTNO ACCTNO CUSTNAME ICNO CISTYPE);
#   SET CISDP.DEPOSIT; IF SECCUST='901'; IF 3000000000<=ACCTNO<=3999999999;
# ============================================================================
print("\nStep 4: Building CISCA (CA customer lookup)...")

con = duckdb.connect(database=":memory:")
cisca = con.execute(f"""
    SELECT
        CAST(CUSTNO   AS BIGINT)  AS CUSTNO,
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(CUSTNAME AS VARCHAR) AS CUSTNAME,
        CASE
            WHEN NEWIC IS NOT NULL AND TRIM(CAST(NEWIC AS VARCHAR)) <> ''
                THEN CAST(NEWIC AS VARCHAR)
            ELSE CAST(OLDIC AS VARCHAR)
        END AS ICNO,
        'CA' AS CISTYPE
    FROM read_parquet('{CISDP_CACHE}')
    WHERE CAST(SECCUST AS VARCHAR) = '901'
      AND CAST(ACCTNO AS BIGINT) BETWEEN 3000000000 AND 3999999999
""").pl()
con.close()
print(f"  CISCA rows: {len(cisca):,}")

# ============================================================================
# STEP 5: BUILD CA  (current account, PRODCD via CAPROD format)
# DATA CA; SET DEPOSIT.CURRENT; PRODCD=PUT(PRODUCT,CAPROD.);
#   IF CURBAL > 0 AND PRODCD NE 'N';
# ============================================================================
print("\nStep 5: Building CA (current account)...")

ca = pl.read_parquet(CA_CACHE)
ca = ca.with_columns(
    pl.col("PRODUCT").cast(pl.Int64, strict=False)
      .map_elements(caprod_format, return_dtype=pl.Utf8)
      .alias("PRODCD")
)
ca = ca.filter((pl.col("CURBAL") > 0) & (pl.col("PRODCD") != "N"))
print(f"  CA rows (CURBAL>0, PRODCD<>'N'): {len(ca):,}")

# ============================================================================
# STEP 6: MERGE CA + CISCA
# MERGE CA(IN=A) CISCA; BY ACCTNO; IF CUSTNAME=' ' THEN CUSTNAME=NAME; IF A;
# BALANCE = SUM(CURBAL,INTPAYBL);
# ============================================================================
print("\nStep 6: Merging CA with CISCA...")

ca    = ca.with_columns(pl.col("ACCTNO").cast(pl.Int64))
cisca = cisca.with_columns(pl.col("ACCTNO").cast(pl.Int64))

ca = ca.join(
    cisca.select(["ACCTNO", "CUSTNAME", "ICNO"]).rename({"CUSTNAME": "CUSTNAME_CIS"}),
    on="ACCTNO", how="left",
)

# SAS MERGE CA(IN=A) CISCA — last-dataset-wins on CUSTNAME (CISCA's CUSTNAME
# overwrites CA's own CUSTNAME when matched). The subsequent SAS check
# "IF CUSTNAME = '   ' THEN CUSTNAME=NAME" references a variable NAME that is
# never assigned anywhere in the original program (absent from both source
# datasets), so SAS always evaluates it as missing/blank; that branch is a
# no-op in the original code and is reproduced as a no-op here.
ca = ca.with_columns(
    pl.when(pl.col("CUSTNAME_CIS").is_not_null())
      .then(pl.col("CUSTNAME_CIS"))
      .otherwise(pl.col("CUSTNAME"))
      .alias("CUSTNAME"),
    (pl.col("CURBAL").fill_null(0) + pl.col("INTPAYBL").fill_null(0)).alias("BALANCE"),
    pl.lit("CA").alias("CISTYPE"),
).drop("CUSTNAME_CIS")
print(f"  CA merged rows: {len(ca):,}")

# ============================================================================
# STEP 7: BUILD CISFD  (FD customer / IC lookup)
# ============================================================================
print("\nStep 7: Building CISFD (FD customer lookup)...")

con = duckdb.connect(database=":memory:")
cisfd = con.execute(f"""
    SELECT
        CAST(CUSTNO   AS BIGINT)  AS CUSTNO,
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(CUSTNAME AS VARCHAR) AS CUSTNAME,
        CASE
            WHEN NEWIC IS NOT NULL AND TRIM(CAST(NEWIC AS VARCHAR)) <> ''
                THEN CAST(NEWIC AS VARCHAR)
            ELSE CAST(OLDIC AS VARCHAR)
        END AS ICNO,
        'FD' AS CISTYPE
    FROM read_parquet('{CISFD_CACHE}')
    WHERE CAST(SECCUST AS VARCHAR) = '901'
      AND (
            (CAST(ACCTNO AS BIGINT) BETWEEN 1000000000 AND 1999999999)
         OR (CAST(ACCTNO AS BIGINT) BETWEEN 7000000000 AND 7999999999)
         OR (CAST(ACCTNO AS BIGINT) BETWEEN 4000000000 AND 6999999999)
      )
""").pl()
con.close()
print(f"  CISFD rows: {len(cisfd):,}")

# ============================================================================
# STEP 8: BUILD FD  (basic FD balances)  DATA FD; SET DEPOSIT.FD; IF CURBAL>0;
# ============================================================================
print("\nStep 8: Building FD (basic fixed deposit balances)...")

fd = pl.read_parquet(FD_CACHE).filter(pl.col("CURBAL") > 0)
print(f"  FD rows (CURBAL>0): {len(fd):,}")

# ============================================================================
# STEP 9: MERGE FD + CISFD  (same pattern as Step 6)
# ============================================================================
print("\nStep 9: Merging FD with CISFD...")

fd    = fd.with_columns(pl.col("ACCTNO").cast(pl.Int64))
cisfd = cisfd.with_columns(pl.col("ACCTNO").cast(pl.Int64))

fd = fd.join(
    cisfd.select(["ACCTNO", "CUSTNAME", "ICNO"]).rename({"CUSTNAME": "CUSTNAME_CIS"}),
    on="ACCTNO", how="left",
)

# Same NAME-is-undefined no-op note from Step 6 applies here.
fd = fd.with_columns(
    pl.when(pl.col("CUSTNAME_CIS").is_not_null())
      .then(pl.col("CUSTNAME_CIS"))
      .otherwise(pl.col("CUSTNAME"))
      .alias("CUSTNAME"),
    (pl.col("CURBAL").fill_null(0) + pl.col("INTPAYBL").fill_null(0)).alias("BALANCE"),
    pl.lit("FD").alias("CISTYPE"),
).drop("CUSTNAME_CIS")
print(f"  FD merged rows: {len(fd):,}")

# ============================================================================
# STEP 10: BUILD CAFD  (target-customer CA+FD combined dataset)
# DATA CAFD; SET CA(IN=A) FD(IN=B); IF CUSTNO IN (...);
#   IF A THEN CABAL=CURBAL; ELSE FDBAL=CURBAL; IF CURBAL > 0 THEN OUTPUT;
# ============================================================================
print("\nStep 10: Building CAFD (combined CA + FD for target customers)...")

TARGET_CUSTNOS = {
    3523050, 11335374, 11880426, 3728510,
    13158067, 14368177, 14368641, 14369065,
    14387105, 14932947, 14960789, 15254645,
    15241330, 15310964, 15352797,
}

ca_tagged = ca.with_columns(pl.lit(True).alias("_IS_CA"))
fd_tagged = fd.with_columns(pl.lit(False).alias("_IS_CA"))

cafd = pl.concat([ca_tagged, fd_tagged], how="diagonal_relaxed")
cafd = cafd.filter(pl.col("CUSTNO").cast(pl.Int64).is_in(TARGET_CUSTNOS))

cafd = cafd.with_columns(
    pl.when(pl.col("_IS_CA")).then(pl.col("CURBAL")).otherwise(None).alias("CABAL"),
    pl.when(pl.col("_IS_CA")).then(None).otherwise(pl.col("CURBAL")).alias("FDBAL"),
).filter(pl.col("CURBAL") > 0).drop("_IS_CA")
print(f"  CAFD rows: {len(cafd):,}")

# ============================================================================
# STEP 11: READ DPCUST FLAT FILE  (fixed-width text)
# INFILE DPCUST; INPUT @001 ACCTNO 11. @012 FUNDNAME $40.;
# ============================================================================
print("\nStep 11: Reading DPCUST flat file...")

_depo_rows = []
with open(INPUT_DPCUST_FILE, "rb") as fh:
    for raw in fh:
        line = raw.rstrip(b"\r\n")
        if len(line) < 51:
            continue
        acctno_str = line[0:11].decode("latin1").strip()
        fundname   = line[11:51].decode("latin1")
        if not acctno_str:
            continue
        _depo_rows.append({"ACCTNO": int(acctno_str), "FUNDNAME": fundname})

depo = pl.DataFrame(_depo_rows, schema={"ACCTNO": pl.Int64, "FUNDNAME": pl.Utf8})
depo = depo.with_columns(pl.col("FUNDNAME").str.replace_all(r"\s", "").alias("FNAMEX"))
print(f"  DEPO rows: {len(depo):,}")

# ============================================================================
# STEP 12: MERGE CAFD + DEPO, APPLY FUND FORMATS (BNM.CAFD)
# MERGE CAFD(IN=A) DEPO; BY ACCTNO; IF A;
# IF FUNDNAME=' ' THEN FUNDNAME=CUSTNAME;
# IF FNAMEX IN &PMFUNDX THEN PMTYP=1; ELSE PMTYP=2;
# PMTYPE=PUT(PMTYP,FUNDTYPE.); FUNDMNE=PUT(FNAMEX,$FUNDMNE.);
# ============================================================================
print("\nStep 12: Merging CAFD with DEPO and applying fund formats...")

# %LET PMFUND lookup compares against COMPRESS(FUNDNAME); the ARTBFMT
# $FUNDMNE keys are already the compressed (no-space) form of PMFUND entries,
# so membership is checked against the compressed PMFUND set directly.
PMFUND_COMPRESSED = {p.replace(" ", "") for p in PMFUND}

cafd = cafd.with_columns(pl.col("ACCTNO").cast(pl.Int64))
depo = depo.with_columns(pl.col("ACCTNO").cast(pl.Int64))

cafd = cafd.join(
    depo.rename({"FUNDNAME": "FUNDNAME_DEPO", "FNAMEX": "FNAMEX_DEPO"}),
    on="ACCTNO", how="left",
)

cafd = cafd.with_columns(
    pl.when(
        pl.col("FUNDNAME_DEPO").is_not_null() & (pl.col("FUNDNAME_DEPO").str.strip_chars() != "")
    ).then(pl.col("FUNDNAME_DEPO")).otherwise(pl.col("CUSTNAME")).alias("FUNDNAME"),
    pl.col("FNAMEX_DEPO").alias("FNAMEX"),   # stays null when no DEPO match, matching SAS
).drop(["FUNDNAME_DEPO", "FNAMEX_DEPO"])

cafd = cafd.with_columns(
    pl.when(pl.col("FNAMEX").is_not_null() & pl.col("FNAMEX").is_in(list(PMFUND_COMPRESSED)))
      .then(1).otherwise(2).alias("PMTYP")
).with_columns(
    pl.col("PMTYP").map_elements(get_fundtype, return_dtype=pl.Utf8).alias("PMTYPE"),
    pl.col("FNAMEX").map_elements(get_fundmne, return_dtype=pl.Utf8).alias("FUNDMNE"),
)

bnm_cafd = cafd
print(f"  BNM.CAFD rows: {len(bnm_cafd):,}")

# ============================================================================
# STEP 13: TABLE 1 SUMMARIES  (SUMCAFD by PMTYPE text label, TOTCAFD overall)
# ============================================================================
print("\nStep 13: Building Table 1 summaries...")

sumcafd = (
    bnm_cafd.select(["PMTYPE", "CABAL", "FDBAL", "CURBAL"])
    .group_by("PMTYPE")
    .agg([
        pl.col("CABAL").fill_null(0).sum().alias("CABAL"),
        pl.col("FDBAL").fill_null(0).sum().alias("FDBAL"),
        pl.col("CURBAL").fill_null(0).sum().alias("CURBAL"),
    ])
    .sort("PMTYPE")   # SAS BY PMTYPE sorts ascending on the formatted text label
)

totcafd_row = sumcafd.select([
    pl.col("CABAL").sum().alias("CABAL"),
    pl.col("FDBAL").sum().alias("FDBAL"),
    pl.col("CURBAL").sum().alias("CURBAL"),
]).row(0, named=True)

# ============================================================================
# STEP 14: SPLIT INTO ARCA (current account) / ARFD (fixed deposit)
# ============================================================================
print("\nStep 14: Splitting CAFD into ARCA / ARFD...")

arca = bnm_cafd.filter(pl.col("CISTYPE") == "CA")
arfd = bnm_cafd.filter(pl.col("CISTYPE") != "CA").select(
    ["ACCTNO", "CUSTNO", "CUSTNAME", "FUNDMNE", "FUNDNAME", "PMTYP"]
)

arca1 = arca.filter(pl.col("PMTYP") == 1)
arca2 = arca.filter(pl.col("PMTYP") != 1)


def _sum_cols(df: pl.DataFrame, cols: list) -> dict:
    if df.is_empty():
        return {c: 0.0 for c in cols}
    return df.select([pl.col(c).fill_null(0).sum().alias(c) for c in cols]).row(0, named=True)


sumarca1 = _sum_cols(arca1, ["CURBAL", "BALANCE"])
sumarca2 = _sum_cols(arca2, ["CURBAL", "BALANCE"])
totarca  = _sum_cols(arca,  ["CURBAL", "BALANCE"])

# ============================================================================
# STEP 15: MERGE ARFD WITH DETAILED FD MASTER (FD.FD / MNIFD)
# MERGE ARFD(IN=A) MNIFD(IN=B); BY ACCTNO; IF A AND B;
# ============================================================================
print("\nStep 15: Merging ARFD with detailed FD master (MNIFD)...")

mnifd = pl.read_parquet(FDCD_CACHE).select(
    ["ACCTNO", "MATID", "DEPODTE", "MATDATE", "TERM", "RATE", "INTPAY", "CURBAL"]
).with_columns(pl.col("ACCTNO").cast(pl.Int64))

arfd = arfd.with_columns(pl.col("ACCTNO").cast(pl.Int64))
arfd = arfd.join(mnifd, on="ACCTNO", how="inner")


def _decode_depodte(raw):
    """PUT(DEPODTE,Z11.) zero-pads to 11 digits; the first 8 characters are
    re-parsed under informat MMDDYY8. (assumed mmddyyyy, no separators)."""
    if raw is None:
        return None
    z11 = f"{int(raw):011d}"
    first8 = z11[:8]
    mm, dd, yyyy = int(first8[0:2]), int(first8[2:4]), int(first8[4:8])
    try:
        return date(yyyy, mm, dd)
    except ValueError:
        return None


def _decode_matdate(raw):
    """PUT(MATDATE,Z8.) zero-pads to 8 digits, re-parsed under informat
    YYMMDD8. (assumed yyyymmdd, no separators)."""
    if raw is None:
        return None
    z8 = f"{int(raw):08d}"
    yyyy, mm, dd = int(z8[0:4]), int(z8[4:6]), int(z8[6:8])
    try:
        return date(yyyy, mm, dd)
    except ValueError:
        return None


arfd = arfd.with_columns([
    pl.when(pl.col("MATID").is_in(["", "."]) | pl.col("MATID").is_null())
      .then(pl.lit("M")).otherwise(pl.col("MATID")).alias("MATID"),
    pl.col("DEPODTE").map_elements(_decode_depodte, return_dtype=pl.Date).alias("_DEPODTE_DT"),
    pl.col("MATDATE").map_elements(_decode_matdate, return_dtype=pl.Date).alias("_MATDATE_DT"),
])

arfd = arfd.with_columns([
    pl.col("_DEPODTE_DT").dt.strftime("%d/%m/%y").alias("DEPDTE"),
    pl.col("_MATDATE_DT").dt.strftime("%d/%m/%y").alias("MATDTE"),
    (pl.col("_MATDATE_DT") - pl.lit(reptdate)).dt.total_days().alias("RMAINDT"),
    (pl.col("CURBAL").fill_null(0) + pl.col("INTPAY").fill_null(0)).alias("BALANCE"),
]).drop(["_DEPODTE_DT", "_MATDATE_DT"])
print(f"  ARFD (detailed) rows: {len(arfd):,}")

arfd1 = arfd.filter(pl.col("PMTYP") == 1).sort("FUNDNAME")
arfd2 = arfd.filter(pl.col("PMTYP") != 1).sort("FUNDNAME")

sumarfd1 = _sum_cols(arfd1, ["TERM", "RMAINDT", "CURBAL", "BALANCE"])
sumarfd2 = _sum_cols(arfd2, ["TERM", "RMAINDT", "CURBAL", "BALANCE"])
totarfd  = _sum_cols(arfd,  ["TERM", "RMAINDT", "CURBAL", "BALANCE"])

# GTCAFD = SET TOTARCA TOTARFD; SUMMARY (no BY). TOTARCA has no TERM/RMAINDT
# fields (missing -> excluded from those sums by PROC SUMMARY), so TERM and
# RMAINDT come solely from TOTARFD, while CURBAL/BALANCE combine both.
gtcafd = {
    "TERM":     totarfd["TERM"],
    "RMAINDT":  totarfd["RMAINDT"],
    "CURBAL":   totarca["CURBAL"] + totarfd["CURBAL"],
    "BALANCE":  totarca["BALANCE"] + totarfd["BALANCE"],
}

# ============================================================================
# STEP 16: REPORT GENERATION  (semicolon-delimited, fixed SAS @col layout)
# ============================================================================
print("\nStep 16: Generating report...")


def _fmt_comma(value, decimals: int = 2) -> str:
    if value is None:
        return ""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return ""
    return f"{v:,.{decimals}f}"


def _fmt_num(value) -> str:
    if value is None:
        return ""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return str(value)
    return str(int(v)) if v == int(v) else str(v)


def _place(buf: list, col: int, text: str) -> None:
    """Overlay *text* into *buf* starting at 1-based column *col* (SAS @col)."""
    start = col - 1
    end = start + len(text)
    if end > len(buf):
        buf.extend([" "] * (end - len(buf)))
    buf[start:end] = list(text)


def _line(fields: list, width: int = 250) -> str:
    buf = [" "] * width
    for col, text in fields:
        _place(buf, col, text)
    return "".join(buf).rstrip()


output_lines: list = []

# ---- Table 1: Summary -----------------------------------------------------
output_lines.append(_line([(1, "P U B L I C   B A N K   B E R H A D")]))
output_lines.append(_line([(1, f"REPORT ID : EIBDARTB @ {TDATE}")]))
output_lines.append(_line([(1, "")]))
output_lines.append(_line([(1, "TABLE 1: SUMMARY TABLE FOR AMANAH RAYA GROUP")]))
output_lines.append(_line([
    (1, "PUBLIC BANK (PBB)"),
    (30, ";;;CURRENT ACCOUNTS"),
    (59, ";FIXED DEPOSITS"),
    (90, ";TOTAL (RM)"),
]))

for idx, row in enumerate(sumcafd.iter_rows(named=True), start=1):
    output_lines.append(_line([
        (1, f"{idx}) {row['PMTYPE']}"),
        (30, f";;;{_fmt_comma(row['CABAL'])}"),
        (59, f";{_fmt_comma(row['FDBAL'])}"),
        (90, f";{_fmt_comma(row['CURBAL'])}"),
    ]))

output_lines.append(_line([
    (1, "TOTAL"),
    (30, f";;;{_fmt_comma(totcafd_row['CABAL'])}"),
    (59, f";{_fmt_comma(totcafd_row['FDBAL'])}"),
    (90, f";{_fmt_comma(totcafd_row['CURBAL'])}"),
]))

# ---- Table 2: Detailed breakdown - I) Current Account ---------------------
output_lines.append(_line([(1, "")]))
output_lines.append(_line([(1, "TABLE 2: DETAILED BREAKDOWN LISTING FOR AMANAH RAYA GROUP")]))
output_lines.append(_line([(1, "I) CURRENT ACCOUNT")]))
output_lines.append(_line([
    (1, "CIS NO."),
    (15, ";CUST. MNEMONIC"),
    (31, ";CUSTOMER FULL NAME"),
    (149, ";CURRENT BALANCE"),
    (169, ";INTEREST (%)"),
    (184, ";BALANCE (CURRENT BALANCE + ACCRUED INTEREST)"),
]))


def _print_ca_section(df: pl.DataFrame, summary: dict, sub_letter: str) -> None:
    ftype = "PUBLIC" if sub_letter == "A" else "NON-PUBLIC"
    for i, row in enumerate(df.iter_rows(named=True)):
        if i == 0:
            output_lines.append(_line([(1, f"({sub_letter}) {ftype} MUTUAL FUND PORTFOLIO")]))
        output_lines.append(_line([
            (1, str(row["CUSTNO"])),
            (15, f";{row.get('FUNDMNE') or ''}"),
            (31, f";{row.get('FUNDNAME') or ''}"),
            (149, f";{_fmt_comma(row.get('CURBAL'))}"),
            (169, f";{_fmt_num(row.get('INTRATE'))}"),
            (184, f";{_fmt_comma(row.get('BALANCE'))}"),
        ]))
    output_lines.append(_line([(1, "")]))
    output_lines.append(_line([
        (1, f"SUBTOTAL ({sub_letter})"),
        (15, ";"),
        (31, ";"),
        (149, f";{_fmt_comma(summary.get('CURBAL'))}"),
        (169, "; -"),
        (184, f";{_fmt_comma(summary.get('BALANCE'))}"),
    ]))
    output_lines.append(_line([(1, "")]))


_print_ca_section(arca1, sumarca1, "A")
_print_ca_section(arca2, sumarca2, "B")

output_lines.append(_line([
    (1, "TOTAL (A)+(B)"),
    (15, ";"),
    (31, ";"),
    (149, f";{_fmt_comma(totarca.get('CURBAL'))}"),
    (169, "; -"),
    (184, f";{_fmt_comma(totarca.get('BALANCE'))}"),
]))

# ---- Table 2: Detailed breakdown - II) Fixed Deposits ----------------------
output_lines.append(_line([(1, "")]))
output_lines.append(_line([(1, "II) FIXED DEPOSITS")]))
output_lines.append(_line([
    (1, "CIS NO."),
    (15, ";CUST. MNEMONIC"),
    (31, ";CUSTOMER FULL NAME"),
    (72, ";TRANSACTION DATE"),
    (90, ";MATURITY DATE"),
    (105, ";ORIGINAL TENOR"),
    (121, ";REMAINING DAYS TO MATURITY"),
    (149, ";CURRENT BALANCE"),
    (169, ";INTEREST (%)"),
    (184, ";BALANCE (CURRENT BALANCE + ACCRUED INTEREST)"),
]))


def _print_fd_section(df: pl.DataFrame, summary: dict, sub_letter: str) -> None:
    ftype = "PUBLIC" if sub_letter == "C" else "NON-PUBLIC"
    rows = list(df.iter_rows(named=True))
    n = len(rows)
    fterm = frmaindt = fcurbal = fbalance = 0.0
    prev_fund = None

    for i, row in enumerate(rows):
        fund = row["FUNDNAME"]
        if fund != prev_fund:
            fterm = frmaindt = fcurbal = fbalance = 0.0
        fterm    += row.get("TERM") or 0
        frmaindt += row.get("RMAINDT") or 0
        fcurbal  += row.get("CURBAL") or 0
        fbalance += row.get("BALANCE") or 0

        if i == 0:
            output_lines.append(_line([(1, f"({sub_letter}) {ftype} MUTUAL FUND PORTFOLIO")]))

        output_lines.append(_line([
            (1, str(row["CUSTNO"])),
            (15, f";{row.get('FUNDMNE') or ''}"),
            (31, f";{row.get('FUNDNAME') or ''}"),
            (72, f";{row.get('DEPDTE') or ''}"),
            (90, f";{row.get('MATDTE') or ''}"),
            (105, f";{_fmt_num(row.get('TERM'))}"),
            (111, str(row.get("MATID") or "")),
            (121, f";{_fmt_num(row.get('RMAINDT'))}"),
            (149, f";{_fmt_comma(row.get('CURBAL'))}"),
            (169, f";{_fmt_num(row.get('RATE'))}"),
            (184, f";{_fmt_comma(row.get('BALANCE'))}"),
        ]))

        next_fund = rows[i + 1]["FUNDNAME"] if i + 1 < n else None
        if next_fund != fund:
            output_lines.append(_line([
                (1, "SUBTOTAL"),
                (15, ";"),
                (31, ";"),
                (72, ";"),
                (90, ";"),
                (105, f";{_fmt_num(fterm)} M"),
                (121, f";{_fmt_num(frmaindt)}"),
                (149, f";{_fmt_comma(fcurbal)}"),
                (169, "; -"),
                (184, f";{_fmt_comma(fbalance)}"),
            ]))
            output_lines.append(_line([(1, "")]))
        prev_fund = fund

    output_lines.append(_line([
        (1, f"SUBTOTAL ({sub_letter})"),
        (15, ";"),
        (31, ";"),
        (72, ";"),
        (90, ";"),
        (105, f";{_fmt_num(summary.get('TERM'))} M"),
        (121, f";{_fmt_num(summary.get('RMAINDT'))}"),
        (149, f";{_fmt_comma(summary.get('CURBAL'))}"),
        (169, "; -"),
        (184, f";{_fmt_comma(summary.get('BALANCE'))}"),
    ]))
    output_lines.append(_line([(1, "")]))


_print_fd_section(arfd1, sumarfd1, "C")
_print_fd_section(arfd2, sumarfd2, "D")

output_lines.append(_line([
    (1, "TOTAL (C)+(D)"),
    (15, ";"),
    (31, ";"),
    (72, ";"),
    (90, ";"),
    (105, f";{_fmt_num(totarfd.get('TERM'))} M"),
    (121, f";{_fmt_num(totarfd.get('RMAINDT'))}"),
    (149, f";{_fmt_comma(totarfd.get('CURBAL'))}"),
    (169, "; -"),
    (184, f";{_fmt_comma(totarfd.get('BALANCE'))}"),
]))

output_lines.append(_line([
    (1, "GRAND TOTAL (A+B+C+D)"),
    (72, ";;"),
    (90, ";"),
    (105, f";{_fmt_num(gtcafd.get('TERM'))} M"),
    (121, f";{_fmt_num(gtcafd.get('RMAINDT'))}"),
    (149, f";{_fmt_comma(gtcafd.get('CURBAL'))}"),
    (169, "; -"),
    (184, f";{_fmt_comma(gtcafd.get('BALANCE'))}"),
]))

# ============================================================================
# WRITE OUTPUT
# ============================================================================
OUTPUT_FILE = OUTPUT_DIR / f"EIBDARTB_{REPTDAY}{REPTMON}{REPTYEAR}.txt"

with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in output_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(output_lines):,}")

print("\n--- Report Preview ---")
for ln in output_lines:
    print(ln)

del ca, fd, cafd, bnm_cafd, arca, arfd, mnifd
gc.collect()

print("\nEIBDARTB complete.")
