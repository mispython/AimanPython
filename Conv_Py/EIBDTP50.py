#!/usr/bin/env python3
"""
Program  : EIBDTP50.py
Purpose  : Produce reports - Top 50 Depositor (Daily RM & FCY).
           Generates three reports:
             FD11TEXT  - Top 100 Largest FD/CA/SA Individual Customers
             FD12TEXT  - Top 100 Largest FD/CA/SA Corporate Customers
             FD2TEXT   - Group of Companies Under Top 100 Corp Depositors

Activated by EIBDTP5J.py
"""

import os
import gc
import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from REPTDATE import get_reptdate_values
from input_date import get_latest_file
from output_date import build_output_file

# Dependency: PBBDPFMT (shared format module, already converted separately).
# Only caprod_format() and saprod_format() are imported because the SAS body
# only contains PUT(PRODUCT,CAPROD.) and PUT(PRODUCT,SAPROD.) calls. FDPROD/
# FDDENOM etc. are NOT imported — no PUT(var,FDPROD.) call exists in this program.
from PBBDPFMT import caprod_format, saprod_format

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

# DEPOSIT DD -> dpd_ca / dpd_fd / dpd_sa (dated filenames, latest per run)
INPUT_DEPOSIT_DIR = BASE_DIR / "input" / "prod" / "deposit_d"

# CISDP DD / CISFD DD -> fixed filenames, no date component
INPUT_CIS_DIR    = Path("/stgsrcsys/host/uat/AII/EIBDARTB")
INPUT_CISDP_FILE = INPUT_CIS_DIR / "CISDP" / "CISDP_deposit.sas7bdat"
INPUT_CISFD_FILE = INPUT_CIS_DIR / "CISFD" / "CISFD_deposit.sas7bdat"

# LIST DD -> fixed filenames, no date component
INPUT_LIST_DIR      = Path("/stgsrcsys/host/uat/AII/EIBDTP50")
INPUT_MNI_LIST_FILE = INPUT_LIST_DIR / "cof_mni_depositor_list.sas7bdat" 
INPUT_TOPDEP_FILE   = INPUT_LIST_DIR / "keep_top_dep_excl_pbb.sas7bdat" 

# Parquet cache directory
CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBDTP50"

# Output directory (fixed filenames -- see note above re: GDG generations)
OUTPUT_DIR = BASE_DIR / "output" / "EIBDTP5J"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FD11 = build_output_file(OUTPUT_DIR, "INDVTP50", date_format="ddmmyy").with_suffix(".txt")   # FD11TEXT (Individual)
OUTPUT_FD12 = build_output_file(OUTPUT_DIR, "CORPTP50", date_format="ddmmyy").with_suffix(".txt")   # FD12TEXT (Corporate)
OUTPUT_FD2  = build_output_file(OUTPUT_DIR, "SUBSTP50", date_format="ddmmyy").with_suffix(".txt")   # FD2TEXT  (Subsidiaries)

# ============================================================================
# CHUNK SIZE FOR STREAMING LARGE .sas7bdat FILES
# ============================================================================
CHUNK_ROWS = 500_000
ROW_LIMIT  = int(os.environ.get("ROW_LIMIT", 0))   # 0 = no limit (test mode via env)

# ============================================================================
# REPORT PAGE CONFIGURATION
# ============================================================================
# JCL DCB=(RECFM=FB,LRECL=133,BLKSIZE=0) -- RECFM is FB, NOT FBA, therefore
# there is NO ASA carriage-control byte reserved in column 1. Page breaks are
# emitted as a literal form-feed character on its own line instead.
PAGE_SIZE    = 60
LRECL        = 133

# ============================================================================
# STEP 1: REPORT DATE  (no reptdate.parquet — derive from REPTDATE.py)
# CALL SYMPUT('REPTYEAR',PUT(REPTDATE,YEAR4.));  -> 4-digit year
# CALL SYMPUT('RDATE',PUT(REPTDATE,DDMMYY8.));
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values(year_format="%Y")
reptdate = reptdate_values.reptdate

RDATE    = reptdate.strftime("%d/%m/%y")   # DDMMYY8.
REPTYEAR = reptdate_values.reptyear
REPTMON  = reptdate_values.reptmon
REPTDAY  = reptdate_values.reptday

# SELECT(DAY(REPTDATE)); WHEN(8)->1 WHEN(15)->2 WHEN(22)->3 OTHERWISE->4
# (exact-day match, NOT a range test). NOWK is computed for fidelity but is
# not referenced anywhere else in the original SAS program body.
_day = reptdate.day
if _day == 8:
    NOWK = "1"
elif _day == 15:
    NOWK = "2"
elif _day == 22:
    NOWK = "3"
else:
    NOWK = "4"

print(f"  Report date : {RDATE}")
print(f"  REPTYEAR    : {REPTYEAR}  REPTMON: {REPTMON}  REPTDAY: {REPTDAY}  NOWK: {NOWK}")

# ============================================================================
# STEP 2: RESOLVE LATEST DEPOSIT INPUT FILES
# ============================================================================
print("\nStep 2: Resolving latest ca / fd / sa file names...")

ca_path = get_latest_file(INPUT_DEPOSIT_DIR, prefix="ca")
fd_path = get_latest_file(INPUT_DEPOSIT_DIR, prefix="fd")
sa_path = get_latest_file(INPUT_DEPOSIT_DIR, prefix="sa")

print(f"  CA (CURRENT) : {ca_path.name}")
print(f"  FD           : {fd_path.name}")
print(f"  SA (SAVING)  : {sa_path.name}")

# ============================================================================
# HELPER: CACHE STAMP  (skip re-conversion if .sas7bdat hasn't changed)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    """Return True when the Parquet cache is newer than the source SAS file."""
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )

# ============================================================================
# HELPER: STREAM .sas7bdat -> PARQUET  (memory-efficient chunked conversion)
# ============================================================================
def sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    """Convert a large .sas7bdat to Parquet in streaming chunks."""
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer    = None
    schema    = None
    total     = 0
    rows_read = 0

    reader = pd.read_sas(
        sas_path,
        encoding="latin1",
        chunksize=CHUNK_ROWS,
    )
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
                        print(f"  [{tag}] WARNING: Cannot cast '{field.name}' "
                              f"from {col.type} to {field.type}: {e} — filling nulls")
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

# ============================================================================
# STEP 3: CACHE ALL SAS FILES TO PARQUET
# ============================================================================
print("\nStep 3: Caching SAS files to Parquet (if needed)...")

CA_CACHE       = CACHE_DIR / f"{ca_path.stem}.parquet"
FD_CACHE       = CACHE_DIR / f"{fd_path.stem}.parquet"
SA_CACHE       = CACHE_DIR / f"{sa_path.stem}.parquet"
CISDP_CACHE    = CACHE_DIR / "cisdp.parquet"
CISFD_CACHE    = CACHE_DIR / "cisfd.parquet"
MNILIST_CACHE  = CACHE_DIR / "cof_mni_depositor_list.parquet"
TOPDEP_CACHE   = CACHE_DIR / "keep_top_dep_excl_pbb.parquet"

for src, cache, tag in (
    (ca_path, CA_CACHE, "CA"),
    (fd_path, FD_CACHE, "FD"),
    (sa_path, SA_CACHE, "SA"),
    (INPUT_CISDP_FILE, CISDP_CACHE, "CISDP"),
    (INPUT_CISFD_FILE, CISFD_CACHE, "CISFD"),
    (INPUT_MNI_LIST_FILE, MNILIST_CACHE, "MNILIST"),
    (INPUT_TOPDEP_FILE, TOPDEP_CACHE, "TOPDEP"),
):
    if not _cache_is_fresh(src, cache):
        sas_to_parquet(src, cache, tag)
    else:
        print(f"  [{tag}] Cache fresh — skipping conversion.")

# ============================================================================
# STEP 4: BUILD CISCA / CISFD  (customer master lookups)
# DATA CISCA(KEEP=CUSTNO ACCTNO CUSTNAME ICNO NEWIC OLDIC INDORG);
#    SET CISDP.DEPOSIT; IF SECCUST='901';
#    IF (3000000000<=ACCTNO<=3999999999);
#    IF NEWIC NE '' THEN ICNO=NEWIC; ELSE ICNO=CUSTNO;
# DATA CISFD(...): SET CISFD.DEPOSIT; IF SECCUST='901';
#    IF (1000000000<=ACCTNO<=1999999999) OR (7000000000<=ACCTNO<=7999999999)
#       OR (4000000000<=ACCTNO<=6999999999);
# ============================================================================
print("\nStep 4: Building CISCA / CISFD customer lookups...")

con = duckdb.connect(database=":memory:")

cisca = con.execute(f"""
    SELECT
        CAST(CUSTNO   AS BIGINT)  AS CUSTNO,
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(CUSTNAME AS VARCHAR) AS CUSTNAME,
        CASE
            WHEN NEWIC IS NOT NULL AND TRIM(CAST(NEWIC AS VARCHAR)) <> ''
                THEN TRIM(CAST(NEWIC AS VARCHAR))
            ELSE CAST(CUSTNO AS VARCHAR)
        END AS ICNO,
        CAST(NEWIC  AS VARCHAR) AS NEWIC,
        CAST(OLDIC  AS VARCHAR) AS OLDIC,
        CAST(INDORG AS VARCHAR) AS INDORG
    FROM read_parquet('{CISDP_CACHE}')
    WHERE SECCUST = '901'
      AND CAST(ACCTNO AS BIGINT) BETWEEN 3000000000 AND 3999999999
""").pl()

cisfd = con.execute(f"""
    SELECT
        CAST(CUSTNO   AS BIGINT)  AS CUSTNO,
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(CUSTNAME AS VARCHAR) AS CUSTNAME,
        CASE
            WHEN NEWIC IS NOT NULL AND TRIM(CAST(NEWIC AS VARCHAR)) <> ''
                THEN TRIM(CAST(NEWIC AS VARCHAR))
            ELSE CAST(CUSTNO AS VARCHAR)
        END AS ICNO,
        CAST(NEWIC  AS VARCHAR) AS NEWIC,
        CAST(OLDIC  AS VARCHAR) AS OLDIC,
        CAST(INDORG AS VARCHAR) AS INDORG
    FROM read_parquet('{CISFD_CACHE}')
    WHERE SECCUST = '901'
      AND (
            CAST(ACCTNO AS BIGINT) BETWEEN 1000000000 AND 1999999999
         OR CAST(ACCTNO AS BIGINT) BETWEEN 7000000000 AND 7999999999
         OR CAST(ACCTNO AS BIGINT) BETWEEN 4000000000 AND 6999999999
      )
""").pl()

con.close()
gc.collect()
print(f"  CISCA rows: {len(cisca):,}   CISFD rows: {len(cisfd):,}")

# ============================================================================
# STEP 5: BUILD CA / FD / SA  (account-level datasets, filtered)
# DATA CA;  SET DEPOSIT.CURRENT; PRODCD=PUT(PRODUCT,CAPROD.);
#           IF CURBAL>0 AND PRODCD NE 'N';
# DATA FD;  SET DEPOSIT.FD;      IF CURBAL>0;
# DATA SA;  SET DEPOSIT.SAVING;  PRODCD=PUT(PRODUCT,SAPROD.);
#           IF CURBAL>0 AND PRODCD NE 'N';
# ============================================================================
print("\nStep 5: Building CA / FD / SA datasets (product format + filter)...")

con = duckdb.connect(database=":memory:")
ca_raw = con.execute(f"SELECT * FROM read_parquet('{CA_CACHE}') WHERE CAST(CURBAL AS DOUBLE) > 0").pl()
fd_raw = con.execute(f"SELECT * FROM read_parquet('{FD_CACHE}') WHERE CAST(CURBAL AS DOUBLE) > 0").pl()
sa_raw = con.execute(f"SELECT * FROM read_parquet('{SA_CACHE}') WHERE CAST(CURBAL AS DOUBLE) > 0").pl()
con.close()
gc.collect()

ca = ca_raw.with_columns(
    pl.col("PRODUCT").cast(pl.Int64).map_elements(caprod_format, return_dtype=pl.Utf8).alias("PRODCD")
).filter(pl.col("PRODCD") != "N")

sa = sa_raw.with_columns(
    pl.col("PRODUCT").cast(pl.Int64).map_elements(saprod_format, return_dtype=pl.Utf8).alias("PRODCD")
).filter(pl.col("PRODCD") != "N")

fd = fd_raw

del ca_raw, fd_raw, sa_raw
gc.collect()
print(f"  CA rows: {len(ca):,}   FD rows: {len(fd):,}   SA rows: {len(sa):,}")

# ============================================================================
# STEP 6: MERGE CA/FD WITH CISCA, SA WITH CISFD
# PROC SORT DATA=CISCA; BY ACCTNO;  PROC SORT DATA=CA; BY ACCTNO;
# DATA CA; MERGE CA(IN=A) CISCA; BY ACCTNO;
#    IF CUSTNAME='   ' THEN CUSTNAME=NAME;  IF A;
# (Analogous logic for FD/SA with CISFD.)
# ============================================================================
print("\nStep 6: Merging account data with customer lookups...")

def _merge_with_cis(acct_df: pl.DataFrame, cis_df: pl.DataFrame) -> pl.DataFrame:
    """SAS MERGE acct(IN=A) cis; BY ACCTNO; IF CUSTNAME blank THEN CUSTNAME=NAME; IF A."""
    acct_pd = acct_df.to_pandas()
    cis_pd = cis_df.select(["ACCTNO", "CUSTNAME", "ICNO", "NEWIC", "OLDIC", "INDORG"]).to_pandas()
    cis_pd = cis_pd.rename(columns={"CUSTNAME": "_CIS_CUSTNAME"})

    merged = acct_pd.merge(cis_pd, on="ACCTNO", how="left")

    # SASC/CIS CUSTNAME wins; fall back to the account's own embedded NAME
    # field only when CIS did not supply a CUSTNAME (matches: SAS MERGE
    # last-dataset-wins semantics with IF CUSTNAME='   ' THEN CUSTNAME=NAME).
    if "_CIS_CUSTNAME" in merged.columns:
        merged["CUSTNAME"] = merged["_CIS_CUSTNAME"].where(
            merged["_CIS_CUSTNAME"].notna() & (merged["_CIS_CUSTNAME"].str.strip() != ""),
            merged.get("NAME"),
        )
        merged.drop(columns=["_CIS_CUSTNAME"], inplace=True)

    return pl.from_pandas(merged)

ca = _merge_with_cis(ca, cisca)
fd = _merge_with_cis(fd, cisfd)
sa = _merge_with_cis(sa, cisfd)

print(f"  CA merged rows: {len(ca):,}   FD merged rows: {len(fd):,}   SA merged rows: {len(sa):,}")

# ============================================================================
# STEP 7: SPLIT INTO IND / ORG  (CUSTCODE / PURPOSE / INDORG rules)
# DATA xxIND xxORG; SET xx;
#    xxBAL = CURBAL;
#    IF CUSTCODE IN (77,78,95,96) THEN OUTPUT xxIND;
#    ELSE DO;
#       IF PURPOSE='2' THEN DELETE;
#       IF INDORG='O' THEN OUTPUT xxORG;
#    END;
# DATA xxIND; SET xxIND; IF PURPOSE='2' THEN DO; ICNO='JOINT'; CUSTNAME=NAME; END;
# PROC SORT DATA=xxIND OUT=xxIND NODUPKEY; BY ACCTNO ICNO CUSTNAME;
# ============================================================================
print("\nStep 7: Splitting CA/FD/SA into IND / ORG branches...")

def _split_ind_org(df: pl.DataFrame, bal_col: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Replicate the CUSTCODE/PURPOSE/INDORG OUTPUT branching + JOINT relabel."""
    df = df.with_columns(pl.col("CURBAL").alias(bal_col))

    custcode_set = [77, 78, 95, 96]
    is_special_custcode = pl.col("CUSTCODE").cast(pl.Int64, strict=False).is_in(custcode_set)

    ind_df = df.filter(is_special_custcode)
    remainder = df.filter(~is_special_custcode)

    # ELSE DO: IF PURPOSE='2' THEN DELETE;  (drop before INDORG check)
    remainder = remainder.filter(pl.col("PURPOSE") != "2")
    org_df = remainder.filter(pl.col("INDORG") == "O")

    # DATA xxIND; SET xxIND; IF PURPOSE='2' THEN DO ICNO='JOINT'; CUSTNAME=NAME; END;
    ind_df = ind_df.with_columns([
        pl.when(pl.col("PURPOSE") == "2").then(pl.lit("JOINT")).otherwise(pl.col("ICNO")).alias("ICNO"),
        pl.when(pl.col("PURPOSE") == "2").then(pl.col("NAME")).otherwise(pl.col("CUSTNAME")).alias("CUSTNAME"),
    ])

    # PROC SORT NODUPKEY BY ACCTNO ICNO CUSTNAME
    ind_df = ind_df.sort(["ACCTNO", "ICNO", "CUSTNAME"]).unique(
        subset=["ACCTNO", "ICNO", "CUSTNAME"], keep="first"
    )

    return ind_df, org_df

caind, caorg = _split_ind_org(ca, "CABAL")
fdind, fdorg = _split_ind_org(fd, "FDBAL")
said, saorg  = _split_ind_org(sa, "SABAL")
# (variable named 'said' to avoid clashing with builtin-ish 'saind'; kept
#  distinct from function name only, logically this IS SAIND)
saind = said

del ca, fd, sa
gc.collect()

print(f"  CAIND:{len(caind):,} CAORG:{len(caorg):,}  "
      f"FDIND:{len(fdind):,} FDORG:{len(fdorg):,}  "
      f"SAIND:{len(saind):,} SAORG:{len(saorg):,}")

# ============================================================================
# REPORT HELPERS
# ============================================================================
def _fmt_comma(value, width: int, decimals: int = 2) -> str:
    """Format number with comma separators, right-justified to *width*."""
    if value is None:
        return " " * width
    try:
        v = float(value)
    except (TypeError, ValueError):
        return " " * width
    s = f"{v:,.{decimals}f}"
    return s.rjust(width)


class _PageWriter:
    """Accumulates report lines with PAGE_SIZE-line pagination.

    RECFM=FB (no trailing 'A') -> no ASA carriage-control byte is reserved.
    A literal form-feed character on its own line marks a new page instead.
    """

    def __init__(self, header_fn):
        self.header_fn = header_fn
        self.lines: list[str] = []
        self.lines_on_page = 0
        self.first_page = True

    def new_page(self, *header_args):
        if not self.first_page:
            self.lines.append("\f")
        self.first_page = False
        header_lines = self.header_fn(*header_args)
        self.lines.extend(header_lines)
        self.lines_on_page = len(header_lines)

    def add_line(self, line: str):
        if self.lines_on_page >= PAGE_SIZE:
            # Re-issue last used header args is not tracked here; caller is
            # expected to call new_page() explicitly on overflow when the
            # header differs by BY-group. For plain continuation, blank header.
            self.lines.append("\f")
            self.lines_on_page = 0
        self.lines.append(line)
        self.lines_on_page += 1

    def write(self, path: Path):
        with open(path, "w", encoding="latin1") as fh:
            for ln in self.lines:
                fh.write(ln.ljust(LRECL if ln != "\f" else 1) + "\n")


def _summary_header(title2: str) -> list[str]:
    return [
        "PUBLIC BANK BERHAD      PROGRAM-ID: EIBDTP50",
        title2 + f" {RDATE}",
        "-" * LRECL,
        f"{'DEPOSITOR':<30}{'TOTAL BALANCE':>18}{'FD BALANCE':>18}{'CA BALANCE':>18}{'SA BALANCE':>18}",
        "-" * LRECL,
    ]


def _detail_header(title2: str) -> list[str]:
    return [
        "PUBLIC BANK BERHAD      PROGRAM-ID: EIBDTP50",
        title2 + f" {RDATE}",
        "-" * LRECL,
        f"{'BRH':<7}{'MNI NO':>12}{'CUSTCD':>8}{'DEPOSITOR':<30}{'CIS NO':>12}"
        f"{'NEW IC':>14}{'OLD IC':>14}{'CURRENT BALANCE':>18}{'PRODUCT':>8}",
        "-" * LRECL,
    ]


def _subs_header(title3: str) -> list[str]:
    return [
        "PUBLIC BANK BERHAD      PROGRAM-ID: EIBDTOP5",
        f"GROUP OF COMPANIES UNDER TOP 100 CORP DEPOSITORS @ {RDATE}",
        f"***** {title3} *****",
        "-" * LRECL,
        f"{'BRH':<7}{'MNI NO':>12}{'DEPOSITOR':<30}{'CIS NO':>12}{'CUSTCD':>8}"
        f"{'CURRENT BALANCE':>18}{'PRODUCT':>8}",
        "-" * LRECL,
    ]

# ============================================================================
# STEP 8: %MACRO PRNREC  (Top 100 summary + detail, run for IND and ORG)
# PROC SUMMARY BY ICNO CUSTNAME VAR CURBAL FDBAL CABAL SABAL SUM=;
# PROC SORT DESCENDING CURBAL; OBS=100;
# PROC PRINT DATA2 (summary Top 100); then join back to DATA1 -> PROC PRINT
# DATA3 BY ICNO CUSTNAME SUM CURBAL;
# ============================================================================
def _run_prnrec(fdind_df, caind_df, saind_df, title2_summary, title2_detail, output_path: Path):
    print(f"\n  Generating {output_path.name} ...")

    data1 = pl.concat(
        [fdind_df, caind_df, saind_df], how="diagonal"
    ).with_columns(
        pl.when((pl.col("ICNO").is_null()) | (pl.col("ICNO").str.strip_chars() == ""))
        .then(pl.lit("XX"))
        .otherwise(pl.col("ICNO"))
        .alias("ICNO")
    )

    for col in ("CURBAL", "FDBAL", "CABAL", "SABAL"):
        if col not in data1.columns:
            data1 = data1.with_columns(pl.lit(0.0).alias(col))
        else:
            data1 = data1.with_columns(pl.col(col).fill_null(0.0))

    # PROC SUMMARY BY ICNO CUSTNAME; VAR CURBAL FDBAL CABAL SABAL; SUM=;
    data2 = (
        data1.filter(pl.col("ICNO").is_not_null())
        .group_by(["ICNO", "CUSTNAME"])
        .agg([
            pl.col("CURBAL").sum().alias("CURBAL"),
            pl.col("FDBAL").sum().alias("FDBAL"),
            pl.col("CABAL").sum().alias("CABAL"),
            pl.col("SABAL").sum().alias("SABAL"),
        ])
    )

    # PROC SORT DESCENDING CURBAL; DATA2(OBS=100);
    data2 = data2.sort("CURBAL", descending=True).head(100)

    # ---- Print Top 100 summary ----
    writer = _PageWriter(_summary_header)
    writer.new_page(title2_summary)
    for row in data2.iter_rows(named=True):
        line = (
            f"{str(row.get('CUSTNAME') or '')[:30]:<30}"
            f"{_fmt_comma(row.get('CURBAL'), 18)}"
            f"{_fmt_comma(row.get('FDBAL'), 18)}"
            f"{_fmt_comma(row.get('CABAL'), 18)}"
            f"{_fmt_comma(row.get('SABAL'), 18)}"
        )
        writer.add_line(line)

    # ---- DATA3 = MERGE DATA1(IN=A) DATA2(IN=B); BY ICNO CUSTNAME; IF A AND B; ----
    top_keys = data2.select(["ICNO", "CUSTNAME"])
    data3 = data1.join(top_keys, on=["ICNO", "CUSTNAME"], how="inner")
    data3 = data3.sort(["ICNO", "CUSTNAME"])

    writer.new_page(title2_detail)
    current_group = None
    group_sum = 0.0
    for row in data3.iter_rows(named=True):
        group_key = (row.get("ICNO"), row.get("CUSTNAME"))
        if current_group is not None and group_key != current_group:
            writer.add_line(f"{'':<49}{'TOTAL':>12}{_fmt_comma(group_sum, 18)}")
            group_sum = 0.0
        current_group = group_key

        branch_str = str(int(row.get("BRANCH") or 0)).rjust(6) if row.get("BRANCH") is not None else " " * 6
        acctno_str = f"{int(row.get('ACCTNO') or 0):>12d}"
        custcode_str = str(row.get("CUSTCODE") or "")[:8].rjust(8)
        custname_str = str(row.get("CUSTNAME") or "")[:30]
        custno_str = f"{int(row.get('CUSTNO') or 0):>12d}" if row.get("CUSTNO") is not None else " " * 12
        newic_str = str(row.get("NEWIC") or "")[:14].rjust(14)
        oldic_str = str(row.get("OLDIC") or "")[:14].rjust(14)
        curbal_str = _fmt_comma(row.get("CURBAL"), 18)
        product_str = str(row.get("PRODUCT") or "")[:8].rjust(8)

        line = (
            f"{branch_str:<7}{acctno_str}{custcode_str}{custname_str:<30}"
            f"{custno_str}{newic_str}{oldic_str}{curbal_str}{product_str}"
        )
        writer.add_line(line)
        group_sum += float(row.get("CURBAL") or 0.0)

    if current_group is not None:
        writer.add_line(f"{'':<49}{'TOTAL':>12}{_fmt_comma(group_sum, 18)}")

    writer.write(output_path)
    print(f"  Output written : {output_path}  ({len(writer.lines):,} lines)")
    return data1, data3


# ============================================================================
# STEP 9: FD11TEXT / FD12TEXT  (Individual / Corporate customers)
# ============================================================================
print("\nStep 9: Generating FD11TEXT (Individual) and FD12TEXT (Corporate)...")

data1_ind, data3_ind = _run_prnrec(
    fdind, caind, saind,
    "TOP 100 LARGEST FD/CA/SA INDIVIDUAL CUSTOMERS AS AT",
    "TOP 100 LARGEST FD/CA/SA INDIVIDUAL CUSTOMERS AS AT",
    OUTPUT_FD11,
)

data1_org, data3_org = _run_prnrec(
    fdorg, caorg, saorg,
    "TOP 100 LARGEST FD/CA/SA CORPORATE CUSTOMERS AS AT",
    "TOP 100 LARGEST FD/CA/SA CORPORATE CUSTOMERS AS AT",
    OUTPUT_FD12,
)

# ============================================================================
# STEP 10: SUBSIDIARIES REPORT (FD2TEXT)
# DATA SUBS_ALL; SET FDORG CAORG SAORG;
#    IF NEWIC NE '' THEN ICNO=NEWIC; ELSE ICNO=OLDIC;
#    IF CURCODE EQ 'MYR' THEN RMAMT=CURBAL; ELSE FCYAMT=CURBAL;
# ============================================================================
print("\nStep 10: Building SUBS_ALL and generating FD2TEXT (Subsidiaries)...")

subs_all = pl.concat([fdorg, caorg, saorg], how="diagonal").with_columns([
    pl.when((pl.col("NEWIC").is_not_null()) & (pl.col("NEWIC").str.strip_chars() != ""))
      .then(pl.col("NEWIC"))
      .otherwise(pl.col("OLDIC"))
      .alias("ICNO"),
    pl.when(pl.col("CURCODE") == "MYR").then(pl.col("CURBAL")).otherwise(None).alias("RMAMT"),
    pl.when(pl.col("CURCODE") != "MYR").then(pl.col("CURBAL")).otherwise(None).alias("FCYAMT"),
])

# PROC SORT DATA=LIST.COF_MNI_DEPOSITOR_LIST OUT=COF_MNI_IDNO(KEEP=DEPID DEPGRP BUSSREG) NODUPKEY; BY BUSSREG;
con = duckdb.connect(database=":memory:")
mni_list = con.execute(f"SELECT * FROM read_parquet('{MNILIST_CACHE}')").pl()
topdep = con.execute(f"SELECT * FROM read_parquet('{TOPDEP_CACHE}')").pl()
con.close()
gc.collect()

cof_mni_idno = (
    mni_list.select(["DEPID", "DEPGRP", "BUSSREG"])
    .sort("BUSSREG")
    .unique(subset=["BUSSREG"], keep="first")
    .rename({"BUSSREG": "NEWIC"})
)

cof_mni_cust = (
    mni_list.select(["DEPID", "DEPGRP", "CUSTNO"])
    .sort("CUSTNO")
    .unique(subset=["CUSTNO"], keep="first")
)

# DATA MNI_IC MNI_ICX(DROP=DEPID DEPGRP);
#    MERGE SUBS_ALL(IN=A) COF_MNI_IDNO(RENAME=(BUSSREG=NEWIC)); BY NEWIC; IF A;
#    IF DEPID>0 THEN OUTPUT MNI_IC; ELSE OUTPUT MNI_ICX;
subs_all_pd = subs_all.sort("NEWIC").to_pandas()
cof_mni_idno_pd = cof_mni_idno.to_pandas()

merged_ic = subs_all_pd.merge(cof_mni_idno_pd, on="NEWIC", how="left")
mni_ic = merged_ic[merged_ic["DEPID"] > 0].copy()
mni_icx = merged_ic[~(merged_ic["DEPID"] > 0)].drop(columns=["DEPID", "DEPGRP"], errors="ignore").copy()

# DATA MNI_CUST MNI_CUSTX(DROP=DEPID);
#    MERGE MNI_ICX(IN=A) COF_MNI_CUST; BY CUSTNO; IF A;
#    IF DEPID>0 THEN OUTPUT MNI_CUST; ELSE OUTPUT MNI_CUSTX;
mni_icx_sorted = mni_icx.sort_values("CUSTNO")
cof_mni_cust_pd = cof_mni_cust.to_pandas()

merged_cust = mni_icx_sorted.merge(cof_mni_cust_pd, on="CUSTNO", how="left")
mni_cust = merged_cust[merged_cust["DEPID"] > 0].copy()
mni_custx = merged_cust[~(merged_cust["DEPID"] > 0)].drop(columns=["DEPID"], errors="ignore").copy()

# DATA MNI_ALL; SET MNI_IC MNI_CUST; PROC SORT; BY CUSTNO;
mni_all = pd.concat([mni_ic, mni_cust], ignore_index=True).sort_values("CUSTNO")

# PROC SORT DATA=LIST.KEEP_TOP_DEP_EXCL_PBB OUT=TOPDEP; BY CUSTNO;
topdep_pd = topdep.sort("CUSTNO").to_pandas()

# DATA SUBS_ALL; MERGE MNI_ALL(IN=A) TOPDEP(IN=B); BY CUSTNO; IF A AND NOT B;
merged_final = mni_all.merge(
    topdep_pd[["CUSTNO"]].assign(_IN_TOPDEP=True), on="CUSTNO", how="left"
)
subs_all_final = merged_final[merged_final["_IN_TOPDEP"].isna()].drop(columns=["_IN_TOPDEP"])
subs_all_final = subs_all_final.sort_values(["DEPID"])

del subs_all, subs_all_pd, merged_ic, mni_ic, mni_icx, mni_icx_sorted, merged_cust
del mni_cust, mni_custx, mni_all, merged_final
gc.collect()

# PROC MEANS DATA=SUBS_ALL NWAY NOPRINT; VAR DEPID; OUTPUT OUT=MAX_ID MIN=S_ID MAX=L_ID;
if len(subs_all_final) > 0:
    max_depid = int(subs_all_final["DEPID"].max())
else:
    max_depid = -1

print(f"  MAX DEPID: {max_depid}")

# PROC SORT DATA=SUBS_ALL; BY CUSTNO ACCTNO;
subs_all_final = subs_all_final.sort_values(["CUSTNO", "ACCTNO"])

# %MACRO PRNSUB; %DO I=0 %TO &MAX %BY 1; ... %END; %MEND; %PRNSUB;
subs_writer = _PageWriter(_subs_header)
last_group_name = ""

for depid in range(0, max_depid + 1):
    subset = subs_all_final[subs_all_final["DEPID"] == depid]
    if subset.empty:
        # No rows for this DEPID -- SAS would still execute PROC PRINT with
        # zero observations (header-only page) using the previous GROUP
        # symbol value carried over from the prior iteration.
        continue

    # GROUP=DEPGRP; CALL SYMPUT('GROUP',GROUP);  -> last row's DEPGRP wins
    last_group_name = str(subset["DEPGRP"].iloc[-1] or "")

    subs_writer.new_page(last_group_name)
    group_total = 0.0
    for _, row in subset.iterrows():
        branch_str = str(int(row.get("BRANCH") or 0)).rjust(6) if pd.notna(row.get("BRANCH")) else " " * 6
        acctno_str = f"{int(row.get('ACCTNO') or 0):>12d}"
        custname_str = str(row.get("CUSTNAME") or "")[:30]
        custno_str = f"{int(row.get('CUSTNO') or 0):>12d}" if pd.notna(row.get("CUSTNO")) else " " * 12
        custcode_str = str(row.get("CUSTCODE") or "")[:8].rjust(8)
        curbal_str = _fmt_comma(row.get("CURBAL"), 18)
        product_str = str(row.get("PRODUCT") or "")[:8].rjust(8)

        line = (
            f"{branch_str:<7}{acctno_str}{custname_str:<30}{custno_str}"
            f"{custcode_str}{curbal_str}{product_str}"
        )
        subs_writer.add_line(line)
        group_total += float(row.get("CURBAL") or 0.0)

    subs_writer.add_line(f"{'':<57}{'TOTAL':>12}{_fmt_comma(group_total, 18)}")

subs_writer.write(OUTPUT_FD2)
print(f"  Output written : {OUTPUT_FD2}  ({len(subs_writer.lines):,} lines)")

# ============================================================================
# TERMINAL SUMMARY OUTPUT
# ============================================================================
print("\n" + "=" * 60)
print("EIBDTP50 OUTPUT SUMMARY")
print("=" * 60)
print(f"Report date            : {RDATE}")
print(f"FD11TEXT (Individual)  : {OUTPUT_FD11}")
print(f"FD12TEXT (Corporate)   : {OUTPUT_FD12}")
print(f"FD2TEXT  (Subsidiaries): {OUTPUT_FD2}")
print(f"Top-100 Individual rows: {len(data3_ind):,}")
print(f"Top-100 Corporate rows : {len(data3_org):,}")
print(f"Subsidiary detail rows : {len(subs_all_final):,}")

gc.collect()
print("\nEIBDTP50 complete.")
