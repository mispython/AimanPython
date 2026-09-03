
"""
Program : EIWFRMCR.py
Purpose : Generate Foreign Remittance Report (Inward & Outward) for
          compliance checking. Reads remittance transactions, filters
          foreign ('F') transactions with STATUS IN ('TI','TO','BR'),
          and splits them into an INWARD extract (STATUS='TI') and an
          OUTWARD extract (STATUS IN ('TO','BR')).
"""

import gc
import sys
from pathlib import Path
from datetime import date, datetime, timedelta

import duckdb
import paramiko
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

# EDW_TRANSFORMATION is not available in this environment, so the SFTP lookup
# below reads the existing DWH control dataset directly. The report date is
# derived later as yesterday; production paths and DRR settings stay unchanged.

SFTP_CONTROL_FILE = Path("/sasdata/dwh/control/ctl_dwh_sftp_info.sas7bdat")


def get_sftp_info(host_desc: str):
    """Read one SFTP configuration row directly from the DWH control file."""
    if not SFTP_CONTROL_FILE.exists():
        raise FileNotFoundError(
            f"SFTP control file not found: {SFTP_CONTROL_FILE}"
        )

    ctl = pd.read_sas(SFTP_CONTROL_FILE, format="sas7bdat", encoding="utf-8")
    ctl.columns = [str(column).upper().strip() for column in ctl.columns]

    required = {"HOST_DESC", "SFTP_ID", "SFTP_PW", "HOST_IP"}
    missing_columns = sorted(required.difference(ctl.columns))
    if missing_columns:
        raise RuntimeError(
            "SFTP control file is missing columns: " + ", ".join(missing_columns)
        )

    descriptions = ctl["HOST_DESC"].fillna("").astype(str).str.strip()
    rows = ctl[descriptions.eq(host_desc)]
    if len(rows) != 1:
        raise RuntimeError(
            f"Expected exactly one SFTP row for '{host_desc}'; found {len(rows)}"
        )

    row = rows.iloc[0]

    # SAS character values can contain fixed-width padding. Remove whitespace
    # using the same convention as the working reference job.
    clean = lambda value: "".join(str(value).split()) if pd.notna(value) else ""
    sftp_id = clean(row["SFTP_ID"])
    sftp_pw = clean(row["SFTP_PW"])
    host_ip = clean(row["HOST_IP"])
    host_key = clean(row["HOST_KEY"]) if "HOST_KEY" in ctl.columns else ""

    empty_values = [
        name for name, value in (
            ("SFTP_ID", sftp_id), ("SFTP_PW", sftp_pw), ("HOST_IP", host_ip)
        ) if not value
    ]
    if empty_values:
        raise RuntimeError(
            f"SFTP row for '{host_desc}' has blank values: {', '.join(empty_values)}"
        )

    return sftp_id, sftp_pw, host_ip, host_key

# ============================================================
# PATH CONFIGURATION
# ============================================================
# # Testing Path
# BASE_DIR    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
# INPUT_DIR   = BASE_DIR / "input" / "prod" / "remittance"
# CACHE_DIR   = BASE_DIR / "input" / "cache" / "EIWFRMCR"
# OUTPUT_DIR  = BASE_DIR / "output" / "EIWFRMCR"

# Prodcution Path
BASE_DIR   = Path("/dwh")
INPUT_DIR  = BASE_DIR / "cmsbdptr"
OUTPUT_DIR = Path("/host_pq/mis/output/rmt")
CACHE_DIR  = Path("/host_pq/mis/parquet/rmt")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

RMT_PREFIX = "remtran"

# Original DSNs: SAP.PBB.COMP.INWARD.REPT / SAP.PBB.COMP.OUTWARD.REPT
# No date component is present in these SAS-catalogued output DSNs, so
# output_date.py's date-stamped naming is not applicable here;
# filenames are static.

# Generate time stamp
reptdate = date.today() - timedelta(days=1)
_ts = reptdate.strftime("%y%m%d")

# _ts = datetime.now().strftime("%y%m%d_%H%M%S")
# _ts = datetime.now().strftime("%y%m%d")

INWARD_REPORT_FILE = OUTPUT_DIR / f"ForeignInwardRemittances_{_ts}.txt"
OUTWARD_REPORT_FILE = OUTPUT_DIR / f"ForeignOutwardRemittances_{_ts}.txt"

# Delimiter used in SAS: DLM = '05'X (hex 05 / ASCII ENQ control character)
# DLM = chr(0x05)
DLM = "\t"

# ============================================================
# CHUNK SIZE FOR STREAMING LARGE .sas7bdat FILES
# ============================================================
CHUNK_ROWS = 500_000

# ============================================================
# REPORT DATE DERIVATION (always yesterday)
# ============================================================
# Use the previous calendar day as the report/input date.
batch_dt = datetime.combine(
    date.today() - timedelta(days=1),
    datetime.min.time(),
)

# Derive the report-date components
RDATE = batch_dt.date()
REPTYEAR = batch_dt.strftime("%y")   # two-digit year
REPTMON = batch_dt.strftime("%m")    # zero-padded month

# Compute NOWK (same logic as SAS SELECT(DAY(REPTDATE)))
day_of_month = batch_dt.day
if 1 <= day_of_month <= 8:
    NOWK = '1'
elif 9 <= day_of_month <= 15:
    NOWK = '2'
elif 16 <= day_of_month <= 22:
    NOWK = '3'
else:
    NOWK = '4'

print(f"Report Date  : {RDATE}")
print(f"Report Year  : {REPTYEAR}")
print(f"Report Month : {REPTMON}")
print(f"Report Week  : {NOWK}")

# ============================================================
# BUILD RMT FILENAME from derived components  (matches SAS naming)
# ============================================================
# Original: SET RMT.REMTRAN&REPTMON&NOWK&REPTYEAR;
rmt_filename = f"remtran{REPTMON}{NOWK}{REPTYEAR}.sas7bdat"
rmt_file = INPUT_DIR / rmt_filename

if not rmt_file.exists():
    raise FileNotFoundError(
        f"Expected RMT file not found: {rmt_file}\n"
        f"Derived from batch date {batch_dt.date()} with NOWK={NOWK}."
    )

print(f"Input RMT File : {rmt_file}")

RMT_CACHE = CACHE_DIR / f"{rmt_file.stem}.parquet"


# ============================================================
# HELPER: CACHE STAMP  (skip re-conversion if .sas7bdat hasn't changed)
# ============================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    """Return True when the Parquet cache is newer than the source SAS file."""
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


# ============================================================
# HELPER: STREAM .sas7bdat TO PARQUET (memory-efficient chunked conversion)
# ============================================================
def sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    """Convert a large .sas7bdat to Parquet in streaming chunks."""
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0

    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
    for chunk in reader:
        table = pa.Table.from_pandas(chunk, preserve_index=False)

        if schema is None:
            # Lock schema on first chunk, but never lock in a 'null' type
            # inferred from a chunk that happened to be blank for a text
            # column - that would silently null out real values found in
            # every later chunk.
            fields = []
            for field in table.schema:
                if pa.types.is_null(field.type):
                    fields.append(pa.field(field.name, pa.string()))
                else:
                    fields.append(field)
            schema = pa.schema(fields)
            table = table.cast(schema, safe=False)
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")
        else:
            # Cast subsequent chunks to match the locked schema
            cast_arrays = []
            for field in schema:
                col = table.column(field.name)
                if col.type != field.type:
                    try:
                        col = col.cast(field.type, safe=False)
                    except Exception as e:
                        # Never silently null out real data. If the locked
                        # schema field is a string, force through as string;
                        # only fall back to nulls for genuine type conflicts.
                        if pa.types.is_string(field.type):
                            col = col.cast(pa.string(), safe=False)
                        else:
                            print(f"  [{tag}] WARNING: Cannot cast '{field.name}' "
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


# ============================================================
# CACHE RMT FILE TO PARQUET (~700 MB - streamed, not loaded whole)
# ============================================================
print("\nCaching RMT file to Parquet (if needed)...")

# # ONE-TIME: force rebuild since the existing cache was written by the old
# # schema-locking logic and may contain silently-nulled columns. Remove this
# # `unlink()` block after the next successful run so caching resumes as normal.
# if RMT_CACHE.exists():
#     print(f"  [RMT] Removing stale cache: {RMT_CACHE.name}")
#     RMT_CACHE.unlink()

if not _cache_is_fresh(rmt_file, RMT_CACHE):
    sas_to_parquet(rmt_file, RMT_CACHE, "RMT")
else:
    print(f"  [RMT] Cache fresh - skipping conversion.")


# # TEMPORARY DIAGNOSTIC - remove after confirming the fix
# con = duckdb.connect(database=":memory:")
# print(con.execute(f"""
#     SELECT BRANCHABB, BNAD1, ANAD1, CURRENCY, COUNTRY
#     FROM read_parquet('{RMT_CACHE}')
#     WHERE BRANCHABB IS NOT NULL
#     LIMIT 10
# """).pl())
# con.close()


# ============================================================
# FIELD FORMATTING HELPERS (SAS PUT var +(-1) DLM +(-1) idiom)
# ============================================================
def _fmt_str(value) -> str:
    """Format a character field: trim leading/trailing blanks.

    Equivalent of SAS `var +(-1)` trimming the trailing blank that list
    output would otherwise add before the delimiter.
    """
    if value is None:
        return " "
    stripped = str(value).strip()
    return stripped if stripped else " "


def _fmt_num(value) -> str:
    """Format a numeric field the way SAS's default (unformatted) PUT would.

    SAS stores all numerics as Float64 internally; whole values are written
    without a trailing '.0'.
    """
    if value is None:
        return ""
    if isinstance(value, float):
        if value != value:  # NaN
            return ""
        if value.is_integer():
            return str(int(value))
    return str(value).strip()


def _fmt_date(value) -> str:
    """Format a transaction date field.

    The original PUT statement carries no explicit date format, so the
    variable's stored/permanent format is assumed. DDMMYYYY is used here
    for report readability.
    """
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%d/%m/%Y")
    return str(value).strip()


def _build_line(fields: list) -> str:
    """Join fields with DLM, including a trailing DLM after the last field
    (matches the SAS `... +(-1)DLM+(-1)` pattern repeated on every field,
    including the last one in each PUT statement)."""
    return DLM.join(fields) + DLM


# ============================================================
# HEADER DEFINITIONS
# ============================================================
INWARD_HEADERS = [
    "TRANSACTION DATE",
    "BRANCH NAME",
    "REFERENCE NO",
    "BENEFICIARY NAME 1",
    "BENEFICIARY NAME 2",
    "BENEFICIARY NAME 3",
    "BENEFICIARY NAME 4",
    "ORDERING CUSTOMER 1",
    "ORDERING CUSTOMER 2",
    "ORDERING CUSTOMER 3",
    "ORDERING CUSTOMER 4",
    "AMOUNT (FCY)",
    "AMOUNT (MYR)",
    "ACCOUNT NO",
    "CURRENCY",
    "PAYMENT MODE",
    "STAFF ID",
    "RESIDENCY",
    "NATIONALITY",
    "BMR STATUS",
    "COUNTRY",
    "CBOP CODE",
]

OUTWARD_HEADERS = [
    "TRANSACTION DATE",
    "BRANCH NAME",
    "REFERENCE NO",
    "ORDERING CUSTOMER 1",
    "ORDERING CUSTOMER 2",
    "ORDERING CUSTOMER 3",
    "ORDERING CUSTOMER 4",
    "ORDERING CUSTOMER ACCOUNT NO",
    "BENEFICIARY NAME 1",
    "BENEFICIARY NAME 2",
    "BENEFICIARY NAME 3",
    "BENEFICIARY NAME 4",
    "AMOUNT (FCY)",
    "AMOUNT (MYR)",
    "ACCOUNT NO",
    "CURRENCY",
    "PAYMENT MODE",
    "STAFF ID",
    "RESIDENCY",
    "NATIONALITY",
    "BMR STATUS",
    "COUNTRY",
    "CBOP CODE",
]


# ============================================================
# DETAIL LINE BUILDERS
# ============================================================
def _build_inward_line(row: dict) -> str:
    """Equivalent of the SAS `DATA _NULL_; SET INWARD;` PUT statement."""
    fields = [
        _fmt_date(row["LASTTRAN"]),
        _fmt_str(row["BRANCHABB"]),
        _fmt_num(row["SERIAL"]),
        _fmt_str(row["BNAD1"]),
        _fmt_str(row["BNAD2"]),
        _fmt_str(row["BNAD3"]),
        _fmt_str(row["BNAD4"]),
        _fmt_str(row["ANAD1"]),
        _fmt_str(row["ANAD2"]),
        _fmt_str(row["ANAD3"]),
        _fmt_str(row["ANAD4"]),
        _fmt_num(row["FORAMT"]),
        _fmt_num(row["AMOUNT"]),
        _fmt_num(row["ACCTNO"]),
        _fmt_str(row["CURRENCY"]),
        _fmt_str(row["PAYMODE"]),
        _fmt_str(row["USERID"]),
        _fmt_str(row["RESIDENT"]),
        _fmt_str(row["APPLNATIONAL"]),
        _fmt_str(row["BMRSTATUS"]),
        _fmt_str(row["COUNTRY"]),
        # _fmt_str(row["ADMIN"]),
        _fmt_num(row["ADMIN"]),
    ]
    return _build_line(fields)


def _build_outward_line(row: dict) -> str:
    """Equivalent of the SAS `DATA _NULL_; SET OUTWARD;` PUT statement."""
    fields = [
        _fmt_date(row["LASTTRAN"]),
        _fmt_str(row["BRANCHABB"]),
        _fmt_num(row["SERIAL"]),
        _fmt_str(row["ANAD1"]),
        _fmt_str(row["ANAD2"]),
        _fmt_str(row["ANAD3"]),
        _fmt_str(row["ANAD4"]),
        _fmt_str(row["PAYREF"]),
        _fmt_str(row["BNAD1"]),
        _fmt_str(row["BNAD2"]),
        _fmt_str(row["BNAD3"]),
        _fmt_str(row["BNAD4"]),
        _fmt_num(row["FORAMT"]),
        _fmt_num(row["AMOUNT"]),
        _fmt_num(row["ACCTNO"]),
        _fmt_str(row["CURRENCY"]),
        _fmt_str(row["PAYMODE"]),
        _fmt_str(row["USERID"]),
        _fmt_str(row["RESIDENT"]),
        _fmt_str(row["APPLNATIONAL"]),
        _fmt_str(row["BMRSTATUS"]),
        _fmt_str(row["COUNTRY"]),
        # _fmt_str(row["ADMIN"]),
        _fmt_num(row["ADMIN"]),
    ]
    return _build_line(fields)


def _write_report(
    df: pl.DataFrame,
    headers: list,
    build_line_fn,
    output_path: Path,
) -> list:
    """Write header line + one detail line per row.

    NOTE: Original DD is RECFM=FB, LRECL=1000 (fixed-block, no ASA carriage
    control - this is a plain delimited data extract, not a printed report).
    Fixed-length byte padding to LRECL=1000 is not reproduced here; each
    record is written as a newline-terminated delimited line, consistent
    with how this extract is consumed downstream (read/split by DLM).
    """
    lines = [_build_line(headers)]
    for row in df.iter_rows(named=True):
        lines.append(build_line_fn(row))

    output_path.write_text("\n".join(lines) + "\n", encoding="latin1")
    return lines


# ============================================================
# READ + FILTER RMT DATA FROM PARQUET CACHE  (DuckDB, filter pushed to SQL)
# ============================================================
# Original:
#   WHERE REMTYPE = 'F' AND STATUS IN ('TI','TO','BR');
#   IF STATUS = 'TI' THEN OUTPUT INWARD; ELSE OUTPUT OUTWARD;
#   KEEP BRANCHABB STATUS SERIAL LASTTRAN ANAD1 ANAD2 ANAD3 ANAD4
#        BNAD1 BNAD2 BNAD3 BNAD4 AMOUNT FORAMT PAYREF
#        ACCTNO CURRENCY PAYMODE USERID RESIDENT APPLNATIONAL
#        BMRSTATUS COUNTRY ADMIN;
KEEP_COLUMNS = [
    "BRANCHABB", "STATUS", "SERIAL", "LASTTRAN",
    "ANAD1", "ANAD2", "ANAD3", "ANAD4",
    "BNAD1", "BNAD2", "BNAD3", "BNAD4",
    "AMOUNT", "FORAMT", "PAYREF",
    "ACCTNO", "CURRENCY", "PAYMODE", "USERID", "RESIDENT",
    "APPLNATIONAL", "BMRSTATUS", "COUNTRY", "ADMIN",
]

print("\nReading + filtering RMT data from Parquet cache...")

con = duckdb.connect(database=":memory:")

filtered_df = con.execute(f"""
    SELECT {', '.join(KEEP_COLUMNS)}
    FROM read_parquet('{RMT_CACHE}')
    WHERE REMTYPE = 'F'
      AND STATUS IN ('TI', 'TO', 'BR')
""").pl()

con.close()
gc.collect()

print(f"  Filtered rows: {len(filtered_df):,}")

# Normalize fixed-width character columns (SAS pads char values with blanks)
str_cols = [c for c, dt in zip(filtered_df.columns, filtered_df.dtypes) if dt == pl.Utf8]
if str_cols:
    filtered_df = filtered_df.with_columns([pl.col(c).str.strip_chars() for c in str_cols])

inward_df = filtered_df.filter(pl.col("STATUS") == "TI")
outward_df = filtered_df.filter(pl.col("STATUS") != "TI")  # STATUS IN ('TO','BR')

del filtered_df
gc.collect()

# ============================================================
# WRITE OUTPUT FILES
# ============================================================
inward_lines = _write_report(inward_df, INWARD_HEADERS, _build_inward_line, INWARD_REPORT_FILE)
outward_lines = _write_report(outward_df, OUTWARD_HEADERS, _build_outward_line, OUTWARD_REPORT_FILE)

# ============================================================
# TERMINAL OUTPUT
# ============================================================
print(f"\nInward Report Output Path  : {INWARD_REPORT_FILE}")
print(f"Inward Records Written     : {inward_df.height}")
# print("Inward Report Preview:")
# for line in inward_lines[:5]:
#     print(line.replace(DLM, "|"))

print(f"\nOutward Report Output Path : {OUTWARD_REPORT_FILE}")
print(f"Outward Records Written    : {outward_df.height}")
# print("Outward Report Preview:")
# for line in outward_lines[:5]:
#     print(line.replace(DLM, "|"))

# ============================================================
# CACHE NOTE
# ============================================================
# The RMT parquet cache (RMT_CACHE) is intentionally kept across runs so a
# second execution against the same source file skips the expensive
# SAS -> Parquet conversion step. Remove it manually or let the freshness
# check (_cache_is_fresh) handle eviction when the source file changes.

del inward_df, outward_df
gc.collect()

# ============================================================
# SFTP CONFIGURATION (DRR - Data Report Repository System)
# ============================================================
# HOST_DESC value that identifies the DRR entry in
# ctl_dwh_sftp_info.sas7bdat.
DRR_HOST_DESC = "Data Report Repository (DRR)"

# Remote folder on the DRR server (from JCL: CD CD-BRC)
DRR_REMOTE_DIR = "CD-BRC"

# ============================================================
# SFTP UPLOAD TO DRR
# ============================================================
def _drr_remote_filename(prefix: str) -> str:
    """
    Build the DRR filename using the run date, matching the JCL's
    %OYYYY.%OMM.%ODD token. %O-prefixed variables in the original
    COZBATCH/NDM script represent the file-transfer run date (today),
    not the report date (REPTYEAR/REPTMON) - kept distinct on purpose.
    """
    today = datetime.now() - timedelta(days=1)
    return f"{prefix}_{today:%Y%m%d}.txt"


def sftp_upload_to_drr(local_files: list) -> None:
    """
    Upload output report file(s) to the DRR server.

    Equivalent of the JCL RUNSFTP step:
        //RUNSFTP  EXEC COZBATCH
        //CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(DRR#SFTP)
        lzopts servercp=$servercp,notrim,overflow=trunc,mode=text
        lzopts linerule=$lr
        CD CD-BRC
        PUT //SAP.PBB.COMP.INWARD.REPT   \
                  ForeignInwardRemittances_%OYYYY.%OMM.%ODD..txt
        PUT //SAP.PBB.COMP.OUTWARD.REPT  \
                  ForeignOutwardRemittances_%OYYYY.%OMM.%ODD..txt
        EOB

    Parameters:
        local_files: list of (local_path: Path, remote_filename_prefix: str)
    """
    sftp_id, sftp_pw, host_ip, host_key = get_sftp_info(DRR_HOST_DESC)

    ssh = paramiko.SSHClient()
    # NOTE: HOST_KEY from ctl_dwh_sftp_info.sas7bdat is available for strict
    # host-key pinning, but its stored format (hex blob / OpenSSH pubkey
    # string / fingerprint) is not confirmed yet - CONFIRM with infra team
    # before enabling strict pinning. AutoAddPolicy is used in the interim.
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    print(f"  Connecting to DRR host {host_ip} as {sftp_id} ...")
    ssh.connect(hostname=host_ip, username=sftp_id, password=sftp_pw)
    sftp = ssh.open_sftp()

    try:
        sftp.chdir(DRR_REMOTE_DIR)
    except IOError:
        sftp.close()
        ssh.close()
        sys.exit(f"Aborting: Remote DRR folder '{DRR_REMOTE_DIR}' not found")

    for local_path, remote_prefix in local_files:
        remote_name = _drr_remote_filename(remote_prefix)
        print(f"  SFTP upload: {local_path.name} -> {DRR_REMOTE_DIR}/{remote_name}")
        sftp.put(str(local_path), remote_name)

    sftp.close()
    ssh.close()
    print("  DRR upload complete.")

# ============================================================
# SFTP REPORTS TO DATA REPORT REPOSITORY SYSTEM (DRR)
# ============================================================
print("\nUploading reports to DRR via SFTP...")
sftp_upload_to_drr([
    (INWARD_REPORT_FILE, "ForeignInwardRemittances"),
    (OUTWARD_REPORT_FILE, "ForeignOutwardRemittances"),
])


print("\nEIWFRMCR complete.")
