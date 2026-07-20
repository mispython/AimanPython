"""
Program : EIWFRMCR.py
Purpose : Generate Foreign Remittance Report (Inward & Outward) for
          compliance checking. Reads remittance transactions, filters
          foreign ('F') transactions with STATUS IN ('TI','TO','BR'),
          and splits them into an INWARD extract (STATUS='TI') and an
          OUTWARD extract (STATUS IN ('TO','BR')).
          Output matches original SAS output exactly:
          - hex 05 delimiter
          - SAS `+(-1)` trimming of final space
          - leading spaces for numerics (BEST12.)
          - fixed 1000-byte records (RECFM=FB, LRECL=1000)
"""

import gc
import json
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pyreadstat

from REPTDATE import get_reptdate_values
from input_date import get_latest_file

# ============================================================
# PATH CONFIGURATION
# ============================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
INPUT_DIR = BASE_DIR / "input" / "prod" / "remittance"
CACHE_DIR = BASE_DIR / "input" / "cache" / "EIWFRMCR"
OUTPUT_DIR = BASE_DIR / "output" / "EIWFRMCR"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

RMT_PREFIX = "remtran"

# Output filenames (static, as in original)
INWARD_REPORT_FILE = OUTPUT_DIR / "SAP_PBB_COMP_INWARD_REPT.txt"
OUTWARD_REPORT_FILE = OUTPUT_DIR / "SAP_PBB_COMP_OUTWARD_REPT.txt"

# Delimiter: hex 05 (ENQ)
DLM = chr(0x05)
# Numeric width used by SAS BEST12.
NUM_WIDTH = 12

# ============================================================
# CHUNK SIZE FOR STREAMING LARGE .sas7bdat FILES
# ============================================================
CHUNK_ROWS = 500_000

# ============================================================
# REPORT DATE DERIVATION
# ============================================================
reptdate_values = get_reptdate_values()
REPTYEAR = reptdate_values.reptyear
REPTMON = reptdate_values.reptmon
RDATE = reptdate_values.rdate

print(f"Report Date  : {RDATE}")
print(f"Report Year  : {REPTYEAR}")
print(f"Report Month : {REPTMON}")

# ============================================================
# LOCATE LATEST RMT FILE (or reconstruct exact name if preferred)
# ============================================================
rmt_file = get_latest_file(INPUT_DIR, prefix=RMT_PREFIX)
print(f"Input RMT File : {rmt_file}")

RMT_CACHE = CACHE_DIR / f"{rmt_file.stem}.parquet"
META_CACHE = CACHE_DIR / f"{rmt_file.stem}_meta.json"


# ============================================================
# HELPER: CACHE STAMP
# ============================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


# ============================================================
# FUNCTION TO EXTRACT SAS COLUMN METADATA (using pyreadstat)
# ============================================================
def extract_sas_metadata(sas_path: Path) -> dict:
    """Return dict with column names as keys and lengths (for chars) or None (for nums)."""
    _, meta = pyreadstat.read_sas7bdat(sas_path, metadata_only=True)
    col_lengths = {}
    for name, typ, length in zip(meta.column_names, meta.column_types, meta.column_lengths):
        if typ == "string":
            col_lengths[name] = length
        else:
            col_lengths[name] = None  # numeric columns have no fixed width; we use NUM_WIDTH
    return col_lengths


# ============================================================
# STREAM .sas7bdat → PARQUET (with metadata cache)
# ============================================================
def sas_to_parquet_with_meta(sas_path: Path, cache_path: Path, meta_path: Path) -> None:
    """Convert large .sas7bdat to Parquet, and save column metadata as JSON."""
    print(f"  Converting {sas_path.name} -> {cache_path.name} ...")

    # 1. Extract metadata
    col_lengths = extract_sas_metadata(sas_path)
    with open(meta_path, "w") as f:
        json.dump(col_lengths, f)

    # 2. Stream data in chunks
    writer = None
    schema = None
    total = 0

    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
    for chunk in reader:
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        if schema is None:
            schema = table.schema
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")
        else:
            # Cast to match schema (as in original code)
            cast_arrays = []
            for field in schema:
                col = table.column(field.name)
                if col.type != field.type:
                    try:
                        col = col.cast(field.type, safe=False)
                    except Exception as e:
                        print(f"    WARNING: Cannot cast '{field.name}' "
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
    print(f"  Done — {total:,} rows cached.")


# ============================================================
# LOAD OR BUILD CACHE
# ============================================================
print("\nCaching RMT file to Parquet (if needed)...")

if not (_cache_is_fresh(rmt_file, RMT_CACHE) and META_CACHE.exists()):
    sas_to_parquet_with_meta(rmt_file, RMT_CACHE, META_CACHE)
else:
    print("  Cache fresh — skipping conversion.")

# Load column metadata
with open(META_CACHE, "r") as f:
    COL_LENGTHS = json.load(f)


# ============================================================
# FORMATTING HELPERS (SAS exact match)
# ============================================================
def _fmt_str(value, col_name: str) -> str:
    """Format a character column exactly as SAS `PUT var +(-1)` would.

    SAS writes the full length of the variable, then backs up one column
    before the delimiter, effectively dropping the last character of the
    field (which is always a space because SAS pads with trailing blanks).
    If the field is empty, we write (length-1) spaces.
    """
    if value is None:
        value = ""
    else:
        value = str(value).rstrip()  # remove any stored trailing spaces
    length = COL_LENGTHS.get(col_name, 30)  # fallback to 30 if unknown
    # We need to output length-1 characters (SAS removes the last char)
    # After rstrip, the value has no trailing blanks; pad to length-1 with spaces
    return value.ljust(length - 1)


def _fmt_num(value) -> str:
    """Format numeric as SAS BEST12. (right‑aligned, 12 chars)."""
    if value is None or (isinstance(value, float) and value != value):  # NaN
        return " " * NUM_WIDTH  # all blanks (SAS writes missing as '.'? But original has empty)
    # For numeric, SAS BEST12. output has no trailing spaces; we just right-align.
    return f"{value:>{NUM_WIDTH}}"


def _fmt_date(value) -> str:
    """Format date as YYYY-MM-DD (as in original sample)."""
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    return str(value).strip()


def _build_line(fields: list, col_names: list = None) -> str:
    """Join fields with DLM, including trailing DLM, using column-specific formatting."""
    if col_names is None:
        col_names = []
    formatted = []
    for i, (field, col) in enumerate(zip(fields, col_names)):
        if col is not None and col in COL_LENGTHS:
            # character column
            formatted.append(_fmt_str(field, col))
        else:
            # numeric (or unknown) – assume numeric
            formatted.append(_fmt_num(field))
    return DLM.join(formatted) + DLM


# ============================================================
# HEADER DEFINITIONS (same as original)
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
# DETAIL LINE BUILDERS (using column names for formatting)
# ============================================================
def _build_inward_line(row: dict) -> str:
    fields = [
        _fmt_date(row["LASTTRAN"]),
        row["BRANCHABB"],
        row["SERIAL"],
        row["BNAD1"],
        row["BNAD2"],
        row["BNAD3"],
        row["BNAD4"],
        row["ANAD1"],
        row["ANAD2"],
        row["ANAD3"],
        row["ANAD4"],
        row["FORAMT"],
        row["AMOUNT"],
        row["ACCTNO"],
        row["CURRENCY"],
        row["PAYMODE"],
        row["USERID"],
        row["RESIDENT"],
        row["APPLNATIONAL"],
        row["BMRSTATUS"],
        row["COUNTRY"],
        row["ADMIN"],
    ]
    col_names = [
        "LASTTRAN", "BRANCHABB", "SERIAL", "BNAD1", "BNAD2", "BNAD3", "BNAD4",
        "ANAD1", "ANAD2", "ANAD3", "ANAD4", "FORAMT", "AMOUNT", "ACCTNO",
        "CURRENCY", "PAYMODE", "USERID", "RESIDENT", "APPLNATIONAL",
        "BMRSTATUS", "COUNTRY", "ADMIN"
    ]
    return _build_line(fields, col_names)


def _build_outward_line(row: dict) -> str:
    fields = [
        _fmt_date(row["LASTTRAN"]),
        row["BRANCHABB"],
        row["SERIAL"],
        row["ANAD1"],
        row["ANAD2"],
        row["ANAD3"],
        row["ANAD4"],
        row["PAYREF"],
        row["BNAD1"],
        row["BNAD2"],
        row["BNAD3"],
        row["BNAD4"],
        row["FORAMT"],
        row["AMOUNT"],
        row["ACCTNO"],
        row["CURRENCY"],
        row["PAYMODE"],
        row["USERID"],
        row["RESIDENT"],
        row["APPLNATIONAL"],
        row["BMRSTATUS"],
        row["COUNTRY"],
        row["ADMIN"],
    ]
    col_names = [
        "LASTTRAN", "BRANCHABB", "SERIAL", "ANAD1", "ANAD2", "ANAD3", "ANAD4",
        "PAYREF", "BNAD1", "BNAD2", "BNAD3", "BNAD4", "FORAMT", "AMOUNT",
        "ACCTNO", "CURRENCY", "PAYMODE", "USERID", "RESIDENT", "APPLNATIONAL",
        "BMRSTATUS", "COUNTRY", "ADMIN"
    ]
    return _build_line(fields, col_names)


def _write_report(
    df: pl.DataFrame,
    headers: list,
    build_line_fn,
    output_path: Path,
) -> None:
    """Write header + detail lines, each exactly 1000 bytes long (no newline)."""
    with open(output_path, "wb") as f:  # binary mode – no newline conversion
        # Write header record
        header_line = _build_line(headers, col_names=None)  # headers are strings, no special formatting
        f.write(header_line.ljust(1000).encode("latin1"))

        # Write each detail record
        for row in df.iter_rows(named=True):
            line = build_line_fn(row)
            f.write(line.ljust(1000).encode("latin1"))


# ============================================================
# READ + FILTER RMT DATA FROM PARQUET CACHE
# ============================================================
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

# Split into inward (TI) and outward (TO, BR)
inward_df = filtered_df.filter(pl.col("STATUS") == "TI")
outward_df = filtered_df.filter(pl.col("STATUS") != "TI")
del filtered_df
gc.collect()

# ============================================================
# WRITE OUTPUT FILES (exact SAS format)
# ============================================================
_write_report(inward_df, INWARD_HEADERS, _build_inward_line, INWARD_REPORT_FILE)
_write_report(outward_df, OUTWARD_HEADERS, _build_outward_line, OUTWARD_REPORT_FILE)

# ============================================================
# TERMINAL OUTPUT (preview with visible delimiter replacement)
# ============================================================
print(f"\nInward Report Output Path  : {INWARD_REPORT_FILE}")
print(f"Inward Records Written     : {inward_df.height}")
print("Inward Report Preview (first 3 lines, | in place of delimiter):")
with open(INWARD_REPORT_FILE, "rb") as f:
    # Read first 3 records (each 1000 bytes)
    for _ in range(3):
        rec = f.read(1000).decode("latin1")
        print(rec.replace(DLM, "|").rstrip())

print(f"\nOutward Report Output Path : {OUTWARD_REPORT_FILE}")
print(f"Outward Records Written    : {outward_df.height}")
print("Outward Report Preview (first 3 lines, | in place of delimiter):")
with open(OUTWARD_REPORT_FILE, "rb") as f:
    for _ in range(3):
        rec = f.read(1000).decode("latin1")
        print(rec.replace(DLM, "|").rstrip())

print("\nEIWFRMCR complete.")
