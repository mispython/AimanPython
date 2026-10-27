#!/usr/bin/env python3
"""
Program : EIBDOFCY.py
Purpose : Outstanding FCY Loan, CA and FD (Indiv and Non-Indiv) —  FCY
"""

import polars as pl
import pandas as pd                 # required for pd.read_sas()
from pathlib import Path
import tempfile
import gc
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values
from input_date import get_latest_file
from output_date import build_output_file

# =============================================================================
# CONFIGURATION
# =============================================================================
CHUNK_ROWS = 500_000
TEMP_DIR   = None   # None = system temp; set a path if the default temp area is too small

# Testing Path
BASE_DIR   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
INPUT_DIR  = Path("/stgsrcsys/host/uat/AII")

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIBDOFCY"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = BASE_DIR  / "output/EIBDOFCY"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# # Production Path
# INPUT_DIR  = Path("/dwh")
# OUTPUT_DIR = Path("/host/mis/output/report") / "EIBDOFCY"
# OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

"""
FD_PATH      : DEPO.FD      — FD dataset (.sas7bdat); WHERE CURCODE NE 'MYR'
CURR_PATH    : DEPO.CURRENT — CA dataset (.sas7bdat); WHERE CURCODE NE 'MYR'
LN_NOTE_PATH : LOAN.LNNOTE  — loan note dataset (.sas7bdat); WHERE CURCODE NE 'MYR'
"""
FD_PATH      = INPUT_DIR / "MNITB"  / "intg_dp_acct_fd_d18.sas7bdat"
CURR_PATH    = INPUT_DIR / "MNITB"  / "intg_dp_acct_current_d18.sas7bdat"
LN_NOTE_PATH = INPUT_DIR / "MNILN"  / "enrh_ln_note_d18.sas7bdat"

# REQUIRED_FD_FCY_COLUMNS   = {"CUSTCODE", "CURCODE", "CURBAL"}
# REQUIRED_CURR_FCY_COLUMNS = {"ACCTNO", "FCY_TYPE", "CURCODE", "CUSTCODE", "CURBALRM"}
# REQUIRED_LN_COLUMNS       = {"ACCTNO", "CURCODE", "CUSTCODE", "CURBAL", "ENTITY_CD"}

REQUIRED_FD_COLUMNS   = {"CUSTCODE", "CURCODE", "CURBAL", "ENTITY_CD"}
REQUIRED_CURR_COLUMNS = {"CUSTCODE", "CURCODE", "CURBAL", "ENTITY_CD"}
REQUIRED_LN_COLUMNS   = {"CUSTCODE", "CURCODE", "CURBAL", "ENTITY_CD"}


# =============================================================================
# Helper — read one .sas7bdat and return a Polars DataFrame
# =============================================================================
# def sas_to_parquet(src: Path, dst: Path, chunk_rows: int = CHUNK_ROWS) -> None:
#     """Chunked .sas7bdat -> Parquet. Schema is locked on the first chunk."""
#     writer, schema = None, None
#     try:
#         # >>>>>>>>>> Uncomment this -> For production <<<<<<<<
#         for chunk in pd.read_sas(str(src), format="sas7bdat", encoding="latin1", chunksize=chunk_rows):
#         # # >>>>>>>>>> Uncomment this -> For testing purposes <<<<<<<<
#         # for chunk in [next(pd.read_sas(str(src), format="sas7bdat", encoding="latin1", chunksize=1000))]:
#             chunk.columns = [str(c).upper().strip() for c in chunk.columns]
#             # Force text columns to string so an all-null first chunk doesn't lock a null type
#             for col in chunk.columns[chunk.dtypes == object]:
#                 chunk[col] = chunk[col].astype("string")
#             table = pa.Table.from_pandas(chunk, preserve_index=False)
#             if writer is None:
#                 schema = table.schema
#                 writer = pq.ParquetWriter(str(dst), schema, compression="snappy")
#             else:
#                 table = table.cast(schema)
#             writer.write_table(table)
#     finally:
#         if writer is not None:
#             writer.close()


# def _read_sas7bdat(path: Path, required: set[str]) -> pl.DataFrame:
#     """
#     Convert one .sas7bdat to a temporary Parquet file, validate the required
#     columns, read only those columns via DuckDB, then delete the temp file.
#     """
#     if not path.exists():
#         raise FileNotFoundError(f"Missing required input file: {path}")

#     with tempfile.TemporaryDirectory(dir=TEMP_DIR) as tmp:
#         tmp_parquet = Path(tmp) / f"{path.stem}.parquet"
#         sas_to_parquet(path, tmp_parquet)

#         missing = sorted(required.difference(pq.read_schema(str(tmp_parquet)).names))
#         if missing:
#             raise ValueError(f"{path} is missing required column(s): {', '.join(missing)}")

#         cols = ", ".join(f'"{c}"' for c in sorted(required))
#         con = duckdb.connect()
#         try:
#             df = con.execute(
#                 f"SELECT {cols} FROM read_parquet('{tmp_parquet.as_posix()}')"
#             ).pl()
#         finally:
#             con.close()
#     # temp folder and Parquet file are removed here
#     return df

def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return cache_path.exists() and cache_path.stat().st_mtime >= sas_path.stat().st_mtime


def _sas_type_to_arrow(dtype) -> pa.DataType:
    if dtype == "object":
        return pa.string()
    if pd.api.types.is_integer_dtype(dtype):
        return pa.int64()
    if pd.api.types.is_float_dtype(dtype):
        return pa.float64()
    return pa.from_numpy_dtype(dtype)


def _sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    tmp_path = cache_path.with_suffix(".parquet.tmp")   # written aside, swapped in when complete
    writer = None
    schema = None
    total = 0
    for chunk in pd.read_sas(sas_path, format="sas7bdat", encoding="latin1", chunksize=CHUNK_ROWS):
        chunk.columns = [str(c).upper().strip() for c in chunk.columns]
        if schema is None:
            schema = pa.schema([pa.field(c, _sas_type_to_arrow(t)) for c, t in chunk.dtypes.items()])
            writer = pq.ParquetWriter(tmp_path, schema, compression="snappy")
        table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)
        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()
    if writer:
        writer.close()
    tmp_path.replace(cache_path)
    print(f"  [{tag}] Done - {total:,} rows cached.")


def _load_cached(sas_path: Path, tag: str) -> Path:
    if not sas_path.exists():
        raise FileNotFoundError(f"Missing required input file: {sas_path}")
    # Cache name = immediate parent folder + file stem, so same-named files in
    # different subfolders never share a cache.
    cache_path = CACHE_DIR / f"{sas_path.parent.name}_{sas_path.stem}.parquet"
    if _cache_is_fresh(sas_path, cache_path):
        print(f"  [{tag}] Cache fresh - skipping conversion.")
    else:
        _sas_to_parquet(sas_path, cache_path, tag)
    return cache_path


def _require_columns(names: list[str], required: set[str], source: Path) -> None:
    """Fail early with a clear message if the cached file lacks needed columns."""
    missing = sorted(required.difference(names))
    if missing:
        raise ValueError(f"{source} is missing required column(s): {', '.join(missing)}")


def _read_pq(cache_path: Path, required: set[str], source: Path) -> pl.DataFrame:
    """Validate columns, then read only the required columns from the Parquet cache via DuckDB."""
    _require_columns(pq.read_schema(cache_path).names, required, source)
    cols = ", ".join(f'"{c}"' for c in sorted(required))
    con = duckdb.connect(database=":memory:")
    try:
        return con.execute(
            f"SELECT {cols} FROM read_parquet('{cache_path.as_posix()}')"
        ).pl()
    finally:
        con.close()


# =============================================================================
# REPORT DATE DERIVATION
# (Replaces: DATA REPTDATE; SET DEPO.REPTDATE; CALL SYMPUT('RDATE', PUT(REPTDATE, DDMMYY10.));)
# REPTDATE is derived as TODAY() - 1; RDATE formatted as DD/MM/YYYY (DDMMYY10.)
# =============================================================================
reptdate_values = get_reptdate_values(year_format="%Y")

REPTDATE = reptdate_values.reptdate
REPTYEAR = reptdate_values.reptyear
REPTMON  = reptdate_values.reptmon
REPTDAY  = reptdate_values.reptday
NOWK     = reptdate_values.nowk
RDATE    = REPTDATE.strftime("%d/%m/%Y")   # DDMMYY10. → DD/MM/YYYY

# =============================================================================
# CACHE INPUTS  (.sas7bdat -> Parquet, reused while the SAS file is unchanged)
# =============================================================================
print("Caching input SAS datasets to Parquet...")

FD_CACHE   = _load_cached(FD_PATH,   "FD_FCY")
CURR_CACHE = _load_cached(CURR_PATH, "CURR_FCY")
LN_NOTE_CACHE  = _load_cached(LN_NOTE_PATH,  "LN_NOTE")

# =============================================================================
# DATA FD  (SET DEPO.FD; WHERE CURCODE NE 'MYR')
# Only FCY FD records are needed — sourced from FD_FCY_PATH.
# DEPO.FD (MYR-only records) is excluded per WHERE CURCODE NE 'MYR'.
# =============================================================================
_fd_raw = _read_pq(FD_CACHE, REQUIRED_FD_COLUMNS, FD_PATH)

fd_raw = (
    _fd_raw
    .with_columns(
        pl.col("CUSTCODE")
          .cast(pl.Float64).cast(pl.Int64).cast(pl.Utf8)
          .str.strip_chars()
    )
    .filter(pl.col("CURCODE") != "MYR")
    .filter(pl.col("ENTITY_CD").fill_null("").str.strip_chars() != "PIBB")   # PBB only
    .select(["CUSTCODE", "CURCODE", "CURBAL"])
)

fd_df = (
    fd_raw
    .with_columns([
        pl.when(pl.col("CUSTCODE").is_in(["77", "78", "95", "96"]))
          .then(pl.lit("A")).otherwise(pl.lit("B"))
          .alias("IND"),
        pl.when(pl.col("CUSTCODE").is_in(["77", "78", "95", "96"]))
          .then(pl.col("CURBAL")).otherwise(pl.lit(None, dtype=pl.Float64))
          .alias("IFDBAL"),
        pl.when(pl.col("CUSTCODE").is_in(["77", "78", "95", "96"]))
          .then(pl.lit(None, dtype=pl.Float64)).otherwise(pl.col("CURBAL"))
          .alias("CFDBAL"),
    ])
    .select(["IND", "CURCODE", "CURBAL", "IFDBAL", "CFDBAL"])
)

# PROC SUMMARY DATA=FD NWAY; BY IND CURCODE; VAR CURBAL IFDBAL CFDBAL; OUTPUT OUT=FD SUM=;
fd_summary = (
    fd_df
    .group_by(["IND", "CURCODE"])
    .agg([
        pl.len().alias("_FREQ_"),
        pl.col("CURBAL").sum(),
        pl.col("IFDBAL").sum(),
        pl.col("CFDBAL").sum(),
    ])
    .sort(["IND", "CURCODE"])
)

# =============================================================================
# DATA CURR  (SET DEPO.CURRENT; WHERE CURCODE NE 'MYR')
# Source: CURR_FCY_PATH file.
# Filter: 3000000000 <= ACCTNO <= 3999999999  (CA account number range)
# CURBALRM renamed to CURBAL; CURCODE already present.
# =============================================================================
_curr_raw = _read_pq(CURR_CACHE, REQUIRED_CURR_COLUMNS, CURR_PATH)

curr_raw = (
    _curr_raw
    .with_columns(
        pl.col("CUSTCODE")
          .cast(pl.Float64).cast(pl.Int64).cast(pl.Utf8)
          .str.strip_chars()
    )
    # .filter(
    #     (pl.col("ACCTNO") >= 3_000_000_000) &
    #     (pl.col("ACCTNO") <= 3_999_999_999)
    #     # (pl.col("FCY_TYPE") == "FCYCA")
    # )
    # .rename({"CURBALRM": "CURBAL"})
    .filter(pl.col("CURCODE") != "MYR")
    .filter(pl.col("ENTITY_CD").fill_null("").str.strip_chars() != "PIBB")   # PBB only
    .select(["CUSTCODE", "CURCODE", "CURBAL"])
)

curr_df = (
    curr_raw
    .with_columns([
        pl.when(pl.col("CUSTCODE").is_in(["77", "78", "95", "96"]))
          .then(pl.lit("C")).otherwise(pl.lit("D"))
          .alias("IND"),
        pl.when(pl.col("CUSTCODE").is_in(["77", "78", "95", "96"]))
          .then(pl.col("CURBAL")).otherwise(pl.lit(None, dtype=pl.Float64))
          .alias("ICABAL"),
        pl.when(pl.col("CUSTCODE").is_in(["77", "78", "95", "96"]))
          .then(pl.lit(None, dtype=pl.Float64)).otherwise(pl.col("CURBAL"))
          .alias("CCABAL"),
    ])
    .select(["IND", "CURCODE", "CURBAL", "ICABAL", "CCABAL"])
)

# PROC SUMMARY DATA=CURR NWAY; BY IND CURCODE; VAR CURBAL ICABAL CCABAL; OUTPUT OUT=CURR SUM=;
curr_summary = (
    curr_df
    .group_by(["IND", "CURCODE"])
    .agg([
        pl.len().alias("_FREQ_"),
        pl.col("CURBAL").sum(),
        pl.col("ICABAL").sum(),
        pl.col("CCABAL").sum(),
    ])
    .sort(["IND", "CURCODE"])
)

curr_summary.write_parquet(OUTPUT_DIR / "CURR.parquet")
curr_summary.write_csv(OUTPUT_DIR / "CURR.csv")

# =============================================================================
# DATA LOAN  (SET LOAN.LNNOTE; WHERE CURCODE NE 'MYR')
# Source: LN_NOTE_PATH file.
# Filter: 2000000000 <= ACCTNO <= 2999999999  (loan account number range)
# =============================================================================
_loan_raw = _read_pq(LN_NOTE_CACHE, REQUIRED_LN_COLUMNS, LN_NOTE_PATH)

loan_raw = (
    _loan_raw
    .with_columns(
        pl.col("CUSTCODE")
          .cast(pl.Float64).cast(pl.Int64).cast(pl.Utf8)
          .str.strip_chars()
    )
    .filter(pl.col("CURCODE") != "MYR")
    .filter(pl.col("ENTITY_CD").fill_null("").str.strip_chars() != "PIBB")   # PBB only
    .select(["CUSTCODE", "CURCODE", "CURBAL"])
)

loan_df = (
    loan_raw
    .with_columns([
        pl.when(pl.col("CUSTCODE").is_in(["77", "78", "95", "96"]))
          .then(pl.lit("E")).otherwise(pl.lit("F"))
          .alias("IND"),
        pl.when(pl.col("CUSTCODE").is_in(["77", "78", "95", "96"]))
          .then(pl.col("CURBAL")).otherwise(pl.lit(None, dtype=pl.Float64))
          .alias("ILNBAL"),
        pl.when(pl.col("CUSTCODE").is_in(["77", "78", "95", "96"]))
          .then(pl.lit(None, dtype=pl.Float64)).otherwise(pl.col("CURBAL"))
          .alias("CLNBAL"),
    ])
    .select(["IND", "CURCODE", "CURBAL", "ILNBAL", "CLNBAL"])
)

# PROC SUMMARY DATA=LOAN NWAY; BY IND CURCODE; VAR CURBAL ILNBAL CLNBAL; OUTPUT OUT=LOAN SUM=;
loan_summary = (
    loan_df
    .group_by(["IND", "CURCODE"])
    .agg([
        pl.len().alias("_FREQ_"),
        pl.col("CURBAL").sum(),
        pl.col("ILNBAL").sum(),
        pl.col("CLNBAL").sum(),
    ])
    .sort(["IND", "CURCODE"])
)

# =============================================================================
# DATA FCY  (SET FD CURR LOAN; BY IND; FILE OutstandingFCY;)
# Produces the detailed section of OutstandingFCY.txt
# =============================================================================
# Combine all three summary datasets (diagonal concat fills missing cols with null)
fcy_combined = pl.concat(
    [fd_summary, curr_summary, loan_summary],
    how="diagonal_relaxed"
).sort(["IND", "CURCODE"])

DLM = "\t"   # tab spacing delimiter
# DLM = "\x05"   # '05'X    - # EBCDIC-style delimiter
# DLM = "|"                   # Standard report delimiter

# IND group ordering matches SAS SET order: A B C D E F
_IND_HEADERS = {
    "A": "INDIVIDUAL - FCY FIXED DEPOSIT",
    "B": "NON INDIVIDUAL - FCY FIXED DEPOSIT",
    "C": "INDIVIDUAL - FCY CURRENT",
    "D": "NON INDIVIDUAL - FCY CURRENT",
    "E": "INDIVIDUAL - FCY LOAN",
    "F": "NON INDIVIDUAL - FCY LOAN",
}

out_txt = build_output_file(
    OUTPUT_DIR,
    "OutstandingFCY",
    date_format="ddmmYYYY"
).with_suffix(".txt")
# out_csv = OUTPUT_DIR / "OutstandingFCY.csv"

with open(out_txt, "w", encoding="utf-8") as f:

    # _N_ = 1 block  (first record written to FILE OutstandingFCY)
    f.write(f"REPORT ID : EIBDOFCY\n")
    f.write(f"PUBLIC BANK BERHAD\n")
    f.write(f"OUTSTANDING FCY LOAN AND DEPOSITS AS AT {RDATE}\n")
    f.write("\n")

    prev_ind = None
    nobs     = 0

    csv_rows = []       # For csv creation

    for row in fcy_combined.iter_rows(named=True):
        ind = row["IND"]

        # FIRST.IND block
        if ind != prev_ind:
            nobs     = 0
            prev_ind = ind

            if ind == "A":
                # PUT @1  'INDIVIDUAL - FCY FIXED DEPOSIT';
                f.write(f"{_IND_HEADERS[ind]}\n")
            else:
                # PUT @1// '<header>';  (two blank lines before header)
                f.write(f"\n\n{_IND_HEADERS[ind]}\n")

            # PUT @01/ 'OBS' DLM+(-1) 'CURCODE' DLM+(-1) 'FREQ' DLM+(-1) 'CURRENT BALANCE' DLM+(-1);
            # The / before OBS produces one blank line (moves to next line)
            f.write("\n")
            f.write(f"OBS{DLM}CURCODE{DLM}FREQ{DLM}CURRENT BALANCE{DLM}\n")

        nobs += 1
        freq   = row.get("_FREQ_") or 0
        curbal = row.get("CURBAL") or 0.0

        # For csv creation
        csv_rows.append({
            "NOBS"            : nobs,
            "IND"             : ind,
            "CURCODE"         : row["CURCODE"],
            "FREQ"            : freq,
            "CURRENT_BALANCE" : curbal,
        })

        # PUT @01 NOBS 3. DLM+(-1) CURCODE $3. DLM+(-1) _FREQ_ 10. DLM+(-1) CURBAL COMMA20.2;
        f.write(
            f"{nobs:3d}{DLM}"
            f"{str(row['CURCODE']):3s}{DLM}"
            f"{freq:10d}{DLM}"
            f"{curbal:>20,.2f}\n"
        )

# For csv creation
pd.DataFrame(csv_rows).to_csv(OUTPUT_DIR / "OutstandingFCY.csv", index=False)

fcy_combined.write_parquet(OUTPUT_DIR / "FCY.parquet")
fcy_combined.write_csv(OUTPUT_DIR / "FCY.csv")

# =============================================================================
# PROC SUMMARY DATA=FCY NWAY; BY CURCODE;
# VAR IFDBAL CFDBAL ICABAL CCABAL ILNBAL CLNBAL CURBAL; OUTPUT OUT=FCY SUM=;
# =============================================================================
fcy_currency_summary = (
    fcy_combined
    .group_by("CURCODE")
    .agg([
        pl.col("IFDBAL").sum(),
        pl.col("CFDBAL").sum(),
        pl.col("ICABAL").sum(),
        pl.col("CCABAL").sum(),
        pl.col("ILNBAL").sum(),
        pl.col("CLNBAL").sum(),
        pl.col("CURBAL").sum(),
    ])
    .sort("CURCODE")
)

fcy_currency_summary.write_parquet(OUTPUT_DIR / "FCY_summary.parquet")
fcy_currency_summary.write_csv(OUTPUT_DIR / "FCY_summary.csv")

# =============================================================================
# DATA _NULL_  (SET FCY END=LAST; FILE OutstandingFCY MOD;)
# Appends the summary section to OutstandingFCY.txt
# =============================================================================
totifd = 0.0
totcfd = 0.0
totica = 0.0
totcca = 0.0
totiln = 0.0
totcln = 0.0

with open(out_txt, "a", encoding="utf-8") as f:

    n = 0

    for row in fcy_currency_summary.iter_rows(named=True):
        n += 1

        if n == 1:
            # PUT @1 / / "SUMMARY OF OUTSTANDING FCY LOAN AND DEPOSITS"
            #        / / 'NOBS' DLM+(-1) ... ;
            # Two '/' before the heading = two blank lines before it
            f.write("\n\n")
            f.write("SUMMARY OF OUTSTANDING FCY LOAN AND DEPOSITS\n")
            f.write("\n")
            f.write(
                f"NOBS{DLM}CURCODE{DLM}"
                f"FCYFD-INDIV{DLM}FCYFD-CORP{DLM}"
                f"FCYCA-INDIV{DLM}FCYCA-CORP{DLM}"
                f"FCYLN-INDIV{DLM}FCYLN-CORP\n"
            )

        ifdbal = row.get("IFDBAL") or 0.0
        cfdbal = row.get("CFDBAL") or 0.0
        icabal = row.get("ICABAL") or 0.0
        ccabal = row.get("CCABAL") or 0.0
        ilnbal = row.get("ILNBAL") or 0.0
        clnbal = row.get("CLNBAL") or 0.0

        # PUT @1 _N_ 3. DLM+(-1) CURCODE $3. DLM+(-1) IFDBAL COMMA20.2 DLM+(-1) ...
        f.write(
            f"{n:3d}{DLM}"
            f"{str(row['CURCODE']):3s}{DLM}"
            f"{ifdbal:>20,.2f}{DLM}"
            f"{cfdbal:>20,.2f}{DLM}"
            f"{icabal:>20,.2f}{DLM}"
            f"{ccabal:>20,.2f}{DLM}"
            f"{ilnbal:>20,.2f}{DLM}"
            f"{clnbal:>20,.2f}\n"
        )

        totifd += ifdbal
        totcfd += cfdbal
        totica += icabal
        totcca += ccabal
        totiln += ilnbal
        totcln += clnbal

    # IF LAST THEN PUT ...  (grand totals line)
    f.write(
        f"   {DLM}"
        f"TOTAL{DLM}"
        f"{totifd:>20,.2f}{DLM}"
        f"{totcfd:>20,.2f}{DLM}"
        f"{totica:>20,.2f}{DLM}"
        f"{totcca:>20,.2f}{DLM}"
        f"{totiln:>20,.2f}{DLM}"
        f"{totcln:>20,.2f}\n"
    )

print(f"\n EIBDOFCY completed — report date {RDATE}, output: {out_txt} \n")

with open(out_txt, "rb") as f:
    content = f.read(1000)
    print(content)
