#!/usr/bin/env python3
"""
Program : EIBDPTER.py
Purpose : FCY Fixed Deposit Maturity Report
          - FDTEXT : Summary of FCY FD maturity by currency (T, T+1, T+2, >T+2)
          - FDDETL : FCY FD detail by customer (balance > FCY 1 million)
          Produces two comma-delimited flat text files (RECFM=FB, LRECL=320).

Input:
    //FD       DD DSN=SAP.PBB.MNIFD.DAILY(0),DISP=SHR
        enrh_dp_fd_cert_d{REPDD}.sas7bdat

Output:
    //FDTEXT   DD DSN=SAP.PBB.FCYFD.REP1.TEXT
    //FDDETL   DD DSN=SAP.PBB.FCYFD.REP2.TEXT

Dependencies : PBBELF   (format_brchcd)
"""

from pathlib import Path
from datetime import date, timedelta
from typing import Optional
import math

import pandas as pd
import polars as pl
import tempfile
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# ============================================================
# Internal dependencies
# ============================================================
from PBBELF import format_brchcd

# ============================================================
# CONTROL FILE CONFIGURATION
# ============================================================
# Control file : batch date per source system
CTL_BATCH_DTTM = Path("/sasdata/dwh/control/ctl_dwh_batch_dttm.sas7bdat")
BATCH_SOURCE   = "DP"          # SOURCE_SYSTEM_CD used for this program


def get_batch_date(source_system_cd: str) -> date:
    """Read BATCH_DTTM for the given SOURCE_SYSTEM_CD from the control file."""
    ctl = pd.read_sas(str(CTL_BATCH_DTTM), encoding="latin1")
    ctl["SOURCE_SYSTEM_CD"] = ctl["SOURCE_SYSTEM_CD"].astype(str).str.strip()
    hit = ctl.loc[ctl["SOURCE_SYSTEM_CD"] == source_system_cd, "BATCH_DTTM"]
    if hit.empty:
        raise ValueError(f"SOURCE_SYSTEM_CD '{source_system_cd}' not found in {CTL_BATCH_DTTM}")
    return pd.Timestamp(hit.iloc[0]).date()


# ============================================================
# REPORT DATE  (from batch control file, SOURCE_SYSTEM_CD = DP)
# OPTIONS YEARCUTOFF=1950 is applied in _parse_orgdate
# ============================================================
REPTDATE = get_batch_date(BATCH_SOURCE)      # date object (T)
T1       = REPTDATE + timedelta(days=1)
T2       = REPTDATE + timedelta(days=2)
T3       = REPTDATE + timedelta(days=3)

REPDD  = REPTDATE.strftime("%d")             # DD  (Z2.)
REPMM  = REPTDATE.strftime("%m")             # MM  (Z2.)
REPYY  = REPTDATE.strftime("%Y")             # YEAR4.
REPDT  = REPTDATE.strftime("%d/%m/%Y")       # DDMMYY8. equivalent as used in the existing program
REPYMD = REPTDATE.strftime("%y%m%d")         # date token in input filename

# ============================================================
# PATH CONFIGURATION
# ============================================================
# Production Path
INPUT_DIR = Path("/sas/deposit/dwh")
FD_CERT   = INPUT_DIR / "enrichment" / f"enrh_dp_fd_cert_d{REPDD}.sas7bdat"
OUTPUT_DIR = Path("/host_pq/mis/output/deposit/report")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# # Testing Path
# BASE_DIR   = Path("/stgsrcsys/host/uat")
# # INPUT_DIR  = BASE_DIR / "input" / "prod" / "EIBDRMFC"  # directory containing fd.sas7bdat
# INPUT_DIR  = BASE_DIR / "AII/EIBDFFCD"                   # directory containing fd.sas7bdat
# OUTPUT_DIR = BASE_DIR / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS" / "output" / "EIBDPTER"
# OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
# # FD_SAS7BDAT = INPUT_DIR / "fd.sas7bdat"
# FD_SAS7BDAT = INPUT_DIR / f"enrh_dp_fd_cert_d14.sas7bdat"

OUTPUT_FDTEXT = OUTPUT_DIR / "FCYFD_REP1.txt"   # SAP.PBB.FCYFD.REP1.TEXT
OUTPUT_FDDETL = OUTPUT_DIR / "FCYFD_REP2.txt"   # SAP.PBB.FCYFD.REP2.TEXT

CHUNK_ROWS = 500_000
TEMP_DIR   = None   # None = system temp; set a path if the default temp area is too small

# ============================================================
# HELPER UTILITIES
# ============================================================

def _to_date(val) -> Optional[date]:
    """MATURE_DT / ORIG_DT are real dates in the new dataset (NaT -> None)."""
    if val is None or pd.isna(val):
        return None
    return pd.Timestamp(val).date()


def _parse_matdate(matdate_val) -> Optional[date]:
    return _to_date(matdate_val)


def _parse_orgdate(orgdate_val) -> Optional[date]:
    return _to_date(orgdate_val)


def _fmt_date_ddmmyy8(d: Optional[date]) -> str:
    """Format date as DD/MM/YY (SAS DDMMYY8. produces dd/mm/yy)."""
    if d is None:
        return ""
    return d.strftime("%d/%m/%y")


# ============================================================
# STEP 0 : Convert sas7bdat into parquet
# ============================================================

def sas_to_parquet(src: Path, dst: Path, chunk_rows: int = CHUNK_ROWS) -> None:
    """Chunked .sas7bdat -> Parquet. Schema is locked on the first chunk."""
    writer, schema = None, None
    try:
        for chunk in pd.read_sas(str(src), encoding="latin1", chunksize=chunk_rows):
            # Force text columns to string so an all-null first chunk doesn't lock a null type
            for col in chunk.columns[chunk.dtypes == object]:
                chunk[col] = chunk[col].astype("string")
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                schema = table.schema
                writer = pq.ParquetWriter(str(dst), schema, compression="snappy")
            else:
                table = table.cast(schema)
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()

# ============================================================
# STEP 1 : Read FD sas7bdat
# ============================================================
COLUMN_MAP = {
    "CUSTOMER_CD": "CUSTCD",
    "CURRENCY_CD": "CURCODE",
    "CURR_BAL":    "CURBAL",
    "MATURE_DT":   "MATDATE",
    "ORIG_DT":     "ORGDATE",
    "ORIG_AMT":    "ORIGAMT",
    "RT":          "RATE",
    "BRANCH_CD":   "BRANCH",
}


def read_fd() -> pd.DataFrame:
    """
    Convert FD_CERT to a temporary Parquet file, read it via DuckDB with the
    PBB entity filter (ENTITY_CD != 'PIBB'), then delete the temp file.
    Columns are renamed to the SAS names used in the rest of the program.
    """
    with tempfile.TemporaryDirectory(dir=TEMP_DIR) as tmp:
        tmp_parquet = Path(tmp) / "fd_cert.parquet"
        sas_to_parquet(FD_CERT, tmp_parquet)

        con = duckdb.connect()
        try:
            # COALESCE keeps null ENTITY_CD rows, matching SAS missing != 'PIBB'
            df = con.execute(
                f"""
                SELECT *
                FROM read_parquet('{tmp_parquet.as_posix()}')
                WHERE TRIM(COALESCE(CAST(ENTITY_CD AS VARCHAR), '')) <> 'PIBB'
                """
            ).df()
        finally:
            con.close()
    # temp folder and Parquet file are removed here

    # Source columns must exist, targets must not already exist
    missing = [c for c in COLUMN_MAP if c not in df.columns]
    if missing:
        raise KeyError(f"FD_CERT is missing expected columns: {missing}")
    clash = [c for c in COLUMN_MAP.values() if c in df.columns]
    if clash:
        raise ValueError(f"FD_CERT already has columns named {clash}; rename would create duplicates")

    df = df.rename(columns=COLUMN_MAP)

    # Every SAS name used later must now exist (plus the columns used as-is)
    required = list(COLUMN_MAP.values()) + ["ENTITY_CD", "FORATE"]
    absent = [c for c in required if c not in df.columns]
    if absent:
        raise KeyError(f"After rename, FD_CERT is missing: {absent}. Columns: {list(df.columns)}")

    return df


# ============================================================
# STEP 2 : DATA FCYT / FCYT1-4  — FCY maturity split
#
#   IF CUSTCD NOT IN (77,78,95,96);
#   IF CURCODE = 'MYR' THEN DELETE;
#   IF MATDATE = .    THEN DELETE;
#   CURBAL = CURBAL / FORATE;
#   Split into FCYT1 (MATDT=T1), FCYT2 (=T2), FCYT3 (=T3), FCYT4 (>T3)
# ============================================================

def build_fcyt(df: pd.DataFrame):
    """
    Apply FCY filters and convert CURBAL to MYR equivalent.
    Returns four sub-dataframes: fcyt1, fcyt2, fcyt3, fcyt4.
    """
    # Decode byte-string columns if necessary (pandas read_sas may return bytes)
    for col in ["CURCODE"]:
        if col in df.columns and df[col].dtype == object:
            df[col] = df[col].apply(
                lambda x: x.decode("latin1").strip() if isinstance(x, bytes) else str(x).strip()
            )

    # Filter: exclude CUSTCD in (77,78,95,96)
    df = df[~df["CUSTCD"].isin([77, 78, 95, 96])].copy()

    # Filter: exclude MYR
    df = df[df["CURCODE"] != "MYR"].copy()

    # Filter: MATDATE must not be missing
    df = df[df["MATDATE"].notna()].copy()

    # FORCE NUMERIC TYPES (FIX)
    df["CURBAL"] = pd.to_numeric(df["CURBAL"], errors="coerce")
    df["FORATE"] = pd.to_numeric(df["FORATE"], errors="coerce")

    # REMOVE INVALID DATA
    df = df[df["FORATE"].notna() & (df["FORATE"] != 0)].copy()

    # SAFE DIVISION
    df["CURBAL"] = df["CURBAL"] / df["FORATE"]

    # Parse MATDATE -> Python date
    df["MATDT"] = df["MATDATE"].apply(_parse_matdate)
    df = df[df["MATDT"].notna()].copy()

    fcyt1 = df[df["MATDT"] == T1].copy()
    fcyt2 = df[df["MATDT"] == T2].copy()
    fcyt3 = df[df["MATDT"] == T3].copy()
    fcyt4 = df[df["MATDT"] >  T3].copy()

    return fcyt1, fcyt2, fcyt3, fcyt4


# ============================================================
# STEP 3 : PROC SUMMARY — summarise each FCYT bucket by CURCODE
#
#   CLASS CURCODE; VAR CURBAL RATE;
#   OUTPUT SUM=CURBAL<n> RATE<n>; RENAME _FREQ_=N<n>
# ============================================================

def summarise_bucket(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    """
    Equivalent of PROC SUMMARY NWAY by CURCODE for one maturity bucket.
    Returns a DataFrame with columns: CURCODE, CURBAL<n>, RATE<n>, N<n>.
    """
    if df.empty:
        return pd.DataFrame(columns=["CURCODE",
                                     f"CURBAL{suffix}",
                                     f"RATE{suffix}",
                                     f"N{suffix}"])
    grp = df.groupby("CURCODE", as_index=False).agg(
        **{f"CURBAL{suffix}": ("CURBAL", "sum"),
           f"RATE{suffix}":   ("RATE",   "sum"),
           f"N{suffix}":      ("CURBAL", "count")}
    )
    return grp


# ============================================================
# STEP 4 : DATA FCYT — merge four buckets and compute averages
#
#   MERGE FCYT1 FCYT2 FCYT3 FCYT4; BY CURCODE;
#   AVGRATE<n> = RATE<n> / N<n>;
#   AVGFIN<n>  = (retained sum of AVGRATE<n>) / 10  — see note below
#   AVGFIN     = SUM(AVGFIN1..4)
#   TOTFD      = SUM(CURBAL1,CURBAL2,CURABL3,CURBAL4)  ← SAS typo: CURABL3 = 0
#   TOTAVG     = SUM(AVGRATE1..4) / 4
#
# NOTE: AVGFIN variables are not printed in the DATA _NULL_ output; they are only
#       declared with FORMAT. The retain (+) accumulator affects their values
#       across rows but since they are unused in output, we compute them faithfully
#       for completeness but they do not appear in the text file.
# ============================================================

def build_fcyt_summary(s1: pd.DataFrame, s2: pd.DataFrame,
                       s3: pd.DataFrame, s4: pd.DataFrame) -> pd.DataFrame:
    """Merge four summary buckets and compute derived columns."""
    # Full outer merge by CURCODE (SAS MERGE semantics: last-dataset-wins for matching rows)
    merged = s1.merge(s2, on="CURCODE", how="outer")
    merged = merged.merge(s3, on="CURCODE", how="outer")
    merged = merged.merge(s4, on="CURCODE", how="outer")

    # Fill missing numeric values with 0 (SAS OPTIONS MISSING=0)
    num_cols = [c for c in merged.columns if c != "CURCODE"]
    merged[num_cols] = merged[num_cols].fillna(0)

    # AVGRATE<n> = RATE<n> / N<n>  (guard against div-by-zero)
    for n in ["1", "2", "3", "4"]:
        rate_col = f"RATE{n}"
        freq_col = f"N{n}"
        avg_col  = f"AVGRATE{n}"
        merged[avg_col] = merged.apply(
            lambda r: r[rate_col] / r[freq_col] if r[freq_col] != 0 else 0,
            axis=1
        )

    # AVGFIN<n>: SAS retain (+) then /10 each row.
    # With MISSING=0 and one row per currency, for the first currency row:
    #   AVGFIN<n> starts at 0, += AVGRATE<n>, then /= 10  => AVGRATE<n>/10
    # For subsequent rows the retained value carries forward (divided by 10 each time).
    # These columns are NOT output to the text file; computed for SAS fidelity.
    for n in ["1", "2", "3", "4"]:
        retain = 0.0
        avgfin_vals = []
        for _, row in merged.iterrows():
            retain = retain + row[f"AVGRATE{n}"]
            retain = retain / 10.0
            avgfin_vals.append(retain)
        merged[f"AVGFIN{n}"] = avgfin_vals

    merged["AVGFIN"] = (merged["AVGFIN1"] + merged["AVGFIN2"] +
                        merged["AVGFIN3"] + merged["AVGFIN4"])

    # TOTFD: SAS typo CURABL3 treated as 0 (MISSING=0); CURBAL3 intentionally omitted
    # to faithfully replicate: TOTFD = SUM(CURBAL1, CURBAL2, CURABL3, CURBAL4)
    merged["TOTFD"] = merged["CURBAL1"] + merged["CURBAL2"] + 0 + merged["CURBAL4"]

    # TOTAVG = SUM(AVGRATE1,AVGRATE2,AVGRATE3,AVGRATE4) / 4
    merged["TOTAVG"] = (merged["AVGRATE1"] + merged["AVGRATE2"] +
                        merged["AVGRATE3"] + merged["AVGRATE4"]) / 4.0

    # Sort ascending by CURCODE (SAS MERGE BY CURCODE implies sorted order)
    merged = merged.sort_values("CURCODE").reset_index(drop=True)

    return merged


# ============================================================
# STEP 5 : DATA _NULL_ FILE FDTEXT — write summary report
#
#   RECFM=FB, LRECL=320  (no ASA carriage control)
#   Comma-delimited columns at fixed @-positions
# ============================================================

def _pad(val, width: int) -> str:
    """Left-justify a value in a fixed-width field."""
    return str(val)[:width].ljust(width)


def _fmt_num(val, total_width: int, decimals: int = 2) -> str:
    """
    Format a numeric value right-justified in a fixed-width field.
    Replicates SAS 15.2 format behaviour.
    """
    try:
        formatted = f"{float(val):<{total_width}.{decimals}f}"
        return formatted[:total_width]
    except (ValueError, TypeError):
        return " " * total_width


def write_fdtext(fcyt: pd.DataFrame, repdt: str) -> None:
    """
    DATA _NULL_ FILE FDTEXT equivalent.

    Column layout (1-based @-positions from SAS PUT statements):
      Header line 1: 'FCYFD MATURITY BY CURRENCY AS AT ' + repdt  @001
      Header line 2: ',' at @001,@015,@030,@050,'MATURITY' at @050,',' at @074
      Header line 3: column sub-headers
      Header line 4: column labels
      Data lines    : one per CURCODE

    All lines are padded to LRECL=320.
    """
    LRECL = 320

    def build_line(spec: list) -> str:
        """
        spec: list of (1-based col position, value_str) tuples.
        Assembles a line by placing each value starting at the given position.
        """
        buf = [" "] * LRECL
        for pos1, val in spec:
            idx = pos1 - 1
            for i, ch in enumerate(val):
                if idx + i < LRECL:
                    buf[idx + i] = ch
        return "".join(buf)

    lines = []

    # IF _N_ = 1: header block
    hdr1 = build_line([(1, f"FCYFD MATURITY BY CURRENCY AS AT {repdt}")])
    lines.append(hdr1)

    hdr2 = build_line([
        (1,  ","),
        (15, ","),
        (30, ","),
        (50, "MATURITY"),
        (74, ","),
    ])
    lines.append(hdr2)

    hdr3 = build_line([
        (1,   ","),
        (15,  "T"),
        (49,  ","),
        (50,  "T1"),
        (99,  ","),
        (100, "T2"),
        (149, ","),
        (150, ">T2"),
    ])
    lines.append(hdr3)

    hdr4 = build_line([
        (1,   "CURRENCY"),
        (14,  ","),
        (15,  "AMOUNT"),
        (29,  ","),
        (30,  "AVG RATE"),
        (49,  ","),
        (50,  "AMOUNT"),
        (74,  ","),
        (75,  "AVG RATE"),
        (99,  ","),
        (100, "AMOUNT"),
        (124, ","),
        (125, "AVG RATE"),
        (149, ","),
        (150, "AMOUNT"),
        (174, ","),
        (175, "AVG RATE"),
        (199, ","),
        (200, "TOTAL FD"),
        (224, ","),
        (225, "AVG RATE"),
        (249, ","),
    ])
    lines.append(hdr4)

    # Data rows
    for _, row in fcyt.iterrows():
        curcode  = str(row["CURCODE"]).strip()
        curbal1  = float(row.get("CURBAL1",  0) or 0)
        avgrate1 = float(row.get("AVGRATE1", 0) or 0)
        curbal2  = float(row.get("CURBAL2",  0) or 0)
        avgrate2 = float(row.get("AVGRATE2", 0) or 0)
        curbal3  = float(row.get("CURBAL3",  0) or 0)
        avgrate3 = float(row.get("AVGRATE3", 0) or 0)
        curbal4  = float(row.get("CURBAL4",  0) or 0)
        avgrate4 = float(row.get("AVGRATE4", 0) or 0)
        totfd    = float(row.get("TOTFD",    0) or 0)
        totavg   = float(row.get("TOTAVG",   0) or 0)

        # SAS format 15.2 right-justified (total width 15, 2 decimals)
        data_line = build_line([
            (1,   curcode),
            (14,  ","),
            (15,  _fmt_num(curbal1,  15, 2)),
            (29,  ","),
            (30,  _fmt_num(avgrate1, 15, 2)),
            (49,  ","),
            (50,  _fmt_num(curbal2,  15, 2)),
            (74,  ","),
            (75,  _fmt_num(avgrate2, 15, 2)),
            (99,  ","),
            (100, _fmt_num(curbal3,  15, 2)),
            (124, ","),
            (125, _fmt_num(avgrate3, 15, 2)),
            (149, ","),
            (150, _fmt_num(curbal4,  15, 2)),
            (174, ","),
            (175, _fmt_num(avgrate4, 15, 2)),
            (199, ","),
            (200, _fmt_num(totfd,    15, 2)),
            (224, ","),
            (225, _fmt_num(totavg,   15, 2)),
            (249, ","),
        ])
        lines.append(data_line)

    with open(OUTPUT_FDTEXT, "w", encoding="latin1") as fh:
        for line in lines:
            fh.write(line + "\n")

    print(f"[FDTEXT] Written {len(lines)} lines -> {OUTPUT_FDTEXT}")
    # Print to terminal
    for line in lines:
        print(line)


# ============================================================
# STEP 6 : DATA CUST — detail dataset for customers > FCY 1M
#
#   KEEP=ORIGAMT ORGDATE MATDATE NAME CURCODE RATE CURBAL CUSTCD BRANCH
#   IF CUSTCD NOT IN (77,78,95,96);
#   IF CURCODE = 'MYR' THEN DELETE;
#   IF CURBAL > 1000000;
#   IF MATDATE = . THEN DELETE;
#   BRCHCD = PUT(BRANCH,BRCHCD.);
#   Sort by DESCENDING CURCODE DESCENDING CURBAL
#   Derive MATURE (DDMMYY8.) and ORIGIN (DDMMYY8.)
# ============================================================

def build_cust(df: pd.DataFrame) -> pd.DataFrame:
    """Build the CUST detail dataset."""
    keep_cols = ["ORIGAMT", "ORGDATE", "MATDATE", "NAME",
                 "CURCODE", "RATE", "CURBAL", "CUSTCD", "BRANCH"]
    # Keep only columns that exist in the dataset
    available = [c for c in keep_cols if c in df.columns]
    cust = df[available].copy()

    # Decode bytes
    for col in ["CURCODE", "NAME"]:
        if col in cust.columns and cust[col].dtype == object:
            cust[col] = cust[col].apply(
                lambda x: x.decode("latin1").strip() if isinstance(x, bytes) else str(x).strip()
            )

    # Filters
    cust = cust[~cust["CUSTCD"].isin([77, 78, 95, 96])].copy()
    cust = cust[cust["CURCODE"] != "MYR"].copy()
    cust = cust[cust["CURBAL"] > 1_000_000].copy()
    cust = cust[cust["MATDATE"].notna()].copy()

    # BRCHCD format
    cust["BRCHCD"] = cust["BRANCH"].apply(
        lambda x: format_brchcd(int(x)) if pd.notna(x) else ""
    )

    # Parse MATDATE -> MATURE (DDMMYY8.)
    cust["MATDT"]  = cust["MATDATE"].apply(_parse_matdate)
    cust["MATURE"] = cust["MATDT"].apply(_fmt_date_ddmmyy8)

    # Parse ORGDATE -> ORIGIN (DDMMYY8.)
    cust["ORGDAT"] = cust["ORGDATE"].apply(_parse_orgdate)
    cust["ORIGIN"] = cust["ORGDAT"].apply(_fmt_date_ddmmyy8)

    # Sort: DESCENDING CURCODE, DESCENDING CURBAL
    cust = cust.sort_values(["CURCODE", "CURBAL"], ascending=[False, False]).reset_index(drop=True)

    return cust


# ============================================================
# STEP 7 : DATA _NULL_ FILE FDDETL — write detail report
#
#   RECFM=FB, LRECL=320  (no ASA carriage control)
# ============================================================

def write_fddetl(cust: pd.DataFrame, repdt: str) -> None:
    """
    DATA _NULL_ FILE FDDETL equivalent.

    Column layout (@-positions, 1-based):
      Header 1: title
      Header 2: column labels
      Data:     one row per customer
    """
    LRECL = 320

    def build_line(spec: list) -> str:
        buf = [" "] * LRECL
        for pos1, val in spec:
            idx = pos1 - 1
            for i, ch in enumerate(val):
                if idx + i < LRECL:
                    buf[idx + i] = ch
        return "".join(buf)

    lines = []

    # IF _N_ = 1: header block
    hdr1 = build_line([(1, f"FCYFD DETAILS BY CUSTOMER(>FCY1MIL) AS AT {repdt}")])
    lines.append(hdr1)

    hdr2 = build_line([
        (1,   "BRANCH"),
        (10,  ","),
        (11,  "CUSTOMER"),
        (59,  ","),
        (60,  "SETTLEMENT AMT"),
        (84,  ","),
        (85,  "AVG RATE"),
        (109, ","),
        (110, "CURRENCY"),
        (134, ","),
        (135, "SETTLEMENT DATE"),
        (159, ","),
        (160, "MATURITY"),
        (168, ","),
    ])
    lines.append(hdr2)

    # Data rows
    for _, row in cust.iterrows():
        brchcd  = str(row.get("BRCHCD", "")).strip()
        name    = str(row.get("NAME",   "")).strip()
        origamt = float(row.get("ORIGAMT", 0) or 0)
        rate    = float(row.get("RATE",    0) or 0)
        curcode = str(row.get("CURCODE", "")).strip()
        origin  = str(row.get("ORIGIN",  "")).strip()
        mature  = str(row.get("MATURE",  "")).strip()

        data_line = build_line([
            (1,   brchcd[:9]),
            (10,  ","),
            (11,  name[:48]),
            (59,  ","),
            (60,  _fmt_num(origamt, 15, 2)),
            (84,  ","),
            (85,  _fmt_num(rate,    15, 2)),
            (109, ","),
            (110, curcode[:24]),
            (134, ","),
            (135, origin[:24]),
            (159, ","),
            (160, mature[:8]),
            (170, ","),
        ])
        lines.append(data_line)

    with open(OUTPUT_FDDETL, "w", encoding="latin1") as fh:
        for line in lines:
            fh.write(line + "\n")

    print(f"[FDDETL] Written {len(lines)} lines -> {OUTPUT_FDDETL}")
    # Print to terminal
    for line in lines:
        print(line)


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    print(f"[EIBDPTER] Report date : {REPDT}")
    print(f"[EIBDPTER] T  = {REPTDATE}")
    print(f"[EIBDPTER] T1 = {T1}")
    print(f"[EIBDPTER] T2 = {T2}")
    print(f"[EIBDPTER] T3 = {T3}")

    # --- Read FD dataset -----------------------------------------------
    print(f"[EIBDPTER] Reading {FD_CERT} ...")
    fd_df = read_fd()

    # --- FCY maturity split --------------------------------------------
    fcyt1, fcyt2, fcyt3, fcyt4 = build_fcyt(fd_df)
    print(f"[EIBDPTER] Maturity split: T1={len(fcyt1)}, T2={len(fcyt2)}, "
          f"T3={len(fcyt3)}, >T3={len(fcyt4)}")

    # --- PROC SUMMARY per bucket ---------------------------------------
    s1 = summarise_bucket(fcyt1, "1")
    s2 = summarise_bucket(fcyt2, "2")
    s3 = summarise_bucket(fcyt3, "3")
    s4 = summarise_bucket(fcyt4, "4")

    # --- Merge and derive averages ------------------------------------
    fcyt = build_fcyt_summary(s1, s2, s3, s4)

    # --- Write FDTEXT (summary) ----------------------------------------
    write_fdtext(fcyt, REPDT)

    # --- Build CUST detail dataset -------------------------------------
    cust = build_cust(fd_df)
    print(f"[EIBDPTER] Customers with FCY balance > 1M: {len(cust)}")

    # --- Write FDDETL (detail) -----------------------------------------
    write_fddetl(cust, REPDT)

    print(f"[EIBDPTER] Output FDTEXT : {OUTPUT_FDTEXT}")
    print(f"[EIBDPTER] Output FDDETL : {OUTPUT_FDDETL}")
    print("[EIBDPTER] Done.")


if __name__ == "__main__":
    main()
