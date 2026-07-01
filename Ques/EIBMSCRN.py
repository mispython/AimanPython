#!/usr/bin/env python3
"""
Program : EIBMSCRN.py
Purpose : Staff sales-recognition (SRS) summary for credit card, deposit
          (DP_SRR1/DP_SCRFD) and ELDS staff-attendance records. Matches
          staff to branch (BRSTAF.txt) and head-office (HOSTAF.txt) staff
          listings, derives category (CAT.1 / CAT.2 CORE / CAT.2 NON-CORE)
          counts and balances per staff/product, and produces:
            - SCRD.SRSBR / SCRD.SRSHO  (PROC SUMMARY outputs, kept as
              Parquet for downstream consumption)
            - Exception listing (unmatched staff against HR file)
            - Final SRSP summary print (semicolon line dump + PROC PRINT)

Amendment history (from original SAS):
- Clone of EIBMSCRA, scheduled to run on 6th at 10:00 AM (ELDS ready 6 AM).

Note on dependencies:
    %INC PGM(PBMISFMT,PBBDPFMT) is a session-level include in the original
    SAS job. No PUT(var, fmt.) call in the program body actually invokes a
    PBMISFMT or PBBDPFMT format, so per conversion convention these are
    NOT imported as live modules here -- only documented as a placeholder.
    # from PBMISFMT import format_brchcd        # NOT directly called in body
    # from PBBDPFMT import sadenom_format       # NOT directly called in body

Note on HRRG:
    The DATA HRRG step (reading RGSTAF.txt) and the consolidated
    DATA LOCAT step were commented out (/* ... */) in the original SAS
    source. They are preserved below as comments only.
"""

import sys
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

import polars as pl
import duckdb

from REPTDATE import get_reptdate_values
from input_date import get_latest_file
from output_date import build_output_file

# ===========================================================================
# PATH CONFIGURATION
# ===========================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
FTP_DIR  = Path("/stgsrcsys/host/uat")

INPUT_DIR = BASE_DIR / "input" / "prod" / "EIBMSCRN"
OUTPUT_DIR = BASE_DIR / "output" / "EIBMSCRN"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# BRHFI - flat file, named LKP_BRANCH
# LKP_BRANCH_FILE = INPUT_DIR / "LKP_BRANCH"
LKP_BRANCH_FILE = Path("/sasdata/rawdata/lookup") / "LKP_BRANCH"

# ELDS - txt file
# ELDS_FILE = INPUT_DIR / "ELDS_SRR.txt"
ELDS_FILE = FTP_DIR / "ELDS_SRR.txt"

# DEP - 2 txt files (concatenated, in this order)
# DEP_FILES = [INPUT_DIR / "DP_SRR1.txt", INPUT_DIR / "DP_SCRFD.txt"]
DEP_FILES = [FTP_DIR / "DP_SRR1.txt", FTP_DIR / "DP_SCRFD.txt"]

# CARDFILE - 2 txt files (concatenated, in this order)
# CARD_FILES = [INPUT_DIR / "ACCT33.txt", INPUT_DIR / "ACCT55.txt"]
CARD_FILES = [FTP_DIR / "ACCT33.txt", FTP_DIR / "ACCT55.txt"]

# HRBR, HRHO, HRRG - text file inputs
# HRBR_FILE = Path("/stgsrcsys/host/ftpfiles") / "BRSTAF.txt"
# HRHO_FILE = Path("/stgsrcsys/host/ftpfiles") / "HOSTAF.txt"
# # HRRG_FILE = Path("/stgsrcsys/host/ftpfiles") / "RGSTAF.txt"   # DATA HRRG step commented out in SAS
HRBR_FILE = INPUT_DIR / "BRSTAF.TXT"
HRHO_FILE = INPUT_DIR / "HOSTAF.TXT"
# HRRG_FILE = Path("/stgsrcsys/host/ftpfiles") / "RGSTAF.txt"   # DATA HRRG step commented out in SAS

CHUNK_SIZE = 500_000   # rows per chunk for large fixed-width files

# ===========================================================================
# REPORT DATE (replaces DATA _NULL_; SET MNITB.REPTDATE;)
# ===========================================================================
reptdate_values = get_reptdate_values()
RDATE = reptdate_values.rdate.strftime("%d/%m/%Y")
RMM = reptdate_values.reptmon
RYY = reptdate_values.reptyear
RYEAR = reptdate_values.rdate.strftime("%Y")
RMON = reptdate_values.rdate.strftime("%b%Y").upper()

PAGE_SIZE = 60


# ===========================================================================
# FIXED-WIDTH COLUMN SPECIFICATIONS (1-based SAS @col converted to 0-based)
# ===========================================================================
# Each spec: (name, start0, end0, kind) kind in {'str', 'int', 'dec2'}
# 'dec2' = implied 2-decimal informat (w.2): divide raw integer by 100.

BRH_SPEC = [
    ("BRANCH", 1, 4, "int"),
    ("BRCHCD", 5, 8, "str"),
]

HRBR_SPEC = [
    ("STAFF", 4, 9, "int"),
    ("BRANCH", 68, 71, "int"),
    ("BRCHCD", 71, 74, "str"),
]

HRHO_SPEC = [
    ("STAFF", 4, 9, "int"),
    ("HOE", 71, 86, "str"),
]

# HRRG_SPEC (commented out in original SAS, kept as placeholder)
# HRRG_SPEC = [
#     ("STAFF", 4, 9, "int"),
#     ("REGN", 71, 86, "str"),
# ]

# FIX 1 — CARD_SPEC: correct SAS @col positions (1-based -> 0-based)
CARD_SPEC = [
    ("ACCTNO", 0,   11,  "int"),   # @001  11.
    ("OPNMM",  13,  15,  "int"),   # @014   2.
    ("OPNYR",  15,  17,  "int"),   # @016   2.
    ("BRANC",  556, 560, "int"),   # @557   4.
    ("SECO",   560, 572, "str"),   # @561  $12.
]

# FIX 2 — DEP_SPEC: correct SAS @col positions (1-based -> 0-based)
DEP_SPEC = [
    ("ACCTNO",  1,   11,  "int"),   # @002  10.
    ("PRIMOFF", 49,  54,  "int"),   # @050   5.
    ("SECNOFF", 55,  60,  "int"),   # @056   5.
    ("XCORE",   60,  61,  "str"),   # @061  $1.
    ("AVGBAL",  61,  75,  "dec2"),  # @062  14.2
    ("YTDBALS", 75,  89,  "dec2"),  # @076  14.2
    ("NUMMTH",  89,  91,  "int"),   # @090   2.
    ("BRANCH",  96,  99,  "int"),   # @097   3.
]

# FIX 3 — ELDS_SPEC: correct SAS @col positions (1-based -> 0-based)
ELDS_SPEC = [
    ("AANUM",   0,  13, "str"),    # @001  $13.
    ("STAFX1",  16, 21, "str"),    # @017   $5.
    ("STAFX2",  25, 30, "str"),    # @026   $5.
    ("STAFX3",  34, 39, "str"),    # @035   $5.
    ("PRODUCT", 50, 68, "str"),    # @051  $18.
    ("YTDBAL",  68, 80, "int"),    # @069  12.
]


# ===========================================================================
# GENERIC FIXED-WIDTH PARSER
# ===========================================================================
def _parse_line(line: str, specs: List[Tuple[str, int, int, str]]) -> dict:
    """Slice one fixed-width record according to specs."""
    row = {}
    for name, start, end, kind in specs:
        raw = line[start:end] if len(line) >= start else ""
        raw = raw.strip()
        if kind == "str":
            row[name] = raw
        elif kind == "int":
            row[name] = int(raw) if raw.lstrip("-").isdigit() else None
        elif kind == "dec2":
            row[name] = int(raw) / 100.0 if raw.lstrip("-").isdigit() else None
    return row


# FIX 7 — parse_fixed_chunk: build column-oriented to suppress DataOrientationWarning
def parse_fixed_chunk(lines: List[str], specs) -> pl.DataFrame:
    schema = {
        name: pl.Utf8 if kind == "str"
        else pl.Float64 if kind == "dec2"
        else pl.Int64
        for name, _, _, kind in specs
    }

    if not lines:
        return pl.DataFrame(schema=schema)

    # Build column-oriented dict to avoid row-orientation inference warning
    col_data: dict = {name: [] for name, *_ in specs}
    for line in lines:
        row = _parse_line(line, specs)
        for name, *_ in specs:
            col_data[name].append(row[name])

    return pl.DataFrame(col_data, schema=schema)


def iter_fixed_chunks(paths: List[Path], specs, chunk_size: int = CHUNK_SIZE):
    """Stream fixed-width records across one or more concatenated files."""
    for path in paths:
        if not path.exists():
            continue
        buffer: List[str] = []
        with open(path, "r", encoding="latin1") as f:
            for line in f:
                buffer.append(line.rstrip("\n"))
                if len(buffer) >= chunk_size:
                    yield parse_fixed_chunk(buffer, specs)
                    buffer = []
        if buffer:
            yield parse_fixed_chunk(buffer, specs)


def read_fixed_whole(path: Path, specs) -> pl.DataFrame:
    """Read a (small) fixed-width file in one shot."""
    if not path.exists():
        return parse_fixed_chunk([], specs)
    with open(path, "r", encoding="latin1") as f:
        lines = [line.rstrip("\n") for line in f]
    return parse_fixed_chunk(lines, specs)


# ===========================================================================
# SMALL LOOKUP TABLES: BRH, HRBR, HRHO
# ===========================================================================
def load_brh() -> pl.DataFrame:
    """DATA BRH; INFILE BRHFI; INPUT BRANCH 3. BRCHCD $3.;"""
    return read_fixed_whole(LKP_BRANCH_FILE, BRH_SPEC)


# FIX 4 — load_hrbr: use spec-based reader at correct byte positions [4:9], [68:71], [71:74]
# Also re-adds the missing IF (002<=BRANCH<=267) filter from the SAS source.
def load_hrbr() -> pl.DataFrame:
    """DATA HRBR; INPUT @005 STAFF 5. @069 BRANCH 3. @072 BRCHCD $3.;
    IF (002<=BRANCH<=267);"""
    df = read_fixed_whole(HRBR_FILE, HRBR_SPEC)
    if df.height == 0:
        return df
    return df.filter(
        pl.col("BRANCH").is_not_null()
        & (pl.col("BRANCH") >= 2)
        & (pl.col("BRANCH") <= 267)
    )


# FIX 5 — load_hrho: use spec-based reader at correct byte positions [4:9], [71:86]
def load_hrho() -> pl.DataFrame:
    """DATA HRHO; INPUT @005 STAFF 5. @072 HOE $15.;"""
    return read_fixed_whole(HRHO_FILE, HRHO_SPEC)


# DATA HRRG step is commented out in the original SAS source:
#   DATA HRRG;
#      INFILE HRRG;
#      INPUT @005 STAFF 5. @072 REGN $15.;
#      BRANCH=000; BRCHCD='   '; LOCT='RG';
#   DATA LOCAT;
#      SET HRBR HRHO HRRG;


# ===========================================================================
# UCARD (CARDFILE): credit-card record extraction
# ===========================================================================
def _typec_from_acctno(acctno: Optional[int]) -> Optional[int]:
    """
    TYPEC=SUBSTR(ACCTNO,1,5);
    ACCTNO is numeric (informat 11.), so SAS implicitly converts it to its
    default character representation (leading zeros dropped) before taking
    the substring, then implicitly converts the 5-char substring back to
    numeric for the IN() comparison. This quirk is preserved faithfully.
    """
    if acctno is None:
        return None
    s = str(int(acctno))
    if len(s) < 5:
        return None
    head = s[:5]
    return int(head) if head.isdigit() else None


_UCARD_TYPES = {3301, 3302, 3305, 3306, 3308, 3309, 5503, 5504}


def transform_ucard_chunk(chunk: pl.DataFrame) -> pl.DataFrame:
    """Apply DATA UCARD logic to a single chunk of CARDFILE records."""
    if chunk.height == 0:
        return chunk

    chunk = chunk.with_columns(
        pl.col("ACCTNO").map_elements(_typec_from_acctno, return_dtype=pl.Int64).alias("TYPEC")
    )
    chunk = chunk.filter(pl.col("TYPEC").is_in(_UCARD_TYPES))
    if chunk.height == 0:
        return chunk

    # IF (OPNYR=06 AND OPNMM>08) OR OPNYR GE 07;
    chunk = chunk.filter(
        ((pl.col("OPNYR") == 6) & (pl.col("OPNMM") > 8)) | (pl.col("OPNYR") >= 7)
    )
    if chunk.height == 0:
        return chunk

    # IF (1<LENGTH(SECO)<6);  -- SAS LENGTH() of blank char returns 1
    def _seco_len(s):
        s = (s or "").strip()
        return 1 if s == "" else len(s)

    chunk = chunk.with_columns(
        pl.col("SECO").map_elements(_seco_len, return_dtype=pl.Int64).alias("_SECOLEN")
    )
    chunk = chunk.filter((pl.col("_SECOLEN") > 1) & (pl.col("_SECOLEN") < 6))
    if chunk.height == 0:
        return chunk

    # STAFF8=SECO (char->num implicit conversion); IF (1<STAFF8<99999);
    def _to_int_or_none(s):
        s = (s or "").strip()
        try:
            return int(s)
        except ValueError:
            return None

    chunk = chunk.with_columns(
        pl.col("SECO").map_elements(_to_int_or_none, return_dtype=pl.Int64).alias("STAFF8")
    )
    chunk = chunk.filter(
        pl.col("STAFF8").is_not_null() & (pl.col("STAFF8") > 1) & (pl.col("STAFF8") < 99999)
    )
    if chunk.height == 0:
        return chunk

    chunk = chunk.with_columns(
        [
            pl.lit("CARD           ").alias("PRODUCT"),
            pl.col("STAFF8").alias("STAFF"),
            pl.lit(0.0).alias("C1BAL"),
            pl.lit(0.0).alias("C2BAL"),
            pl.lit(0.0).alias("C3BAL"),
            pl.lit(0).alias("C1CNT"),
            pl.lit(0).alias("C2CNT"),
            pl.when(pl.col("TYPEC") == 3309).then(1).otherwise(0).alias("C4CNT"),
            pl.when(pl.col("TYPEC") == 3309).then(0).otherwise(1).alias("C5CNT"),
            pl.lit("N").alias("NCORE"),
        ]
    )
    return chunk.select(
        ["STAFF", "PRODUCT", "NCORE", "C1CNT", "C2CNT", "C3CNT" if "C3CNT" in chunk.columns else "C1CNT"]
        if False else ["STAFF", "PRODUCT", "NCORE", "C1CNT", "C2CNT", "C4CNT", "C5CNT", "C1BAL", "C2BAL", "C3BAL"]
    ).with_columns(pl.lit(0).alias("C3CNT")).select(
        ["STAFF", "PRODUCT", "NCORE", "C1CNT", "C2CNT", "C3CNT", "C4CNT", "C5CNT", "C1BAL", "C2BAL", "C3BAL"]
    )


def build_ucard() -> pl.DataFrame:
    """Stream CARDFILE (ACCT33.txt, ACCT55.txt) in chunks and filter."""
    parts = []
    for chunk in iter_fixed_chunks(CARD_FILES, CARD_SPEC):
        transformed = transform_ucard_chunk(chunk)
        if transformed.height > 0:
            parts.append(transformed)
    if not parts:
        return pl.DataFrame(
            schema={
                "STAFF": pl.Int64, "PRODUCT": pl.Utf8, "NCORE": pl.Utf8,
                "C1CNT": pl.Int64, "C2CNT": pl.Int64, "C3CNT": pl.Int64,
                "C4CNT": pl.Int64, "C5CNT": pl.Int64,
                "C1BAL": pl.Float64, "C2BAL": pl.Float64, "C3BAL": pl.Float64,
            }
        )
    return pl.concat(parts, how="vertical_relaxed")


# ===========================================================================
# UCBR / UCEXC / UCHO / UCEXC1  (merge UCARD against HRBR, HRHO)
# ===========================================================================
def split_ucard_against_hrbr(ucard: pl.DataFrame, hrbr: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    DATA UCBR UCEXC;
       MERGE HRBR(IN=A) UCARD(IN=B); BY STAFF;
       IF A AND B     THEN OUTPUT UCBR;
       IF B AND NOT A THEN OUTPUT UCEXC;
    """

    if hrbr.height == 0:
        empty_ucbr = ucard.head(0).with_columns([
            pl.lit(None).cast(pl.Int64).alias("BRANCH"),
            pl.lit(None).cast(pl.Utf8).alias("BRCHCD"),
        ])
        return empty_ucbr, ucard

    hrbr_keys = hrbr.select(["STAFF", "BRANCH", "BRCHCD"]).unique(subset=["STAFF"], keep="first")
    ucbr = ucard.join(hrbr_keys, on="STAFF", how="inner")
    matched_staff = hrbr_keys.select("STAFF")
    ucexc = ucard.join(matched_staff, on="STAFF", how="anti")
    return ucbr, ucexc


def split_ucexc_against_hrho(ucexc: pl.DataFrame, hrho: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    DATA UCHO UCEXC1;
       MERGE HRHO(IN=A) UCEXC(IN=B); BY STAFF;
       IF A AND B     THEN OUTPUT UCHO;
       IF B AND NOT A THEN OUTPUT UCEXC1;
    """
    hrho_keys = hrho.select(["STAFF", "HOE"]).unique(subset=["STAFF"], keep="first")
    ucho = ucexc.join(hrho_keys, on="STAFF", how="inner")
    matched_staff = hrho_keys.select("STAFF")
    ucexc1 = ucexc.join(matched_staff, on="STAFF", how="anti")
    return ucho, ucexc1


# ===========================================================================
# DEP processing (DP_SRR1.txt, DP_SCRFD.txt) -> DPC1, DPC2
# ===========================================================================
def _dep_product(acctno: Optional[int]) -> Optional[str]:
    if acctno is None:
        return None
    if 1_000_000_000 <= acctno <= 1_999_999_999:
        return "FIXED DEPOSITS    "
    if 7_000_000_000 <= acctno <= 7_999_999_999:
        return "FIXED DEPOSITS    "
    if 3_000_000_000 <= acctno <= 3_999_999_999:
        return "CURRENT ACCOUNT   "
    if 4_000_000_000 <= acctno <= 4_999_999_999:
        return "SAVING ACCOUNT    "
    if 6_000_000_000 <= acctno <= 6_999_999_999:
        return "SAVING ACCOUNT    "
    return None


def transform_dep_chunk(chunk: pl.DataFrame, brh: pl.DataFrame) -> pl.DataFrame:
    """DATA DEP; ... ; merge with BRH BY BRANCH; IF A (DEP-originated rows)."""
    if chunk.height == 0:
        return chunk

    chunk = chunk.with_columns(
        pl.col("ACCTNO").map_elements(_dep_product, return_dtype=pl.Utf8).alias("PRODUCT")
    )
    # YTDBAL = ROUND(YTDBALS/NUMMTH)
    chunk = chunk.with_columns(
        pl.when(pl.col("NUMMTH").is_not_null() & (pl.col("NUMMTH") != 0))
        .then((pl.col("YTDBALS") / pl.col("NUMMTH")).round(0))
        .otherwise(None)
        .alias("YTDBAL")
    )
    chunk = chunk.with_columns(
        [
            pl.lit("DEPO").alias("TAG"),
            pl.lit(0.0).alias("C1BAL"),
            pl.lit(0.0).alias("C2BAL"),
            pl.lit(0.0).alias("C3BAL"),
            pl.lit(0).alias("C1CNT"),
            pl.lit(0).alias("C2CNT"),
            pl.lit(0).alias("C3CNT"),
            pl.lit(0).alias("C4CNT"),
            pl.lit(0).alias("C5CNT"),
        ]
    )

    # MERGE BRH DEP(IN=A); BY BRANCH; IF A;  -> left join DEP -> BRH on BRANCH,
    # keep only rows originating from DEP (left join, BRCHCD nullable).
    brh_keys = brh.select(["BRANCH", "BRCHCD"]).unique(subset=["BRANCH"], keep="first")
    return chunk.join(brh_keys, on="BRANCH", how="left")


# FIX 6 — split_dep_into_dpc1_dpc2: XCORE blank comparison
# _parse_line strips all str fields, so SAS XCORE=' ' (space) becomes '' after strip.
# Filter must use "" (empty string) not " " (space).
def split_dep_into_dpc1_dpc2(dep_chunk: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    DATA DPC1; SET DEP; STAFF=PRIMOFF; IF STAFF IN (0,.) THEN DELETE;
       IF XCORE=' ';   <- SAS blank; after strip in Python this is ''
    DATA DPC2; SET DEP; ... IF XCORE IN ('C','N'); ...
    """
    if dep_chunk.height == 0:
        return dep_chunk, dep_chunk

    dpc1 = dep_chunk.with_columns(pl.col("PRIMOFF").alias("STAFF"))
    dpc1 = dpc1.filter(pl.col("STAFF").is_not_null() & (pl.col("STAFF") != 0))
    # SAS: IF XCORE=' ' -> blank field; _parse_line strips space to empty string
    dpc1 = dpc1.filter(pl.col("XCORE") == "")
    dpc1 = dpc1.with_columns(
        [
            pl.lit("X").alias("NCORE"),
            pl.lit("CAT.1         ").alias("CATG"),
            pl.lit(1).alias("C1CNT"),
            pl.col("YTDBAL").alias("C1BAL"),
        ]
    )

    dpc2 = dep_chunk.with_columns(
        pl.when(pl.col("SECNOFF").is_not_null() & (pl.col("SECNOFF") != 0))
        .then(pl.col("SECNOFF"))
        .otherwise(pl.col("PRIMOFF"))
        .alias("STAFF")
    )
    dpc2 = dpc2.filter(pl.col("STAFF").is_not_null() & (pl.col("STAFF") != 0))
    dpc2 = dpc2.filter(pl.col("XCORE").is_in(["C", "N"]))
    dpc2 = dpc2.with_columns(
        [
            pl.when(pl.col("XCORE") == "C")
            .then(pl.lit("CAT.2 CORE    "))
            .otherwise(pl.lit("CAT.2 NON-CORE"))
            .alias("CATG"),
            pl.when(pl.col("XCORE") == "C").then(pl.lit("C")).otherwise(pl.lit("N")).alias("NCORE"),
            pl.when(pl.col("XCORE") == "C").then(1).otherwise(0).alias("C2CNT"),
            pl.when(pl.col("XCORE") == "N").then(1).otherwise(0).alias("C3CNT"),
            pl.when(pl.col("XCORE") == "C").then(pl.col("YTDBAL")).otherwise(0.0).alias("C2BAL"),
            pl.when(pl.col("XCORE") == "N").then(pl.col("YTDBAL")).otherwise(0.0).alias("C3BAL"),
        ]
    )

    keep_cols = ["STAFF", "PRODUCT", "NCORE", "CATG", "TAG", "ACCTNO", "YTDBAL",
                 "C1CNT", "C2CNT", "C3CNT", "C4CNT", "C5CNT", "C1BAL", "C2BAL", "C3BAL"]
    return dpc1.select(keep_cols), dpc2.select(keep_cols)


def build_dpc1_dpc2(brh: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Stream DEP files (DP_SRR1.txt, DP_SCRFD.txt) in chunks."""
    dpc1_parts, dpc2_parts = [], []
    for chunk in iter_fixed_chunks(DEP_FILES, DEP_SPEC):
        dep_t = transform_dep_chunk(chunk, brh)
        dpc1, dpc2 = split_dep_into_dpc1_dpc2(dep_t)
        if dpc1.height > 0:
            dpc1_parts.append(dpc1)
        if dpc2.height > 0:
            dpc2_parts.append(dpc2)

    empty_schema = {
        "STAFF": pl.Int64, "PRODUCT": pl.Utf8, "NCORE": pl.Utf8, "CATG": pl.Utf8,
        "TAG": pl.Utf8, "ACCTNO": pl.Int64, "YTDBAL": pl.Float64,
        "C1CNT": pl.Int64, "C2CNT": pl.Int64, "C3CNT": pl.Int64,
        "C4CNT": pl.Int64, "C5CNT": pl.Int64,
        "C1BAL": pl.Float64, "C2BAL": pl.Float64, "C3BAL": pl.Float64,
    }
    dpc1_all = pl.concat(dpc1_parts, how="vertical_relaxed") if dpc1_parts else pl.DataFrame(schema=empty_schema)
    dpc2_all = pl.concat(dpc2_parts, how="vertical_relaxed") if dpc2_parts else pl.DataFrame(schema=empty_schema)
    return dpc1_all, dpc2_all


# ===========================================================================
# ELDS processing -> staff-attendance categorisation
# ===========================================================================
def _ncore_from_stafx(staf1: str, staf2: str, staf3: str) -> Optional[str]:
    def _in_range(v: str) -> bool:
        v = (v or "").strip()
        return v.isdigit() and "00001" <= v.zfill(5) <= "99999"

    if _in_range(staf1):
        return "X"
    if _in_range(staf2):
        return "C"
    if _in_range(staf3):
        return "N"
    return None


def transform_elds_chunk(chunk: pl.DataFrame, brh: pl.DataFrame) -> pl.DataFrame:
    """DATA ELDS; ... ; merge with BRH BY BRCHCD; IF A (ELDS-originated rows)."""
    if chunk.height == 0:
        return chunk

    chunk = chunk.filter(pl.col("STAFX1") != "*****")
    if chunk.height == 0:
        return chunk

    chunk = chunk.with_columns(
        pl.struct(["STAFX1", "STAFX2", "STAFX3"]).map_elements(
            lambda r: _ncore_from_stafx(r["STAFX1"], r["STAFX2"], r["STAFX3"]),
            return_dtype=pl.Utf8,
        ).alias("NCORE")
    )
    chunk = chunk.with_columns(
        [
            pl.lit("ELDS").alias("TAG"),
            pl.col("AANUM").str.slice(0, 3).alias("BRCHCD"),
            pl.lit(0.0).alias("C1BAL"),
            pl.lit(0.0).alias("C2BAL"),
            pl.lit(0.0).alias("C3BAL"),
            pl.lit(0).alias("C1CNT"),
            pl.lit(0).alias("C2CNT"),
            pl.lit(0).alias("C3CNT"),
            pl.lit(0).alias("C4CNT"),
            pl.lit(0).alias("C5CNT"),
        ]
    )

    # MERGE BRH ELDS(IN=A); BY BRCHCD; IF A;  -> left join ELDS -> BRH on BRCHCD
    brh_keys = brh.select(["BRCHCD", "BRANCH"]).unique(subset=["BRCHCD"], keep="first")
    chunk = chunk.join(brh_keys, on="BRCHCD", how="left")

    def _staff_for_ncore(ncore, s1, s2, s3) -> Optional[int]:
        try:
            if ncore == "X":
                return int(s1)
            if ncore == "C":
                return int(s2)
            if ncore == "N":
                return int(s3)
        except (TypeError, ValueError):
            return None
        return None

    chunk = chunk.with_columns(
        pl.struct(["NCORE", "STAFX1", "STAFX2", "STAFX3"]).map_elements(
            lambda r: _staff_for_ncore(r["NCORE"], r["STAFX1"], r["STAFX2"], r["STAFX3"]),
            return_dtype=pl.Int64,
        ).alias("STAFF")
    )
    chunk = chunk.with_columns(
        [
            pl.when(pl.col("NCORE") == "X").then(pl.lit("CAT.1         "))
            .when(pl.col("NCORE") == "C").then(pl.lit("CAT.2 CORE    "))
            .when(pl.col("NCORE") == "N").then(pl.lit("CAT.2 NON-CORE"))
            .otherwise(None).alias("CATG"),
            pl.when(pl.col("NCORE") == "X").then(1).otherwise(0).alias("C1CNT"),
            pl.when(pl.col("NCORE") == "C").then(1).otherwise(0).alias("C2CNT"),
            pl.when(pl.col("NCORE") == "N").then(1).otherwise(0).alias("C3CNT"),
            pl.when(pl.col("NCORE") == "X").then(pl.col("YTDBAL")).otherwise(0.0).alias("C1BAL"),
            pl.when(pl.col("NCORE") == "C").then(pl.col("YTDBAL")).otherwise(0.0).alias("C2BAL"),
            pl.when(pl.col("NCORE") == "N").then(pl.col("YTDBAL")).otherwise(0.0).alias("C3BAL"),
        ]
    )
    chunk = chunk.filter(pl.col("STAFF").is_not_null())

    keep_cols = ["STAFF", "PRODUCT", "NCORE", "CATG", "TAG", "AANUM", "YTDBAL",
                 "C1CNT", "C2CNT", "C3CNT", "C4CNT", "C5CNT", "C1BAL", "C2BAL", "C3BAL"]
    return chunk.select(keep_cols)


def build_elds(brh: pl.DataFrame) -> pl.DataFrame:
    """Stream ELDS_SRR.txt in chunks."""
    parts = []
    for chunk in iter_fixed_chunks([ELDS_FILE], ELDS_SPEC):
        transformed = transform_elds_chunk(chunk, brh)
        if transformed.height > 0:
            parts.append(transformed)

    empty_schema = {
        "STAFF": pl.Int64, "PRODUCT": pl.Utf8, "NCORE": pl.Utf8, "CATG": pl.Utf8,
        "TAG": pl.Utf8, "AANUM": pl.Utf8, "YTDBAL": pl.Float64,
        "C1CNT": pl.Int64, "C2CNT": pl.Int64, "C3CNT": pl.Int64,
        "C4CNT": pl.Int64, "C5CNT": pl.Int64,
        "C1BAL": pl.Float64, "C2BAL": pl.Float64, "C3BAL": pl.Float64,
    }
    return pl.concat(parts, how="vertical_relaxed") if parts else pl.DataFrame(schema=empty_schema)


# ===========================================================================
# SRS combination + final match against HRHO  (SRSBR / SRSHO / SRSEXC)
# ===========================================================================
def build_srs(elds: pl.DataFrame, dpc1: pl.DataFrame, dpc2: pl.DataFrame) -> pl.DataFrame:
    """DATA SRS; SET ELDS DPC1 DPC2;"""
    return pl.concat([elds, dpc1, dpc2], how="diagonal_relaxed")


def split_srs_against_hrho(srs: pl.DataFrame, hrho: pl.DataFrame, brh: pl.DataFrame
                            ) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    DATA SRSBR SRSHO SRSEXC;
       MERGE HRHO(IN=A) SRS(IN=B); BY STAFF;
       IF A AND B     THEN OUTPUT SRSHO;
       IF B AND NOT A THEN DO;
          IF BRANCH > 0  THEN OUTPUT SRSBR;
                         ELSE OUTPUT SRSEXC;
       END;
    """
    hrho_keys = hrho.select(["STAFF", "HOE"]).unique(subset=["STAFF"], keep="first")
    srsho = srs.join(hrho_keys, on="STAFF", how="inner")

    remaining = srs.join(hrho_keys.select("STAFF"), on="STAFF", how="anti")
    if "BRANCH" not in remaining.columns:
        remaining = remaining.with_columns(pl.lit(None).alias("BRANCH"))

    branch_gt0 = pl.col("BRANCH").is_not_null() & (pl.col("BRANCH") > 0)
    srsbr = remaining.filter(branch_gt0)
    srsexc = remaining.filter(~branch_gt0)
    return srsbr, srsho, srsexc


# ===========================================================================
# ASA / REPORT WRITER HELPERS
# ===========================================================================
@dataclass
class ReportWriter:
    """Minimal ASA fixed-width report writer with page break handling."""
    lines: List[str] = field(default_factory=list)
    line_no_on_page: int = 0

    def title(self, text: str):
        self.lines.append("1" + text)
        self.line_no_on_page = 1

    def header(self, text: str):
        self.lines.append(" " + text)
        self.line_no_on_page += 1

    def blank(self):
        self.lines.append(" ")
        self.line_no_on_page += 1

    def detail(self, text: str, first: bool = False):
        self.lines.append(("0" if first else " ") + text)
        self.line_no_on_page += 1
        if self.line_no_on_page >= PAGE_SIZE:
            self.line_no_on_page = 0

    def dump(self, path: Path):
        with open(path, "w", encoding="latin1", newline="") as f:
            f.write("\n".join(self.lines) + "\n")


def write_exception_report(srsexc: pl.DataFrame, report: ReportWriter):
    """
    TITLE 'EXCEPTION REPORT : UMMATCHED STAFF ID AGAINST HR FILE';
    PROC PRINT DATA=SRSEXC; WHERE TAG='ELDS'; VAR AANUM STAFF PRODUCT YTDBAL;
    PROC PRINT DATA=SRSEXC; WHERE TAG='DEPO'; VAR ACCTNO STAFF PRODUCT YTDBAL;
    """
    report.title("EXCEPTION REPORT : UMMATCHED STAFF ID AGAINST HR FILE")
    report.blank()

    elds_exc = srsexc.filter(pl.col("TAG") == "ELDS") if "TAG" in srsexc.columns else srsexc.head(0)
    report.header(f"{'AANUM':<15}{'STAFF':>8}{'PRODUCT':<20}{'YTDBAL':>15}")
    first = True
    for row in elds_exc.iter_rows(named=True):
        line = f"{row.get('AANUM', '') or '':<15}{row.get('STAFF', '') or '':>8}{row.get('PRODUCT', '') or '':<20}{row.get('YTDBAL', '') if row.get('YTDBAL') is not None else '':>15}"
        report.detail(line, first=first)
        first = False

    report.blank()
    deps_exc = srsexc.filter(pl.col("TAG") == "DEPO") if "TAG" in srsexc.columns else srsexc.head(0)
    report.header(f"{'ACCTNO':<15}{'STAFF':>8}{'PRODUCT':<20}{'YTDBAL':>15}")
    first = True
    for row in deps_exc.iter_rows(named=True):
        line = f"{row.get('ACCTNO', '') if row.get('ACCTNO') is not None else '':<15}{row.get('STAFF', '') or '':>8}{row.get('PRODUCT', '') or '':<20}{row.get('YTDBAL', '') if row.get('YTDBAL') is not None else '':>15}"
        report.detail(line, first=first)
        first = False


# ===========================================================================
# PROC SUMMARY NWAY EQUIVALENTS (via DuckDB)
# ===========================================================================
def summarize_branch(srsbr_all: pl.DataFrame) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    con.register("srsbr_all", srsbr_all.to_arrow())

    result = con.execute("""
        SELECT BRCHCD, STAFF, PRODUCT, NCORE,
            COALESCE(SUM(C1CNT), 0) AS C1CNT,
            COALESCE(SUM(C2CNT), 0) AS C2CNT,
            COALESCE(SUM(C3CNT), 0) AS C3CNT,
            COALESCE(SUM(C4CNT), 0) AS C4CNT,
            COALESCE(SUM(C5CNT), 0) AS C5CNT,
            COALESCE(SUM(C1BAL), 0.0) AS C1BAL,
            COALESCE(SUM(C2BAL), 0.0) AS C2BAL,
            COALESCE(SUM(C3BAL), 0.0) AS C3BAL
        FROM srsbr_all
        GROUP BY BRCHCD, STAFF, PRODUCT, NCORE
    """).fetch_arrow_table()

    con.close()

    if result.num_rows == 0:
        return pl.DataFrame(schema={
            "BRCHCD": pl.Utf8,
            "STAFF": pl.Int64,
            "PRODUCT": pl.Utf8,
            "NCORE": pl.Utf8,
            "C1CNT": pl.Int64,
            "C2CNT": pl.Int64,
            "C3CNT": pl.Int64,
            "C4CNT": pl.Int64,
            "C5CNT": pl.Int64,
            "C1BAL": pl.Float64,
            "C2BAL": pl.Float64,
            "C3BAL": pl.Float64,
        })

    return pl.from_arrow(result)


def summarize_ho(srsho_all: pl.DataFrame) -> pl.DataFrame:
    """
    PROC SUMMARY DATA=SRSHO NWAY;
    CLASS HOE STAFF PRODUCT NCORE;
    VAR C1CNT C2CNT C3CNT C4CNT C5CNT C1BAL C2BAL C3BAL;
    OUTPUT OUT=SCRD.SRSHO (DROP=_TYPE_ _FREQ_) SUM=;
    """
    con = duckdb.connect(database=":memory:")
    con.register("srsho_all", srsho_all.to_arrow())
    result = con.execute("""
        SELECT HOE, STAFF, PRODUCT, NCORE,
            COALESCE(SUM(C1CNT), 0) AS C1CNT,
            COALESCE(SUM(C2CNT), 0) AS C2CNT,
            COALESCE(SUM(C3CNT), 0) AS C3CNT,
            COALESCE(SUM(C4CNT), 0) AS C4CNT,
            COALESCE(SUM(C5CNT), 0) AS C5CNT,
            COALESCE(SUM(C1BAL), 0.0) AS C1BAL,
            COALESCE(SUM(C2BAL), 0.0) AS C2BAL,
            COALESCE(SUM(C3BAL), 0.0) AS C3BAL
        FROM srsho_all
        GROUP BY HOE, STAFF, PRODUCT, NCORE
    """).fetch_arrow_table()
    con.close()

    if result.num_rows == 0:
        return pl.DataFrame(schema={
            "HOE": pl.Utf8,
            "STAFF": pl.Int64,
            "PRODUCT": pl.Utf8,
            "NCORE": pl.Utf8,
            "C1CNT": pl.Int64,
            "C2CNT": pl.Int64,
            "C3CNT": pl.Int64,
            "C4CNT": pl.Int64,
            "C5CNT": pl.Int64,
            "C1BAL": pl.Float64,
            "C2BAL": pl.Float64,
            "C3BAL": pl.Float64,
    })

    return pl.from_arrow(result)


def derive_srss(srsho_sum: pl.DataFrame, srsbr_sum: pl.DataFrame) -> pl.DataFrame:
    """
    DATA SRSS; SET SRSHO SRSBR;
    PROC SUMMARY DATA=SRSS NWAY; CLASS STAFF PRODUCT NCORE;
    VAR C1CNT C2CNT C3CNT C4CNT C5CNT C1BAL C2BAL C3BAL;
    OUTPUT OUT=SRSS (DROP=_TYPE_ _FREQ_) SUM=;

    DATA SRSS; SET SRSS;
       S1CNT=0; S2CNT=0; S3CNT=0; S4CNT=0; S5CNT=0;
       [missing C/C-balance -> 0 already enforced by COALESCE above]
       IF NCORE='X' AND C1CNT>0 THEN S1CNT=1;
       IF NCORE='C' AND C2CNT>0 THEN S2CNT=1;
       IF NCORE='N' THEN DO;
          IF C3CNT>0 THEN S3CNT=1;
          IF C4CNT>0 THEN S4CNT=1;
          IF C5CNT>0 THEN S5CNT=1;
       END;
    """
    srss_raw = pl.concat(
        [srsho_sum.select(["STAFF", "PRODUCT", "NCORE", "C1CNT", "C2CNT", "C3CNT", "C4CNT", "C5CNT", "C1BAL", "C2BAL", "C3BAL"]),
         srsbr_sum.select(["STAFF", "PRODUCT", "NCORE", "C1CNT", "C2CNT", "C3CNT", "C4CNT", "C5CNT", "C1BAL", "C2BAL", "C3BAL"])],
        how="vertical_relaxed",
    )

    con = duckdb.connect(database=":memory:")
    con.register("srss_raw", srss_raw.to_arrow())
    srss_sum = con.execute(
        """
        SELECT STAFF, PRODUCT, NCORE,
               COALESCE(SUM(C1CNT), 0) AS C1CNT,
               COALESCE(SUM(C2CNT), 0) AS C2CNT,
               COALESCE(SUM(C3CNT), 0) AS C3CNT,
               COALESCE(SUM(C4CNT), 0) AS C4CNT,
               COALESCE(SUM(C5CNT), 0) AS C5CNT,
               COALESCE(SUM(C1BAL), 0.0) AS C1BAL,
               COALESCE(SUM(C2BAL), 0.0) AS C2BAL,
               COALESCE(SUM(C3BAL), 0.0) AS C3BAL
        FROM srss_raw
        GROUP BY STAFF, PRODUCT, NCORE
        """
    ).fetch_arrow_table()
    con.close()
    srss = pl.from_arrow(srss_sum)

    srss = srss.with_columns(
        [
            pl.when((pl.col("NCORE") == "X") & (pl.col("C1CNT") > 0)).then(1).otherwise(0).alias("S1CNT"),
            pl.when((pl.col("NCORE") == "C") & (pl.col("C2CNT") > 0)).then(1).otherwise(0).alias("S2CNT"),
            pl.when((pl.col("NCORE") == "N") & (pl.col("C3CNT") > 0)).then(1).otherwise(0).alias("S3CNT"),
            pl.when((pl.col("NCORE") == "N") & (pl.col("C4CNT") > 0)).then(1).otherwise(0).alias("S4CNT"),
            pl.when((pl.col("NCORE") == "N") & (pl.col("C5CNT") > 0)).then(1).otherwise(0).alias("S5CNT"),
        ]
    )
    return srss


def summarize_srsp(srss: pl.DataFrame) -> pl.DataFrame:
    """
    PROC SUMMARY DATA=SRSS NWAY; CLASS PRODUCT;
    VAR S1CNT C1CNT C1BAL S2CNT C2CNT C2BAL S3CNT C3CNT C3BAL S4CNT C4CNT S5CNT C5CNT;
    OUTPUT OUT=SRSP SUM=;
    """
    con = duckdb.connect(database=":memory:")
    con.register("srss", srss.to_arrow())
    result = con.execute(
        """
        SELECT PRODUCT,
               COALESCE(SUM(S1CNT), 0) AS S1CNT, COALESCE(SUM(C1CNT), 0) AS C1CNT, COALESCE(SUM(C1BAL), 0.0) AS C1BAL,
               COALESCE(SUM(S2CNT), 0) AS S2CNT, COALESCE(SUM(C2CNT), 0) AS C2CNT, COALESCE(SUM(C2BAL), 0.0) AS C2BAL,
               COALESCE(SUM(S3CNT), 0) AS S3CNT, COALESCE(SUM(C3CNT), 0) AS C3CNT, COALESCE(SUM(C3BAL), 0.0) AS C3BAL,
               COALESCE(SUM(S4CNT), 0) AS S4CNT, COALESCE(SUM(C4CNT), 0) AS C4CNT,
               COALESCE(SUM(S5CNT), 0) AS S5CNT, COALESCE(SUM(C5CNT), 0) AS C5CNT
        FROM srss
        WHERE PRODUCT IS NOT NULL
        GROUP BY PRODUCT
        """
    ).fetch_arrow_table()
    con.close()
    return pl.from_arrow(result)


def write_srsp_section(srsp: pl.DataFrame, report: ReportWriter):
    """
    DATA SRSP; SET SRSP;
       PUT PRODUCT ';' S1CNT ';' C1CNT ';' C1BAL ';'
           S2CNT ';' C2CNT ';' C2BAL ';'
           S3CNT ';' C3CNT ';' C3BAL ';'
           S4CNT ';' C4CNT ';' S5CNT ';' C5CNT;
    PROC PRINT;
    """
    report.title("FINAL SRSP SUMMARY")
    report.blank()
    first = True
    for row in srsp.iter_rows(named=True):
        prod = row['PRODUCT'] or ''
        line = (
            f"{prod};{row['S1CNT']};{row['C1CNT']};{row['C1BAL']};"
            f"{row['S2CNT']};{row['C2CNT']};{row['C2BAL']};"
            f"{row['S3CNT']};{row['C3CNT']};{row['C3BAL']};"
            f"{row['S4CNT']};{row['C4CNT']};{row['S5CNT']};{row['C5CNT']}"
        )
        report.detail(line, first=first)
        first = False

    report.blank()
    report.header(f"{'PRODUCT':<20}{'S1CNT':>6}{'C1CNT':>8}{'C1BAL':>15}"
                   f"{'S2CNT':>6}{'C2CNT':>8}{'C2BAL':>15}"
                   f"{'S3CNT':>6}{'C3CNT':>8}{'C3BAL':>15}"
                   f"{'S4CNT':>6}{'C4CNT':>8}{'S5CNT':>6}{'C5CNT':>8}")
    for row in srsp.iter_rows(named=True):
        prod = row['PRODUCT'] or ''
        line = (
            f"{prod:<20}{row['S1CNT']:>6}{row['C1CNT']:>8}{row['C1BAL']:>15.2f}"
            f"{row['S2CNT']:>6}{row['C2CNT']:>8}{row['C2BAL']:>15.2f}"
            f"{row['S3CNT']:>6}{row['C3CNT']:>8}{row['C3BAL']:>15.2f}"
            f"{row['S4CNT']:>6}{row['C4CNT']:>8}{row['S5CNT']:>6}{row['C5CNT']:>8}"
        )
        report.detail(line)


# ===========================================================================
# MAIN
# ===========================================================================
def main():
    print(f"[EIBMSCRN] Report date : {RDATE}")
    print(f"[EIBMSCRN] RMM={RMM}  RYY={RYY}  RYEAR={RYEAR}  RMON={RMON}")

    # ---------------- Small lookups ----------------
    brh = load_brh()
    hrbr = load_hrbr()
    hrho = load_hrho()
    print(f"[EIBMSCRN] BRH={brh.height}  HRBR={hrbr.height}  HRHO={hrho.height}")

    # ---------------- CARDFILE -> UCARD -> UCBR/UCEXC -> UCHO/UCEXC1 ----------------
    ucard = build_ucard()
    print(f"[EIBMSCRN] UCARD filtered rows = {ucard.height}")
    ucbr, ucexc = split_ucard_against_hrbr(ucard, hrbr)
    ucho, ucexc1 = split_ucexc_against_hrho(ucexc, hrho)
    print(f"[EIBMSCRN] UCBR={ucbr.height}  UCHO={ucho.height}  UCEXC1={ucexc1.height}")

    # ---------------- DEP -> DPC1/DPC2 ----------------
    dpc1, dpc2 = build_dpc1_dpc2(brh)
    print(f"[EIBMSCRN] DPC1={dpc1.height}  DPC2={dpc2.height}")

    # ---------------- ELDS ----------------
    elds = build_elds(brh)
    print(f"[EIBMSCRN] ELDS processed rows = {elds.height}")

    # ---------------- SRS = ELDS + DPC1 + DPC2 ----------------
    srs = build_srs(elds, dpc1, dpc2)

    # ---------------- Match SRS against HRHO -> SRSBR/SRSHO/SRSEXC ----------------
    srsbr, srsho, srsexc = split_srs_against_hrho(srs, hrho, brh)

    # DATA SRSBR; SET SRSBR UCBR;
    # DATA SRSHO; SET SRSHO UCHO;
    common_cols = ["STAFF", "PRODUCT", "NCORE", "C1CNT", "C2CNT", "C3CNT", "C4CNT", "C5CNT",
                   "C1BAL", "C2BAL", "C3BAL"]
    srsbr_with_brchcd = srsbr.select([c for c in common_cols + ["BRCHCD"] if c in srsbr.columns])
    ucbr_with_brchcd = ucbr.select([c for c in common_cols + ["BRCHCD"] if c in ucbr.columns])
    srsbr_all = pl.concat([srsbr_with_brchcd, ucbr_with_brchcd], how="diagonal_relaxed")

    srsho_with_hoe = srsho.select([c for c in common_cols + ["HOE"] if c in srsho.columns])
    ucho_with_hoe = ucho.select([c for c in common_cols + ["HOE"] if c in ucho.columns])
    srsho_all = pl.concat([srsho_with_hoe, ucho_with_hoe], how="diagonal_relaxed")

    print(f"[EIBMSCRN] SRSBR(all)={srsbr_all.height}  SRSHO(all)={srsho_all.height}  SRSEXC={srsexc.height}")

    # ---------------- PROC SUMMARY -> SCRD.SRSBR / SCRD.SRSHO ----------------
    srsbr_sum = summarize_branch(srsbr_all)
    srsho_sum = summarize_ho(srsho_all)

    srsbr_path = build_output_file(OUTPUT_DIR, "SRSBR").with_suffix(".parquet")
    srsho_path = build_output_file(OUTPUT_DIR, "SRSHO").with_suffix(".parquet")
    srsbr_sum.write_parquet(srsbr_path)
    srsho_sum.write_parquet(srsho_path)
    print(f"[EIBMSCRN] Wrote SCRD.SRSBR -> {srsbr_path}")
    print(f"[EIBMSCRN] Wrote SCRD.SRSHO -> {srsho_path}")

    # ---------------- SRSS / SRSP ----------------
    srss = derive_srss(srsho_sum, srsbr_sum)
    srsp = summarize_srsp(srss)

    # ---------------- ASA REPORT OUTPUT ----------------
    report = ReportWriter()
    write_exception_report(srsexc, report)
    write_srsp_section(srsp, report)

    report_path = build_output_file(OUTPUT_DIR, "EIBMSCRN").with_suffix(".txt")
    report.dump(report_path)
    print(f"[EIBMSCRN] Wrote report -> {report_path}")

    # ---------------- Print to terminal ----------------
    print("\n----- EXCEPTION REPORT (ELDS/DEPO unmatched staff) -----")
    print(srsexc.select([c for c in ["TAG", "STAFF", "PRODUCT", "YTDBAL"] if c in srsexc.columns]))
    print("\n----- FINAL SRSP SUMMARY -----")
    print(srsp)


if __name__ == "__main__":
    main()
