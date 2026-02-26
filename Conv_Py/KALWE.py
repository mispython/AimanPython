# !/usr/bin/env python3
"""
PROGRAM : KALWE
DATE    : 01.11.99
REPORT  : TO EXTRACT DATA FROM KAPITI TABLE 1 & 3 (WEEKLY)
"""

import os
import duckdb
import polars as pl
from datetime import date, datetime

# ==============================================================================
# PATH CONFIGURATION
# ==============================================================================

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR  = os.path.join(BASE_DIR, "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Runtime parameters (replace with actual values or pass via CLI/config)
REPTMON = "202301"   # Reporting month YYYYMM
NOWK    = "01"       # Week number

# Input flat file paths (pipe-delimited .txt)
BNMTBL1_TXT  = os.path.join(INPUT_DIR, "BNMTBL1.txt")   # Kapiti Table 1
BNMTBL3_TXT  = os.path.join(INPUT_DIR, "BNMTBL3.txt")   # Kapiti Table 3
BNMTBLW_TXT  = os.path.join(INPUT_DIR, "BNMTBLW.txt")   # Kapiti Table W (weekly)
DCIWTBL_TXT  = os.path.join(INPUT_DIR, "DCIWTBL.txt")   # DCI weekly table flat file

# Input parquet lookup path
DCIWH_PARQUET = os.path.join(INPUT_DIR, f"DCI{REPTMON}{NOWK}.parquet")  # DCIWH.DCI&REPTMON&NOWK

# Output parquet paths
K1TBL_OUT  = os.path.join(OUTPUT_DIR, f"K1TBL{REPTMON}{NOWK}.parquet")   # BNMK.K1TBL
K3TBL_OUT  = os.path.join(OUTPUT_DIR, f"K3TBL{REPTMON}{NOWK}.parquet")   # BNMK.K3TBL
KWTBL_OUT  = os.path.join(OUTPUT_DIR, f"KWTBL{REPTMON}{NOWK}.parquet")   # BNMK.KWTBL
DCIWTB_OUT = os.path.join(OUTPUT_DIR, f"DCIWTB{REPTMON}{NOWK}.parquet")  # BNMK.DCIWTB

# OPTIONS YEARCUTOFF=1950
# Dates with 2-digit years: if year >= 50 => 19xx, else => 20xx
YEARCUTOFF = 1950


# ==============================================================================
# HELPER: parse YYMMDD8 integer as stored in raw file (e.g. 20230131 -> date)
# SAS stores GWSDT/GWMDT as raw 8-digit integers (YYYYMMDD), then converts:
#   IF GWSDT NE 0 THEN GWSDT = INPUT(PUT(GWSDT, Z8.), YYMMDD8.);
# YYMMDD8. informat: YEARCUTOFF=1950 means 2-digit year 50-99 => 1950-1999
# But PUT(integer, Z8.) zero-pads to 8 chars giving YYYYMMDD or YYMMDDXX.
# In practice the raw value is already YYYYMMDD (8 digits), so we parse directly.
# ==============================================================================

def parse_yymmdd8_int(val) -> date | None:
    """
    Parse integer stored as YYYYMMDD (SAS YYMMDD8. after PUT(x,Z8.)).
    Returns None if val is 0, None, or unparseable.
    """
    if val is None:
        return None
    try:
        iv = int(val)
    except (TypeError, ValueError):
        return None
    if iv == 0:
        return None
    s = f"{iv:08d}"
    try:
        return datetime.strptime(s, "%Y%m%d").date()
    except ValueError:
        return None


def parse_ddmmyy10(val) -> date | None:
    """
    Parse string in DD/MM/YYYY or DD-MM-YYYY format (SAS DDMMYY10. informat).
    Returns None if blank or unparseable.
    """
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d%m%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def parse_yymmdd8_str(val) -> date | None:
    """
    Parse string in YYMMDD8. format (YYYYMMDD or YYMMDD with YEARCUTOFF=1950).
    """
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    # Try YYYYMMDD first (8-digit)
    try:
        return datetime.strptime(s, "%Y%m%d").date()
    except ValueError:
        pass
    # Try YYMMDD (6-digit) with YEARCUTOFF=1950
    try:
        yy = int(s[:2])
        mm = int(s[2:4])
        dd = int(s[4:6])
        yr = (1900 + yy) if yy >= (YEARCUTOFF - 1900) else (2000 + yy)
        return date(yr, mm, dd)
    except (ValueError, IndexError):
        return None


# ==============================================================================
# DATA BNMK.K1TBL&REPTMON&NOWK
# INFILE BNMTBL1 DELIMITER='|' DSD MISSOVER;
# First record: REPTDATE (YYMMDD8.)
# Remaining records: all GW* fields
# Post-processing:
#   IF GWSDT NE 0 THEN GWSDT = INPUT(PUT(GWSDT,Z8.),YYMMDD8.);
#   IF GWMDT NE 0 THEN GWMDT = INPUT(PUT(GWMDT,Z8.),YYMMDD8.);
#   FORMAT GWSDT GWMDT YYMMDD8.;
# ==============================================================================

# Column names and types for K1TBL / KWTBL GW* records
# (both K1TBL and KWTBL share the same layout; KWTBL GWRATD/GWRATC is 11.7 vs 11.6)
GW_COLUMNS = [
    "GWAB", "GWAN", "GWAS", "GWAPP", "GWACS",
    "GWBALA", "GWBALC", "GWPAIA", "GWPAIC",
    "GWSHN", "GWCTP", "GWACT", "GWACD", "GWSAC",
    "GWNANC", "GWCNAL", "GWCCY", "GWCNAR", "GWCNAP",
    "GWDIAA", "GWDIAC", "GWCIAA", "GWCIAC",
    "GWRATD", "GWRATC",
    "GWDIPA", "GWDIPC", "GWCIPA", "GWCIPC",
    "GWPL1D", "GWPL2D", "GWPL1C", "GWPL2C",
    "GWPALA", "GWPALC",
    "GWDLP", "GWDLR",
    "GWSDT", "GWRDT", "GWRRT", "GWPDT", "GWPRT", "GWPCM",
    "GWMOTC", "GWMRTC", "GWMRT", "GWMDT", "GWMCM", "GWMWM",
    "GWMVT", "GWMVTS",
    "GWSRC", "GWUC1", "GWUC2", "GWC2R",
    "GWAMAP", "GWEXR",
    "GWOPT", "GWOCY",
    "GWCBD",
]

# String columns (all others are numeric)
GW_STRING_COLS = {
    "GWAB", "GWAN", "GWAS", "GWAPP", "GWACS",
    "GWSHN", "GWCTP", "GWACT", "GWACD", "GWSAC",
    "GWNANC", "GWCNAL", "GWCCY", "GWCNAR", "GWCNAP",
    "GWPL1D", "GWPL2D", "GWPL1C", "GWPL2C",
    "GWDLP", "GWDLR",
    "GWMOTC", "GWMRTC",
    "GWMVT", "GWMVTS",
    "GWSRC", "GWUC1", "GWUC2", "GWC2R",
    "GWOPT", "GWOCY",
}

# Column labels (LABEL statements)
GW_LABELS = {
    "GWAB":   "BRANCH",
    "GWAN":   "BASIC NUMBER",
    "GWAS":   "ACCOUNT SUFFIX",
    "GWBALA": "BALANCE - ORIGINAL CURRENCY",
    "GWBALC": "BALANCE - LOCAL CURRENCY",
    "GWSHN":  "ACCOUNT SHORT NAME",
    "GWCTP":  "CUSTOMER TYPE",
    "GWACT":  "A/C TYPE",
    "GWACD":  "ANALYSIS CODE",
    "GWSAC":  "SUNDRY ANALYSIS CODE",
    "GWCNAL": "RESIDENCE COUNTRY",
    "GWCCY":  "CCY",
    "GWDIAC": "DR INT ACC - LOCAL CURRENCY",
    "GWCIAC": "CR INT ACC - LOCAL CURRENCY",
    "GWDLP":  "DEAL TYPE",
    "GWDLR":  "DEAL REFERENCE",
    "GWSDT":  "START DATE",
    "GWMDT":  "MATURITY DATE",
    "GWMVT":  "MOVEMENT TYPE",
    "GWMVTS": "MOVEMENT SUB-TYPE",
    "GWC2R":  "CUSTFISS",
    "GWOCY":  "OTHER CURRENCY MNEMONIC",
    "GWEXR":  "TRANSACTED FX/RM RATES",
    "GWCBD":  "CURRENT BUSINESS DATE",
}


def read_gwtbl(filepath: str) -> tuple[date | None, pl.DataFrame]:
    """
    Read a pipe-delimited GW* flat file (BNMTBL1 or BNMTBLW layout).
    First line: REPTDATE (YYMMDD8. string).
    Remaining lines: one GW record per line, pipe-delimited.

    Returns (reptdate, DataFrame).
    Post-processes GWSDT and GWMDT:
      IF GWSDT NE 0 THEN GWSDT = INPUT(PUT(GWSDT,Z8.),YYMMDD8.);
      IF GWMDT NE 0 THEN GWMDT = INPUT(PUT(GWMDT,Z8.),YYMMDD8.);
    GWCBD is parsed as YYMMDD8.
    """
    reptdate: date | None = None
    rows: list[dict] = []

    with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
        for lineno, raw_line in enumerate(fh, start=1):
            # DSD MISSOVER: treat consecutive delimiters as missing, don't fail on short lines
            line = raw_line.rstrip("\r\n")
            fields = line.split("|")

            if lineno == 1:
                # IF _N_=1 THEN INPUT REPTDATE: YYMMDD8.;
                reptdate = parse_yymmdd8_str(fields[0].strip()) if fields else None
                continue

            # Pad fields to expected column count with empty strings
            while len(fields) < len(GW_COLUMNS):
                fields.append("")

            rec: dict = {"REPTDATE": reptdate}
            for i, col in enumerate(GW_COLUMNS):
                raw = fields[i].strip() if i < len(fields) else ""
                if col in GW_STRING_COLS:
                    rec[col] = raw if raw else None
                elif col == "GWCBD":
                    # FORMAT GWCBD YYMMDD8. — parsed as date
                    rec[col] = parse_yymmdd8_str(raw) if raw else None
                else:
                    # Numeric column
                    try:
                        rec[col] = float(raw) if raw else None
                    except ValueError:
                        rec[col] = None
            rows.append(rec)

    if not rows:
        df = pl.DataFrame()
    else:
        df = pl.DataFrame(rows)

    # IF GWSDT NE 0 THEN GWSDT = INPUT(PUT(GWSDT,Z8.),YYMMDD8.);
    # IF GWMDT NE 0 THEN GWMDT = INPUT(PUT(GWMDT,Z8.),YYMMDD8.);
    # GWSDT and GWMDT were read as 8-digit integers; convert to date
    for datecol in ("GWSDT", "GWMDT"):
        if datecol in df.columns:
            df = df.with_columns(
                pl.col(datecol).map_elements(
                    lambda v: parse_yymmdd8_int(v) if v is not None else None,
                    return_dtype=pl.Date,
                ).alias(datecol)
            )

    return reptdate, df


# ==============================================================================
# DATA BNMK.K3TBL&REPTMON&NOWK
# INFILE BNMTBL3 DELIMITER='|' DSD MISSOVER;
# First record: REPTDATE (YYMMDD8.)
# Remaining records: UT* fields
# Post-processing:
#   IF SUBSTR(UTDLP,2,2) IN ('RT','RI') THEN DELETE;
#   MATDT  = INPUT(UTMDT, DDMMYY10.);
#   ISSDT  = INPUT(UTOSD, DDMMYY10.);
#   REPTDATE = INPUT(UTCBD, DDMMYY10.);
#   DDATE    = INPUT(UTCBD, DDMMYY10.);
#   XDATE    = INPUT(UTIDT, DDMMYY10.);
#   FORMAT MATDT ISSDT REPTDATE YYMMDD8.;
#   IF UTSTY='IZD' THEN UTCPR=UTQDS;
#   IF UTSTY IN ('IFD','ILD','ISD','IZD') AND XDATE > DDATE THEN DELETE;
# Note: KALWE K3TBL has no UTBRNM column (added only in KALWEI)
#       and no UTMM1 column.
# ==============================================================================

K3TBL_COLUMNS = [
    "UTSTY", "UTREF", "UTDLP", "UTDLR", "UTSMN",
    "UTCUS", "UTCLC", "UTCTP",
    "UTFCV",
    "UTIDT", "UTLCD", "UTNCD", "UTMDT", "UTCBD",
    "UTCPR", "UTQDS", "UTPCP",
    "UTAMOC", "UTDPF",
    "UTAICT", "UTAICY", "UTAIT",
    "UTDPET", "UTDPEY", "UTDPE",
    "UTASN",
    "UTOSD",
    "UTCA2", "UTSAC", "UTCNAP", "UTCNAR", "UTCNAL", "UTCCY",
    "UTAMTS",
]

K3TBL_STRING_COLS = {
    "UTSTY", "UTREF", "UTDLP", "UTDLR", "UTSMN",
    "UTCUS", "UTCLC", "UTCTP",
    "UTIDT", "UTLCD", "UTNCD", "UTMDT", "UTCBD",
    "UTOSD",
    "UTCA2", "UTSAC", "UTCNAP", "UTCNAR", "UTCNAL", "UTCCY",
}

K3TBL_LABELS = {
    "UTSTY":  "SECURITY TYPE",
    "UTREF":  "PORTFOLIO REFERENCE",
    "UTDLP":  "DEAL PREFIX",
    "UTDLR":  "DEAL REFERENCE",
    "UTSMN":  "SECURITY MNEMONIC",
    "UTCUS":  "CUSTOMER NAME",
    "UTCLC":  "CUSTOMER LOCATION",
    "UTCTP":  "CUSTOMER TYPE",
    "UTFCV":  "FACE VALUE",
    "UTIDT":  "ISSUE DATE",
    "UTLCD":  "LAST COUPON DATE",
    "UTNCD":  "NEXT COUPON DATE",
    "UTMDT":  "MATURITY DATE",
    "UTCBD":  "CURRENT BUSINESS DATE",
    "UTCPR":  "COUPON RATE",
    "UTQDS":  "DISCOUNT RATE",
    "UTPCP":  "DEAL CAPITAL PRICE",
    "UTAMOC": "CURRENT AMOUNT OWNED",
    "UTDPF":  "DISCOUNT/PREMIUM UNEARNED",
    "UTAICT": "ACTUAL INT ACCRUED COUPON TO DATE",
    "UTAICY": "ACTUAL INT ACCRUED COUPON TO YESTERDAY",
    "UTAIT":  "ACTUAL INT ACCRUED P&L TODAY",
    "UTDPET": "DISCOUNT/PREMIUM UNEARNED TODAY",
    "UTDPEY": "DISCOUNT/PREMIUM UNEARNED YESTERDAY",
    "UTDPE":  "DISCOUNT/PREMIUM P&L TODAY",
    "UTASN":  "ASSET NUMBER",
    "UTOSD":  "OWNERSHIP SETTLEMENT DATE",
    "UTCA2":  "ANALYSIS CODE",
    "UTSAC":  "SUNDRY ANALYSIS CODE",
    "UTCNAP": "PARENT COUNTRY",
    "UTCNAR": "RISK COUNTRY",
    "UTCNAL": "RESIDENCE COUNTRY",
    "UTCCY":  "CURRENCY CODE",
    "UTAMTS": "PURCHASE PROCEEDS",
}


def read_k3tbl(filepath: str, columns: list[str], string_cols: set[str]) -> tuple[date | None, pl.DataFrame]:
    """
    Read a pipe-delimited K3TBL flat file.
    First line: REPTDATE (YYMMDD8. string).
    Remaining lines: one UT* record per line, pipe-delimited.

    Applies all post-processing filters and derived columns.
    Returns (reptdate, DataFrame).
    """
    reptdate: date | None = None
    rows: list[dict] = []

    with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
        for lineno, raw_line in enumerate(fh, start=1):
            line = raw_line.rstrip("\r\n")
            fields = line.split("|")

            if lineno == 1:
                # IF _N_=1 THEN INPUT REPTDATE: YYMMDD8.;
                reptdate = parse_yymmdd8_str(fields[0].strip()) if fields else None
                continue

            while len(fields) < len(columns):
                fields.append("")

            rec: dict = {}
            for i, col in enumerate(columns):
                raw = fields[i].strip() if i < len(fields) else ""
                if col in string_cols:
                    rec[col] = raw if raw else None
                else:
                    try:
                        rec[col] = float(raw) if raw else None
                    except ValueError:
                        rec[col] = None

            # IF SUBSTR(UTDLP,2,2) IN ('RT','RI') THEN DELETE;
            utdlp = rec.get("UTDLP") or ""
            if utdlp[1:3] in ("RT", "RI"):
                continue

            # Derive date columns
            utmdt = rec.get("UTMDT") or ""
            utosd = rec.get("UTOSD") or ""
            utcbd = rec.get("UTCBD") or ""
            utidt = rec.get("UTIDT") or ""

            matdt = parse_ddmmyy10(utmdt)   if utmdt else None  # IF UTMDT NE ' '
            issdt = parse_ddmmyy10(utosd)   if utosd else None  # IF UTOSD NE ' '
            rdate = parse_ddmmyy10(utcbd)   if utcbd else None  # IF UTCBD NE ' '
            ddate = parse_ddmmyy10(utcbd)   if utcbd else None  # IF UTCBD NE ' '
            xdate = parse_ddmmyy10(utidt)   if utidt else None  # IF UTIDT NE ' '

            # Use per-record REPTDATE if available, else fall back to file-level
            rec["REPTDATE"] = rdate if rdate is not None else reptdate
            rec["MATDT"]    = matdt
            rec["ISSDT"]    = issdt
            rec["DDATE"]    = ddate
            rec["XDATE"]    = xdate

            # IF UTSTY='IZD' THEN UTCPR=UTQDS;
            utsty = rec.get("UTSTY") or ""
            if utsty == "IZD":
                rec["UTCPR"] = rec.get("UTQDS")

            # IF UTSTY IN ('IFD','ILD','ISD','IZD') THEN IF XDATE > DDATE THEN DELETE;
            if utsty in ("IFD", "ILD", "ISD", "IZD"):
                if xdate is not None and ddate is not None and xdate > ddate:
                    continue

            # EXTDATE column used downstream for sort (UTIDT parsed as date)
            rec["EXTDATE"] = xdate

            rows.append(rec)

    if not rows:
        return reptdate, pl.DataFrame()

    df = pl.DataFrame(rows)
    return reptdate, df


# ==============================================================================
# DATA DCIWTB
# INFILE DCIWTBL DELIMITER='|' MISSOVER DSD FIRSTOBS=1;
# IF _N_=1 THEN INPUT @65 REPTDATE DDMMYY10.;  (column 65 of first line)
# Remaining records: DCI* fields
# Post-processing:
#   GFC2R = INPUT(GFC2R1, 5.);    (convert string to numeric)
# PROC SORT; BY DCDLR;
# ==============================================================================

DCIWTB_COLUMNS = [
    "DCDLP", "DCDLR", "DCCUS", "DCCLC", "DCBCCY",
    "DCBAMT", "C8SPT", "DCACCY",
    "DCCTRD", "DCMTYD",
    "GFCTP", "GFC2R1", "GFCUN", "GFCNAL",
    "DCTRNT", "DCBSI",
    "ODGRP", "GFSAC",
    "STATUS", "OPTYPE",
    "STKRATE",
]

DCIWTB_STRING_COLS = {
    "DCDLP", "DCDLR", "DCCUS", "DCCLC", "DCBCCY",
    "DCACCY",
    "GFCTP", "GFC2R1", "GFCUN", "GFCNAL",
    "DCTRNT", "DCBSI",
    "ODGRP", "GFSAC",
    "STATUS", "OPTYPE",
}

DCIWTB_LABELS = {
    "DCDLP":  "DEAL TYPE",
    "DCDLR":  "DEAL REFERENCE",
    "DCCUS":  "CUSTOMER NAME",
    "DCCLC":  "CUSTOMER LOCATION",
    "DCBCCY": "BASE CURRENCY",
    "DCBAMT": "BASE AMOUNT",
    "C8SPT":  "SPOT RATE",
    "DCACCY": "ALTERNATE CURRENCY",
    "DCCTRD": "CONTRACT DATE",
    "DCMTYD": "MATURITY DATE",
    "GFCTP":  "CUSTOMER TYPE",
    "GFC2R":  "FISS CODE",
    "GFCUN":  "CUSTOMER LONG NAME",
    "GFCNAL": "RESIDENCE COUNTRY",
    "DCTRNT": "TRANSACTION TYPE",
    "DCBSI":  "BUY/SELL",
    "ODGRP":  "CUSTOMER GROUP",
    "GFSAC":  "SUNDRY ANALYSIS CODE",
    "STATUS": "STATUS INDICATOR",
    "OPTYPE": "OPTION TYPE",
    "STKRATE":"STRIKE RATE",
}


def read_dciwtb(filepath: str) -> tuple[date | None, pl.DataFrame]:
    """
    Read the DCIWTBL pipe-delimited flat file.
    First line: REPTDATE parsed from column position 65 (0-based: chars 64+)
                using DDMMYY10. informat.
    Remaining lines: DCI* fields.
    Post-processing:
      DCCTRD and DCMTYD parsed as DDMMYY10. dates.
      GFC2R = INPUT(GFC2R1, 5.) -- numeric conversion of GFC2R1 string.
    Returns (reptdate, DataFrame) sorted by DCDLR.
    """
    reptdate: date | None = None
    rows: list[dict] = []

    with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
        for lineno, raw_line in enumerate(fh, start=1):
            line = raw_line.rstrip("\r\n")

            if lineno == 1:
                # IF _N_=1 THEN INPUT @65 REPTDATE DDMMYY10.;
                # @65 means column 65 (1-based) = index 64 (0-based)
                col65_str = line[64:74].strip() if len(line) > 64 else ""
                reptdate = parse_ddmmyy10(col65_str) if col65_str else None
                continue

            fields = line.split("|")
            while len(fields) < len(DCIWTB_COLUMNS):
                fields.append("")

            rec: dict = {"REPTDATE": reptdate}
            for i, col in enumerate(DCIWTB_COLUMNS):
                raw = fields[i].strip() if i < len(fields) else ""
                if col in DCIWTB_STRING_COLS:
                    rec[col] = raw if raw else None
                elif col in ("DCCTRD", "DCMTYD"):
                    rec[col] = parse_ddmmyy10(raw) if raw else None
                else:
                    try:
                        rec[col] = float(raw) if raw else None
                    except ValueError:
                        rec[col] = None

            # GFC2R = INPUT(GFC2R1, 5.);
            gfc2r1 = rec.get("GFC2R1") or ""
            try:
                rec["GFC2R"] = float(gfc2r1[:5]) if gfc2r1.strip() else None
            except (ValueError, TypeError):
                rec["GFC2R"] = None

            rows.append(rec)

    if not rows:
        return reptdate, pl.DataFrame()

    df = pl.DataFrame(rows)
    # PROC SORT; BY DCDLR;
    if "DCDLR" in df.columns:
        df = df.sort("DCDLR")
    return reptdate, df


# ==============================================================================
# DATA DCIWH (KEEP=DCDLR GFCUN GFC2R)
# SET DCIWH.DCI&REPTMON&NOWK;
# RENAME CUSTICKETNO=DCDLR CUSTNAME=GFCUN CUSTCODE=GFC2R;
# PROC SORT; BY DCDLR;
# ==============================================================================

def read_dciwh(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """
    Read DCIWH.DCI parquet lookup table.
    Rename: CUSTICKETNO -> DCDLR, CUSTNAME -> GFCUN, CUSTCODE -> GFC2R.
    Keep only: DCDLR, GFCUN, GFC2R.
    Sort by DCDLR.
    """
    dciwh = con.execute(f"""
        SELECT
            CUSTICKETNO AS DCDLR,
            CUSTNAME    AS GFCUN,
            CUSTCODE    AS GFC2R
        FROM read_parquet('{DCIWH_PARQUET}')
        ORDER BY CUSTICKETNO
    """).pl()
    return dciwh


# ==============================================================================
# DATA BNMK.DCIWTB&REPTMON&NOWK
# MERGE DCIWTB(IN=A) DCIWH; BY DCDLR; IF A;
# IF DCBCCY='MYR' THEN C8SPT=1;
# ==============================================================================

def build_dciwtb(dciwtb: pl.DataFrame, dciwh: pl.DataFrame) -> pl.DataFrame:
    """
    Left-join DCIWTB with DCIWH on DCDLR (IF A keeps only DCIWTB records).
    DCIWH columns GFCUN and GFC2R overwrite DCIWTB values where matched.
    IF DCBCCY='MYR' THEN C8SPT=1;
    """
    # Left join: DCIWTB(IN=A) => keep all DCIWTB rows
    merged = dciwtb.join(
        dciwh.rename({"GFCUN": "GFCUN_h", "GFC2R": "GFC2R_h"}),
        on="DCDLR",
        how="left",
    )

    # DCIWH values overwrite DCIWTB values (SAS MERGE behaviour: right-side wins)
    merged = merged.with_columns([
        pl.when(pl.col("GFCUN_h").is_not_null())
          .then(pl.col("GFCUN_h"))
          .otherwise(pl.col("GFCUN"))
          .alias("GFCUN"),
        pl.when(pl.col("GFC2R_h").is_not_null())
          .then(pl.col("GFC2R_h"))
          .otherwise(pl.col("GFC2R"))
          .alias("GFC2R"),
    ]).drop(["GFCUN_h", "GFC2R_h"])

    # IF DCBCCY='MYR' THEN C8SPT=1;
    merged = merged.with_columns(
        pl.when(pl.col("DCBCCY") == "MYR")
          .then(pl.lit(1.0))
          .otherwise(pl.col("C8SPT"))
          .alias("C8SPT")
    )

    return merged


# ==============================================================================
# MAIN
# ==============================================================================

def main() -> None:
    print("Running KALWE ...")

    con = duckdb.connect()

    # ------------------------------------------------------------------
    # DATA BNMK.K1TBL&REPTMON&NOWK  (INFILE BNMTBL1)
    # ------------------------------------------------------------------
    print(f"  Reading K1TBL from: {BNMTBL1_TXT}")
    _, k1tbl = read_gwtbl(BNMTBL1_TXT)
    k1tbl.write_parquet(K1TBL_OUT)
    print(f"  K1TBL written to: {K1TBL_OUT}  ({len(k1tbl)} rows)")

    # ------------------------------------------------------------------
    # DATA BNMK.K3TBL&REPTMON&NOWK  (INFILE BNMTBL3)
    # ------------------------------------------------------------------
    print(f"  Reading K3TBL from: {BNMTBL3_TXT}")
    _, k3tbl = read_k3tbl(BNMTBL3_TXT, K3TBL_COLUMNS, K3TBL_STRING_COLS)
    k3tbl.write_parquet(K3TBL_OUT)
    print(f"  K3TBL written to: {K3TBL_OUT}  ({len(k3tbl)} rows)")

    # ------------------------------------------------------------------
    # DATA BNMK.KWTBL&REPTMON&NOWK  (INFILE BNMTBLW)
    # ------------------------------------------------------------------
    print(f"  Reading KWTBL from: {BNMTBLW_TXT}")
    _, kwtbl = read_gwtbl(BNMTBLW_TXT)
    kwtbl.write_parquet(KWTBL_OUT)
    print(f"  KWTBL written to: {KWTBL_OUT}  ({len(kwtbl)} rows)")

    # ------------------------------------------------------------------
    # DATA DCIWTB  (INFILE DCIWTBL)
    # PROC SORT BY DCDLR
    # DATA DCIWH (KEEP=DCDLR GFCUN GFC2R); SET DCIWH.DCI&REPTMON&NOWK;
    # PROC SORT BY DCDLR
    # DATA BNMK.DCIWTB&REPTMON&NOWK; MERGE DCIWTB(IN=A) DCIWH; BY DCDLR; IF A;
    # IF DCBCCY='MYR' THEN C8SPT=1;
    # ------------------------------------------------------------------
    print(f"  Reading DCIWTB from: {DCIWTBL_TXT}")
    _, dciwtb = read_dciwtb(DCIWTBL_TXT)

    print(f"  Reading DCIWH lookup from: {DCIWH_PARQUET}")
    dciwh = read_dciwh(con)

    dciwtb_out = build_dciwtb(dciwtb, dciwh)
    dciwtb_out.write_parquet(DCIWTB_OUT)
    print(f"  DCIWTB written to: {DCIWTB_OUT}  ({len(dciwtb_out)} rows)")

    con.close()
    print("KALWE complete.")


if __name__ == "__main__":
    main()
