# !/usr/bin/env python3
"""
PROGRAM : KALWEI (KALWE - Islamic/Extended variant)
DATE    : 01.11.99
REPORT  : TO EXTRACT DATA FROM KAPITI TABLE 1 & 3 (WEEKLY)

Differences from KALWE:
  - K3TBL has an extra column UTBRNM ($3. BRANCH) between UTREF and UTDLP.
  - K3TBL has an extra column UTMM1 ($3. MARKET MARKER 1) at the end.
  - No DCIWTB / DCIWH section (only 3 output datasets: K1TBL, K3TBL, KWTBL).
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
BNMTBL1_TXT = os.path.join(INPUT_DIR, "BNMTBL1.txt")   # Kapiti Table 1
BNMTBL3_TXT = os.path.join(INPUT_DIR, "BNMTBL3.txt")   # Kapiti Table 3 (Islamic/extended layout)
BNMTBLW_TXT = os.path.join(INPUT_DIR, "BNMTBLW.txt")   # Kapiti Table W (weekly)

# Output parquet paths
K1TBL_OUT = os.path.join(OUTPUT_DIR, f"K1TBL{REPTMON}{NOWK}.parquet")   # BNMK.K1TBL
K3TBL_OUT = os.path.join(OUTPUT_DIR, f"K3TBL{REPTMON}{NOWK}.parquet")   # BNMK.K3TBL
KWTBL_OUT = os.path.join(OUTPUT_DIR, f"KWTBL{REPTMON}{NOWK}.parquet")   # BNMK.KWTBL

# OPTIONS YEARCUTOFF=1950
# Dates with 2-digit years: if year >= 50 => 19xx, else => 20xx
YEARCUTOFF = 1950


# ==============================================================================
# HELPER: parse YYMMDD8 integer as stored in raw file (e.g. 20230131 -> date)
# SAS stores GWSDT/GWMDT as raw 8-digit integers (YYYYMMDD), then converts:
#   IF GWSDT NE 0 THEN GWSDT = INPUT(PUT(GWSDT, Z8.), YYMMDD8.);
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
# DATA BNMK.K1TBL&REPTMON&NOWK  and  DATA BNMK.KWTBL&REPTMON&NOWK
# INFILE BNMTBL1 / BNMTBLW  DELIMITER='|' DSD MISSOVER;
# First record: REPTDATE (YYMMDD8.)
# Remaining records: all GW* fields
# Post-processing:
#   IF GWSDT NE 0 THEN GWSDT = INPUT(PUT(GWSDT,Z8.),YYMMDD8.);
#   IF GWMDT NE 0 THEN GWMDT = INPUT(PUT(GWMDT,Z8.),YYMMDD8.);
#   FORMAT GWSDT GWMDT YYMMDD8.;
# ==============================================================================

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
# DATA BNMK.K3TBL&REPTMON&NOWK  (KALWEI Islamic/Extended layout)
# INFILE BNMTBL3 DELIMITER='|' DSD MISSOVER;
# First record: REPTDATE (YYMMDD8.)
# Remaining records: UT* fields
#
# IMPORTANT: This layout differs from KALWE in two places:
#   1. UTBRNM ($3. BRANCH) inserted after UTREF, before UTDLP.
#   2. UTMM1  ($3. MARKET MARKER 1) appended at end, after UTAMTS.
#
# Post-processing (identical to KALWE):
#   IF SUBSTR(UTDLP,2,2) IN ('RT','RI') THEN DELETE;
#   MATDT  = INPUT(UTMDT, DDMMYY10.);
#   ISSDT  = INPUT(UTOSD, DDMMYY10.);
#   REPTDATE = INPUT(UTCBD, DDMMYY10.);
#   DDATE    = INPUT(UTCBD, DDMMYY10.);
#   XDATE    = INPUT(UTIDT, DDMMYY10.);
#   FORMAT MATDT ISSDT REPTDATE YYMMDD8.;
#   IF UTSTY='IZD' THEN UTCPR=UTQDS;
#   IF UTSTY IN ('IFD','ILD','ISD','IZD') AND XDATE > DDATE THEN DELETE;
# ==============================================================================

# KALWEI K3TBL column list: UTBRNM added after UTREF, UTMM1 added at end
K3TBL_COLUMNS = [
    "UTSTY", "UTREF",
    "UTBRNM",               # Extra column in KALWEI (BRANCH $3.)
    "UTDLP", "UTDLR", "UTSMN",
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
    "UTMM1",                # Extra column in KALWEI (MARKET MARKER 1 $3.)
]

K3TBL_STRING_COLS = {
    "UTSTY", "UTREF",
    "UTBRNM",               # Extra: BRANCH
    "UTDLP", "UTDLR", "UTSMN",
    "UTCUS", "UTCLC", "UTCTP",
    "UTIDT", "UTLCD", "UTNCD", "UTMDT", "UTCBD",
    "UTOSD",
    "UTCA2", "UTSAC", "UTCNAP", "UTCNAR", "UTCNAL", "UTCCY",
    "UTMM1",                # Extra: MARKET MARKER 1
}

K3TBL_LABELS = {
    "UTSTY":  "SECURITY TYPE",
    "UTREF":  "PORTFOLIO REFERENCE",
    "UTBRNM": "BRANCH",
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
    "UTMM1":  "MARKET MARKER 1",
}


def read_k3tbl(filepath: str) -> tuple[date | None, pl.DataFrame]:
    """
    Read the KALWEI pipe-delimited K3TBL flat file (Islamic/extended layout).
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

            while len(fields) < len(K3TBL_COLUMNS):
                fields.append("")

            rec: dict = {}
            for i, col in enumerate(K3TBL_COLUMNS):
                raw = fields[i].strip() if i < len(fields) else ""
                if col in K3TBL_STRING_COLS:
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
# MAIN
# ==============================================================================

def main() -> None:
    print("Running KALWEI ...")

    # ------------------------------------------------------------------
    # DATA BNMK.K1TBL&REPTMON&NOWK  (INFILE BNMTBL1)
    # ------------------------------------------------------------------
    print(f"  Reading K1TBL from: {BNMTBL1_TXT}")
    _, k1tbl = read_gwtbl(BNMTBL1_TXT)
    k1tbl.write_parquet(K1TBL_OUT)
    print(f"  K1TBL written to: {K1TBL_OUT}  ({len(k1tbl)} rows)")

    # ------------------------------------------------------------------
    # DATA BNMK.K3TBL&REPTMON&NOWK  (INFILE BNMTBL3 - Islamic/extended layout)
    # ------------------------------------------------------------------
    print(f"  Reading K3TBL from: {BNMTBL3_TXT}")
    _, k3tbl = read_k3tbl(BNMTBL3_TXT)
    k3tbl.write_parquet(K3TBL_OUT)
    print(f"  K3TBL written to: {K3TBL_OUT}  ({len(k3tbl)} rows)")

    # ------------------------------------------------------------------
    # DATA BNMK.KWTBL&REPTMON&NOWK  (INFILE BNMTBLW)
    # ------------------------------------------------------------------
    print(f"  Reading KWTBL from: {BNMTBLW_TXT}")
    _, kwtbl = read_gwtbl(BNMTBLW_TXT)
    kwtbl.write_parquet(KWTBL_OUT)
    print(f"  KWTBL written to: {KWTBL_OUT}  ({len(kwtbl)} rows)")

    print("KALWEI complete.")


if __name__ == "__main__":
    main()
