#!/usr/bin/env python3
"""
Program : KALWPBBN
Date    : 18/11/2002
Report  : RDIR PART I  (KAPITI ITEMS)
          QUOTED RATES ON NIDS, OWN BAS
          AND REPOS (BNM TABLES 1,3)

Notes:
  - Processes REPOS from KWTBL merged with UTRP (face value).
  - Processes BA/NIDS from K3TBL (instruments IFD/ILD/ISD/IZD/PBA).
  - Uses local NSRSFDORGMT format (1-3 months => '13'), distinct from KALWPIBN (1-3 => '14').
  - Merges result with previous period K3TBE dataset.
  - Outputs report via PROC PRINTTO (redirected to NSRSTXT file) grouped by GROUP.
  - Also writes permanent datasets BNMK.K3TBLA and BNMK.K3TBLB (with extra columns).
"""

import os
import duckdb
import polars as pl
from datetime import datetime, date

# ==============================================================================
# PATH CONFIGURATION
# ==============================================================================

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR  = os.path.join(BASE_DIR, "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Runtime parameters (replace with actual values or pass via CLI/config)
REPTMON  = "202301"   # Reporting month YYYYMM
NOWK     = "01"       # Week number
REPTMON1 = "202212"   # Previous reporting month YYYYMM
NOWK1    = "04"       # Previous week number
RDATE    = "31/01/2023"
SDESC    = "PUBLIC BANK BERHAD"
PDATE    = date(2023, 1, 31)   # Filter: records with start date > PDATE

# %LET AMTIND='D';
AMTIND = "D"

# Page length for ASA report (default 60 lines per page)
PAGE_LENGTH = 60

# Input parquet paths
KWTBL_PARQUET  = os.path.join(INPUT_DIR, f"KWTBL{REPTMON}{NOWK}.parquet")
UTRP_PARQUET   = os.path.join(INPUT_DIR, f"UTRP{REPTMON}{NOWK}.parquet")
K3TBL_PARQUET  = os.path.join(INPUT_DIR, f"K3TBL{REPTMON}{NOWK}.parquet")
K3TBE_PARQUET  = os.path.join(INPUT_DIR, f"K3TBE{REPTMON1}{NOWK1}.parquet")

# Output paths
K3TBLA_OUT_PARQUET = os.path.join(OUTPUT_DIR, f"K3TBLA{REPTMON}{NOWK}.parquet")  # BNMK.K3TBLA
K3TBLB_OUT_PARQUET = os.path.join(OUTPUT_DIR, f"K3TBLB{REPTMON}{NOWK}.parquet")  # BNMK.K3TBLB
K3TBE_OUT_PARQUET  = os.path.join(OUTPUT_DIR, f"K3TBE{REPTMON}{NOWK}.parquet")   # BNMK.K3TBE
NSRSTXT_OUT        = os.path.join(OUTPUT_DIR, f"KALWPBBN_{REPTMON}{NOWK}_report.txt")  # PROC PRINTTO PRINT=NSRSTXT


# ==============================================================================
# PROC FORMAT: NSRSFDORGMT  (KALWPBBN version: 1-3 months => '13')
#
#  LOW - 1   = '12'   /* ORG. MAT. <= 1 MTH      */
#    1 - 3   = '13'   /* ORG. MAT. >1 - 3 MTHS   */
#    3 - 6   = '15'   /* ORG. MAT. >3 - 6 MTHS   */
#    6 - 9   = '16'   /* ORG. MAT. >6 - 9 MTHS   */
#    9 - 12  = '17'   /* ORG. MAT. >9 - 12 MTHS  */
#   12 - 15  = '21'   /* ORG. MAT. >12 - 15 MTHS */
#   15 - 18  = '22'   /* ORG. MAT. >15 - 18 MTHS */
#   18 - 24  = '23'   /* ORG. MAT. >18 - 24 MTHS */
#   24 - 36  = '24'   /* ORG. MAT. >24 - 36 MTHS */
#   36 - 48  = '25'   /* ORG. MAT. >36 - 48 MTHS */
#   48 - 60  = '26'   /* ORG. MAT. >48 - 60 MTHS */
#   60 - HIGH = '30'; /* ORG. MAT. > 60 MTHS     */
# ==============================================================================

def nsrsfdorgmt_format(remmth: float | None) -> str:
    """Apply NSRSFDORGMT (KALWPBBN version) format to remaining months value."""
    if remmth is None:
        return ''
    if remmth <= 1:
        return '12'
    elif remmth <= 3:
        return '13'   # KALWPBBN: 1-3 mths => '13'
    elif remmth <= 6:
        return '15'
    elif remmth <= 9:
        return '16'
    elif remmth <= 12:
        return '17'
    elif remmth <= 15:
        return '21'
    elif remmth <= 18:
        return '22'
    elif remmth <= 24:
        return '23'
    elif remmth <= 36:
        return '24'
    elif remmth <= 48:
        return '25'
    elif remmth <= 60:
        return '26'
    else:
        return '30'


# ==============================================================================
# PROC FORMAT: $NIDSDESC
# '13' = 'ORG. MAT. >1 - 3 MTHS'
# '15' = 'ORG. MAT. >3 - 6 MTHS'
# '16' = 'ORG. MAT. >6 - 9 MTHS'
# '17' = 'ORG. MAT. >9 - 12 MTHS'
# ==============================================================================

NIDSDESC: dict[str, str] = {
    '13': 'ORG. MAT. >1 - 3 MTHS',
    '15': 'ORG. MAT. >3 - 6 MTHS',
    '16': 'ORG. MAT. >6 - 9 MTHS',
    '17': 'ORG. MAT. >9 - 12 MTHS',
}

# ==============================================================================
# PROC FORMAT: $BASDESC
# '12' = 'ORG. MAT. <= 1 MTH'
# '15' = 'ORG. MAT. >1 - 3 MTHS'
# '16' = 'ORG. MAT. >3 - 6 MTHS'
# ==============================================================================

BASDESC: dict[str, str] = {
    '12': 'ORG. MAT. <= 1 MTH',
    '15': 'ORG. MAT. >1 - 3 MTHS',
    '16': 'ORG. MAT. >3 - 6 MTHS',
}

# ==============================================================================
# PROC FORMAT: $SBBADESC
# '11' = 'OVERNIGHT'
# '18' = '> OVERNIGHT TO 1 WK'
# '19' = '> 1 TO 2 WKS'
# '27' = '> 2 TO 3 WKS'
# '28' = '> 3 TO 4 WKS'
# ==============================================================================

SBBADESC: dict[str, str] = {
    '11': 'OVERNIGHT',
    '18': '> OVERNIGHT TO 1 WK',
    '19': '> 1 TO 2 WKS',
    '27': '> 2 TO 3 WKS',
    '28': '> 3 TO 4 WKS',
}


# ==============================================================================
# MACRO DCLVAR - Day arrays used in REMMTH calculation
# RETAIN D1-D12 31, D4 D6 D9 D11 30
#        RD1-RD12 MD1-MD12 31, RD2 MD2 28, RD4 RD6 RD9 RD11 MD4 MD6 MD9 MD11 30
# RPDAYS is the array used in REMMTH (= RD1-RD12)
# ==============================================================================

RPDAYS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]  # RD1-RD12


# ==============================================================================
# MACRO REMMTH - Calculate remaining months to maturity
# ==============================================================================

def calc_remmth(matdte: date, issdte: date) -> float:
    """
    Replicates SAS %REMMTH macro.
    REMMTH = (MDYR - RPYR)*12 + (MDMTH - RPMTH) + (MDDAY - RPDAY) / RPDAYS[RPMTH]
    RPDAYS is the retained array RD1-RD12 (0-indexed by RPMTH-1).
    """
    mdyr  = matdte.year
    mdmth = matdte.month
    mdday = matdte.day

    rpyr  = issdte.year
    rpmth = issdte.month
    rpday = issdte.day

    remy = mdyr  - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday

    # RPDAYS(RPMTH) is 1-based in SAS, 0-based here
    rpdays_val = RPDAYS[rpmth - 1]

    remmth = remy * 12 + remm + remd / rpdays_val
    return remmth


# ==============================================================================
# HELPER: parse date string in DDMMYY10 format (DD/MM/YYYY or DDMMYYYY)
# ==============================================================================

def parse_ddmmyy10(val) -> date | None:
    """Parse SAS DDMMYY10. informat: 'DD/MM/YYYY'."""
    if val is None:
        return None
    s = str(val).strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d%m%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


# ==============================================================================
# QUOTED RATES ON REPOS
# PROC SORT DATA=BNMK.KWTBL&REPTMON&NOWK OUT=K1TBL NODUPKEY; BY GWDLR;
# PROC SORT DATA=BNMKD.UTRP&REPTMON&NOWK OUT=UTRP NODUPKEY; BY DEALREF;
# DATA K1TBL; MERGE K1TBL(IN=A) UTRP(KEEP=DEALREF FACEVALU RENAME=(DEALREF=GWDLR)); BY GWDLR; IF A;
# DATA K1TBLQ (KEEP=BNMCODE AMOUNT BNMC WAMT AMTIND); SET K1TBL (RENAME=(FACEVALU=AMOUNT));
# ==============================================================================

def process_k1tblq(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """
    Process KWTBL merged with UTRP to generate K1TBLQ (REPOS quoted rates).
    Returns DataFrame with columns: BNMCODE, AMOUNT, BNMC, WAMT, AMTIND
    """
    # PROC SORT NODUPKEY BY GWDLR on KWTBL; PROC SORT NODUPKEY BY DEALREF on UTRP
    # DATA K1TBL: MERGE K1TBL(IN=A) UTRP RENAME=(DEALREF=GWDLR); BY GWDLR; IF A;
    k1tbl = con.execute(f"""
        SELECT k.*,
               COALESCE(u.FACEVALU, k.FACEVALU) AS AMOUNT
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY GWDLR ORDER BY GWDLR) AS rn
            FROM read_parquet('{KWTBL_PARQUET}')
        ) k
        LEFT JOIN (
            SELECT DEALREF, FACEVALU,
                   ROW_NUMBER() OVER (PARTITION BY DEALREF ORDER BY DEALREF) AS rn2
            FROM read_parquet('{UTRP_PARQUET}')
        ) u ON k.GWDLR = u.DEALREF AND u.rn2 = 1
        WHERE k.rn = 1
    """).pl()

    rows = []
    for row in k1tbl.iter_rows(named=True):
        gwsdt = row.get("GWSDT")
        if isinstance(gwsdt, str):
            gwsdt = parse_ddmmyy10(gwsdt)

        if gwsdt is None or gwsdt <= PDATE:
            continue

        gwmdt = row.get("GWMDT")
        if isinstance(gwmdt, str):
            gwmdt = parse_ddmmyy10(gwmdt)
        if isinstance(gwsdt, date) and isinstance(gwmdt, date):
            days = (gwmdt - gwsdt).days
        else:
            continue

        gwdlp = str(row.get("GWDLP", ""))
        # SUBSTR(GWDLP,2,2) - SAS is 1-based, chars at positions 2-3
        gwdlp_sub = gwdlp[1:3] if len(gwdlp) >= 3 else ""

        # IF SUBSTR(GWDLP,2,2) IN ('MI','MT','MA','MQ') AND DAYS > 0
        if gwdlp_sub not in {"MI", "MT", "MA", "MQ"}:
            continue
        if days <= 0:
            continue

        amount = row.get("AMOUNT")
        gwratc = row.get("GWRATC")
        if amount is None or gwratc is None:
            continue

        wamt = (amount * gwratc) / 100

        bnmc = None
        if days == 1:
            bnmc = "11"
        elif 2 <= days <= 7:
            bnmc = "18"
        elif 7 < days <= 14:
            bnmc = "19"
        elif 14 < days <= 21:
            bnmc = "27"
        elif 21 < days <= 28:
            bnmc = "28"

        if wamt is not None and bnmc in ("11", "18", "19", "27", "28"):
            bnmcode = f"8430800{bnmc}0000Y"
            rows.append({
                "BNMCODE": bnmcode,
                "AMOUNT":  amount,
                "BNMC":    bnmc,
                "WAMT":    wamt,
                "AMTIND":  AMTIND,
            })

    schema = {
        "BNMCODE": pl.Utf8,
        "AMOUNT":  pl.Float64,
        "BNMC":    pl.Utf8,
        "WAMT":    pl.Float64,
        "AMTIND":  pl.Utf8,
    }
    if rows:
        return pl.DataFrame(rows, schema=schema)
    return pl.DataFrame(schema=schema)


# ==============================================================================
# QUOTED RATES ON BA (BANKERS ACCEPTANCE) AND NIDS
# PROC SORT DATA=BNMKD.K3TBL&REPTMON&NOWK OUT=K3TBL; BY UTDLR EXTDATE;
# PROC SORT DATA=K3TBL NODUPKEY; BY UTDLR;
# DATA K3TBLA (KEEP=BNMCODE AMOUNT OMTH WAMT AMTIND)
#      K3TBLB (KEEP=BNMCODE AMOUNT OMTH WAMT AMTIND)
#      BNMK.K3TBLA (KEEP=BNMCODE AMOUNT OMTH WAMT AMTIND ORIYY ORIMM ORIDD)
#      BNMK.K3TBLB (KEEP=BNMCODE AMOUNT OMTH WAMT AMTIND ORIYY ORIMM ORIDD);
# ==============================================================================

def process_k3tbl(con: duckdb.DuckDBPyConnection) -> tuple[pl.DataFrame, pl.DataFrame,
                                                            pl.DataFrame, pl.DataFrame]:
    """
    Process K3TBL to generate:
      k3tbla      - IFD/ILD/ISD/IZD instruments (for summary, columns: BNMCODE/AMOUNT/OMTH/WAMT/AMTIND)
      k3tblb      - PBA instruments (for summary, same columns)
      k3tbla_full - BNMK.K3TBLA permanent dataset (adds ORIYY/ORIMM/ORIDD)
      k3tblb_full - BNMK.K3TBLB permanent dataset (adds ORIYY/ORIMM/ORIDD)
    PROC SORT BY UTDLR EXTDATE then NODUPKEY BY UTDLR (keep first by EXTDATE).
    """
    k3tbl = con.execute(f"""
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY UTDLR ORDER BY EXTDATE ASC) AS rn
            FROM read_parquet('{K3TBL_PARQUET}')
        )
        WHERE rn = 1
    """).pl()

    rows_a      = []   # for K3TBLA (summary columns only)
    rows_b      = []   # for K3TBLB (summary columns only)
    rows_a_full = []   # for BNMK.K3TBLA (includes ORIYY/ORIMM/ORIDD)
    rows_b_full = []   # for BNMK.K3TBLB (includes ORIYY/ORIMM/ORIDD)

    for row in k3tbl.iter_rows(named=True):
        issdt = row.get("ISSDT")
        if isinstance(issdt, str):
            issdt = parse_ddmmyy10(issdt)
        if issdt is None or issdt <= PDATE:
            continue

        utdlp = str(row.get("UTDLP", ""))
        if utdlp == "MOS":
            continue

        utsty = str(row.get("UTSTY", ""))
        if utsty not in ("IFD", "ILD", "ISD", "IZD", "PBA"):
            continue

        # IF UTSTY='ILD' THEN UTQDS=UTCPR
        utqds = row.get("UTQDS")
        if utsty == "ILD":
            utqds = row.get("UTCPR")

        amount = row.get("UTFCV")
        if amount is None or utqds is None:
            continue

        wamt = (utqds * amount) / 100

        matdte = parse_ddmmyy10(row.get("UTMDT"))
        issdte = parse_ddmmyy10(row.get("UTOSD"))

        if matdte is None or issdte is None:
            continue

        rpyr  = issdte.year
        rpmth = issdte.month
        rpday = issdte.day

        if utsty in ("IFD", "ILD", "ISD", "IZD"):
            # %REMMTH macro then OMTH=PUT(REMMTH,NSRSFDORGMT.)
            remmth = calc_remmth(matdte, issdte)
            omth = nsrsfdorgmt_format(remmth)

            if wamt is not None and omth in ("13", "15", "16", "17"):
                bnmcode = f"8430600{omth}0000Y"
                rec = {
                    "BNMCODE": bnmcode,
                    "AMOUNT":  amount,
                    "OMTH":    omth,
                    "WAMT":    wamt,
                    "AMTIND":  AMTIND,
                }
                rows_a.append(rec)
                # BNMK.K3TBLA: ORIYY/ORIMM/ORIDD not computed for IFD/ILD/ISD/IZD path
                rows_a_full.append({**rec, "ORIYY": None, "ORIMM": None, "ORIDD": None})

        elif utsty == "PBA":
            matdd = matdte.day
            matmm = matdte.month
            matyy = matdte.year

            oridd = matdd - rpday
            orimm = matmm - rpmth
            oriyy = matyy - rpyr

            if oriyy >= 1:
                orimm = orimm + 12

            omth = None
            if   (orimm == 1 and oridd <= 0) or (orimm == 0 and oridd > 0):
                omth = "12"
            elif (orimm == 2 and oridd <= 0) or (orimm == 1 and oridd > 0):
                omth = "15"
            elif (orimm == 3 and oridd <= 0) or (orimm == 2 and oridd > 0):
                omth = "15"
            elif (orimm == 4 and oridd <= 0) or (orimm == 3 and oridd > 0):
                omth = "16"
            elif (orimm == 5 and oridd <= 0) or (orimm == 4 and oridd > 0):
                omth = "16"
            elif (orimm == 6 and oridd <= 0) or (orimm == 5 and oridd > 0):
                omth = "16"

            if wamt is not None and omth in ("12", "15", "16"):
                bnmcode = f"8430700{omth}0000Y"
                rec = {
                    "BNMCODE": bnmcode,
                    "AMOUNT":  amount,
                    "OMTH":    omth,
                    "WAMT":    wamt,
                    "AMTIND":  AMTIND,
                }
                rows_b.append(rec)
                rows_b_full.append({**rec, "ORIYY": oriyy, "ORIMM": orimm, "ORIDD": oridd})

    schema_ab = {
        "BNMCODE": pl.Utf8, "AMOUNT": pl.Float64, "OMTH": pl.Utf8,
        "WAMT": pl.Float64, "AMTIND": pl.Utf8,
    }
    schema_full = {
        "BNMCODE": pl.Utf8, "AMOUNT": pl.Float64, "OMTH": pl.Utf8,
        "WAMT": pl.Float64, "AMTIND": pl.Utf8,
        "ORIYY": pl.Int64, "ORIMM": pl.Int64, "ORIDD": pl.Int64,
    }

    k3tbla      = pl.DataFrame(rows_a,      schema=schema_ab)   if rows_a      else pl.DataFrame(schema=schema_ab)
    k3tblb      = pl.DataFrame(rows_b,      schema=schema_ab)   if rows_b      else pl.DataFrame(schema=schema_ab)
    k3tbla_full = pl.DataFrame(rows_a_full, schema=schema_full) if rows_a_full else pl.DataFrame(schema=schema_full)
    k3tblb_full = pl.DataFrame(rows_b_full, schema=schema_full) if rows_b_full else pl.DataFrame(schema=schema_full)

    return k3tbla, k3tblb, k3tbla_full, k3tblb_full


# ==============================================================================
# PROC SUMMARY DATA=K3TBL3 NWAY;
#   CLASS BNMCODE AMTIND; VAR AMOUNT WAMT; OUTPUT OUT=K3TBL3 SUM=BALANCE WAMT;
# DATA K3TBL3 (KEEP=BNMCODE AMOUNT AMTIND);
#   AMOUNT=(WAMT/BALANCE)*100;
# ==============================================================================

def summarize_and_compute(k3tbl3: pl.DataFrame) -> pl.DataFrame:
    """Replicates PROC SUMMARY NWAY + weighted average rate computation."""
    summary = (
        k3tbl3
        .group_by(["BNMCODE", "AMTIND"])
        .agg([
            pl.col("AMOUNT").sum().alias("BALANCE"),
            pl.col("WAMT").sum().alias("WAMT"),
        ])
    )
    summary = summary.with_columns(
        (pl.col("WAMT") / pl.col("BALANCE") * 100).alias("AMOUNT")
    ).select(["BNMCODE", "AMOUNT", "AMTIND"])

    return summary


# ==============================================================================
# PROC SORT DATA=K3TBL3; BY BNMCODE;
# PROC SORT DATA=BNMK.K3TBE&REPTMON1&NOWK1 OUT=K3TBLE; BY BNMCODE;
# DATA K3TBL3; MERGE K3TBLE K3TBL3; BY BNMCODE;
#   AMOUNT=ROUND(AMOUNT,0.0001);
#   FORMAT AMOUNT 7.4;
#   IF SUBSTR(BNMCODE,1,5) = '84306' THEN GROUP/DESCRIPTION = ...
#   ELSE IF SUBSTR(BNMCODE,1,5) = '84307' ...
#   ELSE IF SUBSTR(BNMCODE,1,5) = '84308' ...
# ==============================================================================

def merge_and_enrich(con: duckdb.DuckDBPyConnection, k3tbl3: pl.DataFrame) -> pl.DataFrame:
    """
    Merge with previous period K3TBE, round AMOUNT, and add GROUP/DESCRIPTION columns.
    SAS MERGE (non-IN guarded): all keys from both datasets; right side updates left.
    """
    k3tble = con.execute(f"""
        SELECT BNMCODE, AMOUNT, AMTIND
        FROM read_parquet('{K3TBE_PARQUET}')
        ORDER BY BNMCODE
    """).pl()

    # Full outer join; current period values overwrite previous period values
    merged = (
        k3tble
        .join(k3tbl3, on="BNMCODE", how="full", suffix="_curr")
        .with_columns([
            pl.when(pl.col("AMOUNT_curr").is_not_null())
              .then(pl.col("AMOUNT_curr"))
              .otherwise(pl.col("AMOUNT"))
              .alias("AMOUNT"),
            pl.when(pl.col("AMTIND_curr").is_not_null())
              .then(pl.col("AMTIND_curr"))
              .otherwise(pl.col("AMTIND"))
              .alias("AMTIND"),
        ])
        .select(["BNMCODE", "AMOUNT", "AMTIND"])
        .sort("BNMCODE")
    )

    # AMOUNT=ROUND(AMOUNT,0.0001); FORMAT AMOUNT 7.4;
    merged = merged.with_columns(
        (pl.col("AMOUNT") * 10000).round(0) / 10000
    )

    # Add GROUP and DESCRIPTION based on BNMCODE prefix (SUBSTR(BNMCODE,1,5))
    groups: list[str] = []
    descs:  list[str] = []
    for row in merged.iter_rows(named=True):
        bnmcode  = str(row.get("BNMCODE", "") or "")
        prefix   = bnmcode[:5]
        # SUBSTR(BNMCODE,8,2) is SAS 1-based chars 8-9 => Python [7:9]
        omth_key = bnmcode[7:9]

        if prefix == "84306":
            groups.append("QUOTED RATES ON NIDS/INIDS ISSUED")
            descs.append(NIDSDESC.get(omth_key, ""))
        elif prefix == "84307":
            groups.append("QUOTED RATES ON OWN BAS/IBAS")
            descs.append(BASDESC.get(omth_key, ""))
        elif prefix == "84308":
            groups.append("QUOTED RATES ON REPOS/SBBA ORIGINAL")
            descs.append(SBBADESC.get(omth_key, ""))
        else:
            groups.append("")
            descs.append("")

    merged = merged.with_columns([
        pl.Series("GROUP",       groups),
        pl.Series("DESCRIPTION", descs),
    ])

    return merged


# ==============================================================================
# PROC PRINTTO PRINT=NSRSTXT NEW; RUN;
# PROC PRINT DATA=K3TBL3; BY GROUP;
#   VAR BNMCODE DESCRIPTION AMTIND AMOUNT;
# OPTIONS NOCENTER NONUMBER NODATE;
# TITLE1 &SDESC;
# TITLE2 'REPORT ON DOMESTIC INTEREST RATE - PART I (KAPITI)';
# TITLE3 'REPORTING DATE :' &RDATE;
# TITLE4;
# ==============================================================================

def format_report(df: pl.DataFrame) -> str:
    """
    Generate report text matching SAS PROC PRINT BY GROUP output with ASA carriage control.
    ASA control chars: '1' = new page, ' ' = single space, '0' = double space.
    Page length = 60 lines.
    """
    lines:      list[str] = []
    line_count: int       = 0
    page_num:   int       = 0

    def emit_page_header() -> None:
        nonlocal line_count, page_num
        page_num += 1
        # '1' = ASA form feed (new page) on first char of line
        lines.append(f"1{SDESC}")
        lines.append(f" REPORT ON DOMESTIC INTEREST RATE - PART I (KAPITI)")
        lines.append(f" REPORTING DATE : {RDATE}")
        lines.append(f" ")
        lines.append(f" ")
        lines.append(f" {'BNMCODE':<16}  {'DESCRIPTION':<30}  {'AMTIND':<6}  {'AMOUNT':>7}")
        lines.append(f" ")
        line_count = 7

    def check_page_break() -> None:
        if line_count >= PAGE_LENGTH:
            emit_page_header()

    # Sort by GROUP then BNMCODE to replicate BY GROUP processing
    df_sorted = df.sort(["GROUP", "BNMCODE"])

    current_group: str | None = None

    # First page header
    emit_page_header()

    for row in df_sorted.iter_rows(named=True):
        group       = str(row.get("GROUP",       "") or "")
        bnmcode     = str(row.get("BNMCODE",     "") or "")
        description = str(row.get("DESCRIPTION", "") or "")
        amtind      = str(row.get("AMTIND",      "") or "")
        amount      = row.get("AMOUNT")

        # BY GROUP break: emit group header line
        if group != current_group:
            if current_group is not None:
                # '0' = ASA double space before new group separator
                lines.append("0")
                line_count += 1
                check_page_break()
            lines.append(f" GROUP={group}")
            lines.append(f" ")
            line_count += 2
            check_page_break()
            current_group = group

        amount_str = f"{amount:7.4f}" if amount is not None else f"{'':>7}"
        data_line  = (
            f" {bnmcode:<16}  {description:<30}  {amtind:<6}  {amount_str:>7}"
        )
        lines.append(data_line)
        line_count += 1
        check_page_break()

    return "\n".join(lines) + "\n"


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    print("Running KALWPBBN ...")

    con = duckdb.connect()

    # -- QUOTED RATES ON REPOS --
    k1tblq = process_k1tblq(con)

    # -- QUOTED RATES ON BA AND NIDS --
    # Also produces permanent BNMK.K3TBLA / BNMK.K3TBLB with extra columns
    k3tbla, k3tblb, k3tbla_full, k3tblb_full = process_k3tbl(con)

    # DATA K3TBL3; SET K3TBLA K3TBLB K1TBLQ;
    k3tbla_union = k3tbla.select(["BNMCODE", "AMOUNT", "WAMT", "AMTIND"])
    k3tblb_union = k3tblb.select(["BNMCODE", "AMOUNT", "WAMT", "AMTIND"])
    k1tblq_union = k1tblq.select(["BNMCODE", "AMOUNT", "WAMT", "AMTIND"])

    k3tbl3 = pl.concat([k3tbla_union, k3tblb_union, k1tblq_union], how="vertical")

    # PROC SUMMARY + weighted average rate
    k3tbl3 = summarize_and_compute(k3tbl3)

    # PROC SORT + MERGE with K3TBE + enrich with GROUP/DESCRIPTION
    k3tbl3 = merge_and_enrich(con, k3tbl3)

    con.close()

    # PROC PRINTTO PRINT=NSRSTXT NEW; PROC PRINT DATA=K3TBL3; BY GROUP;
    report_text = format_report(k3tbl3)
    with open(NSRSTXT_OUT, "w", encoding="utf-8") as f:
        f.write(report_text)
    print(f"Report (NSRSTXT) written to: {NSRSTXT_OUT}")

    # DATA BNMK.K3TBE&REPTMON&NOWK; SET K3TBL3;
    k3tbl3.write_parquet(K3TBE_OUT_PARQUET)
    print(f"Output dataset K3TBE written to: {K3TBE_OUT_PARQUET}")

    # BNMK.K3TBLA and BNMK.K3TBLB permanent datasets
    k3tbla_full.write_parquet(K3TBLA_OUT_PARQUET)
    print(f"Output dataset K3TBLA written to: {K3TBLA_OUT_PARQUET}")

    k3tblb_full.write_parquet(K3TBLB_OUT_PARQUET)
    print(f"Output dataset K3TBLB written to: {K3TBLB_OUT_PARQUET}")


if __name__ == "__main__":
    main()
