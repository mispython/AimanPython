#!/usr/bin/env python3
"""
Program : KALWPIBS
Date    : 18/11/2002
Report  : RDIR PART I  (KAPITI ITEMS)
          QUOTED RATES ON NIDS, OWN BAS
          AND REPOS (BNM TABLES 1,3)

Dependencies:
  - PBBDPFMT : Product mapping and format definitions
               Provides fdorgmt_format() used to bucket remaining months (REMMTH)
               into 2-character BNM codes for BNMCODE construction.
"""

import os
import duckdb
import polars as pl
from datetime import datetime, date

from PBBDPFMT import fdorgmt_format

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
K3TBL_PARQUET  = os.path.join(INPUT_DIR, f"K3TBL{REPTMON}{NOWK}.parquet")
K3TBD_PARQUET  = os.path.join(INPUT_DIR, f"K3TBD{REPTMON1}{NOWK1}.parquet")

# Output paths
K3TBD_OUT_PARQUET = os.path.join(OUTPUT_DIR, f"K3TBD{REPTMON}{NOWK}.parquet")
REPORT_OUT_TXT    = os.path.join(OUTPUT_DIR, f"KALWPIBS_{REPTMON}{NOWK}_report.txt")


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
# DATA K1TBLQ (KEEP=BNMCODE AMOUNT BNMC WAMT AMTIND);
#   SET BNMK.KWTBL&REPTMON&NOWK (RENAME=(GWBALC=AMOUNT));
# Note: KALWPIBS reads KWTBL directly using GWBALC as AMOUNT (no UTRP merge).
#       Filter: SUBSTR(GWDLP,2,2) IN ('MI','MT') only (narrower than KALWPBBS).
# ==============================================================================

def process_k1tblq(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """
    Process KWTBL to generate K1TBLQ (REPOS quoted rates).
    Uses GWBALC renamed to AMOUNT. No UTRP merge.
    Returns DataFrame with columns: BNMCODE, AMOUNT, BNMC, WAMT, AMTIND
    """
    # RENAME=(GWBALC=AMOUNT)
    kwtbl = con.execute(f"""
        SELECT *, GWBALC AS AMOUNT
        FROM read_parquet('{KWTBL_PARQUET}')
    """).pl()

    rows = []
    for row in kwtbl.iter_rows(named=True):
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

        # IF SUBSTR(GWDLP,2,2) IN ('MI','MT') AND DAYS > 0
        if gwdlp_sub not in {"MI", "MT"}:
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
# DATA K3TBLA (KEEP=BNMCODE AMOUNT OMTH WAMT AMTIND)
#      K3TBLB (KEEP=BNMCODE AMOUNT OMTH WAMT AMTIND);
#   SET BNMK.K3TBL&REPTMON&NOWK;
# Note: KALWPIBS reads K3TBL directly with no sort/dedup step.
# ==============================================================================

def process_k3tbl(con: duckdb.DuckDBPyConnection) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Process K3TBL to generate K3TBLA (IFD/ILD/ISD/IZD) and K3TBLB (PBA).
    KALWPIBS reads BNMK.K3TBL directly (no sort or dedup in SAS).
    Returns (k3tbla, k3tblb) DataFrames.
    """
    k3tbl = con.execute(f"""
        SELECT * FROM read_parquet('{K3TBL_PARQUET}')
    """).pl()

    rows_a = []
    rows_b = []

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
            # %REMMTH macro then OMTH=PUT(REMMTH,FDORGMT.)
            remmth = calc_remmth(matdte, issdte)
            omth = fdorgmt_format(remmth)

            if wamt is not None and omth in ("14", "15", "16", "17"):
                bnmcode = f"8430600{omth}0000Y"
                rows_a.append({
                    "BNMCODE": bnmcode,
                    "AMOUNT":  amount,
                    "OMTH":    omth,
                    "WAMT":    wamt,
                    "AMTIND":  AMTIND,
                })

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
                omth = "13"
            elif (orimm == 3 and oridd <= 0) or (orimm == 2 and oridd > 0):
                omth = "14"
            elif (orimm == 4 and oridd <= 0) or (orimm == 3 and oridd > 0):
                omth = "35"
            elif (orimm == 5 and oridd <= 0) or (orimm == 4 and oridd > 0):
                omth = "36"
            elif (orimm == 6 and oridd <= 0) or (orimm == 5 and oridd > 0):
                omth = "37"

            if wamt is not None and omth in ("12", "13", "14", "35", "36", "37"):
                bnmcode = f"8430700{omth}0000Y"
                rows_b.append({
                    "BNMCODE": bnmcode,
                    "AMOUNT":  amount,
                    "OMTH":    omth,
                    "WAMT":    wamt,
                    "AMTIND":  AMTIND,
                })

    schema_a = {"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64, "OMTH": pl.Utf8, "WAMT": pl.Float64, "AMTIND": pl.Utf8}
    schema_b = {"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64, "OMTH": pl.Utf8, "WAMT": pl.Float64, "AMTIND": pl.Utf8}

    k3tbla = pl.DataFrame(rows_a, schema=schema_a) if rows_a else pl.DataFrame(schema=schema_a)
    k3tblb = pl.DataFrame(rows_b, schema=schema_b) if rows_b else pl.DataFrame(schema=schema_b)
    return k3tbla, k3tblb


# ==============================================================================
# PROC SUMMARY DATA=K3TBL3 NWAY;
#   CLASS BNMCODE AMTIND; VAR AMOUNT WAMT; OUTPUT OUT=K3TBL3 SUM=BALANCE WAMT;
# DATA K3TBL3 (KEEP=BNMCODE AMOUNT AMTIND);
#   AMOUNT=(WAMT/BALANCE)*100;
# ==============================================================================

def summarize_and_compute(k3tbl3: pl.DataFrame) -> pl.DataFrame:
    """
    Replicates PROC SUMMARY NWAY + weighted average rate computation.
    """
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
# PROC SORT DATA=BNMK.K3TBD&REPTMON1&NOWK1 OUT=K3TBLD; BY BNMCODE;
# DATA K3TBL3; MERGE K3TBLD K3TBL3; BY BNMCODE;
# ==============================================================================

def merge_with_previous(con: duckdb.DuckDBPyConnection, k3tbl3: pl.DataFrame) -> pl.DataFrame:
    """
    Merge previous period K3TBD with current period K3TBL3 by BNMCODE.
    SAS MERGE (non-IN guarded): all keys from both datasets; right side updates left.
    """
    k3tbld = con.execute(f"""
        SELECT BNMCODE, AMOUNT, AMTIND
        FROM read_parquet('{K3TBD_PARQUET}')
        ORDER BY BNMCODE
    """).pl()

    # Full outer join; current period values overwrite previous period values
    merged = (
        k3tbld
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
    return merged


# ==============================================================================
# PROC PRINT replication with ASA carriage control characters
# OPTIONS NOCENTER NONUMBER NODATE;
# TITLE1 &SDESC;
# TITLE2 'REPORT ON DOMESTIC INTEREST RATE - PART I (KAPITI)';
# TITLE3 'REPORTING DATE :' &RDATE;
# TITLE4;
# ==============================================================================

def format_report(df: pl.DataFrame) -> str:
    """
    Generate report text matching SAS PROC PRINT output with ASA carriage control.
    ASA control chars: '1' = new page, ' ' = single space, '0' = double space.
    Page length = 60 lines.
    """
    lines = []
    line_count = 0
    page_num = 0

    def emit_page_header():
        nonlocal line_count, page_num
        page_num += 1
        header_lines = [
            f"1{SDESC}",
            f" REPORT ON DOMESTIC INTEREST RATE - PART I (KAPITI)",
            f" REPORTING DATE : {RDATE}",
            f" ",
            f" ",
            f" OBS    BNMCODE             AMOUNT    AMTIND",
            f" ",
        ]
        for hl in header_lines:
            lines.append(hl)
        line_count = len(header_lines)

    # First page header
    emit_page_header()

    for obs_idx, row in enumerate(df.iter_rows(named=True), start=1):
        bnmcode = str(row.get("BNMCODE", "") or "")
        amount  = row.get("AMOUNT")
        amtind  = str(row.get("AMTIND", "") or "")

        amount_str = f"{amount:>12.4f}" if amount is not None else f"{'':>12}"
        data_line = f" {obs_idx:<6d} {bnmcode:<18} {amount_str}    {amtind}"
        lines.append(data_line)
        line_count += 1

        # Page break when page length reached
        if line_count >= PAGE_LENGTH:
            emit_page_header()

    return "\n".join(lines) + "\n"


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    print("Running KALWPIBS ...")

    con = duckdb.connect()

    # -- QUOTED RATES ON REPOS --
    k1tblq = process_k1tblq(con)

    # -- QUOTED RATES ON BA AND NIDS --
    k3tbla, k3tblb = process_k3tbl(con)

    # DATA K3TBL3; SET K3TBLA K3TBLB K1TBLQ;
    k3tbla_union = k3tbla.select(["BNMCODE", "AMOUNT", "WAMT", "AMTIND"])
    k3tblb_union = k3tblb.select(["BNMCODE", "AMOUNT", "WAMT", "AMTIND"])
    k1tblq_union = k1tblq.select(["BNMCODE", "AMOUNT", "WAMT", "AMTIND"])

    k3tbl3 = pl.concat([k3tbla_union, k3tblb_union, k1tblq_union], how="vertical")

    # PROC SUMMARY + weighted average rate
    k3tbl3 = summarize_and_compute(k3tbl3)

    # PROC SORT + MERGE with previous period K3TBD
    k3tbl3 = merge_with_previous(con, k3tbl3)

    con.close()

    # OPTIONS NOCENTER NONUMBER NODATE; PROC PRINT DATA=K3TBL3;
    report_text = format_report(k3tbl3)
    with open(REPORT_OUT_TXT, "w", encoding="utf-8") as f:
        f.write(report_text)
    print(f"Report written to: {REPORT_OUT_TXT}")

    # DATA BNMK.K3TBD&REPTMON&NOWK; SET K3TBL3;
    k3tbl3.write_parquet(K3TBD_OUT_PARQUET)
    print(f"Output dataset written to: {K3TBD_OUT_PARQUET}")


if __name__ == "__main__":
    main()
