#!/usr/bin/env python3
"""
Program : LALMP124.py
Purpose : Report on Domestic Assets and Liabilities - Part II
          Builds BNM.LALM&REPTMON&NOWK by aggregating loan data
          across multiple dimensions (NPL, customer, sector, collateral,
          approved limit, maturity, purpose, disbursement/repayment, etc.)
          and appending each block into the target LALM parquet dataset.

Dependencies:
  - PBBLNFMT.py : format functions for loan processing.
                  %INC PGM(PBBLNFMT) in the original SAS makes all formats
                  globally available; relevant functions are explicitly
                  imported here.
  - SECTMAP.py  : sector code mapping / rollup.
                  process_sectmap(df) is the single entry-point function
                  equivalent to %INC PGM(SECTMAP) in the SAS source.
                  It runs the full normalise → expand → rollup pipeline
                  and returns the combined ALM+ALMA dataset with SECTORCD
                  updated.
"""

import os
import duckdb
import polars as pl
from datetime import date, datetime

# ── Dependency imports from PBBLNFMT ─────────────────────────────────────────
# %INC PGM(PBBLNFMT) is present in the original SAS source.
# Only functions directly referenced in this program are imported.
# format_riskcd, format_collcd, format_statecd, format_mthpass, format_ndays,
#   format_busind are available in PBBLNFMT and used for FORMAT statements that
#   influence how CLASS variables are reported; they are applied via their
#   respective format_* functions on demand in each section below.
from PBBLNFMT import (
    format_apprlimt,    # FORMAT APPRLIM2 APPRLIMT.  (Sections 3, 5)
    format_loansize,    # FORMAT APPRLIM2 LOANSIZE.  (Section 5)
    format_riskcd,      # FORMAT RISKCD              (Sections 1–2)
    format_lnormt,      # FORMAT ORIGMT  LNORMT.     (Sections 11, 13)
    format_lnrmmt,      # FORMAT REMAINMT LNRMMT.    (Section 14)
    format_collcd,      # FORMAT COLLCD              (Section 4)
    format_statecd,     # FORMAT STATECD             (Section 8)
    format_busind,      # FORMAT BUSIND              (Section 7)
    format_mthpass,     # FORMAT MTHPASS             (Sections 1–2)
    format_ndays,       # FORMAT NDAYS               (Sections 1–2)
)

# ── Dependency: SECTMAP exposes a single entry-point function ─────────────────
# process_sectmap(df) is the equivalent of %INC PGM(SECTMAP) in SAS.
# Returns the combined ALM + ALMA dataset with SECTORCD normalised and
#   expanded through the full hierarchy rollup pipeline.
from SECTMAP import process_sectmap


# ── Path configuration ────────────────────────────────────────────────────────
BASE_DIR  = os.environ.get("BASE_DIR", "/data")
BNM_DIR   = os.path.join(BASE_DIR, "bnm")
BNM1_DIR  = os.path.join(BASE_DIR, "bnm1")
LOAN_DIR  = os.path.join(BASE_DIR, "loan")

REPTMON   = os.environ.get("REPTMON",  "")   # e.g. "202401"
NOWK      = os.environ.get("NOWK",     "")   # e.g. "01"
NOWK1     = os.environ.get("NOWK1",    "")   # e.g. "04"  (previous week)
RDATE_STR = os.environ.get("RDATE",    "")   # DDMMYYYY e.g. "31012024"
SDATE_STR = os.environ.get("SDATE",    "")   # DDMMYYYY e.g. "23012024"

# Input parquet paths
LOAN_PARQUET   = os.path.join(BNM_DIR,  f"LOAN{REPTMON}{NOWK}.parquet")
LOAN1_PARQUET  = os.path.join(BNM1_DIR, f"LOAN{REPTMON}{NOWK1}.parquet")
ULOAN_PARQUET  = os.path.join(BNM_DIR,  f"ULOAN{REPTMON}{NOWK}.parquet")
LNCOMM_PARQUET = os.path.join(LOAN_DIR, "LNCOMM.parquet")

# Output / working parquet path
LALM_PARQUET   = os.path.join(BNM_DIR,  f"LALM{REPTMON}{NOWK}.parquet")

os.makedirs(BNM_DIR,  exist_ok=True)
os.makedirs(BNM1_DIR, exist_ok=True)


# ── Date parsing helpers ──────────────────────────────────────────────────────
def parse_ddmmyyyy(s: str) -> date:
    """Parse DDMMYYYY string to date."""
    return datetime.strptime(s, "%d%m%Y").date()

RDATE = parse_ddmmyyyy(RDATE_STR) if RDATE_STR else date.today()
SDATE = parse_ddmmyyyy(SDATE_STR) if SDATE_STR else date.today()


# ── DuckDB connection (shared) ────────────────────────────────────────────────
con = duckdb.connect()


# ── Helper: append rows to LALM parquet ──────────────────────────────────────
def append_to_lalm(new_df: pl.DataFrame) -> None:
    """Append new_df (BNMCODE, AMTIND, AMOUNT) to LALM parquet."""
    cols = ["BNMCODE", "AMTIND", "AMOUNT"]
    if new_df.is_empty():
        return
    new_df = new_df.select(cols).cast({
        "BNMCODE": pl.Utf8,
        "AMTIND":  pl.Utf8,
        "AMOUNT":  pl.Float64,
    })
    if os.path.exists(LALM_PARQUET):
        existing = con.execute(
            f"SELECT BNMCODE, AMTIND, AMOUNT FROM read_parquet('{LALM_PARQUET}')"
        ).pl()
        combined = pl.concat([existing, new_df], how="diagonal")
    else:
        combined = new_df
    combined.write_parquet(LALM_PARQUET)


# ── Helper: apply SECTMAP pipeline to a DataFrame with SECTORCD column ────────
def apply_sectmap(df: pl.DataFrame) -> pl.DataFrame:
    """
    Equivalent of %INC PGM(SECTMAP) in the SAS source.
    Delegates to process_sectmap() from SECTMAP.py, which runs the full
    normalisation → expansion → rollup pipeline (inlining $NEWSECT./$VALIDSE.
    format logic) and returns the combined ALM + ALMA dataset with SECTORCD
    updated.  Intermediate helper columns added by SECTMAP are dropped.
    """
    df = process_sectmap(df)
    for col in ("SECTA", "SECVALID", "SECTCD"):
        if col in df.columns:
            df = df.drop(col)
    return df


# ── Helper: apply APPRLIMT format row-wise ────────────────────────────────────
def apply_apprlimt_fmt(df: pl.DataFrame, col: str = "APPRLIM2") -> pl.DataFrame:
    """Add ALMLIMT column = format_apprlimt(APPRLIM2)."""
    return df.with_columns(
        pl.col(col).map_elements(
            lambda v: format_apprlimt(float(v)) if v is not None else "",
            return_dtype=pl.Utf8,
        ).alias("ALMLIMT")
    )


def apply_loansize_fmt(df: pl.DataFrame, col: str = "APPRLIM2") -> pl.DataFrame:
    """Add LOANSIZE column = format_loansize(APPRLIM2)."""
    return df.with_columns(
        pl.col(col).map_elements(
            lambda v: format_loansize(float(v)) if v is not None else "",
            return_dtype=pl.Utf8,
        ).alias("LOANSIZE")
    )


# ── Step 0 : Build LOAN1 from BNM1.LOAN&REPTMON&NOWK1  ───────────────────────
# DATA LOAN1; SET BNM1.LOAN&REPTMON&NOWK1;
#   IF PRODUCT IN (124,145);
#   PRODCD='34120'; AMTIND='I';
def build_loan1() -> pl.DataFrame:
    df = con.execute(f"""
        SELECT * FROM read_parquet('{LOAN1_PARQUET}')
        WHERE PRODUCT IN (124, 145)
    """).pl()
    df = df.with_columns([
        pl.lit("34120").alias("PRODCD"),
        pl.lit("I").alias("AMTIND"),
    ])
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 : NON-PERFORMING LOANS (NPL)
# ═══════════════════════════════════════════════════════════════════════════════

def section_npl_simple():
    """
    PROC SUMMARY by AMTIND, RISKCD where PRODCD[:2]='34' and RISKCD NE ' '.
    BNMCODE = RISKCD || '00000000Y'
    """
    alq = con.execute(f"""
        SELECT AMTIND, RISKCD, SUM(BALANCE) AS AMOUNT
        FROM read_parquet('{LOAN_PARQUET}')
        WHERE LEFT(PRODCD, 2) = '34'
          AND RISKCD IS NOT NULL AND TRIM(RISKCD) <> ''
        GROUP BY AMTIND, RISKCD
    """).pl()

    rows = []
    for row in alq.iter_rows(named=True):
        riskcd = (row["RISKCD"] or "").strip()
        if riskcd:
            rows.append({
                "BNMCODE": f"{riskcd}00000000Y",
                "AMTIND":  row["AMTIND"],
                "AMOUNT":  row["AMOUNT"],
            })
    if rows:
        append_to_lalm(pl.DataFrame(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 : NPL – BY CUSTOMER AND SECTORIAL CODE
# ═══════════════════════════════════════════════════════════════════════════════

def _npl_sector_bnmcode(prefix9: str, sectorcd: str) -> str:
    """
    Build BNMCODE for _TYPE_=6 NPL-by-sector block.
    prefix9 is the first 9 characters (e.g. '349000000').
    Falls through to OTHERWISE → prefix9 + '9999Y'.
    """
    sc = sectorcd or ""
    p1 = sc[:1]; p2 = sc[:2]; p3 = sc[:3]

    if sc in ("0410","0420","0430","9999"):
        return f"{prefix9}{sc}Y"
    if p2 == "01":   return f"{prefix9}0100Y"
    if p2 == "02":   return f"{prefix9}0200Y"
    if p3 == "031":  return f"{prefix9}0310Y"
    if p3 == "032":  return f"{prefix9}0320Y"
    if p1 == "1":    return f"{prefix9}1000Y"
    if p1 == "2":    return f"{prefix9}2000Y"
    if p1 == "3":    return f"{prefix9}3000Y"
    if p1 == "4":    return f"{prefix9}4000Y"
    if p1 == "5":    return f"{prefix9}{sc}Y"
    if p2 == "61":   return f"{prefix9}6100Y"
    if p2 == "62":   return f"{prefix9}6200Y"
    if p2 == "63":   return f"{prefix9}6300Y"
    if p1 == "7":    return f"{prefix9}7000Y"
    if p2 == "81":   return f"{prefix9}8100Y"
    if p2 == "82":   return f"{prefix9}8200Y"
    if p3 == "831":  return f"{prefix9}8310Y"
    if p3 == "832":  return f"{prefix9}8320Y"
    if p3 == "833":  return f"{prefix9}8330Y"
    if p2 in ("90","91","92","93","94","95","96","97","98"):
        return f"{prefix9}9000Y"
    return f"{prefix9}9999Y"   # OTHERWISE


def section_npl_by_cust_sector():
    """
    PROC SUMMARY by AMTIND, SECTORCD, CUSTCD where PRODCD[:2]='34', RISKCD NE ' '.
    _TYPE_=6 (AMTIND+SECTORCD) and _TYPE_=7 (AMTIND+SECTORCD+CUSTCD).
    """
    alq = con.execute(f"""
        SELECT AMTIND, SECTORCD, CUSTCD, SUM(BALANCE) AS AMOUNT
        FROM read_parquet('{LOAN_PARQUET}')
        WHERE LEFT(PRODCD, 2) = '34'
          AND RISKCD IS NOT NULL AND TRIM(RISKCD) <> ''
        GROUP BY GROUPING SETS (
            (AMTIND, SECTORCD),
            (AMTIND, SECTORCD, CUSTCD)
        )
    """).pl()

    rows = []
    for row in alq.iter_rows(named=True):
        amtind   = row["AMTIND"]   or ""
        sectorcd = row["SECTORCD"] or ""
        custcd   = row["CUSTCD"]
        amount   = row["AMOUNT"]

        # _TYPE_=6 : CUSTCD is NULL (AMTIND+SECTORCD grouping)
        if custcd is None:
            bc = _npl_sector_bnmcode("349000000", sectorcd)
            rows.append({"BNMCODE": bc, "AMTIND": amtind, "AMOUNT": amount})

        # _TYPE_=7 : all three present
        else:
            custcd = str(custcd).strip()
            if custcd in ("61", "66"):
                bc = _npl_sector_bnmcode("349006100", sectorcd)
                rows.append({"BNMCODE": bc, "AMTIND": amtind, "AMOUNT": amount})

            if custcd == "77":
                sc = sectorcd or ""
                p2 = sc[:2]; p3 = sc[:3]
                if sc in ("0410","0420","0430"):
                    rows.append({"BNMCODE": f"34900{custcd}00{sc}Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                elif p2 == "01":
                    rows.append({"BNMCODE": f"34900{custcd}000100Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                elif p2 == "02":
                    rows.append({"BNMCODE": f"34900{custcd}000200Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                elif p3 == "031":
                    rows.append({"BNMCODE": f"34900{custcd}000310Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                elif p3 == "032":
                    rows.append({"BNMCODE": f"34900{custcd}000320Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                else:
                    rows.append({"BNMCODE": f"34900{custcd}009999Y",
                                 "AMTIND": amtind, "AMOUNT": amount})

    if rows:
        append_to_lalm(pl.DataFrame(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 : GROSS LOAN – BY APPROVED LIMIT
# ═══════════════════════════════════════════════════════════════════════════════

def section_gross_loan_by_apprlim():
    """
    PROC SUMMARY by AMTIND, APPRLIM2, CUSTCD (all loans).
    FORMAT APPRLIM2 APPRLIMT.
    _TYPE_=6 → ALMLIMT||'00000000Y'
    _TYPE_=7 → various CUSTCD-based BNMCODE patterns.
    """
    alm = con.execute(f"""
        SELECT AMTIND, APPRLIM2, CUSTCD, SUM(BALANCE) AS AMOUNT
        FROM read_parquet('{LOAN_PARQUET}')
        GROUP BY GROUPING SETS (
            (AMTIND, APPRLIM2),
            (AMTIND, APPRLIM2, CUSTCD)
        )
    """).pl()

    alm = apply_apprlimt_fmt(alm)

    rows = []
    for row in alm.iter_rows(named=True):
        amtind  = row["AMTIND"]  or ""
        custcd  = row["CUSTCD"]
        amount  = row["AMOUNT"]
        almlimt = row["ALMLIMT"] or ""

        # _TYPE_=6
        if custcd is None:
            rows.append({"BNMCODE": f"{almlimt}00000000Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        # _TYPE_=7
        else:
            custcd  = str(custcd).strip()
            sme_grp = ("41","42","43","44","46","47","48","49","51","52","53","54")
            if custcd in sme_grp:
                rows.append({"BNMCODE": f"{almlimt}{custcd}000000Y",
                             "AMTIND": amtind, "AMOUNT": amount})
                if custcd in ("41","42","43"):
                    rows.append({"BNMCODE": f"{almlimt}66000000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                if custcd in ("44","46","47"):
                    rows.append({"BNMCODE": f"{almlimt}67000000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                if custcd in ("48","49","51"):
                    rows.append({"BNMCODE": f"{almlimt}68000000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                if custcd in ("52","53","54"):
                    rows.append({"BNMCODE": f"{almlimt}69000000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})

            if custcd in ("61","41","42","43"):
                rows.append({"BNMCODE": f"{almlimt}61000000Y",
                             "AMTIND": amtind, "AMOUNT": amount})
            elif custcd == "77":
                rows.append({"BNMCODE": f"{almlimt}77000000Y",
                             "AMTIND": amtind, "AMOUNT": amount})

    if rows:
        append_to_lalm(pl.DataFrame(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 : GROSS LOAN – BY COLLATERAL TYPE
# ═══════════════════════════════════════════════════════════════════════════════

def section_gross_loan_by_collateral():
    """
    PROC SUMMARY by AMTIND, COLLCD, CUSTCD.
    _TYPE_=6  AND COLLCD IN ('30560','30570','30580') → COLLCD||'00000000Y'
    _TYPE_=7  AND COLLCD NOT IN those → CUSTCD-based routing.
    format_collcd (PBBLNFMT) applied via FORMAT COLLCD in original SAS.
    """
    alm = con.execute(f"""
        SELECT AMTIND, COLLCD, CUSTCD, SUM(BALANCE) AS AMOUNT
        FROM read_parquet('{LOAN_PARQUET}')
        GROUP BY GROUPING SETS (
            (AMTIND, COLLCD),
            (AMTIND, COLLCD, CUSTCD)
        )
    """).pl()

    simple_colls = {"30560", "30570", "30580"}
    rows = []
    for row in alm.iter_rows(named=True):
        amtind = row["AMTIND"] or ""
        collcd = (row["COLLCD"] or "").strip()
        custcd = row["CUSTCD"]
        amount = row["AMOUNT"]

        # _TYPE_=6
        if custcd is None:
            if collcd in simple_colls:
                rows.append({"BNMCODE": f"{collcd}00000000Y",
                             "AMTIND": amtind, "AMOUNT": amount})
        # _TYPE_=7
        else:
            custcd = str(custcd).strip()
            if collcd not in simple_colls:
                if custcd in ("10","02","03","11","12"):
                    rows.append({"BNMCODE": f"{collcd}10000000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                elif custcd in ("20","13","17","30","32","33","34","35",
                                "36","37","38","39","40"):
                    rows.append({"BNMCODE": f"{collcd}20000000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                elif custcd in ("04","05","06"):
                    rows.append({"BNMCODE": f"{collcd}{custcd}000000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                    rows.append({"BNMCODE": f"{collcd}20000000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                elif custcd in ("60","62","63","44","46","47","48","49","51"):
                    rows.append({"BNMCODE": f"{collcd}60000000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                elif custcd in ("61","41","42","43"):
                    rows.append({"BNMCODE": f"{collcd}61000000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                    rows.append({"BNMCODE": f"{collcd}60000000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                elif custcd in ("64","52","53","54","75","57","59"):
                    rows.append({"BNMCODE": f"{collcd}64000000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                    rows.append({"BNMCODE": f"{collcd}60000000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                elif custcd in ("70","71","72","73","74"):
                    rows.append({"BNMCODE": f"{collcd}70000000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                elif custcd in ("76","78"):
                    rows.append({"BNMCODE": f"{collcd}76000000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                elif custcd == "77":
                    rows.append({"BNMCODE": f"{collcd}77000000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                    rows.append({"BNMCODE": f"{collcd}76000000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                elif custcd == "79":
                    rows.append({"BNMCODE": f"{collcd}79000000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                elif custcd in ("80","81","82","83","84","85","86","90","91",
                                "92","95","96","98","99"):
                    rows.append({"BNMCODE": f"{collcd}80000000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                # OTHERWISE – no output

    if rows:
        append_to_lalm(pl.DataFrame(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 : GROSS LOAN – NO OF UTILISED LOAN ACCOUNTS
# ═══════════════════════════════════════════════════════════════════════════════

def section_utilised_loan_count():
    """
    DATA B80510: filter LOAN where PRODCD[:2]='34', exclude zero/near-zero BALANCE.
    PROC SUMMARY by AMTIND, APPRLIM2, CUSTCD; FORMAT APPRLIM2 LOANSIZE.
    _FREQ_ * 1000 = AMOUNT.
    _TYPE_=5 (AMTIND+CUSTCD), _TYPE_=6 (AMTIND+APPRLIM2), _TYPE_=7 (all three).
    """
    b80510 = con.execute(f"""
        SELECT AMTIND, APPRLIM2, CUSTCD, BALANCE,
               ROUND(BALANCE, 2) AS BALX
        FROM read_parquet('{LOAN_PARQUET}')
        WHERE LEFT(PRODCD, 2) = '34'
    """).pl()

    # Exclude zero-balance rows
    b80510 = b80510.filter(
        (pl.col("BALX").abs() > 0.001) & (pl.col("BALANCE").abs() > 0.001)
    )

    alm = con.execute("""
        SELECT AMTIND, APPRLIM2, CUSTCD, COUNT(*) AS FREQ
        FROM b80510
        GROUP BY GROUPING SETS (
            (AMTIND, CUSTCD),
            (AMTIND, APPRLIM2),
            (AMTIND, APPRLIM2, CUSTCD)
        )
    """).pl()

    alm = apply_loansize_fmt(alm)

    rows = []
    for row in alm.iter_rows(named=True):
        amtind   = row["AMTIND"]   or ""
        apprlim2 = row["APPRLIM2"]
        custcd   = row["CUSTCD"]
        freq     = row["FREQ"] or 0
        amount   = freq * 1000
        loansize = row["LOANSIZE"] or ""

        # _TYPE_=5: AMTIND+CUSTCD (APPRLIM2 is NULL)
        if apprlim2 is None and custcd is not None:
            custcd = str(custcd).strip()
            if custcd in ("10","02","03","11","12"):
                rows.append({"BNMCODE": "8051010000000Y", "AMTIND": amtind, "AMOUNT": amount})
            if custcd in ("20","13","17","04","05","06","30","32","33",
                          "34","35","36","37","38","39","40"):
                rows.append({"BNMCODE": "8051020000000Y", "AMTIND": amtind, "AMOUNT": amount})
            if custcd in ("61","41","42","43"):
                rows.append({"BNMCODE": "8051061000000Y", "AMTIND": amtind, "AMOUNT": amount})
            if custcd in ("62","44","46","47"):
                rows.append({"BNMCODE": "8051062000000Y", "AMTIND": amtind, "AMOUNT": amount})
            if custcd in ("63","48","49","51"):
                rows.append({"BNMCODE": "8051063000000Y", "AMTIND": amtind, "AMOUNT": amount})
            if custcd in ("64","52","53","54","57","75","59"):
                rows.append({"BNMCODE": "8051064000000Y", "AMTIND": amtind, "AMOUNT": amount})
            if custcd in ("41","42","43","44","46","47","48","49","51","52","53","54"):
                rows.append({"BNMCODE": "8051065000000Y", "AMTIND": amtind, "AMOUNT": amount})
                rows.append({"BNMCODE": f"80510{custcd}000000Y", "AMTIND": amtind, "AMOUNT": amount})
            if custcd in ("71","72","73","74"):
                rows.append({"BNMCODE": "8051070000000Y", "AMTIND": amtind, "AMOUNT": amount})
            if custcd in ("77","78"):
                rows.append({"BNMCODE": "8051076000000Y", "AMTIND": amtind, "AMOUNT": amount})
                rows.append({"BNMCODE": f"80510{custcd}000000Y", "AMTIND": amtind, "AMOUNT": amount})
            if custcd == "79":
                rows.append({"BNMCODE": "8051079000000Y", "AMTIND": amtind, "AMOUNT": amount})
            if "81" <= custcd <= "99":
                rows.append({"BNMCODE": "8051080000000Y", "AMTIND": amtind, "AMOUNT": amount})

        # _TYPE_=6: AMTIND+APPRLIM2 (CUSTCD is NULL)
        elif apprlim2 is not None and custcd is None:
            rows.append({"BNMCODE": f"{loansize}00000000Y", "AMTIND": amtind, "AMOUNT": amount})

        # _TYPE_=7: all three present
        elif apprlim2 is not None and custcd is not None:
            custcd = str(custcd).strip()
            if custcd == "77":
                rows.append({"BNMCODE": f"{loansize}77000000Y", "AMTIND": amtind, "AMOUNT": amount})
            elif custcd in ("61","41","42","43"):
                rows.append({"BNMCODE": f"{loansize}61000000Y", "AMTIND": amtind, "AMOUNT": amount})
            # OTHERWISE – no output for _TYPE_=7 base
            if custcd in ("41","42","43","44","46","47","48","49","51","52","53","54"):
                rows.append({"BNMCODE": f"{loansize}{custcd}000000Y", "AMTIND": amtind, "AMOUNT": amount})

    if rows:
        append_to_lalm(pl.DataFrame(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6 : LOAN – BY CUSTOMER CODE AND BY PURPOSE CODE
# ═══════════════════════════════════════════════════════════════════════════════

def section_loan_by_cust_purpose():
    """
    PROC SUMMARY by AMTIND, CUSTCD, FISSPURP where PRODCD[:2]='34'.
    Also builds ALM1 for FISSPURP mapping 022x/023x → '0200'.
    _TYPE_=7 only (all three classes).
    """
    alm = con.execute(f"""
        SELECT AMTIND, CUSTCD, FISSPURP, SUM(BALANCE) AS AMOUNT
        FROM read_parquet('{LOAN_PARQUET}')
        WHERE LEFT(PRODCD, 2) = '34'
        GROUP BY AMTIND, CUSTCD, FISSPURP
    """).pl()

    # Build ALM1: remap FISSPURP and append
    alm1_rows = []
    for row in alm.iter_rows(named=True):
        if row["FISSPURP"] in ("0220","0230","0210","0211","0212"):
            alm1_rows.append({**row, "FISSPURP": "0200"})
    alm1 = pl.DataFrame(alm1_rows) if alm1_rows else pl.DataFrame(schema=alm.schema)
    alm_full = pl.concat([alm, alm1], how="diagonal")

    rows = []
    for row in alm_full.iter_rows(named=True):
        amtind   = row["AMTIND"]   or ""
        custcd   = (row["CUSTCD"]   or "").strip()
        fisspurp = (row["FISSPURP"] or "").strip()
        amount   = row["AMOUNT"]

        # _TYPE_=7 only
        if not custcd or not fisspurp:
            continue

        if custcd == "79":
            rows.append({"BNMCODE": f"34000{custcd}00{fisspurp}Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        elif custcd in ("10","02","03","11","12"):
            rows.append({"BNMCODE": f"340001000{fisspurp}Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        elif custcd in ("20","13","17","30","31","32","33","34","35",
                        "37","38","39","40","04","05","45","06"):
            if custcd in ("04","05","06"):
                rows.append({"BNMCODE": f"34000{custcd}00{fisspurp}Y",
                             "AMTIND": amtind, "AMOUNT": amount})
            rows.append({"BNMCODE": f"340002000{fisspurp}Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        elif custcd in ("61","41","42","43"):
            rows.append({"BNMCODE": f"340006100{fisspurp}Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        elif custcd in ("62","44","46","47"):
            rows.append({"BNMCODE": f"340006200{fisspurp}Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        elif custcd in ("63","48","49","51"):
            rows.append({"BNMCODE": f"340006300{fisspurp}Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        elif custcd in ("64","57","75","59","52","53","54"):
            rows.append({"BNMCODE": f"340006400{fisspurp}Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        elif custcd in ("71","72","73","74"):
            rows.append({"BNMCODE": f"340007000{fisspurp}Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        # OTHERWISE – no output

        # Additional ranges (outside SELECT block)
        if "81" <= custcd <= "99":
            rows.append({"BNMCODE": "3400080" + "00" + fisspurp + "Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        if custcd in ("77","78"):
            rows.append({"BNMCODE": f"34000{custcd}00{fisspurp}Y",
                         "AMTIND": amtind, "AMOUNT": amount})
            rows.append({"BNMCODE": "3400076" + "00" + fisspurp + "Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        elif custcd in ("41","42","43","44","46","47","48","49","51","52","53","54"):
            rows.append({"BNMCODE": f"34000{custcd}00{fisspurp}Y",
                         "AMTIND": amtind, "AMOUNT": amount})

    if rows:
        append_to_lalm(pl.DataFrame(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7 : LOAN – BY CUSTOMER CODE AND BY SECTOR CODE
# ═══════════════════════════════════════════════════════════════════════════════

def section_loan_by_cust_sector():
    """
    PROC SUMMARY NWAY by AMTIND, CUSTCD, SECTORCD where PRODCD[:2]='34'
    and SECTORCD NE '0410'. Then %INC SECTMAP. Then BNMCODE routing.
    """
    alm = con.execute(f"""
        SELECT AMTIND, CUSTCD, SECTORCD, SUM(BALANCE) AS AMOUNT
        FROM read_parquet('{LOAN_PARQUET}')
        WHERE LEFT(PRODCD, 2) = '34'
          AND SECTORCD <> '0410'
        GROUP BY AMTIND, CUSTCD, SECTORCD
    """).pl()

    # %INC PGM(SECTMAP)
    alm = apply_sectmap(alm)

    rows = []
    for row in alm.iter_rows(named=True):
        amtind   = row["AMTIND"]   or ""
        custcd   = (row["CUSTCD"]   or "").strip()
        sectorcd = (row["SECTORCD"] or "").strip()
        amount   = row["AMOUNT"]

        if not custcd or not sectorcd:
            continue

        if custcd in ("41","42","43","44","46","47","48","49","51","52","53","54"):
            rows.append({"BNMCODE": f"34000{custcd}00{sectorcd}Y",
                         "AMTIND": amtind, "AMOUNT": amount})

        # SELECT(CUSTCD) block
        if custcd in ("61","41","42","43"):
            rows.append({"BNMCODE": f"340006100{sectorcd}Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        elif custcd in ("62","44","46","47"):
            rows.append({"BNMCODE": f"340006200{sectorcd}Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        elif custcd in ("63","48","49","51"):
            rows.append({"BNMCODE": f"340006300{sectorcd}Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        elif custcd in ("64","57","75","59","52","53","54"):
            rows.append({"BNMCODE": f"340006400{sectorcd}Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        elif custcd in ("71","72","73","74"):
            rows.append({"BNMCODE": f"340007000{sectorcd}Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        elif custcd == "79":
            rows.append({"BNMCODE": f"340007900{sectorcd}Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        elif custcd in ("20","13","17","30","31","32","33","34","35",
                        "37","38","39","40","04","05","45","06"):
            if custcd in ("04","05","06"):
                rows.append({"BNMCODE": f"34000{custcd}00{sectorcd}Y",
                             "AMTIND": amtind, "AMOUNT": amount})
            rows.append({"BNMCODE": f"340002000{sectorcd}Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        elif custcd in ("10","02","03","11","12"):
            rows.append({"BNMCODE": f"340001000{sectorcd}Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        # OTHERWISE – no output

        # Additional range-based rules (outside SELECT)
        if "81" <= custcd <= "99":
            rows.append({"BNMCODE": "3400080" + "00" + sectorcd + "Y",
                         "AMTIND": amtind, "AMOUNT": amount})
            if sectorcd == "9700":
                rows.append({"BNMCODE": "3400085009700Y",
                             "AMTIND": amtind, "AMOUNT": amount})
            elif sectorcd in ("1000","2000","3000","4000","5000",
                              "6000","7000","8000","9000","9999"):
                rows.append({"BNMCODE": "3400085009999Y",
                             "AMTIND": amtind, "AMOUNT": amount})

        if custcd in ("95","96"):
            if sectorcd in ("1000","2000","3000","4000","5000",
                            "6000","7000","8000","9000","9999","9700"):
                rows.append({"BNMCODE": "3400095000000Y",
                             "AMTIND": amtind, "AMOUNT": amount})
            rows.append({"BNMCODE": "3400095" + "00" + sectorcd + "Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        elif custcd in ("77","78"):
            rows.append({"BNMCODE": "3400076" + "00" + sectorcd + "Y",
                         "AMTIND": amtind, "AMOUNT": amount})
            rows.append({"BNMCODE": f"34000{custcd}00{sectorcd}Y",
                         "AMTIND": amtind, "AMOUNT": amount})

    if rows:
        append_to_lalm(pl.DataFrame(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8 : LOAN – SME BY CUSTOMER CODE AND BY STATE CODE
# ═══════════════════════════════════════════════════════════════════════════════

def section_sme_by_state():
    """
    PROC SUMMARY by CUSTCD, STATECD, AMTIND where PRODCD[:2]='34'
    and CUSTCD in SME set.
    _TYPE_=7 → BNMCODE = '34000'||CUSTCD||'000000'||STATECD.
    format_statecd (PBBLNFMT) applied via FORMAT STATECD in original SAS.
    """
    sme_set = ("41","42","43","44","46","47","48","49","51","52","53","54")
    alm = con.execute(f"""
        SELECT CUSTCD, STATECD, AMTIND, SUM(BALANCE) AS AMOUNT
        FROM read_parquet('{LOAN_PARQUET}')
        WHERE LEFT(PRODCD, 2) = '34'
          AND CUSTCD IN {sme_set}
        GROUP BY CUSTCD, STATECD, AMTIND
    """).pl()

    rows = []
    for row in alm.iter_rows(named=True):
        custcd  = (row["CUSTCD"]  or "").strip()
        statecd = (row["STATECD"] or "").strip()
        amtind  = row["AMTIND"]   or ""
        amount  = row["AMOUNT"]
        if custcd and statecd:
            rows.append({"BNMCODE": f"34000{custcd}000000{statecd}",
                         "AMTIND": amtind, "AMOUNT": amount})
    if rows:
        append_to_lalm(pl.DataFrame(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 9 : RM LOANS – TOTAL
# ═══════════════════════════════════════════════════════════════════════════════

def section_rm_loans():
    """
    PROC SUMMARY NWAY by AMTIND, PRODCD where PRODCD[:3] IN ('341','342','343','344').
    WHERE PRODCD IN ('34190','34230','34299') → BNMCODE = PRODCD||'00000000Y'.
    """
    alm = con.execute(f"""
        SELECT AMTIND, PRODCD, SUM(BALANCE) AS AMOUNT
        FROM read_parquet('{LOAN_PARQUET}')
        WHERE LEFT(PRODCD, 3) IN ('341','342','343','344')
        GROUP BY AMTIND, PRODCD
    """).pl()

    rows = []
    for row in alm.iter_rows(named=True):
        prodcd = (row["PRODCD"] or "").strip()
        amtind = row["AMTIND"]  or ""
        amount = row["AMOUNT"]
        if prodcd in ("34190","34230","34299"):
            rows.append({"BNMCODE": f"{prodcd}00000000Y",
                         "AMTIND": amtind, "AMOUNT": amount})
    if rows:
        append_to_lalm(pl.DataFrame(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 10 : RM LOANS – OVERDRAFT BY PRODUCT CODE
# ═══════════════════════════════════════════════════════════════════════════════

def section_rm_overdraft():
    """
    PROC SUMMARY NWAY by AMTIND where PRODCD='34180'.
    BNMCODE = '3418000000000Y'.
    """
    alm = con.execute(f"""
        SELECT AMTIND, SUM(BALANCE) AS AMOUNT
        FROM read_parquet('{LOAN_PARQUET}')
        WHERE PRODCD = '34180'
        GROUP BY AMTIND
    """).pl()

    rows = [{"BNMCODE": "3418000000000Y", "AMTIND": row["AMTIND"] or "",
             "AMOUNT": row["AMOUNT"]}
            for row in alm.iter_rows(named=True)]
    if rows:
        append_to_lalm(pl.DataFrame(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 11 : RM LOANS – BY CUSTOMER AND MATURITY CODE
# ═══════════════════════════════════════════════════════════════════════════════

def section_rm_by_cust_maturity():
    """
    PROC SUMMARY NWAY by AMTIND, ORIGMT, CUSTCD where PRODCD[:3] IN ('341'…).
    _TYPE_=7: short-term (ORIGMT '10'-'17') and long-term ('20'-'33') for
    foreign CUSTCD groups → specific BNMCODE.
    format_lnormt (PBBLNFMT) applied via FORMAT ORIGMT LNORMT. in original SAS.
    """
    alm = con.execute(f"""
        SELECT AMTIND, ORIGMT, CUSTCD, SUM(BALANCE) AS AMOUNT
        FROM read_parquet('{LOAN_PARQUET}')
        WHERE LEFT(PRODCD, 3) IN ('341','342','343','344')
        GROUP BY AMTIND, ORIGMT, CUSTCD
    """).pl()

    rows = []
    for row in alm.iter_rows(named=True):
        amtind = row["AMTIND"] or ""
        origmt = (row["ORIGMT"] or "").strip()
        custcd = (row["CUSTCD"] or "").strip()
        amount = row["AMOUNT"]

        if origmt in ("10","12","13","14","15","16","17"):
            if custcd in ("81","82","83","84"):
                rows.append({"BNMCODE": "3410081100000Y",
                             "AMTIND": amtind, "AMOUNT": amount})
            elif custcd in ("85","86","90","91","92","95","96","98","99"):
                rows.append({"BNMCODE": "3410085100000Y",
                             "AMTIND": amtind, "AMOUNT": amount})

        if origmt in ("20","21","22","23","24","25","26","30","31","32","33"):
            if custcd == "81":
                rows.append({"BNMCODE": "3410081200000Y",
                             "AMTIND": amtind, "AMOUNT": amount})
            elif custcd in ("85","86","90","91","92","95","96","98","99"):
                rows.append({"BNMCODE": "3410085200000Y",
                             "AMTIND": amtind, "AMOUNT": amount})

    if rows:
        append_to_lalm(pl.DataFrame(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 12 : RM TERM LOAN – BY CUSTOMER AND PURPOSE CODE
# ═══════════════════════════════════════════════════════════════════════════════

def section_term_loan_by_prodcd_purpose():
    """
    PROC SUMMARY by AMTIND, PRODCD, FISSPURP where specific PRODCDs.
    _TYPE_=6 → PRODCD||'00000000Y'
    _TYPE_=7 and PRODCD='34111' → specific FISSPURP-based BNMCODE.
    """
    target_prods = ("34111","34112","34113","34114","34115","34116","34117","34120","34149")
    alm = con.execute(f"""
        SELECT AMTIND, PRODCD, FISSPURP, SUM(BALANCE) AS AMOUNT
        FROM read_parquet('{LOAN_PARQUET}')
        WHERE PRODCD IN {target_prods}
        GROUP BY GROUPING SETS (
            (AMTIND, PRODCD),
            (AMTIND, PRODCD, FISSPURP)
        )
    """).pl()

    rows = []
    for row in alm.iter_rows(named=True):
        amtind   = row["AMTIND"]   or ""
        prodcd   = (row["PRODCD"]   or "").strip()
        fisspurp = row["FISSPURP"]
        amount   = row["AMOUNT"]

        if fisspurp is None:
            # _TYPE_=6
            rows.append({"BNMCODE": f"{prodcd}00000000Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        else:
            fisspurp = fisspurp.strip()
            # _TYPE_=7
            if prodcd == "34111":
                if fisspurp[:3] == "043":
                    rows.append({"BNMCODE": "3411100000430Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                if fisspurp[:3] == "021":
                    rows.append({"BNMCODE": "3411100000210Y",
                                 "AMTIND": amtind, "AMOUNT": amount})

    if rows:
        append_to_lalm(pl.DataFrame(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 13 : RM TERM LOAN – BY ORIGINAL MATURITY
# ═══════════════════════════════════════════════════════════════════════════════

def section_term_loan_by_origmt():
    """
    PROC SUMMARY NWAY by AMTIND, ORIGMT where specific PRODCDs.
    SELECT(ORIGMT): short → '3411000100000Y'; others → '3411000'||ORIGMT||'0000Y'.
    format_lnormt (PBBLNFMT) applied via FORMAT ORIGMT LNORMT. in original SAS.
    """
    target_prods = ("34111","34112","34113","34114","34115","34116","34117","34120","34149")
    alm = con.execute(f"""
        SELECT AMTIND, ORIGMT, SUM(BALANCE) AS AMOUNT
        FROM read_parquet('{LOAN_PARQUET}')
        WHERE PRODCD IN {target_prods}
        GROUP BY AMTIND, ORIGMT
    """).pl()

    rows = []
    for row in alm.iter_rows(named=True):
        amtind = row["AMTIND"] or ""
        origmt = (row["ORIGMT"] or "").strip()
        amount = row["AMOUNT"]

        if origmt in ("10","12","13","14","15","16","17"):
            rows.append({"BNMCODE": "3411000100000Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        elif origmt in ("21","22","23","24","25","26","31","32","33"):
            rows.append({"BNMCODE": f"3411000{origmt}0000Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        # OTHERWISE – no output

    if rows:
        append_to_lalm(pl.DataFrame(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 14 : RM TERM LOAN – BY REMAINING MATURITY
# ═══════════════════════════════════════════════════════════════════════════════

def section_term_loan_by_remainmt():
    """
    PROC SUMMARY NWAY by AMTIND, REMAINMT where specific PRODCDs.
    SELECT(REMAINMT): short → '3411000500000Y'; others → '3411000'||REMAINMT||'0000Y'.
    format_lnrmmt (PBBLNFMT) applied via FORMAT REMAINMT LNRMMT. in original SAS.
    """
    target_prods = ("34111","34112","34113","34114","34115","34116","34117","34120","34149")
    alm = con.execute(f"""
        SELECT AMTIND, REMAINMT, SUM(BALANCE) AS AMOUNT
        FROM read_parquet('{LOAN_PARQUET}')
        WHERE PRODCD IN {target_prods}
        GROUP BY AMTIND, REMAINMT
    """).pl()

    rows = []
    for row in alm.iter_rows(named=True):
        amtind   = row["AMTIND"]  or ""
        remainmt = (row["REMAINMT"] or "").strip()
        amount   = row["AMOUNT"]

        if remainmt in ("51","52","53","54","55","56","57"):
            rows.append({"BNMCODE": "3411000500000Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        elif remainmt in ("61","62","63","64","71","72","73"):
            rows.append({"BNMCODE": f"3411000{remainmt}0000Y",
                         "AMTIND": amtind, "AMOUNT": amount})
        # OTHERWISE – no output

    if rows:
        append_to_lalm(pl.DataFrame(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 15 : GROSS LOAN – TOTAL APPROVED LIMIT
# ═══════════════════════════════════════════════════════════════════════════════

def section_total_approved_limit():
    """
    PROC SUMMARY NWAY by AMTIND; VAR APPRLIM2 from LOAN + APPRLIMT from ULOAN.
    BNMCODE = '8150000000000Y'.
    """
    alm = con.execute(f"""
        SELECT AMTIND, SUM(APPRLIM2) AS AMOUNT
        FROM read_parquet('{LOAN_PARQUET}')
        GROUP BY AMTIND
    """).pl()

    ualm = con.execute(f"""
        SELECT AMTIND, SUM(APPRLIMT) AS AMOUNT
        FROM read_parquet('{ULOAN_PARQUET}')
        GROUP BY AMTIND
    """).pl()

    combined = pl.concat([alm, ualm], how="diagonal")
    rows = [{"BNMCODE": "8150000000000Y", "AMTIND": row["AMTIND"] or "",
             "AMOUNT": row["AMOUNT"]}
            for row in combined.iter_rows(named=True)]
    if rows:
        append_to_lalm(pl.DataFrame(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 16 : GROSS LOAN – NO OF UNUTILISED LOAN ACCOUNTS
# ═══════════════════════════════════════════════════════════════════════════════

def section_unutilised_loan_count():
    """
    PROC SUMMARY NWAY by AMTIND, CUSTCD where RLEASAMT=0 from LOAN and ULOAN.
    _FREQ_ * 1000 = AMOUNT. Various BNMCODE patterns.
    """
    alm = con.execute(f"""
        SELECT AMTIND, CUSTCD, COUNT(*) AS FREQ
        FROM read_parquet('{LOAN_PARQUET}')
        WHERE RLEASAMT = 0
        GROUP BY AMTIND, CUSTCD
    """).pl()

    ualm = con.execute(f"""
        SELECT AMTIND, CUSTCD, COUNT(*) AS FREQ
        FROM read_parquet('{ULOAN_PARQUET}')
        WHERE RLEASAMT = 0
        GROUP BY AMTIND, CUSTCD
    """).pl()

    combined = pl.concat([alm, ualm], how="diagonal")

    rows = []
    for row in combined.iter_rows(named=True):
        amtind = row["AMTIND"] or ""
        custcd = (row["CUSTCD"] or "").strip()
        amount = (row["FREQ"] or 0) * 1000

        rows.append({"BNMCODE": "8020000000000Y", "AMTIND": amtind, "AMOUNT": amount})

        if custcd in ("61","41","42","43"):
            rows.append({"BNMCODE": "8020061000000Y", "AMTIND": amtind, "AMOUNT": amount})
        if custcd in ("41","42","43","44","46","47","48","49","51","52","53","54"):
            rows.append({"BNMCODE": "8020065000000Y", "AMTIND": amtind, "AMOUNT": amount})
            rows.append({"BNMCODE": f"80500{custcd}000000Y", "AMTIND": amtind, "AMOUNT": amount})
        if custcd in ("70","71","72","73","74"):
            rows.append({"BNMCODE": "8050070000000Y", "AMTIND": amtind, "AMOUNT": amount})
        if custcd in ("77","78","79"):
            rows.append({"BNMCODE": f"80500{custcd}000000Y", "AMTIND": amtind, "AMOUNT": amount})
        if "81" <= custcd <= "99":
            rows.append({"BNMCODE": "8050080000000Y", "AMTIND": amtind, "AMOUNT": amount})

    if rows:
        append_to_lalm(pl.DataFrame(rows))

    # Post-processing: remap BNMCODE values and re-summarise
    _remap_unutilised_codes()


def _remap_unutilised_codes():
    """
    DATA ALM; SET BNM.LALM...; remap specific BNMCODE values.
    Then PROC SUMMARY for the two remapped codes and append.
    """
    if not os.path.exists(LALM_PARQUET):
        return
    alm = con.execute(f"""
        SELECT BNMCODE, AMTIND, AMOUNT
        FROM read_parquet('{LALM_PARQUET}')
    """).pl()

    remap = {
        "8020065000000Y": "8050065000000Y",
        "8020061000000Y": "8050061000000Y",
        "8051065000000Y": "8050065000000Y",
        "8051061000000Y": "8050061000000Y",
    }
    alm = alm.with_columns(
        pl.col("BNMCODE").replace(remap).alias("BNMCODE")
    )
    alm.write_parquet(LALM_PARQUET)

    # PROC SUMMARY NWAY where BNMCODE IN ('8050065000000Y','8050061000000Y')
    sub = alm.filter(
        pl.col("BNMCODE").is_in(["8050065000000Y","8050061000000Y"])
    ).group_by(["BNMCODE","AMTIND"]).agg(pl.col("AMOUNT").sum())

    append_to_lalm(sub)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 17 : GROSS LOAN – UNDRAWN PORTION BY ORIGINAL MATURITY
# ═══════════════════════════════════════════════════════════════════════════════

def _build_appr_dataset(loan_df: pl.DataFrame) -> pl.DataFrame:
    """
    Manipulation for RC: deduplication of revolving credit (PRODCD='34190')
    accounts using LNCOMM merge.
    """
    lncomm_df = con.execute(f"""
        SELECT * FROM read_parquet('{LNCOMM_PARQUET}')
    """).pl() if os.path.exists(LNCOMM_PARQUET) else pl.DataFrame()

    loan_sorted = loan_df.sort(["ACCTNO","NOTENO"])

    # Split on COMMNO
    almcom   = loan_sorted.filter(pl.col("COMMNO") > 0).sort(["ACCTNO","COMMNO"])
    almnocom = loan_sorted.filter(pl.col("COMMNO") <= 0).sort(["ACCTNO","COMMNO"])

    # Merge ALMCOM with LNCOMM
    if not lncomm_df.is_empty():
        lncomm_sorted = lncomm_df.sort(["ACCTNO","COMMNO"])
        appr = almcom.join(lncomm_sorted, on=["ACCTNO","COMMNO"], how="left")
        # For PRODCD='34190': keep FIRST.ACCTNO or FIRST.COMMNO
        def dedup_rc(df: pl.DataFrame) -> pl.DataFrame:
            df = df.sort(["ACCTNO","COMMNO"])
            rows = []
            seen: set = set()
            for r in df.iter_rows(named=True):
                key    = (r["ACCTNO"], r["COMMNO"])
                prodcd = (r.get("PRODCD") or "").strip()
                if prodcd == "34190":
                    if key not in seen:
                        seen.add(key)
                        rows.append(r)
                else:
                    rows.append(r)
            return pl.DataFrame(rows) if rows else df.clear()
        appr = dedup_rc(appr)
    else:
        appr = almcom

    # ALMNOCOM deduplication for PRODCD='34190'
    almnocom_sorted = almnocom.sort(["ACCTNO","APPRLIM2"])
    appr1_rows: list = []
    dup_rows:   list = []
    seen_acct_appr: set = set()

    for r in almnocom_sorted.iter_rows(named=True):
        prodcd = (r.get("PRODCD") or "").strip()
        key    = (r["ACCTNO"], r.get("APPRLIM2"))
        if prodcd == "34190":
            if key not in seen_acct_appr:
                seen_acct_appr.add(key)
                appr1_rows.append(r)
            else:
                dup_rows.append({**r, "DUPLI": 1})
        else:
            appr1_rows.append(r)

    # Re-merge APPR1 with DUP where BALANCE >= APPRLIM2
    dup_filt = [d for d in dup_rows
                if (d.get("BALANCE") or 0) >= (d.get("APPRLIM2") or 0)]
    dup_keys = {(d["ACCTNO"], d.get("APPRLIM2")) for d in dup_filt}

    appr1_final: list = []
    for r in almnocom_sorted.iter_rows(named=True):
        prodcd = (r.get("PRODCD") or "").strip()
        key    = (r["ACCTNO"], r.get("APPRLIM2"))
        if prodcd == "34190":
            if key in dup_keys:
                if (r.get("BALANCE") or 0) >= (r.get("APPRLIM2") or 0):
                    appr1_final.append(r)
            else:
                acct_key = (r["ACCTNO"], r.get("APPRLIM2"))
                if acct_key not in {(a["ACCTNO"], a.get("APPRLIM2"))
                                    for a in appr1_final}:
                    appr1_final.append(r)
        else:
            appr1_final.append(r)

    appr1     = pl.DataFrame(appr1_final) if appr1_final else almnocom.clear()
    alm_final = pl.concat([appr, appr1], how="diagonal").sort(["ACCTNO"])
    return alm_final


def section_undrawn_by_maturity():
    """
    Undrawn portion by original maturity and purpose; calls _build_appr_dataset.
    _TYPE_=9  (AMTIND+FISSPURP): purpose-based BNMCODE.
    _TYPE_=14 (AMTIND+PRODCD+ORIGMT+FISSPURP): maturity-based BNMCODE.
    """
    loan_df = con.execute(f"SELECT * FROM read_parquet('{LOAN_PARQUET}')").pl()
    alm_rc  = _build_appr_dataset(loan_df)

    # PROC SUMMARY by AMTIND, PRODCD, ORIGMT, FISSPURP; VAR UNDRAWN
    almx = con.execute("""
        SELECT AMTIND, PRODCD, ORIGMT, FISSPURP, SUM(UNDRAWN) AS AMOUNT
        FROM alm_rc
        GROUP BY GROUPING SETS (
            (AMTIND, FISSPURP),
            (AMTIND, PRODCD, ORIGMT, FISSPURP)
        )
    """).pl()

    ualmx = con.execute(f"""
        SELECT AMTIND, PRODCD, ORIGMT, FISSPURP, SUM(UNDRAWN) AS AMOUNT
        FROM read_parquet('{ULOAN_PARQUET}')
        GROUP BY GROUPING SETS (
            (AMTIND, FISSPURP),
            (AMTIND, PRODCD, ORIGMT, FISSPURP)
        )
    """).pl()

    combined = pl.concat([almx, ualmx], how="diagonal")

    long_prods = (
        "34311","34312","34313","34314","34315","34316","34320","34349",
        "34111","34112","34113","34114","34115","34116","34117","34120","34149"
    )
    od_prods = ("34180","34380")

    rows = []
    for row in combined.iter_rows(named=True):
        amtind   = row["AMTIND"]   or ""
        prodcd   = row.get("PRODCD")
        origmt   = (row.get("ORIGMT")   or "").strip()
        fisspurp = (row.get("FISSPURP") or "").strip()
        amount   = row["AMOUNT"]

        # _TYPE_=9: AMTIND+FISSPURP (PRODCD+ORIGMT are NULL)
        if prodcd is None and fisspurp:
            rows.append({"BNMCODE": f"562000000{fisspurp}Y",
                         "AMTIND": amtind, "AMOUNT": amount})
            if fisspurp in ("0220","0230","0210","0211","0212"):
                rows.append({"BNMCODE": "5620000000200Y",
                             "AMTIND": amtind, "AMOUNT": amount})

        # _TYPE_=14: all four present
        elif prodcd is not None:
            prodcd = prodcd.strip()
            if prodcd in long_prods:
                if origmt in ("10","12","13","14","15","16","17"):
                    rows.append({"BNMCODE": "5621000100000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                elif origmt in ("20","21","22","23","24","25","26","31","32","33"):
                    rows.append({"BNMCODE": "5621000200000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
            elif prodcd in od_prods:
                if origmt in ("10","12","13","14","15","16","17"):
                    rows.append({"BNMCODE": "5622000100000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
            else:
                if origmt in ("10","12","13","14","15","16","17"):
                    rows.append({"BNMCODE": "5629900100000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})
                elif origmt in ("20","21","22","23","24","25","26","31","32","33"):
                    rows.append({"BNMCODE": "5629900200000Y",
                                 "AMTIND": amtind, "AMOUNT": amount})

    if rows:
        append_to_lalm(pl.DataFrame(rows))

    # Undrawn by SECTORCD
    almx2 = con.execute("""
        SELECT AMTIND, PRODCD, ORIGMT, SECTORCD, SUM(UNDRAWN) AS AMOUNT
        FROM alm_rc
        WHERE SECTORCD <> '0410'
        GROUP BY AMTIND, PRODCD, ORIGMT, SECTORCD
    """).pl()

    ualmx2 = con.execute(f"""
        SELECT AMTIND, PRODCD, ORIGMT, SECTORCD, SUM(UNDRAWN) AS AMOUNT
        FROM read_parquet('{ULOAN_PARQUET}')
        WHERE SECTORCD <> '0410'
        GROUP BY AMTIND, PRODCD, ORIGMT, SECTORCD
    """).pl()

    combined2 = pl.concat([almx2, ualmx2], how="diagonal")
    # %INC PGM(SECTMAP)
    combined2 = apply_sectmap(combined2)

    sec_rows = []
    for row in combined2.iter_rows(named=True):
        amtind   = row["AMTIND"]   or ""
        sectorcd = (row["SECTORCD"] or "").strip()
        amount   = row["AMOUNT"]
        if sectorcd:
            sec_rows.append({"BNMCODE": f"562000000{sectorcd}Y",
                             "AMTIND": amtind, "AMOUNT": amount})

    if sec_rows:
        append_to_lalm(pl.DataFrame(sec_rows))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 18 : APPLICATIONS APPROVED DURING THE MONTH (COUNT)
# ═══════════════════════════════════════════════════════════════════════════════

def section_approvals_count():
    """
    PROC SUMMARY by AMTIND, CUSTCD where PRODCD[:2]='34' and
    MONTH(APPRDATE)=MONTH(RDATE) and YEAR(APPRDATE)=YEAR(RDATE).
    From LOAN and ULOAN. _FREQ_ * 1000 = AMOUNT.
    _TYPE_=2 (AMTIND only), _TYPE_=3 (AMTIND+CUSTCD).
    """
    rmonth = RDATE.month
    ryear  = RDATE.year

    alm = con.execute(f"""
        SELECT AMTIND, CUSTCD, COUNT(*) AS FREQ
        FROM read_parquet('{LOAN_PARQUET}')
        WHERE LEFT(PRODCD, 2) = '34'
          AND MONTH(APPRDATE) = {rmonth}
          AND YEAR(APPRDATE)  = {ryear}
        GROUP BY GROUPING SETS ((AMTIND), (AMTIND, CUSTCD))
    """).pl()

    ualm = con.execute(f"""
        SELECT AMTIND, CUSTCD, COUNT(*) AS FREQ
        FROM read_parquet('{ULOAN_PARQUET}')
        WHERE LEFT(PRODCD, 2) = '34'
          AND MONTH(APPRDATE) = {rmonth}
          AND YEAR(APPRDATE)  = {ryear}
        GROUP BY GROUPING SETS ((AMTIND), (AMTIND, CUSTCD))
    """).pl()

    combined = pl.concat([alm, ualm], how="diagonal")

    rows = []
    for row in combined.iter_rows(named=True):
        amtind = row["AMTIND"] or ""
        custcd = row["CUSTCD"]
        amount = (row["FREQ"] or 0) * 1000

        if custcd is None:
            # _TYPE_=2
            rows.append({"BNMCODE": "8015000000000Y", "AMTIND": amtind, "AMOUNT": amount})
        else:
            custcd = str(custcd).strip()
            # _TYPE_=3
            if custcd in ("61", "66"):
                rows.append({"BNMCODE": "8015061000000Y", "AMTIND": amtind, "AMOUNT": amount})
            elif custcd == "77":
                rows.append({"BNMCODE": "8015077000000Y", "AMTIND": amtind, "AMOUNT": amount})

    if rows:
        append_to_lalm(pl.DataFrame(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 19 : SPECIAL PURPOSE ITEMS FOR ISLAMIC LOANS (30701-30799)
# ═══════════════════════════════════════════════════════════════════════════════

def section_islamic_special_purpose():
    """
    PROC SUMMARY NWAY by PRODUCT, AMTIND; VAR BALANCE where AMTIND='I'.
    BNMCODE determined by PRODUCT.
    """
    alm = con.execute(f"""
        SELECT PRODUCT, AMTIND, SUM(BALANCE) AS AMOUNT
        FROM read_parquet('{LOAN_PARQUET}')
        WHERE AMTIND = 'I'
        GROUP BY PRODUCT, AMTIND
    """).pl()

    rows = []
    for row in alm.iter_rows(named=True):
        product = row["PRODUCT"]
        amtind  = row["AMTIND"] or ""
        amount  = row["AMOUNT"]

        if product == 180:
            bc = "3070700000000Y"
        elif product in (108,135,136,182):
            bc = "3079900000000Y"
        elif product in (181,193):
            bc = "3070200000000Y"
        elif product in (128,130):
            bc = "3070300000000Y"
        elif product in (131,132):
            bc = "3070300000000Y"
        else:
            bc = "3070100000000Y"

        rows.append({"BNMCODE": bc, "AMTIND": amtind, "AMOUNT": amount})

    if rows:
        append_to_lalm(pl.DataFrame(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 20 : GROSS LOANS BY TYPE OF REPRICING
# ═══════════════════════════════════════════════════════════════════════════════

def section_gross_loan_repricing():
    """
    PROC SUMMARY NWAY MISSING by PRODCD, PRODUCT, AMTIND, NTINDEX, COSTFUND
    where PRODCD[:2]='34' OR PRODCD[:2]='54'; VAR BALANCE.
    BNMCODE based on PRODUCT (124,145) and NTINDEX.
    """
    alm = con.execute(f"""
        SELECT PRODCD, PRODUCT, AMTIND, NTINDEX, COSTFUND, SUM(BALANCE) AS AMOUNT
        FROM read_parquet('{LOAN_PARQUET}')
        WHERE LEFT(PRODCD, 2) IN ('34','54')
        GROUP BY PRODCD, PRODUCT, AMTIND, NTINDEX, COSTFUND
    """).pl()

    rows = []
    for row in alm.iter_rows(named=True):
        product = row["PRODUCT"]
        amtind  = row["AMTIND"]  or ""
        ntindex = row.get("NTINDEX") or 0
        amount  = row["AMOUNT"]

        bic = "     "
        if product in (124,145):
            bic = "30591"
            if ntindex > 0:
                bic = "30595"

        if bic.strip():
            rows.append({"BNMCODE": f"{bic}00000000Y",
                         "AMTIND": amtind, "AMOUNT": amount})

    if rows:
        almloan = pl.DataFrame(rows)
        # PROC SUMMARY NWAY by BNMCODE, AMTIND; SUM AMOUNT
        almloan = (
            almloan
            .group_by(["BNMCODE","AMTIND"])
            .agg(pl.col("AMOUNT").sum())
        )
        append_to_lalm(almloan)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 21 : LOAN – BY SECTOR CODE
# ═══════════════════════════════════════════════════════════════════════════════

def section_loan_by_sector():
    """
    PROC SUMMARY NWAY by SECTORCD, AMTIND where PRODCD[:2]='34' AND SECTORCD NE '0410'.
    %INC SECTMAP. BNMCODE = '340000000'||SECTORCD||'Y'.
    """
    alm = con.execute(f"""
        SELECT SECTORCD, AMTIND, SUM(BALANCE) AS AMOUNT
        FROM read_parquet('{LOAN_PARQUET}')
        WHERE LEFT(PRODCD, 2) = '34'
          AND SECTORCD <> '0410'
        GROUP BY SECTORCD, AMTIND
    """).pl()

    # %INC PGM(SECTMAP)
    alm = apply_sectmap(alm)

    rows = []
    for row in alm.iter_rows(named=True):
        sectorcd = (row["SECTORCD"] or "").strip()
        amtind   = row["AMTIND"] or ""
        amount   = row["AMOUNT"]
        if sectorcd:
            rows.append({"BNMCODE": f"340000000{sectorcd}Y",
                         "AMTIND": amtind, "AMOUNT": amount})

    if rows:
        append_to_lalm(pl.DataFrame(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 22 : DISBURSEMENT, REPAYMENT, APPROVAL – BY PURPOSE CODE
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_disburse_repaid(
    alm1_df: pl.DataFrame,
    alm_df:  pl.DataFrame,
    key_cols: list,
) -> pl.DataFrame:
    """
    Merge previous period (ALM1=A) with current period (ALM=B) by key_cols.
    Compute DISBURSE, REPAID, ROLLOVER, APPRLIM2.
    """
    alm1_renamed = alm1_df.rename({"BALANCE": "LASTBAL", "NOTETERM": "LASTNOTE"})
    merged = alm_df.join(alm1_renamed, on=key_cols, how="outer", suffix="_prev")

    rows = []
    rdate = RDATE
    sdate = SDATE

    for row in merged.iter_rows(named=True):
        apprdate = row.get("APPRDATE")
        apprlim2 = row.get("APPRLIM2") or 0.0
        balance  = row.get("BALANCE")  or 0.0
        lastbal  = row.get("LASTBAL")
        prodcd   = (row.get("PRODCD")  or "").strip()
        noteterm = row.get("NOTETERM")
        earnterm = row.get("EARNTERM")
        lastnote = row.get("LASTNOTE")

        if apprdate and isinstance(apprdate, date):
            if apprdate > rdate:
                continue
            if apprdate < sdate:
                apprlim2 = 0.0

        rollover = 0.0
        if prodcd == "34190" and noteterm != earnterm and noteterm != lastnote:
            rollover = balance

        disburse = 0.0
        repaid   = 0.0
        has_a    = lastbal is not None
        has_b    = balance is not None

        if has_a and has_b:
            if lastbal > balance:
                repaid   = lastbal - balance
            else:
                disburse = balance - lastbal
        if not has_b and has_a:
            repaid = lastbal
        if not has_a and has_b:
            disburse = balance

        keep = {k: row.get(k) for k in
                ("FISSPURP","SECTORCD","AMTIND","CUSTCD")}
        keep.update({
            "DISBURSE": disburse, "REPAID": repaid,
            "APPRLIM2": apprlim2, "ROLLOVER": rollover,
        })
        rows.append(keep)

    return pl.DataFrame(rows) if rows else pl.DataFrame()


def section_disbursement_by_purpose(loan1_df: pl.DataFrame):
    """
    Disbursement / repayment / approval – by FISSPURP.
    Merges LOAN1 (previous period) with LOAN (current) and ULOAN.
    Outputs BNMCODE patterns for 683/783/821 prefixes.
    """
    keep_cols   = ["ACCTNO","NOTENO","FISSPURP","PRODUCT","NOTETERM",
                   "BALANCE","PRODCD","CUSTCD","AMTIND","SECTORCD"]
    prod_filter = ("341","342","343","344")
    prod_list   = (225,226)

    alm1_df = (
        loan1_df
        .filter(
            pl.col("PRODCD").str.slice(0,3).is_in(prod_filter)
            | pl.col("PRODUCT").is_in(prod_list)
        )
        .select([c for c in keep_cols if c in loan1_df.columns])
        .sort(["ACCTNO","NOTENO","CUSTCD","FISSPURP","SECTORCD"])
    )

    loan_df = con.execute(f"SELECT * FROM read_parquet('{LOAN_PARQUET}')").pl()
    alm_df  = (
        loan_df
        .filter(
            pl.col("PRODCD").str.slice(0,3).is_in(prod_filter)
            | pl.col("PRODUCT").is_in(prod_list)
        )
        .sort(["ACCTNO","NOTENO","CUSTCD","FISSPURP","SECTORCD"])
    )

    alm_work = _compute_disburse_repaid(
        alm1_df, alm_df,
        key_cols=["ACCTNO","NOTENO","CUSTCD","FISSPURP","SECTORCD"]
    )

    # ULOAN
    uloan_df   = con.execute(f"SELECT * FROM read_parquet('{ULOAN_PARQUET}')").pl()
    uloan_filt = uloan_df.filter(
        (pl.col("APPRDATE") >= pl.lit(SDATE)) &
        (pl.col("APPRDATE") <= pl.lit(RDATE))
    )

    if not uloan_filt.is_empty():
        alm_work = pl.concat(
            [alm_work, uloan_filt.select(alm_work.columns)], how="diagonal"
        )

    # PROC SUMMARY NWAY by FISSPURP, AMTIND
    summary = (
        alm_work
        .group_by(["FISSPURP","AMTIND"])
        .agg([
            pl.col("DISBURSE").sum(),
            pl.col("REPAID").sum(),
            pl.col("APPRLIM2").sum(),
            pl.col("ROLLOVER").sum(),
        ])
    )

    # Remap FISSPURP 022x/023x → '0200'
    summary1 = summary.filter(
        pl.col("FISSPURP").is_in(["0220","0230","0210","0211","0212"])
    ).with_columns(pl.lit("0200").alias("FISSPURP"))
    summary_full = pl.concat([summary, summary1], how="diagonal")

    rows = []
    for row in summary_full.iter_rows(named=True):
        amtind   = row["AMTIND"]   or ""
        fisspurp = (row["FISSPURP"] or "").strip()
        dis      = row["DISBURSE"]  or 0.0
        rep      = row["REPAID"]    or 0.0
        rol      = row["ROLLOVER"]  or 0.0

        rows.append({"BNMCODE": f"683400000{fisspurp}Y", "AMTIND": amtind, "AMOUNT": dis})
        rows.append({"BNMCODE": f"783400000{fisspurp}Y", "AMTIND": amtind, "AMOUNT": rep})
        rows.append({"BNMCODE": f"821520000{fisspurp}Y", "AMTIND": amtind, "AMOUNT": rol})

    if rows:
        append_to_lalm(pl.DataFrame(rows))

    # SMI sub-report by CUSTCD
    smi_summary = (
        alm_work
        .group_by(["FISSPURP","CUSTCD","AMTIND"])
        .agg([
            pl.col("DISBURSE").sum(),
            pl.col("REPAID").sum(),
            pl.col("APPRLIM2").sum(),
            pl.col("ROLLOVER").sum(),
        ])
    )
    smi1 = smi_summary.filter(
        pl.col("FISSPURP").is_in(["0220","0230","0210","0211","0212"])
    ).with_columns(pl.lit("0200").alias("FISSPURP"))
    smi_full = pl.concat([smi_summary, smi1], how="diagonal")

    _emit_smi_rows(smi_full, key_col="FISSPURP")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 23 : DISBURSEMENT, REPAYMENT, APPROVAL – BY SECTORIAL CODE
# ═══════════════════════════════════════════════════════════════════════════════

def section_disbursement_by_sector(loan1_df: pl.DataFrame):
    """
    Same merge logic as section_disbursement_by_purpose but grouped by SECTORCD.
    """
    keep_cols_1 = ["ACCTNO","NOTENO","SECTORCD","PRODUCT","NOTETERM",
                   "BALANCE","PRODCD","CUSTCD","AMTIND","FISSPURP"]
    prod_filter = ("341","342","343","344")
    prod_list   = (225,226)

    alm1_df = (
        loan1_df
        .filter(
            (pl.col("PRODCD").str.slice(0,3).is_in(prod_filter)
             | pl.col("PRODUCT").is_in(prod_list))
            & (pl.col("SECTORCD") != "0410")
        )
        .select([c for c in keep_cols_1 if c in loan1_df.columns])
        .sort(["ACCTNO","NOTENO","CUSTCD","FISSPURP","SECTORCD"])
    )

    loan_df = con.execute(f"SELECT * FROM read_parquet('{LOAN_PARQUET}')").pl()
    alm_df  = (
        loan_df
        .filter(
            (pl.col("PRODCD").str.slice(0,3).is_in(prod_filter)
             | pl.col("PRODUCT").is_in(prod_list))
            & (pl.col("SECTORCD") != "0410")
        )
        .sort(["ACCTNO","NOTENO","CUSTCD","FISSPURP","SECTORCD"])
    )

    almx_full = _compute_disburse_repaid(
        alm1_df, alm_df,
        key_cols=["ACCTNO","NOTENO","CUSTCD","FISSPURP","SECTORCD"]
    )

    uloan_df   = con.execute(f"SELECT * FROM read_parquet('{ULOAN_PARQUET}')").pl()
    uloan_filt = uloan_df.filter(
        (pl.col("APPRDATE") >= pl.lit(SDATE)) &
        (pl.col("APPRDATE") <= pl.lit(RDATE))
    )

    if not uloan_filt.is_empty():
        almx_full = pl.concat(
            [almx_full, uloan_filt.select(almx_full.columns)], how="diagonal"
        )

    # Keep ALMX copy for SMI sub-report
    almx_copy = almx_full.clone()

    # PROC SUMMARY NWAY by SECTORCD, AMTIND → then %INC SECTMAP
    summary = (
        almx_full
        .group_by(["SECTORCD","AMTIND"])
        .agg([
            pl.col("DISBURSE").sum(),
            pl.col("REPAID").sum(),
            pl.col("APPRLIM2").sum(),
            pl.col("ROLLOVER").sum(),
        ])
    )

    # %INC PGM(SECTMAP)
    summary = apply_sectmap(summary)

    rows = []
    for row in summary.iter_rows(named=True):
        amtind   = row["AMTIND"]   or ""
        sectorcd = (row["SECTORCD"] or "").strip()
        dis      = row["DISBURSE"]  or 0.0
        rep      = row["REPAID"]    or 0.0
        rol      = row["ROLLOVER"]  or 0.0

        rows.append({"BNMCODE": f"683400000{sectorcd}Y", "AMTIND": amtind, "AMOUNT": dis})
        rows.append({"BNMCODE": f"783400000{sectorcd}Y", "AMTIND": amtind, "AMOUNT": rep})
        rows.append({"BNMCODE": f"821520000{sectorcd}Y", "AMTIND": amtind, "AMOUNT": rol})

    if rows:
        append_to_lalm(pl.DataFrame(rows))

    # SMI sub-report by SECTORCD+CUSTCD
    smi_summary = (
        almx_copy
        .group_by(["SECTORCD","CUSTCD","AMTIND"])
        .agg([
            pl.col("DISBURSE").sum(),
            pl.col("REPAID").sum(),
            pl.col("APPRLIM2").sum(),
            pl.col("ROLLOVER").sum(),
        ])
    )
    # %INC PGM(SECTMAP) – applied to SMI sector grouping as well
    smi_summary = apply_sectmap(smi_summary)
    _emit_smi_rows(smi_summary, key_col="SECTORCD")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 22/23 SMI HELPER
# ═══════════════════════════════════════════════════════════════════════════════

def _emit_smi_rows(df: pl.DataFrame, key_col: str):
    """
    Emit ALMSMI rows for each CUSTCD group, writing DISBURSE/REPAID/ROLLOVER
    BNMCODE patterns. key_col is either 'FISSPURP' or 'SECTORCD'.
    """
    rows = []
    for row in df.iter_rows(named=True):
        amtind = row["AMTIND"] or ""
        kv     = (row[key_col] or "").strip()
        custcd = (row["CUSTCD"] or "").strip()
        dis    = row["DISBURSE"]  or 0.0
        rep    = row["REPAID"]    or 0.0
        rol    = row["ROLLOVER"]  or 0.0

        def add(prefix_d, prefix_r, prefix_l, suffix="Y"):
            rows.append({"BNMCODE": f"{prefix_d}{kv}{suffix}", "AMTIND": amtind, "AMOUNT": dis})
            rows.append({"BNMCODE": f"{prefix_r}{kv}{suffix}", "AMTIND": amtind, "AMOUNT": rep})
            rows.append({"BNMCODE": f"{prefix_l}{kv}{suffix}", "AMTIND": amtind, "AMOUNT": rol})

        if custcd in ("41","42","43","44","46","47","48","49","51",
                      "52","53","54","77","78","79"):
            add(f"68340{custcd}00", f"78340{custcd}00", f"82152{custcd}00")

        if custcd in ("61","41","42","43","62","44","46","47",
                      "63","48","49","51","57","59","75","52","53","54"):
            rows.append({"BNMCODE": f"683406000{kv}Y", "AMTIND": amtind, "AMOUNT": dis})
            rows.append({"BNMCODE": f"783406000{kv}Y", "AMTIND": amtind, "AMOUNT": rep})

        if custcd in ("02","03","11","12"):
            add("683401000", "783401000", "821521000")
        if custcd in ("20","13","17","30","32","33","34","35",
                      "36","37","38","39","40","04","05","06"):
            add("683402000", "783402000", "821522000")
        if custcd in ("71","72","73","74"):
            add("683407000", "783407000", "821527000")
        if custcd in ("77","78"):
            add("683407600", "783407600", "821527600")
        if custcd in ("81","82","83","84","85","86","90","91",
                      "92","95","96","98","99"):
            sectorcd_val = row.get("SECTORCD","") or ""
            if sectorcd_val != "9999":
                add("683408000", "783408000", "821528000")
                add("683408500", "783408500", "821528500")
            if kv in ("1000","2000","3000","4000","5000",
                      "6000","7000","8000","9000","9999","9700"):
                rows.append({"BNMCODE": "6834080000000Y", "AMTIND": amtind, "AMOUNT": dis})
                rows.append({"BNMCODE": "7834080000000Y", "AMTIND": amtind, "AMOUNT": rep})
                rows.append({"BNMCODE": "8215280000000Y", "AMTIND": amtind, "AMOUNT": rol})
                rows.append({"BNMCODE": "6834085000000Y", "AMTIND": amtind, "AMOUNT": dis})
                rows.append({"BNMCODE": "7834085000000Y", "AMTIND": amtind, "AMOUNT": rep})
                rows.append({"BNMCODE": "8215285000000Y", "AMTIND": amtind, "AMOUNT": rol})
                if custcd in ("81","82","83","84","85","86","90","91","92","98","99"):
                    rows.append({"BNMCODE": "6834085009999Y", "AMTIND": amtind, "AMOUNT": dis})
                    rows.append({"BNMCODE": "7834085009999Y", "AMTIND": amtind, "AMOUNT": rep})
                    rows.append({"BNMCODE": "8215285009999Y", "AMTIND": amtind, "AMOUNT": rol})
                    rows.append({"BNMCODE": "6834080009999Y", "AMTIND": amtind, "AMOUNT": dis})
                    rows.append({"BNMCODE": "7834080009999Y", "AMTIND": amtind, "AMOUNT": rep})
                    rows.append({"BNMCODE": "8215280009999Y", "AMTIND": amtind, "AMOUNT": rol})

        if custcd in ("95","96"):
            if kv in ("1000","2000","3000","4000","5000",
                      "6000","7000","8000","9000","9999","9700"):
                rows.append({"BNMCODE": "6834095000000Y", "AMTIND": amtind, "AMOUNT": dis})
                rows.append({"BNMCODE": "7834095000000Y", "AMTIND": amtind, "AMOUNT": rep})
                rows.append({"BNMCODE": "8215295000000Y", "AMTIND": amtind, "AMOUNT": rol})
            if (row.get("SECTORCD","") or "") != "9999":
                add("683409500", "783409500", "821529500")

        if custcd in ("77","78"):
            if kv in ("1000","2000","3000","4000","5000",
                      "6000","7000","8000","9000","9999","9700"):
                rows.append({"BNMCODE": "6834076000000Y", "AMTIND": amtind, "AMOUNT": dis})
                rows.append({"BNMCODE": "7834076000000Y", "AMTIND": amtind, "AMOUNT": rep})
                rows.append({"BNMCODE": "8215276000000Y", "AMTIND": amtind, "AMOUNT": rol})

        if custcd in ("41","42","43","61"):
            add("683406100", "783406100", "821526100")
        if custcd in ("62","44","46","47"):
            add("683406200", "783406200", "821526200")
        if custcd in ("63","48","49","51"):
            add("683406300", "783406300", "821526300")
        if custcd in ("64","52","53","54","59","75","57"):
            add("683406400", "783406400", "821526400")
        if custcd in ("41","42","43","44","46","47","48","49","51",
                      "52","53","54","61","62","63","64"):
            add("683406500", "783406500", "821526500")

    if rows:
        append_to_lalm(pl.DataFrame(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 24 : LOAN – BY STATE CODE AND BY SECTOR CODE – 9700
# ═══════════════════════════════════════════════════════════════════════════════

def section_loan_by_state_sector():
    """
    PROC SUMMARY NWAY by SECTORCD, STATECD, AMTIND where PRODCD[:2]='34'
    AND SECTORCD NE '0410'. %INC SECTMAP.
    IF SECTORCD='9700' → BNMCODE = '340000000'||SECTORCD||STATECD.
    """
    alq = con.execute(f"""
        SELECT SECTORCD, STATECD, AMTIND, SUM(BALANCE) AS AMOUNT
        FROM read_parquet('{LOAN_PARQUET}')
        WHERE LEFT(PRODCD, 2) = '34'
          AND SECTORCD <> '0410'
        GROUP BY SECTORCD, STATECD, AMTIND
    """).pl()

    # %INC PGM(SECTMAP)
    alq = apply_sectmap(alq)

    rows = []
    for row in alq.iter_rows(named=True):
        sectorcd = (row["SECTORCD"] or "").strip()
        statecd  = (row["STATECD"]  or "").strip()
        amtind   = row["AMTIND"] or ""
        amount   = row["AMOUNT"]
        if sectorcd == "9700":
            rows.append({"BNMCODE": f"340000000{sectorcd}{statecd}",
                         "AMTIND": amtind, "AMOUNT": amount})

    if rows:
        append_to_lalm(pl.DataFrame(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# FINAL CONSOLIDATION
# ═══════════════════════════════════════════════════════════════════════════════

def final_consolidation():
    """
    PROC SUMMARY NWAY by BNMCODE, AMTIND; VAR AMOUNT; SUM → overwrite LALM.
    """
    if not os.path.exists(LALM_PARQUET):
        return
    final = con.execute(f"""
        SELECT BNMCODE, AMTIND, SUM(AMOUNT) AS AMOUNT
        FROM read_parquet('{LALM_PARQUET}')
        GROUP BY BNMCODE, AMTIND
    """).pl()
    final.write_parquet(LALM_PARQUET)


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    # %INC PGM(PBBLNFMT) – formats imported at module level above
    # %INC PGM(SECTMAP)  – process_sectmap imported at module level above

    # Build LOAN1 (previous period, products 124/145)
    loan1_df = build_loan1()

    # Execute each reporting section in order
    section_npl_simple()
    section_npl_by_cust_sector()
    section_gross_loan_by_apprlim()
    section_gross_loan_by_collateral()
    section_utilised_loan_count()
    # (NO OF UTILISED CURRENT ACCOUNTS – no SAS code present in original)
    section_loan_by_cust_purpose()
    section_loan_by_cust_sector()
    section_sme_by_state()
    section_rm_loans()
    section_rm_overdraft()
    section_rm_by_cust_maturity()
    section_term_loan_by_prodcd_purpose()
    section_term_loan_by_origmt()
    section_term_loan_by_remainmt()
    section_total_approved_limit()
    section_unutilised_loan_count()
    section_undrawn_by_maturity()
    section_approvals_count()
    section_islamic_special_purpose()
    section_gross_loan_repricing()
    section_loan_by_sector()
    section_disbursement_by_purpose(loan1_df)
    section_disbursement_by_sector(loan1_df)
    section_loan_by_state_sector()

    # Final consolidation
    final_consolidation()

    print(f"LALMP124 complete. BNM.LALM written to: {LALM_PARQUET}")


if __name__ == "__main__":
    main()
