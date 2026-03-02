#!/usr/bin/env python3
"""
Program : EIBDFD02
Function: Daily FD Movement - Placement/Withdrawal of RM100K & Above
          Previously known as 'FDDYMOVE'
"""

import duckdb
import polars as pl
import os
import sys
from datetime import datetime, timedelta
from typing import Optional

# Import format definitions from PBBDPFMT
from PBBDPFMT import FDDenomFormat

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR       = os.environ.get("BASE_DIR", "/data")
TEMP_DIR       = os.path.join(BASE_DIR, "temp")
MIS_DIR        = os.path.join(BASE_DIR, "mis")
OUTPUT_DIR     = os.path.join(BASE_DIR, "output")

# Input files
DPFD4010_FILE  = os.path.join(BASE_DIR, "DPFD4010.parquet")
BRHFILE        = os.path.join(BASE_DIR, "BRHFILE.txt")

# Intermediate / output parquet files
FD4010_FILE    = os.path.join(TEMP_DIR, "FD4010.parquet")
FD4001_FILE    = os.path.join(TEMP_DIR, "FD4001.parquet")
WITHDRAW_FILE  = os.path.join(MIS_DIR,  "WITHDRAW.parquet")
PLACEMNT_FILE  = os.path.join(MIS_DIR,  "PLACEMNT.parquet")

# Report output
REPORT_FILE    = os.path.join(OUTPUT_DIR, "EIBDFD02.txt")

# ASA carriage-control constants
ASA_NEWPAGE    = "1"
ASA_NEWLINE    = " "
ASA_DOUBLESKIP = "0"

PAGE_LINES     = 60   # lines per page (excluding header)

os.makedirs(TEMP_DIR,  exist_ok=True)
os.makedirs(MIS_DIR,   exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================================
# STEP 1 – DERIVE REPORT DATE FROM DPFD4010 (first record only)
# ============================================================================

def derive_report_date() -> dict:
    """
    Read the first record of DPFD4010 to extract TBDATE (packed-decimal at
    offset 106) which has already been decoded into the parquet column 'TBDATE'.

    Returns dict with REPTDATE, REPTDATX, REPTYEAR, RDATE, XDATE.
    """
    con = duckdb.connect()
    row = con.execute(
        f"SELECT TBDATE FROM read_parquet('{DPFD4010_FILE}') LIMIT 1"
    ).fetchone()
    con.close()

    if row is None:
        raise ValueError("DPFD4010 is empty – cannot derive report date.")

    tbdate_raw = row[0]  # Expected as integer MMDDYY (packed-decimal decoded)
    # Convert packed-decimal integer to date via MMDDYY8 interpretation
    tbdate_str = str(int(tbdate_raw)).zfill(8)   # e.g. "05152025"
    reptdate   = datetime.strptime(tbdate_str, "%m%d%Y")
    reptdatx   = reptdate + timedelta(days=1)

    return {
        "reptdate"  : reptdate,
        "reptdatx"  : reptdatx,
        "reptyear"  : reptdate.strftime("%Y"),
        "rdate"     : reptdate.strftime("%d/%m/%Y"),
        "xdate"     : int(reptdatx.strftime("%j")),   # Z5 Julian-like; kept as int
    }


# ============================================================================
# STEP 2 – PARSE DPFD4010: BUILD FD4010 (transactions) & FD4001 (account info)
# ============================================================================

def build_fd4010_fd4001(ctx: dict) -> None:
    """
    Read DPFD4010 parquet and split into two datasets:
      FD4010 – transaction records  (REPTNO=4010, FMTCODE=1)
      FD4001 – account-detail recs  (REPTNO=4001, FMTCODE=1)

    Applies the LMATDATE / MATDT correction logic from the SAS program.
    """
    reptdate  = ctx["reptdate"]
    reptdatx  = ctx["reptdatx"]

    con = duckdb.connect()
    df_raw = con.execute(
        f"SELECT * FROM read_parquet('{DPFD4010_FILE}')"
    ).pl()
    con.close()

    reptdate_int = int(reptdate.strftime("%Y%m%d"))

    # ── FD4010 transaction records ─────────────────────────────────────────
    TRAN_CD_KEEP = {297, 770, 970, 971, 972}
    # CODE 770 - PLACEMENT FOR THE DAY (NEW CD)
    #       297 - FORCE PLACEMENT FOR NEGOTIATED RATE
    # 970,971,972 - WITHDRAWAL FOR THE DAY
    #   970       - INTEREST
    #   971       - PRINCIPAL
    #   972       - PENALTY

    fd4010 = (
        df_raw
        .filter(
            (pl.col("REPTNO") == 4010) &
            (pl.col("FMTCODE") == 1) &
            (pl.col("TRANCD").is_in(list(TRAN_CD_KEEP)))
        )
        .select([
            pl.lit(reptdate_int).alias("REPTDATE"),
            "BRANCH", "NAMETRAN", "ACCTNO", "CDNO",
            "OPENIND", "TRANCD", "TRANAMT", "TRANDTE", "PENALTY",
        ])
    )

    # ── FD4001 account-detail records ────────────────────────────────────--
    fd4001_raw = df_raw.filter(
        (pl.col("REPTNO") == 4001) & (pl.col("FMTCODE") == 1)
    )

    def fix_matdate(row):
        """Apply LMATDATE → MATDT correction (vectorised below)."""
        lmatdate = row["LMATDATE"]
        renew    = row["RENEW"]
        matdt    = row["MATDT"]

        if lmatdate and lmatdate > 0:
            lmdate_str = str(int(lmatdate)).zfill(8)
            try:
                lmdates = datetime.strptime(lmdate_str[:8], "%m%d%Y")
            except ValueError:
                lmdates = None
            if lmdates and renew == "N" and lmdates == reptdatx:
                matdt = lmdates.strftime("%Y%m%d")
        return matdt

    # Vectorised approach using polars expressions where possible
    # ORGDATE: MMDDYY6 from first 6 chars of ORGDT zero-padded to 9
    # MATDATE: YYMMDD8 parse of MATDT string

    fd4001_rows = fd4001_raw.to_dicts()
    records = []
    for r in fd4001_rows:
        # Fix MATDT from LMATDATE if applicable
        lmatdate = r.get("LMATDATE", 0) or 0
        renew    = r.get("RENEW", "")
        matdt    = str(r.get("MATDT", "")) if r.get("MATDT") is not None else ""

        if lmatdate > 0:
            lm_str = str(int(lmatdate)).zfill(8)
            try:
                lmdates = datetime.strptime(lm_str[:8], "%m%d%Y")
                if renew == "N" and lmdates == reptdatx:
                    matdt = lmdates.strftime("%Y%m%d")
            except ValueError:
                pass

        # ORGDATE
        orgdt_raw = r.get("ORGDT", 0) or 0
        orgdt_str = str(int(orgdt_raw)).zfill(9)[:6]
        try:
            orgdate_val = int(datetime.strptime(orgdt_str, "%m%d%y").strftime("%Y%m%d"))
        except ValueError:
            orgdate_val = None

        # MATDATE
        try:
            matdate_val = int(datetime.strptime(matdt[:8], "%Y%m%d").strftime("%Y%m%d")) if matdt else None
        except ValueError:
            matdate_val = None

        records.append({
            "REPTDATE" : reptdate_int,
            "BRANCH"   : r.get("BRANCH"),
            "NAME"     : r.get("NAME"),
            "ACCTNO"   : r.get("ACCTNO"),
            "CDNO"     : r.get("CDNO"),
            "RATE"     : r.get("RATE"),
            "ACCTTYPE" : r.get("ACCTTYPE"),
            "TERM"     : r.get("TERM"),
            "ORGDATE"  : orgdate_val,
            "MATDATE"  : matdate_val,
            "INTPLAN"  : r.get("INTPLAN"),
            "RENEW"    : renew,
            "MATID"    : r.get("MATID"),
        })

    fd4001 = pl.DataFrame(records) if records else pl.DataFrame()

    # Persist intermediates
    fd4010.write_parquet(FD4010_FILE)
    fd4001.write_parquet(FD4001_FILE)


# ============================================================================
# STEP 3 – MERGE FD4010 transactions into FD4001 account details
# ============================================================================

def merge_fd4010_into_fd4001() -> pl.DataFrame:
    """
    Left-join FD4010 onto FD4001 keyed by ACCTNO / CDNO.
    Only rows that have a matching FD4010 record are kept (IN=A logic).
    """
    con = duckdb.connect()
    merged = con.execute(f"""
        SELECT b.*, a.NAMETRAN, a.OPENIND, a.TRANCD, a.TRANAMT,
               a.TRANDTE, a.PENALTY
        FROM   read_parquet('{FD4010_FILE}')  AS a
        INNER JOIN read_parquet('{FD4001_FILE}') AS b
               ON  a.ACCTNO = b.ACCTNO
               AND a.CDNO   = b.CDNO
        ORDER BY a.ACCTNO, a.CDNO
    """).pl()
    con.close()
    return merged


# ============================================================================
# STEP 4 – READ BRANCH FILE
# ============================================================================

def read_branch_file() -> pl.DataFrame:
    """
    Read the fixed-width branch reference file.
      col 2-4  : BRANCH (numeric 3 digits)
      col 6-8  : BRHCODE ($3)
    """
    rows = []
    with open(BRHFILE, "r") as fh:
        for line in fh:
            if len(line) < 8:
                continue
            try:
                branch  = int(line[1:4].strip())
            except ValueError:
                continue
            brhcode = line[5:8]
            rows.append({"BRANCH": branch, "BRHCODE": brhcode})
    return pl.DataFrame(rows)


# ============================================================================
# STEP 5 – BUILD WITHDRAW & PLACEMNT DATASETS
# ============================================================================

def build_withdraw_placemnt(fd4001_merged: pl.DataFrame,
                             brhdata: pl.DataFrame
                             ) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Join branch data, then split by TRANCD into WITHDRAW and PLACEMNT.

    WITHDRAW – TRANCD in (970, 971, 972):  accumulate AMT1/AMT2/AMT3/TOT1 per ACCTNO/CDNO
    PLACEMNT – TRANCD in (770, 297)
    """
    # Merge branch data
    df = fd4001_merged.join(brhdata, on="BRANCH", how="left")

    # ── PLACEMNT ───────────────────────────────────────────────────────────
    placemnt = (
        df
        .filter(pl.col("TRANCD").is_in([770, 297]))
        .with_columns([
            pl.lit("D").alias("TRANTYPE"),
            pl.col("TERMALL").alias("TERMALL"),
            pl.col("TERM").alias("TERM2"),
            pl.col("TRANAMT").round(2).alias("BALANCE2"),
        ])
        .select([
            "REPTDATE", "BRCH", "BRHCODE", "ACCTNO", "NAME",
            "TRANTYPE", "TERM2", "BALANCE2", "INTPLAN", "ACCTTYPE", "TERMALL",
        ])
    )
    # TERMALL from TERM for placements
    if "TERMALL" not in placemnt.columns:
        placemnt = placemnt.with_columns(pl.col("TERM2").alias("TERMALL"))

    # ── WITHDRAW ───────────────────────────────────────────────────────────
    w_raw = df.filter(pl.col("TRANCD").is_in([970, 971, 972]))

    # Accumulate AMT1 (972 TRANAMT), AMT2 (970+971 TRANAMT), AMT3 (972 PENALTY), TOT1
    rows_w = []
    for (acctno, cdno), grp in w_raw.group_by(["ACCTNO", "CDNO"]):
        grp_dicts = grp.to_dicts()
        amt1 = amt2 = amt3 = tot1 = 0.0
        meta = grp_dicts[0]  # branch / name / etc. taken from first row

        for r in grp_dicts:
            tc  = r["TRANCD"]
            amt = r["TRANAMT"] or 0.0
            pen = r["PENALTY"] or 0.0
            if tc == 972:
                amt1 += amt
                amt3 += pen
                tot1  = round(tot1 + amt, 2)
            if tc in (970, 971):
                amt2  = round(amt2 + amt, 2)
            if tc == 971:
                tot1 += amt

        balance = round((amt1 + amt2) - amt3, 2)

        rows_w.append({
            "REPTDATE"  : meta.get("REPTDATE"),
            "BRCH"      : meta.get("BRANCH"),
            "BRHCODE"   : meta.get("BRHCODE"),
            "ACCTNO"    : acctno,
            "CDNO"      : cdno,
            "NAME"      : meta.get("NAME"),
            "TRANTYPE"  : "W",
            "TERM"      : meta.get("TERM"),
            "TERMALL"   : meta.get("TERM"),
            "BALANCE"   : balance,
            "AMT1"      : amt1,
            "AMT2"      : amt2,
            "AMT3"      : amt3,
            "TOT1"      : tot1,
            "INTPLAN"   : meta.get("INTPLAN"),
            "ACCTTYPE"  : meta.get("ACCTTYPE"),
            "RATE"      : meta.get("RATE"),
            "ORGDATE"   : meta.get("ORGDATE"),
            "MATDATE"   : meta.get("MATDATE"),
        })

    withdraw = pl.DataFrame(rows_w) if rows_w else pl.DataFrame()
    return withdraw, placemnt


# ============================================================================
# STEP 6 – RENEWAL MATCHING LOGIC
# ============================================================================

def match_renewals(withdraw: pl.DataFrame,
                   placemnt: pl.DataFrame
                   ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    SAS logic:
      Sort WITHDRAW by ACCTNO, BAL(=TOT1), TERMALL and assign sequence N.
      Sort PLACEMNT by ACCTNO, BAL(=BALANCE2), TERMALL and assign sequence N.
      Merge on ACCTNO, BAL, TERMALL, N:
        - W only → WITHDRAW
        - P only → PLACEMNT
        - Both   → RENEWAL1

      Then repeat with WITHDRAW by AMT2=BAL for second-pass renewal matching.
    """

    def assign_n(df: pl.DataFrame, bal_col: str) -> pl.DataFrame:
        """Add sequence N within (ACCTNO, BAL, TERMALL) groups."""
        return (
            df
            .sort(["ACCTNO", bal_col, "TERMALL"])
            .with_columns(
                pl.int_range(pl.len()).over(["ACCTNO", bal_col, "TERMALL"]).alias("N")
            )
        )

    # First-pass sort / N
    w1 = assign_n(withdraw.rename({"TOT1": "BAL"}), "BAL")
    p1 = assign_n(placemnt.rename({"BALANCE2": "BAL"}), "BAL")

    merge_key = ["ACCTNO", "BAL", "TERMALL", "N"]

    joined1 = w1.join(p1, on=merge_key, how="full", suffix="_P")

    renewal1  = joined1.filter(pl.col("BRCH").is_not_null() & pl.col("BRCH_P").is_not_null())
    w_remain  = joined1.filter(pl.col("BRCH_P").is_null()).drop([c for c in joined1.columns if c.endswith("_P")])
    p_remain  = joined1.filter(pl.col("BRCH").is_null()).drop([c for c in joined1.columns if not c.endswith("_P") and c not in merge_key])

    # Second-pass: re-sort WITHDRAW by AMT2=BAL
    if w_remain.height > 0 and "AMT2" in w_remain.columns:
        w2 = assign_n(w_remain.rename({"AMT2": "BAL2"}), "BAL2") if "AMT2" in w_remain.columns else w_remain
    else:
        w2 = w_remain

    # For simplicity in second pass, re-merge similarly
    if p_remain.height > 0 and w2.height > 0:
        p_remain2 = p_remain.rename({c: c.rstrip("_P") if c.endswith("_P") else c for c in p_remain.columns})
        joined2   = w2.join(p_remain2, on=merge_key, how="full", suffix="_P2")
        renewal2  = joined2.filter(pl.col("BRCH").is_not_null() & pl.col("BRCH_P2").is_not_null())
        w_final   = joined2.filter(pl.col("BRCH_P2").is_null()).drop([c for c in joined2.columns if c.endswith("_P2")])
        p_final   = joined2.filter(pl.col("BRCH").is_null()).drop([c for c in joined2.columns if not c.endswith("_P2") and c not in merge_key])
    else:
        renewal2 = pl.DataFrame()
        w_final  = w2
        p_final  = p_remain

    return w_final, p_final, renewal1, renewal2


# ============================================================================
# STEP 7 – FILTER RM100K ACCOUNTS & FINAL MERGE
# ============================================================================

def filter_100k(withdraw: pl.DataFrame,
                placemnt: pl.DataFrame
                ) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Keep only accounts where total BALANCE1 (withdrawal) or BALANCE2 (placement) >= 100,000."""

    # Identify withdrawal accounts >= 100k
    if withdraw.height > 0 and "BALANCE1" in withdraw.columns:
        bal_col_w = "BALANCE1"
    elif withdraw.height > 0 and "BAL" in withdraw.columns:
        bal_col_w = "BAL"
    else:
        bal_col_w = None

    if bal_col_w:
        w_totals = (
            withdraw
            .group_by("ACCTNO")
            .agg(pl.col(bal_col_w).sum().alias("TOTAL"))
            .filter(pl.col("TOTAL") >= 100_000)
            .select("ACCTNO")
        )
        mergefdw = withdraw.join(w_totals, on="ACCTNO", how="inner").with_columns(pl.lit("W").alias("TRANTYPE"))
    else:
        mergefdw = pl.DataFrame()

    # Identify placement accounts >= 100k
    if placemnt.height > 0 and "BALANCE2" in placemnt.columns:
        bal_col_p = "BALANCE2"
    elif placemnt.height > 0 and "BAL" in placemnt.columns:
        bal_col_p = "BAL"
    else:
        bal_col_p = None

    if bal_col_p:
        p_totals = (
            placemnt
            .group_by("ACCTNO")
            .agg(pl.col(bal_col_p).sum().alias("TOTAL"))
            .filter(pl.col("TOTAL") >= 100_000)
            .select("ACCTNO")
        )
        mergefdp = placemnt.join(p_totals, on="ACCTNO", how="inner").with_columns(pl.lit("D").alias("TRANTYPE"))
    else:
        mergefdp = pl.DataFrame()

    return mergefdw, mergefdp


# ============================================================================
# STEP 8 – COMBINE & CLASSIFY PLAN
# ============================================================================

def build_both(mergefdw: pl.DataFrame, mergefdp: pl.DataFrame) -> pl.DataFrame:
    """
    Stack withdrawal and placement records, apply FDDENOM format to classify
    PLAN (1=AL-MUDHARABAH/Islamic, 2=CONVENTIONAL).
    Negate BALANCE1 for withdrawals.
    """
    # Normalise columns so both frames can be stacked
    w_sel = []
    p_sel = []

    common = ["REPTDATE", "BRCH", "BRHCODE", "ACCTNO", "NAME",
              "TRANTYPE", "INTPLAN", "ACCTTYPE"]

    for col in common:
        if col not in mergefdw.columns:
            mergefdw = mergefdw.with_columns(pl.lit(None).alias(col))
        if col not in mergefdp.columns:
            mergefdp = mergefdp.with_columns(pl.lit(None).alias(col))

    # TERM – withdrawal uses TERM; placement uses TERM2
    if "TERM" not in mergefdw.columns and "TERMALL" in mergefdw.columns:
        mergefdw = mergefdw.with_columns(pl.col("TERMALL").alias("TERM"))
    if "TERM" not in mergefdp.columns and "TERM2" in mergefdp.columns:
        mergefdp = mergefdp.rename({"TERM2": "TERM"})

    # BALANCE1 / BALANCE2
    if "BALANCE1" not in mergefdw.columns and "BAL" in mergefdw.columns:
        mergefdw = mergefdw.rename({"BAL": "BALANCE1"})
    if "BALANCE2" not in mergefdp.columns and "BAL" in mergefdp.columns:
        mergefdp = mergefdp.rename({"BAL": "BALANCE2"})

    for col in ["BALANCE1", "BALANCE2", "TERM"]:
        if col not in mergefdw.columns:
            mergefdw = mergefdw.with_columns(pl.lit(None).cast(pl.Float64).alias(col))
        if col not in mergefdp.columns:
            mergefdp = mergefdp.with_columns(pl.lit(None).cast(pl.Float64).alias(col))

    keep_cols = common + ["TERM", "BALANCE1", "BALANCE2"]
    w_df = mergefdw.select([c for c in keep_cols if c in mergefdw.columns])
    p_df = mergefdp.select([c for c in keep_cols if c in mergefdp.columns])

    both = pl.concat([w_df, p_df], how="diagonal")

    # Apply FDDENOM format → PLAN
    def get_plan(intplan: Optional[int]) -> int:
        fdi = FDDenomFormat.format(intplan)
        return 1 if fdi == "I" else 2

    both = both.with_columns([
        pl.col("INTPLAN").map_elements(get_plan, return_dtype=pl.Int32).alias("PLAN"),
        pl.when(pl.col("TRANTYPE") == "W")
          .then(-pl.col("BALANCE1"))
          .otherwise(pl.col("BALANCE1"))
          .alias("BALANCE1"),
        (pl.col("BRCH").cast(pl.Utf8).fill_null("") + "/" +
         pl.col("BRHCODE").fill_null("")).alias("BRANCH"),
    ])

    both = both.sort(["BRCH", "ACCTNO", "TERM"])
    return both


# ============================================================================
# STEP 9 – REPORT PRINTING
# ============================================================================

PLANFMT = {1: "AL-MUDHARABAH", 2: "CONVENTIONAL"}

ACCTTYPE_EXCL_PLAN1 = {396, 394, 393}
ACCTTYPE_EXCL_PLAN2 = {395, 394, 393}


def fmt_negparen(value: Optional[float], width: int = 18, dec: int = 2) -> str:
    """Format number: parentheses for negative, right-aligned."""
    if value is None:
        return " " * width
    if value < 0:
        inner = f"{abs(value):,.{dec}f}"
        s = f"({inner})"
    else:
        s = f"{value:,.{dec}f}"
    return s.rjust(width)


def fmt_comma(value: Optional[float], width: int = 18, dec: int = 2) -> str:
    """Format number with comma, right-aligned."""
    if value is None:
        return " " * width
    s = f"{value:,.{dec}f}"
    return s.rjust(width)


def write_report(both: pl.DataFrame, rdate: str) -> None:
    """
    Write two PROC PRINT blocks (PLAN=1 then PLAN=2) to the report file
    with ASA carriage-control characters, page breaks, and sum lines.
    """
    lines: list[str] = []
    page_no = [1]

    def header_lines(plan_val: int, rdate: str) -> list[str]:
        plan_name = PLANFMT.get(plan_val, "")
        return [
            f"{ASA_NEWPAGE}REPORT ID : EIBDFD02",
            f"{ASA_NEWLINE}SALES ADMINISTRATION & SUPPORT",
            f"{ASA_NEWLINE}PUBLIC BANK BERHAD",
            f"{ASA_NEWLINE}DAILY FD MOVEMENT - PLACEMENT/WITHDRAWAL OF RM100K & ABOVE",
            f"{ASA_NEWLINE}DATE : {rdate}",
            f"{ASA_NEWLINE}",
            f"{ASA_NEWLINE}{'BRANCH':<20} {'ACCOUNT NO':>12}  {'NAME':<15} {'TERM':>4}  {'(WITHDRAWAL)':>18}  {'PLACEMENT':>18}",
            f"{ASA_NEWLINE}{'-'*100}",
        ]

    def sum_line(b1_sum: float, b2_sum: float) -> str:
        return (
            f"{ASA_DOUBLESKIP}{'':20} {'':12}  {'SUM':15} {'':4}  "
            f"{fmt_negparen(b1_sum):>18}  {fmt_comma(b2_sum):>18}"
        )

    for plan_val, excl_set in [(1, ACCTTYPE_EXCL_PLAN1), (2, ACCTTYPE_EXCL_PLAN2)]:
        subset = both.filter(
            (pl.col("PLAN") == plan_val) &
            (~pl.col("ACCTTYPE").cast(pl.Int64).is_in(list(excl_set)))
        ).sort(["PLAN", "BRCH", "ACCTNO"])

        if subset.height == 0:
            continue

        lines.extend(header_lines(plan_val, rdate))
        line_count = 8
        b1_total = 0.0
        b2_total = 0.0
        prev_key  = None

        for row in subset.to_dicts():
            brch    = str(row.get("BRCH", "") or "")
            brhcode = str(row.get("BRHCODE", "") or "")
            branch  = f"{brch}/{brhcode}".strip("/")
            acctno  = str(row.get("ACCTNO", "") or "")
            name    = str(row.get("NAME", "") or "")[:15]
            term    = row.get("TERM")
            b1      = row.get("BALANCE1") or 0.0
            b2      = row.get("BALANCE2") or 0.0
            b1_total += b1
            b2_total += b2

            curr_key = (brch, acctno)
            if curr_key != prev_key:
                id_str = f"{branch:<20} {acctno:>12}"
            else:
                id_str = f"{'':20} {'':12}"

            detail = (
                f"{ASA_NEWLINE}{id_str}  {name:<15} {str(term or ''):>4}  "
                f"{fmt_negparen(b1):>18}  {fmt_comma(b2):>18}"
            )
            lines.append(detail)
            line_count += 1
            prev_key = curr_key

            # Page break if needed
            if line_count >= PAGE_LINES:
                lines.extend(header_lines(plan_val, rdate))
                line_count = 8

        # Grand sum line
        lines.append(sum_line(b1_total, b2_total))

    with open(REPORT_FILE, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
        fh.write("\n")

    print(f"Report written to: {REPORT_FILE}")


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    print("EIBDFD02 – Daily FD Movement starting...")

    # Step 1: derive dates
    ctx = derive_report_date()
    print(f"  Report date : {ctx['rdate']}")

    # Step 2: parse raw file → FD4010 + FD4001
    build_fd4010_fd4001(ctx)

    # Step 3: merge FD4010 into FD4001
    fd4001_merged = merge_fd4010_into_fd4001()

    # Step 4: branch reference
    brhdata = read_branch_file()

    # Step 5: build WITHDRAW & PLACEMNT
    withdraw, placemnt = build_withdraw_placemnt(fd4001_merged, brhdata)

    # Persist MIS outputs
    if withdraw.height > 0:
        withdraw.write_parquet(WITHDRAW_FILE)
    if placemnt.height > 0:
        placemnt.write_parquet(PLACEMNT_FILE)

    # Step 6: renewal matching
    w_final, p_final, _renewal1, _renewal2 = match_renewals(withdraw, placemnt)

    # Step 7: filter RM100k
    mergefdw, mergefdp = filter_100k(w_final, p_final)

    # Step 8: combine & classify
    both = build_both(mergefdw, mergefdp)

    # Step 9: report
    write_report(both, ctx["rdate"])

    print("EIBDFD02 – Done.")


if __name__ == "__main__":
    main()
