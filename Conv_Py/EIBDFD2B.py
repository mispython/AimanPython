#!/usr/bin/env python3
"""
Program : EIBDFD2B
Function: Summary of Daily FD Movement - Placement/Withdrawal of RM1M & Above per Account
          Produces four output files:
            SUMFILE  – EIBDFD1MS  Conventional summary (CSV-delimited)
            RPTFILE  – EIBDFD1MA  Conventional detail  (CSV-delimited)
            RPTFILI  – Islamic detail                  (CSV-delimited)
            ISUMFILE – Islamic summary                 (CSV-delimited)
"""

import duckdb
import polars as pl
import os
from datetime import datetime, timedelta
from typing import Optional

# Import format definitions from PBBDPFMT
from PBBDPFMT import FDDenomFormat

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR      = os.environ.get("BASE_DIR", "/data")
MIS_DIR       = os.path.join(BASE_DIR, "mis")
CRM_DIR       = os.path.join(BASE_DIR, "crm")
MNITB_DIR     = os.path.join(BASE_DIR, "mnitb")
OUTPUT_DIR    = os.path.join(BASE_DIR, "output")

# Input parquet files (produced by EIBDFD02 / MIS library)
WITHDRAW_FILE = os.path.join(MIS_DIR,  "WITHDRAW.parquet")
PLACEMNT_FILE = os.path.join(MIS_DIR,  "PLACEMNT.parquet")
# CRM source: CRM.CISR1FD<REPTDT6>  – path derived at runtime
# MNITB FD source
MNITB_FD_FILE = os.path.join(MNITB_DIR, "FD.parquet")

# Output files
SUMFILE       = os.path.join(OUTPUT_DIR, "EIBDFD1MS.txt")   # Conventional summary
RPTFILE       = os.path.join(OUTPUT_DIR, "EIBDFD1MA.txt")   # Conventional detail
RPTFILI       = os.path.join(OUTPUT_DIR, "EIBDFD1MI.txt")   # Islamic detail
ISUMFILE      = os.path.join(OUTPUT_DIR, "EIBDFD1MIS.txt")  # Islamic summary

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================================
# HELPERS
# ============================================================================

MISSING_NUM = 0      # OPTIONS MISSING=0


def fmt_negparen(value: Optional[float], width: int = 10, dec: int = 2) -> str:
    """Format number: parentheses for negative, right-aligned in given width."""
    if value is None:
        return " " * width
    if value < 0:
        s = f"({abs(value):,.{dec}f})"
    else:
        s = f"{value:,.{dec}f}"
    return s.rjust(width)


def fmt_negparen18(value: Optional[float]) -> str:
    return fmt_negparen(value, width=18, dec=2)


def compbl(s: Optional[str]) -> str:
    """Compress multiple blanks to single blank (SAS COMPBL)."""
    if s is None:
        return ""
    import re
    return re.sub(r" {2,}", " ", s).strip()


def build_planx(cols: list[str]) -> str:
    """
    Build PLANX string from up to 5 COL values (non-blank),
    joining with ' , ' and final ' & ' before the last entry.
    """
    vals = [c for c in cols if c and c.strip() not in ("", "  ")]
    if not vals:
        return ""
    if len(vals) == 1:
        return vals[0]
    return " , ".join(vals[:-1]) + " & " + vals[-1]


def build_ratex(cols: list[str]) -> str:
    """Same join logic as PLANX but for rate columns."""
    return build_planx(cols)


# ============================================================================
# STEP 1 – DERIVE REPORT DATE  (TODAY() - 1)
# ============================================================================

def derive_report_date() -> dict:
    reptdate = datetime.today() - timedelta(days=1)
    return {
        "reptdate" : reptdate,
        "reptyear" : reptdate.strftime("%Y"),
        "rdate"    : reptdate.strftime("%d/%m/%Y"),
        "reptdt6"  : reptdate.strftime("%y%m%d"),   # YYMMDDN6
    }


# ============================================================================
# STEP 2 – LOAD WITHDRAW & PLACEMNT
# ============================================================================

def load_mis() -> tuple[pl.DataFrame, pl.DataFrame]:
    con = duckdb.connect()
    withdraw = con.execute(f"SELECT * FROM read_parquet('{WITHDRAW_FILE}')").pl()
    placemnt = con.execute(f"SELECT * FROM read_parquet('{PLACEMNT_FILE}')").pl()
    con.close()
    return withdraw, placemnt


# ============================================================================
# STEP 3 – BUILD NETV (net movement per account >= RM1M absolute)
# ============================================================================

def build_netv(withdraw: pl.DataFrame, placemnt: pl.DataFrame) -> pl.DataFrame:
    """
    Summarise TRANAMT by ACCTNO for each dataset, merge, compute NETAMT.
    Keep only accounts where ABS(NETAMT) >= 1,000,000.
    """
    def sum_by_acctno(df: pl.DataFrame, alias: str) -> pl.DataFrame:
        if df.height == 0 or "TRANAMT" not in df.columns:
            return pl.DataFrame({"ACCTNO": pl.Series([], dtype=pl.Int64),
                                 alias:    pl.Series([], dtype=pl.Float64)})
        return (
            df.group_by("ACCTNO")
              .agg(pl.col("TRANAMT").sum().alias(alias))
        )

    wd = sum_by_acctno(withdraw, "DAMT")
    pd_ = sum_by_acctno(placemnt, "PAMT")

    con = duckdb.connect()
    netv = con.execute("""
        SELECT COALESCE(w.ACCTNO, p.ACCTNO) AS ACCTNO,
               COALESCE(p.PAMT, 0.0)        AS PAMT,
               COALESCE(w.DAMT, 0.0)        AS DAMT,
               COALESCE(p.PAMT, 0.0) - COALESCE(w.DAMT, 0.0) AS NETAMT
        FROM   wd w
        FULL OUTER JOIN pd_ p ON w.ACCTNO = p.ACCTNO
        WHERE  ABS(COALESCE(p.PAMT,0) - COALESCE(w.DAMT,0)) >= 1000000.0
    """).pl()
    con.close()

    return netv.select("ACCTNO")


# ============================================================================
# STEP 4 – BUILD FD (combined withdraw + placemnt with FDDENOM classification)
# ============================================================================

def build_fd(withdraw: pl.DataFrame, placemnt: pl.DataFrame) -> pl.DataFrame:
    """
    Stack WITHDRAW and PLACEMNT, build BRANCH string, apply FDDENOM format.
    """
    fd = pl.concat([withdraw, placemnt], how="diagonal")

    def fdi_map(intplan) -> str:
        return FDDenomFormat.format(int(intplan) if intplan is not None else None)

    fd = fd.with_columns([
        (pl.col("BRCH").cast(pl.Utf8).fill_null("") + "/" +
         pl.col("BRHCODE").fill_null("")).alias("BRANCH"),
        pl.col("INTPLAN").map_elements(fdi_map, return_dtype=pl.Utf8).alias("FDI"),
    ])

    return fd.sort("ACCTNO")


# ============================================================================
# STEP 5 – MERGE NETV → BOTH (keep only RM1M accounts)
# ============================================================================

def build_both(netv: pl.DataFrame, fd: pl.DataFrame) -> pl.DataFrame:
    return fd.join(netv, on="ACCTNO", how="inner")


# ============================================================================
# STEP 6 – LOAD CRM DATA
# ============================================================================

def load_crm(reptdt6: str) -> pl.DataFrame:
    crm_file = os.path.join(CRM_DIR, f"CISR1FD{reptdt6}.parquet")
    if not os.path.exists(crm_file):
        # Return empty frame with expected columns if file not found
        return pl.DataFrame({
            "ACCTNO":   pl.Series([], dtype=pl.Int64),
            "CUSTNAME": pl.Series([], dtype=pl.Utf8),
            "CUSTNO":   pl.Series([], dtype=pl.Utf8),
            "MNIADDL1": pl.Series([], dtype=pl.Utf8),
            "MNIADDL2": pl.Series([], dtype=pl.Utf8),
        })

    con = duckdb.connect()
    crm = con.execute(f"""
        SELECT ACCTNO, CUSTNAME, CUSTNO, MNIADDL1, MNIADDL2
        FROM   read_parquet('{crm_file}')
        WHERE  SECCUST = '901'
    """).pl()
    con.close()

    # NODUPKEY by ACCTNO
    return crm.unique(subset=["ACCTNO"], keep="first").sort("ACCTNO")


# ============================================================================
# STEP 7 – MERGE CRM INTO BOTH
# ============================================================================

def enrich_both(both: pl.DataFrame, crm: pl.DataFrame) -> pl.DataFrame:
    """
    Left-join CRM onto BOTH by ACCTNO (keep all BOTH rows).
    Apply CUSTNAME fallback, derive TERMX and RATEALL.
    """
    both = both.join(crm, on="ACCTNO", how="left")

    # CUSTNAME fallback: use NAME if CUSTNAME is blank
    if "CUSTNAME" not in both.columns:
        both = both.with_columns(pl.lit(None).cast(pl.Utf8).alias("CUSTNAME"))

    both = both.with_columns([
        pl.when(pl.col("CUSTNAME").is_null() | (pl.col("CUSTNAME").str.strip_chars() == ""))
          .then(pl.col("NAME") if "NAME" in both.columns else pl.lit(""))
          .otherwise(pl.col("CUSTNAME"))
          .alias("CUSTNAME"),
        # TERMX = COMPRESS(TERMALL || MATID)  → strip internal spaces
        (pl.col("TERMALL").cast(pl.Utf8).fill_null("") +
         pl.col("MATID").cast(pl.Utf8).fill_null(""))
          .str.replace_all(r"\s+", "")
          .alias("TERMX"),
        pl.col("RATE").alias("RATEALL"),
    ])

    return both


# ============================================================================
# STEP 8 – SPLIT INTO BOTHI (Islamic) AND BOTH (Conventional)
#          Apply TRANTYPE-based BALANCE1 / BALANCE2 derivation
# ============================================================================

def split_islamic_conv(both: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Derive BALANCE1 (withdrawal, negated) and BALANCE2 (placement) from TRANTYPE.
    Split by FDI into Islamic (BOTHI) and Conventional (BOTH).
    Filter ACCTTYPE exclusions per plan.
    """
    both = both.with_columns([
        pl.when(pl.col("TRANTYPE") == "D")
          .then(pl.col("TRANAMT") if "TRANAMT" in both.columns else pl.lit(0.0))
          .otherwise(pl.lit(None))
          .alias("BALANCE2"),
        pl.when(pl.col("TRANTYPE") == "W")
          .then(-(pl.col("TRANAMT") if "TRANAMT" in both.columns else pl.lit(0.0)))
          .otherwise(pl.lit(None))
          .alias("BALANCE1"),
    ])

    bothi = both.filter(pl.col("FDI") == "I")
    bothc = both.filter(pl.col("FDI") != "I")

    # Conventional: exclude ACCTTYPE in (394, 395)
    if "ACCTTYPE" in bothc.columns:
        bothc = bothc.filter(~pl.col("ACCTTYPE").cast(pl.Int64).is_in([394, 395]))

    # Islamic: exclude ACCTTYPE in (394, 396)
    if "ACCTTYPE" in bothi.columns:
        bothi = bothi.filter(~pl.col("ACCTTYPE").cast(pl.Int64).is_in([394, 396]))

    return bothi, bothc


# ============================================================================
# STEP 9 – MERGE MNITB FD (SECOND, PRODUCT)
# ============================================================================

def merge_mnitb(both: pl.DataFrame) -> pl.DataFrame:
    if not os.path.exists(MNITB_FD_FILE):
        both = both.with_columns([
            pl.lit(None).cast(pl.Int64).alias("SECOND"),
            pl.lit(None).cast(pl.Int64).alias("PRODUCT"),
        ])
        return both

    con = duckdb.connect()
    fd_mnitb = con.execute(f"""
        SELECT ACCTNO, SECOND, PRODUCT
        FROM   read_parquet('{MNITB_FD_FILE}')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO ORDER BY ACCTNO) = 1
    """).pl()
    con.close()

    both = both.join(fd_mnitb, on="ACCTNO", how="left")
    both = both.with_columns(
        pl.col("NETBAL") if "NETBAL" in both.columns else
        (pl.col("BALANCE1").fill_null(0) + pl.col("BALANCE2").fill_null(0)).alias("NETBAL")
    )
    return both


# ============================================================================
# STEP 10 – BUILD SUMREC: summarise BOTH for conventional summary report
# ============================================================================

def build_sumrec(both: pl.DataFrame) -> pl.DataFrame:
    """Aggregate BALANCE1, BALANCE2 by BRCH/BRHCODE/ACCTNO/CUSTNAME."""
    grp_cols = ["BRCH", "BRHCODE", "ACCTNO", "CUSTNAME"]
    agg_cols = [c for c in ["BALANCE1", "BALANCE2"] if c in both.columns]

    sumrec = (
        both
        .group_by(grp_cols)
        .agg([pl.col(c).sum() for c in agg_cols])
    )
    sumrec = sumrec.with_columns(
        (pl.col("BALANCE1").fill_null(0) + pl.col("BALANCE2").fill_null(0)).alias("SETX")
    )
    return sumrec


# ============================================================================
# STEP 11 – PIVOT TERMX and RATEALL (up to 5 per account) → PLANX / RATEX
# ============================================================================

def build_planx_col(df: pl.DataFrame, val_col: str, key_col: str = "ACCTNO") -> pl.DataFrame:
    """
    For each ACCTNO collect up to 5 distinct values of val_col and
    build a comma/& joined string PLANX or RATEX.
    """
    result = []
    for acctno, grp in df.group_by(key_col):
        vals = (
            grp.select(val_col)
               .unique()
               .drop_nulls()
               .filter(pl.col(val_col).str.strip_chars() != "")
               [val_col]
               .to_list()[:5]
        )
        joined = build_planx([str(v) for v in vals])
        result.append({key_col: acctno, "PLANX": compbl(joined)})

    if not result:
        return pl.DataFrame({key_col: pl.Series([], dtype=pl.Int64),
                              "PLANX": pl.Series([], dtype=pl.Utf8)})
    return pl.DataFrame(result)


def build_ratex_col(df: pl.DataFrame, val_col: str = "RATEALL",
                    key_col: str = "ACCTNO") -> pl.DataFrame:
    result = []
    for acctno, grp in df.group_by(key_col):
        vals = (
            grp.select(val_col)
               .unique()
               .drop_nulls()
               [val_col]
               .to_list()[:5]
        )
        joined = build_planx([str(v) for v in vals])
        result.append({key_col: acctno, "RATEX": compbl(joined)})

    if not result:
        return pl.DataFrame({key_col: pl.Series([], dtype=pl.Int64),
                             "RATEX": pl.Series([], dtype=pl.Utf8)})
    return pl.DataFrame(result)


# ============================================================================
# STEP 12 – ATTACH PLANX / RATEX TO SUMREC
# ============================================================================

def enrich_sumrec(sumrec: pl.DataFrame, both: pl.DataFrame) -> pl.DataFrame:
    planx_df = build_planx_col(both, "TERMX")
    ratex_df = build_ratex_col(both, "RATEALL")

    sumrec = sumrec.join(planx_df, on="ACCTNO", how="left")
    sumrec = sumrec.join(ratex_df, on="ACCTNO", how="left")

    sumrec = sumrec.with_columns([
        pl.col("PLANX").fill_null("").map_elements(compbl, return_dtype=pl.Utf8),
        pl.col("RATEX").fill_null("").map_elements(compbl, return_dtype=pl.Utf8),
    ])

    # Attach supplementary cols from BOTH (SECOND, MNIADDL1, MNIADDL2, PRODUCT, CUSTNO)
    extra_cols = ["ACCTNO", "SECOND", "MNIADDL1", "MNIADDL2", "PRODUCT", "CUSTNO"]
    extra_cols = [c for c in extra_cols if c in both.columns]
    both1 = both.select(extra_cols).unique(subset=["ACCTNO"], keep="first")
    sumrec = sumrec.join(both1, on="ACCTNO", how="left")

    sumrec = sumrec.sort(["SETX", "BRCH", "ACCTNO"], descending=[True, False, False])
    return sumrec


# ============================================================================
# STEP 13 – WRITE CONVENTIONAL SUMMARY (SUMFILE / EIBDFD1MS)
# ============================================================================

def write_sumfile(sumrec: pl.DataFrame, rdate: str) -> None:
    lines = []
    ba1t = ba2t = nets = 0.0

    lines.append("REPORT ID : EIBDFD1MS")
    lines.append(
        "SUMMARY OF DAILY FD MOVEMENT"
        "                 - PLACEMENT/WITHDRAWAL OF RM 1M & ABOVE PER ACCOUNT "
        "                                               (CONVENTIONAL)"
    )
    lines.append(f"REPORT DATE : {rdate}")
    lines.append(" ")
    lines.append(
        "BRANCH CODE;ACCOUNT NO.;BRANCH ABBR;NAME OF CUSTOMER (FR. CIS LVL);"
        "DAILY NET INC./(DEC.) (RM'MIL);"
        "TERM (D/M);INTEREST RATE (%P.A.);SECONDARY OFFICER NO.;"
        "NAME1 OF CUSTOMER (FR. A/C LVL);NAME2 OF CUSTOMER (FR. A/C LVL);"
        "CUSTOMER CIS NO.;PRODUCT CODE;"
        "PLACEMENT (RM'MIL);WITHDRAWAL (RM'MIL)"
    )

    for row in sumrec.to_dicts():
        b1 = row.get("BALANCE1") or 0.0
        b2 = row.get("BALANCE2") or 0.0
        setx = row.get("SETX") or 0.0
        ba1t += b1
        ba2t += b2
        nets += setx

        sba1 = round(b1   * 0.000001, 2)
        sba2 = round(b2   * 0.000001, 2)
        snet = round(setx * 0.000001, 2)

        brch     = str(row.get("BRCH")     or "").rjust(3)
        acctno   = str(row.get("ACCTNO")   or "").rjust(10)
        brhcode  = str(row.get("BRHCODE")  or "")[:3]
        custname = str(row.get("CUSTNAME") or "")[:50]
        planx    = str(row.get("PLANX")    or "")[:25]
        ratex    = str(row.get("RATEX")    or "")[:25]
        second   = str(row.get("SECOND")   or "").rjust(5)
        mniaddl1 = str(row.get("MNIADDL1") or "")[:50]
        mniaddl2 = str(row.get("MNIADDL2") or "")[:50]
        custno   = str(row.get("CUSTNO")   or "")[:20]
        product  = str(row.get("PRODUCT")  or "").rjust(3)

        line = (
            f"{brch};{acctno};{brhcode};{custname};"
            f"{fmt_negparen(snet, 10, 2)};"
            f"{planx};{ratex};{second};"
            f"{mniaddl1};{mniaddl2};{custno};{product};"
            f"{fmt_negparen(sba2, 10, 2)};"
            f"{fmt_negparen(sba1, 10, 2)}"
        )
        lines.append(line)

    gba1 = round(ba1t * 0.000001, 2)
    gba2 = round(ba2t * 0.000001, 2)
    gnet = round(nets * 0.000001, 2)
    lines.append(
        f"GRAND TOTAL;;;;{fmt_negparen(gnet, 10, 2)};;;;;;;;"
        f"{fmt_negparen(gba2, 10, 2)};{fmt_negparen(gba1, 10, 2)}"
    )

    with open(SUMFILE, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"Written: {SUMFILE}")


# ============================================================================
# STEP 14 – WRITE CONVENTIONAL DETAIL (RPTFILE / EIBDFD1MA)
# ============================================================================

def write_rptfile(both: pl.DataFrame, rdate: str) -> None:
    """
    Detail report sorted by BRCH, ACCTNO with account totals and grand total.
    """
    # Compute TOTNET per ACCTNO (sum of NETBAL)
    if "NETBAL" not in both.columns:
        both = both.with_columns(
            (pl.col("BALANCE1").fill_null(0) + pl.col("BALANCE2").fill_null(0)).alias("NETBAL")
        )

    totnet_df = (
        both.group_by("ACCTNO")
            .agg(pl.col("NETBAL").sum().alias("TOTNET"))
    )
    both = both.join(totnet_df, on="ACCTNO", how="left")
    both = both.sort(["BRCH", "ACCTNO"])

    lines = []
    lines.append("REPORT ID: EIBDFD1MA")
    lines.append(
        "DAILY FD MOVEMENT-PLACEMENT/WITHDRAWAL OF RM 1M "
        "& ABOVE PER ACCOUNT (CONVENTIONAL)"
    )
    lines.append(f"REPORT DATE : {rdate}")
    lines.append(" ")
    lines.append(
        "BRANCH CODE;BRANCH ABBR;ACCOUNT NO.;NAME OF CUSTOMER (FR. CIS LVL);"
        "NAME1 OF CUSTOMER (FR. A/C LVL);NAME2 OF CUSTOMER (FR. A/C LVL);"
        "CUSTOMER CIS NO.;CD NO.;PLACEMENT (RM);WITHDRAWAL (RM);"
        "DAILY NET INC./(DEC.) (RM);TERM (D/M);INTEREST RATE (% P.A.);"
        "PRODUCT CODE;SECONDARY OFFICER NO.;ACCOUNT TOTAL"
    )

    totwg = totpg = 0.0
    prev_acctno = None

    records = both.to_dicts()
    for i, row in enumerate(records):
        b1 = row.get("BALANCE1") or 0.0
        b2 = row.get("BALANCE2") or 0.0
        netbal = row.get("NETBAL") or 0.0
        totnet = row.get("TOTNET") or 0.0

        totwg += b1
        totpg += b2

        brch     = str(row.get("BRCH")     or "").rjust(3)
        brhcode  = str(row.get("BRHCODE")  or "")[:3]
        acctno   = str(row.get("ACCTNO")   or "").rjust(10)
        custname = str(row.get("CUSTNAME") or "")[:50]
        mniaddl1 = str(row.get("MNIADDL1") or "")[:50]
        mniaddl2 = str(row.get("MNIADDL2") or "")[:50]
        custno   = str(row.get("CUSTNO")   or "")[:20]
        cdno     = str(row.get("CDNO")     or "").rjust(10)
        termx    = str(row.get("TERMX")    or "")[:5]
        rate     = row.get("RATE") or 0.0
        product  = str(row.get("PRODUCT")  or "").rjust(3)
        second   = str(row.get("SECOND")   or "").rjust(5)

        line = (
            f"{brch};{brhcode};{acctno};{custname};"
            f"{mniaddl1};{mniaddl2};{custno};"
            f"{cdno};"
            f"{fmt_negparen18(b2)};"
            f"{fmt_negparen18(b1)};"
            f"{fmt_negparen18(netbal)};"
            f"{termx};{rate:.2f};{product};{second};"
            f"{fmt_negparen18(totnet)}"
        )
        lines.append(line)

        # Blank line after last record for each ACCTNO
        is_last_acctno = (i == len(records) - 1) or (records[i + 1].get("ACCTNO") != row.get("ACCTNO"))
        if is_last_acctno:
            lines.append(" ")

    netgd = totpg + totwg
    lines.append(
        f"GRAND TOTAL;;;;;;;;{fmt_negparen18(totpg)};"
        f"{fmt_negparen18(totwg)};{fmt_negparen18(netgd)}"
    )

    with open(RPTFILE, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"Written: {RPTFILE}")


# ============================================================================
# STEP 15 – WRITE ISLAMIC DETAIL (RPTFILI)
# ============================================================================

def write_rptfili(bothi: pl.DataFrame, rdate: str) -> None:
    """
    Islamic detail report: BY BRCH ACCTNO TERMALL.
    Prints account totals, branch totals, and grand total.
    """
    if "TERMALL" not in bothi.columns:
        bothi = bothi.with_columns(pl.col("TERM").alias("TERMALL"))

    bothi = bothi.sort(["BRCH", "ACCTNO", "TERMALL"])

    lines = []
    lines.append(" ")
    lines.append("PUBLIC ISLAMIC BANK BERHAD")
    lines.append("DAILY INVESTMENT MOVEMENT-PLACEMENT/WITHDRAWAL OF RM 1M")
    lines.append(f"& ABOVE (ISLAMIC TERM DEPOSIT) DATE : {rdate}")
    lines.append(" ")

    totwg = totpg = 0.0
    prev_brch = None
    prev_acctno = None
    totwb = totpb = 0.0
    totwd = totpl = 0.0

    records = bothi.to_dicts()
    for i, row in enumerate(records):
        brch    = row.get("BRCH")
        acctno  = row.get("ACCTNO")
        b1      = row.get("BALANCE1") or 0.0
        b2      = row.get("BALANCE2") or 0.0

        # Branch / account breaks
        new_brch   = (brch != prev_brch)
        new_acctno = (acctno != prev_acctno) or new_brch

        if new_brch:
            # *  LINK NEWPAGE;
            totwb = totpb = 0.0

        if new_acctno:
            totwd = totpl = 0.0

        totwd   += b1;  totpl   += b2
        totwb   += b1;  totpb   += b2
        totwg   += b1;  totpg   += b2
        netbal   = totpl + totwd
        netbrh   = totpb + totwb
        netgd    = totpg + totwg

        if new_acctno:
            lines.append(f"BRCH={str(brch or '').rjust(3)} ACCTNO={str(acctno or '').rjust(10)}")
            lines.append(" ")
            lines.append("BRANCH;NAME;TERM;(WITHDRAWAL);PLACEMENT;RATE;NET INC/(DEC)")
            lines.append(" ")

        branch   = str(row.get("BRANCH")   or "")[:7]
        custname = str(row.get("CUSTNAME") or "")[:50]
        termx    = str(row.get("TERMX")    or "")[:3]
        rate     = row.get("RATE") or 0.0

        lines.append(
            f"{branch:<7};{custname:<50};{termx:<3};"
            f"{fmt_negparen18(b1)};{fmt_negparen18(b2)};{rate:.2f}"
        )

        # Determine look-ahead
        next_acctno = records[i + 1].get("ACCTNO") if i + 1 < len(records) else None
        next_brch   = records[i + 1].get("BRCH")   if i + 1 < len(records) else None
        last_record = (i == len(records) - 1)
        last_acctno = last_record or next_acctno != acctno
        last_brch   = last_record or next_brch   != brch

        if last_acctno:
            lines.append(f";;;{'--'*9}--;{'--'*9}--;;{'--'*9}--")
            lines.append(
                f"ACCOUNT TOTAL;;;{fmt_negparen18(totwd)};"
                f"{fmt_negparen18(totpl)};;{fmt_negparen18(netbal)}"
            )
            lines.append("   ")

        if last_brch:
            lines.append(f";;;{'--'*9}--;{'--'*9}--;;{'--'*9}--")
            lines.append(
                f"BRANCH TOTAL;;;{fmt_negparen18(totwb)};"
                f"{fmt_negparen18(totpb)};;{fmt_negparen18(netbrh)}"
            )
            lines.append("   ")

        if last_record:
            lines.append(f";;;{'--'*9}--;{'--'*9}--;;{'--'*9}--")
            lines.append(
                f"GRAND TOTAL;;;{fmt_negparen18(totwg)};"
                f"{fmt_negparen18(totpg)};;{fmt_negparen18(netgd)}"
            )
            lines.append("   ")

        prev_brch   = brch
        prev_acctno = acctno

    with open(RPTFILI, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"Written: {RPTFILI}")


# ============================================================================
# STEP 16 – BUILD ISUMREC AND WRITE ISLAMIC SUMMARY (ISUMFILE)
# ============================================================================

def build_isumrec(bothi: pl.DataFrame) -> pl.DataFrame:
    grp_cols = ["BRCH", "BRHCODE", "ACCTNO", "CUSTNAME"]
    agg_cols = [c for c in ["BALANCE1", "BALANCE2"] if c in bothi.columns]

    isumrec = (
        bothi
        .group_by(grp_cols)
        .agg([pl.col(c).sum() for c in agg_cols])
    )
    isumrec = isumrec.with_columns(
        (pl.col("BALANCE1").fill_null(0) + pl.col("BALANCE2").fill_null(0)).alias("SETX")
    )

    # PLANX from TERMX per account
    planx_df = build_planx_col(bothi, "TERMX")
    isumrec = isumrec.join(planx_df, on="ACCTNO", how="left")
    isumrec = isumrec.with_columns(
        pl.col("PLANX").fill_null("").map_elements(compbl, return_dtype=pl.Utf8)
    )

    # Attach RATE (highest rate per account) and ACCTTYPE
    if "RATE" in bothi.columns:
        rate_df = (
            bothi.sort("RATE", descending=True)
                 .unique(subset=["ACCTNO"], keep="first")
                 .select(["ACCTNO", "RATE", "ACCTTYPE"])
        )
        isumrec = isumrec.join(rate_df, on="ACCTNO", how="left")

    isumrec = isumrec.sort(["SETX", "BRCH", "ACCTNO"], descending=[True, False, False])
    return isumrec


def write_isumfile(isumrec: pl.DataFrame, rdate: str) -> None:
    lines = []
    ba1t = ba2t = nets = 0.0

    lines.append(f"SUMMARY OF DAILY INVESTMENT MOVEMENT               - PLACEMENT/WITHDRAWAL OF RM 1M & ABOVE")
    lines.append(f"(ISLAMIC TERM DEPOSIT) REPORT DATE : {rdate}")
    lines.append(" ")
    lines.append(";;;;WITHDRAWAL;PLACEMENT;NET INC/(DEC)")
    lines.append("BRANCH;BRHCODE;NAME;ACCTNO;(RM'MIL);(RM'MIL);(RM'MIL);TERM;RATE;PRODTYPE")
    lines.append(" ")

    for row in isumrec.to_dicts():
        b1   = row.get("BALANCE1") or 0.0
        b2   = row.get("BALANCE2") or 0.0
        setx = row.get("SETX")     or 0.0
        ba1t += b1;  ba2t += b2;  nets += setx

        sba1 = round(b1   * 0.000001, 2)
        sba2 = round(b2   * 0.000001, 2)
        snet = round(setx * 0.000001, 2)

        brch     = str(row.get("BRCH")     or "").rjust(3)
        brhcode  = str(row.get("BRHCODE")  or "")[:3]
        custname = str(row.get("CUSTNAME") or "")[:50]
        acctno   = str(row.get("ACCTNO")   or "").rjust(10)
        planx    = str(row.get("PLANX")    or "")[:20]
        rate     = row.get("RATE") or 0.0
        accttype = str(row.get("ACCTTYPE") or "").rjust(8)

        line = (
            f"{brch};{brhcode};{custname};{acctno};"
            f"{fmt_negparen(sba1, 10, 2)};"
            f"{fmt_negparen(sba2, 10, 2)};"
            f"{fmt_negparen(snet, 10, 2)};"
            f"{planx};{rate:.2f};{accttype}"
        )
        lines.append(line)

    gba1 = round(ba1t * 0.000001, 2)
    gba2 = round(ba2t * 0.000001, 2)
    gnet = round(nets * 0.000001, 2)
    lines.append("-" * 165)
    lines.append(
        f";;;;{fmt_negparen(gba1, 10, 2)};"
        f"{fmt_negparen(gba2, 10, 2)};"
        f"{fmt_negparen(gnet, 10, 2)}"
    )

    with open(ISUMFILE, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"Written: {ISUMFILE}")


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    print("EIBDFD2B – Summary Daily FD Movement (RM1M & Above) starting...")

    # Step 1: dates
    ctx = derive_report_date()
    print(f"  Report date : {ctx['rdate']}")

    # Step 2: load MIS data
    withdraw, placemnt = load_mis()

    # Step 3: net movement filter >= RM1M
    netv = build_netv(withdraw, placemnt)

    # Step 4: combined FD dataset with FDDENOM classification
    fd = build_fd(withdraw, placemnt)

    # Step 5: filter to RM1M accounts only
    both = build_both(netv, fd)

    # Step 6 & 7: enrich with CRM data
    crm  = load_crm(ctx["reptdt6"])
    both = enrich_both(both, crm)

    # Step 8: split Islamic / Conventional and derive BALANCE1 / BALANCE2
    bothi, bothc = split_islamic_conv(both)

    # Step 9: merge MNITB FD product data into conventional
    bothc = merge_mnitb(bothc)

    # ── Conventional reports ─────────────────────────────────────────────

    # SUMREC: summarise for conventional summary report
    sumrec = build_sumrec(bothc)
    sumrec = enrich_sumrec(sumrec, bothc)

    # SUMFILE (EIBDFD1MS)
    write_sumfile(sumrec, ctx["rdate"])

    # RPTFILE (EIBDFD1MA)
    write_rptfile(bothc, ctx["rdate"])

    # ── Islamic reports ──────────────────────────────────────────────────

    # RPTFILI
    write_rptfili(bothi, ctx["rdate"])

    # ISUMREC + ISUMFILE
    isumrec = build_isumrec(bothi)
    write_isumfile(isumrec, ctx["rdate"])

    print("EIBDFD2B – Done.")


if __name__ == "__main__":
    main()
