#!/usr/bin/env python3
"""
Program  : LALMPBBP.py
Purpose  : REPORT ON DOMESTIC ASSETS AND LIABILITIES - PART II
           Processes loan, deposit, and OD data to generate BNM ALM codes
           covering NPL, gross loans, Islamic loans, disbursement/repayment,
           sector, purpose, collateral, maturity and repricing breakdowns.
"""

# ============================================================================
# DEPENDENCIES  (%INC PGM(PBBLNFMT) equivalent)
# ============================================================================
from PBBLNFMT import (
    format_apprlimt,
    format_loansize,
    format_newsect,
    format_validse,
    format_odcustcd,
    format_lnrate,
)

# ============================================================================
# STANDARD LIBRARY / THIRD-PARTY IMPORTS
# ============================================================================
import os
from datetime import datetime

import duckdb
import polars as pl

# ============================================================================
# PATH & MACRO-VARIABLE CONFIGURATION
# ============================================================================
INPUT_DIR  = os.environ.get("INPUT_DIR",  "input")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "output")

# SAS macro variable equivalents
REPTMON  = os.environ.get("REPTMON",  "202412")   # e.g. 202412
REPTMON2 = os.environ.get("REPTMON2", "202411")   # previous month
NOWK     = os.environ.get("NOWK",     "01")
REPTDAY  = os.environ.get("REPTDAY",  "31122024")
RDATE    = os.environ.get("RDATE",    "31122024")  # DDMMYYYY
TDATE    = os.environ.get("TDATE",    "20241231")
SDATE    = os.environ.get("SDATE",    "20241201")
SDESC    = os.environ.get("SDESC",    "")

# Derived report date for month/year comparison
_rdate_parsed = datetime.strptime(RDATE, "%d%m%Y")
RDATE_MONTH   = _rdate_parsed.month
RDATE_YEAR    = _rdate_parsed.year

# --- Parquet input paths ---
LOAN_PQ       = os.path.join(INPUT_DIR, f"LOAN{REPTMON}{NOWK}.parquet")
ULOAN_PQ      = os.path.join(INPUT_DIR, f"ULOAN{REPTMON}{NOWK}.parquet")
LNCOMM_PQ     = os.path.join(INPUT_DIR, "LNCOMM.parquet")
LNNOTE_PQ     = os.path.join(INPUT_DIR, "LNNOTE.parquet")
LNFEE_PQ      = os.path.join(INPUT_DIR, f"LNFEE{REPTMON}{NOWK}.parquet")
CURRENT_PQ    = os.path.join(INPUT_DIR, "CURRENT.parquet")
OVERDFT_PQ    = os.path.join(INPUT_DIR, "OVERDFT.parquet")
LNHIST_PQ     = os.path.join(INPUT_DIR, "LNHIST.parquet")
LNNOTE_FULL_PQ= os.path.join(INPUT_DIR, "LNNOTE_FULL.parquet")
LNWOF_PQ      = os.path.join(INPUT_DIR, f"LNWOF{REPTMON}{NOWK}.parquet")
LNWOD_PQ      = os.path.join(INPUT_DIR, f"LNWOD{REPTMON}{NOWK}.parquet")
PLNWOF_PQ     = os.path.join(INPUT_DIR, f"LNWOF{REPTMON2}{NOWK}.parquet")
PLNWOD_PQ     = os.path.join(INPUT_DIR, f"LNWOD{REPTMON2}{NOWK}.parquet")
LOAN_PM_PQ    = os.path.join(INPUT_DIR, f"LOAN{REPTMON2}{NOWK}.parquet")
SASD_LOAN_PQ  = os.path.join(INPUT_DIR, f"SASD_LOAN{REPTMON}.parquet")
DISBURN_PQ    = os.path.join(INPUT_DIR, f"DISBURN{REPTMON}.parquet")
REPAYN_PQ     = os.path.join(INPUT_DIR, f"REPAYN{REPTMON}.parquet")
EXCLUDE_PQ    = os.path.join(INPUT_DIR, f"EXCLUDE_{REPTDAY}.parquet")
FORATEBKP_PQ  = os.path.join(INPUT_DIR, "FORATEBKP.parquet")
ROLLOVER_PQ   = os.path.join(INPUT_DIR, f"ROLLOVER{REPTMON}.parquet")

# --- Output paths ---
LALM_TXT    = os.path.join(OUTPUT_DIR, f"LALM{REPTMON}{NOWK}.txt")
LNAGING_TXT = os.path.join(OUTPUT_DIR, f"LNAGING{REPTMON}{NOWK}.txt")
B80510_TXT  = os.path.join(OUTPUT_DIR, f"B80510{REPTMON}{NOWK}.txt")
B80200_TXT  = os.path.join(OUTPUT_DIR, f"B80200{REPTMON}{NOWK}.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================================
# DUCKDB CONNECTION (used for parquet reads)
# ============================================================================
con = duckdb.connect()


def _load(path: str) -> pl.DataFrame:
    """Load a parquet file into a Polars DataFrame via DuckDB."""
    return con.execute(f"SELECT * FROM read_parquet('{path}')").pl()


# ============================================================================
# HELPER: SECTOR MAPPING PIPELINE
# The large NEWSECT/VALIDSE/ALM2/ALMA pattern repeats ~8 times in the SAS.
# It is factored here into reusable functions.
# ============================================================================

def _apply_newsect_step(df: pl.DataFrame) -> pl.DataFrame:
    """
    Apply NEWSECT / VALIDSE format and produce SECTCD column.
    SAS equivalent:
        SECTA    = PUT(SECTORCD, $NEWSECT.);
        SECVALID = PUT(SECTORCD, $VALIDSE.);
        IF SECTA NE '    ' THEN SECTCD = SECTA; ELSE SECTCD = SECTORCD;
    Then apply INVALID fallback rules onto SECTCD.
    """
    df = df.with_columns([
        pl.col("SECTORCD").map_elements(format_newsect,  return_dtype=pl.String).alias("SECTA"),
        pl.col("SECTORCD").map_elements(format_validse,  return_dtype=pl.String).alias("SECVALID"),
    ])
    df = df.with_columns(
        pl.when(pl.col("SECTA") != "")
          .then(pl.col("SECTA"))
          .otherwise(pl.col("SECTORCD"))
          .alias("SECTCD")
    )
    # Apply INVALID fallback onto SECTCD
    s = pl.col("SECTCD")
    df = df.with_columns(
        pl.when(pl.col("SECVALID") != "INVALID").then(pl.col("SECTCD"))
          .when(s.str.slice(0, 1) == "1").then(pl.lit("1400"))
          .when(s.str.slice(0, 1) == "2").then(pl.lit("2900"))
          .when(s.str.slice(0, 1) == "3").then(pl.lit("3919"))
          .when(s.str.slice(0, 1) == "4").then(pl.lit("4010"))
          .when(s.str.slice(0, 1) == "5").then(pl.lit("5999"))
          .when(s.str.slice(0, 2) == "61").then(pl.lit("6120"))
          .when(s.str.slice(0, 2) == "62").then(pl.lit("6130"))
          .when(s.str.slice(0, 2) == "63").then(pl.lit("6310"))
          .when(s.str.slice(0, 2).is_in(["64","65","66","67","68","69"])).then(pl.lit("6130"))
          .when(s.str.slice(0, 1) == "7").then(pl.lit("7199"))
          .when(s.str.slice(0, 2).is_in(["81","82"])).then(pl.lit("8110"))
          .when(s.str.slice(0, 2).is_in(["83","84","85","86","87","88","89"])).then(pl.lit("8999"))
          .when(s.str.slice(0, 2) == "91").then(pl.lit("9101"))
          .when(s.str.slice(0, 2) == "92").then(pl.lit("9410"))
          .when(s.str.slice(0, 2).is_in(["93","94","95"])).then(pl.lit("9499"))
          .when(s.str.slice(0, 2).is_in(["96","97","98","99"])).then(pl.lit("9999"))
          .otherwise(pl.col("SECTCD"))
          .alias("SECTCD")
    )
    return df


def _expand_sector_rows(df: pl.DataFrame) -> pl.DataFrame:
    """
    Produce DATA ALM2 expansion rows (sector sub-code rollup).
    Each input row can generate multiple output rows with different SECTORCD.
    Returns only the new child rows (not the originals).
    """
    rows: list[dict] = []

    for row in df.to_dicts():
        s = row.get("SECTCD", "") or ""
        # Use a captured copy so each lambda/append gets the right base row
        _base = dict(row)

        def out(code: str, _b: dict = _base) -> None:
            r = dict(_b)
            r["SECTORCD"] = code
            rows.append(r)

        if s in ('1111','1112','1113','1114','1115','1116','1117','1119','1120','1130','1140','1150'):
            if s in ('1111','1113','1115','1117','1119','1112','1114','1116'):
                out('1110')
            out('1100')
        if s in ('2210','2220'):
            out('2200')
        if s in ('2301','2302','2303'):
            out('2300')
            if s == '2303':
                out('2302')
        if s in ('3110','3115','3111','3112','3113','3114'):
            if s in ('3110','3113','3114','3111','3112','3115'):
                out('3100')
            if s in ('3115','3111','3112'):
                out('3110')
        if s in ('3211','3212','3219'): out('3210')
        if s in ('3221','3222'):        out('3220')
        if s in ('3231','3232'):        out('3230')
        if s in ('3241','3242'):        out('3240')
        if s in ('3270','3280','3290','3271','3272','3273','3311','3312','3313'):
            if s in ('3270','3280','3290','3271','3272','3273'): out('3260')
            if s in ('3271','3272','3273'):                      out('3270')
            if s in ('3311','3312','3313'):                      out('3310')
        if s in ('3431','3432','3433'): out('3430')
        if s in ('3551','3552'):        out('3550')
        if s in ('3611','3619'):        out('3610')
        if s in ('3710','3720','3730','3721','3731','3732'):
            out('3700')
            if s == '3721':              out('3720')
            if s in ('3731','3732'):     out('3730')
        if s in ('3811','3813','3814','3819'): out('3800')
        if s in ('3813','3814','3819'):        out('3812')
        if s in ('3832','3834','3835','3833'):
            out('3831')
            if s == '3833': out('3832')
        if s in ('3842','3843','3844'): out('3841')
        if s in ('3851','3852','3853'): out('3850')
        if s in ('3861','3862','3863','3864','3865','3866'): out('3860')
        if s in ('3871','3872'):        out('3870')
        if s in ('3891','3892','3893','3894'): out('3890')
        if s in ('3911','3919'):        out('3910')
        if s in ('3951','3952','3953','3954','3955','3956','3957'):
            out('3950')
            if s in ('3952','3953'):         out('3951')
            if s in ('3955','3956','3957'):  out('3954')
        if s in ('5001','5002','5003','5004','5005','5006','5008'): out('5010')
        if s in ('6110','6120','6130'): out('6100')
        if s in ('6310','6320'):        out('6300')
        if s in ('7111','7112','7117','7113','7114','7115','7116'): out('7110')
        if s in ('7113','7114','7115','7116'):                       out('7112')
        if s in ('7112','7114'):        out('7113')
        if s == '7116':                 out('7115')
        if s in ('7121','7122','7123','7124'):
            out('7120')
            if s == '7124': out('7123')
            if s == '7122': out('7121')
        if s in ('7131','7132','7133','7134'): out('7130')
        if s in ('7191','7192','7193','7199'): out('7190')
        if s in ('7210','7220'):        out('7200')
        if s in ('8110','8120','8130'): out('8100')
        if s in ('8310','8330','8340','8320','8331','8332','8321','8333'):
            out('8300')
            if s in ('8320','8331','8332','8321','8333'): out('8330')
        if s == '8321':                 out('8320')
        if s == '8333':                 out('8332')
        if s in ('8420','8411','8412','8413','8414','8415','8416'):
            out('8400')
            if s in ('8411','8412','8413','8414','8415','8416'): out('8410')
        if s[:2] == '89':
            out('8900')
            if s in ('8910','8911','8912','8913','8914'):
                if s in ('8911','8912','8913','8914'): out('8910')
                if s == '8910':                        out('8914')
            if s in ('8921','8922','8920'):
                if s in ('8921','8922'): out('8920')
                if s == '8920':          out('8922')
            if s in ('8931','8932'):     out('8930')
            if s in ('8991','8999'):     out('8990')
        if s in ('9101','9102','9103'): out('9100')
        if s in ('9201','9202','9203'): out('9200')
        if s in ('9311','9312','9313','9314'): out('9300')
        if s[:2] == '94':
            out('9400')
            if s in ('9433','9434','9435','9432','9431','9440','9450'):
                if s in ('9433','9434','9435'): out('9432')
                out('9430')
            if s in ('9410','9420'): out('9499')

    if not rows:
        return pl.DataFrame(schema=df.schema)
    return pl.from_dicts(rows, schema=df.schema)


def _apply_sector_top_rollup(df: pl.DataFrame) -> pl.DataFrame:
    """
    Produce DATA ALMA rows that roll sub-sectors to 4-digit top codes
    (1000, 2000 … 9000). Returns only the new rollup rows.
    """
    rows: list[dict] = []

    for row in df.to_dicts():
        s = row.get("SECTORCD", "") or ""
        _base = dict(row)

        def out(code: str, _b: dict = _base) -> None:
            r = dict(_b)
            r["SECTORCD"] = code
            rows.append(r)

        if s in ('1100','1200','1300','1400'):             out('1000')
        elif s in ('2100','2200','2300','2400','2900'):    out('2000')
        elif s in ('3100','3120','3210','3220','3230','3240','3250','3260',
                   '3310','3430','3550','3610','3700','3800','3825','3831',
                   '3841','3850','3860','3870','3890','3910','3950','3960'): out('3000')
        elif s in ('4010','4020','4030'):                  out('4000')
        elif s in ('5010','5020','5030','5040','5050','5999'): out('5000')
        elif s in ('6100','6300'):                         out('6000')
        elif s in ('7110','7120','7130','7190','7200'):    out('7000')
        elif s in ('8100','8300','8400','8900'):           out('8000')
        elif s in ('9100','9200','9300','9400','9500','9600'): out('9000')

    if not rows:
        return pl.DataFrame(schema=df.schema)
    return pl.from_dicts(rows, schema=df.schema)


def _full_sector_expand(df: pl.DataFrame) -> pl.DataFrame:
    """
    Full sector mapping pipeline used identically in all ALM sector sections:
      1. Apply NEWSECT / VALIDSE step -> SECTCD
      2. ALM base:  SECTORCD = SECTCD
      3. ALM2:      expanded sub-code child rows
      4. Combine ALM + ALM2; fill blank SECTORCD -> '9999'
      5. ALMA:      top-level rollup rows
      6. Final:     ALM_combined + ALMA; fill blank -> '9999'
    """
    df = _apply_newsect_step(df)
    alm_base = df.with_columns(pl.col("SECTCD").alias("SECTORCD"))
    alm2 = _expand_sector_rows(alm_base)
    alm_combined = pl.concat([alm_base, alm2], how="diagonal")
    alm_combined = alm_combined.with_columns(
        pl.when(pl.col("SECTORCD").str.strip_chars() == "")
          .then(pl.lit("9999"))
          .otherwise(pl.col("SECTORCD"))
          .alias("SECTORCD")
    )
    alma = _apply_sector_top_rollup(alm_combined)
    result = pl.concat([alm_combined, alma], how="diagonal")
    result = result.with_columns(
        pl.when(pl.col("SECTORCD").str.strip_chars() == "")
          .then(pl.lit("9999"))
          .otherwise(pl.col("SECTORCD"))
          .alias("SECTORCD")
    )
    return result


# ============================================================================
# HELPER: ACCUMULATE LALM ROWS
# ============================================================================
_lalm_frames: list[pl.DataFrame] = []

LALM_SCHEMA = {"BNMCODE": pl.String, "AMTIND": pl.String, "AMOUNT": pl.Float64}


def _append_lalm(df: pl.DataFrame) -> None:
    """Append BNMCODE/AMTIND/AMOUNT rows to the global LALM accumulator."""
    if df is not None and df.height > 0:
        _lalm_frames.append(
            df.select(["BNMCODE", "AMTIND", "AMOUNT"])
              .cast({"BNMCODE": pl.String, "AMTIND": pl.String, "AMOUNT": pl.Float64})
        )


# ============================================================================
# LOAD CORE DATASETS
# ============================================================================

# BNM.LOAN&REPTMON&NOWK (raw, used for ULOAN/approved-limit sections)
loan_raw = _load(LOAN_PQ)

# DATA LOAN&REPTMON&NOWK: filter PAIDIND NOT IN ('P','C') OR EIR_ADJ NE .
loan = loan_raw.filter(
    ~pl.col("PAIDIND").is_in(["P", "C"]) | pl.col("EIR_ADJ").is_not_null()
)

# BNM.ULOAN&REPTMON&NOWK
uloan = _load(ULOAN_PQ)

# ============================================================================
# PRE-GEN MAIN DATA FOR LATER MERGE
# DO NOT CHANGE KEY / CAN ADD VARIABLE BUT DO IMPACT ANALYSIS
# ============================================================================

# M_LNCOMM
m_lncomm = (
    _load(LNCOMM_PQ)
    .select(["ACCTNO", "COMMNO", "CUSEDAMT"])
    .sort(["ACCTNO", "COMMNO"])
)

# M_LNNOTE
m_lnnote = (
    _load(LNNOTE_PQ)
    .select(["ACCTNO", "COMMNO", "CJFEE", "BALANCE"])
    .sort(["ACCTNO", "COMMNO"])
)

# M_FEE -> CL_FEE  (FEEPLAN='CL' AND DUETOTAL>0, sum by ACCTNO NOTENO)
_lnfee = _load(LNFEE_PQ).select(["ACCTNO", "NOTENO", "DUETOTAL", "FEEPLAN"])
cl_fee = (
    _lnfee
    .filter((pl.col("FEEPLAN") == "CL") & (pl.col("DUETOTAL") > 0))
    .group_by(["ACCTNO", "NOTENO"])
    .agg(pl.col("DUETOTAL").sum())
)

# ============================================================================
# NO OF COUNTS FOR ISLAMIC LOANS/OD
# ============================================================================

temploan = loan.sort(["ACCTNO", "NOTENO"])

# Remove Islamic OD / product 321
temploan = temploan.filter(
    ~(
        (
            (pl.col("ACCTNO").is_between(2500000000, 2599999999) |
             (pl.col("ACCTNO") == 2850000000)) &
            pl.col("NOTENO").is_between(40000, 49999)
        ) |
        (pl.col("PRODUCT") == 321)
    )
)

# Merge CL_FEE
temploan = (
    temploan
    .join(cl_fee, on=["ACCTNO", "NOTENO"], how="left")
    .with_columns(
        (pl.col("DUETOTAL").fill_null(0.0) * pl.col("FORATE")).alias("CLFEE")
    )
    .drop(["DUETOTAL", "FEEPLAN"])
)

# Merge M_LNCOMM
temploan = (
    temploan.sort(["ACCTNO", "COMMNO"])
    .join(m_lncomm, on=["ACCTNO", "COMMNO"], how="left")
    .filter(pl.col("RLEASAMT") == 0)
    .filter(
        pl.col("COSTCTR").is_between(3000, 3998) |
        pl.col("COSTCTR").is_in([4043, 4048])
    )
)

# NODELETE flag
temploan = temploan.with_columns(
    pl.when(
        (pl.col("ACCTYPE") == "LN") &
        (
            (pl.col("BALANCE") == pl.col("CJFEE")) |
            (pl.col("BALANCE") == pl.col("CLFEE"))
        )
    ).then(1).otherwise(0).alias("NODELETE")
)
temploan = temploan.filter(
    ~((pl.col("ACCTYPE") == "LN") & (pl.col("COMMNO") > 0) &
      (pl.col("CUSEDAMT") > 0) & (pl.col("NODELETE") == 0))
)
temploan = temploan.filter(
    ~((pl.col("ACCTYPE") == "LN") &
      pl.col("PRODUCT").is_between(600, 699) &
      (pl.col("NODELETE") == 0))
)

# TEMPULOAN
tempuloan = (
    uloan.sort(["ACCTNO", "COMMNO"])
    .join(m_lncomm, on=["ACCTNO", "COMMNO"], how="left")
    .join(
        m_lnnote.rename({"BALANCE": "BALANCE_NOTE", "CJFEE": "CJFEE_NOTE"}),
        on=["ACCTNO", "COMMNO"], how="left"
    )
    .filter(pl.col("RLEASAMT") == 0)
    .filter(
        pl.col("COSTCTR").is_between(3000, 3998) |
        pl.col("COSTCTR").is_in([4043, 4048])
    )
    .filter(
        ~((pl.col("ACCTYPE") == "LN") &
          (pl.col("COMMNO") > 0) & (pl.col("CUSEDAMT") > 0))
    )
    .filter(
        ~((pl.col("ACCTYPE") == "LN") & pl.col("PRODUCT").is_between(600, 699))
    )
)

# PROC SUMMARY count by AMTIND CUSTCD -> ALMLOAN (BNMCODE='8020000000000Y', AMTIND='I')
_alm_isl  = temploan.group_by(["AMTIND", "CUSTCD"]).agg(pl.len().alias("_FREQ_"))
_ualm_isl = tempuloan.group_by(["AMTIND", "CUSTCD"]).agg(pl.len().alias("_FREQ_"))

_append_lalm(
    pl.concat([_alm_isl, _ualm_isl], how="diagonal")
    .with_columns([
        pl.lit("I").alias("AMTIND"),
        (pl.col("_FREQ_") * 1000).alias("AMOUNT"),
        pl.lit("8020000000000Y").alias("BNMCODE"),
    ])
)

# ============================================================================
# NON-PERFORMING LOANS (NPL)
# ============================================================================

_alq = (
    loan
    .filter(
        (pl.col("PRODCD").str.slice(0, 2) == "34") &
        (pl.col("RISKCD").str.strip_chars() != "")
    )
    .group_by(["AMTIND", "RISKCD"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("AMOUNT"))
    .with_columns(
        (pl.col("RISKCD") + pl.lit("00000000Y")).alias("BNMCODE")
    )
)
_append_lalm(_alq)

# ============================================================================
# NPL - BY CUSTOMER AND BY SECTORIAL CODE
# ============================================================================

_alq2 = (
    loan
    .filter(
        (pl.col("PRODCD").str.slice(0, 2) == "34") &
        (pl.col("RISKCD").str.strip_chars() != "")
    )
    .group_by(["AMTIND", "SECTORCD", "CUSTCD"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("AMOUNT"))
)


def _npl_sector_custcd_bnm(row: dict) -> list[dict]:
    """
    Translate one ALQ row into zero-or-more ALQLOAN rows.
    _TYPE_=6 in SAS = SECTORCD present, CUSTCD absent (null).
    _TYPE_=7 in SAS = both SECTORCD and CUSTCD present.
    """
    sc  = str(row.get("SECTORCD") or "").strip()
    cd  = str(row.get("CUSTCD")   or "").strip()
    amt = row["AMTIND"]
    amount = row["AMOUNT"]
    out: list[dict] = []

    def mk(bnm: str) -> None:
        out.append({"BNMCODE": bnm, "AMTIND": amt, "AMOUNT": amount})

    def sect_suffix_6100(s: str) -> str:
        if s in ('0410','0420','0430','9999'): return f"349006100{s}Y"
        if s[:2] == '01': return "3490061000100Y"
        if s[:2] == '02': return "3490061000200Y"
        if s[:3] == '031': return "3490061000310Y"
        if s[:3] == '032': return "3490061000320Y"
        if s[0] == '1':  return "3490061001000Y"
        if s[0] == '2':  return "3490061002000Y"
        if s[0] == '3':  return "3490061003000Y"
        if s[0] == '4':  return "3490061004000Y"
        if s[0] == '5':  return "3490061005000Y"
        if s[:2] == '61': return "3490061006100Y"
        if s[:2] == '62': return "3490061006200Y"
        if s[:2] == '63': return "3490061006300Y"
        if s[0] == '7':  return "3490061007000Y"
        if s[:3] == '831': return "3490061008310Y"
        if s[:3] == '832': return "3490061008320Y"
        if s[:3] == '833': return "3490061008330Y"
        if s[:2] in ('90','91','92','93','94','95','96','97','98'): return "3490061009000Y"
        return "3490061009999Y"

    # _TYPE_=6: CUSTCD absent
    if not cd:
        if sc in ('0410','0420','0430','9999'):   mk(f"349000000{sc}Y")
        elif sc[:2] == '01': mk("3490000000100Y")
        elif sc[:2] == '02': mk("3490000000200Y")
        elif sc[:3] == '031': mk("3490000000310Y")
        elif sc[:3] == '032': mk("3490000000320Y")
        elif sc[0] == '1':   mk("3490000001000Y")
        elif sc[0] == '2':   mk("3490000002000Y")
        elif sc[0] == '3':   mk("3490000003000Y")
        elif sc[0] == '4':   mk("3490000004000Y")
        elif sc[0] == '5':   mk(f"349000000{sc}Y")
        elif sc[:2] == '61': mk("3490000006100Y")
        elif sc[:2] == '62': mk("3490000006200Y")
        elif sc[:2] == '63': mk("3490000006300Y")
        elif sc[0] == '7':   mk("3490000007000Y")
        elif sc[:2] == '81': mk("3490000008100Y")
        elif sc[:2] == '82': mk("3490000008200Y")
        elif sc[:3] == '831': mk("3490000008310Y")
        elif sc[:3] == '832': mk("3490000008320Y")
        elif sc[:3] == '833': mk("3490000008330Y")
        elif sc[:2] in ('90','91','92','93','94','95','96','97','98'): mk("3490000009000Y")
        else: mk("3490000009999Y")
        return out

    # _TYPE_=7: CUSTCD present
    if cd in ('61','66'):
        mk(sect_suffix_6100(sc))

    if cd == '77':
        if sc in ('0410','0420','0430'): mk(f"34900{cd}00{sc}Y")
        elif sc[:2] == '01': mk(f"34900{cd}000100Y")
        elif sc[:2] == '02': mk(f"34900{cd}000200Y")
        elif sc[:3] == '031': mk(f"34900{cd}000310Y")
        elif sc[:3] == '032': mk(f"34900{cd}000320Y")
        else: mk(f"34900{cd}009999Y")

    return out


_npl_rows: list[dict] = []
for _r in _alq2.to_dicts():
    _npl_rows.extend(_npl_sector_custcd_bnm(_r))
if _npl_rows:
    _append_lalm(pl.from_dicts(_npl_rows, schema=LALM_SCHEMA))

# ============================================================================
# GROSS LOAN - BY APPROVED LIMIT
# ============================================================================

# PROC SORT NODUPKEY by ACCTNO COMMNO NOTENO
_temp = loan.unique(subset=["ACCTNO", "COMMNO", "NOTENO"], keep="first")

# AFFACCT: count per ACCTNO+COMMNO, keep COUNT>1 and COMMNO>0
_affacct = (
    _temp
    .group_by(["ACCTNO", "COMMNO"])
    .agg(pl.len().alias("COUNT"))
    .filter((pl.col("COUNT") > 1) & (pl.col("COMMNO") > 0))
)

# Merge LNCOMM for CCURAMT, drop COMMNO
_lncomm_ccur = (
    _load(LNCOMM_PQ)
    .select(["ACCTNO", "COMMNO", "CCURAMT"])
    .sort(["ACCTNO", "COMMNO"])
)
_affacct = (
    _affacct
    .join(_lncomm_ccur, on=["ACCTNO", "COMMNO"], how="left")
    .drop("COMMNO")
)

# Join back to loan on ACCTNO+NOTENO
_loan_gl = _temp.join(_affacct, on=["ACCTNO", "NOTENO"], how="left")

# Compute adjusted APPRLIM2
_prodcds_ccuramt = {
    '34114','34115','34117','34120','34149','34600','34190','34690','34170'
}


def _adj_apprlim2(row: dict) -> float:
    acctno      = row.get("ACCTNO", 0) or 0
    noteno      = row.get("NOTENO", 0) or 0
    product     = row.get("PRODUCT", 0) or 0
    prodcd      = str(row.get("PRODCD") or "").strip()
    apprlim2    = float(row.get("APPRLIM2") or 0)
    apprlim2ori = float(row.get("APPRLIM2ORI") or apprlim2)
    ccuramt     = row.get("CCURAMT")
    forate      = float(row.get("FORATE") or 1) or 1
    count       = row.get("COUNT")

    if (
        (2500000000 <= acctno <= 2599999999 or
         2850000000 <= acctno <= 2859999999) and
        40000 <= noteno <= 49999
    ) or product == 321:
        apprlim2 = apprlim2ori

    if count is not None and ccuramt is not None:
        if prodcd in _prodcds_ccuramt:
            apprlim2 = float(ccuramt)
            if prodcd[:3] == '346':
                apprlim2 *= forate
    return apprlim2


_loan_gl = _loan_gl.with_columns(
    pl.struct(_loan_gl.columns)
      .map_elements(_adj_apprlim2, return_dtype=pl.Float64)
      .alias("APPRLIM2")
)

# IF BALANCE = . AND EIR_ADJ NE 0 THEN APPRLIM2 = 0
_loan_gl = _loan_gl.with_columns(
    pl.when(pl.col("BALANCE").is_null() & (pl.col("EIR_ADJ") != 0))
      .then(0.0)
      .otherwise(pl.col("APPRLIM2"))
      .alias("APPRLIM2")
)

# Filter: PRODCD starts '34' or = '54120'
_loan_gl_f = _loan_gl.filter(
    (pl.col("PRODCD").str.slice(0, 2) == "34") | (pl.col("PRODCD") == "54120")
).with_columns(
    pl.col("APPRLIM2")
      .map_elements(format_apprlimt, return_dtype=pl.String)
      .alias("ALMLIMT")
)

# _TYPE_=6 (AMTIND + ALMLIMT only)
_alm_gl6 = (
    _loan_gl_f
    .group_by(["AMTIND", "ALMLIMT"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("AMOUNT"))
)
# _TYPE_=7 (AMTIND + ALMLIMT + CUSTCD)
_alm_gl7 = (
    _loan_gl_f
    .group_by(["AMTIND", "ALMLIMT", "CUSTCD"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("AMOUNT"))
)

_gl_rows: list[dict] = []
for _r in _alm_gl6.to_dicts():
    _gl_rows.append({"BNMCODE": f"{_r['ALMLIMT']}00000000Y",
                     "AMTIND": _r["AMTIND"], "AMOUNT": _r["AMOUNT"]})
for _r in _alm_gl7.to_dicts():
    _limt = _r["ALMLIMT"]
    _cd   = str(_r.get("CUSTCD") or "").strip()
    _amt  = _r["AMTIND"]
    _a    = _r["AMOUNT"]
    if _cd in ('41','42','43','44','46','47','48','49','51','52','53','54'):
        _gl_rows.append({"BNMCODE": f"{_limt}{_cd}000000Y", "AMTIND": _amt, "AMOUNT": _a})
        if _cd in ('41','42','43'):
            _gl_rows.append({"BNMCODE": f"{_limt}66000000Y", "AMTIND": _amt, "AMOUNT": _a})
        if _cd in ('44','46','47'):
            _gl_rows.append({"BNMCODE": f"{_limt}67000000Y", "AMTIND": _amt, "AMOUNT": _a})
        if _cd in ('48','49','51'):
            _gl_rows.append({"BNMCODE": f"{_limt}68000000Y", "AMTIND": _amt, "AMOUNT": _a})
        if _cd in ('52','53','54'):
            _gl_rows.append({"BNMCODE": f"{_limt}69000000Y", "AMTIND": _amt, "AMOUNT": _a})
    if _cd in ('61','41','42','43'):
        _gl_rows.append({"BNMCODE": f"{_limt}61000000Y", "AMTIND": _amt, "AMOUNT": _a})
    elif _cd == '77':
        _gl_rows.append({"BNMCODE": f"{_limt}77000000Y", "AMTIND": _amt, "AMOUNT": _a})
if _gl_rows:
    _append_lalm(pl.from_dicts(_gl_rows, schema=LALM_SCHEMA))

# ============================================================================
# GROSS LOAN - BY COLLATERAL TYPE
# ============================================================================

_alm_coll = (
    loan
    .filter(
        (pl.col("PRODCD").str.slice(0, 2) == "34") | (pl.col("PRODCD") == "54120")
    )
    .group_by(["AMTIND", "COLLCD", "CUSTCD"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("AMOUNT"))
)

_special_coll = {'30560','30570','30580'}
_coll_rows: list[dict] = []
for _r in _alm_coll.to_dicts():
    _coll = str(_r.get("COLLCD") or "").strip()
    _cd   = str(_r.get("CUSTCD") or "").strip()
    _amt  = _r["AMTIND"]
    _a    = _r["AMOUNT"]
    # _TYPE_=6: special COLLCD, no CUSTCD
    if not _cd and _coll in _special_coll:
        _coll_rows.append({"BNMCODE": f"{_coll}00000000Y", "AMTIND": _amt, "AMOUNT": _a})
        continue
    # _TYPE_=7: CUSTCD present, COLLCD NOT special
    if _cd and _coll not in _special_coll:
        if _cd in ('10','02','03','11','12'):
            _coll_rows.append({"BNMCODE": f"{_coll}10000000Y", "AMTIND": _amt, "AMOUNT": _a})
        elif _cd in ('20','13','17','30','32','33','34','35','36','37','38','39','40'):
            _coll_rows.append({"BNMCODE": f"{_coll}20000000Y", "AMTIND": _amt, "AMOUNT": _a})
        elif _cd in ('04','05','06'):
            _coll_rows.append({"BNMCODE": f"{_coll}{_cd}000000Y", "AMTIND": _amt, "AMOUNT": _a})
            _coll_rows.append({"BNMCODE": f"{_coll}20000000Y",    "AMTIND": _amt, "AMOUNT": _a})
        elif _cd in ('60','62','63','44','46','47','48','49','51'):
            _coll_rows.append({"BNMCODE": f"{_coll}60000000Y", "AMTIND": _amt, "AMOUNT": _a})
        elif _cd in ('61','41','42','43'):
            _coll_rows.append({"BNMCODE": f"{_coll}61000000Y", "AMTIND": _amt, "AMOUNT": _a})
            _coll_rows.append({"BNMCODE": f"{_coll}60000000Y", "AMTIND": _amt, "AMOUNT": _a})
        elif _cd in ('64','52','53','54','75','57','59'):
            _coll_rows.append({"BNMCODE": f"{_coll}64000000Y", "AMTIND": _amt, "AMOUNT": _a})
            _coll_rows.append({"BNMCODE": f"{_coll}60000000Y", "AMTIND": _amt, "AMOUNT": _a})
        elif _cd in ('70','71','72','73','74'):
            _coll_rows.append({"BNMCODE": f"{_coll}70000000Y", "AMTIND": _amt, "AMOUNT": _a})
        elif _cd in ('76','78'):
            _coll_rows.append({"BNMCODE": f"{_coll}76000000Y", "AMTIND": _amt, "AMOUNT": _a})
        elif _cd == '77':
            _coll_rows.append({"BNMCODE": f"{_coll}77000000Y", "AMTIND": _amt, "AMOUNT": _a})
            _coll_rows.append({"BNMCODE": f"{_coll}76000000Y", "AMTIND": _amt, "AMOUNT": _a})
        elif _cd == '79':
            _coll_rows.append({"BNMCODE": f"{_coll}79000000Y", "AMTIND": _amt, "AMOUNT": _a})
        elif _cd in ('80','81','82','83','84','85','86','87','88','89','90',
                     '91','92','95','96','98','99'):
            _coll_rows.append({"BNMCODE": f"{_coll}80000000Y", "AMTIND": _amt, "AMOUNT": _a})
            if _cd in ('86','87','88','89'):
                _coll_rows.append({"BNMCODE": f"{_coll}86000000Y", "AMTIND": _amt, "AMOUNT": _a})
if _coll_rows:
    _append_lalm(pl.from_dicts(_coll_rows, schema=LALM_SCHEMA))

# ============================================================================
# GROSS LOAN - NO OF UTILISED LOAN ACCOUNTS (B80510)
# ============================================================================

# DATA TEMPLOAN TEMPLOANBT split
_loan_full = _load(LOAN_PQ)
_bt_mask = (
    (((_loan_full["ACCTNO"].is_between(2500000000, 2599999999)) &
      (_loan_full["NOTENO"].is_between(40000, 49999)))) |
    (_loan_full["PRODUCT"] == 321)
)
_temploan_bt  = _loan_full.filter(_bt_mask).unique(subset=["ACCTNO"], keep="first")
_temploan_b80 = pl.concat([
    _loan_full.filter(~_bt_mask),
    _temploan_bt,
], how="diagonal").sort(["ACCTNO", "NOTENO"])

# Merge CL_FEE
_temploan_b80 = (
    _temploan_b80
    .join(cl_fee, on=["ACCTNO", "NOTENO"], how="left")
    .with_columns(
        (pl.col("DUETOTAL").fill_null(0.0) * pl.col("FORATE")).alias("CLFEE")
    )
    .drop(["DUETOTAL", "FEEPLAN"])
)

# Merge M_LNCOMM
_temploan_b80 = (
    _temploan_b80.sort(["ACCTNO", "COMMNO"])
    .join(m_lncomm, on=["ACCTNO", "COMMNO"], how="left")
)

# DATA B80510: utilised loan filter
_b80510 = (
    _temploan_b80
    .filter(~pl.col("PAIDIND").is_in(["P", "C"]))
    .filter(
        (pl.col("PRODCD").str.slice(0, 2) == "34") | (pl.col("PRODCD") == "54120")
    )
    .filter(pl.col("BALANCE") != pl.col("CJFEE"))
    .with_columns(pl.col("BALANCE").round(2).alias("BALX"))
    .with_columns(
        pl.when(pl.col("BALX") == 0).then(pl.lit("Y")).otherwise(pl.lit(" ")).alias("XIND")
    )
    .filter(pl.col("XIND") != "Y")
)

# ACCTYPE='LN' conditions
_ln = pl.col("ACCTYPE") == "LN"
_b80510 = _b80510.filter(
    ~_ln |
    (
        (pl.col("RLEASAMT") != 0) &
        (~pl.col("PAIDIND").is_in(["P","C"])) &
        (pl.col("BALANCE") > 0) &
        (pl.col("CJFEE") != pl.col("BALANCE"))
    ) |
    (
        (pl.col("RLEASAMT") == 0) &
        (~pl.col("PAIDIND").is_in(["P","C"])) &
        (pl.col("BALANCE") > 0) &
        pl.col("PRODUCT").is_between(600, 699)
    ) |
    (
        (pl.col("RLEASAMT") == 0) &
        (~pl.col("PAIDIND").is_in(["P","C"])) &
        (pl.col("BALANCE") > 0) &
        (pl.col("COMMNO") > 0) &
        (pl.col("CUSEDAMT") > 0)
    )
)
_b80510 = _b80510.filter(
    ~(pl.col("RLEASAMT") != 0) | ~(pl.col("BALANCE") == pl.col("CLFEE"))
)

# UNQ column (row-number based; 34170/34190/34690 always = 1)
_b80510 = _b80510.with_row_index("_N_").with_columns(
    pl.when(pl.col("PRODCD").is_in(["34170","34190","34690"]))
      .then(pl.lit(1))
      .otherwise(pl.col("_N_") + 1)
      .alias("UNQ")
)

# Format APPRLIM2
_b80510 = _b80510.with_columns(
    pl.col("APPRLIM2")
      .map_elements(format_loansize, return_dtype=pl.String)
      .alias("LOANSIZE")
)

# Save BNM.B80510
_b80510.write_csv(B80510_TXT)

# ALM  (all rows, before de-dup) by AMTIND LOANSIZE CUSTCD
_alm_b80_all  = _b80510.group_by(["AMTIND","LOANSIZE","CUSTCD"]).agg(pl.len().alias("_FREQ_"))
_alm_b80_tot6 = _b80510.group_by(["AMTIND","LOANSIZE"]).agg(pl.len().alias("_FREQ_"))
# ALM2 (de-dup by ACCTNO COMMNO UNQ) by AMTIND LOANSIZE CUSTCD
_b80510_dd    = _b80510.unique(subset=["ACCTNO","COMMNO","UNQ"], keep="first")
_alm_b80_dd5  = _b80510_dd.group_by(["AMTIND","CUSTCD"]).agg(pl.len().alias("_FREQ_"))

_b80_rows: list[dict] = []

# _TYPE_=5 (CUSTCD only, de-dup counts)
for _r in _alm_b80_dd5.to_dicts():
    _cd  = str(_r.get("CUSTCD") or "").strip()
    _amt = _r["AMTIND"]
    _fr  = _r["_FREQ_"] * 1000
    if _cd in ('10','02','03','11','12'):
        _b80_rows.append({"BNMCODE":"8051010000000Y","AMTIND":_amt,"AMOUNT":_fr})
    if _cd in ('20','13','17','04','05','06','30','32','33','34','35','36','37','38','39','40'):
        _b80_rows.append({"BNMCODE":"8051020000000Y","AMTIND":_amt,"AMOUNT":_fr})
    if _cd in ('61','41','42','43'):
        _b80_rows.append({"BNMCODE":"8051061000000Y","AMTIND":_amt,"AMOUNT":_fr})
        if _cd in ('41','42','43'):
            _b80_rows.append({"BNMCODE":f"80510{_cd}000000Y","AMTIND":_amt,"AMOUNT":_fr})
            _b80_rows.append({"BNMCODE":"8051066000000Y","AMTIND":_amt,"AMOUNT":_fr})
            _b80_rows.append({"BNMCODE":f"80500{_cd}000000Y","AMTIND":_amt,"AMOUNT":_fr})
    if _cd in ('62','44','46','47'):
        _b80_rows.append({"BNMCODE":"8051062000000Y","AMTIND":_amt,"AMOUNT":_fr})
        if _cd in ('44','46','47'):
            _b80_rows.append({"BNMCODE":f"80510{_cd}000000Y","AMTIND":_amt,"AMOUNT":_fr})
            _b80_rows.append({"BNMCODE":"8051067000000Y","AMTIND":_amt,"AMOUNT":_fr})
            _b80_rows.append({"BNMCODE":f"80500{_cd}000000Y","AMTIND":_amt,"AMOUNT":_fr})
    if _cd in ('63','48','49','51'):
        _b80_rows.append({"BNMCODE":"8051063000000Y","AMTIND":_amt,"AMOUNT":_fr})
        if _cd in ('48','49','51'):
            _b80_rows.append({"BNMCODE":f"80510{_cd}000000Y","AMTIND":_amt,"AMOUNT":_fr})
            _b80_rows.append({"BNMCODE":"8051068000000Y","AMTIND":_amt,"AMOUNT":_fr})
            _b80_rows.append({"BNMCODE":f"80500{_cd}000000Y","AMTIND":_amt,"AMOUNT":_fr})
    if _cd in ('64','52','53','54','75','57','59'):
        _b80_rows.append({"BNMCODE":"8051064000000Y","AMTIND":_amt,"AMOUNT":_fr})
        if _cd in ('52','53','54'):
            _b80_rows.append({"BNMCODE":f"80510{_cd}000000Y","AMTIND":_amt,"AMOUNT":_fr})
            _b80_rows.append({"BNMCODE":"8051069000000Y","AMTIND":_amt,"AMOUNT":_fr})
            _b80_rows.append({"BNMCODE":f"80500{_cd}000000Y","AMTIND":_amt,"AMOUNT":_fr})
    if _cd in ('41','42','43','44','46','47','48','49','51','52','53','54'):
        _b80_rows.append({"BNMCODE":"8051065000000Y","AMTIND":_amt,"AMOUNT":_fr})
    if _cd in ('70','71','72','73','74'):
        _b80_rows.append({"BNMCODE":"8051070000000Y","AMTIND":_amt,"AMOUNT":_fr})
        _b80_rows.append({"BNMCODE":"8050070000000Y","AMTIND":_amt,"AMOUNT":_fr})
    if _cd == '77':
        _b80_rows.append({"BNMCODE":"8051077000000Y","AMTIND":_amt,"AMOUNT":_fr})
        _b80_rows.append({"BNMCODE":"8050077000000Y","AMTIND":_amt,"AMOUNT":_fr})
    if _cd == '78':
        _b80_rows.append({"BNMCODE":"8051078000000Y","AMTIND":_amt,"AMOUNT":_fr})
        _b80_rows.append({"BNMCODE":"8050078000000Y","AMTIND":_amt,"AMOUNT":_fr})
    if _cd == '79':
        _b80_rows.append({"BNMCODE":"8051079000000Y","AMTIND":_amt,"AMOUNT":_fr})
        _b80_rows.append({"BNMCODE":"8050079000000Y","AMTIND":_amt,"AMOUNT":_fr})
    if _cd in ('80','81','82','83','84','85','86','87','88','89','90','91','92','95','96','98','99'):
        _b80_rows.append({"BNMCODE":"8051080000000Y","AMTIND":_amt,"AMOUNT":_fr})
        _b80_rows.append({"BNMCODE":"8050080000000Y","AMTIND":_amt,"AMOUNT":_fr})

# _TYPE_=6 (LOANSIZE only, all records)
for _r in _alm_b80_tot6.to_dicts():
    _ls  = _r["LOANSIZE"]
    _amt = _r["AMTIND"]
    _fr  = _r["_FREQ_"] * 1000
    _b80_rows.append({"BNMCODE": f"{_ls}00000000Y", "AMTIND": _amt, "AMOUNT": _fr})

# _TYPE_=7 (LOANSIZE + CUSTCD, all records)
for _r in _alm_b80_all.to_dicts():
    _ls  = _r["LOANSIZE"]
    _cd  = str(_r.get("CUSTCD") or "").strip()
    _amt = _r["AMTIND"]
    _fr  = _r["_FREQ_"] * 1000
    if _cd == '77':
        _b80_rows.append({"BNMCODE": f"{_ls}77000000Y", "AMTIND": _amt, "AMOUNT": _fr})
    if _cd in ('61','41','42','43'):
        _b80_rows.append({"BNMCODE": f"{_ls}61000000Y", "AMTIND": _amt, "AMOUNT": _fr})
    if _cd in ('41','42','43','44','46','47','48','49','51','52','53','54'):
        _b80_rows.append({"BNMCODE": f"{_ls}{_cd}000000Y", "AMTIND": _amt, "AMOUNT": _fr})

if _b80_rows:
    _append_lalm(pl.from_dicts(_b80_rows, schema=LALM_SCHEMA))

# ============================================================================
# SMR A330 - NO OF UTILISED / UNUTILISED CURRENT ACCOUNTS
# The PROC APPEND blocks for CURROD and CURRUOD are commented out
# in the original SAS program; retained here as reference only.
# ============================================================================

# DATA CURROD CURRUOD:
#   current = _load(CURRENT_PQ)
#   current = current.filter(
#       ~pl.col("OPENIND").is_in(["B","C","P"]) & (pl.col("APPRLIMT") > 1)
#   ).with_columns([
#       pl.when(pl.col("PRODUCT").is_between(160,165))
#         .then(pl.lit("I")).otherwise(pl.lit("D")).alias("AMTIND"),
#       pl.col("CUSTCODE")
#         .map_elements(format_odcustcd, return_dtype=pl.String).alias("CUSTCD"),
#   ]).with_columns([
#       pl.when(pl.col("PRODUCT")==104).then(pl.lit("02")).otherwise(pl.col("CUSTCD")).alias("CUSTCD"),
#       pl.when(pl.col("PRODUCT")==105).then(pl.lit("81")).otherwise(pl.col("CUSTCD")).alias("CUSTCD"),
#   ])
#   curruod = current.filter(pl.col("CURBAL") >= 0)   # UNUTILISED
#   currod  = current.filter(pl.col("CURBAL") <  0)   # UTILISED
#
# NO OF UNUTILISED CURRENT ACCOUNTS (commented out in original SAS):
#   alm = curruod.group_by(["AMTIND"]).agg(pl.len().alias("AMOUNT"))
#   alm = alm.with_columns([
#       (pl.col("AMOUNT") * 1000).alias("AMOUNT"),
#       pl.lit("8020000000000Y").alias("BNMCODE"),
#   ])
#   _append_lalm(alm)
#
# NO OF UTILISED CURRENT ACCOUNTS (commented out in original SAS):
#   alm_currod = currod.group_by(["AMTIND","CUSTCD"]).agg(pl.len().alias("_FREQ_"))
#   _currod_rows = []
#   for _r in alm_currod.to_dicts():
#       _cd  = str(_r.get("CUSTCD") or "").strip()
#       _amt = _r["AMTIND"]
#       _fr  = _r["_FREQ_"] * 1000
#       if _cd in ('10','02','03','11','12'):
#           _currod_rows.append({"BNMCODE":"8051010000000Y","AMTIND":_amt,"AMOUNT":_fr})
#       elif _cd in ('20','13','17','30','32','33','34','35','36','37','38','39',
#                    '40','04','05','06','46','47'):
#           _currod_rows.append({"BNMCODE":"8051020000000Y","AMTIND":_amt,"AMOUNT":_fr})
#       elif _cd in ('62','44','46','47'):
#           _currod_rows.append({"BNMCODE":"8051062000000Y","AMTIND":_amt,"AMOUNT":_fr})
#       elif _cd in ('63','48','49','51'):
#           _currod_rows.append({"BNMCODE":"8051063000000Y","AMTIND":_amt,"AMOUNT":_fr})
#       elif _cd in ('64','52','53','54','75','57','59'):
#           _currod_rows.append({"BNMCODE":"8051064000000Y","AMTIND":_amt,"AMOUNT":_fr})
#       elif _cd in ('70','71','72','73','74'):
#           _currod_rows.append({"BNMCODE":"8051070000000Y","AMTIND":_amt,"AMOUNT":_fr})
#       elif _cd == '78':
#           _currod_rows.append({"BNMCODE":"8051078000000Y","AMTIND":_amt,"AMOUNT":_fr})
#       elif _cd == '79':
#           _currod_rows.append({"BNMCODE":"8051079000000Y","AMTIND":_amt,"AMOUNT":_fr})
#       elif _cd in ('80','81','82','83','84','85','86','90','91','92','95','96','98','99'):
#           _currod_rows.append({"BNMCODE":"8051080000000Y","AMTIND":_amt,"AMOUNT":_fr})
#   if _currod_rows:
#       _append_lalm(pl.from_dicts(_currod_rows, schema=LALM_SCHEMA))
#   (PROC APPEND DATA=ALMLOAN BASE=BNM.LALM is commented out in original SAS)

# ============================================================================
# LOAN - BY CUSTOMER CODE AND BY PURPOSE CODE
# ============================================================================

_alm_purp = (
    loan
    .filter(pl.col("PRODCD").str.slice(0, 2) == "34")
    .group_by(["AMTIND", "CUSTCD", "FISSPURP"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("AMOUNT"))
)
# DATA ALM1: expand 0210-0230 to 0200
_alm_purp = pl.concat([
    _alm_purp,
    _alm_purp
    .filter(pl.col("FISSPURP").is_in(["0220","0230","0210","0211","0212"]))
    .with_columns(pl.lit("0200").alias("FISSPURP")),
], how="diagonal")

_purp_rows: list[dict] = []
for _r in _alm_purp.to_dicts():
    _cd = str(_r.get("CUSTCD") or "").strip()
    _fp = str(_r.get("FISSPURP") or "").strip()
    _amt = _r["AMTIND"]
    _a   = _r["AMOUNT"]
    # _TYPE_=7 only (CUSTCD present)
    if not _cd:
        continue
    if _cd == '79':
        _purp_rows.append({"BNMCODE":f"34000{_cd}00{_fp}Y","AMTIND":_amt,"AMOUNT":_a})
    elif _cd in ('10','02','03','11','12'):
        _purp_rows.append({"BNMCODE":f"340001000{_fp}Y","AMTIND":_amt,"AMOUNT":_a})
    elif _cd in ('20','13','17','30','31','32','33','34','35','37','38','39','40',
                 '04','05','40','45','06'):
        if _cd in ('04','05','06'):
            _purp_rows.append({"BNMCODE":f"34000{_cd}00{_fp}Y","AMTIND":_amt,"AMOUNT":_a})
        _purp_rows.append({"BNMCODE":f"340002000{_fp}Y","AMTIND":_amt,"AMOUNT":_a})
    elif _cd in ('61','41','42','43'):
        _purp_rows.append({"BNMCODE":f"340006100{_fp}Y","AMTIND":_amt,"AMOUNT":_a})
    elif _cd in ('62','44','46','47'):
        _purp_rows.append({"BNMCODE":f"340006200{_fp}Y","AMTIND":_amt,"AMOUNT":_a})
    elif _cd in ('63','48','49','51'):
        _purp_rows.append({"BNMCODE":f"340006300{_fp}Y","AMTIND":_amt,"AMOUNT":_a})
    elif _cd in ('64','57','75','59','52','53','54'):
        _purp_rows.append({"BNMCODE":f"340006400{_fp}Y","AMTIND":_amt,"AMOUNT":_a})
    elif _cd in ('71','72','73','74'):
        _purp_rows.append({"BNMCODE":f"340007000{_fp}Y","AMTIND":_amt,"AMOUNT":_a})
    if '81' <= _cd <= '99':
        # SAS: '3400080'||'00'||FISSPURP||'Y'  (9+4+1 = 14 chars)
        _purp_rows.append({"BNMCODE":f"340008000{_fp}Y","AMTIND":_amt,"AMOUNT":_a})
    if _cd in ('77','78'):
        _purp_rows.append({"BNMCODE":f"34000{_cd}00{_fp}Y","AMTIND":_amt,"AMOUNT":_a})
        # SAS: '3400076'||'00'||FISSPURP||'Y'
        _purp_rows.append({"BNMCODE":f"340007600{_fp}Y","AMTIND":_amt,"AMOUNT":_a})
    elif _cd in ('41','42','43','44','46','47','48','49','51','52','53','54'):
        _purp_rows.append({"BNMCODE":f"34000{_cd}00{_fp}Y","AMTIND":_amt,"AMOUNT":_a})
if _purp_rows:
    _append_lalm(pl.from_dicts(_purp_rows, schema=LALM_SCHEMA))

# ============================================================================
# LOAN - BY CUSTOMER CODE AND BY SECTOR CODE
# ============================================================================

_alm_sc_raw = (
    loan
    .filter(
        (pl.col("PRODCD").str.slice(0, 2) == "34") &
        (pl.col("SECTORCD") != "0410")
    )
    .group_by(["AMTIND", "CUSTCD", "SECTORCD"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("AMOUNT"))
)
_alm_sc_exp = _full_sector_expand(_alm_sc_raw)

_sc_rows: list[dict] = []
for _r in _alm_sc_exp.to_dicts():
    _cd  = str(_r.get("CUSTCD") or "").strip()
    _sc  = str(_r.get("SECTORCD") or "").strip()
    _amt = _r["AMTIND"]
    _a   = _r["AMOUNT"]
    if _cd in ('41','42','43','44','46','47','48','49','51','52','53','54'):
        _sc_rows.append({"BNMCODE":f"34000{_cd}00{_sc}Y","AMTIND":_amt,"AMOUNT":_a})
    if _cd in ('61','41','42','43'):
        _sc_rows.append({"BNMCODE":f"340006100{_sc}Y","AMTIND":_amt,"AMOUNT":_a})
    elif _cd in ('62','44','46','47'):
        _sc_rows.append({"BNMCODE":f"340006200{_sc}Y","AMTIND":_amt,"AMOUNT":_a})
    elif _cd in ('63','48','49','51'):
        _sc_rows.append({"BNMCODE":f"340006300{_sc}Y","AMTIND":_amt,"AMOUNT":_a})
    elif _cd in ('64','57','75','59','52','53','54'):
        _sc_rows.append({"BNMCODE":f"340006400{_sc}Y","AMTIND":_amt,"AMOUNT":_a})
    elif _cd in ('71','72','73','74'):
        _sc_rows.append({"BNMCODE":f"340007000{_sc}Y","AMTIND":_amt,"AMOUNT":_a})
    elif _cd == '79':
        _sc_rows.append({"BNMCODE":f"340007900{_sc}Y","AMTIND":_amt,"AMOUNT":_a})
    elif _cd in ('20','13','17','30','31','32','33','34','35','37','38','39',
                 '40','04','05','40','45','06'):
        if _cd in ('04','05','06'):
            _sc_rows.append({"BNMCODE":f"34000{_cd}00{_sc}Y","AMTIND":_amt,"AMOUNT":_a})
        _sc_rows.append({"BNMCODE":f"340002000{_sc}Y","AMTIND":_amt,"AMOUNT":_a})
    elif _cd in ('10','02','03','11','12'):
        _sc_rows.append({"BNMCODE":f"340001000{_sc}Y","AMTIND":_amt,"AMOUNT":_a})
    if '81' <= _cd <= '99':
        # SAS: '3400080'||'00'||SECTORCD||'Y'
        _sc_rows.append({"BNMCODE":f"340008000{_sc}Y","AMTIND":_amt,"AMOUNT":_a})
    if _cd in ('95','96') and _sc == '9700':
        _sc_rows.append({"BNMCODE":"3400085009700Y","AMTIND":_amt,"AMOUNT":_a})
    if _cd in ('85','86','87','88','89','90','91','92','98','99'):
        if _sc in ('1000','2000','3000','4000','5000','6000','7000','8000','9000','9999'):
            _sc_rows.append({"BNMCODE":"3400085009999Y","AMTIND":_amt,"AMOUNT":_a})
    if _cd in ('95','96'):
        if _sc in ('1000','2000','3000','4000','5000','6000','7000','8000','9000','9999','9700'):
            _sc_rows.append({"BNMCODE":"3400095000000Y","AMTIND":_amt,"AMOUNT":_a})
        # SAS: '3400095'||'00'||SECTORCD||'Y'
        _sc_rows.append({"BNMCODE":f"340009500{_sc}Y","AMTIND":_amt,"AMOUNT":_a})
    elif _cd in ('77','78'):
        # SAS: '3400076'||'00'||SECTORCD||'Y'
        _sc_rows.append({"BNMCODE":f"340007600{_sc}Y","AMTIND":_amt,"AMOUNT":_a})
        _sc_rows.append({"BNMCODE":f"34000{_cd}00{_sc}Y","AMTIND":_amt,"AMOUNT":_a})
if _sc_rows:
    _append_lalm(pl.from_dicts(_sc_rows, schema=LALM_SCHEMA))

# ============================================================================
# LOAN - SME BY CUSTOMER CODE AND BY STATE CODE
# ============================================================================

_sme_custcds = ('41','42','43','44','46','47','48','49','51','52','53','54')
_alm_sme = (
    loan
    .filter(
        (pl.col("PRODCD").str.slice(0, 2) == "34") &
        pl.col("CUSTCD").is_in(_sme_custcds)
    )
    .group_by(["CUSTCD", "STATECD", "AMTIND"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("AMOUNT"))
)
_append_lalm(
    _alm_sme
    .filter(pl.col("STATECD").is_not_null())   # _TYPE_=7 only
    .with_columns(
        (pl.lit("34000") + pl.col("CUSTCD") +
         pl.lit("000000") + pl.col("STATECD")).alias("BNMCODE")
    )
)

# ============================================================================
# RM LOANS & FLOOR STOCKING
# ============================================================================

_alm_rm = (
    loan
    .filter(pl.col("PRODCD").str.slice(0, 3).is_in(["341","342","343","344"]))
    .group_by(["AMTIND", "PRODCD"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("AMOUNT"))
    .filter(pl.col("PRODCD").is_in(["34190","34230","34299","34170"]))
    .with_columns(
        (pl.col("PRODCD") + pl.lit("00000000Y")).alias("BNMCODE")
    )
)
_append_lalm(_alm_rm)

# ============================================================================
# RM LOANS - OVERDRAFT LOANS BY CUSTOMER CODE
# ============================================================================

_alm_od = (
    loan
    .filter(pl.col("PRODCD").is_in(["34180","34240"]))
    .group_by(["AMTIND", "PRODCD"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("AMOUNT"))
    .with_columns(
        pl.when(pl.col("PRODCD") == "34240")
          .then(pl.lit("3424000000000Y"))
          .otherwise(pl.lit("3418000000000Y"))
          .alias("BNMCODE")
    )
)
_append_lalm(_alm_od)

# ============================================================================
# RM LOANS - BY CUSTOMER AND MATURITY CODE
# ============================================================================

_alm_mat = (
    loan
    .filter(pl.col("PRODCD").str.slice(0, 3).is_in(["341","342","343","344"]))
    .group_by(["AMTIND", "ORIGMT", "CUSTCD"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("AMOUNT"))
)

_mat_short = ('10','12','13','14','15','16','17')
_mat_long  = ('20','21','22','23','24','25','26','30','31','32','33')
_mat_rows: list[dict] = []
for _r in _alm_mat.to_dicts():
    _mt  = str(_r.get("ORIGMT") or "").strip()
    _cd  = str(_r.get("CUSTCD") or "").strip()
    _amt = _r["AMTIND"]
    _a   = _r["AMOUNT"]
    if _mt in _mat_short:
        if _cd in ('81','82','83','84'):
            _mat_rows.append({"BNMCODE":"3410081100000Y","AMTIND":_amt,"AMOUNT":_a})
        elif _cd in ('85','86','87','88','89','90','91','92','95','96','98','99'):
            _mat_rows.append({"BNMCODE":"3410085100000Y","AMTIND":_amt,"AMOUNT":_a})
    if _mt in _mat_long:
        if _cd == '81':
            _mat_rows.append({"BNMCODE":"3410081200000Y","AMTIND":_amt,"AMOUNT":_a})
        elif _cd in ('85','86','87','88','89','90','91','92','95','96','98','99'):
            _mat_rows.append({"BNMCODE":"3410085200000Y","AMTIND":_amt,"AMOUNT":_a})
if _mat_rows:
    _append_lalm(pl.from_dicts(_mat_rows, schema=LALM_SCHEMA))

# ============================================================================
# RM TERM LOAN - BY CUSTOMER AND SECTORAL CODE (purpose code variant)
# ============================================================================

_rm_prodcds = ('34111','34112','34113','34114','34115','34116','34117','34120','34149')

_alm_rmt6 = (
    loan
    .filter(pl.col("PRODCD").is_in(_rm_prodcds))
    .group_by(["AMTIND", "PRODCD"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("AMOUNT"))
    .with_columns(
        (pl.col("PRODCD") + pl.lit("00000000Y")).alias("BNMCODE")
    )
)
_append_lalm(_alm_rmt6)

_alm_rmt7 = (
    loan
    .filter(pl.col("PRODCD").is_in(_rm_prodcds))
    .group_by(["AMTIND", "PRODCD", "FISSPURP"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("AMOUNT"))
)
_rmt_rows: list[dict] = []
for _r in _alm_rmt7.to_dicts():
    _pc  = str(_r.get("PRODCD") or "").strip()
    _fp  = str(_r.get("FISSPURP") or "").strip()
    _amt = _r["AMTIND"]
    _a   = _r["AMOUNT"]
    if _pc == '34111':
        if _fp[:3] == '043':
            _rmt_rows.append({"BNMCODE":"3411100000430Y","AMTIND":_amt,"AMOUNT":_a})
        if _fp[:3] == '021':
            _rmt_rows.append({"BNMCODE":"3411100000210Y","AMTIND":_amt,"AMOUNT":_a})
if _rmt_rows:
    _append_lalm(pl.from_dicts(_rmt_rows, schema=LALM_SCHEMA))

# ============================================================================
# RM TERM LOAN - BY ORIGINAL MATURITY
# ============================================================================

_alm_origmt = (
    loan
    .filter(pl.col("PRODCD").is_in(_rm_prodcds))
    .group_by(["AMTIND", "ORIGMT"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("AMOUNT"))
)
_origmt_rows: list[dict] = []
for _r in _alm_origmt.to_dicts():
    _mt  = str(_r.get("ORIGMT") or "").strip()
    _amt = _r["AMTIND"]
    _a   = _r["AMOUNT"]
    if _mt in ('10','12','13','14','15','16','17'):
        _origmt_rows.append({"BNMCODE":"3411000100000Y","AMTIND":_amt,"AMOUNT":_a})
    elif _mt in ('21','22','23','24','25','26','31','32','33'):
        _origmt_rows.append({"BNMCODE":f"3411000{_mt}0000Y","AMTIND":_amt,"AMOUNT":_a})
if _origmt_rows:
    _append_lalm(pl.from_dicts(_origmt_rows, schema=LALM_SCHEMA))

# ============================================================================
# RM TERM LOAN - BY REMAINING MATURITY
# ============================================================================

_alm_remmt = (
    loan
    .filter(pl.col("PRODCD").is_in(_rm_prodcds))
    .group_by(["AMTIND", "REMAINMT"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("AMOUNT"))
)
_remmt_rows: list[dict] = []
for _r in _alm_remmt.to_dicts():
    _mt  = str(_r.get("REMAINMT") or "").strip()
    _amt = _r["AMTIND"]
    _a   = _r["AMOUNT"]
    if _mt in ('51','52','53','54','55','56','57'):
        _remmt_rows.append({"BNMCODE":"3411000500000Y","AMTIND":_amt,"AMOUNT":_a})
    elif _mt in ('61','62','63','64','71','72','73'):
        _remmt_rows.append({"BNMCODE":f"3411000{_mt}0000Y","AMTIND":_amt,"AMOUNT":_a})
if _remmt_rows:
    _append_lalm(pl.from_dicts(_remmt_rows, schema=LALM_SCHEMA))

# ============================================================================
# GROSS LOAN - TOTAL APPROVED LIMIT  (UTILISED + UNUTILISED)
# ============================================================================

_alm_appr = (
    _load(LOAN_PQ)
    .filter(
        ~pl.col("PRODUCT").is_in([150,151,152,181]) &
        (
            (pl.col("PRODCD").str.slice(0, 2) == "34") |
            (pl.col("PRODCD") == "54120") |
            (pl.col("PRODUCT") == 972)
        ) &
        ~pl.col("PAIDIND").is_in(["P","C"])
    )
    .group_by(["AMTIND"])
    .agg(pl.col("APPRLIM2").sum().alias("AMOUNT"))
)
_ualm_appr = (
    uloan
    .filter(
        ~pl.col("PRODUCT").is_in([150,151,152,181]) &
        (
            (pl.col("PRODCD").str.slice(0, 2) == "34") |
            (pl.col("PRODCD") == "54120") |
            (pl.col("PRODUCT") == 972)
        )
    )
    .group_by(["AMTIND"])
    .agg(pl.col("APPRLIMT").sum().alias("AMOUNT"))
)
_append_lalm(
    pl.concat([_alm_appr, _ualm_appr], how="diagonal")
    .with_columns(pl.lit("8150000000000Y").alias("BNMCODE"))
)

# ============================================================================
# GROSS LOAN - NO OF UNUTILISED LOAN ACCOUNTS (B80200)
# ============================================================================

_tlu = (
    loan.sort(["ACCTNO","NOTENO"])
    .filter(
        ~(
            ((pl.col("ACCTNO").is_between(2500000000, 2599999999)) &
             (pl.col("NOTENO").is_between(40000, 49999))) |
            (pl.col("PRODUCT") == 321)
        )
    )
    .join(cl_fee, on=["ACCTNO","NOTENO"], how="left")
    .with_columns(
        (pl.col("DUETOTAL").fill_null(0.0) * pl.col("FORATE")).alias("CLFEE")
    )
    .drop(["DUETOTAL","FEEPLAN"])
    .sort(["ACCTNO","COMMNO"])
    .join(m_lncomm, on=["ACCTNO","COMMNO"], how="left")
    .filter(
        ~(
            pl.col("PRODUCT").is_in([150,151,152,181,30,31,32,33,34]) &
            (pl.col("ACCTYPE") == "OD")
        ) &
        (
            (pl.col("PRODCD").str.slice(0, 2) == "34") |
            (pl.col("PRODCD") == "54120") |
            (pl.col("PRODUCT") == 972)
        ) &
        (pl.col("RLEASAMT") == 0)
    )
    .with_columns(
        pl.when(
            (pl.col("ACCTYPE") == "LN") &
            (
                (pl.col("BALANCE") == pl.col("CJFEE")) |
                (pl.col("BALANCE") == pl.col("CLFEE"))
            )
        ).then(1).otherwise(0).alias("NODELETE")
    )
    .filter(
        ~((pl.col("ACCTYPE") == "LN") & (pl.col("COMMNO") > 0) &
          (pl.col("CUSEDAMT") > 0) & (pl.col("NODELETE") == 0))
    )
    .filter(
        ~((pl.col("ACCTYPE") == "LN") &
          pl.col("PRODUCT").is_between(600,699) &
          (pl.col("NODELETE") == 0))
    )
)

# Save BNM.B80200
_tlu.write_csv(B80200_TXT)

_tuu = (
    uloan.sort(["ACCTNO","COMMNO"])
    .join(m_lncomm, on=["ACCTNO","COMMNO"], how="left")
    .join(
        m_lnnote.rename({"BALANCE":"BALANCE_NOTE","CJFEE":"CJFEE_NOTE"}),
        on=["ACCTNO","COMMNO"], how="left"
    )
    .filter(
        ~pl.col("PRODUCT").is_in([150,151,152,181]) &
        (
            (pl.col("PRODCD").str.slice(0, 2) == "34") |
            (pl.col("PRODCD") == "54120") |
            (pl.col("PRODUCT") == 972)
        ) &
        (pl.col("RLEASAMT") == 0)
    )
    .filter(
        ~((pl.col("ACCTYPE") == "LN") & (pl.col("COMMNO") > 0) &
          (pl.col("CUSEDAMT") > 0))
    )
    .filter(
        ~((pl.col("ACCTYPE") == "LN") & pl.col("PRODUCT").is_between(600,699))
    )
)

_alm_u  = _tlu.group_by(["AMTIND","CUSTCD"]).agg(pl.len().alias("_FREQ_"))
_ualm_u = _tuu.group_by(["AMTIND","CUSTCD"]).agg(pl.len().alias("_FREQ_"))

_un_rows: list[dict] = []
for _r in pl.concat([_alm_u, _ualm_u], how="diagonal").to_dicts():
    _cd  = str(_r.get("CUSTCD") or "").strip()
    _amt = _r["AMTIND"]
    _fr  = _r["_FREQ_"] * 1000
    if _amt in ("D","F"):
        _un_rows.append({"BNMCODE":"8020000000000Y","AMTIND":_amt,"AMOUNT":_fr})
    if _cd in ('61','41','42','43'):
        _un_rows.append({"BNMCODE":"8020061000000Y","AMTIND":_amt,"AMOUNT":_fr})
    if _cd in ('41','42','43','44','46','47','48','49','51','52','53','54'):
        _un_rows.append({"BNMCODE":"8020065000000Y","AMTIND":_amt,"AMOUNT":_fr})
        _un_rows.append({"BNMCODE":f"80500{_cd}000000Y","AMTIND":_amt,"AMOUNT":_fr})
    if _cd in ('70','71','72','73','74'):
        _un_rows.append({"BNMCODE":"8050070000000Y","AMTIND":_amt,"AMOUNT":_fr})
    if _cd in ('77','78','79'):
        _un_rows.append({"BNMCODE":f"80500{_cd}000000Y","AMTIND":_amt,"AMOUNT":_fr})
    if '81' <= _cd <= '99':
        _un_rows.append({"BNMCODE":"8050080000000Y","AMTIND":_amt,"AMOUNT":_fr})
if _un_rows:
    _append_lalm(pl.from_dicts(_un_rows, schema=LALM_SCHEMA))

# Post-remap: 8020065->8050065, 8020061->8050061, 8051065->8050065, 8051061->8050061
# Then re-summarise those two codes and append
_remap_df = (
    pl.concat(_lalm_frames, how="diagonal")
    .with_columns(
        pl.when(pl.col("BNMCODE") == "8020065000000Y").then(pl.lit("8050065000000Y"))
          .when(pl.col("BNMCODE") == "8020061000000Y").then(pl.lit("8050061000000Y"))
          .when(pl.col("BNMCODE") == "8051065000000Y").then(pl.lit("8050065000000Y"))
          .when(pl.col("BNMCODE") == "8051061000000Y").then(pl.lit("8050061000000Y"))
          .otherwise(pl.col("BNMCODE")).alias("BNMCODE")
    )
    .filter(pl.col("BNMCODE").is_in(["8050065000000Y","8050061000000Y"]))
    .group_by(["BNMCODE","AMTIND"])
    .agg(pl.col("AMOUNT").sum())
)
_append_lalm(_remap_df)

# ============================================================================
# GROSS LOAN - UNDRAWN PORTION BY ORIGINAL MATURITY  (RC manipulation)
# ============================================================================

_loan_pid = (
    _load(LOAN_PQ)
    .filter(~pl.col("PAIDIND").is_in(["P","C"]))
    .sort(["ACCTNO","NOTENO"])
)
_lncomm_all = _load(LNCOMM_PQ).sort(["ACCTNO","COMMNO"])

# ALMCOM: COMMNO > 0
_almcom   = _loan_pid.filter(pl.col("COMMNO") > 0).sort(["ACCTNO","COMMNO"])
_almnocom = _loan_pid.filter(pl.col("COMMNO") <= 0)

# APPR: merge LNCOMM; for 34190 keep first per ACCTNO+COMMNO
_appr = _almcom.join(_lncomm_all, on=["ACCTNO","COMMNO"], how="left")
_appr_34190 = (_appr.filter(pl.col("PRODCD") == "34190")
               .unique(subset=["ACCTNO","COMMNO"], keep="first"))
_appr_other = _appr.filter(pl.col("PRODCD") != "34190")
_appr = pl.concat([_appr_34190, _appr_other], how="diagonal")

# APPR1: COMMNO <= 0; for 34190 first per ACCTNO+APPRLIM2 plus dups with BALANCE>=APPRLIM2
_almnocom_s = _almnocom.sort(["ACCTNO","APPRLIM2"])
_appr1_34190 = (_almnocom_s.filter(pl.col("PRODCD") == "34190")
                .unique(subset=["ACCTNO","APPRLIM2"], keep="first"))
_dup_34190 = (
    _almnocom_s.filter(pl.col("PRODCD") == "34190")
    .filter(pl.col("ACCTNO").is_duplicated())
    .filter(pl.col("BALANCE") >= pl.col("APPRLIM2"))
)
_appr1_other = _almnocom_s.filter(pl.col("PRODCD") != "34190")
_appr1 = pl.concat([_appr1_34190, _dup_34190, _appr1_other], how="diagonal")
_alm_rc = pl.concat([_appr, _appr1], how="diagonal").sort("ACCTNO")

# UNDRAWN by FISSPURP (_TYPE_=9) and by PRODCD+ORIGMT (_TYPE_=14)
_uw_base = _alm_rc.filter(
    (pl.col("PRODCD").str.slice(0,2) == "34") | (pl.col("PRODCD") == "54120")
)
_uuw_base = uloan.filter(
    (pl.col("PRODCD").str.slice(0,2) == "34") | (pl.col("PRODCD") == "54120")
)

_alm_uw_fp = (
    pl.concat([
        _uw_base.group_by(["AMTIND","FISSPURP"]).agg(pl.col("UNDRAWN").sum().alias("AMOUNT")),
        _uuw_base.group_by(["AMTIND","FISSPURP"]).agg(pl.col("UNDRAWN").sum().alias("AMOUNT")),
    ], how="diagonal")
    .group_by(["AMTIND","FISSPURP"]).agg(pl.col("AMOUNT").sum())
)
_alm_uw_mt = (
    pl.concat([
        _uw_base.group_by(["AMTIND","PRODCD","ORIGMT"]).agg(pl.col("UNDRAWN").sum().alias("AMOUNT")),
        _uuw_base.group_by(["AMTIND","PRODCD","ORIGMT"]).agg(pl.col("UNDRAWN").sum().alias("AMOUNT")),
    ], how="diagonal")
    .group_by(["AMTIND","PRODCD","ORIGMT"]).agg(pl.col("AMOUNT").sum())
)

_uw_rows: list[dict] = []
# _TYPE_=9: FISSPURP
for _r in _alm_uw_fp.to_dicts():
    _fp  = str(_r.get("FISSPURP") or "").strip()
    _amt = _r["AMTIND"]
    _a   = _r["AMOUNT"]
    _uw_rows.append({"BNMCODE":f"562000000{_fp}Y","AMTIND":_amt,"AMOUNT":_a})
    if _fp in ('0220','0230','0210','0211','0212'):
        _uw_rows.append({"BNMCODE":"5620000000200Y","AMTIND":_amt,"AMOUNT":_a})

# _TYPE_=14: PRODCD + ORIGMT
_term_pcd = {'34311','34312','34313','34314','34315','34316','34320','34349',
             '34111','34112','34113','34114','34115','34116','34117','34120','34149'}
_od_pcd   = {'34180','34380'}
for _r in _alm_uw_mt.to_dicts():
    _pc  = str(_r.get("PRODCD") or "").strip()
    _mt  = str(_r.get("ORIGMT") or "").strip()
    _amt = _r["AMTIND"]
    _a   = _r["AMOUNT"]
    _sh  = ('10','12','13','14','15','16','17')
    _lo  = ('20','21','22','23','24','25','26','31','32','33')
    if _pc in _term_pcd:
        if _mt in _sh: _uw_rows.append({"BNMCODE":"5621000100000Y","AMTIND":_amt,"AMOUNT":_a})
        elif _mt in _lo: _uw_rows.append({"BNMCODE":"5621000200000Y","AMTIND":_amt,"AMOUNT":_a})
    elif _pc in _od_pcd:
        if _mt in _sh: _uw_rows.append({"BNMCODE":"5622000100000Y","AMTIND":_amt,"AMOUNT":_a})
    else:
        if _mt in _sh:   _uw_rows.append({"BNMCODE":"5629900100000Y","AMTIND":_amt,"AMOUNT":_a})
        elif _mt in _lo: _uw_rows.append({"BNMCODE":"5629900200000Y","AMTIND":_amt,"AMOUNT":_a})
if _uw_rows:
    _append_lalm(pl.from_dicts(_uw_rows, schema=LALM_SCHEMA))

# Undrawn by SECTORCD
_uw_sc_raw = (
    pl.concat([
        _uw_base.filter(pl.col("SECTORCD") != "0410")
        .group_by(["AMTIND","PRODCD","ORIGMT","SECTORCD"])
        .agg(pl.col("UNDRAWN").sum().alias("AMOUNT")),
        _uuw_base.filter(pl.col("SECTORCD") != "0410")
        .group_by(["AMTIND","PRODCD","ORIGMT","SECTORCD"])
        .agg(pl.col("UNDRAWN").sum().alias("AMOUNT")),
    ], how="diagonal")
)
_uw_sc_exp = _full_sector_expand(_uw_sc_raw)
_append_lalm(
    _uw_sc_exp
    .with_columns(
        pl.when(pl.col("PRODCD") == "34240")
          .then(pl.lit("562990000") + pl.col("SECTORCD") + pl.lit("Y"))
          .otherwise(pl.lit("562000000") + pl.col("SECTORCD") + pl.lit("Y"))
          .alias("BNMCODE")
    )
)

# ============================================================================
# GROSS LOAN - APPLICATION APPROVED DURING THE MONTH  (PROC APPEND commented out)
# ============================================================================
# alm_apprdate computed here for reference; the PROC APPEND is commented
# out in the original SAS program, so no _append_lalm is called.
#
# _alm_apprdt = (
#     _load(LOAN_PQ)
#     .filter(
#         ((pl.col("PRODCD").str.slice(0,2) == "34") | (pl.col("PRODCD") == "54120")) &
#         (pl.col("APPRDATE").dt.month() == RDATE_MONTH) &
#         (pl.col("APPRDATE").dt.year()  == RDATE_YEAR) &
#         ~pl.col("PAIDIND").is_in(["P","C"])
#     )
#     .group_by(["AMTIND","CUSTCD"]).agg(pl.len().alias("_FREQ_"))
# )
# _ualm_apprdt = (
#     uloan
#     .filter(
#         ((pl.col("PRODCD").str.slice(0,2) == "34") | (pl.col("PRODCD") == "54120")) &
#         (pl.col("APPRDATE").dt.month() == RDATE_MONTH) &
#         (pl.col("APPRDATE").dt.year()  == RDATE_YEAR)
#     )
#     .group_by(["AMTIND","CUSTCD"]).agg(pl.len().alias("_FREQ_"))
# )
# (PROC APPEND DATA=ALMLOAN BASE=BNM.LALM is commented out in original SAS)

# ============================================================================
# SPECIAL PURPOSE ITEMS FOR ISLAMIC LOANS (30701-30799)  -- first block
# ============================================================================

_alm_isl1 = (
    loan
    .filter(pl.col("AMTIND") == "I")
    .group_by(["PRODUCT","AMTIND"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("AMOUNT"))
    .filter(
        ~pl.col("PRODUCT").is_in([146,184,159,160,161,169]) &
        ~pl.col("PRODUCT").is_in([124,145,146,184])
    )
)


def _isl_bnm1(product: int) -> str:
    if product == 144:                          return "3070400000000Y"
    if product == 180:                          return "3070700000000Y"
    if product in (108,135,136,182,138):        return "3079900000000Y"
    if product in (181,193):                    return "3070200000000Y"
    if product in (128,130):                    return "3070300000000Y"
    if product in (131,132):                    return "3070300000000Y"
    return "3070100000000Y"


_append_lalm(
    _alm_isl1
    .with_columns(
        pl.col("PRODUCT")
          .map_elements(_isl_bnm1, return_dtype=pl.String)
          .alias("BNMCODE")
    )
)

# ============================================================================
# SPECIAL PURPOSE ITEMS FOR ISLAMIC LOANS -- CENSUS breakdown
# ============================================================================

_alm_cens = (
    loan
    .group_by(["PRODUCT","CENSUS"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("BAL_AFT_EIR"))
    .filter(pl.col("PRODUCT").is_in([146,184,159,160,161,162,94,95]))
    .with_columns([
        pl.lit("3079900000000Y").alias("BNMCODE"),
        pl.lit("I").alias("AMTIND"),
    ])
)


def _cens_bnm(row: dict) -> str:
    p = row.get("PRODUCT")
    c = row.get("CENSUS")
    if p == 146:
        if c == 146.1: return "3070400000000Y"
    if p == 184:
        if c == 184.1: return "3070400000000Y"
    if p == 159:
        if c == 159.00: return "3070100000000Y"
        if c == 159.01: return "3070400000000Y"
    if p == 160:
        if c == 160.00: return "3070100000000Y"
        if c == 160.01: return "3070400000000Y"
    if p == 161:
        if c == 161.00: return "3070100000000Y"
        if c == 161.01: return "3070400000000Y"
    return "3079900000000Y"


_alm_cens = _alm_cens.with_columns(
    pl.struct(_alm_cens.columns)
      .map_elements(_cens_bnm, return_dtype=pl.String)
      .alias("BNMCODE")
)
_append_lalm(
    _alm_cens
    .group_by(["BNMCODE","AMTIND"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("AMOUNT"))
)

# CENSUST breakdown (product 169)
_alm_censust = (
    loan
    .group_by(["PRODUCT","CENSUST"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("BAL_AFT_EIR"))
    .filter(pl.col("PRODUCT") == 169)
    .with_columns([
        pl.lit("3079900000000Y").alias("BNMCODE"),
        pl.lit("I").alias("AMTIND"),
    ])
)


def _censust_bnm(row: dict) -> str:
    ct = row.get("CENSUST")
    if ct in (16901,16905): return "3070100000000Y"
    if ct in (16902,16906): return "3070400000000Y"
    return "3079900000000Y"


_alm_censust = _alm_censust.with_columns(
    pl.struct(_alm_censust.columns)
      .map_elements(_censust_bnm, return_dtype=pl.String)
      .alias("BNMCODE")
)
_append_lalm(
    _alm_censust
    .group_by(["BNMCODE","AMTIND"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("AMOUNT"))
)

# ============================================================================
# GROSS LOANS BY TYPE OF REPRICING
# ============================================================================

# Hardcode A/C 8126505534 to NTINDEX=999
_loan_repr = loan.with_columns(
    pl.when(pl.col("ACCTNO") == 8126505534).then(999)
      .otherwise(pl.col("NTINDEX")).alias("NTINDEX")
).filter(
    (pl.col("PRODCD").str.slice(0,2) == "34") |
    (pl.col("PRODCD").str.slice(0,2) == "54")
)
_loan_od_repr = _loan_repr.filter(pl.col("ACCTYPE") == "OD")
_loan_ln_repr = _loan_repr.filter(pl.col("ACCTYPE") != "OD")

# OD limit table
_limit_od = (
    _load(OVERDFT_PQ)
    .filter((pl.col("LMTAMT") > 1) & (pl.col("APPRLIMT") > 1))
    .with_columns(
        pl.when(pl.col("LMTINDEX").is_in([18,19,38,39])).then(pl.lit("A"))
          .when(pl.col("LMTINDEX").is_in([1,30])).then(pl.lit("B"))
          .when(pl.col("LMTINDEX") == 0).then(pl.lit("C"))
          .otherwise(pl.lit("Z")).alias("LIDX")
    )
    .sort(["ACCTNO","LIDX"])
    .unique(subset=["ACCTNO"], keep="first")
    .with_columns(
        pl.when(pl.col("LIDX") == "B").then(pl.lit("3059500000000Y"))
          .when(pl.col("LIDX") == "C").then(pl.lit("3059300000000Y"))
          .otherwise(pl.lit("3059700000000Y")).alias("BNMCODE")
    )
)

_od_bal = _loan_od_repr.filter(pl.col("BALANCE") != 0)
_od_merged = (
    _od_bal
    .join(_limit_od.select(["ACCTNO","BNMCODE"]), on="ACCTNO", how="left")
    .with_columns(
        pl.when(pl.col("BNMCODE").is_null()).then(pl.lit("3059500000000Y"))
          .otherwise(pl.col("BNMCODE")).alias("BNMCODE")
    )
)
_append_lalm(
    _od_merged
    .group_by(["BNMCODE","AMTIND"])
    .agg(pl.col("BALANCE").sum().alias("AMOUNT"))
)

# Loan repricing BIC derivation
_alm_repr = (
    _loan_ln_repr
    .group_by(["PRODCD","PRODUCT","AMTIND","NTINDEX","COSTFUND","CFINDEX"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("AMOUNT"))
)

_p_cfidx = frozenset([
    300,301,302,304,305,309,310,315,316,320,325,330,335,340,345,349,
    350,351,355,356,357,358,359,360,361,362,363,364,365,390,391,321,322,
    601,602,603,604,605,608,609,631,632,633,634,635,636,637,639,640,641,
    900,901,902,903,904,905,906,907,908,909,910,912,914,915,916,919,920,
    925,950,951,368,918,913,917,922,578,
])
_p_nt_pos = frozenset([213,216,217,218,600,638,911,249,250,253,254,255,256,257,258,259,260])
_p_nt_zer = frozenset([303,306,307,348,354,610,611,308,367,311,313,369,815])
_p_ff     = frozenset([800,801,802,803,804,805,806,807,808,809,810,811,812,813,814,816,817,818])


def _repr_bic(row: dict) -> str:
    pc  = str(row.get("PRODCD") or "").strip()
    p   = int(row.get("PRODUCT") or 0)
    ni  = int(row.get("NTINDEX")  or 0)
    cf  = float(row.get("COSTFUND") or 0)
    cfi = int(row.get("CFINDEX")  or 0)

    bic = format_lnrate(p)

    if pc not in ('34180','34380','34240'):
        if bic == '30593':
            if ni == 1:           bic = '30595'
            elif ni in (38,39):   bic = '30597'
        if bic == '30591':
            if ni > 0:            bic = '30595'
            if ni in (38,39):     bic = '30597'
        if bic == '30596' and cf <= 0:
            if ni > 0:            bic = '30595'
            else:                 bic = '30593'
            if ni in (38,39):     bic = '30597'
        if bic == '30597':
            if ni == 1:           bic = '30595'
            elif ni == 0:         bic = '30593'
            elif ni in (38,39):   bic = '30597'

    if p in (159,160,161,169):
        bic = '30593'
        if ni == 30:              bic = '30595'
        elif ni in (38,39):       bic = '30597'
    if p in (574,606,607,577):
        bic = '30597'
        if ni == 0:               bic = '30593'
        elif ni == 1:             bic = '30595'
        elif ni in (38,39):       bic = '30597'
    if p in (147,148,173,174):
        bic = '30595'
        if p in (147,173) and ni == 0: bic = '30591'
        if p in (148,174) and ni == 0: bic = '30593'
        if p == 174 and cf > 0:        bic = '30596'
    if p in _p_cfidx:
        if cfi == 997:            bic = '30593'
        elif cf > 0:              bic = '30596'
        else:
            if ni > 0:            bic = '30595'
            elif ni == 0:         bic = '30593'
            if ni in (38,39):     bic = '30597'
    if p in _p_nt_pos:
        if ni > 0:                bic = '30595'
        elif ni == 0:             bic = '30591'
        if ni in (38,39):         bic = '30597'
    if p in _p_nt_zer:
        if ni > 0:                bic = '30595'
        elif ni == 0:             bic = '30593'
        if ni in (38,39):         bic = '30597'
    if p in _p_ff:
        bic = '30597'
        if cf > 0:                bic = '30596'
    if cfi in (926,928,929,930,931,932,933): bic = '30597'
    if ni == 52:                  bic = '30597'
    return bic


_alm_repr = _alm_repr.with_columns(
    pl.struct(_alm_repr.columns)
      .map_elements(_repr_bic, return_dtype=pl.String)
      .alias("BIC")
).filter(pl.col("BIC").str.strip_chars() != "")

_append_lalm(
    _alm_repr
    .with_columns((pl.col("BIC") + pl.lit("00000000Y")).alias("BNMCODE"))
    .group_by(["BNMCODE","AMTIND"])
    .agg(pl.col("AMOUNT").sum())
)

# ============================================================================
# LOAN - BY SECTOR CODE
# ============================================================================

_alm_sonly_raw = (
    loan
    .filter(
        (pl.col("PRODCD").str.slice(0,2) == "34") &
        (pl.col("SECTORCD") != "0410")
    )
    .group_by(["SECTORCD","AMTIND"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("AMOUNT"))
)
_alm_sonly_exp = _full_sector_expand(_alm_sonly_raw)
_append_lalm(
    _alm_sonly_exp
    .filter(pl.col("SECTORCD").str.strip_chars() != "")
    .with_columns(
        (pl.lit("340000000") + pl.col("SECTORCD") + pl.lit("Y")).alias("BNMCODE")
    )
)

# ============================================================================
# DISBURSEMENT, REPAYMENT, APPROVAL - BY PURPOSE CODE
# ============================================================================

_lnwof  = _load(LNWOF_PQ).sort(["ACCTNO","NOTENO"])
_lnwod  = _load(LNWOD_PQ).sort(["ACCTNO","NOTENO"])
_plnwof = _load(PLNWOF_PQ).sort(["ACCTNO","NOTENO"])
_plnwod = _load(PLNWOD_PQ).sort(["ACCTNO","NOTENO"])
_loan_pm   = _load(LOAN_PM_PQ)
_sasd_loan = _load(SASD_LOAN_PQ)

_loant = (
    _plnwof
    .join(_plnwod,   on=["ACCTNO","NOTENO"], how="outer", suffix="_PLNWOD")
    .join(_lnwof,    on=["ACCTNO","NOTENO"], how="outer", suffix="_LNWOF")
    .join(_lnwod,    on=["ACCTNO","NOTENO"], how="outer", suffix="_LNWOD")
    .join(_loan_pm,  on=["ACCTNO","NOTENO"], how="outer", suffix="_LPM")
    .join(_sasd_loan,on=["ACCTNO","NOTENO"], how="outer", suffix="_SASD")
    .sort(["ACCTNO","NOTENO"])
)

# DISPAY = DISBURN + REPAYN + EXCLUDE; sum DISBURSE + REPAID10 per ACCTNO NOTENO
_dispay = pl.concat([
    _load(DISBURN_PQ), _load(REPAYN_PQ), _load(EXCLUDE_PQ),
], how="diagonal")
_disburse_agg = (
    _dispay.sort(["ACCTNO","NOTENO"])
    .group_by(["ACCTNO","NOTENO"])
    .agg([pl.col("DISBURSE").sum(), pl.col("REPAID10").sum()])
)

# FX rate table
_forate_raw = (
    _load(FORATEBKP_PQ)
    .filter(pl.col("REPTDATE").cast(pl.Utf8) <= TDATE)
    .sort(["CURCODE", pl.col("REPTDATE").cast(pl.Utf8)], descending=[False, True])
    .unique(subset=["CURCODE"], keep="first")
)
_forate_map: dict = dict(zip(
    _forate_raw["CURCODE"].to_list(),
    _forate_raw["SPOTRATE"].to_list()
))

_loant = (
    _loant.join(_disburse_agg, on=["ACCTNO","NOTENO"], how="inner")
    .with_columns(
        pl.col("CCY").replace(_forate_map).cast(pl.Float64).alias("_FX")
    )
    .with_columns([
        pl.when(
            pl.col("PRODUCT").is_between(800,850) & (pl.col("CCY") != "MYR")
        ).then(pl.col("DISBURSE") * pl.col("_FX")).otherwise(pl.col("DISBURSE")).alias("DISBURSE"),
        pl.when(
            pl.col("PRODUCT").is_between(800,850) & (pl.col("CCY") != "MYR")
        ).then(pl.col("REPAID10") * pl.col("_FX")).otherwise(pl.col("REPAID10")).alias("REPAID10"),
        # SAS: RENAME=(REPAID=REPAID_EAL REPAID10=REPAID) -- original REPAID becomes REPAID_EAL
        pl.col("REPAID").alias("REPAID_EAL"),
    ])
    # REPAID10 becomes the new REPAID after rename
    .rename({"REPAID10": "REPAID"})
    .with_columns([
        pl.when(pl.col("REPAID_EAL") <= 0).then(0.0).otherwise(pl.col("REPAID_EAL")).alias("REPAID_EAL"),
        pl.when(pl.col("REPAID") <= 0).then(0.0).otherwise(pl.col("REPAID")).alias("REPAID"),
    ])
)

# LNHIST + LNNOTE for RATECHG flag
_lnhist_rc = (
    _load(LNHIST_PQ)
    .filter((pl.col("TRANCODE") == 400) & (pl.col("RATECHG") != 0))
    .unique(subset=["ACCTNO","NOTENO"], keep="first")
    .select(["ACCTNO","NOTENO"])
    .with_columns(pl.lit(True).alias("HAS_RC"))
)
_lnnote_delq = _load(LNNOTE_FULL_PQ).select(["ACCTNO","NOTENO","DELQCD"])

# DISPAY filter (for repayment/disbursement data)
_dispay_filt = (
    _loant
    .filter(
        pl.col("PRODCD").str.slice(0,3).is_in(["341","342","343","344","346"]) |
        pl.col("PRODUCT").is_in([225,226,678,679,993,996])
    )
    .select(["ACCTNO","NOTENO","FISSPURP","PRODUCT","BALANCE","PRODCD",
             "CUSTCD","AMTIND","SECTORCD","DISBURSE","REPAID","BRANCH"])
)

# ALM1 (previous month): used for LASTBAL / LASTNOTE comparison (NOTETERM from prev month)
# SAS: PROC SORT DATA=BNM1.LOAN&REPTMON2&NOWK OUT=ALM1 (KEEP=ACCTNO NOTENO FISSPURP PRODUCT
#       NOTETERM BALANCE PRODCD CUSTCD AMTIND SECTORCD BRANCH CCY FORATE)
_alm1_prev = (
    _loant
    .filter(
        pl.col("PRODCD").str.slice(0,3).is_in(["341","342","343","344","346"]) |
        pl.col("PRODUCT").is_in([225,226])
    )
    .select(["ACCTNO","NOTENO","FISSPURP","PRODUCT","NOTETERM","BALANCE","PRODCD",
             "CUSTCD","AMTIND","SECTORCD","BRANCH","CCY","FORATE"])
    .rename({"BALANCE": "LASTBAL", "NOTETERM": "LASTNOTE"})
)

# ALM (current month): used for APPRLIM2, CURBAL, APPRDATE, EARNTERM, RATECHG etc.
# SAS: PROC SORT DATA=BNM1.LOAN&REPTMON&NOWK OUT=ALM (KEEP=...)
_alm_curr = (
    _load(LOAN_PQ)
    .filter(
        pl.col("PRODCD").str.slice(0,3).is_in(["341","342","343","344","346"]) |
        pl.col("PRODUCT").is_in([225,226])
    )
    .select(["ACCTNO","NOTENO","FISSPURP","PRODUCT","NOTETERM","EARNTERM","BALANCE",
             "CURBAL","APPRDATE","APPRLIM2","PRODCD","CUSTCD","AMTIND","SECTORCD",
             "BRANCH","CCY","FORATE"])
    .join(_lnnote_delq, on=["ACCTNO","NOTENO"], how="left")
    .join(_lnhist_rc,   on=["ACCTNO","NOTENO"], how="left")
    .with_columns(
        pl.when(
            pl.col("HAS_RC").is_not_null() &
            (pl.col("DELQCD").str.strip_chars() == "") &
            (pl.col("ISSDTE").cast(pl.Utf8) < SDATE)
        ).then(pl.lit("Y"))
        .when(pl.col("HAS_RC").is_null()).then(pl.lit("N"))
        .otherwise(pl.lit(None)).alias("RATECHG")
    )
)

# ROLLOVER: 34190/34690 with RATECHG='Y'
# SAS: DATA ALM; MERGE ALM1(IN=A RENAME=(BALANCE=LASTBAL NOTETERM=LASTNOTE)) ALM(IN=B);
#       IF PRODCD IN ('34190','34690') & RATECHG='Y' THEN ROLLOVER=CURBAL; ELSE ROLLOVER=0;
#       DISBURSE=0; REPAID=0;
# Then DATA ALM; MERGE ALM(IN=A) DISPAY(IN=B);   -- overlays DISBURSE and REPAID from dispay
# Then DATA ALM; MERGE DISPAY.ROLLOVER(IN=A) ALM(IN=B DROP=ROLLOVER); -- overrides ROLLOVER
_alm_disb = (
    _alm_curr
    .with_columns([
        pl.col("APPRLIM2").fill_null(0.0),
        pl.when(
            pl.col("PRODCD").is_in(["34190","34690"]) & (pl.col("RATECHG") == "Y")
        ).then(pl.col("CURBAL")).otherwise(0.0).alias("ROLLOVER"),
        pl.lit(0.0).alias("DISBURSE"),
        pl.lit(0.0).alias("REPAID"),
    ])
    # Overlay DISBURSE and REPAID from DISPAY (keep all ALM rows)
    .join(_dispay_filt, on=["ACCTNO","NOTENO"], how="left", suffix="_DISP")
    .with_columns([
        # Use dispay values when present
        pl.when(pl.col("DISBURSE_DISP").is_not_null())
          .then(pl.col("DISBURSE_DISP")).otherwise(pl.col("DISBURSE")).alias("DISBURSE"),
        pl.when(pl.col("REPAID_DISP").is_not_null())
          .then(pl.col("REPAID_DISP")).otherwise(pl.col("REPAID")).alias("REPAID"),
    ])
    # Override ROLLOVER from DISPAY.ROLLOVER file (left-hand side drives, DROP=ROLLOVER from ALM)
    .join(_load(ROLLOVER_PQ).rename({"ROLLOVER": "ROLLOVER_FILE"}),
          on=["ACCTNO","NOTENO"], how="left")
    .with_columns(
        pl.when(pl.col("ROLLOVER_FILE").is_not_null())
          .then(pl.col("ROLLOVER_FILE"))
          .otherwise(pl.col("ROLLOVER"))
          .alias("ROLLOVER")
    )
    .filter(
        (pl.col("DISBURSE") > 0) |
        (pl.col("REPAID")   > 0) |
        (pl.col("ROLLOVER") > 0)
    )
)

# Rollover counts
_alm_rc_cnt = (
    _alm_disb.filter(pl.col("ROLLOVER") > 0)
    .group_by(["FISSPURP","CUSTCD","AMTIND"])
    .agg(pl.len().alias("_FREQ_"))
)

# 8015000000000Y - total rollover count
_append_lalm(
    _alm_rc_cnt
    .with_columns([
        (pl.col("_FREQ_") * 1000).alias("AMOUNT"),
        pl.lit("8015000000000Y").alias("BNMCODE"),
    ])
)

# Rollover count by CUSTCD
_rc_custcd_rows: list[dict] = []
_rc_custcd_map = {
    '41':'8015041000000Y','42':'8015042000000Y','43':'8015043000000Y',
    '44':'8015044000000Y','46':'8015046000000Y','47':'8015047000000Y',
    '48':'8015048000000Y','49':'8015049000000Y','51':'8015051000000Y',
    '52':'8015052000000Y','53':'8015053000000Y','54':'8015054000000Y',
    '77':'8015077000000Y',
}
for _r in _alm_rc_cnt.to_dicts():
    _cd  = str(_r.get("CUSTCD") or "").strip()
    _fp  = str(_r.get("FISSPURP") or "").strip()
    _amt = _r["AMTIND"]
    _fr  = _r["_FREQ_"] * 1000
    if _cd in _rc_custcd_map:
        _rc_custcd_rows.append({"BNMCODE":_rc_custcd_map[_cd],"AMTIND":_amt,"AMOUNT":_fr})
    elif _fp == '0211':
        _rc_custcd_rows.append({"BNMCODE":"8020000000211Y","AMTIND":_amt,"AMOUNT":_fr})
    elif _fp == '0212':
        _rc_custcd_rows.append({"BNMCODE":"8020000000212Y","AMTIND":_amt,"AMOUNT":_fr})
    if _cd in ('41','42','43','61'):
        _rc_custcd_rows.append({"BNMCODE":"8015061000000Y","AMTIND":_amt,"AMOUNT":_fr})
if _rc_custcd_rows:
    _append_lalm(pl.from_dicts(_rc_custcd_rows, schema=LALM_SCHEMA))

# Disbursement/repayment/rollover by FISSPURP
_alm_dp = (
    _alm_disb
    .group_by(["FISSPURP","AMTIND"])
    .agg([
        pl.col("DISBURSE").sum(),
        pl.col("REPAID").sum(),
        pl.col("APPRLIM2").sum(),
        pl.col("ROLLOVER").sum(),
    ])
)
# Expand 0210-0230 -> 0200
_alm_dp = pl.concat([
    _alm_dp,
    _alm_dp.filter(pl.col("FISSPURP").is_in(["0220","0230","0210","0211","0212"]))
             .with_columns(pl.lit("0200").alias("FISSPURP")),
], how="diagonal")

_dp_rows: list[dict] = []
for _r in _alm_dp.to_dicts():
    _fp  = str(_r.get("FISSPURP") or "").strip()
    _amt = _r["AMTIND"]
    _dp_rows += [
        {"BNMCODE":f"683400000{_fp}Y","AMTIND":_amt,"AMOUNT":_r["DISBURSE"]},
        {"BNMCODE":f"783400000{_fp}Y","AMTIND":_amt,"AMOUNT":_r["REPAID"]},
        {"BNMCODE":f"821520000{_fp}Y","AMTIND":_amt,"AMOUNT":_r["ROLLOVER"]},
    ]
if _dp_rows:
    _append_lalm(pl.from_dicts(_dp_rows, schema=LALM_SCHEMA))


def _smi_fp_rows(df: pl.DataFrame) -> list[dict]:
    """SMI figure by CUSTCD x FISSPURP."""
    _sme_set  = {'41','42','43','44','46','47','48','49','51','52','53','54','77','78','79'}
    _sme_core = {'41','42','43','44','46','47','48','49','51','52','53','54'}
    _out: list[dict] = []
    for _r in df.to_dicts():
        _cd  = str(_r.get("CUSTCD")   or "").strip()
        _fp  = str(_r.get("FISSPURP") or "").strip()
        _amt = _r["AMTIND"]
        _d   = _r["DISBURSE"]
        _rp  = _r["REPAID"]
        _ro  = _r["ROLLOVER"]
        if _cd in _sme_set:
            _out += [
                {"BNMCODE":f"68340{_cd}00{_fp}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"78340{_cd}00{_fp}Y","AMTIND":_amt,"AMOUNT":_rp},
                {"BNMCODE":f"82152{_cd}00{_fp}Y","AMTIND":_amt,"AMOUNT":_ro},
            ]
        if _cd in ('61','41','42','43','62','44','46','47','64',
                   '63','48','49','51','57','59','75','52','53','54'):
            _out += [
                {"BNMCODE":f"683406000{_fp}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"783406000{_fp}Y","AMTIND":_amt,"AMOUNT":_rp},
            ]
        if _cd in ('02','03','11','12'):
            _out += [
                {"BNMCODE":f"683401000{_fp}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"783401000{_fp}Y","AMTIND":_amt,"AMOUNT":_rp},
                {"BNMCODE":f"821521000{_fp}Y","AMTIND":_amt,"AMOUNT":_ro},
            ]
        if _cd in ('20','13','17','30','32','33','34','35','36','37','38','39','40','04','05','06'):
            _out += [
                {"BNMCODE":f"683402000{_fp}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"783402000{_fp}Y","AMTIND":_amt,"AMOUNT":_rp},
                {"BNMCODE":f"821522000{_fp}Y","AMTIND":_amt,"AMOUNT":_ro},
            ]
        if _cd in ('71','72','73','74'):
            _out += [
                {"BNMCODE":f"683407000{_fp}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"783407000{_fp}Y","AMTIND":_amt,"AMOUNT":_rp},
                {"BNMCODE":f"821527000{_fp}Y","AMTIND":_amt,"AMOUNT":_ro},
            ]
        if _cd in ('77','78'):
            _out += [
                {"BNMCODE":f"683407600{_fp}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"783407600{_fp}Y","AMTIND":_amt,"AMOUNT":_rp},
                {"BNMCODE":f"821527600{_fp}Y","AMTIND":_amt,"AMOUNT":_ro},
            ]
        if _cd in ('81','82','83','84','85','86','87','88','89','90','91',
                   '92','95','96','98','99'):
            _out += [
                {"BNMCODE":f"683408000{_fp}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"783408000{_fp}Y","AMTIND":_amt,"AMOUNT":_rp},
                {"BNMCODE":f"821528000{_fp}Y","AMTIND":_amt,"AMOUNT":_ro},
            ]
        if _cd in ('41','42','43','61'):
            _out += [
                {"BNMCODE":f"683406100{_fp}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"783406100{_fp}Y","AMTIND":_amt,"AMOUNT":_rp},
                {"BNMCODE":f"821526100{_fp}Y","AMTIND":_amt,"AMOUNT":_ro},
            ]
        if _cd in ('62','44','46','47'):
            _out += [
                {"BNMCODE":f"683406200{_fp}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"783406200{_fp}Y","AMTIND":_amt,"AMOUNT":_rp},
                {"BNMCODE":f"821526200{_fp}Y","AMTIND":_amt,"AMOUNT":_ro},
            ]
        if _cd in ('63','48','49','51'):
            _out += [
                {"BNMCODE":f"683406300{_fp}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"783406300{_fp}Y","AMTIND":_amt,"AMOUNT":_rp},
                {"BNMCODE":f"821526300{_fp}Y","AMTIND":_amt,"AMOUNT":_ro},
            ]
        if _cd in ('64','52','53','54','59','75','57'):
            _out += [
                {"BNMCODE":f"683406400{_fp}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"783406400{_fp}Y","AMTIND":_amt,"AMOUNT":_rp},
                {"BNMCODE":f"821526400{_fp}Y","AMTIND":_amt,"AMOUNT":_ro},
            ]
        if _cd in _sme_core:
            _out += [
                {"BNMCODE":f"683406500{_fp}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"783406500{_fp}Y","AMTIND":_amt,"AMOUNT":_rp},
                {"BNMCODE":f"821526500{_fp}Y","AMTIND":_amt,"AMOUNT":_ro},
            ]
    return _out


# SMI by CUSTCD x FISSPURP
_alm_smi_fp = (
    _alm_disb.group_by(["FISSPURP","CUSTCD","AMTIND"])
    .agg([pl.col("DISBURSE").sum(), pl.col("REPAID").sum(),
          pl.col("APPRLIM2").sum(), pl.col("ROLLOVER").sum()])
)
_alm_smi_fp = pl.concat([
    _alm_smi_fp,
    _alm_smi_fp.filter(pl.col("FISSPURP").is_in(["0220","0230","0210","0211","0212"]))
               .with_columns(pl.lit("0200").alias("FISSPURP")),
], how="diagonal")
_smi_fp = _smi_fp_rows(_alm_smi_fp)
if _smi_fp:
    _append_lalm(pl.from_dicts(_smi_fp, schema=LALM_SCHEMA))

# ============================================================================
# DISBURSEMENT, REPAYMENT - BY SECTORIAL CODE
# ============================================================================

_almx_disb = _alm_disb.clone()

_alm_dp_sc_raw = (
    _alm_disb.group_by(["SECTORCD","AMTIND"])
    .agg([pl.col("DISBURSE").sum(), pl.col("REPAID").sum(),
          pl.col("APPRLIM2").sum(), pl.col("ROLLOVER").sum()])
)
_alm_dp_sc_exp = _full_sector_expand(_alm_dp_sc_raw)

_dp_sc_rows: list[dict] = []
for _r in _alm_dp_sc_exp.to_dicts():
    _sc  = str(_r.get("SECTORCD") or "").strip()
    _amt = _r["AMTIND"]
    _dp_sc_rows += [
        {"BNMCODE":f"683400000{_sc}Y","AMTIND":_amt,"AMOUNT":_r["DISBURSE"]},
        {"BNMCODE":f"783400000{_sc}Y","AMTIND":_amt,"AMOUNT":_r["REPAID"]},
        {"BNMCODE":f"821520000{_sc}Y","AMTIND":_amt,"AMOUNT":_r["ROLLOVER"]},
    ]
if _dp_sc_rows:
    _append_lalm(pl.from_dicts(_dp_sc_rows, schema=LALM_SCHEMA))


def _smi_sc_rows(df: pl.DataFrame) -> list[dict]:
    """SMI figure by CUSTCD x SECTORCD."""
    _sme_set  = {'41','42','43','44','46','47','48','49','51','52','53','54','77','78','79'}
    _sme_core = {'41','42','43','44','46','47','48','49','51','52','53','54'}
    _top = {'1000','2000','3000','4000','5000','6000','7000','8000','9000','9999','9700'}
    _out: list[dict] = []
    for _r in df.to_dicts():
        _cd  = str(_r.get("CUSTCD")   or "").strip()
        _sc  = str(_r.get("SECTORCD") or "").strip()
        _amt = _r["AMTIND"]
        _d   = _r["DISBURSE"]
        _rp  = _r["REPAID"]
        _ro  = _r["ROLLOVER"]
        if _cd in _sme_set:
            _out += [
                {"BNMCODE":f"68340{_cd}00{_sc}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"78340{_cd}00{_sc}Y","AMTIND":_amt,"AMOUNT":_rp},
                {"BNMCODE":f"82152{_cd}00{_sc}Y","AMTIND":_amt,"AMOUNT":_ro},
            ]
        if _cd in ('61','41','42','43','62','44','46','47','64',
                   '63','48','49','51','57','59','75','52','53','54'):
            _out += [
                {"BNMCODE":f"683406000{_sc}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"783406000{_sc}Y","AMTIND":_amt,"AMOUNT":_rp},
            ]
        if _cd in ('02','03','11','12'):
            _out += [
                {"BNMCODE":f"683401000{_sc}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"783401000{_sc}Y","AMTIND":_amt,"AMOUNT":_rp},
                {"BNMCODE":f"821521000{_sc}Y","AMTIND":_amt,"AMOUNT":_ro},
            ]
        if _cd in ('20','13','17','30','32','33','34','35','36','37','38','39','40','04','05','06'):
            _out += [
                {"BNMCODE":f"683402000{_sc}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"783402000{_sc}Y","AMTIND":_amt,"AMOUNT":_rp},
                {"BNMCODE":f"821522000{_sc}Y","AMTIND":_amt,"AMOUNT":_ro},
            ]
        if _cd in ('71','72','73','74'):
            _out += [
                {"BNMCODE":f"683407000{_sc}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"783407000{_sc}Y","AMTIND":_amt,"AMOUNT":_rp},
                {"BNMCODE":f"821527000{_sc}Y","AMTIND":_amt,"AMOUNT":_ro},
            ]
        if _cd in ('81','82','83','84','85','86','87','88','89','90','91',
                   '92','95','96','98','99') and _sc != '9999':
            _out += [
                {"BNMCODE":f"683408000{_sc}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"783408000{_sc}Y","AMTIND":_amt,"AMOUNT":_rp},
                {"BNMCODE":f"821528000{_sc}Y","AMTIND":_amt,"AMOUNT":_ro},
                {"BNMCODE":f"683408500{_sc}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"783408500{_sc}Y","AMTIND":_amt,"AMOUNT":_rp},
                {"BNMCODE":f"821528500{_sc}Y","AMTIND":_amt,"AMOUNT":_ro},
            ]
        if _sc in _top:
            if _cd in ('81','82','83','84','85','86','87','88','89','90','91',
                       '92','95','96','98','99'):
                _out += [
                    {"BNMCODE":"6834080000000Y","AMTIND":_amt,"AMOUNT":_d},
                    {"BNMCODE":"7834080000000Y","AMTIND":_amt,"AMOUNT":_rp},
                    {"BNMCODE":"8215280000000Y","AMTIND":_amt,"AMOUNT":_ro},
                    {"BNMCODE":"6834085000000Y","AMTIND":_amt,"AMOUNT":_d},
                    {"BNMCODE":"7834085000000Y","AMTIND":_amt,"AMOUNT":_rp},
                    {"BNMCODE":"8215285000000Y","AMTIND":_amt,"AMOUNT":_ro},
                ]
            if _cd in ('81','82','83','84','85','86','87','88','89','90','91','92','98','99'):
                _out += [
                    {"BNMCODE":"6834085009999Y","AMTIND":_amt,"AMOUNT":_d},
                    {"BNMCODE":"7834085009999Y","AMTIND":_amt,"AMOUNT":_rp},
                    {"BNMCODE":"8215285009999Y","AMTIND":_amt,"AMOUNT":_ro},
                    {"BNMCODE":"6834080009999Y","AMTIND":_amt,"AMOUNT":_d},
                    {"BNMCODE":"7834080009999Y","AMTIND":_amt,"AMOUNT":_rp},
                    {"BNMCODE":"8215280009999Y","AMTIND":_amt,"AMOUNT":_ro},
                ]
            if _cd in ('95','96'):
                _out += [
                    {"BNMCODE":"6834095000000Y","AMTIND":_amt,"AMOUNT":_d},
                    {"BNMCODE":"7834095000000Y","AMTIND":_amt,"AMOUNT":_rp},
                    {"BNMCODE":"8215295000000Y","AMTIND":_amt,"AMOUNT":_ro},
                ]
            if _cd in ('95','96') and _sc != '9999':
                _out += [
                    {"BNMCODE":f"683409500{_sc}Y","AMTIND":_amt,"AMOUNT":_d},
                    {"BNMCODE":f"783409500{_sc}Y","AMTIND":_amt,"AMOUNT":_rp},
                    {"BNMCODE":f"821529500{_sc}Y","AMTIND":_amt,"AMOUNT":_ro},
                ]
            if _cd in ('77','78'):
                _out += [
                    {"BNMCODE":f"683407600{_sc}Y","AMTIND":_amt,"AMOUNT":_d},
                    {"BNMCODE":f"783407600{_sc}Y","AMTIND":_amt,"AMOUNT":_rp},
                    {"BNMCODE":f"821527600{_sc}Y","AMTIND":_amt,"AMOUNT":_ro},
                    {"BNMCODE":"6834076000000Y","AMTIND":_amt,"AMOUNT":_d},
                    {"BNMCODE":"7834076000000Y","AMTIND":_amt,"AMOUNT":_rp},
                    {"BNMCODE":"8215276000000Y","AMTIND":_amt,"AMOUNT":_ro},
                ]
        if _cd in ('41','42','43','61'):
            _out += [
                {"BNMCODE":f"683406100{_sc}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"783406100{_sc}Y","AMTIND":_amt,"AMOUNT":_rp},
                {"BNMCODE":f"821526100{_sc}Y","AMTIND":_amt,"AMOUNT":_ro},
            ]
        if _cd in ('62','44','46','47'):
            _out += [
                {"BNMCODE":f"683406200{_sc}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"783406200{_sc}Y","AMTIND":_amt,"AMOUNT":_rp},
                {"BNMCODE":f"821526200{_sc}Y","AMTIND":_amt,"AMOUNT":_ro},
            ]
        if _cd in ('63','48','49','51'):
            _out += [
                {"BNMCODE":f"683406300{_sc}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"783406300{_sc}Y","AMTIND":_amt,"AMOUNT":_rp},
                {"BNMCODE":f"821526300{_sc}Y","AMTIND":_amt,"AMOUNT":_ro},
            ]
        if _cd in ('64','52','53','54','59','75','57'):
            _out += [
                {"BNMCODE":f"683406400{_sc}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"783406400{_sc}Y","AMTIND":_amt,"AMOUNT":_rp},
                {"BNMCODE":f"821526400{_sc}Y","AMTIND":_amt,"AMOUNT":_ro},
            ]
        if _cd in _sme_core:
            _out += [
                {"BNMCODE":f"683406500{_sc}Y","AMTIND":_amt,"AMOUNT":_d},
                {"BNMCODE":f"783406500{_sc}Y","AMTIND":_amt,"AMOUNT":_rp},
                {"BNMCODE":f"821526500{_sc}Y","AMTIND":_amt,"AMOUNT":_ro},
            ]
    return _out


# SMI by CUSTCD x SECTORCD
_alm_smi_sc_raw = (
    _almx_disb.group_by(["SECTORCD","CUSTCD","AMTIND"])
    .agg([pl.col("DISBURSE").sum(), pl.col("REPAID").sum(),
          pl.col("APPRLIM2").sum(), pl.col("ROLLOVER").sum()])
)
_alm_smi_sc_exp = _full_sector_expand(_alm_smi_sc_raw)
_smi_sc = _smi_sc_rows(_alm_smi_sc_exp)
if _smi_sc:
    _append_lalm(pl.from_dicts(_smi_sc, schema=LALM_SCHEMA))

# ============================================================================
# LOAN - BY STATE CODE AND BY SECTOR CODE - 9700
# ============================================================================

_alq_state_raw = (
    loan
    .filter(
        (pl.col("PRODCD").str.slice(0,2) == "34") &
        (pl.col("SECTORCD") != "0410")
    )
    .group_by(["SECTORCD","STATECD","AMTIND"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("AMOUNT"))
)
_alq_state_exp = _full_sector_expand(_alq_state_raw)
_append_lalm(
    _alq_state_exp
    .filter(pl.col("SECTORCD") == "9700")
    .with_columns(
        (pl.lit("340000000") + pl.col("SECTORCD") + pl.col("STATECD")).alias("BNMCODE")
    )
)

# ============================================================================
# FINAL CONSOLIDATION
# PROC SUMMARY DATA=BNM.LALM NWAY; CLASS BNMCODE AMTIND; VAR AMOUNT; SUM=;
# ============================================================================

lalm_final = (
    pl.concat(_lalm_frames, how="diagonal")
    .group_by(["BNMCODE","AMTIND"])
    .agg(pl.col("AMOUNT").sum())
    .sort(["BNMCODE","AMTIND"])
)

# Write main output file
lalm_final.write_csv(LALM_TXT)

# Print report (OPTIONS NOCENTER NODATE NONUMBER equivalent)
print(SDESC)
print("REPORT ON DOMESTIC ASSETS AND LIABILITIES PART II - M&I LOAN")
print(f"REPORT DATE : {RDATE}")
print()
for _r in lalm_final.to_dicts():
    _a = _r["AMOUNT"]
    print(f"{_r['BNMCODE']:<14}  {_r['AMTIND']:1}  {_a:>25,.2f}")

# ============================================================================
# AGING REPORT FOR FINANCIAL ACCOUNTING
# ============================================================================

_aging_prodcds = ('34111','34112','34113','34114','34115','34116',
                  '34117','34120','34230','34149')

_lnaging_raw = (
    loan
    .filter(pl.col("PRODCD").is_in(_aging_prodcds))
    .group_by(["PRODCD","REMAINMT","AMTIND"])
    .agg(pl.col("BAL_AFT_EIR").sum().alias("AMOUNT"))
)

_aging_rows: list[dict] = []
for _r in _lnaging_raw.to_dicts():
    _pc  = str(_r.get("PRODCD")   or "").strip()
    _mt  = str(_r.get("REMAINMT") or "").strip()
    _amt = _r["AMTIND"]
    _a   = _r["AMOUNT"]
    # LNTYPE derivation
    if _amt == "D":
        _lntype = "S" if _pc == "34230" else "T"   # STAFF / TERM
    else:
        _lntype = "I"                               # SPTF
    # BNMCODE by REMAINMT
    if _mt in ('51','52','53','54','55','56','57'):
        _aging_rows.append({"BNMCODE":"3411000500000Y","AMOUNT":_a,"LNTYPE":_lntype})
    elif _mt in ('61','62','63','64','71','72','73'):
        _aging_rows.append({"BNMCODE":f"3411000{_mt}0000Y","AMOUNT":_a,"LNTYPE":_lntype})

_lnaging = pl.from_dicts(_aging_rows) if _aging_rows else pl.DataFrame(
    schema={"BNMCODE":pl.String,"AMOUNT":pl.Float64,"LNTYPE":pl.String}
)
_lnaging = (
    _lnaging
    .group_by(["BNMCODE","LNTYPE"])
    .agg(pl.col("AMOUNT").sum())
    .sort(["BNMCODE","LNTYPE"])
)

# Write aging output file
_lnaging.write_csv(LNAGING_TXT)

# Print aging sub-reports
for _lt, _label in [
    ("S", "AGING OF STAFF LOANS"),
    ("T", "AGING OF TERM LOANS"),
    ("I", "AGING OF SPTF LOANS"),
]:
    print()
    print("REPORT ID: LALMPBBP")
    print(SDESC)
    print("FINANCIAL ACCOUNTING, FINANCE DIVISION")
    print(f"{_label} AS AT {RDATE}")
    print()
    _subset = _lnaging.filter(pl.col("LNTYPE") == _lt)
    _total  = 0.0
    for _r in _subset.sort("BNMCODE").to_dicts():
        _a = _r["AMOUNT"]
        _total += _a
        print(f"{_r['BNMCODE']:<14}  {_a:>18,.2f}")
    print(f"{'':14}  {_total:>18,.2f}")

# ============================================================================
# CLOSE DUCKDB CONNECTION
# ============================================================================
con.close()
