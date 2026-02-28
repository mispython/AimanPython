# !/usr/bin/env python3
"""
Program: EIICAPS1.py
Purpose: GENERATE CAP REPORT BY COMPANY (Staff Loans)
"""

import duckdb
import polars as pl
import os
from datetime import date

# ─────────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────────
BASE_DIR    = r"C:/data"
LOAN_DIR    = os.path.join(BASE_DIR, "loan")
LN_DIR      = os.path.join(BASE_DIR, "ln")
RATE_DIR    = os.path.join(BASE_DIR, "rate")
CCRIS_DIR   = os.path.join(BASE_DIR, "ccris")
NPL_DIR     = os.path.join(BASE_DIR, "npl")
OUTPUT_DIR  = os.path.join(BASE_DIR, "output")

os.makedirs(NPL_DIR,   exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

OUTPUT1_PATH    = os.path.join(OUTPUT_DIR, "EIICAPS1.txt")
ICAP1_STAFF_PATH = os.path.join(NPL_DIR,  "ICAP1_STAFF.parquet")

con = duckdb.connect()

# ─────────────────────────────────────────────
# REPTDATE – derive REPTMON, NOWK, REPTYEAR
# ─────────────────────────────────────────────
reptdate_df = con.execute(f"""
    SELECT REPTDATE FROM '{LOAN_DIR}/REPTDATE.parquet'
""").pl()

reptdate_val: date = reptdate_df["REPTDATE"][0]
day = reptdate_val.day
if 1 <= day <= 8:
    wk = "1"
elif 9 <= day <= 15:
    wk = "2"
elif 16 <= day <= 22:
    wk = "3"
else:
    wk = "4"

REPTMON  = f"{reptdate_val.month:02d}"
NOWK     = wk
REPTYEAR = str(reptdate_val.year)[-2:]   # YEAR2. → last 2 digits

# ─────────────────────────────────────────────
# RECRATE macro
# ─────────────────────────────────────────────
# TO BE FURNISHED -RATE
recrate_df = con.execute(f"""
    SELECT RECOVERYRATIO FROM '{RATE_DIR}/IRECRATE.parquet'
""").pl()
RECRATE: float = float(recrate_df["RECOVERYRATIO"][0])

# ─────────────────────────────────────────────
# LAYOUT skeleton
# ─────────────────────────────────────────────
layout_df = pl.DataFrame({
    "GROUPIND": ["OTHERS", "IRREGULAR", "REPOSSESSED", "DEFICIT", "TOTAL"]
})

# ─────────────────────────────────────────────
# HP master  (product filter applied on load)
# ─────────────────────────────────────────────
ln_file = os.path.join(LN_DIR, f"ILN{REPTMON}{NOWK}{REPTYEAR}.parquet")
hp_df = con.execute(f"""
    SELECT * FROM '{ln_file}'
    WHERE PRODUCT IN (103,104,107,108)
      AND (DAYARR >= 90
           OR BORSTAT IN ('F','I','R')
           OR USER5 = 'N')
""").pl()

# ─────────────────────────────────────────────
# CREDSUB
# ─────────────────────────────────────────────
ccris_file = os.path.join(CCRIS_DIR, f"ICREDMSUBAC{REPTMON}{REPTYEAR}.parquet")
credsub_df = con.execute(f"""
    SELECT ACCTNUM AS ACCTNO, DAYSARR AS DAYARR, NOTENO
    FROM '{ccris_file}'
    WHERE FACILITY IN ('34331','34332','34371')
""").pl()

# NODUPKEY: sort desc by DAYARR, keep first per ACCTNO+NOTENO
credsub_df = (
    credsub_df
    .sort(["ACCTNO", "NOTENO", "DAYARR"], descending=[False, False, True])
    .unique(subset=["ACCTNO", "NOTENO"], keep="first")
)

# ─────────────────────────────────────────────
# Merge HP ← CREDSUB  (left join on ACCTNO, NOTENO)
# ─────────────────────────────────────────────
hp_df = hp_df.join(credsub_df, on=["ACCTNO", "NOTENO"], how="left")

# ─────────────────────────────────────────────
# Split into arrears buckets
# ─────────────────────────────────────────────
base = hp_df.filter(
    pl.col("PRODUCT").is_in([103, 104, 107, 108]) & (pl.col("BALANCE") > 0)
)

BORSTAT_EXCL = ["F", "I", "R", "E", "W", "Z"]

current_df = base.filter(
    (pl.col("DAYARR") <= 30) &
    (~pl.col("BORSTAT").is_in(BORSTAT_EXCL)) &
    (~pl.col("USER5").is_in(["N"])) &
    (pl.col("PAIDIND") == "M")
)
month1t2_df = base.filter(
    pl.col("DAYARR").is_between(31, 89) &
    (~pl.col("BORSTAT").is_in(BORSTAT_EXCL)) &
    (~pl.col("USER5").is_in(["N"])) &
    (pl.col("PAIDIND") == "M")
)
month3t5_df = base.filter(
    (~pl.col("BORSTAT").is_in(BORSTAT_EXCL)) &
    ((pl.col("USER5").is_in(["N"]) & (pl.col("DAYARR") <= 182)) |
     pl.col("DAYARR").is_between(90, 182)) &
    (pl.col("PAIDIND") == "M")
)
month6t11_df = base.filter(
    (~pl.col("BORSTAT").is_in(BORSTAT_EXCL)) &
    ((pl.col("USER5").is_in(["N"]) & pl.col("DAYARR").is_between(183, 364)) |
     pl.col("DAYARR").is_between(183, 364)) &
    (pl.col("PAIDIND") == "M")
)
month12ab_df = base.filter(
    (~pl.col("BORSTAT").is_in(BORSTAT_EXCL)) &
    ((pl.col("USER5").is_in(["N"]) & (pl.col("DAYARR") >= 365)) |
     (pl.col("DAYARR") >= 365)) &
    (pl.col("PAIDIND") == "M")
)
irregular_df = base.filter(
    (pl.col("BORSTAT") == "I") & (pl.col("PAIDIND") == "M")
)
repossessed_l12_df = base.filter(
    (pl.col("BORSTAT") == "R") & (pl.col("PAIDIND") == "M") & (pl.col("DAYARR") < 365)
)
repossessed_m12_df = base.filter(
    (pl.col("BORSTAT") == "R") & (pl.col("PAIDIND") == "M") & (pl.col("DAYARR") >= 365)
)
deficit_df = base.filter(
    (pl.col("BORSTAT") == "F") & (pl.col("PAIDIND") == "M")
)


# ─────────────────────────────────────────────
# Helper: build aggregated category rows
# ─────────────────────────────────────────────
def build_group(src: pl.DataFrame, subgind: str, categories: list[dict]) -> pl.DataFrame:
    rows = []
    for cfg in categories:
        rate    = cfg["rate"]
        recrate = cfg["recrate"]
        ob      = src["BALANCE"] * (rate / 100)
        er      = ob * (recrate / 100)
        cap     = (ob - er) if cfg["caprovision"] == "ob_minus_er" else pl.Series([0.0] * len(src))

        tmp = pl.DataFrame({
            "GROUPIND":    ["OTHERS"]         * len(src),
            "SUBGIND":     [subgind]          * len(src),
            "CATEGORY":    [cfg["category"]]  * len(src),
            "RATE":        [rate]             * len(src),
            "RECRATE":     [recrate]          * len(src),
            "BALANCE":     src["BALANCE"].to_list(),
            "OBDEFAULT":   ob.to_list(),
            "EXPECTEDREC": er.to_list(),
            "CAPROVISION": cap.to_list(),
        })
        rows.append(tmp)

    df = pl.concat(rows)
    return df.group_by(["GROUPIND", "SUBGIND", "CATEGORY", "RATE", "RECRATE"]).agg(
        pl.col("OBDEFAULT").sum(),
        pl.col("EXPECTEDREC").sum(),
        pl.col("CAPROVISION").sum(),
        pl.col("BALANCE").sum(),
    )


def fix_not_repossessed(grp_df: pl.DataFrame, subgind: str) -> pl.DataFrame:
    """
    Overwrite EXPECTEDREC for 'NOT REPOSSESSED YET' with the sum of
    EXPECTEDREC from '- 3-5 MONTHS IN ARREARS', '- >6-11 MONTHS IN ARREARS',
    '- OTHERS'.
    """
    sub_cats = ["- 3-5 MONTHS IN ARREARS", "- >6-11 MONTHS IN ARREARS", "- OTHERS"]
    summed_er = grp_df.filter(pl.col("CATEGORY").is_in(sub_cats))["EXPECTEDREC"].sum()
    return grp_df.with_columns(
        pl.when(
            (pl.col("CATEGORY") == "NOT REPOSSESSED YET") &
            (pl.col("SUBGIND") == subgind)
        ).then(pl.lit(summed_er)).otherwise(pl.col("EXPECTEDREC")).alias("EXPECTEDREC")
    )


def apply_no_map(df: pl.DataFrame, no_map: dict) -> pl.DataFrame:
    nos, groups, subgroups, bals = [], [], [], []
    for row in df.iter_rows(named=True):
        cat = row["CATEGORY"]
        if cat in no_map:
            no, grp, subgrp = no_map[cat]
        else:
            no, grp, subgrp = None, " ", " "
        nos.append(no)
        groups.append(grp)
        subgroups.append(subgrp)
        bals.append(f"{row['BALANCE']:,.2f}" if grp not in (" ", "") else "")
    return df.with_columns([
        pl.Series("NO",       nos,      dtype=pl.Int64),
        pl.Series("GROUP",    groups),
        pl.Series("SUBGROUP", subgroups),
        pl.Series("BAL",      bals),
    ])


# ─────────────────────────────────────────────
# CURRENT GROUP
# ─────────────────────────────────────────────
current_cats = [
    {"category": "SUCCESSFUL REPOSSESSION",  "rate": 0.31,  "recrate": RECRATE,  "caprovision": "ob_minus_er"},
    {"category": "- 3-5 MONTHS IN ARREARS",  "rate": 0.08,  "recrate": 25.00,    "caprovision": "ob_minus_er"},
    {"category": "NOT REPOSSESSED YET",       "rate": 0.17,  "recrate": 0.0,      "caprovision": "zero"},
    {"category": "- >6-11 MONTHS IN ARREARS", "rate": 0.08,  "recrate": 25.00,    "caprovision": "ob_minus_er"},
    {"category": "- OTHERS",                  "rate": 0.01,  "recrate": 25.00,    "caprovision": "ob_minus_er"},
    {"category": "CONTINUE PAYING",           "rate": 99.52, "recrate": 100.0,    "caprovision": "zero"},
]
current_grp = build_group(current_df, "CURRENT", current_cats)
current_grp = fix_not_repossessed(current_grp, "CURRENT")
current_grp = apply_no_map(current_grp, {
    "CONTINUE PAYING":           (11, "OTHERS", "CURRENT"),
    "SUCCESSFUL REPOSSESSION":   (12, " ", " "),
    "NOT REPOSSESSED YET":       (13, " ", " "),
    "- 3-5 MONTHS IN ARREARS":   (14, " ", " "),
    "- >6-11 MONTHS IN ARREARS": (15, " ", " "),
    "- OTHERS":                  (16, " ", " "),
})

# ─────────────────────────────────────────────
# 1-2 MONTHS
# ─────────────────────────────────────────────
month1t2_cats = [
    {"category": "CONTINUE PAYING",           "rate": 92.73, "recrate": 100.0,    "caprovision": "zero"},
    {"category": "SUCCESSFUL REPOSSESSION",  "rate": 4.26,  "recrate": RECRATE,  "caprovision": "ob_minus_er"},
    {"category": "- 3-5 MONTHS IN ARREARS",  "rate": 0.99,  "recrate": 25.00,    "caprovision": "ob_minus_er"},
    {"category": "NOT REPOSSESSED YET",       "rate": 3.01,  "recrate": 0.0,      "caprovision": "zero"},
    {"category": "- >6-11 MONTHS IN ARREARS", "rate": 1.17,  "recrate": 25.00,    "caprovision": "ob_minus_er"},
    {"category": "- OTHERS",                  "rate": 0.85,  "recrate": 25.00,    "caprovision": "ob_minus_er"},
]
month1t2_grp = build_group(month1t2_df, "1-2 MTHS", month1t2_cats)
month1t2_grp = fix_not_repossessed(month1t2_grp, "1-2 MTHS")
month1t2_grp = apply_no_map(month1t2_grp, {
    "CONTINUE PAYING":           (21, " ", "1-2 MTHS"),
    "SUCCESSFUL REPOSSESSION":   (22, " ", " "),
    "NOT REPOSSESSED YET":       (23, " ", " "),
    "- 3-5 MONTHS IN ARREARS":   (24, " ", " "),
    "- >6-11 MONTHS IN ARREARS": (25, " ", " "),
    "- OTHERS":                  (26, " ", " "),
})

# ─────────────────────────────────────────────
# 3-5 MONTHS
# ─────────────────────────────────────────────
month3t5_cats = [
    {"category": "CONTINUE PAYING",           "rate": 0.00,  "recrate": 100.0,    "caprovision": "zero"},
    {"category": "SUCCESSFUL REPOSSESSION",  "rate": 62.27, "recrate": RECRATE,  "caprovision": "ob_minus_er"},
    {"category": "- 3-5 MONTHS IN ARREARS",  "rate": 6.43,  "recrate": 25.00,    "caprovision": "ob_minus_er"},
    {"category": "NOT REPOSSESSED YET",       "rate": 37.73, "recrate": 0.0,      "caprovision": "zero"},
    {"category": "- >6-11 MONTHS IN ARREARS", "rate": 6.36,  "recrate": 25.00,    "caprovision": "ob_minus_er"},
    {"category": "- OTHERS",                  "rate": 24.94, "recrate": 25.00,    "caprovision": "ob_minus_er"},
]
month3t5_grp = build_group(month3t5_df, "3-5 MTHS", month3t5_cats)
month3t5_grp = fix_not_repossessed(month3t5_grp, "3-5 MTHS")
month3t5_grp = apply_no_map(month3t5_grp, {
    "CONTINUE PAYING":           (31, " ", "3-5 MTHS"),
    "SUCCESSFUL REPOSSESSION":   (32, " ", " "),
    "NOT REPOSSESSED YET":       (33, " ", " "),
    "- 3-5 MONTHS IN ARREARS":   (34, " ", " "),
    "- >6-11 MONTHS IN ARREARS": (35, " ", " "),
    "- OTHERS":                  (36, " ", " "),
})

# ─────────────────────────────────────────────
# 6-11 MONTHS
# ─────────────────────────────────────────────
month6t11_cats = [
    {"category": "CONTINUE PAYING",           "rate": 0.00,  "recrate": 100.0,    "caprovision": "zero"},
    {"category": "SUCCESSFUL REPOSSESSION",  "rate": 34.91, "recrate": RECRATE,  "caprovision": "ob_minus_er"},
    {"category": "- 3-5 MONTHS IN ARREARS",  "rate": 1.85,  "recrate": 25.00,    "caprovision": "ob_minus_er"},
    {"category": "NOT REPOSSESSED YET",       "rate": 65.09, "recrate": 0.0,      "caprovision": "zero"},
    {"category": "- >6-11 MONTHS IN ARREARS", "rate": 2.04,  "recrate": 25.00,    "caprovision": "ob_minus_er"},
    {"category": "- OTHERS",                  "rate": 61.20, "recrate": 25.00,    "caprovision": "ob_minus_er"},
]
month6t11_grp = build_group(month6t11_df, "6-11 MTHS", month6t11_cats)
month6t11_grp = fix_not_repossessed(month6t11_grp, "6-11 MTHS")
month6t11_grp = apply_no_map(month6t11_grp, {
    "CONTINUE PAYING":           (41, " ", "6-11 MTHS"),
    "SUCCESSFUL REPOSSESSION":   (42, " ", " "),
    "NOT REPOSSESSED YET":       (43, " ", " "),
    "- 3-5 MONTHS IN ARREARS":   (44, " ", " "),
    "- >6-11 MONTHS IN ARREARS": (45, " ", " "),
    "- OTHERS":                  (46, " ", " "),
})

# ─────────────────────────────────────────────
# 12 MONTHS AND ABOVE
# ─────────────────────────────────────────────
if len(month12ab_df) > 0:
    m12_ob  = month12ab_df["BALANCE"] * (100.0 / 100)
    m12_er  = m12_ob * (0.0 / 100)
    m12_cap = pl.Series("CAPROVISION", [0.0] * len(month12ab_df))

    month12ab_grp_raw = pl.DataFrame({
        "GROUPIND":    ["OTHERS"]              * len(month12ab_df),
        "SUBGIND":     [">=12 MTHS"]           * len(month12ab_df),
        "CATEGORY":    ["NOT REPOSSESSED YET"] * len(month12ab_df),
        "RATE":        [100.0]                 * len(month12ab_df),
        "RECRATE":     [0.0]                   * len(month12ab_df),
        "BALANCE":     month12ab_df["BALANCE"].to_list(),
        "OBDEFAULT":   m12_ob.to_list(),
        "EXPECTEDREC": m12_er.to_list(),
        "CAPROVISION": m12_cap.to_list(),
    })
    month12ab_grp = month12ab_grp_raw.group_by(
        ["GROUPIND", "SUBGIND", "CATEGORY", "RATE", "RECRATE"]
    ).agg(
        pl.col("OBDEFAULT").sum(),
        pl.col("EXPECTEDREC").sum(),
        pl.col("CAPROVISION").sum(),
        pl.col("BALANCE").sum(),
    )
    bals_m12 = [f"{r['BALANCE']:,.2f}" for r in month12ab_grp.iter_rows(named=True)]
    month12ab_grp = month12ab_grp.with_columns([
        pl.Series("NO",       [51],        dtype=pl.Int64),
        pl.Series("GROUP",    [" "]),
        pl.Series("SUBGROUP", [">=12 MTHS"]),
        pl.Series("BAL",      bals_m12),
    ])
else:
    month12ab_grp = pl.DataFrame(schema={
        "GROUPIND": pl.Utf8, "SUBGIND": pl.Utf8, "CATEGORY": pl.Utf8,
        "RATE": pl.Float64, "RECRATE": pl.Float64,
        "BALANCE": pl.Float64, "OBDEFAULT": pl.Float64,
        "EXPECTEDREC": pl.Float64, "CAPROVISION": pl.Float64,
        "NO": pl.Int64, "GROUP": pl.Utf8, "SUBGROUP": pl.Utf8, "BAL": pl.Utf8,
    })

# ─────────────────────────────────────────────
# IRREGULAR
# ─────────────────────────────────────────────
if len(irregular_df) > 0:
    irr_ob  = irregular_df["BALANCE"]
    irr_er  = irr_ob * (0.0 / 100)
    irr_cap = irr_ob - irr_er

    irregular_grp_raw = pl.DataFrame({
        "GROUPIND":    ["IRREGULAR"] * len(irregular_df),
        "NO":          [61]          * len(irregular_df),
        "RECRATE":     [0.0]         * len(irregular_df),
        "BALANCE":     irregular_df["BALANCE"].to_list(),
        "OBDEFAULT":   irr_ob.to_list(),
        "EXPECTEDREC": irr_er.to_list(),
        "CAPROVISION": irr_cap.to_list(),
    })
    irregular_grp = irregular_grp_raw.group_by(["GROUPIND", "NO", "RECRATE"]).agg(
        pl.col("OBDEFAULT").sum(),
        pl.col("EXPECTEDREC").sum(),
        pl.col("CAPROVISION").sum(),
        pl.col("BALANCE").sum(),
    )
    irregular_grp = irregular_grp.with_columns(pl.lit("IRREGUALR").alias("GROUP"))
else:
    irregular_grp = pl.DataFrame(schema={
        "GROUPIND": pl.Utf8, "NO": pl.Int64, "RECRATE": pl.Float64,
        "BALANCE": pl.Float64, "OBDEFAULT": pl.Float64,
        "EXPECTEDREC": pl.Float64, "CAPROVISION": pl.Float64,
        "GROUP": pl.Utf8,
    })

# ─────────────────────────────────────────────
# REPOSSESSED < 12 MTHS
# ─────────────────────────────────────────────
if len(repossessed_l12_df) > 0:
    rl12_ob  = repossessed_l12_df["BALANCE"]
    rl12_er  = rl12_ob * (RECRATE / 100)
    rl12_cap = rl12_ob - rl12_er

    rep_l12_raw = pl.DataFrame({
        "GROUPIND":    ["REPOSSESSED"]          * len(repossessed_l12_df),
        "SUBGIND":     ["REPOSSESSED <12 MTHS"] * len(repossessed_l12_df),
        "SUBGROUP":    ["<12 MTHS "]            * len(repossessed_l12_df),
        "RECRATE":     [RECRATE]                * len(repossessed_l12_df),
        "BALANCE":     repossessed_l12_df["BALANCE"].to_list(),
        "OBDEFAULT":   rl12_ob.to_list(),
        "EXPECTEDREC": rl12_er.to_list(),
        "CAPROVISION": rl12_cap.to_list(),
    })
    rep_l12_grp = rep_l12_raw.group_by(
        ["GROUPIND", "SUBGIND", "SUBGROUP", "RECRATE"]
    ).agg(
        pl.col("OBDEFAULT").sum(),
        pl.col("EXPECTEDREC").sum(),
        pl.col("CAPROVISION").sum(),
        pl.col("BALANCE").sum(),
    )
else:
    rep_l12_grp = pl.DataFrame(schema={
        "GROUPIND": pl.Utf8, "SUBGIND": pl.Utf8, "SUBGROUP": pl.Utf8,
        "RECRATE": pl.Float64, "BALANCE": pl.Float64,
        "OBDEFAULT": pl.Float64, "EXPECTEDREC": pl.Float64, "CAPROVISION": pl.Float64,
    })

# ─────────────────────────────────────────────
# REPOSSESSED >= 12 MTHS
# ─────────────────────────────────────────────
if len(repossessed_m12_df) > 0:
    rm12_ob  = repossessed_m12_df["BALANCE"] * (100.0 / 100)
    rm12_er  = rm12_ob * (0.0 / 100)
    rm12_cap = pl.Series("CAPROVISION", [0.0] * len(repossessed_m12_df))

    rep_m12_raw = pl.DataFrame({
        "GROUPIND":    ["REPOSSESSED"]           * len(repossessed_m12_df),
        "SUBGIND":     ["REPOSSESSED >=12 MTHS"] * len(repossessed_m12_df),
        "SUBGROUP":    [">=12 MTHS"]             * len(repossessed_m12_df),
        "RATE":        [100.0]                   * len(repossessed_m12_df),
        "RECRATE":     [0.0]                     * len(repossessed_m12_df),
        "BALANCE":     repossessed_m12_df["BALANCE"].to_list(),
        "OBDEFAULT":   rm12_ob.to_list(),
        "EXPECTEDREC": rm12_er.to_list(),
        "CAPROVISION": rm12_cap.to_list(),
    })
    rep_m12_grp = rep_m12_raw.group_by(
        ["GROUPIND", "SUBGIND", "SUBGROUP", "RATE", "RECRATE"]
    ).agg(
        pl.col("OBDEFAULT").sum(),
        pl.col("EXPECTEDREC").sum(),
        pl.col("CAPROVISION").sum(),
        pl.col("BALANCE").sum(),
    )
else:
    rep_m12_grp = pl.DataFrame(schema={
        "GROUPIND": pl.Utf8, "SUBGIND": pl.Utf8, "SUBGROUP": pl.Utf8,
        "RATE": pl.Float64, "RECRATE": pl.Float64, "BALANCE": pl.Float64,
        "OBDEFAULT": pl.Float64, "EXPECTEDREC": pl.Float64, "CAPROVISION": pl.Float64,
    })

# SAS REPOSSESSED_GRP uses a skeleton frame merged by SUBGROUP so ordering is
# driven by the skeleton (<12 MTHS  first, >=12 MTHS second).
def _ensure_col(df, col, dtype=pl.Utf8, default=None):
    if col not in df.columns:
        df = df.with_columns(pl.lit(default).cast(dtype).alias(col))
    return df

rep_l12_grp = _ensure_col(rep_l12_grp, "RATE", pl.Float64, None)
rep_m12_grp = _ensure_col(rep_m12_grp, "RATE", pl.Float64, None)

repossessed_grp = pl.concat([rep_l12_grp, rep_m12_grp], how="diagonal")
repossessed_grp = repossessed_grp.with_columns([
    pl.lit("REPOSSESSED").alias("GROUP"),
    pl.when(pl.col("SUBGROUP") == "<12 MTHS ").then(pl.lit(71))
      .when(pl.col("SUBGROUP") == ">=12 MTHS").then(pl.lit(72))
      .otherwise(pl.lit(None)).cast(pl.Int64).alias("NO"),
])

# ─────────────────────────────────────────────
# DEFICIT
# ─────────────────────────────────────────────
if len(deficit_df) > 0:
    def_ob  = deficit_df["BALANCE"]
    def_er  = def_ob * (0.0 / 100)
    def_cap = def_ob - def_er

    deficit_grp_raw = pl.DataFrame({
        "GROUPIND":    ["DEFICIT"] * len(deficit_df),
        "NO":          [81]        * len(deficit_df),
        "RECRATE":     [0.0]       * len(deficit_df),
        "BALANCE":     deficit_df["BALANCE"].to_list(),
        "OBDEFAULT":   def_ob.to_list(),
        "EXPECTEDREC": def_er.to_list(),
        "CAPROVISION": def_cap.to_list(),
    })
    deficit_grp = deficit_grp_raw.group_by(["GROUPIND", "NO", "RECRATE"]).agg(
        pl.col("OBDEFAULT").sum(),
        pl.col("EXPECTEDREC").sum(),
        pl.col("CAPROVISION").sum(),
        pl.col("BALANCE").sum(),
    )
    deficit_grp = deficit_grp.with_columns(pl.lit("DEFICIT").alias("GROUP"))
else:
    deficit_grp = pl.DataFrame(schema={
        "GROUPIND": pl.Utf8, "NO": pl.Int64, "RECRATE": pl.Float64,
        "BALANCE": pl.Float64, "OBDEFAULT": pl.Float64,
        "EXPECTEDREC": pl.Float64, "CAPROVISION": pl.Float64,
        "GROUP": pl.Utf8,
    })

# ─────────────────────────────────────────────
# COMBINE all groups
# ─────────────────────────────────────────────
ALL_COLS = [
    "GROUPIND", "SUBGIND", "CATEGORY", "RATE", "RECRATE",
    "BALANCE", "OBDEFAULT", "EXPECTEDREC", "CAPROVISION",
    "NO", "GROUP", "SUBGROUP", "BAL",
]

def align_df(df: pl.DataFrame, cols: list) -> pl.DataFrame:
    for c in cols:
        if c not in df.columns:
            if c == "NO":
                df = df.with_columns(pl.lit(None).cast(pl.Int64).alias(c))
            elif c in ("RATE", "RECRATE", "BALANCE", "OBDEFAULT", "EXPECTEDREC", "CAPROVISION"):
                df = df.with_columns(pl.lit(None).cast(pl.Float64).alias(c))
            else:
                df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(c))
    return df.select(cols)

grp_list = [
    current_grp, month1t2_grp, month3t5_grp, month6t11_grp,
    month12ab_grp, irregular_grp, repossessed_grp, deficit_grp,
]
combine = pl.concat([align_df(g, ALL_COLS) for g in grp_list], how="diagonal")

# ─────────────────────────────────────────────
# TOTAL row
# ─────────────────────────────────────────────
total_df = combine.clone().with_columns(pl.lit("TOTAL").alias("GROUPIND"))
total_df = total_df.with_columns(
    pl.when(
        (pl.col("GROUP").is_null() | (pl.col("GROUP") == " ")) &
        (pl.col("SUBGROUP").is_null() | (pl.col("SUBGROUP") == " "))
    ).then(pl.lit(None).cast(pl.Float64)).otherwise(pl.col("BALANCE")).alias("BALANCE")
)
null_er_cats = ["- 3-5 MONTHS IN ARREARS", "- >6-11 MONTHS IN ARREARS", "- OTHERS"]
total_df = total_df.with_columns(
    pl.when(pl.col("CATEGORY").is_in(null_er_cats))
      .then(pl.lit(None).cast(pl.Float64))
      .otherwise(pl.col("EXPECTEDREC")).alias("EXPECTEDREC")
)
total_summary = total_df.group_by("GROUPIND").agg(
    pl.col("BALANCE").sum(),
    pl.col("OBDEFAULT").sum(),
    pl.col("EXPECTEDREC").sum(),
).with_columns(pl.lit(91).cast(pl.Int64).alias("NO"))

combine = pl.concat([combine, align_df(total_summary, ALL_COLS)], how="diagonal")

# ─────────────────────────────────────────────
# Merge with LAYOUT
# ─────────────────────────────────────────────
combine = combine.join(layout_df, on="GROUPIND", how="right")

# Fix NO for REPOSSESSED (driven by SUBGROUP), IRREGULAR, DEFICIT, TOTAL
combine = combine.with_columns([
    pl.when(pl.col("GROUPIND") == "IRREGULAR").then(pl.lit(61).cast(pl.Int64))
      .when((pl.col("GROUPIND") == "REPOSSESSED") & (pl.col("SUBGROUP") == "<12 MTHS ")).then(pl.lit(71).cast(pl.Int64))
      .when((pl.col("GROUPIND") == "REPOSSESSED") & (pl.col("SUBGROUP") == ">=12 MTHS")).then(pl.lit(72).cast(pl.Int64))
      .when(pl.col("GROUPIND") == "DEFICIT").then(pl.lit(81).cast(pl.Int64))
      .when(pl.col("GROUPIND") == "TOTAL").then(pl.lit(91).cast(pl.Int64))
      .otherwise(pl.col("NO")).alias("NO"),
    pl.col("RECRATE").fill_null(0.0),
])

# RATE1 / RECRATE1
rate1_list, recrate1_list = [], []
for row in combine.iter_rows(named=True):
    rate    = row.get("RATE")
    recrate = row.get("RECRATE") or 0.0
    grpind  = row.get("GROUPIND") or ""
    cat     = row.get("CATEGORY") or ""
    subgind = row.get("SUBGIND") or ""

    r1 = "N/A" if (grpind != "OTHERS" and rate is None) else (
        f"{rate:.2f}".strip() + "%" if rate is not None else "0.00%"
    )
    rate1_list.append(r1)

    if cat == "NOT REPOSSESSED YET" and subgind != ">=12 MTHS":
        recrate1_list.append(" ")
    else:
        recrate1_list.append(f"{recrate:.2f}".strip() + "%")

combine = combine.with_columns([
    pl.Series("RATE1",    rate1_list),
    pl.Series("RECRATE1", recrate1_list),
])

# Fix SUBGROUP / CATEGORY for non-OTHERS
combine = combine.with_columns([
    pl.when(
        (pl.col("GROUPIND") != "OTHERS") &
        (pl.col("SUBGROUP").is_null() | (pl.col("SUBGROUP") == " "))
    ).then(pl.lit("N/A")).otherwise(pl.col("SUBGROUP")).alias("SUBGROUP"),
    pl.when(
        (pl.col("GROUPIND") != "OTHERS") &
        (pl.col("CATEGORY").is_null() | (pl.col("CATEGORY") == ""))
    ).then(pl.lit("N/A")).otherwise(pl.col("CATEGORY")).alias("CATEGORY"),
])

combine = combine.sort("NO")

# ─────────────────────────────────────────────
# PD / LGD  (running totals mirroring SAS retained vars)
# ─────────────────────────────────────────────
PD_CATS = {"SUCCESSFUL REPOSSESSION", "NOT REPOSSESSED YET", "N/A"}

totobdefault   = 0.0
totexpectedrec = 0.0

pd_list, lgd_list, pdnew_list, lgdnew_list = [], [], [], []
amte_list, amtg_list = [], []

for row in combine.iter_rows(named=True):
    groupind    = row.get("GROUPIND") or ""
    cat         = row.get("CATEGORY") or ""
    balance     = row.get("BALANCE") or 0.0
    obdefault   = row.get("OBDEFAULT") or 0.0
    expectedrec = row.get("EXPECTEDREC") or 0.0
    subgroup    = row.get("SUBGROUP") or ""

    pd_val = lgd_val = amte = amtg = None

    if cat in PD_CATS and groupind != "TOTAL":
        totobdefault   += obdefault
        totexpectedrec += expectedrec
        amte = expectedrec
        amtg = obdefault

        pd_val  = round((obdefault / balance) * 100, 2) if balance else 0.0
        lgd_val = round(100 - ((expectedrec / obdefault) * 100), 2) if obdefault else 100.0
        if lgd_val is None:
            lgd_val = 100.0

        if cat == "N/A":
            pd_val = 100.0
        if groupind == "IRREGULAR" or (groupind == "REPOSSESSED" and subgroup == ">=12 MTHS"):
            lgd_val = 100.0

    pd_list.append(pd_val)
    lgd_list.append(lgd_val)
    amte_list.append(amte)
    amtg_list.append(amtg)
    pdnew_list.append(f"{pd_val:,.2f}"  if pd_val  is not None else None)
    lgdnew_list.append(f"{lgd_val:,.2f}" if lgd_val is not None else None)

combine = combine.with_columns([
    pl.Series("PD",     pd_list,    dtype=pl.Float64),
    pl.Series("LGD",    lgd_list,   dtype=pl.Float64),
    pl.Series("PDNEW",  pdnew_list, dtype=pl.Utf8),
    pl.Series("LGDNEW", lgdnew_list, dtype=pl.Utf8),
    pl.Series("AMTE",   amte_list,  dtype=pl.Float64),
    pl.Series("AMTG",   amtg_list,  dtype=pl.Float64),
])

# ─────────────────────────────────────────────
# SUBCOM – sub-group PD/LGD subtotals
# ─────────────────────────────────────────────
subcom = combine.filter(
    pl.col("CATEGORY").is_in(PD_CATS) & (pl.col("GROUPIND") != "TOTAL")
).drop_nulls(subset=["AMTE", "AMTG"])

subcom1 = subcom.group_by("SUBGIND").agg(
    pl.col("PD").sum().alias("PDSUM"),
    pl.col("AMTE").sum().alias("AMTESUM"),
    pl.col("AMTG").sum().alias("AMTGSUM"),
)

subgind_no_map = {"CURRENT": 17, "1-2 MTHS": 27, "3-5 MTHS": 37, "6-11 MTHS": 47}
subcom2_rows = []
for row in subcom1.iter_rows(named=True):
    si = row["SUBGIND"]
    if si in subgind_no_map:
        subcom2_rows.append({
            "SUBGIND":  si,
            "NO":       subgind_no_map[si],
            "CATEGORY": "SUB",
            "PDSUM":    row["PDSUM"],
            "AMTESUM":  row["AMTESUM"],
            "AMTGSUM":  row["AMTGSUM"],
        })

subcom2 = pl.DataFrame(subcom2_rows) if subcom2_rows else pl.DataFrame(schema={
    "SUBGIND": pl.Utf8, "NO": pl.Int64, "CATEGORY": pl.Utf8,
    "PDSUM": pl.Float64, "AMTESUM": pl.Float64, "AMTGSUM": pl.Float64,
})

combine = pl.concat([combine, align_df(subcom2, ALL_COLS)], how="diagonal")
combine = combine.join(subcom1, on="SUBGIND", how="left")

# Compute SUBLGD, PDSUMNEW, SUBLGDNEW; update PDNEW/LGDNEW for TOTAL
sublgd_list, pdsumnew_list, sublgdnew_list = [], [], []
updated_pdnew, updated_lgdnew = [], []

for row in combine.iter_rows(named=True):
    si       = row.get("SUBGIND") or ""
    amtesum  = row.get("AMTESUM")
    amtgsum  = row.get("AMTGSUM")
    pdsum    = row.get("PDSUM")
    groupind = row.get("GROUPIND") or ""
    balance  = row.get("BALANCE") or 0.0

    sublgd = pdsumnew = sublgdnew = None

    if si in ("CURRENT", "1-2 MTHS", "3-5 MTHS", "6-11 MTHS"):
        sublgd    = round(100 - ((amtesum / amtgsum) * 100), 2) if amtgsum else 100.0
        pdsumnew  = f"{pdsum:,.2f}"  if pdsum  is not None else None
        sublgdnew = f"{sublgd:,.2f}" if sublgd is not None else None

    sublgd_list.append(sublgd)
    pdsumnew_list.append(pdsumnew)
    sublgdnew_list.append(sublgdnew)

    if groupind == "TOTAL":
        pd_t  = round((totobdefault / balance) * 100, 2) if balance else 0.0
        lgd_t = round(100 - ((totexpectedrec / totobdefault) * 100), 2) if totobdefault else 100.0
        updated_pdnew.append(f"{pd_t:,.2f}")
        updated_lgdnew.append(f"{lgd_t:,.2f}")
    else:
        updated_pdnew.append(row.get("PDNEW"))
        updated_lgdnew.append(row.get("LGDNEW"))

combine = combine.with_columns([
    pl.Series("SUBLGD",    sublgd_list,    dtype=pl.Float64),
    pl.Series("PDSUMNEW",  pdsumnew_list,  dtype=pl.Utf8),
    pl.Series("SUBLGDNEW", sublgdnew_list, dtype=pl.Utf8),
    pl.Series("PDNEW",     updated_pdnew,  dtype=pl.Utf8),
    pl.Series("LGDNEW",    updated_lgdnew, dtype=pl.Utf8),
])

combine = combine.sort("NO")

# ─────────────────────────────────────────────
# Report utility helpers
# ─────────────────────────────────────────────
LINE_WIDTH  = 237
PAGE_LENGTH = 60
ASA_NORMAL  = " "
ASA_TOP     = "1"

lines_out  = []
line_count = 0

def emit(asa: str, text: str):
    lines_out.append(asa + text)

def emit_line(text: str, asa: str = ASA_NORMAL):
    global line_count
    if asa == ASA_NORMAL and line_count >= PAGE_LENGTH:
        emit(ASA_TOP, text)
        line_count = 1
    else:
        emit(asa, text)
        line_count += 1

def emit_blank():
    emit_line(" " * LINE_WIDTH)

def make_buf() -> list:
    return [" "] * LINE_WIDTH

def place(text, col: int, buf: list):
    if text is None:
        return
    for i, ch in enumerate(str(text)):
        pos = col - 1 + i
        if 0 <= pos < len(buf):
            buf[pos] = ch

def render(buf: list) -> str:
    return "".join(buf)

def fmt20(val) -> str:
    if val is None:
        return ""
    return f"{val:>20,.2f}"


# ─────────────────────────────────────────────
# Write report header
# ─────────────────────────────────────────────
def write_header():
    global line_count
    line_count = 0

    b = make_buf()
    place("RECOVERY RATE OF HIRE PURCHASE LOANS - PIBB", 1, b)
    emit_line(render(b), ASA_TOP)

    emit_line(" " * LINE_WIDTH)

    b = make_buf()
    place("CATEGORY",            1,   b)
    place("MONTHS IN",          16,   b)
    place("OUTSTANDING BALANCE", 34,  b)
    place("AVERAGE",             59,  b)
    place("LOAN STATUS",         74,  b)
    place("OUTSTANDING BALANCE", 104, b)
    place("RECOVERY",            139, b)
    place("EXPECTED RECOVERY",   149, b)
    place("TIMING OF",           169, b)
    place("DISCOUNT",            179, b)
    place("DISCOUNTED",          189, b)
    place("PD",                  214, b)
    place("LOSS GIVEN",          225, b)
    emit_line(render(b))

    b = make_buf()
    place("ARREARS",             16,  b)
    place("AT CURRENT DATE",     34,  b)
    place("DEFAULT RATE",        59,  b)
    place("AT CURRENT DATE X DEFAULT RATE", 104, b)
    place("RATE",                139, b)
    place("AMOUNT",              149, b)
    place("RECOVERY",            169, b)
    place("RATE",                179, b)
    place("EXPECTED",            189, b)
    place("%",                   214, b)
    place("DEFAULT %",           225, b)
    emit_line(render(b))

    b = make_buf()
    place("AT CURRENT DATE", 16, b)
    place("(A)",             34, b)
    place("(B)",             59, b)
    place("(C)=(A)X(B)",    104, b)
    place("(D)",             139, b)
    place("(E)=(C)X(D)",    149, b)
    place("(F)",             169, b)
    place("(G)",             179, b)
    place("RECOVERY(PRESENT VALUE)", 189, b)
    emit_line(render(b))

    b = make_buf()
    place("   (RM)", 34,  b)
    place("   (RM)", 104, b)
    place("   (RM)", 149, b)
    place("   (RM)", 189, b)
    emit_line(render(b))

    emit_line("-" * LINE_WIDTH)


write_header()

# ─────────────────────────────────────────────
# Render each row
# ─────────────────────────────────────────────
for row in combine.iter_rows(named=True):
    groupind    = row.get("GROUPIND") or ""
    subgind     = row.get("SUBGIND") or ""
    category    = row.get("CATEGORY") or ""
    no          = row.get("NO")
    group       = row.get("GROUP") or " "
    subgroup    = row.get("SUBGROUP") or " "
    bal         = row.get("BAL") or ""
    rate1       = row.get("RATE1") or ""
    recrate1    = row.get("RECRATE1") or " "
    balance     = row.get("BALANCE")
    obdefault   = row.get("OBDEFAULT")
    expectedrec = row.get("EXPECTEDREC")
    pdnew       = row.get("PDNEW") or ""
    lgdnew      = row.get("LGDNEW") or ""
    pdsumnew    = row.get("PDSUMNEW") or ""
    sublgdnew   = row.get("SUBLGDNEW") or ""

    def _b():
        return make_buf()

    # ── OTHERS / CURRENT (NO 11-16) ──────────────────────────────
    if groupind == "OTHERS" and subgind == "CURRENT" and no != 17:
        b = _b()
        place(group,           1,   b)
        place(subgroup,        16,  b)
        place(bal,             34,  b)
        place(rate1,           59,  b)
        place(category,        74,  b)
        place(fmt20(obdefault),   104, b)
        place(recrate1,        139, b)
        place(fmt20(expectedrec), 149, b)
        place(pdnew,           214, b)
        place(lgdnew,          225, b)
        emit_line(render(b))
        if no == 16:
            b2 = _b(); place("-" * 164, 74, b2); emit_line(render(b2))

    if subgind == "CURRENT" and no == 17:
        b = _b()
        place(category,   74,  b)
        place(pdsumnew,   214, b)
        place(sublgdnew,  225, b)
        emit_line(render(b))
        b2 = _b(); place("-" * 164, 74, b2); emit_line(render(b2))
        emit_blank()

    # ── OTHERS / 1-2 MTHS (NO 21-26) ─────────────────────────────
    if groupind == "OTHERS" and subgind == "1-2 MTHS" and no != 27:
        b = _b()
        place(group,           1,   b)
        place(subgroup,        16,  b)
        place(bal,             34,  b)
        place(rate1,           59,  b)
        place(category,        74,  b)
        place(fmt20(obdefault),   104, b)
        place(recrate1,        139, b)
        place(fmt20(expectedrec), 149, b)
        place(pdnew,           214, b)
        place(lgdnew,          225, b)
        emit_line(render(b))
        if no == 26:
            b2 = _b(); place("-" * 164, 74, b2); emit_line(render(b2))

    if subgind == "1-2 MTHS" and no == 27:
        b = _b()
        place(category,  74,  b)
        place(pdsumnew,  214, b)
        place(sublgdnew, 225, b)
        emit_line(render(b))
        b2 = _b(); place("-" * 164, 74, b2); emit_line(render(b2))
        emit_blank()

    # ── OTHERS / 3-5 MTHS (NO 31-36) ─────────────────────────────
    if groupind == "OTHERS" and subgind == "3-5 MTHS" and no != 37:
        b = _b()
        place(group,           1,   b)
        place(subgroup,        16,  b)
        place(bal,             34,  b)
        place(rate1,           59,  b)
        place(category,        74,  b)
        place(fmt20(obdefault),   104, b)
        place(recrate1,        139, b)
        place(fmt20(expectedrec), 149, b)
        place(pdnew,           214, b)
        place(lgdnew,          225, b)
        emit_line(render(b))
        if no == 36:
            b2 = _b(); place("-" * 164, 74, b2); emit_line(render(b2))

    if subgind == "3-5 MTHS" and no == 37:
        b = _b()
        place(category,  74,  b)
        place(pdsumnew,  214, b)
        place(sublgdnew, 225, b)
        emit_line(render(b))
        b2 = _b(); place("-" * 164, 74, b2); emit_line(render(b2))
        emit_blank()

    # ── OTHERS / 6-11 MTHS (NO 41-46) ────────────────────────────
    if groupind == "OTHERS" and subgind == "6-11 MTHS" and no != 47:
        b = _b()
        place(group,           1,   b)
        place(subgroup,        16,  b)
        place(bal,             34,  b)
        place(rate1,           59,  b)
        place(category,        74,  b)
        place(fmt20(obdefault),   104, b)
        place(recrate1,        139, b)
        place(fmt20(expectedrec), 149, b)
        place(pdnew,           214, b)
        place(lgdnew,          225, b)
        emit_line(render(b))
        if no == 46:
            b2 = _b(); place("-" * 164, 74, b2); emit_line(render(b2))

    if subgind == "6-11 MTHS" and no == 47:
        b = _b()
        place(category,  74,  b)
        place(pdsumnew,  214, b)
        place(sublgdnew, 225, b)
        emit_line(render(b))
        b2 = _b(); place("-" * 164, 74, b2); emit_line(render(b2))
        emit_blank()

    # ── OTHERS / >=12 MTHS (NO 51) ───────────────────────────────
    if groupind == "OTHERS" and subgind == ">=12 MTHS":
        b = _b()
        place(group,           1,   b)
        place(subgroup,        16,  b)
        place(bal,             34,  b)
        place(rate1,           59,  b)
        place(category,        74,  b)
        place(fmt20(obdefault),   104, b)
        place(recrate1,        139, b)
        place(fmt20(expectedrec), 149, b)
        place(pdnew,           214, b)
        place(lgdnew,          225, b)
        emit_line(render(b))
        if no == 51:
            emit_blank()
            emit_line("-" * LINE_WIDTH)

    # ── IRREGULAR (NO 61) ─────────────────────────────────────────
    if groupind == "IRREGULAR":
        b = _b()
        place("IRREGULAR",     1,   b)
        place(subgroup,        16,  b)
        place(fmt20(balance),     34,  b)
        place(rate1,           59,  b)
        place(category,        74,  b)
        place(fmt20(obdefault),   104, b)
        place(recrate1,        139, b)
        place(fmt20(expectedrec), 149, b)
        place(pdnew,           214, b)
        place(lgdnew,          225, b)
        emit_line(render(b))
        if no == 61:
            emit_blank()
            emit_line("-" * LINE_WIDTH)

    # ── REPOSSESSED <12 MTHS (NO 71) ─────────────────────────────
    if groupind == "REPOSSESSED" and subgind == "REPOSSESSED <12 MTHS":
        b = _b()
        place("REPOSSESSED",   1,   b)
        place(subgroup,        16,  b)
        place(fmt20(balance),     34,  b)
        place(rate1,           59,  b)
        place(category,        74,  b)
        place(fmt20(obdefault),   104, b)
        place(recrate1,        139, b)
        place(fmt20(expectedrec), 149, b)
        place(pdnew,           214, b)
        place(lgdnew,          225, b)
        emit_line(render(b))
        if no == 71:
            emit_blank()

    # ── REPOSSESSED >=12 MTHS (NO 72) ────────────────────────────
    if groupind == "REPOSSESSED" and subgind == "REPOSSESSED >=12 MTHS":
        b = _b()
        place(subgroup,        16,  b)
        place(fmt20(balance),     34,  b)
        place(rate1,           59,  b)
        place(category,        74,  b)
        place(fmt20(obdefault),   104, b)
        place(recrate1,        139, b)
        place(fmt20(expectedrec), 149, b)
        place(pdnew,           214, b)
        place(lgdnew,          225, b)
        emit_line(render(b))
        if no == 72:
            emit_blank()
            emit_line("-" * LINE_WIDTH)

    # ── DEFICIT (NO 81) ───────────────────────────────────────────
    if groupind == "DEFICIT":
        b = _b()
        place("DEFICIT",       1,   b)
        place(subgroup,        16,  b)
        place(fmt20(balance),     34,  b)
        place(rate1,           59,  b)
        place(category,        74,  b)
        place(fmt20(obdefault),   104, b)
        place(recrate1,        139, b)
        place(fmt20(expectedrec), 149, b)
        place(pdnew,           214, b)
        place(lgdnew,          225, b)
        emit_line(render(b))
        if no == 81:
            emit_blank()
            emit_line("=" * LINE_WIDTH)

    # ── TOTAL (NO 91) ─────────────────────────────────────────────
    if groupind == "TOTAL":
        b = _b()
        place("TOTAL",         1,   b)
        place(fmt20(balance),     34,  b)
        place(fmt20(balance),    104,  b)
        place(fmt20(expectedrec), 149, b)
        place(pdnew,           214, b)
        place(lgdnew,          225, b)
        emit_line(render(b))
        if no == 91:
            emit_blank()
            emit_line("-" * LINE_WIDTH)

# ─────────────────────────────────────────────
# Write OUTPUT1
# ─────────────────────────────────────────────
with open(OUTPUT1_PATH, "w", encoding="utf-8") as f:
    for line in lines_out:
        f.write(line + "\n")

# ─────────────────────────────────────────────
# GET CAP – produce NPL.ICAP1_STAFF.parquet
# ─────────────────────────────────────────────
cap_cats = {
    "SUCCESSFUL REPOSSESSION",
    "N/A",
    "- >6-11 MONTHS IN ARREARS",
    "- OTHERS",
    "- 3-5 MONTHS IN ARREARS",
}

combine_cap = combine.with_columns(
    pl.when(
        pl.col("SUBGIND").is_null() | (pl.col("SUBGIND") == "")
    ).then(pl.col("GROUPIND")).otherwise(pl.col("SUBGIND")).alias("SUBGIND")
)

cap_df = combine_cap.filter(pl.col("CATEGORY").is_in(cap_cats))

cap_summary = cap_df.group_by("SUBGIND").agg(
    pl.col("CAPROVISION").sum()
).rename({"SUBGIND": "CATEGORY"})

# PDLGD: NO IN (17,27,37,47)
pdlgd_df = (
    combine_cap
    .filter(pl.col("NO").is_in([17, 27, 37, 47]))
    .select(["SUBGIND", "PDSUM", "SUBLGD"])
    .rename({"SUBGIND": "CATEGORY"})
)

# PDLGD2: NO IN (51,61,71,72,81)
pdlgd2_df = (
    combine_cap
    .filter(pl.col("NO").is_in([51, 61, 71, 72, 81]))
    .select(["SUBGIND", "PD", "LGD"])
    .rename({"SUBGIND": "CATEGORY", "PD": "PDSUM", "LGD": "SUBLGD"})
)

pdlgd_combined = pl.concat([pdlgd_df, pdlgd2_df], how="diagonal")
icap1_staff = cap_summary.join(pdlgd_combined, on="CATEGORY", how="left")
icap1_staff = icap1_staff.rename({"PDSUM": "PD", "SUBLGD": "LGD"})

icap1_staff.write_parquet(ICAP1_STAFF_PATH)

print(f"Report written to      : {OUTPUT1_PATH}")
print(f"ICAP1_STAFF written to : {ICAP1_STAFF_PATH}")
