# !/usr/bin/env python3
"""
Program: EIICAP41.py
Purpose: GENERATE CAP REPORT BY COMPANY
"""

import duckdb
import polars as pl
import os
from datetime import date

# ─────────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────────
BASE_DIR        = r"C:/data"
LOAN_DIR        = os.path.join(BASE_DIR, "loan")
RATE_DIR        = os.path.join(BASE_DIR, "rate")
HP_DIR          = os.path.join(BASE_DIR, "hp")
CCRIS_DIR       = os.path.join(BASE_DIR, "ccris")
NPL_DIR         = os.path.join(BASE_DIR, "npl")
OUTPUT_DIR      = os.path.join(BASE_DIR, "output")

os.makedirs(NPL_DIR,    exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

OUTPUT1_PATH    = os.path.join(OUTPUT_DIR, "EIICAP41.txt")
ICAP1_PATH      = os.path.join(NPL_DIR,   "ICAP1.parquet")

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

REPTMON   = f"{reptdate_val.month:02d}"
NOWK      = wk
REPTYEAR  = str(reptdate_val.year)[-2:]   # YEAR2. → last 2 digits

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
# HP master
# ─────────────────────────────────────────────
hp_file = os.path.join(HP_DIR, f"IHP{REPTMON}{NOWK}{REPTYEAR}.parquet")
hp_df = con.execute(f"SELECT * FROM '{hp_file}'").pl()

# ─────────────────────────────────────────────
# CREDSUB
# ─────────────────────────────────────────────
ccris_file = os.path.join(CCRIS_DIR, f"ICREDMSUBAC{REPTMON}{REPTYEAR}.parquet")
credsub_df = con.execute(f"""
    SELECT ACCTNUM AS ACCTNO, DAYSARR AS DAYARR, NOTENO
    FROM '{ccris_file}'
    WHERE FACILITY IN ('34331','34332')
""").pl()

# NODUPKEY: keep first (max DAYARR) per ACCTNO+NOTENO
credsub_df = (
    credsub_df
    .sort(["ACCTNO", "NOTENO", "DAYARR"], descending=[False, False, True])
    .unique(subset=["ACCTNO", "NOTENO"], keep="first")
)

# ─────────────────────────────────────────────
# Merge HP ← CREDSUB
# ─────────────────────────────────────────────
hp_df = hp_df.join(credsub_df, on=["ACCTNO", "NOTENO"], how="left")

# ─────────────────────────────────────────────
# Split into arrears buckets
# ─────────────────────────────────────────────
base = hp_df.filter(
    pl.col("PRODUCT").is_in([128, 130, 131, 132]) & (pl.col("BALANCE") > 0)
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
# Helper: build category rows from a bucket df
# ─────────────────────────────────────────────
def build_group(src: pl.DataFrame, subgind: str, categories: list[dict]) -> pl.DataFrame:
    """
    Each item in categories:
        category, rate, recrate, obdefault_expr, expectedrec_expr,
        caprovision_expr, no
    Returns aggregated DataFrame per (GROUPIND, SUBGIND, CATEGORY, RATE, RECRATE).
    """
    rows = []
    for cfg in categories:
        rate    = cfg["rate"]
        recrate = cfg["recrate"]
        ob      = src["BALANCE"] * (rate / 100)

        if cfg["expectedrec"] == "fromob":
            er = ob * (recrate / 100)
        else:
            # 'keepprior' – SAS retains EXPECTEDREC from prior iteration;
            # here we keep whatever was last computed (same BALANCE-derived ob)
            er = ob * (recrate / 100)   # effectively 0 when recrate=0

        if cfg["caprovision"] == "ob_minus_er":
            cap = ob - er
        elif cfg["caprovision"] == "zero":
            cap = pl.Series("CAPROVISION", [0.0] * len(src))
        else:
            cap = ob - er

        tmp = pl.DataFrame({
            "GROUPIND":    ["OTHERS"] * len(src),
            "SUBGIND":     [subgind]  * len(src),
            "CATEGORY":    [cfg["category"]] * len(src),
            "RATE":        [rate]     * len(src),
            "RECRATE":     [recrate]  * len(src),
            "BALANCE":     src["BALANCE"].to_list(),
            "OBDEFAULT":   ob.to_list(),
            "EXPECTEDREC": er.to_list(),
            "CAPROVISION": cap.to_list(),
        })
        rows.append(tmp)

    df = pl.concat(rows)
    # PROC SUMMARY NWAY – sum by class vars
    df = df.group_by(["GROUPIND", "SUBGIND", "CATEGORY", "RATE", "RECRATE"]).agg(
        pl.col("OBDEFAULT").sum(),
        pl.col("EXPECTEDREC").sum(),
        pl.col("CAPROVISION").sum(),
        pl.col("BALANCE").sum(),
    )
    return df


def fix_not_repossessed(grp_df: pl.DataFrame, subgind: str, nr_rate: float) -> pl.DataFrame:
    """
    SAS pattern: sum EXPECTEDREC of the 3 sub-categories and overwrite
    'NOT REPOSSESSED YET' EXPECTEDREC with that sum.
    """
    sub_cats = ["- 3-5 MONTHS IN ARREARS", "- >6-11 MONTHS IN ARREARS", "- OTHERS"]
    summed_er = grp_df.filter(pl.col("CATEGORY").is_in(sub_cats))["EXPECTEDREC"].sum()

    grp_df = grp_df.with_columns(
        pl.when(
            (pl.col("CATEGORY") == "NOT REPOSSESSED YET") &
            (pl.col("SUBGIND") == subgind)
        ).then(pl.lit(summed_er)).otherwise(pl.col("EXPECTEDREC")).alias("EXPECTEDREC")
    )
    return grp_df


# ─────────────────────────────────────────────
# CURRENT GROUP
# ─────────────────────────────────────────────
current_cats = [
    {"category": "SUCCESSFUL REPOSSESSION",  "rate": 0.31,  "recrate": RECRATE,
     "expectedrec": "fromob", "caprovision": "ob_minus_er", "no": 12},
    {"category": "- 3-5 MONTHS IN ARREARS",  "rate": 0.08,  "recrate": 25.00,
     "expectedrec": "fromob", "caprovision": "ob_minus_er", "no": 14},
    {"category": "NOT REPOSSESSED YET",       "rate": 0.17,  "recrate": 0.0,
     "expectedrec": "fromob", "caprovision": "zero",        "no": 13},
    {"category": "- >6-11 MONTHS IN ARREARS", "rate": 0.08,  "recrate": 25.00,
     "expectedrec": "fromob", "caprovision": "ob_minus_er", "no": 15},
    {"category": "- OTHERS",                  "rate": 0.01,  "recrate": 25.00,
     "expectedrec": "fromob", "caprovision": "ob_minus_er", "no": 16},
    {"category": "CONTINUE PAYING",           "rate": 99.52, "recrate": 100.0,
     "expectedrec": "fromob", "caprovision": "zero",        "no": 11},
]
current_grp = build_group(current_df, "CURRENT", current_cats)
current_grp = fix_not_repossessed(current_grp, "CURRENT", 0.17)

NO_MAP_CURRENT = {
    "CONTINUE PAYING":           (11, "OTHERS", "CURRENT"),
    "SUCCESSFUL REPOSSESSION":   (12, " ", " "),
    "NOT REPOSSESSED YET":       (13, " ", " "),
    "- 3-5 MONTHS IN ARREARS":   (14, " ", " "),
    "- >6-11 MONTHS IN ARREARS": (15, " ", " "),
    "- OTHERS":                  (16, " ", " "),
}
def apply_no_map(df, no_map):
    nos, groups, subgroups, bals = [], [], [], []
    fmt_comma = lambda v: f"{v:,.2f}" if v is not None else ""
    for row in df.iter_rows(named=True):
        cat = row["CATEGORY"]
        if cat in no_map:
            no, grp, subgrp = no_map[cat]
        else:
            no, grp, subgrp = None, " ", " "
        nos.append(no)
        groups.append(grp)
        subgroups.append(subgrp)
        bal = fmt_comma(row["BALANCE"]) if grp not in (" ", "") else ""
        bals.append(bal)
    return df.with_columns([
        pl.Series("NO", nos, dtype=pl.Int64),
        pl.Series("GROUP", groups),
        pl.Series("SUBGROUP", subgroups),
        pl.Series("BAL", bals),
    ])

current_grp = apply_no_map(current_grp, NO_MAP_CURRENT)

# ─────────────────────────────────────────────
# 1-2 MONTHS
# ─────────────────────────────────────────────
month1t2_cats = [
    {"category": "CONTINUE PAYING",           "rate": 92.73, "recrate": 100.0,
     "expectedrec": "fromob", "caprovision": "zero",        "no": 21},
    {"category": "SUCCESSFUL REPOSSESSION",  "rate": 4.26,  "recrate": RECRATE,
     "expectedrec": "fromob", "caprovision": "ob_minus_er", "no": 22},
    {"category": "- 3-5 MONTHS IN ARREARS",  "rate": 0.99,  "recrate": 25.00,
     "expectedrec": "fromob", "caprovision": "ob_minus_er", "no": 24},
    {"category": "NOT REPOSSESSED YET",       "rate": 3.01,  "recrate": 0.0,
     "expectedrec": "fromob", "caprovision": "zero",        "no": 23},
    {"category": "- >6-11 MONTHS IN ARREARS", "rate": 1.17,  "recrate": 25.00,
     "expectedrec": "fromob", "caprovision": "ob_minus_er", "no": 25},
    {"category": "- OTHERS",                  "rate": 0.85,  "recrate": 25.00,
     "expectedrec": "fromob", "caprovision": "ob_minus_er", "no": 26},
]
month1t2_grp = build_group(month1t2_df, "1-2 MTHS", month1t2_cats)
month1t2_grp = fix_not_repossessed(month1t2_grp, "1-2 MTHS", 3.01)

NO_MAP_1T2 = {
    "CONTINUE PAYING":           (21, " ", "1-2 MTHS"),
    "SUCCESSFUL REPOSSESSION":   (22, " ", " "),
    "NOT REPOSSESSED YET":       (23, " ", " "),
    "- 3-5 MONTHS IN ARREARS":   (24, " ", " "),
    "- >6-11 MONTHS IN ARREARS": (25, " ", " "),
    "- OTHERS":                  (26, " ", " "),
}
month1t2_grp = apply_no_map(month1t2_grp, NO_MAP_1T2)

# ─────────────────────────────────────────────
# 3-5 MONTHS
# ─────────────────────────────────────────────
month3t5_cats = [
    {"category": "CONTINUE PAYING",           "rate": 0.00,  "recrate": 100.0,
     "expectedrec": "fromob", "caprovision": "zero",        "no": 31},
    {"category": "SUCCESSFUL REPOSSESSION",  "rate": 62.27, "recrate": RECRATE,
     "expectedrec": "fromob", "caprovision": "ob_minus_er", "no": 32},
    {"category": "- 3-5 MONTHS IN ARREARS",  "rate": 6.43,  "recrate": 25.00,
     "expectedrec": "fromob", "caprovision": "ob_minus_er", "no": 34},
    {"category": "NOT REPOSSESSED YET",       "rate": 37.73, "recrate": 0.0,
     "expectedrec": "fromob", "caprovision": "zero",        "no": 33},
    {"category": "- >6-11 MONTHS IN ARREARS", "rate": 6.36,  "recrate": 25.00,
     "expectedrec": "fromob", "caprovision": "ob_minus_er", "no": 35},
    {"category": "- OTHERS",                  "rate": 24.94, "recrate": 25.00,
     "expectedrec": "fromob", "caprovision": "ob_minus_er", "no": 36},
]
month3t5_grp = build_group(month3t5_df, "3-5 MTHS", month3t5_cats)
month3t5_grp = fix_not_repossessed(month3t5_grp, "3-5 MTHS", 37.73)

NO_MAP_3T5 = {
    "CONTINUE PAYING":           (31, " ", "3-5 MTHS"),
    "SUCCESSFUL REPOSSESSION":   (32, " ", " "),
    "NOT REPOSSESSED YET":       (33, " ", " "),
    "- 3-5 MONTHS IN ARREARS":   (34, " ", " "),
    "- >6-11 MONTHS IN ARREARS": (35, " ", " "),
    "- OTHERS":                  (36, " ", " "),
}
month3t5_grp = apply_no_map(month3t5_grp, NO_MAP_3T5)

# ─────────────────────────────────────────────
# 6-11 MONTHS
# ─────────────────────────────────────────────
month6t11_cats = [
    {"category": "CONTINUE PAYING",           "rate": 0.00,  "recrate": 100.0,
     "expectedrec": "fromob", "caprovision": "zero",        "no": 41},
    {"category": "SUCCESSFUL REPOSSESSION",  "rate": 34.91, "recrate": RECRATE,
     "expectedrec": "fromob", "caprovision": "ob_minus_er", "no": 42},
    {"category": "- 3-5 MONTHS IN ARREARS",  "rate": 1.85,  "recrate": 25.00,
     "expectedrec": "fromob", "caprovision": "ob_minus_er", "no": 44},
    {"category": "NOT REPOSSESSED YET",       "rate": 65.09, "recrate": 0.0,
     "expectedrec": "fromob", "caprovision": "zero",        "no": 43},
    {"category": "- >6-11 MONTHS IN ARREARS", "rate": 2.04,  "recrate": 25.00,
     "expectedrec": "fromob", "caprovision": "ob_minus_er", "no": 45},
    {"category": "- OTHERS",                  "rate": 61.20, "recrate": 25.00,
     "expectedrec": "fromob", "caprovision": "ob_minus_er", "no": 46},
]
month6t11_grp = build_group(month6t11_df, "6-11 MTHS", month6t11_cats)
month6t11_grp = fix_not_repossessed(month6t11_grp, "6-11 MTHS", 65.09)

NO_MAP_6T11 = {
    "CONTINUE PAYING":           (41, " ", "6-11 MTHS"),
    "SUCCESSFUL REPOSSESSION":   (42, " ", " "),
    "NOT REPOSSESSED YET":       (43, " ", " "),
    "- 3-5 MONTHS IN ARREARS":   (44, " ", " "),
    "- >6-11 MONTHS IN ARREARS": (45, " ", " "),
    "- OTHERS":                  (46, " ", " "),
}
month6t11_grp = apply_no_map(month6t11_grp, NO_MAP_6T11)

# ─────────────────────────────────────────────
# 12 MONTHS AND ABOVE
# ─────────────────────────────────────────────
if len(month12ab_df) > 0:
    m12_ob  = month12ab_df["BALANCE"] * (100.0 / 100)
    m12_er  = m12_ob * (0.0 / 100)
    m12_cap = pl.Series("CAPROVISION", [0.0] * len(month12ab_df))

    month12ab_grp_raw = pl.DataFrame({
        "GROUPIND":    ["OTHERS"]            * len(month12ab_df),
        "SUBGIND":     [">=12 MTHS"]         * len(month12ab_df),
        "CATEGORY":    ["NOT REPOSSESSED YET"] * len(month12ab_df),
        "RATE":        [100.0]               * len(month12ab_df),
        "RECRATE":     [0.0]                 * len(month12ab_df),
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
    fmt_comma = lambda v: f"{v:,.2f}"
    bals_m12 = [fmt_comma(r["BALANCE"]) for r in month12ab_grp.iter_rows(named=True)]
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

# Combine repossessed
def _ensure_col(df, col, dtype=pl.Utf8, default=None):
    if col not in df.columns:
        df = df.with_columns(pl.lit(default).cast(dtype).alias(col))
    return df

rep_l12_grp = _ensure_col(rep_l12_grp, "RATE", pl.Float64, None)
rep_m12_grp = _ensure_col(rep_m12_grp, "RATE", pl.Float64, None)

repossessed_grp = pl.concat([rep_l12_grp, rep_m12_grp], how="diagonal")
repossessed_grp = repossessed_grp.with_columns([
    pl.lit("REPOSSESSED").alias("GROUP"),
    pl.when(pl.col("SUBGIND") == "REPOSSESSED <12 MTHS").then(pl.lit(71))
      .otherwise(pl.lit(72)).cast(pl.Int64).alias("NO"),
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
# Align schemas before concat
ALL_COLS = [
    "GROUPIND", "SUBGIND", "CATEGORY", "RATE", "RECRATE",
    "BALANCE", "OBDEFAULT", "EXPECTEDREC", "CAPROVISION",
    "NO", "GROUP", "SUBGROUP", "BAL",
]

def align_df(df: pl.DataFrame, cols: list) -> pl.DataFrame:
    for c in cols:
        if c not in df.columns:
            if c in ("NO",):
                df = df.with_columns(pl.lit(None).cast(pl.Int64).alias(c))
            elif c in ("RATE", "RECRATE", "BALANCE", "OBDEFAULT",
                       "EXPECTEDREC", "CAPROVISION"):
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
# Nullify BALANCE for rows where GROUP=' ' AND SUBGROUP=' '
total_df = total_df.with_columns(
    pl.when(
        (pl.col("GROUP").is_null() | (pl.col("GROUP") == " ")) &
        (pl.col("SUBGROUP").is_null() | (pl.col("SUBGROUP") == " "))
    ).then(pl.lit(None).cast(pl.Float64)).otherwise(pl.col("BALANCE")).alias("BALANCE")
)
# Nullify EXPECTEDREC for certain categories
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
# Merge with LAYOUT (to preserve GROUPIND order)
# ─────────────────────────────────────────────
combine = combine.join(layout_df, on="GROUPIND", how="right")

# Fix NO for non-OTHERS groups
combine = combine.with_columns([
    pl.when(pl.col("GROUPIND") == "IRREGULAR").then(pl.lit(61).cast(pl.Int64))
      .when(pl.col("GROUPIND") == "DEFICIT").then(pl.lit(81).cast(pl.Int64))
      .when(pl.col("GROUPIND") == "TOTAL").then(pl.lit(91).cast(pl.Int64))
      .otherwise(pl.col("NO")).alias("NO"),
    pl.col("RECRATE").fill_null(0.0),
])

# RATE1, RECRATE1 formatting
def fmt_rate(rate_val) -> str:
    if rate_val is None:
        return "N/A"
    return f"{rate_val:.2f}".rstrip("0").rstrip(".")  + "%" if rate_val else "0%"

def fmt_recrate(recrate_val, category, subgind, groupind, rate_val) -> str:
    if category == "NOT REPOSSESSED YET" and subgind != ">=12 MTHS":
        return " "
    if recrate_val is None:
        recrate_val = 0.0
    s = str(recrate_val)
    # SAS: COMPRESS(PUT(RECRATE,6.2))||'%'
    formatted = f"{recrate_val:.2f}"
    # remove leading spaces (COMPRESS)
    return formatted.strip() + "%"

rate1_list, recrate1_list = [], []
for row in combine.iter_rows(named=True):
    rate    = row.get("RATE")
    recrate = row.get("RECRATE") or 0.0
    grpind  = row.get("GROUPIND") or ""
    cat     = row.get("CATEGORY") or ""
    subgind = row.get("SUBGIND") or ""

    # RATE1
    if grpind != "OTHERS" and rate is None:
        r1 = "N/A"
    elif rate is None:
        r1 = "0.00%"
    else:
        r1 = f"{rate:.2f}".strip() + "%"
    rate1_list.append(r1)

    # RECRATE1
    if cat == "NOT REPOSSESSED YET" and subgind != ">=12 MTHS":
        rr1 = " "
    else:
        rr1 = f"{recrate:.2f}".strip() + "%"
    recrate1_list.append(rr1)

combine = combine.with_columns([
    pl.Series("RATE1",    rate1_list),
    pl.Series("RECRATE1", recrate1_list),
])

# Fix SUBGROUP / CATEGORY for non-OTHERS groups
combine = combine.with_columns([
    pl.when((pl.col("GROUPIND") != "OTHERS") & (pl.col("SUBGROUP").is_null() | (pl.col("SUBGROUP") == " ")))
      .then(pl.lit("N/A")).otherwise(pl.col("SUBGROUP")).alias("SUBGROUP"),
    pl.when((pl.col("GROUPIND") != "OTHERS") & (pl.col("CATEGORY").is_null() | (pl.col("CATEGORY") == "")))
      .then(pl.lit("N/A")).otherwise(pl.col("CATEGORY")).alias("CATEGORY"),
])

combine = combine.sort("NO")

# ─────────────────────────────────────────────
# PD / LGD computation (running totals)
# ─────────────────────────────────────────────
PD_CATS = {"SUCCESSFUL REPOSSESSION", "NOT REPOSSESSED YET", "N/A"}

totobdefault  = 0.0
totexpectedrec = 0.0

pd_list, lgd_list, pdnew_list, lgdnew_list = [], [], [], []
amte_list, amtg_list = [], []

rows_data = combine.to_dicts()

for row in rows_data:
    groupind = row.get("GROUPIND") or ""
    cat      = row.get("CATEGORY") or ""
    balance  = row.get("BALANCE") or 0.0
    obdefault  = row.get("OBDEFAULT") or 0.0
    expectedrec = row.get("EXPECTEDREC") or 0.0
    subgroup = row.get("SUBGROUP") or ""

    pd_val = lgd_val = amte = amtg = None

    if cat in PD_CATS and groupind != "TOTAL":
        balance    = balance   or 0.0
        obdefault  = obdefault  or 0.0
        expectedrec = expectedrec or 0.0

        totobdefault   += obdefault
        totexpectedrec += expectedrec
        amte = expectedrec
        amtg = obdefault

        pd_val = round((obdefault / balance) * 100, 2) if balance else 0.0
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

    if pd_val is not None:
        pdnew_list.append(f"{pd_val:,.2f}")
        lgdnew_list.append(f"{lgd_val:,.2f}")
    else:
        pdnew_list.append(None)
        lgdnew_list.append(None)

combine = combine.with_columns([
    pl.Series("PD",       pd_list,   dtype=pl.Float64),
    pl.Series("LGD",      lgd_list,  dtype=pl.Float64),
    pl.Series("PDNEW",    pdnew_list, dtype=pl.Utf8),
    pl.Series("LGDNEW",   lgdnew_list, dtype=pl.Utf8),
    pl.Series("AMTE",     amte_list, dtype=pl.Float64),
    pl.Series("AMTG",     amtg_list, dtype=pl.Float64),
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

subcom2_rows = []
subgind_no_map = {"CURRENT": 17, "1-2 MTHS": 27, "3-5 MTHS": 37, "6-11 MTHS": 47}
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

# Append subcom2 rows into combine
combine = pl.concat([combine, align_df(subcom2, ALL_COLS)], how="diagonal")

# Merge subcom1 back into combine
combine = combine.join(subcom1, on="SUBGIND", how="left")

# Compute SUBLGD and PDSUMNEW / SUBLGDNEW
sublgd_list, pdsumnew_list, sublgdnew_list = [], [], []
for row in combine.iter_rows(named=True):
    si      = row.get("SUBGIND") or ""
    amtesum = row.get("AMTESUM")
    amtgsum = row.get("AMTGSUM")
    pdsum   = row.get("PDSUM")
    groupind = row.get("GROUPIND") or ""
    balance  = row.get("BALANCE")

    sublgd = pdsumnew = sublgdnew = None

    if si in ("CURRENT", "1-2 MTHS", "3-5 MTHS", "6-11 MTHS"):
        if amtgsum and amtgsum != 0:
            sublgd = round(100 - ((amtesum / amtgsum) * 100), 2)
        else:
            sublgd = 100.0
        pdsumnew  = f"{pdsum:,.2f}"  if pdsum  is not None else None
        sublgdnew = f"{sublgd:,.2f}" if sublgd is not None else None

    if groupind == "TOTAL":
        bal = balance or 0.0
        pd_t  = round((totobdefault / bal) * 100, 2)  if bal else 0.0
        lgd_t = round(100 - ((totexpectedrec / totobdefault) * 100), 2) if totobdefault else 100.0
        row_pdnew  = f"{pd_t:,.2f}"
        row_lgdnew = f"{lgd_t:,.2f}"
    else:
        row_pdnew  = row.get("PDNEW")
        row_lgdnew = row.get("LGDNEW")

    sublgd_list.append(sublgd)
    pdsumnew_list.append(pdsumnew)
    sublgdnew_list.append(sublgdnew)

combine = combine.with_columns([
    pl.Series("SUBLGD",     sublgd_list,    dtype=pl.Float64),
    pl.Series("PDSUMNEW",   pdsumnew_list,  dtype=pl.Utf8),
    pl.Series("SUBLGDNEW",  sublgdnew_list, dtype=pl.Utf8),
])

# Recompute PDNEW/LGDNEW for TOTAL
updated_pdnew, updated_lgdnew = [], []
for row in combine.iter_rows(named=True):
    groupind = row.get("GROUPIND") or ""
    balance  = row.get("BALANCE") or 0.0
    if groupind == "TOTAL":
        pd_t  = round((totobdefault / balance) * 100, 2) if balance else 0.0
        lgd_t = round(100 - ((totexpectedrec / totobdefault) * 100), 2) if totobdefault else 100.0
        updated_pdnew.append(f"{pd_t:,.2f}")
        updated_lgdnew.append(f"{lgd_t:,.2f}")
    else:
        updated_pdnew.append(row.get("PDNEW"))
        updated_lgdnew.append(row.get("LGDNEW"))

combine = combine.with_columns([
    pl.Series("PDNEW",  updated_pdnew,  dtype=pl.Utf8),
    pl.Series("LGDNEW", updated_lgdnew, dtype=pl.Utf8),
])

combine = combine.sort("NO")

# ─────────────────────────────────────────────
# Utility formatting helpers for report
# ─────────────────────────────────────────────
def fmt_comma20(val) -> str:
    if val is None:
        return ""
    return f"{val:>20,.2f}"

def place(text: str, col: int, line_buf: list, width: int = 237) -> None:
    """Write text at 1-based column position into line_buf (list of chars)."""
    if text is None:
        return
    for i, ch in enumerate(str(text)):
        pos = col - 1 + i
        if 0 <= pos < len(line_buf):
            line_buf[pos] = ch

def render_line(buf: list) -> str:
    return "".join(buf)

def make_buf(width: int = 237) -> list:
    return [" "] * width

# ─────────────────────────────────────────────
# WRITE REPORT (OUTPUT1)
# ─────────────────────────────────────────────
LINE_WIDTH  = 237
PAGE_LENGTH = 60
ASA_NORMAL  = " "   # advance one line
ASA_TOP     = "1"   # advance to top of page
ASA_SKIP2   = "0"   # skip one line before printing

lines_out = []
line_count = 0

def emit(asa: str, text: str):
    lines_out.append(asa + text)

def check_page():
    global line_count
    if line_count >= PAGE_LENGTH:
        line_count = 0
        return ASA_TOP
    return ASA_NORMAL

def emit_blank():
    global line_count
    emit(ASA_NORMAL, "")
    line_count += 1

def emit_line(text: str, asa: str = ASA_NORMAL):
    global line_count
    asa_char = check_page() if asa == ASA_NORMAL else asa
    emit(asa_char, text)
    line_count += 1

rows = combine.to_dicts()

header_printed = False

def write_header():
    global line_count
    b = make_buf(LINE_WIDTH)
    emit_line(render_line(b), ASA_TOP)   # page eject first

    b = make_buf(LINE_WIDTH)
    place("RECOVERY RATE OF HIRE PURCHASE LOANS - PIBB", 1, b)
    emit_line(render_line(b))

    emit_line(" " * LINE_WIDTH)  # blank

    b = make_buf(LINE_WIDTH)
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
    emit_line(render_line(b))

    b = make_buf(LINE_WIDTH)
    place("ARREARS",              16,  b)
    place("AT CURRENT DATE",      34,  b)
    place("DEFAULT RATE",         59,  b)
    place("AT CURRENT DATE X DEFAULT RATE", 104, b)
    place("RATE",                139, b)
    place("AMOUNT",              149, b)
    place("RECOVERY",            169, b)
    place("RATE",                179, b)
    place("EXPECTED",            189, b)
    place("%",                   214, b)
    place("DEFAULT %",           225, b)
    emit_line(render_line(b))

    b = make_buf(LINE_WIDTH)
    place("AT CURRENT DATE", 16, b)
    place("(A)",             34, b)
    place("(B)",             59, b)
    place("(C)=(A)X(B)",    104, b)
    place("(D)",             139, b)
    place("(E)=(C)X(D)",    149, b)
    place("(F)",             169, b)
    place("(G)",             179, b)
    place("RECOVERY(PRESENT VALUE)", 189, b)
    emit_line(render_line(b))

    b = make_buf(LINE_WIDTH)
    place("(RM)",    34,  b)
    place("(RM)",   104, b)
    place("(RM)",   149, b)
    place("(RM)",   189, b)
    # indent with spaces as SAS does
    b2 = make_buf(LINE_WIDTH)
    place("   (RM)", 34,  b2)
    place("   (RM)", 104, b2)
    place("   (RM)", 149, b2)
    place("   (RM)", 189, b2)
    emit_line(render_line(b2))

    emit_line("-" * LINE_WIDTH)


write_header()

for row in rows:
    groupind = row.get("GROUPIND") or ""
    subgind  = row.get("SUBGIND") or ""
    category = row.get("CATEGORY") or ""
    no       = row.get("NO")
    group    = row.get("GROUP") or " "
    subgroup = row.get("SUBGROUP") or " "
    bal      = row.get("BAL") or ""
    rate1    = row.get("RATE1") or ""
    recrate1 = row.get("RECRATE1") or " "
    balance  = row.get("BALANCE")
    obdefault   = row.get("OBDEFAULT")
    expectedrec = row.get("EXPECTEDREC")
    pdnew    = row.get("PDNEW") or ""
    lgdnew   = row.get("LGDNEW") or ""
    pdsumnew = row.get("PDSUMNEW") or ""
    sublgdnew = row.get("SUBLGDNEW") or ""

    def _b():
        return make_buf(LINE_WIDTH)

    # ── OTHERS / CURRENT (NO 11-16) ──────────────────────────────
    if groupind == "OTHERS" and subgind == "CURRENT" and no != 17:
        b = _b()
        place(group,          1,   b)
        place(subgroup,       16,  b)
        place(bal,            34,  b)
        place(rate1,          59,  b)
        place(category,       74,  b)
        place(fmt_comma20(obdefault),   104, b)
        place(recrate1,       139, b)
        place(fmt_comma20(expectedrec), 149, b)
        place(pdnew,          214, b)
        place(lgdnew,         225, b)
        emit_line(render_line(b))
        if no == 16:
            b2 = _b()
            place("-" * 164, 74, b2)
            emit_line(render_line(b2))

    if subgind == "CURRENT" and no == 17:
        b = _b()
        place(category,   74,  b)
        place(pdsumnew,   214, b)
        place(sublgdnew,  225, b)
        emit_line(render_line(b))
        b2 = _b()
        place("-" * 164, 74, b2)
        emit_line(render_line(b2))
        emit_blank()

    # ── OTHERS / 1-2 MTHS (NO 21-26) ─────────────────────────────
    if groupind == "OTHERS" and subgind == "1-2 MTHS" and no != 27:
        b = _b()
        place(group,          1,   b)
        place(subgroup,       16,  b)
        place(bal,            34,  b)
        place(rate1,          59,  b)
        place(category,       74,  b)
        place(fmt_comma20(obdefault),   104, b)
        place(recrate1,       139, b)
        place(fmt_comma20(expectedrec), 149, b)
        place(pdnew,          214, b)
        place(lgdnew,         225, b)
        emit_line(render_line(b))
        if no == 26:
            b2 = _b()
            place("-" * 164, 74, b2)
            emit_line(render_line(b2))

    if subgind == "1-2 MTHS" and no == 27:
        b = _b()
        place(category,  74,  b)
        place(pdsumnew,  214, b)
        place(sublgdnew, 225, b)
        emit_line(render_line(b))
        b2 = _b()
        place("-" * 164, 74, b2)
        emit_line(render_line(b2))
        emit_blank()

    # ── OTHERS / 3-5 MTHS (NO 31-36) ─────────────────────────────
    if groupind == "OTHERS" and subgind == "3-5 MTHS" and no != 37:
        b = _b()
        place(group,          1,   b)
        place(subgroup,       16,  b)
        place(bal,            34,  b)
        place(rate1,          59,  b)
        place(category,       74,  b)
        place(fmt_comma20(obdefault),   104, b)
        place(recrate1,       139, b)
        place(fmt_comma20(expectedrec), 149, b)
        place(pdnew,          214, b)
        place(lgdnew,         225, b)
        emit_line(render_line(b))
        if no == 36:
            b2 = _b()
            place("-" * 164, 74, b2)
            emit_line(render_line(b2))

    if subgind == "3-5 MTHS" and no == 37:
        b = _b()
        place(category,  74,  b)
        place(pdsumnew,  214, b)
        place(sublgdnew, 225, b)
        emit_line(render_line(b))
        b2 = _b()
        place("-" * 164, 74, b2)
        emit_line(render_line(b2))
        emit_blank()

    # ── OTHERS / 6-11 MTHS (NO 41-46) ────────────────────────────
    if groupind == "OTHERS" and subgind == "6-11 MTHS" and no != 47:
        b = _b()
        place(group,          1,   b)
        place(subgroup,       16,  b)
        place(bal,            34,  b)
        place(rate1,          59,  b)
        place(category,       74,  b)
        place(fmt_comma20(obdefault),   104, b)
        place(recrate1,       139, b)
        place(fmt_comma20(expectedrec), 149, b)
        place(pdnew,          214, b)
        place(lgdnew,         225, b)
        emit_line(render_line(b))
        if no == 46:
            b2 = _b()
            place("-" * 164, 74, b2)
            emit_line(render_line(b2))

    if subgind == "6-11 MTHS" and no == 47:
        b = _b()
        place(category,  74,  b)
        place(pdsumnew,  214, b)
        place(sublgdnew, 225, b)
        emit_line(render_line(b))
        b2 = _b()
        place("-" * 164, 74, b2)
        emit_line(render_line(b2))
        emit_blank()

    # ── OTHERS / >=12 MTHS (NO 51) ───────────────────────────────
    if groupind == "OTHERS" and subgind == ">=12 MTHS":
        b = _b()
        place(group,          1,   b)
        place(subgroup,       16,  b)
        place(bal,            34,  b)
        place(rate1,          59,  b)
        place(category,       74,  b)
        place(fmt_comma20(obdefault),   104, b)
        place(recrate1,       139, b)
        place(fmt_comma20(expectedrec), 149, b)
        place(pdnew,          214, b)
        place(lgdnew,         225, b)
        emit_line(render_line(b))
        if no == 51:
            emit_blank()
            emit_line("-" * LINE_WIDTH)

    # ── IRREGULAR (NO 61) ─────────────────────────────────────────
    if groupind == "IRREGULAR":
        b = _b()
        place("IRREGULAR",    1,   b)
        place(subgroup,       16,  b)
        place(fmt_comma20(balance),     34,  b)
        place(rate1,          59,  b)
        place(category,       74,  b)
        place(fmt_comma20(obdefault),   104, b)
        place(recrate1,       139, b)
        place(fmt_comma20(expectedrec), 149, b)
        place(pdnew,          214, b)
        place(lgdnew,         225, b)
        emit_line(render_line(b))
        if no == 61:
            emit_blank()
            emit_line("-" * LINE_WIDTH)

    # ── REPOSSESSED <12 MTHS (NO 71) ─────────────────────────────
    if groupind == "REPOSSESSED" and subgind == "REPOSSESSED <12 MTHS":
        b = _b()
        place("REPOSSESSED",  1,   b)
        place(subgroup,       16,  b)
        place(fmt_comma20(balance),     34,  b)
        place(rate1,          59,  b)
        place(category,       74,  b)
        place(fmt_comma20(obdefault),   104, b)
        place(recrate1,       139, b)
        place(fmt_comma20(expectedrec), 149, b)
        place(pdnew,          214, b)
        place(lgdnew,         225, b)
        emit_line(render_line(b))
        if no == 71:
            emit_blank()

    # ── REPOSSESSED >=12 MTHS (NO 72) ────────────────────────────
    if groupind == "REPOSSESSED" and subgind == "REPOSSESSED >=12 MTHS":
        b = _b()
        place(subgroup,       16,  b)
        place(fmt_comma20(balance),     34,  b)
        place(rate1,          59,  b)
        place(category,       74,  b)
        place(fmt_comma20(obdefault),   104, b)
        place(recrate1,       139, b)
        place(fmt_comma20(expectedrec), 149, b)
        place(pdnew,          214, b)
        place(lgdnew,         225, b)
        emit_line(render_line(b))
        if no == 72:
            emit_blank()
            emit_line("-" * LINE_WIDTH)

    # ── DEFICIT (NO 81) ───────────────────────────────────────────
    if groupind == "DEFICIT":
        b = _b()
        place("DEFICIT",      1,   b)
        place(subgroup,       16,  b)
        place(fmt_comma20(balance),     34,  b)
        place(rate1,          59,  b)
        place(category,       74,  b)
        place(fmt_comma20(obdefault),   104, b)
        place(recrate1,       139, b)
        place(fmt_comma20(expectedrec), 149, b)
        place(pdnew,          214, b)
        place(lgdnew,         225, b)
        emit_line(render_line(b))
        if no == 81:
            emit_blank()
            emit_line("=" * LINE_WIDTH)

    # ── TOTAL (NO 91) ─────────────────────────────────────────────
    if groupind == "TOTAL":
        b = _b()
        place("TOTAL",        1,   b)
        place(fmt_comma20(balance),     34,  b)
        place(fmt_comma20(balance),     104, b)
        place(fmt_comma20(expectedrec), 149, b)
        place(pdnew,          214, b)
        place(lgdnew,         225, b)
        emit_line(render_line(b))
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
# GET CAP – produce NPL.ICAP1.parquet
# ─────────────────────────────────────────────
cap_cats = {
    "SUCCESSFUL REPOSSESSION",
    "N/A",
    "- >6-11 MONTHS IN ARREARS",
    "- OTHERS",
    "- 3-5 MONTHS IN ARREARS",
}

combine_with_subgind = combine.with_columns(
    pl.when(
        pl.col("SUBGIND").is_null() | (pl.col("SUBGIND") == "")
    ).then(pl.col("GROUPIND")).otherwise(pl.col("SUBGIND")).alias("SUBGIND")
)

cap_df = combine_with_subgind.filter(pl.col("CATEGORY").is_in(cap_cats))

# PROC SUMMARY → ICAP1 (SUBGIND → CATEGORY, SUM CAPROVISION)
cap_summary = cap_df.group_by("SUBGIND").agg(
    pl.col("CAPROVISION").sum()
).rename({"SUBGIND": "CATEGORY"})

# PDLGD: NO IN (17,27,37,47)
pdlgd_nos = {17, 27, 37, 47}
pdlgd_df = (
    combine_with_subgind
    .filter(pl.col("NO").is_in(pdlgd_nos))
    .select(["SUBGIND", "PDSUM", "SUBLGD"])
    .rename({"SUBGIND": "CATEGORY", "SUBLGD": "SUBLGD"})
)

# PDLGD2: NO IN (51,61,71,72,81)
pdlgd2_nos = {51, 61, 71, 72, 81}
pdlgd2_df = (
    combine_with_subgind
    .filter(pl.col("NO").is_in(pdlgd2_nos))
    .select(["SUBGIND", "PD", "LGD"])
    .rename({"SUBGIND": "CATEGORY", "PD": "PDSUM", "LGD": "SUBLGD"})
)

# Merge: ICAP1 ← PDLGD ← PDLGD2
pdlgd_combined = pl.concat([pdlgd_df, pdlgd2_df], how="diagonal")
icap1 = cap_summary.join(pdlgd_combined, on="CATEGORY", how="left")
icap1 = icap1.rename({"PDSUM": "PD", "SUBLGD": "LGD"})

icap1.write_parquet(ICAP1_PATH)

print(f"Report written to : {OUTPUT1_PATH}")
print(f"ICAP1 written to  : {ICAP1_PATH}")
