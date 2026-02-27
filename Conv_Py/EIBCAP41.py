# !/usr/bin/env python3
"""
Program: EIBCAP41
Purpose: Generate CAP Report by Company for Hire Purchase Loans
"""

import duckdb
import polars as pl
import os
from datetime import date

# ─────────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────────
BASE_DIR       = r"C:\data"
LOAN_DIR       = os.path.join(BASE_DIR, "loan")
RATE_DIR       = os.path.join(BASE_DIR, "rate")
HP_DIR         = os.path.join(BASE_DIR, "hp")
CCRIS_DIR      = os.path.join(BASE_DIR, "ccris")
NPL_DIR        = os.path.join(BASE_DIR, "npl")
OUTPUT_DIR     = os.path.join(BASE_DIR, "output")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(NPL_DIR, exist_ok=True)

OUTPUT1_PATH   = os.path.join(OUTPUT_DIR, "EIBCAP41.txt")
CAP1_PATH      = os.path.join(NPL_DIR,    "cap1.parquet")

con = duckdb.connect()

# ─────────────────────────────────────────────
# REPTDATE – derive REPTMON, NOWK, REPTYEAR
# ─────────────────────────────────────────────
reptdate_df = con.execute(
    f"SELECT REPTDATE FROM read_parquet('{LOAN_DIR}/reptdate.parquet') LIMIT 1"
).pl()

reptdate_val: date = reptdate_df["REPTDATE"][0]
day_val   = reptdate_val.day
month_val = reptdate_val.month
year_val  = reptdate_val.year % 100   # YEAR2. → 2-digit year

if   1  <= day_val <=  8:  wk = "1"
elif 9  <= day_val <= 15:  wk = "2"
elif 16 <= day_val <= 22:  wk = "3"
else:                       wk = "4"

REPTMON  = f"{month_val:02d}"
NOWK     = wk
REPTYEAR = f"{year_val:02d}"

# ─────────────────────────────────────────────
# RECRATE – recovery ratio macro variable
# ─────────────────────────────────────────────
# TO BE FURNISHED -RATE
recrate_df = con.execute(
    f"SELECT RECOVERYRATIO FROM read_parquet('{RATE_DIR}/recrate.parquet') LIMIT 1"
).pl()
RECRATE: float = recrate_df["RECOVERYRATIO"][0]

# ─────────────────────────────────────────────
# LAYOUT skeleton
# ─────────────────────────────────────────────
layout_df = pl.DataFrame({
    "GROUPIND": ["OTHERS", "IRREGULAR", "REPOSSESSED", "DEFICIT", "TOTAL"]
})

# ─────────────────────────────────────────────
# HP base dataset
# ─────────────────────────────────────────────
hp_parquet = os.path.join(HP_DIR, f"hp{REPTMON}{NOWK}{REPTYEAR}.parquet")
hp_df = con.execute(f"SELECT * FROM read_parquet('{hp_parquet}')").pl()

# ─────────────────────────────────────────────
# CREDSUB – CCRIS sub-account data
# ─────────────────────────────────────────────
credsub_parquet = os.path.join(CCRIS_DIR, f"credmsubac{REPTMON}{REPTYEAR}.parquet")
credsub_df = con.execute(f"""
    SELECT ACCTNUM, DAYSARR, NOTENO
    FROM read_parquet('{credsub_parquet}')
    WHERE FACILITY IN ('34331','34332')
""").pl().rename({"ACCTNUM": "ACCTNO", "DAYSARR": "DAYARR"})

# Deduplicate: keep first record per ACCTNO+NOTENO after descending DAYARR sort
credsub_df = (
    credsub_df
    .sort(["ACCTNO", "NOTENO", "DAYARR"], descending=[False, False, True])
    .unique(subset=["ACCTNO", "NOTENO"], keep="first")
)

# Merge HP with CREDSUB (left join)
hp_df = hp_df.join(credsub_df, on=["ACCTNO", "NOTENO"], how="left")

# ─────────────────────────────────────────────
# SPLIT into sub-datasets
# ─────────────────────────────────────────────
VALID_PRODUCTS = [700, 705, 720, 725, 380, 381]
EXCL_BORSTAT   = ["F", "I", "R", "E", "W", "Z"]

base = hp_df.filter(
    pl.col("PRODUCT").is_in(VALID_PRODUCTS) & (pl.col("BALANCE") > 0)
)

current_df = base.filter(
    (pl.col("DAYARR") <= 30) &
    (~pl.col("BORSTAT").is_in(EXCL_BORSTAT)) &
    (pl.col("USER5") != "N") &
    (pl.col("PAIDIND") == "M")
)
month1t2_df = base.filter(
    (pl.col("DAYARR") >= 31) & (pl.col("DAYARR") <= 89) &
    (~pl.col("BORSTAT").is_in(EXCL_BORSTAT)) &
    (pl.col("USER5") != "N") &
    (pl.col("PAIDIND") == "M")
)
month3t5_df = base.filter(
    (~pl.col("BORSTAT").is_in(EXCL_BORSTAT)) &
    (
        ((pl.col("USER5") == "N") & (pl.col("DAYARR") <= 182)) |
        ((pl.col("DAYARR") >= 90) & (pl.col("DAYARR") <= 182))
    ) &
    (pl.col("PAIDIND") == "M")
)
month6t11_df = base.filter(
    (~pl.col("BORSTAT").is_in(EXCL_BORSTAT)) &
    (
        ((pl.col("USER5") == "N") & (pl.col("DAYARR") >= 183) & (pl.col("DAYARR") <= 364)) |
        ((pl.col("DAYARR") >= 183) & (pl.col("DAYARR") <= 364))
    ) &
    (pl.col("PAIDIND") == "M")
)
month12ab_df = base.filter(
    (~pl.col("BORSTAT").is_in(EXCL_BORSTAT)) &
    (
        ((pl.col("USER5") == "N") & (pl.col("DAYARR") >= 365)) |
        (pl.col("DAYARR") >= 365)
    ) &
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
# Helper: build category rows from one source df
# ─────────────────────────────────────────────
def build_group_rows(src: pl.DataFrame, rows_spec: list[dict]) -> pl.DataFrame:
    """
    For each row_spec, multiply each source row by the spec and compute
    OBDEFAULT / EXPECTEDREC / CAPROVISION, then aggregate (sum) by class keys.
    rows_spec items contain: CATEGORY, GROUPIND, SUBGIND, RECRATE, RATE,
    cap_formula (str: 'diff' | 'zero'), expectedrec_formula (str: 'calc' | 'keep')
    """
    frames = []
    for spec in rows_spec:
        rate   = spec["RATE"]
        recrate= spec["RECRATE"]
        cap_f  = spec["cap_formula"]
        er_f   = spec["expectedrec_formula"]

        tmp = src.with_columns([
            pl.lit(spec["CATEGORY"]).alias("CATEGORY"),
            pl.lit(spec["GROUPIND"]).alias("GROUPIND"),
            pl.lit(spec["SUBGIND"]).alias("SUBGIND"),
            pl.lit(recrate).cast(pl.Float64).alias("RECRATE"),
            pl.lit(rate).cast(pl.Float64).alias("RATE"),
            (pl.col("BALANCE") * (rate / 100)).alias("OBDEFAULT"),
        ])
        if er_f == "calc":
            tmp = tmp.with_columns(
                (pl.col("OBDEFAULT") * (recrate / 100)).alias("EXPECTEDREC")
            )
        else:  # keep – EXPECTEDREC stays as previously computed (carried)
            # In SAS: EXPECTEDREC=EXPECTEDREC; means keep previous row value.
            # After grouping by new CATEGORY key, we set it to 0 initially;
            # the "NOT REPOSSESSED YET" row's EXPECTEDREC is overwritten by SUMREPO later.
            tmp = tmp.with_columns(
                pl.lit(0.0).alias("EXPECTEDREC")
            )
        if cap_f == "diff":
            tmp = tmp.with_columns(
                (pl.col("OBDEFAULT") - pl.col("EXPECTEDREC")).alias("CAPROVISION")
            )
        else:  # zero
            tmp = tmp.with_columns(pl.lit(0.0).alias("CAPROVISION"))

        frames.append(tmp)

    combined = pl.concat(frames)
    agg = combined.group_by(
        ["GROUPIND", "SUBGIND", "CATEGORY", "RATE", "RECRATE"]
    ).agg([
        pl.sum("OBDEFAULT"),
        pl.sum("EXPECTEDREC"),
        pl.sum("CAPROVISION"),
        pl.sum("BALANCE"),
    ])
    return agg


def fix_not_repossessed_expectedrec(grp_df: pl.DataFrame,
                                    groupind: str, subgind: str,
                                    rate: float) -> pl.DataFrame:
    """
    Recompute EXPECTEDREC for 'NOT REPOSSESSED YET' row as sum of
    EXPECTEDREC from the three sub-categories.
    """
    sub_cats = ["- 3-5 MONTHS IN ARREARS", "- >6-11 MONTHS IN ARREARS", "- OTHERS"]
    sumrec = grp_df.filter(pl.col("CATEGORY").is_in(sub_cats))["EXPECTEDREC"].sum()
    # Update the NOT REPOSSESSED YET row
    grp_df = grp_df.with_columns(
        pl.when(
            (pl.col("CATEGORY") == "NOT REPOSSESSED YET") &
            (pl.col("GROUPIND") == groupind) &
            (pl.col("SUBGIND") == subgind)
        ).then(pl.lit(sumrec))
        .otherwise(pl.col("EXPECTEDREC"))
        .alias("EXPECTEDREC")
    )
    return grp_df


def assign_no_and_labels(grp_df: pl.DataFrame, mapping: dict) -> pl.DataFrame:
    """mapping: {CATEGORY: (GROUP, SUBGROUP, NO)}"""
    rows = []
    for row in grp_df.iter_rows(named=True):
        cat = row["CATEGORY"]
        if cat in mapping:
            grp, sub, no = mapping[cat]
            row["GROUP"]    = grp
            row["SUBGROUP"] = sub
            row["NO"]       = no
            row["BAL"]      = (
                f"{row['BALANCE']:,.2f}" if grp not in ("", " ", None) or sub not in ("", " ", None) else ""
            )
        else:
            row["GROUP"]    = " "
            row["SUBGROUP"] = " "
            row["NO"]       = None
            row["BAL"]      = ""
        rows.append(row)
    return pl.DataFrame(rows)


# ─────────────────────────────────────────────
# CURRENT GROUP
# ─────────────────────────────────────────────
current_specs = [
    {"CATEGORY": "SUCCESSFUL REPOSSESSION",  "GROUPIND": "OTHERS", "SUBGIND": "CURRENT",
     "RECRATE": RECRATE,  "RATE": 0.23,  "cap_formula": "diff", "expectedrec_formula": "calc"},
    {"CATEGORY": "- 3-5 MONTHS IN ARREARS",  "GROUPIND": "OTHERS", "SUBGIND": "CURRENT",
     "RECRATE": 25.00,    "RATE": 0.06,  "cap_formula": "diff", "expectedrec_formula": "calc"},
    {"CATEGORY": "NOT REPOSSESSED YET",       "GROUPIND": "OTHERS", "SUBGIND": "CURRENT",
     "RECRATE": 0.00,     "RATE": 0.14,  "cap_formula": "zero", "expectedrec_formula": "keep"},
    # *CAPROVISION=0 (commented out in SAS, using CAPROVISION=OBDEFAULT-EXPECTEDREC)
    {"CATEGORY": "- >6-11 MONTHS IN ARREARS","GROUPIND": "OTHERS", "SUBGIND": "CURRENT",
     "RECRATE": 25.00,    "RATE": 0.07,  "cap_formula": "diff", "expectedrec_formula": "calc"},
    {"CATEGORY": "- OTHERS",                  "GROUPIND": "OTHERS", "SUBGIND": "CURRENT",
     "RECRATE": 25.00,    "RATE": 0.01,  "cap_formula": "diff", "expectedrec_formula": "calc"},
    {"CATEGORY": "CONTINUE PAYING",           "GROUPIND": "OTHERS", "SUBGIND": "CURRENT",
     "RECRATE": 100.00,   "RATE": 99.63, "cap_formula": "zero", "expectedrec_formula": "calc"},
]
current_grp = build_group_rows(current_df, current_specs)
current_grp = fix_not_repossessed_expectedrec(current_grp, "OTHERS", "CURRENT", 0.14)

current_mapping = {
    "CONTINUE PAYING":           ("OTHERS", "CURRENT",  11),
    "SUCCESSFUL REPOSSESSION":   (" ",       " ",        12),
    "NOT REPOSSESSED YET":        (" ",       " ",        13),
    "- 3-5 MONTHS IN ARREARS":   (" ",       " ",        14),
    "- >6-11 MONTHS IN ARREARS": (" ",       " ",        15),
    "- OTHERS":                   (" ",       " ",        16),
}
current_grp = assign_no_and_labels(current_grp, current_mapping)

# ─────────────────────────────────────────────
# 3-5 MONTHS GROUP
# ─────────────────────────────────────────────
month3t5_specs = [
    {"CATEGORY": "CONTINUE PAYING",           "GROUPIND": "OTHERS", "SUBGIND": "3-5 MTHS",
     "RECRATE": 100.00,   "RATE": 0.00,   "cap_formula": "zero", "expectedrec_formula": "calc"},
    {"CATEGORY": "SUCCESSFUL REPOSSESSION",   "GROUPIND": "OTHERS", "SUBGIND": "3-5 MTHS",
     "RECRATE": RECRATE,  "RATE": 60.22,  "cap_formula": "diff", "expectedrec_formula": "calc"},
    {"CATEGORY": "- 3-5 MONTHS IN ARREARS",   "GROUPIND": "OTHERS", "SUBGIND": "3-5 MTHS",
     "RECRATE": 25.00,    "RATE": 5.43,   "cap_formula": "diff", "expectedrec_formula": "calc"},
    {"CATEGORY": "NOT REPOSSESSED YET",        "GROUPIND": "OTHERS", "SUBGIND": "3-5 MTHS",
     "RECRATE": 0.00,     "RATE": 39.78,  "cap_formula": "zero", "expectedrec_formula": "keep"},
    {"CATEGORY": "- >6-11 MONTHS IN ARREARS", "GROUPIND": "OTHERS", "SUBGIND": "3-5 MTHS",
     "RECRATE": 25.00,    "RATE": 5.92,   "cap_formula": "diff", "expectedrec_formula": "calc"},
    {"CATEGORY": "- OTHERS",                   "GROUPIND": "OTHERS", "SUBGIND": "3-5 MTHS",
     "RECRATE": 25.00,    "RATE": 28.43,  "cap_formula": "diff", "expectedrec_formula": "calc"},
]
month3t5_grp = build_group_rows(month3t5_df, month3t5_specs)
month3t5_grp = fix_not_repossessed_expectedrec(month3t5_grp, "OTHERS", "3-5 MTHS", 39.78)

month3t5_mapping = {
    "CONTINUE PAYING":           (" ",  "3-5 MTHS", 31),
    "SUCCESSFUL REPOSSESSION":   (" ",  " ",         32),
    "NOT REPOSSESSED YET":        (" ",  " ",         33),
    "- 3-5 MONTHS IN ARREARS":   (" ",  " ",         34),
    "- >6-11 MONTHS IN ARREARS": (" ",  " ",         35),
    "- OTHERS":                   (" ",  " ",         36),
}
month3t5_grp = assign_no_and_labels(month3t5_grp, month3t5_mapping)

# ─────────────────────────────────────────────
# 6-11 MONTHS GROUP
# ─────────────────────────────────────────────
month6t11_specs = [
    {"CATEGORY": "CONTINUE PAYING",           "GROUPIND": "OTHERS", "SUBGIND": "6-11 MTHS",
     "RECRATE": 100.00,   "RATE": 0.00,   "cap_formula": "zero", "expectedrec_formula": "calc"},
    {"CATEGORY": "SUCCESSFUL REPOSSESSION",   "GROUPIND": "OTHERS", "SUBGIND": "6-11 MTHS",
     "RECRATE": RECRATE,  "RATE": 33.59,  "cap_formula": "diff", "expectedrec_formula": "calc"},
    {"CATEGORY": "- 3-5 MONTHS IN ARREARS",   "GROUPIND": "OTHERS", "SUBGIND": "6-11 MTHS",
     "RECRATE": 25.00,    "RATE": 1.38,   "cap_formula": "diff", "expectedrec_formula": "calc"},
    {"CATEGORY": "NOT REPOSSESSED YET",        "GROUPIND": "OTHERS", "SUBGIND": "6-11 MTHS",
     "RECRATE": 0.00,     "RATE": 66.41,  "cap_formula": "zero", "expectedrec_formula": "keep"},
    {"CATEGORY": "- >6-11 MONTHS IN ARREARS", "GROUPIND": "OTHERS", "SUBGIND": "6-11 MTHS",
     "RECRATE": 25.00,    "RATE": 2.44,   "cap_formula": "diff", "expectedrec_formula": "calc"},
    {"CATEGORY": "- OTHERS",                   "GROUPIND": "OTHERS", "SUBGIND": "6-11 MTHS",
     "RECRATE": 25.00,    "RATE": 62.59,  "cap_formula": "diff", "expectedrec_formula": "calc"},
]
month6t11_grp = build_group_rows(month6t11_df, month6t11_specs)
month6t11_grp = fix_not_repossessed_expectedrec(month6t11_grp, "OTHERS", "6-11 MTHS", 66.41)

month6t11_mapping = {
    "CONTINUE PAYING":           (" ",  "6-11 MTHS", 41),
    "SUCCESSFUL REPOSSESSION":   (" ",  " ",          42),
    "NOT REPOSSESSED YET":        (" ",  " ",          43),
    "- 3-5 MONTHS IN ARREARS":   (" ",  " ",          44),
    "- >6-11 MONTHS IN ARREARS": (" ",  " ",          45),
    "- OTHERS":                   (" ",  " ",          46),
}
month6t11_grp = assign_no_and_labels(month6t11_grp, month6t11_mapping)

# ─────────────────────────────────────────────
# 1-2 MONTHS GROUP
# ─────────────────────────────────────────────
month1t2_specs = [
    {"CATEGORY": "CONTINUE PAYING",           "GROUPIND": "OTHERS", "SUBGIND": "1-2 MTHS",
     "RECRATE": 100.00,   "RATE": 93.09,  "cap_formula": "zero", "expectedrec_formula": "calc"},
    {"CATEGORY": "SUCCESSFUL REPOSSESSION",   "GROUPIND": "OTHERS", "SUBGIND": "1-2 MTHS",
     "RECRATE": RECRATE,  "RATE": 4.06,   "cap_formula": "diff", "expectedrec_formula": "calc"},
    {"CATEGORY": "- 3-5 MONTHS IN ARREARS",   "GROUPIND": "OTHERS", "SUBGIND": "1-2 MTHS",
     "RECRATE": 25.00,    "RATE": 0.91,   "cap_formula": "diff", "expectedrec_formula": "calc"},
    {"CATEGORY": "NOT REPOSSESSED YET",        "GROUPIND": "OTHERS", "SUBGIND": "1-2 MTHS",
     "RECRATE": 0.00,     "RATE": 2.85,   "cap_formula": "zero", "expectedrec_formula": "keep"},
    {"CATEGORY": "- >6-11 MONTHS IN ARREARS", "GROUPIND": "OTHERS", "SUBGIND": "1-2 MTHS",
     "RECRATE": 25.00,    "RATE": 1.12,   "cap_formula": "diff", "expectedrec_formula": "calc"},
    {"CATEGORY": "- OTHERS",                   "GROUPIND": "OTHERS", "SUBGIND": "1-2 MTHS",
     "RECRATE": 25.00,    "RATE": 0.82,   "cap_formula": "diff", "expectedrec_formula": "calc"},
]
month1t2_grp = build_group_rows(month1t2_df, month1t2_specs)
month1t2_grp = fix_not_repossessed_expectedrec(month1t2_grp, "OTHERS", "1-2 MTHS", 2.85)

month1t2_mapping = {
    "CONTINUE PAYING":           (" ",  "1-2 MTHS", 21),
    "SUCCESSFUL REPOSSESSION":   (" ",  " ",         22),
    "NOT REPOSSESSED YET":        (" ",  " ",         23),
    "- 3-5 MONTHS IN ARREARS":   (" ",  " ",         24),
    "- >6-11 MONTHS IN ARREARS": (" ",  " ",         25),
    "- OTHERS":                   (" ",  " ",         26),
}
month1t2_grp = assign_no_and_labels(month1t2_grp, month1t2_mapping)

# ─────────────────────────────────────────────
# 12 MONTHS & ABOVE GROUP
# ─────────────────────────────────────────────
month12ab_grp = (
    month12ab_df
    .with_columns([
        pl.lit("NOT REPOSSESSED YET").alias("CATEGORY"),
        pl.lit("OTHERS").alias("GROUPIND"),
        pl.lit(">=12 MTHS").alias("SUBGIND"),
        pl.lit(0.00).cast(pl.Float64).alias("RECRATE"),
        pl.lit(100.00).cast(pl.Float64).alias("RATE"),
        pl.col("BALANCE").alias("OBDEFAULT"),
        pl.lit(0.0).alias("EXPECTEDREC"),
        pl.lit(0.0).alias("CAPROVISION"),
        # *CAPROVISION=OBDEFAULT-EXPECTEDREC  (commented out in SAS)
    ])
    .group_by(["GROUPIND", "SUBGIND", "CATEGORY", "RATE", "RECRATE"])
    .agg([pl.sum("OBDEFAULT"), pl.sum("EXPECTEDREC"), pl.sum("CAPROVISION"), pl.sum("BALANCE")])
)
month12ab_grp = assign_no_and_labels(month12ab_grp, {
    "NOT REPOSSESSED YET": (" ", ">=12 MTHS", 51),
})

# ─────────────────────────────────────────────
# IRREGULAR GROUP
# ─────────────────────────────────────────────
irregular_grp = (
    irregular_df
    .with_columns([
        pl.lit(0.00).cast(pl.Float64).alias("RECRATE"),
        pl.lit("IRREGULAR").alias("GROUPIND"),
        pl.col("BALANCE").alias("OBDEFAULT"),
        (pl.col("BALANCE") * 0.0).alias("EXPECTEDREC"),
        pl.col("BALANCE").alias("CAPROVISION"),
        pl.lit(61).alias("NO"),
    ])
    .group_by(["GROUPIND", "NO", "RECRATE"])
    .agg([pl.sum("OBDEFAULT"), pl.sum("EXPECTEDREC"), pl.sum("CAPROVISION"), pl.sum("BALANCE")])
    .with_columns(pl.lit("IRREGUALR").alias("GROUP"))
)

# ─────────────────────────────────────────────
# REPOSSESSED < 12 MONTHS
# ─────────────────────────────────────────────
repossessed_l12_grp = (
    repossessed_l12_df
    .with_columns([
        pl.lit("REPOSSESSED").alias("GROUPIND"),
        pl.lit("REPOSSESSED <12 MTHS").alias("SUBGIND"),
        pl.lit("<12 MTHS ").alias("SUBGROUP"),
        pl.lit(float(RECRATE)).alias("RECRATE"),
        pl.col("BALANCE").alias("OBDEFAULT"),
        (pl.col("BALANCE") * (RECRATE / 100)).alias("EXPECTEDREC"),
        (pl.col("BALANCE") - pl.col("BALANCE") * (RECRATE / 100)).alias("CAPROVISION"),
    ])
    .group_by(["GROUPIND", "SUBGIND", "SUBGROUP", "RECRATE"])
    .agg([pl.sum("OBDEFAULT"), pl.sum("EXPECTEDREC"), pl.sum("CAPROVISION"), pl.sum("BALANCE")])
)

# ─────────────────────────────────────────────
# REPOSSESSED >= 12 MONTHS
# ─────────────────────────────────────────────
repossessed_m12_grp = (
    repossessed_m12_df
    .with_columns([
        pl.lit("REPOSSESSED").alias("GROUPIND"),
        pl.lit("REPOSSESSED >=12 MTHS").alias("SUBGIND"),
        pl.lit(">=12 MTHS").alias("SUBGROUP"),
        pl.lit(0.00).cast(pl.Float64).alias("RECRATE"),
        pl.lit(100.00).cast(pl.Float64).alias("RATE"),
        pl.col("BALANCE").alias("OBDEFAULT"),
        pl.lit(0.0).alias("EXPECTEDREC"),
        pl.lit(0.0).alias("CAPROVISION"),
    ])
    .group_by(["GROUPIND", "SUBGIND", "SUBGROUP", "RATE", "RECRATE"])
    .agg([pl.sum("OBDEFAULT"), pl.sum("EXPECTEDREC"), pl.sum("CAPROVISION"), pl.sum("BALANCE")])
)

# Combine REPOSSESSED
def _ensure_col(df: pl.DataFrame, col: str, dtype=pl.Utf8, default=None) -> pl.DataFrame:
    if col not in df.columns:
        df = df.with_columns(pl.lit(default).cast(dtype).alias(col))
    return df

repossessed_l12_grp = repossessed_l12_grp.with_columns([
    pl.lit("REPOSSESSED").alias("GROUP"),
    pl.lit(71).alias("NO"),
])
repossessed_m12_grp = repossessed_m12_grp.with_columns([
    pl.lit("REPOSSESSED").alias("GROUP"),
    pl.lit(72).alias("NO"),
])

# Align columns
for col in ["RATE", "CATEGORY", "BAL"]:
    repossessed_l12_grp = _ensure_col(repossessed_l12_grp, col, pl.Utf8, None)
    repossessed_m12_grp = _ensure_col(repossessed_m12_grp, col, pl.Utf8, None)

repossessed_grp = pl.concat([repossessed_l12_grp, repossessed_m12_grp], how="diagonal")

# ─────────────────────────────────────────────
# DEFICIT GROUP
# ─────────────────────────────────────────────
deficit_grp = (
    deficit_df
    .with_columns([
        pl.lit(0.00).cast(pl.Float64).alias("RECRATE"),
        pl.lit("DEFICIT").alias("GROUPIND"),
        pl.col("BALANCE").alias("OBDEFAULT"),
        pl.lit(0.0).alias("EXPECTEDREC"),
        pl.col("BALANCE").alias("CAPROVISION"),
        pl.lit(81).alias("NO"),
    ])
    .group_by(["GROUPIND", "NO", "RECRATE"])
    .agg([pl.sum("OBDEFAULT"), pl.sum("EXPECTEDREC"), pl.sum("CAPROVISION"), pl.sum("BALANCE")])
    .with_columns(pl.lit("DEFICIT").alias("GROUP"))
)

# ─────────────────────────────────────────────
# COMBINE all groups
# ─────────────────────────────────────────────
def _unify(df: pl.DataFrame) -> pl.DataFrame:
    needed = {
        "GROUPIND": (pl.Utf8, None), "SUBGIND": (pl.Utf8, None),
        "CATEGORY": (pl.Utf8, None), "RATE": (pl.Float64, None),
        "RECRATE": (pl.Float64, 0.0), "OBDEFAULT": (pl.Float64, 0.0),
        "EXPECTEDREC": (pl.Float64, 0.0), "CAPROVISION": (pl.Float64, 0.0),
        "BALANCE": (pl.Float64, 0.0), "GROUP": (pl.Utf8, " "),
        "SUBGROUP": (pl.Utf8, " "), "NO": (pl.Int64, None),
        "BAL": (pl.Utf8, ""),
    }
    for col, (dtype, default) in needed.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(default).cast(dtype).alias(col))
    return df.select(list(needed.keys()))

parts = [current_grp, month1t2_grp, month3t5_grp, month6t11_grp,
         month12ab_grp, irregular_grp, repossessed_grp, deficit_grp]
combine = pl.concat([_unify(p) for p in parts], how="diagonal")

# ─────────────────────────────────────────────
# TOTAL row(s)
# ─────────────────────────────────────────────
total_rows = combine.clone().with_columns(pl.lit("TOTAL").alias("GROUPIND"))

# If GROUP=' ' AND SUBGROUP=' ' → BALANCE=null
total_rows = total_rows.with_columns(
    pl.when((pl.col("GROUP") == " ") & (pl.col("SUBGROUP") == " "))
    .then(pl.lit(None).cast(pl.Float64))
    .otherwise(pl.col("BALANCE"))
    .alias("BALANCE")
)
# Categories that carry null EXPECTEDREC for total
null_er_cats = ["- 3-5 MONTHS IN ARREARS", "- >6-11 MONTHS IN ARREARS", "- OTHERS"]
total_rows = total_rows.with_columns(
    pl.when(pl.col("CATEGORY").is_in(null_er_cats))
    .then(pl.lit(None).cast(pl.Float64))
    .otherwise(pl.col("EXPECTEDREC"))
    .alias("EXPECTEDREC")
)

total_summary = (
    total_rows
    .group_by("GROUPIND")
    .agg([
        pl.sum("BALANCE"),
        pl.sum("OBDEFAULT"),
        pl.sum("EXPECTEDREC"),
    ])
    .with_columns(pl.lit(91).alias("NO"))
)

combine = pl.concat([combine, _unify(total_summary)], how="diagonal")

# ─────────────────────────────────────────────
# Merge LAYOUT
# ─────────────────────────────────────────────
combine = combine.join(layout_df, on="GROUPIND", how="left")

# Assign fixed NO for special GROUPIND
combine = combine.with_columns([
    pl.when(pl.col("GROUPIND") == "IRREGULAR").then(pl.lit(61))
      .when(pl.col("GROUPIND") == "DEFICIT").then(pl.lit(81))
      .when(pl.col("GROUPIND") == "TOTAL").then(pl.lit(91))
      .otherwise(pl.col("NO")).alias("NO"),
    pl.when(pl.col("RECRATE").is_null()).then(pl.lit(0.0)).otherwise(pl.col("RECRATE")).alias("RECRATE"),
])

# ─────────────────────────────────────────────
# RATE1 and RECRATE1 formatted strings
# ─────────────────────────────────────────────
def fmt_pct(val, na_override=False) -> str:
    if val is None or na_override:
        return "N/A"
    return f"{val:.2f}".rstrip("0").rstrip(".") + "%" if False else f"{val:.2f}%"


combine = combine.with_columns([
    pl.when(pl.col("RATE").is_null() & (pl.col("GROUPIND") != "OTHERS"))
      .then(pl.lit("N/A"))
      .when(pl.col("RATE").is_null())
      .then(pl.lit(""))
      .otherwise(pl.col("RATE").map_elements(lambda x: f"{x:.2f}%", return_dtype=pl.Utf8))
      .alias("RATE1"),

    pl.when(
        (pl.col("CATEGORY") == "NOT REPOSSESSED YET") &
        (pl.col("SUBGIND") != ">=12 MTHS")
    ).then(pl.lit(" "))
    .otherwise(
        pl.col("RECRATE").map_elements(lambda x: f"{x:.2f}%", return_dtype=pl.Utf8)
    )
    .alias("RECRATE1"),
])

# Non-OTHERS fixes
combine = combine.with_columns([
    pl.when((pl.col("GROUPIND") != "OTHERS") & pl.col("SUBGROUP").is_null())
      .then(pl.lit("N/A"))
      .otherwise(pl.col("SUBGROUP"))
      .alias("SUBGROUP"),
    pl.when((pl.col("GROUPIND") != "OTHERS") & pl.col("CATEGORY").is_null())
      .then(pl.lit("N/A"))
      .otherwise(pl.col("CATEGORY"))
      .alias("CATEGORY"),
])

combine = combine.sort("NO")

# ─────────────────────────────────────────────
# PD / LGD calculations + TOTOBDEFAULT / TOTEXPECTEDREC
# ─────────────────────────────────────────────
target_cats = {"SUCCESSFUL REPOSSESSION", "NOT REPOSSESSED YET", "N/A"}

combine = combine.with_columns([
    pl.lit(None).cast(pl.Float64).alias("PD"),
    pl.lit(None).cast(pl.Float64).alias("LGD"),
    pl.lit(None).cast(pl.Float64).alias("AMTE"),
    pl.lit(None).cast(pl.Float64).alias("AMTG"),
    pl.lit("").alias("PDNEW"),
    pl.lit("").alias("LGDNEW"),
])

rows_list = combine.to_dicts()
totobdefault   = 0.0
totexpectedrec = 0.0

for row in rows_list:
    if row["CATEGORY"] in target_cats and row["GROUPIND"] != "TOTAL":
        bal = row["BALANCE"] or 0.0
        ob  = row["OBDEFAULT"] or 0.0
        er  = row["EXPECTEDREC"] or 0.0
        totobdefault   += ob
        totexpectedrec += er
        row["AMTE"] = er
        row["AMTG"] = ob
        pd_val  = round((ob / bal * 100), 2) if bal else 0.0
        lgd_val = round(100 - (er / ob * 100), 2) if ob else 100.0
        if row["CATEGORY"] == "N/A":
            pd_val = 100.0
        if row["GROUPIND"] == "IRREGULAR" or (
            row["GROUPIND"] == "REPOSSESSED" and row.get("SUBGROUP") == ">=12 MTHS"
        ):
            lgd_val = 100.0
        row["PD"]     = pd_val
        row["LGD"]    = lgd_val
        row["PDNEW"]  = f"{pd_val:,.2f}"
        row["LGDNEW"] = f"{lgd_val:,.2f}"

combine = pl.DataFrame(rows_list)

# ─────────────────────────────────────────────
# SUBCOM – sub-group PD/LGD summary (NO 17,27,37,47)
# ─────────────────────────────────────────────
subgind_sub_nos = {"CURRENT": 17, "1-2 MTHS": 27, "3-5 MTHS": 37, "6-11 MTHS": 47}

subcom1_rows = []
for subgind_val, sub_no in subgind_sub_nos.items():
    sub = combine.filter(
        (pl.col("SUBGIND") == subgind_val) &
        pl.col("CATEGORY").is_in(target_cats) &
        (pl.col("GROUPIND") != "TOTAL")
    )
    pdsum   = sub["PD"].sum()   if sub.height else 0.0
    amtesum = sub["AMTE"].sum() if sub.height else 0.0
    amtgsum = sub["AMTG"].sum() if sub.height else 0.0
    subcom1_rows.append({
        "SUBGIND": subgind_val, "NO": sub_no, "CATEGORY": "SUB",
        "PDSUM": pdsum, "AMTESUM": amtesum, "AMTGSUM": amtgsum,
    })

subcom1 = pl.DataFrame(subcom1_rows) if subcom1_rows else pl.DataFrame()

# Append SUB rows to combine
if subcom1.height:
    sub_rows_for_combine = subcom1.select(["SUBGIND", "NO", "CATEGORY", "PDSUM", "AMTESUM", "AMTGSUM"])
    combine = combine.join(
        subcom1.select(["SUBGIND", "PDSUM", "AMTESUM", "AMTGSUM"]),
        on="SUBGIND", how="left"
    )
    # Add SUB category rows
    sub_combine_rows = []
    for r in subcom1.iter_rows(named=True):
        sub_combine_rows.append({
            "GROUPIND": None, "SUBGIND": r["SUBGIND"], "CATEGORY": "SUB",
            "NO": r["NO"], "PDSUM": r["PDSUM"], "AMTESUM": r["AMTESUM"], "AMTGSUM": r["AMTGSUM"],
            "BALANCE": None, "OBDEFAULT": None, "EXPECTEDREC": None, "CAPROVISION": None,
            "GROUP": None, "SUBGROUP": None, "RATE": None, "RECRATE": None,
            "RATE1": "", "RECRATE1": "", "BAL": "", "PD": None, "LGD": None,
            "AMTE": None, "AMTG": None, "PDNEW": "", "LGDNEW": "",
        })
    sub_add = pl.DataFrame(sub_combine_rows)
    combine = pl.concat([combine, sub_add], how="diagonal")
else:
    combine = combine.with_columns([
        pl.lit(None).cast(pl.Float64).alias("PDSUM"),
        pl.lit(None).cast(pl.Float64).alias("AMTESUM"),
        pl.lit(None).cast(pl.Float64).alias("AMTGSUM"),
    ])

combine = combine.sort("NO")

# Compute SUBLGD / PDSUMNEW / SUBLGDNEW
combine = combine.with_columns([
    pl.lit(None).cast(pl.Float64).alias("SUBLGD"),
    pl.lit("").alias("PDSUMNEW"),
    pl.lit("").alias("SUBLGDNEW"),
])
rows_list2 = combine.to_dicts()
for row in rows_list2:
    sg = row.get("SUBGIND")
    if sg in subgind_sub_nos:
        amtgsum = row.get("AMTGSUM") or 0.0
        amtesum = row.get("AMTESUM") or 0.0
        sublgd  = round(100 - (amtesum / amtgsum * 100), 2) if amtgsum else 100.0
        row["SUBLGD"]    = sublgd
        row["PDSUMNEW"]  = f"{(row.get('PDSUM') or 0.0):,.2f}"
        row["SUBLGDNEW"] = f"{sublgd:,.2f}"
    if row.get("GROUPIND") == "TOTAL":
        bal = row.get("BALANCE") or 0.0
        pd_val  = round((totobdefault / bal * 100), 2) if bal else 0.0
        lgd_val = round(100 - (totexpectedrec / totobdefault * 100), 2) if totobdefault else 100.0
        row["PD"]     = pd_val
        row["LGD"]    = lgd_val
        row["PDNEW"]  = f"{pd_val:,.2f}"
        row["LGDNEW"] = f"{lgd_val:,.2f}"

combine = pl.DataFrame(rows_list2)
combine = combine.sort("NO")

# ─────────────────────────────────────────────
# Helper: fixed-width field placement
# ─────────────────────────────────────────────
def place(line: list, start_col: int, text: str) -> None:
    """Place text into a fixed-width line buffer (1-indexed, SAS @col style)."""
    if text is None:
        return
    idx = start_col - 1
    for i, ch in enumerate(str(text)):
        pos = idx + i
        if pos < len(line):
            line[pos] = ch
        else:
            line.extend([" "] * (pos - len(line) + 1))
            line[pos] = ch


def make_line(width: int = 237) -> list:
    return [" "] * width


def fmt_comma(val, width: int = 20) -> str:
    if val is None or val == "":
        return ""
    try:
        return f"{float(val):>{width},.2f}"
    except Exception:
        return str(val)


def line_str(line: list) -> str:
    return "".join(line).rstrip()


# ─────────────────────────────────────────────
# WRITE OUTPUT REPORT (OUTPUT1)
# ASA carriage control: first character of each record
# '1' = new page, ' ' = single space (next line), '0' = double space,
# '+' = overprint
# ─────────────────────────────────────────────
PAGE_LEN = 60
LINE_WIDTH = 238  # 237 data cols + 1 ASA col

def asa_line(cc: str, content: str) -> str:
    """Prepend ASA carriage control character."""
    return cc + content

rows_final = combine.to_dicts()

output_lines: list[str] = []
first_row = True
line_count = 0


def emit(cc: str, content: str = ""):
    global line_count
    output_lines.append(asa_line(cc, content))
    line_count += 1


def emit_blank(n: int = 1):
    for _ in range(n):
        emit(" ", "")


def emit_dash(char: str = "-", count: int = 237):
    emit(" ", char * count)


for row in rows_final:
    if first_row:
        # Header
        emit("1", "RECOVERY RATE OF HIRE PURCHASE LOANS - PBB")
        emit(" ", "")

        hdr1 = make_line(237)
        place(hdr1,   1, "CATEGORY")
        place(hdr1,  16, "MONTHS IN")
        place(hdr1,  34, "OUTSTANDING BALANCE")
        place(hdr1,  59, "AVERAGE")
        place(hdr1,  74, "LOAN STATUS")
        place(hdr1, 104, "OUTSTANDING BALANCE")
        place(hdr1, 139, "RECOVERY")
        place(hdr1, 149, "EXPECTED RECOVERY")
        place(hdr1, 169, "TIMING OF")
        place(hdr1, 179, "DISCOUNT")
        place(hdr1, 189, "DISCOUNTED")
        place(hdr1, 214, "PD")
        place(hdr1, 225, "LOSS GIVEN")
        emit(" ", line_str(hdr1))

        hdr2 = make_line(237)
        place(hdr2,  16, "ARREARS")
        place(hdr2,  34, "AT CURRENT DATE")
        place(hdr2,  59, "DEFAULT RATE")
        place(hdr2, 104, "AT CURRENT DATE X DEFAULT RATE")
        place(hdr2, 139, "RATE")
        place(hdr2, 149, "AMOUNT")
        place(hdr2, 169, "RECOVERY")
        place(hdr2, 179, "RATE")
        place(hdr2, 189, "EXPECTED")
        place(hdr2, 214, "%")
        place(hdr2, 225, "DEFAULT %")
        emit(" ", line_str(hdr2))

        hdr3 = make_line(237)
        place(hdr3,  16, "AT CURRENT DATE")
        place(hdr3,  34, "(A)")
        place(hdr3,  59, "(B)")
        place(hdr3, 104, "(C)=(A)X(B)")
        place(hdr3, 139, "(D)")
        place(hdr3, 149, "(E)=(C)X(D)")
        place(hdr3, 169, "(F)")
        place(hdr3, 179, "(G)")
        place(hdr3, 189, "RECOVERY(PRESENT VALUE)")
        emit(" ", line_str(hdr3))

        hdr4 = make_line(237)
        place(hdr4,  34, "   (RM)")
        place(hdr4, 104, "   (RM)")
        place(hdr4, 149, "   (RM)")
        place(hdr4, 189, "   (RM)")
        emit(" ", line_str(hdr4))

        emit_dash("-", 237)
        first_row = False

    gi   = row.get("GROUPIND") or ""
    sg   = row.get("SUBGIND") or ""
    cat  = row.get("CATEGORY") or ""
    no   = row.get("NO")
    grp  = row.get("GROUP") or " "
    subg = row.get("SUBGROUP") or " "
    bal  = row.get("BAL") or ""
    r1   = row.get("RATE1") or ""
    rr1  = row.get("RECRATE1") or ""
    ob   = fmt_comma(row.get("OBDEFAULT"), 20)
    er   = fmt_comma(row.get("EXPECTEDREC"), 20)
    bal_fmt = fmt_comma(row.get("BALANCE"), 20)
    pdnew    = row.get("PDNEW") or ""
    lgdnew   = row.get("LGDNEW") or ""
    pdsumnew = row.get("PDSUMNEW") or ""
    slgdnew  = row.get("SUBLGDNEW") or ""

    def make_data_line() -> str:
        ln = make_line(237)
        place(ln,   1, grp)
        place(ln,  16, subg)
        place(ln,  34, bal)
        place(ln,  59, r1)
        place(ln,  74, cat)
        place(ln, 104, ob)
        place(ln, 139, rr1)
        place(ln, 149, er)
        place(ln, 214, pdnew)
        place(ln, 225, lgdnew)
        return line_str(ln)

    # OTHERS / CURRENT
    if gi == "OTHERS" and sg == "CURRENT" and no != 17:
        emit(" ", make_data_line())
        if no == 16:
            ln = make_line(237)
            place(ln, 74, "-" * 164)
            emit(" ", line_str(ln))

    if sg == "CURRENT" and no == 17:
        ln = make_line(237)
        place(ln, 74, cat)
        place(ln, 214, pdsumnew)
        place(ln, 225, slgdnew)
        emit(" ", line_str(ln))
        ln2 = make_line(237)
        place(ln2, 74, "-" * 164)
        emit(" ", line_str(ln2))
        emit_blank(1)

    # OTHERS / 1-2 MTHS
    if gi == "OTHERS" and sg == "1-2 MTHS" and no != 27:
        emit(" ", make_data_line())
        if no == 26:
            ln = make_line(237)
            place(ln, 74, "-" * 164)
            emit(" ", line_str(ln))

    if sg == "1-2 MTHS" and no == 27:
        ln = make_line(237)
        place(ln, 74, cat)
        place(ln, 214, pdsumnew)
        place(ln, 225, slgdnew)
        emit(" ", line_str(ln))
        ln2 = make_line(237)
        place(ln2, 74, "-" * 164)
        emit(" ", line_str(ln2))
        emit_blank(1)

    # OTHERS / 3-5 MTHS
    if gi == "OTHERS" and sg == "3-5 MTHS" and no != 37:
        emit(" ", make_data_line())
        if no == 36:
            ln = make_line(237)
            place(ln, 74, "-" * 164)
            emit(" ", line_str(ln))

    if sg == "3-5 MTHS" and no == 37:
        ln = make_line(237)
        place(ln, 74, cat)
        place(ln, 214, pdsumnew)
        place(ln, 225, slgdnew)
        emit(" ", line_str(ln))
        ln2 = make_line(237)
        place(ln2, 74, "-" * 164)
        emit(" ", line_str(ln2))
        emit_blank(1)

    # OTHERS / 6-11 MTHS
    if gi == "OTHERS" and sg == "6-11 MTHS" and no != 47:
        emit(" ", make_data_line())
        if no == 46:
            ln = make_line(237)
            place(ln, 74, "-" * 164)
            emit(" ", line_str(ln))

    if sg == "6-11 MTHS" and no == 47:
        ln = make_line(237)
        place(ln, 74, cat)
        place(ln, 214, pdsumnew)
        place(ln, 225, slgdnew)
        emit(" ", line_str(ln))
        ln2 = make_line(237)
        place(ln2, 74, "-" * 164)
        emit(" ", line_str(ln2))
        emit_blank(1)

    # OTHERS / >=12 MTHS
    if gi == "OTHERS" and sg == ">=12 MTHS":
        emit(" ", make_data_line())
        if no == 51:
            emit_blank(1)
            emit_dash("-", 237)

    # IRREGULAR
    if gi == "IRREGULAR":
        ln = make_line(237)
        place(ln,   1, "IRREGULAR")
        place(ln,  16, subg)
        place(ln,  34, bal_fmt)
        place(ln,  59, r1)
        place(ln,  74, cat)
        place(ln, 104, ob)
        place(ln, 139, rr1)
        place(ln, 149, er)
        place(ln, 214, pdnew)
        place(ln, 225, lgdnew)
        emit(" ", line_str(ln))
        if no == 61:
            emit_blank(1)
            emit_dash("-", 237)

    # REPOSSESSED <12 MTHS
    if gi == "REPOSSESSED" and sg == "REPOSSESSED <12 MTHS":
        ln = make_line(237)
        place(ln,   1, "REPOSSESSED")
        place(ln,  16, subg)
        place(ln,  34, bal_fmt)
        place(ln,  59, r1)
        place(ln,  74, cat)
        place(ln, 104, ob)
        place(ln, 139, rr1)
        place(ln, 149, er)
        place(ln, 214, pdnew)
        place(ln, 225, lgdnew)
        emit(" ", line_str(ln))
        if no == 71:
            emit_blank(1)

    # REPOSSESSED >=12 MTHS
    if gi == "REPOSSESSED" and sg == "REPOSSESSED >=12 MTHS":
        ln = make_line(237)
        place(ln,  16, subg)
        place(ln,  34, bal_fmt)
        place(ln,  59, r1)
        place(ln,  74, cat)
        place(ln, 104, ob)
        place(ln, 139, rr1)
        place(ln, 149, er)
        place(ln, 214, pdnew)
        place(ln, 225, lgdnew)
        emit(" ", line_str(ln))
        if no == 72:
            emit_blank(1)
            emit_dash("-", 237)

    # DEFICIT
    if gi == "DEFICIT":
        ln = make_line(237)
        place(ln,   1, "DEFICIT")
        place(ln,  16, subg)
        place(ln,  34, bal_fmt)
        place(ln,  59, r1)
        place(ln,  74, cat)
        place(ln, 104, ob)
        place(ln, 139, rr1)
        place(ln, 149, er)
        place(ln, 214, pdnew)
        place(ln, 225, lgdnew)
        emit(" ", line_str(ln))
        if no == 81:
            emit_blank(1)
            emit_dash("=", 237)

    # TOTAL
    if gi == "TOTAL":
        ln = make_line(237)
        place(ln,   1, "TOTAL")
        place(ln,  34, bal_fmt)
        place(ln, 104, bal_fmt)
        place(ln, 149, er)
        place(ln, 214, pdnew)
        place(ln, 225, lgdnew)
        emit(" ", line_str(ln))
        if no == 91:
            emit_blank(1)
            emit_dash("-", 237)

# Write output file
with open(OUTPUT1_PATH, "w", encoding="utf-8") as fout:
    for ln in output_lines:
        fout.write(ln + "\n")

# ─────────────────────────────────────────────
# GET CAP – build NPL.CAP1 parquet
# ─────────────────────────────────────────────
cap_cats   = {"SUCCESSFUL REPOSSESSION", "N/A",
              "- >6-11 MONTHS IN ARREARS", "- OTHERS", "- 3-5 MONTHS IN ARREARS"}
pdlgd_nos  = {17, 27, 37, 47}
pdlgd2_nos = {51, 61, 71, 72, 81}

# Resolve SUBGIND blanks → GROUPIND  (SAS: IF SUBGIND=' ' THEN SUBGIND=GROUPIND)
combine_cap = combine.with_columns(
    pl.when(
        pl.col("SUBGIND").is_null() | (pl.col("SUBGIND") == " ") | (pl.col("SUBGIND") == "")
    ).then(pl.col("GROUPIND"))
    .otherwise(pl.col("SUBGIND"))
    .alias("SUBGIND")
)

cap_df = combine_cap.filter(pl.col("CATEGORY").is_in(cap_cats))

pdlgd_df = (
    combine_cap
    .filter(pl.col("NO").is_in(pdlgd_nos))
    .select(["SUBGIND", "PDSUM", "SUBLGD"])
    .rename({"SUBGIND": "CATEGORY"})
)

pdlgd2_df = (
    combine_cap
    .filter(pl.col("NO").is_in(pdlgd2_nos))
    .select(["SUBGIND", "PD", "LGD"])
    .rename({"SUBGIND": "CATEGORY", "PD": "PDSUM", "LGD": "SUBLGD"})
)

# PROC SUMMARY CAP by SUBGIND → CAPROVISION sum → NPL.CAP1
cap1 = (
    cap_df
    .group_by("SUBGIND")
    .agg(pl.sum("CAPROVISION"))
    .rename({"SUBGIND": "CATEGORY"})
)

# Merge PDLGD and PDLGD2
pdlgd_all = pl.concat([pdlgd_df, pdlgd2_df], how="diagonal").sort("CATEGORY")
cap1 = cap1.join(pdlgd_all, on="CATEGORY", how="left")

# Rename PD/LGD columns: PDSUM→PD, SUBLGD→LGD
cap1 = cap1.rename({"PDSUM": "PD", "SUBLGD": "LGD"})

cap1.write_parquet(CAP1_PATH)

print(f"Report written : {OUTPUT1_PATH}")
print(f"CAP1 parquet   : {CAP1_PATH}")
