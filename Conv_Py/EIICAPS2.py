# !/usr/bin/env python3
"""
Program: EIICAPS2.py
Purpose: GENERATE CAP REPORT - PIBB(STAFF) MOVEMENT OF CAP BY BRANCH
"""

import duckdb
import polars as pl
import os
from datetime import date, timedelta

from PBBELF import format_brchcd

# ─────────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────────
BASE_DIR    = r"C:/data"
LOAN_DIR    = os.path.join(BASE_DIR, "loan")
LN_DIR      = os.path.join(BASE_DIR, "ln")
HP_DIR      = os.path.join(BASE_DIR, "hp")
CCRIS_DIR   = os.path.join(BASE_DIR, "ccris")
NPL_DIR     = os.path.join(BASE_DIR, "npl")
OUTPUT_DIR  = os.path.join(BASE_DIR, "output")

os.makedirs(NPL_DIR,    exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

OUTPUT1_PATH = os.path.join(OUTPUT_DIR, "EIICAPS2.txt")

con = duckdb.connect()

# ─────────────────────────────────────────────
# REPTDATE – derive all macro variables
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
REPTYEAR = str(reptdate_val.year)[-2:]
REPTDAY  = f"{reptdate_val.day:02d}"

# PREVMON = last day of previous month
prevmon_val: date = date(reptdate_val.year, reptdate_val.month, 1) - timedelta(days=1)
REPTMON1  = f"{prevmon_val.month:02d}"
REPTYEAR1 = str(prevmon_val.year)[-2:]

# YEAREND = Dec 31 of previous year
yearend_val: date = date(reptdate_val.year, 1, 1) - timedelta(days=1)
LMON  = f"{yearend_val.month:02d}"
LYEAR = str(yearend_val.year)[-2:]

DATE = f"{REPTDAY}/{REPTMON}/{REPTYEAR}"

# ─────────────────────────────────────────────
# CREDSUB
# ─────────────────────────────────────────────
ccris_file = os.path.join(CCRIS_DIR, f"ICREDMSUBAC{REPTMON}{REPTYEAR}.parquet")
credsub_df = con.execute(f"""
    SELECT ACCTNUM AS ACCTNO, DAYSARR AS DAYARR, NOTENO
    FROM '{ccris_file}'
    WHERE FACILITY IN ('34331','34332','34371')
""").pl()

credsub_df = (
    credsub_df
    .sort(["ACCTNO", "NOTENO", "DAYARR"], descending=[False, False, True])
    .unique(subset=["ACCTNO", "NOTENO"], keep="first")
)

# ─────────────────────────────────────────────
# HP master – staff loan products
# ─────────────────────────────────────────────
ln_file = os.path.join(LN_DIR, f"ILN{REPTMON}{NOWK}{REPTYEAR}.parquet")
hp_raw  = con.execute(f"""
    SELECT * FROM '{ln_file}'
    WHERE BALANCE > 0
      AND PRODUCT IN (103,104,107,108)
      AND (DAYARR >= 90
           OR BORSTAT IN ('F','I','R')
           OR USER5 = 'N')
""").pl()

# Apply COSTCTR → BRANCH overrides and compute BRANCHABBR
def apply_branch_overrides(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns([
        pl.when(pl.col("COSTCTR") == 4048).then(pl.lit(903))
          .when(pl.col("COSTCTR") == 4043).then(pl.lit(906))
          .otherwise(pl.col("BRANCH")).cast(pl.Int64).alias("BRANCH")
    ])
    branch_abbr = [format_brchcd(int(b)) for b in df["BRANCH"].to_list()]
    df = df.with_columns([
        pl.Series("BRANCHABBR", branch_abbr),
        pl.lit("PIBB").alias("IND"),
    ])
    return df

hp_raw = apply_branch_overrides(hp_raw)

# Merge HP ← CREDSUB
hp_df = hp_raw.join(credsub_df, on=["ACCTNO", "NOTENO"], how="left")

# ─────────────────────────────────────────────
# PIBB – assign CATEGORY
# ─────────────────────────────────────────────
BORSTAT_EXCL = ["F", "I", "R", "E", "W", "Z"]

def assign_category(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        pl.when(
            (pl.col("DAYARR") <= 30) &
            (~pl.col("BORSTAT").is_in(BORSTAT_EXCL)) &
            (~pl.col("USER5").is_in(["N"])) &
            (pl.col("PAIDIND") == "M")
        ).then(pl.lit("CURRENT"))
        .when(
            pl.col("DAYARR").is_between(31, 89) &
            (~pl.col("BORSTAT").is_in(BORSTAT_EXCL)) &
            (~pl.col("USER5").is_in(["N"])) &
            (pl.col("PAIDIND") == "M")
        ).then(pl.lit("1-2 MTHS"))
        .when(
            (~pl.col("BORSTAT").is_in(BORSTAT_EXCL)) &
            ((pl.col("USER5").is_in(["N"]) & (pl.col("DAYARR") <= 182)) |
             pl.col("DAYARR").is_between(90, 182)) &
            (pl.col("PAIDIND") == "M")
        ).then(pl.lit("3-5 MTHS"))
        .when(
            (~pl.col("BORSTAT").is_in(BORSTAT_EXCL)) &
            ((pl.col("USER5").is_in(["N"]) & pl.col("DAYARR").is_between(183, 364)) |
             pl.col("DAYARR").is_between(183, 364)) &
            (pl.col("PAIDIND") == "M")
        ).then(pl.lit("6-11 MTHS"))
        .when(
            (~pl.col("BORSTAT").is_in(BORSTAT_EXCL)) &
            ((pl.col("USER5").is_in(["N"]) & (pl.col("DAYARR") >= 365)) |
             (pl.col("DAYARR") >= 365)) &
            (pl.col("PAIDIND") == "M")
        ).then(pl.lit(">=12 MTHS"))
        .when(
            (pl.col("BORSTAT") == "I") & (pl.col("PAIDIND") == "M")
        ).then(pl.lit("IRREGULAR"))
        .when(
            (pl.col("BORSTAT") == "R") & (pl.col("PAIDIND") == "M") &
            (pl.col("DAYARR") < 365)
        ).then(pl.lit("REPOSSESSED <12 MTHS"))
        .when(
            (pl.col("BORSTAT") == "R") & (pl.col("PAIDIND") == "M") &
            (pl.col("DAYARR") >= 365)
        ).then(pl.lit("REPOSSESSED >=12 MTHS"))
        .when(
            (pl.col("BORSTAT") == "F") & (pl.col("PAIDIND") == "M")
        ).then(pl.lit("DEFICIT"))
        .otherwise(pl.lit(" "))
        .alias("CATEGORY")
    )
    return df.filter(pl.col("CATEGORY") != " ")

hp_df = assign_category(hp_df)

pibb_df = hp_df.select([
    "ACCTNO", "NOTENO", "BRANCH", "BRANCHABBR", "PRODUCT",
    "CATEGORY", "IND", "BALANCE", "PAIDIND", "MARKETVL",
    pl.col("VINNO").str.lstrip().str.slice(0, 13).alias("AANO"),
])

# ─────────────────────────────────────────────
# PIBB – ISUMBAL_STAFF
# ─────────────────────────────────────────────
isumbal_staff_df = pibb_df.group_by("CATEGORY").agg(
    pl.col("BALANCE").sum()
)
isumbal_staff_path = os.path.join(NPL_DIR, "ISUMBAL_STAFF.parquet")
isumbal_staff_df.write_parquet(isumbal_staff_path)

# ─────────────────────────────────────────────
# Merge ICAP1_STAFF + ISUMBAL_STAFF → ICOUNTCAP
# ─────────────────────────────────────────────
icap1_staff_path = os.path.join(NPL_DIR, "ICAP1_STAFF.parquet")
icap1_staff_df   = con.execute(f"SELECT * FROM '{icap1_staff_path}'").pl()
isumbal_s_df     = con.execute(f"SELECT * FROM '{isumbal_staff_path}'").pl()

icountcap_df = icap1_staff_df.join(isumbal_s_df, on="CATEGORY", how="outer",
                                   suffix="_SB")

icountcap_df = icountcap_df.with_columns([
    pl.when(pl.col("BALANCE").is_null() | (pl.col("BALANCE") == 0))
      .then(pl.lit(0.0))
      .otherwise((pl.col("CAPROVISION") / pl.col("BALANCE")) * 100)
      .alias("CARATE")
])
carate_100_cats = [">=12 MTHS", "IRREGULAR", "DEFICIT", "REPOSSESSED >=12 MTHS"]
icountcap_df = icountcap_df.with_columns(
    pl.when(pl.col("CATEGORY").is_in(carate_100_cats))
      .then(pl.lit(100.0))
      .otherwise(pl.col("CARATE"))
      .alias("CARATE")
)
icountcap_df = icountcap_df.select(["CATEGORY", "CARATE", "PD", "LGD"])

# ─────────────────────────────────────────────
# NPL.ICAP_STAFF{REPTMON}{REPTYEAR}
# ─────────────────────────────────────────────
icap_staff_cur_path = os.path.join(NPL_DIR, f"ICAP_STAFF{REPTMON}{REPTYEAR}.parquet")

icap_staff_cur_df = pibb_df.join(icountcap_df, on="CATEGORY", how="left")
icap_staff_cur_df = icap_staff_cur_df.with_columns(
    ((pl.col("BALANCE") * pl.col("CARATE")) / 100).alias("CAP")
)
icap_staff_cur_df.write_parquet(icap_staff_cur_path)

# ─────────────────────────────────────────────
# OPENING BALANCE from last year-end period
# ─────────────────────────────────────────────
icap_staff_prev_path = os.path.join(NPL_DIR, f"ICAP_STAFF{LMON}{LYEAR}.parquet")
icap_prev_df = con.execute(f"SELECT * FROM '{icap_staff_prev_path}'").pl()

if "CATEGORY1" in icap_prev_df.columns:
    icap_prev_df = icap_prev_df.drop("CATEGORY1")

icap_prev_df = icap_prev_df.filter(
    pl.col("CATEGORY").is_not_null() & (pl.col("CATEGORY") != " ")
)
icap_prev_df = icap_prev_df.with_columns(
    pl.when(pl.col("CATEGORY") == ">=6 MTHS")
      .then(pl.lit("6-11 MTHS"))
      .otherwise(pl.col("CATEGORY"))
      .alias("CATEGORY")
)
# *IF IND='PIBB' THEN OUTPUT IOPENBAL;

iopenbal_df = icap_prev_df.select([
    "ACCTNO", "NOTENO", "CAP", "CATEGORY", "PRODUCT", "BRANCH", "BRANCHABBR"
]).rename({"CAP": "OPEN_BALANCE", "CATEGORY": "CATEGORY1"})

# ─────────────────────────────────────────────
# INDVIHP – merge opening with current
# ─────────────────────────────────────────────
icap_staff_cur_df = con.execute(f"SELECT * FROM '{icap_staff_cur_path}'").pl()

indvihp_df = iopenbal_df.join(
    icap_staff_cur_df, on=["ACCTNO", "NOTENO"], how="outer", suffix="_CUR"
)

indvihp_df = indvihp_df.with_columns(
    pl.when(
        pl.col("OPEN_BALANCE").is_not_null() & pl.col("CAP").is_null()
    ).then(pl.lit("P"))
    .when(
        pl.col("CAP").is_not_null() & pl.col("OPEN_BALANCE").is_null()
    ).then(pl.lit("C"))
    .otherwise(pl.lit(" "))
    .alias("STATUS")
)

indvihp_df = indvihp_df.with_columns(
    pl.when(pl.col("STATUS") == "P")
      .then(pl.col("CATEGORY1"))
      .otherwise(pl.col("CATEGORY"))
      .alias("CATEGORY")
)

indvihp_df = indvihp_df.with_columns(
    pl.when(pl.col("PRODUCT").is_in([983, 993, 678, 679, 698, 699]))
      .then(pl.lit(0.0))
      .otherwise(pl.col("CAP"))
      .alias("CAP")
)

# ─────────────────────────────────────────────
# SUSPEND / WRBACK / NET / WRIOFF_BAL
# ─────────────────────────────────────────────
def compute_suspend_wrback(df: pl.DataFrame) -> pl.DataFrame:
    rows = []
    for row in df.iter_rows(named=True):
        row         = dict(row)
        cap          = row.get("CAP") or 0.0
        open_balance = row.get("OPEN_BALANCE") or 0.0
        status       = row.get("STATUS") or " "

        charcap = cap - open_balance
        suspend = None
        wrback  = None

        if status == "P":
            if cap < open_balance:
                suspend = 0.0
                wrback  = cap - open_balance
        elif status == "C":
            suspend = cap
            wrback  = cap - suspend   # always 0
        else:
            if charcap < 0:
                wrback  = charcap
            else:
                suspend = charcap

        suspend = suspend if suspend is not None else 0.0
        wrback  = wrback  if wrback  is not None else 0.0
        wrback  = wrback * -1
        net     = suspend - wrback

        row["CAP"]          = cap
        row["OPEN_BALANCE"] = open_balance
        row["CHARCAP"]      = charcap
        row["SUSPEND"]      = suspend
        row["WRBACK"]       = wrback
        row["NET"]          = net
        row["WRIOFF_BAL"]   = 0.0   # TO BE HARDCODED
        rows.append(row)
    return pl.DataFrame(rows, schema=df.schema | {
        "CHARCAP": pl.Float64, "SUSPEND": pl.Float64,
        "WRBACK": pl.Float64,  "NET": pl.Float64, "WRIOFF_BAL": pl.Float64,
    })

indvihp_df = compute_suspend_wrback(indvihp_df)

branch1_list = []
for row in indvihp_df.iter_rows(named=True):
    abbr   = row.get("BRANCHABBR") or ""
    branch = row.get("BRANCH") or 0
    branch1_list.append(f"{abbr} {int(branch):03d}")
indvihp_df = indvihp_df.with_columns(pl.Series("BRANCH1", branch1_list))
indvihp_df.write_parquet(icap_staff_cur_path)

# ─────────────────────────────────────────────
# Merge with NPL.WMIS write-off data
# ─────────────────────────────────────────────
wmis_path = os.path.join(NPL_DIR, "WMIS.parquet")

if os.path.exists(wmis_path):
    wmis_df = con.execute(f"SELECT * FROM '{wmis_path}'").pl()
    wmis_df = wmis_df.with_columns(
        pl.when(pl.col("CATEGORY") == ">=6 MTHS")
          .then(pl.lit("6-11 MTHS"))
          .otherwise(pl.col("CATEGORY"))
          .alias("CATEGORY")
    )

    icap_staff_cur_df = con.execute(f"SELECT * FROM '{icap_staff_cur_path}'").pl()
    icap_staff_cur_df = icap_staff_cur_df.join(
        wmis_df.select(["ACCTNO"]).unique().with_columns(pl.lit("Y").alias("_WO_FLAG")),
        on="ACCTNO", how="left"
    )
    icap_staff_cur_df = icap_staff_cur_df.with_columns(
        pl.when(pl.col("_WO_FLAG") == "Y").then(pl.lit("Y"))
          .otherwise(pl.lit("N")).alias("WRITEOFF")
    ).drop("_WO_FLAG")
    icap_staff_cur_df.write_parquet(icap_staff_cur_path)

    # IF STATUS='P' AND WRITEOFF='Y' → recompute SUSPEND/WRBACK
    updated_rows = []
    for row in icap_staff_cur_df.iter_rows(named=True):
        row = dict(row)
        if row.get("STATUS") == "P" and row.get("WRITEOFF") == "Y":
            wrioff_bal   = row.get("WRIOFF_BAL") or 0.0
            open_balance = row.get("OPEN_BALANCE") or 0.0
            suspend = wrioff_bal - open_balance
            if suspend < 0:
                wrback  = open_balance - wrioff_bal
                suspend = 0.0
            else:
                wrback = 0.0
            row["SUSPEND"] = suspend
            row["WRBACK"]  = wrback
        row["NET"] = (row.get("SUSPEND") or 0.0) - (row.get("WRBACK") or 0.0)
        updated_rows.append(row)
    icap_staff_cur_df = pl.DataFrame(updated_rows)
    icap_staff_cur_df.write_parquet(icap_staff_cur_path)

# ─────────────────────────────────────────────
# WOF (written-off HP accounts)
# ─────────────────────────────────────────────
wof_file = os.path.join(HP_DIR, f"IHPWO{REPTMON}{NOWK}{REPTYEAR}.parquet")
if os.path.exists(wof_file):
    wof_df = con.execute(f"""
        SELECT ACCTNO, NOTENO, PRODUCT FROM '{wof_file}'
    """).pl()

    icap_staff_cur_df = con.execute(f"SELECT * FROM '{icap_staff_cur_path}'").pl()
    icap_staff_cur_df = icap_staff_cur_df.join(
        wof_df, on=["ACCTNO", "NOTENO"], how="left", suffix="_WOF"
    )
    if "PRODUCT_WOF" in icap_staff_cur_df.columns:
        icap_staff_cur_df = icap_staff_cur_df.drop("PRODUCT_WOF")
    icap_staff_cur_df.write_parquet(icap_staff_cur_path)

# ─────────────────────────────────────────────
# GENERATE SUMMARY REPORT – PROC TABULATE equivalent
# ─────────────────────────────────────────────
TBL2  = "PIBB(STAFF) MOVEMENT OF CAP BY BRANCH AS AT"
title = f"{TBL2} {DATE}"

icap_staff_cur_df = con.execute(f"SELECT * FROM '{icap_staff_cur_path}'").pl()

for col in ["BALANCE", "OPEN_BALANCE", "SUSPEND", "WRBACK", "WRIOFF_BAL", "CAP", "NET"]:
    if col in icap_staff_cur_df.columns:
        icap_staff_cur_df = icap_staff_cur_df.with_columns(pl.col(col).fill_null(0.0))

branch_summary = (
    icap_staff_cur_df
    .group_by("BRANCH1")
    .agg([
        pl.len().alias("N"),
        pl.col("BALANCE").sum(),
        pl.col("OPEN_BALANCE").sum(),
        pl.col("SUSPEND").sum(),
        pl.col("WRBACK").sum(),
        pl.col("WRIOFF_BAL").sum(),
        pl.col("CAP").sum(),
        pl.col("NET").sum(),
    ])
    .sort("BRANCH1")
)

total_row = pl.DataFrame({
    "BRANCH1":      ["TOTAL"],
    "N":            [icap_staff_cur_df.height],
    "BALANCE":      [icap_staff_cur_df["BALANCE"].sum()],
    "OPEN_BALANCE": [icap_staff_cur_df["OPEN_BALANCE"].sum()],
    "SUSPEND":      [icap_staff_cur_df["SUSPEND"].sum()],
    "WRBACK":       [icap_staff_cur_df["WRBACK"].sum()],
    "WRIOFF_BAL":   [icap_staff_cur_df["WRIOFF_BAL"].sum()],
    "CAP":          [icap_staff_cur_df["CAP"].sum()],
    "NET":          [icap_staff_cur_df["NET"].sum()],
})
summary_df = pl.concat([branch_summary, total_row])

# Format helpers
def fc(v) -> str:
    if v is None:
        return " " * 20
    return f"{v:>20,.2f}"

def fi(v) -> str:
    if v is None:
        return " " * 7
    return f"{int(v):>7,}"

COL_SEP = "|"
RTS     = 15

HDR_COLS = [
    ("BALANCE",              20),
    ("OPENING BALANCE",      20),
    ("CHARGE FOR THE YEAR",  20),
    ("WRITTEN BACK TO P & L",20),
    ("WRITTEN-OFF",          20),
    ("CLOSING BALANCE",      20),
    ("NET INCREASE/DECREASE",20),
]
N_COL_W    = 7
LINE_SEP_CHAR = "-"

sep_line = "+" + LINE_SEP_CHAR * RTS + "+" + LINE_SEP_CHAR * N_COL_W + "".join(
    "+" + LINE_SEP_CHAR * w for _, w in HDR_COLS
) + "+"

def make_hdr_line(labels: list[tuple[str, int]], prefix_w: int) -> str:
    parts = [" " * prefix_w, COL_SEP, " " * N_COL_W]
    for label, w in labels:
        parts.append(COL_SEP)
        parts.append(f"{label:^{w}}"[:w])
    return "".join(parts)

def make_data_line(branch: str, n: int,
                   bal, open_bal, suspend, wrback, wrioff, cap, net) -> str:
    return "".join([
        f"{branch:<{RTS}}"[:RTS],
        COL_SEP, fi(n),
        COL_SEP, fc(bal),
        COL_SEP, fc(open_bal),
        COL_SEP, fc(suspend),
        COL_SEP, fc(wrback),
        COL_SEP, fc(wrioff),
        COL_SEP, fc(cap),
        COL_SEP, fc(net),
    ])

lines_out = []

def wr(line: str, asa: str = " "):
    lines_out.append(asa + line)

wr(title, "1")
wr(sep_line)
hdr1 = [("NO OF ACCOUNT", N_COL_W)] + HDR_COLS
wr(make_hdr_line(hdr1, RTS))
wr(sep_line)

for row in summary_df.iter_rows(named=True):
    wr(make_data_line(
        row["BRANCH1"], row["N"],
        row["BALANCE"], row["OPEN_BALANCE"], row["SUSPEND"],
        row["WRBACK"], row["WRIOFF_BAL"], row["CAP"], row["NET"]
    ))

wr(sep_line)

with open(OUTPUT1_PATH, "w", encoding="utf-8") as f:
    for line in lines_out:
        f.write(line + "\n")

print(f"Report written to               : {OUTPUT1_PATH}")
print(f"ICAP_STAFF{REPTMON}{REPTYEAR}   : {icap_staff_cur_path}")
