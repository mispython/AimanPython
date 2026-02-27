# !/usr/bin/env python3
"""
Program: EIBCAP42
Purpose: Generate PBB Movement of CAP by Branch report and build NPL.CAP parquet
"""

import duckdb
import polars as pl
import os
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

from PBBELF import format_brchcd, BRCHCD_MAP

# ─────────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────────
BASE_DIR   = r"C:\data"
LOAN_DIR   = os.path.join(BASE_DIR, "loan")
HP_DIR     = os.path.join(BASE_DIR, "hp")
LN_DIR     = os.path.join(BASE_DIR, "ln")
WOF_DIR    = os.path.join(BASE_DIR, "wof")
CCRIS_DIR  = os.path.join(BASE_DIR, "ccris")
NPL_DIR    = os.path.join(BASE_DIR, "npl")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
WMIS_PATH  = os.path.join(OUTPUT_DIR, "wmis.txt")   # flat WMIS infile

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(NPL_DIR,    exist_ok=True)

OUTPUT1_PATH = os.path.join(OUTPUT_DIR, "EIBCAP42.txt")

con = duckdb.connect()

# ─────────────────────────────────────────────
# REPTDATE – derive all macro variables
# ─────────────────────────────────────────────
reptdate_df = con.execute(
    f"SELECT REPTDATE FROM read_parquet('{LOAN_DIR}/reptdate.parquet') LIMIT 1"
).pl()

reptdate_val: date = reptdate_df["REPTDATE"][0]
day_val   = reptdate_val.day
month_val = reptdate_val.month
year_val  = reptdate_val.year

if   1  <= day_val <=  8:  wk = "1"
elif 9  <= day_val <= 15:  wk = "2"
elif 16 <= day_val <= 22:  wk = "3"
else:                       wk = "4"

# YEAREND = MDY(1,1,YEAR(REPTDATE))-1  → Dec 31 of previous year
yearend_val = date(year_val, 1, 1) - timedelta(days=1)

# PREVMON = MDY(MONTH(REPTDATE),1,YEAR(REPTDATE))-1  → last day of previous month
prevmon_val = date(year_val, month_val, 1) - timedelta(days=1)

REPTMON   = f"{month_val:02d}"
NOWK      = wk
REPTYEAR  = f"{year_val % 100:02d}"
REPTDAY   = f"{day_val:02d}"
REPTMON1  = f"{prevmon_val.month:02d}"
REPTYEAR1 = f"{prevmon_val.year % 100:02d}"
LMON      = f"{yearend_val.month:02d}"
LYEAR     = f"{yearend_val.year % 100:02d}"
DATE_STR  = f"{REPTDAY}/{REPTMON}/{REPTYEAR}"

# ─────────────────────────────────────────────
# CREDSUB
# ─────────────────────────────────────────────
credsub_parquet = os.path.join(CCRIS_DIR, f"credmsubac{REPTMON}{REPTYEAR}.parquet")
credsub_df = con.execute(f"""
    SELECT ACCTNUM, DAYSARR, NOTENO
    FROM read_parquet('{credsub_parquet}')
    WHERE FACILITY IN ('34331','34332')
""").pl().rename({"ACCTNUM": "ACCTNO", "DAYSARR": "DAYARR"})

credsub_df = (
    credsub_df
    .sort(["ACCTNO", "NOTENO", "DAYARR"], descending=[False, False, True])
    .unique(subset=["ACCTNO", "NOTENO"], keep="first")
)

# ─────────────────────────────────────────────
# HP dataset (HP library, products 700,705,720,725,380,381)
# ─────────────────────────────────────────────
hp_parquet = os.path.join(HP_DIR, f"hp{REPTMON}{NOWK}{REPTYEAR}.parquet")
hp_raw = con.execute(f"SELECT * FROM read_parquet('{hp_parquet}')").pl()

hp_raw = hp_raw.filter(pl.col("BALANCE") > 0)

# COSTCTR remapping
hp_raw = hp_raw.with_columns([
    pl.when(pl.col("COSTCTR") == 8044).then(pl.lit(902))
      .when(pl.col("COSTCTR") == 8048).then(pl.lit(903))
      .otherwise(pl.col("BRANCH"))
      .alias("BRANCH"),
])

# BRANCHABBR via PBBELF format_brchcd
hp_raw = hp_raw.with_columns(
    pl.col("BRANCH").map_elements(format_brchcd, return_dtype=pl.Utf8).alias("BRANCHABBR")
)

# IND assignment
hp_raw = hp_raw.with_columns(
    pl.when(pl.col("PRODUCT").is_in([700, 705, 720, 725, 380, 381]))
      .then(pl.lit("PBB"))
      .otherwise(pl.lit(" "))
      .alias("IND")
    # *IF PRODUCT IN (128,130) THEN IND='PIBB';
)

hp_df = hp_raw.filter(pl.col("IND") != " ")

# Merge CREDSUB
hp_df = hp_df.join(credsub_df, on=["ACCTNO", "NOTENO"], how="left")

# ─────────────────────────────────────────────
# PBB – categorise
# ─────────────────────────────────────────────
EXCL_BORSTAT = ["F", "I", "R", "E", "W", "Z"]

def assign_category(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns([
        pl.when(
            (pl.col("DAYARR") <= 30) &
            (~pl.col("BORSTAT").is_in(EXCL_BORSTAT)) &
            (pl.col("USER5") != "N") & (pl.col("PAIDIND") == "M")
        ).then(pl.lit("CURRENT"))
        .when(
            (pl.col("DAYARR") >= 31) & (pl.col("DAYARR") <= 89) &
            (~pl.col("BORSTAT").is_in(EXCL_BORSTAT)) &
            (pl.col("USER5") != "N") & (pl.col("PAIDIND") == "M")
        ).then(pl.lit("1-2 MTHS"))
        .when(
            (~pl.col("BORSTAT").is_in(EXCL_BORSTAT)) &
            (
                ((pl.col("USER5") == "N") & (pl.col("DAYARR") <= 182)) |
                ((pl.col("DAYARR") >= 90) & (pl.col("DAYARR") <= 182))
            ) & (pl.col("PAIDIND") == "M")
        ).then(pl.lit("3-5 MTHS"))
        .when(
            (~pl.col("BORSTAT").is_in(EXCL_BORSTAT)) &
            (
                ((pl.col("USER5") == "N") & (pl.col("DAYARR") >= 183) & (pl.col("DAYARR") <= 364)) |
                ((pl.col("DAYARR") >= 183) & (pl.col("DAYARR") <= 364))
            ) & (pl.col("PAIDIND") == "M")
        ).then(pl.lit("6-11 MTHS"))
        .when(
            (~pl.col("BORSTAT").is_in(EXCL_BORSTAT)) &
            (
                ((pl.col("USER5") == "N") & (pl.col("DAYARR") >= 365)) |
                (pl.col("DAYARR") >= 365)
            ) & (pl.col("PAIDIND") == "M")
        ).then(pl.lit(">=12 MTHS"))
        .when((pl.col("BORSTAT") == "I") & (pl.col("PAIDIND") == "M"))
          .then(pl.lit("IRREGULAR"))
        .when(
            (pl.col("BORSTAT") == "R") & (pl.col("PAIDIND") == "M") & (pl.col("DAYARR") < 365)
        ).then(pl.lit("REPOSSESSED <12 MTHS"))
        .when(
            (pl.col("BORSTAT") == "R") & (pl.col("PAIDIND") == "M") & (pl.col("DAYARR") >= 365)
        ).then(pl.lit("REPOSSESSED >=12 MTHS"))
        .when((pl.col("BORSTAT") == "F") & (pl.col("PAIDIND") == "M"))
          .then(pl.lit("DEFICIT"))
        .otherwise(pl.lit(" "))
        .alias("CATEGORY"),
    ])

hp_df = assign_category(hp_df)

pbb_df = hp_df.filter(pl.col("CATEGORY") != " ").with_columns([
    pl.col("VINNO").cast(pl.Utf8).str.strip_chars().str.slice(0, 13).alias("AANO"),
]).select([
    "ACCTNO", "NOTENO", "BRANCH", "BRANCHABBR", "PRODUCT",
    "CATEGORY", "IND", "BALANCE", "AANO", "PAIDIND", "MARKETVL",
])

# ─────────────────────────────────────────────
# PBB – SUMBAL by CATEGORY
# ─────────────────────────────────────────────
sumbal_df = (
    pbb_df.group_by("CATEGORY")
    .agg(pl.sum("BALANCE"))
)
sumbal_path = os.path.join(NPL_DIR, f"sumbal.parquet")
sumbal_df.write_parquet(sumbal_path)

# ─────────────────────────────────────────────
# COUNTCAP – merge CAP1 with SUMBAL
# Dependency: EIBCAP41.py → produces npl/cap1.parquet
# ─────────────────────────────────────────────
cap1_path = os.path.join(NPL_DIR, "cap1.parquet")
cap1_df   = con.execute(f"SELECT * FROM read_parquet('{cap1_path}')").pl()

countcap_df = (
    cap1_df.join(sumbal_df, on="CATEGORY", how="left", suffix="_bal")
    .with_columns([
        pl.when(pl.col("BALANCE") != 0)
          .then((pl.col("CAPROVISION") / pl.col("BALANCE")) * 100)
          .otherwise(pl.lit(0.0))
          .alias("CARATE"),
    ])
    .with_columns([
        pl.when(pl.col("CATEGORY").is_in([">=12 MTHS", "IRREGULAR", "DEFICIT", "REPOSSESSED >=12 MTHS"]))
          .then(pl.lit(100.0))
          .otherwise(pl.col("CARATE"))
          .alias("CARATE"),
    ])
    .select(["CATEGORY", "CARATE", "PD", "LGD"])
)

# ─────────────────────────────────────────────
# NPL.CAP{REPTMON}{REPTYEAR} – first build
# ─────────────────────────────────────────────
cap_path = os.path.join(NPL_DIR, f"cap{REPTMON}{REPTYEAR}.parquet")

cap_current_df = (
    pbb_df.join(countcap_df, on="CATEGORY", how="left")
    .with_columns([
        ((pl.col("BALANCE") * pl.col("CARATE")) / 100).alias("CAP"),
    ])
)
cap_current_df.write_parquet(cap_path)

# ─────────────────────────────────────────────
# GET OPENING BALANCE from year-start month
# ─────────────────────────────────────────────
cap_lmon_path = os.path.join(NPL_DIR, f"cap{LMON}{LYEAR}.parquet")

openbal_cols = ["ACCTNO", "NOTENO", "CAP", "CATEGORY", "PRODUCT", "BRANCH", "BRANCHABBR"]

if os.path.exists(cap_lmon_path):
    openbal_df = con.execute(f"SELECT * FROM read_parquet('{cap_lmon_path}')").pl()
    # DROP CATEGORY1 if it exists from a prior run
    if "CATEGORY1" in openbal_df.columns:
        openbal_df = openbal_df.drop("CATEGORY1")
    openbal_df = openbal_df.filter(
        pl.col("CATEGORY").is_not_null() & (pl.col("CATEGORY") != " ")
    )
    # IF CATEGORY='>=6 MTHS' THEN CATEGORY='6-11 MTHS'
    openbal_df = openbal_df.with_columns(
        pl.when(pl.col("CATEGORY") == ">=6 MTHS")
          .then(pl.lit("6-11 MTHS"))
          .otherwise(pl.col("CATEGORY"))
          .alias("CATEGORY")
    )
    openbal_df = openbal_df.select(openbal_cols).rename({
        "CAP": "OPEN_BALANCE", "CATEGORY": "CATEGORY1"
    })
    # *IF IND='PBB' THEN OUTPUT OPENBAL;
else:
    openbal_df = pl.DataFrame(schema={
        "ACCTNO": pl.Int64, "NOTENO": pl.Int64,
        "OPEN_BALANCE": pl.Float64, "CATEGORY1": pl.Utf8,
        "PRODUCT": pl.Int64, "BRANCH": pl.Int64, "BRANCHABBR": pl.Utf8,
    })

# ─────────────────────────────────────────────
# INDVHP merge
# ─────────────────────────────────────────────
cap_df_curr = con.execute(f"SELECT * FROM read_parquet('{cap_path}')").pl()

indvhp = cap_df_curr.join(openbal_df, on=["ACCTNO", "NOTENO"], how="outer_coalesce")

indvhp = indvhp.with_columns([
    pl.when(~pl.col("ACCTNO").is_in(cap_df_curr["ACCTNO"]) & pl.col("ACCTNO").is_in(openbal_df["ACCTNO"]))
      .then(pl.lit("P"))
      .when(pl.col("ACCTNO").is_in(cap_df_curr["ACCTNO"]) & ~pl.col("ACCTNO").is_in(openbal_df["ACCTNO"]))
      .then(pl.lit("C"))
      .otherwise(pl.lit(None).cast(pl.Utf8))
      .alias("STATUS"),
])

# Re-implement merge flags properly via indicator columns
left_keys  = set(zip(cap_df_curr["ACCTNO"].to_list(),  cap_df_curr["NOTENO"].to_list()))
right_keys = set(zip(openbal_df["ACCTNO"].to_list(),   openbal_df["NOTENO"].to_list())) if openbal_df.height else set()

indvhp_rows = []
for row in indvhp.iter_rows(named=True):
    key = (row["ACCTNO"], row["NOTENO"])
    in_a = key in left_keys
    in_b = key in right_keys
    if in_b and not in_a:
        row["STATUS"] = "P"
    elif in_a and not in_b:
        row["STATUS"] = "C"
    else:
        row["STATUS"] = None
    if row["STATUS"] == "P":
        row["CATEGORY"] = row.get("CATEGORY1")
    # IF PRODUCT IN (983,993,678,679,698,699) THEN CAP=0
    if row.get("PRODUCT") in (983, 993, 678, 679, 698, 699):
        row["CAP"] = 0.0
    indvhp_rows.append(row)

indvhp = pl.DataFrame(indvhp_rows)

# ─────────────────────────────────────────────
# Compute SUSPEND / WRBACK / NET / WRIOFF_BAL / BRANCH1
# ─────────────────────────────────────────────
rows_out = []
for row in indvhp.iter_rows(named=True):
    cap_val  = row.get("CAP") or 0.0
    open_bal = row.get("OPEN_BALANCE") or 0.0
    row["CAP"]          = cap_val
    row["OPEN_BALANCE"] = open_bal
    charcap = cap_val - open_bal
    row["CHARCAP"] = charcap

    suspend = None
    wrback  = None
    status  = row.get("STATUS")

    if status == "P":
        if cap_val < open_bal:
            suspend = 0.0
            wrback  = cap_val - open_bal
    elif status == "C":
        suspend = cap_val
        wrback  = cap_val - cap_val  # = 0
    else:
        if charcap < 0:
            wrback = charcap
        else:
            suspend = charcap

    row["SUSPEND"]    = suspend if suspend is not None else 0.0
    row["WRBACK"]     = wrback  if wrback  is not None else 0.0
    row["SUSPEND"]    = row["SUSPEND"] if row["SUSPEND"] is not None else 0.0
    row["WRBACK"]     = row["WRBACK"]  if row["WRBACK"]  is not None else 0.0
    row["WRBACK"]     = row["WRBACK"] * -1
    row["NET"]        = row["SUSPEND"] - row["WRBACK"]
    row["WRIOFF_BAL"] = 0.0

    branchabbr = row.get("BRANCHABBR") or ""
    branch_num = row.get("BRANCH") or 0
    row["BRANCH1"] = f"{branchabbr} {int(branch_num):03d}"
    rows_out.append(row)

cap_final = pl.DataFrame(rows_out)
cap_final.write_parquet(cap_path)

# ─────────────────────────────────────────────
# WRITTEN OFF FIGURE – %MACRO PROCESS
# Applies only for quarter-end months (03, 06, 09, 12)
# * WRITTEN OFF FIGURE TO BE PROVIDED BY HPCC FOR HARDCODING UNTIL YEAR END *
# ─────────────────────────────────────────────
wmis_npl_path = os.path.join(NPL_DIR, "wmis.parquet")

if REPTMON in ("03", "06", "09", "12"):
    # DATA WOFF – read flat WMIS file
    woff_rows = []
    if os.path.exists(WMIS_PATH):
        with open(WMIS_PATH, "r", encoding="utf-8") as f:
            for line in f:
                if len(line) < 240:
                    line = line.ljust(240)
                try:
                    acctno   = int(line[141:151].strip() or 0)
                    noteno   = int(line[151:156].strip() or 0)
                    # iiswoff = float(line[161:177].strip() or 0)
                    # spwoff  = float(line[177:193].strip() or 0)
                    ddwoff   = int(line[209:211].strip() or 0)
                    mmwoff   = int(line[212:214].strip() or 0)
                    yywoff   = int(line[215:219].strip() or 0)
                    capbal   = float(line[219:235].strip() or 0)
                    costctr  = int(line[235:239].strip() or 0)
                    wrioff_bal = capbal
                    woffdt   = f"{mmwoff:02d}/{yywoff:04d}"
                    # IF ((3000<=COSTCTR<=3999) OR COSTCTR IN (4043,4048)) THEN DELETE
                    if (3000 <= costctr <= 3999) or costctr in (4043, 4048):
                        continue
                    woff_rows.append({
                        "ACCTNO": acctno, "NOTENO": noteno,
                        "WRIOFF_BAL": wrioff_bal,
                    })
                except Exception:
                    continue

    woff_df = pl.DataFrame(woff_rows) if woff_rows else pl.DataFrame(
        schema={"ACCTNO": pl.Int64, "NOTENO": pl.Int64, "WRIOFF_BAL": pl.Float64}
    )

    # DATA HPWO – previous month HP file categories
    hp_prev_parquet = os.path.join(WOF_DIR, f"hp{REPTMON1}{NOWK}{REPTYEAR1}.parquet")
    if os.path.exists(hp_prev_parquet):
        hpwo_raw = con.execute(f"SELECT * FROM read_parquet('{hp_prev_parquet}')").pl()
        hpwo_raw = assign_category(hpwo_raw)
        hpwo_df  = hpwo_raw.select(["ACCTNO", "NOTENO", "CATEGORY"])
    else:
        hpwo_df = pl.DataFrame(schema={"ACCTNO": pl.Int64, "NOTENO": pl.Int64, "CATEGORY": pl.Utf8})

    # Merge WOFF + HPWO, keep WOFF rows
    woff_df = woff_df.join(hpwo_df, on=["ACCTNO", "NOTENO"], how="left")
    woff_df = woff_df.with_columns(pl.lit(reptdate_val).alias("REPTDATE"))

    # NPL.WMIS – remove current REPTDATE entries then append
    if os.path.exists(wmis_npl_path):
        wmis_existing = con.execute(f"SELECT * FROM read_parquet('{wmis_npl_path}')").pl()
        wmis_existing = wmis_existing.filter(pl.col("REPTDATE") != reptdate_val)
        wmis_updated  = pl.concat([wmis_existing, woff_df], how="diagonal")
    else:
        wmis_updated = woff_df

    wmis_updated.write_parquet(wmis_npl_path)

# ─────────────────────────────────────────────
# NPL.WMIS – fix CATEGORY and sort
# ─────────────────────────────────────────────
if os.path.exists(wmis_npl_path):
    wmis_df = con.execute(f"SELECT * FROM read_parquet('{wmis_npl_path}')").pl()
    # DATA NPL.WMIS(DROP=CATEGORY RENAME=(CATEGORY1=CATEGORY))
    wmis_df = wmis_df.with_columns(
        pl.when(pl.col("CATEGORY") == "REPOSSESSED >=12 MTH")
          .then(pl.lit("REPOSSESSED >=12 MTHS"))
          .otherwise(pl.col("CATEGORY"))
          .alias("CATEGORY1")
    ).drop("CATEGORY").rename({"CATEGORY1": "CATEGORY"})
    wmis_df.write_parquet(wmis_npl_path)

    woff_sorted = wmis_df.sort(["ACCTNO", "NOTENO"])
    # IF CATEGORY='>=6 MTHS' THEN CATEGORY='6-11 MTHS'
    woff_sorted = woff_sorted.with_columns(
        pl.when(pl.col("CATEGORY") == ">=6 MTHS")
          .then(pl.lit("6-11 MTHS"))
          .otherwise(pl.col("CATEGORY"))
          .alias("CATEGORY")
    )
else:
    woff_sorted = pl.DataFrame(schema={
        "ACCTNO": pl.Int64, "NOTENO": pl.Int64,
        "WRIOFF_BAL": pl.Float64, "CATEGORY": pl.Utf8,
    })

# Merge CAP with WOFF writeoff flags
cap_df2 = con.execute(f"SELECT * FROM read_parquet('{cap_path}')").pl().sort(["ACCTNO", "NOTENO"])

woff_keys = set(zip(woff_sorted["ACCTNO"].to_list(), woff_sorted["NOTENO"].to_list())) \
    if woff_sorted.height else set()
woff_map  = {(r["ACCTNO"], r["NOTENO"]): r["WRIOFF_BAL"]
             for r in woff_sorted.iter_rows(named=True)} if woff_sorted.height else {}

cap_rows = []
for row in cap_df2.iter_rows(named=True):
    key = (row["ACCTNO"], row["NOTENO"])
    row["WRITEOFF"]   = "Y" if key in woff_keys else "N"
    row["WRIOFF_BAL"] = woff_map.get(key, row.get("WRIOFF_BAL", 0.0))
    cap_rows.append(row)

cap_df2 = pl.DataFrame(cap_rows)
cap_df2.write_parquet(cap_path)

# STATUS='P' AND WRITEOFF='Y' recalculation
rows_recalc = []
for row in cap_df2.iter_rows(named=True):
    if row.get("STATUS") == "P" and row.get("WRITEOFF") == "Y":
        wrioff = row.get("WRIOFF_BAL") or 0.0
        open_b = row.get("OPEN_BALANCE") or 0.0
        suspend = wrioff - open_b
        if suspend < 0:
            wrback  = open_b - wrioff
            suspend = 0.0
        else:
            wrback = 0.0
        row["SUSPEND"] = suspend
        row["WRBACK"]  = wrback
    row["NET"] = (row.get("SUSPEND") or 0.0) - (row.get("WRBACK") or 0.0)
    rows_recalc.append(row)

cap_df2 = pl.DataFrame(rows_recalc)
cap_df2.write_parquet(cap_path)

# Merge WOF (written-off HP records)
hpwo_parquet = os.path.join(HP_DIR, f"hpwo{REPTMON}{NOWK}{REPTYEAR}.parquet")
if os.path.exists(hpwo_parquet):
    wof_df = con.execute(
        f"SELECT ACCTNO, NOTENO, PRODUCT FROM read_parquet('{hpwo_parquet}')"
    ).pl()
    cap_df3 = cap_df2.join(wof_df, on=["ACCTNO", "NOTENO"], how="left", suffix="_wof")
else:
    cap_df3 = cap_df2

cap_df3.write_parquet(cap_path)

# ─────────────────────────────────────────────
# GENERATE SUMMARY REPORT – PBB MOVEMENT OF CAP BY BRANCH
# * GENERATE SUMMARY REPORT *
# TBL1 = PBB MOVEMENT OF CAP BY BRANCH AS AT
# ─────────────────────────────────────────────
TBL1 = "PBB MOVEMENT OF CAP BY BRANCH AS AT"
PAGE_WIDTH  = 200
PAGE_HEIGHT = 100

report_df = con.execute(f"SELECT * FROM read_parquet('{cap_path}')").pl()

# Aggregate by BRANCH1 / BRANCHABBR
branch_agg = (
    report_df
    .group_by(["BRANCH1", "BRANCHABBR"])
    .agg([
        pl.count("ACCTNO").alias("N"),
        pl.sum("BALANCE").alias("BALANCE"),
        pl.sum("OPEN_BALANCE").alias("OPEN_BALANCE"),
        pl.sum("SUSPEND").alias("SUSPEND"),
        pl.sum("WRBACK").alias("WRBACK"),
        pl.sum("WRIOFF_BAL").alias("WRIOFF_BAL"),
        pl.sum("CAP").alias("CAP"),
        pl.sum("NET").alias("NET"),
    ])
    .sort("BRANCH1")
)

totals = report_df.select([
    pl.count("ACCTNO").alias("N"),
    pl.sum("BALANCE").alias("BALANCE"),
    pl.sum("OPEN_BALANCE").alias("OPEN_BALANCE"),
    pl.sum("SUSPEND").alias("SUSPEND"),
    pl.sum("WRBACK").alias("WRBACK"),
    pl.sum("WRIOFF_BAL").alias("WRIOFF_BAL"),
    pl.sum("CAP").alias("CAP"),
    pl.sum("NET").alias("NET"),
])

def fc(val) -> str:
    """Format COMMA20.2"""
    if val is None: return " " * 20
    return f"{float(val):>20,.2f}"

def fi(val) -> str:
    """Format COMMA7 integer"""
    if val is None: return " " * 7
    return f"{int(val):>7,}"

COL_HEADERS = [
    ("BRANCH",        15),
    ("NO OF ACCOUNT",  7),
    ("BALANCE",       20),
    ("OPENING BALANCE",20),
    ("CHARGE FOR THE YEAR",20),
    ("WRITTEN BACK TO P & L",20),
    ("WRITTEN-OFF",   20),
    ("CLOSING BALANCE",20),
    ("NET INCREASE/DECREASE",20),
]

output_lines: list[str] = []

def emit(cc: str, content: str = "") -> None:
    output_lines.append(cc + content)

# Page header
emit("1", f"{TBL1} {DATE_STR}")
emit(" ", "")

# Column header row
hdr = (
    f"{'BRANCH':<15} {'NO OF ACCOUNT':>7} {'BALANCE':>20} {'OPENING BALANCE':>20}"
    f" {'CHARGE FOR THE YEAR':>20} {'WRITTEN BACK TO P & L':>20}"
    f" {'WRITTEN-OFF':>20} {'CLOSING BALANCE':>20} {'NET INCREASE/DECREASE':>20}"
)
emit(" ", hdr)
emit(" ", "-" * len(hdr))

for row in branch_agg.iter_rows(named=True):
    line = (
        f"{(row['BRANCH1'] or ''):<15} {fi(row['N'])}"
        f" {fc(row['BALANCE'])} {fc(row['OPEN_BALANCE'])}"
        f" {fc(row['SUSPEND'])} {fc(row['WRBACK'])}"
        f" {fc(row['WRIOFF_BAL'])} {fc(row['CAP'])} {fc(row['NET'])}"
    )
    emit(" ", line)

# TOTAL row
emit(" ", "-" * len(hdr))
tot = totals.row(0, named=True)
total_line = (
    f"{'TOTAL':<15} {fi(tot['N'])}"
    f" {fc(tot['BALANCE'])} {fc(tot['OPEN_BALANCE'])}"
    f" {fc(tot['SUSPEND'])} {fc(tot['WRBACK'])}"
    f" {fc(tot['WRIOFF_BAL'])} {fc(tot['CAP'])} {fc(tot['NET'])}"
)
emit(" ", total_line)

with open(OUTPUT1_PATH, "w", encoding="utf-8") as fout:
    for line in output_lines:
        fout.write(line + "\n")

print(f"Report written : {OUTPUT1_PATH}")
print(f"CAP parquet    : {cap_path}")
