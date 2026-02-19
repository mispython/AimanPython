# !/usr/bin/env python3
"""
Program : EIBMMEF2
Purpose : Performance Report on Product 574 (MEF Report)
          Incorporates conventional retail product codes 607 into MEF report.
          Produces two sections:
            1) MEF summary report with CGC/non-CGC guaranteed breakdown
            2) MEF Impaired Loans (IL) report
ESMR    : 2012-1093 (CWK)
"""

import duckdb
import polars as pl
import os
from datetime import date, datetime

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR       = r"/data"
SAS_DIR        = os.path.join(BASE_DIR, "sasdata")       # SAS.* parquet files
LOAN_DIR       = os.path.join(BASE_DIR, "loan")          # LOAN.REPTDATE, current/prev month loan
DISPAY_DIR     = os.path.join(BASE_DIR, "dispay")        # DISPAY.DISPAYMTH<mm>
CCRIS_DIR      = os.path.join(BASE_DIR, "ccris")         # CCRIS.CREDMSUBAC, CCRIS.PROVSUBAC
OUTPUT_DIR     = os.path.join(BASE_DIR, "output")

OUTPUT_FILE    = os.path.join(OUTPUT_DIR, "PBMEF.RPT.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

PAGE_LENGTH    = 60   # lines per page (ASA default)

# =============================================================================
# HELPER: ASA carriage control characters
# =============================================================================
# '1' = skip to new page, ' ' = single space, '0' = double space, '+' = no advance

def asa_line(cc: str, text: str) -> str:
    """Return a line prefixed with ASA carriage-control character."""
    return f"{cc}{text}\n"

def new_page_lines(lines: list) -> list:
    """Mark the first line of a new page with '1', rest with ' '."""
    result = []
    for i, ln in enumerate(lines):
        cc = '1' if i == 0 else ' '
        result.append(asa_line(cc, ln))
    return result

# =============================================================================
# STEP 1: Read REPTDATE and derive macro variables
# =============================================================================
reptdate_path = os.path.join(LOAN_DIR, "REPTDATE.parquet")
con = duckdb.connect()

reptdate_row = con.execute(f"""
    SELECT REPTDATE FROM read_parquet('{reptdate_path}') LIMIT 1
""").fetchone()

reptdate: date = reptdate_row[0]
if isinstance(reptdate, (int, float)):
    # SAS date numeric -> days since 1960-01-01
    reptdate = date(1960, 1, 1) + __import__('datetime').timedelta(days=int(reptdate))

reptdate_dt = reptdate if isinstance(reptdate, date) else reptdate.date()

reptmon  = f"{reptdate_dt.month:02d}"           # REPTMON  e.g. "07"
prevmon_val = reptdate_dt.month - 1
prevmon_val = 12 if prevmon_val == 0 else prevmon_val
prevmon  = f"{prevmon_val:02d}"                 # PREVMON  e.g. "06"
nowk     = "4"                                  # NOWK always '4'
reptyear = f"{reptdate_dt.year % 100:02d}"      # REPTYEAR 2-digit year
rdate    = reptdate_dt.strftime("%d/%m/%y")     # DDMMYY8  e.g. "31/07/24"

# =============================================================================
# STEP 2: Load previous month loan data (PRVMTH) -- products 574 or 607
# =============================================================================
# *** PREVIOUS MTH ***
prvmth_path = os.path.join(SAS_DIR, f"LOAN{prevmon}{nowk}.parquet")
prvmth = con.execute(f"""
    SELECT *
    FROM read_parquet('{prvmth_path}')
    WHERE PRODUCT = 574 OR PRODUCT = 607
""").pl()

# =============================================================================
# STEP 3: Load current month loan data (CURMTH) -- products 574 or 607, active
# =============================================================================
# *** CURRENT MTH ***
curmth_path = os.path.join(SAS_DIR, f"LOAN{reptmon}{nowk}.parquet")
curmth = con.execute(f"""
    SELECT *, 1 AS NOACC
    FROM read_parquet('{curmth_path}')
    WHERE PAIDIND NOT IN ('P','C')
      AND BALANCE <> 0
      AND (PRODUCT = 574 OR PRODUCT = 607)
""").pl()

# =============================================================================
# STEP 4: Merge PRVMTH + CURMTH -> LOANX  (left/outer merge on ACCTNO, NOTENO)
# =============================================================================
# Rename BALANCE in prvmth to LASTBAL before merge
prvmth_renamed = prvmth.rename({"BALANCE": "LASTBAL"})

loanx = prvmth_renamed.join(
    curmth,
    on=["ACCTNO", "NOTENO"],
    how="outer_coalesce"
)

# =============================================================================
# STEP 5: Load DISPAY (disbursements / repayments > 0)
# =============================================================================
dispay_path = os.path.join(DISPAY_DIR, f"DISPAYMTH{reptmon}.parquet")
dispay = con.execute(f"""
    SELECT *
    FROM read_parquet('{dispay_path}')
    WHERE DISBURSE > 0 OR REPAID > 0
""").pl()

# =============================================================================
# STEP 6: Inner merge LOANX + DISPAY -> LOAN
# =============================================================================
loan = loanx.join(dispay, on=["ACCTNO", "NOTENO"], how="inner")

# =============================================================================
# STEP 7: Split into WITH (CENSUS in 574.00,574.02) and WITHX (CENSUS=574.01)
# =============================================================================
with_df  = loan.filter(pl.col("CENSUS").is_in([574.00, 574.02]))
withx_df = loan.filter(pl.col("CENSUS") == 574.01)

# =============================================================================
# STEP 8: Build WITH_DIS  -- disbursement/repayment rows for WITH
# =============================================================================
def build_dis_rows(df: pl.DataFrame) -> pl.DataFrame:
    """Expand each row into disbursement and/or repayment typed rows."""
    rows = []
    for row in df.iter_rows(named=True):
        dis = row.get("DISBURSE") or 0
        rep = row.get("REPAID")   or 0
        if dis not in (None, 0):
            r = dict(row)
            r["TYPE"]   = "DISBURSEMENT"
            r["NOACC1"] = 1
            r["NODIS"]  = 1
            r["NOREP"]  = None
            rows.append(r)
        if rep not in (None, 0):
            r = dict(row)
            r["TYPE"]   = "REPAYMENT"
            r["NOACC2"] = 1
            r["NOREP"]  = 1
            r["NODIS"]  = None
            rows.append(r)
    if rows:
        return pl.DataFrame(rows)
    schema = {**{c: df.schema[c] for c in df.columns},
              "TYPE": pl.Utf8, "NOACC1": pl.Int64,
              "NOACC2": pl.Int64, "NODIS": pl.Int64, "NOREP": pl.Int64}
    return pl.DataFrame(schema=schema)

with_dis = build_dis_rows(with_df)

# =============================================================================
# STEP 9: Build WITH_BAL -- outstanding balance rows for WITH (from LOANX)
# =============================================================================
with_bal = (
    loanx
    .filter(pl.col("CENSUS").is_in([574.00, 574.02]))
    .filter(~pl.col("BALANCE").is_in([None, 0]))
    .select([pl.col("BALANCE"), pl.lit(1).alias("NOBAL"), pl.lit("OUTSTANDING").alias("TYPE")])
)

# =============================================================================
# STEP 10: Union WITH_DIS + WITH_BAL, then PROC SUMMARY by TYPE
# =============================================================================
# Align columns before union
def align_union(df1: pl.DataFrame, df2: pl.DataFrame) -> pl.DataFrame:
    all_cols = list(dict.fromkeys(list(df1.columns) + list(df2.columns)))
    def add_missing(df):
        for c in all_cols:
            if c not in df.columns:
                df = df.with_columns(pl.lit(None).cast(pl.Float64).alias(c))
        return df.select(all_cols)
    return pl.concat([add_missing(df1), add_missing(df2)])

withnew = align_union(with_dis, with_bal)

# PROC SUMMARY NWAY by TYPE; VAR REPAID NOREP DISBURSE NODIS BALANCE NOBAL
def summarize_by_type(df: pl.DataFrame, var_cols: list) -> pl.DataFrame:
    existing = [c for c in var_cols if c in df.columns]
    agg = [pl.col(c).cast(pl.Float64).sum().alias(c) for c in existing]
    return (
        df.with_columns([pl.col(c).cast(pl.Float64) if c in df.columns else pl.lit(None).cast(pl.Float64).alias(c)
                         for c in var_cols])
          .group_by("TYPE")
          .agg(agg)
    )

withf = summarize_by_type(withnew, ["REPAID","NOREP","DISBURSE","NODIS","BALANCE","NOBAL"])

# =============================================================================
# STEP 11: Build WITHX_DIS for WITHX
# =============================================================================
def build_disx_rows(df: pl.DataFrame) -> pl.DataFrame:
    rows = []
    for row in df.iter_rows(named=True):
        dis = row.get("DISBURSE") or 0
        rep = row.get("REPAID")   or 0
        r = dict(row)
        r["NOACC1"] = 1
        r["NOACC2"] = 1
        if dis not in (None, 0):
            r["TYPE"]   = "DISBURSEMENT"
            r["NODISX"] = 1
        else:
            r["NODISX"] = None
        if rep not in (None, 0):
            r["TYPE"]   = "REPAYMENT"
            r["NOREPX"] = 1
        else:
            r["NOREPX"] = None
        rows.append(r)
    if rows:
        result = pl.DataFrame(rows)
        # Rename REPAID->REPAIDX, DISBURSE->DISBURSEX
        if "REPAID"   in result.columns: result = result.rename({"REPAID":   "REPAIDX"})
        if "DISBURSE" in result.columns: result = result.rename({"DISBURSE": "DISBURSEX"})
        return result
    return pl.DataFrame()

withx_dis = build_disx_rows(withx_df)

# =============================================================================
# STEP 12: Build WITHX_BAL (from LOANX, CENSUS=574.01)
# =============================================================================
withx_bal = (
    loanx
    .filter(pl.col("CENSUS") == 574.01)
    .filter(~pl.col("BALANCE").is_in([None, 1]))   # SAS: BALANCEX NOT IN (.,1)
    .select([
        pl.col("BALANCE").alias("BALANCEX"),
        pl.lit(1).alias("NOBALX"),
        pl.lit("OUTSTANDING").alias("TYPE")
    ])
)

# =============================================================================
# STEP 13: Union WITHX_DIS + WITHX_BAL -> WITHXNEW, then PROC SUMMARY
# =============================================================================
withxnew = align_union(withx_dis, withx_bal)
withxf   = summarize_by_type(withxnew, ["REPAIDX","NOREPX","DISBURSEX","NODISX","BALANCEX","NOBALX"])

# =============================================================================
# STEP 14: Merge WITHF + WITHXF -> CGC (by TYPE); add GROUP='X'
# =============================================================================
cgc = withf.join(withxf, on="TYPE", how="outer_coalesce").with_columns(pl.lit("X").alias("GROUP"))

# PROC SUMMARY on CGC by GROUP; VAR DISBURSE NODIS DISBURSEX NODISX
groupx = (
    cgc
    .with_columns([pl.col(c).cast(pl.Float64) for c in ["DISBURSE","NODIS","DISBURSEX","NODISX"]
                   if c in cgc.columns])
    .group_by("GROUP")
    .agg([pl.col(c).sum() for c in ["DISBURSE","NODIS","DISBURSEX","NODISX"] if c in cgc.columns])
    .with_columns(pl.lit("DISBURSEMENT").alias("TYPE"))
)

# =============================================================================
# STEP 15: Merge CGC + GROUPX by TYPE -> ALL_CGC
# =============================================================================
all_cgc = cgc.join(
    groupx.select(["TYPE"] + [c for c in groupx.columns if c not in cgc.columns or c == "TYPE"]),
    on="TYPE", how="left", suffix="_GX"
)

# Build final reporting columns
def get_val(row, col):
    v = row.get(col)
    return 0.0 if v is None else float(v)

report_rows = []
for row in all_cgc.iter_rows(named=True):
    t = row.get("TYPE", "")
    if t == "REPAYMENT":
        noacct   = get_val(row, "NOREP")
        amount   = get_val(row, "REPAID")
        noacctx  = get_val(row, "NOREPX")
        amountx  = get_val(row, "REPAIDX")
    elif t == "DISBURSEMENT":
        noacct   = get_val(row, "NODIS")
        amount   = get_val(row, "DISBURSE")
        noacctx  = get_val(row, "NODISX")
        amountx  = get_val(row, "DISBURSEX")
    elif t == "OUTSTANDING":
        noacct   = get_val(row, "NOBAL")
        amount   = get_val(row, "BALANCE")
        noacctx  = get_val(row, "NOBALX")
        amountx  = get_val(row, "BALANCEX")
    else:
        continue
    totacct   = noacct + noacctx
    totamount = amount + amountx
    report_rows.append({
        "TYPE":      t,
        "NOACCT":    noacct,
        "AMOUNT":    amount,
        "NOACCTX":   noacctx,
        "AMOUNTX":   amountx,
        "TOTACCT":   totacct,
        "TOTAMOUNT": totamount,
    })

all_cgc_final = pl.DataFrame(report_rows)

# =============================================================================
# STEP 16: Write Section 1 -- PBMF report (MEF summary)
# =============================================================================
def fmt_amount(v) -> str:
    """Format as COMMA15.2 (right-justified in 15 chars)."""
    if v is None:
        return f"{'0.00':>15}"
    return f"{v:>15,.2f}"

def fmt_count(v) -> str:
    if v is None:
        return f"{'0':>10}"
    return f"{int(v):>10,}"

output_lines = []   # list of (cc, text) tuples

def emit(cc: str, text: str):
    output_lines.append(f"{cc}{text}\n")

# -- Header (first call _N_=1)
emit('1', 'PUBLIC BANK BERHAD')
emit(' ', f'PERFORMANCE REPORT ON PRODUCT 574 AS AT {rdate}')
emit(' ', 'REPORT ID : EIBMMEF2')
emit(' ', '                          ')
emit(' ', '                          ')

col_mef      = 'MEF'
col_with     = 'WITH CGC GUARANTEED'
col_without  = 'WITHOUT CGC GUARANTEED'
col_total    = 'TOTAL'

line = f"{col_mef:<19}{col_with:<30}{col_without:<30}{col_total}"
emit(' ', line)

# sub-header
line2 = (
    f"{'':19}"
    f"{'NO ACCT':<10}{'AMOUNT':<20}"
    f"{'NO ACCT':<10}{'AMOUNT':<20}"
    f"{'NO ACCT':<10}{'AMOUNT'}"
)
emit(' ', line2)

# Data rows -- ordered: DISBURSEMENT, REPAYMENT, OUTSTANDING
type_order = ["DISBURSEMENT", "REPAYMENT", "OUTSTANDING"]
cgc_dict   = {r["TYPE"]: r for r in report_rows}

for t in type_order:
    if t not in cgc_dict:
        continue
    r = cgc_dict[t]
    line = (
        f"{t:<19}"
        f"{fmt_count(r['NOACCT'])}"
        f"{fmt_amount(r['AMOUNT'])}"
        f"  "
        f"{fmt_count(r['NOACCTX'])}"
        f"{fmt_amount(r['AMOUNTX'])}"
        f"  "
        f"{fmt_count(r['TOTACCT'])}"
        f"{fmt_amount(r['TOTAMOUNT'])}"
    )
    emit(' ', line)

# =============================================================================
# SECTION 2: Impaired Loans (IL)
# =============================================================================
# -- Reload LOANX for the IL section (same data already in loanx)
# -- Merge with CREDMSUB and PROVSUBAC

credmsub_path  = os.path.join(CCRIS_DIR, f"CREDMSUBAC{reptmon}{reptyear}.parquet")
provsubac_path = os.path.join(CCRIS_DIR, f"PROVSUBAC{reptmon}{reptyear}.parquet")

credmsub = con.execute(f"""
    SELECT ACCTNUM AS ACCTNO, NOTENO, BRANCH, DAYSARR AS DAYARR, MTHARR
    FROM read_parquet('{credmsub_path}')
""").pl()

provsubac = con.execute(f"""
    SELECT ACCTNUM AS ACCTNO, NOTENO, BRANCH
    FROM read_parquet('{provsubac_path}')
    WHERE IMPAIRED_LOAN = 'Y'
""").pl()

# Merge LOANX + CREDMSUB + PROVSUBAC(inner)
loanx_cc = (
    loanx
    .join(credmsub,  on=["ACCTNO","NOTENO","BRANCH"], how="left")
    .join(provsubac, on=["ACCTNO","NOTENO","BRANCH"], how="inner")
)

# ILCAT format mapping
ILCAT = {
    "A": "< 3 MTHS",
    "B": "3 TO LESS THAN 6 MTHS",
    "C": "6 TO LESS THAN 9 MTHS",
    "D": ">= 9 MTHS",
}

# MEF -- CENSUS in (574.00,574.02), BALANCE not 0/null
def assign_typx(mtharr):
    if mtharr is None:
        return "D"
    if mtharr < 3:
        return "A"
    if mtharr < 6:
        return "B"
    if mtharr < 9:
        return "C"
    return "D"

def build_il_summary(df: pl.DataFrame, bal_col: str, suffix: str) -> pl.DataFrame:
    """Build PROC SUMMARY equivalent for IL section."""
    rows = []
    for row in df.filter(~pl.col("BALANCE").is_in([None, 0])).iter_rows(named=True):
        typx = assign_typx(row.get("MTHARR"))
        bal  = float(row.get("BALANCE") or 0)
        rows.append({"TYPX": typx, bal_col: bal,
                     f"NO1{suffix}": 1 if typx=="A" else None,
                     f"NO2{suffix}": 1 if typx=="B" else None,
                     f"NO3{suffix}": 1 if typx=="C" else None,
                     f"NO4{suffix}": 1 if typx=="D" else None})
    if not rows:
        return pl.DataFrame({"TYPX": pl.Series([], dtype=pl.Utf8)})
    tmp = pl.DataFrame(rows)
    agg_cols = [bal_col, f"NO1{suffix}", f"NO2{suffix}", f"NO3{suffix}", f"NO4{suffix}"]
    return (
        tmp
        .with_columns([pl.col(c).cast(pl.Float64) for c in agg_cols])
        .group_by("TYPX")
        .agg([pl.col(c).sum() for c in agg_cols])
    )

mef_df  = loanx_cc.filter(pl.col("CENSUS").is_in([574.00, 574.02]))
mefxx_df= loanx_cc.filter(pl.col("CENSUS") == 574.01)

mefx  = build_il_summary(mef_df,   "BALANCE",  "")    # NO1,NO2,NO3,NO4
mefxx = build_il_summary(mefxx_df, "BALANCEX", "X")   # NO1X,NO2X,NO3X,NO4X

all_il = mefx.join(mefxx, on="TYPX", how="outer_coalesce")

# Build ALL_IL reporting rows
il_rows = []
no_map  = {"A":"NO1","B":"NO2","C":"NO3","D":"NO4"}

for row in all_il.iter_rows(named=True):
    typx    = row.get("TYPX","")
    nk      = no_map.get(typx,"NO1")
    amount  = float(row.get("BALANCE")  or 0)
    noacct  = float(row.get(nk)         or 0)
    amountx = float(row.get("BALANCEX") or 0)
    noacctx = float(row.get(f"{nk}X")   or 0)
    totacct   = noacct  + noacctx
    totamount = amount  + amountx
    il_rows.append({
        "TYPX":      typx,
        "AMOUNT":    amount,
        "NOACCT":    noacct,
        "AMOUNTX":   amountx,
        "NOACCTX":   noacctx,
        "TOTACCT":   totacct,
        "TOTAMOUNT": totamount,
    })

# Sort by TYPX (A,B,C,D)
il_rows.sort(key=lambda x: x["TYPX"])

# -- IL Header
emit(' ', '                  ')
emit(' ', '                  ')
emit(' ', 'PUBLIC BANK BERHAD')
emit(' ', f'PERFORMANCE REPORT ON PRODUCT 574 AS AT {rdate}')
emit(' ', 'REPORT ID : EIBMMEF2 (IMPARED LOANS)')
emit(' ', '                          ')
emit(' ', '                          ')

line_hdr = (
    f"{'MEF (IL)':<29}"
    f"{'WITH CGC GUARANTEED':<30}"
    f"{'WITHOUT CGC GUARANTEED':<30}"
    f"{'TOTAL'}"
)
emit(' ', line_hdr)

line_sub = (
    f"{'':29}"
    f"{'NO ACCT':<10}{'AMOUNT':<20}"
    f"{'NO ACCT':<10}{'AMOUNT':<20}"
    f"{'NO ACCT':<10}{'AMOUNT'}"
)
emit(' ', line_sub)

# Data rows + running totals
gacc=0; gamt=0.0; gaccx=0; gamtx=0.0; gtacc=0; gtamt=0.0

for r in il_rows:
    typx_label = ILCAT.get(r["TYPX"], "")
    line = (
        f"{typx_label:<29}"
        f"{fmt_count(r['NOACCT'])}"
        f"{fmt_amount(r['AMOUNT'])}"
        f"  "
        f"{fmt_count(r['NOACCTX'])}"
        f"{fmt_amount(r['AMOUNTX'])}"
        f"  "
        f"{fmt_count(r['TOTACCT'])}"
        f"{fmt_amount(r['TOTAMOUNT'])}"
    )
    emit(' ', line)
    gacc  += int(r["NOACCT"])
    gamt  += r["AMOUNT"]
    gaccx += int(r["NOACCTX"])
    gamtx += r["AMOUNTX"]
    gtacc += int(r["TOTACCT"])
    gtamt += r["TOTAMOUNT"]

# TOTAL line
total_line = (
    f"{'TOTAL':<29}"
    f"{fmt_count(gacc)}"
    f"{fmt_amount(gamt)}"
    f"  "
    f"{fmt_count(gaccx)}"
    f"{fmt_amount(gamtx)}"
    f"  "
    f"{fmt_count(gtacc)}"
    f"{fmt_amount(gtamt)}"
)
emit(' ', total_line)

# =============================================================================
# STEP 17: Write output file with ASA carriage control & pagination
# =============================================================================
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    line_count = 0
    first_page = True
    for raw_line in output_lines:
        cc   = raw_line[0]
        text = raw_line[1:]

        if cc == '1':
            # new page
            if not first_page:
                # pad remaining lines of current page
                while line_count < PAGE_LENGTH:
                    f.write(f" \n")
                    line_count += 1
            first_page = False
            line_count = 0
            f.write(f"1{text}")
            line_count += 1
        else:
            f.write(f"{cc}{text}")
            line_count += 1

        if line_count >= PAGE_LENGTH:
            line_count = 0

con.close()
print(f"Report written to: {OUTPUT_FILE}")
