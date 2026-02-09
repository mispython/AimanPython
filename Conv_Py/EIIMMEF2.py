#!/usr/bin/env python3
"""
EIIMMEF2 Report Generation
Performance Report on Product 428 and 439
"""

import duckdb
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import calendar


# ============================================================================
# PATH SETUP
# ============================================================================
BASE_PATH = Path(__file__).resolve().parent

INPUT_PATH = BASE_PATH / "/data/input"
OUTPUT_PATH = BASE_PATH / "/output"

# Input files
REPTDATE_FILE = INPUT_PATH / "reptdate.parquet"
DISPAY_FILE = INPUT_PATH / "dispay.parquet"
LOAN_FILES_PATTERN = INPUT_PATH / "loan_{month}{week}.parquet"
CREDMSUB_FILE = INPUT_PATH / "credmsub.parquet"

# Output file
OUTPUT_FILE = OUTPUT_PATH / "PBMEF2.txt"

# Ensure output directory exists
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)


# ============================================================================
# INITIALIZE DUCKDB CONNECTION
# ============================================================================
con = duckdb.connect()


# ============================================================================
# GET REPORTING DATE AND CALCULATE PARAMETERS
# ============================================================================
reptdate_df = pl.read_parquet(REPTDATE_FILE)
reptdate = reptdate_df.select("REPTDATE").item(0, 0)

if isinstance(reptdate, str):
    reptdate = datetime.strptime(reptdate, "%Y-%m-%d")

reptmon = reptdate.month
prevmon = reptmon - 1 if reptmon > 1 else 12
nowk = 4
reptyear = reptdate.strftime("%y")
rdate = reptdate.strftime("%d/%m/%y")

print(f"Report Date: {rdate}")
print(f"Current Month: {reptmon:02d}")
print(f"Previous Month: {prevmon:02d}")
print()


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
def format_number(value, width=10, decimals=0):
    """Format number with comma separator"""
    if value is None or (isinstance(value, float) and pl.Float64 is not None):
        value = 0
    if decimals > 0:
        return f"{value:,.{decimals}f}".rjust(width)
    else:
        return f"{int(value):,}".rjust(width)


def write_asa_line(f, line, carriage_control=' '):
    """Write line with ASA carriage control character"""
    f.write(f"{carriage_control}{line}\n")


# ============================================================================
# PART 1: MEF REPORT (DISBURSEMENT, REPAYMENT, OUTSTANDING)
# ============================================================================

print("Processing MEF Report (Disbursement, Repayment, Outstanding)...")

# Load previous and current month loan data
prvmth_file = INPUT_PATH / f"loan_{prevmon:02d}{nowk}.parquet"
curmth_file = INPUT_PATH / f"loan_{reptmon:02d}{nowk}.parquet"

query_prvmth = f"""
    SELECT *
    FROM read_parquet('{prvmth_file}')
    WHERE PRODUCT IN (428, 439)
"""
prvmth = con.execute(query_prvmth).pl()

query_curmth = f"""
    SELECT *, 1 as NOACC
    FROM read_parquet('{curmth_file}')
    WHERE PAIDIND NOT IN ('P', 'C') 
    AND BALANCE != 0
    AND PRODUCT IN (428, 439)
"""
curmth = con.execute(query_curmth).pl()

# Merge datasets
loanx = prvmth.rename({"BALANCE": "LASTBAL"}).join(
    curmth, on=["ACCTNO", "NOTENO"], how="full", suffix="_cur"
)

# Load disbursement/payment data
dispay_file_month = INPUT_PATH / f"idispaymth{reptmon:02d}.parquet"
query_dispay = f"""
    SELECT *
    FROM read_parquet('{dispay_file_month}')
    WHERE DISBURSE > 0 OR REPAID > 0
"""
dispay = con.execute(query_dispay).pl()

# Merge loan and dispay data
loan = loanx.join(dispay, on=["ACCTNO", "NOTENO"], how="inner")

# Split into WITH and WITHX (with and without CGC)
with_df = loan.filter(
    (pl.col("CENSUS").is_in([428.00, 428.02]))
)

withx_df = loan.filter(
    (pl.col("CENSUS").is_in([428.01, 428.03])) | (pl.col("PRODUCT") == 439)
)

# Process WITH disbursement/repayment
with_dis = with_df.with_columns([
    pl.when(pl.col("DISBURSE").fill_null(0) != 0)
    .then(pl.lit("DISBURSEMENT"))
    .when(pl.col("REPAID").fill_null(0) != 0)
    .then(pl.lit("REPAYMENT"))
    .otherwise(pl.lit(None))
    .alias("TYPE"),
    pl.when(pl.col("DISBURSE").fill_null(0) != 0)
    .then(pl.lit(1))
    .otherwise(pl.lit(None))
    .alias("NOACC1"),
    pl.when(pl.col("REPAID").fill_null(0) != 0)
    .then(pl.lit(1))
    .otherwise(pl.lit(None))
    .alias("NOACC2")
])

with_dis = with_dis.with_columns([
    pl.when(pl.col("DISBURSE").fill_null(0) != 0)
    .then(pl.col("NOACC1"))
    .otherwise(pl.lit(None))
    .alias("NODIS"),
    pl.when(pl.col("REPAID").fill_null(0) != 0)
    .then(pl.col("NOACC2"))
    .otherwise(pl.lit(None))
    .alias("NOREP")
])

# Process WITH balance
with_bal = loanx.filter(
    (pl.col("CENSUS").is_in([428.00, 428.02])) &
    (pl.col("BALANCE").fill_null(0) != 0)
).with_columns([
    pl.lit("OUTSTANDING").alias("TYPE"),
    pl.lit(1).alias("NOBAL")
]).select(["BALANCE", "NOBAL", "TYPE"])

# Combine WITH data
withnew = pl.concat([with_dis, with_bal], how="diagonal")

# Aggregate WITH data
withf = withnew.group_by("TYPE").agg([
    pl.col("REPAID").sum().alias("REPAID"),
    pl.col("NOREP").sum().alias("NOREP"),
    pl.col("DISBURSE").sum().alias("DISBURSE"),
    pl.col("NODIS").sum().alias("NODIS"),
    pl.col("BALANCE").sum().alias("BALANCE"),
    pl.col("NOBAL").sum().alias("NOBAL")
])

# Process WITHX disbursement/repayment
withx_dis = withx_df.with_columns([
    pl.when(pl.col("DISBURSE").fill_null(0) != 0)
    .then(pl.lit("DISBURSEMENT"))
    .when(pl.col("REPAID").fill_null(0) != 0)
    .then(pl.lit("REPAYMENT"))
    .otherwise(pl.lit(None))
    .alias("TYPE"),
    pl.when(pl.col("DISBURSE").fill_null(0) != 0)
    .then(pl.lit(1))
    .otherwise(pl.lit(None))
    .alias("NOACC1"),
    pl.when(pl.col("REPAID").fill_null(0) != 0)
    .then(pl.lit(1))
    .otherwise(pl.lit(None))
    .alias("NOACC2")
])

withx_dis = withx_dis.with_columns([
    pl.when(pl.col("DISBURSE").fill_null(0) != 0)
    .then(pl.col("NOACC1"))
    .otherwise(pl.lit(None))
    .alias("NODISX"),
    pl.when(pl.col("REPAID").fill_null(0) != 0)
    .then(pl.col("NOACC2"))
    .otherwise(pl.lit(None))
    .alias("NOREPX")
]).rename({"REPAID": "REPAIDX", "DISBURSE": "DISBURSEX"})

# Process WITHX balance
withx_bal = loanx.filter(
    ((pl.col("CENSUS").is_in([428.01, 428.03])) | (pl.col("PRODUCT") == 439)) &
    (pl.col("BALANCE").fill_null(0) != 0)
).with_columns([
    pl.col("BALANCE").alias("BALANCEX"),
    pl.lit("OUTSTANDING").alias("TYPE"),
    pl.lit(1).alias("NOBALX")
]).select(["BALANCEX", "NOBALX", "TYPE"])

# Combine WITHX data
withxnew = pl.concat([withx_dis, withx_bal], how="diagonal")

# Aggregate WITHX data
withxf = withxnew.group_by("TYPE").agg([
    pl.col("REPAIDX").sum().alias("REPAIDX"),
    pl.col("NOREPX").sum().alias("NOREPX"),
    pl.col("DISBURSEX").sum().alias("DISBURSEX"),
    pl.col("NODISX").sum().alias("NODISX"),
    pl.col("BALANCEX").sum().alias("BALANCEX"),
    pl.col("NOBALX").sum().alias("NOBALX")
])

# Merge CGC data
cgc = withf.join(withxf, on="TYPE", how="full").with_columns([
    pl.lit("X").alias("GROUP")
])

# Create GROUP summary for disbursement
groupx = cgc.group_by("GROUP").agg([
    pl.col("DISBURSE").sum().alias("DISBURSE"),
    pl.col("NODIS").sum().alias("NODIS"),
    pl.col("DISBURSEX").sum().alias("DISBURSEX"),
    pl.col("NODISX").sum().alias("NODISX")
]).with_columns([
    pl.lit("DISBURSEMENT").alias("TYPE")
])

# Merge all CGC data
all_cgc = cgc.join(groupx, on="TYPE", how="full", suffix="_grp")

# Calculate final amounts and accounts
all_cgc = all_cgc.with_columns([
    pl.when(pl.col("TYPE") == "REPAYMENT")
    .then(pl.col("NOREP"))
    .when(pl.col("TYPE") == "DISBURSEMENT")
    .then(pl.col("NODIS"))
    .when(pl.col("TYPE") == "OUTSTANDING")
    .then(pl.col("NOBAL"))
    .otherwise(pl.lit(None))
    .fill_null(0)
    .alias("NOACCT"),

    pl.when(pl.col("TYPE") == "REPAYMENT")
    .then(pl.col("REPAID"))
    .when(pl.col("TYPE") == "DISBURSEMENT")
    .then(pl.col("DISBURSE"))
    .when(pl.col("TYPE") == "OUTSTANDING")
    .then(pl.col("BALANCE"))
    .otherwise(pl.lit(None))
    .fill_null(0)
    .alias("AMOUNT"),

    pl.when(pl.col("TYPE") == "REPAYMENT")
    .then(pl.col("NOREPX"))
    .when(pl.col("TYPE") == "DISBURSEMENT")
    .then(pl.col("NODISX"))
    .when(pl.col("TYPE") == "OUTSTANDING")
    .then(pl.col("NOBALX"))
    .otherwise(pl.lit(None))
    .fill_null(0)
    .alias("NOACCTX"),

    pl.when(pl.col("TYPE") == "REPAYMENT")
    .then(pl.col("REPAIDX"))
    .when(pl.col("TYPE") == "DISBURSEMENT")
    .then(pl.col("DISBURSEX"))
    .when(pl.col("TYPE") == "OUTSTANDING")
    .then(pl.col("BALANCEX"))
    .otherwise(pl.lit(None))
    .fill_null(0)
    .alias("AMOUNTX")
])

all_cgc = all_cgc.with_columns([
    (pl.col("NOACCT") + pl.col("NOACCTX")).alias("TOTACCT"),
    (pl.col("AMOUNT") + pl.col("AMOUNTX")).alias("TOTAMOUNT")
])

print("✓ MEF Report data processed")
print()


# ============================================================================
# PART 2: IMPAIRED LOANS (IL) REPORT
# ============================================================================

print("Processing Impaired Loans (IL) Report...")

# Load CCRIS data
credmsub_file_full = INPUT_PATH / f"icredmsubac{reptmon:02d}{reptyear}.parquet"
query_credmsub = f"""
    SELECT 
        ACCTNUM as ACCTNO,
        NOTENO,
        BRANCH,
        DAYSARR as DAYARR,
        MTHARR
    FROM read_parquet('{credmsub_file_full}')
"""
credmsub = con.execute(query_credmsub).pl()

# Merge loanx with CCRIS data
loanx_il = loanx.join(credmsub, on=["ACCTNO", "NOTENO", "BRANCH"], how="left")

# Process MEF (with CGC)
mef = loanx_il.filter(
    (pl.col("CENSUS").is_in([428.00, 428.02])) &
    (pl.col("LOANSTAT") == 3) &
    (pl.col("BALANCE").fill_null(0) != 0)
).with_columns([
    pl.when(pl.col("MTHARR") < 3)
    .then(pl.lit("A"))
    .when(pl.col("MTHARR") < 6)
    .then(pl.lit("B"))
    .when(pl.col("MTHARR") < 9)
    .then(pl.lit("C"))
    .otherwise(pl.lit("D"))
    .alias("TYPX"),

    pl.when(pl.col("MTHARR") < 3).then(pl.lit(1)).otherwise(pl.lit(None)).alias("NO1"),
    pl.when(pl.col("MTHARR") < 6).then(pl.lit(1)).otherwise(pl.lit(None)).alias("NO2"),
    pl.when(pl.col("MTHARR") < 9).then(pl.lit(1)).otherwise(pl.lit(None)).alias("NO3"),
    pl.when(pl.col("MTHARR") >= 9).then(pl.lit(1)).otherwise(pl.lit(None)).alias("NO4")
])

mefx = mef.group_by("TYPX").agg([
    pl.col("BALANCE").sum().alias("BALANCE"),
    pl.col("NO1").sum().alias("NO1"),
    pl.col("NO2").sum().alias("NO2"),
    pl.col("NO3").sum().alias("NO3"),
    pl.col("NO4").sum().alias("NO4")
])

# Process MEFXX (without CGC)
mefxx = loanx_il.filter(
    ((pl.col("CENSUS").is_in([428.01, 428.03])) | (pl.col("PRODUCT") == 439)) &
    (pl.col("LOANSTAT") == 3) &
    (pl.col("BALANCE").fill_null(0) != 0)
).with_columns([
    pl.col("BALANCE").alias("BALANCEX"),

    pl.when(pl.col("MTHARR") < 3)
    .then(pl.lit("A"))
    .when(pl.col("MTHARR") < 6)
    .then(pl.lit("B"))
    .when(pl.col("MTHARR") < 9)
    .then(pl.lit("C"))
    .otherwise(pl.lit("D"))
    .alias("TYPX"),

    pl.when(pl.col("MTHARR") < 3).then(pl.lit(1)).otherwise(pl.lit(None)).alias("NO1X"),
    pl.when(pl.col("MTHARR") < 6).then(pl.lit(1)).otherwise(pl.lit(None)).alias("NO2X"),
    pl.when(pl.col("MTHARR") < 9).then(pl.lit(1)).otherwise(pl.lit(None)).alias("NO3X"),
    pl.when(pl.col("MTHARR") >= 9).then(pl.lit(1)).otherwise(pl.lit(None)).alias("NO4X")
])

mefxx_agg = mefxx.group_by("TYPX").agg([
    pl.col("BALANCEX").sum().alias("BALANCEX"),
    pl.col("NO1X").sum().alias("NO1X"),
    pl.col("NO2X").sum().alias("NO2X"),
    pl.col("NO3X").sum().alias("NO3X"),
    pl.col("NO4X").sum().alias("NO4X")
])

# Merge IL data
all_il = mefx.join(mefxx_agg, on="TYPX", how="full")

# Calculate final IL amounts
all_il = all_il.with_columns([
    pl.when(pl.col("TYPX") == "A")
    .then(pl.col("BALANCE"))
    .when(pl.col("TYPX") == "B")
    .then(pl.col("BALANCE"))
    .when(pl.col("TYPX") == "C")
    .then(pl.col("BALANCE"))
    .when(pl.col("TYPX") == "D")
    .then(pl.col("BALANCE"))
    .otherwise(pl.lit(None))
    .fill_null(0)
    .alias("AMOUNT"),

    pl.when(pl.col("TYPX") == "A")
    .then(pl.col("NO1"))
    .when(pl.col("TYPX") == "B")
    .then(pl.col("NO2"))
    .when(pl.col("TYPX") == "C")
    .then(pl.col("NO3"))
    .when(pl.col("TYPX") == "D")
    .then(pl.col("NO4"))
    .otherwise(pl.lit(None))
    .fill_null(0)
    .alias("NOACCT"),

    pl.when(pl.col("TYPX") == "A")
    .then(pl.col("BALANCEX"))
    .when(pl.col("TYPX") == "B")
    .then(pl.col("BALANCEX"))
    .when(pl.col("TYPX") == "C")
    .then(pl.col("BALANCEX"))
    .when(pl.col("TYPX") == "D")
    .then(pl.col("BALANCEX"))
    .otherwise(pl.lit(None))
    .fill_null(0)
    .alias("AMOUNTX"),

    pl.when(pl.col("TYPX") == "A")
    .then(pl.col("NO1X"))
    .when(pl.col("TYPX") == "B")
    .then(pl.col("NO2X"))
    .when(pl.col("TYPX") == "C")
    .then(pl.col("NO3X"))
    .when(pl.col("TYPX") == "D")
    .then(pl.col("NO4X"))
    .otherwise(pl.lit(None))
    .fill_null(0)
    .alias("NOACCTX")
])

all_il = all_il.with_columns([
    (pl.col("NOACCT") + pl.col("NOACCTX")).alias("TOTACCT"),
    (pl.col("AMOUNT") + pl.col("AMOUNTX")).alias("TOTAMOUNT")
])

print("✓ IL Report data processed")
print()


# ============================================================================
# WRITE OUTPUT REPORT
# ============================================================================

print(f"Writing report to: {OUTPUT_FILE}")

with open(OUTPUT_FILE, 'w') as f:
    # PART 1: MEF Report
    write_asa_line(f, "PUBLIC ISLAMIC BANK BERHAD", '1')  # New page
    write_asa_line(f, f"PERFORMANCE REPORT ON PRODUCT 428 AND 439 AS AT {rdate}")
    write_asa_line(f, "REPORT ID : EIIMMEF2")
    write_asa_line(f, "                          ")
    write_asa_line(f, "                          ")
    write_asa_line(f, "MEF                  WITH CGC GUARANTEED          WITHOUT CGC GUARANTEED      TOTAL")
    write_asa_line(f,
                   "                     NO ACCT     AMOUNT           NO ACCT     AMOUNT           NO ACCT     AMOUNT")

    # Sort by TYPE to ensure consistent order
    type_order = {"DISBURSEMENT": 1, "REPAYMENT": 2, "OUTSTANDING": 3}
    all_cgc_sorted = all_cgc.sort(
        pl.col("TYPE").map_elements(lambda x: type_order.get(x, 4), return_dtype=pl.Int64)
    )

    for row in all_cgc_sorted.iter_rows(named=True):
        type_name = row["TYPE"].ljust(20)
        noacct = format_number(row["NOACCT"], 8, 0)
        amount = format_number(row["AMOUNT"], 15, 2)
        noacctx = format_number(row["NOACCTX"], 8, 0)
        amountx = format_number(row["AMOUNTX"], 15, 2)
        totacct = format_number(row["TOTACCT"], 8, 0)
        totamount = format_number(row["TOTAMOUNT"], 15, 2)

        line = f"{type_name}{noacct} {amount} {noacctx} {amountx} {totacct} {totamount}"
        write_asa_line(f, line)

    # PART 2: IL Report
    write_asa_line(f, "                  ", ' ')
    write_asa_line(f, "                  ", ' ')
    write_asa_line(f, "PUBLIC ISLAMIC BANK BERHAD", '1')  # New page
    write_asa_line(f, f"PERFORMANCE REPORT ON PRODUCT 428 AND 439 AS AT {rdate}")
    write_asa_line(f, "REPORT ID : EIIMMEF2 (IMPARED LOANS)")
    write_asa_line(f, "                          ")
    write_asa_line(f, "                          ")
    write_asa_line(f, "MEF (IL)                     WITH CGC GUARANTEED          WITHOUT CGC GUARANTEED      TOTAL")
    write_asa_line(f,
                   "                             NO ACCT     AMOUNT           NO ACCT     AMOUNT           NO ACCT     AMOUNT")

    # IL category labels
    il_labels = {
        "A": "< 3 MTHS",
        "B": "3 TO LESS THAN 6 MTHS",
        "C": "6 TO LESS THAN 9 MTHS",
        "D": ">= 9 MTHS"
    }

    # Initialize totals
    gacc, gamt, gaccx, gamtx, gtacc, gtamt = 0, 0.0, 0, 0.0, 0, 0.0

    # Sort by TYPX
    all_il_sorted = all_il.sort("TYPX")

    for row in all_il_sorted.iter_rows(named=True):
        typx_label = il_labels.get(row["TYPX"], "").ljust(28)
        noacct = format_number(row["NOACCT"], 8, 0)
        amount = format_number(row["AMOUNT"], 15, 2)
        noacctx = format_number(row["NOACCTX"], 8, 0)
        amountx = format_number(row["AMOUNTX"], 15, 2)
        totacct = format_number(row["TOTACCT"], 8, 0)
        totamount = format_number(row["TOTAMOUNT"], 15, 2)

        line = f"{typx_label} {noacct} {amount} {noacctx} {amountx} {totacct} {totamount}"
        write_asa_line(f, line)

        # Accumulate totals
        gacc += row["NOACCT"]
        gamt += row["AMOUNT"]
        gaccx += row["NOACCTX"]
        gamtx += row["AMOUNTX"]
        gtacc += row["TOTACCT"]
        gtamt += row["TOTAMOUNT"]

    # Write totals
    total_label = "TOTAL".ljust(28)
    total_line = f"{total_label} {format_number(gacc, 8, 0)} {format_number(gamt, 15, 2)} {format_number(gaccx, 8, 0)} {format_number(gamtx, 15, 2)} {format_number(gtacc, 8, 0)} {format_number(gtamt, 15, 2)}"
    write_asa_line(f, total_line)

print(f"✓ Report generated successfully: {OUTPUT_FILE}")
print()

# Close DuckDB connection
con.close()

print("=" * 70)
print("EIIMMEF2 Report Generation Complete!")
print("=" * 70)
