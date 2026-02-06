#!/usr/bin/env python3
"""
File Name: EIIPBMC2
Performance Report on PB Micro Finance
Generates reports for disbursement, repayment, outstanding, and impaired loans
"""

import duckdb
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# INPUT_DIR = Path("/mnt/user-data/uploads")
# OUTPUT_DIR = Path("/mnt/user-data/outputs")

BASE_DIR = Path(__file__).resolve().parent

INPUT_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

# Input files
LOAN_FILE = INPUT_DIR / "sap_pibb_mniln_0.parquet"
SAS_LOAN_FILE = INPUT_DIR / "sap_pibb_sasdata_loan.parquet"
DISPAY_FILE = INPUT_DIR / "sap_pibb_dispay_idispaymth.parquet"
PRVLN_FILE = INPUT_DIR / "sap_pibb_mniln_-4.parquet"
CCRIS_FILE = INPUT_DIR / "sap_pbb_ccris_credsub_credmsubac.parquet"

# Output file
OUTPUT_FILE = OUTPUT_DIR / "pbmc_rpt.txt"


# ============================================================================
# INITIALIZE DUCKDB CONNECTION
# ============================================================================
con = duckdb.connect()


# ============================================================================
# STEP 1: READ REPTDATE AND SET VARIABLES
# ============================================================================
print("Step 1: Reading report date...")

reptdate_df = pl.read_parquet(LOAN_FILE).select(['REPTDATE']).head(1)
reptdate = reptdate_df['REPTDATE'][0]

# Calculate previous month
mm1 = reptdate.month - 1
if mm1 == 0:
    mm1 = 12

NOWK = '4'
REPTMON = f"{reptdate.month:02d}"
PREVMON = f"{mm1:02d}"
REPTYEAR = f"{reptdate.year % 100:02d}"
RDATE = reptdate.strftime('%d/%m/%y')

print(f"Report Date: {reptdate} ({RDATE})")
print(f"Current Month: {REPTMON}, Previous Month: {PREVMON}")

# Update file paths with date suffix
SAS_LOAN_PREV_FILE = INPUT_DIR / f"sap_pibb_sasdata_loan{PREVMON}{NOWK}.parquet"
SAS_LOAN_CURR_FILE = INPUT_DIR / f"sap_pibb_sasdata_loan{REPTMON}{NOWK}.parquet"
DISPAY_FILE = INPUT_DIR / f"sap_pibb_dispay_idispaymth{REPTMON}.parquet"
CCRIS_FILE = INPUT_DIR / f"sap_pbb_ccris_credsub_credmsubac{REPTMON}{REPTYEAR}.parquet"


# ============================================================================
# PART 1: DISBURSEMENT, REPAYMENT, AND OUTSTANDING REPORT
# ============================================================================
print("\n=== Part 1: Disbursement/Repayment/Outstanding Report ===")


# ============================================================================
# STEP 2: PROCESS LOAN DATA
# ============================================================================
print("Step 2: Processing loan data...")

# Read previous and current month loan data
try:
    loan_prev = pl.read_parquet(SAS_LOAN_PREV_FILE).drop('BALANCE').rename({'BAL_AFT_EIR': 'BALANCE'})
except:
    loan_prev = pl.DataFrame()

loan_curr = pl.read_parquet(SAS_LOAN_CURR_FILE).drop('BALANCE').rename({'BAL_AFT_EIR': 'BALANCE'})

# Merge previous and current
if len(loan_prev) > 0:
    loanx = loan_prev.join(loan_curr, on=['ACCTNO', 'NOTENO'], how='outer_coalesce')
else:
    loanx = loan_curr

# Filter for PB Micro products
loanx = loanx.filter(
    ((pl.col('PRODUCT') == 428) & pl.col('CENSUS').is_in([428.00, 428.01, 428.02, 428.03])) |
    (pl.col('PRODUCT') == 439)
)

print(f"LOANX records: {len(loanx)}")


# ============================================================================
# STEP 3: PROCESS DISPAY DATA
# ============================================================================
print("Step 3: Processing disbursement/payment data...")

dispay_data = pl.read_parquet(DISPAY_FILE).filter(
    (pl.col('DISBURSE') > 0) | (pl.col('REPAID') > 0)
)

print(f"DISPAY records: {len(dispay_data)}")


# ============================================================================
# STEP 4: PROCESS CURRENT MONTH DATA
# ============================================================================
print("Step 4: Processing current month data...")

curmth = loan_curr.filter(
    (pl.col('PAIDIND').is_in(['P', 'C']).not_()) | (pl.col('EIR_ADJ').is_not_null())
).filter(
    ((pl.col('PRODUCT') == 428) & pl.col('CENSUS').is_in([428.00, 428.01, 428.02, 428.03])) |
    (pl.col('PRODUCT') == 439)
)

print(f"CURMTH records: {len(curmth)}")


# ============================================================================
# STEP 5: MERGE DISPAY WITH CURRENT MONTH
# ============================================================================
print("Step 5: Merging dispay with current month...")

dispx = curmth.join(dispay_data, on=['ACCTNO', 'NOTENO'], how='left')


# ============================================================================
# STEP 6: MERGE DISPAY WITH LOANX
# ============================================================================
print("Step 6: Merging dispay with loanx...")

dispay_merged = loanx.join(dispay_data, on=['ACCTNO', 'NOTENO'], how='inner')

print(f"DISPAY merged records: {len(dispay_merged)}")


# ============================================================================
# STEP 7: SPLIT INTO WITH CGC AND WITHOUT CGC
# ============================================================================
print("Step 7: Splitting WITH and WITHOUT CGC...")

# WITH CGC: CENSUS 428.00, 428.02
with_cgc = dispay_merged.filter(
    (pl.col('PRODUCT') == 428) & pl.col('CENSUS').is_in([428.00, 428.02])
)

# WITHOUT CGC: CENSUS 428.01, 428.03 or PRODUCT 439
without_cgc = dispay_merged.filter(
    ((pl.col('PRODUCT') == 428) & pl.col('CENSUS').is_in([428.01, 428.03])) |
    (pl.col('PRODUCT') == 439)
)

print(f"WITH CGC: {len(with_cgc)}, WITHOUT CGC: {len(without_cgc)}")


# ============================================================================
# STEP 8: PROCESS WITH CGC DISBURSEMENT/REPAYMENT
# ============================================================================
print("Step 8: Processing WITH CGC data...")

with_dis_list = []

# Disbursement records
with_disbursement = with_cgc.filter(pl.col('DISBURSE').is_not_null() & (pl.col('DISBURSE') != 0))
if len(with_disbursement) > 0:
    with_dis_list.append(
        with_disbursement.with_columns([
            pl.lit('DISBURSEMENT').alias('TYPE'),
            pl.lit(1).alias('NOACC1'),
            pl.col('DISBURSE').alias('NODIS')
        ])
    )

# Repayment records
with_repayment = with_cgc.filter(pl.col('REPAID').is_not_null() & (pl.col('REPAID') != 0))
if len(with_repayment) > 0:
    with_dis_list.append(
        with_repayment.with_columns([
            pl.lit('REPAYMENT').alias('TYPE'),
            pl.lit(1).alias('NOACC2'),
            pl.col('REPAID').alias('NOREP')
        ])
    )

if with_dis_list:
    with_dis = pl.concat(with_dis_list)
else:
    with_dis = pl.DataFrame({'TYPE': [], 'DISBURSE': [], 'REPAID': [], 'NODIS': [], 'NOREP': []})


# ============================================================================
# STEP 9: PROCESS WITH CGC BALANCE
# ============================================================================
print("Step 9: Processing WITH CGC balance...")

with_bal = curmth.filter(
    (pl.col('PRODUCT') == 428) & pl.col('CENSUS').is_in([428.00, 428.02]) &
    (pl.col('BALANCE').is_not_null()) & (pl.col('BALANCE') != 0)
).with_columns([
    pl.lit('OUTSTANDING').alias('TYPE'),
    pl.lit(1).alias('NOBAL')
])


# ============================================================================
# STEP 10: COMBINE AND SUMMARIZE WITH CGC
# ============================================================================
print("Step 10: Summarizing WITH CGC data...")

withnew = pl.concat([with_dis, with_bal]) if len(with_dis) > 0 else with_bal

withf = withnew.group_by('TYPE').agg([
    pl.col('REPAID').sum().alias('REPAID'),
    pl.col('NOREP').sum().alias('NOREP'),
    pl.col('DISBURSE').sum().alias('DISBURSE'),
    pl.col('NODIS').sum().alias('NODIS'),
    pl.col('BALANCE').sum().alias('BALANCE'),
    pl.col('NOBAL').sum().alias('NOBAL')
]).fill_null(0)


# ============================================================================
# STEP 11: PROCESS WITHOUT CGC
# ============================================================================
print("Step 11: Processing WITHOUT CGC data...")

withx_dis_list = []

# Disbursement records
withx_disbursement = without_cgc.filter(pl.col('DISBURSE').is_not_null() & (pl.col('DISBURSE') != 0))
if len(withx_disbursement) > 0:
    withx_dis_list.append(
        withx_disbursement.with_columns([
            pl.lit('DISBURSEMENT').alias('TYPE'),
            pl.lit(1).alias('NOACC1'),
            pl.col('DISBURSE').alias('DISBURSEX'),
            pl.col('DISBURSE').alias('NODISX')
        ])
    )

# Repayment records
withx_repayment = without_cgc.filter(pl.col('REPAID').is_not_null() & (pl.col('REPAID') != 0))
if len(withx_repayment) > 0:
    withx_dis_list.append(
        withx_repayment.with_columns([
            pl.lit('REPAYMENT').alias('TYPE'),
            pl.lit(1).alias('NOACC2'),
            pl.col('REPAID').alias('REPAIDX'),
            pl.col('REPAID').alias('NOREPX')
        ])
    )

if withx_dis_list:
    withx_dis = pl.concat(withx_dis_list)
else:
    withx_dis = pl.DataFrame({'TYPE': [], 'DISBURSEX': [], 'REPAIDX': [], 'NODISX': [], 'NOREPX': []})

# Balance
withx_bal = curmth.filter(
    (((pl.col('PRODUCT') == 428) & pl.col('CENSUS').is_in([428.01, 428.03])) |
     (pl.col('PRODUCT') == 439)) &
    (pl.col('BALANCE').is_not_null()) & (pl.col('BALANCE') != 0)
).with_columns([
    pl.lit('OUTSTANDING').alias('TYPE'),
    pl.col('BALANCE').alias('BALANCEX'),
    pl.lit(1).alias('NOBALX')
])

# Combine and summarize
withxnew = pl.concat([withx_dis, withx_bal]) if len(withx_dis) > 0 else withx_bal

withxf = withxnew.group_by('TYPE').agg([
    pl.col('REPAIDX').sum().alias('REPAIDX'),
    pl.col('NOREPX').sum().alias('NOREPX'),
    pl.col('DISBURSEX').sum().alias('DISBURSEX'),
    pl.col('NODISX').sum().alias('NODISX'),
    pl.col('BALANCEX').sum().alias('BALANCEX'),
    pl.col('NOBALX').sum().alias('NOBALX')
]).fill_null(0)


# ============================================================================
# STEP 12: MERGE AND CREATE FINAL CGC DATA
# ============================================================================
print("Step 12: Creating final CGC data...")

cgc = withf.join(withxf, on='TYPE', how='outer_coalesce').with_columns([
    pl.lit('X').alias('GROUP')
]).fill_null(0)

# Calculate totals for disbursement
groupx = cgc.group_by('GROUP').agg([
    pl.col('DISBURSE').sum().alias('DISBURSE'),
    pl.col('NODIS').sum().alias('NODIS'),
    pl.col('DISBURSEX').sum().alias('DISBURSEX'),
    pl.col('NODISX').sum().alias('NODISX')
]).with_columns([
    pl.lit('DISBURSEMENT').alias('TYPE')
])

# Merge
all_cgc = cgc.join(groupx, on='TYPE', how='left').fill_null(0)


# ============================================================================
# STEP 13: FORMAT CGC OUTPUT DATA
# ============================================================================
print("Step 13: Formatting CGC output...")


def format_cgc_row(row):
    """Format CGC data for output"""
    type_val = row['TYPE']

    if type_val == 'REPAYMENT':
        noacct = row.get('NOREP', 0) or 0
        amount = row.get('REPAID', 0) or 0
        noacctx = row.get('NOREPX', 0) or 0
        amountx = row.get('REPAIDX', 0) or 0
    elif type_val == 'DISBURSEMENT':
        noacct = row.get('NODIS', 0) or 0
        amount = row.get('DISBURSE', 0) or 0
        noacctx = row.get('NODISX', 0) or 0
        amountx = row.get('DISBURSEX', 0) or 0
    elif type_val == 'OUTSTANDING':
        noacct = row.get('NOBAL', 0) or 0
        amount = row.get('BALANCE', 0) or 0
        noacctx = row.get('NOBALX', 0) or 0
        amountx = row.get('BALANCEX', 0) or 0
    else:
        noacct = amount = noacctx = amountx = 0

    totacct = noacct + noacctx
    totamount = amount + amountx

    return {
        'TYPE': type_val,
        'NOACCT': int(noacct),
        'AMOUNT': float(amount),
        'NOACCTX': int(noacctx),
        'AMOUNTX': float(amountx),
        'TOTACCT': int(totacct),
        'TOTAMOUNT': float(totamount)
    }


all_cgc_formatted = pl.DataFrame([
    format_cgc_row(row) for row in all_cgc.iter_rows(named=True)
])


# ============================================================================
# PART 2: IMPAIRED LOANS REPORT
# ============================================================================
print("\n=== Part 2: Impaired Loans Report ===")


# ============================================================================
# STEP 14: PROCESS LOAN DATA FOR IL
# ============================================================================
print("Step 14: Processing loan data for impaired loans...")

loan_il = pl.read_parquet(SAS_LOAN_CURR_FILE).filter(
    (pl.col('PAIDIND').is_in(['P', 'C']).not_()) | (pl.col('EIR_ADJ').is_not_null())
).drop('BALANCE').rename({'BAL_AFT_EIR': 'BALANCE'})


# ============================================================================
# STEP 15: MERGE WITH CCRIS DATA
# ============================================================================
print("Step 15: Merging with CCRIS data...")

credmsub = pl.read_parquet(CCRIS_FILE).select([
    'ACCTNUM', 'NOTENO', 'BRANCH', 'DAYSARR', 'MTHARR'
]).rename({'ACCTNUM': 'ACCTNO', 'DAYSARR': 'DAYARR'})

loan_il = loan_il.join(credmsub, on=['ACCTNO', 'NOTENO', 'BRANCH'], how='left')


# ============================================================================
# STEP 16: PROCESS MEF (WITH CGC) IMPAIRED LOANS
# ============================================================================
print("Step 16: Processing MEF (WITH CGC) impaired loans...")

mef = loan_il.filter(
    (pl.col('BALANCE').is_not_null()) & (pl.col('BALANCE') != 0) &
    (pl.col('CENSUS').is_in([428.00, 428.02])) &
    (pl.col('LOANSTAT') == 3)
)


def assign_il_category(mtharr):
    """Assign impaired loan category based on months in arrears"""
    if mtharr is None:
        return 'D'
    if mtharr < 3:
        return 'A'
    elif mtharr < 6:
        return 'B'
    elif mtharr < 9:
        return 'C'
    else:
        return 'D'


mef = mef.with_columns([
    pl.struct(['MTHARR']).map_elements(
        lambda x: assign_il_category(x['MTHARR']),
        return_dtype=pl.Utf8
    ).alias('TYPX')
])

# Add count columns
mef = mef.with_columns([
    pl.when(pl.col('TYPX') == 'A').then(1).otherwise(0).alias('NO1'),
    pl.when(pl.col('TYPX') == 'B').then(1).otherwise(0).alias('NO2'),
    pl.when(pl.col('TYPX') == 'C').then(1).otherwise(0).alias('NO3'),
    pl.when(pl.col('TYPX') == 'D').then(1).otherwise(0).alias('NO4')
])

# Summarize
mefx = mef.group_by('TYPX').agg([
    pl.col('BALANCE').sum().alias('BALANCE'),
    pl.col('NO1').sum().alias('NO1'),
    pl.col('NO2').sum().alias('NO2'),
    pl.col('NO3').sum().alias('NO3'),
    pl.col('NO4').sum().alias('NO4')
])


# ============================================================================
# STEP 17: PROCESS MEFXX (WITHOUT CGC) IMPAIRED LOANS
# ============================================================================
print("Step 17: Processing MEFXX (WITHOUT CGC) impaired loans...")

mefxx = loan_il.filter(
    (((pl.col('CENSUS').is_in([428.01, 428.03])) | (pl.col('PRODUCT') == 439)) &
     (pl.col('LOANSTAT') == 3)) &
    (pl.col('BALANCE').is_not_null()) & (pl.col('BALANCE') != 0)
).with_columns([
    pl.col('BALANCE').alias('BALANCEX')
])

mefxx = mefxx.with_columns([
    pl.struct(['MTHARR']).map_elements(
        lambda x: assign_il_category(x['MTHARR']),
        return_dtype=pl.Utf8
    ).alias('TYPX')
])

# Add count columns
mefxx = mefxx.with_columns([
    pl.when(pl.col('TYPX') == 'A').then(1).otherwise(0).alias('NO1X'),
    pl.when(pl.col('TYPX') == 'B').then(1).otherwise(0).alias('NO2X'),
    pl.when(pl.col('TYPX') == 'C').then(1).otherwise(0).alias('NO3X'),
    pl.when(pl.col('TYPX') == 'D').then(1).otherwise(0).alias('NO4X')
])

# Summarize
mefxx_sum = mefxx.group_by('TYPX').agg([
    pl.col('BALANCEX').sum().alias('BALANCEX'),
    pl.col('NO1X').sum().alias('NO1X'),
    pl.col('NO2X').sum().alias('NO2X'),
    pl.col('NO3X').sum().alias('NO3X'),
    pl.col('NO4X').sum().alias('NO4X')
])


# ============================================================================
# STEP 18: MERGE AND FORMAT IL DATA
# ============================================================================
print("Step 18: Merging and formatting IL data...")

all_il = mefx.join(mefxx_sum, on='TYPX', how='outer_coalesce').fill_null(0)


def format_il_row(row):
    """Format IL data for output"""
    typx = row['TYPX']

    if typx == 'A':
        noacct = row.get('NO1', 0) or 0
        amount = row.get('BALANCE', 0) or 0
        noacctx = row.get('NO1X', 0) or 0
        amountx = row.get('BALANCEX', 0) or 0
    elif typx == 'B':
        noacct = row.get('NO2', 0) or 0
        amount = row.get('BALANCE', 0) or 0
        noacctx = row.get('NO2X', 0) or 0
        amountx = row.get('BALANCEX', 0) or 0
    elif typx == 'C':
        noacct = row.get('NO3', 0) or 0
        amount = row.get('BALANCE', 0) or 0
        noacctx = row.get('NO3X', 0) or 0
        amountx = row.get('BALANCEX', 0) or 0
    elif typx == 'D':
        noacct = row.get('NO4', 0) or 0
        amount = row.get('BALANCE', 0) or 0
        noacctx = row.get('NO4X', 0) or 0
        amountx = row.get('BALANCEX', 0) or 0
    else:
        noacct = amount = noacctx = amountx = 0

    totacct = noacct + noacctx
    totamount = amount + amountx

    return {
        'TYPX': typx,
        'NOACCT': int(noacct),
        'AMOUNT': float(amount),
        'NOACCTX': int(noacctx),
        'AMOUNTX': float(amountx),
        'TOTACCT': int(totacct),
        'TOTAMOUNT': float(totamount)
    }


all_il_formatted = pl.DataFrame([
    format_il_row(row) for row in all_il.iter_rows(named=True)
])

# Sort by TYPX
all_il_formatted = all_il_formatted.sort('TYPX')


# ============================================================================
# STEP 19: WRITE OUTPUT REPORT
# ============================================================================
print("Step 19: Writing output report...")

ILCAT_FORMAT = {
    'A': '< 3 MTHS',
    'B': '3 TO LESS THAN 6 MTHS',
    'C': '6 TO LESS THAN 9 MTHS',
    'D': '>= 9 MTHS'
}

with open(OUTPUT_FILE, 'w') as f:
    # Write Part 1: CGC Report
    f.write('PUBLIC ISLAMIC BANK BERHAD\n')
    f.write(f'PERFORMANCE REPORT ON PB MICRO FINANCE AS AT {RDATE}\n')
    f.write('REPORT ID : EIIPBMC2\n')
    f.write('                          \n')
    f.write('                          \n')
    f.write(f"{'PB MICRO':<20}{'WITH CGC GUARANTEED':<30}{'WITHOUT CGC GUARANTEED':<30}{'TOTAL':<20}\n")
    f.write(f"{' ' * 20}{'NO ACCT':<10}{'AMOUNT':<20}{'NO ACCT':<10}{'AMOUNT':<20}{'NO ACCT':<10}{'AMOUNT':<20}\n")

    for row in all_cgc_formatted.iter_rows(named=True):
        type_val = row['TYPE']
        noacct = row['NOACCT']
        amount = row['AMOUNT']
        noacctx = row['NOACCTX']
        amountx = row['AMOUNTX']
        totacct = row['TOTACCT']
        totamount = row['TOTAMOUNT']

        f.write(
            f"{type_val:<20}{noacct:<10}{amount:<20.2f}{noacctx:<10}{amountx:<20.2f}{totacct:<10}{totamount:<20.2f}\n")

    # Write Part 2: IL Report
    f.write('                  \n')
    f.write('                  \n')
    f.write('PUBLIC ISLAMIC BANK BERHAD\n')
    f.write(f'PERFORMANCE REPORT ON PB MICRO FINANCE AS AT {RDATE}\n')
    f.write('REPORT ID : EIIPBMC2 (IMPARED LOANS)\n')
    f.write('                          \n')
    f.write('                          \n')
    f.write(f"{'PB MICRO (IL)':<30}{'WITH CGC GUARANTEED':<30}{'WITHOUT CGC GUARANTEED':<30}{'TOTAL':<30}\n")
    f.write(f"{' ' * 30}{'NO ACCT':<10}{'AMOUNT':<20}{'NO ACCT':<10}{'AMOUNT':<20}{'NO ACCT':<10}{'AMOUNT':<20}\n")

    # Track totals
    gacc = gamt = gaccx = gamtx = gtacc = gtamt = 0

    for row in all_il_formatted.iter_rows(named=True):
        typx = row['TYPX']
        typx_label = ILCAT_FORMAT.get(typx, '')
        noacct = row['NOACCT']
        amount = row['AMOUNT']
        noacctx = row['NOACCTX']
        amountx = row['AMOUNTX']
        totacct = row['TOTACCT']
        totamount = row['TOTAMOUNT']

        f.write(
            f"{typx_label:<30}{noacct:<10}{amount:<20.2f}{noacctx:<10}{amountx:<20.2f}{totacct:<10}{totamount:<20.2f}\n")

        gacc += noacct
        gamt += amount
        gaccx += noacctx
        gamtx += amountx
        gtacc += totacct
        gtamt += totamount

    # Write totals
    f.write(f"{'TOTAL':<30}{gacc:<10}{gamt:<20.2f}{gaccx:<10}{gamtx:<20.2f}{gtacc:<10}{gtamt:<20.2f}\n")

print(f"\nOutput written to: {OUTPUT_FILE}")
print(f"CGC records: {len(all_cgc_formatted)}")
print(f"IL records: {len(all_il_formatted)}")
print("\nProcessing complete!")

# Close DuckDB connection
con.close()
