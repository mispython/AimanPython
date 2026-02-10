#!/usr/bin/env python3
"""
File Name: EIPWRDAL
RDAL PBCS Data Processing
Processes banking data and generates RDAL and NSRS output files
"""

import duckdb
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import sys


# ============================================================================
# PATH CONFIGURATION
# ============================================================================

# Input paths
INPUT_BASE_PATH = Path("./data/input")
LOAN_PATH = INPUT_BASE_PATH / "loan"
PBCS_PATH = INPUT_BASE_PATH / "pbcs"
BNM_BASE_PATH = INPUT_BASE_PATH / "bnm/d2025"

# Output paths
OUTPUT_BASE_PATH = Path("/data/output")
RDAL_OUTPUT = OUTPUT_BASE_PATH / "rdal_pbcs.txt"
NSRS_OUTPUT = OUTPUT_BASE_PATH / "nsrs_rdal_pbcs.txt"

# Ensure output directory exists
OUTPUT_BASE_PATH.mkdir(parents=True, exist_ok=True)


# ============================================================================
# DATE CALCULATIONS
# ============================================================================

def calculate_report_dates():
    """
    Calculate reporting dates based on current date
    Mimics SAS REPTDATE logic
    """
    today = datetime.now()

    # First day of current month
    first_of_month = datetime(today.year, today.month, 1)

    # Last day of previous month
    reptdate = first_of_month - timedelta(days=1)

    day = reptdate.day
    month = reptdate.month
    year = reptdate.year

    # Determine week and start day based on day of month
    if day == 8:
        sdd = 1
        wk = '1'
        wk1 = '4'
    elif day == 15:
        sdd = 9
        wk = '2'
        wk1 = '1'
    elif day == 22:
        sdd = 16
        wk = '3'
        wk1 = '2'
    else:
        sdd = 23
        wk = '4'
        wk1 = '3'

    mm = month
    if wk == '1':
        mm1 = mm - 1
        if mm1 == 0:
            mm1 = 12
    else:
        mm1 = mm

    sdate = datetime(year, mm, sdd)

    return {
        'reptdate': reptdate,
        'nowk': wk,
        'nowk1': wk1,
        'reptmon': f'{mm:02d}',
        'reptmon1': f'{mm1:02d}',
        'reptyear': str(year),
        'reptday': f'{day:02d}',
        'rdate': reptdate.strftime('%d%m%Y'),
        'fdate': reptdate.strftime('%d%m%Y'),
        'sdate': sdate.strftime('%d%m%Y'),
        'sdesc': 'PUBLIC BANK BERHAD'
    }


# Calculate dates
dates = calculate_report_dates()
print(f"Processing for date: {dates['rdate']}")
print(f"Report year: {dates['reptyear']}, Month: {dates['reptmon']}, Week: {dates['nowk']}")


# ============================================================================
# DATA LOADING
# ============================================================================

def load_pbbrdal_data():
    """Load PBBRDAL reference data with item codes and zero amounts"""
    # This would typically be loaded from a reference file
    # For now, creating a placeholder structure
    # In production, this should be loaded from the actual PBBMRDLF include file
    return pl.DataFrame({
        'ITCODE': pl.Series([], dtype=pl.Utf8),
        'AMTIND': pl.Series([], dtype=pl.Utf8),
        'AMOUNT': pl.Series([], dtype=pl.Float64)
    })


def load_alw_data(dates):
    """Load ALW data from BNM and PBCS sources"""
    reptmon = dates['reptmon']
    nowk = dates['nowk']
    reptyear = dates['reptyear']

    bnm_path = BNM_BASE_PATH / f"D{reptyear}" / f"ALW{reptmon}{nowk}.parquet"
    pbcs_path = PBCS_PATH / f"CCLW{reptmon}{nowk}.parquet"

    dfs = []

    if bnm_path.exists():
        df_bnm = pl.read_parquet(bnm_path)
        dfs.append(df_bnm)

    if pbcs_path.exists():
        df_pbcs = pl.read_parquet(pbcs_path)
        dfs.append(df_pbcs)

    if dfs:
        return pl.concat(dfs)
    else:
        return pl.DataFrame({
            'ITCODE': pl.Series([], dtype=pl.Utf8),
            'AMTIND': pl.Series([], dtype=pl.Utf8),
            'AMOUNT': pl.Series([], dtype=pl.Float64)
        })


def load_loan_data():
    """Load loan note data for CAG processing"""
    loan_path = LOAN_PATH / "LNNOTE.parquet"

    if loan_path.exists():
        return pl.read_parquet(loan_path)
    else:
        return pl.DataFrame({
            'PZIPCODE': pl.Series([], dtype=pl.Int64),
            'LOANTYPE': pl.Series([], dtype=pl.Utf8),
            'BALANCE': pl.Series([], dtype=pl.Float64)
        })


# ============================================================================
# FORMAT MAPPINGS (from PBBLNFMT)
# ============================================================================

def apply_lnprod_format(loantype):
    """Apply LNPROD format - placeholder implementation"""
    # This should match the actual PBBLNFMT definitions
    return loantype


def apply_lndenom_format(loantype):
    """Apply LNDENOM format - placeholder implementation"""
    # This should match the actual PBBLNFMT definitions
    return 'D'


# ============================================================================
# DATA PROCESSING
# ============================================================================

# Load PBBRDAL reference data
pbbrdal = load_pbbrdal_data()

# Process PBBRDAL1 - set amount indicators and zero amounts
pbbrdal1 = pbbrdal.with_columns([
    pl.when(pl.col('ITCODE').str.slice(1, 1) == '0')
    .then(pl.lit(' '))
    .otherwise(pl.lit('D'))
    .alias('AMTIND'),
    pl.lit(0.0).alias('AMOUNT')
])

# Load ALW data
alw = load_alw_data(dates)

# Merge ALW and PBBRDAL1
if len(alw) > 0 and len(pbbrdal1) > 0:
    rdal = alw.join(
        pbbrdal1,
        on=['ITCODE', 'AMTIND'],
        how='outer',
        suffix='_pbb'
    ).with_columns([
        pl.coalesce(['AMOUNT', 'AMOUNT_pbb', pl.lit(0.0)]).alias('AMOUNT')
    ]).select(['ITCODE', 'AMTIND', 'AMOUNT'])
elif len(alw) > 0:
    rdal = alw
elif len(pbbrdal1) > 0:
    rdal = pbbrdal1
else:
    rdal = pl.DataFrame({
        'ITCODE': pl.Series([], dtype=pl.Utf8),
        'AMTIND': pl.Series([], dtype=pl.Utf8),
        'AMOUNT': pl.Series([], dtype=pl.Float64)
    })

# Remove unwanted items
# rdal = rdal.filter(
#     ~(
#             (pl.col('ITCODE').str.slice(0, 5).is_between('30221', '30228')) |
#             (pl.col('ITCODE').str.slice(0, 5).is_between('30231', '30238')) |
#             (pl.col('ITCODE').str.slice(0, 5).is_between('30091', '30098')) |
#             (pl.col('ITCODE').str.slice(0, 5).is_between('40151', '40158')) |
#             (pl.col('ITCODE').str.slice(0, 5) == 'NSSTS')
#     )
# )

rdal = rdal.filter(
    ~(
        (pl.col('ITCODE').str.slice(0, 5).is_between(pl.lit('30221'), pl.lit('30228'))) |
        (pl.col('ITCODE').str.slice(0, 5).is_between(pl.lit('30231'), pl.lit('30238'))) |
        (pl.col('ITCODE').str.slice(0, 5).is_between(pl.lit('30091'), pl.lit('30098'))) |
        (pl.col('ITCODE').str.slice(0, 5).is_between(pl.lit('40151'), pl.lit('40158'))) |
        (pl.col('ITCODE').str.slice(0, 5) == 'NSSTS')
    )
)

# # Alternative - More like SAS logic
# prefix5 = pl.col('ITCODE').str.slice(0, 5)
#
# rdal = rdal.filter(
#     ~(
#         ((prefix5 >= '30221') & (prefix5 <= '30228')) |
#         ((prefix5 >= '30231') & (prefix5 <= '30238')) |
#         ((prefix5 >= '30091') & (prefix5 <= '30098')) |
#         ((prefix5 >= '40151') & (prefix5 <= '40158')) |
#         (prefix5 == 'NSSTS')
#     )
# )


# ============================================================================
# CAG PROCESSING (Loan Data)
# ============================================================================

loan_data = load_loan_data()

if len(loan_data) > 0:
    # Filter for specific zip codes
    cag_zipcodes = [2002, 2013, 3039, 3047, 800003098, 800003114,
                    800004016, 800004022, 800004029, 800040050,
                    800040053, 800050024, 800060024, 800060045,
                    800060081, 80060085]

    cag = loan_data.filter(pl.col('PZIPCODE').is_in(cag_zipcodes))

    # Apply formats and set ITCODE
    cag = cag.with_columns([
        pl.col('LOANTYPE').map_elements(apply_lnprod_format, return_dtype=pl.Utf8).alias('PRODCD'),
        pl.col('LOANTYPE').map_elements(apply_lndenom_format, return_dtype=pl.Utf8).alias('AMTIND'),
        pl.lit('7511100000000Y').alias('ITCODE')
    ])

    # Summarize by ITCODE and AMTIND
    cag_summary = cag.group_by(['ITCODE', 'AMTIND']).agg([
        pl.col('BALANCE').sum().alias('AMOUNT')
    ])

    # Combine with RDAL
    rdal = pl.concat([rdal, cag_summary])

# Remove specific item codes
rdal = rdal.filter(pl.col('ITCODE') != '4364008110000Y')

# Apply absolute value except for specific item
rdal = rdal.with_columns([
    pl.when(pl.col('ITCODE') != '3400061006120Y')
    .then(pl.col('AMOUNT').abs())
    .otherwise(pl.col('AMOUNT'))
    .alias('AMOUNT')
])


# ============================================================================
# SPLIT DATA INTO AL, OB, SP
# ============================================================================

# Filter out F and # records for initial split
rdal_filtered = rdal.filter(
    ~pl.col('ITCODE').str.slice(13, 1).is_in(['F', '#'])
)

# Split into AL, OB, SP based on conditions
al_data = rdal_filtered.filter(
    (pl.col('AMTIND') != ' ') &
    ~(pl.col('ITCODE').str.slice(0, 3) == '307') &
    ~(pl.col('ITCODE').str.slice(0, 5) == '40190') &
    ~(pl.col('ITCODE').str.slice(0, 5) == '40191') &
    ~(pl.col('ITCODE').str.slice(0, 4) == 'SSTS') &
    (pl.col('ITCODE').str.slice(0, 1) != '5') &
    ~(pl.col('ITCODE').str.slice(0, 3).is_in(['685', '785']))
)

ob_data = rdal_filtered.filter(
    (pl.col('AMTIND') != ' ') &
    (pl.col('ITCODE').str.slice(0, 1) == '5')
)

# SP data - complex conditions
sp_conditions = (
        ((pl.col('AMTIND') != ' ') & (pl.col('ITCODE').str.slice(0, 3) == '307')) |
        ((pl.col('AMTIND') != ' ') & (pl.col('ITCODE').str.slice(0, 5) == '40190')) |
        ((pl.col('AMTIND') != ' ') & (pl.col('ITCODE').str.slice(0, 5) == '40191')) |
        ((pl.col('AMTIND') != ' ') & (pl.col('ITCODE').str.slice(0, 3).is_in(['685', '785']))) |
        (pl.col('ITCODE').str.slice(1, 1) == '0')
)

sp_data = rdal_filtered.filter(sp_conditions)

# Handle SSTS special case
ssts_data = rdal_filtered.filter(
    (pl.col('AMTIND') != ' ') & (pl.col('ITCODE').str.slice(0, 4) == 'SSTS')
).with_columns([
    pl.lit('4017000000000Y').alias('ITCODE')
])

sp_data = pl.concat([sp_data, ssts_data])


# ============================================================================
# WRITE RDAL OUTPUT FILE
# ============================================================================

def write_rdal_file(al_data, ob_data, sp_data, dates, output_path):
    """Write RDAL output file with proper formatting"""

    with open(output_path, 'w') as f:
        # Write header
        phead = f"RDAL{dates['reptday']}{dates['reptmon']}{dates['reptyear']}"
        f.write(f"{phead}\n")

        # Write AL section
        f.write("AL\n")

        # Sort and aggregate AL data
        al_sorted = al_data.sort(['ITCODE', 'AMTIND'])

        # Group by ITCODE and aggregate
        al_grouped = al_sorted.group_by('ITCODE', maintain_order=True).agg([
            pl.when(pl.col('AMTIND') == 'D')
            .then(pl.col('AMOUNT'))
            .otherwise(0.0)
            .sum()
            .alias('AMOUNTD'),
            pl.when(pl.col('AMTIND') == 'I')
            .then(pl.col('AMOUNT'))
            .otherwise(0.0)
            .sum()
            .alias('AMOUNTI'),
            pl.when(pl.col('AMTIND') == 'F')
            .then(pl.col('AMOUNT'))
            .otherwise(0.0)
            .sum()
            .alias('AMOUNTF')
        ])

        for row in al_grouped.iter_rows(named=True):
            itcode = row['ITCODE']

            # Skip certain items on specific days
            proceed = True
            if dates['reptday'] in ['08', '22']:
                if itcode == '4003000000000Y' and itcode[0:2] in ['68', '78']:
                    proceed = False
            if itcode == '4966000000000F':
                proceed = False

            if proceed:
                amountd = round(row['AMOUNTD'] / 1000)
                amounti = round(row['AMOUNTI'] / 1000)
                amountf = round(row['AMOUNTF'] / 1000)
                amountd_total = amountd + amounti + amountf

                f.write(f"{itcode};{amountd_total};{amounti};{amountf}\n")

        # Write OB section
        f.write("OB\n")

        ob_sorted = ob_data.sort(['ITCODE', 'AMTIND'])

        ob_grouped = ob_sorted.group_by('ITCODE', maintain_order=True).agg([
            pl.when(pl.col('AMTIND') == 'D')
            .then(pl.col('AMOUNT'))
            .otherwise(0.0)
            .sum()
            .alias('AMOUNTD'),
            pl.when(pl.col('AMTIND') == 'I')
            .then(pl.col('AMOUNT'))
            .otherwise(0.0)
            .sum()
            .alias('AMOUNTI'),
            pl.when(pl.col('AMTIND') == 'F')
            .then(pl.col('AMOUNT'))
            .otherwise(0.0)
            .sum()
            .alias('AMOUNTF')
        ])

        for row in ob_grouped.iter_rows(named=True):
            amountd = round(row['AMOUNTD'] / 1000)
            amounti = round(row['AMOUNTI'] / 1000)
            amountf = round(row['AMOUNTF'] / 1000)
            amountd_total = amountd + amounti

            f.write(f"{row['ITCODE']};{amountd_total};{amounti};{amountf}\n")

        # Write SP section
        f.write("SP\n")

        sp_sorted = sp_data.sort('ITCODE')

        sp_grouped = sp_sorted.group_by('ITCODE', maintain_order=True).agg([
            pl.when(pl.col('AMTIND') == 'D')
            .then(pl.col('AMOUNT'))
            .otherwise(0.0)
            .sum()
            .alias('AMOUNTD'),
            pl.when(pl.col('AMTIND') == 'F')
            .then(pl.col('AMOUNT'))
            .otherwise(0.0)
            .sum()
            .alias('AMOUNTF')
        ])

        for row in sp_grouped.iter_rows(named=True):
            amountd = round(row['AMOUNTD'] / 1000)
            amountf = round(row['AMOUNTF'] / 1000)
            amountd_total = amountd + amountf

            f.write(f"{row['ITCODE']};{amountd_total};{amountf}\n")


# Write first RDAL file
write_rdal_file(al_data, ob_data, sp_data, dates, RDAL_OUTPUT)
print(f"RDAL file written to: {RDAL_OUTPUT}")


# ============================================================================
# PROCESS DATA FOR NSRS (Second Processing)
# ============================================================================

# Handle # records by converting to Y and negating amount
rdal_processed = rdal.with_columns([
    pl.when(pl.col('ITCODE').str.slice(13, 1) == '#')
    .then(
        pl.col('ITCODE').str.slice(0, 13) + 'Y'
    )
    .otherwise(pl.col('ITCODE'))
    .alias('ITCODE'),
    pl.when(pl.col('ITCODE').str.slice(13, 1) == '#')
    .then(pl.col('AMOUNT') * -1)
    .otherwise(pl.col('AMOUNT'))
    .alias('AMOUNT')
])

# Re-aggregate after the transformation
rdal_agg = rdal_processed.group_by(['ITCODE', 'AMTIND']).agg([
    pl.col('AMOUNT').sum()
])

# Re-split into AL, OB, SP for NSRS
rdal_filtered2 = rdal_agg

al_data2 = rdal_filtered2.filter(
    (pl.col('AMTIND') != ' ') &
    ~(pl.col('ITCODE').str.slice(0, 3) == '307') &
    ~(pl.col('ITCODE').str.slice(0, 5) == '40190') &
    ~(pl.col('ITCODE').str.slice(0, 5) == '40191') &
    ~(pl.col('ITCODE').str.slice(0, 4) == 'SSTS') &
    (pl.col('ITCODE').str.slice(0, 1) != '5') &
    ~(pl.col('ITCODE').str.slice(0, 3).is_in(['685', '785']))
)

ob_data2 = rdal_filtered2.filter(
    (pl.col('AMTIND') != ' ') &
    (pl.col('ITCODE').str.slice(0, 1) == '5')
)

sp_conditions2 = (
        ((pl.col('AMTIND') != ' ') & (pl.col('ITCODE').str.slice(0, 3) == '307')) |
        ((pl.col('AMTIND') != ' ') & (pl.col('ITCODE').str.slice(0, 5) == '40190')) |
        ((pl.col('AMTIND') != ' ') & (pl.col('ITCODE').str.slice(0, 5) == '40191')) |
        ((pl.col('AMTIND') != ' ') & (pl.col('ITCODE').str.slice(0, 3).is_in(['685', '785']))) |
        (pl.col('ITCODE').str.slice(1, 1) == '0')
)

sp_data2 = rdal_filtered2.filter(sp_conditions2)

ssts_data2 = rdal_filtered2.filter(
    (pl.col('AMTIND') != ' ') & (pl.col('ITCODE').str.slice(0, 4) == 'SSTS')
).with_columns([
    pl.lit('4017000000000Y').alias('ITCODE')
])

sp_data2 = pl.concat([sp_data2, ssts_data2])


# ============================================================================
# WRITE NSRS OUTPUT FILE
# ============================================================================

def write_nsrs_file(al_data, ob_data, sp_data, dates, output_path):
    """Write NSRS output file with proper formatting"""

    with open(output_path, 'w') as f:
        # Write header
        phead = f"RDAL{dates['reptday']}{dates['reptmon']}{dates['reptyear']}"
        f.write(f"{phead}\n")

        # Write AL section
        f.write("AL\n")

        al_sorted = al_data.sort(['ITCODE', 'AMTIND'])

        al_grouped = al_sorted.group_by('ITCODE', maintain_order=True).agg([
            pl.when(pl.col('AMTIND') == 'D')
            .then(pl.col('AMOUNT'))
            .otherwise(0.0)
            .sum()
            .alias('AMOUNTD'),
            pl.when(pl.col('AMTIND') == 'I')
            .then(pl.col('AMOUNT'))
            .otherwise(0.0)
            .sum()
            .alias('AMOUNTI'),
            pl.when(pl.col('AMTIND') == 'F')
            .then(pl.col('AMOUNT'))
            .otherwise(0.0)
            .sum()
            .alias('AMOUNTF')
        ])

        for row in al_grouped.iter_rows(named=True):
            itcode = row['ITCODE']

            # Skip certain items on specific days
            proceed = True
            if dates['reptday'] in ['08', '22']:
                if itcode == '4003000000000Y' and itcode[0:2] in ['68', '78']:
                    proceed = False

            if proceed:
                amountd_raw = row['AMOUNTD']
                amounti_raw = row['AMOUNTI']
                amountf_raw = row['AMOUNTF']

                # Scale down if ITCODE starts with '80'
                if itcode[0:2] == '80':
                    amountd = round(amountd_raw / 1000)
                    amounti = round(amounti_raw / 1000)
                    amountf = round(amountf_raw / 1000)
                else:
                    amountd = round(amountd_raw)
                    amounti = round(amounti_raw)
                    amountf = round(amountf_raw)

                amountd_total = amountd + amounti + amountf

                f.write(f"{itcode};{amountd_total};{amounti};{amountf}\n")

        # Write OB section
        f.write("OB\n")

        ob_sorted = ob_data.sort(['ITCODE', 'AMTIND'])

        ob_grouped = ob_sorted.group_by('ITCODE', maintain_order=True).agg([
            pl.when(pl.col('AMTIND') == 'D')
            .then(pl.col('AMOUNT'))
            .otherwise(0.0)
            .sum()
            .alias('AMOUNTD'),
            pl.when(pl.col('AMTIND') == 'I')
            .then(pl.col('AMOUNT'))
            .otherwise(0.0)
            .sum()
            .alias('AMOUNTI'),
            pl.when(pl.col('AMTIND') == 'F')
            .then(pl.col('AMOUNT'))
            .otherwise(0.0)
            .sum()
            .alias('AMOUNTF')
        ])

        for row in ob_grouped.iter_rows(named=True):
            itcode = row['ITCODE']

            amountd_raw = row['AMOUNTD']
            amounti_raw = row['AMOUNTI']
            amountf_raw = row['AMOUNTF']

            # Scale down if ITCODE starts with '80'
            if itcode[0:2] == '80':
                amountd = round(amountd_raw / 1000)
                amounti = round(amounti_raw / 1000)
                amountf = round(amountf_raw / 1000)
            else:
                amountd = round(amountd_raw)
                amounti = round(amounti_raw)
                amountf = round(amountf_raw)

            amountd_total = amountd + amounti

            f.write(f"{itcode};{amountd_total};{amounti};{amountf}\n")

        # Write SP section
        f.write("SP\n")

        sp_sorted = sp_data.sort('ITCODE')

        sp_grouped = sp_sorted.group_by('ITCODE', maintain_order=True).agg([
            pl.when(pl.col('AMTIND') == 'D')
            .then(pl.col('AMOUNT'))
            .otherwise(0.0)
            .sum()
            .alias('AMOUNTD'),
            pl.when(pl.col('AMTIND') == 'F')
            .then(pl.col('AMOUNT'))
            .otherwise(0.0)
            .sum()
            .alias('AMOUNTF')
        ])

        for row in sp_grouped.iter_rows(named=True):
            itcode = row['ITCODE']

            amountd_raw = row['AMOUNTD']
            amountf_raw = row['AMOUNTF']

            amountd_total = amountd_raw + amountf_raw

            # Scale down if ITCODE starts with '80'
            if itcode[0:2] == '80':
                amountd = round(amountd_total / 1000)
            else:
                amountd = round(amountd_total)

            amountf = round(amountf_raw)

            f.write(f"{itcode};{amountd};{amountf}\n")


# Write NSRS file
write_nsrs_file(al_data2, ob_data2, sp_data2, dates, NSRS_OUTPUT)
print(f"NSRS file written to: {NSRS_OUTPUT}")


# ============================================================================
# GENERATE FTP SCRIPT
# ============================================================================

SFTP_OUTPUT = OUTPUT_BASE_PATH / "ftp_commands.txt"

with open(SFTP_OUTPUT, 'w') as f:
    ftp_filename = f"nsrs_EAB_PBCS_{dates['fdate']}.txt"
    f.write(f"PUT //SAP.PBB.NSRS.RDAL.PBCS {ftp_filename}\n")

print(f"FTP commands written to: {SFTP_OUTPUT}")

print("\n" + "=" * 70)
print("Processing complete!")
print("=" * 70)
print(f"Output files:")
print(f"  - RDAL: {RDAL_OUTPUT}")
print(f"  - NSRS: {NSRS_OUTPUT}")
print(f"  - SFTP: {SFTP_OUTPUT}")
