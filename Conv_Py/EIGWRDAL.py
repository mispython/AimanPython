#!/usr/bin/env python3
"""
File Name: EIGWRDAL
Processes BIC codes, merges datasets, and generates RDAL and NSRS output files
"""

import polars as pl
import duckdb
from pathlib import Path
from datetime import datetime


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# INPUT_DIR = Path("input")
# OUTPUT_DIR = Path("output")

BASE_DIR = Path(__file__).resolve().parent

INPUT_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

PGM_DIR = Path("pgm")

# Create directories if they don't exist
INPUT_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# Define output file paths
RDAL_FILE = OUTPUT_DIR / "RDAL.txt"
NSRS_FILE = OUTPUT_DIR / "NSRS.txt"


# ============================================================================
# DEFINE MACRO VARIABLES (THESE WOULD TYPICALLY COME FROM ENVIRONMENT OR CONFIG)
# ============================================================================
REPTDAY = "15"  # Report day
REPTMON = "01"  # Report month
REPTYEAR = "2025"  # Report year
NOWK = "4"  # Week indicator (4 = monthly)


# ============================================================================
# INITIALIZE DUCKDB CONNECTION
# ============================================================================
conn = duckdb.connect()


# ============================================================================
# FORMAT DEFINITIONS
# ============================================================================

def load_parquet(filename):
    """Load parquet file using DuckDB"""
    filepath = INPUT_DIR / f"{filename}.parquet"
    return conn.execute(f"SELECT * FROM '{filepath}'").pl()


def get_bics():
    """
    GET_BICS macro implementation
    Loads additional BICs based on NOWK value and merges with existing data
    """
    # Load the appropriate additional BICs
    if NOWK == "4":
        # MONTHLY: Load PBBMRDLF
        pbbrdal = load_parquet("PBBMRDLF")
    else:
        # WEEKLY: Load PBBWRDLF
        pbbrdal = load_parquet("PBBWRDLF")

    # MRGBIC macro implementation
    # Additional BICs with zero amount
    pbbrdal1 = pbbrdal.with_columns([
        pl.when(pl.col("ITCODE").str.slice(1, 1) == "0")
        .then(pl.lit(" "))
        .otherwise(pl.lit("D"))
        .alias("AMTIND"),
        pl.lit(0.0).alias("AMOUNT")
    ])

    # Load existing ALW data
    alw_filename = f"ALW{REPTMON}{NOWK}"
    alw = load_parquet(alw_filename)

    # Merge datasets
    rdal = alw.join(
        pbbrdal1,
        on=["ITCODE", "AMTIND"],
        how="outer",
        suffix="_add"
    ).with_columns([
        pl.when(pl.col("AMOUNT").is_not_null() & pl.col("AMOUNT_add").is_not_null())
        .then(pl.col("AMOUNT"))
        .when(pl.col("AMOUNT").is_null() & pl.col("AMOUNT_add").is_not_null())
        .then(pl.col("AMOUNT_add"))
        .otherwise(pl.col("AMOUNT"))
        .alias("AMOUNT")
    ]).select(pl.exclude("AMOUNT_add"))

    # Remove unwanted items
    rdal = rdal.filter(
        ~(
                (pl.col("ITCODE").str.slice(0, 5) >= "30221") &
                (pl.col("ITCODE").str.slice(0, 5) <= "30228")
        ) &
        ~(
                (pl.col("ITCODE").str.slice(0, 5) >= "30231") &
                (pl.col("ITCODE").str.slice(0, 5) <= "30238")
        ) &
        ~(
                (pl.col("ITCODE").str.slice(0, 5) >= "30091") &
                (pl.col("ITCODE").str.slice(0, 5) <= "30098")
        ) &
        ~(
                (pl.col("ITCODE").str.slice(0, 5) >= "40151") &
                (pl.col("ITCODE").str.slice(0, 5) <= "40158")
        ) &
        (pl.col("ITCODE").str.slice(0, 5) != "NSSTS")
    )

    return rdal


# Process BICs
rdal = get_bics()

# Process CAG data from LNNOTE
lnnote = load_parquet("LNNOTE")

cag = lnnote.filter(
    pl.col("PZIPCODE").is_in([
        2002, 2013, 3039, 3047, 800003098, 800003114,
        800004016, 800004022, 800004029, 800040050,
        800040053, 800050024, 800060024, 800060045,
        800060081, 80060085
    ])
).with_columns([
    pl.lit("7511100000000Y").alias("ITCODE"),
    pl.lit("D").alias("AMTIND")  # Assuming LNDENOM format returns 'D' for these loan types
])

# Summarize CAG data
cag_summary = cag.group_by(["ITCODE", "AMTIND"]).agg([
    pl.col("BALANCE").sum().alias("AMOUNT")
])

# Combine RDAL and CAG
rdal = pl.concat([rdal, cag_summary])

# Remove specific ITCODE and process amounts
rdal = rdal.filter(
    pl.col("ITCODE") != "4364008110000Y"
).with_columns([
    pl.when(pl.col("ITCODE") != "3400061006120Y")
    .then(pl.col("AMOUNT").abs())
    .otherwise(pl.col("AMOUNT"))
    .alias("AMOUNT")
])

# Process records with '#' in position 14
rdal = rdal.with_columns([
    pl.when(pl.col("ITCODE").str.slice(13, 1) == "#")
    .then(pl.col("ITCODE").str.slice(0, 13) + "Y")
    .otherwise(pl.col("ITCODE"))
    .alias("ITCODE"),
    pl.when(pl.col("ITCODE").str.slice(13, 1) == "#")
    .then(pl.col("AMOUNT") * -1)
    .otherwise(pl.col("AMOUNT"))
    .alias("AMOUNT")
])

# Summarize RDAL data after transformations
rdal = rdal.group_by(["ITCODE", "AMTIND"]).agg([
    pl.col("AMOUNT").sum().alias("AMOUNT")
])


# Split into AL, OB, and SP datasets
def split_datasets(df):
    """Split data into AL, OB, and SP based on ITCODE patterns"""
    # Filter out F and # records
    df = df.filter(~pl.col("ITCODE").str.slice(13, 1).is_in(["F", "#"]))

    # Initialize output lists
    al_data = []
    ob_data = []
    sp_data = []

    for row in df.iter_rows(named=True):
        itcode = row["ITCODE"]
        amtind = row["AMTIND"]
        amount = row["AMOUNT"]

        if amtind != " ":
            itcode_prefix = itcode[:3]
            itcode_prefix5 = itcode[:5]
            itcode_prefix4 = itcode[:4]
            itcode_first = itcode[0]

            if itcode_prefix == "307":
                sp_data.append(row)
            elif itcode_prefix5 in ["40190", "40191"]:
                sp_data.append(row)
            elif itcode_prefix4 == "SSTS":
                row["ITCODE"] = "4017000000000Y"
                sp_data.append(row)
            elif itcode_first != "5":
                if itcode_prefix in ["685", "785"]:
                    sp_data.append(row)
                else:
                    al_data.append(row)
            else:
                ob_data.append(row)
        elif itcode[1] == "0":
            sp_data.append(row)

    return (
        pl.DataFrame(al_data) if al_data else pl.DataFrame(schema=df.schema),
        pl.DataFrame(ob_data) if ob_data else pl.DataFrame(schema=df.schema),
        pl.DataFrame(sp_data) if sp_data else pl.DataFrame(schema=df.schema)
    )


al, ob, sp = split_datasets(rdal)

# Sort datasets
al = al.sort(["ITCODE", "AMTIND"])
ob = ob.sort(["ITCODE", "AMTIND"])
sp = sp.sort(["ITCODE"])


def write_rdal_file():
    """Write RDAL output file"""
    phead = f"RDAL{REPTDAY}{REPTMON}{REPTYEAR}"

    with open(RDAL_FILE, 'w') as f:
        f.write(f"{phead}\n")

        # Write AL section
        f.write("AL\n")
        if len(al) > 0:
            current_itcode = None
            amountd = amounti = amountf = 0

            for row in al.iter_rows(named=True):
                itcode = row["ITCODE"]
                amtind = row["AMTIND"]
                amount = row["AMOUNT"]

                # Check PROCEED logic
                proceed = True
                if REPTDAY in ["08", "22"]:
                    if itcode == "4003000000000Y" and itcode[:2] in ["68", "78"]:
                        proceed = False
                if itcode == "4966000000000F":
                    proceed = False

                if not proceed:
                    continue

                if current_itcode is None:
                    current_itcode = itcode

                if current_itcode != itcode:
                    # Write previous ITCODE
                    total = amountd + amounti + amountf
                    f.write(f"{current_itcode};{total};{amounti};{amountf}\n")
                    amountd = amounti = amountf = 0
                    current_itcode = itcode

                amount_rounded = round(amount / 1000)
                if amtind == "D":
                    amountd += amount_rounded
                elif amtind == "I":
                    amounti += amount_rounded
                elif amtind == "F":
                    amountf += amount_rounded

            # Write last ITCODE
            if current_itcode is not None:
                total = amountd + amounti + amountf
                f.write(f"{current_itcode};{total};{amounti};{amountf}\n")

        # Write OB section
        f.write("OB\n")
        if len(ob) > 0:
            current_itcode = None
            amountd = amounti = amountf = 0

            for row in ob.iter_rows(named=True):
                itcode = row["ITCODE"]
                amtind = row["AMTIND"]
                amount = row["AMOUNT"]

                if current_itcode is None:
                    current_itcode = itcode

                if current_itcode != itcode:
                    # Write previous ITCODE
                    total = amountd + amounti + amountf
                    f.write(f"{current_itcode};{total};{amounti};{amountf}\n")
                    amountd = amounti = amountf = 0
                    current_itcode = itcode

                if amtind == "D":
                    amountd += round(amount / 1000)
                elif amtind == "I":
                    amounti += round(amount / 1000)
                elif amtind == "F":
                    amountf += round(amount / 1000)

            # Write last ITCODE
            if current_itcode is not None:
                total = amountd + amounti + amountf
                f.write(f"{current_itcode};{total};{amounti};{amountf}\n")

        # Write SP section
        f.write("SP\n")
        if len(sp) > 0:
            current_itcode = None
            amountd = amountf = 0

            for row in sp.iter_rows(named=True):
                itcode = row["ITCODE"]
                amtind = row["AMTIND"]
                amount = row["AMOUNT"]

                if current_itcode is None:
                    current_itcode = itcode

                if current_itcode != itcode:
                    # Write previous ITCODE
                    total = amountd + amountf
                    f.write(f"{current_itcode};{total};{amountf}\n")
                    amountd = amountf = 0
                    current_itcode = itcode

                amount_rounded = round(amount / 1000)
                if amtind == "D":
                    amountd += amount_rounded
                elif amtind == "F":
                    amountf += amount_rounded

            # Write last ITCODE
            if current_itcode is not None:
                total = amountd + amountf
                f.write(f"{current_itcode};{total};{amountf}\n")


def write_nsrs_file():
    """Write NSRS output file"""
    phead = f"RDAL{REPTDAY}{REPTMON}{REPTYEAR}"

    with open(NSRS_FILE, 'w') as f:
        f.write(f"{phead}\n")

        # Write AL section
        f.write("AL\n")
        if len(al) > 0:
            current_itcode = None
            amountd = amounti = amountf = 0

            for row in al.iter_rows(named=True):
                itcode = row["ITCODE"]
                amtind = row["AMTIND"]
                amount = row["AMOUNT"]

                # Check PROCEED logic
                proceed = True
                if REPTDAY in ["08", "22"]:
                    if itcode == "4003000000000Y" and itcode[:2] in ["68", "78"]:
                        proceed = False

                if not proceed:
                    continue

                if current_itcode is None:
                    current_itcode = itcode

                if current_itcode != itcode:
                    # Write previous ITCODE
                    total = amountd + amounti + amountf
                    f.write(f"{current_itcode};{total};{amounti};{amountf}\n")
                    amountd = amounti = amountf = 0
                    current_itcode = itcode

                amount_rounded = round(amount)
                if itcode[:2] == "80":
                    amount_rounded = round(amount / 1000)

                if amtind == "D":
                    amountd += amount_rounded
                elif amtind == "I":
                    amounti += amount_rounded
                elif amtind == "F":
                    amountf += amount_rounded

            # Write last ITCODE
            if current_itcode is not None:
                total = amountd + amounti + amountf
                f.write(f"{current_itcode};{total};{amounti};{amountf}\n")

        # Write OB section
        f.write("OB\n")
        if len(ob) > 0:
            current_itcode = None
            amountd = amounti = amountf = 0

            for row in ob.iter_rows(named=True):
                itcode = row["ITCODE"]
                amtind = row["AMTIND"]
                amount = row["AMOUNT"]

                if current_itcode is None:
                    current_itcode = itcode

                if current_itcode != itcode:
                    # Write previous ITCODE
                    total = amountd + amounti
                    f.write(f"{current_itcode};{total};{amounti};{amountf}\n")
                    amountd = amounti = amountf = 0
                    current_itcode = itcode

                amount_val = amount
                if itcode[:2] == "80":
                    amount_val = amount / 1000

                if amtind == "D":
                    amountd += round(amount_val)
                elif amtind == "I":
                    amounti += round(amount_val)
                elif amtind == "F":
                    amountf += round(amount_val)

            # Write last ITCODE
            if current_itcode is not None:
                total = amountd + amounti
                f.write(f"{current_itcode};{total};{amounti};{amountf}\n")

        # Write SP section
        f.write("SP\n")
        if len(sp) > 0:
            current_itcode = None
            amountd = amountf = 0

            for row in sp.iter_rows(named=True):
                itcode = row["ITCODE"]
                amtind = row["AMTIND"]
                amount = row["AMOUNT"]

                if current_itcode is None:
                    current_itcode = itcode

                if current_itcode != itcode:
                    # Write previous ITCODE
                    total = amountd + amountf
                    if current_itcode[:2] == "80":
                        total = round(total / 1000)
                    f.write(f"{current_itcode};{total};{amountf}\n")
                    amountd = amountf = 0
                    current_itcode = itcode

                amount_rounded = round(amount)
                if amtind == "D":
                    amountd += amount_rounded
                elif amtind == "F":
                    amountf += amount_rounded

            # Write last ITCODE
            if current_itcode is not None:
                total = amountd + amountf
                if current_itcode[:2] == "80":
                    total = round(total / 1000)
                f.write(f"{current_itcode};{total};{amountf}\n")


# ============================================================================
# GENERATE OUTPUT FILES
# ============================================================================
write_rdal_file()
write_nsrs_file()


# ============================================================================
# CLOSE DUCKDB CONNECTION
# ============================================================================
conn.close()

print(f"Successfully generated {RDAL_FILE}")
print(f"Successfully generated {NSRS_FILE}")
print(f"Total RDAL records processed: {len(rdal)}")
print(f"  AL records: {len(al)}")
print(f"  OB records: {len(ob)}")
print(f"  SP records: {len(sp)}")
