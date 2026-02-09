#!/usr/bin/env python3
"""
EIIMPORT Report Generation
Islamic Banking Portfolio Exposure Report
Reports investment, deposit, derivatives and sukuk holdings by Shariah concept
"""

import duckdb
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import calendar


# ============================================================================
# PATH SETUP
# ============================================================================
BASE_PATH = Path("/data")
INPUT_PATH = BASE_PATH / "input"
OUTPUT_PATH = BASE_PATH / "output"

# Input files
REPTDATE_FILE = INPUT_PATH / "reptdate.parquet"
SAVING_FILE = INPUT_PATH / "saving.parquet"
CURRENT_FILE = INPUT_PATH / "current.parquet"
FD_FILE = INPUT_PATH / "fd.parquet"
EQUT_FILE_PATTERN = INPUT_PATH / "iutfx{year}{month}{day}.parquet"
W1AL_FILE_PATTERN = INPUT_PATH / "w1al{month}{week}.parquet"

# Output file
OUTPUT_FILE = OUTPUT_PATH / "BANK_PORTF.txt"

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

# Determine week based on day of month
day = reptdate.day
if 1 <= day <= 8:
    nowk = '1'
    # Set to last day of previous month
    reptdate1 = reptdate.replace(day=1) - timedelta(days=1)
elif 9 <= day <= 15:
    nowk = '2'
elif 16 <= day <= 22:
    nowk = '3'
else:
    nowk = '4'

rdate = reptdate.strftime("%d/%m/%Y")
reptday = f"{reptdate.day:02d}"
reptmon = f"{reptdate.month:02d}"
reptyear1 = reptdate.strftime("%y")
reptyear = reptdate.strftime("%Y")
reptdate_int = int(reptdate.strftime("%y%j"))  # Z5 format: YYDDD

print(f"Report Date: {rdate}")
print(f"Week: {nowk}")
print(f"REPTDATE (Z5): {reptdate_int}")
print()


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def write_asa_line(f, line, carriage_control=' '):
    """Write line with ASA carriage control character"""
    f.write(f"{carriage_control}{line}\n")


def format_amount(value):
    """Format amount with 2 decimal places"""
    if value is None:
        return "0.00"
    return f"{value:.2f}"


# ============================================================================
# PROCESS SAVINGS ACCOUNTS (WADIAH)
# ============================================================================

print("Processing Savings Accounts...")

query_sa = f"""
    SELECT *
    FROM read_parquet('{SAVING_FILE}')
    WHERE PRODUCT IN (204, 207, 214, 215)
    AND OPENIND NOT IN ('B', 'C', 'P')
    AND CURBAL >= 0
"""
sa = con.execute(query_sa).pl()

sa = sa.with_columns([
    pl.lit(reptdate_int).alias("REPTDATE"),
    pl.lit("WADIAH").alias("CONCEPT1"),
    pl.col("CURBAL").alias("AMOUNT1")
])

# Aggregate savings
sum_sa = sa.group_by(["REPTDATE", "CONCEPT1"]).agg([
    pl.col("AMOUNT1").sum().alias("AMOUNT1"),
    pl.col("INTPAYBL").sum().alias("INTPAYBL")
])

# Calculate total
test1 = sum_sa.select([
    pl.col("REPTDATE"),
    pl.col("AMOUNT1").alias("TOTAL1")
])

# Create base structure
base_sa = pl.DataFrame({
    "CONCEPT1": ["WADIAH"],
    "AMOUNT1": [sum_sa.select(pl.col("AMOUNT1").sum()).item(0, 0) if len(sum_sa) > 0 else 0.0],
    "TOTAL1": [test1.select(pl.col("TOTAL1").sum()).item(0, 0) if len(test1) > 0 else 0.0]
})

print(f"✓ Processed {len(sa)} savings accounts")


# ============================================================================
# PROCESS CURRENT ACCOUNTS (MULTIPLE CONCEPTS)
# ============================================================================

print("Processing Current Accounts...")

current_products = [32, 33, 60, 61, 62, 63, 64, 66, 67, 70, 71, 73, 93, 94, 95, 96, 97,
                    160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 182, 183, 184,
                    185, 186, 440, 441, 442, 443, 444]

query_ca = f"""
    SELECT *
    FROM read_parquet('{CURRENT_FILE}')
    WHERE PRODUCT IN ({','.join(map(str, current_products))})
    AND OPENIND NOT IN ('B', 'C', 'P')
    AND CURBAL >= 0
"""
ca = con.execute(query_ca).pl()

# Assign concepts based on product
ca = ca.with_columns([
    pl.lit(reptdate_int).alias("REPTDATE"),
    pl.when(pl.col("PRODUCT").is_in([60, 61, 62, 63, 64, 66, 67, 93, 96, 97, 160, 161, 162, 163, 164, 165, 182]))
    .then(pl.lit("WADIAH"))
    .when(pl.col("PRODUCT").is_in([32, 70, 73, 94, 95, 166, 183, 184, 185, 187, 188]))
    .then(pl.lit("BAI BITHAMAN AJIL"))
    .when(pl.col("PRODUCT").is_in([169]))
    .then(pl.lit("MURABAHAH"))
    .when(pl.col("PRODUCT").is_in([33, 71, 167, 168]))
    .then(pl.lit("BAI AL INAH"))
    .when(pl.col("PRODUCT").is_in([440, 441, 442, 443, 444]))
    .then(pl.lit("QARD"))
    .when(pl.col("PRODUCT").is_in([186]))
    .then(pl.lit("MUSYARAKAH"))
    .otherwise(pl.lit(None))
    .alias("CONCEPT2"),
    pl.col("CURBAL").alias("AMOUNT2")
])

# Filter out null concepts
ca = ca.filter(pl.col("CONCEPT2").is_not_null())

# Aggregate current accounts
sum_ca = ca.group_by(["REPTDATE", "CONCEPT2"]).agg([
    pl.col("AMOUNT2").sum().alias("AMOUNT2"),
    pl.col("INTPAYBL").sum().alias("INTPAYBL")
])

# Calculate total
test2 = sum_ca.select([
    pl.col("REPTDATE"),
    pl.col("AMOUNT2").sum().alias("TOTAL2")
])

total2 = test2.select(pl.col("TOTAL2").sum()).item(0, 0) if len(test2) > 0 else 0.0

# Create base structure with all concepts
ca_concepts = ["WADIAH", "BAI BITHAMAN AJIL", "BAI AL INAH", "MURABAHAH", "QARD", "MUSYARAKAH"]
base2_data = []
for concept in ca_concepts:
    concept_data = sum_ca.filter(pl.col("CONCEPT2") == concept)
    amount = concept_data.select(pl.col("AMOUNT2").sum()).item(0, 0) if len(concept_data) > 0 else 0.0
    base2_data.append({
        "CONCEPT2": concept,
        "AMOUNT2": amount,
        "TOTAL2": total2
    })

base_ca = pl.DataFrame(base2_data)

print(f"✓ Processed {len(ca)} current accounts")


# ============================================================================
# PROCESS FIXED DEPOSITS - MGIA (MUDARABAH & ISTISMAR)
# ============================================================================

print("Processing Fixed Deposits (MGIA)...")

query_mgia = f"""
    SELECT *
    FROM read_parquet('{FD_FILE}')
    WHERE PRODUCT IN (302, 396, 315, 394)
    AND OPENIND NOT IN ('B', 'C', 'P')
    AND CURBAL >= 0
"""
mgia = con.execute(query_mgia).pl()

mgia = mgia.with_columns([
    pl.lit(reptdate_int).alias("REPTDATE"),
    pl.when(pl.col("PRODUCT").is_in([302, 396]))
    .then(pl.lit("MUDARABAH"))
    .when(pl.col("PRODUCT").is_in([315, 394]))
    .then(pl.lit("ISTISMAR"))
    .otherwise(pl.lit(None))
    .alias("CONCEPT3"),
    pl.col("CURBAL").alias("AMOUNT3")
])

mgia = mgia.filter(pl.col("CONCEPT3").is_not_null())

# Aggregate
sum_mgia = mgia.group_by(["REPTDATE", "CONCEPT3"]).agg([
    pl.col("AMOUNT3").sum().alias("AMOUNT3"),
    pl.col("INTPAYBL").sum().alias("INTPAYBL")
])

# Calculate total
test3 = sum_mgia.select([
    pl.col("REPTDATE"),
    pl.col("AMOUNT3").sum().alias("TOTAL3")
])

total3 = test3.select(pl.col("TOTAL3").sum()).item(0, 0) if len(test3) > 0 else 0.0

# Create base structure
mgia_concepts = ["MUDARABAH", "ISTISMAR"]
base3_data = []
for concept in mgia_concepts:
    concept_data = sum_mgia.filter(pl.col("CONCEPT3") == concept)
    amount = concept_data.select(pl.col("AMOUNT3").sum()).item(0, 0) if len(concept_data) > 0 else 0.0
    base3_data.append({
        "CONCEPT3": concept,
        "AMOUNT3": amount,
        "TOTAL3": total3
    })

base_mgia = pl.DataFrame(base3_data)

print(f"✓ Processed {len(mgia)} MGIA deposits")


# ============================================================================
# PROCESS COMMODITY MURABAHAH
# ============================================================================

print("Processing Commodity Murabahah...")

query_comm = f"""
    SELECT *
    FROM read_parquet('{FD_FILE}')
    WHERE PRODUCT IN (316, 393)
    AND OPENIND NOT IN ('B', 'C', 'P')
    AND CURBAL >= 0
"""
comm = con.execute(query_comm).pl()

comm = comm.with_columns([
    pl.lit(reptdate_int).alias("REPTDATE"),
    pl.lit("COMMODITY MURABAHAH TAWWARRUQ").alias("CONCEPT4"),
    pl.col("CURBAL").alias("AMOUNT4")
])

# Aggregate
sum_comm = comm.group_by(["REPTDATE", "CONCEPT4"]).agg([
    pl.col("AMOUNT4").sum().alias("AMOUNT4"),
    pl.col("INTPAYBL").sum().alias("INTPAYBL")
])

# Calculate total
test4 = sum_comm.select([
    pl.col("REPTDATE"),
    pl.col("AMOUNT4").sum().alias("TOTAL4")
])

total4 = test4.select(pl.col("TOTAL4").sum()).item(0, 0) if len(test4) > 0 else 0.0

# Create base structure
base_comm = pl.DataFrame({
    "CONCEPT4": ["COMMODITY MURABAHAH TAWARRUQ"],
    "AMOUNT4": [total4],
    "TOTAL4": [total4]
})

print(f"✓ Processed {len(comm)} Commodity Murabahah deposits")


# ============================================================================
# PROCESS EQUITY/DERIVATIVES (EQUT)
# ============================================================================

print("Processing Equity/Derivatives...")

equt_file = INPUT_PATH / f"iutfx{reptyear1}{reptmon}{reptday}.parquet"

if equt_file.exists():
    query_eqt = f"""
        SELECT *
        FROM read_parquet('{equt_file}')
    """
    eqt = con.execute(query_eqt).pl()

    eqt = eqt.with_columns([
        pl.lit(reptdate_int).alias("REPTDATE"),
        pl.when(pl.col("DEALTYPE") == "BCS")
        .then(pl.lit("ISTISMAR"))
        .when(pl.col("DEALTYPE") == "BCI")
        .then(pl.lit("MUDARABAH"))
        .when(pl.col("DEALTYPE") == "BCT")
        .then(pl.lit("COMMODITY MURABAHAH TAWARRUQ"))
        .when(pl.col("DEALTYPE") == "BCW")
        .then(pl.lit("WADIAH"))
        .otherwise(pl.lit(None))
        .alias("CONCEPT5"),
        pl.col("AMTPAY").alias("AMOUNT5")
    ])

    eqt = eqt.filter(pl.col("CONCEPT5").is_not_null())

    # Aggregate
    sum_eqt = eqt.group_by(["REPTDATE", "CONCEPT5"]).agg([
        pl.col("AMOUNT5").sum().alias("AMOUNT5")
    ])

    # Calculate total
    test5 = sum_eqt.select([
        pl.col("REPTDATE"),
        pl.col("AMOUNT5").sum().alias("TOTAL5")
    ])

    total5 = test5.select(pl.col("TOTAL5").sum()).item(0, 0) if len(test5) > 0 else 0.0

    # Create base structure
    eqt_concepts = ["MUDARABAH", "ISTISMAR", "WADIAH", "COMMODITY MURABAHAH TAWARRUQ"]
    base5_data = []
    for concept in eqt_concepts:
        concept_data = sum_eqt.filter(pl.col("CONCEPT5") == concept)
        amount = concept_data.select(pl.col("AMOUNT5").sum()).item(0, 0) if len(concept_data) > 0 else 0.0
        base5_data.append({
            "CONCEPT5": concept,
            "AMOUNT5": amount,
            "TOTAL5": total5
        })

    base_eqt = pl.DataFrame(base5_data)

    print(f"✓ Processed {len(eqt)} equity/derivative transactions")
else:
    print(f"⚠ Warning: Equity file not found: {equt_file}")
    total5 = 0.0
    base_eqt = pl.DataFrame({
        "CONCEPT5": ["MUDARABAH", "ISTISMAR", "WADIAH", "COMMODITY MURABAHAH TAWARRUQ"],
        "AMOUNT5": [0.0, 0.0, 0.0, 0.0],
        "TOTAL5": [0.0, 0.0, 0.0, 0.0]
    })


# ============================================================================
# PROCESS INI (BAI AL INAH)
# ============================================================================

print("Processing INI (Bai Al Inah)...")

w1al_file = INPUT_PATH / f"w1al{reptmon}{nowk}.parquet"

if w1al_file.exists():
    query_ini = f"""
        SELECT *
        FROM read_parquet('{w1al_file}')
        WHERE SET_ID = 'F142150NIDI'
    """
    ini = con.execute(query_ini).pl()

    ini = ini.with_columns([
        pl.lit(reptdate_int).alias("REPTDATE"),
        pl.lit("BAI AL INAH").alias("CONCEPT6"),
        pl.col("AMOUNT").alias("AMOUNT6")
    ])

    # Aggregate
    sum_ini = ini.group_by(["REPTDATE", "CONCEPT6"]).agg([
        pl.col("AMOUNT6").sum().alias("AMOUNT6")
    ])

    # Calculate total
    test6 = sum_ini.select([
        pl.col("REPTDATE"),
        pl.col("AMOUNT6").sum().alias("TOTAL6")
    ])

    total6 = test6.select(pl.col("TOTAL6").sum()).item(0, 0) if len(test6) > 0 else 0.0

    base_ini = pl.DataFrame({
        "CONCEPT6": ["BAI AL INAH"],
        "AMOUNT6": [total6],
        "TOTAL6": [total6]
    })

    print(f"✓ Processed {len(ini)} INI transactions")
else:
    print(f"⚠ Warning: W1AL file not found: {w1al_file}")
    total6 = 0.0
    base_ini = pl.DataFrame({
        "CONCEPT6": ["BAI AL INAH"],
        "AMOUNT6": [0.0],
        "TOTAL6": [0.0]
    })


# ============================================================================
# CALCULATE TERM TOTAL
# ============================================================================

print("Calculating term total...")

# Sum of all term deposits
high_term = total3 + total4 + total5 + total6

print(f"Term Total: {high_term:,.2f}")


# ============================================================================
# BUILD PORTFOLIO EXPOSURE DATA
# ============================================================================

print("Building portfolio exposure report...")

# Collect all detail records
portex_records = []

# From Savings
for row in base_sa.iter_rows(named=True):
    portex_records.append({
        "ID": 1.1,
        "CONCEPT": row["CONCEPT1"],
        "AMT": row["AMOUNT1"]
    })

# From Current
for row in base_ca.iter_rows(named=True):
    portex_records.append({
        "ID": 2.1,
        "CONCEPT": row["CONCEPT2"],
        "AMT": row["AMOUNT2"]
    })

# From MGIA
for row in base_mgia.iter_rows(named=True):
    portex_records.append({
        "ID": 3.11,
        "CONCEPT": row["CONCEPT3"],
        "AMT": row["AMOUNT3"]
    })

# From Commodity
for row in base_comm.iter_rows(named=True):
    portex_records.append({
        "ID": 3.21,
        "CONCEPT": row["CONCEPT4"],
        "AMT": row["AMOUNT4"]
    })

# From Equity
for row in base_eqt.iter_rows(named=True):
    portex_records.append({
        "ID": 3.31,
        "CONCEPT": row["CONCEPT5"],
        "AMT": row["AMOUNT5"]
    })

# From INI
for row in base_ini.iter_rows(named=True):
    portex_records.append({
        "ID": 3.41,
        "CONCEPT": row["CONCEPT6"],
        "AMT": row["AMOUNT6"]
    })

portex = pl.DataFrame(portex_records)

# Build total records
total_records = [
    {"ID": 1.0, "CONCEPT": "1.SAVINGS", "AMT": base_sa.select(pl.col("TOTAL1")).item(0, 0)},
    {"ID": 2.0, "CONCEPT": "2.CURRENT", "AMT": total2},
    {"ID": 3.0, "CONCEPT": "3.TERM", "AMT": high_term},
    {"ID": 3.1, "CONCEPT": "A.MGIA", "AMT": total3},
    {"ID": 3.2, "CONCEPT": "B.COMMODITY MURABAHAH", "AMT": total4},
    {"ID": 3.3, "CONCEPT": "C.SHORT TERM DEPOSIT", "AMT": total5},
    {"ID": 3.4, "CONCEPT": "D.INI", "AMT": total6}
]

total = pl.DataFrame(total_records)

# Combine all records
portall = pl.concat([portex, total]).sort("ID")

print(f"✓ Generated {len(portall)} report lines")


# ============================================================================
# WRITE OUTPUT REPORT
# ============================================================================

print(f"Writing report to: {OUTPUT_FILE}")

with open(OUTPUT_FILE, 'w') as f:
    # Header section
    write_asa_line(f, ' ', ' ')
    write_asa_line(f, 'TITLE: REPORTING REQUIREMENTS FOR ISLAMIC BANKING', ' ')
    write_asa_line(f, '                                                  PORTFOLIO EXPOSURE ', ' ')
    write_asa_line(f, '       (INVESTMENT,DEPOSIT,DERIVATIVES AND SUKUK HOLDING)', ' ')
    write_asa_line(f,
                   '                                                         ACCORDING TO PRODUCT AND SHARIAH APPROVED',
                   ' ')
    write_asa_line(f, f'       AS AT :{rdate}', ' ')
    write_asa_line(f, 'ITEM/CONCEPT', ' ')
    write_asa_line(f, 'DEPOSIT/INVESTMENT;RM(AMOUNT);', ' ')

    # Data rows
    for row in portall.iter_rows(named=True):
        concept = row["CONCEPT"]
        amt = format_amount(row["AMT"])
        line = f"{concept};{amt};"
        write_asa_line(f, line, ' ')

print(f"✓ Report generated successfully: {OUTPUT_FILE}")
print()

# Close DuckDB connection
con.close()

print("=" * 70)
print("EIIMPORT Report Generation Complete!")
print("=" * 70)
print()
print("Summary:")
print(f"  Savings Total:    RM {base_sa.select(pl.col('TOTAL1')).item(0, 0):,.2f}")
print(f"  Current Total:    RM {total2:,.2f}")
print(f"  Term Total:       RM {high_term:,.2f}")
print(f"    - MGIA:         RM {total3:,.2f}")
print(f"    - Commodity:    RM {total4:,.2f}")
print(f"    - Short Term:   RM {total5:,.2f}")
print(f"    - INI:          RM {total6:,.2f}")
