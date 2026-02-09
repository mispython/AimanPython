#!/usr/bin/env python3
"""
EIBMBAEI Report Generation
Profile on BAE Personal Financing-I Customers
Generates 7 distribution reports using PROC TABULATE format
"""

import duckdb
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import sys


# ============================================================================
# PATH SETUP
# ============================================================================
BASE_PATH = Path("/data")
INPUT_PATH = BASE_PATH / "input"
OUTPUT_PATH = BASE_PATH / "output"

# Input files
REPTDATE_FILE = INPUT_PATH / "reptdate.parquet"
LOAN_FILE_PATTERN = INPUT_PATH / "loan_{month}{week}.parquet"
ELDS_FILE_PATTERN = INPUT_PATH / "ieln{month}{week}{year}.parquet"
CISLN_FILE = INPUT_PATH / "cisln_loan.parquet"

# Output file
OUTPUT_FILE = OUTPUT_PATH / "MATURE_BAE.txt"

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

reptdat1 = reptdate + timedelta(days=1)
mm = reptdat1.month

wk1, wk2, wk3, wk4 = '1', '2', '3', '4'
reptyea2 = reptdate.strftime("%y")
reptyea1 = reptdate.strftime("%Y")
reptmon1 = f"{reptdate.month:02d}"
reptday = f"{reptdate.day:02d}"
rdate = reptdate.strftime("%d/%m/%y")
reptmon = f"{mm:02d}"
reptyear = int(reptdat1.strftime("%Y"))

print(f"Report Date: {rdate}")
print(f"Report Month: {reptmon}")
print()


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def write_asa_line(f, line, carriage_control=' '):
    """Write line with ASA carriage control character"""
    f.write(f"{carriage_control}{line}\n")


def format_number(value, decimals=0):
    """Format number with comma separator"""
    if value is None or value == 0:
        return "0" if decimals == 0 else "0.00"
    if decimals > 0:
        return f"{value:,.{decimals}f}"
    else:
        return f"{int(value):,}"


def calculate_percentage(part, total):
    """Calculate percentage"""
    if total == 0:
        return 0.0
    return (part / total) * 100.0


def generate_tabulate_report(data, class_col, value_cols, title, row_label,
                             col1_label, col2_label, page_num=1):
    """
    Generate a SAS PROC TABULATE style report
    """
    lines = []

    # Title section
    if page_num == 1:
        lines.append(('1', 'PUBLIC ISLAMIC BANK BERHAD: REPORT ID: EIBMBAEI'))
        lines.append((' ', f'PROFILE ON BAE PERSONAL FINANCING-I CUSTOMERS {rdate}'))
    else:
        lines.append(('1', 'PUBLIC ISLAMIC BANK BERHAD: REPORT ID: EIBMBAEI'))
        lines.append((' ', f'PROFILE ON BAE PERSONAL FINANCING-I CUSTOMERS {rdate}'))

    lines.append((' ', title))
    lines.append((' ', 'COORBRH=IBU'))
    lines.append((' ', ''))

    # Calculate totals
    total_col1 = data.select(pl.col(value_cols[0]).sum()).item(0, 0) or 0
    total_col2 = data.select(pl.col(value_cols[1]).sum()).item(0, 0) or 0

    # Header line
    header = f"{row_label:<30}|          |          |              |                  |              |"
    lines.append((' ', header))

    col1_lbl = col1_label
    col2_lbl = col2_label

    header2 = f"{' ':<30}|{col1_lbl:^10}|{' ':^10}|{col2_lbl:^18}|{' ':^14}|"
    lines.append((' ', header2))

    header3 = f"{' ':<30}|{'NO OF A/C':>10}|PERCENTAGE|    AMOUNT(RM)    |  PERCENTAGE  |"
    lines.append((' ', header3))

    header4 = f"{' ':<30}|{' ':^10}|    (%)   |                  |     (%)      |"
    lines.append((' ', header4))

    lines.append((' ', '-' * 100))

    # Data rows
    sorted_data = data.sort(class_col)

    for row in sorted_data.iter_rows(named=True):
        label = row[class_col]
        val1 = row[value_cols[0]] or 0
        val2 = row[value_cols[1]] or 0

        pct1 = calculate_percentage(val1, total_col1)
        pct2 = calculate_percentage(val2, total_col2)

        val1_str = format_number(val1, 0)
        pct1_str = format_number(pct1, 2)
        val2_str = format_number(val2, 2)
        pct2_str = format_number(pct2, 2)

        line = f"{label:<30}|{val1_str:>10}|{pct1_str:>10}|{val2_str:>18}|{pct2_str:>14}|"
        lines.append((' ', line))

    # Total line
    lines.append((' ', '-' * 100))
    total_val1_str = format_number(total_col1, 0)
    total_val2_str = format_number(total_col2, 2)
    total_line = f"{'TOTAL':<30}|{total_val1_str:>10}|{'100.00':>10}|{total_val2_str:>18}|{'100.00':>14}|"
    lines.append((' ', total_line))
    lines.append((' ', '-' * 100))

    return lines


# ============================================================================
# LOAD AND PROCESS LOAN DATA
# ============================================================================

print("Loading loan data...")

loan_file = INPUT_PATH / f"loan_{reptmon1}{wk4}.parquet"
query_loan = f"""
    SELECT *
    FROM read_parquet('{loan_file}')
    WHERE PRODUCT = 135
"""
lnote = con.execute(query_loan).pl()

# Add computed columns
lnote = lnote.with_columns([
    pl.lit(1).alias("NOACCT"),
    (pl.col("NOTETERM") - 3).alias("NOTETERM_ADJ")
])

# Apply NETPROC logic
lnote = lnote.with_columns([
    pl.when(pl.col("APPRLIM2") > 150000)
    .then(pl.col("APPRLIM2"))
    .otherwise(pl.col("NETPROC"))
    .alias("NETPROC_FINAL")
])

# Create TENURE categories
lnote = lnote.with_columns([
    pl.when(pl.col("NOTETERM_ADJ") == 24).then(pl.lit("01.  24 MONTHS"))
    .when(pl.col("NOTETERM_ADJ") == 36).then(pl.lit("02.  36 MONTHS"))
    .when(pl.col("NOTETERM_ADJ") == 48).then(pl.lit("03.  48 MONTHS"))
    .when(pl.col("NOTETERM_ADJ") == 60).then(pl.lit("04.  60 MONTHS"))
    .when(pl.col("NOTETERM_ADJ") == 72).then(pl.lit("05.  72 MONTHS"))
    .when(pl.col("NOTETERM_ADJ") == 84).then(pl.lit("06.  84 MONTHS"))
    .when(pl.col("NOTETERM_ADJ") == 96).then(pl.lit("07.  96 MONTHS"))
    .when(pl.col("NOTETERM_ADJ") == 108).then(pl.lit("08. 108 MONTHS"))
    .when(pl.col("NOTETERM_ADJ") == 120).then(pl.lit("09. 120 MONTHS"))
    .when(pl.col("NOTETERM_ADJ") == 132).then(pl.lit("10. 132 MONTHS"))
    .when(pl.col("NOTETERM_ADJ") == 144).then(pl.lit("11. 144 MONTHS"))
    .when(pl.col("NOTETERM_ADJ") == 156).then(pl.lit("12. 156 MONTHS"))
    .when(pl.col("NOTETERM_ADJ") == 168).then(pl.lit("13. 168 MONTHS"))
    .when(pl.col("NOTETERM_ADJ") == 180).then(pl.lit("14. 180 MONTHS"))
    .when(pl.col("NOTETERM_ADJ") == 192).then(pl.lit("15. 192 MONTHS"))
    .when(pl.col("NOTETERM_ADJ") == 204).then(pl.lit("16. 204 MONTHS"))
    .when(pl.col("NOTETERM_ADJ") == 216).then(pl.lit("17. 216 MONTHS"))
    .when(pl.col("NOTETERM_ADJ") == 228).then(pl.lit("18. 228 MONTHS"))
    .otherwise(pl.lit("19. 240 MONTHS"))
    .alias("TENURE")
])

# Create STATE categories
lnote = lnote.with_columns([
    pl.when(pl.col("STATECD") == "A").then(pl.lit("01. PERAK              "))
    .when(pl.col("STATECD") == "B").then(pl.lit("02. SELANGOR           "))
    .when(pl.col("STATECD") == "C").then(pl.lit("03. PAHANG             "))
    .when(pl.col("STATECD") == "D").then(pl.lit("04. KELANTAN           "))
    .when(pl.col("STATECD") == "J").then(pl.lit("05. JOHOR              "))
    .when(pl.col("STATECD") == "K").then(pl.lit("06. KEDAH              "))
    .when(pl.col("STATECD") == "L").then(pl.lit("07. WILAYAH LABUAN     "))
    .when(pl.col("STATECD") == "M").then(pl.lit("08. MELAKA             "))
    .when(pl.col("STATECD") == "N").then(pl.lit("09. NEGERI SEMBILAN    "))
    .when(pl.col("STATECD") == "P").then(pl.lit("10. PENANG             "))
    .when(pl.col("STATECD") == "Q").then(pl.lit("11. SARAWAK            "))
    .when(pl.col("STATECD") == "R").then(pl.lit("12. PERLIS             "))
    .when(pl.col("STATECD") == "S").then(pl.lit("13. SABAH              "))
    .when(pl.col("STATECD") == "T").then(pl.lit("14. TERENGGANU         "))
    .when(pl.col("STATECD") == "W").then(pl.lit("15. WILAYAH PERSEKUTUAN"))
    .otherwise(pl.lit("16. UNKNOWN            "))
    .alias("STATED")
])

# Create LMTGRP categories
lnote = lnote.with_columns([
    pl.when(pl.col("NETPROC_FINAL") <= 10000).then(pl.lit("01.UP TO  10,000   "))
    .when(pl.col("NETPROC_FINAL") <= 15000).then(pl.lit("02.> 10,000- 15,000"))
    .when(pl.col("NETPROC_FINAL") <= 20000).then(pl.lit("03.> 15,000- 20,000"))
    .when(pl.col("NETPROC_FINAL") <= 30000).then(pl.lit("04.> 20,000- 30,000"))
    .when(pl.col("NETPROC_FINAL") <= 40000).then(pl.lit("05.> 30,000- 40,000"))
    .when(pl.col("NETPROC_FINAL") <= 50000).then(pl.lit("06.> 40,000- 50,000"))
    .when(pl.col("NETPROC_FINAL") <= 60000).then(pl.lit("07.> 50,000- 60,000"))
    .when(pl.col("NETPROC_FINAL") <= 70000).then(pl.lit("08.> 60,000- 70,000"))
    .when(pl.col("NETPROC_FINAL") <= 80000).then(pl.lit("09.> 70,000- 80,000"))
    .when(pl.col("NETPROC_FINAL") <= 90000).then(pl.lit("10.> 80,000- 90,000"))
    .when(pl.col("NETPROC_FINAL") <= 100000).then(pl.lit("11.> 90,000-100,000"))
    .when(pl.col("NETPROC_FINAL") <= 110000).then(pl.lit("12.>100,000-110,000"))
    .when(pl.col("NETPROC_FINAL") <= 120000).then(pl.lit("13.>110,000-120,000"))
    .when(pl.col("NETPROC_FINAL") <= 130000).then(pl.lit("14.>120,000-130,000"))
    .when(pl.col("NETPROC_FINAL") <= 140000).then(pl.lit("15.>130,000-140,000"))
    .when(pl.col("NETPROC_FINAL") <= 150000).then(pl.lit("16.>140,000-150,000"))
    .otherwise(pl.lit("17.>150,000        "))
    .alias("LMTGRP")
])

print(f"✓ Loaded {len(lnote)} loan records")


# ============================================================================
# REPORT 1: DISTRIBUTION BY STATE
# ============================================================================

print("Generating Report 1: Distribution by State...")

rpt1 = lnote.group_by("STATED").agg([
    pl.col("NOACCT").sum().alias("NOACCT"),
    pl.col("BALANCE").sum().alias("BALANCE")
])


# ============================================================================
# REPORT 2: DISTRIBUTION BY FINANCING LIMIT
# ============================================================================

print("Generating Report 2: Distribution by Financing Limit...")

rpt2 = lnote.group_by("LMTGRP").agg([
    pl.col("NOACCT").sum().alias("NOACCT"),
    pl.col("BALANCE").sum().alias("BALANCE")
])

# Create dummy data to ensure all categories exist
lmtgrp_categories = [
    "01.UP TO  10,000   ", "02.> 10,000- 15,000", "03.> 15,000- 20,000",
    "04.> 20,000- 30,000", "05.> 30,000- 40,000", "06.> 40,000- 50,000",
    "07.> 50,000- 60,000", "08.> 60,000- 70,000", "09.> 70,000- 80,000",
    "10.> 80,000- 90,000", "11.> 90,000-100,000", "12.>100,000-110,000",
    "13.>110,000-120,000", "14.>120,000-130,000", "15.>130,000-140,000",
    "16.>140,000-150,000", "17.>150,000        "
]

dum2 = pl.DataFrame({
    "LMTGRP": lmtgrp_categories,
    "NOACCT": [0] * len(lmtgrp_categories),
    "BALANCE": [0.0] * len(lmtgrp_categories)
})

rpt2 = dum2.join(rpt2, on="LMTGRP", how="left", suffix="_act").with_columns([
    pl.coalesce(["NOACCT_act", "NOACCT"]).alias("NOACCT"),
    pl.coalesce(["BALANCE_act", "BALANCE"]).alias("BALANCE")
]).select(["LMTGRP", "NOACCT", "BALANCE"])


# ============================================================================
# REPORT 3: DISTRIBUTION BY TENURE
# ============================================================================

print("Generating Report 3: Distribution by Tenure...")

rpt3 = lnote.group_by("TENURE").agg([
    pl.col("NOACCT").sum().alias("NOACCT"),
    pl.col("BALANCE").sum().alias("BALANCE")
])

# Create dummy data for all tenure categories
tenure_categories = [
    "01.  24 MONTHS", "02.  36 MONTHS", "03.  48 MONTHS", "04.  60 MONTHS",
    "05.  72 MONTHS", "06.  84 MONTHS", "07.  96 MONTHS", "08. 108 MONTHS",
    "09. 120 MONTHS", "10. 132 MONTHS", "11. 144 MONTHS", "12. 156 MONTHS",
    "13. 168 MONTHS", "14. 180 MONTHS", "15. 192 MONTHS", "16. 204 MONTHS",
    "17. 216 MONTHS", "18. 228 MONTHS", "19. 240 MONTHS"
]

dum3 = pl.DataFrame({
    "TENURE": tenure_categories,
    "NOACCT": [0] * len(tenure_categories),
    "BALANCE": [0.0] * len(tenure_categories)
})

rpt3 = dum3.join(rpt3, on="TENURE", how="left", suffix="_act").with_columns([
    pl.coalesce(["NOACCT_act", "NOACCT"]).alias("NOACCT"),
    pl.coalesce(["BALANCE_act", "BALANCE"]).alias("BALANCE")
]).select(["TENURE", "NOACCT", "BALANCE"])


# ============================================================================
# LOAD CISLN DATA AND JOIN
# ============================================================================

print("Loading customer demographic data...")

cisln = pl.read_parquet(CISLN_FILE)
cisln = cisln.filter(pl.col("SECCUST") == "901").select([
    "ACCTNO", "GENDER", "RACE", "BIRTHDAT"
]).unique(subset=["ACCTNO"])

# Merge with loan data
p135 = lnote.join(cisln, on="ACCTNO", how="left")

# Calculate age
p135 = p135.with_columns([
    pl.when(pl.col("BIRTHDAT").is_not_null())
    .then(reptyear - pl.col("BIRTHDAT").str.slice(4, 4).cast(pl.Int32))
    .otherwise(0)
    .alias("AGE")
])

# Create age groups
p135 = p135.with_columns([
    pl.when((pl.col("AGE") >= 18) & (pl.col("AGE") <= 30)).then(pl.lit("1. 18 - 30 "))
    .when((pl.col("AGE") >= 31) & (pl.col("AGE") <= 40)).then(pl.lit("2. 31 - 40 "))
    .when((pl.col("AGE") >= 41) & (pl.col("AGE") <= 50)).then(pl.lit("3. 41 - 50 "))
    .when((pl.col("AGE") >= 51) & (pl.col("AGE") <= 55)).then(pl.lit("4. 51 - 55 "))
    .otherwise(pl.lit("5. 56 - 58 "))
    .alias("AGEGP")
])

# Create race categories
p135 = p135.with_columns([
    pl.when(pl.col("RACE") == "1").then(pl.lit("MALAY     "))
    .when(pl.col("RACE") == "2").then(pl.lit("CHINESE   "))
    .when(pl.col("RACE") == "3").then(pl.lit("INDIAN    "))
    .otherwise(pl.lit("OTHER     "))
    .alias("RACED")
])

# Create gender categories
p135 = p135.with_columns([
    pl.when(pl.col("GENDER") == "M").then(pl.lit("MALE  "))
    .when(pl.col("GENDER") == "F").then(pl.lit("FEMALE"))
    .otherwise(pl.lit("N/A   "))
    .alias("GENDERX")
])


# ============================================================================
# REPORT 4: DISTRIBUTION BY RACE
# ============================================================================

print("Generating Report 4: Distribution by Race...")

rpt4 = p135.group_by("RACED").agg([
    pl.col("NOACCT").sum().alias("NOACCT"),
    pl.col("BALANCE").sum().alias("BALANCE")
])


# ============================================================================
# REPORT 5: DISTRIBUTION BY AGE
# ============================================================================

print("Generating Report 5: Distribution by Age...")

rpt5 = p135.group_by("AGEGP").agg([
    pl.col("NOACCT").sum().alias("NOACCT"),
    pl.col("BALANCE").sum().alias("BALANCE")
])


# ============================================================================
# REPORT 6: DISTRIBUTION BY GENDER
# ============================================================================

print("Generating Report 6: Distribution by Gender...")

rpt6 = p135.group_by("GENDERX").agg([
    pl.col("NOACCT").sum().alias("NOACCT"),
    pl.col("BALANCE").sum().alias("BALANCE")
])


# ============================================================================
# LOAD ELDS DATA FOR REPORT 7
# ============================================================================

print("Loading ELDS approval data...")

elds_files = []
for week in [wk1, wk2, wk3, wk4]:
    elds_file = INPUT_PATH / f"ieln{reptmon1}{week}{reptyea2}.parquet"
    if elds_file.exists():
        elds_files.append(str(elds_file))

if elds_files:
    query_elds = f"""
        SELECT *
        FROM read_parquet([{','.join("'" + f + "'" for f in elds_files)}])
        WHERE STATUS = 'APPROVED'
        AND PRODUCT = 135
    """
    elds = con.execute(query_elds).pl()

    elds = elds.with_columns([
        pl.lit(1).alias("NOACCT"),
        pl.when(pl.col("GINCOME").is_null()).then(pl.lit(99999999)).otherwise(pl.col("GINCOME")).alias("GINCOME_ADJ")
    ])

    # Create salary groups
    elds = elds.with_columns([
        pl.when(pl.col("GINCOME_ADJ") < 1000).then(pl.lit("01. BELOW 1,000  "))
        .when(pl.col("GINCOME_ADJ") == 1000).then(pl.lit("02. 1,000        "))
        .when(pl.col("GINCOME_ADJ") <= 1500).then(pl.lit("03. >1,000-1,500 "))
        .when(pl.col("GINCOME_ADJ") <= 2000).then(pl.lit("04. >1,500-2,000 "))
        .when(pl.col("GINCOME_ADJ") <= 2500).then(pl.lit("05. >2,000-2,500 "))
        .when(pl.col("GINCOME_ADJ") <= 3000).then(pl.lit("06. >2,500-3,000 "))
        .when(pl.col("GINCOME_ADJ") <= 3500).then(pl.lit("07. >3,000-3,500 "))
        .when(pl.col("GINCOME_ADJ") <= 4000).then(pl.lit("08. >3,500-4,000 "))
        .when(pl.col("GINCOME_ADJ") <= 4500).then(pl.lit("09. >4,000-4,500 "))
        .when(pl.col("GINCOME_ADJ") <= 5000).then(pl.lit("10. >4,500-5,000 "))
        .when(pl.col("GINCOME_ADJ") <= 5500).then(pl.lit("11. >5,000-5,500 "))
        .when(pl.col("GINCOME_ADJ") <= 6000).then(pl.lit("12. >5,500-6,000 "))
        .when(pl.col("GINCOME_ADJ") <= 6500).then(pl.lit("13. >6,000-6,500 "))
        .when(pl.col("GINCOME_ADJ") <= 7000).then(pl.lit("14. >6,500-7,000 "))
        .when(pl.col("GINCOME_ADJ") <= 7500).then(pl.lit("15. >7,000-7,500 "))
        .when(pl.col("GINCOME_ADJ") <= 8000).then(pl.lit("16. >7,500-8,000 "))
        .when(pl.col("GINCOME_ADJ") <= 9000).then(pl.lit("17. >8,000-9,000 "))
        .when(pl.col("GINCOME_ADJ") <= 10000).then(pl.lit("18. >9,000-10,000"))
        .when(pl.col("GINCOME_ADJ") <= 99999998).then(pl.lit("19. ABOVE 10,000 "))
        .otherwise(pl.lit("20. N/A"))
        .alias("SALGRP")
    ])

    rpt7 = elds.group_by("SALGRP").agg([
        pl.col("NOACCT").sum().alias("NOACCT"),
        pl.col("AMOUNT").sum().alias("AMOUNT")
    ])

    # Create dummy data for all salary categories
    salary_categories = [
        "01. BELOW 1,000  ", "02. 1,000        ", "03. >1,000-1,500 ",
        "04. >1,500-2,000 ", "05. >2,000-2,500 ", "06. >2,500-3,000 ",
        "07. >3,000-3,500 ", "08. >3,500-4,000 ", "09. >4,000-4,500 ",
        "10. >4,500-5,000 ", "11. >5,000-5,500 ", "12. >5,500-6,000 ",
        "13. >6,000-6,500 ", "14. >6,500-7,000 ", "15. >7,000-7,500 ",
        "16. >7,500-8,000 ", "17. >8,000-9,000 ", "18. >9,000-10,000",
        "19. ABOVE 10,000 ", "20. N/A"
    ]

    dum7 = pl.DataFrame({
        "SALGRP": salary_categories,
        "NOACCT": [0] * len(salary_categories),
        "AMOUNT": [0.0] * len(salary_categories)
    })

    rpt7 = dum7.join(rpt7, on="SALGRP", how="left", suffix="_act").with_columns([
        pl.coalesce(["NOACCT_act", "NOACCT"]).alias("NOACCT"),
        pl.coalesce(["AMOUNT_act", "AMOUNT"]).alias("AMOUNT")
    ]).select(["SALGRP", "NOACCT", "AMOUNT"])
else:
    print("Warning: No ELDS files found, Report 7 will be empty")
    rpt7 = None

print("✓ All data processed")
print()


# ============================================================================
# WRITE OUTPUT REPORT
# ============================================================================

print(f"Writing report to: {OUTPUT_FILE}")

with open(OUTPUT_FILE, 'w') as f:
    # Report 1: Distribution by State
    lines = generate_tabulate_report(
        rpt1, "STATED", ["NOACCT", "BALANCE"],
        "1.  DISTRIBUTION BY STATE",
        "STATE",
        "", "",
        page_num=1
    )
    for cc, line in lines:
        write_asa_line(f, line, cc)

    # Report 2: Distribution by Financing Limit
    lines = generate_tabulate_report(
        rpt2, "LMTGRP", ["NOACCT", "BALANCE"],
        "2.  DISTRIBUTION BY FINANCING LIMIT",
        "APPROVED LIMIT",
        "", "",
        page_num=2
    )
    for cc, line in lines:
        write_asa_line(f, line, cc)

    # Report 3: Distribution by Tenure
    lines = generate_tabulate_report(
        rpt3, "TENURE", ["NOACCT", "BALANCE"],
        "3.  DISTRIBUTION BY TENURE",
        "TENURE (MONTHS)",
        "", "",
        page_num=3
    )
    for cc, line in lines:
        write_asa_line(f, line, cc)

    # Report 4: Distribution by Race
    lines = generate_tabulate_report(
        rpt4, "RACED", ["NOACCT", "BALANCE"],
        "4.  DISTRIBUTION BY RACE",
        "RACE",
        "", "",
        page_num=4
    )
    for cc, line in lines:
        write_asa_line(f, line, cc)

    # Report 5: Distribution by Age
    lines = generate_tabulate_report(
        rpt5, "AGEGP", ["NOACCT", "BALANCE"],
        "5.  DISTRIBUTION BY AGE",
        "AGE RANGE",
        "", "",
        page_num=5
    )
    for cc, line in lines:
        write_asa_line(f, line, cc)

    # Report 6: Distribution by Gender
    lines = generate_tabulate_report(
        rpt6, "GENDERX", ["NOACCT", "BALANCE"],
        "6.  DISTRIBUTION BY GENDER",
        "GENDER",
        "", "",
        page_num=6
    )
    for cc, line in lines:
        write_asa_line(f, line, cc)

    # Report 7: Distribution by Salary Range
    if rpt7 is not None:
        lines = generate_tabulate_report(
            rpt7, "SALGRP", ["NOACCT", "AMOUNT"],
            "7.  DISTRIBUTION BY SALARY RANGE (APPROVED FOR THE MONTH)",
            "SALARY RANGE",
            "", "",
            page_num=7
        )
        for cc, line in lines:
            write_asa_line(f, line, cc)

print(f"✓ Report generated successfully: {OUTPUT_FILE}")
print()

# Close DuckDB connection
con.close()

print("=" * 70)
print("EIBMBAEI Report Generation Complete!")
print("=" * 70)
