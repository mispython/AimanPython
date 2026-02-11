#!/usr/bin/env python3
"""
Program: WISDPBBE
To extract the information from RDAL1 extraction to be used for MIS reporting
"""

import polars as pl
import duckdb
from pathlib import Path
from datetime import datetime
import os

# =====================================================================
# CONFIGURATION AND PATH SETUP
# =====================================================================

# Define base paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

# Create output directory if it doesn't exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input parquet file paths
SETITEMS_FILE = DATA_DIR / "SETITEMS.parquet"
ALWGL_FILE = DATA_DIR / "ALWGL.parquet"
ALWJE_FILE = DATA_DIR / "ALWJE.parquet"

# Output file path
OUTPUT_FILE = OUTPUT_DIR / "WISDAY.txt"

# Parameters (from SAS macro variables)
# These would typically be passed as function parameters
REPTMON = os.getenv("REPTMON", "01")  # Reporting month (MMYY format)
NOWK = os.getenv("NOWK", "001")  # Week indicator
REPTYEAR = os.getenv("REPTYEAR", "2026")  # Reporting year
RDATE = os.getenv("RDATE", "110226")  # Report date (DDMMYY format)

# =====================================================================
# STEP 1: LOAD AND PREPARE SETITEMS DATA
# =====================================================================

# Load SETITEMS parquet file
setitems_df = pl.read_parquet(SETITEMS_FILE)

# Keep only SET_ID and remove duplicates
items = setitems_df.select("SET_ID").unique()

# =====================================================================
# STEP 2: CREATE GL TABLE
# =====================================================================

# Load ALWGL data
alwgl_df = pl.read_parquet(ALWGL_FILE)

# Join with ITEMS and create GL table
gl = alwgl_df.join(items, on="SET_ID", how="inner").select([
    "BRNO",
    "SET_ID",
    "GLAMT",
    pl.col("ACCT_NO").str.slice(7, 5).alias("ACK_NO")
])

# =====================================================================
# STEP 3: CREATE JE TABLE
# =====================================================================

# Load ALWJE data
alwje_df = pl.read_parquet(ALWJE_FILE)

# Join with ITEMS and create JE table
je = alwje_df.join(items, on="SET_ID", how="inner").select([
    "BRNO",
    "SET_ID",
    "JEAMT",
    pl.col("ACCT_NO").str.slice(7, 5).alias("ACK_NO"),
    "EFFDATE"
])

# =====================================================================
# STEP 4: SUMMARIZE JE TABLE
# =====================================================================

# Group by BRNO, SET_ID, ACK_NO, EFFDATE and sum JEAMT
je_summary = je.group_by(["BRNO", "SET_ID", "ACK_NO", "EFFDATE"]).agg(
    pl.col("JEAMT").sum()
).sort(["BRNO", "SET_ID", "ACK_NO", "EFFDATE"])


# =====================================================================
# STEP 5: PROCESS JE DATA WITH DATE EXPANSION
# =====================================================================

# Convert date strings to proper format
def parse_sas_date(date_str):
    """Parse SAS date format DDMMYY to datetime"""
    if not date_str or len(date_str) < 6:
        return None
    day = int(date_str[0:2])
    month = int(date_str[2:4])
    year = int(date_str[4:6])
    # Add century (assume 20xx for 00-99)
    year = 2000 + year if year < 100 else year
    return datetime(year, month, day)


def format_sas_date(date_obj, format_str):
    """Format datetime to SAS date format DDMMYY"""
    if format_str == "DDMMYY8":
        return date_obj.strftime("%d%m%y")
    return str(date_obj)


# Parse macro variables into dates
rept_start = parse_sas_date(REPTMON + REPTYEAR)
rept_end = parse_sas_date(RDATE)

# Expand JE data to include all dates
je_expanded_list = []

for row in je_summary.iter_rows(named=True):
    brno = row["BRNO"]
    set_id = row["SET_ID"]
    ack_no = row["ACK_NO"]
    jeamt = row["JEAMT"]
    effdate = row["EFFDATE"]

    # Parse EFFDATE
    if isinstance(effdate, str):
        effdate_parsed = parse_sas_date(effdate)
    else:
        effdate_parsed = effdate

    jevalue = 0
    reptdate = rept_start

    # Output rows before effective date with JEVALUE=0
    while reptdate < effdate_parsed:
        je_expanded_list.append({
            "BRNO": brno,
            "SET_ID": set_id,
            "ACK_NO": ack_no,
            "REPTDATE": reptdate,
            "JEVALUE": jevalue,
            "JEAMT": jeamt
        })
        reptdate = datetime(reptdate.year, reptdate.month, reptdate.day + 1)

    # Add the effective date row
    jevalue += jeamt
    je_expanded_list.append({
        "BRNO": brno,
        "SET_ID": set_id,
        "ACK_NO": ack_no,
        "REPTDATE": reptdate,
        "JEVALUE": jevalue,
        "JEAMT": jeamt
    })
    reptdate = datetime(reptdate.year, reptdate.month, reptdate.day + 1)

    # Fill until end date
    if reptdate <= rept_end:
        while reptdate <= rept_end:
            je_expanded_list.append({
                "BRNO": brno,
                "SET_ID": set_id,
                "ACK_NO": ack_no,
                "REPTDATE": reptdate,
                "JEVALUE": jevalue,
                "JEAMT": jeamt
            })
            reptdate = datetime(reptdate.year, reptdate.month, reptdate.day + 1)

je_expanded = pl.DataFrame(je_expanded_list)

# =====================================================================
# STEP 6: SUMMARIZE GL TABLE
# =====================================================================

gl_summary = gl.group_by(["BRNO", "SET_ID", "ACK_NO"]).agg(
    pl.col("GLAMT").sum()
)

# =====================================================================
# STEP 7: MERGE GL AND JE DATA
# =====================================================================

# Merge GL and JE
merged = gl_summary.join(
    je_expanded.select(["BRNO", "SET_ID", "ACK_NO", "REPTDATE", "JEVALUE"]),
    on=["BRNO", "SET_ID", "ACK_NO"],
    how="outer"
).sort(["BRNO", "SET_ID", "ACK_NO", "REPTDATE"])

# =====================================================================
# STEP 8: CREATE FINAL OUTPUT
# =====================================================================

# Create final output with AMOUNT calculation
output_list = []

for row in merged.iter_rows(named=True):
    brno = row["BRNO"]
    set_id = row["SET_ID"]
    ack_no = row["ACK_NO"]
    glamt = row["GLAMT"]
    jevalue = row["JEVALUE"]
    reptdate = row["REPTDATE"]

    # If only GL exists (no JE data)
    if glamt is not None and jevalue is None:
        for d in range((rept_end - rept_start).days + 1):
            current_date = rept_start + __import__('datetime').timedelta(days=d)
            output_list.append({
                "BRNO": brno,
                "SET_ID": set_id,
                "ACK_NO": ack_no,
                "REPTDATE": current_date,
                "AMOUNT": glamt
            })
    else:
        # Merge GL and JE values
        amount = (glamt or 0) + (jevalue or 0)
        output_list.append({
            "BRNO": brno,
            "SET_ID": set_id,
            "ACK_NO": ack_no,
            "REPTDATE": reptdate,
            "AMOUNT": amount
        })

wisday_output = pl.DataFrame(output_list).sort(["BRNO", "SET_ID", "ACK_NO", "REPTDATE"])

# =====================================================================
# STEP 9: WRITE OUTPUT
# =====================================================================

# Write to text file
wisday_output.write_csv(str(OUTPUT_FILE), separator=",")

print(f"Output written to {OUTPUT_FILE}")
print(f"Total records: {len(wisday_output)}")
