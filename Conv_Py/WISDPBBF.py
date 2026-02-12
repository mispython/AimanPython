#!/usr/bin/env python3
"""
Program: WISDPBBF
To list the set_id from RDAL1 to be used for MIS reporting
"""

import polars as pl
from pathlib import Path

# =====================================================================
# CONFIGURATION AND PATH SETUP
# =====================================================================

# Define base paths
BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"

# Create output directory if it doesn't exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Output file path
OUTPUT_FILE = OUTPUT_DIR / "SETITEMS.txt"

# =====================================================================
# STEP 1: CREATE SETITEMS DATASET
# =====================================================================

# Define the SET_ID data (columns 1-14, right-padded)
set_id_data = "R139111CASH"

# Create DataFrame with SET_ID column
setitems = pl.DataFrame({
    "SET_ID": [set_id_data]
})

# =====================================================================
# STEP 2: WRITE OUTPUT
# =====================================================================

# Write to text file
setitems.write_csv(str(OUTPUT_FILE), separator=",")

print(f"Output written to {OUTPUT_FILE}")
print(f"Total records: {len(setitems)}")
print("\nData:")
print(setitems)
