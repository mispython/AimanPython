# !/usr/bin/env python3
"""
PROGRAM : EIBQPBCM                                          
DATE    : 18.03.2002                                        
PURPOSE : PB PREMIUM CLUB MEMBER LISTING. FOR BOTH ELIGIBLE 
            AND AUTOMATIC MEMBERS...BASED ON NAMELST, NAMELST1
            NAMELST3, AND NAMELST4 PRODUCE BY EIBQADR1,       
            EIBQADR2, EIBQADR3, AND EIBQADR4 RESPECTIVELY.    
          LOAN LIST MEMBER ALSO INCLUDE, FOR UNICARD USERS. 
"""

import duckdb
import polars as pl
from pathlib import Path

# ---------------------------------------------------------------------------
# Path Configuration
# ---------------------------------------------------------------------------
INPUT_DIR  = Path("input")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PARQUET_NEW     = INPUT_DIR / "addr_new.parquet"
PARQUET_AUT     = INPUT_DIR / "addr_aut.parquet"
PARQUET_P50     = INPUT_DIR / "addr_p50.parquet"
PARQUET_HL      = INPUT_DIR / "addr_hl.parquet"
PARQUET_HP      = INPUT_DIR / "addr_hp.parquet"
PARQUET_REPTDATE = INPUT_DIR / "addr_reptdate.parquet"

OUTPUT_FILE = OUTPUT_DIR / "EIBQPBCM_output.txt"

# ---------------------------------------------------------------------------
# Dependency placeholder: %INC PGM(PBBELF)
# This includes the PBBELF macro/format library (branch code formats, etc.)
# ---------------------------------------------------------------------------
# %INC PGM(PBBELF)

# ---------------------------------------------------------------------------
# PROC FORMAT equivalents
# ---------------------------------------------------------------------------
SAPROD_MAP = {
    200: 'PLUS',
    202: 'YAA',
    203: '50PLUS',
    212: 'WISE',
    213: 'PB SAVELINK',
    214: 'MUDHARABAH BESTARI',
    150: 'ACE NORMAL',
    152: 'ACE EXTERNAL',
    156: 'PB CURRENTLINK',
    157: 'PB CURRENTLINK EXT',
    204: 'AL-WADIAH SA',
    100: 'PLUS CURRENT',
    102: 'PLUS CA EXT',
    160: 'AL-WADIAH CA',
    162: 'AL-WADIAH CA EXT',
}

SAPURP_MAP = {
    '1': 'PERSONAL',
    '2': 'JOINT',
    '5': 'ON-BEHALF',
    '6': 'ON-BEHALF',
}


def fmt_saprod(val):
    return SAPROD_MAP.get(val, 'UNKNOWN')


def fmt_sapurp(val):
    return SAPURP_MAP.get(str(val).strip(), 'UNKNOWN')


# ---------------------------------------------------------------------------
# BRCHCD format: branch numeric -> branch name string
# This format is defined in PBBELF. Without its source, we replicate the
# zero-padded numeric code as a placeholder (e.g., 001, 002, ...).
# In production, replace the lambda below with the actual BRCHCD mapping.
# ---------------------------------------------------------------------------
def fmt_brchcd(branch_val):
    # Placeholder: BRCHCD format from PBBELF dependency
    # Replace with actual branch code name mapping when available.
    return str(branch_val)


def build_branch_string(branch_val):
    """Replicate: PUT(BRANCH,Z3.) || '/' || PUT(BRANCH,BRCHCD.)"""
    try:
        numeric_part = str(int(branch_val)).zfill(3)
    except (ValueError, TypeError):
        numeric_part = str(branch_val).zfill(3)
    name_part = fmt_brchcd(branch_val)
    return f"{numeric_part}/{name_part}"


# ---------------------------------------------------------------------------
# Read REPTDATE to obtain report date macro variable &RDATE
# (WORDDATX18. format: e.g., "18 March     2002")
# ---------------------------------------------------------------------------
def get_report_date():
    con = duckdb.connect()
    df = con.execute(f"SELECT REPTDATE FROM '{PARQUET_REPTDATE}' LIMIT 1").fetchdf()
    con.close()
    if df.empty:
        return ""
    rdate = df["REPTDATE"].iloc[0]
    # Replicate WORDDATX18. format: day month(word,padded) year, 18 chars total
    import datetime
    if hasattr(rdate, 'strftime'):
        dt = rdate
    else:
        dt = pl.Series([rdate]).cast(pl.Date)[0]
        dt = datetime.date(dt.year, dt.month, dt.day)
    months = ["January", "February", "March", "April", "May", "June",
              "July", "August", "September", "October", "November", "December"]
    formatted = f"{dt.day} {months[dt.month - 1]:<9} {dt.year}"
    return formatted[:18]


# ---------------------------------------------------------------------------
# Load a parquet file, compute BRCH, drop BRANCH, rename BRCH -> BRANCH
# ---------------------------------------------------------------------------
def load_and_transform(parquet_path: Path) -> pl.DataFrame:
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM '{parquet_path}'").pl()
    con.close()
    branch_col = df["BRANCH"].to_list()
    brch_values = [build_branch_string(v) for v in branch_col]
    df = df.drop("BRANCH").with_columns(
        pl.Series("BRANCH", brch_values)
    )
    return df


# ---------------------------------------------------------------------------
# Main Processing
# ---------------------------------------------------------------------------
def main():
    # Load and transform each input dataset
    namelst1 = load_and_transform(PARQUET_NEW)   # ADDR.NEW  -> NAMELST1
    namelst  = load_and_transform(PARQUET_AUT)   # ADDR.AUT  -> NAMELST
    namelst3 = load_and_transform(PARQUET_P50)   # ADDR.P50  -> NAMELST3
    namelst4 = load_and_transform(PARQUET_HL)    # ADDR.HL   -> NAMELST4
    namelst5 = load_and_transform(PARQUET_HP)    # ADDR.HP   -> NAMELST5

    # Combine all datasets: SET NAMELST NAMELST1 NAMELST3 NAMELST4 NAMELST5
    # Use diagonal_relaxed to handle differing schemas gracefully
    namelist = pl.concat(
        [namelst, namelst1, namelst3, namelst4, namelst5],
        how="diagonal_relaxed"
    )

    # PROC SORT DATA=NAMELIST; BY NAME;
    namelist = namelist.sort("NAME")

    # Write output file:
    # PUT @0001 NAME  $40.
    #     @0041 OLDIC $20.
    #     @0061 NEWIC $20.
    # Fixed-width text output (positions are 1-based in SAS, 0-based here)
    # Total record length: 80 characters
    lines = []
    for row in namelist.iter_rows(named=True):
        name_val  = str(row.get("NAME",  "") or "").ljust(40)[:40]
        oldic_val = str(row.get("OLDIC", "") or "").ljust(20)[:20]
        newic_val = str(row.get("NEWIC", "") or "").ljust(20)[:20]
        line = f"{name_val}{oldic_val}{newic_val}"
        lines.append(line)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")

    print(f"Output written to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
