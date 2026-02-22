# !/usr/bin/env python3
"""
PROGRAM : EIBQADR2
PURPOSE : PRINT NAME LABELS FOR ADDR.AUT (AUTO MEMBERS) IN POSTCODE ORDER, 3-UP LABEL FORMAT.
NOTE    : INPUT ADDR.NAMELST IS OUTPUT FROM PGM EIBQADR2
          INPUT: ADDR.SAVINGS, ADDR.AUT
          OUTPUT: PRINT (LABEL REPORT .txt WITH ASA CC)
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

PARQUET_ADDR_SAVINGS = INPUT_DIR / "addr_savings.parquet"
PARQUET_ADDR_AUT     = INPUT_DIR / "addr_aut.parquet"

OUTPUT_FILE = OUTPUT_DIR / "EIBQADR2_labels.txt"

# Page layout
PAGE_LENGTH      = 90    # PS=90 in OPTIONS
LABELS_PER_PAGE  = 24    # IF ACCT>24 THEN PUT _PAGE_
LABEL_ROWS       = 7     # rows per label block (TAGLN + NAMELN1-NAMELN6)
LABEL_COLS       = 3     # 3-up labels
# Column start positions (SAS 1-based: @1, @43, @85 -> 0-based: 0, 42, 84)
COL_POSITIONS    = [0, 42, 84]
COL_WIDTH        = 40


# ---------------------------------------------------------------------------
# Helper: read parquet via DuckDB -> Polars DataFrame
# ---------------------------------------------------------------------------
def read_parquet(path: Path) -> pl.DataFrame:
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM '{path}'").pl()
    con.close()
    return df


# ---------------------------------------------------------------------------
# Postcode extraction logic
# Replicates the SAS ARRAY/VERIFY loop scanning NAMELN1-NAMELN8.
# Returns right-justified 7-char string (mirrors SAS RIGHT()).
# ---------------------------------------------------------------------------
def extract_postcode(row: dict) -> str:
    digits = set("0123456789")
    for i in range(1, 9):
        col = f"NAMELN{i}"
        val = str(row.get(col) or "").ljust(40)
        first5 = val[:5]
        if all(c in digits for c in first5) and len(first5) == 5:
            c6 = val[5:6]
            c7 = val[6:7]
            c8 = val[7:8]
            if c6 == "" or c6 not in digits:
                return first5.rjust(7)
            elif c7 == "" or c7 not in digits:
                return val[:6].rjust(7)
            elif c8 == "" or c8 not in digits:
                return val[:7].rjust(7)
    return " "   # IF I > 8 THEN POSTCODE = ' '


# ---------------------------------------------------------------------------
# ACCTCD derivation
# ACCTCD = REVERSE(PUT(ACCTNO,10.))
# ACCTCD = TRANSLATE(ACCTCD,'1234567890','0987654321')
# Reverse the 10-char zero-padded number, then complement each digit (9 - d).
# ---------------------------------------------------------------------------
def derive_acctcd(acctno) -> str:
    s       = str(int(acctno)).zfill(10)
    rev     = s[::-1]
    flipped = "".join(str(9 - int(c)) for c in rev)
    return flipped


def derive_tagln(branch, acctno) -> str:
    """PUT(BRANCH,Z3.) || '/' || ACCTCD"""
    branch_str = str(int(branch)).zfill(3)
    acctcd     = derive_acctcd(acctno)
    return f"{branch_str}/{acctcd}"


# ---------------------------------------------------------------------------
# Build the label lines for a single row (7 strings)
# ---------------------------------------------------------------------------
def build_label_lines(row: dict) -> list:
    tagln  = str(row.get("TAGLN", "") or "").ljust(COL_WIDTH)[:COL_WIDTH]
    fields = [tagln]
    for i in range(1, 7):
        val = str(row.get(f"NAMELN{i}", "") or "").ljust(COL_WIDTH)[:COL_WIDTH]
        fields.append(val)
    return fields


# ---------------------------------------------------------------------------
# ASA carriage-control helper
# ---------------------------------------------------------------------------
def asa(cc: str, text: str = "") -> str:
    return cc + text


# ---------------------------------------------------------------------------
# Render one group of up to 3 labels into ASA-prefixed report lines
# Mirrors: PUT // @1 LINE1(1) @43 LINE1(2) @85 LINE1(3)
#               / @1 LINE2(1) ... (7 rows total)
# ---------------------------------------------------------------------------
def render_label_group(group: list, output: list) -> None:
    """
    group  : up to 3 elements, each = [tagln, nameln1..nameln6]
    output : accumulator list of ASA-prefixed strings
    """
    while len(group) < LABEL_COLS:
        group.append([" " * COL_WIDTH] * LABEL_ROWS)

    for row_idx in range(LABEL_ROWS):
        # '//' (double space) before first row of each label group -> ASA '0'
        cc = "0" if row_idx == 0 else " "

        line_content = ""
        for col_idx in range(LABEL_COLS):
            start_pos    = COL_POSITIONS[col_idx]
            cell_text    = group[col_idx][row_idx].ljust(COL_WIDTH)[:COL_WIDTH]
            line_content = line_content.ljust(start_pos) + cell_text

        output.append(asa(cc, line_content))


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------
def main():
    # DATA ADDR; SET ADDR.SAVINGS; DROP NAMELN1;
    addr = read_parquet(PARQUET_ADDR_SAVINGS)
    if "NAMELN1" in addr.columns:
        addr = addr.drop("NAMELN1")
    # PROC SORT BY ACCTNO
    addr = addr.sort("ACCTNO")

    # DATA AUT; DROP BRANCH; SET ADDR.AUT; RENAME NAME=NAMELN1;
    aut_df     = read_parquet(PARQUET_ADDR_AUT)
    branch_col = aut_df["BRANCH"].to_list()   # preserve before drop for TAGLN
    aut_df     = aut_df.drop("BRANCH")
    aut_df     = aut_df.rename({"NAME": "NAMELN1"})
    aut_df     = aut_df.with_columns(pl.Series("BRANCH_ORIG", branch_col))
    # PROC SORT BY ACCTNO
    aut_df = aut_df.sort("ACCTNO")

    # DATA AUT; DROP POSTCODE; MERGE ADDR AUT(IN=A); BY ACCTNO; IF A;
    merged = aut_df.join(addr, on="ACCTNO", how="left", suffix="_ADDR")

    # Resolve NAMELN2-NAMELN8: prefer AUT values, fill from ADDR if missing
    for i in range(2, 9):
        col      = f"NAMELN{i}"
        col_addr = f"{col}_ADDR"
        if col_addr in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(col).is_null())
                  .then(pl.col(col_addr))
                  .otherwise(pl.col(col))
                  .alias(col)
            ).drop(col_addr)

    # IF SUBSTR(NAMELN4,1,6)='MALAYS' THEN NAMELN4='   '  (same for 5,6)
    for col in ("NAMELN4", "NAMELN5", "NAMELN6"):
        if col in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(col).cast(pl.Utf8).str.slice(0, 6) == "MALAYS")
                  .then(pl.lit("   "))
                  .otherwise(pl.col(col))
                  .alias(col)
            )

    # DROP POSTCODE (from ADDR merge); re-derived below
    if "POSTCODE" in merged.columns:
        merged = merged.drop("POSTCODE")

    # Derive POSTCODE, ACCTCD, TAGLN
    rows      = merged.to_dicts()
    postcodes = []
    taglns    = []
    for row in rows:
        postcodes.append(extract_postcode(row))
        taglns.append(derive_tagln(row["BRANCH_ORIG"], row["ACCTNO"]))

    merged = merged.with_columns([
        pl.Series("POSTCODE", postcodes),
        pl.Series("TAGLN",    taglns),
    ])

    # PROC SORT BY POSTCODE
    merged = merged.sort("POSTCODE")

    # ---------------------------------------------------------------------------
    # *------------------------------------------------*
    # *  PRINT NAME LABELS IN POSTCODE ORDER           *
    # *------------------------------------------------*
    # TITLE; (no title)
    # ---------------------------------------------------------------------------
    output_lines     = []
    page_label_count = 0
    group_buffer     = []
    new_page_pending = True

    rows_sorted = merged.to_dicts()

    for idx, row in enumerate(rows_sorted):
        is_endfile = (idx == len(rows_sorted) - 1)

        # ACCT+1; IF ACCT>24 THEN DO; ACCT=0; PUT _PAGE_; END;
        page_label_count += 1
        if page_label_count > LABELS_PER_PAGE:
            page_label_count = 1
            new_page_pending = True

        label_lines = build_label_lines(row)
        group_buffer.append(label_lines)

        # IF M = 3 OR ENDFILE THEN DO; ... END;
        if len(group_buffer) == LABEL_COLS or is_endfile:
            render_label_group(group_buffer, output_lines)
            if new_page_pending and output_lines:
                # Patch first line of the just-rendered group with ASA '1' (new page)
                first_idx = len(output_lines) - LABEL_ROWS
                if first_idx >= 0:
                    output_lines[first_idx] = "1" + output_lines[first_idx][1:]
                new_page_pending = False
            group_buffer = []

    # Write output file with ASA carriage control characters
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for line in output_lines:
            f.write(line + "\n")

    print(f"Label report written to: {OUTPUT_FILE}  ({len(output_lines)} lines)")


if __name__ == "__main__":
    main()
