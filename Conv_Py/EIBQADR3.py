# !/usr/bin/env python3
"""
PROGRAM : EIBQADR3
PURPOSE : PRINT NAME LABELS FOR ADDR.P50 (50 PLUS SAVINGS ACCOUNT MEMBERS) IN POSTCODE ORDER, 3-UP LABEL FORMAT.
          INPUT : ADDR.SAVINGS, ADDR.P50
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
PARQUET_ADDR_P50     = INPUT_DIR / "addr_p50.parquet"

OUTPUT_FILE = OUTPUT_DIR / "EIBQADR3_labels.txt"

# Page / label layout constants
PAGE_LENGTH     = 90    # PS=90
LABELS_PER_PAGE = 24    # IF ACCT > 24 THEN PUT _PAGE_
LABEL_ROWS      = 7     # TAGLN + NAMELN1-NAMELN6
LABEL_COLS      = 3     # 3-up labels per row
# SAS @1, @43, @85 -> 0-based offsets 0, 42, 84
COL_POSITIONS   = [0, 42, 84]
COL_WIDTH       = 40


# ---------------------------------------------------------------------------
# Helper: read parquet via DuckDB -> Polars DataFrame
# ---------------------------------------------------------------------------
def read_parquet(path: Path) -> pl.DataFrame:
    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM '{path}'").pl()
    con.close()
    return df


# ---------------------------------------------------------------------------
# Postcode extraction
# Replicates SAS ARRAY/VERIFY loop over NAMELN1-NAMELN8.
# Scans each line; if first 5 chars are all digits, captures 5-, 6-, or 7-char
# postcode depending on character at position 6/7/8 being non-digit.
# Returns right-justified 7-char string (mirrors SAS RIGHT()).
# ---------------------------------------------------------------------------
def extract_postcode(row: dict) -> str:
    digits = set("0123456789")
    for i in range(1, 9):
        val    = str(row.get(f"NAMELN{i}") or "").ljust(40)
        first5 = val[:5]
        if len(first5) == 5 and all(c in digits for c in first5):
            c6 = val[5:6]
            c7 = val[6:7]
            c8 = val[7:8]
            if not c6 or c6 not in digits:
                return first5.rjust(7)
            elif not c7 or c7 not in digits:
                return val[:6].rjust(7)
            elif not c8 or c8 not in digits:
                return val[:7].rjust(7)
    return " "   # IF I > 8 THEN POSTCODE = ' '


# ---------------------------------------------------------------------------
# ACCTCD derivation
# ACCTCD = REVERSE(PUT(ACCTNO,10.))
# ACCTCD = TRANSLATE(ACCTCD,'1234567890','0987654321')
# Zero-pad to 10 chars, reverse, then complement each digit (d -> 9-d).
# ---------------------------------------------------------------------------
def derive_acctcd(acctno) -> str:
    rev     = str(int(acctno)).zfill(10)[::-1]
    return "".join(str(9 - int(c)) for c in rev)


def derive_tagln(branch, acctno) -> str:
    """PUT(BRANCH,Z3.) || '/' || ACCTCD"""
    return f"{str(int(branch)).zfill(3)}/{derive_acctcd(acctno)}"


# ---------------------------------------------------------------------------
# Build 7-line label block for one record
# Returns [tagln, nameln1, nameln2, nameln3, nameln4, nameln5, nameln6]
# ---------------------------------------------------------------------------
def build_label_lines(row: dict) -> list:
    lines = [str(row.get("TAGLN") or "").ljust(COL_WIDTH)[:COL_WIDTH]]
    for i in range(1, 7):
        lines.append(str(row.get(f"NAMELN{i}") or "").ljust(COL_WIDTH)[:COL_WIDTH])
    return lines


# ---------------------------------------------------------------------------
# ASA carriage-control helper
# ---------------------------------------------------------------------------
def asa(cc: str, text: str = "") -> str:
    return cc + text


# ---------------------------------------------------------------------------
# Render a group of up to 3 label records into ASA-prefixed output lines.
# Mirrors SAS:
#   PUT // @1 LINE1(1) @43 LINE1(2) @85 LINE1(3)
#        /  @1 LINE2(1) ...  (7 rows total)
# '//' -> double-space before = ASA '0' on first row
# '/'  -> single-space        = ASA ' ' on subsequent rows
# ---------------------------------------------------------------------------
def render_label_group(group: list, output: list) -> None:
    # Pad group to exactly LABEL_COLS slots with blank labels
    while len(group) < LABEL_COLS:
        group.append([" " * COL_WIDTH] * LABEL_ROWS)

    for row_idx in range(LABEL_ROWS):
        cc = "0" if row_idx == 0 else " "   # '//' = double space for row 0

        line_content = ""
        for col_idx in range(LABEL_COLS):
            start = COL_POSITIONS[col_idx]
            cell  = group[col_idx][row_idx].ljust(COL_WIDTH)[:COL_WIDTH]
            line_content = line_content.ljust(start) + cell

        output.append(asa(cc, line_content))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    # DATA ADDR; SET ADDR.SAVINGS; DROP NAMELN1;
    addr = read_parquet(PARQUET_ADDR_SAVINGS)
    if "NAMELN1" in addr.columns:
        addr = addr.drop("NAMELN1")
    # PROC SORT BY ACCTNO
    addr = addr.sort("ACCTNO")

    # DATA P50; DROP BRANCH; SET ADDR.P50; RENAME NAME=NAMELN1;
    p50_df     = read_parquet(PARQUET_ADDR_P50)
    branch_col = p50_df["BRANCH"].to_list()   # preserve for TAGLN before DROP
    p50_df     = p50_df.drop("BRANCH")
    p50_df     = p50_df.rename({"NAME": "NAMELN1"})
    p50_df     = p50_df.with_columns(pl.Series("BRANCH_ORIG", branch_col))
    # PROC SORT BY ACCTNO
    p50_df = p50_df.sort("ACCTNO")

    # DATA P50; DROP POSTCODE; MERGE ADDR P50(IN=A); BY ACCTNO; IF A;
    merged = p50_df.join(addr, on="ACCTNO", how="left", suffix="_ADDR")

    # Resolve NAMELN2-NAMELN8: prefer P50 values; fill from ADDR if null
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

    # IF SUBSTR(NAMELN4,1,6)='MALAYS' THEN NAMELN4='   '  (same for 5, 6)
    for col in ("NAMELN4", "NAMELN5", "NAMELN6"):
        if col in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(col).cast(pl.Utf8).str.slice(0, 6) == "MALAYS")
                  .then(pl.lit("   "))
                  .otherwise(pl.col(col))
                  .alias(col)
            )

    # DROP POSTCODE (comes from ADDR); will be re-derived below
    if "POSTCODE" in merged.columns:
        merged = merged.drop("POSTCODE")

    # Derive POSTCODE, ACCTCD, TAGLN row-by-row
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

    # ------------------------------------------------
    #  PRINT NAME LABELS IN POSTCODE ORDER
    # ------------------------------------------------
    # TITLE;  (no title)
    output_lines     = []
    page_label_count = 0
    group_buffer     = []
    new_page_pending = True

    rows_sorted = merged.to_dicts()
    total_rows  = len(rows_sorted)

    for idx, row in enumerate(rows_sorted):
        is_endfile = (idx == total_rows - 1)

        # ACCT+1; IF ACCT > 24 THEN DO; PUT _PAGE_; ACCT=0; END;
        page_label_count += 1
        if page_label_count > LABELS_PER_PAGE:
            page_label_count = 1
            new_page_pending = True

        group_buffer.append(build_label_lines(row))

        # IF M = 3 OR ENDFILE THEN DO; ... END;
        if len(group_buffer) == LABEL_COLS or is_endfile:
            render_label_group(group_buffer, output_lines)
            if new_page_pending and output_lines:
                # Patch first line of this group with ASA '1' (new page / PUT _PAGE_)
                first_idx = len(output_lines) - LABEL_ROWS
                if first_idx >= 0:
                    output_lines[first_idx] = "1" + output_lines[first_idx][1:]
                new_page_pending = False
            group_buffer = []

    # Write output file with ASA carriage-control characters
    with open(OUTPUT_FILE, "w", encoding="utf-8") as fh:
        for line in output_lines:
            fh.write(line + "\n")

    print(f"Label report written to: {OUTPUT_FILE}  ({len(output_lines)} lines)")


if __name__ == "__main__":
    main()
