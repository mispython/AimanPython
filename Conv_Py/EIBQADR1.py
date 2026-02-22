# !/usr/bin/env python3
"""
PROGRAM : EIBQADR1
PURPOSE : PRINT NAME LABELS FOR ADDR.NEW (NEW MEMBERS) IN POSTCODE ORDER, 3-UP LABEL FORMAT.
          INPUT: ADDR.SAVINGS, ADDR.NEW                     
          OUTPUT: PRINT (LABEL REPORT .txt WITH ASA CC)
"""
# +--------------------------------------------------------------+

# +--------------------------------------------------------------+

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
PARQUET_ADDR_NEW     = INPUT_DIR / "addr_new.parquet"

OUTPUT_FILE = OUTPUT_DIR / "EIBQADR1_labels.txt"

# Page layout
PAGE_LENGTH      = 90    # PS=90 in OPTIONS
LABELS_PER_PAGE  = 24    # IF ACCT>24 THEN PUT _PAGE_
LABEL_ROWS       = 7     # rows per label block (TAGLN + NAMELN1-NAMELN6)
LABEL_COLS       = 3     # 3-up labels
# Column start positions (1-based in SAS: @1, @43, @85)
COL_POSITIONS    = [0, 42, 84]   # 0-based
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
# Replicates the SAS ARRAY/VERIFY loop that scans NAMELN1-NAMELN8 for a
# field whose first 5 chars are all digits, then captures 5, 6, or 7-char
# postcode depending on subsequent characters.
# ---------------------------------------------------------------------------
def extract_postcode(row: dict) -> str:
    """
    Scan NAMELN1..NAMELN8 for a line whose first 5 chars are all digits.
    Capture 5, 6, or 7 char postcode based on position 6/7/8 being non-digit.
    Return right-justified 7-char string (RIGHT() in SAS pads left with spaces).
    """
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
# i.e. reverse the 10-char zero-padded account number,
# then swap each digit d -> (9 - d as digit char)
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
# Build the label record for a single row
# ---------------------------------------------------------------------------
def build_label_lines(row: dict) -> list:
    """Return list of 7 strings (TAGLN, NAMELN1..NAMELN6), each max 40 chars."""
    tagln    = str(row.get("TAGLN", "") or "").ljust(COL_WIDTH)[:COL_WIDTH]
    fields   = [tagln]
    for i in range(1, 7):
        val  = str(row.get(f"NAMELN{i}", "") or "").ljust(COL_WIDTH)[:COL_WIDTH]
        fields.append(val)
    return fields


# ---------------------------------------------------------------------------
# ASA carriage-control helpers
# ASA codes: '1'=new page, ' '=single space, '0'=double space
# PUT // @1 ... means: double-space (blank line) then print
# PUT /  @1 ... means: single newline then print
# ---------------------------------------------------------------------------
def asa(cc: str, text: str = "") -> str:
    return cc + text


# ---------------------------------------------------------------------------
# Render one group of up to 3 labels into report lines (with ASA CC)
# Mirrors: PUT // @1 LINE1(1) @43 LINE1(2) @85 LINE1(3)
#               / @1 LINE2(1) @43 LINE2(2) @85 LINE2(3) ... (7 rows)
# ---------------------------------------------------------------------------
def render_label_group(
    group: list,       # list of up to 3 label-records, each a list of 7 strings
    output: list,
    first_on_page: bool,
) -> None:
    """
    group  : up to 3 elements, each = [tagln, nameln1..nameln6]
    output : accumulator list of ASA-prefixed strings
    """
    # Pad group to always 3 slots (empty labels)
    while len(group) < LABEL_COLS:
        group.append([" " * COL_WIDTH] * LABEL_ROWS)

    for row_idx in range(LABEL_ROWS):
        # First row of label group uses '//' (double space = blank line before)
        if row_idx == 0:
            cc = "0"   # ASA '0' = double space (skip one blank line before)
        else:
            cc = " "   # ASA ' ' = single space

        line_content = ""
        for col_idx in range(LABEL_COLS):
            start_pos    = COL_POSITIONS[col_idx]
            cell_text    = group[col_idx][row_idx].ljust(COL_WIDTH)[:COL_WIDTH]
            # Pad line_content to reach start_pos
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

    # DATA NEW; DROP BRANCH; SET ADDR.NEW; RENAME NAME=NAMELN1;
    new_df = read_parquet(PARQUET_ADDR_NEW)
    branch_col = new_df["BRANCH"].to_list()   # save before drop
    new_df     = new_df.drop("BRANCH")
    new_df     = new_df.rename({"NAME": "NAMELN1"})
    # Restore BRANCH temporarily for TAGLN derivation; it was dropped in SAS too,
    # but TAGLN = PUT(BRANCH,Z3.)||'/'||ACCTCD uses original BRANCH before drop.
    new_df = new_df.with_columns(pl.Series("BRANCH_ORIG", branch_col))
    # PROC SORT BY ACCTNO
    new_df = new_df.sort("ACCTNO")

    # DATA NEW; DROP POSTCODE; MERGE ADDR NEW(IN=A); BY ACCTNO; IF A;
    merged = new_df.join(addr, on="ACCTNO", how="left", suffix="_ADDR")

    # Resolve NAMELN columns: prefer NEW values, fill from ADDR if missing
    for i in range(2, 9):
        col = f"NAMELN{i}"
        col_addr = f"{col}_ADDR"
        if col_addr in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(col).is_null())
                  .then(pl.col(col_addr))
                  .otherwise(pl.col(col))
                  .alias(col)
            ).drop(col_addr)

    # Clean NAMELN4/5/6: if first 6 chars == 'MALAYS' set to blank
    for col in ("NAMELN4", "NAMELN5", "NAMELN6"):
        if col in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(col).cast(pl.Utf8).str.slice(0, 6) == "MALAYS")
                  .then(pl.lit("   "))
                  .otherwise(pl.col(col))
                  .alias(col)
            )

    # Drop POSTCODE (from ADDR merge); will be re-derived
    if "POSTCODE" in merged.columns:
        merged = merged.drop("POSTCODE")

    # Derive POSTCODE, ACCTCD, TAGLN
    rows = merged.to_dicts()
    postcodes, taglns = [], []
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
    output_lines = []
    # First line of output uses '1' (new page) to start
    page_label_count = 0
    group_buffer     = []   # collects up to 3 label records before printing
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

        # IF M = 3 OR ENDFILE THEN DO; print group
        if len(group_buffer) == LABEL_COLS or is_endfile:
            if new_page_pending:
                # Mark first line of group with '1' (new page ASA)
                # We handle this by overriding the CC of the first output line of this group
                render_label_group(group_buffer, output_lines, first_on_page=True)
                # Patch the first line of this group to use '1' (new page)
                if output_lines:
                    last_group_start = -(LABEL_ROWS)
                    # Find first line of just-appended group
                    first_idx = len(output_lines) - LABEL_ROWS
                    if first_idx >= 0:
                        line = output_lines[first_idx]
                        output_lines[first_idx] = "1" + line[1:]
                new_page_pending = False
            else:
                render_label_group(group_buffer, output_lines, first_on_page=False)
            group_buffer = []

    # Write output file with ASA carriage control characters
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for line in output_lines:
            f.write(line + "\n")

    print(f"Label report written to: {OUTPUT_FILE}  ({len(output_lines)} lines)")


if __name__ == "__main__":
    main()
