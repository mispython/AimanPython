# !/usr/bin/env python3
"""
PROGRAM : EIBQADR5
PURPOSE : PRINT NAME LABELS FOR ADDR.HP (HIRE PURCHASE MEMBERS) IN POSTMAIL ORDER, 3-UP LABEL FORMAT.
          INPUT : NAME.LNNAME, ADDR.HP
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

PARQUET_NAME_LNNAME = INPUT_DIR / "name_lnname.parquet"
PARQUET_ADDR_HP     = INPUT_DIR / "addr_hp.parquet"

OUTPUT_FILE = OUTPUT_DIR / "EIBQADR5_labels.txt"

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
# Postmail (postcode) extraction
# Replicates SAS ARRAY/VERIFY loop over NAMELN1-NAMELN8.
# Note: NAMELN5 is COMPRESS(NAMELN5,'') before scanning (all spaces stripped)
# — handled during row transformation before this function is called.
# Returns right-justified 7-char string (mirrors SAS RIGHT()).
# ---------------------------------------------------------------------------
def extract_postmail(row: dict) -> str:
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
    return " "   # IF I > 8 THEN POSTMAIL = ' '


# ---------------------------------------------------------------------------
# ACCTCD derivation
# ACCTCD = REVERSE(PUT(ACCTNO,10.))
# ACCTCD = TRANSLATE(ACCTCD,'1234567890','0987654321')
# Zero-pad to 10 chars, reverse, then complement each digit (d -> 9-d).
# ---------------------------------------------------------------------------
def derive_acctcd(acctno) -> str:
    rev = str(int(acctno)).zfill(10)[::-1]
    return "".join(str(9 - int(c)) for c in rev)


def derive_tagln(branch, acctno) -> str:
    """PUT(BRANCH,Z3.) || '/' || ACCTCD"""
    return f"{str(int(branch)).zfill(3)}/{derive_acctcd(acctno)}"


# ---------------------------------------------------------------------------
# Build 7-line label block for one record
# Returns [tagln, nameln1, ..., nameln6]
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
# '//' -> double-space before = ASA '0' on first row of group
# '/'  -> single-space        = ASA ' ' on subsequent rows
# ---------------------------------------------------------------------------
def render_label_group(group: list, output: list) -> None:
    while len(group) < LABEL_COLS:
        group.append([" " * COL_WIDTH] * LABEL_ROWS)

    for row_idx in range(LABEL_ROWS):
        cc = "0" if row_idx == 0 else " "

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
    # DATA LNAME; SET NAME.LNNAME; DROP NAMELN1;
    lname = read_parquet(PARQUET_NAME_LNNAME)
    if "NAMELN1" in lname.columns:
        lname = lname.drop("NAMELN1")
    # PROC SORT BY ACCTNO
    lname = lname.sort("ACCTNO")

    # DATA HP; SET ADDR.HP; RENAME NAME=NAMELN1;
    # NOTE: BRANCH is NOT dropped here — kept for TAGLN derivation.
    hp_df = read_parquet(PARQUET_ADDR_HP)
    hp_df = hp_df.rename({"NAME": "NAMELN1"})
    # PROC SORT BY ACCTNO
    hp_df = hp_df.sort("ACCTNO")

    # DATA HP; MERGE LNAME HP(IN=A); BY ACCTNO; IF A;
    merged = hp_df.join(lname, on="ACCTNO", how="left", suffix="_LNAME")

    # Resolve NAMELN2-NAMELN8: prefer HP values; fill from LNAME if null
    for i in range(2, 9):
        col       = f"NAMELN{i}"
        col_lname = f"{col}_LNAME"
        if col_lname in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(col).is_null())
                  .then(pl.col(col_lname))
                  .otherwise(pl.col(col))
                  .alias(col)
            ).drop(col_lname)

    # IF SUBSTR(NAMELN4,1,8)='MALAYSIA' THEN NAMELN4='      '  (same for 5)
    # Note: EIBQADR5 checks 8 chars ('MALAYSIA') vs EIBQADR4's 6 chars ('MALAYS')
    for col in ("NAMELN4", "NAMELN5"):
        if col in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(col).cast(pl.Utf8).str.slice(0, 8) == "MALAYSIA")
                  .then(pl.lit("      "))
                  .otherwise(pl.col(col))
                  .alias(col)
            )

    # NAMELN5 = COMPRESS(NAMELN5,'')
    # SAS COMPRESS(str,'') removes all spaces from str
    if "NAMELN5" in merged.columns:
        merged = merged.with_columns(
            pl.col("NAMELN5").cast(pl.Utf8).str.replace_all(" ", "").alias("NAMELN5")
        )

    # Derive POSTMAIL, ACCTCD, TAGLN row-by-row
    rows      = merged.to_dicts()
    postmails = []
    taglns    = []
    for row in rows:
        postmails.append(extract_postmail(row))
        taglns.append(derive_tagln(row["BRANCH"], row["ACCTNO"]))

    merged = merged.with_columns([
        pl.Series("POSTMAIL", postmails),
        pl.Series("TAGLN",    taglns),
    ])

    # PROC SORT DATA=HP OUT=HP; BY POSTMAIL;
    merged = merged.sort("POSTMAIL")

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

        # ACCT+1; IF ACCT>24 THEN DO; ACCT=0; PUT _PAGE_; END;
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
