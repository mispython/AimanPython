#!/usr/bin/env python3
"""
Program  : EIBMIRAT.py
Date     : 17 September 2001
Purpose  : Deposits, Average Rate on All Islamic Fixed Deposit Products
            on Monthly Basis. (Refer SMRA275)
           Based on average of 4 weekly extractions.
"""

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR      = Path("/data")
FD_DIR        = BASE_DIR / "fd"
FD1_DIR       = BASE_DIR / "fd1"
FD2_DIR       = BASE_DIR / "fd2"
FD3_DIR       = BASE_DIR / "fd3"

REPTDATE_FILE = FD_DIR / "reptdate.parquet"
FD_FILE       = FD_DIR / "fd.parquet"
FD1_FILE      = FD1_DIR / "fd.parquet"
FD2_FILE      = FD2_DIR / "fd.parquet"
FD3_FILE      = FD3_DIR / "fd.parquet"
BRHFILE       = BASE_DIR / "brhfile.txt"   # fixed-width branch code file

OUTPUT_DIR    = BASE_DIR / "output"
OUTPUT_REPORT = OUTPUT_DIR / "eibmirat_report.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# REPORT / PAGE CONSTANTS
# ============================================================================

PAGE_LENGTH = 60
LINE_WIDTH  = 132

# ============================================================================
# INTPLAN -> ITEM MAPPING
# ============================================================================

INTPLAN_ITEM: dict[int, str] = {
    340: "01 MTH",   448: "01 MTH",
    352: "02 MTHS",  449: "02 MTHS",
    341: "03 MTHS",  450: "03 MTHS",
    353: "04 MTHS",  451: "04 MTHS",
    354: "05 MTHS",  452: "05 MTHS",
    342: "06 MTHS",  453: "06 MTHS",
    355: "07 MTHS",  454: "07 MTHS",
    356: "08 MTHS",  455: "08 MTHS",
    343: "09 MTHS",  456: "09 MTHS",
    357: "10 MTHS",  457: "10 MTHS",
    358: "11 MTHS",  458: "11 MTHS",
    344: "12 MTHS",  459: "12 MTHS",
    588: "13 MTHS",  461: "13 MTHS",
    589: "14 MTHS",  462: "14 MTHS",
    345: "15 MTHS",  463: "15 MTHS",
    590: "16 MTHS",
    591: "17 MTHS",
    346: "18 MTHS",  464: "18 MTHS",
    592: "19 MTHS",
    593: "20 MTHS",
    347: "21 MTHS",  465: "21 MTHS",
    594: "22 MTHS",
    595: "23 MTHS",
    348: "24 MTHS",  466: "24 MTHS",
    596: "25 MTHS",
    597: "26 MTHS",
    359: "27 MTHS",
    598: "28 MTHS",
    599: "29 MTHS",
    580: "30 MTHS",
    581: "33 MTHS",
    349: "36 MTHS",  467: "36 MTHS",
    582: "39 MTHS",
    583: "42 MTHS",
    584: "45 MTHS",
    350: "48 MTHS",  468: "48 MTHS",
    585: "51 MTHS",
    586: "54 MTHS",
    587: "57 MTHS",
    351: "60 MTHS",  469: "60 MTHS",
}

# Ordered ITEM values for consistent sort
ITEM_ORDER = [
    "01 MTH",  "02 MTHS", "03 MTHS", "04 MTHS", "05 MTHS",
    "06 MTHS", "07 MTHS", "08 MTHS", "09 MTHS", "10 MTHS",
    "11 MTHS", "12 MTHS", "13 MTHS", "14 MTHS", "15 MTHS",
    "16 MTHS", "17 MTHS", "18 MTHS", "19 MTHS", "20 MTHS",
    "21 MTHS", "22 MTHS", "23 MTHS", "24 MTHS", "25 MTHS",
    "26 MTHS", "27 MTHS", "28 MTHS", "29 MTHS", "30 MTHS",
    "33 MTHS", "36 MTHS", "39 MTHS", "42 MTHS", "45 MTHS",
    "48 MTHS", "51 MTHS", "54 MTHS", "57 MTHS", "60 MTHS",
]

ITEM_RANK = {item: idx for idx, item in enumerate(ITEM_ORDER)}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def fmt_ddmmyy8(d: date) -> str:
    """Format date as DD/MM/YY (SAS DDMMYY8.)"""
    return d.strftime("%d/%m/%y")


def fmt_year2(d: date) -> str:
    """Format year as 2-digit (SAS YEAR2.)"""
    return d.strftime("%y")


def fmt_year4(d: date) -> str:
    """Format year as 4-digit (SAS YEAR4.)"""
    return d.strftime("%Y")


def fmt_z2(n: int) -> str:
    return f"{n:02d}"


def fmt_comma18_2(val) -> str:
    if val is None:
        return " " * 18
    try:
        return f"{float(val):,.2f}".rjust(18)
    except (TypeError, ValueError):
        return " " * 18


def fmt_8_5(val) -> str:
    """Format as 8.5 (8 wide, 5 decimal places)"""
    if val is None:
        return " " * 8
    try:
        return f"{float(val):.5f}".rjust(8)
    except (TypeError, ValueError):
        return " " * 8


# ============================================================================
# STEP 1: READ REPTDATE & DERIVE MACRO VARIABLES
# ============================================================================

con = duckdb.connect()

reptdate_df = con.execute(f"SELECT * FROM read_parquet('{REPTDATE_FILE}')").pl()
row_rep = reptdate_df.row(0, named=True)

REPTDATE: date = row_rep["REPTDATE"]
if isinstance(REPTDATE, datetime):
    REPTDATE = REPTDATE.date()

day = REPTDATE.day
if day == 8:
    NOWK = "1"
elif day == 15:
    NOWK = "2"
elif day == 22:
    NOWK = "3"
else:
    NOWK = "4"

REPTYRS  = fmt_year2(REPTDATE)
REPTYEAR = fmt_year4(REPTDATE)
REPTMON  = fmt_z2(REPTDATE.month)
REPTDAY  = fmt_z2(day)
RDATE    = fmt_ddmmyy8(REPTDATE)

TODAY_STR = fmt_ddmmyy8(date.today())

# ============================================================================
# STEP 2: LOAD ALL FOUR WEEKLY FD EXTRACTIONS
# ============================================================================

fd_all_query = f"""
SELECT * FROM read_parquet('{FD_FILE}')
UNION ALL
SELECT * FROM read_parquet('{FD1_FILE}')
UNION ALL
SELECT * FROM read_parquet('{FD2_FILE}')
UNION ALL
SELECT * FROM read_parquet('{FD3_FILE}')
"""
fd_raw = con.execute(fd_all_query).pl()

# ============================================================================
# STEP 3: MAP INTPLAN -> ITEM, COMPUTE INTEREST, FILTER
# ============================================================================

def map_item(intplan_val) -> str:
    if intplan_val is None:
        return ""
    try:
        return INTPLAN_ITEM.get(int(intplan_val), "")
    except (TypeError, ValueError):
        return ""


fd_raw = fd_raw.with_columns([
    pl.col("INTPLAN").map_elements(map_item, return_dtype=pl.Utf8).alias("ITEM"),
    ((pl.col("RATE") * pl.col("CURBAL")) / 100.0).alias("INTEREST"),
])

# Filter: OPENIND IN ('D','O') and ITEM is not blank
fd_raw = fd_raw.filter(
    pl.col("OPENIND").cast(pl.Utf8).is_in(["D", "O"]) &
    (pl.col("ITEM").str.strip_chars() != "")
)

# ============================================================================
# STEP 4: LOAD BRHFILE (fixed-width: col 2-4 = BRANCH 3., col 6-8 = BRHCODE $3.)
# ============================================================================

brhdata_rows = []
if BRHFILE.exists():
    with open(BRHFILE, "r", encoding="utf-8") as bf:
        for line in bf:
            # SAS INPUT @2 BRANCH 3. @6 BRHCODE $3.
            # 1-based positions -> 0-based: BRANCH = [1:4], BRHCODE = [5:8]
            branch_str  = line[1:4].strip()  if len(line) >= 4 else ""
            brhcode_str = line[5:8].strip()  if len(line) >= 8 else ""
            if branch_str:
                try:
                    brhdata_rows.append({
                        "BRANCH":  int(branch_str),
                        "BRHCODE": brhcode_str,
                    })
                except ValueError:
                    pass

brhdata = pl.DataFrame(brhdata_rows) if brhdata_rows else pl.DataFrame(
    {"BRANCH": pl.Series([], dtype=pl.Int64),
     "BRHCODE": pl.Series([], dtype=pl.Utf8)}
)

# ============================================================================
# STEP 5: MERGE FD WITH BRHDATA (inner join — IF PRESENT=1)
# ============================================================================

fd = fd_raw.join(brhdata, on="BRANCH", how="inner")

# ============================================================================
# STEP 6: SORT BY BRANCH, ITEM (using ITEM_RANK for correct tenure order)
# ============================================================================

fd = fd.with_columns(
    pl.col("ITEM").map_elements(
        lambda x: ITEM_RANK.get(x.strip(), 999), return_dtype=pl.Int32
    ).alias("_ITEM_RANK")
)
fd = fd.sort(["BRANCH", "_ITEM_RANK"])

# ============================================================================
# STEP 7: PRODUCE REPORT (replicate DATA WRITE with FILE PRINT, HEADER=NEWPAGE)
# ============================================================================

def write_report(df: pl.DataFrame, output_path: Path) -> None:
    """
    Replicate the SAS DATA WRITE step with PUT statements, FILE PRINT,
    HEADER=NEWPAGE, and BY BRANCH ITEM group totalling.
    ASA carriage control: '1' = form-feed, ' ' = advance one line.
    """
    lines:     list[str] = []
    page_cnt   = 0
    line_count = PAGE_LENGTH  # force header on first record

    # Separator lines
    dash_line  = "-" * 115
    equal_line = "=" * 115

    def new_page() -> None:
        nonlocal page_cnt, line_count
        page_cnt += 1
        # ASA '1' = form-feed
        lines.append(
            f"1"
            f"{'P U B L I C   B A N K   B E R H A D':>70}"
            f"{'PAGE NO : ' + str(page_cnt):>20}"
        )
        lines.append(
            f" "
            f"{'AVERAGE RATE ON ISLAMIC FIXED DEPOSIT PRODUCTS ON MONTHLY':>70}"
            f"{'BASIS AS AT : ' + RDATE:>35}"
            f"{'DATE    : ' + TODAY_STR:>20}"
        )
        lines.append(
            f" "
            f"{'(BASE ON : AVERAGE OF 4 WEEKLY EXTRACTIONS)':>60}"
        )
        lines.append(f" {'AL-MUDHARABAH GENERAL':>50}")
        lines.append(f" ")
        lines.append(
            f" "
            f"{'BRANCH':<10}"
            f"{'BRANCH CODE':<15}"
            f"{'INVESTMENT(MGIA)':<20}"
            f"{'AMOUNT':>18}"
            f"{'INTEREST / YEAR':>18}"
            f"{'AVERAGE INTEREST':>18}"
        )
        lines.append(
            f" "
            f"{'______':<10}"
            f"{'___________':<15}"
            f"{'________________':<20}"
            f"{'______':>18}"
            f"{'_______________':>18}"
            f"{'________________':>18}"
        )
        lines.append(f" ")
        line_count = 8

    # Accumulators
    amt_item  = 0.0
    int_item  = 0.0
    amt_brch  = 0.0
    int_brch  = 0.0
    amt_bank  = 0.0
    int_bank  = 0.0

    prev_branch = None
    prev_item   = None
    first_in_item = True

    rows = df.to_dicts()
    total_rows = len(rows)

    for idx, r in enumerate(rows):
        branch  = r.get("BRANCH")
        brhcode = str(r.get("BRHCODE") or "")
        item    = str(r.get("ITEM")    or "")
        curbal  = float(r.get("CURBAL")   or 0.0)
        rate    = float(r.get("RATE")     or 0.0)
        interest = (rate * curbal) / 100.0

        is_first_item   = (branch != prev_branch) or (item != prev_item)
        is_last_row     = (idx == total_rows - 1)
        next_r          = rows[idx + 1] if not is_last_row else None
        is_last_item    = is_last_row or (
            next_r is not None and (
                next_r.get("BRANCH") != branch or next_r.get("ITEM") != item
            )
        )
        is_last_branch  = is_last_row or (
            next_r is not None and next_r.get("BRANCH") != branch
        )

        # Accumulate
        amt_item += curbal
        int_item += interest
        amt_brch += curbal
        int_brch += interest
        amt_bank += curbal
        int_bank += interest

        # IF FIRST.ITEM: print BRANCH, BRHCODE, ITEM prefix
        if is_first_item:
            if line_count >= PAGE_LENGTH - 4:
                new_page()
            lines.append(
                f" "
                f"{str(branch):<10}"
                f"{brhcode:<15}"
                f"{item:<20}"
            )
            line_count += 1

        # IF LAST.ITEM: compute avg, print amounts
        if is_last_item:
            avgrate  = (int_item / amt_item * 100) if amt_item != 0 else 0.0
            amt_item_out = amt_item / 4
            int_item_out = int_item / 4
            # Append amounts to the last printed line
            if lines:
                last_line = lines[-1].rstrip()
                lines[-1] = (
                    last_line
                    + f"{fmt_comma18_2(amt_item_out):>18}"
                    + f"  {fmt_comma18_2(int_item_out):>18}"
                    + f"  {fmt_8_5(avgrate):>8}"
                )
            amt_item = 0.0
            int_item = 0.0

        # IF LAST.BRANCH: print branch total
        if is_last_branch:
            avgrate  = (int_brch / amt_brch * 100) if amt_brch != 0 else 0.0
            amt_brch_out = amt_brch / 4
            int_brch_out = int_brch / 4

            if line_count >= PAGE_LENGTH - 4:
                new_page()

            lines.append(f" {dash_line}")
            lines.append(
                f" "
                f"{'BRANCH TOTAL : ':>40}"
                f"{fmt_comma18_2(amt_brch_out):>18}"
                f"  {fmt_comma18_2(int_brch_out):>18}"
                f"  {fmt_8_5(avgrate):>8}"
            )
            lines.append(f" ")
            lines.append(f" ")
            line_count += 4
            amt_brch = 0.0
            int_brch = 0.0

        # EOF: print company total
        if is_last_row:
            avgrate  = (int_bank / amt_bank * 100) if amt_bank != 0 else 0.0
            amt_bank_out = amt_bank / 4
            int_bank_out = int_bank / 4

            if line_count >= PAGE_LENGTH - 4:
                new_page()

            lines.append(f" {equal_line}")
            lines.append(
                f" "
                f"{'COMPANY TOTAL : ':>40}"
                f"{fmt_comma18_2(amt_bank_out):>18}"
                f"  {fmt_comma18_2(int_bank_out):>18}"
                f"  {fmt_8_5(avgrate):>8}"
            )
            line_count += 2

        prev_branch = branch
        prev_item   = item

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


write_report(fd, OUTPUT_REPORT)

print(f"Report written to: {OUTPUT_REPORT}")
