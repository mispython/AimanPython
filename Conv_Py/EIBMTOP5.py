# !/usr/bin/env python3
"""
Program : EIBMTOP5
Purpose : Produce report of PB Subsidiaries under Top 50 Corporate Depositors.
          Extracts current account (CA) and fixed deposit (FD) records for
            specific CIS customer numbers, merges with deposit master data,
            and produces a grouped PROC PRINT-style report with subtotals.

Dependencies:
  %INC PGM(PBBLNFMT) -> loan format definitions
  %INC PGM(PBBELF)   -> EL/ELI BNM code definitions
  %INC PGM(PBBDPFMT) -> deposit product format definitions
  Note: None of the above format modules' functions are directly invoked in
        this program; they are loaded into the SAS session for potential use.
"""

import os
import sys
from datetime import date, datetime

import duckdb
import polars as pl

# ==============================================================================
# PATH CONFIGURATION
# ==============================================================================

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR  = os.path.join(BASE_DIR, "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Input parquet paths
DEPOSIT_REPTDATE_PARQUET = os.path.join(INPUT_DIR, "DEPOSIT_REPTDATE.parquet")  # DEPOSIT.REPTDATE
CISLN_DEPOSIT_PARQUET    = os.path.join(INPUT_DIR, "CISLN_DEPOSIT.parquet")     # CISLN.DEPOSIT
CISDP_DEPOSIT_PARQUET    = os.path.join(INPUT_DIR, "CISDP_DEPOSIT.parquet")     # CISDP.DEPOSIT
DEPOSIT_CURRENT_PARQUET  = os.path.join(INPUT_DIR, "DEPOSIT_CURRENT.parquet")   # DEPOSIT.CURRENT
DEPOSIT_FD_PARQUET       = os.path.join(INPUT_DIR, "DEPOSIT_FD.parquet")        # DEPOSIT.FD

# Output report path  (PROC PRINTTO PRINT=FD2TEXT NEW)
FD2TEXT_OUT = os.path.join(OUTPUT_DIR, "FD2TEXT.txt")

# OPTIONS YEARCUTOFF=1950
YEARCUTOFF = 1950

# Page length  (OPTIONS PS=60)
PAGE_LENGTH = 60

# Line size  (OPTIONS LS=132)
LINE_SIZE = 132

# ==============================================================================
# CIS CUSTOMER NUMBERS TO REPORT
# IF CUSTNO IN (53227,169990,170108,3562038,3721354);
# ==============================================================================

REPORT_CUSTNOS: set[int] = {53227, 169990, 170108, 3562038, 3721354}

# ==============================================================================
# PRODUCT EXCLUSIONS
# CA: PRODUCT NOT IN (400,401,402,403,404,405,406,407,408,409,410)
# FD: PRODUCT NOT IN (350,351,352,353,354,355,356,357)
# ==============================================================================

CA_EXCL_PRODUCTS: set[int] = {400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410}
FD_EXCL_PRODUCTS: set[int] = {350, 351, 352, 353, 354, 355, 356, 357}

# ==============================================================================
# HELPERS
# ==============================================================================

SAS_EPOCH = date(1960, 1, 1)


def sas_date_to_python(val) -> date | None:
    """Convert SAS internal date integer (days since 1960-01-01) to Python date."""
    if val is None:
        return None
    try:
        from datetime import timedelta
        return SAS_EPOCH + timedelta(days=int(val))
    except (TypeError, ValueError):
        return None


def read_reptdate(con: duckdb.DuckDBPyConnection) -> date:
    """
    DATA REPTDATE; SET DEPOSIT.REPTDATE;
    Read the single REPTDATE value and return as Python date.
    """
    df = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{DEPOSIT_REPTDATE_PARQUET}') LIMIT 1"
    ).pl()
    if df.is_empty():
        raise ValueError(f"No REPTDATE found in: {DEPOSIT_REPTDATE_PARQUET}")

    val = df["REPTDATE"][0]
    if isinstance(val, date):
        return val
    if isinstance(val, int):
        return sas_date_to_python(val)
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(str(val), fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Cannot parse REPTDATE: {val!r}")


def derive_macros(reptdate: date) -> dict:
    """
    DATA REPTDATE; SET DEPOSIT.REPTDATE;
      SELECT(DAY(REPTDATE));
        WHEN (8)  CALL SYMPUT('NOWK','1');
        WHEN (15) CALL SYMPUT('NOWK','2');
        WHEN (22) CALL SYMPUT('NOWK','3');
        OTHERWISE CALL SYMPUT('NOWK','4');
      END;
      CALL SYMPUT('REPTYEAR', PUT(REPTDATE,YEAR4.));
      CALL SYMPUT('REPTMON',  PUT(MONTH(REPTDATE),Z2.));
      CALL SYMPUT('REPTDAY',  PUT(DAY(REPTDATE),Z2.));
      CALL SYMPUT('RDATE',    PUT(REPTDATE,DDMMYY8.));
    """
    day = reptdate.day
    if day == 8:
        nowk = "1"
    elif day == 15:
        nowk = "2"
    elif day == 22:
        nowk = "3"
    else:
        nowk = "4"

    return {
        "NOWK":     nowk,
        "REPTYEAR": str(reptdate.year),
        "REPTMON":  f"{reptdate.month:02d}",
        "REPTDAY":  f"{reptdate.day:02d}",
        # DDMMYY8. format: DD/MM/YY  (8 characters)
        "RDATE":    reptdate.strftime("%d/%m/%y"),
        # Full 4-digit year version used in report titles for clarity
        "RDATE_FULL": reptdate.strftime("%d/%m/%Y"),
    }


def format_comma16_2(val) -> str:
    """
    FORMAT CURBAL COMMA16.2  — 16-character field with 2 decimal places,
    comma-thousands separator, right-justified.
    SAS COMMA16.2: width 16 including sign, commas and decimal point.
    """
    if val is None:
        return " " * 16
    try:
        fval = float(val)
    except (TypeError, ValueError):
        return " " * 16
    # Format with commas and 2 decimal places
    formatted = f"{fval:,.2f}"
    # Right-justify within width 16
    return formatted.rjust(16)


# ==============================================================================
# DATA PROCESSING
# ==============================================================================

def build_caorg(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """
    Build CAORG dataset — current account records.

    DATA CISCA;
      SET CISLN.DEPOSIT;
      IF (3000000000<=ACCTNO<=3999999999);
    PROC SORT DATA=CISCA; BY ACCTNO;
    PROC SORT DATA=DEPOSIT.CURRENT OUT=CA; BY ACCTNO;
    DATA CAORG;
      MERGE CA(IN=A) CISCA; BY ACCTNO;
      IF A AND PURPOSE NE '2' AND
         PRODUCT NOT IN (400,401,402,403,404,405,406,407,408,409,410);
      CABAL = CURBAL;
      IF INDORG = 'O';
    """
    # CISCA: filter CISLN.DEPOSIT for ACCTNO range 3000000000-3999999999
    cisca = con.execute(f"""
        SELECT *
        FROM read_parquet('{CISLN_DEPOSIT_PARQUET}')
        WHERE ACCTNO BETWEEN 3000000000 AND 3999999999
    """).pl()

    # CA: DEPOSIT.CURRENT (all rows)
    ca = con.execute(f"""
        SELECT *
        FROM read_parquet('{DEPOSIT_CURRENT_PARQUET}')
    """).pl()

    # MERGE CA(IN=A) CISCA; BY ACCTNO; IF A
    # Left join: keep all CA rows; bring in matching CISCA columns
    # Determine which columns come from CISCA vs CA to handle overlaps
    cisca_cols = set(cisca.columns)
    ca_cols    = set(ca.columns)
    overlap    = cisca_cols & ca_cols - {"ACCTNO"}

    if overlap:
        cisca_renamed = cisca.rename({c: f"{c}_CIS" for c in overlap})
    else:
        cisca_renamed = cisca

    caorg = ca.join(cisca_renamed, on="ACCTNO", how="left")

    # IF A AND PURPOSE NE '2' AND PRODUCT NOT IN (...)
    # IF INDORG = 'O'
    excl = CA_EXCL_PRODUCTS
    caorg = caorg.filter(
        (pl.col("PURPOSE").cast(pl.Utf8) != "2") &
        (~pl.col("PRODUCT").cast(pl.Int64).is_in(list(excl))) &
        (pl.col("INDORG").cast(pl.Utf8) == "O")
    )

    # CABAL = CURBAL
    caorg = caorg.with_columns(
        pl.col("CURBAL").alias("CABAL")
    )

    return caorg


def build_fdorg(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """
    Build FDORG dataset — fixed deposit records.

    DATA CISFD;
      SET CISDP.DEPOSIT;
      IF (1000000000<=ACCTNO<=1999999999) OR
         (7000000000<=ACCTNO<=7999999999);
    PROC SORT DATA=CISFD;             BY ACCTNO;
    PROC SORT DATA=DEPOSIT.FD OUT=FD; BY ACCTNO;
    DATA FDORG;
      MERGE CISFD FD(IN=A); BY ACCTNO;
      IF A AND PURPOSE NE '2' AND
         PRODUCT NOT IN (350,351,352,353,354,355,356,357);
      FDBAL = CURBAL;
      IF INDORG = 'O';
    """
    # CISFD: filter CISDP.DEPOSIT for ACCTNO ranges
    cisfd = con.execute(f"""
        SELECT *
        FROM read_parquet('{CISDP_DEPOSIT_PARQUET}')
        WHERE (ACCTNO BETWEEN 1000000000 AND 1999999999)
           OR (ACCTNO BETWEEN 7000000000 AND 7999999999)
    """).pl()

    # FD: DEPOSIT.FD (all rows)
    fd = con.execute(f"""
        SELECT *
        FROM read_parquet('{DEPOSIT_FD_PARQUET}')
    """).pl()

    # MERGE CISFD FD(IN=A); BY ACCTNO; IF A
    # Note: order is CISFD first, FD(IN=A) — so IN=A flag is for FD.
    # Keep rows where FD has a match (IN=A means must exist in FD).
    # Left join FD with CISFD to get CISFD columns only where ACCTNO matches.
    cisfd_cols = set(cisfd.columns)
    fd_cols    = set(fd.columns)
    overlap    = cisfd_cols & fd_cols - {"ACCTNO"}

    if overlap:
        cisfd_renamed = cisfd.rename({c: f"{c}_CIS" for c in overlap})
    else:
        cisfd_renamed = cisfd

    fdorg = fd.join(cisfd_renamed, on="ACCTNO", how="left")

    # IF A AND PURPOSE NE '2' AND PRODUCT NOT IN (...)
    # IF INDORG = 'O'
    excl = FD_EXCL_PRODUCTS
    fdorg = fdorg.filter(
        (pl.col("PURPOSE").cast(pl.Utf8) != "2") &
        (~pl.col("PRODUCT").cast(pl.Int64).is_in(list(excl))) &
        (pl.col("INDORG").cast(pl.Utf8) == "O")
    )

    # FDBAL = CURBAL
    fdorg = fdorg.with_columns(
        pl.col("CURBAL").alias("FDBAL")
    )

    return fdorg


def build_data1(caorg: pl.DataFrame, fdorg: pl.DataFrame) -> pl.DataFrame:
    """
    DATA DATA1;
      SET FDORG CAORG;
      IF CUSTNO IN (53227,169990,170108,3562038,3721354);
    PROC SORT; BY CUSTNO ACCTNO;

    Combine FDORG and CAORG (vertical stack), filter by CUSTNO,
    sort by CUSTNO then ACCTNO.
    """
    # Align schemas for vertical concat: use common columns + fill missing with null
    fd_cols = set(fdorg.columns)
    ca_cols = set(caorg.columns)
    all_cols = fd_cols | ca_cols

    def align(df: pl.DataFrame, target_cols: set) -> pl.DataFrame:
        for col in target_cols:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))
        return df.select(sorted(target_cols))

    fdorg_aligned = align(fdorg, all_cols)
    caorg_aligned = align(caorg, all_cols)

    combined = pl.concat([fdorg_aligned, caorg_aligned], how="vertical")

    # IF CUSTNO IN (53227,169990,170108,3562038,3721354)
    data1 = combined.filter(
        pl.col("CUSTNO").cast(pl.Int64).is_in(sorted(REPORT_CUSTNOS))
    )

    # PROC SORT; BY CUSTNO ACCTNO;
    data1 = data1.sort(["CUSTNO", "ACCTNO"])

    return data1


# ==============================================================================
# REPORT GENERATION
#
# PROC PRINTTO PRINT=FD2TEXT NEW;
# TITLE1 'PUBLIC BANK BERHAD      PROGRAM-ID: EIBMTOP5';
# TITLE2 'PB SUBSIDIARIES UNDER TOP 50 CORP DEPOSITORS @ ' "&RDATE";
# PROC PRINT DATA=DATA1 SPLIT='*';
#      FORMAT  CURBAL COMMA16.2;
#      BY CUSTNO;
#      LABEL BRANCH   = 'BRANCH*CODE'
#            ACCTNO   = 'MNI NO'
#            CUSTNAME = 'DEPOSITOR'
#            CUSTNO   = 'CIS NO'
#            CUSTCODE = 'CUSTCD'
#            CURBAL   = 'CURRENT*BALANCE'
#            PRODUCT  = 'PRODUCT';
#      VAR BRANCH ACCTNO CUSTNAME CUSTNO CUSTCODE CURBAL PRODUCT;
#      SUM CURBAL;
#
# SPLIT='*' means column headers split on '*' across two lines.
# BY CUSTNO prints a group header and subtotal per CUSTNO.
# SUM CURBAL prints a running subtotal per group and grand total at end.
# Column widths based on SAS PROC PRINT defaults and LS=132.
# ==============================================================================

# Column display order: VAR BRANCH ACCTNO CUSTNAME CUSTNO CUSTCODE CURBAL PRODUCT
VAR_ORDER = ["BRANCH", "ACCTNO", "CUSTNAME", "CUSTNO", "CUSTCODE", "CURBAL", "PRODUCT"]

# Labels with SPLIT='*': line 1 / line 2
COL_LABELS: dict[str, tuple[str, str]] = {
    "BRANCH":   ("BRANCH", "CODE"),
    "ACCTNO":   ("MNI NO", ""),
    "CUSTNAME": ("DEPOSITOR", ""),
    "CUSTNO":   ("CIS NO", ""),
    "CUSTCODE": ("CUSTCD", ""),
    "CURBAL":   ("CURRENT", "BALANCE"),
    "PRODUCT":  ("PRODUCT", ""),
}

# Column widths (characters) — based on data types and SAS defaults
COL_WIDTHS: dict[str, int] = {
    "BRANCH":   8,
    "ACCTNO":   14,
    "CUSTNAME": 30,
    "CUSTNO":   10,
    "CUSTCODE": 8,
    "CURBAL":   16,
    "PRODUCT":  8,
}


def _col_val(row: dict, col: str) -> str:
    """Format a single column value for report output."""
    val = row.get(col)
    if col == "CURBAL":
        return format_comma16_2(val)
    if val is None:
        return ""
    return str(val)


def _pad(val: str, width: int, right_align: bool = False) -> str:
    """Pad or truncate a string to exactly width characters."""
    if right_align:
        return val.rjust(width)[:width]
    return val.ljust(width)[:width]


def format_report(data1: pl.DataFrame, rdate: str) -> str:
    """
    Generate the full report matching SAS PROC PRINT output with:
    - ASA carriage control characters (first character of each line)
    - Page headers with TITLE1 / TITLE2
    - BY CUSTNO group headers and subtotals
    - Grand total for CURBAL
    - SPLIT='*' two-line column headers
    - FORMAT CURBAL COMMA16.2
    - Page length = 60 lines
    """
    lines:      list[str] = []
    line_count: int       = 0
    page_num:   int       = 0

    # Build separator line
    sep_width = sum(COL_WIDTHS[c] + 2 for c in VAR_ORDER) + 4  # leading OBS col
    sep_line  = "-" * min(sep_width, LINE_SIZE)

    def emit_page_header() -> None:
        nonlocal line_count, page_num
        page_num += 1
        # ASA '1' = form feed / new page
        lines.append(f"1PUBLIC BANK BERHAD      PROGRAM-ID: EIBMTOP5")
        lines.append(f" PB SUBSIDIARIES UNDER TOP 50 CORP DEPOSITORS @ {rdate}")
        lines.append(f" ")

        # Column header line 1 (SPLIT='*' upper part)
        hdr1 = "  OBS  "
        for col in VAR_ORDER:
            label1, _ = COL_LABELS[col]
            right = (col == "CURBAL")
            hdr1 += "  " + _pad(label1, COL_WIDTHS[col], right_align=right)
        lines.append(f" {hdr1}")

        # Column header line 2 (SPLIT='*' lower part)
        hdr2 = "       "
        for col in VAR_ORDER:
            _, label2 = COL_LABELS[col]
            right = (col == "CURBAL")
            hdr2 += "  " + _pad(label2, COL_WIDTHS[col], right_align=right)
        lines.append(f" {hdr2}")

        lines.append(f" {sep_line}")
        line_count = 6

    def check_page_break() -> None:
        if line_count >= PAGE_LENGTH:
            emit_page_header()

    # Grand total accumulator
    grand_total: float = 0.0
    obs_idx:     int   = 0

    # Emit first page header
    emit_page_header()

    current_custno = None
    group_total:   float = 0.0

    for row in data1.iter_rows(named=True):
        custno = row.get("CUSTNO")

        # BY CUSTNO group break
        if custno != current_custno:
            # Emit subtotal for previous group
            if current_custno is not None:
                sub_line = "  -----" + "  " * (len(VAR_ORDER) - 2)
                # Place subtotal under CURBAL column
                sub_parts = "       "
                for col in VAR_ORDER:
                    if col == "CURBAL":
                        sub_parts += "  " + format_comma16_2(group_total)
                    else:
                        sub_parts += "  " + " " * COL_WIDTHS[col]
                lines.append(f" {sub_parts}")
                line_count += 1
                check_page_break()
                lines.append(f" ")
                line_count += 1
                check_page_break()

                grand_total += group_total
                group_total = 0.0

            # Group header: BY CUSTNO=value
            lines.append(f"0CUSTNO={custno}")
            line_count += 1
            check_page_break()
            current_custno = custno

        # Data row
        obs_idx += 1
        curbal_val = row.get("CURBAL")
        try:
            group_total += float(curbal_val) if curbal_val is not None else 0.0
        except (TypeError, ValueError):
            pass

        # Build data line: OBS + all VAR columns
        data_parts = f"  {obs_idx:<4d} "
        for col in VAR_ORDER:
            cell = _col_val(row, col)
            right = (col == "CURBAL")
            data_parts += "  " + _pad(cell, COL_WIDTHS[col], right_align=right)

        lines.append(f" {data_parts}")
        line_count += 1
        check_page_break()

    # Emit subtotal for last group
    if current_custno is not None:
        sub_parts = "       "
        for col in VAR_ORDER:
            if col == "CURBAL":
                sub_parts += "  " + format_comma16_2(group_total)
            else:
                sub_parts += "  " + " " * COL_WIDTHS[col]
        lines.append(f" {sub_parts}")
        line_count += 1
        check_page_break()
        lines.append(f" ")
        line_count += 1
        grand_total += group_total

    # Grand total line
    lines.append(f" {sep_line}")
    line_count += 1
    check_page_break()

    gt_parts = "       "
    for col in VAR_ORDER:
        if col == "CURBAL":
            gt_parts += "  " + format_comma16_2(grand_total)
        else:
            gt_parts += "  " + " " * COL_WIDTHS[col]
    lines.append(f" {gt_parts}")
    line_count += 1

    return "\n".join(lines) + "\n"


# ==============================================================================
# MAIN
# ==============================================================================

def main() -> None:
    """
    OPTIONS SORTDEV=3390 YEARCUTOFF=1950;

    Main entry point:
      1. Read REPTDATE and derive macro variables.
      2. %INC PGM(PBBLNFMT,PBBELF,PBBDPFMT)  -- format modules loaded (no direct calls here).
      3. Build CAORG (current accounts).
      4. Build FDORG (fixed deposits).
      5. Combine and filter to DATA1.
      6. PROC PRINT -> FD2TEXT report.
    """
    print("Running EIBMTOP5 ...")

    con = duckdb.connect()

    # ------------------------------------------------------------------
    # DATA REPTDATE; SET DEPOSIT.REPTDATE;
    # ------------------------------------------------------------------
    reptdate = read_reptdate(con)
    mvars    = derive_macros(reptdate)

    rdate    = mvars["RDATE"]       # DD/MM/YY  (DDMMYY8.)
    print(f"  REPTDATE={reptdate}  NOWK={mvars['NOWK']}  RDATE={rdate}")

    # ------------------------------------------------------------------
    # %INC PGM(PBBLNFMT,PBBELF,PBBDPFMT);
    # Format definitions are available via the imported modules.
    # No format functions are directly invoked in this program.

    # Placeholder for PBBLNFMT,PBBELF,PBBDPFMT
    # from PBBLNFMT import (
    #     format_oddenom, format_lndenom, format_lnprod,
    #     format_odcustcd, format_locustcd, format_lncustcd,
    #     format_statecd, format_apprlimt, format_loansize,
    #     format_mthpass, format_lnormt, format_lnrmmt,
    #     format_collcd, format_riskcd, format_busind,
    # )
    #
    # from PBBELF import (
    #     format_brchcd, format_cacbrch, format_cacname,
    #     format_regioff, format_regnew, format_ctype,
    #     format_brchrvr,
    # )
    #
    # from PBBDPFMT import (
    #     SADenomFormat, SAProductFormat, FDDenomFormat,
    #     FDProductFormat, CADenomFormat, CAProductFormat,
    #     FCYTermFormat, ProductLists, fdorgmt_format,
    # )
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # *---------------------------------------------------------------*
    # *  PRODUCE REPORTS TOP 50 DEPOSITOR                             *
    # *---------------------------------------------------------------*
    # DATA CAORG: current account records
    # ------------------------------------------------------------------
    print("  Building CAORG ...")
    caorg = build_caorg(con)
    print(f"  CAORG: {len(caorg)} rows")

    # ------------------------------------------------------------------
    # DATA FDORG: fixed deposit records
    # ------------------------------------------------------------------
    print("  Building FDORG ...")
    fdorg = build_fdorg(con)
    print(f"  FDORG: {len(fdorg)} rows")

    con.close()

    # ------------------------------------------------------------------
    # DATA DATA1; SET FDORG CAORG;
    # IF CUSTNO IN (53227,169990,170108,3562038,3721354);
    # PROC SORT; BY CUSTNO ACCTNO;
    # ------------------------------------------------------------------
    print("  Building DATA1 ...")
    data1 = build_data1(caorg, fdorg)
    print(f"  DATA1: {len(data1)} rows")

    # ------------------------------------------------------------------
    # PROC PRINTTO PRINT=FD2TEXT NEW;
    # PROC PRINT DATA=DATA1 SPLIT='*'; BY CUSTNO; SUM CURBAL;
    # ------------------------------------------------------------------
    print("  Generating report ...")
    report = format_report(data1, rdate)

    with open(FD2TEXT_OUT, "w", encoding="utf-8") as fh:
        fh.write(report)
    print(f"  Report written to: {FD2TEXT_OUT}")
    print("EIBMTOP5 complete.")


if __name__ == "__main__":
    main()
