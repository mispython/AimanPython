#!/usr/bin/env python3
"""
Program  : EIIMFD03.py
Purpose  : FD MOVEMENT OF RM500,000 AND ABOVE
           Reads current-period FD data (FD.FD) and previous-period FD data
            (FD1.FD), filters to open/dormant accounts (OPENIND IN ('O','D')),
            accumulates CURBAL per (BRANCH, ACCTNO, INTPLAN) to handle
            multiple sub-records, merges current vs previous to compute
            MOVEMENT = CURRBAL - PREVBAL, retains only rows where
            ABS(MOVEMENT) >= 500,000, enriches with customer names from
            CISSAFD.DEPOSIT (SECCUST=901) and branch abbreviations from
            BRHFILE, then produces a PROC PRINT-equivalent fixed-width ASA
            report written to FDTEXT.

           OPTIONS YEARCUTOFF=1950 NONUMBER NODATE NOCENTER MISSING=0

           Report columns (VAR order):
               BRANCH  BRABBR  ACCTNO  CUSTNAME  INTPLAN  CURRBAL  PREVBAL  MOVEMENT

           Output: FDTEXT.txt  (ASA carriage-control, 60 lines/page)

Dependencies:
    PBBDPFMT — imported for format completeness per %INC PGM(PBBDPFMT).
               No PBBDPFMT format functions are directly called in this
                program; the %INC was used to load the SAS format catalog.
"""

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, timedelta

# PBBDPFMT imported for format completeness per %INC PGM(PBBDPFMT).
# No PBBDPFMT format functions are directly called in this program.
# from PBBDPFMT import ...

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR     = Path(__file__).resolve().parent
DATA_DIR     = BASE_DIR / "data"

# FD library  -> current-period FD parquet (DD FD / LIBNAME FD)
FD_DIR       = DATA_DIR / "fd"

# FD1 library -> previous-period FD parquet (DD FD1 / LIBNAME FD1)
FD1_DIR      = DATA_DIR / "fd1"

# CISSAFD library -> CISSAFD.DEPOSIT parquet
CISSAFD_DIR  = DATA_DIR / "cissafd"

# Branch flat file (DD BRHFILE, LRECL=80)
BRHFILE      = DATA_DIR / "BRHFILE.txt"

OUTPUT_DIR   = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# PROC PRINTTO PRINT=FDTEXT NEW  -> output file
FDTEXT       = OUTPUT_DIR / "FDTEXT.txt"

# ============================================================================
# REPORT CONSTANTS   (OPTIONS NOCENTER, implicit LS=132, PS=60)
# ============================================================================

PAGE_LENGTH  = 60
LINE_WIDTH   = 132

INSTITUTION  = "PUBLIC ISLAMIC BANK BERHAD"
REPORT_ID    = "EIIMFD03"

# ============================================================================
# ASA CARRIAGE-CONTROL HELPERS
# ============================================================================

ASA_NEWPAGE  = "1"    # form feed / new page
ASA_SPACE1   = " "    # single space (normal next line)
ASA_SPACE2   = "0"    # double space (skip one line before)


def asa(ctl: str, text: str) -> str:
    return f"{ctl}{text}\n"


# ============================================================================
# NUMBER FORMATTING HELPERS
# ============================================================================

def fmt_comma17_2(value) -> str:
    """FORMAT=COMMA17.2 — right-justified 17-char float with thousands commas."""
    if value is None:
        return " " * 17
    return f"{float(value):>17,.2f}"


def fmt_int(value, width: int) -> str:
    """Numeric integer format, right-justified to given width."""
    if value is None:
        return " " * width
    return f"{int(value):>{width}}"


def fmt_str(value, width: int) -> str:
    """String format, left-justified, truncated/padded to given width."""
    if value is None:
        return " " * width
    return f"{str(value):<{width}.{width}}"


# ============================================================================
# STEP 1: REPTDATE  —  read RDATE from FD.REPTDATE
# DATA _NULL_; SET FD.REPTDATE;
#   CALL SYMPUT('RDATE', PUT(REPTDATE, DDMMYY8.));
# ============================================================================

def derive_rdate(fd_dir: Path) -> str:
    reptdate_file = fd_dir / "REPTDATE.parquet"
    con = duckdb.connect()
    row = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{reptdate_file}') LIMIT 1"
    ).fetchone()
    con.close()

    if row is None:
        raise RuntimeError(f"No rows in {reptdate_file}")

    val = row[0]
    if isinstance(val, (int, float)):
        reptdate = date(1960, 1, 1) + timedelta(days=int(val))
    elif isinstance(val, date):
        reptdate = val
    else:
        from datetime import datetime as dt
        reptdate = dt.strptime(str(val)[:10], "%Y-%m-%d").date()

    # DDMMYY8. -> e.g. "31/03/26" — SAS displays as DD/MM/YY
    return reptdate.strftime("%d/%m/%y")


# ============================================================================
# STEP 2: Load and accumulate FD data
#
# DATA FD; SET FD.FD; IF OPENIND IN ('O','D');
# PROC SORT; BY BRANCH ACCTNO INTPLAN;
# DATA FD; DROP CURBAL; SET FD; BY BRANCH ACCTNO INTPLAN;
#   IF FIRST.INTPLAN THEN CURRBAL=0.00;
#   CURRBAL+CURBAL;
#   IF LAST.INTPLAN THEN OUTPUT;
# ============================================================================

OPENIND_FILTER = {"O", "D"}


def load_and_accumulate(
    lib_dir: Path,
    dataset: str,
    bal_col_in: str,
    bal_col_out: str,
) -> pl.DataFrame:
    """
    Load <lib_dir>/<dataset>.parquet, filter OPENIND IN ('O','D'),
    sort by (BRANCH, ACCTNO, INTPLAN), accumulate CURBAL within each
    (BRANCH, ACCTNO, INTPLAN) group (DOT-SUM equivalent), keep last row
    per group (LAST.INTPLAN), rename accumulated column to bal_col_out,
    and drop the original CURBAL.

    Parameters
    ----------
    lib_dir     : library directory
    dataset     : parquet file stem (no extension)
    bal_col_in  : source column name for current balance ('CURBAL')
    bal_col_out : output accumulated balance column name ('CURRBAL' or 'PREVBAL')
    """
    parquet_file = lib_dir / f"{dataset}.parquet"
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{parquet_file}')").pl()
    con.close()

    # IF OPENIND IN ('O','D')
    df = df.filter(pl.col("OPENIND").is_in(OPENIND_FILTER))

    # PROC SORT BY BRANCH ACCTNO INTPLAN
    df = df.sort(["BRANCH", "ACCTNO", "INTPLAN"])

    # Accumulate CURBAL within (BRANCH, ACCTNO, INTPLAN) — keep last row
    # SAS: IF FIRST.INTPLAN THEN CURRBAL=0; CURRBAL+CURBAL; IF LAST.INTPLAN THEN OUTPUT;
    accumulated = (
        df.group_by(["BRANCH", "ACCTNO", "INTPLAN"], maintain_order=True)
        .agg(pl.col(bal_col_in).sum().alias(bal_col_out))
    )

    # Bring back the non-CURBAL columns from the last row of each group
    # (retain all columns except CURBAL; CURBAL itself is replaced by accumulated sum)
    drop_cols = [bal_col_in]
    other_cols = [c for c in df.columns if c not in drop_cols + ["BRANCH", "ACCTNO", "INTPLAN"]]

    last_rows = (
        df.group_by(["BRANCH", "ACCTNO", "INTPLAN"], maintain_order=True)
        .agg([pl.col(c).last().alias(c) for c in other_cols])
    )

    result = accumulated.join(last_rows, on=["BRANCH", "ACCTNO", "INTPLAN"], how="left")

    return result


# ============================================================================
# STEP 3: Merge FD (current) with FD1 (previous), compute MOVEMENT,
#         filter ABS(MOVEMENT) >= 500,000
#
# DATA FD;
#   MERGE FD FD1; BY BRANCH ACCTNO INTPLAN;
#   IF PREVBAL=. THEN PREVBAL=0;
#   IF CURRBAL=. THEN CURRBAL=0;
#   MOVEMENT = CURRBAL - PREVBAL;
#   IF ABS(MOVEMENT) >= 500000 THEN OUTPUT;
# ============================================================================

def merge_and_filter(fd_curr: pl.DataFrame, fd_prev: pl.DataFrame) -> pl.DataFrame:
    """
    Full outer merge of current and previous FD snapshots on
    (BRANCH, ACCTNO, INTPLAN).  Null balances default to 0.
    Retains rows where |MOVEMENT| >= 500,000.
    """
    # Retain only the balance column from fd_prev to avoid column clashes
    fd_prev_slim = fd_prev.select(["BRANCH", "ACCTNO", "INTPLAN", "PREVBAL"])

    merged = fd_curr.join(
        fd_prev_slim,
        on=["BRANCH", "ACCTNO", "INTPLAN"],
        how="full",
        coalesce=True,
    )

    merged = merged.with_columns([
        pl.col("PREVBAL").fill_null(0.0),
        pl.col("CURRBAL").fill_null(0.0),
    ]).with_columns(
        (pl.col("CURRBAL") - pl.col("PREVBAL")).alias("MOVEMENT")
    ).filter(
        pl.col("MOVEMENT").abs() >= 500_000
    )

    return merged


# ============================================================================
# STEP 4: Enrich with CISSAFD customer names  (SECCUST=901)
#
# DATA CISSAFD (KEEP=ACCTNO CUSTNAME);
#   SET CISSAFD.DEPOSIT; IF SECCUST=901;
# PROC SORT CISSAFD BY ACCTNO;
# DATA FD; MERGE FD(IN=A) CISSAFD(IN=B); BY ACCTNO; IF A;
# ============================================================================

def load_cissafd(cissafd_dir: Path) -> pl.DataFrame:
    deposit_file = cissafd_dir / "DEPOSIT.parquet"
    con = duckdb.connect()
    df = con.execute(
        f"SELECT ACCTNO, CUSTNAME FROM read_parquet('{deposit_file}') "
        f"WHERE SECCUST = 901"
    ).pl()
    con.close()
    return df.sort("ACCTNO")


# ============================================================================
# STEP 5: Enrich with branch abbreviations from BRHFILE
#
# DATA BRHDATA; INFILE BRHFILE LRECL=80;
#   INPUT @2 BRANCH 3. @6 BRABBR $3.;
# PROC SORT BRHDATA BY BRANCH;
# DATA FD; MERGE FD(IN=A) BRHDATA(IN=B); BY BRANCH; IF A;
# ============================================================================

def load_brhdata(brhfile: Path) -> pl.DataFrame:
    """
    Fixed-width: BRANCH at @2 (3 chars), BRABBR at @6 (3 chars).
    SAS 1-based positions -> Python 0-based: [1:4] and [5:8].
    """
    records = []
    with open(brhfile, "r") as fh:
        for line in fh:
            if len(line) < 8:
                continue
            branch_str = line[1:4].strip()
            brabbr_str = line[5:8].strip()
            if branch_str.isdigit():
                records.append({"BRANCH": int(branch_str), "BRABBR": brabbr_str})
    return pl.DataFrame(records, schema={"BRANCH": pl.Int64, "BRABBR": pl.Utf8})


# ============================================================================
# STEP 6: PROC PRINT equivalent  —  write ASA report to FDTEXT
#
# PROC PRINT DATA=FD LABEL;
#   FORMAT PREVBAL CURRBAL MOVEMENT COMMA17.2;
#   LABEL PREVBAL='PREVIOUS BALANCE' CURRBAL='CURRENT BALANCE';
#   VAR BRANCH BRABBR ACCTNO CUSTNAME INTPLAN CURRBAL PREVBAL MOVEMENT;
#   SUM PREVBAL CURRBAL MOVEMENT;
#   TITLE1 'PUBLIC ISLAMIC BANK BERHAD';
#   TITLE2 'FD MOVEMENT OF RM500,000 AND ABOVE AS AT ' "&RDATE";
#   TITLE3 'REPORT ID: EIIMFD03';
#
# Column order: BRANCH BRABBR ACCTNO CUSTNAME INTPLAN CURRBAL PREVBAL MOVEMENT
# SUM statement: running column totals printed at end.
# ============================================================================

# Column widths
W_OBS      =  6    # observation number (PROC PRINT default)
W_BRANCH   =  6
W_BRABBR   =  6
W_ACCTNO   = 15
W_CUSTNAME = 30
W_INTPLAN  =  8
W_CURRBAL  = 17    # COMMA17.2
W_PREVBAL  = 17
W_MOVEMENT = 17
SEP        =  2    # inter-column gap


def _build_header(rdate: str) -> list[str]:
    """Build title and column-header lines."""
    title2 = f"FD MOVEMENT OF RM500,000 AND ABOVE AS AT {rdate}"

    # Column header labels (LABEL option applies CURRBAL/PREVBAL labels)
    h = (
        f"{'OBS':>{W_OBS}}"
        f"  {'BRANCH':>{W_BRANCH}}"
        f"  {'BRABBR':<{W_BRABBR}}"
        f"  {'ACCTNO':>{W_ACCTNO}}"
        f"  {'CUSTNAME':<{W_CUSTNAME}}"
        f"  {'INTPLAN':>{W_INTPLAN}}"
        f"  {'CURRENT BALANCE':>{W_CURRBAL}}"     # LABEL CURRBAL='CURRENT BALANCE'
        f"  {'PREVIOUS BALANCE':>{W_PREVBAL}}"    # LABEL PREVBAL='PREVIOUS BALANCE'
        f"  {'MOVEMENT':>{W_MOVEMENT}}"
    )
    uline = "-" * len(h)

    return [
        asa(ASA_NEWPAGE, INSTITUTION),
        asa(ASA_SPACE1,  title2),
        asa(ASA_SPACE1,  f"REPORT ID: {REPORT_ID}"),
        asa(ASA_SPACE1,  ""),
        asa(ASA_SPACE2,  h),
        asa(ASA_SPACE1,  uline),
    ]


def write_report(df: pl.DataFrame, rdate: str) -> None:
    """
    Write PROC PRINT-equivalent ASA report for FD movements.
    Rows are implicitly in the sort order of the incoming DataFrame
    (BRANCH, ACCTNO, INTPLAN from upstream sorts).
    SUM PREVBAL CURRBAL MOVEMENT  -> grand totals printed at end.
    """
    # PROC PRINT default order is observation order (no BY in PROC PRINT)
    # Upstream data is sorted by BRANCH then ACCTNO; maintain that order.

    sum_currbal  = 0.0
    sum_prevbal  = 0.0
    sum_movement = 0.0

    header_lines = _build_header(rdate)
    row_width    = len(header_lines[4]) - 2   # strip ASA char + newline

    with open(FDTEXT, "w") as fh:
        line_count = PAGE_LENGTH + 1   # force first page header

        def emit(ctl: str, text: str) -> None:
            nonlocal line_count
            if line_count >= PAGE_LENGTH and ctl != ASA_NEWPAGE:
                for hdr in header_lines:
                    fh.write(hdr)
                line_count = len(header_lines)
            fh.write(asa(ctl, text))
            if ctl != ASA_NEWPAGE:
                line_count += 1
            else:
                line_count = len(header_lines)

        # Emit first page header
        for hdr in header_lines:
            fh.write(hdr)
        line_count = len(header_lines)

        obs = 0
        for row in df.to_dicts():
            obs      += 1
            branch   = row.get("BRANCH")
            brabbr   = row.get("BRABBR",   "") or ""
            acctno   = row.get("ACCTNO")
            custname = row.get("CUSTNAME", "") or ""
            intplan  = row.get("INTPLAN")
            currbal  = float(row.get("CURRBAL",  0) or 0)
            prevbal  = float(row.get("PREVBAL",  0) or 0)
            movement = float(row.get("MOVEMENT", 0) or 0)

            if line_count >= PAGE_LENGTH:
                for hdr in header_lines:
                    fh.write(hdr)
                line_count = len(header_lines)

            detail = (
                f"{fmt_int(obs, W_OBS)}"
                f"  {fmt_int(branch, W_BRANCH)}"
                f"  {fmt_str(brabbr, W_BRABBR)}"
                f"  {fmt_int(acctno, W_ACCTNO)}"
                f"  {fmt_str(custname, W_CUSTNAME)}"
                f"  {fmt_int(intplan, W_INTPLAN)}"
                f"  {fmt_comma17_2(currbal)}"
                f"  {fmt_comma17_2(prevbal)}"
                f"  {fmt_comma17_2(movement)}"
            )
            fh.write(asa(ASA_SPACE1, detail))
            line_count += 1

            sum_currbal  += currbal
            sum_prevbal  += prevbal
            sum_movement += movement

        # SUM line — printed at end (no BY groups in PROC PRINT here)
        uline = "-" * (W_OBS + SEP + W_BRANCH + SEP + W_BRABBR + SEP +
                       W_ACCTNO + SEP + W_CUSTNAME + SEP + W_INTPLAN + SEP +
                       W_CURRBAL + SEP + W_PREVBAL + SEP + W_MOVEMENT)
        if line_count >= PAGE_LENGTH:
            for hdr in header_lines:
                fh.write(hdr)
        fh.write(asa(ASA_SPACE1, uline))

        sum_line = (
            f"{'':>{W_OBS}}"
            f"  {'':>{W_BRANCH}}"
            f"  {'':>{W_BRABBR}}"
            f"  {'':>{W_ACCTNO}}"
            f"  {'':>{W_CUSTNAME}}"
            f"  {'':>{W_INTPLAN}}"
            f"  {fmt_comma17_2(sum_currbal)}"
            f"  {fmt_comma17_2(sum_prevbal)}"
            f"  {fmt_comma17_2(sum_movement)}"
        )
        fh.write(asa(ASA_SPACE1, sum_line))

    print(f"FDTEXT report written -> {FDTEXT}  ({obs} records)")


# ============================================================================
# MAIN
# ============================================================================

def main():
    # Step 1: reporting date
    rdate = derive_rdate(FD_DIR)

    # Step 2a: current-period FD
    # DATA FD; SET FD.FD; IF OPENIND IN ('O','D');
    # PROC SORT BY BRANCH ACCTNO INTPLAN;
    # DATA FD; ... CURRBAL+CURBAL; IF LAST.INTPLAN THEN OUTPUT;
    fd_curr = load_and_accumulate(FD_DIR,  "FD",  "CURBAL", "CURRBAL")

    # Step 2b: previous-period FD
    # DATA FD1; SET FD1.FD; IF OPENIND IN ('O','D');
    # PROC SORT BY BRANCH ACCTNO INTPLAN;
    # DATA FD1; ... PREVBAL+CURBAL; IF LAST.INTPLAN THEN OUTPUT;
    fd_prev = load_and_accumulate(FD1_DIR, "FD",  "CURBAL", "PREVBAL")

    # Step 3: merge and filter |MOVEMENT| >= 500,000
    # DATA FD; MERGE FD FD1; BY BRANCH ACCTNO INTPLAN;
    fd = merge_and_filter(fd_curr, fd_prev)

    # Step 4: sort by ACCTNO, merge CISSAFD (SECCUST=901) for CUSTNAME
    fd = fd.sort("ACCTNO")
    cissafd = load_cissafd(CISSAFD_DIR)
    fd = fd.join(cissafd, on="ACCTNO", how="left")   # IF A (keep all FD rows)

    # Step 5: sort by BRANCH, merge BRHDATA for BRABBR
    brhdata = load_brhdata(BRHFILE)
    fd = fd.sort("BRANCH").join(
        brhdata.sort("BRANCH"), on="BRANCH", how="left"
    )   # IF A (keep all FD rows)

    # Step 6: PROC PRINTTO PRINT=FDTEXT NEW + PROC PRINT
    # Default PROC PRINT output order follows the current sort (BRANCH then ACCTNO)
    write_report(fd, rdate)


if __name__ == "__main__":
    main()
