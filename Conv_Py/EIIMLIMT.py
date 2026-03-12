#!/usr/bin/env python3
"""
Program  : EIIMLIMT.py
Title    : GROSS LOAN APPROVED LIMIT FOR OD, RC AND OTHER LOANS
Created  : 31 DEC 2004
Modified : ESMR07-1607
Purpose  : Reads loan data for the current reporting period, merges with
            branch reference data, categorizes accounts into OD / HP / RC / LN
            product groups, summarizes approved limits by PROD and BRHCODE, and
            produces four fixed-width ASA reports:
               EIILMTOD  – Overdraft approved limits
               EIILMTHP  – Hire-purchase approved limits
               EIILMTRC  – Revolving-credit approved limits
               EIILMTLN  – Loan (term & corporate) approved limits
           Output files are plain .txt with ASA carriage-control characters.
           Page length: 60 lines per page.
"""

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, timedelta

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR     = Path(__file__).resolve().parent
DATA_DIR     = BASE_DIR / "data"
LOAN_DIR     = DATA_DIR / "loan"          # LOAN library  -> SAP.PIBB.MNILN(0) etc.
OUTPUT_DIR   = BASE_DIR / "output"

# Input files
BRHFILE      = DATA_DIR / "BRHFILE.txt"  # Branch reference flat file (LRECL=80)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Output report files
REPORT_OD    = OUTPUT_DIR / "EIILMTOD.txt"
REPORT_HP    = OUTPUT_DIR / "EIILMTHP.txt"
REPORT_RC    = OUTPUT_DIR / "EIILMTRC.txt"
REPORT_LN    = OUTPUT_DIR / "EIILMTLN.txt"

# Report layout constants
PAGE_LENGTH  = 60    # PS=60
LINE_WIDTH   = 132   # LS=132 (default SAS wide listing)

# ============================================================================
# ASA CARRIAGE CONTROL HELPERS
# ============================================================================

ASA_NEWPAGE  = "1"   # form feed / new page
ASA_SPACE1   = " "   # single space (normal next line)
ASA_SPACE2   = "0"   # double space (skip one line)
ASA_SPACE3   = "-"   # triple space (skip two lines)


def asa_line(ctl: str, text: str) -> str:
    """Build one ASA carriage-control line (control char in column 1)."""
    return f"{ctl}{text}\n"


# ============================================================================
# NUMBER FORMATTING HELPERS
# ============================================================================

def fmt_comma9(value) -> str:
    """FORMAT=COMMA9. -> right-justified 9-char integer with thousands commas."""
    if value is None:
        return " " * 9
    return f"{int(value):>9,}"


def fmt_comma20_2(value) -> str:
    """FORMAT=COMMA20.2 -> right-justified 20-char float with 2 decimals & commas."""
    if value is None:
        return " " * 20
    return f"{float(value):>20,.2f}"


# ============================================================================
# STEP 1: REPTDATE  –  derive NOWK, REPTMON, REPDATE
# DATA REPTDATE; SET LOAN.REPTDATE; SELECT(DAY(REPTDATE)); ... CALL SYMPUT(...);
# ============================================================================

def derive_report_params(loan_dir: Path) -> dict:
    """
    Read REPTDATE from LOAN.REPTDATE parquet (first row) and derive
    the NOWK, REPTMON, REPDATE macro variables.
    """
    reptdate_file = loan_dir / "REPTDATE.parquet"
    con = duckdb.connect()
    row = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{reptdate_file}') LIMIT 1"
    ).fetchone()
    con.close()

    if row is None:
        raise RuntimeError(f"No rows in {reptdate_file}")

    reptdate_val = row[0]
    # Handle SAS integer date (days since 1960-01-01) or Python date/datetime
    if isinstance(reptdate_val, (int, float)):
        reptdate = date(1960, 1, 1) + timedelta(days=int(reptdate_val))
    else:
        reptdate = reptdate_val if isinstance(reptdate_val, date) else reptdate_val.date()

    day = reptdate.day
    if   day == 8:   wk = "1"
    elif day == 15:  wk = "2"
    elif day == 22:  wk = "3"
    else:            wk = "4"

    reptmon  = f"{reptdate.month:02d}"
    repdate  = reptdate.strftime("%d/%m/%Y")   # DDMMYY8. displayed as DD/MM/YYYY

    return {
        "NOWK"    : wk,
        "REPTMON" : reptmon,
        "REPDATE" : repdate,
        "reptdate": reptdate,
    }


# ============================================================================
# STEP 2: SDESC  –  institution description
# DATA SDESC; SET LOAN.SDESC; CALL SYMPUT('SDESC', PUT(SDESC,$36.));
# ============================================================================

def load_sdesc(loan_dir: Path) -> str:
    """Read SDESC from LOAN.SDESC parquet (first row)."""
    sdesc_file = loan_dir / "SDESC.parquet"
    con = duckdb.connect()
    row = con.execute(
        f"SELECT SDESC FROM read_parquet('{sdesc_file}') LIMIT 1"
    ).fetchone()
    con.close()
    if row is None:
        return ""
    return str(row[0]).strip()[:36]


# ============================================================================
# STEP 3: BRHDATA  –  branch reference
# DATA BRHDATA; INFILE BRHFILE LRECL=80; INPUT @2 BRANCH 3. @6 BRHCODE $3.;
# ============================================================================

def load_brhdata(brhfile: Path) -> pl.DataFrame:
    """
    Read branch reference flat file.
    Fixed-width: BRANCH at column 2 (3 chars), BRHCODE at column 6 (3 chars).
    SAS INPUT uses 1-based @-positions; Python uses 0-based slicing.
    """
    records = []
    with open(brhfile, "r") as fh:
        for line in fh:
            if len(line) < 8:
                continue
            branch_str  = line[1:4].strip()    # @2, 3.  -> index 1:4
            brhcode_str = line[5:8].strip()    # @6, $3. -> index 5:8
            if branch_str.isdigit():
                records.append({
                    "BRANCH" : int(branch_str),
                    "BRHCODE": brhcode_str,
                })
    return pl.DataFrame(records, schema={"BRANCH": pl.Int64, "BRHCODE": pl.Utf8})


# ============================================================================
# STEP 4: LN  –  load and filter loan / uniplan-loan datasets
#
# DATA LN (KEEP=ACCTNO NOTENO PRODCD PRODUCT BRANCH APPRLIMT APPRLIM2);
#      SET LOAN.LOAN&REPTMON&NOWK
#          LOAN.ULOAN&REPTMON&NOWK (RENAME=APPRLIMT=APPRLIM2);
#      IF PRODUCT IN (150,151) THEN DELETE;
#      IF (SUBSTR(PRODCD,1,2) EQ '34' OR PRODCD EQ '54120') AND PAIDIND NE 'P';
# ============================================================================

EXCL_PRODUCTS = {150, 151}
KEEP_COLS = ["ACCTNO", "NOTENO", "PRODCD", "PRODUCT", "BRANCH", "APPRLIMT", "APPRLIM2", "PAIDIND"]


def load_ln(loan_dir: Path, reptmon: str, nowk: str) -> pl.DataFrame:
    """
    Load LOAN and ULOAN parquet files, apply filters, and return combined LN dataset.
    ULOAN has APPRLIMT renamed to APPRLIM2; for LOAN rows APPRLIM2 is null.
    """
    con = duckdb.connect()

    loan_file  = loan_dir / f"LOAN{reptmon}{nowk}.parquet"
    uloan_file = loan_dir / f"ULOAN{reptmon}{nowk}.parquet"

    # LOAN dataset: APPRLIM2 comes from the file; APPRLIMT stays as is
    loan_df = con.execute(
        f"SELECT ACCTNO, NOTENO, PRODCD, PRODUCT, BRANCH, APPRLIMT, "
        f"       APPRLIMT AS APPRLIM2, PAIDIND "
        f"FROM read_parquet('{loan_file}')"
    ).pl()

    # ULOAN dataset: APPRLIMT in source is renamed to APPRLIM2;
    # APPRLIMT for ULOAN rows is set to null (not present in that file)
    uloan_df = con.execute(
        f"SELECT ACCTNO, NOTENO, PRODCD, PRODUCT, BRANCH, "
        f"       NULL AS APPRLIMT, APPRLIMT AS APPRLIM2, PAIDIND "
        f"FROM read_parquet('{uloan_file}')"
    ).pl()

    con.close()

    combined = pl.concat([loan_df, uloan_df], how="diagonal_relaxed")

    # IF PRODUCT IN (150,151) THEN DELETE;
    combined = combined.filter(~pl.col("PRODUCT").is_in(EXCL_PRODUCTS))

    # IF (SUBSTR(PRODCD,1,2) EQ '34' OR PRODCD EQ '54120') AND PAIDIND NE 'P';
    combined = combined.filter(
        (
            (pl.col("PRODCD").str.slice(0, 2) == "34") |
            (pl.col("PRODCD") == "54120")
        ) &
        (pl.col("PAIDIND") != "P")
    )

    return combined


# ============================================================================
# STEP 5: Merge LN with BRHDATA, route into OD / HP / RC / LN / LN2
#
# MERGE LN(IN=A) BRHDATA; BY BRANCH; IF A;
# IF BRHCODE='' THEN BRHCODE='XXX';
# Routing by PRODUCT:
#   OD  -> 93,160,166,168          APPRLIM3=0
#   HP  -> 128,130,131,132         APPRLIM3=0
#   RC  -> 184                     APPRLIM3=APPRLIM2
#   LN  -> 110..145,194,195,162,   APPRLIM3=0
#           925,999
#   LN2 -> 180,181,182,183,193     APPRLIM3=APPRLIM2
# ============================================================================

OD_PRODUCTS  = {93, 160, 166, 168}
HP_PRODUCTS  = {128, 130, 131, 132}
RC_PRODUCTS  = {184}
LN_PRODUCTS  = {110, 111, 112, 113, 114, 115, 116, 117, 120, 122, 124, 127,
                129, 135, 136, 138, 139, 140, 141, 142, 143, 145, 194, 195,
                162, 925, 999}
LN2_PRODUCTS = {180, 181, 182, 183, 193}


def route_records(ln: pl.DataFrame, brhdata: pl.DataFrame):
    """
    Left-merge LN with BRHDATA (keeping all LN rows), default BRHCODE='XXX',
    then split into OD, HP, RC, LN, LN2 DataFrames.
    Returns a tuple: (od_df, hp_df, rc_df, ln_df, ln2_df)
    """
    # Left join: IF A (keep only LN rows)
    merged = ln.join(brhdata, on="BRANCH", how="left")

    # IF BRHCODE='' THEN BRHCODE='XXX'
    merged = merged.with_columns(
        pl.when(pl.col("BRHCODE").is_null() | (pl.col("BRHCODE") == ""))
        .then(pl.lit("XXX"))
        .otherwise(pl.col("BRHCODE"))
        .alias("BRHCODE")
    )

    def subset(products: set, prod_label: str, apprlim3_src: str) -> pl.DataFrame:
        """Filter rows by product set, add PROD and APPRLIM3 columns."""
        df = merged.filter(pl.col("PRODUCT").is_in(products)).with_columns([
            pl.lit(prod_label).alias("PROD"),
            (pl.col(apprlim3_src) if apprlim3_src != "0"
             else pl.lit(0.0).cast(pl.Float64)).alias("APPRLIM3"),
        ])
        return df

    od_df  = subset(OD_PRODUCTS,  "OD",  "0")
    hp_df  = subset(HP_PRODUCTS,  "HP",  "0")
    rc_df  = subset(RC_PRODUCTS,  "RC",  "APPRLIM2")
    ln_df  = subset(LN_PRODUCTS,  "LN",  "0")
    ln2_df = subset(LN2_PRODUCTS, "LN",  "APPRLIM2")

    return od_df, hp_df, rc_df, ln_df, ln2_df


# ============================================================================
# STEP 6: PROC SUMMARY  –  aggregate by PROD and BRHCODE
# ============================================================================

def summarise(df: pl.DataFrame, amount_col: str) -> pl.DataFrame:
    """
    Equivalent to PROC SUMMARY NWAY; CLASS PROD BRHCODE; VAR <amount_col> APPRLIM3;
    OUTPUT OUT=... (RENAME=(_FREQ_=NOACCT) DROP=_TYPE_) SUM=;
    """
    return (
        df.group_by(["PROD", "BRHCODE"])
        .agg([
            pl.len().alias("NOACCT"),
            pl.col(amount_col).sum().alias(amount_col),
            pl.col("APPRLIM3").sum().alias("APPRLIM3"),
        ])
        .sort(["PROD", "BRHCODE"])
    )


# ============================================================================
# STEP 7: PROC REPORT  –  produce fixed-width ASA report
#
# Layout (mirroring SAS PROC REPORT HEADLINE SPLIT='*'):
#   Col 1     : ASA carriage control
#   PROD      : width 8
#   BRHCODE   : width 7
#   NOACCT    : COMMA9.   (9 chars)
#   AMOUNT    : COMMA20.2 (20 chars)
#   APPRLIM3  : COMMA20.2 (20 chars)
#
# Breaks: BREAK AFTER PROD (double underline, summarize, page);
#         RBREAK AFTER (double underline, summarize at end);
#         COMPUTE AFTER PROD: LINE ' ';
# ============================================================================

# Column widths
W_PROD     =  8
W_BRHCODE  =  7
W_NOACCT   =  9
W_AMOUNT   = 20
W_APPRLIM3 = 20
COL_GAP    =  2   # spaces between columns (SAS default column spacing)

DLINE = "=" * (W_PROD + W_BRHCODE + W_NOACCT + W_AMOUNT + W_APPRLIM3 + COL_GAP * 4)
ULINE = "-" * len(DLINE)


def _header_lines(sdesc: str, report_id: str, repdate: str,
                  prod_label: str, amount_label: str) -> list[str]:
    """Build title and column-header lines (ASA)."""
    lines = []
    lines.append(asa_line(ASA_NEWPAGE, sdesc.ljust(LINE_WIDTH)))
    lines.append(asa_line(ASA_SPACE1,  f"REPORT ID : {report_id}"))
    lines.append(asa_line(ASA_SPACE1,  f"LOAN APPROVED LIMIT AS AT {repdate}"))
    lines.append(asa_line(ASA_SPACE1,  ""))

    # Column headers (HEADLINE equivalent)
    h1 = (
        f"{'PROD':<{W_PROD}}"
        f"{'BRHCODE':>{W_BRHCODE}}"
        f"  {'NO OF A/C':>{W_NOACCT}}"
        f"  {'APPROVED LIMIT':>{W_AMOUNT}}"
        f"  {'O/W CORP':>{W_APPRLIM3}}"
    )
    h2 = (
        f"{'':<{W_PROD}}"
        f"{'':{W_BRHCODE}}"
        f"  {'':{W_NOACCT}}"
        f"  {'':{W_AMOUNT}}"
        f"  {'APP.LIMIT':>{W_APPRLIM3}}"
    )
    lines.append(asa_line(ASA_SPACE2, h1))
    lines.append(asa_line(ASA_SPACE1, h2))
    lines.append(asa_line(ASA_SPACE1, DLINE))
    return lines


def _detail_line(row: dict, amount_col: str) -> str:
    """Format one detail row."""
    prod     = str(row.get("PROD",     "") or "").ljust(W_PROD)
    brhcode  = str(row.get("BRHCODE",  "") or "").rjust(W_BRHCODE)
    noacct   = fmt_comma9(row.get("NOACCT",   0))
    amount   = fmt_comma20_2(row.get(amount_col, 0))
    apprlim3 = fmt_comma20_2(row.get("APPRLIM3",  0))
    return f"{prod}{brhcode}  {noacct}  {amount}  {apprlim3}"


def _subtotal_line(prod: str, noacct_sum: int,
                   amount_sum: float, apprlim3_sum: float) -> str:
    """Format a BREAK AFTER PROD summarize row."""
    return (
        f"{'':>{W_PROD}}"
        f"{'':>{W_BRHCODE}}"
        f"  {fmt_comma9(noacct_sum)}"
        f"  {fmt_comma20_2(amount_sum)}"
        f"  {fmt_comma20_2(apprlim3_sum)}"
    )


def write_report(
    df: pl.DataFrame,
    amount_col: str,
    prod_label_col: str,
    report_id: str,
    sdesc: str,
    repdate: str,
    out_file: Path,
) -> None:
    """
    Generate PROC REPORT-equivalent fixed-width ASA report.

    BREAK AFTER PROD / DUL DOL SUMMARIZE PAGE  ->  double underline + subtotal + new page
    RBREAK AFTER     / DUL DOL SUMMARIZE        ->  grand total at end
    COMPUTE AFTER PROD: LINE ' ';               ->  blank line after subtotal
    """
    # Determine column header label for PROD
    # (LN report uses 'PRODUCT' as the DEFINE label; others use 'PROD')
    prod_header = "PRODUCT" if report_id == "EIILMTLN" else "PROD"

    with open(out_file, "w") as fh:
        page_num   = 0
        line_count = PAGE_LENGTH + 1   # force first-page header

        grand_noacct   = 0
        grand_amount   = 0.0
        grand_apprlim3 = 0.0

        def new_page():
            nonlocal page_num, line_count
            page_num  += 1
            line_count  = 0
            for hdr in _header_lines(sdesc, report_id, repdate,
                                     prod_header, amount_col):
                fh.write(hdr)
                line_count += 1

        def write_line(asa: str, text: str):
            nonlocal line_count
            if line_count >= PAGE_LENGTH:
                new_page()
            fh.write(asa_line(asa, text))
            line_count += 1

        prods = df["PROD"].unique(maintain_order=False).sort().to_list()

        for prod in prods:
            prod_df = df.filter(pl.col("PROD") == prod).sort("BRHCODE")

            prod_noacct   = 0
            prod_amount   = 0.0
            prod_apprlim3 = 0.0

            # Detail rows
            for row in prod_df.to_dicts():
                write_line(ASA_SPACE1, _detail_line(row, amount_col))
                prod_noacct   += int(row.get("NOACCT",    0) or 0)
                prod_amount   += float(row.get(amount_col, 0) or 0)
                prod_apprlim3 += float(row.get("APPRLIM3",  0) or 0)

            # BREAK AFTER PROD: DUL (double underline)
            write_line(ASA_SPACE1, DLINE)
            write_line(ASA_SPACE1, DLINE)
            # DOL SUMMARIZE (subtotal row)
            write_line(ASA_SPACE1,
                       _subtotal_line(prod, prod_noacct, prod_amount, prod_apprlim3))
            # COMPUTE AFTER PROD: LINE ' ';
            write_line(ASA_SPACE1, "")
            # PAGE -> new page after each PROD group
            line_count = PAGE_LENGTH + 1

            grand_noacct   += prod_noacct
            grand_amount   += prod_amount
            grand_apprlim3 += prod_apprlim3

        # RBREAK AFTER: DUL DOL SUMMARIZE (grand total)
        if line_count >= PAGE_LENGTH:
            new_page()
        fh.write(asa_line(ASA_SPACE1, DLINE))
        fh.write(asa_line(ASA_SPACE1, DLINE))
        fh.write(asa_line(ASA_SPACE1,
                 _subtotal_line("", grand_noacct, grand_amount, grand_apprlim3)))

    print(f"Report written -> {out_file}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    # Step 1: derive reporting parameters
    params  = derive_report_params(LOAN_DIR)
    nowk    = params["NOWK"]
    reptmon = params["REPTMON"]
    repdate = params["REPDATE"]

    # Step 2: institution description
    sdesc = load_sdesc(LOAN_DIR)

    # Step 3: branch reference
    brhdata = load_brhdata(BRHFILE)

    # Step 4: load and filter loan data
    ln_raw = load_ln(LOAN_DIR, reptmon, nowk)

    # Step 5: sort LN and BRHDATA by BRANCH, merge, route into product groups
    ln_sorted  = ln_raw.sort("BRANCH")
    brh_sorted = brhdata.sort("BRANCH")
    od_df, hp_df, rc_df, ln_df, ln2_df = route_records(ln_sorted, brh_sorted)

    # ---- OD report ----
    # PROC SUMMARY DATA=OD; VAR APPRLIM2 APPRLIM3;
    od_sum = summarise(od_df, "APPRLIM2")
    write_report(od_sum, "APPRLIM2", "PROD", "EIILMTOD", sdesc, repdate, REPORT_OD)

    # ---- HP report ----
    # PROC SUMMARY DATA=HP; VAR APPRLIMT APPRLIM3;
    hp_sum = summarise(hp_df, "APPRLIMT")
    write_report(hp_sum, "APPRLIMT", "PROD", "EIILMTHP", sdesc, repdate, REPORT_HP)

    # ---- RC report ----
    # PROC SUMMARY DATA=RC; VAR APPRLIM2 APPRLIM3;
    rc_sum = summarise(rc_df, "APPRLIM2")
    write_report(rc_sum, "APPRLIM2", "PROD", "EIILMTRC", sdesc, repdate, REPORT_RC)

    # ---- LN report ----
    # DATA LN; SET LN LN2;  (combine before summary)
    # PROC SUMMARY DATA=LN; VAR APPRLIM2 APPRLIM3;
    ln_combined = pl.concat([ln_df, ln2_df], how="diagonal_relaxed")
    ln_sum = summarise(ln_combined, "APPRLIM2")
    write_report(ln_sum, "APPRLIM2", "PROD", "EIILMTLN", sdesc, repdate, REPORT_LN)


if __name__ == "__main__":
    main()
