#!/usr/bin/env python3
"""
Program  : EIBMLN02.py
Date     : 07.04.99
Report   : 1. BUMI LOANS
           2. RC : AMOUNT APPROVED AND NO OF ACCOUNTS
           3. MOVEMENT OF RC OF RM1,000,000 & ABOVE
           4. LISTING OF LOAN ACCOUNTS FOR M9 REPORTING
"""

import duckdb
import polars as pl
from pathlib import Path
from datetime import date

from PBBLNFMT import format_lnprod, format_lncustcd

# ============================================================================
# CONFIGURATION / PATH SETUP
# ============================================================================

BASE_DIR   = Path(".")
BNM_DIR    = BASE_DIR / "data" / "bnm"
BTR_DIR    = BASE_DIR / "data" / "btr"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Report output file
REPORT_FILE = OUTPUT_DIR / "EIBMLN02.txt"

# Page length (ASA carriage control, 60 lines per page)
PAGE_LENGTH = 60

# OPTIONS YEARCUTOFF=1950 NOCENTER NODATE NONUMBER MISSING=0

# ============================================================================
# HELPER: ASA CARRIAGE CONTROL REPORT WRITER
# ============================================================================

class ReportWriter:
    """Manages ASA carriage control report output."""

    def __init__(self, filepath: Path, page_length: int = PAGE_LENGTH):
        self.filepath    = filepath
        self.page_length = page_length
        self.lines       = []
        self.line_count  = 0
        self.title1      = ""
        self.title2      = ""
        self.title3      = ""
        self.title4      = ""

    def set_titles(self, t1="", t2="", t3="", t4=""):
        self.title1 = t1
        self.title2 = t2
        self.title3 = t3
        self.title4 = t4

    def _build_header(self):
        hdr = []
        if self.title1: hdr.append(self.title1)
        if self.title2: hdr.append(self.title2)
        if self.title3: hdr.append(self.title3)
        if self.title4: hdr.append(self.title4)
        hdr.append("")  # blank line after titles
        return hdr

    def start_section(self):
        """Emit page-break header for a new report section."""
        header = self._build_header()
        for i, h in enumerate(header):
            cc = "1" if i == 0 else " "
            self.lines.append(cc + h)
        self.line_count = len(header)

    def write_line(self, text: str, cc: str = " "):
        """Write a line with given ASA carriage control character."""
        if self.line_count >= self.page_length:
            self._emit_page_break()
        self.lines.append(cc + text)
        self.line_count += 1

    def _emit_page_break(self):
        header = self._build_header()
        self.lines.append("1" + header[0])
        for h in header[1:]:
            self.lines.append(" " + h)
        self.line_count = len(header)

    def write_blank(self):
        self.write_line("")

    def flush(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            f.write("\n".join(self.lines) + "\n")


# ============================================================================
# HELPER: FORMAT NUMBERS
# ============================================================================

def fmt_comma17_2(val) -> str:
    if val is None:
        return " " * 17
    try:
        return f"{float(val):>17,.2f}"
    except Exception:
        return " " * 17


def fmt_comma9(val) -> str:
    if val is None:
        return " " * 9
    try:
        return f"{int(val):>9,}"
    except Exception:
        return " " * 9


# ============================================================================
# STEP 1: READ REPTDATE AND DERIVE MACRO VARIABLES
# ============================================================================

def get_report_date() -> dict:
    reptdate_path = BNM_DIR / "REPTDATE.parquet"
    con = duckdb.connect()
    row = con.execute(f"SELECT REPTDATE FROM read_parquet('{reptdate_path}') LIMIT 1").fetchone()
    con.close()
    reptdate: date = row[0]

    day   = reptdate.day
    month = reptdate.month

    if   day == 8:  nowk = "1"
    elif day == 15: nowk = "2"
    elif day == 22: nowk = "3"
    else:           nowk = "4"

    reptday  = str(day).zfill(2)
    reptmon  = str(month).zfill(2)
    rdate    = reptdate.strftime("%d/%m/%Y")
    mm1      = month - 1 if month > 1 else 12
    reptmon1 = str(mm1).zfill(2)

    return {
        "nowk":     nowk,
        "reptday":  reptday,
        "reptmon":  reptmon,
        "rdate":    rdate,
        "reptmon1": reptmon1,
        "reptdate": reptdate,
    }


# ============================================================================
# STEP 2: READ SDESC
# ============================================================================

def get_sdesc() -> str:
    sdesc_path = BNM_DIR / "SDESC.parquet"
    con = duckdb.connect()
    row = con.execute(f"SELECT SDESC FROM read_parquet('{sdesc_path}') LIMIT 1").fetchone()
    con.close()
    return str(row[0])[:26] if row else ""


# ============================================================================
# BUMI CUSTOMER CODES
# %LET CUSS=('41','42','43','52','53','54','57','59','61',
#            '64','66','69','71','72','73','74','75','77');
# format_lncustcd() from PBBLNFMT maps raw integer CUSTCD to 2-char formatted code.
# CUSS is the set of those formatted codes that represent Bumiputra-related entities.
# ============================================================================

CUSS = {"41","42","43","52","53","54","57","59","61",
        "64","66","69","71","72","73","74","75","77"}


def is_bumi_custcd(custcd_raw) -> bool:
    """
    Return True if the formatted customer code falls within CUSS.
    Uses format_lncustcd() from PBBLNFMT to map raw integer CUSTCD to 2-char code,
    equivalent to the SAS IF CUSTCD IN &CUSS check after PROC FORMAT.
    """
    try:
        return format_lncustcd(int(custcd_raw)) in CUSS
    except (TypeError, ValueError):
        return False


# ============================================================================
# SECTION 1: BUMI LOANS (TL/OD/RC)
# (TL INCLUDES SPTF,STAFF,CAGAMAS,TUK)
# ============================================================================

# Loan type classification based on PRODCD values produced by format_lnprod()
# from PBBLNFMT. These match exactly the WHEN clauses in the SAS SELECT(PRODCD).
TL_PRODCDS       = {"34111","34112","34113","34114","54120","34320",
                    "34115","34116","34117","34120","34149","34230"}
OD_PRODCDS       = {"34180","34240","34380"}
RC_PRODCDS       = {"34190"}
OD_EXCL_PRODUCTS = {150, 151, 152}


def classify_lntyp(prodcd: str, product: int) -> str | None:
    """
    Classify a loan row into LNTYP (TL/OD/RC) using the PRODCD value that was
    produced by format_lnprod() in PBBLNFMT, mirroring the SAS SELECT(PRODCD) block.
    Returns None if the row should be excluded (OTHERWISE / DELETE).
    """
    if prodcd in TL_PRODCDS:
        return "TL"
    if prodcd in OD_PRODCDS:
        # IF PRODUCT IN (150,151,152) THEN DELETE inside the OD WHEN block
        if product in OD_EXCL_PRODUCTS:
            return None
        return "OD"
    if prodcd in RC_PRODCDS:
        return "RC"
    return None


def build_loan_data(reptmon: str, nowk: str, reptmon1: str) -> tuple:
    """
    Load LOAN, ULOAN, BTRAD (current month) and prior-month LOAN parquet files.
    Returns (loan_df, uloan_df, bt_df, loan1_df) as Polars DataFrames.
    """
    loan_path  = BNM_DIR / f"LOAN{reptmon}{nowk}.parquet"
    uloan_path = BNM_DIR / f"ULOAN{reptmon}{nowk}.parquet"
    bt_path    = BTR_DIR / f"BTRAD{reptmon}{nowk}.parquet"
    loan1_path = BNM_DIR / f"LOAN{reptmon1}{nowk}.parquet"

    con = duckdb.connect()

    # LOAN dataset: remove OD rows with PRODUCT in (151,152,181)
    loan_df = con.execute(f"""
        SELECT *
        FROM read_parquet('{loan_path}')
        WHERE NOT (ACCTYPE = 'OD' AND PRODUCT IN (151,152,181))
    """).pl()

    # ULOAN dataset: remove accounts 3000000000-3999999999 with PRODUCT in (151,152,181)
    uloan_df = con.execute(f"""
        SELECT *
        FROM read_parquet('{uloan_path}')
        WHERE NOT (ACCTNO BETWEEN 3000000000 AND 3999999999
                   AND PRODUCT IN (151,152,181))
    """).pl()

    # BT trade dataset: filter PRODCD starting with '34'
    bt_df = con.execute(f"""
        SELECT *
        FROM read_parquet('{bt_path}')
        WHERE SUBSTR(PRODCD,1,2) = '34'
    """).pl()

    # Prior-month LOAN for MOVEMENT section (RC only, PRODCD='34190')
    loan1_df = con.execute(f"""
        SELECT BRANCH, ACCTNO, NAME, BALANCE
        FROM read_parquet('{loan1_path}')
        WHERE PRODCD = '34190'
    """).pl()

    con.close()
    return loan_df, uloan_df, bt_df, loan1_df


def section1_bumi_loans(loan_df: pl.DataFrame, bt_df: pl.DataFrame,
                        rdate: str, sdesc: str, writer: ReportWriter):
    """
    BUMI LOANS (TL/OD/RC).
    - CUSTCD filtered via format_lncustcd() from PBBLNFMT mapped against CUSS.
    - LNTYP assigned using PRODCD codes produced by format_lnprod() from PBBLNFMT
      via classify_lntyp(), mirroring the SAS SELECT(PRODCD) block.
    """
    rows = []

    for row in loan_df.iter_rows(named=True):
        # format_lncustcd() maps raw CUSTCD integer to 2-char code; check against CUSS
        if not is_bumi_custcd(row.get("CUSTCD")):
            continue

        # PRODCD on the dataset was produced by format_lnprod() equivalent upstream;
        # classify_lntyp() uses those PRODCD codes to assign TL/OD/RC.
        prodcd  = str(row.get("PRODCD", "") or "").strip()
        product = int(row.get("PRODUCT", 0) or 0)
        lntyp   = classify_lntyp(prodcd, product)
        if lntyp is None:
            continue

        rows.append({
            "BRANCH":  row.get("BRANCH", ""),
            "LNTYP":   lntyp,
            "BALANCE": float(row.get("BALANCE", 0) or 0),
        })

    # BT trade data — LNTYP='BT', CUSTCD filtered via format_lncustcd against CUSS
    for row in bt_df.iter_rows(named=True):
        if not is_bumi_custcd(row.get("CUSTCD")):
            continue
        rows.append({
            "BRANCH":  row.get("BRANCH", ""),
            "LNTYP":   "BT",
            "BALANCE": float(row.get("BALANCE", 0) or 0),
        })

    if not rows:
        return

    df       = pl.DataFrame(rows)
    lntypes  = sorted(df["LNTYP"].unique().to_list())
    branches = sorted(df["BRANCH"].unique().to_list())

    writer.set_titles(
        t1=sdesc,
        t2=f"BUMI LOANS AS AT {rdate}",
    )
    writer.start_section()

    col_w = 20
    hdr   = f"{'BRANCH':<9}"
    for lt in lntypes:
        hdr += f"  {lt:>{col_w}}{'NUMBER':>9}"
    hdr += f"  {'TOTAL':>{col_w}}{'NUMBER':>9}"
    writer.write_line(hdr)
    writer.write_line("-" * len(hdr))

    grand_sum   = 0.0
    grand_count = 0
    lt_totals   = {lt: (0.0, 0) for lt in lntypes}

    for branch in branches:
        bdf   = df.filter(pl.col("BRANCH") == branch)
        line  = f"{str(branch):<9}"
        b_sum = 0.0
        b_cnt = 0
        for lt in lntypes:
            sub = bdf.filter(pl.col("LNTYP") == lt)
            s   = float(sub["BALANCE"].sum()) if len(sub) > 0 else 0.0
            n   = len(sub)
            line += f"  {fmt_comma17_2(s)}{fmt_comma9(n)}"
            b_sum += s
            b_cnt += n
            lt_totals[lt] = (lt_totals[lt][0] + s, lt_totals[lt][1] + n)
        line += f"  {fmt_comma17_2(b_sum)}{fmt_comma9(b_cnt)}"
        grand_sum   += b_sum
        grand_count += b_cnt
        writer.write_line(line)

    writer.write_line("-" * len(hdr))
    total_line = f"{'TOTAL':<9}"
    for lt in lntypes:
        s, n = lt_totals[lt]
        total_line += f"  {fmt_comma17_2(s)}{fmt_comma9(n)}"
    total_line += f"  {fmt_comma17_2(grand_sum)}{fmt_comma9(grand_count)}"
    writer.write_line(total_line)
    writer.write_blank()


# ============================================================================
# SECTION 2: RC - AMOUNT APPROVED AND NO OF ACCOUNTS
# ============================================================================

def section2_rc_approved(loan_df: pl.DataFrame, uloan_df: pl.DataFrame,
                         rdate: str, sdesc: str, writer: ReportWriter):
    """
    RC: AMOUNT APPROVED AND NO. OF ACCOUNTS.
    Merges LOAN and ULOAN sorted by ACCTNO, keeps FIRST.ACCTNO where PRODCD='34190'.
    PRODCD='34190' is the revolving credit code produced by format_lnprod() in PBBLNFMT.
    """
    rc_loan  = loan_df.filter(pl.col("PRODCD") == "34190").select(["ACCTNO","BRANCH","APPRLIMT"])
    rc_uloan = uloan_df.filter(pl.col("PRODCD") == "34190").select(["ACCTNO","BRANCH","APPRLIMT"])

    # Concatenate and keep first occurrence per ACCTNO (equivalent to FIRST.ACCTNO after BY ACCTNO)
    combined = (pl.concat([rc_loan, rc_uloan])
                .sort("ACCTNO")
                .unique(subset=["ACCTNO"], keep="first"))

    branches = sorted(combined["BRANCH"].unique().to_list())

    writer.set_titles(
        t1=sdesc,
        t2=f"RC : AMOUNT APPROVED AND NO. OF ACCOUNTS AS AT {rdate}",
    )
    writer.start_section()

    hdr = f"{'BRANCH':<9}  {'AMOUNT':>17}{'NUMBER':>9}"
    writer.write_line(hdr)
    writer.write_line("-" * len(hdr))

    grand_sum = 0.0
    grand_cnt = 0
    for branch in branches:
        sub = combined.filter(pl.col("BRANCH") == branch)
        s   = float(sub["APPRLIMT"].sum())
        n   = len(sub)
        writer.write_line(f"{str(branch):<9}  {fmt_comma17_2(s)}{fmt_comma9(n)}")
        grand_sum += s
        grand_cnt += n

    writer.write_line("-" * len(hdr))
    writer.write_line(f"{'TOTAL':<9}  {fmt_comma17_2(grand_sum)}{fmt_comma9(grand_cnt)}")
    writer.write_blank()


# ============================================================================
# SECTION 3: MOVEMENT OF RC OF RM1,000,000 & ABOVE
# ============================================================================

def section3_rc_movement(loan_df: pl.DataFrame, loan1_df: pl.DataFrame,
                         rdate: str, sdesc: str, writer: ReportWriter):
    """
    Movement of RC accounts (PRODCD='34190') where |CURBAL - PREVBAL| > 1,000,000.
    PRODCD='34190' is the revolving credit code produced by format_lnprod() in PBBLNFMT.
    """
    cur = (loan_df.filter(pl.col("PRODCD") == "34190")
           .group_by(["BRANCH","ACCTNO","NAME"])
           .agg(pl.col("BALANCE").sum().alias("CURBAL")))

    prev = (loan1_df
            .group_by(["BRANCH","ACCTNO","NAME"])
            .agg(pl.col("BALANCE").sum().alias("PREVBAL")))

    merged = cur.join(prev, on=["BRANCH","ACCTNO"], how="full", suffix="_prev")
    merged = merged.with_columns([
        pl.col("CURBAL").fill_null(0.0),
        pl.col("PREVBAL").fill_null(0.0),
    ])
    merged = merged.with_columns(
        (pl.col("CURBAL") - pl.col("PREVBAL")).alias("MOVEMENT")
    )
    merged = (merged
              .filter(pl.col("MOVEMENT").abs() > 1_000_000)
              .sort(["BRANCH","ACCTNO"]))

    writer.set_titles(
        t1=sdesc,
        t2=f"MOVEMENT OF RC OF RM1,000,000 AND ABOVE AS AT {rdate}",
    )
    writer.start_section()

    hdr = (f"{'BRANCH':<9} {'ACCTNO':>12} {'NAME':<30} "
           f"{'CURRENT BALANCE':>17} {'PREVIOUS BALANCE':>17} {'MOVEMENT':>17}")
    writer.write_line(hdr)
    writer.write_line("-" * len(hdr))

    sum_cur  = 0.0
    sum_prev = 0.0
    sum_mov  = 0.0
    for row in merged.iter_rows(named=True):
        cur_b  = float(row["CURBAL"]   or 0)
        pre_b  = float(row["PREVBAL"]  or 0)
        mov    = float(row["MOVEMENT"] or 0)
        name   = str(row.get("NAME","")   or "")[:30]
        acctno = str(row.get("ACCTNO","") or "")
        branch = str(row.get("BRANCH","") or "")
        writer.write_line(
            f"{branch:<9} {acctno:>12} {name:<30} "
            f"{fmt_comma17_2(cur_b)} {fmt_comma17_2(pre_b)} {fmt_comma17_2(mov)}"
        )
        sum_cur  += cur_b
        sum_prev += pre_b
        sum_mov  += mov

    writer.write_line("-" * len(hdr))
    writer.write_line(
        f"{'':9} {'':>12} {'':30} "
        f"{fmt_comma17_2(sum_cur)} {fmt_comma17_2(sum_prev)} {fmt_comma17_2(sum_mov)}"
    )
    writer.write_blank()


# ============================================================================
# SECTION 4: LISTING OF LOAN ACCOUNTS FOR M9 REPORTING
# ============================================================================

# WHERE CUSTCODE IN (11,12,13,30,32,33,34,35,36,04,05,06,37,38,39,40)
M9_CUSTCODES = {11,12,13,30,32,33,34,35,36,4,5,6,37,38,39,40}


def section4_m9_listing(loan_df: pl.DataFrame, rdate: str,
                        sdesc: str, writer: ReportWriter):
    """
    Listing of loan accounts for M9 reporting.
    Filters by raw CUSTCODE integer values as specified in the SAS WHERE clause.
    Columns: ACCTNO NOTENO NAME BALANCE CUSTCODE, grouped BY BRANCH, SUM BALANCE.
    """
    m9 = (loan_df
          .filter(pl.col("CUSTCODE").is_in(list(M9_CUSTCODES)))
          .select(["BRANCH","ACCTNO","NOTENO","NAME","BALANCE","CUSTCODE"])
          .sort(["BRANCH","ACCTNO"]))

    writer.set_titles(
        t1=sdesc,
        t2=f"LISTING OF LOAN ACCOUNTS FOR M9 REPORTING AS AT {rdate}",
        t3="(FOR BNM/BRG/M9 REPORT - LOANS TO BUMIPUTRA COMMUNITY)",
    )
    writer.start_section()

    hdr = (f"{'ACCTNO':>14} {'NOTENO':>10} {'NAME':<30} "
           f"{'BALANCE':>17} {'CUSTCODE':>8}")
    writer.write_line(hdr)

    branches = sorted(m9["BRANCH"].unique().to_list())
    for branch in branches:
        writer.write_line(f"BRANCH: {branch}")
        bdf        = m9.filter(pl.col("BRANCH") == branch)
        branch_sum = 0.0
        for row in bdf.iter_rows(named=True):
            balance = float(row["BALANCE"] or 0)
            branch_sum += balance
            writer.write_line(
                f"{str(row['ACCTNO']   or ''):>14} "
                f"{str(row['NOTENO']   or ''):>10} "
                f"{str(row['NAME']     or '')[:30]:<30} "
                f"{fmt_comma17_2(balance)} "
                f"{str(row['CUSTCODE'] or ''):>8}"
            )
        writer.write_line("-" * 82)
        writer.write_line(
            f"{'':>14} {'':>10} {'':30} {fmt_comma17_2(branch_sum)} {'':>8}"
        )
        writer.write_blank()


# ============================================================================
# MAIN
# ============================================================================

def main():
    params   = get_report_date()
    sdesc    = get_sdesc()
    reptmon  = params["reptmon"]
    reptmon1 = params["reptmon1"]
    nowk     = params["nowk"]
    rdate    = params["rdate"]

    loan_df, uloan_df, bt_df, loan1_df = build_loan_data(reptmon, nowk, reptmon1)

    writer = ReportWriter(REPORT_FILE, PAGE_LENGTH)

    # Section 1: Bumi Loans (TL/OD/RC)
    section1_bumi_loans(loan_df, bt_df, rdate, sdesc, writer)

    # Section 2: RC Amount Approved and No. of Accounts
    section2_rc_approved(loan_df, uloan_df, rdate, sdesc, writer)

    # Section 3: Movement of RC of RM1,000,000 & Above
    section3_rc_movement(loan_df, loan1_df, rdate, sdesc, writer)

    # Section 4: Listing of Loan Accounts for M9 Reporting
    section4_m9_listing(loan_df, rdate, sdesc, writer)

    writer.flush()
    print(f"Report written to: {REPORT_FILE}")


if __name__ == "__main__":
    main()
