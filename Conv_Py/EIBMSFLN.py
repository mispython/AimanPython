#!/usr/bin/env python3
"""
Program  : EIBMSFLN.py
Purpose  : Staff Loan Position for HRD (Monthly) - PUBLIC BANK BERHAD / PUBLIC ISLAMIC BANK BERHAD
           Report 1 (PROC TABULATE): Summary by PRODIND / PROD
           Report 2 (PROC REPORT):   Detail listing by PRODUCT / BRANCH / NAME
           REPORT ID: EIBMSFLN
"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
"""
- The original SAS program includes %INC PGM(PBBLNFMT) which registers all
    PBBLNFMT formats globally in SAS.
- However, none of those formats (ODDENOM, LNDENOM, LNPROD, CUSTCD, STATECD, etc.)
    are actually called anywhere in this program.
- The only format used is LOANFMT, which is defined locally above.
- Therefore no import from PBBLNFMT is required here.
"""

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR       = Path("/data")
BNM_DIR        = BASE_DIR / "bnm"
BNMI_DIR       = BASE_DIR / "bnmi"
BNM1_DIR       = BASE_DIR / "bnm1"
BNM2_DIR       = BASE_DIR / "bnm2"

REPTDATE_FILE  = BNM1_DIR / "reptdate.parquet"
LNNOTE1_FILE   = BNM1_DIR / "lnnote.parquet"
LNNOTE2_FILE   = BNM2_DIR / "lnnote.parquet"

OUTPUT_DIR     = BASE_DIR / "output"
OUTPUT_REPORT  = OUTPUT_DIR / "eibmsfln_report.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# REPORT / PAGE CONSTANTS
# ============================================================================

PAGE_LENGTH = 60    # OPTIONS PS=60
LINE_WIDTH  = 132   # OPTIONS LS=132

# ============================================================================
# PROC FORMAT: VALUE LOANFMT (numeric PRODUCT -> label)
# ============================================================================

def fmt_loanfmt(product) -> str:
    """
    Replicate SAS PROC FORMAT VALUE LOANFMT for EIBMSFLN.
    Maps numeric PRODUCT code to descriptive label string.
    """
    if product is None:
        return ""
    try:
        p = int(product)
    except (TypeError, ValueError):
        return ""

    if p in (4, 5):         return "04-05 - HOUSING LOAN"
    elif p in (6, 7):       return "06-07 - HL ALTERNATIVE FINANCING"
    elif p == 10:           return "10 - FESTIVE ADVANCES"
    elif p == 15:           return "15 - CAR LOAN"
    elif p == 20:           return "20 - MOTORCYCLE LOAN"
    elif p == 25:           return "25 - PURCHASE OF COMPUTER"
    elif p == 26:           return "26 - MEDICAL EXPENSES"
    elif p == 27:           return "27 - FUNERAL EXPENSES"
    elif p == 28:           return "28 - DISASTER RELIEF"
    elif p in (29, 30):     return "29-30 - PURSUIT OF FURTHER STUDIES"
    elif p in (31, 32):     return "31-32 - RENOVATION LOAN"
    elif p == 33:           return "33 - ECOPARK MEMBERSHIP"
    elif p == 34:           return "34 - OTHER PURPOSE"
    elif p in (60, 61):     return "60-61 - RENOVATION LOAN"
    elif p == 62:           return "62 - PIVB RENOVATION (PROG)"
    elif p == 63:           return "63 - HHB CAR LOAN"
    elif p == 70:           return "70 - PIVB HOUSING LOAN"
    elif p == 71:           return "71 - PIVB CAR LOAN"
    elif p == 72:           return "72 - PIVB MOTOCYCLE LOAN"
    elif p == 73:           return "73 - PIVB PURCHASE OF COMPUTER"
    elif p == 74:           return "74 - PIVB MEDICAL EXPENSES"
    elif p == 75:           return "75 - PIVB FUNERAL EXPENSES"
    elif p == 76:           return "76 - PIVB DISASTER RELIEF"
    elif p == 77:           return "77 - PIVB STUDY LOAN"
    elif p == 78:           return "78 - PIVB RENOVATION"
    elif p == 100:          return "100 - STAFF ISLAMIC LOAN"
    elif p == 102:          return "102 - HOUSE FINANCING"
    elif p == 103:          return "103 - CAR FINANCING"
    elif p == 104:          return "104 - MOTORCYCLE FINANCING"
    elif p == 105:          return "105 - RENOVATION FINANCING"
    elif p == 106:          return "106 - HOUSE FINANCING"
    elif p == 107:          return "107 - CAR FINANCING"
    elif p == 108:          return "108 - MOTORCYCLE FINANCING"
    else:                   return ""


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def fmt_ddmmyy10(d: date) -> str:
    """Format date as DD/MM/YYYY (SAS DDMMYY10.)"""
    return d.strftime("%d/%m/%Y")


def fmt_ddmmyy8(d: date) -> str:
    """Format date as DD/MM/YY (SAS DDMMYY8.)"""
    return d.strftime("%d/%m/%y")


def fmt_z2(n: int) -> str:
    return f"{n:02d}"


def fmt_year4(d: date) -> str:
    return d.strftime("%Y")


def fmt_worddate9(d: date) -> str:
    """
    Replicate SAS WORDDATE9. then UPCASE.
    SAS WORDDATE9. produces abbreviated month + year e.g. 'Jan 2001' -> UPCASE -> 'JAN 2001'.
    """
    return d.strftime("%b %Y").upper()


def fmt_comma18_2(val) -> str:
    """SAS COMMA18.2 format"""
    if val is None:
        return " " * 18
    try:
        return f"{float(val):,.2f}".rjust(18)
    except (TypeError, ValueError):
        return " " * 18


def fmt_comma15_2(val) -> str:
    """SAS COMMA15.2 format"""
    if val is None:
        return " " * 15
    try:
        return f"{float(val):,.2f}".rjust(15)
    except (TypeError, ValueError):
        return " " * 15


def fmt_comma10(val) -> str:
    """SAS COMMA10. format (integer)"""
    if val is None:
        return " " * 10
    try:
        return f"{int(val):,}".rjust(10)
    except (TypeError, ValueError):
        return " " * 10


def fmt_8_0(val) -> str:
    """SAS F=8.0 format"""
    if val is None:
        return "0".rjust(8)
    try:
        return str(int(val)).rjust(8)
    except (TypeError, ValueError):
        return "0".rjust(8)


def fmt_5_2(val) -> str:
    """SAS FORMAT=5.2"""
    if val is None:
        return " " * 5
    try:
        return f"{float(val):.2f}".rjust(5)
    except (TypeError, ValueError):
        return " " * 5


# ============================================================================
# STEP 1: READ REPTDATE & DERIVE MACRO VARIABLES
# ============================================================================

con = duckdb.connect()

reptdate_df = con.execute(
    f"SELECT * FROM read_parquet('{REPTDATE_FILE}')"
).pl()
row_rep = reptdate_df.row(0, named=True)

REPTDATE: date = row_rep["REPTDATE"]
if isinstance(REPTDATE, datetime):
    REPTDATE = REPTDATE.date()

day = REPTDATE.day
if day == 8:
    WK = "1"
elif day == 15:
    WK = "2"
elif day == 22:
    WK = "3"
else:
    WK = "4"

NOWK    = WK
REPTMON = fmt_z2(REPTDATE.month)
RDATE   = fmt_ddmmyy10(REPTDATE)
RYEAR   = fmt_year4(REPTDATE)
WMONTH  = fmt_worddate9(REPTDATE)   # e.g. 'JAN 2001'

# ============================================================================
# STEP 2: DATA LOANTEMP
#   SET BNM.LOAN&REPTMON&NOWK + BNMI.LOAN&REPTMON&NOWK
#   Filter: PRODCD='34230' AND CURBAL >= 0
#   Derive: CUNDRAW, PROD, NOOFAC, PRODIND
#   Exclude: PRODUCT NOT IN (62,63,70,71,72,73,74,75,76,77,78,79,100,101,106,107,108)
# ============================================================================

loan_file_bnm  = BNM_DIR  / f"loan{REPTMON}{NOWK}.parquet"
loan_file_bnmi = BNMI_DIR / f"loan{REPTMON}{NOWK}.parquet"

# IF PRODUCT NOT IN (...) filter — products excluded from PBB report
LOANTEMP_EXCLUDE = {62, 63, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
                    100, 101, 106, 107, 108}

# PRODIND Islamic classification
PRODIND_ISLAMIC = {102, 103, 104, 105}

raw_loan = con.execute(f"""
    SELECT * FROM read_parquet('{loan_file_bnm}')
    WHERE PRODCD = '34230' AND CURBAL >= 0
    UNION ALL
    SELECT * FROM read_parquet('{loan_file_bnmi}')
    WHERE PRODCD = '34230' AND CURBAL >= 0
""").pl()


def build_loantemp(df: pl.DataFrame) -> pl.DataFrame:
    """
    Replicate DATA LOANTEMP:
    - RETAIN CUNDRAW: CUNDRAW = UNDRAWN if UNDRAWN > 0, else missing (None)
    - PROD = PUT(PRODUCT, LOANFMT.)
    - NOOFAC = 1
    - PRODIND: IF PRODUCT IN (102,103,104,105) -> 'STAFF ISLAMIC FINANCING'
               ELSE -> 'STAFF CONVENTIONAL LOAN'
    - IF PRODUCT NOT IN (62,63,...) filter (keep only matching rows)
    """
    rows = df.to_dicts()
    out  = []
    for r in rows:
        product = r.get("PRODUCT")
        undrawn = float(r.get("UNDRAWN") or 0)

        # RETAIN CUNDRAW: set to UNDRAWN when > 0, else missing (SAS .)
        r["CUNDRAW"] = undrawn if undrawn > 0 else None

        # PROD label from LOANFMT format
        r["PROD"] = fmt_loanfmt(product)

        # NOOFAC = 1
        r["NOOFAC"] = 1

        # PRODIND classification
        p = int(product) if product is not None else 0
        if p in PRODIND_ISLAMIC:
            r["PRODIND"] = "STAFF ISLAMIC FINANCING"
        else:
            r["PRODIND"] = "STAFF CONVENTIONAL LOAN"

        # IF PRODUCT NOT IN (...): exclude these products from PBB report
        if p in LOANTEMP_EXCLUDE:
            continue

        out.append(r)

    return pl.DataFrame(out) if out else pl.DataFrame()


loantemp = build_loantemp(raw_loan).sort(["ACCTNO", "NOTENO"])

# ============================================================================
# STEP 3: DATA LNOTE
#   SET BNM1.LNNOTE + BNM2.LNNOTE; KEEP ACCTNO NOTENO ORGTYPE
# ============================================================================

lnote = con.execute(f"""
    SELECT ACCTNO, NOTENO, ORGTYPE FROM read_parquet('{LNNOTE1_FILE}')
    UNION ALL
    SELECT ACCTNO, NOTENO, ORGTYPE FROM read_parquet('{LNNOTE2_FILE}')
""").pl().sort(["ACCTNO", "NOTENO"])

# ============================================================================
# STEP 4: DATA LOANA
#   MERGE LOANTEMP(IN=A) LNOTE; BY ACCTNO NOTENO;
#   IF A;  IF ORGTYPE='S' THEN DELETE;
# ============================================================================

loana = (
    loantemp.join(lnote, on=["ACCTNO", "NOTENO"], how="left", suffix="_LN")
            .filter(pl.col("ORGTYPE").cast(pl.Utf8).fill_null("") != "S")
)

# PROC SORT DATA=LOANA; BY PRODUCT;
loana_by_product = loana.sort("PRODUCT")

# PROC SORT DATA=LOANA OUT=LOANB; BY PRODUCT BRANCH NAME;
loanb = loana.sort(["PRODUCT", "BRANCH", "NAME"])

# ============================================================================
# STEP 5: PROC TABULATE RENDERING
#   TABLE PRODIND=' '*(PROD=' ' ALL='SUB-TOTAL') ALL='GRAND TOTAL',
#         APPRLIMT*(N='NO. OF ACCOUNT'*F=8.0 SUM='AMOUNT'*F=COMMA18.2)
#         CURBAL  *(N='NO. OF ACCOUNT'*F=8.0 SUM='AMOUNT'*F=COMMA18.2)
#         CUNDRAW *(N='NO. OF ACCOUNT'*F=8.0 SUM='AMOUNT'*F=COMMA18.2)
#   /RTS=40 BOX='PRODUCT TYPE'
# ============================================================================

def render_tabulate(
    df: pl.DataFrame,
    title1: str,
    title2: str,
    footnote: str,
    lines: list[str],
    rts: int = 40,
) -> None:
    """
    Replicate PROC TABULATE:
    Rows: PRODIND * (PROD + SUB-TOTAL) + GRAND TOTAL
    Cols: APPRLIMT*(N, SUM) | CURBAL*(N, SUM) | CUNDRAW*(N, SUM)
    /RTS=40 BOX='PRODUCT TYPE'
    ASA '1' = new page, ' ' = single line advance.
    MISSING=0: missing numeric cells shown as 0.
    """
    sep = "-" * LINE_WIDTH

    # Box header and column headings
    hdr1 = (
        f"{'PRODUCT TYPE':<{rts}}"
        f"  {'LOANS APPROVED AMOUNT':<32}"
        f"  {'OUTSTANDING AS AT END OF THIS MONTH':<38}"
        f"  {'UNDRAWN AMOUNT':<32}"
    )
    hdr2 = (
        f"{'':>{rts}}"
        f"  {'NO. OF ACCOUNT':>14}  {'AMOUNT':>16}"
        f"  {'NO. OF ACCOUNT':>14}  {'AMOUNT':>16}"
        f"  {'NO. OF ACCOUNT':>14}  {'AMOUNT':>16}"
    )

    # ASA '1' = form-feed / new page
    lines.append(f"1{title1.center(LINE_WIDTH)}")
    lines.append(f" {title2.center(LINE_WIDTH)}")
    lines.append(f" ")
    lines.append(f" {hdr1}")
    lines.append(f" {hdr2}")
    lines.append(f" {sep}")

    # Accumulate into nested dict: prodind -> prod -> stats
    struct: dict[str, dict[str, dict[str, float]]] = {}
    for r in df.iter_rows(named=True):
        prodind = str(r.get("PRODIND") or "")
        prod    = str(r.get("PROD")    or "")
        appr    = float(r.get("APPRLIMT") or 0)
        curbal  = float(r.get("CURBAL")   or 0)
        cundraw = r.get("CUNDRAW")  # None = SAS missing (.)

        struct.setdefault(prodind, {}).setdefault(prod, {
            "appr_n": 0, "appr_sum": 0.0,
            "curbal_n": 0, "curbal_sum": 0.0,
            "cundraw_n": 0, "cundraw_sum": 0.0,
        })
        struct[prodind][prod]["appr_n"]    += 1
        struct[prodind][prod]["appr_sum"]  += appr
        struct[prodind][prod]["curbal_n"]  += 1
        struct[prodind][prod]["curbal_sum"] += curbal
        if cundraw is not None:
            struct[prodind][prod]["cundraw_n"]   += 1
            struct[prodind][prod]["cundraw_sum"] += float(cundraw)

    def fmt_row(label: str, d: dict) -> str:
        return (
            f" {label:<{rts}}"
            f"  {fmt_8_0(d['appr_n'])}  {fmt_comma18_2(d['appr_sum'])}"
            f"  {fmt_8_0(d['curbal_n'])}  {fmt_comma18_2(d['curbal_sum'])}"
            f"  {fmt_8_0(d['cundraw_n'])}  {fmt_comma18_2(d['cundraw_sum'])}"
        )

    def zero_acc() -> dict:
        return {"appr_n": 0, "appr_sum": 0.0,
                "curbal_n": 0, "curbal_sum": 0.0,
                "cundraw_n": 0, "cundraw_sum": 0.0}

    def add_acc(dst: dict, src: dict) -> None:
        for k in dst:
            dst[k] += src.get(k, 0)

    grand = zero_acc()

    for prodind in sorted(struct.keys()):
        subtotal = zero_acc()
        for prod in sorted(struct[prodind].keys()):
            d = struct[prodind][prod]
            lines.append(fmt_row(f"{prodind}  {prod}", d))
            add_acc(subtotal, d)

        # ALL='SUB-TOTAL' row per PRODIND group
        lines.append(f" {sep}")
        lines.append(fmt_row(f"{prodind}  SUB-TOTAL", subtotal))
        lines.append(f" {sep}")
        add_acc(grand, subtotal)

    # ALL='GRAND TOTAL' row
    lines.append(f" {sep}")
    lines.append(fmt_row("GRAND TOTAL", grand))
    lines.append(f" {sep}")
    lines.append(f" ")
    lines.append(f" {footnote}")
    lines.append(f" ")


# ============================================================================
# STEP 6: PROC REPORT RENDERING
#   COLUMN PRODUCT BRANCH NAME ACCTNO NOTENO ISSDTE INTRATE
#          APPRLIMT UNDRAWN CURBAL NOOFAC
#   BREAK AFTER BRANCH: LINE @020 112*'-'
#   BREAK AFTER PRODUCT /PAGE: SUB TOTAL + 132*'-'
#   RBREAK AFTER /PAGE: 132*'-' + GRAND TOTAL + 132*'-'
# ============================================================================

DASH_132 = "-" * 132
DASH_112 = "-" * 112


def render_report(
    df: pl.DataFrame,
    title1: str,
    title2: str,
    footnote: str,
    lines: list[str],
) -> None:
    """
    Replicate PROC REPORT DATA=LOANB SPLIT='*' NOWINDOWS HEADSKIP:
      BREAK AFTER BRANCH: separator at column 20 (112 dashes)
      BREAK AFTER PRODUCT /PAGE: sub-total line + 132 dashes, then new page
      RBREAK AFTER /PAGE: grand total page
    Column positions replicate SAS @001 ... @030 ... @082 @099 @116.
    """

    col_hdr1 = (
        f" {'PRODUCT':>7}  {'BRANCH':>6}  {'NAME':<24}  "
        f"{'A/C NO':>10}  {'NOTE':>7}  {'ISSUE':>10}  "
        f"{'INT.':>5}  {'LOAN APPROVED':>15}  "
        f"{'UNDRAWN':>15}  {'O/S BALANCE':>15}"
    )
    col_hdr2 = (
        f" {'':>7}  {'':>6}  {'':24}  "
        f"{'':>10}  {'NO':>7}  {'DATE':>10}  "
        f"{'RATE':>5}  {'AMOUNT':>15}  "
        f"{'AMOUNT':>15}  {'':>15}"
    )

    def page_header() -> None:
        lines.append(f"1{title1.center(LINE_WIDTH)}")
        lines.append(f" {title2.center(LINE_WIDTH)}")
        lines.append(f" ")
        lines.append(col_hdr1)
        lines.append(col_hdr2)
        lines.append(f" ")  # HEADSKIP blank line after header

    line_count_ref = [PAGE_LENGTH]  # force new page at first product

    product_vals = sorted(df["PRODUCT"].drop_nulls().unique().to_list())

    gt_noofac  = 0
    gt_appr    = 0.0
    gt_undrawn = 0.0
    gt_curbal  = 0.0

    for prod_val in product_vals:
        # BREAK AFTER PRODUCT /PAGE -> new page per product group
        page_header()
        line_count_ref[0] = 6

        prod_df   = df.filter(pl.col("PRODUCT") == prod_val)
        p_noofac  = 0
        p_appr    = 0.0
        p_undrawn = 0.0
        p_curbal  = 0.0

        branch_vals = sorted(prod_df["BRANCH"].drop_nulls().unique().to_list())

        for branch_val in branch_vals:
            branch_df = prod_df.filter(pl.col("BRANCH") == branch_val)

            for row in branch_df.sort("NAME").iter_rows(named=True):
                if line_count_ref[0] >= PAGE_LENGTH - 4:
                    page_header()
                    line_count_ref[0] = 6

                issdte = row.get("ISSDTE")
                if isinstance(issdte, datetime):
                    issdte_str = fmt_ddmmyy8(issdte.date())
                elif isinstance(issdte, date):
                    issdte_str = fmt_ddmmyy8(issdte)
                else:
                    issdte_str = str(issdte or "").strip()[:10]

                # Column positions: @001=1, @030=30, @082=82, @099=99, @116=116
                lines.append(
                    f" {str(prod_val or ''):>7}  "
                    f"{str(branch_val or ''):>6}  "
                    f"{str(row.get('NAME') or '')[:24]:<24}  "
                    f"{str(row.get('ACCTNO') or ''):>10}  "
                    f"{str(row.get('NOTENO') or ''):>7}  "
                    f"{issdte_str:>10}  "
                    f"{fmt_5_2(row.get('INTRATE'))}  "
                    f"{fmt_comma15_2(row.get('APPRLIMT'))}  "
                    f"{fmt_comma15_2(row.get('UNDRAWN'))}  "
                    f"{fmt_comma15_2(row.get('CURBAL'))}"
                )
                line_count_ref[0] += 1

                p_noofac  += int(row.get("NOOFAC") or 1)
                p_appr    += float(row.get("APPRLIMT") or 0)
                p_undrawn += float(row.get("UNDRAWN")  or 0)
                p_curbal  += float(row.get("CURBAL")   or 0)

            # BREAK AFTER BRANCH: LINE @020 112*'-'
            lines.append(f"                    {DASH_112}")
            line_count_ref[0] += 1

        # BREAK AFTER PRODUCT:
        # LINE @001 'SUB TOTAL ' @030 'NO OF A/C : ' NOOFAC.SUM COMMA10
        #      @082 APPRLIMT.SUM COMMA15.2  @099 UNDRAWN.SUM COMMA15.2
        #      @116 CURBAL.SUM COMMA15.2
        # LINE @001 132*'-'
        # SAS @030 = position 30; field widths to @082: gap of 52 chars from @030
        sub_line = (
            f" SUB TOTAL "                             # @001  (11 chars)
            f"{'NO OF A/C :':>18} {fmt_comma10(p_noofac)}"  # @030 area (29 chars total so far=40)
            f"{'':>41}"                                 # pad to @082 (41 chars)
            f"{fmt_comma15_2(p_appr)}  "               # @082 APPRLIMT (17 chars)
            f"{fmt_comma15_2(p_undrawn)}  "            # @099 UNDRAWN (17 chars)
            f"{fmt_comma15_2(p_curbal)}"               # @116 CURBAL
        )
        lines.append(sub_line)
        lines.append(f" {DASH_132}")
        line_count_ref[0] += 2

        gt_noofac  += p_noofac
        gt_appr    += p_appr
        gt_undrawn += p_undrawn
        gt_curbal  += p_curbal

    # RBREAK AFTER /PAGE: new page for grand total
    # LINE @001 132*'-'
    # LINE @001 'GRAND TOTAL ' @030 'NO OF A/C : ' NOOFAC.SUM COMMA10
    #      @082 APPRLIMT.SUM COMMA15.2  @099 UNDRAWN.SUM COMMA15.2
    #      @116 CURBAL.SUM COMMA15.2
    # LINE @001 132*'-'
    lines.append(f"1")  # ASA '1' = form-feed
    lines.append(f" {DASH_132}")
    grand_line = (
        f" GRAND TOTAL "                               # @001  (13 chars)
        f"{'NO OF A/C :':>16} {fmt_comma10(gt_noofac)}"  # @030 area
        f"{'':>41}"                                    # pad to @082
        f"{fmt_comma15_2(gt_appr)}  "                 # @082 APPRLIMT
        f"{fmt_comma15_2(gt_undrawn)}  "              # @099 UNDRAWN
        f"{fmt_comma15_2(gt_curbal)}"                 # @116 CURBAL
    )
    lines.append(grand_line)
    lines.append(f" {DASH_132}")
    lines.append(f" ")
    lines.append(f" {footnote}")
    lines.append(f" ")


# ============================================================================
# STEP 7: PRODUCE REPORTS
# ============================================================================

report_lines: list[str] = []

# --- PROC TABULATE ---
render_tabulate(
    df=loana_by_product,
    title1="PUBLIC BANK BERHAD/PUBLIC ISLAMIC BANK BERHAD",
    title2=(
        f"STAFF LOAN/STAFF FINANCING POSITION REPORT FOR THE MONTH OF"
        f" {WMONTH} {RYEAR}"
    ),
    footnote="REPORT ID : EIBMSFLN",
    lines=report_lines,
    rts=40,
)

# --- PROC REPORT ---
render_report(
    df=loanb,
    title1="PUBLIC BANK BERHAD/PUBLIC ISLAMIC BANK BERHAD",
    title2=f"STAFF LOAN/STAFF FINANCING AS AT {RDATE}",
    footnote="REPORT ID : EIBMSFLN",
    lines=report_lines,
)

# ============================================================================
# WRITE REPORT
# ============================================================================

with open(OUTPUT_REPORT, "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines) + "\n")

print(f"Report written to: {OUTPUT_REPORT}")
