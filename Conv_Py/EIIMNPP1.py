#!/usr/bin/env python3
"""
Program  : EIIMNPP1.py
Purpose  : Movements of Interest in Suspense (IIS) and Specific Provision (SP)
            reports for Public Islamic Bank NPL accounts (HP only, 3 months & above).
           Equivalent to SAS program EIIMNPP1 / EIFMNPP1.
           Produces three separate output files:
             - RPSIIS  : IIS movement report       (NPL6.IIS)
             - RPSSP1  : SP movement report (SP1)  (NPL6.SP1)
             - RPSSP2  : SP movement report (SP2)  (NPL6.SP2)

NOTE: The HL&FL and Islamic Loans sections (NPOL6 / NPIL6) were
        commented out in the original SAS with /* STOP ... STOP */
        and are preserved below as commented placeholders.
"""

import duckdb
import polars as pl
import os
from datetime import date

# ─────────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────────
BASE_DIR   = r"C:/data"
NPL6_DIR   = os.path.join(BASE_DIR, "npl6")
# NPOL6_DIR  = os.path.join(BASE_DIR, "npol6")   # HL & FL – stopped FR SEP 04
# NPIL6_DIR  = os.path.join(BASE_DIR, "npil6")   # Islamic Loans – stopped FR SEP 04
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

RPSIIS_PATH = os.path.join(OUTPUT_DIR, "EIIMNPP1_IIS.txt")
RPSSP1_PATH = os.path.join(OUTPUT_DIR, "EIIMNPP1_SP1.txt")
RPSSP2_PATH = os.path.join(OUTPUT_DIR, "EIIMNPP1_SP2.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

con = duckdb.connect()

# ─────────────────────────────────────────────
# REPTDATE – derive RDATE macro
# ─────────────────────────────────────────────
reptdate_path = os.path.join(NPL6_DIR, "REPTDATE.parquet")
reptdate_df   = con.execute(f"SELECT REPTDATE FROM '{reptdate_path}'").pl()
reptdate_val  = reptdate_df["REPTDATE"][0]
if isinstance(reptdate_val, int):
    reptdate_val = date.fromordinal(reptdate_val)

RDATE = reptdate_val.strftime("%d/%m/%y")   # DDMMYY8.

# ─────────────────────────────────────────────
# Report constants
# ─────────────────────────────────────────────
PAGE_LINES = 60      # default page length
LINE_WIDTH  = 220    # wide enough for all columns

ASA_PAGE = "1"
ASA_NORM = " "

# ─────────────────────────────────────────────
# Formatting helpers
# ─────────────────────────────────────────────
def fmt_comma(val, width: int = 14, decimals: int = 2) -> str:
    """COMMA14.2 equivalent – right-justified with comma thousands separator."""
    if val is None:
        return " " * width
    try:
        s = f"{float(val):,.{decimals}f}"
        return f"{s:>{width}}"[:width]
    except (TypeError, ValueError):
        return " " * width

def fmt_plain(val, width: int) -> str:
    """Right-justified integer/string, no commas."""
    if val is None:
        return " " * width
    return f"{str(val):>{width}}"[:width]

def fmt_left(val, width: int) -> str:
    """Left-justified string, padded/truncated."""
    if val is None:
        return " " * width
    return f"{str(val):<{width}}"[:width]

def make_buf(width: int = LINE_WIDTH) -> list:
    return [" "] * width

def place(text: str, col: int, buf: list):
    for i, ch in enumerate(str(text)):
        pos = col - 1 + i
        if 0 <= pos < len(buf):
            buf[pos] = ch

def render(buf: list) -> str:
    return "".join(buf)

# ─────────────────────────────────────────────────────────────────────────────
# Generic PROC PRINT renderer
#
# Mirrors SAS PROC PRINT with:
#   BY BRANCH LOANTYP RISK
#   PAGEBY BRANCH
#   SUMBY RISK
#   SUM <numeric_vars>
#   N (observation counter per BY group)
#   LABEL (column header = label text)
# ─────────────────────────────────────────────────────────────────────────────
def render_proc_print(
    data_df:        pl.DataFrame,
    var_order:      list[str],          # VAR statement column order
    labels:         dict[str, str],     # LABEL statement
    numeric_vars:   list[str],          # SUM statement vars (COMMA14.2)
    title1:         str,
    title2:         str,
    title3:         str,
    title4:         str = "",
    rdate:          str = "",
) -> list[str]:
    """
    Produce an ASA-formatted fixed-width report equivalent to the SAS PROC PRINT.
    Returns a list of ASA lines (first char = carriage control).
    """
    lines_out: list[str] = []

    def wr(text: str = "", asa: str = ASA_NORM):
        lines_out.append(asa + text)

    # ── Column layout ─────────────────────────────────────────────────────────
    # Each column: label (wrapped to 4 lines), fixed width derived from label
    # and data.  We use a generous minimum width per column type.
    CHAR_COLS = {c for c in var_order if c not in set(numeric_vars + ["ACCTNO", "DAYS"])}
    NUM_W     = 16    # width for numeric COMMA14.2 columns (14 data + 2 gap)
    MIN_W     = 10

    col_widths: dict[str, int] = {}
    for col in var_order:
        lbl   = labels.get(col, col)
        # Split label into words to estimate display width needed
        w = max(MIN_W, max((len(ln) for ln in lbl.split()), default=MIN_W))
        if col in numeric_vars:
            w = max(w, 16)
        col_widths[col] = w + 1   # +1 gap between columns

    # ── Page header helper ────────────────────────────────────────────────────
    def emit_page_header(branch, loantyp, risk, pagecnt) -> int:
        wr(title1, ASA_PAGE)
        wr(title2)
        wr(title3)
        if title4:
            wr(title4)
        wr(f"  BRANCH: {branch}    LOAN TYPE: {loantyp}    RISK: {risk}"
           f"    PAGE: {pagecnt}")
        wr(" ")

        # Column headers (single line – labels can be long; use shortened labels)
        hdr = ""
        for col in var_order:
            lbl = labels.get(col, col).split("(")[0].strip()[:col_widths[col] - 1]
            hdr += f"{lbl:<{col_widths[col]}}"
        wr(hdr)
        wr("-" * min(len(hdr), LINE_WIDTH))
        return 6 + (1 if title4 else 0)    # LINECNT after header

    # ── Accumulate subtotals ──────────────────────────────────────────────────
    def zero_sums() -> dict[str, float]:
        return {c: 0.0 for c in numeric_vars}

    def add_sums(acc: dict, row: dict):
        for c in numeric_vars:
            acc[c] += float(row.get(c) or 0.0)

    def emit_sum_row(prefix: str, acc: dict, obs_n: int | None = None):
        row_str = f"{fmt_left(prefix, 15)}"
        if obs_n is not None:
            row_str += f"  N={obs_n:<6}"
        else:
            row_str += " " * 9
        for col in var_order:
            if col in numeric_vars:
                row_str += fmt_comma(acc[col], col_widths[col])
            else:
                row_str += " " * col_widths[col]
        wr(row_str)
        wr("-" * min(len(row_str), LINE_WIDTH))

    # ── Main loop ─────────────────────────────────────────────────────────────
    LINECNT   = PAGE_LINES   # force header on first record
    PAGECNT   = 0
    obs_total = 0            # N counter (PROC PRINT N option)

    prev_branch  = None
    prev_loantyp = None
    prev_risk    = None

    # Running sums
    risk_sums   = zero_sums()
    risk_n      = 0

    rows   = data_df.to_dicts()
    n_rows = len(rows)

    for idx, row in enumerate(rows):
        branch  = row.get("BRANCH")
        loantyp = row.get("LOANTYP") or ""
        risk    = row.get("RISK")    or ""

        first_branch  = (branch  != prev_branch)
        first_loantyp = (loantyp != prev_loantyp) or first_branch
        first_risk    = (risk    != prev_risk)     or first_loantyp

        is_last   = (idx == n_rows - 1)
        next_row  = rows[idx + 1] if not is_last else None
        last_risk    = is_last or (next_row["RISK"]    != risk    or
                                   next_row["LOANTYP"] != loantyp or
                                   next_row["BRANCH"]  != branch)
        last_loantyp = is_last or (next_row["LOANTYP"] != loantyp or
                                   next_row["BRANCH"]  != branch)
        last_branch  = is_last or (next_row["BRANCH"]  != branch)

        # PAGEBY BRANCH – new page when branch changes
        if first_branch or LINECNT >= PAGE_LINES:
            PAGECNT += 1
            LINECNT  = emit_page_header(branch, loantyp, risk, PAGECNT)

        if first_risk:
            risk_sums = zero_sums()
            risk_n    = 0

        # ── Detail row ────────────────────────────────────────────────────────
        det = " "
        for col in var_order:
            val = row.get(col)
            if col in numeric_vars:
                det += fmt_comma(val, col_widths[col])
            else:
                det += fmt_left(str(val or ""), col_widths[col])
        wr(det)
        LINECNT  += 1
        obs_total += 1
        risk_n   += 1
        add_sums(risk_sums, row)

        if LINECNT >= PAGE_LINES and not last_risk:
            PAGECNT += 1
            LINECNT  = emit_page_header(branch, loantyp, risk, PAGECNT)

        # SUMBY RISK – subtotal when risk group ends
        if last_risk:
            emit_sum_row(f"** RISK={risk} TOTAL **", risk_sums, risk_n)
            LINECNT += 2

        if last_loantyp:
            pass    # SAS PROC PRINT does not emit a LOANTYP subtotal; BY group change handled above

        if last_branch:
            wr(f"  Total observations for BRANCH {branch}: N={risk_n}")
            wr(" ")
            LINECNT += 2

    # Grand total N line (PROC PRINT N option prints grand N at bottom)
    wr(" ")
    wr(f"  Total observations: N={obs_total}")

    return lines_out


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1 – GETTING IIS ACCOUNTS FROM HP
# PROC PRINTTO PRINT=RPSIIS NEW
# ─────────────────────────────────────────────────────────────────────────────

# PROC SORT DATA=NPL6.IIS OUT=NPLIIS; BY BRANCH LOANTYP RISK DAYS ACCTNO;
iis_path = os.path.join(NPL6_DIR, "IIS.parquet")
npliis_df = con.execute(f"""
    SELECT * FROM '{iis_path}'
    ORDER BY BRANCH, LOANTYP, RISK, DAYS, ACCTNO
""").pl()

IIS_LABELS = {
    "ACCTNO":  "MNI ACCOUNT NO",
    "NAME":    "NAME",
    "DAYS":    "NO OF DAYS PAST DUE",
    "BORSTAT": "BORROWER'S STATUS",
    "NETPROC": "LIMIT",
    "CURBAL":  "CURRENT BAL (A)",
    "UHC":     "UNEARNED HIRING CHARGES (B)",
    "NETBAL":  "NET BAL (A-B=C)",
    "IISP":    "OPENING BAL FOR FINANCIAL YEAR (D)",
    "SUSPEND": "INTEREST SUSPENDED DURING THE PERIOD (E)",
    "RECOVER": "WRITTEN BACK TO PROFIT & LOSS (F)",
    "RECC":    "REVERSAL OF CURRENT YEAR IIS (G)",
    "IISPW":   "WRITTEN OFF (H)",
    "IIS":     "IIS CLOSING BAL (D+E-F-G-H=I)",
    "OIP":     "OPENING BAL FOR FINANCIAL YEAR (J)",
    "OISUSP":  "OI SUSPENDED DURING THE PERIOD (K)",
    "OIRECV":  "WRITTEN BACK TO PROFIT & LOSS (L)",
    "OIRECC":  "REVERSAL OF CURRENT YEAR OI (M)",
    "OIW":     "WRITTEN OFF (N)",
    "OI":      "OI CLOSING BAL (J+K-L-M-N=O)",
    "TOTIIS":  "TOTAL CLOSING BAL AS AT RPT DATE (I+O)",
}

IIS_VAR_ORDER = [
    "ACCTNO", "NAME", "DAYS", "BORSTAT", "NETPROC", "CURBAL", "UHC",
    "NETBAL", "IISP", "SUSPEND", "RECOVER", "RECC", "IISPW", "IIS",
    "OIP", "OISUSP", "OIRECV", "OIRECC", "OIW", "OI", "TOTIIS",
]

IIS_NUMERIC = [
    "NETPROC", "CURBAL", "UHC", "NETBAL", "IISP", "SUSPEND", "RECOVER",
    "RECC", "IISPW", "IIS", "OIP", "OISUSP", "OIRECV", "OIRECC",
    "OIW", "OI", "TOTIIS",
]

rpsiis_lines = render_proc_print(
    data_df      = npliis_df,
    var_order    = IIS_VAR_ORDER,
    labels       = IIS_LABELS,
    numeric_vars = IIS_NUMERIC,
    title1       = "PUBLIC ISLAMIC BANK - (NPL FROM 3 MONTHS & ABOVE)",
    title2       = "MOVEMENTS OF INTEREST IN SUSPENSE FOR THE MONTH ENDING "
                   f"AS AT : {RDATE}",
    title3       = "REPORT ID : EIFMNPP1",
    rdate        = RDATE,
)

with open(RPSIIS_PATH, "w", encoding="utf-8") as f:
    for line in rpsiis_lines:
        f.write(line + "\n")

print(f"IIS report written to : {RPSIIS_PATH}")

# /* STOP FR SEP 04
# *--------------------------------------------------------------------*
# *  GETTING IIS ACCOUNTS FROM HL & FL                                 *
# *--------------------------------------------------------------------*
# PROC SORT DATA=NPOL6.IIS OUT=NPOLIIS; BY BRANCH LOANTYP RISK DAYS ACCTNO;
# PROC PRINT DATA=NPOLIIS LABEL N; ...
#   LABEL CURRBAL = 'CURRENT BAL (A)'
#         INTEREST= 'INTEREST (B)'
#         NETBAL  = 'NET BAL (A+B=C)' ...
#   TITLE2 'MOVEMENTS OF INTEREST IN SUSPENSE FOR THE MONTH ENDING AS AT : ' &RDATE;
# ----
# *  GETTING IIS ACCOUNTS FROM ISLAMIC LOANS                           *
# PROC SORT DATA=NPIL6.IIS OUT=NPILIIS; BY BRANCH LOANTYP RISK DAYS ACCTNO;
# PROC PRINT DATA=NPILIIS LABEL N; ...
#   LABEL INTEREST='UNEARNED HIRING CHGES(UHC)/INTEREST(B)'
#         IIS = 'IIS CLOSING BAL (ACTUARIAL)' ...
#   TITLE2 'MOVEMENTS OF INTEREST IN SUSPENSE FOR ISLAMIC LOANS FOR THE MONTH ENDING AS AT : ' &RDATE;
# STOP */

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2 – GETTING SP1 ACCOUNTS FROM HP
# PROC PRINTTO PRINT=RPSSP1 NEW
# ─────────────────────────────────────────────────────────────────────────────

# PROC SORT DATA=NPL6.SP1 OUT=NPLSP1; BY BRANCH LOANTYP RISK DAYS ACCTNO;
sp1_path  = os.path.join(NPL6_DIR, "SP1.parquet")
nplsp1_df = con.execute(f"""
    SELECT * FROM '{sp1_path}'
    ORDER BY BRANCH, LOANTYP, RISK, DAYS, ACCTNO
""").pl()

SP1_LABELS = {
    "ACCTNO":   "MNI ACCOUNT NO",
    "NAME":     "NAME",
    "DAYS":     "NO OF DAYS PAST DUE",
    "BORSTAT":  "BORROWER'S STATUS",
    "NETPROC":  "LIMIT",
    "CURBAL":   "CURRENT BAL (A)",
    "UHC":      "UNEARNED HIRING CHARGES (B)",
    "NETBAL":   "NET BAL (A-B=C)",
    "IIS":      "IIS (E)",
    "OSPRIN":   "PRINCIPAL OUTSTANDING (C-D-E=F)",
    "MARKETVL": "REALISABLE VALUE (G)",
    "NETEXP":   "NET EXPOSURE (F-G=H)",
    "SPP1":     "OPENING BAL FOR FINANCIAL YEAR (I)",
    "SPPL":     "PROVISION MADE AGAINST PROFIT & LOSS (J)",
    "RECOVER":  "WRITTEN BACK TO PROFIT & LOSS (K)",
    "SPPW":     "WRITTEN OFF AGAINST PROVISION (L)",
    "SP":       "CLOSING BAL AS AT RPT DATE (I+J-K-L)",
}

SP1_VAR_ORDER = [
    "ACCTNO", "NAME", "DAYS", "BORSTAT", "NETPROC", "CURBAL", "UHC",
    "NETBAL", "IIS", "OSPRIN", "MARKETVL", "NETEXP", "SPP1",
    "SPPL", "RECOVER", "SPPW", "SP",
]

SP1_NUMERIC = [
    "NETPROC", "CURBAL", "UHC", "NETBAL", "IIS", "OSPRIN", "MARKETVL",
    "NETEXP", "SPP1", "SPPL", "RECOVER", "SPPW", "SP",
]

rpssp1_lines = render_proc_print(
    data_df      = nplsp1_df,
    var_order    = SP1_VAR_ORDER,
    labels       = SP1_LABELS,
    numeric_vars = SP1_NUMERIC,
    title1       = "PUBLIC ISLAMIC BANK - (NPL FROM 3 MONTHS & ABOVE)",
    title2       = "MOVEMENTS OF SPECIFIC PROVISION FOR THE MONTH ENDING",
    title3       = f"(BASED ON PURCHASE PRICE LESS DEPRECIATION) AS AT : {RDATE}",
    title4       = "REPORT ID : EIFMNPP1",
    rdate        = RDATE,
)

with open(RPSSP1_PATH, "w", encoding="utf-8") as f:
    for line in rpssp1_lines:
        f.write(line + "\n")

print(f"SP1 report written to : {RPSSP1_PATH}")

# /* STOP FR SEP 04
# *--------------------------------------------------------------------*
# *  GETTING SP1 ACCOUNTS FROM HL & FL                                 *
# *--------------------------------------------------------------------*
# PROC SORT DATA=NPOL6.SP1 OUT=NPOLSP1; BY BRANCH LOANTYP RISK DAYS ACCTNO;
# PROC PRINT DATA=NPOLSP1 LABEL N; ...
#   LABEL CURRBAL='CURRENT BAL (A)'  INTEREST='INTEREST (B)'
#         NETBAL='NET BAL (A+B=C)'   OSPRIN='PRINCIPAL OUTSTANDING (D=A)'
#         REALISVL='REALISABLE VALUE (G)' ...
#   VAR ACCTNO NAME DAYS BORSTAT NETPROC CURRBAL INTEREST NETBAL
#       OSPRIN REALISVL NETEXP SPP1 SPPL RECOVER SPPW SP LIABCODE;
#   TITLE3 '(BASED ON PURCHASE PRICE LESS DEPRECIATION)'
#   TITLE4 'AS AT : ' &RDATE;
# ----
# *  GETTING SP1 ACCOUNTS FROM ISLAMIC LOANS                           *
# PROC SORT DATA=NPIL6.SP1 OUT=NPILSP1; BY BRANCH LOANTYP RISK DAYS ACCTNO;
# PROC PRINT DATA=NPILSP1 LABEL N; ...
#   LABEL INTEREST='UNEARNED HIRING CHGES(UHC)/INTEREST(B)'
#         INTINSUS='INTEREST IN SUSPENSE (I+O=D)'
#         OSPRIN='PRINCIPAL OUTSTANDING (D=A)' ...
#   VAR ACCTNO NAME DAYS BORSTAT NETPROC CURRBAL INTEREST NETBAL
#       INTINSUS OSPRIN REALISVL NETEXP SPP1 SPPL RECOVER SPPW SP LIABCODE;
#   TITLE2 'MOVEMENTS OF SPECIFIC PROVISION FOR THE ISLAMIC LOANS';
#   TITLE3 '(BASED ON PURCHASE PRICE LESS DEPRECIATION)';
#   TITLE4 'FOR MONTH ENDING AS AT : ' &RDATE;
# STOP */

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3 – GETTING SP2 ACCOUNTS FROM HP
# PROC PRINTTO PRINT=RPSSP2 NEW
# ─────────────────────────────────────────────────────────────────────────────

# PROC SORT DATA=NPL6.SP2 OUT=NPLSP2; BY BRANCH LOANTYP RISK DAYS ACCTNO;
sp2_path  = os.path.join(NPL6_DIR, "SP2.parquet")
nplsp2_df = con.execute(f"""
    SELECT * FROM '{sp2_path}'
    ORDER BY BRANCH, LOANTYP, RISK, DAYS, ACCTNO
""").pl()

SP2_LABELS = {
    "ACCTNO":   "MNI ACCOUNT NO",
    "NAME":     "NAME",
    "VINNO":    "AA NUMBER",
    "DAYS":     "NO OF DAYS PAST DUE",
    "BORSTAT":  "BORROWER'S STATUS",
    "NETPROC":  "LIMIT",
    "CURBAL":   "CURRENT BAL (A)",
    "UHC":      "UNEARNED HIRING CHARGES (B)",
    "NETBAL":   "NET BAL (A-B=C)",
    "IIS":      "IIS (E)",
    "OSPRIN":   "PRINCIPAL OUTSTANDING (C-D-E=F)",
    "OTHERFEE": "OTHER FEES",
    "MARKETVL": "REALISABLE VALUE (G)",
    "NETEXP":   "NET EXPOSURE (F-G=H)",
    "SPP2":     "OPENING BAL FOR FINANCIAL YEAR (I)",
    "SPPL":     "PROVISION MADE AGAINST PROFIT & LOSS (J)",
    "RECOVER":  "WRITTEN BACK TO PROFIT & LOSS (K)",
    "SPPW":     "WRITTEN OFF AGAINST PROVISION (L)",
    "SP":       "CLOSING BAL AS AT RPT DATE (I+J-K-L)",
}

SP2_VAR_ORDER = [
    "ACCTNO", "NAME", "VINNO", "DAYS", "BORSTAT", "NETPROC", "CURBAL",
    "UHC", "NETBAL", "IIS", "OSPRIN", "OTHERFEE", "MARKETVL", "NETEXP",
    "SPP2", "SPPL", "RECOVER", "SPPW", "SP",
]

SP2_NUMERIC = [
    "NETPROC", "CURBAL", "UHC", "NETBAL", "IIS", "OSPRIN", "OTHERFEE",
    "MARKETVL", "NETEXP", "SPP2", "SPPL", "RECOVER", "SPPW", "SP",
]

rpssp2_lines = render_proc_print(
    data_df      = nplsp2_df,
    var_order    = SP2_VAR_ORDER,
    labels       = SP2_LABELS,
    numeric_vars = SP2_NUMERIC,
    title1       = "PUBLIC ISLAMIC BANK - (NPL FROM 3 MONTHS & ABOVE)",
    title2       = "MOVEMENTS OF SPECIFIC PROVISION FOR THE MONTH ENDING",
    title3       = f"(BASED ON DEPRECIATED PP FOR UNSCHEDULED GOODS) AS AT : {RDATE}",
    title4       = "REPORT ID : EIFMNPP1",
    rdate        = RDATE,
)

with open(RPSSP2_PATH, "w", encoding="utf-8") as f:
    for line in rpssp2_lines:
        f.write(line + "\n")

print(f"SP2 report written to : {RPSSP2_PATH}")
