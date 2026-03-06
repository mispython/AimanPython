#!/usr/bin/env python3
"""
Program  : EIBMLN03.py
Invoke by: EIBMRPTS
Date     : 29-1-2001  (SMR/OTHERS: A260)
Changes  : Include Weighted Average Lending Rate on HPD
Objective: WEIGHTED AVERAGE LENDING RATE (RDIR II)
           - Prescribed Rates, Non-Prescribed Rates, All
             (Exclude Staff Loans, Exclude Penalty Rates and
              Loans Under Litigation (RISKRATING < 1))
           - Prescribed Rates (Exclude Staff Loans Only)
  Output : Flat file to SRS (SAP.PBB.M4LOAN => HOELOAN)
Date     : 23-7-2001  (SMR/OTHERS: A262)
Changes  : Include Weighted Average Lending Rate on HPD
           (Acct Type 9) into flat file to SRS.
"""

import duckdb
import polars as pl
from pathlib import Path
from datetime import date

from PBBLNFMT import (
    HOME_ISLAMIC,
    HOME_CONVENTIONAL,
    SWIFT_ISLAMIC,
    SWIFT_CONVENTIONAL,
)

# ============================================================================
# CONFIGURATION / PATH SETUP
# ============================================================================

BASE_DIR   = Path(".")
BNM_DIR    = BASE_DIR / "data" / "bnm"
ODGP3_DIR  = BASE_DIR / "data" / "odgp3"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Report output file (printed report)
REPORT_FILE = OUTPUT_DIR / "EIBMLN03.txt"

# Flat file output for SRS (SAP.PBB.M4LOAN => HOELOAN)
M4LOAN_FILE = OUTPUT_DIR / "M4LOAN.txt"

# Page length (ASA carriage control, 60 lines per page)
PAGE_LENGTH = 60

# OPTIONS YEARCUTOFF=1950 NOCENTER NODATE NONUMBER MISSING=0

# ============================================================================
# PROC FORMAT equivalent: $LNFMT
# ============================================================================

LNFMT_MAP = {
    "P1": "PRESCRIBED RATE (HOUSING LOANS)",
    "P2": "PRESCRIBED RATE (BNM FUNDED LOANS)",
    "P3": "NON-PRESCRIBED RATE (HOUSING LOANS)",
    "P4": "NON-PRESCRIBED RATE (OTHER LOANS)",
    "P5": "FLOOR STOCKING",
}


def fmt_lnfmt(code: str) -> str:
    return LNFMT_MAP.get(code, code)


# ============================================================================
# LN03FMT: product -> loantyp mapping
# Referenced in SAS as PUT(PRODUCT, LN03FMT.) for ACCTYPE='LN' rows.
# HOME_ISLAMIC, HOME_CONVENTIONAL, SWIFT_ISLAMIC, SWIFT_CONVENTIONAL are
# imported from PBBLNFMT and used here to classify prescribed housing loans.
# ============================================================================

def format_ln03fmt(product: int) -> str:
    """
    Map product code to loan type category for WALR calculation.
    P1 = Prescribed Rate (Housing Loans)
    P2 = Prescribed Rate (BNM Funded Loans)
    P3 = Non-Prescribed Rate (Housing Loans)
    P4 = Non-Prescribed Rate (Other Loans)
    P5 = Floor Stocking
    SL = Staff Loans (excluded)

    HOME_ISLAMIC, HOME_CONVENTIONAL from PBBLNFMT identify prescribed housing products.
    SWIFT_ISLAMIC, SWIFT_CONVENTIONAL from PBBLNFMT identify prescribed BNM-funded products.
    """
    # Staff loans (product codes 1-99)
    if 1 <= product <= 99:
        return "SL"

    # Prescribed housing loans — Islamic and conventional product lists from PBBLNFMT
    if product in HOME_ISLAMIC or product in HOME_CONVENTIONAL:
        return "P1"

    # Non-prescribed housing loans
    if product in {110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
                   139, 140, 141, 142, 147, 173, 400, 409, 410, 412,
                   413, 414, 415, 423, 431, 432, 433, 440, 466, 472,
                   473, 474, 479, 484, 486, 489, 494, 600, 638, 650,
                   651, 664, 677}:
        return "P3"

    # BNM funded loans (prescribed) — SWIFT Islamic and conventional from PBBLNFMT
    if product in SWIFT_ISLAMIC or product in SWIFT_CONVENTIONAL:
        return "P2"

    # Other BNM funded products
    if product in {192, 193, 194, 195, 196}:
        return "P2"

    # Floor stocking
    if product in {392, 612}:
        return "P5"

    # Default: non-prescribed other loans
    return "P4"


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
        self.titles      = []

    def set_titles(self, *titles):
        self.titles = [t for t in titles if t]

    def _emit_header(self):
        for i, t in enumerate(self.titles):
            cc = "1" if i == 0 else " "
            self.lines.append(cc + t)
        if self.titles:
            self.lines.append(" ")
        self.line_count = len(self.titles) + (1 if self.titles else 0)

    def start_section(self):
        """Emit page-break header for a new report section."""
        self._emit_header()

    def write_line(self, text: str, cc: str = " "):
        if self.line_count >= self.page_length:
            self._emit_header()
        self.lines.append(cc + text)
        self.line_count += 1

    def write_blank(self):
        self.write_line("")

    def flush(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            f.write("\n".join(self.lines) + "\n")


# ============================================================================
# FORMAT HELPERS
# ============================================================================

def fmt_comma18_2(val) -> str:
    try:
        return f"{float(val):>18,.2f}"
    except Exception:
        return " " * 18


# ============================================================================
# STEP 1: READ REPTDATE
# ============================================================================

def get_report_date() -> dict:
    reptdate_path = BNM_DIR / "REPTDATE.parquet"
    con = duckdb.connect()
    row = con.execute(f"SELECT REPTDATE FROM read_parquet('{reptdate_path}') LIMIT 1").fetchone()
    con.close()
    reptdate: date = row[0]

    day   = reptdate.day
    month = reptdate.month
    year  = reptdate.year

    if   day == 8:  nowk = "1"
    elif day == 15: nowk = "2"
    elif day == 22: nowk = "3"
    else:           nowk = "4"

    return {
        "nowk":     nowk,
        "reptyear": str(year),
        "reptmon":  str(month).zfill(2),
        "reptday":  str(day).zfill(2),
        "rdate":    reptdate.strftime("%d/%m/%Y"),
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
# STEP 3: LOAD AND PREPARE LOAN DATA
# Exclude Staff Loans, Determine Prescribed/Non-Prescribed Rates
# ============================================================================

def load_gp3() -> pl.DataFrame:
    """Load GP3 risk data; LOANSTAT=1 flags accounts under litigation (RISKCODE < 1)."""
    gp3_path = ODGP3_DIR / "GP3.parquet"
    con = duckdb.connect()
    gp3 = con.execute(f"""
        SELECT ACCTNO,
               TRY_CAST(RISKCODE AS INTEGER) AS RISKRTE,
               CASE WHEN TRY_CAST(RISKCODE AS INTEGER) < 1 THEN 1 ELSE 0 END AS LOANSTAT
        FROM read_parquet('{gp3_path}')
    """).pl()
    con.close()
    return gp3


def load_loan(reptmon: str, nowk: str) -> pl.DataFrame:
    """
    Load loan data.
    Exclude: Cagamas products (PRODUCT IN (124,145) with PRODCD='54120')
             and FCY products (PRODUCT 800-899).
    """
    loan_path = BNM_DIR / f"LOAN{reptmon}{nowk}.parquet"
    con = duckdb.connect()
    loan = con.execute(f"""
        SELECT *
        FROM read_parquet('{loan_path}')
        WHERE NOT (PRODUCT IN (124, 145) AND PRODCD = '54120')
          AND PRODUCT NOT BETWEEN 800 AND 899
    """).pl()
    con.close()
    return loan


def build_loan_merged(loan_df: pl.DataFrame, gp3_df: pl.DataFrame) -> pl.DataFrame:
    """
    Merge LOAN with GP3, determine LOANTYP per row.
    - Excludes PRODCD='34111' (Hire Purchase)
    - Keeps only PRODCD starting with '34'
    - Excludes staff loans (LOANTYP='SL')
    - format_ln03fmt() uses HOME_ISLAMIC, HOME_CONVENTIONAL, SWIFT_ISLAMIC,
      SWIFT_CONVENTIONAL from PBBLNFMT to classify prescribed housing/BNM products.
    """
    merged = loan_df.join(gp3_df.select(["ACCTNO","LOANSTAT","RISKRTE"]),
                          on="ACCTNO", how="left")
    merged = merged.with_columns([
        pl.col("LOANSTAT").fill_null(0),
        pl.col("RISKRTE").fill_null(1),
    ])

    # Exclude Hire Purchase (34111)
    merged = merged.filter(pl.col("PRODCD") != "34111")

    # Keep only PRODCD starting with '34'
    merged = merged.filter(pl.col("PRODCD").str.starts_with("34"))

    loantyp_list  = []
    loanstat_list = []
    brhno_list    = []

    for row in merged.iter_rows(named=True):
        acctype = str(row.get("ACCTYPE", "") or "").strip()
        product = int(row.get("PRODUCT", 0) or 0)
        intrate = float(row.get("INTRATE", 0) or 0)
        spread  = float(row.get("SPREAD",  0) or 0)
        riskrte = int(row.get("RISKRTE",   1) or 1)
        loanstat = int(row.get("LOANSTAT", 0) or 0)

        if acctype == "OD":
            loantyp = "P4"
            if product == 119:
                loantyp = "P1"
            elif product in {120, 137, 138, 154, 155, 192, 193, 194, 195}:
                loantyp = "P3"
        elif acctype == "LN":
            if product == 911:
                loantyp = "P3"
            elif product in {225, 226}:
                loantyp = "P1" if (intrate <= 9 or spread <= 1.75) else "P3"
            else:
                # format_ln03fmt uses HOME_ISLAMIC, HOME_CONVENTIONAL,
                # SWIFT_ISLAMIC, SWIFT_CONVENTIONAL from PBBLNFMT
                loantyp = format_ln03fmt(product)
        else:
            loantyp = "P4"

        # Override LOANSTAT if RISKRTE < 1 (loans under litigation)
        if riskrte < 1:
            loanstat = 1

        loantyp_list.append(loantyp)
        loanstat_list.append(loanstat)
        brhno_list.append(row.get("BRANCH", ""))

    merged = merged.with_columns([
        pl.Series("LOANTYP",  loantyp_list),
        pl.Series("LOANSTAT", loanstat_list),
        pl.Series("BRHNO",    brhno_list),
    ])

    # Exclude staff loans
    merged = merged.filter(pl.col("LOANTYP") != "SL")

    return merged.select(["LOANTYP","PRODCD","LOANSTAT","INTRATE","BALANCE","BRHNO"])


# ============================================================================
# SECTION: WEIGHTED AVERAGE LENDING RATE
# Exclude Penalty Rates and Loans Under Litigation (LOANSTAT=1)
# ============================================================================

def section_walr(loan_df: pl.DataFrame, sdesc: str, rdate: str, writer: ReportWriter):
    """
    Weighted Average Lending Rate (exclude penalty/litigation: LOANSTAT=1).
    CLASS LOANTYP INTRATE; VAR BALANCE; PRODUCT (weighted amount) = INTRATE * BALANCE.
    """
    filtered = loan_df.filter(pl.col("LOANSTAT") == 1)
    alm = (filtered
           .group_by(["LOANTYP","INTRATE"])
           .agg(pl.col("BALANCE").sum().alias("BALANCE"))
           .sort(["LOANTYP","INTRATE"]))
    alm = alm.with_columns(
        (pl.col("INTRATE") * pl.col("BALANCE")).alias("PRODUCT")
    )

    writer.set_titles(
        "REPORT ID : EIBMLN03",
        sdesc,
        f"WEIGHTED AVERAGE LENDING RATE AS AT {rdate}",
    )
    writer.start_section()

    hdr = f"{'LOANTYP':<40} {'INTRATE':>10} {'BALANCE':>18} {'PRODUCT':>18}"
    writer.write_line(hdr)
    writer.write_line("-" * len(hdr))

    current_lt = None
    lt_bal     = 0.0
    lt_prd     = 0.0
    grand_bal  = 0.0
    grand_prd  = 0.0

    for row in alm.iter_rows(named=True):
        lt      = str(row["LOANTYP"])
        intrate = float(row["INTRATE"] or 0)
        bal     = float(row["BALANCE"] or 0)
        prd     = float(row["PRODUCT"] or 0)

        if current_lt is not None and lt != current_lt:
            writer.write_line("-" * len(hdr))
            writer.write_line(
                f"{fmt_lnfmt(current_lt):<40} {'':>10} "
                f"{fmt_comma18_2(lt_bal)} {fmt_comma18_2(lt_prd)}"
            )
            writer.write_blank()
            lt_bal = 0.0
            lt_prd = 0.0

        current_lt = lt
        writer.write_line(
            f"{fmt_lnfmt(lt):<40} {intrate:>10.2f} {fmt_comma18_2(bal)} {fmt_comma18_2(prd)}"
        )
        lt_bal    += bal
        lt_prd    += prd
        grand_bal += bal
        grand_prd += prd

    if current_lt:
        writer.write_line("-" * len(hdr))
        writer.write_line(
            f"{fmt_lnfmt(current_lt):<40} {'':>10} "
            f"{fmt_comma18_2(lt_bal)} {fmt_comma18_2(lt_prd)}"
        )
        writer.write_blank()

    writer.write_line("=" * len(hdr))
    writer.write_line(
        f"{'TOTAL':<40} {'':>10} {fmt_comma18_2(grand_bal)} {fmt_comma18_2(grand_prd)}"
    )
    writer.write_blank()


# ============================================================================
# SECTION: PRESCRIBED RATES (Including Penalty/Litigation)
# ============================================================================

def section_prescribed(loan_df: pl.DataFrame, sdesc: str, rdate: str, writer: ReportWriter):
    """
    Prescribed rates (LOANTYP IN ('P1','P2')), including penalty/litigation accounts.
    Prints by LOANTYP first, then collapsed by INTRATE only.
    """
    filtered = loan_df.filter(pl.col("LOANTYP").is_in(["P1","P2"]))
    alm = (filtered
           .group_by(["LOANTYP","INTRATE"])
           .agg(pl.col("BALANCE").sum().alias("BALANCE"))
           .sort(["LOANTYP","INTRATE"]))
    alm = alm.with_columns(
        (pl.col("INTRATE") * pl.col("BALANCE")).alias("PRODUCT")
    )

    writer.set_titles(
        "REPORT ID : EIBMLN03",
        sdesc,
        f"WEIGHTED AVERAGE LENDING RATE (PRESCRIBED) AS AT {rdate}",
        "(INCLUDES ACCOUNTS WITH PENALTY RATES & UNDER LITIGATION)",
    )
    writer.start_section()

    hdr = f"{'LOANTYP':<40} {'INTRATE':>10} {'BALANCE':>18} {'PRODUCT':>18}"
    writer.write_line(hdr)
    writer.write_line("-" * len(hdr))

    current_lt = None
    lt_bal     = 0.0
    lt_prd     = 0.0
    grand_bal  = 0.0
    grand_prd  = 0.0

    for row in alm.iter_rows(named=True):
        lt      = str(row["LOANTYP"])
        intrate = float(row["INTRATE"] or 0)
        bal     = float(row["BALANCE"] or 0)
        prd     = float(row["PRODUCT"] or 0)

        if current_lt is not None and lt != current_lt:
            writer.write_line("-" * len(hdr))
            writer.write_line(
                f"{fmt_lnfmt(current_lt):<40} {'':>10} "
                f"{fmt_comma18_2(lt_bal)} {fmt_comma18_2(lt_prd)}"
            )
            writer.write_blank()
            lt_bal = 0.0
            lt_prd = 0.0

        current_lt = lt
        writer.write_line(
            f"{fmt_lnfmt(lt):<40} {intrate:>10.2f} {fmt_comma18_2(bal)} {fmt_comma18_2(prd)}"
        )
        lt_bal    += bal
        lt_prd    += prd
        grand_bal += bal
        grand_prd += prd

    if current_lt:
        writer.write_line("-" * len(hdr))
        writer.write_line(
            f"{fmt_lnfmt(current_lt):<40} {'':>10} "
            f"{fmt_comma18_2(lt_bal)} {fmt_comma18_2(lt_prd)}"
        )
        writer.write_blank()

    writer.write_line("=" * len(hdr))
    writer.write_line(
        f"{'TOTAL':<40} {'':>10} {fmt_comma18_2(grand_bal)} {fmt_comma18_2(grand_prd)}"
    )
    writer.write_blank()

    # PROC SUMMARY collapsed by INTRATE only (second print block in SAS)
    alm2 = (alm
            .group_by("INTRATE")
            .agg([pl.col("BALANCE").sum(), pl.col("PRODUCT").sum()])
            .sort("INTRATE"))

    writer.set_titles(
        "REPORT ID : EIBMLN03",
        sdesc,
        f"WEIGHTED AVERAGE LENDING RATE (PRESCRIBED) AS AT {rdate}",
        "(INCLUDES ACCOUNTS WITH PENALTY RATES & UNDER LITIGATION)",
    )
    writer.start_section()

    hdr2 = f"{'INTRATE':>10} {'BALANCE':>18} {'PRODUCT':>18}"
    writer.write_line(hdr2)
    writer.write_line("-" * len(hdr2))

    g_bal = 0.0
    g_prd = 0.0
    for row in alm2.iter_rows(named=True):
        intrate = float(row["INTRATE"] or 0)
        bal     = float(row["BALANCE"] or 0)
        prd     = float(row["PRODUCT"] or 0)
        writer.write_line(f"{intrate:>10.2f} {fmt_comma18_2(bal)} {fmt_comma18_2(prd)}")
        g_bal += bal
        g_prd += prd

    writer.write_line("=" * len(hdr2))
    writer.write_line(f"{'':>10} {fmt_comma18_2(g_bal)} {fmt_comma18_2(g_prd)}")
    writer.write_blank()


# ============================================================================
# FLAT FILE OUTPUT FOR SRS
# Output to M4LOAN flat file
# - Exclude Staff Loans
# - Exclude Penalty Rates and Loans Under Litigation
# ============================================================================

def output_flat_file(loan_df: pl.DataFrame, reptyear: str, reptmon: str, reptday: str):
    """
    Output flat file for SRS (M4LOAN).
    Header : REPTYEAR + REPTMON + REPTDAY
    Type 01: '001' 'M4 0000' '01' INTRATE Z4. BALANCE Z15.  (prescribed P1/P2, LOANSTAT=1)
    Type 04: '001' 'M4 0000' '04' INTRATE Z4. BALANCE Z15.  (all excl HP, LOANSTAT=1)
    Type 09: BRHNO Z3. 'M4 0000' '09' INTRATE Z4. BALANCE Z15.  (by branch, all)
    EOF record.
    """
    # Prescribed Rates (SRS ACC TYPE 1): LOANSTAT=1, LOANTYP IN ('P1','P2')
    alm = (loan_df
           .filter((pl.col("LOANSTAT") == 1) & pl.col("LOANTYP").is_in(["P1","P2"]))
           .group_by("INTRATE")
           .agg(pl.col("BALANCE").sum())
           .sort("INTRATE"))

    # Prescribed & Non-Prescribed (SRS ACC TYPE 4): LOANSTAT=1, PRODCD != '34111'
    alm1 = (loan_df
            .filter((pl.col("LOANSTAT") == 1) & (pl.col("PRODCD") != "34111"))
            .group_by("INTRATE")
            .agg(pl.col("BALANCE").sum())
            .sort("INTRATE"))

    # By Branch (SRS ACC TYPE 9): all loans, grouped by BRHNO and INTRATE
    loan_brh = (loan_df
                .group_by(["BRHNO","INTRATE"])
                .agg(pl.col("BALANCE").sum())
                .sort(["BRHNO","INTRATE"]))

    lines = []

    # Header record
    lines.append(f"{reptyear}{reptmon}{reptday}")

    # Type 01
    for row in alm.iter_rows(named=True):
        intrate = round(float(row["INTRATE"] or 0) * 100)
        balance = round(float(row["BALANCE"] or 0) * 100)
        lines.append(f"001M4 000001{intrate:04d}{balance:015d}")

    # Type 04
    for row in alm1.iter_rows(named=True):
        intrate = round(float(row["INTRATE"] or 0) * 100)
        balance = round(float(row["BALANCE"] or 0) * 100)
        lines.append(f"001M4 000004{intrate:04d}{balance:015d}")

    # Type 09 (by branch)
    for row in loan_brh.iter_rows(named=True):
        brhno   = str(row["BRHNO"] or "").zfill(3)[:3]
        intrate = round(float(row["INTRATE"] or 0) * 100)
        balance = round(float(row["BALANCE"] or 0) * 100)
        lines.append(f"{brhno}M4 000009{intrate:04d}{balance:015d}")

    lines.append("EOF")

    with open(M4LOAN_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print(f"Flat file written to: {M4LOAN_FILE}")


# ============================================================================
# COMMENTED SECTION FROM SAS (retained as placeholder/reference)
# ============================================================================
#
# /*
# PROC FORMAT;
#    VALUE $PRODF
#       'D' = 'CONVENTIONAL'
#       'I' = 'ISLAMIC BANKING'
#       OTHER = ' ';
#
# PROC PRINT DATA=LOAN NOOBS LABEL;
#    FORMAT AMTIND $PRODF. BALANCE WAMT COMMA20.2 WAVRATE 6.2;
#    LABEL BALANCE = 'TOTAL BALANCE'
#          WAMT    = 'TOTAL WEIGHTED AMOUNT'
#          WAVRATE = 'WEIGHTED AVERAGE RATE';
#    VAR BRANCH BALANCE WAMT WAVRATE;
#    BY AMTIND;
#    PAGEBY AMTIND;
#    SUMBY AMTIND;
#    SUM BALANCE WAMT;
# RUN;
# */


# ============================================================================
# MAIN
# ============================================================================

def main():
    params   = get_report_date()
    sdesc    = get_sdesc()
    reptmon  = params["reptmon"]
    nowk     = params["nowk"]
    rdate    = params["rdate"]
    reptyear = params["reptyear"]
    reptday  = params["reptday"]

    # Load and merge loan data with GP3 risk ratings
    gp3_df  = load_gp3()
    loan_df = load_loan(reptmon, nowk)
    loan_df = build_loan_merged(loan_df, gp3_df)

    # Printed reports
    writer = ReportWriter(REPORT_FILE, PAGE_LENGTH)

    # Section: Weighted Average Lending Rate (exclude penalty/litigation)
    section_walr(loan_df, sdesc, rdate, writer)

    # Section: Prescribed Rates (including penalty/litigation)
    section_prescribed(loan_df, sdesc, rdate, writer)

    writer.flush()
    print(f"Report written to: {REPORT_FILE}")

    # Flat file output for SRS
    output_flat_file(loan_df, reptyear, reptmon, reptday)


if __name__ == "__main__":
    main()
