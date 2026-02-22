# !/usr/bin/env python3
"""
 PROGRAM : EIFMNP05 (JCL: EIIMNP05)
 DATE    : 18.03.98
 MODIFY  : ESMR 2004-720, 2004-579, 2006-1048
 REPORT  : MOVEMENTS OF SPECIFIC PROVISION FOR THE MONTH ENDING
           (BASED ON PURCHASE PRICE LESS DEPRECIATION)
"""

import duckdb
import polars as pl
import math
from datetime import date
from pathlib import Path

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR       = Path("/data/npl")
INPUT_DIR      = BASE_DIR / "parquet"
REPORT_DIR     = BASE_DIR / "reports"

# Input parquet files
REPTDATE_FILE  = INPUT_DIR / "reptdate.parquet"
LOAN_FILE_TMPL = INPUT_DIR / "loan{reptmon}.parquet"
IIS_FILE       = INPUT_DIR / "iis.parquet"
# REALISVL_FILE  = INPUT_DIR / "realisvl.parquet"   # commented out in original

# Output files
SP1_OUT        = INPUT_DIR / "sp1.parquet"

# Report output
REPORT_FILE    = REPORT_DIR / "EIFMNP05_report.txt"

REPORT_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# FORMAT DEFINITIONS
# =============================================================================
# %INC PGM(PBBLNFMT);  -- placeholder for PBBLNFMT format definitions
# %INC PGM(PBBELF);    -- placeholder for PBBELF format definitions

BRCHCD_MAP: dict[int, str] = {
    # Populate from PBBLNFMT definitions
}


def brchcd_format(ntbrch: int) -> str:
    return BRCHCD_MAP.get(ntbrch, str(ntbrch))


def branch_label(ntbrch: int) -> str:
    return f"{brchcd_format(ntbrch)} {ntbrch:03d}"


def lntyp_format(loantype: int) -> str:
    """
    PROC FORMAT VALUE LNTYP:
      110,115,983             = 'HPD AITAB'
      700,705,993,996         = 'HPD CONVENTIONAL'
      200-299                 = 'HOUSING LOANS'
      300-499,504-550,900-980 = 'FIXED LOANS'
      OTHER                   = 'OTHERS'
    """
    if loantype in (110, 115, 983):
        return "HPD AITAB"
    elif loantype in (700, 705, 993, 996):
        return "HPD CONVENTIONAL"
    elif 200 <= loantype <= 299:
        return "HOUSING LOANS"
    elif (300 <= loantype <= 499) or (504 <= loantype <= 550) or (900 <= loantype <= 980):
        return "FIXED LOANS"
    else:
        return "OTHERS"


def risk_label(days: int, borstat: str) -> str:
    if days > 364:
        return "BAD"
    elif days > 273:
        return "DOUBTFUL"
    elif days > 182:
        return "SUBSTANDARD 2"
    else:
        return "SUBSTANDARD-1"


# =============================================================================
# UHC CALCULATION HELPER
# =============================================================================
def uhc_val(remmth2: int, termchg: float, earnterm: int) -> float:
    if earnterm == 0:
        return 0.0
    return remmth2 * (remmth2 + 1) * termchg / (earnterm * (earnterm + 1))


# =============================================================================
# STEP 1: READ REPTDATE
# =============================================================================
def load_reptdate() -> tuple[pl.DataFrame, dict]:
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM '{REPTDATE_FILE}'").pl()
    con.close()
    row = df.row(0, named=True)
    reptdate_val: date = row["REPTDATE"]
    reptmon = reptdate_val.month
    rdate_str = reptdate_val.strftime("%d %B %Y").upper()
    return df, {
        "RDATE": rdate_str,
        "REPTMON": f"{reptmon:02d}",
        "REPTDATE": reptdate_val,
    }


# =============================================================================
# STEP 2: BUILD LOANWOFF (merge loan + IIS)
# =============================================================================
def build_loanwoff(reptmon: str) -> pl.DataFrame:
    """
    PROC SORT DATA=NPL.IIS (KEEP=ACCTNO IIS) OUT=IIS; BY ACCTNO;
    DATA LOANWOFF;
     * MERGE NPL.LOAN&REPTMON NPL.REALISVL (IN=BB) IIS;
       MERGE NPL.LOAN&REPTMON IIS;
       BY ACCTNO;
    """
    loan_file = str(LOAN_FILE_TMPL).format(reptmon=reptmon)
    iis_file  = str(IIS_FILE)
    con = duckdb.connect()
    loanwoff = con.execute(f"""
        SELECT l.*, i.IIS
        FROM '{loan_file}' l
        LEFT JOIN (SELECT ACCTNO, IIS FROM '{iis_file}') i ON l.ACCTNO = i.ACCTNO
        ORDER BY l.ACCTNO
    """).pl()
    con.close()

    # Post-merge fixups
    def fix_row(r: dict) -> dict:
        if r.get("LOANTYPE") in (983, 993):
            r["WDOWNIND"] = "N"
        # IF BB THEN HARDCODE = 'N'; ELSE HARDCODE = 'N';  -- commented out in original
        earnterm = r.get("EARNTERM")
        noteterm = r.get("NOTETERM")
        if not earnterm or earnterm == 0:
            r["EARNTERM"] = noteterm or 0
        return r

    return pl.from_dicts([fix_row(r) for r in loanwoff.to_dicts()],
                         infer_schema_length=None)


# =============================================================================
# STEP 3a: CALCULATE SP FOR EXISTING NPL ACCOUNTS (LOAN1)
# =============================================================================
def calc_loan1(loanwoff: pl.DataFrame, reptdate_val: date) -> pl.DataFrame:
    """
    DATA LOAN1 -- Existing NPL (EXIST='Y')
    """
    stmth = 1
    styr  = reptdate_val.year
    df    = loanwoff.filter(pl.col("EXIST") == "Y")
    out_rows = []

    for row in df.to_dicts():
        acctno   = row.get("ACCTNO")
        noteno   = row.get("NOTENO")
        name     = row.get("NAME", "") or ""
        ntbrch   = row.get("NTBRCH", 0) or 0
        netproc  = row.get("NETPROC", 0) or 0
        curbal   = row.get("CURBAL", 0) or 0
        borstat  = row.get("BORSTAT", "") or ""
        days     = row.get("DAYS", 0) or 0
        loantype = row.get("LOANTYPE", 0) or 0
        issdte   = row.get("ISSDTE")
        bldate   = row.get("BLDATE")
        termchg  = row.get("TERMCHG", 0) or 0
        earnterm = row.get("EARNTERM", 0) or 0
        costctr  = row.get("COSTCTR", 0) or 0
        pendbrh  = row.get("PENDBRH", 0) or 0
        iis      = row.get("IIS", 0) or 0
        spp1     = row.get("SPP1", 0) or 0
        appvalue = row.get("APPVALUE", 0) or 0
        census7  = row.get("CENSUS7", "") or ""
        hardcode = row.get("HARDCODE", "") or ""
        wrealvl  = row.get("WREALVL", 0) or 0
        wsppl    = row.get("WSPPL")
        wsp      = row.get("WSP")

        uhc = 0.0
        # IF TERMCHG = 0 THEN IISPREV = IISP;  -- commented out in original
        if bldate and bldate > date(1900, 1, 1) and termchg > 0:
            if days > 91 or borstat in ("F", "R", "I"):
                remmth1 = (earnterm - ((bldate.year - issdte.year) * 12
                           + bldate.month - issdte.month + 1)) if issdte else 0
                remmth2 = (earnterm - ((reptdate_val.year - issdte.year) * 12
                           + reptdate_val.month - issdte.month + 1)) if issdte else 0
                remmths = (earnterm - ((styr - issdte.year) * 12
                           + stmth - issdte.month + 1)) if issdte else 0
                if remmth2 < 0:
                    remmth2 = 0
                if loantype in (110, 115):
                    remmth1 -= 3
                else:
                    remmth1 -= 1
                # IF REMMTH1 >= REMMTH2 THEN ...  -- commented out in original
                if remmth2 > 0:
                    uhc = uhc_val(remmth2, termchg, earnterm)

        curbal = curbal or 0
        netbal = curbal - uhc
        # OSPRIN = CURBAL - UHC - IISPREV - IIS;  -- commented out in original
        osprin = curbal - uhc - iis

        marketvl = 0.0; netexp = 0.0; sp = 0.0

        if (appvalue > 0 and days > 91
                and borstat not in ("F", "R", "I", "Y", "W")):
            age = int(reptdate_val.year - issdte.year +
                      (reptdate_val.month - issdte.month) / 12) if issdte else 0
            if census7 != "9":
                marketvl = appvalue - appvalue * age * 0.2
            if hardcode == "Y":
                marketvl = wrealvl
            if marketvl < 0:
                marketvl = 0.0
            netexp = osprin - marketvl
            if days > 364:
                sp = netexp
            elif days > 273:
                sp = netexp / 2
            elif days > 182:
                sp = netexp * 0.2
            else:
                sp = 0.0
        else:
            if borstat != "R":
                marketvl = 0.0
            if hardcode == "Y":
                marketvl = wrealvl
            netexp = osprin - marketvl
            if days > 364 or borstat in ("F", "R", "I", "W"):
                sp = netexp
            elif days > 273:
                sp = netexp / 2
            elif days > 91 and borstat == "Y":
                sp = netexp / 5
            else:
                sp = 0.0

        if sp < 0:
            sp = 0.0
        sppl   = sp - spp1
        sppw   = 0.0; recover = 0.0
        if sppl < 0:
            sppl = 0.0
        if hardcode == "Y":
            if wsppl is not None:
                sppl = wsppl
            if wsp is not None:
                sp = wsp

        if borstat == "W":
            sppw    = spp1
            sppl    = 0.0
            sp      = 0.0
            marketvl = 0.0
        else:
            recover = spp1 - sp
            if recover < 0:
                recover = 0.0

        risk    = risk_label(days, borstat)
        branch  = branch_label(ntbrch)
        loantyp = lntyp_format(loantype)

        out_rows.append({
            "BRANCH": branch, "NTBRCH": ntbrch, "ACCTNO": acctno,
            "NOTENO": noteno, "NAME": name, "DAYS": days, "BORSTAT": borstat,
            "NETPROC": netproc, "CURBAL": curbal, "UHC": uhc, "NETBAL": netbal,
            "IIS": iis, "OSPRIN": osprin, "MARKETVL": marketvl,
            "NETEXP": netexp, "SPP1": spp1, "SPPL": sppl,
            "RECOVER": recover, "SPPW": sppw, "SP": sp,
            "RISK": risk, "LOANTYP": loantyp, "COSTCTR": costctr,
            "PENDBRH": pendbrh,
        })

    return pl.from_dicts(out_rows, infer_schema_length=None) if out_rows else pl.DataFrame()


# =============================================================================
# STEP 3b: CALCULATE SP FOR CURRENT NPL ACCOUNTS (LOAN2)
# =============================================================================
def calc_loan2(loanwoff: pl.DataFrame, reptdate_val: date) -> pl.DataFrame:
    """
    DATA LOAN2 -- Current NPL (EXIST != 'Y')
    """
    stmth = 1
    styr  = reptdate_val.year
    df    = loanwoff.filter(pl.col("EXIST") != "Y")
    out_rows = []

    for row in df.to_dicts():
        acctno   = row.get("ACCTNO")
        noteno   = row.get("NOTENO")
        name     = row.get("NAME", "") or ""
        ntbrch   = row.get("NTBRCH", 0) or 0
        netproc  = row.get("NETPROC", 0) or 0
        curbal   = row.get("CURBAL", 0) or 0
        borstat  = row.get("BORSTAT", "") or ""
        days     = row.get("DAYS", 0) or 0
        loantype = row.get("LOANTYPE", 0) or 0
        issdte   = row.get("ISSDTE")
        bldate   = row.get("BLDATE")
        termchg  = row.get("TERMCHG", 0) or 0
        earnterm = row.get("EARNTERM", 0) or 0
        costctr  = row.get("COSTCTR", 0) or 0
        pendbrh  = row.get("PENDBRH", 0) or 0
        iis      = row.get("IIS", 0) or 0
        spp1     = 0.0; recover = 0.0; sppw = 0.0  # RETAIN SPP1 RECOVER SPPW 0
        appvalue = row.get("APPVALUE", 0) or 0
        census7  = row.get("CENSUS7", "") or ""
        hardcode = row.get("HARDCODE", "") or ""
        wrealvl  = row.get("WREALVL", 0) or 0
        wsppl    = row.get("WSPPL")
        wsp      = row.get("WSP")

        uhc = 0.0
        if bldate and bldate > date(1900, 1, 1) and termchg > 0:
            remmth1 = (earnterm - ((bldate.year - issdte.year) * 12
                       + bldate.month - issdte.month + 1)) if issdte else 0
            remmth2 = (earnterm - ((reptdate_val.year - issdte.year) * 12
                       + reptdate_val.month - issdte.month + 1)) if issdte else 0
            remmths = (earnterm - ((styr - issdte.year) * 12
                       + stmth - issdte.month + 1)) if issdte else 0
            if remmth2 < 0:
                remmth2 = 0
            if loantype in (110, 115):
                remmth1 -= 3
            else:
                remmth1 -= 1
            # IF REMMTH1 >= REMMTH2 THEN ...  -- commented out in original
            if remmth2 > 0:
                uhc = uhc_val(remmth2, termchg, earnterm)
        else:
            remmth2 = (earnterm - ((reptdate_val.year - issdte.year) * 12
                       + reptdate_val.month - issdte.month + 1)) if issdte else 0
            if remmth2 < 0:
                remmth2 = 0
            if remmth2 > 0:
                uhc = uhc_val(remmth2, termchg, earnterm)

        netbal = curbal - uhc
        # OSPRIN = CURBAL - UHC - IISPREV - IIS;  -- commented out in original
        osprin = curbal - uhc - iis

        marketvl = 0.0; netexp = 0.0; sp = 0.0

        if (appvalue > 0 and days > 91
                and borstat not in ("F", "R", "I", "Y", "W")):
            age = int(reptdate_val.year - issdte.year +
                      (reptdate_val.month - issdte.month) / 12) if issdte else 0
            if census7 != "9":
                marketvl = appvalue - appvalue * age * 0.2
            if hardcode == "Y":
                marketvl = wrealvl
            if marketvl < 0:
                marketvl = 0.0
            netexp = osprin - marketvl
            if days > 364:
                sp = netexp
            elif days > 273:
                sp = netexp / 2
            elif days > 182:
                sp = netexp * 0.2
            else:
                sp = 0.0
        else:
            if borstat != "R":
                marketvl = 0.0
            if hardcode == "Y":
                marketvl = wrealvl
            netexp = osprin - marketvl
            if days > 364 or borstat in ("F", "R", "I", "W"):
                sp = netexp
            elif days > 273:
                sp = netexp / 2
            elif days > 91 and borstat == "Y":
                sp = netexp / 5
            else:
                sp = 0.0

        if sp < 0:
            sp = 0.0
        sppl = sp
        if hardcode == "Y":
            if wsppl is not None:
                sppl = wsppl
            if wsp is not None:
                sp = wsp

        risk    = risk_label(days, borstat)
        branch  = branch_label(ntbrch)
        loantyp = lntyp_format(loantype)

        out_rows.append({
            "BRANCH": branch, "NTBRCH": ntbrch, "ACCTNO": acctno,
            "NOTENO": noteno, "NAME": name, "DAYS": days, "BORSTAT": borstat,
            "NETPROC": netproc, "CURBAL": curbal, "UHC": uhc, "NETBAL": netbal,
            "IIS": iis, "OSPRIN": osprin, "MARKETVL": marketvl,
            "NETEXP": netexp, "SPP1": spp1, "SPPL": sppl,
            "RECOVER": recover, "SPPW": sppw, "SP": sp,
            "RISK": risk, "LOANTYP": loantyp, "COSTCTR": costctr,
            "PENDBRH": pendbrh,
        })

    return pl.from_dicts(out_rows, infer_schema_length=None) if out_rows else pl.DataFrame()


# =============================================================================
# STEP 4: COMBINE INTO LOAN3 & SAVE SP1
# =============================================================================
def build_loan3(loan1: pl.DataFrame, loan2: pl.DataFrame) -> pl.DataFrame:
    """
    DATA LOAN3 NPL.SP1;
    SET LOAN1 LOAN2;
    WHERE 3000<=COSTCTR<=3999;
    """
    combined = pl.concat([loan1, loan2], how="diagonal")
    loan3 = combined.filter(
        (pl.col("COSTCTR") >= 3000) & (pl.col("COSTCTR") <= 3999)
    )
    return loan3


# =============================================================================
# REPORT HELPERS
# =============================================================================
PAGE_LEN = 60


def format_num(val, width: int = 14, dec: int = 2) -> str:
    """Format number as COMMA14.2 equivalent."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        val = 0
    return f"{val:,.{dec}f}".rjust(width)


def format_count(val, width: int = 7) -> str:
    if val is None:
        val = 0
    return f"{int(val):,}".rjust(width)


class ReportWriter:
    """ASA carriage control report writer."""
    ASA_FIRST  = "1"
    ASA_DOUBLE = "0"
    ASA_SINGLE = " "

    def __init__(self, filepath: Path):
        self.filepath = filepath
        self.lines: list[str] = []
        self.current_line = 0
        self.page_num = 0

    def new_page(self):
        self.page_num += 1
        self.current_line = 0

    def write(self, text: str, cc: str = " "):
        self.lines.append(cc + text)
        if cc != "+":
            self.current_line += 1
        if self.current_line >= PAGE_LEN:
            self.new_page()

    def title_block(self, title1: str, title2: str, title3: str = ""):
        self.write(title1.center(132), self.ASA_FIRST)
        self.write(title2.center(132), self.ASA_SINGLE)
        if title3:
            self.write(title3.center(132), self.ASA_SINGLE)
        self.write("", self.ASA_SINGLE)

    def flush(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            f.write("\n".join(self.lines) + "\n")


# =============================================================================
# TABULATE REPORT (%MACRO TBLS, I=3)
# =============================================================================
def produce_tabulate_report(loan3: pl.DataFrame, rdate: str,
                             writer: ReportWriter) -> None:
    """
    PROC TABULATE DATA=LOAN3 FORMAT=COMMA14.2 MISSING NOSEPS;
    Two tables: (1) LOANTYP x RISK x BRANCH, (2) LOANTYP x BRANCH
    """
    title1 = "PUBLIC ISLAMIC BANK - (NPL FROM 3 MONTHS & ABOVE)"
    title2 = f"MOVEMENTS OF SPECIFIC PROVISION FOR THE MONTH ENDING {rdate} (EXISTING AND CURRENT)"
    title3 = "(BASED ON PURCHASE PRICE LESS DEPRECIATION)"

    num_vars = ["CURBAL", "UHC", "NETBAL", "IIS", "OSPRIN", "MARKETVL",
                "NETEXP", "SPP1", "SPPL", "RECOVER", "SPPW", "SP"]
    col_labels = {
        "CURBAL":   "CURRENT BAL (A)",
        "UHC":      "UNEARNED HIRING CHARGES (B)",
        "NETBAL":   "NET BAL (A-B=C)",
        "IIS":      "IIS (E)",
        "OSPRIN":   "PRINCIPAL OUTSTANDING (C-E=F)",
        "MARKETVL": "REALISABLE VALUE (G)",
        "NETEXP":   "NET EXPOSURE (F-G=H)",
        "SPP1":     "OPENING BAL FOR FINANCIAL YEAR (I)",
        "SPPL":     "PROVISION MADE AGAINST PROFIT & LOSS (J)",
        "RECOVER":  "WRITTEN BACK TO PROFIT & LOSS (K)",
        "SPPW":     "WRITTEN OFF AGAINST PROVISION (L)",
        "SP":       "CLOSING BAL AS AT RPT DATE (I+J-K-L)",
    }

    # ---- TABLE 1: LOANTYP x RISK x BRANCH ----
    writer.new_page()
    writer.title_block(title1, title2, title3)
    _write_tabulate_by_risk_branch(loan3, num_vars, col_labels, writer)

    # ---- TABLE 2: LOANTYP x BRANCH ----
    writer.new_page()
    writer.title_block(title1, title2, title3)
    _write_tabulate_by_branch(loan3, num_vars, col_labels, writer)


def _write_tabulate_by_risk_branch(df: pl.DataFrame, num_vars: list,
                                    col_labels: dict, writer: ReportWriter) -> None:
    loan_types = sorted(df["LOANTYP"].unique().to_list())
    risks      = ["SUBSTANDARD-1", "SUBSTANDARD 2", "DOUBTFUL", "BAD"]
    hdr_width  = 29

    for lt in loan_types:
        lt_df = df.filter(pl.col("LOANTYP") == lt)
        writer.write(f"{'RISK        BRANCH':<{hdr_width}} {'NO OF ACCOUNT':>13} " +
                     "  ".join(f"{col_labels[v][:20]:>22}" for v in num_vars[:3]), " ")
        writer.write("-" * 200, " ")

        grand_sums = {v: 0.0 for v in num_vars}; grand_n = 0
        for risk in risks:
            r_df = lt_df.filter(pl.col("RISK") == risk)
            branches  = sorted(r_df["BRANCH"].unique().to_list())
            risk_sums = {v: 0.0 for v in num_vars}; risk_n = 0
            for br in branches:
                b_df = r_df.filter(pl.col("BRANCH") == br)
                n    = len(b_df)
                sums = {v: b_df[v].fill_null(0).sum() for v in num_vars}
                lbl  = f"{risk[:14]:<14} {br[:14]:<14}"
                line = (f"{lbl:<{hdr_width}} {format_count(n):>13} " +
                        "  ".join(format_num(sums[v]) for v in num_vars))
                writer.write(line, " ")
                for v in num_vars:
                    risk_sums[v] += sums[v]
                risk_n += n
            lbl  = f"{risk[:14]:<14} {'SUB-TOTAL':<14}"
            line = (f"{lbl:<{hdr_width}} {format_count(risk_n):>13} " +
                    "  ".join(format_num(risk_sums[v]) for v in num_vars))
            writer.write(line, "0")
            for v in num_vars:
                grand_sums[v] += risk_sums[v]
            grand_n += risk_n

        lbl  = f"{'TOTAL':<{hdr_width}}"
        line = (f"{lbl} {format_count(grand_n):>13} " +
                "  ".join(format_num(grand_sums[v]) for v in num_vars))
        writer.write(line, "0")
        writer.write("", " ")


def _write_tabulate_by_branch(df: pl.DataFrame, num_vars: list,
                               col_labels: dict, writer: ReportWriter) -> None:
    loan_types = sorted(df["LOANTYP"].unique().to_list())
    hdr_width  = 9

    for lt in loan_types:
        lt_df = df.filter(pl.col("LOANTYP") == lt)
        writer.write(f"{'BRANCH':<{hdr_width}} {'NO OF ACCOUNT':>13} " +
                     "  ".join(f"{col_labels[v][:20]:>22}" for v in num_vars[:3]), " ")
        writer.write("-" * 200, " ")
        branches   = sorted(lt_df["BRANCH"].unique().to_list())
        total_sums = {v: 0.0 for v in num_vars}; total_n = 0
        for br in branches:
            b_df = lt_df.filter(pl.col("BRANCH") == br)
            n    = len(b_df)
            sums = {v: b_df[v].fill_null(0).sum() for v in num_vars}
            lbl  = f"{br[:9]:<{hdr_width}}"
            line = (f"{lbl} {format_count(n):>13} " +
                    "  ".join(format_num(sums[v]) for v in num_vars))
            writer.write(line, " ")
            for v in num_vars:
                total_sums[v] += sums[v]
            total_n += n
        lbl  = f"{'TOTAL':<{hdr_width}}"
        line = (f"{lbl} {format_count(total_n):>13} " +
                "  ".join(format_num(total_sums[v]) for v in num_vars))
        writer.write(line, "0")
        writer.write("", " ")


# =============================================================================
# DETAIL REPORT (%MACRO DTLS, I=3) -- only for KLM 001 and PJN 034
# /* DISCONTINUE AS PER LETTER DATED 26/08/03 FR STATISTICS
# %DTLS;  */
# =============================================================================
def produce_detail_report(loan3: pl.DataFrame, rdate: str,
                           writer: ReportWriter) -> None:
    """
    PROC PRINT LABEL N;
    WHERE BRANCH = 'KLM 001' OR BRANCH = 'PJN 034';
    BY LOANTYP BRANCH RISK; PAGEBY BRANCH; SUMBY RISK;
    """
    title1 = "PUBLIC ISLAMIC BANK - (NPL FROM 3 MONTHS & ABOVE)"
    title2 = f"MOVEMENTS OF SPECIFIC PROVISION FOR THE MONTH ENDING {rdate} (EXISTING AND CURRENT)"
    title3 = "(BASED ON PURCHASE PRICE LESS DEPRECIATION)"

    labels = {
        "ACCTNO":   "MNI ACCOUNT NO",
        "DAYS":     "NO OF DAYS PAST DUE",
        "BORSTAT":  "BORROWER'S STATUS",
        "NETPROC":  "LIMIT",
        "CURBAL":   "CURRENT BAL (A)",
        "UHC":      "UNEARNED HIRING CHARGES (B)",
        "NETBAL":   "NET BAL (A-B=C)",
        "IIS":      "CURRENT YEAR IIS (E)",
        "OSPRIN":   "PRINCIPAL OUTSTANDING (C-E=F)",
        "MARKETVL": "REALISABLE VALUE (G)",
        "NETEXP":   "NET EXPOSURE (F-G=H)",
        "SPP1":     "OPENING BAL FOR FINANCIAL YEAR (I)",
        "SPPL":     "PROVISION MADE AGAINST PROFIT & LOSS (J)",
        "RECOVER":  "WRITTEN BACK TO PROFIT & LOSS (K)",
        "SPPW":     "WRITTEN OFF AGAINST PROVISION (L)",
        "SP":       "CLOSING BAL AS AT RPT DATE (I+J-K-L)",
    }
    num_vars = ["NETPROC", "CURBAL", "UHC", "NETBAL", "IIS", "OSPRIN",
                "MARKETVL", "NETEXP", "SPP1", "SPPL", "RECOVER", "SPPW", "SP"]

    # WHERE BRANCH = 'KLM 001' OR BRANCH = 'PJN 034'
    filtered = loan3.filter(pl.col("BRANCH").is_in(["KLM 001", "PJN 034"]))
    sorted_df = filtered.sort(["LOANTYP", "BRANCH", "RISK", "DAYS", "ACCTNO"])

    prev_branch = None; prev_loantyp = None; prev_risk = None
    risk_sums   = {v: 0.0 for v in num_vars}; total_n = 0
    total_sums  = {v: 0.0 for v in num_vars}

    for row in sorted_df.to_dicts():
        loantyp = row.get("LOANTYP", "") or ""
        branch  = row.get("BRANCH", "") or ""
        risk    = row.get("RISK", "") or ""

        if branch != prev_branch or loantyp != prev_loantyp:
            writer.new_page()
            writer.title_block(title1, title2, title3)
            writer.write(f"LOAN TYPE: {loantyp}  BRANCH: {branch}", " ")
            header = (f"{'MNI ACCOUNT NO':<20} {'NAME':<30} {'DAYS':>5} {'STAT':<5} " +
                      " ".join(f"{labels[v][:14]:>15}" for v in num_vars))
            writer.write(header, " ")
            writer.write("-" * 200, " ")

        if risk != prev_risk and prev_risk is not None:
            sumline = (f"{'RISK TOTAL: ' + prev_risk:<55} " +
                       " ".join(format_num(risk_sums[v]) for v in num_vars))
            writer.write(sumline, "0")
            risk_sums = {v: 0.0 for v in num_vars}

        acctno = row.get("ACCTNO", "") or ""
        name   = (row.get("NAME", "") or "")[:30]
        days   = row.get("DAYS", 0) or 0
        bstat  = row.get("BORSTAT", "") or ""
        line   = (f"{str(acctno):<20} {name:<30} {days:>5} {bstat:<5} " +
                  " ".join(format_num(row.get(v, 0)) for v in num_vars))
        writer.write(line, " ")
        for v in num_vars:
            val = row.get(v, 0) or 0
            risk_sums[v]  += val
            total_sums[v] += val
        total_n += 1
        prev_loantyp = loantyp; prev_branch = branch; prev_risk = risk

    if prev_risk is not None:
        sumline = (f"{'RISK TOTAL: ' + prev_risk:<55} " +
                   " ".join(format_num(risk_sums[v]) for v in num_vars))
        writer.write(sumline, "0")

    totline = (f"{'GRAND TOTAL':<55} " +
               " ".join(format_num(total_sums[v]) for v in num_vars))
    writer.write(totline, "0")
    writer.write(f"N = {total_n}", " ")


# =============================================================================
# MAIN
# =============================================================================
def main():
    # Load REPTDATE
    _, macro_vars = load_reptdate()
    reptdate_val: date = macro_vars["REPTDATE"]
    reptmon = macro_vars["REPTMON"]
    rdate   = macro_vars["RDATE"]

    # Build LOANWOFF
    loanwoff = build_loanwoff(reptmon)

    # Calculate LOAN1 (existing NPL)
    loan1 = calc_loan1(loanwoff, reptdate_val)

    # Calculate LOAN2 (current NPL)
    loan2 = calc_loan2(loanwoff, reptdate_val)

    # Build LOAN3 (combined) and save SP1
    loan3 = build_loan3(loan1, loan2)
    loan3.write_parquet(str(SP1_OUT))

    # Produce reports
    # OPTIONS NOCENTER NODATE NONUMBER MISSING=0
    writer = ReportWriter(REPORT_FILE)

    # %TBLS (I=3)
    produce_tabulate_report(loan3, rdate, writer)

    # /* DISCONTINUE AS PER LETTER DATED 26/08/03 FR STATISTICS
    # %DTLS;  */
    produce_detail_report(loan3, rdate, writer)

    writer.flush()
    print(f"Report written to: {REPORT_FILE}")
    print(f"SP1 dataset saved to: {SP1_OUT}")


if __name__ == "__main__":
    main()
