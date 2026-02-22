# !/usr/bin/env python3
"""
 PROGRAM : EIFMNP07 (JCL: EIIMNP07)
 DATE    : 03.04.98
 MODIFY  : ESMR 2004-720, 2004-579, 2006-1048
 REPORT  : STATISTICS ON ASSET QUALITY - MOVEMENTS IN NPL
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
WAQ_FILE       = INPUT_DIR / "waq.parquet"

# Output files
AQ_OUT         = INPUT_DIR / "aq.parquet"

# Report output
REPORT_FILE    = REPORT_DIR / "EIFMNP07_report.txt"

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
      128,130,983,131,132     = 'HPD AITAB'
      700,705,993,996,380,381 = 'HPD CONVENTIONAL'
      200-299                 = 'HOUSING LOANS'
      OTHER                   = 'OTHERS'
    """
    if loantype in (128, 130, 983, 131, 132):
        return "HPD AITAB"
    elif loantype in (700, 705, 993, 996, 380, 381):
        return "HPD CONVENTIONAL"
    elif 200 <= loantype <= 299:
        return "HOUSING LOANS"
    else:
        return "OTHERS"


def risk_label(days: int, borstat: str, user5: str) -> str:
    if days > 364 or borstat == "W":
        return "BAD"
    elif days > 273:
        return "DOUBTFUL"
    elif days > 182:
        return "SUBSTANDARD 2"
    elif days < 90 and user5 == "N":
        return "SUBSTANDARD-1"
    else:
        return "SUBSTANDARD-1"


# =============================================================================
# MACRO DCLVAR / NXTBLDT helpers
# =============================================================================
LDAY = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
        7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}


def uhc_val(remmth2: int, termchg: float, earnterm: int) -> float:
    if earnterm == 0:
        return 0.0
    return remmth2 * (remmth2 + 1) * termchg / (earnterm * (earnterm + 1))


def iis_sum_partial(remmths: int, remmth2: int, termchg: float,
                    earnterm: int) -> float:
    """
    DO REMMTH = REMMTHS TO REMMTH2 BY -1;
       ACCRINT + 2*(REMMTH+1)*TERMCHG/(EARNTERM*(EARNTERM+1));
    END;
    """
    if remmths < remmth2 or earnterm == 0:
        return 0.0
    total = 0.0
    for rm in range(remmths, remmth2 - 1, -1):
        total += 2 * (rm + 1) * termchg / (earnterm * (earnterm + 1))
    return total


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
# STEP 2: BUILD LOANWOFF (loan + waq)
# =============================================================================
def build_loanwoff(reptmon: str) -> pl.DataFrame:
    """
    PROC SORT DATA=NPL.LOAN&REPTMON; BY ACCTNO;
    PROC SORT DATA=NPL.WAQ; BY ACCTNO;
    DATA LOANWOFF;
      MERGE NPL.LOAN&REPTMON NPL.WAQ (IN=AA DROP=NOTENO NTBRCH);
      BY ACCTNO;
    """
    loan_file = str(LOAN_FILE_TMPL).format(reptmon=reptmon)
    waq_file  = str(WAQ_FILE)
    con = duckdb.connect()
    loanwoff = con.execute(f"""
        SELECT l.*,
               CASE WHEN w.ACCTNO IS NOT NULL THEN 'Y' ELSE 'N' END AS WRITEOFF,
               w.* EXCLUDE (ACCTNO, NOTENO, NTBRCH)
        FROM '{loan_file}' l
        LEFT JOIN '{waq_file}' w ON l.ACCTNO = w.ACCTNO
        ORDER BY l.ACCTNO
    """).pl()
    con.close()

    def fix_row(r: dict) -> dict:
        if r.get("LOANTYPE") in (983, 993):
            r["WDOWNIND"] = "N"
        earnterm = r.get("EARNTERM")
        noteterm = r.get("NOTETERM")
        if not earnterm or earnterm == 0:
            r["EARNTERM"] = noteterm or 0
        return r

    return pl.from_dicts([fix_row(r) for r in loanwoff.to_dicts()],
                         infer_schema_length=None)


# =============================================================================
# STEP 3a: CALCULATE STATISTICS FOR EXISTING NPL ACCOUNTS (LOAN1)
# =============================================================================
def calc_loan1(loanwoff: pl.DataFrame, reptdate_val: date) -> pl.DataFrame:
    """
    DATA LOAN1 -- Existing NPL (EXIST='Y')
    """
    stmth = 1
    enmth = 12
    styr  = reptdate_val.year
    enyr  = styr - 1
    df    = loanwoff.filter(pl.col("EXIST") == "Y")
    out_rows = []

    for row in df.to_dicts():
        acctno   = row.get("ACCTNO")
        noteno   = row.get("NOTENO")
        name     = row.get("NAME", "") or ""
        ntbrch   = row.get("NTBRCH", 0) or 0
        days     = row.get("DAYS", 0) or 0
        loantype = row.get("LOANTYPE", 0) or 0
        issdte   = row.get("ISSDTE")
        bldate   = row.get("BLDATE")
        termchg  = row.get("TERMCHG", 0) or 0
        earnterm = row.get("EARNTERM", 0) or 0
        user5    = row.get("USER5", "") or ""
        writeoff = row.get("WRITEOFF", "N") or "N"
        wdownind = row.get("WDOWNIND", "") or ""
        costctr  = row.get("COSTCTR", 0) or 0
        pendbrh  = row.get("PENDBRH", 0) or 0
        borstat  = row.get("BORSTAT", "") or ""
        curbal   = row.get("CURBAL", 0) or 0
        curbalp  = row.get("CURBALP", 0) or 0
        netbalp  = row.get("NETBALP", 0) or 0
        uhcp     = row.get("UHCP", 0) or 0
        feeamt   = row.get("FEEAMT", 0) or 0
        feetot2  = row.get("FEETOT2", 0) or 0
        feeamt8  = row.get("FEEAMT8", 0) or 0
        feeytd   = row.get("FEEYTD", 0) or 0
        feepdytd = row.get("FEEPDYTD", 0) or 0
        oip      = row.get("OIP", 0) or 0
        netproc  = row.get("NETPROC", 0) or 0

        if writeoff == "Y" and wdownind != "Y":
            borstat = "W"

        curbal = curbal or 0
        # * IF LOANTYPE IN (380,381) THEN ADJUST = FEEAMT - FEETOT2;
        # * ELSE ADJUST = FEEAMT8 - FEETOT2;
        adjust = (feeamt or 0) - (feetot2 or 0)
        accrint = 0.0; uhc = 0.0; oi = 0.0; recover = 0.0; pl_val = 0.0
        remmth1 = 0; remmth2 = 0; remmths = 0
        newnpl = 0.0; nplw = 0.0; npl = 0.0

        if (days < 90 and borstat in (" ", "A", "C", "S", "T", "Y")
                and curbal >= 0 and user5 != "N") or loantype in (983, 993):
            pl_val = netbalp or 0
            if days == 0 and curbal == 0:
                recover = netbalp or 0
                pl_val  = 0.0
        else:
            if loantype in (380, 381):
                oi = feeamt or 0
            else:
                oi = feeamt or 0
            accrint = feeytd or 0

            if borstat == "F":
                curbalp = (curbalp or 0) - (uhcp or 0)

            if termchg > 0 or (user5 == "N" and loantype not in (983, 993)):
                remmth1 = (earnterm - ((bldate.year - issdte.year) * 12
                           + bldate.month - issdte.month + 1)) if (bldate and issdte) else 0
                remmth2 = (earnterm - ((reptdate_val.year - issdte.year) * 12
                           + reptdate_val.month - issdte.month + 1)) if issdte else 0
                remmths = (earnterm - ((styr - issdte.year) * 12
                           + stmth - issdte.month + 1)) if issdte else 0
                if remmth2 < 0:
                    remmth2 = 0
                remmth1 -= 1
                if remmths >= remmth2:
                    accrint += iis_sum_partial(remmths, remmth2, termchg, earnterm)
                if remmth2 > 0:
                    uhc = uhc_val(remmth2, termchg, earnterm)

            if borstat == "W":
                nplw = netbalp or 0
            else:
                recover = (curbalp or 0) - curbal + (feepdytd or 0)
                if recover < 0:
                    curbalp = (curbalp or 0) - recover
                    recover = 0.0
            npl = curbal - uhc + oi
            if borstat == "W":
                npl = 0.0

        branch  = branch_label(ntbrch)
        loantyp = lntyp_format(loantype)

        # Written-off and LOANTYPE overrides
        if writeoff == "Y" or loantype in (983, 993):
            accrint = row.get("WACCRINT", 0) or 0
            newnpl  = row.get("WNEWNPL", 0) or 0
            adjust  = 0.0
            if wdownind != "Y":
                recover = row.get("WRECOVER", 0) or 0
                pl_val  = 0.0
                npl     = 0.0
                nplw    = ((netbalp or 0) + (newnpl or 0) + (accrint or 0)
                           - (recover or 0) - (pl_val or 0))
            else:
                nplw = row.get("WNPLW", 0) or 0
                npl  = ((netbalp or 0) + (newnpl or 0) + (accrint or 0)
                        - (recover or 0) - (nplw or 0) - (pl_val or 0))

        out_rows.append({
            "BRANCH": branch, "ACCTNO": acctno, "NOTENO": noteno,
            "NAME": name, "DAYS": days, "CURBALP": curbalp, "CURBAL": curbal,
            "NETBALP": netbalp, "NEWNPL": newnpl, "ACCRINT": accrint,
            "RECOVER": recover, "PL": pl_val, "NPLW": nplw, "NPL": npl,
            "LOANTYPE": loantype, "LOANTYP": loantyp, "OIP": oip,
            "ADJUST": adjust, "USER5": user5, "BORSTAT": borstat,
            "COSTCTR": costctr, "PENDBRH": pendbrh,
        })

    return pl.from_dicts(out_rows, infer_schema_length=None) if out_rows else pl.DataFrame()


# =============================================================================
# STEP 3b: CALCULATE STATISTICS FOR CURRENT NPL ACCOUNTS (LOAN2)
# =============================================================================
def calc_loan2(loanwoff: pl.DataFrame, reptdate_val: date) -> pl.DataFrame:
    """
    DATA LOAN2 -- Current NPL (EXIST != 'Y')
    """
    stmth = 1
    enmth = 12
    styr  = reptdate_val.year
    enyr  = styr - 1
    df    = loanwoff.filter(pl.col("EXIST") != "Y")
    out_rows = []

    for row in df.to_dicts():
        acctno   = row.get("ACCTNO")
        noteno   = row.get("NOTENO")
        name     = row.get("NAME", "") or ""
        ntbrch   = row.get("NTBRCH", 0) or 0
        days     = row.get("DAYS", 0) or 0
        loantype = row.get("LOANTYPE", 0) or 0
        issdte   = row.get("ISSDTE")
        bldate   = row.get("BLDATE")
        termchg  = row.get("TERMCHG", 0) or 0
        earnterm = row.get("EARNTERM", 0) or 0
        user5    = row.get("USER5", "") or ""
        writeoff = row.get("WRITEOFF", "N") or "N"
        wdownind = row.get("WDOWNIND", "") or ""
        costctr  = row.get("COSTCTR", 0) or 0
        pendbrh  = row.get("PENDBRH", 0) or 0
        borstat  = row.get("BORSTAT", "") or ""
        curbal   = row.get("CURBAL", 0) or 0
        curbalp  = row.get("CURBALP", 0) or 0
        netbalp  = row.get("NETBALP", 0) or 0
        feeamt   = row.get("FEEAMT", 0) or 0
        feetot2  = row.get("FEETOT2", 0) or 0
        feeamt8  = row.get("FEEAMT8", 0) or 0
        oip      = row.get("OIP", 0) or 0

        remmth1 = 0; remmth2 = 0
        if writeoff == "Y" and wdownind != "Y":
            borstat = "W"

        uhc = 0.0; oi = 0.0
        # ADJUST = 0;
        # /* IF LOANTYPE IN (380,381) THEN ADJUST = FEEAMT - FEETOT2;
        #    ELSE ADJUST = FEEAMT8 - FEETOT2; */
        adjust = 0.0

        if (bldate and bldate > date(1900, 1, 1) and termchg > 0) or user5 == "N":
            remmth1 = (earnterm - ((bldate.year - issdte.year) * 12
                       + bldate.month - issdte.month + 1)) if (bldate and issdte) else 0
            remmth2 = (earnterm - ((reptdate_val.year - issdte.year) * 12
                       + reptdate_val.month - issdte.month + 1)) if issdte else 0
            if remmth2 < 0:
                remmth2 = 0
            remmth1 -= 1
            if remmth2 > 0:
                uhc = uhc_val(remmth2, termchg, earnterm)
        else:
            remmth2 = (earnterm - ((reptdate_val.year - issdte.year) * 12
                       + reptdate_val.month - issdte.month + 1)) if issdte else 0
            if remmth2 > 0:
                uhc = uhc_val(remmth2, termchg, earnterm)

        if loantype in (380, 381):
            oi = feeamt or 0
        else:
            oi = feeamt or 0

        newnpl = curbal - uhc + oi
        npl    = newnpl
        accrint = 0.0; recover = 0.0; pl_val = 0.0; nplw = 0.0

        branch  = branch_label(ntbrch)
        loantyp = lntyp_format(loantype)

        # Written-off and LOANTYPE overrides
        if writeoff == "Y" or loantype in (983, 993):
            accrint = row.get("WACCRINT", 0) or 0
            newnpl  = row.get("WNEWNPL", 0) or 0
            adjust  = 0.0
            if wdownind != "Y":
                recover = row.get("WRECOVER", 0) or 0
                pl_val  = 0.0
                npl     = 0.0
                nplw    = ((netbalp or 0) + (newnpl or 0) + (accrint or 0)
                           - (recover or 0) - (pl_val or 0))
            else:
                nplw = row.get("WNPLW", 0) or 0
                npl  = ((netbalp or 0) + (newnpl or 0) + (accrint or 0)
                        - (recover or 0) - (nplw or 0) - (pl_val or 0))

        out_rows.append({
            "BRANCH": branch, "ACCTNO": acctno, "NOTENO": noteno,
            "NAME": name, "DAYS": days, "CURBALP": curbalp, "CURBAL": curbal,
            "NETBALP": netbalp, "NEWNPL": newnpl, "ACCRINT": accrint,
            "RECOVER": recover, "PL": pl_val, "NPLW": nplw, "NPL": npl,
            "LOANTYPE": loantype, "LOANTYP": loantyp, "OIP": oip,
            "ADJUST": adjust, "USER5": user5, "BORSTAT": borstat,
            "COSTCTR": costctr, "PENDBRH": pendbrh,
        })

    return pl.from_dicts(out_rows, infer_schema_length=None) if out_rows else pl.DataFrame()


# =============================================================================
# STEP 4: COMBINE & BUILD LOAN / NPL.AQ
# =============================================================================
def build_loan(loan1: pl.DataFrame, loan2: pl.DataFrame) -> pl.DataFrame:
    """
    DATA LOAN NPL.AQ;
    ARRAY VBL NETBALP NEWNPL ACCRINT RECOVER PL NPLW NPL;
    SET LOAN1 LOAN2;
    WHERE (3000<=COSTCTR<=3999) OR COSTCTR IN (4043,4048);
    """
    vbl_cols = ["NETBALP", "NEWNPL", "ACCRINT", "RECOVER", "PL", "NPLW", "NPL"]
    combined = pl.concat([loan1, loan2], how="diagonal")

    # Null-fill VBL array: DO OVER VBL; IF VBL = . THEN VBL = 0; END;
    for col in vbl_cols:
        if col in combined.columns:
            combined = combined.with_columns(pl.col(col).fill_null(0))

    # CHKNPL = NETBALP+NEWNPL+ACCRINT-RECOVER-PL-NPLW;
    combined = combined.with_columns(
        (pl.col("NETBALP") + pl.col("NEWNPL") + pl.col("ACCRINT")
         - pl.col("RECOVER") - pl.col("PL") - pl.col("NPLW")).alias("CHKNPL")
    )

    combined = combined.with_columns(
        pl.struct(["DAYS", "BORSTAT", "USER5"]).map_elements(
            lambda r: risk_label(r["DAYS"] or 0, r["BORSTAT"] or "", r["USER5"] or ""),
            return_dtype=pl.Utf8
        ).alias("RISK")
    )

    loan = combined.filter(
        ((pl.col("COSTCTR") >= 3000) & (pl.col("COSTCTR") <= 3999))
        | pl.col("COSTCTR").is_in([4043, 4048])
    )
    return loan


# =============================================================================
# REPORTING
# =============================================================================
PAGE_LEN = 60


def format_num(val, width: int = 17, dec: int = 2) -> str:
    """Format number as COMMA17.2 equivalent."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        val = 0
    return f"{val:,.{dec}f}".rjust(width)


def format_num14(val) -> str:
    """Format number as COMMA14.2 equivalent."""
    return format_num(val, 14, 2)


def format_count(val, width: int = 7) -> str:
    if val is None:
        val = 0
    return f"{int(val):,}".rjust(width)


class ReportWriter:
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

    def title_block(self, title1: str, title2: str):
        self.write(title1.center(132), self.ASA_FIRST)
        self.write(title2.center(132), self.ASA_SINGLE)
        self.write("", self.ASA_SINGLE)

    def flush(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            f.write("\n".join(self.lines) + "\n")


# =============================================================================
# TABULATE REPORT
# =============================================================================
def produce_tabulate_report(loan: pl.DataFrame, rdate: str,
                             writer: ReportWriter) -> None:
    """
    PROC TABULATE DATA=LOAN FORMAT=COMMA17.2 MISSING NOSEPS;
    Two tables: (1) LOANTYP x RISK x BRANCH, (2) LOANTYP x BRANCH
    """
    title1 = "PUBLIC ISLAMIC BANK - (NPL FROM 3 MONTHS & ABOVE)"
    title2 = f"STATISTICS ON ASSET QUALITY - MOVEMENTS IN NPL {rdate}"

    num_vars = ["CURBALP", "CURBAL", "NETBALP", "NEWNPL", "PL", "ACCRINT",
                "RECOVER", "NPLW", "NPL", "ADJUST"]
    col_labels = {
        "CURBALP":  "BAL AS AT PREV YEAR (WITH UHC)",
        "CURBAL":   "BAL AS AT END OF RPT DATE (WITH UHC)",
        "NETBALP":  "NET BAL AS AT PREV YEAR (A)",
        "NEWNPL":   "NEW NPL DURING CURRENT YEAR (B)",
        "ACCRINT":  "ACCRUED INTEREST (C)",
        "RECOVER":  "RECOVERIES (D)",
        "PL":       "NPL CLASSIFIED AS PERFORMING (E)",
        "NPLW":     "NPL WRITTEN-OFF (F)",
        "ADJUST":   "ADJUSTMENT",
        "NPL":      "NPL AS AT END OF RPT DATE (A+B+C-D-E-F)",
    }

    # ---- TABLE 1: LOANTYP x RISK x BRANCH ----
    writer.new_page()
    writer.title_block(title1, title2)
    _write_tabulate_by_risk_branch(loan, num_vars, col_labels, writer)

    # ---- TABLE 2: LOANTYP x BRANCH ----
    writer.new_page()
    writer.title_block(title1, title2)
    _write_tabulate_by_branch(loan, num_vars, col_labels, writer)


def _write_tabulate_by_risk_branch(df: pl.DataFrame, num_vars: list,
                                    col_labels: dict, writer: ReportWriter) -> None:
    loan_types = sorted(df["LOANTYP"].unique().to_list())
    risks      = ["SUBSTANDARD-1", "SUBSTANDARD 2", "DOUBTFUL", "BAD"]
    hdr_width  = 29

    for lt in loan_types:
        lt_df = df.filter(pl.col("LOANTYP") == lt)
        writer.write(f"{'RISK        BRANCH':<{hdr_width}} {'NO OF ACCOUNT':>13} " +
                     "  ".join(f"{col_labels.get(v, v)[:22]:>24}" for v in num_vars[:3]), " ")
        writer.write("-" * 220, " ")
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
    hdr_width  = 10  # BOX='BRANCH' RTS=10
    for lt in loan_types:
        lt_df = df.filter(pl.col("LOANTYP") == lt)
        writer.write(f"{'BRANCH':<{hdr_width}} {'NO OF ACCOUNT':>13} " +
                     "  ".join(f"{col_labels.get(v, v)[:22]:>24}" for v in num_vars[:3]), " ")
        writer.write("-" * 220, " ")
        branches   = sorted(lt_df["BRANCH"].unique().to_list())
        total_sums = {v: 0.0 for v in num_vars}; total_n = 0
        for br in branches:
            b_df = lt_df.filter(pl.col("BRANCH") == br)
            n    = len(b_df)
            sums = {v: b_df[v].fill_null(0).sum() for v in num_vars}
            lbl  = f"{br[:10]:<{hdr_width}}"
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
# DETAIL REPORT
# /* DISCONTINUE AS PER LETTER DATED 26/08/03 FR STATISTICS */
# =============================================================================
def produce_detail_report(loan: pl.DataFrame, rdate: str,
                           writer: ReportWriter) -> None:
    """
    PROC SORT DATA=LOAN; BY LOANTYP BRANCH RISK DAYS ACCTNO;
    PROC PRINT LABEL N;
    BY LOANTYP BRANCH RISK; PAGEBY BRANCH; SUMBY RISK;
    """
    title1 = "PUBLIC ISLAMIC BANK - (NPL FROM 3 MONTHS & ABOVE)"
    title2 = f"STATISTICS ON ASSET QUALITY - MOVEMENTS IN NPL {rdate}"

    labels = {
        "ACCTNO":  "MNI ACCOUNT NO",
        "DAYS":    "NO OF DAYS PAST DUE",
        "CURBALP": "BAL AS AT PREV YEAR (WITH UHC)",
        "CURBAL":  "BAL AS AT END OF RPT DATE (WITH UHC)",
        "NETBALP": "NET BAL AS AT PREV YEAR (A)",
        "NEWNPL":  "NEW NPL DURING CURRENT YEAR (B)",
        "ACCRINT": "ACCRUED INTEREST (C)",
        "RECOVER": "RECOVERIES (D)",
        "PL":      "NPL CLASSIFIED AS PERFORMING (E)",
        "NPLW":    "NPL WRITTEN-OFF (F)",
        "ADJUST":  "ADJUSTMENT",
        "NPL":     "NPL AS AT END OF RPT DATE (A+B+C-D-E-F)",
    }
    num_vars = ["CURBALP", "CURBAL", "NETBALP", "NEWNPL", "ACCRINT",
                "RECOVER", "PL", "NPLW", "NPL", "ADJUST"]

    sorted_df   = loan.sort(["LOANTYP", "BRANCH", "RISK", "DAYS", "ACCTNO"])
    prev_branch = None; prev_loantyp = None; prev_risk = None
    risk_sums   = {v: 0.0 for v in num_vars}; total_n = 0
    total_sums  = {v: 0.0 for v in num_vars}

    for row in sorted_df.to_dicts():
        loantyp = row.get("LOANTYP", "") or ""
        branch  = row.get("BRANCH", "") or ""
        risk    = row.get("RISK", "") or ""

        if branch != prev_branch or loantyp != prev_loantyp:
            writer.new_page()
            writer.title_block(title1, title2)
            writer.write(f"LOAN TYPE: {loantyp}  BRANCH: {branch}", " ")
            header = (f"{'MNI ACCOUNT NO':<20} {'NAME':<30} {'DAYS':>5} " +
                      " ".join(f"{labels.get(v, v)[:14]:>15}" for v in num_vars))
            writer.write(header, " ")
            writer.write("-" * 220, " ")

        if risk != prev_risk and prev_risk is not None:
            sumline = (f"{'RISK TOTAL: ' + prev_risk:<55} " +
                       " ".join(format_num14(risk_sums[v]) for v in num_vars))
            writer.write(sumline, "0")
            risk_sums = {v: 0.0 for v in num_vars}

        acctno = row.get("ACCTNO", "") or ""
        name   = (row.get("NAME", "") or "")[:30]
        days   = row.get("DAYS", 0) or 0
        line   = (f"{str(acctno):<20} {name:<30} {days:>5} " +
                  " ".join(format_num14(row.get(v, 0)) for v in num_vars))
        writer.write(line, " ")
        for v in num_vars:
            val = row.get(v, 0) or 0
            risk_sums[v]  += val
            total_sums[v] += val
        total_n += 1
        prev_loantyp = loantyp; prev_branch = branch; prev_risk = risk

    if prev_risk is not None:
        sumline = (f"{'RISK TOTAL: ' + prev_risk:<55} " +
                   " ".join(format_num14(risk_sums[v]) for v in num_vars))
        writer.write(sumline, "0")

    totline = (f"{'GRAND TOTAL':<55} " +
               " ".join(format_num14(total_sums[v]) for v in num_vars))
    writer.write(totline, "0")
    writer.write(f"N = {total_n}", " ")


# =============================================================================
# DISCREPANCY REPORT (commented out in original)
# =============================================================================
#  /*
# PROC PRINT LABEL N;
#    FORMAT CURBALP CURBAL NETBALP NEWNPL ACCRINT RECOVER PL NPLW NPL
#           CHKNPL COMMA14.2;
#    LABEL ACCTNO  = 'MNI ACCOUNT NO'
#          DAYS    = 'NO OF DAYS PAST DUE'
#          CURBALP = 'BAL AS AT PREV YEAR (WITH UHC)'
#          CURBAL  = 'BAL AS AT END OF RPT DATE (WITH UHC)'
#          NETBALP = 'NET BAL AS AT PREV YEAR (A)'
#          NEWNPL  = 'NEW NPL DURING CURRENT YEAR (B)'
#          ACCRINT = 'ACCRUED INTEREST (C)'
#          RECOVER = 'RECOVERIES (D)'
#          PL      = 'NPL CLASSIFIED AS PERFORMING (E)'
#          NPLW    = 'NPL WRITTEN-OFF (F)'
#          NPL     = 'NPL AS AT END OF RPT DATE (A+B+C-D-E-F)'
#          CHKNPL  = '(A+B+C-D-E-F)';
#    VAR ACCTNO NAME DAYS CURBALP CURBAL NETBALP NEWNPL ACCRINT RECOVER
#        PL NPLW NPL CHKNPL;
#    BY LOANTYP BRANCH RISK;
#    WHERE ROUND(CHKNPL,0.01) ^= ROUND(NPL,0.01);
#    TITLE1 'PUBLIC ISLAMIC BANK - (NPL FROM 3 MONTHS & ABOVE)';
#    TITLE2 'STATISTICS ON ASSET QUALITY - MOVEMENTS IN NPL' &RDATE;
#    TITLE3 '(DISCREPANCY REPORT)';    */


# =============================================================================
# MAIN
# =============================================================================
def main():
    _, macro_vars = load_reptdate()
    reptdate_val: date = macro_vars["REPTDATE"]
    reptmon = macro_vars["REPTMON"]
    rdate   = macro_vars["RDATE"]

    loanwoff = build_loanwoff(reptmon)

    loan1 = calc_loan1(loanwoff, reptdate_val)
    loan2 = calc_loan2(loanwoff, reptdate_val)

    loan = build_loan(loan1, loan2)
    loan.write_parquet(str(AQ_OUT))

    writer = ReportWriter(REPORT_FILE)

    # PROC TABULATE
    produce_tabulate_report(loan, rdate, writer)

    # /* DISCONTINUE AS PER LETTER DATED 26/08/03 FR STATISTICS */
    # PROC SORT DATA=LOAN; BY LOANTYP BRANCH RISK DAYS ACCTNO;
    # PROC PRINT LABEL N;
    produce_detail_report(loan, rdate, writer)

    writer.flush()
    print(f"Report written to: {REPORT_FILE}")
    print(f"AQ dataset saved to: {AQ_OUT}")


if __name__ == "__main__":
    main()
