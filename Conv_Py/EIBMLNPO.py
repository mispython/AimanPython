#!/usr/bin/env python3
"""
Program  : EIBMLNPO.py
Amendment: 31/05/04 (RHA); minor changes 28-07-2004 (MAA3);
           2007-1412: Include new reason codes 05 & 06 (RHA 1-11-2007)
Purpose  : Monthly Report for Loan Paidoff
           - Report 1: By Region / Branch / Reasons
           - Report 2: List of Customers with Loan Paidoff by Branch
           - Report 3: By Reasons / Regions / Branch
"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
from PBMISFMT import format_lnpogrp   # LNPOGRP. format (branch -> region label)

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR       = Path("/data")
LOAN_DIR       = BASE_DIR / "loan"
BNM_DIR        = BASE_DIR / "bnm"

REPTDATE_FILE  = LOAN_DIR / "reptdate.parquet"
SDESC_FILE     = BNM_DIR  / "sdesc.parquet"
LNNOTE_FILE    = LOAN_DIR / "lnnote.parquet"
LNCOMM_FILE    = LOAN_DIR / "lncomm.parquet"
BRHFILE        = BASE_DIR / "brhfile.txt"   # fixed-width branch reference file

OUTPUT_DIR     = BASE_DIR / "output"
OUTPUT_REPORT  = OUTPUT_DIR / "eibmlnpo_report.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# CONSTANTS
# ============================================================================

PAGE_LENGTH = 60
LINE_WIDTH  = 132

# %LET HPD: excluded loan types
HPD = {128, 130, 131, 132, 380, 381, 700, 705, 720, 725}

# BRH codes to exclude (head-office / special branches)
EXCLUDE_BRH = {
    "H10", "H12", "H15", "H16", "H17", "H18", "H24", "H25",
    "H14", "H19", "H20", "H23", "H22", "H21",
}

# PAIDOFF reasons lookup
PAIDOFF_REASONS: dict[str, str] = {
    "1":  "REFINANCING FROM OFIS",
    "01": "REFINANCING FROM OFIS",
    " 1": "REFINANCING FROM OFIS",
    "1 ": "REFINANCING FROM OFIS",
    "2":  "PROPERTY SOLD",
    "02": "PROPERTY SOLD",
    " 2": "PROPERTY SOLD",
    "2 ": "PROPERTY SOLD",
    "3":  "FINAL PAYMENT/INSTALMENT",
    "03": "FINAL PAYMENT/INSTALMENT",
    " 3": "FINAL PAYMENT/INSTALMENT",
    "3 ": "FINAL PAYMENT/INSTALMENT",
    "4":  "EPF WITHDRAWAL",
    "04": "EPF WITHDRAWAL",
    " 4": "EPF WITHDRAWAL",
    "4 ": "EPF WITHDRAWAL",
    "5":  "MIGRATION OF ACCOUNTS UNDER PROGRESIVE RELEASE",
    "05": "MIGRATION OF ACCOUNTS UNDER PROGRESIVE RELEASE",
    " 5": "MIGRATION OF ACCOUNTS UNDER PROGRESIVE RELEASE",
    "5 ": "MIGRATION OF ACCOUNTS UNDER PROGRESIVE RELEASE",
    "6":  "PAIDOFF FROM EXCESS FUNDS",
    "06": "PAIDOFF FROM EXCESS FUNDS",
    " 6": "PAIDOFF FROM EXCESS FUNDS",
    "6 ": "PAIDOFF FROM EXCESS FUNDS",
    "7":  "REFINANCING FROM BANK RAKYAT",
    "07": "REFINANCING FROM BANK RAKYAT",
    " 7": "REFINANCING FROM BANK RAKYAT",
    "7 ": "REFINANCING FROM BANK RAKYAT",
    "8":  "REFINANCING FROM BSN",
    "08": "REFINANCING FROM BSN",
    " 8": "REFINANCING FROM BSN",
    "8 ": "REFINANCING FROM BSN",
    "9":  "REFINANCING FROM MBSB",
    "09": "REFINANCING FROM MBSB",
    " 9": "REFINANCING FROM MBSB",
    "9 ": "REFINANCING FROM MBSB",
    "10": "REFINANCING FROM BANK ISLAM",
    "99": "OTHERS",
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def fmt_ddmmyy8(d: date) -> str:
    """Format date as DD/MM/YY (SAS DDMMYY8.)"""
    return d.strftime("%d/%m/%y")


def fmt_z2(n: int) -> str:
    return f"{n:02d}"


def fmt_year4(d: date) -> str:
    return d.strftime("%Y")


def fmt_comma5(val) -> str:
    if val is None:
        return "0".rjust(5)
    try:
        return f"{int(val):,}".rjust(5)
    except (TypeError, ValueError):
        return "0".rjust(5)


def fmt_comma13_2(val) -> str:
    if val is None:
        return "0.00".rjust(13)
    try:
        return f"{float(val):,.2f}".rjust(13)
    except (TypeError, ValueError):
        return "0.00".rjust(13)


def get_reason_label(paidoff) -> str:
    """Map PAIDOFF code to reason description."""
    return PAIDOFF_REASONS.get(str(paidoff or "").strip(), "NOT SPECIFIED ??")


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
    SDD, WK, WK1 = 1, "1", "4"
elif day == 15:
    SDD, WK, WK1 = 9, "2", "1"
elif day == 22:
    SDD, WK, WK1 = 16, "3", "2"
else:
    SDD, WK, WK1 = 23, "4", "3"

MM = REPTDATE.month
MM1 = (MM - 1) if (WK == "1" and MM > 1) else (12 if WK == "1" else MM)

NOWK      = WK
NOWK1     = WK1
REPTMON   = fmt_z2(MM)
REPTMON1  = fmt_z2(MM1)
REPTYEAR  = fmt_year4(REPTDATE)
RDATE     = fmt_ddmmyy8(REPTDATE)
REPTDAY   = fmt_z2(day)

# ============================================================================
# STEP 2: READ SDESC
# ============================================================================

sdesc_df = con.execute(
    f"SELECT * FROM read_parquet('{SDESC_FILE}')"
).pl()
SDESC = str(sdesc_df["SDESC"][0]).ljust(26)[:26]

# ============================================================================
# STEP 3: LOAD AND FILTER LNNOTE
# ============================================================================

lnnote_raw = con.execute(
    f"SELECT * FROM read_parquet('{LNNOTE_FILE}')"
).pl()


def process_lnnote(df: pl.DataFrame) -> pl.DataFrame:
    """
    Apply DATA LNNOTE filters and derivations:
    - BRANCH = PENDBRH fallback to NTBRCH then ACCBRCH
    - PAIDOFF > ' ', PAIDIND = 'P'
    - LOANTYPE NOT IN HPD
    - LSTTRNCD NE 652
    - Derive PERIOD from LASTTRAN
    - LOANAMT from NETPROC (100-200) or ORGBAL
    - Filter PERIOD IN ('THIS MONTH','THIS YEAR')
    """
    rows = df.to_dicts()
    out  = []

    for r in rows:
        paidoff  = str(r.get("PAIDOFF")  or "").strip()
        paidind  = str(r.get("PAIDIND")  or "").strip()
        loantype = r.get("LOANTYPE")
        lsttrncd = r.get("LSTTRNCD")

        if not paidoff:
            continue
        if paidind != "P":
            continue
        if loantype is not None and int(loantype) in HPD:
            continue
        if lsttrncd is not None and int(lsttrncd) == 652:
            continue

        # BRANCH resolution
        branch = r.get("PENDBRH")
        if not branch or branch == 0:
            branch = r.get("NTBRCH")
        if not branch or branch == 0:
            branch = r.get("ACCBRCH")
        r["BRANCH"] = branch

        # PERIOD derivation from LASTTRAN (numeric MMDDYYYYHHMM or similar)
        lasttran = r.get("LASTTRAN")
        period   = None
        if lasttran is not None and lasttran > 0:
            lasttran_str = str(int(lasttran)).zfill(11)
            procmont = lasttran_str[0:2]
            procyear = lasttran_str[4:8]
            if procyear == REPTYEAR and procmont == REPTMON:
                period = "THIS MONTH"
            elif procyear == REPTYEAR:
                period = "THIS YEAR"
            else:
                period = "UNDEFINED"

        r["PERIOD"] = period

        if period not in ("THIS MONTH", "THIS YEAR"):
            continue

        # LOANAMT derivation
        lt = int(loantype) if loantype is not None else 0
        if 100 <= lt <= 200:
            r["LOANAMT"] = float(r.get("NETPROC") or 0)
        else:
            r["LOANAMT"] = float(r.get("ORGBAL") or 0)

        out.append(r)

    return pl.DataFrame(out) if out else pl.DataFrame()


lnnote = process_lnnote(lnnote_raw)

# ============================================================================
# STEP 4: MERGE LNNOTE WITH LNCOMM (COMMITMENT SEGMENT)
# ============================================================================

commit = con.execute(
    f"SELECT * FROM read_parquet('{LNCOMM_FILE}')"
).pl()

lnnote_sorted = lnnote.sort(["ACCTNO", "COMMNO"])
commit_sorted = commit.sort(["ACCTNO", "COMMNO"])

notecomm = lnnote_sorted.join(
    commit_sorted, on=["ACCTNO", "COMMNO"], how="left", suffix="_COMM"
)

# Override LOANAMT for progressive release notes (NOTENO 10000-19999, 30000-39999)
def apply_corgamt(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        noteno  = r.get("NOTENO")
        corgamt = r.get("CORGAMT")
        if noteno is not None and corgamt is not None:
            n = int(noteno)
            if (10000 <= n <= 19999) or (30000 <= n <= 39999):
                r["LOANAMT"] = float(corgamt)
        out.append(r)
    return out

lnnote_final = pl.DataFrame(apply_corgamt(notecomm.to_dicts()))

# ============================================================================
# STEP 5: LOAD BRHFILE (fixed-width: @02 BRANCH 3., @06 BRH $3.,
#                        @12 BRHNAME $25., @45 STATE $1.)
# ============================================================================

brhdata_rows = []
if BRHFILE.exists():
    with open(BRHFILE, "r", encoding="utf-8") as bf:
        for line in bf:
            # 1-based SAS positions -> 0-based Python slices
            branch_s  = line[1:4].strip()   if len(line) > 4  else ""
            brh_s     = line[5:8].strip()   if len(line) > 8  else ""
            brhname_s = line[11:36].strip() if len(line) > 36 else ""
            state_s   = line[44:45].strip() if len(line) > 45 else ""
            if branch_s:
                try:
                    brhdata_rows.append({
                        "BRANCH":  int(branch_s),
                        "BRH":     brh_s,
                        "BRHNAME": brhname_s,
                        "STATE":   state_s,
                    })
                except ValueError:
                    pass

brhdata = pl.DataFrame(brhdata_rows) if brhdata_rows else pl.DataFrame(
    {"BRANCH": pl.Series([], dtype=pl.Int64),
     "BRH":    pl.Series([], dtype=pl.Utf8),
     "BRHNAME":pl.Series([], dtype=pl.Utf8),
     "STATE":  pl.Series([], dtype=pl.Utf8)}
)

# ============================================================================
# STEP 6: MERGE LNNOTE WITH BRHDATA, EXCLUDE HEAD-OFFICE BRH CODES
# ============================================================================

lnnote_sorted2 = lnnote_final.sort("BRANCH")
brhdata_sorted = brhdata.sort("BRANCH")

loanbr = lnnote_sorted2.join(brhdata_sorted, on="BRANCH", how="left")

# Exclude specific BRH codes
loanbr = loanbr.filter(
    ~pl.col("BRH").cast(pl.Utf8).is_in(EXCLUDE_BRH)
)

# ============================================================================
# STEP 7: ASSIGN GROUP (LNPOGRP. format via PBMISFMT)
# ============================================================================

loanbr = loanbr.with_columns(
    pl.col("BRANCH").map_elements(
        lambda b: format_lnpogrp(int(b) if b is not None else None),
        return_dtype=pl.Utf8,
    ).alias("GROUP")
)

loangrp = loanbr.sort(["GROUP", "BRH", "BRANCH", "PAIDOFF", "ACCTNO"])

# ============================================================================
# STEP 8: SUMMARIES FOR REPORT 1 (BY GROUP/BRH/BRANCH/PAIDOFF)
# ============================================================================

by_cols_r1 = ["GROUP", "BRH", "BRANCH", "PAIDOFF"]

report1_base = loangrp.sort(by_cols_r1)

# Monthly summary
r1m = (
    report1_base.filter(pl.col("PERIOD") == "THIS MONTH")
    .group_by(by_cols_r1)
    .agg([
        pl.len().alias("NOACCTM"),
        pl.col("LOANAMT").sum().alias("AMOUNTM"),
        pl.col("LSTTRNAM").sum().alias("AMOUNTLM"),
    ])
)

# Year-to-date summary
r1y = (
    report1_base.filter(pl.col("PERIOD").is_in(["THIS MONTH", "THIS YEAR"]))
    .group_by(by_cols_r1)
    .agg([
        pl.len().alias("NOACCTY"),
        pl.col("LOANAMT").sum().alias("AMOUNTY"),
        pl.col("LSTTRNAM").sum().alias("AMOUNTLY"),
    ])
)

report1a = r1m.join(r1y, on=by_cols_r1, how="outer").sort(by_cols_r1)
for col in ["NOACCTM", "NOACCTY", "AMOUNTM", "AMOUNTY", "AMOUNTLM", "AMOUNTLY"]:
    if col in report1a.columns:
        report1a = report1a.with_columns(pl.col(col).fill_null(0))

# ============================================================================
# STEP 9: THIS MONTH DETAIL FOR REPORT 2
# ============================================================================

thimonth = loangrp.filter(pl.col("PERIOD") == "THIS MONTH").sort(["BRH", "NAME"])

# ============================================================================
# STEP 10: SUMMARIES FOR REPORT 3 (BY PAIDOFF/GROUP/BRH/BRANCH)
# ============================================================================

by_cols_r3 = ["PAIDOFF", "GROUP", "BRH", "BRANCH"]

report3_base = loangrp.sort(by_cols_r3)

r3m = (
    report3_base.filter(pl.col("PERIOD") == "THIS MONTH")
    .group_by(by_cols_r3)
    .agg([
        pl.len().alias("NOACCTM"),
        pl.col("LOANAMT").sum().alias("AMOUNTM"),
        pl.col("LSTTRNAM").sum().alias("AMOUNTLM"),
    ])
)

r3y = (
    report3_base.filter(pl.col("PERIOD").is_in(["THIS MONTH", "THIS YEAR"]))
    .group_by(by_cols_r3)
    .agg([
        pl.len().alias("NOACCTY"),
        pl.col("LOANAMT").sum().alias("AMOUNTY"),
        pl.col("LSTTRNAM").sum().alias("AMOUNTLY"),
    ])
)

report3a = r3m.join(r3y, on=by_cols_r3, how="outer").sort(by_cols_r3)
for col in ["NOACCTM", "NOACCTY", "AMOUNTM", "AMOUNTY", "AMOUNTLM", "AMOUNTLY"]:
    if col in report3a.columns:
        report3a = report3a.with_columns(pl.col(col).fill_null(0))

# ============================================================================
# STEP 11: REPORT RENDERING HELPERS
# ============================================================================

DASH_129  = "-" * 129
DASH_124  = "-" * 124
DASH_118  = "-" * 118
DASH_108  = "-" * 108
DASH_108E = "=" * 108
STAR_119  = "*" * 119
STAR_120  = "*" * 120
STAR_83   = "*" * 83
STAR_48   = "*" * 48


def report_header(lines: list, title1: str, title2: str,
                  title3: str, title4: str) -> None:
    """Emit ASA '1' form-feed page header."""
    lines.append(f"1{title1.center(LINE_WIDTH)}")
    lines.append(f" {title2.center(LINE_WIDTH)}")
    lines.append(f" {title3.center(LINE_WIDTH)}")
    lines.append(f" {title4.center(LINE_WIDTH)}")
    lines.append(f" ")


def col_header_r1r3() -> str:
    return (
        f"  {'REGION':<33}  {'BRH':<6}  {'BR':>6}  {'REASONS':>4}  "
        f"{'CURR MONTH COUNT':>5}  {'YTD COUNT':>5}  "
        f"{'APPRV LMT CUR MO':>13}  {'APPRV LMT YTD':>13}  "
        f"{'AMT PAIDOFF CUR':>13}  {'AMT PAIDOFF YTD':>13}"
    )


def fmt_r1r3_row(group, brh, branch, paidoff,
                 noacctm, noaccty, amountm, amounty,
                 amountlm, amountly) -> str:
    return (
        f"  {str(group or ''):<33}  {str(brh or ''):<6}  "
        f"{str(branch or ''):>6}  {str(paidoff or ''):>4}  "
        f"{fmt_comma5(noacctm)}  {fmt_comma5(noaccty)}  "
        f"{fmt_comma13_2(amountm)}  {fmt_comma13_2(amounty)}  "
        f"{fmt_comma13_2(amountlm)}  {fmt_comma13_2(amountly)}"
    )


# ============================================================================
# STEP 12: REPORT 1 - BY REGION / BRANCH / REASONS
# ============================================================================

report_lines: list[str] = []

report_header(
    report_lines,
    title1="REPORT NAME : EIBMLNPO - 1",
    title2=SDESC.strip(),
    title3="MONTHLY REPORT FOR LOAN PAIDOFF",
    title4=f"BY REGION BY BRANCH BY REASONS AS AT : {RDATE}",
)

report_lines.append(f" {col_header_r1r3()}")
report_lines.append(f" {DASH_129}")

line_count = 6

# Iterate GROUP -> BRH -> BRANCH -> PAIDOFF
for group_val in sorted(report1a["GROUP"].unique().to_list()):
    grp = report1a.filter(pl.col("GROUP") == group_val)
    g_noacctm = g_noaccty = g_amountm = g_amounty = g_amountlm = g_amountly = 0.0

    for brh_val in sorted(grp["BRH"].unique().to_list()):
        brh_grp = grp.filter(pl.col("BRH") == brh_val)
        b_noacctm = b_noaccty = b_amountm = b_amounty = b_amountlm = b_amountly = 0.0

        for row in brh_grp.sort(["BRANCH", "PAIDOFF"]).iter_rows(named=True):
            if line_count >= PAGE_LENGTH - 4:
                report_header(
                    report_lines,
                    title1="REPORT NAME : EIBMLNPO - 1",
                    title2=SDESC.strip(),
                    title3="MONTHLY REPORT FOR LOAN PAIDOFF",
                    title4=f"BY REGION BY BRANCH BY REASONS AS AT : {RDATE}",
                )
                report_lines.append(f" {col_header_r1r3()}")
                report_lines.append(f" {DASH_129}")
                line_count = 6

            report_lines.append(f" {fmt_r1r3_row(row['GROUP'], row['BRH'], row['BRANCH'], row['PAIDOFF'], row['NOACCTM'], row['NOACCTY'], row['AMOUNTM'], row['AMOUNTY'], row['AMOUNTLM'], row['AMOUNTLY'])}")
            line_count += 1

            b_noacctm  += float(row["NOACCTM"]  or 0)
            b_noaccty  += float(row["NOACCTY"]  or 0)
            b_amountm  += float(row["AMOUNTM"]  or 0)
            b_amounty  += float(row["AMOUNTY"]  or 0)
            b_amountlm += float(row["AMOUNTLM"] or 0)
            b_amountly += float(row["AMOUNTLY"] or 0)

        # BREAK AFTER BRH subtotal
        report_lines.append(f" ")
        report_lines.append(f"   {DASH_129}")
        report_lines.append(
            f"                 SUB-TOTAL FOR THIS BRANCH : -- >    {brh_val:<3}"
            f"  {fmt_comma5(b_noacctm)}  {fmt_comma5(b_noaccty)}"
            f"  {fmt_comma13_2(b_amountm)}  {fmt_comma13_2(b_amounty)}"
            f"  {fmt_comma13_2(b_amountlm)}  {fmt_comma13_2(b_amountly)}"
        )
        report_lines.append(f"   {DASH_129}")
        report_lines.append(f" ")
        line_count += 5

        g_noacctm  += b_noacctm
        g_noaccty  += b_noaccty
        g_amountm  += b_amountm
        g_amounty  += b_amounty
        g_amountlm += b_amountlm
        g_amountly += b_amountly

    # BREAK AFTER GROUP total
    report_lines.append(f" ")
    report_lines.append(f"   {DASH_129}")
    report_lines.append(
        f"                 TOTAL ---> {str(group_val):<33}"
        f"  {fmt_comma5(g_noacctm)}  {fmt_comma5(g_noaccty)}"
        f"  {fmt_comma13_2(g_amountm)}  {fmt_comma13_2(g_amounty)}"
        f"  {fmt_comma13_2(g_amountlm)}  {fmt_comma13_2(g_amountly)}"
    )
    report_lines.append(f"   {DASH_129}")
    report_lines.append(f" ")
    line_count += 5

# RBREAK AFTER grand total
gt_noacctm  = float(report1a["NOACCTM"].sum()  or 0)
gt_noaccty  = float(report1a["NOACCTY"].sum()  or 0)
gt_amountm  = float(report1a["AMOUNTM"].sum()  or 0)
gt_amounty  = float(report1a["AMOUNTY"].sum()  or 0)
gt_amountlm = float(report1a["AMOUNTLM"].sum() or 0)
gt_amountly = float(report1a["AMOUNTLY"].sum() or 0)

report_lines.append(f" ")
report_lines.append(f" ")
report_lines.append(f"             {STAR_119}")
report_lines.append(
    f"                 G R A N D   T O T A L  --->  "
    f"  {fmt_comma5(gt_noacctm)}  {fmt_comma5(gt_noaccty)}"
    f"  {fmt_comma13_2(gt_amountm)}  {fmt_comma13_2(gt_amounty)}"
    f"  {fmt_comma13_2(gt_amountlm)}  {fmt_comma13_2(gt_amountly)}"
)
report_lines.append(f"             {STAR_119}")
report_lines.append(f" ")

# ============================================================================
# STEP 13: REPORT 2 - LIST OF CUSTOMERS WITH LOAN PAIDOFF BY BRANCH
# ============================================================================

report_header(
    report_lines,
    title1="REPORT NAME : EIBMLNPO - 2",
    title2=SDESC.strip(),
    title3="LIST OF CUSTOMER WITH LOAN PAIDOFF",
    title4=f"BY BRANCH BY CUSTOMER FOR THE MONTH OF AT : {RDATE}",
)

col_header_r2 = (
    f"  {'BRH':<6}  {'BRANCH':>6}  {'NAME OF CUSTOMER':<24}  "
    f"{'ACCOUNT NO':>10}  {'NOTE NO':>7}  {'LOAN TYPE':>4}  "
    f"{'APPROVED LIMIT':>13}  {'AMT PAIDOFF':>13}  {'REASONS':>8}"
)
report_lines.append(f" {col_header_r2}")
report_lines.append(f" {DASH_108}")
line_count = 6

for brh_val in sorted(thimonth["BRH"].unique().to_list()):
    brh_grp = thimonth.filter(pl.col("BRH") == brh_val).sort("NAME")
    b_loanamt  = 0.0
    b_lsttrnam = 0.0

    for row in brh_grp.iter_rows(named=True):
        if line_count >= PAGE_LENGTH - 4:
            report_header(
                report_lines,
                title1="REPORT NAME : EIBMLNPO - 2",
                title2=SDESC.strip(),
                title3="LIST OF CUSTOMER WITH LOAN PAIDOFF",
                title4=f"BY BRANCH BY CUSTOMER FOR THE MONTH OF AT : {RDATE}",
            )
            report_lines.append(f" {col_header_r2}")
            report_lines.append(f" {DASH_108}")
            line_count = 6

        report_lines.append(
            f"  {str(row.get('BRH') or ''):<6}  "
            f"{str(row.get('BRANCH') or ''):>6}  "
            f"{str(row.get('NAME') or '')[:24]:<24}  "
            f"{str(row.get('ACCTNO') or ''):>10}  "
            f"{str(row.get('NOTENO') or ''):>7}  "
            f"{str(row.get('LOANTYPE') or ''):>4}  "
            f"{fmt_comma13_2(row.get('LOANAMT'))}  "
            f"{fmt_comma13_2(row.get('LSTTRNAM'))}  "
            f"{str(row.get('PAIDOFF') or ''):>8}"
        )
        line_count += 1
        b_loanamt  += float(row.get("LOANAMT")  or 0)
        b_lsttrnam += float(row.get("LSTTRNAM") or 0)

    # BREAK AFTER BRH
    report_lines.append(f" ")
    report_lines.append(f"   {DASH_108}")
    report_lines.append(
        f"   {brh_val:<3}"
        f"                 <-------- BRANCH TOTAL -------->  "
        f"{fmt_comma13_2(b_loanamt)}  "
        f"{fmt_comma13_2(b_lsttrnam)}"
    )
    report_lines.append(f"   {DASH_108}")
    report_lines.append(f" ")
    line_count += 5

# RBREAK AFTER grand total + reason legend
gt2_loanamt  = float(thimonth["LOANAMT"].sum()  or 0)
gt2_lsttrnam = float(thimonth["LSTTRNAM"].sum() or 0)

report_lines.append(f" ")
report_lines.append(f" ")
report_lines.append(f"                       {STAR_83}")
report_lines.append(
    f"                        G R A N D  T O T A L ---->  "
    f"{fmt_comma13_2(gt2_loanamt)}  "
    f"{fmt_comma13_2(gt2_lsttrnam)}"
)
report_lines.append(f"                       {STAR_83}")
report_lines.append(f" ")
report_lines.append(f" ")
report_lines.append(f"   {DASH_108E}")
report_lines.append(f"                    PLEASE NOTE THE FOLLOWING REASONS FOR PAID OFF : ")
report_lines.append(f"                               1 = REFINANCING FROM OTHER FINANCIAL INSTITUTIONS")
report_lines.append(f"                               2 = PROPERTY SOLD")
report_lines.append(f"                               3 = FINAL PAYMENT/INSTALMENT")
report_lines.append(f"                               4 = EPF WITHDRAWAL")
report_lines.append(f"                               5 = MIGRATION OF ACCOUNTS UNDER PROGRESIVE RELEASE")
report_lines.append(f"                               6 = PAIDOFF FROM EXCESS FUNDS")
report_lines.append(f"                               7 = REFINANCING FROM BANK RAKYAT")
report_lines.append(f"                               8 = REFINANCING FROM BSN")
report_lines.append(f"                               9 = REFINANCING FROM MBSB")
report_lines.append(f"                              10 = REFINANCING FROM BANK ISLAM")
report_lines.append(f"                              99 = FOR OTHERS")
report_lines.append(f"               {STAR_48}")
report_lines.append(f" ")

# ============================================================================
# STEP 14: REPORT 3 - BY REASONS / REGIONS / BRANCH
# ============================================================================

report_header(
    report_lines,
    title1="REPORT NAME : EIBMLNPO - 3",
    title2=SDESC.strip(),
    title3="MONTHLY REPORT FOR LOAN PAID OFF",
    title4=f"BY REASONS BY REGIONS BY BRANCH AS AT : {RDATE}",
)

col_header_r3 = (
    f"  {'REASONS':>4}  {'REGION':<33}  {'BRH':<6}  {'BR':>6}  "
    f"{'CURR MONTH COUNT':>5}  {'YTD COUNT':>5}  "
    f"{'APPRV LMT CUR MO':>13}  {'APPRV LMT YTD':>13}  "
    f"{'AMT PAIDOFF CUR':>13}  {'AMT PAIDOFF YTD':>13}"
)
report_lines.append(f" {col_header_r3}")
report_lines.append(f" {DASH_124}")
line_count = 6


def fmt_r3_row(paidoff, group, brh, branch,
               noacctm, noaccty, amountm, amounty,
               amountlm, amountly) -> str:
    return (
        f"  {str(paidoff or ''):>4}  {str(group or ''):<33}  "
        f"{str(brh or ''):<6}  {str(branch or ''):>6}  "
        f"{fmt_comma5(noacctm)}  {fmt_comma5(noaccty)}  "
        f"{fmt_comma13_2(amountm)}  {fmt_comma13_2(amounty)}  "
        f"{fmt_comma13_2(amountlm)}  {fmt_comma13_2(amountly)}"
    )


for paid_val in sorted(report3a["PAIDOFF"].unique().to_list()):
    paid_grp = report3a.filter(pl.col("PAIDOFF") == paid_val)
    p_noacctm = p_noaccty = p_amountm = p_amounty = p_amountlm = p_amountly = 0.0

    for group_val in sorted(paid_grp["GROUP"].unique().to_list()):
        grp = paid_grp.filter(pl.col("GROUP") == group_val)
        g_noacctm = g_noaccty = g_amountm = g_amounty = g_amountlm = g_amountly = 0.0

        for row in grp.sort(["BRH", "BRANCH"]).iter_rows(named=True):
            if line_count >= PAGE_LENGTH - 4:
                report_header(
                    report_lines,
                    title1="REPORT NAME : EIBMLNPO - 3",
                    title2=SDESC.strip(),
                    title3="MONTHLY REPORT FOR LOAN PAID OFF",
                    title4=f"BY REASONS BY REGIONS BY BRANCH AS AT : {RDATE}",
                )
                report_lines.append(f" {col_header_r3}")
                report_lines.append(f" {DASH_124}")
                line_count = 6

            report_lines.append(f" {fmt_r3_row(row['PAIDOFF'], row['GROUP'], row['BRH'], row['BRANCH'], row['NOACCTM'], row['NOACCTY'], row['AMOUNTM'], row['AMOUNTY'], row['AMOUNTLM'], row['AMOUNTLY'])}")
            line_count += 1

            g_noacctm  += float(row["NOACCTM"]  or 0)
            g_noaccty  += float(row["NOACCTY"]  or 0)
            g_amountm  += float(row["AMOUNTM"]  or 0)
            g_amounty  += float(row["AMOUNTY"]  or 0)
            g_amountlm += float(row["AMOUNTLM"] or 0)
            g_amountly += float(row["AMOUNTLY"] or 0)

        # BREAK AFTER GROUP sub-total within PAIDOFF
        report_lines.append(f"               {DASH_118}")
        report_lines.append(
            f"               SUB-TOTAL FOR REGION  "
            f"  {fmt_comma5(g_noacctm)}  {fmt_comma5(g_noaccty)}"
            f"  {fmt_comma13_2(g_amountm)}  {fmt_comma13_2(g_amounty)}"
            f"  {fmt_comma13_2(g_amountlm)}  {fmt_comma13_2(g_amountly)}"
        )
        report_lines.append(f"               {DASH_118}")
        report_lines.append(f" ")
        line_count += 4

        p_noacctm  += g_noacctm
        p_noaccty  += g_noaccty
        p_amountm  += g_amountm
        p_amounty  += g_amounty
        p_amountlm += g_amountlm
        p_amountly += g_amountly

    # BREAK AFTER PAIDOFF total
    reason_label = get_reason_label(paid_val)
    report_lines.append(f" ")
    report_lines.append(f"        {DASH_124}")
    report_lines.append(
        f"               TOTAL ---> {reason_label:<25}"
        f"  {fmt_comma5(p_noacctm)}  {fmt_comma5(p_noaccty)}"
        f"  {fmt_comma13_2(p_amountm)}  {fmt_comma13_2(p_amounty)}"
        f"  {fmt_comma13_2(p_amountlm)}  {fmt_comma13_2(p_amountly)}"
    )
    report_lines.append(f"        {DASH_124}")
    report_lines.append(f" ")
    line_count += 5

# RBREAK AFTER grand total
gt3_noacctm  = float(report3a["NOACCTM"].sum()  or 0)
gt3_noaccty  = float(report3a["NOACCTY"].sum()  or 0)
gt3_amountm  = float(report3a["AMOUNTM"].sum()  or 0)
gt3_amounty  = float(report3a["AMOUNTY"].sum()  or 0)
gt3_amountlm = float(report3a["AMOUNTLM"].sum() or 0)
gt3_amountly = float(report3a["AMOUNTLY"].sum() or 0)

report_lines.append(f" ")
report_lines.append(f" ")
report_lines.append(f"            {STAR_120}")
report_lines.append(
    f"               G R A N D  T O T A L -->  "
    f"  {fmt_comma5(gt3_noacctm)}  {fmt_comma5(gt3_noaccty)}"
    f"  {fmt_comma13_2(gt3_amountm)}  {fmt_comma13_2(gt3_amounty)}"
    f"  {fmt_comma13_2(gt3_amountlm)}  {fmt_comma13_2(gt3_amountly)}"
)
report_lines.append(f"            {STAR_120}")
report_lines.append(f" ")

# ============================================================================
# WRITE REPORT
# ============================================================================

with open(OUTPUT_REPORT, "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines) + "\n")

print(f"Report written to: {OUTPUT_REPORT}")
