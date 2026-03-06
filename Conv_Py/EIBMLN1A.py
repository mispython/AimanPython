#!/usr/bin/env python3
"""
Program  : EIBMLN1A.py
Date     : 30.11.01 (SMR A295)
Purpose  : Undrawn Term Loan / Overdraft / Loan Others by Collaterals
Notes    : EXCLUDE - VOSTRO 104,105 ('33110')
           N/A     - NORMAL HL SOLD TO CAGAMAS 225,226 ('54120')
           OTHER LOANS - RC 350,910,925 ('34190')
                       - STAFF LOANS 4-33 ('34230')
"""

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR      = Path("/data")
BNM_DIR       = BASE_DIR / "bnm"
BNM1_DIR      = BASE_DIR / "bnm1"

REPTDATE_FILE = BNM_DIR  / "reptdate.parquet"
SDESC_FILE    = BNM_DIR  / "sdesc.parquet"

OUTPUT_DIR    = BASE_DIR / "output"
OUTPUT_REPORT = OUTPUT_DIR / "eibmln1a_report.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# REPORT / PAGE CONSTANTS
# ============================================================================

PAGE_LENGTH = 60
LINE_WIDTH  = 132

TBL1 = "UNDRAWN OVERDRAFT FACILITIES"
TBL2 = "UNDRAWN LOAN OTHERS"
TBL3 = "UNDRAWN TERM LOANS"

# ============================================================================
# FORMAT: TYPFMT (collateral type -> description)
# ============================================================================

TYPFMT: dict[int, str] = {
    1: "PBB OWN FIXED DEPOSIT, 30308 (0%)",
    2: "DISCOUNT HOUSE/CAGAMAS, 30314 (10%)",
    3: "FIN INST FIXED DEPOSIT/GUARANTEE, 30332 (20%)",
    4: "STATUTORY BODIES, 30336 (20%)",
    5: "FIRST CHARGE, 30342 (50%)",
    6: "SHARES/UNIT TRUSTS, 30360 (100%)",
    7: "OTHERS, 30360 (100%)",
}


def fmt_typfmt(val) -> str:
    if val is None:
        return ""
    try:
        return TYPFMT.get(int(val), "")
    except (TypeError, ValueError):
        return ""


# ============================================================================
# %MACRO COLL: LIABCODE -> TYP
# ============================================================================

def assign_typ(liabcode) -> int:
    """Replicate %MACRO COLL SELECT(LIABCODE) logic."""
    lc = str(liabcode).strip() if liabcode is not None else ""
    if lc in ("07", "12", "13", "14", "21", "24", "48", "49"):
        return 1
    if lc in ("17", "26", "29"):
        return 2
    if lc in ("06", "11", "16", "30", "18", "27", "03"):
        return 3
    if lc == "25":
        return 4
    if lc == "50":
        return 5
    if lc in ("15", "08", "42"):
        return 6
    return 7


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def fmt_ddmmyy8(d: date) -> str:
    """Format date as DD/MM/YY (SAS DDMMYY8.)"""
    return d.strftime("%d/%m/%y")


def fmt_z2(n: int) -> str:
    return f"{n:02d}"


def fmt_13_2(val) -> str:
    """Format as 13.2 fixed-width."""
    if val is None:
        return "0.00".rjust(13)
    try:
        return f"{float(val):.2f}".rjust(13)
    except (TypeError, ValueError):
        return "0.00".rjust(13)


def fmt_comma17_2(val) -> str:
    if val is None:
        return " " * 17
    try:
        return f"{float(val):,.2f}".rjust(17)
    except (TypeError, ValueError):
        return " " * 17


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
    NOWK = "1"
elif day == 15:
    NOWK = "2"
elif day == 22:
    NOWK = "3"
else:
    NOWK = "4"

REPTMON = fmt_z2(REPTDATE.month)
RDATE   = fmt_ddmmyy8(REPTDATE)

# ============================================================================
# STEP 2: READ SDESC
# ============================================================================

sdesc_df = con.execute(
    f"SELECT * FROM read_parquet('{SDESC_FILE}')"
).pl()
SDESC = str(sdesc_df["SDESC"][0]).ljust(26)[:26]

# ============================================================================
# STEP 3: LOAD LOAN AND LNCOMM DATA
# ============================================================================

loan_file  = BNM_DIR  / f"loan{REPTMON}{NOWK}.parquet"
uloan_file = BNM_DIR  / f"uloan{REPTMON}{NOWK}.parquet"
lncomm_file = BNM1_DIR / "lncomm.parquet"

alw_query = f"""
SELECT * FROM read_parquet('{loan_file}')
ORDER BY ACCTNO, NOTENO
"""
alw = con.execute(alw_query).pl()

lncomm_query = f"""
SELECT * FROM read_parquet('{lncomm_file}')
ORDER BY ACCTNO, COMMNO
"""
lncomm = con.execute(lncomm_query).pl()

# ============================================================================
# STEP 4: RC DEDUPLICATION LOGIC FOR COMMNO > 0  (APPR)
# ============================================================================

# ALWCOM: COMMNO > 0, sorted by ACCTNO, COMMNO
alwcom = alw.filter(pl.col("COMMNO") > 0).sort(["ACCTNO", "COMMNO"])

# Merge ALWCOM with LNCOMM
appr = alwcom.join(lncomm, on=["ACCTNO", "COMMNO"], how="left", suffix="_LN")

# IF A & PRODCD='34190': keep FIRST.ACCTNO or FIRST.COMMNO rows
# For non-34190: keep all
def filter_appr(df: pl.DataFrame) -> pl.DataFrame:
    rows = df.to_dicts()
    out  = []
    seen_acct  = {}
    seen_comm  = {}
    for r in rows:
        prodcd  = str(r.get("PRODCD") or "").strip()
        acctno  = r.get("ACCTNO")
        commno  = r.get("COMMNO")
        key_ac  = (acctno, commno)
        if prodcd == "34190":
            if acctno not in seen_acct or key_ac not in seen_comm:
                seen_acct[acctno] = True
                seen_comm[key_ac] = True
                out.append(r)
        else:
            out.append(r)
    return pl.DataFrame(out) if out else pl.DataFrame()

appr = filter_appr(appr)

# ============================================================================
# STEP 5: RC DEDUPLICATION LOGIC FOR COMMNO <= 0  (APPR1)
# ============================================================================

# ALWNOCOM: COMMNO <= 0, sorted by ACCTNO, APPRLIM2
alwnocom = alw.filter(pl.col("COMMNO") <= 0).sort(["ACCTNO", "APPRLIM2"])

def split_appr1_dup(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Split into APPR1 (first per ACCTNO/APPRLIM2 for 34190, else all)
    and DUP (duplicates for 34190).
    """
    rows   = df.to_dicts()
    appr1  = []
    dup    = []
    seen   = {}
    for r in rows:
        prodcd   = str(r.get("PRODCD")   or "").strip()
        acctno   = r.get("ACCTNO")
        apprlim2 = r.get("APPRLIM2")
        key      = (acctno, apprlim2)
        if prodcd == "34190":
            if key not in seen:
                seen[key] = True
                appr1.append(r)
            else:
                r["DUPLI"] = 1
                dup.append(r)
        # Non-34190 not appended here — only 34190 goes through this split
    return (
        pl.DataFrame(appr1) if appr1 else pl.DataFrame(),
        pl.DataFrame(dup)   if dup   else pl.DataFrame(),
    )

appr1_df, dup_df = split_appr1_dup(alwnocom)

# DUPLI: filter WHERE BALANCE >= APPRLIM2, keep ACCTNO, APPRLIM2, DUPLI
if not dup_df.is_empty():
    dupli = (
        dup_df.filter(pl.col("BALANCE") >= pl.col("APPRLIM2"))
              .sort(["ACCTNO", "APPRLIM2"])
              .select(["ACCTNO", "APPRLIM2", "DUPLI"])
    )
else:
    dupli = pl.DataFrame({"ACCTNO": [], "APPRLIM2": [], "DUPLI": []})

# Re-merge ALWNOCOM with DUPLI to rebuild APPR1 with correct dedup
appr1_final = alwnocom.join(
    dupli, on=["ACCTNO", "APPRLIM2"], how="left", suffix="_DUP"
)

def filter_appr1_final(df: pl.DataFrame) -> pl.DataFrame:
    rows = df.to_dicts()
    out  = []
    seen = {}
    for r in rows:
        prodcd   = str(r.get("PRODCD")   or "").strip()
        acctno   = r.get("ACCTNO")
        apprlim2 = r.get("APPRLIM2")
        dupli    = r.get("DUPLI")
        balance  = float(r.get("BALANCE") or 0)
        key      = (acctno, apprlim2)

        if prodcd == "34190":
            if dupli == 1:
                if balance >= (apprlim2 or 0):
                    out.append(r)
            else:
                if key not in seen:
                    seen[key] = True
                    out.append(r)
        else:
            out.append(r)
    return pl.DataFrame(out) if out else pl.DataFrame()

appr1_final = filter_appr1_final(appr1_final)

# ============================================================================
# STEP 6: COMBINE APPR + APPR1, APPLY LIABCODE AND %COLL MACRO
# ============================================================================

loan_combined = pl.concat([appr, appr1_final], how="diagonal").sort("ACCTNO")

keep_cols = [
    "ACCTNO", "NOTENO", "CUSTCODE", "PRODUCT", "ACCTYPE", "EXPRDATE", "ORIGMT",
    "PRODCD", "BRANCH", "CURBAL", "INTAMT", "NETPROC", "LIABCODE", "TYP",
    "LOANSTAT", "APPVALUE", "FEEAMT", "BALANCE", "CLOSEDTE", "APPRLIMT",
    "ISSDTE", "RLEASAMT", "APPRLIM2", "COL1", "AMTIND", "UNDRAWN",
    "COL2", "SECTORCD",
]

def apply_liabcode_typ(rows: list[dict]) -> list[dict]:
    """Apply OD LIABCODE derivation from COL1/COL2, then assign TYP via %COLL."""
    out = []
    for r in rows:
        acctype = str(r.get("ACCTYPE") or "")
        col1    = str(r.get("COL1")    or "")
        col2    = str(r.get("COL2")    or "")

        if acctype == "OD":
            liab1 = col1[0:2] if len(col1) >= 2 else ""
            liab2 = col1[3:5] if len(col1) >= 5 else ""
            liab3 = col2[0:2] if len(col2) >= 2 else ""
            liab4 = col2[3:5] if len(col2) >= 5 else ""
            if liab1.strip():
                liabcode = liab1
            elif liab2.strip():
                liabcode = liab2
            elif liab3.strip():
                liabcode = liab3
            elif liab4.strip():
                liabcode = liab4
            else:
                liabcode = " "
            r["LIABCODE"] = liabcode

        r["TYP"] = assign_typ(r.get("LIABCODE"))

        # Keep only required columns
        out.append({k: r.get(k) for k in keep_cols if k in r or k == "TYP"})
    return out

loan_rows = apply_liabcode_typ(loan_combined.to_dicts())
loan_df = pl.DataFrame(loan_rows)

# ============================================================================
# STEP 7: LOAD ULOAN, MERGE WITH LNCOMM (CCOLLTRL -> LIABCODE), APPLY %COLL
# ============================================================================

uloan_query = f"""
SELECT * FROM read_parquet('{uloan_file}')
ORDER BY ACCTNO, COMMNO
"""
uloan_raw = con.execute(uloan_query).pl()

mrguloan = uloan_raw.join(lncomm, on=["ACCTNO", "COMMNO"], how="left", suffix="_LN")

# Rename CCOLLTRL -> LIABCODE
if "CCOLLTRL" in mrguloan.columns:
    mrguloan = mrguloan.rename({"CCOLLTRL": "LIABCODE"})

uloan_rows = []
for r in mrguloan.to_dicts():
    r["TYP"] = assign_typ(r.get("LIABCODE"))
    uloan_rows.append(r)

uloan_df = pl.DataFrame(uloan_rows) if uloan_rows else pl.DataFrame()

# ============================================================================
# STEP 8: COMBINE LOAN + ULOAN, CLASSIFY INTO LOAN1/LOAN2/LOAN3
# ============================================================================

# Re-read REPTDATE for IF _N_=1 THEN SET BNM.REPTDATE pattern
combined = pl.concat([loan_df, uloan_df], how="diagonal").sort("ACCTNO")

loan1_rows = []  # OD accounts
loan2_rows = []  # Loan Others (34190, 34230, 54120)
loan3_rows = []  # Term Loans (everything else)

for r in combined.to_dicts():
    acctype = str(r.get("ACCTYPE") or "")
    prodcd  = str(r.get("PRODCD")  or "").strip()
    origmt  = str(r.get("ORIGMT")  or "")

    # Filter: PRODCD prefix '34' OR PRODCD='54120'
    if not (prodcd[:2] == "34" or prodcd == "54120"):
        continue

    # REMMTH determination
    if acctype == "OD":
        remmth = 1
    else:
        # IF ORIGMT < '20' THEN REMMTH = 1; ELSE REMMTH = 13
        remmth = 1 if origmt < "20" else 13

    r["REMMTH"] = remmth

    keep = {
        "BRANCH":   r.get("BRANCH"),
        "TYP":      r.get("TYP"),
        "REMMTH":   remmth,
        "UNDRAWN":  r.get("UNDRAWN"),
        "AMTIND":   r.get("AMTIND"),
        "ACCTNO":   r.get("ACCTNO"),
        "LIABCODE": r.get("LIABCODE"),
    }

    if acctype == "OD":
        loan1_rows.append(keep)
    elif prodcd in ("34190", "34230", "54120"):
        loan2_rows.append(keep)
    else:
        loan3_rows.append(keep)

loan1 = pl.DataFrame(loan1_rows) if loan1_rows else pl.DataFrame(
    {"BRANCH": [], "TYP": [], "REMMTH": [], "UNDRAWN": [],
     "AMTIND": [], "ACCTNO": [], "LIABCODE": []}
)
loan2 = pl.DataFrame(loan2_rows) if loan2_rows else loan1.clear()
loan3 = pl.DataFrame(loan3_rows) if loan3_rows else loan1.clear()

datasets = {1: loan1, 2: loan2, 3: loan3}
tbl_labels = {1: TBL1, 2: TBL2, 3: TBL3}

# ============================================================================
# STEP 9: REPORT RENDERING HELPERS
# ============================================================================

def fmt_branch_col_header(typ_vals: list[int]) -> tuple[str, str]:
    """Build column headers for TYP-based tabulate."""
    h1 = f"{'BRANCH':<8}"
    h2 = f"{'':8}"
    sep = "-" * LINE_WIDTH
    for t in typ_vals:
        label = fmt_typfmt(t)[:20]
        h1 += f"  {label:>13}"
        h2 += f"  {'UNDRAWN':>13}"
    h1 += f"  {'TOTAL':>13}"
    h2 += f"  {'UNDRAWN':>13}"
    return h1, sep


def new_page(lines: list, titles: list[str], col_header: str,
             sep: str, line_count_ref: list) -> None:
    lines.append(f"1{'REPORT ID: EIBMLNA1'.center(LINE_WIDTH)}")
    for t in titles:
        lines.append(f" {t.center(LINE_WIDTH)}")
    lines.append(f" ")
    lines.append(f" {col_header}")
    lines.append(f" {sep}")
    line_count_ref[0] = 4 + len(titles)


def render_tabulate_typ(
    df: pl.DataFrame,
    titles: list[str],
    lines: list[str],
    remmth_filter=None,
    amtind_filter=None,
) -> None:
    """
    Replicate PROC TABULATE FORMAT=13.2:
    TABLE (BRANCH ALL='TOTAL'), (TYP ALL='TOTAL') * UNDRAWN * SUM
    """
    subset = df.clone()

    if remmth_filter is not None:
        if remmth_filter == "ne1":
            subset = subset.filter(pl.col("REMMTH") != 1)
        elif remmth_filter == "eq1":
            subset = subset.filter(pl.col("REMMTH") == 1)

    if amtind_filter is not None:
        subset = subset.filter(pl.col("AMTIND") == amtind_filter)

    if subset.is_empty():
        return

    typ_vals = sorted(subset["TYP"].drop_nulls().unique().to_list())
    branch_vals = sorted(subset["BRANCH"].drop_nulls().unique().to_list())

    col_header, sep = fmt_branch_col_header(typ_vals)
    line_count_ref  = [PAGE_LENGTH]  # force new page

    # Build pivot: branch x typ -> undrawn sum
    pivot: dict = {}
    for r in subset.iter_rows(named=True):
        b  = r.get("BRANCH")
        t  = r.get("TYP")
        ud = float(r.get("UNDRAWN") or 0)
        pivot.setdefault(b, {}).setdefault(t, 0.0)
        pivot[b][t] += ud

    # Grand totals per TYP
    grand: dict[int, float] = {t: 0.0 for t in typ_vals}
    grand_total = 0.0

    for branch in branch_vals:
        if line_count_ref[0] >= PAGE_LENGTH - 4:
            new_page(lines, titles, col_header, sep, line_count_ref)

        row_total = 0.0
        cells     = ""
        for t in typ_vals:
            val = pivot.get(branch, {}).get(t, 0.0)
            cells += f"  {fmt_13_2(val)}"
            row_total += val
            grand[t]  += val
        cells += f"  {fmt_13_2(row_total)}"
        grand_total += row_total

        lines.append(f" {str(branch):<8}{cells}")
        line_count_ref[0] += 1

    # TOTAL row
    if line_count_ref[0] >= PAGE_LENGTH - 4:
        new_page(lines, titles, col_header, sep, line_count_ref)
    lines.append(f" {sep}")
    total_cells = ""
    for t in typ_vals:
        total_cells += f"  {fmt_13_2(grand[t])}"
    total_cells += f"  {fmt_13_2(grand_total)}"
    lines.append(f" {'TOTAL':<8}{total_cells}")
    line_count_ref[0] += 2


def render_tabulate_summary(
    df: pl.DataFrame,
    title3: str,
    title4: str,
    lines: list[str],
    amtind_filter=None,
) -> None:
    """
    Replicate PROC TABULATE FORMAT=COMMA17.2:
    TABLE (BRANCH ALL='TOTAL'), UNDRAWN * SUM='TOTAL'
    """
    subset = df.clone()
    if amtind_filter is not None:
        subset = subset.filter(pl.col("AMTIND") == amtind_filter)

    if subset.is_empty():
        return

    branch_vals = sorted(subset["BRANCH"].drop_nulls().unique().to_list())
    col_header  = f"{'BRANCH':<8}  {'TOTAL':>17}"
    sep         = "-" * LINE_WIDTH
    line_count_ref = [PAGE_LENGTH]

    title1_str = f"REPORT ID: EIBMLNA1"
    title2_str = f"{SDESC.strip()}    DATE : {RDATE}"

    def _new_page() -> None:
        lines.append(f"1{title1_str.center(LINE_WIDTH)}")
        lines.append(f" {title2_str.center(LINE_WIDTH)}")
        lines.append(f" {title3.center(LINE_WIDTH)}")
        lines.append(f" {title4.center(LINE_WIDTH)}")
        lines.append(f" ")
        lines.append(f" {col_header}")
        lines.append(f" {sep}")
        line_count_ref[0] = 7

    pivot = (
        subset.group_by("BRANCH")
              .agg(pl.col("UNDRAWN").sum())
    )
    branch_map = {r["BRANCH"]: float(r["UNDRAWN"] or 0)
                  for r in pivot.iter_rows(named=True)}
    grand = 0.0

    for branch in branch_vals:
        if line_count_ref[0] >= PAGE_LENGTH - 4:
            _new_page()
        val   = branch_map.get(branch, 0.0)
        lines.append(f" {str(branch):<8}  {fmt_comma17_2(val)}")
        line_count_ref[0] += 1
        grand += val

    if line_count_ref[0] >= PAGE_LENGTH - 4:
        _new_page()
    lines.append(f" {sep}")
    lines.append(f" {'TOTAL':<8}  {fmt_comma17_2(grand)}")
    line_count_ref[0] += 2


# ============================================================================
# STEP 10: PRODUCE ALL REPORT SECTIONS
# ============================================================================

report_lines: list[str] = []

title1_base = "REPORT ID: EIBMLNA1"
title2_base = f"{SDESC.strip()}    DATE : {RDATE}"


def build_titles(tbl_label: str, subtitle3: str, subtitle4: str) -> list[str]:
    return [title2_base, subtitle3, subtitle4]


# +------------------------------------------------------------+
# | CONVENTIONAL & ISLAMIC : ORIGINAL MATURITY > 1 YEAR        |
# +------------------------------------------------------------+
# %BNKTBL1: I=1 TO 3, REMMTH NE 1
for i in range(1, 4):
    render_tabulate_typ(
        df=datasets[i],
        titles=[
            title2_base,
            f"{tbl_labels[i]} WITH ORIGINAL MATURITY MORE THAN 1 YEAR",
            "(CONVENTIONAL + ISLAMIC)",
        ],
        lines=report_lines,
        remmth_filter="ne1",
        amtind_filter=None,
    )

# +------------------------------------------------------------+
# | ISLAMIC : ORIGINAL MATURITY > 1 YEAR                       |
# +------------------------------------------------------------+
# %ISLTBL1: I=1 TO 3, REMMTH NE 1, AMTIND='I'
for i in range(1, 4):
    render_tabulate_typ(
        df=datasets[i],
        titles=[
            title2_base,
            f"{tbl_labels[i]} WITH ORIGINAL MATURITY MORE THAN 1 YEAR",
            "(ISLAMIC)",
        ],
        lines=report_lines,
        remmth_filter="ne1",
        amtind_filter="I",
    )

# +------------------------------------------------------------+
# | CONVENTIONAL & ISLAMIC : ORIGINAL MATURITY <= 1 YEAR       |
# +------------------------------------------------------------+
# %BNKTBL2: I=2 TO 3, REMMTH EQ 1  (NOTE: only LOAN2 and LOAN3)
for i in range(2, 4):
    render_tabulate_typ(
        df=datasets[i],
        titles=[
            title2_base,
            f"{tbl_labels[i]} WITH ORIGINAL MATURITY UP TO 1 YEAR",
            "(CONVENTIONAL + ISLAMIC)",
        ],
        lines=report_lines,
        remmth_filter="eq1",
        amtind_filter=None,
    )

# +------------------------------------------------------------+
# | ISLAMIC : ORIGINAL MATURITY <= 1 YEAR                      |
# +------------------------------------------------------------+
# %ISLTBL2: I=1 TO 3, REMMTH EQ 1, AMTIND='I'
for i in range(1, 4):
    render_tabulate_typ(
        df=datasets[i],
        titles=[
            title2_base,
            f"{tbl_labels[i]} WITH ORIGINAL MATURITY UP TO 1 YEAR",
            "(ISLAMIC)",
        ],
        lines=report_lines,
        remmth_filter="eq1",
        amtind_filter="I",
    )

# +------------------------------------------------------------+
# | SUMMARY - CONVENTIONAL + ISLAMIC                           |
# +------------------------------------------------------------+
# %BNKTBL3: I=1 TO 3, no filter
for i in range(1, 4):
    render_tabulate_summary(
        df=datasets[i],
        title3=f"TOTAL {tbl_labels[i]}",
        title4="(CONVENTIONAL+ISLAMIC)",
        lines=report_lines,
        amtind_filter=None,
    )

# +------------------------------------------------------------+
# | SUMMARY - ISLAMIC                                          |
# +------------------------------------------------------------+
# %ISLTBL3: I=1 TO 3, AMTIND='I'
for i in range(1, 4):
    render_tabulate_summary(
        df=datasets[i],
        title3=f"TOTAL {tbl_labels[i]}",
        title4="(ISLAMIC)",
        lines=report_lines,
        amtind_filter="I",
    )

# ============================================================================
# WRITE REPORT
# ============================================================================

with open(OUTPUT_REPORT, "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines) + "\n")

print(f"Report written to: {OUTPUT_REPORT}")
