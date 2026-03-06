#!/usr/bin/env python3
"""
Program  : EIBMCEXT.py
Date     : 14.09.01
Purpose  : Listing of Accounts with Sector Codes 0311-0316
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

REPTDATE_FILE = BNM_DIR / "reptdate.parquet"
SDESC_FILE    = BNM_DIR / "sdesc.parquet"

OUTPUT_DIR    = BASE_DIR / "output"
OUTPUT_REPORT = OUTPUT_DIR / "eibmcext_report.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# REPORT / PAGE CONSTANTS
# ============================================================================

PAGE_LENGTH = 65   # OPTIONS PS=65
LINE_WIDTH  = 132

# %LET NPRODUCT=(500,520)  -- retained as reference constant
NPRODUCT = (500, 520)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def fmt_ddmmyy8(d: date) -> str:
    """Format date as DD/MM/YY (SAS DDMMYY8.)"""
    return d.strftime("%d/%m/%y")


def fmt_z2(n: int) -> str:
    return f"{n:02d}"


def fmt_comma15_2(val) -> str:
    if val is None:
        return " " * 15
    try:
        return f"{float(val):,.2f}".rjust(15)
    except (TypeError, ValueError):
        return " " * 15


def fmt_risk(val) -> str:
    """RISK format: numeric risk category to display label."""
    mapping = {0: "  0%", 10: " 10%", 20: " 20%", 50: " 50%", 100: "100%"}
    if val is None:
        return " " * 4
    try:
        return mapping.get(int(val), "    ")
    except (TypeError, ValueError):
        return "    "


# ============================================================================
# STEP 1: READ REPTDATE & DERIVE MACRO VARIABLES
# ============================================================================

con = duckdb.connect()

reptdate_df = con.execute(f"SELECT * FROM read_parquet('{REPTDATE_FILE}')").pl()
row = reptdate_df.row(0, named=True)

REPTDATE: date = row["REPTDATE"]
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
if WK == "1":
    MM1 = MM - 1 if MM > 1 else 12
else:
    MM1 = MM

NOWK      = WK
NOWK1     = WK1
REPTMON   = fmt_z2(MM)
REPTMON1  = fmt_z2(MM1)
REPTYEAR  = str(REPTDATE.year)
REPTDAY   = fmt_z2(day)
RDATE     = fmt_ddmmyy8(REPTDATE)

# ============================================================================
# STEP 2: READ SDESC
# ============================================================================

sdesc_df = con.execute(f"SELECT * FROM read_parquet('{SDESC_FILE}')").pl()
SDESC = str(sdesc_df["SDESC"][0]).ljust(26)[:26]

# ============================================================================
# STEP 3: LOAD LOAN DATA, SPLIT INTO OD AND LOAN DATASETS
# ============================================================================

loan_file = BNM_DIR / f"loan{REPTMON}{NOWK}.parquet"

raw_query = f"SELECT * FROM read_parquet('{loan_file}')"
raw = con.execute(raw_query).pl()

# Filter: FISSPURP IN ('0311','0312','0313','0314','0315','0316')
fisspurp_codes = {"0311", "0312", "0313", "0314", "0315", "0316"}

raw = raw.filter(pl.col("FISSPURP").cast(pl.Utf8).is_in(fisspurp_codes))


def build_liabcod1_and_risk(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Process each row to derive LIABCOD1 and RISKCAT,
    then split into OD and LOAN output datasets.
    """
    od_rows   = []
    loan_rows = []

    for r in rows:
        acctype = str(r.get("ACCTYPE") or "")
        col1    = str(r.get("COL1")    or "")

        liab1    = ""
        liab2    = ""
        liabcode = ""

        if acctype == "OD":
            liab1 = col1[0:2] if len(col1) >= 2 else col1.ljust(2)[:2]
            liab2 = col1[3:5] if len(col1) >= 5 else ""
            if liab1.strip():
                liabcode = liab1
            elif liab2.strip():
                liabcode = liab2
            else:
                liabcode = " "

        liabcod1 = "0" + (liabcode.strip() if liabcode else "")

        # RISKCAT assignment via SELECT(LIABCOD1)
        risk_0  = {"007", "012", "013", "014", "024", "048", "049", "021"}
        risk_10 = {"017", "026", "029"}
        risk_20 = {"003", "025", "006", "011", "016", "030", "018", "027"}

        if liabcod1 in risk_0:
            riskcat = 0
        elif liabcod1 in risk_10:
            riskcat = 10
        elif liabcod1 in risk_20:
            riskcat = 20
        else:
            riskcat = None

        r["LIAB1"]    = liab1
        r["LIAB2"]    = liab2
        r["LIABCOD1"] = liabcod1
        r["RISKCAT"]  = riskcat

        # Only output if RISKCAT IN (0, 10, 20)
        if riskcat not in (0, 10, 20):
            continue

        if acctype == "LN":
            loan_rows.append({
                "BRANCH":   r.get("BRANCH"),
                "ACCTNO":   r.get("ACCTNO"),
                "NAME":     r.get("NAME"),
                "NOTENO":   r.get("NOTENO"),
                "APPVALUE": r.get("APPVALUE"),
                "BALANCE":  r.get("BALANCE"),
                "FISSPURP": r.get("FISSPURP"),
                "LIABCOD1": liabcod1,
                "RISKCAT":  riskcat,
            })
        elif acctype == "OD":
            od_rows.append({
                "BRANCH":   r.get("BRANCH"),
                "ACCTNO":   r.get("ACCTNO"),
                "NAME":     r.get("NAME"),
                "APPVALUE": r.get("APPVALUE"),
                "BALANCE":  r.get("BALANCE"),
                "FISSPURP": r.get("FISSPURP"),
                "LIAB1":    liab1,
                "LIAB2":    liab2,
                "RISKCAT":  riskcat,
            })

    return od_rows, loan_rows


od_rows, loan_rows = build_liabcod1_and_risk(raw.to_dicts())

od_df   = pl.DataFrame(od_rows)   if od_rows   else pl.DataFrame()
loan_df = pl.DataFrame(loan_rows) if loan_rows else pl.DataFrame()

# ============================================================================
# STEP 4: SORT
# ============================================================================

if not od_df.is_empty():
    od_df = od_df.sort(["BRANCH", "FISSPURP", "RISKCAT"])

if not loan_df.is_empty():
    loan_df = loan_df.sort(["BRANCH", "FISSPURP"])

# ============================================================================
# STEP 5: REPORT RENDERING
# ============================================================================

report_titles = [
    "REPORT ID: EIBMCEXT",
    SDESC.strip(),
    f"LISTING OF ACCOUNTS WITH FISS PURPOSE CODES 0311-0316  DATE : {RDATE}",
]


def write_report(
    df: pl.DataFrame,
    by_cols: list[str],
    var_cols: list[str],
    col_labels: dict[str, str],
    title4: str,
    lines: list[str],
    sum_col: str = "BALANCE",
    sumby_col: str = "FISSPURP",
) -> None:
    """
    Replicate PROC PRINT with BY, SUM, SUMBY.
    ASA carriage control: '1' = form-feed, ' ' = single space.
    """
    line_count = PAGE_LENGTH  # trigger new page at start

    def col_header() -> str:
        return "  ".join(col_labels.get(c, c).ljust(17) for c in var_cols)

    separator = "-" * LINE_WIDTH

    def new_page(by_vals: dict) -> None:
        nonlocal line_count
        lines.append(f"1{report_titles[0].center(LINE_WIDTH)}")
        for t in report_titles[1:]:
            lines.append(f" {t.center(LINE_WIDTH)}")
        lines.append(f" {title4.center(LINE_WIDTH)}")
        lines.append(f" ")
        # BY group header
        by_header = "  ".join(
            f"{col_labels.get(k, k)}={by_vals.get(k, '')}" for k in by_cols
        )
        lines.append(f" {by_header}")
        lines.append(f" {separator}")
        lines.append(f" {col_header()}")
        lines.append(f" {separator}")
        line_count = 8 + len(report_titles)

    if df.is_empty():
        return

    # Iterate over BY groups (first by_col, then second)
    sumby_total = 0.0
    prev_sumby  = None

    for by1_val in sorted(df[by_cols[0]].unique().to_list()):
        grp1 = df.filter(pl.col(by_cols[0]) == by1_val)

        for by2_val in sorted(grp1[by_cols[1]].unique().to_list()):
            grp2 = grp1.filter(pl.col(by_cols[1]) == by2_val)

            current_by = {by_cols[0]: by1_val, by_cols[1]: by2_val}

            if line_count >= PAGE_LENGTH - 4:
                new_page(current_by)
            else:
                by_header = "  ".join(
                    f"{col_labels.get(k, k)}={current_by.get(k, '')}"
                    for k in by_cols
                )
                lines.append(f" {by_header}")
                lines.append(f" {separator}")
                line_count += 2

            # Print SUMBY subtotal break if SUMBY value changes
            if prev_sumby is not None and prev_sumby != by2_val:
                lines.append(
                    f" {'SUBTOTAL (' + str(prev_sumby) + ')':<30}"
                    f"{fmt_comma15_2(sumby_total):>15}"
                )
                lines.append(f" {separator}")
                line_count += 2
                sumby_total = 0.0
            prev_sumby = by2_val

            for data_row in grp2.iter_rows(named=True):
                if line_count >= PAGE_LENGTH - 4:
                    new_page(current_by)

                cells = []
                for c in var_cols:
                    val = data_row.get(c)
                    if c == sum_col:
                        cells.append(fmt_comma15_2(val).rjust(17))
                    elif c == "RISKCAT":
                        cells.append(fmt_risk(val).ljust(17))
                    else:
                        cells.append(str(val or "").ljust(17))
                lines.append(f" {'  '.join(cells)}")
                line_count += 1
                sumby_total += float(data_row.get(sum_col) or 0)

    # Final SUMBY total
    if prev_sumby is not None:
        lines.append(
            f" {'SUBTOTAL (' + str(prev_sumby) + ')':<30}"
            f"{fmt_comma15_2(sumby_total):>15}"
        )
        lines.append(f" {separator}")


report_lines: list[str] = []

# --- OD REPORT ---
if not od_df.is_empty():
    write_report(
        df=od_df,
        by_cols=["BRANCH", "FISSPURP"],
        var_cols=["ACCTNO", "NAME", "APPVALUE", "BALANCE", "RISKCAT", "LIAB1", "LIAB2"],
        col_labels={
            "BRANCH":   "BRANCH NO",
            "ACCTNO":   "ACCOUNT NO",
            "NAME":     "NAME",
            "LIAB1":    "COLL CODE 1",
            "LIAB2":    "COLL CODE 2",
            "APPVALUE": "APPRAISAL VALUE",
            "RISKCAT":  "RISK CATEGORY",
            "BALANCE":  "OUTSTANDING BALANCE",
            "FISSPURP": "FISS PURPOSE",
        },
        title4="OD ACCOUNTS (RISK CATEGORY : 0%-20%)",
        lines=report_lines,
        sum_col="BALANCE",
        sumby_col="FISSPURP",
    )

# --- LOAN REPORT ---
if not loan_df.is_empty():
    write_report(
        df=loan_df,
        by_cols=["BRANCH", "FISSPURP"],
        var_cols=["ACCTNO", "NAME", "APPVALUE", "BALANCE", "LIABCOD1"],
        col_labels={
            "BRANCH":   "BRANCH NO",
            "ACCTNO":   "ACCOUNT NO",
            "NAME":     "NAME",
            "LIABCOD1": "COLL CODE",
            "RISKCAT":  "RISK CATEGORY",
            "APPVALUE": "APPRAISAL VALUE",
            "BALANCE":  "OUTSTANDING BALANCE",
            "FISSPURP": "FISS PURPOSE",
        },
        title4="LOAN ACCOUNTS (RISK CATEGORY : 0%-20%)",
        lines=report_lines,
        sum_col="BALANCE",
        sumby_col="FISSPURP",
    )

# ============================================================================
# STEP 6: WRITE REPORT
# ============================================================================

with open(OUTPUT_REPORT, "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines) + "\n")

print(f"Report written to: {OUTPUT_REPORT}")
