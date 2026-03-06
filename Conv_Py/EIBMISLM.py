#!/usr/bin/env python3
"""
Program  : EIBMISLM.py
Purpose  : Profile reports on Islamic (Personal/Joint) deposit accounts
            for Islamic Banking Department, Public Islamic Bank Berhad.
           Produces 8 PROC TABULATE reports (1,2,3,4,6,7,8) across:
             REPT1  - Al-Wadiah Savings Account     (products 204 & 215)
             REPT2  - Al-Wadiah Current Account      (products 160,162,164,182,168)
             REPT3  - Mudharabah Bestari SA (214)
             REPT4  - Al-Wadiah Staff Acc (215)      filtered from AWSA
             REPT6  - Basic Wadiah SA (207)
             REPT7  - Basic Wadiah CA (93)
             REPT8  - Mudharabah CA (96)
           Trigger: 2nd of the month.
"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
# %INC PGM(PBMISFMT) - Provides $RACE., $PURPOSE., HUNDRED. format functions.
#                      format_race(), format_purpose(), format_hundred() used below.
# %INC PGM(PBBDPFMT) - Included in SAS source but no PBBDPFMT formats are
#                      directly referenced in this program; omitted from import.

from PBMISFMT import format_race, format_purpose, format_hundred

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime

# ============================================================================
# OPTIONS / MACRO VARIABLES (SAS equivalents)
# YEARCUTOFF=1930 NOCENTER MISSING=0 NODATE NONUMBER
# ============================================================================

AGELIMIT = 12   # %LET AGELIMIT = 12
MAXAGE   = 18   # %LET MAXAGE   = 18
AGEBELOW = 11   # %LET AGEBELOW = 11

MISSING_NUM = 0  # SAS MISSING=0

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR    = Path("/data")
DEPOSIT_DIR = BASE_DIR / "deposit"   # SAP.PIBB.MNITB(0)
MIS_DIR     = BASE_DIR / "mis"       # SAP.PIBB.ISLM.SASDATA
OUTPUT_DIR  = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPTDATE_FILE = DEPOSIT_DIR / "reptdate.parquet"
OUTPUT_REPORT = OUTPUT_DIR  / "eibmislm_report.txt"  # ISLM1 / PROC PRINTTO

# ============================================================================
# ASA CARRIAGE CONTROL CONSTANTS
# ============================================================================

ASA_NEWPAGE     = '1'  # Form feed / new page
ASA_SINGLESPACE = ' '  # Single space
ASA_DOUBLESPACE = '0'  # Double space

PAGE_LENGTH = 60
PAGE_WIDTH  = 133   # LRECL=133 from JCL DCB

# ============================================================================
# DATA REPTDATE: derive macro variables from DEPOSIT.REPTDATE
# ============================================================================

con = duckdb.connect()

reptdate_df = con.execute(
    f"SELECT * FROM read_parquet('{REPTDATE_FILE}')"
).pl()
row_rep = reptdate_df.row(0, named=True)

REPTDATE_VAL: date = row_rep["REPTDATE"]
if isinstance(REPTDATE_VAL, datetime):
    REPTDATE_VAL = REPTDATE_VAL.date()

REPTYEAR = f"{REPTDATE_VAL.year}"                    # YEAR4.
REPTMON  = f"{REPTDATE_VAL.month:02d}"               # Z2.
REPTDAY  = REPTDATE_VAL.day                          # numeric day (used in division)
REPTDAY_STR = f"{REPTDAY:02d}"                       # Z2.
RDATE    = REPTDATE_VAL.strftime("%d/%m/%y")         # DDMMYY8.  e.g. '02/06/24'
# EDATE as Z5. = SAS date serial (days since 01JAN1960)
EDATE    = (REPTDATE_VAL - date(1960, 1, 1)).days

# ============================================================================
# HELPER: load MIS parquet, apply standard filters and derived columns
#   SET MIS.<TABLE>&REPTMON
#   IF REPTDATE LE &EDATE
#   [IF PRODUCT = <N>]           (optional product filter)
#   AVGAMT = CURBAL   / &REPTDAY
#   NOACCT = NOACCT   / &REPTDAY
#   ACCYTD = ACCYTD   / &REPTDAY
#   IF REPTDATE < &EDATE THEN CURBAL = 0.00
# ============================================================================

def load_mis(table_name: str, product_filter: int = None) -> pl.DataFrame:
    """
    Load MIS.<table_name>&REPTMON parquet, apply REPTDATE filter,
    optional PRODUCT filter, and compute derived columns.
    Returns DataFrame sorted by DEPRANGE, RACE, PURPOSE.
    """
    fpath = MIS_DIR / f"{table_name.lower()}{REPTMON}.parquet"
    df = con.execute(f"SELECT * FROM read_parquet('{fpath}')").pl()

    rows = []
    for r in df.to_dicts():
        # REPTDATE column may be stored as date serial (int) or date
        rep_raw = r.get("REPTDATE")
        if isinstance(rep_raw, (date, datetime)):
            rep_serial = (rep_raw.date() if isinstance(rep_raw, datetime)
                          else rep_raw)
            rep_serial = (rep_serial - date(1960, 1, 1)).days
        else:
            rep_serial = int(rep_raw or 0)

        # IF REPTDATE LE &EDATE
        if rep_serial > EDATE:
            continue

        # Optional PRODUCT filter
        if product_filter is not None:
            if int(r.get("PRODUCT") or 0) != product_filter:
                continue

        # Derived columns (divide by REPTDAY)
        reptday_d = float(REPTDAY) if REPTDAY else 1.0
        curbal    = float(r.get("CURBAL")  or 0.0)
        noacct    = float(r.get("NOACCT")  or 0.0)
        accytd    = float(r.get("ACCYTD")  or 0.0)

        avgamt = curbal  / reptday_d
        noacct = noacct  / reptday_d
        accytd = accytd  / reptday_d

        # IF REPTDATE < &EDATE THEN CURBAL = 0.00
        if rep_serial < EDATE:
            curbal = 0.0

        r["AVGAMT"] = avgamt
        r["NOACCT"] = noacct
        r["ACCYTD"] = accytd
        r["CURBAL"]  = curbal
        rows.append(r)

    if not rows:
        return pl.DataFrame()

    result = pl.DataFrame(rows)
    # PROC SORT BY DEPRANGE RACE PURPOSE
    sort_cols = [c for c in ["DEPRANGE", "RACE", "PURPOSE"] if c in result.columns]
    if sort_cols:
        result = result.sort(sort_cols)
    return result


# ============================================================================
# REPORT GENERATION HELPERS
# ============================================================================

# Column layout matching SAS TABLE statement:
#   NOACCT: SUM='NO OF A/C'  COMMA10.  |  PCTSUM COMMA6.2
#   ACCYTD: SUM='OPEN YTD '  COMMA10.  |  PCTSUM COMMA6.2
#   CURBAL: SUM='O/S BALANCE' COMMA15.2 | PCTSUM COMMA6.2 | PCTSUM<NOACCT>=AVG AMT HUNDRED.
#   AVGAMT: SUM='AVERAGE BAL' COMMA15.2 | PCTSUM COMMA6.2 | PCTSUM<NOACCT>=AVG BAL  HUNDRED.
#   PURPOSE='A/C TYPE'*NOACCT SUM COMMA8.
#   RACE*NOACCT SUM COMMA8.

RTS_W   = 30   # RTS=30
C10_W   = 10   # COMMA10.
C62_W   = 9    # COMMA6.2
C15_W   = 15   # COMMA15.2
C8_W    = 8    # COMMA8.
HND_W   = 13   # HUNDRED. (format_hundred returns up to ~13 chars)


def fmt_c10(val) -> str:
    """COMMA10. integer format."""
    try:
        return f"{int(round(float(val or MISSING_NUM))):,}".rjust(C10_W)
    except (TypeError, ValueError):
        return '0'.rjust(C10_W)


def fmt_c62(val) -> str:
    """COMMA6.2 percent format."""
    try:
        return f"{float(val or MISSING_NUM):,.2f}".rjust(C62_W)
    except (TypeError, ValueError):
        return '0.00'.rjust(C62_W)


def fmt_c15(val) -> str:
    """COMMA15.2 amount format."""
    try:
        return f"{float(val or MISSING_NUM):,.2f}".rjust(C15_W)
    except (TypeError, ValueError):
        return '0.00'.rjust(C15_W)


def fmt_c8(val) -> str:
    """COMMA8. integer format."""
    try:
        return f"{int(round(float(val or MISSING_NUM))):,}".rjust(C8_W)
    except (TypeError, ValueError):
        return '0'.rjust(C8_W)


def fmt_hnd(val) -> str:
    """HUNDRED. picture format (from PBMISFMT.format_hundred)."""
    try:
        return format_hundred(float(val or MISSING_NUM)).rjust(HND_W)
    except (TypeError, ValueError):
        return format_hundred(0.0).rjust(HND_W)


def pct(part, total) -> float:
    """Safe percentage calculation."""
    try:
        t = float(total or 0.0)
        return (float(part or 0.0) / t * 100.0) if t != 0.0 else 0.0
    except (TypeError, ValueError, ZeroDivisionError):
        return 0.0


def page_header(title1: str, title2: str, title3: str,
                title4: str, title5: str) -> list:
    """Generate page header with ASA carriage control."""
    lines = []
    lines.append(f"{ASA_NEWPAGE}{title1}")
    lines.append(f"{ASA_SINGLESPACE}{title2}")
    lines.append(f"{ASA_SINGLESPACE}{title3}")
    lines.append(f"{ASA_SINGLESPACE}{title4}")
    lines.append(f"{ASA_SINGLESPACE}{title5}")
    lines.append(f"{ASA_DOUBLESPACE}")
    return lines


def build_tabulate(
    df: pl.DataFrame,
    deprange_col: str,
    box_label: str,
    title1: str,
    title2: str,
    title3: str,
    title4: str,
    title5: str,
    avg_bal_label: str = "AVG BAL/ACCOUNT",
) -> list:
    """
    Replicate PROC TABULATE MISSING with PRINTMISS layout:
      TABLE deprange_col='DEPOSIT RANGE' ALL='TOTAL',
            NOACCT*(NO OF A/C  %) ACCYTD*(OPEN YTD %)
            CURBAL*(O/S BALANCE % AVG AMT/ACCOUNT)
            AVGAMT*(AVERAGE BAL  % <avg_bal_label>)
            PURPOSE='A/C TYPE'*NOACCT
            RACE*NOACCT
            / RTS=30 BOX=<box_label> PRINTMISS
    """
    lines = page_header(title1, title2, title3, title4, title5)

    if df.is_empty():
        lines.append(f"{ASA_SINGLESPACE}NO DATA")
        return lines

    # Derive all unique DEPRANGE values (PRINTMISS -> include all present)
    depranges = sorted(df[deprange_col].unique().to_list()) if deprange_col in df.columns else []

    # Derive PURPOSE and RACE categories using format functions (PRINTMISS)
    purposes = sorted(df["PURPOSE"].unique().to_list()) if "PURPOSE" in df.columns else []
    races    = sorted(df["RACE"].unique().to_list())    if "RACE"    in df.columns else []

    # ---- Column header block ----
    # Main columns: NOACCT(2), ACCYTD(2), CURBAL(3), AVGAMT(3)
    # + PURPOSE cols, RACE cols
    purpose_labels = [format_purpose(p) for p in purposes]
    race_labels    = [format_race(r)    for r in races]

    hdr_top  = f"{ASA_SINGLESPACE}{box_label:<{RTS_W}}"
    hdr_top += f"  {'NO OF A/C':>{C10_W}}  {'%':>{C62_W}}"
    hdr_top += f"  {'OPEN YTD ':>{C10_W}}  {'%':>{C62_W}}"
    hdr_top += f"  {'O/S BALANCE':>{C15_W}}  {'%':>{C62_W}}  {'AVG AMT/ACCOUNT':>{HND_W}}"
    hdr_top += f"  {'AVERAGE BAL':>{C15_W}}  {'%':>{C62_W}}  {avg_bal_label:>{HND_W}}"
    for pl_  in purpose_labels:
        hdr_top += f"  {pl_[:C8_W]:>{C8_W}}"
    for rl   in race_labels:
        hdr_top += f"  {rl[:C8_W]:>{C8_W}}"
    lines.append(hdr_top)

    sep_len = (RTS_W
               + (C10_W + C62_W + 3) * 2
               + (C15_W + C62_W + HND_W + 5) * 2
               + (C8_W + 2) * (len(purposes) + len(races)))
    lines.append(f"{ASA_SINGLESPACE}{'-' * sep_len}")

    # ---- Aggregate totals for TOTAL row ----
    tot_noacct = 0.0; tot_accytd = 0.0; tot_curbal = 0.0; tot_avgamt = 0.0
    tot_purp: dict   = {p: 0.0 for p in purposes}
    tot_race: dict   = {r: 0.0 for r in races}

    def row_data(sub: pl.DataFrame) -> dict:
        """Aggregate a subset into summary dict."""
        if sub.is_empty():
            return {"noacct": 0.0, "accytd": 0.0, "curbal": 0.0, "avgamt": 0.0,
                    "purp": {p: 0.0 for p in purposes},
                    "race": {r: 0.0 for r in races}}
        na = sub["NOACCT"].sum() if "NOACCT" in sub.columns else 0.0
        ay = sub["ACCYTD"].sum() if "ACCYTD" in sub.columns else 0.0
        cb = sub["CURBAL"].sum() if "CURBAL" in sub.columns else 0.0
        am = sub["AVGAMT"].sum() if "AVGAMT" in sub.columns else 0.0
        purp_sums = {}
        for p in purposes:
            mask = sub.filter(pl.col("PURPOSE") == p) if "PURPOSE" in sub.columns else pl.DataFrame()
            purp_sums[p] = mask["NOACCT"].sum() if (not mask.is_empty() and "NOACCT" in mask.columns) else 0.0
        race_sums = {}
        for r in races:
            mask = sub.filter(pl.col("RACE") == r) if "RACE" in sub.columns else pl.DataFrame()
            race_sums[r] = mask["NOACCT"].sum() if (not mask.is_empty() and "NOACCT" in mask.columns) else 0.0
        return {"noacct": na, "accytd": ay, "curbal": cb, "avgamt": am,
                "purp": purp_sums, "race": race_sums}

    grand = row_data(df)

    for dr in depranges:
        sub = df.filter(pl.col(deprange_col) == dr)
        d   = row_data(sub)

        row_str  = f"{ASA_SINGLESPACE}{str(dr):<{RTS_W}}"
        row_str += f"  {fmt_c10(d['noacct'])}  {fmt_c62(pct(d['noacct'], grand['noacct']))}"
        row_str += f"  {fmt_c10(d['accytd'])}  {fmt_c62(pct(d['accytd'], grand['accytd']))}"
        row_str += (f"  {fmt_c15(d['curbal'])}  {fmt_c62(pct(d['curbal'], grand['curbal']))}"
                    f"  {fmt_hnd(pct(d['curbal'], d['noacct']))}")
        row_str += (f"  {fmt_c15(d['avgamt'])}  {fmt_c62(pct(d['avgamt'], grand['avgamt']))}"
                    f"  {fmt_hnd(pct(d['avgamt'], d['noacct']))}")
        for p in purposes:
            row_str += f"  {fmt_c8(d['purp'].get(p, 0.0))}"
        for r in races:
            row_str += f"  {fmt_c8(d['race'].get(r, 0.0))}"
        lines.append(row_str)

        # Accumulate grand totals
        tot_noacct += d['noacct']; tot_accytd += d['accytd']
        tot_curbal += d['curbal']; tot_avgamt += d['avgamt']
        for p in purposes: tot_purp[p] += d['purp'].get(p, 0.0)
        for r in races:    tot_race[r] += d['race'].get(r, 0.0)

    # ALL='TOTAL' row
    lines.append(f"{ASA_SINGLESPACE}{'-' * sep_len}")
    tot_str  = f"{ASA_SINGLESPACE}{'TOTAL':<{RTS_W}}"
    tot_str += f"  {fmt_c10(tot_noacct)}  {fmt_c62(100.0)}"
    tot_str += f"  {fmt_c10(tot_accytd)}  {fmt_c62(100.0)}"
    tot_str += (f"  {fmt_c15(tot_curbal)}  {fmt_c62(100.0)}"
                f"  {fmt_hnd(pct(tot_curbal, tot_noacct))}")
    tot_str += (f"  {fmt_c15(tot_avgamt)}  {fmt_c62(100.0)}"
                f"  {fmt_hnd(pct(tot_avgamt, tot_noacct))}")
    for p in purposes: tot_str += f"  {fmt_c8(tot_purp[p])}"
    for r in races:    tot_str += f"  {fmt_c8(tot_race[r])}"
    lines.append(tot_str)

    return lines


# ============================================================================
# REPORT 1: REPT1 - Al-Wadiah Savings Account (products 204 & 215)
#   SET MIS.AWSA&REPTMON  (no product filter - all products in AWSA)
#   CLASS DEPRANG2 RACE PURPOSE   <- note: uses DEPRANG2 not DEPRANGE
# ============================================================================

rept1 = load_mis("AWSA")

report_lines = build_tabulate(
    df            = rept1,
    deprange_col  = "DEPRANG2",
    box_label     = "AL-WADIAH SAVINGS ACCOUNT",
    title1        = "REPORT ID : EIBMISLM - 1",
    title2        = "PUBLIC ISLAMIC BANK BERHAD",
    title3        = "FOR ISLAMIC BANKING DEPARTMENT",
    title4        = "PROFILE ON ISLAMIC(PERSONAL/JOINT) AL-WADIAH SAVINGS ACCOUNT",
    title5        = f"REPORT AS AT {RDATE}",
)

# ============================================================================
# REPORT 2: REPT2 - Al-Wadiah Current Account (products 160,162,164,182,168)
#   SET MIS.AWCB&REPTMON  (no product filter)
#   CLASS DEPRANGE RACE PURPOSE
# ============================================================================

rept2 = load_mis("AWCB")

report_lines += build_tabulate(
    df            = rept2,
    deprange_col  = "DEPRANGE",
    box_label     = "AL-WADIAH CURRENT ACCOUNT",
    title1        = "REPORT ID : EIBMISLM - 2",
    title2        = "PUBLIC ISLAMIC BANK BERHAD",
    title3        = "FOR ISLAMIC BANKING DEPARTMENT",
    title4        = "PROFILE ON ISLAMIC(PERSONAL/JOINT) AL-WADIAH CURRENT ACCOUNT",
    title5        = f"REPORT AS AT {RDATE}",
)

# ============================================================================
# REPORT 3: REPT3 - Mudharabah Bestari SA (product 214)
#   SET MIS.MUDH&REPTMON  (no product filter)
#   CLASS DEPRANGE RACE PURPOSE
# ============================================================================

rept3 = load_mis("MUDH")

report_lines += build_tabulate(
    df            = rept3,
    deprange_col  = "DEPRANGE",
    box_label     = "MUDHARABAH BESTARI SAVINGS",
    title1        = "REPORT ID : EIBMISLM - 3",
    title2        = "PUBLIC ISLAMIC BANK BERHAD",
    title3        = "FOR ISLAMIC BANKING DEPARTMENT",
    title4        = "PROFILE ON ISLAMIC(PERSONAL/JOINT)MUDHARABAH BESTARI SA (214)",
    title5        = f"REPORT AS AT {RDATE}",
    avg_bal_label = "AVG BALANCE",
)

# ============================================================================
# REPORT 4: REPT4 - Al-Wadiah Staff Acc (product 215)
#   SET MIS.AWSA&REPTMON  IF PRODUCT=215
#   CLASS DEPRANGE RACE PURPOSE
# ============================================================================

rept4 = load_mis("AWSA", product_filter=215)

report_lines += build_tabulate(
    df            = rept4,
    deprange_col  = "DEPRANGE",
    box_label     = "AL-WADIAH STAFF ACCOUNTS",
    title1        = "REPORT ID : EIBMISLM - 4",
    title2        = "PUBLIC ISLAMIC BANK BERHAD",
    title3        = "FOR ISLAMIC BANKING DEPARTMENT",
    title4        = "PROFILE ON ISLAMIC(PERSONAL/JOINT) AL-WADIAH STAFF ACC (215)",
    title5        = f"REPORT AS AT {RDATE}",
)

# ============================================================================
# REPORT 6: REPT6 - Basic Wadiah SA (product 207)
#   SET MIS.AWSB&REPTMON  IF PRODUCT=207
#   CLASS DEPRANGE RACE PURPOSE
# ============================================================================

rept6 = load_mis("AWSB", product_filter=207)

report_lines += build_tabulate(
    df            = rept6,
    deprange_col  = "DEPRANGE",
    box_label     = "BASIC WADIAH SAVINGS ACCOUNT",
    title1        = "REPORT ID : EIBMISLM - 6",
    title2        = "PUBLIC ISLAMIC BANK BERHAD",
    title3        = "FOR ISLAMIC BANKING DEPARTMENT",
    title4        = "PROFILE ON ISLAMIC(PERSONAL/JOINT) BASIC WADIAH SA (207)",
    title5        = f"REPORT AS AT {RDATE}",
)

# ============================================================================
# REPORT 7: REPT7 - Basic Wadiah CA (product 93)
#   SET MIS.AWCA&REPTMON  IF PRODUCT=093
#   CLASS DEPRANGE RACE PURPOSE
# ============================================================================

rept7 = load_mis("AWCA", product_filter=93)

report_lines += build_tabulate(
    df            = rept7,
    deprange_col  = "DEPRANGE",
    box_label     = "BASIC WADIAH CURRENT ACCOUNT",
    title1        = "REPORT ID : EIBMISLM - 7",
    title2        = "PUBLIC ISLAMIC BANK BERHAD",
    title3        = "FOR ISLAMIC BANKING DEPARTMENT",
    title4        = "PROFILE ON ISLAMIC(PERSONAL/JOINT) BASIC WADIAH CA (93)",
    title5        = f"REPORT AS AT {RDATE}",
)

# ============================================================================
# REPORT 8: REPT8 - Mudharabah CA (product 96)
#   SET MIS.AWCA&REPTMON  IF PRODUCT=96
#   CLASS DEPRANGE RACE PURPOSE
# ============================================================================

rept8 = load_mis("AWCA", product_filter=96)

report_lines += build_tabulate(
    df            = rept8,
    deprange_col  = "DEPRANGE",
    box_label     = "MUDHARABAH CURRENT ACCOUNT",
    title1        = "REPORT ID : EIBMISLM - 8",
    title2        = "PUBLIC ISLAMIC BANK BERHAD",
    title3        = "FOR ISLAMIC BANKING DEPARTMENT",
    title4        = "PROFILE ON ISLAMIC(PERSONAL/JOINT) MUDHARABAH CA (96)",
    title5        = f"REPORT AS AT {RDATE}",
)

# ============================================================================
# WRITE REPORT OUTPUT (PROC PRINTTO PRINT=ISLM1 -> OUTPUT_REPORT)
# ============================================================================

with open(OUTPUT_REPORT, "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines) + "\n")

print(f"Report written to: {OUTPUT_REPORT}")
