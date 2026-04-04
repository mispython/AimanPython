#!/usr/bin/env python3
"""
Program : EIBMBAEI.py
Purpose : Profile on BAE Personal Financing-I Customers.
          Generates 7 distribution reports (State, Financing Limit, Tenure,
          Race, Age, Gender, Salary Range) written to a fixed-width ASA
          carriage-control report file (SASLIST → MATURE_BAE.txt).

Dependency notes
----------------
%INC PGM(BRANCHCD, PBMISFMT, PBBDPFMT) is present in the original SAS but
none of their formats are called via PUT() anywhere in this program's DATA
steps.  All three formats used in the program body (TERMS, AGEGP, $STATX)
are defined locally within EIBMBAEI itself.  No imports from those modules
are therefore required or added here; they are listed below for traceability.

  BRANCHCD : session-level include – no format called in this program
  PBMISFMT : session-level include – no format called in this program
  PBBDPFMT : session-level include – no format called in this program
"""

import duckdb
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import sys

# ============================================================================
# PATH SETUP
# ============================================================================

BASE_PATH  = Path("/data")
# //LOAN  DD DSN=SAP.PIBB.MNILN(0)  → REPTDATE lives here
LOAN_PATH  = BASE_PATH / "PIBB" / "MNILN"
# //BNM1  DD DSN=SAP.PIBB.SASDATA   → weekly loan snapshots
BNM1_PATH  = BASE_PATH / "PIBB" / "SASDATA"
# //ELDS  DD DSN=SAP.PIBB.ELDS.BNM(0) → ELDS approval files
ELDS_PATH  = BASE_PATH / "PIBB" / "ELDS"
# //CISLN DD DSN=SAP.PBB.CISBEXT.LN  → CIS customer data (SAS dataset → Parquet)
CISLN_PATH = BASE_PATH / "CISLN" / "LOAN.parquet"
# //SASLIST DD → output report
OUTPUT_PATH = BASE_PATH / "output"
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_PATH / "MATURE_BAE.txt"

# ============================================================================
# REPORT CONSTANTS  (LRECL=150, FBA)
# ============================================================================

LRECL = 150   # logical record length

# ============================================================================
# HELPER: write one ASA line (first byte = carriage-control character)
# '1' = form feed (new page)
# ' ' = single space (normal advance)
# '0' = double space
# ============================================================================

def write_asa(f, cc: str, text: str) -> None:
    """Write a single ASA FBA line padded/truncated to LRECL."""
    line = f"{cc}{text}"
    # Pad to LRECL but do NOT exceed it (LRECL=150 means 1 CC + 149 data)
    f.write(line[:LRECL].ljust(LRECL) + "\n")


# ============================================================================
# NUMBER FORMATTERS  (mirror SAS picture formats used in PROC TABULATE)
# ============================================================================

def fmt_comma10(v) -> str:
    """COMMA10.  – integer, right-aligned in 10 chars, comma thousands."""
    if v is None:
        return " " * 10
    return f"{int(v):>10,}"


def fmt_comma12(v) -> str:
    """COMMA12.  – integer, right-aligned in 12 chars."""
    if v is None:
        return " " * 12
    return f"{int(v):>12,}"


def fmt_comma14d2(v) -> str:
    """COMMA14.2 – float, right-aligned in 14 chars, 2 decimal places."""
    if v is None:
        return " " * 14
    return f"{float(v):>14,.2f}"


def fmt_comma18d2(v) -> str:
    """COMMA18.2 – float, right-aligned in 18 chars, 2 decimal places."""
    if v is None:
        return " " * 18
    return f"{float(v):>18,.2f}"


def pct(part, total) -> float:
    """Percentage safe-divide."""
    if not total:
        return 0.0
    return float(part) / float(total) * 100.0


# ============================================================================
# PROC TABULATE EMULATION
#
# SAS PROC TABULATE column structure (RTS=30):
#
#  Reports 1,2,5,6,7  (NO OF A/C – COMMA10.):
#  ├── Row label        30 chars  (RTS=30)
#  ├── NO OF A/C        10 chars  (COMMA10.)
#  ├── PERCENTAGE(%)    14 chars  (COMMA14.2)
#  ├── AMOUNT(RM)       18 chars  (COMMA18.2)
#  └── PERCENTAGE(%)    14 chars  (COMMA14.2)
#  Total data width = 10+14+18+14 = 56 + separators
#
#  Reports 3,4  (NO OF ACCTS – COMMA12.):
#  Same but first numeric column is 12 wide (COMMA12.)
# ============================================================================

_SEP = "|"

def _tabulate_header(row_label: str, acct_hdr: str, acct_width: int) -> list[tuple]:
    """Return header lines as (cc, text) tuples."""
    w = acct_width
    lines = []
    # Separator line
    sep_line = (
        f"{row_label:<30}{_SEP}"
        f"{'':>{w}}{_SEP}"
        f"{'':>14}{_SEP}"
        f"{'':>18}{_SEP}"
        f"{'':>14}{_SEP}"
    )
    lines.append((" ", sep_line))
    # Column header row 1
    h1 = (
        f"{' ':<30}{_SEP}"
        f"{acct_hdr:^{w}}{_SEP}"
        f"{'PERCENTAGE(%)':^14}{_SEP}"
        f"{'AMOUNT(RM)':^18}{_SEP}"
        f"{'PERCENTAGE(%)':^14}{_SEP}"
    )
    lines.append((" ", h1))
    # Divider
    div = "-" * (30 + 1 + w + 1 + 14 + 1 + 18 + 1 + 14 + 1)
    lines.append((" ", div))
    return lines


def _tabulate_row(label: str, n: int, n_pct: float,
                  amt: float, amt_pct: float, acct_width: int) -> tuple:
    """Return one data row as (cc, text)."""
    w = acct_width
    if w == 10:
        n_str = fmt_comma10(n)
    else:
        n_str = fmt_comma12(n)
    line = (
        f"{label:<30}{_SEP}"
        f"{n_str}{_SEP}"
        f"{fmt_comma14d2(n_pct)}{_SEP}"
        f"{fmt_comma18d2(amt)}{_SEP}"
        f"{fmt_comma14d2(amt_pct)}{_SEP}"
    )
    return (" ", line)


def write_tabulate(
    f,
    title1: str,
    title2: str,
    title3: str,
    title4: str,
    data: pl.DataFrame,
    class_col: str,
    n_col: str,
    amt_col: str,
    row_label: str,
    acct_hdr: str,
    acct_width: int = 10,
) -> None:
    """
    Write one PROC TABULATE equivalent section to the report file.
    Titles are written with ASA form-feed ('1') on TITLE1 (new page),
    single-space (' ') on subsequent title lines.
    """
    # Totals
    total_n   = data.select(pl.col(n_col).sum()).item(0, 0) or 0
    total_amt = data.select(pl.col(amt_col).sum()).item(0, 0) or 0.0

    # TITLE lines – '1' on first title = new page
    write_asa(f, "1", title1)
    write_asa(f, " ", title2)
    write_asa(f, " ", title3)
    write_asa(f, " ", title4)
    write_asa(f, " ", "")

    # Column headers
    for cc, line in _tabulate_header(row_label, acct_hdr, acct_width):
        write_asa(f, cc, line)

    # Data rows (sorted by class_col, MISSING rows included per PRINTMISS)
    for row in data.sort(class_col, nulls_last=True).iter_rows(named=True):
        lbl = str(row[class_col]) if row[class_col] is not None else " "
        n   = row[n_col]   or 0
        amt = row[amt_col] or 0.0
        cc, line = _tabulate_row(
            lbl, n, pct(n, total_n), amt, pct(amt, total_amt), acct_width
        )
        write_asa(f, cc, line)

    # TOTAL row (ALL='TOTAL')
    div = "-" * (30 + 1 + acct_width + 1 + 14 + 1 + 18 + 1 + 14 + 1)
    write_asa(f, " ", div)
    _, tot_line = _tabulate_row(
        "TOTAL", total_n, 100.0, total_amt, 100.0, acct_width
    )
    write_asa(f, " ", tot_line)
    write_asa(f, " ", div)


# ============================================================================
# LOCAL FORMAT FUNCTIONS  (mirrors PROC FORMAT definitions in EIBMBAEI)
# ============================================================================

def fmt_terms(noteterm: int) -> str:
    """
    VALUE TERMS
      24='01.  24 MONTHS' 36='02.  36 MONTHS' ... OTHER='19. 240 MONTHS'
    """
    _map = {
         24: "01.  24 MONTHS",
         36: "02.  36 MONTHS",
         48: "03.  48 MONTHS",
         60: "04.  60 MONTHS",
         72: "05.  72 MONTHS",
         84: "06.  84 MONTHS",
         96: "07.  96 MONTHS",
        108: "08. 108 MONTHS",
        120: "09. 120 MONTHS",
        132: "10. 132 MONTHS",
        144: "11. 144 MONTHS",
        156: "12. 156 MONTHS",
        168: "13. 168 MONTHS",
        180: "14. 180 MONTHS",
        192: "15. 192 MONTHS",
        204: "16. 204 MONTHS",
        216: "17. 216 MONTHS",
        228: "18. 228 MONTHS",
    }
    return _map.get(noteterm, "19. 240 MONTHS")


def fmt_agegp(age: int) -> str:
    """
    VALUE AGEGP
      18-30='1. 18 - 30 ' 31-40='2. 31 - 40 ' 41-50='3. 41 - 50 '
      51-55='4. 51 - 55 ' OTHER='5. 56 - 58 '
    """
    if 18 <= age <= 30:
        return "1. 18 - 30 "
    elif 31 <= age <= 40:
        return "2. 31 - 40 "
    elif 41 <= age <= 50:
        return "3. 41 - 50 "
    elif 51 <= age <= 55:
        return "4. 51 - 55 "
    else:
        return "5. 56 - 58 "


def fmt_statx(statecd: str) -> str:
    """
    VALUE $STATX  A='01. PERAK' ... W='15. WILAYAH PERSEKUTUAN'
    """
    _map = {
        "A": "01. PERAK              ",
        "B": "02. SELANGOR           ",
        "C": "03. PAHANG             ",
        "D": "04. KELANTAN           ",
        "J": "05. JOHOR              ",
        "K": "06. KEDAH              ",
        "L": "07. WILAYAH LABUAN     ",
        "M": "08. MELAKA             ",
        "N": "09. NEGERI SEMBILAN    ",
        "P": "10. PENANG             ",
        "Q": "11. SARAWAK            ",
        "R": "12. PERLIS             ",
        "S": "13. SABAH              ",
        "T": "14. TERENGGANU         ",
        "W": "15. WILAYAH PERSEKUTUAN",
    }
    return _map.get(str(statecd).strip().upper(), "16. UNKNOWN            ")


# ============================================================================
# LOAD REPTDATE
# SAS: DATA REPTDATE; SET LOAN.REPTDATE;
# LOAN library = SAP.PIBB.MNILN(0) → LOAN_PATH / REPTDATE.parquet
# ============================================================================

reptdate_df = pl.read_parquet(LOAN_PATH / "REPTDATE.parquet")
reptdate_val = reptdate_df.select("REPTDATE").item(0, 0)

if isinstance(reptdate_val, str):
    reptdate = datetime.strptime(reptdate_val, "%Y-%m-%d").date()
else:
    reptdate = reptdate_val  # already a date/datetime

# REPTDAT1 = REPTDATE + 1
reptdat1 = reptdate + timedelta(days=1)

# Macro variable equivalents
wk1 = "1"
wk2 = "2"
wk3 = "3"
wk4 = "4"

reptyea2  = reptdate.strftime("%y")           # 2-digit year of REPTDATE
reptyea1  = reptdate.strftime("%Y")           # 4-digit year of REPTDATE
reptmon1  = f"{reptdate.month:02d}"           # 2-digit month of REPTDATE
reptday   = f"{reptdate.day:02d}"
rdate     = reptdate.strftime("%d/%m/%y")     # DDMMYY8. → dd/mm/yy
reptmon   = f"{reptdat1.month:02d}"           # month of REPTDAT1 (next day)
reptyear  = int(reptdat1.strftime("%Y"))      # 4-digit year of REPTDAT1

print(f"Report Date  : {rdate}")
print(f"Report Month : {reptmon}")

# ============================================================================
# DuckDB connection
# ============================================================================

con = duckdb.connect()

# ============================================================================
# DATA LNOTE
# SAS: SET SASD.LOAN&REPTMON1&WK4;   ← SASD = BNM1 library (PIBB.SASDATA)
#      IF PRODUCT = 135;
#      NOACCT = 1;
#      NOTETERM = NOTETERM - 3;
#      TENURE = PUT(NOTETERM, TERMS.);
#      STATED = PUT(STATECD, $STATX.);
#      IF APPRLIM2 > 150000 THEN NETPROC = APPRLIM2;
#      IF (NETPROC <= 10000) THEN LMTGRP = '01...'; ELSE ...
# ============================================================================

loan_file = BNM1_PATH / f"LOAN{reptmon1}{wk4}.parquet"

lnote = con.execute(f"""
    SELECT *
    FROM read_parquet('{loan_file}')
    WHERE PRODUCT = 135
""").pl()

# NOACCT = 1; NOTETERM = NOTETERM - 3
lnote = lnote.with_columns([
    pl.lit(1).alias("NOACCT"),
    (pl.col("NOTETERM") - 3).alias("NOTETERM"),
])

# TENURE = PUT(NOTETERM, TERMS.)
lnote = lnote.with_columns(
    pl.col("NOTETERM").map_elements(fmt_terms, return_dtype=pl.Utf8).alias("TENURE")
)

# STATED = PUT(STATECD, $STATX.)
lnote = lnote.with_columns(
    pl.col("STATECD").map_elements(fmt_statx, return_dtype=pl.Utf8).alias("STATED")
)

# IF APPRLIM2 > 150000 THEN NETPROC = APPRLIM2  (overwrite NETPROC in place)
lnote = lnote.with_columns(
    pl.when(pl.col("APPRLIM2") > 150000)
    .then(pl.col("APPRLIM2"))
    .otherwise(pl.col("NETPROC"))
    .alias("NETPROC")
)

# LMTGRP assignment (uses updated NETPROC)
lnote = lnote.with_columns(
    pl.when(pl.col("NETPROC") <=  10000).then(pl.lit("01.UP TO  10,000   "))
    .when(pl.col("NETPROC") <=  15000).then(pl.lit("02.> 10,000- 15,000"))
    .when(pl.col("NETPROC") <=  20000).then(pl.lit("03.> 15,000- 20,000"))
    .when(pl.col("NETPROC") <=  30000).then(pl.lit("04.> 20,000- 30,000"))
    .when(pl.col("NETPROC") <=  40000).then(pl.lit("05.> 30,000- 40,000"))
    .when(pl.col("NETPROC") <=  50000).then(pl.lit("06.> 40,000- 50,000"))
    .when(pl.col("NETPROC") <=  60000).then(pl.lit("07.> 50,000- 60,000"))
    .when(pl.col("NETPROC") <=  70000).then(pl.lit("08.> 60,000- 70,000"))
    .when(pl.col("NETPROC") <=  80000).then(pl.lit("09.> 70,000- 80,000"))
    .when(pl.col("NETPROC") <=  90000).then(pl.lit("10.> 80,000- 90,000"))
    .when(pl.col("NETPROC") <= 100000).then(pl.lit("11.> 90,000-100,000"))
    .when(pl.col("NETPROC") <= 110000).then(pl.lit("12.>100,000-110,000"))
    .when(pl.col("NETPROC") <= 120000).then(pl.lit("13.>110,000-120,000"))
    .when(pl.col("NETPROC") <= 130000).then(pl.lit("14.>120,000-130,000"))
    .when(pl.col("NETPROC") <= 140000).then(pl.lit("15.>130,000-140,000"))
    .when(pl.col("NETPROC") <= 150000).then(pl.lit("16.>140,000-150,000"))
    .otherwise(pl.lit("17.>150,000        "))
    .alias("LMTGRP")
)

# ============================================================================
# PROC SUMMARY  RPT1..RPT3
# ============================================================================

# RPT1: CLASS STATED; VAR NOACCT BALANCE;
rpt1 = lnote.group_by("STATED").agg([
    pl.col("NOACCT").sum().alias("NOACCT"),
    pl.col("BALANCE").sum().alias("BALANCE"),
])

# RPT2: CLASS LMTGRP; VAR NOACCT BALANCE;
rpt2_raw = lnote.group_by("LMTGRP").agg([
    pl.col("NOACCT").sum().alias("NOACCT"),
    pl.col("BALANCE").sum().alias("BALANCE"),
])

# RPT3: CLASS TENURE; VAR BALANCE NOACCT;
rpt3_raw = lnote.group_by("TENURE").agg([
    pl.col("NOACCT").sum().alias("NOACCT"),
    pl.col("BALANCE").sum().alias("BALANCE"),
])

# ============================================================================
# DUM2 / DUM3 – ensure all category labels appear even with zero counts
# SAS: DATA DUM2; NOACCT=0; BALANCE=0; LMTGRP='01...'; OUTPUT; ...
#      DATA RPT2; MERGE DUM2 RPT2; BY LMTGRP;
# ============================================================================

_lmtgrp_cats = [
    "01.UP TO  10,000   ", "02.> 10,000- 15,000", "03.> 15,000- 20,000",
    "04.> 20,000- 30,000", "05.> 30,000- 40,000", "06.> 40,000- 50,000",
    "07.> 50,000- 60,000", "08.> 60,000- 70,000", "09.> 70,000- 80,000",
    "10.> 80,000- 90,000", "11.> 90,000-100,000", "12.>100,000-110,000",
    "13.>110,000-120,000", "14.>120,000-130,000", "15.>130,000-140,000",
    "16.>140,000-150,000", "17.>150,000        ",
]
dum2 = pl.DataFrame({
    "LMTGRP": _lmtgrp_cats,
    "NOACCT": [0] * len(_lmtgrp_cats),
    "BALANCE": [0.0] * len(_lmtgrp_cats),
})
rpt2 = (
    dum2.join(rpt2_raw, on="LMTGRP", how="left", suffix="_r")
    .with_columns([
        pl.coalesce(["NOACCT_r",  "NOACCT"]).alias("NOACCT"),
        pl.coalesce(["BALANCE_r", "BALANCE"]).alias("BALANCE"),
    ])
    .select(["LMTGRP", "NOACCT", "BALANCE"])
)

_tenure_cats = [
    "01.  24 MONTHS", "02.  36 MONTHS", "03.  48 MONTHS", "04.  60 MONTHS",
    "05.  72 MONTHS", "06.  84 MONTHS", "07.  96 MONTHS", "08. 108 MONTHS",
    "09. 120 MONTHS", "10. 132 MONTHS", "11. 144 MONTHS", "12. 156 MONTHS",
    "13. 168 MONTHS", "14. 180 MONTHS", "15. 192 MONTHS", "16. 204 MONTHS",
    "17. 216 MONTHS", "18. 228 MONTHS", "19. 240 MONTHS",
]
dum3 = pl.DataFrame({
    "TENURE": _tenure_cats,
    "NOACCT": [0] * len(_tenure_cats),
    "BALANCE": [0.0] * len(_tenure_cats),
})
rpt3 = (
    dum3.join(rpt3_raw, on="TENURE", how="left", suffix="_r")
    .with_columns([
        pl.coalesce(["NOACCT_r",  "NOACCT"]).alias("NOACCT"),
        pl.coalesce(["BALANCE_r", "BALANCE"]).alias("BALANCE"),
    ])
    .select(["TENURE", "NOACCT", "BALANCE"])
)

# ============================================================================
# DATA CISLN
# SAS: SET CISLN.LOAN;  IF SECCUST='901';  KEEP ACCTNO GENDER RACE BIRTHDAT;
#      PROC SORT NODUPKEYS BY ACCTNO;
# CISLN library → SAP.PBB.CISBEXT.LN (SAS dataset → Parquet)
# ============================================================================

cisln = (
    pl.read_parquet(CISLN_PATH)
    .filter(pl.col("SECCUST") == "901")
    .select(["ACCTNO", "GENDER", "RACE", "BIRTHDAT"])
    .unique(subset=["ACCTNO"], keep="first")
)

# ============================================================================
# DATA P135
# SAS: MERGE LNOTE(IN=A) CISLN; BY ACCTNO; IF A;
#      BYEAR = SUBSTR(BIRTHDAT, 5, 4);  ← 1-based positions 5-8
#      AGE   = &REPTYEAR - BYEAR;
#      AGEGP = PUT(AGE, AGEGP.);
#      RACED = ...
#      GENDERX = ...
# ============================================================================

p135 = lnote.join(cisln, on="ACCTNO", how="left")

# AGE: BYEAR = SUBSTR(BIRTHDAT,5,4) → Python 0-based slice(4,4) = same 4 chars
p135 = p135.with_columns(
    pl.when(
        pl.col("BIRTHDAT").is_not_null()
        & (pl.col("BIRTHDAT").str.strip_chars() != "")
        & (pl.col("BIRTHDAT").str.strip_chars() != "    ")
    )
    .then(reptyear - pl.col("BIRTHDAT").str.slice(4, 4).cast(pl.Int32))
    .otherwise(0)
    .alias("AGE")
)

# AGEGP = PUT(AGE, AGEGP.)
p135 = p135.with_columns(
    pl.col("AGE").map_elements(fmt_agegp, return_dtype=pl.Utf8).alias("AGEGP")
)

# RACED
p135 = p135.with_columns(
    pl.when(pl.col("RACE") == "1").then(pl.lit("MALAY     "))
    .when(pl.col("RACE") == "2").then(pl.lit("CHINESE   "))
    .when(pl.col("RACE") == "3").then(pl.lit("INDIAN    "))
    .otherwise(pl.lit("OTHER     "))
    .alias("RACED")
)

# GENDERX (default N/A, then overwrite)
p135 = p135.with_columns(
    pl.when(pl.col("GENDER") == "M").then(pl.lit("MALE  "))
    .when(pl.col("GENDER") == "F").then(pl.lit("FEMALE"))
    .otherwise(pl.lit("N/A   "))
    .alias("GENDERX")
)

# ============================================================================
# PROC SUMMARY  RPT4..RPT6
# ============================================================================

# RPT4: CLASS RACED; VAR NOACCT BALANCE;
rpt4 = p135.group_by("RACED").agg([
    pl.col("NOACCT").sum().alias("NOACCT"),
    pl.col("BALANCE").sum().alias("BALANCE"),
])

# RPT5: CLASS AGEGP; VAR NOACCT BALANCE;
rpt5 = p135.group_by("AGEGP").agg([
    pl.col("NOACCT").sum().alias("NOACCT"),
    pl.col("BALANCE").sum().alias("BALANCE"),
])

# RPT6: CLASS GENDERX; VAR NOACCT BALANCE;
rpt6 = p135.group_by("GENDERX").agg([
    pl.col("NOACCT").sum().alias("NOACCT"),
    pl.col("BALANCE").sum().alias("BALANCE"),
])

# ============================================================================
# DATA ELDS
# SAS: SET ELDS.IELN&REPTMON1&WK1&REPTYEA2
#          ELDS.IELN&REPTMON1&WK2&REPTYEA2
#          ELDS.IELN&REPTMON1&WK3&REPTYEA2
#          ELDS.IELN&REPTMON1&WK4&REPTYEA2;
#      IF STATUS='APPROVED'; IF PRODUCT=135;
#      NOACCT=1;
#      IF GINCOME=. THEN GINCOME=99999999;
#      SALGRP = ...
# ============================================================================

elds_files = []
for wk in [wk1, wk2, wk3, wk4]:
    fp = ELDS_PATH / f"IELN{reptmon1}{wk}{reptyea2}.parquet"
    if fp.exists():
        elds_files.append(str(fp))

rpt7 = None
if elds_files:
    file_list = ", ".join(f"'{f}'" for f in elds_files)
    elds = con.execute(f"""
        SELECT *
        FROM read_parquet([{file_list}])
        WHERE STATUS = 'APPROVED'
          AND PRODUCT = 135
    """).pl()

    # NOACCT=1; GINCOME null → 99999999
    elds = elds.with_columns([
        pl.lit(1).alias("NOACCT"),
        pl.col("GINCOME").fill_null(99999999).alias("GINCOME"),
    ])

    # SALGRP
    elds = elds.with_columns(
        pl.when(pl.col("GINCOME") <  1000              ).then(pl.lit("01. BELOW 1,000  "))
        .when(pl.col("GINCOME") == 1000                ).then(pl.lit("02. 1,000        "))
        .when(pl.col("GINCOME") <= 1500                ).then(pl.lit("03. >1,000-1,500 "))
        .when(pl.col("GINCOME") <= 2000                ).then(pl.lit("04. >1,500-2,000 "))
        .when(pl.col("GINCOME") <= 2500                ).then(pl.lit("05. >2,000-2,500 "))
        .when(pl.col("GINCOME") <= 3000                ).then(pl.lit("06. >2,500-3,000 "))
        .when(pl.col("GINCOME") <= 3500                ).then(pl.lit("07. >3,000-3,500 "))
        .when(pl.col("GINCOME") <= 4000                ).then(pl.lit("08. >3,500-4,000 "))
        .when(pl.col("GINCOME") <= 4500                ).then(pl.lit("09. >4,000-4,500 "))
        .when(pl.col("GINCOME") <= 5000                ).then(pl.lit("10. >4,500-5,000 "))
        .when(pl.col("GINCOME") <= 5500                ).then(pl.lit("11. >5,000-5,500 "))
        .when(pl.col("GINCOME") <= 6000                ).then(pl.lit("12. >5,500-6,000 "))
        .when(pl.col("GINCOME") <= 6500                ).then(pl.lit("13. >6,000-6,500 "))
        .when(pl.col("GINCOME") <= 7000                ).then(pl.lit("14. >6,500-7,000 "))
        .when(pl.col("GINCOME") <= 7500                ).then(pl.lit("15. >7,000-7,500 "))
        .when(pl.col("GINCOME") <= 8000                ).then(pl.lit("16. >7,500-8,000 "))
        .when(pl.col("GINCOME") <= 9000                ).then(pl.lit("17. >8,000-9,000 "))
        .when(pl.col("GINCOME") <= 10000               ).then(pl.lit("18. >9,000-10,000"))
        .when(pl.col("GINCOME") <= 99999998            ).then(pl.lit("19. ABOVE 10,000 "))
        .otherwise(                                          pl.lit("20. N/A          "))
        .alias("SALGRP")
    )

    # PROC SUMMARY CLASS SALGRP; VAR AMOUNT NOACCT;
    rpt7_raw = elds.group_by("SALGRP").agg([
        pl.col("NOACCT").sum().alias("NOACCT"),
        pl.col("AMOUNT").sum().alias("AMOUNT"),
    ])

    # DUM7 merge (ensure all 20 salary categories present)
    _salgrp_cats = [
        "01. BELOW 1,000  ", "02. 1,000        ", "03. >1,000-1,500 ",
        "04. >1,500-2,000 ", "05. >2,000-2,500 ", "06. >2,500-3,000 ",
        "07. >3,000-3,500 ", "08. >3,500-4,000 ", "09. >4,000-4,500 ",
        "10. >4,500-5,000 ", "11. >5,000-5,500 ", "12. >5,500-6,000 ",
        "13. >6,000-6,500 ", "14. >6,500-7,000 ", "15. >7,000-7,500 ",
        "16. >7,500-8,000 ", "17. >8,000-9,000 ", "18. >9,000-10,000",
        "19. ABOVE 10,000 ", "20. N/A          ",
    ]
    dum7 = pl.DataFrame({
        "SALGRP": _salgrp_cats,
        "NOACCT": [0] * len(_salgrp_cats),
        "AMOUNT": [0.0] * len(_salgrp_cats),
    })
    rpt7 = (
        dum7.join(rpt7_raw, on="SALGRP", how="left", suffix="_r")
        .with_columns([
            pl.coalesce(["NOACCT_r", "NOACCT"]).alias("NOACCT"),
            pl.coalesce(["AMOUNT_r", "AMOUNT"]).alias("AMOUNT"),
        ])
        .select(["SALGRP", "NOACCT", "AMOUNT"])
    )
else:
    print("WARNING: No ELDS files found for Report 7.", file=sys.stderr)

# ============================================================================
# WRITE SASLIST OUTPUT
# TITLE1 = 'PUBLIC ISLAMIC BANK BERHAD: REPORT ID: EIBMBAEI'
# TITLE2 = 'PROFILE ON BAE PERSONAL FINANCING-I CUSTOMERS &RDATE'
# TITLE3 = varies per report
# TITLE4 = 'COORBRH=IBU'
# LRECL=150, RECFM=FBA
# ============================================================================

T1 = "PUBLIC ISLAMIC BANK BERHAD: REPORT ID: EIBMBAEI"
T2 = f"PROFILE ON BAE PERSONAL FINANCING-I CUSTOMERS {rdate}"
T4 = "COORBRH=IBU"

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:

    # Report 1: Distribution by State
    # TABLE STATED='STATE ' ALL='TOTAL', NOACCT*(SUM='NO OF A/C'*F=COMMA10. ...
    write_tabulate(
        f, T1, T2, "1.  DISTRIBUTION BY STATE", T4,
        data=rpt1, class_col="STATED",
        n_col="NOACCT", amt_col="BALANCE",
        row_label="STATE",
        acct_hdr="NO OF A/C", acct_width=10,
    )

    # Report 2: Distribution by Financing Limit
    # TABLE LMTGRP='APPROVED LIMIT' ALL='TOTAL', NOACCT*(SUM='NO OF A/C'*F=COMMA10. ...
    write_tabulate(
        f, T1, T2, "2.  DISTRIBUTION BY FINANCING LIMIT", T4,
        data=rpt2, class_col="LMTGRP",
        n_col="NOACCT", amt_col="BALANCE",
        row_label="APPROVED LIMIT",
        acct_hdr="NO OF A/C", acct_width=10,
    )

    # Report 3: Distribution by Tenure
    # TABLE TENURE='TENURE (MONTHS)' ALL='TOTAL', NOACCT*(SUM='NO OF ACCTS'*F=COMMA12. ...
    write_tabulate(
        f, T1, T2, "3.  DISTRIBUTION BY TENURE", T4,
        data=rpt3, class_col="TENURE",
        n_col="NOACCT", amt_col="BALANCE",
        row_label="TENURE (MONTHS)",
        acct_hdr="NO OF ACCTS", acct_width=12,
    )

    # Report 4: Distribution by Race
    # TABLE RACED='RACE          ' ALL='TOTAL', NOACCT*(SUM='NO OF ACCTS'*F=COMMA12. ...
    write_tabulate(
        f, T1, T2, "4.  DISTRIBUTION BY RACE", T4,
        data=rpt4, class_col="RACED",
        n_col="NOACCT", amt_col="BALANCE",
        row_label="RACE          ",
        acct_hdr="NO OF ACCTS", acct_width=12,
    )

    # Report 5: Distribution by Age
    # TABLE AGEGP='AGE RANGE' ALL='TOTAL', NOACCT*(SUM='NO OF A/C'*F=COMMA10. ...
    write_tabulate(
        f, T1, T2, "5.  DISTRIBUTION BY AGE", T4,
        data=rpt5, class_col="AGEGP",
        n_col="NOACCT", amt_col="BALANCE",
        row_label="AGE RANGE",
        acct_hdr="NO OF A/C", acct_width=10,
    )

    # Report 6: Distribution by Gender
    # TABLE GENDERX='GENDER   ' ALL='TOTAL', NOACCT*(SUM='NO OF A/C'*F=COMMA10. ...
    write_tabulate(
        f, T1, T2, "6.  DISTRIBUTION BY GENDER", T4,
        data=rpt6, class_col="GENDERX",
        n_col="NOACCT", amt_col="BALANCE",
        row_label="GENDER   ",
        acct_hdr="NO OF A/C", acct_width=10,
    )

    # Report 7: Distribution by Salary Range (approved for the month)
    # TABLE SALGRP='SALARY RANGE' ALL='TOTAL', NOACCT*(SUM='NO OF A/C'*F=COMMA10. ...
    if rpt7 is not None:
        write_tabulate(
            f, T1, T2,
            "7.  DISTRIBUTION BY SALARY RANGE (APPROVED FOR THE MONTH)", T4,
            data=rpt7, class_col="SALGRP",
            n_col="NOACCT", amt_col="AMOUNT",
            row_label="SALARY RANGE",
            acct_hdr="NO OF A/C", acct_width=10,
        )

con.close()
print(f"Report written to: {OUTPUT_FILE}")
