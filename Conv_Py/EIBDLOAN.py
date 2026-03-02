#!/usr/bin/env python3
"""
Program : EIBDLOAN
Function: Daily Loans Summarised by Branch
          Report Owner: Product Development (Retail Banking Div)
          Report ID   : EIBDLOAN
          Produces:
            Part 1 – Branch Summary (Term Loan / Revolving Credit / HP)
            Part 2 – Customer Daily Movement >= RM1M (Term Loan / RC / HP)
"""

import duckdb
import polars as pl
import os
from datetime import datetime, timedelta
from typing import Optional

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR      = os.environ.get("BASE_DIR",    "/data")
MIS_DIR       = os.path.join(BASE_DIR, "mis")         # SAP.PBB.MIS.D<YEAR>
MIS1_DIR      = os.path.join(BASE_DIR, "mis1")        # SAP.PBB.MIS.D<PREVYEAR>
OUTPUT_DIR    = os.path.join(BASE_DIR, "output")

# Input files (fixed-width already decoded to parquet)
DATEFILE      = os.path.join(BASE_DIR, "DATEFILE.txt")        # LRECL=80, @01 EXTDATE 11.
FEEFILE       = os.path.join(BASE_DIR, "FEEFILE.parquet")     # fee-plan file
ACCTFILE      = os.path.join(BASE_DIR, "ACCTFILE.parquet")    # loan account file
BRANCHF       = os.path.join(BASE_DIR, "BRANCHF.txt")         # branch reference file

# Output report
REPORT_FILE   = os.path.join(OUTPUT_DIR, "EIBDLOAN.txt")

# ASA carriage-control
ASA_NEWPAGE = "1"
ASA_NEWLINE = " "
ASA_SKIP2   = "0"

PAGE_LINES  = 60

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================================
# LOAN-TYPE CLASSIFICATION SETS
# ============================================================================

REVCRE_TYPES = {302, 350, 364, 365, 506, 902, 903, 910, 925, 951}
HPLOAN_TYPES = {128, 130, 380, 381, 700, 705}
# Everything else → LNNOTE (term loan)

# ============================================================================
# HELPERS
# ============================================================================

def fmt_comma(value: Optional[float], width: int = 18, dec: int = 2) -> str:
    """COMMA18.2 right-aligned."""
    if value is None:
        return " " * width
    s = f"{value:,.{dec}f}"
    return s.rjust(width)


def fmt_comma20(value: Optional[float]) -> str:
    return fmt_comma(value, 20, 2)


def fmt_int(value: Optional[float], width: int = 10) -> str:
    """Integer format, right-aligned."""
    if value is None:
        return " " * width
    return str(int(value)).rjust(width)


# ============================================================================
# STEP 1 – DERIVE REPORT DATE FROM DATEFILE
# ============================================================================

def derive_report_date() -> dict:
    """
    Read first record of DATEFILE (LRECL=80, @01 EXTDATE 11.).
    REPTDATE = MMDDYY8 parse of first 8 chars of zero-padded EXTDATE.
    Derive PREVDATE (REPTDATE-1), DLETDATE (REPTDATE-3).
    Handle YY roll-back for Jan 1.
    """
    with open(DATEFILE, "r") as fh:
        line = fh.readline()

    extdate_raw = int(line[:11].strip())
    extdate_str = str(extdate_raw).zfill(11)[:8]       # first 8 of Z11
    reptdate    = datetime.strptime(extdate_str, "%m%d%Y")
    prevdate    = reptdate - timedelta(days=1)
    dletdate    = reptdate - timedelta(days=3)

    # YY for PREVYEAR – roll back one year on Jan 1
    if reptdate.month == 1 and reptdate.day == 1:
        yy = reptdate.year - 1
    else:
        yy = reptdate.year

    return {
        "reptdate"  : reptdate,
        "prevdate"  : prevdate,
        "dletdate"  : dletdate,
        "extdate"   : extdate_raw,
        "reptyear"  : reptdate.strftime("%Y"),
        "prevyear"  : str(yy).zfill(4),
        "reptmon"   : reptdate.strftime("%m"),
        "reptday"   : reptdate.strftime("%d"),
        "rdate"     : reptdate.strftime("%d/%m/%Y"),
        "reptdate_z": int(reptdate.strftime("%j")),     # Z5
        "prevday"   : prevdate.strftime("%d"),
        "dletday"   : dletdate.strftime("%d"),
    }


# ============================================================================
# STEP 2 – LOAD & FILTER FEEFILE → FEEPO
# ============================================================================

def build_feepo() -> pl.DataFrame:
    """
    Load FEEFILE, apply all FEEPLN / LOANTYPE inclusion rules,
    then PROC SUMMARY by ACCTNO / NOTENO summing FEEAMTA / FEEAMTB / FEEAMTC.
    """
    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{FEEFILE}')").pl()
    con.close()

    def keep_row(row: dict) -> bool:
        acctno   = row.get("ACCTNO",   0) or 0
        loantype = row.get("LOANTYPE", 0) or 0
        feepln   = str(row.get("FEEPLN", "") or "").strip()

        if acctno < 3_000_000_000:
            if feepln == "PA":
                return True
            if loantype in (241, 242, 243, 362) and feepln in ("LA","LB","LW","PO","RP"):
                return True
            if loantype == 138 and feepln in ("PO","RP","MI","PM","IH"):
                return True
            if loantype == 182 and feepln in ("PO","RP","MI","PM","IH","LA","LV","LW"):
                return True
            if loantype in (244, 245) and feepln in ("LA","LB","LW","PO","RP"):
                return True
            if loantype == 363 and feepln in ("LA","LB","LV","LW","PO","RP","CR"):
                return True
            if loantype == 569 and feepln in ("LA","LW","PO","RP","CR"):
                return True
            return False

        if acctno > 8_000_000_000:
            if loantype in (117,118,119,126) and feepln in ("BC","LG","LX","PL","LF","IP","MC","99"):
                return True
            if loantype in (113,115,116,127) and feepln in ("BC","LE","LX","PL","LF","IP","MC","99"):
                return True
            if loantype in (227,228,230,231,234,235,236,237,238,239,
                            240,241,242,359) and feepln in ("LA","PL","RP","99","LD"):
                return True
            if loantype in (128,130,380,381,700,705,983,993,996) and feepln == "LD":
                return True
            if loantype in (720,725) and feepln in ("AD","AC","DC","IP","LF","MC","PC","RE",
                                                    "BA","LD","RI","SC","SE","SF","TC","99"):
                return True
            return False

        return False

    mask = [keep_row(r) for r in df.to_dicts()]
    df   = df.filter(pl.Series(mask))

    # PROC SUMMARY: SUM FEEAMTA, FEEAMTB, FEEAMTC by ACCTNO, NOTENO
    feepo = (
        df.group_by(["ACCTNO", "NOTENO"])
          .agg([
              pl.col("FEEAMTA").sum(),
              pl.col("FEEAMTB").sum(),
              pl.col("FEEAMTC").sum(),
          ])
          .sort(["ACCTNO", "NOTENO"])
    )
    return feepo


# ============================================================================
# STEP 3 – LOAD ACCTFILE & BUILD LOAN DATASET
# ============================================================================

def build_loan(ctx: dict, feepo: pl.DataFrame) -> pl.DataFrame:
    """
    Load ACCTFILE parquet, apply BRANCH derivation, output filter,
    merge FEEPO, apply FEEAMT / BALANCE computation logic.
    """
    con  = duckdb.connect()
    loan = con.execute(f"SELECT * FROM read_parquet('{ACCTFILE}')").pl()
    con.close()

    reptdate_int = int(ctx["reptdate"].strftime("%Y%m%d"))

    # BRANCH derivation
    loan = loan.with_columns([
        pl.when(pl.col("PENDBRH") != 0)
          .then(pl.col("PENDBRH"))
          .when(pl.col("NTBRCH")  != 0)
          .then(pl.col("NTBRCH"))
          .otherwise(pl.col("ACCBRCH"))
          .alias("BRANCH"),
        pl.lit(reptdate_int).alias("REPTDATE"),
        pl.lit(ctx["extdate"]).alias("EXTDATE"),
    ])

    # Output filter:
    # ((REVERSED NE 'Y') AND (NOTENO NE .) AND (PAIDIND NE 'P'))
    # OR ((PAIDIND EQ 'P') AND (LASTTRAN EQ REPTDATE))
    loan = loan.filter(
        (
            (pl.col("REVERSED") != "Y") &
            pl.col("NOTENO").is_not_null() &
            (pl.col("PAIDIND") != "P")
        ) |
        (
            (pl.col("PAIDIND") == "P") &
            (pl.col("LASTTRAN") == reptdate_int)
        )
    )

    # Merge FEEPO (left join, keep A)
    loan = loan.join(feepo, on=["ACCTNO", "NOTENO"], how="left")

    # Keep LOANTYPE <= 980
    loan = loan.filter(pl.col("LOANTYPE") <= 980)

    # ── FEEAMT / BALANCE for ACCTNO < 3,000,000,000 ─────────────────────
    def _sum(*cols) -> pl.Expr:
        """SUM of expressions, treating null as 0."""
        exprs = [pl.col(c).fill_null(0) for c in cols]
        result = exprs[0]
        for e in exprs[1:]:
            result = result + e
        return result

    def compute_balance_low(row: dict) -> float:
        lt      = row.get("LOANTYPE", 0) or 0
        ntint   = str(row.get("NTINT", "") or "")
        curbal  = row.get("CURBAL",   0.0) or 0.0
        intearn = row.get("INTEARN",  0.0) or 0.0
        intamt  = row.get("INTAMT",   0.0) or 0.0
        intearn2= row.get("INTEARN2", 0.0) or 0.0
        intearn3= row.get("INTEARN3", 0.0) or 0.0
        accrual = row.get("ACCRUAL",  0.0) or 0.0
        feeamt  = row.get("FEEAMT",   0.0) or 0.0
        feeamta = row.get("FEEAMTA",  0.0) or 0.0
        nfeeamt5= row.get("NFEEAMT5", 0.0) or 0.0
        nfeeamt6= row.get("NFEEAMT6", 0.0) or 0.0
        nfeeamt7= row.get("NFEEAMT7", 0.0) or 0.0

        # FEEAMT adjustments
        low_a = set(range(100,104)) | set(range(110,119)) | set(range(120,126)) | \
                set(range(135,137)) | set(range(180,183)) | set(range(193,197)) | \
                set(range(241,246)) | {127,129,138,270,362,363,569}
        if lt in low_a:
            feeamt = feeamt + feeamta

        mid_a = set(range(300,302)) | {309,310,345} | set(range(900,902)) | \
                {904,905,914,915,919,920}
        if lt in mid_a:
            feeamt = feeamt + nfeeamt5 + nfeeamt6 + nfeeamt7

        mid_b = set(range(225,235)) | set(range(355,360)) | set(range(515,521)) | \
                set(range(524,528)) | set(range(559,562)) | set(range(564,569)) | \
                {200,201,204,205,211,212,214,215,219,220,
                 304,305,315,320,325,330,335,340,350,361,391,
                 500,504,505,509,510,530,555,556,570,910,925}
        if lt in mid_b:
            feeamt = feeamt + nfeeamt5

        # BALANCE
        if ntint == "A":
            if lt == 196:
                return curbal + intearn + (-intamt) + (-intearn2) + intearn3 + feeamt
            else:
                return curbal + intearn + (-intamt) + feeamt
        else:
            return curbal + accrual + feeamt

    def compute_balance_high(row: dict) -> float:
        lt       = row.get("LOANTYPE",  0)   or 0
        ntint    = str(row.get("NTINT", "") or "")
        curbal   = row.get("CURBAL",    0.0) or 0.0
        intearn  = row.get("INTEARN",   0.0) or 0.0
        intamt   = row.get("INTAMT",    0.0) or 0.0
        intearn2 = row.get("INTEARN2",  0.0) or 0.0
        intearn3 = row.get("INTEARN3",  0.0) or 0.0
        accrual  = row.get("ACCRUAL",   0.0) or 0.0
        feeamt   = row.get("FEEAMT",    0.0) or 0.0
        feeamt8  = row.get("FEEAMT8",   0.0) or 0.0
        feeamt4  = row.get("FEEAMT4",   0.0) or 0.0
        feeamt5  = row.get("FEEAMT13",  0.0) or 0.0   # FEEAMT5 = FEEAMT13
        feeamta  = row.get("FEEAMTA",   0.0) or 0.0
        feeamtb  = row.get("FEEAMTB",   0.0) or 0.0

        rc_types = {227,228,230,231,234,235,236,237,238,239,240,241,242,359}
        if lt in rc_types:
            feeamt4 = feeamta
        elif lt in (991, 992, 994, 995):
            feeamt  = feeamt8

        hp_types = {128,130,380,381,700,705,720,725,983,993,996}
        if lt in hp_types:
            if lt in (720, 725):
                feeamt8 = feeamt8 + feeamta + feeamtb
            else:
                feeamt8 = feeamt8 + feeamta
            feeamt = feeamt8

        if ntint == "A":
            return curbal + intearn + (-intamt) + (-intearn2) + intearn3 + feeamt
        else:
            return curbal + accrual + feeamt + feeamt4 + feeamt5

    # Apply row-wise computation
    rows = loan.to_dicts()
    balances = []
    for r in rows:
        acctno = r.get("ACCTNO", 0) or 0
        if acctno < 3_000_000_000:
            balances.append(compute_balance_low(r))
        elif acctno > 8_000_000_000:
            balances.append(compute_balance_high(r))
        else:
            # Mid-range accounts: use simple curbal+accrual
            balances.append((r.get("CURBAL") or 0.0) + (r.get("ACCRUAL") or 0.0))

    loan = loan.with_columns(pl.Series("BALANCE", balances))
    return loan


# ============================================================================
# STEP 4 – PERSIST MIS.LNDLY<REPTDAY>
# ============================================================================

def save_lndly(loan: pl.DataFrame, reptday: str) -> str:
    keep = ["ACCTNO", "NOTENO", "NAME", "BALANCE", "BRANCH",
            "LOANTYPE", "CURBAL", "REPTDATE", "EXTDATE"]
    keep = [c for c in keep if c in loan.columns]
    out  = os.path.join(MIS_DIR, f"LNDLY{reptday}.parquet")
    loan.select(keep).write_parquet(out)
    return out


# ============================================================================
# STEP 5 – LOAD LNDLY (current & previous day)
# ============================================================================

def load_lndly(mis_dir: str, day: str) -> pl.DataFrame:
    path = os.path.join(mis_dir, f"LNDLY{day}.parquet")
    con  = duckdb.connect()
    df   = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()
    return df


# ============================================================================
# STEP 6 – SPLIT BY LOAN TYPE
# ============================================================================

def split_loan_types(df: pl.DataFrame,
                     suffix: str = "") -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Returns (lnnote, hploan, revcre) DataFrames.
    suffix is added to column names when loading previous day to avoid clash.
    """
    revcre = df.filter( pl.col("LOANTYPE").is_in(list(REVCRE_TYPES)))
    hploan = df.filter( pl.col("LOANTYPE").is_in(list(HPLOAN_TYPES)))
    lnnote = df.filter(
        ~pl.col("LOANTYPE").is_in(list(REVCRE_TYPES | HPLOAN_TYPES))
    )
    return lnnote, hploan, revcre


# ============================================================================
# STEP 7 – BRANCH SUMMARY: PROC SUMMARY + BRANCH MERGE
# ============================================================================

def branch_summary(df: pl.DataFrame,
                   branch_df: pl.DataFrame,
                   cnt_col: str,
                   amt_col: str) -> pl.DataFrame:
    """Summarise BALANCE by BRANCH, count accounts, merge branch names."""
    summ = (
        df.group_by("BRANCH")
          .agg([
              pl.col("BALANCE").sum().alias(amt_col),
              pl.len().alias(cnt_col),
          ])
    )
    merged = summ.join(branch_df, on="BRANCH", how="left")
    return merged.sort("BRANCH")


# ============================================================================
# STEP 8 – READ BRANCH FILE
# ============================================================================

def read_branch_file() -> pl.DataFrame:
    """
    Fixed-width:
      @001 BANK       $1.
      @002 BRANCH      3.
      @006 ABBREV     $3.
      @012 BRCHNAME  $30.
    """
    rows = []
    with open(BRANCHF, "r") as fh:
        for line in fh:
            if len(line) < 12:
                continue
            try:
                branch = int(line[1:4].strip())
            except ValueError:
                continue
            bank     = line[0:1]
            abbrev   = line[5:8].strip()
            brchname = line[11:41].strip()
            rows.append({"BANK": bank, "BRANCH": branch,
                         "ABBREV": abbrev, "BRCHNAME": brchname})
    return pl.DataFrame(rows).sort("BRANCH")


# ============================================================================
# STEP 9 – WRITE BRANCH SUMMARY TABLE (Part 1)
# ============================================================================

# Row title space and data column widths to match PROC TABULATE RTS=60
RTS = 60
COL_W_CNT  = 10    # F=10.
COL_W_AMT  = 18    # COMMA18.2
COL_W_VAR  = 20    # COMMA20.2


def _tabulate_branch(df_curr: pl.DataFrame,
                     df_prev: pl.DataFrame,
                     branch_df: pl.DataFrame,
                     cnt_curr: str, amt_curr: str,
                     cnt_prev: str, amt_prev: str,
                     section_title: str,
                     col_hdr: str,
                     lines: list[str]) -> None:
    """
    Render a PROC TABULATE-style table (branch rows + TOTAL) into `lines`.
    """
    curr = branch_summary(df_curr, branch_df, cnt_curr, amt_curr)
    prev = branch_summary(df_prev, branch_df, cnt_prev, amt_prev)

    merged = curr.join(prev, on="BRANCH", how="outer", suffix="_P")
    # Ensure ABBREV and BRCHNAME come from the left side
    if "ABBREV_P" in merged.columns:
        merged = merged.drop("ABBREV_P")
    if "BRCHNAME_P" in merged.columns:
        merged = merged.drop("BRCHNAME_P")

    merged = merged.with_columns([
        pl.col(amt_curr).fill_null(0),
        pl.col(cnt_curr).fill_null(0),
        pl.col(amt_prev).fill_null(0),
        pl.col(cnt_prev).fill_null(0),
    ])
    merged = merged.with_columns(
        (pl.col(amt_curr) - pl.col(amt_prev)).alias("VARIANCE")
    ).sort("BRANCH")

    hdr_box  = "BRANCH".ljust(RTS)
    hdr_data = (
        f"{'NO OF ACCOUNTS':>{COL_W_CNT}}"
        f" {'PREVIOUS AMT':>{COL_W_AMT}}"
        f" {'CURRENT AMT':>{COL_W_AMT}}"
        f" {'VARIANCE':>{COL_W_VAR}}"
    )
    sep = "-" * (RTS + COL_W_CNT + COL_W_AMT * 2 + COL_W_VAR + 4)

    lines.append(f"{ASA_NEWLINE}{section_title}")
    lines.append(f"{ASA_NEWLINE}{hdr_box} {hdr_data}")
    lines.append(f"{ASA_NEWLINE}{col_hdr}")
    lines.append(f"{ASA_NEWLINE}{sep}")

    tot_cnt = tot_prev = tot_curr = tot_var = 0.0
    for row in merged.to_dicts():
        branch   = str(row.get("BRANCH")   or "").rjust(4)
        abbrev   = str(row.get("ABBREV")   or "").ljust(3)
        brchname = str(row.get("BRCHNAME") or "").ljust(30)
        row_hdr  = f"{branch} {abbrev} {brchname}".ljust(RTS)

        cnt  = row.get(cnt_curr) or 0
        prev = row.get(amt_prev) or 0.0
        curr = row.get(amt_curr) or 0.0
        var  = row.get("VARIANCE") or 0.0

        tot_cnt  += cnt
        tot_prev += prev
        tot_curr += curr
        tot_var  += var

        data = (
            f"{fmt_int(cnt, COL_W_CNT)}"
            f" {fmt_comma(prev, COL_W_AMT)}"
            f" {fmt_comma(curr, COL_W_AMT)}"
            f" {fmt_comma20(var)}"
        )
        lines.append(f"{ASA_NEWLINE}{row_hdr} {data}")

    # TOTAL row
    lines.append(f"{ASA_NEWLINE}{sep}")
    total_hdr = "TOTAL".ljust(RTS)
    total_data = (
        f"{fmt_int(tot_cnt, COL_W_CNT)}"
        f" {fmt_comma(tot_prev, COL_W_AMT)}"
        f" {fmt_comma(tot_curr, COL_W_AMT)}"
        f" {fmt_comma20(tot_var)}"
    )
    lines.append(f"{ASA_NEWLINE}{total_hdr} {total_data}")
    lines.append(f"{ASA_NEWLINE}{sep}")


# ============================================================================
# STEP 10 – CUSTOMER MOVEMENT: FILTER >= RM1M, DEDUP, BRANCH MERGE
# ============================================================================

def build_movement(df_curr: pl.DataFrame,
                   df_prev: pl.DataFrame,
                   curr_bal: str,
                   prev_bal: str) -> pl.DataFrame:
    """
    Summarise current and previous balances by ACCTNO, filter ABS >= 1M,
    merge detail back, dedup to first ACCTNO.
    """
    # Summarise current
    c_sum = (
        df_curr.group_by("ACCTNO")
               .agg(pl.col("BALANCE").sum().alias(curr_bal))
    )
    # Summarise previous (renamed BALANCE → PBALANCE)
    if "BALANCE" not in df_prev.columns:
        df_prev = df_prev.rename({"PBALANCE": "BALANCE"}) \
            if "PBALANCE" in df_prev.columns else df_prev
    p_sum = (
        df_prev.group_by("ACCTNO")
               .agg(pl.col("BALANCE").sum().alias(prev_bal))
    )

    merged = c_sum.join(p_sum, on="ACCTNO", how="outer")
    merged = merged.with_columns([
        pl.col(curr_bal).fill_null(0),
        pl.col(prev_bal).fill_null(0),
    ])
    # Filter ABS(curr - prev) >= 1,000,000
    merged = merged.filter(
        (pl.col(curr_bal) - pl.col(prev_bal)).abs() >= 1_000_000
    )

    # Merge back detail (NAME, BRANCH, LOANTYPE) from current day,
    # then previous day; keep first ACCTNO
    detail = pl.concat([df_curr, df_prev], how="diagonal")
    result = merged.join(detail, on="ACCTNO", how="left")
    result = result.unique(subset=["ACCTNO"], keep="first")
    return result


# ============================================================================
# STEP 11 – WRITE MOVEMENT REPORT SECTION (Part 2 – PROC REPORT)
# ============================================================================

MOV_COL_W = {
    "ABBREV"  :  6,
    "NAME"    : 25,
    "ACCTNO"  : 20,
    "CURR"    : 18,
    "PREV"    : 18,
    "MOVEMENT": 18,
}

MOV_HDRS = [
    ("BRANCH", ""),
    ("NAME OF CUSTOMER", ""),
    ("ACCOUNT NO", ""),
    ("CURRENT BALANCE", ""),
    ("PREVIOUS BALANCE", ""),
    ("NET INCREASE/", "(DECREASE)"),
]


def _movement_header() -> list[str]:
    cols  = list(MOV_COL_W.keys())
    widths = list(MOV_COL_W.values())
    h1 = " ".join(MOV_HDRS[i][0].center(widths[i])[:widths[i]] for i in range(len(cols)))
    h2 = " ".join(MOV_HDRS[i][1].center(widths[i])[:widths[i]] for i in range(len(cols)))
    sep = " ".join("-" * w for w in widths)
    return [
        f"{ASA_NEWLINE}{h1}",
        f"{ASA_NEWLINE}{h2}",
        f"{ASA_NEWLINE}{sep}",
    ]


def write_movement_section(df: pl.DataFrame,
                           branch_df: pl.DataFrame,
                           curr_col: str,
                           prev_col: str,
                           title3: str,
                           title4: str,
                           rdate: str,
                           lines: list[str]) -> None:
    """Write one PROC REPORT block for a loan type movement section."""
    # Merge branch
    if "BRANCH" in df.columns:
        df = df.join(branch_df, on="BRANCH", how="left")
    df = df.sort(["BRANCH", "ACCTNO"])

    # Title block
    lines.append(f"{ASA_NEWPAGE}REPORT ID : EIBDLOAN")
    lines.append(f"{ASA_NEWLINE}PUBLIC BANK BERHAD - RETAIL BANKING DIVISION")
    lines.append(f"{ASA_NEWLINE}REPORT TITLE : EIBDLOAN")
    lines.append(f"{ASA_NEWLINE}{title4} : {rdate}")
    lines.append(f"{ASA_NEWLINE}NET INCREASED/(DECREASE) OF RM1 MILLION & ABOVE PER CUSTOMER")
    lines.append(f"{ASA_NEWLINE}")

    lines.extend(_movement_header())
    line_count = 8

    tot_curr = tot_prev = 0.0
    for row in df.to_dicts():
        if line_count >= PAGE_LINES:
            lines.extend(_movement_header())
            line_count = 3

        abbrev = str(row.get("ABBREV") or "")[:6].ljust(6)
        name   = str(row.get("NAME")   or "")[:25].ljust(25)
        acctno = str(row.get("ACCTNO") or "").rjust(20)
        curr   = row.get(curr_col) or 0.0
        prev   = row.get(prev_col) or 0.0
        move   = curr - prev

        tot_curr += curr
        tot_prev += prev

        row_line = (
            f"{abbrev} {name} {acctno} "
            f"{fmt_comma(curr)} {fmt_comma(prev)} {fmt_comma(move)}"
        )
        lines.append(f"{ASA_NEWLINE}{row_line}")
        line_count += 1

    # RBREAK AFTER / DUL OL SUMMARIZE
    sep_dbl = " ".join("=" * w for w in MOV_COL_W.values())
    sep_sgl = " ".join("-" * w for w in MOV_COL_W.values())
    tot_move = tot_curr - tot_prev
    blank_w  = MOV_COL_W["ABBREV"] + 1 + MOV_COL_W["NAME"] + 1 + MOV_COL_W["ACCTNO"]
    summary  = (
        f"{' ' * blank_w} "
        f"{fmt_comma(tot_curr)} {fmt_comma(tot_prev)} {fmt_comma(tot_move)}"
    )
    lines.append(f"{ASA_NEWLINE}{sep_sgl}")
    lines.append(f"{ASA_NEWLINE}{sep_dbl}")
    lines.append(f"{ASA_NEWLINE}{summary}")
    lines.append(f"{ASA_NEWLINE}{sep_dbl}")


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    print("EIBDLOAN – Daily Loans Summarised by Branch starting...")

    # Step 1: derive dates
    ctx = derive_report_date()
    print(f"  Report date : {ctx['rdate']}")

    # Step 2: fee-plan data
    feepo = build_feepo()

    # Step 3: build LOAN dataset with BALANCE
    loan  = build_loan(ctx, feepo)

    # Step 4: persist MIS.LNDLY<REPTDAY>
    save_lndly(loan, ctx["reptday"])

    # Step 5: load current and previous day LNDLY
    lndly_curr = load_lndly(MIS_DIR,  ctx["reptday"])
    lndly_prev = load_lndly(MIS1_DIR, ctx["prevday"])

    # Step 6: split by loan type (current day)
    lnnote, hploan, revcre     = split_loan_types(lndly_curr)
    plnnote, phploan, prevcre  = split_loan_types(lndly_prev)

    # Step 8: branch reference
    branch_df = read_branch_file()

    lines: list[str] = []

    # ── PART 1: BRANCH SUMMARY REPORTS ───────────────────────────────────

    # Title block for Part 1
    lines.append(f"{ASA_NEWPAGE}REPORT ID : EIBDLOAN")
    lines.append(f"{ASA_NEWLINE}PUBLIC BANK BERHAD")
    lines.append(f"{ASA_NEWLINE}BRANCH SUMMARY REPORT ON DAILY TERM LOAN OUTSTANDING(RM)")
    lines.append(f"{ASA_NEWLINE}AS AT {ctx['rdate']}")
    lines.append(f"{ASA_NEWLINE}")

    _tabulate_branch(
        lnnote, plnnote, branch_df,
        "NOACCT", "BRLNAMT", "PNOACCT", "PBRLNAMT",
        "TERM LOAN",
        "OUTSTANDING TERM LOAN (RM)",
        lines,
    )

    lines.append(f"{ASA_NEWLINE}")
    lines.append(f"{ASA_NEWLINE}BRANCH SUMMARY REPORT ON DAILY REVOLVING CREDIT OUTSTANDING(RM)")
    lines.append(f"{ASA_NEWLINE}AS AT {ctx['rdate']}")
    lines.append(f"{ASA_NEWLINE}")

    _tabulate_branch(
        revcre, prevcre, branch_df,
        "REVACC", "BRRVAMT", "PREVACC", "PBRRVAMT",
        "REVOLVING CREDIT",
        "OUTSTANDING REVOLVING CREDIT (RM)",
        lines,
    )

    lines.append(f"{ASA_NEWLINE}")
    lines.append(f"{ASA_NEWLINE}BRANCH SUMMARY REPORT ON DAILY HP OUTSTANDING(RM)")
    lines.append(f"{ASA_NEWLINE}AS AT {ctx['rdate']}")
    lines.append(f"{ASA_NEWLINE}")

    _tabulate_branch(
        hploan, phploan, branch_df,
        "HPACC", "BRHPAMT", "PHPACC", "PBRHPAMT",
        "HP",
        "OUTSTANDING HP (RM)",
        lines,
    )

    # ── PART 2: CUSTOMER MOVEMENT >= RM1M ────────────────────────────────

    # Rename BALANCE in previous-day frames to PBALANCE for merging
    plnnote_r = plnnote.rename({"BALANCE": "PBALANCE"}) if "BALANCE" in plnnote.columns else plnnote
    prevcre_r = prevcre.rename({"BALANCE": "PBALANCE"}) if "BALANCE" in prevcre.columns else prevcre
    phploan_r = phploan.rename({"BALANCE": "PBALANCE"}) if "BALANCE" in phploan.columns else phploan

    # Term Loan movement
    dmloan = build_movement(lnnote, plnnote_r, "DLTOTOL", "PDLTOTOL")
    write_movement_section(
        dmloan, branch_df, "DLTOTOL", "PDLTOTOL",
        "BRANCH SUMMARY REPORT ON DAILY TERM LOAN OUTSTANDING(RM)",
        "DAILY MOVEMENT IN BANK'S TERM LOAN ACCOUNTS AS AT",
        ctx["rdate"], lines,
    )

    # Revolving Credit movement
    dmcred = build_movement(revcre, prevcre_r, "DRTOTOL", "PDRTOTOL")
    write_movement_section(
        dmcred, branch_df, "DRTOTOL", "PDRTOTOL",
        "BRANCH SUMMARY REPORT ON DAILY REVOLVING CREDIT OUTSTANDING(RM)",
        "DAILY MOVEMENT IN BANK'S REVOLVING CREDIT ACCOUNTS AS AT",
        ctx["rdate"], lines,
    )

    # HP movement
    dmhp = build_movement(hploan, phploan_r, "DHPTOTOL", "PDHPTOTO")
    write_movement_section(
        dmhp, branch_df, "DHPTOTOL", "PDHPTOTO",
        "BRANCH SUMMARY REPORT ON DAILY HP OUTSTANDING(RM)",
        "DAILY MOVEMENT IN BANK'S HP ACCOUNTS AS AT",
        ctx["rdate"], lines,
    )

    # Write output
    with open(REPORT_FILE, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"Report written: {REPORT_FILE}")

    print("EIBDLOAN – Done.")


if __name__ == "__main__":
    main()
