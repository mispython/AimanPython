#!/usr/bin/env python3
"""
Program  : EIVWSTAF.py
Purpose  : Weekly Listing for Staff New Loan and Paid Loan (PIVB)
           Report 1: Staff Paid Loan List (LNSETTLE)
           Report 2: Staff New Loan List (LNRPT1A)
           Report 3: Staff Migration Loan List (LNRPT1B)
           Report 4: Staff Full Release Loan List (LNRPT1C)
           REPORT ID: EIVWSTAF (PIVB)
"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
# No PBBLNFMT or other dependency imports required.
# No PBBLNFMT format functions are called anywhere in this program.

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime, timedelta

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR        = Path("/data")
MNILN_DIR       = BASE_DIR / "mniln"
IMNILN_DIR      = BASE_DIR / "imniln"
PAY_DIR         = BASE_DIR / "pay"
IPAY_DIR        = BASE_DIR / "ipay"
LNHIST_DIR      = BASE_DIR / "lnhist"      # LIBNAME LNHIST

REPTDATE_FILE   = MNILN_DIR / "reptdate.parquet"
LNNOTE_FILE     = MNILN_DIR / "lnnote.parquet"
LNCOMM_FILE     = MNILN_DIR / "lncomm.parquet"
ILNNOTE_FILE    = IMNILN_DIR / "lnnote.parquet"
ILNCOMM_FILE    = IMNILN_DIR / "lncomm.parquet"
SVBASE_FILE     = LNHIST_DIR / "svbase.parquet"  # LNHIST.SVBASE (PIVB history base)

OUTPUT_DIR      = BASE_DIR / "output"
OUTPUT_REPORT   = OUTPUT_DIR / "eivwstaf_report.txt"
# PROC APPEND target — updated svbase written back
OUTPUT_SVBASE   = LNHIST_DIR / "svbase.parquet"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# REPORT / PAGE CONSTANTS
# ============================================================================

PAGE_LENGTH = 60    # OPTIONS PS=60
LINE_WIDTH  = 132   # OPTIONS LS=132

# ============================================================================
# LOANTYPE FILTER for EIVWSTAF (PIVB)
# WHERE LOANTYPE IN (62,70,71,72,73,74,75,76,77,78,106,107,108)
# Both MNILN and IMNILN use the same filter (no COSTCTR distinction)
# ============================================================================

PIVB_LOANTYPES = {62, 70, 71, 72, 73, 74, 75, 76, 77, 78, 106, 107, 108}


def pivb_loantype_filter(loantype, costctr=None) -> bool:
    """Replicate PIVB LNNOTE WHERE filter."""
    if loantype is None:
        return False
    return int(loantype) in PIVB_LOANTYPES


# ============================================================================
# HELPER FUNCTIONS (identical to EIBWSTAF)
# ============================================================================

SAS_EPOCH = date(1960, 1, 1)


def sas_date_to_python(val) -> date | None:
    if val is None:
        return None
    try:
        n = int(float(val))
        if n == 0:
            return None
        return SAS_EPOCH + timedelta(days=n)
    except (TypeError, ValueError, OverflowError):
        return None


def parse_sas_z11_date(val) -> date | None:
    """Parse LASTTRAN / ISSUEDT packed numeric Z11. -> date (MMDDYYYY in chars 0-7)."""
    if val is None:
        return None
    try:
        s = str(int(float(val))).zfill(11)
        mm = int(s[0:2]); dd = int(s[2:4]); yy = int(s[4:8])
        if mm == 0 or dd == 0 or yy == 0:
            return None
        return date(yy, mm, dd)
    except (TypeError, ValueError, OverflowError):
        return None


def parse_payeffdt(val) -> str:
    """PAYEFF = chars[9:11]||'/'||chars[7:9]||'/'||chars[2:4] from Z11."""
    if val is None:
        return ""
    try:
        s = str(int(float(val))).zfill(11)
        return f"{s[9:11]}/{s[7:9]}/{s[2:4]}"
    except (TypeError, ValueError):
        return ""


def parse_freleas(val) -> date | None:
    """FULRELDTE from FRELEAS Z11. chars[0:8] as MMDDYYYY."""
    if val is None or val == 0:
        return None
    try:
        s = str(int(float(val))).zfill(11)
        mm = int(s[0:2]); dd = int(s[2:4]); yy = int(s[4:8])
        if mm == 0 or dd == 0 or yy == 0:
            return None
        return date(yy, mm, dd)
    except (TypeError, ValueError, OverflowError):
        return None


def fmt_ddmmyy10(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def fmt_ddmmyy8(d) -> str:
    if d is None:
        return " " * 8
    if isinstance(d, datetime):
        d = d.date()
    if not isinstance(d, date):
        return " " * 8
    return d.strftime("%d/%m/%y")


def fmt_z2(n: int) -> str:
    return f"{n:02d}"


def fmt_year2(d: date) -> str:
    return d.strftime("%y")


def fmt_comma14_2(val) -> str:
    if val is None:
        return " " * 14
    try:
        return f"{float(val):,.2f}".rjust(14)
    except (TypeError, ValueError):
        return " " * 14


def fmt_comma12_2(val) -> str:
    if val is None:
        return " " * 12
    try:
        return f"{float(val):,.2f}".rjust(12)
    except (TypeError, ValueError):
        return " " * 12


def fmt_comma8(val) -> str:
    if val is None:
        return " " * 8
    try:
        return f"{int(val):,}".rjust(8)
    except (TypeError, ValueError):
        return " " * 8


def fmt_4_2(val) -> str:
    if val is None:
        return " " * 4
    try:
        return f"{float(val):.2f}".rjust(4)
    except (TypeError, ValueError):
        return " " * 4


def to_sas_numeric(d: date) -> int:
    return (d - SAS_EPOCH).days


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

PREVDATE = date(REPTDATE.year, REPTDATE.month, 1) - timedelta(days=1)

day = REPTDATE.day
if day == 8:
    SDD, WK = 1, "01"
elif day == 15:
    SDD, WK = 9, "02"
elif day == 22:
    SDD, WK = 16, "03"
else:
    SDD, WK = 23, "04"

MMP = REPTDATE.month
YYP = REPTDATE.year

PDATE_DT = date(YYP, MMP, 1)
SDATE_DT = date(YYP, MMP, SDD)

EDATE    = to_sas_numeric(REPTDATE)
PDATE    = to_sas_numeric(PDATE_DT)
SDATE    = to_sas_numeric(SDATE_DT)
NOWK     = WK
STRDAY   = SDD
RDATE    = fmt_ddmmyy10(REPTDATE)
REPTMON  = fmt_z2(REPTDATE.month)
REPTDAY  = REPTDATE.day
REPTMTH  = REPTDATE.month
REPTYEAR = REPTDATE.year

if WK == "04":
    REPTMM = fmt_z2(REPTDATE.month)
    REPTYY = fmt_year2(REPTDATE)
else:
    REPTMM = fmt_z2(PREVDATE.month)
    REPTYY = fmt_year2(PREVDATE)

# ============================================================================
# STEP 2: LOAD LNNOTE + LNCOMM WITH APPRLIMT DERIVATION
# ============================================================================

def load_and_merge_notes(
    lnnote_file: Path,
    lncomm_file: Path,
    loantype_filter_fn,
) -> pl.DataFrame:
    """
    Replicate:
      PROC SORT LNNOTE WHERE filter; BY ACCTNO COMMNO;
      PROC SORT LNCOMM; BY ACCTNO COMMNO;
      DATA: MERGE LNNOTE(IN=A) LNCOMM; BY ACCTNO COMMNO; IF A;
      APPRLIMT derivation
    """
    lnnote_raw = con.execute(
        f"SELECT * FROM read_parquet('{lnnote_file}')"
    ).pl()
    lncomm_raw = con.execute(
        f"SELECT * FROM read_parquet('{lncomm_file}')"
    ).pl()

    filtered_rows = [
        r for r in lnnote_raw.iter_rows(named=True)
        if loantype_filter_fn(r.get("LOANTYPE"), r.get("COSTCTR"))
    ]
    if not filtered_rows:
        return pl.DataFrame()

    lnnote_filt   = pl.DataFrame(filtered_rows).sort(["ACCTNO", "COMMNO"])
    lncomm_sorted = lncomm_raw.sort(["ACCTNO", "COMMNO"])

    merged = lnnote_filt.join(
        lncomm_sorted, on=["ACCTNO", "COMMNO"], how="left", suffix="_COMM"
    )

    def derive_apprlimt(r: dict) -> float:
        commno  = int(r.get("COMMNO") or 0)
        revovli = str(r.get("REVOVLI") or "").strip()
        corgamt = float(r.get("CORGAMT") or 0)
        ccuramt = float(r.get("CCURAMT") or 0)
        orgbal  = float(r.get("ORGBAL")  or 0)
        if commno > 0:
            return corgamt if revovli == "N" else ccuramt
        return orgbal

    rows = []
    for r in merged.iter_rows(named=True):
        r["APPRLIMT"] = derive_apprlimt(r)
        rows.append(r)

    return pl.DataFrame(rows) if rows else pl.DataFrame()


lnnote  = load_and_merge_notes(LNNOTE_FILE,  LNCOMM_FILE,  pivb_loantype_filter)
ilnnote = load_and_merge_notes(ILNNOTE_FILE, ILNCOMM_FILE, pivb_loantype_filter)

# ============================================================================
# STEP 3: DATA LOAN &INTGRVAR
# ============================================================================

KEEP_COLS = {
    "LOANTYPE", "NTBRCH", "ORGTYPE", "ACCTNO", "CURBAL",
    "NOTENO", "NAME", "APPRLIMT", "ISSDTE", "PAIDIND", "BLDATE",
    "BILPAY", "PAYAMT", "INTRATE", "STAFFNO", "PAYEFF",
    "ORGBAL", "LASTTRAN", "LSTTRNAM", "LSTTRNCD", "NOOFAC",
    "RESTIND", "FLAG1", "FULRELDTE",
}

combined_notes = pl.concat(
    [df for df in [lnnote, ilnnote] if not df.is_empty()],
    how="diagonal"
)


def build_loan(df: pl.DataFrame) -> pl.DataFrame:
    rows = df.to_dicts()
    out  = []
    for r in rows:
        r["LASTTRAN"]  = parse_sas_z11_date(r.get("LASTTRAN"))
        r["ISSDTE"]    = parse_sas_z11_date(r.get("ISSUEDT"))
        r["PAYEFF"]    = parse_payeffdt(r.get("PAYEFFDT"))
        freleas        = r.get("FRELEAS")
        r["FULRELDTE"] = parse_freleas(freleas) if (
            freleas is not None and freleas != 0
        ) else None
        r["NOOFAC"]    = 1
        kept = {k: r.get(k) for k in KEEP_COLS if k in r or k in (
            "ISSDTE", "LASTTRAN", "PAYEFF", "FULRELDTE", "NOOFAC"
        )}
        out.append(kept)
    return pl.DataFrame(out) if out else pl.DataFrame()


loan = build_loan(combined_notes).sort(["ACCTNO", "NOTENO"])

# ============================================================================
# STEP 4: DATA LNSETTLE
# ============================================================================

settle_rows = []
for r in loan.iter_rows(named=True):
    paidind  = str(r.get("PAIDIND") or "").strip()
    lasttran = r.get("LASTTRAN")
    if paidind not in ("P", "C"):
        continue
    if lasttran is None:
        continue
    if isinstance(lasttran, datetime):
        lasttran = lasttran.date()
    if not (STRDAY <= lasttran.day  <= REPTDAY
            and lasttran.month == REPTMTH
            and lasttran.year  == REPTYEAR):
        continue

    r["SETTDT"]  = r.pop("LASTTRAN", None)
    r["SETTAMT"] = r.pop("LSTTRNAM", None)
    r["SETTCD"]  = r.pop("LSTTRNCD", None)
    lsttrncd     = r.get("SETTCD")
    r["LSTRNDSC"] = (
        "LAST TRANCODE EQ 652" if (lsttrncd is not None and int(lsttrncd) == 652)
        else "LAST TRANCODE NE 652"
    )
    settle_rows.append(r)

lnsettle = (
    pl.DataFrame(settle_rows).sort(["LSTRNDSC", "LOANTYPE", "NTBRCH"])
    if settle_rows else pl.DataFrame()
)

# ============================================================================
# STEP 5: LOAD LNHIST.SVBASE (PIVB history base)
# Note: PIVB uses SVBASE (vs STBASE for PBB)
# ============================================================================

hist = pl.DataFrame()
if SVBASE_FILE.exists():
    hist = con.execute(
        f"SELECT ACCTNO, NOTENO FROM read_parquet('{SVBASE_FILE}')"
    ).pl().sort(["ACCTNO", "NOTENO"]).unique(subset=["ACCTNO", "NOTENO"], keep="first")

# ============================================================================
# STEP 6: DATA LNRELES + LNRELS1
# ============================================================================

loan_sorted = loan.sort(["ACCTNO", "NOTENO"])
hist_keys   = set(
    zip(hist["ACCTNO"].to_list(), hist["NOTENO"].to_list())
) if not hist.is_empty() else set()

lnreles_rows = []
lnrels1_rows = []

for r in loan_sorted.iter_rows(named=True):
    acctno = r.get("ACCTNO")
    noteno = r.get("NOTENO")
    if (acctno, noteno) in hist_keys:
        continue

    fulreldte = r.get("FULRELDTE")
    flag1     = str(r.get("FLAG1")   or "").strip()
    restind   = str(r.get("RESTIND") or "").strip()
    nmn       = None

    if fulreldte is not None:
        frd_sas = to_sas_numeric(fulreldte)
        if PDATE <= frd_sas <= EDATE and flag1 == "M" and restind == "M":
            nmn = "Y"
            r["ISSDTE"] = fulreldte

    issdte_val = r.get("ISSDTE")
    if isinstance(issdte_val, datetime):
        issdte_val = issdte_val.date()
    issdte_sas = to_sas_numeric(issdte_val) if issdte_val is not None else None

    passes = nmn == "Y" or (
        issdte_sas is not None and PDATE <= issdte_sas <= EDATE
    )
    if not passes:
        continue

    r["NMN"] = nmn
    # PIVB uses NWI='Y' instead of NID='Y'
    r["NWI"] = "Y"
    lnreles_rows.append(r)
    lnrels1_rows.append({"ACCTNO": acctno, "NOTENO": noteno})

lnreles = (
    pl.DataFrame(lnreles_rows).sort("ACCTNO")
    if lnreles_rows else pl.DataFrame()
)

# PROC APPEND DATA=LNRELS1 BASE=LNHIST.SVBASE
if lnrels1_rows:
    new_keys = pl.DataFrame(lnrels1_rows)
    if not hist.is_empty():
        updated_svbase = pl.concat([hist, new_keys], how="diagonal").unique(
            subset=["ACCTNO", "NOTENO"], keep="first"
        )
    else:
        updated_svbase = new_keys
    updated_svbase.write_parquet(OUTPUT_SVBASE)

# ============================================================================
# STEP 7: LNRPT1A + LNRPT1B SPLIT
#   EIVWSTAF (PIVB) differs from EIBWSTAF (PBB) in the (A AND B) block:
#   PBB:  IF (ORGBAL=SETTAMT) -> DELETE; IF NID='Y' AND PAIDIND NE 'P' -> LNRPT1A
#   PIVB: IF NWI='Y' -> LNRPT1A  (no ORGBAL=SETTAMT DELETE branch)
#         IF (ORGBAL=SETTAMT) OR NMN='Y' -> LNRPT1B
# ============================================================================

settle_keys = set(lnsettle["ACCTNO"].to_list()) if not lnsettle.is_empty() else set()
settle_map  = {
    r["ACCTNO"]: r
    for r in lnsettle.iter_rows(named=True)
} if not lnsettle.is_empty() else {}

lnrpt1a_rows = []
lnrpt1b_rows = []

for r in (lnreles.iter_rows(named=True) if not lnreles.is_empty() else []):
    acctno = r.get("ACCTNO")
    nmn    = r.get("NMN")
    nwi    = r.get("NWI")
    in_b   = acctno in settle_keys

    if not in_b:
        # A AND NOT B
        if nmn == "Y":
            lnrpt1b_rows.append(r)
        else:
            lnrpt1a_rows.append(r)
    else:
        # A AND B — PIVB logic
        settle_row = settle_map.get(acctno, {})
        orgbal  = float(r.get("ORGBAL")   or 0)
        settamt = float(settle_row.get("SETTAMT") or 0)

        # IF NWI='Y' -> LNRPT1A
        if nwi == "Y":
            lnrpt1a_rows.append(r)
        # IF (ORGBAL=SETTAMT) OR NMN='Y' -> LNRPT1B
        if (orgbal == settamt) or (nmn == "Y"):
            lnrpt1b_rows.append(r)

lnrpt1a = pl.DataFrame(lnrpt1a_rows) if lnrpt1a_rows else pl.DataFrame()
lnrpt1b = pl.DataFrame(lnrpt1b_rows) if lnrpt1b_rows else pl.DataFrame()

# ============================================================================
# STEP 8: DATA LNPAY
# ============================================================================

lnpay_file  = PAY_DIR  / f"lnpay{NOWK}.parquet"
ilnpay_file = IPAY_DIR / f"ilnpay{NOWK}.parquet"

lnpay_parts = []
for fp in [lnpay_file, ilnpay_file]:
    if fp.exists():
        part = con.execute(
            f"SELECT * FROM read_parquet('{fp}') WHERE PAYAMT != 0"
        ).pl()
        lnpay_parts.append(part)

if lnpay_parts:
    lnpay_raw = pl.concat(lnpay_parts, how="diagonal")
    lnpay_rows = []
    for r in lnpay_raw.iter_rows(named=True):
        effdate = r.get("EFFDATE")
        if isinstance(effdate, (int, float)):
            effdate = sas_date_to_python(effdate)
        elif isinstance(effdate, datetime):
            effdate = effdate.date()
        payeffmm = fmt_z2(effdate.month) if effdate else "  "
        payeffyy = fmt_year2(effdate)    if effdate else "  "
        payeff   = f"99/{payeffmm}/{payeffyy}"
        lnpay_rows.append({
            "ACCTNO":  r.get("ACCTNO"),
            "NOTENO":  r.get("NOTENO"),
            "PAYEFF":  payeff,
            "PAYAMT":  r.get("PAYAMT"),
        })
    lnpay = (
        pl.DataFrame(lnpay_rows)
          .sort(["ACCTNO", "NOTENO", "PAYEFF"])
          .unique(subset=["ACCTNO", "NOTENO"], keep="first")
    )
else:
    lnpay = pl.DataFrame(
        {"ACCTNO": [], "NOTENO": [], "PAYEFF": [], "PAYAMT": []}
    )

# ============================================================================
# STEP 9: MERGE LNRPT1B WITH LNPAY -> LNRPT1B / LNRPT1C
# ============================================================================

if not lnrpt1b.is_empty():
    lnrpt1b_sorted = lnrpt1b.sort(["ACCTNO", "NOTENO"])
    lnpay_sorted   = lnpay.sort(["ACCTNO", "NOTENO"])
    merged_b = lnrpt1b_sorted.join(
        lnpay_sorted, on=["ACCTNO", "NOTENO"], how="left", suffix="_PAY"
    )
    for col in ["PAYEFF", "PAYAMT"]:
        pay_col = f"{col}_PAY"
        if pay_col in merged_b.columns:
            merged_b = merged_b.with_columns(
                pl.when(pl.col(pay_col).is_not_null())
                  .then(pl.col(pay_col))
                  .otherwise(pl.col(col))
                  .alias(col)
            ).drop(pay_col)

    rpt1b_final = []
    rpt1c_final = []
    for r in merged_b.iter_rows(named=True):
        if r.get("NMN") == "Y":
            rpt1c_final.append(r)
        else:
            rpt1b_final.append(r)
    lnrpt1b = pl.DataFrame(rpt1b_final) if rpt1b_final else pl.DataFrame()
    lnrpt1c = pl.DataFrame(rpt1c_final) if rpt1c_final else pl.DataFrame()
else:
    lnrpt1c = pl.DataFrame()

lnrpt1a = lnrpt1a.sort(["LOANTYPE", "NTBRCH"]) if not lnrpt1a.is_empty() else lnrpt1a
lnrpt1b = lnrpt1b.sort(["LOANTYPE", "NTBRCH"]) if not lnrpt1b.is_empty() else lnrpt1b
lnrpt1c = lnrpt1c.sort(["LOANTYPE", "NTBRCH"]) if not lnrpt1c.is_empty() else lnrpt1c

# ============================================================================
# REPORT RENDERING (identical structure to EIBWSTAF, PIVB titles/IDs)
# ============================================================================

DASH_128 = "-" * 128
DASH_130 = "-" * 130
DASH_123 = "-" * 123
DASH_121 = "-" * 121
EQ_128   = "=" * 128


def new_page(lines: list, title1: str, title2: str,
             col_hdr: str, line_count_ref: list) -> None:
    lines.append(f"1{title1.center(LINE_WIDTH)}")
    lines.append(f" {title2.center(LINE_WIDTH)}")
    lines.append(f" ")
    lines.append(f" {col_hdr}")
    lines.append(f" ")
    line_count_ref[0] = 5


def render_settle_report(
    df: pl.DataFrame, title2: str, lines: list
) -> None:
    """Replicate PROC REPORT LNSETTLE for PIVB."""
    title1  = "REPORT ID : EIVWSTAF (PIVB)"
    col_hdr = (
        f" {'FAC':>3}  {'BRH CDE':>3}  {'ORG TYP':>3}  "
        f"{'STAFF NUM':>5}  {'A/C NO':>10}  {'NOTE NO':>5}  "
        f"{'NAME':<24}  {'APPROVED LIMIT':>14}  "
        f"{'PAID DATE':>10}  {'SETTLEMENT AMOUNT':>12}  "
        f"{'MONTHLY REPAYMENT':>12}  {'LST TRN CDE':>3}"
    )
    line_count_ref = [PAGE_LENGTH]
    gt_noofac = gt_appr = gt_sett = gt_pay = 0.0
    lstrndsc_vals = sorted(df["LSTRNDSC"].unique().to_list())

    for lstrndsc_val in lstrndsc_vals:
        sub_df    = df.filter(pl.col("LSTRNDSC") == lstrndsc_val)
        sl_noofac = sl_appr = sl_sett = sl_pay = 0.0
        lt_vals   = sorted(sub_df["LOANTYPE"].unique().to_list())

        for lt_val in lt_vals:
            lt_df     = sub_df.filter(pl.col("LOANTYPE") == lt_val)
            ft_noofac = ft_appr = ft_sett = ft_pay = 0.0
            brh_vals  = sorted(lt_df["NTBRCH"].unique().to_list())

            for brh_val in brh_vals:
                brh_df   = lt_df.filter(pl.col("NTBRCH") == brh_val)
                b_noofac = b_appr = b_sett = b_pay = 0.0

                for row in brh_df.iter_rows(named=True):
                    if line_count_ref[0] >= PAGE_LENGTH - 4:
                        new_page(lines, title1, title2, col_hdr, line_count_ref)
                    lines.append(
                        f" {str(lt_val or ''):>3}  "
                        f"{str(brh_val or ''):>3}  "
                        f"{str(row.get('ORGTYPE') or ''):>3}  "
                        f"{str(row.get('STAFFNO') or ''):>5}  "
                        f"{str(row.get('ACCTNO') or ''):>10}  "
                        f"{str(row.get('NOTENO') or ''):>5}  "
                        f"{str(row.get('NAME') or '')[:24]:<24}  "
                        f"{fmt_comma14_2(row.get('APPRLIMT'))}  "
                        f"{fmt_ddmmyy8(row.get('SETTDT')):>10}  "
                        f"{fmt_comma12_2(row.get('SETTAMT'))}  "
                        f"{fmt_comma12_2(row.get('PAYAMT'))}  "
                        f"{str(row.get('SETTCD') or ''):>3}"
                    )
                    line_count_ref[0] += 1
                    b_noofac += 1
                    b_appr   += float(row.get("APPRLIMT") or 0)
                    b_sett   += float(row.get("SETTAMT")  or 0)
                    b_pay    += float(row.get("PAYAMT")   or 0)

                # BREAK AFTER NTBRCH /SKIP
                lines.append(f" ")
                lines.append(f"        {DASH_121}")
                lines.append(
                    f"                NO OF A/C :"
                    f"  {fmt_comma8(b_noofac)}"
                    f"{'':>41}{fmt_comma14_2(b_appr)}"
                    f"  {fmt_comma12_2(b_sett)}"
                    f"  {fmt_comma12_2(b_pay)}"
                )
                lines.append(f" ")
                line_count_ref[0] += 4
                ft_noofac += b_noofac; ft_appr += b_appr
                ft_sett   += b_sett;   ft_pay  += b_pay

            # BREAK AFTER LOANTYPE /SKIP
            lines.append(f" ")
            lines.append(f" {DASH_128}")
            lines.append(
                f" FAC TOTAL        NO OF A/C :"
                f"  {fmt_comma8(ft_noofac)}"
                f"{'':>41}{fmt_comma14_2(ft_appr)}"
                f"  {fmt_comma12_2(ft_sett)}"
                f"  {fmt_comma12_2(ft_pay)}"
            )
            lines.append(f" {DASH_128}")
            lines.append(f" ")
            line_count_ref[0] += 5
            sl_noofac += ft_noofac; sl_appr += ft_appr
            sl_sett   += ft_sett;   sl_pay  += ft_pay

        # BREAK AFTER LSTRNDSC /SKIP
        lines.append(f" ")
        lines.append(f" {EQ_128}")
        lines.append(
            f" SUB TOTAL        NO OF A/C :"
            f"  {fmt_comma8(sl_noofac)}"
            f"{'':>41}{fmt_comma14_2(sl_appr)}"
            f"  {fmt_comma12_2(sl_sett)}"
            f"  {fmt_comma12_2(sl_pay)}"
        )
        lines.append(f" {EQ_128}")
        lines.append(f" ")
        line_count_ref[0] += 5
        gt_noofac += sl_noofac; gt_appr += sl_appr
        gt_sett   += sl_sett;   gt_pay  += sl_pay

    # RBREAK AFTER /SKIP
    lines.append(f" ")
    lines.append(
        f" GRAND TOTAL      NO OF A/C :  "
        f"{fmt_comma8(gt_noofac)}"
        f"{'':>41}{fmt_comma14_2(gt_appr)}"
        f"  {fmt_comma12_2(gt_sett)}"
        f"  {fmt_comma12_2(gt_pay)}"
    )
    lines.append(f" {EQ_128}")


def render_loan_report(
    df: pl.DataFrame, title1: str, title2: str,
    issdte_label: str, lines: list
) -> None:
    """Replicate PROC REPORT for LNRPT1A / LNRPT1B / LNRPT1C (PIVB)."""
    col_hdr = (
        f" {'FAC':>3}  {'BR CODE':>4}  {'ORG. TYPE':>4}  "
        f"{'EMP.NO':>6}  {'A/C NO':>10}  {'NOTE NO':>5}  "
        f"{'NAME':<24}  {'APPROVED LIMIT':>14}  "
        f"{issdte_label:>10}  {'PAYMENT EFF. DATE':>9}  "
        f"{'PAYMENT AMOUNT':>14}  {'INT. RATE':>4}"
    )
    line_count_ref = [PAGE_LENGTH]
    gt_noofac = gt_appr = gt_pay = 0.0
    lt_vals   = sorted(df["LOANTYPE"].unique().to_list())

    for lt_val in lt_vals:
        lt_df     = df.filter(pl.col("LOANTYPE") == lt_val)
        st_noofac = st_appr = st_pay = 0.0
        brh_vals  = sorted(lt_df["NTBRCH"].unique().to_list())

        for brh_val in brh_vals:
            brh_df   = lt_df.filter(pl.col("NTBRCH") == brh_val)
            b_noofac = b_appr = b_pay = 0.0

            for row in brh_df.iter_rows(named=True):
                if line_count_ref[0] >= PAGE_LENGTH - 4:
                    new_page(lines, title1, title2, col_hdr, line_count_ref)
                lines.append(
                    f" {str(lt_val or ''):>3}  "
                    f"{str(brh_val or ''):>4}  "
                    f"{str(row.get('ORGTYPE') or ''):>4}  "
                    f"{str(row.get('STAFFNO') or ''):>6}  "
                    f"{str(row.get('ACCTNO') or ''):>10}  "
                    f"{str(row.get('NOTENO') or ''):>5}  "
                    f"{str(row.get('NAME') or '')[:24]:<24}  "
                    f"{fmt_comma14_2(row.get('APPRLIMT'))}  "
                    f"{fmt_ddmmyy8(row.get('ISSDTE')):>10}  "
                    f"{str(row.get('PAYEFF') or ''):>9}  "
                    f"{fmt_comma14_2(row.get('PAYAMT'))}  "
                    f"{fmt_4_2(row.get('INTRATE'))}"
                )
                line_count_ref[0] += 1
                b_noofac += 1
                b_appr   += float(row.get("APPRLIMT") or 0)
                b_pay    += float(row.get("PAYAMT")   or 0)

            # BREAK AFTER NTBRCH /SKIP
            lines.append(f" ")
            lines.append(f"        {DASH_123}")
            lines.append(
                f"              NO OF A/C :"
                f"  {fmt_comma8(b_noofac)}"
                f"{'':>46}{fmt_comma14_2(b_appr)}"
                f"  {fmt_comma14_2(b_pay)}"
            )
            lines.append(f" ")
            line_count_ref[0] += 4
            st_noofac += b_noofac; st_appr += b_appr; st_pay += b_pay

        # BREAK AFTER LOANTYPE /SKIP
        lines.append(f" ")
        lines.append(f" {DASH_130}")
        lines.append(
            f" SUB TOTAL    NO OF A/C :"
            f"  {fmt_comma8(st_noofac)}"
            f"{'':>46}{fmt_comma14_2(st_appr)}"
            f"  {fmt_comma14_2(st_pay)}"
        )
        lines.append(f" {DASH_130}")
        lines.append(f" ")
        line_count_ref[0] += 5
        gt_noofac += st_noofac; gt_appr += st_appr; gt_pay += st_pay

    # RBREAK AFTER /SKIP
    lines.append(f" ")
    lines.append(f" {DASH_130}")
    lines.append(
        f" GRAND TOTAL  NO OF A/C :  "
        f"{fmt_comma8(gt_noofac)}"
        f"{'':>46}{fmt_comma14_2(gt_appr)}"
        f"  {fmt_comma14_2(gt_pay)}"
    )
    lines.append(f" {DASH_130}")


# ============================================================================
# PRODUCE ALL REPORTS
# ============================================================================

report_lines: list[str] = []

# Report 1: Staff Paid Loan List
if not lnsettle.is_empty():
    render_settle_report(
        df=lnsettle,
        title2=f"WEEKLY REPORT FOR STAFF PAID LOAN LIST AS AT {RDATE}",
        lines=report_lines,
    )

# Report 2: Staff New Loan List
if not lnrpt1a.is_empty():
    render_loan_report(
        df=lnrpt1a,
        title1="REPORT ID : EIVWSTAF (PIVB)",
        title2=f"WEEKLY REPORT FOR STAFF NEW LOAN LIST AS AT {RDATE}",
        issdte_label="ISSUE DATE",
        lines=report_lines,
    )

# Report 3: Staff Migration Loan List
if not lnrpt1b.is_empty():
    render_loan_report(
        df=lnrpt1b,
        title1="REPORT ID : EIVWSTAF (PIVB)",
        title2=f"WEEKLY REPORT FOR STAFF MIGRATION LOAN LIST AS AT {RDATE}",
        issdte_label="ISSUE DATE",
        lines=report_lines,
    )

# Report 4: Staff Full Release Loan List
if not lnrpt1c.is_empty():
    render_loan_report(
        df=lnrpt1c,
        title1="REPORT ID : EIVWSTAF (PIVB)",
        title2=f"WEEKLY REPORT FOR STAFF FULL RELEASE LOAN LIST AS AT {RDATE}",
        issdte_label="FULL REL.DATE",
        lines=report_lines,
    )

# ============================================================================
# WRITE REPORT
# ============================================================================

with open(OUTPUT_REPORT, "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines) + "\n")

print(f"Report written to: {OUTPUT_REPORT}")
