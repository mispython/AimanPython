#!/usr/bin/env python3
"""
Program : EIIMRM04.py
Purpose : Loans & Advances, By Time To Maturity For ALCO
          (Weighted Average Yield By Maturity Profile) - repricing gap
          report, split by FIXED-RATE vs BLR (base-lending-rate) interest
          type, each rendered at PRODUCT-detail level and PRODBIG-summary
          level (4 PROC TABULATE reports total).

          Structurally in the same EIIMRM0x family as EIIMRM01-03.py (same
          REPTDATE-driven date derivation style, same chunked sas7bdat ->
          Parquet -> cache pattern), but this program is a full loan/OD
          book repricing-gap engine, not a deposit-maturity report: it
          combines an overdraft-limit file, the monthly/weekly loan ledger,
          note-level terms, and pending rate-override records, then walks
          each account's billing schedule to bucket balances into
          maturity/repricing bands.

Dependency:
    %INC PGM(PBBLNFMT); -> from PBBLNFMT import format_lnprod, format_odprod
    LNPROD.  is used ("IF PUT(PRODUCT,LNPROD.)='N' THEN DELETE" -- drops
             write-off/inactive loan products before they enter START).
    ODPROD.  is used ("IF PUT(PRODUCT,ODPROD.)='N' THEN DELETE" -- same
             purpose for OD products).
    LNPRDF., SLNPRDF., ODPRDF., SODPRDF., SUBTYPF., REMFMT., $RATEFMT. are
    ALL declared locally inside THIS SAS program's own PROC FORMAT block
    (not part of PBBLNFMT.py) and are therefore implemented as local Python
    functions/dicts below, not imported from PBBLNFMT. $RATEFMT. in
    particular is declared but never referenced anywhere else in the SAS
    source body -- it is dead code and is kept here only for documentation
    parity (see rate_format() below).

============================================================================
PHYSICAL INPUT DATASETS  (each cached to Parquet independently, using the
same chunked sas7bdat -> Parquet -> cache pattern as EIIMRM01.py)
============================================================================
1. overdft.sas7bdat  (JCL //OD DD DSN=SAP.PIBB.MNILIMT(0), a GDG(0)
   "current generation" catalogued dataset -> treated as a fixed filename,
   same convention as EIBDLN1M.py's INPUT_BRANCH_FILE).
   File : INPUT_OD_FILE
   Cols used : ACCTNO, LMTENDDT, RISKCODE
   Used  : DATA OD step -- derives LMTEND from LMTENDDT, dedupes to one row
           per ACCTNO, and is later merged onto LOAN by ACCTNO to enrich OD
           accounts with limit-expiry date / risk code.

2. loan<REPTMON><NOWK>.sas7bdat  (JCL //BNM DD DSN=SAP.PIBB.SASDATA,
   member BNM.LOAN&REPTMON&NOWK). Filename is fully deterministic from
   REPTMON + NOWK (the week-of-month code derived below), so
   input_date.get_latest_file() is NOT used here (per project convention:
   only used when a filename is date-scanned, not when it is directly
   constructible from macro-style values) -- the path is built directly.
   File : INPUT_LOAN_FILE
   Cols used : ACCTNO, NOTENO, PRODUCT, PRODCD, ACCTYPE, AMTIND, CURBAL,
               BALANCE, INTRATE, FEEAMT, NTINT, INTEARN, INTAMT, INTEARN2,
               INTEARN3, EXPRDATE, PAYFREQ, PAYAMT, ISSDTE, RISKRTE
   Used  : the master loan/OD ledger for the report period; RISKRTE is
           assumed to already be a physical column on this ledger for LN
           accounts (the SAS source only ever assigns RISKRTE explicitly in
           the OD branch via "RISKRTE = RISKCODE" -- for LN accounts it is
           never assigned anywhere in the visible DATA START step, so it
           must already exist on this source dataset).

3. lnnote.sas7bdat  (JCL //LNNOTE DD DSN=SAP.PIBB.MNILN(0), GDG(0) ->
   fixed filename).
   File : INPUT_LNNOTE_FILE
   Cols used : ACCTNO, NOTENO, NTINDEX, LOANTYPE, CENSUS, PAYEFFDT
   Used  : note-level terms, merged onto LOAN by ACCTNO+NOTENO after being
           enriched with PENDFIN (pending-rate-override) fields.

4. pend.sas7bdat  (same //LNNOTE DD, member PEND -> fixed filename).
   File : INPUT_PEND_FILE
   Cols used : ACCTNO, NOTENO, RATEOVER, RELDTE
   Used  : pending rate-override records, processed into
           REALPEND/SECOND/THIRD/REPRPEND and UPDATE-chained into PENDFIN.

No reptdate.parquet is read -- REPTDATE.py supplies the report date, and
the SDD/WK/WK1/MM/MM1 week-of-month derivation (an exact-day match on
8/15/22/otherwise, DIFFERENT from REPTDATE.py's day-range-based NOWK) is
computed locally below because it feeds the LOAN input filename directly.

output_date.py is NOT used: the output //TEMP DD (SAP.PIBB.EIIMRM04.TEXT)
is a fixed GDG-style catalogued name carrying no date token, so the output
file uses a static filename, per project convention.

============================================================================
OUTPUT
============================================================================
//TEMP DD DSN=SAP.PIBB.EIIMRM04.TEXT, DISP=OLD (JCL header for this member
was not supplied in full, but PROC PRINTTO PRINT=TEMP NEW; matches the same
//TEMP DD convention used by EIIMRM01-03). RECFM is assumed FB (fixed name,
no ASA byte) consistent with the rest of the EIIMRM family; PROC TABULATE
here uses FORMCHAR='           ' (all blank), meaning NO box-drawing
characters at all -- the report is plain columnar text with no borders,
unlike EIIMRM01-03's boxed layout. Page length defaults to 60 lines;
page boundaries are marked with a form-feed character (no ASA byte).

============================================================================
KNOWN SAS SOURCE BUG -- TITLE4 LAG ACROSS THE FOUR REPORTS
============================================================================
The four TITLE4 statements are set BEFORE/AFTER the wrong PROC TABULATE
calls in the original source, so each report actually displays the title
text that was *intended* for the report before it:
  Report 1 (FIX  detail)          displays "RM DENOMINATION (FIXED RATE)"
  Report 2 (BLR  detail)          displays "RM DENOMINATION (FIXED RATE)"
  Report 3 (FIX  PRODBIG summary) displays "RM DENOMINATION (BLR)"
  Report 4 (BLR  PRODBIG summary) displays "RM DENOMINATION (FIXED RATE) SUMMARY"
This is preserved verbatim below (see REPORT_TITLES) rather than corrected.
"""

import gc
from pathlib import Path
from datetime import date, datetime

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from REPTDATE import get_reptdate_values
from PBBLNFMT_AII import format_lnprod, format_odprod

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# BASE_DIR = Path("/sas/loan/dwh")

BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR  = Path("/stgsrcsys/host/uat/AII")

INPUT_OD_DIR     = STG_DIR / "sasdata"
INPUT_LOAN_DIR   = STG_DIR / "sasdata"
INPUT_LNNOTE_DIR = STG_DIR / "sasdata"
INPUT_PEND_DIR   = STG_DIR / "sasdata"

INPUT_OD_FILE     = INPUT_OD_DIR / "intg_dp_acct_overdft_d23.sas7bdat"
INPUT_LNNOTE_FILE = INPUT_LNNOTE_DIR / "enrh_ln_note_d23.sas7bdat"
INPUT_PEND_FILE   = INPUT_PEND_DIR / "pend.sas7bdat"
# INPUT_LOAN_FILE is resolved after REPTMON/NOWK are derived (Step 1 below).

CACHE_DIR = BASE_DIR / "input" / "cache" / "EIIMRPTS"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR  = BASE_DIR / "output" / "EIIMRPTS"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "EIIMRM04.txt"

CHUNK_ROWS = 500_000
PAGE_SIZE  = 60          # lines per page (not specified in SAS -> default)

# ============================================================================
# STEP 1: REPORT DATE / WEEK-OF-MONTH DERIVATION
# (DATA _NULL_; SET LNNOTE.REPTDATE; SELECT(DAY(REPTDATE)) ...)
# This exact-day match (8/15/22/otherwise) is DIFFERENT from REPTDATE.py's
# day-RANGE-based NOWK, and unlike EIIMRM01-03 (where the equivalent value
# was dead/unused), WK here directly feeds the LOAN input filename, so it
# is computed locally rather than sourced from REPTDATE.get_reptdate_values.
# ============================================================================
print("Step 1: Deriving report date and week-of-month code...")

reptdate_values = get_reptdate_values(year_format="%Y")
reptdate = reptdate_values.reptdate

# Ensure reptdate is a date, not a datetime
if hasattr(reptdate, 'date'):
    reptdate = reptdate.date()

_day = reptdate.day
if _day == 8:
    SDD, WK, WK1 = 1, "1", "4"
elif _day == 15:
    SDD, WK, WK1 = 9, "2", "1"
elif _day == 22:
    SDD, WK, WK1 = 16, "3", "2"
else:
    SDD, WK, WK1 = 23, "4", "3"

MM = reptdate.month
if WK == "1":
    MM1 = MM - 1
    if MM1 == 0:
        MM1 = 12
else:
    MM1 = MM

SDATE = date(reptdate.year, MM, SDD)

NOWK      = WK
NOWK1     = WK1
REPTMON   = f"{MM:02d}"
REPTMON1  = f"{MM1:02d}"
REPTYEAR  = reptdate.strftime("%y")
REPTDAY   = reptdate.strftime("%d")
RDATE     = reptdate.strftime("%d/%m/%y")            # PUT(REPTDATE,DDMMYY8.)
SDATE_STR = SDATE.strftime("%d/%m/%y")               # PUT(SDATE,DDMMYY8.)
# BTYPE = PUT('PBB',$3.) is set via SYMPUT in the SAS source but never
# referenced again anywhere else in the program body -- dead code, kept
# here only for documentation parity.
BTYPE = "PBB"

RPYR, RPMTH, RPDAY = reptdate.year, reptdate.month, reptdate.day

print(f"  RDATE            : {RDATE}   SDATE: {SDATE_STR}")
print(f"  REPTMON/NOWK     : {REPTMON}/{NOWK}   REPTMON1/NOWK1: {REPTMON1}/{NOWK1}")
print(f"  Output file      : {OUTPUT_FILE.name}")

# LOAN input filename is fully deterministic from REPTMON+NOWK -- built
# directly, not resolved via input_date.get_latest_file().
# INPUT_LOAN_FILE = INPUT_LOAN_DIR / f"loan{REPTMON}{NOWK}.sas7bdat"
# INPUT_LOAN_FILE = INPUT_LOAN_DIR / f"iln{REPTMON}{NOWK}{REPTYEAR}.sas7bdat"
INPUT_LOAN_FILE = INPUT_LOAN_DIR / f"iloan083.sas7bdat"

# ============================================================================
# DAYS-IN-MONTH HELPER  (equivalent of the RETAIN D1-D12/RD1-RD12 arrays --
# functionally identical to the SAS RETAIN pattern of 31/28-or-29/31/30/...,
# so a plain calendar lookup replaces the mutable array bookkeeping)
# ============================================================================
_DAYS_IN_MONTH_BASE = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
                       7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}


def _ldays(mm: int, yy: int) -> int:
    if mm == 2:
        return 29 if (yy % 4 == 0) else 28
    return _DAYS_IN_MONTH_BASE[mm]


def _as_date(v):
    """Coerce a date or datetime value to a plain date; pass through None."""
    if v is None:
        return None
    if hasattr(v, "date") and not isinstance(v, date):
        return v.date()
    return v


def _remmth_04(rpt_date: date, matdt: date) -> float:
    """%REMMTH macro (this program's variant, parameterised by &REPTDATE
    and recomputed on every call). MDDAYS/MD2 are computed in the SAS
    source but the clip is applied against RPDAYS(RPMTH) only (MDDAYS is
    never referenced in the REMMTH formula itself) -- that unused
    MD2/MDDAYS bookkeeping is therefore omitted here as dead code, exactly
    mirroring what the formula actually uses."""
    rpyr, rpmth, rpday = rpt_date.year, rpt_date.month, rpt_date.day
    mdyr, mdmth, mdday = matdt.year, matdt.month, matdt.day
    days_in_rpmth = _ldays(rpmth, rpyr)
    if mdday > days_in_rpmth:
        mdday = days_in_rpmth
    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday
    return remy * 12 + remm + remd / days_in_rpmth


def _nxtbldt(bldate: date, payfreq, freq, issdte: date) -> date:
    """%NXTBLDT macro -- computes the next billing date."""
    if payfreq == "6":
        dd = bldate.day + 14
        mm = bldate.month
        yy = bldate.year
        if dd > _ldays(mm, yy):
            dd -= _ldays(mm, yy)
            mm += 1
            if mm > 12:
                mm -= 12
                yy += 1
    else:
        dd = issdte.day
        mm = bldate.month + (freq or 0)
        yy = bldate.year
        if mm > 12:
            mm -= 12
            yy += 1
    if dd > _ldays(mm, yy):
        dd = _ldays(mm, yy)
    return date(yy, mm, dd)


_PAYFREQ_TO_FREQ = {"1": 1, "2": 3, "3": 6, "4": 12}   # SELECT(PAYFREQ); OTHERWISE -> missing


def _sas_round(x: float) -> float:
    if x >= 0:
        return float(int(x + 0.5))
    return float(-int(-x + 0.5))


# ============================================================================
# PROC FORMAT EQUIVALENTS (all LOCAL to this program -- see docstring)
# ============================================================================
def _ranges_to_set(*specs) -> set:
    """Expand a mix of ints and (lo, hi) inclusive-range tuples into a set."""
    out = set()
    for s in specs:
        if isinstance(s, tuple):
            out.update(range(s[0], s[1] + 1))
        else:
            out.add(s)
    return out


# --- VALUE REMFMT (local to EIIMRM04 -- LOW-1, then 1-mth increments to
# 24, then year-range buckets, then 60-HIGH, OTHER=blank; NO special
# 91-97/99 codes here, unlike EIIMRM01-03's REMFMT) ---------------------
def remfmt_format(value) -> str:
    if value is None or value <= 1:
        return ">  0-1 MTH"
    if value <= 2:
        return ">  1-2 MTHS"
    if value <= 3:
        return ">  2-3 MTHS"
    if value <= 4:
        return ">  3-4 MTHS"
    if value <= 5:
        return ">  4-5 MTHS"
    if value <= 6:
        return ">  5-6 MTHS"
    if value <= 7:
        return ">  6-7 MTHS"
    if value <= 8:
        return ">  7-8 MTHS"
    if value <= 9:
        return ">  8-9 MTHS"
    if value <= 10:
        return ">  9-10 MTHS"
    if value <= 11:
        return "> 10-11 MTHS"
    if value <= 12:
        return "> 11-12 MTHS"
    if value <= 13:
        return "> 12-13 MTHS"
    if value <= 14:
        return "> 13-14 MTHS"
    if value <= 15:
        return "> 14-15 MTHS"
    if value <= 16:
        return "> 15-16 MTHS"
    if value <= 17:
        return "> 16-17 MTHS"
    if value <= 18:
        return "> 17-18 MTHS"
    if value <= 19:
        return "> 18-19 MTHS"
    if value <= 20:
        return "> 19-20 MTHS"
    if value <= 21:
        return "> 20-21 MTHS"
    if value <= 22:
        return "> 21-22 MTHS"
    if value <= 23:
        return "> 22-23 MTHS"
    if value <= 24:
        return "> 23-24 MTHS"
    if value <= 36:
        return ">2-3 YRS    "
    if value <= 48:
        return ">3-4 YRS    "
    if value <= 60:
        return ">4-5 YRS    "
    return ">5 YRS      "


_REMFMT_ORDER = [
    ">  0-1 MTH", ">  1-2 MTHS", ">  2-3 MTHS", ">  3-4 MTHS", ">  4-5 MTHS",
    ">  5-6 MTHS", ">  6-7 MTHS", ">  7-8 MTHS", ">  8-9 MTHS", ">  9-10 MTHS",
    "> 10-11 MTHS", "> 11-12 MTHS", "> 12-13 MTHS", "> 13-14 MTHS", "> 14-15 MTHS",
    "> 15-16 MTHS", "> 16-17 MTHS", "> 17-18 MTHS", "> 18-19 MTHS", "> 19-20 MTHS",
    "> 20-21 MTHS", "> 21-22 MTHS", "> 22-23 MTHS", "> 23-24 MTHS",
    ">2-3 YRS    ", ">3-4 YRS    ", ">4-5 YRS    ", ">5 YRS      ",
    "     TOTAL", "  ",
]
_REMFMT_ORDER_INDEX = {label: i for i, label in enumerate(_REMFMT_ORDER)}


def _remmth1_sort_key(label: str) -> int:
    return _REMFMT_ORDER_INDEX.get(label, len(_REMFMT_ORDER))


# --- VALUE SUBTYPF ------------------------------------------------------
_SUBTYPF_MAP = {
    5: "PRINCIPAL",
    5.5: "WAREMM(MTH)",
    11: "INSTALMENT ",
    12: "REPRICING  ",
    13: "NO-REPRICE",
    6: "UNEARN INT",
    7: "ACCRUED INT",
    8: "FEE AMOUNT",
    9: "NPL",
}


def subtypf_format(code) -> str:
    return _SUBTYPF_MAP.get(code, "")


# --- VALUE $RATEFMT -- declared in the SAS source but never referenced
# anywhere else in the program body; kept here only for parity, unused. ---
_RATEFMT_MAP = {
    "30591": "FIXED RATE", "30592": "FIXED RATE", "30593": "FIXED RATE",
    "30595": "FLOATING RATE", "30596": "FLOATING RATE", "30597": "FLOATING RATE",
}


def rate_format(code) -> str:
    return _RATEFMT_MAP.get(code, "")


# --- VALUE LNPRDF (PRODTYP for LN accounts) -----------------------------
_LNPRDF_ENTRIES = [
    (_ranges_to_set(227, 228), "  1.HOME PLAN 1"),
    (_ranges_to_set(230, 231), "  1.HOME PLAN 2"),
    (_ranges_to_set(232, 233), "  1.HOME PLAN 3"),
    (_ranges_to_set(237, 238), "  1.HOME PLAN 6"),
    (_ranges_to_set(239, 240), "  1.HOME PLAN 7"),
    (_ranges_to_set(241, 243), "  1.HOME PLAN 8"),
    (_ranges_to_set(234), "  1.MORE PLAN 1"),
    (_ranges_to_set(235), "  1.MORE PLAN 2"),
    (_ranges_to_set(236), "  1.MORE PLAN 3"),
    (_ranges_to_set(242), "  1.MORE PLAN 4"),
    (_ranges_to_set(380, 381), "  2.HIRE PURCHASE"),
    (_ranges_to_set(390), "  3.LEASING"),
    (_ranges_to_set(209, 210, 211, 212, 214, 215), "  1.HOME OWN BEF 5"),
    (_ranges_to_set(204, 205, 200, 201, 219, 220, 225, 226, 245, 246, 247), "  1.OTHER HOUSING"),
    (_ranges_to_set(309, 310, 904, 905), "  6.BRIDGING"),
    (_ranges_to_set(300, 301, 900, 901, 530, 362, 364, 365, 506), "  7.FIXED"),
    (_ranges_to_set((1, 100)), "  4.STAFF"),
    (_ranges_to_set(359, 906, 363), "  5.SWIFT"),
    (_ranges_to_set(360, 908), " 17.BLOCK DISC"),
    (_ranges_to_set(361, 907), "  9.SMILAX"),
    (_ranges_to_set(531), " 11.SRGF"),
    (_ranges_to_set(160, 162, 163, 164), " 12.ABBA OD"),
    (_ranges_to_set((110, 118), 139, 140), " 12.ABBA HOUSE"),
    (_ranges_to_set(120), " 12.ABBA TERM"),
    (_ranges_to_set((194, 196)), " 12.ABBA CONSUMER"),
    (_ranges_to_set(181), " 12.ABBA SYNDICATE"),
    (_ranges_to_set(180, 183), " 12.ABBA SYNDICATE(FIXED)"),
    (_ranges_to_set(127, 126), " 12.ABBA SWIFT"),
    (_ranges_to_set(129), " 12.ABBA SMILAX"),
    (_ranges_to_set(193), " 12.ABBA LEASE"),
    (_ranges_to_set(137), " 12.ABBA OTHR PLAN"),
    (_ranges_to_set(135, 136, 138), " 12.ABBA PERSONAL"),
    (_ranges_to_set(197, 170), " 12.ABBA OTHER"),
    (_ranges_to_set(122), " 12.ABBA UNIT TRST"),
    (_ranges_to_set(141, 142), " 12.ABBA HOUSE BFR"),
    (_ranges_to_set(143), " 12.ABBA TERM BFR"),
    (_ranges_to_set(182), " 12.ABBA SYN.BULLET(FIXED)"),
    (_ranges_to_set(564, 565), " 13.FUND FOR FOOD"),
    (_ranges_to_set(561), " 13.L&MCOST HOUSE"),
    (_ranges_to_set(559, 560, 567), " 13.NEF LOAN"),
    (_ranges_to_set(555, 556), " 14.SFT LOAN"),
    (_ranges_to_set(566, 568, 570, 573, 909), " 15.SMI"),
    (_ranges_to_set(569), " 15.SFSMI2"),
    (_ranges_to_set(521, 522, 523, 528), " 16.CGC TUK"),
    (_ranges_to_set(517), " 16.CGC ASL"),
    (_ranges_to_set(527), " 16.CGC NEF"),
    (_ranges_to_set(524, 525), " 16.CGC FSMI"),
    (_ranges_to_set(526), " 16.CGC FFF"),
    (_ranges_to_set(910, 350, 925, 302, 902, 903, 951), " 10.REVOLVING CRDT"),
    (_ranges_to_set(914, 915, 919, 920, 950), "  8.SYNDICATED"),
    (_ranges_to_set(345), " 18.(MISC)CONTRACT"),
    (_ranges_to_set(304, 305), " 18.(MISC)FLASH"),
    (_ranges_to_set(355), " 18.(MISC)PB EXEC"),
    (_ranges_to_set(356), " 18.(MISC)HOME FURNH"),
    (_ranges_to_set(504, 505, 509, 510, 515), " 18.(MISC)PRIN GUARAN"),
    (_ranges_to_set(325), " 18.(MISC)PROF.ADVAN"),
    (_ranges_to_set(357), " 18.(MISC)SHARE"),
    (_ranges_to_set(518, 519), " 18.(MISC)SLS-FIXED"),
    (_ranges_to_set(335), " 18.(MISC)UNIT TRUST"),
    (_ranges_to_set(358), " 18.(MISC)UNIFLEX"),
    (_ranges_to_set(320), " 18.(MISC)UNSECURED"),
    (_ranges_to_set(391), " 18.(MISC)CON.DURABLE"),
    (_ranges_to_set(330), " 18.(MISC)QUICK CASH"),
    (_ranges_to_set(131, 132), " 19.AITAB VARIABLE"),
    (_ranges_to_set(720, 725), " 20.HP VARIABLE"),
]
_LNPRDF_OTHER = " 21.OTHERS"


def lnprdf_format(product) -> str:
    for codes, label in _LNPRDF_ENTRIES:
        if product in codes:
            return label
    return _LNPRDF_OTHER


# --- VALUE SLNPRDF (PRODBIG for LN accounts) ----------------------------
_SLNPRDF_ENTRIES = [
    (_ranges_to_set(227, 228, 230, 231, 232, 233), "  1.HOME 1YR FIX"),
    (_ranges_to_set(237, 238), "  1.HOME 3YRS FIX"),
    (_ranges_to_set(239, 240), "  1.HOME 5YRS FIX"),
    (_ranges_to_set(241, 243), "  1.HOME 1YR FIX"),
    (_ranges_to_set(234), "  1.MORE 1YR FIX"),
    (_ranges_to_set(235), "  1.MORE 3YRS FIX"),
    (_ranges_to_set(236), "  1.MORE 5YRS FIX"),
    (_ranges_to_set(242), "  1.MORE 1YR FIX"),
    (_ranges_to_set(380, 381), "  2.HIRE PURCHASE"),
    (_ranges_to_set(390), "  9.LEASING"),
    (_ranges_to_set(209, 210, 211, 212, 214, 215, 204, 205, 200, 201, 219, 220, 225, 226, 245, 246, 247), "  1.OTHER HOUSING"),
    (_ranges_to_set(309, 310, 904, 905), "  5.BRIDGING"),
    (_ranges_to_set(300, 301, 900, 901, 530, 362), "  6.FIXED TIER"),
    (_ranges_to_set((1, 100)), " 10.STAFF"),
    (_ranges_to_set(359, 906, 363), "  3.SWIFT"),
    (_ranges_to_set(360, 908), "  6.FIXED LOAN"),
    (_ranges_to_set(361, 907), "  4.SMILAX"),
    (_ranges_to_set(531), " 12.FUNDED BNM"),
    (_ranges_to_set((110, 118), 139, 140), " 11.ABBA HOUSE"),
    (_ranges_to_set(120), " 11.ABBA OTH TERM"),
    (_ranges_to_set((194, 196)), " 11.ABBA CONSUMER"),
    (_ranges_to_set(181), " 11.ABBA SYNDICATE"),
    (_ranges_to_set(180, 183), " 11.ABBA SYNDICATE(FIXED)"),
    (_ranges_to_set(127, 126), " 11.ABBA SWIFT"),
    (_ranges_to_set(129), " 11.ABBA SMILAX"),
    (_ranges_to_set(193), " 11.ABBA OTH TERM"),
    (_ranges_to_set(137), " 11.ABBA OTH TERM"),
    (_ranges_to_set(135, 136, 138), " 11.ABBA PERSONAL"),
    (_ranges_to_set(197, 170), " 11.ABBA OTH TERM"),
    (_ranges_to_set(122), " 11.ABBA UNIT TRST"),
    (_ranges_to_set(141, 142), " 11.ABBA HOUSE BFR"),
    (_ranges_to_set(143), " 11.ABBA TERM BFR"),
    (_ranges_to_set(182), " 11.ABBA SYN.BULLET(FIXED)"),
    (_ranges_to_set(564, 565, 569), " 12.FUNDED BNM"),
    (_ranges_to_set(561), " 12.FUNDED BNM"),
    (_ranges_to_set(559, 560, 567), " 12.FUNDED BNM"),
    (_ranges_to_set(555, 556), " 12.FUNDED BNM"),
    (_ranges_to_set(566, 568, 570, 573, 909), " 12.FUNDED BNM"),
    (_ranges_to_set(521, 522, 523, 528), " 13.CGC"),
    (_ranges_to_set(517), " 13.CGC"),
    (_ranges_to_set(527), " 13.CGC"),
    (_ranges_to_set(524, 525), " 13.CGC"),
    (_ranges_to_set(526), " 13.CGC"),
    (_ranges_to_set(910, 350, 925, 302, 902, 903, 951), "  8.REVOLVING CRDT"),
    (_ranges_to_set(914, 915, 919, 920, 950), "  7.SYNDICATED"),
    (_ranges_to_set(345), "  6.FIXED LOAN"),
    (_ranges_to_set(304, 305), "  6.FIXED LOAN"),
    (_ranges_to_set(355), "  6.FIXED LOAN"),
    (_ranges_to_set(356), "  6.FIXED LOAN"),
    (_ranges_to_set(504, 505, 509, 510, 515), "  6.FIXED LOAN"),
    (_ranges_to_set(325), "  6.FIXED LOAN"),
    (_ranges_to_set(357), "  6.FIXED LOAN"),
    (_ranges_to_set(518, 519), "  6.FIXED LOAN"),
    (_ranges_to_set(335), "  6.FIXED LOAN"),
    (_ranges_to_set(358), "  6.FIXED LOAN"),
    (_ranges_to_set(320), "  6.FIXED LOAN"),
    (_ranges_to_set(391), "  6.FIXED LOAN"),
    (_ranges_to_set(330, 364, 365, 506), "  6.FIXED LOAN"),
    (_ranges_to_set(131, 132), " 14.AITAB VARIABLE"),
    (_ranges_to_set(720, 725), " 15.HP VARIABLE"),
]
_SLNPRDF_OTHER = " 16.OTHERS"


def slnprdf_format(product) -> str:
    for codes, label in _SLNPRDF_ENTRIES:
        if product in codes:
            return label
    return _SLNPRDF_OTHER


# --- VALUE ODPRDF / SODPRDF (PRODTYP/PRODBIG for OD accounts) -----------
_ODPRDF_ABBA = _ranges_to_set((60, 64), (160, 166))


def odprdf_format(product) -> str:
    return " 12.ABBA OD" if product in _ODPRDF_ABBA else "  1.CONV OD"


def sodprdf_format(product) -> str:
    return " 11.ABBA OD" if product in _ODPRDF_ABBA else "  1.CONV OD"


# ============================================================================
# HELPER: CACHE STAMP + STREAM .sas7bdat -> PARQUET  (EIIMRM01.py pattern)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


def _sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0

    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
    for chunk in reader:
        if schema is None:
            fields = []
            for col, dtype in chunk.dtypes.items():
                if dtype == 'object':
                    pa_type = pa.string()
                elif pd.api.types.is_integer_dtype(dtype):
                    pa_type = pa.int64()
                elif pd.api.types.is_float_dtype(dtype):
                    pa_type = pa.float64()
                else:
                    pa_type = pa.from_numpy_dtype(dtype)
                fields.append(pa.field(col, pa_type))
            schema = pa.schema(fields)
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")

        table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)
        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer:
        writer.close()
    print(f"  [{tag}] Done - {total:,} rows cached.")


def _load_cached(sas_path: Path, tag: str) -> Path:
    cache_path = CACHE_DIR / f"{sas_path.stem}.parquet"
    if _cache_is_fresh(sas_path, cache_path):
        print(f"  [{tag}] Cache fresh - skipping conversion.")
    else:
        _sas_to_parquet(sas_path, cache_path, tag)
    return cache_path


# ============================================================================
# STEP 2: CACHE INPUT SAS DATASETS TO PARQUET
# ============================================================================
print("\nStep 2: Caching input SAS datasets to Parquet...")
OD_CACHE     = _load_cached(INPUT_OD_FILE, "OD")
LOAN_CACHE   = _load_cached(INPUT_LOAN_FILE, "LOAN")
LNNOTE_CACHE = _load_cached(INPUT_LNNOTE_FILE, "LNNOTE")
PEND_CACHE   = _load_cached(INPUT_PEND_FILE, "PEND")

# ============================================================================
# STEP 3: DATA OD; SET OD.OVERDFT; WHERE LMTENDDT NE . AND LMTENDDT > 0;
#         LMTEND = INPUT(SUBSTR(PUT(LMTENDDT,Z11.),1,8),MMDDYY8.);
#         IF LMTEND = . THEN DELETE;
#         PROC SORT ... NODUPKEYS; BY ACCTNO;
# ============================================================================
print("\nStep 3: Building OD (limit-expiry lookup, deduped by ACCTNO)...")

con = duckdb.connect(database=":memory:")
od_raw = con.execute(f"""
    SELECT
        CAST(ACCTNO    AS BIGINT) AS ACCTNO,
        CAST(LMTENDDT  AS DOUBLE) AS LMTENDDT,
        CAST(RISKCODE  AS INTEGER) AS RISKCODE
    FROM read_parquet('{OD_CACHE.as_posix()}')
    WHERE LMTENDDT IS NOT NULL AND LMTENDDT > 0
      AND ENTITY_CD = 'PIBB'
""").pl()
con.close()


def _parse_lmtend(lmtenddt: float):
    """LMTEND = INPUT(SUBSTR(PUT(LMTENDDT,Z11.),1,8),MMDDYY8.);
    LMTENDDT is zero-padded to an 11-character string; the first 8 digits
    are taken as an unseparated MMDDYYYY date (8-char width, 4-digit year --
    the only self-consistent reading of an unseparated MMDDYYw. informat
    with no delimiters present)."""
    try:
        s = f"{int(lmtenddt):011d}"[:8]
        mm, dd, yyyy = int(s[0:2]), int(s[2:4]), int(s[4:8])
        return date(yyyy, mm, dd)
    except (ValueError, OverflowError):
        return None


od_by_acct = {}
for r in od_raw.iter_rows(named=True):
    lmtend = _parse_lmtend(r["LMTENDDT"])
    if lmtend is None:
        continue
    acctno = r["ACCTNO"]
    # PROC SORT NODUPKEYS BY ACCTNO -- keep first occurrence per ACCTNO.
    if acctno not in od_by_acct:
        od_by_acct[acctno] = {
            "ACCTNO": acctno, "LMTEND": lmtend,
            "LMTENDDT": r["LMTENDDT"], "RISKCODE": r["RISKCODE"],
        }

del od_raw
gc.collect()
print(f"  OD rows (deduped): {len(od_by_acct):,}")

# ============================================================================
# STEP 4: LOAN LEDGER  (PROC SORT DATA=BNM.LOAN&REPTMON&NOWK OUT=LOAN;
#         BY ACCTNO NOTENO; WHERE PRODUCT NOT IN (700,705,380,381,128,130,500,520);)
# ============================================================================
print("\nStep 4: Loading LOAN ledger...")

con = duckdb.connect(database=":memory:")
loan_raw = con.execute(f"""
    SELECT
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(NOTENO   AS BIGINT)  AS NOTENO,
        CAST(PRODUCT  AS INTEGER) AS PRODUCT,
        CAST(PRODCD   AS VARCHAR) AS PRODCD,
        CAST(ACCTYPE  AS VARCHAR) AS ACCTYPE,
        CAST(AMTIND   AS VARCHAR) AS AMTIND,
        CAST(CURBAL   AS DOUBLE)  AS CURBAL,
        CAST(BALANCE  AS DOUBLE)  AS BALANCE,
        CAST(INTRATE  AS DOUBLE)  AS INTRATE,
        CAST(FEEAMT   AS DOUBLE)  AS FEEAMT,
        CAST(NTINT    AS VARCHAR) AS NTINT,
        CAST(INTEARN  AS DOUBLE)  AS INTEARN,
        CAST(INTAMT   AS DOUBLE)  AS INTAMT,
        CAST(INTEARN2 AS DOUBLE)  AS INTEARN2,
        CAST(INTEARN3 AS DOUBLE)  AS INTEARN3,
        CASE WHEN EXPRDATE IS NOT NULL 
             THEN CAST(date_add(DATE '1960-01-01', INTERVAL (CAST(EXPRDATE AS INTEGER)) DAY) AS DATE)
             ELSE NULL END AS EXPRDATE,
        CAST(PAYFREQ  AS VARCHAR) AS PAYFREQ,
        CAST(PAYAMT   AS DOUBLE)  AS PAYAMT,
        CASE WHEN ISSDTE IS NOT NULL 
             THEN CAST(date_add(DATE '1960-01-01', INTERVAL (CAST(ISSDTE AS INTEGER)) DAY) AS DATE)
             ELSE NULL END AS ISSDTE,
        CAST(RISKRTE  AS INTEGER) AS RISKRTE
    FROM read_parquet('{LOAN_CACHE.as_posix()}')
    WHERE PRODUCT NOT IN (700,705,380,381,128,130,500,520)
    ORDER BY ACCTNO, NOTENO
""").pl()
con.close()

loan_raw = loan_raw.with_columns(
    pl.col("EXPRDATE").cast(pl.Date),
    pl.col("ISSDTE").cast(pl.Date),
)

print(f"  LOAN rows: {len(loan_raw):,}")

# con = duckdb.connect(database=":memory:")
# loan_raw = con.execute(f"""
#     SELECT
#         CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
#         CAST(NOTENO   AS BIGINT)  AS NOTENO,
#         CAST(PRODUCT  AS INTEGER) AS PRODUCT,
#         CAST(PRODCD   AS VARCHAR) AS PRODCD,
#         CAST(ACCTYPE  AS VARCHAR) AS ACCTYPE,
#         CAST(AMTIND   AS VARCHAR) AS AMTIND,
#         CAST(CURBAL   AS DOUBLE)  AS CURBAL,
#         CAST(BALANCE  AS DOUBLE)  AS BALANCE,
#         CAST(INTRATE  AS DOUBLE)  AS INTRATE,
#         CAST(FEEAMT   AS DOUBLE)  AS FEEAMT,
#         CAST(NTINT    AS VARCHAR) AS NTINT,
#         CAST(INTEARN  AS DOUBLE)  AS INTEARN,
#         CAST(INTAMT   AS DOUBLE)  AS INTAMT,
#         CAST(INTEARN2 AS DOUBLE)  AS INTEARN2,
#         CAST(INTEARN3 AS DOUBLE)  AS INTEARN3,
#         CAST(EXPRDATE AS DATE)    AS EXPRDATE,
#         CAST(PAYFREQ  AS VARCHAR) AS PAYFREQ,
#         CAST(PAYAMT   AS DOUBLE)  AS PAYAMT,
#         CAST(ISSDTE   AS DATE)    AS ISSDTE,
#         CAST(RISKRTE  AS INTEGER) AS RISKRTE
#     FROM read_parquet('{LOAN_CACHE.as_posix()}')
#     WHERE PRODUCT NOT IN (700,705,380,381,128,130,500,520)
#       AND ENTITY_CD = 'PIBB'
#     ORDER BY ACCTNO, NOTENO
# """).pl()
# con.close()
# print(f"  LOAN rows: {len(loan_raw):,}")

# DATA LOAN; MERGE LOAN(IN=A) OD(IN=B); BY ACCTNO; IF A;
# Enrich each loan row with OD's limit-expiry/risk-code where a match
# exists by ACCTNO; keep every LOAN row regardless of OD match (IF A).
loan_enriched = []
for r in loan_raw.iter_rows(named=True):
    od = od_by_acct.get(r["ACCTNO"])
    row = dict(r)
    row["LMTEND"] = od["LMTEND"] if od else None
    row["OD_RISKCODE"] = od["RISKCODE"] if od else None
    loan_enriched.append(row)

del loan_raw, od_by_acct
gc.collect()

# ============================================================================
# STEP 5: LNNOTE  (PROC SORT DATA=LNNOTE.LNNOTE OUT=LNNOTE
#         (KEEP=ACCTNO NOTENO NTINT PAYEFFDT NTINDEX LOANTYPE CENSUS);
#         BY ACCTNO NOTENO; WHERE LOANTYPE NOT IN (...);)
# ============================================================================
print("\nStep 5: Loading LNNOTE...")

con = duckdb.connect(database=":memory:")
lnnote_raw = con.execute(f"""
    SELECT
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(NOTENO   AS BIGINT)  AS NOTENO,
        CAST(PAYEFFDT AS DOUBLE)  AS PAYEFFDT,
        CAST(NTINDEX  AS INTEGER) AS NTINDEX,
        CAST(LOANTYPE AS INTEGER) AS LOANTYPE,
        CAST(CENSUS   AS VARCHAR) AS CENSUS
    FROM read_parquet('{LNNOTE_CACHE.as_posix()}')
    WHERE LOANTYPE NOT IN (700,705,380,381,128,130,500,520)
      AND ENTITY_CD = 'PIBB'
""").pl()
con.close()
print(f"  LNNOTE rows: {len(lnnote_raw):,}")

# ============================================================================
# STEP 6: PEND  (PROC SORT DATA=LNNOTE.PEND OUT=PEND(KEEP=ACCTNO NOTENO
#         RATEOVER RELDTE); BY ACCTNO NOTENO;)
# ============================================================================
print("\nStep 6: Loading PEND and building PENDFIN...")

con = duckdb.connect(database=":memory:")
pend_raw = con.execute(f"""
    SELECT
        CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
        CAST(NOTENO   AS BIGINT)  AS NOTENO,
        CAST(RATEOVER AS DOUBLE)  AS RATEOVER,
        CAST(RELDTE   AS VARCHAR) AS RELDTE
    FROM read_parquet('{PEND_CACHE.as_posix()}')
    ORDER BY ACCTNO, NOTENO
""").pl()
con.close()

# con = duckdb.connect(database=":memory:")
# pend_raw = con.execute(f"""
#     SELECT
#         CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
#         CAST(NOTENO   AS BIGINT)  AS NOTENO,
#         CAST(RATEOVER AS DOUBLE)  AS RATEOVER,
#         CAST(RELDTE   AS VARCHAR) AS RELDTE
#     FROM read_parquet('{PEND_CACHE.as_posix()}')
#     WHERE ENTITY_CD = 'PIBB'
#     ORDER BY ACCTNO, NOTENO
# """).pl()
# con.close()


def _parse_reldte(reldte: str):
    """INPUT(SUBSTR(RELDTE,6,4)||SUBSTR(RELDTE,2,4),MMDDYY8.)
    RELDTE positions are 1-based in SAS; position 6 for 4 chars, then
    position 2 for 4 chars, concatenated and parsed as an 8-digit MMDDYYYY
    string (0-based Python slices: [5:9] then [1:5])."""
    if reldte is None:
        return None
    s = (reldte[5:9] + reldte[1:5])
    try:
        mm, dd, yyyy = int(s[0:2]), int(s[2:4]), int(s[4:8])
        return date(yyyy, mm, dd)
    except (ValueError, IndexError):
        return None


# DATA REALPEND SECOND THIRD REPRPEND; SET PEND; BY ACCTNO NOTENO;
# "FIRST.ACCTNO OR FIRST.NOTENO" / "LAST.ACCTNO OR LAST.NOTENO" collapse to
# plain FIRST.NOTENO / LAST.NOTENO since FIRST.ACCTNO always implies
# FIRST.NOTENO under nested BY ACCTNO NOTENO -- so position is simply the
# row's position within its own (ACCTNO,NOTENO) group.
groups: dict = {}
for r in pend_raw.iter_rows(named=True):
    groups.setdefault((r["ACCTNO"], r["NOTENO"]), []).append(r)

realpend_rows, second_rows, third_rows, reprpend_rows = [], [], [], []
for (acctno, noteno), rows in groups.items():
    n = len(rows)
    for i, r in enumerate(rows):
        rateover = r["RATEOVER"] or 0.0
        if rateover > 0:
            if i == 0:
                realpend_rows.append({
                    "ACCTNO": acctno, "NOTENO": noteno,
                    "REALISDT": _parse_reldte(r["RELDTE"]),
                })
            elif i == n - 1:
                third_rows.append({
                    "ACCTNO": acctno, "NOTENO": noteno,
                    "REALISD3": _parse_reldte(r["RELDTE"]), "RATEOVE3": rateover,
                })
            else:
                second_rows.append({
                    "ACCTNO": acctno, "NOTENO": noteno,
                    "REALISD2": _parse_reldte(r["RELDTE"]), "RATEOVE2": rateover,
                })
        else:
            reprpend_rows.append({
                "ACCTNO": acctno, "NOTENO": noteno,
                "REPRICDT": _parse_reldte(r["RELDTE"]),
            })

del pend_raw, groups
gc.collect()

# PROC SORT DATA=REPRPEND NODUPKEYS; BY ACCTNO NOTENO;  -- keep first per key
reprpend_dedup = {}
for r in reprpend_rows:
    key = (r["ACCTNO"], r["NOTENO"])
    if key not in reprpend_dedup:
        reprpend_dedup[key] = r


def _sas_update(master: dict, trans_rows: list, key_fields) -> dict:
    """DATA out; UPDATE master transaction; BY key_fields; RUN;
    master: dict keyed by key-tuple -> row dict.
    Returns a new dict: every master key is kept, transaction-only keys are
    added, and for matching keys, non-missing transaction field values
    overwrite master values (later transaction rows win for repeated keys)."""
    result = {k: dict(v) for k, v in master.items()}
    for t in trans_rows:
        key = tuple(t[f] for f in key_fields)
        if key not in result:
            result[key] = {f: t[f] for f in key_fields}
        for f, v in t.items():
            if f in key_fields:
                continue
            if v is not None:
                result[key][f] = v
    return result


realpend_by_key = {(r["ACCTNO"], r["NOTENO"]): r for r in realpend_rows}
realpen2 = _sas_update(realpend_by_key, second_rows, ("ACCTNO", "NOTENO"))
realpen2 = _sas_update(realpen2, third_rows, ("ACCTNO", "NOTENO"))
pendfin = _sas_update(realpen2, list(reprpend_dedup.values()), ("ACCTNO", "NOTENO"))

del realpend_rows, second_rows, third_rows, reprpend_rows, reprpend_dedup, realpend_by_key, realpen2
gc.collect()
print(f"  PENDFIN rows: {len(pendfin):,}")

# ============================================================================
# STEP 7: DATA LNNOTE; MERGE LNNOTE(IN=A) PENDFIN(IN=B); BY ACCTNO NOTENO;
#         IF B THEN PENDIND='Y'; [PAYEFFDT -> PAYEFDT reconstruction]
# ============================================================================
print("\nStep 7: Merging LNNOTE with PENDFIN and deriving PAYEFDT...")


def _derive_payefdt(payeffdt):
    """PAYCY/PAYMM/PAYDD are sliced from PUT(PAYEFFDT,Z11.). Only PAYCY is
    scoped to "IF PAYEFFDT NOT IN (.,0)"; PAYMM, PAYDD and the final
    PAYEFDT assignment run UNCONDITIONALLY for every row regardless of that
    condition (a dangling-scope artefact in the SAS source, same pattern as
    documented in other EIIMRM0x programs), preserved here verbatim -- so
    PAYCY can be missing (None) while PAYMM/PAYDD are still computed from
    PUT(PAYEFFDT,Z11.), which for a missing/zero PAYEFFDT still yields a
    zero-string slice. The trailing "ELSE" with no statement inside the
    DD-clamp block (PAYMM NE 2 AND PAYDD > 31) is unreachable dead code in
    the SAS source (every month other than February is covered by the two
    listed month sets), so it is a no-op here too."""
    z11 = f"{int(payeffdt or 0):011d}"
    payCY = int(z11[0:4]) if (payeffdt not in (None, 0)) else None
    payMM = int(z11[7:9])
    payDD = int(z11[9:11])
    if payMM == 2 and payDD > 29:
        payDD = 29 if (payCY is not None and payCY % 4 == 0) else 28
    if payMM != 2 and payDD > 31:
        if payMM in (1, 3, 5, 7, 8, 10, 12):
            payDD = 31
        elif payMM in (4, 6, 9, 11):
            payDD = 30
        # else: unreachable (see docstring) -- no-op
    try:
        return date(payCY if payCY is not None else 1, payMM, payDD) if payCY else None
    except ValueError:
        return None


lnnote_final = {}
for r in lnnote_raw.iter_rows(named=True):
    key = (r["ACCTNO"], r["NOTENO"])
    row = dict(r)
    pend = pendfin.get(key)
    row["PENDIND"] = "Y" if pend else None
    if pend:
        row.update({k: v for k, v in pend.items() if k not in ("ACCTNO", "NOTENO")})
    row["PAYEFDT"] = _derive_payefdt(row["PAYEFFDT"])
    lnnote_final[key] = row

del lnnote_raw, pendfin
gc.collect()
print(f"  LNNOTE (final) rows: {len(lnnote_final):,}")

# ============================================================================
# STEP 8: DATA LOAN&REPTMON&NOWK; MERGE LOAN(IN=A) LNNOTE(IN=C);
#         BY ACCTNO NOTENO; [INTTYPE derivation]; IF A AND LOANTYPE NOT IN (...);
# ============================================================================
print("\nStep 8: Merging LOAN with LNNOTE and deriving INTTYPE...")

_LOANTYPE_EXCLUDE = {700, 705, 128, 130, 380, 381, 500, 520}
_FIX_PRODUCTS = {350, 910, 925, 302, 902, 903, 951}

merged_records = []
for row in loan_enriched:
    key = (row["ACCTNO"], row["NOTENO"])
    note = lnnote_final.get(key)

    ntindex = note["NTINDEX"] if note else None
    acctype = row["ACCTYPE"]
    amtind = row["AMTIND"]

    if ntindex in (1, 30, 997) or (acctype == "OD" and amtind != "I"):
        inttype = "BLR"
    elif ntindex != 1 or (acctype == "OD" and amtind == "I"):
        inttype = "FIX"
    else:
        inttype = "OTH"
    if row["PRODUCT"] in _FIX_PRODUCTS:
        inttype = "FIX"

    loantype = note["LOANTYPE"] if note else None
    # IF A AND LOANTYPE NOT IN (...)  -- LOAN(A) is always true here since
    # we are iterating LOAN's own rows; a missing LOANTYPE (no LNNOTE
    # match) is "not in" the exclude set, so unmatched LOAN rows pass too.
    if loantype in _LOANTYPE_EXCLUDE:
        continue

    rec = dict(row)
    rec["INTTYPE"] = inttype
    rec["NTINDEX"] = ntindex
    rec["LOANTYPE"] = loantype
    rec["CENSUS"] = note["CENSUS"] if note else None
    rec["PAYEFDT"] = note["PAYEFDT"] if note else None
    rec["REPRICDT"] = note.get("REPRICDT") if note else None
    # IF PAYFREQ IN ('5','9',' ') OR PRODUCT IN (...) THEN REPRICDT = PAYEFDT;
    if rec["PAYFREQ"] in ("5", "9", " ", None) or rec["PRODUCT"] in _FIX_PRODUCTS:
        rec["REPRICDT"] = rec["PAYEFDT"]
    merged_records.append(rec)

del loan_enriched, lnnote_final
gc.collect()
print(f"  Merged loan/OD records: {len(merged_records):,}")

# ============================================================================
# STEP 9: DATA START (COMPRESS=YES); SET LOAN&REPTMON&NOWK;
#         WHERE (SUBSTR(PRODCD,1,2) IN ('34','54') AND LOANTYPE NOT IN (...))
#               OR LOANTYPE IN (131,132,720,725);
#         [full per-account repricing/maturity bucket engine]
# ============================================================================
print("\nStep 9: Running DATA START (repricing/maturity bucket engine)...")


def _prodcd_prefix_ok(prodcd) -> bool:
    return isinstance(prodcd, str) and prodcd[:2] in ("34", "54")


def _in_or_missing(value, exclude_set) -> bool:
    """SAS 'NOT IN' with a missing value is TRUE (missing is never IN a
    list of real values)."""
    return value not in exclude_set


_LOANTYPE_900_901 = {900, 901}
_LOANTYPE_244_245_247 = {244, 245, 247}
_START_LOANTYPE_EXCLUDE = {700, 705, 128, 130, 380, 381, 500, 520}
_START_LOANTYPE_INCLUDE = {131, 132, 720, 725}

# DEBUG
print(f"Type of reptdate: {type(reptdate)}")
print(f"reptdate value: {reptdate}")


def _run_start(rec: dict) -> list:
    """Emulates the DATA START step body for one merged loan/OD record.
    Returns a list of emitted output row dicts (each OUTPUT statement in
    the SAS source -> one dict here)."""
    out = []

    prodcd = rec["PRODCD"]
    loantype = rec["LOANTYPE"]
    if not (
        (_prodcd_prefix_ok(prodcd) and _in_or_missing(loantype, _START_LOANTYPE_EXCLUDE))
        or (loantype in _START_LOANTYPE_INCLUDE)
    ):
        return out

    acctype = rec["ACCTYPE"]
    product = rec["PRODUCT"]
    ntindex = rec["NTINDEX"]
    census = rec["CENSUS"]
    exprdate = rec["EXPRDATE"]
    repricdt = rec["REPRICDT"]
    payfreq = rec["PAYFREQ"]
    payamt = rec["PAYAMT"] or 0.0
    intrate = rec["INTRATE"] or 0.0
    issdte = rec["ISSDTE"]

    riskrte = rec["RISKRTE"]
    curbal = rec["CURBAL"] or 0.0
    acrint = 0.0
    feeamt = rec["FEEAMT"] or 0.0
    matdt = None

    if acctype == "LN":
        if format_lnprod(product) == "N":
            return out
        prodtyp = lnprdf_format(product)
        prodbig = slnprdf_format(product)

        if loantype in _LOANTYPE_900_901:
            costfund = rec.get("COSTFUND", 0.0) or 0.0
            if ntindex == 1:
                prodtyp = "  6.FIXED CORPORATE BLR"
                prodbig = "  7.FIXED CORPORATE BLR"
            elif costfund == 0 and (payfreq == "5" or payamt == 0):
                prodtyp = "  6.FIXED CORPORATE BULLET(FIXED RATE)"
                prodbig = "  7.FIXED CORPORATE BULLET(FIXED RATE)"
                repricdt = rec.get("EXPRDATE")
            elif costfund != 0 and (payfreq == "5" or payamt == 0):
                prodtyp = "  6.FIXED COF BULLET(FIXED RATE)"
                prodbig = "  7.FIXED COF BULLET(FIXED RATE)"
                repricdt = rec.get("EXPRDATE")
            else:
                prodtyp = "  6.CORPORATE FIXED"
                prodbig = "  7.CORPORATE FIXED"

        if loantype in _LOANTYPE_244_245_247:
            censusx = (census or "").strip()
            if censusx[:1] == "8":
                plan = censusx[1:2]
                prodtyp = f"  1.HOME PLAN {plan}"
                plan_i = int(plan) if plan.isdigit() else 0
                if plan_i <= 3:
                    prodbig = "  1.HOME 1YR FIX"
                elif plan_i <= 6:
                    prodbig = "  1.HOME 3YRS FIX"
                elif plan_i <= 7:
                    prodbig = "  1.HOME 5YRS FIX"
                else:
                    prodbig = "  1.HOME 1YR FIX"
            elif censusx[:1] == "3":
                plan = censusx[1:2]
                prodtyp = f"  1.MORE PLAN {plan}"
                if plan == "1":
                    prodbig = "  1.MORE 1YR FIX"
                elif plan == "2":
                    prodbig = "  1.MORE 3YRS FIX"
                elif plan == "3":
                    prodbig = "  1.HOME 5YRS FIX"
                else:
                    prodbig = "  1.MORE 1YR FIX"
            elif censusx[:1] == "2":
                plan = censusx[3:4] if len(censusx) > 3 else ""
                if plan in ("1", "2"):
                    prodbig = "  1.HOME 9 FIX"
                    prodtyp = "  1.HOME PLAN 9"
                elif plan in ("3", "4"):
                    prodbig = "  1.MORE 9 FIX"
                    prodtyp = "  1.MORE PLAN 9"
            else:
                prodtyp = "  1.HOME PLAN 1"
                prodbig = "  1.HOME 1YR FIX"

        if prodbig == "  1.OTHER HOUSING" and ntindex == 2:
            prodbig = "  1.OTHER PRESCRB"

        ntint = rec["NTINT"]
        balance = rec["BALANCE"] or 0.0
        if ntint != "A":
            acrint = balance - curbal - feeamt
            unearn = 0.0
        else:
            acrint = rec["INTEARN"] or 0.0
            unearn = (rec["INTAMT"] or 0.0) - (rec["INTEARN2"] or 0.0) + (rec["INTEARN3"] or 0.0)

        matdt = exprdate
        if repricdt is not None and repricdt > date(1, 1, 1):
            exprdate = repricdt
    else:
        if format_odprod(product) == "N":
            return out
        prodtyp = odprdf_format(product)
        prodbig = sodprdf_format(product)
        exprdate = rec["LMTEND"]
        riskrte = rec["OD_RISKCODE"]
        curbal = rec["BALANCE"]
        acrint = 0.0
        feeamt = 0.0
        unearn = 0.0
        matdt = exprdate

    risk_flag = riskrte in (1, 2, 3, 4)

    if risk_flag:
        amount = curbal if curbal is not None else 0.0
        out.append(dict(PRODTYP=prodtyp, PRODBIG=prodbig, PRODUCT=product, NTINDEX=ntindex,
                         SUBTYP=9, REMMTH1="     TOTAL", AMOUNT=amount, YIELD=0.0, INTTYPE=rec["INTTYPE"]))

    for subtyp, amt in ((7, acrint), (8, feeamt), (6, unearn)):
        out.append(dict(PRODTYP=prodtyp, PRODBIG=prodbig, PRODUCT=product, NTINDEX=ntindex,
                         SUBTYP=subtyp, REMMTH1="     TOTAL", AMOUNT=amt, YIELD=0.0, INTTYPE=rec["INTTYPE"]))

    remmth = _remmth_04(reptdate, matdt) if matdt else 0.0
    if not risk_flag:
        amount55 = curbal
        yield55 = (curbal or 0.0) * remmth
    else:
        amount55 = 0.0
        yield55 = 0.0
    out.append(dict(PRODTYP=prodtyp, PRODBIG=prodbig, PRODUCT=product, NTINDEX=ntindex,
                     SUBTYP=5.5, REMMTH1="     TOTAL", AMOUNT=amount55, YIELD=yield55, INTTYPE=rec["INTTYPE"]))

    amount5 = curbal
    yield5 = ((curbal or 0.0) * intrate) if not risk_flag else 0.0
    out.append(dict(PRODTYP=prodtyp, PRODBIG=prodbig, PRODUCT=product, NTINDEX=ntindex,
                     SUBTYP=5, REMMTH1="     TOTAL", AMOUNT=amount5, YIELD=yield5, INTTYPE=rec["INTTYPE"]))

    if not risk_flag:
        freq = _PAYFREQ_TO_FREQ.get(payfreq)

        bldate = None
        if payfreq in ("5", "9", " ", None) or product in _FIX_PRODUCTS:
            bldate = exprdate
        else:
            bldate = issdte
            # # DEBUG
            # print(f"bldate type: {type(bldate)}, exprdate type: {type(exprdate)}, reptdate type: {type(reptdate)}")
            # while bldate is not None and exprdate is not None and bldate <= reptdate:
            #     bldate = _nxtbldt(bldate, payfreq, freq, issdte)

        if exprdate is not None and (bldate is None or bldate > exprdate or (curbal or 0.0) <= payamt):
            bldate = exprdate

        totbal = curbal
        subtyp_inst = 11 if acctype == "LN" else None

        cur_curbal = curbal or 0.0
        while exprdate is not None and bldate is not None and bldate <= exprdate:
            matdt_i = bldate
            remmth_i = _remmth_04(reptdate, matdt_i)
            remmth1_i = remfmt_format(remmth_i)
            if bldate == exprdate:
                break
            amount_i = payamt
            yield_i = amount_i * intrate
            out.append(dict(PRODTYP=prodtyp, PRODBIG=prodbig, PRODUCT=product, NTINDEX=ntindex,
                             SUBTYP=(subtyp_inst if subtyp_inst is not None else 11),
                             REMMTH1=remmth1_i, AMOUNT=amount_i, YIELD=yield_i, INTTYPE=rec["INTTYPE"]))
            cur_curbal -= payamt
            if cur_curbal <= payamt:
                amount_i = cur_curbal
            bldate = _nxtbldt(bldate, payfreq, freq, issdte)
            if bldate > exprdate or cur_curbal <= payamt:
                bldate = exprdate

        if repricdt is not None or product in _FIX_PRODUCTS or rec["INTTYPE"] == "BLR":
            amount_f = totbal
            yield_f = (totbal or 0.0) * intrate
            subtyp_f = 12
        elif repricdt is None and ntindex != 1:
            amount_f = totbal
            yield_f = (totbal or 0.0) * intrate
            subtyp_f = 13
        else:
            amount_f, yield_f, subtyp_f = None, None, None

        remmth1_final = ">  0-1 MTH" if rec["INTTYPE"] == "BLR" else remfmt_format(
            _remmth_04(reptdate, bldate) if bldate else None
        )
        if subtyp_f is not None:
            out.append(dict(PRODTYP=prodtyp, PRODBIG=prodbig, PRODUCT=product, NTINDEX=ntindex,
                             SUBTYP=subtyp_f, REMMTH1=remmth1_final, AMOUNT=amount_f, YIELD=yield_f,
                             INTTYPE=rec["INTTYPE"]))

    return out


start_rows = []
for rec in merged_records:
    start_rows.extend(_run_start(rec))

del merged_records
gc.collect()
print(f"  START rows emitted: {len(start_rows):,}")

# ============================================================================
# STEP 10: PROC SUMMARY (detail level) -- CLASS PRODTYP PRODUCT NTINDEX
#          SUBTYP REMMTH1; WHERE INTTYPE='FIX'/'BLR'; then WAYLD=YIELD/AMOUNT
# ============================================================================
print("\nStep 10: Building detail-level FIX/BLR summaries...")


def _group_sum(rows, key_fields, sum_fields):
    groups = {}
    for r in rows:
        key = tuple(r.get(f) for f in key_fields)
        g = groups.setdefault(key, {f: None for f in sum_fields})
        for f in sum_fields:
            v = r.get(f)
            if v is not None:
                g[f] = (g[f] or 0.0) + v
    out = []
    for key, sums in groups.items():
        rec = dict(zip(key_fields, key))
        rec.update(sums)
        out.append(rec)
    return out


def _wayld(amount, yld):
    if amount in (None, 0):
        return None
    return (yld or 0.0) / amount


def _build_fixblr(rows, inttype, key_fields):
    subset = [r for r in rows if r["INTTYPE"] == inttype]
    grouped = _group_sum(subset, key_fields, ("AMOUNT", "YIELD"))
    for g in grouped:
        g["WAYLD"] = _wayld(g["AMOUNT"], g["YIELD"])
    return grouped


DETAIL_KEYS = ("PRODTYP", "PRODUCT", "NTINDEX", "SUBTYP", "REMMTH1")

fix_detail = _build_fixblr(start_rows, "FIX", DETAIL_KEYS)
blr_detail = _build_fixblr(start_rows, "BLR", DETAIL_KEYS)


def _build_fixblr2_grand(detail_rows, id_fields):
    """PROC SUMMARY WHERE SUBTYP IN (11,12,13); CLASS <id_fields>;
    then SET fix fix2; IF REMMTH1 NE blank; GRANDTOT='GRAND TOTAL';
    IF REMMTH1 NE TOTAL AND SUBTYP=13 THEN AMOUNT=0; WAYLD=0;
    then a further GRANDTOT/SUBTYP/REMMTH1-level PROC SUMMARY (FIX3/BLR3)."""
    subset2 = [r for r in detail_rows if r["SUBTYP"] in (11, 12, 13)]
    grouped2 = _group_sum(subset2, id_fields + ("SUBTYP",), ("AMOUNT", "YIELD"))
    for g in grouped2:
        g["WAYLD"] = _wayld(g["AMOUNT"], g["YIELD"])
        g["REMMTH1"] = "     TOTAL"

    combined = [dict(r) for r in detail_rows] + grouped2
    combined = [r for r in combined if (r.get("REMMTH1") or "").strip() != ""]
    for r in combined:
        r["GRANDTOT"] = "GRAND TOTAL"
        if r["REMMTH1"] != "     TOTAL" and r["SUBTYP"] == 13:
            r["AMOUNT"] = 0.0
            r["WAYLD"] = 0.0
    return combined


def _build_fix3_style(combined_rows, sum_yield: bool):
    grand = _group_sum(combined_rows, ("GRANDTOT", "SUBTYP", "REMMTH1"),
                        ("AMOUNT", "YIELD") if sum_yield else ("AMOUNT",))
    for g in grand:
        if "YIELD" not in g:
            g["YIELD"] = None
        g["WAYLD"] = _wayld(g["AMOUNT"], g["YIELD"])
    final = combined_rows + grand
    for r in final:
        if not (r.get("PRODTYP") or "").strip():
            r["PRODTYP"] = "GRAND TOTAL"
    return final


fix2_combined = _build_fixblr2_grand(fix_detail, ("PRODTYP", "PRODUCT", "NTINDEX"))
fix3 = _build_fix3_style(fix2_combined, sum_yield=True)

blr2_combined = _build_fixblr2_grand(blr_detail, ("PRODTYP", "PRODUCT", "NTINDEX"))
blr3 = _build_fix3_style(blr2_combined, sum_yield=False)

print(f"  FIX3 (detail) rows: {len(fix3):,}   BLR3 (detail) rows: {len(blr3):,}")

# ============================================================================
# STEP 11: PRODBIG-LEVEL SUMMARY (/* MORE SUMMARY */) -- same pipeline as
# Step 10 but grouped by PRODBIG only (no PRODUCT/NTINDEX split).
# ============================================================================
print("\nStep 11: Building PRODBIG-level FIX/BLR summaries...")

SUMMARY_KEYS = ("PRODBIG", "SUBTYP", "REMMTH1")

fix_summary = _build_fixblr(start_rows, "FIX", SUMMARY_KEYS)
blr_summary = _build_fixblr(start_rows, "BLR", SUMMARY_KEYS)


def _build_fixblr2_grand_prodbig(detail_rows):
    subset2 = [r for r in detail_rows if r["SUBTYP"] in (11, 12, 13)]
    grouped2 = _group_sum(subset2, ("PRODBIG", "SUBTYP"), ("AMOUNT", "YIELD"))
    for g in grouped2:
        g["WAYLD"] = _wayld(g["AMOUNT"], g["YIELD"])
        g["REMMTH1"] = "     TOTAL"

    combined = [dict(r) for r in detail_rows] + grouped2
    combined = [r for r in combined if (r.get("REMMTH1") or "").strip() != ""]
    for r in combined:
        r["GRANDTOT"] = "GRAND TOTAL"
        if r["REMMTH1"] != "     TOTAL" and r["SUBTYP"] == 13:
            r["AMOUNT"] = 0.0
            r["WAYLD"] = 0.0
    return combined


def _build_fix3_style_prodbig(combined_rows, sum_yield: bool):
    grand = _group_sum(combined_rows, ("GRANDTOT", "SUBTYP", "REMMTH1"),
                        ("AMOUNT", "YIELD") if sum_yield else ("AMOUNT",))
    for g in grand:
        if "YIELD" not in g:
            g["YIELD"] = None
        g["WAYLD"] = _wayld(g["AMOUNT"], g["YIELD"])
    final = combined_rows + grand
    for r in final:
        if not (r.get("PRODBIG") or "").strip():
            r["PRODBIG"] = "GRAND TOTAL"
    return final


fix2_pb_combined = _build_fixblr2_grand_prodbig(fix_summary)
fix3_pb = _build_fix3_style_prodbig(fix2_pb_combined, sum_yield=True)

blr2_pb_combined = _build_fixblr2_grand_prodbig(blr_summary)
blr3_pb = _build_fix3_style_prodbig(blr2_pb_combined, sum_yield=False)

print(f"  FIX3 (PRODBIG summary) rows: {len(fix3_pb):,}   BLR3 (PRODBIG summary) rows: {len(blr3_pb):,}")

del start_rows
gc.collect()

# ============================================================================
# STEP 12: REPORT RENDERING  (PROC TABULATE emulation, FORMCHAR all-blank
# -> no box-drawing characters at all; RTS=40 row-label width; CONDENSE)
# ============================================================================
print("\nStep 12: Rendering reports...")

LABEL_WIDTH   = 40   # RTS=40
AMOUNT_WIDTH  = 12   # F=COMMA12.
WAYLD_WIDTH   = 6    # F=4.2 with a little breathing room
HEADER_ROWS   = 2
FF            = "\f"

# See docstring "KNOWN SAS SOURCE BUG" section -- each report displays the
# title text that was intended for the PREVIOUS report, preserved verbatim.
REPORT_TITLES = [
    "RM DENOMINATION (FIXED RATE)",
    "RM DENOMINATION (FIXED RATE)",
    "RM DENOMINATION (BLR)",
    "RM DENOMINATION (FIXED RATE) SUMMARY",
]


def _fmt_amount(value) -> str:
    if value is None:
        return "0".rjust(AMOUNT_WIDTH)
    v = float(value)
    s = f"{v:,.0f}"
    if len(s) > AMOUNT_WIDTH:
        s = f"{v:.0f}"
    return s.rjust(AMOUNT_WIDTH)


def _fmt_wayld(value) -> str:
    if value is None:
        return "0".rjust(WAYLD_WIDTH)
    return f"{float(value):.2f}".rjust(WAYLD_WIDTH)


def _title_block(program_title4: str) -> list:
    return [
        "PUBLIC ISLAMIC BANK BERHAD",
        f"REPRICING GAP AS AT {RDATE}",
        "RISK MANAGEMENT REPORT : EIIMRM04",
        program_title4,
        "",
    ]


def _remmth1_present_order(rows) -> list:
    present = sorted({r.get("REMMTH1") or "" for r in rows}, key=_remmth1_sort_key)
    return present


def _render_detail_tabulate(rows: list, title4: str, group_dims) -> list:
    """TABLE (PRODTYP)*(PRODUCT*NTINDEX*SUBTYP), (REMMTH1)*SUM*(AMOUNT WAYLD)
    / BOX='LOANS AND ADVANCES' RTS=40 CONDENSE;
    Row key = group_dims (PRODTYP,PRODUCT,NTINDEX,SUBTYP) or (PRODBIG,SUBTYP);
    each present REMMTH1 bucket becomes a repeating AMOUNT/WAYLD column."""
    if not rows:
        return []

    remmth1_cols = _remmth1_present_order(rows)
    cell = {}
    row_keys, seen = [], set()
    for r in rows:
        key = tuple(r.get(f) for f in group_dims)
        cell.setdefault(key, {})[r.get("REMMTH1") or ""] = r
        if key not in seen:
            seen.add(key)
            row_keys.append(key)
    row_keys.sort()

    output: list = []
    lines_on_page = 0

    def _emit_header(with_titles: bool):
        nonlocal lines_on_page
        block = []
        if with_titles:
            block.append(FF)
            block.extend(_title_block(title4))
        header1 = " " * LABEL_WIDTH
        header2 = " " * LABEL_WIDTH
        for col in remmth1_cols:
            header1 += col.strip().rjust(AMOUNT_WIDTH + WAYLD_WIDTH + 1)
            header2 += "BAL O/S (RM)".rjust(AMOUNT_WIDTH) + "YIELD".rjust(WAYLD_WIDTH) + " "
        block.append(header1)
        block.append(header2)
        output.extend(block)
        lines_on_page = len(block)

    _emit_header(with_titles=True)

    for key in row_keys:
        if lines_on_page >= PAGE_SIZE:
            _emit_header(with_titles=True)

        label_parts = [str(v) if v is not None else "" for v in key]
        label = " ".join(label_parts)[:LABEL_WIDTH].ljust(LABEL_WIDTH)

        line = label
        for col in remmth1_cols:
            rec = cell[key].get(col)
            amount = rec["AMOUNT"] if rec else None
            wayld = rec["WAYLD"] if rec else None
            line += _fmt_amount(amount) + _fmt_wayld(wayld) + " "
        output.append(line)
        lines_on_page += 1

    return output


report_lines = []
report_lines += _render_detail_tabulate(fix3, REPORT_TITLES[0], ("PRODTYP", "PRODUCT", "NTINDEX", "SUBTYP"))
report_lines += _render_detail_tabulate(blr3, REPORT_TITLES[1], ("PRODTYP", "PRODUCT", "NTINDEX", "SUBTYP"))
report_lines += _render_detail_tabulate(fix3_pb, REPORT_TITLES[2], ("PRODBIG", "SUBTYP"))
report_lines += _render_detail_tabulate(blr3_pb, REPORT_TITLES[3], ("PRODBIG", "SUBTYP"))

# ============================================================================
# STEP 13: WRITE OUTPUT
# ============================================================================
with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in report_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(report_lines):,}")
# print("\n--- Report preview (first 40 lines) ---")
# for ln in report_lines[:40]:
#     print(ln)

print("\nEIIMRM04 complete.")
