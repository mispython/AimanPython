#!/usr/bin/env python3
"""
Program : EIFMNP03.py
Date    : 12.03.98
Modify  : ESMR 2004-720, 2004-579, 2006-1048
Report  : MOVEMENTS OF INTEREST IN SUSPENSE FOR THE MONTH ENDING

Dependencies:
  PBBLNFMT - Imported for session-level format availability (loan/OD product
             formats). No format function from PBBLNFMT is directly invoked in
             this program's logic; the only loan-type classification used here
             is the local LNTYP format defined below.
  PBBELF   - format_brchcd() is called directly via PUT(NTBRCH, BRCHCD.) to
             build the BRANCH display string.
  NPLNTB   - Branch-transfer and HP-centre reassignment rules applied inside
             the MONTHLY macro to the IISPREV dataset for HP/leasing accounts
             where PAIDIND='P'.
"""

import os
import duckdb
import polars as pl
from datetime import date, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------
from PBBLNFMT import (          # noqa: F401  — imported for session availability
    format_lnprod, format_lndenom, format_lnrate,
    format_liqpfmt, format_lnfmt, format_lnlob,
)
from PBBELF import format_brchcd  # PUT(NTBRCH, BRCHCD.)

# ---------------------------------------------------------------------------
# Path / file configuration
# ---------------------------------------------------------------------------
NPL_BASE        = Path(os.environ.get("NPL_BASE", "/data/npl"))

# Resolved dynamically after reading REPTDATE
INPUT_NPL_REPTDATE  = NPL_BASE / "NPL_REPTDATE.parquet"
INPUT_NPL_WIIS      = NPL_BASE / "NPL_WIIS.parquet"

# Paths set dynamically in main() once REPTMON / PREVMON are known
INPUT_NPL_LOAN      = None   # NPL_LOAN{MM}.parquet
INPUT_NPL_PLOAN     = None   # NPL_PLOAN{MM}.parquet
INPUT_NPL_IIS_PREV  = None   # NPL_IIS{MM-1}.parquet
OUTPUT_NPL_IIS_MON  = None   # NPL_IIS{MM}.parquet
OUTPUT_NPL_IIS      = NPL_BASE / "NPL_IIS.parquet"

OUTPUT_REPORT_SUMMARY = NPL_BASE / "NPL_IIS_SUMMARY_REPORT.txt"
OUTPUT_REPORT_DETAIL  = NPL_BASE / "NPL_IIS_DETAIL_REPORT.txt"

# ---------------------------------------------------------------------------
# Page layout constants (ASA carriage control)
# ---------------------------------------------------------------------------
PAGE_LINES = 60   # lines per page (SAS default)


# ===========================================================================
# Local PROC FORMAT  –  LNTYP
# (Defined within EIFMNP03 itself, NOT from PBBLNFMT)
# ===========================================================================
def format_lntyp(loantype: int) -> str:
    """
    Local PROC FORMAT LNTYP equivalent.
      128, 130, 983             -> 'HPD AITAB'
      700,705,380,381,993,996,
      720,725                   -> 'HPD CONVENTIONAL'
      200-299                   -> 'HOUSING LOANS'
      OTHER                     -> 'OTHERS'
    """
    if loantype in (128, 130, 983):
        return "HPD AITAB"
    if loantype in (700, 705, 380, 381, 993, 996, 720, 725):
        return "HPD CONVENTIONAL"
    if 200 <= loantype <= 299:
        return "HOUSING LOANS"
    return "OTHERS"


# ===========================================================================
# Macro: DCLVAR / NXTBLDT  (day-array helpers — used by the commented-out
# OVINT macro; retained here as comments for traceability)
# ===========================================================================
#   RETAIN D1-D12 31  D4 D6 D9 D11 30
#   ARRAY LDAY D1-D12
#
# NXTBLDT macro:
#   DD = DAY(ISSDTE)
#   MM = MONTH(BLDATE) + 1
#   YY = YEAR(BLDATE)
#   IF MM > 12 THEN MM=1; YY+1
#   IF MM=2: leap-year check -> D2=28 or 29
#   IF DD > LDAY(MM) THEN DD=LDAY(MM)
#   BLDATE = MDY(MM,DD,YY)

_DAYS_IN_MONTH = {1:31,2:28,3:31,4:30,5:31,6:30,7:31,8:31,9:30,10:31,11:30,12:31}

def _next_bldate(bldate: date, issdte: date) -> date:
    """Python equivalent of %NXTBLDT macro."""
    dd = issdte.day
    mm = bldate.month + 1
    yy = bldate.year
    if mm > 12:
        mm = 1
        yy += 1
    if mm == 2:
        d2 = 29 if (yy % 4 == 0 and (yy % 100 != 0 or yy % 400 == 0)) else 28
        lday = d2
    else:
        lday = _DAYS_IN_MONTH[mm]
    if dd > lday:
        dd = lday
    return date(yy, mm, dd)


# ===========================================================================
# NPLNTB branch-transfer logic
# (SAS: %INC PGM(NPLNTB) — all blocks are commented-out in SAS source;
#  the Python equivalent is likewise kept as commented-out code for
#  traceability.  No active branch remapping is applied.)
# ===========================================================================
def apply_nplntb(pendbrh: int, ntbrch: int, costctr: int):
    """
    Equivalent of %INC PGM(NPLNTB).
    All remapping rules are commented-out in the original SAS source.
    Returns (pendbrh, ntbrch, costctr) unchanged.

    *** TRANSFER OF BRANCH ***
    # IF  PENDBRH=236 THEN PENDBRH=069; ELSE
    # IF  PENDBRH=033 THEN PENDBRH=140; ELSE
    # ...  (full list in NPLNTB.py)
    #
    *** SETUP OF HP CENTRE ***
    # IF  PENDBRH=024 THEN PENDBRH=800; ELSE
    # ...  (full list in NPLNTB.py)
    """
    # All remapping rules are commented out in the SAS source.
    return pendbrh, ntbrch, costctr


# ===========================================================================
# Helper utilities
# ===========================================================================
def _s(v, default=0.0):
    """SAS SUM() semantics: None treated as zero."""
    return float(v) if v is not None else default


def _si(v, default=0):
    return int(v) if v is not None else default


def _branch_label(ntbrch: int) -> str:
    """PUT(NTBRCH, BRCHCD.) || ' ' || PUT(NTBRCH, Z3.)"""
    return f"{format_brchcd(ntbrch)} {ntbrch:03d}"


def _risk(days: int, borstat: str, user5: str) -> str:
    """Risk classification applied in DATA LOAN3."""
    if days > 364 or borstat == 'W':
        return "BAD"
    if days > 273:
        return "DOUBTFUL"
    if days > 182:
        return "SUBSTANDARD 2"
    # days < 90 AND user5='N'  ->  SUBSTANDARD-1
    # ELSE                     ->  SUBSTANDARD-1
    return "SUBSTANDARD-1"


# ===========================================================================
# SAS SUM() wrapper (null-ignoring multi-argument sum)
# ===========================================================================
def _sum(*args):
    return sum(_s(a) for a in args)


# ===========================================================================
# DATA REPTDATE
# ===========================================================================
def read_reptdate(path: Path):
    """
    DATA REPTDATE;
      SET NPL.REPTDATE;
      IF MONTH(REPTDATE)=1 THEN MM1=12; ELSE MM1=MONTH(REPTDATE)-1;
      CALL SYMPUT('RDATE',   PUT(REPTDATE,WORDDATX18.));
      CALL SYMPUT('REPTMON', PUT(MONTH(REPTDATE),Z2.));
      CALL SYMPUT('PREVMON', PUT(MM1,Z2.));
    """
    row = pl.read_parquet(path).row(0, named=True)
    reptdate = row["REPTDATE"]
    mm1 = 12 if reptdate.month == 1 else reptdate.month - 1
    rdate   = reptdate.strftime("%d %B %Y").upper()
    reptmon = f"{reptdate.month:02d}"
    prevmon = f"{mm1:02d}"
    styr    = reptdate.year     # RETAIN STYR — set on _N_=1
    stmth   = 1                 # RETAIN STMTH 1
    return reptdate, rdate, reptmon, prevmon, styr, stmth


# ===========================================================================
# DATA LOANWOFF  (merge LOAN&REPTMON with WIIS)
# ===========================================================================
def build_loanwoff(df_loan: pl.DataFrame, df_wiis: pl.DataFrame) -> pl.DataFrame:
    """
    PROC SORT DATA=NPL.LOAN&REPTMON; BY ACCTNO;
    PROC SORT DATA=NPL.WIIS;         BY ACCTNO;
    DATA LOANWOFF;
      MERGE NPL.LOAN&REPTMON  NPL.WIIS(IN=AA DROP=NOTENO NTBRCH);
      BY ACCTNO;
      IF LOANTYPE IN (380,381) THEN FEEAMT=FEETOT2;
      IF AA THEN WRITEOFF='Y'; ELSE WRITEOFF='N';
      IF LOANTYPE IN (983,993) THEN WDOWNIND='N';
      IF IISP=. THEN IISP=0;
      IF OIP =. THEN OIP =0;
      IF EARNTERM IN (0,.) THEN EARNTERM=NOTETERM;
    """
    wiis_drop = [c for c in df_wiis.columns if c not in ("ACCTNO",) and c not in ("NOTENO","NTBRCH")]
    df_w = df_wiis.select(["ACCTNO"] + [c for c in wiis_drop if c != "ACCTNO"]).with_columns(
        pl.lit("Y").alias("_AA")
    )

    df = df_loan.join(df_w, on="ACCTNO", how="left")

    df = df.with_columns([
        pl.when(pl.col("_AA") == "Y").then(pl.lit("Y")).otherwise(pl.lit("N")).alias("WRITEOFF"),
        # IF LOANTYPE IN (380,381) THEN FEEAMT=FEETOT2
        pl.when(pl.col("LOANTYPE").is_in([380, 381]))
          .then(pl.col("FEETOT2"))
          .otherwise(pl.col("FEEAMT"))
          .alias("FEEAMT"),
        # IF LOANTYPE IN (983,993) THEN WDOWNIND='N'
        pl.when(pl.col("LOANTYPE").is_in([983, 993]))
          .then(pl.lit("N"))
          .otherwise(pl.col("WDOWNIND"))
          .alias("WDOWNIND"),
        pl.col("IISP").fill_null(0.0),
        pl.col("OIP").fill_null(0.0),
        # IF EARNTERM IN (0,.) THEN EARNTERM=NOTETERM
        pl.when((pl.col("EARNTERM").is_null()) | (pl.col("EARNTERM") == 0))
          .then(pl.col("NOTETERM"))
          .otherwise(pl.col("EARNTERM"))
          .alias("EARNTERM"),
    ]).drop("_AA")

    return df


# ===========================================================================
# Core IIS calculation helpers  (shared between LOAN1 and LOAN2)
# ===========================================================================

def _calc_iis_termchg(loantype, bldate, issdte, reptdate, termchg, earnterm):
    """
    Calculate unearned hiring charges (UHC) and base IIS loop sum.
    Returns (iis, uhc, remmth2).
    """
    iis = 0.0
    uhc = 0.0
    remmth2 = 0

    if bldate is None or issdte is None:
        return iis, uhc, remmth2

    remmth1 = earnterm - ((bldate.year  - issdte.year)  * 12 + bldate.month  - issdte.month  + 1)
    remmth2 = earnterm - ((reptdate.year - issdte.year) * 12 + reptdate.month - issdte.month + 1)
    if remmth2 < 0:
        remmth2 = 0

    if loantype in (128, 130):
        remmth1 -= 3
    else:
        remmth1 -= 1

    if remmth1 >= remmth2:
        for remmth in range(remmth1, remmth2 - 1, -1):
            iis += 2 * (remmth + 1) * termchg / (earnterm * (earnterm + 1))

    if remmth2 > 0:
        uhc = remmth2 * (remmth2 + 1) * termchg / (earnterm * (earnterm + 1))

    return iis, uhc, remmth2


def _calc_suspend_loop(issdte, styr, stmth, remmth2, termchg, earnterm):
    """
    Calculates SUSPEND via the REMMTHS loop.
    DO REMMTH = REMMTHS TO REMMTH2 BY -1:
       SUSPEND + 2*(REMMTH+1)*TERMCHG/(EARNTERM*(EARNTERM+1));
    """
    remmths = earnterm - ((styr - issdte.year) * 12 + stmth - issdte.month + 1)
    suspend = 0.0
    for remmth in range(remmths, remmth2 - 1, -1):
        suspend += 2 * (remmth + 1) * termchg / (earnterm * (earnterm + 1))
    return suspend


# ===========================================================================
# DATA LOAN1  — IIS for EXISTING NPL accounts  (EXIST='Y')
# ===========================================================================
def process_loan1(df_loanwoff: pl.DataFrame, reptdate: date, styr: int, stmth: int) -> list:
    """
    DATA LOAN1;
      SET LOANWOFF;
      IF EXIST='Y';
      ...complex IIS calculation...
    """
    results = []

    for r in df_loanwoff.filter(pl.col("EXIST") == "Y").iter_rows(named=True):
        iis     = 0.0
        suspend = 0.0
        uhc     = 0.0
        oi      = 0.0
        oisusp  = 0.0
        recover = 0.0
        oirecv  = 0.0
        oirecc  = 0.0
        oiw     = 0.0
        recc    = 0.0

        iisp    = _s(r.get("IISP"))
        oip     = _s(r.get("OIP"))
        iispw   = _s(r.get("IISPW"))
        writeoff  = r.get("WRITEOFF", "N")
        wdownind  = r.get("WDOWNIND") or ""
        borstat   = r.get("BORSTAT") or ""
        loantype  = _si(r.get("LOANTYPE"))
        termchg   = _s(r.get("TERMCHG"))
        earnterm  = _si(r.get("EARNTERM"))
        days      = _si(r.get("DAYS"))
        user5     = r.get("USER5") or ""
        issdte    = r.get("ISSDTE")
        bldate    = r.get("BLDATE")
        curbal    = _s(r.get("CURBAL"))
        feetot2   = _s(r.get("FEETOT2"))
        feeamta   = _s(r.get("FEEAMTA"))
        feeamt5   = _s(r.get("FEEAMT5"))
        feeamt    = _s(r.get("FEEAMT"))
        accrual   = _s(r.get("ACCRUAL"))
        marketvl  = _s(r.get("MARKETVL"))
        ntbrch    = _si(r.get("NTBRCH"))
        rescheind = r.get("RESCHEIND") or ""

        if writeoff == "Y" and wdownind != "Y":
            borstat = "W"

        npl_cond = (days > 89 or borstat in ("F","R","I")
                    or (user5 == "N" and loantype not in (983, 993)))

        if bldate is not None and bldate > date(1960, 1, 1) and termchg > 0:
            if npl_cond:
                iis, uhc, remmth2 = _calc_iis_termchg(
                    loantype, bldate, issdte, reptdate, termchg, earnterm)
                # OI = SUM(FEETOT2,(-1)*FEEAMTA,FEEAMT5)
                oi = _sum(feetot2, -feeamta, feeamt5)
                # SUSPEND loop (REMMTHS to REMMTH2)
                suspend = _calc_suspend_loop(issdte, styr, stmth, remmth2, termchg, earnterm)
                if loantype not in (128, 130):
                    # OISUSP = SUM(FEEAMT,(-1)*FEEAMTA,FEEAMT5)
                    oisusp = _sum(feeamt, -feeamta, feeamt5)
                if remmth2 > 0:
                    uhc = remmth2 * (remmth2 + 1) * termchg / (earnterm * (earnterm + 1))
        elif npl_cond:
            oi     = _sum(feetot2, -feeamta, feeamt5)
            oisusp = _sum(feeamt,  -feeamta, feeamt5)

        netbal = curbal - uhc

        if netbal <= iisp:
            if days > 89 or borstat in ("F","R","I") or user5 == "N":
                iis = netbal

        if borstat == "W":
            iispw = iisp
            oiw   = oip
        else:
            recover = iisp + suspend - iis
            if recover < 0:
                suspend += -recover   # suspend = suspend - recover  (recover < 0)
                recover = 0.0
            if recover > iisp:
                recc    = recover - iisp
                recover = iisp

            if loantype not in (128, 130):
                oirecv = oip - oi
                if oirecv < 0:
                    oisusp -= oirecv   # oisusp = oisusp - oirecv
                    oirecv = 0.0
                if oisusp < 0:
                    oirecv -= oisusp
                if oirecv > oip:
                    oirecc = oirecv - oip
                    oirecv = oip

        # Handle zero TERMCHG
        if termchg == 0:
            if borstat == "R":
                netexp = curbal - iisp - marketvl
            else:
                netexp = curbal - iisp
            if (netexp > 0 and days > 89) or borstat == "R":
                iis    = recover
                recover = 0.0
                oi     = _sum(feetot2, -feeamta, feeamt5)
                oirecv = 0.0

        # IF LOANTYPE IN (720,725) THEN IIS=ACCRUAL
        if loantype in (720, 725):
            iis = accrual

        # OISUSP = OIRECV + OIRECC + OIW - OIP + OI  (recalculate twice per SAS)
        oisusp = oirecv + oirecc + oiw - oip + oi
        if oisusp < 0:
            oirecv -= oisusp
        if oirecv > oip:
            oirecc = oirecv - oip
            oirecv = oip
        oisusp = oirecv + oirecc + oiw - oip + oi

        # BRANCH = PUT(NTBRCH,BRCHCD.) || ' ' || PUT(NTBRCH,Z3.)
        branch = _branch_label(ntbrch)

        # WRITEOFF='Y' block
        if writeoff == "Y":
            suspend = _s(r.get("WSUSPEND"))
            oisusp  = _s(r.get("WOISUSP"))
            if wdownind != "Y":
                recover = _s(r.get("WRECOVER"))
                recc    = _s(r.get("WRECC"))
                oirecv  = _s(r.get("WOIRECV"))
                oirecc  = _s(r.get("WOIRECC"))
                iis   = 0.0
                iispw = _sum(iisp, suspend, -recover, -recc)
                oi    = 0.0
                oiw   = _sum(oip, oisusp, -oirecv, -oirecc)
            else:
                oisusp = _s(r.get("WOISUSP"))
                iispw  = _s(r.get("WIISPW"))
                iis    = _sum(iisp, suspend, -recover, -recc, -iispw)
                if iis < 0:
                    recover = 0.0
                iis    = _sum(iisp, suspend, -recover, -recc, -iispw)
                oiw    = _s(r.get("WOIW"))
                oi     = _sum(oip, oisusp, -oirecv, -oirecc, -oiw)
                if oi < 0:
                    oirecv = 0.0
                    oirecc = 0.0
                oi = _sum(oip, oisusp, -oirecv, -oirecc, -oiw)
            # null-to-zero guards
            oip    = _s(oip);    iisp   = _s(iisp)
            suspend= _s(suspend); oisusp = _s(oisusp)
            recover= _s(recover); oirecv = _s(oirecv)
            recc   = _s(recc);   oirecc = _s(oirecc)

        totiis = iis + oi

        # RESCHEIND='Y' block
        if rescheind == "Y":
            suspend = _s(r.get("WSUSPEND"))
            oisusp  = _s(r.get("WOISUSP"))
            recover = _s(r.get("WRECOVER"))
            recc    = _s(r.get("WRECC"))
            oirecv  = _s(r.get("WOIRECV"))
            oirecc  = _s(r.get("WOIRECC"))
            iis     = _sum(iisp, suspend, -recover, -recc, -iispw)
            oi      = _sum(oip,  oisusp,  -oirecv,  -oirecc, -oiw)
            totiis  = iis + oi

        results.append({
            "BRANCH": branch, "NTBRCH": ntbrch,
            "ACCTNO": r.get("ACCTNO"), "NOTENO": r.get("NOTENO"),
            "NAME": r.get("NAME"),     "NETPROC": r.get("NETPROC"),
            "CURBAL": curbal, "BORSTAT": borstat, "DAYS": days,
            "IIS": iis, "UHC": uhc, "NETBAL": netbal,
            "IISP": iisp, "SUSPEND": suspend, "RECOVER": recover,
            "RECC": recc, "IISPW": iispw,
            "OIP": oip, "OISUSP": oisusp, "OI": oi,
            "OIRECV": oirecv, "OIRECC": oirecc, "OIW": oiw,
            "TOTIIS": totiis,
            "LOANTYP": format_lntyp(loantype),
            "EXIST": "Y",
            "COSTCTR": r.get("COSTCTR"), "PENDBRH": r.get("PENDBRH"),
            "USER5": user5, "WDOWNIND": wdownind,
            "RESCHEIND": rescheind, "ACCRUAL": accrual,
            "LOANTYPE": loantype,
        })

    return results


# ===========================================================================
# DATA LOAN2  — IIS for CURRENT NPL accounts  (EXIST ^= 'Y')
# ===========================================================================
def process_loan2(df_loanwoff: pl.DataFrame, reptdate: date) -> list:
    """
    DATA LOAN2;
      SET LOANWOFF;
      IF EXIST ^= 'Y';
      ...
    """
    results = []

    for r in df_loanwoff.filter(pl.col("EXIST") != "Y").iter_rows(named=True):
        iis     = 0.0
        uhc     = 0.0
        oi      = 0.0
        suspend = 0.0
        oisusp  = 0.0
        recover = 0.0
        oirecv  = 0.0
        oirecc  = 0.0
        oiw     = 0.0
        recc    = 0.0
        iispw   = 0.0

        iisp      = _s(r.get("IISP"))
        oip       = _s(r.get("OIP"))
        writeoff  = r.get("WRITEOFF", "N")
        wdownind  = r.get("WDOWNIND") or ""
        borstat   = r.get("BORSTAT") or ""
        loantype  = _si(r.get("LOANTYPE"))
        termchg   = _s(r.get("TERMCHG"))
        earnterm  = _si(r.get("EARNTERM"))
        days      = _si(r.get("DAYS"))
        user5     = r.get("USER5") or ""
        issdte    = r.get("ISSDTE")
        bldate    = r.get("BLDATE")
        curbal    = _s(r.get("CURBAL"))
        feetot2   = _s(r.get("FEETOT2"))
        feeamta   = _s(r.get("FEEAMTA"))
        feeamt5   = _s(r.get("FEEAMT5"))
        accrual   = _s(r.get("ACCRUAL"))
        ntbrch    = _si(r.get("NTBRCH"))
        rescheind = r.get("RESCHEIND") or ""

        if writeoff == "Y" and wdownind != "Y":
            borstat = "W"

        # SAS condition: IF BLDATE>0 & TERMCHG>0 OR (USER5='N' AND ...)
        # Python: must bracket correctly
        bldate_ok = (bldate is not None and bldate > date(1960, 1, 1) and termchg > 0)
        user5_ok  = (user5 == "N" and loantype not in (983, 993))

        if bldate_ok or user5_ok:
            if issdte is not None and bldate is not None:
                iis, uhc, remmth2 = _calc_iis_termchg(
                    loantype, bldate, issdte, reptdate, termchg, earnterm)
            # else: iis=0, uhc=0, remmth2=0  (bldate/issdte missing)
        else:
            # ELSE branch: only UHC
            if issdte is not None and reptdate is not None:
                remmth2 = earnterm - (
                    (reptdate.year - issdte.year) * 12
                    + reptdate.month - issdte.month + 1)
                if remmth2 < 0:
                    remmth2 = 0
                if remmth2 > 0:
                    uhc = remmth2 * (remmth2 + 1) * termchg / (earnterm * (earnterm + 1))

        oi = _sum(feetot2, -feeamta, feeamt5)

        if loantype in (720, 725):
            iis = accrual

        suspend = iis
        oisusp  = oi
        netbal  = curbal - uhc

        if writeoff == "Y":
            suspend = _s(r.get("WSUSPEND"))
            oisusp  = _s(r.get("WOISUSP"))
            if wdownind != "Y":
                recover = _s(r.get("WRECOVER"))
                recc    = _s(r.get("WRECC"))
                oirecv  = _s(r.get("WOIRECV"))
                oirecc  = _s(r.get("WOIRECC"))
                iis   = 0.0
                iispw = _sum(iisp, suspend, -recover, -recc)
                oi    = 0.0
                oiw   = _sum(oip, oisusp, -oirecv, -oirecc)
            else:
                oisusp = _s(r.get("WOISUSP"))
                iispw  = _s(r.get("WIISPW"))
                iis    = _sum(iisp, suspend, -recover, -recc, -iispw)
                if iis < 0:
                    recover = 0.0
                iis    = _sum(iisp, suspend, -recover, -recc, -iispw)
                oiw    = _s(r.get("WOIW"))
                oi     = _sum(oip, oisusp, -oirecv, -oirecc, -oiw)
                if oi < 0:
                    oirecv = 0.0
                    oirecc = 0.0
                oi = _sum(oip, oisusp, -oirecv, -oirecc, -oiw)
            oip    = _s(oip);    iisp   = _s(iisp)
            suspend= _s(suspend); oisusp = _s(oisusp)
            recover= _s(recover); oirecv = _s(oirecv)
            recc   = _s(recc);   oirecc = _s(oirecc)

        totiis = iis + oi

        # RESCHEIND='Y' block
        if rescheind == "Y":
            suspend = _s(r.get("WSUSPEND"))
            oisusp  = _s(r.get("WOISUSP"))
            recover = _s(r.get("WRECOVER"))
            recc    = _s(r.get("WRECC"))
            oirecv  = _s(r.get("WOIRECV"))
            oirecc  = _s(r.get("WOIRECC"))
            iis     = _sum(iisp, suspend, -recover, -recc, -iispw)
            oi      = _sum(oip,  oisusp,  -oirecv,  -oirecc, -oiw)
            totiis  = iis + oi

        branch = _branch_label(ntbrch)

        results.append({
            "BRANCH": branch, "NTBRCH": ntbrch,
            "ACCTNO": r.get("ACCTNO"), "NOTENO": r.get("NOTENO"),
            "NAME": r.get("NAME"),     "NETPROC": r.get("NETPROC"),
            "CURBAL": curbal, "BORSTAT": borstat, "DAYS": days,
            "IIS": iis, "UHC": uhc, "NETBAL": netbal,
            "IISP": iisp, "SUSPEND": suspend, "RECOVER": recover,
            "RECC": recc, "IISPW": iispw,
            "OIP": oip, "OISUSP": oisusp, "OI": oi,
            "OIRECV": oirecv, "OIRECC": oirecc, "OIW": oiw,
            "TOTIIS": totiis,
            "LOANTYP": format_lntyp(loantype),
            "EXIST": r.get("EXIST"),
            "COSTCTR": r.get("COSTCTR"), "PENDBRH": r.get("PENDBRH"),
            "USER5": user5, "WDOWNIND": wdownind,
            "RESCHEIND": rescheind, "ACCRUAL": accrual,
            "LOANTYPE": loantype,
        })

    return results


# ===========================================================================
# %MACRO MONTHLY — previous-month IIS merge logic
# ===========================================================================

def _apply_user5_ois_adj(row: dict) -> dict:
    """
    Repeated USER5='N' adjustment block that appears in multiple branches
    of the MONTHLY macro.
    """
    iisp   = _s(row["IISP"]);   iis    = _s(row["IIS"])
    oip    = _s(row["OIP"]);    oi     = _s(row["OI"])
    user5  = row.get("USER5","")
    iispw  = _s(row.get("IISPW"))

    suspend = _s(row["SUSPEND"])
    recover = _s(row["RECOVER"])
    recc    = _s(row["RECC"])
    oisusp  = _s(row["OISUSP"])
    oirecv  = _s(row["OIRECV"])
    oirecc  = _s(row["OIRECC"])

    if user5 == "N":
        if iis < iisp:
            suspend = 0.0; recover = iisp - iis; recc = 0.0
        elif iis >= iisp:
            suspend = iis - iisp; recover = 0.0; recc = 0.0
        if iisp == 0:
            suspend = iis; recc = iis - suspend
        if oi < oip:
            oisusp = 0.0; oirecv = oip - oi; oirecc = 0.0
        elif oi >= oip:
            oisusp = oi - oip; oirecv = 0.0; oirecc = 0.0
        if oip == 0:
            oisusp = oi; oirecc = oi - oisusp

    row.update({
        "SUSPEND": suspend, "RECOVER": recover, "RECC": recc,
        "OISUSP": oisusp, "OIRECV": oirecv, "OIRECC": oirecc,
    })
    return row


def _merge_monthly_loan1(df_loan1_rows: list, iisprev_rows: list) -> list:
    """
    Equivalent of the DATA LOAN1 merge step inside %MACRO MONTHLY (EXISTING NPL).

    PROC SORT DATA=NPL.IIS&PREVMON (...RENAME...) OUT=IISPREV NODUPKEY; BY ACCTNO NOTENO;
    DATA IISPREV;
       SET IISPREV;
       IF LOANTYPE IN (128,130,...) AND PAIDIND='P' THEN %INC PGM(NPLNTB);
       BRANCH = PUT(NTBRCH,BRCHCD.) || ' ' || PUT(NTBRCH,Z3.);
       null-guards on PDAYS/PSUSPEND/POISUSP/PIISP/POIP/POI;
    DATA LOAN1(MERGE IISPREV LOAN1); BY ACCTNO; ...
    """
    # Index prev by ACCTNO (NODUPKEY on ACCTNO NOTENO)
    prev_by_acctno: dict = {}
    seen = set()
    for p in iisprev_rows:
        key = (p["ACCTNO"], p["NOTENO"])
        if key not in seen:
            seen.add(key)
            prev_by_acctno[p["ACCTNO"]] = p   # keep first per ACCTNO NOTENO

    # Apply NPLNTB to IISPREV rows for eligible loan types
    hp_leasing_types = {128,130,131,132,380,381,390,700,705,720,725,983,993,996}
    for p in prev_by_acctno.values():
        ltype = _si(p.get("LOANTYPE"))
        if ltype in hp_leasing_types and p.get("PAIDIND") == "P":
            pendbrh, ntbrch, costctr = apply_nplntb(
                _si(p.get("PENDBRH")), _si(p.get("NTBRCH")), _si(p.get("COSTCTR")))
            p["PENDBRH"] = pendbrh; p["NTBRCH"] = ntbrch; p["COSTCTR"] = costctr
        p["BRANCH"]   = _branch_label(_si(p.get("NTBRCH")))
        p["PDAYS"]    = _si(p.get("PDAYS"))
        p["PSUSPEND"] = _s(p.get("PSUSPEND"))
        p["POISUSP"]  = _s(p.get("POISUSP"))
        p["PIISP"]    = _s(p.get("PIISP"))
        p["POIP"]     = _s(p.get("POIP"))
        p["POI"]      = _s(p.get("POI"))

    results = []
    loan1_by_acctno = {row["ACCTNO"]: row for row in df_loan1_rows}
    all_accts = set(prev_by_acctno) | set(loan1_by_acctno)

    for acctno in all_accts:
        a_row = loan1_by_acctno.get(acctno)
        b_row = prev_by_acctno.get(acctno)

        in_a = a_row is not None
        in_b = b_row is not None

        # IF ((A AND B) OR (B AND NOT A)) AND EXIST='Y'
        if not (((in_a and in_b) or (in_b and not in_a))
                and (a_row or b_row or {}).get("EXIST","") == "Y"):
            # If a_row EXIST check fails via b_row when A not present, use b's EXIST
            exist_val = (a_row or b_row or {}).get("EXIST","")
            if not (((in_a and in_b) or (in_b and not in_a)) and exist_val == "Y"):
                continue

        row = dict(a_row or {})
        row.update({k: v for k, v in (b_row or {}).items() if k not in row})
        # previous-month fields (from IISPREV)
        pdays    = _si((b_row or {}).get("PDAYS"))
        psuspend = _s((b_row or {}).get("PSUSPEND"))
        poisusp  = _s((b_row or {}).get("POISUSP"))
        piisp    = _s((b_row or {}).get("PIISP"))
        poip     = _s((b_row or {}).get("POIP"))
        ppoi     = _s((b_row or {}).get("POI"))
        precc    = _s((b_row or {}).get("PRECC"))
        poirecc  = _s((b_row or {}).get("POIRECC"))
        precover = _s((b_row or {}).get("PRECOVER"))
        poirecv  = _s((b_row or {}).get("POIRECV"))

        borstat  = row.get("BORSTAT","")
        rescheind= row.get("RESCHEIND","")
        days     = _si(row.get("DAYS"))
        curbal   = _s(row.get("CURBAL"))
        iisp     = _s(row.get("IISP"))
        oip      = _s(row.get("OIP"))
        iispw    = _s(row.get("IISPW"))
        oiw      = _s(row.get("OIW"))
        iis      = _s(row.get("IIS"))
        oi       = _s(row.get("OI"))
        suspend  = _s(row.get("SUSPEND"))
        oisusp   = _s(row.get("OISUSP"))
        recover  = _s(row.get("RECOVER"))
        recc     = _s(row.get("RECC"))
        oirecv   = _s(row.get("OIRECV"))
        oirecc   = _s(row.get("OIRECC"))
        user5    = row.get("USER5","")

        # --- A/C SETTLE FOR EXISTING NPL ---
        if ((in_b and not in_a) or (curbal <= 0 and ppoi <= 0)) and \
           borstat not in ("F","I","R","W","S"):
            row["IISP"]    = piisp;    row["RECOVER"] = piisp
            row["SUSPEND"] = psuspend; row["RECC"]    = psuspend
            row["OIP"]     = poip;     row["OIRECV"]  = poip
            row["OISUSP"]  = poisusp;  row["OIRECC"]  = poisusp
            row["CURBAL"]  = 0.0;      row["NETBAL"]  = 0.0; row["DAYS"] = 0
            row["OI"]  = _sum(poip, poisusp, -poip, -poisusp)
            row["IIS"] = _sum(piisp, psuspend, -piisp, -psuspend)
            row["TOTIIS"] = row["IIS"] + row["OI"]
            results.append(row); continue

        if borstat == "W" or rescheind == "Y":
            results.append(row); continue

        if in_a and in_b:
            # --- CONTINUE PERFORMING (days < 90 and pdays < 90) ---
            if days < 90 and pdays < 90:
                row["SUSPEND"] = iis
                row = _apply_user5_ois_adj({**row,
                    "IIS":iis,"OI":oi,"IISP":iisp,"OIP":oip,"IISPW":iispw})
                results.append(row); continue

            # --- TURN PERFORMING (days < 90 and pdays >= 90) ---
            if days < 90 and pdays >= 90:
                if borstat not in ("F","I","R"):
                    row["SUSPEND"] = psuspend; row["RECC"]   = psuspend
                    row["OISUSP"]  = poisusp;  row["OIRECC"] = poisusp
                    row["TOTIIS"]  = iis + oi
                row = _apply_user5_ois_adj({**row,
                    "IIS":iis,"OI":oi,"IISP":iisp,"OIP":oip,"IISPW":iispw})
                results.append(row); continue

            # --- TURN NPL FROM PERFORMING (days >= 90 and pdays < 90) ---
            if days >= 90 and pdays < 90:
                if borstat not in ("F","I","R"):
                    new_recc    = precc
                    new_recover = precover
                    new_suspend = _sum(iis, iisp, -new_recover, new_recc)
                    if new_suspend < 0:
                        new_recover = _sum(new_recover, -new_suspend)
                        new_suspend = 0.0
                        if new_recover > iisp:
                            new_recc = _sum(new_recc, new_recover - iisp)
                    new_oirecc = poirecc
                    new_oirecv = poirecv
                    new_oisusp = _sum(oi, oip, -new_oirecv, new_oirecc)
                    if new_oisusp < 0:
                        new_oirecv = _sum(new_oirecv, -new_oisusp)
                        new_oisusp = 0.0
                        if new_oirecv > oip:
                            new_oirecc = _sum(new_oirecc, new_oirecv - oip)
                    row.update({"RECC":new_recc,"RECOVER":new_recover,"SUSPEND":new_suspend,
                                "OIRECC":new_oirecc,"OIRECV":new_oirecv,"OISUSP":new_oisusp,
                                "TOTIIS":iis+oi})
                row = _apply_user5_ois_adj({**row,
                    "IIS":iis,"OI":oi,"IISP":iisp,"OIP":oip,"IISPW":iispw})
                results.append(row); continue

            # --- CONTINUE NPL (days >= 90 and pdays >= 90) ---
            if days >= 90 and pdays >= 90:
                if borstat not in ("F","I","R"):
                    new_recover = precover
                    new_recc    = precc
                    new_suspend = _sum(iis, -iisp, new_recover, new_recc)
                    if new_suspend < 0:
                        new_recover = _sum(new_recover, -new_suspend)
                        new_suspend = 0.0
                        if new_recover > iisp:
                            new_recc = _sum(new_recc, new_recover, -iisp)
                    new_oirecv = poirecv
                    new_oirecc = poirecc
                    new_oisusp = _sum(oi, -oip, new_oirecv, new_oirecc)
                    if new_oisusp < 0:
                        new_oirecv = _sum(new_oirecv, -new_oisusp)
                        new_oisusp = 0.0
                        if new_oirecv > oip:
                            new_oirecc = _sum(new_oirecc, new_oirecv, -oip)
                    row.update({"RECOVER":new_recover,"RECC":new_recc,"SUSPEND":new_suspend,
                                "OIRECV":new_oirecv,"OIRECC":new_oirecc,"OISUSP":new_oisusp,
                                "TOTIIS":iis+oi})
                results.append(row); continue

        results.append(row)
    return results


def _merge_monthly_loan2(df_loan2_rows: list, iisprev_rows: list,
                         df_ploan: pl.DataFrame) -> list:
    """
    Equivalent of the DATA LOAN2 merge step inside %MACRO MONTHLY (CURRENT NPL).

    DATA IISPREV (filtered: PIISP=0 AND POIP=0 AND EXIST NE 'Y');
    DATA LOAN2 (MERGE IISPREV LOAN2); BY ACCTNO; ...
    """
    # Rebuild IISPREV filtered to PIISP=0 AND POIP=0 AND EXIST NE 'Y'
    ploan_accts = set(df_ploan["ACCTNO"].to_list())

    prev_filtered: dict = {}
    seen = set()
    for p in iisprev_rows:
        key = (p["ACCTNO"], p["NOTENO"])
        if key in seen:
            continue
        seen.add(key)
        if _s(p.get("PIISP")) == 0 and _s(p.get("POIP")) == 0 and p.get("EXIST","") != "Y":
            p["BRANCH"] = _branch_label(_si(p.get("NTBRCH")))
            prev_filtered[p["ACCTNO"]] = p

    loan2_by_acctno = {row["ACCTNO"]: row for row in df_loan2_rows}
    all_accts = set(prev_filtered) | set(loan2_by_acctno)

    results = []
    for acctno in all_accts:
        a_row = loan2_by_acctno.get(acctno)
        b_row = prev_filtered.get(acctno)

        in_a = a_row is not None
        in_b = b_row is not None

        row = dict(a_row or {})
        row.update({k: v for k, v in (b_row or {}).items() if k not in row})

        pdays    = _si((b_row or {}).get("PDAYS"))
        psuspend = _s((b_row or {}).get("PSUSPEND"))
        poisusp  = _s((b_row or {}).get("POISUSP"))
        piisp    = _s((b_row or {}).get("PIISP"))
        poip     = _s((b_row or {}).get("POIP"))
        precc    = _s((b_row or {}).get("PRECC"))
        poirecc  = _s((b_row or {}).get("POIRECC"))
        precover = _s((b_row or {}).get("PRECOVER"))
        poirecv  = _s((b_row or {}).get("POIRECV"))

        borstat   = row.get("BORSTAT","")
        rescheind = row.get("RESCHEIND","")
        days      = _si(row.get("DAYS"))
        iisp      = _s(row.get("IISP"))
        oip       = _s(row.get("OIP"))
        iispw     = _s(row.get("IISPW"))
        oiw       = _s(row.get("OIW"))
        iis       = _s(row.get("IIS"))
        oi        = _s(row.get("OI"))
        suspend   = _s(row.get("SUSPEND"))
        oisusp    = _s(row.get("OISUSP"))
        recover   = _s(row.get("RECOVER"))
        recc      = _s(row.get("RECC"))
        oirecv    = _s(row.get("OIRECV"))
        oirecc    = _s(row.get("OIRECC"))
        user5     = row.get("USER5","")

        # IF (B AND NOT A): A/C settled
        if in_b and not in_a:
            row["IISP"]    = piisp;    row["RECOVER"] = piisp
            row["SUSPEND"] = psuspend; row["RECC"]    = psuspend
            row["OIP"]     = poip;     row["OIRECV"]  = poip
            row["OISUSP"]  = poisusp;  row["OIRECC"]  = poisusp
            row["CURBAL"]  = 0.0;      row["NETBAL"]  = 0.0; row["DAYS"] = 0
            row["OI"]  = _sum(poip, poisusp, -poip,  -poisusp)
            row["IIS"] = _sum(piisp, psuspend, -piisp, -psuspend)
            row["TOTIIS"] = row["IIS"] + row["OI"]
            results.append(row); continue

        # NEW NPL FOR THE MTH
        if in_a and not in_b:
            if (days >= 90 or borstat in ("F","I","R","W") or user5 == "N"):
                results.append(row)
            continue

        if borstat == "W" or rescheind == "Y":
            results.append(row); continue

        if in_a and in_b and borstat != "W":
            # --- CONTINUE PERFORMING ---
            if days < 90 and pdays < 90:
                if borstat not in ("F","I","R"):
                    row["SUSPEND"] = psuspend; row["RECC"]   = psuspend
                    row["OISUSP"]  = poisusp;  row["OIRECC"] = poisusp
                    row["TOTIIS"]  = iis + oi
                row = _apply_user5_ois_adj({**row,
                    "IIS":iis,"OI":oi,"IISP":iisp,"OIP":oip,"IISPW":iispw})
                if user5 != "N":
                    row["SUSPEND"] = _sum(row["SUSPEND"], row["RECC"])
                    row["OISUSP"]  = _sum(row["OISUSP"],  row["OIRECC"])
                results.append(row); continue

            # --- TURN PERFORMING FROM NPL ---
            if days < 90 and pdays >= 90:
                if borstat not in ("F","I","R"):
                    row["SUSPEND"] = psuspend; row["RECC"]   = psuspend
                    row["OISUSP"]  = poisusp;  row["OIRECC"] = poisusp
                    row["OI"]  = _sum(oip, poisusp, -poip, -poisusp, -oiw)
                    row["IIS"] = _sum(iisp, psuspend, -piisp, -psuspend, -iispw)
                    row["TOTIIS"] = row["IIS"] + row["OI"]
                row = _apply_user5_ois_adj({**row,
                    "IIS":_s(row.get("IIS")),"OI":_s(row.get("OI")),
                    "IISP":iisp,"OIP":oip,"IISPW":iispw})
                results.append(row); continue

            # --- TURN NPL FROM PERFORMING ---
            if days >= 90 and pdays < 90:
                if borstat not in ("F","I","R"):
                    row["RECC"]   = _sum(recc, precc)
                    row["SUSPEND"]= _sum(suspend, row["RECC"])
                    row["OIRECC"] = _sum(oirecc, poirecc)
                    row["OISUSP"] = _sum(oisusp, row["OIRECC"])
                    row["TOTIIS"] = iis + oi
                row = _apply_user5_ois_adj({**row,
                    "IIS":iis,"OI":oi,"IISP":iisp,"OIP":oip,"IISPW":iispw})
                results.append(row); continue

            # --- CONTINUE NPL ---
            if days >= 90 and pdays >= 90:
                if borstat not in ("F","I","R"):
                    row["RECC"]   = _sum(recc, precc)
                    row["SUSPEND"]= _sum(suspend, row["RECC"])
                    row["OIRECC"] = _sum(oirecc, poirecc)
                    row["OISUSP"] = _sum(oisusp, row["OIRECC"])
                    row["TOTIIS"] = iis + oi
                results.append(row); continue

        results.append(row)
    return results


# ===========================================================================
# %MACRO MONTHLY  — top-level dispatcher
# ===========================================================================
def run_monthly(loan1_rows: list, loan2_rows: list, reptmon: str, prevmon: str,
                iis_prev_path: Path, ploan_path: Path) -> tuple:
    """
    %MACRO MONTHLY;
      %IF "&REPTMON" EQ "01" %THEN %DO;
        ... zero out IISPCUM / OIPCUM / POI
      %END;
      %ELSE %DO;
        ... full merge logic
      %END;
    %MEND MONTHLY;
    """
    if reptmon == "01":
        # %IF "&REPTMON" EQ "01": add zero columns, no prev-month merge
        for row in loan1_rows:
            row.update({"IISPCUM": 0.0, "OIPCUM": 0.0, "POI": 0.0})
        for row in loan2_rows:
            row.update({"IISPCUM": 0.0, "OIPCUM": 0.0, "POI": 0.0})
        return loan1_rows, loan2_rows

    # Read NPL.IIS&PREVMON — NODUPKEY BY ACCTNO NOTENO
    # Rename: DAYS->PDAYS, SUSPEND->PSUSPEND, OISUSP->POISUSP,
    #         IISP->PIISP, OIP->POIP, OI->POI, RECC->PRECC,
    #         OIRECC->POIRECC, RECOVER->PRECOVER, OIRECV->POIRECV
    df_prev = pl.read_parquet(iis_prev_path).rename({
        "DAYS":    "PDAYS",    "SUSPEND": "PSUSPEND", "OISUSP":  "POISUSP",
        "IISP":    "PIISP",    "OIP":     "POIP",     "OI":      "POI",
        "RECC":    "PRECC",    "OIRECC":  "POIRECC",  "RECOVER": "PRECOVER",
        "OIRECV":  "POIRECV",
    })
    # NODUPKEY by ACCTNO NOTENO — keep first occurrence
    df_prev = df_prev.unique(subset=["ACCTNO","NOTENO"], keep="first")
    iisprev_rows = df_prev.to_dicts()

    # PLOAN for LOAN2 filtering
    df_ploan = pl.read_parquet(ploan_path).select(
        ["ACCTNO","NOTENO","CURBAL","DAYS","BORSTAT","NTBRCH","COSTCTR"])

    loan1_out = _merge_monthly_loan1(loan1_rows, iisprev_rows)
    loan2_out = _merge_monthly_loan2(loan2_rows, iisprev_rows, df_ploan)
    return loan1_out, loan2_out


# ===========================================================================
# Report writers
# ===========================================================================
def _write_report_header(f, page: int, rdate: str, tbl_label: str):
    """Write page header with ASA form-feed ('1')."""
    f.write(f"1{'PUBLIC BANK - (NPL FROM 3 MONTHS & ABOVE) - NEW':^132}\n")
    f.write(f" MOVEMENTS OF INTEREST IN SUSPENSE FOR THE MONTH ENDING {rdate} {tbl_label}\n")
    f.write(f" {'':^132}\n")
    f.write(" \n")


def write_summary_report(df: pl.DataFrame, output_path: Path, rdate: str):
    """
    PROC TABULATE equivalent — grouped summary by LOANTYP / RISK / BRANCH.
    ASA carriage control: '1' = form-feed, ' ' = single space.
    """
    COL_W = 15
    HDR = (f"{'RISK/BRANCH':<29}{'N':>10}"
           f"{'CURBAL':>{COL_W}}{'UHC':>{COL_W}}{'NETBAL':>{COL_W}}"
           f"{'IISP':>{COL_W}}{'SUSPEND':>{COL_W}}{'RECOVER':>{COL_W}}"
           f"{'RECC':>{COL_W}}{'IISPW':>{COL_W}}{'IIS':>{COL_W}}"
           f"{'OIP':>{COL_W}}{'OISUSP':>{COL_W}}{'OIRECV':>{COL_W}}"
           f"{'OIRECC':>{COL_W}}{'OIW':>{COL_W}}{'OI':>{COL_W}}"
           f"{'TOTIIS':>{COL_W}}")
    agg_cols = [
        pl.len().alias("N"),
        *[pl.sum(c).alias(c) for c in [
            "CURBAL","UHC","NETBAL","IISP","SUSPEND","RECOVER","RECC",
            "IISPW","IIS","OIP","OISUSP","OIRECV","OIRECC","OIW","OI","TOTIIS"]]
    ]
    summary = (df.group_by(["LOANTYP","RISK","BRANCH"])
                 .agg(agg_cols)
                 .sort(["LOANTYP","RISK","BRANCH"]))

    with open(output_path, "w") as f:
        page = 1
        _write_report_header(f, page, rdate, "(EXISTING AND CURRENT)")
        f.write(f" {HDR}\n")
        f.write(f" {'-'*len(HDR)}\n")
        lines = 6
        cur_ltyp = None

        def fmt(v): return f"{v:>{COL_W},.2f}"

        for row in summary.iter_rows(named=True):
            if lines >= PAGE_LINES - 2:
                page += 1
                _write_report_header(f, page, rdate, "(EXISTING AND CURRENT)")
                f.write(f" {HDR}\n"); f.write(f" {'-'*len(HDR)}\n")
                lines = 6

            if cur_ltyp != row["LOANTYP"]:
                cur_ltyp = row["LOANTYP"]
                f.write(f" {cur_ltyp}\n"); lines += 1

            rb = f"  {row['RISK']:<15}{row['BRANCH']:<12}"
            line = (f"{rb:<29}{row['N']:>10,}"
                    + "".join(fmt(row[c]) for c in [
                        "CURBAL","UHC","NETBAL","IISP","SUSPEND","RECOVER","RECC",
                        "IISPW","IIS","OIP","OISUSP","OIRECV","OIRECC","OIW","OI","TOTIIS"]))
            f.write(f" {line}\n"); lines += 1


def write_detail_report(df: pl.DataFrame, output_path: Path, rdate: str):
    """
    PROC PRINT equivalent — sorted by LOANTYP / BRANCH / RISK / DAYS / ACCTNO.
    PAGEBY BRANCH; SUMBY RISK.
    ASA carriage control.
    """
    df_sorted = df.sort(["LOANTYP","BRANCH","RISK","DAYS","ACCTNO"])

    HDR_LINE = (f"{'MNI ACCOUNT NO':<15}{'NAME':<25}{'DAYS':>6}"
                f"{'BORSTAT':>8}{'LIMIT':>15}"
                f"{'CURBAL(A)':>15}{'UHC(B)':>15}{'NETBAL(C)':>15}"
                f"{'IISP(D)':>15}{'SUSPEND(E)':>15}{'RECOVER(F)':>15}"
                f"{'RECC(G)':>15}{'IISPW(H)':>15}{'IIS(I)':>15}"
                f"{'OIP(J)':>15}{'OISUSP(K)':>15}{'OIRECV(L)':>15}"
                f"{'OIRECC(M)':>15}{'OIW(N)':>15}{'OI(O)':>15}"
                f"{'TOTIIS':>15}")

    def fmt(v): return f"{_s(v):>15,.2f}"

    with open(output_path, "w") as f:
        page = 1
        lines = PAGE_LINES + 1   # force header on first row
        cur_ltyp = cur_branch = cur_risk = None

        risk_sums  = {c: 0.0 for c in ["NETPROC","CURBAL","UHC","NETBAL",
                                         "IISP","SUSPEND","RECOVER","RECC",
                                         "IISPW","IIS","OIP","OISUSP","OIRECV",
                                         "OIRECC","OIW","OI","TOTIIS"]}

        def write_header():
            nonlocal lines, page
            f.write(f"1{'PUBLIC BANK - (NPL FROM 3 MONTHS & ABOVE) - NEW':^180}\n")
            f.write(f" MOVEMENTS OF INTEREST IN SUSPENSE FOR THE MONTH ENDING {rdate} (EXISTING AND CURRENT)\n")
            f.write(f" {HDR_LINE}\n")
            f.write(f" {'-'*len(HDR_LINE)}\n")
            lines = 4
            page += 1

        def write_risk_sum(label):
            s = risk_sums
            line = (f"{'':15}{'*** ' + label + ' SUBTOTAL ***':<25}{'':>6}{'':>8}"
                    + "".join(fmt(s[c]) for c in [
                        "NETPROC","CURBAL","UHC","NETBAL",
                        "IISP","SUSPEND","RECOVER","RECC","IISPW","IIS",
                        "OIP","OISUSP","OIRECV","OIRECC","OIW","OI","TOTIIS"]))
            f.write(f" {line}\n")
            for c in risk_sums: risk_sums[c] = 0.0

        write_header()

        for row in df_sorted.iter_rows(named=True):
            if lines >= PAGE_LINES:
                write_header()

            if cur_ltyp != row["LOANTYP"]:
                if cur_risk is not None: write_risk_sum(cur_risk)
                cur_ltyp = row["LOANTYP"]
                cur_branch = cur_risk = None
                f.write(f" {cur_ltyp}\n"); lines += 1

            if cur_branch != row["BRANCH"]:
                if cur_risk is not None: write_risk_sum(cur_risk)
                cur_branch = row["BRANCH"]
                cur_risk   = None
                f.write(f" BRANCH: {cur_branch}\n"); lines += 1

            if cur_risk != row["RISK"]:
                if cur_risk is not None: write_risk_sum(cur_risk)
                cur_risk = row["RISK"]
                f.write(f"   RISK: {cur_risk}\n"); lines += 1

            sum_cols = ["NETPROC","CURBAL","UHC","NETBAL",
                        "IISP","SUSPEND","RECOVER","RECC","IISPW","IIS",
                        "OIP","OISUSP","OIRECV","OIRECC","OIW","OI","TOTIIS"]
            for c in sum_cols:
                risk_sums[c] += _s(row.get(c))

            line = (f"{str(row.get('ACCTNO','')):<15}{str(row.get('NAME','')):<25}"
                    f"{_si(row.get('DAYS')):>6}{str(row.get('BORSTAT',''))[:2]:>8}"
                    + "".join(fmt(row.get(c)) for c in [
                        "NETPROC","CURBAL","UHC","NETBAL",
                        "IISP","SUSPEND","RECOVER","RECC","IISPW","IIS",
                        "OIP","OISUSP","OIRECV","OIRECC","OIW","OI","TOTIIS"]))
            f.write(f" {line}\n"); lines += 1

        if cur_risk is not None:
            write_risk_sum(cur_risk)


# ===========================================================================
# main()
# ===========================================================================
def main():
    global INPUT_NPL_LOAN, INPUT_NPL_PLOAN, INPUT_NPL_IIS_PREV, OUTPUT_NPL_IIS_MON

    print("EIFMNP03 — NPL Interest In Suspense Processing")
    print("=" * 70)

    # -----------------------------------------------------------------------
    # DATA REPTDATE
    # -----------------------------------------------------------------------
    reptdate, rdate, reptmon, prevmon, styr, stmth = read_reptdate(INPUT_NPL_REPTDATE)
    print(f"Reporting Date : {rdate}   reptmon={reptmon}  prevmon={prevmon}")

    INPUT_NPL_LOAN      = NPL_BASE / f"NPL_LOAN{reptmon}.parquet"
    INPUT_NPL_PLOAN     = NPL_BASE / f"NPL_PLOAN{reptmon}.parquet"
    INPUT_NPL_IIS_PREV  = NPL_BASE / f"NPL_IIS{prevmon}.parquet"
    OUTPUT_NPL_IIS_MON  = NPL_BASE / f"NPL_IIS{reptmon}.parquet"

    # -----------------------------------------------------------------------
    # PROC SORT + DATA LOANWOFF
    # -----------------------------------------------------------------------
    print("Reading LOAN and WIIS...")
    df_loan = pl.read_parquet(INPUT_NPL_LOAN).sort("ACCTNO")
    df_wiis = pl.read_parquet(INPUT_NPL_WIIS).sort("ACCTNO")
    df_loanwoff = build_loanwoff(df_loan, df_wiis)
    print(f"  LOANWOFF rows: {len(df_loanwoff):,}")

    # -----------------------------------------------------------------------
    # DATA LOAN1 / LOAN2  (base IIS calculations)
    # -----------------------------------------------------------------------
    print("Calculating IIS — existing NPL (LOAN1)...")
    loan1_rows = process_loan1(df_loanwoff, reptdate, styr, stmth)

    print("Calculating IIS — current NPL (LOAN2)...")
    loan2_rows = process_loan2(df_loanwoff, reptdate)

    print(f"  LOAN1 base rows: {len(loan1_rows):,}   LOAN2 base rows: {len(loan2_rows):,}")

    # -----------------------------------------------------------------------
    # %MACRO MONTHLY
    # -----------------------------------------------------------------------
    print("Running MONTHLY macro (prev-month merge)...")
    loan1_rows, loan2_rows = run_monthly(
        loan1_rows, loan2_rows, reptmon, prevmon,
        INPUT_NPL_IIS_PREV, INPUT_NPL_PLOAN)

    # -----------------------------------------------------------------------
    # DATA LOAN3 = SET LOAN1 LOAN2  +  RISK classification + WHERE filter
    # -----------------------------------------------------------------------
    print("Building LOAN3...")
    df_loan3 = pl.DataFrame(loan1_rows + loan2_rows)

    df_loan3 = df_loan3.with_columns(
        pl.struct(["DAYS","BORSTAT","USER5"])
        .map_elements(
            lambda x: _risk(_si(x["DAYS"]), x["BORSTAT"] or "", x["USER5"] or ""),
            return_dtype=pl.Utf8)
        .alias("RISK")
    )

    # WHERE (COSTCTR < 3000 OR COSTCTR > 3999) AND
    #       COSTCTR NOT IN (4043,4048) AND COSTCTR NE .
    df_loan3 = df_loan3.filter(
        pl.col("COSTCTR").is_not_null() &
        (~pl.col("COSTCTR").is_in([4043, 4048])) &
        ((pl.col("COSTCTR") < 3000) | (pl.col("COSTCTR") > 3999))
    )

    # PROC SORT NODUPKEY BY ACCTNO NOTENO  (applied to all three output datasets)
    df_loan3     = df_loan3.unique(subset=["ACCTNO","NOTENO"], keep="first")
    df_iis_month = df_loan3.clone()
    df_iis       = df_loan3.clone()

    print(f"  LOAN3 rows after filter/dedup: {len(df_loan3):,}")

    # -----------------------------------------------------------------------
    # Output: NPL.IIS&REPTMON  +  NPL.IIS
    # -----------------------------------------------------------------------
    df_iis_month.write_parquet(OUTPUT_NPL_IIS_MON)
    df_iis.write_parquet(OUTPUT_NPL_IIS)
    print(f"  Written: {OUTPUT_NPL_IIS_MON}")
    print(f"  Written: {OUTPUT_NPL_IIS}")

    # -----------------------------------------------------------------------
    # Reports   (%TBLS and %DTLS — both executed for I=3)
    # OPTIONS NOCENTER NODATE NONUMBER MISSING=0
    # %LET TBL3=(EXISTING AND CURRENT)
    # -----------------------------------------------------------------------
    print("Writing summary report (%TBLS)...")
    write_summary_report(df_loan3, OUTPUT_REPORT_SUMMARY, rdate)
    print(f"  Written: {OUTPUT_REPORT_SUMMARY}")

    print("Writing detail report (%DTLS)...")
    write_detail_report(df_loan3, OUTPUT_REPORT_DETAIL, rdate)
    print(f"  Written: {OUTPUT_REPORT_DETAIL}")

    # -----------------------------------------------------------------------
    # Console summary
    # -----------------------------------------------------------------------
    print("\n" + "=" * 70)
    for col in ("IIS","OI","TOTIIS","SUSPEND","RECOVER"):
        if col in df_loan3.columns:
            print(f"  Total {col:10s}: {df_loan3[col].sum():>20,.2f}")

    risk_tbl = (df_loan3.group_by("RISK")
                        .agg([pl.len().alias("N"), pl.sum("TOTIIS")])
                        .sort("RISK"))
    print("\n  By RISK:")
    for row in risk_tbl.iter_rows(named=True):
        print(f"    {row['RISK']:<20}  {row['N']:>6,} accts  TOTIIS {row['TOTIIS']:>20,.2f}")
    print("=" * 70)
    print("Processing complete.")


if __name__ == "__main__":
    main()
