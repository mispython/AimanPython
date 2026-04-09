#!/usr/bin/env python3
"""
Program : EIFMNP06.py
Date    : 18.03.98
Modify  : ESMR 2004-720, 2004-579, 2006-1048, 2006-1281
Report  : MOVEMENTS OF SPECIFIC PROVISION FOR THE MONTH ENDING
          (BASED ON DEPRECIATED PURCHASE PRICE FOR UNSCHEDULED GOODS)

Dependencies:
  PBBLNFMT - Imported for session-level format availability (loan/OD product
             formats). No format function from PBBLNFMT is directly invoked in
             this program's logic; the only loan-type classification used here
             is the local LNTYP format defined below as format_lntyp().
  PBBELF   - format_brchcd() is called directly via PUT(NTBRCH, BRCHCD.) to
             build the BRANCH display string in DATA LOAN1 and DATA LOAN2.
  NPLNTB   - Branch-transfer and HP-centre reassignment rules applied inside
             the MONTHLY macro to the SP2PREV dataset for HP/leasing accounts
             where PAIDIND='P'. All remapping rules are commented-out in the
             original SAS NPLNTB source; likewise kept as comments here.
"""

import os
import polars as pl
from datetime import date
from pathlib import Path

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------
from PBBLNFMT import (          # noqa: F401  — imported for session availability
    format_lnprod, format_lndenom, format_lnrate,
    format_liqpfmt, format_lnfmt, format_lnlob,
)
from PBBELF import format_brchcd   # PUT(NTBRCH, BRCHCD.)

# ---------------------------------------------------------------------------
# Path / file configuration
# ---------------------------------------------------------------------------
NPL_BASE = Path(os.environ.get("NPL_BASE", "/data/npl"))

INPUT_NPL_REPTDATE  = NPL_BASE / "NPL_REPTDATE.parquet"
INPUT_NPL_WSP2      = NPL_BASE / "NPL_WSP2.parquet"

# Resolved dynamically in main() once REPTMON / PREVMON are known
INPUT_NPL_LOAN      = None   # NPL_LOAN{MM}.parquet
INPUT_NPL_PLOAN     = None   # NPL_PLOAN{MM}.parquet
INPUT_NPL_IIS       = None   # NPL_IIS{MM}.parquet   (for IIS merge)
INPUT_NPL_SP2_PREV  = None   # NPL_SP2{MM-1}.parquet
OUTPUT_NPL_SP2_MON  = None   # NPL_SP2{MM}.parquet
OUTPUT_NPL_SP2      = NPL_BASE / "NPL_SP2.parquet"

OUTPUT_REPORT_SUMMARY = NPL_BASE / "NPL_SP2_SUMMARY_REPORT.txt"
OUTPUT_REPORT_DETAIL  = NPL_BASE / "NPL_SP2_DETAIL_REPORT.txt"

# ---------------------------------------------------------------------------
# Page layout constant (SAS default page length)
# ---------------------------------------------------------------------------
PAGE_LINES = 60


# ===========================================================================
# Local PROC FORMAT — LNTYP
# (Defined within EIFMNP06 itself, NOT from PBBLNFMT)
# ===========================================================================
def format_lntyp(loantype: int) -> str:
    """
    PROC FORMAT;
      VALUE LNTYP 128,130,983             = 'HPD AITAB'
                  700,705,993,996,380,381,
                  720,725                 = 'HPD CONVENTIONAL'
                  200-299                 = 'HOUSING LOANS'
                  OTHER                   = 'OTHERS';
    """
    if loantype in (128, 130, 983):
        return "HPD AITAB"
    if loantype in (700, 705, 993, 996, 380, 381, 720, 725):
        return "HPD CONVENTIONAL"
    if 200 <= loantype <= 299:
        return "HOUSING LOANS"
    return "OTHERS"


# ===========================================================================
# NPLNTB branch-transfer / HP-centre logic
# (SAS: %INC PGM(NPLNTB) — all remapping blocks are commented-out in the
#  original SAS source; likewise kept as comments here for traceability.)
# ===========================================================================
def apply_nplntb(pendbrh: int, ntbrch: int, costctr: int):
    """
    Equivalent of %INC PGM(NPLNTB).
    All remapping rules are commented-out in the original SAS source.
    Returns (pendbrh, ntbrch, costctr) unchanged.

    *** TRANSFER OF BRANCH ***
    # IF  PENDBRH=236 THEN PENDBRH=069; ELSE
    # IF  PENDBRH=033 THEN PENDBRH=140; ELSE
    # IF  PENDBRH=048 THEN PENDBRH=113; ELSE
    # IF  PENDBRH=107 THEN PENDBRH=171; ELSE
    # IF  PENDBRH=111 THEN PENDBRH=231; ELSE
    # IF  PENDBRH=138 THEN PENDBRH=094; ELSE
    # IF  PENDBRH=162 THEN PENDBRH=036; ELSE
    # IF  PENDBRH=184 THEN PENDBRH=032; ELSE
    # IF  PENDBRH=223 THEN PENDBRH=024; ELSE
    # IF  PENDBRH=227 THEN PENDBRH=081; ELSE
    # IF  PENDBRH=229 THEN PENDBRH=151; ELSE
    # IF  PENDBRH=240 THEN PENDBRH=133; ELSE
    # IF  PENDBRH=241 THEN PENDBRH=019; ELSE
    # IF  PENDBRH=246 THEN PENDBRH=146; ELSE
    # IF  PENDBRH=250 THEN PENDBRH=092; ELSE
    # IF  PENDBRH=051 THEN PENDBRH=209; ELSE
    # IF  PENDBRH=173 THEN PENDBRH=056; ELSE   ESMR:2009-1451
    # IF  PENDBRH=255 THEN PENDBRH=068;        ESMR:2009-2086
    # IF  PENDBRH=200 THEN PENDBRH=122;        ESMR:2010-3206

    *** SETUP OF HP CENTRE ***
    # IF  PENDBRH=024 THEN PENDBRH=800; ELSE
    # IF  PENDBRH=045 THEN PENDBRH=800; ELSE
    # ... (full list in NPLNTB.py)
    """
    # All remapping rules are commented out in the SAS source.
    return pendbrh, ntbrch, costctr


# ===========================================================================
# Helpers
# ===========================================================================
def _s(v, default: float = 0.0) -> float:
    """SAS SUM() null-ignoring semantics."""
    return float(v) if v is not None else default


def _si(v, default: int = 0) -> int:
    return int(v) if v is not None else default


def _sum(*args) -> float:
    """Multi-argument SAS SUM() — ignores None."""
    return sum(_s(a) for a in args)


def _branch_label(ntbrch: int) -> str:
    """PUT(NTBRCH, BRCHCD.) || ' ' || PUT(NTBRCH, Z3.)"""
    return f"{format_brchcd(ntbrch)} {ntbrch:03d}"


def _risk(days: int, borstat: str, user5: str) -> str:
    """
    Risk classification applied in DATA LOAN3:
      IF DAYS>364 OR BORSTAT='W' THEN RISK='BAD'
      ELSE IF DAYS>273 THEN RISK='DOUBTFUL'
      ELSE IF DAYS>182 THEN RISK='SUBSTANDARD 2'
      ELSE IF DAYS<90 AND USER5='N' THEN RISK='SUBSTANDARD-1'
      ELSE RISK='SUBSTANDARD-1'
    """
    if days > 364 or borstat == "W":
        return "BAD"
    if days > 273:
        return "DOUBTFUL"
    if days > 182:
        return "SUBSTANDARD 2"
    return "SUBSTANDARD-1"


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
    row      = pl.read_parquet(path).row(0, named=True)
    reptdate = row["REPTDATE"]
    mm1      = 12 if reptdate.month == 1 else reptdate.month - 1
    rdate    = reptdate.strftime("%d %B %Y").upper()
    reptmon  = f"{reptdate.month:02d}"
    prevmon  = f"{mm1:02d}"
    styr     = reptdate.year   # RETAIN STYR — set on _N_=1 from YEAR(REPTDATE)
    stmth    = 1               # RETAIN STMTH 1
    return reptdate, rdate, reptmon, prevmon, styr, stmth


# ===========================================================================
# DATA LOANWOFF  (merge LOAN&REPTMON with WSP2, then with IIS)
# ===========================================================================
def build_loanwoff(df_loan: pl.DataFrame,
                   df_wsp2: pl.DataFrame,
                   df_iis:  pl.DataFrame) -> pl.DataFrame:
    """
    PROC SORT DATA=NPL.LOAN&REPTMON; BY ACCTNO;
    PROC SORT DATA=NPL.WSP2;         BY ACCTNO;
    PROC SORT DATA=NPL.IIS&REPTMON (KEEP=ACCTNO IIS) OUT=IIS; BY ACCTNO;

    DATA LOANWOFF;
      MERGE NPL.LOAN&REPTMON  NPL.WSP2(IN=AA DROP=NOTENO NTBRCH);
      BY ACCTNO;
      IF LOANTYPE IN (983,993) THEN WDOWNIND='N';
      IF AA THEN WRITEOFF='Y'; ELSE WRITEOFF='N';
      IF EARNTERM IN (0,.) THEN EARNTERM=NOTETERM;

    DATA LOANWOFF;
      MERGE LOANWOFF(IN=A) IIS;
      BY ACCTNO;
      IF A;
    """
    # Drop NOTENO and NTBRCH from WSP2 before merge, per SAS DROP=
    wsp2_cols = [c for c in df_wsp2.columns if c not in ("NOTENO", "NTBRCH")]
    df_w = df_wsp2.select(wsp2_cols).with_columns(pl.lit("Y").alias("_AA"))

    df = df_loan.join(df_w, on="ACCTNO", how="left")

    df = df.with_columns([
        pl.when(pl.col("_AA") == "Y")
          .then(pl.lit("Y"))
          .otherwise(pl.lit("N"))
          .alias("WRITEOFF"),
        # IF LOANTYPE IN (983,993) THEN WDOWNIND='N'
        pl.when(pl.col("LOANTYPE").is_in([983, 993]))
          .then(pl.lit("N"))
          .otherwise(pl.col("WDOWNIND"))
          .alias("WDOWNIND"),
        # IF EARNTERM IN (0,.) THEN EARNTERM=NOTETERM
        pl.when(pl.col("EARNTERM").is_null() | (pl.col("EARNTERM") == 0))
          .then(pl.col("NOTETERM"))
          .otherwise(pl.col("EARNTERM"))
          .alias("EARNTERM"),
    ]).drop("_AA")

    # Second DATA LOANWOFF: merge with IIS, IF A (keep only loan rows)
    df_iis_sel = df_iis.select(["ACCTNO", "IIS"])
    df = df.join(df_iis_sel, on="ACCTNO", how="left")
    df = df.with_columns(pl.col("IIS").fill_null(0.0))

    return df


# ===========================================================================
# Core SP calculation  (shared logic for LOAN1 and LOAN2)
# ===========================================================================
def _calc_uhc(loantype: int, bldate, issdte, reptdate: date,
              termchg: float, earnterm: int) -> float:
    """
    IF BLDATE>0 & TERMCHG>0 THEN DO;
      REMMTH2 = EARNTERM - ((YEAR(REPTDATE)-YEAR(ISSDTE))*12 +
                MONTH(REPTDATE)-MONTH(ISSDTE)+1);
      IF REMMTH2<0 THEN REMMTH2=0;
      IF REMMTH2>0 THEN
        UHC = REMMTH2*(REMMTH2+1)*TERMCHG/(EARNTERM*(EARNTERM+1));
    """
    if bldate is None or issdte is None:
        return 0.0
    if not (bldate > date(1960, 1, 1) and termchg > 0):
        return 0.0
    remmth2 = earnterm - (
        (reptdate.year - issdte.year) * 12
        + reptdate.month - issdte.month + 1)
    if remmth2 < 0:
        remmth2 = 0
    if remmth2 > 0:
        return remmth2 * (remmth2 + 1) * termchg / (earnterm * (earnterm + 1))
    return 0.0


def _calc_sp(days: int, borstat: str, user5: str,
             loantype: int, appvalue: float, census7: str,
             hardcode: str, wrealvl: float,
             osprin: float, otherfee: float,
             reptdate: date, issdte) -> tuple:
    """
    Core SP / MARKETVL / NETEXP calculation block used identically in
    both LOAN1 and LOAN2.
    Returns (sp, marketvl, netexp).
    """
    marketvl = 0.0
    netexp   = 0.0
    sp       = 0.0

    hp_types = (705, 128, 700, 130, 380, 381, 720, 725)

    use_appvalue = (
        appvalue > 0
        and (loantype in hp_types or census7 == "9")
        and (days > 89 or user5 == "N")
        and borstat not in ("F", "R", "I", "Y", "W")
        and loantype not in (983, 993)
    )

    if use_appvalue:
        age = 0
        if issdte is not None:
            age = int(reptdate.year - issdte.year
                      + (reptdate.month - issdte.month) / 12)
        if census7 != "9":
            marketvl = appvalue - appvalue * age * 0.2
        if hardcode == "Y":
            marketvl = wrealvl
        if marketvl < 0:
            marketvl = 0.0

        if days > 273:
            netexp = osprin + otherfee
        else:
            netexp = osprin + otherfee - marketvl

        # SELECT; WHEN...
        if days > 364:
            sp = netexp
        elif days > 273:
            sp = netexp / 2
        elif days > 89:
            sp = netexp * 0.2
        elif days < 90:      # WHEN (DAYS<90)
            sp = netexp * 0.2
        else:
            sp = 0.0
    else:
        if borstat not in ("R",):
            marketvl = 0.0
        if hardcode == "Y":
            marketvl = wrealvl
        netexp = osprin + otherfee - marketvl

        if days > 364 or borstat in ("F", "R", "I", "W"):
            sp = netexp
        elif days > 273:
            sp = netexp / 2
        elif days > 89 and borstat == "Y":
            sp = netexp / 5
        else:
            sp = 0.0

    if sp < 0:
        sp = 0.0
    return sp, marketvl, netexp


# ===========================================================================
# DATA LOAN1  — SP for EXISTING NPL accounts  (EXIST='Y')
# ===========================================================================
def process_loan1(df_loanwoff: pl.DataFrame,
                  reptdate: date) -> list:
    """
    DATA LOAN1;
      KEEP BRANCH NTBRCH ACCTNO NOTENO NAME DAYS BORSTAT NETPROC CURBAL
           UHC NETBAL IIS OSPRIN MARKETVL NETEXP SPP2 SPPL RECOVER
           SPPW SP LOANTYP VINNO CENSUS7 OTHERFEE EXIST COSTCTR USER5
           PENDBRH WDOWNIND RESCHEIND;
      SET LOANWOFF;
      IF EXIST='Y';
      ...
    """
    results = []

    for r in df_loanwoff.filter(pl.col("EXIST") == "Y").iter_rows(named=True):
        writeoff  = r.get("WRITEOFF", "N") or "N"
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
        iis       = _s(r.get("IIS"))
        spp2      = _s(r.get("SPP2"))
        appvalue  = _s(r.get("APPVALUE"))
        census7   = str(r.get("CENSUS7") or "")
        hardcode  = r.get("HARDCODE") or "N"
        wrealvl   = _s(r.get("WREALVL"))
        ntbrch    = _si(r.get("NTBRCH"))
        rescheind = r.get("RESCHEIND") or ""

        if writeoff == "Y" and wdownind != "Y":
            borstat = "W"

        # UHC calculation
        # IF BLDATE>0 & TERMCHG>0 THEN DO;
        #   IF DAYS>89 | BORSTAT IN ('F','R','I') OR USER5='N' THEN DO;
        #     REMMTH2 = ...
        #     IF REMMTH2>0 THEN UHC = ...
        #   END;
        # END;
        uhc = 0.0
        npl_cond = (days > 89 or borstat in ("F", "R", "I") or user5 == "N")
        if (bldate is not None and bldate > date(1960, 1, 1)
                and termchg > 0 and npl_cond):
            uhc = _calc_uhc(loantype, bldate, issdte, reptdate, termchg, earnterm)

        # IF CURBAL=. THEN CURBAL=0  (already handled by _s above)
        netbal = curbal - uhc
        osprin = curbal - uhc - iis

        # OTHERFEE
        if loantype in (380, 381):
            otherfee = _sum(r.get("FEEAMT"), -_s(r.get("FEETOT2")))
        else:
            otherfee = _sum(r.get("FEEAMT8"),
                            -_s(r.get("FEETOT2")),
                            r.get("FEEAMTA"),
                            -_s(r.get("FEEAMT5")))
        if otherfee < 0:
            otherfee = 0.0
        if loantype in (983, 993):
            otherfee = 0.0

        sp, marketvl, netexp = _calc_sp(
            days, borstat, user5, loantype, appvalue, census7,
            hardcode, wrealvl, osprin, otherfee, reptdate, issdte)

        # SPPL = SP - SPP2; IF SPPL<0 THEN SPPL=0
        sppl = sp - spp2
        if sppl < 0:
            sppl = 0.0

        # IF HARDCODE='Y' THEN DO;
        #   IF WSPPL NE . THEN SPPL=WSPPL;
        #   IF WSP NE . THEN SP=WSP;
        # END;
        if hardcode == "Y":
            wsppl = r.get("WSPPL")
            if wsppl is not None:
                sppl = _s(wsppl)
            wsp = r.get("WSP")
            if wsp is not None:
                sp = _s(wsp)

        sppw    = 0.0
        recover = 0.0

        if borstat == "W":
            sppw     = spp2
            sp       = 0.0
            marketvl = 0.0
        else:
            recover = spp2 - sp
        if recover < 0:
            recover = 0.0

        # BRANCH = PUT(NTBRCH,BRCHCD.) || ' ' || PUT(NTBRCH,Z3.)
        branch = _branch_label(ntbrch)
        loantyp = format_lntyp(loantype)

        # IF WRITEOFF='Y' THEN DO; ...
        if writeoff == "Y":
            sppl     = _s(r.get("WSPPL"))
            otherfee = 0.0
            if wdownind != "Y":
                recover = _s(r.get("WRECOVER"))
                sp      = 0.0
                sppw    = _sum(spp2, sppl, -recover)
            else:
                sppw = _s(r.get("WSPPW"))
                if netexp <= 0:
                    recover = 0.0
                sp = _sum(spp2, sppl, -recover, -sppw)
                if netexp <= 0 and sp > 0:
                    recover = sp
                    sp = 0.0

        # IF RESCHEIND='Y' THEN DO;
        #   SPLL    = WSPLL;   ← note: SPLL (local temp), not SPPL
        #   RECOVER = WRECOVER;
        #   SPPW    = WSPPW;
        #   SP      = SUM(SPP2,SPPL,(-1)*RECOVER,(-1)*SPPW);
        # END;
        if rescheind == "Y":
            # SPLL = WSPLL  (local variable used inside the block only)
            _spll   = _s(r.get("WSPLL"))   # noqa: F841 — SPLL is local to this block
            recover = _s(r.get("WRECOVER"))
            sppw    = _s(r.get("WSPPW"))
            sp      = _sum(spp2, sppl, -recover, -sppw)

        results.append({
            "BRANCH":    branch,
            "NTBRCH":    ntbrch,
            "ACCTNO":    r.get("ACCTNO"),
            "NOTENO":    r.get("NOTENO"),
            "NAME":      r.get("NAME"),
            "DAYS":      days,
            "BORSTAT":   borstat,
            "NETPROC":   r.get("NETPROC"),
            "CURBAL":    curbal,
            "UHC":       uhc,
            "NETBAL":    netbal,
            "IIS":       iis,
            "OSPRIN":    osprin,
            "MARKETVL":  marketvl,
            "NETEXP":    netexp,
            "SPP2":      spp2,
            "SPPL":      sppl,
            "RECOVER":   recover,
            "SPPW":      sppw,
            "SP":        sp,
            "LOANTYP":   loantyp,
            "LOANTYPE":  loantype,
            "VINNO":     r.get("VINNO"),
            "CENSUS7":   census7,
            "OTHERFEE":  otherfee,
            "EXIST":     "Y",
            "COSTCTR":   r.get("COSTCTR"),
            "USER5":     user5,
            "PENDBRH":   r.get("PENDBRH"),
            "WDOWNIND":  wdownind,
            "RESCHEIND": rescheind,
            "PAIDIND":   r.get("PAIDIND"),
        })

    return results


# ===========================================================================
# DATA LOAN2  — SP for CURRENT NPL accounts  (EXIST ^= 'Y')
# ===========================================================================
def process_loan2(df_loanwoff: pl.DataFrame,
                  reptdate: date) -> list:
    """
    DATA LOAN2;
      KEEP BRANCH NTBRCH ACCTNO NOTENO NAME DAYS BORSTAT NETPROC CURBAL
           UHC NETBAL IIS OSPRIN MARKETVL NETEXP SPP2 SPPL RECOVER
           SPPW SP LOANTYP VINNO CENSUS7 OTHERFEE EXIST COSTCTR USER5
           PENDBRH WDOWNIND RESCHEIND;
      SET LOANWOFF;
      IF EXIST ^= 'Y';
      ...
    """
    results = []

    for r in df_loanwoff.filter(pl.col("EXIST") != "Y").iter_rows(named=True):
        writeoff  = r.get("WRITEOFF", "N") or "N"
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
        iis       = _s(r.get("IIS"))
        spp2      = _s(r.get("SPP2"))
        appvalue  = _s(r.get("APPVALUE"))
        census7   = str(r.get("CENSUS7") or "")
        hardcode  = r.get("HARDCODE") or "N"
        wrealvl   = _s(r.get("WREALVL"))
        ntbrch    = _si(r.get("NTBRCH"))
        rescheind = r.get("RESCHEIND") or ""

        if writeoff == "Y" and wdownind != "Y":
            borstat = "W"

        # UHC — LOAN2 uses BLDATE>0 & TERMCHG>0 (no extra DAYS/BORSTAT gate)
        # ELSE DO block calculates REMMTH2 without the REMMTH1 side-tracks
        uhc = 0.0
        if (bldate is not None and bldate > date(1960, 1, 1) and termchg > 0):
            uhc = _calc_uhc(loantype, bldate, issdte, reptdate, termchg, earnterm)
        else:
            # ELSE DO: straight REMMTH2 only
            if issdte is not None:
                remmth2 = earnterm - (
                    (reptdate.year - issdte.year) * 12
                    + reptdate.month - issdte.month + 1)
                if remmth2 < 0:
                    remmth2 = 0
                if remmth2 > 0 and earnterm > 0:
                    uhc = remmth2 * (remmth2 + 1) * termchg / (earnterm * (earnterm + 1))

        netbal = curbal - uhc
        osprin = curbal - uhc - iis

        # OTHERFEE
        if loantype in (380, 381):
            otherfee = _sum(r.get("FEEAMT"), -_s(r.get("FEETOT2")))
        else:
            otherfee = _sum(r.get("FEEAMT8"),
                            -_s(r.get("FEETOT2")),
                            r.get("FEEAMTA"),
                            -_s(r.get("FEEAMT5")))
        if otherfee < 0:
            otherfee = 0.0
        if loantype in (983, 993):
            otherfee = 0.0

        sp, marketvl, netexp = _calc_sp(
            days, borstat, user5, loantype, appvalue, census7,
            hardcode, wrealvl, osprin, otherfee, reptdate, issdte)

        # SPPL = SP  (no SPP2 subtraction for current NPL — it's a new provision)
        sppl = sp

        # IF HARDCODE='Y' THEN DO;
        #   IF WSPPL NE . THEN SPPL=WSPPL;
        #   IF WSP   NE . THEN SP=WSP;
        # END;
        if hardcode == "Y":
            wsppl = r.get("WSPPL")
            if wsppl is not None:
                sppl = _s(wsppl)
            wsp = r.get("WSP")
            if wsp is not None:
                sp = _s(wsp)

        # BRANCH, LOANTYP
        branch  = _branch_label(ntbrch)
        loantyp = format_lntyp(loantype)

        sppw    = 0.0
        recover = 0.0

        # IF WRITEOFF='Y' THEN DO; ...
        if writeoff == "Y":
            sppl     = _s(r.get("WSPPL"))
            otherfee = 0.0
            if wdownind != "Y":
                recover = _s(r.get("WRECOVER"))
                sp      = 0.0
                sppw    = _sum(spp2, sppl, -recover)
            else:
                sppw = _s(r.get("WSPPW"))
                if netexp <= 0:
                    recover = 0.0
                sp = _sum(spp2, sppl, -recover, -sppw)
                if netexp <= 0 and sp > 0:
                    recover = sp
                    sp = 0.0

        # IF RESCHEIND='Y' THEN DO;
        #   SPLL    = WSPLL;
        #   RECOVER = WRECOVER;
        #   SPPW    = WSPPW;
        #   SP      = SUM(SPP2,SPPL,(-1)*RECOVER,(-1)*SPPW);
        # END;
        if rescheind == "Y":
            _spll   = _s(r.get("WSPLL"))   # noqa: F841 — SPLL local to this block
            recover = _s(r.get("WRECOVER"))
            sppw    = _s(r.get("WSPPW"))
            sp      = _sum(spp2, sppl, -recover, -sppw)

        results.append({
            "BRANCH":    branch,
            "NTBRCH":    ntbrch,
            "ACCTNO":    r.get("ACCTNO"),
            "NOTENO":    r.get("NOTENO"),
            "NAME":      r.get("NAME"),
            "DAYS":      days,
            "BORSTAT":   borstat,
            "NETPROC":   r.get("NETPROC"),
            "CURBAL":    curbal,
            "UHC":       uhc,
            "NETBAL":    netbal,
            "IIS":       iis,
            "OSPRIN":    osprin,
            "MARKETVL":  marketvl,
            "NETEXP":    netexp,
            "SPP2":      spp2,
            "SPPL":      sppl,
            "RECOVER":   recover,
            "SPPW":      sppw,
            "SP":        sp,
            "LOANTYP":   loantyp,
            "LOANTYPE":  loantype,
            "VINNO":     r.get("VINNO"),
            "CENSUS7":   census7,
            "OTHERFEE":  otherfee,
            "EXIST":     r.get("EXIST"),
            "COSTCTR":   r.get("COSTCTR"),
            "USER5":     user5,
            "PENDBRH":   r.get("PENDBRH"),
            "WDOWNIND":  wdownind,
            "RESCHEIND": rescheind,
            "PAIDIND":   r.get("PAIDIND"),
        })

    return results


# ===========================================================================
# %MACRO MONTHLY — prior-month SP merge
# ===========================================================================

def _settled_row(row: dict, pspp2: float, psppl: float) -> dict:
    """
    A/C settled block — used in both LOAN1 and LOAN2 settled paths:
      SPPL=PSPPL; RECOVER=SUM(PSPP2,PSPPL);
      CURBAL=0; NETBAL=0; UHC=0; IIS=0; MARKETVL=0;
      OSPRIN=SUM(CURBAL,(-1)*UHC,(-1)*IIS);
      NETEXP=SUM(OSPRIN,(-1)*MARKETVL);
      DAYS=0;
      SP=SUM(SPP2,SPPL,(-1)*RECOVER,(-1)*SPPW);
    """
    row = dict(row)
    row["SPPL"]    = psppl
    row["RECOVER"] = _sum(pspp2, psppl)
    row["CURBAL"]  = 0.0
    row["NETBAL"]  = 0.0
    row["UHC"]     = 0.0
    row["IIS"]     = 0.0
    row["MARKETVL"]= 0.0
    row["OSPRIN"]  = 0.0   # SUM(0,-0,-0)
    row["NETEXP"]  = 0.0   # SUM(0,-0)
    row["DAYS"]    = 0
    row["SP"]      = _sum(row.get("SPP2"), row.get("SPPL"),
                          -row["RECOVER"], -_s(row.get("SPPW")))
    return row


def _apply_user5_sppl_adj(row: dict, pspp2: float, psppl: float,
                           psp: float) -> dict:
    """
    USER5='N' SPPL/RECOVER adjustment block that appears in multiple
    branches of the MONTHLY macro for both LOAN1 and LOAN2:
      IF USER5='N' AND SPP2>=SP THEN DO; SPPL=0; RECOVER=SPP2-SP; END;
      IF USER5='N' AND SPP2<SP  THEN DO; SPPL=SP-SPP2; RECOVER=0; END;
    """
    row  = dict(row)
    user5 = row.get("USER5", "")
    sp    = _s(row.get("SP"))
    spp2  = _s(row.get("SPP2"))
    if user5 == "N":
        if spp2 >= sp:
            row["SPPL"]    = 0.0
            row["RECOVER"] = spp2 - sp
        else:
            row["SPPL"]    = sp - spp2
            row["RECOVER"] = 0.0
    return row


def _merge_monthly_loan1(loan1_rows: list, sp2prev_rows: list) -> list:
    """
    Existing NPL (LOAN1) prior-month merge — DATA LOAN1 inside %MACRO MONTHLY.

    PROC SORT DATA=NPL.SP2&PREVMON
      (RENAME=(DAYS=PDAYS SPP2=PSPP2 SPPL=PSPPL SP=PSP
               RECOVER=PRECOVER BORSTAT=PBORSTAT))
      OUT=SP2PREV NODUPKEY; BY ACCTNO NOTENO;

    DATA SP2PREV;
      SET SP2PREV;
      IF LOANTYPE IN (128,...) AND PAIDIND='P' THEN %INC PGM(NPLNTB);
      BRANCH = PUT(NTBRCH,BRCHCD.) || ' ' || PUT(NTBRCH,Z3.);
      null-guards on PDAYS/PSPP2/PSPPL/PSP/PRECOVER;

    DATA LOAN1(MERGE SP2PREV LOAN1); BY ACCTNO;
      IF ((A AND B) OR (B AND NOT A)) AND EXIST='Y';
      ...all transition branches...
    """
    hp_leasing = {128, 130, 131, 132, 380, 381, 390, 700, 705, 720, 725,
                  983, 993, 996}

    # Build SP2PREV index — NODUPKEY on ACCTNO NOTENO
    seen: set = set()
    prev_by_acctno: dict = {}
    for p in sp2prev_rows:
        key = (p.get("ACCTNO"), p.get("NOTENO"))
        if key in seen:
            continue
        seen.add(key)
        ltype = _si(p.get("LOANTYPE"))
        if ltype in hp_leasing and p.get("PAIDIND") == "P":
            pb, nb, cc = apply_nplntb(
                _si(p.get("PENDBRH")), _si(p.get("NTBRCH")), _si(p.get("COSTCTR")))
            p["PENDBRH"] = pb; p["NTBRCH"] = nb; p["COSTCTR"] = cc
        p["BRANCH"]   = _branch_label(_si(p.get("NTBRCH")))
        p["PDAYS"]    = _si(p.get("PDAYS"))    if p.get("PDAYS")    is not None else 0
        p["PSPP2"]    = _s(p.get("PSPP2"))     if p.get("PSPP2")    is not None else 0.0
        p["PSPPL"]    = _s(p.get("PSPPL"))     if p.get("PSPPL")    is not None else 0.0
        p["PSP"]      = _s(p.get("PSP"))        if p.get("PSP")      is not None else 0.0
        p["PRECOVER"] = _s(p.get("PRECOVER"))   if p.get("PRECOVER") is not None else 0.0
        prev_by_acctno[p["ACCTNO"]] = p

    loan1_by_acctno = {row["ACCTNO"]: row for row in loan1_rows}
    all_accts = set(prev_by_acctno) | set(loan1_by_acctno)

    results = []
    for acctno in all_accts:
        a_row = loan1_by_acctno.get(acctno)
        b_row = prev_by_acctno.get(acctno)
        in_a  = a_row is not None
        in_b  = b_row is not None

        # IF ((A AND B) OR (B AND NOT A)) AND EXIST='Y'
        exist = (a_row or b_row or {}).get("EXIST", "")
        if not (((in_a and in_b) or (in_b and not in_a)) and exist == "Y"):
            continue

        # Merge: start from loan1 row, fill missing fields from prev
        row = dict(a_row or b_row)
        if a_row and b_row:
            for k, v in b_row.items():
                if k not in row:
                    row[k] = v

        pdays    = _si((b_row or {}).get("PDAYS"))
        pspp2    = _s((b_row or {}).get("PSPP2"))
        psppl    = _s((b_row or {}).get("PSPPL"))
        psp      = _s((b_row or {}).get("PSP"))
        precover = _s((b_row or {}).get("PRECOVER"))

        borstat   = row.get("BORSTAT", "") or ""
        rescheind = row.get("RESCHEIND", "") or ""
        days      = _si(row.get("DAYS"))
        curbal    = _s(row.get("CURBAL"))
        user5     = row.get("USER5", "") or ""

        # --- A/C SETTLE FOR EXISTING NPL ---
        # IF ((B AND NOT A) OR (CURBAL LE 0 AND PSP LE 0)) AND
        #    BORSTAT NOT IN ('F','I','R','W','S')
        if ((in_b and not in_a) or (curbal <= 0 and psp <= 0)) and \
                borstat not in ("F", "I", "R", "W", "S"):
            row = _settled_row(row, pspp2, psppl)
            results.append(row)
            continue

        # ELSE DO — handle status-based outputs
        if borstat == "W" or rescheind == "Y":
            results.append(row)
            continue

        if in_a and in_b:
            sp   = _s(row.get("SP"))
            spp2 = _s(row.get("SPP2"))

            # --- CONTINUE PERFORMING (days<90 and pdays<90) ---
            if days < 90 and pdays < 90:
                if borstat not in ("F", "I", "R"):
                    row["SPPL"]    = psppl
                    row["RECOVER"] = _sum(pspp2, psppl)
                row = _apply_user5_sppl_adj(row, pspp2, psppl, psp)
                results.append(row)
                continue

            # --- TURN PERFORMING (days<90, pdays>=90) ---
            if days < 90 and pdays >= 90:
                if borstat not in ("F", "I", "R"):
                    row["IIS"]     = 0.0
                    if user5 != "N":
                        row["MARKETVL"] = 0.0
                    row["OSPRIN"]  = _sum(curbal, -_s(row.get("UHC")),
                                         -row["IIS"])
                    row["NETEXP"]  = _sum(row["OSPRIN"], -row["MARKETVL"])
                    row["SPPL"]    = psppl
                    row["RECOVER"] = _sum(pspp2, psppl)
                row = _apply_user5_sppl_adj(row, pspp2, psppl, psp)
                results.append(row)
                continue

            # --- TURN NPL FROM PERFORMING (days>=90, pdays<90) ---
            if days >= 90 and pdays < 90:
                if borstat not in ("F", "I", "R"):
                    row["SPPL"]    = _sum(sp, psppl)
                    row["RECOVER"] = _sum(psppl, pspp2)
                row = _apply_user5_sppl_adj(row, pspp2, psppl, psp)
                results.append(row)
                continue

            # --- CONTINUE NPL (days>=90, pdays>=90) ---
            if days >= 90 and pdays >= 90:
                if borstat not in ("F", "I", "R"):
                    sppl_new = _sum(sp, -pspp2)
                    if sppl_new < 0:
                        recover_new = -sppl_new
                        sppl_new    = 0.0
                    else:
                        recover_new = 0.0
                    row["SPPL"]    = sppl_new
                    row["RECOVER"] = recover_new
                results.append(row)
                continue

        results.append(row)
    return results


def _merge_monthly_loan2(loan2_rows: list, sp2prev_rows: list,
                          df_ploan: pl.DataFrame) -> list:
    """
    Current NPL (LOAN2) prior-month merge — DATA LOAN2 inside %MACRO MONTHLY.

    PROC SORT DATA=NPL.PLOAN&REPTMON OUT=PLOAN
      (KEEP=ACCTNO NOTENO CURBAL DAYS BORSTAT NTBRCH COSTCTR); BY ACCTNO;

    DATA SP2PREV;
      MERGE SP2PREV(IN=A) PLOAN(IN=B); BY ACCTNO;
      IF PSPP2 EQ 0 AND EXIST NE 'Y';
      BRANCH = PUT(NTBRCH,BRCHCD.) || ' ' || PUT(NTBRCH,Z3.);

    DATA LOAN2(MERGE SP2PREV LOAN2); BY ACCTNO;
      ...all transition branches...
    """
    # Filter SP2PREV: PSPP2=0 AND EXIST NE 'Y', merged with PLOAN
    ploan_accts = set(df_ploan["ACCTNO"].to_list())

    seen: set = set()
    prev_filtered: dict = {}
    for p in sp2prev_rows:
        key = (p.get("ACCTNO"), p.get("NOTENO"))
        if key in seen:
            continue
        seen.add(key)
        if _s(p.get("PSPP2")) == 0 and p.get("EXIST", "") != "Y":
            p["BRANCH"] = _branch_label(_si(p.get("NTBRCH")))
            prev_filtered[p["ACCTNO"]] = p

    loan2_by_acctno = {row["ACCTNO"]: row for row in loan2_rows}
    all_accts = set(prev_filtered) | set(loan2_by_acctno)

    results = []
    for acctno in all_accts:
        a_row = loan2_by_acctno.get(acctno)
        b_row = prev_filtered.get(acctno)
        in_a  = a_row is not None
        in_b  = b_row is not None

        row = dict(a_row or b_row)
        if a_row and b_row:
            for k, v in b_row.items():
                if k not in row:
                    row[k] = v

        pdays    = _si((b_row or {}).get("PDAYS"))
        pspp2    = _s((b_row or {}).get("PSPP2"))
        psppl    = _s((b_row or {}).get("PSPPL"))
        precover = _s((b_row or {}).get("PRECOVER"))

        borstat   = row.get("BORSTAT", "") or ""
        rescheind = row.get("RESCHEIND", "") or ""
        days      = _si(row.get("DAYS"))
        user5     = row.get("USER5", "") or ""
        curbal    = _s(row.get("CURBAL"))

        # IF (B AND NOT A): A/C settled
        if in_b and not in_a:
            row = _settled_row(row, pspp2, psppl)
            results.append(row)
            continue

        # NEW NPL FOR THE MTH: (A AND NOT B) AND
        # (DAYS>=90 OR BORSTAT IN ('F','I','R','W') OR USER5='N')
        if in_a and not in_b:
            if (days >= 90 or borstat in ("F", "I", "R", "W") or user5 == "N"):
                results.append(row)
            continue

        if borstat == "W" or rescheind == "Y":
            results.append(row)
            continue

        if in_a and in_b:
            sp   = _s(row.get("SP"))
            spp2 = _s(row.get("SPP2"))

            # --- CONTINUE PERFORMING (days<90, pdays<90) ---
            if days < 90 and pdays < 90:
                if borstat not in ("F", "I", "R"):
                    row["SPPL"]    = psppl
                    row["RECOVER"] = precover
                if borstat in ("F", "I", "R"):
                    row["RECOVER"] = precover
                    row["SPPL"]    = _sum(sp, row["RECOVER"])
                row = _apply_user5_sppl_adj(row, pspp2, psppl, _s(row.get("SP")))
                results.append(row)
                continue

            # --- TURN PERFORMING FROM NPL (days<90, pdays>=90) ---
            if days < 90 and pdays >= 90:
                if borstat not in ("F", "I", "R"):
                    row["IIS"]     = 0.0
                    row["MARKETVL"]= 0.0
                    row["OSPRIN"]  = _sum(curbal, -_s(row.get("UHC")), -row["IIS"])
                    row["NETEXP"]  = _sum(row["OSPRIN"], -row["MARKETVL"])
                    row["SPPL"]    = psppl
                    row["RECOVER"] = psppl
                row = _apply_user5_sppl_adj(row, pspp2, psppl, _s(row.get("SP")))
                results.append(row)
                continue

            # --- TURN NPL FROM PERFORMING (days>=90, pdays<90) ---
            if days >= 90 and pdays < 90:
                if borstat not in ("F", "I", "R"):
                    row["SPPL"]    = _sum(sp, psppl)
                    row["RECOVER"] = psppl
                row = _apply_user5_sppl_adj(row, pspp2, psppl, _s(row.get("SP")))
                results.append(row)
                continue

            # --- CONTINUE NPL (days>=90, pdays>=90) ---
            if days >= 90 and pdays >= 90:
                row["RECOVER"] = precover
                row["SPPL"]    = _sum(sp, row["RECOVER"])
                results.append(row)
                continue

        results.append(row)
    return results


# ===========================================================================
# %MACRO MONTHLY — top-level dispatcher
# ===========================================================================
def run_monthly(loan1_rows: list, loan2_rows: list,
                reptmon: str, sp2_prev_path: Path,
                ploan_path: Path) -> tuple:
    """
    %MACRO MONTHLY;
      %IF "&REPTMON" EQ "01" %THEN %DO;
        DATA LOAN1; SET LOAN1; SPPLCUM=0; RUN;
        DATA LOAN2; SET LOAN2; SPPLCUM=0; RUN;
      %END;
      %ELSE %DO;
        PROC SORT DATA=NPL.SP2&PREVMON (...RENAME...) OUT=SP2PREV NODUPKEY;
        ...full merge logic...
      %END;
    %MEND MONTHLY;
    """
    if reptmon == "01":
        # January: no prior-month data — zero out SPPLCUM
        for row in loan1_rows:
            row["SPPLCUM"] = 0.0
        for row in loan2_rows:
            row["SPPLCUM"] = 0.0
        return loan1_rows, loan2_rows

    # Read NPL.SP2&PREVMON with rename
    # RENAME=(DAYS=PDAYS SPP2=PSPP2 SPPL=PSPPL SP=PSP
    #         RECOVER=PRECOVER BORSTAT=PBORSTAT)
    rename_map = {
        "DAYS":    "PDAYS",
        "SPP2":    "PSPP2",
        "SPPL":    "PSPPL",
        "SP":      "PSP",
        "RECOVER": "PRECOVER",
        "BORSTAT": "PBORSTAT",
    }
    df_prev = pl.read_parquet(sp2_prev_path)
    existing = {c for c in rename_map if c in df_prev.columns}
    df_prev  = df_prev.rename({k: v for k, v in rename_map.items() if k in existing})
    sp2prev_rows = df_prev.to_dicts()

    # Read PLOAN for LOAN2 filter
    df_ploan = pl.read_parquet(ploan_path).select(
        ["ACCTNO", "NOTENO", "CURBAL", "DAYS", "BORSTAT", "NTBRCH", "COSTCTR"])

    loan1_out = _merge_monthly_loan1(loan1_rows, sp2prev_rows)
    loan2_out = _merge_monthly_loan2(loan2_rows, sp2prev_rows, df_ploan)
    return loan1_out, loan2_out


# ===========================================================================
# Report writers
# ===========================================================================

def _page_header(f, page: int, rdate: str, tbl_label: str):
    """ASA form-feed header ('1' = new page)."""
    f.write(f"1{'PUBLIC BANK - (NPL FROM 3 MONTHS & ABOVE) - NEW':^132}\n")
    f.write(f" MOVEMENTS OF SPECIFIC PROVISION FOR THE MONTH ENDING"
            f" {rdate} {tbl_label}\n")
    f.write(" \n")


def write_summary_report(df: pl.DataFrame, output_path: Path, rdate: str):
    """
    PROC TABULATE equivalent.
    Tables:
      1. LOANTYP x (RISK x (BRANCH + SUB-TOTAL)) + TOTAL
      2. LOANTYP x (BRANCH + TOTAL)
    ASA carriage control throughout.
    """
    SUM_VARS = ["CURBAL", "UHC", "NETBAL", "IIS", "OSPRIN", "OTHERFEE",
                "MARKETVL", "NETEXP", "SPP2", "SPPL", "RECOVER", "SPPW", "SP"]
    COL_W = 14

    def fmt(v):
        return f"{_s(v):>{COL_W},.2f}"

    def agg_group(df_in, by_cols):
        return (df_in.group_by(by_cols)
                     .agg([pl.len().alias("N")]
                          + [pl.sum(c).alias(c) for c in SUM_VARS])
                     .sort(by_cols))

    summary_risk   = agg_group(df, ["LOANTYP", "RISK", "BRANCH"])
    summary_branch = agg_group(df, ["LOANTYP", "BRANCH"])

    col_header = (f"{'RISK/BRANCH':<29}{'N':>10}"
                  + "".join(f"{c:>{COL_W}}" for c in SUM_VARS))

    with open(output_path, "w") as f:
        # ---- Table 1: RISK x BRANCH ----
        page = 1
        _page_header(f, page, rdate, "(EXISTING AND CURRENT)")
        f.write(f" {col_header}\n")
        f.write(f" {'-' * len(col_header)}\n")
        lines = 4
        cur_ltyp = None

        for row in summary_risk.iter_rows(named=True):
            if lines >= PAGE_LINES - 2:
                page += 1
                _page_header(f, page, rdate, "(EXISTING AND CURRENT)")
                f.write(f" {col_header}\n")
                f.write(f" {'-' * len(col_header)}\n")
                lines = 4

            if cur_ltyp != row["LOANTYP"]:
                if cur_ltyp is not None:
                    f.write(" \n"); lines += 1
                cur_ltyp = row["LOANTYP"]
                f.write(f" {cur_ltyp}\n"); lines += 1

            rb   = f"  {row['RISK']:<15}{row['BRANCH']:<12}"
            line = (f"{rb:<29}{row['N']:>10,}"
                    + "".join(fmt(row[c]) for c in SUM_VARS))
            f.write(f" {line}\n"); lines += 1

        # ---- Table 2: BRANCH only ----
        f.write("1")   # ASA form-feed
        _page_header(f, page + 1, rdate, "(EXISTING AND CURRENT)")
        col_hdr2 = (f"{'BRANCH':<9}{'N':>10}"
                    + "".join(f"{c:>{COL_W}}" for c in SUM_VARS))
        f.write(f" {col_hdr2}\n")
        f.write(f" {'-' * len(col_hdr2)}\n")
        cur_ltyp = None

        for row in summary_branch.iter_rows(named=True):
            if cur_ltyp != row["LOANTYP"]:
                cur_ltyp = row["LOANTYP"]
                f.write(f" {cur_ltyp}\n")
            line = (f"{row['BRANCH']:<9}{row['N']:>10,}"
                    + "".join(fmt(row[c]) for c in SUM_VARS))
            f.write(f" {line}\n")


def write_detail_report(df: pl.DataFrame, output_path: Path, rdate: str):
    """
    PROC PRINT equivalent.
    SORT: BY LOANTYP BRANCH RISK DAYS ACCTNO
    PAGEBY BRANCH; SUMBY RISK;
    VAR: ACCTNO NAME VINNO DAYS BORSTAT NETPROC CURBAL UHC NETBAL OTHERFEE
         IIS OSPRIN MARKETVL NETEXP SPP2 SPPL RECOVER SPPW SP
    ASA carriage control.
    """
    LABEL = {
        "ACCTNO":   "MNI ACCOUNT NO",
        "VINNO":    "AA NUMBER",
        "DAYS":     "NO OF DAYS PAST DUE",
        "BORSTAT":  "BORROWER'S STATUS",
        "NETPROC":  "LIMIT",
        "CURBAL":   "CURRENT BAL (A)",
        "UHC":      "UNEARNED HIRING CHARGES (B)",
        "NETBAL":   "NET BAL (A-B=C)",
        "IIS":      "IIS (E)",
        "OSPRIN":   "PRINCIPAL OUTSTANDING (C-E=F)",
        "OTHERFEE": "OTHER FEES",
        "MARKETVL": "REALISABLE VALUE (G)",
        "NETEXP":   "NET EXPOSURE (F-G=H)",
        "SPP2":     "OPENING BAL FOR FINANCIAL YEAR (I)",
        "SPPL":     "PROVISION MADE AGAINST PROFIT & LOSS (J)",
        "RECOVER":  "WRITTEN BACK TO PROFIT & LOSS (K)",
        "SPPW":     "WRITTEN OFF AGAINST PROVISION (L)",
        "SP":       "CLOSING BAL AS AT RPT DATE (I+J-K-L)",
    }
    PRINT_VARS  = ["ACCTNO", "NAME", "VINNO", "DAYS", "BORSTAT", "NETPROC",
                   "CURBAL", "UHC", "NETBAL", "OTHERFEE",
                   "IIS", "OSPRIN", "MARKETVL", "NETEXP",
                   "SPP2", "SPPL", "RECOVER", "SPPW", "SP"]
    SUM_VARS    = ["NETPROC", "CURBAL", "UHC", "NETBAL", "IIS", "OSPRIN",
                   "MARKETVL", "NETEXP", "SPP2", "SPPL", "RECOVER", "SPPW",
                   "SP", "OTHERFEE"]
    NUMERIC     = {"NETPROC", "CURBAL", "UHC", "NETBAL", "IIS", "OSPRIN",
                   "MARKETVL", "NETEXP", "SPP2", "SPPL", "RECOVER", "SPPW",
                   "SP", "OTHERFEE"}
    COL_W = 16

    df_sorted = df.sort(["LOANTYP", "BRANCH", "RISK", "DAYS", "ACCTNO"])

    def fmt_num(v):
        return f"{_s(v):>{COL_W},.2f}"

    def fmt_val(col, v):
        if col in NUMERIC:
            return fmt_num(v)
        if col == "DAYS":
            return f"{_si(v):>6}"
        return f"{str(v or ''):<16}"

    HDR_LINE = "  ".join(f"{LABEL.get(c, c):<{COL_W}}" for c in PRINT_VARS)

    def write_page_header(f, page, rdate):
        f.write(f"1{'PUBLIC BANK - (NPL FROM 3 MONTHS & ABOVE) - NEW':^180}\n")
        f.write(f" MOVEMENTS OF SPECIFIC PROVISION FOR THE MONTH ENDING"
                f" {rdate} (EXISTING AND CURRENT)\n")
        f.write(f" {HDR_LINE}\n")
        f.write(f" {'-' * len(HDR_LINE)}\n")

    risk_sums = {c: 0.0 for c in SUM_VARS}

    def write_risk_subtotal(f, risk_label):
        line = f"{'*** ' + risk_label + ' SUBTOTAL ***':<40}"
        line += "  ".join(fmt_num(risk_sums[c]) for c in SUM_VARS)
        f.write(f" {line}\n")
        for c in risk_sums:
            risk_sums[c] = 0.0

    with open(output_path, "w") as f:
        page = 1
        lines = PAGE_LINES + 1
        cur_ltyp = cur_branch = cur_risk = None

        for row in df_sorted.iter_rows(named=True):
            if lines >= PAGE_LINES:
                write_page_header(f, page, rdate)
                lines = 4; page += 1

            if cur_ltyp != row["LOANTYP"]:
                if cur_risk is not None:
                    write_risk_subtotal(f, cur_risk)
                cur_ltyp = row["LOANTYP"]
                cur_branch = cur_risk = None
                f.write(f" {cur_ltyp}\n"); lines += 1

            if cur_branch != row["BRANCH"]:
                if cur_risk is not None:
                    write_risk_subtotal(f, cur_risk)
                # PAGEBY BRANCH — new page on branch change
                write_page_header(f, page, rdate)
                lines = 4; page += 1
                cur_branch = row["BRANCH"]
                cur_risk   = None
                f.write(f" BRANCH: {cur_branch}\n"); lines += 1

            if cur_risk != row["RISK"]:
                if cur_risk is not None:
                    write_risk_subtotal(f, cur_risk)
                cur_risk = row["RISK"]
                f.write(f"   RISK: {cur_risk}\n"); lines += 1

            for c in SUM_VARS:
                risk_sums[c] += _s(row.get(c))

            line = "  ".join(fmt_val(c, row.get(c)) for c in PRINT_VARS)
            f.write(f" {line}\n"); lines += 1

        if cur_risk is not None:
            write_risk_subtotal(f, cur_risk)


# ===========================================================================
# main()
# ===========================================================================
def main():
    global INPUT_NPL_LOAN, INPUT_NPL_PLOAN, INPUT_NPL_IIS
    global INPUT_NPL_SP2_PREV, OUTPUT_NPL_SP2_MON

    print("EIFMNP06 — NPL Specific Provision Processing")
    print("=" * 70)

    # -----------------------------------------------------------------------
    # DATA REPTDATE
    # -----------------------------------------------------------------------
    reptdate, rdate, reptmon, prevmon, styr, stmth = read_reptdate(INPUT_NPL_REPTDATE)
    print(f"Reporting Date : {rdate}   reptmon={reptmon}  prevmon={prevmon}")

    INPUT_NPL_LOAN     = NPL_BASE / f"NPL_LOAN{reptmon}.parquet"
    INPUT_NPL_PLOAN    = NPL_BASE / f"NPL_PLOAN{reptmon}.parquet"
    INPUT_NPL_IIS      = NPL_BASE / f"NPL_IIS{reptmon}.parquet"
    INPUT_NPL_SP2_PREV = NPL_BASE / f"NPL_SP2{prevmon}.parquet"
    OUTPUT_NPL_SP2_MON = NPL_BASE / f"NPL_SP2{reptmon}.parquet"

    # -----------------------------------------------------------------------
    # PROC SORT + DATA LOANWOFF  (two-pass merge per SAS)
    # -----------------------------------------------------------------------
    print("Reading input files...")
    df_loan = pl.read_parquet(INPUT_NPL_LOAN).sort("ACCTNO")
    df_wsp2 = pl.read_parquet(INPUT_NPL_WSP2).sort("ACCTNO")
    df_iis  = (pl.read_parquet(INPUT_NPL_IIS)
                 .select(["ACCTNO", "IIS"])
                 .sort("ACCTNO"))

    print(f"  LOAN rows : {len(df_loan):,}")
    print(f"  WSP2 rows : {len(df_wsp2):,}")
    print(f"  IIS  rows : {len(df_iis):,}")

    df_loanwoff = build_loanwoff(df_loan, df_wsp2, df_iis)
    print(f"  LOANWOFF  : {len(df_loanwoff):,}")

    # -----------------------------------------------------------------------
    # DATA LOAN1 / LOAN2  (base SP calculations)
    # -----------------------------------------------------------------------
    print("Calculating SP — existing NPL (LOAN1)...")
    loan1_rows = process_loan1(df_loanwoff, reptdate)

    print("Calculating SP — current NPL (LOAN2)...")
    loan2_rows = process_loan2(df_loanwoff, reptdate)

    print(f"  LOAN1 base: {len(loan1_rows):,}   LOAN2 base: {len(loan2_rows):,}")

    # -----------------------------------------------------------------------
    # %MACRO MONTHLY
    # -----------------------------------------------------------------------
    print("Running MONTHLY macro (prior-month SP merge)...")
    loan1_rows, loan2_rows = run_monthly(
        loan1_rows, loan2_rows, reptmon,
        INPUT_NPL_SP2_PREV, INPUT_NPL_PLOAN)

    # -----------------------------------------------------------------------
    # DATA LOAN3 = SET LOAN1 LOAN2
    # + RISK classification
    # + WHERE filter
    # + PROC SORT NODUPKEY BY ACCTNO NOTENO  (applied to all three outputs)
    # -----------------------------------------------------------------------
    print("Building LOAN3...")
    df_loan3 = pl.DataFrame(loan1_rows + loan2_rows)

    df_loan3 = df_loan3.with_columns(
        pl.struct(["DAYS", "BORSTAT", "USER5"])
        .map_elements(
            lambda x: _risk(_si(x["DAYS"]),
                            x["BORSTAT"] or "",
                            x["USER5"] or ""),
            return_dtype=pl.Utf8)
        .alias("RISK")
    )

    # WHERE (COSTCTR<3000 OR COSTCTR>3999) AND
    #       COSTCTR NOT IN (4043,4048) AND COSTCTR NE .
    df_loan3 = df_loan3.filter(
        pl.col("COSTCTR").is_not_null()
        & (~pl.col("COSTCTR").is_in([4043, 4048]))
        & ((pl.col("COSTCTR") < 3000) | (pl.col("COSTCTR") > 3999))
    )

    # PROC SORT NODUPKEY; BY ACCTNO NOTENO;
    df_loan3 = df_loan3.unique(subset=["ACCTNO", "NOTENO"], keep="first")

    print(f"  LOAN3 after filter/dedup: {len(df_loan3):,}")

    # -----------------------------------------------------------------------
    # Outputs: NPL.SP2&REPTMON  and  NPL.SP2
    # (NODUPKEY already applied — same frame written to both)
    # -----------------------------------------------------------------------
    df_loan3.write_parquet(OUTPUT_NPL_SP2_MON)
    df_loan3.write_parquet(OUTPUT_NPL_SP2)
    print(f"  Written: {OUTPUT_NPL_SP2_MON}")
    print(f"  Written: {OUTPUT_NPL_SP2}")

    # -----------------------------------------------------------------------
    # OPTIONS NOCENTER NODATE NONUMBER MISSING=0
    # %TBLS (I=3 only — EXISTING AND CURRENT)
    # %DTLS (I=3 only — EXISTING AND CURRENT)
    # -----------------------------------------------------------------------
    print("Writing summary report (%TBLS)...")
    write_summary_report(df_loan3, OUTPUT_REPORT_SUMMARY, rdate)
    print(f"  Written: {OUTPUT_REPORT_SUMMARY}")

    print("Writing detail report (%DTLS)...")
    write_detail_report(df_loan3, OUTPUT_REPORT_DETAIL, rdate)
    print(f"  Written: {OUTPUT_REPORT_DETAIL}")

    # -----------------------------------------------------------------------
    # Console summary statistics
    # -----------------------------------------------------------------------
    print("\n" + "=" * 70)
    for col in ("CURBAL", "SP", "SPPL", "RECOVER", "SPPW"):
        if col in df_loan3.columns:
            print(f"  Total {col:<10}: {df_loan3[col].sum():>20,.2f}")

    risk_tbl = (df_loan3.group_by("RISK")
                        .agg([pl.len().alias("N"), pl.sum("SP")])
                        .sort("RISK"))
    print("\n  By RISK:")
    for row in risk_tbl.iter_rows(named=True):
        print(f"    {row['RISK']:<20}  {row['N']:>6,} accts  SP {row['SP']:>20,.2f}")
    print("=" * 70)
    print("Processing complete.")


if __name__ == "__main__":
    main()
