#!/usr/bin/env python3
"""
Program  : EIFMNP03.py (JCL: EIIMNP03)
Date     : 12.03.98
Modify   : ESMR 2004-720, 2004-579, 2006-1048
Report   : Movements of Interest in Suspense for the Month Ending
"""

# OPTIONS NOCENTER YEARCUTOFF=1950;

# %INC PGM(PBBLNFMT);
from PBBLNFMT_clau import (
    format_lndenom, format_lnprod, format_lncustcd, format_statecd,
    format_apprlimt, format_loansize, format_mthpass, format_lnormt,
    format_lnrmmt, format_collcd, format_riskcd, format_busind,
    is_more_plan, is_hire_purchase, is_islamic_product, is_fcy_product
)
# The following functions from PBBLNFMT are available but not directly used
# in this program's logic:
# format_oddenom, format_odprod, format_odcustcd, format_locustcd,
# format_odrate, format_lnrate, format_liqpfmt, format_sltype, format_ln03fmt,
# format_lnfmt, format_lnlob, format_odfmt, format_odlob, format_fisstype,
# format_fissgroup, format_sectcd, format_secdes, format_secta, format_sectb,
# format_indsect, format_criscd, format_rvrsect, format_rvrcris, format_rvrse,
# format_fisspur, format_newsect, format_validse, format_statepost

# %INC PGM(PBBELF);
from PBBELF_clau import format_brchcd
# The following functions from PBBELF are available but not directly used
# in this program's logic:
# format_cacbrch, format_cacname, format_regioff, format_regnew,
# format_ctype, format_brchrvr

import duckdb
import polars as pl
import math
from datetime import date
from pathlib import Path

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR       = Path("/data/npl")
INPUT_DIR      = BASE_DIR / "parquet"
OUTPUT_DIR     = BASE_DIR / "output"
REPORT_DIR     = BASE_DIR / "reports"

# Input parquet files
REPTDATE_FILE  = INPUT_DIR / "reptdate.parquet"
LOAN_FILE_TMPL = INPUT_DIR / "loan{reptmon}.parquet"   # e.g. loan03.parquet
WIIS_FILE      = INPUT_DIR / "wiis.parquet"
IIS_PREV_TMPL  = INPUT_DIR / "iis{prevmon}.parquet"    # e.g. iis02.parquet
PLOAN_TMPL     = INPUT_DIR / "ploan{reptmon}.parquet"

# Output files (saved as parquet for downstream use)
IIS_OUT_TMPL   = INPUT_DIR / "iis{reptmon}.parquet"
IIS_LATEST     = INPUT_DIR / "iis.parquet"

# Report output
REPORT_FILE    = REPORT_DIR / "EIFMNP03_report.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# PROC FORMAT - Local format definitions
# =============================================================================

def lntyp_format(loantype: int) -> str:
    """
    PROC FORMAT VALUE LNTYP:
      128,130,983,131,132     = 'HPD AITAB'
      700,705,380,381,993,996 = 'HPD CONVENTIONAL'
      200-299                 = 'HOUSING LOANS'
      OTHER                   = 'OTHERS'
    """
    if loantype in (128, 130, 983, 131, 132):
        return "HPD AITAB"
    elif loantype in (700, 705, 380, 381, 993, 996):
        return "HPD CONVENTIONAL"
    elif 200 <= loantype <= 299:
        return "HOUSING LOANS"
    else:
        return "OTHERS"


# =============================================================================
# BRANCH LABEL HELPER
# Uses format_brchcd from PBBELF — equivalent to PUT(NTBRCH,BRCHCD.)
# =============================================================================

def branch_label(ntbrch: int) -> str:
    """Equivalent to PUT(NTBRCH,BRCHCD.)||' '||PUT(NTBRCH,Z3.)"""
    return f"{format_brchcd(ntbrch)} {ntbrch:03d}"


# =============================================================================
# MACRO DCLVAR / NXTBLDT helpers
# %MACRO DCLVAR — RETAIN D1-D12 array for days-per-month
# %MACRO NXTBLDT — advance BLDATE by one month, capping day to month-end
# =============================================================================

LDAY = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
        7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}


def next_bldate(bldate: date, issdte: date) -> date:
    """
    %MACRO NXTBLDT
    Advance bldate by one month, capping day to month-end.
    """
    dd = issdte.day
    mm = bldate.month + 1
    yy = bldate.year
    if mm > 12:
        mm = 1
        yy += 1
    lday = dict(LDAY)
    if mm == 2:
        lday[2] = 29 if yy % 4 == 0 else 28
    if dd > lday[mm]:
        dd = lday[mm]
    return date(yy, mm, dd)


# =============================================================================
# MACRO OVINT — commented out in original SAS, preserved as comment below
# =============================================================================
#   %MACRO OVINT(I)
#   IF LOANTYPE = 705 THEN DO;
#      IF NOTETERM > 12 THEN TERM = 12; ELSE TERM = NOTETERM;
#      TRATE = NOTETERM*INTRATE;
#      APR = TRATE*(300*TERM+TRATE)/
#            ((NOTETERM*TRATE)+(150*TERM*(NOTETERM+1)))*12/TERM;
#      RATE = (APR+1)/100;
#   END;
#   ELSE RATE = 8/100;
#   BILAMT = BILPAY;
#   BILAMTL = ORGBAL-BILPAY*(NOTETERM-1);
#   OITEMP = 0; BLDTE = BLDATE;
#   DO REMMTH = REMMTH&I TO REMMTH2 BY -1;
#      IF REMMTH = 0 THEN AMT = BILAMTL; ELSE AMT = BILAMT;
#      %NXTBLDT
#      OITEMP + AMT*RATE*(REPTDATE-BLDATE)/365;
#   END;
#   BLDATE = BLDTE;
#   %MEND OVINT;


# =============================================================================
# IIS CALCULATION HELPERS
# =============================================================================

def iis_sum(remmth1: int, remmth2: int, termchg: float, earnterm: int) -> float:
    """
    DO REMMTH = REMMTH1 TO REMMTH2 BY -1;
       IIS + 2*(REMMTH+1)*TERMCHG/(EARNTERM*(EARNTERM+1));
    END;
    """
    if remmth1 < remmth2 or earnterm == 0:
        return 0.0
    total = 0.0
    for rm in range(remmth1, remmth2 - 1, -1):
        total += 2 * (rm + 1) * termchg / (earnterm * (earnterm + 1))
    return total


def uhc_val(remmth2: int, termchg: float, earnterm: int) -> float:
    """UHC = REMMTH2*(REMMTH2+1)*TERMCHG/(EARNTERM*(EARNTERM+1))"""
    if earnterm == 0:
        return 0.0
    return remmth2 * (remmth2 + 1) * termchg / (earnterm * (earnterm + 1))


# =============================================================================
# STEP 1: DATA REPTDATE — read report date and derive macro variables
# =============================================================================

def load_reptdate() -> tuple[pl.DataFrame, dict]:
    """
    DATA REPTDATE;
       SET NPL.REPTDATE;
       IF MONTH(REPTDATE) = 1 THEN MM1 = 12;
       ELSE MM1 = MONTH(REPTDATE)-1;
       CALL SYMPUT('RDATE', PUT(REPTDATE,WORDDATX18.));
       CALL SYMPUT('REPTMON', PUT(MONTH(REPTDATE),Z2.));
       CALL SYMPUT('PREVMON', PUT(MM1,Z2.));
    """
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM '{REPTDATE_FILE}'").pl()
    con.close()
    row = df.row(0, named=True)
    reptdate_val: date = row["REPTDATE"]
    reptmon = reptdate_val.month
    prevmon = 12 if reptmon == 1 else reptmon - 1
    rdate_str = reptdate_val.strftime("%d %B %Y").upper()
    macro_vars = {
        "RDATE":    rdate_str,
        "REPTMON":  f"{reptmon:02d}",
        "PREVMON":  f"{prevmon:02d}",
        "REPTDATE": reptdate_val,
    }
    return df, macro_vars


# =============================================================================
# STEP 2: DATA LOANWOFF — merge LOAN with WIIS (written-off accounts)
# =============================================================================

def build_loanwoff(reptmon: str) -> pl.DataFrame:
    """
    PROC SORT DATA=NPL.LOAN&REPTMON; BY ACCTNO;
    PROC SORT DATA=NPL.WIIS;         BY ACCTNO;
    DATA LOANWOFF;
       MERGE NPL.LOAN&REPTMON NPL.WIIS (IN=AA DROP=NOTENO NTBRCH);
       BY ACCTNO;
       IF LOANTYPE IN (380,381) THEN FEEAMT = FEETOT2;
       IF AA THEN WRITEOFF = 'Y'; ELSE WRITEOFF = 'N';
       IF LOANTYPE IN (983,993) THEN WDOWNIND = 'N';
       IF IISP = . THEN IISP = 0;
       IF OIP  = . THEN OIP  = 0;
       IF EARNTERM IN (0,.) THEN EARNTERM = NOTETERM;
    """
    loan_file = str(LOAN_FILE_TMPL).format(reptmon=reptmon)
    con = duckdb.connect()
    loanwoff = con.execute(f"""
        SELECT l.*,
               CASE WHEN w.ACCTNO IS NOT NULL THEN 'Y' ELSE 'N' END AS WRITEOFF,
               w.* EXCLUDE (ACCTNO, NOTENO, NTBRCH)
        FROM '{loan_file}' l
        LEFT JOIN '{WIIS_FILE}' w ON l.ACCTNO = w.ACCTNO
        ORDER BY l.ACCTNO
    """).pl()
    con.close()

    out_rows = []
    for row in loanwoff.to_dicts():
        if row.get("LOANTYPE") in (380, 381):
            row["FEEAMT"] = row.get("FEETOT2") or 0
        if row.get("LOANTYPE") in (983, 993):
            row["WDOWNIND"] = "N"
        if not row.get("IISP"):
            row["IISP"] = 0
        if not row.get("OIP"):
            row["OIP"] = 0
        earnterm = row.get("EARNTERM")
        noteterm = row.get("NOTETERM")
        if not earnterm or earnterm == 0:
            row["EARNTERM"] = noteterm
        out_rows.append(row)
    return pl.from_dicts(out_rows, infer_schema_length=None)


# =============================================================================
# STEP 3a: DATA LOAN1 — calculate IIS for EXISTING NPL accounts (EXIST='Y')
# =============================================================================

def calc_loan1(loanwoff: pl.DataFrame, reptdate_val: date) -> pl.DataFrame:
    """
    DATA LOAN1;
       KEEP BRANCH NTBRCH ACCTNO NOTENO NAME NETPROC CURBAL BORSTAT DAYS
            IIS UHC NETBAL IISP SUSPEND RECOVER RECC IISPW OIP OISUSP OI
            OIRECV OIRECC OIW TOTIIS LOANTYP EXIST COSTCTR PENDBRH USER5
            WDOWNIND RESCHEIND ACCRUAL;
       SET LOANWOFF;
       IF EXIST = 'Y';
       ...
    """
    stmth = 1
    styr  = reptdate_val.year

    df = loanwoff.filter(pl.col("EXIST") == "Y")
    out_rows = []

    for row in df.to_dicts():
        acctno    = row.get("ACCTNO")
        noteno    = row.get("NOTENO")
        name      = row.get("NAME", "") or ""
        ntbrch    = row.get("NTBRCH", 0) or 0
        netproc   = row.get("NETPROC", 0) or 0
        curbal    = row.get("CURBAL", 0) or 0
        borstat   = row.get("BORSTAT", "") or ""
        days      = row.get("DAYS", 0) or 0
        loantype  = row.get("LOANTYPE", 0) or 0
        issdte    = row.get("ISSDTE")
        bldate    = row.get("BLDATE")
        termchg   = row.get("TERMCHG", 0) or 0
        earnterm  = row.get("EARNTERM", 0) or 0
        user5     = row.get("USER5", "") or ""
        writeoff  = row.get("WRITEOFF", "N") or "N"
        wdownind  = row.get("WDOWNIND", "") or ""
        rescheind = row.get("RESCHEIND", "") or ""
        accrual   = row.get("ACCRUAL", 0) or 0
        costctr   = row.get("COSTCTR", 0) or 0
        pendbrh   = row.get("PENDBRH", 0) or 0
        iisp      = row.get("IISP", 0) or 0
        oip       = row.get("OIP", 0) or 0
        iispw     = row.get("IISPW", 0) or 0
        feetot2   = row.get("FEETOT2", 0) or 0
        feeamta   = row.get("FEEAMTA", 0) or 0
        feeamt5   = row.get("FEEAMT5", 0) or 0
        feeamt    = row.get("FEEAMT", 0) or 0
        marketvl  = row.get("MARKETVL", 0) or 0

        # IF WRITEOFF = 'Y' AND WDOWNIND ^= 'Y' THEN BORSTAT = 'W';
        if writeoff == "Y" and wdownind != "Y":
            borstat = "W"

        iis = 0.0; suspend = 0.0; uhc = 0.0; oi = 0.0
        oisusp = 0.0; recover = 0.0; oirecv = 0.0
        oirecc = 0.0; oiw = 0.0; recc = 0.0

        # IF BLDATE > 0 & TERMCHG > 0 THEN DO;
        if bldate and bldate > date(1900, 1, 1) and termchg > 0:
            if (days > 89
                    or borstat in ("F", "R", "I")
                    or (user5 == "N" and loantype not in (983, 993))):
                remmth1 = (earnterm
                           - ((bldate.year - issdte.year) * 12
                              + bldate.month - issdte.month + 1))
                remmth2 = (earnterm
                           - ((reptdate_val.year - issdte.year) * 12
                              + reptdate_val.month - issdte.month + 1))
                remmths = (earnterm
                           - ((styr - issdte.year) * 12
                              + stmth - issdte.month + 1))
                if remmth2 < 0:
                    remmth2 = 0
                if loantype in (128, 130):
                    remmth1 -= 3
                else:
                    remmth1 -= 1
                if remmth1 >= remmth2:
                    iis = iis_sum(remmth1, remmth2, termchg, earnterm)
                oi = feetot2 + (-1) * feeamta + feeamt5
                suspend = iis_sum(remmths, remmth2, termchg, earnterm)
                if loantype not in (128, 130):
                    oisusp = feeamt + (-1) * feeamta + feeamt5
                if remmth2 > 0:
                    uhc = uhc_val(remmth2, termchg, earnterm)
        # ELSE IF DAYS > 89 | BORSTAT IN ('F','R','I') OR (USER5='N'...) THEN DO;
        elif (days > 89
              or borstat in ("F", "R", "I")
              or (user5 == "N" and loantype not in (983, 993))):
            oi     = feetot2 + (-1) * feeamta + feeamt5
            oisusp = feeamt  + (-1) * feeamta + feeamt5

        # IF CURBAL = . THEN CURBAL = 0;
        netbal = curbal - uhc

        # IF NETBAL <= IISP THEN IF DAYS>89 | BORSTAT IN('F','R','I') OR USER5='N'
        if netbal <= iisp:
            if (days > 89
                    or borstat in ("F", "R", "I")
                    or user5 == "N"):
                iis = netbal

        # IF BORSTAT = 'W' THEN DO;
        if borstat == "W":
            iispw = iisp
            oiw   = oip
        else:
            recover = iisp + suspend - iis
            if recover < 0:
                suspend = suspend - recover
                recover = 0.0
            if recover > iisp:
                recc    = recover - iisp
                recover = iisp
            if loantype not in (128, 130):
                oirecv = oip - oi
                if oirecv < 0:
                    oisusp = oisusp - oirecv
                    oirecv = 0.0
                if oisusp < 0:
                    oirecv = oirecv - oisusp
                if oirecv > oip:
                    oirecc = oirecv - oip
                    oirecv = oip

        # IF TERMCHG = 0 THEN DO;
        if termchg == 0:
            if borstat == "R":
                netexp = curbal - iisp - marketvl
            else:
                netexp = curbal - iisp
            if (netexp > 0 and days > 89) or borstat == "R":
                iis    = recover
                recover = 0.0
                oi     = feetot2 + (-1) * feeamta + feeamt5
                oirecv = 0.0

        # IF LOANTYPE IN (131,132) THEN IIS = ACCRUAL;
        if loantype in (131, 132):
            iis = accrual

        oisusp = oirecv + oirecc + oiw - oip + oi
        if oisusp < 0:
            oirecv = oirecv - oisusp
        if oirecv > oip:
            oirecc = oirecv - oip
            oirecv = oip
        oisusp = oirecv + oirecc + oiw - oip + oi

        # BRANCH = PUT(NTBRCH,BRCHCD.)||' '||PUT(NTBRCH,Z3.);
        branch  = branch_label(ntbrch)
        loantyp = lntyp_format(loantype)

        # IF WRITEOFF = 'Y' THEN DO;
        if writeoff == "Y":
            suspend = row.get("WSUSPEND", 0) or 0
            oisusp  = row.get("WOISUSP",  0) or 0
            if wdownind != "Y":
                recover = row.get("WRECOVER", 0) or 0
                recc    = row.get("WRECC",    0) or 0
                oirecv  = row.get("WOIRECV",  0) or 0
                oirecc  = row.get("WOIRECC",  0) or 0
                iis     = 0.0
                iispw   = iisp + suspend + (-1) * recover + (-1) * recc
                oi      = 0.0
                oiw     = oip  + oisusp  + (-1) * oirecv  + (-1) * oirecc
            else:
                oisusp  = row.get("WOISUSP", 0) or 0
                iispw   = row.get("WIISPW",  0) or 0
                iis     = iisp + suspend + (-1) * recover + (-1) * recc + (-1) * iispw
                if iis < 0:
                    recover = 0.0
                iis     = iisp + suspend + (-1) * recover + (-1) * recc + (-1) * iispw
                oiw     = row.get("WOIW", 0) or 0
                oi      = oip  + oisusp  + (-1) * oirecv  + (-1) * oirecc + (-1) * oiw
                if oi < 0:
                    oirecv = 0.0
                    oirecc = 0.0
                oi      = oip  + oisusp  + (-1) * oirecv  + (-1) * oirecc + (-1) * oiw
            # IF OIP=. THEN OIP=0; etc.
            iisp    = iisp    or 0
            oip     = oip     or 0
            suspend = suspend or 0
            oisusp  = oisusp  or 0
            recover = recover or 0
            oirecv  = oirecv  or 0
            recc    = recc    or 0
            oirecc  = oirecc  or 0

        totiis = iis + oi

        # IF RESCHEIND = 'Y' THEN DO;
        if rescheind == "Y":
            suspend = row.get("WSUSPEND", 0) or 0
            oisusp  = row.get("WOISUSP",  0) or 0
            recover = row.get("WRECOVER", 0) or 0
            recc    = row.get("WRECC",    0) or 0
            oirecv  = row.get("WOIRECV",  0) or 0
            oirecc  = row.get("WOIRECC",  0) or 0
            iis     = iisp + suspend + (-1) * recover + (-1) * recc + (-1) * iispw
            oi      = oip  + oisusp  + (-1) * oirecv  + (-1) * oirecc + (-1) * oiw
            totiis  = iis + oi

        out_rows.append({
            "BRANCH":    branch,    "NTBRCH":    ntbrch,    "ACCTNO":    acctno,
            "NOTENO":    noteno,    "NAME":      name,       "NETPROC":   netproc,
            "CURBAL":    curbal,    "BORSTAT":   borstat,   "DAYS":      days,
            "IIS":       iis,       "UHC":       uhc,        "NETBAL":    netbal,
            "IISP":      iisp,      "SUSPEND":   suspend,   "RECOVER":   recover,
            "RECC":      recc,      "IISPW":     iispw,
            "OIP":       oip,       "OISUSP":    oisusp,    "OI":        oi,
            "OIRECV":    oirecv,    "OIRECC":    oirecc,    "OIW":       oiw,
            "TOTIIS":    totiis,    "LOANTYP":   loantyp,   "EXIST":     row.get("EXIST", ""),
            "COSTCTR":   costctr,   "PENDBRH":   pendbrh,   "USER5":     user5,
            "WDOWNIND":  wdownind,  "RESCHEIND": rescheind, "ACCRUAL":   accrual,
            "LOANTYPE":  loantype,
        })

    return pl.from_dicts(out_rows, infer_schema_length=None) if out_rows else pl.DataFrame()


# =============================================================================
# STEP 3b: DATA LOAN2 — calculate IIS for CURRENT NPL accounts (EXIST != 'Y')
# =============================================================================

def calc_loan2(loanwoff: pl.DataFrame, reptdate_val: date) -> pl.DataFrame:
    """
    DATA LOAN2;
       KEEP BRANCH NTBRCH ACCTNO NOTENO NAME NETPROC CURBAL BORSTAT DAYS
            IIS UHC NETBAL IISP SUSPEND RECOVER RECC IISPW OIP OISUSP OI
            OIRECV OIRECC OIW TOTIIS LOANTYP EXIST COSTCTR PENDBRH USER5
            WDOWNIND RESCHEIND ACCRUAL;
       SET LOANWOFF;
       IF EXIST ^= 'Y';
       ...
    """
    df = loanwoff.filter(pl.col("EXIST") != "Y")
    out_rows = []

    for row in df.to_dicts():
        acctno    = row.get("ACCTNO")
        noteno    = row.get("NOTENO")
        name      = row.get("NAME", "") or ""
        ntbrch    = row.get("NTBRCH", 0) or 0
        netproc   = row.get("NETPROC", 0) or 0
        curbal    = row.get("CURBAL", 0) or 0
        borstat   = row.get("BORSTAT", "") or ""
        days      = row.get("DAYS", 0) or 0
        loantype  = row.get("LOANTYPE", 0) or 0
        issdte    = row.get("ISSDTE")
        bldate    = row.get("BLDATE")
        termchg   = row.get("TERMCHG", 0) or 0
        earnterm  = row.get("EARNTERM", 0) or 0
        user5     = row.get("USER5", "") or ""
        writeoff  = row.get("WRITEOFF", "N") or "N"
        wdownind  = row.get("WDOWNIND", "") or ""
        rescheind = row.get("RESCHEIND", "") or ""
        accrual   = row.get("ACCRUAL", 0) or 0
        costctr   = row.get("COSTCTR", 0) or 0
        pendbrh   = row.get("PENDBRH", 0) or 0
        iisp      = row.get("IISP", 0) or 0
        oip       = row.get("OIP", 0) or 0
        iispw     = row.get("IISPW", 0) or 0
        feetot2   = row.get("FEETOT2", 0) or 0
        feeamta   = row.get("FEEAMTA", 0) or 0
        feeamt5   = row.get("FEEAMT5", 0) or 0
        oirecv    = 0.0
        oirecc    = 0.0
        recover   = 0.0
        recc      = 0.0
        oiw       = 0.0
        iispw     = iispw or 0

        # IF WRITEOFF = 'Y' AND WDOWNIND ^= 'Y' THEN BORSTAT = 'W';
        if writeoff == "Y" and wdownind != "Y":
            borstat = "W"

        iis = 0.0; uhc = 0.0

        # OI is always computed (SUM(FEETOT2,(-1)*FEEAMTA,FEEAMT5)) unconditionally
        oi = feetot2 + (-1) * feeamta + feeamt5

        # IF BLDATE > 0 & TERMCHG > 0 OR (USER5='N' AND LOANTYPE NOT IN (983,993))
        if ((bldate and bldate > date(1900, 1, 1) and termchg > 0)
                or (user5 == "N" and loantype not in (983, 993))):
            remmth1 = (earnterm
                       - ((bldate.year - issdte.year) * 12
                          + bldate.month - issdte.month + 1)) if (bldate and issdte) else 0
            remmth2 = (earnterm
                       - ((reptdate_val.year - issdte.year) * 12
                          + reptdate_val.month - issdte.month + 1)) if issdte else 0
            if remmth2 < 0:
                remmth2 = 0
            if loantype in (128, 130):
                remmth1 -= 3
            else:
                remmth1 -= 1
            if remmth1 >= remmth2:
                iis = iis_sum(remmth1, remmth2, termchg, earnterm)
            if remmth2 > 0:
                uhc = uhc_val(remmth2, termchg, earnterm)
        else:
            remmth2 = (earnterm
                       - ((reptdate_val.year - issdte.year) * 12
                          + reptdate_val.month - issdte.month + 1)) if issdte else 0
            if remmth2 < 0:
                remmth2 = 0
            if remmth2 > 0:
                uhc = uhc_val(remmth2, termchg, earnterm)

        # IF LOANTYPE IN (131,132) THEN IIS = ACCRUAL;
        if loantype in (131, 132):
            iis = accrual

        suspend = iis
        oisusp  = oi
        netbal  = curbal - uhc

        # IF WRITEOFF = 'Y' THEN DO;
        if writeoff == "Y":
            suspend = row.get("WSUSPEND", 0) or 0
            oisusp  = row.get("WOISUSP",  0) or 0
            if wdownind != "Y":
                recover = row.get("WRECOVER", 0) or 0
                recc    = row.get("WRECC",    0) or 0
                oirecv  = row.get("WOIRECV",  0) or 0
                oirecc  = row.get("WOIRECC",  0) or 0
                iis     = 0.0
                iispw   = iisp + suspend + (-1) * recover + (-1) * recc
                oi      = 0.0
                oiw     = oip  + oisusp  + (-1) * oirecv  + (-1) * oirecc
            else:
                oisusp  = row.get("WOISUSP", 0) or 0
                iispw   = row.get("WIISPW",  0) or 0
                iis     = iisp + suspend + (-1) * recover + (-1) * recc + (-1) * iispw
                if iis < 0:
                    recover = 0.0
                iis     = iisp + suspend + (-1) * recover + (-1) * recc + (-1) * iispw
                oiw     = row.get("WOIW", 0) or 0
                oi      = oip  + oisusp  + (-1) * oirecv  + (-1) * oirecc + (-1) * oiw
                if oi < 0:
                    oirecv = 0.0
                    oirecc = 0.0
                oi      = oip  + oisusp  + (-1) * oirecv  + (-1) * oirecc + (-1) * oiw
            # IF OIP=. THEN OIP=0; etc.
            iisp    = iisp    or 0
            oip     = oip     or 0
            suspend = suspend or 0
            oisusp  = oisusp  or 0
            recover = recover or 0
            oirecv  = oirecv  or 0
            recc    = recc    or 0
            oirecc  = oirecc  or 0

        totiis = iis + oi

        # BRANCH = PUT(NTBRCH,BRCHCD.)||' '||PUT(NTBRCH,Z3.);
        branch  = branch_label(ntbrch)
        loantyp = lntyp_format(loantype)

        # IF RESCHEIND = 'Y' THEN DO;
        if rescheind == "Y":
            suspend = row.get("WSUSPEND", 0) or 0
            oisusp  = row.get("WOISUSP",  0) or 0
            recover = row.get("WRECOVER", 0) or 0
            recc    = row.get("WRECC",    0) or 0
            oirecv  = row.get("WOIRECV",  0) or 0
            oirecc  = row.get("WOIRECC",  0) or 0
            iis     = iisp + suspend + (-1) * recover + (-1) * recc + (-1) * iispw
            oi      = oip  + oisusp  + (-1) * oirecv  + (-1) * oirecc + (-1) * oiw
            totiis  = iis + oi

        out_rows.append({
            "BRANCH":    branch,    "NTBRCH":    ntbrch,    "ACCTNO":    acctno,
            "NOTENO":    noteno,    "NAME":      name,       "NETPROC":   netproc,
            "CURBAL":    curbal,    "BORSTAT":   borstat,   "DAYS":      days,
            "IIS":       iis,       "UHC":       uhc,        "NETBAL":    netbal,
            "IISP":      iisp,      "SUSPEND":   suspend,   "RECOVER":   recover,
            "RECC":      recc,      "IISPW":     iispw,
            "OIP":       oip,       "OISUSP":    oisusp,    "OI":        oi,
            "OIRECV":    oirecv,    "OIRECC":    oirecc,    "OIW":       oiw,
            "TOTIIS":    totiis,    "LOANTYP":   loantyp,   "EXIST":     row.get("EXIST", ""),
            "COSTCTR":   costctr,   "PENDBRH":   pendbrh,   "USER5":     user5,
            "WDOWNIND":  wdownind,  "RESCHEIND": rescheind, "ACCRUAL":   accrual,
            "LOANTYPE":  loantype,
        })

    return pl.from_dicts(out_rows, infer_schema_length=None) if out_rows else pl.DataFrame()


# =============================================================================
# STEP 4: %MACRO MONTHLY — compare previous month NPL accounts
# =============================================================================

def apply_monthly(loan1: pl.DataFrame, loan2: pl.DataFrame,
                  reptmon: str, prevmon: str,
                  reptdate_val: date) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    %MACRO MONTHLY
    %IF "&REPTMON" EQ "01" %THEN %DO — zero out cumulative fields.
    %ELSE %DO — merge with previous month IIS data.
    """
    # %IF "&REPTMON" EQ "01" %THEN %DO;
    if reptmon == "01":
        for col in ("IISPCUM", "OIPCUM", "POI"):
            loan1 = loan1.with_columns(pl.lit(0.0).alias(col))
            loan2 = loan2.with_columns(pl.lit(0.0).alias(col))
        return loan1, loan2

    # %ELSE %DO;
    iis_prev_file = str(IIS_PREV_TMPL).format(prevmon=prevmon)
    ploan_file    = str(PLOAN_TMPL).format(reptmon=reptmon)
    con = duckdb.connect()

    # PROC SORT DATA=NPL.IIS&PREVMON (DROP=POI RENAME=(...)) OUT=IISPREV NODUPKEY;
    # BY ACCTNO NOTENO;
    iisprev_raw = con.execute(f"""
        SELECT ACCTNO, NOTENO, NTBRCH, LOANTYPE, PAIDIND, EXIST,
               DAYS     AS PDAYS,
               SUSPEND  AS PSUSPEND,
               OISUSP   AS POISUSP,
               IISP     AS PIISP,
               OIP      AS POIP,
               OI       AS POI,
               RECC     AS PRECC,
               OIRECC   AS POIRECC,
               RECOVER  AS PRECOVER,
               OIRECV   AS POIRECV
        FROM '{iis_prev_file}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO, NOTENO ORDER BY ACCTNO) = 1
        ORDER BY ACCTNO, NOTENO
    """).pl()
    con.close()

    # DATA IISPREV;
    #    SET IISPREV;
    #    IF LOANTYPE IN (128,130,131,132,380,381,390,700,705,720,725,983,993,996)
    #    AND PAIDIND='P' THEN DO;
    #       %INC PGM(NPLNTB);  -- NPLNTB contains branch transfer mappings and HP centre
    #       setup that are entirely commented out in the original SAS source.
    #       No active logic to apply.
    #    END;
    #    BRANCH = PUT(NTBRCH,BRCHCD.)||' '||PUT(NTBRCH,Z3.);
    #    IF PDAYS=. THEN PDAYS=0; ...
    npl_types = (128, 130, 131, 132, 380, 381, 390,
                 700, 705, 720, 725, 983, 993, 996)

    iisprev_rows = []
    for r in iisprev_raw.to_dicts():
        # %INC PGM(NPLNTB) — all branch-transfer and HP-centre logic in NPLNTB
        # is entirely commented out in the original SAS; no active code to apply.
        if r.get("LOANTYPE") in npl_types and r.get("PAIDIND") == "P":
            pass
        r["BRANCH"] = branch_label(r.get("NTBRCH", 0) or 0)
        r["PDAYS"]    = r.get("PDAYS")    or 0
        r["PSUSPEND"] = r.get("PSUSPEND") or 0
        r["POISUSP"]  = r.get("POISUSP")  or 0
        r["PIISP"]    = r.get("PIISP")    or 0
        r["POIP"]     = r.get("POIP")     or 0
        r["POI"]      = r.get("POI")      or 0
        iisprev_rows.append(r)

    iisprev = (pl.from_dicts(iisprev_rows, infer_schema_length=None)
               if iisprev_rows else pl.DataFrame())

    # *** EXISTING NPL ***
    loan1 = _monthly_loan1(loan1, iisprev)

    # *** CURRENT NPL ***
    loan2 = _monthly_loan2(loan2, iisprev, ploan_file)

    return loan1, loan2


# =============================================================================
# MONTHLY helper: EXISTING NPL (LOAN1)
# =============================================================================

def _monthly_loan1(loan1: pl.DataFrame, iisprev: pl.DataFrame) -> pl.DataFrame:
    """
    PROC SORT DATA=LOAN1; BY ACCTNO;
    DATA LOAN1 (DROP=PDAYS PSUSPEND POISUSP PIISP POIP PRECC POIRECC PRECOVER POIRECV);
       MERGE IISPREV(IN=B) LOAN1(IN=A);
       BY ACCTNO;
       IF ((A AND B) OR (B AND NOT A)) AND EXIST='Y';
       ...
    """
    prev_cols = ["ACCTNO", "PDAYS", "PSUSPEND", "POISUSP", "PIISP",
                 "POIP", "POI", "PRECC", "POIRECC", "PRECOVER", "POIRECV"]
    iisprev_sel = (iisprev.select([c for c in prev_cols if c in iisprev.columns])
                   if not iisprev.is_empty() else pl.DataFrame())

    iisprev_dict = {}
    if not iisprev_sel.is_empty():
        for r in iisprev_sel.to_dicts():
            iisprev_dict[r["ACCTNO"]] = r

    loan1_accts = set(loan1["ACCTNO"].to_list()) if not loan1.is_empty() else set()

    drop_cols = {"PDAYS", "PSUSPEND", "POISUSP", "PIISP", "POIP",
                 "PRECC", "POIRECC", "PRECOVER", "POIRECV"}
    out_rows = []

    # (B AND NOT A) — accounts in iisprev not in loan1, with EXIST='Y'
    for acctno, prev in iisprev_dict.items():
        if acctno not in loan1_accts:
            # A/C SETTLE FOR EXISTING NPL
            piisp    = prev.get("PIISP", 0) or 0
            psuspend = prev.get("PSUSPEND", 0) or 0
            poisusp  = prev.get("POISUSP", 0) or 0
            poip     = prev.get("POIP", 0) or 0
            iispw    = 0.0; oiw = 0.0
            iisp     = piisp;    recover = iisp;    suspend = psuspend; recc   = suspend
            oip_out  = poip;     oirecv  = oip_out; oisusp  = poisusp;  oirecc = oisusp
            curbal   = 0;        netbal  = 0;        days    = 0
            oi  = oip_out + oisusp + (-1) * oirecv + (-1) * oirecc + (-1) * oiw
            iis = iisp    + suspend + (-1) * recover + (-1) * recc  + (-1) * iispw
            totiis = iis + oi
            out_rows.append({
                "BRANCH":  prev.get("BRANCH", ""), "NTBRCH":  prev.get("NTBRCH", 0),
                "ACCTNO":  acctno, "NOTENO":  None, "NAME":    "",
                "NETPROC": 0,      "CURBAL":  curbal, "BORSTAT": "",
                "DAYS":    days,   "IIS":     iis,   "UHC":    0.0,
                "NETBAL":  netbal, "IISP":    iisp,  "SUSPEND": suspend,
                "RECOVER": recover,"RECC":    recc,  "IISPW":  iispw,
                "OIP":     oip_out,"OISUSP":  oisusp,"OI":     oi,
                "OIRECV":  oirecv, "OIRECC":  oirecc,"OIW":    oiw,
                "TOTIIS":  totiis, "LOANTYP": "",    "EXIST":  "Y",
                "COSTCTR": 0,      "PENDBRH": 0,     "USER5":  "",
                "WDOWNIND":"",     "RESCHEIND":"",   "ACCRUAL":0.0,
                "LOANTYPE":0,
            })

    # (A AND B) — accounts in both loan1 and iisprev, with EXIST='Y'
    for row in (loan1.to_dicts() if not loan1.is_empty() else []):
        acctno = row.get("ACCTNO")
        exist  = row.get("EXIST", "") or ""
        in_b   = acctno in iisprev_dict

        if not in_b or exist != "Y":
            continue

        prev     = iisprev_dict[acctno]
        borstat  = row.get("BORSTAT", "") or ""
        curbal   = row.get("CURBAL",  0)  or 0
        days     = row.get("DAYS",    0)  or 0
        user5    = row.get("USER5",   "") or ""
        rescheind= row.get("RESCHEIND","") or ""
        iisp     = row.get("IISP",    0)  or 0
        oip      = row.get("OIP",     0)  or 0
        iispw    = row.get("IISPW",   0)  or 0
        oiw      = row.get("OIW",     0)  or 0
        iis      = row.get("IIS",     0)  or 0
        oi       = row.get("OI",      0)  or 0
        suspend  = row.get("SUSPEND", 0)  or 0
        oisusp   = row.get("OISUSP",  0)  or 0
        recover  = row.get("RECOVER", 0)  or 0
        recc     = row.get("RECC",    0)  or 0
        oirecv   = row.get("OIRECV",  0)  or 0
        oirecc   = row.get("OIRECC",  0)  or 0
        netbal   = row.get("NETBAL",  0)  or 0

        pdays    = prev.get("PDAYS",    0) or 0
        psuspend = prev.get("PSUSPEND", 0) or 0
        poisusp  = prev.get("POISUSP",  0) or 0
        piisp    = prev.get("PIISP",    0) or 0
        poip     = prev.get("POIP",     0) or 0
        poi      = prev.get("POI",      0) or 0
        precc    = prev.get("PRECC",    0) or 0
        poirecc  = prev.get("POIRECC",  0) or 0
        precover = prev.get("PRECOVER", 0) or 0
        poirecv  = prev.get("POIRECV",  0) or 0

        # *** A/C SETTLE FOR EXISTING NPL ***
        # IF ((B AND NOT A) OR (CURBAL LE 0 AND POI LE 0)) AND
        #    BORSTAT NOT IN ('F','I','R','W','S')
        if (curbal <= 0 and poi <= 0) and borstat not in ("F", "I", "R", "W", "S"):
            iisp     = piisp;    recover = iisp;    suspend = psuspend; recc   = suspend
            oip      = poip;     oirecv  = oip;     oisusp  = poisusp;  oirecc = oisusp
            curbal   = 0;        netbal  = 0;        days    = 0
            oi       = oip    + oisusp + (-1) * oirecv  + (-1) * oirecc + (-1) * oiw
            iis      = iisp   + suspend + (-1) * recover + (-1) * recc  + (-1) * iispw
            totiis   = iis + oi
            row.update({"IISP": iisp, "RECOVER": recover, "SUSPEND": suspend,
                        "RECC": recc, "OIP": oip, "OIRECV": oirecv,
                        "OISUSP": oisusp, "OIRECC": oirecc, "CURBAL": curbal,
                        "NETBAL": netbal, "DAYS": days, "OI": oi,
                        "IIS": iis, "TOTIIS": totiis})
            out_rows.append({k: v for k, v in row.items() if k not in drop_cols})
            continue

        # IF BORSTAT IN ('W') OR RESCHEIND='Y' THEN OUTPUT;
        if borstat == "W" or rescheind == "Y":
            out_rows.append({k: v for k, v in row.items() if k not in drop_cols})
            continue

        # IF (A AND B) THEN DO;
        # *** CONTINUE PERFORMING ***
        if days < 90 and pdays < 90:
            if user5 == "N":
                if iis < iisp:
                    suspend = 0.0; recover = iisp - iis; recc = 0.0
                if iis >= iisp:
                    suspend = iis - iisp; recover = 0.0; recc = 0.0
                if iisp == 0:
                    suspend = iis; recc = iis - suspend
                if oi < oip:
                    oisusp = 0.0; oirecv = oip - oi; oirecc = 0.0
                if oi >= oip:
                    oisusp = oi - oip; oirecv = 0.0; oirecc = 0.0
                if oip == 0:
                    oisusp = oi; oirecc = oi - oisusp
            row.update({"SUSPEND": suspend, "RECOVER": recover, "RECC": recc,
                        "OISUSP": oisusp, "OIRECV": oirecv, "OIRECC": oirecc})
            out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

        # *** TURN PERFORMING ***
        if days < 90 and pdays >= 90:
            if borstat not in ("F", "I", "R"):
                suspend = psuspend; recc   = psuspend
                oisusp  = poisusp;  oirecc = poisusp
                totiis  = iis + oi
            if user5 == "N":
                if iis < iisp:
                    suspend = 0.0; recover = iisp - iis; recc = 0.0
                if iis >= iisp:
                    suspend = iis - iisp; recover = 0.0; recc = 0.0
                if iisp == 0:
                    suspend = iis; recc = iis - suspend
                if oi < oip:
                    oisusp = 0.0; oirecv = oip - oi; oirecc = 0.0
                if oi >= oip:
                    oisusp = oi - oip; oirecv = 0.0; oirecc = 0.0
                if oip == 0:
                    oisusp = oi; oirecc = oi - oisusp
            row.update({"SUSPEND": suspend, "RECOVER": recover, "RECC": recc,
                        "OISUSP": oisusp, "OIRECV": oirecv, "OIRECC": oirecc,
                        "TOTIIS": iis + oi})
            out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

        # *** TURN NPL FR PERFORMING ***
        if days >= 90 and pdays < 90:
            if borstat not in ("F", "I", "R"):
                recc    = precc;    recover = precover
                suspend = iis + iisp + (-1) * recover + recc
                if suspend < 0:
                    recover = recover + (-1) * suspend; suspend = 0.0
                    if recover > iisp:
                        recc = recc + recover - iisp
                oirecc  = poirecc;  oirecv  = poirecv
                oisusp  = oi + oip + (-1) * oirecv + oirecc
                if oisusp < 0:
                    oirecv = oirecv + (-1) * oisusp; oisusp = 0.0
                    if oirecv > oip:
                        oirecc = oirecc + oirecv - oip
                totiis  = iis + oi
            if user5 == "N":
                if iis < iisp:
                    suspend = 0.0; recover = iisp - iis; recc = 0.0
                if iis >= iisp:
                    suspend = iis - iisp; recover = 0.0; recc = 0.0
                if iisp == 0:
                    suspend = iis; recc = iis - suspend
                if oi < oip:
                    oisusp = 0.0; oirecv = oip - oi; oirecc = 0.0
                if oi >= oip:
                    oisusp = oi - oip; oirecv = 0.0; oirecc = 0.0
                if oip == 0:
                    oisusp = oi; oirecc = oi - oisusp
            row.update({"SUSPEND": suspend, "RECOVER": recover, "RECC": recc,
                        "OISUSP": oisusp, "OIRECV": oirecv, "OIRECC": oirecc,
                        "TOTIIS": iis + oi})
            out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

        # *** CONTINUE NPL ***
        if days >= 90 and pdays >= 90:
            if borstat not in ("F", "I", "R"):
                recover = precover; recc   = precc
                suspend = iis + (-1) * iisp + recover + recc
                if suspend < 0:
                    recover = recover + (-1) * suspend; suspend = 0.0
                    if recover > iisp:
                        recc = recc + recover + (-1) * iisp
                oirecv  = poirecv;  oirecc = poirecc
                oisusp  = oi + (-1) * oip + oirecv + oirecc
                if oisusp < 0:
                    oirecv = oirecv + (-1) * oisusp; oisusp = 0.0
                    if oirecv > oip:
                        oirecc = oirecc + oirecv + (-1) * oip
                totiis  = iis + oi
            row.update({"SUSPEND": suspend, "RECOVER": recover, "RECC": recc,
                        "OISUSP": oisusp, "OIRECV": oirecv, "OIRECC": oirecc,
                        "TOTIIS": iis + oi})
            out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

        # *** SPECIAL USER5=N ***
        if user5 == "N" and iis == 0 and iisp == 0:
            row["SUSPEND"] = iis

    return pl.from_dicts(out_rows, infer_schema_length=None) if out_rows else pl.DataFrame()


# =============================================================================
# MONTHLY helper: CURRENT NPL (LOAN2)
# =============================================================================

def _monthly_loan2(loan2: pl.DataFrame, iisprev: pl.DataFrame,
                   ploan_file: str) -> pl.DataFrame:
    """
    PROC SORT DATA=NPL.PLOAN&REPTMON OUT=PLOAN
       (KEEP=ACCTNO NOTENO CURBAL DAYS BORSTAT NTBRCH COSTCTR);
    DATA IISPREV;
       MERGE IISPREV(IN=A) PLOAN(IN=B);
       BY ACCTNO;
       IF PIISP EQ 0 AND POIP EQ 0 AND EXIST NE 'Y';
    DATA LOAN2 (...);
       MERGE IISPREV(IN=B) LOAN2(IN=A);
       BY ACCTNO;
       ...
    """
    con = duckdb.connect()
    ploan = con.execute(f"""
        SELECT ACCTNO, NOTENO, CURBAL, DAYS, BORSTAT, NTBRCH, COSTCTR
        FROM '{ploan_file}'
        ORDER BY ACCTNO
    """).pl()
    con.close()

    # DATA IISPREV — filter where PIISP=0 AND POIP=0 AND EXIST!='Y'
    if not iisprev.is_empty():
        ploan_accts = set(ploan["ACCTNO"].to_list())
        iisprev2_rows = []
        for r in iisprev.to_dicts():
            piisp = r.get("PIISP", 0) or 0
            poip  = r.get("POIP",  0) or 0
            exist = r.get("EXIST", "") or ""
            if piisp == 0 and poip == 0 and exist != "Y":
                r["BRANCH"] = branch_label(r.get("NTBRCH", 0) or 0)
                iisprev2_rows.append(r)
        iisprev2 = (pl.from_dicts(iisprev2_rows, infer_schema_length=None)
                    if iisprev2_rows else pl.DataFrame())
    else:
        iisprev2 = pl.DataFrame()

    iisprev2_dict  = {}
    if not iisprev2.is_empty():
        for r in iisprev2.to_dicts():
            iisprev2_dict[r["ACCTNO"]] = r

    loan2_accts = set(loan2["ACCTNO"].to_list()) if not loan2.is_empty() else set()

    drop_cols = {"PDAYS", "PSUSPEND", "POISUSP", "PIISP", "POIP",
                 "PRECC", "POIRECC", "PRECOVER", "POIRECV"}
    out_rows = []

    # (B AND NOT A) — settled accounts: in iisprev2 but not in loan2
    for acctno, prev in iisprev2_dict.items():
        if acctno not in loan2_accts:
            piisp    = prev.get("PIISP",    0) or 0
            psuspend = prev.get("PSUSPEND", 0) or 0
            poisusp  = prev.get("POISUSP",  0) or 0
            poip     = prev.get("POIP",     0) or 0
            iispw    = 0.0; oiw = 0.0
            iisp     = piisp;    recover = iisp;    suspend = psuspend; recc   = suspend
            oip_out  = poip;     oirecv  = oip_out; oisusp  = poisusp;  oirecc = oisusp
            curbal   = 0;        netbal  = 0;        days    = 0
            oi  = oip_out + oisusp + (-1) * oirecv  + (-1) * oirecc + (-1) * oiw
            iis = iisp    + suspend + (-1) * recover + (-1) * recc  + (-1) * iispw
            totiis = iis + oi
            out_rows.append({
                "BRANCH":  prev.get("BRANCH", ""), "NTBRCH":  prev.get("NTBRCH", 0),
                "ACCTNO":  acctno, "NOTENO":  None, "NAME":    "",
                "NETPROC": 0,      "CURBAL":  curbal, "BORSTAT": "",
                "DAYS":    days,   "IIS":     iis,   "UHC":    0.0,
                "NETBAL":  netbal, "IISP":    iisp,  "SUSPEND": suspend,
                "RECOVER": recover,"RECC":    recc,  "IISPW":  iispw,
                "OIP":     oip_out,"OISUSP":  oisusp,"OI":     oi,
                "OIRECV":  oirecv, "OIRECC":  oirecc,"OIW":    oiw,
                "TOTIIS":  totiis, "LOANTYP": "",    "EXIST":  "",
                "COSTCTR": 0,      "PENDBRH": 0,     "USER5":  "",
                "WDOWNIND":"",     "RESCHEIND":"",   "ACCRUAL":0.0,
                "LOANTYPE":0,
            })

    for row in (loan2.to_dicts() if not loan2.is_empty() else []):
        acctno    = row.get("ACCTNO")
        in_b      = acctno in iisprev2_dict
        prev      = iisprev2_dict.get(acctno, {})

        borstat   = row.get("BORSTAT",  "") or ""
        curbal    = row.get("CURBAL",   0)  or 0
        days      = row.get("DAYS",     0)  or 0
        user5     = row.get("USER5",    "") or ""
        rescheind = row.get("RESCHEIND","") or ""
        iisp      = row.get("IISP",     0)  or 0
        oip       = row.get("OIP",      0)  or 0
        iispw     = row.get("IISPW",    0)  or 0
        oiw       = row.get("OIW",      0)  or 0
        iis       = row.get("IIS",      0)  or 0
        oi        = row.get("OI",       0)  or 0
        suspend   = row.get("SUSPEND",  0)  or 0
        oisusp    = row.get("OISUSP",   0)  or 0
        recover   = row.get("RECOVER",  0)  or 0
        recc      = row.get("RECC",     0)  or 0
        oirecv    = row.get("OIRECV",   0)  or 0
        oirecc    = row.get("OIRECC",   0)  or 0

        pdays    = prev.get("PDAYS",    0) or 0
        psuspend = prev.get("PSUSPEND", 0) or 0
        poisusp  = prev.get("POISUSP",  0) or 0
        piisp    = prev.get("PIISP",    0) or 0
        poip     = prev.get("POIP",     0) or 0
        precc    = prev.get("PRECC",    0) or 0
        poirecc  = prev.get("POIRECC",  0) or 0
        precover = prev.get("PRECOVER", 0) or 0
        poirecv  = prev.get("POIRECV",  0) or 0

        # *** NEW NPL FOR THE MTH ***
        # IF (A AND NOT B) AND (DAYS GE 90 OR BORSTAT IN ('F','I','R','W') OR USER5='N')
        if not in_b:
            if (days >= 90 or borstat in ("F", "I", "R", "W") or user5 == "N"):
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})
            continue

        # IF BORSTAT IN ('W') OR RESCHEIND='Y' THEN OUTPUT;
        if borstat == "W" or rescheind == "Y":
            out_rows.append({k: v for k, v in row.items() if k not in drop_cols})
            continue

        # IF (A AND B) AND BORSTAT NOT IN ('W') THEN DO;
        if in_b and borstat != "W":

            # *** CONTINUE PERFORMING ***
            if days < 90 and pdays < 90:
                if borstat not in ("F", "I", "R"):
                    suspend = psuspend; recc   = psuspend
                    oisusp  = poisusp;  oirecc = poisusp
                if user5 == "N":
                    if iis < iisp:
                        suspend = 0.0; recover = iisp - iis; recc = 0.0
                    if iis >= iisp:
                        suspend = iis - iisp; recover = 0.0; recc = 0.0
                    if iisp == 0:
                        suspend = iis; recc = iis - suspend
                    if oi < oip:
                        oisusp = 0.0; oirecv = oip - oi; oirecc = 0.0
                    if oi >= oip:
                        oisusp = oi - oip; oirecv = 0.0; oirecc = 0.0
                    if oip == 0:
                        oisusp = oi; oirecc = oi - oisusp
                else:
                    # ELSE DO — only when USER5 != 'N'
                    suspend = suspend + recc
                    oisusp  = oisusp  + oirecc
                row.update({"SUSPEND": suspend, "RECC": recc, "OISUSP": oisusp,
                            "OIRECC": oirecc, "RECOVER": recover, "OIRECV": oirecv,
                            "TOTIIS": iis + oi})
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

            # *** TURN PERFORMING FR NPL ***
            if days < 90 and pdays >= 90:
                if borstat not in ("F", "I", "R"):
                    suspend = psuspend; recc   = psuspend
                    oisusp  = poisusp;  oirecc = poisusp
                    oi  = oip  + oisusp + (-1) * oirecv  + (-1) * oirecc + (-1) * oiw
                    iis = iisp + suspend + (-1) * recover + (-1) * recc  + (-1) * iispw
                    totiis = iis + oi
                if user5 == "N":
                    if iis < iisp:
                        suspend = 0.0; recover = iisp - iis; recc = 0.0
                    if iis >= iisp:
                        suspend = iis - iisp; recover = 0.0; recc = 0.0
                    if iisp == 0:
                        suspend = iis; recc = iis - suspend
                    if oi < oip:
                        oisusp = 0.0; oirecv = oip - oi; oirecc = 0.0
                    if oi >= oip:
                        oisusp = oi - oip; oirecv = 0.0; oirecc = 0.0
                    if oip == 0:
                        oisusp = oi; oirecc = oi - oisusp
                row.update({"SUSPEND": suspend, "RECC": recc, "OISUSP": oisusp,
                            "OIRECC": oirecc, "RECOVER": recover, "OIRECV": oirecv,
                            "IIS": iis, "OI": oi, "TOTIIS": iis + oi})
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

            # *** TURN NPL FR PERFORMING ***
            if days >= 90 and pdays < 90:
                if borstat not in ("F", "I", "R"):
                    recc   = recc   + precc
                    suspend= suspend + recc
                    oirecc = oirecc + poirecc
                    oisusp = oisusp + oirecc
                if user5 == "N":
                    if iis < iisp:
                        suspend = 0.0; recover = iisp - iis; recc = 0.0
                    if iis >= iisp:
                        suspend = iis - iisp; recover = 0.0; recc = 0.0
                    if iisp == 0:
                        suspend = iis; recc = iis - suspend
                    if oi < oip:
                        oisusp = 0.0; oirecv = oip - oi; oirecc = 0.0
                    if oi >= oip:
                        oisusp = oi - oip; oirecv = 0.0; oirecc = 0.0
                    if oip == 0:
                        oisusp = oi; oirecc = oi - oisusp
                row.update({"SUSPEND": suspend, "RECC": recc, "OISUSP": oisusp,
                            "OIRECC": oirecc, "RECOVER": recover, "OIRECV": oirecv,
                            "TOTIIS": iis + oi})
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

            # *** CONTINUE NPL ***
            if days >= 90 and pdays >= 90:
                if borstat not in ("F", "I", "R"):
                    recc   = recc   + precc
                    suspend= suspend + recc
                    oirecc = oirecc + poirecc
                    oisusp = oisusp + oirecc
                row.update({"SUSPEND": suspend, "RECC": recc, "OISUSP": oisusp,
                            "OIRECC": oirecc, "TOTIIS": iis + oi})
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

            # *** SPECIAL USER5=N ***
            if user5 == "N" and iis == 0 and iisp == 0:
                row["SUSPEND"] = iis

    return pl.from_dicts(out_rows, infer_schema_length=None) if out_rows else pl.DataFrame()


# =============================================================================
# STEP 5: DATA LOAN3 — combine LOAN1 & LOAN2, assign RISK
# =============================================================================

def build_loan3(loan1: pl.DataFrame, loan2: pl.DataFrame) -> pl.DataFrame:
    """
    DATA LOAN3 NPL.IIS&REPTMON NPL.IIS;
       SET LOAN1 LOAN2;
       LENGTH RISK $13;
       IF DAYS > 364 OR BORSTAT = 'W'  THEN RISK = 'BAD';
       ELSE IF DAYS > 273               THEN RISK = 'DOUBTFUL';
       ELSE IF DAYS > 182               THEN RISK = 'SUBSTANDARD 2';
       ELSE IF DAYS < 90 AND USER5='N'  THEN RISK = 'SUBSTANDARD-1';
       ELSE RISK = 'SUBSTANDARD-1';
       WHERE (3000<=COSTCTR<=3999) OR COSTCTR IN (4043,4048);
    PROC SORT DATA=LOAN3     NODUPKEY; BY ACCTNO NOTENO;
    PROC SORT DATA=NPL.IIS&REPTMON NODUPKEY; BY ACCTNO NOTENO;
    PROC SORT DATA=NPL.IIS   NODUPKEY; BY ACCTNO NOTENO;
    """
    combined = pl.concat([loan1, loan2], how="diagonal")

    def assign_risk(days: int, borstat: str, user5: str) -> str:
        if days > 364 or borstat == "W":
            return "BAD"
        elif days > 273:
            return "DOUBTFUL"
        elif days > 182:
            return "SUBSTANDARD 2"
        elif days < 90 and user5 == "N":
            return "SUBSTANDARD-1"
        else:
            return "SUBSTANDARD-1"

    combined = combined.with_columns(
        pl.struct(["DAYS", "BORSTAT", "USER5"]).map_elements(
            lambda r: assign_risk(
                r["DAYS"]    or 0,
                r["BORSTAT"] or "",
                r["USER5"]   or ""
            ),
            return_dtype=pl.Utf8
        ).alias("RISK")
    )

    loan3 = combined.filter(
        ((pl.col("COSTCTR") >= 3000) & (pl.col("COSTCTR") <= 3999))
        | pl.col("COSTCTR").is_in([4043, 4048])
    )

    # PROC SORT NODUPKEY BY ACCTNO NOTENO
    loan3 = loan3.unique(subset=["ACCTNO", "NOTENO"], keep="first")
    return loan3


# =============================================================================
# REPORTING HELPERS
# OPTIONS NOCENTER NODATE NONUMBER MISSING=0
# =============================================================================

PAGE_LEN = 60   # default page length (lines per page)


def format_num(val, width: int = 15, dec: int = 2) -> str:
    """Format number as COMMA15.2 equivalent. MISSING=0 so None/NaN -> 0."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        val = 0.0
    return f"{val:>{width},.{dec}f}"


def format_count(val, width: int = 7) -> str:
    """Format count as COMMA7. equivalent."""
    if val is None:
        val = 0
    return f"{int(val):>{width},}"


class ReportWriter:
    """ASA carriage control report writer (one character prefix per line)."""
    ASA_NEW_PAGE = "1"   # form feed / new page
    ASA_DOUBLE   = "0"   # double space (blank line before)
    ASA_SINGLE   = " "   # single space
    ASA_OVERPRINT= "+"   # overprint / no advance

    def __init__(self, filepath: Path):
        self.filepath    = filepath
        self.lines: list[str] = []
        self.line_count  = 0
        self.page_num    = 0

    def _new_page_internal(self):
        self.page_num  += 1
        self.line_count = 0

    def write(self, text: str, cc: str = " "):
        """Append a line with ASA carriage control prefix."""
        self.lines.append(cc + text)
        if cc != "+":
            self.line_count += 1
        if self.line_count >= PAGE_LEN:
            self._new_page_internal()

    def new_page(self):
        """Force a new page (ASA '1')."""
        self._new_page_internal()

    def title_block(self, title1: str, title2: str):
        """
        TITLE1 '...';
        TITLE2 '...';
        Emits title1 with ASA new-page, title2 with single space.
        """
        self.write(title1.center(132), self.ASA_NEW_PAGE)
        self.write(title2.center(132), self.ASA_SINGLE)
        self.write("", self.ASA_SINGLE)

    def flush(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            f.write("\n".join(self.lines) + "\n")


# =============================================================================
# %MACRO TBLS — PROC TABULATE report (I=3, i.e. LOAN3 only)
# =============================================================================

def produce_tabulate_report(loan3: pl.DataFrame, rdate: str,
                             writer: ReportWriter) -> None:
    """
    %MACRO TBLS;
       %DO I = 3 %TO 3;
          PROC TABULATE DATA=LOAN&I FORMAT=COMMA15.2 MISSING NOSEPS;
             TABLE 1: LOANTYP x RISK x BRANCH
             TABLE 2: LOANTYP x BRANCH
          TITLE1 'PUBLIC ISLAMIC BANK - (NPL FROM 3 MONTHS & ABOVE) - NEW';
          TITLE2 &TTL &RDATE &&TBL&I;   (&TBL3 = '(EXISTING AND CURRENT)')
       %END;
    %MEND TBLS;
    """
    title1 = "PUBLIC ISLAMIC BANK - (NPL FROM 3 MONTHS & ABOVE) - NEW"
    title2 = (f"MOVEMENTS OF INTEREST IN SUSPENSE FOR THE MONTH ENDING "
              f"{rdate} (EXISTING AND CURRENT)")

    num_vars = ["CURBAL", "UHC", "NETBAL", "IISP", "SUSPEND", "RECOVER",
                "RECC", "IISPW", "IIS", "OIP", "OISUSP", "OIRECV",
                "OIRECC", "OIW", "OI", "TOTIIS"]
    col_labels = {
        "CURBAL":  "CURRENT BAL (A)",
        "UHC":     "UNEARNED HIRING CHARGES (B)",
        "NETBAL":  "NET BAL (A-B=C)",
        "IISP":    "OPENING BAL FOR FINANCIAL YEAR (D)",
        "SUSPEND": "INTEREST SUSPENDED DURING THE PERIOD (E)",
        "RECOVER": "WRITTEN BACK TO PROFIT & LOSS (F)",
        "RECC":    "REVERSAL OF CURRENT YEAR IIS (G)",
        "IISPW":   "WRITTEN OFF (H)",
        "IIS":     "IIS CLOSING BAL (D+E-F-G-H=I)",
        "OIP":     "OPENING BAL FOR FINANCIAL YEAR (J)",
        "OISUSP":  "OI SUSPENDED DURING THE PERIOD (K)",
        "OIRECV":  "WRITTEN BACK TO PROFIT & LOSS (L)",
        "OIRECC":  "REVERSAL OF CURRENT YEAR OI (M)",
        "OIW":     "WRITTEN OFF (N)",
        "OI":      "OI CLOSING BAL (J+K-L-M-N=O)",
        "TOTIIS":  "TOTAL CLOSING BAL AS AT RPT DATE (I+O)",
    }

    # ---- TABLE 1: LOANTYP x RISK x BRANCH (BOX='RISK  BRANCH' RTS=29) ----
    writer.new_page()
    writer.title_block(title1, title2)
    _tabulate_by_risk_branch(loan3, num_vars, col_labels, writer)

    # ---- TABLE 2: LOANTYP x BRANCH (BOX='BRANCH' RTS=9) ----
    writer.new_page()
    writer.title_block(title1, title2)
    _tabulate_by_branch(loan3, num_vars, col_labels, writer)


def _tabulate_by_risk_branch(df: pl.DataFrame, num_vars: list,
                              col_labels: dict, writer: ReportWriter) -> None:
    """
    TABLE LOANTYP=' ',
          RISK=' '*(BRANCH=' ' ALL='SUB-TOTAL') ALL='TOTAL',
          N='NO OF ACCOUNT'*F=COMMA7.  SUM=' '*(...) ...
          / BOX='RISK        BRANCH' RTS=29;
    """
    rts       = 29
    risks     = ["SUBSTANDARD-1", "SUBSTANDARD 2", "DOUBTFUL", "BAD"]
    loan_types = sorted(df["LOANTYP"].drop_nulls().unique().to_list())

    for lt in loan_types:
        lt_df = df.filter(pl.col("LOANTYP") == lt)

        # Column header
        header = (f"{'LOANTYP: ' + lt}")
        writer.write(header, ReportWriter.ASA_DOUBLE)

        col_hdr = (f"{'RISK        BRANCH':<{rts}} {'NO OF ACCOUNT':>13}  " +
                   "  ".join(f"{col_labels[v][:22]:>22}" for v in num_vars))
        writer.write(col_hdr, ReportWriter.ASA_SINGLE)
        writer.write("-" * min(len(col_hdr) + 2, 200), ReportWriter.ASA_SINGLE)

        grand_n    = 0
        grand_sums = {v: 0.0 for v in num_vars}

        for risk in risks:
            r_df     = lt_df.filter(pl.col("RISK") == risk)
            branches = sorted(r_df["BRANCH"].drop_nulls().unique().to_list())
            risk_n   = 0
            risk_sums= {v: 0.0 for v in num_vars}

            for br in branches:
                b_df = r_df.filter(pl.col("BRANCH") == br)
                n    = len(b_df)
                sums = {v: float(b_df[v].fill_null(0).sum()) for v in num_vars}
                lbl  = f"{risk[:14]:<14} {br[:14]:<14}"
                line = (f"{lbl:<{rts}} {format_count(n):>13}  " +
                        "  ".join(format_num(sums[v]) for v in num_vars))
                writer.write(line, ReportWriter.ASA_SINGLE)
                risk_n += n
                for v in num_vars:
                    risk_sums[v] += sums[v]

            # ALL='SUB-TOTAL'
            lbl  = f"{risk[:14]:<14} {'SUB-TOTAL':<14}"
            line = (f"{lbl:<{rts}} {format_count(risk_n):>13}  " +
                    "  ".join(format_num(risk_sums[v]) for v in num_vars))
            writer.write(line, ReportWriter.ASA_DOUBLE)
            grand_n += risk_n
            for v in num_vars:
                grand_sums[v] += risk_sums[v]

        # ALL='TOTAL'
        lbl  = f"{'TOTAL':<{rts}}"
        line = (f"{lbl} {format_count(grand_n):>13}  " +
                "  ".join(format_num(grand_sums[v]) for v in num_vars))
        writer.write(line, ReportWriter.ASA_DOUBLE)
        writer.write("", ReportWriter.ASA_SINGLE)


def _tabulate_by_branch(df: pl.DataFrame, num_vars: list,
                         col_labels: dict, writer: ReportWriter) -> None:
    """
    TABLE LOANTYP=' ', BRANCH=' ' ALL='TOTAL',
          N='NO OF ACCOUNT'*F=COMMA7.  SUM=' '*(...) ...
          / BOX='BRANCH' RTS=9;
    """
    rts        = 9
    loan_types = sorted(df["LOANTYP"].drop_nulls().unique().to_list())

    for lt in loan_types:
        lt_df = df.filter(pl.col("LOANTYP") == lt)

        header = f"{'LOANTYP: ' + lt}"
        writer.write(header, ReportWriter.ASA_DOUBLE)

        col_hdr = (f"{'BRANCH':<{rts}} {'NO OF ACCOUNT':>13}  " +
                   "  ".join(f"{col_labels[v][:22]:>22}" for v in num_vars))
        writer.write(col_hdr, ReportWriter.ASA_SINGLE)
        writer.write("-" * min(len(col_hdr) + 2, 200), ReportWriter.ASA_SINGLE)

        branches   = sorted(lt_df["BRANCH"].drop_nulls().unique().to_list())
        total_n    = 0
        total_sums = {v: 0.0 for v in num_vars}

        for br in branches:
            b_df = lt_df.filter(pl.col("BRANCH") == br)
            n    = len(b_df)
            sums = {v: float(b_df[v].fill_null(0).sum()) for v in num_vars}
            lbl  = f"{br[:9]:<{rts}}"
            line = (f"{lbl} {format_count(n):>13}  " +
                    "  ".join(format_num(sums[v]) for v in num_vars))
            writer.write(line, ReportWriter.ASA_SINGLE)
            total_n += n
            for v in num_vars:
                total_sums[v] += sums[v]

        # ALL='TOTAL'
        lbl  = f"{'TOTAL':<{rts}}"
        line = (f"{lbl} {format_count(total_n):>13}  " +
                "  ".join(format_num(total_sums[v]) for v in num_vars))
        writer.write(line, ReportWriter.ASA_DOUBLE)
        writer.write("", ReportWriter.ASA_SINGLE)


# =============================================================================
# %MACRO DTLS — PROC PRINT detail report (I=3, i.e. LOAN3 only)
# /* DISCONTINUE AS PER LETTER DATED 26/08/03 FR STATISTICS */
# =============================================================================

def produce_detail_report(loan3: pl.DataFrame, rdate: str,
                           writer: ReportWriter) -> None:
    """
    %MACRO DTLS;
       %DO I = 3 %TO 3;
          PROC SORT DATA=LOAN&I; BY LOANTYP BRANCH RISK DAYS ACCTNO;
          PROC PRINT LABEL N;
             BY LOANTYP BRANCH RISK; PAGEBY BRANCH; SUMBY RISK;
          TITLE1 'PUBLIC ISLAMIC BANK - (NPL FROM 3 MONTHS & ABOVE) - NEW';
          TITLE2 &TTL &RDATE &&TBL&I;
       %END;
    %MEND DTLS;
    /* DISCONTINUE AS PER LETTER DATED 26/08/03 FR STATISTICS */
    """
    title1 = "PUBLIC ISLAMIC BANK - (NPL FROM 3 MONTHS & ABOVE) - NEW"
    title2 = (f"MOVEMENTS OF INTEREST IN SUSPENSE FOR THE MONTH ENDING "
              f"{rdate} (EXISTING AND CURRENT)")

    num_vars = ["NETPROC", "CURBAL", "UHC", "NETBAL", "IISP", "SUSPEND",
                "RECOVER", "RECC", "IISPW", "IIS", "OIP", "OISUSP",
                "OIRECV", "OIRECC", "OIW", "OI", "TOTIIS"]
    labels = {
        "ACCTNO":  "MNI ACCOUNT NO",
        "DAYS":    "NO OF DAYS PAST DUE",
        "BORSTAT": "BORROWER'S STATUS",
        "NETPROC": "LIMIT",
        "CURBAL":  "CURRENT BAL (A)",
        "UHC":     "UNEARNED HIRING CHARGES (B)",
        "NETBAL":  "NET BAL (A-B=C)",
        "IISP":    "OPENING BAL FOR FINANCIAL YEAR (D)",
        "SUSPEND": "INTEREST SUSPENDED DURING THE PERIOD (E)",
        "RECOVER": "WRITTEN BACK TO PROFIT & LOSS (F)",
        "RECC":    "REVERSAL OF CURRENT YEAR IIS (G)",
        "IISPW":   "WRITTEN OFF (H)",
        "IIS":     "IIS CLOSING BAL (D+E-F-G-H=I)",
        "OIP":     "OPENING BAL FOR FINANCIAL YEAR (J)",
        "OISUSP":  "OI SUSPENDED DURING THE PERIOD (K)",
        "OIRECV":  "WRITTEN BACK TO PROFIT & LOSS (L)",
        "OIRECC":  "REVERSAL OF CURRENT YEAR OI (M)",
        "OIW":     "WRITTEN OFF (N)",
        "OI":      "OI CLOSING BAL (J+K-L-M-N=O)",
        "TOTIIS":  "TOTAL CLOSING BAL AS AT RPT DATE (I+O)",
    }

    # PROC SORT BY LOANTYP BRANCH RISK DAYS ACCTNO
    sorted_df = loan3.sort(["LOANTYP", "BRANCH", "RISK", "DAYS", "ACCTNO"])

    prev_loantyp = None
    prev_branch  = None
    prev_risk    = None
    risk_sums    = {v: 0.0 for v in num_vars}
    risk_n       = 0
    total_n      = 0
    total_sums   = {v: 0.0 for v in num_vars}

    col_hdr = (f"{'MNI ACCOUNT NO':<20} {'NAME':<30} "
               f"{'NO OF DAYS PAST DUE':>20} {'STAT':<5}  " +
               "  ".join(f"{labels[v][:15]:>15}" for v in num_vars))

    def emit_risk_sum(risk_label_str: str) -> None:
        sumline = (f"{'SUMBY RISK: ' + risk_label_str:<55}  " +
                   "  ".join(format_num(risk_sums[v]) for v in num_vars))
        writer.write(sumline, ReportWriter.ASA_DOUBLE)

    for row in sorted_df.to_dicts():
        loantyp = row.get("LOANTYP", "") or ""
        branch  = row.get("BRANCH",  "") or ""
        risk    = row.get("RISK",    "") or ""

        # PAGEBY BRANCH — new page when BRANCH changes (within LOANTYP)
        if branch != prev_branch or loantyp != prev_loantyp:
            if prev_risk is not None:
                emit_risk_sum(prev_risk)
                risk_sums = {v: 0.0 for v in num_vars}; risk_n = 0
            writer.new_page()
            writer.title_block(title1, title2)
            writer.write(f"LOANTYP={loantyp}  BRANCH={branch}", ReportWriter.ASA_SINGLE)
            writer.write(col_hdr, ReportWriter.ASA_SINGLE)
            writer.write("-" * min(len(col_hdr) + 2, 200), ReportWriter.ASA_SINGLE)
            prev_risk = None

        # SUMBY RISK — print risk subtotal before switching risk group
        elif risk != prev_risk and prev_risk is not None:
            emit_risk_sum(prev_risk)
            risk_sums = {v: 0.0 for v in num_vars}; risk_n = 0

        acctno = str(row.get("ACCTNO", "") or "")
        name   = (str(row.get("NAME",   "") or ""))[:30]
        days   = row.get("DAYS",    0) or 0
        bstat  = row.get("BORSTAT", "") or ""
        line   = (f"{acctno:<20} {name:<30} {days:>20} {bstat:<5}  " +
                  "  ".join(format_num(row.get(v, 0) or 0) for v in num_vars))
        writer.write(line, ReportWriter.ASA_SINGLE)

        for v in num_vars:
            val = row.get(v, 0) or 0
            risk_sums[v]  += val
            total_sums[v] += val
        risk_n  += 1
        total_n += 1

        prev_loantyp = loantyp
        prev_branch  = branch
        prev_risk    = risk

    # Final risk subtotal
    if prev_risk is not None:
        emit_risk_sum(prev_risk)

    # Grand total (N = total count)
    totline = (f"{'TOTAL':<55}  " +
               "  ".join(format_num(total_sums[v]) for v in num_vars))
    writer.write(totline, ReportWriter.ASA_DOUBLE)
    writer.write(f"N = {total_n}", ReportWriter.ASA_SINGLE)


# =============================================================================
# MAIN
# =============================================================================

def main():
    # DATA REPTDATE — load report date and macro variables
    reptdate_df, macro_vars = load_reptdate()
    reptdate_val: date = macro_vars["REPTDATE"]
    reptmon            = macro_vars["REPTMON"]
    prevmon            = macro_vars["PREVMON"]
    rdate              = macro_vars["RDATE"]

    # DATA LOANWOFF — merge LOAN with WIIS
    loanwoff = build_loanwoff(reptmon)

    # DATA LOAN1 — existing NPL accounts
    loan1 = calc_loan1(loanwoff, reptdate_val)

    # DATA LOAN2 — current NPL accounts
    loan2 = calc_loan2(loanwoff, reptdate_val)

    # %MONTHLY — compare with previous month
    loan1, loan2 = apply_monthly(loan1, loan2, reptmon, prevmon, reptdate_val)

    # DATA LOAN3 — combine, assign RISK, filter COSTCTR
    loan3 = build_loan3(loan1, loan2)

    # Save output datasets (NPL.IIS&REPTMON and NPL.IIS)
    iis_out = str(IIS_OUT_TMPL).format(reptmon=reptmon)
    loan3.write_parquet(iis_out)
    loan3.write_parquet(str(IIS_LATEST))

    # OPTIONS NOCENTER NODATE NONUMBER MISSING=0
    writer = ReportWriter(REPORT_FILE)

    # %TBLS (I=3)
    produce_tabulate_report(loan3, rdate, writer)

    # /* DISCONTINUE AS PER LETTER DATED 26/08/03 FR STATISTICS */
    # %DTLS (I=3)
    produce_detail_report(loan3, rdate, writer)

    writer.flush()
    print(f"Report written to : {REPORT_FILE}")
    print(f"IIS dataset saved : {iis_out}")
    print(f"IIS latest saved  : {IIS_LATEST}")


if __name__ == "__main__":
    main()
