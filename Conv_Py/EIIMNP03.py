# !/usr/bin/env python3
"""
 PROGRAM : EIFMNP03 (JCL: EIIMNP03)
 DATE    : 12.03.98
 MODIFY  : ESMR 2004-720, 2004-579, 2006-1048
 REPORT  : MOVEMENTS OF INTEREST IN SUSPENSE FOR THE MONTH ENDING
"""

import duckdb
import polars as pl
import math
from datetime import date, datetime
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
# FORMAT DEFINITIONS
# =============================================================================
# %INC PGM(PBBLNFMT);  -- placeholder for PBBLNFMT format definitions
# %INC PGM(PBBELF);    -- placeholder for PBBELF format definitions

# from PBBLNFMT import format_oddenom, format_lndenom, format_lnprod, format_odcustcd, format_locustcd,
#                      format_lncustcd, format_statecd, format_apprlimt, format_loansize, format_mthpass,
#                      format_lnormt, format_lnrmmt, format_collcd, format_riskcd,format_busind, is_more_plan,
#                      is_hire_purchase, is_islamic_product, is_fcy_product
# from PBBELF import lntyp_format,

# BRCHCD format: maps NTBRCH numeric code to branch string abbreviation
# This is defined in PBBLNFMT include. Placeholder mapping below.
BRCHCD_MAP: dict[int, str] = {
    # e.g. 1: 'KLM', 34: 'PJN', ...
    # Populate from PBBLNFMT definitions
}


def brchcd_format(ntbrch: int) -> str:
    """Equivalent to PUT(NTBRCH, BRCHCD.) SAS format."""
    return BRCHCD_MAP.get(ntbrch, str(ntbrch))


def branch_label(ntbrch: int) -> str:
    """Equivalent to PUT(NTBRCH,BRCHCD.)||' '||PUT(NTBRCH,Z3.)"""
    return f"{brchcd_format(ntbrch)} {ntbrch:03d}"


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


def risk_label(days: int, borstat: str, user5: str) -> str:
    """Assign RISK classification."""
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


# =============================================================================
# MACRO DCLVAR / NXTBLDT helpers
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
    # Leap year check for February
    lday = dict(LDAY)
    if mm == 2:
        lday[2] = 29 if yy % 4 == 0 else 28
    if dd > lday[mm]:
        dd = lday[mm]
    return date(yy, mm, dd)


# =============================================================================
# STEP 1: READ REPTDATE & DERIVE MACRO VARIABLES
# =============================================================================
def load_reptdate() -> tuple[pl.DataFrame, dict]:
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM '{REPTDATE_FILE}'").pl()
    row = df.row(0, named=True)
    reptdate_val: date = row["REPTDATE"]
    reptmon = reptdate_val.month
    prevmon = 12 if reptmon == 1 else reptmon - 1
    rdate_str = reptdate_val.strftime("%d %B %Y").upper()
    macro_vars = {
        "RDATE": rdate_str,
        "REPTMON": f"{reptmon:02d}",
        "PREVMON": f"{prevmon:02d}",
        "REPTDATE": reptdate_val,
    }
    con.close()
    return df, macro_vars


# =============================================================================
# STEP 2: MERGE LOAN WITH WIIS (written-off accounts)
# =============================================================================
def build_loanwoff(reptmon: str, reptdate_df: pl.DataFrame) -> pl.DataFrame:
    """
    PROC SORT DATA=NPL.LOAN&REPTMON; BY ACCTNO;
    PROC SORT DATA=NPL.WIIS; BY ACCTNO;
    DATA LOANWOFF; MERGE NPL.LOAN&REPTMON NPL.WIIS (IN=AA DROP=NOTENO NTBRCH); BY ACCTNO;
    """
    loan_file = str(LOAN_FILE_TMPL).format(reptmon=reptmon)
    wiis_file = str(WIIS_FILE)
    con = duckdb.connect()
    # Left-join loan with wiis; flag AA = account in WIIS
    loanwoff = con.execute(f"""
        SELECT l.*,
               CASE WHEN w.ACCTNO IS NOT NULL THEN 'Y' ELSE 'N' END AS WRITEOFF,
               w.* EXCLUDE (ACCTNO, NOTENO, NTBRCH)
        FROM '{loan_file}' l
        LEFT JOIN '{wiis_file}' w ON l.ACCTNO = w.ACCTNO
        ORDER BY l.ACCTNO
    """).pl()
    con.close()

    # Post-merge transformations
    def fix_row(row: dict) -> dict:
        if row.get("LOANTYPE") in (380, 381):
            row["FEEAMT"] = row.get("FEETOT2", 0) or 0
        if row.get("WRITEOFF") == "Y":
            pass  # WRITEOFF already set
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
        return row

    records = [fix_row(r) for r in loanwoff.to_dicts()]
    return pl.from_dicts(records, infer_schema_length=None)


# =============================================================================
# COMPUTE IIS LOOP (SUM over REMMTH range)
# =============================================================================
def iis_sum(remmth1: int, remmth2: int, termchg: float,
            earnterm: int) -> float:
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
# STEP 3a: CALCULATE IIS FOR EXISTING NPL ACCOUNTS (LOAN1)
# =============================================================================
def calc_loan1(loanwoff: pl.DataFrame, reptdate_val: date) -> pl.DataFrame:
    """
    DATA LOAN1 -- Existing NPL (EXIST='Y')
    """
    stmth = 1
    styr = reptdate_val.year

    out_rows = []
    # filter EXIST = 'Y'
    df = loanwoff.filter(pl.col("EXIST") == "Y")

    for row in df.to_dicts():
        acctno    = row.get("ACCTNO")
        noteno    = row.get("NOTENO")
        name      = row.get("NAME", "")
        ntbrch    = row.get("NTBRCH", 0)
        netproc   = row.get("NETPROC", 0) or 0
        curbal    = row.get("CURBAL") or 0
        borstat   = row.get("BORSTAT", "") or ""
        days      = row.get("DAYS", 0) or 0
        loantype  = row.get("LOANTYPE", 0) or 0
        issdte    = row.get("ISSDTE")
        bldate    = row.get("BLDATE")
        termchg   = row.get("TERMCHG", 0) or 0
        earnterm  = row.get("EARNTERM", 0) or 0
        noteterm  = row.get("NOTETERM", 0) or 0
        user5     = row.get("USER5", "") or ""
        writeoff  = row.get("WRITEOFF", "N") or "N"
        wdownind  = row.get("WDOWNIND", "") or ""
        rescheind = row.get("RESCHEIND", "") or ""
        accrual   = row.get("ACCRUAL", 0) or 0
        costctr   = row.get("COSTCTR", 0) or 0
        pendbrh   = row.get("PENDBRH", 0) or 0

        iisp   = row.get("IISP", 0) or 0
        oip    = row.get("OIP", 0) or 0
        iispw  = row.get("IISPW", 0) or 0
        feetot2 = row.get("FEETOT2", 0) or 0
        feeamta = row.get("FEEAMTA", 0) or 0
        feeamt5 = row.get("FEEAMT5", 0) or 0
        feeamt  = row.get("FEEAMT", 0) or 0
        marketvl = row.get("MARKETVL", 0) or 0

        # Written-off adjustment
        if writeoff == "Y" and wdownind != "Y":
            borstat = "W"

        iis = 0.0; suspend = 0.0; uhc = 0.0; oi = 0.0
        oisusp = 0.0; recover = 0.0; oirecv = 0.0
        oirecc = 0.0; oiw = 0.0; recc = 0.0

        if bldate and bldate > date(1900, 1, 1) and termchg > 0:
            if (days > 89 or borstat in ("F", "R", "I")
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
                oi = (feetot2 or 0) + (-1) * (feeamta or 0) + (feeamt5 or 0)
                suspend = iis_sum(remmths, remmth2, termchg, earnterm)
                if loantype not in (128, 130):
                    oisusp = (feeamt or 0) + (-1) * (feeamta or 0) + (feeamt5 or 0)
                if remmth2 > 0:
                    uhc = uhc_val(remmth2, termchg, earnterm)
        elif (days > 89 or borstat in ("F", "R", "I")
              or (user5 == "N" and loantype not in (983, 993))):
            oi = (feetot2 or 0) + (-1) * (feeamta or 0) + (feeamt5 or 0)
            oisusp = (feeamt or 0) + (-1) * (feeamta or 0) + (feeamt5 or 0)

        curbal = curbal or 0
        netbal = curbal - uhc
        if netbal <= iisp:
            if (days > 89 or borstat in ("F", "R", "I") or user5 == "N"):
                iis = netbal

        if borstat == "W":
            iispw = iisp
            oiw = oip
        else:
            recover = iisp + suspend - iis
            if recover < 0:
                suspend = suspend - recover
                recover = 0.0
            if recover > iisp:
                recc = recover - iisp
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

        # TERMCHG = 0 logic
        if termchg == 0:
            if borstat == "R":
                netexp = curbal - iisp - marketvl
            else:
                netexp = curbal - iisp
            if (netexp > 0 and days > 89) or borstat == "R":
                iis = recover; recover = 0.0
                oi = (feetot2 or 0) + (-1) * (feeamta or 0) + (feeamt5 or 0)
                oirecv = 0.0

        if loantype in (131, 132):
            iis = accrual

        oisusp = oirecv + oirecc + oiw - oip + oi
        if oisusp < 0:
            oirecv = oirecv - oisusp
        if oirecv > oip:
            oirecc = oirecv - oip
            oirecv = oip
        oisusp = oirecv + oirecc + oiw - oip + oi

        branch = branch_label(ntbrch)
        loantyp = lntyp_format(loantype)

        # Written-off overrides
        if writeoff == "Y":
            suspend = row.get("WSUSPEND", 0) or 0
            oisusp  = row.get("WOISUSP", 0) or 0
            if wdownind != "Y":
                recover = row.get("WRECOVER", 0) or 0
                recc    = row.get("WRECC", 0) or 0
                oirecv  = row.get("WOIRECV", 0) or 0
                oirecc  = row.get("WOIRECC", 0) or 0
                iis = 0.0
                iispw = (iisp or 0) + (suspend or 0) - (recover or 0) - (recc or 0)
                oi  = 0.0
                oiw = (oip or 0) + (oisusp or 0) - (oirecv or 0) - (oirecc or 0)
            else:
                oisusp = row.get("WOISUSP", 0) or 0
                iispw  = row.get("WIISPW", 0) or 0
                iis    = (iisp or 0) + (suspend or 0) - (recover or 0) - (recc or 0) - (iispw or 0)
                if iis < 0:
                    recover = 0.0
                iis    = (iisp or 0) + (suspend or 0) - (recover or 0) - (recc or 0) - (iispw or 0)
                oiw    = row.get("WOIW", 0) or 0
                oi     = (oip or 0) + (oisusp or 0) - (oirecv or 0) - (oirecc or 0) - (oiw or 0)
                if oi < 0:
                    oirecv = 0.0; oirecc = 0.0
                oi     = (oip or 0) + (oisusp or 0) - (oirecv or 0) - (oirecc or 0) - (oiw or 0)

            # Null safety
            for attr, default in [("OIP", 0), ("IISP", 0), ("SUSPEND", 0),
                                   ("OISUSP", 0), ("RECOVER", 0), ("OIRECV", 0),
                                   ("RECC", 0), ("OIRECC", 0)]:
                pass  # already handled above

        totiis = iis + oi

        # RESCHEIND override
        if rescheind == "Y":
            suspend = row.get("WSUSPEND", 0) or 0
            oisusp  = row.get("WOISUSP", 0) or 0
            recover = row.get("WRECOVER", 0) or 0
            recc    = row.get("WRECC", 0) or 0
            oirecv  = row.get("WOIRECV", 0) or 0
            oirecc  = row.get("WOIRECC", 0) or 0
            iis     = (iisp or 0) + (suspend or 0) - (recover or 0) - (recc or 0) - (iispw or 0)
            oi      = (oip or 0) + (oisusp or 0) - (oirecv or 0) - (oirecc or 0) - (oiw or 0)
            totiis  = iis + oi

        out_rows.append({
            "BRANCH": branch, "NTBRCH": ntbrch, "ACCTNO": acctno,
            "NOTENO": noteno, "NAME": name, "NETPROC": netproc,
            "CURBAL": curbal, "BORSTAT": borstat, "DAYS": days,
            "IIS": iis, "UHC": uhc, "NETBAL": netbal,
            "IISP": iisp, "SUSPEND": suspend, "RECOVER": recover,
            "RECC": recc, "IISPW": iispw,
            "OIP": oip, "OISUSP": oisusp, "OI": oi,
            "OIRECV": oirecv, "OIRECC": oirecc, "OIW": oiw,
            "TOTIIS": totiis, "LOANTYP": loantyp, "EXIST": row.get("EXIST", ""),
            "COSTCTR": costctr, "PENDBRH": pendbrh, "USER5": user5,
            "WDOWNIND": wdownind, "RESCHEIND": rescheind, "ACCRUAL": accrual,
            "LOANTYPE": loantype,
        })

    return pl.from_dicts(out_rows, infer_schema_length=None) if out_rows else pl.DataFrame()


# =============================================================================
# STEP 3b: CALCULATE IIS FOR CURRENT NPL ACCOUNTS (LOAN2)
# =============================================================================
def calc_loan2(loanwoff: pl.DataFrame, reptdate_val: date) -> pl.DataFrame:
    """
    DATA LOAN2 -- Current NPL (EXIST != 'Y')
    """
    df = loanwoff.filter(pl.col("EXIST") != "Y")
    out_rows = []

    for row in df.to_dicts():
        acctno    = row.get("ACCTNO")
        noteno    = row.get("NOTENO")
        name      = row.get("NAME", "")
        ntbrch    = row.get("NTBRCH", 0)
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
        oiw       = 0.0; recc = 0.0; oirecc = 0.0; recover = 0.0

        if writeoff == "Y" and wdownind != "Y":
            borstat = "W"

        iis = 0.0; uhc = 0.0; oi = 0.0

        if (bldate and bldate > date(1900, 1, 1) and termchg > 0) or \
           (user5 == "N" and loantype not in (983, 993)):
            remmth1 = (earnterm
                       - ((bldate.year - issdte.year) * 12
                          + bldate.month - issdte.month + 1)) if bldate and issdte else 0
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

        oi = (feetot2 or 0) + (-1) * (feeamta or 0) + (feeamt5 or 0)

        if loantype in (131, 132):
            iis = accrual

        suspend = iis
        oisusp  = oi
        netbal  = curbal - uhc

        if writeoff == "Y":
            suspend = row.get("WSUSPEND", 0) or 0
            oisusp  = row.get("WOISUSP", 0) or 0
            if wdownind != "Y":
                recover = row.get("WRECOVER", 0) or 0
                recc    = row.get("WRECC", 0) or 0
                oirecv  = row.get("WOIRECV", 0) or 0
                oirecc  = row.get("WOIRECC", 0) or 0
                iis  = 0.0
                iispw = (iisp or 0) + (suspend or 0) - (recover or 0) - (recc or 0)
                oi   = 0.0
                oiw  = (oip or 0) + (oisusp or 0) - (oirecv or 0) - (oirecc or 0)
            else:
                oisusp = row.get("WOISUSP", 0) or 0
                iispw  = row.get("WIISPW", 0) or 0
                iis    = (iisp or 0) + (suspend or 0) - (recover or 0) - (recc or 0) - (iispw or 0)
                if iis < 0:
                    recover = 0.0
                iis    = (iisp or 0) + (suspend or 0) - (recover or 0) - (recc or 0) - (iispw or 0)
                oiw    = row.get("WOIW", 0) or 0
                oi     = (oip or 0) + (oisusp or 0) - (oirecv or 0) - (oirecc or 0) - (oiw or 0)
                if oi < 0:
                    oirecv = 0.0; oirecc = 0.0
                oi     = (oip or 0) + (oisusp or 0) - (oirecv or 0) - (oirecc or 0) - (oiw or 0)
        else:
            oirecv = 0.0

        totiis = iis + oi
        branch = branch_label(ntbrch)
        loantyp = lntyp_format(loantype)

        if rescheind == "Y":
            suspend = row.get("WSUSPEND", 0) or 0
            oisusp  = row.get("WOISUSP", 0) or 0
            recover = row.get("WRECOVER", 0) or 0
            recc    = row.get("WRECC", 0) or 0
            oirecv  = row.get("WOIRECV", 0) or 0
            oirecc  = row.get("WOIRECC", 0) or 0
            iis     = (iisp or 0) + (suspend or 0) - (recover or 0) - (recc or 0) - (iispw or 0)
            oi      = (oip or 0) + (oisusp or 0) - (oirecv or 0) - (oirecc or 0) - (oiw or 0)
            totiis  = iis + oi

        out_rows.append({
            "BRANCH": branch, "NTBRCH": ntbrch, "ACCTNO": acctno,
            "NOTENO": noteno, "NAME": name, "NETPROC": netproc,
            "CURBAL": curbal, "BORSTAT": borstat, "DAYS": days,
            "IIS": iis, "UHC": uhc, "NETBAL": netbal,
            "IISP": iisp, "SUSPEND": suspend, "RECOVER": recover,
            "RECC": recc, "IISPW": iispw,
            "OIP": oip, "OISUSP": oisusp, "OI": oi,
            "OIRECV": oirecv, "OIRECC": oirecc, "OIW": oiw,
            "TOTIIS": totiis, "LOANTYP": loantyp, "EXIST": row.get("EXIST", ""),
            "COSTCTR": costctr, "PENDBRH": pendbrh, "USER5": user5,
            "WDOWNIND": wdownind, "RESCHEIND": rescheind, "ACCRUAL": accrual,
            "LOANTYPE": loantype,
        })

    return pl.from_dicts(out_rows, infer_schema_length=None) if out_rows else pl.DataFrame()


# =============================================================================
# STEP 4: COMPARE PREVIOUS MONTH NPL (%MACRO MONTHLY)
# =============================================================================
def apply_monthly(loan1: pl.DataFrame, loan2: pl.DataFrame,
                  reptmon: str, prevmon: str,
                  reptdate_val: date) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    %MACRO MONTHLY
    If REPTMON = '01': zero out cumulative fields.
    Else: merge with previous month IIS data.
    """
    # %INC PGM(NPLNTB);  -- placeholder for NPLNTB program logic

    if reptmon == "01":
        # DATA LOAN1; SET LOAN1; IISPCUM=0; OIPCUM=0; POI=0;
        for col in ("IISPCUM", "OIPCUM", "POI"):
            if col not in loan1.columns:
                loan1 = loan1.with_columns(pl.lit(0.0).alias(col))
            else:
                loan1 = loan1.with_columns(pl.lit(0.0).alias(col))
        for col in ("IISPCUM", "OIPCUM", "POI"):
            if col not in loan2.columns:
                loan2 = loan2.with_columns(pl.lit(0.0).alias(col))
            else:
                loan2 = loan2.with_columns(pl.lit(0.0).alias(col))
        return loan1, loan2

    # ELSE: load previous month IIS
    iis_prev_file = str(IIS_PREV_TMPL).format(prevmon=prevmon)
    ploan_file    = str(PLOAN_TMPL).format(reptmon=reptmon)
    con = duckdb.connect()

    # PROC SORT DATA=NPL.IIS&PREVMON (DROP=POI RENAME=(...)) OUT=IISPREV NODUPKEY
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

    # DATA IISPREV: %INC PGM(NPLNTB) conditional + null-fill
    # %INC PGM(NPLNTB);  -- placeholder: applies payment adjustments for qualified loan types
    npl_types = (128, 130, 131, 132, 380, 381, 390, 700, 705, 720, 725, 983, 993, 996)

    def process_iisprev_row(r: dict) -> dict:
        """Apply NPLNTB logic placeholder and null-fills."""
        # NPLNTB adjustments applied when loantype in npl_types and PAIDIND='P'
        # %INC PGM(NPLNTB); -- placeholder
        if r.get("LOANTYPE") in npl_types and r.get("PAIDIND") == "P":
            pass  # NPLNTB logic would execute here
        r["BRANCH"] = branch_label(r.get("NTBRCH", 0))
        for f in ("PDAYS", "PSUSPEND", "POISUSP", "PIISP", "POIP", "POI"):
            if r.get(f) is None:
                r[f] = 0.0
        return r

    iisprev_rows = [process_iisprev_row(r) for r in iisprev_raw.to_dicts()]
    iisprev = pl.from_dicts(iisprev_rows, infer_schema_length=None) if iisprev_rows else pl.DataFrame()

    # *** EXISTING NPL ***
    loan1 = _monthly_loan1(loan1, iisprev)
    # *** CURRENT NPL ***
    loan2 = _monthly_loan2(loan2, iisprev, ploan_file, reptmon)

    return loan1, loan2


def _monthly_loan1(loan1: pl.DataFrame, iisprev: pl.DataFrame) -> pl.DataFrame:
    """
    DATA LOAN1 -- merge with iisprev for existing NPL monthly comparison.
    """
    # Merge iisprev (B) with loan1 (A) by ACCTNO
    iisprev_sel = iisprev.select(["ACCTNO", "PDAYS", "PSUSPEND", "POISUSP",
                                   "PIISP", "POIP", "POI", "PRECC", "POIRECC",
                                   "PRECOVER", "POIRECV"])
    merged = loan1.join(iisprev_sel, on="ACCTNO", how="left")

    # Filter: (A AND B) OR (B AND NOT A) AND EXIST='Y'
    # Since we start from loan1 (which is all A), we keep rows that match B
    # and those from B not in A are handled via full outer join
    iisprev_accts = set(iisprev_sel["ACCTNO"].to_list())

    out_rows = []
    drop_cols = {"PDAYS", "PSUSPEND", "POISUSP", "PIISP", "POIP",
                 "PRECC", "POIRECC", "PRECOVER", "POIRECV"}

    for row in merged.to_dicts():
        acctno   = row.get("ACCTNO")
        in_a     = True  # row is from loan1
        in_b     = acctno in iisprev_accts
        exist    = row.get("EXIST", "")

        if not ((in_a and in_b) or (in_b and not in_a)) or exist != "Y":
            continue

        borstat  = row.get("BORSTAT", "") or ""
        curbal   = row.get("CURBAL", 0) or 0
        days     = row.get("DAYS", 0) or 0
        rescheind = row.get("RESCHEIND", "") or ""
        user5    = row.get("USER5", "") or ""
        iisp     = row.get("IISP", 0) or 0
        oip      = row.get("OIP", 0) or 0
        iispw    = row.get("IISPW", 0) or 0
        oiw      = row.get("OIW", 0) or 0
        iis      = row.get("IIS", 0) or 0
        oi       = row.get("OI", 0) or 0
        suspend  = row.get("SUSPEND", 0) or 0
        oisusp   = row.get("OISUSP", 0) or 0
        recover  = row.get("RECOVER", 0) or 0
        recc     = row.get("RECC", 0) or 0
        oirecv   = row.get("OIRECV", 0) or 0
        oirecc   = row.get("OIRECC", 0) or 0
        netbal   = row.get("NETBAL", 0) or 0

        pdays    = row.get("PDAYS", 0) or 0
        psuspend = row.get("PSUSPEND", 0) or 0
        poisusp  = row.get("POISUSP", 0) or 0
        piisp    = row.get("PIISP", 0) or 0
        poip     = row.get("POIP", 0) or 0
        poi      = row.get("POI", 0) or 0
        precc    = row.get("PRECC", 0) or 0
        poirecc  = row.get("POIRECC", 0) or 0
        precover = row.get("PRECOVER", 0) or 0
        poirecv  = row.get("POIRECV", 0) or 0

        # *** A/C SETTLE FOR EXISTING NPL ***
        if (not in_a or (curbal <= 0 and poi <= 0)) and \
                borstat not in ("F", "I", "R", "W", "S"):
            iisp    = piisp; recover = iisp; suspend = psuspend; recc = suspend
            oip     = poip;  oirecv  = oip;  oisusp  = poisusp;  oirecc = oisusp
            curbal  = 0; netbal = 0; days = 0
            oi     = (oip + oisusp - oirecv - oirecc - oiw)
            iis    = (iisp + suspend - recover - recc - iispw)
            totiis = iis + oi
            row.update({"IISP": iisp, "RECOVER": recover, "SUSPEND": suspend,
                        "RECC": recc, "OIP": oip, "OIRECV": oirecv,
                        "OISUSP": oisusp, "OIRECC": oirecc, "CURBAL": curbal,
                        "NETBAL": netbal, "DAYS": days, "OI": oi,
                        "IIS": iis, "TOTIIS": totiis})
            out_rows.append({k: v for k, v in row.items() if k not in drop_cols})
            continue

        if borstat == "W" or rescheind == "Y":
            out_rows.append({k: v for k, v in row.items() if k not in drop_cols})
            continue

        if in_a and in_b:
            # *** CONTINUE PERFORMING ***
            if days < 90 and pdays < 90:
                if user5 == "N":
                    if iis < iisp:
                        suspend = 0; recover = iisp - iis; recc = 0
                    if iis >= iisp:
                        suspend = iis - iisp; recover = 0; recc = 0
                    if iisp == 0:
                        suspend = iis; recc = iis - suspend
                    if oi < oip:
                        oisusp = 0; oirecv = oip - oi; oirecc = 0
                    if oi >= oip:
                        oisusp = oi - oip; oirecv = 0; oirecc = 0
                    if oip == 0:
                        oisusp = oi; oirecc = oi - oisusp
                row.update({"SUSPEND": suspend, "RECOVER": recover, "RECC": recc,
                            "OISUSP": oisusp, "OIRECV": oirecv, "OIRECC": oirecc})
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

            # *** TURN PERFORMING ***
            if days < 90 and pdays >= 90:
                if borstat not in ("F", "I", "R"):
                    suspend = psuspend; recc = psuspend
                    oisusp  = poisusp;  oirecc = poisusp
                if user5 == "N":
                    if iis < iisp:
                        suspend = 0; recover = iisp - iis; recc = 0
                    if iis >= iisp:
                        suspend = iis - iisp; recover = 0; recc = 0
                    if iisp == 0:
                        suspend = iis; recc = iis - suspend
                    if oi < oip:
                        oisusp = 0; oirecv = oip - oi; oirecc = 0
                    if oi >= oip:
                        oisusp = oi - oip; oirecv = 0; oirecc = 0
                    if oip == 0:
                        oisusp = oi; oirecc = oi - oisusp
                row.update({"SUSPEND": suspend, "RECOVER": recover, "RECC": recc,
                            "OISUSP": oisusp, "OIRECV": oirecv, "OIRECC": oirecc,
                            "TOTIIS": iis + oi})
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

            # *** TURN NPL FR PERFORMING ***
            if days >= 90 and pdays < 90:
                if borstat not in ("F", "I", "R"):
                    recc    = precc; recover = precover
                    suspend = iis + iisp - recover + recc
                    if suspend < 0:
                        recover = recover - suspend; suspend = 0
                        if recover > iisp:
                            recc = recc + recover - iisp
                    oirecc  = poirecc; oirecv = poirecv
                    oisusp  = oi + oip - oirecv + oirecc
                    if oisusp < 0:
                        oirecv = oirecv - oisusp; oisusp = 0
                        if oirecv > oip:
                            oirecc = oirecc + oirecv - oip
                if user5 == "N":
                    if iis < iisp:
                        suspend = 0; recover = iisp - iis; recc = 0
                    if iis >= iisp:
                        suspend = iis - iisp; recover = 0; recc = 0
                    if iisp == 0:
                        suspend = iis; recc = iis - suspend
                    if oi < oip:
                        oisusp = 0; oirecv = oip - oi; oirecc = 0
                    if oi >= oip:
                        oisusp = oi - oip; oirecv = 0; oirecc = 0
                    if oip == 0:
                        oisusp = oi; oirecc = oi - oisusp
                row.update({"SUSPEND": suspend, "RECOVER": recover, "RECC": recc,
                            "OISUSP": oisusp, "OIRECV": oirecv, "OIRECC": oirecc,
                            "TOTIIS": iis + oi})
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

            # *** CONTINUE NPL ***
            if days >= 90 and pdays >= 90:
                if borstat not in ("F", "I", "R"):
                    recover = precover; recc = precc
                    suspend = iis - iisp + recover + recc
                    if suspend < 0:
                        recover = recover - suspend; suspend = 0
                        if recover > iisp:
                            recc = recc + recover - iisp
                    oirecv  = poirecv; oirecc = poirecc
                    oisusp  = oi - oip + oirecv + oirecc
                    if oisusp < 0:
                        oirecv = oirecv - oisusp; oisusp = 0
                        if oirecv > oip:
                            oirecc = oirecc + oirecv - oip
                row.update({"SUSPEND": suspend, "RECOVER": recover, "RECC": recc,
                            "OISUSP": oisusp, "OIRECV": oirecv, "OIRECC": oirecc,
                            "TOTIIS": iis + oi})
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

            # ***SPECIAL USER5=N***
            if user5 == "N" and iis == 0 and iisp == 0:
                row["SUSPEND"] = iis

    return pl.from_dicts(out_rows, infer_schema_length=None) if out_rows else pl.DataFrame()


def _monthly_loan2(loan2: pl.DataFrame, iisprev: pl.DataFrame,
                   ploan_file: str, reptmon: str) -> pl.DataFrame:
    """
    DATA LOAN2 -- current NPL monthly comparison.
    """
    con = duckdb.connect()
    ploan = con.execute(f"""
        SELECT ACCTNO, NOTENO, CURBAL, DAYS, BORSTAT, NTBRCH, COSTCTR
        FROM '{ploan_file}'
        ORDER BY ACCTNO
    """).pl()
    con.close()

    # DATA IISPREV: merge with PLOAN, keep where PIISP=0 AND POIP=0 AND EXIST!='Y'
    iisprev_sel = iisprev.select(["ACCTNO", "NTBRCH", "PDAYS", "PSUSPEND",
                                   "POISUSP", "PIISP", "POIP", "POI", "PRECC",
                                   "POIRECC", "PRECOVER", "POIRECV", "EXIST"])
    iisprev_ploan = iisprev_sel.join(ploan.select(["ACCTNO"]), on="ACCTNO", how="left")
    iisprev_filt = iisprev_ploan.filter(
        (pl.col("PIISP") == 0) & (pl.col("POIP") == 0) & (pl.col("EXIST") != "Y")
    )
    filt_rows = []
    for r in iisprev_filt.to_dicts():
        r["BRANCH"] = branch_label(r.get("NTBRCH", 0))
        filt_rows.append(r)
    iisprev2 = pl.from_dicts(filt_rows, infer_schema_length=None) if filt_rows else pl.DataFrame()

    # Merge IISPREV2 (B) with LOAN2 (A)
    iisprev2_accts = set(iisprev2["ACCTNO"].to_list()) if not iisprev2.is_empty() else set()
    iisprev2_dict  = {r["ACCTNO"]: r for r in iisprev2.to_dicts()} if not iisprev2.is_empty() else {}

    out_rows = []
    loan2_accts = set(loan2["ACCTNO"].to_list()) if not loan2.is_empty() else set()

    # B AND NOT A (settled accounts from iisprev2 not in loan2)
    for acctno, prev_row in iisprev2_dict.items():
        if acctno not in loan2_accts:
            piisp    = prev_row.get("PIISP", 0) or 0
            psuspend = prev_row.get("PSUSPEND", 0) or 0
            poisusp  = prev_row.get("POISUSP", 0) or 0
            poip     = prev_row.get("POIP", 0) or 0
            iispw    = 0.0; oiw = 0.0; oirecc = 0.0; oirecv = 0.0
            iisp = piisp; recover = iisp; suspend = psuspend; recc = suspend
            oip_out = poip; oirecv_out = oip_out; oisusp = poisusp; oirecc_out = oisusp
            oi = (oip_out + oisusp - oirecv_out - oirecc_out - oiw)
            iis = (iisp + suspend - recover - recc - iispw)
            totiis = iis + oi
            out_rows.append({
                "BRANCH": prev_row.get("BRANCH", ""),
                "NTBRCH": prev_row.get("NTBRCH", 0),
                "ACCTNO": acctno, "NOTENO": None, "NAME": "",
                "NETPROC": 0, "CURBAL": 0, "BORSTAT": "",
                "DAYS": 0, "IIS": iis, "UHC": 0, "NETBAL": 0,
                "IISP": iisp, "SUSPEND": suspend, "RECOVER": recover,
                "RECC": recc, "IISPW": iispw,
                "OIP": oip_out, "OISUSP": oisusp, "OI": oi,
                "OIRECV": oirecv_out, "OIRECC": oirecc_out, "OIW": oiw,
                "TOTIIS": totiis, "LOANTYP": "", "EXIST": "",
                "COSTCTR": 0, "PENDBRH": 0, "USER5": "",
                "WDOWNIND": "", "RESCHEIND": "", "ACCRUAL": 0, "LOANTYPE": 0,
            })

    drop_cols = {"PDAYS", "PSUSPEND", "POISUSP", "PIISP", "POIP",
                 "PRECC", "POIRECC", "PRECOVER", "POIRECV"}

    for row in loan2.to_dicts():
        acctno   = row.get("ACCTNO")
        in_a     = True
        in_b     = acctno in iisprev2_accts
        prev     = iisprev2_dict.get(acctno, {})

        borstat  = row.get("BORSTAT", "") or ""
        curbal   = row.get("CURBAL", 0) or 0
        days     = row.get("DAYS", 0) or 0
        user5    = row.get("USER5", "") or ""
        rescheind = row.get("RESCHEIND", "") or ""
        iisp     = row.get("IISP", 0) or 0
        oip      = row.get("OIP", 0) or 0
        iispw    = row.get("IISPW", 0) or 0
        oiw      = row.get("OIW", 0) or 0
        iis      = row.get("IIS", 0) or 0
        oi       = row.get("OI", 0) or 0
        suspend  = row.get("SUSPEND", 0) or 0
        oisusp   = row.get("OISUSP", 0) or 0
        recover  = row.get("RECOVER", 0) or 0
        recc     = row.get("RECC", 0) or 0
        oirecv   = row.get("OIRECV", 0) or 0
        oirecc   = row.get("OIRECC", 0) or 0

        pdays    = prev.get("PDAYS", 0) or 0
        psuspend = prev.get("PSUSPEND", 0) or 0
        poisusp  = prev.get("POISUSP", 0) or 0
        piisp    = prev.get("PIISP", 0) or 0
        poip     = prev.get("POIP", 0) or 0
        precc    = prev.get("PRECC", 0) or 0
        poirecc  = prev.get("POIRECC", 0) or 0
        precover = prev.get("PRECOVER", 0) or 0
        poirecv  = prev.get("POIRECV", 0) or 0

        # *** NEW NPL FOR THE MTH ***
        if in_a and not in_b:
            if (days >= 90 or borstat in ("F", "I", "R", "W") or user5 == "N"):
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})
            continue

        if borstat == "W" or rescheind == "Y":
            out_rows.append({k: v for k, v in row.items() if k not in drop_cols})
            continue

        if in_a and in_b and borstat != "W":
            # *** CONTINUE PERFORMING ***
            if days < 90 and pdays < 90:
                if borstat not in ("F", "I", "R"):
                    suspend = psuspend; recc = psuspend
                    oisusp  = poisusp;  oirecc = poisusp
                if user5 == "N":
                    if iis < iisp:
                        suspend = 0; recover = iisp - iis; recc = 0
                    if iis >= iisp:
                        suspend = iis - iisp; recover = 0; recc = 0
                    if iisp == 0:
                        suspend = iis; recc = iis - suspend
                    if oi < oip:
                        oisusp = 0; oirecv = oip - oi; oirecc = 0
                    if oi >= oip:
                        oisusp = oi - oip; oirecv = 0; oirecc = 0
                    if oip == 0:
                        oisusp = oi; oirecc = oi - oisusp
                else:
                    suspend = (suspend or 0) + (recc or 0)
                    oisusp  = (oisusp or 0) + (oirecc or 0)
                row.update({"SUSPEND": suspend, "RECC": recc, "OISUSP": oisusp,
                            "OIRECC": oirecc, "RECOVER": recover, "OIRECV": oirecv,
                            "TOTIIS": iis + oi})
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

            # *** TURN PERFORMING FR NPL ***
            if days < 90 and pdays >= 90:
                if borstat not in ("F", "I", "R"):
                    suspend = psuspend; recc = psuspend
                    oisusp  = poisusp;  oirecc = poisusp
                    oi2 = (oip + oisusp - oirecv - oirecc - oiw)
                    iis2 = (iisp + suspend - recover - recc - iispw)
                    oi = oi2; iis = iis2
                if user5 == "N":
                    if iis < iisp:
                        suspend = 0; recover = iisp - iis; recc = 0
                    if iis >= iisp:
                        suspend = iis - iisp; recover = 0; recc = 0
                    if iisp == 0:
                        suspend = iis; recc = iis - suspend
                    if oi < oip:
                        oisusp = 0; oirecv = oip - oi; oirecc = 0
                    if oi >= oip:
                        oisusp = oi - oip; oirecv = 0; oirecc = 0
                    if oip == 0:
                        oisusp = oi; oirecc = oi - oisusp
                row.update({"SUSPEND": suspend, "RECC": recc, "OISUSP": oisusp,
                            "OIRECC": oirecc, "RECOVER": recover, "OIRECV": oirecv,
                            "IIS": iis, "OI": oi, "TOTIIS": iis + oi})
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

            # *** TURN NPL FR PERFORMING ***
            if days >= 90 and pdays < 90:
                if borstat not in ("F", "I", "R"):
                    recc    = (recc or 0) + (precc or 0)
                    suspend = (suspend or 0) + (recc or 0)
                    oirecc  = (oirecc or 0) + (poirecc or 0)
                    oisusp  = (oisusp or 0) + (oirecc or 0)
                if user5 == "N":
                    if iis < iisp:
                        suspend = 0; recover = iisp - iis; recc = 0
                    if iis >= iisp:
                        suspend = iis - iisp; recover = 0; recc = 0
                    if iisp == 0:
                        suspend = iis; recc = iis - suspend
                    if oi < oip:
                        oisusp = 0; oirecv = oip - oi; oirecc = 0
                    if oi >= oip:
                        oisusp = oi - oip; oirecv = 0; oirecc = 0
                    if oip == 0:
                        oisusp = oi; oirecc = oi - oisusp
                row.update({"SUSPEND": suspend, "RECC": recc, "OISUSP": oisusp,
                            "OIRECC": oirecc, "RECOVER": recover, "OIRECV": oirecv,
                            "TOTIIS": iis + oi})
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

            # *** CONTINUE NPL ***
            if days >= 90 and pdays >= 90:
                if borstat not in ("F", "I", "R"):
                    recc    = (recc or 0) + (precc or 0)
                    suspend = (suspend or 0) + (recc or 0)
                    oirecc  = (oirecc or 0) + (poirecc or 0)
                    oisusp  = (oisusp or 0) + (oirecc or 0)
                row.update({"SUSPEND": suspend, "RECC": recc, "OISUSP": oisusp,
                            "OIRECC": oirecc, "TOTIIS": iis + oi})
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

            # ***SPECIAL USER5=N***
            if user5 == "N" and iis == 0 and iisp == 0:
                row["SUSPEND"] = iis

    return pl.from_dicts(out_rows, infer_schema_length=None) if out_rows else pl.DataFrame()


# =============================================================================
# STEP 5: COMBINE LOAN1 & LOAN2 INTO LOAN3
# =============================================================================
def build_loan3(loan1: pl.DataFrame, loan2: pl.DataFrame) -> pl.DataFrame:
    """
    DATA LOAN3 NPL.IIS&REPTMON NPL.IIS;
    SET LOAN1 LOAN2;
    WHERE (3000<=COSTCTR<=3999) OR COSTCTR IN (4043,4048);
    """
    combined = pl.concat([loan1, loan2], how="diagonal")
    combined = combined.with_columns(
        pl.struct(["DAYS", "BORSTAT", "USER5"]).map_elements(
            lambda r: risk_label(r["DAYS"] or 0, r["BORSTAT"] or "", r["USER5"] or ""),
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
# =============================================================================
PAGE_LEN = 60

def format_num(val, width=15, dec=2) -> str:
    """Format number as COMMA15.2 equivalent."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        val = 0
    formatted = f"{val:,.{dec}f}"
    return formatted.rjust(width)


def format_count(val, width=7) -> str:
    """Format count as COMMA7. equivalent."""
    if val is None:
        val = 0
    return f"{int(val):,}".rjust(width)


class ReportWriter:
    """ASA carriage control report writer."""
    ASA_FIRST    = "1"  # new page / form feed
    ASA_DOUBLE   = "0"  # double space (blank line before)
    ASA_SINGLE   = " "  # single space
    ASA_NO_ADV   = "+"  # overprint (no advance)

    def __init__(self, filepath: Path):
        self.filepath = filepath
        self.lines: list[str] = []
        self.current_line = 0
        self.page_num = 0

    def new_page(self):
        self.page_num += 1
        self.current_line = 0

    def write(self, text: str, cc: str = " "):
        """Append a line with ASA carriage control character."""
        self.lines.append(cc + text)
        if cc != "+":
            self.current_line += 1
        if self.current_line >= PAGE_LEN:
            self.new_page()

    def title_block(self, title1: str, title2: str, title3: str = ""):
        self.write(title1.center(132), self.ASA_FIRST)
        self.write(title2.center(132), self.ASA_SINGLE)
        if title3:
            self.write(title3.center(132), self.ASA_SINGLE)
        self.write("", self.ASA_SINGLE)

    def flush(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            f.write("\n".join(self.lines) + "\n")


# =============================================================================
# TABULATE REPORT (%MACRO TBLS, I=3)
# =============================================================================
def produce_tabulate_report(loan3: pl.DataFrame, rdate: str,
                             writer: ReportWriter) -> None:
    """
    PROC TABULATE DATA=LOAN3 FORMAT=COMMA15.2 MISSING NOSEPS;
    Two tables: (1) LOANTYP x RISK x BRANCH, (2) LOANTYP x BRANCH
    """
    title1 = "PUBLIC ISLAMIC BANK - (NPL FROM 3 MONTHS & ABOVE) - NEW"
    title2 = f"MOVEMENTS OF INTEREST IN SUSPENSE FOR THE MONTH ENDING {rdate} (EXISTING AND CURRENT)"

    num_vars = ["CURBAL", "UHC", "NETBAL", "IISP", "SUSPEND", "RECOVER",
                "RECC", "IISPW", "IIS", "OIP", "OISUSP", "OIRECV",
                "OIRECC", "OIW", "OI", "TOTIIS"]
    col_labels = {
        "CURBAL":   "CURRENT BAL (A)",
        "UHC":      "UNEARNED HIRING CHARGES (B)",
        "NETBAL":   "NET BAL (A-B=C)",
        "IISP":     "OPENING BAL FOR FINANCIAL YEAR (D)",
        "SUSPEND":  "INTEREST SUSPENDED DURING THE PERIOD (E)",
        "RECOVER":  "WRITTEN BACK TO PROFIT & LOSS (F)",
        "RECC":     "REVERSAL OF CURRENT YEAR IIS (G)",
        "IISPW":    "WRITTEN OFF (H)",
        "IIS":      "IIS CLOSING BAL (D+E-F-G-H=I)",
        "OIP":      "OPENING BAL FOR FINANCIAL YEAR (J)",
        "OISUSP":   "OI SUSPENDED DURING THE PERIOD (K)",
        "OIRECV":   "WRITTEN BACK TO PROFIT & LOSS (L)",
        "OIRECC":   "REVERSAL OF CURRENT YEAR OI (M)",
        "OIW":      "WRITTEN OFF (N)",
        "OI":       "OI CLOSING BAL (J+K-L-M-N=O)",
        "TOTIIS":   "TOTAL CLOSING BAL AS AT RPT DATE (I+O)",
    }

    # ---- TABLE 1: LOANTYP x RISK x BRANCH ----
    writer.new_page()
    writer.title_block(title1, title2)
    _write_tabulate_by_risk_branch(loan3, num_vars, col_labels, writer)

    # ---- TABLE 2: LOANTYP x BRANCH ----
    writer.new_page()
    writer.title_block(title1, title2)
    _write_tabulate_by_branch(loan3, num_vars, col_labels, writer)


def _write_tabulate_by_risk_branch(df: pl.DataFrame, num_vars: list,
                                    col_labels: dict, writer: ReportWriter) -> None:
    """TABLE ... RISK x BRANCH ALL='SUB-TOTAL' ALL='TOTAL' with BOX='RISK BRANCH' RTS=29"""
    loan_types = sorted(df["LOANTYP"].unique().to_list())
    risks      = ["SUBSTANDARD-1", "SUBSTANDARD 2", "DOUBTFUL", "BAD"]
    hdr_width  = 29

    for lt in loan_types:
        lt_df = df.filter(pl.col("LOANTYP") == lt)
        writer.write(f"{'RISK        BRANCH':<{hdr_width}} {'NO OF ACCOUNT':>13} " +
                     "  ".join(f"{col_labels[v][:20]:>22}" for v in num_vars[:3]), " ")
        writer.write("-" * 200, " ")

        grand_sums = {v: 0.0 for v in num_vars}
        grand_n    = 0
        for risk in risks:
            r_df = lt_df.filter(pl.col("RISK") == risk)
            branches = sorted(r_df["BRANCH"].unique().to_list())
            risk_sums = {v: 0.0 for v in num_vars}; risk_n = 0
            for br in branches:
                b_df = r_df.filter(pl.col("BRANCH") == br)
                n  = len(b_df)
                sums = {v: b_df[v].fill_null(0).sum() for v in num_vars}
                lbl  = f"{risk[:14]:<14} {br[:14]:<14}"
                line = (f"{lbl:<{hdr_width}} {format_count(n):>13} " +
                        "  ".join(format_num(sums[v]) for v in num_vars))
                writer.write(line, " ")
                for v in num_vars:
                    risk_sums[v] += sums[v]
                risk_n += n
            # SUB-TOTAL
            lbl = f"{risk[:14]:<14} {'SUB-TOTAL':<14}"
            line = (f"{lbl:<{hdr_width}} {format_count(risk_n):>13} " +
                    "  ".join(format_num(risk_sums[v]) for v in num_vars))
            writer.write(line, "0")
            for v in num_vars:
                grand_sums[v] += risk_sums[v]
            grand_n += risk_n

        # TOTAL
        lbl = f"{'TOTAL':<{hdr_width}}"
        line = (f"{lbl} {format_count(grand_n):>13} " +
                "  ".join(format_num(grand_sums[v]) for v in num_vars))
        writer.write(line, "0")
        writer.write("", " ")


def _write_tabulate_by_branch(df: pl.DataFrame, num_vars: list,
                               col_labels: dict, writer: ReportWriter) -> None:
    """TABLE LOANTYP x BRANCH ALL='TOTAL' with BOX='BRANCH' RTS=9"""
    loan_types = sorted(df["LOANTYP"].unique().to_list())
    hdr_width  = 9

    for lt in loan_types:
        lt_df = df.filter(pl.col("LOANTYP") == lt)
        writer.write(f"{'BRANCH':<{hdr_width}} {'NO OF ACCOUNT':>13} " +
                     "  ".join(f"{col_labels[v][:20]:>22}" for v in num_vars[:3]), " ")
        writer.write("-" * 200, " ")
        branches   = sorted(lt_df["BRANCH"].unique().to_list())
        total_sums = {v: 0.0 for v in num_vars}; total_n = 0
        for br in branches:
            b_df = lt_df.filter(pl.col("BRANCH") == br)
            n    = len(b_df)
            sums = {v: b_df[v].fill_null(0).sum() for v in num_vars}
            lbl  = f"{br[:9]:<{hdr_width}}"
            line = (f"{lbl} {format_count(n):>13} " +
                    "  ".join(format_num(sums[v]) for v in num_vars))
            writer.write(line, " ")
            for v in num_vars:
                total_sums[v] += sums[v]
            total_n += n
        lbl  = f"{'TOTAL':<{hdr_width}}"
        line = (f"{lbl} {format_count(total_n):>13} " +
                "  ".join(format_num(total_sums[v]) for v in num_vars))
        writer.write(line, "0")
        writer.write("", " ")


# =============================================================================
# DETAIL PRINT REPORT (%MACRO DTLS, I=3)
# =============================================================================
def produce_detail_report(loan3: pl.DataFrame, rdate: str,
                           writer: ReportWriter) -> None:
    """
    PROC SORT DATA=LOAN3; BY LOANTYP BRANCH RISK DAYS ACCTNO;
    PROC PRINT LABEL N;
    BY LOANTYP BRANCH RISK; PAGEBY BRANCH; SUMBY RISK;
    /* DISCONTINUE AS PER LETTER DATED 26/08/03 FR STATISTICS */
    """
    title1 = "PUBLIC ISLAMIC BANK - (NPL FROM 3 MONTHS & ABOVE) - NEW"
    title2 = f"MOVEMENTS OF INTEREST IN SUSPENSE FOR THE MONTH ENDING {rdate} (EXISTING AND CURRENT)"

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
    num_vars = ["NETPROC", "CURBAL", "UHC", "NETBAL", "IISP", "SUSPEND",
                "RECOVER", "RECC", "IISPW", "IIS", "OIP", "OISUSP",
                "OIRECV", "OIRECC", "OIW", "OI", "TOTIIS"]

    sorted_df = loan3.sort(["LOANTYP", "BRANCH", "RISK", "DAYS", "ACCTNO"])

    prev_branch = None; prev_loantyp = None; prev_risk = None
    risk_sums   = {v: 0.0 for v in num_vars}; risk_n = 0
    total_n     = 0; total_sums = {v: 0.0 for v in num_vars}

    header_cols = (f"{'MNI ACCOUNT NO':<20} {'NAME':<30} {'DAYS':>5} "
                   f"{'STAT':<5} " +
                   " ".join(f"{labels[v][:14]:>15}" for v in num_vars))

    for row in sorted_df.to_dicts():
        loantyp = row.get("LOANTYP", "") or ""
        branch  = row.get("BRANCH", "") or ""
        risk    = row.get("RISK", "") or ""

        # Page break on new BRANCH (PAGEBY BRANCH)
        if branch != prev_branch or loantyp != prev_loantyp:
            writer.new_page()
            writer.title_block(title1, title2)
            writer.write(f"LOAN TYPE: {loantyp}  BRANCH: {branch}", " ")
            writer.write(header_cols, " ")
            writer.write("-" * 200, " ")

        # SUMBY RISK: print risk subtotal before switching
        if risk != prev_risk and prev_risk is not None:
            sumline = (f"{'RISK TOTAL: ' + prev_risk:<55} " +
                       " ".join(format_num(risk_sums[v]) for v in num_vars))
            writer.write(sumline, "0")
            risk_sums = {v: 0.0 for v in num_vars}; risk_n = 0

        acctno = row.get("ACCTNO", "") or ""
        name   = (row.get("NAME", "") or "")[:30]
        days   = row.get("DAYS", 0) or 0
        bstat  = row.get("BORSTAT", "") or ""
        line   = (f"{str(acctno):<20} {name:<30} {days:>5} {bstat:<5} " +
                  " ".join(format_num(row.get(v, 0)) for v in num_vars))
        writer.write(line, " ")

        for v in num_vars:
            val = row.get(v, 0) or 0
            risk_sums[v]  += val
            total_sums[v] += val
        risk_n  += 1
        total_n += 1

        prev_loantyp = loantyp; prev_branch = branch; prev_risk = risk

    # Final risk subtotal
    if prev_risk is not None:
        sumline = (f"{'RISK TOTAL: ' + prev_risk:<55} " +
                   " ".join(format_num(risk_sums[v]) for v in num_vars))
        writer.write(sumline, "0")

    # Grand total
    totline = (f"{'GRAND TOTAL':<55} " +
               " ".join(format_num(total_sums[v]) for v in num_vars))
    writer.write(totline, "0")
    writer.write(f"N = {total_n}", " ")


# =============================================================================
# MAIN
# =============================================================================
def main():
    # Load REPTDATE
    reptdate_df, macro_vars = load_reptdate()
    reptdate_val: date = macro_vars["REPTDATE"]
    reptmon  = macro_vars["REPTMON"]
    prevmon  = macro_vars["PREVMON"]
    rdate    = macro_vars["RDATE"]

    # Build LOANWOFF
    loanwoff = build_loanwoff(reptmon, reptdate_df)

    # Calculate LOAN1 (existing NPL)
    loan1 = calc_loan1(loanwoff, reptdate_val)

    # Calculate LOAN2 (current NPL)
    loan2 = calc_loan2(loanwoff, reptdate_val)

    # Apply monthly comparison
    loan1, loan2 = apply_monthly(loan1, loan2, reptmon, prevmon, reptdate_val)

    # Build LOAN3 (combined)
    loan3 = build_loan3(loan1, loan2)

    # Save output datasets
    iis_out = str(IIS_OUT_TMPL).format(reptmon=reptmon)
    loan3.write_parquet(iis_out)
    loan3.write_parquet(str(IIS_LATEST))

    # Produce reports
    # OPTIONS NOCENTER NODATE NONUMBER MISSING=0
    writer = ReportWriter(REPORT_FILE)

    # %TBLS (I=3)
    produce_tabulate_report(loan3, rdate, writer)

    # /* DISCONTINUE AS PER LETTER DATED 26/08/03 FR STATISTICS */
    # %DTLS (I=3)
    produce_detail_report(loan3, rdate, writer)

    writer.flush()
    print(f"Report written to: {REPORT_FILE}")
    print(f"IIS dataset saved to: {iis_out}")
    print(f"IIS latest saved to:  {IIS_LATEST}")


if __name__ == "__main__":
    main()
