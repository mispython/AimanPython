# !/usr/bin/env python3
"""
 PROGRAM : EIFMNP06 (JCL: EIIMNP06)
 DATE    : 18.03.98
 MODIFY  : ESMR 2004-720, 2004-579, 2006-1048, 2006-1281
 REPORT  : MOVEMENTS OF SPECIFIC PROVISION FOR THE MONTH ENDING
           (BASED ON DEPRECIATED PURCHASE PRICE FOR UNSCHEDULED GOODS)
"""

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
REPORT_DIR     = BASE_DIR / "reports"

# Input parquet files
REPTDATE_FILE  = INPUT_DIR / "reptdate.parquet"
LOAN_FILE_TMPL = INPUT_DIR / "loan{reptmon}.parquet"
WSP2_FILE      = INPUT_DIR / "wsp2.parquet"
IIS_FILE_TMPL  = INPUT_DIR / "iis{reptmon}.parquet"
SP2_PREV_TMPL  = INPUT_DIR / "sp2{prevmon}.parquet"
PLOAN_TMPL     = INPUT_DIR / "ploan{reptmon}.parquet"

# Output files
SP2_OUT_TMPL   = INPUT_DIR / "sp2{reptmon}.parquet"
SP2_LATEST     = INPUT_DIR / "sp2.parquet"

# Report output
REPORT_FILE    = REPORT_DIR / "EIFMNP06_report.txt"

REPORT_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# FORMAT DEFINITIONS
# =============================================================================
# %INC PGM(PBBLNFMT);  -- placeholder for PBBLNFMT format definitions
# %INC PGM(PBBELF);    -- placeholder for PBBELF format definitions

BRCHCD_MAP: dict[int, str] = {
    # Populate from PBBLNFMT definitions
}


def brchcd_format(ntbrch: int) -> str:
    return BRCHCD_MAP.get(ntbrch, str(ntbrch))


def branch_label(ntbrch: int) -> str:
    return f"{brchcd_format(ntbrch)} {ntbrch:03d}"


def lntyp_format(loantype: int) -> str:
    """
    PROC FORMAT VALUE LNTYP:
      128,130,983,131,132     = 'HPD AITAB'
      700,705,993,996,380,381 = 'HPD CONVENTIONAL'
      200-299                 = 'HOUSING LOANS'
      OTHER                   = 'OTHERS'
    """
    if loantype in (128, 130, 983, 131, 132):
        return "HPD AITAB"
    elif loantype in (700, 705, 993, 996, 380, 381):
        return "HPD CONVENTIONAL"
    elif 200 <= loantype <= 299:
        return "HOUSING LOANS"
    else:
        return "OTHERS"


def risk_label(days: int, borstat: str, user5: str) -> str:
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
# UHC HELPER
# =============================================================================
def uhc_val(remmth2: int, termchg: float, earnterm: int) -> float:
    if earnterm == 0:
        return 0.0
    return remmth2 * (remmth2 + 1) * termchg / (earnterm * (earnterm + 1))


# =============================================================================
# STEP 1: READ REPTDATE
# =============================================================================
def load_reptdate() -> tuple[pl.DataFrame, dict]:
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM '{REPTDATE_FILE}'").pl()
    con.close()
    row = df.row(0, named=True)
    reptdate_val: date = row["REPTDATE"]
    reptmon = reptdate_val.month
    prevmon = 12 if reptmon == 1 else reptmon - 1
    rdate_str = reptdate_val.strftime("%d %B %Y").upper()
    return df, {
        "RDATE": rdate_str,
        "REPTMON": f"{reptmon:02d}",
        "PREVMON": f"{prevmon:02d}",
        "REPTDATE": reptdate_val,
    }


# =============================================================================
# STEP 2: BUILD LOANWOFF (loan + wsp2 + iis)
# =============================================================================
def build_loanwoff(reptmon: str) -> pl.DataFrame:
    """
    PROC SORT DATA=NPL.LOAN&REPTMON; BY ACCTNO;
    PROC SORT DATA=NPL.WSP2; BY ACCTNO;
    PROC SORT DATA=NPL.IIS&REPTMON (KEEP=ACCTNO IIS) OUT=IIS; BY ACCTNO;
    DATA LOANWOFF;
      MERGE NPL.LOAN&REPTMON NPL.WSP2 (IN=AA DROP=NOTENO NTBRCH);
      BY ACCTNO;
    DATA LOANWOFF;
      MERGE LOANWOFF(IN=A) IIS;
      BY ACCTNO;
      IF A;
    """
    loan_file = str(LOAN_FILE_TMPL).format(reptmon=reptmon)
    wsp2_file = str(WSP2_FILE)
    iis_file  = str(IIS_FILE_TMPL).format(reptmon=reptmon)
    con = duckdb.connect()

    loanwoff = con.execute(f"""
        SELECT l.*,
               CASE WHEN w.ACCTNO IS NOT NULL THEN 'Y' ELSE 'N' END AS WRITEOFF,
               w.* EXCLUDE (ACCTNO, NOTENO, NTBRCH),
               i.IIS
        FROM '{loan_file}' l
        LEFT JOIN '{wsp2_file}' w ON l.ACCTNO = w.ACCTNO
        LEFT JOIN (SELECT ACCTNO, IIS FROM '{iis_file}') i ON l.ACCTNO = i.ACCTNO
        ORDER BY l.ACCTNO
    """).pl()
    con.close()

    def fix_row(r: dict) -> dict:
        if r.get("LOANTYPE") in (983, 993):
            r["WDOWNIND"] = "N"
        # IF BB THEN HARDCODE = 'N'; ELSE HARDCODE = 'N';  -- commented in original
        earnterm = r.get("EARNTERM")
        noteterm = r.get("NOTETERM")
        if not earnterm or earnterm == 0:
            r["EARNTERM"] = noteterm or 0
        return r

    return pl.from_dicts([fix_row(r) for r in loanwoff.to_dicts()],
                         infer_schema_length=None)


# =============================================================================
# SP CALCULATION HELPERS
# =============================================================================
def _calc_sp_row(row: dict, reptdate_val: date, is_existing: bool,
                 stmth: int, styr: int) -> dict:
    """
    Shared SP calculation logic for both LOAN1 and LOAN2.
    """
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
    costctr   = row.get("COSTCTR", 0) or 0
    pendbrh   = row.get("PENDBRH", 0) or 0
    vinno     = row.get("VINNO", "") or ""
    census7   = row.get("CENSUS7", "") or ""
    appvalue  = row.get("APPVALUE", 0) or 0
    hardcode  = row.get("HARDCODE", "") or ""
    wrealvl   = row.get("WREALVL", 0) or 0
    wsppl     = row.get("WSPPL")
    wsp       = row.get("WSP")
    iis       = row.get("IIS", 0) or 0
    feeamt    = row.get("FEEAMT", 0) or 0
    feetot2   = row.get("FEETOT2", 0) or 0
    feeamta   = row.get("FEEAMTA", 0) or 0
    feeamt5   = row.get("FEEAMT5", 0) or 0
    feeamt8   = row.get("FEEAMT8", 0) or 0

    if is_existing:
        spp2 = row.get("SPP2", 0) or 0
    else:
        spp2 = 0.0

    if writeoff == "Y" and wdownind != "Y":
        borstat = "W"

    uhc = 0.0

    if bldate and bldate > date(1900, 1, 1) and termchg > 0:
        if is_existing:
            cond = days > 89 or borstat in ("F", "R", "I") or user5 == "N"
        else:
            cond = True  # always compute for current

        if cond or not is_existing:
            remmth1 = (earnterm - ((bldate.year - issdte.year) * 12
                       + bldate.month - issdte.month + 1)) if issdte else 0
            remmth2 = (earnterm - ((reptdate_val.year - issdte.year) * 12
                       + reptdate_val.month - issdte.month + 1)) if issdte else 0
            remmths = (earnterm - ((styr - issdte.year) * 12
                       + stmth - issdte.month + 1)) if issdte else 0
            if remmth2 < 0:
                remmth2 = 0
            if loantype in (128, 130):
                remmth1 -= 3
            else:
                remmth1 -= 1
            # IF REMMTH1 >= REMMTH2 THEN ...  -- commented out in original
            if remmth2 > 0:
                uhc = uhc_val(remmth2, termchg, earnterm)
    else:
        remmth2 = (earnterm - ((reptdate_val.year - issdte.year) * 12
                   + reptdate_val.month - issdte.month + 1)) if issdte else 0
        if remmth2 < 0:
            remmth2 = 0
        if remmth2 > 0:
            uhc = uhc_val(remmth2, termchg, earnterm)

    curbal  = curbal or 0
    netbal  = curbal - uhc
    osprin  = curbal - uhc - iis

    # OTHERFEE calculation
    if loantype in (380, 381):
        otherfee = (feeamt or 0) - (feetot2 or 0)
    else:
        otherfee = (feeamt8 or 0) - (feetot2 or 0) + (feeamta or 0) - (feeamt5 or 0)
    if otherfee < 0:
        otherfee = 0.0
    if loantype in (983, 993):
        otherfee = 0.0

    # Determine conditions for market value branch
    use_market_branch = (
        appvalue > 0
        and (loantype in (705, 128, 700, 130, 380, 381) or census7 == "9")
        and (days > 89 or user5 == "N")
        and borstat not in ("F", "R", "I", "Y", "W")
        and loantype not in (983, 993)
    )

    marketvl = 0.0; netexp = 0.0; sp = 0.0

    if use_market_branch:
        age = int(reptdate_val.year - issdte.year +
                  (reptdate_val.month - issdte.month) / 12) if issdte else 0
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
        # IF LOANTYPE IN (128,130) THEN NETEXP = OSPRIN - MARKETVL;  -- commented in original
        if days > 364:
            sp = netexp
        elif days > 273:
            sp = netexp / 2
        elif days > 89:
            sp = netexp * 0.2
        elif days < 90:
            sp = netexp * 0.2
        else:
            sp = 0.0
    else:
        if borstat != "R":
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

    if is_existing:
        sppl = sp - spp2
        if sppl < 0:
            sppl = 0.0
    else:
        sppl = sp

    sppw = 0.0; recover = 0.0

    if hardcode == "Y":
        if wsppl is not None:
            sppl = wsppl
        if wsp is not None:
            sp = wsp

    if is_existing:
        if borstat == "W":
            sppw    = spp2
            sp      = 0.0
            marketvl = 0.0
        else:
            recover = spp2 - sp
            if recover < 0:
                recover = 0.0

    branch  = branch_label(ntbrch)
    loantyp = lntyp_format(loantype)

    # Written-off overrides
    if writeoff == "Y":
        sppl     = row.get("WSPPL", 0) or 0
        otherfee = 0.0
        if wdownind != "Y":
            recover = row.get("WRECOVER", 0) or 0
            sp      = 0.0
            sppw    = (spp2 or 0) + (sppl or 0) - (recover or 0)
        else:
            sppw = row.get("WSPPW", 0) or 0
            if netexp <= 0:
                recover = 0.0
            sp = (spp2 or 0) + (sppl or 0) - (recover or 0) - (sppw or 0)
            if netexp <= 0 and sp > 0:
                recover = sp
                sp = 0.0

    # RESCHEIND override
    if rescheind == "Y":
        # SPLL = WSPLL;  -- note: SPLL appears to be a typo for SPPL in the SAS source
        sppl    = row.get("WSPLL", row.get("WSPPL", 0)) or 0
        recover = row.get("WRECOVER", 0) or 0
        sppw    = row.get("WSPPW", 0) or 0
        sp      = (spp2 or 0) + (sppl or 0) - (recover or 0) - (sppw or 0)

    return {
        "BRANCH": branch, "NTBRCH": ntbrch, "ACCTNO": acctno,
        "NOTENO": noteno, "NAME": name, "DAYS": days, "BORSTAT": borstat,
        "NETPROC": netproc, "CURBAL": curbal, "UHC": uhc, "NETBAL": netbal,
        "IIS": iis, "OSPRIN": osprin, "MARKETVL": marketvl,
        "NETEXP": netexp, "SPP2": spp2, "SPPL": sppl,
        "RECOVER": recover, "SPPW": sppw, "SP": sp,
        "LOANTYP": loantyp, "VINNO": vinno, "CENSUS7": census7,
        "OTHERFEE": otherfee, "EXIST": row.get("EXIST", ""),
        "COSTCTR": costctr, "USER5": user5, "PENDBRH": pendbrh,
        "WDOWNIND": wdownind, "RESCHEIND": rescheind, "LOANTYPE": loantype,
    }


def calc_loan1(loanwoff: pl.DataFrame, reptdate_val: date) -> pl.DataFrame:
    """DATA LOAN1 -- Existing NPL (EXIST='Y')"""
    df = loanwoff.filter(pl.col("EXIST") == "Y")
    out_rows = [_calc_sp_row(r, reptdate_val, True, 1, reptdate_val.year)
                for r in df.to_dicts()]
    return pl.from_dicts(out_rows, infer_schema_length=None) if out_rows else pl.DataFrame()


def calc_loan2(loanwoff: pl.DataFrame, reptdate_val: date) -> pl.DataFrame:
    """DATA LOAN2 -- Current NPL (EXIST != 'Y')"""
    df = loanwoff.filter(pl.col("EXIST") != "Y")
    # IF DAYS > 182 OR BORSTAT NOT IN (' ','S');  -- commented in original
    out_rows = [_calc_sp_row(r, reptdate_val, False, 1, reptdate_val.year)
                for r in df.to_dicts()]
    return pl.from_dicts(out_rows, infer_schema_length=None) if out_rows else pl.DataFrame()


# =============================================================================
# STEP 4: %MACRO MONTHLY
# =============================================================================
def apply_monthly(loan1: pl.DataFrame, loan2: pl.DataFrame,
                  reptmon: str, prevmon: str,
                  reptdate_val: date) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    %MACRO MONTHLY
    """
    # %INC PGM(NPLNTB);  -- placeholder

    if reptmon == "01":
        for col in ("SPPLCUM",):
            if col not in loan1.columns:
                loan1 = loan1.with_columns(pl.lit(0.0).alias(col))
            else:
                loan1 = loan1.with_columns(pl.lit(0.0).alias(col))
        for col in ("SPPLCUM",):
            if col not in loan2.columns:
                loan2 = loan2.with_columns(pl.lit(0.0).alias(col))
            else:
                loan2 = loan2.with_columns(pl.lit(0.0).alias(col))
        return loan1, loan2

    sp2_prev_file = str(SP2_PREV_TMPL).format(prevmon=prevmon)
    ploan_file    = str(PLOAN_TMPL).format(reptmon=reptmon)
    con = duckdb.connect()

    # PROC SORT DATA=NPL.SP2&PREVMON (RENAME=(...)) OUT=SP2PREV NODUPKEY
    sp2prev_raw = con.execute(f"""
        SELECT ACCTNO, NOTENO, NTBRCH, LOANTYPE, PAIDIND, EXIST,
               DAYS     AS PDAYS,
               SPP2     AS PSPP2,
               SPPL     AS PSPPL,
               SP       AS PSP,
               RECOVER  AS PRECOVER,
               BORSTAT  AS PBORSTAT
        FROM '{sp2_prev_file}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO, NOTENO ORDER BY ACCTNO) = 1
        ORDER BY ACCTNO, NOTENO
    """).pl()
    con.close()

    # %INC PGM(NPLNTB);  -- placeholder
    npl_types = (128, 130, 131, 132, 380, 381, 390, 700, 705, 720, 725, 983, 993, 996)

    def process_sp2prev_row(r: dict) -> dict:
        # %INC PGM(NPLNTB);  -- placeholder: NPLNTB adjustments for qualified types
        if r.get("LOANTYPE") in npl_types and r.get("PAIDIND") == "P":
            pass  # NPLNTB logic would execute here
        r["BRANCH"] = branch_label(r.get("NTBRCH", 0))
        for f in ("PDAYS", "PSPP2", "PSPPL", "PSP", "PRECOVER"):
            if r.get(f) is None:
                r[f] = 0.0
        return r

    sp2prev_rows = [process_sp2prev_row(r) for r in sp2prev_raw.to_dicts()]
    sp2prev = pl.from_dicts(sp2prev_rows, infer_schema_length=None) if sp2prev_rows else pl.DataFrame()

    # *** EXISTING NPL ***
    loan1 = _monthly_loan1_sp2(loan1, sp2prev)
    # *** CURRENT NPL ***
    loan2 = _monthly_loan2_sp2(loan2, sp2prev, ploan_file)

    return loan1, loan2


def _monthly_loan1_sp2(loan1: pl.DataFrame, sp2prev: pl.DataFrame) -> pl.DataFrame:
    """DATA LOAN1 -- merge with sp2prev for existing NPL SP monthly comparison."""
    drop_cols = {"PDAYS", "PSPPL", "PSP", "PSPP2", "PRECOVER", "PBORSTAT"}
    sp2prev_dict = {r["ACCTNO"]: r for r in sp2prev.to_dicts()} if not sp2prev.is_empty() else {}
    sp2prev_accts = set(sp2prev_dict.keys())

    out_rows = []
    for row in loan1.to_dicts():
        acctno    = row.get("ACCTNO")
        in_a      = True
        in_b      = acctno in sp2prev_accts
        exist     = row.get("EXIST", "")
        prev      = sp2prev_dict.get(acctno, {})

        if not ((in_a and in_b) or (in_b and not in_a)) or exist != "Y":
            continue

        borstat   = row.get("BORSTAT", "") or ""
        curbal    = row.get("CURBAL", 0) or 0
        days      = row.get("DAYS", 0) or 0
        rescheind = row.get("RESCHEIND", "") or ""
        user5     = row.get("USER5", "") or ""
        spp2      = row.get("SPP2", 0) or 0
        sp        = row.get("SP", 0) or 0
        sppl      = row.get("SPPL", 0) or 0
        recover   = row.get("RECOVER", 0) or 0
        sppw      = row.get("SPPW", 0) or 0
        uhc       = row.get("UHC", 0) or 0
        iis       = row.get("IIS", 0) or 0
        marketvl  = row.get("MARKETVL", 0) or 0
        osprin    = row.get("OSPRIN", 0) or 0
        netbal    = row.get("NETBAL", 0) or 0
        netexp    = row.get("NETEXP", 0) or 0

        pdays     = prev.get("PDAYS", 0) or 0
        pspp2     = prev.get("PSPP2", 0) or 0
        psppl     = prev.get("PSPPL", 0) or 0
        psp       = prev.get("PSP", 0) or 0
        precover  = prev.get("PRECOVER", 0) or 0

        # *** A/C SETTLE FOR EXISTING NPL ***
        if (not in_a or (curbal <= 0 and psp <= 0)) and \
                borstat not in ("F", "I", "R", "W", "S"):
            sppl     = psppl
            recover  = (pspp2 or 0) + (psppl or 0)
            curbal   = 0; netbal = 0; uhc = 0; iis = 0; marketvl = 0
            osprin   = (curbal) - (uhc) - (iis)
            netexp   = (osprin) - (marketvl)
            days     = 0
            sp2      = (spp2 or 0) + (sppl or 0) - (recover or 0) - (sppw or 0)
            row.update({"SPPL": sppl, "RECOVER": recover, "CURBAL": curbal,
                        "NETBAL": netbal, "UHC": uhc, "IIS": iis,
                        "MARKETVL": marketvl, "OSPRIN": osprin,
                        "NETEXP": netexp, "DAYS": days, "SP": sp2})
            out_rows.append({k: v for k, v in row.items() if k not in drop_cols})
            continue

        if borstat == "W" or rescheind == "Y":
            out_rows.append({k: v for k, v in row.items() if k not in drop_cols})
            continue

        if in_a and in_b:
            # *** CONTINUE PERFORMING ***
            if days < 90 and pdays < 90:
                if borstat not in ("F", "I", "R"):
                    sppl    = psppl
                    recover = (pspp2 or 0) + (psppl or 0)
                if user5 == "N" and spp2 >= sp:
                    sppl = 0; recover = spp2 - sp
                if user5 == "N" and spp2 < sp:
                    sppl = sp - spp2; recover = 0
                row.update({"SPPL": sppl, "RECOVER": recover})
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

            # *** TURN PERFORMING ***
            if days < 90 and pdays >= 90:
                if borstat not in ("F", "I", "R"):
                    iis      = 0
                    if user5 != "N":
                        marketvl = 0
                    osprin = (curbal) - (uhc) - (iis)
                    netexp = (osprin) - (marketvl)
                    sppl   = psppl
                    recover = (pspp2 or 0) + (psppl or 0)
                if user5 == "N" and spp2 >= sp:
                    sppl = 0; recover = spp2 - sp
                if user5 == "N" and spp2 < sp:
                    sppl = sp - spp2; recover = 0
                row.update({"IIS": iis, "MARKETVL": marketvl, "OSPRIN": osprin,
                            "NETEXP": netexp, "SPPL": sppl, "RECOVER": recover})
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

            # *** TURN NPL FR PERFORMING ***
            if days >= 90 and pdays < 90:
                if borstat not in ("F", "I", "R"):
                    sppl   = (sp or 0) + (psppl or 0)
                    recover = (psppl or 0) + (pspp2 or 0)
                if user5 == "N" and spp2 >= sp:
                    sppl = 0; recover = spp2 - sp
                if user5 == "N" and spp2 < sp:
                    sppl = sp - spp2; recover = 0
                row.update({"SPPL": sppl, "RECOVER": recover})
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

            # *** CONTINUE NPL ***
            if days >= 90 and pdays >= 90:
                if borstat not in ("F", "I", "R"):
                    sppl = (sp or 0) - (pspp2 or 0)
                    if sppl < 0:
                        recover = sppl * (-1)
                        sppl = 0.0
                row.update({"SPPL": sppl, "RECOVER": recover})
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

    return pl.from_dicts(out_rows, infer_schema_length=None) if out_rows else pl.DataFrame()


def _monthly_loan2_sp2(loan2: pl.DataFrame, sp2prev: pl.DataFrame,
                        ploan_file: str) -> pl.DataFrame:
    """DATA LOAN2 -- current NPL SP monthly comparison."""
    con = duckdb.connect()
    ploan = con.execute(f"""
        SELECT ACCTNO, NOTENO, CURBAL, DAYS, BORSTAT, NTBRCH, COSTCTR
        FROM '{ploan_file}'
        ORDER BY ACCTNO
    """).pl()
    con.close()

    sp2prev_dict  = {r["ACCTNO"]: r for r in sp2prev.to_dicts()} if not sp2prev.is_empty() else {}
    ploan_accts   = set(ploan["ACCTNO"].to_list()) if not ploan.is_empty() else set()

    # DATA SP2PREV: merge with PLOAN, keep where PSPP2=0 AND EXIST!='Y'
    sp2prev_filt_dict = {}
    for acctno, prev in sp2prev_dict.items():
        if (prev.get("PSPP2", 0) or 0) == 0 and prev.get("EXIST", "") != "Y":
            r = dict(prev)
            r["BRANCH"] = branch_label(r.get("NTBRCH", 0))
            sp2prev_filt_dict[acctno] = r

    drop_cols = {"PDAYS", "PSPPL", "PSP", "PSPP2", "PRECOVER", "PBORSTAT"}
    loan2_accts = set(loan2["ACCTNO"].to_list()) if not loan2.is_empty() else set()
    out_rows = []

    # B AND NOT A  (settled accounts from sp2prev_filt not in loan2)
    for acctno, prev_row in sp2prev_filt_dict.items():
        if acctno not in loan2_accts:
            psppl    = prev_row.get("PSPPL", 0) or 0
            pspp2    = prev_row.get("PSPP2", 0) or 0
            sppw     = 0.0; spp2 = 0.0; iis = 0.0
            sppl     = psppl
            recover  = (pspp2 or 0) + (psppl or 0)
            marketvl = 0.0; uhc = 0.0; curbal = 0.0
            osprin   = curbal - uhc - iis
            netexp   = osprin - marketvl
            sp       = (spp2 or 0) + (sppl or 0) - (recover or 0) - (sppw or 0)
            out_rows.append({
                "BRANCH": prev_row.get("BRANCH", ""),
                "NTBRCH": prev_row.get("NTBRCH", 0),
                "ACCTNO": acctno, "NOTENO": None, "NAME": "",
                "DAYS": 0, "BORSTAT": "", "NETPROC": 0,
                "CURBAL": curbal, "UHC": uhc, "NETBAL": 0,
                "IIS": iis, "OSPRIN": osprin, "MARKETVL": marketvl,
                "NETEXP": netexp, "SPP2": spp2, "SPPL": sppl,
                "RECOVER": recover, "SPPW": sppw, "SP": sp,
                "LOANTYP": "", "VINNO": "", "CENSUS7": "", "OTHERFEE": 0,
                "EXIST": "", "COSTCTR": 0, "USER5": "", "PENDBRH": 0,
                "WDOWNIND": "", "RESCHEIND": "", "LOANTYPE": 0,
            })

    for row in loan2.to_dicts():
        acctno    = row.get("ACCTNO")
        in_a      = True
        in_b      = acctno in sp2prev_filt_dict
        prev      = sp2prev_filt_dict.get(acctno, {})
        borstat   = row.get("BORSTAT", "") or ""
        days      = row.get("DAYS", 0) or 0
        user5     = row.get("USER5", "") or ""
        rescheind = row.get("RESCHEIND", "") or ""
        spp2      = row.get("SPP2", 0) or 0
        sp        = row.get("SP", 0) or 0
        sppl      = row.get("SPPL", 0) or 0
        recover   = row.get("RECOVER", 0) or 0
        sppw      = row.get("SPPW", 0) or 0
        iis       = row.get("IIS", 0) or 0
        marketvl  = row.get("MARKETVL", 0) or 0
        curbal    = row.get("CURBAL", 0) or 0
        uhc       = row.get("UHC", 0) or 0
        osprin    = row.get("OSPRIN", 0) or 0
        netexp    = row.get("NETEXP", 0) or 0

        pdays    = prev.get("PDAYS", 0) or 0
        pspp2    = prev.get("PSPP2", 0) or 0
        psppl    = prev.get("PSPPL", 0) or 0
        precover = prev.get("PRECOVER", 0) or 0
        pborstat = prev.get("PBORSTAT", "") or ""

        # *** NEW NPL FOR THE MTH ***
        if in_a and not in_b:
            if (days >= 90 or borstat in ("F", "I", "R", "W") or user5 == "N"):
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})
            continue

        if in_a and in_b:
            if borstat == "W" or rescheind == "Y":
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})
                continue

            # *** CONTINUE PERFORMING ***
            if days < 90 and pdays < 90:
                if borstat not in ("F", "I", "R"):
                    sppl    = psppl
                    recover = precover
                if borstat in ("F", "I", "R"):
                    recover = precover
                    sppl = (sp or 0) + (recover or 0)
                if user5 == "N" and spp2 >= sp:
                    sppl = 0; recover = spp2 - sp
                if user5 == "N" and spp2 < sp:
                    sppl = sp - spp2; recover = 0
                row.update({"SPPL": sppl, "RECOVER": recover})
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

            # *** TURN PERFORMING ***
            if days < 90 and pdays >= 90:
                if borstat not in ("F", "I", "R"):
                    iis      = 0; marketvl = 0
                    osprin   = (curbal) - (uhc) - (iis)
                    netexp   = (osprin) - (marketvl)
                    sppl     = psppl
                    recover  = psppl
                if user5 == "N" and spp2 >= sp:
                    sppl = 0; recover = spp2 - sp
                if user5 == "N" and spp2 < sp:
                    sppl = sp - spp2; recover = 0
                row.update({"IIS": iis, "MARKETVL": marketvl, "OSPRIN": osprin,
                            "NETEXP": netexp, "SPPL": sppl, "RECOVER": recover})
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

            # *** TURN NPL FR PERFORMING ***
            if days >= 90 and pdays < 90:
                if borstat not in ("F", "I", "R"):
                    sppl   = (sp or 0) + (psppl or 0)
                    recover = psppl
                if user5 == "N" and spp2 >= sp:
                    sppl = 0; recover = spp2 - sp
                if user5 == "N" and spp2 < sp:
                    sppl = sp - spp2; recover = 0
                row.update({"SPPL": sppl, "RECOVER": recover})
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

            # *** CONTINUE NPL ***
            if days >= 90 and pdays >= 90:
                recover = precover
                sppl    = (sp or 0) + (recover or 0)
                row.update({"SPPL": sppl, "RECOVER": recover})
                out_rows.append({k: v for k, v in row.items() if k not in drop_cols})

    return pl.from_dicts(out_rows, infer_schema_length=None) if out_rows else pl.DataFrame()


# =============================================================================
# STEP 5: COMBINE INTO LOAN3
# =============================================================================
def build_loan3(loan1: pl.DataFrame, loan2: pl.DataFrame) -> pl.DataFrame:
    """
    DATA LOAN3 NPL.SP2&REPTMON NPL.SP2;
    SET LOAN1 LOAN2;
    WHERE (3000<=COSTCTR<=3999) OR COSTCTR IN (4043,4048);
    PROC SORT DATA=LOAN3 NODUPKEY; BY ACCTNO NOTENO;
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
    loan3 = loan3.unique(subset=["ACCTNO", "NOTENO"], keep="first")
    return loan3


# =============================================================================
# REPORTING
# =============================================================================
PAGE_LEN = 60


def format_num(val, width: int = 14, dec: int = 2) -> str:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        val = 0
    return f"{val:,.{dec}f}".rjust(width)


def format_count(val, width: int = 7) -> str:
    if val is None:
        val = 0
    return f"{int(val):,}".rjust(width)


class ReportWriter:
    ASA_FIRST  = "1"
    ASA_DOUBLE = "0"
    ASA_SINGLE = " "

    def __init__(self, filepath: Path):
        self.filepath = filepath
        self.lines: list[str] = []
        self.current_line = 0
        self.page_num = 0

    def new_page(self):
        self.page_num += 1
        self.current_line = 0

    def write(self, text: str, cc: str = " "):
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


def produce_tabulate_report(loan3: pl.DataFrame, rdate: str,
                             writer: ReportWriter) -> None:
    """
    PROC TABULATE DATA=LOAN3 FORMAT=COMMA14.2 MISSING NOSEPS;
    ** TITLE3 '(BASED ON DEPRECIATED PP FOR UNSCHEDULED GOODS)';
    """
    title1 = "PUBLIC ISLAMIC BANK - (NPL FROM 3 MONTHS & ABOVE) - NEW"
    title2 = f"MOVEMENTS OF SPECIFIC PROVISION FOR THE MONTH ENDING {rdate} (EXISTING AND CURRENT)"
    # ** TITLE3 '(BASED ON DEPRECIATED PP FOR UNSCHEDULED GOODS)';  -- commented in original

    num_vars = ["CURBAL", "UHC", "NETBAL", "IIS", "OSPRIN", "OTHERFEE",
                "MARKETVL", "NETEXP", "SPP2", "SPPL", "RECOVER", "SPPW", "SP"]
    col_labels = {
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
    loan_types = sorted(df["LOANTYP"].unique().to_list())
    risks      = ["SUBSTANDARD-1", "SUBSTANDARD 2", "DOUBTFUL", "BAD"]
    hdr_width  = 29

    for lt in loan_types:
        lt_df = df.filter(pl.col("LOANTYP") == lt)
        writer.write(f"{'RISK        BRANCH':<{hdr_width}} {'NO OF ACCOUNT':>13} " +
                     "  ".join(f"{col_labels[v][:20]:>22}" for v in num_vars[:3]), " ")
        writer.write("-" * 200, " ")
        grand_sums = {v: 0.0 for v in num_vars}; grand_n = 0
        for risk in risks:
            r_df = lt_df.filter(pl.col("RISK") == risk)
            branches  = sorted(r_df["BRANCH"].unique().to_list())
            risk_sums = {v: 0.0 for v in num_vars}; risk_n = 0
            for br in branches:
                b_df = r_df.filter(pl.col("BRANCH") == br)
                n    = len(b_df)
                sums = {v: b_df[v].fill_null(0).sum() for v in num_vars}
                lbl  = f"{risk[:14]:<14} {br[:14]:<14}"
                line = (f"{lbl:<{hdr_width}} {format_count(n):>13} " +
                        "  ".join(format_num(sums[v]) for v in num_vars))
                writer.write(line, " ")
                for v in num_vars:
                    risk_sums[v] += sums[v]
                risk_n += n
            lbl  = f"{risk[:14]:<14} {'SUB-TOTAL':<14}"
            line = (f"{lbl:<{hdr_width}} {format_count(risk_n):>13} " +
                    "  ".join(format_num(risk_sums[v]) for v in num_vars))
            writer.write(line, "0")
            for v in num_vars:
                grand_sums[v] += risk_sums[v]
            grand_n += risk_n
        lbl  = f"{'TOTAL':<{hdr_width}}"
        line = (f"{lbl} {format_count(grand_n):>13} " +
                "  ".join(format_num(grand_sums[v]) for v in num_vars))
        writer.write(line, "0")
        writer.write("", " ")


def _write_tabulate_by_branch(df: pl.DataFrame, num_vars: list,
                               col_labels: dict, writer: ReportWriter) -> None:
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


def produce_detail_report(loan3: pl.DataFrame, rdate: str,
                           writer: ReportWriter) -> None:
    """
    PROC PRINT LABEL N;
    BY LOANTYP BRANCH RISK; PAGEBY BRANCH; SUMBY RISK;
    /* DISCONTINUE AS PER LETTER DATED 26/08/03 FR STATISTICS  */
    """
    title1 = "PUBLIC ISLAMIC BANK - (NPL FROM 3 MONTHS & ABOVE) - NEW"
    title2 = f"MOVEMENTS OF SPECIFIC PROVISION FOR THE MONTH ENDING {rdate} (EXISTING AND CURRENT)"
    # ** TITLE3 '(BASED ON DEPRECIATED PP FOR UNSCHEDULED GOODS)';  -- commented in original

    num_vars = ["NETPROC", "CURBAL", "UHC", "NETBAL", "OTHERFEE",
                "IIS", "OSPRIN", "MARKETVL", "NETEXP",
                "SPP2", "SPPL", "RECOVER", "SPPW", "SP"]

    sorted_df   = loan3.sort(["LOANTYP", "BRANCH", "RISK", "DAYS", "ACCTNO"])
    prev_branch = None; prev_loantyp = None; prev_risk = None
    risk_sums   = {v: 0.0 for v in num_vars}; total_n = 0
    total_sums  = {v: 0.0 for v in num_vars}

    for row in sorted_df.to_dicts():
        loantyp = row.get("LOANTYP", "") or ""
        branch  = row.get("BRANCH", "") or ""
        risk    = row.get("RISK", "") or ""

        if branch != prev_branch or loantyp != prev_loantyp:
            writer.new_page()
            writer.title_block(title1, title2)
            writer.write(f"LOAN TYPE: {loantyp}  BRANCH: {branch}", " ")
            header = (f"{'MNI ACCOUNT NO':<20} {'NAME':<25} {'VINNO':<15} "
                      f"{'DAYS':>5} {'STAT':<5} " +
                      " ".join(f"{v[:12]:>14}" for v in num_vars))
            writer.write(header, " ")
            writer.write("-" * 200, " ")

        if risk != prev_risk and prev_risk is not None:
            sumline = (f"{'RISK TOTAL: ' + prev_risk:<70} " +
                       " ".join(format_num(risk_sums[v]) for v in num_vars))
            writer.write(sumline, "0")
            risk_sums = {v: 0.0 for v in num_vars}

        acctno = row.get("ACCTNO", "") or ""
        name   = (row.get("NAME", "") or "")[:25]
        vinno  = (row.get("VINNO", "") or "")[:15]
        days   = row.get("DAYS", 0) or 0
        bstat  = row.get("BORSTAT", "") or ""
        line   = (f"{str(acctno):<20} {name:<25} {vinno:<15} "
                  f"{days:>5} {bstat:<5} " +
                  " ".join(format_num(row.get(v, 0)) for v in num_vars))
        writer.write(line, " ")
        for v in num_vars:
            val = row.get(v, 0) or 0
            risk_sums[v]  += val
            total_sums[v] += val
        total_n += 1
        prev_loantyp = loantyp; prev_branch = branch; prev_risk = risk

    if prev_risk is not None:
        sumline = (f"{'RISK TOTAL: ' + prev_risk:<70} " +
                   " ".join(format_num(risk_sums[v]) for v in num_vars))
        writer.write(sumline, "0")

    totline = (f"{'GRAND TOTAL':<70} " +
               " ".join(format_num(total_sums[v]) for v in num_vars))
    writer.write(totline, "0")
    writer.write(f"N = {total_n}", " ")


# =============================================================================
# MAIN
# =============================================================================
def main():
    _, macro_vars = load_reptdate()
    reptdate_val: date = macro_vars["REPTDATE"]
    reptmon  = macro_vars["REPTMON"]
    prevmon  = macro_vars["PREVMON"]
    rdate    = macro_vars["RDATE"]

    loanwoff = build_loanwoff(reptmon)

    loan1 = calc_loan1(loanwoff, reptdate_val)
    loan2 = calc_loan2(loanwoff, reptdate_val)

    loan1, loan2 = apply_monthly(loan1, loan2, reptmon, prevmon, reptdate_val)

    loan3 = build_loan3(loan1, loan2)

    sp2_out = str(SP2_OUT_TMPL).format(reptmon=reptmon)
    loan3.write_parquet(sp2_out)
    loan3.write_parquet(str(SP2_LATEST))

    writer = ReportWriter(REPORT_FILE)

    # %TBLS (I=3)
    produce_tabulate_report(loan3, rdate, writer)

    # /* DISCONTINUE AS PER LETTER DATED 26/08/03 FR STATISTICS  */
    produce_detail_report(loan3, rdate, writer)

    writer.flush()
    print(f"Report written to: {REPORT_FILE}")
    print(f"SP2 dataset saved to: {sp2_out}")
    print(f"SP2 latest saved to:  {SP2_LATEST}")


if __name__ == "__main__":
    main()
