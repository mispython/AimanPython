#!/usr/bin/env python3
"""
Program  : DEPM500K.py
Title    : DEPOSIT MOVEMENT OF 500K AND ABOVE
Created  : (original)
Modified : ESMR07-1607
Purpose  : Produces three fixed-width ASA reports for Public Islamic Bank Berhad
           covering deposit movements of RM 500,000 and above in the reporting month:

           1. SAVING DEPOSITS MOVEMENT (SDMVNT)  — Report ID: DEPM500K (saving section)
              If no records: prints a "no customer" notice.
              If records exist: lists ACCTNO, NAME, OLDBAL, NEWBAL, CHANGE (RM million).

           2. DEMAND DEPOSITS CREDIT MOVEMENT (CRMOVE) — DDMVNT credit side
              Columns: BRANCH, BRCH (branch abbreviation), ACCTNO, CUSTNAME,
                       OLDBAL, NEWBAL, MOVEMENT (RM million).
              Enriched with CISCADP customer names (SECCUST=901) and
              BRHFILE branch abbreviations.

           3. DEMAND DEPOSITS OD MOVEMENT (ODMOVE) — DDMVNT overdraft side
              Columns: BRCH, ACCTNO, NAME, OLDBAL, NEWBAL, MOVEMENT (RM million).

           Output: three .txt files with ASA carriage-control characters,
                   60 lines per page, LS=132.

           OPTIONS NODATE NONUMBER NOCENTER MISSING=0

Dependencies:
    PBBDPFMT — imported for format completeness (%INC PGM(PBBDPFMT,PBMISFMT)).
               No PBBDPFMT format functions are directly called in this program.
    PBMISFMT — format_brchcd() is used to apply BRCHCD. format to BRANCH
               (BRCH = PUT(BRANCH, BRCHCD.)).
"""

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, timedelta

# PBBDPFMT imported for format completeness per %INC PGM(PBBDPFMT,PBMISFMT).
# No PBBDPFMT format functions are directly called in this program.
# from PBBDPFMT import ...

from PBMISFMT import format_brchcd   # PUT(BRANCH, BRCHCD.)

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR     = Path(__file__).resolve().parent
DATA_DIR     = BASE_DIR / "data"

# BNM library  -> SAP.PIBB.BNM (REPTDATE)
BNM_DIR      = DATA_DIR / "bnm"

# MIS library  -> SAP.PIBB.MIS.D&REPTYEAR  (SDMVNT, DDMVNT parquet files)
# Resolved at runtime after REPTYEAR is known; placeholder set here.
MIS_DIR      = DATA_DIR / "mis"

# CISCADP library -> CISCADP.DEPOSIT parquet
CISCADP_DIR  = DATA_DIR / "ciscadp"

# Branch flat file (DD BRHFILE, LRECL=80)
BRHFILE      = DATA_DIR / "BRHFILE.txt"

OUTPUT_DIR   = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Output report files
REPORT_SAVING = OUTPUT_DIR / "DEPM500K_SAVING.txt"
REPORT_CRMOVE = OUTPUT_DIR / "DEPM500K_CRMOVE.txt"
REPORT_ODMOVE = OUTPUT_DIR / "DEPM500K_ODMOVE.txt"

# ============================================================================
# REPORT CONSTANTS   (OPTIONS LS=132 PS=60 NOCENTER)
# ============================================================================

PAGE_LENGTH  = 60
LINE_WIDTH   = 132

REPORT_ID    = "DEPM500K"
INSTITUTION  = "PUBLIC ISLAMIC BANK BERHAD"

# ============================================================================
# ASA CARRIAGE-CONTROL HELPERS
# ============================================================================

ASA_NEWPAGE = "1"   # new page
ASA_SPACE1  = " "   # single space
ASA_SPACE2  = "0"   # double space (skip one line)


def asa(ctl: str, text: str) -> str:
    return f"{ctl}{text}\n"


# ============================================================================
# NUMBER FORMATTING HELPERS
# ============================================================================

def fmt_20(value) -> str:
    """FORMAT=20. — right-justified 20-char integer (no commas)."""
    if value is None:
        return " " * 20
    return f"{int(value):>20}"


def fmt_d25(value) -> str:
    """FORMAT=$25. — left-justified 25-char string."""
    if value is None:
        return " " * 25
    return f"{str(value):<25.25}"


def fmt_d40(value) -> str:
    """FORMAT=$40. — left-justified 40-char string."""
    if value is None:
        return " " * 40
    return f"{str(value):<40.40}"


def fmt_d6(value) -> str:
    """FORMAT=$6. — left-justified 6-char string."""
    if value is None:
        return " " * 6
    return f"{str(value):<6.6}"


def fmt_6(value) -> str:
    """FORMAT=6. — right-justified 6-char integer."""
    if value is None:
        return " " * 6
    return f"{int(value):>6}"


def fmt_comma16_2(value) -> str:
    """FORMAT=COMMA16.2 — right-justified 16-char float with commas."""
    if value is None:
        return " " * 16
    return f"{float(value):>16,.2f}"


def fmt_14_1(value) -> str:
    """FORMAT=14.1 — right-justified 14-char float, 1 decimal."""
    if value is None:
        return " " * 14
    return f"{float(value):>14.1f}"


def fmt_10_1(value) -> str:
    """FORMAT=10.1 — right-justified 10-char float, 1 decimal."""
    if value is None:
        return " " * 10
    return f"{float(value):>10.1f}"


# ============================================================================
# STEP 1: REPTDATE  —  derive REPTMON, REPTYEAR, RDATE
# DATA REPTDATE; SET BNM.REPTDATE;
#   CALL SYMPUT('REPTMON',  PUT(MONTH(REPTDATE), Z2.));
#   CALL SYMPUT('REPTYEAR', PUT(REPTDATE, YEAR4.));
#   CALL SYMPUT('RDATE',    PUT(REPTDATE, DDMMYY8.));
# ============================================================================

def derive_report_params(bnm_dir: Path) -> dict:
    reptdate_file = bnm_dir / "REPTDATE.parquet"
    con = duckdb.connect()
    row = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{reptdate_file}') LIMIT 1"
    ).fetchone()
    con.close()

    if row is None:
        raise RuntimeError(f"No rows in {reptdate_file}")

    val = row[0]
    if isinstance(val, (int, float)):
        reptdate = date(1960, 1, 1) + timedelta(days=int(val))
    elif isinstance(val, date):
        reptdate = val
    else:
        from datetime import datetime
        reptdate = datetime.strptime(str(val)[:10], "%Y-%m-%d").date()

    return {
        "REPTMON"  : f"{reptdate.month:02d}",
        "REPTYEAR" : str(reptdate.year),
        "RDATE"    : reptdate.strftime("%d/%m/%Y"),
        "reptdate" : reptdate,
    }


# ============================================================================
# STEP 2: BRHDATA  —  branch reference flat file
# DATA BRHDATA; INFILE BRHFILE LRECL=80;
#   INPUT @2 BRANCH 3. @6 BRABBR $3.;
# ============================================================================

def load_brhdata(brhfile: Path) -> pl.DataFrame:
    """
    Fixed-width: BRANCH at @2 (3 chars), BRABBR at @6 (3 chars).
    SAS 1-based positions map to Python 0-based: [1:4] and [5:8].
    """
    records = []
    with open(brhfile, "r") as fh:
        for line in fh:
            if len(line) < 8:
                continue
            branch_str = line[1:4].strip()
            brabbr_str = line[5:8].strip()
            if branch_str.isdigit():
                records.append({"BRANCH": int(branch_str), "BRABBR": brabbr_str})
    return pl.DataFrame(records, schema={"BRANCH": pl.Int64, "BRABBR": pl.Utf8})


# ============================================================================
# SAVING MACRO  — %MACRO SAVING
# ============================================================================

def run_saving(mis_dir: Path, reptmon: str, reptyear: str) -> None:
    """
    Equivalent to %MACRO SAVING / %SAVING.

    Reads MIS.SDMVNT&REPTMON. If empty, prints notice. Otherwise writes
    the saving deposits movement report with ACCTNO, NAME, OLDBAL, NEWBAL, CHANGE.
    """
    sdmvnt_file = mis_dir / f"SDMVNT{reptmon}.parquet"

    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{sdmvnt_file}')").pl()
    con.close()

    sdmvnt_n = len(df)   # CALL SYMPUT('SDMVNT', N) — NOBS count

    title3 = "SAVING DEPOSITS MOVEMENT OF 500K AND ABOVE"
    title4 = f"IN THE MONTH OF {reptmon}/{reptyear}"

    with open(REPORT_SAVING, "w") as fh:
        # TITLE1, TITLE2, TITLE3, TITLE4
        fh.write(asa(ASA_NEWPAGE, REPORT_ID))
        fh.write(asa(ASA_SPACE1,  INSTITUTION))
        fh.write(asa(ASA_SPACE1,  title3))
        fh.write(asa(ASA_SPACE1,  title4))
        fh.write(asa(ASA_SPACE1,  ""))

        if sdmvnt_n == 0:
            # %IF &SDMVNT EQ 0 %THEN %DO;  DATA _NULL_; FILE PRINT; PUT; PUT; PUT @5 '***...';
            fh.write(asa(ASA_SPACE1, ""))
            fh.write(asa(ASA_SPACE1, ""))
            bar = "*" * 50
            fh.write(asa(ASA_SPACE1, f"     {bar}"))
            fh.write(asa(ASA_SPACE1,  "     NO CUSTOMER WITH MOVEMENT OF 500K      AND ABOVE"))
            fh.write(asa(ASA_SPACE1, f"     IN THE MONTH OF {reptmon}/{reptyear}"))
            fh.write(asa(ASA_SPACE1, f"     {bar}"))
            print(f"SAVING: no records for {reptmon}/{reptyear}.")
        else:
            # %ELSE %DO; PROC REPORT DATA=MIS.SDMVNT&REPTMON ...
            # Columns: ACCTNO NAME OLDBAL NEWBAL CHANGE
            # CHANGE = (INT(ROUND(SUM(NEWBAL.SUM,(-1)*OLDBAL.SUM),100000)/100000))/10
            # i.e. net change rounded to nearest 100,000, converted to RM million (/10 of 100k)

            # Column headers (HEADLINE, SPLIT='*')
            h1 = (
                f"{'ACCOUNT NUMBER':>20}  "
                f"{'NAME OF CUSTOMER':<25}  "
                f"{'LAST MONTH BALANCE':>16}  "
                f"{'THIS MONTH BALANCE':>16}  "
                f"{'INCREASE/':>14}"
            )
            h2 = (
                f"{'':>20}  "
                f"{'':>25}  "
                f"{'':>16}  "
                f"{'':>16}  "
                f"{'DECREASE':>14}"
            )
            h3 = (
                f"{'':>20}  "
                f"{'':>25}  "
                f"{'':>16}  "
                f"{'':>16}  "
                f"{'(RM MILLION)':>14}"
            )
            uline = "-" * (20 + 2 + 25 + 2 + 16 + 2 + 16 + 2 + 14)
            fh.write(asa(ASA_SPACE2, h1))
            fh.write(asa(ASA_SPACE1, h2))
            fh.write(asa(ASA_SPACE1, h3))
            fh.write(asa(ASA_SPACE1, uline))

            line_count  = 7
            page_num    = 1
            sum_oldbal  = 0.0
            sum_newbal  = 0.0

            for row in df.to_dicts():
                if line_count >= PAGE_LENGTH:
                    fh.write(asa(ASA_NEWPAGE, REPORT_ID))
                    fh.write(asa(ASA_SPACE1,  INSTITUTION))
                    fh.write(asa(ASA_SPACE1,  title3))
                    fh.write(asa(ASA_SPACE1,  title4))
                    fh.write(asa(ASA_SPACE1,  ""))
                    fh.write(asa(ASA_SPACE2,  h1))
                    fh.write(asa(ASA_SPACE1,  h2))
                    fh.write(asa(ASA_SPACE1,  h3))
                    fh.write(asa(ASA_SPACE1,  uline))
                    line_count = 8

                acctno  = row.get("ACCTNO")
                name    = row.get("NAME", "") or ""
                oldbal  = float(row.get("OLDBAL", 0) or 0)
                newbal  = float(row.get("NEWBAL", 0) or 0)

                # CHANGE = (INT(ROUND(SUM(NEWBAL.SUM,(-1)*OLDBAL.SUM),100000)/100000))/10
                net    = newbal - oldbal
                change = int(round(net / 100000) * 100000 / 100000) / 10

                line = (
                    f"{fmt_20(acctno)}  "
                    f"{fmt_d25(name)}  "
                    f"{fmt_comma16_2(oldbal)}  "
                    f"{fmt_comma16_2(newbal)}  "
                    f"{fmt_14_1(change)}"
                )
                fh.write(asa(ASA_SPACE1, line))
                line_count  += 1
                sum_oldbal  += oldbal
                sum_newbal  += newbal

            # RBREAK AFTER / PAGE  — page break and grand total
            grand_net    = sum_newbal - sum_oldbal
            grand_change = int(round(grand_net / 100000) * 100000 / 100000) / 10
            fh.write(asa(ASA_SPACE1, uline))
            grand_line = (
                f"{'':>20}  "
                f"{'':>25}  "
                f"{fmt_comma16_2(sum_oldbal)}  "
                f"{fmt_comma16_2(sum_newbal)}  "
                f"{fmt_14_1(grand_change)}"
            )
            fh.write(asa(ASA_SPACE1, grand_line))

    print(f"SAVING report written -> {REPORT_SAVING}  ({sdmvnt_n} records)")


# ============================================================================
# DEMAND DEPOSITS MACRO  — %MACRO DEMDDEPT
# ============================================================================

def load_ciscadp(ciscadp_dir: Path) -> pl.DataFrame:
    """
    DATA CISCADP (KEEP=ACCTNO CUSTNAME);
      SET CISCADP.DEPOSIT; IF SECCUST=901;
    """
    deposit_file = ciscadp_dir / "DEPOSIT.parquet"
    con = duckdb.connect()
    df = con.execute(
        f"SELECT ACCTNO, CUSTNAME FROM read_parquet('{deposit_file}') "
        f"WHERE SECCUST = 901"
    ).pl()
    con.close()
    return df.sort("ACCTNO")


def build_crmove_odmove(mis_dir: Path, reptmon: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    DATA CRMOVE ODMOVE;
      LENGTH BRCH $6.;
      SET MIS.DDMVNT&REPTMON;
      BRCH = PUT(BRANCH, BRCHCD.);
      Routing logic based on OLDBAL / NEWBAL sign.
    """
    ddmvnt_file = mis_dir / f"DDMVNT{reptmon}.parquet"
    con = duckdb.connect()
    src = con.execute(f"SELECT * FROM read_parquet('{ddmvnt_file}')").pl()
    con.close()

    cr_rows: list[dict] = []
    od_rows: list[dict] = []

    for row in src.to_dicts():
        branch  = row.get("BRANCH")
        acctno  = row.get("ACCTNO")
        name    = row.get("NAME", "")
        oldbal  = float(row.get("OLDBAL", 0) or 0)
        newbal  = float(row.get("NEWBAL", 0) or 0)

        # BRCH = PUT(BRANCH, BRCHCD.)
        brch = format_brchcd(branch) if branch is not None else "XXX"

        base = {
            "BRANCH" : branch,
            "BRCH"   : brch,
            "ACCTNO" : acctno,
            "NAME"   : name,
            "OLDBAL" : oldbal,
            "NEWBAL" : newbal,
        }

        if oldbal < 0 and newbal >= 0:
            # OD -> CR transition
            # MOVEMENT = ROUND((0 + OLDBAL)*0.000001,.01)  -> OD output
            mv_od = round((0 + oldbal) * 0.000001, 2)
            od_rows.append({**base, "MOVEMENT": mv_od})
            # MOVEMENT = ROUND((0 + NEWBAL)*0.000001,.01)
            # IF NEWBAL > 0 THEN OUTPUT CRMOVE
            if newbal > 0:
                mv_cr = round((0 + newbal) * 0.000001, 2)
                cr_rows.append({**base, "MOVEMENT": mv_cr})

        elif oldbal >= 0 and newbal < 0:
            # CR -> OD transition
            # MOVEMENT = ROUND((0 - OLDBAL)*0.000001,.01); IF OLDBAL > 0 THEN OUTPUT CRMOVE
            mv_cr = round((0 - oldbal) * 0.000001, 2)
            if oldbal > 0:
                cr_rows.append({**base, "MOVEMENT": mv_cr})
            # MOVEMENT = ROUND((0 - NEWBAL)*0.000001,.01); OUTPUT ODMOVE
            mv_od = round((0 - newbal) * 0.000001, 2)
            od_rows.append({**base, "MOVEMENT": mv_od})

        elif oldbal < 0 and newbal < 0:
            # Both OD: MOVEMENT = ROUND((OLDBAL - NEWBAL)*0.000001,.01)
            mv_od = round((oldbal - newbal) * 0.000001, 2)
            od_rows.append({**base, "MOVEMENT": mv_od})

        else:
            # Both >= 0: MOVEMENT = ROUND((NEWBAL - OLDBAL)*0.000001,.01); OUTPUT CRMOVE
            mv_cr = round((newbal - oldbal) * 0.000001, 2)
            cr_rows.append({**base, "MOVEMENT": mv_cr})

    schema_cr = {
        "BRANCH": pl.Int64, "BRCH": pl.Utf8, "ACCTNO": pl.Int64,
        "NAME": pl.Utf8, "OLDBAL": pl.Float64, "NEWBAL": pl.Float64,
        "MOVEMENT": pl.Float64,
    }
    schema_od = {
        "BRANCH": pl.Int64, "BRCH": pl.Utf8, "ACCTNO": pl.Int64,
        "NAME": pl.Utf8, "OLDBAL": pl.Float64, "NEWBAL": pl.Float64,
        "MOVEMENT": pl.Float64,
    }
    crmove = pl.DataFrame(cr_rows, schema=schema_cr) if cr_rows else pl.DataFrame(schema=schema_cr)
    odmove = pl.DataFrame(od_rows, schema=schema_od) if od_rows else pl.DataFrame(schema=schema_od)

    return crmove, odmove


def enrich_crmove(
    crmove: pl.DataFrame,
    ciscadp: pl.DataFrame,
    brhdata: pl.DataFrame,
) -> pl.DataFrame:
    """
    PROC SORT CRMOVE BY ACCTNO;
    MERGE CRMOVE(IN=A) CISCADP(IN=B) BY ACCTNO; IF A;
    PROC SORT CRMOVE BY BRANCH;
    MERGE CRMOVE(IN=A) BRHDATA(IN=B) BY BRANCH; IF A;
    (CISCADP brings CUSTNAME; BRHDATA brings BRABBR — already have BRCH from BRCHCD.)
    """
    # Merge with CISCADP by ACCTNO (left join, keep only CRMOVE rows)
    crmove = crmove.sort("ACCTNO").join(
        ciscadp.sort("ACCTNO"), on="ACCTNO", how="left"
    )

    # Merge with BRHDATA by BRANCH (left join, keep only CRMOVE rows)
    crmove = crmove.sort("BRANCH").join(
        brhdata.sort("BRANCH"), on="BRANCH", how="left"
    )

    return crmove


def _write_dline(fh, width: int) -> None:
    fh.write(asa(ASA_SPACE1, "=" * width))


def _write_sline(fh, width: int) -> None:
    fh.write(asa(ASA_SPACE1, "-" * width))


def write_crmove_report(
    df: pl.DataFrame,
    reptmon: str,
    reptyear: str,
) -> None:
    """
    PROC REPORT DATA=CRMOVE NOWD HEADLINE SPLIT='*';
      COLUMN BRANCH BRCH ACCTNO CUSTNAME OLDBAL NEWBAL MOVEMENT;
      RBREAK AFTER / PAGE DUL OL SUMMARIZE;
    """
    title3 = "DEMAND DEPOSITS CREDIT MOVEMENT OF 500K AND ABOVE"
    title4 = "IN THEIR CURRENT ACCOUNTS"
    title5 = f"IN THE MONTH OF {reptmon}/{reptyear}"

    # Column widths matching DEFINE FORMAT sizes
    W_BRANCH   =  6    # FORMAT=6.
    W_BRCH     =  6    # FORMAT=$6.
    W_ACCTNO   = 20    # FORMAT=20.
    W_CUSTNAME = 40    # FORMAT=$40.
    W_OLDBAL   = 16    # COMMA16.2
    W_NEWBAL   = 16    # COMMA16.2
    W_MOVEMENT = 10    # FORMAT=10.1
    SEP        =  2

    row_width = (W_BRANCH + SEP + W_BRCH + SEP + W_ACCTNO + SEP +
                 W_CUSTNAME + SEP + W_OLDBAL + SEP + W_NEWBAL + SEP + W_MOVEMENT)

    h1 = (
        f"{'BRANCH':>{W_BRANCH}}"
        f"  {'BRABBR':<{W_BRCH}}"
        f"  {'ACCOUNT NUMBER':>{W_ACCTNO}}"
        f"  {'NAME OF CUSTOMER':<{W_CUSTNAME}}"
        f"  {'LAST MONTH BALANCE':>{W_OLDBAL}}"
        f"  {'THIS MONTH BALANCE':>{W_NEWBAL}}"
        f"  {'INCREASE/':>{W_MOVEMENT}}"
    )
    h2 = (
        f"{'':>{W_BRANCH}}"
        f"  {'':>{W_BRCH}}"
        f"  {'':>{W_ACCTNO}}"
        f"  {'':>{W_CUSTNAME}}"
        f"  {'':>{W_OLDBAL}}"
        f"  {'':>{W_NEWBAL}}"
        f"  {'DECREASE':>{W_MOVEMENT}}"
    )
    h3 = (
        f"{'':>{W_BRANCH}}"
        f"  {'':>{W_BRCH}}"
        f"  {'':>{W_ACCTNO}}"
        f"  {'':>{W_CUSTNAME}}"
        f"  {'':>{W_OLDBAL}}"
        f"  {'':>{W_NEWBAL}}"
        f"  {'(RM MILLION)':>{W_MOVEMENT}}"
    )
    uline = "-" * row_width
    dline = "=" * row_width

    crmove_n = len(df)

    with open(REPORT_CRMOVE, "w") as fh:
        def write_titles():
            fh.write(asa(ASA_NEWPAGE, REPORT_ID))
            fh.write(asa(ASA_SPACE1,  INSTITUTION))
            fh.write(asa(ASA_SPACE1,  title3))
            fh.write(asa(ASA_SPACE1,  title4))
            fh.write(asa(ASA_SPACE1,  title5))
            fh.write(asa(ASA_SPACE1,  ""))
            fh.write(asa(ASA_SPACE2,  h1))
            fh.write(asa(ASA_SPACE1,  h2))
            fh.write(asa(ASA_SPACE1,  h3))
            fh.write(asa(ASA_SPACE1,  uline))

        if crmove_n == 0:
            # %IF &CRMOVE EQ 0 %THEN %DO;
            write_titles()
            fh.write(asa(ASA_SPACE1, ""))
            fh.write(asa(ASA_SPACE1, ""))
            bar = "*" * 56
            fh.write(asa(ASA_SPACE1, f"     {bar}"))
            fh.write(asa(ASA_SPACE1,  "     NO CUSTOMER WITH CREDIT MOVEMENT OF 500K      AND ABOVE"))
            fh.write(asa(ASA_SPACE1, f"     IN THE MONTH OF {reptmon}/{reptyear}"))
            fh.write(asa(ASA_SPACE1, f"     {bar}"))
        else:
            write_titles()
            line_count = 9

            sum_oldbal   = 0.0
            sum_newbal   = 0.0
            sum_movement = 0.0

            for row in df.to_dicts():
                if line_count >= PAGE_LENGTH:
                    write_titles()
                    line_count = 9

                branch   = row.get("BRANCH")
                brch     = row.get("BRCH", "") or ""
                acctno   = row.get("ACCTNO")
                custname = row.get("CUSTNAME", "") or ""
                oldbal   = float(row.get("OLDBAL",   0) or 0)
                newbal   = float(row.get("NEWBAL",   0) or 0)
                movement = float(row.get("MOVEMENT", 0) or 0)

                line = (
                    f"{fmt_6(branch)}"
                    f"  {fmt_d6(brch)}"
                    f"  {fmt_20(acctno)}"
                    f"  {fmt_d40(custname)}"
                    f"  {fmt_comma16_2(oldbal)}"
                    f"  {fmt_comma16_2(newbal)}"
                    f"  {fmt_10_1(movement)}"
                )
                fh.write(asa(ASA_SPACE1, line))
                line_count   += 1
                sum_oldbal   += oldbal
                sum_newbal   += newbal
                sum_movement += movement

            # RBREAK AFTER / PAGE DUL OL SUMMARIZE
            # DUL = double underline, OL = overline before total, SUMMARIZE = grand total
            fh.write(asa(ASA_SPACE1, dline))
            fh.write(asa(ASA_SPACE1, dline))
            grand = (
                f"{'':>{W_BRANCH}}"
                f"  {'':>{W_BRCH}}"
                f"  {'':>{W_ACCTNO}}"
                f"  {'':>{W_CUSTNAME}}"
                f"  {fmt_comma16_2(sum_oldbal)}"
                f"  {fmt_comma16_2(sum_newbal)}"
                f"  {fmt_10_1(sum_movement)}"
            )
            fh.write(asa(ASA_SPACE1, grand))

    print(f"CRMOVE report written -> {REPORT_CRMOVE}  ({crmove_n} records)")


def write_odmove_report(
    df: pl.DataFrame,
    reptmon: str,
    reptyear: str,
) -> None:
    """
    PROC REPORT DATA=ODMOVE NOWD HEADLINE SPLIT='*';
      COLUMN BRCH ACCTNO NAME OLDBAL NEWBAL MOVEMENT;
      RBREAK AFTER / PAGE DUL OL SUMMARIZE;
    """
    title3 = "DEMAND DEPOSITS OD MOVEMENT OF 500K      AND ABOVE"
    title4 = "IN THEIR CURRENT ACCOUNTS"
    title5 = f"IN THE MONTH OF {reptmon}/{reptyear}"

    W_BRCH     =  6    # FORMAT=$6.
    W_ACCTNO   = 20    # FORMAT=20.
    W_NAME     = 25    # FORMAT=$25.
    W_OLDBAL   = 16    # COMMA16.2
    W_NEWBAL   = 16    # COMMA16.2
    W_MOVEMENT = 10    # FORMAT=10.1
    SEP        =  2

    row_width = (W_BRCH + SEP + W_ACCTNO + SEP + W_NAME + SEP +
                 W_OLDBAL + SEP + W_NEWBAL + SEP + W_MOVEMENT)

    h1 = (
        f"{'BRANCH':<{W_BRCH}}"
        f"  {'ACCOUNT NUMBER':>{W_ACCTNO}}"
        f"  {'NAME OF CUSTOMER':<{W_NAME}}"
        f"  {'LAST MONTH BALANCE':>{W_OLDBAL}}"
        f"  {'THIS MONTH BALANCE':>{W_NEWBAL}}"
        f"  {'INCREASE/':>{W_MOVEMENT}}"
    )
    h2 = (
        f"{'':>{W_BRCH}}"
        f"  {'':>{W_ACCTNO}}"
        f"  {'':>{W_NAME}}"
        f"  {'':>{W_OLDBAL}}"
        f"  {'':>{W_NEWBAL}}"
        f"  {'DECREASE':>{W_MOVEMENT}}"
    )
    h3 = (
        f"{'':>{W_BRCH}}"
        f"  {'':>{W_ACCTNO}}"
        f"  {'':>{W_NAME}}"
        f"  {'':>{W_OLDBAL}}"
        f"  {'':>{W_NEWBAL}}"
        f"  {'(RM MILLION)':>{W_MOVEMENT}}"
    )
    uline = "-" * row_width
    dline = "=" * row_width

    odmove_n = len(df)

    with open(REPORT_ODMOVE, "w") as fh:
        def write_titles():
            fh.write(asa(ASA_NEWPAGE, REPORT_ID))
            fh.write(asa(ASA_SPACE1,  INSTITUTION))
            fh.write(asa(ASA_SPACE1,  title3))
            fh.write(asa(ASA_SPACE1,  title4))
            fh.write(asa(ASA_SPACE1,  title5))
            fh.write(asa(ASA_SPACE1,  ""))
            fh.write(asa(ASA_SPACE2,  h1))
            fh.write(asa(ASA_SPACE1,  h2))
            fh.write(asa(ASA_SPACE1,  h3))
            fh.write(asa(ASA_SPACE1,  uline))

        if odmove_n == 0:
            # %IF &ODMOVE EQ 0 %THEN %DO;
            write_titles()
            fh.write(asa(ASA_SPACE1, ""))
            fh.write(asa(ASA_SPACE1, ""))
            bar = "*" * 51
            fh.write(asa(ASA_SPACE1, f"     {bar}"))
            fh.write(asa(ASA_SPACE1,  "     NO CUSTOMER WITH OD MOVEMENT OF 500K      AND ABOVE"))
            fh.write(asa(ASA_SPACE1, f"     IN THE MONTH OF {reptmon}/{reptyear}"))
            fh.write(asa(ASA_SPACE1, f"     {bar}"))
        else:
            write_titles()
            line_count = 9

            sum_oldbal   = 0.0
            sum_newbal   = 0.0
            sum_movement = 0.0

            for row in df.to_dicts():
                if line_count >= PAGE_LENGTH:
                    write_titles()
                    line_count = 9

                brch     = row.get("BRCH", "") or ""
                acctno   = row.get("ACCTNO")
                name     = row.get("NAME", "") or ""
                oldbal   = float(row.get("OLDBAL",   0) or 0)
                newbal   = float(row.get("NEWBAL",   0) or 0)
                movement = float(row.get("MOVEMENT", 0) or 0)

                line = (
                    f"{fmt_d6(brch)}"
                    f"  {fmt_20(acctno)}"
                    f"  {fmt_d25(name)}"
                    f"  {fmt_comma16_2(oldbal)}"
                    f"  {fmt_comma16_2(newbal)}"
                    f"  {fmt_10_1(movement)}"
                )
                fh.write(asa(ASA_SPACE1, line))
                line_count   += 1
                sum_oldbal   += oldbal
                sum_newbal   += newbal
                sum_movement += movement

            # RBREAK AFTER / PAGE DUL OL SUMMARIZE
            fh.write(asa(ASA_SPACE1, dline))
            fh.write(asa(ASA_SPACE1, dline))
            grand = (
                f"{'':>{W_BRCH}}"
                f"  {'':>{W_ACCTNO}}"
                f"  {'':>{W_NAME}}"
                f"  {fmt_comma16_2(sum_oldbal)}"
                f"  {fmt_comma16_2(sum_newbal)}"
                f"  {fmt_10_1(sum_movement)}"
            )
            fh.write(asa(ASA_SPACE1, grand))

    print(f"ODMOVE report written -> {REPORT_ODMOVE}  ({odmove_n} records)")


# ============================================================================
# MAIN
# ============================================================================

def main():
    # Step 1: Reporting parameters
    params   = derive_report_params(BNM_DIR)
    reptmon  = params["REPTMON"]
    reptyear = params["REPTYEAR"]

    # LIBNAME MIS "SAP.PIBB.MIS.D&REPTYEAR" — resolve MIS path with year
    mis_dir = MIS_DIR / reptyear   # e.g. data/mis/2026
    if not mis_dir.exists():
        mis_dir = MIS_DIR           # fall back to base MIS dir

    # Step 2: %SAVING
    run_saving(mis_dir, reptmon, reptyear)

    # Step 3: %DEMDDEPT — demand deposits

    # DATA CRMOVE ODMOVE — route DDMVNT rows by balance sign
    crmove, odmove = build_crmove_odmove(mis_dir, reptmon)

    # PROC SORT CRMOVE BY ACCTNO + MERGE with CISCADP (SECCUST=901) + MERGE with BRHDATA
    ciscadp = load_ciscadp(CISCADP_DIR)
    brhdata = load_brhdata(BRHFILE)
    crmove  = enrich_crmove(crmove, ciscadp, brhdata)

    # Write CRMOVE report
    write_crmove_report(crmove, reptmon, reptyear)

    # Write ODMOVE report
    write_odmove_report(odmove, reptmon, reptyear)


if __name__ == "__main__":
    main()
