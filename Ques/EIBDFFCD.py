#!/usr/bin/env python3
"""
Program : EIBDFFCD.py
Purpose : Daily foreign currency (FCY) fixed deposit conversion.
          Adds FORATE, FORBAL and CURBALUS columns to the daily FD certificate
          enrichment dataset (enrh_dp_fd_cert_d{REPDD}.sas7bdat) and converts
          CURBAL to MYR for non-MYR currencies. The existing dataset is
          replaced in place. On month end the same output is also written as
          enrh_dp_fd_cert_m{REPMM}.sas7bdat.

          The calculation runs inside SAS through saspy (original DATA step),
          so the dataset stays in native .sas7bdat format with its existing
          attributes preserved.

          SMR 2006-229. MNI3 / 10363.
"""

import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import saspy

# =============================================================================
# SAS JCL / DATA STEP HEADER (kept as comments)
# =============================================================================
# //EIBDFFCD JOB MISEIS,EIBWFFCY,CLASS=A,MSGCLASS=X,MSGLEVEL=(1,1),
# //         NOTIFY=&SYSUID,REGION=8M
# //* SMR 2006-229. MNI3 / 10363.
# //EIBDFFCD  EXEC SAS609
# //FD        DD DSN=SAP.PBB.MNIFD.DAILY(0),DISP=OLD
# //FCYFD     DD DSN=SAP.PBB.FCYFD,DISP=SHR
# //PGM       DD DSN=SAP.BNM.PROGRAM,DISP=SHR
#
# OPTIONS YEARCUTOFF=1950;
# PROC FORMAT LIB=FCYFD CNTLOUT=FOFMT;RUN;
# PROC FORMAT CNTLIN=FOFMT;RUN;
# *;
# PROC SORT DATA=FD.FD OUT=FD;
#    BY ACCTNO CDNO;
# *;
# DATA FD.FD;
#    SET FD;
#    IF CURCODE NE 'MYR' THEN DO;
#       FORATE = PUT(CURCODE,$FORATE.);
#       FORBAL = CURBAL;
#       CURBAL = ROUND(CURBAL * FORATE,.01);
#    END;
#    CURBALUS = CURBAL / PUT('USD',$FORATE.);
# *;
# RUN;
# PROC PRINT;
#   WHERE CURCODE NE 'MYR';

# =============================================================================
# DEPENDENCY NOTES
# =============================================================================
# REPTDATE.py     : not used. The report date follows ctl_dwh_batch_dttm
#                   (source system DP), as required for this program.
# input_date.py   : not used. The daily filename is fixed by the batch day
#                   (d{REPDD}) and the FORATE file has no date token.
# output_date.py  : not used. Output names are fixed by batch day / month
#                   (d{REPDD} / m{REPMM}) and written by SAS.
# GET_BATCH_DATE.py (get_batch_date_dwh): its signature was not provided, so the
#                   control file is read directly in get_batch_date() below.
# PBBDPFMT.py     : not used. The $FORATE. format is rebuilt in SAS from
#                   forate.sas7bdat (PROC FORMAT CNTLIN).

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

# Control file holding the batch date per source system
CTL_BATCH_PATH = BASE_DIR / "input" / "ctl_dwh_batch_dttm.sas7bdat"
BATCH_SOURCE_SYSTEM = "DP"

# MNIFD input (daily FD certificate enrichment) - also the output location
ENRICHMENT_DIR = Path("/sas/deposit/dwh/enrichment")

# FCYFD exchange rate input ($FORATE. format source)
FORATE_DIR = Path("/extdwh/host/forate")
FORATE_PATH = FORATE_DIR / "forate.sas7bdat"
FORATE_MEMBER = "forate"

# saspy configuration name (sascfg_personal.py)
SASPY_CFGNAME = "default"

# =============================================================================
# CONSTANTS
# =============================================================================
NEW_COLUMNS = ("FORATE", "FORBAL", "CURBALUS")
REQUIRED_COLUMNS = ("ACCTNO", "CDNO", "CURCODE", "CURBAL")
SAS_EPOCH = datetime(1960, 1, 1)


# =============================================================================
# BATCH DATE (ctl_dwh_batch_dttm, SOURCE_SYSTEM_CD = DP)
# =============================================================================
def get_batch_date() -> date:
    ctl = pd.read_sas(str(CTL_BATCH_PATH), encoding="latin1")
    ctl.columns = [str(c).upper() for c in ctl.columns]
    ctl["SOURCE_SYSTEM_CD"] = ctl["SOURCE_SYSTEM_CD"].astype(str).str.strip()
    rows = ctl[ctl["SOURCE_SYSTEM_CD"] == BATCH_SOURCE_SYSTEM]
    if rows.empty:
        raise ValueError(
            f"SOURCE_SYSTEM_CD '{BATCH_SOURCE_SYSTEM}' not found in {CTL_BATCH_PATH}"
        )
    value = rows.iloc[0]["BATCH_DTTM"]
    if isinstance(value, (int, float)):  # raw SAS datetime (seconds since 1960)
        return (SAS_EPOCH + timedelta(seconds=float(value))).date()
    return pd.Timestamp(value).date()


def is_month_end(batch_date: date) -> bool:
    return (batch_date + timedelta(days=1)).day == 1


# =============================================================================
# FRESHNESS CHECK
# The daily filename has only the day (d{REPDD}), so last month's file with the
# same day would match. A file is only accepted when its last-modified date is
# on or after the batch date.
# =============================================================================
def assert_fresh(path: Path, batch_date: date, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    modified = datetime.fromtimestamp(path.stat().st_mtime)
    if modified.date() < batch_date:
        raise RuntimeError(
            f"{label} is not ready: {path.name} was last modified "
            f"{modified:%Y-%m-%d %H:%M:%S}, earlier than batch date {batch_date}."
        )
    print(f"[FRESH] {label}: {path.name} modified {modified:%Y-%m-%d %H:%M:%S}")


# =============================================================================
# HEADER INSPECTION (column names / types only, first row read)
# =============================================================================
def read_header(path: Path) -> pd.DataFrame:
    reader = pd.read_sas(str(path), encoding="latin1", chunksize=1, iterator=True)
    try:
        return next(iter(reader))
    except StopIteration as exc:
        raise ValueError(f"{path} contains no rows") from exc


def validate_fd_columns(fd_path: Path) -> None:
    header = read_header(fd_path)
    cols = {str(c).upper() for c in header.columns}

    missing = [c for c in REQUIRED_COLUMNS if c not in cols]
    if missing:
        raise ValueError(f"{fd_path.name} is missing required columns: {missing}")

    # Guard: converting twice would multiply CURBAL by the rate again.
    already = [c for c in NEW_COLUMNS if c in cols]
    if already:
        raise RuntimeError(
            f"{fd_path.name} already contains {already}; it has already been "
            f"converted. Aborting to avoid applying the exchange rate twice."
        )


# =============================================================================
# $FORATE. FORMAT BUILD  (PROC FORMAT ... CNTLOUT / CNTLIN equivalent)
# forate.sas7bdat is loaded into the $FORATE. format. Both layouts supported:
#   - format control layout : FMTNAME / START / LABEL   -> used as CNTLIN directly
#   - plain rate table      : CURCODE + (FORATE | RATE) -> converted to CNTLIN
# =============================================================================
def build_format_code() -> str:
    header = read_header(FORATE_PATH)
    upper = {str(c).upper(): c for c in header.columns}

    if "START" in upper and "LABEL" in upper:
        where = "(where=(upcase(strip(fmtname))='FORATE'))" if "FMTNAME" in upper else ""
        return f"proc format cntlin=FCYFD.{FORATE_MEMBER}{where}; run;\n"

    rate_key = "FORATE" if "FORATE" in upper else "RATE" if "RATE" in upper else None
    if "CURCODE" in upper and rate_key:
        rate_col = upper[rate_key]
        is_char = header[rate_col].dtype == object
        label_expr = f"strip({rate_col})" if is_char else f"strip(put({rate_col},best32.))"
        return (
            f"data work.fmt_forate;\n"
            f"   set FCYFD.{FORATE_MEMBER}(keep={upper['CURCODE']} {rate_col});\n"
            f"   retain fmtname 'FORATE' type 'C';\n"
            f"   length start label $32;\n"
            f"   start = strip({upper['CURCODE']});\n"
            f"   label = {label_expr};\n"
            f"   keep fmtname type start label;\n"
            f"run;\n"
            f"proc sort data=work.fmt_forate nodupkey; by start; run;\n"
            f"proc format cntlin=work.fmt_forate; run;\n"
        )

    raise ValueError(f"Unrecognised forate layout, columns found: {sorted(upper)}")


# =============================================================================
# SAS PROGRAM (original logic, run inside SAS through saspy)
# =============================================================================
def build_sas_program(daily: str, monthly: str | None) -> str:
    code = (
        "options yearcutoff=1950;\n"
        f'libname FD "{ENRICHMENT_DIR}";\n'
        f'libname FCYFD "{FORATE_DIR}" access=readonly;\n'
        f"{build_format_code()}"
        "*;\n"
        f"proc sort data=FD.{daily} out=FD;\n"
        "   by ACCTNO CDNO;\n"
        "run;\n"
        "*;\n"
        f"data FD.{daily};\n"
        "   set FD;\n"
        "   if CURCODE ne 'MYR' then do;\n"
        "      FORATE = put(CURCODE,$FORATE.);\n"
        "      FORBAL = CURBAL;\n"
        "      CURBAL = round(CURBAL * FORATE,.01);\n"
        "   end;\n"
        "   CURBALUS = CURBAL / put('USD',$FORATE.);\n"
        "*;\n"
        "run;\n"
    )
    if monthly:
        # Month end: same output as the daily dataset
        code += f"data FD.{monthly};\n   set FD.{daily};\nrun;\n"
    code += (
        f"proc print data=FD.{daily};\n"
        "   where CURCODE ne 'MYR';\n"
        "run;\n"
    )
    return code


def run_sas(program: str) -> None:
    sas = saspy.SASsession(cfgname=SASPY_CFGNAME)
    try:
        sas.set_results("TEXT")
        result = sas.submit(program)
        log = result.get("LOG", "")
        listing = result.get("LST", "")

        errors = [ln for ln in log.splitlines() if ln.startswith("ERROR")]
        if errors:
            print(log)
            raise RuntimeError("SAS reported errors:\n" + "\n".join(errors))

        print(listing)
    finally:
        sas.endsas()


# =============================================================================
# MAIN
# =============================================================================
def main() -> None:
    batch_date = get_batch_date()
    repdd = f"{batch_date.day:02d}"
    repmm = f"{batch_date.month:02d}"
    month_end = is_month_end(batch_date)
    print(f"Batch date ({BATCH_SOURCE_SYSTEM}) : {batch_date}  REPDD={repdd}  REPMM={repmm}")
    print(f"Month end            : {month_end}")

    daily = f"enrh_dp_fd_cert_d{repdd}"
    monthly = f"enrh_dp_fd_cert_m{repmm}" if month_end else None
    fd_path = ENRICHMENT_DIR / f"{daily}.sas7bdat"

    assert_fresh(fd_path, batch_date, "MNIFD input")
    assert_fresh(FORATE_PATH, batch_date, "FCYFD (FORATE) input")
    validate_fd_columns(fd_path)

    run_sas(build_sas_program(daily, monthly))

    print(f"[OUTPUT] {fd_path}")
    if monthly:
        print(f"[OUTPUT] {ENRICHMENT_DIR / (monthly + '.sas7bdat')}")
    print("Done.")


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        print(f"[EIBDFFCD] {exc}", file=sys.stderr)
        sys.exit(1)
