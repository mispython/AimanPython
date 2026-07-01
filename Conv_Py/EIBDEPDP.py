"""
Program : EIBDEPDP.py
Purpose : Daily accumulation of BNM EPCU data (loan disbursement,
          loan payment, credit card payment) into monthly datasets.

ORIGINAL JCL (kept for traceability, not executable in Python):
//EIBDEPDP JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=8M
/*JOBPARM S=S1M1
//* TO RUN DAILY TO ACCUMULATE DP DATA (ESMR: 2011-1379)
//*
//GETFILE  EXEC SAS609
//DPLD      DD DSN=RBP2.B033.LN.BNM.LOANDISB.RPT(0),DISP=SHR
//DPLP      DD DSN=RBP2.B033.MIS.BNM.LOANPYMT.RPT(0),DISP=SHR
//DPCC      DD DSN=RBP2.B033.UC.BNM.CRDTPYMT.RPT(0),DISP=SHR
//LOAN      DD DSN=SAP.PBB.MNILN.DAILY(0),DISP=SHR
//BNMLD     DD DSN=SAP.PBB.EPCU.LOANDISB,DISP=OLD
//BNMLP     DD DSN=SAP.PBB.EPCU.LOANPYMT,DISP=OLD
//BNMCC     DD DSN=SAP.PBB.EPCU.CRDTPYMT,DISP=OLD
//SASLIST  DD SYSOUT=X
//PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
//SYSIN    DD *
"""

from __future__ import annotations

from pathlib import Path
from datetime import date
from typing import Optional

import polars as pl

from REPTDATE import get_reptdate_values
from input_date import get_latest_file
# from output_date import build_output_file
# NOTE: build_output_file() is not used here. Its supported date formats
# (ddmmyy / ddmmYYYY) do not match this program's output naming
# convention, which appends only the 2-digit REPTMON to the dataset
# prefix (e.g. DPLD06), matching the original SAS macro
# "&PREFIX&REPTMON". The monthly filename is therefore built directly
# using REPTMON derived from get_reptdate_values().

# NOTE: This SAS program contains no PUT(var, fmt.) calls against any
# shared format library (PBBLNFMT / PBBDPFMT / PBBELF / PBMISFMT), and
# LOAN.REPTDATE is replaced entirely by REPTDATE.py. No format-library
# import is required for this program.


# ============================================================
# PATH CONFIGURATION
# ============================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDEPDP")

# Daily raw fixed-width input files (replacing DD: DPLD / DPLP / DPCC)
INPUT_DIR = BASE_DIR / "input"

# Monthly accumulated output datasets (replacing libraries BNMLD / BNMLP / BNMCC)
OUTPUT_DIR = BASE_DIR / "output"
BNMLD_DIR = OUTPUT_DIR / "BNMLD"
BNMLP_DIR = OUTPUT_DIR / "BNMLP"
BNMCC_DIR = OUTPUT_DIR / "BNMCC"

for _directory in (INPUT_DIR, OUTPUT_DIR, BNMLD_DIR, BNMLP_DIR, BNMCC_DIR):
    _directory.mkdir(parents=True, exist_ok=True)


# ============================================================
# FIXED-WIDTH COLUMN SPECIFICATION
# (Identical INPUT statement is shared by DPLD, DPLP and DPCC)
#
# INPUT @001   ACCTNO          11.
#       @013   TRANAMT         11.2
#       @024   TRANDT       MMDDYY6.
#       @033   TRANTME         10.
#       @043   ONLTRAN         10.
#       @053   CHEQNO          10.
#       @063   CURRCODE        $3.
# ============================================================
_RECORD_LENGTH = 65
_COL_ACCTNO = (0, 11)
_COL_TRANAMT = (12, 23)
_COL_TRANDT = (23, 29)
_COL_TRANTME = (32, 42)
_COL_ONLTRAN = (42, 52)
_COL_CHEQNO = (52, 62)
_COL_CURRCODE = (62, 65)


def _parse_numeric(raw: str, decimals: int = 0) -> Optional[float]:
    """Replicate SAS numeric informat parsing (w. / w.d), including an
    implied decimal point when no explicit '.' is present in the field.
    Blank fields become SAS missing (None), matching MISSOVER behaviour.
    """
    text = raw.strip()
    if not text:
        return None

    sign = 1
    if text[0] == "-":
        sign = -1
        text = text[1:]
    elif text[0] == "+":
        text = text[1:]

    if not text:
        return None

    if "." in text:
        return sign * float(text)

    if decimals:
        return sign * (int(text) / (10 ** decimals))

    return sign * float(int(text))


def _parse_mmddyy6(raw: str) -> Optional[date]:
    """Replicate SAS MMDDYY6. informat under OPTIONS YEARCUTOFF=1930.

    Two-digit year yy: yy >= 30 -> 19yy, else 20yy.
    """
    text = raw.strip()
    if len(text) < 6 or not text.isdigit():
        return None

    mm = int(text[0:2])
    dd = int(text[2:4])
    yy = int(text[4:6])
    year = 1900 + yy if yy >= 30 else 2000 + yy

    return date(year, mm, dd)


def read_fixed_width_file(file_path: Path) -> pl.DataFrame:
    """Read a mainframe fixed-width DP transaction file (DPLD / DPLP / DPCC
    layout) and return a Polars DataFrame with SAS-equivalent informats
    applied. MISSOVER semantics: short/blank trailing fields become missing.
    """
    acctno, tranamt, trandt, trantme, onltran, cheqno, currcode = (
        [], [], [], [], [], [], []
    )

    with file_path.open("r", encoding="latin1") as fh:
        for line in fh:
            line = line.rstrip("\r\n")
            if not line.strip():
                continue
            line = line.ljust(_RECORD_LENGTH)

            acctno.append(_parse_numeric(line[_COL_ACCTNO[0]:_COL_ACCTNO[1]]))
            tranamt.append(_parse_numeric(line[_COL_TRANAMT[0]:_COL_TRANAMT[1]], decimals=2))
            trandt.append(_parse_mmddyy6(line[_COL_TRANDT[0]:_COL_TRANDT[1]]))
            trantme.append(_parse_numeric(line[_COL_TRANTME[0]:_COL_TRANTME[1]]))
            onltran.append(_parse_numeric(line[_COL_ONLTRAN[0]:_COL_ONLTRAN[1]]))
            cheqno.append(_parse_numeric(line[_COL_CHEQNO[0]:_COL_CHEQNO[1]]))
            currcode.append(line[_COL_CURRCODE[0]:_COL_CURRCODE[1]].strip() or None)

    return pl.DataFrame(
        {
            "ACCTNO": acctno,
            "TRANAMT": tranamt,
            "TRANDT": trandt,
            "TRANTME": trantme,
            "ONLTRAN": onltran,
            "CHEQNO": cheqno,
            "CURRCODE": currcode,
        },
        schema={
            "ACCTNO": pl.Int64,
            "TRANAMT": pl.Float64,
            "TRANDT": pl.Date,
            "TRANTME": pl.Int64,
            "ONLTRAN": pl.Int64,
            "CHEQNO": pl.Int64,
            "CURRCODE": pl.Utf8,
        },
    )


def accumulate_monthly(
    daily_df: pl.DataFrame,
    monthly_dir: Path,
    prefix: str,
    reptmon: str,
    reptday: str,
    reptdt: int,
) -> Path:
    """Replicate the %ACCUM macro:

    %IF "&REPTDAY" EQ "01" %THEN
        DATA BNMx.<PREFIX>&REPTMON; SET <daily>; RUN;
    %ELSE
        DATA BNMx.<PREFIX>&REPTMON; SET BNMx.<PREFIX>&REPTMON;
            IF REPTDATE EQ "&RDATE" THEN DELETE;
        RUN;
        PROC APPEND DATA=<daily> BASE=BNMx.<PREFIX>&REPTMON; RUN;
    """
    monthly_file = monthly_dir / f"{prefix}{reptmon}.parquet"

    if reptday == "01":
        result_df = daily_df
    else:
        if monthly_file.exists():
            existing_df = pl.read_parquet(monthly_file)
            existing_df = existing_df.filter(pl.col("REPTDATE") != reptdt)
        else:
            existing_df = daily_df.clear()
        result_df = pl.concat([existing_df, daily_df], how="vertical_relaxed")

    result_df.write_parquet(monthly_file)
    return monthly_file


def main() -> None:
    # ------------------------------------------------------------
    # DATA REPTDATE (KEEP=REPTDATE);
    #   SET LOAN.REPTDATE;
    #   CALL SYMPUT('REPTYEAR', PUT(REPTDATE, YEAR2.));
    #   CALL SYMPUT('REPTMON', PUT(MONTH(REPTDATE), Z2.));
    #   CALL SYMPUT('REPTDAY', PUT(DAY(REPTDATE), Z2.));
    #   CALL SYMPUT('RDATE', PUT(REPTDATE, Z5.));
    # RUN;
    # NOTE: No reptdate.parquet exists in production; report date is
    # always derived through REPTDATE.py.
    # ------------------------------------------------------------
    reptdate_values = get_reptdate_values()
    reptyear = reptdate_values.reptyear
    reptmon = reptdate_values.reptmon
    reptday = reptdate_values.reptday
    rdate = reptdate_values.reptdt  # numeric filter key equivalent to SAS &RDATE

    print(
        f"[REPTDATE] REPTDATE={reptdate_values.reptdate} REPTYEAR={reptyear} "
        f"REPTMON={reptmon} REPTDAY={reptday} RDATE={rdate}"
    )

    # ------------------------------------------------------------
    # Resolve latest daily input files (replacing DD: DPLD / DPLP / DPCC)
    # ------------------------------------------------------------
    dpld_path = get_latest_file(INPUT_DIR, prefix="DPLD")
    dplp_path = get_latest_file(INPUT_DIR, prefix="DPLP")
    dpcc_path = get_latest_file(INPUT_DIR, prefix="DPCC")

    # ------------------------------------------------------------
    # DATA DPLD; INFILE DPLD MISSOVER; INPUT ...; REPTDATE=&RDATE; RUN;
    # PROC PRINT DATA=DPLD(OBS=10); RUN;
    # ------------------------------------------------------------
    dpld_df = read_fixed_width_file(dpld_path).with_columns(pl.lit(rdate).alias("REPTDATE"))
    print("\n--- DPLD (first 10 records) ---")
    print(dpld_df.head(10))

    # ------------------------------------------------------------
    # DATA DPLP; INFILE DPLP MISSOVER; INPUT ...; REPTDATE=&RDATE; RUN;
    # PROC PRINT DATA=DPLP(OBS=10); RUN;
    # ------------------------------------------------------------
    dplp_df = read_fixed_width_file(dplp_path).with_columns(pl.lit(rdate).alias("REPTDATE"))
    print("\n--- DPLP (first 10 records) ---")
    print(dplp_df.head(10))

    # ------------------------------------------------------------
    # DATA DPCC; INFILE DPCC MISSOVER; INPUT ...; REPTDATE=&RDATE; RUN;
    # PROC PRINT DATA=DPCC(OBS=10); RUN;
    # ------------------------------------------------------------
    dpcc_df = read_fixed_width_file(dpcc_path).with_columns(pl.lit(rdate).alias("REPTDATE"))
    print("\n--- DPCC (first 10 records) ---")
    print(dpcc_df.head(10))

    # ------------------------------------------------------------
    # %MACRO ACCUM;
    #   %IF "&REPTDAY" EQ "01" %THEN
    #       DATA BNMLD.DPLD&REPTMON; SET DPLD; RUN;
    #       DATA BNMLP.DPLP&REPTMON; SET DPLP; RUN;
    #       DATA BNMCC.DPCC&REPTMON; SET DPCC; RUN;
    #   %ELSE
    #       DATA BNMLD.DPLD&REPTMON; SET BNMLD.DPLD&REPTMON;
    #           IF REPTDATE EQ "&RDATE" THEN DELETE; RUN;
    #       PROC APPEND DATA=DPLD BASE=BNMLD.DPLD&REPTMON; RUN;
    #       ... (same pattern for DPLP / DPCC)
    # %MEND ACCUM;
    # %ACCUM;
    # ------------------------------------------------------------
    ld_file = accumulate_monthly(dpld_df, BNMLD_DIR, "DPLD", reptmon, reptday, rdate)
    lp_file = accumulate_monthly(dplp_df, BNMLP_DIR, "DPLP", reptmon, reptday, rdate)
    cc_file = accumulate_monthly(dpcc_df, BNMCC_DIR, "DPCC", reptmon, reptday, rdate)

    print(f"\n[OUTPUT] BNMLD monthly file : {ld_file}")
    print(pl.read_parquet(ld_file))

    print(f"\n[OUTPUT] BNMLP monthly file : {lp_file}")
    print(pl.read_parquet(lp_file))

    print(f"\n[OUTPUT] BNMCC monthly file : {cc_file}")
    print(pl.read_parquet(cc_file))


if __name__ == "__main__":
    main()
