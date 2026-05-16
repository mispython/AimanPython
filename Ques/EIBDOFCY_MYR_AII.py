# =============================================================================
# Program  : EIBDOFCY_MYR
# ESMR     : 2010-1782 (AAB)
# Desc     : Outstanding MYR Loan, CA and FD (Indiv and Non-Indiv) — MYR only
# =============================================================================

import polars as pl
import pandas as pd                                              # required for pd.read_sas()
from pathlib import Path

from REPTDATE import get_reptdate_values

# =============================================================================
# CONFIGURATION
# =============================================================================
base        = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
output_path = base / "output/EIBDOFCY"
output_path.mkdir(parents=True, exist_ok=True)

# Input file paths
# FD_PATH   : DEPO.FD      — fixed deposit (.sas7bdat); MYR rows only (CURCODE == 'MYR')
# CURR_PATH : DEPO.CURRENT — current account (.sas7bdat); MYR rows only (CURCODE == 'MYR')
# LN_PATH   : LOAN.LNNOTE  — loan (.sas7bdat); uses CCY (not CURCODE); MYR rows only (CCY == 'MYR')
#             No FCY path — this program processes MYR only; no foreign currency data is read.
FD_PATH   = base / "input/uat/fd260513.sas7bdat"               # DEPO.FD   (SET DEPO.FD)
CURR_PATH = base / "input/uat/ca260513.sas7bdat"               # DEPO.CURRENT (SET DEPO.CURRENT)
LN_PATH   = base / "input/uat/ln260513.sas7bdat"               # LOAN.LNNOTE  (SET LOAN.LNNOTE)

# Required column sets for early validation per input file
# Required column sets for early validation per input file
# FD_PATH   : no CURCODE column — all records are MYR; CURCODE added as literal downstream
# CURR_PATH : no CURCODE column — all records are MYR; CURCODE added as literal downstream
# LN_PATH   : uses CCY (not CURCODE) to identify currency
REQUIRED_FD_COLUMNS   = {"CUSTCODE", "CURBAL"}
REQUIRED_CURR_COLUMNS = {"CUSTCODE", "CURBAL"}
REQUIRED_LN_COLUMNS   = {"CUSTCODE", "CCY",   "CURBAL"}        # loan uses CCY, not CURCODE


# =============================================================================
# Helper — read one .sas7bdat and return a Polars DataFrame
# (mirrors _read_sas7bdat() in EIQBNMR1.py)
# =============================================================================
def _read_sas7bdat(path: Path) -> pl.DataFrame:
    """Read one .sas7bdat file and return a Polars DataFrame."""
    if not path.exists():
        raise FileNotFoundError(f"Missing required input file: {path}")
    pandas_df = pd.read_sas(path, format="sas7bdat", encoding="latin1")
    pandas_df.columns = [str(c).upper().strip() for c in pandas_df.columns]
    return pl.from_pandas(pandas_df)


# Helper — fail early with a clear message if columns are missing
# (mirrors _require_columns() in EIQBNMR1.py)
def _require_columns(df: pl.DataFrame, required: set[str], source: Path) -> None:
    """Fail early with a clear message if the SAS file lacks needed columns."""
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(f"{source} is missing required column(s): {', '.join(missing)}")


# =============================================================================
# REPORT DATE DERIVATION
# (Replaces: DATA REPTDATE; SET DEPO.REPTDATE; CALL SYMPUT('RDATE', PUT(REPTDATE, DDMMYY10.));)
# REPTDATE is derived as TODAY() - 1; RDATE formatted as DD/MM/YYYY (DDMMYY10.)
# =============================================================================
reptdate_values = get_reptdate_values(year_format="%Y")

REPTDATE = reptdate_values.reptdate
REPTYEAR = reptdate_values.reptyear
REPTMON  = reptdate_values.reptmon
REPTDAY  = reptdate_values.reptday
NOWK     = reptdate_values.nowk
RDATE    = REPTDATE.strftime("%d/%m/%Y")   # DDMMYY10. → DD/MM/YYYY

# =============================================================================
# DATA FD  (SET DEPO.FD; WHERE CURCODE EQ 'MYR')
# FD_PATH contains no CURCODE column — all records are MYR fixed deposits.
# CURCODE added as literal 'MYR' for downstream consistency.
# =============================================================================
_fd_raw = _read_sas7bdat(FD_PATH)
_require_columns(_fd_raw, REQUIRED_FD_COLUMNS, FD_PATH)
fd_raw = (
    _fd_raw
    .with_columns([
        pl.col("CUSTCODE").cast(pl.Utf8).str.strip_chars(),
        pl.lit("MYR").alias("CURCODE"),                         # no CURCODE in file; all records are MYR
    ])
    .select(["CUSTCODE", "CURCODE", "CURBAL"])
)

fd_df = (
    fd_raw
    .with_columns([
        pl.when(pl.col("CUSTCODE").is_in(["77", "78", "95", "96"]))
          .then(pl.lit("A")).otherwise(pl.lit("B"))
          .alias("IND"),
        pl.when(pl.col("CUSTCODE").is_in(["77", "78", "95", "96"]))
          .then(pl.col("CURBAL")).otherwise(pl.lit(None, dtype=pl.Float64))
          .alias("IFDBAL"),
        pl.when(pl.col("CUSTCODE").is_in(["77", "78", "95", "96"]))
          .then(pl.lit(None, dtype=pl.Float64)).otherwise(pl.col("CURBAL"))
          .alias("CFDBAL"),
    ])
    .select(["IND", "CURCODE", "CURBAL", "IFDBAL", "CFDBAL"])
)

# PROC SUMMARY DATA=FD NWAY; BY IND CURCODE; VAR CURBAL IFDBAL CFDBAL; OUTPUT OUT=FD SUM=;
fd_summary = (
    fd_df
    .group_by(["IND", "CURCODE"])
    .agg([
        pl.len().alias("_FREQ_"),
        pl.col("CURBAL").sum(),
        pl.col("IFDBAL").sum(),
        pl.col("CFDBAL").sum(),
    ])
    .sort(["IND", "CURCODE"])
)

fd_summary.write_parquet(output_path / "FD_MYR.parquet")

# =============================================================================
# DATA CURR  (SET DEPO.CURRENT; WHERE CURCODE EQ 'MYR')
# CURR_PATH contains no CURCODE column — all records are MYR current accounts.
# CURCODE added as literal 'MYR' for downstream consistency.
# =============================================================================
_curr_raw = _read_sas7bdat(CURR_PATH)
_require_columns(_curr_raw, REQUIRED_CURR_COLUMNS, CURR_PATH)
curr_raw = (
    _curr_raw
    .with_columns([
        pl.col("CUSTCODE").cast(pl.Utf8).str.strip_chars(),
        pl.lit("MYR").alias("CURCODE"),                         # no CURCODE in file; all records are MYR
    ])
    .select(["CUSTCODE", "CURCODE", "CURBAL"])
)

curr_df = (
    curr_raw
    .with_columns([
        pl.when(pl.col("CUSTCODE").is_in(["77", "78", "95", "96"]))
          .then(pl.lit("C")).otherwise(pl.lit("D"))
          .alias("IND"),
        pl.when(pl.col("CUSTCODE").is_in(["77", "78", "95", "96"]))
          .then(pl.col("CURBAL")).otherwise(pl.lit(None, dtype=pl.Float64))
          .alias("ICABAL"),
        pl.when(pl.col("CUSTCODE").is_in(["77", "78", "95", "96"]))
          .then(pl.lit(None, dtype=pl.Float64)).otherwise(pl.col("CURBAL"))
          .alias("CCABAL"),
    ])
    .select(["IND", "CURCODE", "CURBAL", "ICABAL", "CCABAL"])
)

# PROC SUMMARY DATA=CURR NWAY; BY IND CURCODE; VAR CURBAL ICABAL CCABAL; OUTPUT OUT=CURR SUM=;
curr_summary = (
    curr_df
    .group_by(["IND", "CURCODE"])
    .agg([
        pl.len().alias("_FREQ_"),
        pl.col("CURBAL").sum(),
        pl.col("ICABAL").sum(),
        pl.col("CCABAL").sum(),
    ])
    .sort(["IND", "CURCODE"])
)

curr_summary.write_parquet(output_path / "CURR_MYR.parquet")

# =============================================================================
# DATA LOAN  (SET LOAN.LNNOTE; WHERE CCY EQ 'MYR')
# MYR loan only — loan table uses CCY (not CURCODE) to identify currency.
# No foreign currency processed in this program.
# CCY is aliased to CURCODE for downstream consistency.
# =============================================================================
_loan_raw = _read_sas7bdat(LN_PATH)
_require_columns(_loan_raw, REQUIRED_LN_COLUMNS, LN_PATH)
loan_raw = (
    _loan_raw
    .with_columns(pl.col("CUSTCODE").cast(pl.Utf8).str.strip_chars())
    .filter(pl.col("CCY") == "MYR")
    .rename({"CCY": "CURCODE"})                                 # align to CURCODE for consistency
    .select(["CUSTCODE", "CURCODE", "CURBAL"])
)

loan_df = (
    loan_raw
    .with_columns([
        pl.when(pl.col("CUSTCODE").is_in(["77", "78", "95", "96"]))
          .then(pl.lit("E")).otherwise(pl.lit("F"))
          .alias("IND"),
        pl.when(pl.col("CUSTCODE").is_in(["77", "78", "95", "96"]))
          .then(pl.col("CURBAL")).otherwise(pl.lit(None, dtype=pl.Float64))
          .alias("ILNBAL"),
        pl.when(pl.col("CUSTCODE").is_in(["77", "78", "95", "96"]))
          .then(pl.lit(None, dtype=pl.Float64)).otherwise(pl.col("CURBAL"))
          .alias("CLNBAL"),
    ])
    .select(["IND", "CURCODE", "CURBAL", "ILNBAL", "CLNBAL"])
)

# PROC SUMMARY DATA=LOAN NWAY; BY IND CURCODE; VAR CURBAL ILNBAL CLNBAL; OUTPUT OUT=LOAN SUM=;
loan_summary = (
    loan_df
    .group_by(["IND", "CURCODE"])
    .agg([
        pl.len().alias("_FREQ_"),
        pl.col("CURBAL").sum(),
        pl.col("ILNBAL").sum(),
        pl.col("CLNBAL").sum(),
    ])
    .sort(["IND", "CURCODE"])
)

loan_summary.write_parquet(output_path / "LOAN_MYR.parquet")

# =============================================================================
# DATA MYR  (SET FD CURR LOAN; BY IND; FILE OUTMYR;)
# Produces the detailed section of OUTMYR.txt
# =============================================================================

# Combine all three summary datasets (diagonal concat fills missing cols with null)
myr_combined = pl.concat(
    [fd_summary, curr_summary, loan_summary],
    how="diagonal"
).sort(["IND", "CURCODE"])

DLM = "\x05"   # '05'X

# IND group ordering matches SAS SET order: A B C D E F
_IND_HEADERS = {
    "A": "INDIVIDUAL - MYR FIXED DEPOSIT",
    "B": "NON INDIVIDUAL - MYR FIXED DEPOSIT",
    "C": "INDIVIDUAL - MYR CURRENT",
    "D": "NON INDIVIDUAL - MYR CURRENT",
    "E": "INDIVIDUAL - MYR LOAN",
    "F": "NON INDIVIDUAL - MYR LOAN",
}

outmyr_path = output_path / "OUTMYR.txt"

with open(outmyr_path, "w", encoding="utf-8") as f:

    # _N_ = 1 block  (first record written to FILE OUTMYR)
    f.write("REPORT ID : EIBDOFCY\n")
    f.write("PUBLIC BANK BERHAD\n")
    f.write(f"OUTSTANDING MYR LOAN AND DEPOSITS AS AT {RDATE}\n")
    f.write("\n")

    prev_ind  = None
    nobs      = 0

    for row in myr_combined.iter_rows(named=True):
        ind = row["IND"]

        # FIRST.IND block
        if ind != prev_ind:
            nobs     = 0
            prev_ind = ind

            if ind == "A":
                # PUT @1  'INDIVIDUAL - MYR FIXED DEPOSIT';
                f.write(f"{_IND_HEADERS[ind]}\n")
            else:
                # PUT @1// '<header>';  (two blank lines before header)
                f.write(f"\n\n{_IND_HEADERS[ind]}\n")

            # PUT @01/ 'OBS' DLM+(-1) 'CURCODE' DLM+(-1) 'FREQ' DLM+(-1) 'CURRENT BALANCE' DLM+(-1);
            # The / before OBS produces one blank line (moves to next line)
            f.write("\n")
            f.write(f"OBS{DLM}CURCODE{DLM}FREQ{DLM}CURRENT BALANCE{DLM}\n")

        nobs += 1
        freq   = row.get("_FREQ_") or 0
        curbal = row.get("CURBAL") or 0.0

        # PUT @01 NOBS 3. DLM+(-1) CURCODE $3. DLM+(-1) _FREQ_ 10. DLM+(-1) CURBAL COMMA20.2;
        f.write(
            f"{nobs:3d}{DLM}"
            f"{str(row['CURCODE']):3s}{DLM}"
            f"{freq:10d}{DLM}"
            f"{curbal:>20,.2f}\n"
        )

myr_combined.write_parquet(output_path / "MYR.parquet")

# =============================================================================
# PROC SUMMARY DATA=MYR NWAY; BY CURCODE;
# VAR IFDBAL CFDBAL ICABAL CCABAL ILNBAL CLNBAL CURBAL; OUTPUT OUT=MYR SUM=;
# =============================================================================
myr_currency_summary = (
    myr_combined
    .group_by("CURCODE")
    .agg([
        pl.col("IFDBAL").sum(),
        pl.col("CFDBAL").sum(),
        pl.col("ICABAL").sum(),
        pl.col("CCABAL").sum(),
        pl.col("ILNBAL").sum(),
        pl.col("CLNBAL").sum(),
        pl.col("CURBAL").sum(),
    ])
    .sort("CURCODE")
)

myr_currency_summary.write_parquet(output_path / "MYR_summary.parquet")

# =============================================================================
# DATA _NULL_  (SET MYR END=LAST; FILE OUTMYR MOD;)
# Appends the summary section to OUTMYR.txt
# =============================================================================
totifd = 0.0
totcfd = 0.0
totica = 0.0
totcca = 0.0
totiln = 0.0
totcln = 0.0

with open(outmyr_path, "a", encoding="utf-8") as f:

    n = 0

    for row in myr_currency_summary.iter_rows(named=True):
        n += 1

        if n == 1:
            # PUT @1 / / "SUMMARY OF OUTSTANDING MYR LOAN AND DEPOSITS"
            #        / / 'NOBS' DLM+(-1) ... ;
            # Two '/' before the heading = two blank lines before it
            f.write("\n\n")
            f.write("SUMMARY OF OUTSTANDING MYR LOAN AND DEPOSITS\n")
            f.write("\n")
            f.write(
                f"NOBS{DLM}CURCODE{DLM}"
                f"MYRFD-INDIV{DLM}MYRFD-CORP{DLM}"
                f"MYRCA-INDIV{DLM}MYRCA-CORP{DLM}"
                f"MYRLN-INDIV{DLM}MYRLN-CORP\n"
            )

        ifdbal = row.get("IFDBAL") or 0.0
        cfdbal = row.get("CFDBAL") or 0.0
        icabal = row.get("ICABAL") or 0.0
        ccabal = row.get("CCABAL") or 0.0
        ilnbal = row.get("ILNBAL") or 0.0
        clnbal = row.get("CLNBAL") or 0.0

        # PUT @1 _N_ 3. DLM+(-1) CURCODE $3. DLM+(-1) IFDBAL COMMA20.2 DLM+(-1) ...
        f.write(
            f"{n:3d}{DLM}"
            f"{str(row['CURCODE']):3s}{DLM}"
            f"{ifdbal:>20,.2f}{DLM}"
            f"{cfdbal:>20,.2f}{DLM}"
            f"{icabal:>20,.2f}{DLM}"
            f"{ccabal:>20,.2f}{DLM}"
            f"{ilnbal:>20,.2f}{DLM}"
            f"{clnbal:>20,.2f}\n"
        )

        totifd += ifdbal
        totcfd += cfdbal
        totica += icabal
        totcca += ccabal
        totiln += ilnbal
        totcln += clnbal

    # IF LAST THEN PUT ...  (grand totals line)
    f.write(
        f"   {DLM}"
        f"TOT{DLM}"
        f"{totifd:>20,.2f}{DLM}"
        f"{totcfd:>20,.2f}{DLM}"
        f"{totica:>20,.2f}{DLM}"
        f"{totcca:>20,.2f}{DLM}"
        f"{totiln:>20,.2f}{DLM}"
        f"{totcln:>20,.2f}\n"
    )

# =============================================================================
# //* FTP TO SAS DATAWAREHOUSE
# //*%OPC SCAN
# //*%OPC SETVAR TODD=(ODD - 61CD)
# //*%OPC SETVAR TOMM=(OMM - 61CD)
# //*%OPC SETVAR TOYYYY=(OYYYY - 61CD)
# //*RUNSFTP  EXEC COZBATCH
# //*CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(CSASSFTP)
# //*lzopts servercp=$servercp,notrim,overflow=trunc,mode=text
# //*lzopts linerule=$lr
# //*cd TextFile/TD/PBB/CFTWG
# //*put //SAP.PBB.EIBDOFCY.DAILY  OutstandingFCY_%ODD.%OMM.%OYYYY..xls
# //*- rm OutstandingFCY_%TODD.%TOMM.%TOYYYY..xls
# //*EOB
# =============================================================================

# =============================================================================
# //* FTP HOST DATASETS TO DATA REPORT REPOSITORY SYSTEM (DRR)
# //*%OPC SCAN
# //RUNSFTP  EXEC COZBATCH
# //CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(DRR#SFTP)
# //lzopts servercp=$servercp,notrim,overflow=trunc,mode=text
# //lzopts linerule=$lr
# //cd TD/INTERBANK/CFTWG
# //put //SAP.PBB.EIBDOFCY.DAILY  OutstandingFCY_%ODD.%OMM.%OYYYY..xls
# //EOB
# =============================================================================

print(f"EIBDOFCY_MYR completed — report date {RDATE}, output: {outmyr_path}")
