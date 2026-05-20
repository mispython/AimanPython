"""
Program : EIBDOFCY_MYR_FCY.py
Purpose : Outstanding FCY Loan, CA and FD (Indiv and Non-Indiv) — MYR + FCY
"""

import polars as pl
import pandas as pd                 # required for pd.read_sas()
from pathlib import Path

from REPTDATE import get_reptdate_values
from input_date import get_latest_file
from output_date import build_output_file

# =============================================================================
# CONFIGURATION
# =============================================================================
base        = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
input_path  = base / "input/uat"
output_path = base / "output/EIBDOFCY"
output_path.mkdir(parents=True, exist_ok=True)

# >>>>> Input file paths <<<<<
"""
FD_PATH     : DEPO.FD      — fixed deposit (.sas7bdat); FCY FD filtered by CURCODE != 'MYR'
FD_FCY_PATH : DP.FCY&REPTYEAR&REPTMON&REPTDAY — FCY deposit file (.sas7bdat);
              also source for FCY current account (DATA FCYCA; SET DP.FCY...
              RENAME=(CURBALRM=CURBAL PRODUCT=PRODUCT_CODE MTDAVBAL=MTDAVBAL_FCY))
              No separate CURR_FCY_PATH — FCY CA data resides in this same file.
CURR_PATH   : DEPO.CURRENT — current account (.sas7bdat); MYR rows only
LN_PATH     : LOAN.LNNOTE  — loan (.sas7bdat); uses CCY (not CURCODE); FCY filtered from same file
"""
# FD_PATH     = input_path / "fd260513.sas7bdat"             # DEPO.FD      (SET DEPO.FD)
# FD_FCY_PATH = input_path / "fcyfd260513.sas7bdat"          # DP.FCY<REPTYEAR><REPTMON><REPTDAY>
# CURR_PATH   = input_path / "ca260513.sas7bdat"             # DEPO.CURRENT (SET DEPO.CURRENT)
LN_PATH     = input_path / "ln260513.sas7bdat"             # LOAN.LNNOTE  (SET LOAN.LNNOTE)
FD_PATH     = get_latest_file(input_path, "fd" )            # DEPO.FD      (SET DEPO.FD)
FD_FCY_PATH = get_latest_file(input_path, "fcyfd" )         # DP.FCY<REPTYEAR><REPTMON><REPTDAY>
CURR_PATH   = get_latest_file(input_path, "ca")             # DEPO.CURRENT (SET DEPO.CURRENT)
# LN_PATH     = get_latest_file(input_path, "ln")             # LOAN.LNNOTE  (SET LOAN.LNNOTE)

# >>>>> Required column sets for early validation per input file <<<<<
"""
FD_PATH     : no CURCODE column — all records are MYR; CURCODE added as literal downstream
FD_FCY_PATH : has CURCODE; balance column is already CURBAL (no rename required)
CURR_PATH   : no CURCODE column — all records are MYR; CURCODE added as literal downstream
LN_PATH     : uses CCY (not CURCODE) to identify currency
"""
REQUIRED_FD_COLUMNS     = {"CUSTCODE", "CURBAL"}
REQUIRED_FD_FCY_COLUMNS = {"CUSTCODE", "CURCODE", "CURBAL"}
REQUIRED_CURR_COLUMNS   = {"CUSTCODE", "CURBAL"}
REQUIRED_LN_COLUMNS     = {"CUSTCODE", "CCY",     "CURBAL"}    # loan uses CCY, not CURCODE


# =============================================================================
# Helper — read one .sas7bdat and return a Polars DataFrame
# (mirrors _read_sas7bdat() in EIQBNMR1.py)
# =============================================================================
def _read_sas7bdat(path: Path) -> pl.DataFrame:
    """Read one .sas7bdat file and return a Polars DataFrame."""
    if not path.exists():
        raise FileNotFoundError(f"Missing required input file: {path}")
    
    # >>>>>>>>>> Uncomment this -> For production <<<<<<<<<<
    pandas_df = pd.read_sas(
        path,
        format="sas7bdat",
        encoding="latin1",
    )

    # # >>>>>>>>>> Uncomment this -> For testing purposes <<<<<<<<<<
    # reader = pd.read_sas(
    #     path,
    #     format="sas7bdat",
    #     encoding="latin1",
    #     chunksize = 10000          
    # )
    # pandas_df = next(reader)

    pandas_df.columns = [
        str(c).upper().strip()
        for c in pandas_df.columns
    ]

    return pl.from_pandas(pandas_df)


# Helper — fail early with a clear message if columns are missing
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
# DATA FD  (SET DEPO.FD; WHERE CURCODE NE 'MYR')
# FD_PATH contains no CURCODE column — all records are MYR fixed deposits.
# FCY FD records are sourced separately from FD_FCY_PATH.
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

fd_summary.write_parquet(output_path / "FD.parquet")
fd_summary.write_csv(output_path / "FD.csv")

# =============================================================================
# DATA FCYCA  (SET DP.FCY<REPTYEAR><REPTMON><REPTDAY>
#                  (IN=A RENAME=(CURBALRM=CURBAL PRODUCT=PRODUCT_CODE MTDAVBAL=MTDAVBAL_FCY)))
# FCY current account — sourced from FD_FCY_PATH (.sas7bdat).
# No separate CURR_FCY_PATH exists; the FCY CA data resides in this same file.
# The actual dataset column is already named CURBAL (no rename required).
# All records in this file are FCY; CURCODE is present and identifies the currency.
# =============================================================================
_fcy_raw = _read_sas7bdat(FD_FCY_PATH)
_require_columns(_fcy_raw, REQUIRED_FD_FCY_COLUMNS, FD_FCY_PATH)
curr_fcy_raw = (
    _fcy_raw
    .with_columns(pl.col("CUSTCODE").cast(pl.Utf8).str.strip_chars())
    .select(["CUSTCODE", "CURCODE", "CURBAL"])
)

curr_fcy_df = (
    curr_fcy_raw
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

# =============================================================================
# DATA CURR  (SET DEPO.CURRENT; WHERE CURCODE EQ 'MYR')
# CURR_PATH contains no CURCODE column — all records are MYR current accounts.
# CURCODE added as literal 'MYR' for downstream consistency.
# =============================================================================
_curr_raw = _read_sas7bdat(CURR_PATH)
_require_columns(_curr_raw, REQUIRED_CURR_COLUMNS, CURR_PATH)
curr_myr_raw = (
    _curr_raw
    .with_columns([
        pl.col("CUSTCODE").cast(pl.Utf8).str.strip_chars(),
        pl.lit("MYR").alias("CURCODE"),                         # no CURCODE in file; all records are MYR
    ])
    .select(["CUSTCODE", "CURCODE", "CURBAL"])
)

curr_myr_df = (
    curr_myr_raw
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

# Combine MYR CA + FCY CA into one CURR summary
# PROC SUMMARY DATA=CURR NWAY; BY IND CURCODE; VAR CURBAL ICABAL CCABAL; OUTPUT OUT=CURR SUM=;
curr_summary = (
    pl.concat([curr_myr_df, curr_fcy_df], how="vertical")
    .group_by(["IND", "CURCODE"])
    .agg([
        pl.len().alias("_FREQ_"),
        pl.col("CURBAL").sum(),
        pl.col("ICABAL").sum(),
        pl.col("CCABAL").sum(),
    ])
    .sort(["IND", "CURCODE"])
)

curr_summary.write_parquet(output_path / "CURR.parquet")
curr_summary.write_csv(output_path / "CURR.csv")

# =============================================================================
# DATA LOAN  (SET LOAN.LNNOTE; WHERE CCY NE 'MYR')
# Loan table uses CCY (not CURCODE) to identify currency.
# FCY and MYR loans reside in the same LN_PATH file; FCY rows filtered by CCY != 'MYR'.
# CCY is aliased to CURCODE for downstream consistency.
# =============================================================================
_loan_raw = _read_sas7bdat(LN_PATH)
_require_columns(_loan_raw, REQUIRED_LN_COLUMNS, LN_PATH)
loan_raw = (
    _loan_raw
    .with_columns(pl.col("CUSTCODE").cast(pl.Utf8).str.strip_chars())
    .filter(pl.col("CCY") != "MYR")
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

loan_summary.write_parquet(output_path / "LOAN.parquet")
loan_summary.write_csv(output_path / "LOAN.csv")

# =============================================================================
# DATA FCY  (SET FD CURR LOAN; BY IND; FILE OUTFCY;)
# Produces the detailed section of OUTFCY.txt
# =============================================================================

# Combine all three summary datasets (diagonal concat fills missing cols with null)
fcy_combined = pl.concat(
    [fd_summary, curr_summary, loan_summary],
    how="diagonal"
).sort(["IND", "CURCODE"])

DLM = "\x05"   # '05'X

# IND group ordering matches SAS SET order: A B C D E F
_IND_HEADERS = {
    "A": "INDIVIDUAL - FCY FIXED DEPOSIT",
    "B": "NON INDIVIDUAL - FCY FIXED DEPOSIT",
    "C": "INDIVIDUAL - FCY CURRENT",
    "D": "NON INDIVIDUAL - FCY CURRENT",
    "E": "INDIVIDUAL - FCY LOAN",
    "F": "NON INDIVIDUAL - FCY LOAN",
}

outfcy_path = output_path / "OUTFCY.txt"
outfcy_path = output_path / "OUTFCY.csv"

with open(outfcy_path, "w", encoding="utf-8") as f:

    # _N_ = 1 block  (first record written to FILE OUTFCY)
    f.write(f"REPORT ID : EIBDOFCY\n")
    f.write(f"PUBLIC BANK BERHAD\n")
    f.write(f"OUTSTANDING FCY LOAN AND DEPOSITS AS AT {RDATE}\n")
    f.write("\n")

    prev_ind  = None
    nobs      = 0

    for row in fcy_combined.iter_rows(named=True):
        ind = row["IND"]

        # FIRST.IND block
        if ind != prev_ind:
            nobs     = 0
            prev_ind = ind

            if ind == "A":
                # PUT @1  'INDIVIDUAL - FCY FIXED DEPOSIT';
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

fcy_combined.write_parquet(output_path / "FCY.parquet")
fcy_combined.write_csv(output_path / "FCY.csv")

# =============================================================================
# PROC SUMMARY DATA=FCY NWAY; BY CURCODE;
# VAR IFDBAL CFDBAL ICABAL CCABAL ILNBAL CLNBAL CURBAL; OUTPUT OUT=FCY SUM=;
# =============================================================================
fcy_currency_summary = (
    fcy_combined
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

fcy_currency_summary.write_parquet(output_path / "FCY_summary.parquet")
fcy_currency_summary.write_csv(output_path / "FCY_summary.csv")

# =============================================================================
# DATA _NULL_  (SET FCY END=LAST; FILE OUTFCY MOD;)
# Appends the summary section to OUTFCY.txt
# =============================================================================
totifd = 0.0
totcfd = 0.0
totica = 0.0
totcca = 0.0
totiln = 0.0
totcln = 0.0

with open(outfcy_path, "a", encoding="utf-8") as f:

    n = 0

    for row in fcy_currency_summary.iter_rows(named=True):
        n += 1

        if n == 1:
            # PUT @1 / / "SUMMARY OF OUTSTANDING FCY LOAN AND DEPOSITS"
            #        / / 'NOBS' DLM+(-1) ... ;
            # Two '/' before the heading = two blank lines before it
            f.write("\n\n")
            f.write("SUMMARY OF OUTSTANDING FCY LOAN AND DEPOSITS\n")
            f.write("\n")
            f.write(
                f"NOBS{DLM}CURCODE{DLM}"
                f"FCYFD-INDIV{DLM}FCYFD-CORP{DLM}"
                f"FCYCA-INDIV{DLM}FCYCA-CORP{DLM}"
                f"FCYLN-INDIV{DLM}FCYLN-CORP\n"
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

print(f"\n EIBDOFCY_MYR_FCY completed — report date {RDATE}, output: {outfcy_path} \n")
