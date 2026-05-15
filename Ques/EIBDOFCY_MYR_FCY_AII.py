"""
Program  : EIBDOFCY.py
ESMR     : 2010-1782 (AAB)
Desc     : Outstanding FCY Loan, CA and FD (Indiv and Non-Indiv)
"""

import polars as pl
import pandas as pd             # + ADDED: required for pd.read_sas()
from pathlib import Path

from REPTDATE import get_reptdate_values

# =============================================================================
# CONFIGURATION
# =============================================================================
base        = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
output_path = base / "output/EIBDOFCY"
output_path.mkdir(parents=True, exist_ok=True)

# + ADDED: Declare all input file paths here (replaces duckdb parquet reads)
FD_PATH         = base / "input/uat/fd260513.sas7bdat"               # DEPO.FD   (SET DEPO.FD) - MYR
FD_FCY_PATH     = base / "input/uat/fdFCY.sas7bdat"               # DEPO.FD   (SET DEPO.FD) - FCY
CURR_PATH       = base / "input/uat/ca260513.sas7bdat"               # DEPO.CURRENT (SET DEPO.CURRENT) - MYR
CURR_FCY_PATH   = base / "input/uat/caFCY.sas7bdat"               # DEPO.CURRENT (SET DEPO.CURRENT) - FCY
LOAN_PATH       = base / "input/uat/ln260513.sas7bdat"               # LOAN.LNNOTE  (SET LOAN.LNNOTE) - MYR
LOAN_FCY_PATH   = base / "input/uat/lnFCY.sas7bdat"               # LOAN.LNNOTE  (SET LOAN.LNNOTE) - FCY

# + ADDED: Required column sets for early validation per input file
REQUIRED_FD_COLUMNS   = {"CUSTCODE", "CURCODE", "CURBAL"}
REQUIRED_CURR_COLUMNS = {"CUSTCODE", "CURCODE", "CURBAL"}
REQUIRED_LOAN_COLUMNS = {"CUSTCODE", "CURCODE", "CURBAL"}


# =============================================================================
# + ADDED: Helper — read one .sas7bdat and return a Polars DataFrame
#          (mirrors _read_sas7bdat() in EIQBNMR1.py)
# =============================================================================
def _read_sas7bdat(path: Path) -> pl.DataFrame:
    """Read one .sas7bdat file and return a Polars DataFrame."""
    if not path.exists():
        raise FileNotFoundError(f"Missing required input file: {path}")
    
    pandas_df = pd.read_sas(
        path,
        format="sas7bdat",
        encoding="latin1",
    )
    
    pandas_df.columns = [
        str(column).upper().strip()
        for column in pandas_df.columns
    ]

    # For testing purposes
    print("\nDEBUG COLUMN NAMES:")
    print(pandas_df.columns.tolist())
    print(pandas_df.head(10))
    
    return pl.from_pandas(pandas_df)


# + ADDED: Helper — fail early with a clear message if columns are missing
#          (mirrors _require_columns() in EIQBNMR1.py)
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
# =============================================================================
# + CHANGED: Read .sas7bdat via _read_sas7bdat() instead of duckdb/parquet
_fd_raw = _read_sas7bdat(FD_PATH)
_require_columns(_fd_raw, REQUIRED_FD_COLUMNS, FD_PATH)
fd_raw = (
    _fd_raw
    .with_columns(pl.col("CUSTCODE").cast(pl.Utf8).str.strip_chars())
    .filter(pl.col("CURCODE") != "MYR")
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
fd_summary.write_parquet(output_path / "FD.csv")

# =============================================================================
# DATA CURR  (SET DEPO.CURRENT; WHERE CURCODE NE 'MYR')
# =============================================================================
# + CHANGED: Read .sas7bdat via _read_sas7bdat() instead of duckdb/parquet
_curr_raw = _read_sas7bdat(CURR_PATH)
_require_columns(_curr_raw, REQUIRED_CURR_COLUMNS, CURR_PATH)
curr_raw = (
    _curr_raw
    .with_columns(pl.col("CUSTCODE").cast(pl.Utf8).str.strip_chars())
    .filter(pl.col("CURCODE") != "MYR")
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

curr_summary.write_parquet(output_path / "CURR.parquet")
curr_summary.write_parquet(output_path / "CURR.csv")

# =============================================================================
# DATA LOAN  (SET LOAN.LNNOTE; WHERE CURCODE NE 'MYR')
# =============================================================================
# + CHANGED: Read .sas7bdat via _read_sas7bdat() instead of duckdb/parquet
_loan_raw = _read_sas7bdat(LOAN_PATH)
_require_columns(_loan_raw, REQUIRED_LOAN_COLUMNS, LOAN_PATH)
loan_raw = (
    _loan_raw
    .with_columns(pl.col("CUSTCODE").cast(pl.Utf8).str.strip_chars())
    .filter(pl.col("CURCODE") != "MYR")
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
loan_summary.write_parquet(output_path / "LOAN.csv")

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
            f.write(f"\n")
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

fcy_combined.write_parquet(output_path / "FCY_combined.parquet")
fcy_combined.write_parquet(output_path / "FCY_combined.csv")

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

fcy_currency_summary.write_parquet(output_path / "FCY_CURR_sum.parquet")
fcy_currency_summary.write_parquet(output_path / "FCY_CURR_sum.csv")

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

    rows = fcy_currency_summary.iter_rows(named=True)
    n    = 0

    for row in rows:
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

print(f"EIBDOFCY completed — report date {RDATE}, output: {outfcy_path}")
