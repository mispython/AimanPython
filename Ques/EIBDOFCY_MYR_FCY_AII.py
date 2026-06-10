"""
Program : EIBDOFCY_MYR_FCY.py
Purpose : Outstanding FCY Loan, CA and FD (Indiv and Non-Indiv)
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
BASE_DIR   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
INPUT_DIR  = BASE_DIR  / "input/prod"
OUTPUT_DIR = BASE_DIR  / "output/EIBDOFCY"

# INPUT_DIR  = Path("/dwh")
# OUTPUT_DIR = Path("/host/mis/output/report") / "EIBDOFCY"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# >>>>> Input file paths <<<
"""
FD_PATH   : DEPO.FD      — fixed deposit (.sas7bdat); has CURCODE; filter WHERE CURCODE NE 'MYR'
CURR_PATH : DEPO.CURRENT — current account (.sas7bdat); has CURCODE; filter WHERE CURCODE NE 'MYR'
LN_PATH   : LOAN.LNNOTE  — loan (.sas7bdat); has CURCODE; filter WHERE CURCODE NE 'MYR'
"""
FD_PATH   = INPUT_DIR / "fd260609.sas7bdat"
CURR_PATH = INPUT_DIR / "ca260609.sas7bdat"
LN_PATH   = INPUT_DIR / "ln260609.sas7bdat"
# FD_PATH   = get_latest_file(INPUT_DIR / "dpd_fd", "fd")    # DEPO.FD      (SET DEPO.FD)
# CURR_PATH = get_latest_file(INPUT_DIR / "dpd_ca", "ca")    # DEPO.CURRENT (SET DEPO.CURRENT)
# LN_PATH   = get_latest_file(INPUT_DIR / "lnd_ln", "ln")    # LOAN.LNNOTE  (SET LOAN.LNNOTE)

# >>>>> Required column sets for early validation per input file <<<
"""
All three source files contain CURCODE and mixed MYR+FCY records.
FCY rows are selected by filtering WHERE CURCODE NE 'MYR', matching the original SAS.
"""
REQUIRED_FD_COLUMNS   = {"CUSTCODE", "CURCODE", "CURBAL"}
REQUIRED_CURR_COLUMNS = {"CUSTCODE", "CURCODE", "CURBAL"}
REQUIRED_LN_COLUMNS   = {"CUSTCODE", "CURCODE", "CURBAL"}

# INDIVIDUAL custcode values (SAS: IF CUSTCODE IN ('77','78','95','96'))
INDIVIDUAL_CUSTCODES = ["77", "78", "95", "96"]

# Delimiter — SAS DLM='05'X (EBCDIC ENQ control character)
DLM = "\t"      # TAB separator — visually equivalent to '05'X for report display
# DLM = "\x05"  # '05'X — raw EBCDIC-style delimiter (matches original SAS mainframe byte)
# DLM = "|"     # Standard report delimiter (debug/dev only)

# IND group labels — matches SAS SELECT(IND) WHEN blocks
_IND_HEADERS = {
    "A": "INDIVIDUAL - FCY FIXED DEPOSIT",
    "B": "NON INDIVIDUAL - FCY FIXED DEPOSIT",
    "C": "INDIVIDUAL - FCY CURRENT",
    "D": "NON INDIVIDUAL - FCY CURRENT",
    "E": "INDIVIDUAL - FCY LOAN",
    "F": "NON INDIVIDUAL - FCY LOAN",
}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def _read_sas7bdat(path: Path) -> pl.DataFrame:
    """Read one .sas7bdat file and return a Polars DataFrame."""
    if not path.exists():
        raise FileNotFoundError(f"Missing required input file: {path}")

    # # >>>>>>>>>> Uncomment this -> For production <<<<<<<<
    # pandas_df = pd.read_sas(
    #     path,
    #     format="sas7bdat",
    #     encoding="latin1",
    # )

    # >>>>>>>>>> Uncomment this -> For testing purposes <<<<<<<<
    reader = pd.read_sas(
        path,
        format="sas7bdat",
        encoding="latin1",
        chunksize=1000,
    )
    pandas_df = next(reader)

    pandas_df.columns = [
        str(c).upper().strip()
        for c in pandas_df.columns
    ]

    return pl.from_pandas(pandas_df)


def _require_columns(df: pl.DataFrame, required: set[str], source: Path) -> None:
    """Fail early with a clear message if the SAS file lacks needed columns."""
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(f"{source} is missing required column(s): {', '.join(missing)}")


def _assign_ind(
    df: pl.DataFrame,
    ind_indiv: str,
    ind_corp: str,
    bal_indiv_col: str,
    bal_corp_col: str,
) -> pl.DataFrame:
    """
    Assign IND flag and split CURBAL into individual/corporate balance columns.
    Mirrors the SAS IF CUSTCODE IN ('77','78','95','96') THEN ... ELSE ... pattern.
    """
    return df.with_columns([
        pl.when(pl.col("CUSTCODE").is_in(INDIVIDUAL_CUSTCODES))
          .then(pl.lit(ind_indiv)).otherwise(pl.lit(ind_corp))
          .alias("IND"),
        pl.when(pl.col("CUSTCODE").is_in(INDIVIDUAL_CUSTCODES))
          .then(pl.col("CURBAL")).otherwise(pl.lit(None, dtype=pl.Float64))
          .alias(bal_indiv_col),
        pl.when(pl.col("CUSTCODE").is_in(INDIVIDUAL_CUSTCODES))
          .then(pl.lit(None, dtype=pl.Float64)).otherwise(pl.col("CURBAL"))
          .alias(bal_corp_col),
    ])


def _summarise(df: pl.DataFrame, bal_cols: list[str]) -> pl.DataFrame:
    """
    PROC SUMMARY NWAY; BY IND CURCODE; VAR ...; OUTPUT OUT=... SUM=;
    Groups by IND and CURCODE, summing all balance columns plus row count.
    """
    return (
        df
        .group_by(["IND", "CURCODE"])
        .agg(
            [pl.len().alias("_FREQ_"), pl.col("CURBAL").sum()]
            + [pl.col(c).sum() for c in bal_cols]
        )
        .sort(["IND", "CURCODE"])
    )


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
# DATA FD  (SET DEPO.FD; WHERE CURCODE NE 'MYR';)
# DEPO.FD contains both MYR and FCY records; FCY rows selected by CURCODE NE 'MYR'.
# CUSTCODE IN ('77','78','95','96') → INDIVIDUAL (IND=A), else NON-INDIVIDUAL (IND=B).
# =============================================================================
_fd_raw = _read_sas7bdat(FD_PATH)
_require_columns(_fd_raw, REQUIRED_FD_COLUMNS, FD_PATH)

fd_df = (
    _fd_raw
    .with_columns(pl.col("CUSTCODE").cast(pl.Utf8).str.strip_chars())
    .filter(pl.col("CURCODE") != "MYR")                        # WHERE CURCODE NE 'MYR'
    .select(["CUSTCODE", "CURCODE", "CURBAL"])
    .pipe(_assign_ind, "A", "B", "IFDBAL", "CFDBAL")
    .select(["IND", "CURCODE", "CURBAL", "IFDBAL", "CFDBAL"])
)

# PROC SUMMARY DATA=FD NWAY; BY IND CURCODE; VAR CURBAL IFDBAL CFDBAL; OUTPUT OUT=FD SUM=;
fd_summary = _summarise(fd_df, ["IFDBAL", "CFDBAL"])

fd_summary.write_parquet(OUTPUT_DIR / "FD.parquet")
fd_summary.write_csv(OUTPUT_DIR / "FD.csv")


# =============================================================================
# DATA CURR  (SET DEPO.CURRENT; WHERE CURCODE NE 'MYR';)
# DEPO.CURRENT contains both MYR and FCY records; FCY rows selected by CURCODE NE 'MYR'.
# CUSTCODE IN ('77','78','95','96') → INDIVIDUAL (IND=C), else NON-INDIVIDUAL (IND=D).
# =============================================================================
_curr_raw = _read_sas7bdat(CURR_PATH)
_require_columns(_curr_raw, REQUIRED_CURR_COLUMNS, CURR_PATH)

curr_df = (
    _curr_raw
    .with_columns(pl.col("CUSTCODE").cast(pl.Utf8).str.strip_chars())
    .filter(pl.col("CURCODE") != "MYR")                        # WHERE CURCODE NE 'MYR'
    .select(["CUSTCODE", "CURCODE", "CURBAL"])
    .pipe(_assign_ind, "C", "D", "ICABAL", "CCABAL")
    .select(["IND", "CURCODE", "CURBAL", "ICABAL", "CCABAL"])
)

# PROC SUMMARY DATA=CURR NWAY; BY IND CURCODE; VAR CURBAL ICABAL CCABAL; OUTPUT OUT=CURR SUM=;
curr_summary = _summarise(curr_df, ["ICABAL", "CCABAL"])

curr_summary.write_parquet(OUTPUT_DIR / "CURR.parquet")
curr_summary.write_csv(OUTPUT_DIR / "CURR.csv")


# =============================================================================
# DATA LOAN  (SET LOAN.LNNOTE; WHERE CURCODE NE 'MYR';)
# LOAN.LNNOTE contains both MYR and FCY records; FCY rows selected by CURCODE NE 'MYR'.
# CUSTCODE IN ('77','78','95','96') → INDIVIDUAL (IND=E), else NON-INDIVIDUAL (IND=F).
# =============================================================================
_loan_raw = _read_sas7bdat(LN_PATH)
_require_columns(_loan_raw, REQUIRED_LN_COLUMNS, LN_PATH)

loan_df = (
    _loan_raw
    .with_columns(pl.col("CUSTCODE").cast(pl.Utf8).str.strip_chars())
    .filter(pl.col("CURCODE") != "MYR")                        # WHERE CURCODE NE 'MYR'
    .select(["CUSTCODE", "CURCODE", "CURBAL"])
    .pipe(_assign_ind, "E", "F", "ILNBAL", "CLNBAL")
    .select(["IND", "CURCODE", "CURBAL", "ILNBAL", "CLNBAL"])
)

# PROC SUMMARY DATA=LOAN NWAY; BY IND CURCODE; VAR CURBAL ILNBAL CLNBAL; OUTPUT OUT=LOAN SUM=;
loan_summary = _summarise(loan_df, ["ILNBAL", "CLNBAL"])

loan_summary.write_parquet(OUTPUT_DIR / "LOAN.parquet")
loan_summary.write_csv(OUTPUT_DIR / "LOAN.csv")


# =============================================================================
# DATA FCY  (SET FD CURR LOAN; BY IND; FILE OUTFCY;)
# Combines all three summaries and writes the detailed section of the report.
# =============================================================================

# Combine all three summary datasets (diagonal concat fills missing cols with null)
fcy_combined = (
    pl.concat([fd_summary, curr_summary, loan_summary], how="diagonal")
    .sort(["IND", "CURCODE"])
)

fcy_combined.write_parquet(OUTPUT_DIR / "FCY.parquet")
fcy_combined.write_csv(OUTPUT_DIR / "FCY.csv")

# Output file — named OutstandingFCY(DDMMYYYY).txt, e.g. OutstandingFCY09062026.txt
out_txt = build_output_file(
    OUTPUT_DIR,
    "OutstandingFCY",
    date_format="ddmmYYYY",
).with_suffix(".txt")

with open(out_txt, "w", encoding="utf-8") as f:

    # _N_ = 1 block — report header (PUT @1 'REPORT ID...' / 'PUBLIC BANK...' / ... /)
    f.write("REPORT ID : EIBDOFCY\n")
    f.write("PUBLIC BANK BERHAD\n")
    f.write(f"OUTSTANDING FCY LOAN AND DEPOSITS AS AT {RDATE}\n")
    f.write("\n")

    prev_ind = None
    nobs     = 0

    for row in fcy_combined.iter_rows(named=True):
        ind = row["IND"]

        # FIRST.IND block
        if ind != prev_ind:
            nobs     = 0
            prev_ind = ind

            if ind == "A":
                # PUT @1 'INDIVIDUAL - FCY FIXED DEPOSIT';  (no leading blank lines for first group)
                f.write(f"{_IND_HEADERS[ind]}\n")
            else:
                # PUT @1// '<header>';  (two blank lines before each subsequent group header)
                f.write(f"\n\n{_IND_HEADERS[ind]}\n")

            # PUT @01/ 'OBS' DLM+(-1) 'CURCODE' DLM+(-1) 'FREQ' DLM+(-1) 'CURRENT BALANCE' DLM+(-1);
            # The leading / produces one blank line before the column header row
            f.write("\n")
            f.write(f"OBS{DLM}CURCODE{DLM}FREQ{DLM}CURRENT BALANCE{DLM}\n")

        nobs  += 1
        freq   = row.get("_FREQ_") or 0
        curbal = row.get("CURBAL") or 0.0

        # PUT @01 NOBS 3. DLM+(-1) CURCODE $3. DLM+(-1) _FREQ_ 10. DLM+(-1) CURBAL COMMA20.2;
        f.write(
            f"{nobs:3d}{DLM}"
            f"{str(row['CURCODE']):3s}{DLM}"
            f"{freq:10d}{DLM}"
            f"{curbal:>20,.2f}\n"
        )


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

fcy_currency_summary.write_parquet(OUTPUT_DIR / "FCY_summary.parquet")
fcy_currency_summary.write_csv(OUTPUT_DIR / "FCY_summary.csv")


# =============================================================================
# DATA _NULL_  (SET FCY END=LAST; FILE OUTFCY MOD;)
# Appends the summary section to the report file.
# =============================================================================
totifd = 0.0
totcfd = 0.0
totica = 0.0
totcca = 0.0
totiln = 0.0
totcln = 0.0

with open(out_txt, "a", encoding="utf-8") as f:

    n = 0

    for row in fcy_currency_summary.iter_rows(named=True):
        n += 1

        if n == 1:
            # PUT @1 / / "SUMMARY OF OUTSTANDING FCY LOAN AND DEPOSITS"
            #        / / 'NOBS' DLM+(-1) ... ;
            # Two '/' before heading = two blank lines
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

    # IF LAST THEN PUT ... (grand totals line)
    f.write(
        f"   {DLM}"
        f"TOTAL{DLM}"
        f"{totifd:>20,.2f}{DLM}"
        f"{totcfd:>20,.2f}{DLM}"
        f"{totica:>20,.2f}{DLM}"
        f"{totcca:>20,.2f}{DLM}"
        f"{totiln:>20,.2f}{DLM}"
        f"{totcln:>20,.2f}\n"
    )

print(f"\n EIBDOFCY_MYR_FCY completed — report date {RDATE}, output: {out_txt} \n")
print(fcy_currency_summary)
