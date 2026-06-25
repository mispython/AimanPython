# =============================================================================
# Program Name : EIBDFFCD
# Purpose      : Convert foreign currency fixed deposit balances to MYR and USD
#                equivalents using format-based exchange rates (FORATE).
#                SMR 2006-229. MNI3 / 10363.
# =============================================================================

import pandas as pd
import polars as pl
from pathlib import Path

from input_date import get_latest_file

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
FD_DIR      = BASE_DIR / "input" / "fd"
FCYFD_DIR   = BASE_DIR / "input" / "fcyfd"
OUTPUT_DIR  = BASE_DIR / "output" / "EIBDFFCD"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# FILE RESOLUTION
# fdXXXXXX.sas7bdat   → yymmdd  (e.g. fd260625.sas7bdat)
# fcyfdXXXXX.sas7bdat → mmwyy   (e.g. fcyfd06126.sas7bdat)
# =============================================================================
FD_PATH    = get_latest_file(FD_DIR,    prefix="fd")
FCYFD_PATH = get_latest_file(FCYFD_DIR, prefix="fcyfd")

# =============================================================================
# HELPER: $FORATE. format lookup
# In SAS: PROC FORMAT LIB=FCYFD CNTLOUT=FOFMT reads the format catalogue from
# the FCYFD library and re-applies it via PROC FORMAT CNTLIN=FOFMT, loading
# $FORATE. into session memory.
# Here we read the FCYFD sas7bdat directly and build an equivalent lookup dict.
#
# Expected FCYFD columns (standard SAS format CNTLOUT dataset):
#   FMTNAME, START, END, LABEL (LABEL holds the exchange rate as a string)
# =============================================================================

def load_forate_map(fcyfd_path: Path) -> dict[str, float]:
    """
    Reads the FCYFD format dataset and builds a currency-code → MYR rate map.
    Replicates PROC FORMAT LIB=FCYFD / CNTLIN=FOFMT / $FORATE. lookup.
    Only rows where FMTNAME = '$FORATE' (case-insensitive) are loaded.
    """
    pdf = pd.read_sas(str(fcyfd_path), encoding="latin1")
    pdf.columns = [c.upper() for c in pdf.columns]
    df = pl.from_pandas(pdf)

    forate_rows = df.filter(
        pl.col("FMTNAME").str.strip_chars().str.to_uppercase() == "$FORATE"
    )

    forate_map: dict[str, float] = {}
    for row in forate_rows.iter_rows(named=True):
        curcode = str(row.get("START", "") or "").strip().upper()
        label   = str(row.get("LABEL",  "") or "").strip()
        if curcode and label:
            try:
                forate_map[curcode] = float(label)
            except ValueError:
                # If LABEL is not a valid float, the rate is unparseable;
                # leave it absent so callers can detect the missing rate.
                pass

    return forate_map


def put_forate(curcode: str, forate_map: dict[str, float]):
    """
    Replicates SAS PUT(CURCODE, $FORATE.).
    Returns the float rate if found, or None if not found.
    In SAS a missing format value returns blank → subsequent arithmetic
    produces a missing (null) value, NOT a neutral 1.0 fallback.
    """
    return forate_map.get(curcode.strip().upper(), None)


# =============================================================================
# STEP 1: Read and sort FD dataset
# PROC SORT DATA=FD.FD OUT=FD BY ACCTNO CDNO;
# =============================================================================

def read_fd(path: Path) -> pl.DataFrame:
    pdf = pd.read_sas(str(path), encoding="latin1")
    pdf.columns = [c.upper() for c in pdf.columns]
    df = pl.from_pandas(pdf)
    df = df.sort(["ACCTNO", "CDNO"])
    return df


# =============================================================================
# STEP 2: Apply foreign currency conversion
# DATA FD.FD;
#   SET FD;
#   IF CURCODE NE 'MYR' THEN DO;
#     FORATE  = PUT(CURCODE, $FORATE.);
#     FORBAL  = CURBAL;
#     CURBAL  = ROUND(CURBAL * FORATE, .01);
#   END;
#   CURBALUS = CURBAL / PUT('USD', $FORATE.);
# RUN;
# =============================================================================

def apply_fx_conversion(df: pl.DataFrame, forate_map: dict[str, float]) -> pl.DataFrame:
    usd_rate = put_forate("USD", forate_map)

    rows = df.to_dicts()
    result = []

    for row in rows:
        curcode = str(row.get("CURCODE", "") or "").strip().upper()

        if curcode != "MYR":
            forate = put_forate(curcode, forate_map)
            row["FORATE"] = forate  # None if not found → mirrors SAS blank/missing

            curbal = row.get("CURBAL")
            row["FORBAL"] = curbal

            if curbal is not None and forate is not None:
                row["CURBAL"] = round(float(curbal) * forate, 2)
            else:
                # SAS: CURBAL * missing → missing
                row["CURBAL"] = None
        else:
            # MYR rows: FORATE and FORBAL are not assigned in the SAS DATA step
            row.setdefault("FORATE", None)
            row.setdefault("FORBAL", None)

        curbal_myr = row.get("CURBAL")
        if curbal_myr is not None and usd_rate is not None and usd_rate != 0:
            row["CURBALUS"] = float(curbal_myr) / usd_rate
        else:
            # SAS: missing / missing → missing
            row["CURBALUS"] = None

        result.append(row)

    return pl.from_dicts(result)


# =============================================================================
# STEP 3: Write updated FD dataset
# DATA FD.FD overwrites the member — output is the converted dataset.
# PROC PRINT is a diagnostic listing to the SAS output window only;
# no external report file is produced by this program.
# =============================================================================

def write_fd_output(df: pl.DataFrame) -> Path:
    out_path = OUTPUT_DIR / "FD.parquet"
    df.write_parquet(str(out_path))
    return out_path


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    print(f"FD input    : {FD_PATH}")
    print(f"FCYFD input : {FCYFD_PATH}")

    print("Loading $FORATE. format map from FCYFD...")
    forate_map = load_forate_map(FCYFD_PATH)
    print(f"  Loaded {len(forate_map)} currency rate(s): {list(forate_map.keys())}")

    print("Reading FD dataset...")
    fd_df = read_fd(FD_PATH)
    print(f"  Rows read: {len(fd_df)}")

    print("Applying FX conversion...")
    fd_converted = apply_fx_conversion(fd_df, forate_map)

    fcy_preview = fd_converted.filter(
        pl.col("CURCODE").str.strip_chars().str.to_uppercase() != "MYR"
    )
    print(f"  FCY rows converted : {len(fcy_preview)}")
    print(f"  MYR rows unchanged : {len(fd_converted) - len(fcy_preview)}")
    print(fcy_preview.select(["ACCTNO", "CDNO", "CURCODE", "FORATE", "FORBAL", "CURBAL", "CURBALUS"]))

    print("Writing output dataset...")
    out_path = write_fd_output(fd_converted)
    print(f"Output written to: {out_path}")

    print("Done.")
