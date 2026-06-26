"""
Program : EIBDFFCD.py
Purpose : Convert foreign currency fixed deposit balances to MYR and USD
          equivalents using format-based exchange rates (FORATE).
"""

import pandas as pd
import polars as pl
from pathlib import Path
import re
from typing import Optional

from input_date import get_latest_file

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
INPUT_DIR  = BASE_DIR / "input" / "prod" / "EIBDFFCD"
OUTPUT_DIR = BASE_DIR / "output" / "EIBDFFCD"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# FILE RESOLUTION
# fcyfdXXXXXX.sas7bdat  → yymmdd strictly (6-digit, rejects 5-digit mmwyy)
# fcyXXXXXX.sas7bdat    → yymmdd strictly (6-digit, rejects 5-digit mmwyy)
# fd is not used — the SAS FD libref points to fcyfd data, not the fd file.
# =============================================================================

def get_latest_yymmdd(directory: Path, prefix: str) -> Path:
    """
    Resolves the latest file with the given prefix using strictly yymmdd
    (6-digit) suffix. Rejects mmwyy (5-digit) variants sharing the same prefix.
    """
    pattern = re.compile(
        rf"^{re.escape(prefix)}(\d{{2}})(\d{{2}})(\d{{2}})\.sas7bdat$",
        re.IGNORECASE,
    )
    candidates = []
    for f in directory.iterdir():
        m = pattern.match(f.name)
        if m:
            yy, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
            year = 2000 + yy if yy < 100 else yy
            candidates.append(((year, mm, dd), f))
    if not candidates:
        raise FileNotFoundError(
            f"No {prefix} yymmdd files found in {directory}"
        )
    latest = max(candidates, key=lambda x: x[0])
    print(f"[FILE_RESOLVER] Selected latest {prefix} (yymmdd): {latest[1].name}")
    return latest[1]


FCYFD_PATH = get_latest_yymmdd(INPUT_DIR, prefix="fcyfd")
FCY_PATH   = get_latest_yymmdd(INPUT_DIR, prefix="fcy")

# =============================================================================
# $FORATE. exchange rate map
# In SAS, PROC FORMAT LIB=FCYFD loads $FORATE. from the FCYFD format catalogue
# (.sas7bcat) into session memory. This catalogue is separate from the FCYFD
# .sas7bdat data file. The PUT(CURCODE,$FORATE.) call performs a rate lookup.
# Since .sas7bcat files are not readable in Python, populate FORATE_MAP from
# your external rate source (DB, config, etc.).
# A missing currency code returns None — replicating SAS blank/missing, which
# causes downstream arithmetic to produce missing (null), not a fake rate.
# =============================================================================
FORATE_MAP: dict[str, float] = {
    # Populate from your external rate source (DB, config, etc.)
    # "USD": 4.47,
    # "SGD": 3.30,
    # "GBP": 5.60,
    # "EUR": 4.85,
    # "JPY": 0.030,
}


def put_forate(curcode: str) -> Optional[float]:
    """
    Replicates SAS PUT(CURCODE, $FORATE.).
    Returns float rate if found, None if not found.
    SAS returns blank on missing format → arithmetic produces missing (null).
    """
    return FORATE_MAP.get(str(curcode).strip().upper(), None)


# =============================================================================
# STEP 1: Read inputs
# =============================================================================

def read_sas(path: Path) -> pl.DataFrame:
    pdf = pd.read_sas(str(path), encoding="latin1")
    pdf.columns = [c.upper() for c in pdf.columns]
    return pl.from_pandas(pdf)


# =============================================================================
# STEP 2: Attach CDNO from FCY into FCYFD via ACCTNO join
# FCY is only used to supply CDNO — all other columns come from FCYFD.
# PROC SORT DATA=FD.FD OUT=FD BY ACCTNO CDNO sorts the joined result.
# =============================================================================

def attach_cdno(fcyfd_df: pl.DataFrame, fcy_df: pl.DataFrame) -> pl.DataFrame:
    cdno_df = fcy_df.select(["ACCTNO", "CDNO"])
    joined = fcyfd_df.join(cdno_df, on="ACCTNO", how="left")
    joined = joined.sort(["ACCTNO", "CDNO"])
    return joined


# =============================================================================
# STEP 3: Apply foreign currency conversion
# DATA FD.FD;
#   SET FD;     ← FD here is the sorted FCYFD work dataset (with CDNO attached)
#   IF CURCODE NE 'MYR' THEN DO;
#     FORATE  = PUT(CURCODE, $FORATE.);
#     FORBAL  = CURBAL;
#     CURBAL  = ROUND(CURBAL * FORATE, .01);
#   END;
#   CURBALUS = CURBAL / PUT('USD', $FORATE.);
# RUN;
# =============================================================================

def apply_fx_conversion(df: pl.DataFrame) -> pl.DataFrame:
    usd_rate = put_forate("USD")

    rows = df.to_dicts()
    result = []

    for row in rows:
        curcode = str(row.get("CURCODE", "") or "").strip().upper()

        if curcode != "MYR":
            forate = put_forate(curcode)
            row["FORATE"] = forate

            curbal = row.get("CURBAL")
            row["FORBAL"] = curbal

            if curbal is not None and forate is not None:
                row["CURBAL"] = round(float(curbal) * forate, 2)
            else:
                # SAS: CURBAL * missing → missing
                row["CURBAL"] = None
        else:
            # MYR rows: FORATE and FORBAL not assigned in SAS DATA step
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
# STEP 4: Write output dataset
# DATA FD.FD overwrites the FD library member with FCY-converted records.
# PROC PRINT WHERE CURCODE NE 'MYR' is a diagnostic listing to SAS output
# window only — no external report file is produced by this program.
# =============================================================================

def write_output(df: pl.DataFrame) -> Path:
    out_path = OUTPUT_DIR / "FD.parquet"
    df.write_parquet(str(out_path))
    return out_path


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    print(f"FCYFD input : {FCYFD_PATH}")
    print(f"FCY input   : {FCY_PATH}")

    print("Reading FCYFD dataset...")
    fcyfd_df = read_sas(FCYFD_PATH)
    print(f"  FCYFD rows : {len(fcyfd_df)}")

    print("Reading FCY dataset (for CDNO)...")
    fcy_df = read_sas(FCY_PATH)
    print(f"  FCY rows   : {len(fcy_df)}")

    print("Attaching CDNO from FCY and sorting by ACCTNO, CDNO...")
    fcyfd_with_cdno = attach_cdno(fcyfd_df, fcy_df)
    print(f"  Rows after join : {len(fcyfd_with_cdno)}")

    print("Applying FX conversion...")
    fd_converted = apply_fx_conversion(fcyfd_with_cdno)

    fcy_non_myr = fd_converted.filter(
        pl.col("CURCODE").str.strip_chars().str.to_uppercase() != "MYR"
    )
    print(f"  FCY rows converted : {len(fcy_non_myr)}")
    print(f"  MYR rows unchanged : {len(fd_converted) - len(fcy_non_myr)}")
    print(fcy_non_myr.select([
        c for c in ["ACCTNO", "CDNO", "CURCODE", "FORATE", "FORBAL", "CURBAL", "CURBALUS"]
        if c in fcy_non_myr.columns
    ]))

    print("Writing output dataset...")
    out_path = write_output(fd_converted)
    print(f"Output written to   : {out_path}")

    print("Done.")
