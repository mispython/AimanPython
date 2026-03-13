#!/usr/bin/env python3
"""
Program : EINTRERR.py
Purpose : DUPLICATION CHECK (LOAN INTRADAY FILES)
          Checks each of the five loan intraday parquet datasets for duplicate rows.
          If any duplicates are found, prints an error report
            and exits with abend code 77 (equivalent to ABORT ABEND 77).

Original JCL job : EINTRERR
MSGCLASS=X  MSGLEVEL=(1,1)  REGION=64M
Step VERIFY   EXEC SAS609
  LOAN  DD DSN=SAP.LOAN.FIX.LATE  DISP=SHR
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import polars as pl

# ---------------------------------------------------------------------------
# OPTIONS NOCENTER NODATE NONUMBER
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
BASE_DIR   = Path(__file__).resolve().parent
INPUT_DIR  = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"

INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# LOAN DD → input directory  (SAP.LOAN.FIX.LATE)
LOAN_DIR = INPUT_DIR / "LOAN_FIX_LATE"
LOAN_DIR.mkdir(parents=True, exist_ok=True)

SASLIST_TXT = OUTPUT_DIR / "EINTRERR_SASLIST.txt"

# ---------------------------------------------------------------------------
# Datasets to check  (%EXEC macro calls: RNR, RNRMOR, RNRHP, RNRHPMOR, AKPK)
# ---------------------------------------------------------------------------
DATASETS = ["RNR", "RNRMOR", "RNRHP", "RNRHPMOR", "AKPK"]


# ---------------------------------------------------------------------------
# %MACRO EXEC(DSN) equivalent
# PROC CONTENTS → get observation count (row count) per dataset.
# PROC APPEND   → accumulate into DUPERR list.
# ---------------------------------------------------------------------------
def get_nobs(dsn: str) -> int:
    """
    Return the number of rows (NOBS) in LOAN.<DSN> parquet.
    Equivalent of PROC CONTENTS DATA=LOAN.&DSN OUT=TEMP NOPRINT
    followed by reading TEMP(OBS=1).NOBS.
    """
    path = LOAN_DIR / f"{dsn}.parquet"
    if not path.exists():
        print(f"WARNING: {path} not found – treating NOBS as 0.")
        return 0
    con = duckdb.connect()
    row = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()
    con.close()
    return int(row[0]) if row else 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    # Build DUPERR equivalent: list of (MEMNAME, NOBS)
    duperr: list[tuple[str, int]] = []
    for dsn in DATASETS:
        nobs = get_nobs(dsn)
        duperr.append((dsn, nobs))

    # DATA _NULL_ – write status report to SASLIST
    title = "DUPLICATION CHECK - LOAN INTRADAY FILES"
    output_lines: list[str] = [title, ""]

    freqcn = 0
    for memname, nobs in duperr:
        status = "OK" if nobs == 0 else "ERROR"
        # PUT @01 MEMNAME @09 ':' NOBS 5. @16 '( ' STATUS ')'
        line = f"{memname:<8}:{nobs:>5}  ( {status} )"
        output_lines.append(line)
        freqcn += nobs

    # Write SASLIST output
    SASLIST_TXT.write_text("\n".join(output_lines) + "\n", encoding="utf-8")
    print("\n".join(output_lines))

    # IF EOF AND FREQCN > 0 → duplicate found
    if freqcn > 0:
        error_box = (
            "+--------+--------+-------+-------+--------+-------+\n"
            "|  ERROR MESSAGE(S) STATUS                         |\n"
            "+--------+--------+-------+-------+--------+-------+\n"
            "|  DUPLICATE FOUND !! SET TO COMPLETE &            |\n"
            "|     EMAIL SASLIST OUTPUT TO LOANS & SAS-TEAM     |\n"
            "+========+========+=======+=======+========+=======+"
        )
        print(error_box)
        # Append error box to SASLIST file
        with SASLIST_TXT.open("a", encoding="utf-8") as fh:
            fh.write("\n" + error_box + "\n")

        # ABORT ABEND 77
        sys.exit(77)


if __name__ == "__main__":
    main()
