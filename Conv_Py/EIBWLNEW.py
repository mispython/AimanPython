#!/usr/bin/env python3
"""
Program : EIBWLNEW.py
Purpose : NPL loan data preparation orchestrator.
          - Reads DATEFILE to derive REPTDATE and broadcast to LOAN/ILOAN/NAME/INAME.
          - Invokes EIBLNOTE and EIBLNEXT (loan note preparation and routing).
          - Builds FXRATE spot-rate lookup from FORATE.FORATEBKP parquet.
          - Updates LOAN.LNNOTE with product-code / census-tract overrides
            from LNWOF (PBB conventional)    (ESMR2011-3795).
          - Updates ILOAN.LNNOTE with product-code / census-tract overrides
            from ILNWOF (PIBB Islamic)        (ESMR2011-3853).

Original JCL job : EIBWLNEW
  MSGCLASS=X  MSGLEVEL=(1,1)  REGION=8M
  Step SAS609

Change history (from JCL):
  NPH  25/09/2006  2006-1235  XFR BR BTW204, BAM060, PRA121 TO 801.
  MAA3 19-10-2006  2006-1338  IDIC TO MAP FROM ACCT LEVEL CUSTCODE
  MAA3 14-11-2006  2006-0956  STATUS DT FOR REGULARISED ACCTS

OPC conditional DD routing (replicated as runtime date check):
  If today's day is in (8, 15, 22, last-calendar-day):
      LOAN  → output/loan/      (SAP.PBB.MNILN(+1))
      ILOAN → output/iloan/     (SAP.PIBB.MNILN(+1))
  Otherwise:
      LOAN  → output/loan1/     (SAP.PBB.MNILN1(+1))
      ILOAN → output/iloan1/    (SAP.PIBB.MNILN1(+1))
"""

from __future__ import annotations

import calendar
from datetime import date
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl

# Dependency programs
import EIBLNOTE
import EIBLNEXT

# ---------------------------------------------------------------------------
# OPTIONS NOCENTER YEARCUTOFF=1950 LS=132
# ---------------------------------------------------------------------------
YEARCUTOFF = 1950

# ---------------------------------------------------------------------------
# Path setup  (DD name → local directory/file)
# ---------------------------------------------------------------------------
BASE_DIR   = Path(__file__).resolve().parent
INPUT_DIR  = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"

# Input DD mappings
DATEFILE_PATH  = INPUT_DIR / "DATEFILE.dat"          # RBP2.SAS.B033.DATEFILE   (LRECL=80)
FORATE_DIR     = INPUT_DIR / "forate"                # SAP.PBB.FCYCA  (FORATE library)
LNWOF_DIR      = INPUT_DIR / "lnwof"                 # SAP.PBB.LOANWOF(0)
ILNWOF_DIR     = INPUT_DIR / "ilnwof"                # SAP.PIBB.LOANWOF(0)

# Output library roots
# OPC conditional: use MNILN(+1) on week-end dates, else MNILN1(+1)
# Resolution deferred to runtime in _resolve_loan_dirs().
LOAN_DIR_MNILN   = OUTPUT_DIR / "loan"               # SAP.PBB.MNILN(+1)
LOAN_DIR_MNILN1  = OUTPUT_DIR / "loan1"              # SAP.PBB.MNILN1(+1)
ILOAN_DIR_MNILN  = OUTPUT_DIR / "iloan"              # SAP.PIBB.MNILN(+1)
ILOAN_DIR_MNILN1 = OUTPUT_DIR / "iloan1"             # SAP.PIBB.MNILN1(+1)
NAME_DIR         = OUTPUT_DIR / "name"               # SAP.PBB.LNNAME
INAME_DIR        = OUTPUT_DIR / "iname"              # SAP.PIBB.LNNAME

for _d in [LOAN_DIR_MNILN, LOAN_DIR_MNILN1, ILOAN_DIR_MNILN, ILOAN_DIR_MNILN1,
           NAME_DIR, INAME_DIR, FORATE_DIR, LNWOF_DIR, ILNWOF_DIR]:
    _d.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# OPC conditional: determine which LOAN/ILOAN output directories to use
# %OPC SETFORM OCDATE=(DD) / OCLASTC=(DD)
# Week-boundary days: 8, 15, 22, and last calendar day of the month.
# ---------------------------------------------------------------------------
def _resolve_loan_dirs(reptdate: date) -> tuple[Path, Path]:
    """
    Return (loan_dir, iloan_dir) based on the OPC date condition:
      OCDATE IN (08, 15, 22, OCLASTC)  → MNILN(+1)
      otherwise                         → MNILN1(+1)
    """
    last_day = calendar.monthrange(reptdate.year, reptdate.month)[1]
    if reptdate.day in (8, 15, 22, last_day):
        return LOAN_DIR_MNILN, ILOAN_DIR_MNILN
    return LOAN_DIR_MNILN1, ILOAN_DIR_MNILN1


# ---------------------------------------------------------------------------
# DATA LOAN.REPTDATE / NAME.REPTDATE / ILOAN.REPTDATE / INAME.REPTDATE
# INFILE DATEFILE LRECL=80 OBS=1;
# INPUT @01 EXTDATE 11.;
# REPTDATE = INPUT(SUBSTR(PUT(EXTDATE,Z11.),1,8), MMDDYY8.)
# ---------------------------------------------------------------------------
def read_reptdate() -> date:
    """
    Read first 80-byte record of DATEFILE, extract EXTDATE (11 digits at col 1),
    parse as MMDDYYYY → Python date.
    """
    with open(DATEFILE_PATH, "rb") as fh:
        rec = fh.read(80)

    raw = rec[0:11].decode("latin-1", errors="replace").strip()
    extdate_str = raw.zfill(11)[:8]   # PUT(EXTDATE,Z11.) then SUBSTR(,1,8)
    # MMDDYY8.: positions MM(0:2) DD(2:4) YY(4:8)
    mm = int(extdate_str[0:2])
    dd = int(extdate_str[2:4])
    yy = int(extdate_str[4:8])
    return date(yy, mm, dd)


def write_reptdate(reptdate: date, *dirs: Path) -> None:
    """Write REPTDATE parquet to each output library directory."""
    df = pl.DataFrame({"REPTDATE": [reptdate]})
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
        df.write_parquet(str(d / "reptdate.parquet"))


# ---------------------------------------------------------------------------
# PROC SORT DATA=FORATE.FORATEBKP OUT=FXRATE  (keep latest rate per CURCODE)
# BY CURCODE DESCENDING REPTDATE; WHERE REPTDATE <= &RDATE;
# PROC SORT NODUPKEY; BY CURCODE;
# ---------------------------------------------------------------------------
def build_fxrate(reptdate: date) -> pl.DataFrame:
    """
    Read FORATE.FORATEBKP parquet, filter REPTDATE <= reptdate,
    keep most-recent SPOTRATE per CURCODE.
    """
    path = FORATE_DIR / "foratebkp.parquet"
    if not path.exists():
        return pl.DataFrame(schema={"CURCODE": pl.Utf8, "SPOTRATE": pl.Float64})

    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    con.close()

    df = df.filter(pl.col("REPTDATE") <= reptdate)
    df = (
        df.sort(["CURCODE", "REPTDATE"], descending=[False, True])
          .unique(subset=["CURCODE"], keep="first")
          .select(["CURCODE", "SPOTRATE"])
    )
    return df


# ---------------------------------------------------------------------------
# DATA FOFMT – build format CNTLIN table (PROC FORMAT CNTLIN=FOFMT)
# Produces a dict used as $FORATE. lookup: CURCODE → SPOTRATE string
# ---------------------------------------------------------------------------
def build_forate_lookup(fxrate: pl.DataFrame) -> dict[str, float]:
    """Return {CURCODE: SPOTRATE} mapping (equivalent of $FORATE. format)."""
    return {
        row["CURCODE"]: float(row["SPOTRATE"])
        for row in fxrate.iter_rows(named=True)
    }


# ---------------------------------------------------------------------------
# Apply SPOTRATE to LNNOTE where CURCODE != 'MYR'
# IF CURCODE NE 'MYR' THEN SPOTRATE = PUT(CURCODE,$FORATE.)*1;
# ---------------------------------------------------------------------------
def _apply_spotrate(df: pl.DataFrame, forate: dict[str, float]) -> pl.DataFrame:
    """Add/update SPOTRATE column using the $FORATE. lookup."""
    if "SPOTRATE" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("SPOTRATE"))

    rows = []
    for row in df.iter_rows(named=True):
        r = dict(row)
        curcode = (r.get("CURCODE") or "").strip()
        if curcode != "MYR":
            r["SPOTRATE"] = forate.get(curcode)
        rows.append(r)
    return pl.DataFrame(rows)


# ---------------------------------------------------------------------------
# Generic LNNOTE update with LNWOF overrides
# Covers both PBB (ESMR2011-3795) and PIBB (ESMR2011-3853) logic.
# ---------------------------------------------------------------------------
def update_lnnote_with_wof(
    lnnote_path: Path,
    lnwof_path: Path,
    out_path: Path,
    forate: dict[str, float],
    label: str,
) -> None:
    """
    Replicate:
      DATA LNNOTE; SET <lib>.LNNOTE; RUN;
      DATA LNWOF(KEEP=ACCTNO NOTENO WRITE_DOWN_BAL PRODUCT CENSUS_TRT ORICODE);
        SET <wof>.LNWOF; RUN;
      DATA <lib>.LNNOTE(DROP=PRODUCT CENSUS_TRT);
        MERGE LNNOTE(IN=A) LNWOF(IN=B); BY ACCTNO NOTENO;
        IF PRODUCT NOT IN (.,0) THEN LOANTYPE=PRODUCT;
        IF ORICODE NOT IN (.,0) THEN LOANTYPE=ORICODE;
        IF CENSUS_TRT NOT IN (.,0) THEN CENSUS=CENSUS_TRT;
        IF A;
        IF CURCODE NE 'MYR' THEN SPOTRATE = PUT(CURCODE,$FORATE.)*1;
      RUN;
    """
    print(f"  [{label}] Loading LNNOTE from {lnnote_path}")
    con = duckdb.connect()
    lnnote = con.execute(f"SELECT * FROM read_parquet('{lnnote_path}')").pl()
    con.close()

    lnwof_keep = ["ACCTNO", "NOTENO", "WRITE_DOWN_BAL", "PRODUCT", "CENSUS_TRT", "ORICODE"]

    if lnwof_path.exists():
        print(f"  [{label}] Loading LNWOF from {lnwof_path}")
        con2  = duckdb.connect()
        lnwof = con2.execute(f"SELECT * FROM read_parquet('{lnwof_path}')").pl()
        con2.close()
        lnwof = lnwof.select([c for c in lnwof_keep if c in lnwof.columns])
    else:
        print(f"  [{label}] LNWOF not found – skipping WOF merge.")
        lnwof = pl.DataFrame(schema={c: pl.Utf8 for c in lnwof_keep})

    # Left-merge LNNOTE (A) with LNWOF (B) on ACCTNO / NOTENO
    df = lnnote.join(lnwof, on=["ACCTNO", "NOTENO"], how="left", suffix="_WOF")

    # Apply overrides row-by-row
    rows = []
    for row in df.iter_rows(named=True):
        r = dict(row)

        product    = r.get("PRODUCT")
        oricode    = r.get("ORICODE")
        census_trt = r.get("CENSUS_TRT")

        # IF PRODUCT NOT IN (.,0) THEN LOANTYPE=PRODUCT
        if product is not None and product != 0:
            r["LOANTYPE"] = product

        # IF ORICODE NOT IN (.,0) THEN LOANTYPE=ORICODE
        if oricode is not None and oricode != 0:
            r["LOANTYPE"] = oricode

        # IF CENSUS_TRT NOT IN (.,0) THEN CENSUS=CENSUS_TRT
        if census_trt is not None and census_trt != 0:
            r["CENSUS"] = census_trt

        rows.append(r)

    df = pl.DataFrame(rows)

    # DROP=PRODUCT CENSUS_TRT
    drop_cols = [c for c in ("PRODUCT", "CENSUS_TRT") if c in df.columns]
    if drop_cols:
        df = df.drop(drop_cols)

    # IF CURCODE NE 'MYR' THEN SPOTRATE = PUT(CURCODE,$FORATE.)*1
    df = _apply_spotrate(df, forate)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(str(out_path))
    print(f"  [{label}] Written {len(df):,} rows → {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    print("EIBWLNEW started.")

    # ------------------------------------------------------------------
    # DATA LOAN.REPTDATE / NAME.REPTDATE / ILOAN.REPTDATE / INAME.REPTDATE
    # ------------------------------------------------------------------
    reptdate = read_reptdate()
    print(f"REPTDATE = {reptdate}")

    # Resolve OPC-conditional output directories
    loan_dir, iloan_dir = _resolve_loan_dirs(reptdate)
    print(f"LOAN  library → {loan_dir}")
    print(f"ILOAN library → {iloan_dir}")

    write_reptdate(reptdate, loan_dir, iloan_dir, NAME_DIR, INAME_DIR)

    # PROC PRINT DATA=LOAN.REPTDATE  (informational)
    print(f"LOAN.REPTDATE: {reptdate}")

    # ------------------------------------------------------------------
    # %INC PGM(EIBLNOTE)
    # ------------------------------------------------------------------
    print("Invoking EIBLNOTE...")
    EIBLNOTE.main()

    # ------------------------------------------------------------------
    # %INC PGM(EIBLNEXT)
    # ------------------------------------------------------------------
    print("Invoking EIBLNEXT...")
    EIBLNEXT.main()

    # ------------------------------------------------------------------
    # PROC SORT FORATE.FORATEBKP → FXRATE (spot rates)
    # ------------------------------------------------------------------
    print("Building FXRATE lookup...")
    fxrate = build_fxrate(reptdate)
    forate = build_forate_lookup(fxrate)
    print(f"FXRATE: {len(forate)} currency codes loaded.")

    # ------------------------------------------------------------------
    # UPDATE PRODUCT CODE CENSUS TRACT FOR PBB (ESMR2011-3795)
    # DATA LOAN.LNNOTE – merge with LNWOF
    # ------------------------------------------------------------------
    print("Updating LOAN.LNNOTE with WOF overrides (PBB)...")
    update_lnnote_with_wof(
        lnnote_path = loan_dir  / "lnnote.parquet",
        lnwof_path  = LNWOF_DIR / "lnwof.parquet",
        out_path    = loan_dir  / "lnnote.parquet",
        forate      = forate,
        label       = "PBB LOAN",
    )

    # ------------------------------------------------------------------
    # UPDATE PRODUCT CODE CENSUS TRACT IN PIBB (ESMR2011-3853)
    # DATA ILOAN.LNNOTE – merge with ILNWOF
    # ------------------------------------------------------------------
    print("Updating ILOAN.LNNOTE with WOF overrides (PIBB)...")
    update_lnnote_with_wof(
        lnnote_path = iloan_dir  / "lnnote.parquet",
        lnwof_path  = ILNWOF_DIR / "ilnwof.parquet",
        out_path    = iloan_dir  / "lnnote.parquet",
        forate      = forate,
        label       = "PIBB ILOAN",
    )

    print("EIBWLNEW completed successfully.")


if __name__ == "__main__":
    main()
