#!/usr/bin/env python3
"""
Program  : EIBDLNEX.py
Purpose  : Daily Loan Extraction (ESMR 06-1428).
           Orchestrates the full daily loan extraction pipeline:
             1. Reads REPTDATE from DATEFILE and derives &RDATE macro.
             2. Writes REPTDATE to LOAN / NAME / ILOAN / INAME libraries.
             3. Calls EIBLNOTE (loan data preparation) and EIBLNEXT (routing).
             4. Applies FX spot-rate lookup via FORATE (SAP.PBB.FCYCA).
             5. Merges LNWOF write-down data into LOAN.LNNOTE:
                  - Overrides LOANTYPE with PRODUCT / ORICODE if present.
                  - Overrides CENSUS   with CENSUS_TRT if present.
                  - Attaches SPOTRATE for non-MYR accounts.
             6. Same merge for ILOAN.LNNOTE via ILNWOF (ESMR2011-3853).

           Dependencies:
             EIBLNOTE  - Loan data preparation (builds working datasets)
             EIBLNEXT  - Routes working datasets into LOAN / ILOAN libraries
             FORATE    - FX rate table (SAP.PBB.FCYCA -> FORATE.FORATEBKP)

           Scheduling note:
             In the original JCL the %OPC conditional runs a weekly "copy
             from prior period" branch on days 08, 15, 22, and last calendar
             day of month.  That branch (PROC COPY) is replicated here as a
             straight parquet copy using shutil; the daily branch (EIBLNOTE +
             EIBLNEXT + enrichment) runs otherwise.
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
import duckdb
import polars as pl

# ============================================================================
# DEPENDENCY IMPORTS
# %INC PGM(EIBLNOTE)
# %INC PGM(EIBLNEXT)
# ============================================================================
from EIBLNOTE import (
    build_feplan,
    build_feepo,
    build_loand,
    build_rnrhpmor,
    build_lnnote,
    enrich_lnnote_with_rate,
    build_oldnote,
    enrich_lnnote_with_oldnote,
    build_lnacct,
    build_lnacc4,
    build_lncomm,
    build_hpcomp,
    build_lnname,
    build_liab,
    build_name8,
    build_name9,
    build_lnrate,
    build_pend,
    NPL_DIR   as EIBLNOTE_NPL_DIR,
    LOAN_DIR  as EIBLNOTE_LOAN_DIR,
    ILOAN_DIR as EIBLNOTE_ILOAN_DIR,
)
from EIBLNEXT import (
    split_lnnote,
    split_lnacct,
    split_lnacc4,
    filter_liab,
    filter_pend,
    filter_lncomm,
    filter_name8,
    filter_name9,
    filter_lnname,
    LOAN_DIR  as EIBLNEXT_LOAN_DIR,
    ILOAN_DIR as EIBLNEXT_ILOAN_DIR,
    NAME_DIR  as EIBLNEXT_NAME_DIR,
    INAME_DIR as EIBLNEXT_INAME_DIR,
)

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR    = Path(".")

# Input source paths
DATE_DIR    = BASE_DIR / "data" / "pibb" / "date"        # DATEFILE
FORATE_DIR  = BASE_DIR / "data" / "pibb" / "forate"      # FORATE.FORATEBKP (SAP.PBB.FCYCA)
LNWOF_DIR   = BASE_DIR / "data" / "pibb" / "lnwof"       # SAP.PBB.LOANWOF
ILNWOF_DIR  = BASE_DIR / "data" / "pibb" / "ilnwof"      # SAP.PIBB.LOANWOF

# Working / output library paths (mirrors EIBLNOTE / EIBLNEXT)
NPL_DIR     = EIBLNOTE_NPL_DIR
LOAN_DIR    = EIBLNEXT_LOAN_DIR
ILOAN_DIR   = EIBLNEXT_ILOAN_DIR
NAME_DIR    = EIBLNEXT_NAME_DIR
INAME_DIR   = EIBLNEXT_INAME_DIR

# "Prior period" libraries (WLOAN / WILOAN / WNAME / WINAME in JCL)
WLOAN_DIR   = BASE_DIR / "data" / "pibb" / "wloan"
WILOAN_DIR  = BASE_DIR / "data" / "pibb" / "wiloan"
WNAME_DIR   = BASE_DIR / "data" / "pibb" / "wname"
WINAME_DIR  = BASE_DIR / "data" / "pibb" / "winame"

for _d in [LOAN_DIR, ILOAN_DIR, NAME_DIR, INAME_DIR, NPL_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# ============================================================================
# HELPERS
# ============================================================================
def _read(path: Path) -> pl.DataFrame:
    """Read a parquet file; return empty DataFrame if not found."""
    if not path.exists():
        return pl.DataFrame()
    return duckdb.connect().execute(
        f"SELECT * FROM read_parquet('{path}')"
    ).pl()


def _parse_z11_mmddyy(val) -> Optional[date]:
    """
    Replicates: INPUT(SUBSTR(PUT(val,Z11.),1,8),MMDDYY8.)
    Zero-pad to 11, take first 8 chars, parse as MMDDYY.
    """
    if val is None or val == 0:
        return None
    try:
        s = str(int(val)).zfill(11)[:8]
        return date(int(s[4:8]), int(s[0:2]), int(s[2:4]))
    except Exception:
        return None


def _copy_parquet_dir(src_dir: Path, dst_dir: Path) -> None:
    """
    Replicates PROC COPY IN=src OUT=dst: copies all *.parquet files from
    src_dir into dst_dir (creates dst_dir if missing).
    """
    dst_dir.mkdir(parents=True, exist_ok=True)
    for src_file in src_dir.glob("*.parquet"):
        shutil.copy2(src_file, dst_dir / src_file.name)


# ============================================================================
# STEP 0: Read REPTDATE and derive &RDATE macro
# DATA LOAN.REPTDATE NAME.REPTDATE ILOAN.REPTDATE INAME.REPTDATE;
#   INFILE DATEFILE LRECL=80 OBS=1;
#   INPUT @01 EXTDATE 11.;
#   REPTDATE = INPUT(SUBSTR(PUT(EXTDATE,Z11.),1,8),MMDDYY8.);
#   CALL SYMPUT('RDATE', REPTDATE);
# ============================================================================
def read_reptdate() -> Optional[date]:
    """
    Read EXTDATE from DATEFILE (first record only), derive REPTDATE via
    MMDDYY8. parsing, and return it as a Python date (= &RDATE macro).
    Also writes REPTDATE to all four output libraries.
    """
    datefile = DATE_DIR / "datefile.parquet"
    if not datefile.exists():
        print("WARNING: DATEFILE not found; RDATE will be None.")
        return None

    df = duckdb.connect().execute(
        f"SELECT EXTDATE FROM read_parquet('{datefile}') LIMIT 1"
    ).pl()

    if df.is_empty():
        print("WARNING: DATEFILE is empty; RDATE will be None.")
        return None

    extdate  = df["EXTDATE"][0]
    reptdate = _parse_z11_mmddyy(extdate)
    print(f"REPTDATE = {reptdate}")

    # Write REPTDATE record to all four libraries
    reptdate_df = pl.DataFrame({"REPTDATE": [reptdate]})
    for lib_dir in (LOAN_DIR, NAME_DIR, ILOAN_DIR, INAME_DIR):
        lib_dir.mkdir(parents=True, exist_ok=True)
        reptdate_df.write_parquet(str(lib_dir / "reptdate.parquet"))

    return reptdate


# ============================================================================
# STEP 1 (weekly branch): PROC COPY from prior-period libraries
# PROC COPY IN=WLOAN  OUT=LOAN;
# PROC COPY IN=WILOAN OUT=ILOAN;
# PROC COPY IN=WNAME  OUT=NAME;
# PROC COPY IN=WINAME OUT=INAME;
# ============================================================================
def weekly_copy() -> None:
    """
    Replicate PROC COPY from prior-period (W*) libraries into current
    output libraries.  Runs when today is on a scheduled weekly date
    (8th, 15th, 22nd, or last calendar day of the month).
    """
    print("Weekly branch: copying prior-period datasets...")
    _copy_parquet_dir(WLOAN_DIR,  LOAN_DIR)
    _copy_parquet_dir(WILOAN_DIR, ILOAN_DIR)
    _copy_parquet_dir(WNAME_DIR,  NAME_DIR)
    _copy_parquet_dir(WINAME_DIR, INAME_DIR)
    print("Weekly copy completed.")


# ============================================================================
# STEP 2 (daily branch): EIBLNOTE + EIBLNEXT pipeline
# %INC PGM(EIBLNOTE);
# %INC PGM(EIBLNEXT);
# ============================================================================
def run_eiblnote(rdate: Optional[date]) -> None:
    """Execute the full EIBLNOTE pipeline (loan data preparation)."""
    print("Running EIBLNOTE pipeline...")

    # ---- FEPLAN ----
    print("Building FEPLAN...")
    feplan = build_feplan()
    feplan.write_parquet(str(NPL_DIR / "feplan.parquet"))
    print(f"FEPLAN: {len(feplan):,} rows")

    # ---- FEEPO ----
    print("Building FEEPO...")
    feepo = build_feepo(feplan)
    feepo.write_parquet(str(NPL_DIR / "feepo.parquet"))
    print(f"FEEPO: {len(feepo):,} rows")

    # ---- LOAND ----
    print("Building LOAND...")
    loand = build_loand()
    loand.write_parquet(str(NPL_DIR / "loand.parquet"))
    print(f"LOAND: {len(loand):,} rows")

    # ---- RNRHPMOR ----
    print("Building RNRHPMOR...")
    rnrhpmor = build_rnrhpmor()
    rnrhpmor.write_parquet(str(NPL_DIR / "rnrhpmor.parquet"))
    print(f"RNRHPMOR: {len(rnrhpmor):,} rows")

    # ---- LNRATE ----
    print("Building LNRATE...")
    lnrate = build_lnrate(rdate)
    lnrate.write_parquet(str(NPL_DIR / "lnrate.parquet"))
    lnrate.write_parquet(str(EIBLNOTE_LOAN_DIR  / "lnrate.parquet"))
    lnrate.write_parquet(str(EIBLNOTE_ILOAN_DIR / "lnrate.parquet"))
    print(f"LNRATE: {len(lnrate):,} rows")

    # ---- PEND ----
    print("Building PEND...")
    pend = build_pend(lnrate)
    pend.write_parquet(str(NPL_DIR / "pend.parquet"))
    print(f"PEND: {len(pend):,} rows")

    # ---- LNNOTE (initial merge) ----
    print("Building LNNOTE (initial merge)...")
    lnnote = build_lnnote(loand, feepo, rnrhpmor, rdate)

    # ---- Enrich LNNOTE with USURYIDX rate ----
    lnnote = enrich_lnnote_with_rate(lnnote, lnrate)

    # ---- OLDNOTE ----
    print("Building OLDNOTE...")
    oldnote = build_oldnote(lnnote)
    oldnote.write_parquet(str(NPL_DIR / "oldnote.parquet"))
    print(f"OLDNOTE: {len(oldnote):,} rows")

    # ---- Final LNNOTE enrichment with OLDNOTE ----
    print("Enriching LNNOTE with OLDNOTE...")
    lnnote = enrich_lnnote_with_oldnote(lnnote, oldnote)
    lnnote.write_parquet(str(NPL_DIR / "lnnote.parquet"))
    print(f"LNNOTE: {len(lnnote):,} rows")

    # ---- LNACCT ----
    print("Building LNACCT...")
    lnacct = build_lnacct()
    lnacct.write_parquet(str(NPL_DIR / "lnacct.parquet"))
    print(f"LNACCT: {len(lnacct):,} rows")

    # ---- LNACC4 ----
    print("Building LNACC4...")
    lnacc4 = build_lnacc4()
    lnacc4.write_parquet(str(NPL_DIR / "lnacc4.parquet"))
    print(f"LNACC4: {len(lnacc4):,} rows")

    # ---- LNCOMM ----
    print("Building LNCOMM...")
    lncomm = build_lncomm()
    lncomm.write_parquet(str(NPL_DIR / "lncomm.parquet"))
    print(f"LNCOMM: {len(lncomm):,} rows")

    # ---- HPCOMP ----
    print("Building HPCOMP...")
    hpcomp = build_hpcomp()
    hpcomp.write_parquet(str(EIBLNOTE_LOAN_DIR  / "hpcomp.parquet"))
    hpcomp.write_parquet(str(EIBLNOTE_ILOAN_DIR / "hpcomp.parquet"))
    print(f"HPCOMP: {len(hpcomp):,} rows")

    # ---- LNNAME ----
    print("Building LNNAME...")
    lnname = build_lnname()
    lnname.write_parquet(str(NPL_DIR / "lnname.parquet"))
    print(f"LNNAME: {len(lnname):,} rows")

    # ---- LIAB ----
    print("Building LIAB...")
    liab = build_liab()
    liab.write_parquet(str(NPL_DIR / "liab.parquet"))
    print(f"LIAB: {len(liab):,} rows")

    # ---- NAME8 ----
    print("Building NAME8...")
    name8 = build_name8()
    name8.write_parquet(str(NPL_DIR / "name8.parquet"))
    print(f"NAME8: {len(name8):,} rows")

    # ---- NAME9 ----
    print("Building NAME9...")
    name9 = build_name9()
    name9.write_parquet(str(NPL_DIR / "name9.parquet"))
    print(f"NAME9: {len(name9):,} rows")

    print("EIBLNOTE pipeline completed.")


def run_eiblnext() -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Execute the full EIBLNEXT pipeline (routing into LOAN / ILOAN).
    Returns (loan_lnnote, iloan_lnnote) for downstream enrichment.
    """
    print("Running EIBLNEXT pipeline...")

    print("Splitting LNNOTE...")
    loan_lnnote, iloan_lnnote = split_lnnote()

    print("Splitting LNACCT...")
    split_lnacct()

    print("Splitting LNACC4...")
    split_lnacc4()

    print("Filtering LIAB...")
    filter_liab(loan_lnnote, iloan_lnnote)

    print("Filtering PEND...")
    filter_pend(loan_lnnote, iloan_lnnote)

    print("Filtering LNCOMM...")
    filter_lncomm(loan_lnnote, iloan_lnnote)

    print("Filtering NAME8...")
    filter_name8(loan_lnnote, iloan_lnnote)

    print("Filtering NAME9...")
    filter_name9(loan_lnnote, iloan_lnnote)

    print("Filtering LNNAME...")
    filter_lnname(loan_lnnote, iloan_lnnote)

    print("EIBLNEXT pipeline completed.")
    return loan_lnnote, iloan_lnnote


# ============================================================================
# STEP 3: Build FX rate lookup table
# PROC SORT DATA=FORATE.FORATEBKP OUT=FXRATE;
#   BY CURCODE DESCENDING REPTDATE;
#   WHERE REPTDATE <= TODAY()-1;
# PROC SORT DATA=FXRATE(KEEP=SPOTRATE CURCODE) NODUPKEY; BY CURCODE;
# DATA FOFMT; ... PROC FORMAT CNTLIN=FOFMT;
# ============================================================================
def build_fxrate() -> dict[str, float]:
    """
    Read FORATE.FORATEBKP, filter REPTDATE <= yesterday, keep the most
    recent SPOTRATE per CURCODE.
    Returns a dict {curcode: spotrate} replicating the $FORATE. format.
    """
    foratebkp = FORATE_DIR / "foratebkp.parquet"
    if not foratebkp.exists():
        print("WARNING: FORATE.FORATEBKP not found; SPOTRATE will not be applied.")
        return {}

    yesterday = date.today() - timedelta(days=1)

    df = duckdb.connect().execute(
        f"""
        SELECT CURCODE, SPOTRATE, REPTDATE
        FROM read_parquet('{foratebkp}')
        WHERE REPTDATE <= DATE '{yesterday}'
        ORDER BY CURCODE, REPTDATE DESC
        """
    ).pl()

    if df.is_empty():
        return {}

    # NODUPKEY by CURCODE -> keep first (most recent) per CURCODE
    df = df.unique(subset=["CURCODE"], keep="first")

    # Build {curcode: spotrate} dict (replicates $FORATE. character format)
    return {
        str(row["CURCODE"]).strip(): float(row["SPOTRATE"] or 0)
        for row in df.iter_rows(named=True)
        if row.get("CURCODE") and row.get("SPOTRATE") is not None
    }


# ============================================================================
# STEP 4: Merge LNWOF into LOAN.LNNOTE (ESMR2011-3795)
# DATA LNNOTE; SET LOAN.LNNOTE;
# DATA LNWOF(KEEP=ACCTNO NOTENO WRITE_DOWN_BAL PRODUCT CENSUS_TRT ORICODE);
#   SET LNWOF.LNWOF;
# DATA LOAN.LNNOTE(DROP=PRODUCT CENSUS_TRT);
#   MERGE LNNOTE(IN=A) LNWOF(IN=B); BY ACCTNO NOTENO;
#   IF PRODUCT NOT IN (.,0) THEN LOANTYPE=PRODUCT;
#   IF ORICODE NOT IN (.,0) THEN LOANTYPE=ORICODE;
#   IF CENSUS_TRT NOT IN (.,0) THEN CENSUS=CENSUS_TRT;
#   IF A;
#   IF CURCODE NE 'MYR' THEN SPOTRATE = PUT(CURCODE,$FORATE.)*1;
# ============================================================================
def enrich_lnnote_with_lnwof(
    lnnote: pl.DataFrame,
    lnwof_path: Path,
    fxrate: dict[str, float],
    out_path: Path,
) -> pl.DataFrame:
    """
    Merge LNNOTE (left) with LNWOF on ACCTNO/NOTENO.
    Apply LOANTYPE override from PRODUCT/ORICODE, CENSUS from CENSUS_TRT,
    and attach SPOTRATE for non-MYR accounts.
    Writes result to out_path and returns the enriched DataFrame.
    """
    lnwof_df = _read(lnwof_path)

    if not lnwof_df.is_empty():
        # Keep only required LNWOF columns
        keep_cols = [c for c in ("ACCTNO", "NOTENO", "WRITE_DOWN_BAL",
                                  "PRODUCT", "CENSUS_TRT", "ORICODE")
                     if c in lnwof_df.columns]
        lnwof_slim = lnwof_df.select(keep_cols)
        df = lnnote.join(lnwof_slim, on=["ACCTNO", "NOTENO"], how="left")
    else:
        df = lnnote
        # Ensure expected columns exist as nulls
        for col in ("PRODUCT", "CENSUS_TRT", "ORICODE", "WRITE_DOWN_BAL"):
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))

    rows = []
    for row in df.iter_rows(named=True):
        r = dict(row)

        # IF PRODUCT NOT IN (.,0) THEN LOANTYPE=PRODUCT
        product = r.get("PRODUCT")
        if product is not None and product != 0:
            r["LOANTYPE"] = product

        # IF ORICODE NOT IN (.,0) THEN LOANTYPE=ORICODE
        oricode = r.get("ORICODE")
        if oricode is not None and oricode != 0:
            r["LOANTYPE"] = oricode

        # IF CENSUS_TRT NOT IN (.,0) THEN CENSUS=CENSUS_TRT
        census_trt = r.get("CENSUS_TRT")
        if census_trt is not None and census_trt != 0:
            r["CENSUS"] = census_trt

        # IF CURCODE NE 'MYR' THEN SPOTRATE = PUT(CURCODE,$FORATE.)*1
        curcode = (r.get("CURCODE") or "").strip()
        if curcode and curcode != "MYR":
            r["SPOTRATE"] = fxrate.get(curcode)
        else:
            r.setdefault("SPOTRATE", None)

        # DROP=PRODUCT CENSUS_TRT
        r.pop("PRODUCT", None)
        r.pop("CENSUS_TRT", None)

        rows.append(r)

    result = pl.DataFrame(rows) if rows else pl.DataFrame()
    result.write_parquet(str(out_path))
    print(f"{out_path.name}: {len(result):,} rows")
    return result


# ============================================================================
# SCHEDULING HELPER: determine if today is a "weekly copy" day
# //*%OPC COMP=(&OCDATE..EQ.(08,15,22,&OCLASTC))
# Weekly branch fires on the 8th, 15th, 22nd, or last calendar day of month.
# ============================================================================
def _is_weekly_copy_day(run_date: Optional[date] = None) -> bool:
    """
    Return True if run_date (defaults to today) falls on a scheduled weekly
    copy day: 8th, 15th, 22nd, or the last day of the month.
    """
    d = run_date or date.today()
    # Last day of month: next month's 1st day minus one day
    if d.month == 12:
        last_day = date(d.year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(d.year, d.month + 1, 1) - timedelta(days=1)
    return d.day in (8, 15, 22, last_day.day)


# ============================================================================
# MAIN
# ============================================================================
def main():
    print("EIBDLNEX started.")

    # ---- Determine run branch ----
    # DATA _NULL_; CALL SYMPUT('RUN','N'); -> default
    # //*%OPC COMP=(&OCDATE..EQ.(08,15,22,&OCLASTC)) -> set to 'Y' on weekly days
    run_flag = "Y" if _is_weekly_copy_day() else "N"
    print(f"RUN flag = {run_flag}")

    if run_flag == "Y":
        # ---- Weekly branch: PROC COPY from prior-period libraries ----
        weekly_copy()
    else:
        # ---- Daily branch ----

        # Read REPTDATE -> &RDATE
        rdate = read_reptdate()

        # %INC PGM(EIBLNOTE)
        run_eiblnote(rdate)

        # %INC PGM(EIBLNEXT)
        _loan_lnnote, _iloan_lnnote = run_eiblnext()

    # ---- FX rate table (runs regardless of branch) ----
    print("Building FX rate table...")
    fxrate = build_fxrate()
    print(f"FX rates loaded: {len(fxrate)} currencies")

    # ---- Enrich LOAN.LNNOTE with LNWOF (ESMR2011-3795) ----
    print("Enriching LOAN.LNNOTE with LNWOF...")
    loan_lnnote_path = LOAN_DIR / "lnnote.parquet"
    loan_lnnote      = _read(loan_lnnote_path)
    if not loan_lnnote.is_empty():
        enrich_lnnote_with_lnwof(
            loan_lnnote,
            lnwof_path = LNWOF_DIR  / "lnwof.parquet",
            fxrate     = fxrate,
            out_path   = loan_lnnote_path,
        )

    # ---- Enrich ILOAN.LNNOTE with ILNWOF (ESMR2011-3853) ----
    print("Enriching ILOAN.LNNOTE with ILNWOF...")
    iloan_lnnote_path = ILOAN_DIR / "lnnote.parquet"
    iloan_lnnote      = _read(iloan_lnnote_path)
    if not iloan_lnnote.is_empty():
        enrich_lnnote_with_lnwof(
            iloan_lnnote,
            lnwof_path = ILNWOF_DIR / "ilnwof.parquet",
            fxrate     = fxrate,
            out_path   = iloan_lnnote_path,
        )

    print("EIBDLNEX completed successfully.")


if __name__ == "__main__":
    main()
