#!/usr/bin/env python3
"""
Program : EIBDPTER.py
Purpose : FCY Fixed Deposit Maturity Report
          - FDTEXT : Summary of FCY FD maturity by currency (T, T+1, T+2, >T+2)
          - FDDETL : FCY FD detail by customer (balance > FCY 1 million)
          Produces two comma-delimited flat text files (RECFM=FB, LRECL=320).

Dependencies : PBBELF   (format_brchcd)
               REPTDATE (get_reptdate_values)
               input_date (get_latest_file)
"""

# ============================================================
# Standard library
# ============================================================
from pathlib import Path
from datetime import date, timedelta
from typing import Optional
import math

# ============================================================
# Third-party
# ============================================================
import pandas as pd
import polars as pl

# ============================================================
# Internal dependencies
# ============================================================
from REPTDATE import get_reptdate_values
from PBBELF   import format_brchcd
from input_date import get_latest_file

# ============================================================
# PATH CONFIGURATION
# ============================================================
BASE_DIR   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
INPUT_DIR  = BASE_DIR / "input"   # directory containing fd.sas7bdat
OUTPUT_DIR = BASE_DIR / "output" / "EIBDPTER"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FD_SAS7BDAT = INPUT_DIR / "fd.sas7bdat"

OUTPUT_FDTEXT = OUTPUT_DIR / "FCYFD_REP1.txt"   # SAP.PBB.FCYFD.REP1.TEXT
OUTPUT_FDDETL = OUTPUT_DIR / "FCYFD_REP2.txt"   # SAP.PBB.FCYFD.REP2.TEXT

# ============================================================
# REPORT DATE  (replaces DATA REPTDATE / FD.REPTDATE)
# OPTIONS YEARCUTOFF=1950 — handled inside REPTDATE.py
# ============================================================
reptdate_values = get_reptdate_values()

REPTDATE = reptdate_values.reptdate          # date object  (T)
T1       = REPTDATE + timedelta(days=1)
T2       = REPTDATE + timedelta(days=2)
T3       = REPTDATE + timedelta(days=3)

REPDD  = reptdate_values.reptday             # DD  (Z2.)
REPMM  = reptdate_values.reptmon             # MM  (Z2.)
REPYY  = REPTDATE.strftime("%Y")             # 4-digit year (YEAR4.)
REPDT  = REPTDATE.strftime("%d/%m/%Y")       # DDMMYY8. equivalent  (dd/mm/yyyy)

# ============================================================
# HELPER UTILITIES
# ============================================================

def _parse_matdate(matdate_val) -> Optional[date]:
    """
    Replicate SAS:
      MATDAT = INPUT(MATDATE,$20.);
      YY = SUBSTR(MATDAT,1,4);
      MM = SUBSTR(MATDAT,5,2);
      DD = SUBSTR(MATDAT,7,2);
      MATDT = MDY(MM,DD,YY);

    MATDATE is stored as a numeric in the SAS dataset (likely YYYYMMDD integer).
    PUT(MATDATE,$20.) gives the string representation; the first 8 chars are YYYYMMDD.
    """
    if matdate_val is None or (isinstance(matdate_val, float) and math.isnan(matdate_val)):
        return None
    try:
        s = str(int(matdate_val)).zfill(8)   # e.g. "20251231"
        yy = int(s[0:4])
        mm = int(s[4:6])
        dd = int(s[6:8])
        return date(yy, mm, dd)
    except Exception:
        return None


def _parse_orgdate(orgdate_val) -> Optional[date]:
    """
    Replicate SAS:
      ORGDAT = INPUT(SUBSTR(PUT(ORGDATE, Z11.),1,8), MMDDYY10.);

    PUT(ORGDATE, Z11.) -> 11-char zero-padded numeric string
    SUBSTR(...,1,8)    -> first 8 characters   e.g. "01231998" (MMDDYYYY)
    INPUT(...,MMDDYY10.) -> parse as MM DD YY (YEARCUTOFF=1950: yy<50 -> 2000+yy, else 1900+yy)
    """
    if orgdate_val is None or (isinstance(orgdate_val, float) and math.isnan(orgdate_val)):
        return None
    try:
        s = str(int(orgdate_val)).zfill(11)[:8]   # first 8 chars of Z11. representation
        mm = int(s[0:2])
        dd = int(s[2:4])
        yy = int(s[4:6])
        # YEARCUTOFF=1950: two-digit years >= 50 -> 19xx, else 20xx
        # Note: s[4:8] could be 4-digit if the numeric was large enough.
        # MMDDYY10. reads only 2-digit year from position 5-6 of an 8-char string.
        year = (1900 + yy) if yy >= 50 else (2000 + yy)
        return date(year, mm, dd)
    except Exception:
        return None


def _fmt_date_ddmmyy8(d: Optional[date]) -> str:
    """Format date as DD/MM/YY (SAS DDMMYY8. produces dd/mm/yy)."""
    if d is None:
        return ""
    return d.strftime("%d/%m/%y")


# ============================================================
# STEP 1 : Read FD sas7bdat
# ============================================================

def read_fd() -> pd.DataFrame:
    """Read the entire FD dataset from the SAS7BDAT file."""
    df = pd.read_sas(str(FD_SAS7BDAT), encoding="latin1")
    return df


# ============================================================
# STEP 2 : DATA FCYT / FCYT1-4  — FCY maturity split
#
#   IF CUSTCD NOT IN (77,78,95,96);
#   IF CURCODE = 'MYR' THEN DELETE;
#   IF MATDATE = .    THEN DELETE;
#   CURBAL = CURBAL / FORATE;
#   Split into FCYT1 (MATDT=T1), FCYT2 (=T2), FCYT3 (=T3), FCYT4 (>T3)
# ============================================================

def build_fcyt(df: pd.DataFrame):
    """
    Apply FCY filters and convert CURBAL to MYR equivalent.
    Returns four sub-dataframes: fcyt1, fcyt2, fcyt3, fcyt4.
    """
    # Decode byte-string columns if necessary (pandas read_sas may return bytes)
    for col in ["CURCODE"]:
        if col in df.columns and df[col].dtype == object:
            df[col] = df[col].apply(
                lambda x: x.decode("latin1").strip() if isinstance(x, bytes) else str(x).strip()
            )

    # Filter: exclude CUSTCD in (77,78,95,96)
    df = df[~df["CUSTCD"].isin([77, 78, 95, 96])].copy()

    # Filter: exclude MYR
    df = df[df["CURCODE"] != "MYR"].copy()

    # Filter: MATDATE must not be missing
    df = df[df["MATDATE"].notna()].copy()

    # CURBAL = CURBAL / FORATE
    df["CURBAL"] = df["CURBAL"] / df["FORATE"]

    # Parse MATDATE -> Python date
    df["MATDT"] = df["MATDATE"].apply(_parse_matdate)
    df = df[df["MATDT"].notna()].copy()

    fcyt1 = df[df["MATDT"] == T1].copy()
    fcyt2 = df[df["MATDT"] == T2].copy()
    fcyt3 = df[df["MATDT"] == T3].copy()
    fcyt4 = df[df["MATDT"] >  T3].copy()

    return fcyt1, fcyt2, fcyt3, fcyt4


# ============================================================
# STEP 3 : PROC SUMMARY — summarise each FCYT bucket by CURCODE
#
#   CLASS CURCODE; VAR CURBAL RATE;
#   OUTPUT SUM=CURBAL<n> RATE<n>; RENAME _FREQ_=N<n>
# ============================================================

def summarise_bucket(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    """
    Equivalent of PROC SUMMARY NWAY by CURCODE for one maturity bucket.
    Returns a DataFrame with columns: CURCODE, CURBAL<n>, RATE<n>, N<n>.
    """
    if df.empty:
        return pd.DataFrame(columns=["CURCODE",
                                     f"CURBAL{suffix}",
                                     f"RATE{suffix}",
                                     f"N{suffix}"])
    grp = df.groupby("CURCODE", as_index=False).agg(
        **{f"CURBAL{suffix}": ("CURBAL", "sum"),
           f"RATE{suffix}":   ("RATE",   "sum"),
           f"N{suffix}":      ("CURBAL", "count")}
    )
    return grp


# ============================================================
# STEP 4 : DATA FCYT — merge four buckets and compute averages
#
#   MERGE FCYT1 FCYT2 FCYT3 FCYT4; BY CURCODE;
#   AVGRATE<n> = RATE<n> / N<n>;
#   AVGFIN<n>  = (retained sum of AVGRATE<n>) / 10  — see note below
#   AVGFIN     = SUM(AVGFIN1..4)
#   TOTFD      = SUM(CURBAL1,CURBAL2,CURABL3,CURBAL4)  ← SAS typo: CURABL3 = 0
#   TOTAVG     = SUM(AVGRATE1..4) / 4
#
# NOTE: AVGFIN variables are not printed in the DATA _NULL_ output; they are only
#       declared with FORMAT. The retain (+) accumulator affects their values
#       across rows but since they are unused in output, we compute them faithfully
#       for completeness but they do not appear in the text file.
# ============================================================

def build_fcyt_summary(s1: pd.DataFrame, s2: pd.DataFrame,
                       s3: pd.DataFrame, s4: pd.DataFrame) -> pd.DataFrame:
    """Merge four summary buckets and compute derived columns."""
    # Full outer merge by CURCODE (SAS MERGE semantics: last-dataset-wins for matching rows)
    merged = s1.merge(s2, on="CURCODE", how="outer")
    merged = merged.merge(s3, on="CURCODE", how="outer")
    merged = merged.merge(s4, on="CURCODE", how="outer")

    # Fill missing numeric values with 0 (SAS OPTIONS MISSING=0)
    num_cols = [c for c in merged.columns if c != "CURCODE"]
    merged[num_cols] = merged[num_cols].fillna(0)

    # AVGRATE<n> = RATE<n> / N<n>  (guard against div-by-zero)
    for n in ["1", "2", "3", "4"]:
        rate_col = f"RATE{n}"
        freq_col = f"N{n}"
        avg_col  = f"AVGRATE{n}"
        merged[avg_col] = merged.apply(
            lambda r: r[rate_col] / r[freq_col] if r[freq_col] != 0 else 0,
            axis=1
        )

    # AVGFIN<n>: SAS retain (+) then /10 each row.
    # With MISSING=0 and one row per currency, for the first currency row:
    #   AVGFIN<n> starts at 0, += AVGRATE<n>, then /= 10  => AVGRATE<n>/10
    # For subsequent rows the retained value carries forward (divided by 10 each time).
    # These columns are NOT output to the text file; computed for SAS fidelity.
    for n in ["1", "2", "3", "4"]:
        retain = 0.0
        avgfin_vals = []
        for _, row in merged.iterrows():
            retain = retain + row[f"AVGRATE{n}"]
            retain = retain / 10.0
            avgfin_vals.append(retain)
        merged[f"AVGFIN{n}"] = avgfin_vals

    merged["AVGFIN"] = (merged["AVGFIN1"] + merged["AVGFIN2"] +
                        merged["AVGFIN3"] + merged["AVGFIN4"])

    # TOTFD: SAS typo CURABL3 treated as 0 (MISSING=0); CURBAL3 intentionally omitted
    # to faithfully replicate: TOTFD = SUM(CURBAL1, CURBAL2, CURABL3, CURBAL4)
    merged["TOTFD"] = merged["CURBAL1"] + merged["CURBAL2"] + 0 + merged["CURBAL4"]

    # TOTAVG = SUM(AVGRATE1,AVGRATE2,AVGRATE3,AVGRATE4) / 4
    merged["TOTAVG"] = (merged["AVGRATE1"] + merged["AVGRATE2"] +
                        merged["AVGRATE3"] + merged["AVGRATE4"]) / 4.0

    # Sort ascending by CURCODE (SAS MERGE BY CURCODE implies sorted order)
    merged = merged.sort_values("CURCODE").reset_index(drop=True)

    return merged


# ============================================================
# STEP 5 : DATA _NULL_ FILE FDTEXT — write summary report
#
#   RECFM=FB, LRECL=320  (no ASA carriage control)
#   Comma-delimited columns at fixed @-positions
# ============================================================

def _pad(val, width: int) -> str:
    """Left-justify a value in a fixed-width field."""
    return str(val)[:width].ljust(width)


def _fmt_num(val, total_width: int, decimals: int = 2) -> str:
    """
    Format a numeric value right-justified in a fixed-width field.
    Replicates SAS 15.2 format behaviour.
    """
    try:
        formatted = f"{float(val):>{total_width}.{decimals}f}"
        return formatted[:total_width]
    except (ValueError, TypeError):
        return " " * total_width


def write_fdtext(fcyt: pd.DataFrame, repdt: str) -> None:
    """
    DATA _NULL_ FILE FDTEXT equivalent.

    Column layout (1-based @-positions from SAS PUT statements):
      Header line 1: 'FCYFD MATURITY BY CURRENCY AS AT ' + repdt  @001
      Header line 2: ',' at @001,@015,@030,@050,'MATURITY' at @050,',' at @074
      Header line 3: column sub-headers
      Header line 4: column labels
      Data lines    : one per CURCODE

    All lines are padded to LRECL=320.
    """
    LRECL = 320

    def build_line(spec: list) -> str:
        """
        spec: list of (1-based col position, value_str) tuples.
        Assembles a line by placing each value starting at the given position.
        """
        buf = [" "] * LRECL
        for pos1, val in spec:
            idx = pos1 - 1
            for i, ch in enumerate(val):
                if idx + i < LRECL:
                    buf[idx + i] = ch
        return "".join(buf)

    lines = []

    # IF _N_ = 1: header block
    hdr1 = build_line([(1, f"FCYFD MATURITY BY CURRENCY AS AT {repdt}")])
    lines.append(hdr1)

    hdr2 = build_line([
        (1,  ","),
        (15, ","),
        (30, ","),
        (50, "MATURITY"),
        (74, ","),
    ])
    lines.append(hdr2)

    hdr3 = build_line([
        (1,   ","),
        (15,  "T"),
        (49,  ","),
        (50,  "T1"),
        (99,  ","),
        (100, "T2"),
        (149, ","),
        (150, ">T2"),
    ])
    lines.append(hdr3)

    hdr4 = build_line([
        (1,   "CURRENCY"),
        (14,  ","),
        (15,  "AMOUNT"),
        (29,  ","),
        (30,  "AVG RATE"),
        (49,  ","),
        (50,  "AMOUNT"),
        (74,  ","),
        (75,  "AVG RATE"),
        (99,  ","),
        (100, "AMOUNT"),
        (124, ","),
        (125, "AVG RATE"),
        (149, ","),
        (150, "AMOUNT"),
        (174, ","),
        (175, "AVG RATE"),
        (199, ","),
        (200, "TOTAL FD"),
        (224, ","),
        (225, "AVG RATE"),
        (249, ","),
    ])
    lines.append(hdr4)

    # Data rows
    for _, row in fcyt.iterrows():
        curcode  = str(row["CURCODE"]).strip()
        curbal1  = float(row.get("CURBAL1",  0) or 0)
        avgrate1 = float(row.get("AVGRATE1", 0) or 0)
        curbal2  = float(row.get("CURBAL2",  0) or 0)
        avgrate2 = float(row.get("AVGRATE2", 0) or 0)
        curbal3  = float(row.get("CURBAL3",  0) or 0)
        avgrate3 = float(row.get("AVGRATE3", 0) or 0)
        curbal4  = float(row.get("CURBAL4",  0) or 0)
        avgrate4 = float(row.get("AVGRATE4", 0) or 0)
        totfd    = float(row.get("TOTFD",    0) or 0)
        totavg   = float(row.get("TOTAVG",   0) or 0)

        # SAS format 15.2 right-justified (total width 15, 2 decimals)
        data_line = build_line([
            (1,   curcode),
            (14,  ","),
            (15,  _fmt_num(curbal1,  15, 2)),
            (29,  ","),
            (30,  _fmt_num(avgrate1, 15, 2)),
            (49,  ","),
            (50,  _fmt_num(curbal2,  15, 2)),
            (74,  ","),
            (75,  _fmt_num(avgrate2, 15, 2)),
            (99,  ","),
            (100, _fmt_num(curbal3,  15, 2)),
            (124, ","),
            (125, _fmt_num(avgrate3, 15, 2)),
            (149, ","),
            (150, _fmt_num(curbal4,  15, 2)),
            (174, ","),
            (175, _fmt_num(avgrate4, 15, 2)),
            (199, ","),
            (200, _fmt_num(totfd,    15, 2)),
            (224, ","),
            (225, _fmt_num(totavg,   15, 2)),
            (249, ","),
        ])
        lines.append(data_line)

    with open(OUTPUT_FDTEXT, "w", encoding="latin1") as fh:
        for line in lines:
            fh.write(line + "\n")

    print(f"[FDTEXT] Written {len(lines)} lines -> {OUTPUT_FDTEXT}")
    # Print to terminal
    for line in lines:
        print(line)


# ============================================================
# STEP 6 : DATA CUST — detail dataset for customers > FCY 1M
#
#   KEEP=ORIGAMT ORGDATE MATDATE NAME CURCODE RATE CURBAL CUSTCD BRANCH
#   IF CUSTCD NOT IN (77,78,95,96);
#   IF CURCODE = 'MYR' THEN DELETE;
#   IF CURBAL > 1000000;
#   IF MATDATE = . THEN DELETE;
#   BRCHCD = PUT(BRANCH,BRCHCD.);
#   Sort by DESCENDING CURCODE DESCENDING CURBAL
#   Derive MATURE (DDMMYY8.) and ORIGIN (DDMMYY8.)
# ============================================================

def build_cust(df: pd.DataFrame) -> pd.DataFrame:
    """Build the CUST detail dataset."""
    keep_cols = ["ORIGAMT", "ORGDATE", "MATDATE", "NAME",
                 "CURCODE", "RATE", "CURBAL", "CUSTCD", "BRANCH"]
    # Keep only columns that exist in the dataset
    available = [c for c in keep_cols if c in df.columns]
    cust = df[available].copy()

    # Decode bytes
    for col in ["CURCODE", "NAME"]:
        if col in cust.columns and cust[col].dtype == object:
            cust[col] = cust[col].apply(
                lambda x: x.decode("latin1").strip() if isinstance(x, bytes) else str(x).strip()
            )

    # Filters
    cust = cust[~cust["CUSTCD"].isin([77, 78, 95, 96])].copy()
    cust = cust[cust["CURCODE"] != "MYR"].copy()
    cust = cust[cust["CURBAL"] > 1_000_000].copy()
    cust = cust[cust["MATDATE"].notna()].copy()

    # BRCHCD format
    cust["BRCHCD"] = cust["BRANCH"].apply(
        lambda x: format_brchcd(int(x)) if pd.notna(x) else ""
    )

    # Parse MATDATE -> MATURE (DDMMYY8.)
    cust["MATDT"]  = cust["MATDATE"].apply(_parse_matdate)
    cust["MATURE"] = cust["MATDT"].apply(_fmt_date_ddmmyy8)

    # Parse ORGDATE -> ORIGIN (DDMMYY8.)
    cust["ORGDAT"] = cust["ORGDATE"].apply(_parse_orgdate)
    cust["ORIGIN"] = cust["ORGDAT"].apply(_fmt_date_ddmmyy8)

    # Sort: DESCENDING CURCODE, DESCENDING CURBAL
    cust = cust.sort_values(["CURCODE", "CURBAL"], ascending=[False, False]).reset_index(drop=True)

    return cust


# ============================================================
# STEP 7 : DATA _NULL_ FILE FDDETL — write detail report
#
#   RECFM=FB, LRECL=320  (no ASA carriage control)
# ============================================================

def write_fddetl(cust: pd.DataFrame, repdt: str) -> None:
    """
    DATA _NULL_ FILE FDDETL equivalent.

    Column layout (@-positions, 1-based):
      Header 1: title
      Header 2: column labels
      Data:     one row per customer
    """
    LRECL = 320

    def build_line(spec: list) -> str:
        buf = [" "] * LRECL
        for pos1, val in spec:
            idx = pos1 - 1
            for i, ch in enumerate(val):
                if idx + i < LRECL:
                    buf[idx + i] = ch
        return "".join(buf)

    lines = []

    # IF _N_ = 1: header block
    hdr1 = build_line([(1, f"FCYFD DETAILS BY CUSTOMER(>FCY1MIL) AS AT {repdt}")])
    lines.append(hdr1)

    hdr2 = build_line([
        (1,   "BRANCH"),
        (10,  ","),
        (11,  "CUSTOMER"),
        (59,  ","),
        (60,  "SETTLEMENT AMT"),
        (84,  ","),
        (85,  "AVG RATE"),
        (109, ","),
        (110, "CURRENCY"),
        (134, ","),
        (135, "SETTLEMENT DATE"),
        (159, ","),
        (160, "MATURITY"),
        (168, ","),
    ])
    lines.append(hdr2)

    # Data rows
    for _, row in cust.iterrows():
        brchcd  = str(row.get("BRCHCD", "")).strip()
        name    = str(row.get("NAME",   "")).strip()
        origamt = float(row.get("ORIGAMT", 0) or 0)
        rate    = float(row.get("RATE",    0) or 0)
        curcode = str(row.get("CURCODE", "")).strip()
        origin  = str(row.get("ORIGIN",  "")).strip()
        mature  = str(row.get("MATURE",  "")).strip()

        data_line = build_line([
            (1,   brchcd[:9]),
            (10,  ","),
            (11,  name[:48]),
            (59,  ","),
            (60,  _fmt_num(origamt, 15, 2)),
            (84,  ","),
            (85,  _fmt_num(rate,    15, 2)),
            (109, ","),
            (110, curcode[:24]),
            (134, ","),
            (135, origin[:24]),
            (159, ","),
            (160, mature[:8]),
            (170, ","),
        ])
        lines.append(data_line)

    with open(OUTPUT_FDDETL, "w", encoding="latin1") as fh:
        for line in lines:
            fh.write(line + "\n")

    print(f"[FDDETL] Written {len(lines)} lines -> {OUTPUT_FDDETL}")
    # Print to terminal
    for line in lines:
        print(line)


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    print(f"[EIBDPTER] Report date : {REPDT}")
    print(f"[EIBDPTER] T  = {REPTDATE}")
    print(f"[EIBDPTER] T1 = {T1}")
    print(f"[EIBDPTER] T2 = {T2}")
    print(f"[EIBDPTER] T3 = {T3}")

    # --- Read FD dataset -----------------------------------------------
    print(f"[EIBDPTER] Reading {FD_SAS7BDAT} ...")
    fd_df = read_fd()

    # --- FCY maturity split --------------------------------------------
    fcyt1, fcyt2, fcyt3, fcyt4 = build_fcyt(fd_df)
    print(f"[EIBDPTER] Maturity split: T1={len(fcyt1)}, T2={len(fcyt2)}, "
          f"T3={len(fcyt3)}, >T3={len(fcyt4)}")

    # --- PROC SUMMARY per bucket ---------------------------------------
    s1 = summarise_bucket(fcyt1, "1")
    s2 = summarise_bucket(fcyt2, "2")
    s3 = summarise_bucket(fcyt3, "3")
    s4 = summarise_bucket(fcyt4, "4")

    # --- Merge and derive averages ------------------------------------
    fcyt = build_fcyt_summary(s1, s2, s3, s4)

    # --- Write FDTEXT (summary) ----------------------------------------
    write_fdtext(fcyt, REPDT)

    # --- Build CUST detail dataset -------------------------------------
    cust = build_cust(fd_df)
    print(f"[EIBDPTER] Customers with FCY balance > 1M: {len(cust)}")

    # --- Write FDDETL (detail) -----------------------------------------
    write_fddetl(cust, REPDT)

    print(f"[EIBDPTER] Output FDTEXT : {OUTPUT_FDTEXT}")
    print(f"[EIBDPTER] Output FDDETL : {OUTPUT_FDDETL}")
    print("[EIBDPTER] Done.")


if __name__ == "__main__":
    main()
