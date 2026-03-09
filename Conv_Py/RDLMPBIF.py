#!/usr/bin/env python3
"""
Program  : RDLMPBIF.py
Purpose  : Process PBIF (Public Bank Islamic Financing) client data -
             merge with mechanical charges (MECHRG), compute FIU balances,
             disbursements, repayments, undrawn amounts, and derive
             next billing date (MATDTE) based on FREQ and STDATES.
           Output: PBIF dataset (deduplicated by CLIENTNO, MATDTE).

           Note   : This module is included (%INC PGM(RDLMPBIF)) by a calling
                    program when REPTQ='Y' (last day of month / quarter-end).
                    Callable via main() with date parameters passed from caller.
"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
"""
- %INC PGM(PBBLNFMT) is present in the original SAS source as suite boilerplate.
- After reviewing all format calls in this program, no PBBLNFMT format function
    (format_lndenom, format_lnprod, format_custcd, etc.) is actually invoked.
- All CUSTFISS remapping is done via inline if/elif logic below.
- Therefore no import from PBBLNFMT is required.
"""

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime
from typing import Optional
from dateutil.relativedelta import relativedelta

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR        = Path(".")
PBIF_DIR        = BASE_DIR / "data" / "pbif"
REPTDATE_FILE   = BASE_DIR / "data" / "bnm" / "reptdate.parquet"

# Dynamic input: PBIF.CLIEN&REPTYEAR&REPTMON&REPTDAY
# These are resolved at runtime from REPTDATE
MECHRG_FILE     = BASE_DIR / "data" / "mechrg.txt"   # INFILE MECHRG (fixed-width text)

OUTPUT_DIR      = BASE_DIR / "output"
OUTPUT_PBIF     = OUTPUT_DIR / "pbif.parquet"          # default; overridden in main()
OUTPUT_REPORT   = OUTPUT_DIR / "rdlmpbif_report.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# DAYS-IN-MONTH ARRAYS  (%MACRO DCLVAR)
# D1-D12:  standard month lengths (base: 31, overrides for 30-day months)
# RD1-RD12, MD1-MD12: same pattern (used for RETAIN; values same as LDAY)
# ============================================================================

def days_in_month(mm: int, yy: int) -> int:
    """Return days in month mm of year yy (replicates LDAY array + leap logic)."""
    if mm == 2:
        return 29 if (yy % 4 == 0) else 28
    if mm in (4, 6, 9, 11):
        return 30
    return 31


# ============================================================================
# %MACRO NXTBLDT: advance MATDTE by FREQ months, clamping day to month end
# ============================================================================

def next_billing_date(matdte: date, freq: int) -> date:
    """
    Replicate %MACRO NXTBLDT:
      MM = MONTH(MATDTE) + FREQ
      If MM > 12: MM -= 12, YY += 1
      If DD > days_in_month(MM, YY): DD = days_in_month(MM, YY)
      MATDTE = MDY(MM, DD, YY)
    Note: FREQ is always 6 or 12 so MM can only overflow by 12 max.
    """
    dd = matdte.day
    mm = matdte.month + freq
    yy = matdte.year
    if mm > 12:
        mm -= 12
        yy += 1
    max_dd = days_in_month(mm, yy)
    if dd > max_dd:
        dd = max_dd
    return date(yy, mm, dd)


# ============================================================================
# CUSTFISS remapping (mirrors IF/ELSE IF chain in SAS)
# ============================================================================

def remap_custfiss(custcd) -> str:
    """
    Replicate CUSTFISS remapping logic:
      IF CUSTFISS IN ('41','42','43','66') THEN CUSTFISS='41'
      IF CUSTFISS IN ('44','47','67')      THEN CUSTFISS='44'
      IF CUSTFISS IN ('46')               THEN CUSTFISS='46'
      IF CUSTFISS IN ('48','49','51','68') THEN CUSTFISS='48'
      IF CUSTFISS IN ('52','53','54','69') THEN CUSTFISS='52'
      (else: unchanged)
    """
    c = str(custcd or "").strip()
    if c in ("41", "42", "43", "66"):
        return "41"
    if c in ("44", "47", "67"):
        return "44"
    if c == "46":
        return "46"
    if c in ("48", "49", "51", "68"):
        return "48"
    if c in ("52", "53", "54", "69"):
        return "52"
    return c


# ============================================================================
# STEP 1: DATA PBIF
#   SET PBIF.CLIEN&REPTYEAR&REPTMON&REPTDAY
#   IF ENTITY='PBBH'
#   Derive APPRLIMX, PRODCD, FISSPURP, AMTIND, CUSTFISS, CUSTCX
# ============================================================================

def build_pbif_step1(df: pl.DataFrame) -> pl.DataFrame:
    """
    Apply DATA PBIF step 1 transformations:
    - APPRLIMX = INLIMIT
    - PRODCD   = '30591'
    - FISSPURP = '0470'
    - AMTIND   = 'D'
    - CUSTFISS = remap(CUSTCD)
    - CUSTCX   = CUSTFISS
    """
    rows = df.to_dicts()
    out  = []
    for r in rows:
        r["APPRLIMX"] = r.get("INLIMIT")
        r["PRODCD"]   = "30591"
        r["FISSPURP"] = "0470"
        r["AMTIND"]   = "D"
        custfiss      = remap_custfiss(r.get("CUSTCD"))
        r["CUSTFISS"] = custfiss
        r["CUSTCX"]   = custfiss
        out.append(r)
    return pl.DataFrame(out) if out else pl.DataFrame()


# ============================================================================
# STEP 2: DATA MECHRG
#   INFILE MECHRG (fixed-width)
#   @001 CLIENTNO $9.  @010 PDATE YYMMDD8.  @020 UVAL1 12.2
#   @034 UVAL2 12.2    @048 UVAL3 12.2
#   INTVAL = SUM(UVAL1, UVAL2, UVAL3)
#   IF PDATE = &MDATE
#   Then PROC SUMMARY NWAY: CLASS CLIENTNO; VAR INTVAL; SUM=
# ============================================================================

def read_mechrg(filepath: Path, mdate: date) -> pl.DataFrame:
    """
    Read fixed-width MECHRG file and filter by PDATE = mdate.
    SAS column positions (1-based):
      @001 CLIENTNO  $9.  -> chars [0:9]
      @010 PDATE YYMMDD8. -> chars [9:17]  (format YYMMDD8. = YYYYMMDD)
      @020 UVAL1    12.2  -> chars [19:31]
      @034 UVAL2    12.2  -> chars [33:45]
      @048 UVAL3    12.2  -> chars [47:59]
    """
    rows = []
    if not filepath.exists():
        return pl.DataFrame({
            "CLIENTNO": pl.Series([], dtype=pl.Utf8),
            "INTVAL":   pl.Series([], dtype=pl.Float64),
        })

    mdate_str = mdate.strftime("%Y%m%d")

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            # Pad line to avoid index errors
            line = line.rstrip("\n").ljust(60)

            clientno = line[0:9].strip()
            pdate_s  = line[9:17].strip()
            uval1_s  = line[19:31].strip()
            uval2_s  = line[33:45].strip()
            uval3_s  = line[47:59].strip()

            if pdate_s != mdate_str:
                continue

            def safe_float(s: str) -> float:
                try:
                    return float(s) if s else 0.0
                except ValueError:
                    return 0.0

            intval = safe_float(uval1_s) + safe_float(uval2_s) + safe_float(uval3_s)
            rows.append({"CLIENTNO": clientno, "INTVAL": intval})

    if not rows:
        return pl.DataFrame({
            "CLIENTNO": pl.Series([], dtype=pl.Utf8),
            "INTVAL":   pl.Series([], dtype=pl.Float64),
        })

    raw = pl.DataFrame(rows)

    # PROC SUMMARY NWAY: CLASS CLIENTNO; VAR INTVAL; SUM=
    return (
        raw.group_by("CLIENTNO")
           .agg(pl.col("INTVAL").sum())
    )


# ============================================================================
# STEP 3: MERGE PBIF(IN=A) MECHRG; BY CLIENTNO
#   IF A
#   IF FIU=0.00 AND PRMTHFIU=0.00 THEN DELETE
#   IF INTVAL=. THEN INTVAL=0.00
#   FIU = SUM(FIU, INTVAL, PRMTHFIU)
#   BALANCE = FIU
#   UFIU=0; DISBURSE=0; REPAID=0; ROLLOVER=0
#   IF BALANCE < 0  THEN BALANCE=0
#   IF FIU     < 0  THEN UFIU=FIU
#   IF PRMTHFIU< 0  THEN PRMTHFIU=0
#   IF BALANCE >= 0 THEN:
#     IF BALANCE > PRMTHFIU THEN DISBURSE = BALANCE - PRMTHFIU
#                           ELSE REPAID   = PRMTHFIU - BALANCE
#   UNDRAWN = INLIMIT - BALANCE
#   IF FIU=0.00 THEN DELETE
# ============================================================================

def apply_balance_logic(df: pl.DataFrame) -> pl.DataFrame:
    """
    Replicate DATA PBIF merge/balance derivation logic.
    """
    rows = df.to_dicts()
    out  = []
    for r in rows:
        fiu      = float(r.get("FIU")      or 0.0)
        prmthfiu = float(r.get("PRMTHFIU") or 0.0)
        intval   = r.get("INTVAL")
        inlimit  = float(r.get("INLIMIT")  or 0.0)

        # IF FIU=0.00 AND PRMTHFIU=0.00 THEN DELETE
        if fiu == 0.0 and prmthfiu == 0.0:
            continue

        # IF INTVAL=. THEN INTVAL=0.00
        if intval is None:
            intval = 0.0
        else:
            intval = float(intval)

        # FIU = SUM(FIU, INTVAL, PRMTHFIU)
        fiu = fiu + intval + prmthfiu
        r["FIU"]      = fiu
        r["INTVAL"]   = intval

        # BALANCE = FIU
        balance  = fiu
        ufiu     = 0.0
        disburse = 0.0
        repaid   = 0.0
        rollover = 0.0

        # IF BALANCE < 0 THEN BALANCE = 0
        if balance < 0.0:
            balance = 0.0

        # IF FIU < 0 THEN UFIU = FIU
        if fiu < 0.0:
            ufiu = fiu

        # IF PRMTHFIU < 0 THEN PRMTHFIU = 0
        if prmthfiu < 0.0:
            prmthfiu = 0.0

        # IF BALANCE >= 0:
        if balance >= 0.0:
            if balance > prmthfiu:
                disburse = balance - prmthfiu
            else:
                repaid = prmthfiu - balance

        # UNDRAWN = INLIMIT - BALANCE
        undrawn = inlimit - balance

        r["BALANCE"]  = balance
        r["UFIU"]     = ufiu
        r["DISBURSE"] = disburse
        r["REPAID"]   = repaid
        r["ROLLOVER"] = rollover
        r["UNDRAWN"]  = undrawn
        r["PRMTHFIU"] = prmthfiu

        # IF FIU = 0.00 THEN DELETE
        if fiu == 0.0:
            continue

        out.append(r)

    return pl.DataFrame(out) if out else pl.DataFrame()


# ============================================================================
# PROC PRINT (intermediate diagnostic print — replicated as text output)
#   VAR BRANCH CLIENTNO BALANCE CUSTCX FISSPURP INLIMIT UNDRAWN
#       SECTORCD DISBURSE REPAID FIU ACCTNO PRMTHFIU UFIU INTVAL
#   SUM BALANCE REPAID DISBURSE UNDRAWN FIU PRMTHFIU UFIU INTVAL
# ============================================================================

PRINT_COLS = [
    "BRANCH", "CLIENTNO", "BALANCE", "CUSTCX", "FISSPURP",
    "INLIMIT", "UNDRAWN", "SECTORCD", "DISBURSE", "REPAID",
    "FIU", "ACCTNO", "PRMTHFIU", "UFIU", "INTVAL",
]
SUM_COLS = ["BALANCE", "REPAID", "DISBURSE", "UNDRAWN", "FIU",
            "PRMTHFIU", "UFIU", "INTVAL"]


def fmt_val(val, width: int = 14) -> str:
    if val is None:
        return "0".rjust(width)
    try:
        f = float(val)
        return f"{f:,.2f}".rjust(width)
    except (TypeError, ValueError):
        return str(val).rjust(width)


def write_proc_print(pbif: pl.DataFrame, report_path: Path) -> None:
    """Write intermediate diagnostic PROC PRINT to text file."""
    print_lines: list[str] = []

    available_cols = [c for c in PRINT_COLS if c in pbif.columns]
    print_lines.append(" " + "  ".join(f"{c:>14}" for c in available_cols))
    print_lines.append(" " + "-" * (16 * len(available_cols)))

    totals: dict[str, float] = {c: 0.0 for c in SUM_COLS}

    for row in pbif.iter_rows(named=True):
        cells = "  ".join(fmt_val(row.get(c)) for c in available_cols)
        print_lines.append(f" {cells}")
        for c in SUM_COLS:
            if c in row and row[c] is not None:
                try:
                    totals[c] += float(row[c])
                except (TypeError, ValueError):
                    pass

    # SUM row
    print_lines.append(" " + "-" * (16 * len(available_cols)))
    sum_cells = "  ".join(
        fmt_val(totals.get(c, "")) if c in SUM_COLS else " " * 14
        for c in available_cols
    )
    print_lines.append(f" {sum_cells}")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(print_lines) + "\n")


# ============================================================================
# STEP 4: DATA PBIF (second pass)
#   DROP CUSTCD
#   %DCLVAR (RETAIN arrays — handled via days_in_month function)
#   Read REPTDATE on first row (_N_=1)
#   Derive FREQ: 12 if INLIMIT < 1000000, else 6
#   Derive MATDTE:
#     Start at REPTDATE
#     IF STDATES > 0: start at STDATES, advance by FREQ until > REPTDATE
# ============================================================================

def compute_matdte(df: pl.DataFrame, reptdate: date) -> pl.DataFrame:
    """
    DATA PBIF (DROP CUSTCD):
      FREQ = 6 if INLIMIT >= 1000000 else 12.
      MATDTE = REPTDATE.
      IF STDATES > 0: MATDTE = STDATES; advance while MATDTE <= REPTDATE.
    PROC SORT NODUPKEY; BY CLIENTNO MATDTE;
    """
    pbif_rows = df.to_dicts()
    step2_rows = []

    for r in pbif_rows:
        # DROP CUSTCD
        r.pop("CUSTCD", None)

        inlimit = float(r.get("INLIMIT") or 0.0)
        freq    = 6 if inlimit >= 1_000_000.0 else 12

        stdates = r.get("STDATES")
        matdte  = reptdate

        if stdates is not None and stdates != 0:
            # Convert STDATES to date if necessary
            if isinstance(stdates, datetime):
                stdates = stdates.date()
            elif isinstance(stdates, (int, float)) and stdates > 0:
                # SAS date numeric: days since 01-Jan-1960
                try:
                    from datetime import timedelta
                    sas_epoch = date(1960, 1, 1)
                    stdates   = sas_epoch + timedelta(days=int(stdates))
                except (TypeError, ValueError, OverflowError):
                    stdates   = reptdate

            matdte = stdates
            # DO WHILE (MATDTE <= REPTDATE): advance by FREQ
            safety = 0
            while matdte <= reptdate:
                matdte = next_billing_date(matdte, freq)
                safety += 1
                if safety > 1000:
                    break   # guard against infinite loop

        r["MATDTE"] = matdte
        r["FREQ"]   = freq
        step2_rows.append(r)

    out = pl.DataFrame(step2_rows) if step2_rows else pl.DataFrame()

    # PROC SORT DATA=PBIF OUT=PBIF NODUPKEY; BY CLIENTNO MATDTE
    if not out.is_empty():
        out = (
            out.sort(["CLIENTNO", "MATDTE"])
               .unique(subset=["CLIENTNO", "MATDTE"], keep="first")
        )
    return out


# ============================================================================
# MAIN — callable by EIBDFACT (and standalone)
# ============================================================================

def main(reptdate: Optional[date] = None,
         reptyear: Optional[str]  = None,
         reptmon:  Optional[str]  = None,
         reptday:  Optional[str]  = None,
         mdate:    Optional[date] = None) -> pl.DataFrame:
    """
    Entry point for RDLMPBIF — called by EIBDFACT when REPTQ='Y'
    (quarter-end / last-day-of-month path).
    If date parameters are not supplied, reads from REPTDATE_FILE.
    Returns enriched PBIF dataframe.
    """
    con = duckdb.connect()

    if reptdate is None:
        reptdate_df  = con.execute(
            f"SELECT * FROM read_parquet('{REPTDATE_FILE}')"
        ).pl()
        row_rep      = reptdate_df.row(0, named=True)
        reptdate_val = row_rep["REPTDATE"]
        if isinstance(reptdate_val, datetime):
            reptdate_val = reptdate_val.date()
        reptdate = reptdate_val
        reptyear = f"{reptdate.year}"
        reptmon  = f"{reptdate.month:02d}"
        reptday  = f"{reptdate.day:02d}"

    if mdate is None:
        mdate = reptdate

    # ----------------------------------------------------------------
    # STEP 1: Load and filter PBIF.CLIEN<REPTYEAR><REPTMON><REPTDAY>
    # ----------------------------------------------------------------
    clien_file = PBIF_DIR / f"clien{reptyear}{reptmon}{reptday}.parquet"
    pbif_raw   = con.execute(
        f"SELECT * FROM read_parquet('{clien_file}') WHERE ENTITY = 'PBBH'"
    ).pl()

    pbif = build_pbif_step1(pbif_raw).sort("CLIENTNO")

    # ----------------------------------------------------------------
    # STEP 2: Load and aggregate MECHRG
    # ----------------------------------------------------------------
    mechrg = read_mechrg(MECHRG_FILE, mdate)

    # ----------------------------------------------------------------
    # STEP 3: Merge PBIF + MECHRG; apply balance logic
    # ----------------------------------------------------------------
    pbif_merged = pbif.join(mechrg, on="CLIENTNO", how="left")
    pbif        = apply_balance_logic(pbif_merged)

    # ----------------------------------------------------------------
    # PROC PRINT (intermediate diagnostic)
    # ----------------------------------------------------------------
    write_proc_print(pbif, OUTPUT_REPORT)

    # ----------------------------------------------------------------
    # STEP 4: Compute MATDTE; deduplicate
    # ----------------------------------------------------------------
    pbif_final = compute_matdte(pbif, reptdate)

    # ----------------------------------------------------------------
    # Write output parquet
    # ----------------------------------------------------------------
    out_path = OUTPUT_DIR / f"PBIF{reptyear}{reptmon}{reptday}.parquet"
    pbif_final.write_parquet(out_path)

    return pbif_final


if __name__ == "__main__":
    main()
