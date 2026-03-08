#!/usr/bin/env python3
"""
Program  : EIBDRB02.py
Purpose  : Daily Summary Report on Reasons for Over-the-Counter Withdrawals
           Based on Receipts by Branch.

           Produces two CSV-style semicolon-delimited reports:
             1. RM Fixed Deposit Withdrawals   (RMWDRAW)
             2. FCY Fixed Deposit Withdrawals  (FCYWDRAW)

           Each report is broken into four sub-groups (J=1..4):
             J=1  RM / PBB  / Individual     (CURCODE='MYR', ACCTTYPE='C', CUSTCODE in 77,78,95,96)
             J=2  RM / PIBB / Individual     (CURCODE='MYR', ACCTTYPE='I', CUSTCODE in 77,78,95,96)
             J=3  RM / PBB  / Non-Individual (CURCODE='MYR', ACCTTYPE='C', CUSTCODE NOT in 77,78,95,96)
             J=4  RM / PIBB / Non-Individual (CURCODE='MYR', ACCTTYPE='I', CUSTCODE NOT in 77,78,95,96)
           For FCY the same split applies but CURCODE != 'MYR'.

           Dependencies:
             PBBELF  - Format definitions (BRCHCD, REGNEW lookups)

           Input files (parquet):
             DEPOSIT.REPTDATE   - Reporting date table
             MIS.FDWDRW{MM}     - Monthly FD withdrawal transactions
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
from pathlib import Path
from datetime import date

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
import duckdb
import polars as pl

# ============================================================================
# DEPENDENCY IMPORTS
# %INC PGM(PBBELF)
# ============================================================================
from PBBELF import format_brchcd, format_regnew

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR     = Path(".")
DEPOSIT_DIR  = BASE_DIR / "data" / "deposit"   # DEPOSIT.REPTDATE
MIS_DIR      = BASE_DIR / "data" / "mis"        # MIS.FDWDRW{MM}
OUTPUT_DIR   = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# REASON CODES  (W01 .. W16)
# ============================================================================
REASON_CODES = [f"W{i:02d}" for i in range(1, 17)]   # 'W01'..'W16'

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


def _safe_div(num: float, den: float) -> float:
    """Division with zero-denominator guard."""
    return (num / den) * 100.0 if den and den != 0 else 0.0


# ============================================================================
# STEP 1: Read REPTDATE
# DATA REPTDATE; SET DEPOSIT.REPTDATE;
#   CALL SYMPUT('REPTYEAR', PUT(REPTDATE,YEAR4.));
#   CALL SYMPUT('REPTMON',  PUT(MONTH(REPTDATE),Z2.));
#   CALL SYMPUT('RPTDATE',  PUT(REPTDATE,8.));
#   CALL SYMPUT('RDATE',    PUT(REPTDATE,DDMMYY8.));
# ============================================================================
def read_reptdate() -> dict:
    """
    Read REPTDATE from DEPOSIT.REPTDATE and derive the macro equivalents:
      REPTYEAR  - 4-digit year string
      REPTMON   - 2-digit zero-padded month string
      RPTDATE   - SAS numeric date value (days since 1960-01-01) as integer
      RDATE     - DDMMYY8. formatted string e.g. '31/01/25'
      reptdate  - Python date object
    """
    path = DEPOSIT_DIR / "reptdate.parquet"
    df   = _read(path)
    if df.is_empty():
        raise FileNotFoundError(f"DEPOSIT.REPTDATE not found at {path}")

    raw = df["REPTDATE"][0]
    if isinstance(raw, (int, float)):
        # SAS date numeric: days since 1960-01-01
        reptdate = date(1960, 1, 1).toordinal() + int(raw) - 1
        reptdate = date.fromordinal(reptdate)
    elif isinstance(raw, date):
        reptdate = raw
    else:
        reptdate = date.fromisoformat(str(raw))

    return {
        "reptdate" : reptdate,
        "REPTYEAR" : str(reptdate.year),
        "REPTMON"  : f"{reptdate.month:02d}",
        "RPTDATE"  : str((reptdate - date(1960, 1, 1)).days),  # SAS numeric
        "RDATE"    : reptdate.strftime("%d/%m/%y"),            # DDMMYY8.
    }


# ============================================================================
# STEP 2: Load and filter MIS.FDWDRW{REPTMON}
# DATA WDRAW;
#   SET MIS.FDWDRW&REPTMON;
#   WHERE REPTDATE EQ &RPTDATE;
#   TYPE   = 1;
#   BRABBR = PUT(BRANCH, BRCHCD.);
#   REGION = PUT(BRANCH, REGNEW.);
# ============================================================================
def load_wdraw(reptmon: str, rptdate_numeric: int) -> pl.DataFrame:
    """
    Load MIS.FDWDRW{MM}, filter to reporting date, derive BRABBR / REGION.
    rptdate_numeric is the SAS days-since-1960 integer.
    """
    path = MIS_DIR / f"fdwdrw{reptmon}.parquet"
    df   = _read(path)
    if df.is_empty():
        return pl.DataFrame()

    # WHERE REPTDATE EQ &RPTDATE (SAS numeric date comparison)
    df = df.filter(pl.col("REPTDATE") == rptdate_numeric)

    # TYPE = 1
    df = df.with_columns(pl.lit(1).alias("TYPE"))

    # BRABBR = PUT(BRANCH, BRCHCD.)
    # REGION = PUT(BRANCH, REGNEW.)
    df = df.with_columns([
        pl.col("BRANCH").map_elements(
            lambda b: format_brchcd(int(b)) if b is not None else "",
            return_dtype=pl.Utf8
        ).alias("BRABBR"),
        pl.col("BRANCH").map_elements(
            lambda b: format_regnew(int(b)) if b is not None else "",
            return_dtype=pl.Utf8
        ).alias("REGION"),
    ])
    return df


# ============================================================================
# STEP 3a: %RMGROUP – split WDRAW into 4 groups (CURCODE='MYR', PRODCD≠394)
# DATA WDRAW1 WDRAW2 WDRAW3 WDRAW4;
#   SET WDRAW;
#   IF CURCODE EQ 'MYR' THEN DO;
#     IF PRODCD NE 394;
#       IF ACCTTYPE='C' THEN ...
#       ELSE IF ACCTTYPE='I' THEN ...
#   END;
# ============================================================================
def rmgroup(wdraw: pl.DataFrame) -> tuple[pl.DataFrame, ...]:
    """Split WDRAW into four RM sub-groups."""
    base = wdraw.filter(
        (pl.col("CURCODE") == "MYR") & (pl.col("PRODCD") != 394)
    )
    ind_codes   = [77, 78, 95, 96]
    wdraw1 = base.filter(
        (pl.col("ACCTTYPE") == "C") & pl.col("CUSTCODE").is_in(ind_codes)
    )
    wdraw2 = base.filter(
        (pl.col("ACCTTYPE") == "I") & pl.col("CUSTCODE").is_in(ind_codes)
    )
    wdraw3 = base.filter(
        (pl.col("ACCTTYPE") == "C") & (~pl.col("CUSTCODE").is_in(ind_codes))
    )
    wdraw4 = base.filter(
        (pl.col("ACCTTYPE") == "I") & (~pl.col("CUSTCODE").is_in(ind_codes))
    )
    return wdraw1, wdraw2, wdraw3, wdraw4


# ============================================================================
# STEP 3b: %FCYGROUP – split WDRAW into 4 groups (CURCODE≠'MYR')
# ============================================================================
def fcygroup(wdraw: pl.DataFrame) -> tuple[pl.DataFrame, ...]:
    """Split WDRAW into four FCY sub-groups."""
    base = wdraw.filter(pl.col("CURCODE") != "MYR")
    ind_codes = [77, 78, 95, 96]
    wdraw1 = base.filter(
        (pl.col("ACCTTYPE") == "C") & pl.col("CUSTCODE").is_in(ind_codes)
    )
    wdraw2 = base.filter(
        (pl.col("ACCTTYPE") == "I") & pl.col("CUSTCODE").is_in(ind_codes)
    )
    wdraw3 = base.filter(
        (pl.col("ACCTTYPE") == "C") & (~pl.col("CUSTCODE").is_in(ind_codes))
    )
    wdraw4 = base.filter(
        (pl.col("ACCTTYPE") == "I") & (~pl.col("CUSTCODE").is_in(ind_codes))
    )
    return wdraw1, wdraw2, wdraw3, wdraw4


# ============================================================================
# STEP 4: %MAINPROC – accumulate counts/amounts by branch and reason code
#
# DATA FDRAW&J; SET WDRAW&J; BY BRANCH;
#   IF FIRST.BRANCH THEN ... initialise C1..C16, A1..A16, TOT_CNT, TOT_AMT;
#   IF RSONCODE = 'W' || PUT(&I,Z2.) THEN C&I+1; A&I+TRANAMT;
#   TOT_CNT+1; TOT_AMT+TRANAMT;
#   IF LAST.BRANCH THEN OUTPUT;
# ============================================================================
def mainproc(wdraw: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Group by BRANCH, accumulate C1..C16 (counts) and A1..A16 (amounts)
    per RSONCODE, plus TOT_CNT / TOT_AMT.
    Returns (fdraw, total) DataFrames.
    """
    if wdraw.is_empty():
        return pl.DataFrame(), pl.DataFrame()

    records = []
    for (branch,), grp in wdraw.group_by(["BRANCH"], maintain_order=False):
        # Ensure consistent columns exist
        first_row = grp.row(0, named=True)
        brabbr  = first_row.get("BRABBR", "")
        region  = first_row.get("REGION", "")
        type_   = first_row.get("TYPE", 1)

        counts  = {f"C{i}": 0 for i in range(1, 17)}
        amounts = {f"A{i}": 0.0 for i in range(1, 17)}
        tot_cnt = 0
        tot_amt = 0.0

        for row in grp.iter_rows(named=True):
            rsoncode = str(row.get("RSONCODE", "") or "").strip()
            tranamt  = float(row.get("TRANAMT", 0) or 0)
            for i in range(1, 17):
                if rsoncode == f"W{i:02d}":
                    counts[f"C{i}"]  += 1
                    amounts[f"A{i}"] += tranamt
                    break
            tot_cnt += 1
            tot_amt += tranamt

        rec = {
            "BRANCH"  : branch,
            "BRABBR"  : brabbr,
            "REGION"  : region,
            "TYPE"    : type_,
            "TOT_CNT" : tot_cnt,
            "TOT_AMT" : tot_amt,
        }
        rec.update(counts)
        rec.update(amounts)
        records.append(rec)

    fdraw = pl.DataFrame(records)

    # ------------------------------------------------------------------
    # PROC SUMMARY / TOTAL
    # OUTPUT OUT=TOTAL&J SUM=RC1..RC16 RA1..RA16 GTC GTA;
    # ------------------------------------------------------------------
    if fdraw.is_empty():
        return fdraw, pl.DataFrame()

    total_rec: dict = {}
    for i in range(1, 17):
        total_rec[f"RC{i}"] = float(fdraw[f"C{i}"].sum())
        total_rec[f"RA{i}"] = float(fdraw[f"A{i}"].sum())
    total_rec["GTC"] = float(fdraw["TOT_CNT"].sum())
    total_rec["GTA"] = float(fdraw["TOT_AMT"].sum())
    total_rec["TYPE"] = 1

    # DATA TOTAL&J – compute percentage compositions (PC/PA)
    gtc = total_rec["GTC"]
    gta = total_rec["GTA"]
    pgtc = 0.0
    pgta = 0.0
    for i in range(1, 17):
        total_rec[f"PC{i}"] = round(_safe_div(total_rec[f"RC{i}"], gtc), 0)
        total_rec[f"PA{i}"] = round(_safe_div(total_rec[f"RA{i}"], gta), 0)
        pgtc += total_rec[f"PC{i}"]
        pgta += total_rec[f"PA{i}"]
    total_rec["PGTC"] = round(pgtc, 0)
    total_rec["PGTA"] = round(pgta, 0)

    total = pl.DataFrame([total_rec])
    return fdraw, total


# ============================================================================
# STEP 5: %REPORT – write the semicolon-delimited report file
#
# DATA _NULL_; FILE &OUTPUT;
#   ... headers and data rows with ';' delimiters ...
# ============================================================================
def _fmt_comma16(val) -> str:
    """Replicate SAS COMMA16. format: integer with comma separators."""
    if val is None:
        return "0"
    try:
        return f"{int(round(float(val))):,}"
    except (TypeError, ValueError):
        return "0"


def _fmt_pct3(val) -> str:
    """Replicate SAS 3. format for percentages (integer, max 3 digits)."""
    if val is None:
        return "0"
    try:
        return str(int(round(float(val))))
    except (TypeError, ValueError):
        return "0"


def write_report(
    output_path: Path,
    fdraw_list : list[pl.DataFrame],
    total_list : list[pl.DataFrame],
    rtitle     : str,
    rdate      : str,
    hdr_labels : list[str],
    append     : bool = False,
) -> None:
    """
    Write the four-section report (J=1..4) to output_path.
    append=False writes from scratch; append=True appends (FILE &OUTPUT MOD).

    Report structure mirrors SAS %REPORT macro:
      - Header block for J=1 (individual section header)
      - Section label for J=3 (non-individual section header)
      - Per-J: column headers + data rows per BRANCH + TOTAL row + % row
    """
    mode = "a" if append else "w"
    with open(output_path, mode, encoding="utf-8") as fh:

        for j in range(1, 5):
            idx    = j - 1
            fdraw  = fdraw_list[idx]
            total  = total_list[idx]
            hdr_lbl = hdr_labels[idx]

            # ---- Section headers ----
            if j == 1:
                fh.write(f"REPORT ID : EIBDRB01\n")
                fh.write(
                    f"TITLE : DAILY SUMMARY REPORT ON REASONS FOR {rtitle} "
                    f"OVER-THE-COUNTER BASED ON RECEIPTS BY BRANCH\n"
                )
                fh.write(f"REPORTING DATE : {rdate}\n")
                fh.write(" \n")
                fh.write("(A) INDIVIDUAL CUSTOMER (CUSTOMER CODE: 77,78,95 AND 96)\n")

            if j == 3:
                fh.write(" \n")
                fh.write("(B) NON-INDIVIDUAL CUSTOMER\n")

            # ---- Column header block ----
            fh.write(" \n")
            fh.write(f"   {hdr_lbl}\n")
            fh.write(
                "   BRCH;BRCH;REGION;;;;;;;;;;;;;;;"
                "BY REASON CODE\n"
            )
            # Reason-code header row
            rc_hdr = ";".join(
                f"W{i:02d};W{i:02d}" for i in range(1, 17)
            )
            fh.write(f"   CODE;ABBR;;{rc_hdr};TOTAL;TOTAL\n")
            # NO./RM sub-header row
            nr_hdr = ";".join(
                "NO.;RM" for _ in range(16)
            )
            fh.write(f"   ;;;{nr_hdr};NO.;RM\n")

            # ---- Data rows ----
            if not fdraw.is_empty():
                for row in fdraw.sort("BRANCH").iter_rows(named=True):
                    parts = [
                        str(row.get("BRANCH", "")),
                        str(row.get("BRABBR", "")),
                        str(row.get("REGION", "")),
                    ]
                    for i in range(1, 17):
                        parts.append(str(int(row.get(f"C{i}", 0) or 0)))
                        parts.append(_fmt_comma16(row.get(f"A{i}", 0)))
                    parts.append(str(int(row.get("TOT_CNT", 0) or 0)))
                    parts.append(_fmt_comma16(row.get("TOT_AMT", 0)))
                    fh.write("   " + ";".join(parts) + "\n")

            # ---- TOTAL row ----
            if not total.is_empty():
                trow = total.row(0, named=True)
                t_parts = [
                    "",   # BRANCH blank
                    "TOTAL",
                    "",
                ]
                for i in range(1, 17):
                    t_parts.append(str(int(trow.get(f"RC{i}", 0) or 0)))
                    t_parts.append(_fmt_comma16(trow.get(f"RA{i}", 0)))
                t_parts.append(str(int(trow.get("GTC", 0) or 0)))
                t_parts.append(_fmt_comma16(trow.get("GTA", 0)))
                fh.write("   " + ";".join(t_parts) + "\n")

                # ---- % COMPOSITION row ----
                p_parts = [
                    "",
                    "% COMPOSITION",
                    "",
                ]
                for i in range(1, 17):
                    p_parts.append(_fmt_pct3(trow.get(f"PC{i}", 0)))
                    p_parts.append(_fmt_pct3(trow.get(f"PA{i}", 0)))
                p_parts.append(_fmt_pct3(trow.get("PGTC", 0)))
                p_parts.append(_fmt_pct3(trow.get("PGTA", 0)))
                fh.write("   " + ";".join(p_parts) + "\n")


# ============================================================================
# MAIN
# ============================================================================
def main():
    print("EIBDRB02 started.")

    # OPTIONS NOCENTER NONUMBER NODATE MISSING=0
    # (applied implicitly by the output formatters returning '0' for missing)

    # ---- Read REPTDATE ----
    macro = read_reptdate()
    reptdate : date = macro["reptdate"]
    reptmon  : str  = macro["REPTMON"]
    rptdate_num     = int(macro["RPTDATE"])
    rdate    : str  = macro["RDATE"]
    print(f"Reporting date: {reptdate}  RDATE={rdate}  REPTMON={reptmon}")

    # ---- Load withdrawal transactions ----
    print(f"Loading MIS.FDWDRW{reptmon}...")
    wdraw = load_wdraw(reptmon, rptdate_num)
    print(f"WDRAW: {len(wdraw):,} rows loaded")

    # ---- HDR labels (matches %LET HDR1..HDR4 in SAS) ----
    # %LET HDR1=(I) PBB;  HDR2=(II) PIBB;  HDR3=(I) PBB;  HDR4=(II) PIBB
    hdr_labels = ["(I) PBB", "(II) PIBB", "(I) PBB", "(II) PIBB"]

    # ==========================================================================
    # GENERATE REPORT FOR RM FIXED DEPOSIT
    # DATA _NULL_; CALL SYMPUT('RTITLE', ...); CALL SYMPUT('OUTPUT', ...);
    # ==========================================================================
    rtitle_rm = "FD WITHDRAWALS (PBB&PIBB)"
    output_rm = OUTPUT_DIR / "RMWDRAW"
    print("Processing RM group...")
    wdraw1_rm, wdraw2_rm, wdraw3_rm, wdraw4_rm = rmgroup(wdraw)
    fdraw_rm_list = []
    total_rm_list = []
    for wg in (wdraw1_rm, wdraw2_rm, wdraw3_rm, wdraw4_rm):
        fd, tot = mainproc(wg)
        fdraw_rm_list.append(fd)
        total_rm_list.append(tot)
    print(f"Writing RM report to {output_rm}...")
    write_report(
        output_path = output_rm,
        fdraw_list  = fdraw_rm_list,
        total_list  = total_rm_list,
        rtitle      = rtitle_rm,
        rdate       = rdate,
        hdr_labels  = hdr_labels,
        append      = False,
    )
    print("RM report written.")

    # ==========================================================================
    # GENERATE REPORT FOR FCY FIXED DEPOSIT
    # DATA _NULL_; CALL SYMPUT('RTITLE', ...); CALL SYMPUT('OUTPUT', ...);
    # ==========================================================================
    rtitle_fcy = "FCY FD WITHDRAWALS"
    output_fcy = OUTPUT_DIR / "FCYWDRAW"
    print("Processing FCY group...")
    wdraw1_fcy, wdraw2_fcy, wdraw3_fcy, wdraw4_fcy = fcygroup(wdraw)
    fdraw_fcy_list = []
    total_fcy_list = []
    for wg in (wdraw1_fcy, wdraw2_fcy, wdraw3_fcy, wdraw4_fcy):
        fd, tot = mainproc(wg)
        fdraw_fcy_list.append(fd)
        total_fcy_list.append(tot)
    print(f"Writing FCY report to {output_fcy}...")
    write_report(
        output_path = output_fcy,
        fdraw_list  = fdraw_fcy_list,
        total_list  = total_fcy_list,
        rtitle      = rtitle_fcy,
        rdate       = rdate,
        hdr_labels  = hdr_labels,
        append      = False,
    )
    print("FCY report written.")

    print("EIBDRB02 completed successfully.")


if __name__ == "__main__":
    main()
