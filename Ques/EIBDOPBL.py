#!/usr/bin/env python3
"""
Program : EIBDOPBL.py
Purpose : Breakdown Individual & Non-Individual by Deposit
          Reads FISS PBB and PIBB semicolon-delimited text files,
          classifies by BIC code, summarises CURBAL by TYPE,
          and writes two output files:
            OUTFILE1 - formatted report (fixed-width)
            OUTFILE2 - delimited/CSV-style data
"""

from pathlib import Path
from datetime import date
import polars as pl

from REPTDATE import get_reptdate_values

# ============================================================
# PATHS
# ============================================================
# # Production Path
# BASE_DIR    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
# INPUT_DIR   = BASE_DIR / "input" / "prod" / "EIBDRMFC"
# OUTPUT_DIR  = BASE_DIR / "output" / "EIBDOPBL"

# FISSD_FILE  = INPUT_DIR / "FISSD.txt"
# FISSID_FILE = INPUT_DIR / "FISSID.txt"

# Testing Path
BASE_DIR    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
INPUT_DIR   = BASE_DIR / "input" / "prod" / "EIBDRMFC"
OUTPUT_DIR  = BASE_DIR / "output" / "EIBDOPBL"

FISSD_FILE  = INPUT_DIR / "FISSD.txt"
FISSID_FILE = INPUT_DIR / "FISSID.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# REPORT DATE  (replaces DATA REPTDATE / CALL SYMPUT)
# ============================================================
_rd     = get_reptdate_values()
REPDD   = _rd.reptday                          # Z2.  day
REPMM   = _rd.reptmon                          # Z2.  month
REPYY   = _rd.reptdate.strftime("%Y")          # YEAR4.
REPDT   = _rd.reptdate.strftime("%d/%m/%Y")    # DDMMYY8.  (e.g. 27/06/2026)

# ============================================================
# OUTPUT FILENAMES
# ============================================================
OUTFILE1 = OUTPUT_DIR / f"PBB_FISS_DAILY_REP_{REPDD}{REPMM}{REPYY}.txt"
OUTFILE2 = OUTPUT_DIR / f"PBB_FISS_DAILY_REP_DELI_{REPDD}{REPMM}{REPYY}.txt"

# ============================================================
# HELPER: read semicolon-delimited FISS text file
#         INFILE ... DELIMITER=';' FIRSTOBS=2
#         INFORMAT BIC $15.;  INFORMAT CURBAL 30.;
# ============================================================
def read_fiss(filepath: Path) -> pl.DataFrame:
    import csv, io

    raw = filepath.read_text(encoding="latin1")
    reader = csv.reader(io.StringIO(raw), delimiter=";")
    rows = list(reader)
    data_rows = rows[1:]          # FIRSTOBS=2  → skip header

    records = []
    for row in data_rows:
        if len(row) < 2:
            continue
        bic    = row[0].strip()[:15]   # $15.
        try:
            curbal = float(row[1].strip())   # 30. numeric
        except ValueError:
            curbal = None
        records.append({"BIC": bic, "CURBAL": curbal})

    return pl.DataFrame(records, schema={"BIC": pl.Utf8, "CURBAL": pl.Float64})


# ============================================================
# DATA FISS_PBB  — classify by BICODE, split CURBAL into I/C
# ============================================================
def classify_pbb(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns([
        pl.col("BIC").str.slice(0, 5).alias("BICODE"),
        pl.col("BIC").str.slice(5, 2).alias("TYPEX"),
    ])

    # TYPE assignment (IF BICODE = '...' THEN TYPE=...)
    df = df.with_columns(
        pl.when(pl.col("BICODE") == "95313").then(pl.lit("CA"))
        .when(pl.col("BICODE") == "95312").then(pl.lit("SA"))
        .when(pl.col("BICODE") == "95311").then(pl.lit("FD"))
        .when(pl.col("BICODE") == "96311").then(pl.lit("FCYFD"))
        .when(pl.col("BICODE") == "96313").then(pl.lit("FCYCA"))
        .otherwise(pl.lit(None))
        .alias("TYPE")
    )

    # CURBAL_I / CURBAL_C split on TYPEX
    df = df.with_columns([
        pl.when(pl.col("TYPEX") == "08").then(pl.col("CURBAL")).otherwise(None).alias("CURBAL_I"),
        pl.when(pl.col("TYPEX") == "09").then(pl.col("CURBAL")).otherwise(None).alias("CURBAL_C"),
    ])

    # IF TYPE=' ' THEN DELETE
    df = df.filter(pl.col("TYPE").is_not_null())

    return df


# ============================================================
# DATA FISS_PIBB — classify by BICODE, split into ICURBAL_I/C
# ============================================================
def classify_pibb(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns([
        pl.col("BIC").str.slice(0, 5).alias("BICODE"),
        pl.col("BIC").str.slice(5, 2).alias("TYPEX"),
    ])

    # TYPE assignment (note: FD code differs — 95315 for PIBB vs 95311 for PBB)
    df = df.with_columns(
        pl.when(pl.col("BICODE") == "95313").then(pl.lit("CA"))
        .when(pl.col("BICODE") == "95312").then(pl.lit("SA"))
        .when(pl.col("BICODE") == "95315").then(pl.lit("FD"))
        .when(pl.col("BICODE") == "96311").then(pl.lit("FCYFD"))
        .when(pl.col("BICODE") == "96313").then(pl.lit("FCYCA"))
        .otherwise(pl.lit(None))
        .alias("TYPE")
    )

    # ICURBAL_I / ICURBAL_C split on TYPEX
    df = df.with_columns([
        pl.when(pl.col("TYPEX") == "08").then(pl.col("CURBAL")).otherwise(None).alias("ICURBAL_I"),
        pl.when(pl.col("TYPEX") == "09").then(pl.col("CURBAL")).otherwise(None).alias("ICURBAL_C"),
    ])

    # IF TYPE=' ' THEN DELETE
    df = df.filter(pl.col("TYPE").is_not_null())

    return df


# ============================================================
# DATA FISS — combine PBB + PIBB, filter NON <> '00'
# ============================================================
def build_fiss(pbb: pl.DataFrame, pibb: pl.DataFrame) -> pl.DataFrame:
    # SET FISS_PBB FISS_PIBB  (vertical concatenation)
    # Align columns so concat works cleanly
    pbb_cols  = {"BIC", "CURBAL", "BICODE", "TYPEX", "TYPE", "CURBAL_I",  "CURBAL_C"}
    pibb_cols = {"BIC", "CURBAL", "BICODE", "TYPEX", "TYPE", "ICURBAL_I", "ICURBAL_C"}

    for c in ["ICURBAL_I", "ICURBAL_C"]:
        if c not in pbb.columns:
            pbb = pbb.with_columns(pl.lit(None).cast(pl.Float64).alias(c))
    for c in ["CURBAL_I", "CURBAL_C"]:
        if c not in pibb.columns:
            pibb = pibb.with_columns(pl.lit(None).cast(pl.Float64).alias(c))

    all_cols = ["BIC", "CURBAL", "BICODE", "TYPEX", "TYPE",
                "CURBAL_I", "CURBAL_C", "ICURBAL_I", "ICURBAL_C"]

    fiss = pl.concat([pbb.select(all_cols), pibb.select(all_cols)])

    # NON = SUBSTR(BIC,8,2);  IF NON NE '00' THEN DELETE;
    fiss = fiss.with_columns(
        pl.col("BIC").str.slice(7, 2).alias("NON")   # 0-based: positions 8-9
    )
    fiss = fiss.filter(pl.col("NON") == "00")

    return fiss


# ============================================================
# PROC SUMMARY — SUM CURBAL_I CURBAL_C ICURBAL_I ICURBAL_C by TYPE
# ============================================================
def summarise_fiss(fiss: pl.DataFrame) -> pl.DataFrame:
    summary = fiss.group_by("TYPE").agg([
        pl.col("CURBAL_I").sum().alias("CURBAL_I"),
        pl.col("CURBAL_C").sum().alias("CURBAL_C"),
        pl.col("ICURBAL_I").sum().alias("ICURBAL_I"),
        pl.col("ICURBAL_C").sum().alias("ICURBAL_C"),
    ])

    # Fill nulls → 0,  compute TOTAL
    summary = summary.with_columns([
        pl.col("CURBAL_I").fill_null(0),
        pl.col("CURBAL_C").fill_null(0),
        pl.col("ICURBAL_I").fill_null(0),
        pl.col("ICURBAL_C").fill_null(0),
    ]).with_columns(
        (pl.col("CURBAL_I") + pl.col("CURBAL_C") +
         pl.col("ICURBAL_I") + pl.col("ICURBAL_C")).alias("TOTAL")
    )

    return summary


# ============================================================
# DATA _NULL_ — write OUTFILE1 (formatted report, RECFM=FB LRECL=138)
# ============================================================
def write_report(summary: pl.DataFrame, outfile: Path) -> None:
    sep1 = "-" * 9
    sep2 = "-" * 35
    sep3 = "-" * 17

    lines = []

    # Header block (_N_ = 1)
    lines.append(
        f"{'BREAKDOWN INDIVIDUAL & NON INDIVIDUAL BY DEPOSIT @  ':<52}{REPDT}"
    )
    lines.append(" " * 40)
    lines.append(f"{sep1}{sep2}{sep2}{sep3}{'-' * 3}")
    lines.append(" " * 47 + "CURBAL")
    lines.append(f"{sep1}{sep2}{sep2}{sep3}{'-' * 3}")
    lines.append(" " * 26 + "PBB" + " " * 33 + "PIBB")
    lines.append(f"{sep1}{' ' * 1}{sep2}{' ' * 1}{sep2}{' ' * 1}{sep3}")
    lines.append(
        f"{' ' * 2}{'TYPE':<7}"
        f"{'INDIVIDUAL':>17}"
        f"{'NON-INDIVIDUAL':>19}"
        f"{'INDIVIDUAL':>17}"
        f"{'NON-INDIVIDUAL':>19}"
        f"{'TOTAL':>18}"
    )
    lines.append(f"{sep1}{' ' * 1}{sep2}{' ' * 1}{sep2}{' ' * 1}{sep3}")

    # Data rows
    for row in summary.sort("TYPE").iter_rows(named=True):
        type_val   = (row["TYPE"] or "")[:6].ljust(6)
        curbal_i   = int(row["CURBAL_I"]  or 0)
        curbal_c   = int(row["CURBAL_C"]  or 0)
        icurbal_i  = int(row["ICURBAL_I"] or 0)
        icurbal_c  = int(row["ICURBAL_C"] or 0)
        total      = int(row["TOTAL"]     or 0)

        # PUT @3 TYPE $6. @15 CURBAL_I 10. @30 CURBAL_C 10.
        #     @48 ICURBAL_I 10. @62 ICURBAL_C 10. @80 TOTAL 10.
        line = (
            f"  {type_val:<7}"                          # @3  (1-based → index 2)
            # + " " * (15 - 2 - 6 - 2)               # pad to @15
            + f" {curbal_i:>16}"                      # @15 CURBAL_I  10.
            # + " " * (30 - 15 - 10)                  # pad to @30
            + f" {curbal_c:>18}"                      # @30 CURBAL_C  10.
            # + " " * (48 - 30 - 10)                  # pad to @48
            + f" {icurbal_i:>16}"                     # @48 ICURBAL_I 10.
            # + " " * (62 - 48 - 10)                  # pad to @62
            + f" {icurbal_c:>18}"                     # @62 ICURBAL_C 10.
            # + " " * (80 - 62 - 10)                  # pad to @80
            + f" {total:>17}"                         # @80 TOTAL     10.
        )
        lines.append(line)

    # Pad / truncate each line to LRECL=138
    padded = [ln.ljust(138)[:138] for ln in lines]

    outfile.write_text("\n".join(padded) + "\n", encoding="latin1")
    print(f"\n[OUTFILE1] Written: {outfile}")
    print("\n".join(padded))


# ============================================================
# DATA _NULL_ — write OUTFILE2 (delimited / CSV-style, RECFM=FB LRECL=138)
# ============================================================
def write_delimited(summary: pl.DataFrame, outfile: Path) -> None:
    lines = []

    for row in summary.sort("TYPE").iter_rows(named=True):
        type_val  = (row["TYPE"] or "")
        curbal_i  = int(row["CURBAL_I"]  or 0)
        curbal_c  = int(row["CURBAL_C"]  or 0)
        icurbal_i = int(row["ICURBAL_I"] or 0)
        icurbal_c = int(row["ICURBAL_C"] or 0)
        total     = int(row["TOTAL"]     or 0)

        # PUT @1 TYPE ',' @15 CURBAL_I ',' @30 CURBAL_C ','
        #     @48 ICURBAL_I ',' @62 ICURBAL_C ',' @80 TOTAL ',';
        line = (
            f"{type_val},"
            + " " * max(0, 14 - len(type_val))
            + f"{curbal_i},"
            + " " * max(0, 14 - len(str(curbal_i)))
            + f"{curbal_c},"
            + " " * max(0, 17 - len(str(curbal_c)))
            + f"{icurbal_i},"
            + " " * max(0, 13 - len(str(icurbal_i)))
            + f"{icurbal_c},"
            + " " * max(0, 17 - len(str(icurbal_c)))
            + f"{total},"
        )
        lines.append(line.ljust(138)[:138])

    outfile.write_text("\n".join(lines) + "\n", encoding="latin1")
    print(f"\n[OUTFILE2] Written: {outfile}")
    print("\n".join(lines))


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    # Read raw FISS files
    fiss_pbb_raw  = read_fiss(FISSD_FILE)
    fiss_pibb_raw = read_fiss(FISSID_FILE)

    # Classify
    fiss_pbb  = classify_pbb(fiss_pbb_raw)
    fiss_pibb = classify_pibb(fiss_pibb_raw)

    # PROC PRINT DATA=FISS (before summary) — terminal preview
    fiss = build_fiss(fiss_pbb, fiss_pibb)
    print("[PROC PRINT] FISS (before summary):")
    print(fiss)

    # Summarise
    summary = summarise_fiss(fiss)

    # PROC PRINT DATA=FISS (after summary)
    print("[PROC PRINT] FISS (after summary):")
    print(summary)

    # Write outputs
    write_report(summary, OUTFILE1)
    write_delimited(summary, OUTFILE2)

    print(f"\n[DONE] Report date : {REPDT}")
    print(f"       OUTFILE1    : {OUTFILE1}")
    print(f"       OUTFILE2    : {OUTFILE2}")


if __name__ == "__main__":
    main()
