#!/usr/bin/env python3
"""
Program : EIBDRB02.py
Purpose : Daily Summary Report on Reasons for FD Withdrawals
          Over-The-Counter Based on Receipts by Branch.
          Produces two reports:
            (1) RM Fixed Deposit withdrawals  -> RMWDRAW  (DSN: SAP.PBB.EIBDRB2A)
            (2) FCY Fixed Deposit withdrawals -> FCYWDRAW (DSN: SAP.PBB.EIBDRB2B)
          Each report is split into:
            (A) Individual customer   (CUSTCODE IN 77,78,95,96)
            (B) Non-individual customer
          and, within each, by ACCTTYPE 'C' (PBB) / 'I' (PIBB).
"""

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from REPTDATE import get_reptdate_values
from PBBELF import format_brchcd, format_regnew

# %INC PGM(PBBELF); in the SAS source only drives PUT(BRANCH,BRCHCD.) and
# PUT(BRANCH,REGNEW.) — those are the only two PBBELF formats invoked in this
# program's body, so only format_brchcd/format_regnew are imported. Other
# PBBELF tables (CACBRCH, CTYPE, branch lists, etc.) are not referenced here.

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

# Input directories - Testing
INPUT_DEPOSIT_DIR = BASE_DIR / "input" / "prod" / "EIBDRB02"   # MIS.FDWDRW&REPTMON
CACHE_DIR         = BASE_DIR / "input" / "prod" / "EIBDRB02"

# Output
OUTPUT_DIR = BASE_DIR / "output" / "EIBDRB02"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# STEP 1: REPORT DATE  (no reptdate.parquet — derive from REPTDATE.py)
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values()
reptdate = reptdate_values.rdate

REPTYEAR = reptdate.strftime("%Y")          # PUT(REPTDATE,YEAR4.)
REPTMON  = reptdate.strftime("%m")          # PUT(MONTH(REPTDATE),Z2.)
RDATE    = reptdate.strftime("%d/%m/%y")    # PUT(REPTDATE,DDMMYY8.)
RPTDATE  = reptdate                         # WHERE REPTDATE EQ &RPTDATE

print(f"  Report year  : {REPTYEAR}")
print(f"  Report month : {REPTMON}")
print(f"  Report date  : {RDATE}")

# ============================================================================
# STEP 2: RESOLVE INPUT FILE NAME
# ============================================================================
# MIS.FDWDRW&REPTMON encodes only the month in its filename (e.g. FDWDRW07),
# not a day/year, so there is no "latest file" candidate set to pick from —
# input_date.get_latest_file() does not apply. The filename is built directly
# from REPTMON, the same convention already used for EIMISR01.
print("\nStep 2: Resolving FDWDRW input file...")

fdwdrw_path = INPUT_DEPOSIT_DIR / f"fdwdrw{REPTMON}.sas7bdat"
print(f"  FDWDRW : {fdwdrw_path.name}")

# ============================================================================
# STEP 3: CACHE SAS FILE TO PARQUET  (skip re-conversion if unchanged)
# ============================================================================
def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    """Return True when the Parquet cache is newer than the source SAS file."""
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


def sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    """Convert a .sas7bdat file to Parquet."""
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    df = pd.read_sas(sas_path, encoding="latin1")
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, cache_path, compression="snappy")
    print(f"  [{tag}] Done — {len(df):,} rows cached.")


print("\nStep 3: Caching FDWDRW to Parquet (if needed)...")

FDWDRW_CACHE = CACHE_DIR / f"fdwdrw{REPTMON}.parquet"

if not _cache_is_fresh(fdwdrw_path, FDWDRW_CACHE):
    sas_to_parquet(fdwdrw_path, FDWDRW_CACHE, "FDWDRW")
else:
    print("  [FDWDRW] Cache fresh — skipping conversion.")

# ============================================================================
# STEP 4: BUILD WDRAW
# DATA WDRAW; SET MIS.FDWDRW&REPTMON; WHERE REPTDATE EQ &RPTDATE;
#   TYPE=1; BRABBR=PUT(BRANCH,BRCHCD.); REGION=PUT(BRANCH,REGNEW.);
# ============================================================================
print("\nStep 4: Building WDRAW (reporting-date filter)...")

con = duckdb.connect(database=":memory:")
wdraw = con.execute(f"""
    SELECT
        CAST(BRANCH             AS INTEGER) AS BRANCH,
        CAST(TRIM(CURCODE)      AS VARCHAR) AS CURCODE,
        CAST(PRODCD             AS INTEGER) AS PRODCD,
        CAST(TRIM(ACCTTYPE)     AS VARCHAR) AS ACCTTYPE,
        CAST(CUSTCODE           AS INTEGER) AS CUSTCODE,
        CAST(TRIM(RSONCODE)     AS VARCHAR) AS RSONCODE,
        CAST(TRANAMT            AS DOUBLE)  AS TRANAMT
    FROM read_parquet('{FDWDRW_CACHE}')
    WHERE CAST(REPTDATE AS DATE) = DATE '{RPTDATE.isoformat()}'
""").pl()
con.close()

wdraw = wdraw.with_columns([
    pl.lit(1).alias("TYPE"),
    pl.col("BRANCH").map_elements(format_brchcd, return_dtype=pl.Utf8).alias("BRABBR"),
    pl.col("BRANCH").map_elements(format_regnew, return_dtype=pl.Utf8).alias("REGION"),
])

print(f"  WDRAW rows: {len(wdraw):,}")

# ============================================================================
# FORMAT HELPERS  (approximate SAS default/explicit numeric PUT widths)
# ============================================================================
def _best12(value) -> str:
    """Unformatted numeric PUT default (~BEST12.)."""
    if value is None:
        value = 0
    if float(value).is_integer():
        text = str(int(value))
    else:
        text = f"{value:.6f}".rstrip("0").rstrip(".")
    return text.rjust(12)


def _comma16(value) -> str:
    """COMMA16. — comma-separated, 0 decimals, width 16."""
    if value is None:
        value = 0
    return f"{value:,.0f}".rjust(16)


def _fmt3(value) -> str:
    """FORMAT ... 3. — rounded integer, width 3."""
    if value is None:
        value = 0
    return f"{round(value):d}".rjust(3)


# ============================================================================
# STEP 5: FDRAW&J  (per-branch reason-code pivot, replaces PROC SORT +
# accumulation-by-BRANCH DATA step with a direct group-by aggregation)
# RSONCODE = 'W01'..'W16' -> C1..C16 (count), A1..A16 (sum TRANAMT)
# TOT_CNT / TOT_AMT accumulate regardless of RSONCODE match.
# RSONCODE and TYPE are retained in the SAS KEEP list but never referenced
# by the report output, so they are not carried into the aggregation.
# ============================================================================
REASON_CODES = [f"W{i:02d}" for i in range(1, 17)]


def build_fdraw(df: pl.DataFrame) -> pl.DataFrame:
    """Aggregate reason-code counts/amounts per BRANCH."""
    if df.is_empty():
        return df

    agg_exprs = [
        pl.col("BRABBR").first().alias("BRABBR"),
        pl.col("REGION").first().alias("REGION"),
    ]
    for i, code in enumerate(REASON_CODES, start=1):
        agg_exprs.append(
            pl.when(pl.col("RSONCODE") == code).then(1).otherwise(0)
              .sum().alias(f"C{i}")
        )
        agg_exprs.append(
            pl.when(pl.col("RSONCODE") == code).then(pl.col("TRANAMT")).otherwise(0.0)
              .sum().alias(f"A{i}")
        )
    agg_exprs.append(pl.len().alias("TOT_CNT"))
    agg_exprs.append(pl.col("TRANAMT").sum().alias("TOT_AMT"))

    return df.group_by("BRANCH").agg(agg_exprs).sort("BRANCH")


# ============================================================================
# STEP 6: TOTAL&J  (PROC SUMMARY CLASS TYPE — TYPE is constant=1, so this is
# simply the grand total across all branches) + PC/PA/PGTC/PGTA percentages
# ============================================================================
def build_total(fdraw: pl.DataFrame) -> dict:
    """Grand totals (RC/RA/GTC/GTA) and % composition (PC/PA/PGTC/PGTA)."""
    totals: dict = {}
    for i in range(1, 17):
        totals[f"RC{i}"] = fdraw[f"C{i}"].sum()
        totals[f"RA{i}"] = fdraw[f"A{i}"].sum()
    totals["GTC"] = fdraw["TOT_CNT"].sum()
    totals["GTA"] = fdraw["TOT_AMT"].sum()

    gtc = totals["GTC"] or 0
    gta = totals["GTA"] or 0
    pgtc = 0.0
    pgta = 0.0
    for i in range(1, 17):
        pc = (totals[f"RC{i}"] / gtc * 100) if gtc else 0.0
        pa = (totals[f"RA{i}"] / gta * 100) if gta else 0.0
        totals[f"PC{i}"] = pc
        totals[f"PA{i}"] = pa
        pgtc += pc
        pgta += pa
    totals["PGTC"] = pgtc
    totals["PGTA"] = pgta
    return totals


# ============================================================================
# STEP 7: REPORT WRITER
# RECFM=FB (not FBA) — plain semicolon-delimited text, NO ASA carriage
# control. '@1' items start at column 1; '@4' items start at column 4
# (3 leading blanks).
# ============================================================================
HDR_MAP = {1: "(I) PBB", 2: "(II) PIBB", 3: "(I) PBB", 4: "(II) PIBB"}
CUST_INDIV = [77, 78, 95, 96]

# Literal header row copied verbatim from the SAS PUT statement
# ('REGION' followed directly by 15 semicolons then 'BY REASON CODE').
HEADER_ROW2 = "BRCH;BRCH;REGION;;;;;;;;;;;;;;;BY REASON CODE"


def write_group_report(wdraw_group: pl.DataFrame, rtitle: str, output_path: Path) -> None:
    """Build FDRAW/TOTAL for J=1..4 and write the semicolon-delimited report."""

    def subset(accttype: str, cust_in_list: bool) -> pl.DataFrame:
        cond = pl.col("ACCTTYPE") == accttype
        if cust_in_list:
            cond = cond & pl.col("CUSTCODE").is_in(CUST_INDIV)
        else:
            cond = cond & (~pl.col("CUSTCODE").is_in(CUST_INDIV))
        return wdraw_group.filter(cond)

    wdraw_by_j = {
        1: subset("C", True),   # WDRAW1: PBB  individual
        2: subset("I", True),   # WDRAW2: PIBB individual
        3: subset("C", False),  # WDRAW3: PBB  non-individual
        4: subset("I", False),  # WDRAW4: PIBB non-individual
    }

    lines: list[str] = []

    # ---- initial header block (written once, FILE ... without MOD) ----
    lines.append("REPORT ID : EIBDRB01")
    lines.append(
        f"TITLE : DAILY SUMMARY REPORT ON REASONS FOR {rtitle} "
        "OVER-THE-COUNTER BASED ON RECEIPTS BY BRANCH"
    )
    lines.append(f"REPORTING DATE : {RDATE}")
    lines.append(" ")
    lines.append("(A) INDIVIDUAL CUSTOMER (CUSTOMER CODE: 77,78,95 AND 96)")

    for j in range(1, 5):
        if j == 3:
            # This marker is written unconditionally, independent of data.
            lines.append(" ")
            lines.append("(B) NON-INDIVIDUAL CUSTOMER")

        fdraw = build_fdraw(wdraw_by_j[j])
        if fdraw.is_empty():
            # DATA _NULL_ SET FDRAW&J / TOTAL&J with 0 obs never executes,
            # so no header, detail, or total lines are produced for this J.
            continue

        totals = build_total(fdraw)

        # ---- header rows (IF _N_=1 block) ----
        lines.append(" ")
        lines.append("   " + HDR_MAP[j])
        lines.append("   " + HEADER_ROW2)

        row3 = ["CODE", "ABBR", ""]
        for code in REASON_CODES:
            row3 += [code, code]
        row3 += ["TOTAL", "TOTAL"]
        lines.append("   " + ";".join(row3))

        row4 = ["", "", ""]
        for _ in REASON_CODES:
            row4 += ["NO.", "RM"]
        lines.append("   " + ";".join(row4))

        # ---- branch detail rows ----
        for row in fdraw.iter_rows(named=True):
            fields = [
                _best12(row["BRANCH"]),
                str(row["BRABBR"] or ""),
                str(row["REGION"] or ""),
            ]
            for i in range(1, 17):
                fields.append(_best12(row[f"C{i}"]))
                fields.append(_comma16(row[f"A{i}"]))
            fields.append(_best12(row["TOT_CNT"]))
            fields.append(_comma16(row["TOT_AMT"]))
            lines.append("   " + ";".join(fields))

        # ---- grand total row ----
        total_fields = ["", "TOTAL", ""]
        for i in range(1, 17):
            total_fields.append(_best12(totals[f"RC{i}"]))
            total_fields.append(_comma16(totals[f"RA{i}"]))
        total_fields.append(_best12(totals["GTC"]))
        total_fields.append(_comma16(totals["GTA"]))
        lines.append("   " + ";".join(total_fields))

        # ---- % composition row ----
        pct_fields = ["", "% COMPOSITION", ""]
        for i in range(1, 17):
            pct_fields.append(_fmt3(totals[f"PC{i}"]))
            pct_fields.append(_fmt3(totals[f"PA{i}"]))
        pct_fields.append(_fmt3(totals["PGTC"]))
        pct_fields.append(_fmt3(totals["PGTA"]))
        lines.append("   " + ";".join(pct_fields))

    with open(output_path, "w", encoding="latin1") as fh:
        for ln in lines:
            fh.write(ln + "\n")

    print(f"  Output written : {output_path}")
    print(f"  Total lines    : {len(lines):,}")
    print("  --- Preview ---")
    for preview_line in lines[:10]:
        print(f"  {preview_line}")


# ============================================================================
# STEP 8: RM FIXED DEPOSIT REPORT  (RMGROUP: CURCODE='MYR' & PRODCD<>394)
# ============================================================================
print("\nStep 8: Generating RM fixed deposit withdrawal report...")

# CALL SYMPUT('RTITLE',PUT('FD WITHDRAWALS (PBB&PIBB)',$25.));
RTITLE_RM = "FD WITHDRAWALS (PBB&PIBB)"

# NOTE: "IF PRODCD NE 394;" inside "IF CURCODE EQ 'MYR' THEN DO;" is a bare
# subsetting IF — a non-MYR match on 394 never occurs (block only entered
# when MYR), so it simply excludes PRODCD=394 rows from the MYR subset.
wdraw_rm = wdraw.filter(
    (pl.col("CURCODE") == "MYR") & (pl.col("PRODCD") != 394)
)

write_group_report(wdraw_rm, RTITLE_RM, OUTPUT_DIR / "EIBDRB2A.txt")  # DD: RMWDRAW

# ============================================================================
# STEP 9: FCY FIXED DEPOSIT REPORT  (FCYGROUP: CURCODE<>'MYR')
# ============================================================================
print("\nStep 9: Generating FCY fixed deposit withdrawal report...")

# CALL SYMPUT('RTITLE',PUT('FCY FD WITHDRAWALS',$18.));
RTITLE_FCY = "FCY FD WITHDRAWALS"

wdraw_fcy = wdraw.filter(pl.col("CURCODE") != "MYR")

write_group_report(wdraw_fcy, RTITLE_FCY, OUTPUT_DIR / "EIBDRB2B.txt")  # DD: FCYWDRAW

print("\nEIBDRB02 complete.")
