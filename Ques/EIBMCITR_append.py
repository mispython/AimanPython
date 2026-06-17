#!/usr/bin/env python3
"""
Program : EIBMCITR.py
Purpose : Monthly Accumulated Report for Cash-In-Transit
          Reads the 4 latest TLBTRAN .sas7bdat files (representing 4 weeks of
          the report month) and a DBRANCH.txt fixed-width branch reference file.
          Filters TRANCODE IN (2222, 2223), aggregates CASHOUT and account counts
          per REPTDATE/BRANCH for the current month, then accumulates into a
          persistent yearly CIT parquet (CIT_{year}.parquet).
          On each run, only the current month's columns are updated; all other
          months already stored in the parquet are preserved untouched.
          Finally writes a formatted Cash-In-Transit report (CITLIST .txt).
"""

import duckdb
import polars as pl
import pandas as pd
import re
from pathlib import Path

from REPTDATE import get_reptdate_values
from output_date import build_output_file

# ============================================================================
# REPORT DATE (from REPTDATE module)
# ============================================================================
reptdate_values  = get_reptdate_values()
REPTDATE         = reptdate_values.reptdate
REPTYEAR         = reptdate_values.reptyear       # 2-digit year  (PUT(REPTDATE,YEAR2.))
REPTMON          = reptdate_values.reptmon        # zero-padded month e.g. "02"

_rv4             = get_reptdate_values(year_format="%Y")
REPTYEAR2        = _rv4.reptyear                  # 4-digit year  (PUT(REPTDATE,YEAR4.))

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
INPUT_DIR  = BASE_DIR / "input"  / "prod" / "EIBMCITR"
CIT_DIR    = BASE_DIR / "input"  / "prod" / "EIBMCITR"
OUTPUT_DIR = BASE_DIR / "output" / "EIBMCITR"

# # Production paths (uncomment for prod)
# INPUT_DIR  = Path("/dwh")
# CIT_DIR    = Path("/sas/cit")
# OUTPUT_DIR = Path("/host/mis/output/report")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CIT_DIR.mkdir(parents=True, exist_ok=True)

# Branch reference fixed-width flat file
INPUT_BRHFILE = INPUT_DIR / "DBRANCH.TXT"
# INPUT_BRHFILE = Path("/stgsrcsys/host/ftpfiles") / "DBRANCH.txt"

# 4 weekly TLBTRAN input files for the current report month
# INPUT_TLBTRAN_WK1 = INPUT_DIR / f"tlbtran{REPTMON}1{REPTYEAR}.sas7bdat"
# INPUT_TLBTRAN_WK2 = INPUT_DIR / f"tlbtran{REPTMON}2{REPTYEAR}.sas7bdat"
# INPUT_TLBTRAN_WK3 = INPUT_DIR / f"tlbtran{REPTMON}3{REPTYEAR}.sas7bdat"
# INPUT_TLBTRAN_WK4 = INPUT_DIR / f"tlbtran{REPTMON}4{REPTYEAR}.sas7bdat"
INPUT_TLBTRAN_WK1 = INPUT_DIR / "tlbtran04126.sas7bdat"
INPUT_TLBTRAN_WK2 = INPUT_DIR / "tlbtran04226.sas7bdat"
INPUT_TLBTRAN_WK3 = INPUT_DIR / "tlbtran04326.sas7bdat"
INPUT_TLBTRAN_WK4 = INPUT_DIR / "tlbtran04426.sas7bdat"

# Persistent yearly CIT accumulation parquet — one file per year
CIT_YEAR_FILE = CIT_DIR / f"CIT_{REPTYEAR2}.parquet"

# Output report
OUTPUT_CITLIST = build_output_file(OUTPUT_DIR, "EIBMCITR_CITLIST").with_suffix(".txt")

# ============================================================================
# INPUT FILE EXISTENCE CHECK — fail fast before any processing
# ============================================================================
_REQUIRED_INPUTS = {
    "TLBTRAN Week 1": INPUT_TLBTRAN_WK1,
    "TLBTRAN Week 2": INPUT_TLBTRAN_WK2,
    "TLBTRAN Week 3": INPUT_TLBTRAN_WK3,
    "TLBTRAN Week 4": INPUT_TLBTRAN_WK4,
    "Branch File"   : INPUT_BRHFILE,
}
_missing = [
    f"  [{label}] {path}"
    for label, path in _REQUIRED_INPUTS.items()
    if not path.exists()
]
if _missing:
    raise FileNotFoundError(
        "The following required input files are missing:\n" + "\n".join(_missing)
    )

# ============================================================================
# SCHEMA HELPERS
# ============================================================================

def _all_month_cols() -> list[str]:
    """Return all 24 monthly column names in canonical order."""
    return [
        col
        for m in range(1, 13)
        for col in (f"AMOUNT{str(m).zfill(2)}", f"NOACCT{str(m).zfill(2)}")
    ]

def _canonical_col_order() -> list[str]:
    return ["BRANCH"] + _all_month_cols()

def _scaffold_all_months(df: pl.DataFrame) -> pl.DataFrame:
    """Ensure every AMOUNT01–AMOUNT12 / NOACCT01–NOACCT12 column exists (fill 0)."""
    for m in range(1, 13):
        mm = str(m).zfill(2)
        if f"AMOUNT{mm}" not in df.columns:
            df = df.with_columns(pl.lit(0.0).cast(pl.Float64).alias(f"AMOUNT{mm}"))
        if f"NOACCT{mm}" not in df.columns:
            df = df.with_columns(pl.lit(0).cast(pl.Int64).alias(f"NOACCT{mm}"))
    return df

def _canonical_order(df: pl.DataFrame) -> pl.DataFrame:
    """Reorder columns to: BRANCH, AMOUNT01, NOACCT01, ..., AMOUNT12, NOACCT12."""
    return df.select(_canonical_col_order())

# ============================================================================
# STEP 1 — READ BRANCH REFERENCE FILE
# ============================================================================

def _read_branch_file(path: Path) -> pl.DataFrame:
    """Parse DBRANCH.txt fixed-width flat file.

    SAS equivalent:
        DATA BRANCH (KEEP=BRANCH BRABBR);
          INFILE BRHFILE;
          INPUT @002 BRANCH   3.
                @006 BRABBR  $3.
                @050 OPENIND $1.;
          IF OPENIND NOT IN ('C',' ') AND BRANCH NE 279 OR BRANCH = 219;
        PROC SORT DATA=BRANCH; BY BRANCH;

    Byte offsets are 1-based in SAS; converted to 0-based Python slices:
        BRANCH  : @002, width 3 -> [1:4]
        BRABBR  : @006, width 3 -> [5:8]
        OPENIND : @050, width 1 -> [49:50]
    """
    records = []
    with open(path, "r", encoding="latin1") as fh:
        for raw_line in fh:
            line = raw_line.rstrip("\n").rstrip("\r").ljust(50)

            branch_raw  = line[1:4].strip()
            brabbr      = line[5:8].strip()
            openind     = line[49:50].strip()

            try:
                branch = int(branch_raw)
            except ValueError:
                continue

            # SAS: IF OPENIND NOT IN ('C',' ') AND BRANCH NE 279 OR BRANCH = 219
            if not ((openind not in ("C", "") and branch != 279) or branch == 219):
                continue

            records.append({"BRANCH": branch, "BRABBR": brabbr})

    return (
        pl.DataFrame(
            records,
            schema={"BRANCH": pl.Int64, "BRABBR": pl.Utf8},
        )
        .sort("BRANCH")
    )

# ============================================================================
# STEP 2 — LOAD AND FILTER TLBTRAN WEEKLY FILES
# ============================================================================

def _read_sas7bdat(path: Path) -> pl.DataFrame:
    """Read one .sas7bdat file and return a Polars DataFrame."""
    pdf = pd.read_sas(path, format="sas7bdat", encoding="latin1")
    pdf.columns = [str(c).upper().strip() for c in pdf.columns]
    return pl.from_pandas(pdf)

def _load_tlbtran_all() -> pl.DataFrame:
    """Load and concatenate all 4 weekly TLBTRAN files, filter TRANCODE IN (2222,2223).

    SAS equivalent:
        DATA ALL;
          SET BNM.TLBTRAN&REPTYEAR&REPTMON&WK1
              BNM.TLBTRAN&REPTYEAR&REPTMON&WK2
              BNM.TLBTRAN&REPTYEAR&REPTMON&WK3
              BNM.TLBTRAN&REPTYEAR&REPTMON&WK4;
          IF TRANCODE IN (2222, 2223);
        PROC SORT DATA=ALL; BY REPTDATE BRANCH;
    """
    weekly_dfs = [
        _read_sas7bdat(p)
        for p in (
            INPUT_TLBTRAN_WK1,
            INPUT_TLBTRAN_WK2,
            INPUT_TLBTRAN_WK3,
            INPUT_TLBTRAN_WK4,
        )
    ]
    df = pl.concat(weekly_dfs, how="diagonal_relaxed")
    df = df.filter(pl.col("TRANCODE").cast(pl.Utf8).is_in(["2222", "2223"]))
    df = df.sort(["REPTDATE", "BRANCH"])
    return df

# ============================================================================
# STEP 3 — DERIVE NOACCT FLAG
# ============================================================================

def _derive_noacct(df: pl.DataFrame) -> pl.DataFrame:
    """Assign NOACCT=1 on the first row of each (REPTDATE, BRANCH) group.

    SAS equivalent:
        DATA ALL; SET ALL; BY REPTDATE BRANCH;
          IF FIRST.REPTDATE OR FIRST.BRANCH THEN NOACCT = 1;

    Summing NOACCT per BRANCH yields the count of distinct
    (REPTDATE, BRANCH) combinations — matching SAS PROC SUMMARY behaviour.
    """
    df = df.with_columns([
        (
            (pl.col("REPTDATE") != pl.col("REPTDATE").shift(1)) |
            (pl.col("BRANCH")   != pl.col("BRANCH").shift(1))
        )
        .fill_null(True)
        .cast(pl.Int64)
        .alias("NOACCT")
    ])
    return df

# ============================================================================
# STEP 4 — AGGREGATE CURRENT MONTH (PROC SUMMARY)
# ============================================================================

def _detect_month_from_files() -> str:
    """Detect the report month from the input TLBTRAN filenames.

    Pattern: tlbtran{MM}{W}{YY}.sas7bdat
        tlbtran02126 → MM="02", W="1", YY="26"
        tlbtran03126 → MM="03", W="1", YY="26"

    All 4 weekly files must agree on the same month.
    """
    pattern = re.compile(r"tlbtran(\d{2})\d{1}\d{2}\.sas7bdat", re.IGNORECASE)

    detected = set()
    for p in (INPUT_TLBTRAN_WK1, INPUT_TLBTRAN_WK2, INPUT_TLBTRAN_WK3, INPUT_TLBTRAN_WK4):
        m = pattern.match(p.name)
        if m:
            detected.add(m.group(1))

    if len(detected) == 0:
        raise ValueError("Could not detect month from any TLBTRAN filename.")
    if len(detected) > 1:
        raise ValueError(f"Conflicting months detected across TLBTRAN files: {detected}")

    return detected.pop()  # e.g. "02"


def _build_month_summary(df: pl.DataFrame, con: duckdb.DuckDBPyConnection, file_month: str) -> pl.DataFrame:
    """Aggregate CASHOUT and NOACCT to branch level for the detected file month.

    Uses file_month (from filename) instead of REPTMON (from system date),
    so data always lands in its correct month column regardless of when
    the program is run.

    SAS equivalent:
        PROC SUMMARY DATA=ALL NWAY;
          CLASS REPTDATE BRANCH; VAR CASHOUT NOACCT;
          OUTPUT OUT=CIT SUM=AMOUNT&REPTMON NOACCT&REPTMON;

        DATA CIT; SET CIT; IF AMOUNT&REPTMON = 0 THEN DELETE;

        PROC SUMMARY DATA=CIT NWAY;
          CLASS BRANCH; VAR AMOUNT&REPTMON NOACCT&REPTMON;
          OUTPUT OUT=FINAL SUM=;
    """
    amt_col    = f"AMOUNT{file_month}"
    noacct_col = f"NOACCT{file_month}"

    con.register("all_noacct", df.to_pandas())

    df_agg = con.execute(f"""
        SELECT
            BRANCH,
            SUM(CASHOUT) AS {amt_col},
            SUM(NOACCT)  AS {noacct_col}
        FROM all_noacct
        GROUP BY BRANCH
        HAVING SUM(CASHOUT) <> 0
        ORDER BY BRANCH
    """).pl()

    return df_agg

# ============================================================================
# STEP 5 — ACCUMULATE INTO YEARLY CIT PARQUET
# ============================================================================

def _update_cit_year(df_month: pl.DataFrame, file_month: str) -> pl.DataFrame:
    """Persist the current month's data into the yearly CIT parquet.

    Uses file_month (from filename) to determine which columns to update,
    so re-running for an older month never corrupts other months' data.
    """
    amt_col    = f"AMOUNT{file_month}"
    noacct_col = f"NOACCT{file_month}"

    df_month = df_month.with_columns(pl.col("BRANCH").cast(pl.Int64))

    # ── FIRST RUN: no parquet exists yet ──────────────────────────────────
    if not CIT_YEAR_FILE.exists():
        df_out = _scaffold_all_months(df_month)
        df_out = _canonical_order(df_out)
        df_out.write_parquet(CIT_YEAR_FILE)
        print(f"  Created new yearly CIT parquet: {CIT_YEAR_FILE.name}")
        return df_out.sort("BRANCH")

    # ── SUBSEQUENT RUNS ────────────────────────────────────────────────────
    df_existing = pl.read_parquet(CIT_YEAR_FILE)
    df_existing = df_existing.with_columns(pl.col("BRANCH").cast(pl.Int64))
    df_existing = _scaffold_all_months(df_existing)

    # Drop only this file's month columns — all other months are untouched
    cols_to_drop = [c for c in [amt_col, noacct_col] if c in df_existing.columns]
    if cols_to_drop:
        df_existing = df_existing.drop(cols_to_drop)

    # Full outer join: preserves all branches from both sides
    df_merged = df_existing.join(
        df_month.select(["BRANCH", amt_col, noacct_col]),
        on="BRANCH",
        how="full",
        coalesce=True,
    )

    df_merged = df_merged.with_columns([
        pl.col(c).fill_null(0) for c in df_merged.columns if c != "BRANCH"
    ])

    df_merged = _scaffold_all_months(df_merged)
    df_merged = _canonical_order(df_merged)

    df_merged.write_parquet(CIT_YEAR_FILE)
    print(f"  Updated yearly CIT parquet: {CIT_YEAR_FILE.name} (month {file_month} written)")
    return df_merged.sort("BRANCH")

# ============================================================================
# STEP 6 — MERGE WITH BRANCH REFERENCE
# ============================================================================

def _merge_with_branch(
    df_cit_year: pl.DataFrame,
    df_branch: pl.DataFrame,
    con: duckdb.DuckDBPyConnection,
) -> pl.DataFrame:
    """Merge CIT year data with branch reference, compute SUMAMT/SUMACT, assign NO.

    SAS equivalent:
        DATA CIT;
          MERGE CIT.CIT&REPTYEAR(IN=A) BRANCH(IN=B);
          IF B;
          RETAIN NO 0; BY BRANCH;
          SUMAMT = SUM(OF AMOUNT01-AMOUNT12);
          SUMACT = SUM(OF NOACCT01-NOACCT12);
          NO + 1;
    """
    amount_cols = [f"AMOUNT{str(m).zfill(2)}" for m in range(1, 13)]
    noacct_cols = [f"NOACCT{str(m).zfill(2)}" for m in range(1, 13)]

    amount_select = ", ".join(
        f'COALESCE(c."{col}", 0) AS "{col}"' for col in amount_cols
    )
    noacct_select = ", ".join(
        f'COALESCE(c."{col}", 0) AS "{col}"' for col in noacct_cols
    )

    con.register("cit_year",   df_cit_year.to_pandas())
    con.register("branch_ref", df_branch.to_pandas())

    df = con.execute(f"""
        SELECT
            b.BRANCH,
            b.BRABBR,
            {amount_select},
            {noacct_select}
        FROM branch_ref b
        LEFT JOIN cit_year c ON c.BRANCH = b.BRANCH
        ORDER BY b.BRANCH
    """).pl()

    # Cast all amount/noacct columns to consistent dtypes before horizontal sum
    # to avoid Polars internal panic from mixed i32/i64/f64 in sum_horizontal
    df = df.with_columns(
        [pl.col(c).cast(pl.Float64) for c in amount_cols] +
        [pl.col(c).cast(pl.Int64)   for c in noacct_cols]
    )

    df = df.with_columns([
        pl.sum_horizontal([pl.col(c) for c in amount_cols]).alias("SUMAMT"),
        pl.sum_horizontal([pl.col(c) for c in noacct_cols]).cast(pl.Int64).alias("SUMACT"),
    ])

    # SAS: RETAIN NO 0; NO + 1; → sequential row number starting at 1
    df = df.with_columns(
        pl.int_range(1, pl.len() + 1, dtype=pl.Int64).alias("NO")
    )

    return df

# ============================================================================
# STEP 7 — COMPUTE GRAND TOTALS
# ============================================================================

def _compute_totals(df: pl.DataFrame) -> dict:
    """Replicate PROC SUMMARY NWAY (no CLASS) summing all AMOUNT/NOACCT/SUMAMT/SUMACT.

    SAS equivalent:
        PROC SUMMARY DATA=CIT NWAY;
          VAR NOACCT01-NOACCT12 SUMACT AMOUNT01-AMOUNT12 SUMAMT;
          OUTPUT OUT=TOTCIT SUM=;
    """
    sum_cols = (
        [f"NOACCT{str(m).zfill(2)}" for m in range(1, 13)]
        + ["SUMACT"]
        + [f"AMOUNT{str(m).zfill(2)}" for m in range(1, 13)]
        + ["SUMAMT"]
    )
    return {
        col: (df[col].fill_null(0).sum() if col in df.columns else 0)
        for col in sum_cols
    }

# ============================================================================
# REPORT WRITING HELPERS
# ============================================================================

def _fmt_amount(val) -> str:
    """Format with COMMA20. (no decimals, comma thousands separator)."""
    try:
        return f"{int(val):,}" if val is not None else "0"
    except (ValueError, TypeError):
        return "0"

def _fmt_noacct(val) -> str:
    """Format with COMMA20."""
    try:
        return f"{int(val):,}" if val is not None else "0"
    except (ValueError, TypeError):
        return "0"

def _place(buf: list, pos: int, text: str) -> None:
    """Place text into buf (list of chars) at 1-based SAS @pos column."""
    idx = pos - 1
    for i, ch in enumerate(text):
        dest = idx + i
        if dest >= len(buf):
            buf.extend([" "] * (dest - len(buf) + 1))
        buf[dest] = ch

def _render(buf: list) -> str:
    return "".join(buf).rstrip()

# ============================================================================
# STEP 8 — WRITE REPORT
# ============================================================================

def _write_report(df: pl.DataFrame, totals: dict, output_file: Path) -> None:
    """Write the Cash-In-Transit report replicating SAS DATA _NULL_ FILE CITLIST output.

    SAS equivalent (DATA _NULL_ SET CIT):
        FILE CITLIST;
        IF _N_ = 1 THEN DO;
            PUT @1 '1CASH-IN-TRANSIT REPORT' "&REPTYEAR2";
            PUT <header line 1>;
            PUT <header line 2>;
        END;
        PUT @002 NO  @006 BRANCH  @013 BRABBR
            @020 AMOUNT01  @038 NOACCT01  ...
            @344 SUMAMT    @362 SUMACT;

    Followed by DATA _NULL_ SET TOTCIT (FILE CITLIST MOD):
        PUT @013 'TOTAL' @020 AMOUNT01 ... @362 SUMACT;

    ASA carriage-control: '1' at column 1 signals a form feed (new page).
    Page length defaults to 60 lines (not specified in OPTIONS, using standard default).

    Column positions (1-based @col notation):
        NO=2, BRANCH=6, BRABBR=13
        AMOUNT01=20, NOACCT01=38
        AMOUNT02=47, NOACCT02=65
        AMOUNT03=74, NOACCT03=92
        AMOUNT04=101, NOACCT04=119
        AMOUNT05=128, NOACCT05=146
        AMOUNT06=155, NOACCT06=173
        AMOUNT07=182, NOACCT07=200
        AMOUNT08=209, NOACCT08=227
        AMOUNT09=236, NOACCT09=254
        AMOUNT10=263, NOACCT10=281
        AMOUNT11=290, NOACCT11=308
        AMOUNT12=317, NOACCT12=335
        SUMAMT=344, SUMACT=362
    """
    MONTH_LABELS     = ["JAN","FEB","MAR","APR","MAY","JUN",
                        "JUL","AUG","SEP","OCT","NOV","DEC"]
    AMT_POSITIONS    = [20,  47,  74, 101, 128, 155, 182, 209, 236, 263, 290, 317]
    NOACCT_POSITIONS = [38,  65,  92, 119, 146, 173, 200, 227, 254, 281, 308, 335]
    SUMAMT_POS       = 344
    SUMACT_POS       = 362
    BUF_WIDTH        = 400

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as f:

        # ── PAGE TITLE ─────────────────────────────────────────────────────
        # SAS: PUT @1 '1CASH-IN-TRANSIT REPORT' "&REPTYEAR2";
        # '1' at column 1 = ASA form-feed carriage-control character.
        title_buf = list(" " * BUF_WIDTH)
        _place(title_buf, 1, f"1CASH-IN-TRANSIT REPORT {REPTYEAR2}")
        f.write(_render(title_buf) + "\n")

        # ── HEADER LINE 1 ──────────────────────────────────────────────────
        h1 = list(" " * BUF_WIDTH)
        _place(h1, 2,  "NO")
        _place(h1, 6,  "BRANCH")
        _place(h1, 13, "BRANCH")
        for amt_pos, noacct_pos, label in zip(AMT_POSITIONS, NOACCT_POSITIONS, MONTH_LABELS):
            _place(h1, amt_pos,    label)
            _place(h1, noacct_pos, "NO OF")
        _place(h1, SUMAMT_POS, "TOTAL")
        _place(h1, SUMACT_POS, "TOTAL NO OF")
        f.write(_render(h1) + "\n")

        # ── HEADER LINE 2 ──────────────────────────────────────────────────
        h2 = list(" " * BUF_WIDTH)
        _place(h2, 6, "CODE")
        for noacct_pos in NOACCT_POSITIONS:
            _place(h2, noacct_pos, "TRANSIT")
        _place(h2, SUMACT_POS, "TRANSIT")
        f.write(_render(h2) + "\n")

        # ── DATA ROWS ──────────────────────────────────────────────────────
        for row in df.iter_rows(named=True):
            buf = list(" " * BUF_WIDTH)
            _place(buf, 2,  str(int(row["NO"])))
            _place(buf, 6,  str(int(row["BRANCH"])) if row["BRANCH"] is not None else "")
            _place(buf, 13, str(row["BRABBR"] or ""))
            for i, (amt_pos, noacct_pos) in enumerate(zip(AMT_POSITIONS, NOACCT_POSITIONS)):
                mm = str(i + 1).zfill(2)
                _place(buf, amt_pos,    _fmt_amount(row.get(f"AMOUNT{mm}", 0)))
                _place(buf, noacct_pos, _fmt_noacct(row.get(f"NOACCT{mm}", 0)))
            _place(buf, SUMAMT_POS, _fmt_amount(row.get("SUMAMT", 0)))
            _place(buf, SUMACT_POS, _fmt_noacct(row.get("SUMACT", 0)))
            f.write(_render(buf) + "\n")

        # ── TOTAL ROW (DATA _NULL_ SET TOTCIT — FILE CITLIST MOD) ──────────
        # SAS: PUT @013 'TOTAL' @020 AMOUNT01 ... @362 SUMACT;
        tot = list(" " * BUF_WIDTH)
        _place(tot, 13, "TOTAL")
        for i, (amt_pos, noacct_pos) in enumerate(zip(AMT_POSITIONS, NOACCT_POSITIONS)):
            mm = str(i + 1).zfill(2)
            _place(tot, amt_pos,    _fmt_amount(totals.get(f"AMOUNT{mm}", 0)))
            _place(tot, noacct_pos, _fmt_noacct(totals.get(f"NOACCT{mm}", 0)))
        _place(tot, SUMAMT_POS, _fmt_amount(totals.get("SUMAMT", 0)))
        _place(tot, SUMACT_POS, _fmt_noacct(totals.get("SUMACT", 0)))
        f.write(_render(tot) + "\n")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

print("=" * 70)
print("CASH-IN-TRANSIT MONTHLY ACCUMULATED REPORT")
print("=" * 70)
print(f"Report Date  : {REPTDATE.strftime('%d/%m/%Y')}")
print(f"Report Month : {REPTMON}")
print(f"Report Year  : {REPTYEAR2}")
print(f"CIT Parquet  : {CIT_YEAR_FILE}")
print()

con = duckdb.connect(database=":memory:")

try:
    # ── Step 1: Branch reference ───────────────────────────────────────────
    print("Step 1: Reading branch reference file (DBRANCH.txt)...")
    df_branch = _read_branch_file(INPUT_BRHFILE)
    print(f"  Branches loaded: {len(df_branch):,}")

    # ── Step 2: Load and filter weekly TLBTRAN files ───────────────────────
    print("\nStep 2: Loading and filtering tlbtran weekly files...")
    df_all = _load_tlbtran_all()
    print(f"  Transactions after TRANCODE filter: {len(df_all):,}")

    # ── Step 3: Derive NOACCT first-row flags ──────────────────────────────
    print("\nStep 3: Deriving NOACCT first-row flags per (REPTDATE, BRANCH)...")
    df_all = _derive_noacct(df_all)

    # ── Step 4: Detect month from filenames ────────────────────────────────
    print("\nStep 4: Detecting report month from TLBTRAN filenames...")
    file_month = _detect_month_from_files()
    print(f"  Detected month from files: {file_month}")

    # ── Step 4b: Aggregate current month to branch level ──────────────────
    print(f"\nStep 4b: Aggregating month {file_month} CASHOUT and NOACCT by BRANCH...")
    df_month = _build_month_summary(df_all, con, file_month)
    print(f"  Branches with non-zero CASHOUT this month: {len(df_month):,}")

    # ── Step 5: Accumulate into yearly CIT parquet ─────────────────────────
    print(f"\nStep 5: Updating yearly CIT parquet [{CIT_YEAR_FILE.name}]...")
    df_cit_year = _update_cit_year(df_month, file_month)
    print(f"  Total branches in yearly parquet: {len(df_cit_year):,}")

    # ── Step 6: Merge with branch reference ────────────────────────────────
    print("\nStep 6: Merging CIT year data with branch reference...")
    df_report = _merge_with_branch(df_cit_year, df_branch, con)
    print(f"  Report rows: {len(df_report):,}")

    # ── Step 7: Grand totals ───────────────────────────────────────────────
    print("\nStep 7: Computing grand totals (TOTCIT)...")
    totals = _compute_totals(df_report)

    # ── Step 8: Write report ───────────────────────────────────────────────
    print("\nStep 8: Writing report...")
    _write_report(df_report, totals, OUTPUT_CITLIST)
    print(f"  Report saved: {OUTPUT_CITLIST}")

    # ── Preview ────────────────────────────────────────────────────────────
    print(f"\n{'=' * 70}")
    print(f"PREVIEW: {OUTPUT_CITLIST.name}")
    print("=" * 70)
    with open(OUTPUT_CITLIST, "r", encoding="utf-8") as f:
        print(f.read())
    print("=" * 70)

    print("\nGENERATED FILES:")
    print(f"  Cash-In-Transit Report : {OUTPUT_CITLIST}")
    print(f"  Yearly CIT Parquet     : {CIT_YEAR_FILE}")
    print("\nREPORT GENERATION COMPLETE")

finally:
    con.close()
