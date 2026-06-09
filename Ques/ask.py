#!/usr/bin/env python3
"""
Program : EIBMCITR.py
Purpose : Monthly Accumulated Report for Cash-In-Transit
          Reads the 4 latest TLBTRAN .sas7bdat files (representing 4 weeks of
          the report month) and a DBRANCH.txt fixed-width branch reference file.
          Filters TRANCODE IN (2222, 2223), aggregates CASHOUT and account counts
          per REPTDATE/BRANCH, accumulates monthly totals into CIT{year}.parquet,
          then writes a formatted Cash-In-Transit report.
"""

import duckdb
import polars as pl
import pandas as pd
from pathlib import Path

from REPTDATE import get_reptdate_values
from input_date import get_latest_file
from output_date import build_output_file

# =====================================================================
# CIT SCHEMA (SAS-LIKE FIXED STRUCTURE)
# =====================================================================
CIT_SCHEMA = {
    "BRANCH": pl.Int64,
}

for m in range(1, 13):
    mm = str(m).zfill(2)
    CIT_SCHEMA[f"AMOUNT{mm}"] = pl.Float64
    CIT_SCHEMA[f"NOACCT{mm}"] = pl.Int64

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
INPUT_DIR  = BASE_DIR / "input" / "prod" / "EIBMCITR"
CIT_DIR    = BASE_DIR / "input" / "prod" / "EIBMCITR"
OUTPUT_DIR = BASE_DIR / "output" / "EIBMCITR"

# # Production Path
# INPUT_DIR  = Path("/dwh")
# CIT_DIR    = Path("/sas/cit")
# OUTPUT_DIR = Path("/host/mis/output/report")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CIT_DIR.mkdir(parents=True, exist_ok=True)

# Branch reference fixed-width flat file
INPUT_BRHFILE = INPUT_DIR / "DBRANCH.TXT"
# INPUT_BRHFILE = Path("/stgsrcsys/host/ftpfiles") / "DBRANCH.txt"

# ============================================================================
# REPORT DATE (from REPTDATE module)
# ============================================================================
reptdate_values = get_reptdate_values()
REPTDATE  = reptdate_values.reptdate
REPTYEAR  = reptdate_values.reptyear       # 2-digit year
REPTMON   = reptdate_values.reptmon        # zero-padded month

_rv4 = get_reptdate_values(year_format="%Y")
REPTYEAR2 = _rv4.reptyear                  # 4-digit year (PUT(REPTDATE,YEAR4.))

WK1 = "01"
WK2 = "02"
WK3 = "03"
WK4 = "04"

# ============================================================================
# DERIVED INPUT PATHS
# ============================================================================
# Resolve the 4 latest TLBTRAN files available in INPUT_DIR.
# The program runs on the last day of the month or the 1st of the following
# month; either way, the 4 most-recently-dated TLBTRAN files represent the
# 4 weeks of the report month.
#
# get_latest_file returns only the single most-recent file, so we collect
# all TLBTRAN candidates and sort by parsed date to pick the top 4.

from input_date import extract_key, SUPPORTED_EXTENSIONS

def _get_latest_n_files(directory: Path, prefix: str, n: int) -> list:
    """Return the n most-recently-dated files matching prefix in directory."""
    files = [
        f for f in directory.iterdir()
        if f.is_file()
        and f.suffix.lower() in SUPPORTED_EXTENSIONS
        and f.name.upper().startswith(prefix.upper())
        and extract_key(f.name) is not None
    ]
    if len(files) < n:
        raise FileNotFoundError(
            f"Expected at least {n} files with prefix '{prefix}' in {directory}, "
            f"found {len(files)}."
        )
    files_sorted = sorted(files, key=lambda f: extract_key(f.name), reverse=True)
    # Return in ascending date order (week 1 first)
    return list(reversed(files_sorted[:n]))

# INPUT_TLBTRAN_FILES = _get_latest_n_files(INPUT_DIR, "tlbtran", 4)

# INPUT_TLBTRAN_WK1 = INPUT_TLBTRAN_FILES[0]
# INPUT_TLBTRAN_WK2 = INPUT_TLBTRAN_FILES[1]
# INPUT_TLBTRAN_WK3 = INPUT_TLBTRAN_FILES[2]
# INPUT_TLBTRAN_WK4 = INPUT_TLBTRAN_FILES[3]

INPUT_TLBTRAN_WK1 = INPUT_DIR / "tlbtran01126.sas7bdat"
INPUT_TLBTRAN_WK2 = INPUT_DIR / "tlbtran01226.sas7bdat"
INPUT_TLBTRAN_WK3 = INPUT_DIR / "tlbtran01326.sas7bdat"
INPUT_TLBTRAN_WK4 = INPUT_DIR / "tlbtran01426.sas7bdat"

# Persistent yearly CIT accumulation parquet
CIT_YEAR_FILE = CIT_DIR / f"CIT{REPTYEAR}.parquet"

# Output paths
OUTPUT_CITLIST = build_output_file(OUTPUT_DIR, "EIBMCITR_CITLIST").with_suffix(".txt")
# Output example: EIBMCITR_CITLIST_310526.txt

# ============================================================================
# INPUT FILE EXISTENCE CHECK — fail fast before any processing
# ============================================================================
_REQUIRED_INPUTS = {
    "TLBTRAN Week 1" : INPUT_TLBTRAN_WK1,
    "TLBTRAN Week 2" : INPUT_TLBTRAN_WK2,
    "TLBTRAN Week 3" : INPUT_TLBTRAN_WK3,
    "TLBTRAN Week 4" : INPUT_TLBTRAN_WK4,
    "Branch File"    : INPUT_BRHFILE,
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

# =========================================================
# MODULE A — SAS CIT SCHEMA ENGINE (PLACE HERE)
# =========================================================

def cit_schema():
    schema = {"BRANCH": pl.Int64}

    for m in range(1, 13):
        mm = str(m).zfill(2)
        schema[f"AMOUNT{mm}"] = pl.Float64
        schema[f"NOACCT{mm}"] = pl.Int64

    return schema


def create_empty_cit_year():
    return pl.DataFrame(schema=cit_schema())
          
# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _create_empty_cit_year() -> pl.DataFrame:
    """SAS-equivalent empty dataset with full schema"""

    data = {}

    data["BRANCH"] = pl.Series([], dtype=pl.Int64)

    for m in range(1, 13):
        mm = str(m).zfill(2)
        data[f"AMOUNT{mm}"] = pl.Series([], dtype=pl.Float64)
        data[f"NOACCT{mm}"] = pl.Series([], dtype=pl.Int64)

    return pl.DataFrame(data)


def _safe_read_cit_parquet(path: Path) -> pl.DataFrame:
    """
    Safe parquet reader (SAS-style robustness)
    Prevents corrupted or schema-less parquet crashes
    """

    try:
        if not path.exists():
            return _create_empty_cit_year()

        # extra safety: detect broken files
        if path.stat().st_size < 500:
            return _create_empty_cit_year()

        df = pl.read_parquet(path)

        # schema validation (VERY IMPORTANT)
        if df.is_empty() or "BRANCH" not in df.columns:
            return _create_empty_cit_year()

        return df

    except Exception:
        # any corruption fallback
        return _create_empty_cit_year()


def _read_sas7bdat(path: Path) -> pl.DataFrame:
    """Read one .sas7bdat file and return a Polars DataFrame."""
    if not path.exists():
        raise FileNotFoundError(f"Missing required input file: {path}")

    pandas_df = pd.read_sas(
        path,
        format="sas7bdat",
        encoding="latin1",
    )
    pandas_df.columns = [
        str(col).upper().strip()
        for col in pandas_df.columns
    ]
    return pl.from_pandas(pandas_df)


def _read_branch_file(path: Path) -> pl.DataFrame:
    """Parse DBRANCH.txt fixed-width flat file.

    SAS equivalent:
        DATA BRH (KEEP=D_TRX_BRANCH D_TRX_BRCHCODE);
            INFILE BRHFILE;
            INPUT @002 D_TRX_BRCHCODE   3.
                  @006 D_TRX_BRANCH    $3.
                  @050 STATUS   $1.
                  ;
            IF STATUS = 'O' AND D_TRX_BRCHCODE NOT IN (101, 187, 279);
        PROC SORT DATA=BRH; BY D_TRX_BRANCH;

    Byte offsets are 1-based in SAS; converted to 0-based Python slices:
        D_TRX_BRCHCODE : @002, width 3  -> [1:4]
        D_TRX_BRANCH   : @006, width 3  -> [5:8]
        STATUS         : @050, width 1  -> [49:50]
    """
    records = []
    with open(path, "r", encoding="latin1") as fh:
        for raw_line in fh:
            line = raw_line.rstrip("\n").rstrip("\r")
            if len(line) < 50:
                line = line.ljust(50)

            brchcode_raw = line[1:4].strip()
            branch_raw   = line[5:8].strip()
            status       = line[49:50].strip()

            try:
                brchcode = int(brchcode_raw)
            except ValueError:
                continue

            # SAS: IF STATUS = 'O' AND D_TRX_BRCHCODE NOT IN (101,187,279)
            if status != "O":
                continue
            if brchcode in (101, 187, 279):
                continue

            records.append({
                "D_TRX_BRCHCODE": brchcode,
                "D_TRX_BRANCH":   branch_raw,
            })

    df = pl.DataFrame(
        records,
        schema={
            "D_TRX_BRCHCODE": pl.Int64,
            "D_TRX_BRANCH":   pl.Utf8,
        },
    ).sort("D_TRX_BRANCH")

    return df


def _load_tlbtran_all(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
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
        _read_sas7bdat(wk_path)
        for wk_path in (
            INPUT_TLBTRAN_WK1,
            INPUT_TLBTRAN_WK2,
            INPUT_TLBTRAN_WK3,
            INPUT_TLBTRAN_WK4,
        )
    ]

    df_all = pl.concat(weekly_dfs, how="diagonal")

    # ----- DEBUG data type START -----
    print("\n=== SCHEMA ===")
    print(df_all.schema)

    print("\n=== DTYPES ===")
    print(df_all.dtypes)

    # print("\n=== TRANCODE SAMPLE ===")
    # print(df_all.select("TRANCODE").head(20))
    # # ----- DEBUG data type END -----

    df_all = df_all.filter(pl.col("TRANCODE").is_in(["2222", "2223"]))
    df_all = df_all.sort(["REPTDATE", "BRANCH"])

    con.register("tlbtran_all", df_all.to_pandas())
    return df_all


def _derive_noacct(df_all: pl.DataFrame) -> pl.DataFrame:
    """Assign NOACCT=1 on the first row of each (REPTDATE, BRANCH) group.

    SAS equivalent:
        DATA ALL;
            SET ALL; BY REPTDATE BRANCH;
            IF FIRST.REPTDATE OR FIRST.BRANCH THEN NOACCT = 1;

    NOACCT=1 marks the first row per (REPTDATE, BRANCH) group; 0 elsewhere.
    Summing NOACCT per BRANCH then yields the count of distinct
    (REPTDATE, BRANCH) combinations — matching SAS PROC SUMMARY behaviour.
    """
    df_all = df_all.with_columns([
        (
            (pl.col("REPTDATE") != pl.col("REPTDATE").shift(1)) |
            (pl.col("BRANCH")   != pl.col("BRANCH").shift(1))
        ).fill_null(True).cast(pl.Int64).alias("NOACCT")
    ])
    return df_all


# =========================================================
# MODULE B — MONTHLY FINAL BUILDER (SAS PROC SUMMARY)
# =========================================================

def build_month_final(con, df_all):
    amount_col = f"AMOUNT{REPTMON}"
    noacct_col = f"NOACCT{REPTMON}"

    con.register("all_noacct", df_all.to_pandas())

    df = con.execute(f"""
        WITH cit AS (
            SELECT
                BRANCH,
                REPTDATE,
                SUM(CASHOUT) AS AMT,
                SUM(NOACCT) AS CNT
            FROM all_noacct
            GROUP BY REPTDATE, BRANCH
        )
        SELECT
            BRANCH,
            SUM(AMT) AS "{amount_col}",
            SUM(CNT) AS "{noacct_col}"
        FROM cit
        GROUP BY BRANCH
    """).df()

    return pl.from_pandas(df)


# =========================================================
# MODULE C — SAS YEAR MERGE ENGINE
# =========================================================

def safe_read_cit(path: Path) -> pl.DataFrame:
    try:
        if not path.exists():
            return create_empty_cit_year()

        df = pl.read_parquet(path)

        if df.is_empty():
            return create_empty_cit_year()

        return df

    except Exception:
        return create_empty_cit_year()


def append_to_cit_year(df_final: pl.DataFrame) -> pl.DataFrame:

    full_schema = create_empty_cit_year()

    if not CIT_YEAR_FILE.exists():

        df_year = full_schema.join(
            df_final,
            on="BRANCH",
            how="full",
            coalesce=True
        ).fill_null(0)

        df_year.write_parquet(CIT_YEAR_FILE)
        return df_year.sort("BRANCH")

    df_existing = safe_read_cit(CIT_YEAR_FILE)

    df_existing = full_schema.join(
        df_existing,
        on="BRANCH",
        how="left"
    ).fill_null(0)

    amount_col = f"AMOUNT{REPTMON}"
    noacct_col = f"NOACCT{REPTMON}"

    for c in (amount_col, noacct_col):
        if c in df_existing.columns:
            df_existing = df_existing.drop(c)

    df_year = df_existing.join(
        df_final,
        on="BRANCH",
        how="full",
        coalesce=True
    ).fill_null(0)

    df_year.write_parquet(CIT_YEAR_FILE)

    return df_year.sort("BRANCH")


def _merge_with_branch(
    con: duckdb.DuckDBPyConnection,
    df_cit_year: pl.DataFrame,
    df_branch: pl.DataFrame,
) -> pl.DataFrame:
    """Merge CIT year data with branch reference, compute SUMAMT/SUMACT, assign NO.

    SAS equivalent:
        DATA CIT;
            MERGE CIT.CIT&REPTYEAR(IN=A) BRANCH(IN=B);
            IF B;
            RETAIN NO 0;
            BY BRANCH;
            SUMAMT = SUM(OF AMOUNT01-AMOUNT12);
            SUMACT = SUM(OF NOACCT01-NOACCT12);
            NO + 1;

    SAS keeps rows where BRANCH exists in the BRANCH dataset (IF B).
    BRANCH in CIT maps to D_TRX_BRCHCODE in the BRANCH file.
    """
    con.register("cit_year",   df_cit_year.to_pandas())
    con.register("branch_ref", df_branch.to_pandas())

    amount_select = ", ".join(
        f'COALESCE(c."AMOUNT{str(m).zfill(2)}", 0) AS "AMOUNT{str(m).zfill(2)}"'
        for m in range(1, 13)
    )
    noacct_select = ", ".join(
        f'COALESCE(c."NOACCT{str(m).zfill(2)}", 0) AS "NOACCT{str(m).zfill(2)}"'
        for m in range(1, 13)
    )

    merged = con.execute(f"""
        SELECT
            b.D_TRX_BRCHCODE AS BRANCH,
            b.D_TRX_BRANCH   AS BRABBR,
            {amount_select},
            {noacct_select}
        FROM branch_ref b
        LEFT JOIN cit_year c
            ON c.BRANCH = b.D_TRX_BRCHCODE
        ORDER BY b.D_TRX_BRANCH
    """).df()

    df = pl.from_pandas(merged)

    amount_col_names = [f"AMOUNT{str(m).zfill(2)}" for m in range(1, 13)]
    noacct_col_names = [f"NOACCT{str(m).zfill(2)}" for m in range(1, 13)]

    df = df.with_columns([
        pl.sum_horizontal([pl.col(c) for c in amount_col_names]).alias("SUMAMT"),
        pl.sum_horizontal([pl.col(c) for c in noacct_col_names]).alias("SUMACT"),
    ])

    # SAS: RETAIN NO 0; NO + 1;  -> sequential row number starting at 1
    df = df.with_columns(
        pl.int_range(1, pl.len() + 1, dtype=pl.Int64).alias("NO")
    )

    return df


def _compute_totals(df_cit_report: pl.DataFrame) -> dict:
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
        col: (df_cit_report[col].fill_null(0).sum() if col in df_cit_report.columns else 0)
        for col in sum_cols
    }


# ============================================================================
# REPORT FORMATTING
# ============================================================================

def _fmt_amount(val) -> str:
    """Format AMOUNT columns with COMMA20. (no decimals, comma thousands separator)."""
    if val is None:
        return "0"
    try:
        return f"{int(val):,}"
    except (ValueError, TypeError):
        return "0"


def _fmt_noacct(val) -> str:
    """Format NOACCT columns with COMMA20."""
    if val is None:
        return "0"
    try:
        return f"{int(val):,}"
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


def _write_report(df_cit_report: pl.DataFrame, totals: dict, output_file: Path) -> None:
    """Write the Cash-In-Transit report replicating SAS DATA _NULL_ FILE CITLIST output.

    SAS equivalent (DATA _NULL_ SET CIT):
        FILE CITLIST;
        IF _N_ = 1 THEN DO;
            PUT @1 '1CASH-IN-TRANSIT REPORT' "&REPTYEAR2";
            PUT <header line 1>;
            PUT <header line 2>;
        END;
        PUT @002 NO  @006 BRANCH  @013 BRABBR
            @020 AMOUNT01  @038 NOACCT01 ...
            @344 SUMAMT    @362 SUMACT;

    Followed by DATA _NULL_ SET TOTCIT (FILE CITLIST MOD):
        PUT @013 'TOTAL' @020 AMOUNT01 ... @362 SUMACT;

    ASA carriage-control: '1' at column 1 signals a form feed (new page).
    No page size (PS=) is specified in the original OPTIONS statement — the
    report is a single continuous flat file with no pagination.

    Column positions follow the SAS @col notation (1-based):
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
    MONTH_LABELS = [
        "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
        "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
    ]

    AMT_POSITIONS    = [20, 47, 74, 101, 128, 155, 182, 209, 236, 263, 290, 317]
    NOACCT_POSITIONS = [38, 65, 92, 119, 146, 173, 200, 227, 254, 281, 308, 335]
    SUMAMT_POS       = 344
    SUMACT_POS       = 362

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as f:

        # ── PAGE TITLE ────────────────────────────────────────────────────
        # SAS: PUT @1 '1CASH-IN-TRANSIT REPORT' "&REPTYEAR2";
        # '1' at column 1 is the ASA form-feed carriage-control character.
        title_buf = list(" " * 400)
        _place(title_buf, 1, f"1CASH-IN-TRANSIT REPORT {REPTYEAR2}")
        f.write(_render(title_buf) + "\n")

        # ── HEADER LINE 1 ─────────────────────────────────────────────────
        h1_buf = list(" " * 400)
        _place(h1_buf, 2,  "NO")
        _place(h1_buf, 6,  "BRANCH")
        _place(h1_buf, 13, "BRANCH")
        for amt_pos, noacct_pos, label in zip(
            AMT_POSITIONS, NOACCT_POSITIONS, MONTH_LABELS
        ):
            _place(h1_buf, amt_pos,    label)
            _place(h1_buf, noacct_pos, "NO OF")
        _place(h1_buf, SUMAMT_POS, "TOTAL")
        _place(h1_buf, SUMACT_POS, "TOTAL NO OF")
        f.write(_render(h1_buf) + "\n")

        # ── HEADER LINE 2 ─────────────────────────────────────────────────
        h2_buf = list(" " * 400)
        _place(h2_buf, 6, "CODE")
        for noacct_pos in NOACCT_POSITIONS:
            _place(h2_buf, noacct_pos, "TRANSIT")
        _place(h2_buf, SUMACT_POS, "TRANSIT")
        f.write(_render(h2_buf) + "\n")

        # ── DATA ROWS ─────────────────────────────────────────────────────
        for row in df_cit_report.iter_rows(named=True):
            row_buf = list(" " * 400)
            _place(row_buf, 2,  str(int(row["NO"])))
            _place(row_buf, 6,  str(int(row["BRANCH"])) if row["BRANCH"] is not None else "")
            _place(row_buf, 13, str(row["BRABBR"] or ""))

            for i, (amt_pos, noacct_pos) in enumerate(
                zip(AMT_POSITIONS, NOACCT_POSITIONS)
            ):
                mm = str(i + 1).zfill(2)
                _place(row_buf, amt_pos,    _fmt_amount(row.get(f"AMOUNT{mm}", 0)))
                _place(row_buf, noacct_pos, _fmt_noacct(row.get(f"NOACCT{mm}", 0)))

            _place(row_buf, SUMAMT_POS, _fmt_amount(row.get("SUMAMT", 0)))
            _place(row_buf, SUMACT_POS, _fmt_noacct(row.get("SUMACT", 0)))
            f.write(_render(row_buf) + "\n")

        # ── TOTAL ROW (DATA _NULL_ SET TOTCIT — FILE CITLIST MOD) ─────────
        # SAS: PUT @013 'TOTAL' @020 AMOUNT01 ... @362 SUMACT;
        tot_buf = list(" " * 400)
        _place(tot_buf, 13, "TOTAL")
        for i, (amt_pos, noacct_pos) in enumerate(
            zip(AMT_POSITIONS, NOACCT_POSITIONS)
        ):
            mm = str(i + 1).zfill(2)
            _place(tot_buf, amt_pos,    _fmt_amount(totals.get(f"AMOUNT{mm}", 0)))
            _place(tot_buf, noacct_pos, _fmt_noacct(totals.get(f"NOACCT{mm}", 0)))
        _place(tot_buf, SUMAMT_POS, _fmt_amount(totals.get("SUMAMT", 0)))
        _place(tot_buf, SUMACT_POS, _fmt_noacct(totals.get("SUMACT", 0)))
        f.write(_render(tot_buf) + "\n")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

print("=" * 70)
print("CASH-IN-TRANSIT MONTHLY ACCUMULATED REPORT")
print("=" * 70)
print(f"\nReport Date   : {REPTDATE.strftime('%d/%m/%Y')}")
print(f"Report Month  : {REPTMON}")
print(f"Report Year   : {REPTYEAR2}")
# print(f"\nResolved tlbtran input files:")
# for i, p in enumerate(INPUT_TLBTRAN_FILES, 1):
#     print(f"  Week {i}: {p.name}")

con = duckdb.connect(database=":memory:")

try:
    print("\nStep 1: Reading branch reference file (DBRANCH.txt)...")
    df_branch = _read_branch_file(INPUT_BRHFILE)
    print(f"Branches loaded: {len(df_branch):,}")

    print("\nStep 2: Loading and filtering tlbtran weekly files...")
    df_all = _load_tlbtran_all(con)
    print(f"Transactions after TRANCODE filter: {len(df_all):,}")

    print("\nStep 3: Deriving NOACCT first-row flags per (REPTDATE, BRANCH)...")
    df_all = _derive_noacct(df_all)

    print("\nStep 4: Aggregating CASHOUT and NOACCT to FINAL (by BRANCH)...")
    df_final = build_month_final(con, df_all)
    print(f"Branches with non-zero CASHOUT: {len(df_final):,}")

    # DEBUG START HERE
    print("DEBUG df_final.columns:")
    print(df_final.columns)

    print("DEBUG df_final.schema:")
    print(df_final.schema)
    # DEBUG END HERE

    print(f"\nStep 5: Appending to yearly CIT file [{CIT_YEAR_FILE.name}]...")
    df_cit_year = append_to_cit_year(df_final)
    print(f"Yearly CIT rows: {len(df_cit_year):,}")

    print("\nStep 6: Merging CIT year data with branch reference...")
    df_cit_report = _merge_with_branch(con, df_cit_year, df_branch)
    print(f"Report rows: {len(df_cit_report):,}")

    print("\nStep 7: Computing grand totals (TOTCIT)...")
    totals = _compute_totals(df_cit_report)

    print("\nStep 8: Writing report...")
    _write_report(df_cit_report, totals, OUTPUT_CITLIST)
    print(f"Report saved: {OUTPUT_CITLIST}")

    print(f"\n========== PREVIEW: {OUTPUT_CITLIST.name} ==========\n")
    with open(OUTPUT_CITLIST, "r", encoding="utf-8") as f:
        print(f.read())
    print(f"========== END PREVIEW ==========\n")

    print("\n" + "=" * 70)
    print("GENERATED REPORT:")
    print("=" * 70)
    print(f"  Cash-In-Transit Report : {OUTPUT_CITLIST}")
    print("\nREPORT GENERATION COMPLETE")

finally:
    con.close()
