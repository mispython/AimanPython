#!/usr/bin/env python3
"""
Program : EIBMCITR.py
Purpose : Monthly Accumulated Report for Cash-In-Transit
          Reads 4 weekly TLBTRAN .sas7bdat files and a DBRANCH.txt fixed-width file.
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

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
INPUT_DIR  = BASE_DIR / "input" / "prod"
CIT_DIR    = BASE_DIR / "input" / "cit"
OUTPUT_DIR = BASE_DIR / "output" / "EIBMCITR"

# # Production Path
# INPUT_DIR  = Path("/dwh")
# CIT_DIR    = Path("/sas/cit")
# OUTPUT_DIR = Path("/host/mis/output/report")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CIT_DIR.mkdir(parents=True, exist_ok=True)

# Input paths - 4 weekly TLBTRAN .sas7bdat files (one per week of the month)
# File name example: TLBTRAN2605011.sas7bdat (reptyear + reptmon + wk)
INPUT_BNM_WK1 = get_latest_file(INPUT_DIR, "TLBTRAN")   # Week 1
# Resolved at runtime below after REPTMON/REPTYEAR are known

# Branch reference fixed-width flat file
INPUT_BRHFILE = INPUT_DIR / "DBRANCH.txt"
# INPUT_BRHFILE = Path("/sas/refdata") / "DBRANCH.txt"

# Output paths
OUTPUT_CITLIST = build_output_file(OUTPUT_DIR, "EIBMCITR_CITLIST").with_suffix(".txt")
# Output example: EIBMCITR_CITLIST_180526.txt

# ============================================================================
# REPORT DATE (from REPTDATE module)
# ============================================================================
reptdate_values = get_reptdate_values()
REPTDATE  = reptdate_values.reptdate
REPTYEAR  = reptdate_values.reptyear       # 2-digit year
REPTMON   = reptdate_values.reptmon        # zero-padded month
REPTDAY   = reptdate_values.reptday
NOWK      = reptdate_values.nowk

from REPTDATE import get_reptdate_values as _grv
_rv4 = _grv(year_format="%Y")
REPTYEAR2 = _rv4.reptyear                  # 4-digit year (PUT(REPTDATE,YEAR4.))

WK1 = "01"
WK2 = "02"
WK3 = "03"
WK4 = "04"

# ============================================================================
# DERIVED INPUT PATHS — TLBTRAN weekly files
# ============================================================================
# SAS: SET BNM.TLBTRAN&REPTYEAR&REPTMON&WK1 ... &WK4
# File name pattern: TLBTRAN{reptyear}{reptmon}{wk}.sas7bdat
def _tlbtran_path(wk: str) -> Path:
    return INPUT_DIR / f"TLBTRAN{REPTYEAR}{REPTMON}{wk}.sas7bdat"

INPUT_TLBTRAN_WK1 = _tlbtran_path(WK1)
INPUT_TLBTRAN_WK2 = _tlbtran_path(WK2)
INPUT_TLBTRAN_WK3 = _tlbtran_path(WK3)
INPUT_TLBTRAN_WK4 = _tlbtran_path(WK4)

# Persistent yearly CIT accumulation parquet
CIT_YEAR_FILE = CIT_DIR / f"CIT{REPTYEAR}.parquet"

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


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

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
            # Pad line to at least 50 characters to avoid short-line IndexErrors
            line = raw_line.rstrip("\n").rstrip("\r")
            if len(line) < 50:
                line = line.ljust(50)

            brchcode_raw = line[1:4].strip()
            branch_raw   = line[5:8].strip()
            status       = line[49:50].strip()

            # Parse D_TRX_BRCHCODE as integer; skip unparseable rows
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
    weekly_dfs = []
    for wk_path in (
        INPUT_TLBTRAN_WK1,
        INPUT_TLBTRAN_WK2,
        INPUT_TLBTRAN_WK3,
        INPUT_TLBTRAN_WK4,
    ):
        weekly_dfs.append(_read_sas7bdat(wk_path))

    df_all = pl.concat(weekly_dfs, how="diagonal")
    df_all = df_all.filter(pl.col("TRANCODE").is_in([2222, 2223]))
    df_all = df_all.sort(["REPTDATE", "BRANCH"])

    con.register("tlbtran_all", df_all.to_pandas())
    return df_all


def _derive_noacct(df_all: pl.DataFrame) -> pl.DataFrame:
    """Assign NOACCT=1 on the first row of each (REPTDATE, BRANCH) group.

    SAS equivalent:
        DATA ALL;
            SET ALL; BY REPTDATE BRANCH;
            IF FIRST.REPTDATE OR FIRST.BRANCH THEN NOACCT = 1;

    In SAS, NOACCT is not set on non-first rows (it retains the previous value
    or stays missing).  Because the subsequent PROC SUMMARY sums NOACCT, the
    semantically correct equivalent is: set NOACCT=1 for the first row of each
    (REPTDATE, BRANCH) group and 0 elsewhere so the sum equals group count = 1
    per group (which is what FIRST.BRANCH achieves when data is sorted).
    """
    df_all = df_all.with_columns([
        (
            (pl.col("REPTDATE") != pl.col("REPTDATE").shift(1)) |
            (pl.col("BRANCH")   != pl.col("BRANCH").shift(1))
        ).fill_null(True).cast(pl.Int64).alias("NOACCT")
    ])
    return df_all


def _aggregate_to_cit(
    con: duckdb.DuckDBPyConnection,
    df_all: pl.DataFrame,
) -> pl.DataFrame:
    """Aggregate CASHOUT and NOACCT by (REPTDATE, BRANCH), then by BRANCH.

    SAS equivalent:
        PROC SUMMARY DATA=ALL NWAY;
            CLASS REPTDATE BRANCH;
            VAR CASHOUT NOACCT;
            OUTPUT OUT=CIT(DROP=_TYPE_ _FREQ_)
                   SUM=AMOUNT&REPTMON NOACCT&REPTMON;
        DATA CIT; SET CIT; IF AMOUNT&REPTMON = 0 THEN DELETE;

        PROC SUMMARY DATA=CIT NWAY;
            CLASS BRANCH;
            VAR AMOUNT&REPTMON NOACCT&REPTMON;
            OUTPUT OUT=FINAL(DROP=_TYPE_ _FREQ_)
                   SUM=;
    """
    amount_col = f"AMOUNT{REPTMON}"
    noacct_col = f"NOACCT{REPTMON}"

    con.register("all_noacct", df_all.to_pandas())
    cit = con.execute(f"""
        SELECT
            BRANCH,
            SUM(CASHOUT) AS "{amount_col}",
            SUM(NOACCT)  AS "{noacct_col}"
        FROM all_noacct
        GROUP BY BRANCH
        HAVING SUM(CASHOUT) <> 0
    """).df()

    return pl.from_pandas(cit)


def _append_to_cit_year(df_final: pl.DataFrame) -> pl.DataFrame:
    """Replicate the %APPEND macro: initialise year file in January, merge otherwise.

    SAS equivalent:
        %MACRO APPEND;
            %IF "&REPTMON" EQ "01" %THEN %DO;
                DATA CIT.CIT&REPTYEAR;
                    SET FINAL; OUTPUT;
                    IF BRANCH NE . THEN DO;
                        AMOUNT02=0; NOACCT02=0; ... AMOUNT12=0; NOACCT12=0;
                    END;
            %END;
            %ELSE %DO;
                PROC SORT DATA=FINAL; BY BRANCH;
                DATA CIT.CIT&REPTYEAR;
                    MERGE CIT.CIT&REPTYEAR FINAL; BY BRANCH;
            %END;
        %MEND APPEND;
    """
    amount_col = f"AMOUNT{REPTMON}"
    noacct_col = f"NOACCT{REPTMON}"

    if REPTMON == "01":
        # January: initialise the yearly file with zero stubs for months 02-12
        stub_cols = {}
        for m in range(2, 13):
            mm = str(m).zfill(2)
            stub_cols[f"AMOUNT{mm}"] = pl.lit(0.0).cast(pl.Float64)
            stub_cols[f"NOACCT{mm}"] = pl.lit(0).cast(pl.Int64)
        df_cit_year = df_final.with_columns(
            [expr.alias(name) for name, expr in stub_cols.items()]
        )
        df_cit_year.write_parquet(CIT_YEAR_FILE)
    else:
        # Other months: merge into existing yearly accumulation file
        df_existing = pl.read_parquet(CIT_YEAR_FILE)
        # Drop the current-month columns from existing if they already exist
        # (re-running the same month replaces previous values)
        for col in (amount_col, noacct_col):
            if col in df_existing.columns:
                df_existing = df_existing.drop(col)

        df_cit_year = (
            df_existing
            .join(df_final, on="BRANCH", how="outer_coalesce")
        )
        df_cit_year.write_parquet(CIT_YEAR_FILE)

    return pl.read_parquet(CIT_YEAR_FILE).sort("BRANCH")


def _merge_with_branch(
    con: duckdb.DuckDBPyConnection,
    df_cit_year: pl.DataFrame,
    df_branch: pl.DataFrame,
) -> pl.DataFrame:
    """Merge CIT year data with branch reference (INNER on D_TRX_BRCHCODE = BRANCH).

    SAS equivalent:
        DATA CIT;
            MERGE CIT.CIT&REPTYEAR(IN=A) BRANCH(IN=B);
            IF B;
            RETAIN NO 0;
            BY BRANCH;
            SUMAMT = SUM(OF AMOUNT01-AMOUNT12);
            SUMACT = SUM(OF NOACCT01-NOACCT12);
            NO + 1;

    SAS keeps rows where BRANCH exists in BRANCH dataset (IF B).
    BRANCH in CIT maps to D_TRX_BRCHCODE in BRANCH file.
    """
    con.register("cit_year",  df_cit_year.to_pandas())
    con.register("branch_ref", df_branch.to_pandas())

    amount_cols = ", ".join(
        f'COALESCE(c."AMOUNT{str(m).zfill(2)}", 0) AS "AMOUNT{str(m).zfill(2)}"'
        for m in range(1, 13)
    )
    noacct_cols = ", ".join(
        f'COALESCE(c."NOACCT{str(m).zfill(2)}", 0) AS "NOACCT{str(m).zfill(2)}"'
        for m in range(1, 13)
    )

    merged = con.execute(f"""
        SELECT
            b.D_TRX_BRCHCODE AS BRANCH,
            b.D_TRX_BRANCH   AS BRABBR,
            {amount_cols},
            {noacct_cols}
        FROM branch_ref b
        LEFT JOIN cit_year c
            ON c.BRANCH = b.D_TRX_BRCHCODE
        ORDER BY b.D_TRX_BRANCH
    """).df()

    df = pl.from_pandas(merged)

    # Compute SUMAMT and SUMACT (SAS SUM treats NULL as 0 — already COALESCED above)
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
    totals = {}
    for col in sum_cols:
        if col in df_cit_report.columns:
            totals[col] = df_cit_report[col].fill_null(0).sum()
        else:
            totals[col] = 0
    return totals


# ============================================================================
# REPORT FORMATTING
# ============================================================================

def _fmt_amount(val) -> str:
    """Format AMOUNT columns with COMMA20. (no decimals in original SAS format)."""
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


def _write_report(df_cit_report: pl.DataFrame, totals: dict, output_file: Path) -> None:
    """Write the Cash-In-Transit report replicating SAS DATA _NULL_ FILE CITLIST output.

    SAS equivalent (DATA _NULL_ SET CIT):
        FILE CITLIST;
        IF _N_ = 1 THEN DO;
            PUT @1 '1CASH-IN-TRANSIT REPORT' "&REPTYEAR2";
            PUT <header line 1 — column labels>;
            PUT <header line 2 — subheadings>;
        END;
        PUT @002 NO  @006 BRANCH  @013 BRABBR
            @020 AMOUNT01  @038 NOACCT01 ... @344 SUMAMT @362 SUMACT;

    The leading '1' in '1CASH-IN-TRANSIT REPORT' is the ASA form-feed character.
    Column positions follow the SAS @col notation (1-based); Python uses 0-based
    indexing, so each @N maps to string position N-1.

    ASA carriage control:
        '1' at position 0 -> form feed (new page)
        ' ' at position 0 -> single space (normal line advance)

    SAS DATA _NULL_ TOTCIT: appended with FILE CITLIST MOD (append mode).
    """
    output_file.parent.mkdir(parents=True, exist_ok=True)

    MONTH_LABELS = [
        "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
        "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
    ]

    # Column positions (1-based @col from SAS PUT statements):
    # NO=2, BRANCH=6, BRABBR=13, then for each month pair:
    #   AMOUNT_col at positions: 20,47,74,101,128,155,182,209,236,263,290,317
    #   NOACCT_col at positions: 38,65,92,119,146,173,200,227,254,281,308,335
    # SUMAMT=344, SUMACT=362
    # Field widths are derived from COMMA20. (20 chars) and labels.

    AMT_POSITIONS   = [20, 47, 74, 101, 128, 155, 182, 209, 236, 263, 290, 317]
    NOACCT_POSITIONS = [38, 65, 92, 119, 146, 173, 200, 227, 254, 281, 308, 335]
    SUMAMT_POS  = 344
    SUMACT_POS  = 362
    FIELD_WIDTH = 18   # width reserved per numeric value (COMMA20. -> up to 20, padded)

    def _place(buf: list, pos: int, text: str, width: int = 0) -> None:
        """Place text into buf (list of chars) at 0-based position pos."""
        idx = pos - 1  # SAS @pos is 1-based
        s = text if width == 0 else text[:width].rjust(width)
        for i, ch in enumerate(s):
            dest = idx + i
            if dest >= len(buf):
                buf.extend([" "] * (dest - len(buf) + 1))
            buf[dest] = ch

    def _render(buf: list) -> str:
        return "".join(buf)

    with open(output_file, "w", encoding="utf-8") as f:

        # ── PAGE TITLE (ASA '1' = form feed / new page) ──────────────────
        # SAS: PUT @1 '1CASH-IN-TRANSIT REPORT' "&REPTYEAR2";
        # The '1' is the ASA carriage-control character at column 1.
        title_buf = list(" " * 400)
        _place(title_buf, 1, f"1CASH-IN-TRANSIT REPORT {REPTYEAR2}")
        f.write(_render(title_buf).rstrip() + "\n")

        # ── HEADER LINE 1 ─────────────────────────────────────────────────
        # SAS: PUT @002 <empty> @006 <empty> @013 <empty>
        #          @020 'JAN'  @038 'NO OF'  @047 'FEB' ... @362 'TOTAL NO OF'
        h1_buf = list(" " * 400)
        _place(h1_buf, 2,   "NO")
        _place(h1_buf, 6,   "BRANCH")
        _place(h1_buf, 13,  "BRANCH")
        for i, (amt_pos, noacct_pos, label) in enumerate(
            zip(AMT_POSITIONS, NOACCT_POSITIONS, MONTH_LABELS)
        ):
            _place(h1_buf, amt_pos,    label)
            _place(h1_buf, noacct_pos, "NO OF")
        _place(h1_buf, SUMAMT_POS,  "TOTAL")
        _place(h1_buf, SUMACT_POS,  "TOTAL NO OF")
        f.write(_render(h1_buf).rstrip() + "\n")

        # ── HEADER LINE 2 ─────────────────────────────────────────────────
        # SAS: PUT @006 'CODE' @038 'TRANSIT' @065 'TRANSIT' ... @362 'TRANSIT'
        h2_buf = list(" " * 400)
        _place(h2_buf, 6,  "CODE")
        for noacct_pos in NOACCT_POSITIONS:
            _place(h2_buf, noacct_pos, "TRANSIT")
        _place(h2_buf, SUMACT_POS, "TRANSIT")
        f.write(_render(h2_buf).rstrip() + "\n")

        # ── DATA ROWS (DATA _NULL_ SET CIT) ───────────────────────────────
        for row in df_cit_report.iter_rows(named=True):
            row_buf = list(" " * 400)
            _place(row_buf, 2,  str(int(row["NO"])))
            _place(row_buf, 6,  str(int(row["BRANCH"])) if row["BRANCH"] is not None else "")
            _place(row_buf, 13, str(row["BRABBR"] or ""))

            for i, (amt_pos, noacct_pos) in enumerate(
                zip(AMT_POSITIONS, NOACCT_POSITIONS)
            ):
                mm  = str(i + 1).zfill(2)
                amt = _fmt_amount(row.get(f"AMOUNT{mm}", 0))
                noa = _fmt_noacct(row.get(f"NOACCT{mm}", 0))
                _place(row_buf, amt_pos,    amt)
                _place(row_buf, noacct_pos, noa)

            _place(row_buf, SUMAMT_POS, _fmt_amount(row.get("SUMAMT", 0)))
            _place(row_buf, SUMACT_POS, _fmt_noacct(row.get("SUMACT", 0)))
            f.write(_render(row_buf).rstrip() + "\n")

        # ── TOTAL ROW (DATA _NULL_ SET TOTCIT — FILE CITLIST MOD) ─────────
        # SAS: PUT @013 'TOTAL' @020 AMOUNT01 ... @362 SUMACT;
        tot_buf = list(" " * 400)
        _place(tot_buf, 13, "TOTAL")

        for i, (amt_pos, noacct_pos) in enumerate(
            zip(AMT_POSITIONS, NOACCT_POSITIONS)
        ):
            mm  = str(i + 1).zfill(2)
            amt = _fmt_amount(totals.get(f"AMOUNT{mm}", 0))
            noa = _fmt_noacct(totals.get(f"NOACCT{mm}", 0))
            _place(tot_buf, amt_pos,    amt)
            _place(tot_buf, noacct_pos, noa)

        _place(tot_buf, SUMAMT_POS, _fmt_amount(totals.get("SUMAMT", 0)))
        _place(tot_buf, SUMACT_POS, _fmt_noacct(totals.get("SUMACT", 0)))
        f.write(_render(tot_buf).rstrip() + "\n")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

print("=" * 70)
print("CASH-IN-TRANSIT MONTHLY ACCUMULATED REPORT")
print("=" * 70)
print(f"\nReport Date   : {REPTDATE.strftime('%d/%m/%Y')}")
print(f"Report Month  : {REPTMON}")
print(f"Report Year   : {REPTYEAR2}")

con = duckdb.connect(database=":memory:")

try:
    print("\nStep 1: Reading branch reference file (DBRANCH.txt)...")
    df_branch = _read_branch_file(INPUT_BRHFILE)
    print(f"Branches loaded: {len(df_branch):,}")

    print("\nStep 2: Loading and filtering TLBTRAN weekly files...")
    df_all = _load_tlbtran_all(con)
    print(f"Transactions after TRANCODE filter: {len(df_all):,}")

    print("\nStep 3: Deriving NOACCT first-row flags per (REPTDATE, BRANCH)...")
    df_all = _derive_noacct(df_all)

    print("\nStep 4: Aggregating CASHOUT and NOACCT to FINAL (by BRANCH)...")
    df_final = _aggregate_to_cit(con, df_all)
    print(f"Branches with non-zero CASHOUT: {len(df_final):,}")

    print(f"\nStep 5: Appending to yearly CIT file [{CIT_YEAR_FILE.name}]...")
    df_cit_year = _append_to_cit_year(df_final)
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
