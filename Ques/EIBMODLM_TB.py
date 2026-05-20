#!/usr/bin/env python3
"""
Program : EIBMODLM.py
Purpose : Report on Accounts with Overdraft Limits
          Generates two reports:
            1. Public Bank Berhad - Accounts with OD Limits (ODPLAN 100-105)
            2. Public Islamic Bank Berhad - Accounts with CLF-i Limits (ODPLAN 106)
          NAME column is resolved by joining lm{month}{week}{year}.sas7bdat ACCTNO
          against cisr1ca{month}{week}{year}.sas7bdat ACCTNO and taking CUSTNAME as NAME.
          Accounts with no matching CUSTNAME will show a blank NAME field.
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
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

INPUT_DIR  = BASE_DIR / "input" / "prod"
OUTPUT_DIR = BASE_DIR / "output" / "EIBMODLM"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input paths - Public Bank
INPUT_PBB_CURRENT  = get_latest_file(INPUT_DIR, "ca")       # File name example - ca05226.sas7bdat
INPUT_PBB_OVERDFT  = get_latest_file(INPUT_DIR, "lm")       # File name example - lm05226.sas7bdat

# Input paths - Islamic Bank
INPUT_PIBB_CURRENT  = get_latest_file(INPUT_DIR, "ica")     # File name example - ica05226.sas7bdat
INPUT_PIBB_OVERDFT  = get_latest_file(INPUT_DIR, "ilm")     # File name example - ilm05226.sas7bdat

# Shared customer name lookup file (ACCTNO -> CUSTNAME mapped as NAME)
INPUT_CUSTNAME     = get_latest_file(INPUT_DIR, "cisr1ca")  # File name example - cisr1ca05226.sas7bdat

# Output paths
OUTPUT_PBB_REPORT  = build_output_file(OUTPUT_DIR, "PBB_ODLIMIT_REPORT").with_suffix(".txt")
OUTPUT_PIBB_REPORT = build_output_file(OUTPUT_DIR, "PIBB_ODLIMIT_REPORT").with_suffix(".txt")
# Output example: OUTPUT_PBB_REPORT -> PBB_ODLIMIT_REPORT_180526.txt
# Output example: OUTPUT_PIBB_REPORT -> PIBB_ODLIMIT_REPORT_180526.txt

# Report configuration
PAGE_SIZE = 50  # PS=50 in OPTIONS


# ============================================================================
# REPORT DATE (from REPTDATE module - no reptdate.parquet file is read)
# ============================================================================
reptdate_values = get_reptdate_values()
REPTDATE    = reptdate_values.reptdate
REPTYEAR    = reptdate_values.reptyear
REPTMON     = reptdate_values.reptmon
REPTDAY     = reptdate_values.reptday
NOWK        = reptdate_values.nowk
REPORT_DATE = REPTDATE.strftime('%d/%m/%y')

# ============================================================================
# INPUT FILE EXISTENCE CHECK — fail fast before any processing
# ============================================================================
_REQUIRED_INPUTS = {
    "PBB  Current Accounts" : INPUT_PBB_CURRENT,
    "PBB  Overdraft Data"   : INPUT_PBB_OVERDFT,
    "PIBB Current Accounts" : INPUT_PIBB_CURRENT,
    "PIBB Overdraft Data"   : INPUT_PIBB_OVERDFT,
    "Customer Name Lookup"  : INPUT_CUSTNAME,
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
    pandas_df = pd.read_sas(
        path,
        format="sas7bdat",
        encoding="latin1",
    )

    pandas_df.columns = [
        str(col).upper().strip()
        for col in pandas_df.columns
    ]

    print(f"\nDEBUG COLUMN NAMES [{path.name}]:")
    # print(pandas_df.columns.tolist())
    print(pandas_df.head(10))

    return pl.from_pandas(pandas_df)


def _build_odplan_condition(odplan_filter) -> str:
    if isinstance(odplan_filter, list):
        return f"ODPLAN IN ({','.join(map(str, odplan_filter))})"
    return f"ODPLAN = {odplan_filter}"


def _safe_float(value) -> float:
    return float(value) if value is not None else 0.0


def _safe_int(value) -> int:
    return int(value) if value is not None else 0


def _safe_text(value, length) -> str:
    return str(value)[:length] if value is not None else ''


def _get_report_titles(is_islamic) -> tuple:
    if is_islamic:
        return (
            'P U B L I C   I S L A M I C  B A N K   B E R H A D',
            'REPORT TITLE: ACCOUNTS WITH CLF-i LIMITS',
            'CLF-i',
        )
    return (
        'P U B L I C   B A N K   B E R H A D',
        'REPORT TITLE: ACCOUNTS WITH OD LIMITS',
        'OD',
    )


def _load_custname_lookup(con: duckdb.DuckDBPyConnection) -> None:
    """Load cisr1ca05226.sas7bdat and register ACCTNO -> CUSTNAME as NAME
    into DuckDB as the 'custname_lookup' table.

    Join logic:
        if ACCTNO in lm05226 == ACCTNO in cisr1ca05226,
        then CUSTNAME from cisr1ca05226 is used as NAME.
    Unmatched overdraft accounts will carry a NULL / blank NAME.
    """
    custname_df = _read_sas7bdat(INPUT_CUSTNAME)

    # Ensure only the columns we need are kept; rename CUSTNAME -> NAME
    required = {"ACCTNO", "CUSTNAME"}
    missing  = required - set(custname_df.columns)
    if missing:
        raise ValueError(
            f"{INPUT_CUSTNAME.name} is missing required column(s): {', '.join(sorted(missing))}"
        )

    lookup_pd = (
        custname_df
        .select(["ACCTNO", "CUSTNAME"])
        .rename({"CUSTNAME": "NAME"})
        .to_pandas()
    )
    con.register('custname_lookup', lookup_pd)


def _load_current_accounts(
    con: duckdb.DuckDBPyConnection,
    current_file: Path,
    odplan_filter,
) -> pd.DataFrame:
    """Load and filter current accounts from .sas7bdat."""
    current_df = _read_sas7bdat(current_file)
    con.register('current_raw', current_df.to_pandas())
    odplan_condition = _build_odplan_condition(odplan_filter)
    current = con.execute(f"""
        SELECT
            ACCTNO,
            CASE
                WHEN CURBAL < 0 THEN (-1) * CURBAL
                ELSE CURBAL
            END AS BALANCE,
            CASE
                WHEN CURBAL >= 0 THEN 'CR'
                ELSE NULL
            END AS CRI
        FROM current_raw
        WHERE DEPTYPE IN ('D', 'N')
          AND APPRLIMT > 1
          AND {odplan_condition}
    """).df()
    con.register('current', current)
    return current


def _load_overdraft_data(
    con: duckdb.DuckDBPyConnection,
    overdft_file: Path,
) -> pd.DataFrame:
    """Load and filter overdraft data from .sas7bdat, then enrich with NAME
    by left-joining against the custname_lookup table on ACCTNO.

    Accounts whose ACCTNO does not appear in cisr1ca05226.sas7bdat will
    have NAME set to an empty string.
    """
    ovdr_df = _read_sas7bdat(overdft_file)
    con.register('ovdr_raw', ovdr_df.to_pandas())
    ovdr = con.execute("""
        SELECT
            o.ACCTNO,
            o.BRANCH,
            o.LMTBASER,
            o.LMTRATE,
            o.LMTAMT,
            o.LMTCOLL,
            o.APPRLIMT,
            o.ODSTATUS,
            COALESCE(c.NAME, '') AS NAME
        FROM ovdr_raw o
        LEFT JOIN custname_lookup c
            ON o.ACCTNO = c.ACCTNO
        WHERE o.APPRLIMT > 1
          AND o.LMTTYPE IN ('Y', 'A')
    """).df()
    con.register('ovdr', ovdr)
    return ovdr


def _pivot_overdraft_limits(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    odmerg = con.execute("""
        WITH ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY ACCTNO ORDER BY LMTAMT DESC) AS RCNT
            FROM ovdr
        ),
        limited AS (
            SELECT * FROM ranked WHERE RCNT <= 5
        )
        SELECT
            ACCTNO,
            MAX(BRANCH)   AS BRANCH,
            MAX(LMTBASER) AS LMTBASER,
            MAX(NAME)     AS NAME,
            MAX(ODSTATUS) AS ODSTATUS,
            MAX(APPRLIMT) AS APPRLIMT,
            MAX(CASE WHEN RCNT = 1 THEN LMTAMT END) AS LIMIT1,
            MAX(CASE WHEN RCNT = 1 THEN LMTRATE END) AS RATE1,
            MAX(CASE WHEN RCNT = 1 THEN LMTCOLL END) AS COLL1,
            MAX(CASE WHEN RCNT = 2 THEN LMTAMT END) AS LIMIT2,
            MAX(CASE WHEN RCNT = 2 THEN LMTRATE END) AS RATE2,
            MAX(CASE WHEN RCNT = 2 THEN LMTCOLL END) AS COLL2,
            MAX(CASE WHEN RCNT = 3 THEN LMTAMT END) AS LIMIT3,
            MAX(CASE WHEN RCNT = 3 THEN LMTRATE END) AS RATE3,
            MAX(CASE WHEN RCNT = 3 THEN LMTCOLL END) AS COLL3,
            MAX(CASE WHEN RCNT = 4 THEN LMTAMT END) AS LIMIT4,
            MAX(CASE WHEN RCNT = 4 THEN LMTRATE END) AS RATE4,
            MAX(CASE WHEN RCNT = 4 THEN LMTCOLL END) AS COLL4,
            MAX(CASE WHEN RCNT = 5 THEN LMTAMT END) AS LIMIT5,
            MAX(CASE WHEN RCNT = 5 THEN LMTRATE END) AS RATE5,
            MAX(CASE WHEN RCNT = 5 THEN LMTCOLL END) AS COLL5
        FROM limited
        GROUP BY ACCTNO
    """).df()
    con.register('odmerg', odmerg)
    return odmerg


def _merge_current_with_overdraft(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    ovdrm = con.execute("""
        SELECT
            c.ACCTNO,
            c.BALANCE,
            c.CRI,
            o.BRANCH,
            o.LMTBASER,
            o.NAME,
            o.ODSTATUS,
            o.APPRLIMT,
            COALESCE(o.LIMIT1, 0)   AS LIMIT1,
            COALESCE(o.RATE1, 0.0)  AS RATE1,
            o.COLL1,
            COALESCE(o.LIMIT2, 0)   AS LIMIT2,
            COALESCE(o.RATE2, 0.0)  AS RATE2,
            o.COLL2,
            COALESCE(o.LIMIT3, 0)   AS LIMIT3,
            COALESCE(o.RATE3, 0.0)  AS RATE3,
            o.COLL3,
            COALESCE(o.LIMIT4, 0)   AS LIMIT4,
            COALESCE(o.RATE4, 0.0)  AS RATE4,
            o.COLL4,
            COALESCE(o.LIMIT5, 0)   AS LIMIT5,
            COALESCE(o.RATE5, 0.0)  AS RATE5,
            o.COLL5,
            (COALESCE(o.LIMIT1, 0) + COALESCE(o.LIMIT2, 0) +
             COALESCE(o.LIMIT3, 0) + COALESCE(o.LIMIT4, 0) +
             COALESCE(o.LIMIT5, 0)) AS LIMITS,
            1 AS NOACCT
        FROM current c
        INNER JOIN odmerg o ON c.ACCTNO = o.ACCTNO
    """).df()
    con.register('ovdrm', ovdrm)
    return ovdrm


def _format_branch_codes(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute("""
        SELECT *,
            CASE
                WHEN BRANCH < 10  THEN '00' || CAST(BRANCH AS VARCHAR)
                WHEN BRANCH < 100 THEN '0'  || CAST(BRANCH AS VARCHAR)
                ELSE CAST(BRANCH AS VARCHAR)
            END AS BRN
        FROM ovdrm
        ORDER BY BRN, ACCTNO
    """).df()


def _write_branch_subtotal(
    report_file,
    branch_total_limit: float,
    branch_account_count: int,
    branch_total_operative: float,
) -> None:

    label_width = 35
    value_width = 20
    total_width = 170

    report_file.write("\n")

    report_file.write(" " * (total_width - 55) + "-" * 55 + "\n")

    report_file.write(
        f"{'TOTAL APPROVED LIMITS =':>{label_width}} "
        f"{branch_total_limit:>{value_width},.2f}\n"
    )

    report_file.write(
        f"{'TOTAL ACCOUNTS =':>{label_width}} "
        f"{branch_account_count:>{value_width},}\n"
    )

    report_file.write(
        f"{'TOTAL OPERATIVE LIMITS =':>{label_width}} "
        f"{branch_total_operative:>{value_width},.2f}\n"
    )

    report_file.write(" " * (total_width - 55) + "-" * 55 + "\n\n")


def _write_branch_header(
    report_file,
    title1: str,
    title2: str,
    report_date: str,
    od_label: str,
) -> None:

    line_width = 170

    report_file.write(f" {title1}\n")
    report_file.write(f" {title2}\n")
    report_file.write(f" REPORT AS AT {report_date}\n")
    report_file.write("\n")

    # HEADER LINE 1
    report_file.write(
        f"{'BRN':<4}"
        f"{'ACCOUNT NO':<12}"
        f"{'NAME OF CUSTOMER':<27}"
        f"{'BASE':>6}"
        f"{od_label:>5}"
        f"{'OUTSTANDING':>15}"
        f"{'APPROVED':>15}"
        f"{'LIMIT1':>15}"
        f"{'RATE1':>8}"
        f"{'COLL1':>8}"
        f"{'LIMIT2':>15}"
        f"{'RATE2':>8}"
        f"{'COLL2':>8}\n"
    )

    # HEADER LINE 2
    report_file.write(
        f"{'':<49}"
        f"{'RATE':>6}"
        f"{'ST':>5}"
        f"{'BALANCE':>15}"
        f"{'LIMIT':>15}\n"
    )

    report_file.write("-" * line_width + "\n")


def _build_detail_line(row) -> str:

    return (
        f"{_safe_text(row['BRN'], 3):<4}"
        f"{_safe_int(row['ACCTNO']):<12}"
        f"{_safe_text(row['NAME'], 25):<27}"
        f"{_safe_float(row['LMTBASER']):>6.2f}"
        f"{_safe_text(row['ODSTATUS'], 2):>5}"
        f"{_safe_float(row['BALANCE']):>15,.2f}"
        f"{_safe_float(row['APPRLIMT']):>15,.2f}"
        f"{_safe_float(row['LIMIT1']):>15,.2f}"
        f"{_safe_float(row['RATE1']):>8.2f}"
        f"{_safe_text(row['COLL1'], 5):>8}\n"
    )


def _build_limit2_line(row) -> str:

    if not row['LIMIT2'] or row['LIMIT2'] <= 0:
        return ''

    return (
        f"{'':<92}"
        f"{_safe_float(row['LIMIT2']):>15,.2f}"
        f"{_safe_float(row['RATE2']):>8.2f}"
        f"{_safe_text(row['COLL2'], 5):>8}\n"
    )


def _write_report_file(
    brnref: pd.DataFrame,
    output_file: Path,
    is_islamic: bool,
    report_date: str,
) -> None:
    title1, title2, od_label = _get_report_titles(is_islamic)
    current_brn = None
    branch_totals = {"approved": 0.0, "operative": 0.0, "accounts": 0}

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w', encoding='utf-8') as report_file:
        for _, row in brnref.iterrows():
            branch_changed = row['BRN'] != current_brn
            if branch_changed:
                if current_brn is not None:
                    _write_branch_subtotal(
                        report_file,
                        branch_totals["approved"],
                        branch_totals["accounts"],
                        branch_totals["operative"],
                    )
                current_brn = row['BRN']
                branch_totals = {"approved": 0.0, "operative": 0.0, "accounts": 0}
                _write_branch_header(report_file, title1, title2, report_date, od_label)

            report_file.write(_build_detail_line(row))
            extra_line = _build_limit2_line(row)
            if extra_line:
                report_file.write(extra_line)

            branch_totals["approved"]  += _safe_float(row['APPRLIMT'])
            branch_totals["operative"] += _safe_float(row['LIMITS'])
            branch_totals["accounts"]  += 1

        if current_brn is not None:
            _write_branch_subtotal(
                report_file,
                branch_totals["approved"],
                branch_totals["accounts"],
                branch_totals["operative"],
            )


def generate_od_report(
    current_file: Path,
    overdft_file: Path,
    output_file: Path,
    is_islamic: bool = False,
    odplan_filter=None,
) -> bool:
    """
    Generate overdraft limit report with NAME resolved from cisr1ca05226.sas7bdat.

    NAME resolution logic:
        if ACCTNO in lm05226.sas7bdat == ACCTNO in cisr1ca05226.sas7bdat
        then NAME = CUSTNAME from cisr1ca05226.sas7bdat
        else NAME = '' (blank)

    Args:
        current_file:   Path to current accounts .sas7bdat file
        overdft_file:   Path to overdraft .sas7bdat file
        output_file:    Path to output report .txt file
        is_islamic:     Boolean indicating if this is Islamic bank report
        odplan_filter:  List of ODPLAN codes or single value

    Returns:
        True if the report was generated successfully, False otherwise.
    """
    print(f"\n{'=' * 70}")
    print(f"Generating {'Islamic Bank CLF-i' if is_islamic else 'Public Bank OD'} Limits Report")
    print(f"{'=' * 70}")
    print(f"\nReport Date: {REPORT_DATE}")

    # A fresh in-memory DuckDB connection is created for every report run
    # so that registered tables from the previous run cannot bleed through.
    con = duckdb.connect(database=':memory:')

    try:
        print("\nStep 1: Loading customer name lookup (cisr1ca05226)...")
        _load_custname_lookup(con)
        print("Customer name lookup registered.")

        print("\nStep 2: Processing current accounts...")
        current = _load_current_accounts(con, current_file, odplan_filter)
        print(f"Current accounts: {len(current):,}")

        print("\nStep 3: Processing overdraft data (with NAME join)...")
        ovdr = _load_overdraft_data(con, overdft_file)
        print(f"Overdraft records: {len(ovdr):,}")
        matched = (ovdr['NAME'] != '').sum()
        print(f"  NAME matched from cisr1ca05226 : {matched:,} / {len(ovdr):,}")

        print("\nStep 4: Pivoting limits (up to 5 per account)...")
        odmerg = _pivot_overdraft_limits(con)
        print(f"Accounts with pivoted limits: {len(odmerg):,}")

        print("\nStep 5: Merging current accounts with overdraft data...")
        ovdrm = _merge_current_with_overdraft(con)
        print(f"Merged records: {len(ovdrm):,}")

        print("\nStep 6: Formatting branch codes...")
        brnref = _format_branch_codes(con)
        print(f"Final records with branch codes: {len(brnref):,}")

        print("\nStep 7: Generating report...")
        _write_report_file(brnref, output_file, is_islamic, REPORT_DATE)
        print(f"Report saved: {output_file}")

        print("\nReport Statistics:")
        print(f"  Total Accounts : {len(brnref):,}")
        print(f"  Total Branches : {brnref['BRN'].nunique()}")
        if len(brnref) > 0:
            print(f"  Total Approved Limits  : {brnref['APPRLIMT'].sum():,.2f}")
            print(f"  Total Operative Limits : {brnref['LIMITS'].sum():,.2f}")

        print(f"\n========== PREVIEW: {output_file.name} ==========\n")
        with open(output_file, 'r', encoding='utf-8') as f:
            print(f.read())
        print(f"========== END PREVIEW ==========\n")

        return True

    except Exception as e:
        print(f"\n[ERROR] Report generation failed for {output_file.name}: {type(e).__name__}: {e}")
        return False

    finally:
        con.close()


# ============================================================================
# MAIN EXECUTION
# ============================================================================

print("=" * 70)
print("OVERDRAFT LIMITS REPORT GENERATION")
print("=" * 70)

results = {}

# ============================================================================
# PART 1: PUBLIC BANK - OD LIMITS (ODPLAN 100-105)
# ============================================================================

results["PBB"] = generate_od_report(
    current_file=INPUT_PBB_CURRENT,
    overdft_file=INPUT_PBB_OVERDFT,
    output_file=OUTPUT_PBB_REPORT,
    is_islamic=False,
    odplan_filter=[100, 101, 102, 103, 104, 105],
)

# ============================================================================
# PART 2: PUBLIC ISLAMIC BANK - CLF-i LIMITS (ODPLAN 106)
# ============================================================================

results["PIBB"] = generate_od_report(
    current_file=INPUT_PIBB_CURRENT,
    overdft_file=INPUT_PIBB_OVERDFT,
    output_file=OUTPUT_PIBB_REPORT,
    is_islamic=True,
    odplan_filter=106,
)

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 70)
print("GENERATED REPORTS:")
print("=" * 70)

if results["PBB"]:
    print(f"  1. Public Bank OD Limits     : {OUTPUT_PBB_REPORT}")
else:
    print(f"  1. Public Bank OD Limits     : [FAILED]")

if results["PIBB"]:
    print(f"  2. Islamic Bank CLF-i Limits : {OUTPUT_PIBB_REPORT}")
else:
    print(f"  2. Islamic Bank CLF-i Limits : [FAILED]")

if all(results.values()):
    print("\nREPORT GENERATION COMPLETE")
else:
    print("\nREPORT GENERATION COMPLETED WITH ERRORS — review output above.")
