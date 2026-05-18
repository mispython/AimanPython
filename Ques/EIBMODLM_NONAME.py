#!/usr/bin/env python3
"""
Program : EIBMODLM_NONAME.py
Purpose : Report on Accounts with Overdraft Limits (No NAME column variant)
          Generates two reports:
            1. Public Bank Berhad - Accounts with OD Limits (ODPLAN 100-105)
            2. Public Islamic Bank Berhad - Accounts with CLF-i Limits (ODPLAN 106)
"""

import duckdb
import polars as pl
import pandas as pd
from pathlib import Path

from REPTDATE import get_reptdate_values

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

INPUT_DIR  = BASE_DIR / "input" / "uat"
OUTPUT_DIR = BASE_DIR / "output" / "EIBMODLM_NONAME"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input paths - Public Bank
INPUT_PBB_CURRENT = INPUT_DIR / "ca05226.sas7bdat"
INPUT_PBB_OVERDFT = INPUT_DIR / "lm05226.sas7bdat"

# Input paths - Islamic Bank
INPUT_PIBB_CURRENT = INPUT_DIR / "ica05226.sas7bdat"
INPUT_PIBB_OVERDFT = INPUT_DIR / "lm05226.sas7bdat"

# Output paths
OUTPUT_PBB_REPORT  = OUTPUT_DIR / "PBB_ODLIMIT_REPORT.txt"
OUTPUT_PIBB_REPORT = OUTPUT_DIR / "PIBB_ODLIMIT_REPORT.txt"

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

    print(f"\nDEBUG COLUMN NAMES [{path.name}]:")
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


def _clear_registered_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Drop all stale registered tables from a previous run to avoid
    cross-contamination between the PBB and PIBB report passes."""
    for table in ('current_raw', 'ovdr_raw', 'current', 'ovdr', 'odmerg', 'ovdrm'):
        try:
            con.execute(f"DROP VIEW IF EXISTS {table}")
            con.execute(f"DROP TABLE IF EXISTS {table}")
        except Exception:
            pass


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
    """Load and filter overdraft data from .sas7bdat."""
    ovdr_df = _read_sas7bdat(overdft_file)
    con.register('ovdr_raw', ovdr_df.to_pandas())
    ovdr = con.execute("""
        SELECT
            ACCTNO,
            BRANCH,
            LMTBASER,
            LMTRATE,
            LMTAMT,
            LMTCOLL,
            APPRLIMT,
            ODSTATUS
        FROM ovdr_raw
        WHERE APPRLIMT > 1
          AND LMTTYPE IN ('Y', 'A')
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
            MAX(BRANCH) AS BRANCH,
            MAX(LMTBASER) AS LMTBASER,
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
            o.ODSTATUS,
            o.APPRLIMT,
            COALESCE(o.LIMIT1, 0) AS LIMIT1,
            COALESCE(o.RATE1, 0.0) AS RATE1,
            o.COLL1,
            COALESCE(o.LIMIT2, 0) AS LIMIT2,
            COALESCE(o.RATE2, 0.0) AS RATE2,
            o.COLL2,
            COALESCE(o.LIMIT3, 0) AS LIMIT3,
            COALESCE(o.RATE3, 0.0) AS RATE3,
            o.COLL3,
            COALESCE(o.LIMIT4, 0) AS LIMIT4,
            COALESCE(o.RATE4, 0.0) AS RATE4,
            o.COLL4,
            COALESCE(o.LIMIT5, 0) AS LIMIT5,
            COALESCE(o.RATE5, 0.0) AS RATE5,
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
                WHEN BRANCH < 10 THEN '00' || CAST(BRANCH AS VARCHAR)
                WHEN BRANCH < 100 THEN '0' || CAST(BRANCH AS VARCHAR)
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
    report_file.write(' \n')
    report_file.write(' ' + ' ' * 25 + '-' * 49 + '\n')
    report_file.write(' ' + ' ' * 25 + f"TOTAL APPROVED LIMITS  = {branch_total_limit:>20,.2f}\n")
    report_file.write(' \n')
    report_file.write(' ' + ' ' * 25 + f"TOTAL ACCOUNTS         = {branch_account_count:>6}\n")
    report_file.write(' \n')
    report_file.write(' ' + ' ' * 25 + f"TOTAL OPERATIVE LIMITS = {branch_total_operative:>20,.2f}\n")
    report_file.write(' ' + ' ' * 25 + '-' * 49 + '\n')
    report_file.write(' \n')


def _write_branch_header(
    report_file,
    title1: str,
    title2: str,
    report_date: str,
    od_label: str,
) -> None:
    # ASA carriage control: '1' = new page
    report_file.write('1')
    report_file.write(f"  {title1}\n")
    report_file.write(f"   {title2}\n")
    report_file.write(f"   REPORT AS AT {report_date}\n")
    report_file.write('   \n')
    report_file.write(
        f" BRN ACCOUNT NO BASE {od_label:>5} OUSTANDING      "
        "APPROVED        LIMIT1      RATE1 COLL1    LIMIT2      RATE2 COLL2    LIMIT3      RATE3 COLL3\n"
    )
    report_file.write(
        "                RATE ST   BALANCE          "
        "LIMIT                                                                     \n"
    )
    report_file.write(' ' + '-' * 107 + '\n')


def _build_detail_line(row) -> str:
    line = ' '
    line += f"{_safe_text(row['BRN'], 3):<3} "
    line += f"{_safe_int(row['ACCTNO']):<10} "
    line += f"{_safe_float(row['LMTBASER']):>5.2f} "
    line += f"{_safe_text(row['ODSTATUS'], 5):<5} "
    line += f"{_safe_float(row['BALANCE']):>12,.2f} "
    line += f"{_safe_text(row['CRI'], 2):<2} "
    line += f"{_safe_float(row['APPRLIMT']):>12,.2f} "
    line += f"{_safe_float(row['LIMIT1']):>11,.2f} "
    line += f"{_safe_float(row['RATE1']):>5.2f} "
    line += f"{_safe_text(row['COLL1'], 5):<5}\n"
    return line


def _build_limit2_line(row) -> str:
    if not row['LIMIT2'] or row['LIMIT2'] <= 0:
        return ''
    limit2 = _safe_float(row['LIMIT2'])
    rate2  = _safe_float(row['RATE2'])
    coll2  = _safe_text(row['COLL2'], 5)
    return ' ' + ' ' * 80 + f"{limit2:>11,.2f} {rate2:>5.2f} {coll2:<5}\n"


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
    Generate overdraft limit report (no NAME column variant).

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

    # Create a fresh DuckDB connection per report run to avoid stale
    # registered tables from the previous run contaminating this one.
    con = duckdb.connect(database=':memory:')

    try:
        print("\nStep 1: Processing current accounts...")
        current = _load_current_accounts(con, current_file, odplan_filter)
        print(f"Current accounts: {len(current):,}")

        print("\nStep 2: Processing overdraft data...")
        ovdr = _load_overdraft_data(con, overdft_file)
        print(f"Overdraft records: {len(ovdr):,}")

        print("\nStep 3: Pivoting limits (up to 5 per account)...")
        odmerg = _pivot_overdraft_limits(con)
        print(f"Accounts with pivoted limits: {len(odmerg):,}")

        print("\nStep 4: Merging current accounts with overdraft data...")
        ovdrm = _merge_current_with_overdraft(con)
        print(f"Merged records: {len(ovdrm):,}")

        print("\nStep 5: Formatting branch codes...")
        brnref = _format_branch_codes(con)
        print(f"Final records with branch codes: {len(brnref):,}")

        print("\nStep 6: Generating report...")
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
        print(f"\n[ERROR] Report generation failed: {e}")
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
