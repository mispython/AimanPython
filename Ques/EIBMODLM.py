#!/usr/bin/env python3
"""
Program : EIBMODLM.py
Purpose : Report on Accounts with Overdraft Limits
          Generates two reports:
            1. Public Bank Berhad - Accounts with OD Limits (ODPLAN 100-105)
            2. Public Islamic Bank Berhad - Accounts with CLF-i Limits (ODPLAN 106)
"""

import duckdb
import polars as pl
from pathlib import Path

from REPTDATE import get_reptdate_values

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

# Input paths - Public Bank
INPUT_PBB_CURRENT  = BASE_DIR / "input/ca05226.sas7bdat"
INPUT_PBB_OVERDFT  = BASE_DIR / "input/lm05226.sas7bdat"

# Input paths - Islamic Bank
INPUT_PIBB_CURRENT  = BASE_DIR / "input/ica05226.sas7bdat"
INPUT_PIBB_OVERDFT  = BASE_DIR / "input/lm05226.sas7bdat"

# Output paths
OUTPUT_PBB_REPORT  = BASE_DIR / "output/PBB_ODLIMIT_REPORT.txt"
OUTPUT_PIBB_REPORT = BASE_DIR / "output/PIBB_ODLIMIT_REPORT.txt"

# Report configuration
PAGE_SIZE = 50  # PS=50 in OPTIONS


# ============================================================================
# REPORT DATE (from REPTDATE module - no reptdate.parquet file is read)
# ============================================================================
reptdate_values = get_reptdate_values()
REPORT_DATE = reptdate_values.reptdate.strftime('%d/%m/%y')


# ============================================================================
# INITIALIZE DUCKDB CONNECTION
# ============================================================================
con = duckdb.connect(database=':memory:')


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _build_odplan_condition(odplan_filter):
    if isinstance(odplan_filter, list):
        return f"ODPLAN IN ({','.join(map(str, odplan_filter))})"
    return f"ODPLAN = {odplan_filter}"


def _safe_float(value):
    return float(value) if value is not None else 0.0


def _safe_int(value):
    return int(value) if value is not None else 0


def _safe_text(value, length):
    return str(value)[:length] if value is not None else ''


def _get_report_titles(is_islamic):
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


def _load_sas_table(filepath, table_name):
    """Load a .sas7bdat file into DuckDB via Polars."""
    try:
        import pyreadstat
        df_pd, meta = pyreadstat.read_sas7bdat(str(filepath))
        df_pl = pl.from_pandas(df_pd)
    except ImportError:
        raise ImportError("pyreadstat is required to read .sas7bdat files.")
    con.register(table_name, df_pl.to_pandas())
    return df_pl


def _load_current_accounts(current_file, odplan_filter):
    _load_sas_table(current_file, 'current_raw')
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


def _load_overdraft_data(overdft_file):
    _load_sas_table(overdft_file, 'ovdr_raw')
    ovdr = con.execute("""
        SELECT
            ACCTNO,
            BRANCH,
            LMTBASER,
            LMTRATE,
            LMTAMT,
            LMTCOLL,
            NAME,
            APPRLIMT,
            ODSTATUS
        FROM ovdr_raw
        WHERE APPRLIMT > 1
          AND LMTTYPE IN ('Y', 'A')
    """).df()
    con.register('ovdr', ovdr)
    return ovdr


def _pivot_overdraft_limits():
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
            MAX(NAME) AS NAME,
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


def _merge_current_with_overdraft():
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


def _format_branch_codes():
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


def _write_branch_subtotal(report_file, branch_total_limit, branch_account_count, branch_total_operative):
    report_file.write(' \n')
    report_file.write(' ' + ' ' * 25 + '-' * 49 + '\n')
    report_file.write(' ' + ' ' * 25 + f"TOTAL APPROVED LIMITS  = {branch_total_limit:>20,.2f}\n")
    report_file.write(' \n')
    report_file.write(' ' + ' ' * 25 + f"TOTAL ACCOUNTS         = {branch_account_count:>6}\n")
    report_file.write(' \n')
    report_file.write(' ' + ' ' * 25 + f"TOTAL OPERATIVE LIMITS = {branch_total_operative:>20,.2f}\n")
    report_file.write(' ' + ' ' * 25 + '-' * 49 + '\n')
    report_file.write(' \n')


def _write_branch_header(report_file, title1, title2, report_date, od_label):
    # ASA carriage control: '1' = new page
    report_file.write('1')
    report_file.write(f"  {title1}\n")
    report_file.write(f"   {title2}\n")
    report_file.write(f"   REPORT AS AT {report_date}\n")
    report_file.write('   \n')
    report_file.write(
        f" BRN ACCOUNT NO NAME OF CUSTOMER          BASE {od_label:>5} OUSTANDING      "
        "APPROVED        LIMIT1      RATE1 COLL1    LIMIT2      RATE2 COLL2    LIMIT3      RATE3 COLL3\n"
    )
    report_file.write(
        "                                           RATE ST   BALANCE          "
        "LIMIT                                                                     \n"
    )
    report_file.write(' ' + '-' * 132 + '\n')


def _build_detail_line(row):
    line = ' '
    line += f"{_safe_text(row['BRN'], 3):<3} "
    line += f"{_safe_int(row['ACCTNO']):<10} "
    line += f"{_safe_text(row['NAME'], 25):<25} "
    line += f"{_safe_float(row['LMTBASER']):>5.2f} "
    line += f"{_safe_text(row['ODSTATUS'], 5):<5} "
    line += f"{_safe_float(row['BALANCE']):>12,.2f} "
    line += f"{_safe_text(row['CRI'], 2):<2} "
    line += f"{_safe_float(row['APPRLIMT']):>12,.2f} "
    line += f"{_safe_float(row['LIMIT1']):>11,.2f} "
    line += f"{_safe_float(row['RATE1']):>5.2f} "
    line += f"{_safe_text(row['COLL1'], 5):<5}\n"
    return line


def _build_limit2_line(row):
    if not row['LIMIT2'] or row['LIMIT2'] <= 0:
        return ''
    limit2 = _safe_float(row['LIMIT2'])
    rate2 = _safe_float(row['RATE2'])
    coll2 = _safe_text(row['COLL2'], 5)
    return ' ' + ' ' * 105 + f"{limit2:>11,.2f} {rate2:>5.2f} {coll2:<5}\n"


def _write_report_file(brnref, output_file, is_islamic, report_date):
    title1, title2, od_label = _get_report_titles(is_islamic)
    current_brn = None
    branch_totals = {"approved": 0.0, "operative": 0.0, "accounts": 0}

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w') as report_file:
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

            branch_totals["approved"] += _safe_float(row['APPRLIMT'])
            branch_totals["operative"] += _safe_float(row['LIMITS'])
            branch_totals["accounts"] += 1

        if current_brn is not None:
            _write_branch_subtotal(
                report_file,
                branch_totals["approved"],
                branch_totals["accounts"],
                branch_totals["operative"],
            )


def generate_od_report(current_file, overdft_file,
                       output_file, is_islamic=False, odplan_filter=None):
    """
    Generate overdraft limit report.

    Args:
        current_file: Path to current accounts .sas7bdat file
        overdft_file: Path to overdraft .sas7bdat file
        output_file: Path to output report .txt file
        is_islamic: Boolean indicating if this is Islamic bank report
        odplan_filter: List of ODPLAN codes or single value
    """
    print(f"\n{'=' * 70}")
    print(f"Generating {'Islamic Bank CLF-i' if is_islamic else 'Public Bank OD'} Limits Report")
    print(f"{'=' * 70}")

    print(f"\nReport Date: {REPORT_DATE}")

    print("\nStep 1: Processing current accounts...")
    current = _load_current_accounts(current_file, odplan_filter)
    print(f"Current accounts: {len(current):,}")

    print("\nStep 2: Processing overdraft data...")
    ovdr = _load_overdraft_data(overdft_file)
    print(f"Overdraft records: {len(ovdr):,}")

    print("\nStep 3: Pivoting limits (up to 5 per account)...")
    odmerg = _pivot_overdraft_limits()
    print(f"Accounts with pivoted limits: {len(odmerg):,}")

    print("\nStep 4: Merging current accounts with overdraft data...")
    ovdrm = _merge_current_with_overdraft()
    print(f"Merged records: {len(ovdrm):,}")

    print("\nStep 5: Formatting branch codes...")
    brnref = _format_branch_codes()
    print(f"Final records with branch codes: {len(brnref):,}")

    print("\nStep 6: Generating report...")
    _write_report_file(brnref, output_file, is_islamic, REPORT_DATE)
    print(f"Report saved: {output_file}")

    print("\nReport Statistics:")
    print(f"  Total Accounts: {len(brnref):,}")
    print(f"  Total Branches: {brnref['BRN'].nunique()}")
    if len(brnref) > 0:
        print(f"  Total Approved Limits: {brnref['APPRLIMT'].sum():,.2f}")
        print(f"  Total Operative Limits: {brnref['LIMITS'].sum():,.2f}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

print("=" * 70)
print("OVERDRAFT LIMITS REPORT GENERATION")
print("=" * 70)


# ============================================================================
# PART 1: PUBLIC BANK - OD LIMITS (ODPLAN 100-105)
# ============================================================================

try:
    generate_od_report(
        current_file=INPUT_PBB_CURRENT,
        overdft_file=INPUT_PBB_OVERDFT,
        output_file=OUTPUT_PBB_REPORT,
        is_islamic=False,
        odplan_filter=[100, 101, 102, 103, 104, 105]
    )
except Exception as e:
    print(f"\nError generating Public Bank report: {e}")


# ============================================================================
# PART 2: PUBLIC ISLAMIC BANK - CLF-i LIMITS (ODPLAN 106)
# ============================================================================

try:
    generate_od_report(
        current_file=INPUT_PIBB_CURRENT,
        overdft_file=INPUT_PIBB_OVERDFT,
        output_file=OUTPUT_PIBB_REPORT,
        is_islamic=True,
        odplan_filter=106
    )
except Exception as e:
    print(f"\nError generating Islamic Bank report: {e}")


# ============================================================================
# SUMMARY
# ============================================================================
con.close()

print(f"\nGenerated Reports:\n  1. Public Bank OD Limits    : {OUTPUT_PBB_REPORT}\n  2. Islamic Bank CLF-i Limits: {OUTPUT_PIBB_REPORT}")
