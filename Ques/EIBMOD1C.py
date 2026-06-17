#!/usr/bin/env python3
"""
Program : EIBMOD1C.py
Purpose : OD Listing by FISS Purpose Code (for all CustCodes)
          Produces reports for both Public Bank Berhad (PBB) and Public Islamic Bank Berhad (PIBB).
          NAME column is resolved by joining stg_dp_limit.sas7bdat ACCTNO
          against the overdraft data ACCTNO and taking NAME.
          Accounts with no matching NAME will show a blank NAME field.
          Output is a fixed-width plain-text report with form-feed page breaks,
          following the same layout and pagination approach as EIBMODLM_DONE.py.
          Runs after EIBXODLC.py in the scheduling pipeline.
"""

from pathlib import Path

import duckdb
import pandas as pd
import polars as pl

from REPTDATE import get_reptdate_values

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

# Testing Path
BASE_DIR   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS2")
INPUT_DIR  = BASE_DIR / "output" / "EIBXODLC"        # Output path from EIBXODLC.py
OUTPUT_DIR = BASE_DIR / "output" / "EIBMOD1C"

# # Production Path
# INPUT_DIR  = Path("/dwh")
# OUTPUT_DIR = Path("/host/mis/output/report") / "EIBMOD1C"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# PBB parquet inputs (ODRAF1 — sorted BY BRANCH FISSPURP ACCTNO from EIBXODLC)
PBB_ODLC_PATH_1   = INPUT_DIR / "PBB"  / "ODLC_OVERDRAFT1_06.parquet"

# PIBB parquet inputs
PIBB_ODLCI_PATH_1 = INPUT_DIR / "PIBB" / "ODLCI_OVERDRAFT1_06.parquet"

# Shared customer name lookup file (ACCTNO -> NAME)
INPUT_CUSTNAME     = BASE_DIR / "input/prod" / "stg_dp_limit.sas7bdat"
# INPUT_CUSTNAME   = Path("/sas/deposit/dwh/staging") / "stg_dp_limit.sas7bdat"

# Output report files
PBB_OUTPUT_PATH   = OUTPUT_DIR / "PBB"  / "ODRAFT1_ODLC.txt"
PIBB_OUTPUT_PATH  = OUTPUT_DIR / "PIBB" / "ODRAFT1_ODLCI.txt"

# ============================================================================
# REPORT LAYOUT CONSTANTS
# ============================================================================
PAGE_SIZE = 60       # PS=60 (default when not specified in OPTIONS)

# The subtotal block written after each FISSPURP group (3 lines):
#   dashes line (1) + subtotal line (1) + dashes line (1)
FISSPURP_SUBTOTAL_LINES = 3

# The grand-total block written after each BRANCH (3 lines):
#   dashes (1) + grand total (1) + dashes (1)
BRANCH_GRANDTOTAL_LINES = 3

# ============================================================================
# REPORT DATE
# ============================================================================
reptdate_values = get_reptdate_values()
REPTDATE    = reptdate_values.reptdate
REPTMON     = reptdate_values.reptmon
REPORT_DATE = REPTDATE.strftime('%d/%m/%y')

# ============================================================================
# INPUT FILE EXISTENCE CHECK — fail fast before any processing
# ============================================================================
_REQUIRED_INPUTS = {
    "PBB  Overdraft Data (ODRAF1)"  : PBB_ODLC_PATH_1,
    "PIBB Overdraft Data (ODRAF1)"  : PIBB_ODLCI_PATH_1,
    "Customer Name Lookup"          : INPUT_CUSTNAME,
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

def _safe_float(value) -> float:
    try:
        return float(value) if value is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


def _safe_int(value) -> int:
    try:
        return int(value) if value is not None else 0
    except (ValueError, TypeError):
        return 0


def _safe_text(value, length: int) -> str:
    if value is None:
        return ''
    return str(value).strip()[:length]


def _fmt_branch(value) -> str:
    # """Format branch as zero-padded 3-digit string (SAS Z3. format)."""
    """Format branch without leading zeroes."""
    if value is None:
        return '   '
    text = str(value).strip()
    if text.endswith('.0'):
        text = text[:-2]
    digits = ''.join(ch for ch in text if ch.isdigit())
    # if digits:
    #     return digits.zfill(3)[-3:]
    # return text[:3].zfill(3)
    if digits:
        return str(int(digits))
    return text


# ============================================================================
# CUSTOMER NAME LOOKUP
# ============================================================================

def _load_custname_lookup(con: duckdb.DuckDBPyConnection) -> None:
    """Load stg_dp_limit.sas7bdat and register ACCTNO -> NAME lookup into DuckDB.

    Join logic:
        if ACCTNO in stg_dp_limit == ACCTNO in overdraft data,
        then NAME from stg_dp_limit is used.
    Unmatched accounts carry a blank NAME.
    """
    pandas_df = pd.read_sas(INPUT_CUSTNAME, format='sas7bdat', encoding='latin1')
    pandas_df.columns = [str(c).upper().strip() for c in pandas_df.columns]

    required = {'ACCTNO', 'NAME'}
    missing  = required - set(pandas_df.columns)
    if missing:
        raise ValueError(
            f"{INPUT_CUSTNAME.name} is missing required column(s): {', '.join(sorted(missing))}"
        )

    lookup_pd = (
        pandas_df[['ACCTNO', 'NAME']]
        .drop_duplicates(subset=['ACCTNO'], keep='first')
        .reset_index(drop=True)
    )
    con.register('custname_lookup', lookup_pd)


# ============================================================================
# DATA LOADING
# ============================================================================

def _load_odraft1(con: duckdb.DuckDBPyConnection, parquet_path: Path) -> pd.DataFrame:
    """Load ODRAF1 parquet and enrich with NAME via left-join on stg_dp_limit.

    SAS equivalent:
        DATA ODRAFT1;  (output of EIBXODLC — already sorted BY BRANCH FISSPURP ACCTNO)
        SET ODLC.ODRAF1&REPTMON;

    NAME is resolved from stg_dp_limit.sas7bdat.
    Accounts with no match carry NAME = ''.
    """
    df = con.execute(f"""
        SELECT
            o.*,
            COALESCE(c.NAME, '') AS NAME
        FROM read_parquet('{parquet_path}') o
        LEFT JOIN custname_lookup c
            ON CAST(o.ACCTNO AS BIGINT) = CAST(c.ACCTNO AS BIGINT)
        ORDER BY o.BRANCH, o.FISSPURP, o.ACCTNO
    """).df()
    return df


# ============================================================================
# REPORT TITLE & HEADER BUILDERS
# ============================================================================

def _get_report_titles(is_islamic: bool) -> tuple:
    if is_islamic:
        return (
            'REPORT NO :  ODLIST                         PUBLIC ISLAMIC BANK BERHAD',
            'PROGRAM ID:  EIBMOD1C',
        )
    return (
        'REPORT NO :  ODLIST                           PUBLIC BANK BERHAD',
        'PROGRAM ID:  EIBMOD1C',
    )


def _build_title_lines(title1: str, title2: str, report_date: str, branch: str) -> list:
    return [
        f"1{title1}\n",
        f" {title2}\n",
        f" OD LISTING BY FISS PURPOSE CODE (FOR ALL CUSTCODES)"
        f"{' ' * 59}REPORT DATE: {report_date}\n",
        f"   **\n",
        f"\n",
        f" BRANCH={branch}\n",
        f"\n",
    ]


def _build_primary_header_lines() -> list:
    """Primary table header: ACCTNO .. LIMIT2."""
    hdr1 = (
        f"   {'ACCOUNT':>10}  {'':<24}  {'APPROVE':>12}"
        f"  {'OUTSTANDING':>12}  {'PUR':<4}  {'SEC':<4}  {'CUST':>4}"
        f"  {'ST':<3}  {'FLAT':>5}"
        f"  {'':>12}  {'':>12}"
    )
    hdr2 = (
        f"   {'NUMBER':>10}  {'CUSTOMER NAME':<24}  {'LIMIT':>12}"
        f"  {'BALANCE':>12}  {'POSE':<4}  {'TOR':<4}  {'CODE':>4}"
        f"  {'CD':<3}  {'RATE':>5}"
        f"  {'LIMIT1':>12}  {'LIMIT2':>12}"
    )
    underline = '   ' + '-' * len(hdr2.rstrip())
    return [
        f"{hdr1}\n",
        f"{hdr2}\n",
        f"{underline}\n",
        f"\n",
    ]


def _build_secondary_header_lines() -> list:
    """Secondary table header: LIMIT3 .. COLL5."""
    hdr = (
        f"\n   "
        f"{'LIMIT3':>12}  {'RATE3':>5}  {'COLL3':>5}"
        f"  {'LIMIT4':>12}  {'RATE4':>5}  {'COLL4':>5}"
        f"  {'LIMIT5':>12}  {'RATE5':>5}  {'COLL5':>5}"
        f"  {'RATE1':>5}  {'COLL1':>5}  {'RATE2':>5}  {'COLL2':>5}\n"
    )
    underline = f"   {'-' * 96}\n"
    return [hdr, underline]


# ============================================================================
# DETAIL LINE BUILDERS
# ============================================================================

def _fmt_balance(value) -> str:
    return f"{_safe_float(value):.2f}"[:11]


def _build_primary_detail_line(row: dict) -> str:
    """Primary detail line: ACCTNO .. LIMIT2."""
    return (
        f"   {_safe_int(row.get('ACCTNO')):>10} "
        f" {_safe_text(row.get('NAME'), 15):<24} "
        f" {_safe_float(row.get('APPRLIMT')):>12.2f} "
        f" {_fmt_balance(row.get('BALANCE')):>12} "
        f" {_safe_text(row.get('FISSPURP'), 4):>4} "
        f" {_safe_text(row.get('SECTORCD'), 4):>4} "
        f" {_safe_int(row.get('CUSTCD')):>4} "
        f" {_safe_text(row.get('STATE'), 3):<3} "
        f" {_safe_float(row.get('FLATRATE')):>5.2f} "
        f" {_safe_float(row.get('LIMIT1')):>12.2f} "
        f" {_safe_float(row.get('LIMIT2')):>12.2f}\n"
    )


def _build_secondary_detail_line(row: dict) -> str:
    """Secondary detail line: LIMIT3 .. COLL5 (plus RATE1/COLL1/RATE2/COLL2)."""
    return (
        f"   "
        f"{_safe_float(row.get('LIMIT3')):>12.2f}  "
        f"{_safe_float(row.get('RATE3')):>5.2f}  "
        f"{_safe_text(row.get('COL3'), 5):>5}  "
        f"{_safe_float(row.get('LIMIT4')):>12.2f}  "
        f"{_safe_float(row.get('RATE4')):>5.2f}  "
        f"{_safe_text(row.get('COL4'), 5):>5}  "
        f"{_safe_float(row.get('LIMIT5')):>12.2f}  "
        f"{_safe_float(row.get('RATE5')):>5.2f}  "
        f"{_safe_text(row.get('COL5'), 5):>5}  "
        f"{_safe_float(row.get('RATE1')):>5.2f}  "
        f"{_safe_text(row.get('COL1'), 5):>5}  "
        f"{_safe_float(row.get('RATE2')):>5.2f}  "
        f"{_safe_text(row.get('COL2'), 5):>5}\n"
    )


# ============================================================================
# SUBTOTAL LINE BUILDERS
# ============================================================================

def _write_fisspurp_subtotal(
    report_file,
    fisspurp: str,
    balance_sum: float,
) -> None:
    """Write FISSPURP-level subtotal block (3 lines).

    SAS equivalent (COMPUTE AFTER FISSPURP):
        LINE @015 52*'-';
        LINE @015 'SUBTOTAL FOR FISS PURPOSE   '  FISSPURP $4.  @054 BALANCE.SUM  13.2;
        LINE @015 52*'-';
    """
    sep_line = ' ' * 15 + '-' * 52
    report_file.write(sep_line + '\n')
    subtotal_str = f"{balance_sum:.2f}"[:13]
    report_file.write(
        f"{' ' * 15}{'SUBTOTAL FOR FISS PURPOSE   '}"
        f"{_safe_text(fisspurp, 4):<4}"
        f"{subtotal_str:>20}\n"
    )
    report_file.write(sep_line + '\n')


def _write_branch_grandtotal(
    report_file,
    branch: str,
    balance_sum: float,
) -> None:
    """Write BRANCH-level grand total block (3 lines).

    SAS equivalent (COMPUTE AFTER BRANCH):
        LINE @015 52*'-';
        LINE @015 'GRAND TOTAL FOR BRANCH   '  BRANCH Z3.  @054 BALANCE.SUM  13.2;
        LINE @015 52*'-';
    """
    sep_line = ' ' * 15 + '-' * 52
    report_file.write(sep_line + '\n')
    grandtotal_str = f"{balance_sum:.2f}"[:13]
    report_file.write(
        f"{' ' * 15}{'GRAND TOTAL FOR BRANCH   '}"
        f"{_fmt_branch(branch)}"
        f"{grandtotal_str:>24}\n"
    )
    report_file.write(sep_line + '\n')


# ============================================================================
# REPORT FILE WRITER
# ============================================================================

def _write_report_file(
    odraft_df: pd.DataFrame,
    output_file: Path,
    is_islamic: bool,
    report_date: str,
) -> None:
    title1, title2 = _get_report_titles(is_islamic)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w', encoding='utf-8') as report_file:
        add_form_feed = False

        for branch_code, branch_df in odraft_df.groupby('BRANCH', sort=False):
            branch_str         = _fmt_branch(branch_code)
            branch_balance_sum = 0.0

            title_lines           = _build_title_lines(title1, title2, report_date, branch_str)
            primary_header_lines  = _build_primary_header_lines()
            secondary_header_lines = _build_secondary_header_lines()

            fixed_primary   = len(title_lines) + len(primary_header_lines)
            fixed_secondary = len(title_lines) + len(secondary_header_lines)

            fisspurp_groups = list(branch_df.groupby('FISSPURP', sort=False))
            total_fp_groups = len(fisspurp_groups)

            # Collect per-FISSPURP balance sums for secondary pass reuse
            fp_balance_sums = {}

            # ── PRIMARY TABLE PASS ───────────────────────────────────────────
            page_lines_used = 0

            def _start_primary_page():
                nonlocal add_form_feed, page_lines_used
                if add_form_feed:
                    report_file.write('\f')
                for line in title_lines:
                    report_file.write(line)
                for line in primary_header_lines:
                    report_file.write(line)
                add_form_feed = True
                page_lines_used = fixed_primary

            def _ensure_primary(needed: int):
                nonlocal page_lines_used
                if page_lines_used + needed > PAGE_SIZE:
                    _start_primary_page()

            _start_primary_page()

            for fp_idx, (fisspurp_code, fp_df) in enumerate(fisspurp_groups):
                fp_rows       = list(fp_df.iterrows())
                total_fp_rows = len(fp_rows)
                fp_balance    = 0.0
                is_last_fp    = (fp_idx == total_fp_groups - 1)

                for row_idx, (_, row) in enumerate(fp_rows):
                    is_last_row = (row_idx == total_fp_rows - 1)
                    lines_needed = 1 + (FISSPURP_SUBTOTAL_LINES if is_last_row else 0)
                    if is_last_row and is_last_fp:
                        lines_needed += BRANCH_GRANDTOTAL_LINES
                    _ensure_primary(lines_needed)

                    report_file.write(_build_primary_detail_line(row))
                    page_lines_used += 1
                    fp_balance += _safe_float(row.get('BALANCE'))

                _ensure_primary(FISSPURP_SUBTOTAL_LINES)
                _write_fisspurp_subtotal(report_file, str(fisspurp_code), fp_balance)
                page_lines_used += FISSPURP_SUBTOTAL_LINES

                fp_balance_sums[fisspurp_code] = fp_balance
                branch_balance_sum += fp_balance

            _ensure_primary(BRANCH_GRANDTOTAL_LINES)
            _write_branch_grandtotal(report_file, branch_code, branch_balance_sum)
            page_lines_used += BRANCH_GRANDTOTAL_LINES

            # ── SECONDARY TABLE PASS ─────────────────────────────────────────
            page_lines_used = 0

            def _start_secondary_page():
                nonlocal add_form_feed, page_lines_used
                if add_form_feed:
                    report_file.write('\f')
                for line in title_lines:
                    report_file.write(line)
                for line in secondary_header_lines:
                    report_file.write(line)
                add_form_feed = True
                page_lines_used = fixed_secondary

            def _ensure_secondary(needed: int):
                nonlocal page_lines_used
                if page_lines_used + needed > PAGE_SIZE:
                    _start_secondary_page()

            _start_secondary_page()

            for fp_idx, (fisspurp_code, fp_df) in enumerate(fisspurp_groups):
                fp_rows       = list(fp_df.iterrows())
                total_fp_rows = len(fp_rows)
                fp_balance    = fp_balance_sums[fisspurp_code]
                is_last_fp    = (fp_idx == total_fp_groups - 1)

                for row_idx, (_, row) in enumerate(fp_rows):
                    is_last_row  = (row_idx == total_fp_rows - 1)
                    lines_needed = 1 + (FISSPURP_SUBTOTAL_LINES if is_last_row else 0)
                    if is_last_row and is_last_fp:
                        lines_needed += BRANCH_GRANDTOTAL_LINES
                    _ensure_secondary(lines_needed)

                    report_file.write(_build_secondary_detail_line(row))
                    page_lines_used += 1

                _ensure_secondary(FISSPURP_SUBTOTAL_LINES)
                _write_fisspurp_subtotal(report_file, str(fisspurp_code), fp_balance)
                page_lines_used += FISSPURP_SUBTOTAL_LINES

            _ensure_secondary(BRANCH_GRANDTOTAL_LINES)
            _write_branch_grandtotal(report_file, branch_code, branch_balance_sum)
            page_lines_used += BRANCH_GRANDTOTAL_LINES


# ============================================================================
# REPORT GENERATOR (main entry per bank)
# ============================================================================

def generate_od_listing_report(
    parquet_path: Path,
    output_file: Path,
    is_islamic: bool,
) -> bool:
    """Generate OD listing report for one bank entity.

    NAME resolution:
        ACCTNO in parquet == ACCTNO in stg_dp_limit.sas7bdat → NAME
        No match → NAME = '' (blank)

    Args:
        parquet_path : Path to ODRAF1 parquet (output of EIBXODLC)
        output_file  : Path to output .txt report
        is_islamic   : True for PIBB / CLF-i, False for PBB / OD
    Returns:
        True if successful, False otherwise.
    """
    bank_label = 'Public Islamic Bank Berhad (PIBB)' if is_islamic else 'Public Bank Berhad (PBB)'
    print(f"\n{'=' * 70}")
    print(f"Generating OD Listing Report — {bank_label}")
    print(f"{'=' * 70}")
    print(f"Report Date : {REPORT_DATE}")

    con = duckdb.connect(database=':memory:')
    try:
        print("\nStep 1: Loading customer name lookup (stg_dp_limit)...")
        _load_custname_lookup(con)
        print("Customer name lookup registered.")

        print("\nStep 2: Loading and enriching ODRAFT1 data...")
        odraft_df = _load_odraft1(con, parquet_path)
        print(f"ODRAFT1 rows loaded : {len(odraft_df):,}")
        matched = (odraft_df['NAME'].str.strip() != '').sum()
        print(f"NAME matched        : {matched:,} / {len(odraft_df):,}")

        print("\nStep 3: Generating report...")
        _write_report_file(odraft_df, output_file, is_islamic, REPORT_DATE)
        print(f"Report saved : {output_file}")

        print("\nReport Statistics:")
        print(f"  Total Accounts    : {len(odraft_df):,}")
        print(f"  Total Branches    : {odraft_df['BRANCH'].nunique()}")
        print(f"  Total FISS Groups : {odraft_df['FISSPURP'].nunique()}")
        if len(odraft_df) > 0:
            print(f"  Total Balance     : {odraft_df['BALANCE'].sum():,.2f}")

        print(f"\n{'=' * 20} PREVIEW: {output_file.name} {'=' * 20}\n")
        with open(output_file, 'r', encoding='utf-8') as f:
            print(f.read())
        print(f"{'=' * 20} END PREVIEW {'=' * 20}\n")

        return True

    except Exception as exc:
        print(f"\n[ERROR] Report generation failed for {output_file.name}: {type(exc).__name__}: {exc}")
        return False

    finally:
        con.close()


# ============================================================================
# MAIN EXECUTION
# ============================================================================

print('=' * 70)
print('OD LISTING BY FISS PURPOSE CODE — REPORT GENERATION')
print('=' * 70)

results = {}

# ============================================================================
# PART 1: PUBLIC BANK BERHAD (PBB)
# ============================================================================
results['PBB'] = generate_od_listing_report(
    parquet_path=PBB_ODLC_PATH_1,
    output_file=PBB_OUTPUT_PATH,
    is_islamic=False,
)

# ============================================================================
# FOR PIBB
# ============================================================================
results['PIBB'] = generate_od_listing_report(
    parquet_path=PIBB_ODLCI_PATH_1,
    output_file=PIBB_OUTPUT_PATH,
    is_islamic=True,
)

# ============================================================================
# SUMMARY
# ============================================================================

print('\n' + '=' * 70)
print('GENERATED REPORTS:')
print('=' * 70)

if results['PBB']:
    print(f"  1. Public Bank OD Listing          : {PBB_OUTPUT_PATH}")
else:
    print(f"  1. Public Bank OD Listing          : [FAILED]")

if results['PIBB']:
    print(f"  2. Public Islamic Bank OD Listing  : {PIBB_OUTPUT_PATH}")
else:
    print(f"  2. Public Islamic Bank OD Listing  : [FAILED]")

if all(results.values()):
    print('\nREPORT GENERATION COMPLETE')
else:
    print('\nREPORT GENERATION COMPLETED WITH ERRORS — review output above.')
