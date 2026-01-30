#!/usr/bin/env python3
"""
FILE NAME: EIBMCOLR
REPORT: BANK'S TOTAL LOANS AND ADVANCES BY COLLATERALS

Generating loan collateral reports:
1. Report by branch with risk categories
2. Summary report for all loans
3. Summary report for conventional loans
4. Summary report for Islamic loans
5. Variance report comparing current vs previous month
"""

import duckdb
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import sys
from dateutil.relativedelta import relativedelta


# ============================================================================
# Configuration and Path Setup
# ============================================================================

# Input/Output paths
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input files
REPTDATE_FILE = INPUT_DIR / "reptdate.parquet"
LOAN_FILE_TEMPLATE = INPUT_DIR / "loan{month}{week}.parquet"
BRANCH_FILE = INPUT_DIR / "branch.parquet"
TOTIIS_FILE_TEMPLATE = INPUT_DIR / "totiis{month}.parquet"
RCAT_PREV_FILE = INPUT_DIR / "eibmcolr_rcat_prev.dat"

# Output files
RCAT_OUTPUT_FILE = OUTPUT_DIR / "eibmcolr_rcat.dat"
REPORT1_FILE = OUTPUT_DIR / "eibmcolr_by_branch.txt"
REPORT2_FILE = OUTPUT_DIR / "eibmcolr_summary_all.txt"
REPORT3_FILE = OUTPUT_DIR / "eibmcolr_summary_conv.txt"
REPORT4_FILE = OUTPUT_DIR / "eibmcolr_summary_islamic.txt"
REPORT5_FILE = OUTPUT_DIR / "eibmcolr_variance.txt"

# Report configuration
PAGE_LENGTH = 65  # PS=65
NPRODUCT = [500, 520]  # Products to exclude


# ============================================================================
# Format Definitions
# ============================================================================

def format_risk(riskcat):
    """Format risk category percentage"""
    risk_formats = {
        0: '  0%',
        10: ' 10%',
        20: ' 20%',
        50: ' 50%',
        100: '100%'
    }
    return risk_formats.get(riskcat, f'{riskcat:3d}%')


# ============================================================================
# Main Processing Functions
# ============================================================================

def process_reptdate():
    """
    Process REPTDATE to extract macro variables and calculate dates
    Returns: dict with macro variables
    """
    df = pl.read_parquet(REPTDATE_FILE)

    if len(df) == 0:
        raise ValueError("REPTDATE file is empty")

    reptdate = df['REPTDATE'][0]
    day = reptdate.day
    mm = reptdate.month
    year = reptdate.year

    # Determine week number and starting day
    if day == 8:
        sdd = 1
        wk = '1'
        wk1 = '4'
    elif day == 15:
        sdd = 9
        wk = '2'
        wk1 = '1'
    elif day == 22:
        sdd = 16
        wk = '3'
        wk1 = '2'
    else:  # OTHERWISE
        sdd = 23
        wk = '4'
        wk1 = '3'

    # Calculate MM1 (previous month if WK='1')
    if wk == '1':
        mm1 = mm - 1
        if mm1 == 0:
            mm1 = 12
    else:
        mm1 = mm

    # Calculate SDATE
    sdate = datetime(year, mm, sdd)

    return {
        'NOWK': wk,
        'NOWK1': wk1,
        'REPTMON': f"{mm:02d}",
        'REPTMON1': f"{mm1:02d}",
        'REPTYEAR': str(year),
        'REPTDAY': f"{day:02d}",
        'RDATE': f"{day:02d}/{mm:02d}/{year}",
        'SDATE': f"{sdate.day:02d}/{sdate.month:02d}/{year}",
        'reptdate_obj': reptdate
    }


def classify_collateral(liabcod1, fisspurp):
    """
    Classify loan based on liability code and purpose
    Returns: (bnmcode, riskcat)
    """
    if liabcod1 in ['007', '012', '013', '014', '024', '048', '049', '117']:
        return '30307', 0
    elif liabcod1 == '021':
        return '30309', 0
    elif liabcod1 in ['017', '026', '029']:
        return '30009/17', 10
    elif liabcod1 in ['006', '016']:
        return '30323', 20
    elif liabcod1 in ['011', '030']:
        return '30325', 20
    elif liabcod1 in ['018', '027']:
        return '30327', 20
    elif liabcod1 == '003':
        return '30009/10', 20
    elif liabcod1 == '025':
        return '30335', 20
    elif liabcod1 in ['050', '118']:
        bnmcode = '30341'
        if fisspurp in ['0311', '0312', '0313', '0314', '0315', '0316']:
            riskcat = 50
        else:
            riskcat = 100
        return bnmcode, riskcat
    elif liabcod1 in ['019', '028', '031']:
        return '30351', 100
    else:
        return '30359', 100


def process_loan_data(macro_vars):
    """
    Process loan data: filter, classify, and handle collateral
    """
    # Construct loan filename
    loan_file = INPUT_DIR / f"loan{macro_vars['REPTMON']}{macro_vars['NOWK']}.parquet"

    if not loan_file.exists():
        raise FileNotFoundError(f"Loan file not found: {loan_file}")

    # Read loan data
    con = duckdb.connect()

    # Read and filter loan data
    loan_df = con.execute(f"""
        SELECT *
        FROM read_parquet('{loan_file}')
        WHERE PRODCD NOT IN ('N', '54120')
          AND PRODUCT NOT IN ({','.join(map(str, NPRODUCT))})
    """).pl()

    con.close()

    # Process liability codes for OD accounts
    def extract_liabcode(row):
        """Extract liability code from COL1/COL2 for OD accounts"""
        if row['ACCTYPE'] == 'OD':
            col1 = row['COL1'] if row['COL1'] is not None else ''
            col2 = row['COL2'] if row['COL2'] is not None else ''

            # Extract 4 possible liability codes
            liab1 = col1[0:2].strip() if len(col1) >= 2 else ''
            liab2 = col1[3:5].strip() if len(col1) >= 5 else ''
            liab3 = col2[0:2].strip() if len(col2) >= 2 else ''
            liab4 = col2[3:5].strip() if len(col2) >= 5 else ''

            # Priority order
            if liab1:
                return liab1
            elif liab2:
                return liab2
            elif liab3:
                return liab3
            elif liab4:
                return liab4
            else:
                return ''
        return ''

    # Add LIABCODE and LIABCOD1
    loan_df = loan_df.with_columns([
        pl.struct(['ACCTYPE', 'COL1', 'COL2']).map_elements(
            lambda x: extract_liabcode(x),
            return_dtype=pl.Utf8
        ).alias('LIABCODE')
    ])

    loan_df = loan_df.with_columns([
        (pl.lit('0') + pl.col('LIABCODE')).alias('LIABCOD1')
    ])

    # Classify collateral
    def classify_row(row):
        """Classify loan and return bnmcode and riskcat"""
        return classify_collateral(row['LIABCOD1'], row.get('FISSPURP', ''))

    classifications = loan_df.select(['LIABCOD1', 'FISSPURP']).to_dicts()
    bnmcodes = []
    riskcats = []

    for row in classifications:
        bnmcode, riskcat = classify_collateral(row['LIABCOD1'], row.get('FISSPURP', ''))
        bnmcodes.append(bnmcode)
        riskcats.append(riskcat)

    loan_df = loan_df.with_columns([
        pl.Series('BNMCODE', bnmcodes),
        pl.Series('RISKCAT', riskcats)
    ])

    # Handle LN accounts with collateral value logic
    ln_records = []
    od_records = []

    for row in loan_df.iter_rows(named=True):
        if row['ACCTYPE'] == 'LN':
            # NO COLLATERAL VALUE FOR OD
            if row['LIABCOD1'] != '050':
                appvalue = row.get('APPVALUE', 0) or 0
                balance = row.get('BALANCE', 0) or 0

                if appvalue > 0 and appvalue < balance:
                    # SCENARIO 3 FOR 1 COLLATERAL ONLY
                    # Split into two records
                    row1 = dict(row)
                    row1['BALANCE'] = appvalue
                    ln_records.append(row1)

                    row2 = dict(row)
                    row2['BALANCE'] = balance - appvalue
                    row2['BNMCODE'] = '30359'
                    row2['RISKCAT'] = 100
                    ln_records.append(row2)
                else:
                    ln_records.append(dict(row))
            else:
                ln_records.append(dict(row))
        else:  # OD
            od_records.append(dict(row))

    # Separate OD and LN
    odac = pl.DataFrame(od_records) if od_records else pl.DataFrame()
    lnac = pl.DataFrame(ln_records) if ln_records else pl.DataFrame()

    # Add TOTIIS=0, NOTENO=0 for OD accounts
    if len(odac) > 0:
        odac = odac.with_columns([
            pl.lit(0.0).alias('TOTIIS'),
            pl.lit(0).alias('NOTENO')
        ])

    return odac, lnac, macro_vars


def merge_totiis(lnac, macro_vars):
    """
    Merge TOTIIS data with loan accounts
    """
    totiis_file = INPUT_DIR / f"totiis{macro_vars['REPTMON']}.parquet"

    if not totiis_file.exists():
        print(f"Warning: TOTIIS file not found: {totiis_file}")
        # Add TOTIIS=0 if file doesn't exist
        if len(lnac) > 0:
            lnac = lnac.with_columns([
                pl.lit(0.0).alias('TOTIIS')
            ])
        return lnac

    # Read TOTIIS data
    totiis_df = pl.read_parquet(totiis_file)
    totiis_df = totiis_df.select(['ACCTNO', 'NOTENO', 'TOTIIS']).unique()

    # Merge with loan accounts
    lnac = lnac.join(totiis_df, on=['ACCTNO', 'NOTENO'], how='left')

    # Fill missing TOTIIS with 0
    lnac = lnac.with_columns([
        pl.col('TOTIIS').fill_null(0.0)
    ])

    return lnac


def merge_branch_data(loan_df):
    """
    Merge branch information with loan data
    """
    branch_file = BRANCH_FILE

    if not branch_file.exists():
        print(f"Warning: Branch file not found: {branch_file}")
        return loan_df

    # Read branch data
    branch_df = pl.read_parquet(branch_file)

    # Create BRBOTH field
    branch_df = branch_df.with_columns([
        (pl.col('BRANCH').cast(pl.Utf8).str.zfill(3) + '-' + pl.col('BRHCODE')).alias('BRBOTH')
    ])

    # Merge with loan data
    loan_df = loan_df.join(branch_df.select(['BRANCH', 'BRBOTH']), on='BRANCH', how='left')

    return loan_df


def generate_report_by_branch(lnbr, macro_vars, output_file):
    """
    Generate report by branch with risk categories
    PROC PRINT with PAGEBY BRANCH
    """
    lines = []

    # Title section
    lines.append('1' + ' ' * 131)
    lines.append(' ' + 'REPORT ID: EIBMCOLR'.center(131))
    lines.append(' ' + 'PUBLIC BANK BERHAD'.center(131))
    lines.append(' ' + f"TOTAL LOANS AND ADVANCES BY COLLATERAL AS AT : {macro_vars['RDATE']}".center(131))
    lines.append(' ' + '(CONVENTIONAL + ISLAMIC) BY BRANCHES'.center(131))
    lines.append(' ')

    # Process by branch
    branches = sorted(lnbr['BRANCH'].unique().to_list())

    for branch in branches:
        branch_data = lnbr.filter(pl.col('BRANCH') == branch).sort(['RISKCAT', 'BNMCODE'])

        # New page for each branch
        lines.append('1' + ' ' * 131)
        lines.append(' ' + f"BRANCH={branch}".ljust(131))
        lines.append(' ')

        # Process by risk category
        riskcats = sorted(branch_data['RISKCAT'].unique().to_list())

        for riskcat in riskcats:
            risk_data = branch_data.filter(pl.col('RISKCAT') == riskcat)

            # Risk category header
            lines.append(' ' + '-' * 131)
            lines.append(' ' + f"RISK CATEGORY={format_risk(riskcat)}".ljust(131))
            lines.append(' ')

            # Column headers
            header = ' ' + 'BNMCODE'.ljust(12) + 'BALANCE (RM)'.rjust(22) + 'IIS AMT (RM)'.rjust(
                22) + 'NET AMT (RM)'.rjust(22)
            lines.append(header)
            lines.append(' ' + '-' * 78)

            # Detail lines
            riskcat_bal = 0.0
            riskcat_iis = 0.0
            riskcat_net = 0.0

            for row in risk_data.iter_rows(named=True):
                bnmcode = row['BNMCODE']
                balance = row['BALANCE']
                totiis = row['TOTIIS']
                netbal = row['NETBAL']

                riskcat_bal += balance
                riskcat_iis += totiis
                riskcat_net += netbal

                line = (' ' + bnmcode.ljust(12) +
                        f"{balance:,.2f}".rjust(22) +
                        f"{totiis:,.2f}".rjust(22) +
                        f"{netbal:,.2f}".rjust(22))
                lines.append(line)

            # Risk category subtotal
            lines.append(' ' + '=' * 78)
            subtotal = (' ' + f"{riskcat}".ljust(12) +
                        f"{riskcat_bal:,.2f}".rjust(22) +
                        f"{riskcat_iis:,.2f}".rjust(22) +
                        f"{riskcat_net:,.2f}".rjust(22))
            lines.append(subtotal)
            lines.append(' ')

        # Branch total
        branch_bal = branch_data['BALANCE'].sum()
        branch_iis = branch_data['TOTIIS'].sum()
        branch_net = branch_data['NETBAL'].sum()

        lines.append(' ' + '=' * 78)
        total = (' ' + 'TOTAL'.ljust(12) +
                 f"{branch_bal:,.2f}".rjust(22) +
                 f"{branch_iis:,.2f}".rjust(22) +
                 f"{branch_net:,.2f}".rjust(22))
        lines.append(total)
        lines.append(' ' + '=' * 78)

    # Write to file
    with open(output_file, 'w', encoding='utf-8') as f:
        for line in lines:
            f.write(line + '\n')


def generate_summary_report(ln, title4, output_file, type_filter, amtind_filter=None):
    """
    Generate summary report for all/conventional/Islamic loans
    """
    lines = []

    # Title section
    lines.append('1' + ' ' * 131)
    lines.append(' ' + 'REPORT ID: EIBMCOLR'.center(131))
    lines.append(' ' + 'PUBLIC BANK BERHAD'.center(131))
    lines.append(' ' + title4.center(131))
    lines.append(' ')

    # Filter data
    if amtind_filter:
        data = ln.filter((pl.col('_TYPE_') == type_filter) & (pl.col('AMTIND') == amtind_filter))
    else:
        data = ln.filter(pl.col('_TYPE_') == type_filter)

    data = data.sort(['RISKCAT', 'BNMCODE'])

    # Process by risk category
    riskcats = sorted(data['RISKCAT'].unique().to_list())

    grand_bal = 0.0
    grand_iis = 0.0
    grand_net = 0.0

    for riskcat in riskcats:
        risk_data = data.filter(pl.col('RISKCAT') == riskcat)

        # Risk category header
        lines.append(' ' + '-' * 131)
        lines.append(' ' + f"RISK".ljust(12))
        lines.append(' ' + f"CATEGORY={format_risk(riskcat)}".ljust(131))
        lines.append(' ')

        # Column headers
        header = ' ' + 'BNMCODE'.ljust(12) + 'BALANCE (RM)'.rjust(22) + 'IIS AMT (RM)'.rjust(22) + 'NET AMT (RM)'.rjust(
            22)
        lines.append(header)
        lines.append(' ' + '-' * 78)

        # Detail lines
        riskcat_bal = 0.0
        riskcat_iis = 0.0
        riskcat_net = 0.0

        for row in risk_data.iter_rows(named=True):
            bnmcode = row['BNMCODE']
            balance = row['BALANCE']
            totiis = row['TOTIIS']
            netbal = row['NETBAL']

            riskcat_bal += balance
            riskcat_iis += totiis
            riskcat_net += netbal

            line = (' ' + bnmcode.ljust(12) +
                    f"{balance:,.2f}".rjust(22) +
                    f"{totiis:,.2f}".rjust(22) +
                    f"{netbal:,.2f}".rjust(22))
            lines.append(line)

        # Risk category subtotal
        lines.append(' ' + '=' * 78)
        subtotal = (' ' + f"{riskcat}".ljust(12) +
                    f"{riskcat_bal:,.2f}".rjust(22) +
                    f"{riskcat_iis:,.2f}".rjust(22) +
                    f"{riskcat_net:,.2f}".rjust(22))
        lines.append(subtotal)
        lines.append(' ')

        grand_bal += riskcat_bal
        grand_iis += riskcat_iis
        grand_net += riskcat_net

    # Grand total
    lines.append(' ' + '=' * 78)
    total = (' ' + 'TOTAL'.ljust(12) +
             f"{grand_bal:,.2f}".rjust(22) +
             f"{grand_iis:,.2f}".rjust(22) +
             f"{grand_net:,.2f}".rjust(22))
    lines.append(total)
    lines.append(' ' + '=' * 78)

    # Write to file
    with open(output_file, 'w', encoding='utf-8') as f:
        for line in lines:
            f.write(line + '\n')


def generate_variance_report(lnvar, macro_vars, pdtedmy, output_file):
    """
    Generate variance report comparing current vs previous month
    """
    lines = []

    # Title section
    lines.append('1' + ' ' * 131)
    lines.append(' ' + 'REPORT ID: EIBMCOLR'.center(131))
    lines.append(' ' + 'PUBLIC BANK BERHAD'.center(131))
    lines.append(' ' + f"VARIANCE REPORT ON TOTAL LOANS AND ADVANCES AS AT : {macro_vars['RDATE']}".center(131))
    lines.append(' ' + 'BY RISK CATEGORY'.center(131))
    lines.append(' ')

    # Process by risk category and bnmcode
    lnvar = lnvar.sort(['RISKCAT', 'BNMCODE', 'BRANCH'])

    riskcats = sorted(lnvar['RISKCAT'].unique().to_list())

    for riskcat in riskcats:
        riskcat_data = lnvar.filter(pl.col('RISKCAT') == riskcat)
        bnmcodes = sorted(riskcat_data['BNMCODE'].unique().to_list())

        for bnmcode in bnmcodes:
            bnm_data = riskcat_data.filter(pl.col('BNMCODE') == bnmcode)

            # New page for each bnmcode
            lines.append('1' + ' ' * 131)
            lines.append(' ' + f"RISKCAT={format_risk(riskcat)} BNMCODE={bnmcode}".ljust(131))
            lines.append(' ')

            # Column headers
            header = (' ' + 'BRANCH'.ljust(8) +
                      'CURR MTH BAL'.rjust(18) +
                      'CURR MTH IIS'.rjust(18) +
                      'CURR MTH NET'.rjust(18) +
                      'INCREASE/(DECREASE)'.rjust(22) +
                      'PREV MTH NET'.rjust(18))
            lines.append(header)
            lines.append(' ' + '-' * 102)

            # Detail lines
            bnm_bal = 0.0
            bnm_iis = 0.0
            bnm_net = 0.0
            bnm_var = 0.0
            bnm_prev = 0.0

            for row in bnm_data.iter_rows(named=True):
                branch = row['BRANCH']
                balance = row['BALANCE']
                totiis = row['TOTIIS']
                netbal = row['NETBAL']
                varbal = row['VARBAL']
                prevnetb = row['PREVNETB']

                bnm_bal += balance
                bnm_iis += totiis
                bnm_net += netbal
                bnm_var += varbal
                bnm_prev += prevnetb

                line = (' ' + f"{branch:03d}".ljust(8) +
                        f"{balance:,.2f}".rjust(18) +
                        f"{totiis:,.2f}".rjust(18) +
                        f"{netbal:,.2f}".rjust(18) +
                        f"{varbal:,.2f}".rjust(22) +
                        f"{prevnetb:,.2f}".rjust(18))
                lines.append(line)

            # BNMCODE subtotal
            lines.append(' ' + '=' * 102)
            subtotal = (' ' + f"{bnmcode}".ljust(8) +
                        f"{bnm_bal:,.2f}".rjust(18) +
                        f"{bnm_iis:,.2f}".rjust(18) +
                        f"{bnm_net:,.2f}".rjust(18) +
                        f"{bnm_var:,.2f}".rjust(22) +
                        f"{bnm_prev:,.2f}".rjust(18))
            lines.append(subtotal)
            lines.append(' ')

    # Write to file
    with open(output_file, 'w', encoding='utf-8') as f:
        for line in lines:
            f.write(line + '\n')


def write_rcat_file(lnbr, macro_vars, output_file):
    """
    Write flat file output in fixed format
    """
    with open(output_file, 'wb') as f:
        # First record: date
        date_record = f"{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{macro_vars['REPTDAY']}\n"
        f.write(date_record.encode('utf-8'))

        # Data records
        for row in lnbr.sort(['BRANCH', 'RISKCAT', 'BNMCODE']).iter_rows(named=True):
            branch = f"{row['BRANCH']:03d}"
            riskcat = f"{row['RISKCAT']:03d}"
            bnmcode = f"{row['BNMCODE']:8s}"
            balance = f"{row['BALANCE']:015.2f}".replace('.', '')
            totiis = f"{row['TOTIIS']:015.2f}".replace('.', '')
            netbal = f"{row['NETBAL']:015.2f}".replace('.', '')

            record = f"{branch}{riskcat}{bnmcode}{balance}{totiis}{netbal}\n"
            f.write(record.encode('utf-8'))

        # EOF record
        f.write(b"EOF\n")


def read_prev_rcat_file(rcat_file):
    """
    Read previous month's RCAT file
    Returns: DataFrame with previous month data and date
    """
    if not rcat_file.exists():
        print(f"Warning: Previous RCAT file not found: {rcat_file}")
        return None, None

    records = []
    pdtedmy = None

    with open(rcat_file, 'rb') as f:
        lines = f.readlines()

        if len(lines) == 0:
            return None, None

        # First line: date
        first_line = lines[0].decode('utf-8').strip()
        if len(first_line) >= 8:
            # Extract date: YYYYMMDD
            pdtedmy = f"{first_line[6:8]}/{first_line[4:6]}/{first_line[2:4]}"

        # Process data lines
        for line in lines[1:]:
            line_str = line.decode('utf-8').strip()

            if line_str.startswith('EOF'):
                break

            if len(line_str) >= 51:
                try:
                    branch = int(line_str[0:3])
                    riskcat = int(line_str[4:7])
                    bnmcode = line_str[8:16].strip()
                    prevbal = float(line_str[17:32]) / 100  # Remove decimal
                    prevnetb = float(line_str[51:66]) / 100  # Remove decimal

                    records.append({
                        'BRANCH': branch,
                        'RISKCAT': riskcat,
                        'BNMCODE': bnmcode,
                        'PREVBAL': prevbal,
                        'PREVNETB': prevnetb
                    })
                except:
                    continue

    if len(records) == 0:
        return None, pdtedmy

    return pl.DataFrame(records), pdtedmy


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Main execution function"""
    try:
        print("Starting EIBMCOLR Report Generation...")
        print("=" * 80)

        # Step 1: Process REPTDATE
        print("\n1. Processing REPTDATE...")
        macro_vars = process_reptdate()
        print(f"   Report Date: {macro_vars['RDATE']}")
        print(f"   Week: {macro_vars['NOWK']}, Month: {macro_vars['REPTMON']}, Year: {macro_vars['REPTYEAR']}")

        # Step 2: Process loan data
        print("\n2. Processing loan data...")
        odac, lnac, macro_vars = process_loan_data(macro_vars)
        print(f"   OD accounts: {len(odac)}")
        print(f"   LN accounts: {len(lnac)}")

        # Step 3: Merge TOTIIS data
        print("\n3. Merging TOTIIS data...")
        lnac = merge_totiis(lnac, macro_vars)

        # Step 4: Combine OD and LN, calculate NETBAL
        print("\n4. Combining accounts and calculating net balance...")
        loan = pl.concat([odac, lnac]) if len(odac) > 0 and len(lnac) > 0 else (lnac if len(lnac) > 0 else odac)
        loan = loan.with_columns([
            (pl.col('BALANCE') - pl.col('TOTIIS')).alias('NETBAL')
        ])

        # Step 5: Merge branch data
        print("\n5. Merging branch data...")
        loan = merge_branch_data(loan)
        print(f"   Total loan records: {len(loan)}")

        # Step 6: Summarize by branch
        print("\n6. Summarizing by branch...")
        lnbr = loan.group_by(['BRANCH', 'RISKCAT', 'BNMCODE']).agg([
            pl.col('BALANCE').sum(),
            pl.col('TOTIIS').sum(),
            pl.col('NETBAL').sum()
        ]).sort(['BRANCH', 'RISKCAT', 'BNMCODE'])

        # Step 7: Generate report by branch
        print("\n7. Generating report by branch...")
        generate_report_by_branch(lnbr, macro_vars, REPORT1_FILE)
        print(f"   ✓ Report saved to {REPORT1_FILE}")

        # Step 8: Summarize for all loans
        print("\n8. Summarizing all loans...")
        ln = loan.group_by(['RISKCAT', 'BNMCODE', 'AMTIND']).agg([
            pl.col('BALANCE').sum(),
            pl.col('TOTIIS').sum(),
            pl.col('NETBAL').sum()
        ])

        # Add _TYPE_ column to simulate SAS PROC SUMMARY
        # _TYPE_=6 means grouped by RISKCAT and BNMCODE (no AMTIND)
        # _TYPE_=7 means grouped by all three variables
        ln_type6 = loan.group_by(['RISKCAT', 'BNMCODE']).agg([
            pl.col('BALANCE').sum(),
            pl.col('TOTIIS').sum(),
            pl.col('NETBAL').sum()
        ]).with_columns([
            pl.lit(6).alias('_TYPE_'),
            pl.lit('').alias('AMTIND')
        ])

        ln_type7 = ln.with_columns([
            pl.lit(7).alias('_TYPE_')
        ])

        ln = pl.concat([ln_type6, ln_type7])

        # Step 9: Generate summary reports
        print("\n9. Generating summary reports...")

        # All loans
        generate_summary_report(ln, 'SUMMARY REPORT FOR ALL LOANS', REPORT2_FILE, 6)
        print(f"   ✓ All loans summary saved to {REPORT2_FILE}")

        # Conventional loans
        generate_summary_report(ln, 'SUMMARY REPORT FOR CONVENTIONAL LOANS ONLY', REPORT3_FILE, 7, 'D')
        print(f"   ✓ Conventional loans summary saved to {REPORT3_FILE}")

        # Islamic loans
        generate_summary_report(ln, 'SUMMARY REPORT FOR ISLAMIC LOANS ONLY', REPORT4_FILE, 7, 'I')
        print(f"   ✓ Islamic loans summary saved to {REPORT4_FILE}")

        # Step 10: Write RCAT flat file
        print("\n10. Writing RCAT flat file...")
        write_rcat_file(lnbr, macro_vars, RCAT_OUTPUT_FILE)
        print(f"   ✓ RCAT file saved to {RCAT_OUTPUT_FILE}")

        # Step 11: Generate variance report
        print("\n11. Generating variance report...")
        lnpv, pdtedmy = read_prev_rcat_file(RCAT_PREV_FILE)

        if lnpv is not None:
            # Merge current and previous data
            lnmrg = lnbr.join(lnpv, on=['BRANCH', 'RISKCAT', 'BNMCODE'], how='outer')

            # Fill nulls with 0
            lnmrg = lnmrg.with_columns([
                pl.col('BALANCE').fill_null(0.0),
                pl.col('TOTIIS').fill_null(0.0),
                pl.col('NETBAL').fill_null(0.0),
                pl.col('PREVBAL').fill_null(0.0),
                pl.col('PREVNETB').fill_null(0.0)
            ])

            # Calculate variance
            lnmrg = lnmrg.with_columns([
                (pl.col('BALANCE') - pl.col('PREVBAL')).alias('VARBAL')
            ])

            # Summarize
            lnvar = lnmrg.group_by(['RISKCAT', 'BNMCODE', 'BRANCH']).agg([
                pl.col('BALANCE').sum(),
                pl.col('TOTIIS').sum(),
                pl.col('NETBAL').sum(),
                pl.col('VARBAL').sum(),
                pl.col('PREVNETB').sum()
            ]).sort(['RISKCAT', 'BNMCODE', 'BRANCH'])

            # Generate variance report
            generate_variance_report(lnvar, macro_vars, pdtedmy, REPORT5_FILE)
            print(f"   ✓ Variance report saved to {REPORT5_FILE}")
            print(f"   Previous month date: {pdtedmy}")
        else:
            print("   ! Skipped variance report (no previous data)")

        print("\n" + "=" * 80)
        print("✓ EIBMCOLR Report Generation Complete!")
        print(f"\nGenerated files:")
        print(f"  1. {REPORT1_FILE}")
        print(f"  2. {REPORT2_FILE}")
        print(f"  3. {REPORT3_FILE}")
        print(f"  4. {REPORT4_FILE}")
        if lnpv is not None:
            print(f"  5. {REPORT5_FILE}")
        print(f"  6. {RCAT_OUTPUT_FILE}")

    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
