# !/usr/bin/env python3
"""
Program: EIBMBPOS
            BR-DEP-POS (Branch Deposit Position Report)
Purpose: Generate branch-wise product position report including:
         - Deposit products (Savings, Current, Fixed Deposit)
         - PB Premium Club accounts
         - PB Telebanking subscribers
Report Options: NOCENTER YEARCUTOFF=1950 NODATE NONUMBER LS=132 PS=65
"""

import polars as pl
import duckdb
from datetime import datetime
from pathlib import Path

# ============================================================================
# CONFIGURATION - Define all paths early
# ============================================================================

# Input paths
INPUT_PATH_REPTDATE = "SAP_PBB_MNILN_0_REPTDATE.parquet"
INPUT_PATH_SAVING = "DEPOSIT_SAVING.parquet"
INPUT_PATH_CURRENT = "DEPOSIT_CURRENT.parquet"
INPUT_PATH_FD = "FD_FD.parquet"
INPUT_PATH_LNNOTE = "SAP_PBB_MNILN_0_LNNOTE.parquet"
INPUT_PATH_SLOAN = "SASDATA_LOAN{reptmon}{nowk}.parquet"  # Template, will be formatted
INPUT_PATH_KAPITI5 = "KAPITI5.txt"

# Output path
OUTPUT_PATH_REPORT = "BR_DEP_POS_REPORT.txt"

# Page configuration
PAGE_LENGTH = 65
LRECL = 133  # 132 + 1 for ASA carriage control

# Product codes
SAVING_PRODUCTS = {
    (200, 201): '1 PLUS SAVING ACCOUNTS',
    (202,): '2 YOUNG ACHIEVER S/A',
    (212,): '3 WISE SAVING ACCOUNTS',
    (203,): '4 50 PLUS SAVING ACCOUNTS',
    (205,): '5 BASIC SAVING ACCOUNTS',
    (206,): '6 BASIC 55 SAVING ACCOUNTS',
    (213,): '7 PB SAVELINK ACCOUNTS'
}

CURRENT_PRODUCTS_CREDIT = {
    (100, 102, 106, 180): '1A PLUS ACCOUNTS (CREDIT)'
}

CURRENT_PRODUCTS_DEBIT = {
    (100, 102, 106, 180): '1B PLUS ACCOUNTS (DEBIT)'
}

CURRENT_PRODUCTS_OTHER = {
    (150, 151, 152, 181): '2 ACE ACCOUNTS',
    (90,): '3 BASIC CURRENT ACOUNTS',
    (91,): '4 BASIC 55 CURRENT ACOUNTS',
    (156, 157, 158): '5 PB CURRENTLINK ACOUNTS'
}

FD_PRODUCTS = {
    'golden': [360, 361, 362, 363, 364, 365, 366, 460],
    'life': [320, 321, 322, 323, 324, 325, 326, 327, 328, 329, 330,
             331, 420, 421, 422, 423, 424, 425, 426, 427, 428, 429,
             430, 431],
    'plus': [400, 401, 402, 403, 404, 405, 406, 407, 408, 409,
             410, 411, 412, 413, 414, 415, 416, 417, 418, 419,
             600, 601, 602, 603, 604, 605, 606, 607, 608, 609,
             610, 611, 612, 613, 614, 615, 616, 617, 618, 619,
             620, 621, 622, 623, 624, 625, 626, 627, 628, 629,
             630, 631, 632, 633, 634, 635, 636, 637, 638, 639,
             300, 301, 302, 303, 304, 305, 306, 307, 308, 309,
             310, 311, 312, 313, 314, 315, 316, 317, 318, 319,
             373, 374, 375, 376, 377, 378, 379, 380, 381, 382,
             383, 384,
             500, 501, 502, 503, 504, 505, 506, 507, 508, 509,
             510, 511, 512, 513, 514, 515, 516, 517, 518, 519,
             520, 521, 522, 523, 524, 525, 526, 527, 528, 529,
             530, 531, 532, 533, 534, 535, 536, 537, 538, 539]
}

LOAN_TYPES = [110, 111, 112, 113, 114, 115, 116, 200, 201, 204, 205,
              209, 210, 211, 212, 214, 215, 225, 226, 227, 228, 230,
              231, 232, 233, 234]

PB_PREMIUM_CURRENT_PRODUCTS = [150, 151, 152, 100, 102, 160, 162]
PB_PREMIUM_USER3 = ['2', '4']


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def format_asa_line(line_content, carriage_control=' '):
    """Format a line with ASA carriage control character."""
    return f"{carriage_control}{line_content}"


def get_report_date_info():
    """
    Process REPTDATE to generate macro variables.
    """
    reptdate_df = pl.read_parquet(INPUT_PATH_REPTDATE)
    reptdate_row = reptdate_df.row(0, named=True)
    reptdate = reptdate_row['REPTDATE']

    day = reptdate.day

    # SELECT(DAY(REPTDATE))
    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'

    # Format dates
    reptyrs = f"{str(reptdate.year)[2:]}"
    reptyear = f"{reptdate.year:04d}"
    reptmon = f"{reptdate.month:02d}"
    reptday = f"{reptdate.day:02d}"
    rdate = f"{reptdate.day:02d}/{reptdate.month:02d}/{str(reptdate.year)[2:]}"

    return {
        'NOWK': nowk,
        'REPTYRS': reptyrs,
        'REPTYEAR': reptyear,
        'REPTMON': reptmon,
        'REPTDAY': reptday,
        'RDATE': rdate,
        'REPTDATE': reptdate
    }


def get_product_item(product, product_map):
    """Get ITEM description from product code using mapping."""
    for products, item in product_map.items():
        if product in products:
            return item
    return None


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def process_br_dep_pos():
    """Main processing function for BR-DEP-POS report."""
    print("=" * 80)
    print("BR-DEP-POS - Branch Deposit Position Report")
    print("=" * 80)

    # Get report date information
    print("Initializing report parameters...")
    macro_vars = get_report_date_info()
    rdate = macro_vars['RDATE']
    nowk = macro_vars['NOWK']
    reptmon = macro_vars['REPTMON']
    print(f"Report Date: {rdate}")
    print(f"Report Week: {nowk}")
    print()

    con = duckdb.connect()

    try:
        # ====================================================================
        # Process SAVING accounts
        # ====================================================================
        print("Processing SAVING accounts...")

        saving_df = con.execute(f"""
            SELECT * FROM read_parquet('{INPUT_PATH_SAVING}')
            WHERE OPENIND NOT IN ('B', 'C', 'P', 'Z')
        """).pl()

        # Process SAVING and PBSAVE
        saving_records = []
        pbsave_records = []

        for row in saving_df.iter_rows(named=True):
            product = row['PRODUCT']
            user3 = row.get('USER3', '')

            # Determine ITEM for regular saving
            item = get_product_item(product, SAVING_PRODUCTS)

            if item:
                saving_records.append({
                    **row,
                    'ITEM': item,
                    'CATG': '(A) SAVINGS ACCOUNTS',
                    'GROUP': '(1) DEPOSIT PRODUCTS'
                })

            # PB Premium Club members
            if user3 in PB_PREMIUM_USER3:
                pbsave_records.append({
                    **row,
                    'ITEM': '2 SAVING ACCOUNTS',
                    'CATG': '(A) CLUB MEMBERS ON :',
                    'GROUP': '(2) PB PREMIUM CLUB'
                })

        saving_df = pl.DataFrame(saving_records) if saving_records else pl.DataFrame()
        pbsave_df = pl.DataFrame(pbsave_records) if pbsave_records else pl.DataFrame()

        # ====================================================================
        # Process CURRENT accounts
        # ====================================================================
        print("Processing CURRENT accounts...")

        current_df = con.execute(f"""
            SELECT * FROM read_parquet('{INPUT_PATH_CURRENT}')
            WHERE OPENIND NOT IN ('B', 'C', 'P')
        """).pl()

        current_records = []
        pbcurr_records = []

        for row in current_df.iter_rows(named=True):
            product = row['PRODUCT']
            curbal = row.get('CURBAL', 0) or 0
            user3 = row.get('USER3', '')

            # Check for PLUS accounts (credit/debit)
            if product in CURRENT_PRODUCTS_CREDIT[(100, 102, 106, 180)]:
                if curbal < 0:
                    item = '1B PLUS ACCOUNTS (DEBIT)'
                else:
                    item = '1A PLUS ACCOUNTS (CREDIT)'

                current_records.append({
                    **row,
                    'ITEM': item,
                    'CATG': '(B) CURRENT ACCOUNTS',
                    'GROUP': '(1) DEPOSIT PRODUCTS'
                })
            else:
                # Check other products
                item = get_product_item(product, CURRENT_PRODUCTS_OTHER)
                if item:
                    current_records.append({
                        **row,
                        'ITEM': item,
                        'CATG': '(B) CURRENT ACCOUNTS',
                        'GROUP': '(1) DEPOSIT PRODUCTS'
                    })

            # PB Premium Club members
            if product in PB_PREMIUM_CURRENT_PRODUCTS and user3 in PB_PREMIUM_USER3:
                pbcurr_records.append({
                    **row,
                    'ITEM': '1 CURRENT ACCOUNTS',
                    'CATG': '(A) CLUB MEMBERS ON :',
                    'GROUP': '(2) PB PREMIUM CLUB'
                })

        current_df = pl.DataFrame(current_records) if current_records else pl.DataFrame()
        pbcurr_df = pl.DataFrame(pbcurr_records) if pbcurr_records else pl.DataFrame()

        # ====================================================================
        # Process FIXED DEPOSIT accounts
        # ====================================================================
        print("Processing FIXED DEPOSIT accounts...")

        fd_df = con.execute(f"""
            SELECT * FROM read_parquet('{INPUT_PATH_FD}')
            WHERE OPENIND NOT IN ('B', 'C', 'P')
        """).pl()

        # Categorize FD products
        fd_records = []
        for row in fd_df.iter_rows(named=True):
            intplan = row['INTPLAN']

            if intplan in FD_PRODUCTS['golden']:
                item = '3 PB GOLDEN 50 PLUS'
            elif intplan in FD_PRODUCTS['life']:
                item = '2 FIXED DEPOSIT LIFE'
            elif intplan in FD_PRODUCTS['plus']:
                item = '1 PLUS FIXED DEPOSIT'
            else:
                continue  # Skip if not in any category

            fd_records.append({
                **row,
                'PRODUCT': intplan,  # Rename INTPLAN to PRODUCT
                'ITEM': item,
                'CATG': '(C) FIXED DEPOSIT ACCOUNT',
                'GROUP': '(1) DEPOSIT PRODUCTS'
            })

        fd_df = pl.DataFrame(fd_records) if fd_records else pl.DataFrame()

        # Sort and aggregate FD by account
        if len(fd_df) > 0:
            fd_df = fd_df.sort(['BRANCH', 'ITEM', 'ACCTNO', 'CDNO'])

            # Aggregate by ACCTNO (similar to FIRST.ACCTNO / LAST.ACCTNO logic)
            fdsort_df = fd_df.groupby(['BRANCH', 'GROUP', 'CATG', 'ITEM', 'ACCTNO']).agg([
                pl.sum('CURBAL').alias('AMOUNT'),
                pl.count('CDNO').alias('NOOFCD')
            ])
        else:
            fdsort_df = pl.DataFrame()

        # ====================================================================
        # Process LOAN accounts for PB Premium Club
        # ====================================================================
        print("Processing LOAN accounts for PB Premium Club...")

        # Load LNNOTE
        loan_df = con.execute(f"""
            SELECT * FROM read_parquet('{INPUT_PATH_LNNOTE}')
            WHERE (REVERSED <> 'Y' OR REVERSED IS NULL)
            AND (PAIDIND <> 'P' OR PAIDIND IS NULL)
            AND NOTENO IS NOT NULL
            AND ((NOTENO < 100) OR (20000 <= NOTENO AND NOTENO <= 29999) OR
                 ((30000 <= NOTENO AND NOTENO <= 39999) AND FLAG1 = 'F'))
            AND LOANTYPE IN ({','.join(map(str, LOAN_TYPES))})
            AND (RISKRATE NOT IN ('1', '2', '3', '4') OR RISKRATE IS NULL)
            AND CUSTCODE IN (77, 78, 95, 96)
            AND ORGTYPE IN ('E', 'M', 'N', 'S')
        """).pl()

        # Remove duplicates
        loan_df = loan_df.unique(subset=['ACCTNO', 'NOTENO'], keep='first')
        if 'SECTOR' in loan_df.columns:
            loan_df = loan_df.drop('SECTOR')

        # Load SLOAN (SASDATA.LOAN{REPTMON}{NOWK})
        sloan_path = INPUT_PATH_SLOAN.format(reptmon=reptmon, nowk=nowk)

        if Path(sloan_path).exists():
            sloan_df = con.execute(f"""
                SELECT * FROM read_parquet('{sloan_path}')
                WHERE (PRODUCT IN (200, 201, 204, 205, 209, 210, 211, 212,
                                   214, 215, 225, 226, 227, 228, 230, 231,
                                   232, 233, 234) AND APPRLIMT >= 150000)
                OR (PRODUCT IN (110, 111, 112, 113, 114, 115, 116) AND
                    NETPROC >= 150000)
            """).pl()

            sloan_df = sloan_df.unique(subset=['ACCTNO', 'NOTENO'], keep='first')
            if 'SECTOR' in sloan_df.columns:
                sloan_df = sloan_df.drop('SECTOR')

            # Merge LOAN and SLOAN
            pbloan_df = loan_df.join(
                sloan_df,
                on=['ACCTNO', 'NOTENO'],
                how='inner'
            )
        else:
            print(f"Warning: SLOAN file not found: {sloan_path}")
            pbloan_df = pl.DataFrame()

        # Create PBLOANS
        if len(pbloan_df) > 0:
            pbloans_df = pbloan_df.with_columns([
                pl.lit('(2) PB PREMIUM CLUB').alias('GROUP'),
                pl.lit('(A) CLUB MEMBERS ON :').alias('CATG'),
                pl.lit('3 HOUSING LOANS').alias('ITEM'),
                pl.col('BALANCE').alias('LEDGBAL')
            ])
        else:
            pbloans_df = pl.DataFrame()

        # ====================================================================
        # Combine PB Premium datasets
        # ====================================================================
        print("Combining PB Premium datasets...")

        pbprem_dfs = []
        for df in [pbsave_df, pbcurr_df, pbloans_df]:
            if len(df) > 0:
                pbprem_dfs.append(df)

        if pbprem_dfs:
            pbprem_df = pl.concat(pbprem_dfs)
        else:
            pbprem_df = pl.DataFrame()

        # ====================================================================
        # Process KAPITI5 (Telebanking)
        # ====================================================================
        print("Processing KAPITI5 (Telebanking)...")

        kapiti5_records = []

        if Path(INPUT_PATH_KAPITI5).exists():
            with open(INPUT_PATH_KAPITI5, 'r') as f:
                for line in f:
                    if len(line) < 303:
                        continue

                    cardin = line[0:20].strip()
                    issuedt = line[20:28].strip()
                    expiredt = line[28:36].strip()
                    cardstat = line[36:40].strip()
                    servicen = line[40:48].strip()
                    services = line[48:52].strip()
                    lastlog = line[52:60].strip()
                    address1 = line[60:100].strip()
                    address2 = line[100:140].strip()
                    legalid = line[140:156].strip()
                    legaltyp = line[156:160].strip()
                    custname = line[160:200].strip()
                    remark1 = line[200:240].strip()
                    pbborpfb = line[200:203].strip()
                    branches = line[208:238].strip()
                    remark2 = line[240:280].strip()
                    acctno = line[280:300].strip()
                    accttype = line[300:302].strip()
                    priacind = line[302:303].strip()

                    # Extract branch number from BRANCHES
                    # RIGHT(COMPRESS(BRANCHES,'ABCDEFGHIJKLMNOPQRSTUVWXYZ ():/'))
                    import re
                    branci = re.sub(r'[ABCDEFGHIJKLMNOPQRSTUVWXYZ ():/]', '', branches).strip()

                    try:
                        branch = int(branci) if branci else 0
                    except:
                        branch = 0

                    # Filter: PBBORPFB = 'PBB' AND BRANCH < 500
                    if pbborpfb == 'PBB' and branch < 500:
                        kapiti5_records.append({
                            'CARDIN': cardin,
                            'BRANCH': branch,
                            'ACCTNO': acctno,
                            'AMT': 0.0,
                            'GROUP': '(3) SERVICES',
                            'CATG': '(A) PB TELEBANKING',
                            'ITEM': '1 SUBSCRIBERS'
                        })

        kapiti5_df = pl.DataFrame(kapiti5_records) if kapiti5_records else pl.DataFrame()

        # Remove duplicates by BRANCH and ACCTNO
        if len(kapiti5_df) > 0:
            kapiti5_df = kapiti5_df.sort(['BRANCH', 'ACCTNO'])

        # ====================================================================
        # Summarize all datasets
        # ====================================================================
        print("Summarizing datasets...")

        # PROC SUMMARY for SAVING
        if len(saving_df) > 0:
            saving_summary = saving_df.groupby(['BRANCH', 'GROUP', 'CATG', 'ITEM']).agg([
                pl.count('CURBAL').alias('NOACCT'),
                pl.sum('CURBAL').alias('AMOUNT')
            ])
        else:
            saving_summary = pl.DataFrame()

        # PROC SUMMARY for CURRENT
        if len(current_df) > 0:
            current_summary = current_df.groupby(['BRANCH', 'GROUP', 'CATG', 'ITEM']).agg([
                pl.count('CURBAL').alias('NOACCT'),
                pl.sum('CURBAL').alias('AMOUNT')
            ])
        else:
            current_summary = pl.DataFrame()

        # PROC SUMMARY for FD
        if len(fdsort_df) > 0:
            fd_summary = fdsort_df.groupby(['BRANCH', 'GROUP', 'CATG', 'ITEM']).agg([
                pl.count('ACCTNO').alias('NOACCT'),
                pl.sum('NOOFCD').alias('NOOFCD'),
                pl.sum('AMOUNT').alias('AMOUNT')
            ])
            # Add AMOUNT1 (same as AMOUNT for compatibility)
            fd_summary = fd_summary.with_columns([
                pl.col('AMOUNT').alias('AMOUNT1')
            ])
        else:
            fd_summary = pl.DataFrame()

        # PROC SUMMARY for PBPREM
        if len(pbprem_df) > 0:
            pbprem_summary = pbprem_df.groupby(['BRANCH', 'GROUP', 'CATG', 'ITEM']).agg([
                pl.count('CURBAL').alias('NOACCT'),
                pl.sum('CURBAL').alias('AMOUNT')
            ])
        else:
            pbprem_summary = pl.DataFrame()

        # PROC SUMMARY for TELEBANK
        if len(kapiti5_df) > 0:
            telebank_summary = kapiti5_df.groupby(['BRANCH', 'GROUP', 'CATG', 'ITEM']).agg([
                pl.count('AMT').alias('NOACCT'),
                pl.sum('AMT').alias('AMOUNT')
            ])
        else:
            telebank_summary = pl.DataFrame()

        # ====================================================================
        # Combine all summaries
        # ====================================================================
        print("Combining all summaries...")

        # Ensure all dataframes have the same columns
        required_cols = ['BRANCH', 'GROUP', 'CATG', 'ITEM', 'NOACCT', 'AMOUNT']

        all_summaries = []
        for df in [saving_summary, current_summary, pbprem_summary, telebank_summary]:
            if len(df) > 0:
                # Add NOOFCD column if not present
                if 'NOOFCD' not in df.columns:
                    df = df.with_columns([pl.lit(0).alias('NOOFCD')])
                all_summaries.append(df.select(required_cols + ['NOOFCD']))

        # FD summary already has NOOFCD
        if len(fd_summary) > 0:
            all_summaries.append(fd_summary.select(required_cols + ['NOOFCD']))

        if all_summaries:
            pdrdata_df = pl.concat(all_summaries)
        else:
            pdrdata_df = pl.DataFrame()

        # Sort
        if len(pdrdata_df) > 0:
            pdrdata_df = pdrdata_df.sort(['BRANCH', 'GROUP', 'CATG', 'ITEM'])

        # ====================================================================
        # Generate report
        # ====================================================================
        print("Generating report...")

        with open(OUTPUT_PATH_REPORT, 'w') as f:
            generate_report(f, pdrdata_df, rdate)

        print(f"Generated: {OUTPUT_PATH_REPORT}")
        print("BR-DEP-POS processing complete")

    finally:
        con.close()


def generate_report(f, df, rdate):
    """
    Generate formatted report output.
    Mimics the DATA WRITE step with HEADER=NEWPAGE.
    """

    if len(df) == 0:
        print("No data to report")
        return

    date_today = datetime.now()
    pagecnt = 0

    # Accumulators
    aaccnt = 0
    afdcnt = 0
    atotol = 0.0
    baccnt = 0
    bfdcnt = 0
    btotol = 0.0

    prev_branch = None
    prev_group = None
    prev_catg = None
    first_record = True

    for idx, row in enumerate(df.iter_rows(named=True)):
        branch = row['BRANCH']
        group = row['GROUP']
        catg = row['CATG']
        item = row['ITEM']
        noacct = row['NOACCT']
        noofcd = row.get('NOOFCD', 0)
        amount = row['AMOUNT']

        # IF FIRST.BRANCH
        if branch != prev_branch:
            # IF _N_ NE 1 THEN PUT _PAGE_
            if not first_record:
                # Write page eject before new branch
                pagecnt += 1
                write_page_header(f, branch, rdate, date_today, pagecnt)
            else:
                pagecnt += 1
                write_page_header(f, branch, rdate, date_today, pagecnt)
                first_record = False

        # IF FIRST.GROUP
        if group != prev_group or branch != prev_branch:
            line = f"{group}"
            f.write(format_asa_line(line.ljust(LRECL - 1), ' ') + '\n')
            f.write(format_asa_line(' ' * (LRECL - 1), ' ') + '\n')

        # IF FIRST.CATG
        if catg != prev_catg or group != prev_group or branch != prev_branch:
            line = f"    {catg}"
            f.write(format_asa_line(line.ljust(LRECL - 1), ' ') + '\n')
            f.write(format_asa_line(' ' * (LRECL - 1), ' ') + '\n')

        # IF ITEM = '3 HOUSING LOANS' THEN AMOUNT = 0.00
        if item == '3 HOUSING LOANS':
            amount = 0.00

        # Accumulate totals
        aaccnt += noacct
        afdcnt += noofcd
        atotol += amount
        baccnt += noacct
        bfdcnt += noofcd
        btotol += amount

        # Write detail line
        line = f"        {item:<34}{noacct:>7,}{noofcd:>24,}{amount:>18,.2f}"
        f.write(format_asa_line(line.ljust(LRECL - 1), ' ') + '\n')

        # Check if last record of branch
        is_last_branch = (idx == len(df) - 1 or
                          df.row(idx + 1, named=True)['BRANCH'] != branch)

        if is_last_branch:
            # Reset branch totals
            baccnt = 0
            bfdcnt = 0
            btotol = 0.0

        prev_branch = branch
        prev_group = group
        prev_catg = catg


def write_page_header(f, branch, rdate, date_today, pagecnt):
    """
    NEWPAGE: subroutine
    Write page header.
    """
    date_str = f"{date_today.day:02d}/{date_today.month:02d}/{str(date_today.year)[2:]}"

    # Line 1: Report name, branch, bank name, date
    line1 = f"REPORT NAME : BR-DEP-POS :  BRANCH : {branch:3d}{'':9}P U B L I C   B A N K   B E R H A D{'':34}DATE : {date_str}"
    f.write(format_asa_line(line1.ljust(LRECL - 1), '1') + '\n')

    # Line 2: Products position as at
    line2 = f"{'':50}PRODUCTS POSITION AS AT : {rdate}"
    f.write(format_asa_line(line2.ljust(LRECL - 1), ' ') + '\n')

    # Line 3: Column header line 1
    line3 = f"{'':38}NO. OF ACCOUNTS/"
    f.write(format_asa_line(line3.ljust(LRECL - 1), ' ') + '\n')

    # Line 4: Column headers
    line4 = f"     PRODUCT TYPES{'':20}MEMBERS/SUBSCRIBERS{'':6}NO OF RECEIPTS{'':8}AMOUNT OUTSTANDING (RM)"
    f.write(format_asa_line(line4.ljust(LRECL - 1), ' ') + '\n')

    # Line 5: Underlines (using overprint)
    line5_base = ' ' * (LRECL - 1)
    f.write(format_asa_line(line5_base, ' ') + '\n')

    # Overprint line with underscores
    line5_over = f"     {'_' * 13}{'':24}{'_' * 19}{'':7}{'_' * 14}{'':7}{'_' * 22}"
    f.write(format_asa_line(line5_over.ljust(LRECL - 1), '+') + '\n')

    # Blank line
    f.write(format_asa_line(' ' * (LRECL - 1), ' ') + '\n')


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    process_br_dep_pos()
