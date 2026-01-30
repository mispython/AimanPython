#!/usr/bin/env python3
"""
File Name: EIBMODLM
Report on Accounts with Overdraft Limits
Generates two reports:
1. Public Bank - Accounts with OD Limits (ODPLAN 100-105)
2. Public Islamic Bank - Accounts with CLF-i Limits (ODPLAN 106)
"""

import duckdb
from datetime import datetime
from pathlib import Path


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# Input paths - Public Bank
INPUT_PBB_REPTDATE = "input/deposit_reptdate.parquet"
INPUT_PBB_CURRENT = "input/deposit_current.parquet"
INPUT_PBB_OVERDFT = "input/odlimit_overdft.parquet"

# Input paths - Islamic Bank
INPUT_PIBB_REPTDATE = "input/deposit_reptdate_pibb.parquet"
INPUT_PIBB_CURRENT = "input/deposit_current_pibb.parquet"
INPUT_PIBB_OVERDFT = "input/odlimit_overdft_pibb.parquet"

# Output paths
OUTPUT_PBB_REPORT = "/mnt/user-data/outputs/pbb_odlimit_report.txt"
OUTPUT_PIBB_REPORT = "/mnt/user-data/outputs/pibb_odlimit_report.txt"

# Report configuration
PAGE_SIZE = 50  # PS=50 in OPTIONS


# ============================================================================
# INITIALIZE DUCKDB CONNECTION
# ============================================================================
con = duckdb.connect(database=':memory:')


# ============================================================================
# FUNCTION TO GENERATE REPORT
# ============================================================================

def generate_od_report(reptdate_file, current_file, overdft_file,
                       output_file, is_islamic=False, odplan_filter=None):
    """
    Generate overdraft limit report

    Args:
        reptdate_file: Path to report date parquet file
        current_file: Path to current accounts parquet file
        overdft_file: Path to overdraft parquet file
        output_file: Path to output report file
        is_islamic: Boolean indicating if this is Islamic bank report
        odplan_filter: List of ODPLAN codes or single value
    """

    print(f"\n{'=' * 70}")
    print(f"Generating {'Islamic Bank CLF-i' if is_islamic else 'Public Bank OD'} Limits Report")
    print(f"{'=' * 70}")


    # ========================================================================
    # STEP 1: Get Report Date
    # ========================================================================
    print("\nStep 1: Reading report date...")

    reptdate_df = con.execute(f"""
        SELECT * FROM read_parquet('{reptdate_file}')
        LIMIT 1
    """).fetchdf()

    reptdate = reptdate_df['REPTDATE'].iloc[0]
    RDATE = reptdate.strftime('%d/%m/%y')
    print(f"Report Date: {RDATE}")


    # ========================================================================
    # STEP 2: Process Current Accounts
    # ========================================================================
    print("\nStep 2: Processing current accounts...")

    # Build ODPLAN filter condition
    if isinstance(odplan_filter, list):
        odplan_condition = f"ODPLAN IN ({','.join(map(str, odplan_filter))})"
    else:
        odplan_condition = f"ODPLAN = {odplan_filter}"

    current = con.execute(f"""
        SELECT 
            ACCTNO,
            CASE 
                WHEN CURBAL < 0 THEN (-1) * CURBAL
                ELSE CURBAL
            END as BALANCE,
            CASE 
                WHEN CURBAL >= 0 THEN 'CR'
                ELSE NULL
            END as CRI
        FROM read_parquet('{current_file}')
        WHERE DEPTYPE IN ('D', 'N')
          AND APPRLIMT > 1
          AND {odplan_condition}
    """).df()

    con.register('current', current)
    print(f"Current accounts: {len(current):,}")


    # ========================================================================
    # STEP 3: Process Overdraft Data
    # ========================================================================
    print("\nStep 3: Processing overdraft data...")

    ovdr = con.execute(f"""
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
        FROM read_parquet('{overdft_file}')
        WHERE APPRLIMT > 1
          AND LMTTYPE IN ('Y', 'A')
    """).df()

    con.register('ovdr', ovdr)
    print(f"Overdraft records: {len(ovdr):,}")


    # ========================================================================
    # STEP 4: Add Row Counter and Pivot Limits (Up to 5 per Account)
    # ========================================================================
    print("\nStep 4: Pivoting limits (up to 5 per account)...")

    # Add row counter
    ovdr_with_counter = con.execute("""
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY ACCTNO ORDER BY LMTAMT DESC) as RCNT
        FROM ovdr
    """).df()

    con.register('ovdr_with_counter', ovdr_with_counter)

    # Filter to keep only first 5 limits per account
    ovdr_filtered = con.execute("""
        SELECT * FROM ovdr_with_counter
        WHERE RCNT <= 5
    """).df()

    con.register('ovdr_filtered', ovdr_filtered)

    # Pivot the limits into separate columns
    odmerg = con.execute("""
        SELECT 
            ACCTNO,
            MAX(BRANCH) as BRANCH,
            MAX(LMTBASER) as LMTBASER,
            MAX(NAME) as NAME,
            MAX(ODSTATUS) as ODSTATUS,
            MAX(APPRLIMT) as APPRLIMT,
            MAX(CASE WHEN RCNT = 1 THEN LMTAMT END) as LIMIT1,
            MAX(CASE WHEN RCNT = 1 THEN LMTRATE END) as RATE1,
            MAX(CASE WHEN RCNT = 1 THEN LMTCOLL END) as COLL1,
            MAX(CASE WHEN RCNT = 2 THEN LMTAMT END) as LIMIT2,
            MAX(CASE WHEN RCNT = 2 THEN LMTRATE END) as RATE2,
            MAX(CASE WHEN RCNT = 2 THEN LMTCOLL END) as COLL2,
            MAX(CASE WHEN RCNT = 3 THEN LMTAMT END) as LIMIT3,
            MAX(CASE WHEN RCNT = 3 THEN LMTRATE END) as RATE3,
            MAX(CASE WHEN RCNT = 3 THEN LMTCOLL END) as COLL3,
            MAX(CASE WHEN RCNT = 4 THEN LMTAMT END) as LIMIT4,
            MAX(CASE WHEN RCNT = 4 THEN LMTRATE END) as RATE4,
            MAX(CASE WHEN RCNT = 4 THEN LMTCOLL END) as COLL4,
            MAX(CASE WHEN RCNT = 5 THEN LMTAMT END) as LIMIT5,
            MAX(CASE WHEN RCNT = 5 THEN LMTRATE END) as RATE5,
            MAX(CASE WHEN RCNT = 5 THEN LMTCOLL END) as COLL5
        FROM ovdr_filtered
        GROUP BY ACCTNO
    """).df()

    con.register('odmerg', odmerg)
    print(f"Accounts with pivoted limits: {len(odmerg):,}")


    # ========================================================================
    # STEP 5: Merge Current with Overdraft
    # ========================================================================
    print("\nStep 5: Merging current accounts with overdraft data...")

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
            COALESCE(o.LIMIT1, 0) as LIMIT1,
            COALESCE(o.RATE1, 0.0) as RATE1,
            o.COLL1,
            COALESCE(o.LIMIT2, 0) as LIMIT2,
            COALESCE(o.RATE2, 0.0) as RATE2,
            o.COLL2,
            COALESCE(o.LIMIT3, 0) as LIMIT3,
            COALESCE(o.RATE3, 0.0) as RATE3,
            o.COLL3,
            COALESCE(o.LIMIT4, 0) as LIMIT4,
            COALESCE(o.RATE4, 0.0) as RATE4,
            o.COLL4,
            COALESCE(o.LIMIT5, 0) as LIMIT5,
            COALESCE(o.RATE5, 0.0) as RATE5,
            o.COLL5,
            (COALESCE(o.LIMIT1, 0) + COALESCE(o.LIMIT2, 0) + 
             COALESCE(o.LIMIT3, 0) + COALESCE(o.LIMIT4, 0) + 
             COALESCE(o.LIMIT5, 0)) as LIMITS,
            1 as NOACCT
        FROM current c
        INNER JOIN odmerg o ON c.ACCTNO = o.ACCTNO
    """).df()

    con.register('ovdrm', ovdrm)
    print(f"Merged records: {len(ovdrm):,}")


    # ========================================================================
    # STEP 6: Format Branch Code (BRN)
    # ========================================================================
    print("\nStep 6: Formatting branch codes...")

    brnref = con.execute("""
        SELECT *,
            CASE 
                WHEN BRANCH < 10 THEN '00' || CAST(BRANCH AS VARCHAR)
                WHEN BRANCH < 100 THEN '0' || CAST(BRANCH AS VARCHAR)
                ELSE CAST(BRANCH AS VARCHAR)
            END as BRN
        FROM ovdrm
        ORDER BY BRN, ACCTNO
    """).df()

    print(f"Final records with branch codes: {len(brnref):,}")


    # ========================================================================
    # STEP 7: Generate Report
    # ========================================================================
    print("\nStep 7: Generating report...")

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w') as f:
        # Report titles
        if is_islamic:
            title1 = 'P U B L I C   I S L A M I C  B A N K   B E R H A D'
            title2 = 'REPORT TITLE: ACCOUNTS WITH CLF-i LIMITS'
            od_label = 'CLF-i'
        else:
            title1 = 'P U B L I C   B A N K   B E R H A D'
            title2 = 'REPORT TITLE: ACCOUNTS WITH OD LIMITS'
            od_label = 'OD'

        # Iterate by branch (BRN)
        current_brn = None
        brn_apprlimt = 0.0
        brn_limits = 0.0
        brn_noacct = 0
        line_count = 0

        for idx, row in brnref.iterrows():
            # Check for branch break
            if row['BRN'] != current_brn:
                # Print previous branch subtotal
                if current_brn is not None:
                    f.write(' ')
                    f.write('\n')
                    f.write(' ')
                    f.write(' ' * 25 + '-' * 49 + '\n')
                    f.write(' ')
                    f.write(' ' * 25 + f"TOTAL APPROVED LIMITS  = {brn_apprlimt:>20,.2f}\n")
                    f.write(' ')
                    f.write('\n')
                    f.write(' ')
                    f.write(' ' * 25 + f"TOTAL ACCOUNTS         = {brn_noacct:>6}\n")
                    f.write(' ')
                    f.write('\n')
                    f.write(' ')
                    f.write(' ' * 25 + f"TOTAL OPERATIVE LIMITS = {brn_limits:>20,.2f}\n")
                    f.write(' ')
                    f.write(' ' * 25 + '-' * 49 + '\n')
                    f.write(' ')
                    f.write('\n')

                    line_count = 0  # Reset for new page

                # Start new branch - print header
                current_brn = row['BRN']
                brn_apprlimt = 0.0
                brn_limits = 0.0
                brn_noacct = 0

                # Page header
                f.write('1')  # New page
                f.write(f"  {title1}\n")
                f.write(' ')
                f.write(f"  {title2}\n")
                f.write(' ')
                f.write(f"  REPORT AS AT {RDATE}\n")
                f.write(' ')
                f.write('  \n')

                # Column headers (compact format to fit in 133 chars)
                f.write(' ')
                f.write(f"BRN ACCOUNT NO NAME OF CUSTOMER          BASE {od_label:>5} OUSTANDING      ")
                f.write(
                    f"APPROVED        LIMIT1      RATE1 COLL1    LIMIT2      RATE2 COLL2    LIMIT3      RATE3 COLL3\n")

                f.write(' ')
                f.write(f"                                          RATE ST   BALANCE          ")
                f.write(f"LIMIT                                                                     \n")

                f.write(' ')
                f.write('-' * 132 + '\n')

                line_count = 8

            # Print detail line
            line = ' '

            # BRN (3 chars)
            line += f"{row['BRN'][:3]:<3} "

            # ACCTNO (10 chars)
            acctno = int(row['ACCTNO']) if row['ACCTNO'] else 0
            line += f"{acctno:<10} "

            # NAME (25 chars)
            name = str(row['NAME'])[:25] if row['NAME'] else ''
            line += f"{name:<25} "

            # LMTBASER (5.2 format)
            lmtbaser = float(row['LMTBASER']) if row['LMTBASER'] else 0.0
            line += f"{lmtbaser:>5.2f} "

            # ODSTATUS (2-5 chars depending on bank)
            odstatus = str(row['ODSTATUS'])[:5] if row['ODSTATUS'] else ''
            line += f"{odstatus:<5} "

            # BALANCE (12.2 with comma)
            balance = float(row['BALANCE']) if row['BALANCE'] else 0.0
            line += f"{balance:>12,.2f} "

            # CRI (2 chars)
            cri = str(row['CRI'])[:2] if row['CRI'] else ''
            line += f"{cri:<2} "

            # APPRLIMT (12.2 with comma)
            apprlimt = float(row['APPRLIMT']) if row['APPRLIMT'] else 0.0
            line += f"{apprlimt:>12,.2f} "

            # LIMIT1 (12.2 with comma) - truncated display due to space
            limit1 = float(row['LIMIT1']) if row['LIMIT1'] else 0.0
            line += f"{limit1:>11,.2f} "

            # RATE1 (5.2)
            rate1 = float(row['RATE1']) if row['RATE1'] else 0.0
            line += f"{rate1:>5.2f} "

            # COLL1 (5 chars)
            coll1 = str(row['COLL1'])[:5] if row['COLL1'] else ''
            line += f"{coll1:<5} "

            # Add newline and continue on next line for remaining limits if needed
            line += '\n'
            f.write(line)

            # Additional line for LIMIT2-5 if space permits (abbreviated for 133 char limit)
            if row['LIMIT2'] and row['LIMIT2'] > 0:
                line2 = ' ' + ' ' * 105  # Indent to align
                limit2 = float(row['LIMIT2']) if row['LIMIT2'] else 0.0
                rate2 = float(row['RATE2']) if row['RATE2'] else 0.0
                coll2 = str(row['COLL2'])[:5] if row['COLL2'] else ''
                line2 += f"{limit2:>11,.2f} {rate2:>5.2f} {coll2:<5}\n"
                f.write(line2)
                line_count += 1

            # Accumulate branch totals
            brn_apprlimt += apprlimt
            brn_limits += float(row['LIMITS']) if row['LIMITS'] else 0.0
            brn_noacct += 1

            line_count += 1

        # Print final branch subtotal
        if current_brn is not None:
            f.write(' ')
            f.write('\n')
            f.write(' ')
            f.write(' ' * 25 + '-' * 49 + '\n')
            f.write(' ')
            f.write(' ' * 25 + f"TOTAL APPROVED LIMITS  = {brn_apprlimt:>20,.2f}\n")
            f.write(' ')
            f.write('\n')
            f.write(' ')
            f.write(' ' * 25 + f"TOTAL ACCOUNTS         = {brn_noacct:>6}\n")
            f.write(' ')
            f.write('\n')
            f.write(' ')
            f.write(' ' * 25 + f"TOTAL OPERATIVE LIMITS = {brn_limits:>20,.2f}\n")
            f.write(' ')
            f.write(' ' * 25 + '-' * 49 + '\n')
            f.write(' ')
            f.write('\n')

    print(f"Report saved: {output_file}")

    # Statistics
    print(f"\nReport Statistics:")
    print(f"  Total Accounts: {len(brnref):,}")
    print(f"  Total Branches: {brnref['BRN'].nunique()}")
    if len(brnref) > 0:
        total_apprlimt = brnref['APPRLIMT'].sum()
        total_limits = brnref['LIMITS'].sum()
        print(f"  Total Approved Limits: {total_apprlimt:,.2f}")
        print(f"  Total Operative Limits: {total_limits:,.2f}")


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
        reptdate_file=INPUT_PBB_REPTDATE,
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
        reptdate_file=INPUT_PIBB_REPTDATE,
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
print("\n" + "=" * 70)
print("REPORT GENERATION COMPLETE")
print("=" * 70)
print(f"\nGenerated Reports:")
print(f"  1. Public Bank OD Limits: {OUTPUT_PBB_REPORT}")
print(f"  2. Islamic Bank CLF-i Limits: {OUTPUT_PIBB_REPORT}")
print("\nConversion complete!")

# Close DuckDB connection
con.close()
