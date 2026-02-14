# !/usr/bin/env python3
"""
Program: EIFMNP07
Purpose: STATISTICS ON ASSET QUALITY - MOVEMENTS IN NPL

Date: 03.04.98
Modified: ESMR 2004-720, 2004-579, 2006-1048

Process NPL accounts and calculate asset quality statistics including:
- Net balance movements (previous year vs current)
- New NPL during current year
- Accrued interest
- Recoveries and write-offs
- NPL classified as performing
- Classification by loan type, risk category, and branch
"""

import polars as pl
from datetime import datetime
from pathlib import Path

# Setup paths
INPUT_NPL_REPTDATE = "NPL.REPTDATE.parquet"
INPUT_NPL_WAQ = "NPL.WAQ.parquet"
OUTPUT_NPL_AQ = "NPL.AQ.parquet"
OUTPUT_REPORT_SUMMARY = "NPL_AQ_SUMMARY_REPORT.txt"
OUTPUT_REPORT_DETAIL = "NPL_AQ_DETAIL_REPORT.txt"

# Global variables
REPTMON = None
RDATE = None
INPUT_NPL_LOAN = None


def format_loantype(loantype):
    """Format loan type description (PROC FORMAT LNTYP)"""
    if loantype in [128, 130, 983]:
        return "HPD AITAB"
    elif loantype in [700, 705, 993, 996, 380, 381, 720, 725]:
        return "HPD CONVENTIONAL"
    elif 200 <= loantype <= 299:
        return "HOUSING LOANS"
    else:
        return "OTHERS"


def classify_risk(days, borstat, user5):
    """Classify risk category based on days overdue and borrower status"""
    if days > 364 or borstat == 'W':
        return "BAD"
    elif days > 273:
        return "DOUBTFUL"
    elif days > 182:
        return "SUBSTANDARD 2"
    elif days < 90 and user5 == 'N':
        return "SUBSTANDARD-1"
    else:
        return "SUBSTANDARD-1"


def safe_float(value):
    """Safely convert value to float, returning 0 if None"""
    return float(value) if value is not None else 0.0


def safe_int(value):
    """Safely convert value to int, returning 0 if None"""
    return int(value) if value is not None else 0


def write_summary_report(df, output_file, rdate):
    """Write summary tabulation report with ASA carriage control"""

    summary = df.group_by(["LOANTYP", "RISK", "BRANCH"]).agg([
        pl.count("ACCTNO").alias("N"),
        pl.sum("CURBALP").alias("CURBALP"),
        pl.sum("CURBAL").alias("CURBAL"),
        pl.sum("NETBALP").alias("NETBALP"),
        pl.sum("NEWNPL").alias("NEWNPL"),
        pl.sum("ACCRINT").alias("ACCRINT"),
        pl.sum("RECOVER").alias("RECOVER"),
        pl.sum("PL").alias("PL"),
        pl.sum("NPLW").alias("NPLW"),
        pl.sum("NPL").alias("NPL"),
        pl.sum("ADJUST").alias("ADJUST")
    ]).sort(["LOANTYP", "RISK", "BRANCH"])

    with open(output_file, 'w') as f:
        f.write('1')
        f.write(f"{'PUBLIC BANK - (NPL FROM 3 MONTHS & ABOVE)':^132}\n")
        f.write(' ')
        f.write(f"STATISTICS ON ASSET QUALITY - MOVEMENTS IN NPL {rdate}\n")
        f.write(' \n')

        f.write(' ')
        f.write(f"{'RISK/BRANCH':<29}{'NO OF':>10}{'PREV YEAR':>17}{'RPT DATE':>17}{'NET BAL':>17}\n")
        f.write(' ')
        f.write(f"{'':<29}{'ACCOUNT':>10}{'BAL (UHC)':>17}{'BAL (UHC)':>17}{'PREV YR (A)':>17}\n")
        f.write(' ')
        f.write('-' * 132 + '\n')

        current_loantyp = None

        for row in summary.iter_rows(named=True):
            if current_loantyp != row['LOANTYP']:
                if current_loantyp is not None:
                    f.write(' \n')
                current_loantyp = row['LOANTYP']
                f.write(' ')
                f.write(f"{current_loantyp}\n")

            f.write(' ')
            risk_branch = f"  {row['RISK']:<15}{row['BRANCH']:<12}"
            f.write(f"{risk_branch:<29}{row['N']:>10,}{row['CURBALP']:>17,.2f}")
            f.write(f"{row['CURBAL']:>17,.2f}{row['NETBALP']:>17,.2f}\n")

        f.write(' ')
        f.write('=' * 132 + '\n')


def write_detail_report(df, output_file, rdate):
    """Write detailed report with ASA carriage control"""

    df_sorted = df.sort(["LOANTYP", "BRANCH", "RISK", "DAYS", "ACCTNO"])

    with open(output_file, 'w') as f:
        lines_on_page = 0
        page_num = 1
        current_branch = None
        current_risk = None

        def write_header():
            nonlocal lines_on_page
            f.write('1')
            f.write(f"{'PUBLIC BANK - (NPL FROM 3 MONTHS & ABOVE)':<100}Page {page_num}\n")
            f.write(' ')
            f.write(f"STATISTICS ON ASSET QUALITY - MOVEMENTS IN NPL {rdate}\n")
            f.write(' \n')
            f.write(' ')
            f.write(f"{'ACCTNO':<15}{'NAME':<25}{'DAYS':>6}{'NETBALP':>15}{'NPL':>15}\n")
            f.write(' ')
            f.write('-' * 100 + '\n')
            lines_on_page = 6

        write_header()

        for row_dict in df_sorted.iter_rows(named=True):
            if lines_on_page >= 55:
                page_num += 1
                write_header()

            if current_branch != row_dict['BRANCH']:
                if current_branch is not None:
                    f.write(' \n')
                    lines_on_page += 1
                current_branch = row_dict['BRANCH']
                current_risk = None
                f.write(' ')
                f.write(f"BRANCH: {current_branch}\n")
                lines_on_page += 1

            if current_risk != row_dict['RISK']:
                current_risk = row_dict['RISK']
                f.write(' ')
                f.write(f"  RISK: {current_risk}\n")
                lines_on_page += 1

            f.write(' ')
            acctno = str(row_dict['ACCTNO'])[:15]
            name = str(row_dict.get('NAME', ''))[:25]
            days = safe_int(row_dict['DAYS'])
            netbalp = safe_float(row_dict['NETBALP'])
            npl = safe_float(row_dict['NPL'])

            f.write(f"{acctno:<15}{name:<25}{days:>6}{netbalp:>15,.2f}{npl:>15,.2f}\n")
            lines_on_page += 1

        f.write(' ')
        f.write('=' * 100 + '\n')


def process_existing_npl(df_loanwoff, reptdate, styr):
    """
    Calculate statistics for existing NPL accounts (LOAN1 equivalent)
    DATA LOAN1 processing logic
    """

    results = []
    stmth = 1

    for row_dict in df_loanwoff.filter(pl.col("EXIST") == "Y").iter_rows(named=True):

        writeoff = row_dict.get('WRITEOFF', 'N')
        wdownind = row_dict.get('WDOWNIND', '')
        borstat = row_dict.get('BORSTAT', '')

        if writeoff == 'Y' and wdownind != 'Y':
            borstat = 'W'

        curbal = safe_float(row_dict.get('CURBAL'))
        days = safe_int(row_dict.get('DAYS'))
        user5 = row_dict.get('USER5')
        loantype = safe_int(row_dict.get('LOANTYPE'))

        accrint = 0.0
        uhc = 0.0
        oi = 0.0
        recover = 0.0
        profit_loss = 0.0
        newnpl = 0.0

        # Calculate ADJUST
        feeamt = safe_float(row_dict.get('FEEAMT'))
        feetot2 = safe_float(row_dict.get('FEETOT2'))
        adjust = feeamt - feetot2

        # Check if account becomes performing
        if days < 90 and borstat in [' ', 'A', 'C', 'S', 'T', 'Y'] and curbal >= 0 and \
                user5 != 'N' or loantype in [983, 993]:
            netbalp = safe_float(row_dict.get('NETBALP'))
            profit_loss = netbalp
            if days == 0 and curbal == 0:
                recover = netbalp
                profit_loss = 0
        else:
            # Calculate interest components
            if loantype in [380, 381]:
                oi = feeamt
            else:
                oi = feeamt

            accrint = safe_float(row_dict.get('FEEYTD'))

            curbalp = safe_float(row_dict.get('CURBALP'))
            uhcp = safe_float(row_dict.get('UHCP'))

            if borstat == 'F':
                curbalp = curbalp - uhcp

            termchg = safe_float(row_dict.get('TERMCHG'))
            bldate = row_dict.get('BLDATE')
            issdte = row_dict.get('ISSDTE')
            earnterm = safe_int(row_dict.get('EARNTERM'))

            # Calculate ACCRINT
            if termchg > 0 or (user5 == 'N' and loantype not in [983, 993]):
                if bldate and issdte:
                    remmth1 = earnterm - ((bldate.year - issdte.year) * 12 + bldate.month - issdte.month + 1)
                    remmth2 = earnterm - ((reptdate.year - issdte.year) * 12 + reptdate.month - issdte.month + 1)
                    remmths = earnterm - ((styr - issdte.year) * 12 + stmth - issdte.month + 1)

                    if remmth2 < 0:
                        remmth2 = 0

                    remmth1 = remmth1 - 1

                    if remmths >= remmth2:
                        for remmth in range(remmths, remmth2 - 1, -1):
                            accrint += 2 * (remmth + 1) * termchg / (earnterm * (earnterm + 1))

                    if remmth2 > 0:
                        uhc = remmth2 * (remmth2 + 1) * termchg / (earnterm * (earnterm + 1))

            # Calculate RECOVER and NPL
            netbalp = safe_float(row_dict.get('NETBALP'))
            feepdytd = safe_float(row_dict.get('FEEPDYTD'))

            if borstat == 'W':
                nplw = netbalp
            else:
                recover = curbalp - curbal + feepdytd

            if recover < 0:
                curbalp = curbalp - recover
                recover = 0

            npl = curbal - uhc + oi

            if borstat == 'W':
                npl = 0

        ntbrch = safe_int(row_dict.get('NTBRCH'))
        branch = f"{ntbrch:03d}"
        loantyp = format_loantype(loantype)

        netbalp = safe_float(row_dict.get('NETBALP'))
        curbalp = safe_float(row_dict.get('CURBALP'))
        nplw = 0.0

        # Handle writeoff and special cases
        if writeoff == 'Y' or loantype in [983, 993]:
            accrint = safe_float(row_dict.get('WACCRINT'))
            newnpl = safe_float(row_dict.get('WNEWNPL'))
            adjust = 0

            if wdownind != 'Y':
                recover = safe_float(row_dict.get('WRECOVER'))
                profit_loss = 0
                npl = 0
                nplw = netbalp + newnpl + accrint - recover - profit_loss
            else:
                nplw = safe_float(row_dict.get('WNPLW'))
                npl = netbalp + newnpl + accrint - recover - nplw - profit_loss

        result = {
            'BRANCH': branch,
            'ACCTNO': row_dict.get('ACCTNO'),
            'NOTENO': row_dict.get('NOTENO'),
            'NAME': row_dict.get('NAME'),
            'DAYS': days,
            'CURBALP': curbalp,
            'CURBAL': curbal,
            'NETBALP': netbalp,
            'NEWNPL': newnpl,
            'ACCRINT': accrint,
            'RECOVER': recover,
            'PL': profit_loss,
            'NPLW': nplw,
            'NPL': npl,
            'LOANTYPE': loantype,
            'LOANTYP': loantyp,
            'OIP': row_dict.get('OIP'),
            'ADJUST': adjust,
            'USER5': user5,
            'BORSTAT': borstat,
            'COSTCTR': row_dict.get('COSTCTR'),
            'PENDBRH': row_dict.get('PENDBRH')
        }

        results.append(result)

    return pl.DataFrame(results) if results else pl.DataFrame()


def process_current_npl(df_loanwoff, reptdate, styr):
    """
    Calculate statistics for current NPL accounts (LOAN2 equivalent)
    DATA LOAN2 processing logic
    """

    results = []

    for row_dict in df_loanwoff.filter(pl.col("EXIST") != "Y").iter_rows(named=True):

        writeoff = row_dict.get('WRITEOFF', 'N')
        wdownind = row_dict.get('WDOWNIND', '')
        borstat = row_dict.get('BORSTAT', '')

        if writeoff == 'Y' and wdownind != 'Y':
            borstat = 'W'

        uhc = 0.0
        oi = 0.0
        adjust = 0.0

        bldate = row_dict.get('BLDATE')
        termchg = safe_float(row_dict.get('TERMCHG'))
        user5 = row_dict.get('USER5')
        loantype = safe_int(row_dict.get('LOANTYPE'))
        issdte = row_dict.get('ISSDTE')
        earnterm = safe_int(row_dict.get('EARNTERM'))

        # Calculate UHC
        if bldate is not None and bldate > datetime(1960, 1, 1).date() and termchg > 0 or user5 == 'N':
            if issdte and reptdate:
                remmth2 = earnterm - ((reptdate.year - issdte.year) * 12 + reptdate.month - issdte.month + 1)

                if remmth2 < 0:
                    remmth2 = 0

                if remmth2 > 0:
                    uhc = remmth2 * (remmth2 + 1) * termchg / (earnterm * (earnterm + 1))
        else:
            if issdte and reptdate:
                remmth2 = earnterm - ((reptdate.year - issdte.year) * 12 + reptdate.month - issdte.month + 1)
                if remmth2 > 0:
                    uhc = remmth2 * (remmth2 + 1) * termchg / (earnterm * (earnterm + 1))

        # Calculate OI
        feeamt = safe_float(row_dict.get('FEEAMT'))
        if loantype in [380, 381]:
            oi = feeamt
        else:
            oi = feeamt

        curbal = safe_float(row_dict.get('CURBAL'))
        newnpl = curbal - uhc + oi
        npl = newnpl

        ntbrch = safe_int(row_dict.get('NTBRCH'))
        branch = f"{ntbrch:03d}"
        loantyp = format_loantype(loantype)

        netbalp = safe_float(row_dict.get('NETBALP'))
        curbalp = safe_float(row_dict.get('CURBALP'))
        accrint = 0.0
        recover = 0.0
        profit_loss = 0.0
        nplw = 0.0

        # Handle writeoff and special cases
        if writeoff == 'Y' or loantype in [983, 993]:
            accrint = safe_float(row_dict.get('WACCRINT'))
            newnpl = safe_float(row_dict.get('WNEWNPL'))
            adjust = 0

            if wdownind != 'Y':
                recover = safe_float(row_dict.get('WRECOVER'))
                profit_loss = 0
                npl = 0
                nplw = netbalp + newnpl + accrint - recover - profit_loss
            else:
                nplw = safe_float(row_dict.get('WNPLW'))
                npl = netbalp + newnpl + accrint - recover - nplw - profit_loss

        result = {
            'BRANCH': branch,
            'ACCTNO': row_dict.get('ACCTNO'),
            'NOTENO': row_dict.get('NOTENO'),
            'NAME': row_dict.get('NAME'),
            'DAYS': safe_int(row_dict.get('DAYS')),
            'CURBALP': curbalp,
            'CURBAL': curbal,
            'NETBALP': netbalp,
            'NEWNPL': newnpl,
            'ACCRINT': accrint,
            'RECOVER': recover,
            'PL': profit_loss,
            'NPLW': nplw,
            'NPL': npl,
            'LOANTYPE': loantype,
            'LOANTYP': loantyp,
            'OIP': row_dict.get('OIP'),
            'ADJUST': adjust,
            'USER5': user5,
            'BORSTAT': borstat,
            'COSTCTR': row_dict.get('COSTCTR'),
            'PENDBRH': row_dict.get('PENDBRH')
        }

        results.append(result)

    return pl.DataFrame(results) if results else pl.DataFrame()


def main():
    """
    Main processing function for EIFMNP07
    """
    global REPTMON, RDATE, INPUT_NPL_LOAN

    print("EIFMNP07 - NPL Asset Quality Statistics Processing")
    print("=" * 70)

    # Read REPTDATE
    df_reptdate = pl.read_parquet(INPUT_NPL_REPTDATE)
    reptdate = df_reptdate['REPTDATE'][0]
    reptmon = f"{reptdate.month:02d}"

    rdate = reptdate.strftime("%d %B %Y").upper()

    REPTMON = reptmon
    RDATE = rdate

    INPUT_NPL_LOAN = f"NPL.LOAN{reptmon}.parquet"

    print(f"Reporting Month: {reptmon}")
    print(f"Reporting Date: {rdate}")
    print("=" * 70)

    # Read input files
    print("\nReading input files...")
    df_loan_month = pl.read_parquet(INPUT_NPL_LOAN).sort("ACCTNO")
    df_waq = pl.read_parquet(INPUT_NPL_WAQ).sort("ACCTNO")

    print(f"LOAN records: {len(df_loan_month):,}")
    print(f"WAQ records: {len(df_waq):,}")

    # Merge LOAN with WAQ
    df_waq_select = df_waq.select([c for c in df_waq.columns if c not in ['NOTENO', 'NTBRCH']])
    df_waq_select = df_waq_select.with_columns(pl.lit('Y').alias('_WRITEOFF_FLAG'))

    df_loanwoff = df_loan_month.join(df_waq_select, on="ACCTNO", how="left")

    # Set WRITEOFF flag and adjust fields
    df_loanwoff = df_loanwoff.with_columns([
        pl.when(pl.col("_WRITEOFF_FLAG") == 'Y')
        .then(pl.lit('Y'))
        .otherwise(pl.lit('N'))
        .alias('WRITEOFF'),

        pl.when(pl.col("LOANTYPE").is_in([983, 993]))
        .then(pl.lit('N'))
        .otherwise(pl.col("WDOWNIND"))
        .alias('WDOWNIND'),

        pl.when((pl.col("EARNTERM") == 0) | pl.col("EARNTERM").is_null())
        .then(pl.col("NOTETERM"))
        .otherwise(pl.col("EARNTERM"))
        .alias('EARNTERM')
    ]).drop('_WRITEOFF_FLAG')

    print(f"Merged LOANWOFF records: {len(df_loanwoff):,}")

    # Process existing NPL accounts (LOAN1)
    styr = reptdate.year

    print("\nProcessing existing NPL accounts...")
    df_loan1 = process_existing_npl(df_loanwoff, reptdate, styr)
    print(f"Existing NPL accounts: {len(df_loan1):,}")

    # Process current NPL accounts (LOAN2)
    print("Processing current NPL accounts...")
    df_loan2 = process_current_npl(df_loanwoff, reptdate, styr)
    print(f"Current NPL accounts: {len(df_loan2):,}")

    # Combine LOAN1 and LOAN2
    print("\nCombining existing and current NPL accounts...")
    df_loan = pl.concat([df_loan1, df_loan2])

    # Add RISK classification
    df_loan = df_loan.with_columns(
        pl.struct(['DAYS', 'BORSTAT', 'USER5'])
        .map_elements(lambda x: classify_risk(x['DAYS'], x['BORSTAT'], x['USER5']), return_dtype=pl.Utf8)
        .alias('RISK')
    )

    # Fill nulls with 0 for numeric columns
    numeric_cols = ['NETBALP', 'NEWNPL', 'ACCRINT', 'RECOVER', 'PL', 'NPLW', 'NPL']
    for col in numeric_cols:
        if col in df_loan.columns:
            df_loan = df_loan.with_columns(pl.col(col).fill_null(0))

    # Add check column for discrepancies
    df_loan = df_loan.with_columns(
        (pl.col('NETBALP') + pl.col('NEWNPL') + pl.col('ACCRINT') -
         pl.col('RECOVER') - pl.col('PL') - pl.col('NPLW')).alias('CHKNPL')
    )

    # Filter by COSTCTR
    df_loan = df_loan.filter(
        ((pl.col("COSTCTR") < 3000) | (pl.col("COSTCTR") > 3999)) &
        (~pl.col("COSTCTR").is_in([4043, 4048])) &
        (pl.col("COSTCTR").is_not_null())
    )

    print(f"Combined records after filtering: {len(df_loan):,}")

    # Write output dataset
    print(f"\nWriting output file...")
    df_loan.write_parquet(OUTPUT_NPL_AQ)
    print(f"  {OUTPUT_NPL_AQ} ({len(df_loan):,} records)")

    # Generate reports
    print("\nGenerating reports...")
    write_summary_report(df_loan, OUTPUT_REPORT_SUMMARY, rdate)
    print(f"  {OUTPUT_REPORT_SUMMARY}")

    write_detail_report(df_loan, OUTPUT_REPORT_DETAIL, rdate)
    print(f"  {OUTPUT_REPORT_DETAIL}")

    # Display summary statistics
    if len(df_loan) > 0:
        print("\n" + "=" * 70)
        print("SUMMARY STATISTICS")
        print("=" * 70)

        total_netbalp = df_loan["NETBALP"].sum()
        total_newnpl = df_loan["NEWNPL"].sum()
        total_accrint = df_loan["ACCRINT"].sum()
        total_recover = df_loan["RECOVER"].sum()
        total_pl = df_loan["PL"].sum()
        total_nplw = df_loan["NPLW"].sum()
        total_npl = df_loan["NPL"].sum()

        print(f"Net Balance Previous Year (A): {total_netbalp:,.2f}")
        print(f"New NPL During Current Year (B): {total_newnpl:,.2f}")
        print(f"Accrued Interest (C): {total_accrint:,.2f}")
        print(f"Recoveries (D): {total_recover:,.2f}")
        print(f"NPL Classified as Performing (E): {total_pl:,.2f}")
        print(f"NPL Written-Off (F): {total_nplw:,.2f}")
        print(f"NPL as at End of Report Date: {total_npl:,.2f}")
        print(
            f"Formula Check (A+B+C-D-E-F): {total_netbalp + total_newnpl + total_accrint - total_recover - total_pl - total_nplw:,.2f}")

        # Count by risk category
        risk_counts = df_loan.group_by("RISK").agg(
            pl.count().alias("count"),
            pl.sum("NPL").alias("total_npl")
        ).sort("RISK")

        print("\nBreakdown by Risk Category:")
        for row in risk_counts.iter_rows(named=True):
            print(f"  {row['RISK']:<20}: {row['count']:>6,} accounts, NPL: {row['total_npl']:>15,.2f}")

        # Check for discrepancies
        discrepancies = df_loan.filter(
            (pl.col('CHKNPL') - pl.col('NPL')).abs() > 0.01
        )

        if len(discrepancies) > 0:
            print(f"\nWARNING: {len(discrepancies)} accounts with discrepancies detected")
            print("(Difference between calculated NPL and formula > 0.01)")
        else:
            print("\nNo discrepancies detected in NPL calculations")

        print("=" * 70)

    print("\nProcessing complete.")


if __name__ == "__main__":
    main()
