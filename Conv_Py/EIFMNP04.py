# !/usr/bin/env python3
"""
Program: EIFMNP04
Purpose: MOVEMENTS OF SPECIFIC PROVISION FOR THE MONTH ENDING
         For Public Finance NPL accounts (6 months and above)

Date: 18.03.98

Note: This program processes NPL accounts for Public Finance division,
      which has different loan types and risk classification thresholds
      compared to HP (Hire Purchase) division.

      DISCONTINUED as per letter dated 26/08/03 from Statistics
"""

import polars as pl
from datetime import datetime
from pathlib import Path

# Setup paths
INPUT_NPL_REPTDATE = "NPL.REPTDATE.parquet"
INPUT_NPL_IIS = "NPL.IIS.parquet"
OUTPUT_NPL_SP = "NPL.SP.parquet"
OUTPUT_REPORT_SUMMARY = "NPL_SP_PF_SUMMARY_REPORT.txt"
OUTPUT_REPORT_DETAIL = "NPL_SP_PF_DETAIL_REPORT.txt"

# Global variables
REPTMON = None
RDATE = None
INPUT_NPL_LOAN = None


def format_loantype(loantype):
    """Format loan type description for Public Finance (different from HP)"""
    if loantype in [110, 115, 983]:
        return "HPD AITAB"
    elif loantype in [700, 705, 993]:
        return "HPD CONVENTIONAL"
    elif 200 <= loantype <= 299:
        return "HOUSING LOANS"
    elif (300 <= loantype <= 499) or (504 <= loantype <= 550) or (900 <= loantype <= 980):
        return "FIXED LOANS"
    else:
        return "OTHERS"


def classify_risk(days, borstat):
    """
    Classify risk category for Public Finance NPL
    Note: Different thresholds than HP division
    """
    if days > 364 or borstat in ['F', 'R', 'I', 'T', 'W']:
        return "BAD"
    elif days > 273:
        return "DOUBTFUL"
    else:
        return "SUBSTANDARD"


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
        pl.sum("CURBAL").alias("CURBAL"),
        pl.sum("UHC").alias("UHC"),
        pl.sum("NETBAL").alias("NETBAL"),
        pl.sum("IIS").alias("IIS"),
        pl.sum("OSPRIN").alias("OSPRIN"),
        pl.sum("MARKETVL").alias("MARKETVL"),
        pl.sum("NETEXP").alias("NETEXP"),
        pl.sum("SPP").alias("SPP"),
        pl.sum("SPPL").alias("SPPL"),
        pl.sum("RECOVER").alias("RECOVER"),
        pl.sum("SPPW").alias("SPPW"),
        pl.sum("SP").alias("SP")
    ]).sort(["LOANTYP", "RISK", "BRANCH"])

    with open(output_file, 'w') as f:
        f.write('1')
        f.write(f"{'PUBLIC FINANCE - (NPL FROM 6 MONTHS & ABOVE)':^132}\n")
        f.write(' ')
        f.write(f"MOVEMENTS OF SPECIFIC PROVISION FOR THE MONTH ENDING {rdate}\n")
        f.write(' ')
        f.write(f"{'(EXISTING AND CURRENT)':^132}\n")
        f.write(' \n')

        f.write(' ')
        f.write(f"{'RISK/BRANCH':<26}{'NO OF':>10}{'CURRENT':>15}{'UHC':>15}{'NET BAL':>15}\n")
        f.write(' ')
        f.write(f"{'':<26}{'ACCOUNT':>10}{'BAL (A)':>15}{'(B)':>15}{'(C)':>15}\n")
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
            risk_branch = f"  {row['RISK']:<13}{row['BRANCH']:<11}"
            f.write(f"{risk_branch:<26}{row['N']:>10,}{row['CURBAL']:>15,.2f}")
            f.write(f"{row['UHC']:>15,.2f}{row['NETBAL']:>15,.2f}\n")

        f.write(' ')
        f.write('=' * 132 + '\n')


def write_detail_report(df, output_file, rdate):
    """
    Write detailed report with ASA carriage control
    Only for KLM 001 or PJN 034 branches
    """

    # Filter for specific branches
    df_filtered = df.filter(
        (pl.col("BRANCH") == "KLM 001") | (pl.col("BRANCH") == "PJN 034")
    )

    if len(df_filtered) == 0:
        print("  No records found for branches KLM 001 or PJN 034")
        return

    df_sorted = df_filtered.sort(["LOANTYP", "BRANCH", "RISK", "DAYS", "ACCTNO"])

    with open(output_file, 'w') as f:
        lines_on_page = 0
        page_num = 1
        current_branch = None
        current_risk = None

        def write_header():
            nonlocal lines_on_page
            f.write('1')
            f.write(f"{'PUBLIC FINANCE - (NPL FROM 6 MONTHS & ABOVE)':<100}Page {page_num}\n")
            f.write(' ')
            f.write(f"MOVEMENTS OF SPECIFIC PROVISION FOR THE MONTH ENDING {rdate}\n")
            f.write(' ')
            f.write(f"{'(EXISTING AND CURRENT)':^100}\n")
            f.write(' \n')
            f.write(' ')
            f.write(f"{'ACCTNO':<15}{'NAME':<25}{'DAYS':>6}{'CURBAL':>15}{'SP':>15}\n")
            f.write(' ')
            f.write('-' * 100 + '\n')
            lines_on_page = 7

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
            curbal = safe_float(row_dict['CURBAL'])
            sp = safe_float(row_dict['SP'])

            f.write(f"{acctno:<15}{name:<25}{days:>6}{curbal:>15,.2f}{sp:>15,.2f}\n")
            lines_on_page += 1

        f.write(' ')
        f.write('=' * 100 + '\n')


def process_existing_npl(df_loanwoff, reptdate, styr, stmth):
    """
    Calculate SP for existing NPL accounts (LOAN1 equivalent)
    For Public Finance division
    """

    results = []

    for row_dict in df_loanwoff.filter(pl.col("EXIST") == "Y").iter_rows(named=True):
        uhc = 0.0

        bldate = row_dict.get('BLDATE')
        termchg = safe_float(row_dict.get('TERMCHG'))
        days = safe_int(row_dict.get('DAYS'))
        borstat = row_dict.get('BORSTAT', '')
        loantype = safe_int(row_dict.get('LOANTYPE'))
        issdte = row_dict.get('ISSDTE')
        noteterm = safe_int(row_dict.get('NOTETERM'))

        # Calculate UHC
        if bldate is not None and bldate > datetime(1960, 1, 1).date() and termchg > 0:
            if days > 182 or borstat in ['F', 'R', 'I', 'T']:
                if issdte and bldate:
                    remmth2 = noteterm - ((reptdate.year - issdte.year) * 12 + reptdate.month - issdte.month + 1)

                    if remmth2 < 0:
                        remmth2 = 0

                    if remmth2 > 0:
                        uhc = remmth2 * (remmth2 + 1) * termchg / (noteterm * (noteterm + 1))

        curbal = safe_float(row_dict.get('CURBAL'))
        netbal = curbal - uhc
        iis = safe_float(row_dict.get('IIS'))
        osprin = curbal - uhc - iis

        # Calculate MARKETVL
        marketvl = 0.0
        if borstat in ['R', 'T']:
            marketvl = safe_float(row_dict.get('MARKETVL'))

        netexp = osprin - marketvl

        # Calculate SP
        sp = 0.0
        if days > 364 or borstat in ['F', 'R', 'I', 'T', 'W']:
            sp = netexp
        elif days > 273:
            sp = netexp / 2

        if sp < 0:
            sp = 0

        spp = safe_float(row_dict.get('SPP'))
        sppl = sp - spp

        if sppl < 0:
            sppl = 0

        hardcode = row_dict.get('HARDCODE', 'N')
        if hardcode == 'Y':
            wsppl = row_dict.get('WSPPL')
            if wsppl is not None:
                sppl = safe_float(wsppl)
            wsp = row_dict.get('WSP')
            if wsp is not None:
                sp = safe_float(wsp)

        sppw = 0.0
        recover = 0.0

        if borstat == 'W':
            sppw = spp
            sppl = 0
            sp = 0
        else:
            recover = spp - sp

        if recover < 0:
            recover = 0

        risk = classify_risk(days, borstat)

        ntbrch = safe_int(row_dict.get('NTBRCH'))
        branch = f"{ntbrch:03d}"
        loantyp = format_loantype(loantype)

        result = {
            'BRANCH': branch,
            'NTBRCH': ntbrch,
            'ACCTNO': row_dict.get('ACCTNO'),
            'NOTENO': row_dict.get('NOTENO'),
            'NAME': row_dict.get('NAME'),
            'DAYS': days,
            'BORSTAT': borstat,
            'NETPROC': row_dict.get('NETPROC'),
            'CURBAL': curbal,
            'UHC': uhc,
            'NETBAL': netbal,
            'IIS': iis,
            'OSPRIN': osprin,
            'MARKETVL': marketvl,
            'NETEXP': netexp,
            'SPP': spp,
            'SPPL': sppl,
            'RECOVER': recover,
            'SPPW': sppw,
            'SP': sp,
            'RISK': risk,
            'LOANTYP': loantyp,
            'COSTCTR': row_dict.get('COSTCTR'),
            'PENDBRH': row_dict.get('PENDBRH')
        }

        results.append(result)

    return pl.DataFrame(results) if results else pl.DataFrame()


def process_current_npl(df_loanwoff, reptdate, styr, stmth):
    """
    Calculate SP for current NPL accounts (LOAN2 equivalent)
    For Public Finance division
    """

    results = []

    for row_dict in df_loanwoff.filter(pl.col("EXIST") != "Y").iter_rows(named=True):
        uhc = 0.0

        bldate = row_dict.get('BLDATE')
        termchg = safe_float(row_dict.get('TERMCHG'))
        borstat = row_dict.get('BORSTAT', '')
        loantype = safe_int(row_dict.get('LOANTYPE'))
        issdte = row_dict.get('ISSDTE')
        noteterm = safe_int(row_dict.get('NOTETERM'))

        # Calculate UHC
        if bldate is not None and bldate > datetime(1960, 1, 1).date() and termchg > 0:
            if issdte and reptdate:
                remmth2 = noteterm - ((reptdate.year - issdte.year) * 12 + reptdate.month - issdte.month + 1)

                if remmth2 < 0:
                    remmth2 = 0

                if remmth2 > 0:
                    uhc = remmth2 * (remmth2 + 1) * termchg / (noteterm * (noteterm + 1))
        else:
            if issdte and reptdate:
                remmth2 = noteterm - ((reptdate.year - issdte.year) * 12 + reptdate.month - issdte.month + 1)
                if remmth2 < 0:
                    remmth2 = 0
                if remmth2 > 0:
                    uhc = remmth2 * (remmth2 + 1) * termchg / (noteterm * (noteterm + 1))

        curbal = safe_float(row_dict.get('CURBAL'))
        netbal = curbal - uhc
        iis = safe_float(row_dict.get('IIS'))
        osprin = curbal - uhc - iis

        # Calculate MARKETVL
        marketvl = 0.0
        if borstat in ['R', 'T']:
            marketvl = safe_float(row_dict.get('MARKETVL'))

        netexp = osprin - marketvl

        # Calculate SP
        days = safe_int(row_dict.get('DAYS'))
        sp = 0.0
        if days > 364 or borstat in ['F', 'R', 'I', 'T', 'W']:
            sp = netexp
        elif days > 273:
            sp = netexp / 2

        if sp < 0:
            sp = 0

        sppl = sp

        hardcode = row_dict.get('HARDCODE', 'N')
        if hardcode == 'Y':
            wsppl = row_dict.get('WSPPL')
            if wsppl is not None:
                sppl = safe_float(wsppl)
            wsp = row_dict.get('WSP')
            if wsp is not None:
                sp = safe_float(wsp)

        risk = classify_risk(days, borstat)

        ntbrch = safe_int(row_dict.get('NTBRCH'))
        branch = f"{ntbrch:03d}"
        loantyp = format_loantype(loantype)

        # Initialize values that don't exist for current NPL
        spp = 0.0
        recover = 0.0
        sppw = 0.0

        result = {
            'BRANCH': branch,
            'NTBRCH': ntbrch,
            'ACCTNO': row_dict.get('ACCTNO'),
            'NOTENO': row_dict.get('NOTENO'),
            'NAME': row_dict.get('NAME'),
            'DAYS': days,
            'BORSTAT': borstat,
            'NETPROC': row_dict.get('NETPROC'),
            'CURBAL': curbal,
            'UHC': uhc,
            'NETBAL': netbal,
            'IIS': iis,
            'OSPRIN': osprin,
            'MARKETVL': marketvl,
            'NETEXP': netexp,
            'SPP': spp,
            'SPPL': sppl,
            'RECOVER': recover,
            'SPPW': sppw,
            'SP': sp,
            'RISK': risk,
            'LOANTYP': loantyp,
            'COSTCTR': row_dict.get('COSTCTR'),
            'PENDBRH': row_dict.get('PENDBRH')
        }

        results.append(result)

    return pl.DataFrame(results) if results else pl.DataFrame()


def main():
    """
    Main processing function for EIFMNP04
    PUBLIC FINANCE NPL Specific Provision Processing
    """
    global REPTMON, RDATE, INPUT_NPL_LOAN

    print("EIFMNP04 - Public Finance NPL Specific Provision Processing")
    print("=" * 70)
    print("NOTE: DISCONTINUED as per letter dated 26/08/03 from Statistics")
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
    df_loan_month = pl.read_parquet(INPUT_NPL_LOAN)
    df_iis = pl.read_parquet(INPUT_NPL_IIS).select(["ACCTNO", "IIS"])

    print(f"LOAN records: {len(df_loan_month):,}")
    print(f"IIS records: {len(df_iis):,}")

    # Merge LOAN with IIS
    df_loanwoff = df_loan_month.join(df_iis, on="ACCTNO", how="left")
    df_loanwoff = df_loanwoff.with_columns(pl.col("IIS").fill_null(0))

    print(f"Merged LOANWOFF records: {len(df_loanwoff):,}")

    # Process existing NPL accounts (LOAN1)
    styr = reptdate.year
    stmth = 1

    print("\nProcessing existing NPL accounts...")
    df_loan1 = process_existing_npl(df_loanwoff, reptdate, styr, stmth)
    print(f"Existing NPL accounts: {len(df_loan1):,}")

    # Process current NPL accounts (LOAN2)
    print("Processing current NPL accounts...")
    df_loan2 = process_current_npl(df_loanwoff, reptdate, styr, stmth)
    print(f"Current NPL accounts: {len(df_loan2):,}")

    # Combine LOAN1 and LOAN2 (LOAN3)
    print("\nCombining existing and current NPL accounts...")
    df_loan3 = pl.concat([df_loan1, df_loan2])

    print(f"Combined records: {len(df_loan3):,}")

    # Write output
    print(f"\nWriting output file...")
    df_loan3.write_parquet(OUTPUT_NPL_SP)
    print(f"  {OUTPUT_NPL_SP} ({len(df_loan3):,} records)")

    # Generate reports
    print("\nGenerating reports...")
    write_summary_report(df_loan3, OUTPUT_REPORT_SUMMARY, rdate)
    print(f"  {OUTPUT_REPORT_SUMMARY}")

    # Detail report only for specific branches
    print("\nGenerating detail report (KLM 001 and PJN 034 only)...")
    write_detail_report(df_loan3, OUTPUT_REPORT_DETAIL, rdate)
    print(f"  {OUTPUT_REPORT_DETAIL}")

    # Display summary statistics
    if len(df_loan3) > 0:
        print("\n" + "=" * 70)
        print("SUMMARY STATISTICS")
        print("=" * 70)

        total_curbal = df_loan3["CURBAL"].sum()
        total_sp = df_loan3["SP"].sum()
        total_sppl = df_loan3["SPPL"].sum()
        total_recover = df_loan3["RECOVER"].sum()

        print(f"Total Current Balance: {total_curbal:,.2f}")
        print(f"Total Specific Provision (SP): {total_sp:,.2f}")
        print(f"Total Provision Made (SPPL): {total_sppl:,.2f}")
        print(f"Total Written Back (RECOVER): {total_recover:,.2f}")

        # Count by risk category
        risk_counts = df_loan3.group_by("RISK").agg(
            pl.count().alias("count"),
            pl.sum("SP").alias("total_sp")
        ).sort("RISK")

        print("\nBreakdown by Risk Category:")
        for row in risk_counts.iter_rows(named=True):
            print(f"  {row['RISK']:<15}: {row['count']:>6,} accounts, SP: {row['total_sp']:>15,.2f}")

        print("=" * 70)

    print("\nProcessing complete.")
    print("\nNOTE: This report is DISCONTINUED and provided for historical reference only.")


if __name__ == "__main__":
    main()
