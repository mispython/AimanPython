# !/usr/bin/env python3
"""
Program: EIFMNP06
Purpose: MOVEMENTS OF SPECIFIC PROVISION FOR THE MONTH ENDING
         (BASED ON DEPRECIATED PURCHASE PRICE FOR UNSCHEDULED GOODS)

Date: 18.03.98
Modified: ESMR 2004-720, 2004-579, 2006-1048, 2006-1281

Process NPL accounts and calculate specific provisions including:
- Current balance and unearned hiring charges
- Principal outstanding and market value
- Movements of Specific Provision (SP)
- Classification by loan type, risk category, and branch
"""

import duckdb
import polars as pl
from datetime import datetime
from pathlib import Path

# Setup paths
INPUT_NPL_REPTDATE = "NPL.REPTDATE.parquet"
INPUT_NPL_WSP2 = "NPL.WSP2.parquet"
OUTPUT_NPL_SP2 = "NPL.SP2.parquet"
OUTPUT_REPORT_SUMMARY = "NPL_SP2_SUMMARY_REPORT.txt"
OUTPUT_REPORT_DETAIL = "NPL_SP2_DETAIL_REPORT.txt"

# Global variables
REPTMON = None
PREVMON = None
RDATE = None
INPUT_NPL_LOAN = None
INPUT_NPL_PLOAN = None
INPUT_NPL_IIS = None
INPUT_NPL_SP2_PREV = None
OUTPUT_NPL_SP2_MONTH = None


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
        pl.sum("CURBAL").alias("CURBAL"),
        pl.sum("UHC").alias("UHC"),
        pl.sum("NETBAL").alias("NETBAL"),
        pl.sum("IIS").alias("IIS"),
        pl.sum("OSPRIN").alias("OSPRIN"),
        pl.sum("MARKETVL").alias("MARKETVL"),
        pl.sum("NETEXP").alias("NETEXP"),
        pl.sum("SPP2").alias("SPP2"),
        pl.sum("SPPL").alias("SPPL"),
        pl.sum("RECOVER").alias("RECOVER"),
        pl.sum("SPPW").alias("SPPW"),
        pl.sum("SP").alias("SP"),
        pl.sum("OTHERFEE").alias("OTHERFEE")
    ]).sort(["LOANTYP", "RISK", "BRANCH"])

    with open(output_file, 'w') as f:
        f.write('1')
        f.write(f"{'PUBLIC BANK - (NPL FROM 3 MONTHS & ABOVE) - NEW':^132}\n")
        f.write(' ')
        f.write(f"MOVEMENTS OF SPECIFIC PROVISION FOR THE MONTH ENDING {rdate}\n")
        f.write(' ')
        f.write(f"{'(EXISTING AND CURRENT)':^132}\n")
        f.write(' \n')

        f.write(' ')
        f.write(f"{'RISK/BRANCH':<29}{'NO OF':>10}{'CURRENT':>15}{'UHC':>15}{'NET BAL':>15}\n")
        f.write(' ')
        f.write(f"{'':<29}{'ACCOUNT':>10}{'BAL (A)':>15}{'(B)':>15}{'(C)':>15}\n")
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
            f.write(f"{risk_branch:<29}{row['N']:>10,}{row['CURBAL']:>15,.2f}")
            f.write(f"{row['UHC']:>15,.2f}{row['NETBAL']:>15,.2f}\n")

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
            f.write(f"{'PUBLIC BANK - (NPL FROM 3 MONTHS & ABOVE) - NEW':<100}Page {page_num}\n")
            f.write(' ')
            f.write(f"MOVEMENTS OF SPECIFIC PROVISION FOR THE MONTH ENDING {rdate}\n")
            f.write(' ')
            f.write(f"{'(EXISTING AND CURRENT)':^100}\n")
            f.write(' \n')
            f.write(' ')
            f.write(f"{'ACCTNO':<15}{'NAME':<25}{'DAYS':>6}{'STAT':<5}{'CURBAL':>15}{'SP':>15}\n")
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
            borstat = str(row_dict.get('BORSTAT', ''))[:4]
            curbal = safe_float(row_dict['CURBAL'])
            sp = safe_float(row_dict['SP'])

            f.write(f"{acctno:<15}{name:<25}{days:>6}{borstat:<5}{curbal:>15,.2f}{sp:>15,.2f}\n")
            lines_on_page += 1

        f.write(' ')
        f.write('=' * 100 + '\n')


def process_existing_npl(df_loanwoff, reptdate, styr, stmth):
    """
    Calculate SP for existing NPL accounts (LOAN1 equivalent)
    DATA LOAN1 processing logic
    """

    results = []

    for row_dict in df_loanwoff.filter(pl.col("EXIST") == "Y").iter_rows(named=True):
        uhc = 0.0

        writeoff = row_dict.get('WRITEOFF', 'N')
        wdownind = row_dict.get('WDOWNIND', '')
        borstat = row_dict.get('BORSTAT', '')

        if writeoff == 'Y' and wdownind != 'Y':
            borstat = 'W'

        bldate = row_dict.get('BLDATE')
        termchg = safe_float(row_dict.get('TERMCHG'))
        days = safe_int(row_dict.get('DAYS'))
        user5 = row_dict.get('USER5')
        loantype = safe_int(row_dict.get('LOANTYPE'))
        issdte = row_dict.get('ISSDTE')
        earnterm = safe_int(row_dict.get('EARNTERM'))

        # Calculate UHC
        if bldate is not None and bldate > datetime(1960, 1, 1).date() and termchg > 0:
            if days > 89 or borstat in ['F', 'R', 'I'] or user5 == 'N':
                if issdte and bldate:
                    remmth2 = earnterm - ((reptdate.year - issdte.year) * 12 + reptdate.month - issdte.month + 1)

                    if remmth2 < 0:
                        remmth2 = 0

                    if remmth2 > 0:
                        uhc = remmth2 * (remmth2 + 1) * termchg / (earnterm * (earnterm + 1))

        curbal = safe_float(row_dict.get('CURBAL'))
        netbal = curbal - uhc
        iis = safe_float(row_dict.get('IIS'))
        osprin = curbal - uhc - iis

        # Calculate OTHERFEE
        if loantype in [380, 381]:
            feeamt = safe_float(row_dict.get('FEEAMT'))
            feetot2 = safe_float(row_dict.get('FEETOT2'))
            otherfee = feeamt - feetot2
        else:
            feeamt8 = safe_float(row_dict.get('FEEAMT8'))
            feetot2 = safe_float(row_dict.get('FEETOT2'))
            feeamta = safe_float(row_dict.get('FEEAMTA'))
            feeamt5 = safe_float(row_dict.get('FEEAMT5'))
            otherfee = feeamt8 - feetot2 + feeamta - feeamt5

        if otherfee < 0:
            otherfee = 0

        if loantype in [983, 993]:
            otherfee = 0

        appvalue = safe_float(row_dict.get('APPVALUE'))
        census7 = str(row_dict.get('CENSUS7', ''))
        hardcode = row_dict.get('HARDCODE', 'N')
        marketvl = 0.0
        sp = 0.0
        netexp = 0.0

        # Calculate SP
        if appvalue > 0 and (loantype in [705, 128, 700, 130, 380, 381, 720, 725] or census7 == '9') and \
           (days > 89 or user5 == 'N') and \
           borstat not in ['F', 'R', 'I', 'Y', 'W'] and \
           loantype not in [983, 993]:

            age = int(reptdate.year - issdte.year + (reptdate.month - issdte.month) / 12) if issdte else 0

            if census7 != '9':
                marketvl = appvalue - appvalue * age * 0.2

            if hardcode == 'Y':
                marketvl = safe_float(row_dict.get('WREALVL'))

            if marketvl < 0:
                marketvl = 0

            if days > 273:
                netexp = osprin + otherfee
            else:
                netexp = osprin + otherfee - marketvl

            if days > 364:
                sp = netexp
            elif days > 273:
                sp = netexp / 2
            elif days > 89:
                sp = netexp * 0.2
            elif days < 90:
                sp = netexp * 0.2
            else:
                sp = 0
        else:
            if borstat not in ['R']:
                marketvl = 0

            if hardcode == 'Y':
                marketvl = safe_float(row_dict.get('WREALVL'))

            netexp = osprin + otherfee - marketvl

            if days > 364 or borstat in ['F', 'R', 'I', 'W']:
                sp = netexp
            elif days > 273:
                sp = netexp / 2
            elif days > 89 and borstat == 'Y':
                sp = netexp / 5
            else:
                sp = 0

        if sp < 0:
            sp = 0

        spp2 = safe_float(row_dict.get('SPP2'))
        sppl = sp - spp2

        if sppl < 0:
            sppl = 0

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
            sppw = spp2
            sp = 0
            marketvl = 0
        else:
            recover = spp2 - sp

        if recover < 0:
            recover = 0

        ntbrch = safe_int(row_dict.get('NTBRCH'))
        branch = f"{ntbrch:03d}"
        loantyp = format_loantype(loantype)

        # Handle writeoff cases
        if writeoff == 'Y':
            sppl = safe_float(row_dict.get('WSPPL'))
            otherfee = 0

            if wdownind != 'Y':
                recover = safe_float(row_dict.get('WRECOVER'))
                sp = 0
                sppw = spp2 + sppl - recover
            else:
                sppw = safe_float(row_dict.get('WSPPW'))
                if netexp <= 0:
                    recover = 0
                sp = spp2 + sppl - recover - sppw
                if netexp <= 0 and sp > 0:
                    recover = sp
                    sp = 0

        # Handle reschedule indicator
        rescheind = row_dict.get('RESCHEIND')
        if rescheind == 'Y':
            sppl = safe_float(row_dict.get('WSPLL'))
            recover = safe_float(row_dict.get('WRECOVER'))
            sppw = safe_float(row_dict.get('WSPPW'))
            sp = spp2 + sppl - recover - sppw

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
            'SPP2': spp2,
            'SPPL': sppl,
            'RECOVER': recover,
            'SPPW': sppw,
            'SP': sp,
            'LOANTYP': loantyp,
            'VINNO': row_dict.get('VINNO'),
            'CENSUS7': census7,
            'OTHERFEE': otherfee,
            'EXIST': 'Y',
            'COSTCTR': row_dict.get('COSTCTR'),
            'USER5': user5,
            'PENDBRH': row_dict.get('PENDBRH'),
            'WDOWNIND': wdownind,
            'RESCHEIND': rescheind
        }

        results.append(result)

    return pl.DataFrame(results) if results else pl.DataFrame()


def process_current_npl(df_loanwoff, reptdate, styr, stmth):
    """
    Calculate SP for current NPL accounts (LOAN2 equivalent)
    DATA LOAN2 processing logic
    """

    results = []

    for row_dict in df_loanwoff.filter(pl.col("EXIST") != "Y").iter_rows(named=True):
        uhc = 0.0

        writeoff = row_dict.get('WRITEOFF', 'N')
        wdownind = row_dict.get('WDOWNIND', '')
        borstat = row_dict.get('BORSTAT', '')

        if writeoff == 'Y' and wdownind != 'Y':
            borstat = 'W'

        bldate = row_dict.get('BLDATE')
        termchg = safe_float(row_dict.get('TERMCHG'))
        user5 = row_dict.get('USER5')
        loantype = safe_int(row_dict.get('LOANTYPE'))
        issdte = row_dict.get('ISSDTE')
        earnterm = safe_int(row_dict.get('EARNTERM'))

        # Calculate UHC
        if bldate is not None and bldate > datetime(1960, 1, 1).date() and termchg > 0:
            if issdte and reptdate:
                remmth2 = earnterm - ((reptdate.year - issdte.year) * 12 + reptdate.month - issdte.month + 1)

                if remmth2 < 0:
                    remmth2 = 0

                if remmth2 > 0:
                    uhc = remmth2 * (remmth2 + 1) * termchg / (earnterm * (earnterm + 1))
        else:
            if issdte and reptdate:
                remmth2 = earnterm - ((reptdate.year - issdte.year) * 12 + reptdate.month - issdte.month + 1)
                if remmth2 < 0:
                    remmth2 = 0
                if remmth2 > 0:
                    uhc = remmth2 * (remmth2 + 1) * termchg / (earnterm * (earnterm + 1))

        curbal = safe_float(row_dict.get('CURBAL'))
        netbal = curbal - uhc
        iis = safe_float(row_dict.get('IIS'))
        osprin = curbal - uhc - iis

        # Calculate OTHERFEE
        if loantype in [380, 381]:
            feeamt = safe_float(row_dict.get('FEEAMT'))
            feetot2 = safe_float(row_dict.get('FEETOT2'))
            otherfee = feeamt - feetot2
        else:
            feeamt8 = safe_float(row_dict.get('FEEAMT8'))
            feetot2 = safe_float(row_dict.get('FEETOT2'))
            feeamta = safe_float(row_dict.get('FEEAMTA'))
            feeamt5 = safe_float(row_dict.get('FEEAMT5'))
            otherfee = feeamt8 - feetot2 + feeamta - feeamt5

        if otherfee < 0:
            otherfee = 0

        if loantype in [983, 993]:
            otherfee = 0

        appvalue = safe_float(row_dict.get('APPVALUE'))
        census7 = str(row_dict.get('CENSUS7', ''))
        hardcode = row_dict.get('HARDCODE', 'N')
        days = safe_int(row_dict.get('DAYS'))
        marketvl = 0.0
        sp = 0.0
        netexp = 0.0

        # Calculate SP
        if appvalue > 0 and (loantype in [705, 130, 700, 128, 380, 381, 720, 725] or census7 == '9') and \
           (days > 89 or user5 == 'N') and \
           borstat not in ['F', 'R', 'I', 'Y', 'W'] and \
           loantype not in [983, 993]:

            age = int(reptdate.year - issdte.year + (reptdate.month - issdte.month) / 12) if issdte else 0

            if census7 != '9':
                marketvl = appvalue - appvalue * age * 0.2

            if hardcode == 'Y':
                marketvl = safe_float(row_dict.get('WREALVL'))

            if marketvl < 0:
                marketvl = 0

            if days > 273:
                netexp = osprin + otherfee
            else:
                netexp = osprin + otherfee - marketvl

            if days > 364:
                sp = netexp
            elif days > 273:
                sp = netexp / 2
            elif days > 89:
                sp = netexp * 0.2
            elif days < 90:
                sp = netexp * 0.2
            else:
                sp = 0
        else:
            if borstat not in ['R']:
                marketvl = 0

            if hardcode == 'Y':
                marketvl = safe_float(row_dict.get('WREALVL'))

            netexp = osprin + otherfee - marketvl

            if days > 364 or borstat in ['F', 'R', 'I', 'W']:
                sp = netexp
            elif days > 273:
                sp = netexp / 2
            elif days > 89 and borstat == 'Y':
                sp = netexp / 5
            else:
                sp = 0

        if sp < 0:
            sp = 0

        sppl = sp

        if hardcode == 'Y':
            wsppl = row_dict.get('WSPPL')
            if wsppl is not None:
                sppl = safe_float(wsppl)
            wsp = row_dict.get('WSP')
            if wsp is not None:
                sp = safe_float(wsp)

        ntbrch = safe_int(row_dict.get('NTBRCH'))
        branch = f"{ntbrch:03d}"
        loantyp = format_loantype(loantype)

        spp2 = safe_float(row_dict.get('SPP2'))
        sppw = 0.0
        recover = 0.0

        # Handle writeoff cases
        if writeoff == 'Y':
            sppl = safe_float(row_dict.get('WSPPL'))
            otherfee = 0

            if wdownind != 'Y':
                recover = safe_float(row_dict.get('WRECOVER'))
                sp = 0
                sppw = spp2 + sppl - recover
            else:
                sppw = safe_float(row_dict.get('WSPPW'))
                if netexp <= 0:
                    recover = 0
                sp = spp2 + sppl - recover - sppw
                if netexp <= 0 and sp > 0:
                    recover = sp
                    sp = 0

        # Handle reschedule indicator
        rescheind = row_dict.get('RESCHEIND')
        if rescheind == 'Y':
            sppl = safe_float(row_dict.get('WSPLL'))
            recover = safe_float(row_dict.get('WRECOVER'))
            sppw = safe_float(row_dict.get('WSPPW'))
            sp = spp2 + sppl - recover - sppw

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
            'SPP2': spp2,
            'SPPL': sppl,
            'RECOVER': recover,
            'SPPW': sppw,
            'SP': sp,
            'LOANTYP': loantyp,
            'VINNO': row_dict.get('VINNO'),
            'CENSUS7': census7,
            'OTHERFEE': otherfee,
            'EXIST': row_dict.get('EXIST'),
            'COSTCTR': row_dict.get('COSTCTR'),
            'USER5': user5,
            'PENDBRH': row_dict.get('PENDBRH'),
            'WDOWNIND': wdownind,
            'RESCHEIND': rescheind
        }

        results.append(result)

    return pl.DataFrame(results) if results else pl.DataFrame()


def main():
    """
    Main processing function for EIFMNP06
    """
    global REPTMON, PREVMON, RDATE, INPUT_NPL_LOAN, INPUT_NPL_PLOAN
    global INPUT_NPL_IIS, INPUT_NPL_SP2_PREV, OUTPUT_NPL_SP2_MONTH

    print("EIFMNP06 - NPL Specific Provision Processing")
    print("=" * 70)

    # Read REPTDATE
    df_reptdate = pl.read_parquet(INPUT_NPL_REPTDATE)
    reptdate = df_reptdate['REPTDATE'][0]
    reptmon = f"{reptdate.month:02d}"

    mm1 = 12 if reptdate.month == 1 else reptdate.month - 1
    prevmon = f"{mm1:02d}"

    rdate = reptdate.strftime("%d %B %Y").upper()

    REPTMON = reptmon
    PREVMON = prevmon
    RDATE = rdate

    INPUT_NPL_LOAN = f"NPL.LOAN{reptmon}.parquet"
    INPUT_NPL_PLOAN = f"NPL.PLOAN{reptmon}.parquet"
    INPUT_NPL_IIS = f"NPL.IIS{reptmon}.parquet"
    INPUT_NPL_SP2_PREV = f"NPL.SP2{prevmon}.parquet"
    OUTPUT_NPL_SP2_MONTH = f"NPL.SP2{reptmon}.parquet"

    print(f"Reporting Month: {reptmon}")
    print(f"Previous Month: {prevmon}")
    print(f"Reporting Date: {rdate}")
    print("=" * 70)

    # Read input files
    print("\nReading input files...")
    df_loan_month = pl.read_parquet(INPUT_NPL_LOAN).sort("ACCTNO")
    df_wsp2 = pl.read_parquet(INPUT_NPL_WSP2).sort("ACCTNO")
    df_iis = pl.read_parquet(INPUT_NPL_IIS).select(["ACCTNO", "IIS"]).sort("ACCTNO")

    print(f"LOAN records: {len(df_loan_month):,}")
    print(f"WSP2 records: {len(df_wsp2):,}")
    print(f"IIS records: {len(df_iis):,}")

    # Merge LOAN with WSP2
    df_wsp2_select = df_wsp2.select([c for c in df_wsp2.columns if c not in ['NOTENO', 'NTBRCH']])
    df_wsp2_select = df_wsp2_select.with_columns(pl.lit('Y').alias('_WRITEOFF_FLAG'))

    df_loanwoff = df_loan_month.join(df_wsp2_select, on="ACCTNO", how="left")

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

    # Merge with IIS
    df_loanwoff = df_loanwoff.join(df_iis, on="ACCTNO", how="left")
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

    # Add RISK classification
    df_loan3 = df_loan3.with_columns(
        pl.struct(['DAYS', 'BORSTAT', 'USER5'])
        .map_elements(lambda x: classify_risk(x['DAYS'], x['BORSTAT'], x['USER5']), return_dtype=pl.Utf8)
        .alias('RISK')
    )

    # Filter by COSTCTR
    df_loan3 = df_loan3.filter(
        ((pl.col("COSTCTR") < 3000) | (pl.col("COSTCTR") > 3999)) &
        (~pl.col("COSTCTR").is_in([4043, 4048])) &
        (pl.col("COSTCTR").is_not_null())
    )

    # Remove duplicates
    df_loan3 = df_loan3.unique(subset=["ACCTNO", "NOTENO"], keep="first")

    print(f"Combined records after filtering: {len(df_loan3):,}")

    # Write outputs
    print(f"\nWriting output files...")
    df_loan3.write_parquet(OUTPUT_NPL_SP2_MONTH)
    df_loan3.write_parquet(OUTPUT_NPL_SP2)

    print(f"  {OUTPUT_NPL_SP2_MONTH} ({len(df_loan3):,} records)")
    print(f"  {OUTPUT_NPL_SP2} ({len(df_loan3):,} records)")

    # Generate reports
    print("\nGenerating reports...")
    write_summary_report(df_loan3, OUTPUT_REPORT_SUMMARY, rdate)
    print(f"  {OUTPUT_REPORT_SUMMARY}")

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
            print(f"  {row['RISK']:<20}: {row['count']:>6,} accounts, SP: {row['total_sp']:>15,.2f}")

        print("=" * 70)

    print("\nProcessing complete.")


if __name__ == "__main__":
    main()
