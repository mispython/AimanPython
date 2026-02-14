# !/usr/bin/env python3
"""
Program: EIFMNP03 (converted from SAS)
Purpose: MOVEMENTS OF INTEREST IN SUSPENSE FOR THE MONTH ENDING

Date: 12.03.98
Modified: ESMR 2004-720, 2004-579, 2006-1048

Process NPL accounts and calculate interest in suspense movements including:
- Interest in suspense (IIS)
- Overdue interest (OI)
- Suspensions, recoveries, and write-offs
- Classification by loan type, risk category, and branch
"""

import polars as pl
from datetime import datetime
from pathlib import Path

# Setup paths
INPUT_NPL_REPTDATE = "NPL.REPTDATE.parquet"
INPUT_NPL_WIIS = "NPL.WIIS.parquet"
OUTPUT_NPL_IIS = "NPL.IIS.parquet"
OUTPUT_REPORT_SUMMARY = "NPL_IIS_SUMMARY_REPORT.txt"
OUTPUT_REPORT_DETAIL = "NPL_IIS_DETAIL_REPORT.txt"

# Global variables
REPTMON = None
PREVMON = None
RDATE = None
INPUT_NPL_LOAN = None
INPUT_NPL_PLOAN = None
INPUT_NPL_IIS_PREV = None
OUTPUT_NPL_IIS_MONTH = None


def format_loantype(loantype):
    """Format loan type description (PROC FORMAT LNTYP)"""
    if loantype in [128, 130, 983]:
        return "HPD AITAB"
    elif loantype in [700, 705, 380, 381, 993, 996, 720, 725]:
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
        pl.sum("IISP").alias("IISP"),
        pl.sum("SUSPEND").alias("SUSPEND"),
        pl.sum("RECOVER").alias("RECOVER"),
        pl.sum("RECC").alias("RECC"),
        pl.sum("IISPW").alias("IISPW"),
        pl.sum("IIS").alias("IIS"),
        pl.sum("OIP").alias("OIP"),
        pl.sum("OISUSP").alias("OISUSP"),
        pl.sum("OIRECV").alias("OIRECV"),
        pl.sum("OIRECC").alias("OIRECC"),
        pl.sum("OIW").alias("OIW"),
        pl.sum("OI").alias("OI"),
        pl.sum("TOTIIS").alias("TOTIIS")
    ]).sort(["LOANTYP", "RISK", "BRANCH"])

    with open(output_file, 'w') as f:
        f.write('1')
        f.write(f"{'PUBLIC BANK - (NPL FROM 3 MONTHS & ABOVE) - NEW':^132}\n")
        f.write(' ')
        f.write(f"MOVEMENTS OF INTEREST IN SUSPENSE FOR THE MONTH ENDING {rdate}\n")
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
            f.write(f"MOVEMENTS OF INTEREST IN SUSPENSE FOR THE MONTH ENDING {rdate}\n")
            f.write(' ')
            f.write(f"{'(EXISTING AND CURRENT)':^100}\n")
            f.write(' \n')
            f.write(' ')
            f.write(f"{'ACCTNO':<15}{'NAME':<25}{'DAYS':>6}{'IIS':>15}{'TOTIIS':>15}\n")
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
            iis = safe_float(row_dict['IIS'])
            totiis = safe_float(row_dict['TOTIIS'])

            f.write(f"{acctno:<15}{name:<25}{days:>6}{iis:>15,.2f}{totiis:>15,.2f}\n")
            lines_on_page += 1

        f.write(' ')
        f.write('=' * 100 + '\n')


def process_existing_npl(df_loanwoff, reptdate, styr, stmth):
    """
    Calculate IIS for existing NPL accounts (LOAN1 equivalent)
    DATA LOAN1 processing logic
    """

    results = []

    for row_dict in df_loanwoff.filter(pl.col("EXIST") == "Y").iter_rows(named=True):
        iis = 0.0
        suspend = 0.0
        uhc = 0.0
        oi = 0.0
        oisusp = 0.0
        recover = 0.0
        oirecv = 0.0
        oirecc = 0.0
        oiw = 0.0

        iisp = safe_float(row_dict.get('IISP'))
        oip = safe_float(row_dict.get('OIP'))
        iispw = safe_float(row_dict.get('IISPW'))

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

        # Calculate IIS and related values
        if bldate is not None and bldate > datetime(1960, 1, 1).date() and termchg > 0:
            if days > 89 or borstat in ['F', 'R', 'I'] or (user5 == 'N' and loantype not in [983, 993]):
                if issdte and bldate:
                    remmth1 = earnterm - ((bldate.year - issdte.year) * 12 + bldate.month - issdte.month + 1)
                    remmth2 = earnterm - ((reptdate.year - issdte.year) * 12 + reptdate.month - issdte.month + 1)
                    remmths = earnterm - ((styr - issdte.year) * 12 + stmth - issdte.month + 1)

                    if remmth2 < 0:
                        remmth2 = 0

                    if loantype in [128, 130]:
                        remmth1 = remmth1 - 3
                    else:
                        remmth1 = remmth1 - 1

                    if remmth1 >= remmth2:
                        for remmth in range(remmth1, remmth2 - 1, -1):
                            iis += 2 * (remmth + 1) * termchg / (earnterm * (earnterm + 1))

                    feetot2 = safe_float(row_dict.get('FEETOT2'))
                    feeamta = safe_float(row_dict.get('FEEAMTA'))
                    feeamt5 = safe_float(row_dict.get('FEEAMT5'))
                    oi = feetot2 - feeamta + feeamt5

                    for remmth in range(remmths, remmth2 - 1, -1):
                        suspend += 2 * (remmth + 1) * termchg / (earnterm * (earnterm + 1))

                    if loantype not in [128, 130]:
                        feeamt = safe_float(row_dict.get('FEEAMT'))
                        oisusp = feeamt - feeamta + feeamt5

                    if remmth2 > 0:
                        uhc = remmth2 * (remmth2 + 1) * termchg / (earnterm * (earnterm + 1))
        elif days > 89 or borstat in ['F', 'R', 'I'] or (user5 == 'N' and loantype not in [983, 993]):
            feetot2 = safe_float(row_dict.get('FEETOT2'))
            feeamta = safe_float(row_dict.get('FEEAMTA'))
            feeamt5 = safe_float(row_dict.get('FEEAMT5'))
            feeamt = safe_float(row_dict.get('FEEAMT'))
            oi = feetot2 - feeamta + feeamt5
            oisusp = feeamt - feeamta + feeamt5

        curbal = safe_float(row_dict.get('CURBAL'))
        netbal = curbal - uhc

        if netbal <= iisp:
            if days > 89 or borstat in ['F', 'R', 'I'] or user5 == 'N':
                iis = netbal

        recc = 0.0

        if borstat == 'W':
            iispw = iisp
            oiw = oip
        else:
            recover = iisp + suspend - iis
            if recover < 0:
                suspend = suspend - recover
                recover = 0
            if recover > iisp:
                recc = recover - iisp
                recover = iisp

            if loantype not in [128, 130]:
                oirecv = oip - oi
                if oirecv < 0:
                    oisusp = oisusp - oirecv
                    oirecv = 0
                if oisusp < 0:
                    oirecv = oirecv - oisusp
                if oirecv > oip:
                    oirecc = oirecv - oip
                    oirecv = oip

        # Handle zero TERMCHG case
        if termchg == 0:
            marketvl = safe_float(row_dict.get('MARKETVL'))
            if borstat in ['R']:
                netexp = curbal - iisp - marketvl
            else:
                netexp = curbal - iisp

            if (netexp > 0 and days > 89) or borstat in ['R']:
                iis = recover
                recover = 0
                feetot2 = safe_float(row_dict.get('FEETOT2'))
                feeamta = safe_float(row_dict.get('FEEAMTA'))
                feeamt5 = safe_float(row_dict.get('FEEAMT5'))
                oi = feetot2 - feeamta + feeamt5
                oirecv = 0

        # Special handling for loan types 720, 725
        if loantype in [720, 725]:
            iis = safe_float(row_dict.get('ACCRUAL'))

        # Recalculate OISUSP
        oisusp = oirecv + oirecc + oiw - oip + oi
        if oisusp < 0:
            oirecv = oirecv - oisusp
        if oirecv > oip:
            oirecc = oirecv - oip
            oirecv = oip

        oisusp = oirecv + oirecc + oiw - oip + oi

        ntbrch = safe_int(row_dict.get('NTBRCH'))
        branch = f"{ntbrch:03d}"
        loantyp = format_loantype(loantype)

        # Handle writeoff cases
        if writeoff == 'Y':
            suspend = safe_float(row_dict.get('WSUSPEND'))
            oisusp = safe_float(row_dict.get('WOISUSP'))

            if wdownind != 'Y':
                recover = safe_float(row_dict.get('WRECOVER'))
                recc = safe_float(row_dict.get('WRECC'))
                oirecv = safe_float(row_dict.get('WOIRECV'))
                oirecc = safe_float(row_dict.get('WOIRECC'))
                iis = 0
                iispw = iisp + suspend - recover - recc
                oi = 0
                oiw = oip + oisusp - oirecv - oirecc
            else:
                oisusp = safe_float(row_dict.get('WOISUSP'))
                iispw = safe_float(row_dict.get('WIISPW'))
                iis = iisp + suspend - recover - recc - iispw
                if iis < 0:
                    recover = 0
                iis = iisp + suspend - recover - recc - iispw
                oiw = safe_float(row_dict.get('WOIW'))
                oi = oip + oisusp - oirecv - oirecc - oiw
                if oi < 0:
                    oirecv = 0
                    oirecc = 0
                oi = oip + oisusp - oirecv - oirecc - oiw

            # Ensure non-null values
            oip = safe_float(oip)
            iisp = safe_float(iisp)
            suspend = safe_float(suspend)
            oisusp = safe_float(oisusp)
            recover = safe_float(recover)
            oirecv = safe_float(oirecv)
            recc = safe_float(recc)
            oirecc = safe_float(oirecc)

        # Handle reschedule indicator
        rescheind = row_dict.get('RESCHEIND')
        if rescheind == 'Y':
            suspend = safe_float(row_dict.get('WSUSPEND'))
            oisusp = safe_float(row_dict.get('WOISUSP'))
            recover = safe_float(row_dict.get('WRECOVER'))
            recc = safe_float(row_dict.get('WRECC'))
            oirecv = safe_float(row_dict.get('WOIRECV'))
            oirecc = safe_float(row_dict.get('WOIRECC'))
            iis = iisp + suspend - recover - recc - iispw
            oi = oip + oisusp - oirecv - oirecc - oiw

        totiis = iis + oi

        result = {
            'BRANCH': branch,
            'NTBRCH': ntbrch,
            'ACCTNO': row_dict.get('ACCTNO'),
            'NOTENO': row_dict.get('NOTENO'),
            'NAME': row_dict.get('NAME'),
            'NETPROC': row_dict.get('NETPROC'),
            'CURBAL': curbal,
            'BORSTAT': borstat,
            'DAYS': days,
            'IIS': iis,
            'UHC': uhc,
            'NETBAL': netbal,
            'IISP': iisp,
            'SUSPEND': suspend,
            'RECOVER': recover,
            'RECC': recc,
            'IISPW': iispw,
            'OIP': oip,
            'OISUSP': oisusp,
            'OI': oi,
            'OIRECV': oirecv,
            'OIRECC': oirecc,
            'OIW': oiw,
            'TOTIIS': totiis,
            'LOANTYP': loantyp,
            'EXIST': 'Y',
            'COSTCTR': row_dict.get('COSTCTR'),
            'PENDBRH': row_dict.get('PENDBRH'),
            'USER5': user5,
            'WDOWNIND': wdownind,
            'RESCHEIND': rescheind,
            'ACCRUAL': row_dict.get('ACCRUAL')
        }

        results.append(result)

    return pl.DataFrame(results) if results else pl.DataFrame()


def process_current_npl(df_loanwoff, reptdate):
    """
    Calculate IIS for current NPL accounts (LOAN2 equivalent)
    DATA LOAN2 processing logic
    """

    results = []

    for row_dict in df_loanwoff.filter(pl.col("EXIST") != "Y").iter_rows(named=True):
        iis = 0.0
        uhc = 0.0
        oi = 0.0

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

        if bldate is not None and bldate > datetime(1960, 1, 1).date() and termchg > 0 or (user5 == 'N' and loantype not in [983, 993]):
            if issdte and reptdate:
                remmth1 = earnterm - ((bldate.year - issdte.year) * 12 + bldate.month - issdte.month + 1)
                remmth2 = earnterm - ((reptdate.year - issdte.year) * 12 + reptdate.month - issdte.month + 1)

                if remmth2 < 0:
                    remmth2 = 0

                if loantype in [128, 130]:
                    remmth1 = remmth1 - 3
                else:
                    remmth1 = remmth1 - 1

                if remmth1 >= remmth2:
                    for remmth in range(remmth1, remmth2 - 1, -1):
                        iis += 2 * (remmth + 1) * termchg / (earnterm * (earnterm + 1))

                if remmth2 > 0:
                    uhc = remmth2 * (remmth2 + 1) * termchg / (earnterm * (earnterm + 1))
        else:
            if issdte and reptdate:
                remmth2 = earnterm - ((reptdate.year - issdte.year) * 12 + reptdate.month - issdte.month + 1)
                if remmth2 < 0:
                    remmth2 = 0
                if remmth2 > 0:
                    uhc = remmth2 * (remmth2 + 1) * termchg / (earnterm * (earnterm + 1))

        feetot2 = safe_float(row_dict.get('FEETOT2'))
        feeamta = safe_float(row_dict.get('FEEAMTA'))
        feeamt5 = safe_float(row_dict.get('FEEAMT5'))
        oi = feetot2 - feeamta + feeamt5

        if loantype in [720, 725]:
            iis = safe_float(row_dict.get('ACCRUAL'))

        suspend = iis
        oisusp = oi

        curbal = safe_float(row_dict.get('CURBAL'))
        netbal = curbal - uhc

        iisp = safe_float(row_dict.get('IISP'))
        oip = safe_float(row_dict.get('OIP'))
        iispw = 0.0
        oiw = 0.0
        recover = 0.0
        recc = 0.0
        oirecv = 0.0
        oirecc = 0.0

        if writeoff == 'Y':
            suspend = safe_float(row_dict.get('WSUSPEND'))
            oisusp = safe_float(row_dict.get('WOISUSP'))

            if wdownind != 'Y':
                recover = safe_float(row_dict.get('WRECOVER'))
                recc = safe_float(row_dict.get('WRECC'))
                oirecv = safe_float(row_dict.get('WOIRECV'))
                oirecc = safe_float(row_dict.get('WOIRECC'))
                iis = 0
                iispw = iisp + suspend - recover - recc
                oi = 0
                oiw = oip + oisusp - oirecv - oirecc
            else:
                oisusp = safe_float(row_dict.get('WOISUSP'))
                iispw = safe_float(row_dict.get('WIISPW'))
                iis = iisp + suspend - recover - recc - iispw
                if iis < 0:
                    recover = 0
                iis = iisp + suspend - recover - recc - iispw
                oiw = safe_float(row_dict.get('WOIW'))
                oi = oip + oisusp - oirecv - oirecc - oiw
                if oi < 0:
                    oirecv = 0
                    oirecc = 0
                oi = oip + oisusp - oirecv - oirecc - oiw

            oip = safe_float(oip)
            iisp = safe_float(iisp)
            suspend = safe_float(suspend)
            oisusp = safe_float(oisusp)
            recover = safe_float(recover)
            oirecv = safe_float(oirecv)
            recc = safe_float(recc)
            oirecc = safe_float(oirecc)

        rescheind = row_dict.get('RESCHEIND')
        if rescheind == 'Y':
            suspend = safe_float(row_dict.get('WSUSPEND'))
            oisusp = safe_float(row_dict.get('WOISUSP'))
            recover = safe_float(row_dict.get('WRECOVER'))
            recc = safe_float(row_dict.get('WRECC'))
            oirecv = safe_float(row_dict.get('WOIRECV'))
            oirecc = safe_float(row_dict.get('WOIRECC'))
            iis = iisp + suspend - recover - recc - iispw
            oi = oip + oisusp - oirecv - oirecc - oiw

        totiis = iis + oi

        ntbrch = safe_int(row_dict.get('NTBRCH'))
        branch = f"{ntbrch:03d}"
        loantyp = format_loantype(loantype)

        result = {
            'BRANCH': branch,
            'NTBRCH': ntbrch,
            'ACCTNO': row_dict.get('ACCTNO'),
            'NOTENO': row_dict.get('NOTENO'),
            'NAME': row_dict.get('NAME'),
            'NETPROC': row_dict.get('NETPROC'),
            'CURBAL': curbal,
            'BORSTAT': borstat,
            'DAYS': safe_int(row_dict.get('DAYS')),
            'IIS': iis,
            'UHC': uhc,
            'NETBAL': netbal,
            'IISP': iisp,
            'SUSPEND': suspend,
            'RECOVER': recover,
            'RECC': recc,
            'IISPW': iispw,
            'OIP': oip,
            'OISUSP': oisusp,
            'OI': oi,
            'OIRECV': oirecv,
            'OIRECC': oirecc,
            'OIW': oiw,
            'TOTIIS': totiis,
            'LOANTYP': loantyp,
            'EXIST': row_dict.get('EXIST'),
            'COSTCTR': row_dict.get('COSTCTR'),
            'PENDBRH': row_dict.get('PENDBRH'),
            'USER5': user5,
            'WDOWNIND': wdownind,
            'RESCHEIND': rescheind,
            'ACCRUAL': row_dict.get('ACCRUAL')
        }

        results.append(result)

    return pl.DataFrame(results) if results else pl.DataFrame()


def main():
    """
    Main processing function for EIFMNP03
    """
    global REPTMON, PREVMON, RDATE, INPUT_NPL_LOAN, INPUT_NPL_PLOAN
    global INPUT_NPL_IIS_PREV, OUTPUT_NPL_IIS_MONTH

    print("EIFMNP03 - NPL Interest In Suspense Processing")
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
    INPUT_NPL_IIS_PREV = f"NPL.IIS{prevmon}.parquet"
    OUTPUT_NPL_IIS_MONTH = f"NPL.IIS{reptmon}.parquet"

    print(f"Reporting Month: {reptmon}")
    print(f"Previous Month: {prevmon}")
    print(f"Reporting Date: {rdate}")
    print("=" * 70)

    # Read input files
    print("\nReading input files...")
    df_loan_month = pl.read_parquet(INPUT_NPL_LOAN).sort("ACCTNO")
    df_wiis = pl.read_parquet(INPUT_NPL_WIIS).sort("ACCTNO")

    print(f"LOAN records: {len(df_loan_month):,}")
    print(f"WIIS records: {len(df_wiis):,}")

    # Merge LOAN with WIIS
    df_wiis_select = df_wiis.select([c for c in df_wiis.columns if c not in ['NOTENO', 'NTBRCH']])
    df_wiis_select = df_wiis_select.with_columns(pl.lit('Y').alias('_WRITEOFF_FLAG'))

    df_loanwoff = df_loan_month.join(df_wiis_select, on="ACCTNO", how="left")

    # Set WRITEOFF flag and adjust FEEAMT for certain loan types
    df_loanwoff = df_loanwoff.with_columns([
        pl.when(pl.col("_WRITEOFF_FLAG") == 'Y')
        .then(pl.lit('Y'))
        .otherwise(pl.lit('N'))
        .alias('WRITEOFF'),

        pl.when(pl.col("LOANTYPE").is_in([380, 381]))
        .then(pl.col("FEETOT2"))
        .otherwise(pl.col("FEEAMT"))
        .alias('FEEAMT'),

        pl.when(pl.col("LOANTYPE").is_in([983, 993]))
        .then(pl.lit('N'))
        .otherwise(pl.col("WDOWNIND"))
        .alias('WDOWNIND'),

        pl.col("IISP").fill_null(0),
        pl.col("OIP").fill_null(0),

        pl.when((pl.col("EARNTERM") == 0) | pl.col("EARNTERM").is_null())
        .then(pl.col("NOTETERM"))
        .otherwise(pl.col("EARNTERM"))
        .alias('EARNTERM')
    ]).drop('_WRITEOFF_FLAG')

    print(f"Merged LOANWOFF records: {len(df_loanwoff):,}")

    # Process existing NPL accounts (LOAN1)
    styr = reptdate.year
    stmth = 1

    print("\nProcessing existing NPL accounts...")
    df_loan1 = process_existing_npl(df_loanwoff, reptdate, styr, stmth)
    print(f"Existing NPL accounts: {len(df_loan1):,}")

    # Process current NPL accounts (LOAN2)
    print("Processing current NPL accounts...")
    df_loan2 = process_current_npl(df_loanwoff, reptdate)
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
    df_loan3.write_parquet(OUTPUT_NPL_IIS_MONTH)
    df_loan3.write_parquet(OUTPUT_NPL_IIS)

    print(f"  {OUTPUT_NPL_IIS_MONTH} ({len(df_loan3):,} records)")
    print(f"  {OUTPUT_NPL_IIS} ({len(df_loan3):,} records)")

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

        total_iis = df_loan3["IIS"].sum()
        total_oi = df_loan3["OI"].sum()
        total_totiis = df_loan3["TOTIIS"].sum()
        total_suspend = df_loan3["SUSPEND"].sum()
        total_recover = df_loan3["RECOVER"].sum()

        print(f"Total Interest in Suspense (IIS): {total_iis:,.2f}")
        print(f"Total Overdue Interest (OI): {total_oi:,.2f}")
        print(f"Total Interest in Suspense (IIS + OI): {total_totiis:,.2f}")
        print(f"Total Suspended: {total_suspend:,.2f}")
        print(f"Total Recovered: {total_recover:,.2f}")

        # Count by risk category
        risk_counts = df_loan3.group_by("RISK").agg(
            pl.count().alias("count"),
            pl.sum("TOTIIS").alias("total_totiis")
        ).sort("RISK")

        print("\nBreakdown by Risk Category:")
        for row in risk_counts.iter_rows(named=True):
            print(f"  {row['RISK']:<20}: {row['count']:>6,} accounts, TOTIIS: {row['total_totiis']:>15,.2f}")

        print("=" * 70)

    print("\nProcessing complete.")


if __name__ == "__main__":
    main()
