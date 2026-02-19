# !/usr/bin/env python3
"""
Program: EIBMRM02
Date: 10.12.99
Purpose: FD by Individual/Non-Individual by Time to Maturity for ALCO
         - Uses ORIGINAL MATURITY (TERM) for FD classification (KEY DIFFERENCE from RM01)
         - Only includes FD, not Savings/Current
         - Splits by TYPE: INDIVIDUALS vs NON-INDIVIDUALS
"""

import polars as pl
import duckdb
from datetime import datetime
from pathlib import Path

# Configuration
INPUT_PATH_REPTDATE = "FD_REPTDATE.parquet"
INPUT_PATH_FD = "FD_FD.parquet"
OUTPUT_PATH_REPORT = "EIBMRM02_REPORT.txt"
LRECL = 133


def format_asa_line(line, cc=' '): return f"{cc}{line}"


def get_remfmt(val):
    if val is None or val < 0: return '       '
    if val == 91: return ' 1 MONTH'
    if val == 92: return ' 3 MONTHS'
    if val == 93: return ' 6 MONTHS'
    if val == 94: return ' 9 MONTHS'
    if val == 95: return '12 MONTHS'
    if val == 96: return '15 MONTHS'
    if val == 97: return 'ABOVE 15 MONTHS'
    if val == 99: return 'OVERDUE FD'
    if val < 1: return '>0-1 MTH'
    if val < 2: return '>1-2 MTHS'
    if val < 3: return '>2-3 MTHS'
    if val < 4: return '>3-4 MTHS'
    if val < 5: return '>4-5 MTHS'
    if val < 6: return '>5-6 MTHS'
    if val < 7: return '>6-7 MTHS'
    if val < 8: return '>7-8 MTHS'
    if val < 9: return '>8-9 MTHS'
    if val < 10: return '>9-10 MTHS'
    if val < 11: return '>10-11 MTHS'
    if val < 12: return '>11-12 MTHS'
    if val < 24: return '>1-2 YRS'
    if val < 36: return '>2-3 YRS'
    if val < 48: return '>3-4 YRS'
    if val < 60: return '>4-5 YRS'
    return '>5 YRS'


SUBTTL_MAP = {'A': 'ORIGINAL MATURITY', 'B': 'OVERDUE FD', 'C': 'NEW FD FOR THE MONTH'}


def calc_remmth(matdt, reptdate, rpdays):
    mdyr, mdmth, mdday = matdt.year, matdt.month, matdt.day
    rpyr, rpmth, rpday = reptdate.year, reptdate.month, reptdate.day
    if mdmth == 2 and mdyr % 4 == 0:
        md2 = 29
    else:
        md2 = 28
    if mdday > rpdays[rpmth - 1]: mdday = rpdays[rpmth - 1]
    return (mdyr - rpyr) * 12 + (mdmth - rpmth) + (mdday - rpday) / rpdays[rpmth - 1]


def get_dates():
    df = pl.read_parquet(INPUT_PATH_REPTDATE)
    rd = df.row(0, named=True)['REPTDATE']
    day = rd.day
    nowk = '1' if day == 8 else '2' if day == 15 else '3' if day == 22 else '4'
    rdate = f"{rd.day:02d}/{rd.month:02d}/{str(rd.year)[2:]}"
    rd2 = 29 if rd.year % 4 == 0 else 28
    rpdays = [31, rd2, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    return {'NOWK': nowk, 'RDATE': rdate, 'REPTDATE': rd, 'RPDAYS': rpdays}


def process_eibmrm02():
    print("EIBMRM02 - FD by Individual/Non-Individual by ORIGINAL MATURITY")
    mv = get_dates()
    rdate, reptdate, rpdays = mv['RDATE'], mv['REPTDATE'], mv['RPDAYS']

    con = duckdb.connect()
    all_records = []

    # FD Processing - KEY DIFFERENCE: Uses ORIGINAL MATURITY (TERM)
    fd_df = con.execute(f"SELECT * FROM read_parquet('{INPUT_PATH_FD}')").pl()
    for row in fd_df.iter_rows(named=True):
        openind, curbal, intplan = row.get('OPENIND'), row.get('CURBAL', 0) or 0, row.get('INTPLAN')
        if openind not in ['O', 'D'] or curbal <= 0: continue

        # Determine TYPE (KEY FEATURE of RM02)
        custcd = row.get('CUSTCD')
        type_val = '  INDIVIDUALS  ' if custcd in [77, 78, 95, 96] else 'NON-INDIVIDUALS'

        # Determine SUBTYP
        if (340 <= intplan <= 359) or (448 <= intplan <= 459) or (461 <= intplan <= 469) or (580 <= intplan <= 599):
            subtyp = 'SPTF'
            staff = 'Y' if (448 <= intplan <= 459 or 461 <= intplan <= 469) else 'N'
        else:
            subtyp = 'CONVENTIONAL'
            staff = 'Y' if (400 <= intplan <= 419 or 420 <= intplan <= 449 or intplan == 460) else 'N'

        matdate = row.get('MATDATE')
        if not matdate: continue
        try:
            matdt = datetime.strptime(str(matdate).zfill(8), '%Y%m%d')
        except:
            continue

        rate, term = row.get('RATE', 0) or 0, row.get('TERM', 0) or 0
        cost = curbal * rate

        if openind == 'D' or matdt < reptdate:
            # Overdue
            all_records.append({'PRODTYP': 'FIXED DEPOSIT', 'SUBTYP': subtyp, 'SUBTTL': 'B',
                                'TYPE': type_val, 'REMMTH': 99, 'AMOUNT': curbal, 'COST': cost})
        else:
            # KEY DIFFERENCE: Use TERM (original maturity) not remaining maturity
            remmth_actual = calc_remmth(matdt, reptdate, rpdays)  # Calculate for new FD check only

            # Output with TERM (original maturity)
            all_records.append({'PRODTYP': 'FIXED DEPOSIT', 'SUBTYP': subtyp, 'SUBTTL': 'A',
                                'TYPE': type_val, 'REMMTH': term, 'AMOUNT': curbal, 'COST': cost})

            # New FD check (uses actual remaining maturity)
            if (term - remmth_actual) < 1:
                remmth_new = term - 0.5
                if remmth_new <= 1:
                    remmth_new = 91
                elif remmth_new <= 3:
                    remmth_new = 92
                elif remmth_new <= 6:
                    remmth_new = 93
                elif remmth_new <= 9:
                    remmth_new = 94
                elif remmth_new <= 12:
                    remmth_new = 95
                elif remmth_new <= 15:
                    remmth_new = 96
                else:
                    remmth_new = 97
                all_records.append({'PRODTYP': 'FIXED DEPOSIT', 'SUBTYP': subtyp, 'SUBTTL': 'C',
                                    'TYPE': type_val, 'REMMTH': remmth_new, 'AMOUNT': curbal, 'COST': cost})

    if not all_records:
        print("No records");
        return

    dep_df = pl.DataFrame(all_records)
    summary_df = dep_df.groupby(['TYPE', 'PRODTYP', 'SUBTYP', 'SUBTTL', 'REMMTH']).agg([
        pl.sum('AMOUNT').alias('AMOUNT'), pl.sum('COST').alias('COST')
    ])
    summary_df = summary_df.with_columns([
        pl.when((pl.col('SUBTYP').is_in(['SPTF', 'CONVENTIONAL'])) & (pl.col('AMOUNT') != 0))
        .then(pl.col('COST') / pl.col('AMOUNT')).otherwise(0).alias('WACOST'),
        (pl.col('AMOUNT') / 1000).round(0).alias('AMOUNT')
    ])
    summary_df = summary_df.sort(['TYPE', 'PRODTYP', 'SUBTYP', 'SUBTTL', 'REMMTH'])

    with open(OUTPUT_PATH_REPORT, 'w') as f:
        f.write(format_asa_line('PUBLIC BANK BERHAD'.center(LRECL - 1), '1') + '\n')
        f.write(format_asa_line(f'TIME TO MATURITY AS AT {rdate}'.center(LRECL - 1)) + '\n')
        f.write(format_asa_line('RISK MANAGEMENT REPORT : EIBMRM02'.center(LRECL - 1)) + '\n')
        f.write(format_asa_line('RM DENOMINATION'.center(LRECL - 1)) + '\n')
        f.write(format_asa_line(' ' * (LRECL - 1)) + '\n')

        for row in summary_df.iter_rows(named=True):
            line = f"{row['TYPE']:<17} {row['PRODTYP']:<15} {SUBTTL_MAP.get(row['SUBTTL'], row['SUBTTL']):<20} {row['SUBTYP']:<15} {get_remfmt(row['REMMTH']):<15} {row['AMOUNT']:>12,.0f} {row['WACOST']:>12.2f}"
            f.write(format_asa_line(line.ljust(LRECL - 1)) + '\n')

    print(f"Generated: {OUTPUT_PATH_REPORT}")
    con.close()


if __name__ == "__main__":
    process_eibmrm02()
