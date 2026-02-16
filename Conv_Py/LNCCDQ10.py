# !/usr/bin/env python3
"""
Program: LNCCDQ10
Date: 31.07.01
Function: Details for NPL accounts for CCD PFB (A monthly report)
Modify: 22 FEB 2006 ESMR 2005-610
"""

import polars as pl
from datetime import datetime
from pathlib import Path

INPUT_PATH_LOANTEMP = "BNM_LOANTEMP.parquet"
INPUT_PATH_BRANCH = "RBP2_B033_PBB_BRANCH.parquet"
OUTPUT_PATH_ARREARS_RPS = "SAP_PBB_NPL_ARREARS_RPS.txt"
LRECL_RPS = 134
HPD_PRODUCTS = [380, 381, 700, 705, 720, 725, 128, 130, 131, 132]

ARRCLASS_MAP = {1: 'CURRENT', 2: '1 MTH', 3: '2 MTHS', 4: '3 MTHS', 5: '4 MTHS',
                6: '5 MTHS', 7: '6 MTHS', 8: '7 MTHS', 9: '8 MTHS', 10: '9-11 MTHS',
                11: '1-<2 YRS', 12: '2-<3 YRS', 13: '3-<4 YRS', 14: '>4 YRS', 15: 'DEFICIT'}


def format_asa_line(line_content, carriage_control=' '):
    return f"{carriage_control}{line_content}"


def process_lnccdq10(rdate, preptdte):
    print("=" * 80)
    print("LNCCDQ10 - NPL Accounts (3+ months) Report")
    print("=" * 80)

    loantemp_df = pl.read_parquet(INPUT_PATH_LOANTEMP)
    branch_df = pl.read_parquet(INPUT_PATH_BRANCH)

    # Filter for HP products with balance > 0
    filtered_df = loantemp_df.filter(
        (pl.col('BALANCE') > 0) &
        (pl.col('BORSTAT') != 'Z') &
        (pl.col('PRODUCT').is_in(HPD_PRODUCTS))
    )

    # Merge with branch data
    filtered_df = filtered_df.join(
        branch_df.select(['BRANCH', 'BRHCODE']),
        on='BRANCH',
        how='left'
    )

    # Filter for NPL criteria
    records = []
    for row in filtered_df.iter_rows(named=True):
        arrear2 = row['ARREAR2']
        borstat = row['BORSTAT']
        issdte = row['ISSDTE']
        daydiff = row['DAYDIFF']

        if (arrear2 >= 3 or borstat in ['R', 'I', 'T', 'F', 'Y']):
            records.append(row)
        elif issdte and issdte >= preptdte and daydiff >= 8:
            records.append(row)

    if not records:
        print("No records found for LNCCDQ10")
        Path(OUTPUT_PATH_ARREARS_RPS).touch()
        return

    report_df = pl.DataFrame(records)

    # Categorize
    categorized_records = []
    for row in report_df.iter_rows(named=True):
        product = row['PRODUCT']
        if product in [380, 381, 700, 705, 720, 725]:
            categorized_records.append({**row, 'CAT': 'A', 'TYPE': 'HP DIRECT(CONV) '})
        if product in [380, 381]:
            categorized_records.append({**row, 'CAT': 'B', 'TYPE': 'HP (380,381) '})
        if product in [128, 130, 131, 132]:
            categorized_records.append({**row, 'CAT': 'C', 'TYPE': 'AITAB '})

    if not categorized_records:
        Path(OUTPUT_PATH_ARREARS_RPS).touch()
        return

    report_df = pl.DataFrame(categorized_records)

    # Add arrears class description and override for BORSTAT F
    report_df = report_df.with_columns([
        pl.when(pl.col('BORSTAT') == 'F')
        .then(pl.lit(15))
        .otherwise(pl.col('ARREAR2'))
        .alias('ARREAR2'),
        pl.col('ARREAR2').map_dict(ARRCLASS_MAP, default='UNKNOWN').alias('ARREARS')
    ])

    # Sort
    report_df = report_df.sort(['CAT', 'BRANCH', 'ARREAR2', 'DAYDIFF'])

    # Generate report
    with open(OUTPUT_PATH_ARREARS_RPS, 'w') as f:
        generate_report_output(f, report_df, rdate)

    print(f"Generated: {OUTPUT_PATH_ARREARS_RPS}")
    print("LNCCDQ10 processing complete")


def generate_report_output(f, df, rdate):
    pagecnt = 0
    linecnt = 0
    total = 0
    totalcap = 0
    prev_cat = None
    prev_branch = None
    prev_arrear2 = None
    brharr = brhamt = brhcap = brhcapb = 0

    for idx, row in enumerate(df.iter_rows(named=True)):
        cat = row['CAT']
        branch = row['BRANCH']
        arrear2 = row['ARREAR2']

        if cat != prev_cat:
            pagecnt = 1
            brharr = brhamt = brhcap = brhcapb = 0
            linecnt = 6
            write_page_header(f, branch, row['TYPE'], rdate, pagecnt)
        elif branch != prev_branch:
            pagecnt = 1
            brhamt = brhcapb = 0
            linecnt = 6
            write_page_header(f, branch, row['TYPE'], rdate, pagecnt)

        if arrear2 != prev_arrear2:
            brharr = brhcap = 0

        if linecnt > 56:
            pagecnt += 1
            linecnt = 6
            write_page_header(f, branch, row['TYPE'], rdate, pagecnt)

        # Write 4 detail lines per account
        brhcode = row.get('BRHCODE', str(branch).zfill(3))
        f.write(format_asa_line(
            f"{brhcode} {row['ACCTNO']:<10} {str(row.get('NAME', ''))[:24]:<24} {row['NOTENO']:>12} {row['PRODUCT']:5d} {row.get('BORSTAT', ''):<7} {row['ISSDTE'].strftime('%d/%m/%y') if row['ISSDTE'] else '  /  /  ':>8} {row.get('DAYDIFF', 0):>4} {row.get('ARREARS', ''):<12} {row.get('BALANCE', 0):17,.2f} {row.get('NOISTLPD', 0):7,d} {str(row.get('DELQCD', '')):<5}".ljust(
                LRECL_RPS - 1)) + '\n')
        lastran = row.get('LASTRAN')
        maturdt = row.get('MATURDT')
        f.write(format_asa_line(
            f"    {lastran.strftime('%d/%m/%y') if lastran else '  /  /  '} {maturdt.strftime('%d/%m/%y') if maturdt else '  /  /  '} {row.get('LSTTRNAM', 0):17,.2f} {row.get('PAYAMT', 0):11,.2f} {str(row.get('COLLDESC', ''))[:50]:<50}".ljust(
                LRECL_RPS - 1)) + '\n')
        f.write(format_asa_line(f"{'':34}{row.get('CAP', 0):16,.2f}".ljust(LRECL_RPS - 1)) + '\n')
        f.write(format_asa_line(' ' * (LRECL_RPS - 1)) + '\n')
        linecnt += 4

        brharr += row.get('BALANCE', 0)
        brhamt += row.get('BALANCE', 0)
        brhcap += row.get('CAP', 0)
        brhcapb += row.get('CAP', 0)
        totalcap += row.get('CAP', 0)
        total += row.get('BALANCE', 0)

        is_last_arrear2 = (idx == len(df) - 1 or df.row(idx + 1, named=True)['ARREAR2'] != arrear2 or
                           df.row(idx + 1, named=True)['BRANCH'] != branch or df.row(idx + 1, named=True)['CAT'] != cat)
        if is_last_arrear2:
            sep = ' ' * 40 + '-' * 40 + ' ' * 40 + '-' * 10
            f.write(format_asa_line(sep.ljust(LRECL_RPS - 1)) + '\n')
            f.write(format_asa_line(
                f"    SUBTOTAL{'':24}{brhcap:16,.2f}{'':43}{brharr:17,.2f}".ljust(LRECL_RPS - 1)) + '\n')
            f.write(format_asa_line(sep.ljust(LRECL_RPS - 1)) + '\n')
            f.write(format_asa_line(' ' * (LRECL_RPS - 1)) + '\n')
            linecnt += 4

        if linecnt > 56:
            pagecnt += 1
            linecnt = 6
            write_page_header(f, branch, row['TYPE'], rdate, pagecnt)

        is_last_branch = (idx == len(df) - 1 or df.row(idx + 1, named=True)['BRANCH'] != branch or
                          df.row(idx + 1, named=True)['CAT'] != cat)
        if is_last_branch:
            sep = ' ' * 40 + '-' * 40 + ' ' * 40 + '-' * 10
            f.write(format_asa_line(sep.ljust(LRECL_RPS - 1)) + '\n')
            f.write(format_asa_line(
                f"    BRANCH TOTAL{'':18}{brhcapb:16,.2f}{'':43}{brhamt:17,.2f}".ljust(LRECL_RPS - 1)) + '\n')
            f.write(format_asa_line(sep.ljust(LRECL_RPS - 1)) + '\n')
            f.write(format_asa_line(' ' * (LRECL_RPS - 1)) + '\n')
            linecnt += 4

        is_last_cat = (idx == len(df) - 1 or df.row(idx + 1, named=True)['CAT'] != cat)
        if is_last_cat:
            sep = ' ' * 40 + '-' * 40 + ' ' * 40 + '-' * 10
            f.write(format_asa_line(sep.ljust(LRECL_RPS - 1)) + '\n')
            f.write(format_asa_line(
                f"    GRAND TOTAL{'':19}{totalcap:16,.2f}{'':43}{total:17,.2f}".ljust(LRECL_RPS - 1)) + '\n')
            f.write(format_asa_line(sep.ljust(LRECL_RPS - 1)) + '\n')
            f.write(format_asa_line(' ' * (LRECL_RPS - 1)) + '\n')
            total = totalcap = 0

        prev_cat = cat
        prev_branch = branch
        prev_arrear2 = arrear2


def write_page_header(f, branch, cat_type, rdate, pagecnt):
    f.write(format_asa_line(
        f"PROGRAM-ID : LNCCDQ10 - BRANCH : {branch:3d}{'':6}P U B L I C   B A N K   B E R H A D{'':31}PAGE NO.: {pagecnt}".ljust(
            LRECL_RPS - 1), '1') + '\n')
    f.write(
        format_asa_line(f"{'':38}{cat_type}2 MTHS & ABOVE AND NEW A/C WITH ARREAR AS AT {rdate}".ljust(LRECL_RPS - 1),
                        ' ') + '\n')
    f.write(format_asa_line(' ' * (LRECL_RPS - 1), ' ') + '\n')
    f.write(format_asa_line(
        f"BRH ACCTNO{'':6}NAME{'':29}NOTENO{'':4}PRODUCT  BORSTAT  ISSUE DT  DAYS  ARREARS{'':16}BALANCE  NO ISTL  DELQ".ljust(
            LRECL_RPS - 1), ' ') + '\n')
    f.write(format_asa_line(
        f"    LST TR DT  MAT. DATE{'':16}LST TR AMT{'':5}ISTL AMT{'':14}COLLATERAL DESCRIPTION{'':13}   PAID  CODE".ljust(
            LRECL_RPS - 1), ' ') + '\n')
    f.write(format_asa_line(f"{'':34}CAPCLOSE".ljust(LRECL_RPS - 1), ' ') + '\n')
    f.write(format_asa_line(('-' * 40 + ' ' * 40 + '-' * 40 + ' ' * 10).ljust(LRECL_RPS - 1), ' ') + '\n')


if __name__ == "__main__":
    from datetime import datetime

    rdate = "22/02/24"
    preptdte = datetime(2024, 1, 1)
    process_lnccdq10(rdate, preptdte)
