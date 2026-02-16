# !/usr/bin/env python3
"""
Program: LNCCDW10
Function: Details for written-off A/Cs for 983 & 993 HP ONLY (A monthly report)
"""

import polars as pl
from datetime import datetime
from pathlib import Path

INPUT_PATH_LOANTEMP = "BNM_LOANTEMP.parquet"
INPUT_PATH_BRANCH = "RBP2_B033_PBB_BRANCH.parquet"
INPUT_PATH_LNNOTE = "SAP_PBB_MNILN_0_LNNOTE.parquet"
INPUT_PATH_HPWOFF = "SAP_PBB_NPL_WOFF_HPWOFF.parquet"
OUTPUT_PATH_WOFFHP_RPS = "SAP_PBB_NPL_WOFFHP_RPS.txt"
LRECL_RPS = 134


def format_asa_line(line_content, carriage_control=' '):
    return f"{carriage_control}{line_content}"


def process_lnccdw10(rdate):
    print("=" * 80)
    print("LNCCDW10 - Written-Off HP Accounts Report")
    print("=" * 80)

    loantemp_df = pl.read_parquet(INPUT_PATH_LOANTEMP)
    branch_df = pl.read_parquet(INPUT_PATH_BRANCH)

    # Filter for specific products and not BORSTAT E
    filtered_df = loantemp_df.filter(
        (pl.col('PRODUCT').is_in([983, 993, 678, 679, 698, 699])) &
        (pl.col('BORSTAT') != 'E')
    )

    if len(filtered_df) == 0:
        print("No records found")
        Path(OUTPUT_PATH_WOFFHP_RPS).touch()
        return

    # Merge with branch data
    filtered_df = filtered_df.join(
        branch_df.select(['BRANCH', 'BRHCODE']),
        on='BRANCH',
        how='left'
    )

    # Load LNNOTE for model description
    lnnote_df = pl.read_parquet(INPUT_PATH_LNNOTE).select(['ACCTNO', 'NOTENO', 'MODELDES'])
    filtered_df = filtered_df.join(lnnote_df, on=['ACCTNO', 'NOTENO'], how='left')

    # Load write-off data
    hpwoff_df = pl.read_parquet(INPUT_PATH_HPWOFF).select(['ACCTNO', 'SPALLOW', 'IIS', 'CAPBAL', 'WOFFDT'])
    filtered_df = filtered_df.join(hpwoff_df, on='ACCTNO', how='left')

    # Calculate TOTWOFF
    filtered_df = filtered_df.with_columns([
        pl.when(pl.col('CAPBAL') > 0)
        .then(pl.col('CAPBAL'))
        .otherwise(pl.col('SPALLOW').fill_null(0) + pl.col('IIS').fill_null(0))
        .alias('TOTWOFF')
    ])

    # Add category based on BORSTAT
    categorized_records = []
    for row in filtered_df.iter_rows(named=True):
        borstat = row['BORSTAT']
        catgy_map = {'F': 'DEFICIT', 'I': 'IRREGULAR', 'R': 'REPO', 'T': 'TOT LOSS', 'E': 'B.STATUS=E'}
        catgy = catgy_map.get(borstat, 'NON REPO')
        categorized_records.append({**row, 'CATGY': catgy})

    report_df = pl.DataFrame(categorized_records)
    report_df = report_df.sort(['BRANCH', 'CATGY', 'BALANCE'], descending=[False, False, True])

    # Generate report
    with open(OUTPUT_PATH_WOFFHP_RPS, 'w') as f:
        generate_report_output(f, report_df, rdate)

    print(f"Generated: {OUTPUT_PATH_WOFFHP_RPS}")
    print("LNCCDW10 processing complete")


def generate_report_output(f, df, rdate):
    pagecnt = 0
    linecnt = 0
    totbrbal = totbrsa = totbriis = totbrwof = totbrac = 0
    prev_branch = prev_catgy = None
    brbal = brsa = briis = brwoff = brac = 0

    for idx, row in enumerate(df.iter_rows(named=True)):
        branch = row['BRANCH']
        catgy = row['CATGY']
        brabbr = row.get('BRHCODE', str(branch).zfill(3))

        if branch != prev_branch or catgy != prev_catgy:
            pagecnt = 1
            brbal = brsa = briis = brwoff = brac = 0
            linecnt = 6
            write_page_header(f, branch, brabbr, catgy, rdate, pagecnt)

        if linecnt > 56:
            pagecnt += 1
            linecnt = 6
            write_page_header(f, branch, brabbr, catgy, rdate, pagecnt)

        # Write detail line
        f.write(format_asa_line(
            f"{str(row['ACCTNO']):<10}  {row['NOTENO']:>6}  {str(row.get('NAME', ''))[:24]:<24}  {row['ISSDTE'].strftime('%d/%m/%y') if row['ISSDTE'] else '  /  /  '}  {row.get('DAYDIFF', 0):5d}  {row.get('BALANCE', 0):12.2f}  {row.get('WOFFDT').strftime('%d/%m/%y') if row.get('WOFFDT') else '  /  /  '}  {row.get('SPALLOW', 0):10.2f}  {row.get('IIS', 0):10.2f}  {row.get('TOTWOFF', 0):12.2f}  {row.get('LASTRAN').strftime('%d/%m/%y') if row.get('LASTRAN') else '  /  /  '}  {str(row.get('LSTTRNCD', '')):<3}  {str(row.get('MODELDES', '')):<6}".ljust(
                LRECL_RPS - 1)) + '\n')
        linecnt += 1

        brbal += row.get('BALANCE', 0)
        brsa += row.get('SPALLOW', 0)
        briis += row.get('IIS', 0)
        brwoff += row.get('TOTWOFF', 0)
        brac += 1
        totbrbal += row.get('BALANCE', 0)
        totbrsa += row.get('SPALLOW', 0)
        totbriis += row.get('IIS', 0)
        totbrwof += row.get('TOTWOFF', 0)
        totbrac += 1

        is_last_catgy = (
                    idx == len(df) - 1 or df.row(idx + 1, named=True)['CATGY'] != catgy or df.row(idx + 1, named=True)[
                'BRANCH'] != branch)
        if is_last_catgy:
            f.write(format_asa_line((' ' * 40 + '-' * 91).ljust(LRECL_RPS - 1)) + '\n')
            f.write(format_asa_line(
                f"CATEGORY TOTAL{'':14}NO OF A/C : {brac:8d}{'':18}{brbal:12.2f}  {brsa:10.2f}  {briis:10.2f}  {brwoff:12.2f}".ljust(
                    LRECL_RPS - 1)) + '\n')
            f.write(format_asa_line((' ' * 40 + '-' * 91).ljust(LRECL_RPS - 1)) + '\n')
            f.write(format_asa_line(' ' * (LRECL_RPS - 1)) + '\n')
            linecnt += 4

        is_last_branch = (idx == len(df) - 1 or df.row(idx + 1, named=True)['BRANCH'] != branch)
        if is_last_branch:
            f.write(format_asa_line(
                f"BRANCH TOTAL{'':16}NO OF A/C : {totbrac:10d}{'':8}{totbrbal:12.2f}  {totbrsa:10.2f}  {totbriis:10.2f}  {totbrwof:12.2f}".ljust(
                    LRECL_RPS - 1)) + '\n')
            f.write(format_asa_line((' ' * 40 + '-' * 91).ljust(LRECL_RPS - 1)) + '\n')
            f.write(format_asa_line(' ' * (LRECL_RPS - 1)) + '\n')
            totbrbal = totbrsa = totbriis = totbrwof = totbrac = 0

        prev_branch = branch
        prev_catgy = catgy


def write_page_header(f, branch, brabbr, catgy, rdate, pagecnt):
    f.write(format_asa_line(
        f"PROGRAM-ID : LNCCDW10 - BRANCH : {branch:3d} ({brabbr})  P U B L I C   B A N K   B E R H A D{'':29}PAGE NO.: {pagecnt}".ljust(
            LRECL_RPS - 1), '1') + '\n')
    f.write(format_asa_line(
        f"{'':39}WRITTEN OFF A/C FOR HP(983,993,678,679,698,699) {catgy:10} AS AT {rdate}".ljust(LRECL_RPS - 1),
        ' ') + '\n')
    f.write(format_asa_line(' ' * (LRECL_RPS - 1), ' ') + '\n')
    f.write(format_asa_line(
        f"{'':11}NOTE{'':27}ISSUE{'':36}WRITTEN   SPECIFIC  INT.IN     TOT.WOFF{'':15}LASTTRAN TRN  AKPK".ljust(
            LRECL_RPS - 1), ' ') + '\n')
    f.write(format_asa_line(
        f"ACCTNO     NUM   NAME{'':21}DATE  DAYS    BALANCE    -OFF DT   ALLOW(A)  SUSP.(B)   (A+B) / CAP  DATE     CDE   TAG".ljust(
            LRECL_RPS - 1), ' ') + '\n')
    f.write(format_asa_line(('-' * 134).ljust(LRECL_RPS - 1), ' ') + '\n')


if __name__ == "__main__":
    rdate = "22/02/24"
    process_lnccdw10(rdate)
