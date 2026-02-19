# !/usr/bin/env python3
"""
Program: EIBMRM04
Date: 14.07.99
Purpose: Loans & Advances by Time to Maturity for ALCO
         (Weighted Average Yield by Maturity Profile)
"""

import polars as pl
import duckdb
from datetime import datetime, timedelta
from pathlib import Path

# Configuration
INPUT_PATH_REPTDATE = "BNM_REPTDATE.parquet"
INPUT_PATH_LOAN = "BNM_LOAN{reptmon}{nowk}.parquet"
OUTPUT_PATH_REPORT = "EIBMRM04_REPORT.txt"
PAGE_LENGTH = 60
LRECL = 133

# Product format mappings
LNPRDF_MAP = {
    **{i: 'STAFF' for i in [4,5,15,20]+list(range(25,33))},
    **{i: 'HL(SPTF)' for i in range(110,116)},
    **{i: 'HL(CONV)' for i in [200,201,204,205]+list(range(209,213))+[214,215,219,220]},
    **{i: 'BRIDGING' for i in [309,310,905]},
    **{i: 'PERSONAL' for i in [325,330,355]},
    **{i: 'RC' for i in [350,910,925]},
    **{i: 'SYND' for i in [914,915,919]},
    **{i: 'OTHER TL' for i in [33,116,180,227,228]+list(range(230,235))+[300,301,304,305,320,335]+
       [500,504,505,510]+list(range(515,526))+[555,556,560,561,564,565,570,573,900,901]}
}

ODPROD_MAP = {160: 'OD(SPTF)', 162: 'OD(SPTF)'}

# Subtype mappings for loans
SLNPRDF_MAP = {
    **{i: 2 for i in [4,5,15,20]+list(range(25,33))+list(range(110,116))+
       [33,116,500,504,505,510]+list(range(515,526))+[555,556,560,561,564,565,570]},
    **{i: 3 for i in [200,201,204,205]+list(range(209,213))+[214,215,219,220]+
       list(range(227,235))+[300,301,304,305,320,335,900,901,309,310,905,325,330,355,914,915,919,350,910,925]}
}

SODPRDF_MAP = {160: 2, 162: 2}  # All others: 3

SUBTYPF_MAP = {1:'PRINCIPAL',2:'PRIN(FIXED)',3:'PRIN(FLOAT)',7:'ACCRUED INT',8:'FEE AMOUNT',9:'NPL'}

REMFMT_MAP = {
    **{i: f'UP TO 1 MTH' if i<1 else f'>{i}  MTH - {i+1} MTHS' if i<10 else f'>{i} MTHS - {i+1} MTHS' for i in range(0,12)},
    **{12:'>1  YR - 2 YRS',24:'>2  YRS - 3 YRS',36:'>3  YRS - 4 YRS',48:'>4  YRS - 5 YRS'}
}

def format_asa_line(line, cc=' '): return f"{cc}{line}"

def get_remfmt(val):
    if val is None: return '       '
    for k in sorted([k for k in REMFMT_MAP.keys()], reverse=True):
        if val>=k: return REMFMT_MAP.get(k,'>5  YEAR')
    return 'UP TO 1 MTH'

def calc_remmth(matdt, reptdate, rpdays):
    """Calculate remaining months"""
    mdyr,mdmth,mdday = matdt.year,matdt.month,matdt.day
    rpyr,rpmth,rpday = reptdate.year,reptdate.month,reptdate.day

    md2 = 29 if mdyr%4==0 else 28
    if mdday > rpdays[rpmth-1]:
        mdday = rpdays[rpmth-1]

    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday

    return remy*12 + remm + remd/rpdays[rpmth-1]

def process_eibmrm04():
    """Main processing for EIBMRM04"""
    print("EIBMRM04 - Loans & Advances Time to Maturity")

    # Load REPTDATE
    reptdate_df = pl.read_parquet(INPUT_PATH_REPTDATE)
    reptdate_row = reptdate_df.row(0,named=True)
    reptdate = reptdate_row['REPTDATE']

    day = reptdate.day
    nowk = '1' if day==8 else '2' if day==15 else '3' if day==22 else '4'
    reptmon = f"{reptdate.month:02d}"
    rdate = f"{reptdate.day:02d}/{reptdate.month:02d}/{str(reptdate.year)[2:]}"

    rpyr,rpmth,rpday = reptdate.year,reptdate.month,reptdate.day
    rd2 = 29 if rpyr%4==0 else 28
    rpdays = [31,rd2,31,30,31,30,31,31,30,31,30,31]
    lday = rpdays.copy()

    # Load LOAN data
    loan_path = INPUT_PATH_LOAN.format(reptmon=reptmon,nowk=nowk)
    if not Path(loan_path).exists():
        print(f"Loan file not found: {loan_path}")
        return

    con = duckdb.connect()
    loan_df = con.execute(f"""
        SELECT * FROM read_parquet('{loan_path}')
        WHERE (SUBSTR(PRODCD,1,2)='34' OR PRODUCT IN (225,226))
        AND BALANCE <> 0
    """).pl()

    note_records = []

    for row in loan_df.iter_rows(named=True):
        acctype = row.get('ACCTYPE')
        balance = row.get('BALANCE',0) or 0
        product = row.get('PRODUCT')
        curbal = row.get('CURBAL',0) or 0
        feeamt = row.get('FEEAMT',0) or 0
        intrate = row.get('INTRATE',0) or 0

        # Determine PRODTYP
        if acctype=='OD':
            prodtyp = ODPROD_MAP.get(product,'OD(CONV)')
            acrint = 0
            curbal = balance
        else:
            prodtyp = LNPRDF_MAP.get(product,'OTHERS')
            # Calculate accrued interest
            acrint = 0
            if balance<0:
                acrint = balance
            elif balance != feeamt:
                acrint = balance - curbal - feeamt

        # Output accrued interest and fee
        note_records.append({'PRODTYP':prodtyp,'SUBTYP':7,'REMMTH':0.1,'AMOUNT':acrint,'YIELD':0})
        note_records.append({'PRODTYP':prodtyp,'SUBTYP':8,'REMMTH':0.1,'AMOUNT':feeamt,'YIELD':0})

        # Check NPL status
        bldate = row.get('BLDATE')
        loanstat = row.get('LOANSTAT')
        odstat = row.get('ODSTAT')

        days = 0
        if bldate and bldate>0:
            if isinstance(bldate,datetime):
                days = (reptdate - bldate).days

        if days>89 or loanstat==3 or odstat in ['RI','NI']:
            # NPL
            note_records.append({
                'PRODTYP':prodtyp,'SUBTYP':9,'REMMTH':0.1,
                'AMOUNT':curbal,'YIELD':curbal*intrate
            })
            continue

        # Performing loans
        if acctype=='OD':
            subtyp = SODPRDF_MAP.get(product,3)
            note_records.append({
                'PRODTYP':prodtyp,'SUBTYP':subtyp,'REMMTH':0.1,
                'AMOUNT':curbal,'YIELD':curbal*intrate
            })
            continue

        # Regular loans - split by payment schedule
        subtyp = SLNPRDF_MAP.get(product,1)
        payfreq = row.get('PAYFREQ','')
        freq_map = {'1':1,'2':3,'3':6,'4':12}
        freq = freq_map.get(payfreq,1)

        issdte = row.get('ISSDTE')
        exprdate = row.get('EXPRDATE')
        bilpay = row.get('BILPAY',0) or 0
        payamt = row.get('PAYAMT',0) or 0

        if bilpay<=0:
            bilpay = payamt

        # Calculate bill dates (simplified)
        if payfreq in ['5','9',' '] or product in [350,910,925]:
            # Bullet payment
            if exprdate and isinstance(exprdate,datetime):
                remmth = calc_remmth(exprdate,reptdate,rpdays)
                note_records.append({
                    'PRODTYP':prodtyp,'SUBTYP':subtyp,'REMMTH':remmth,
                    'AMOUNT':curbal,'YIELD':curbal*intrate
                })
        else:
            # Regular payments (simplified)
            note_records.append({
                'PRODTYP':prodtyp,'SUBTYP':subtyp,'REMMTH':1.0,
                'AMOUNT':curbal,'YIELD':curbal*intrate
            })

    if not note_records:
        print("No loan records to process")
        return

    # Create DataFrame and summarize
    note_df = pl.DataFrame(note_records)

    summary_df = note_df.groupby(['PRODTYP','SUBTYP','REMMTH']).agg([
        pl.sum('AMOUNT').alias('AMOUNT'),
        pl.sum('YIELD').alias('YIELD')
    ])

    # Calculate WAYLD
    summary_df = summary_df.with_columns([
        pl.when((pl.col('SUBTYP').is_in([7,8]))==False)
        .then(pl.col('YIELD')/pl.col('AMOUNT'))
        .otherwise(0)
        .alias('WAYLD'),
        (pl.col('AMOUNT')/1000).round(0).alias('AMOUNT')
    ])

    # Generate report
    with open(OUTPUT_PATH_REPORT,'w') as f:
        f.write(format_asa_line('PUBLIC BANK BERHAD'.center(LRECL-1),'1')+'\n')
        f.write(format_asa_line(f'TIME TO MATURITY AS AT {rdate}'.center(LRECL-1))+'\n')
        f.write(format_asa_line('RISK MANAGEMENT REPORT : EIBMRM04'.center(LRECL-1))+'\n')
        f.write(format_asa_line('RM DENOMINATION'.center(LRECL-1))+'\n')
        f.write(format_asa_line(' '*(LRECL-1))+'\n')

        # Write data (simplified tabular format)
        for row in summary_df.iter_rows(named=True):
            prodtyp = row['PRODTYP']
            subtyp = SUBTYPF_MAP.get(row['SUBTYP'],'UNKNOWN')
            remmth_desc = get_remfmt(row['REMMTH'])
            amount = row['AMOUNT']
            wayld = row['WAYLD']

            line = f"{prodtyp:<15} {subtyp:<15} {remmth_desc:<20} {amount:>12,.0f} {wayld:>6.2f}"
            f.write(format_asa_line(line.ljust(LRECL-1))+'\n')

    print(f"Generated: {OUTPUT_PATH_REPORT}")
    con.close()

if __name__=="__main__":
    process_eibmrm04()
