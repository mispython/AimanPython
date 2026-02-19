# !/usr/bin/env python3
"""
Program: EIBMRATE
Date: 02 JUNE 2000
Purpose: Average Rate on All Fixed Deposit Products on Monthly Basis
         - Excludes Islamic FD and unupdated products
         - Method: Average of 4 weekly extractions
"""

import polars as pl
import duckdb
from datetime import datetime
from pathlib import Path

# Paths
INPUT_PATH_REPTDATE = "FD_REPTDATE.parquet"
INPUT_PATH_SDESC = "BNM_SDESC.parquet"
INPUT_PATH_FD = "FD_FD.parquet"
INPUT_PATH_FD1 = "FD1_FD.parquet"
INPUT_PATH_FD2 = "FD2_FD.parquet"
INPUT_PATH_FD3 = "FD3_FD.parquet"
OUTPUT_PATH_REPORT = "EIBMRATE_REPORT.txt"
PAGE_LENGTH = 60
LRECL = 133

# Product mappings
ITEM_MAP = {
    'N1': ([370,371,372], 'POWERINVEST'),
    'N2': ([360,361,362,363,364,365,366], 'PBB GOLDEN 50 PLUS'),
    'N3': (list(range(320,332)), 'FIXED DEPOSIT LIFE'),
    'S1': (list(range(420,432)), 'STAFF FIXED DEPOSIT LIFE'),
    'S2': ([460], 'STAFF FIXED DEPOSIT GOLDEN 50+'),
    'S3': (list(range(400,420))+list(range(600,640)), 'STAFF FIXED DEPOSIT NORMAL'),
    'N4': (list(range(300,320))+list(range(373,385))+list(range(500,540)), 'FIXED DEPOSIT NORMAL')
}

def format_asa_line(line, cc=' '): return f"{cc}{line}"

def get_dates():
    df = pl.read_parquet(INPUT_PATH_REPTDATE)
    rd = df.row(0,named=True)['REPTDATE']
    day = rd.day
    nowk = '1' if day==8 else '2' if day==15 else '3' if day==22 else '4'
    return {'NOWK':nowk,'RDATE':f"{rd.day:02d}/{rd.month:02d}/{str(rd.year)[2:]}",
            'REPTDATE':rd}

def process_eibmrate():
    print("EIBMRATE - Average Rate on FD Products")
    mv = get_dates()
    rdate,reptdate = mv['RDATE'],mv['REPTDATE']

    sdesc_df = pl.read_parquet(INPUT_PATH_SDESC)
    sdesc = sdesc_df.row(0,named=True)['SDESC'][:36]

    # Load all 4 weekly FD files
    con = duckdb.connect()
    fds = []
    for path in [INPUT_PATH_FD,INPUT_PATH_FD1,INPUT_PATH_FD2,INPUT_PATH_FD3]:
        if Path(path).exists():
            fds.append(con.execute(f"SELECT * FROM read_parquet('{path}')").pl())

    if not fds:
        print("No FD files found")
        return

    fd_df = pl.concat(fds)
    rpyr,rpmth,rpday = reptdate.year,reptdate.month,reptdate.day
    rd2 = 29 if rpyr%4==0 else 28

    # Categorize
    records = []
    for row in fd_df.iter_rows(named=True):
        intplan = row.get('INTPLAN')
        item,desc = None,None
        for k,(codes,d) in ITEM_MAP.items():
            if intplan in codes:
                item,desc = k,d
                break
        if item:  # Exclude unidentified (Kelly's request 20JUL2001)
            records.append({**row,'ITEM':item,'DESC':desc})

    if not records:
        print("No valid FD records")
        return

    result_df = pl.DataFrame(records).sort(['BRANCH','ITEM'])

    with open(OUTPUT_PATH_REPORT,'w') as f:
        generate_report(f,result_df,rdate,sdesc,datetime.now())

    print(f"Generated: {OUTPUT_PATH_REPORT}")
    con.close()

def generate_report(f,df,rdate,sdesc,date_today):
    pagecnt = 0
    amtitem,intitem = 0.0,0.0
    amtbrch,intbrch = 0.0,0.0
    amtbank,intbank = 0.0,0.0
    prev_branch,prev_item = None,None
    first = True

    for idx,row in enumerate(df.iter_rows(named=True)):
        branch,item = row['BRANCH'],row['ITEM']
        desc = row['DESC']
        rate = row.get('RATE',0) or 0
        curbal = row.get('CURBAL',0) or 0

        if first or branch!=prev_branch:
            if not first:
                write_branch_total(f,amtbrch,intbrch)
                amtbrch=intbrch=0.0
            pagecnt+=1
            write_header(f,sdesc,rdate,date_today,pagecnt)
            first=False

        interest = (rate*curbal)/100
        amtitem+=curbal
        intitem+=interest
        amtbrch+=curbal
        intbrch+=interest
        amtbank+=curbal
        intbank+=interest

        is_last_item = (idx==len(df)-1 or df.row(idx+1,named=True)['ITEM']!=item or
                        df.row(idx+1,named=True)['BRANCH']!=branch)

        if is_last_item:
            avgrate = (intitem/amtitem*100) if amtitem>0 else 0
            line = f"  {branch:3d}         {desc:<32} {amtitem/4:>18,.2f}  {intitem/4:>18,.2f}    {avgrate:8.5f}"
            f.write(format_asa_line(line.ljust(LRECL-1))+' ')
            amtitem=intitem=0.0

        is_last_branch = (idx==len(df)-1 or df.row(idx+1,named=True)['BRANCH']!=branch)
        if is_last_branch:
            write_branch_total(f,amtbrch,intbrch)
            amtbrch=intbrch=0.0

        prev_branch,prev_item = branch,item

    # Company total
    avgrate = (intbank/amtbank*100) if amtbank>0 else 0
    f.write(format_asa_line(('='*120).ljust(LRECL-1))+' ')
    line = f"                             COMPANY TOTAL :  {amtbank/4:>18,.2f}  {intbank/4:>18,.2f}    {avgrate:8.5f}"
    f.write(format_asa_line(line.ljust(LRECL-1))+' ')

def write_branch_total(f,amt,intr):
    avgrate = (intr/amt*100) if amt>0 else 0
    f.write(format_asa_line(('-'*120).ljust(LRECL-1))+' ')
    line = f"                             BRANCH TOTAL :   {amt/4:>18,.2f}  {intr/4:>18,.2f}    {avgrate:8.5f}"
    f.write(format_asa_line(line.ljust(LRECL-1))+' ')
    f.write(format_asa_line(' '*(LRECL-1))+' ')

def write_header(f,sdesc,rdate,date_today,pagecnt):
    f.write(format_asa_line(' '*(LRECL-1),'1')+' ')
    line1 = f"                                          {sdesc:<36}                                                    PAGE NO : {pagecnt}"
    f.write(format_asa_line(line1.ljust(LRECL-1))+' ')
    line2 = f"                   AVERAGE RATE ON ALL FIXED DEPOSIT PRODUCTS ON MONTHLY             BASIS AS AT : {rdate}     DATE    : {date_today.strftime('%d/%m/%y')}"
    f.write(format_asa_line(line2.ljust(LRECL-1))+' ')
    line3 = "                             (METHOD BY : AVERAGE OF 4 WEEKLY EXTRACTIONS)"
    f.write(format_asa_line(line3.ljust(LRECL-1))+' ')
    f.write(format_asa_line(' '*(LRECL-1))+' ')
    hdr = "  BRANCH         FIXED DEPOSIT PRODUCTS                           AMOUNTS           INTEREST / YEAR      AVERAGE INTEREST"
    f.write(format_asa_line(hdr.ljust(LRECL-1))+' ')
    under = "  ______         ______________________                           _______           _______________      ________________"
    f.write(format_asa_line(' '*(LRECL-1))+' ')
    f.write(format_asa_line(under.ljust(LRECL-1),'+')+' ')
    f.write(format_asa_line(' '*(LRECL-1))+' ')

if __name__=="__main__":
    process_eibmrate()
