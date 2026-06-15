import polars as pl
from datetime import datetime

LN_REPTDATE = 'data/ln/reptdate.parquet'
CISLN_LOAN = 'data/cisln/loan.parquet'
CISDP_DEPOSIT = 'data/cisdp/deposit.parquet'
LOAN_DIR = 'data/loan/'
BT_DIR = 'data/bt/'
AMC_DIR = 'data/amc/'
BRHFILE = 'data/brhfile.parquet'
COLDBR = 'data/coldbr.txt'
COLDHQ = 'data/coldhq.txt'

HP_PRODUCTS = [501, 502, 503, 504, 505]

df_reptdate = pl.read_parquet(LN_REPTDATE)
reptdate = df_reptdate['REPTDATE'][0]

day = reptdate.day
if day == 8:
    wk = '1'
    wk1 = '4'
elif day == 15:
    wk = '2'
    wk1 = '1'
elif day == 22:
    wk = '3'
    wk1 = '2'
else:
    wk = '4'
    wk1 = '3'

mm = reptdate.month
yy1 = reptdate.year
yy2 = reptdate.year
mm1 = mm - 1
if mm1 == 0:
    mm1 = 12
    yy1 = reptdate.year - 1
mm2 = mm1 - 1
if mm2 == 0:
    mm2 = 12
    yy2 = reptdate.year - 1

nowk = wk
nowk1 = wk1
reptmon = f'{mm:02d}'
reptmon1 = f'{mm1:02d}'
reptmon2 = f'{mm2:02d}'
reptyr = f'{reptdate.year:04d}'
reptyr1 = f'{yy1:04d}'
reptyr2 = f'{yy2:04d}'
rdate = reptdate.strftime('%d%m%y')

df_lncis = pl.read_parquet(CISLN_LOAN)
df_lncis = df_lncis.filter(
    (~pl.col('CACCCODE').is_in(['017', '021', '028'])) &
    (pl.col('SECCUST') == '901')
).with_columns([
    pl.when(pl.col('NEWIC').str.strip_chars() != '')
      .then(pl.col('NEWIC'))
      .otherwise(pl.col('OLDIC'))
      .alias('ICNO')
]).select(['ACCTNO', 'ICNO', 'CUSTNAME', 'PRIPHONE', 'SECPHONE'])

df_dpcis = pl.read_parquet(CISDP_DEPOSIT)
df_dpcis = df_dpcis.filter(
    (~pl.col('CACCCODE').is_in(['017', '021', '028'])) &
    (pl.col('SECCUST') == '901')
).with_columns([
    pl.when(pl.col('NEWIC').str.strip_chars() != '')
      .then(pl.col('NEWIC'))
      .otherwise(pl.col('OLDIC'))
      .alias('ICNO')
]).select(['ACCTNO', 'ICNO', 'CUSTNAME', 'PRIPHONE', 'SECPHONE'])

df_cis = pl.concat([df_lncis, df_dpcis])

df_lnacc = pl.read_parquet(f'{LOAN_DIR}LOAN{reptmon}{nowk}.parquet')
df_lnacc = df_lnacc.filter(~pl.col('PRODUCT').is_in(HP_PRODUCTS))
df_lnacc = df_lnacc.with_columns([
    pl.when(pl.col('ACCTYPE') == 'OD')
      .then(pl.lit('OD'))
      .otherwise(pl.lit('FL'))
      .alias('PRODTYPE')
]).select(['ACCTNO', 'NOTENO', 'BRANCH', 'CURBAL', 'BALANCE', 'APPRLIMT', 'APPRLIM2', 'PRODTYPE', 'UNDRAWN'])

df_btacc = pl.read_parquet(f'{BT_DIR}BTMAST{reptmon}{nowk}.parquet')
df_btacc = df_btacc.filter(
    (pl.col('SUBACCT') == 'OV') &
    (pl.col('CUSTCD').str.strip_chars() != '') &
    (pl.col('DCURBAL').is_not_null())
).with_columns([
    pl.lit('TB').alias('PRODTYPE'),
    pl.col('ACCTNO').alias('ACCT'),
    pl.lit(0).alias('NOTENO')
]).select(['ACCT', 'BRANCH', 'DCURBAL', 'DBALANCE', 'APPRLIMT', 'PRODTYPE', 'DUNDRAWN', 'NOTENO'])

df_btacc = df_btacc.rename({
    'ACCT': 'ACCTNO',
    'DCURBAL': 'CURBAL',
    'DBALANCE': 'BALANCE',
    'DUNDRAWN': 'UNDRAWN'
})

df_btacc.write_parquet(f'{AMC_DIR}BTACC.parquet')

df_amcacc = pl.concat([df_lnacc, df_btacc]).sort('ACCTNO')

df_amcacc = df_amcacc.join(df_cis.sort('ACCTNO'), on='ACCTNO', how='inner')
df_amcacc = df_amcacc.filter(pl.col('ICNO').str.strip_chars() != '')

df_amcacc = df_amcacc.sort(['BRANCH', 'ICNO'])

df_amclimt = df_amcacc.group_by(['BRANCH', 'ICNO']).agg([
    pl.col('APPRLIMT').sum()
])

df_amclimt = df_amclimt.filter(pl.col('APPRLIMT') > 1000000).select(['BRANCH', 'ICNO'])

df_amcacc = df_amclimt.join(df_amcacc.sort(['BRANCH', 'ICNO']), on=['BRANCH', 'ICNO'], how='inner')

df_brhdata = pl.read_parquet(BRHFILE).select(['BRANCH', 'BRABBR'])

df_amc_current = df_amcacc.sort('BRANCH').join(df_brhdata.sort('BRANCH'), on='BRANCH', how='left')

df_amc_current.write_parquet(f'{AMC_DIR}AMC{reptmon}{nowk}.parquet')

df_camc = df_amc_current.sort(['BRANCH', 'ICNO', 'ACCTNO', 'NOTENO'])

df_camc = df_camc.with_columns([
    pl.col('APPRLIMT').fill_null(0),
    pl.col('BALANCE').fill_null(0),
    pl.col('UNDRAWN').fill_null(0)
])

df_camc = df_camc.with_columns([
    pl.when((pl.col('APPRLIMT') > 0) & (pl.col('PRODTYPE') != 'FL'))
      .then(((pl.col('APPRLIMT') - pl.col('UNDRAWN')) / pl.col('APPRLIMT') * 100).round(2))
      .otherwise(pl.lit(None))
      .alias('CPERCT')
])

df_pamc = pl.read_parquet(f'{AMC_DIR}AMC{reptmon1}{nowk}.parquet')
df_pamc = df_pamc.sort(['BRANCH', 'ICNO', 'ACCTNO', 'NOTENO']).rename({
    'APPRLIMT': 'PLIMT',
    'BALANCE': 'PBAL',
    'UNDRAWN': 'PUNDRAWN'
})

df_pamc = df_pamc.with_columns([
    pl.col('PLIMT').fill_null(0),
    pl.col('PBAL').fill_null(0),
    pl.col('PUNDRAWN').fill_null(0)
])

df_pamc = df_pamc.with_columns([
    pl.when((pl.col('PLIMT') > 0) & (pl.col('PRODTYPE') != 'FL'))
      .then(((pl.col('PLIMT') - pl.col('PUNDRAWN')) / pl.col('PLIMT') * 100).round(2))
      .otherwise(pl.lit(None))
      .alias('PPERCT')
])

df_amc = df_pamc.join(df_camc, on=['BRANCH', 'ICNO', 'ACCTNO', 'NOTENO'], how='outer_coalesce')

df_amc = df_amc.sort(['BRANCH', 'CUSTNAME', 'PRODTYPE'])

def write_coldbr_report(df):
    lines = []
    current_branch = None
    page_count = 0
    line_count = 0
    
    bcuaplmt = bcuosbal = bcuudraw = 0
    bpraplmt = bprosbal = bprudraw = 0
    cuaplmt = cuosbal = cuudraw = 0
    praplmt = prosbal = prudraw = 0
    
    current_custname = None
    
    for row in df.iter_rows(named=True):
        if current_branch != row['BRANCH']:
            if current_branch is not None:
                lines.append(f"BR TOTAL: {bcuaplmt:>14,.2f}{bcuosbal:>14,.2f}                        {bpraplmt:>14,.2f}{bprosbal:>14,.2f}")
                lines.append('=' * 132)
            
            bcuaplmt = bcuosbal = bcuudraw = 0
            bpraplmt = bprosbal = bprudraw = 0
            current_branch = row['BRANCH']
            page_count += 1
            line_count = 0
            
            lines.append(' ' * 118 + f"PAGE NO : {page_count}")
            lines.append("PUBLIC BANK BERHAD")
            lines.append(f"DETAIL REPORT ON CUSTOMER UNDER ACCOUNT MANAGEMENT CONCEPT(AMC) AS AT {rdate}")
            lines.append("REPORT ID: EIMBRAMC")
            lines.append("     ")
            lines.append(f"BRANCH CODE= {row['BRANCH']:03d}")
            lines.append("                            CURRENT MONTH                             PREVIOUS MONTH")
            lines.append("NAME(TEL/NO)/               ---------------------------------------   ---------------------------------------")
            lines.append("I/C NO       FACILITY  APP/OPER LIMIT    O/S BALANCE  UTILISED  APP/OPER LIMIT    O/S BALANCE  UTILISED  OFFICER    REMARKS")
            lines.append('-' * 132)
            line_count = 9
        
        if current_custname != row['CUSTNAME']:
            if current_custname is not None:
                cupcent = ((cuaplmt - cuudraw) / cuaplmt * 100).round(2) if cuaplmt > 0 else 0
                prpcent = ((praplmt - prudraw) / praplmt * 100).round(2) if praplmt > 0 else 0
                
                lines.append('-' * 132)
                lines.append(f"TOTAL:  {cuaplmt:>14,.2f}{cuosbal:>14,.2f}{cupcent:>6.2f}%  {praplmt:>14,.2f}{prosbal:>14,.2f}{prpcent:>6.2f}%")
                lines.append('-' * 132)
                lines.append("      ")
                line_count += 4
                
                cuaplmt = cuosbal = cuudraw = 0
                praplmt = prosbal = prudraw = 0
            
            current_custname = row['CUSTNAME']
            cust = f"{row['CUSTNAME']} ({row['PRIPHONE']}/{row['SECPHONE']})"
            lines.append(cust)
            line_count += 1
        
        if line_count > 55:
            page_count += 1
            lines.append(' ' * 118 + f"PAGE NO : {page_count}")
            lines.append("PUBLIC BANK BERHAD")
            lines.append(f"DETAIL REPORT ON CUSTOMER UNDER ACCOUNT MANAGEMENT CONCEPT(AMC) AS AT {rdate}")
            line_count = 3
        
        cperct_str = f"{row['CPERCT']:>6.2f}%" if row['CPERCT'] is not None else "      "
        pperct_str = f"{row['PPERCT']:>6.2f}%" if row['PPERCT'] is not None else "      "
        
        lines.append(
            f"{row['ICNO']:<15}{row['PRODTYPE']:<7}"
            f"{row['APPRLIMT']:>14,.2f}{row['BALANCE']:>14,.2f}{cperct_str} "
            f"{row['PLIMT']:>14,.2f}{row['PBAL']:>14,.2f}{pperct_str}"
        )
        line_count += 1
        
        bcuaplmt += row['APPRLIMT']
        bcuosbal += row['BALANCE']
        bcuudraw += row['UNDRAWN']
        bpraplmt += row['PLIMT']
        bprosbal += row['PBAL']
        bprudraw += row['PUNDRAWN']
        
        cuaplmt += row['APPRLIMT']
        cuosbal += row['BALANCE']
        cuudraw += row['UNDRAWN']
        praplmt += row['PLIMT']
        prosbal += row['PBAL']
        prudraw += row['PUNDRAWN']
    
    if current_custname is not None:
        cupcent = ((cuaplmt - cuudraw) / cuaplmt * 100) if cuaplmt > 0 else 0
        prpcent = ((praplmt - prudraw) / praplmt * 100) if praplmt > 0 else 0
        
        lines.append('-' * 132)
        lines.append(f"TOTAL:  {cuaplmt:>14,.2f}{cuosbal:>14,.2f}{cupcent:>6.2f}%  {praplmt:>14,.2f}{prosbal:>14,.2f}{prpcent:>6.2f}%")
        lines.append('-' * 132)
        lines.append("      ")
    
    if current_branch is not None:
        lines.append(f"BR TOTAL: {bcuaplmt:>14,.2f}{bcuosbal:>14,.2f}                        {bpraplmt:>14,.2f}{bprosbal:>14,.2f}")
        lines.append('=' * 132)
    
    with open(COLDBR, 'w') as f:
        for line in lines:
            f.write(line + '\n')

write_coldbr_report(df_amc)

df_pamc_summary = df_pamc.with_columns([
    pl.when(pl.col('PRODTYPE') == 'FL')
      .then(pl.col('PLIMT'))
      .otherwise(pl.lit(0))
      .alias('PFLLIMT'),
    pl.when(pl.col('PRODTYPE') == 'FL')
      .then(pl.lit(0))
      .otherwise(pl.col('PUNDRAWN'))
      .alias('PUNDRAW'),
    pl.when(pl.col('PRODTYPE') != 'FL')
      .then(pl.col('PLIMT'))
      .otherwise(pl.lit(0))
      .alias('PODLIMT')
]).group_by(['BRANCH', 'ICNO']).agg([
    pl.col('PFLLIMT').sum(),
    pl.col('PODLIMT').sum(),
    pl.col('PBAL').sum(),
    pl.col('PUNDRAW').sum()
]).group_by('BRANCH').agg([
    pl.col('PFLLIMT').sum(),
    pl.col('PODLIMT').sum(),
    pl.col('PBAL').sum(),
    pl.col('PUNDRAW').sum(),
    pl.len().alias('PCUST')
])

df_camc_summary = df_camc.with_columns([
    pl.when(pl.col('PRODTYPE') == 'FL')
      .then(pl.col('APPRLIMT'))
      .otherwise(pl.lit(0))
      .alias('CFLLIMT'),
    pl.when(pl.col('PRODTYPE') == 'FL')
      .then(pl.lit(0))
      .otherwise(pl.col('UNDRAWN'))
      .alias('CUNDRAW'),
    pl.when(pl.col('PRODTYPE') != 'FL')
      .then(pl.col('APPRLIMT'))
      .otherwise(pl.lit(0))
      .alias('CODLIMT')
]).group_by(['BRANCH', 'ICNO']).agg([
    pl.col('CFLLIMT').sum(),
    pl.col('CODLIMT').sum(),
    pl.col('BALANCE').sum(),
    pl.col('CUNDRAW').sum()
]).group_by('BRANCH').agg([
    pl.col('CFLLIMT').sum(),
    pl.col('CODLIMT').sum(),
    pl.col('BALANCE').sum(),
    pl.col('CUNDRAW').sum(),
    pl.len().alias('CCUST')
])

df_amclimt_summary = df_camc_summary.join(df_pamc_summary, on='BRANCH', how='outer_coalesce')

df_amclimt_summary = df_amclimt_summary.with_columns([
    pl.when(pl.col('CODLIMT') > 0)
      .then(((pl.col('CODLIMT') - pl.col('CUNDRAW')) / pl.col('CODLIMT') * 100).round(2))
      .otherwise(pl.lit(0))
      .alias('CPERCT'),
    pl.when(pl.col('PODLIMT') > 0)
      .then(((pl.col('PODLIMT') - pl.col('PUNDRAW')) / pl.col('PODLIMT') * 100).round(2))
      .otherwise(pl.lit(0))
      .alias('PPERCT')
])

def write_coldhq_report(df):
    lines = []
    page_count = 1
    line_count = 0
    
    lines.append(' ' * 114 + f"PAGE NO : {page_count}")
    lines.append("PUBLIC BANK BERHAD")
    lines.append(f"SUMMARY REPORT BY BRANCH ON CUSTOMER UNDER ACCOUNT MANAGEMENT CONCEPT(AMC) AS AT {rdate}")
    lines.append("REPORT ID: EIMBRAMC")
    lines.append("     ")
    lines.append(f"                       {reptmon}/{reptyr}                                               {reptmon1}/{reptyr1}")
    lines.append("     ---------------------------------------------------   ---------------------------------------------------")
    lines.append("     NO OF    APPROVED/                TOTAL       %       NO OF    APPROVED/                TOTAL       %")
    lines.append("BR   CUST     OPERATIVE LTD            OUTSTANDING UTILISED CUST     OPERATIVE LTD            OUTSTANDING UTILISED")
    lines.append("              RM(`000)                 BALANCE                       RM(`000)                 BALANCE")
    lines.append("                                       (FL+OD+TB)  (OD+TB)                                    (FL+OD+TB)  (OD+TB)")
    lines.append("              FL       OD+TB           RM(`000)                      FL       OD+TB           RM(`000)")
    lines.append('-' * 107)
    line_count = 12
    
    tccust = tcfllimt = tcodlimt = tbalance = tcundraw = 0
    tpcust = tpfllimt = tpodlimt = tpbal = tpundraw = 0
    
    for row in df.iter_rows(named=True):
        if line_count > 55:
            page_count += 1
            lines.append(' ' * 114 + f"PAGE NO : {page_count}")
            line_count = 1
        
        cfllimt = round(row['CFLLIMT'] / 1000, 1)
        codlimt = round(row['CODLIMT'] / 1000, 1)
        balance = round(row['BALANCE'] / 1000, 1)
        cundraw = round(row['CUNDRAW'] / 1000, 1)
        pfllimt = round(row['PFLLIMT'] / 1000, 1)
        podlimt = round(row['PODLIMT'] / 1000, 1)
        pbal = round(row['PBAL'] / 1000, 1)
        pundraw = round(row['PUNDRAW'] / 1000, 1)
        
        lines.append(
            f"{row['BRANCH']:03d} {row['CCUST']:>7,} {cfllimt:>10,.0f} {codlimt:>10,.0f} {balance:>11,.0f}    "
            f"{row['CPERCT']:>6.2f} {row['PCUST']:>7,} {pfllimt:>10,.0f} {podlimt:>10,.0f} {pbal:>11,.0f}    {row['PPERCT']:>6.2f}"
        )
        line_count += 1
        
        tccust += row['CCUST']
        tcfllimt += cfllimt
        tcodlimt += codlimt
        tbalance += balance
        tcundraw += cundraw
        tpcust += row['PCUST']
        tpfllimt += pfllimt
        tpodlimt += podlimt
        tpbal += pbal
        tpundraw += pundraw
    
    tcperct = ((tcodlimt - tcundraw) / tcodlimt * 100).round(2) if tcodlimt > 0 else 0
    tpperct = ((tpodlimt - tpundraw) / tpodlimt * 100).round(2) if tpodlimt > 0 else 0
    
    lines.append('-' * 107)
    lines.append(
        f"TOT {tccust:>7,} {tcfllimt:>10,.0f} {tcodlimt:>10,.0f} {tbalance:>11,.0f}    "
        f"{tcperct:>6.2f} {tpcust:>7,} {tpfllimt:>10,.0f} {tpodlimt:>10,.0f} {tpbal:>11,.0f}    {tpperct:>6.2f}"
    )
    lines.append('=' * 107)
    
    with open(COLDHQ, 'w') as f:
        for line in lines:
            f.write(line + '\n')

write_coldhq_report(df_amclimt_summary)

print(f"Reports generated:")
print(f"  {COLDBR}")
print(f"  {COLDHQ}")
