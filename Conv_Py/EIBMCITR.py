import polars as pl
from datetime import datetime, timedelta
import os

BRHFILE = 'data/brhfile.parquet'
BNM_DIR = 'data/bnm/'
CIT_DIR = 'data/cit/'
CITLIST = 'data/citlist.txt'

today = datetime.now()
first_of_month = datetime(today.year, today.month, 1)
reptdate = first_of_month - timedelta(days=1)

wk1 = '01'
wk2 = '02'
wk3 = '03'
wk4 = '04'
rdate = reptdate.strftime('%d%m%y')
reptmon = reptdate.strftime('%m')
reptyear = reptdate.strftime('%y')
reptyear2 = reptdate.strftime('%Y')

df_branch = pl.read_parquet(BRHFILE)
df_branch = df_branch.filter(
    ((~pl.col('OPENIND').is_in(['C', ' '])) & (pl.col('BRANCH') != 279)) | 
    (pl.col('BRANCH') == 219)
).select(['BRANCH', 'BRABBR'])

tlbtran_files = [
    f'{BNM_DIR}TLBTRAN{reptyear}{reptmon}{wk1}.parquet',
    f'{BNM_DIR}TLBTRAN{reptyear}{reptmon}{wk2}.parquet',
    f'{BNM_DIR}TLBTRAN{reptyear}{reptmon}{wk3}.parquet',
    f'{BNM_DIR}TLBTRAN{reptyear}{reptmon}{wk4}.parquet'
]

df_all = pl.concat([pl.read_parquet(f) for f in tlbtran_files if os.path.exists(f)])
df_all = df_all.filter(pl.col('TRANCODE').is_in([2222, 2223]))
df_all = df_all.sort(['REPTDATE', 'BRANCH'])

df_all = df_all.with_columns([
    ((pl.col('REPTDATE') != pl.col('REPTDATE').shift(1)) | 
     (pl.col('BRANCH') != pl.col('BRANCH').shift(1))).fill_null(True).alias('first_flag')
])

df_all = df_all.with_columns([
    pl.when(pl.col('first_flag'))
      .then(pl.lit(1))
      .otherwise(pl.lit(0))
      .alias('NOACCT')
])

df_cit = df_all.group_by(['REPTDATE', 'BRANCH']).agg([
    pl.col('CASHOUT').sum().alias(f'AMOUNT{reptmon}'),
    pl.col('NOACCT').sum().alias(f'NOACCT{reptmon}')
])

df_cit = df_cit.filter(pl.col(f'AMOUNT{reptmon}') != 0)

df_final = df_cit.group_by('BRANCH').agg([
    pl.col(f'AMOUNT{reptmon}').sum(),
    pl.col(f'NOACCT{reptmon}').sum()
])

cit_file = f'{CIT_DIR}CIT{reptyear}.parquet'

if reptmon == '01':
    df_cit_year = df_final.with_columns([
        pl.lit(0.0).alias('AMOUNT02'), pl.lit(0).alias('NOACCT02'),
        pl.lit(0.0).alias('AMOUNT03'), pl.lit(0).alias('NOACCT03'),
        pl.lit(0.0).alias('AMOUNT04'), pl.lit(0).alias('NOACCT04'),
        pl.lit(0.0).alias('AMOUNT05'), pl.lit(0).alias('NOACCT05'),
        pl.lit(0.0).alias('AMOUNT06'), pl.lit(0).alias('NOACCT06'),
        pl.lit(0.0).alias('AMOUNT07'), pl.lit(0).alias('NOACCT07'),
        pl.lit(0.0).alias('AMOUNT08'), pl.lit(0).alias('NOACCT08'),
        pl.lit(0.0).alias('AMOUNT09'), pl.lit(0).alias('NOACCT09'),
        pl.lit(0.0).alias('AMOUNT10'), pl.lit(0).alias('NOACCT10'),
        pl.lit(0.0).alias('AMOUNT11'), pl.lit(0).alias('NOACCT11'),
        pl.lit(0.0).alias('AMOUNT12'), pl.lit(0).alias('NOACCT12')
    ])
    df_cit_year.write_parquet(cit_file)
else:
    df_existing = pl.read_parquet(cit_file)
    df_cit_year = df_existing.join(df_final, on='BRANCH', how='outer_coalesce')
    df_cit_year.write_parquet(cit_file)

df_cit_year = pl.read_parquet(cit_file).sort('BRANCH')

df_cit_report = df_cit_year.join(df_branch, on='BRANCH', how='inner')

df_cit_report = df_cit_report.with_columns([
    (pl.int_range(1, pl.len() + 1)).alias('NO')
])

amount_cols = [f'AMOUNT{str(i).zfill(2)}' for i in range(1, 13)]
noacct_cols = [f'NOACCT{str(i).zfill(2)}' for i in range(1, 13)]

df_cit_report = df_cit_report.with_columns([
    pl.sum_horizontal([pl.col(c).fill_null(0) for c in amount_cols]).alias('SUMAMT'),
    pl.sum_horizontal([pl.col(c).fill_null(0) for c in noacct_cols]).alias('SUMACT')
])

df_totcit = df_cit_report.select(
    noacct_cols + amount_cols + ['SUMACT', 'SUMAMT']
).sum()

def format_number(val):
    if val is None or (isinstance(val, float) and pl.Float64.is_nan(val)):
        return '0'
    if isinstance(val, float):
        return f'{val:,.0f}'
    return f'{val:,}'

def write_report():
    lines = []
    
    lines.append(f'1CASH-IN-TRANSIT REPORT {reptyear2}')
    
    header1 = (
        f"{'NO':>5} "
        f"{'BRANCH':>6} "
        f"{'BRANCH':>6} "
        f"{'JAN':>17} "
        f"{'NO OF':>17} "
        f"{'FEB':>17} "
        f"{'NO OF':>17} "
        f"{'MAR':>17} "
        f"{'NO OF':>17} "
        f"{'APR':>17} "
        f"{'NO OF':>17} "
        f"{'MAY':>17} "
        f"{'NO OF':>17} "
        f"{'JUN':>17} "
        f"{'NO OF':>17} "
        f"{'JUL':>17} "
        f"{'NO OF':>17} "
        f"{'AUG':>17} "
        f"{'NO OF':>17} "
        f"{'SEP':>17} "
        f"{'NO OF':>17} "
        f"{'OCT':>17} "
        f"{'NO OF':>17} "
        f"{'NOV':>17} "
        f"{'NO OF':>17} "
        f"{'DEC':>17} "
        f"{'NO OF':>17} "
        f"{'TOTAL':>17} "
        f"{'TOTAL NO OF':>17}"
    )
    lines.append(header1)
    
    header2 = (
        f"{'':>5} "
        f"{'CODE':>6} "
        f"{'':>6} "
        f"{'':>17} "
        f"{'TRANSIT':>17} "
        f"{'':>17} "
        f"{'TRANSIT':>17} "
        f"{'':>17} "
        f"{'TRANSIT':>17} "
        f"{'':>17} "
        f"{'TRANSIT':>17} "
        f"{'':>17} "
        f"{'TRANSIT':>17} "
        f"{'':>17} "
        f"{'TRANSIT':>17} "
        f"{'':>17} "
        f"{'TRANSIT':>17} "
        f"{'':>17} "
        f"{'TRANSIT':>17} "
        f"{'':>17} "
        f"{'TRANSIT':>17} "
        f"{'':>17} "
        f"{'TRANSIT':>17} "
        f"{'':>17} "
        f"{'TRANSIT':>17} "
        f"{'':>17} "
        f"{'TRANSIT':>17} "
        f"{'':>17} "
        f"{'TRANSIT':>17}"
    )
    lines.append(header2)
    
    for row in df_cit_report.iter_rows(named=True):
        line = (
            f"{row['NO']:>5} "
            f"{row['BRANCH']:>6} "
            f"{row['BRABBR']:>6} "
            f"{format_number(row.get('AMOUNT01', 0)):>17} "
            f"{format_number(row.get('NOACCT01', 0)):>17} "
            f"{format_number(row.get('AMOUNT02', 0)):>17} "
            f"{format_number(row.get('NOACCT02', 0)):>17} "
            f"{format_number(row.get('AMOUNT03', 0)):>17} "
            f"{format_number(row.get('NOACCT03', 0)):>17} "
            f"{format_number(row.get('AMOUNT04', 0)):>17} "
            f"{format_number(row.get('NOACCT04', 0)):>17} "
            f"{format_number(row.get('AMOUNT05', 0)):>17} "
            f"{format_number(row.get('NOACCT05', 0)):>17} "
            f"{format_number(row.get('AMOUNT06', 0)):>17} "
            f"{format_number(row.get('NOACCT06', 0)):>17} "
            f"{format_number(row.get('AMOUNT07', 0)):>17} "
            f"{format_number(row.get('NOACCT07', 0)):>17} "
            f"{format_number(row.get('AMOUNT08', 0)):>17} "
            f"{format_number(row.get('NOACCT08', 0)):>17} "
            f"{format_number(row.get('AMOUNT09', 0)):>17} "
            f"{format_number(row.get('NOACCT09', 0)):>17} "
            f"{format_number(row.get('AMOUNT10', 0)):>17} "
            f"{format_number(row.get('NOACCT10', 0)):>17} "
            f"{format_number(row.get('AMOUNT11', 0)):>17} "
            f"{format_number(row.get('NOACCT11', 0)):>17} "
            f"{format_number(row.get('AMOUNT12', 0)):>17} "
            f"{format_number(row.get('NOACCT12', 0)):>17} "
            f"{format_number(row.get('SUMAMT', 0)):>17} "
            f"{format_number(row.get('SUMACT', 0)):>17}"
        )
        lines.append(line)
    
    tot_row = df_totcit.to_dicts()[0]
    total_line = (
        f"{'':>5} "
        f"{'':>6} "
        f"{'TOTAL':>6} "
        f"{format_number(tot_row.get('AMOUNT01', 0)):>17} "
        f"{format_number(tot_row.get('NOACCT01', 0)):>17} "
        f"{format_number(tot_row.get('AMOUNT02', 0)):>17} "
        f"{format_number(tot_row.get('NOACCT02', 0)):>17} "
        f"{format_number(tot_row.get('AMOUNT03', 0)):>17} "
        f"{format_number(tot_row.get('NOACCT03', 0)):>17} "
        f"{format_number(tot_row.get('AMOUNT04', 0)):>17} "
        f"{format_number(tot_row.get('NOACCT04', 0)):>17} "
        f"{format_number(tot_row.get('AMOUNT05', 0)):>17} "
        f"{format_number(tot_row.get('NOACCT05', 0)):>17} "
        f"{format_number(tot_row.get('AMOUNT06', 0)):>17} "
        f"{format_number(tot_row.get('NOACCT06', 0)):>17} "
        f"{format_number(tot_row.get('AMOUNT07', 0)):>17} "
        f"{format_number(tot_row.get('NOACCT07', 0)):>17} "
        f"{format_number(tot_row.get('AMOUNT08', 0)):>17} "
        f"{format_number(tot_row.get('NOACCT08', 0)):>17} "
        f"{format_number(tot_row.get('AMOUNT09', 0)):>17} "
        f"{format_number(tot_row.get('NOACCT09', 0)):>17} "
        f"{format_number(tot_row.get('AMOUNT10', 0)):>17} "
        f"{format_number(tot_row.get('NOACCT10', 0)):>17} "
        f"{format_number(tot_row.get('AMOUNT11', 0)):>17} "
        f"{format_number(tot_row.get('NOACCT11', 0)):>17} "
        f"{format_number(tot_row.get('AMOUNT12', 0)):>17} "
        f"{format_number(tot_row.get('NOACCT12', 0)):>17} "
        f"{format_number(tot_row.get('SUMAMT', 0)):>17} "
        f"{format_number(tot_row.get('SUMACT', 0)):>17}"
    )
    lines.append(total_line)
    
    with open(CITLIST, 'w') as f:
        for line in lines:
            f.write(line + '\n')

write_report()

print(f"Report generated: {CITLIST}")
