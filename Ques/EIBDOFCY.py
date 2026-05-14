import polars as pl
import duckdb
from pathlib import Path
import datetime
import sys

from REPTDATE import get_reptdate_values

# Configuration
base = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

depo_path = base / "input/uat/DEPO"
loan_path = base / "input/uat/LOAN"
output_path = base / "output/EIBDOFCY"
output_path.mkdir(exist_ok=True)

# # DATA REPTDATE; SET DEPO.REPTDATE;
# reptdate_df = pl.read_parquet(depo_path / "REPTDATE.parquet")

# # CALL SYMPUT equivalent
# first_row = reptdate_df.row(0)
# RDATE = first_row['REPTDATE'].strftime('%d%m%Y')  # DDMMYY10.

# REPORT DATE DERIVATION  (shared equivalent of SAS DATA REPTDATE step)
reptdate_values = get_reptdate_values(year_format="%Y")

REPTDATE = reptdate_values.reptdate
REPTYEAR = reptdate_values.reptyear
REPTMON  = reptdate_values.reptmon
REPTDAY  = reptdate_values.reptday
NOWK     = reptdate_values.nowk
RDATE    = REPTDATE.strftime("%d/%m/%y")

print(f"RDATE: {RDATE}")

# DATA FD; SET DEPO.FD;
fd_df = pl.read_parquet(depo_path / "FD.parquet").filter(
    pl.col('CURCODE') != 'MYR'
).with_columns([
    # IF CUSTCODE IN ('77','78','95','96') THEN DO;
    pl.when(pl.col('CUSTCODE').is_in(['77', '78', '95', '96']))
    .then(pl.struct([
        pl.lit('A').alias('IND'),  # INDFD
        pl.col('CURBAL').alias('IFDBAL'),
        pl.lit(None).alias('CFDBAL')  # Initialize as null
    ]))
    .otherwise(pl.struct([
        pl.lit('B').alias('IND'),  # COPRFD
        pl.lit(None).alias('IFDBAL'),  # Initialize as null
        pl.col('CURBAL').alias('CFDBAL')
    ]))
    .alias('ind_data')
]).with_columns([
    pl.col('ind_data').struct.field('IND').alias('IND'),
    pl.col('ind_data').struct.field('IFDBAL').alias('IFDBAL'),
    pl.col('ind_data').struct.field('CFDBAL').alias('CFDBAL')
]).select([
    'IND', 'CURCODE', 'CURBAL', 'IFDBAL', 'CFDBAL'
])

# PROC SORT; BY IND CURCODE;
fd_sorted = fd_df.sort(['IND', 'CURCODE'])

# PROC SUMMARY DATA=FD NWAY; BY IND CURCODE;
fd_summary = fd_sorted.group_by(['IND', 'CURCODE']).agg([
    pl.col('CURBAL').sum().alias('CURBAL'),
    pl.col('IFDBAL').sum().alias('IFDBAL'),
    pl.col('CFDBAL').sum().alias('CFDBAL')
])

fd_summary.write_parquet(output_path / "FD.parquet")
fd_summary.write_csv(output_path / "FD.csv")

# DATA CURR; SET DEPO.CURRENT;
curr_df = pl.read_parquet(depo_path / "CURRENT.parquet").filter(
    pl.col('CURCODE') != 'MYR'
).with_columns([
    # IF CUSTCODE IN ('77','78','95','96') THEN DO;
    pl.when(pl.col('CUSTCODE').is_in(['77', '78', '95', '96']))
    .then(pl.struct([
        pl.lit('C').alias('IND'),  # INDCA
        pl.col('CURBAL').alias('ICABAL'),
        pl.lit(None).alias('CCABAL')
    ]))
    .otherwise(pl.struct([
        pl.lit('D').alias('IND'),  # COPRCA
        pl.lit(None).alias('ICABAL'),
        pl.col('CURBAL').alias('CCABAL')
    ]))
    .alias('ind_data')
]).with_columns([
    pl.col('ind_data').struct.field('IND').alias('IND'),
    pl.col('ind_data').struct.field('ICABAL').alias('ICABAL'),
    pl.col('ind_data').struct.field('CCABAL').alias('CCABAL')
]).select([
    'IND', 'CURCODE', 'CURBAL', 'ICABAL', 'CCABAL'
])

# PROC SORT; BY IND CURCODE;
curr_sorted = curr_df.sort(['IND', 'CURCODE'])

# PROC SUMMARY DATA=CURR NWAY; BY IND CURCODE;
curr_summary = curr_sorted.group_by(['IND', 'CURCODE']).agg([
    pl.col('CURBAL').sum().alias('CURBAL'),
    pl.col('ICABAL').sum().alias('ICABAL'),
    pl.col('CCABAL').sum().alias('CCABAL')
])

curr_summary.write_parquet(output_path / "CURR.parquet")
curr_summary.write_csv(output_path / "CURR.csv")

# DATA LOAN; SET LOAN.LNNOTE;
loan_df = pl.read_parquet(loan_path / "LNNOTE.parquet").filter(
    pl.col('CURCODE') != 'MYR'
).with_columns([
    # IF CUSTCODE IN ('77','78','95','96') THEN DO;
    pl.when(pl.col('CUSTCODE').is_in(['77', '78', '95', '96']))
    .then(pl.struct([
        pl.lit('E').alias('IND'),  # INDLN
        pl.col('CURBAL').alias('ILNBAL'),
        pl.lit(None).alias('CLNBAL')
    ]))
    .otherwise(pl.struct([
        pl.lit('F').alias('IND'),  # COPRLN
        pl.lit(None).alias('ILNBAL'),
        pl.col('CURBAL').alias('CLNBAL')
    ]))
    .alias('ind_data')
]).with_columns([
    pl.col('ind_data').struct.field('IND').alias('IND'),
    pl.col('ind_data').struct.field('ILNBAL').alias('ILNBAL'),
    pl.col('ind_data').struct.field('CLNBAL').alias('CLNBAL')
]).select([
    'IND', 'CURCODE', 'CURBAL', 'ILNBAL', 'CLNBAL'
])

# PROC SORT; BY IND CURCODE;
loan_sorted = loan_df.sort(['IND', 'CURCODE'])

# PROC SUMMARY DATA=LOAN NWAY; BY IND CURCODE;
loan_summary = loan_sorted.group_by(['IND', 'CURCODE']).agg([
    pl.col('CURBAL').sum().alias('CURBAL'),
    pl.col('ILNBAL').sum().alias('ILNBAL'),
    pl.col('CLNBAL').sum().alias('CLNBAL')
])

loan_summary.write_parquet(output_path / "LOAN.parquet")
loan_summary.write_csv(output_path / "LOAN.csv")

# DATA FCY; SET FD CURR LOAN;
fcy_combined = pl.concat([fd_summary, curr_summary, loan_summary], how="diagonal")

# Sort by IND for BY group processing
fcy_sorted = fcy_combined.sort('IND')

# Generate detailed report - equivalent to DATA FCY with FILE OUTFCY
with open(output_path / "OUTFCY.txt", "w") as f:
    f.write("REPORT ID : EIBDOFCY\n")
    f.write("PUBLIC BANK BERHAD\n")
    f.write(f"OUTSTANDING FCY LOAN AND DEPOSITS AS AT {RDATE}\n\n")
    
    current_ind = None
    nobs = 0
    
    for row in fcy_sorted.iter_rows(named=True):
        if row['IND'] != current_ind:
            current_ind = row['IND']
            nobs = 0
            
            # Write section headers
            if row['IND'] == 'A':
                f.write("INDIVIDUAL - FCY FIXED DEPOSIT\n")
            elif row['IND'] == 'B':
                f.write("\n\nNON INDIVIDUAL - FCY FIXED DEPOSIT\n")
            elif row['IND'] == 'C':
                f.write("\n\nINDIVIDUAL - FCY CURRENT\n")
            elif row['IND'] == 'D':
                f.write("\n\nNON INDIVIDUAL - FCY CURRENT\n")
            elif row['IND'] == 'E':
                f.write("\n\nINDIVIDUAL - FCY LOAN\n")
            elif row['IND'] == 'F':
                f.write("\n\nNON INDIVIDUAL - FCY LOAN\n")
            
            # Write column headers
            f.write("\nOBS\x05CURCODE\x05FREQ\x05CURRENT BALANCE\n")
        
        nobs += 1
        # Write data row with DLM (ASCII 05) as separator
        f.write(f"{nobs:3d}\x05{row['CURCODE']:3}\x05{row.get('_FREQ_', 1):10d}\x05{row['CURBAL']:20,.2f}\n")

print("Detailed FCY report generated")

# PROC SORT; BY CURCODE;
fcy_by_currency = fcy_combined.sort('CURCODE')

# PROC SUMMARY DATA=FCY NWAY; BY CURCODE;
fcy_currency_summary = fcy_by_currency.group_by('CURCODE').agg([
    pl.col('IFDBAL').sum().alias('IFDBAL'),
    pl.col('CFDBAL').sum().alias('CFDBAL'),
    pl.col('ICABAL').sum().alias('ICABAL'),
    pl.col('CCABAL').sum().alias('CCABAL'),
    pl.col('ILNBAL').sum().alias('ILNBAL'),
    pl.col('CLNBAL').sum().alias('CLNBAL'),
    pl.col('CURBAL').sum().alias('CURBAL')
])

fcy_currency_summary.write_parquet(output_path / "FCY.parquet")
fcy_currency_summary.write_csv(output_path / "FCY.csv")

# Generate summary report - equivalent to DATA _NULL_ with FILE OUTFCY MOD
with open(output_path / "OUTFCY.txt", "a") as f:
    f.write("\n\nSUMMARY OF OUTSTANDING FCY LOAN AND DEPOSITS\n\n")
    f.write("NOBS\x05CURCODE\x05FCYFD-INDIV\x05FCYFD-CORP\x05FCYCA-INDIV\x05FCYCA-CORP\x05FCYLN-INDIV\x05FCYLN-CORP\n")
    
    # Calculate running totals
    totifd = 0.0
    totcfd = 0.0
    totica = 0.0
    totcca = 0.0
    totiln = 0.0
    totcln = 0.0
    
    for i, row in enumerate(fcy_currency_summary.iter_rows(named=True), 1):
        ifdbal = row.get('IFDBAL', 0) or 0
        cfdbal = row.get('CFDBAL', 0) or 0
        icabal = row.get('ICABAL', 0) or 0
        ccabal = row.get('CCABAL', 0) or 0
        ilnbal = row.get('ILNBAL', 0) or 0
        clnbal = row.get('CLNBAL', 0) or 0
        
        f.write(f"{i:3d}\x05{row['CURCODE']:3}\x05{ifdbal:20,.2f}\x05{cfdbal:20,.2f}\x05{icabal:20,.2f}\x05{ccabal:20,.2f}\x05{ilnbal:20,.2f}\x05{clnbal:20,.2f}\n")
        
        totifd += ifdbal
        totcfd += cfdbal
        totica += icabal
        totcca += ccabal
        totiln += ilnbal
        totcln += clnbal
    
    # Write grand totals
    f.write(f"\x05TOTAL\x05{totifd:20,.2f}\x05{totcfd:20,.2f}\x05{totica:20,.2f}\x05{totcca:20,.2f}\x05{totiln:20,.2f}\x05{totcln:20,.2f}\n")

print("Summary FCY report generated")
print("PROCESSING COMPLETED SUCCESSFULLY")
