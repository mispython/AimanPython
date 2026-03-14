"""
Program: KALMPBBP.py
Date: 11/12/96 (Original SAS)
Purpose: RDAL PART II (KAPITI ITEMS)

This program processes domestic assets and liabilities Part II data,
including foreign exchange contracts, derivatives (DCI/options), and NIDs.
"""

import os
import polars as pl
import duckdb
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import math

# =========================================================================
# ENVIRONMENT SETUP
# =========================================================================
REPTMON = os.getenv('REPTMON', '')  # Reporting month
NOWK = os.getenv('NOWK', '')  # Week number
REPTDAY = os.getenv('REPTDAY', '')  # Reporting day
TDATE = os.getenv('TDATE', '')  # Reporting date (YYYYMMDD format)
AMTIND = os.getenv('AMTIND', "'D'")  # Amount indicator
RDATE = os.getenv('RDATE', '')  # Formatted report date
SDESC = os.getenv('SDESC', '')  # System description

# Path Configuration
UPLOAD_DIR = os.getenv('UPLOAD_DIR', '/mnt/user-data/uploads')
OUTPUT_DIR = os.getenv('OUTPUT_DIR', '/mnt/user-data/outputs')
WORK_DIR = '/home/claude'

# Input files
BNMTBL2_FILE = os.path.join(UPLOAD_DIR, 'bnmtbl2.txt')
DCIMTBL_FILE = os.path.join(UPLOAD_DIR, 'dcimtbl.txt')

# Input parquet files
K1TBL_FILE = os.path.join(UPLOAD_DIR, f'K1TBL{REPTMON}{NOWK}.parquet')
K3TBL_FILE = os.path.join(UPLOAD_DIR, f'K3TBL{REPTMON}{NOWK}.parquet')
DCIWH_FILE = os.path.join(UPLOAD_DIR, f'DCI{REPTMON}{NOWK}.parquet')
DCIWH_MAT_FILE = os.path.join(UPLOAD_DIR, f'DCIMATURED{REPTMON}{NOWK}.parquet')
RNID_FILE = os.path.join(UPLOAD_DIR, f'RNID{REPTDAY}.parquet')

# Output files
K2TBL_OUT = os.path.join(OUTPUT_DIR, f'K2TBL{REPTMON}{NOWK}.parquet')
DCIMTB_OUT = os.path.join(OUTPUT_DIR, f'DCIMTB{REPTMON}{NOWK}.parquet')
KALM_OUT = os.path.join(OUTPUT_DIR, f'KALM{REPTMON}{NOWK}.parquet')
KALM_FINAL_OUT = os.path.join(OUTPUT_DIR, f'KALM{REPTMON}{NOWK}_FINAL.parquet')
KALM_REPORT = os.path.join(OUTPUT_DIR, f'KALM{REPTMON}{NOWK}_REPORT.txt')


# =========================================================================
# UTILITY FUNCTIONS
# =========================================================================

def calculate_remaining_months(start_date, end_date):
    """
    Calculate remaining months from start_date to end_date.
    Equivalent to SAS %REMMTH macro.
    """
    if pd.isna(start_date) or pd.isna(end_date):
        return None

    mdyr = end_date.year
    mdmth = end_date.month
    mdday = end_date.day

    rpyr = start_date.year
    rpmth = start_date.month
    rpday = start_date.day

    # Get days in reporting month
    if rpmth == 2:
        rpdays_month = 29 if (rpyr % 4 == 0) else 28
    elif rpmth in [4, 6, 9, 11]:
        rpdays_month = 30
    else:
        rpdays_month = 31

    # Adjust maturity day if needed
    if mdday > rpdays_month:
        mdday = rpdays_month

    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday

    remmth = remy * 12 + remm + (remd / rpdays_month)

    return remmth


def apply_fdrmmt_format(remmth):
    """
    Apply FDRMMT format for remaining maturity classification.
    Maps remaining months to BNM code suffix.
    """
    if remmth is None or pd.isna(remmth):
        return None

    if remmth <= 1:
        return '01'
    elif remmth <= 3:
        return '02'
    elif remmth <= 6:
        return '03'
    elif remmth <= 9:
        return '04'
    elif remmth <= 12:
        return '05'
    else:
        return '06'


def apply_fdorgmt_format(remmth):
    """
    Apply FDORGMT format for original maturity classification.
    Maps original months to BNM code suffix.
    """
    if remmth is None or pd.isna(remmth):
        return None

    if remmth <= 1:
        return '07'
    elif remmth <= 3:
        return '08'
    elif remmth <= 6:
        return '09'
    elif remmth <= 9:
        return '10'
    elif remmth <= 12:
        return '11'
    else:
        return '12'


def apply_orimat_format(nummonth):
    """
    Apply ORIMAT format for original maturity classification.
    Used in K1TABL processing.
    """
    if nummonth is None or pd.isna(nummonth):
        return None

    if nummonth <= 1:
        return '07'
    elif nummonth <= 3:
        return '08'
    elif nummonth <= 6:
        return '09'
    elif nummonth <= 9:
        return '10'
    elif nummonth <= 12:
        return '11'
    else:
        return '12'


# =========================================================================
# READ INPUT FILE - K2TBL (BNMTBL2)
# =========================================================================

def read_k2tbl():
    """
    Read K2TBL input file from BNMTBL2.
    First line contains REPTDATE, subsequent lines contain deal data.
    """
    print("Reading K2TBL from BNMTBL2...")

    # Read first line for REPTDATE
    with open(BNMTBL2_FILE, 'r') as f:
        first_line = f.readline().strip()
        reptdate = datetime.strptime(first_line, '%Y%m%d')

    # Read remaining data
    df = pl.read_csv(
        BNMTBL2_FILE,
        separator='|',
        skip_rows=1,
        has_header=False,
        null_values=[''],
        columns={
            'column_1': pl.Utf8,  # OMABD
            'column_2': pl.Utf8,  # OMAND
            'column_3': pl.Utf8,  # OMASD
            'column_4': pl.Utf8,  # OSAPP
            'column_5': pl.Utf8,  # OSBDT
            'column_6': pl.Float64,  # ORINWP
            'column_7': pl.Float64,  # SALEAMT
            'column_8': pl.Float64,  # ORINWR
            'column_9': pl.Float64,  # PURCAMT
            'column_10': pl.Utf8,  # OMCUST
            'column_11': pl.Utf8,  # OMCLC
            'column_12': pl.Utf8,  # GFCTP
            'column_13': pl.Utf8,  # GFCA2
            'column_14': pl.Utf8,  # GFSAC
            'column_15': pl.Utf8,  # GFCNAL
            'column_16': pl.Utf8,  # OMCCY
            'column_17': pl.Utf8,  # GFCNAR
            'column_18': pl.Utf8,  # GFCNAP
            'column_19': pl.Utf8,  # OSDLP
            'column_20': pl.Utf8,  # OSDLR
            'column_21': pl.Int64,  # OSCTRD
            'column_22': pl.Int64,  # OXPURD
            'column_23': pl.Utf8,  # OMMVT
            'column_24': pl.Utf8,  # OMMVTS
            'column_25': pl.Utf8,  # OSSRC
            'column_26': pl.Utf8,  # OSUC1
            'column_27': pl.Utf8,  # OSUC2
            'column_28': pl.Float64,  # OXAMAP
            'column_29': pl.Float64,  # OXEXR
            'column_30': pl.Utf8,  # OXOPT
            'column_31': pl.Utf8,  # OXPCCY
            'column_32': pl.Utf8,  # OXSCCY
            'column_33': pl.Utf8,  # OMPRF
            'column_34': pl.Utf8,  # OSARC
            'column_35': pl.Utf8,  # OSCANR
        }
    ).rename({
        'column_1': 'OMABD',
        'column_2': 'OMAND',
        'column_3': 'OMASD',
        'column_4': 'OSAPP',
        'column_5': 'OSBDT',
        'column_6': 'ORINWP',
        'column_7': 'SALEAMT',
        'column_8': 'ORINWR',
        'column_9': 'PURCAMT',
        'column_10': 'OMCUST',
        'column_11': 'OMCLC',
        'column_12': 'GFCTP',
        'column_13': 'GFCA2',
        'column_14': 'GFSAC',
        'column_15': 'GFCNAL',
        'column_16': 'OMCCY',
        'column_17': 'GFCNAR',
        'column_18': 'GFCNAP',
        'column_19': 'OSDLP',
        'column_20': 'OSDLR',
        'column_21': 'OSCTRD',
        'column_22': 'OXPURD',
        'column_23': 'OMMVT',
        'column_24': 'OMMVTS',
        'column_25': 'OSSRC',
        'column_26': 'OSUC1',
        'column_27': 'OSUC2',
        'column_28': 'OXAMAP',
        'column_29': 'OXEXR',
        'column_30': 'OXOPT',
        'column_31': 'OXPCCY',
        'column_32': 'OXSCCY',
        'column_33': 'OMPRF',
        'column_34': 'OSARC',
        'column_35': 'OSCANR',
    })

    # Add REPTDATE column
    df = df.with_columns(pl.lit(reptdate).alias('REPTDATE'))

    # Write to parquet
    df.write_parquet(K2TBL_OUT)
    print(f"K2TBL written to {K2TBL_OUT}")

    return df


# =========================================================================
# READ AND PROCESS DCI DATA
# =========================================================================

def read_dcimtbl():
    """
    Read DCI master table from DCIMTBL file.
    ESMR 2013-1170: Incorporate DCI into FISS.
    """
    print("Reading DCIMTBL...")

    # Read first line for REPTDATE (at position 64)
    with open(DCIMTBL_FILE, 'r') as f:
        first_line = f.readline().strip()
        # REPTDATE is at position 64 in DDMMYY10 format
        reptdate_str = first_line[63:73]  # 10 characters
        reptdate = datetime.strptime(reptdate_str, '%d/%m/%Y')

    # Read remaining data
    df = pl.read_csv(
        DCIMTBL_FILE,
        separator='|',
        skip_rows=1,
        has_header=False,
        null_values=[''],
        columns={
            'column_1': pl.Utf8,  # DCDLP
            'column_2': pl.Utf8,  # DCDLR
            'column_3': pl.Utf8,  # DCCUS
            'column_4': pl.Utf8,  # DCCLC
            'column_5': pl.Utf8,  # DCBCCY
            'column_6': pl.Float64,  # DCBAMT
            'column_7': pl.Float64,  # C8SPT
            'column_8': pl.Utf8,  # DCACCY
            'column_9': pl.Utf8,  # CRDD
            'column_10': pl.Utf8,  # MTDD
            'column_11': pl.Utf8,  # GFCTP
            'column_12': pl.Utf8,  # GFC2R1
            'column_13': pl.Utf8,  # GFCUN
            'column_14': pl.Utf8,  # GFCNAL
            'column_15': pl.Utf8,  # DCTRNT
            'column_16': pl.Utf8,  # DCBSI
            'column_17': pl.Utf8,  # ODGRP
            'column_18': pl.Utf8,  # GFSAC
            'column_19': pl.Utf8,  # OPTYPE
            'column_20': pl.Float64,  # STKRATE
        }
    ).rename({
        'column_1': 'DCDLP',
        'column_2': 'DCDLR',
        'column_3': 'DCCUS',
        'column_4': 'DCCLC',
        'column_5': 'DCBCCY',
        'column_6': 'DCBAMT',
        'column_7': 'C8SPT',
        'column_8': 'DCACCY',
        'column_9': 'CRDD',
        'column_10': 'MTDD',
        'column_11': 'GFCTP',
        'column_12': 'GFC2R1',
        'column_13': 'GFCUN',
        'column_14': 'GFCNAL',
        'column_15': 'DCTRNT',
        'column_16': 'DCBSI',
        'column_17': 'ODGRP',
        'column_18': 'GFSAC',
        'column_19': 'OPTYPE',
        'column_20': 'STKRATE',
    })

    # Convert GFC2R1 to numeric GFC2R
    df = df.with_columns([
        pl.col('GFC2R1').cast(pl.Int64).alias('GFC2R')
    ])

    # Convert dates (DDMMYYYY format)
    def parse_ddmmyyyy(date_str):
        if date_str and len(date_str) >= 10:
            day = date_str[0:2]
            month = date_str[3:5]
            year = date_str[6:10]
            return f"{year}-{month}-{day}"
        return None

    df = df.with_columns([
        pl.col('CRDD').map_elements(parse_ddmmyyyy).str.strptime(pl.Date, '%Y-%m-%d').alias('DCCTRD'),
        pl.col('MTDD').map_elements(parse_ddmmyyyy).str.strptime(pl.Date, '%Y-%m-%d').alias('DCMTYD'),
    ]).drop(['CRDD', 'MTDD'])

    # Add REPTDATE
    df = df.with_columns(pl.lit(reptdate).alias('REPTDATE'))

    # Sort by DCDLR
    df = df.sort('DCDLR')

    return df


def read_dciwh():
    """
    Read DCI warehouse data and merge customer information.
    """
    print("Reading DCIWH data...")

    # Read active DCI
    df1 = pl.read_parquet(DCIWH_FILE).select([
        pl.col('CUSTICKETNO').alias('DCDLR'),
        pl.col('CUSTNAME').alias('GFCUN'),
        pl.col('CUSTCODE').alias('GFC2R'),
    ])

    # Read matured DCI
    df2 = pl.read_parquet(DCIWH_MAT_FILE).select([
        pl.col('CUSTICKETNO').alias('DCDLR'),
        pl.col('CUSTNAME').alias('GFCUN'),
        pl.col('CUSTCODE').alias('GFC2R'),
    ])

    # Combine and sort
    df = pl.concat([df1, df2]).sort('DCDLR')

    return df


def process_dcimtb():
    """
    Process DCI master table - merge with warehouse data and apply logic.
    """
    print("Processing DCIMTB...")

    # Read data
    dcimtbl = read_dcimtbl()
    dciwh = read_dciwh()

    # Merge with warehouse data
    df = dcimtbl.join(dciwh, on='DCDLR', how='left', suffix='_wh')

    # Update GFCUN and GFC2R from warehouse if available
    df = df.with_columns([
        pl.when(pl.col('GFCUN_wh').is_not_null())
        .then(pl.col('GFCUN_wh'))
        .otherwise(pl.col('GFCUN'))
        .alias('GFCUN'),

        pl.when(pl.col('GFC2R_wh').is_not_null())
        .then(pl.col('GFC2R_wh'))
        .otherwise(pl.col('GFC2R'))
        .alias('GFC2R'),
    ]).drop(['GFCUN_wh', 'GFC2R_wh'])

    # If DCBCCY='MYR' then C8SPT=1
    df = df.with_columns([
        pl.when(pl.col('DCBCCY') == 'MYR')
        .then(1.0)
        .otherwise(pl.col('C8SPT'))
        .alias('C8SPT')
    ])

    # Write merged DCIMTB
    df.write_parquet(DCIMTB_OUT)
    print(f"DCIMTB written to {DCIMTB_OUT}")

    return df


def process_dci_bnmcodes(df):
    """
    Process DCI data to generate BNMCODE mappings.
    Creates DCIMTB dataset with BNMCODE, AMOUNT, AMTIND.
    """
    print("Processing DCI BNM codes...")

    # Rename for processing
    df = df.with_columns([
        pl.col('DCBAMT').alias('AMOUNT'),
        pl.col('GFC2R').alias('CUSTCD'),
        pl.lit(AMTIND).alias('AMTIND'),
    ])

    # Initialize BNMCODE and DCIAMT
    df = df.with_columns([
        pl.lit(None).cast(pl.Utf8).alias('BNMCODE'),
        pl.lit(None).cast(pl.Float64).alias('DCIAMT'),
    ])

    results = []

    # Process each row
    for row in df.iter_rows(named=True):
        bnmcode = None
        amount = row['AMOUNT']
        custcd = row['CUSTCD']

        # DCI Processing
        if row['DCDLP'] == 'DCI':
            # Customer Leg
            if row['DCBSI'] == 'B' and row['DCTRNT'] == 'C':
                if row['DCBCCY'] == 'MYR':
                    if custcd in [1]:
                        bnmcode = '7850401000000Y'
                    elif custcd in [2]:
                        bnmcode = '7850402000000Y'
                    elif custcd in [3]:
                        bnmcode = '7850403000000Y'
                    elif custcd in [7]:
                        bnmcode = '7850407000000Y'
                    elif custcd in [12]:
                        bnmcode = '7850412000000Y'
                    elif custcd not in list(range(1, 4)) + [7] + list(range(10, 13)) + list(range(80, 100)):
                        bnmcode = '7850415000000Y'
                    elif custcd in list(range(81, 85)):
                        bnmcode = '7850481000000Y'
                    elif custcd in list(range(85, 100)):
                        bnmcode = '7850485000000Y'
                else:
                    if row['DCACCY'] == 'MYR':
                        if custcd in [1]:
                            bnmcode = '6850401000000Y'
                        elif custcd in [2]:
                            bnmcode = '6850402000000Y'
                        elif custcd in [3]:
                            bnmcode = '6850403000000Y'
                        elif custcd in [7]:
                            bnmcode = '6850407000000Y'
                        elif custcd in [12]:
                            bnmcode = '6850412000000Y'
                        elif custcd not in list(range(1, 4)) + [7] + list(range(10, 13)) + list(range(80, 100)):
                            bnmcode = '6850415000000Y'
                        elif custcd in list(range(81, 85)):
                            bnmcode = '6850481000000Y'
                        elif custcd in list(range(85, 100)):
                            bnmcode = '6850485000000Y'

                    # Calculate adjusted amount
                    if row['DCBCCY'] == 'JPY':
                        dciamt = round(amount, 0)
                    else:
                        dciamt = round(amount, 2)
                    amount = dciamt * row['STKRATE']

            # Interbank Leg
            elif row['DCBSI'] == 'S' and row['DCTRNT'] == 'I':
                custcd_str = str(custcd).zfill(2) if isinstance(custcd, int) else custcd

                if row['DCBCCY'] == 'MYR':
                    if custcd_str in ['01', '02', '03', '07', '12', '15', '81', '85']:
                        if row['GFCTP'] in ['BC']:
                            bnmcode = '6850401000000Y'
                        elif row['GFCTP'] in ['BB']:
                            bnmcode = '6850402000000Y'
                        elif row['GFCTP'] in ['BI']:
                            bnmcode = '6850403000000Y'
                        elif row['GFCTP'] in ['BJ']:
                            bnmcode = '6850407000000Y'
                        elif row['GFCTP'] in ['BM']:
                            bnmcode = '6850412000000Y'
                        elif row['GFCTP'] in ['BA', 'BE']:
                            bnmcode = '6850481000000Y'
                        elif (row['GFCTP'] not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW']
                              and row['GFCNAL'] == 'MY'):
                            bnmcode = '6850415000000Y'
                        elif (row['GFCTP'] not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW']
                              and row['GFCNAL'] not in [' ', 'MY']):
                            bnmcode = '6850485000000Y'
                else:
                    if row['DCACCY'] == 'MYR':
                        if custcd_str in ['01', '02', '03', '07', '12', '15', '81', '85']:
                            if row['GFCTP'] in ['BC']:
                                bnmcode = '7850401000000Y'
                            elif row['GFCTP'] in ['BB']:
                                bnmcode = '7850402000000Y'
                            elif row['GFCTP'] in ['BI']:
                                bnmcode = '7850403000000Y'
                            elif row['GFCTP'] in ['BJ']:
                                bnmcode = '7850407000000Y'
                            elif row['GFCTP'] in ['BM']:
                                bnmcode = '7850412000000Y'
                            elif row['GFCTP'] in ['BA', 'BE']:
                                bnmcode = '7850481000000Y'
                            elif (row['GFCTP'] not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW']
                                  and row['GFCNAL'] == 'MY'):
                                bnmcode = '7850415000000Y'
                            elif (row['GFCTP'] not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW']
                                  and row['GFCNAL'] not in [' ', 'MY']):
                                bnmcode = '7850485000000Y'

                        # Calculate adjusted amount
                        if row['DCBCCY'] == 'JPY':
                            dciamt = round(amount, 0)
                        else:
                            dciamt = round(amount, 2)
                        amount = dciamt * row['STKRATE']

        # Foreign Exchange Options (RFO, TFO, PFO)
        elif row['DCDLP'] in ['RFO', 'TFO', 'PFO']:
            bnmcode = process_fx_options(row, custcd)

            # Adjust amount for certain conditions
            if bnmcode and row['DCBCCY'] not in ['MYR'] and row['DCACCY'] == 'MYR':
                if row['DCBCCY'] == 'JPY':
                    dciamt = round(amount, 0)
                else:
                    dciamt = round(amount, 2)
                amount = dciamt * row['STKRATE']

        # Output if BNMCODE is assigned
        if bnmcode:
            results.append({
                'BNMCODE': bnmcode,
                'AMOUNT': amount,
                'AMTIND': AMTIND
            })

    # Create DataFrame from results
    result_df = pl.DataFrame(results)

    return result_df


def process_fx_options(row, custcd):
    """
    Process foreign exchange options (RFO, TFO, PFO).
    This function handles the complex nested IF-THEN logic for options.
    """
    bnmcode = None
    custcd_str = str(custcd).zfill(2) if isinstance(custcd, int) else custcd

    # BUY + CALL
    if row['DCBSI'] == 'B' and row['OPTYPE'] == 'CALL':
        if row['DCBCCY'] == 'MYR':
            if row['DCTRNT'] == 'C':
                if custcd in [1]:
                    bnmcode = '7850401000000Y'
                elif custcd in [2]:
                    bnmcode = '7850402000000Y'
                elif custcd in [3]:
                    bnmcode = '7850403000000Y'
                elif custcd in [7]:
                    bnmcode = '7850407000000Y'
                elif custcd in [12]:
                    bnmcode = '7850412000000Y'
                elif custcd not in list(range(1, 4)) + [7] + list(range(10, 13)) + list(range(80, 100)):
                    bnmcode = '7850415000000Y'
                elif custcd in list(range(81, 85)):
                    bnmcode = '7850481000000Y'
                elif custcd in list(range(85, 100)):
                    bnmcode = '7850485000000Y'
            elif row['DCTRNT'] == 'I':
                if custcd_str in ['01', '02', '03', '07', '12', '15', '81', '85']:
                    if row['GFCTP'] in ['BC']:
                        bnmcode = '7850401000000Y'
                    elif row['GFCTP'] in ['BB']:
                        bnmcode = '7850402000000Y'
                    elif row['GFCTP'] in ['BI']:
                        bnmcode = '7850403000000Y'
                    elif row['GFCTP'] in ['BJ']:
                        bnmcode = '7850407000000Y'
                    elif row['GFCTP'] in ['BM']:
                        bnmcode = '7850412000000Y'
                    elif row['GFCTP'] in ['BA', 'BE']:
                        bnmcode = '7850481000000Y'
                    elif (row['GFCTP'] not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW']
                          and row['GFCNAL'] == 'MY'):
                        bnmcode = '7850415000000Y'
                    elif (row['GFCTP'] not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW']
                          and row['GFCNAL'] not in [' ', 'MY']):
                        bnmcode = '7850485000000Y'
        else:
            if row['DCACCY'] == 'MYR':
                if row['DCTRNT'] == 'C':
                    if custcd in [1]:
                        bnmcode = '6850401000000Y'
                    elif custcd in [2]:
                        bnmcode = '6850402000000Y'
                    elif custcd in [3]:
                        bnmcode = '6850403000000Y'
                    elif custcd in [7]:
                        bnmcode = '6850407000000Y'
                    elif custcd in [12]:
                        bnmcode = '6850412000000Y'
                    elif custcd not in list(range(1, 4)) + [7] + list(range(10, 13)) + list(range(80, 100)):
                        bnmcode = '6850415000000Y'
                    elif custcd in list(range(81, 85)):
                        bnmcode = '6850481000000Y'
                    elif custcd in list(range(85, 100)):
                        bnmcode = '6850485000000Y'
                elif row['DCTRNT'] == 'I':
                    if custcd_str in ['01', '02', '03', '07', '12', '15', '81', '85']:
                        if row['GFCTP'] in ['BC']:
                            bnmcode = '6850401000000Y'
                        elif row['GFCTP'] in ['BB']:
                            bnmcode = '6850402000000Y'
                        elif row['GFCTP'] in ['BI']:
                            bnmcode = '6850403000000Y'
                        elif row['GFCTP'] in ['BJ']:
                            bnmcode = '6850407000000Y'
                        elif row['GFCTP'] in ['BM']:
                            bnmcode = '6850412000000Y'
                        elif row['GFCTP'] in ['BA', 'BE']:
                            bnmcode = '6850481000000Y'
                        elif (row['GFCTP'] not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW']
                              and row['GFCNAL'] == 'MY'):
                            bnmcode = '6850415000000Y'
                        elif (row['GFCTP'] not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW']
                              and row['GFCNAL'] not in [' ', 'MY']):
                            bnmcode = '6850485000000Y'

    # BUY + PUT
    elif row['DCBSI'] == 'B' and row['OPTYPE'] == 'PUT':
        if row['DCBCCY'] == 'MYR':
            if row['DCTRNT'] == 'C':
                if custcd in [1]:
                    bnmcode = '6850401000000Y'
                elif custcd in [2]:
                    bnmcode = '6850402000000Y'
                elif custcd in [3]:
                    bnmcode = '6850403000000Y'
                elif custcd in [7]:
                    bnmcode = '6850407000000Y'
                elif custcd in [12]:
                    bnmcode = '6850412000000Y'
                elif custcd not in list(range(1, 4)) + [7] + list(range(10, 13)) + list(range(80, 100)):
                    bnmcode = '6850415000000Y'
                elif custcd in list(range(81, 85)):
                    bnmcode = '6850481000000Y'
                elif custcd in list(range(85, 100)):
                    bnmcode = '6850485000000Y'
            elif row['DCTRNT'] == 'I':
                if custcd_str in ['01', '02', '03', '07', '12', '15', '81', '85']:
                    if row['GFCTP'] in ['BC']:
                        bnmcode = '6850401000000Y'
                    elif row['GFCTP'] in ['BB']:
                        bnmcode = '6850402000000Y'
                    elif row['GFCTP'] in ['BI']:
                        bnmcode = '6850403000000Y'
                    elif row['GFCTP'] in ['BJ']:
                        bnmcode = '6850407000000Y'
                    elif row['GFCTP'] in ['BM']:
                        bnmcode = '6850412000000Y'
                    elif row['GFCTP'] in ['BA', 'BE']:
                        bnmcode = '6850481000000Y'
                    elif (row['GFCTP'] not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW']
                          and row['GFCNAL'] == 'MY'):
                        bnmcode = '6850415000000Y'
                    elif (row['GFCTP'] not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW']
                          and row['GFCNAL'] not in [' ', 'MY']):
                        bnmcode = '6850485000000Y'
        else:
            if row['DCACCY'] == 'MYR':
                if row['DCTRNT'] == 'C':
                    if custcd in [1]:
                        bnmcode = '7850401000000Y'
                    elif custcd in [2]:
                        bnmcode = '7850402000000Y'
                    elif custcd in [3]:
                        bnmcode = '7850403000000Y'
                    elif custcd in [7]:
                        bnmcode = '7850407000000Y'
                    elif custcd in [12]:
                        bnmcode = '7850412000000Y'
                    elif custcd not in list(range(1, 4)) + [7] + list(range(10, 13)) + list(range(80, 100)):
                        bnmcode = '7850415000000Y'
                    elif custcd in list(range(81, 85)):
                        bnmcode = '7850481000000Y'
                    elif custcd in list(range(85, 100)):
                        bnmcode = '7850485000000Y'
                elif row['DCTRNT'] == 'I':
                    if custcd_str in ['01', '02', '03', '07', '12', '15', '81', '85']:
                        if row['GFCTP'] in ['BC']:
                            bnmcode = '7850401000000Y'
                        elif row['GFCTP'] in ['BB']:
                            bnmcode = '7850402000000Y'
                        elif row['GFCTP'] in ['BI']:
                            bnmcode = '7850403000000Y'
                        elif row['GFCTP'] in ['BJ']:
                            bnmcode = '7850407000000Y'
                        elif row['GFCTP'] in ['BM']:
                            bnmcode = '7850412000000Y'
                        elif row['GFCTP'] in ['BA', 'BE']:
                            bnmcode = '7850481000000Y'
                        elif (row['GFCTP'] not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW']
                              and row['GFCNAL'] == 'MY'):
                            bnmcode = '7850415000000Y'
                        elif (row['GFCTP'] not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW']
                              and row['GFCNAL'] not in [' ', 'MY']):
                            bnmcode = '7850485000000Y'

    # SELL + CALL
    elif row['DCBSI'] == 'S' and row['OPTYPE'] == 'CALL':
        if row['DCBCCY'] == 'MYR':
            if row['DCTRNT'] == 'I':
                if custcd_str in ['01', '02', '03', '07', '12', '15', '81', '85']:
                    if row['GFCTP'] in ['BC']:
                        bnmcode = '6850401000000Y'
                    elif row['GFCTP'] in ['BB']:
                        bnmcode = '6850402000000Y'
                    elif row['GFCTP'] in ['BI']:
                        bnmcode = '6850403000000Y'
                    elif row['GFCTP'] in ['BJ']:
                        bnmcode = '6850407000000Y'
                    elif row['GFCTP'] in ['BM']:
                        bnmcode = '6850412000000Y'
                    elif row['GFCTP'] in ['BA', 'BE']:
                        bnmcode = '6850481000000Y'
                    elif (row['GFCTP'] not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW']
                          and row['GFCNAL'] == 'MY'):
                        bnmcode = '6850415000000Y'
                    elif (row['GFCTP'] not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW']
                          and row['GFCNAL'] not in [' ', 'MY']):
                        bnmcode = '6850485000000Y'
            elif row['DCTRNT'] == 'C':
                if custcd in [1]:
                    bnmcode = '6850401000000Y'
                elif custcd in [2]:
                    bnmcode = '6850402000000Y'
                elif custcd in [3]:
                    bnmcode = '6850403000000Y'
                elif custcd in [7]:
                    bnmcode = '6850407000000Y'
                elif custcd in [12]:
                    bnmcode = '6850412000000Y'
                elif custcd not in list(range(1, 4)) + [7] + list(range(10, 13)) + list(range(80, 100)):
                    bnmcode = '6850415000000Y'
                elif custcd in list(range(81, 85)):
                    bnmcode = '6850481000000Y'
                elif custcd in list(range(85, 100)):
                    bnmcode = '6850485000000Y'
        else:
            if row['DCACCY'] == 'MYR':
                if row['DCTRNT'] == 'I':
                    if custcd_str in ['01', '02', '03', '07', '12', '15', '81', '85']:
                        if row['GFCTP'] in ['BC']:
                            bnmcode = '7850401000000Y'
                        elif row['GFCTP'] in ['BB']:
                            bnmcode = '7850402000000Y'
                        elif row['GFCTP'] in ['BI']:
                            bnmcode = '7850403000000Y'
                        elif row['GFCTP'] in ['BJ']:
                            bnmcode = '7850407000000Y'
                        elif row['GFCTP'] in ['BM']:
                            bnmcode = '7850412000000Y'
                        elif row['GFCTP'] in ['BA', 'BE']:
                            bnmcode = '7850481000000Y'
                        elif (row['GFCTP'] not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW']
                              and row['GFCNAL'] == 'MY'):
                            bnmcode = '7850415000000Y'
                        elif (row['GFCTP'] not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW']
                              and row['GFCNAL'] not in [' ', 'MY']):
                            bnmcode = '7850485000000Y'
                elif row['DCTRNT'] == 'C':
                    if custcd in [1]:
                        bnmcode = '7850401000000Y'
                    elif custcd in [2]:
                        bnmcode = '7850402000000Y'
                    elif custcd in [3]:
                        bnmcode = '7850403000000Y'
                    elif custcd in [7]:
                        bnmcode = '7850407000000Y'
                    elif custcd in [12]:
                        bnmcode = '7850412000000Y'
                    elif custcd not in list(range(1, 4)) + [7] + list(range(10, 13)) + list(range(80, 100)):
                        bnmcode = '7850415000000Y'
                    elif custcd in list(range(81, 85)):
                        bnmcode = '7850481000000Y'
                    elif custcd in list(range(85, 100)):
                        bnmcode = '7850485000000Y'

    # SELL + PUT
    elif row['DCBSI'] == 'S' and row['OPTYPE'] == 'PUT':
        if row['DCBCCY'] == 'MYR':
            if row['DCTRNT'] == 'I':
                if custcd_str in ['01', '02', '03', '07', '12', '15', '81', '85']:
                    if row['GFCTP'] in ['BC']:
                        bnmcode = '7850401000000Y'
                    elif row['GFCTP'] in ['BB']:
                        bnmcode = '7850402000000Y'
                    elif row['GFCTP'] in ['BI']:
                        bnmcode = '7850403000000Y'
                    elif row['GFCTP'] in ['BJ']:
                        bnmcode = '7850407000000Y'
                    elif row['GFCTP'] in ['BM']:
                        bnmcode = '7850412000000Y'
                    elif row['GFCTP'] in ['BA', 'BE']:
                        bnmcode = '7850481000000Y'
                    elif (row['GFCTP'] not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW']
                          and row['GFCNAL'] == 'MY'):
                        bnmcode = '7850415000000Y'
                    elif (row['GFCTP'] not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW']
                          and row['GFCNAL'] not in [' ', 'MY']):
                        bnmcode = '7850485000000Y'
            elif row['DCTRNT'] == 'C':
                if custcd in [1]:
                    bnmcode = '7850401000000Y'
                elif custcd in [2]:
                    bnmcode = '7850402000000Y'
                elif custcd in [3]:
                    bnmcode = '7850403000000Y'
                elif custcd in [7]:
                    bnmcode = '7850407000000Y'
                elif custcd in [12]:
                    bnmcode = '7850412000000Y'
                elif custcd not in list(range(1, 4)) + [7] + list(range(10, 13)) + list(range(80, 100)):
                    bnmcode = '7850415000000Y'
                elif custcd in list(range(81, 85)):
                    bnmcode = '7850481000000Y'
                elif custcd in list(range(85, 100)):
                    bnmcode = '7850485000000Y'
        else:
            if row['DCACCY'] == 'MYR':
                if row['DCTRNT'] == 'I':
                    if custcd_str in ['01', '02', '03', '07', '12', '15', '81', '85']:
                        if row['GFCTP'] in ['BC']:
                            bnmcode = '6850401000000Y'
                        elif row['GFCTP'] in ['BB']:
                            bnmcode = '6850402000000Y'
                        elif row['GFCTP'] in ['BI']:
                            bnmcode = '6850403000000Y'
                        elif row['GFCTP'] in ['BJ']:
                            bnmcode = '6850407000000Y'
                        elif row['GFCTP'] in ['BM']:
                            bnmcode = '6850412000000Y'
                        elif row['GFCTP'] in ['BA', 'BE']:
                            bnmcode = '6850481000000Y'
                        elif (row['GFCTP'] not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW']
                              and row['GFCNAL'] == 'MY'):
                            bnmcode = '6850415000000Y'
                        elif (row['GFCTP'] not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW']
                              and row['GFCNAL'] not in [' ', 'MY']):
                            bnmcode = '6850485000000Y'
                elif row['DCTRNT'] == 'C':
                    if custcd in [1]:
                        bnmcode = '6850401000000Y'
                    elif custcd in [2]:
                        bnmcode = '6850402000000Y'
                    elif custcd in [3]:
                        bnmcode = '6850403000000Y'
                    elif custcd in [7]:
                        bnmcode = '6850407000000Y'
                    elif custcd in [12]:
                        bnmcode = '6850412000000Y'
                    elif custcd not in list(range(1, 4)) + [7] + list(range(10, 13)) + list(range(80, 100)):
                        bnmcode = '6850415000000Y'
                    elif custcd in list(range(81, 85)):
                        bnmcode = '6850481000000Y'
                    elif custcd in list(range(85, 100)):
                        bnmcode = '6850485000000Y'

    return bnmcode


# =========================================================================
# PROCESS K1TABL - Foreign Exchange Contracts
# =========================================================================

def process_k1tabl():
    """
    Process K1TBL data - Foreign exchange contracts.
    """
    print("Processing K1TABL...")

    # Read K1TBL
    df = pl.read_parquet(K1TBL_FILE).with_columns([
        pl.col('GWBALC').alias('AMOUNT'),
    ])

    # Extract BRANCH
    df = df.with_columns([
        pl.col('GWAB').str.slice(0, 4).cast(pl.Int64).alias('BRANCH'),
        pl.lit(AMTIND).alias('AMTIND'),
    ])

    # Take absolute value if AMOUNT < 0
    df = df.with_columns([
        pl.when(pl.col('AMOUNT') < 0)
        .then(pl.col('AMOUNT').abs())
        .otherwise(pl.col('AMOUNT'))
        .alias('AMOUNT')
    ])

    results = []

    # Process each row
    for row in df.iter_rows(named=True):
        bnmcode = None

        # Special case: XAT/MYR cross with specific conditions
        if (((row['GWCCY'] == 'XAT' and row['GWOCY'] != 'MYR') or
             (row['GWCCY'] != 'MYR' and row['GWOCY'] == 'XAT')) and
                row['GWMVT'] == 'P' and row['GWMVTS'] == 'P' and row['GWCTP'] != 'BW'):

            if row['GWDLP'] in ['SF1', 'SF2', 'FF1', 'FF2', 'TS1', 'TS2']:
                # Output two records
                results.append({
                    'BRANCH': row['BRANCH'],
                    'BNMCODE': '5763900000000Y',
                    'AMOUNT': row['AMOUNT'],
                    'AMTIND': AMTIND
                })
                results.append({
                    'BRANCH': row['BRANCH'],
                    'BNMCODE': '5760000000000Y',
                    'AMOUNT': row['AMOUNT'],
                    'AMTIND': AMTIND
                })
                continue

        # Delete if GWOCY or GWCCY is XAU
        if row['GWOCY'] == 'XAU' or row['GWCCY'] == 'XAU':
            continue

        # Both currencies not MYR, Purchase + Purchase
        if (row['GWCCY'] != 'MYR' and row['GWOCY'] != 'MYR' and
                row['GWMVT'] == 'P' and row['GWMVTS'] == 'P' and row['GWCTP'] != 'BW'):

            if row['GWDLP'] == 'FXS':
                bnmcode = '5761000000000Y'
            elif row['GWDLP'] in ['FXO', 'FXF', 'FBP']:
                bnmcode = '5762000000000Y'
            elif row['GWDLP'] in ['SF2', 'FF1', 'FF2']:
                bnmcode = '5763100000000Y'
            elif row['GWDLP'] in ['SF1', 'TS1', 'TS2']:
                bnmcode = '5763200000000Y'

        # Delete if XAT involved in specific codes
        if bnmcode == '5763100000000Y' and (row['GWCCY'] == 'XAT' or row['GWOCY'] == 'XAT'):
            continue
        if bnmcode == '5763200000000Y' and (row['GWCCY'] == 'XAT' or row['GWOCY'] == 'XAT'):
            continue

        # Output main record
        if bnmcode:
            results.append({
                'BRANCH': row['BRANCH'],
                'BNMCODE': bnmcode,
                'AMOUNT': row['AMOUNT'],
                'AMTIND': AMTIND
            })

            # Additional output for certain codes
            if bnmcode in ['5761000000000Y', '5763100000000Y', '5763200000000Y', '5762000000000Y']:
                results.append({
                    'BRANCH': row['BRANCH'],
                    'BNMCODE': '5760000000000Y',
                    'AMOUNT': row['AMOUNT'],
                    'AMTIND': AMTIND
                })

    result_df = pl.DataFrame(results)

    return result_df


# =========================================================================
# PROCESS K2TABL - Foreign Exchange Deals
# =========================================================================

def process_k2tabl(df):
    """
    Process K2TBL data - Foreign exchange deals.
    """
    print("Processing K2TABL...")

    # Extract BRANCH
    df = df.with_columns([
        pl.col('OMABD').str.slice(0, 4).cast(pl.Int64).alias('BRANCH'),
        pl.lit(AMTIND).alias('AMTIND'),
        pl.lit(None).cast(pl.Utf8).alias('BNMCODE'),
    ])

    results = []

    # Process each row
    for row in df.iter_rows(named=True):
        bnmcode = None
        amount = None

        # Purchase CCY != MYR, Sale CCY = MYR, Purchase + Purchase
        if (row['OXPCCY'] != 'MYR' and row['OXSCCY'] == 'MYR' and
                row['OMMVT'] == 'P' and row['OMMVTS'] == 'P'):

            if row['OSDLP'] in ['FXS']:
                bnmcode = get_k2_bnmcode_6850(row['GFCTP'], row['GFCNAL'], '01')
            elif row['OSDLP'] in ['FXF', 'FXO', 'FBP']:
                bnmcode = get_k2_bnmcode_6850(row['GFCTP'], row['GFCNAL'], '02')
            elif row['OSDLP'] in ['TS1', 'SF1', 'FF1', 'TS2', 'SF2', 'FF2']:
                bnmcode = get_k2_bnmcode_6850(row['GFCTP'], row['GFCNAL'], '03')

            if bnmcode:
                amount = row['ORINWR'] * row['OXEXR']
                results.append({
                    'BRANCH': row['BRANCH'],
                    'BNMCODE': bnmcode,
                    'AMOUNT': amount,
                    'AMTIND': AMTIND
                })

        # Sale CCY != MYR, Purchase CCY = MYR, Purchase + Sale
        if (row['OXSCCY'] != 'MYR' and row['OXPCCY'] == 'MYR' and
                row['OMMVT'] == 'P' and row['OMMVTS'] == 'S'):

            if row['OSDLP'] in ['FXS']:
                bnmcode = get_k2_bnmcode_7850(row['GFCTP'], row['GFCNAL'], '01')
            elif row['OSDLP'] in ['FXF', 'FXO', 'FBP']:
                bnmcode = get_k2_bnmcode_7850(row['GFCTP'], row['GFCNAL'], '02')
            elif row['OSDLP'] in ['TS1', 'SF1', 'FF1', 'TS2', 'SF2', 'FF2']:
                bnmcode = get_k2_bnmcode_7850(row['GFCTP'], row['GFCNAL'], '03')

            if bnmcode:
                amount = row['ORINWP'] * row['OXEXR']
                results.append({
                    'BRANCH': row['BRANCH'],
                    'BNMCODE': bnmcode,
                    'AMOUNT': amount,
                    'AMTIND': AMTIND
                })

    result_df = pl.DataFrame(results)

    return result_df


def get_k2_bnmcode_6850(gfctp, gfcnal, suffix):
    """Get BNMCODE for 6850 series based on customer type and nationality."""
    if gfctp == 'BC':
        return f'6850{suffix}01000000Y'
    elif gfctp == 'BB':
        return f'6850{suffix}02000000Y'
    elif gfctp == 'BI':
        return f'6850{suffix}03000000Y'
    elif gfctp == 'BJ':
        return f'6850{suffix}07000000F'
    elif gfctp == 'BM':
        return f'6850{suffix}12000000Y'
    elif gfctp in ['BA', 'BE']:
        return f'6850{suffix}81000000Y'
    else:
        if gfctp not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW'] and gfcnal == 'MY':
            return f'6850{suffix}15000000Y'
        if gfctp not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW'] and gfcnal not in [' ', 'MY']:
            return f'6850{suffix}85000000Y'
    return None


def get_k2_bnmcode_7850(gfctp, gfcnal, suffix):
    """Get BNMCODE for 7850 series based on customer type and nationality."""
    if gfctp == 'BC':
        return f'7850{suffix}01000000Y'
    elif gfctp == 'BB':
        return f'7850{suffix}02000000Y'
    elif gfctp == 'BI':
        return f'7850{suffix}03000000Y'
    elif gfctp == 'BJ':
        return f'7850{suffix}07000000F'
    elif gfctp == 'BM':
        return f'7850{suffix}12000000Y'
    elif gfctp in ['BA', 'BE']:
        return f'7850{suffix}81000000Y'
    else:
        if gfctp not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW'] and gfcnal == 'MY':
            return f'7850{suffix}15000000Y'
        if gfctp not in ['BA', 'BB', 'BC', 'BE', 'BI', 'BJ', 'BM', 'BW'] and gfcnal not in [' ', 'MY']:
            return f'7850{suffix}85000000Y'
    return None


# =========================================================================
# PROCESS K3TABL - NID Data
# =========================================================================

def process_k3tbl1():
    """
    Process K3TBL1 - NID by remaining maturity.
    """
    print("Processing K3TBL1...")

    # Read K3TBL
    df = pl.read_parquet(K3TBL_FILE)

    # Get REPTDATE from first row
    reptdate_val = df.select('REPTDATE').to_series()[0] if 'REPTDATE' in df.columns else None
    if not reptdate_val:
        reptdate_val = datetime.strptime(TDATE, '%Y%m%d')

    # Calculate AMOUNT
    df = df.with_columns([
        pl.when(pl.col('UTSTY') == 'IDC')
        .then(pl.col('UTAMOC') + pl.col('UTDPF'))
        .otherwise(pl.col('UTAMOC') - pl.col('UTDPF'))
        .alias('AMOUNT'),
        pl.lit(AMTIND).alias('AMTIND'),
    ])

    # Filter for relevant security types and references
    df = df.filter(
        pl.col('UTSTY').is_in(['IFD', 'ILD', 'ISD', 'IZD', 'IDC', 'IDP', 'IZP']) &
        pl.col('UTREF').is_in(['PFD', 'PLD', 'PSD', 'PZD', 'PDC'])
    )

    results = []

    # Process each row
    for row in df.iter_rows(named=True):
        if row['MATDT'] and row['MATDT'] > reptdate_val:
            # Calculate remaining months
            remmth = calculate_remaining_months(reptdate_val, row['MATDT'])
            if remmth is not None:
                remmt = apply_fdrmmt_format(remmth)
                if remmt:
                    bnmcode = f'4215000{remmt}0000Y'
                    results.append({
                        'BNMCODE': bnmcode,
                        'AMOUNT': row['AMOUNT'],
                        'AMTIND': AMTIND
                    })

    result_df = pl.DataFrame(results) if results else pl.DataFrame(
        schema={'BNMCODE': pl.Utf8, 'AMOUNT': pl.Float64, 'AMTIND': pl.Utf8})

    return result_df


def process_k3tbl2():
    """
    Process K3TBL2 - NID by original maturity (weighted average rates).
    """
    print("Processing K3TBL2...")

    # Read K3TBL
    df = pl.read_parquet(K3TBL_FILE)

    # Calculate AMOUNT
    df = df.with_columns([
        pl.when(pl.col('UTSTY') == 'IDC')
        .then(pl.col('UTAMOC') + pl.col('UTDPF'))
        .otherwise(pl.col('UTAMOC') - pl.col('UTDPF'))
        .alias('AMOUNT'),
        pl.lit(AMTIND).alias('AMTIND'),
    ])

    # Set ISSDT to REPTDATE if blank
    reptdate_val = df.select('REPTDATE').to_series()[0] if 'REPTDATE' in df.columns else datetime.strptime(TDATE,
                                                                                                           '%Y%m%d')

    df = df.with_columns([
        pl.when(pl.col('ISSDT').is_null())
        .then(pl.lit(reptdate_val))
        .otherwise(pl.col('ISSDT'))
        .alias('ISSDT')
    ])

    # Filter for relevant security types and references
    df = df.filter(
        pl.col('UTSTY').is_in(['IFD', 'ILD', 'ISD', 'IZD', 'IDC', 'IDP', 'IZP']) &
        pl.col('UTREF').is_in(['PFD', 'PLD', 'PSD', 'PZD', 'PDC'])
    )

    results = []

    # Process each row
    for row in df.iter_rows(named=True):
        if row['MATDT'] and row['ISSDT']:
            # Calculate original maturity months
            remmth = calculate_remaining_months(row['ISSDT'], row['MATDT'])
            if remmth is not None:
                origmt = apply_fdorgmt_format(remmth)
                if origmt:
                    bnmcode = f'4215000{origmt}0000Y'
                    results.append({
                        'BNMCODE': bnmcode,
                        'AMOUNT': row['AMOUNT'],
                        'AMTIND': AMTIND
                    })

    result_df = pl.DataFrame(results) if results else pl.DataFrame(
        schema={'BNMCODE': pl.Utf8, 'AMOUNT': pl.Float64, 'AMTIND': pl.Utf8})

    return result_df


# =========================================================================
# PROCESS NIDTBL - NID Reference Data
# =========================================================================

def process_nidtbl():
    """
    Process NIDTBL - NID reference data from RNID file.
    """
    print("Processing NIDTBL...")

    # Read RNID
    df = pl.read_parquet(RNID_FILE)

    # Filter for active NIDs with positive balance
    df = df.filter(
        (pl.col('NIDSTAT') == 'N') &
        (pl.col('CURBAL') > 0)
    )

    # Set REPTDATE
    reptdate_val = datetime.strptime(TDATE, '%Y%m%d')

    df = df.with_columns([
        pl.col('CURBAL').alias('AMOUNT'),
        pl.lit(AMTIND).alias('AMTIND'),
    ])

    results = []

    # Process by remaining maturity
    for row in df.iter_rows(named=True):
        if row['MATDT'] and row['MATDT'] > reptdate_val:
            remmth = calculate_remaining_months(reptdate_val, row['MATDT'])
            if remmth is not None:
                remmt = apply_fdrmmt_format(remmth)
                if remmt:
                    bnmcode = f'4215000{remmt}0000Y'
                    results.append({
                        'BNMCODE': bnmcode,
                        'AMOUNT': row['AMOUNT'],
                        'AMTIND': AMTIND
                    })

        # Process by original maturity
        if row['MATDT'] and row['STARTDT'] and row['MATDT'] >= row['STARTDT']:
            remmth = calculate_remaining_months(row['STARTDT'], row['MATDT'])
            if remmth is not None:
                origmt = apply_fdorgmt_format(remmth)

                # Apply special rule: if origmt in ('12','13') then origmt='14'
                if origmt in ['12', '13']:
                    origmt = '14'

                if origmt:
                    bnmcode = f'4215000{origmt}0000Y'
                    results.append({
                        'BNMCODE': bnmcode,
                        'AMOUNT': row['AMOUNT'],
                        'AMTIND': AMTIND
                    })

    result_df = pl.DataFrame(results) if results else pl.DataFrame(
        schema={'BNMCODE': pl.Utf8, 'AMOUNT': pl.Float64, 'AMTIND': pl.Utf8})

    return result_df


# =========================================================================
# MAIN PROCESSING
# =========================================================================

def main():
    """Main processing function."""
    print("=" * 70)
    print("KALMPBBP - RDAL PART II (KAPITI ITEMS)")
    print("=" * 70)

    # Dependencies check
    # %INC PGM(PBBDPFMT) - Format definitions (not converted, formats inlined)
    # %INC PGM(KALMPBBF) - KALM format file (not converted, formats inlined)
    # %INC PGM(KALMNIDS) - KALM NID processing (assumed to be K3TABI dataset)

    # Read and process K2TBL
    k2tbl_df = read_k2tbl()

    # Process DCI data
    dcimtb_df = process_dcimtb()
    dcimtb_bnm = process_dci_bnmcodes(dcimtb_df)

    # Process K1TABL
    k1tabl = process_k1tabl()

    # Process K2TABL
    k2tabl = process_k2tabl(k2tbl_df)

    # Process K3TBL
    k3tbl1 = process_k3tbl1()
    k3tbl2 = process_k3tbl2()

    # Process NIDTBL
    nidtbl = process_nidtbl()

    # Note: K3TABI is from %INC PGM(KALMNIDS) - assumed to be an external dataset
    # If K3TABI dataset exists, it should be read here
    # For now, we'll create an empty placeholder
    k3tabi = pl.DataFrame(schema={'BNMCODE': pl.Utf8, 'AMOUNT': pl.Float64, 'AMTIND': pl.Utf8})

    # Combine all K3 datasets
    k3tabl = pl.concat([k3tbl1, k3tabi, k3tbl2])

    # Combine all datasets based on NOWK value
    print(f"\nCombining datasets (NOWK={NOWK})...")

    # Drop BRANCH column from datasets that have it
    datasets_to_combine = []

    if 'BRANCH' in k1tabl.columns:
        datasets_to_combine.append(k1tabl.drop('BRANCH'))
    else:
        datasets_to_combine.append(k1tabl)

    if 'BRANCH' in k2tabl.columns:
        datasets_to_combine.append(k2tabl.drop('BRANCH'))
    else:
        datasets_to_combine.append(k2tabl)

    datasets_to_combine.extend([k3tabl, nidtbl, dcimtb_bnm])

    kalm = pl.concat(datasets_to_combine)

    # Write intermediate KALM file
    kalm.write_parquet(KALM_OUT)
    print(f"KALM intermediate written to {KALM_OUT}")

    # Final consolidation - summarize by BNMCODE and AMTIND
    print("\nPerforming final consolidation...")
    kalm_final = kalm.group_by(['BNMCODE', 'AMTIND']).agg([
        pl.col('AMOUNT').sum().alias('AMOUNT')
    ])

    # Write final KALM file
    kalm_final.write_parquet(KALM_FINAL_OUT)
    print(f"KALM final written to {KALM_FINAL_OUT}")

    # Generate report
    print("\nGenerating report...")
    generate_report(kalm_final)

    print("\n" + "=" * 70)
    print("Processing complete!")
    print("=" * 70)


def generate_report(df):
    """
    Generate ASA carriage control report.
    Page length: 60 lines per page.
    """
    with open(KALM_REPORT, 'w') as f:
        page_num = 1
        line_num = 0
        page_size = 60

        # Write report header
        def write_header():
            nonlocal line_num
            f.write(' ' + SDESC + '\n')
            f.write(' REPORT ON DOMESTIC ASSETS AND LIABILITIES PART II- KAPITI\n')
            f.write(f' REPORT DATE : {RDATE}\n')
            f.write(' \n')
            f.write(' BNMCODE          AMTIND           AMOUNT\n')
            f.write(' ' + '-' * 70 + '\n')
            line_num = 6

        # Write first page header
        write_header()

        # Write data rows
        for row in df.iter_rows(named=True):
            # Check if new page needed
            if line_num >= page_size - 2:
                f.write('1')  # Form feed (ASA carriage control)
                page_num += 1
                line_num = 0
                write_header()

            # Format amount with comma separator
            amount_str = f"{row['AMOUNT']:>30,.2f}"

            # Write data line
            f.write(f" {row['BNMCODE']:<14}   {row['AMTIND']:<1}   {amount_str}\n")
            line_num += 1

    print(f"Report written to {KALM_REPORT}")


# =========================================================================
# EXECUTION
# =========================================================================

if __name__ == '__main__':
    import pandas as pd  # For date handling in calculate_remaining_months

    main()
