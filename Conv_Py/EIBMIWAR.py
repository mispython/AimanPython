# !/usr/bin/env python3
"""
Program: EIBMIWAR
Date: 26.07.01
Purpose: Weighted Average Rate (W.A.R) for all Islamic products
Report: WEIGHTED AVERAGE RATE FOR ALL ISLAMIC PRODUCTS
"""

import polars as pl
import duckdb
from datetime import datetime, timedelta
from pathlib import Path
import struct

# ============================================================================
# CONFIGURATION - Define all paths early
# ============================================================================

# Input paths
INPUT_PATH_REPTDATE = "SAP_PBB_MNILN_0_REPTDATE.parquet"
INPUT_PATH_CURRENT = "DEPOSIT_CURRENT.parquet"
INPUT_PATH_SAVING = "DEPOSIT_SAVING.parquet"
INPUT_PATH_FD = "FD_FD.parquet"
INPUT_PATH_LNNOTE = "SAP_PBB_MNILN_0_LNNOTE.parquet"
INPUT_PATH_RATE = "RATE.txt"
INPUT_PATH_BNMTBL3 = "BNMTBL3.dat"  # Binary file with packed decimal

# Output path
OUTPUT_PATH_REPORT = "EIBMIWAR_REPORT.txt"

# Page configuration
PAGE_LENGTH = 60
LRECL = 133  # 132 + 1 for ASA carriage control

# Product codes
CURRENT_PRODUCTS = [161, 163, 165]
SAVING_PRODUCTS = [204]
FD_INTPLANS = [340, 341, 342, 343, 344, 345, 346, 347, 348, 349, 350, 351, 352,
               353, 354, 355, 356, 357, 358, 359, 588, 589, 590, 591, 592, 593,
               594, 595, 596, 597, 598, 599, 580, 581, 582, 583, 584, 585, 586, 587]
LOAN_TYPES = [100, 101, 102, 103, 110, 111, 112, 113, 114, 115, 116,
              117, 118, 120, 121, 122, 123, 124, 125, 127, 135, 180]


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def format_asa_line(line_content, carriage_control=' '):
    """Format a line with ASA carriage control character."""
    return f"{carriage_control}{line_content}"


def get_report_date_info():
    """
    Process REPTDATE to generate macro variables.
    Mirrors the DATA _NULL_ step in original SAS.
    """
    reptdate_df = pl.read_parquet(INPUT_PATH_REPTDATE)
    reptdate_row = reptdate_df.row(0, named=True)
    reptdate = reptdate_row['REPTDATE']

    day = reptdate.day

    # SELECT(DAY(REPTDATE))
    if day == 8:
        sdd = 1
        wk = '1'
        wk1 = '4'
    elif day == 15:
        sdd = 9
        wk = '2'
        wk1 = '1'
    elif day == 22:
        sdd = 16
        wk = '3'
        wk1 = '2'
    else:
        sdd = 23
        wk = '4'
        wk1 = '3'

    mm = reptdate.month

    # Calculate previous month for WK = '1'
    if wk == '1':
        mm1 = mm - 1
        if mm1 == 0:
            mm1 = 12
    else:
        mm1 = mm

    # SDATE = MDY(MM,SDD,YEAR(REPTDATE))
    sdate = datetime(reptdate.year, mm, sdd)

    # Format dates
    rdate = f"{reptdate.day:02d}/{reptdate.month:02d}/{str(reptdate.year)[2:]}"
    sdate_str = f"{sdate.day:02d}/{sdate.month:02d}/{str(sdate.year)[2:]}"

    return {
        'NOWK': wk,
        'NOWK1': wk1,
        'REPTMON': f"{mm:02d}",
        'REPTMON1': f"{mm1:02d}",
        'REPTYEAR': f"{reptdate.year:04d}",
        'RDATE': rdate,
        'REPTDAY': f"{reptdate.day:02d}",
        'SDATE': sdate_str,
        'REPTDATE': reptdate,
        'SDATE_DT': sdate
    }


def read_packed_decimal(data, start_pos, length, decimals):
    """
    Read packed decimal (PD) format from binary data.

    Args:
        data: bytes object
        start_pos: starting position (0-based)
        length: number of bytes (e.g., PD8.2 = 8 bytes)
        decimals: number of decimal places

    Returns:
        float value
    """
    try:
        packed_bytes = data[start_pos:start_pos + length]

        # Convert packed decimal to integer
        result = 0
        for i, byte in enumerate(packed_bytes[:-1]):
            high = (byte >> 4) & 0x0F
            low = byte & 0x0F
            result = result * 100 + high * 10 + low

        # Last byte contains sign
        last_byte = packed_bytes[-1]
        high = (last_byte >> 4) & 0x0F
        sign_nibble = last_byte & 0x0F
        result = result * 10 + high

        # Apply sign (0x0C = positive, 0x0D = negative)
        if sign_nibble == 0x0D:
            result = -result

        # Apply decimal places
        result = result / (10 ** decimals)

        return result
    except:
        return 0.0


def read_bnmtbl3_file():
    """
    Read K3TBL data from BNMTBL3 binary file with packed decimal fields.
    """
    if not Path(INPUT_PATH_BNMTBL3).exists():
        return pl.DataFrame()

    records = []

    with open(INPUT_PATH_BNMTBL3, 'rb') as f:
        # Read first record to get REPTDATE
        first_line = f.read(230)  # Approximate record length
        if len(first_line) >= 30:
            # INPUT @24 REPTDATE YYMMDD6.
            reptdate_str = first_line[23:29].decode('ascii', errors='ignore')
            try:
                reptdate = datetime.strptime(reptdate_str, '%y%m%d')
            except:
                reptdate = None

        # Read remaining records
        f.seek(0)
        while True:
            line = f.read(230)
            if len(line) < 230:
                break

            if line[0:1] == b' ':  # Skip header line
                continue

            try:
                # Parse fixed-width text fields
                utsty = line[0:3].decode('ascii', errors='ignore').strip()
                utref = line[3:19].decode('ascii', errors='ignore').strip()
                utdlp = line[19:22].decode('ascii', errors='ignore').strip()
                utdlr = line[22:35].decode('ascii', errors='ignore').strip()
                utsmn = line[35:51].decode('ascii', errors='ignore').strip()
                utcus = line[51:57].decode('ascii', errors='ignore').strip()
                utclc = line[57:60].decode('ascii', errors='ignore').strip()
                utctp = line[60:62].decode('ascii', errors='ignore').strip()

                # Parse packed decimal fields
                # @63  UTFCV  PD8.2
                utfcv = read_packed_decimal(line, 62, 8, 2)

                # Parse date fields
                utidt = line[70:80].decode('ascii', errors='ignore').strip()
                utlcd = line[80:90].decode('ascii', errors='ignore').strip()
                utncd = line[90:100].decode('ascii', errors='ignore').strip()
                utmdt = line[100:110].decode('ascii', errors='ignore').strip()
                utcbd = line[110:120].decode('ascii', errors='ignore').strip()

                # @121 UTCPR  PD7.7
                utcpr = read_packed_decimal(line, 120, 7, 7)
                # @128 UTQDS  PD7.7
                utqds = read_packed_decimal(line, 127, 7, 7)
                # @135 UTPCP  PD7.7
                utpcp = read_packed_decimal(line, 134, 7, 7)
                # @142 CURBAL PD8.2
                curbal = read_packed_decimal(line, 141, 8, 2)
                # @150 UTDPF  PD8.2
                utdpf = read_packed_decimal(line, 149, 8, 2)
                # @158 UTAICT PD8.2
                utaict = read_packed_decimal(line, 157, 8, 2)
                # @166 UTAICY PD8.2
                utaicy = read_packed_decimal(line, 165, 8, 2)
                # @174 UTAIT  PD8.2
                utait = read_packed_decimal(line, 173, 8, 2)
                # @182 UTDPET PD8.2
                utdpet = read_packed_decimal(line, 181, 8, 2)
                # @190 UTDPEY PD8.2
                utdpey = read_packed_decimal(line, 189, 8, 2)
                # @198 UTDPE  PD8.2
                utdpe = read_packed_decimal(line, 197, 8, 2)
                # @206 UTASN  PD4.
                utasn = read_packed_decimal(line, 205, 4, 0)

                utosd = line[209:219].decode('ascii', errors='ignore').strip()
                utca2 = line[219:221].decode('ascii', errors='ignore').strip()
                utsac = line[221:223].decode('ascii', errors='ignore').strip()
                utcnap = line[223:225].decode('ascii', errors='ignore').strip()
                utcnar = line[225:227].decode('ascii', errors='ignore').strip()
                utcnal = line[227:229].decode('ascii', errors='ignore').strip()

                # Parse dates
                matdt = None
                issdt = None
                if utmdt and utmdt != ' ' * 10:
                    try:
                        matdt = datetime.strptime(utmdt, '%Y/%m/%d')
                    except:
                        pass

                if utosd and utosd != ' ' * 10:
                    try:
                        issdt = datetime.strptime(utosd, '%Y/%m/%d')
                    except:
                        pass

                # Calculate RATE
                rate = utqds
                if utqds == 0 or utqds > 10:
                    rate = utcpr

                # Filter conditions
                if utsty in ['IDS', 'CMB', 'KHA', 'MGI', 'SBA', 'BNN'] and utref == 'IINV':
                    records.append({
                        'UTSTY': utsty,
                        'UTREF': utref,
                        'UTDLP': utdlp,
                        'UTDLR': utdlr,
                        'UTSMN': utsmn,
                        'UTCUS': utcus,
                        'UTCLC': utclc,
                        'UTCTP': utctp,
                        'UTFCV': utfcv,
                        'UTIDT': utidt,
                        'UTLCD': utlcd,
                        'UTNCD': utncd,
                        'UTMDT': utmdt,
                        'UTCBD': utcbd,
                        'UTCPR': utcpr,
                        'UTQDS': utqds,
                        'UTPCP': utpcp,
                        'CURBAL': curbal,
                        'UTDPF': utdpf,
                        'UTAICT': utaict,
                        'UTAICY': utaicy,
                        'UTAIT': utait,
                        'UTDPET': utdpet,
                        'UTDPEY': utdpey,
                        'UTDPE': utdpe,
                        'UTASN': utasn,
                        'UTOSD': utosd,
                        'UTCA2': utca2,
                        'UTSAC': utsac,
                        'UTCNAP': utcnap,
                        'UTCNAR': utcnar,
                        'UTCNAL': utcnal,
                        'MATDT': matdt,
                        'ISSDT': issdt,
                        'REPTDATE': reptdate,
                        'RATE': rate
                    })
            except Exception as e:
                continue

    if records:
        return pl.DataFrame(records)
    else:
        return pl.DataFrame()


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def process_eibmiwar():
    """Main processing function for EIBMIWAR."""
    print("=" * 80)
    print("EIBMIWAR - Weighted Average Rate for Islamic Products")
    print("=" * 80)

    # Get report date information
    print("Initializing report parameters...")
    macro_vars = get_report_date_info()
    rdate = macro_vars['RDATE']
    print(f"Report Date: {rdate}")
    print()

    con = duckdb.connect()

    try:
        # ====================================================================
        # Load and filter CURRENT accounts
        # ====================================================================
        print("Processing CURRENT accounts...")
        current_df = con.execute(f"""
            SELECT * FROM read_parquet('{INPUT_PATH_CURRENT}')
            WHERE OPENIND NOT IN ('B', 'C', 'P')
            AND PRODUCT IN ({','.join(map(str, CURRENT_PRODUCTS))})
            AND MTDLOWBA >= 25000
        """).pl()

        # Remove duplicates and drop SECTOR
        current_df = current_df.unique(subset=['PRODUCT', 'ACCTNO'], keep='first')
        if 'SECTOR' in current_df.columns:
            current_df = current_df.drop('SECTOR')

        # ====================================================================
        # Load and filter SAVING accounts
        # ====================================================================
        print("Processing SAVING accounts...")
        saving_df = con.execute(f"""
            SELECT * FROM read_parquet('{INPUT_PATH_SAVING}')
            WHERE OPENIND NOT IN ('B', 'C', 'P')
            AND PRODUCT IN ({','.join(map(str, SAVING_PRODUCTS))})
        """).pl()

        saving_df = saving_df.unique(subset=['PRODUCT', 'ACCTNO'], keep='first')
        if 'SECTOR' in saving_df.columns:
            saving_df = saving_df.drop('SECTOR')

        # ====================================================================
        # Load and filter FD accounts
        # ====================================================================
        print("Processing FD accounts...")
        fd_df = con.execute(f"""
            SELECT * FROM read_parquet('{INPUT_PATH_FD}')
            WHERE OPENIND NOT IN ('B', 'C', 'P')
            AND INTPLAN IN ({','.join(map(str, FD_INTPLANS))})
        """).pl()

        fd_df = fd_df.sort(['INTPLAN', 'ACCTNO'])

        # ====================================================================
        # Load and filter LOAN accounts
        # ====================================================================
        print("Processing LOAN accounts...")
        loan_df = con.execute(f"""
            SELECT * FROM read_parquet('{INPUT_PATH_LNNOTE}')
            WHERE (REVERSED <> 'Y' OR REVERSED IS NULL)
            AND NOTENO IS NOT NULL
            AND (PAIDIND <> 'P' OR PAIDIND IS NULL)
            AND LOANTYPE IN ({','.join(map(str, LOAN_TYPES))})
        """).pl()

        loan_df = loan_df.unique(subset=['LOANTYPE', 'ACCTNO'], keep='first')
        if 'SECTOR' in loan_df.columns:
            loan_df = loan_df.drop('SECTOR')

        # ====================================================================
        # Load RATES file
        # ====================================================================
        print("Processing RATES file...")
        rates_records = []

        if Path(INPUT_PATH_RATE).exists():
            with open(INPUT_PATH_RATE, 'r') as f:
                for line in f:
                    if len(line) < 29:
                        continue

                    trancd = line[0:3].strip()
                    prodcd = line[3:6].strip()
                    tier = line[6:15].strip()
                    try:
                        rate = float(line[15:20].strip())
                    except:
                        rate = 0.0
                    yyyy = line[20:24].strip()
                    mmmm = line[24:26].strip()
                    dddd = line[26:28].strip()

                    # Filter conditions
                    if (trancd == '215' and prodcd == '204') or \
                            (trancd == '210' and prodcd in ['110', '111', '340', '341', '342',
                                                            '343', '344', '345', '346', '347', '348', '349', '350',
                                                            '351']):
                        rates_records.append({
                            'TRANCD': trancd,
                            'PRODCD': prodcd,
                            'TIER': tier,
                            'RATE': rate,
                            'YYYY': yyyy,
                            'MMMM': mmmm,
                            'DDDD': dddd
                        })

        rates_df = pl.DataFrame(rates_records) if rates_records else pl.DataFrame()

        # ====================================================================
        # Load K3TBL (BNMTBL3) file
        # ====================================================================
        print("Processing K3TBL (BNMTBL3) file...")
        k3tbl_df = read_bnmtbl3_file()

        # ====================================================================
        # Process CURRENT accounts
        # ====================================================================
        print("Categorizing CURRENT accounts...")

        # Add TRANCD and PRODCD
        ccurrent_df = current_df.with_columns([
            pl.lit('210').alias('TRANCD'),
            pl.when(pl.col('PRODUCT') == 161).then(pl.lit('110'))
            .when(pl.col('PRODUCT') == 163).then(pl.lit('111'))
            .when(pl.col('PRODUCT') == 165).then(pl.lit('110'))
            .otherwise(pl.lit('000'))
            .alias('PRODCD')
        ])

        # Merge with RATES
        if len(rates_df) > 0:
            mcurrent_df = ccurrent_df.join(
                rates_df,
                on=['TRANCD', 'PRODCD'],
                how='left'
            )
        else:
            mcurrent_df = ccurrent_df.with_columns([
                pl.lit(0.0).alias('RATE')
            ])

        # Create ICURRENT with TYPES, SUBTYPE, COSTS
        icurrent_df = mcurrent_df.with_columns([
            pl.lit('WADIAH CURRENT ACCOUNT').alias('TYPES'),
            pl.when(pl.col('PRODUCT') == 161).then(pl.lit('AL-WADIAH GOVERNMENT'))
            .when(pl.col('PRODUCT') == 163).then(pl.lit('AL-WADIAH HOUSING'))
            .when(pl.col('PRODUCT') == 165).then(pl.lit('WADIAH IOCP GOVERNMENT'))
            .otherwise(pl.lit('CURRENT NOT IDENTIFIED'))
            .alias('SUBTYPE'),
            ((pl.col('CURBAL') * pl.col('RATE')) / 100).alias('COSTS')
        ])

        # ====================================================================
        # Process SAVING accounts
        # ====================================================================
        print("Categorizing SAVING accounts...")

        csaving_df = saving_df.with_columns([
            pl.when(pl.col('PRODUCT') == 204)
            .then(pl.lit('215'))
            .otherwise(pl.lit(''))
            .alias('TRANCD'),
            pl.when(pl.col('PRODUCT') == 204)
            .then(pl.lit('204'))
            .otherwise(pl.lit(''))
            .alias('PRODCD')
        ])

        # Merge with RATES
        if len(rates_df) > 0:
            msaving_df = csaving_df.join(
                rates_df,
                on=['TRANCD', 'PRODCD'],
                how='left'
            )
        else:
            msaving_df = csaving_df.with_columns([
                pl.lit(0.0).alias('RATE')
            ])

        isaving_df = msaving_df.with_columns([
            pl.lit('WADIAH SAVINGS ACCOUNT').alias('TYPES'),
            pl.when(pl.col('PRODUCT') == 204).then(pl.lit('SAVING NORMAL'))
            .otherwise(pl.lit('SAVING NOT INDENTIFY'))
            .alias('SUBTYPE'),
            ((pl.col('CURBAL') * pl.col('RATE')) / 100).alias('COSTS')
        ])

        # ====================================================================
        # Process FD accounts
        # ====================================================================
        print("Categorizing FD accounts...")

        fd_subtype_map = {
            340: '01 MONTH', 341: '03 MONTH', 342: '06 MONTH', 343: '09 MONTH',
            344: '12 MONTH', 345: '15 MONTH', 346: '18 MONTH', 347: '21 MONTH',
            348: '24 MONTH', 349: '36 MONTH', 350: '48 MONTH', 351: '60 MONTH',
            352: '02 MONTH', 353: '04 MONTH', 354: '05 MONTH', 355: '07 MONTH',
            356: '08 MONTH', 357: '10 MONTH', 358: '11 MONTH', 588: '13 MONTH',
            589: '14 MONTH', 590: '16 MONTH', 591: '17 MONTH', 592: '19 MONTH',
            593: '20 MONTH', 594: '22 MONTH', 595: '23 MONTH', 596: '25 MONTH',
            597: '26 MONTH', 359: '27 MONTH', 598: '28 MONTH', 599: '29 MONTH',
            580: '30 MONTH', 581: '33 MONTH', 582: '39 MONTH', 583: '42 MONTH',
            584: '45 MONTH', 585: '51 MONTH', 586: '54 MONTH', 587: '57 MONTH'
        }

        ifd_df = fd_df.with_columns([
            pl.lit('GENERAL INVESTMENT ACCOUNT').alias('TYPES'),
            pl.col('INTPLAN').map_dict(fd_subtype_map, default='FD NOT INDENTIFY').alias('SUBTYPE'),
            ((pl.col('CURBAL') * pl.col('RATE')) / 100).alias('COSTS')
        ])

        # ====================================================================
        # Process LOAN accounts
        # ====================================================================
        print("Categorizing LOAN accounts...")

        # Calculate RATE for ABBA loans
        iloan_records = []
        for row in loan_df.iter_rows(named=True):
            loantype = row['LOANTYPE']
            noteterm = row.get('NOTETERM', 0) or 0
            ntapr = row.get('NTAPR', 0) or 0
            intrate = row.get('INTRATE', 0) or 0
            curbal = row.get('CURBAL', 0) or 0

            # Calculate RATE
            if loantype in [100, 101, 102, 103, 110, 111, 112, 113, 114, 115, 116, 117, 118,
                            120, 121, 122, 123, 124, 125, 127]:
                year = noteterm / 12
                yearr = year - 2
                rater = ntapr * yearr
                rate = (9 + rater) / year if year > 0 else 0
            else:
                rate = intrate

            # Determine SUBTYPE
            if loantype in [100, 101, 102, 103, 110, 111, 112, 113, 114, 115, 116, 117, 118]:
                subtype = 'ABBA HOUSE FINANCING'
            elif loantype in [120, 121, 122, 123, 124, 125]:
                subtype = 'ABBA TERM FINANCING'
            elif loantype == 127:
                subtype = 'ABBA SWIFT FINANCING'
            elif loantype == 135:
                subtype = 'BAI AL-EINAH'
                # Special rate mapping for BAI AL-EINAH
                rate_map = {12: 12.68, 18: 12.87, 24: 12.91, 30: 12.88, 36: 12.83,
                            42: 12.76, 48: 12.68, 54: 12.59, 60: 12.51}
                rate = rate_map.get(noteterm, rate)
            elif loantype == 180:
                subtype = 'PUTRA'
            else:
                subtype = 'LOAN NOT INDENTIFY'

            iloan_records.append({
                **row,
                'TYPES': 'FINANCING',
                'SUBTYPE': subtype,
                'RATE': rate,
                'COSTS': (curbal * rate) / 100
            })

        iloan_df = pl.DataFrame(iloan_records) if iloan_records else pl.DataFrame()

        # ====================================================================
        # Process K3TBL (Investment Stock)
        # ====================================================================
        print("Categorizing K3TBL accounts...")

        if len(k3tbl_df) > 0:
            ik3tbl_df = k3tbl_df.with_columns([
                pl.lit('INVESTMENT STOCK').alias('TYPES'),
                pl.when((pl.col('UTSTY') == 'BNN') & (pl.col('UTREF') == 'IINV'))
                .then(pl.lit('BNM BILLS/NOTES'))
                .when((pl.col('UTSTY') == 'IDS') & (pl.col('UTREF') == 'IINV'))
                .then(pl.lit('ISLAMIC DEBT SECURITIES'))
                .when((pl.col('UTSTY') == 'CMB') & (pl.col('UTREF') == 'IINV'))
                .then(pl.lit('CAGAMAS BOND'))
                .when((pl.col('UTSTY') == 'KHA') & (pl.col('UTREF') == 'IINV'))
                .then(pl.lit('KHAZANAH BOND'))
                .when((pl.col('UTSTY') == 'MGI') & (pl.col('UTREF') == 'IINV'))
                .then(pl.lit('MUDHARABAH GENERAL INVESTMENT CERTIFICATE'))
                .when((pl.col('UTSTY') == 'SBA') & (pl.col('UTREF') == 'IINV'))
                .then(pl.lit('SECONDARY BANKERS ACCEPTANCE'))
                .when(pl.col('UTREF') == 'IINV')
                .then(pl.lit('KAPITI NO IDENTIFY'))
                .otherwise(pl.lit(''))
                .alias('SUBTYPE'),
                ((pl.col('CURBAL') * pl.col('RATE')) / 100).alias('COSTS')
            ])
        else:
            ik3tbl_df = pl.DataFrame()

        # ====================================================================
        # Combine all datasets
        # ====================================================================
        print("Combining all datasets...")

        # Select common columns
        common_cols = ['TYPES', 'SUBTYPE', 'CURBAL', 'RATE', 'COSTS']

        all_dfs = []
        for df in [icurrent_df, isaving_df, ifd_df, iloan_df, ik3tbl_df]:
            if len(df) > 0:
                all_dfs.append(df.select(common_cols))

        if all_dfs:
            all_df = pl.concat(all_dfs)
        else:
            all_df = pl.DataFrame(schema={col: pl.Float64 if col in ['CURBAL', 'RATE', 'COSTS'] else pl.Utf8
                                          for col in common_cols})

        # Sort
        all_df = all_df.sort(['TYPES', 'SUBTYPE'])

        # ====================================================================
        # Summarize for calculation of weighted average cost
        # ====================================================================
        print("Calculating weighted average rates...")

        summary_df = all_df.groupby(['TYPES', 'SUBTYPE']).agg([
            pl.count('CURBAL').alias('NOACCT'),
            pl.sum('CURBAL').alias('CURBAL'),
            pl.sum('RATE').alias('RATE'),
            pl.sum('COSTS').alias('COSTS')
        ])

        # Calculate RATESS (average rate)
        summary_df = summary_df.with_columns([
            (pl.col('RATE') / pl.col('NOACCT')).alias('RATESS')
        ])

        # Sort
        summary_df = summary_df.sort(['TYPES', 'SUBTYPE'])

        # ====================================================================
        # Generate report
        # ====================================================================
        print("Generating report...")

        with open(OUTPUT_PATH_REPORT, 'w') as f:
            generate_report(f, summary_df, rdate)

        print(f"Generated: {OUTPUT_PATH_REPORT}")
        print("EIBMIWAR processing complete")

    finally:
        con.close()


def generate_report(f, df, rdate):
    """
    Generate tabulated report.
    Mimics PROC TABULATE output.
    """

    # Write titles
    f.write(format_asa_line('PUBLIC ISLAMIC BANK BERHAD'.center(LRECL - 1), '1') + '\n')
    f.write(format_asa_line(f'WEIGHTED AVERAGE RATE (W.A.R) {rdate}'.center(LRECL - 1), ' ') + '\n')
    f.write(format_asa_line('ISLAMIC BANKING DIVISION'.center(LRECL - 1), ' ') + '\n')
    f.write(format_asa_line('REPORT NAME : EIBMIWAR'.center(LRECL - 1), ' ') + '\n')
    f.write(format_asa_line(' ' * (LRECL - 1), ' ') + '\n')

    # Write column headers
    header1 = (f"{'TYPE OF PRODUCTS':<62}|"
               f"{'TOTAL AVERAGE':>18}|{'NO OF':>9}|"
               f"{'EFFECTIVE':>10}|{'TOTAL (A) X (B)':>18}|")
    f.write(format_asa_line(header1.ljust(LRECL - 1), ' ') + '\n')

    header2 = (f"{' ':<62}|"
               f"{'DAILY BALANCE(A)':>18}|{'ACCOUNTS':>9}|"
               f"{'RATE(%)(B)':>10}|{' ':>18}|")
    f.write(format_asa_line(header2.ljust(LRECL - 1), ' ') + '\n')

    separator = '-' * 62 + '+' + '-' * 18 + '+' + '-' * 9 + '+' + '-' * 10 + '+' + '-' * 18 + '+'
    f.write(format_asa_line(separator.ljust(LRECL - 1), ' ') + '\n')

    # Calculate totals
    total_curbal = df['CURBAL'].sum()
    total_noacct = df['NOACCT'].sum()
    total_ratess = df['RATESS'].sum()
    total_costs = df['COSTS'].sum()

    # Group by TYPES
    current_type = None

    for row in df.iter_rows(named=True):
        types = row['TYPES']
        subtype = row['SUBTYPE']
        curbal = row['CURBAL']
        noacct = row['NOACCT']
        ratess = row['RATESS']
        costs = row['COSTS']

        # Write type header if new type
        if types != current_type:
            if current_type is not None:
                f.write(format_asa_line(' ' * (LRECL - 1), ' ') + '\n')

            type_line = f"{types:<62}|{' ':>18}|{' ':>9}|{' ':>10}|{' ':>18}|"
            f.write(format_asa_line(type_line.ljust(LRECL - 1), ' ') + '\n')
            current_type = types

        # Write subtype detail line
        detail_line = (f"  {subtype:<60}|"
                       f"{curbal:>18,.2f}|"
                       f"{noacct:>9,}|"
                       f"{ratess:>10.5f}|"
                       f"{costs:>18,.2f}|")
        f.write(format_asa_line(detail_line.ljust(LRECL - 1), ' ') + '\n')

    # Write separator before total
    f.write(format_asa_line(separator.ljust(LRECL - 1), ' ') + '\n')

    # Write total line
    total_line = (f"{'TOTAL':<62}|"
                  f"{total_curbal:>18,.2f}|"
                  f"{total_noacct:>9,}|"
                  f"{total_ratess:>10.5f}|"
                  f"{total_costs:>18,.2f}|")
    f.write(format_asa_line(total_line.ljust(LRECL - 1), ' ') + '\n')

    # Write final separator
    f.write(format_asa_line(separator.ljust(LRECL - 1), ' ') + '\n')


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    process_eibmiwar()
