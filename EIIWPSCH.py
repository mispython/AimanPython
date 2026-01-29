#!/usr/bin/env python3
"""
File Name: EIIWPSCH
Weekly Loan Schedule Payment Extraction to Warehouse - Islamic Bank Version
Extracts loan payment schedules for weekly periods:
- Week 1: 01st - 08th
- Week 2: 09th - 15th
- Week 3: 16th - 22nd
- Week 4: 23rd - Month End
"""

import duckdb
import struct
import pandas as pd
from datetime import datetime
from pathlib import Path


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# Input paths
INPUT_LN_REPTDATE = "input/EIIWPSCH/ln_reptdate.parquet"
INPUT_LN_LNNOTE = "input/EIIWPSCH/ln_lnnote.parquet"
INPUT_PAYFILE = "input/EIIWPSCH/paysfile.dat"  # Binary file with packed decimal

# Output paths
OUTPUT_ILNPAY_WEEK = "/mnt/user-data/outputs/ilnpay_week{week}.parquet"  # e.g., ilnpay_week01.parquet
OUTPUT_ILNPWFTP = "/mnt/user-data/outputs/ilnpwftp.dat"  # Transport file (binary)
OUTPUT_PRINT_REPORT = "/mnt/user-data/outputs/ilnpay_print_report.txt"  # Print output

# Configuration
MAX_PRINT_OBS = 50  # Maximum observations to print


# ============================================================================
# PACKED DECIMAL UTILITIES
# ============================================================================

def unpack_pd(data, length, decimals=0):
    """
    Unpack IBM packed decimal (PD) format to Python number

    Args:
        data: bytes object containing packed decimal
        length: number of bytes in packed decimal field
        decimals: number of decimal places

    Returns:
        Numeric value or None if invalid
    """
    if not data or len(data) < length:
        return None

    pd_bytes = data[:length]

    # Convert packed decimal to string
    result = ''
    for byte in pd_bytes:
        high = (byte >> 4) & 0x0F
        low = byte & 0x0F

        if high <= 9:
            result += str(high)
        if low <= 9:
            result += str(low)

    # Handle sign (last nibble)
    last_nibble = pd_bytes[-1] & 0x0F
    is_negative = last_nibble in (0x0B, 0x0D)  # B or D indicates negative

    # Remove sign nibble from result
    if len(result) > 0:
        result = result[:-1]

    if not result or result == '':
        return 0

    try:
        value = int(result)
        if decimals > 0:
            value = value / (10 ** decimals)
        if is_negative:
            value = -value
        return value
    except (ValueError, TypeError):
        return None


def read_packed_decimal_file(filepath):
    """
    Read binary file with packed decimal fields matching SAS INPUT statement

    Record layout (positions 1-based as in SAS):
    @001 ACCTNO      PD6.      - Account number (6 bytes)
    @007 NOTENO      PD3.      - Note number (3 bytes)
    @011 EFFDAT      PD6.      - Effective date (6 bytes)
    @018 PAYTYPE      $1.      - Payment type (1 byte)
    @019 NOPAY       PD3.      - Number of payments (3 bytes)
    @022 PAYAMT      PD8.2     - Payment amount (8 bytes, 2 decimals)
    @054 SCHFREQ      $1.      - Schedule frequency (1 byte)
    @061 PAYMAINTAIN PD6.      - Payment maintain date (6 bytes)
    """
    records = []

    try:
        with open(filepath, 'rb') as f:
            while True:
                # Read enough bytes for the full record
                record = f.read(100)  # Read 100 bytes per record

                if not record or len(record) < 66:
                    break

                # Extract fields (converting from 1-based to 0-based indexing)
                acctno = unpack_pd(record[0:6], 6, 0)  # @001, PD6
                noteno = unpack_pd(record[6:9], 3, 0)  # @007, PD3
                effdat = unpack_pd(record[10:16], 6, 0)  # @011, PD6 (YYYYMMDD as packed)
                paytype = chr(record[17]) if record[17] < 128 else ' '  # @018, $1
                nopay = unpack_pd(record[18:21], 3, 0)  # @019, PD3
                payamt = unpack_pd(record[21:29], 8, 2)  # @022, PD8.2
                schfreq = chr(record[53]) if len(record) > 53 and record[53] < 128 else ' '  # @054, $1
                paymaintain = unpack_pd(record[60:66], 6, 0) if len(record) >= 66 else 0  # @061, PD6

                records.append({
                    'ACCTNO': acctno,
                    'NOTENO': noteno,
                    'EFFDAT': effdat,
                    'PAYTYPE': paytype,
                    'NOPAY': nopay,
                    'PAYAMT': payamt,
                    'SCHFREQ': schfreq,
                    'PAYMAINTAIN': paymaintain
                })
    except FileNotFoundError:
        print(f"Warning: Payment file not found: {filepath}")
        return []

    return records


def process_effdate(effdat_packed):
    """
    Process effective date from packed decimal YYYYMMDD format
    Returns tuple: (day, month, year, adjusted_day, effdate)
    """
    if not effdat_packed or effdat_packed == 0:
        return None, None, None, None, None

    # Convert packed decimal to YYYYMMDD string (11 chars with leading zeros)
    effdat_str = f"{int(effdat_packed):011d}"

    # Extract components
    yy = int(effdat_str[0:4])  # First 4 digits
    mm = int(effdat_str[7:9])  # Positions 8-9 (0-based: 7-9)
    dd = int(effdat_str[9:11])  # Positions 10-11 (0-based: 9-11)

    payday = dd

    # Adjust day for special values (31, 99, 30, 29)
    if dd in (31, 99):
        if mm in (1, 3, 5, 7, 8, 10, 12):
            dd = 31
        elif mm == 2:
            if yy % 4 == 0:
                dd = 29
            else:
                dd = 28
        else:
            dd = 30
    elif dd in (30, 29):
        if mm == 2:
            if yy % 4 == 0:
                dd = 29
            else:
                dd = 28

    # Create date
    try:
        effdate = datetime(yy, mm, dd)
        return payday, mm, yy, dd, effdate
    except (ValueError, TypeError):
        return payday, mm, yy, dd, None


def process_paymaintain(paymaintain_packed):
    """
    Process payment maintain date from packed decimal
    Returns datetime or None
    """
    if not paymaintain_packed or paymaintain_packed <= 0:
        return None

    # Convert to MMDDYYYY format (first 8 digits of 11-char representation)
    paymaint_str = f"{int(paymaintain_packed):011d}"
    mmddyyyy_str = paymaint_str[0:8]

    try:
        mm = int(mmddyyyy_str[0:2])
        dd = int(mmddyyyy_str[2:4])
        yyyy = int(mmddyyyy_str[4:8])
        return datetime(yyyy, mm, dd)
    except (ValueError, TypeError):
        return None


# ============================================================================
# INITIALIZE DUCKDB CONNECTION
# ============================================================================
con = duckdb.connect(database=':memory:')


# ============================================================================
# STEP 1: PROCESS REPORT DATE AND DETERMINE WEEK
# ============================================================================
print("Step 1: Processing report date and determining week...")

# Read REPTDATE from parquet
reptdate_df = con.execute(f"""
    SELECT * FROM read_parquet('{INPUT_LN_REPTDATE}')
""").fetchdf()

reptdate = reptdate_df['REPTDATE'].iloc[0]
reptday = reptdate.day

# Determine week based on day of month
if reptday == 8:
    NOWK = '01'
elif reptday == 15:
    NOWK = '02'
elif reptday == 22:
    NOWK = '03'
else:
    NOWK = '04'

# Create macro variables
REPTMON = f"{reptdate.month:02d}"
REPTYEAR = reptdate.strftime('%y')

print(f"Report Date: {reptdate.strftime('%Y-%m-%d')}")
print(f"Report Day: {reptday}")
print(f"Week Number: {NOWK}")
print(f"Report Month: {REPTMON}")
print(f"Report Year: {REPTYEAR}")


# ============================================================================
# STEP 2: READ AND PROCESS PAYMENT FILE
# ============================================================================
print("\nStep 2: Reading and processing payment file...")

# Read packed decimal payment file
pay_records = read_packed_decimal_file(INPUT_PAYFILE)
print(f"Payment records read: {len(pay_records):,}")

if len(pay_records) == 0:
    print("Warning: No payment records found. Creating empty dataset.")

# Process each record
processed_records = []
for rec in pay_records:
    payday, mm, yy, dd, effdate = process_effdate(rec['EFFDAT'])
    paymaintain = process_paymaintain(rec['PAYMAINTAIN'])

    processed_records.append({
        'ACCTNO': rec['ACCTNO'],
        'NOTENO': rec['NOTENO'],
        'PAYTYPE': rec['PAYTYPE'],
        'NOPAY': rec['NOPAY'],
        'PAYAMT': rec['PAYAMT'],
        'SCHFREQ': rec['SCHFREQ'],
        'PAYDAY': payday,
        'EFFDATE': effdate,
        'PAYMAINTAIN': paymaintain
    })

# Create DataFrame
if processed_records:
    # con.execute("CREATE TABLE lnpay AS SELECT * FROM processed_records")
    # print(f"Processed payment records: {len(processed_records):,}")

    df_pay = pd.DataFrame(processed_records)

    # Register DataFrame with DuckDB
    con.register("lnpay", df_pay)

    print(f"Payment records processed: {len(df_pay):,}")
else:
    # Create empty table with correct schema
    con.execute("""
        CREATE TABLE lnpay (
            ACCTNO BIGINT,
            NOTENO INTEGER,
            PAYTYPE VARCHAR,
            NOPAY INTEGER,
            PAYAMT DOUBLE,
            SCHFREQ VARCHAR,
            PAYDAY INTEGER,
            EFFDATE DATE,
            PAYMAINTAIN DATE
        )
    """)
    print("Created empty lnpay table")


# ============================================================================
# STEP 3: LOAD LOAN NOTE DATA WITH FILTER
# ============================================================================
print("\nStep 3: Loading loan note data...")

# Load and filter for non-paid loans (PAIDIND NE 'P')
lnnote = con.execute(f"""
    SELECT 
        ACCTNO,
        NOTENO
    FROM read_parquet('{INPUT_LN_LNNOTE}')
    WHERE PAIDIND <> 'P'
""").df()

con.register('lnnote', lnnote)
print(f"Loan note records loaded (PAIDIND <> 'P'): {len(lnnote):,}")


# ============================================================================
# STEP 4: MERGE LOAN NOTES WITH PAYMENT DATA
# ============================================================================
print("\nStep 4: Merging loan notes with payment data...")

# Merge (keep only records in lnnote - IF A)
ilnpay_week = con.execute("""
    SELECT 
        n.ACCTNO,
        n.NOTENO,
        p.PAYTYPE,
        p.NOPAY,
        p.PAYAMT,
        p.SCHFREQ,
        p.PAYDAY,
        p.EFFDATE,
        p.PAYMAINTAIN
    FROM lnnote n
    LEFT JOIN lnpay p 
        ON n.ACCTNO = p.ACCTNO 
        AND n.NOTENO = p.NOTENO
""").df()

print(f"Final payment schedule records: {len(ilnpay_week):,}")


# ============================================================================
# STEP 5: SAVE WEEKLY PAYMENT DATA TO PARQUET
# ============================================================================
print("\nStep 5: Saving weekly payment data...")

output_parquet = OUTPUT_ILNPAY_WEEK.format(week=NOWK)
Path(output_parquet).parent.mkdir(parents=True, exist_ok=True)

con.register('ilnpay_week', ilnpay_week)
con.execute(f"COPY ilnpay_week TO '{output_parquet}' (FORMAT PARQUET)")
print(f"Weekly payment data saved: {output_parquet}")


# ============================================================================
# STEP 6: GENERATE PRINT REPORT (PROC PRINT)
# ============================================================================
print("\nStep 6: Generating print report...")

Path(OUTPUT_PRINT_REPORT).parent.mkdir(parents=True, exist_ok=True)

with open(OUTPUT_PRINT_REPORT, 'w') as f:
    # Write header with ASA carriage control
    f.write('1')  # New page
    f.write(f"{'Islamic Bank - Weekly Loan Payment Schedule':^132}\n")
    f.write(' ')
    f.write(f"{'Week ' + NOWK + ' Extract':^132}\n")
    f.write(' ')
    f.write(f"{'Report Date: ' + reptdate.strftime('%Y-%m-%d'):^132}\n")
    f.write(' ')
    f.write('\n')

    # Column headers
    f.write(' ')
    header = f"{'OBS':<6}{'ACCTNO':<15}{'NOTENO':<10}{'PAYTYPE':<10}{'NOPAY':<10}"
    header += f"{'PAYAMT':<15}{'SCHFREQ':<10}{'PAYDAY':<10}{'EFFDATE':<12}{'PAYMAINTAIN':<12}\n"
    f.write(header)

    f.write(' ')
    f.write('-' * 132 + '\n')

    # Data rows (first 50 observations)
    obs_count = 0
    for idx, row in ilnpay_week.iterrows():
        if obs_count >= MAX_PRINT_OBS:
            break

        obs_count += 1
        line = ' '  # ASA carriage control

        # OBS number
        line += f"{obs_count:<6}"

        # ACCTNO
        acctno = int(row['ACCTNO']) if row['ACCTNO'] is not None else 0
        line += f"{acctno:<15}"

        # NOTENO
        noteno = int(row['NOTENO']) if row['NOTENO'] is not None else 0
        line += f"{noteno:<10}"

        # PAYTYPE
        paytype = str(row['PAYTYPE']) if row['PAYTYPE'] is not None else ''
        line += f"{paytype:<10}"

        # NOPAY
        nopay = int(row['NOPAY']) if row['NOPAY'] is not None else 0
        line += f"{nopay:<10}"

        # PAYAMT
        payamt = float(row['PAYAMT']) if row['PAYAMT'] is not None else 0.0
        line += f"{payamt:<15.2f}"

        # SCHFREQ
        schfreq = str(row['SCHFREQ']) if row['SCHFREQ'] is not None else ''
        line += f"{schfreq:<10}"

        # PAYDAY
        payday = int(row['PAYDAY']) if row['PAYDAY'] is not None else 0
        line += f"{payday:<10}"

        # EFFDATE (DATE8. format: DDMMMYYY, e.g., 15JAN2026)
        if row['EFFDATE'] is not None:
            effdate_str = row['EFFDATE'].strftime('%d%b%Y').upper()
        else:
            effdate_str = ''
        line += f"{effdate_str:<12}"

        # PAYMAINTAIN (DATE8. format)
        if row['PAYMAINTAIN'] is not None:
            paymaint_str = row['PAYMAINTAIN'].strftime('%d%b%Y').upper()
        else:
            paymaint_str = ''
        line += f"{paymaint_str:<12}"

        line += '\n'
        f.write(line)

    # Footer
    f.write(' ')
    f.write('\n')
    f.write(' ')
    f.write(f"NOTE: Only first {MAX_PRINT_OBS} observations displayed\n")
    f.write(' ')
    f.write(f"Total observations: {len(ilnpay_week):,}\n")

print(f"Print report saved: {OUTPUT_PRINT_REPORT}")


# ============================================================================
# STEP 7: CREATE TRANSPORT FILE (PROC CPORT)
# ============================================================================
print("\nStep 7: Creating transport file...")

# Note: PROC CPORT creates a SAS transport file format
# For Python, we'll create a binary file containing the parquet data
# This simulates the transport file functionality

Path(OUTPUT_ILNPWFTP).parent.mkdir(parents=True, exist_ok=True)

# Read the parquet file we just created
with open(output_parquet, 'rb') as src:
    parquet_data = src.read()

# Write to transport file (in this case, just copy the parquet data)
with open(OUTPUT_ILNPWFTP, 'wb') as dst:
    # Write a simple header to identify this as our transport format
    header = b'ILNPAY_TRANSPORT_V1'
    dst.write(struct.pack(f'{len(header)}s', header))
    dst.write(struct.pack('I', len(parquet_data)))  # Write data length
    dst.write(parquet_data)

print(f"Transport file created: {OUTPUT_ILNPWFTP}")
print(f"Transport file size: {len(parquet_data):,} bytes")


# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 70)
print("ISLAMIC BANK - WEEKLY LOAN PAYMENT EXTRACTION COMPLETE")
print("=" * 70)
print(f"\nWeek Extracted: Week {NOWK}")
print(f"Report Period: {reptdate.strftime('%Y-%m-%d')}")
print(f"Total Records: {len(ilnpay_week):,}")

print(f"\nGenerated Files:")
print(f"  1. Weekly Payment Data: {output_parquet}")
print(f"  2. Print Report: {OUTPUT_PRINT_REPORT}")
print(f"  3. Transport File: {OUTPUT_ILNPWFTP}")

# Statistics
if len(ilnpay_week) > 0:
    stats = con.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT ACCTNO) as unique_accounts,
            SUM(PAYAMT) as total_payment_amount,
            AVG(PAYAMT) as avg_payment_amount,
            MIN(EFFDATE) as earliest_date,
            MAX(EFFDATE) as latest_date
        FROM ilnpay_week
        WHERE PAYAMT IS NOT NULL
    """).fetchone()

    print(f"\nPayment Statistics:")
    print(f"  Total Records: {stats[0]:,}")
    print(f"  Unique Accounts: {stats[1]:,}")
    if stats[2]:
        print(f"  Total Payment Amount: ${stats[2]:,.2f}")
        print(f"  Average Payment: ${stats[3]:,.2f}")
    if stats[4]:
        print(f"  Earliest Payment Date: {stats[4]}")
    if stats[5]:
        print(f"  Latest Payment Date: {stats[5]}")

print("\nConversion complete!")

# Close DuckDB connection
con.close()
