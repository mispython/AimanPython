#!/usr/bin/env python3
"""
SAS to Python Conversion: EIMNOSTE
Report on Foreign Exchange Transaction
Processes Walker and Deposit nostro files and generates summary report
"""

import duckdb
import struct
from datetime import datetime
from pathlib import Path

import pandas as pd

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# Input paths
INPUT_MNITB_REPTDATE = "input/mnitb_reptdate.parquet"
INPUT_WKNOST = "input/wknost.dat"  # Walker file (fixed-width text)
INPUT_DPNOST = "input/dpnost.dat"  # Deposit file (fixed-width with packed decimal)

# Output paths
OUTPUT_REPTDATE = "/mnt/user-data/outputs/nostro_reptdate.parquet"
OUTPUT_WAK_DATASET = "/mnt/user-data/outputs/wak_{year}{month}.parquet"
OUTPUT_DP_DATASET = "/mnt/user-data/outputs/dp_{year}{month}.parquet"
OUTPUT_REPORT = "/mnt/user-data/outputs/eimnoste_report.txt"
OUTPUT_SFTP_SCRIPT = "/mnt/user-data/outputs/sftp_commands.txt"

# Report configuration
PAGE_LENGTH = 60


# ============================================================================
# PACKED DECIMAL UTILITIES
# ============================================================================

def unpack_pd(data, length, decimals=0):
    """Unpack IBM packed decimal format"""
    if not data or len(data) < length:
        return 0.0

    pd_bytes = data[:length]
    result = ''

    for byte in pd_bytes:
        high = (byte >> 4) & 0x0F
        low = byte & 0x0F

        if high <= 9:
            result += str(high)
        if low <= 9:
            result += str(low)

    # Handle sign
    last_nibble = pd_bytes[-1] & 0x0F
    is_negative = last_nibble in (0x0B, 0x0D)

    # Remove sign nibble
    if len(result) > 0:
        result = result[:-1]

    if not result:
        return 0.0

    try:
        value = float(result)
        if decimals > 0:
            value = value / (10 ** decimals)
        if is_negative:
            value = -value
        return value
    except (ValueError, TypeError):
        return 0.0


# ============================================================================
# INITIALIZE DUCKDB CONNECTION
# ============================================================================
con = duckdb.connect(database=':memory:')

# ============================================================================
# STEP 1: PROCESS REPORT DATE AND DETERMINE WEEK
# ============================================================================
print("Step 1: Processing report date and determining week...")

reptdate_df = con.execute(f"""
    SELECT * FROM read_parquet('{INPUT_MNITB_REPTDATE}')
""").fetchdf()

reptdate = reptdate_df['REPTDATE'].iloc[0]
reptday = reptdate.day

# Determine week based on day of month
if 5 <= reptday <= 10:
    NOWK = '1'
elif 11 <= reptday <= 18:
    NOWK = '2'
elif 19 <= reptday <= 25:
    NOWK = '3'
else:
    NOWK = '4'

# Create macro variables
REPTYEAR = reptdate.strftime('%y')
REPTMON = f"{reptdate.month:02d}"
REPTDAY = f"{reptdate.day:02d}"
RDATE = reptdate.strftime('%d/%m/%y')
FILDT = reptdate.strftime('%d%m%y')

print(f"Report Date: {RDATE}")
print(f"Week: {NOWK}, Year: {REPTYEAR}, Month: {REPTMON}, Day: {REPTDAY}")
print(f"File Date: {FILDT}")

# Save REPTDATE dataset
Path(OUTPUT_REPTDATE).parent.mkdir(parents=True, exist_ok=True)
reptdate_out = reptdate_df[['REPTDATE']]
con.register('reptdate_out', reptdate_out)
con.execute(f"COPY reptdate_out TO '{OUTPUT_REPTDATE}' (FORMAT PARQUET)")

# ============================================================================
# STEP 2: READ WALKER FILE
# ============================================================================
print("\nStep 2: Reading Walker file...")

walker_records = []

try:
    with open(INPUT_WKNOST, 'r') as f:
        for line in f:
            if len(line.strip()) == 0:
                continue

            # Parse fixed-width fields
            sbc = int(line[0:8].strip()) if line[0:8].strip() else 0
            dd = int(line[8:10].strip()) if line[8:10].strip() else 0
            mm = int(line[11:13].strip()) if line[11:13].strip() else 0
            yy = int(line[14:16].strip()) if line[14:16].strip() else 0
            dd1 = int(line[16:18].strip()) if line[16:18].strip() else 0
            mm1 = int(line[19:21].strip()) if line[19:21].strip() else 0
            yy1 = int(line[22:24].strip()) if line[22:24].strip() else 0
            nbr = line[24:48].strip()
            desc = line[48:108].strip()
            forcur = int(line[108:121].strip()) if line[108:121].strip() else 0
            sign = line[121:122]
            rmcur = int(line[122:133].strip()) if line[122:133].strip() else 0
            agentno = line[133:136].strip()
            curcode = line[136:139].strip()
            trancode = line[139:142].strip()
            name = line[142:172].strip() if len(line) >= 172 else ''

            # Apply transformations
            forcur = forcur / 100
            rmcur = rmcur / 100

            # Create dates
            try:
                trxdt = datetime(2000 + yy, mm, dd) if yy < 50 else datetime(1900 + yy, mm, dd)
            except (ValueError, TypeError):
                trxdt = None

            try:
                preffdt = datetime(2000 + yy1, mm1, dd1) if yy1 < 50 else datetime(1900 + yy1, mm1, dd1)
            except (ValueError, TypeError):
                preffdt = None

            # Apply sign
            if sign == 'D':
                forcur = forcur * -1
                rmcur = rmcur * -1

            walker_records.append({
                'SBC': sbc,
                'NBR': nbr,
                'DESC': desc,
                'FORCUR': forcur,
                'SIGN': sign,
                'RMCUR': rmcur,
                'AGENTNO': agentno,
                'CURCODE': curcode,
                'TRANCODE': trancode,
                'NAME': name,
                'TRXDT': trxdt,
                'PREFFDT': preffdt
            })

except FileNotFoundError:
    print(f"Warning: Walker file not found: {INPUT_WKNOST}")

print(f"Walker records loaded: {len(walker_records):,}")

# Create Walker dataset with transaction descriptions
if walker_records:
    # con.execute("CREATE TABLE walker AS SELECT * FROM walker_records")

    df_walker = pd.DataFrame(walker_records)

    # Register DataFrame with DuckDB
    con.register("walker",df_walker)
else:
    con.execute("""
        CREATE TABLE walker (
            SBC BIGINT,
            NBR VARCHAR,
            DESC VARCHAR,
            FORCUR DOUBLE,
            SIGN VARCHAR,
            RMCUR DOUBLE,
            AGENTNO VARCHAR,
            CURCODE VARCHAR,
            TRANCODE VARCHAR,
            NAME VARCHAR,
            TRXDT DATE,
            PREFFDT DATE,
            TRDESC VARCHAR
        )
    """)

# Add transaction descriptions
walker_trdesc_map = {
    '001': 'OUTWARD DD',
    '003': 'OUTWARD MT',
    '006': 'OUTWARD TT',
    '011': 'BANK GUARANTEE',
    '013': 'ECRF PRE SHIPMENT',
    '015': 'ECRF POST SHIPMENT',
    '021': 'FBEP CLEAN',
    '023': 'FBEP DOCUMENTARY',
    '025': 'FBEP AP',
    '031': 'FOBC CLEAN',
    '033': 'OBC DOCUMENTARY',
    '041': 'IBC CLEAN',
    '043': 'IBC DOCUMENTARY',
    '051': 'BR',
    '055': 'DPC',
    '061': 'LC',
    '063': 'ILC',
    '081': 'INWARD DD',
    '083': 'INWARD MT',
    '085': 'INWARD TT',
    '091': 'MISCELLANEOUS',
    '092': 'TREASURY CREDIT',
    '093': 'TREASURY DEBIT'
}

walker_df = con.execute("""
    SELECT 
        SBC, NBR, DESC, FORCUR, SIGN, RMCUR, AGENTNO, CURCODE, 
        TRANCODE, NAME, TRXDT, PREFFDT,
        CASE 
            WHEN TRANCODE = '001' THEN 'OUTWARD DD'
            WHEN TRANCODE = '003' THEN 'OUTWARD MT'
            WHEN TRANCODE = '006' THEN 'OUTWARD TT'
            WHEN TRANCODE = '011' THEN 'BANK GUARANTEE'
            WHEN TRANCODE = '013' THEN 'ECRF PRE SHIPMENT'
            WHEN TRANCODE = '015' THEN 'ECRF POST SHIPMENT'
            WHEN TRANCODE = '021' THEN 'FBEP CLEAN'
            WHEN TRANCODE = '023' THEN 'FBEP DOCUMENTARY'
            WHEN TRANCODE = '025' THEN 'FBEP AP'
            WHEN TRANCODE = '031' THEN 'FOBC CLEAN'
            WHEN TRANCODE = '033' THEN 'OBC DOCUMENTARY'
            WHEN TRANCODE = '041' THEN 'IBC CLEAN'
            WHEN TRANCODE = '043' THEN 'IBC DOCUMENTARY'
            WHEN TRANCODE = '051' THEN 'BR'
            WHEN TRANCODE = '055' THEN 'DPC'
            WHEN TRANCODE = '061' THEN 'LC'
            WHEN TRANCODE = '063' THEN 'ILC'
            WHEN TRANCODE = '081' THEN 'INWARD DD'
            WHEN TRANCODE = '083' THEN 'INWARD MT'
            WHEN TRANCODE = '085' THEN 'INWARD TT'
            WHEN TRANCODE = '091' THEN 'MISCELLANEOUS'
            WHEN TRANCODE = '092' THEN 'TREASURY CREDIT'
            WHEN TRANCODE = '093' THEN 'TREASURY DEBIT'
            ELSE 'OTHERS'
        END as TRDESC
    FROM walker
""").df()

con.register('walker_with_desc', walker_df)

# Save Walker dataset
output_wak = OUTPUT_WAK_DATASET.format(year=REPTYEAR, month=REPTMON)
Path(output_wak).parent.mkdir(parents=True, exist_ok=True)
con.execute(f"COPY walker_with_desc TO '{output_wak}' (FORMAT PARQUET)")
print(f"Walker dataset saved: {output_wak}")

# ============================================================================
# STEP 3: READ DEPOSIT FILE (WITH PACKED DECIMAL)
# ============================================================================
print("\nStep 3: Reading Deposit file...")

deposit_records = []

try:
    with open(INPUT_DPNOST, 'rb') as f:
        while True:
            line = f.read(200)  # LRECL=200

            if not line or len(line) < 85:
                break

            # Parse fields
            acctno = int(line[0:10].decode('ascii', errors='ignore').strip()) if line[0:10].decode('ascii',
                                                                                                   errors='ignore').strip() else None
            agentno = line[10:13].decode('ascii', errors='ignore').strip()
            name = line[13:38].decode('ascii', errors='ignore').strip()
            trind = line[38:39].decode('ascii', errors='ignore').strip()
            trtype = line[39:41].decode('ascii', errors='ignore').strip()
            sign = line[41:42].decode('ascii', errors='ignore').strip()

            # Packed decimal fields
            forcur = unpack_pd(line[42:49], 7, 2)
            rmcur = unpack_pd(line[49:56], 7, 2)

            # Date components
            yy = int(line[56:60].decode('ascii', errors='ignore').strip()) if line[56:60].decode('ascii',
                                                                                                 errors='ignore').strip() else 0
            mm = int(line[60:62].decode('ascii', errors='ignore').strip()) if line[60:62].decode('ascii',
                                                                                                 errors='ignore').strip() else 0
            dd = int(line[62:64].decode('ascii', errors='ignore').strip()) if line[62:64].decode('ascii',
                                                                                                 errors='ignore').strip() else 0
            yy1 = int(line[64:68].decode('ascii', errors='ignore').strip()) if line[64:68].decode('ascii',
                                                                                                  errors='ignore').strip() else 0
            mm1 = int(line[68:70].decode('ascii', errors='ignore').strip()) if line[68:70].decode('ascii',
                                                                                                  errors='ignore').strip() else 0
            dd1 = int(line[70:72].decode('ascii', errors='ignore').strip()) if line[70:72].decode('ascii',
                                                                                                  errors='ignore').strip() else 0

            curcode = line[72:75].decode('ascii', errors='ignore').strip()
            billind = line[75:76].decode('ascii', errors='ignore').strip()

            yy2 = int(line[76:80].decode('ascii', errors='ignore').strip()) if len(line) > 80 and line[76:80].decode(
                'ascii', errors='ignore').strip() else 0
            mm2 = int(line[80:82].decode('ascii', errors='ignore').strip()) if len(line) > 82 and line[80:82].decode(
                'ascii', errors='ignore').strip() else 0
            dd2 = int(line[82:84].decode('ascii', errors='ignore').strip()) if len(line) > 84 and line[82:84].decode(
                'ascii', errors='ignore').strip() else 0

            # Create dates
            try:
                startdt = datetime(yy, mm, dd) if yy > 0 and mm > 0 and dd > 0 else None
            except (ValueError, TypeError):
                startdt = None

            try:
                enddt = datetime(yy1, mm1, dd1) if yy1 > 0 and mm1 > 0 and dd1 > 0 else None
            except (ValueError, TypeError):
                enddt = None

            try:
                trxdt = datetime(yy2, mm2, dd2) if yy2 > 0 and mm2 > 0 and dd2 > 0 else None
            except (ValueError, TypeError):
                trxdt = None

            # Apply sign
            if sign == 'D':
                forcur = forcur * -1
                rmcur = rmcur * -1

            # Set default agent number
            if not agentno:
                agentno = '000'

            # Only keep records with valid account number
            if acctno is not None:
                deposit_records.append({
                    'ACCTNO': acctno,
                    'AGENTNO': agentno,
                    'NAME': name,
                    'TRIND': trind,
                    'TRTYPE': trtype,
                    'SIGN': sign,
                    'FORCUR': forcur,
                    'RMCUR': rmcur,
                    'CURCODE': curcode,
                    'BILLIND': billind,
                    'STARTDT': startdt,
                    'ENDDT': enddt,
                    'TRXDT': trxdt
                })

except FileNotFoundError:
    print(f"Warning: Deposit file not found: {INPUT_DPNOST}")

print(f"Deposit records loaded: {len(deposit_records):,}")

# Create Deposit dataset
if deposit_records:
    # con.execute("CREATE TABLE deposit AS SELECT * FROM deposit_records")

    df_depo = pd.DataFrame(deposit_records)

    # Register DataFrame with DuckDB
    con.register("deposit", df_depo)

else:
    con.execute("""
        CREATE TABLE deposit (
            ACCTNO BIGINT,
            AGENTNO VARCHAR,
            NAME VARCHAR,
            TRIND VARCHAR,
            TRTYPE VARCHAR,
            SIGN VARCHAR,
            FORCUR DOUBLE,
            RMCUR DOUBLE,
            CURCODE VARCHAR,
            BILLIND VARCHAR,
            STARTDT DATE,
            ENDDT DATE,
            TRXDT DATE
        )
    """)

# Filter and add transaction descriptions
deposit_df = con.execute("""
    WITH filtered AS (
        SELECT *
        FROM deposit
        WHERE NOT (ACCTNO >= 3997200109 AND ACCTNO <= 3997204029)
    ),
    with_trdesc AS (
        SELECT *,
            CASE
                -- Bill indicator based descriptions
                WHEN BILLIND IN ('L', 'X', 'I', 'O', 'G') THEN
                    CASE BILLIND
                        WHEN 'L' THEN 'IMPORT LC'
                        WHEN 'X' THEN 'EXPORT LC'
                        WHEN 'I' THEN 'INWARD BILLS COLL'
                        WHEN 'O' THEN 'OUTWARD BILL COLL'
                        WHEN 'G' THEN 'BANK GUARANTEE'
                    END
                -- Transaction indicator based descriptions
                WHEN TRIND = 'O' THEN
                    CASE
                        WHEN TRTYPE = 'TT' THEN 'OUTWARD TT'
                        WHEN TRTYPE = 'DD' THEN 'OUTWARD DD'
                        WHEN TRTYPE IN ('MT', 'TF') THEN 'OUTWARD MT'
                        ELSE 'OTHERS'
                    END
                WHEN TRIND = 'R' THEN
                    CASE
                        WHEN TRTYPE = 'TT' THEN 'REPLACE TT'
                        WHEN TRTYPE = 'DD' THEN 'REPLACE DD'
                        WHEN TRTYPE = 'MT' THEN 'REPLACE MT'
                        ELSE 'OTHERS'
                    END
                WHEN TRIND = 'I' THEN
                    CASE
                        WHEN TRTYPE = 'TT' THEN 'INWARD TT'
                        WHEN TRTYPE = 'DD' THEN 'INWARD DD'
                        ELSE 'OTHERS'
                    END
                WHEN TRIND = 'B' THEN
                    CASE
                        WHEN TRTYPE = 'TT' THEN 'B BACK TT'
                        WHEN TRTYPE = 'DD' THEN 'B BACK DD'
                        ELSE 'OTHERS'
                    END
                WHEN TRIND = 'C' THEN
                    CASE
                        WHEN TRTYPE = 'TT' THEN 'CANCEL TT'
                        WHEN TRTYPE = 'DD' THEN 'CANCEL DD'
                        ELSE 'OTHERS'
                    END
                ELSE 'OTHERS'
            END as TRDESC
        FROM filtered
    )
    SELECT * FROM with_trdesc
""").df()

con.register('deposit_with_desc', deposit_df)

# Save Deposit dataset
output_dp = OUTPUT_DP_DATASET.format(year=REPTYEAR, month=REPTMON)
con.execute(f"COPY deposit_with_desc TO '{output_dp}' (FORMAT PARQUET)")
print(f"Deposit dataset saved: {output_dp}")

# ============================================================================
# STEP 4: MERGE NAMES FROM WALKER AND DEPOSIT
# ============================================================================
print("\nStep 4: Merging names...")

# Get unique names
allname = con.execute("""
    WITH wkname AS (
        SELECT DISTINCT AGENTNO, NAME
        FROM walker_with_desc
    ),
    dpname AS (
        SELECT DISTINCT AGENTNO, NAME
        FROM deposit_with_desc
    ),
    combined AS (
        SELECT AGENTNO, NAME FROM wkname
        UNION ALL
        SELECT AGENTNO, NAME FROM dpname
    ),
    ranked AS (
        SELECT AGENTNO, NAME,
               ROW_NUMBER() OVER (PARTITION BY AGENTNO ORDER BY NAME) as rn
        FROM combined
    )
    SELECT AGENTNO, NAME
    FROM ranked
    WHERE rn = 1
""").df()

con.register('allname', allname)
print(f"Unique agent names: {len(allname):,}")

# ============================================================================
# STEP 5: COMBINE WALKER AND DEPOSIT DATA
# ============================================================================
print("\nStep 5: Combining Walker and Deposit data...")

allrec = con.execute("""
    WITH combined AS (
        SELECT AGENTNO, CURCODE, TRDESC, SIGN, FORCUR, RMCUR, TRXDT
        FROM walker_with_desc

        UNION ALL

        SELECT AGENTNO, CURCODE, TRDESC, SIGN, FORCUR, RMCUR, TRXDT
        FROM deposit_with_desc
    )
    SELECT c.*, n.NAME
    FROM combined c
    LEFT JOIN allname n ON c.AGENTNO = n.AGENTNO
""").df()

con.register('allrec', allrec)
print(f"Combined records: {len(allrec):,}")

# ============================================================================
# STEP 6: SUMMARIZE DATA
# ============================================================================
print("\nStep 6: Summarizing data...")

summary = con.execute("""
    SELECT 
        CURCODE,
        AGENTNO,
        NAME,
        TRDESC,
        SIGN,
        COUNT(*) as NOTRAN,
        SUM(FORCUR) as FORCUR,
        SUM(RMCUR) as RMCUR
    FROM allrec
    GROUP BY CURCODE, AGENTNO, NAME, TRDESC, SIGN
    ORDER BY CURCODE, AGENTNO, NAME, TRDESC, SIGN
""").df()

print(f"Summary records: {len(summary):,}")

# ============================================================================
# STEP 7: GENERATE REPORT
# ============================================================================
print("\nStep 7: Generating report...")

Path(OUTPUT_REPORT).parent.mkdir(parents=True, exist_ok=True)

with open(OUTPUT_REPORT, 'w') as f:
    # Report header
    f.write('1')  # New page
    f.write(f"{'REPORT ID : EIMNOSTE':^132}\n")
    f.write(' ')
    f.write(f"{'REPORT ON FOREIGN EXCHANGE TRANSACTION AS AT ' + RDATE:^132}\n")
    f.write(' ')
    f.write('\n')

    # Column headers
    f.write(' ')
    f.write(f"{'CURRENCY':<8} {'AGENT':<5} {'NAME':<30} {'TRANSACTION':<20} ")
    f.write(f"{'DEBIT(D)/':<9} {'NO OF':<8} {'FOREIGN':<17} {'RM':<17}\n")

    f.write(' ')
    f.write(f"{'CODE':<8} {'NO':<5} {'':<30} {'DESCRIPTION':<20} ")
    f.write(f"{'CREDIT(C)':<9} {'TRANS':<8} {'AMOUNT':<17} {'AMOUNT':<17}\n")

    f.write(' ')
    f.write(f"{'':<8} {'':<5} {'':<30} {'':<20} {'':<9} {'ACTION':<8} {'':<17} {'':<17}\n")

    f.write(' ')
    f.write('-' * 132 + '\n')

    # Data rows
    current_curcode = None
    current_agentno = None
    agent_forcur = 0.0
    agent_rmcur = 0.0
    total_forcur = 0.0
    total_rmcur = 0.0

    for idx, row in summary.iterrows():
        # Check for agent break
        if current_agentno is not None and row['AGENTNO'] != current_agentno:
            # Print agent subtotal
            f.write(' ')
            f.write(' ' * 94 + '-' * 36 + '\n')
            f.write(' ')
            f.write(' ' * 94 + f"{agent_forcur:>17,.2f} {agent_rmcur:>17,.2f}\n")
            f.write(' ')
            f.write(' ' * 94 + '-' * 36 + '\n')
            f.write(' ')
            f.write('\n')
            agent_forcur = 0.0
            agent_rmcur = 0.0

        # Print detail line
        line = ' '

        # CURCODE (only on first row of currency group)
        if row['CURCODE'] != current_curcode:
            line += f"{row['CURCODE']:<8} "
            current_curcode = row['CURCODE']
        else:
            line += ' ' * 9

        # AGENTNO (only on first row of agent group)
        if row['AGENTNO'] != current_agentno:
            line += f"{row['AGENTNO']:<5} "
            current_agentno = row['AGENTNO']
        else:
            line += ' ' * 6

        # NAME
        name = row['NAME'] if row['NAME'] else ''
        line += f"{name[:30]:<30} "

        # TRDESC
        trdesc = row['TRDESC'] if row['TRDESC'] else ''
        line += f"{trdesc[:20]:<20} "

        # SIGN
        sign = row['SIGN'] if row['SIGN'] else ''
        line += f"{sign:<9} "

        # NOTRAN
        notran = int(row['NOTRAN']) if row['NOTRAN'] else 0
        line += f"{notran:>8,} "

        # FORCUR
        forcur = float(row['FORCUR']) if row['FORCUR'] else 0.0
        line += f"{forcur:>17,.2f} "

        # RMCUR
        rmcur = float(row['RMCUR']) if row['RMCUR'] else 0.0
        line += f"{rmcur:>17,.2f}"

        line += '\n'
        f.write(line)

        # Accumulate totals
        agent_forcur += forcur
        agent_rmcur += rmcur
        total_forcur += forcur
        total_rmcur += rmcur

    # Print final agent subtotal
    if current_agentno is not None:
        f.write(' ')
        f.write(' ' * 94 + '-' * 36 + '\n')
        f.write(' ')
        f.write(' ' * 94 + f"{agent_forcur:>17,.2f} {agent_rmcur:>17,.2f}\n")
        f.write(' ')
        f.write(' ' * 94 + '-' * 36 + '\n')
        f.write(' ')
        f.write('\n')

    # Print grand total
    f.write(' ')
    f.write('\n')
    f.write(' ')
    f.write(' ' * 86 + f"{'TOTAL':>7} {total_forcur:>17,.2f} {total_rmcur:>17,.2f}\n")
    f.write(' ')
    f.write(' ' * 94 + '=' * 36 + '\n')

print(f"Report saved: {OUTPUT_REPORT}")

# ============================================================================
# STEP 8: GENERATE SFTP SCRIPT
# ============================================================================
print("\nStep 8: Generating SFTP script...")

Path(OUTPUT_SFTP_SCRIPT).parent.mkdir(parents=True, exist_ok=True)

with open(OUTPUT_SFTP_SCRIPT, 'w') as f:
    f.write(f"PUT //SAP.PBB.EIMNOSTE.TEXT(+1) EIMNOSTE_{FILDT}.TXT\n")

print(f"SFTP script saved: {OUTPUT_SFTP_SCRIPT}")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 70)
print("FOREIGN EXCHANGE TRANSACTION REPORT COMPLETE")
print("=" * 70)
print(f"\nReport Date: {RDATE}")
print(f"Week: {NOWK}")
print(f"Total Transactions: {len(allrec):,}")
print(f"Summary Records: {len(summary):,}")

print(f"\nGenerated Files:")
print(f"  1. Report Date: {OUTPUT_REPTDATE}")
print(f"  2. Walker Dataset: {output_wak}")
print(f"  3. Deposit Dataset: {output_dp}")
print(f"  4. Report: {OUTPUT_REPORT}")
print(f"  5. SFTP Script: {OUTPUT_SFTP_SCRIPT}")

# Transaction totals
if len(summary) > 0:
    totals = con.execute("""
        SELECT 
            SUM(FORCUR) as total_forcur,
            SUM(RMCUR) as total_rmcur,
            COUNT(DISTINCT CURCODE) as num_currencies,
            COUNT(DISTINCT AGENTNO) as num_agents
        FROM summary
    """).fetchone()

    print(f"\nTransaction Summary:")
    print(f"  Total Foreign Amount: {totals[0]:,.2f}" if totals[0] else "  Total Foreign Amount: 0.00")
    print(f"  Total RM Amount: {totals[1]:,.2f}" if totals[1] else "  Total RM Amount: 0.00")
    print(f"  Number of Currencies: {totals[2]}")
    print(f"  Number of Agents: {totals[3]}")

print("\nConversion complete!")

# Close DuckDB connection
con.close()
