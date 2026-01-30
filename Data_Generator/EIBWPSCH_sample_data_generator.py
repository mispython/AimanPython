#!/usr/bin/env python3
"""
Sample Data Generator for Weekly Loan Payment Schedule
Generates synthetic test data including binary file with packed decimal format
"""

import duckdb
import struct
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# Configuration
OUTPUT_DIR = Path("input/EIBWPSCH")
REPORT_DATE = datetime(2026, 1, 8)  # Day 8 = Week 01
NUM_LOANS = 500
NUM_PAYMENTS_PER_LOAN = 2  # Average payments per loan

# Create output directory
OUTPUT_DIR.mkdir(exist_ok=True)
print(f"Generating sample data in {OUTPUT_DIR}/")

# Initialize DuckDB
con = duckdb.connect(':memory:')


# ============================================================================
# PACKED DECIMAL UTILITIES
# ============================================================================

def pack_decimal(value, length, decimals=0):
    """
    Pack a number into IBM packed decimal format

    Args:
        value: Number to pack
        length: Number of bytes for packed decimal
        decimals: Number of decimal places

    Returns:
        bytes object in packed decimal format
    """
    if value is None:
        value = 0

    # Adjust for decimal places
    if decimals > 0:
        value = int(value * (10 ** decimals))
    else:
        value = int(value)

    # Determine sign
    if value < 0:
        sign = 0x0D  # Negative
        value = abs(value)
    else:
        sign = 0x0C  # Positive

    # Convert to string and pad
    value_str = str(value)
    # Each byte holds 2 digits, last nibble is sign
    total_digits = (length * 2) - 1
    value_str = value_str.zfill(total_digits)

    # Pack into bytes
    result = bytearray()
    for i in range(0, len(value_str), 2):
        if i + 1 < len(value_str):
            high = int(value_str[i])
            low = int(value_str[i + 1])
        else:
            high = int(value_str[i])
            low = 0
        result.append((high << 4) | low)

    # Add sign to last nibble
    if len(result) > 0:
        result[-1] = (result[-1] & 0xF0) | sign

    # Pad to required length
    while len(result) < length:
        result.insert(0, 0x00)

    return bytes(result[:length])


# ============================================================================
# 1. Generate Report Date
# ============================================================================
print("\n1. Generating report date file...")
con.execute(f"""
    CREATE TABLE reptdate AS
    SELECT DATE '{REPORT_DATE.strftime('%Y-%m-%d')}' as REPTDATE
""")

output_file = OUTPUT_DIR / "ln_reptdate.parquet"
con.execute(f"COPY reptdate TO '{output_file}' (FORMAT PARQUET)")
print(f"   Created: {output_file}")
print(f"   Report Date: {REPORT_DATE.strftime('%Y-%m-%d')} (Day {REPORT_DATE.day} = Week 01)")


# ============================================================================
# 2. Generate Loan Note Data
# ============================================================================
print("\n2. Generating loan note data...")

loan_notes = []
for i in range(NUM_LOANS):
    acctno = 1000000000 + i  # 10-digit account numbers
    # Generate 1-3 notes per account
    num_notes = random.randint(1, 3)
    for j in range(num_notes):
        noteno = (j + 1) * 10  # 10, 20, 30, etc.
        loan_notes.append({
            'ACCTNO': acctno,
            'NOTENO': noteno
        })

# con.execute("CREATE TABLE loan_notes AS SELECT * FROM loan_notes")
df_loan_notes = pd.DataFrame(loan_notes)
con.execute("CREATE TABLE loan_notes AS SELECT * FROM df_loan_notes")

output_file = OUTPUT_DIR / "ln_lnnote.parquet"
con.execute(f"COPY loan_notes TO '{output_file}' (FORMAT PARQUET)")
print(f"   Created: {output_file}")
print(f"   Loan notes: {len(loan_notes):,}")


# ============================================================================
# 3. Generate Payment Schedule Binary File
# ============================================================================
print("\n3. Generating payment schedule binary file...")

payment_types = ['M', 'Q', 'S', 'A']  # Monthly, Quarterly, Semi-annual, Annual
schedule_freqs = ['M', 'Q', 'S', 'A', 'W', 'B']  # + Weekly, Bi-weekly

payment_records = []
binary_records = []

for loan in loan_notes:
    # Some loans have payments, some don't
    if random.random() < 0.8:  # 80% have payments
        num_payments = random.randint(1, NUM_PAYMENTS_PER_LOAN)

        for k in range(num_payments):
            # Generate payment details
            acctno = loan['ACCTNO']
            noteno = loan['NOTENO']

            # Effective date - various scenarios
            day_choices = [15, 31, 99, 28, 29, 30] + list(range(1, 29))
            day = random.choice(day_choices)
            month = random.randint(1, 12)
            year = random.randint(2026, 2027)

            # EFFDAT in YYYYMMDD format
            effdat = year * 10000 + month * 100 + day

            paytype = random.choice(payment_types)
            nopay = random.randint(1, 60)  # 1 to 60 payments
            payamt = random.uniform(100, 50000)  # Payment amount
            schfreq = random.choice(schedule_freqs)

            # Payment maintain date (some have it, some don't)
            if random.random() < 0.5:
                pmaint_month = random.randint(1, 12)
                pmaint_day = random.randint(1, 28)
                pmaint_year = random.randint(2025, 2026)
                paymaintain = pmaint_month * 1000000 + pmaint_day * 10000 + pmaint_year
            else:
                paymaintain = 0

            # Store for reference
            payment_records.append({
                'ACCTNO': acctno,
                'NOTENO': noteno,
                'EFFDAT': effdat,
                'PAYTYPE': paytype,
                'NOPAY': nopay,
                'PAYAMT': payamt,
                'SCHFREQ': schfreq,
                'PAYMAINTAIN': paymaintain
            })

            # Create binary record with packed decimals
            record = bytearray(100)  # 100 bytes per record

            # @001 ACCTNO PD6.
            pd_acctno = pack_decimal(acctno, 6, 0)
            record[0:6] = pd_acctno

            # @007 NOTENO PD3.
            pd_noteno = pack_decimal(noteno, 3, 0)
            record[6:9] = pd_noteno

            # @011 EFFDAT PD6.
            pd_effdat = pack_decimal(effdat, 6, 0)
            record[10:16] = pd_effdat

            # @018 PAYTYPE $1.
            record[17] = ord(paytype)

            # @019 NOPAY PD3.
            pd_nopay = pack_decimal(nopay, 3, 0)
            record[18:21] = pd_nopay

            # @022 PAYAMT PD8.2
            pd_payamt = pack_decimal(payamt, 8, 2)
            record[21:29] = pd_payamt

            # @054 SCHFREQ $1.
            record[53] = ord(schfreq)

            # @061 PAYMAINTAIN PD6.
            pd_paymaint = pack_decimal(paymaintain, 6, 0)
            record[60:66] = pd_paymaint

            binary_records.append(bytes(record))

# Write binary file
output_file = OUTPUT_DIR / "paysfile.dat"
with open(output_file, 'wb') as f:
    for record in binary_records:
        f.write(record)

print(f"   Created: {output_file}")
print(f"   Payment records: {len(payment_records):,}")
print(f"   Binary file size: {len(binary_records) * 100:,} bytes")


# ============================================================================
# 4. Generate Statistics
# ============================================================================
print("\n" + "=" * 70)
print("SAMPLE DATA GENERATION COMPLETE")
print("=" * 70)

print(f"\nData Statistics:")
print(f"  Report Date: {REPORT_DATE.strftime('%Y-%m-%d')}")
print(f"  Week Number: 01 (Day {REPORT_DATE.day})")
print(f"  Loan Notes: {len(loan_notes):,}")
print(f"  Payment Records: {len(payment_records):,}")

# Payment statistics
if payment_records:
    total_amount = sum(p['PAYAMT'] for p in payment_records)
    avg_amount = total_amount / len(payment_records)
    total_nopay = sum(p['NOPAY'] for p in payment_records)

    print(f"\nPayment Details:")
    print(f"  Total Payment Amount: ${total_amount:,.2f}")
    print(f"  Average Payment: ${avg_amount:,.2f}")
    print(f"  Total Scheduled Payments: {total_nopay:,}")
    print(f"  Loans with Payments: {len(set(p['ACCTNO'] for p in payment_records)):,}")

# Sample effective dates
print(f"\nSample Effective Dates (YYYYMMDD):")
for i, p in enumerate(payment_records[:5]):
    print(f"  {i + 1}. EFFDAT={p['EFFDAT']} (Day {p['EFFDAT'] % 100})")

# Payment type distribution
paytype_dist = {}
for p in payment_records:
    pt = p['PAYTYPE']
    paytype_dist[pt] = paytype_dist.get(pt, 0) + 1

print(f"\nPayment Type Distribution:")
for pt, count in sorted(paytype_dist.items()):
    print(f"  {pt}: {count:,} ({count * 100 / len(payment_records):.1f}%)")

# Records with payment maintain dates
with_paymaint = sum(1 for p in payment_records if p['PAYMAINTAIN'] > 0)
print(f"\nPayment Maintain Dates:")
print(f"  With dates: {with_paymaint:,} ({with_paymaint * 100 / len(payment_records):.1f}%)")
print(f"  Without dates: {len(payment_records) - with_paymaint:,}")

print(f"\nGenerated Files:")
print(f"  {OUTPUT_DIR / 'ln_reptdate.parquet'}")
print(f"  {OUTPUT_DIR / 'ln_lnnote.parquet'}")
print(f"  {OUTPUT_DIR / 'paysfile.dat'}")

print(f"\nYou can now run: python weekly_loan_payment_schedule.py")
print("=" * 70)


# ============================================================================
# 5. Verify Packed Decimal Encoding (Sample)
# ============================================================================
print("\nVerifying Packed Decimal Encoding (First Record):")
if binary_records:
    first_record = binary_records[0]
    first_payment = payment_records[0]

    print(f"\nOriginal Values:")
    print(f"  ACCTNO: {first_payment['ACCTNO']}")
    print(f"  NOTENO: {first_payment['NOTENO']}")
    print(f"  PAYAMT: ${first_payment['PAYAMT']:.2f}")

    print(f"\nBinary Representation (first 30 bytes):")
    hex_str = ' '.join(f'{b:02X}' for b in first_record[:30])
    print(f"  {hex_str}")

    print(f"\nField Positions:")
    print(f"  @001-006 (ACCTNO):  {' '.join(f'{b:02X}' for b in first_record[0:6])}")
    print(f"  @007-009 (NOTENO):  {' '.join(f'{b:02X}' for b in first_record[6:9])}")
    print(f"  @011-016 (EFFDAT):  {' '.join(f'{b:02X}' for b in first_record[10:16])}")
    print(f"  @022-029 (PAYAMT):  {' '.join(f'{b:02X}' for b in first_record[21:29])}")

# Close connection
con.close()
