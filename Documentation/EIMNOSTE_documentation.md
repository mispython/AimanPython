📦 Delivered File:
foreign_exchange_transaction_report.py - Complete conversion of EIMNOSTE report
🎯 Program Overview:
This program processes nostro account foreign exchange transactions from two sources and generates a comprehensive summary report.
Two Data Sources:

Walker File (wknost.dat) - Fixed-width text format
Deposit File (dpnost.dat) - Fixed-width with packed decimal fields

🔑 Key Features:
1. Week Determination (Different from previous programs!)
pythonif 5 <= day <= 10:    NOWK = '1'
elif 11 <= day <= 18: NOWK = '2'
elif 19 <= day <= 25: NOWK = '3'
else:                 NOWK = '4'

### 2. **Walker File Processing**
- Fixed-width text format
- 172+ character records
- Integer amounts (divided by 100)
- Transaction code mapping (23 different types)

### 3. **Deposit File Processing** ⭐
- Fixed-width with **packed decimal fields**
- PD7.2 format for FORCUR and RMCUR
- Account number filtering (excludes range 3997200109-3997204029)
- Complex transaction description logic based on BILLIND and TRIND

### 4. **Transaction Code Mappings**

**Walker Transactions (23 types):**
- 001: OUTWARD DD
- 003: OUTWARD MT
- 006: OUTWARD TT
- 011: BANK GUARANTEE
- 061: LC (Letter of Credit)
- 081-085: INWARD transactions
- 091-093: MISCELLANEOUS, TREASURY CREDIT/DEBIT

**Deposit Transactions:**
- Based on BILLIND: L (Import LC), X (Export LC), I (Inward Bills), O (Outward Bills), G (Bank Guarantee)
- Based on TRIND: O (Outward), R (Replace), I (Inward), B (Buy Back), C (Cancel)

### 5. **Report Generation**
- ASA carriage control
- Currency code grouping
- Agent number subtotals
- Grand total
- Formatted with proper alignment

### 6. **Data Processing Flow**

1. Read REPTDATE → Determine Week (1-4)
           ↓
2. Read Walker File → Map transaction codes → Add descriptions
           ↓
3. Read Deposit File → Unpack packed decimals → Map transactions
           ↓
4. Merge names from both sources → Deduplicate by AGENTNO
           ↓
5. Combine Walker + Deposit → Join with names
           ↓
6. Summarize by CURCODE, AGENTNO, NAME, TRDESC, SIGN
           ↓
7. Generate Report → Agent subtotals → Grand total
           ↓
8. Create SFTP script for file transfer


## 📊 Report Format: ##

                        REPORT ID : EIMNOSTE
         REPORT ON FOREIGN EXCHANGE TRANSACTION AS AT 15/01/26

CURRENCY AGENT NAME                           TRANSACTION          DEBIT(D)/ NO OF      FOREIGN            RM
CODE     NO                                   DESCRIPTION          CREDIT(C) TRANS      AMOUNT             AMOUNT
                                                                            ACTION
------------------------------------------------------------------------------------------------------------------------------------
USD      001   BANK A                         OUTWARD TT           C            5        1,234,567.89      5,678,901.23
               BANK A                         INWARD DD            D            2          234,567.89        987,654.32
                                                                                      -----------------  -----------------
                                                                                         1,469,135.78      6,666,555.55
                                                                                      -----------------  -----------------
              TOTAL                                                                 =    1,469,135.78      6,666,555.55
                                                                                      
