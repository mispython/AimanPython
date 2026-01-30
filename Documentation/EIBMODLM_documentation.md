## 📦 Delivered File: ##
overdraft_limits_report.py - Complete conversion generating both Public Bank and Islamic Bank overdraft reports

## 🎯 Program Overview: ##
This program generates two separate reports on accounts with overdraft/credit limits:
Two Reports Generated:

Public Bank - Accounts with OD (Overdraft) Limits
Public Islamic Bank - Accounts with CLF-i (Cash Line Facility-i) Limits

## 🔑 Key Features: ##
### 1. Dual Report Generation ###
Same code logic, different filters and titles:
AspectPublic BankIslamic BankODPLAN Filter100, 101, 102, 103, 104, 105106Title"ACCOUNTS WITH OD LIMITS""ACCOUNTS WITH CLF-i LIMITS"Status Label"OD ST""CLF-i ST"Bank NamePUBLIC BANK BERHADPUBLIC ISLAMIC BANK BERHAD

### 2. Complex Limit Pivoting ⭐ ###
Transforms multiple limit records per account into columns:
Before (Multiple Rows):
ACCTNO  LMTAMT    LMTRATE  LMTCOLL
123456  100000    5.50     ABC
123456   50000    6.00     XYZ
123456   25000    5.75     DEF
After (Single Row):
ACCTNO  LIMIT1   RATE1  COLL1  LIMIT2  RATE2  COLL2  LIMIT3  RATE3  COLL3
123456  100000   5.50   ABC    50000   6.00   XYZ    25000   5.75   DEF
Up to 5 limits per account are supported.

### 3. Branch Code Formatting ###
Converts numeric branch codes to 3-digit strings with leading zeros:
pythonBRANCH   →   BRN
1        →   '001'
45       →   '045'
123      →   '123'

### 4. Balance Sign Handling ###
pythonif CURBAL < 0:
    BALANCE = (-1) * CURBAL  # Convert to positive
    CRI = NULL
else:
    BALANCE = CURBAL
    CRI = 'CR'  # Mark as credit balance
```

### 5. **Filtering Criteria** ###

**Current Accounts:**
- DEPTYPE IN ('D', 'N')
- APPRLIMT > 1
- ODPLAN: 100-105 (PBB) or 106 (PIBB)

**Overdraft Records:**
- APPRLIMT > 1
- LMTTYPE IN ('Y', 'A')

### 6. **Report Format with Branch Subtotals** ###
```
  P U B L I C   B A N K   B E R H A D
  REPORT TITLE: ACCOUNTS WITH OD LIMITS
  REPORT AS AT 15/01/26

BRN ACCOUNT NO NAME OF CUSTOMER          BASE OD    OUSTANDING      APPROVED        LIMIT1      RATE1 COLL1
                                          RATE ST    BALANCE         LIMIT
------------------------------------------------------------------------------------------------------------------------------------
001 1234567890 ABC COMPANY SDN BHD       5.50 OK     1,234,567.89    10,000,000.00   5,000,000.00  5.50 ABC
                                                                                       3,000,000.00  6.00 XYZ

                         -------------------------------------------------
                         TOTAL APPROVED LIMITS  =         10,000,000.00

                         TOTAL ACCOUNTS         =                  1

                         TOTAL OPERATIVE LIMITS =          8,000,000.00
                         -------------------------------------------------
```

### 7. **Calculated Fields** ###

- **LIMITS**: Sum of LIMIT1 through LIMIT5 (operative limits)
- **NOACCT**: Counter for number of accounts (always 1 per row)
- **BALANCE**: Absolute value of CURBAL with sign indicator

## 📊 Processing Steps: ##
```
1. Read REPTDATE → Format as DD/MM/YY
           ↓
2. Filter Current Accounts → DEPTYPE, APPRLIMT, ODPLAN
           ↓
3. Filter Overdraft Records → APPRLIMT, LMTTYPE
           ↓
4. Add Row Counter → ROW_NUMBER() per ACCTNO
           ↓
5. Pivot Limits → Transform rows to columns (LIMIT1-5)
           ↓
6. Merge Current + Overdraft → INNER JOIN on ACCTNO
           ↓
7. Format Branch Codes → Zero-pad to 3 digits
           ↓
8. Generate Report → Group by BRN, print subtotals


## 🔧 Technical Highlights: ##
Window Functions for Row Numbering
sqlROW_NUMBER() OVER (PARTITION BY ACCTNO ORDER BY LMTAMT DESC) as RCNT
Conditional Aggregation for Pivoting
sqlMAX(CASE WHEN RCNT = 1 THEN LMTAMT END) as LIMIT1,
MAX(CASE WHEN RCNT = 2 THEN LMTAMT END) as LIMIT2,
...
COALESCE for NULL Handling
sqlCOALESCE(o.LIMIT2, 0) as LIMIT2,
COALESCE(o.RATE2, 0.0) as RATE2
