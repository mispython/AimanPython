# EIEMCRLS - SAS to Python Conversion Documentation

## Overview
This document describes the conversion of the EIEMCRLS SAS program to Python, maintaining exact functional and formatting equivalence.

## Program Purpose
EIQPROM2 generates automailing listings for reinstatement of loans, processing loan data to create:
- Physical mail output files (EMCPBB, EMCPIB)
- Email statement files (EMLPBB, EMLPIB) 
- Email index files (EMXPBB, EMXPIB)
- Summary control files (EMCPBBS, EMCPIBS, EMLPBBS, EMLPIBS)
- Management report with ASA carriage control

## Key Technical Specifications

### Input Files (Parquet Format)
1. **PROMOTE.LOAN{month}** - Promoted loan data filtered by month
   - Key columns: GUAREND, REPAID, ACCTNO, BRANCH, NOTENO
   - Filter: REPAID > 100000

2. **LN.LNNAME** - PBB (Personal Banking) customer names
   - Merged with filtered loan list

3. **LNI.LNNAME** - PIB (Islamic Banking) customer names
   - Merged with filtered loan list

### Output Files (Text Format)

#### Detail Files (Fixed Format)
- **EMCPBB.txt** - PBB physical mail records (249 chars/line)
- **EMLPBB.txt** - PBB email records (291 chars/line)
- **EMXPBB.txt** - PBB email index (164 chars/line)
- **EMCPIB.txt** - PIB physical mail records (249 chars/line)
- **EMLPIB.txt** - PIB email records (291 chars/line)
- **EMXPIB.txt** - PIB email index (164 chars/line)

#### Summary Files (Fixed Format)
- **EMCPBBS.txt** - PBB physical mail control (53 chars)
- **EMLPBBS.txt** - PBB email control (53 chars)
- **EMCPIBS.txt** - PIB physical mail control (53 chars)
- **EMLPIBS.txt** - PIB email control (52 chars)

#### Report File
- **eiqprom2_report.txt** - Management report with ASA carriage control

## Data Flow

```
PROMOTE.LOAN{month} ──┐
                      ├─> Filter (REPAID > 100000)
                      │   Remove duplicates by GUAREND
                      │   Keep first (highest REPAID)
                      │
                      ├─> Merge with LN.LNNAME ──> PBBNAME
                      │                              │
                      │                              ├─> Non-email ──> EMCPBB + EMCPBBS
                      │                              │
                      │                              └─> Email ──────> EMLPBB + EMLPBBS + EMXPBB
                      │
                      └─> Merge with LNI.LNNAME ─> PIBNAME
                                                     │
                                                     ├─> Non-email ──> EMCPIB + EMCPIBS
                                                     │
                                                     └─> Email ──────> EMLPIB + EMLPIBS + EMXPIB

PBBNAME + PIBNAME ──> Combined ──> Report (with ASA)
```

## Technical Implementation Details

### 1. Date Calculations
SAS uses TODAY()-DAY(TODAY()) to get the last day of previous month.
Python equivalent:
```python
first_of_month = today.replace(day=1)
reptdate = first_of_month - timedelta(days=1)
```

Date formats:
- REPTDT: DDMMYY (e.g., "310125" for Jan 31, 2025)
- INDXDT: YYYYMMDD (e.g., "20250131")
- RDATE: DD/MM/YY (e.g., "31/01/25")
- REPTMON: MM zero-padded (e.g., "01")
- REPTYR: YYYY (e.g., "2025")
- MTHNAM: 3-letter month uppercase (e.g., "JAN")

### 2. ID Encryption
The %ENCR_ID macro is replaced with a Python function using SHA-256 hashing:
```python
def encrypt_id(id_value):
    hash_obj = hashlib.sha256(id_str.encode())
    hash_digest = hash_obj.digest()
    encoded = base64.b64encode(hash_digest).decode('ascii')[:24]
    return encoded
```
Output: 24-character masked ID string

### 3. Email vs Physical Mail Split
Records are split based on:
- MAILCODE in (' ', '13', '14') AND EMAILADD not empty → Email
- Otherwise → Physical mail

### 4. File Format Specifications

#### EMCPBB/EMCPIB Format (249 characters)
```
Position  Length  Field       Format
1         1       Record Type 'B'
2-7       6       Report Date DDMMYY
8-47      40      Name Line 1 Left-aligned
48-87     40      Name Line 2 Left-aligned
88-127    40      Name Line 3 Left-aligned
128-167   40      Name Line 4 Left-aligned
168-207   40      Name Line 5 Left-aligned
208-214   7       Branch      Zero-padded numeric
215-225   11      Account No  Zero-padded numeric
226-249   24      Masked ID   Left-aligned
```

#### EMLPBB/EMLPIB Format (291 characters)
```
Position  Length  Field       Format
1         1       Record Type 'B'
2-7       6       Report Date DDMMYY
8-47      40      Name Line 1 Left-aligned
48-87     40      Name Line 2 Left-aligned
88-127    40      Name Line 3 Left-aligned
128-167   40      Name Line 4 Left-aligned
168-207   40      Name Line 5 Left-aligned
208-214   7       Branch      Zero-padded numeric
215-225   11      Account No  Zero-padded numeric
226        1       Space
227-266   40      Variable ID Left-aligned
267        1       Space
268-291   24      Masked ID   Left-aligned
```

#### EMXPBB/EMXPIB Format (164 characters)
```
Position  Length  Field          Format
1-40      40      Variable ID    Left-aligned
41-100    60      Email Address  Left-aligned
101-107   7       Statement Date MMMYYYY (e.g., "JAN2025")
108-147   40      Name Line 1    Left-aligned
148-164   17      New IC         Left-aligned
```

#### Summary File Format (53/52 characters)
```
Position  Length  Field         Format
1-8       8       File ID       'LNRIHLCP'/'LNRIHLCE'/'LNRIHLIP'/'LNRIHLIPE'
9-30      22      Spaces
31-36     6       Report Date   DDMMYY
37        1       Space (except EMLPIBS which has no space)
38-53     16      Record Count  Zero-padded numeric
```

### 5. Variable ID Generation
For email statements:
```
PBB: "PBB_EMAIL_STMT_RIL_C" + {ROWCNT:010d} + "_" + {INDXDT}
PIB: "PIB_EMAIL_STMT_RIL_C" + {ROWCNT:010d} + "_" + {INDXDT}
```
Example: "PBB_EMAIL_STMT_RIL_C0000000001_20250131"

### 6. Report Format with ASA Carriage Control

ASA (American Standards Association) carriage control characters:
- '1' = Form feed (new page)
- ' ' = Single space (normal line)
- '0' = Double space
- '-' = Triple space
- '+' = Overprint

Page specifications:
- Length: 60 lines per page
- Headers on each page:
  - Line 1: Form feed + blank
  - Line 2: Title centered
  - Line 3: Subtitle with date centered
  - Line 4: Blank
  - Line 5: Column headers (line 1)
  - Line 6: Column headers (line 2)
  - Line 7: Separator line

Report structure:
```
BRANCH BRANCH A/C NO    NOTE PRODUCT  NAME OF BORROWER/CUSTOMER               MAIL CODE
CODE                     NO   CODE
-----------------------------------------------------------------
 123   123  1234567890  1234 PROD001  JOHN DOE                                13

   ---------------------------------------------------------------------------------------
   NO OF BORROWER/CUSTOMER :      125

(Repeated for each branch)
```

### 7. Data Processing Logic

#### Step 1: Filter and Deduplicate
```sql
SELECT * FROM PROMOTE.LOAN
WHERE REPAID > 100000
ORDER BY GUAREND, REPAID DESC
```
Then keep first record per GUAREND (highest REPAID value)

#### Step 2: Merge Operations
Inner join filtered loans with customer names:
- LN.LNNAME → PBBNAME
- LNI.LNNAME → PIBNAME

#### Step 3: Email/Mail Split
Apply business rule for email eligibility

#### Step 4: Output Generation
Write fixed-format files with exact positioning

### 8. Technology Stack

**Libraries Used:**
- `duckdb`: SQL queries on Parquet files (efficient, fast)
- `polars`: DataFrame operations (faster than pandas)
- `datetime`: Date calculations
- `hashlib`: SHA-256 encryption
- `base64`: Encoding for masked IDs
- `pathlib`: File system operations

**Why These Choices:**
- DuckDB: Superior Parquet reading performance, SQL interface
- Polars: Faster than Pandas, better memory efficiency
- No sorting before output: Data pre-sorted in queries

### 9. Performance Optimizations

1. **Single Query Filtering**: Combined WHERE and ORDER BY in DuckDB
2. **Deduplication**: Using Polars `.unique()` with keep='first'
3. **Minimal Sorts**: Only sort when absolutely necessary
4. **Batch Processing**: Write files iteratively to manage memory
5. **No Intermediate Files**: Direct DataFrame to file output

### 10. Error Handling Considerations

The code handles:
- Missing/null values in name fields
- Empty email addresses
- Null branch/account numbers
- Empty datasets (0 records)

### 11. Testing Checklist

To validate the conversion:

1. **Data Validation**
   - [ ] Record counts match at each step
   - [ ] Deduplication works correctly (first GUAREND kept)
   - [ ] Merge results correct (inner join)
   - [ ] Email/mail split correct

2. **File Format Validation**
   - [ ] Line lengths exact (249, 291, 164, 53/52 chars)
   - [ ] Field positions correct
   - [ ] Numeric formatting (zero-padding)
   - [ ] Text alignment (left/right)
   - [ ] Control file headers match

3. **Report Validation**
   - [ ] ASA characters correct
   - [ ] Page breaks at 60 lines
   - [ ] Headers on each page
   - [ ] Branch subtotals correct
   - [ ] Grand total matches

4. **Date Validation**
   - [ ] REPTDT format DDMMYY
   - [ ] INDXDT format YYYYMMDD
   - [ ] Month name uppercase
   - [ ] Last day of previous month

5. **Business Logic**
   - [ ] REPAID > 100000 filter
   - [ ] Highest REPAID per GUAREND
   - [ ] Email criteria (MAILCODE + EMAILADD)
   - [ ] Encrypted ID 24 characters

## Configuration

Update these paths in the script:
```python
# Input paths
PROMOTE_LOAN_PATH = "/data/input/promote_loan_{month}.parquet"
LN_LNNAME_PATH = "/data/input/ln_lnname.parquet"
LNI_LNNAME_PATH = "/data/input/lni_lnname.parquet"

# Output paths
EMCPBB_PATH = "/data/output/emcpbb.txt"
# ... etc
```

## Execution

```bash
python3 eiqprom2.py
```

Expected console output:
```
Starting EIQPROM2 processing...
Report Date: 31/01/25
Report Month: 01

Step 1: Loading and filtering PROMOTE.LOAN data...
  Records in RLSLIST: 1234

Step 2: Processing PBB data...
  Records in PBBNAME after merge: 567
  Records in PBBNAME (non-email): 450
  Records in MAILPBB (email): 117
  
  Writing EMCPBB file...
  EMCPBB records written: 450
  
  Processing MAILPBB email statements...
  Writing EMLPBB file...
  EMLPBB records written: 117
  EMXPBB records written: 117

Step 3: Processing PIB data...
  Records in PIBNAME after merge: 667
  Records in PIBNAME (non-email): 550
  Records in MAILPIB (email): 117
  
  Writing EMCPIB file...
  EMCPIB records written: 550
  
  Processing MAILPIB email statements...
  Writing EMLPIB file...
  EMLPIB records written: 117
  EMXPIB records written: 117

Step 4: Generating report...
  Total records for report: 1000
  Report written to: /data/output/eiqprom2_report.txt
  Total pages: 18

======================================================================
EIQPROM2 processing completed successfully!
======================================================================
```

## Maintenance Notes

1. **Date Logic**: Report always runs for previous month-end
2. **File IDs**: 
   - LNRIHLCP = Loan Reinstatement High Level Consumer Physical
   - LNRIHLCE = Loan Reinstatement High Level Consumer Email
   - LNRIHLIP = Loan Reinstatement High Level Islamic Physical
   - LNRIHLIPE = Loan Reinstatement High Level Islamic Physical Email
3. **MAILCODE Values**: ' ', '13', '14' indicate email-eligible
4. **REPAID Threshold**: 100,000 minimum for inclusion

## Dependencies

```
duckdb>=0.9.0
polars>=0.19.0
pyarrow>=14.0.0
```

Install with:
```bash
pip install duckdb polars pyarrow
```
