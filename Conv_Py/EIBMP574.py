# !/usr/bin/env python3
"""
Program : EIBMP574
Purpose : Performance Report on Product 574 and Impaired Loans listing.
          Incorporates conventional retail product codes 607 into MEF report.
          Produces two PROC PRINT-style report sections:
            1) Summary totals: O/S Balance, No of Account, Disbursement, Repayment
            2) Impaired Loans detail listing (with CCRIS arrears data)
ESMR    : 2012-1093 (CWK)
"""

import duckdb
import polars as pl
import os
from datetime import date

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR      = r"/data"
SAS_DIR       = os.path.join(BASE_DIR, "sasdata")       # SAS.* parquet files
LOAN_DIR      = os.path.join(BASE_DIR, "loan")          # LOAN.REPTDATE
DISPAY_DIR    = os.path.join(BASE_DIR, "dispay")        # DISPAY.DISPAYMTH<mm>
CCRIS_DIR     = os.path.join(BASE_DIR, "ccris")         # CCRIS parquet files
OUTPUT_DIR    = os.path.join(BASE_DIR, "output")

OUTPUT_FILE   = os.path.join(OUTPUT_DIR, "P574.REPT.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

PAGE_LENGTH   = 60   # lines per page (ASA default)

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def fmt_comma15_2(v) -> str:
    """COMMA15.2 format: right-justified 15 chars with comma thousands."""
    if v is None:
        return f"{'0.00':>15}"
    return f"{float(v):>15,.2f}"

def fmt_comma10(v) -> str:
    """COMMA10. format: right-justified 10 chars with comma."""
    if v is None:
        return f"{'0':>10}"
    return f"{int(v):>10,}"

class ReportWriter:
    """Handles ASA carriage-control output with pagination."""

    def __init__(self, filepath: str, page_length: int = 60):
        self.filepath    = filepath
        self.page_length = page_length
        self._lines      = []
        self._page_lines = 0
        self._first_page = True

    def _flush_pending(self):
        pass  # collected in list, written at close

    def put(self, cc: str, text: str):
        """Emit one line with ASA carriage-control character."""
        self._lines.append((cc, text))

    def write(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            page_count = 0
            for (cc, text) in self._lines:
                if cc == '1':
                    if page_count > 0:
                        # pad to page boundary
                        while (page_count % self.page_length) != 0:
                            f.write(f" \n")
                            page_count += 1
                    f.write(f"1{text}\n")
                    page_count += 1
                else:
                    f.write(f"{cc}{text}\n")
                    page_count += 1


# =============================================================================
# STEP 1: Read REPTDATE and derive macro variables
# =============================================================================
reptdate_path = os.path.join(LOAN_DIR, "REPTDATE.parquet")
con = duckdb.connect()

reptdate_row = con.execute(f"""
    SELECT REPTDATE FROM read_parquet('{reptdate_path}') LIMIT 1
""").fetchone()

reptdate = reptdate_row[0]
if isinstance(reptdate, (int, float)):
    import datetime as _dt
    reptdate = date(1960, 1, 1) + _dt.timedelta(days=int(reptdate))

reptdate_dt  = reptdate if isinstance(reptdate, date) else reptdate.date()
reptmon      = f"{reptdate_dt.month:02d}"
prevmon_val  = reptdate_dt.month - 1 or 12
prevmon      = f"{prevmon_val:02d}"
nowk         = "4"
reptyear     = f"{reptdate_dt.year % 100:02d}"
rdate        = reptdate_dt.strftime("%d/%m/%y")   # DDMMYY8.

# =============================================================================
# STEP 2: Load PRVMTH -- products 574, or 607 with CENSUS in (574.00,574.01,574.02)
# =============================================================================
# *** PREVIOUS MTH ***
prvmth_path = os.path.join(SAS_DIR, f"LOAN{prevmon}{nowk}.parquet")
prvmth = con.execute(f"""
    SELECT *
    FROM read_parquet('{prvmth_path}')
    WHERE PRODUCT = 574
       OR (PRODUCT = 607 AND CENSUS IN (574.00, 574.01, 574.02))
""").pl()

# =============================================================================
# STEP 3: Load CURMTH -- products 574 or 607+CENSUS, active, non-zero balance
# =============================================================================
# *** CURRENT MTH ***
curmth_path = os.path.join(SAS_DIR, f"LOAN{reptmon}{nowk}.parquet")
curmth = con.execute(f"""
    SELECT *, 1 AS NOACC
    FROM read_parquet('{curmth_path}')
    WHERE PAIDIND NOT IN ('P','C')
      AND BALANCE <> 0
      AND (PRODUCT = 574
           OR (PRODUCT = 607 AND CENSUS IN (574.00, 574.01, 574.02)))
""").pl()

# =============================================================================
# STEP 4: Load DISPAY (disbursements/repayments > 0) -- sorted output
# =============================================================================
dispay_path = os.path.join(DISPAY_DIR, f"DISPAYMTH{reptmon}.parquet")
dispay = con.execute(f"""
    SELECT ACCTNO, NOTENO, DISBURSE, REPAID, CENSUS
    FROM read_parquet('{dispay_path}')
    WHERE DISBURSE > 0 OR REPAID > 0
    ORDER BY ACCTNO, NOTENO
""").pl()

# =============================================================================
# STEP 5: Merge PRVMTH + CURMTH -> LOAN (outer, rename BALANCE->LASTBAL in prev)
# =============================================================================
prvmth_r = prvmth.rename({"BALANCE": "LASTBAL"})
loan = prvmth_r.join(curmth, on=["ACCTNO","NOTENO"], how="outer_coalesce")

# =============================================================================
# STEP 6: Merge LOAN + DISPAY (left-join, keep A), filter CENSUS
# =============================================================================
loan = (
    loan
    .join(dispay, on=["ACCTNO","NOTENO"], how="left", suffix="_D")
    .filter(pl.col("CENSUS").is_in([574.00, 574.01, 574.02]))
)

# =============================================================================
# STEP 7: PROC SUMMARY -- sum BALANCE, NOACC, DISBURSE, REPAID
# =============================================================================
sum_cols = ["BALANCE","NOACC","DISBURSE","REPAID"]
ln1 = (
    loan
    .with_columns([pl.col(c).cast(pl.Float64) for c in sum_cols if c in loan.columns])
    .select([pl.col(c).sum().alias(c) for c in sum_cols if c in loan.columns])
)

# =============================================================================
# STEP 8: Write REPORT 1 -- PROC PRINT LN1 (summary totals)
# =============================================================================
rpt = ReportWriter(OUTPUT_FILE, PAGE_LENGTH)

# Titles (SAS TITLE / TITLE1 / TITLE2)
rpt.put('1', 'PUBLIC BANK BERHAD')
rpt.put(' ', f'PERFORMANCE REPORT ON PRODUCT 574 AS AT {rdate}')
rpt.put(' ', 'REPORT ID : EIBMP574')
rpt.put(' ', '')

# Column header with SPLIT='*' labels
# Labels: BALANCE='O/S BALANCE', NOACC='NO OF*ACCOUNT',
#         DISBURSE='DISBURSEMENT*AMOUNT', REPAID='REPAYMENT*AMOUNT'
hdr1 = (
    f"{'O/S BALANCE':>15}  "
    f"{'NO OF':>10}  "
    f"{'DISBURSEMENT':>15}  "
    f"{'REPAYMENT':>15}"
)
hdr2 = (
    f"{'':>15}  "
    f"{'ACCOUNT':>10}  "
    f"{'AMOUNT':>15}  "
    f"{'AMOUNT':>15}"
)
rpt.put(' ', hdr1)
rpt.put(' ', hdr2)
rpt.put(' ', '-' * 65)

# Data row
row0 = ln1.row(0, named=True)
bal     = row0.get("BALANCE",  0) or 0
noacc   = row0.get("NOACC",    0) or 0
disburse= row0.get("DISBURSE", 0) or 0
repaid  = row0.get("REPAID",   0) or 0

data_line = (
    f"{fmt_comma15_2(bal)}  "
    f"{fmt_comma10(noacc)}  "
    f"{fmt_comma15_2(disburse)}  "
    f"{fmt_comma15_2(repaid)}"
)
rpt.put(' ', data_line)
rpt.put(' ', '-' * 65)

# SUM line
rpt.put(' ', data_line)
rpt.put(' ', '')

# =============================================================================
# STEP 9: Load CCRIS data for Impaired Loans section
# =============================================================================
credmsub_path  = os.path.join(CCRIS_DIR, f"CREDMSUBAC{reptmon}{reptyear}.parquet")
provsubac_path = os.path.join(CCRIS_DIR, f"PROVSUBAC{reptmon}{reptyear}.parquet")

credmsub = con.execute(f"""
    SELECT ACCTNUM AS ACCTNO, NOTENO, BRANCH,
           DAYSARR AS DAYARR, MTHARR
    FROM read_parquet('{credmsub_path}')
    ORDER BY ACCTNUM, NOTENO, BRANCH
""").pl()

provsubac = con.execute(f"""
    SELECT ACCTNUM AS ACCTNO, NOTENO, BRANCH
    FROM read_parquet('{provsubac_path}')
    WHERE IMPAIRED_LOAN = 'Y'
    ORDER BY ACCTNUM, NOTENO, BRANCH
""").pl()

# =============================================================================
# STEP 10: Merge CURMTH + CREDMSUB + PROVSUBAC(inner) -> LN2
# =============================================================================
ln2 = (
    curmth
    .join(credmsub,  on=["ACCTNO","NOTENO","BRANCH"], how="left")
    .join(provsubac, on=["ACCTNO","NOTENO","BRANCH"], how="inner")
    .filter(~pl.col("BALANCE").is_in([None, 0]))
)

# =============================================================================
# STEP 11: Write REPORT 2 -- PROC PRINT LN2 (impaired loans detail)
# =============================================================================
# Titles
rpt.put('1', 'PUBLIC BANK BERHAD')
rpt.put(' ', f'PRODUCT 574 (IMPAIRED LOANS) AS AT {rdate}')
rpt.put(' ', 'REPORT ID : EIBMP574')
rpt.put(' ', '')

# Column headers with SPLIT='*' labels
# ACCTNO='ACCOUNT*NO', NOTENO='NOTE*NO', LOANSTAT='LOAN*STATUS',
# BALANCE='O/S BALANCE', DAYARR='DAYS IN*ARREARS', MTHARR='MTHS IN*ARREARS'
hdr_a1 = (
    f"{'ACCOUNT':>15}  "
    f"{'NOTE':>8}  "
    f"{'LOAN':>10}  "
    f"{'O/S BALANCE':>15}  "
    f"{'DAYS IN':>8}  "
    f"{'MTHS IN':>8}"
)
hdr_a2 = (
    f"{'NO':>15}  "
    f"{'NO':>8}  "
    f"{'STATUS':>10}  "
    f"{'':>15}  "
    f"{'ARREARS':>8}  "
    f"{'ARREARS':>8}"
)
rpt.put(' ', hdr_a1)
rpt.put(' ', hdr_a2)
rpt.put(' ', '-' * 75)

sum_bal = 0.0
for row in ln2.iter_rows(named=True):
    acctno   = str(row.get("ACCTNO","") or "")
    noteno   = str(row.get("NOTENO","") or "")
    loanstat = str(row.get("LOANSTAT","") or "")
    balance  = float(row.get("BALANCE", 0) or 0)
    dayarr   = row.get("DAYARR") or 0
    mtharr   = row.get("MTHARR") or 0
    sum_bal += balance

    detail = (
        f"{acctno:>15}  "
        f"{noteno:>8}  "
        f"{loanstat:>10}  "
        f"{fmt_comma15_2(balance)}  "
        f"{int(dayarr):>8}  "
        f"{int(mtharr):>8}"
    )
    rpt.put(' ', detail)

# SUM line
rpt.put(' ', '-' * 75)
sum_line = (
    f"{'':>15}  "
    f"{'':>8}  "
    f"{'':>10}  "
    f"{fmt_comma15_2(sum_bal)}  "
    f"{'':>8}  "
    f"{'':>8}"
)
rpt.put(' ', sum_line)

# =============================================================================
# STEP 12: Write output
# =============================================================================
rpt.write()
con.close()
print(f"Report written to: {OUTPUT_FILE}")
