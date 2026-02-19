# !/usr/bin/env python3
"""
Program : EIBMISL2
Purpose : Monthly Extraction for Islamic Products
          Profile on Islamic (Personal/Joint) Al-Wadiah Savings Account
          Products 204 and 215 -- PROC TABULATE-style report with
            deposit range, race, and purpose breakdowns.
Dependency: %INC PGM(PBMISFMT, PBBDPFMT)
# Placeholder: PBMISFMT formats -- SACUSTCD, STATECD, SAPROD, SADENOM,
#              DDRANGE, ISARANGE, CARANGE, IWSRNGE, CARANGE
# Placeholder: PBBDPFMT formats -- SACUSTCD, SADENOM, etc.
# Placeholder: $RACE, $PURPOSE format mappings (from PBMISFMT)
"""

import duckdb
import polars as pl
import os
from datetime import date

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR    = r"/data"
DEPOSIT_DIR = os.path.join(BASE_DIR, "deposit")  # DEPOSIT.REPTDATE, DEPOSIT.SAVING
OUTPUT_DIR  = os.path.join(BASE_DIR, "output")

OUTPUT_FILE = os.path.join(OUTPUT_DIR, "EIBMISL2.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

PAGE_LENGTH = 60

# YEARCUTOFF = 1930 (affects 2-digit year interpretation -- noted but date parsing uses full dates)

# =============================================================================
# FORMAT DEFINITIONS -- from PBMISFMT and PBBDPFMT
# Dependency: %INC PGM(PBMISFMT, PBBDPFMT)
# =============================================================================
from PBBDPFMT import SADenomFormat, SAProductFormat
from PBMISFMT import (
    format_race, format_purpose,
    format_iwsrnge,
    invalue_isarange, format_isarangd,
    invalue_carange,  format_caranged,
    format_sadprg,
)

def apply_fmt(val, fmt_fn, default: str = "") -> str:
    """Apply a format function to val; return default on None/error."""
    if val is None:
        return default
    try:
        result = fmt_fn(val)
        return result if result is not None else default
    except (TypeError, ValueError, KeyError):
        return default

# Wrappers that match the original dict-based call pattern
def _sacustcd(custcode) -> str:
    """SACUSTCD: customer code -> BNM counterparty string (not used in grouping; kept for compatibility)."""
    return str(custcode) if custcode is not None else ''

def _statecd(branch) -> str:
    """STATECD: branch number -> single-letter state code.
    Not directly available from PBMISFMT (which maps branch -> 3-letter code).
    Branch-to-state mapping is approximated via BRCHCD -> $STATE chain.
    Kept as pass-through; programs that need it should join to a branch table."""
    return str(branch) if branch is not None else ''

def _saprod(product) -> str:
    """SAPROD (PBBDPFMT): product -> BNM account code string."""
    return SAProductFormat.format(int(product) if product is not None else None)

def _sadenom(product) -> str:
    """SADENOM (PBBDPFMT): product -> 'I' (Islamic) or 'D' (Domestic)."""
    return SADenomFormat.format(int(product) if product is not None else None)

def _ddrange(avgamt) -> str:
    """DDRANGE: average amount -> savings deposit range label (SADPRG format)."""
    return format_sadprg(float(avgamt) if avgamt is not None else None)

def _isarange(curbal) -> int:
    """ISARANGE (INVALUE): current balance -> discrete bucket for Islamic savings range."""
    return invalue_isarange(float(curbal) if curbal is not None else None)

def _carange(curbal) -> int:
    """CARANGE (INVALUE): current balance -> discrete bucket for CA range."""
    return invalue_carange(float(curbal) if curbal is not None else None)

def _iwsrnge(curbal) -> str:
    """IWSRNGE: current balance -> Islamic Wadiah savings range label."""
    return format_iwsrnge(float(curbal) if curbal is not None else None)

# =============================================================================
# HELPERS
# =============================================================================
SAS_EPOCH = date(1960, 1, 1)

def parse_sas_date(v) -> date | None:
    if v is None:
        return None
    if isinstance(v, date):
        return v
    if isinstance(v, (int, float)):
        import datetime as _dt
        return SAS_EPOCH + _dt.timedelta(days=int(v))
    return None

def parse_mmddyy8(val) -> date | None:
    """Parse MMDDYYYY 8-digit integer."""
    if val is None:
        return None
    s = f"{int(val):08d}"
    try:
        return date(int(s[4:8]), int(s[:2]), int(s[2:4]))
    except (ValueError, TypeError):
        return None

def fmt_comma10(v) -> str:
    if v is None: return f"{'0':>10}"
    return f"{float(v):>10,.0f}"

def fmt_comma6_2(v) -> str:
    if v is None: return f"{'0.00':>6}"
    return f"{float(v):>6,.2f}"

def fmt_comma15_2(v) -> str:
    if v is None: return f"{'0.00':>15}"
    return f"{float(v):>15,.2f}"

class ReportWriter:
    def __init__(self, filepath, page_length=60):
        self.filepath, self.page_length, self._lines = filepath, page_length, []
    def put(self, cc, text):
        self._lines.append((cc, text))
    def write(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            pc = 0
            for cc, text in self._lines:
                if cc == '1':
                    if pc > 0:
                        while pc % self.page_length != 0:
                            f.write(" \n"); pc += 1
                    f.write(f"1{text}\n"); pc += 1
                else:
                    f.write(f"{cc}{text}\n"); pc += 1

# =============================================================================
# STEP 1: REPTDATE
# =============================================================================
con = duckdb.connect()
reptdate_path = os.path.join(DEPOSIT_DIR, "REPTDATE.parquet")
rr = con.execute(f"SELECT REPTDATE FROM read_parquet('{reptdate_path}') LIMIT 1").fetchone()
reptdate = rr[0]
reptdate_dt = parse_sas_date(reptdate) or date.today()

day = reptdate_dt.day
if   1  <= day <= 8:  nowk = '1'
elif 9  <= day <= 15: nowk = '2'
elif 16 <= day <= 22: nowk = '3'
else:                 nowk = '4'

reptyear = str(reptdate_dt.year)
reptmon  = f"{reptdate_dt.month:02d}"
reptday  = reptdate_dt.day
rdate    = reptdate_dt.strftime("%d/%m/%y")

# =============================================================================
# STEP 2: Load DEPOSIT.SAVING, filter products 204, 215; process active accounts
# =============================================================================
saving_path = os.path.join(DEPOSIT_DIR, "SAVING.parquet")
saving_raw  = con.execute(f"SELECT * FROM read_parquet('{saving_path}')").pl()

alwsa_rows = []
for row in saving_raw.filter(pl.col("PRODUCT").is_in([204, 215])).iter_rows(named=True):
    openind = (row.get("OPENIND") or "").strip()
    if openind in ("B", "C", "P"):
        continue

    # ACCYTD: opened this year with no close date
    accytd = 0
    opendt_raw  = row.get("OPENDT")
    closedt_raw = row.get("CLOSEDT")
    if opendt_raw and opendt_raw != 0 and (closedt_raw is None or closedt_raw == 0):
        opendt = parse_mmddyy8(opendt_raw)
        if opendt and opendt.year == reptdate_dt.year:
            accytd = 1

    product  = int(row.get("PRODUCT") or 0)
    custcode = row.get("CUSTCODE")
    branch   = row.get("BRANCH")
    curbal   = float(row.get("CURBAL") or 0)
    avgamt   = float(row.get("AVGAMT") or 0)

    custcd   = _sacustcd(custcode)
    statecd  = _statecd(branch)
    prodcd   = _saprod(product)
    amtind   = _sadenom(product)
    avgrnge  = _ddrange(avgamt)

    # range based on product
    if product == 214:
        range_val = _isarange(curbal)
    else:
        range_val = _carange(curbal)

    purpose = (row.get("PURPOSE") or "").strip() or "0"
    race    = (row.get("RACE")    or "").strip() or "0"

    alwsa_rows.append({
        "PRODUCT":  product,
        "CUSTCD":   custcd,
        "STATECD":  statecd,
        "PRODCD":   prodcd,
        "AMTIND":   amtind,
        "AVGRNGE":  avgrnge,
        "RANGE":    range_val,
        "CURBAL":   curbal,
        "AVGAMT":   avgamt,
        "ACCYTD":   accytd,
        "PURPOSE":  purpose,
        "RACE":     race,
    })

alwsa = pl.DataFrame(alwsa_rows)

# =============================================================================
# STEP 3: Compute DEPRANGE from CURBAL using IWSRNGE format
# =============================================================================
alwsa1 = alwsa.with_columns([
    pl.col("CURBAL").map_elements(
        lambda v: _iwsrnge(v), return_dtype=pl.Utf8
    ).alias("DEPRANGE"),
    pl.col("PURPOSE").fill_null("0"),
    pl.col("RACE").fill_null("0"),
])

# =============================================================================
# STEP 4: PROC SUMMARY by PURPOSE, RACE, CUSTCD, AVGRNGE, DEPRANGE, PRODUCT
#         OUTPUT: NOACCT=_FREQ_, AVGACCT=N(AVGAMT), AVGAMT=SUM(AVGAMT),
#                 ACCYTD=SUM(ACCYTD), CURBAL=SUM(CURBAL)
# =============================================================================
group_cols = ["PURPOSE", "RACE", "CUSTCD", "AVGRNGE", "DEPRANGE", "PRODUCT"]
alwsa1_sum = (
    alwsa1
    .with_columns([pl.col(c).cast(pl.Float64) for c in ["AVGAMT", "CURBAL", "ACCYTD"]])
    .group_by(group_cols)
    .agg([
        pl.len().alias("NOACCT"),
        pl.col("AVGAMT").count().alias("AVGACCT"),
        pl.col("AVGAMT").sum().alias("AVGAMT"),
        pl.col("ACCYTD").sum().alias("ACCYTD"),
        pl.col("CURBAL").sum().alias("CURBAL"),
    ])
)

# AVGAMT = AVGAMT / REPTDAY
alwsa1_final = alwsa1_sum.with_columns(
    (pl.col("AVGAMT") / reptday).alias("AVGAMT")
)

# =============================================================================
# STEP 5: Produce PROC TABULATE-style report grouped by DEPRANGE
# =============================================================================
rpt = ReportWriter(OUTPUT_FILE, PAGE_LENGTH)

rpt.put('1', "REPORT ID : EIBMISL2")
rpt.put(' ', "PUBLIC ISLAMIC BANK BERHAD")
rpt.put(' ', "FOR ISLAMIC BANKING DEPARTMENT")
rpt.put(' ', "PROFILE ON ISLAMIC(PERSONAL/JOINT) AL-WADIAH SAVINGS ACCOUNT")
rpt.put(' ', f"PRODUCT (204,215), REPORT AS AT {rdate}")
rpt.put(' ', "")

# Header
hdr = (
    f"{'DEPOSIT RANGE':<30}"
    f"{'NO OF A/C':>10}{'%':>6}"
    f"{'OPEN YTD':>10}{'%':>6}"
    f"{'O/S BALANCE':>15}{'%':>6}{'AVG AMT/ACCT':>15}"
    f"{'AVERAGE BAL':>15}{'%':>6}{'AVG BAL/ACCT':>15}"
)
rpt.put(' ', hdr)
rpt.put(' ', "-" * len(hdr))

# Aggregate by DEPRANGE for report
range_summary = (
    alwsa1_final
    .group_by("DEPRANGE")
    .agg([
        pl.col("NOACCT").sum(),
        pl.col("ACCYTD").sum(),
        pl.col("CURBAL").sum(),
        pl.col("AVGAMT").sum(),
    ])
    .sort("DEPRANGE")
)

total_noacct = float(range_summary["NOACCT"].sum() or 1)
total_accytd = float(range_summary["ACCYTD"].sum() or 0)
total_curbal = float(range_summary["CURBAL"].sum() or 0)
total_avgamt = float(range_summary["AVGAMT"].sum() or 0)

for row in range_summary.iter_rows(named=True):
    deprange = row.get("DEPRANGE") or ""
    noacct   = float(row.get("NOACCT") or 0)
    accytd   = float(row.get("ACCYTD") or 0)
    curbal   = float(row.get("CURBAL") or 0)
    avgamt   = float(row.get("AVGAMT") or 0)
    pct_n    = (noacct / total_noacct * 100) if total_noacct else 0
    pct_a    = (accytd / (total_accytd or 1) * 100)
    pct_c    = (curbal / (total_curbal or 1) * 100)
    avg_amt_per_acct = (curbal / noacct * 100) if noacct else 0  # PCTSUM<NOACCT>=HUNDRED.
    pct_avg  = (avgamt / (total_avgamt or 1) * 100)
    avg_bal_per_acct = (avgamt / noacct * 100) if noacct else 0

    line = (
        f"{deprange:<30}"
        f"{fmt_comma10(noacct)}{fmt_comma6_2(pct_n)}"
        f"{fmt_comma10(accytd)}{fmt_comma6_2(pct_a)}"
        f"{fmt_comma15_2(curbal)}{fmt_comma6_2(pct_c)}{fmt_comma15_2(avg_amt_per_acct)}"
        f"{fmt_comma15_2(avgamt)}{fmt_comma6_2(pct_avg)}{fmt_comma15_2(avg_bal_per_acct)}"
    )
    rpt.put(' ', line)

# TOTAL row
rpt.put(' ', "-" * len(hdr))
total_avg_per = (total_curbal / total_noacct * 100) if total_noacct else 0
total_avgbal  = (total_avgamt / total_noacct * 100) if total_noacct else 0
total_line = (
    f"{'TOTAL':<30}"
    f"{fmt_comma10(total_noacct)}{fmt_comma6_2(100.0)}"
    f"{fmt_comma10(total_accytd)}{fmt_comma6_2(100.0)}"
    f"{fmt_comma15_2(total_curbal)}{fmt_comma6_2(100.0)}{fmt_comma15_2(total_avg_per)}"
    f"{fmt_comma15_2(total_avgamt)}{fmt_comma6_2(100.0)}{fmt_comma15_2(total_avgbal)}"
)
rpt.put(' ', total_line)

rpt.write()
con.close()
print(f"Report written to: {OUTPUT_FILE}")
