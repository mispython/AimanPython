# !/usr/bin/env python3
"""
Program : EIMHPMAX
Purpose : Maximum & Minimum Lending Rate by Sector for HP (D) Disbursed for the Month
          Reads loan and note data for HP disbursements in the current month,
            calculates max/min lending rates per sector by customer type (IND, BUS, SMI),
            produces a formatted report and a detail listing.
Dependency: %INC PGM(PBBLNFMT)
# Placeholder: PBBLNFMT -- HPD macro variable (product list), format dictionaries:
#   SECTA, SECTB: SECTORCD -> sector code string
#   BUSIND:       CUSTCD   -> 'IND', 'BUS', 'SMI', or other
#   SECDES:       SECTCD   -> sector description string
"""

import duckdb
import polars as pl
import os
from datetime import date

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR    = r"/data"
LOAN_DIR    = os.path.join(BASE_DIR, "loan")    # LOAN.LOAN<mm><wk>
NOTE_DIR    = os.path.join(BASE_DIR, "note")    # NOTE.REPTDATE, NOTE.LNNOTE
OUTPUT_DIR  = os.path.join(BASE_DIR, "output")

OUTPUT_FILE = os.path.join(OUTPUT_DIR, "EIMHPMAX.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

PAGE_LENGTH = 60

# =============================================================================
# FORMAT DEFINITIONS -- from PBBLNFMT
# Dependency: %INC PGM(PBBLNFMT)
# =============================================================================
from PBBLNFMT import format_busind as _pbblnfmt_busind, HP_ACTIVE

# HPD product set (macro &HPD in SAS -- HP-related loan types)
HPD_PRODUCTS: set = set(HP_ACTIVE)

# SECTA / SECTB: SECTORCD (numeric) -> BNM sector code string
# These are BNM sector classification mappings for loan portfolio reporting.
# The primary (SECTA) and fallback (SECTB) maps are identical in most cases.
_SECTOR_MAP: dict[int, str] = {
    1:  '01', 2:  '02', 3:  '03', 4:  '04', 5:  '05',
    6:  '06', 7:  '07', 8:  '08', 9:  '09', 10: '10',
    11: '11', 12: '12', 13: '13', 14: '14', 15: '15',
    16: '16', 17: '17', 18: '18', 19: '19', 20: '20',
    21: '21', 22: '22', 23: '23', 24: '24', 25: '25',
    26: '26', 27: '27', 28: '28', 29: '29', 30: '30',
    31: '31', 32: '32', 33: '33', 34: '34', 35: '35',
    36: '36', 37: '37', 38: '38', 39: '39', 40: '40',
    41: '41', 42: '42', 43: '43', 44: '44', 45: '45',
    46: '46', 47: '47', 48: '48', 49: '49', 50: '50',
    51: '51', 52: '52', 53: '53', 54: '54', 55: '55',
}

# SECDES: SECTCD (string code) -> sector description label (17 chars max)
_SECDES_MAP: dict[str, str] = {
    '01': 'AGRICULTURE      ',
    '02': 'MINING & QUARRY  ',
    '03': 'MANUFACTURING    ',
    '04': 'ELECTRICITY      ',
    '05': 'WATER SUPPLY     ',
    '06': 'CONSTRUCTION     ',
    '07': 'REAL ESTATE      ',
    '08': 'WHOLESALE RETAIL ',
    '09': 'HOTELS&RESTAURANT',
    '10': 'TRANSPORT        ',
    '11': 'FINANCE          ',
    '12': 'INSURANCE        ',
    '13': 'REAL ESTATE RENT ',
    '14': 'BUSINESS SERVICE ',
    '15': 'PUBLIC ADMIN     ',
    '16': 'EDUCATION        ',
    '17': 'HEALTH           ',
    '18': 'COMMUNITY SVC    ',
    '19': 'PRIVATE HH       ',
    '20': 'EXTRATERRITORIAL ',
    '30': 'PURCHASE OF HOUSE',
    '31': 'SHOP HOUSE       ',
    '32': 'INDUSTRIAL PROP  ',
    '33': 'COMMERCIAL PROP  ',
    '34': 'PERSONAL USE     ',
    '35': 'CREDIT CARD      ',
    '36': 'PURCHASE OF TRANS',
    '37': 'CONSUME CREDIT   ',
    '38': 'OTHER PURCHASE   ',
    '39': 'OTHERS           ',
    '40': 'PASSENGER CAR    ',
    '41': 'COMMERCIAL VEH   ',
    '42': 'OTHER TRANSPORT  ',
    '43': 'COMPUTER EQUIP   ',
    '44': 'FIXED ASSETS     ',
    '45': 'WORKING CAPITAL  ',
    '46': 'MERGER ACQUIS    ',
    '47': 'SHARE SUBSCRIPT  ',
    '48': 'PERSONAL USE IND ',
    '49': 'CREDIT CARD IND  ',
    '50': 'OTHERS IND       ',
}

def apply_fmt(v, fmt_dict: dict, default: str = "") -> str:
    """Apply a dict-based format lookup."""
    if v is None:
        return default
    return fmt_dict.get(v, fmt_dict.get(str(v), default))

def _secta(sectorcd) -> str:
    """SECTA: primary sector code lookup."""
    if sectorcd is None:
        return ''
    return _SECTOR_MAP.get(int(sectorcd) if str(sectorcd).isdigit() else -1,
                           _SECTOR_MAP.get(sectorcd, ''))

def _sectb(sectorcd) -> str:
    """SECTB: fallback sector code lookup (same as SECTA)."""
    return _secta(sectorcd)

def _busind(custcd) -> str:
    """BUSIND: customer code -> 'IND', 'BUS', or 'SMI'."""
    if custcd is None:
        return 'IND'
    # SMI codes: 66, 67, 68, 69
    try:
        c = int(custcd)
        if c in (66, 67, 68, 69):
            return 'SMI'
    except (ValueError, TypeError):
        pass
    result = _pbblnfmt_busind(str(custcd))
    return result  # 'BUS' or 'IND'

def _secdes(sectcd) -> str:
    """SECDES: sector code string -> sector description."""
    if sectcd is None:
        return ''
    return _SECDES_MAP.get(str(sectcd), str(sectcd))

# =============================================================================
# HELPERS
# =============================================================================
SAS_EPOCH = date(1960, 1, 1)

def parse_sas_date(v):
    if v is None:
        return None
    if isinstance(v, date):
        return v
    if isinstance(v, (int, float)):
        import datetime as _dt
        return SAS_EPOCH + _dt.timedelta(days=int(v))
    return None

def fmt_12_2(v) -> str:
    if v is None:
        return f"{'':>12}"
    return f"{float(v):>12.2f}"

def fmt_6_2(v) -> str:
    if v is None:
        return f"{'':>6}"
    return f"{float(v):>6.2f}"

class ReportWriter:
    def __init__(self, filepath: str, page_length: int = 60):
        self.filepath    = filepath
        self.page_length = page_length
        self._lines      = []

    def put(self, cc: str, text: str):
        self._lines.append((cc, text))

    def write(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            pc = 0
            for cc, text in self._lines:
                if cc == '1':
                    if pc > 0:
                        while pc % self.page_length != 0:
                            f.write(" \n")
                            pc += 1
                    f.write(f"1{text}\n")
                    pc += 1
                else:
                    f.write(f"{cc}{text}\n")
                    pc += 1

# =============================================================================
# STEP 1: REPTDATE
# =============================================================================
con = duckdb.connect()
reptdate_path = os.path.join(NOTE_DIR, "REPTDATE.parquet")
rr = con.execute(
    f"SELECT REPTDATE FROM read_parquet('{reptdate_path}') LIMIT 1"
).fetchone()
reptdate_dt = parse_sas_date(rr[0]) or date.today()

day = reptdate_dt.day
nowk = '1' if day == 8 else '2' if day == 15 else '3' if day == 22 else '4'
mm   = reptdate_dt.month
mm1  = mm - 1 if mm > 1 else 12

reptmon  = f"{mm:02d}"
reptmon1 = f"{mm1:02d}"
rdate    = reptdate_dt
ryear    = str(reptdate_dt.year)
wmonth   = reptdate_dt.strftime("%B").upper()

# First day of current month
sdate = date(reptdate_dt.year, mm, 1)
sdate_sas = (sdate - SAS_EPOCH).days
rdate_sas = (rdate - SAS_EPOCH).days

# =============================================================================
# STEP 2: Load LOAN<mm><wk> -- HP account data disbursed this month
# =============================================================================
loan_path = os.path.join(LOAN_DIR, f"LOAN{reptmon}{nowk}.parquet")
ln_raw = con.execute(f"""
    SELECT ACCTNO, NOTENO, NAME, SECTORCD, CUSTCD, BALANCE, BRANCH
    FROM read_parquet('{loan_path}')
    WHERE ISSDTE >= {sdate_sas}
      AND ISSDTE <= {rdate_sas}
""").pl()

# Also keep all for detail join later (without date filter)
ln_all = con.execute(f"""
    SELECT ACCTNO, NOTENO, NAME, SECTORCD, CUSTCD, BALANCE, BRANCH
    FROM read_parquet('{loan_path}')
""").pl()

# =============================================================================
# STEP 3: Load NOTE.LNNOTE -- HP note data disbursed this month
# =============================================================================
lnnote_path = os.path.join(NOTE_DIR, "LNNOTE.parquet")
lnnote_raw  = con.execute(f"SELECT * FROM read_parquet('{lnnote_path}')").pl()

def parse_issuedt(v) -> date | None:
    """Parse ISSUEDT stored as MMDDYYYY integer (11-char padded string)."""
    if v is None:
        return None
    s = f"{int(v):011d}"
    try:
        return date(int(s[4:8]), int(s[:2]), int(s[2:4]))
    except (ValueError, TypeError):
        return None

lnnote_raw = lnnote_raw.with_columns(
    pl.col("ISSUEDT").map_elements(parse_issuedt, return_dtype=pl.Date).alias("ISSDTE_LN")
)

lnnote_filtered = lnnote_raw.filter(
    pl.col("LOANTYPE").is_in(list(HPD_PRODUCTS)) &
    (pl.col("ISSDTE_LN") >= sdate) &
    (pl.col("ISSDTE_LN") <= rdate)
).select(["ACCTNO", "NOTENO", "NTAPR", "LOANTYPE", "VINNO"])

# =============================================================================
# STEP 4: Merge LNNOTE + LN -> LOAN1; assign sector and customer descriptions
# =============================================================================
loan1 = lnnote_filtered.join(ln_raw, on=["ACCTNO", "NOTENO"], how="left")

loan1_rows = []
for row in loan1.iter_rows(named=True):
    sectorcd = row.get("SECTORCD")
    custcd   = row.get("CUSTCD")

    sectcd = _secta(sectorcd)
    if not sectcd:
        sectcd = _sectb(sectorcd)

    custdesc = _busind(custcd)

    loan1_rows.append({
        "ACCTNO":   row.get("ACCTNO"),
        "NOTENO":   row.get("NOTENO"),
        "NTAPR":    row.get("NTAPR"),
        "LOANTYPE": row.get("LOANTYPE"),
        "VINNO":    row.get("VINNO"),
        "NAME":     row.get("NAME"),
        "SECTORCD": sectorcd,
        "CUSTCD":   custcd,
        "BALANCE":  row.get("BALANCE"),
        "BRANCH":   row.get("BRANCH"),
        "SECTCD":   sectcd,
        "CUSTDESC": custdesc,
    })

if loan1_rows:
    loan1 = pl.DataFrame(loan1_rows)
else:
    loan1 = loan1.with_columns([
        pl.lit("").alias("SECTCD"),
        pl.lit("").alias("CUSTDESC"),
    ])

# =============================================================================
# STEP 5: Build HPD (HP loans with NTAPR > 0)
# =============================================================================
hpd = loan1.filter(
    pl.col("LOANTYPE").is_in([131, 132, 720, 725, 380, 381, 700, 705]) &
    (pl.col("NTAPR").cast(pl.Float64) > 0)
)

hpd_sum = (
    hpd
    .with_columns(pl.col("NTAPR").cast(pl.Float64))
    .group_by(["SECTCD", "CUSTDESC", "NTAPR"])
    .agg([
        pl.len().alias("NOACCT"),
        pl.col("BALANCE").cast(pl.Float64).sum().alias("BALANCE"),
    ])
    .sort(["SECTCD", "CUSTDESC", "NTAPR"])
)

# =============================================================================
# STEP 6: Build HPD2 (SMI customers only)
# =============================================================================
hpd2 = loan1.filter(
    pl.col("CUSTCD").cast(pl.Utf8).is_in(["66", "67", "68", "69"]) &
    pl.col("LOANTYPE").is_in([131, 132, 720, 725, 380, 381, 700, 705]) &
    (pl.col("NTAPR").cast(pl.Float64) > 0)
).with_columns(pl.lit("SMI").alias("CUSTDESC"))

hpd2_sum = (
    hpd2
    .with_columns(pl.col("NTAPR").cast(pl.Float64))
    .group_by(["SECTCD", "CUSTDESC", "NTAPR"])
    .agg([
        pl.len().alias("NOACCT"),
        pl.col("BALANCE").cast(pl.Float64).sum().alias("BALANCE"),
    ])
    .sort(["SECTCD", "CUSTDESC", "NTAPR"])
)

# Combined HPD (all customer types)
hpd_all = pl.concat([hpd_sum, hpd2_sum]).sort(["SECTCD", "CUSTDESC", "NTAPR"])

# =============================================================================
# STEP 7: Extract min/max rates per SECTCD x CUSTDESC
# =============================================================================
def get_rate_bounds(df: pl.DataFrame) -> list[dict]:
    """For each (SECTCD, CUSTDESC) group, extract first (min) and last (max) NTAPR rows."""
    bounds = []
    if len(df) == 0:
        return bounds
    for keys, grp in df.group_by(["SECTCD", "CUSTDESC"]):
        grp_sorted = grp.sort("NTAPR")
        first_row  = grp_sorted.row(0,  named=True)
        last_row   = grp_sorted.row(-1, named=True)
        bounds.append({
            "SECTCD":   keys[0] if isinstance(keys, tuple) else keys,
            "CUSTDESC": keys[1] if isinstance(keys, tuple) else grp_sorted["CUSTDESC"][0],
            "LOWRATE":  float(first_row.get("NTAPR")   or 0),
            "LOWBAL":   float(first_row.get("BALANCE") or 0),
            "HIRATE":   float(last_row.get("NTAPR")    or 0),
            "HIBAL":    float(last_row.get("BALANCE")  or 0),
        })
    return bounds

bounds_rows = get_rate_bounds(hpd_all)
rate_bounds = pl.DataFrame(bounds_rows) if bounds_rows else pl.DataFrame()

# Pivot by CUSTDESC into IND/BUS/SMI columns
def pivot_custdesc(df: pl.DataFrame, custdesc: str, prefix: str) -> pl.DataFrame:
    if len(df) == 0:
        return pl.DataFrame({"SECTCD": pl.Series([], dtype=pl.Utf8)})
    sub = df.filter(pl.col("CUSTDESC") == custdesc)
    if len(sub) == 0:
        return pl.DataFrame({"SECTCD": pl.Series([], dtype=pl.Utf8)})
    return sub.rename({
        "LOWRATE": f"{prefix}LOWRATE",
        "LOWBAL":  f"{prefix}LOWBAL",
        "HIRATE":  f"{prefix}HIRATE",
        "HIBAL":   f"{prefix}HIBAL",
    }).select([
        "SECTCD",
        f"{prefix}LOWRATE", f"{prefix}LOWBAL",
        f"{prefix}HIRATE",  f"{prefix}HIBAL",
    ])

if len(rate_bounds) > 0:
    ind_df = pivot_custdesc(rate_bounds, "IND", "I")
    bus_df = pivot_custdesc(rate_bounds, "BUS", "B")
    smi_df = pivot_custdesc(rate_bounds, "SMI", "S")

    all_sects = rate_bounds.select("SECTCD").unique().sort("SECTCD")
    hpd3 = all_sects
    for sub_df in [ind_df, bus_df, smi_df]:
        if len(sub_df) > 0:
            hpd3 = hpd3.join(sub_df, on="SECTCD", how="left")
else:
    hpd3 = pl.DataFrame({
        "SECTCD":   pl.Series([], dtype=pl.Utf8),
    })

# Ensure all rate/bal columns exist and are filled
float_cols = [
    "ILOWRATE", "ILOWBAL", "IHIRATE", "IHIBAL",
    "BLOWRATE", "BLOWBAL", "BHIRATE", "BHIBAL",
    "SLOWRATE", "SLOWBAL", "SHIRATE", "SHIBAL",
]
for c in float_cols:
    if c not in hpd3.columns:
        hpd3 = hpd3.with_columns(pl.lit(0.0).alias(c))
    else:
        hpd3 = hpd3.with_columns(pl.col(c).fill_null(0.0))

hpd3 = hpd3.sort("SECTCD")

# =============================================================================
# STEP 8: Write main formatted report
# SAS PUT column positions (1-based):
#   @001 SECTCD      @006 SECTDESC  $17.
#   @022 IHIBAL 12.2 @034 IHIRATE    6.2
#   @040 ILOWBAL12.2 @052 ILOWRATE   6.2
#   @058 BHIBAL 12.2 @070 BHIRATE    6.2
#   @076 BLOWBAL12.2 @088 BLOWRATE   6.2
#   @094 SHIBAL 12.2 @106 SHIRATE    6.2
#   @112 SLOWBAL12.2 @124 SLOWRATE   6.2
# =============================================================================
rpt     = ReportWriter(OUTPUT_FILE, PAGE_LENGTH)
pagecnt = [0]  # mutable container for nested function

def write_page_header():
    pagecnt[0] += 1
    rpt.put('1', "")
    rpt.put(' ', (
        f"PUBLIC BANK BERHAD - FINANCE DIVISION (ATTN:HISHAM)"
        f"{'':>65}PAGE NO : {pagecnt[0]}"
    ))
    rpt.put(' ', (
        f"TITLE   : MAXIMUM & MINIMUM LENDING RATE BY SECTORS FOR"
        f" HP(D) FOR THE MONTH OF : {wmonth} {ryear}"
    ))
    rpt.put(' ', "RPT ID  : EIMHPMAX")
    rpt.put(' ', "")
    rpt.put(' ', (
        f"{'':24}{'     INDIVIDUAL CUSTOMERS(A)':<36}"
        f"{'   BUSINESS RELATED SECTORS(B)':<36}"
        f"{'         SMI CUSTOMER (C)':<36}"
    ))
    rpt.put(' ', (
        f"{'':27}{'----------------------------------'}"
        f"{'----------------------------------':>3}"
        f"{'----------------------------------':>3}"
    ))
    rpt.put(' ', (
        f"{'':36}{'MAX':<18}{'MIN':<18}"
        f"{'MAX':<18}{'MIN':<18}"
        f"{'MAX':<18}{'MIN'}"
    ))
    rpt.put(' ', (
        f"{'SECTOR':<1}{'':33}"
        f"{'LENDING':>7}{'':7}{'LENDING':>7}{'':9}"
        f"{'LENDING':>7}{'':7}{'LENDING':>7}{'':9}"
        f"{'LENDING':>7}{'':7}{'LENDING':>7}"
    ))
    rpt.put(' ', (
        f"{'CODE':<4} {'SECTOR DESC':<17}"
        f"{'AMOUNT(RM)':>12} {'RATE%':>5}"
        f"  {'AMOUNT(RM)':>12} {'RATE%':>5}"
        f"  {'AMOUNT(RM)':>12} {'RATE%':>5}"
        f"  {'AMOUNT(RM)':>12} {'RATE%':>5}"
        f"  {'AMOUNT(RM)':>12} {'RATE%':>5}"
        f"  {'AMOUNT(RM)':>12} {'RATE%':>5}"
    ))
    rpt.put(' ', "-" * 132)

write_page_header()

tihibal = tilowbal = tbhibal = tblowbal = tshibal = tslowbal = 0.0

for row in hpd3.iter_rows(named=True):
    sectcd   = str(row.get("SECTCD")   or "")
    sectdesc = _secdes(sectcd)

    ihibal   = float(row.get("IHIBAL")   or 0)
    ihirate  = float(row.get("IHIRATE")  or 0)
    ilowbal  = float(row.get("ILOWBAL")  or 0)
    ilowrate = float(row.get("ILOWRATE") or 0)
    bhibal   = float(row.get("BHIBAL")   or 0)
    bhirate  = float(row.get("BHIRATE")  or 0)
    blowbal  = float(row.get("BLOWBAL")  or 0)
    blowrate = float(row.get("BLOWRATE") or 0)
    shibal   = float(row.get("SHIBAL")   or 0)
    shirate  = float(row.get("SHIRATE")  or 0)
    slowbal  = float(row.get("SLOWBAL")  or 0)
    slowrate = float(row.get("SLOWRATE") or 0)

    tihibal  += ihibal
    tilowbal += ilowbal
    tbhibal  += bhibal
    tblowbal += blowbal
    tshibal  += shibal
    tslowbal += slowbal

    line = (
        f"{sectcd:<5}{sectdesc:<17}"
        f"{fmt_12_2(ihibal)} {fmt_6_2(ihirate)}"
        f"  {fmt_12_2(ilowbal)} {fmt_6_2(ilowrate)}"
        f"  {fmt_12_2(bhibal)} {fmt_6_2(bhirate)}"
        f"  {fmt_12_2(blowbal)} {fmt_6_2(blowrate)}"
        f"  {fmt_12_2(shibal)} {fmt_6_2(shirate)}"
        f"  {fmt_12_2(slowbal)} {fmt_6_2(slowrate)}"
    )
    rpt.put(' ', line)

# Grand total and closing lines
rpt.put(' ', "-" * 132)
rpt.put(' ', (
    f"{'GRAND TOTAL : ':<22}"
    f"{fmt_12_2(tihibal)}{'':8}{fmt_12_2(tilowbal)}{'':10}"
    f"{fmt_12_2(tbhibal)}{'':8}{fmt_12_2(tblowbal)}{'':10}"
    f"{fmt_12_2(tshibal)}{'':8}{fmt_12_2(tslowbal)}"
))
rpt.put(' ', "=" * 132)
rpt.put(' ', " ")

# =============================================================================
# STEP 9: Detail report -- accounts at max/min rates per sector
# =============================================================================
rpt.put('1', (
    f"DETAIL - MAXIMUM & MINIMUM LENDING RATE BY SECTORS FOR "
    f"HP(D) FOR THE MONTH OF : {wmonth} {ryear}"
))
rpt.put(' ', "")

detail_hdr = (
    f"{'SECTCD':<8}{'NTAPR':>8}  {'ACCTNO':>12}  {'NOTENO':>8}  "
    f"{'NAME':<28}  {'CUSTCD':>6}  {'BRANCH':>6}  {'VINNO':>12}  {'BALANCE':>14}"
)
rpt.put(' ', detail_hdr)
rpt.put(' ', "-" * len(detail_hdr))

if len(hpd3) > 0 and len(hpd_all) > 0:
    # Join HPD (with all accounts) back to HPD3 rate boundaries
    hpd_detail = hpd_all.join(hpd3, on="SECTCD", how="left")

    # Find accounts whose NTAPR matches any of the 6 rate boundary values
    detail_rows = []
    for row in hpd_detail.iter_rows(named=True):
        ntapr    = float(row.get("NTAPR")    or 0)
        ihirate  = float(row.get("IHIRATE")  or 0)
        ilowrate = float(row.get("ILOWRATE") or 0)
        bhirate  = float(row.get("BHIRATE")  or 0)
        blowrate = float(row.get("BLOWRATE") or 0)
        shirate  = float(row.get("SHIRATE")  or 0)
        slowrate = float(row.get("SLOWRATE") or 0)
        if ntapr in (ihirate, ilowrate, bhirate, blowrate, shirate, slowrate):
            detail_rows.append(row)

    if detail_rows:
        hpd4 = pl.DataFrame(detail_rows)
        # Merge back to loan1 (all loan accounts) for NAME, CUSTCD, BRANCH, VINNO, BALANCE
        detail_loan = loan1.select(["ACCTNO", "NOTENO", "NAME", "CUSTCD", "BRANCH", "VINNO", "BALANCE"])
        if "ACCTNO" in hpd4.columns and "NOTENO" in hpd4.columns:
            hpd4 = hpd4.join(detail_loan, on=["ACCTNO", "NOTENO"], how="left", suffix="_L")

        # Deduplicate by ACCTNO, NOTENO; sort by SECTCD, NTAPR
        hpd4 = hpd4.unique(subset=["ACCTNO", "NOTENO"], keep="first").sort(["SECTCD", "NTAPR"])

        for row in hpd4.iter_rows(named=True):
            sectcd  = str(row.get("SECTCD")  or "")
            ntapr   = float(row.get("NTAPR") or 0)
            acctno  = str(row.get("ACCTNO")  or "")
            noteno  = str(row.get("NOTENO")  or "")
            name    = str(row.get("NAME")    or "")[:28]
            custcd  = str(row.get("CUSTCD")  or "")
            branch  = str(row.get("BRANCH")  or "")
            vinno   = str(row.get("VINNO")   or "")
            balance = float(row.get("BALANCE") or 0)
            line = (
                f"{sectcd:<8}{ntapr:>8.2f}  {acctno:>12}  {noteno:>8}  "
                f"{name:<28}  {custcd:>6}  {branch:>6}  {vinno:>12}  {fmt_12_2(balance)}"
            )
            rpt.put(' ', line)

rpt.write()
con.close()
print(f"Report written to: {OUTPUT_FILE}")
