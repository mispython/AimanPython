"""
SAS to Python Conversion: Islamic Bank Current Account Opened/Closed Report
Generates monthly report on current account activity with cumulative tracking.
Produces two outputs:
  - MIS/CURRCmm.parquet  : detail extract of accounts closed this month
  - MIS/CURRFmm.parquet  : branch x product summary with YTD cumulative columns
  - CURRENT_ACCOUNT_REPORT.txt : PROC TABULATE equivalent with ASA carriage control
"""

import duckdb
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path

# ============================================================================
# CONFIGURATION – all paths declared up front
# ============================================================================

# --- Input paths -----------------------------------------------------------
INPUT_REPTDATE       = "data/DEPOSIT/REPTDATE.parquet"
INPUT_CURRENT        = "data/DEPOSIT/CURRENT.parquet"
INPUT_CURRF_TEMPLATE = "data/MIS/CURRF{mon}.parquet"       # previous-month cumulative
FORMAT_DDCUSTCD      = "data/formats/DDCUSTCD.parquet"     # CUSTCODE -> CUSTFISS map

# --- Output paths ----------------------------------------------------------
OUTPUT_MIS_DIR       = Path("data/MIS")
OUTPUT_MIS_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_REPORT_DIR    = Path("output")
OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_CURRC_TEMPLATE = OUTPUT_MIS_DIR  / "CURRC{mon}.parquet"
OUTPUT_CURRF_TEMPLATE = OUTPUT_MIS_DIR  / "CURRF{mon}.parquet"
OUTPUT_REPORT         = OUTPUT_REPORT_DIR / "CURRENT_ACCOUNT_REPORT.txt"

# --- Report constants ------------------------------------------------------
PAGE_LENGTH = 60
ASA_NEW_PAGE = '1'
ASA_SPACE    = ' '

# ============================================================================
# PRODUCT FILTER – pre-built frozen set for O(1) membership test
# ============================================================================

_INDIVIDUAL_CODES = {
    21, 32, 33, 70, 71, 73,
    90, 91, 92, 93, 94, 95, 96, 97,
    137, 138, 74, 23, 24, 25,
    46, 47, 48, 49, 75, 76, 45,
    13, 14, 15, 16, 17, 18, 19,
    7, 8, 22, 5, 6, 81, 20,
}

VALID_PRODUCTS = frozenset(
    _INDIVIDUAL_CODES
    | set(range(50,  68))   # 050-067
    | set(range(100, 107))  # 100-106
    | set(range(108, 126))  # 108-125
    | set(range(150, 171))  # 150-170
    | set(range(174, 189))  # 174-188
    | set(range(191, 199))  # 191-198
)

# CUSTFISS codes that keep DNBFISME unchanged
_DNBFISME_RETAIN = frozenset({
    '04','05','06','30','31','32','33','34',
    '35','37','38','39','40','45'
})

# ============================================================================
# HELPER – load DDCUSTCD format mapping
# ============================================================================

def _load_ddcustcd() -> dict:
    """Return {CUSTCODE (int) -> CUSTFISS (str)} from the format parquet."""
    path = Path(FORMAT_DDCUSTCD)
    if not path.exists():
        print("  Warning: DDCUSTCD format file not found – using str(CUSTCODE) fallback.")
        return {}
    df = duckdb.execute(f"SELECT * FROM '{path}'").pl()
    return dict(zip(df['CUSTCODE'].to_list(), df['CUSTFISS'].to_list()))

DDCUSTCD_MAP: dict = _load_ddcustcd()


def derive_custfiss(product: int, custcode) -> str:
    """Reproduce the SAS SELECT(PRODUCT) / OTHERWISE logic."""
    if product == 104:
        return '02'
    if product == 105:
        return '81'
    # OTHERWISE: PUT(CUSTCODE, DDCUSTCD.)
    return DDCUSTCD_MAP.get(int(custcode), str(custcode)) if custcode is not None else ''

# ============================================================================
# HELPER – numeric-to-date parsers (match SAS INPUT/SUBSTR behaviour)
# ============================================================================

def _parse_lasttran(val) -> object | None:
    """LASTTRAN stored as integer; first 6 digits are MMDDYY."""
    if val is None or val == 0:
        return None
    try:
        s = str(int(val)).zfill(9)[:6]          # Z9. then SUBSTR 1,6
        return datetime.strptime(s, '%m%d%y')
    except (ValueError, OverflowError):
        return None


def _parse_bdate(val) -> object | None:
    """BDATE stored as integer; first 8 digits are MMDDYYYY."""
    if val is None or val == 0:
        return None
    try:
        s = str(int(val)).zfill(11)[:8]         # Z11. then SUBSTR 1,8
        return datetime.strptime(s, '%m%d%Y')
    except (ValueError, OverflowError):
        return None

# ============================================================================
# STEP 1 – Read REPTDATE, derive macro-variable equivalents
# ============================================================================

print("Step 1: Reading REPTDATE …")

con = duckdb.connect()

reptdate_row = con.execute(f"SELECT REPTDATE FROM '{INPUT_REPTDATE}' LIMIT 1").fetchone()[0]

# SAS date = days since 1960-01-01
if isinstance(reptdate_row, (int, float)):
    reptdate_dt = datetime(1960, 1, 1) + timedelta(days=int(reptdate_row))
else:
    reptdate_dt = reptdate_row

MM      = reptdate_dt.month
MM1     = MM - 1 if MM > 1 else 12
RDATE   = reptdate_dt.strftime('%d/%m/%y')
RYEAR   = reptdate_dt.year
RMONTH  = reptdate_dt.month
REPTMON = f"{MM:02d}"
REPTMON1= f"{MM1:02d}"
RDAY    = reptdate_dt.day

print(f"  Reporting date : {RDATE}   month={REPTMON}   prev={REPTMON1}")

# ============================================================================
# STEP 2 – Load CURRENT, apply branch remaps + product filter
# ============================================================================

print("\nStep 2: Loading CURRENT …")

# DuckDB filters rows early; product membership tested in Python afterwards.
curr_raw = con.execute(f"SELECT * FROM '{INPUT_CURRENT}'").pl()
print(f"  Raw rows: {len(curr_raw)}")

# --- branch remaps (vectorised) ---
curr_raw = curr_raw.with_columns(
    pl.col('BRANCH')
      .replace({996: 168, 250: 92})
      .alias('BRANCH')
)

# --- product filter (vectorised) ---
valid_set = VALID_PRODUCTS
curr_raw = curr_raw.filter(pl.col('PRODUCT').is_in(list(valid_set)))
print(f"  After product filter: {len(curr_raw)}")

# --- OPENIND='Z' -> 'O' + CLOSEMH=0 (vectorised) ---
curr_raw = curr_raw.with_columns(
    pl.when(pl.col('OPENIND') == 'Z')
      .then(pl.lit('O'))
      .otherwise(pl.col('OPENIND'))
      .alias('OPENIND'),

    pl.when(pl.col('OPENIND') == 'Z')
      .then(pl.lit(0))
      .otherwise(pl.col('CLOSEMH'))
      .alias('CLOSEMH'),
)

# --- Retention filter: OPENIND='O'  OR  (OPENIND in B/C/P AND CLOSEMH=1) ---
curr_raw = curr_raw.filter(
    (pl.col('OPENIND') == 'O')
    | (pl.col('OPENIND').is_in(['B','C','P']) & (pl.col('CLOSEMH') == 1))
)
print(f"  After retention filter: {len(curr_raw)}")

# --- Derive NOACCT / BCLOSE / CCLOSE (vectorised) ---
curr_raw = curr_raw.with_columns(
    # NOACCT = 1 only when OPENIND is NOT in B/C/P  (i.e. it is 'O')
    pl.when(pl.col('OPENIND').is_in(['B','C','P']))
      .then(pl.lit(0))
      .otherwise(pl.lit(1))
      .alias('NOACCT'),

    # CCLOSE = CLOSEMH only when OPENIND='C'
    pl.when(pl.col('OPENIND') == 'C')
      .then(pl.col('CLOSEMH'))
      .otherwise(pl.lit(0))
      .alias('CCLOSE'),

    # BCLOSE = CLOSEMH when OPENIND in ('B','P')
    pl.when(pl.col('OPENIND').is_in(['B','P']))
      .then(pl.col('CLOSEMH'))
      .otherwise(pl.lit(0))
      .alias('BCLOSE'),
)

# ============================================================================
# STEP 3 – Extract CURRC (closed-account detail this month)
# ============================================================================

print("\nStep 3: Building CURRC closed-account detail …")

currc_detail = curr_raw.filter(
    pl.col('OPENIND').is_in(['B','C','P']) & (pl.col('CLOSEMH') == 1)
)

# Derive columns that require row-level logic – drop to pandas once only
currc_pd = currc_detail.to_pandas()

currc_pd['YTDAVBAL']  = currc_pd['YTDAVAMT']
currc_pd['DNBFI_ORI'] = currc_pd['DNBFISME']

# CUSTFISS via vectorized .apply (row-level SELECT logic)
currc_pd['CUSTFISS'] = currc_pd.apply(
    lambda r: derive_custfiss(int(r['PRODUCT']), r['CUSTCODE']), axis=1
)

# Conditional DNBFISME reset
currc_pd.loc[~currc_pd['CUSTFISS'].isin(_DNBFISME_RETAIN), 'DNBFISME'] = '0'

# Date parsing
currc_pd['LASTTRAN'] = currc_pd['LASTTRAN'].apply(_parse_lasttran)
currc_pd['DOBMNI']   = currc_pd['BDATE'].apply(_parse_bdate)

currc_df = pl.from_pandas(currc_pd)

# Keep only the columns specified in the SAS KEEP statement
CURRC_KEEP = [
    'BRANCH','ACCTNO','OPENDT','CLOSEDT','OPENIND','CURBAL','CUSTCODE',
    'MTDAVBAL','YTDAVBAL','CUSTFISS','DOBMNI','PRODUCT','ACCPROF','APPRLIMT',
    'CENSUST','CHGIND','COSTCTR','DATE_LST_DEP','DEPTYPE','DNBFI_ORI','DNBFISME',
    'FLATRATE','INTPD','INTPLAN','INTRSTPD','INTYTD','L_DEP','LASTTRAN','LIMIT1',
    'LIMIT2','LIMIT3','LIMIT4','LIMIT5','MAXPROF','ODINTACC','ODINTCHR','ODPLAN',
    'ORGCODE','ORGTYPE','PURPOSE','RATE1','RATE2','RATE3','RATE4','RATE5','RETURNS_Y',
    'RISKCODE','SECOND','SECTOR','SERVICE','STATE','USER2','USER3','USER5',
    'DSR','REPAY_TYPE_CD','MTD_REPAID_AMT','INDUSTRIAL_SECTOR_CD',
    'WRITE_DOWN_BAL','MTD_DISBURSED_AMT','MTD_REPAY_TYPE10_AMT',
    'MTD_REPAY_TYPE20_AMT','MTD_REPAY_TYPE30_AMT','MODIFIED_FACILITY_IND',
    'STMT_CYCLE','NXT_STMT_CYCLE_DT','INTPLAN_IBCA',
]

currc_df = currc_df.select([c for c in CURRC_KEEP if c in currc_df.columns])

currc_path = str(OUTPUT_CURRC_TEMPLATE).format(mon=REPTMON)
currc_df.write_parquet(currc_path)
print(f"  CURRC rows: {len(currc_df)}  →  {currc_path}")

# ============================================================================
# STEP 4 – PROC SUMMARY: aggregate by BRANCH + PRODUCT
# ============================================================================

print("\nStep 4: Aggregating by BRANCH × PRODUCT …")

curr_summary = curr_raw.group_by(['BRANCH', 'PRODUCT']).agg([
    pl.col('OPENMH').sum().alias('OPENMH'),
    pl.col('CLOSEMH').sum().alias('CLOSEMH'),
    pl.col('NOACCT').sum().alias('NOACCT'),
    pl.col('CURBAL').sum().alias('CURBAL'),
    pl.col('BCLOSE').sum().alias('BCLOSE'),
    pl.col('CCLOSE').sum().alias('CCLOSE'),
])

print(f"  Summary rows: {len(curr_summary)}")

# ============================================================================
# STEP 5 – %PROCESS macro: cumulative merge or January initialisation
# ============================================================================

print("\nStep 5: Cumulative processing …")

if int(REPTMON) > 1:
    prev_path = str(INPUT_CURRF_TEMPLATE).format(mon=REPTMON1)
    print(f"  Loading previous month: {prev_path}")

    try:
        currp = con.execute(f"SELECT * FROM '{prev_path}'").pl()

        # Rename cumulative cols to temporary names
        currp = currp.rename({'OPENCUM': 'OPENCUX', 'CLOSECUM': 'CLOSECUX'})

        # Branch remap for previous-month data (only 250->092)
        currp = currp.with_columns(
            pl.col('BRANCH').replace({250: 92}).alias('BRANCH')
        )

        # Re-aggregate previous month (in case source had duplicates)
        currp = currp.group_by(['BRANCH', 'PRODUCT']).agg([
            pl.col('OPENCUX').sum().alias('OPENCUX'),
            pl.col('CLOSECUX').sum().alias('CLOSECUX'),
        ])

        # Outer join: previous cumulative LEFT JOIN current month
        curr_merged = currp.join(curr_summary, on=['BRANCH','PRODUCT'], how='outer')

        # Fill all nulls with 0
        fill_cols = ['OPENCUX','CLOSECUX','OPENMH','CLOSEMH',
                     'NOACCT','CURBAL','BCLOSE','CCLOSE']
        curr_merged = curr_merged.with_columns([
            pl.col(c).fill_null(0) for c in fill_cols
        ])

        # Derive cumulative and net-change columns
        curr_final = curr_merged.with_columns([
            (pl.col('OPENMH')   + pl.col('OPENCUX') ).alias('OPENCUM'),
            (pl.col('CLOSEMH')  + pl.col('CLOSECUX')).alias('CLOSECUM'),
            (pl.col('OPENMH')   - pl.col('CLOSEMH') ).alias('NETCHGMH'),
        ]).with_columns(
            (pl.col('OPENCUM')  - pl.col('CLOSECUM')).alias('NETCHGYR'),
        )

    except Exception as exc:
        print(f"  Warning: could not load previous month ({exc}). Falling back to January path.")
        curr_final = curr_summary.with_columns([
            pl.col('OPENMH').alias('OPENCUM'),
            pl.col('CLOSEMH').alias('CLOSECUM'),
            (pl.col('OPENMH')  - pl.col('CLOSEMH')).alias('NETCHGMH'),
            (pl.col('OPENMH')  - pl.col('CLOSEMH')).alias('NETCHGYR'),
        ])
else:
    print("  January – initialising cumulative columns from monthly values.")
    # January path also applies the 250->092 remap (present in the ELSE branch of the macro)
    curr_summary = curr_summary.with_columns(
        pl.col('BRANCH').replace({250: 92}).alias('BRANCH')
    )
    curr_final = curr_summary.with_columns([
        pl.col('OPENMH').alias('OPENCUM'),
        pl.col('CLOSEMH').alias('CLOSECUM'),
        (pl.col('OPENMH')  - pl.col('CLOSEMH')).alias('NETCHGMH'),
        (pl.col('OPENMH')  - pl.col('CLOSEMH')).alias('NETCHGYR'),
    ])

# ============================================================================
# STEP 6 – Save CURRF (cumulative summary for next month)
# ============================================================================

currf_path = str(OUTPUT_CURRF_TEMPLATE).format(mon=REPTMON)
curr_final.write_parquet(currf_path)
print(f"\nStep 6: CURRF saved  →  {currf_path}  ({len(curr_final)} rows)")

# ============================================================================
# STEP 7 – Aggregate by BRANCH for the report
# ============================================================================

print("\nStep 7: Aggregating by BRANCH for report …")

report_data = curr_final.group_by('BRANCH').agg([
    pl.col('OPENMH').sum().alias('OPENMH'),
    pl.col('OPENCUM').sum().alias('OPENCUM'),
    pl.col('CLOSEMH').sum().alias('CLOSEMH'),
    pl.col('BCLOSE').sum().alias('BCLOSE'),
    pl.col('CCLOSE').sum().alias('CCLOSE'),
    pl.col('CLOSECUM').sum().alias('CLOSECUM'),
    pl.col('NOACCT').sum().alias('NOACCT'),
    pl.col('CURBAL').sum().alias('CURBAL'),
    pl.col('NETCHGMH').sum().alias('NETCHGMH'),
    pl.col('NETCHGYR').sum().alias('NETCHGYR'),
]).sort('BRANCH')

totals = report_data.select([
    pl.col(c).sum().alias(c) for c in [
        'OPENMH','OPENCUM','CLOSEMH','BCLOSE','CCLOSE',
        'CLOSECUM','NOACCT','CURBAL','NETCHGMH','NETCHGYR'
    ]
])

print(f"  Report branches: {len(report_data)}")

# ============================================================================
# STEP 8 – Write formatted report (PROC TABULATE equivalent)
# ============================================================================
# Layout reproduces SAS PROC TABULATE with:
#   FORMCHAR='           '  (all box chars → space)
#   NOSEPS                  (no horizontal rules between rows)
#   BOX='BRANCH'  RTS=10
#   FORMAT=COMMA10.  (default); CURBAL overridden to COMMA17.2
#
# Column order (from TABLE statement):
#   OPENMH  OPENCUM  CLOSEMH  BCLOSE  CCLOSE  CLOSECUM  NOACCT  CURBAL  NETCHGMH  NETCHGYR
#
# Widths:  10 10 10 10 10 10 10 17 10 10   (BRANCH header col = 10)
# All widths match their COMMA format width.  No separator chars between columns.
# ============================================================================

print("\nStep 8: Writing report …")

# Column definitions: (label-lines, width)
# Labels are split into lines that each fit within the column width.
# SAS right-justifies each label line within the column width.
COLUMNS = [
    # (label_lines_top_to_bottom, width)
    (['CURRENT MONTH', 'OPENED'],                10),
    (['CUMULATIVE', 'OPENED'],                   10),
    (['CURRENT MONTH', 'CLOSED'],                10),
    (['CLOSED BY', 'BANK'],                      10),
    (['CLOSED BY', 'CUSTOMER'],                  10),
    (['CUMULATIVE', 'CLOSED'],                   10),
    (['NO. OF', 'ACCTS'],                        10),
    (['TOTAL(RM)', 'O/S'],                       17),
    (['NET CHANGE FOR', 'THE MONTH'],            10),
    (['NET CHANGE YEAR', 'TO DATE'],             10),
]

# Determine maximum number of label lines across all columns
MAX_LABEL_LINES = max(len(labels) for labels, _ in COLUMNS)

RTS = 10   # row-title size (BRANCH column width)


def _fmt_comma10(value: int | float) -> str:
    """COMMA10. : width 10, no decimals, comma as thousands separator."""
    return f"{int(value):>10,}"


def _fmt_comma17_2(value: float) -> str:
    """COMMA17.2 : width 17, 2 decimals, comma as thousands separator."""
    return f"{value:>17,.2f}"


def _format_value(col_idx: int, value) -> str:
    """Route to the correct formatter based on column position."""
    if col_idx == 7:          # CURBAL column
        return _fmt_comma17_2(float(value) if value else 0.0)
    return _fmt_comma10(int(value) if value else 0)


with open(OUTPUT_REPORT, 'w') as f:
    # ---- TITLE lines (written by TITLE1 / TITLE2) -------------------------
    f.write(f"{ASA_NEW_PAGE}PUBLIC ISLAMIC BANK BERHAD\n")
    f.write(f"{ASA_SPACE}CURRENT ACCOUNT OPENED/CLOSED FOR THE MONTH AS AT {RDATE}\n")

    # ---- Column-label block ------------------------------------------------
    # Line 0 (top): BOX label occupies RTS width on the first label line
    for line_idx in range(MAX_LABEL_LINES):
        row = ''
        # Row-header portion
        if line_idx == 0:
            row += 'BRANCH'.rjust(RTS)
        else:
            row += ' ' * RTS

        # Data columns – each preceded by the FORMCHAR vline (one space)
        for col_idx, (labels, width) in enumerate(COLUMNS):
            # Pad label list so every column has MAX_LABEL_LINES entries
            padded = [''] * (MAX_LABEL_LINES - len(labels)) + labels
            row += ' ' + padded[line_idx].rjust(width)

        f.write(f"{ASA_SPACE}{row}\n")

    # ---- Data rows ---------------------------------------------------------
    report_pd = report_data.to_pandas()

    for _, row in report_pd.iterrows():
        line = str(int(row['BRANCH'])).rjust(RTS)
        values = [
            row['OPENMH'], row['OPENCUM'], row['CLOSEMH'],
            row['BCLOSE'], row['CCLOSE'], row['CLOSECUM'],
            row['NOACCT'], row['CURBAL'], row['NETCHGMH'], row['NETCHGYR'],
        ]
        for col_idx, val in enumerate(values):
            line += ' ' + _format_value(col_idx, val)
        f.write(f"{ASA_SPACE}{line}\n")

    # ---- TOTAL row ---------------------------------------------------------
    tot = totals.row(0, named=True)
    line = 'TOTAL'.rjust(RTS)
    tot_values = [
        tot['OPENMH'], tot['OPENCUM'], tot['CLOSEMH'],
        tot['BCLOSE'], tot['CCLOSE'], tot['CLOSECUM'],
        tot['NOACCT'], tot['CURBAL'], tot['NETCHGMH'], tot['NETCHGYR'],
    ]
    for col_idx, val in enumerate(tot_values):
        line += ' ' + _format_value(col_idx, val)
    f.write(f"{ASA_SPACE}{line}\n")

print(f"  Report written  →  {OUTPUT_REPORT}")

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 70)
print("CONVERSION COMPLETE")
print("=" * 70)
print(f"  CURRC  : {currc_path}")
print(f"  CURRF  : {currf_path}")
print(f"  Report : {OUTPUT_REPORT}")
print(f"  Date   : {RDATE}")
