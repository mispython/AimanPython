"""
File Name: EIIMRPTC_EIIMDPN3
Islamic Bank FD Account Opened/Closed Report
Generates a monthly report on fixed-deposit account activity with cumulative year-to-date tracking.

Outputs
-------
data/MIS/FDCmm.parquet          – detail extract of FD accounts closed this month
data/MIS/FDFmm.parquet          – branch × product summary with YTD cumulative columns
output/FD_ACCOUNT_REPORT.txt    – PROC TABULATE equivalent with ASA carriage control
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
INPUT_FD_FD          = "data/FD/FD.parquet"            # source for NOCD counts
INPUT_DEPOSIT_FD     = "data/DEPOSIT/FD.parquet"       # main FD deposit data
INPUT_FDFF_TEMPLATE  = "data/MIS/FDF{mon}.parquet"     # previous-month cumulative

# --- Output paths ----------------------------------------------------------
OUTPUT_MIS_DIR       = Path("data/MIS")
OUTPUT_MIS_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_REPORT_DIR    = Path("output")
OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FDC_TEMPLATE  = OUTPUT_MIS_DIR   / "FDC{mon}.parquet"
OUTPUT_FDF_TEMPLATE  = OUTPUT_MIS_DIR   / "FDF{mon}.parquet"
OUTPUT_REPORT        = OUTPUT_REPORT_DIR / "FD_ACCOUNT_REPORT.txt"

# --- Report constants ------------------------------------------------------
PAGE_LENGTH  = 60
ASA_NEW_PAGE = '1'
ASA_SPACE    = ' '


# ============================================================================
# PRODUCT FILTER – pre-built frozen set
# ============================================================================

VALID_PRODUCTS = frozenset(
    set(range(300, 317))          # 300-316 inclusive
    | {393, 394, 396}
)


# ============================================================================
# HELPER – numeric-to-date parsers (match SAS INPUT/SUBSTR behaviour)
# ============================================================================

def _parse_lasttran(val) -> object | None:
    """LASTTRAN integer; first 6 digits are MMDDYY."""
    if val is None or val == 0:
        return None
    try:
        s = str(int(val)).zfill(9)[:6]
        return datetime.strptime(s, '%m%d%y')
    except (ValueError, OverflowError):
        return None


def _parse_bdate(val) -> object | None:
    """BDATE integer; first 8 digits are MMDDYYYY."""
    if val is None or val == 0:
        return None
    try:
        s = str(int(val)).zfill(11)[:8]
        return datetime.strptime(s, '%m%d%Y')
    except (ValueError, OverflowError):
        return None


# ============================================================================
# STEP 1 – Read REPTDATE, derive macro-variable equivalents
# ============================================================================

print("Step 1: Reading REPTDATE …")

con = duckdb.connect()

reptdate_row = con.execute(f"SELECT REPTDATE FROM '{INPUT_REPTDATE}' LIMIT 1").fetchone()[0]

if isinstance(reptdate_row, (int, float)):
    reptdate_dt = datetime(1960, 1, 1) + timedelta(days=int(reptdate_row))
else:
    reptdate_dt = reptdate_row

MM       = reptdate_dt.month
MM1      = MM - 1 if MM > 1 else 12
RDATE    = reptdate_dt.strftime('%d/%m/%y')
RYEAR    = reptdate_dt.year
RMONTH   = reptdate_dt.month
REPTMON  = f"{MM:02d}"
REPTMON1 = f"{MM1:02d}"
RDAY     = reptdate_dt.day

print(f"  Reporting date : {RDATE}   month={REPTMON}   prev={REPTMON1}")


# ============================================================================
# STEP 2 – Build FDC NOCD counts from FD.FD
#
# SAS logic: sort FD.FD by ACCTNO (WHERE OPENIND IN ('O','D')),
# then count rows per ACCTNO using FIRST./LAST. pattern.
# Polars equivalent: filter then group_by + count.
# ============================================================================

print("\nStep 2: Computing NOCD counts from FD.FD …")

fdc_source = con.execute(f"""
    SELECT ACCTNO
    FROM   '{INPUT_FD_FD}'
    WHERE  OPENIND IN ('O', 'D')
""").pl()

# group_by ACCTNO, count rows -> NOCD  (minimum 1 per group, matching SAS +1 on FIRST)
nocd_df = (
    fdc_source
    .group_by('ACCTNO')
    .agg(pl.len().alias('NOCD'))
)

print(f"  NOCD lookup rows: {len(nocd_df)}")


# ============================================================================
# STEP 3 – Load DEPOSIT.FD, apply branch / product / status filters
# ============================================================================

print("\nStep 3: Loading DEPOSIT.FD …")

fd_raw = con.execute(f"SELECT * FROM '{INPUT_DEPOSIT_FD}'").pl()
print(f"  Raw rows: {len(fd_raw)}")

# --- branch: delete 227, remap 250 -> 92 ---
fd_raw = fd_raw.filter(pl.col('BRANCH') != 227)
fd_raw = fd_raw.with_columns(
    pl.col('BRANCH').replace({250: 92}).alias('BRANCH')
)

# --- product filter ---
fd_raw = fd_raw.filter(pl.col('PRODUCT').is_in(list(VALID_PRODUCTS)))
print(f"  After product filter: {len(fd_raw)}")

# --- OPENIND 'Z' -> 'O', CLOSEMH -> 0 ---
fd_raw = fd_raw.with_columns(
    pl.when(pl.col('OPENIND') == 'Z')
      .then(pl.lit('O'))
      .otherwise(pl.col('OPENIND'))
      .alias('OPENIND'),

    pl.when(pl.col('OPENIND') == 'Z')
      .then(pl.lit(0))
      .otherwise(pl.col('CLOSEMH'))
      .alias('CLOSEMH'),
)

# --- Retention: OPENIND='O'  OR  (OPENIND in B/C/P AND CLOSEMH=1) ---
fd_raw = fd_raw.filter(
    (pl.col('OPENIND') == 'O')
    | (pl.col('OPENIND').is_in(['B', 'C', 'P']) & (pl.col('CLOSEMH') == 1))
)
print(f"  After retention filter: {len(fd_raw)}")

# --- NOACCT flag (no BCLOSE/CCLOSE split in FD program) ---
fd_raw = fd_raw.with_columns(
    pl.when(pl.col('OPENIND').is_in(['B', 'C', 'P']))
      .then(pl.lit(0))
      .otherwise(pl.lit(1))
      .alias('NOACCT')
)


# ============================================================================
# STEP 4 – Extract FDC detail (closed accounts this month) and save
# ============================================================================

print("\nStep 4: Building FDC closed-account detail …")

fdc_detail = fd_raw.filter(
    pl.col('OPENIND').is_in(['B', 'C', 'P']) & (pl.col('CLOSEMH') == 1)
)

# Derive columns requiring row-level logic – single pandas round-trip
fdc_pd = fdc_detail.to_pandas()
fdc_pd['YTDAVBAL'] = fdc_pd['YTDAVAMT']
fdc_pd['CUSTFISS'] = 0                                          # literal 0 per SAS
fdc_pd['LASTTRAN'] = fdc_pd['LASTTRAN'].apply(_parse_lasttran)
fdc_pd['DOBMNI']   = fdc_pd['BDATE'].apply(_parse_bdate)

fdc_df = pl.from_pandas(fdc_pd)

# KEEP list matches the SAS KEEP statement exactly
FDC_KEEP = [
    'BRANCH','ACCTNO','OPENDT','CLOSEDT','OPENIND','CURBAL','CUSTCODE',
    'MTDAVBAL','YTDAVBAL','CUSTFISS','DOBMNI','PRODUCT','COSTCTR','DEPTYPE',
    'DNBFISME','INTPD','INTPLAN','INTRSTPD','INTYTD','LASTTRAN','ORGCODE',
    'ORGTYPE','PURPOSE','RISKCODE','SECOND','SECTOR','SERVICE','STATE',
    'USER2','USER3','USER5','STMT_CYCLE','NXT_STMT_CYCLE_DT',
]

fdc_df = fdc_df.select([c for c in FDC_KEEP if c in fdc_df.columns])

fdc_path = str(OUTPUT_FDC_TEMPLATE).format(mon=REPTMON)
fdc_df.write_parquet(fdc_path)
print(f"  FDC rows: {len(fdc_df)}  →  {fdc_path}")


# ============================================================================
# STEP 5 – Left-join NOCD counts onto main FD, then summarise
#
# SAS: MERGE FDC FD(IN=A); BY ACCTNO; IF A;
# keeps all FD rows; FD rows without a NOCD match get NOCD = 0.
# ============================================================================

print("\nStep 5: Merging NOCD and summarising …")

fd_with_nocd = fd_raw.join(nocd_df, on='ACCTNO', how='left')
fd_with_nocd = fd_with_nocd.with_columns(
    pl.col('NOCD').fill_null(0).alias('NOCD')
)

# PROC SUMMARY by BRANCH × PRODUCT
fd_summary = fd_with_nocd.group_by(['BRANCH', 'PRODUCT']).agg([
    pl.col('OPENMH').sum().alias('OPENMH'),
    pl.col('CLOSEMH').sum().alias('CLOSEMH'),
    pl.col('NOACCT').sum().alias('NOACCT'),
    pl.col('NOCD').sum().alias('NOCD'),
    pl.col('CURBAL').sum().alias('CURBAL'),
])

print(f"  Summary rows: {len(fd_summary)}")


# ============================================================================
# STEP 6 – %PROCESS: cumulative merge or January initialisation
# ============================================================================

print("\nStep 6: Cumulative processing …")

if int(REPTMON) > 1:
    prev_path = str(INPUT_FDFF_TEMPLATE).format(mon=REPTMON1)
    print(f"  Loading previous month: {prev_path}")

    try:
        fdp = con.execute(f"SELECT * FROM '{prev_path}'").pl()

        # Rename cumulative columns to temporary names
        fdp = fdp.rename({'OPENCUM': 'OPENCUX', 'CLOSECUM': 'CLOSECUX'})

        # Branch remap on previous-month data (only 250 -> 092)
        fdp = fdp.with_columns(
            pl.col('BRANCH').replace({250: 92}).alias('BRANCH')
        )

        # Re-aggregate previous month (guard against duplicates in stored file)
        fdp = fdp.group_by(['BRANCH', 'PRODUCT']).agg([
            pl.col('OPENCUX').sum().alias('OPENCUX'),
            pl.col('CLOSECUX').sum().alias('CLOSECUX'),
        ])

        # Outer join: previous cumulative ∪ current month
        fd_merged = fdp.join(fd_summary, on=['BRANCH', 'PRODUCT'], how='outer')

        # Fill nulls with 0 (matches SAS IF x=. THEN x=0 block)
        fill_cols = ['OPENCUX', 'CLOSECUX', 'OPENMH', 'CLOSEMH',
                     'NOACCT', 'CURBAL', 'NOCD']
        fd_merged = fd_merged.with_columns([
            pl.col(c).fill_null(0) for c in fill_cols
        ])

        # Derive cumulative and net-change columns
        fd_final = fd_merged.with_columns([
            (pl.col('OPENMH')  + pl.col('OPENCUX') ).alias('OPENCUM'),
            (pl.col('CLOSEMH') + pl.col('CLOSECUX')).alias('CLOSECUM'),
            (pl.col('OPENMH')  - pl.col('CLOSEMH') ).alias('NETCHGMH'),
        ]).with_columns(
            (pl.col('OPENCUM') - pl.col('CLOSECUM')).alias('NETCHGYR'),
        )

    except Exception as exc:
        print(f"  Warning: could not load previous month ({exc}). "
              "Falling back to January path.")
        fd_final = fd_summary.with_columns([
            pl.col('OPENMH').alias('OPENCUM'),
            pl.col('CLOSEMH').alias('CLOSECUM'),
            (pl.col('OPENMH')  - pl.col('CLOSEMH')).alias('NETCHGMH'),
            (pl.col('OPENMH')  - pl.col('CLOSEMH')).alias('NETCHGYR'),
        ])
else:
    print("  January – initialising cumulative columns from monthly values.")
    # January path also carries the 250->092 remap (present in the SAS ELSE branch)
    fd_summary = fd_summary.with_columns(
        pl.col('BRANCH').replace({250: 92}).alias('BRANCH')
    )
    fd_final = fd_summary.with_columns([
        pl.col('OPENMH').alias('OPENCUM'),
        pl.col('CLOSEMH').alias('CLOSECUM'),
        (pl.col('OPENMH')  - pl.col('CLOSEMH')).alias('NETCHGMH'),
        (pl.col('OPENMH')  - pl.col('CLOSEMH')).alias('NETCHGYR'),
    ])


# ============================================================================
# STEP 7 – Save FDF cumulative summary
# ============================================================================

fdf_path = str(OUTPUT_FDF_TEMPLATE).format(mon=REPTMON)
fd_final.write_parquet(fdf_path)
print(f"\nStep 7: FDF saved  →  {fdf_path}  ({len(fd_final)} rows)")


# ============================================================================
# STEP 8 – Aggregate by BRANCH for the report
# ============================================================================

print("\nStep 8: Aggregating by BRANCH for report …")

report_data = fd_final.group_by('BRANCH').agg([
    pl.col('OPENMH').sum().alias('OPENMH'),
    pl.col('OPENCUM').sum().alias('OPENCUM'),
    pl.col('CLOSEMH').sum().alias('CLOSEMH'),
    pl.col('CLOSECUM').sum().alias('CLOSECUM'),
    pl.col('NOACCT').sum().alias('NOACCT'),
    pl.col('NOCD').sum().alias('NOCD'),
    pl.col('CURBAL').sum().alias('CURBAL'),
    pl.col('NETCHGMH').sum().alias('NETCHGMH'),
    pl.col('NETCHGYR').sum().alias('NETCHGYR'),
]).sort('BRANCH')

totals = report_data.select([
    pl.col(c).sum().alias(c) for c in [
        'OPENMH', 'OPENCUM', 'CLOSEMH', 'CLOSECUM', 'NOACCT',
        'NOCD', 'CURBAL', 'NETCHGMH', 'NETCHGYR',
    ]
])

print(f"  Report branches: {len(report_data)}")


# ============================================================================
# STEP 9 – Write formatted report (PROC TABULATE equivalent)
#
# Layout reproduces SAS PROC TABULATE with:
#   FORMCHAR='           '   → all box-drawing chars are spaces
#   NOSEPS                   → no horizontal rules between data rows
#   BOX='BRANCH'  RTS=10
#   FORMAT=COMMA10.          → default; CURBAL overridden to COMMA18.2
#
# Column order (from TABLE statement):
#   OPENMH  OPENCUM  CLOSEMH  CLOSECUM  NOACCT  NOCD  CURBAL  NETCHGMH  NETCHGYR
#
# Widths:  10 10 10 10 10 10 18 10 10   (RTS = 10)
# Single-space vline separator between every pair of adjacent columns.
# ============================================================================

print("\nStep 9: Writing report …")

RTS = 10          # row-title size (BRANCH column width)

# Column definitions: (label_lines, width)
# Labels are split so each line fits within the column width.
# SAS right-justifies each label line; top-padded so all columns share the same baseline.
COLUMNS = [
    (['CURRENT MONTH', 'OPENED'],                10),
    (['CUMULATIVE',    'OPENED'],                10),
    (['CURRENT MONTH', 'CLOSED'],                10),
    (['CUMULATIVE',    'CLOSED'],                10),
    (['NO.OF',         'ACCTS'],                 10),
    (['NO.OF',         'CDS'],                   10),
    (['TOTAL (RM)',    'O/S'],                   18),   # COMMA18.2
    (['NET CHANGE FOR','THE MONTH'],             10),
    (['NET CHANGE YEAR','TO DATE'],              10),
]

MAX_LABEL_LINES = max(len(labels) for labels, _ in COLUMNS)


def _fmt_comma10(value) -> str:
    """COMMA10. : width 10, no decimals, comma thousands separator."""
    return f"{int(value):>10,}"


def _fmt_comma18_2(value) -> str:
    """COMMA18.2 : width 18, 2 decimals, comma thousands separator."""
    return f"{float(value):>18,.2f}"


def _format_value(col_idx: int, value) -> str:
    """Route to the correct formatter based on column position."""
    if col_idx == 6:          # CURBAL column
        return _fmt_comma18_2(value if value else 0.0)
    return _fmt_comma10(value if value else 0)


with open(OUTPUT_REPORT, 'w') as f:
    # ---- TITLE lines (TITLE1 / TITLE2) ------------------------------------
    f.write(f"{ASA_NEW_PAGE}PUBLIC ISLAMIC BANK BERHAD\n")
    f.write(f"{ASA_SPACE}FD ACCOUNT OPENED/CLOSED FOR THE MONTH AS AT {RDATE}\n")

    # ---- Column-label block ------------------------------------------------
    for line_idx in range(MAX_LABEL_LINES):
        row = ''
        # Row-header portion: BOX label on first line, blanks thereafter
        if line_idx == 0:
            row += 'BRANCH'.rjust(RTS)
        else:
            row += ' ' * RTS

        # Data columns – each preceded by the FORMCHAR vline (one space)
        for col_idx, (labels, width) in enumerate(COLUMNS):
            padded = [''] * (MAX_LABEL_LINES - len(labels)) + labels
            row += ' ' + padded[line_idx].rjust(width)

        f.write(f"{ASA_SPACE}{row}\n")

    # ---- Data rows ---------------------------------------------------------
    report_pd = report_data.to_pandas()

    for _, row in report_pd.iterrows():
        line = str(int(row['BRANCH'])).rjust(RTS)
        values = [
            row['OPENMH'], row['OPENCUM'], row['CLOSEMH'], row['CLOSECUM'],
            row['NOACCT'], row['NOCD'], row['CURBAL'],
            row['NETCHGMH'], row['NETCHGYR'],
        ]
        for col_idx, val in enumerate(values):
            line += ' ' + _format_value(col_idx, val)
        f.write(f"{ASA_SPACE}{line}\n")

    # ---- TOTAL row ---------------------------------------------------------
    tot = totals.row(0, named=True)
    line = 'TOTAL'.rjust(RTS)
    tot_values = [
        tot['OPENMH'], tot['OPENCUM'], tot['CLOSEMH'], tot['CLOSECUM'],
        tot['NOACCT'], tot['NOCD'], tot['CURBAL'],
        tot['NETCHGMH'], tot['NETCHGYR'],
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
print(f"  FDC    : {fdc_path}")
print(f"  FDF    : {fdf_path}")
print(f"  Report : {OUTPUT_REPORT}")
print(f"  Date   : {RDATE}")
