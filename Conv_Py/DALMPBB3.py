#!/usr/bin/env python3
"""
Program : DALMPBB3.py
Report  : ACE REPORT FOR PRODUCT DEVELOPMENT
Date    : 05.12.97
Purpose : Manipulate the extracted savings and current account data sets.
          Modified on 18th July 2000 to include Bonuslink Saving
          and Bonuslink Current accounts.

          Produces three breakdown-by-deposit-range reports:
            1. BREAKDOWN OF ACE ACCOUNTS AS AT <date>
               (main ACE report – 68 balance bands, columns: NO OF ACCT / TOTAL RM)
            2. BREAKDOWN OF PB CURRENTLINK A/C AS AT <date>  (GENDATA macro)
            3. BREAKDOWN OF BASIC CURRENT A/C AS AT <date>   (GENDATA macro)
            4. BREAKDOWN OF BASIC 55 CURRENT A/C AS AT <date>(GENDATA macro)
          Each GENDATA report uses 49 balance bands with 3 value columns:
            NO OF ACCOUNT / AMOUNT OUTSTANDING (RM) / TOTAL
"""

import duckdb
import polars as pl
import os
from datetime import datetime, date

# ---------------------------------------------------------------------------
# DEPENDENCY: PBBDPFMT – product lists and format definitions
# ---------------------------------------------------------------------------
from PBBDPFMT import ProductLists, SAProductFormat

# ---------------------------------------------------------------------------
# PATH CONFIGURATION
# ---------------------------------------------------------------------------
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR  = os.path.join(BASE_DIR, "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Input parquet paths
REPTDATE_PATH = os.path.join(INPUT_DIR, "deposit_reptdate.parquet")
CURRENT_PATH  = os.path.join(INPUT_DIR, "deposit_current.parquet")
SAVING_PATH   = os.path.join(INPUT_DIR, "deposit_saving.parquet")

# Output report files (ASA carriage-control text)
OUTPUT_ACE     = os.path.join(OUTPUT_DIR, "DALMPBB3_ACE.txt")
OUTPUT_BONUC   = os.path.join(OUTPUT_DIR, "DALMPBB3_BONUC.txt")
OUTPUT_BASIC   = os.path.join(OUTPUT_DIR, "DALMPBB3_BASIC.txt")
OUTPUT_BASIC55 = os.path.join(OUTPUT_DIR, "DALMPBB3_BASIC55.txt")

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
PAGE_LENGTH = 60   # lines per page (ASA carriage control)

# Product code sets drawn directly from PBBDPFMT ProductLists,
# mirroring the SAS %INC PGM(PBBDPFMT) which loads these definitions.
#
# ACE products: SAS IF DEPTYPE IN ('D','N') AND PRODUCT IN (150,151,152,181)
# ProductLists.ACE_PRODUCTS = {40, 42, 43, 150, 151, 152, 181}; we intersect
# to the four codes actually named in this program's DATA step filter.
ACE_PRODUCTS     = ProductLists.ACE_PRODUCTS & {150, 151, 152, 181}

# BASIC current account (product 90) – member of CURX_PRODUCTS in PBBDPFMT
BASIC_PRODUCTS   = ProductLists.CURX_PRODUCTS & {90}

# BASIC 55 current account (product 91) – member of CURX_PRODUCTS in PBBDPFMT
BASIC55_PRODUCTS = ProductLists.CURX_PRODUCTS & {91}

# Bonuslink current account / PB CURRENTLINK (156,157,158) – CURX_PRODUCTS
BONUC_PRODUCTS   = ProductLists.CURX_PRODUCTS & {156, 157, 158}

# Bonuslink saving / PB SAVELINK (213) – standalone savings product
# (213 is present in SAProductFormat.MAPPINGS in PBBDPFMT as 'PB SAVELINK')
BONUS_PRODUCTS   = {213}

# ---------------------------------------------------------------------------
# READ REPORT DATE & DERIVE MACRO VARIABLES
# ---------------------------------------------------------------------------

def get_meta() -> dict:
    """
    Reads DEPOSIT.REPTDATE and derives:
      NOWK     : week indicator (1-4) based on day-of-month
      REPTYEAR : 4-digit year
      REPTMON  : zero-padded 2-digit month
      REPTDAY  : zero-padded 2-digit day
      RDATE    : DD/MM/YY  (DDMMYY8.)
      PREMON   : zero-padded previous month
    """
    con = duckdb.connect()
    row = con.execute(
        f"SELECT reptdate FROM read_parquet('{REPTDATE_PATH}') LIMIT 1"
    ).fetchone()
    con.close()

    if not row:
        raise RuntimeError("No row found in deposit_reptdate")

    val = row[0]
    if isinstance(val, (datetime, date)):
        d = val if isinstance(val, date) else val.date()
    else:
        d = datetime.strptime(str(val), '%Y-%m-%d').date()

    day = d.day
    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'

    mth = d.month - 1
    if mth == 0:
        mth = 12

    return {
        'NOWK':     nowk,
        'REPTYEAR': str(d.year),
        'REPTMON':  f"{d.month:02d}",
        'REPTDAY':  f"{d.day:02d}",
        'RDATE':    d.strftime('%d/%m/%y'),   # DDMMYY8.
        'PREMON':   f"{mth:02d}",
        'date_obj': d,
    }


# ---------------------------------------------------------------------------
# DATA SPLIT – load DEPOSIT.CURRENT + DEPOSIT.SAVING and route to datasets
# ---------------------------------------------------------------------------

def load_and_split() -> dict[str, pl.DataFrame]:
    """
    DATA SAVG BONUS BASIC BASIC55 BONUC:
      SET DEPOSIT.CURRENT DEPOSIT.SAVING
      WHERE OPENIND NOT IN ('B','C','P')
      Routes by PRODUCT and DEPTYPE.
    Returns dict of DataFrames keyed by dataset name (SAVG, BONUS, BASIC, BASIC55, BONUC).
    Each keeps only CURBAL and RACE columns as per SAS KEEP= options.
    """
    con = duckdb.connect()

    # Combine CURRENT and SAVING (union all)
    combined_sql = f"""
        SELECT CURBAL, RACE, PRODUCT,
               COALESCE(DEPTYPE, '') AS DEPTYPE,
               OPENIND
        FROM read_parquet('{CURRENT_PATH}')
        WHERE OPENIND NOT IN ('B','C','P')
        UNION ALL
        SELECT CURBAL, RACE, PRODUCT,
               COALESCE(DEPTYPE, '') AS DEPTYPE,
               OPENIND
        FROM read_parquet('{SAVING_PATH}')
        WHERE OPENIND NOT IN ('B','C','P')
    """
    combined = con.execute(combined_sql).pl()
    con.close()

    # Route each row to the appropriate output dataset
    savg_rows    = []
    bonus_rows   = []
    basic_rows   = []
    basic55_rows = []
    bonuc_rows   = []

    for row in combined.iter_rows(named=True):
        product = row.get('PRODUCT') or 0
        deptype = str(row.get('DEPTYPE') or '')
        keep    = {'CURBAL': row.get('CURBAL'), 'RACE': str(row.get('RACE') or '')}

        # IF DEPTYPE IN ('D','N') AND PRODUCT IN (150,151,152,181) -> SAVG
        if deptype in ('D', 'N') and product in ACE_PRODUCTS:
            savg_rows.append(keep)
        # ELSE IF PRODUCT IN (90) -> BASIC
        elif product in BASIC_PRODUCTS:
            basic_rows.append(keep)
        # ELSE IF PRODUCT IN (91) -> BASIC55
        elif product in BASIC55_PRODUCTS:
            basic55_rows.append(keep)
        # ELSE IF PRODUCT IN (156,157,158) -> BONUC
        elif product in BONUC_PRODUCTS:
            bonuc_rows.append(keep)
        # ELSE IF PRODUCT IN (213) -> BONUS
        elif product in BONUS_PRODUCTS:
            bonus_rows.append(keep)

    def to_df(rows):
        return pl.DataFrame(rows) if rows else pl.DataFrame(
            schema={"CURBAL": pl.Float64, "RACE": pl.Utf8}
        )

    return {
        'SAVG':    to_df(savg_rows),
        'BONUS':   to_df(bonus_rows),
        'BASIC':   to_df(basic_rows),
        'BASIC55': to_df(basic55_rows),
        'BONUC':   to_df(bonuc_rows),
    }


# ---------------------------------------------------------------------------
# ASA REPORT WRITER
# ---------------------------------------------------------------------------

def asa_line(cc: str, text: str) -> str:
    return f"{cc}{text}\n"


class ReportWriter:
    """ASA carriage-control report writer with page-break support."""

    def __init__(self, filepath: str):
        self.filepath    = filepath
        self.lines: list[str] = []
        self.body_count  = 0
        self.page_num    = 0

    def new_page(self, header_lines: list[str]):
        """Emit a new-page header (ASA '1' for first line, ' ' for rest)."""
        self.page_num  += 1
        self.body_count = 0
        first = True
        for h in header_lines:
            cc = '1' if first else ' '
            self.lines.append(asa_line(cc, h))
            first = False

    def write(self, cc: str, text: str):
        if self.body_count >= PAGE_LENGTH and self.page_num > 0:
            # Automatic page break triggers caller to re-emit header via HEADER=NEWPAGE
            # We handle this inline: emit a blank form-feed marker
            self.lines.append(asa_line('1', ''))
            self.body_count = 0
        self.lines.append(asa_line(cc, text))
        self.body_count += 1

    def save(self):
        with open(self.filepath, 'w', encoding='utf-8') as f:
            f.writelines(self.lines)


# ---------------------------------------------------------------------------
# BALANCE-BAND DEFINITIONS
# ---------------------------------------------------------------------------

# 68 bands used in the main ACE (SAVG2) report
# Each entry: (seq_label, noacc_key, range_key, lo_excl, hi_incl_or_None)
# lo_excl=None means -inf; hi_incl=None means +inf
ACE_BANDS = [
    (' 0)       RM NEGATIVE BALANCE    ', 'NOACC0',  'RANGE0',  None,       0,      True),   # < 0
    (' 1)       ZERO BALANCE           ', 'NOACCX',  'RANGEX',  0,          0,      False),  # == 0 special
    (' 2)      ABOVE RM0.00 -        RM5.00',   'NOACC1',  'RANGE1',  0,     5,      False),
    (' 3)      ABOVE RM5.00 -        RM10.00',  'NOACC2',  'RANGE2',  5,     10,     False),
    (' 4)      ABOVE RM10.00 -       RM50.00',  'NOACC3',  'RANGE3',  10,    50,     False),
    (' 5)      ABOVE RM50.00 -       RM100.00', 'NOACC4',  'RANGE4',  50,    100,    False),
    (' 6)     ABOVE  RM100.00 -      RM500.00', 'NOACC5',  'RANGE5',  100,   500,    False),
    (' 7)     ABOVE RM500.00 -      RM1000.00', 'NOACC6',  'RANGE6',  500,   1000,   False),
    (' 8)    ABOVE RM1000.00 -      RM1500.00', 'NOACC7',  'RANGE7',  1000,  1500,   False),
    (' 9)    ABOVE RM1500.00 -      RM2000.00', 'NOACC8',  'RANGE8',  1500,  2000,   False),
    ('10)    ABOVE RM2000.00 -      RM2500.00', 'NOACC9',  'RANGE9',  2000,  2500,   False),
    ('11)    ABOVE RM2500.00 -      RM3000.00', 'NOACC10', 'RANGE10', 2500,  3000,   False),
    ('12)    ABOVE RM3000.00 -      RM3500.00', 'NOACC11', 'RANGE11', 3000,  3500,   False),
    ('13)    ABOVE RM3500.00 -      RM4000.00', 'NOACC12', 'RANGE12', 3500,  4000,   False),
    ('14)    ABOVE RM4000.00 -      RM4500.00', 'NOACC13', 'RANGE13', 4000,  4500,   False),
    ('15)    ABOVE RM4500.00 -      RM5000.00', 'NOACC14', 'RANGE14', 4500,  5000,   False),
    ('16)    ABOVE RM5000.00 -      RM6000.00', 'NOACC15', 'RANGE15', 5000,  6000,   False),
    ('17)    ABOVE RM6000.00 -      RM7000.00', 'NOACC16', 'RANGE16', 6000,  7000,   False),
    ('18)    ABOVE RM7000.00 -      RM8000.00', 'NOACC17', 'RANGE17', 7000,  8000,   False),
    ('19)    ABOVE RM8000.00 -      RM9000.00', 'NOACC18', 'RANGE18', 8000,  9000,   False),
    ('20)    ABOVE RM9000.00 -    RM10,000.00', 'NOACC19', 'RANGE19', 9000,  10000,  False),
    ('21)  ABOVE RM10,000.00 -    RM15,000.00', 'NOACC20', 'RANGE20', 10000, 15000,  False),
    ('22)  ABOVE RM15,000.00 -    RM20,000.00', 'NOACC21', 'RANGE21', 15000, 20000,  False),
    ('23)  ABOVE RM20,000.00 -    RM25,000.00', 'NOACC22', 'RANGE22', 20000, 25000,  False),
    ('24)  ABOVE RM25,000.00 -    RM30,000.00', 'NOACC23', 'RANGE23', 25000, 30000,  False),
    ('25)  ABOVE RM30,000.00 -    RM35,000.00', 'NOACC24', 'RANGE24', 30000, 35000,  False),
    ('26)  ABOVE RM35,000.00 -    RM40,000.00', 'NOACC25', 'RANGE25', 35000, 40000,  False),
    ('27)  ABOVE RM40,000.00 -    RM45,000.00', 'NOACC26', 'RANGE26', 40000, 45000,  False),
    ('28)  ABOVE RM45,000.00 -    RM50,000.00', 'NOACC27', 'RANGE27', 45000, 50000,  False),
    ('29)  ABOVE RM50,000.00 -    RM55,000.00', 'NOACC28', 'RANGE28', 50000, 55000,  False),
    ('30)  ABOVE RM55,000.00 -    RM60,000.00', 'NOACC29', 'RANGE29', 55000, 60000,  False),
    ('31)  ABOVE RM60,000.00 -    RM65,000.00', 'NOACC30', 'RANGE30', 60000, 65000,  False),
    ('32)  ABOVE RM65,000.00 -    RM70,000.00', 'NOACC31', 'RANGE31', 65000, 70000,  False),
    ('33)  ABOVE RM70,000.00 -    RM75,000.00', 'NOACC32', 'RANGE32', 70000, 75000,  False),
    ('34)  ABOVE RM75,000.00 -    RM80,000.00', 'NOACC33', 'RANGE33', 75000, 80000,  False),
    ('35)  ABOVE RM80,000.00 -    RM85,000.00', 'NOACC34', 'RANGE34', 80000, 85000,  False),
    ('36)  ABOVE RM85,000.00 -    RM90,000.00', 'NOACC35', 'RANGE35', 85000, 90000,  False),
    ('37)  ABOVE RM90,000.00 -    RM95,000.00', 'NOACC36', 'RANGE36', 90000, 95000,  False),
    ('38)  ABOVE RM95,000.00 -   RM100,000.00', 'NOACC37', 'RANGE37', 95000, 100000, False),
    ('39) ABOVE RM100,000.00 -   RM150,000.00', 'NOACC38', 'RANGE38', 100000,150000, False),
    ('40) ABOVE RM150,000.00 -   RM200,000.00', 'NOACC39', 'RANGE39', 150000,200000, False),
    ('41) ABOVE RM200,000.00 -   RM300,000.00', 'NOACC40', 'RANGE40', 200000,300000, False),
    ('42) ABOVE RM300,000.00 -   RM400,000.00', 'NOACC41', 'RANGE41', 300000,400000, False),
    ('43) ABOVE RM400,000.00 -   RM500,000.00', 'NOACC42', 'RANGE42', 400000,500000, False),
    ('44) ABOVE RM500,000.00 -   RM510,000.00', 'NOACC43', 'RANGE43', 500000,510000, False),
    ('45) ABOVE RM510,000.00 -   RM600,000.00', 'NOACC44', 'RANGE44', 510000,600000, False),
    ('46) ABOVE RM600,000.00 -   RM700,000.00', 'NOACC45', 'RANGE45', 600000,700000, False),
    ('47) ABOVE RM700,000.00 -   RM800,000.00', 'NOACC46', 'RANGE46', 700000,800000, False),
    ('48) ABOVE RM800,000.00 -   RM900,000.00', 'NOACC47', 'RANGE47', 800000,900000, False),
    ('49) ABOVE RM900,000.00 -   RM1,000,000.00',  'NOACC48', 'RANGE48', 900000,   1000000,  False),
    ('50) ABOVE RM1,000,000.00 - RM1,100,000.00',  'NOACC49', 'RANGE49', 1000000,  1100000,  False),
    ('51) ABOVE RM1,100,000.00 - RM1,200,000.00',  'NOACC50', 'RANGE50', 1100000,  1200000,  False),
    ('52) ABOVE RM1,200,000.00 - RM1,300,000.00',  'NOACC51', 'RANGE51', 1200000,  1300000,  False),
    ('53) ABOVE RM1,300,000.00 - RM1,400,000.00',  'NOACC52', 'RANGE52', 1300000,  1400000,  False),
    ('54) ABOVE RM1,400,000.00 - RM1,500,000.00',  'NOACC53', 'RANGE53', 1400000,  1500000,  False),
    ('55) ABOVE RM1,500,000.00 - RM1,510,000.00',  'NOACC54', 'RANGE54', 1500000,  1510000,  False),
    ('56) ABOVE RM1,510,000.00 - RM1,550,000.00',  'NOACC55', 'RANGE55', 1510000,  1550000,  False),
    ('57) ABOVE RM1,550,000.00 - RM1,600,000.00',  'NOACC56', 'RANGE56', 1550000,  1600000,  False),
    ('58) ABOVE RM1,600,000.00 - RM1,700,000.00',  'NOACC57', 'RANGE57', 1600000,  1700000,  False),
    ('59) ABOVE RM1,700,000.00 - RM1,800,000.00',  'NOACC58', 'RANGE58', 1700000,  1800000,  False),
    ('60) ABOVE RM1,800,000.00 - RM1,900,000.00',  'NOACC59', 'RANGE59', 1800000,  1900000,  False),
    ('61) ABOVE RM1,900,000.00 - RM2,000,000.00',  'NOACC60', 'RANGE60', 1900000,  2000000,  False),
    ('62) ABOVE RM2,000,000.00 - RM3,000,000.00',  'NOACC61', 'RANGE61', 2000000,  3000000,  False),
    ('63) ABOVE RM3,000,000.00 - RM3,510,000.00',  'NOACC62', 'RANGE62', 3000000,  3510000,  False),
    ('64) ABOVE RM3,510,000.00 - RM3,550,000.00',  'NOACC63', 'RANGE63', 3510000,  3550000,  False),
    ('65) ABOVE RM3,550,000.00 - RM4,000,000.00',  'NOACC64', 'RANGE64', 3550000,  4000000,  False),
    ('66) ABOVE RM4,000,000.00 - RM5,000,000.00',  'NOACC65', 'RANGE65', 4000000,  5000000,  False),
    ('67) ABOVE RM5,000,000.00 - RM10,000,000.00', 'NOACC66', 'RANGE66', 5000000,  10000000, False),
    ('68) ABOVE RM10,000,000.00',                  'NOACC67', 'RANGE67', 10000000, None,      False),
]
# Tuple positions: 0=label, 1=noacc_key(unused), 2=range_key(unused),
#                  3=lo_exclusive, 4=hi_inclusive_or_upper_exclusive, 5=is_negative_band

# 49 bands used in the %GENDATA macro reports
GENDATA_BANDS = [
    (' 1)       $NEGATIVE BALANCE    ',    None,    0,        True),   # < 0
    (' 2)       $0.00 -         $5.00',    0,       5,        False),  # >= 0 <= 5
    (' 3)       $5.01 -        $10.00',    5,       10,       False),
    (' 4)      $10.01 -        $50.00',    10,      50,       False),
    (' 5)      $50.01 -       $100.00',    50,      100,      False),
    (' 6)     $100.01 -       $500.00',    100,     500,      False),
    (' 7)     $500.01 -      $1000.00',    500,     1000,     False),
    (' 8)    $1000.01 -      $1500.00',    1000,    1500,     False),
    (' 9)    $1500.01 -      $2000.00',    1500,    2000,     False),
    ('10)    $2000.01 -      $2500.00',    2000,    2500,     False),
    ('11)    $2500.01 -      $3000.00',    2500,    3000,     False),
    ('12)    $3000.01 -      $3500.00',    3000,    3500,     False),
    ('13)    $3500.01 -      $4000.00',    3500,    4000,     False),
    ('14)    $4000.01 -      $4500.00',    4000,    4500,     False),
    ('15)    $4500.01 -      $5000.00',    4500,    5000,     False),
    ('16)    $5000.01 -      $6000.00',    5000,    6000,     False),
    ('17)    $6000.01 -      $7000.00',    6000,    7000,     False),
    ('18)    $7000.01 -      $8000.00',    7000,    8000,     False),
    ('19)    $8000.01 -      $9000.00',    8000,    9000,     False),
    ('20)    $9000.01 -    $10,000.00',    9000,    10000,    False),
    ('21)  $10,000.01 -    $15,000.00',    10000,   15000,    False),
    ('22)  $15,000.01 -    $20,000.00',    15000,   20000,    False),
    ('23)  $20,000.01 -    $25,000.00',    20000,   25000,    False),
    ('24)  $25,000.01 -    $30,000.00',    25000,   30000,    False),
    ('25)  $30,000.01 -    $35,000.00',    30000,   35000,    False),
    ('26)  $35,000.01 -    $40,000.00',    35000,   40000,    False),
    ('27)  $40,000.01 -    $45,000.00',    40000,   45000,    False),
    ('28)  $45,000.01 -    $50,000.00',    45000,   50000,    False),
    ('29)  $50,000.01 -    $55,000.00',    50000,   55000,    False),
    ('30)  $55,000.01 -    $60,000.00',    55000,   60000,    False),
    ('31)  $60,000.01 -    $65,000.00',    60000,   65000,    False),
    ('32)  $65,000.01 -    $70,000.00',    65000,   70000,    False),
    ('33)  $70,000.01 -    $75,000.00',    70000,   75000,    False),
    ('34)  $75,000.01 -    $80,000.00',    75000,   80000,    False),
    ('35)  $80,000.01 -    $85,000.00',    80000,   85000,    False),
    ('36)  $85,000.01 -    $90,000.00',    85000,   90000,    False),
    ('37)  $90,000.01 -    $95,000.00',    90000,   95000,    False),
    ('38)  $95,000.01 -   $100,000.00',    95000,   100000,   False),
    ('39) $100,000.01 -   $150,000.00',    100000,  150000,   False),
    ('40) $150,000.01 -   $200,000.00',    150000,  200000,   False),
    ('41) $200,000.01 -   $300,000.00',    200000,  300000,   False),
    ('42) $300,000.01 -   $500,000.00',    300000,  500000,   False),
    ('43) $500,000.01 - $1,000,000.00',    500000,  1000000,  False),
    ('44) $1,000,000.01 - $2,000,000.00',  1000000, 2000000,  False),
    ('45) $2,000,000.01 - $3,000,000.00',  2000000, 3000000,  False),
    ('46) $3,000,000.01 - $4,000,000.00',  3000000, 4000000,  False),
    ('47) $4,000,000.01 - $5,000,000.00',  4000000, 5000000,  False),
    ('48) $5,000,000.01 - $10,000,000.00', 5000000, 10000000, False),
    ('49) ABOVE $10,000,000.00',            10000000,None,    False),
]
# Tuple positions: 0=label, 1=lo_exclusive(None=-inf), 2=hi_inclusive(None=+inf), 3=is_negative_band


# ---------------------------------------------------------------------------
# BAND ASSIGNMENT HELPERS
# ---------------------------------------------------------------------------

def assign_ace_band(curbal) -> int:
    """
    Returns the 0-based index into ACE_BANDS for a given CURBAL value.
    Returns -1 if no band matched (OTHERWISE – should not occur).
    Mirrors the SAS SELECT in DATA SAVG2 for the main ACE report.
    """
    if curbal is None:
        return -1
    v = float(curbal)
    if v < 0:
        return 0   # NOACC0
    if v == 0:
        return 1   # NOACCX
    # Bands 2..67 use (lo, hi] exclusive-lo, inclusive-hi
    # ACE_BANDS[i] = (label, nk, rk, lo, hi, is_neg)
    for i, band in enumerate(ACE_BANDS[2:], start=2):
        lo  = band[3]
        hi  = band[4]
        neg = band[5]
        if neg:
            continue
        if hi is None:
            if v > lo:
                return i
        else:
            if lo < v <= hi:
                return i
    return -1   # OTHERWISE (no match)


def assign_gendata_band(curbal) -> int:
    """
    Returns the 0-based index into GENDATA_BANDS for a given CURBAL.
    Returns -1 for no match (OTHERWISE – not printed).
    Mirrors the SAS SELECT in %GENDATA / DATA RPTDATA.
    """
    if curbal is None:
        return -1
    v = float(curbal)
    if v < 0:
        return 0   # NOACC0
    # Bands 1..48: lo_exclusive to hi_inclusive  (band[1], band[2]]
    # Band 1: 0 <= v <= 5  (lo=0 inclusive for the zero case in GENDATA)
    for i, band in enumerate(GENDATA_BANDS[1:], start=1):
        lo  = band[1]   # exclusive lower bound
        hi  = band[2]   # inclusive upper bound (None = +inf)
        neg = band[3]
        if neg:
            continue
        if hi is None:
            if v > lo:
                return i
        else:
            if lo < v <= hi:
                return i
    return -1


# ---------------------------------------------------------------------------
# SUMMARISE into band counts & amounts
# ---------------------------------------------------------------------------

def summarise_ace(df: pl.DataFrame) -> dict:
    """
    Equivalent to the DATA SAVG2 step that accumulates NOACC0..NOACC67,
    RANGE0..RANGE67 using SAS retained variables via END=LAST.
    Returns a dict {band_index: (noacc, range_sum)}.
    """
    counts: dict[int, list] = {i: [0, 0.0] for i in range(len(ACE_BANDS))}
    counts[-1] = [0, 0.0]   # OTHERWISE bucket

    for row in df.iter_rows(named=True):
        v   = row.get('CURBAL')
        idx = assign_ace_band(v)
        amt = float(v) if v is not None else 0.0
        if idx >= 0:
            counts[idx][0] += 1
            counts[idx][1] += amt
        else:
            counts[-1][0] += 1
            counts[-1][1] += amt

    return counts


def summarise_gendata(df: pl.DataFrame) -> tuple[dict, dict]:
    """
    Equivalent to the %GENDATA DATA RPTDATA step.
    Returns (noacc_dict, cabal_dict) both keyed by 0-based band index.
    RANGE is always equal to CABAL in the SAS code (RANGE_i+CURBAL same as CABAL_i+CURBAL).
    """
    noacc: dict[int, int]   = {i: 0   for i in range(len(GENDATA_BANDS))}
    cabal: dict[int, float] = {i: 0.0 for i in range(len(GENDATA_BANDS))}

    for row in df.iter_rows(named=True):
        v   = row.get('CURBAL')
        idx = assign_gendata_band(v)
        amt = float(v) if v is not None else 0.0
        if idx >= 0:
            noacc[idx] += 1
            cabal[idx] += amt
        # OTHERWISE is ignored (no output)

    return noacc, cabal


# ---------------------------------------------------------------------------
# REPORT: ACE BREAKDOWN (main SAVG2 report – 68 bands, 2 columns)
# ---------------------------------------------------------------------------

def report_ace(df: pl.DataFrame, rdate: str):
    """
    DATA _NULL_ / FILE PRINT HEADER=NEWPAGE  for the main ACE report.
    Columns: @1 LABEL  @45 NOACC COMMA11.0  @56 RANGE COMMA18.2
    Separator at col 1-105.
    """
    rw = ReportWriter(OUTPUT_ACE)

    def newpage():
        rw.new_page([
            f"{'PROGRAM-ID : DALMPBB3':<39}P U B L I C   B A N K   B E R H A D",
            f"{'':>36}BREAKDOWN OF ACE ACCOUNTS AS AT {rdate}",
            '',
            f"{'':>59}OUTSTANDING BALANCE",
            f"{'':>4}DEPOSIT RANGE{'':>35}NO OF ACCT{'':>15}TOTAL (RM)",
            '',
            '',
        ])

    newpage()

    counts = summarise_ace(df)

    totacc = sum(counts[i][0] for i in range(len(ACE_BANDS)) if i >= 0)
    totamt = sum(counts[i][1] for i in range(len(ACE_BANDS)) if i >= 0)

    for i, band in enumerate(ACE_BANDS):
        label = band[0]
        n     = counts[i][0]
        amt   = counts[i][1]
        rw.write(' ', f"{label:<44}{n:>11,}      {amt:>18,.2f}")

    sep = '-' * 42 + '-' * 42 + '-' * 25
    rw.write(' ', sep)
    rw.write(' ', f"{'    TOTAL              ':<44}{totacc:>11,}      {totamt:>18,.2f}")
    rw.write(' ', sep)

    rw.save()
    print(f"[DALMPBB3] ACE report written -> {OUTPUT_ACE}")


# ---------------------------------------------------------------------------
# %GENDATA equivalent – reusable function for 3 report variants
# ---------------------------------------------------------------------------

def gendata_report(df: pl.DataFrame, rptitle: str, rdate: str, output_path: str):
    """
    Equivalent to the SAS %GENDATA macro.
    Summarises RPTDATA (49 bands) and writes a report with 3 value columns:
      @38 NOACC COMMA11.0   @60 CABAL COMMA18.2   @90 RANGE COMMA18.2
    Note: In SAS, RANGE_i = CABAL_i (both accumulate +CURBAL), so RANGE == CABAL.

    Header (NEWPAGE):
      @1  'PROGRAM-ID : DALMPBB3'  @40 'P U B L I C   B A N K   B E R H A D'
      @37 &RPTITLE &RDATE
      blank
      @7  'DEPOSIT RANGE'  @40 'NO OF ACCOUNT'  @62 'AMOUNT OUTSTANDING (RM)'  @102 'TOTAL'
      blank blank
    """
    rw = ReportWriter(output_path)

    def newpage():
        rw.new_page([
            f"{'PROGRAM-ID : DALMPBB3':<39}P U B L I C   B A N K   B E R H A D",
            f"{'':>36}{rptitle}{rdate}",
            '',
            f"{'':>6}DEPOSIT RANGE{'':>27}NO OF ACCOUNT{'':>22}AMOUNT OUTSTANDING (RM){'':>40}TOTAL",
            '',
            '',
        ])

    newpage()

    noacc, cabal = summarise_gendata(df)

    # TOTACC note: SAS sums in a specific order that includes all 49 bands.
    # TOTCA / TOTAMT both equal the sum of all CABAL values (RANGE==CABAL).
    totacc = sum(noacc.values())
    totca  = sum(cabal.values())
    totamt = totca   # TOTAMT = SUM(TOTCA)

    for i, band in enumerate(GENDATA_BANDS):
        label = band[0]
        n     = noacc[i]
        ca    = cabal[i]
        rg    = ca     # RANGE_i == CABAL_i
        # @1 label  @38 NOACC  @60 CABAL  @90 RANGE
        rw.write(' ',
            f"{label:<37}"
            f"{n:>11,}"
            f"{'':>10}"
            f"{ca:>18,.2f}"
            f"{'':>10}"
            f"{rg:>18,.2f}"
        )

    sep = (
        '-' * 41
        + '-' * 41
        + '-' * 25
    )
    rw.write(' ', sep)
    rw.write(' ',
        f"{'    TOTAL              ':<37}"
        f"{totacc:>11,}"
        f"{'':>10}"
        f"{totca:>18,.2f}"
        f"{'':>10}"
        f"{totamt:>18,.2f}"
    )
    rw.write(' ', sep)

    rw.save()
    print(f"[DALMPBB3] {rptitle.strip()} report written -> {output_path}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    meta = get_meta()
    print(f"[DALMPBB3] rdate={meta['RDATE']}  NOWK={meta['NOWK']}")

    # DATA SAVG BONUS BASIC BASIC55 BONUC
    datasets = load_and_split()
    for name, df in datasets.items():
        print(f"[DALMPBB3] {name}: {len(df)} records")

    # -----------------------------------------------------------------------
    # Main ACE report (SAVG2 – 68 bands)
    # -----------------------------------------------------------------------
    report_ace(datasets['SAVG'], meta['RDATE'])

    # -----------------------------------------------------------------------
    # %GENDATA calls (49 bands each)
    # -----------------------------------------------------------------------

    # BREAKDOWN OF PB CURRENTLINK A/C
    gendata_report(
        df          = datasets['BONUC'],
        rptitle     = 'BREAKDOWN OF PB CURRENTLINK A/C AS AT ',
        rdate       = meta['RDATE'],
        output_path = OUTPUT_BONUC,
    )

    # BREAKDOWN OF BASIC CURRENT A/C
    gendata_report(
        df          = datasets['BASIC'],
        rptitle     = 'BREAKDOWN OF BASIC CURRENT A/C AS AT ',
        rdate       = meta['RDATE'],
        output_path = OUTPUT_BASIC,
    )

    # BREAKDOWN OF BASIC 55 CURRENT A/C
    gendata_report(
        df          = datasets['BASIC55'],
        rptitle     = 'BREAKDOWN OF BASIC 55 CURRENT A/C AS AT ',
        rdate       = meta['RDATE'],
        output_path = OUTPUT_BASIC55,
    )

    print("[DALMPBB3] Done.")


if __name__ == "__main__":
    main()
