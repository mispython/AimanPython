#!/usr/bin/env python3
"""
Program : DALMPBB2.py
Report  : ACE REPORT FOR PRODUCT DEVELOPMENT
Date    : 29.11.97
Purpose : Manipulate the extracted savings and current account data sets.
          Profile on ACE accounts summarised by deposit range, account type,
          age group and race via PROC TABULATE equivalent.
"""

import duckdb
import polars as pl
import os
from datetime import datetime, date

# ---------------------------------------------------------------------------
# DEPENDENCY: PBBDPFMT – product lists and format definitions
# ---------------------------------------------------------------------------
from PBBDPFMT import ProductLists

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

# Output report file (ASA carriage-control text)
OUTPUT_REPORT  = os.path.join(OUTPUT_DIR, "DALMPBB2_REPORT.txt")
# Diagnostic print of zero/null OPENDT records (mirrors DATA AAA / PROC PRINT)
OUTPUT_AAA     = os.path.join(OUTPUT_DIR, "DALMPBB2_AAA.txt")
# Diagnostic print of unmatched DEPRANGE records (mirrors PROC PRINT DATA=SAVG3)
OUTPUT_SAVG3   = os.path.join(OUTPUT_DIR, "DALMPBB2_SAVG3.txt")

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
PAGE_LENGTH = 60   # lines per page (ASA carriage control)

# ACE product codes (from PBBDPFMT ProductLists.ACE_PRODUCTS)
# Original SAS: IF PRODUCT = 150 OR PRODUCT = 151 OR PRODUCT = 152 OR PRODUCT = 181
ACE_PRODUCTS = ProductLists.ACE_PRODUCTS   # {40, 42, 43, 150, 151, 152, 181}
# The SAS filter is specifically 150,151,152,181 – intersect with ProductLists
ACE_FILTER   = {150, 151, 152, 181}

# ---------------------------------------------------------------------------
# FORMAT DEFINITIONS
# ---------------------------------------------------------------------------

def fmt_race(val: str) -> str:
    """$RACE format"""
    if val == '1': return 'MALAY'
    if val == '2': return 'CHINESE'
    if val == '3': return 'INDIAN'
    return 'OTHERS'


def fmt_hundred(val) -> str:
    """
    PICTURE HUNDRED format:
      LOW-<0  = '0,000,009.99' (PREFIX='-' MULT=1)
      0-HIGH  = '0,000,009.99' (MULT=1)
    Renders a value as a 12-character picture with optional leading '-'.
    """
    if val is None:
        return ' ' * 12
    v = float(val)
    negative = v < 0
    formatted = f"{abs(v):>12,.2f}"
    # Apply picture mask: prefix '-' if negative
    if negative:
        formatted = f"-{formatted.lstrip()}"
        formatted = formatted.rjust(13)
    return formatted


def assign_deprange(curbal) -> tuple[str, bool]:
    """
    Returns (deprange_label, is_other) mirroring the SAS SELECT on CURBAL.
    is_other=True means the OTHERWISE branch was taken -> output to SAVG3.
    """
    if curbal is None:
        return ('>RM200,000 AND MISSING', True)
    v = float(curbal)
    if v < 0:
        return ('  NEGATIVE BALANCE     ', False)
    if v <= 500:
        return ('  RM500 & BELOW        ', False)
    if v <= 1000:
        return ('>RM    500 - RM  1,000', False)
    if v <= 1500:
        return ('>RM  1,000 - RM  1,500', False)
    if v <= 2000:
        return ('>RM  1,500 - RM  2,000', False)
    if v <= 3000:
        return ('>RM  2,000 - RM  3,000', False)
    if v <= 4000:
        return ('>RM  3,000 - RM  4,000', False)
    if v <= 5000:
        return ('>RM  4,000 - RM  5,000', False)
    if v <= 10000:
        return ('>RM  5,000 - RM 10,000', False)
    if v <= 30000:
        return ('>RM 10,000 - RM 30,000', False)
    if v <= 50000:
        return ('>RM 30,000 - RM 50,000', False)
    if v <= 75000:
        return ('>RM 50,000 - RM 75,000', False)
    if v <= 100000:
        return ('>RM 75,000 - RM100,000', False)
    if v <= 150000:
        return ('>RM100,000 - RM150,000', False)
    if v <= 200000:
        return ('>RM150,000 - RM200,000', False)
    if v > 200000:
        return ('>RM200,000 AND ABOVE',   False)
    # OTHERWISE (should not be reached given the above coverage)
    return ('>RM200,000 AND MISSING', True)


DEPRANGE_ORDER = [
    '  NEGATIVE BALANCE     ',
    '  RM500 & BELOW        ',
    '>RM    500 - RM  1,000',
    '>RM  1,000 - RM  1,500',
    '>RM  1,500 - RM  2,000',
    '>RM  2,000 - RM  3,000',
    '>RM  3,000 - RM  4,000',
    '>RM  4,000 - RM  5,000',
    '>RM  5,000 - RM 10,000',
    '>RM 10,000 - RM 30,000',
    '>RM 30,000 - RM 50,000',
    '>RM 50,000 - RM 75,000',
    '>RM 75,000 - RM100,000',
    '>RM100,000 - RM150,000',
    '>RM150,000 - RM200,000',
    '>RM200,000 AND ABOVE',
]

ACCTYPE_ORDER  = ['PERSONAL', 'JOINT', 'OTHER']
AGEGROUP_ORDER = ['18-30', '31-50', '> 50']
RACE_ORDER     = ['MALAY', 'CHINESE', 'INDIAN', 'OTHERS']

# ---------------------------------------------------------------------------
# ASA REPORT WRITER
# ---------------------------------------------------------------------------

def asa_line(cc: str, text: str) -> str:
    return f"{cc}{text}\n"


class ReportWriter:
    """Simple ASA carriage-control report writer with automatic page breaks."""

    def __init__(self, filepath: str, titles: list[str]):
        self.filepath    = filepath
        self.titles      = titles
        self.lines: list[str] = []
        self.body_count  = 0
        self.page_num    = 1
        self._emit_page_header()

    def _emit_page_header(self):
        self.lines.append(asa_line('1', self.titles[0] if self.titles else ''))
        for t in self.titles[1:]:
            self.lines.append(asa_line(' ', t))
        self.lines.append(asa_line(' ', ''))
        self.body_count = 0

    def write(self, cc: str, text: str):
        if self.body_count >= PAGE_LENGTH:
            self.page_num += 1
            self._emit_page_header()
        self.lines.append(asa_line(cc, text))
        self.body_count += 1

    def blank(self, n: int = 1):
        for _ in range(n):
            self.write(' ', '')

    def save(self):
        with open(self.filepath, 'w', encoding='utf-8') as f:
            f.writelines(self.lines)


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
      RDATE    : DD/MM/YY string (DDMMYY8. -> 'DD/MM/YY')
      PREMON   : zero-padded 2-digit previous month
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
    premon = f"{mth:02d}"

    return {
        'NOWK':     nowk,
        'REPTYEAR': str(d.year),
        'REPTMON':  f"{d.month:02d}",
        'REPTDAY':  f"{d.day:02d}",
        'RDATE':    d.strftime('%d/%m/%y'),   # DDMMYY8. -> 'DD/MM/YY'
        'PREMON':   premon,
        'date_obj': d,
    }


# ---------------------------------------------------------------------------
# DATA AAA – records with OPENDT = 0 or null (diagnostic print)
# ---------------------------------------------------------------------------

def print_aaa():
    """
    DATA AAA: SET DEPOSIT.CURRENT; IF OPENDT = 0 OR OPENDT = . THEN OUTPUT;
    PROC PRINT DATA=AAA;
    Writes diagnostic listing to OUTPUT_AAA.
    """
    con = duckdb.connect()
    aaa = con.execute(f"""
        SELECT *
        FROM read_parquet('{CURRENT_PATH}')
        WHERE OPENDT = 0 OR OPENDT IS NULL
    """).pl()
    con.close()

    with open(OUTPUT_AAA, 'w', encoding='utf-8') as f:
        f.write(f"1{'PROC PRINT: DEPOSIT.CURRENT WHERE OPENDT=0 OR OPENDT IS NULL'}\n")
        f.write(f" {'-' * 80}\n")
        if aaa.is_empty():
            f.write(f" {'(No records)'}\n")
        else:
            f.write(f" {aaa.to_string()}\n")

    print(f"[DALMPBB2] AAA diagnostic written -> {OUTPUT_AAA}  ({len(aaa)} records)")


# ---------------------------------------------------------------------------
# DATA SAVG – filter and enrich ACE current account records
# ---------------------------------------------------------------------------

def build_savg(meta: dict) -> pl.DataFrame:
    """
    DATA SAVG:
      SET DEPOSIT.CURRENT
      WHERE OPENIND NOT IN ('B','C','P')
      IF PRODUCT IN (150,151,152,181):
        - ACCYTD = 1 if opened in report year and not yet closed
        - AGE derived from BDATE; default 51 if unknown
    """
    reptyear = int(meta['REPTYEAR'])
    rdate_str = meta['RDATE']   # DD/MM/YY

    con = duckdb.connect()
    raw = con.execute(f"""
        SELECT *
        FROM read_parquet('{CURRENT_PATH}')
        WHERE OPENIND NOT IN ('B','C','P')
          AND PRODUCT IN (150, 151, 152, 181)
    """).pl()
    con.close()

    if raw.is_empty():
        return pl.DataFrame()

    records = []
    for row in raw.iter_rows(named=True):
        opendt  = row.get('OPENDT')  or 0
        closedt = row.get('CLOSEDT') or 0
        bdate   = row.get('BDATE')   or 0

        # ACCYTD: opened this year and not yet closed
        accytd = 0
        if opendt != 0 and closedt == 0:
            try:
                opendt_str = f"{int(opendt):011d}"[:8]   # MMDDYYYY
                open_year  = int(opendt_str[4:8])
                if open_year == reptyear:
                    accytd = 1
            except Exception:
                pass

        # AGE: derived from BDATE
        age = 51
        if bdate != 0:
            try:
                bdate_str = f"{int(bdate):011d}"[:8]   # MMDDYYYY
                birth_year = int(bdate_str[4:8])
                age = reptyear - birth_year
                if age < 0:
                    age = 51
            except Exception:
                age = 51

        records.append({
            'BRANCH':  row.get('BRANCH'),
            'ACCTNO':  row.get('ACCTNO'),
            'PRODUCT': row.get('PRODUCT'),
            'CURBAL':  row.get('CURBAL'),
            'RACE':    str(row.get('RACE') or ''),
            'PURPOSE': str(row.get('PURPOSE') or ''),
            'OPENDT':  opendt,
            'ACCYTD':  accytd,
            'AGE':     age,
        })

    return pl.DataFrame(records)


# ---------------------------------------------------------------------------
# DATA SAVG2 / SAVG3 – assign DEPRANGE, AGEGROUP, ACCTYPE
# ---------------------------------------------------------------------------

def build_savg2_savg3(savg: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    DATA SAVG2 SAVG3:
      Assigns AGEGROUP, ACCTYPE, DEPRANGE.
      Records hitting the OTHERWISE branch of DEPRANGE go to SAVG3 (and also SAVG2).
    """
    savg2_rows = []
    savg3_rows = []

    for row in savg.iter_rows(named=True):
        age     = row.get('AGE') or 51
        purpose = row.get('PURPOSE') or ''
        curbal  = row.get('CURBAL')

        # AGEGROUP
        if 17 < age < 31:
            agegroup = '18-30'
        elif 30 < age < 51:
            agegroup = '31-50'
        else:
            agegroup = '> 50'

        # ACCTYPE
        if purpose in ('1', '4'):
            acctype = 'PERSONAL'
        elif purpose == '2':
            acctype = 'JOINT'
        else:
            acctype = 'OTHER'

        # DEPRANGE
        deprange, is_other = assign_deprange(curbal)

        enriched = dict(row)
        enriched['AGEGROUP'] = agegroup
        enriched['ACCTYPE']  = acctype
        enriched['DEPRANGE'] = deprange

        if is_other:
            savg3_rows.append(enriched)
        savg2_rows.append(enriched)

    savg2 = pl.DataFrame(savg2_rows) if savg2_rows else pl.DataFrame()
    savg3 = pl.DataFrame(savg3_rows) if savg3_rows else pl.DataFrame()
    return savg2, savg3


# ---------------------------------------------------------------------------
# PROC PRINT DATA=SAVG3  (diagnostic)
# ---------------------------------------------------------------------------

def print_savg3(savg3: pl.DataFrame):
    with open(OUTPUT_SAVG3, 'w', encoding='utf-8') as f:
        f.write(f"1{'PROC PRINT: SAVG3 (OTHERWISE DEPRANGE records)'}\n")
        f.write(f" {'-' * 80}\n")
        if savg3.is_empty():
            f.write(f" {'(No records)'}\n")
        else:
            f.write(f" {savg3.to_string()}\n")
    print(f"[DALMPBB2] SAVG3 diagnostic written -> {OUTPUT_SAVG3}  ({len(savg3)} records)")


# ---------------------------------------------------------------------------
# PROC SUMMARY equivalent – aggregate SAVG2 by CLASS variables
# ---------------------------------------------------------------------------

def summarise_savg2(savg2: pl.DataFrame) -> pl.DataFrame:
    """
    PROC SUMMARY DATA=SAVG2 NWAY;
      CLASS DEPRANGE ACCTYPE AGEGROUP RACE;
      VAR CURBAL ACCYTD;
      OUTPUT OUT=SAVG2 (RENAME=(_FREQ_=NOACCT) DROP=_TYPE_) SUM=;

    Then the DATA SAVG2 step computes cumulative AMOUNT and CNT with
    reset at LAST.DEPRANGE – but this was only used for the commented-out
    AVGAMT1 calculation. We keep the summed dataset as the tabulate input.
    """
    if savg2.is_empty():
        return pl.DataFrame()

    # Apply $RACE format before grouping
    savg2 = savg2.with_columns(
        pl.col("RACE").map_elements(fmt_race, return_dtype=pl.Utf8).alias("RACE_FMT")
    )

    agg = (
        savg2
        .group_by(["DEPRANGE", "ACCTYPE", "AGEGROUP", "RACE_FMT"])
        .agg([
            pl.len().alias("NOACCT"),
            pl.col("CURBAL").sum().alias("CURBAL"),
            pl.col("ACCYTD").sum().alias("ACCYTD"),
        ])
        .sort(["DEPRANGE", "ACCTYPE", "AGEGROUP", "RACE_FMT"])
    )

    # DATA SAVG2 retained-total logic (AMOUNT+CURBAL, CNT+NOACCT reset at LAST.DEPRANGE)
    # This was used for the commented-out AVGAMT1 = AMOUNT/CNT.
    # The current PROC TABULATE uses CURBAL directly, so we just pass agg through.
    # /*   AVGAMT1 = AMOUNT / CNT; */
    return agg


# ---------------------------------------------------------------------------
# PROC TABULATE equivalent
# ---------------------------------------------------------------------------

def report_tabulate(agg: pl.DataFrame, rdate: str):
    """
    PROC TABULATE DATA=SAVG2 MISSING;
      TABLE DEPRANGE ALL,
            NOACCT*(SUM PCTSUM)
            ACCYTD*(SUM PCTSUM)
            CURBAL*(SUM PCTSUM PCTSUM<NOACCT>)
            ACCTYPE*NOACCT*SUM
            AGEGROUP*NOACCT*SUM
            RACE*NOACCT*SUM
            / RTS=28 BOX='ACE ACCOUNTS' PRINTMISS;
    """
    titles = [
        'P U B L I C   B A N K   B E R H A D',
        'FOR PRODUCT DEVELOPMENT & MARKETING',
        f'PROFILE ON ACE ACCOUNTS AS AT {rdate}',
    ]
    rw = ReportWriter(OUTPUT_REPORT, titles)

    # Grand totals
    total_noacct = int(agg["NOACCT"].sum()) if not agg.is_empty() else 0
    total_accytd = float(agg["ACCYTD"].sum() or 0)
    total_curbal = float(agg["CURBAL"].sum() or 0)

    # Column header layout  (RTS=28 -> row label width 28)
    RTS = 28
    sep = '-' * 160

    rw.write(' ', sep)
    rw.write(' ', (
        f"{'ACE ACCOUNTS':<{RTS}}"
        f"{'NO OF A/C':>12}{'%':>8}"
        f"{'OPEN YTD':>12}{'%':>8}"
        f"{'AMOUNT':>20}{'%':>8}{'AVG AMT/ACCOUNT':>16}"
        f"{'PERSONAL':>10}{'JOINT':>10}{'OTHER':>10}"
        f"{'18-30':>10}{'31-50':>10}{'> 50':>10}"
        f"{'MALAY':>10}{'CHINESE':>10}{'INDIAN':>10}{'OTHERS':>10}"
    ))
    rw.write(' ', sep)

    def row_for_subset(subset: pl.DataFrame, label: str):
        n      = int(subset["NOACCT"].sum())
        ytd    = float(subset["ACCYTD"].sum() or 0)
        bal    = float(subset["CURBAL"].sum() or 0)
        pct_n  = (n   / total_noacct * 100) if total_noacct else 0.0
        pct_y  = (ytd / total_accytd * 100) if total_accytd else 0.0
        pct_b  = (bal / total_curbal * 100) if total_curbal else 0.0
        # PCTSUM<NOACCT> = CURBAL.SUM / NOACCT.SUM  (avg amt per account)
        avg_amt = bal / n if n else 0.0

        # ACCTYPE breakdown
        def _cnt_by(col: str, cats: list[str]) -> list[int]:
            grp = subset.group_by(col).agg(pl.col("NOACCT").sum().alias("s"))
            d   = dict(zip(grp[col].to_list(), grp["s"].to_list()))
            return [int(d.get(c, 0)) for c in cats]

        acc_cnts  = _cnt_by("ACCTYPE",  ACCTYPE_ORDER)
        age_cnts  = _cnt_by("AGEGROUP", AGEGROUP_ORDER)
        race_cnts = _cnt_by("RACE_FMT", RACE_ORDER)

        acc_str  = "".join(f"{c:>10,}" for c in acc_cnts)
        age_str  = "".join(f"{c:>10,}" for c in age_cnts)
        race_str = "".join(f"{c:>10,}" for c in race_cnts)

        line = (
            f"{label:<{RTS}}"
            f"{n:>12,}{pct_n:>8.2f}"
            f"{ytd:>12,.0f}{pct_y:>8.2f}"
            f"{bal:>20,.2f}{pct_b:>8.2f}"
            f"{fmt_hundred(avg_amt):>16}"
            f"{acc_str}{age_str}{race_str}"
        )
        rw.write(' ', line)

    for dr in DEPRANGE_ORDER:
        subset = agg.filter(pl.col("DEPRANGE") == dr)
        # PRINTMISS: always print all rows, even if empty
        if subset.is_empty():
            # Emit a zero row (PRINTMISS equivalent)
            zero_line = (
                f"{dr:<{RTS}}"
                f"{'0':>12}{'0.00':>8}"
                f"{'0':>12}{'0.00':>8}"
                f"{'0.00':>20}{'0.00':>8}"
                f"{'0.00':>16}"
                + "".join(f"{'0':>10}" for _ in range(len(ACCTYPE_ORDER) +
                                                        len(AGEGROUP_ORDER) +
                                                        len(RACE_ORDER)))
            )
            rw.write(' ', zero_line)
        else:
            row_for_subset(subset, dr)

    # TOTAL row
    rw.write(' ', sep)
    row_for_subset(agg, 'TOTAL')
    rw.write(' ', sep)

    rw.save()
    print(f"[DALMPBB2] Tabulate report written -> {OUTPUT_REPORT}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    meta = get_meta()
    print(f"[DALMPBB2] rdate={meta['RDATE']}  reptyear={meta['REPTYEAR']}  NOWK={meta['NOWK']}")

    # DATA AAA / PROC PRINT  (diagnostic – zero/null OPENDT records)
    print_aaa()

    # DATA SAVG
    savg = build_savg(meta)
    print(f"[DALMPBB2] SAVG records (ACE filtered): {len(savg)}")

    # DATA SAVG2 / SAVG3
    savg2, savg3 = build_savg2_savg3(savg)
    print(f"[DALMPBB2] SAVG2 records: {len(savg2)}  SAVG3 (otherwise): {len(savg3)}")

    # PROC PRINT DATA=SAVG3
    print_savg3(savg3)

    # PROC SORT DATA=SAVG2; BY DEPRANGE ACCTYPE AGEGROUP RACE
    # (handled inside summarise_savg2 via group_by sort)

    # PROC SUMMARY + DATA SAVG2 (running totals)
    agg = summarise_savg2(savg2)

    # PROC TABULATE
    report_tabulate(agg, meta['RDATE'])

    print("[DALMPBB2] Done.")


if __name__ == "__main__":
    main()
