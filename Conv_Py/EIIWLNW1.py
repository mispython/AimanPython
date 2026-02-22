# !/usr/bin/env python3
"""
PROGRAM : EIIWLNW1
JCL     : EIIWLNW1 JOB MSGCLASS=X,MSGLEVEL=(1,1)
PURPOSE : EXTRACTION OF LOANS INFORMATION FOR DATA WAREHOUSE. PROCESSES LOAN, HIRE PURCHASE (HP),
            AND OVERDRAFT RECORDS FROM MULTIPLE SOURCES, APPLIES CLASSIFICATIONS,
            COMPUTES DERIVED FIELDS, AND WRITES OUTPUT PARQUET DATASETS.
DATE    : 21.10.99
FUNCTION: LOANS EXTRACTION
"""

import struct
import datetime
import math
import re
import sys
import os
import polars as pl
import duckdb
from pathlib import Path

# ---------------------------------------------------------------------------
# Path Configuration
# ---------------------------------------------------------------------------
INPUT_DIR    = Path("input")
OUTPUT_DIR   = Path("output")
BNM_DIR      = INPUT_DIR / "bnm"          # SAP.PIBB.SASDATA
BNM1_DIR     = INPUT_DIR / "bnm1"         # SAP.PIBB.MNILN(0)
MNITB_DIR    = INPUT_DIR / "mnitb"        # SAP.PIBB.MNITB(0)
CIS_DIR      = INPUT_DIR / "cis"          # SAP.PBB.CISBEXT.LN
CISD_DIR     = INPUT_DIR / "cisd"         # SAP.PBB.CISBEXT.DP
BNM2_DIR     = INPUT_DIR / "bnm2"         # SAP.PIBB.MNILN.MTDINT
MNICRM_DIR   = INPUT_DIR / "mnicrm"       # SAP.PIBB.CRMCUMLN
MNINPL_DIR   = INPUT_DIR / "mninpl"       # SAP.PBB.NPL.SASDATA
ODGP3_DIR    = INPUT_DIR / "odgp3"        # SAP.PIBB.MNILIMT(0)
WOFF_DIR     = INPUT_DIR / "woff"         # SAP.PIBB.NPL.HP.SASDATA
HPWO_DIR     = INPUT_DIR / "hpwo"         # SAP.PIBB.HP.WOFF
MISMLN_DIR   = INPUT_DIR / "mismln"       # SAP.PIBB.MLN.SASDATA
FEEYTD_DIR   = INPUT_DIR / "feeytd"       # SAP.PIBB.FEEYTD
LNFILE_DIR   = INPUT_DIR / "lnfile"       # SAP.PBB.LNMIG
HIST_DIR     = INPUT_DIR / "hist"         # SAP.PBB.HIST.BASE
HP_OUT_DIR   = OUTPUT_DIR / "hp"          # SAP.PIBB.HPDATAWH
LOAN_OUT_DIR = OUTPUT_DIR / "loan"        # SAP.PIBB.LNDATAWH

# Binary input files (fixed-width / mainframe format)
HISTFILE     = INPUT_DIR / "histfile.dat"  # RBP2.B033.HISTFILE.FRMJAN23
REFNOTE_FILE = INPUT_DIR / "refnote.dat"   # RBP2.B033.LN.MIGR.CCRISFL(0)
PAYFI_FILE   = INPUT_DIR / "payfi.dat"     # RBP2.SAS.B033.PAYSFILE
FEED31_FILE  = INPUT_DIR / "feed31.dat"    # RBP2.B033.DEC31.FEE

for d in [OUTPUT_DIR, HP_OUT_DIR, LOAN_OUT_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# OPTIONS YEARCUTOFF=1950 NOCENTER NODATE NONUMBER MISSING=0
MISSING_NUM = 0

# ---------------------------------------------------------------------------
# %INC PGM(PBBDPFMT,PBBLNFMT,PBBELF)
# Dependency formats from PBBDPFMT, PBBLNFMT, PBBELF
# These are referenced format catalogs; their mappings are embedded below
# where used throughout the program.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# PROC FORMAT  $FISSCD
# ---------------------------------------------------------------------------
FISSCD_MAP = {
    '0440': '0440', '0441': '0440', '0460': '0460',
    '1110': '0110', '1120': '0120', '1131': '0131', '1139': '0132',
    '1140': '0139', '1210': '0110', '1220': '0120', '1230': '0132',
    '1290': '0139', '2111': '0311', '2112': '0312', '2113': '0313',
    '2114': '0314', '2115': '0315', '2116': '0316', '2121': '0321',
    '2122': '0322', '2123': '0323', '2124': '0324', '2129': '0329',
    '2210': '0313', '2211': '0314', '2212': '0315', '2213': '0316',
    '2221': '0321', '2222': '0322', '2223': '0323', '2224': '0324',
    '2229': '0329', '2310': '0313', '2311': '0314', '2312': '0315',
    '2313': '0316', '2321': '0321', '2322': '0322', '2323': '0323',
    '2324': '0324', '2329': '0329', '3100': '0211', '3101': '0212',
    '3200': '0200', '3900': '0200', '4100': '0430', '4200': '0420',
    '4300': '0410', '5100': '0390', '5200': '0390', '5300': '0470',
    '5400': '0311', '5401': '0312', '5402': '0313', '5403': '0314',
    '5404': '0315', '5405': '0316', '5406': '0321', '5407': '0322',
    '5408': '0323', '5409': '0324', '5410': '0329', '5411': '0311',
    '5412': '0312', '5413': '0313', '5414': '0314', '5415': '0315',
    '5416': '0316', '5417': '0321', '5418': '0322', '5419': '0323',
    '5420': '0324', '5421': '0329', '5500': '0410', '5501': '0410',
    '5600': '0990', '5700': '0990', '5800': '0990', '9000': '0990',
    '9001': '0990',
}

# ---------------------------------------------------------------------------
# Date helpers  (SAS date epoch = 01-JAN-1960)
# ---------------------------------------------------------------------------
SAS_EPOCH = datetime.date(1960, 1, 1)


def sas_to_date(v) -> datetime.date | None:
    if v is None or v == 0:
        return None
    try:
        return SAS_EPOCH + datetime.timedelta(days=int(v))
    except (ValueError, OverflowError):
        return None


def date_to_sas(dt: datetime.date | None):
    if dt is None:
        return None
    return (dt - SAS_EPOCH).days


def parse_ddmmyy8(s: str) -> int | None:
    """Parse DDMMYYYY or DD/MM/YY or DD-MM-YY -> SAS int."""
    s = str(s).strip()
    for fmt in ('%d%m%Y', '%d/%m/%y', '%d-%m-%y', '%d/%m/%Y', '%d-%m-%Y'):
        try:
            return date_to_sas(datetime.datetime.strptime(s, fmt).date())
        except ValueError:
            pass
    return None


def fmt_ddmmyy8(v) -> str:
    dt = sas_to_date(v)
    return dt.strftime('%d/%m/%y') if dt else '        '


def parse_mmddyy8(s: str) -> int | None:
    """Parse MMDDYYYY or MM/DD/YY -> SAS int."""
    s = str(s).strip()
    for fmt in ('%m%d%Y', '%m/%d/%y', '%m/%d/%Y', '%m-%d-%Y'):
        try:
            return date_to_sas(datetime.datetime.strptime(s, fmt).date())
        except ValueError:
            pass
    return None


def decode_z11_date_mmddyy(packed_int: int) -> int | None:
    """
    Decode a SAS internal date stored as integer in MMDDYYYY format
    (PUT(x, Z11.) gives 11-char string; first 8 chars = MMDDYYYY).
    """
    s = f"{int(packed_int):011d}"
    return parse_mmddyy8(s[:8])


def decode_z11_date_ddmmyy(packed_int: int) -> int | None:
    s = f"{int(packed_int):011d}"
    return parse_ddmmyy8(s[:8])


def parse_zdb13_2(raw: bytes) -> float:
    """ZDB13.2 -- zoned decimal, 13 digits, 2 implied decimals."""
    try:
        s = raw.decode('cp037', errors='replace').strip()
        return float(s) / 100.0
    except (ValueError, UnicodeDecodeError):
        return 0.0


def decode_pd(raw: bytes, d: int = 0) -> float:
    """Decode IBM packed-decimal (PD) bytes."""
    if not raw or all(b == 0 for b in raw):
        return 0.0
    h = raw.hex()
    sign = h[-1].upper()
    try:
        val = int(h[:-1])
    except ValueError:
        return 0.0
    if sign == 'D':
        val = -val
    return val / (10 ** d)


# ---------------------------------------------------------------------------
# DAYARR -> MTHARR conversion (SELECT WHEN ladder) — LOAN version (max 24 mth)
# ---------------------------------------------------------------------------
def dayarr_to_mtharr_loan(dayarr) -> int:
    """Replicate SELECT WHEN ladder in DATA LOAN (up to 24 months)."""
    if dayarr is None:
        return 0
    d = int(dayarr)
    if d > 729: return int((d / 365) * 12)
    if d > 698: return 23
    if d > 668: return 22
    if d > 638: return 21
    if d > 608: return 20
    if d > 577: return 19
    if d > 547: return 18
    if d > 516: return 17
    if d > 486: return 16
    if d > 456: return 15
    if d > 424: return 14
    if d > 394: return 13
    if d > 364: return 12
    if d > 333: return 11
    if d > 303: return 10
    if d > 273: return 9
    if d > 243: return 8
    if d > 213: return 7
    if d > 182: return 6
    if d > 151: return 5
    if d > 121: return 4
    if d > 89:  return 3
    if d > 59:  return 2
    if d > 30:  return 1
    return 0


# ---------------------------------------------------------------------------
# DAYARR -> MTHARR conversion — LNPD / ILNPD version (up to 32 months)
# ---------------------------------------------------------------------------
def dayarr_to_mtharr_pd(dayarr) -> int:
    """Replicate SELECT WHEN ladder in DATA LOAN.ILNPD (up to 32 months)."""
    if dayarr is None:
        return 0
    d = int(dayarr)
    if d > 1004: return int((d / 365) * 12)
    if d > 974:  return 32
    if d > 944:  return 31
    if d > 913:  return 30
    if d > 883:  return 29
    if d > 852:  return 28
    if d > 821:  return 27
    if d > 791:  return 26
    if d > 760:  return 25
    if d > 729:  return 24
    if d > 698:  return 23
    if d > 668:  return 22
    if d > 638:  return 21
    if d > 608:  return 20
    if d > 577:  return 19
    if d > 547:  return 18
    if d > 516:  return 17
    if d > 486:  return 16
    if d > 456:  return 15
    if d > 424:  return 14
    if d > 394:  return 13
    if d > 364:  return 12
    if d > 333:  return 11
    if d > 303:  return 10
    if d > 273:  return 9
    if d > 243:  return 8
    if d > 213:  return 7
    if d > 182:  return 6   # note: SAS code has duplicate WHEN(>182)=6
    if d > 151:  return 5
    if d > 121:  return 4
    if d > 89:   return 3
    if d > 59:   return 2
    if d > 30:   return 1
    return 0


def mtharr_ccris(dayarr) -> int:
    """IF DAYARR > 0 THEN MTHARR_CCRIS = FLOOR(DAYARR/30.00050);  *23-0616"""
    if dayarr is None or dayarr <= 0:
        return 0
    return math.floor(dayarr / 30.00050)


# ---------------------------------------------------------------------------
# PAYEFDT date adjustment (SAS month-end clamping logic)
# ---------------------------------------------------------------------------
def adjust_payefdt(payd: int, paym: int, payy: int) -> int | None:
    """
    Replicate the month-end day clamping applied to PAYEFDT derivation.
    Returns SAS date integer or None.
    """
    if paym == 2 and payd > 29:
        payd = 29 if (payy % 4 == 0) else 28
    elif paym != 2 and payd > 31:
        if paym in (1, 3, 5, 7, 8, 10, 12):
            payd = 31
        elif paym in (4, 6, 9, 11):
            payd = 30
    try:
        return date_to_sas(datetime.date(payy, paym, payd))
    except ValueError:
        return None


def derive_payefdt_from_z11(payeffdt_int: int) -> int | None:
    """
    IF PAYEFFDT NOT IN (.,0) THEN
       PAYEFDTO = (DD || '/' || MM || '/' || YY)
    Then clamp and MDY(PAYM, PAYD, PAYY).
    PUT(PAYEFFDT,Z11.) -> 11 chars
      positions 10-11 = DD (1-based SAS col @10 len 2 -> index 9:11)
      positions 8-9   = MM (index 7:9)
      positions 3-4   = YY (index 2:4)
    """
    s = f"{int(payeffdt_int):011d}"
    try:
        dd   = int(s[9:11])
        mm   = int(s[7:9])
        yy2  = int(s[2:4])
        yyyy = 2000 + yy2 if yy2 < 50 else 1900 + yy2
        return adjust_payefdt(dd, mm, yyyy)
    except (ValueError, IndexError):
        return None


# ---------------------------------------------------------------------------
# REMAINMT calculation (month-count loop from REPTDATE to EXPRDATE)
# ---------------------------------------------------------------------------
def calc_remainmt(reptdate_sas: int, exprdate_sas: int) -> int:
    """
    Replicate the DO UNTIL loop in DATA LOAN.ILNPD:
    Counts calendar months from REPTDATE to EXPRDATE.
    """
    if reptdate_sas is None or exprdate_sas is None:
        return 0
    if reptdate_sas > exprdate_sas:
        return 0

    folmonth   = reptdate_sas
    nummonth   = 0
    reptdt     = sas_to_date(reptdate_sas)
    nextday_base = reptdt.day

    while True:
        folmonth_dt = sas_to_date(folmonth)
        nextmon  = folmonth_dt.month + 1
        nextyear = folmonth_dt.year
        if nextmon > 12:
            nextmon  -= 12
            nextyear += 1
        nextday = nextday_base
        # Month-end clamping
        try:
            if nextday in (29, 30, 31) and nextmon == 2:
                next_dt = datetime.date(nextyear, 3, 1) - datetime.timedelta(days=1)
            elif nextday == 31 and nextmon in (4, 6, 9, 11):
                next_dt = datetime.date(nextyear, nextmon, 30)
            elif nextday == 30 and nextmon in (1, 3, 5, 7, 8, 10, 12):
                next_dt = datetime.date(nextyear, nextmon, 31)
            else:
                next_dt = datetime.date(nextyear, nextmon, nextday)
        except ValueError:
            next_dt = datetime.date(nextyear, nextmon, 1) + datetime.timedelta(days=nextday - 1)

        folmonth = date_to_sas(next_dt)
        nummonth += 1
        if exprdate_sas <= folmonth:
            break
        if nummonth > 9999:   # safety guard
            break

    return nummonth


# ---------------------------------------------------------------------------
# Helper: load parquet via DuckDB returning Polars DataFrame
# ---------------------------------------------------------------------------
def load_parquet(path: Path | str, columns: list[str] | None = None) -> pl.DataFrame:
    path = str(path)
    con  = duckdb.connect()
    if columns:
        cols = ', '.join(f'"{c}"' for c in columns)
        sql  = f"SELECT {cols} FROM read_parquet('{path}')"
    else:
        sql  = f"SELECT * FROM read_parquet('{path}')"
    return con.execute(sql).pl()


def load_parquet_glob(pattern: str, columns: list[str] | None = None) -> pl.DataFrame:
    con = duckdb.connect()
    if columns:
        cols = ', '.join(f'"{c}"' for c in columns)
        sql  = f"SELECT {cols} FROM read_parquet('{pattern}')"
    else:
        sql  = f"SELECT * FROM read_parquet('{pattern}')"
    return con.execute(sql).pl()


def safe_str(v, default: str = '') -> str:
    return str(v).strip() if v is not None else default


def safe_int(v, default: int = 0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def safe_float(v, default: float = 0.0) -> float:
    try:
        f = float(v)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return default


def substr(s: str, start_1based: int, length: int) -> str:
    """SAS SUBSTR(s, start, length) — 1-based indexing."""
    idx = start_1based - 1
    return str(s)[idx: idx + length] if s else ''


# ---------------------------------------------------------------------------
# GET REPTDATE
# DATA LOAN.REPTDATE (KEEP=REPTDATE);
#   SET BNM1.REPTDATE;
# ---------------------------------------------------------------------------
def get_reptdate() -> dict:
    """
    Load REPTDATE from BNM1 parquet, compute all macro variables.
    Returns dict with keys: NOWK, NOWK1, NOWKS, REPTMON, REPTMON1,
    REPTMON2, REPTYEAR, REPTDAY, RDATE, REPTDATE (SAS int), SDATE.
    """
    df = load_parquet(BNM1_DIR / "reptdate.parquet")
    row = df.row(0, named=True)
    reptdate_sas = row['REPTDATE']

    reptdate_dt  = sas_to_date(reptdate_sas)
    day_val      = reptdate_dt.day

    if day_val == 8:
        sdd, wk, wk1 = 1,  '1', '4'
    elif day_val == 15:
        sdd, wk, wk1 = 9,  '2', '1'
    elif day_val == 22:
        sdd, wk, wk1 = 16, '3', '2'
    else:
        sdd, wk, wk1 = 23, '4', '3'

    mm  = reptdate_dt.month
    mm2 = mm - 1 if mm > 1 else 12
    mm1 = (mm - 1 if mm > 1 else 12) if wk == '1' else mm

    sdate_sas = date_to_sas(datetime.date(reptdate_dt.year, mm, 1))

    reptyear_2 = str(reptdate_dt.year)[-2:]

    return {
        'NOWK'     : wk,
        'NOWK1'    : wk1,
        'NOWKS'    : '4',
        'REPTMON'  : f"{mm:02d}",
        'REPTMON1' : f"{mm1:02d}",
        'REPTMON2' : f"{mm2:02d}",
        'REPTYEAR' : reptyear_2,
        'REPTDAY'  : f"{day_val:02d}",
        'RDATE'    : fmt_ddmmyy8(reptdate_sas),
        'REPTDATE' : reptdate_sas,
        'SDATE'    : fmt_ddmmyy8(sdate_sas),
        'SDD'      : sdd,
        'MM'       : mm,
        'MM1'      : mm1,
        'MM2'      : mm2,
        'SDATE_SAS': sdate_sas,
    }


# ---------------------------------------------------------------------------
# %MACRO GET_FEE
# ---------------------------------------------------------------------------
def macro_get_fee(macros: dict) -> pl.DataFrame:
    """
    Replicate %GET_FEE macro.
    If REPTMON=12 AND NOWK=4: merge FEE31DEC with FEED31 + FEE30DEC.
    Otherwise: load BNM1.LNNOTE directly.
    Returns LNNOTE DataFrame sorted by ACCTNO, NOTENO.
    """
    reptmon = macros['REPTMON']
    nowk    = macros['NOWK']

    if reptmon == "12" and nowk == "4":
        # DATA FEE31DEC; INFILE FEED31;
        # INPUT @011 ACCTNO 11. @022 NOTENO 5. @027 FEEPLAN $2. ...
        # Fixed-width binary read of FEED31_FILE
        fee31_rows = []
        if FEED31_FILE.exists():
            raw = FEED31_FILE.read_bytes()
            # LRECL derived from last field: @073+1=74 bytes
            lrecl = 74
            lines = (raw.split(b'\n') if b'\n' in raw
                     else [raw[i:i+lrecl] for i in range(0, len(raw), lrecl)])
            for line in lines:
                if len(line) < 73:
                    if not line.strip():
                        continue
                    line = line.ljust(74, b' ')
                try:
                    acctno   = int(line[10:21].decode('cp037', errors='replace').strip() or '0')
                    noteno   = int(line[21:26].decode('cp037', errors='replace').strip() or '0')
                    feeplan  = line[26:28].decode('cp037', errors='replace').strip()
                    feecatgy = line[28:30].decode('cp037', errors='replace').strip()
                    trancode = int(line[30:33].decode('cp037', errors='replace').strip() or '0')
                    fee31d   = parse_zdb13_2(line[59:72])
                    reversed_flag = line[72:73].decode('cp037', errors='replace').strip()
                    # IF TRANCODE IN (760,761) AND FEECATGY IN ('LT','MS','TF')
                    if trancode not in (760, 761):
                        continue
                    if feecatgy not in ('LT', 'MS', 'TF'):
                        continue
                    # IF REVERSED = 'R' THEN FEE31D = FEE31D * (-1)
                    if reversed_flag == 'R':
                        fee31d = -fee31d
                    fee31_rows.append({'ACCTNO': acctno, 'NOTENO': noteno, 'FEE31D': fee31d})
                except (ValueError, IndexError):
                    continue

        fee31_df = pl.DataFrame(fee31_rows) if fee31_rows else pl.DataFrame(
            {'ACCTNO': [], 'NOTENO': [], 'FEE31D': []})

        # PROC SUMMARY ... SUM=FEE31D
        if not fee31_df.is_empty():
            fee31_df = fee31_df.group_by(['ACCTNO', 'NOTENO']).agg(
                pl.col('FEE31D').sum())

        # PROC SORT DATA=FEEYTD.FEE30DEC
        fee30_df = load_parquet(FEEYTD_DIR / "fee30dec.parquet",
                                columns=['ACCTNO', 'NOTENO', 'FEE30D'])

        # DATA FEE31DEC; MERGE FEE31DEC FEE30DEC; FEEYTD = SUM(FEE30D, FEE31D)
        merged = fee30_df.join(fee31_df, on=['ACCTNO', 'NOTENO'], how='left')
        merged = merged.with_columns([
            (pl.col('FEE30D').fill_null(0) + pl.col('FEE31D').fill_null(0))
            .alias('FEEYTD')
        ])
        # PROC SORT NODUPKEYS
        fee31dec = merged.unique(subset=['ACCTNO', 'NOTENO'])

        # PROC SORT DATA=BNM1.LNNOTE OUT=LNNOTE (DROP=FEEYTD INTPDYTD ACCRUYTD)
        lnnote_cols = [c for c in load_parquet(BNM1_DIR / "lnnote.parquet").columns
                       if c not in ('FEEYTD', 'INTPDYTD', 'ACCRUYTD')]
        lnnote = load_parquet(BNM1_DIR / "lnnote.parquet", columns=lnnote_cols)
        # DATA LNNOTE; MERGE LNNOTE FEEYTD.FEE31DEC; IF A; INTPDYTD=TOTPDEOP; ACCRUYTD=ACCRUEOP
        lnnote = lnnote.join(
            fee31dec.select(['ACCTNO', 'NOTENO', 'FEEYTD']),
            on=['ACCTNO', 'NOTENO'], how='left')
        lnnote = lnnote.with_columns([
            pl.col('TOTPDEOP').alias('INTPDYTD'),
            pl.col('ACCRUEOP').alias('ACCRUYTD'),
        ])
    else:
        # PROC SORT DATA=BNM1.LNNOTE OUT=LNNOTE
        lnnote = load_parquet(BNM1_DIR / "lnnote.parquet")

    return lnnote.sort(['ACCTNO', 'NOTENO'])


# ---------------------------------------------------------------------------
# Read HISTFILE binary (fixed-width)
# INPUT @001 ACCTNO PD6. @007 NOTENO PD3. @019 USERID $8. @034 POSTDT PD6.
# ---------------------------------------------------------------------------
def read_histfile() -> pl.DataFrame:
    """
    DATA HIST; INFILE HISTFILE MISSOVER;
    INPUT @001 ACCTNO PD6. @007 NOTENO PD3. @019 USERID $8. @034 POSTDT PD6.;
    IF POSTDT NOT IN (.,0) THEN POSTDATE = INPUT(SUBSTR(PUT(POSTDT,Z11.),1,8),MMDDYY8.)
    """
    rows = []
    if not HISTFILE.exists():
        return pl.DataFrame({'ACCTNO': [], 'NOTENO': [], 'USERID': [], 'POSTDATE': []})

    raw   = HISTFILE.read_bytes()
    lrecl = 39   # last field @034 PD6 ends at pos 34+6-1=39
    lines = (raw.split(b'\n') if b'\n' in raw
             else [raw[i:i+lrecl] for i in range(0, len(raw), lrecl)])

    for line in lines:
        if len(line) < lrecl:
            if not line.strip():
                continue
            line = line.ljust(lrecl, b'\x00')
        acctno  = decode_pd(line[0:6],   0)
        noteno  = decode_pd(line[6:9],   0)
        userid  = line[18:26].decode('cp037', errors='replace').strip()
        postdt  = decode_pd(line[33:39], 0)
        postdate = None
        if postdt and postdt != 0:
            postdate = decode_z11_date_mmddyy(int(postdt))
        rows.append({'ACCTNO': int(acctno), 'NOTENO': int(noteno),
                     'USERID': userid, 'POSTDATE': postdate})
    return pl.DataFrame(rows)


# ---------------------------------------------------------------------------
# Read REFNOTE fixed-width text file
# INPUT @001 ACCTNO 11. @012 OLDNOTE 5. @017 NOTENO 5.
#       @022 COSTCTR 7. @029 LASTDATE $10. @039 AANO $20.
# ---------------------------------------------------------------------------
def read_refnote() -> pl.DataFrame:
    rows = []
    if not REFNOTE_FILE.exists():
        return pl.DataFrame({'ACCTNO': [], 'NOTENO': [], 'OLDNOTE': []})

    with open(REFNOTE_FILE, 'rb') as fh:
        for line in fh:
            line = line.rstrip(b'\n\r')
            if len(line) < 21:
                continue
            try:
                acctno  = int(line[0:11])
                oldnote = int(line[11:16])
                noteno  = int(line[16:21])
                # costctr, lastdate, aano not kept in KEEP=
                rows.append({'ACCTNO': acctno, 'NOTENO': noteno, 'OLDNOTE': oldnote})
            except (ValueError, IndexError):
                continue
    df = pl.DataFrame(rows) if rows else pl.DataFrame(
        {'ACCTNO': [], 'NOTENO': [], 'OLDNOTE': []})
    # PROC SORT NODUPKEY BY ACCTNO NOTENO
    return df.unique(subset=['ACCTNO', 'NOTENO'])


# ---------------------------------------------------------------------------
# Read PAYFI binary file
# INPUT @001 ACCTNO PD6. @007 NOTENO PD3. @011 PAYEFDX PD6.
# ---------------------------------------------------------------------------
def read_payfi() -> pl.DataFrame:
    rows = []
    if not PAYFI_FILE.exists():
        return pl.DataFrame({'ACCTNO': [], 'NOTENO': [], 'PAYEFDT': []})

    raw   = PAYFI_FILE.read_bytes()
    lrecl = 16   # @011 PD6 ends at pos 11+6-1=16
    lines = (raw.split(b'\n') if b'\n' in raw
             else [raw[i:i+lrecl] for i in range(0, len(raw), lrecl)])

    for line in lines:
        if len(line) < lrecl:
            if not line.strip():
                continue
            line = line.ljust(lrecl, b'\x00')
        acctno  = int(decode_pd(line[0:6],  0))
        noteno  = int(decode_pd(line[6:9],  0))
        payefdx = decode_pd(line[10:16], 0)
        payefdt = None
        if payefdx and payefdx != 0:
            s    = f"{int(payefdx):011d}"
            # PAYCY=substr(z11,1,4), PAYMM=substr(z11,8,2), PAYDD=substr(z11,10,2)
            try:
                paycy = int(s[0:4])
                paymm = int(s[7:9])
                paydd = int(s[9:11])
                payefdt = adjust_payefdt(paydd, paymm, paycy)
            except (ValueError, IndexError):
                payefdt = None
        rows.append({'ACCTNO': acctno, 'NOTENO': noteno, 'PAYEFDT': payefdt})

    df = pl.DataFrame(rows) if rows else pl.DataFrame(
        {'ACCTNO': [], 'NOTENO': [], 'PAYEFDT': []})
    # PROC SORT NODUPKEY (descending PAYEFDT first, then deduplicate by ACCTNO NOTENO)
    df = df.sort(['ACCTNO', 'NOTENO', 'PAYEFDT'], descending=[False, False, True])
    return df.unique(subset=['ACCTNO', 'NOTENO'], keep='first')


# ---------------------------------------------------------------------------
# Build LNNOTE with HIST + REFNOTE + derived fields
# ---------------------------------------------------------------------------
def build_lnnote(lnnote: pl.DataFrame, reptdate_sas: int) -> pl.DataFrame:
    """
    DATA LNNOTE(DROP=OLDNOTE);
      MERGE LNNOTE(IN=A) HIST REFNOTE;
      BY ACCTNO NOTENO; IF A;
      -- derive TIMELATE, NONACCRUAL, SENDBILL, LNUSER2, USINDEX,
         REFNOTENO, SCORE1MI, SCORE2CT, date conversions, etc.
    """
    hist     = read_histfile()
    # DATA HIST; SET HIST.UP2DEC13 HIST.UP2DEC15 HIST.UP2DEC22 HIST;
    hist_parts = [hist]
    for part in ['up2dec13', 'up2dec15', 'up2dec22']:
        p = HIST_DIR / f"{part}.parquet"
        if p.exists():
            hist_parts.append(load_parquet(p))
    hist = pl.concat(hist_parts, how='diagonal')
    # PROC SORT DESCENDING POSTDATE, then NODUPKEY BY ACCTNO NOTENO
    hist = hist.sort(['ACCTNO', 'NOTENO', 'POSTDATE'], descending=[False, False, True])
    hist = hist.unique(subset=['ACCTNO', 'NOTENO'], keep='first')

    refnote = read_refnote()

    # Merge LNNOTE + HIST + REFNOTE by ACCTNO NOTENO; IF A
    df = lnnote.join(hist.select(['ACCTNO', 'NOTENO', 'POSTDATE', 'USERID']),
                     on=['ACCTNO', 'NOTENO'], how='left')
    df = df.join(refnote, on=['ACCTNO', 'NOTENO'], how='left')

    # Apply derived fields row by row
    rows_out = []
    for row in df.to_dicts():
        # LENGTH TIMELATE $15.
        # TIMELATE = PUT(TMLATE15,Z3.) || '\' || PUT(TMLATE30,Z3.) || ...
        tl15 = safe_int(row.get('TMLATE15'), 0)
        tl30 = safe_int(row.get('TMLATE30'), 0)
        tl60 = safe_int(row.get('TMLATE60'), 0)
        tl90 = safe_int(row.get('TMLATE90'), 0)
        row['TIMELATE']  = f"{tl15:03d}\\{tl30:03d}\\{tl60:03d}\\{tl90:03d}"
        row['NONACCRUAL'] = row.get('AUTNACCR')
        row['SENDBILL']   = row.get('SENDNBL')
        row['LNUSER2']    = row.get('USER5')
        row['USINDEX']    = row.get('USURYIDX')

        # REFNOTENO: IF OLDNOTE NOT IN (.,0) THEN REFNOTENO=OLDNOTE ELSE REFNOTENO=BONUSANO
        oldnote = safe_float(row.get('OLDNOTE'))
        if oldnote not in (None, 0.0, float('nan')) and not math.isnan(oldnote):
            row['REFNOTENO'] = int(oldnote)
        else:
            row['REFNOTENO'] = row.get('BONUSANO')

        row['SCORE1MI'] = row.get('MORTGIND')
        row['SCORE2CT'] = row.get('CONTRTYPE')

        # IF FRELEAS NOT IN (.,0) THEN FULLREL_DT = ...
        freleas = safe_float(row.get('FRELEAS'))
        if freleas not in (None, 0.0) and not math.isnan(freleas):
            row['FULLREL_DT'] = decode_z11_date_mmddyy(int(freleas))
            row['ORGISSDTE']  = decode_z11_date_mmddyy(int(freleas))

        # IF DEATHDATE NOT IN (.,0) THEN DEATHDATE = ...
        deathdate = safe_float(row.get('DEATHDATE'))
        if deathdate not in (None, 0.0) and not math.isnan(deathdate):
            row['DEATHDATE'] = decode_z11_date_mmddyy(int(deathdate))

        # IF VARSTDTE NOT IN (.,0) THEN VARSTDTE = ...
        varstdte = safe_float(row.get('VARSTDTE'))
        if varstdte not in (None, 0.0) and not math.isnan(varstdte):
            row['VARSTDTE'] = decode_z11_date_mmddyy(int(varstdte))

        # IF FCLOSUREDT NOT IN (.,0) THEN RRCOUNTDTE = ...
        fclosuredt = safe_float(row.get('FCLOSUREDT'))
        if fclosuredt not in (None, 0.0) and not math.isnan(fclosuredt):
            row['RRCOUNTDTE'] = decode_z11_date_mmddyy(int(fclosuredt))

        # IF INTSTDTE NOT IN (.,0) THEN ...
        # DD=SUBSTR(INTSTDTE,11,2); MM=SUBSTR(INTSTDTE,9,2); YY=SUBSTR(INTSTDTE,2,4)
        intstdte = safe_float(row.get('INTSTDTE'))
        if intstdte not in (None, 0.0) and not math.isnan(intstdte):
            s = f"{int(intstdte):011d}"
            try:
                dd_s = s[10:12] if len(s) >= 12 else s[-2:]
                mm_s = s[8:10]
                yy_s = s[1:5]
                row['INTSTDTE'] = date_to_sas(
                    datetime.date(int(yy_s), int(mm_s), int(dd_s)))
            except (ValueError, IndexError):
                pass

        # IF CPNSTDTE NOT IN (.,0) THEN CPNSTDTE = ...
        cpnstdte = safe_float(row.get('CPNSTDTE'))
        if cpnstdte not in (None, 0.0) and not math.isnan(cpnstdte):
            row['CPNSTDTE'] = decode_z11_date_mmddyy(int(cpnstdte))

        # IF VALUEDTE NOT IN (.,0) THEN VALUATION_DT = INPUT(VALUEDTE, DDMMYY8.)
        valuedte = row.get('VALUEDTE')
        if valuedte not in (None, 0, '', '.'):
            row['VALUATION_DT'] = parse_ddmmyy8(str(valuedte))

        rows_out.append(row)

    return pl.DataFrame(rows_out)


# ---------------------------------------------------------------------------
# Merge LNNOTE with LNCOMM (approved limit / undrawn)
# ---------------------------------------------------------------------------
def merge_lnnote_comm(lnnote: pl.DataFrame, reptdate_sas: int) -> pl.DataFrame:
    """
    *** APPROVED LIMIT AND UNDRAWN AMT ***
    PROC SORT DATA=LNNOTE; BY ACCTNO COMMNO;
    PROC SORT DATA=BNM1.LNCOMM OUT=COMM; BY ACCTNO COMMNO;
    DATA LNNOTE ...; MERGE LNNOTE(IN=A) COMM; BY ACCTNO COMMNO; IF A;
    """
    comm = load_parquet(BNM1_DIR / "lncomm.parquet")
    df   = lnnote.join(comm, on=['ACCTNO', 'COMMNO'], how='left', suffix='_COMM')

    rows_out = []
    for row in df.to_dicts():
        row['BRANCH']   = row.get('NTBRCH', row.get('BRANCH'))
        row['COMMTYPE'] = row.get('CPRODUCT')
        row['ESCROWRBAL'] = row.get('ECSRRSRV')
        row['LN_UTILISE_LOCAT_CD'] = row.get('STATE', row.get('LN_UTILISE_LOCAT_CD'))

        # IF PAIDIND NE 'P' THEN ...
        paidind = safe_str(row.get('PAIDIND'))
        if paidind != 'P':
            ntint   = safe_str(row.get('NTINT'))
            balance = safe_float(row.get('BALANCE'))
            curbal  = safe_float(row.get('CURBAL'))
            feeamt  = safe_float(row.get('FEEAMT'))
            if ntint != 'A':
                row['INTERDUE'] = balance + (-1 * curbal) + (-1 * feeamt)
            else:
                row['INTERDUE'] = 0
        rows_out.append(row)
    return pl.DataFrame(rows_out)


# ---------------------------------------------------------------------------
# Merge LNNOTE with NPL indicator
# ---------------------------------------------------------------------------
def merge_lnnote_npl(lnnote: pl.DataFrame, reptmon1: str) -> pl.DataFrame:
    """
    *** NPL IND ***
    PROC SORT DATA=MNINPL.TOTIIS&REPTMON1 OUT=NPL (KEEP=ACCTNO NOTENO NPLIND)
    DATA LNNOTE; MERGE LNNOTE(IN=A) NPL(IN=B); BY ACCTNO NOTENO; IF A;
    """
    npl_file = MNINPL_DIR / f"totiis{reptmon1}.parquet"
    if npl_file.exists():
        npl = load_parquet(npl_file, columns=['ACCTNO', 'NOTENO', 'NPLIND'])
        return lnnote.join(npl, on=['ACCTNO', 'NOTENO'], how='left')
    return lnnote


# ---------------------------------------------------------------------------
# Load LOAN base datasets
# DATA LOAN; SET BNM.LOAN&RM&NK BNM.LNWOD&RM&NK BNM.LNWOF&RM&NK
# ---------------------------------------------------------------------------
def load_loan_base(macros: dict) -> pl.DataFrame:
    rm = macros['REPTMON']
    nk = macros['NOWK']
    parts = []
    for prefix in ('loan', 'lnwod', 'lnwof'):
        p = BNM_DIR / f"{prefix}{rm}{nk}.parquet"
        if p.exists():
            parts.append(load_parquet(p))
    df = pl.concat(parts, how='diagonal') if parts else pl.DataFrame()
    # CUSTFISS=CUSTCD; SECTFISS=SECTORCD
    if 'CUSTCD' in df.columns:
        df = df.with_columns(pl.col('CUSTCD').alias('CUSTFISS'))
    if 'SECTORCD' in df.columns:
        df = df.with_columns(pl.col('SECTORCD').alias('SECTFISS'))
    return df


# ---------------------------------------------------------------------------
# %MACRO GET_GP3 — merge risk grade for week 4
# ---------------------------------------------------------------------------
def macro_get_gp3(loan: pl.DataFrame, macros: dict) -> pl.DataFrame:
    if macros['NOWK'] == "4":
        gp3_file = ODGP3_DIR / "gp3.parquet"
        if gp3_file.exists():
            gp3 = load_parquet(gp3_file, columns=['ACCTNO', 'RISKCODE'])
            gp3 = gp3.with_columns(
                pl.col('RISKCODE').cast(pl.Utf8).str.slice(0, 1)
                .cast(pl.Int64).alias('RISKRTE'))
            loan = loan.join(gp3.select(['ACCTNO', 'RISKRTE']),
                             on='ACCTNO', how='left', suffix='_GP3')
            if 'RISKRTE_GP3' in loan.columns:
                loan = loan.with_columns(
                    pl.when(pl.col('RISKRTE_GP3').is_not_null())
                    .then(pl.col('RISKRTE_GP3'))
                    .otherwise(pl.col('RISKRTE'))
                    .alias('RISKRTE')
                ).drop('RISKRTE_GP3')
    return loan


# ---------------------------------------------------------------------------
# Build OVERDFT dataset
# ---------------------------------------------------------------------------
def build_overdft(reptdate_sas: int) -> pl.DataFrame:
    """
    PROC SORT DATA=MNITB.CURRENT OUT=CURRENT WHERE CURBAL < 0;
    PROC SORT DATA=ODGP3.OVERDFT ... NODUPKEY;
    DATA OVERDFT; MERGE CURRENT OVERDFT; ...
    """
    current_file = MNITB_DIR / "current.parquet"
    overdft_file = ODGP3_DIR  / "overdft.parquet"

    if not current_file.exists():
        return pl.DataFrame()

    current = load_parquet(current_file)
    current = current.filter(pl.col('CURBAL') < 0)

    rows_out = []
    overdft_df = None
    if overdft_file.exists():
        overdft_df = load_parquet(overdft_file)
        overdft_df = overdft_df.unique(subset=['ACCTNO'], keep='first')
        overdft_df = overdft_df.rename({'APPRLIMT': 'APPRLIM2'})

    for row in current.to_dicts():
        acctno = row['ACCTNO']
        # Merge OVERDFT row
        od_row = {}
        if overdft_df is not None:
            matches = overdft_df.filter(pl.col('ACCTNO') == acctno)
            if not matches.is_empty():
                od_row = matches.row(0, named=True)

        exoddate = safe_float(row.get('EXODDATE'))
        tempoddt = safe_float(row.get('TEMPODDT'))
        curbal   = safe_float(row.get('CURBAL'))
        oddate   = None

        if (exoddate != 0 or tempoddt != 0) and curbal <= 0:
            exoddt = decode_z11_date_mmddyy(int(exoddate)) if exoddate else None
            tempdt = decode_z11_date_mmddyy(int(tempoddt)) if tempoddt else None

            if exoddt and tempdt:
                oddate = exoddt if exoddt < tempdt else tempdt
            else:
                oddate = tempdt if exoddate == 0 else exoddt

        dayarr = (reptdate_sas - oddate + 1) if oddate else None

        rows_out.append({
            'ACCTNO'              : acctno,
            'APPRLIM2'            : od_row.get('APPRLIM2'),
            'ASCORE_PERM'         : od_row.get('ASCORE_PERM'),
            'ASCORE_LTST'         : od_row.get('ASCORE_LTST'),
            'ASCORE_COMM'         : od_row.get('ASCORE_COMM'),
            'INDUSTRIAL_SECTOR_CD': od_row.get('INDUSTRIAL_SECTOR_CD'),
            'DAYARR'              : dayarr,
        })

    return pl.DataFrame(rows_out)


# ---------------------------------------------------------------------------
# Merge LOAN with LNNOTE (main merge block)
# DATA LOAN; DROP ...; MERGE LOAN(IN=A) LNNOTE(IN=B); BY ACCTNO NOTENO;
# ---------------------------------------------------------------------------
def merge_loan_lnnote(loan: pl.DataFrame, lnnote: pl.DataFrame,
                      rdate_str: str, reptdate_sas: int) -> pl.DataFrame:
    """
    Replicate the large DATA LOAN block that merges with LNNOTE
    and derives many fields.
    """
    drop_from_lnnote = {'BILTOT', 'FLAG1', 'FLAG5', 'USER1', 'USER2',
                        'USER3', 'USER4', 'PZIPCODE', 'INTPYTD1'}
    df = loan.join(lnnote, on=['ACCTNO', 'NOTENO'], how='left', suffix='_LN')

    thisdate_sas = parse_ddmmyy8(rdate_str)

    rows_out = []
    for row in df.to_dicts():
        row['TOTBNP']   = row.get('BILTOT')
        row['CAGATAG']  = row.get('PZIPCODE')
        row['U1CLASSI'] = row.get('USER1')
        row['U2RACECO'] = row.get('USER2')
        row['U3MYCODE'] = row.get('USER3')
        row['U4RESIDE'] = row.get('USER4')
        row['F1RELMOD'] = row.get('FLAG1')
        row['F5ACCONV'] = row.get('FLAG5')

        lasttran = safe_float(row.get('LASTTRAN'))
        if lasttran not in (None, 0.0) and not math.isnan(lasttran):
            row['LASTRAN'] = decode_z11_date_mmddyy(int(lasttran))

        birthdt = safe_float(row.get('BIRTHDT'))
        if birthdt not in (None, 0.0) and not math.isnan(birthdt):
            row['DOBMNI'] = decode_z11_date_mmddyy(int(birthdt))

        apprdate = safe_float(row.get('APPRDATE'))
        if apprdate not in (None, 0.0) and not math.isnan(apprdate):
            row['APPRDATE'] = decode_z11_date_mmddyy(int(apprdate))

        maturedt = safe_float(row.get('MATUREDT'))
        if maturedt not in (None, 0.0) and not math.isnan(maturedt):
            row['MATUREDT'] = decode_z11_date_mmddyy(int(maturedt))

        assmdate = safe_float(row.get('ASSMDATE'))
        if assmdate not in (None, float('nan')):
            row['ASSMDATE'] = decode_z11_date_mmddyy(int(assmdate))

        rows_out.append(row)

    df = pl.DataFrame(rows_out)

    # DATA LOAN; SET LOAN; IF MATUREDT=EXPRDATE THEN MATUREDT=.
    df = df.with_columns(
        pl.when(pl.col('MATUREDT') == pl.col('EXPRDATE'))
        .then(None)
        .otherwise(pl.col('MATUREDT'))
        .alias('MATUREDT')
    )
    return df


# ---------------------------------------------------------------------------
# Derive CENSUS fields, COLLDESC extractions, DAYARR, MTHARR in LOAN
# ---------------------------------------------------------------------------
def derive_loan_fields(loan: pl.DataFrame, macros: dict) -> pl.DataFrame:
    """
    DATA LOAN (DROP=CENSUS THISDATE DAYARR_MO);
      SET LOAN (DROP=LASTTRAN BIRTHDT);
      LASTTRAN=LASTRAN; PAYEFDT=...; NEWBAL=...; CENSUS0-5; MAKE/MODEL/REGNO;
      DAYARR=...; MTHARR=...; MTHARR_CCRIS=...
    """
    reptdate_sas = macros['REPTDATE']
    rdate_str    = macros['RDATE']
    thisdate_sas = parse_ddmmyy8(rdate_str)

    rows_out = []
    for row in loan.to_dicts():
        row['LASTTRAN'] = row.get('LASTRAN')

        # PAYEFDT derivation from PAYEFFDT
        payeffdt = safe_float(row.get('PAYEFFDT'))
        if payeffdt not in (None, 0.0) and not math.isnan(payeffdt):
            row['PAYEFDT'] = derive_payefdt_from_z11(int(payeffdt))

        balance  = safe_float(row.get('BALANCE'))
        feeamt   = safe_float(row.get('FEEAMT'))
        accrual  = safe_float(row.get('ACCRUAL'))
        row['NEWBAL'] = balance - (feeamt + accrual)

        # CENSUS sub-fields: PUT(CENSUS, 7.2) -> "XXXX.XX"
        census_val = safe_float(row.get('CENSUS'))
        census_str = f"{census_val:07.2f}" if not math.isnan(census_val) else '0000.00'
        row['CENSUS0'] = row.get('CENSUS')
        row['CENSUS1'] = census_str[0:2]
        row['CENSUS3'] = census_str[2:3]
        row['CENSUS4'] = census_str[3:4]
        row['CENSUS5'] = census_str[5:7]

        colldesc = safe_str(row.get('COLLDESC'), '')
        row['MAKE']    = colldesc[0:16]
        row['MODEL']   = colldesc[15:36]   # SUBSTR(COLLDESC,16,21)
        # /* NEWSEC = SUBSTR(COLLDESC,38,2); ESMR:2009-1776 */
        row['REGNO']   = colldesc[39:52]   # SUBSTR(COLLDESC,40,13)
        row['EXREGNO'] = colldesc[52:65]   # SUBSTR(COLLDESC,53,13)

        # DAYARR
        bldate   = safe_float(row.get('BLDATE'))
        dayarr   = None
        if thisdate_sas and bldate and bldate > 0:
            dayarr = thisdate_sas - int(bldate)

        # IF OLDNOTEDAYARR > 0 AND 98000 <= NOTENO <= 98999 THEN ...
        oldnotedayarr = safe_float(row.get('OLDNOTEDAYARR'))
        noteno_val    = safe_int(row.get('NOTENO'))
        if oldnotedayarr and oldnotedayarr > 0 and 98000 <= noteno_val <= 98999:
            if dayarr is not None and dayarr < 0:
                dayarr = 0
            dayarr = (dayarr or 0) + int(oldnotedayarr)

        # IF DAYARR_MO NE . THEN DAYARR = DAYARR_MO
        dayarr_mo = row.get('DAYARR_MO')
        if dayarr_mo is not None and not (isinstance(dayarr_mo, float) and math.isnan(dayarr_mo)):
            dayarr = dayarr_mo

        row['DAYARR'] = dayarr

        # COLLAGE
        collyear = safe_float(row.get('COLLYEAR'))
        if collyear and collyear > 0 and thisdate_sas:
            thisdate_dt = sas_to_date(thisdate_sas)
            row['COLLAGE'] = round((thisdate_dt.year - int(collyear)) + 1, 1)

        # MTHARR via SELECT WHEN ladder (loan version, max 24)
        row['MTHARR']       = dayarr_to_mtharr_loan(dayarr)
        row['MTHARR_CCRIS'] = mtharr_ccris(dayarr)

        rows_out.append(row)

    return pl.DataFrame(rows_out)


# ---------------------------------------------------------------------------
# Build NAMEX from BNM1.NAME8
# ---------------------------------------------------------------------------
def build_namex() -> pl.DataFrame:
    name8_file = BNM1_DIR / "name8.parquet"
    if not name8_file.exists():
        return pl.DataFrame({'ACCTNO': [], 'DATEREGV': [],
                             'VEHI_CHASSIS_NUM': [], 'VEHI_ENGINE_NUM': []})

    name8 = load_parquet(name8_file)
    rows_out = []
    for row in name8.to_dicts():
        dteregvh = safe_float(row.get('DTEREGVH'))
        dateregv = None
        if dteregvh not in (None, 0.0) and not math.isnan(dteregvh):
            # DATEREGV = MM/DD/YY  (positions 4-5, 6-7, 10-11 of Z11)
            s = f"{int(dteregvh):011d}"
            try:
                mm_s = s[3:5]; dd_s = s[5:7]; yy_s = s[9:11]
                dateregv = f"{mm_s}/{dd_s}/{yy_s}"
            except IndexError:
                pass

        vehi_chassis = ''
        vehi_engine  = ''
        if safe_str(row.get('SEQKEY')) == '8':
            vehi_chassis = safe_str(row.get('LINEFOUR'))[:22]
            vehi_engine  = safe_str(row.get('LINETHRE'))[:22]

        rows_out.append({
            'ACCTNO'          : row.get('ACCTNO'),
            'DATEREGV'        : dateregv,
            'VEHI_CHASSIS_NUM': vehi_chassis,
            'VEHI_ENGINE_NUM' : vehi_engine,
        })

    df = pl.DataFrame(rows_out)
    return df.sort('ACCTNO')


# ---------------------------------------------------------------------------
# Build NOTEX from BNM1.LNNOTE
# ---------------------------------------------------------------------------
def build_notex() -> pl.DataFrame:
    """
    DATA NOTEX; KEEP ACCTNO NOTENO PRIMOFHP POINTAMT CANO CEILINGO CEILINGU USER5;
      SET BNM1.LNNOTE;
      PRIMOFHP = SUBSTR(PUT(PRMOFFHP,5.),3,3);
      CANO = ESCRACCT;
    """
    lnnote = load_parquet(BNM1_DIR / "lnnote.parquet")
    rows_out = []
    for row in lnnote.to_dicts():
        prmoffhp = safe_float(row.get('PRMOFFHP'))
        primofhp = f"{int(prmoffhp):5d}"[2:5] if prmoffhp else '   '
        rows_out.append({
            'ACCTNO'  : row.get('ACCTNO'),
            'NOTENO'  : row.get('NOTENO'),
            'PRIMOFHP': primofhp,
            'POINTAMT': row.get('POINTAMT'),
            'CANO'    : row.get('ESCRACCT'),
            'CEILINGO': row.get('CEILINGO'),
            'CEILINGU': row.get('CEILINGU'),
            'USER5'   : row.get('USER5'),
        })
    df = pl.DataFrame(rows_out)
    return df.sort(['ACCTNO', 'NOTENO'])


# ---------------------------------------------------------------------------
# Build CIS and CISD (customer info)
# ---------------------------------------------------------------------------
def build_cisd() -> pl.DataFrame:
    """
    DATA CISD; KEEP ACCTNO CUSTNO NAME LONGNAME SECTOR_CODE SIC_ISS
                    TURNOVER NOEMPLO UPD_DT_SALE UPD_DT_EMPLOYEE;
      SET CISD.DEPOSIT; IF SECCUST='901'; NAME=CUSTNAM1;
    """
    df = load_parquet(CISD_DIR / "deposit.parquet")
    df = df.filter(pl.col('SECCUST') == '901')
    df = df.with_columns(pl.col('CUSTNAM1').alias('NAME'))
    keep = ['ACCTNO', 'CUSTNO', 'NAME', 'LONGNAME', 'SECTOR_CODE',
            'SIC_ISS', 'TURNOVER', 'NOEMPLO', 'UPD_DT_SALE', 'UPD_DT_EMPLOYEE']
    existing = [c for c in keep if c in df.columns]
    df = df.select(existing)
    return df.unique(subset=['ACCTNO'], keep='first')


def build_cis() -> pl.DataFrame:
    """
    DATA CIS (KEEP=ACCTNO DOBCIS U2RACECO);
      SET CIS.LOAN; IF SECCUST='901';
      BDD=SUBSTR(BIRTHDAT,1,2); BMM=SUBSTR(BIRTHDAT,3,2); BYY=SUBSTR(BIRTHDAT,5,4);
      DOBCIS=MDY(BMM,BDD,BYY);
      NAME=CUSTNAM1; U2RACECO=RACE;
    """
    df = load_parquet(CIS_DIR / "loan.parquet")
    df = df.filter(pl.col('SECCUST') == '901')
    rows_out = []
    for row in df.to_dicts():
        birthdat = safe_str(row.get('BIRTHDAT'), '00000000')
        try:
            bdd = int(birthdat[0:2])
            bmm = int(birthdat[2:4])
            byy = int(birthdat[4:8])
            dobcis = date_to_sas(datetime.date(byy, bmm, bdd))
        except (ValueError, IndexError):
            dobcis = None
        rows_out.append({
            'ACCTNO'   : row.get('ACCTNO'),
            'DOBCIS'   : dobcis,
            'U2RACECO' : row.get('RACE'),
        })
    df = pl.DataFrame(rows_out)
    return df.unique(subset=['ACCTNO'], keep='first')


# ---------------------------------------------------------------------------
# Extract SECTOR for OD accounts
# ---------------------------------------------------------------------------
def build_ovdft_sector(positive: bool = False) -> pl.DataFrame:
    """
    *--EXTRACT SECTOR FIELD FOR OD--*
    DATA OVDFT(KEEP=ACCTNO SECTOR);
      SET MNITB.CURRENT(RENAME=(SECTOR=SECT));
      IF OPENIND NOT IN ('B','C','P') AND CURBAL LT 0 (or GE 0 AND APPRLIMT GT 0);
      SECTOR=PUT(SECT,$4.);
    """
    current_file = MNITB_DIR / "current.parquet"
    if not current_file.exists():
        return pl.DataFrame({'ACCTNO': [], 'SECTOR': []})
    current = load_parquet(current_file)
    current = current.rename({'SECTOR': 'SECT'})
    if positive:
        # OVDFT1: CURBAL GE 0 AND APPRLIMT GT 0
        current = current.filter(
            (~pl.col('OPENIND').is_in(['B', 'C', 'P'])) &
            (pl.col('CURBAL') >= 0) &
            (pl.col('APPRLIMT') > 0))
    else:
        # OVDFT: CURBAL LT 0
        current = current.filter(
            (~pl.col('OPENIND').is_in(['B', 'C', 'P'])) &
            (pl.col('CURBAL') < 0))

    current = current.with_columns(
        pl.col('SECT').cast(pl.Utf8).str.zfill(4).alias('SECTOR'))
    return current.select(['ACCTNO', 'SECTOR']).sort('ACCTNO')


# ---------------------------------------------------------------------------
# Build ILNPD (paid-off / reversed loans)
# ---------------------------------------------------------------------------
def build_ilnpd(macros: dict) -> pl.DataFrame:
    """
    DATA LOAN.ILNPD&RM&NK&YR;
      SET BNM1.LNNOTE BNM1.RVNOTE(IN=B);
      IF B THEN PAIDIND=''; IF PAIDIND EQ 'P' OR PAIDIND='';
      ... many derived fields ...
    """
    reptdate_sas = macros['REPTDATE']
    rdate_str    = macros['RDATE']
    reptmon1     = macros['REPTMON1']
    thisdate_sas = parse_ddmmyy8(rdate_str)

    lnnote = load_parquet(BNM1_DIR / "lnnote.parquet")
    lnnote = lnnote.with_columns(pl.lit(False).alias('_B'))

    rvnote_file = BNM1_DIR / "rvnote.parquet"
    if rvnote_file.exists():
        rvnote = load_parquet(rvnote_file)
        rvnote = rvnote.with_columns(pl.lit(True).alias('_B'))
        combined = pl.concat([lnnote, rvnote], how='diagonal')
    else:
        combined = lnnote

    # IF B THEN PAIDIND=''; IF PAIDIND EQ 'P' OR PAIDIND=''
    combined = combined.with_columns(
        pl.when(pl.col('_B')).then(pl.lit('')).otherwise(pl.col('PAIDIND'))
        .alias('PAIDIND'))
    combined = combined.filter(
        (pl.col('PAIDIND') == 'P') | (pl.col('PAIDIND') == ''))

    # %INC PGM(PBBDPFMT, PBBLNFMT) -- format maps for CUSTCD, LOANTYPE
    # CUSTCD=PUT(CUSTCODE, LNCUSTCD.);  PRODCD=PUT(LOANTYPE, LNPROD.)
    # These formats come from PBBLNFMT dependency; map as identity / stub
    # LNCUSTCD. and LNPROD. are defined in PBBLNFMT -- applied as-is
    def fmt_lncustcd(v) -> str:
        return safe_str(v)  # placeholder: format returns code as string

    def fmt_lnprod(v) -> str:
        return safe_str(v)  # placeholder: LNPROD. format from PBBLNFMT

    # $STATEPOST., $STATECD., STATECD. from PBBELF dependency
    # $CRISCD. from PBBLNFMT / PBBDPFMT dependency
    # &POSTCD, &FCY, &HP, &COUNTRYCD from PBBELF macros
    # These sets/maps are defined in %INC PGM(PBBELF) -- applied as stubs
    POSTCD    = set()      # from PBBELF: &POSTCD state codes using postcode
    FCY       = set()      # from PBBELF: foreign currency products
    HP        = set()      # from PBBELF: hire purchase products
    COUNTRYCD = set()      # from PBBELF: country codes

    rows_out = []
    for row in combined.to_dicts():
        custcode = safe_str(row.get('CUSTCODE'))
        row['CUSTCD']   = fmt_lncustcd(custcode)
        row['CUSTFISS'] = row['CUSTCD']
        loantype = safe_str(row.get('LOANTYPE'))
        row['PRODCD']   = fmt_lnprod(loantype)

        # EXPRDATE = INPUT(SUBSTR(PUT(NOTEMAT,Z11.),1,8),MMDDYY8.)
        notemat = safe_float(row.get('NOTEMAT'))
        if notemat not in (None, 0.0) and not math.isnan(notemat):
            row['EXPRDATE'] = decode_z11_date_mmddyy(int(notemat))

        row['CAGATAG'] = row.get('PZIPCODE')

        for fld, src in [('BIRTHDT', 'BIRTHDT'), ('ISSDTE', 'ISSUEDT'),
                         ('ORGISSDTE', 'FRELEAS'), ('LASTTRAN', 'LASTTRAN'),
                         ('MATUREDT', 'MATUREDT'), ('CPNSTDTE', 'CPNSTDTE')]:
            v = safe_float(row.get(src))
            if v not in (None, 0.0) and not math.isnan(v):
                row[fld] = decode_z11_date_mmddyy(int(v))

        valuedte = row.get('VALUEDTE')
        if valuedte not in (None, 0, '', '.'):
            row['VALUATION_DT'] = parse_ddmmyy8(str(valuedte))

        row['FULLREL_DT'] = row.get('ORGISSDTE')

        # PAYEFDT
        payeffdt = safe_float(row.get('PAYEFFDT'))
        if payeffdt not in (None, 0.0) and not math.isnan(payeffdt):
            row['PAYEFDT'] = derive_payefdt_from_z11(int(payeffdt))

        # DAYARR
        bldate = safe_float(row.get('BLDATE'))
        dayarr = None
        if bldate not in (None, float('nan')):
            dayarr = reptdate_sas - int(bldate)

        row['DAYARR']       = dayarr
        row['MTHARR']       = dayarr_to_mtharr_pd(dayarr)
        row['MTHARR_CCRIS'] = mtharr_ccris(dayarr)

        # COLLAGE
        collyear = safe_float(row.get('COLLYEAR'))
        if collyear and collyear > 0 and thisdate_sas:
            row['COLLAGE'] = round((sas_to_date(thisdate_sas).year - int(collyear)) + 1, 1)

        # REMAINMT (month loop)
        exprdate = row.get('EXPRDATE')
        remainmt = 0
        if exprdate and reptdate_sas:
            remainmt = calc_remainmt(reptdate_sas, int(exprdate))
        row['REMAINMT'] = remainmt

        # STATECD derivation -- uses PBBELF format maps (stubbed)
        state   = safe_str(row.get('STATE'))
        product = safe_int(row.get('PRODUCT'))
        statecd = ''
        if state in POSTCD:
            statecd = state  # PUT(STATE, $STATEPOST.) -- from PBBELF
        elif product in FCY and state in COUNTRYCD:
            statecd = 'Z'
        else:
            statecd = state  # PUT(STATE, $STATECD.) -- from PBBELF
            if product in FCY:
                if state in ('00', '99', '099', '0099', '00099', '000099'):
                    statecd = 'Z'
        if not statecd.strip():
            statecd = str(row.get('BRANCH', ''))  # STATECD. numeric fmt from PBBELF

        row['STATECD'] = statecd

        row['SECTFISS'] = row.get('SECTOR')
        # FISSPURP=PUT(CRISPURP,$CRISCD.) -- from PBBDPFMT/PBBLNFMT
        row['FISSPURP'] = safe_str(row.get('CRISPURP'))   # stub

        colldesc = safe_str(row.get('COLLDESC'), '')
        row['MAKE']    = colldesc[0:16]
        row['MODEL']   = colldesc[15:36]
        census_val = safe_float(row.get('CENSUS'))
        census_str = f"{census_val:07.2f}" if not math.isnan(census_val) else '0000.00'
        row['CENSUS1'] = census_str[0:2]
        row['CENSUS4'] = census_str[3:4]
        row['REGNO']   = colldesc[39:52]

        row['RENAME_NTBRCH'] = row.get('NTBRCH')
        row['RENAME_LOANTYPE'] = row.get('LOANTYPE')
        row['U1CLASSI'] = row.get('USER1')
        row['U2RACECO'] = row.get('USER2')
        row['U3MYCODE'] = row.get('USER3')
        row['U4RESIDE'] = row.get('USER4')
        row['F1RELMOD'] = row.get('FLAG1')
        row['F5ACCONV'] = row.get('FLAG5')
        row['TOTBNP']   = row.get('BILTOT')
        row['CENSUS0']  = row.get('CENSUS')
        row['REFNOTENO']= row.get('BONUSANO')

        rows_out.append(row)

    df = pl.DataFrame(rows_out)
    # RENAME NTBRCH=BRANCH LOANTYPE=PRODUCT
    if 'RENAME_NTBRCH' in df.columns:
        df = df.with_columns(pl.col('RENAME_NTBRCH').alias('BRANCH'))
    if 'RENAME_LOANTYPE' in df.columns:
        df = df.with_columns(pl.col('RENAME_LOANTYPE').alias('PRODUCT'))

    return df


# ---------------------------------------------------------------------------
# Build LNPDX (HP detection + approved limits)
# ---------------------------------------------------------------------------
def build_lnpdx(lnpd: pl.DataFrame, macros: dict) -> pl.DataFrame:
    """
    DATA LNPDX &LNNOTEPD;
      MERGE LNPD(IN=A) LNCOMM(IN=B); BY ACCTNO COMMNO; IF A;
      IF PRODCD='34111' THEN LNTYPE='HP'; APPRLIMT=...
    """
    lncomm = load_parquet(BNM1_DIR / "lncomm.parquet")
    df = lnpd.join(lncomm, on=['ACCTNO', 'COMMNO'], how='left', suffix='_CMM')

    rows_out = []
    for row in df.to_dicts():
        prodcd  = safe_str(row.get('PRODCD'))
        product = safe_int(row.get('PRODUCT'))
        commno  = safe_float(row.get('COMMNO'))
        curbal  = safe_float(row.get('CURBAL'))
        rebate  = safe_float(row.get('REBATE'))
        intearn4 = safe_float(row.get('INTEARN4'))
        revovli = safe_str(row.get('REVOVLI'))
        corgamt = safe_float(row.get('CORGAMT'))
        ccuramt = safe_float(row.get('CCURAMT'))
        orgbal  = safe_float(row.get('ORGBAL'))
        balance = safe_float(row.get('BALANCE'))
        feeamt  = safe_float(row.get('FEEAMT'))
        cavaiamt = safe_float(row.get('CAVAIAMT'))
        lntype   = ''

        if prodcd == '34111':
            lntype   = 'HP'
            apprlimt = curbal - (rebate + intearn4)
        else:
            if commno and commno > 0:
                if revovli == 'N':
                    apprlimt = corgamt
                else:
                    apprlimt = ccuramt
            else:
                apprlimt = orgbal

        # Product range check for RC
        if product < 800 or product > 899:
            if prodcd == '34190':
                lntype = 'RC'

        if prodcd not in ('34111', '34190', '34180', '34600'):
            lntype = 'TL'

        apprlim2 = safe_float(row.get('APPRLIM2'))
        if commno and commno not in (None, 0.0) and not math.isnan(commno):
            if lntype == 'RC':
                apprlim2 = ccuramt
            elif lntype == 'TL':
                rleasamt = corgamt if (corgamt - cavaiamt) > corgamt else (corgamt - cavaiamt)
                apprlim2 = balance - feeamt + (apprlimt - rleasamt)
        else:
            if lntype == 'RC':
                apprlim2 = orgbal
            elif lntype == 'TL':
                apprlim2 = balance - feeamt

        if prodcd == '34111':
            apprlim2 = balance - feeamt

        row['LNTYPE']   = lntype
        row['APPRLIMT'] = apprlimt
        row['APPRLIM2'] = apprlim2
        row['UNDRAWN']  = safe_float(row.get('UNUSEAMT'))
        if commno in (None, 0.0) or (isinstance(commno, float) and math.isnan(commno)):
            row['UNDRAWN'] = 0

        rows_out.append(row)

    return pl.DataFrame(rows_out)


# ---------------------------------------------------------------------------
# Build CUM (monthly average balance) from MNICRM
# ---------------------------------------------------------------------------
def build_cum(macros: dict) -> pl.DataFrame:
    """
    *** COMBINE LOAN & CURBALMTD (HMK2) ***
    PROC SORT DATA=MNICRM.LN&REPTMON2&NOWKS ... OUT=PRVCUM
    PROC SORT DATA=MNICRM.LN&REPTMON&NOWK  ... OUT=CUM
    DATA CUM (KEEP=ACCTNO NOTENO CURAVMTH CUBALYTD)
    """
    rm2 = macros['REPTMON2']; nks = macros['NOWKS']
    rm  = macros['REPTMON'];  nk  = macros['NOWK']
    reptday = int(macros['REPTDAY'])
    reptmon2 = macros['REPTMON2']

    prvcum_file = MNICRM_DIR / f"ln{rm2}{nks}.parquet"
    cum_file    = MNICRM_DIR / f"ln{rm}{nk}.parquet"

    cols = ['ACCTNO', 'NOTENO', 'CUBALYTD', 'DAYYTD']
    prvcum = load_parquet(prvcum_file, columns=cols) if prvcum_file.exists() else pl.DataFrame()
    cum_df = (load_parquet(cum_file, columns=cols + ['CURBAL'])
              if cum_file.exists() else pl.DataFrame())

    if prvcum.is_empty() and cum_df.is_empty():
        return pl.DataFrame({'ACCTNO': [], 'NOTENO': [], 'CURAVMTH': [], 'CUBALYTD': []})

    merged = (cum_df.rename({'CUBALYTD': 'BAL', 'DAYYTD': 'DAY'})
              .join(prvcum.rename({'CUBALYTD': 'PRVBAL', 'DAYYTD': 'PRVDAY'}),
                    on=['ACCTNO', 'NOTENO'], how='left'))

    rows_out = []
    for row in merged.to_dicts():
        bal    = safe_float(row.get('BAL'))
        prvbal = safe_float(row.get('PRVBAL'))
        day    = safe_float(row.get('DAY'))
        cubal  = bal if reptmon2 == "12" else (bal + (-1 * prvbal))
        daymtd = reptday
        curavmth = cubal / daymtd if daymtd != 0 else 0
        cubalytd = bal / day if bal > 0 and day else 0
        rows_out.append({'ACCTNO': row['ACCTNO'], 'NOTENO': row['NOTENO'],
                         'CURAVMTH': curavmth, 'CUBALYTD': cubalytd})
    return pl.DataFrame(rows_out)


# ---------------------------------------------------------------------------
# Build ULOAN dataset
# ---------------------------------------------------------------------------
def build_uloan(macros: dict) -> pl.DataFrame:
    """
    DATA ULOAN&RM&NK&YR;
      SET BNM.ULOAN&RM&NK;
      IF (3000000000<=ACCTNO<=3999999999) THEN CRISPURP=PUT(CCRICODE,Z4.)
      RENAME SECTORCD=SECTFISS;
    """
    rm = macros['REPTMON']; nk = macros['NOWK']
    uloan_file = BNM_DIR / f"uloan{rm}{nk}.parquet"
    if not uloan_file.exists():
        return pl.DataFrame()
    df = load_parquet(uloan_file)
    rows_out = []
    for row in df.to_dicts():
        acctno = safe_float(row.get('ACCTNO'))
        if 3000000000 <= acctno <= 3999999999:
            ccricode = safe_int(row.get('CCRICODE'))
            row['CRISPURP'] = f"{ccricode:04d}"
        if 'SECTORCD' in row:
            row['SECTFISS'] = row['SECTORCD']
        rows_out.append(row)
    df = pl.DataFrame(rows_out)
    return df.sort('ACCTNO')


# ---------------------------------------------------------------------------
# %MACRO PROCESS — conditional HP write for specific months
# ---------------------------------------------------------------------------
def macro_process(macros: dict, ihp: pl.DataFrame) -> None:
    """
    %IF REPTMON IN (02,05,08,11) THEN
       DATA HPWO.IHP&RM&NK&YR; SET HP.IHP&RM&NK&YR;
    """
    if macros['REPTMON'] in ("02", "05", "08", "11"):
        rm = macros['REPTMON']; nk = macros['NOWK']; yr = macros['REPTYEAR']
        out = HPWO_DIR / f"ihp{rm}{nk}{yr}.parquet"
        ihp.write_parquet(out)


# ---------------------------------------------------------------------------
# HP_PRODUCTS set (from PBBELF %INC -- &HP macro variable)
# These are the Hire Purchase product codes referenced as &HP throughout
# ---------------------------------------------------------------------------
# &HP product set from PBBELF -- representative set; full set in PBBELF
HP_PRODUCTS = {
    678, 679, 698, 699, 983, 993, 996,   # IHPWO split products
    # additional HP products would be defined in PBBELF
}


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def main():
    print("=== EIIWLNW1: Loans Data Warehouse Extraction ===")

    # -----------------------------------------------------------------------
    # GET REPTDATE
    # -----------------------------------------------------------------------
    macros = get_reptdate()
    reptdate_sas = macros['REPTDATE']
    print(f"REPTDATE: {macros['RDATE']}  NOWK:{macros['NOWK']}  "
          f"REPTMON:{macros['REPTMON']}  REPTYEAR:{macros['REPTYEAR']}")

    rm  = macros['REPTMON']
    nk  = macros['NOWK']
    yr  = macros['REPTYEAR']
    rm1 = macros['REPTMON1']

    # Save REPTDATE parquet
    reptdate_df = pl.DataFrame({'REPTDATE': [reptdate_sas]})
    reptdate_df.write_parquet(LOAN_OUT_DIR / "reptdate.parquet")

    # -----------------------------------------------------------------------
    # %MACRO GET_FEE -> LNNOTE
    # -----------------------------------------------------------------------
    print("Loading LNNOTE (%GET_FEE) ...")
    lnnote = macro_get_fee(macros)

    # -----------------------------------------------------------------------
    # DATA HIST (HISTFILE binary) + HIST.UP2DEC* + sort/dedup
    # DATA REFNOTE (REFNOTE fixed-width)
    # DATA LNNOTE: MERGE LNNOTE + HIST + REFNOTE
    # -----------------------------------------------------------------------
    print("Building LNNOTE with HIST + REFNOTE ...")
    lnnote = build_lnnote(lnnote, reptdate_sas)

    # -----------------------------------------------------------------------
    # *** APPROVED LIMIT AND UNDRAWN AMT ***
    # MERGE LNNOTE + BNM1.LNCOMM
    # -----------------------------------------------------------------------
    print("Merging LNNOTE with LNCOMM ...")
    lnnote = merge_lnnote_comm(lnnote, reptdate_sas)

    # -----------------------------------------------------------------------
    # *** NPL IND ***
    # MERGE LNNOTE + MNINPL.TOTIIS&REPTMON1
    # -----------------------------------------------------------------------
    print("Merging NPL indicator ...")
    lnnote = merge_lnnote_npl(lnnote, rm1)

    # -----------------------------------------------------------------------
    # DATA LOAN: SET BNM.LOAN&RM&NK BNM.LNWOD&RM&NK BNM.LNWOF&RM&NK
    # -----------------------------------------------------------------------
    print("Loading LOAN base datasets ...")
    loan = load_loan_base(macros)

    # -----------------------------------------------------------------------
    # %MACRO GET_GP3 -- risk grade for week 4
    # -----------------------------------------------------------------------
    loan = macro_get_gp3(loan, macros)

    # -----------------------------------------------------------------------
    # OVERDFT dataset (from MNITB.CURRENT + ODGP3.OVERDFT)
    # -----------------------------------------------------------------------
    print("Building OVERDFT ...")
    overdft = build_overdft(reptdate_sas)
    if not overdft.is_empty():
        loan = loan.join(overdft, on='ACCTNO', how='left', suffix='_OD')
        loan = loan.with_columns(
            pl.col('APPRLIM2').fill_null(0))

    # -----------------------------------------------------------------------
    # DATA LOAN; MERGE LOAN + MISMLN.LNVG&REPTMON; BY ACCTNO NOTENO; IF A
    # -----------------------------------------------------------------------
    lnvg_file = MISMLN_DIR / f"lnvg{rm}.parquet"
    if lnvg_file.exists():
        lnvg = load_parquet(lnvg_file)
        loan = loan.join(lnvg, on=['ACCTNO', 'NOTENO'], how='left', suffix='_VG')

    # -----------------------------------------------------------------------
    # DATA LOAN: MERGE LOAN + LNNOTE (large derived-field block)
    # -----------------------------------------------------------------------
    print("Merging LOAN with LNNOTE ...")
    loan = merge_loan_lnnote(loan, lnnote, macros['RDATE'], reptdate_sas)

    # -----------------------------------------------------------------------
    # DATA LOAN; SET LOAN; IF MATUREDT=EXPRDATE THEN MATUREDT=.
    # (already done in merge_loan_lnnote)
    # -----------------------------------------------------------------------

    # -----------------------------------------------------------------------
    # PROC SORT DATA=LOAN; BY ACCTNO NOTENO
    # PROC SORT DATA=LNFILE.TOTPAY OUT=TOTPAY(DROP=DATE); BY ACCTNO NOTENO
    # DATA LOAN; MERGE LOAN + TOTPAY; BY ACCTNO NOTENO; IF A
    # -----------------------------------------------------------------------
    loan = loan.sort(['ACCTNO', 'NOTENO'])
    totpay_file = LNFILE_DIR / "totpay.parquet"
    if totpay_file.exists():
        totpay = load_parquet(totpay_file)
        if 'DATE' in totpay.columns:
            totpay = totpay.drop('DATE')
        loan = loan.join(totpay, on=['ACCTNO', 'NOTENO'], how='left', suffix='_TP')

    # -----------------------------------------------------------------------
    # *-- EXTRACT SECTOR FIELD FOR OD --*
    # DATA OVDFT; MERGE LOAN + OVDFT; BY ACCTNO; IF A
    # -----------------------------------------------------------------------
    ovdft_sec = build_ovdft_sector(positive=False)
    loan = loan.join(ovdft_sec.rename({'SECTOR': 'SECTOR_OD'}),
                     on='ACCTNO', how='left')

    # -----------------------------------------------------------------------
    # DATA LOAN (DROP=CENSUS THISDATE DAYARR_MO) -- derive CENSUS, DAYARR, MTHARR etc.
    # -----------------------------------------------------------------------
    print("Deriving LOAN fields (CENSUS, DAYARR, MTHARR) ...")
    loan = derive_loan_fields(loan, macros)

    # -----------------------------------------------------------------------
    # DATA NAMEX (from BNM1.NAME8) + merge into LOAN
    # -----------------------------------------------------------------------
    print("Building NAMEX ...")
    namex = build_namex()
    loan  = loan.sort('ACCTNO')
    loan  = loan.join(namex, on='ACCTNO', how='left', suffix='_NX')

    # -----------------------------------------------------------------------
    # DATA NOTEX (from BNM1.LNNOTE) + merge into LOAN
    # DATA LOAN2; MERGE LOANX + NOTEX; BY ACCTNO NOTENO; IF A
    # -----------------------------------------------------------------------
    print("Building NOTEX ...")
    notex = build_notex()
    loan  = loan.sort(['ACCTNO', 'NOTENO'])
    loan  = loan.join(notex, on=['ACCTNO', 'NOTENO'], how='left', suffix='_NTX')

    # -----------------------------------------------------------------------
    # DATA CISD + CIS (customer master)
    # DATA LOAN; MERGE CIS LOAN(IN=A); BY ACCTNO; IF A; RENAME REMAINMH=REMAINMT
    # -----------------------------------------------------------------------
    print("Building CIS/CISD ...")
    # cisd = build_cisd()   -- CISD used for LNR1 fields; merged into final output
    cis  = build_cis()
    loan = loan.join(cis, on='ACCTNO', how='left', suffix='_CIS')
    if 'REMAINMH' in loan.columns:
        loan = loan.rename({'REMAINMH': 'REMAINMT'})

    # -----------------------------------------------------------------------
    # DATA LOAN (PAYEFDT date clamping from PAYEFDTO string parts)
    # -----------------------------------------------------------------------
    rows_out = []
    for row in loan.to_dicts():
        payefdto = safe_str(row.get('PAYEFDTO'), '')
        if payefdto and len(payefdto) >= 8:
            try:
                payd = int(payefdto[0:2])
                paym = int(payefdto[3:5])
                payy_2 = int(payefdto[6:8])
                payy = 2000 + payy_2 if payy_2 < 50 else 1900 + payy_2
                row['PAYEFDT'] = adjust_payefdt(payd, paym, payy)
            except (ValueError, IndexError):
                pass
        rows_out.append(row)
    loan = pl.DataFrame(rows_out)

    # -----------------------------------------------------------------------
    # Read PAYFI binary + merge PAYFI -> LOAN (PAYFI takes priority)
    # DATA LOAN; MERGE PAYFI LOAN(IN=A); BY ACCTNO NOTENO; IF A
    # -----------------------------------------------------------------------
    print("Reading PAYFI binary file ...")
    payfi = read_payfi()
    loan  = loan.sort(['ACCTNO', 'NOTENO'])
    loan  = payfi.join(loan, on=['ACCTNO', 'NOTENO'], how='right', suffix='_LN')
    # PAYFI.PAYEFDT takes precedence (left join)
    if 'PAYEFDT_LN' in loan.columns:
        loan = loan.with_columns(
            pl.when(pl.col('PAYEFDT').is_not_null())
            .then(pl.col('PAYEFDT'))
            .otherwise(pl.col('PAYEFDT_LN'))
            .alias('PAYEFDT')
        ).drop('PAYEFDT_LN')

    # -----------------------------------------------------------------------
    # *** OUTPUT TO RESPECTIVE DATASET ***
    # DATA LOAN.ILN&RM&NK&YR (DROP=NAME)
    # -----------------------------------------------------------------------
    ilnname = f"iln{rm}{nk}{yr}"
    print(f"Writing LOAN.{ilnname} ...")
    loan_out = loan.clone()
    if 'NAME' in loan_out.columns:
        loan_out = loan_out.drop('NAME')
    for drop_col in ('LASTRAN', 'PAYEFFDT', 'SBA', 'FEEAMT2', 'MORTGIND'):
        if drop_col in loan_out.columns:
            loan_out = loan_out.drop(drop_col)
    loan_out.write_parquet(LOAN_OUT_DIR / f"{ilnname}.parquet", compression='zstd')

    # -----------------------------------------------------------------------
    # DATA LOAN.ILNPD&RM&NK&YR (paid-off / reversed notes)
    # -----------------------------------------------------------------------
    print(f"Building LOAN.ILNPD{rm}{nk}{yr} ...")
    ilnpd = build_ilnpd(macros)

    # MERGE ILNPD + LNCOMM -> LNPDX
    print("Building LNPDX (HP detection / approved limits) ...")
    lnpdx = build_lnpdx(ilnpd, macros)

    # PROC SORT DATA=NAMEX; MERGE LNPDX + NAMEX
    lnpdx = lnpdx.sort('ACCTNO')
    lnpdx = lnpdx.join(namex, on='ACCTNO', how='left', suffix='_NX2')

    # PROC SORT DATA=BNM2.MTDINT OUT=MTD; MERGE LNPDX + MTD
    mtd_file = BNM2_DIR / "mtdint.parquet"
    if mtd_file.exists():
        mtd = load_parquet(mtd_file)
        mtd = mtd.sort(['ACCTNO', 'NOTENO'])
        lnpdx = lnpdx.sort(['ACCTNO', 'NOTENO'])
        lnpdx = lnpdx.join(mtd, on=['ACCTNO', 'NOTENO'], how='left', suffix='_MTD')
        lnpdx = lnpdx.with_columns(pl.col('MTDINT').fill_null(0))

    ilnpd_name = f"ilnpd{rm}{nk}{yr}"
    print(f"Writing LOAN.{ilnpd_name} ...")
    lnpdx_out = lnpdx.clone()
    for drop_col in ('NAME', 'RSN', 'CPNSTDTE', 'ORGISSDTE'):
        if drop_col in lnpdx_out.columns:
            lnpdx_out = lnpdx_out.drop(drop_col)
    lnpdx_out.write_parquet(LOAN_OUT_DIR / f"{ilnpd_name}.parquet", compression='zstd')

    # -----------------------------------------------------------------------
    # *-- EXTRACT HP AND HPDEALER --*
    # DATA IHP; SET LOAN; IF PRODUCT IN &HP
    # -----------------------------------------------------------------------
    print("Extracting HP records ...")
    # &HP product set from PBBELF (%INC PGM(PBBELF))
    if 'PRODUCT' in loan.columns:
        ihp = loan.filter(pl.col('PRODUCT').is_in(list(HP_PRODUCTS)))
    else:
        ihp = pl.DataFrame()

    # -----------------------------------------------------------------------
    # *-- EXTRACT WOFF INFO INTO HP DATASET --*
    # DATA WOFFTOT WOFFTOT1; SET WOFF.WOFFTOT;
    # -----------------------------------------------------------------------
    wofftot_file = WOFF_DIR / "wofftot.parquet"
    if wofftot_file.exists() and not ihp.is_empty():
        wofftot_all = load_parquet(wofftot_file)
        wofftot  = wofftot_all.filter(pl.col('NOTENO').is_null())
        wofftot1 = wofftot_all.filter(pl.col('NOTENO').is_not_null())
        # Merge by ACCTNO (WOFFTOT -- no NOTENO)
        ihp = ihp.sort('ACCTNO')
        wofftot = wofftot.drop('NOTENO') if 'NOTENO' in wofftot.columns else wofftot
        ihp = ihp.join(wofftot.sort('ACCTNO'), on='ACCTNO', how='left', suffix='_WO')
        # Merge by ACCTNO + NOTENO (WOFFTOT1)
        ihp = ihp.sort(['ACCTNO', 'NOTENO'])
        ihp = ihp.join(wofftot1.sort(['ACCTNO', 'NOTENO']),
                       on=['ACCTNO', 'NOTENO'], how='left', suffix='_WO1')

    # -----------------------------------------------------------------------
    # DATA HP.IHP&RM&NK&YR + HP.IHPWO&RM&NK&YR (write-off HP split)
    # -----------------------------------------------------------------------
    hp_drop = {'LATENOTICE', 'GUARNOTICE', 'INTSTDTE', 'USERID',
               'POSTDATE', 'SM_STATUS', 'SM_DATE', 'RR_IL_RECLASS_DT'}
    wo_products = {678, 679, 698, 699, 983, 993, 996}

    if not ihp.is_empty():
        ihp_clean = ihp.clone()
        for c in hp_drop:
            if c in ihp_clean.columns:
                ihp_clean = ihp_clean.drop(c)
        ihpwo_mask = pl.col('PRODUCT').is_in(list(wo_products))
        ihpwo = ihp_clean.filter(ihpwo_mask)
        ihp_main = ihp_clean.filter(~ihpwo_mask)

        ihp_name  = f"ihp{rm}{nk}{yr}"
        ihpwo_name = f"ihpwo{rm}{nk}{yr}"
        print(f"Writing HP.{ihp_name} ({ihp_main.height} rows) ...")
        ihp_main.write_parquet(HP_OUT_DIR / f"{ihp_name}.parquet", compression='zstd')
        print(f"Writing HP.{ihpwo_name} ({ihpwo.height} rows) ...")
        ihpwo.write_parquet(HP_OUT_DIR / f"{ihpwo_name}.parquet", compression='zstd')

    # -----------------------------------------------------------------------
    # DATA HP.IHPPD&RM&NK&YR (HP paid-off subset from ILNPD)
    # -----------------------------------------------------------------------
    ihppd_name = f"ihppd{rm}{nk}{yr}"
    if 'PRODUCT' in lnpdx_out.columns:
        ihppd = lnpdx_out.filter(pl.col('PRODUCT').is_in(list(HP_PRODUCTS)))
        for drop_col in ('NAME', 'RR_IL_RECLASS_DT', 'PRIMOFHP', 'MO_MAIN_DT'):
            if drop_col in ihppd.columns:
                ihppd = ihppd.drop(drop_col)
        print(f"Writing HP.{ihppd_name} ({ihppd.height} rows) ...")
        ihppd.write_parquet(HP_OUT_DIR / f"{ihppd_name}.parquet", compression='zstd')

    # -----------------------------------------------------------------------
    # DATA HP.IHPCO&RM&NK&YR (HP component from BNM1.HPCOMP)
    # -----------------------------------------------------------------------
    hpcomp_file = BNM1_DIR / "hpcomp.parquet"
    if hpcomp_file.exists():
        hpco = load_parquet(hpcomp_file)
        ihpco_name = f"ihpco{rm}{nk}{yr}"
        print(f"Writing HP.{ihpco_name} ...")
        hpco.write_parquet(HP_OUT_DIR / f"{ihpco_name}.parquet", compression='zstd')

    # -----------------------------------------------------------------------
    # *** COMBINE LOAN & CURBALMTD (HMK2) ***
    # Build CUM + merge into LOAN.ILN and LOAN.ILNPD
    # -----------------------------------------------------------------------
    print("Building CUM (monthly average balance) ...")
    cum = build_cum(macros)

    # DATA LOAN.ILN&RM&NK&YR (COMPRESS=YES) -- merge with CUM, split HP
    iln_base = load_parquet(LOAN_OUT_DIR / f"{ilnname}.parquet")
    iln_base = iln_base.sort(['ACCTNO', 'NOTENO'])
    iln_merged = iln_base.join(cum, on=['ACCTNO', 'NOTENO'], how='left', suffix='_CUM')
    for drop_col in ('PAYD', 'PAYM', 'PAYY', 'ORGISSDTE'):
        if drop_col in iln_merged.columns:
            iln_merged = iln_merged.drop(drop_col)
    # Filter out HP write-off products for LOAN.ILN
    if 'PRODUCT' in iln_merged.columns:
        iln_final = iln_merged.filter(~pl.col('PRODUCT').is_in(list(wo_products)))
    else:
        iln_final = iln_merged
    iln_final.write_parquet(LOAN_OUT_DIR / f"{ilnname}.parquet", compression='zstd')
    # Work copy (unfiltered)
    iln_merged.write_parquet(OUTPUT_DIR / f"{ilnname}.parquet", compression='zstd')
    print(f"Updated LOAN.{ilnname} ({iln_final.height} rows, excl. WO products)")

    # DATA LOAN.ILNPD&RM&NK&YR -- merge with CUM
    ilnpd_base = load_parquet(LOAN_OUT_DIR / f"{ilnpd_name}.parquet")
    ilnpd_base = ilnpd_base.sort(['ACCTNO', 'NOTENO'])
    ilnpd_merged = ilnpd_base.join(cum, on=['ACCTNO', 'NOTENO'], how='left', suffix='_CUM')
    for drop_col in ('NAME', 'ORGISSDTE', 'RSN', 'CPNSTDTE'):
        if drop_col in ilnpd_merged.columns:
            ilnpd_merged = ilnpd_merged.drop(drop_col)
    # IF LASTTRAN < &SDATE THEN CURAVMTH = 0
    sdate_sas = macros['SDATE_SAS']
    if 'LASTTRAN' in ilnpd_merged.columns and 'CURAVMTH' in ilnpd_merged.columns:
        ilnpd_merged = ilnpd_merged.with_columns(
            pl.when(pl.col('LASTTRAN') < sdate_sas)
            .then(0.0)
            .otherwise(pl.col('CURAVMTH'))
            .alias('CURAVMTH')
        )
    ilnpd_merged.write_parquet(LOAN_OUT_DIR / f"{ilnpd_name}.parquet", compression='zstd')
    # Work copy
    ilnpd_merged.write_parquet(OUTPUT_DIR / f"{ilnpd_name}.parquet", compression='zstd')
    print(f"Updated LOAN.{ilnpd_name} ({ilnpd_merged.height} rows)")

    # -----------------------------------------------------------------------
    # DATA OVDFT1 + ULOAN
    # -----------------------------------------------------------------------
    print("Building ULOAN ...")
    ovdft1 = build_ovdft_sector(positive=True)
    uloan  = build_uloan(macros)
    if not uloan.is_empty():
        uloan = uloan.join(ovdft1, on='ACCTNO', how='left', suffix='_OD1')
        # IF SECTOR = ' ' THEN SECTOR = SECTPORI
        if 'SECTOR' in uloan.columns and 'SECTPORI' in uloan.columns:
            uloan = uloan.with_columns(
                pl.when(pl.col('SECTOR').str.strip_chars() == '')
                .then(pl.col('SECTPORI'))
                .otherwise(pl.col('SECTOR'))
                .alias('SECTOR')
            )
        for drop_col in ('SECTPORI',):
            if drop_col in uloan.columns:
                uloan = uloan.drop(drop_col)

        uloan_name = f"uloan{rm}{nk}{yr}"
        uloan.write_parquet(LOAN_OUT_DIR / f"{uloan_name}.parquet", compression='zstd')
        uloan.write_parquet(OUTPUT_DIR   / f"{uloan_name}.parquet", compression='zstd')
        print(f"Written LOAN.{uloan_name} ({uloan.height} rows)")

    # -----------------------------------------------------------------------
    # %MACRO PROCESS -- conditional HP copy for quarter-end months
    # -----------------------------------------------------------------------
    if not ihp.is_empty():
        macro_process(macros, ihp)

    # -----------------------------------------------------------------------
    # ****** END ********************
    # PROC CPORT (transport to FTP datasets) --
    # Output parquets serve as the Python equivalent of PROC CPORT files.
    # Files written:
    #   LOAN_OUT_DIR/iln{rm}{nk}{yr}.parquet
    #   LOAN_OUT_DIR/ilnpd{rm}{nk}{yr}.parquet
    #   LOAN_OUT_DIR/uloan{rm}{nk}{yr}.parquet
    #   HP_OUT_DIR/ihp{rm}{nk}{yr}.parquet  + ihpwo/ihppd/ihpco variants
    # -----------------------------------------------------------------------
    print("=== EIIWLNW1 complete ===")


if __name__ == "__main__":
    main()
