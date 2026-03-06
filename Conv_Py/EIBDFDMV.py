#!/usr/bin/env python3
"""
Program  : EIBDFDMV.py
Purpose  : Monthly Fixed Deposit Interest Rates Exception Report.
           Identifies FD accounts with total receipts above RM 1.0 million
            and compares offered rate vs counter rate.
           Produces:
             - PROC REPORT: detail listing by NAME / ACCTNO with break totals
             - PROC PRINT:  summary count and total amount (XNUM)
           Output written to: eibdfdmv_report.txt
"""

# ============================================================================
# No %INC program dependencies declared in SAS source.
# All logic is self-contained within this program.
# ============================================================================

import struct
import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime, timedelta
from itertools import groupby

# ============================================================================
# OPTIONS (SAS equivalents)
# YEARCUTOFF=1990  -> 2-digit years 00-89 = 2000-2089, 90-99 = 1990-1999
# NOCENTER NODATE NONUMBER -> handled in report formatting
# ============================================================================

MISSING_NUM = 0

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR   = Path("/data")
FDC_DIR    = BASE_DIR / "fdc"        # FDC libref (output SAS dataset & REPTDATE)
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
FDC_DIR.mkdir(parents=True, exist_ok=True)

# Input flat files
DPFD4001_FILE = BASE_DIR / "input" / "dpfd4001.dat"   # binary PD input (LRECL=1692)
BRANCHFL_FILE = BASE_DIR / "input" / "branchfl.txt"   # branch lookup text file
CUSTFLE_FILE  = BASE_DIR / "input" / "custfle.txt"    # customer file
NAMEFLE_FILE  = BASE_DIR / "input" / "namefle.txt"    # name file
RATEFLE_FILE  = BASE_DIR / "input" / "ratefle.txt"    # rate file

OUTPUT_REPORT = OUTPUT_DIR / "eibdfdmv_report.txt"

# Parquet output (FDC.FD1MC, FDC.REPTDATE)
FDC_REPTDATE_FILE = FDC_DIR / "reptdate.parquet"
FDC_FD1MC_FILE    = FDC_DIR / "fd1mc.parquet"

# ============================================================================
# ASA CARRIAGE CONTROL CONSTANTS
# ============================================================================

ASA_NEWPAGE     = '1'
ASA_SINGLESPACE = ' '
ASA_DOUBLESPACE = '0'

PAGE_LENGTH = 60
PAGE_WIDTH  = 132

# ============================================================================
# PACKED DECIMAL HELPER
# Reads N bytes from a byte buffer at offset (0-based) as packed decimal.
# Optional decimal_places shifts the result.
# ============================================================================

def read_pd(buf: bytes, offset: int, length: int, decimal_places: int = 0):
    """
    Decode IBM packed-decimal (PD) from byte buffer.
    Each byte holds two BCD digits; last nibble is sign (C=+, D=-).
    """
    if offset + length > len(buf):
        return 0
    chunk  = buf[offset: offset + length]
    digits = ""
    for i, b in enumerate(chunk):
        hi = (b >> 4) & 0x0F
        lo =  b       & 0x0F
        if i < length - 1:
            digits += str(hi) + str(lo)
        else:
            digits += str(hi)
            sign = lo
    val = int(digits) if digits else 0
    if sign in (0xD, 0xB):
        val = -val
    if decimal_places:
        val = val / (10 ** decimal_places)
    return val


def read_char(buf: bytes, offset: int, length: int) -> str:
    """Read fixed-length EBCDIC/ASCII character field from byte buffer."""
    try:
        return buf[offset: offset + length].decode("latin-1").strip()
    except Exception:
        return ""

# ============================================================================
# DATA FDC.REPTDATE
#   DTE = TODAY() - 1
#   REPTDATE = DTE
#   REPTDTE  = DD/MM/YYYY  -> REPTDT macro variable ($10.)
# ============================================================================

yesterday    = date.today() - timedelta(days=1)
REPTDT       = yesterday.strftime("%d/%m/%Y")   # PUT(REPTDTE,$10.)
REPTDATE_VAL = yesterday

# Persist FDC.REPTDATE as parquet
reptdate_row = pl.DataFrame({"REPTDATE": [REPTDATE_VAL]})
reptdate_row.write_parquet(FDC_REPTDATE_FILE)

# ============================================================================
# DATA MGRFD
#   INFILE DPFD4001 LRECL=1692
#   Reads packed-decimal records; filters REPTNO=4001, FMTCODE=1
#   Applies INTPLAN/DEPSTA/DEPTNO filters
#   Derives date string fields (DEPND1, DEPNDT, LASTDT, LASTDDT, MATNDT)
# ============================================================================

# Valid INTPLAN ranges: 300-319, 373-384, 500-539
def is_valid_intplan(intplan: int) -> bool:
    return (300 <= intplan <= 319) or (373 <= intplan <= 384) or (500 <= intplan <= 539)


def parse_packed_date_z11(val: int) -> tuple:
    """
    Replicate PUT(field,Z11.) -> MMDDCCYY layout used in SAS.
    Z11. on a PD6. date stores as MMDDCCYYYY (11 digits zero-padded).
    Returns (cy, mm, dd) as strings.
    """
    s   = f"{val:011d}"   # zero-pad to 11 digits
    mm  = s[0:2]
    dd  = s[2:4]
    cy  = s[4:8]          # 4-digit year
    return cy, mm, dd


def parse_matdte_z8(val: int) -> tuple:
    """
    Replicate PUT(MATDTE,Z8.) -> CCYYMMDD layout.
    Returns (cy, mm, dd) as strings.
    """
    s  = f"{val:08d}"
    cy = s[0:4]
    mm = s[4:6]
    dd = s[6:8]
    return cy, mm, dd


mgrfd_rows = []

if DPFD4001_FILE.exists():
    LRECL = 1692
    with open(DPFD4001_FILE, "rb") as fh:
        while True:
            buf = fh.read(LRECL)
            if len(buf) < LRECL:
                break

            # @03 BANKNO PD2.   (0-based offset: 2, length 2)
            bankno  = read_pd(buf,  2, 2)
            # @24 REPTNO PD3.   (offset 23, length 3)
            reptno  = read_pd(buf, 23, 3)
            # @27 FMTCODE PD2.  (offset 26, length 2)
            fmtcode = read_pd(buf, 26, 2)

            if reptno != 4001 or fmtcode != 1:
                continue

            # @106 BRANCH  PD4.   offset 105, len 4
            branch   = read_pd(buf, 105, 4)
            # @110 ACCTNO  PD6.   offset 109, len 6
            acctno   = read_pd(buf, 109, 6)
            # @122 CUSTCODE $1.   offset 121, len 1
            custcode = read_char(buf, 121, 1)
            # @124 DEPTNO  PD3.   offset 123, len 3
            deptno   = read_pd(buf, 123, 3)
            # @127 DEPTDT  PD6.   offset 126, len 6
            deptdt   = read_pd(buf, 126, 6)
            # @134 NAME    $15.   offset 133, len 15
            name     = read_char(buf, 133, 15)
            # @149 DEPID   PD6.   offset 148, len 6
            depid    = read_pd(buf, 148, 6)
            # @155 DEPSTA  $1.    offset 154, len 1
            depsta   = read_char(buf, 154, 1)
            # @156 CURBAL  PD6.2  offset 155, len 6, 2 decimals
            curbal   = read_pd(buf, 155, 6, decimal_places=2)
            # @162 AVLINT  PD6.2  offset 161, len 6, 2 decimals (not used further)
            # @179 MATDTE  PD5.   offset 178, len 5
            matdte   = read_pd(buf, 178, 5)
            # @189 RATE1   PD3.3  offset 188, len 3, 3 decimals
            rate1    = read_pd(buf, 188, 3, decimal_places=3)
            # @205 DEPTERM PD2.   offset 204, len 2
            depterm  = read_pd(buf, 204, 2)
            # @207 DEPTEID $1.    offset 206, len 1 (not kept)
            # @208 INTPLAN PD2.   offset 207, len 2
            intplan  = read_pd(buf, 207, 2)
            # @392 LASTMAT PD6.   offset 391, len 6
            lastmat  = read_pd(buf, 391, 6)

            if not acctno:
                continue
            if depsta in ('C', 'P'):
                continue
            if deptno in (77, 78, 95):
                continue
            if not is_valid_intplan(intplan):
                continue

            # IF LASTMAT=0 OR LASTMAT=. THEN LASTMAT=DEPTDT
            if not lastmat:
                lastmat = deptdt

            balanc = curbal

            # Derive deposit date strings from DEPTDT (PD6.)
            depdcy, depdmm, depddd = parse_packed_date_z11(deptdt)
            depnd1  = depdcy + depdmm + depddd          # CCYYMMDD
            depndt  = depddd + '/' + depdmm + '/' + depdcy  # DD/MM/CCYY

            # Derive last maturity date strings from LASTMAT (PD6.)
            lastcy, lastmm, lastdd = parse_packed_date_z11(lastmat)
            lastdt  = lastcy  + lastmm + lastdd          # CCYYMMDD
            lastddt = lastdd  + lastmm + lastcy           # DDMMCCYY

            # Derive maturity date strings from MATDTE (PD5.)
            matcy, matmm, matdd = parse_matdte_z8(matdte)
            matndt = matdd + '/' + matmm + '/' + matcy   # DD/MM/CCYY

            mgrfd_rows.append({
                "BRANCH":   branch,
                "ACCTNO":   acctno,
                "CUSTCODE": custcode,
                "DEPTNO":   deptno,
                "DEPTDT":   deptdt,
                "NAME":     name,
                "DEPID":    depid,
                "DEPSTA":   depsta,
                "CURBAL":   curbal,
                "MATDTE":   matdte,
                "RATE1":    rate1,
                "DEPTERM":  depterm,
                "INTPLAN":  intplan,
                "BALANC":   balanc,
                "DEPND1":   depnd1,
                "DEPNDT":   depndt,
                "LASTDT":   lastdt,
                "LASTDDT":  lastddt,
                "MATNDT":   matndt,
            })

mgrfd = sorted(mgrfd_rows, key=lambda r: r["BRANCH"])

# ============================================================================
# DATA BRABR
#   INFILE BRANCHFL
#   INPUT @02 BRANCH 3. @06 BRN $3.
# ============================================================================

brabr: dict = {}   # BRANCH -> BRN
if BRANCHFL_FILE.exists():
    with open(BRANCHFL_FILE, "r", encoding="latin-1") as fh:
        for line in fh:
            if len(line) >= 8:
                try:
                    branch = int(line[1:4].strip())
                    brn    = line[5:8].strip()
                    brabr[branch] = brn
                except ValueError:
                    pass

# ============================================================================
# DATA MRGFDBR
#   MERGE MGRFD(IN=A) BRABR; BY BRANCH; IF A
# ============================================================================

mrgfdbr = []
for r in mgrfd:
    r["BRN"] = brabr.get(r["BRANCH"], "")
    mrgfdbr.append(r)

# Sort by ACCTNO
mrgfdbr.sort(key=lambda r: r["ACCTNO"])

# ============================================================================
# DATA CUSTM1
#   INFILE CUSTFLE
#   INPUT @01 ACCTNO 11. @13 CUSTNO 11. @30 CUSTID 1.
# DATA CUSTM (KEEP=ACCTNO CUSTNO)
#   BY ACCTNO; IF FIRST.ACCTNO AND CUSTID=1 THEN OUTPUT
# ============================================================================

custm: dict = {}   # ACCTNO -> CUSTNO
if CUSTFLE_FILE.exists():
    seen_acct: set = set()
    with open(CUSTFLE_FILE, "r", encoding="latin-1") as fh:
        for line in fh:
            if len(line) < 31:
                continue
            try:
                acctno_c = int(line[0:11].strip()  or 0)
                custno_c = int(line[12:23].strip()  or 0)
                custid_c = int(line[29:30].strip()  or 0)
            except ValueError:
                continue
            if acctno_c not in seen_acct:
                seen_acct.add(acctno_c)
                if custid_c == 1:
                    custm[acctno_c] = custno_c

# ============================================================================
# DATA MGRCU
#   MERGE MRGFDBR(IN=A) CUSTM(IN=B); BY ACCTNO; IF A
# ============================================================================

mgrcu = []
for r in mrgfdbr:
    r["CUSTNO"] = custm.get(r["ACCTNO"], None)
    mgrcu.append(r)

mgrcu.sort(key=lambda r: (r["CUSTNO"] or 0))

# ============================================================================
# DATA NAMES
#   INFILE NAMEFLE
#   INPUT @01 CUSTNO 11. @12 NAMEL $40.
# ============================================================================

names: dict = {}   # CUSTNO -> NAMEL
if NAMEFLE_FILE.exists():
    with open(NAMEFLE_FILE, "r", encoding="latin-1") as fh:
        for line in fh:
            if len(line) < 12:
                continue
            try:
                custno_n = int(line[0:11].strip() or 0)
                namel_n  = line[11:51].strip()
                names[custno_n] = namel_n
            except ValueError:
                pass

# ============================================================================
# DATA MGRCN
#   MERGE MGRCU(IN=C) NAMES(IN=D); BY CUSTNO; IF C
#   IF NAMEL=' ' THEN NAMEQ=NAME; ELSE NAMEQ=NAMEL
# ============================================================================

mgrcn = []
for r in mgrcu:
    custno = r.get("CUSTNO")
    namel  = names.get(custno, "") if custno else ""
    r["NAMEQ"] = r["NAME"] if not namel else namel
    mgrcn.append(r)

mgrcn.sort(key=lambda r: r["NAMEQ"])

# ============================================================================
# PROC SUMMARY DATA=MGRCN NWAY; CLASS NAMEQ; VAR CURBAL; SUM= -> NMBAL (AMOUNT)
# DATA GT25COM: SET NMBAL; IF AMOUNT < 1000000.01
#   (keeps customers with total < 1,000,000.01)
# DATA MRGFL &FDDAT
#   MERGE MGRCN GT25COM(IN=F); BY NAMEQ; IF F THEN DELETE
#   (keeps only customers NOT in GT25COM -> AMOUNT >= 1,000,000.01)
# ============================================================================

# Aggregate CURBAL by NAMEQ
nameq_totals: dict = {}
for r in mgrcn:
    nq = r["NAMEQ"]
    nameq_totals[nq] = nameq_totals.get(nq, 0.0) + r["CURBAL"]

# gt25com = set of NAMEQ where AMOUNT < 1,000,000.01
gt25com_set = {nq for nq, amt in nameq_totals.items() if amt < 1_000_000.01}

# MRGFL: keep rows where NAMEQ NOT in gt25com (i.e. AMOUNT >= 1,000,000.01)
FDDAT_KEEP = {"NAMEQ","BRANCH","BRN","ACCTNO","DEPID","DEPTDT","MATDTE","BALANC",
              "DEPTNO","DEPSTA","CURBAL","RATE1","CUSTCODE","INTPLAN","CUSTNO",
              "DEPND1","DEPNDT","DEPTERM","MATNDT","LASTDT","LASTDDT"}

mrgfl = [
    {k: v for k, v in r.items() if k in FDDAT_KEEP}
    for r in mgrcn
    if r["NAMEQ"] not in gt25com_set
]
mrgfl.sort(key=lambda r: r["INTPLAN"])

# ============================================================================
# DATA RATE01
#   INFILE RATEFLE
#   INPUT @04 INTPLAN 3. @16 RATE $5. @21 EDATE $8.  (CCYYMMDD)
#   Filter: valid INTPLAN ranges
# PROC SORT BY INTPLAN DESCENDING EDATE
# DATA RATE02: retain REC counter per INTPLAN group
# DATA RATE03: IF 1<=REC<=5 THEN RATEDT=RATE||EDATE; output
# PROC TRANSPOSE -> RATE04 (COL1..COL5 per INTPLAN)
# DATA RATE05: split each COL into R (5 chars) and D (8 chars), R/D per slot
# ============================================================================

rate01_rows = []
if RATEFLE_FILE.exists():
    with open(RATEFLE_FILE, "r", encoding="latin-1") as fh:
        for line in fh:
            if len(line) < 29:
                continue
            try:
                intplan_r = int(line[3:6].strip()  or 0)
                rate_r    = line[15:20].strip()
                edate_r   = line[20:28].strip()
            except ValueError:
                continue
            if is_valid_intplan(intplan_r):
                rate01_rows.append({
                    "INTPLAN": intplan_r,
                    "RATE":    rate_r,
                    "EDATE":   edate_r,
                })

# Sort BY INTPLAN DESCENDING EDATE
rate01_rows.sort(key=lambda r: (r["INTPLAN"], r["EDATE"]), reverse=False)
rate01_rows.sort(key=lambda r: r["INTPLAN"])
# Stable descending EDATE within each INTPLAN
from itertools import groupby as _groupby
rate01_sorted = []
for ip, grp in _groupby(rate01_rows, key=lambda r: r["INTPLAN"]):
    rate01_sorted.extend(sorted(grp, key=lambda r: r["EDATE"], reverse=True))

# DATA RATE02/RATE03: keep up to 5 RATEDT per INTPLAN
rate05: dict = {}   # INTPLAN -> {R1D..R5D, D1..D5}
for ip, grp in _groupby(rate01_sorted, key=lambda r: r["INTPLAN"]):
    slots = list(grp)[:5]
    entry: dict = {}
    for idx in range(1, 6):
        if idx <= len(slots):
            rate_str  = slots[idx - 1]["RATE"].zfill(5)
            edate_str = slots[idx - 1]["EDATE"]
            entry[f"R{idx}D"] = float(rate_str) / 1000.0
            entry[f"D{idx}"]  = edate_str
        else:
            entry[f"R{idx}D"] = 0.0
            entry[f"D{idx}"]  = ""
    rate05[ip] = entry

# ============================================================================
# DATA MGRRAT
#   MERGE RATE05 MRGFL(IN=A); BY INTPLAN; IF A
#   Rate selection: LASTDT > D1 -> NR=R1D, else > D2 -> NR=R2D, ... else NR=R5D
#   RI='Y' if RATE1=NR else RI='N'
#   OFFRDT = OFFRDD||OFFRMM||OFFRCY
# ============================================================================

mgrrat_rows = []
for r in mrgfl:
    ip   = r["INTPLAN"]
    rats = rate05.get(ip, {f"R{i}D": 0.0 for i in range(1,6)} |
                          {f"D{i}":  ""   for i in range(1,6)})
    lastdt = r.get("LASTDT", "")

    # Rate selection by comparing LASTDT (CCYYMMDD) with D slots
    nr = rats["R5D"]
    rd = rats["D5"]
    for i in range(1, 5):
        if lastdt > rats[f"D{i}"]:
            nr = rats[f"R{i}D"]
            rd = rats[f"D{i}"]
            break

    offrcy = rd[0:4] if len(rd) >= 8 else ""
    offrmm = rd[4:6] if len(rd) >= 8 else ""
    offrdd = rd[6:8] if len(rd) >= 8 else ""
    offrdt = offrdd + offrmm + offrcy

    ri = 'Y' if r["RATE1"] == nr else 'N'

    mgrrat_rows.append({**r,
        "NR":      nr,
        "RD":      rd,
        "OFFRDT":  offrdt,
        "RI":      ri,
    })

# Sort BY ACCTNO
mgrrat_rows.sort(key=lambda r: r["ACCTNO"])

# DATA MGRRAT (second pass): REC=0; derive CURBALY/CURBALN; FIRST.ACCTNO -> REC=1
for r in mgrrat_rows:
    r["REC"]     = 0
    r["CURBALY"] = r["CURBAL"] if r["RI"] == 'Y' else 0.0
    r["CURBALN"] = r["CURBAL"] if r["RI"] == 'N' else 0.0

seen_acctno: set = set()
for r in mgrrat_rows:
    if r["ACCTNO"] not in seen_acctno:
        seen_acctno.add(r["ACCTNO"])
        r["REC"] = 1

# FDC.FD1MC: save full mgrrat to parquet
pl.DataFrame(mgrrat_rows).write_parquet(FDC_FD1MC_FILE)

# ============================================================================
# PROC SUMMARY DATA=MGRRAT NWAY; VAR REC CURBAL; SUM= -> XNUM
# ============================================================================

total_rec    = sum(r["REC"]    for r in mgrrat_rows)
total_curbal = sum(r["CURBAL"] for r in mgrrat_rows)

# ============================================================================
# PROC SORT DATA=MGRRAT BY NAMEQ ACCTNO MATDTE
# PROC REPORT DATA=MGRRAT NOWD HEADSKIP HEADLINE SPLIT='*'
#   COLUMN NAMEQ BRN ACCTNO DEPID CURBAL DEPTERM DEPNDT MATNDT RATE1 NR CURBALN CURBALY
#   GROUP: NAMEQ BRN ACCTNO (with break subtotals after ACCTNO and NAMEQ)
# ============================================================================

mgrrat_rows.sort(key=lambda r: (r["NAMEQ"], r["ACCTNO"], r["MATDTE"]))


def fmt_comma(val, width: int = 18, dec: int = 2) -> str:
    try:
        return f"{float(val or MISSING_NUM):,.{dec}f}".rjust(width)
    except (TypeError, ValueError):
        return "0.00".rjust(width)


def fmt_comma7(val) -> str:
    try:
        return f"{float(val or MISSING_NUM):,.2f}".rjust(7)
    except (TypeError, ValueError):
        return "0.00".rjust(7)


def fmt_comma12(val) -> str:
    return fmt_comma(val, width=12, dec=2)


def fmt_int(val, width: int = 10) -> str:
    try:
        return str(int(round(float(val or MISSING_NUM)))).rjust(width)
    except (TypeError, ValueError):
        return "0".rjust(width)


def report_header() -> list:
    lines = []
    lines.append(f"{ASA_NEWPAGE}  P U B L I C   B A N K   B E R H A D")
    lines.append(f"{ASA_SINGLESPACE}  REPORT ID: PDR/D/FDRATE1")
    lines.append(f"{ASA_SINGLESPACE}  REPORT TITLE: MONTHLY FIXED DEPOSIT INTEREST RATES EXCEPTION")
    lines.append(f"{ASA_SINGLESPACE}                REPORT AS AT {REPTDT}")
    lines.append(f"{ASA_SINGLESPACE}  FOR FD ACCOUNTS WITH TOTAL RECEIPTS ABOVE RM 1.0 M")
    lines.append(f"{ASA_DOUBLESPACE}")
    # Column headers (HEADSKIP HEADLINE)
    col_hdr1 = (
        f"{'NAME OF CUSTOMER':<35} "
        f"{'BRN':<3} "
        f"{'ACCOUNT':>10} "
        f"{'RECEIPT':>7} "
        f"{'RECEIPT':>12} "
        f"{'TERM':>2} "
        f"{'DEPOSIT':>10} "
        f"{'MATURITY':>10} "
        f"{'OFFERED':>7} "
        f"{'COUNTER':>7}"
    )
    col_hdr2 = (
        f"{'':35} "
        f"{'':3} "
        f"{'NUMBER':>10} "
        f"{'NUMBER':>7} "
        f"{'AMOUNT':>12} "
        f"{'':2} "
        f"{'DATE':>10} "
        f"{'DATE':>10} "
        f"{'RATE':>7} "
        f"{'RATE':>7}"
    )
    lines.append(f"{ASA_SINGLESPACE}{col_hdr1}")
    lines.append(f"{ASA_SINGLESPACE}{col_hdr2}")
    lines.append(f"{ASA_SINGLESPACE}{'-' * 110}")
    return lines


report_lines = report_header()

# GROUP BY NAMEQ then ACCTNO, iterate with break logic
def break_line(label: str, curbal_sum: float, curbaly_sum: float, curbaln_sum: float) -> list:
    lines = []
    sep   = " " * 30 + "-" * 61
    lines.append(f"{ASA_SINGLESPACE}{sep}")
    detail = (
        f"{' ' * 30}"
        f"{label:<12}{fmt_comma(curbal_sum, 18, 2)}"
        f"  C={fmt_comma12(curbaly_sum)}"
        f"  S={fmt_comma12(curbaln_sum)}"
    )
    lines.append(f"{ASA_SINGLESPACE}{detail}")
    lines.append(f"{ASA_SINGLESPACE}{sep}")
    return lines


for nameq, nq_grp in _groupby(mgrrat_rows, key=lambda r: r["NAMEQ"]):
    nq_rows       = list(nq_grp)
    nq_curbal     = 0.0
    nq_curbaly    = 0.0
    nq_curbaln    = 0.0

    for acctno, acct_grp in _groupby(nq_rows, key=lambda r: r["ACCTNO"]):
        acct_rows  = list(acct_grp)
        acct_curbal  = 0.0
        acct_curbaly = 0.0
        acct_curbaln = 0.0

        for r in acct_rows:
            curbal   = float(r.get("CURBAL")   or 0.0)
            curbaly  = float(r.get("CURBALY")  or 0.0)
            curbaln  = float(r.get("CURBALN")  or 0.0)
            acct_curbal  += curbal
            acct_curbaly += curbaly
            acct_curbaln += curbaln

            row_str = (
                f"{ASA_SINGLESPACE}"
                f"{str(nameq)[:35]:<35} "
                f"{str(r.get('BRN','')):<3} "
                f"{fmt_int(acctno, 10)} "
                f"{fmt_int(r.get('DEPID', 0), 7)} "
                f"{fmt_comma12(curbal)} "
                f"{fmt_int(r.get('DEPTERM', 0), 2)} "
                f"{str(r.get('DEPNDT','')):<10} "
                f"{str(r.get('MATNDT','')):<10} "
                f"{fmt_comma7(r.get('RATE1', 0))} "
                f"{fmt_comma7(r.get('NR', 0))}"
            )
            report_lines.append(row_str)

        # BREAK AFTER ACCTNO
        report_lines.extend(
            break_line("ACCT TOT =", acct_curbal, acct_curbaly, acct_curbaln)
        )
        nq_curbal  += acct_curbal
        nq_curbaly += acct_curbaly
        nq_curbaln += acct_curbaln

    # BREAK AFTER NAMEQ
    report_lines.extend(
        break_line("NAME TOT =", nq_curbal, nq_curbaly, nq_curbaln)
    )

# ============================================================================
# PROC PRINT DATA=XNUM LABEL NOOBS
#   FORMAT REC 12. CURBAL COMMA18.2
#   LABEL REC='NO.OF ACCTS' CURBAL='TOTAL AMOUNT'
# ============================================================================

report_lines.append(f"{ASA_NEWPAGE}")
report_lines.append(f"{ASA_SINGLESPACE}{'NO.OF ACCTS':>20}  {'TOTAL AMOUNT':>20}")
report_lines.append(f"{ASA_SINGLESPACE}{'-'*20}  {'-'*20}")
report_lines.append(
    f"{ASA_SINGLESPACE}{str(int(round(total_rec))).rjust(20)}  {fmt_comma(total_curbal, 20, 2)}"
)

# ============================================================================
# WRITE REPORT OUTPUT
# ============================================================================

with open(OUTPUT_REPORT, "w", encoding="utf-8") as fh:
    fh.write("\n".join(report_lines) + "\n")

print(f"Report written to: {OUTPUT_REPORT}")
print(f"FDC.REPTDATE saved to: {FDC_REPTDATE_FILE}")
print(f"FDC.FD1MC    saved to: {FDC_FD1MC_FILE}")
