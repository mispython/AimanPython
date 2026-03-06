#!/usr/bin/env python3
"""
Program  : EIBMFDIS.py
Purpose  : Monthly Mudharabah General Investment (MGIA) Position Report.
           Reads binary PD flat file; produces 4 positional fixed-width reports:
             EIBMFDIS-1  : MGIA by FD tenure (all products)
             EIBMFDIS-2  : MGIA by branches  (all products)
             EIBMFDIS-3  : Staff MGIA by FD tenure
             EIBMFDIS-4  : Staff MGIA by branches
           Bumi (CUSTCD=77) vs Non-Bumi split in all reports.
           Output written to: eibmfdis_report.txt
"""

# ============================================================================
# No %INC program dependencies declared in SAS source.
# All logic is self-contained within this program.
# ============================================================================

from pathlib import Path
from datetime import date, datetime
from itertools import groupby as _groupby

# ============================================================================
# OPTIONS
# YEARCUTOFF=1990 NOCENTER NODATE NONUMBER
# ============================================================================

MISSING_NUM = 0

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR   = Path("/data")
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DEPOSIT_FILE = BASE_DIR / "input" / "deposit.dat"    # SAP binary PD, LRECL=1693
BRHFILE_FILE = BASE_DIR / "input" / "brhfile.txt"    # branch code lookup LRECL=80

OUTPUT_REPORT = OUTPUT_DIR / "eibmfdis_report.txt"

# ============================================================================
# ASA CARRIAGE CONTROL CONSTANTS
# ============================================================================

ASA_NEWPAGE     = '1'
ASA_SINGLESPACE = ' '
ASA_DOUBLESPACE = '0'

PAGE_WIDTH  = 132

# ============================================================================
# PACKED DECIMAL HELPER
# ============================================================================

def read_pd(buf: bytes, offset: int, length: int, decimal_places: int = 0):
    """Decode IBM packed-decimal from byte buffer (0-based offset)."""
    if offset + length > len(buf):
        return 0
    chunk  = buf[offset: offset + length]
    digits = ""
    sign   = 0xC
    for i, b in enumerate(chunk):
        hi = (b >> 4) & 0x0F
        lo =  b       & 0x0F
        if i < length - 1:
            digits += str(hi) + str(lo)
        else:
            digits += str(hi)
            sign    = lo
    val = int(digits) if digits else 0
    if sign in (0xD, 0xB):
        val = -val
    if decimal_places:
        val = val / (10 ** decimal_places)
    return val


def read_char(buf: bytes, offset: int, length: int) -> str:
    try:
        return buf[offset: offset + length].decode("latin-1")
    except Exception:
        return " " * length

# ============================================================================
# INTPLAN -> MTH mapping (full product set, EIBMFDIS-1/2)
# ============================================================================

INTPLAN_TO_MTH_ALL = {
    340: 1,  448: 1,
    352: 2,  449: 2,
    341: 3,  450: 3,
    353: 4,  451: 4,
    354: 5,  452: 5,
    342: 6,  453: 6,
    355: 7,  454: 7,
    356: 8,  455: 8,
    343: 9,  456: 9,
    357: 10, 457: 10,
    358: 11, 458: 11,
    344: 12, 459: 12,
    588: 13, 461: 13,
    589: 14, 462: 14,
    345: 15, 463: 15,
    590: 16,
    591: 17,
    346: 18, 464: 18,
    592: 19,
    593: 20,
    347: 21, 465: 21,
    594: 22,
    595: 23,
    348: 24, 466: 24,
    596: 25,
    597: 26,
    359: 27,
    598: 28,
    599: 29,
    580: 30,
    581: 33,
    349: 36, 467: 36,
    582: 39,
    583: 42,
    584: 45,
    350: 48, 468: 48,
    585: 51,
    586: 54,
    587: 57,
    351: 60,
}

# INTPLAN -> MTH mapping (staff products only, EIBMFDIS-3/4)
INTPLAN_TO_MTH_STAFF = {
    448: 1,  449: 2,  450: 3,  451: 4,  452: 5,  453: 6,
    454: 7,  455: 8,  456: 9,  457: 10, 458: 11, 459: 12,
    461: 13, 462: 14, 463: 15, 464: 18, 465: 21, 466: 24,
    467: 36, 468: 48,
}

# Valid INTPLAN filter for DATA FD
def is_valid_fd_intplan(ip: int) -> bool:
    return ((340 <= ip <= 359) or
            (448 <= ip <= 459) or
            (461 <= ip <= 469) or
            (580 <= ip <= 599))

# ============================================================================
# DATA FD + DATA REPTDATE
#   INFILE DEPOSIT LRECL=1693
#   BANKNO=33, REPTNO=4001, FMTCODE=1
#   OPENIND IN ('D','O')
# ============================================================================

RDATE = ""   # set from REPTDATE record

fd_rows = []

if DEPOSIT_FILE.exists():
    LRECL = 1693
    with open(DEPOSIT_FILE, "rb") as fh:
        reptdate_read = False
        while True:
            buf = fh.read(LRECL)
            if len(buf) < LRECL:
                break

            # @3  BANKNO  PD2.   offset 2, len 2
            bankno  = read_pd(buf, 2, 2)
            # @24 REPTNO  PD3.   offset 23, len 3
            reptno  = read_pd(buf, 23, 3)
            # @27 FMTCODE PD2.   offset 26, len 2
            fmtcode = read_pd(buf, 26, 2)

            # DATA REPTDATE: read TBDATE from first record @106 PD6.
            if not reptdate_read:
                tbdate = read_pd(buf, 105, 6)
                # INPUT(SUBSTR(PUT(TBDATE,Z11.),1,8),MMDDYY8.)
                s = f"{tbdate:011d}"
                mm_r = s[0:2]; dd_r = s[2:4]; yy_r = s[4:8]
                try:
                    repdate = date(int(yy_r), int(mm_r), int(dd_r))
                    RDATE   = repdate.strftime("%d/%m/%y")  # DDMMYY6. -> DD/MM/YY
                except Exception:
                    RDATE = ""
                reptdate_read = True

            if bankno != 33 or reptno != 4001 or fmtcode != 1:
                continue

            # @106 BRANCH   PD4.   offset 105, len 4
            branch   = read_pd(buf, 105, 4)
            # @110 ACCTNO   PD6.   offset 109, len 6
            acctno   = read_pd(buf, 109, 6)
            # @116 STATEC   $6.    offset 115, len 6 (not used further)
            # @124 CUSTCD   PD3.   offset 123, len 3
            custcd   = read_pd(buf, 123, 3)
            # @134 NAME     $15.   offset 133, len 15 (not used further)
            # @149 CDNO     PD6.   offset 148, len 6 (not used further)
            # @155 OPENIND  $1.    offset 154, len 1
            openind  = read_char(buf, 154, 1).strip()
            # @156 CURBAL   PD6.2  offset 155, len 6, 2 decimals
            curbal   = read_pd(buf, 155, 6, decimal_places=2)
            # @208 INTPLAN  PD2.   offset 207, len 2
            intplan  = read_pd(buf, 207, 2)

            if openind not in ('D', 'O'):
                continue
            if not is_valid_fd_intplan(intplan):
                continue

            fd_rows.append({
                "BRANCH":  branch,
                "ACCTNO":  acctno,
                "CUSTCD":  custcd,
                "OPENIND": openind,
                "CURBAL":  curbal,
                "INTPLAN": intplan,
            })

# ============================================================================
# DATA BRHDATA
#   INFILE BRHFILE LRECL=80
#   INPUT @2 BRANCH 3. @6 BRHCODE $3.
# ============================================================================

brhdata: dict = {}   # BRANCH -> BRHCODE
if BRHFILE_FILE.exists():
    with open(BRHFILE_FILE, "r", encoding="latin-1") as fh:
        for line in fh:
            if len(line) >= 8:
                try:
                    br  = int(line[1:4].strip())
                    brc = line[5:8].strip()
                    brhdata[br] = brc
                except ValueError:
                    pass

# ============================================================================
# MERGE FD + BRHDATA BY BRANCH (IF PRESENT=1)
# Sort by ACCTNO; derive MTH
# ============================================================================

# Sort FD by BRANCH for merge
fd_rows.sort(key=lambda r: r["BRANCH"])

tbmerg1 = []
for r in fd_rows:
    r["BRHCODE"] = brhdata.get(r["BRANCH"], "")
    tbmerg1.append(r)

# Sort by ACCTNO for FIRST.ACCTNO logic
tbmerg1.sort(key=lambda r: r["ACCTNO"])

# Derive MTH (full product map) + count Bumi/Non-Bumi unique accounts
bacctno  = 0
nbacctno = 0
seen_all: set = set()
for r in tbmerg1:
    r["MTH"] = INTPLAN_TO_MTH_ALL.get(r["INTPLAN"], 0)
    if r["ACCTNO"] not in seen_all:
        seen_all.add(r["ACCTNO"])
        if r["CUSTCD"] == 77:
            bacctno += 1
        else:
            nbacctno += 1

BACCTNO_ALL  = bacctno
NBACCTNO_ALL = nbacctno

# ============================================================================
# REPORT OUTPUT HELPERS
# ============================================================================

def fmt_c9(val) -> str:
    """COMMA9. integer."""
    try:
        return f"{int(round(float(val or MISSING_NUM))):,}".rjust(9)
    except (TypeError, ValueError):
        return "0".rjust(9)


def fmt_c15(val) -> str:
    """COMMA15.2 amount."""
    try:
        return f"{float(val or MISSING_NUM):,.2f}".rjust(15)
    except (TypeError, ValueError):
        return "0.00".rjust(15)


def fmt_z3(val) -> str:
    return str(int(val or 0)).zfill(3)


def fmt_z2(val) -> str:
    return str(int(val or 0)).zfill(2)


def report_page_header(report_no: str, title_line2: str, tenure_line: str,
                        page_cnt: int, mth: int = None) -> list:
    """
    Generate NEWPAGE header block matching SAS FILE PRINT HEADER= structure.
    ASA '1' on first line of each page.
    """
    repdt_str = RDATE  # already DDMMYY8. equivalent (DD/MM/YY -> use DD/MM/YYYY for clarity)
    lines = []
    l1 = (f"{ASA_NEWPAGE}"
          f"{'REPORT NO.  : ' + report_no:<49}"
          f"{'P U B L I C   B A N K   B E R H A D':<62}"
          f"REPORT DATE : {repdt_str}")
    l2 = (f"{ASA_SINGLESPACE}"
          f"{' ' * 48}"
          f"{title_line2:<62}"
          f"PAGE NO.   : {page_cnt}")
    lines.append(l1)
    lines.append(l2)
    if tenure_line:
        lines.append(
            f"{ASA_SINGLESPACE}{tenure_line:<39}"
            f"{'MONTHLY MUDHARABAH GENERAL INVESTMENT POSITION REPORT'}"
        )
    else:
        lines.append(
            f"{ASA_SINGLESPACE}{'BY BRANCHES ':<39}"
            f"{'MONTHLY MUDHARABAH GENERAL INVESTMENT POSITION REPORT'}"
        )
    lines.append(
        f"{ASA_SINGLESPACE}"
        f"{'                <<<<<     B U M I P U T R A     >>>>>':<55}"
        f"{'<<<<< N O N   B U M I P U T R A >>>>>':<39}"
        f"{'<<<<<         T O T A L         >>>>>'}"
    )
    lines.append(
        f"{ASA_SINGLESPACE}"
        f"{'BRANCH  BRANCH  NO. OF     NO. OF     CLOSING':<55}"
        f"{'NO. OF     NO. OF     CLOSING':<39}"
        f"{'NO. OF     NO. OF     CLOSING'}"
    )
    lines.append(
        f"{ASA_SINGLESPACE}"
        f"{'NUMBER  CODE    ACCOUNTS   RECEIPTS   BALANCE':<55}"
        f"{'ACCOUNTS   RECEIPTS   BALANCE':<39}"
        f"{'ACCOUNTS   RECEIPTS   BALANCE'}"
    )
    lines.append(
        f"{ASA_SINGLESPACE}"
        f"{'------  ------  ---------  ---------  ---------------':<55}"
        f"{'---------  ---------  ---------------':<39}"
        f"{'---------  ---------  ---------------'}"
    )
    return lines


def detail_row(branch: int, brhcode: str,
               bacctot: int, brcptot: int, bbrhamt: float,
               nacctot: int, nrcptot: int, nbrhamt: float,
               tacctot: int, trcptot: int, tbrhamt: float) -> str:
    return (
        f"{ASA_SINGLESPACE} {fmt_z3(branch)}"
        f"        {str(brhcode):<3}"
        f"  {fmt_c9(bacctot)}"
        f"  {fmt_c9(brcptot)}"
        f"  {fmt_c15(bbrhamt)}"
        f"  {fmt_c9(nacctot)}"
        f"  {fmt_c9(nrcptot)}"
        f"  {fmt_c15(nbrhamt)}"
        f"  {fmt_c9(tacctot)}"
        f"  {fmt_c9(trcptot)}"
        f"  {fmt_c15(tbrhamt)}"
    )


def total_row(label: str,
              ba: int, br: int, bb: float,
              na: int, nr: int, nb: float,
              ta: int, tr: int, tb: float) -> list:
    sep = (f"{ASA_SINGLESPACE}"
           f"{'':16}{'=========  =========  ==============='}"
           f"  {'=========  =========  ==============='}"
           f"  {'=========  =========  ==============='}")
    tot = (f"{ASA_SINGLESPACE}"
           f"    {label:<13}"
           f"{fmt_c9(ba)}  {fmt_c9(br)}  {fmt_c15(bb)}"
           f"  {fmt_c9(na)}  {fmt_c9(nr)}  {fmt_c15(nb)}"
           f"  {fmt_c9(ta)}  {fmt_c9(tr)}  {fmt_c15(tb)}")
    return [sep, tot, sep]


# ============================================================================
# REPORTS 1 & 2: Full product set (TBMERG1 sorted BY MTH BRANCH ACCTNO)
# ============================================================================

# Sort for Report 1: BY MTH BRANCH ACCTNO
tbmerg1.sort(key=lambda r: (r["MTH"], r["BRANCH"], r["ACCTNO"]))

report_lines: list = []
rpt_rows: list     = []   # accumulates branch-level summary for RPT (Report 2)

# ---- Report 1: EIBMFDIS-1 by tenure ----
page_cnt_r1 = 0

for mth, mth_grp in _groupby(tbmerg1, key=lambda r: r["MTH"]):
    mth_rows = list(mth_grp)
    page_cnt_r1 += 1
    tenure_line = f"FD TENURE: {fmt_z2(mth)} MONTHS"
    report_lines.extend(
        report_page_header("EIBMFDIS - 1",
                           "AL-MUDHARABAH GENERAL INVESTMENT (MGIA)",
                           tenure_line, page_cnt_r1, mth)
    )

    lt = {"a": 0, "r": 0, "b": 0.0,
          "na": 0, "nr": 0, "nb": 0.0,
          "ta": 0, "tr": 0, "tb": 0.0}

    for branch, br_grp in _groupby(mth_rows, key=lambda r: r["BRANCH"]):
        br_rows = list(br_grp)
        brhcode = br_rows[0].get("BRHCODE", "")
        ba = br = 0; bb = 0.0
        na = nr = 0; nb = 0.0
        seen_br: set = set()

        for r in br_rows:
            is_first = r["ACCTNO"] not in seen_br
            seen_br.add(r["ACCTNO"])
            if r["CUSTCD"] == 77:
                if is_first: ba += 1
                br += 1; bb += r["CURBAL"]
            else:
                if is_first: na += 1
                nr += 1; nb += r["CURBAL"]

        ta = ba + na; tr = br + nr; tb = bb + nb
        report_lines.append(detail_row(branch, brhcode, ba, br, bb, na, nr, nb, ta, tr, tb))

        lt["a"] += ba; lt["r"] += br; lt["b"] += bb
        lt["na"] += na; lt["nr"] += nr; lt["nb"] += nb
        lt["ta"] += ta; lt["tr"] += tr; lt["tb"] += tb

        # Accumulate into RPT for Report 2
        rpt_rows.append({
            "BRANCH": branch, "BRHCODE": brhcode,
            "BACCTOT": ba, "BRCPTOT": br, "BBRHAMT": bb,
            "NACCTOT": na, "NRCPTOT": nr, "NBRHAMT": nb,
            "TACCTOT": ta, "TRCPTOT": tr, "TBRHAMT": tb,
        })

    report_lines.extend(
        total_row("T O T A L",
                  lt["a"], lt["r"], lt["b"],
                  lt["na"], lt["nr"], lt["nb"],
                  lt["ta"], lt["tr"], lt["tb"])
    )

# ---- Report 2: EIBMFDIS-2 by branches ----
# Consolidate RPT rows by BRANCH
rpt_rows.sort(key=lambda r: r["BRANCH"])
page_cnt_r2 = 0
grand = {k: 0 for k in ["ba","br","bb","na","nr","nb","ta","tr","tb"]}

page_cnt_r2 += 1
report_lines.extend(
    report_page_header("EIBMFDIS - 2",
                       "AL-MUDHARABAH GENERAL INVESTMENT (MGIA)",
                       "", page_cnt_r2)
)

for branch, br_grp in _groupby(rpt_rows, key=lambda r: r["BRANCH"]):
    br_list  = list(br_grp)
    brhcode  = br_list[0]["BRHCODE"]
    fba = fbr = 0; fbb = 0.0
    fna = fnr = 0; fnb = 0.0
    fta = ftr = 0; ftb = 0.0
    for x in br_list:
        fba += x["BACCTOT"]; fbr += x["BRCPTOT"]; fbb += x["BBRHAMT"]
        fna += x["NACCTOT"]; fnr += x["NRCPTOT"]; fnb += x["NBRHAMT"]
        fta += x["TACCTOT"]; ftr += x["TRCPTOT"]; ftb += x["TBRHAMT"]

    report_lines.append(detail_row(branch, brhcode, fba, fbr, fbb, fna, fnr, fnb, fta, ftr, ftb))
    grand["ba"] += fba; grand["br"] += fbr; grand["bb"] += fbb
    grand["na"] += fna; grand["nr"] += fnr; grand["nb"] += fnb
    grand["ta"] += fta; grand["tr"] += ftr; grand["tb"] += ftb

report_lines.extend(
    total_row("GRAND TOTAL",
              grand["ba"], grand["br"], grand["bb"],
              grand["na"], grand["nr"], grand["nb"],
              grand["ta"], grand["tr"], grand["tb"])
)
report_lines.append(
    f"{ASA_DOUBLESPACE}     T O T A L  BUMIPUTRA ACCOUNTS     = {BACCTNO_ALL}"
)
report_lines.append(
    f"{ASA_SINGLESPACE}               NON-BUMIPUTRA ACCOUNTS = {NBACCTNO_ALL}"
)

# ============================================================================
# REPORTS 3 & 4: Staff products only (INTPLAN 448-459, 461-469)
# ============================================================================

# DATA STAFFIFD: filter from TBMERG1 (already sorted by ACCTNO)
tbmerg1.sort(key=lambda r: r["ACCTNO"])

bacctno_s  = 0
nbacctno_s = 0
seen_staff: set = set()

staffifd = []
for r in tbmerg1:
    ip = r["INTPLAN"]
    if not ((448 <= ip <= 459) or (461 <= ip <= 469)):
        continue
    r2 = {**r, "MTH": INTPLAN_TO_MTH_STAFF.get(ip, 0)}
    if r2["ACCTNO"] not in seen_staff:
        seen_staff.add(r2["ACCTNO"])
        if r2["CUSTCD"] == 77:
            bacctno_s += 1
        else:
            nbacctno_s += 1
    staffifd.append(r2)

BACCTNO_STAFF  = bacctno_s
NBACCTNO_STAFF = nbacctno_s

# Sort BY MTH BRANCH ACCTNO
staffifd.sort(key=lambda r: (r["MTH"], r["BRANCH"], r["ACCTNO"]))

rpt1_rows: list  = []
page_cnt_r3 = 0

# ---- Report 3: EIBMFDIS-3 Staff by tenure ----
for mth, mth_grp in _groupby(staffifd, key=lambda r: r["MTH"]):
    mth_rows = list(mth_grp)
    page_cnt_r3 += 1
    tenure_line = f"FD TENURE: {fmt_z2(mth)} MONTHS"
    report_lines.extend(
        report_page_header("EIBMFDIS - 3",
                           "STAFF AL-MUDHARABAH GENERAL INVESTMENT (MGIA)",
                           tenure_line, page_cnt_r3, mth)
    )

    lt = {"a": 0, "r": 0, "b": 0.0,
          "na": 0, "nr": 0, "nb": 0.0,
          "ta": 0, "tr": 0, "tb": 0.0}

    for branch, br_grp in _groupby(mth_rows, key=lambda r: r["BRANCH"]):
        br_rows = list(br_grp)
        brhcode = br_rows[0].get("BRHCODE", "")
        ba = br = 0; bb = 0.0
        na = nr = 0; nb = 0.0
        seen_br: set = set()

        for r in br_rows:
            is_first = r["ACCTNO"] not in seen_br
            seen_br.add(r["ACCTNO"])
            if r["CUSTCD"] == 77:
                if is_first: ba += 1
                br += 1; bb += r["CURBAL"]
            else:
                if is_first: na += 1
                nr += 1; nb += r["CURBAL"]

        ta = ba + na; tr = br + nr; tb = bb + nb
        report_lines.append(detail_row(branch, brhcode, ba, br, bb, na, nr, nb, ta, tr, tb))

        lt["a"] += ba; lt["r"] += br; lt["b"] += bb
        lt["na"] += na; lt["nr"] += nr; lt["nb"] += nb
        lt["ta"] += ta; lt["tr"] += tr; lt["tb"] += tb

        rpt1_rows.append({
            "BRANCH": branch, "BRHCODE": brhcode,
            "BACCTOT": ba, "BRCPTOT": br, "BBRHAMT": bb,
            "NACCTOT": na, "NRCPTOT": nr, "NBRHAMT": nb,
            "TACCTOT": ta, "TRCPTOT": tr, "TBRHAMT": tb,
        })

    report_lines.extend(
        total_row("T O T A L",
                  lt["a"], lt["r"], lt["b"],
                  lt["na"], lt["nr"], lt["nb"],
                  lt["ta"], lt["tr"], lt["tb"])
    )

# ---- Report 4: EIBMFDIS-4 Staff by branches ----
rpt1_rows.sort(key=lambda r: r["BRANCH"])
page_cnt_r4 = 0
grand4 = {k: 0 for k in ["ba","br","bb","na","nr","nb","ta","tr","tb"]}

page_cnt_r4 += 1
report_lines.extend(
    report_page_header("EIBMFDIS - 4",
                       "STAFF AL-MUDHARABAH GENERAL INVESTMENT (MGIA)",
                       "", page_cnt_r4)
)

for branch, br_grp in _groupby(rpt1_rows, key=lambda r: r["BRANCH"]):
    br_list  = list(br_grp)
    brhcode  = br_list[0]["BRHCODE"]
    fba = fbr = 0; fbb = 0.0
    fna = fnr = 0; fnb = 0.0
    fta = ftr = 0; ftb = 0.0
    for x in br_list:
        fba += x["BACCTOT"]; fbr += x["BRCPTOT"]; fbb += x["BBRHAMT"]
        fna += x["NACCTOT"]; fnr += x["NRCPTOT"]; fnb += x["NBRHAMT"]
        fta += x["TACCTOT"]; ftr += x["TRCPTOT"]; ftb += x["TBRHAMT"]

    report_lines.append(detail_row(branch, brhcode, fba, fbr, fbb, fna, fnr, fnb, fta, ftr, ftb))
    grand4["ba"] += fba; grand4["br"] += fbr; grand4["bb"] += fbb
    grand4["na"] += fna; grand4["nr"] += fnr; grand4["nb"] += fnb
    grand4["ta"] += fta; grand4["tr"] += ftr; grand4["tb"] += ftb

report_lines.extend(
    total_row("GRAND TOTAL",
              grand4["ba"], grand4["br"], grand4["bb"],
              grand4["na"], grand4["nr"], grand4["nb"],
              grand4["ta"], grand4["tr"], grand4["tb"])
)
report_lines.append(
    f"{ASA_DOUBLESPACE}     T O T A L  BUMIPUTRA ACCOUNTS     = {BACCTNO_STAFF}"
)
report_lines.append(
    f"{ASA_SINGLESPACE}               NON-BUMIPUTRA ACCOUNTS = {NBACCTNO_STAFF}"
)

# ============================================================================
# WRITE REPORT OUTPUT
# ============================================================================

with open(OUTPUT_REPORT, "w", encoding="utf-8") as fh:
    fh.write("\n".join(report_lines) + "\n")

print(f"Report written to: {OUTPUT_REPORT}")
