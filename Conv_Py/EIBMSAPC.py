#!/usr/bin/env python3
"""
Program : EIBMSAPC.py
Purpose : BNM treasury purchase/sale deal BNM-code derivation from the
          BNMTBLX flat-file feed, rolled up at four granularities
          (per-code, per-7-char-prefix, per-2-char-category, and per
          category+maturity), producing KAPX.

          Originally %INC PGM(EIBMSAPC) inside EIBPTH1A -- a SAS open-code
          fragment (no own JCL). Unlike KALMLIFE, this program owns a
          complete DATA REPTDATE step of its own, so it is fully
          self-contained for report-date purposes. KAPX is later appended
          by EIBPTH1A into its SP dataset ("DATA SP; SET SP K3FEI KAPX;").

Dependency:
    REPTDATE.py -> get_monthly_reptdate_values() (REPTDATE = last day of the
        previous month, matching the SAS REPTDATE formula).

Physical input:
    BNMTBLX  (JCL //BNMTBLX DD DSN=SAP.PBB.KAPITIX.TXT(0))
        This is NOT a SAS dataset -- the ".TXT" DSN and GDG(0) relative
        generation confirm a plain fixed-width mainframe flat file, read
        here via byte-offset slicing (never parquet/read_csv), and resolved
        via input_date.get_latest_file() since the physical filename is
        non-deterministic (GDG "latest generation").
"""

from pathlib import Path
from datetime import date

import polars as pl

from REPTDATE import get_monthly_reptdate_values
from input_date import get_latest_file

# ============================================================================
# STEP 0: REPORT-DATE / MACRO-VARIABLE CONTEXT
# ============================================================================


def _derive_context() -> dict:
    monthly = get_monthly_reptdate_values(year_format="%Y")
    reptdate = monthly.reptdate  # last day of previous month

    day_of_month = reptdate.day
    # SELECT(DAY(REPTDATE)): REPTDATE is always a month-end date (28-31), so
    # WHEN(8)/WHEN(15)/WHEN(22) never fire in practice -- OTHERWISE always
    # applies. Preserved verbatim (dead branches kept, see below).
    if day_of_month == 8:
        sdd, wk, wk1, wk2, wk3 = 1, "1", "4", None, None
    elif day_of_month == 15:
        sdd, wk, wk1, wk2, wk3 = 9, "2", "1", None, None
    elif day_of_month == 22:
        sdd, wk, wk1, wk2, wk3 = 16, "3", "2", None, None
    else:
        sdd, wk, wk1, wk2, wk3 = 23, "4", "3", "2", "1"

    mm = reptdate.month
    if wk == "1":
        mm1 = mm - 1 if mm - 1 != 0 else 12
    else:
        mm1 = mm
    mm2 = mm - 1 if mm - 1 != 0 else 12

    return {
        "reptdate": reptdate,
        "reptyear": reptdate.strftime("%Y"),
        "reptmon": reptdate.strftime("%m"),
        "reptmon1": f"{mm1:02d}",
        "reptmon2": f"{mm2:02d}",
        "reptday": f"{day_of_month:02d}",
        "rdate": reptdate.strftime("%d/%m/%y"),
        "sdate": date(reptdate.year, mm, sdd),
        "sdesc": "PUBLIC BANK BERHAD",
        # NOWK1/2/3 computed properly here (unlike EIBPTH1A's own copy of
        # this step, which hardcodes them to '1'/'2'/'3' -- a SAS quirk).
        # Neither this program nor EIBPTH1A references NOWK1/2/3 downstream,
        # so this only affects documentation/macro-var parity, not output.
        "nowk": wk,
        "nowk1": wk1,
        "nowk2": wk2,
        "nowk3": wk3,
    }


_CTX = _derive_context()
REPTDATE = _CTX["reptdate"]
REPTYEAR = _CTX["reptyear"]
REPTMON = _CTX["reptmon"]

RPYR, RPMTH, RPDAY = REPTDATE.year, REPTDATE.month, REPTDATE.day

print("EIBMSAPC: Deriving report-date context...")
print(f"  REPTDATE : {REPTDATE.isoformat()}   RDATE : {_CTX['rdate']}")

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
STG_DIR = Path("/stgsrcsys/host/uat/AII")

INPUT_BNMTBLX_DIR = STG_DIR / "flatfile"
INPUT_BNMTBLX_FILE = get_latest_file(INPUT_BNMTBLX_DIR, prefix="bnmtblx")

# ============================================================================
# PROC FORMAT VALUE ORGMT.  LOW-12='50'; 12-HIGH='60';
# (Overlap at 12 resolves to the FIRST-listed range per SAS format rules.)
# ============================================================================


def format_orgmt(remmth: float) -> str:
    return "50" if remmth <= 12 else "60"


# ============================================================================
# %DCLVAR / %REMMTH macros
# RPDAYS (RD1-RD12) is fixed per DCLVAR's RETAIN statement -- Feb is fixed
# at 28 and is NEVER adjusted for the report year's leap-year status here
# (unlike EIIMRM01.py's local RD_DAYS, which does adjust). D1-D12 (LDAY) and
# MD1-MD12 (MDDAYS, only MD2 ever assigned) are declared in DCLVAR but never
# referenced elsewhere in the program body -- dead declarations, omitted.
# ============================================================================
RPDAYS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


def _remmth(matdt: date) -> float:
    mdyr, mdmth, mdday = matdt.year, matdt.month, matdt.day
    days_in_rpmth = RPDAYS[RPMTH - 1]
    if mdday > days_in_rpmth:
        mdday = days_in_rpmth
    remy = mdyr - RPYR
    remm = mdmth - RPMTH
    remd = mdday - RPDAY
    return remy * 12 + remm + remd / days_in_rpmth


def _sas_round(x: float) -> float:
    if x >= 0:
        return float(int(x + 0.5))
    return float(-int(-x + 0.5))


# ============================================================================
# STEP 1: DATA RAW; INFILE BNMTBLX FIRSTOBS=2; INPUT @col ... ;
# Fixed-width byte-offset parsing (1-indexed SAS column -> 0-indexed slice).
# ============================================================================
print("\nEIBMSAPC Step 1: Parsing BNMTBLX fixed-width flat file...")


def _num_informat(raw: str, decimals: int) -> float:
    """Numeric informat w.d (e.g. 16.2): implied decimal point 'decimals'
    places from the right UNLESS the text already contains a literal '.'."""
    raw = raw.strip()
    if raw == "":
        return 0.0
    if "." in raw:
        return float(raw)
    sign = -1 if raw.startswith("-") else 1
    raw = raw.lstrip("+-")
    if not raw.isdigit():
        return 0.0
    return sign * int(raw) / (10 ** decimals)


_MONTHS_RANGE = range(1, 13)

raw_rows = []
with open(INPUT_BNMTBLX_FILE, "r", encoding="latin1") as fh:
    lines = fh.readlines()

for line in lines[1:]:  # FIRSTOBS=2 -> skip header line
    line = line.rstrip("\n").ljust(120)

    utdlp = line[0:3]
    utdlr = line[3:16]        # dead beyond this point, parsed for parity
    utsty = line[16:19]
    utcus = line[19:25]       # dead
    utclc = line[25:28]       # dead
    gfctp = line[28:30]       # dead
    gfcnal = line[30:32]      # dead
    svtlx = line[32:52]       # dead
    utosd = line[52:62]       # dead
    uttrd = line[62:72]       # dead
    utmdd = line[72:74]
    utmmm = line[75:77]
    utmyy = line[78:82]
    utfcv_raw = line[82:98]
    utbfcy = line[100:103]    # dead

    if utdlp[0:2] == "FT":  # IF SUBSTR(UTDLP,1,2)='FT' THEN DELETE;
        continue

    try:
        utmdt = date(int(utmyy), int(utmmm), int(utmdd))
    except ValueError:
        continue

    utfcv = _num_informat(utfcv_raw, 2)

    raw_rows.append({
        "UTDLP": utdlp, "UTSTY": utsty, "UTMDT": utmdt, "UTFCV": utfcv,
    })

print(f"  RAW rows after 'FT' delete filter: {len(raw_rows):,}")

# ============================================================================
# STEP 2: DATA DUMMY (KEEP=BNMCODE); nested-loop placeholder BNMCODEs
# ============================================================================
print("\nEIBMSAPC Step 2: Building DUMMY BNMCODE placeholders...")

dummy_codes = []
for h in (6, 7):
    for i in (0, 3, 4, 10, 21, 22, 23, 50, 70, 90):
        for j in (0, 50, 60):
            dummy_codes.append(f"{h}87{i:02d}80{j:02d}0000Y")
dummy_codes.sort()

# ============================================================================
# STEP 3: DATA PURCHASE SALE; SET RAW; ... %REMMTH;
# ============================================================================
print("\nEIBMSAPC Step 3: Splitting into PURCHASE / SALE with REMMTH...")

purchase_rows, sale_rows = [], []
for r in raw_rows:
    typeprsl = r["UTDLP"][2:3]
    remmth = _remmth(r["UTMDT"])
    rec = {"UTSTY": r["UTSTY"], "UTFCV": r["UTFCV"], "REMMTH": remmth}
    if typeprsl == "P":
        purchase_rows.append(rec)
    if typeprsl == "S":
        sale_rows.append(rec)

print(f"  PURCHASE rows: {len(purchase_rows):,}   SALE rows: {len(sale_rows):,}")

# ============================================================================
# STEP 4: DATA PURCODE / SALCODE (KEEP=BNMCODE AMOUNT)
# ============================================================================
print("\nEIBMSAPC Step 4: Building PURCODE / SALCODE BNMCODEs...")


def _purcode_bnmcode(utsty: str, origdate: str):
    if utsty in ("SSD", "SLD", "SDC", "LDC", "SZD", "SFD"):
        return f"6870380{origdate}0000Y"
    elif utsty in ("PBA", "SBA"):
        return f"6870480{origdate}0000Y"
    elif utsty in ("DBD", "DBZ", "MTN", "PNB"):
        return f"6871080{origdate}0000Y"
    elif utsty in ("MGS",):
        return f"6872180{origdate}0000Y"
    elif utsty in ("MTB",):
        return f"6872280{origdate}0000Y"
    elif utsty in ("MGI",):
        return f"6872380{origdate}0000Y"
    elif utsty in ("CB1", "CNT"):
        return f"6875080{origdate}0000Y"
    elif utsty in ("ISB", "IDS", "IBZ", "KHA", "SAC", "SCM", "SCD", "SMC", "ITB", "BMC"):
        return f"6877080{origdate}0000Y"
    elif utsty in ("BMN", "BMF"):
        return f"6879080{origdate}0000Y"
    return None  # no branch matched -> BNMCODE stays blank (preserved as-is)


def _salcode_bnmcode(utsty: str, origdate: str):
    if utsty in ("SSD", "SLD", "SDC", "LDC", "SZD", "SFD"):
        return f"7870380{origdate}0000Y"
    elif utsty in ("PBA", "SBA"):
        return f"7870480{origdate}0000Y"
    elif utsty in ("DBD", "DBZ", "MTN", "PNB"):
        return f"7871080{origdate}0000Y"
    elif utsty in ("MGS",):
        return f"7872180{origdate}0000Y"
    elif utsty in ("MTB",):
        return f"7872280{origdate}0000Y"
    elif utsty in ("MGI",):
        return f"7872380{origdate}0000Y"
    elif utsty in ("CB1", "CNT"):
        return f"7875080{origdate}0000Y"
    elif utsty in ("ISB", "IDS", "IBZ", "KHA", "SAC", "SCM", "SCD", "SMC", "ITB", "BMC"):
        return f"7877080{origdate}0000Y"
    elif utsty in ("BMN", "BMF"):
        # Source re-invokes PUT(REMMTH,ORGMT.) explicitly on this final
        # branch rather than reusing the already-computed ORIGDATE; the
        # value is identical either way, kept as a plain reuse here.
        return f"7879080{origdate}0000Y"
    return None


purcode_rows = []
for r in purchase_rows:
    origdate = format_orgmt(r["REMMTH"])
    bnmcode = _purcode_bnmcode(r["UTSTY"], origdate)
    purcode_rows.append({"BNMCODE": bnmcode or "", "AMOUNT": r["UTFCV"]})

salcode_rows = []
for r in sale_rows:
    origdate = format_orgmt(r["REMMTH"])
    bnmcode = _salcode_bnmcode(r["UTSTY"], origdate)
    salcode_rows.append({"BNMCODE": bnmcode or "", "AMOUNT": r["UTFCV"]})

purcode_rows.sort(key=lambda r: r["BNMCODE"])
salcode_rows.sort(key=lambda r: r["BNMCODE"])

# ============================================================================
# STEP 5: DATA SALPUR; MERGE PURCODE SALCODE; BY BNMCODE;
# Real purchase/sale codes never collide (prefix '6' vs '7'), so this is a
# key union. FLAG-02: rows with an unmatched UTSTY keep a blank BNMCODE and
# COULD collide across PURCODE/SALCODE on that blank key; SAS MERGE would
# apply "last-dataset-wins" (SALCODE overwrites PURCODE) for such a
# collision. Reproduced here via ordered dict overlay (last-write-wins),
# which matches SAS MERGE semantics for the common (non-duplicate) case.
# ============================================================================
print("\nEIBMSAPC Step 5: Merging PURCODE + SALCODE by BNMCODE...")

salpur: dict = {}
for r in purcode_rows:
    salpur[r["BNMCODE"]] = r["AMOUNT"]
for r in salcode_rows:
    salpur[r["BNMCODE"]] = r["AMOUNT"]

# ============================================================================
# STEP 6: DATA MERG; MERGE DUMMY SALPUR; BY BNMCODE; IF AMOUNT=. THEN 0.00;
# ============================================================================
print("\nEIBMSAPC Step 6: Merging DUMMY + SALPUR...")

merg: dict = {code: None for code in dummy_codes}
for code, amt in salpur.items():
    merg[code] = amt
merg = {code: (amt if amt is not None else 0.00) for code, amt in merg.items()}

merg_rows = [{"BNMCODE": code, "AMOUNT": amt} for code, amt in merg.items()]

# ============================================================================
# STEP 7: DATA MESS; SET MERG; PREX/CATX/MATX derived substrings.
# ============================================================================
mess_rows = [
    {
        "BNMCODE": r["BNMCODE"],
        "AMOUNT": r["AMOUNT"],
        "PREX": r["BNMCODE"][0:7],
        "CATX": r["BNMCODE"][0:2],
        "MATX": r["BNMCODE"][7:9],
    }
    for r in merg_rows
]

# ============================================================================
# STEP 8: PROC SUMMARY roll-ups -- MERGX (by BNMCODE), PREX, CATX, MATX
# ============================================================================
print("\nEIBMSAPC Step 8: Building roll-up summaries...")


def _group_sum(rows, key_fields, val_field="AMOUNT"):
    groups: dict = {}
    for r in rows:
        key = tuple(r[f] for f in key_fields)
        groups[key] = (groups.get(key, 0.0) or 0.0) + (r[val_field] or 0.0)
    return groups


mergx = _group_sum(merg_rows, ["BNMCODE"])
prex_sum = _group_sum(mess_rows, ["PREX"])
catx_sum = _group_sum(mess_rows, ["CATX"])
matx_sum = _group_sum(mess_rows, ["CATX", "MATX"])

# DATA PREX(KEEP=BNMCODE AMOUNT): BNMCODE=COMPRESS(PREX||'000000Y');
prex_rows = [
    {"BNMCODE": f"{prex}000000Y", "AMOUNT": amt}
    for (prex,), amt in prex_sum.items()
]

# DATA CATX(KEEP=BNMCODE AMOUNT): BNMCODE=COMPRESS(CATX||'70080000000Y');
catx_rows = [
    {"BNMCODE": f"{catx}70080000000Y", "AMOUNT": amt}
    for (catx,), amt in catx_sum.items()
]

# DATA MATX(KEEP=BNMCODE AMOUNT): IF MATX='00' THEN DELETE;
#                                  BNMCODE=COMPRESS(CATX||'70080'||MATX||'0000Y');
matx_rows = [
    {"BNMCODE": f"{catx}70080{matx}0000Y", "AMOUNT": amt}
    for (catx, matx), amt in matx_sum.items()
    if matx != "00"
]

mergx_rows = [{"BNMCODE": k[0], "AMOUNT": v} for k, v in mergx.items()]
mergx_rows.sort(key=lambda r: r["BNMCODE"])
prex_rows.sort(key=lambda r: r["BNMCODE"])
catx_rows.sort(key=lambda r: r["BNMCODE"])
matx_rows.sort(key=lambda r: r["BNMCODE"])

# ============================================================================
# STEP 9: DATA KAPX; MERGE MERGX PREX CATX MATX; BY BNMCODE;
#         AMTIND='D'; ITCODE=BNMCODE;
# Last-dataset-wins overlay in MERGE statement order (MERGX, PREX, CATX,
# MATX); the four code-construction patterns are disjoint in practice, so
# this behaves as a straightforward union of roll-up rows.
# ============================================================================
print("\nEIBMSAPC Step 9: Building KAPX...")

kapx_map: dict = {}
for src in (mergx_rows, prex_rows, catx_rows, matx_rows):
    for r in src:
        kapx_map[r["BNMCODE"]] = r["AMOUNT"]

kapx_pre = [{"ITCODE": code, "AMTIND": "D", "AMOUNT": amt} for code, amt in kapx_map.items()]

# PROC SUMMARY DATA=KAPX NWAY; CLASS ITCODE AMTIND; VAR AMOUNT; SUM=;
_kapx_groups = _group_sum(kapx_pre, ["ITCODE", "AMTIND"])
KAPX = pl.DataFrame(
    [{"ITCODE": k[0], "AMTIND": k[1], "AMOUNT": v} for k, v in _kapx_groups.items()]
)

print(f"  KAPX rows (module-level output): {KAPX.height:,}")
print("EIBMSAPC complete.")
