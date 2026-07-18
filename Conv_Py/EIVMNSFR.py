#!/usr/bin/env python3
"""
Program : EIVMNSFR.py
Purpose : Automate the Net Stable Funding Ratio (NSFR) report for entity
          and consolidated level (PIVB).

          Combines three maturity-bucket data feeds - the mainframe GL
          feed (GLPIVB), the equation-derived feed (EQUA) and the manually
          maintained feed (MNL1) - summarises them by BNM item code, and
          merges the totals against a fixed positional item-code template
          (TEMPL) to produce the delimited NSFR report used downstream by
          the FTP job step.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import polars as pl

from REPTDATE import get_monthly_reptdate_values
# from input_date import get_latest_file
# output_date.build_output_file() is NOT used here: SAS derives the output
# filename component RPTDT via PUT(REPTDATE,YYMMDDN8.) (8-digit YYYYMMDD),
# which does not match either pattern in output_date.DATE_FORMATS
# ("ddmmyy" / "ddmmYYYY"). Per project convention this is a non-standard
# output naming pattern, so the filename is built directly from REPTDATE
# values in main() instead.

# ----------------------------------------------------------------------------
# %INC PGM(PBBELF,PBLCRFMT);  -- session-level include in the original SAS.
# Neither PBBELF nor PBLCRFMT format functions are invoked via PUT(var,fmt.)
# anywhere in this program body. The only PUT(...,fmt.) call in the whole
# program uses the *local* PROC FORMAT $NSFRCD defined further below. Per
# project convention (session-level %INC with no direct format call = a
# comment-only reference), neither module is imported here.
# from PBBELF import ...      # not used - no direct PUT(var, fmt.) call
# from PBLCRFMT import ...    # not used - no direct PUT(var, fmt.) call
# ----------------------------------------------------------------------------

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
# INPUT_DIR = BASE_DIR / "input"
# OUTPUT_DIR = BASE_DIR / "output"

INPUT_DIR  = Path("/stgsrcsys/host/uat")
OUTPUT_DIR = BASE_DIR / "output" / "EIVMNSFR"

# SAP.PIVB.NSFR.TEMPLATE -- static fixed-position item-code list, not a dated
# input (analogous to BRHFILE/LKP_BRANCH convention).
TEMPLATE_FILE = INPUT_DIR / "NSFR_TEMPLATE.txt"

# Dated monthly feeds - resolved via input_date.get_latest_file() below.
# EQUA_INPUT = "eqnsf"      # SAP.PIVB.EQNSF.MTHEND.TXT(0)
# GLPIVB_PREFIX = "glpivb"   # SAP.APPL.PIVB.MTHEND.LCR(0)
# MNL1_PREFIX = "mnlnsf"     # SAP.PIVB.MNL.NSFR.LCR(0)

EQUA_DIR   = INPUT_DIR / "EQNSF.txt"
GLPIVB_DIR = INPUT_DIR / "MTHEND_LCR.txt"
MNL1_DIR   = INPUT_DIR / "MNL_NSFR.txt"

DLM = "\x05"  # SAS: DLM='05'X

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# LOCAL FORMAT: $NSFRCD (PROC FORMAT defined in this program - NOT PBBELF/
# PBLCRFMT). Maps a GL SET_ID description string to a "<item><'_'><bucket>"
# tag; item = 4-digit BNM item code, bucket = 1/2/3 maturity bucket.
# ============================================================================
# NSFRCD_FMT: dict[str, str] = {
#     "S-SHARE": "0006_3",
#     "S-OS SALES": "0074_1",
#     "S-SSTPAY": "0076_1",
#     "S-PBS DLRS": "0076_1",
#     "S-REMI CA": "0076_1",
#     "S-LEASE ROUA": "0076_3",
#     "S-PROVTAX(C)": "0076_3",
#     "S-PROVOTH": "0076_3",
#     "S-ALLW COMM": "0076_3",
#     "S-PROVCLGFEE": "0076_3",
#     "S-AFRECADV": "0076_3",
#     "S-ACCEXP": "0076_3",
#     "S-SUNCRE KAP": "0076_3",
#     "S-SUNCRE": "0076_3",
#     "S-LOANCONTRO": "0076_3",
#     "S-PBS PAYB": "0076_3",
#     "S-PETTY CASH": "0084_1",
#     "S-STADEPBNM": "0085_3",
#     "S-BNMFL 1MTH": "0116_1",
#     "S-PBB CUR": "0116_1",
#     "S-PIBB CUR": "0116_1",
#     "S-PB CA OTH": "0116_1",
#     "S-CB": "0116_1",
#     "S-REMI FD": "0116_1",
#     "S-LBFD": "0116_1",
#     "S-MBFL 1MTH": "0116_1",
#     "S-LBFL 1MTH": "0116_1",
#     "S-DNBFI 1MTH": "0116_1",
#     "S-LBFL": "0116_1",
#     "LCR-FIOPSDEP": "0140_1",
#     "S-BNM FIX": "0152_1",
#     "S-BNM": "0152_1",
#     "S-MARGIN COL": "0245_3",
#     "S-DTAX": "0247_3",
#     "S-IA": "0247_3",
#     "S-O/S PUR C": "0248_1",
#     "S-CLIENT CTL": "0248_1",
#     "S-RCF": "0256_1",
#     "S-SM F": "0256_1",
#     "S-TLF": "0256_1",
#     "OBS00100100": "0260_1",
# }

NSFRCD_FMT: dict[str, str] = {
    # === ITEM 6: Tier 1 and Tier 2 capital ===
    "S-SHARE": "0006_3",
    
    # === ITEM 11: Less stable deposits ===
    # (Will be populated from MNL or EQ)
    
    # === ITEM 13: Unsecured funding from non-financial corporates ===
    "S-OS SALES": "0076_1",        # Already mapped
    "S-STD R/NR": "0076_1",        # 
    "S-SUNDEBT": "0076_1",         # 
    "S-SUNDEBTREC": "0076_1",      # 
    "S-REMI CA": "0076_1",         # Already mapped
    "S-SSTPAY": "0076_1",          # Already mapped
    "S-PBS DLRS": "0076_1",        # Already mapped
    "S-ACCEXP": "0076_3",          # Already mapped - BUCKET 3
    "S-AFRECADV": "0076_3",        # Already mapped - BUCKET 3
    "S-ALLW COMM": "0076_3",       # Already mapped - BUCKET 3
    "S-LEASE ROUA": "0076_3",      # Already mapped - BUCKET 3
    "S-LOANCONTRO": "0076_3",      # Already mapped - BUCKET 3
    "S-PBS PAYB": "0076_3",        # Already mapped - BUCKET 3
    "S-PROVCLGFEE": "0076_3",      # Already mapped - BUCKET 3
    "S-PROVOTH": "0076_3",         # Already mapped - BUCKET 3
    "S-PROVTAX(C)": "0076_3",      # Already mapped - BUCKET 3
    "S-SUNCRE": "0076_3",          # Already mapped - BUCKET 3
    "S-SUNCRE KAP": "0076_3",      # Already mapped - BUCKET 3
    
    # === ITEM 25: Unsecured funding from sovereigns/PSEs/MDBs/NDBs ===
    "S-REVCRE": "0025_1",          # 
    "S-REVREAFMGS": "0025_1",      # Check amount
    "S-REVREPBNM1": "0025_1",      # Check amount
    "S-REVREPBNM2": "0025_1",      # Check amount
    "S-REVREPOBNM": "0025_1",      # Check amount
    "S-REVREPOHFT": "0025_1",      # Check amount
    "S-REVREPOMGI": "0025_1",      # Check amount
    "S-REVREPOMGS": "0025_1",      # Check amount
    "S-REVRES": "0025_1",          # 
    
    # === ITEM 32: Unsecured funding from other legal entities ===
    "S-CURPNL": "0032_1",          # 
    "S-ACCDEPNO/E": "0032_1",      # 
    "S-ACCDEPRENO": "0032_1",      # 
    "S-DEPNCOPMSW": "0032_1",      # 
    "S-AFS UN-ISL": "0032_1",      # 
    "S-CL CTL CR": "0032_1",       # 
    "S-GUARANTEE": "0032_1",       # 
    "S-REMI FD 30": "0032_1",      # Check amount
    "S-REMI FD 32": "0032_1",      # Check amount
    "S-REMISIERS": "0032_1",       # Check amount
    "S-SUNDEP": "0032_1",          # 
    "S-ACCDEPNF&F": "0032_1",      # 
    "S-ACCDEPNMV": "0032_1",       # 
    "S-DEPNF&F": "0032_1",         # 
    "S-DEPNH/W": "0032_1",         # 
    "S-DEPNMV": "0032_1",          # Check amount
    "S-DEPNO/E": "0032_1",         # 
    "S-F&F": "0032_1",             # 
    "S-PDEPNSW": "0032_1",         # 
    "S-PDEPRENO": "0032_1",        # Check amount
    "S-REG RES": "0032_1",         # 
    "S-REMISIERS": "0032_1",       # Check amount
    "S-RENO": "0032_1",            # 
    "S-REPOMGS": "0032_1",         # Check amount
    "S-RETPROF": "0032_1",         # 
    # "S-REVREAFMGS": "0032_1",      # Check amount
    "S-SUNDEP": "0032_1",          # 
    "S-UNREAL UQS": "0032_1",      # 
    "S-UNREALMGS": "0032_1",       # 
    
    # === ITEM 74: Trade date payables ===
    # "S-OS SALES": "0074_1",        # Already mapped
    
    # === ITEM 76: Already mapped above ===
    
    # === ITEM 84: Coins and banknotes ===
    "S-PETTY CASH": "0084_1",      # Already mapped
    
    # === ITEM 85: Total central bank reserves ===
    "S-STADEPBNM": "0085_3",       # Already mapped
    
    # === ITEM 116: Deposits/UA Funds held at financial institutions ===
    "S-BNMFL 1MTH": "0116_1",      # Already mapped
    "S-PBB CUR": "0116_1",         # Already mapped
    "S-PIBB CUR": "0116_1",        # Already mapped
    "S-PB CA OTH": "0116_1",       # Already mapped
    "S-CB": "0116_1",              # Already mapped
    "S-REMI FD": "0116_1",         # Already mapped
    "S-LBFD": "0116_1",            # Already mapped
    "S-MBFL 1MTH": "0116_1",       # Already mapped
    "S-LBFL 1MTH": "0116_1",       # Already mapped
    "S-DNBFI 1MTH": "0116_1",      # Already mapped
    "S-LBFL": "0116_1",            # Already mapped
    "LCR-FIOPSDEP": "0140_1",      # Already mapped
    
    # === ITEM 140: Unsecured loans/financing to financial institutions ===
    "S-BNM": "0152_1",             # Already mapped
    "S-BNM FIX": "0152_1",         # Already mapped
    
    # === ITEM 152: Loans/Financing to central banks ===
    "S-BNM": "0152_1",             # Already mapped
    "S-BNM FIX": "0152_1",         # Already mapped
    "S-BNM O/N": "0152_1",         # Check amount
    "S-BNM(AFS)": "0152_1",        # Check amount
    "S-BNMFL": "0152_1",           # Check amount
    "S-BNM BILL M": "0152_1",      # Check amount
    "S-BNM BILL T": "0152_1",      # Check amount
    
    # === ITEM 206: Other short-term unsecured instruments ===
    "S-HFT": "0206_1",             # 
    "S-MGS": "0206_1",             # 
    "S-MTN LN": "0206_1",          # 
    "S-TERMLN": "0206_1",          # 
    "S-ISLAMIC(A)": "0206_1",      # 
    "S-ISLPDS (I)": "0206_1",      #
    
    # === ITEM 245: Cash or other assets to CCP default fund ===
    "S-MARGIN COL": "0245_3",      # Already mapped - 3,112.12
    
    # === ITEM 246: Required stable funding for IM and CCP ===
    "S-MARGIN COL": "0245_3",      # Already mapped
    
    # === ITEM 247: Items deducted from regulatory capital ===
    "S-DTAX": "0247_3",            # Already mapped 
    "S-IA": "0247_3",              # Already mapped 
    "S-IAISLDE(D)": "0247_3",      # 
    "S-D T ASSETS": "0247_3",      # 
    
    # === ITEM 248: Trade date receivables ===
    "S-O/S PUR C": "0248_1",       # Already mapped 
    "S-CLIENT CTL": "0248_1",      # Already mapped 
    
    # === ITEM 249: Interdependent assets ===
    # (Will be populated from EQ or MNL)
    
    # === ITEM 251: All other assets 100% treatment ===
    # (Will be populated from EQ or MNL)
    
    # === ITEM 256: Irrevocable/conditionally revocable credit facilities ===
    "S-RCF": "0256_1",             # Already mapped 
    "S-SM F": "0256_1",            # Already mapped 
    "S-TLF": "0256_1",             # Already mapped 
    
    # === ITEM 260: Guarantees and letters of credit ===
    "OBS00100100": "0260_1",       # Already mapped 
    "S-GUARANTEE": "0260_1",       # 
    
    # === ITEM 282: Unsecured funding from PSEs/NDBs (D. Additional) ===
    # (Will be populated from EQ)
}

NSFRCD_OTHER = "      "  # OTHER = '      '


def nsfrcd_fmt(value: Optional[str]) -> str:
    """VALUE $NSFRCD (local PROC FORMAT)."""
    if value is None:
        return NSFRCD_OTHER
    return NSFRCD_FMT.get(value, NSFRCD_OTHER)


# ============================================================================
# NUMERIC PARSING HELPERS (SAS informats)
# ============================================================================
def _parse_comma_number(raw: str) -> Optional[float]:
    """COMMA20.2 informat: strip thousands separators / currency symbols,
    honour parenthesised negatives; blank -> missing."""
    raw = raw.strip()
    if not raw:
        return None
    negative = raw.startswith("(") and raw.endswith(")")
    if negative:
        raw = raw[1:-1]
    raw = raw.replace(",", "").replace("$", "")
    if not raw:
        return None
    value = float(raw)
    return -value if negative else value


def _parse_plain_number(raw: str) -> Optional[float]:
    """Plain numeric informat (17.2 / 16.); blank -> missing."""
    raw = raw.strip()
    if not raw:
        return None
    return float(raw)


def _format_comma20_2(value: Optional[float]) -> str:
    """FORMAT ... COMMA20.2 for report output. MISSING=' ' -> blank field."""
    if value is None:
        return " " * 20
    return f"{value:,.2f}".rjust(20)


# ============================================================================
# DATA TEMPLATE
#   INFILE TEMPL LRECL=1000 DSD; INPUT @001 DESC $CHAR500.;
#   ITEM = _N_;  IF ITEM > 1;  *Exclude header/title row;
#   PROC SORT; BY ITEM; RUN;   -- no-op: ITEM already ascends with file order
# ============================================================================
@dataclass
class TemplateRow:
    item: int
    desc: str


def read_template(path: Path) -> list[TemplateRow]:
    rows: list[TemplateRow] = []
    with path.open("r", encoding="latin1") as fh:
        for line_no, line in enumerate(fh, start=1):
            if line_no == 1:
                continue  # ITEM = _N_; IF ITEM > 1
            desc = line.rstrip("\n").ljust(500)[:500]
            rows.append(TemplateRow(item=line_no, desc=desc))
    return rows


# ============================================================================
# DATA GL
#   INFILE GLPIVB;
#   INPUT @002 SET_ID $19.  @042 AMOUNT COMMA20.2  @062 SIGN $1.;
#   GLFMT = PUT(SET_ID,$NSFRCD.);
#   IF GLFMT NE '' THEN DO;
#      ITEM = SUBSTR(GLFMT,1,4)*1; I = SUBSTRN(GLFMT,6,1);
#      ARRAY BUCKET UTNMA1-UTNMA3;  BUCKET(I) = AMOUNT/1000;  OUTPUT;
#      IF SET_ID='LCR-FIOPSDEP' THEN UTNMA1 = -UTNMA1;
#      SELECT(SET_ID);
#         WHEN('S-STADEPBNM')  DO; ITEM=86;  OUTPUT; END;
#         WHEN('LCR-FIOPSDEP') DO; ITEM=116; OUTPUT; END;
#         WHEN('S-MARGIN COL') DO; ITEM=246; OUTPUT; END;
#         OTHERWISE;
#      END;
#   END;
#   PROC SORT DATA=GL; BY ITEM; RUN;   PROC PRINT; RUN;
# ============================================================================
@dataclass
class GLRecord:
    item: int
    utnma1: Optional[float]
    utnma2: Optional[float]
    utnma3: Optional[float]


def read_gl(path: Path) -> list[GLRecord]:
    records: list[GLRecord] = []
    unmapped = set()    # DEBUG
    mapped = set()      # DEBUG
    with path.open("r", encoding="latin1") as fh:
        for raw_line in fh:
            line = raw_line.rstrip("\n")
            if len(line) < 62:
                line = line.ljust(62)

            set_id = line[1:20].strip()      # @002 SET_ID $19.
            amount_raw = line[41:61]         # @042 AMOUNT COMMA20.2
            # sign = line[61:62]             # @062 SIGN $1. - read but not
            #                                  referenced anywhere downstream
            amount = _parse_comma_number(amount_raw)

            glfmt = nsfrcd_fmt(set_id)
            if glfmt.strip() == "":
                unmapped.add(set_id)        # DEBUG
                continue  # IF GLFMT NE '' guard - unmatched SET_ID -> no rows
            mapped.add(set_id)      # DEBUG

            item = int(glfmt[0:4])       # SUBSTR(GLFMT,1,4)*1
            bucket_idx = int(glfmt[5])   # SUBSTRN(GLFMT,6,1)

            bucket = [None, None, None]  # ARRAY BUCKET UTNMA1-UTNMA3
            if amount is not None:
                bucket[bucket_idx - 1] = amount / 1000

            records.append(GLRecord(item=item, utnma1=bucket[0], utnma2=bucket[1], utnma3=bucket[2]))

            if set_id == "LCR-FIOPSDEP" and bucket[0] is not None:
                bucket[0] = -bucket[0]

            extra_item: Optional[int] = None
            if set_id == "S-STADEPBNM":
                extra_item = 86
            elif set_id == "LCR-FIOPSDEP":
                extra_item = 116
            elif set_id == "S-MARGIN COL":
                extra_item = 246

            if extra_item is not None:
                records.append(GLRecord(item=extra_item, utnma1=bucket[0], utnma2=bucket[1], utnma3=bucket[2]))

    # DEBUG
    print(f"\n--- MAPPED SET_IDs ({len(mapped)}) ---")
    for s in sorted(mapped):
        print(f"  '{s}' -> {nsfrcd_fmt(s)}")

    # DEBUG    
    print(f"\n--- UNMAPPED SET_IDs ({len(unmapped)}) ---")
    for s in sorted(unmapped):
        print(f"  '{s}'")

    return records


def gl_records_to_df(records: list[GLRecord]) -> pl.DataFrame:
    return pl.DataFrame(
        [{"item": r.item, "utnma1": r.utnma1, "utnma2": r.utnma2, "utnma3": r.utnma3} for r in records],
        schema={"item": pl.Int64, "utnma1": pl.Float64, "utnma2": pl.Float64, "utnma3": pl.Float64},
    ).sort("item")  # PROC SORT DATA=GL; BY ITEM;


# ============================================================================
# DATA EQ
#   INFILE EQUA END=EOF DLM='|' DSD;
#   INPUT UTNREF :$10.  UTNMA1-UTNMA5 :17.2  UTNTTL :17.2;
#   ITEM = SUBSTR(UTNREF,3,5)*1;
#   PROC PRINT; RUN;   -- no sort: printed in file order
#   (END=EOF flag is read by SAS but never referenced in this DATA step)
# ============================================================================
@dataclass
class EQRecord:
    item: int
    utnma1: Optional[float]
    utnma2: Optional[float]
    utnma3: Optional[float]
    utnma4: Optional[float]
    utnma5: Optional[float]
    utnttl: Optional[float]


def read_eq(path: Path) -> list[EQRecord]:
    records: list[EQRecord] = []
    with path.open("r", encoding="latin1") as fh:
        for raw_line in fh:
            line = raw_line.rstrip("\n")
            if not line:
                continue
            fields = line.split("|")
            fields += [""] * (7 - len(fields))

            utnref = fields[0].strip()
            utnma1 = _parse_plain_number(fields[1])
            utnma2 = _parse_plain_number(fields[2])
            utnma3 = _parse_plain_number(fields[3])
            utnma4 = _parse_plain_number(fields[4])
            utnma5 = _parse_plain_number(fields[5])
            utnttl = _parse_plain_number(fields[6])

            item = int(utnref[2:7])  # SUBSTR(UTNREF,3,5)*1

            records.append(EQRecord(item, utnma1, utnma2, utnma3, utnma4, utnma5, utnttl))
    return records


def eq_records_to_df(records: list[EQRecord]) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "item": r.item, "utnma1": r.utnma1, "utnma2": r.utnma2, "utnma3": r.utnma3,
                "utnma4": r.utnma4, "utnma5": r.utnma5, "utnttl": r.utnttl,
            }
            for r in records
        ],
        schema={
            "item": pl.Int64, "utnma1": pl.Float64, "utnma2": pl.Float64, "utnma3": pl.Float64,
            "utnma4": pl.Float64, "utnma5": pl.Float64, "utnttl": pl.Float64,
        },
    )


# ============================================================================
# DATA NSFR
#   INFILE MNL1 DELIMITER=',' DSD FIRSTOBS=2;
#   INPUT LINE :$10.  UTNMA1-UTNMA3 :16.;
#   ITEM = SUBSTR(LINE,6,3)*1;   DROP LINE;
# ============================================================================
@dataclass
class NSFRRecord:
    item: int
    utnma1: Optional[float]
    utnma2: Optional[float]
    utnma3: Optional[float]


def read_nsfr_manual(path: Path) -> list[NSFRRecord]:
    records: list[NSFRRecord] = []
    with path.open("r", encoding="latin1") as fh:
        lines = fh.readlines()

    for raw_line in lines[1:]:  # FIRSTOBS=2 skips the header row
        line = raw_line.rstrip("\n")
        if not line:
            continue
        fields = line.split(",")
        fields += [""] * (4 - len(fields))

        line_field = fields[0].strip()
        utnma1 = _parse_plain_number(fields[1])
        utnma2 = _parse_plain_number(fields[2])
        utnma3 = _parse_plain_number(fields[3])

        item = int(line_field[5:8])  # SUBSTR(LINE,6,3)*1

        records.append(NSFRRecord(item, utnma1, utnma2, utnma3))
    return records


def nsfr_records_to_df(records: list[NSFRRecord]) -> pl.DataFrame:
    return pl.DataFrame(
        [{"item": r.item, "utnma1": r.utnma1, "utnma2": r.utnma2, "utnma3": r.utnma3} for r in records],
        schema={"item": pl.Int64, "utnma1": pl.Float64, "utnma2": pl.Float64, "utnma3": pl.Float64},
    )


# ============================================================================
# DATA LCRALL
#   SET GL(IN=B) EQ(IN=C) NSFR;  WHERE ITEM > 0;
#   IF B THEN SRC='WLK'; ELSE IF C THEN SRC='EQU'; ELSE SRC='MNL';
#   PROC PRINT; VAR SRC ITEM UTNMA1 UTNMA2 UTNMA3; RUN;
#   PROC SORT DATA=LCRALL; BY ITEM; RUN;
# ============================================================================
def build_lcrall(gl_df: pl.DataFrame, eq_df: pl.DataFrame, nsfr_df: pl.DataFrame) -> pl.DataFrame:
    gl_df = gl_df.with_columns(pl.lit("WLK").alias("src"))
    eq_df = eq_df.with_columns(pl.lit("EQU").alias("src"))
    nsfr_df = nsfr_df.with_columns(pl.lit("MNL").alias("src"))

    lcrall = pl.concat([gl_df, eq_df, nsfr_df], how="diagonal_relaxed")
    return lcrall.filter(pl.col("item") > 0)


# ============================================================================
# PROC SUMMARY DATA=LCRALL NWAY; BY ITEM;
#   VAR UTNMA1 UTNMA2 UTNMA3 UTNTTL;  OUTPUT OUT=TOTLCR(DROP=_FREQ_ _TYPE_) SUM=;
#   PROC PRINT; RUN;
# (BY-group processing requires sorted input in SAS; group_by + final .sort()
# below achieves the same grouped/ordered result without a separate physical
# sort pass.)
# ============================================================================
def summarize_totlcr(lcrall: pl.DataFrame) -> pl.DataFrame:
    return (
        lcrall.group_by("item")
        .agg(
            [
                pl.col("utnma1").sum().alias("utnma1"),
                pl.col("utnma2").sum().alias("utnma2"),
                pl.col("utnma3").sum().alias("utnma3"),
                pl.col("utnttl").sum().alias("utnttl"),
            ]
        )
        .sort("item")
    )


# ============================================================================
# PROC TRANSPOSE DATA=TOTLCR OUT=GTOTLCR PREFIX=L; ID ITEM; VAR UTNTTL;
# NOTE: GTOTLCR is produced in the original SAS but never referenced again by
# any subsequent step (no PROC PRINT/merge consumes it). Retained here only
# for structural parity with the original program.
# ============================================================================
def transpose_totlcr(totlcr: pl.DataFrame) -> pl.DataFrame:
    wide_row = {f"L{row['item']}": row["utnttl"] for row in totlcr.iter_rows(named=True)}
    return pl.DataFrame([wide_row]) if wide_row else pl.DataFrame()


# ============================================================================
# DATA _NULL_;
#   MERGE TEMPLATE(IN=A) TOTLCR;  BY ITEM;  IF A;
#   DLM='05'X;  FILE LCROUT;  FORMAT UTNMA1 UTNMA2 UTNMA3 COMMA20.2;
#   IF _N_=1 THEN DO; PUT @001 'PUBLIC INVESTMENT BANK BERHAD'; END;
#   UTNMA1=ABS(UTNMA1); UTNMA2=ABS(UTNMA2); UTNMA3=ABS(UTNMA3);
#   PUT @001 DESC $CHAR500. DLM UTNMA1 DLM UTNMA2 DLM UTNMA3 DLM DLM;
# LCROUT is RECFM=FB (not FBA) -> no ASA carriage control required.
# ============================================================================
# def build_report_lines(template_rows: list[TemplateRow], totlcr: pl.DataFrame) -> list[str]:
#     totlcr_map = {
#         row["item"]: (row["utnma1"], row["utnma2"], row["utnma3"])
#         for row in totlcr.iter_rows(named=True)
#     }

#     lines: list[str] = []
#     for idx, trow in enumerate(template_rows):
#         if idx == 0:
#             lines.append("PUBLIC INVESTMENT BANK BERHAD")

#         utnma1, utnma2, utnma3 = totlcr_map.get(trow.item, (None, None, None))
#         utnma1 = abs(utnma1) if utnma1 is not None else None
#         utnma2 = abs(utnma2) if utnma2 is not None else None
#         utnma3 = abs(utnma3) if utnma3 is not None else None

#         line = (
#             f"{trow.desc}{DLM}"
#             f"{_format_comma20_2(utnma1)}{DLM}"
#             f"{_format_comma20_2(utnma2)}{DLM}"
#             f"{_format_comma20_2(utnma3)}{DLM}"
#             f"{DLM}"
#         )
#         lines.append(line)
#     return lines

def build_report_lines(template_rows: list[TemplateRow], totlcr: pl.DataFrame) -> list[str]:
    totlcr_map = {
        row["item"]: (row["utnma1"], row["utnma2"], row["utnma3"])
        for row in totlcr.iter_rows(named=True)
    }

    lines: list[str] = []
    for idx, trow in enumerate(template_rows):
        if idx == 0:
            lines.append("PUBLIC INVESTMENT BANK BERHAD")

        utnma1, utnma2, utnma3 = totlcr_map.get(trow.item, (None, None, None))
        utnma1 = abs(utnma1) if utnma1 is not None else None
        utnma2 = abs(utnma2) if utnma2 is not None else None
        utnma3 = abs(utnma3) if utnma3 is not None else None

        # Fixed width format - NO DELIMITERS
        line = (
            f"{trow.desc:<500}"  # Description padded to 500 chars
            f"{_format_comma20_2(utnma1)}"  # 20 chars
            f"{_format_comma20_2(utnma2)}"  # 20 chars
            f"{_format_comma20_2(utnma3)}"  # 20 chars
        )
        lines.append(line)
    return lines


def write_report(lines: list[str], output_path: Path) -> None:
    with output_path.open("w", encoding="latin1", newline="") as fh:
        for line in lines:
            fh.write(line + "\n")


# ============================================================================
# DATA _NULL_; FILE SFTPFL;
#   PUT @1 "put //SAP.PIVB.NSFR.MTHEND.TEXT(+1)  NSFR_&RPTDT._MTH.XLS";
# ============================================================================
def write_sftp_command(rptdt: str, output_path: Path) -> Path:
    sftp_path = output_path.parent / "sftp_command.txt"
    with sftp_path.open("w", encoding="latin1") as fh:
        fh.write(f"put //SAP.PIVB.NSFR.MTHEND.TEXT(+1)  NSFR_{rptdt}_MTH.XLS\n")
    return sftp_path


# ----------------------------------------------------------------------------
# JCL step RUNSFTP (COZBATCH) - mainframe SFTP submission of the report file.
# This is mainframe/JCL infrastructure, not portable to Python; left as a
# commented placeholder for reference only, not executed:
#
# export PASSWD_DSN='OPER.PBB.CONTROL(SAS#SFTP)'
# $coz_bin/cozsftp $ssh_opts -b- sas2finlcr@192.168.56.10 <<EOB
# lzopts servercp=$servercp,notrim,overflow=trunc,mode=text
# lzopts linerule=$lr
# //           DD DISP=SHR,DSN=&&FTPPUT
# //           DD *
# EOB
# ----------------------------------------------------------------------------


def main() -> None:
    # DATA REPTDATE; REPTDATE = TODAY()-DAY(TODAY());  -> last day of prior month
    reptdate_values = get_monthly_reptdate_values()
    reptmon = reptdate_values.reptmon                       # REPTMON: Z2.
    reptday = reptdate_values.reptday                        # REPTDAY: Z2.
    rdate = reptdate_values.ddmmyy8                           # RDATE:  DDMMYY8.
    rptdt = reptdate_values.reptdate.strftime("%Y%m%d")       # RPTDT:  YYMMDDN8.

    print(f"Report date (REPTDATE) : {reptdate_values.reptdate}")
    print(f"REPTMON                : {reptmon}")
    print(f"REPTDAY                : {reptday}")
    print(f"RDATE (DDMMYY8.)       : {rdate}")
    print(f"RPTDT (YYMMDDN8.)      : {rptdt}")

    # glpivb_file = get_latest_file(INPUT_DIR, prefix=GLPIVB_PREFIX)
    # equa_file = get_latest_file(INPUT_DIR, prefix=EQUA_PREFIX)
    # mnl1_file = get_latest_file(INPUT_DIR, prefix=MNL1_PREFIX)

    glpivb_file = GLPIVB_DIR
    equa_file   = EQUA_DIR
    mnl1_file   = MNL1_DIR

    template_rows = read_template(TEMPLATE_FILE)

    gl_df = gl_records_to_df(read_gl(glpivb_file))
    print("\n--- GL (sorted by ITEM) ---")
    print(gl_df)

    eq_df = eq_records_to_df(read_eq(equa_file))
    print("\n--- EQ ---")
    print(eq_df)

    nsfr_df = nsfr_records_to_df(read_nsfr_manual(mnl1_file))

    lcrall = build_lcrall(gl_df, eq_df, nsfr_df)
    print("\n--- LCRALL (SRC, ITEM, UTNMA1-3) ---")
    print(lcrall.select(["src", "item", "utnma1", "utnma2", "utnma3"]))

    totlcr = summarize_totlcr(lcrall)
    print("\n--- TOTLCR ---")
    print(totlcr)

    _gtotlcr = transpose_totlcr(totlcr)  # produced for SAS parity only, unused further

    report_lines = build_report_lines(template_rows, totlcr)

    # RPTDT uses YYMMDDN8., not covered by output_date.py's DATE_FORMATS
    output_filename = f"NSFR_{rptdt}_MTH"
    output_path = OUTPUT_DIR / f"{output_filename}.txt"
    write_report(report_lines, output_path)

    sftp_path = write_sftp_command(rptdt, output_path)

    print(f"\nOutput report written to : {output_path}")
    print(f"SFTP command file        : {sftp_path}")
    # print("\n--- Report contents ---")
    # for line in report_lines:
    #     print(line)


if __name__ == "__main__":
    main()
