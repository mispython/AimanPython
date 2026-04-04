#!/usr/bin/env python3
"""
Program : EIBWCATR.py
Purpose : CIS Weekly Deposit – transpose multi-party ICNO/NAME into a single
          keyed row per account and write DEPOSIT.CISDEPWK.parquet.
"""

import struct
from pathlib import Path

import polars as pl

# ============================================================================
# PATH SETUP
# ============================================================================

BASE_IN  = Path("input_flat")          # root for fixed-width .txt inputs
BASE_OUT = Path("output_parquet")

(BASE_OUT / "DEPOSIT").mkdir(parents=True, exist_ok=True)

PATHS = {
    # Fixed-width flat files  (mainframe INFILE datasets)
    "DPTRBL1":  BASE_IN / "BNMCTR" / "CISWEEK" / "ACCT01.txt",
    "DPTRBL2":  BASE_IN / "BNMCTR" / "CISWEEK" / "ACCT02.txt",
    "DPTRBL3":  BASE_IN / "BNMCTR" / "CISWEEK" / "ACCT03.txt",
    "DPTRBL4":  BASE_IN / "BNMCTR" / "CISWEEK" / "ACCT04.txt",
    "DPTRBL5":  BASE_IN / "BNMCTR" / "CISWEEK" / "ACCT05.txt",
    "DPTRBL6":  BASE_IN / "BNMCTR" / "CISWEEK" / "ACCT06.txt",
    "DPTRBL7":  BASE_IN / "BNMCTR" / "CISWEEK" / "ACCT07.txt",
    "DPTRBL8":  BASE_IN / "BNMCTR" / "CISWEEK" / "ACCT08.txt",
    "DPTRBL9":  BASE_IN / "BNMCTR" / "CISWEEK" / "ACCT09.txt",
    "DPTRBL10": BASE_IN / "BNMCTR" / "CISWEEK" / "ACCT10.txt",
    "DPTRBORG": BASE_IN / "BNMCTR" / "CISWEEK" / "ORG" / "OTH.txt",
    "DPTRB999": BASE_IN / "BNMCTR" / "CISWEEK" / "ORG" / "SA9.txt",
}

# Output Parquet
OUT_CISDEPWK = BASE_OUT / "DEPOSIT" / "CISDEPWK.parquet"

# ============================================================================
# FIXED-WIDTH RECORD LAYOUT
# (SAS 1-based column → Python 0-based slice end = start + length - 1)
#
#  @001 BANKNO    3.    → cols  0: 3   (numeric)
#  @004 APPLCODE $5.   → cols  3: 8   (char)
#  @010 ACCTNO   10.   → cols  9:19   (numeric)
#  @029 PRISEC   $1.   → cols 28:29   (char)
#  @030 RELCODE   3.   → cols 29:32   (numeric)
#  @033 CUSTNO  $11.   → cols 32:43   (char)
#  @053 INDORG   $1.   → cols 52:53   (char)
#  @054 ACCTBRCH  7.   → cols 53:60   (numeric)
#  @061 ICNOX   $39.   → cols 60:99   (char)
#  @101 ECPCODE   3.   → cols100:103  (numeric)
#  @104 NAME    $40.   → cols103:143  (char)
#
# Total minimum record length = 143 bytes.
# ============================================================================

RECORD_LEN = 143  # minimum; longer lines are safe (extra bytes ignored)


def _decode(raw: bytes) -> str:
    """Decode a byte slice as EBCDIC or ASCII, stripping trailing whitespace."""
    try:
        return raw.decode("cp037").rstrip()   # EBCDIC (mainframe)
    except Exception:
        return raw.decode("latin-1").rstrip() # ASCII fallback


def parse_flat_file(path: Path) -> pl.DataFrame:
    """
    Read a fixed-width mainframe flat file and return a Polars DataFrame with
    columns matching the SAS INPUT statement, plus derived ICNO.

    ICNO derivation (mirrors SAS):
      IF ICNOX = '  ' OR SUBSTR(ICNOX,4,5) IN ('99999','DUPLI')
      THEN ICNO = CUSTNO;
      ELSE ICNO = ICNOX;

    Note: SAS SUBSTR is 1-based, so SUBSTR(ICNOX,4,5) = Python ICNOX[3:8].
    """
    rows: list[dict] = []

    with open(path, "rb") as fh:
        for raw_line in fh:
            # Strip newline bytes; pad if shorter than expected
            line = raw_line.rstrip(b"\r\n")
            if len(line) < RECORD_LEN:
                line = line.ljust(RECORD_LEN)

            bankno   = _decode(line[0:3]).strip()
            applcode = _decode(line[3:8])
            acctno_s = _decode(line[9:19]).strip()
            prisec   = _decode(line[28:29])
            relcode  = _decode(line[29:32]).strip()
            custno   = _decode(line[32:43])
            indorg   = _decode(line[52:53])
            acctbrch = _decode(line[53:60]).strip()
            icnox    = _decode(line[60:99])
            ecpcode  = _decode(line[100:103]).strip()
            name     = _decode(line[103:143])

            # Convert numeric strings
            try:
                bankno_n = int(bankno) if bankno else None
            except ValueError:
                bankno_n = None
            try:
                acctno_n = int(acctno_s) if acctno_s else None
            except ValueError:
                acctno_n = None
            try:
                relcode_n = int(relcode) if relcode else None
            except ValueError:
                relcode_n = None
            try:
                acctbrch_n = int(acctbrch) if acctbrch else None
            except ValueError:
                acctbrch_n = None
            try:
                ecpcode_n = int(ecpcode) if ecpcode else None
            except ValueError:
                ecpcode_n = None

            # ICNO derivation
            # SAS SUBSTR(ICNOX,4,5) = characters at positions 4-8 (1-based) = [3:8] in Python
            icnox_stripped = icnox.strip()
            sub_icnox = icnox[3:8] if len(icnox) >= 8 else ""
            if icnox_stripped == "" or sub_icnox in ("99999", "DUPLI"):
                icno = custno
            else:
                icno = icnox

            rows.append({
                "BANKNO":   bankno_n,
                "APPLCODE": applcode,
                "ACCTNO":   acctno_n,
                "PRISEC":   prisec,
                "RELCODE":  relcode_n,
                "CUSTNO":   custno,
                "INDORG":   indorg,
                "ACCTBRCH": acctbrch_n,
                "ICNOX":    icnox,
                "ECPCODE":  ecpcode_n,
                "NAME":     name,
                "ICNO":     icno,
            })

    return pl.DataFrame(rows)


def load_dep(path: Path) -> pl.DataFrame:
    """
    Parse flat file and sort by ACCTNO, ICNO (mirrors PROC SORT BY ACCTNO ICNO).
    """
    return parse_flat_file(path).sort(["ACCTNO", "ICNO"])


# ============================================================================
# MULTI-PARTY TRANSPOSE  (DEP2..DEP8 → TRANS02..TRANS08)
#
# SAS logic per stream:
#   1. PROC TRANSPOSE DATA=DEPn OUT=TRANSn BY ACCTNO; VAR ICNO;
#      → produces COL1..COLn per ACCTNO group
#   2. PROC TRANSPOSE DATA=DEPn OUT=TRANSnN RENAME(COL1=COLN1..COLn=COLNn);
#      BY ACCTNO; VAR NAME;
#      → produces COLN1..COLNn per ACCTNO group
#   3. DATA TRANSn; MERGE TRANSn TRANSnN; BY ACCTNO;
#   4. PROC SORT DATA=DEPn OUT=DEPn NODUPKEYS; BY ACCTNO;
#   5. DATA TRANSn; MERGE TRANSn DEPn; BY ACCTNO;
#      KEY    = COMPRESS(COL1)||','||..||COMPRESS(COLn)
#      KEYNAME = COMPBL(COLN1||','||..||COLNn)
#      JOIN   = n
# ============================================================================

def make_trans(df_dep: pl.DataFrame, n: int, join_val: int) -> pl.DataFrame:
    """
    Pivot ICNO and NAME into wide format, merge back with deduped base row,
    and compute KEY / KEYNAME / JOIN.
    """
    # Step 1 & 2: pivot ICNO → COL1..COLn, NAME → COLN1..COLNn
    icno_wide = (
        df_dep.group_by("ACCTNO")
        .agg([pl.col("ICNO").nth(i).alias(f"COL{i+1}") for i in range(n)])
    )
    name_wide = (
        df_dep.group_by("ACCTNO")
        .agg([pl.col("NAME").nth(i).alias(f"COLN{i+1}") for i in range(n)])
    )

    # Step 3: merge transposed ICNO and NAME wide tables
    trans = icno_wide.join(name_wide, on="ACCTNO", how="inner")

    # Step 4 & 5: NODUPKEYS DEPn then merge with transposed table
    base = df_dep.unique(subset=["ACCTNO"], keep="first")
    trans = trans.join(base, on="ACCTNO", how="left")

    icno_cols = [f"COL{i+1}" for i in range(n)]
    name_cols = [f"COLN{i+1}" for i in range(n)]

    # KEY  = COMPRESS each COLi (remove all spaces) joined by ','
    # KEYNAME = COMPBL of all COLNi joined by ',' (collapse runs of spaces to single space)
    return trans.with_columns([
        pl.concat_str(
            *[pl.col(c).fill_null("").str.replace_all(" ", "") for c in icno_cols],
            separator=","
        ).alias("KEY"),
        pl.concat_str(
            *[pl.col(c).fill_null("").str.replace_all(r"\s+", " ") for c in name_cols],
            separator=","
        ).alias("KEYNAME"),
        pl.lit(join_val).alias("JOIN"),
    ])


# ============================================================================
# SINGLE-PARTY STREAMS  (DPTRBL1, DPTRBORG, DPTRB999 → TRANS01, TRANORG, TRAN999)
#
# SAS:
#   DATA TRANSx;
#     INFILE <ddname>;
#     ... INPUT / ICNO derivation ...;
#     KEY     = COMPRESS(ICNO);
#     KEYNAME = NAME;          ← raw NAME, NOT COMPBL
#     JOIN    = n;
#   [ PROC SORT NODUPKEYS BY ACCTNO; ]  ← only for TRANORG and TRAN999, NOT TRANS01
# ============================================================================

def make_single(
    path: Path,
    join_val: int,
    nodupkeys: bool = False,
) -> pl.DataFrame:
    """
    Build a single-party stream DataFrame.

    Parameters
    ----------
    path       : Path to the fixed-width flat file.
    join_val   : Value assigned to the JOIN column.
    nodupkeys  : If True, deduplicate by ACCTNO (mirrors PROC SORT NODUPKEYS).
                 TRANS01 does NOT get deduplicated; TRANORG and TRAN999 do.
    """
    df = parse_flat_file(path)

    df = df.with_columns([
        # KEY = COMPRESS(ICNO) – remove all spaces
        pl.col("ICNO").fill_null("").str.replace_all(" ", "").alias("KEY"),
        # KEYNAME = NAME  (raw, no COMPBL – SAS uses bare assignment: KEYNAME = NAME)
        pl.col("NAME").alias("KEYNAME"),
        pl.lit(join_val).alias("JOIN"),
    ])

    if nodupkeys:
        df = df.unique(subset=["ACCTNO"], keep="first")

    return df


# ============================================================================
# PIPELINE
# ============================================================================

# ── Load and sort DEP2..DEP10 (flat files)  ─────────────────────────────────
DEP2  = load_dep(PATHS["DPTRBL2"])
DEP3  = load_dep(PATHS["DPTRBL3"])
DEP4  = load_dep(PATHS["DPTRBL4"])
DEP5  = load_dep(PATHS["DPTRBL5"])
DEP6  = load_dep(PATHS["DPTRBL6"])
DEP7  = load_dep(PATHS["DPTRBL7"])
DEP8  = load_dep(PATHS["DPTRBL8"])
# DEP9 and DEP10 are read and printed in SAS (PROC PRINT) but are NOT included
# in the final SET statement; loaded here for parity only.
DEP9  = load_dep(PATHS["DPTRBL9"])
DEP10 = load_dep(PATHS["DPTRBL10"])

# ── Build TRANS02..TRANS08 (multi-party transposed streams)  ─────────────────
TRANS02 = make_trans(DEP2,  2, join_val=2)
TRANS03 = make_trans(DEP3,  3, join_val=3)
TRANS04 = make_trans(DEP4,  4, join_val=4)
TRANS05 = make_trans(DEP5,  5, join_val=5)
TRANS06 = make_trans(DEP6,  6, join_val=6)
TRANS07 = make_trans(DEP7,  7, join_val=7)
TRANS08 = make_trans(DEP8,  8, join_val=8)

# ── Single-party streams (read from flat files, not Parquet)  ────────────────
# TRANS01: no NODUPKEYS (SAS omits PROC SORT NODUPKEYS for this stream)
TRANS01 = make_single(PATHS["DPTRBL1"],  join_val=1, nodupkeys=False)
# TRANORG: PROC SORT DATA=TRANORG NODUPKEYS BY ACCTNO
TRANORG = make_single(PATHS["DPTRBORG"], join_val=0, nodupkeys=True)
# TRAN999: PROC SORT DATA=TRAN999 NODUPKEYS BY ACCTNO
TRAN999 = make_single(PATHS["DPTRB999"], join_val=1, nodupkeys=True)

# ── Final SET and NODUPKEYS  ─────────────────────────────────────────────────
# SAS drops COL*/COLN* columns via SET ... (DROP=...) and drops _NAME_ (the
# PROC TRANSPOSE metadata column, which we never create).  The vertical_relaxed
# concat handles differing schemas (COL1..COLn present only in their respective
# streams) by filling missing columns with null.
combined = pl.concat(
    [TRANS02, TRANS03, TRANS04, TRANS05, TRANS06, TRANS07, TRANS08,
     TRANS01, TRANORG, TRAN999],
    how="vertical_relaxed",
)

# PROC SORT DATA=DEPOSIT.CISDEPWK NODUPKEYS; BY ACCTNO;
# Keep only the columns that survive the DROP= clauses (KEY, KEYNAME, JOIN
# plus the base columns from DEPn; COL*/COLN* are dropped).
drop_cols = [c for c in combined.columns if c.startswith("COL")]
CISDEPWK = (
    combined
    .drop(drop_cols)
    .unique(subset=["ACCTNO"], keep="first")
    .sort("ACCTNO")
)

# ── Write output Parquet  ────────────────────────────────────────────────────
CISDEPWK.write_parquet(OUT_CISDEPWK)

print(f"Wrote: {OUT_CISDEPWK}  ({len(CISDEPWK):,} rows)")
