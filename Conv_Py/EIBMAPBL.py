#!/usr/bin/env python3
"""
Program : EIBMAPBL.py  (originally EIBWLIQ1 / EIBWPBLF step in JCL EIBMAPBL)
Purpose : NEW LIQUIDITY FRAMEWORK FOR PBIF — breakdown of PBIF exposures by
          remaining-maturity profile.

Dependencies:
    - PBBELF, PBBDPFMT : %INC'd in the original SAS ("%INC PGM(PBBELF,PBBDPFMT);")
      but no PUT(var, fmt.) call against either module's formats is present
      anywhere in this program's body. Kept as comment/placeholder only --
      NOT wired up as live imports since none of their functions are used.
    - RDALPBIF.py / RDLMPBIF.py : live imports, selected at runtime based on
      REPTQ (whether REPTDATE is the last day of a month).
"""

# from PBBELF import ...     # placeholder only -- no PBBELF format is
                              # actually referenced anywhere in this program.
# from PBBDPFMT import ...   # placeholder only -- no PBBDPFMT format is
                              # actually referenced anywhere in this program.

import gc
from pathlib import Path

import polars as pl

from REPTDATE import get_reptdate_values
import RDALPBIF
import RDLMPBIF

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

INPUT_CLIENT_DIR = BASE_DIR / "input" / "prod" / "EIBMAPBL"
INPUT_MECHRG_FILE = BASE_DIR / "input" / "prod" / "EIBMAPBL" / "MECHRG.txt"

CACHE_DIR  = BASE_DIR / "input" / "prod" / "EIBMAPBL"
OUTPUT_DIR = BASE_DIR / "output" / "EIBMAPBL"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

LRECL = 133   # DCB=(RECFM=FB,LRECL=133,BLKSIZE=0)

# ============================================================================
# STEP 1: REPORT DATE  (no reptdate.parquet -- derive from REPTDATE.py)
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values(year_format="%y")
reptdate  = reptdate_values.reptdate
REPTYEAR  = reptdate_values.reptyear   # 2-digit year (YEAR2.)
REPTMON   = reptdate_values.reptmon    # Z2.
REPTDAY   = reptdate_values.reptday    # Z2.
REPTYR    = reptdate.year              # 4-digit year (YEAR4. equivalent)

# REPTQ='N'; IF DAY(REPTDATE+1)=1 THEN REPTQ='Y';
next_day = reptdate.replace(day=reptdate.day) + __import__("datetime").timedelta(days=1)
REPTQ = 'Y' if next_day.day == 1 else 'N'

print(f"  Report date : {reptdate.strftime('%d/%m/%y')}")
print(f"  REPTYEAR/REPTMON/REPTDAY : {REPTYEAR}/{REPTMON}/{REPTDAY}")
print(f"  REPTQ (month-end?) : {REPTQ}")

OUTPUT_FILE = OUTPUT_DIR / f"PBIFLIQ{REPTMON}{REPTYEAR}.dat"

# ============================================================================
# STEP 2: RESOLVE CLIENT DATASET  (name embeds REPTYEAR/REPTMON/REPTDAY --
# this is the exact report date, not a "latest available" search, so the
# path is constructed directly rather than via input_date.get_latest_file())
# ============================================================================
print("\nStep 2: Resolving CLIENT dataset...")

CLIENT_SAS_PATH   = INPUT_CLIENT_DIR / f"CLIEN{REPTYEAR}{REPTMON}{REPTDAY}.sas7bdat"
CLIENT_CACHE_PATH = CACHE_DIR / f"CLIEN{REPTYEAR}{REPTMON}{REPTDAY}.parquet"

print(f"  CLIENT source : {CLIENT_SAS_PATH.name}")


def _cache_is_fresh(sas_path: Path, cache_path: Path) -> bool:
    return (
        cache_path.exists()
        and cache_path.stat().st_mtime >= sas_path.stat().st_mtime
    )


def _sas_to_parquet(sas_path: Path, cache_path: Path, tag: str) -> None:
    """Chunked .sas7bdat -> Parquet conversion (pattern from EIBDLN1M.py)."""
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer, schema, total = None, None, 0

    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=500_000)
    for chunk in reader:
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        if schema is None:
            schema = table.schema
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")
        else:
            cast_arrays = []
            for field in schema:
                col = table.column(field.name)
                if col.type != field.type:
                    try:
                        col = col.cast(field.type, safe=False)
                    except Exception:
                        col = pa.nulls(len(col), type=field.type)
                cast_arrays.append(col)
            table = pa.Table.from_arrays(cast_arrays, schema=schema)
        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()

    if writer:
        writer.close()
    print(f"  [{tag}] Done -- {total:,} rows cached.")


if not _cache_is_fresh(CLIENT_SAS_PATH, CLIENT_CACHE_PATH):
    _sas_to_parquet(CLIENT_SAS_PATH, CLIENT_CACHE_PATH, "CLIENT")
else:
    print("  [CLIENT] Cache fresh -- skipping conversion.")

# ============================================================================
# STEP 3: %RDAL -- select RDALPBIF or RDLMPBIF based on REPTQ
# ============================================================================
print("\nStep 3: Building PBIF via RDALPBIF/RDLMPBIF...")

if REPTQ != 'Y':
    pbif = RDALPBIF.build_pbif(str(CLIENT_CACHE_PATH), reptdate)
else:
    pbif = RDLMPBIF.build_pbif(str(CLIENT_CACHE_PATH), INPUT_MECHRG_FILE, reptdate)

print(f"  PBIF rows after RDAL/RDLM step: {len(pbif):,}")

# ============================================================================
# STEP 4: DAYS-IN-MONTH ARRAYS FOR REPORT MONTH (RD1-RD12 equivalent)
# RETAIN RD1-RD12=31, RD4/RD6/RD9/RD11=30, RD2=28 (29 if report year leap)
# ============================================================================
_RP_DAYS = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
            7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
if REPTYR % 4 == 0:
    _RP_DAYS[2] = 29

RPMTH = reptdate.month
RPDAY = reptdate.day
RPYR  = REPTYR

# ============================================================================
# STEP 5: DATA PBIF; LENGTH ITEM $6; %DCLVAR; SET PBIF;
# DAYS=MATDTE-&RDATX; IF DAYS<8 THEN REMMTH=0.1; ELSE %REMMTH;
# ============================================================================
print("\nStep 5: Computing DAYS / REMMTH...")


def _remmth(matdte, rpyr: int, rpmth: int, rpday: int) -> float:
    """%REMMTH macro."""
    mdyr, mdmth, mdday = matdte.year, matdte.month, matdte.day
    # MD2 = 29/28 computed in SAS but never used afterwards -- kept only
    # as a comment for fidelity, no functional effect.
    # md2 = 29 if (mdmth == 2 and mdyr % 4 == 0) else 28

    rp_days_for_rpmth = _RP_DAYS[rpmth]
    if mdday > rp_days_for_rpmth:
        mdday = rp_days_for_rpmth

    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday
    return remy * 12 + remm + remd / rp_days_for_rpmth


rows = pbif.to_dicts()
step5_rows = []
for row in rows:
    matdte = row["MATDTE"]
    days = (matdte - reptdate).days
    if days < 8:
        remmth = 0.1
    else:
        remmth = _remmth(matdte, RPYR, RPMTH, RPDAY)
    row["DAYS"] = days
    row["REMMTH"] = remmth
    step5_rows.append(row)

pbif = pl.DataFrame(step5_rows)

# ============================================================================
# STEP 6: ITEM/AMOUNT/PART CLASSIFICATION AND MULTI-ROW OUTPUT
# IF CUSTCX IN ('77','78','95','96') THEN ITEM='A1.08'; ELSE ITEM='A1.04';
# AMOUNT=BALANCE; PART='2-RM'; OUTPUT; PART='1-RM'; OUTPUT;
# ITEM='A1.28'; AMOUNT=(INLIMIT-BALANCE); IF AMOUNT<0 THEN AMOUNT=0;
# PART='2-RM'; OUTPUT;
# ============================================================================
print("\nStep 6: Building multi-part PBIF observations...")

pbif = pbif.with_columns(
    pl.when(pl.col("CUSTCX").is_in(["77", "78", "95", "96"]))
      .then(pl.lit("A1.08")).otherwise(pl.lit("A1.04")).alias("_ITEM_BASE")
)

part_2rm_base = pbif.with_columns([
    pl.col("_ITEM_BASE").alias("ITEM"),
    pl.col("BALANCE").alias("AMOUNT"),
    pl.lit("2-RM").alias("PART"),
])
part_1rm_base = pbif.with_columns([
    pl.col("_ITEM_BASE").alias("ITEM"),
    pl.col("BALANCE").alias("AMOUNT"),
    pl.lit("1-RM").alias("PART"),
])
part_2rm_undrawn = pbif.with_columns([
    pl.lit("A1.28").alias("ITEM"),
    pl.when((pl.col("INLIMIT") - pl.col("BALANCE")) < 0.0)
      .then(0.0).otherwise(pl.col("INLIMIT") - pl.col("BALANCE")).alias("AMOUNT"),
    pl.lit("2-RM").alias("PART"),
])

keep_cols = [c for c in pbif.columns if c not in ("_ITEM_BASE", "ITEM", "AMOUNT", "PART")]
select_cols = keep_cols + ["ITEM", "AMOUNT", "PART"]

pbif_stage1 = pl.concat([
    part_2rm_base.select(select_cols),
    part_1rm_base.select(select_cols),
    part_2rm_undrawn.select(select_cols),
])

# ============================================================================
# STEP 7: DATA PBIF2; SET PBIF; AMOUNT=(INLIMIT-BALANCE)*0.20;
# IF AMOUNT<0 THEN AMOUNT=0; ITEM='A1.28'; PART='1-RM';
# (uses the *original* per-record REMMTH etc. before the STEP 6 multi-row
#  expansion, matching "DATA PBIF2; SET PBIF;" referring to the DATA PBIF
#  produced at the end of STEP 5/6 body -- i.e. one row per base record)
# ============================================================================
print("\nStep 7: Building PART=1-RM undrawn (20%) rows...")

pbif2 = pbif.with_columns([
    pl.when((pl.col("INLIMIT") - pl.col("BALANCE")) * 0.20 < 0.0)
      .then(0.0).otherwise((pl.col("INLIMIT") - pl.col("BALANCE")) * 0.20).alias("AMOUNT"),
    pl.lit("A1.28").alias("ITEM"),
    pl.lit("1-RM").alias("PART"),
]).select(select_cols)

# ============================================================================
# STEP 8: DATA PBIF; SET PBIF PBIF2;  (concatenate)
# PROC SUMMARY DATA=PBIF NWAY; CLASS PART ITEM REMMTH; VAR AMOUNT;
# OUTPUT OUT=PBIF(DROP=_TYPE_ _FREQ_) SUM=;
# ============================================================================
print("\nStep 8: Concatenating and summarizing by PART/ITEM/REMMTH...")

pbif_all = pl.concat([pbif_stage1, pbif2])

pbif_summary = (
    pbif_all
    .group_by(["PART", "ITEM", "REMMTH"])
    .agg(pl.col("AMOUNT").sum().alias("AMOUNT"))
)

# ============================================================================
# STEP 9: DATA PBIF2; SET PBIF; FORMAT REMMTH REMFMT.;
# WHERE PART='2-RM' AND ITEM='A1.04';
# ============================================================================
print("\nStep 9: Filtering PART=2-RM / ITEM=A1.04 and bucketing REMMTH...")


def _remfmt(remmth: float) -> str:
    """PROC FORMAT VALUE REMFMT."""
    if remmth <= 0.255:
        return 'UP TO 1 WK'
    elif remmth <= 1:
        return '>1 WK - 1 MTH'
    elif remmth <= 3:
        return '>1 MTH - 3 MTHS'
    elif remmth <= 6:
        return '>3 - 6 MTHS'
    elif remmth <= 12:
        return '>6 MTHS - 1 YR'
    elif remmth <= 36:
        return '>1 YR  - 3 YR'
    elif remmth <= 60:
        return '>3 YR  - 5 YR'
    else:
        return '>5 YR'


pbif2_filtered = pbif_summary.filter(
    (pl.col("PART") == "2-RM") & (pl.col("ITEM") == "A1.04")
).with_columns(
    pl.col("REMMTH").map_elements(_remfmt, return_dtype=pl.Utf8).alias("REMMTH_BUCKET")
)

# ============================================================================
# STEP 10: PROC SUMMARY DATA=PBIF2 NWAY; CLASS ITEM REMMTH; VAR AMOUNT;
# OUTPUT OUT=PBIF3(DROP=_TYPE_ _FREQ_) SUM=;
# NOTE: CLASS REMMTH groups by the FORMATTED (bucketed) value since REMFMT
# is assigned to REMMTH -- replicated here by grouping on REMMTH_BUCKET.
# ============================================================================
print("\nStep 10: Final summary by ITEM / REMMTH bucket...")

pbif3 = (
    pbif2_filtered
    .group_by(["ITEM", "REMMTH_BUCKET"])
    .agg(pl.col("AMOUNT").sum().alias("AMOUNT"))
    .sort(["ITEM", "REMMTH_BUCKET"])
)

print(pbif3)

# ============================================================================
# STEP 11: $ITEMF FORMAT
# ============================================================================
_ITEMF_MAP = {
    'A1.01':  'A1.01  LOANS: CORP - FIXED TERM LOANS',
    'A1.02':  'A1.02  LOANS: CORP - REVOLVING LOANS',
    'A1.03':  'A1.03  LOANS: CORP - OVERDRAFTS',
    'A1.04':  'A1.04  LOANS: CORP - OTHERS',
    'A1.05':  'A1.05  LOANS: IND  - HOUSING LOANS',
    'A1.07':  'A1.07  LOANS: IND  - OVERDRAFTS',
    'A1.08':  'A1.08  LOANS: IND  - OTHERS',
    'A1.08A': 'A1.08A LOANS: IND  - REVOLVING LOANS',
    'A1.12':  'A1.12  DEPOSITS: CORP - FIXED',
    'A1.13':  'A1.13  DEPOSITS: CORP - SAVINGS',
    'A1.14':  'A1.14  DEPOSITS: CORP - CURRENT',
    'A1.15':  'A1.15  DEPOSITS: IND  - FIXED',
    'A1.16':  'A1.16  DEPOSITS: IND  - SAVINGS',
    'A1.17':  'A1.17  DEPOSITS: IND  - CURRENT',
    'A1.25':  'A1.25  UNDRAWN OD FACILITIES GIVEN',
    'A1.28':  'A1.28  UNDRAWN PORTION OF OTHER C/F GIVEN',
    'A2.01':  'A2.01  INTERBANK LENDING/DEPOSITS',
    'A2.02':  'A2.02  REVERSE REPO',
    'A2.03':  'A2.03  DEBT SEC: GOVT PP/BNM BILLS/CAG',
    'A2.04':  'A2.04  DECT SEC: FIN INST PAPERS',
    'A2.05':  'A2.05  DEBT SEC: TRADE PAPERS',
    'A2.06':  'A2.06  CORP DEBT: GOVT-GUARANTEED',
    'A2.08':  'A2.08  CORP DEBT: NON-GUARANTEED',
    'A2.09':  'A2.09  FX EXCHG CONTRACTS RECEIVABLE',
    'A2.14':  'A2.14  INTERBANK BORROWINGS/DEPOSITS',
    'A2.15':  'A2.15  INTERBANK REPOS',
    'A2.16':  'A2.16  NON-INTERBANK REPOS',
    'A2.17':  'A2.17  NIDS ISSUED',
    'A2.18':  'A2.18  BAS PAYABLE',
    'A2.19':  'A2.19  FX EXCHG CONTRACTS PAYABLE',
    'B1.12':  'B1.12  DEPOSITS: CORP - FIXED',
    'B1.15':  'B1.15  DEPOSITS: IND  - FIXED',
    'B2.01':  'B2.01  INTERBANK LENDING/DEPOSITS',
    'B2.09':  'B2.09  FX EXCHG CONTRACTS RECEIVABLE',
    'B2.14':  'B2.14  INTERBANK BORROWINGS/DEPOSITS',
    'B2.19':  'B2.19  FX EXCHG CONTRACTS PAYABLE',
}


def _itemf(code: str) -> str:
    return _ITEMF_MAP.get(code, code)


# ============================================================================
# STEP 12: DATA _NULL_; SET PBIF3; FORMAT REMMTH REMFMT. ITEM $ITEMF.;
# FILE PBIFNLF;
# IF _N_=1 THEN PUT @1 'NLFPBIF' "&REPTDAY" "&REPTMON" "&REPTYEAR";
# PUT @1 ITEM ';' REMMTH ';' AMOUNT +(-1) ';';
# ============================================================================
print("\nStep 12: Generating output report (RECFM=FB, LRECL=133)...")

output_lines: list[str] = []

header = f"NLFPBIF{REPTDAY}{REPTMON}{REPTYEAR}"
output_lines.append(header.ljust(LRECL))

for row in pbif3.iter_rows(named=True):
    item_label = _itemf(row["ITEM"])
    remmth_label = row["REMMTH_BUCKET"]
    amount_str = f"{row['AMOUNT']:.2f}"
    line = f"{item_label};{remmth_label};{amount_str};"
    output_lines.append(line.ljust(LRECL)[:LRECL] if len(line) <= LRECL else line)

with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for ln in output_lines:
        fh.write(ln + "\n")

print(f"\n  Output written : {OUTPUT_FILE}")
print(f"  Total lines    : {len(output_lines):,}")
print("\n  Output content:")
for ln in output_lines:
    print(f"    {ln.rstrip()}")

gc.collect()
print("\nEIBMAPBL complete.")
