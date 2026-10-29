"""
Program : EIBDRMFC.py
Purpose : GENERATE A DAILY REPORT SEGREGATED INTO RINGGIT AND
          FOREIGN CURRENCY DEPOSIT
"""

from pathlib import Path
from datetime import date, timedelta
import pandas as pd 
import polars as pl
import duckdb

# ============================================================
# CONTROL FILE CONFIGURATION
# ============================================================
# Control file : batch date per source system
CTL_BATCH_DTTM = Path("/sasdata/dwh/control/ctl_dwh_batch_dttm.sas7bdat")
BATCH_SOURCE   = "DP"          # SOURCE_SYSTEM_CD used for this program


def get_batch_date(source_system_cd: str) -> date:
    """Read BATCH_DTTM for the given SOURCE_SYSTEM_CD from the control file."""
    ctl = pd.read_sas(str(CTL_BATCH_DTTM), encoding="latin1")
    ctl["SOURCE_SYSTEM_CD"] = ctl["SOURCE_SYSTEM_CD"].astype(str).str.strip()
    hit = ctl.loc[ctl["SOURCE_SYSTEM_CD"] == source_system_cd, "BATCH_DTTM"]
    if hit.empty:
        raise ValueError(f"SOURCE_SYSTEM_CD '{source_system_cd}' not found in {CTL_BATCH_DTTM}")
    return pd.Timestamp(hit.iloc[0]).date()


# ============================================================
# REPORT DATE  (from batch control file, SOURCE_SYSTEM_CD = DP)
# OPTIONS YEARCUTOFF=1950 is applied in _parse_orgdate
# ============================================================
REPTDATE = get_batch_date(BATCH_SOURCE)      # date object (T)
# REPTDATE = date(2026, 10, 27)         # UAT testing date
T1       = REPTDATE + timedelta(days=1)
T2       = REPTDATE + timedelta(days=2)
T3       = REPTDATE + timedelta(days=3)

REPDD  = REPTDATE.strftime("%d")             # DD  (Z2.)
REPMM  = REPTDATE.strftime("%m")             # MM  (Z2.)
REPYY  = REPTDATE.strftime("%Y")             # YEAR4.
REPDT  = REPTDATE.strftime("%d/%m/%Y")       # DDMMYY8. equivalent as used in the existing program
REPYMD = REPTDATE.strftime("%y%m%d")         # date token in input filename

# ============================================================
# PATH CONFIGURATION
# ============================================================
# Production Path
BASE_DIR        = Path("/dwh")
INPUT_FCY_DIR   = BASE_DIR / "dp_fcy"
INPUT_FDCD_DIR  = BASE_DIR / "dpd_fdcd"
OUTPUT_DIR      = Path("/host_pq/mis/output/deposit/report")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# # Testing Path
# BASE_DIR   = Path("/stgsrcsys/host/uat/AII")
# INPUT_DIR  = BASE_DIR / "from_dwh"
# OUTPUT_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS") / "output/EIBDRMFC"
# OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "FDRMFC.txt"

# ============================================================
# REPORT DATE DERIVATION  (replaces DATA REPTDATE / CALL SYMPUT)
# ============================================================
RDATE = REPTDATE.strftime("%d/%m/%y")       # PUT(REPTDATE, DDMMYY8.)
SDATE = REPTDATE.toordinal()                # PUT(REPTDATE, 8.)  → ordinal

# ============================================================
# READ INPUT: MNIFD.FD  (.sas7bdat)
# ============================================================
# fd_path = INPUT_DIR / "fd.sas7bdat"
fd_path = INPUT_FCY_DIR / f"fcy{REPYMD}.sas7bdat"

fd_pd = pd.read_sas(str(fd_path), format="sas7bdat", encoding="latin1")
fd = pl.from_pandas(fd_pd)

fd = fd.with_columns([
    pl.col("CURBALRM").cast(pl.Float64),
    pl.col("FORATE").str.strip_chars().cast(pl.Float64),
    pl.col("RATE").cast(pl.Float64),
    pl.col("MATDATE").cast(pl.Int64),
])

# ============================================================
# DATA FD STEP
#   KEEP  CURCODE CURBALRM FORATE MATDATE RATE
#   WHERE CURBALRM GT 0
# ============================================================
fd = fd.select([
    "CURCODE", "CURBALRM", "FORATE", "MATDATE", "RATE",
]).filter(pl.col("CURBALRM") > 0)

# Convert CURBALRM to MYR equivalent when CURCODE != 'MYR'
fd = fd.with_columns(
    pl.when(pl.col("CURCODE") != "MYR")
    .then(pl.col("CURBALRM") / pl.col("FORATE"))
    .otherwise(pl.col("CURBALRM"))
    .alias("CURBALRM")
)

# ============================================================
# READ INPUT: fdcd{yymmdd}.sas7bdat  — LOCAL (MYR) DEPOSITS
#   Read AFTER the FCY-only conversion so MYR bypasses the
#   FORATE division entirely. CURBALRM here is already in RM.
# ============================================================
myr_path = INPUT_FDCD_DIR / f"fdcd{REPYMD}.sas7bdat"
myr_pd   = pd.read_sas(str(myr_path), format="sas7bdat", encoding="latin1")
myr      = pl.from_pandas(myr_pd)

# # DEBUG
# bal_name = "CURBALRM" if "CURBALRM" in myr.columns else "CURBAL"
# print("=== RAW fdcd file ===")
# print("rows          :", myr.height)
# print("rows (bal>0)  :", myr.filter(pl.col(bal_name) > 0).height)
# print("CURBALRM sum  :", myr[bal_name].sum())
# print("SAS expected  : 156242125961.64")
# print("Your output   : 156242125967.62")

# # DEBUG
# print("=== fdcd schema & numeric sums ===")
# TARGET = 156242125961.64
# for name, dtype in myr.schema.items():
#     if dtype in (pl.Float32, pl.Float64, pl.Int8, pl.Int16, pl.Int32, pl.Int64,
#                  pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
#         s = myr[name].sum()
#         delta = (s - TARGET) if isinstance(s, (int, float)) else None
#         flag  = "  <-- ≈ SAS" if delta is not None and abs(delta) < 1.0 else ""
#         print(f"  {name:20s}  sum = {s}{flag}")
# print("  (target from SAS)                    ", TARGET)

bal_col = "CURBALRM" if "CURBALRM" in myr.columns else "CURBAL"

myr = myr.select([
    pl.lit("MYR").alias("CURCODE"),
    pl.col(bal_col).cast(pl.Float64).alias("CURBALRM"),
    pl.lit(1.0).alias("FORATE"),         # unused downstream; kept for schema
    pl.col("MATDATE").cast(pl.Int64),
    pl.col("RATE").cast(pl.Float64),
]).filter(pl.col("CURBALRM") > 0)

fd = pl.concat([fd, myr], how="vertical")

# Parse MATDATE (stored as numeric YYYYMMDD integer) → Python ordinal
# SAS: DD=SUBSTR(PUT(MATDATE,Z8.),7,2); MM=...,5,2; YY=...,1,4
# MATDATE NOT IN (0,.) → valid dates only
def parse_matdate_ordinal(val) -> int:
    if val is None or val == 0:
        return 0
    try:
        ival = int(val)
        if ival > 10_000_000:                      # YYYYMMDD
            s = f"{ival:08d}"
            return date(int(s[0:4]), int(s[4:6]), int(s[6:8])).toordinal()
        return date(1960, 1, 1).toordinal() + ival # SAS date
    except Exception:
        return 0

fd = fd.with_columns(
    pl.col("MATDATE").map_elements(parse_matdate_ordinal, return_dtype=pl.Int64)
    .alias("MATDATE_ORD")
)

# REPTDATE = &SDATE (report date ordinal)
# TENOR = MATDATE - REPTDATE
# AMTENOR = CURBALRM * TENOR
# AMTENORATE = AMTENOR * RATE
fd = fd.with_columns(
    pl.lit(SDATE).cast(pl.Int64).alias("REPTDATE"),
).with_columns(
    (pl.col("MATDATE_ORD") - pl.col("REPTDATE")).alias("TENOR"),
).with_columns(
    (pl.col("CURBALRM") * pl.col("TENOR")).alias("AMTENOR"),
).with_columns(
    (pl.col("AMTENOR") * pl.col("RATE")).alias("AMTENORATE"),
)

# IF MATDATE EQ REPTDATE THEN DELETE
fd = fd.filter(pl.col("MATDATE_ORD") != pl.col("REPTDATE"))

# ============================================================
# PROC SUMMARY: SUM by CURCODE
# ============================================================
con = duckdb.connect(database=":memory:")
con.register("fd", fd.to_arrow())

# allfd = con.execute("""
#     SELECT
#         CURCODE,
#         COUNT(*)           AS NOACCT,
#         SUM(CURBALRM)      AS CURBALRM,
#         SUM(AMTENOR)       AS AMTENOR,
#         SUM(AMTENORATE)    AS AMTENORATE
#     FROM fd
#     GROUP BY CURCODE
#     ORDER BY CURCODE
# """).pl()
allfd = con.execute("""
    SELECT
        CURCODE,
        COUNT(*)                  AS NOACCT,
        SUM(CAST(CURBALRM AS DECIMAL(38, 6)))   AS CURBALRM,
        SUM(CAST(AMTENOR  AS DECIMAL(38, 6)))   AS AMTENOR,
        SUM(CAST(AMTENORATE AS DECIMAL(38, 6))) AS AMTENORATE
    FROM fd
    GROUP BY CURCODE
    ORDER BY CURCODE
""").pl()

# DATA FD: derive averages
allfd = allfd.with_columns(
    (pl.col("AMTENOR")    / pl.col("CURBALRM"))   .alias("AVGTENOR"),
    (pl.col("AMTENORATE") / pl.col("AMTENOR"))  .alias("AVGRATE"),
)

con.close()

# ============================================================
# REPORT GENERATION  (DATA _NULL_ / FILE SASLIST)
# RECFM=FB, LRECL=150
# ============================================================
LRECL    = 150
PAGE_LEN = 60   # default page length (lines per page)

def pad(line: str, lrecl: int = LRECL) -> str:
    """Pad / truncate line to fixed record length (no ASA for FB)."""
    return line.ljust(lrecl)[:lrecl]

rows = allfd.to_dicts()

report_lines: list[str] = []

# Header block (written when _N_ = 1)
report_lines.append(pad(f"REPORT ID : EIBDRMFC"))
report_lines.append(pad(f"OUTSTANDING CUSTOMERS FCY FIXED DEPOSIT PLACEMENTS"))
report_lines.append(pad(f"(FOR INDIVIDUALS AND CORPORATES) AS AT {RDATE}"))
report_lines.append(pad(""))
# report_lines.append(pad(
#     f"{'CURRENCY CODE':<24}"
#     f",{'OUTSTANDING BALANCE':>20}"
#     f"{' ' * 4},{'AVERAGE TENOR (DAYS)':>20}"
#     f"{' ' * 4},{'AVERAGE RATE (%)':>16}"
#     f"{' ' * 8},{'NO OF O/S ACCOUNT':>16}"
# ))
report_lines.append(pad(
    f"{'CURRENCY CODE':<24}"
    f",{'OUTSTANDING BALANCE':<24}"
    f",{'AVERAGE TENOR (DAYS)':<24}"
    f",{'AVERAGE RATE (%)':<24}"
    f",{'NO OF O/S ACCOUNT':<17}"
))

# Detail lines
for row in rows:
    curcode     = str(row["CURCODE"]).strip()
    CURBALRM      = row["CURBALRM"]
    avgtenor    = row["AVGTENOR"]
    avgrate     = row["AVGRATE"]
    noacct      = int(row["NOACCT"])

    # SAS column positions (1-based): @001, @025, @050, @075, @100
    # CURBALRM 20.2  AVGTENOR 16.  AVGRATE 16.2  NOACCT 16.
    # line = (
    #     f"{curcode:<24}"
    #     f",{CURBALRM:>20.2f}"
    #     f"{' ' * 4},{int(round(avgtenor)):>20}"
    #     f"{' ' * 4},{avgrate:>16.2f}"
    #     f"{' ' * 8},{noacct:>16}"
    # )
    line = (
        f"{curcode:<24}"
        f",{CURBALRM:>20.2f}"
        f"{' ' * 4},{avgtenor:>16.0f}"          # 16.  (0 decimals)
        f"{' ' * 8},{avgrate:>16.2f}"           # 16.2
        f"{' ' * 8},{noacct:>16}"               # 16.
    )
    report_lines.append(pad(line))

# ============================================================
# WRITE OUTPUT FILE
# ============================================================
with open(OUTPUT_FILE, "w", encoding="latin1") as f:
    for line in report_lines:
        f.write(line + "\n")

# ============================================================
# PRINT RESULTS TO TERMINAL
# ============================================================
print(f"\n[OUTPUT] {OUTPUT_FILE}\n")
for line in report_lines:
    print(line)
