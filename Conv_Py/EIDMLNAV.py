from __future__ import annotations
import polars as pl
from pathlib import Path
from datetime import date

# ----------------------------
# BASE PATHS (adjust as needed)
# ----------------------------
BASE_INPUT_PATH  = Path("INPUT")
BASE_OUTPUT_PATH = Path("OUTPUT")
BASE_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# SAS libs → folders
LN_DIR  = BASE_INPUT_PATH  / "LN"       # SAP.PBB.MNILN.DAILY(0)
MIS_DIR = BASE_OUTPUT_PATH / "MIS"      # SAP.PBB.MLN.SASDATA
MIS_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------
# SAS date helpers (days since 1960)
# --------------------------------
SAS_EPOCH = date(1960, 1, 1)
def sas_to_py(n: int) -> date:
    return SAS_EPOCH.fromordinal(SAS_EPOCH.toordinal() + int(n))

# ==============================================================
# DATA REPTDATE;  macros: PREVDAY, REPTDAY, REPTMON, RDATE (numeric)
# ==============================================================
rept_fp = LN_DIR / "REPTDATE.parquet"
REPTDATE_val = int(pl.read_parquet(rept_fp).select(pl.col("REPTDATE").first()).item())
_dt = sas_to_py(REPTDATE_val)
PREVDAY  = int(f"{(_dt.day-1) if _dt.day>1 else 31:02d}")  # only used as number; Z2. visual not needed
REPTDAY  = _dt.day
REPTDAY_Z = f"{REPTDAY:02d}"
REPTMON  = f"{_dt.month:02d}"
RDATE    = REPTDATE_val  # SAS numeric date

# ==============================================================
# DATA TODAY_BAL;  SET LN.LNNOTE (KEEP ACCTNO NOTENO BALANCE)
# ==============================================================
today_bal = pl.read_parquet(LN_DIR / "LNNOTE.parquet").select(["ACCTNO","NOTENO","BALANCE"])

# ==============================================================
# %GET_AVGBAL macro
#   If REPTDAY==1:
#       MAIN_AVGBAL = TODAY_BAL with BAL01, MTDAVBAL_MIS= BALANCE,
#       LAST_AVGBAL=BALANCE, LAST_DAY=1
#   Else:
#       MAIN_AVGBAL <- MIS.LNVG_MM (prior snapshot)
#       TEMP_BAL with BALdd
#       FULL OUTER MERGE by keys (A or B)
#       If RERUN (LAST_DAY>PREVDAY): back out extra days iteratively
#       MTDAVBAL_MIS = (LAST_AVGBAL*LAST_DAY + BALdd) / REPTDAY
#       LAST_AVGBAL = MTDAVBAL_MIS; LAST_DAY = (LAST_DAY or PREVDAY) + 1
# ==============================================================

out_month_fp = MIS_DIR / f"LNVG_{REPTMON}.parquet"   # snapshot with BAL01.., LAST_AVGBAL, LAST_DAY, etc.

if REPTDAY == 1:
    MAIN_AVGBAL = (
        today_bal
        .with_columns([
            pl.col("BALANCE").alias(f"BAL{REPTDAY_Z}"),
            pl.col("BALANCE").alias("MTDAVBAL_MIS"),
            pl.col("BALANCE").alias("LAST_AVGBAL"),
            pl.lit(1).alias("LAST_DAY"),
        ])
    )
else:
    # Load prior month snapshot (if missing, behave like day-1 bootstrap)
    if out_month_fp.exists():
        MAIN_AVGBAL = pl.read_parquet(out_month_fp)
    else:
        MAIN_AVGBAL = pl.DataFrame({"ACCTNO":[], "NOTENO":[], "LAST_AVGBAL":[],"LAST_DAY":[]})

    # TEMP_BAL with today's BALdd
    TEMP_BAL = today_bal.rename({"BALANCE": f"BAL{REPTDAY_Z}"}).select(["ACCTNO","NOTENO",f"BAL{REPTDAY_Z}"])

    # FULL OUTER MERGE by keys (IF B OR A)
    MAIN_AVGBAL = MAIN_AVGBAL.join(TEMP_BAL, on=["ACCTNO","NOTENO"], how="outer")

    # Collect all existing BAL** columns for rerun calculation
    bal_cols = [c for c in MAIN_AVGBAL.columns if c.startswith("BAL") and len(c)==5]  # e.g., BAL01
    # Ensure LAST_AVGBAL / LAST_DAY exist
    if "LAST_AVGBAL" not in MAIN_AVGBAL.columns: MAIN_AVGBAL = MAIN_AVGBAL.with_columns(pl.lit(None).alias("LAST_AVGBAL"))
    if "LAST_DAY"    not in MAIN_AVGBAL.columns: MAIN_AVGBAL = MAIN_AVGBAL.with_columns(pl.lit(None).alias("LAST_DAY"))

    PREVDAY_INT = int(f"{_dt.day-1:02d}") if _dt.day>1 else 31
    REPTDAY_INT = REPTDAY

    # Row-wise adjust (rerun back-out + new average)
    def upd(row: dict) -> tuple[float|None, int|None, float|None]:
        last_day = row.get("LAST_DAY", None)
        last_avg = row.get("LAST_AVGBAL", None)
        # if last_day is missing, set to PREVDAY (SAS: if LAST_DAY EQ . then LAST_DAY=&PREVDAY)
        if last_day is None:
            last_day = PREVDAY_INT
        # RERUN backout: while last_day > PREVDAY, remove that day's BALxx from average
        if (last_avg is not None) and (last_day is not None) and (last_day > PREVDAY_INT):
            ld = int(last_day)
            la = float(last_avg)
            while ld > PREVDAY_INT:
                col = f"BAL{ld:02d}"
                curr = row.get(col, None)
                curr = 0.0 if (curr is None) else float(curr)
                if ld-1 > 0:
                    la = ((la * ld) - curr) / (ld - 1)
                else:
                    la = None
                ld -= 1
            last_day, last_avg = ld, la
        # compute today
        bal_today = row.get(f"BAL{REPTDAY_INT:02d}", None)
        if (last_avg is None) or (last_day is None) or (bal_today is None):
            mtd = None
        else:
            mtd = (last_avg * float(last_day) + float(bal_today)) / float(REPTDAY_INT)
        # update LASTs
        new_last_avg = mtd
        new_last_day = (last_day if last_day is not None else PREVDAY_INT) + 1
        return (mtd, new_last_day, new_last_avg)

    MAIN_AVGBAL = MAIN_AVGBAL.with_columns(
        pl.struct(MAIN_AVGBAL.columns).map_elements(upd, return_dtype=pl.Struct(
            [pl.Field("MTDAVBAL_MIS", pl.Float64), pl.Field("LAST_DAY", pl.Int64), pl.Field("LAST_AVGBAL", pl.Float64)]
        )).alias("_u")
    ).with_columns([
        pl.col("_u").struct.field("MTDAVBAL_MIS"),
        pl.col("_u").struct.field("LAST_DAY"),
        pl.col("_u").struct.field("LAST_AVGBAL"),
    ]).drop("_u")

# ==============================================================
# Save snapshot: MIS.LNVG_&REPTMON (all columns incl. BALdd, LAST_*)
# Then produce MIS.LNVG&REPTMON (ACCTNO,NOTENO,MTDAVBAL_MIS) sorted
# ==============================================================
MAIN_AVGBAL.write_parquet(out_month_fp)

out_final_fp = MIS_DIR / f"LNVG{REPTMON}.parquet"
(
    MAIN_AVGBAL
    .select(["ACCTNO","NOTENO","MTDAVBAL_MIS"])
    .sort(["ACCTNO","NOTENO"])
    .write_parquet(out_final_fp)
)

# --- short summary ---
print(f"EIDMLNAV OK | REPTMON={REPTMON} REPTDAY={REPTDAY_Z} | snapshot=LNVG_{REPTMON} rows={MAIN_AVGBAL.height}")
