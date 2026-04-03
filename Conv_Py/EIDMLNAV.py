#!/usr/bin/env python3
# =============================================================================
# Program  : EIDMLNAV
# Purpose  : Daily accumulation of month-to-date average balance (MTDAVBAL_MIS)
#            for loans. Maintains a rolling monthly snapshot LNVG_<MM>.parquet
#            and produces a final LNVG<MM>.parquet with ACCTNO/NOTENO/MTDAVBAL_MIS.
# =============================================================================
# Conversion notes:
#   - LN.REPTDATE and LN.LNNOTE are SAS datasets → Parquet.
#   - PREVDAY = DAY(REPTDATE-1): the actual day-number of yesterday, i.e. the
#     last day of the previous month when REPTDATE is the 1st.  The converted
#     code uses (REPTDATE - timedelta(days=1)).day to handle months with 28/29/30
#     days correctly — the hardcoded fallback of 31 used previously was wrong
#     for April, June, September, November and February.
#   - SAS SUM(x, y) ignores missing values: SUM(., x) = x.  This is replicated
#     with a sas_sum() helper so that new accounts (LAST_AVG=None) and zero-
#     balance days (BAL=None) are handled correctly.
#   - The LAST_DAY=. fallback in SAS occurs AFTER the MTD computation.
#     The converted upd() function preserves this exact order.
#   - map_elements with return_dtype=pl.Struct requires the callable to return
#     a dict, not a tuple.
# =============================================================================

from __future__ import annotations
from pathlib import Path
from datetime import date, timedelta
import polars as pl

# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------
BASE_INPUT_PATH  = Path("INPUT")
BASE_OUTPUT_PATH = Path("OUTPUT")
BASE_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

LN_DIR  = BASE_INPUT_PATH  / "LN"       # SAP.PBB.MNILN.DAILY(0)
MIS_DIR = BASE_OUTPUT_PATH / "MIS"      # SAP.PBB.MLN.SASDATA
MIS_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------------
# SAS date helper
# --------------------------------------------------------------------------
_SAS_EPOCH = date(1960, 1, 1)

def sas_to_py(n: int) -> date:
    return _SAS_EPOCH + timedelta(days=int(n))

# =============================================================================
# DATA REPTDATE;
#   SET LN.REPTDATE;
#   CALL SYMPUT('PREVDAY', PUT(DAY(REPTDATE-1), Z2.));
#   CALL SYMPUT('REPTDAY', PUT(DAY(REPTDATE),   Z2.));
#   CALL SYMPUT('REPTMON', PUT(MONTH(REPTDATE), Z2.));
#   CALL SYMPUT('RDATE',   REPTDATE);
# =============================================================================
REPTDATE_val = int(
    pl.read_parquet(LN_DIR / "REPTDATE.parquet")
    .select(pl.col("REPTDATE").first())
    .item()
)

_dt      = sas_to_py(REPTDATE_val)
_dt_prev = _dt - timedelta(days=1)          # REPTDATE - 1

# SAS PUT(DAY(REPTDATE-1), Z2.) = actual day-number of yesterday.
# On the 1st of a month this is the last day of the previous month
# (28, 29, 30, or 31 depending on that month), NOT always 31.
PREVDAY      = _dt_prev.day                  # integer, e.g. 30 for July 1st
PREVDAY_Z2   = f"{PREVDAY:02d}"              # Z2. formatted string
REPTDAY      = _dt.day
REPTDAY_Z    = f"{REPTDAY:02d}"              # Z2. formatted string
REPTMON      = f"{_dt.month:02d}"
RDATE        = REPTDATE_val                  # SAS numeric date integer


# =============================================================================
# DATA TODAY_BAL;
#   SET LN.LNNOTE;
#   KEEP ACCTNO NOTENO BALANCE;
# =============================================================================
today_bal = (
    pl.read_parquet(LN_DIR / "LNNOTE.parquet")
    .select(["ACCTNO", "NOTENO", "BALANCE"])
)


# =============================================================================
# %GET_AVGBAL macro
# =============================================================================

out_month_fp = MIS_DIR / f"LNVG_{REPTMON}.parquet"   # rolling monthly snapshot

# --------------------------------------------------------------------------
# Helper: replicate SAS SUM() which ignores missing (None) values.
# SUM(., x) = x, SUM(x, .) = x, SUM(., .) = . (None)
# --------------------------------------------------------------------------
def sas_sum(*args) -> float | None:
    vals = [float(a) for a in args if a is not None]
    return sum(vals) if vals else None


if REPTDAY == 1:
    # -----------------------------------------------------------------------
    # %IF &REPTDAY EQ 1
    #   DATA MAIN_AVGBAL;
    #     SET TODAY_BAL(KEEP=ACCTNO NOTENO BALANCE);
    #     RENAME BALANCE=BAL&REPTDAY;   <- SAS RENAME applies at output;
    #     MTDAVBAL_MIS = BALANCE;            within step BALANCE is original name
    #     LAST_AVGBAL  = BALANCE;
    #     LAST_DAY     = 1;
    # SAS RENAME removes BALANCE from the output dataset; replicated by drop().
    # -----------------------------------------------------------------------
    MAIN_AVGBAL = (
        today_bal
        .with_columns([
            pl.col("BALANCE").alias(f"BAL{REPTDAY_Z}"),
            pl.col("BALANCE").alias("MTDAVBAL_MIS"),
            pl.col("BALANCE").alias("LAST_AVGBAL"),
            pl.lit(1).cast(pl.Int64).alias("LAST_DAY"),
        ])
        .drop("BALANCE")        # SAS RENAME BALANCE=BAL01 removes BALANCE
    )

else:
    # -----------------------------------------------------------------------
    # %ELSE IF &REPTDAY > 1
    #   PROC SORT DATA=MIS.LNVG_&REPTMON OUT=MAIN_AVGBAL; BY ACCTNO NOTENO;
    #   (Pre-sort omitted — Polars join does not require sorted inputs.)
    # -----------------------------------------------------------------------
    if out_month_fp.exists():
        MAIN_AVGBAL = pl.read_parquet(out_month_fp)
    else:
        # Bootstrap: snapshot file missing — treat like a fresh start
        MAIN_AVGBAL = pl.DataFrame(
            {"ACCTNO": pl.Series([], dtype=pl.Int64),
             "NOTENO": pl.Series([], dtype=pl.Int64),
             "LAST_AVGBAL": pl.Series([], dtype=pl.Float64),
             "LAST_DAY":    pl.Series([], dtype=pl.Int64)}
        )

    # PROC SORT DATA=TODAY_BAL(RENAME=(BALANCE=BAL&REPTDAY))
    #           OUT=TEMP_BAL(KEEP=ACCTNO NOTENO BAL&REPTDAY);
    TEMP_BAL = (
        today_bal
        .rename({"BALANCE": f"BAL{REPTDAY_Z}"})
        .select(["ACCTNO", "NOTENO", f"BAL{REPTDAY_Z}"])
    )

    # MERGE MAIN_AVGBAL(IN=A) TEMP_BAL(IN=B); BY ACCTNO NOTENO; IF B OR A;
    MAIN_AVGBAL = MAIN_AVGBAL.join(
        TEMP_BAL, on=["ACCTNO", "NOTENO"], how="full"
    )

    # Ensure LAST_AVGBAL / LAST_DAY columns exist (outer join may not have them
    # if MAIN_AVGBAL was empty)
    if "LAST_AVGBAL" not in MAIN_AVGBAL.columns:
        MAIN_AVGBAL = MAIN_AVGBAL.with_columns(
            pl.lit(None).cast(pl.Float64).alias("LAST_AVGBAL")
        )
    if "LAST_DAY" not in MAIN_AVGBAL.columns:
        MAIN_AVGBAL = MAIN_AVGBAL.with_columns(
            pl.lit(None).cast(pl.Int64).alias("LAST_DAY")
        )

    # ------------------------------------------------------------------
    # Row-wise update function — replicates the DATA step logic exactly,
    # preserving SAS statement order:
    #
    #   IF LAST_DAY > &PREVDAY THEN DO;            /* RERUN backout */
    #     DO UNTIL (LAST_DAY EQ &PREVDAY);
    #       CURR_BAL   = BALxx;
    #       LAST_AVGBAL= ((LAST_AVGBAL*LAST_DAY)-CURR_BAL)/SUM(LAST_DAY,-1);
    #       LAST_DAY   = SUM(LAST_DAY,-1);
    #     END;
    #   END;
    #   MTDAVBAL_MIS = SUM(LAST_AVGBAL*LAST_DAY, BAL&REPTDAY) / &REPTDAY;
    #   LAST_AVGBAL  = MTDAVBAL_MIS;
    #   IF LAST_DAY EQ . THEN LAST_DAY = &PREVDAY; /* AFTER MTD: new accounts */
    #   LAST_DAY     = SUM(LAST_DAY, 1);
    #
    # IMPORTANT ordering:
    #   - The LAST_DAY=. fallback is placed AFTER MTDAVBAL_MIS computation.
    #   - SAS SUM() treats missing as zero contribution — replicated via
    #     sas_sum() so that new accounts (LAST_AVGBAL=.) correctly yield
    #     MTDAVBAL_MIS = BAL_today / REPTDAY.
    # ------------------------------------------------------------------
    _REPTDAY_INT = REPTDAY
    _PREVDAY_INT = PREVDAY        # actual last day of previous month (correct)
    _BAL_COL     = f"BAL{REPTDAY_Z}"

    def upd(row: dict) -> dict:
        last_day: int | None  = row.get("LAST_DAY")
        last_avg: float | None = row.get("LAST_AVGBAL")

        # --- RERUN backout: only entered when LAST_DAY > PREVDAY ----------
        # In SAS, missing (.) is never > anything, so new accounts skip this.
        # Python None comparisons raise TypeError; guard with explicit None check.
        if (last_day is not None) and (last_avg is not None) \
                and (last_day > _PREVDAY_INT):
            ld = int(last_day)
            la = float(last_avg)
            while ld > _PREVDAY_INT:
                col   = f"BAL{ld:02d}"
                curr  = row.get(col)
                curr  = 0.0 if curr is None else float(curr)
                denom = ld - 1
                la    = ((la * ld) - curr) / denom if denom > 0 else None
                ld   -= 1
            last_day = ld
            last_avg = la

        # --- MTDAVBAL_MIS = SUM(LAST_AVGBAL*LAST_DAY, BAL&REPTDAY) / REPTDAY
        # SUM() ignores missing: if last_avg or last_day is None the first
        # term contributes 0; if bal_today is None it contributes 0.
        bal_today = row.get(_BAL_COL)
        term1 = (float(last_avg) * float(last_day)) \
                if (last_avg is not None and last_day is not None) else None
        mtd = sas_sum(term1, bal_today)
        if mtd is not None:
            mtd = mtd / float(_REPTDAY_INT)

        # --- LAST_AVGBAL = MTDAVBAL_MIS
        new_last_avg = mtd

        # --- IF LAST_DAY EQ . THEN LAST_DAY = &PREVDAY  (AFTER MTD)
        if last_day is None:
            last_day = _PREVDAY_INT

        # --- LAST_DAY = SUM(LAST_DAY, 1)
        new_last_day = last_day + 1

        # Return a dict — required by Polars map_elements with Struct return type
        return {
            "MTDAVBAL_MIS": mtd,
            "LAST_DAY":     new_last_day,
            "LAST_AVGBAL":  new_last_avg,
        }

    _struct_dtype = pl.Struct([
        pl.Field("MTDAVBAL_MIS", pl.Float64),
        pl.Field("LAST_DAY",     pl.Int64),
        pl.Field("LAST_AVGBAL",  pl.Float64),
    ])

    MAIN_AVGBAL = (
        MAIN_AVGBAL
        .with_columns(
            pl.struct(MAIN_AVGBAL.columns)
              .map_elements(upd, return_dtype=_struct_dtype)
              .alias("_u")
        )
        .with_columns([
            pl.col("_u").struct.field("MTDAVBAL_MIS"),
            pl.col("_u").struct.field("LAST_DAY"),
            pl.col("_u").struct.field("LAST_AVGBAL"),
        ])
        .drop("_u")
    )


# =============================================================================
# DATA MIS.LNVG_&REPTMON;   <- rolling snapshot (all BALdd, LAST_*, MTDAVBAL_MIS)
#   SET MAIN_AVGBAL;
# =============================================================================
MAIN_AVGBAL.write_parquet(out_month_fp)

# =============================================================================
# PROC SORT DATA=MIS.LNVG_&REPTMON
#   OUT=MIS.LNVG&REPTMON(KEEP=ACCTNO NOTENO MTDAVBAL_MIS);
#   BY ACCTNO NOTENO;
# =============================================================================
out_final_fp = MIS_DIR / f"LNVG{REPTMON}.parquet"

(
    MAIN_AVGBAL
    .select(["ACCTNO", "NOTENO", "MTDAVBAL_MIS"])
    .sort(["ACCTNO", "NOTENO"])
    .write_parquet(out_final_fp)
)

print(
    f"EIDMLNAV OK | REPTMON={REPTMON} REPTDAY={REPTDAY_Z} "
    f"PREVDAY={PREVDAY_Z2} | snapshot={out_month_fp.name} rows={MAIN_AVGBAL.height}"
)
