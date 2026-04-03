#!/usr/bin/env python3
"""
Program  : EIBDREP2.py
Purpose  : Match EPF repayment batch records (LNTXT) against the monthly
           HP loan rolling file (REPOLN<MM>) and write a fixed-width output
           file (COMBO) for downstream processing.
"""

from __future__ import annotations
from pathlib import Path
from datetime import date, timedelta
from calendar import monthrange
import polars as pl

# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------
BASE_FLAT = Path("input_flat")      # fixed-width .txt flat file inputs
BASE_IN   = Path("input_parquet")   # SAS dataset → Parquet inputs
BASE_OUT  = Path("output_parquet")  # Parquet rolling files (LOAN library)
TXT_OUT   = Path("output_txt")      # fixed-width text file outputs

TXT_OUT.mkdir(parents=True, exist_ok=True)

LNTXT_TXT = BASE_FLAT / "LNTXT" / "LNTXT.txt"

# --------------------------------------------------------------------------
# SAS epoch + helpers
# --------------------------------------------------------------------------
SAS_EPOCH = date(1960, 1, 1)

def _sas_days(d: date) -> int:
    return (d - SAS_EPOCH).days

def _shift_month_end(d: date, k: int) -> date:
    """INTNX('MONTH', d, k, 'E'): last day of month after shifting k months."""
    y = d.year + ((d.month - 1 + k) // 12)
    m = (d.month - 1 + k) % 12 + 1
    return date(y, m, monthrange(y, m)[1])

def _num_dec(s: str, decimals: int = 0):
    """Parse a right-justified numeric string with implied decimal places."""
    s = s.strip()
    if not s or not s.lstrip("-").isdigit():
        return None
    val = int(s)
    return val / (10 ** decimals) if decimals else val

# --------------------------------------------------------------------------
# DATA REPTDATE (KEEP=REPTDATE);
#   SET LN.REPTDATE;
#   MM2 = MONTH(INTNX('MONTH',REPTDATE,-1,'E'));
#   MM3 = MONTH(INTNX('MONTH',REPTDATE,-2,'E'));
#   CALL SYMPUT('REPTYEAR', PUT(REPTDATE, YEAR2.));
#   CALL SYMPUT('REPTMON',  PUT(MONTH(REPTDATE), Z2.));
#   CALL SYMPUT('REPTDAY',  PUT(DAY(REPTDATE),   Z2.));
#   CALL SYMPUT('RDATE',    PUT(REPTDATE, Z5.));
#   CALL SYMPUT('REPTMON2', PUT(MM2, Z2.));
#   CALL SYMPUT('REPTMON3', PUT(MM3, Z2.));
# --------------------------------------------------------------------------
T_REPTDATE   = pl.read_parquet(BASE_IN / "LN" / "REPTDATE.parquet")
REPTDATE_DT  = T_REPTDATE.select(pl.col("REPTDATE").cast(pl.Date)).to_series().item()

REPTYEAR = f"{REPTDATE_DT.year  % 100:02d}"
REPTMON  = f"{REPTDATE_DT.month     :02d}"
REPTDAY  = f"{REPTDATE_DT.day       :02d}"
RDATE    = _sas_days(REPTDATE_DT)           # integer SAS date

REPTMON2 = f"{_shift_month_end(REPTDATE_DT, -1).month:02d}"
REPTMON3 = f"{_shift_month_end(REPTDATE_DT, -2).month:02d}"


# --------------------------------------------------------------------------
# DATA REPODATE;
#   INFILE LNTXT;
#   INPUT @002  ACCTNO    10.
#         @012  NOTENO     5.
#         @017  PROJDT    11.
#         @028  REPODT    11.
#         @039  NOTETYPE  $3.
#         @042  STATUS    $1.
#         @043  REMARK    $50.
#         ;
#         IF REPODT GT 0 THEN DO;
#           DD = SUBSTR(PUT(REPODT,Z11.),3,2);
#           MM = SUBSTR(PUT(REPODT,Z11.),1,2);
#           YY = SUBSTR(PUT(REPODT,Z11.),5,4);
#         END;
#         REPTDATE = MDY(MM,DD,YY);
#         REPTDATE = REPTDATE - 1;
#         IF STATUS='N' AND REMARK='LOAN ACCOUNT NOT FOUND' THEN DELETE;
#
# LNTXT is a mainframe fixed-width flat file. All @pos are 1-based.
# SAS Z11. zero-pads the integer to 11 digits: layout is MMDDYYYY___
#   MM = positions 1-2  → Python s[0:2]
#   DD = positions 3-4  → Python s[2:4]
#   YY = positions 5-8  → Python s[4:8]
# --------------------------------------------------------------------------
raw_lntxt = LNTXT_TXT.read_bytes().decode("latin-1").splitlines()

repodate_records = []
for line in raw_lntxt:
    rec = line.ljust(92)    # @043 + $50. = 92 bytes minimum

    acctno_s = rec[1:11].strip()    # @002 10.
    noteno_s = rec[11:16].strip()   # @012  5.
    projdt   = rec[16:27]           # @017 11.  (kept as-is)
    repodt_s = rec[27:38].strip()   # @028 11.
    notetype = rec[38:41]           # @039 $3.
    status   = rec[41:42]           # @042 $1.
    remark   = rec[42:92]           # @043 $50.

    # IF STATUS='N' AND REMARK='LOAN ACCOUNT NOT FOUND' THEN DELETE
    if status.strip() == "N" and remark.strip() == "LOAN ACCOUNT NOT FOUND":
        continue

    # Parse REPODT → REPTDATE
    reptdate = None
    repodt_val = int(repodt_s) if repodt_s.lstrip("-").isdigit() else 0
    if repodt_val > 0:
        s = f"{repodt_val:011d}"
        try:
            mm = int(s[0:2])
            dd = int(s[2:4])
            yy = int(s[4:8])
            reptdate = _sas_days(date(yy, mm, dd)) - 1   # REPTDATE - 1
        except (ValueError, OverflowError):
            reptdate = None

    repodate_records.append({
        "ACCTNO":   int(acctno_s) if acctno_s.isdigit() else None,
        "NOTENO":   int(noteno_s) if noteno_s.isdigit() else None,
        "PROJDT":   projdt,
        "REPODT":   repodt_val,
        "NOTETYPE": notetype,
        "STATUS":   status,
        "REMARK":   remark,
        "REPTDATE": reptdate,
    })

REPODATE = pl.DataFrame(
    repodate_records,
    schema={
        "ACCTNO":   pl.Int64,
        "NOTENO":   pl.Int64,
        "PROJDT":   pl.Utf8,
        "REPODT":   pl.Int64,
        "NOTETYPE": pl.Utf8,
        "STATUS":   pl.Utf8,
        "REMARK":   pl.Utf8,
        "REPTDATE": pl.Int64,
    },
).sort(["ACCTNO", "NOTENO", "REPTDATE"])

# DATA _NULL_: PROCESS flag
PROCESS = REPODATE.height > 0

# --------------------------------------------------------------------------
# %MACRO EXECUTE — only runs when PROCESS = Y
# --------------------------------------------------------------------------
if PROCESS:

    # PROC MEANS MIN/MAX on REPTDATE
    STDATE = REPODATE["REPTDATE"].min()
    LTDATE = REPODATE["REPTDATE"].max()

    # ------------------------------------------------------------------
    # DATA REPO;
    #   SET LOAN.REPOLN&REPTMON LOANP.REPOLN&REPTMON2 LOANQ.REPOLN&REPTMON3;
    #   WHERE &STDATE <= REPTDATE <= &LTDATE;
    #
    # LOAN/LOANP/LOANQ are the rolling monthly files produced by EIBDREPO.
    # All three live under BASE_OUT/LOAN/ in this project layout.
    # ------------------------------------------------------------------
    def _read_monthly(mm: str) -> pl.DataFrame:
        p = BASE_OUT / "LOAN" / f"REPOLN{mm}.parquet"
        return pl.read_parquet(p) if p.exists() else pl.DataFrame(schema={
            "ACCTNO": pl.Int64, "NOTENO": pl.Int64, "REPTDATE": pl.Int64,
        })

    REPO = (
        pl.concat([
            _read_monthly(REPTMON),
            _read_monthly(REPTMON2),
            _read_monthly(REPTMON3),
        ], how="vertical_relaxed")
        .with_columns(pl.col("REPTDATE").cast(pl.Int64))
        .filter(
            (pl.col("REPTDATE") >= STDATE) &
            (pl.col("REPTDATE") <= LTDATE)
        )
    )

    # ------------------------------------------------------------------
    # DATA ALL;
    #   MERGE REPO(IN=A) REPODATE(IN=B);
    #   BY ACCTNO NOTENO REPTDATE;
    #   IF A AND B;    ← inner join
    # ------------------------------------------------------------------
    ALL = REPO.join(REPODATE, on=["ACCTNO", "NOTENO", "REPTDATE"], how="inner")

    if ALL.height == 0:
        print("Merged output is empty; nothing written.")
    else:
        # Validate required columns
        need_cols = [
            "LOANTYPE", "CURBAL", "INTEARN4", "REBATE", "APPVALUE",
            "ORGBAL", "NETPROC", "ECSRRSRV", "FEETOT2", "PAYAMT",
            "TOTBILL", "TOTNPAID", "OVERINT", "PAYOFF", "BILLPAY",
        ]
        missing = [c for c in need_cols if c not in ALL.columns]
        if missing:
            raise ValueError(f"REPO missing required columns: {missing}")

        # --------------------------------------------------------------
        # Derived fields (before the ×100 multiplication)
        # IF LOANTYPE IN (380,381,700,705,993,996,128,130,983):
        #     PRINOUT = CURBAL - (INTEARN4 + REBATE)
        # IF LOANTYPE IN (720,725,131,132):
        #     PRINOUT = CURBAL
        # REBATES  = INTEARN4 + REBATE
        # TTAMTPAY = APPVALUE + (ORGBAL - NETPROC)
        # LESSDEPO = (APPVALUE - NETPROC) + (ORGBAL - CURBAL)
        # IF LOANTYPE IN (130,381,705): LESSDEPO = LESSDEPO + ECSRRSRV
        # LESSSTAT = REBATE + INTEARN4
        # ADDARRS  = FEETOT2
        # --------------------------------------------------------------
        grp1 = [380, 381, 700, 705, 993, 996, 128, 130, 983]
        grp2 = [720, 725, 131, 132]
        grp3 = [130, 381, 705]

        ALL = (
            ALL
            .with_columns([
                pl.when(pl.col("LOANTYPE").is_in(grp1))
                  .then(pl.col("CURBAL") - (pl.col("INTEARN4") + pl.col("REBATE")))
                  .when(pl.col("LOANTYPE").is_in(grp2))
                  .then(pl.col("CURBAL"))
                  .otherwise(pl.lit(None).cast(pl.Float64))
                  .alias("PRINOUT"),
                (pl.col("INTEARN4") + pl.col("REBATE")).alias("REBATES"),
                (pl.col("APPVALUE") + (pl.col("ORGBAL") - pl.col("NETPROC"))).alias("TTAMTPAY"),
                ((pl.col("APPVALUE") - pl.col("NETPROC")) + (pl.col("ORGBAL") - pl.col("CURBAL"))).alias("LESSDEPO"),
            ])
            .with_columns(
                pl.when(pl.col("LOANTYPE").is_in(grp3))
                  .then(pl.col("LESSDEPO") + pl.col("ECSRRSRV"))
                  .otherwise(pl.col("LESSDEPO"))
                  .alias("LESSDEPO")
            )
            .with_columns([
                (pl.col("REBATE") + pl.col("INTEARN4")).alias("LESSSTAT"),
                pl.col("FEETOT2").alias("ADDARRS"),
            ])
        )

        # ×100 scaling (SAS multiplies before PUT to produce integer output)
        mult_cols = [
            "CURBAL", "APPVALUE", "NETPROC", "ORGBAL", "REBATE", "INTEARN4",
            "PAYAMT", "TOTBILL", "PRINOUT", "REBATES", "TOTNPAID", "FEETOT2",
            "TTAMTPAY", "LESSDEPO", "LESSSTAT", "ADDARRS", "OVERINT", "PAYOFF",
        ]
        ALL = ALL.with_columns([
            (pl.col(c) * 100).alias(c) for c in mult_cols
        ])

        ALL = ALL.with_columns(pl.lit(0).alias("ZERO"))

        # --------------------------------------------------------------
        # FILE COMBO / PUT — fixed-width text output
        #
        # SAS PUT layout (LRECL=500, RECFM=FB):
        # @001  ZERO       1.      1 char
        # @002  ACCTNO    10.     10 chars
        # @012  NOTENO     5.      5 chars
        # @017  PROJDT    11.     11 chars
        # @028  REPODT    11.     11 chars
        # @039  NOTETYPE  $3.      3 chars
        # @042  STATUS    $1.      1 char
        # @043  REMARK    $65.    65 chars  ← output width is 65 (wider than
        #                                     the 50-char input)
        # @108  PAYOFF    15.     15 chars
        # @123  APPVALUE  15.     15 chars
        # @138  NETPROC   15.     15 chars
        # @153  ORGBAL    15.     15 chars
        # @168  REBATE    15.     15 chars
        # @183  INTEARN4  15.     15 chars
        # @198  PAYAMT    15.     15 chars
        # @213  LOANTYPE   5.      5 chars
        # @218  BILLPAY    7.      7 chars
        # @225  TOTBILL   15.     15 chars
        # @240  PRINOUT   15.     15 chars
        # @255  REBATES   15.     15 chars
        # @270  TOTNPAID  15.     15 chars
        # @285  FEETOT2   15.     15 chars
        # @300  TTAMTPAY  15.     15 chars
        # @315  LESSDEPO  15.     15 chars
        # @330  LESSSTAT  15.     15 chars
        # @345  ADDARRS   15.     15 chars
        # @360  OVERINT   15.     15 chars
        # @375  REPTDATE   6.      6 chars  ← SAS numeric date (days since 1960)
        # Total through @375+6-1 = 380 chars
        # --------------------------------------------------------------
        def _i(val, width: int) -> str:
            """Format integer value right-justified, zero-fill nulls."""
            if val is None:
                return " " * width
            return f"{int(val):>{width}}"

        def _f(val, width: int) -> str:
            """Format float as integer (already ×100), right-justified."""
            if val is None:
                return " " * width
            return f"{int(round(val)):>{width}}"

        def _s(val, width: int) -> str:
            """Format string left-justified, space-padded."""
            return str(val or "").ljust(width)[:width]

        REPTYEAR_str = f"{REPTDATE_DT.year % 100:02d}"
        REPTMON_str  = REPTMON
        REPTDAY_str  = REPTDAY
        out_name = f"OUTMIS_{REPTYEAR_str}{REPTMON_str}{REPTDAY_str}.txt"
        out_path = TXT_OUT / out_name

        with out_path.open("w", encoding="latin-1", newline="") as fout:
            for row in ALL.iter_rows(named=True):
                line = (
                    _i(row.get("ZERO"),      1)    # @001  1.
                    + _i(row.get("ACCTNO"),  10)   # @002 10.
                    + _i(row.get("NOTENO"),   5)   # @012  5.
                    + _s(row.get("PROJDT"),  11)   # @017 11.
                    + _i(row.get("REPODT"),  11)   # @028 11.
                    + _s(row.get("NOTETYPE"), 3)   # @039 $3.
                    + _s(row.get("STATUS"),   1)   # @042 $1.
                    + _s(row.get("REMARK"),  65)   # @043 $65.
                    + _f(row.get("PAYOFF"),  15)   # @108 15.
                    + _f(row.get("APPVALUE"),15)   # @123 15.
                    + _f(row.get("NETPROC"), 15)   # @138 15.
                    + _f(row.get("ORGBAL"),  15)   # @153 15.
                    + _f(row.get("REBATE"),  15)   # @168 15.
                    + _f(row.get("INTEARN4"),15)   # @183 15.
                    + _f(row.get("PAYAMT"),  15)   # @198 15.
                    + _i(row.get("LOANTYPE"), 5)   # @213  5.
                    + _i(row.get("BILLPAY"),  7)   # @218  7.
                    + _f(row.get("TOTBILL"), 15)   # @225 15.
                    + _f(row.get("PRINOUT"), 15)   # @240 15.
                    + _f(row.get("REBATES"), 15)   # @255 15.
                    + _f(row.get("TOTNPAID"),15)   # @270 15.
                    + _f(row.get("FEETOT2"), 15)   # @285 15.
                    + _f(row.get("TTAMTPAY"),15)   # @300 15.
                    + _f(row.get("LESSDEPO"),15)   # @315 15.
                    + _f(row.get("LESSSTAT"),15)   # @330 15.
                    + _f(row.get("ADDARRS"), 15)   # @345 15.
                    + _f(row.get("OVERINT"), 15)   # @360 15.
                    + _i(row.get("REPTDATE"), 6)   # @375  6. (SAS date int)
                )
                fout.write(line + "\n")

        print(f"Written: {out_path}")

else:
    print("No REPODATE records to process (PROCESS = N). Nothing written.")
