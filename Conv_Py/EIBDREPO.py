#!/usr/bin/env python3
"""
Program  : EIBDREPO.py
Purpose  : Daily accumulation of HP loan repayment data into monthly rolling
           Parquet dataset (LOAN/REPOLN<MM>.parquet).
           Reads ACCTFILE (packed-decimal flat file) and OITXT (fixed-width
           flat file), merges them, and appends to the monthly rolling file.
"""

from pathlib import Path
from datetime import date, timedelta
import polars as pl

# PBBLNFMT import — provides HP_ALL product list (equivalent to SAS &HP macro)
from PBBLNFMT import HP_ALL

# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------
BASE_FLAT = Path("input_flat")      # binary / fixed-width flat file inputs
BASE_IN   = Path("input_parquet")   # SAS dataset → Parquet inputs
BASE_OUT  = Path("output_parquet")  # Parquet output root

ACCTFILE_BIN = BASE_FLAT / "ACCTFILE" / "ACCTFILE.dat"
OITXT_TXT    = BASE_FLAT / "OITXT"   / "OITXT.txt"

(BASE_OUT / "LOAN").mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------------
# SAS epoch
# --------------------------------------------------------------------------
SAS_EPOCH = date(1960, 1, 1)

def _sas_days(d: date) -> int:
    return (d - SAS_EPOCH).days

# --------------------------------------------------------------------------
# DATA REPTDATE (KEEP=REPTDATE);
#   SET LN.REPTDATE;
#   CALL SYMPUT('REPTYEAR', PUT(REPTDATE, YEAR2.));
#   CALL SYMPUT('REPTMON',  PUT(MONTH(REPTDATE), Z2.));
#   CALL SYMPUT('REPTDAY',  PUT(DAY(REPTDATE),   Z2.));
#   CALL SYMPUT('RDATE',    PUT(REPTDATE, Z5.));
# --------------------------------------------------------------------------
T_REPTDATE = pl.read_parquet(BASE_IN / "LN" / "REPTDATE.parquet")
REPTDATE   = T_REPTDATE.select(pl.col("REPTDATE").cast(pl.Date)).to_series().item()

REPTYEAR = f"{REPTDATE.year  % 100:02d}"
REPTMON  = f"{REPTDATE.month     :02d}"
REPTDAY  = f"{REPTDATE.day       :02d}"
RDATE    = f"{_sas_days(REPTDATE):05d}"   # SAS PUT(date, Z5.)


# --------------------------------------------------------------------------
# Packed-decimal (PD) decoder
#
# SAS PD informat: n bytes encode n*2 digits (last nibble = sign: C/F=+, D=-).
# For PD8.2 the value has 2 implied decimal places (divide by 100).
# --------------------------------------------------------------------------
def _decode_pd(data: bytes, length: int, decimals: int = 0):
    """
    Decode a SAS packed-decimal field.
    length   : number of bytes to read
    decimals : implied decimal places (e.g. PD8.2 → decimals=2)
    Returns  : float or None if all bytes are zero
    """
    chunk = data[:length]
    if all(b == 0 for b in chunk):
        return None
    digits = ""
    for i, byte in enumerate(chunk):
        hi = (byte >> 4) & 0x0F
        lo = byte & 0x0F
        if i < length - 1:
            digits += str(hi) + str(lo)
        else:
            digits += str(hi)
            sign_nibble = lo
    negative = sign_nibble == 0xD
    value = int(digits)
    if decimals:
        value = value / (10 ** decimals)
    return -value if negative else value


# --------------------------------------------------------------------------
# DATA LNFILE;
#   INFILE ACCTFILE;
#   INPUT @001  ACCTNO     PD6.
#         @081  NOTENO     PD3.
#         @085  LOANTYPE   PD2.
#         @121  CURBAL     PD8.2
#         @145  APPVALUE   PD8.2
#         @262  ORGBAL     PD8.2
#         @270  NETPROC    PD8.2
#         @278  TOTFEE     PD8.2
#         @303  REBATEX    PD8.2
#         @416  INTEARN4X  PD8.2
#         @424  PAYAMT     PD8.2
#         @441  FEETOTAL   PD8.2
#         @449  FEETOT2    PD8.2
#         @1116 ECSRRSRV   PD8.2
#         @1194 OVERFEE    PD8.2
#         ;
#         REPTDATE = &RDATE;
#         IF LOANTYPE IN &HP;   ← HP_ALL from PBBLNFMT
#
# All @pos values are 1-based; Python slice = [pos-1 : pos-1+len].
# PD informat lengths: PD6.=6 bytes, PD3.=3 bytes, PD2.=2 bytes, PD8.2=8 bytes.
# --------------------------------------------------------------------------
# Determine record length from JCL DCB (not stated explicitly; derive from
# last field: @1194 OVERFEE PD8.2 → byte 1194+8-1=1201 → LRECL≥1201).
ACCTFILE_LRECL = 1201

raw_acct = ACCTFILE_BIN.read_bytes()
n_records = len(raw_acct) // ACCTFILE_LRECL

lnfile_records = []
for i in range(n_records):
    rec = raw_acct[i * ACCTFILE_LRECL : (i + 1) * ACCTFILE_LRECL]

    loantype = _decode_pd(rec[84:86],   2, 0)   # @085 PD2.
    if loantype not in HP_ALL:
        continue

    lnfile_records.append({
        "ACCTNO":    _decode_pd(rec[0:6],       6, 0),    # @001 PD6.
        "NOTENO":    _decode_pd(rec[80:83],      3, 0),    # @081 PD3.
        "LOANTYPE":  loantype,                              # @085 PD2.
        "CURBAL":    _decode_pd(rec[120:128],    8, 2),    # @121 PD8.2
        "APPVALUE":  _decode_pd(rec[144:152],    8, 2),    # @145 PD8.2
        "ORGBAL":    _decode_pd(rec[261:269],    8, 2),    # @262 PD8.2
        "NETPROC":   _decode_pd(rec[269:277],    8, 2),    # @270 PD8.2
        "TOTFEE":    _decode_pd(rec[277:285],    8, 2),    # @278 PD8.2
        "REBATEX":   _decode_pd(rec[302:310],    8, 2),    # @303 PD8.2
        "INTEARN4X": _decode_pd(rec[415:423],    8, 2),    # @416 PD8.2
        "PAYAMT":    _decode_pd(rec[423:431],    8, 2),    # @424 PD8.2
        "FEETOTAL":  _decode_pd(rec[440:448],    8, 2),    # @441 PD8.2
        "FEETOT2":   _decode_pd(rec[448:456],    8, 2),    # @449 PD8.2
        "ECSRRSRV":  _decode_pd(rec[1115:1123],  8, 2),   # @1116 PD8.2
        "OVERFEE":   _decode_pd(rec[1193:1201],  8, 2),   # @1194 PD8.2
        "REPTDATE":  int(RDATE),
    })

LNFILE = pl.DataFrame(
    lnfile_records,
    schema={
        "ACCTNO":    pl.Int64,
        "NOTENO":    pl.Int64,
        "LOANTYPE":  pl.Int64,
        "CURBAL":    pl.Float64,
        "APPVALUE":  pl.Float64,
        "ORGBAL":    pl.Float64,
        "NETPROC":   pl.Float64,
        "TOTFEE":    pl.Float64,
        "REBATEX":   pl.Float64,
        "INTEARN4X": pl.Float64,
        "PAYAMT":    pl.Float64,
        "FEETOTAL":  pl.Float64,
        "FEETOT2":   pl.Float64,
        "ECSRRSRV":  pl.Float64,
        "OVERFEE":   pl.Float64,
        "REPTDATE":  pl.Int64,
    },
)


# --------------------------------------------------------------------------
# DATA OI (DROP=YY MM DD);
#   INFILE OITXT;
#   INPUT @002  ACCTNO    11.
#         @013  NOTENO     5.
#         @019  YY        $4.
#         @023  MM        $2.
#         @025  DD        $2.
#         @037  OVERINT   15.2
#         @054  PAYOFF    15.2
#         @071  TOTNPAID  15.2
#         @088  TOTBILL   15.2
#         @105  BILLPAY    7.
#         @105  BILLCNT    7.   ← same byte position as BILLPAY
#         @112  BILLSIGN  $1.
#         @114  REBATE    15.2
#         @131  INTEARN4  15.2
#         ;
#         IF BILLSIGN = '-' THEN BILLPAY = 0;
#         REPTDATE = MDY(MM,DD,YY);
#
# OITXT is a mainframe fixed-width flat file. All @pos are 1-based.
# Numeric informats with .2 mean divide the integer value by 100.
# --------------------------------------------------------------------------
raw_oi_lines = OITXT_TXT.read_bytes().decode("latin-1").splitlines()

def _num_dec(s: str, decimals: int = 0):
    """Parse a right-justified numeric field with implied decimals."""
    s = s.strip()
    if not s or not s.lstrip("-").isdigit():
        return None
    val = int(s)
    return val / (10 ** decimals) if decimals else val

oi_records = []
for line in raw_oi_lines:
    rec = line.ljust(145)

    yy_s = rec[18:22].strip()   # @019 $4.
    mm_s = rec[22:24].strip()   # @023 $2.
    dd_s = rec[24:26].strip()   # @025 $2.

    try:
        reptdate_dt = date(int(yy_s), int(mm_s), int(dd_s))
        reptdate    = _sas_days(reptdate_dt)
    except (ValueError, TypeError):
        reptdate = None

    billsign = rec[111:112]                         # @112 $1.
    billpay_raw = _num_dec(rec[104:111])            # @105  7.
    billpay = 0 if billsign == "-" else billpay_raw
    billcnt = _num_dec(rec[104:111])                # @105  7. (same position)

    oi_records.append({
        "ACCTNO":   int(rec[1:12].strip())  if rec[1:12].strip().isdigit() else None,   # @002 11.
        "NOTENO":   _num_dec(rec[12:17]),           # @013  5.
        "OVERINT":  _num_dec(rec[36:51],  2),       # @037 15.2
        "PAYOFF":   _num_dec(rec[53:68],  2),       # @054 15.2
        "TOTNPAID": _num_dec(rec[70:85],  2),       # @071 15.2
        "TOTBILL":  _num_dec(rec[87:102], 2),       # @088 15.2
        "BILLPAY":  billpay,                         # @105  7. (zeroed if '-')
        "BILLCNT":  billcnt,                         # @105  7. (same bytes)
        "BILLSIGN": billsign,                        # @112  $1.
        "REBATE":   _num_dec(rec[113:128], 2),      # @114 15.2
        "INTEARN4": _num_dec(rec[130:145], 2),      # @131 15.2
        "REPTDATE": reptdate,
    })

OI = pl.DataFrame(
    oi_records,
    schema={
        "ACCTNO":   pl.Int64,
        "NOTENO":   pl.Int64,
        "OVERINT":  pl.Float64,
        "PAYOFF":   pl.Float64,
        "TOTNPAID": pl.Float64,
        "TOTBILL":  pl.Float64,
        "BILLPAY":  pl.Float64,
        "BILLCNT":  pl.Float64,
        "BILLSIGN": pl.Utf8,
        "REBATE":   pl.Float64,
        "INTEARN4": pl.Float64,
        "REPTDATE": pl.Int64,
    },
)

# --------------------------------------------------------------------------
# DATA LNFILE;
#   MERGE LNFILE(IN=A) OI(IN=B);
#   BY ACCTNO NOTENO REPTDATE;
#   IF A;                        ← keep all LNFILE rows; OI enriches them
#
# Pre-sort omitted — Polars left-join does not require sorted inputs.
# --------------------------------------------------------------------------
LNFILE = LNFILE.join(OI, on=["ACCTNO", "NOTENO", "REPTDATE"], how="left")


# --------------------------------------------------------------------------
# %MACRO ACCUM
# IF "&REPTDAY" EQ "01":
#   DATA LOAN.REPOLN&REPTMON; SET LNFILE;
# ELSE:
#   Remove today's RDATE from existing monthly file, then append LNFILE.
# --------------------------------------------------------------------------
monthly_path = BASE_OUT / "LOAN" / f"REPOLN{REPTMON}.parquet"

if REPTDAY == "01":
    LNFILE.write_parquet(monthly_path)
else:
    if monthly_path.exists():
        EXIST = pl.read_parquet(monthly_path)
        EXIST = EXIST.filter(pl.col("REPTDATE") != int(RDATE))
        OUT   = pl.concat([EXIST, LNFILE], how="vertical_relaxed")
    else:
        OUT = LNFILE
    OUT.write_parquet(monthly_path)

print(f"Written: {monthly_path}")
