from pathlib import Path
from datetime import date
import polars as pl

# ---------- paths ----------
BASE_IN  = Path("input_parquet")      # expects: AIAIN.parquet, AIAOUT.parquet
BASE_OUT = Path("output_parquet")     # writes AIA/TRANBAIA<MM><YY>.parquet + AIAFILE_<MM><YY>.txt
BASE_OUT.mkdir(parents=True, exist_ok=True)

# ---------- 1) REPTDATE & "macro" values ----------
TODAY = date.today()
dd = TODAY.day

if dd == 8:
    SDD, WK, WK1 = 1,  "1", "4"
elif dd == 15:
    SDD, WK, WK1 = 9,  "2", "1"
elif dd == 22:
    SDD, WK, WK1 = 16, "3", "2"
else:
    SDD, WK, WK1 = 23, "4", "3"

REPTYEAR = f"{TODAY.year % 100:02d}"   # SAS YEAR2.
REPTMON  = f"{TODAY.month:02d}"        # SAS Z2.
REPTDAY  = f"{TODAY.day:02d}"          # SAS Z2.
NOWK     = WK
REPTDATE = TODAY                       # store as a true date

# ---------- 2) AIA2PBB (DROP=IND; filter IND!='FF') ----------
# SAS INFILE AIAIN FIRSTOBS=2 LRECL=150 ... -> already parsed as Parquet
# Expect: IND, NEWIC, POLICYNO, BLFREQ
AIA2PBB = (
    pl.read_parquet(BASE_IN / "AIAIN.parquet")
    .filter(pl.col("IND") != "FF")
    .drop("IND")
)

# ---------- 3) PBB2AIA (retain/forward-fill BLDATE; drop IND in ('PB','FF')) ----------
# SAS INFILE AIAOUT ... header carries BLDATE -> forward_fill here.
# Expect: IND, BLDATE, ACCTNO, MODALPREM, NEWIC, POLICYNO, BLSTAT, INSTID
PBB2AIA_raw = pl.read_parquet(BASE_IN / "AIAOUT.parquet")

PBB2AIA = (
    PBB2AIA_raw
    .with_columns(pl.col("BLDATE").forward_fill())
    .filter(~pl.col("IND").is_in(["PB", "FF"]))
    .drop("IND")
)

# ---------- 4) Sort like SAS (optional) & MERGE BY POLICYNO NEWIC; keep left (IF A) ----------
PBB2AIA = PBB2AIA.sort(["POLICYNO", "NEWIC"])
AIA2PBB = AIA2PBB.sort(["POLICYNO", "NEWIC"])

COMBINE = (
    PBB2AIA.join(AIA2PBB, on=["POLICYNO", "NEWIC"], how="left")
    .with_columns(pl.lit(REPTDATE).alias("DATE"))
)

# ---------- 5) Monthly append to AIA/TRANBAIA<MM><YY>.parquet (PROC APPEND logic) ----------
monthly_name = f"TRANBAIA{REPTMON}{REPTYEAR}.parquet"
monthly_dir  = BASE_OUT / "AIA"
monthly_dir.mkdir(parents=True, exist_ok=True)
monthly_path = monthly_dir / monthly_name

if monthly_path.exists():
    base = pl.read_parquet(monthly_path)
    # SAS: delete rows for the same DATE then append
    base = base.filter(pl.col("DATE") != REPTDATE)
    out = pl.concat([base, COMBINE], how="vertical_relaxed")
else:
    out = COMBINE

out.write_parquet(monthly_path)

# ---------- 6) TRANBAIA totals (SAS retained sums TOTAL+MODALPREM; TOTCN+CN) ----------
TRANBAIA = pl.read_parquet(monthly_path).with_columns(
    pl.lit(1).alias("CN")
)

# Grand totals across the table (SAS ends up printing overall totals)
totals = TRANBAIA.select(
    pl.col("MODALPREM").sum().alias("TOTAL"),
    pl.col("CN").sum().alias("TOTCN")
).row(0)

TOTAL_value, TOTCN_value = totals

TRANBAIA = TRANBAIA.with_columns(
    pl.lit(TOTAL_value).alias("TOTAL"),
    pl.lit(TOTCN_value).alias("TOTCN")
)

# Optional convenience: persist this enriched table
TRANBAIA_OUT = monthly_dir / f"TRANBAIA{REPTMON}{REPTYEAR}_with_totals.parquet"
TRANBAIA.write_parquet(TRANBAIA_OUT)

# ---------- 7) Fixed-width text report (SAS DATA _NULL_ FILE AIAFILE PUT ...) ----------
REPORT_PATH = monthly_dir / f"AIAFILE_{REPTMON}{REPTYEAR}.txt"

def _blank_line(width: int = 120) -> str:
    return " " * width

def _put(buf: list, pos1: int, text: str):
    """Place text at 1-based column 'pos1' into a list buffer."""
    i = pos1 - 1
    for j, ch in enumerate(text):
        need = i + j + 1 - len(buf)
        if need > 0:
            buf.extend([" "] * need)
        buf[i + j] = ch

def _fmt_left(s, width: int) -> str:
    s = "" if s is None else str(s)
    return f"{s:<{width}}"[:width]

def _fmt_right_num(val, width: int, decimals: int = 0) -> str:
    if val is None:
        return " " * width
    try:
        if decimals == 0:
            return f"{int(round(float(val))):>{width}d}"[-width:]
        return f"{float(val):>{width}.{decimals}f}"[-width:]
    except Exception:
        return " " * width

def _fmt_bldate(val) -> str:
    """SAS '6.' — treat as YYMMDD6.
       If val is a datetime.date -> format %y%m%d.
       If already a 6-digit string -> keep. Else best-effort digits right-aligned.
    """
    if val is None:
        return " " * 6
    if isinstance(val, date):
        return f"{val:%y%m%d}"
    s = str(val)
    if len(s) == 6 and s.isdigit():
        return s
    digs = "".join(ch for ch in s if ch.isdigit())[-6:]
    return digs.rjust(6)[:6] if digs else " " * 6

# Compute (or read) totals for header printing
TOTAL_for_header = TOTAL_value
TOTCN_for_header = TOTCN_value

with REPORT_PATH.open("w", encoding="utf-8", newline="\n") as f:
    # Header section (SAS: IF _N_=1 THEN DO;)
    f.write(_blank_line() + "\n")  # blank line

    buf = list(_blank_line())
    _put(buf, 1, "TOTAL PREMIUM (RM) : ")
    _put(buf, 22, _fmt_right_num(TOTAL_for_header, width=12, decimals=2))
    f.write("".join(buf).rstrip() + "\n")

    f.write(_blank_line() + "\n")  # blank line

    buf = list(_blank_line())
    _put(buf, 1, "TOTAL NUMBER OF TRANSACTION : ")
    _put(buf, 31, _fmt_right_num(TOTCN_for_header, width=10, decimals=0))
    f.write("".join(buf).rstrip() + "\n")

    f.write(_blank_line() + "\n")  # blank line

    # Column headings at fixed positions
    buf = list(_blank_line())
    _put(buf, 1,  "BLDATE")
    _put(buf, 8,  "ACCTNO")
    _put(buf, 32, "MODALPREM")
    _put(buf, 42, "NEWIC")
    _put(buf, 55, "POLICYNO")
    _put(buf, 76, "BLSTAT")
    _put(buf, 84, "INSTID")
    _put(buf, 100,"BLFREQ")
    f.write("".join(buf).rstrip() + "\n")

    # Detail lines matching SAS PUT positions/widths
    # @001 BLDATE 6.
    # @008 ACCTNO $20.
    # @029 MODALPREM 12.2
    # @042 NEWIC $12.
    # @055 POLICYNO $20.
    # @076 BLSTAT $2.
    # @084 INSTID $15.
    # @100 BLFREQ $1.
    for row in TRANBAIA.iter_rows(named=True):
        buf = list(_blank_line(120))
        _put(buf, 1,   _fmt_bldate(row.get("BLDATE")))
        _put(buf, 8,   _fmt_left(row.get("ACCTNO"),   20))
        _put(buf, 29,  _fmt_right_num(row.get("MODALPREM"), 12, 2))
        _put(buf, 42,  _fmt_left(row.get("NEWIC"),    12))
        _put(buf, 55,  _fmt_left(row.get("POLICYNO"), 20))
        _put(buf, 76,  _fmt_left(row.get("BLSTAT"),   2))
        _put(buf, 84,  _fmt_left(row.get("INSTID"),   15))
        _put(buf, 100, _fmt_left(row.get("BLFREQ"),   1))
        f.write("".join(buf).rstrip() + "\n")

print(f"[OK] Monthly parquet  : {monthly_path}")
print(f"[OK] Totals parquet   : {TRANBAIA_OUT}")
print(f"[OK] Text report      : {REPORT_PATH}")
