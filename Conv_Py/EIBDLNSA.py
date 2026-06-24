# ============================================================
# PROGRAM : EIBDLNSA.py
# PURPOSE : IBU Daily Report - Movements of Product 135, 136
#           Branch Summary on Daily O/S (RM) - BAE Personal
#           Financing-I and PLUS BAE Personal Financing-I
# ============================================================

from pathlib import Path
import pandas as pd
import polars as pl

from REPTDATE import get_reptdate_values
from input_date  import get_latest_file
from output_date import build_output_file

# ------------------------------------------------------------
# Report date
# ------------------------------------------------------------
reptdate_values = get_reptdate_values()

REPTMON  = reptdate_values.reptmon    # current month  e.g. "06"
REPTDAY  = reptdate_values.reptday    # current day    e.g. "18"
REPTYEAR = reptdate_values.reptyear   # 2-digit year   e.g. "26"
NOWK     = reptdate_values.nowk       # week number    e.g. "1"-"4"
RDATE    = reptdate_values.reptdate.strftime("%d/%m/%y")  # DDMMYY8.

# Previous month (MM1 = MM - 1; if MM1 = 0 then MM1 = 12)
# NOTE: NOWK is hardcoded to '4' in the SAS source (NOWK='4')
NOWK = "4"

# ------------------------------------------------------------
# Path configuration
# ------------------------------------------------------------
BASE_DIR   = Path(r"C:\Users\aiman\Desktop\SAS_Python_Migration")
MIS_DIR    = BASE_DIR / "MIS"          # .sas7bdat loan files  (prefix: ln)
BRANCH_DIR = BASE_DIR / "BRANCHF"      # flat file  (no date prefix)
OUTPUT_DIR = BASE_DIR / "OUTPUT" / "EIBDLNSA"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input files
LOAN_FILE   = get_latest_file(MIS_DIR,   prefix="ln")   # MIS.LOAN&REPTMON&NOWK
PREVLN_FILE = get_latest_file(MIS_DIR,   prefix="ln")   # MIS.LOAN&REPTMON1&NOWK
#   NOTE: Both loan references resolve to the two most-recent 'ln' files.
#         get_latest_file returns the single latest; for the previous-month
#         file a second call is made below after excluding the current file.
BRANCH_FILE = BRANCH_DIR / "branch.txt"                 # BRANCHF flat file

# Resolve current (latest) and previous (second-latest) loan files
_all_ln = sorted(
    [
        f for f in MIS_DIR.iterdir()
        if f.is_file() and f.name.startswith("ln") and f.suffix.lower() == ".sas7bdat"
    ],
    key=lambda f: f.name,
    reverse=True,
)
LOAN_FILE   = _all_ln[0] if len(_all_ln) > 0 else None
PREVLN_FILE = _all_ln[1] if len(_all_ln) > 1 else None

# Output file  (prefix only; build_output_file appends _DDMMYY)
OUTPUT_FILE = build_output_file(OUTPUT_DIR, prefix="EIBDLNSA").with_suffix(".txt")

# ------------------------------------------------------------
# Constants
# ------------------------------------------------------------
TARGET_PRODUCTS = [135, 136]
PAGE_LINES      = 60
LINE_WIDTH      = 132

# ASA carriage control characters
ASA_FF = "1"   # form-feed  (new page)
ASA_SP = " "   # single space (normal line)
ASA_DS = "0"   # double space

# Report titles
TITLE1 = "REPORT ID : EIBDLNSA"
TITLE2 = "PUBLIC BANK BERHAD"
TITLE3 = "BRANCH SUMM ON DAILY O/S (RM) - BAE PERSONAL FINANCING-I"
TITLE4 = "BRANCH SUMM ON DAILY O/S (RM) - PLUS BAE PERSONAL FINANCING-I"

# ------------------------------------------------------------
# Column layout  (mirrors PROC TABULATE / BOX='BRANCH' RTS=21)
#
#   ASA(1) | BRANCH(21) | per product: CURR TOT(7) PREV TOT(7)
#                                       CURR AMOUNT(15) PREV AMOUNT(15)
#   separator spaces between columns = 1
# ------------------------------------------------------------
W_RTS      = 21    # RTS=21  (BOX='BRANCH')
W_INT      =  7    # COMMA7.
W_AMT      = 15    # COMMA15.2
COL_SEP    =  1    # 1 space between each column

PRODUCTS_SORTED = sorted(TARGET_PRODUCTS)

# Per-product block width: CURR TOT + PREV TOT + CURR AMT + PREV AMT + separators
_PER_PROD_W = (W_INT + COL_SEP) * 2 + (W_AMT + COL_SEP) * 2   # = 48

# ------------------------------------------------------------
# Formatters
# ------------------------------------------------------------
def _fmt_comma7(val) -> str:
    try:
        return f"{int(val):>{W_INT},}"
    except (TypeError, ValueError):
        return " " * W_INT


def _fmt_comma15_2(val) -> str:
    try:
        return f"{float(val):>{W_AMT},.2f}"
    except (TypeError, ValueError):
        return " " * W_AMT


# ------------------------------------------------------------
# Report header builder
# ------------------------------------------------------------
def _build_header_lines(first_page: bool = False) -> list[str]:
    lines: list[str] = []
    cc = ASA_FF if first_page else ASA_FF   # always form-feed on new page

    lines.append(cc      + TITLE1.center(LINE_WIDTH))
    lines.append(ASA_SP  + TITLE2.center(LINE_WIDTH))
    lines.append(ASA_SP  + TITLE3.center(LINE_WIDTH))
    lines.append(ASA_SP  + TITLE4.center(LINE_WIDTH))
    lines.append(ASA_SP  + f"AS AT {RDATE}".center(LINE_WIDTH))
    lines.append(ASA_SP  + "")

    # ---- Column header row 1: product number labels ----
    hdr1 = " " * W_RTS
    for p in PRODUCTS_SORTED:
        block = f"{'PRODUCT ' + str(p):^{_PER_PROD_W}}"
        hdr1 += block
    lines.append(ASA_SP + hdr1)

    # ---- Column header row 2: measure labels ----
    hdr2 = f"{'BRANCH':<{W_RTS}}"
    for _ in PRODUCTS_SORTED:
        hdr2 += (
            f"{'CURR TOT':>{W_INT}}{' ' * COL_SEP}"
            f"{'PREV TOT':>{W_INT}}{' ' * COL_SEP}"
            f"{'CURR AMOUNT':>{W_AMT}}{' ' * COL_SEP}"
            f"{'PREV AMOUNT':>{W_AMT}}{' ' * COL_SEP}"
        )
    lines.append(ASA_SP + hdr2)

    # ---- Separator line ----
    sep = "-" * W_RTS
    for _ in PRODUCTS_SORTED:
        sep += "-" * _PER_PROD_W
    lines.append(ASA_SP + sep)

    return lines


# ------------------------------------------------------------
# Detail line builder
# ------------------------------------------------------------
def _build_detail_line(label: str, row_map: dict, asa: str = ASA_SP) -> str:
    """
    row_map: { product_int -> { NOACCT, PNOACCT, BRLNAMT, PBRLNAMT } }
    """
    body = f"{label:<{W_RTS}}"
    for p in PRODUCTS_SORTED:
        d = row_map.get(p, {})
        body += (
            f"{_fmt_comma7(d.get('NOACCT',  0))}{' ' * COL_SEP}"
            f"{_fmt_comma7(d.get('PNOACCT', 0))}{' ' * COL_SEP}"
            f"{_fmt_comma15_2(d.get('BRLNAMT',  0.0))}{' ' * COL_SEP}"
            f"{_fmt_comma15_2(d.get('PBRLNAMT', 0.0))}{' ' * COL_SEP}"
        )
    return asa + body


# ------------------------------------------------------------
# Step 1 : Read & summarise current-month loan file
# ------------------------------------------------------------
print(f"[INFO] Reading current loan file  : {LOAN_FILE}")
loan_df = pl.from_pandas(pd.read_sas(str(LOAN_FILE), encoding="latin1"))
loan_df = loan_df.filter(pl.col("PRODUCT").is_in(TARGET_PRODUCTS))

loan_summ = (
    loan_df
    .group_by(["BRANCH", "PRODUCT"])
    .agg([
        pl.col("BALANCE").sum().alias("BRLNAMT"),
        pl.len().alias("NOACCT"),
    ])
)

# ------------------------------------------------------------
# Step 2 : Read & summarise previous-month loan file
# ------------------------------------------------------------
print(f"[INFO] Reading previous loan file : {PREVLN_FILE}")
prevln_df = pl.from_pandas(pd.read_sas(str(PREVLN_FILE), encoding="latin1"))
prevln_df = prevln_df.filter(pl.col("PRODUCT").is_in(TARGET_PRODUCTS))

prevln_summ = (
    prevln_df
    .group_by(["BRANCH", "PRODUCT"])
    .agg([
        pl.col("BALANCE").sum().alias("PBRLNAMT"),
        pl.len().alias("PNOACCT"),
    ])
)

# ------------------------------------------------------------
# Step 3 : Merge LOAN + PREVLN  BY BRANCH PRODUCT
# ------------------------------------------------------------
loan_merged = (
    loan_summ
    .join(prevln_summ, on=["BRANCH", "PRODUCT"], how="full", coalesce=True)
    .fill_null(0)
    .sort(["BRANCH", "PRODUCT"])
)

# ------------------------------------------------------------
# Step 4 : Read BRANCH flat file
#   INPUT @001 BANK $1.  @002 BRANCH 3.  @006 ABBREV $3.
#         @012 BRCHNAME $30.
# ------------------------------------------------------------
print(f"[INFO] Reading branch file        : {BRANCH_FILE}")
branch_records: list[dict] = []
with open(BRANCH_FILE, "r", encoding="latin1") as fh:
    for raw in fh:
        line = raw.rstrip("\n").ljust(41)
        bank      = line[0:1]        # @001  $1.
        branch_s  = line[1:4]        # @002   3.
        abbrev    = line[5:8]        # @006  $3.
        brchname  = line[11:41]      # @012  $30.
        try:
            branch_int = int(branch_s.strip())
        except ValueError:
            continue
        branch_records.append({
            "BRANCH":   branch_int,
            "BANK":     bank.strip(),
            "ABBREV":   abbrev.strip(),
            "BRCHNAME": brchname.strip(),
        })

branch_df = pl.DataFrame(branch_records).with_columns(
    pl.col("BRANCH").cast(pl.Int64)
)

# ------------------------------------------------------------
# Step 5 : Merge LOAN(IN=A) + BRANCH  BY BRANCH
#          IF A  ->  left join on loan_merged
#          VARIANLN = BRLNAMT - PBRLNAMT
# ------------------------------------------------------------
loans_df = (
    loan_merged
    .join(branch_df, on="BRANCH", how="left")
    .with_columns(
        (pl.col("BRLNAMT") - pl.col("PBRLNAMT")).alias("VARIANLN")
    )
    .sort(["BRANCH", "PRODUCT"])
)

# ------------------------------------------------------------
# Step 6 : Pivot into report structure
#          { branch_id -> { product -> { metrics } } }
# ------------------------------------------------------------
branch_label_map: dict[int, str] = {
    row["BRANCH"]: f"{row['BRANCH']:03d} {row['BRCHNAME']}"
    for row in branch_df.iter_rows(named=True)
}

branch_data: dict[int, dict[int, dict]] = {}
grand:       dict[int, dict]            = {
    p: {"NOACCT": 0, "PNOACCT": 0, "BRLNAMT": 0.0, "PBRLNAMT": 0.0}
    for p in PRODUCTS_SORTED
}

for row in loans_df.iter_rows(named=True):
    br = int(row["BRANCH"])
    pr = int(row["PRODUCT"])
    if br not in branch_data:
        branch_data[br] = {}
    branch_data[br][pr] = {
        "NOACCT":  row["NOACCT"],
        "PNOACCT": row["PNOACCT"],
        "BRLNAMT": row["BRLNAMT"],
        "PBRLNAMT":row["PBRLNAMT"],
    }
    if pr in grand:
        grand[pr]["NOACCT"]   += row["NOACCT"]
        grand[pr]["PNOACCT"]  += row["PNOACCT"]
        grand[pr]["BRLNAMT"]  += row["BRLNAMT"]
        grand[pr]["PBRLNAMT"] += row["PBRLNAMT"]

# ------------------------------------------------------------
# Step 7 : Write report
# ------------------------------------------------------------
out_lines:      list[str] = []
page_line_count: int      = 0


def _emit_header(first_page: bool = False) -> int:
    hdrs = _build_header_lines(first_page=first_page)
    out_lines.extend(hdrs)
    return len(hdrs)


def _check_page_break() -> None:
    global page_line_count
    if page_line_count >= PAGE_LINES:
        page_line_count = _emit_header(first_page=False)


# First page
page_line_count = _emit_header(first_page=True)

for branch_id in sorted(branch_data.keys()):
    _check_page_break()
    label  = branch_label_map.get(branch_id, f"{branch_id:03d}")
    out_lines.append(_build_detail_line(label, branch_data[branch_id]))
    page_line_count += 1

# Grand Total (double-space before, then the total row)
_check_page_break()
out_lines.append(ASA_DS + "")
page_line_count += 1

_check_page_break()
out_lines.append(_build_detail_line("GRAND TOTAL", grand))
page_line_count += 1

# ------------------------------------------------------------
# Step 8 : Persist to file
# ------------------------------------------------------------
with open(OUTPUT_FILE, "w", encoding="latin1") as fh:
    for line in out_lines:
        fh.write(line + "\n")

print(f"\n[OUTPUT] Report written to : {OUTPUT_FILE}")
print(f"[OUTPUT] Total lines       : {len(out_lines)}")

# Terminal preview
print("\n" + "=" * (LINE_WIDTH + 1))
for line in out_lines:
    print(line)
print("=" * (LINE_WIDTH + 1))
