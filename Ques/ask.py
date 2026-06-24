# ------------------------------------------------------------
# Report date
# ------------------------------------------------------------
reptdate_values = get_reptdate_values()

REPTMON  = reptdate_values.reptmon
REPTDAY  = reptdate_values.reptday
REPTYEAR = reptdate_values.reptyear
RDATE    = reptdate_values.reptdate.strftime("%d/%m/%y")

# Current month = reptdate month (MM), previous month = MM-1
# Year rolls back when current month is January (01 -> 12 of prior year)
_mm_int  = int(REPTMON)
_yy_int  = int(REPTYEAR)

_curr_mm = _mm_int
_curr_yy = _yy_int

_prev_mm = _mm_int - 1 if _mm_int > 1 else 12
_prev_yy = _yy_int if _mm_int > 1 else _yy_int - 1

CURR_LOAN_NAME = f"ln{_curr_mm:02d}4{_curr_yy:02d}.sas7bdat"
PREV_LOAN_NAME = f"ln{_prev_mm:02d}4{_prev_yy:02d}.sas7bdat"

# ------------------------------------------------------------
# Path configuration
# ------------------------------------------------------------
BASE_DIR   = Path("/dwh")
MIS_DIR    = BASE_DIR / "ln_ln"
BRANCH_DIR = Path("/sasdata/rawdata/lookup")
OUTPUT_DIR = BASE_DIR / "OUTPUT" / "EIBDLNSA"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LOAN_FILE   = MIS_DIR / CURR_LOAN_NAME
PREVLN_FILE = MIS_DIR / PREV_LOAN_NAME

OUTPUT_FILE = build_output_file(OUTPUT_DIR, prefix="EIBDLNSA").with_suffix(".txt")

==================

BASE_DIR   = Path("/dwh")
MIS_DIR    = BASE_DIR / "ln_ln"          # .sas7bdat loan files  (prefix: ln)
BRANCH_DIR = Path("/sasdata/rawdata/lookup")      # flat file  (no date prefix)
OUTPUT_DIR = BASE_DIR / "OUTPUT" / "EIBDLNSA"
