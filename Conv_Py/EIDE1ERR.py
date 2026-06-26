"""
Program : EIDE1ERR.py
Purpose : Validation Error Report (IBM vs DETICA)
          Compares record counts between DETICA and IBM HOST
          datasets and generates a formatted validation report.
"""

import sys
from pathlib import Path
from datetime import datetime

import duckdb
import polars as pl

from REPTDATE import get_reptdate_values

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
BASE_DIR   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
INPUT_DIR  = BASE_DIR / "input/uat/EIDE1ERR"
OUTPUT_DIR = BASE_DIR / "output/EIDE1ERR"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DATEFL_PATH = INPUT_DIR / "COUNTI_NEW.txt"   # IBM counts    (IBMFL in SAS)
DETFL_PATH  = INPUT_DIR / "COUNTD_NEW.txt"   # DETICA counts (DETFL in SAS)

# ------------------------------------------------------------
# Report-date derivation  (equivalent of DATA REPTDATE step)
# ------------------------------------------------------------
rv      = get_reptdate_values()
DAY     = rv.reptday                                      # Z2. day
MTH     = rv.reptmon                                      # Z2. month
YEAR    = rv.rdate.strftime("%Y")                         # YEAR4.
TIME    = datetime.now().strftime("%I:%M:%S %p").upper()  # TIMEAMPM11.

# ------------------------------------------------------------
# Output filename  (date + time encoded)
# ------------------------------------------------------------
_ts          = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_PATH  = OUTPUT_DIR / f"EIDE1ERR_{_ts}.txt"

# ============================================================
# Read DETFL  (DETICA counts)
# INFILE DETFL FIRSTOBS=4  → skip first 3 lines
# INPUT @01 DETNAME $40.  @42 DET_CNT 12.
# IF DET_CNT >= 0
# UID + 1  (SAS retain-counter, starts at 1)
# ============================================================
def read_detica(path: Path) -> pl.DataFrame:
    rows = []
    with open(path, encoding="latin1") as fh:
        lines = fh.readlines()

    uid = 0
    for line in lines[3:]:          # FIRSTOBS=4 → skip lines 0-2
        # pad / right-fill to at least 53 chars to safely slice
        line = line.rstrip("\n").rstrip("\r")
        padded = line.ljust(53)

        detname = padded[0:40].rstrip()          # @01 $40.
        det_cnt_raw = padded[41:53].strip()      # @42 12.

        try:
            det_cnt = int(det_cnt_raw)
        except ValueError:
            continue

        if det_cnt < 0:
            continue

        uid += 1
        rows.append({"UID": uid, "DETNAME": detname, "DET_CNT": det_cnt})

    return pl.DataFrame(rows, schema={"UID": pl.Int64, "DETNAME": pl.Utf8, "DET_CNT": pl.Int64})


# ============================================================
# Read IBMFL  (IBM counts)
# INPUT @01 IBMNAME $25.  @29 IBM_CNT 9.
# IF IBM_CNT >= 0
# UID + 1
# ============================================================
def read_ibm(path: Path) -> pl.DataFrame:
    rows = []
    with open(path, encoding="latin1") as fh:
        lines = fh.readlines()

    uid = 0
    for line in lines:
        line = line.rstrip("\n").rstrip("\r")
        padded = line.ljust(37)

        ibmname    = padded[0:25].rstrip()       # @01 $25.
        ibm_cnt_raw = padded[28:37].strip()      # @29 9.

        try:
            ibm_cnt = int(ibm_cnt_raw)
        except ValueError:
            continue

        if ibm_cnt < 0:
            continue

        uid += 1
        rows.append({"UID": uid, "IBMNAME": ibmname, "IBM_CNT": ibm_cnt})

    return pl.DataFrame(rows, schema={"UID": pl.Int64, "IBMNAME": pl.Utf8, "IBM_CNT": pl.Int64})


# ============================================================
# Merge DETICA + IBM on UID  (MERGE … BY UID; IF A AND B)
# Compute DIFF and TALLY
# ============================================================
def build_count(detica: pl.DataFrame, ibm: pl.DataFrame) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    con.register("detica", detica)
    con.register("ibm",    ibm)

    count = con.execute("""
        SELECT
            d.UID,
            d.DETNAME,
            d.DET_CNT,
            i.IBM_CNT,
            (d.DET_CNT - i.IBM_CNT)              AS DIFF,
            CASE WHEN (d.DET_CNT - i.IBM_CNT) = 0
                 THEN 'Y' ELSE 'N' END            AS TALLY
        FROM detica d
        INNER JOIN ibm i ON d.UID = i.UID
        ORDER BY d.UID
    """).pl()

    con.close()
    return count


# ============================================================
# Report formatting helpers
# ============================================================
LRECL    = 133
PAGE_LEN = 60   # lines per page (default)
SEP_LINE = "-" * 132


def _header_lines() -> list[str]:
    """Equivalent of the IF _N_=1 header block in DATA _NULL_."""
    lines = []

    # Line 1  (ASA '1' = form-feed / new page)
    l1 = (
        f"{'REPORT DATE : '}{DAY}-{MTH}-{YEAR}"
        .ljust(37)
        + "P U B L I C  B A N K  B E R H A D     "
        .center(70)
        + f"REPORT TIME : {TIME}"
    )
    lines.append(l1[:132])

    # Line 2
    l2 = " " * 55 + "  DETICA DAILY JOBS SCHEDULE        "
    lines.append(l2[:132])

    # Blank line
    lines.append("")

    # Separator
    lines.append(SEP_LINE)

    # Column headers – row 1
    h1 = (
        f"{'Obs':<5}"
        f"{'Batch Name (DETICA)':<34}"
        f"{'No. Of Records':>14}"
        f"  "
        f"{'No. Of Records':>14}"
        f"  "
        f"{'No. Of Records':>14}"
        f"  "
        f"{'Tally':^13}"
        f"  "
        f"{'Done By':<7}"
    )
    lines.append(h1[:132])

    # Column headers – row 2
    h2 = (
        " " * 39
        + f"{'(DETICA)':>14}"
        + "  "
        + f"{'(IBM HOST)':>14}"
        + "  "
        + f"{'(Variance)':>14}"
        + "  "
        + f"{'(Y/N)':^13}"
    )
    lines.append(h2[:132])

    # Separator
    lines.append(SEP_LINE)

    return lines


def _detail_lines(row: dict) -> list[str]:
    """
    PUT @001 UID 3.  @006 DETNAME $40.  @040 DET_CNT 12.
        @058 IBM_CNT 12.  @075 DIFF 12.  @096 TALLY $1.
    / @001 132*'-'
    / @001 (blank)
    """
    uid      = int(row["UID"])
    detname  = str(row["DETNAME"])
    det_cnt  = int(row["DET_CNT"])
    ibm_cnt  = int(row["IBM_CNT"])
    diff     = int(row["DIFF"])
    tally    = str(row["TALLY"])

    # SAS column positions are 1-based; Python slices are 0-based
    # @001  UID        3.   → cols 1-3
    # @006  DETNAME   $40.  → cols 6-45
    # @040  DET_CNT   12.   → cols 40-51
    # @058  IBM_CNT   12.   → cols 58-69
    # @075  DIFF      12.   → cols 75-86
    # @096  TALLY      $1.  → col  96
    buf = [" "] * 133
    uid_s     = f"{uid:>3}"
    detname_s = f"{detname:<40}"
    det_s     = f"{det_cnt:>12}"
    ibm_s     = f"{ibm_cnt:>12}"
    diff_s    = f"{diff:>12}"
    tally_s   = tally[:1]

    for i, ch in enumerate(uid_s,     start=0):   buf[i]   = ch
    for i, ch in enumerate(detname_s, start=5):   buf[i]   = ch
    for i, ch in enumerate(det_s,     start=39):  buf[i]   = ch
    for i, ch in enumerate(ibm_s,     start=57):  buf[i]   = ch
    for i, ch in enumerate(diff_s,    start=74):  buf[i]   = ch
    buf[95] = tally_s

    detail = "".join(buf[0:132])
    sep    = SEP_LINE
    blank  = ""

    return [detail, sep, blank]


def _footer_line() -> str:
    return (
        "# Call application team (CIS) if any of the records not TALLY."
    )


# ============================================================
# Write report  (DATA _NULL_ equivalent)
# ============================================================
def write_report(count: pl.DataFrame, output_path: Path) -> None:
    records  = count.to_dicts()
    out_lines: list[str] = []

    header = _header_lines()
    out_lines.extend(header)

    for row in records:
        out_lines.extend(_detail_lines(row))

    out_lines.append(_footer_line())

    # Write with LRECL=133 fixed-width lines
    with open(output_path, "w", encoding="latin1") as fh:
        for line in out_lines:
            # Pad / truncate to LRECL
            fh.write(f"{line:<{LRECL}}\n")

    print(f"[OUTPUT] {output_path}")

    # Also print to terminal
    for line in out_lines:
        print(line)


# ============================================================
# Main
# ============================================================
def main() -> None:
    print(f"[INFO] Reading DETICA file : {DETFL_PATH}")
    detica = read_detica(DETFL_PATH)

    print(f"[INFO] Reading IBM file    : {DATEFL_PATH}")
    ibm    = read_ibm(DATEFL_PATH)

    count  = build_count(detica, ibm)

    print(f"[INFO] Matched rows        : {len(count)}")

    write_report(count, OUTPUT_PATH)


if __name__ == "__main__":
    main()
