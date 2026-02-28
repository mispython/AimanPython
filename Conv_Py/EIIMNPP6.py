# !/usr/bin/env python3
"""
Program  : EIIMNPP6.py
Purpose  : Read a consolidated NPL RPS report (INFIL01), detect branch header
            lines by locating the 'BRANCH :' tag at column 25, derive the CAC
            code (format CACBRCH) and route each subsequent line to the
            appropriate CAC-specific intermediate file (OUTFIL01-06).
           Then consolidate all six CAC files into a single combined output
            file (OUTFIL0X), prepending the standard SINPLRPS header per CAC.

           Detection method: single-line scan — BRCHTAG read from col 25-32
            of the current LINE1; BRANCH read from col 34-36 of the same line.
           NEWPAGE emits exactly ONE P001-prefixed line (LINE1 only).

           Structurally identical to EIIMNPP5; processes a different input
            feed (separate JCL INFIL01 DD allocation / dataset).

           CAC routing:
             911 → OUTFIL01  (KL)
             912 → OUTFIL02  (City Centre)
             916 → OUTFIL03  (Butterworth/SJ)
             915 → OUTFIL04  (Penang)
             914 → OUTFIL05  (Johor Bahru)
             913 → OUTFIL06  (Kelang)

OPTIONS YEARCUTOFF=1950 NOCENTER NODATE NONUMBER MISSING=0
"""

import os
import io

from PBBELF import format_cacbrch   # PUT(BRCHNUM, CACBRCH.) equivalent

# ─────────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────────
BASE_DIR   = r"C:/data"
INPUT_DIR  = os.path.join(BASE_DIR, "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

INFIL01_PATH  = os.path.join(INPUT_DIR,  "EIIMNPP6_INFIL01.txt")
OUTFIL01_PATH = os.path.join(OUTPUT_DIR, "EIIMNPP6_CAC911.txt")
OUTFIL02_PATH = os.path.join(OUTPUT_DIR, "EIIMNPP6_CAC912.txt")
OUTFIL03_PATH = os.path.join(OUTPUT_DIR, "EIIMNPP6_CAC916.txt")
OUTFIL04_PATH = os.path.join(OUTPUT_DIR, "EIIMNPP6_CAC915.txt")
OUTFIL05_PATH = os.path.join(OUTPUT_DIR, "EIIMNPP6_CAC914.txt")
OUTFIL06_PATH = os.path.join(OUTPUT_DIR, "EIIMNPP6_CAC913.txt")
OUTFIL0X_PATH = os.path.join(OUTPUT_DIR, "EIIMNPP6_COMBINED.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─────────────────────────────────────────────
# Column positions (1-based, converted to 0-based slices)
# INPUT @1 LINE1 $CHAR134. @25 BRCHTAG $8.
# INPUT @34 BRANCH $3.
# ─────────────────────────────────────────────
LINE_LEN      = 134
BRCHTAG_COL   = 25    # 1-based start of BRCHTAG
BRCHTAG_WIDTH = 8
BRANCH_COL    = 34    # 1-based start of BRANCH
BRANCH_WIDTH  = 3

BRCHTAG_START = BRCHTAG_COL - 1
BRCHTAG_END   = BRCHTAG_START + BRCHTAG_WIDTH
BRANCH_START  = BRANCH_COL - 1
BRANCH_END    = BRANCH_START + BRANCH_WIDTH

# ─────────────────────────────────────────────
# CAC → file path mapping
# ─────────────────────────────────────────────
CAC_FILE_MAP = {
    "911": OUTFIL01_PATH,
    "912": OUTFIL02_PATH,
    "916": OUTFIL03_PATH,
    "915": OUTFIL04_PATH,
    "914": OUTFIL05_PATH,
    "913": OUTFIL06_PATH,
}

CAC_CONSOLIDATION_ORDER = [
    ("911", OUTFIL01_PATH),
    ("912", OUTFIL02_PATH),
    ("916", OUTFIL03_PATH),
    ("915", OUTFIL04_PATH),
    ("914", OUTFIL05_PATH),
    ("913", OUTFIL06_PATH),
]

# ─────────────────────────────────────────────
# Open per-CAC output file handles
# ─────────────────────────────────────────────
cac_handles: dict[str, io.TextIOWrapper] = {
    cac: open(path, "w", encoding="utf-8")
    for cac, path in CAC_FILE_MAP.items()
}

def write_to_cac(cac: str, text: str):
    if cac in cac_handles:
        cac_handles[cac].write(text + "\n")

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def pad(s: str, length: int = LINE_LEN) -> str:
    """Pad / truncate to fixed width (mirrors $CHAR134.)."""
    return f"{str(s or ''):<{length}}"[:length]

def make_outbuf(width: int = 140) -> list:
    return [" "] * width

def place(text: str, col: int, buf: list):
    for i, ch in enumerate(str(text)):
        pos = col - 1 + i
        if 0 <= pos < len(buf):
            buf[pos] = ch

def render(buf: list) -> str:
    return "".join(buf)

def put_p001(line_content: str) -> str:
    """@1 'P001' @5 <content $CHAR134.>"""
    b = make_outbuf()
    place("P001", 1, b)
    place(pad(line_content), 5, b)
    return render(b).rstrip()

def make_p000_header(cac: str) -> str:
    """@1 'P000PBBEDPPBBEDP' @133 'B<cac>'"""
    b = make_outbuf(140)
    place("P000PBBEDPPBBEDP", 1,   b)
    place(f"B{cac}",          133, b)
    return render(b).rstrip()

# ─────────────────────────────────────────────
# DATA A – parse INFIL01
#
# SAS logic:
#   RETAIN BRCH '   ' CACIND 0 CAC '000'
#   INPUT @1 LINE1 $CHAR134. @25 BRCHTAG $8. @;   ← read line, extract BRCHTAG
#   IF BRCHTAG = 'BRANCH :' THEN DO;
#      INPUT @34 BRANCH $3.;                       ← extract BRANCH from same line
#      ... derive CAC, route, emit E255 + P000 + LINK NEWPAGE
#   END;
#   ELSE DO;
#      ... route data lines as P001
#   END;
#   NEWPAGE: PUT @1 'P001' @5 LINE1 $CHAR134.;    ← only ONE header line emitted
# ─────────────────────────────────────────────
def process_infil01():
    brch   = "   "
    cacind = 0
    cac    = "000"

    with open(INFIL01_PATH, "r", encoding="utf-8") as fh:
        raw_lines = fh.readlines()

    lines = [ln.rstrip("\n").rstrip("\r") for ln in raw_lines]
    total = len(lines)
    i     = 0

    while i < total:
        line1   = pad(lines[i])
        # @25 BRCHTAG $8.  (1-based col 25, width 8)
        brchtag = line1[BRCHTAG_START:BRCHTAG_END]
        i += 1

        if brchtag == "BRANCH :":
            # INPUT @34 BRANCH $3. – cols 34-36 of the same LINE1
            branch  = line1[BRANCH_START:BRANCH_END].strip()
            brchnum = 0
            try:
                brchnum = int(branch)
            except ValueError:
                pass

            # CAC = PUT(BRCHNUM, CACBRCH.)
            cac    = format_cacbrch(brchnum)
            cacind = 1 if cac != "000" else 0
            brno   = "B" + cac.strip()   # noqa: F841

            if cacind == 1:
                if brch != branch:
                    brch = branch

                # PUT @1 'E255'
                write_to_cac(cac, "E255")
                # PUT @1 'P000REPORT NO :  SINPLRPS REPORTS'
                write_to_cac(cac, "P000REPORT NO :  SINPLRPS REPORTS")

                # LINK NEWPAGE – only LINE1
                write_to_cac(cac, put_p001(line1))

        else:
            # ELSE DO – data line: route to current CAC file as P001
            if cacind == 1:
                # PUT @1 'P001' @5 _INFILE_
                write_to_cac(cac, put_p001(line1))
            # else: skip (non-CAC branch)

# ─────────────────────────────────────────────
# Run parser
# ─────────────────────────────────────────────
process_infil01()

for handle in cac_handles.values():
    handle.close()

# ─────────────────────────────────────────────
# Consolidation into OUTFIL0X
# FIRSTOBS=3 → skip first 2 lines of each CAC file
# ─────────────────────────────────────────────
with open(OUTFIL0X_PATH, "w", encoding="utf-8") as outfx:
    for cac, cac_path in CAC_CONSOLIDATION_ORDER:
        if not os.path.exists(cac_path):
            continue

        with open(cac_path, "r", encoding="utf-8") as cac_fh:
            cac_lines = cac_fh.readlines()

        outfx.write("E255\n")
        outfx.write(make_p000_header(cac) + "\n")
        outfx.write("P000REPORT NO :  SINPLRPS REPORTS\n")

        for line in cac_lines[2:]:
            outfx.write(line if line.endswith("\n") else line + "\n")

print(f"CAC split files written to : {OUTPUT_DIR}")
print(f"Combined output written to : {OUTFIL0X_PATH}")
