# !/usr/bin/env python3
"""
Program  : EIIMNPP4.py
Purpose  : Read a consolidated NPL RPS report (INFIL01), detect page headers
            that begin with 'PUBLIC', extract the BRANCH tag, derive the CAC
            code (format CACBRCH) and route each page to the appropriate
            CAC-specific intermediate file (OUTFIL01-06).
           Then consolidate all six CAC files into a single combined output
            file (OUTFIL0X), prepending the standard SINPLRPS header per CAC.

           This version reads SIX header lines (LINE1-LINE6) per page break,
            with the BRCHTAG = 'BRANCH=' check wrapped in a DO/END block.
           Structurally identical to EIIMNPP3; differs only in the input
            source file it processes (separate JCL INFIL01 DD allocation).

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

INFIL01_PATH  = os.path.join(INPUT_DIR,  "EIIMNPP4_INFIL01.txt")
OUTFIL01_PATH = os.path.join(OUTPUT_DIR, "EIIMNPP4_CAC911.txt")
OUTFIL02_PATH = os.path.join(OUTPUT_DIR, "EIIMNPP4_CAC912.txt")
OUTFIL03_PATH = os.path.join(OUTPUT_DIR, "EIIMNPP4_CAC916.txt")
OUTFIL04_PATH = os.path.join(OUTPUT_DIR, "EIIMNPP4_CAC915.txt")
OUTFIL05_PATH = os.path.join(OUTPUT_DIR, "EIIMNPP4_CAC914.txt")
OUTFIL06_PATH = os.path.join(OUTPUT_DIR, "EIIMNPP4_CAC913.txt")
OUTFIL0X_PATH = os.path.join(OUTPUT_DIR, "EIIMNPP4_COMBINED.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─────────────────────────────────────────────
# CAC → file path mapping  (matches SAS routing)
# ─────────────────────────────────────────────
CAC_FILE_MAP = {
    "911": OUTFIL01_PATH,
    "912": OUTFIL02_PATH,
    "916": OUTFIL03_PATH,
    "915": OUTFIL04_PATH,
    "914": OUTFIL05_PATH,
    "913": OUTFIL06_PATH,
}

# CAC order used in the final OUTFIL0X consolidation
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
    """Write a line to the CAC-specific output file."""
    if cac in cac_handles:
        cac_handles[cac].write(text + "\n")

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
LINE_LEN = 134   # $CHAR134.

def pad(s: str, length: int = LINE_LEN) -> str:
    """Pad or truncate string to fixed length (mirrors $CHAR134.)."""
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
    """Build a 'P001' prefixed output line: @1 'P001' @5 <content>."""
    b = make_outbuf()
    place("P001", 1, b)
    place(pad(line_content), 5, b)
    return render(b).rstrip()

def newpage_lines(line1, line2, line3, line4, line5, line6) -> list[str]:
    """
    LINK NEWPAGE: emit 6 P001-prefixed header lines.
    PUT @1 'P001' @5 LINE1 $CHAR134. /
        @1 'P001' @5 LINE2 $CHAR134. /
        ...
        @1 'P001' @5 LINE6 $CHAR134.
    """
    return [
        put_p001(line1),
        put_p001(line2),
        put_p001(line3),
        put_p001(line4),
        put_p001(line5),
        put_p001(line6),
    ]

# ─────────────────────────────────────────────
# DATA A – parse INFIL01
#
# SAS logic (6-line header variant, with DO/END wrapper):
#   RETAIN BRCH '   ' CACIND 0 CAC '000'
#   INPUT @1 LINE1 $CHAR134. @;           ← read LINE1, hold position
#   IF SUBSTR(LINE1,1,6) = 'PUBLIC' THEN DO;
#      INPUT / @1 LINE2 / LINE3 / LINE4 / LINE5 /
#              @1 LINE6 @1 BRCHTAG $7. @;  ← read 5 more lines, peek BRCHTAG
#      IF BRCHTAG NE 'BRANCH=' THEN BRANCH = BRCH;
#      IF BRCHTAG = 'BRANCH=' THEN DO;    ← explicit DO/END block
#         INPUT @12 BRANCH $3.;           ← cols 12-14 of LINE6
#         BRCHNUM = BRANCH;
#         CAC = PUT(BRCHNUM, CACBRCH.);
#         ... derive cacind, brno
#         IF CACIND = 1 THEN DO;
#            ... route + emit E255 + P000 + LINK NEWPAGE
#         END;
#      END;
#   END;
#   ELSE DO;
#      ... route data lines as P001
#   END;
# ─────────────────────────────────────────────
def process_infil01():
    """Main parsing loop – equivalent to DATA A."""
    brch   = "   "
    cacind = 0
    cac    = "000"

    with open(INFIL01_PATH, "r", encoding="utf-8") as fh:
        raw_lines = fh.readlines()

    lines = [ln.rstrip("\n").rstrip("\r") for ln in raw_lines]
    total = len(lines)
    i     = 0

    while i < total:
        line1 = pad(lines[i])

        if line1[:6] == "PUBLIC":
            # Read the next 5 lines: LINE2, LINE3, LINE4, LINE5, LINE6
            line2 = pad(lines[i + 1]) if i + 1 < total else pad("")
            line3 = pad(lines[i + 2]) if i + 2 < total else pad("")
            line4 = pad(lines[i + 3]) if i + 3 < total else pad("")
            line5 = pad(lines[i + 4]) if i + 4 < total else pad("")
            line6 = pad(lines[i + 5]) if i + 5 < total else pad("")
            i    += 6   # consumed 6 lines total (LINE1 + 5 more)

            # @1 BRCHTAG $7. – first 7 chars of LINE6
            brchtag = line6[:7]

            if brchtag != "BRANCH=":
                branch = brch

            # IF BRCHTAG = 'BRANCH=' THEN DO; ... END;
            if brchtag == "BRANCH=":
                # INPUT @12 BRANCH $3. – cols 12-14 of LINE6 (1-based → [11:14])
                branch  = line6[11:14].strip()
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

                    # LINK NEWPAGE – emit 6 P001 header lines
                    for out_line in newpage_lines(line1, line2, line3, line4, line5, line6):
                        write_to_cac(cac, out_line)

        else:
            # ELSE DO – data line: route to current CAC file as P001
            if cacind == 1:
                data_line = lines[i]
                i += 1
                # PUT @1 'P001' @5 _INFILE_
                write_to_cac(cac, put_p001(data_line))
            else:
                i += 1   # skip lines for non-CAC branches

# ─────────────────────────────────────────────
# Run the parser
# ─────────────────────────────────────────────
process_infil01()

# Close all per-CAC handles
for handle in cac_handles.values():
    handle.close()

# ─────────────────────────────────────────────
# Consolidation: write all 6 CAC files into OUTFIL0X
#
# For each CAC:
#   IF _N_ = 1 THEN DO;
#      PUT @1 'E255';
#      PUT @1 'P000PBBEDPPBBEDP' @133 'B<cac>';
#      PUT @1 'P000REPORT NO :  SINPLRPS REPORTS';
#   END;
#   INFILE OUTFIL0N FIRSTOBS=3;   ← skip first 2 lines
#   INPUT; PUT _INFILE_;
# ─────────────────────────────────────────────
def make_p000_header(cac: str) -> str:
    """
    PUT @1 'P000PBBEDPPBBEDP' @133 'B<cac>'
    Col 1-4='P000', col 5-20='PBBEDPPBBEDP', col 133-136='B<cac>' (1-based)
    """
    b = make_outbuf(140)
    place("P000PBBEDPPBBEDP", 1,   b)
    place(f"B{cac}",          133, b)
    return render(b).rstrip()

with open(OUTFIL0X_PATH, "w", encoding="utf-8") as outfx:
    for cac, cac_path in CAC_CONSOLIDATION_ORDER:
        if not os.path.exists(cac_path):
            continue

        with open(cac_path, "r", encoding="utf-8") as cac_fh:
            cac_lines = cac_fh.readlines()

        # Write 3-line preamble (_N_ = 1 block)
        outfx.write("E255\n")
        outfx.write(make_p000_header(cac) + "\n")
        outfx.write("P000REPORT NO :  SINPLRPS REPORTS\n")

        # FIRSTOBS=3 – skip the first 2 lines of the CAC file
        for line in cac_lines[2:]:
            outfx.write(line if line.endswith("\n") else line + "\n")

print(f"CAC split files written to : {OUTPUT_DIR}")
print(f"Combined output written to : {OUTFIL0X_PATH}")
