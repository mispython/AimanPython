# !/usr/bin/env python3
"""
Program: EIBMCCAP
Purpose: JCL orchestrator – runs the full CAP computation pipeline in sequence:
         EIBCAP41 → EIBCAPS1 → EIBCAP42 → EIBCAPS2 → EIBCAP43 → EIBCAPS3 → EIBCAP44

ESMR 2013-673 CHANGE THE COMPUTATION OF CAP
"""

import os
import sys
import shutil
import logging
from datetime import date

# ─────────────────────────────────────────────
# PATH CONFIGURATION
# Mirrors the JCL DD statements for every step
# ─────────────────────────────────────────────
BASE_DIR   = r"C:\data"
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
NPL_DIR    = os.path.join(BASE_DIR, "npl")

# ── Output text datasets (JCL DELETE step equivalents) ───────────────────────
# DD01  SAP.PBB.CAPCAMP.TEXT
CAPCAMP_TEXT        = os.path.join(OUTPUT_DIR, "EIBCAP41.txt")
# DD02  SAP.PBB.CAPBRCH.TEXT
CAPBRCH_TEXT        = os.path.join(OUTPUT_DIR, "EIBCAP42.txt")
# DD03  SAP.PBB.CAPCATE.TEXT
CAPCATE_TEXT        = os.path.join(OUTPUT_DIR, "EIBCAP43.txt")
# DD04  SAP.PBB.FSAS5.TEXT  (CCRIS interface output from EIBCAP44)
FSAS5_TEXT          = os.path.join(OUTPUT_DIR, "ccris.txt")
# DD05  SAP.PBB.CAPCAMP.STAFF.TEXT
CAPCAMP_STAFF_TEXT  = os.path.join(OUTPUT_DIR, "EIBCAPS1.txt")
# DD06  SAP.PBB.CAPBRCH.STAFF.TEXT
CAPBRCH_STAFF_TEXT  = os.path.join(OUTPUT_DIR, "EIBCAPS2.txt")
# DD07  SAP.PBB.CAPCATE.STAFF.TEXT
CAPCATE_STAFF_TEXT  = os.path.join(OUTPUT_DIR, "EIBCAPS3.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(NPL_DIR,    exist_ok=True)

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# DELETE step – remove stale output files
# Equivalent to IEFBR14 with DISP=(MOD,DELETE,DELETE)
# ─────────────────────────────────────────────
_DELETE_FILES = [
    CAPCAMP_TEXT,
    CAPBRCH_TEXT,
    CAPCATE_TEXT,
    FSAS5_TEXT,
    CAPCAMP_STAFF_TEXT,
    CAPBRCH_STAFF_TEXT,
    CAPCATE_STAFF_TEXT,
]

log.info("DELETE step – removing stale output files")
for _f in _DELETE_FILES:
    if os.path.exists(_f):
        os.remove(_f)
        log.info("  Deleted : %s", _f)

# ─────────────────────────────────────────────
# Import each step as a module.
# Each module executes its logic at import time (top-level script pattern),
# so the import order matches the JCL step execution order.
# ─────────────────────────────────────────────

# ── EIBCAP41 – PBB HP CAP computation → NPL.CAP1, CAPCAMP.TEXT ──────────────
log.info("STEP EIBCAP41 – PBB HP CAP computation")
try:
    import EIBCAP41  # noqa: F401  produces npl/cap1.parquet + EIBCAP41.txt
    log.info("  EIBCAP41 completed successfully")
except Exception as exc:
    log.error("  EIBCAP41 FAILED: %s", exc)
    sys.exit(1)

# ── EIBCAPS1 – Staff LN CAP computation → NPL.CAP1_STAFF, CAPCAMP.STAFF.TEXT ─
log.info("STEP EIBCAPS1 – Staff LN CAP computation")
try:
    import EIBCAPS1  # noqa: F401  produces npl/cap1_staff.parquet + EIBCAPS1.txt
    log.info("  EIBCAPS1 completed successfully")
except Exception as exc:
    log.error("  EIBCAPS1 FAILED: %s", exc)
    sys.exit(1)

# ── EIBCAP42 – PBB CAP by Branch report → NPL.CAP{mm}{yy}, CAPBRCH.TEXT ─────
log.info("STEP EIBCAP42 – PBB CAP by Branch")
try:
    import EIBCAP42  # noqa: F401  produces npl/cap{REPTMON}{REPTYEAR}.parquet + EIBCAP42.txt
    log.info("  EIBCAP42 completed successfully")
except Exception as exc:
    log.error("  EIBCAP42 FAILED: %s", exc)
    sys.exit(1)

# ── EIBCAPS2 – Staff CAP by Branch report → NPL.CAP_STAFF{mm}{yy}, CAPBRCH.STAFF.TEXT ─
log.info("STEP EIBCAPS2 – Staff CAP by Branch")
try:
    import EIBCAPS2  # noqa: F401  produces npl/cap_staff{REPTMON}{REPTYEAR}.parquet + EIBCAPS2.txt
    log.info("  EIBCAPS2 completed successfully")
except Exception as exc:
    log.error("  EIBCAPS2 FAILED: %s", exc)
    sys.exit(1)

# ── EIBCAP43 – PBB CAP by Category report → CAPCATE.TEXT ────────────────────
log.info("STEP EIBCAP43 – PBB CAP by Category")
try:
    import EIBCAP43  # noqa: F401  produces EIBCAP43.txt
    log.info("  EIBCAP43 completed successfully")
except Exception as exc:
    log.error("  EIBCAP43 FAILED: %s", exc)
    sys.exit(1)

# ── EIBCAPS3 – Staff CAP by Category report → CAPCATE.STAFF.TEXT ─────────────
log.info("STEP EIBCAPS3 – Staff CAP by Category")
try:
    import EIBCAPS3  # noqa: F401  produces EIBCAPS3.txt
    log.info("  EIBCAPS3 completed successfully")
except Exception as exc:
    log.error("  EIBCAPS3 FAILED: %s", exc)
    sys.exit(1)

# ── EIBCAP44 – Interface CAP to ECCRIS for all accounts → FSAS5.TEXT ─────────
# * INTERFACE CAP TO ECCRIS FOR ALL ACCOUNTS *
log.info("STEP EIBCAP44 – Interface CAP to ECCRIS")
try:
    import EIBCAP44  # noqa: F401  produces output/ccris.txt
    log.info("  EIBCAP44 completed successfully")
except Exception as exc:
    log.error("  EIBCAP44 FAILED: %s", exc)
    sys.exit(1)

# ─────────────────────────────────────────────
# Final summary – confirm all output files exist
# ─────────────────────────────────────────────
log.info("Pipeline complete – verifying output files")
all_ok = True
for _f in _DELETE_FILES:
    if os.path.exists(_f):
        size = os.path.getsize(_f)
        log.info("  OK  (%10d bytes)  %s", size, _f)
    else:
        log.warning("  MISSING : %s", _f)
        all_ok = False

if all_ok:
    log.info("All output files produced successfully.")
else:
    log.warning("One or more output files are missing – review logs above.")
    sys.exit(2)
