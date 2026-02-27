# !/usr/bin/env python3
"""
Program: EIBMLIQP
Purpose: Python conversion of the SAS control program that derives reporting macros,
         removes prior output files, and runs DALWPBBD, EIBMRLFM, and EIBMTOP5.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict

import duckdb

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from DALWPBBD import main as run_dalwpbbd
from EIBMRLFM import main as run_eibmrlfm
from EIBMTOP5 import main as run_eibmtop5

# //EIBMLIQP JOB MIS,MISEIS,COND=(4,LT),CLASS=A,MSGCLASS=X,
# //         NOTIFY=&SYSUID,USER=OPCC
# /*JOBPARM S=S1M2
# //PRINT1   OUTPUT CLASS=R,
# //         NAME='MS EMILY ONG AI WENG',
# //         ROOM='26TH FLOOR FINANCIAL ACCOUNTING',
# //         BUILDING='MENARA PBB',
# //         ADDRESS=('FINANCE DIVISION','MENARA PUBLIC BANK',
# //         '146 JALAN AMPANG','50450 KUALA LUMPUR'),
# //         DEST=S1.RMT5
# //*
# //DELETE   EXEC PGM=IEFBR14
# //DD1      DD DISP=(MOD,DELETE,DELETE),
# //            SPACE=(TRK,(1,10)),
# //            DSN=SAP.PBB.FISS.TEXT
# //DD1      DD DISP=(MOD,DELETE,DELETE),
# //            SPACE=(TRK,(1,10)),
# //            DSN=SAP.PBB.NSRS.TEXT
# //DD2      DD DISP=(MOD,DELETE,DELETE),
# //            SPACE=(TRK,(1,10)),
# //            DSN=SAP.PBB.INDTOP50.TEXT
# //DD3      DD DISP=(MOD,DELETE,DELETE),
# //            SPACE=(TRK,(1,10)),
# //            DSN=SAP.PBB.CORTOP50.TEXT
# //DD4      DD DISP=(MOD,DELETE,DELETE),
# //            SPACE=(TRK,(1,10)),
# //            DSN=SAP.PBB.TOP50.TEXT
# //*
# %INC PGM(DALWPBBD);
# %INC PGM(EIBMRLFM);
# %INC PGM(EIBMTOP5);

# OPTIONS SORTDEV=3390 YEARCUTOFF=1950;

# =============================================================================
# PATHS (defined early)
# =============================================================================
ROOT_PATH = Path(os.environ.get("AIMANPY_ROOT", Path.cwd()))
DATA_PATH = ROOT_PATH / "data"
DATA_INPUT_PATH = DATA_PATH / "input"
OUTPUT_PATH = ROOT_PATH / "output"

OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

PREDELETE_OUTPUTS = (
    OUTPUT_PATH / "FISS.txt",
    OUTPUT_PATH / "NSRS.txt",
    OUTPUT_PATH / "FD11TEXT.txt",
    OUTPUT_PATH / "FD12TEXT.txt",
    OUTPUT_PATH / "FD2TEXT.txt",
)

REPTDATE_CANDIDATES = (
    DATA_INPUT_PATH / "DEPOSIT_REPTDATE.parquet",
    DATA_INPUT_PATH / "REPTDATE.parquet",
    DATA_PATH / "REPTDATE.parquet",
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
LOGGER = logging.getLogger(__name__)


def _resolve_reptdate_file() -> Path:
    for path in REPTDATE_CANDIDATES:
        if path.exists():
            return path
    checked = ", ".join(str(p) for p in REPTDATE_CANDIDATES)
    raise FileNotFoundError(f"Unable to locate REPTDATE parquet. Checked: {checked}")


def _read_reptdate() -> date:
    reptdate_parquet = _resolve_reptdate_file()
    with duckdb.connect() as con:
        row = con.execute(
            "SELECT REPTDATE FROM read_parquet(?) LIMIT 1",
            [str(reptdate_parquet)],
        ).fetchone()

    if not row or row[0] is None:
        raise ValueError(f"REPTDATE missing in {reptdate_parquet}")

    rept_val = row[0]
    if isinstance(rept_val, datetime):
        return rept_val.date()
    if isinstance(rept_val, date):
        return rept_val
    return datetime.fromisoformat(str(rept_val)).date()


def _derive_macro_vars(reptdate_value: date) -> Dict[str, str]:
    if reptdate_value.day == 8:
        nowk = "1"
    elif reptdate_value.day == 15:
        nowk = "2"
    elif reptdate_value.day == 22:
        nowk = "3"
    else:
        nowk = "4"

    return {
        "NOWK": nowk,
        "REPTYEAR": f"{reptdate_value.year:04d}",
        "REPTMON": f"{reptdate_value.month:02d}",
        "REPTDAY": f"{reptdate_value.day:02d}",
        "RDATE": reptdate_value.strftime("%d/%m/%y"),
    }


def _delete_prior_outputs() -> None:
    for out_file in PREDELETE_OUTPUTS:
        if out_file.exists():
            out_file.unlink()
            LOGGER.info("Deleted prior output %s", out_file)


def main() -> None:
    reptdate = _read_reptdate()
    macros = _derive_macro_vars(reptdate)

    reptmon_compound = f"{macros['REPTYEAR']}{macros['REPTMON']}"

    os.environ["REPTDATE"] = reptdate.isoformat()
    os.environ["NOWK"] = macros["NOWK"]
    os.environ["REPTYEAR"] = macros["REPTYEAR"]
    os.environ["REPTMON"] = macros["REPTMON"]
    os.environ["REPTDAY"] = macros["REPTDAY"]
    os.environ["RDATE"] = macros["RDATE"]

    LOGGER.info(
        "REPTDATE=%s NOWK=%s REPTYEAR=%s REPTMON=%s REPTDAY=%s RDATE=%s",
        os.environ["REPTDATE"],
        os.environ["NOWK"],
        os.environ["REPTYEAR"],
        os.environ["REPTMON"],
        os.environ["REPTDAY"],
        os.environ["RDATE"],
    )

    _delete_prior_outputs()

    dal_result = run_dalwpbbd(reptmon=reptmon_compound, nowk=macros["NOWK"])
    LOGGER.info("DALWPBBD result: %s", dal_result)

    run_eibmrlfm()
    run_eibmtop5()


if __name__ == "__main__":
    main()
