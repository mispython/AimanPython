#!/usr/bin/env python3
"""
Program: EIIMLIQP
Purpose: Python conversion of the SAS control program that derives reporting
         macro variables, pre-deletes output files, and runs DALWPBBD,
         EIIMRLFM, and EIBMTOP5.
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
from EIIMRLFM import main as run_eiimrlfm
from EIBMTOP5 import main as run_eibmtop5

# //EIIMLIQP JOB MIS,MISEIS,COND=(4,LT),CLASS=A,MSGCLASS=X,
# //         NOTIFY=&SYSUID,USER=OPCC
# /*JOBPARM S=S1M2
# //*
# //* TOP5O ISLAMIC - REFER JOB EIBDMISA.
# //*
# //PRINT1   OUTPUT CLASS=R,
# //         NAME='MS KOH JIAK WEI',
# //         ROOM='26TH FLOOR BNM REPORTING',
# //         BUILDING='MENARA PBB',
# //         ADDRESS=('FINANCE DIVISION','MENARA PUBLIC BANK',
# //         '146 JALAN AMPANG','50450 KUALA LUMPUR'),
# //         DEST=S1.RMT5
# //*
# //DELETE   EXEC PGM=IEFBR14
# //DD1      DD DISP=(MOD,DELETE,DELETE),
# //            SPACE=(TRK,(1,10)),
# //            DSN=SAP.PIBB.FISS.TEXT
# //DD2      DD DISP=(MOD,DELETE,DELETE),
# //            SPACE=(TRK,(1,10)),
# //            DSN=SAP.PIBB.NSRS.TEXT
# //*
# //EIBMRLFM EXEC SAS609,REGION=6M,WORK='120000,8000'
# //PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
# //DEPOSIT  DD DSN=SAP.PIBB.MNITB(0),DISP=SHR
# //FD       DD DSN=SAP.PIBB.MNIFD(0),DISP=SHR
# //LOAN     DD DSN=SAP.PIBB.MNILN(0),DISP=SHR
# //CISLN    DD DSN=SAP.PBB.CISBEXT.DP,DISP=SHR
# //CISDP    DD DSN=SAP.PBB.CRM.CISBEXT,DISP=SHR
# //BNMTBL1  DD DSN=SAP.PIBB.KAPITI1(0),DISP=SHR
# //BNMTBL3  DD DSN=SAP.PIBB.KAPITI3(0),DISP=SHR
# //PROVSUB  DD DSN=SAP.PIBB.CCRIS.PROVSUB(0),DISP=SHR
# //BNMK     DD DSN=SAP.PIBB.KAPITI.SASDATA,DISP=SHR
# //PAY      DD DSN=SAP.PIBB.LNPAYSCH,DISP=SHR
# //BNM      DD DSN=&&TEMP,DISP=(NEW,DELETE,DELETE),
# //            DCB=(RECFM=FS,LRECL=27648,BLKSIZE=27648),
# //            SPACE=(CYL,(600,300)),UNIT=(SYSDA,5)
# //LCR      DD DSN=SAP.PIBB.LCR.SASDATA,DISP=OLD
# //NID      DD DSN=SAP.PIBB.RNID.SASDATA,DISP=SHR
# //BNM1     DD DSN=SAP.PIBB.SASDATA,DISP=SHR
# //FISS     DD DSN=SAP.PIBB.FISS.TEXT,DISP=(NEW,CATLG,DELETE),
# //            DCB=(RECFM=FB,LRECL=80,BLKSIZE=6320),
# //            SPACE=(TRK,(5,5)),UNIT=SYSDA
# //NSRS     DD DSN=SAP.PIBB.NSRS.TEXT,DISP=(NEW,CATLG,DELETE),
# //            DCB=(RECFM=FB,LRECL=80,BLKSIZE=6320),
# //            SPACE=(TRK,(5,5)),UNIT=SYSDA
# //FD11TEXT DD DSN=&&INDV,DISP=(NEW,DELETE,DELETE),
# //            DCB=(RECFM=FB,LRECL=320,BLKSIZE=0),
# //            SPACE=(CYL,(50,50),RLSE),UNIT=(SYSDA,5)
# //FD12TEXT DD DSN=&&CORP,DISP=(NEW,DELETE,DELETE),
# //            DCB=(RECFM=FB,LRECL=320,BLKSIZE=0),
# //            SPACE=(CYL,(50,50),RLSE),UNIT=(SYSDA,5)
# //FD2TEXT  DD DSN=&&SUBS,DISP=(NEW,DELETE,DELETE),
# //            DCB=(RECFM=FB,LRECL=320,BLKSIZE=0),
# //            SPACE=(CYL,(50,50),RLSE),UNIT=(SYSDA,5)
# //SASLIST  DD SYSOUT=*
# //SYSIN     DD *

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
)

REPTDATE_CANDIDATES = (
    DATA_INPUT_PATH / "DEPOSIT_REPTDATE.parquet",
    DATA_INPUT_PATH / "REPTDATE.parquet",
    DATA_PATH / "DEPOSIT_REPTDATE.parquet",
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

    # LIBNAME WALK  "SAP.PIBB.D&REPTYEAR" DISP=SHR;
    walk_libref = f"SAP.PIBB.D{macros['REPTYEAR']}"

    os.environ["REPTDATE"] = reptdate.isoformat()
    os.environ["NOWK"] = macros["NOWK"]
    os.environ["REPTYEAR"] = macros["REPTYEAR"]
    os.environ["REPTMON"] = macros["REPTMON"]
    os.environ["REPTDAY"] = macros["REPTDAY"]
    os.environ["RDATE"] = macros["RDATE"]
    os.environ["WALK_LIBREF"] = walk_libref

    LOGGER.info(
        "REPTDATE=%s NOWK=%s REPTYEAR=%s REPTMON=%s REPTDAY=%s RDATE=%s WALK_LIBREF=%s",
        os.environ["REPTDATE"],
        os.environ["NOWK"],
        os.environ["REPTYEAR"],
        os.environ["REPTMON"],
        os.environ["REPTDAY"],
        os.environ["RDATE"],
        os.environ["WALK_LIBREF"],
    )

    _delete_prior_outputs()

    reptmon_compound = f"{macros['REPTYEAR']}{macros['REPTMON']}"
    dal_result = run_dalwpbbd(reptmon=reptmon_compound, nowk=macros["NOWK"])
    LOGGER.info("DALWPBBD result: %s", dal_result)

    run_eiimrlfm()
    run_eibmtop5()


if __name__ == "__main__":
    main()
