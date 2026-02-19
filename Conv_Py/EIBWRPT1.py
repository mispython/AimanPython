#!/usr/bin/env python3
"""
Program: EIBWRPT1

This migration preserves the original step order and intent:
1) Delete previously generated report outputs.
2) Execute each reporting program in sequence.
3) Copy generated report files to print destinations.

Notes:
- Input datasets are assumed to be parquet files.
- Report outputs are treated as text files (.txt).
- If any downstream program produces binary report output, update the output
  extension to .dat for that specific program.
- Commented JCL lines are preserved as Python comments.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import duckdb
import polars as pl


# -----------------------------------------------------------------------------
# PATH SETUP (defined early, per migration requirement)
# -----------------------------------------------------------------------------
BASE_DIR = Path(os.environ.get("EIBWRPT1_BASE_DIR", "/data/bnm_eibwrpt1"))
PARQUET_DIR = BASE_DIR / "parquet"
REPORT_DIR = BASE_DIR / "reports"
PROGRAM_DIR = BASE_DIR / "programs"
PRINT_DIR = BASE_DIR / "print"

# Logical dataset to local path mapping
DATASET_TO_PATH = {
    "SAP.PBB.MNITB(0)": PARQUET_DIR / "SAP_PBB_MNITB_0.parquet",
    "SAP.PIBB.MNITB(0)": PARQUET_DIR / "SAP_PIBB_MNITB_0.parquet",
    "SAP.PIBB.ISLM.SASDATA": PARQUET_DIR / "SAP_PIBB_ISLM_SASDATA.parquet",
    "SAP.PBB.MIS.D&YEAR": PARQUET_DIR / "SAP_PBB_MIS_DYEAR.parquet",
    "SAP.PBB.MIS.ISLAMIC.FD": PARQUET_DIR / "SAP_PBB_MIS_ISLAMIC_FD.parquet",
    "SAP.PBB.CRMCUMLN": PARQUET_DIR / "SAP_PBB_CRMCUMLN.parquet",
    "SAP.PBB.MNILN(0)": PARQUET_DIR / "SAP_PBB_MNILN_0.parquet",
    "SAP.PBB.SASDATA": PARQUET_DIR / "SAP_PBB_SASDATA.parquet",
    "SAP.PBB.NPL.SASDATA": PARQUET_DIR / "SAP_PBB_NPL_SASDATA.parquet",
}


@dataclass(frozen=True)
class ProgramStep:
    step_name: str
    sysin_program: str
    input_datasets: tuple[str, ...]
    output_dataset: str


@dataclass(frozen=True)
class PrintStep:
    step_name: str
    source_dataset: str
    output_tag: str


# //DELETE EXEC PGM=IEFBR14
DELETE_DATASETS = [
    "SAP.PBB.DIBMIS01.TEXT",
    "SAP.PBB.DIBMIS02.TEXT",
    "SAP.PBB.EIBWIS01.TEXT",
    "SAP.PBB.DIBMISA2.TEXT",
    "SAP.PBB.DIBMISA3.TEXT",
    "SAP.PIBB.DIIMISB2.TEXT",
    "SAP.PBB.EIBMISWT.TEXT",
    "SAP.PIBB.DIIMISA2.TEXT",
    "SAP.PIBB.DIIMISA3.TEXT",
    "SAP.PIBB.DIIMISB2.TEXT",
    "SAP.PIBB.DIIMISA5.TEXT",
    "SAP.PIBB.DIIMISA6.TEXT",
    "SAP.PIBB.DIIMISC1.TEXT",
    "SAP.PIBB.DIIMISC2.TEXT",
    "SAP.PIBB.DIIMISC3.TEXT",
    "SAP.PIBB.DIIMISC5.TEXT",
    "SAP.PIBB.DIIMISC6.TEXT",
    "SAP.PIBB.DIIMISC7.TEXT",
    "SAP.PIBB.DIIMIS7Y.TEXT",
    "SAP.PBB.DIBMISM1.TEXT",
]

PROGRAM_STEPS = [
    ProgramStep("DIBMIS01", "DIBMIS01", ("SAP.PBB.MNITB(0)", "SAP.PIBB.MNITB(0)", "SAP.PIBB.ISLM.SASDATA"), "SAP.PBB.DIBMIS01.TEXT"),
    ProgramStep("DIBMISM1", "DIBMISM1", ("SAP.PBB.MNITB(0)", "SAP.PIBB.MNITB(0)", "SAP.PIBB.ISLM.SASDATA"), "SAP.PBB.DIBMISM1.TEXT"),
    ProgramStep("DIBMIS02", "DIBMIS02", ("SAP.PBB.MIS.D&YEAR", "SAP.PBB.MNITB(0)"), "SAP.PBB.DIBMIS02.TEXT"),
    ProgramStep("EIBWIS01", "EIBWIS01", ("SAP.PIBB.MNITB(0)",), "SAP.PBB.EIBWIS01.TEXT"),
    ProgramStep("DIBMISA2", "DIBMISA2", ("SAP.PBB.MIS.D&YEAR", "SAP.PBB.MNITB(0)"), "SAP.PBB.DIBMISA2.TEXT"),
    ProgramStep("DIBMISA3", "DIBMISA3", ("SAP.PBB.MIS.D&YEAR", "SAP.PBB.MNITB(0)"), "SAP.PBB.DIBMISA3.TEXT"),
    ProgramStep("EIBMISWT", "EIBMISWT", ("SAP.PBB.CRMCUMLN", "SAP.PBB.MNILN(0)", "SAP.PBB.SASDATA", "SAP.PBB.NPL.SASDATA"), "SAP.PBB.EIBMISWT.TEXT"),
    ProgramStep("DIIMISB2", "DIIMISB2", ("SAP.PBB.MIS.ISLAMIC.FD", "SAP.PIBB.MNITB(0)"), "SAP.PIBB.DIIMISB2.TEXT"),
    ProgramStep("DIIMISA2", "DIIMISA2", ("SAP.PBB.MIS.ISLAMIC.FD", "SAP.PIBB.MNITB(0)"), "SAP.PIBB.DIIMISA2.TEXT"),
    ProgramStep("DIIMISA3", "DIIMISA3", ("SAP.PBB.MIS.ISLAMIC.FD", "SAP.PIBB.MNITB(0)"), "SAP.PIBB.DIIMISA3.TEXT"),
    ProgramStep("DIIMISA5", "DIIMISA5", ("SAP.PBB.MIS.ISLAMIC.FD", "SAP.PIBB.MNITB(0)"), "SAP.PIBB.DIIMISA5.TEXT"),
    ProgramStep("DIIMISA6", "DIIMISA6", ("SAP.PBB.MIS.ISLAMIC.FD", "SAP.PIBB.MNITB(0)"), "SAP.PIBB.DIIMISA6.TEXT"),
    ProgramStep("DIIMISC1", "DIIMISC1", ("SAP.PBB.MIS.ISLAMIC.FD", "SAP.PIBB.MNITB(0)"), "SAP.PIBB.DIIMISC1.TEXT"),
    ProgramStep("DIIMISC2", "DIIMISC2", ("SAP.PBB.MIS.ISLAMIC.FD", "SAP.PIBB.MNITB(0)"), "SAP.PIBB.DIIMISC2.TEXT"),
    ProgramStep("DIIMISC3", "DIIMISC3", ("SAP.PBB.MIS.ISLAMIC.FD", "SAP.PIBB.MNITB(0)"), "SAP.PIBB.DIIMISC3.TEXT"),
    ProgramStep("DIIMISC5", "DIIMISC5", ("SAP.PBB.MIS.ISLAMIC.FD", "SAP.PIBB.MNITB(0)"), "SAP.PIBB.DIIMISC5.TEXT"),
    ProgramStep("DIIMISC6", "DIIMISC6", ("SAP.PBB.MIS.ISLAMIC.FD", "SAP.PIBB.MNITB(0)"), "SAP.PIBB.DIIMISC6.TEXT"),
    ProgramStep("DIIMISC7", "DIIMISC7", ("SAP.PBB.MIS.ISLAMIC.FD", "SAP.PIBB.MNITB(0)"), "SAP.PIBB.DIIMISC7.TEXT"),
    ProgramStep("DIIMIS7Y", "DIIMIS7Y", ("SAP.PBB.MIS.ISLAMIC.FD", "SAP.PIBB.MNITB(0)"), "SAP.PIBB.DIIMIS7Y.TEXT"),
]

PRINT_STEPS = [
    PrintStep("PRINT1", "SAP.PBB.DIBMIS01.TEXT", "PRINTX"),
    PrintStep("PRINT21", "SAP.PIBB.DIIMISC7.TEXT", "PRINTX"),
    PrintStep("PRINT2", "SAP.PBB.DIBMIS02.TEXT", "PRINT2"),
    PrintStep("PRINT3", "SAP.PIBB.DIIMIS7Y.TEXT", "PRINTX"),
    PrintStep("PRINT4", "SAP.PBB.DIBMISA2.TEXT", "PRINTX"),
    PrintStep("PRINT5", "SAP.PBB.DIBMISA3.TEXT", "PRINTX"),
    PrintStep("PRINT7", "SAP.PBB.EIBMISWT.TEXT", "PRINTX"),
    PrintStep("PRINT8", "SAP.PIBB.DIIMISB2.TEXT", "PRINTX"),
    PrintStep("PRINT9", "SAP.PIBB.DIIMISA2.TEXT", "PRINTX"),
    PrintStep("PRINT10", "SAP.PIBB.DIIMISA3.TEXT", "PRINTX"),
    PrintStep("PRINT12", "SAP.PIBB.DIIMISA5.TEXT", "PRINTX"),
    PrintStep("PRINT13", "SAP.PIBB.DIIMISA6.TEXT", "PRINTX"),
    PrintStep("PRINT14", "SAP.PIBB.DIIMISC1.TEXT", "PRINTX"),
    PrintStep("PRINT15", "SAP.PIBB.DIIMISC2.TEXT", "PRINTX"),
    PrintStep("PRINT16", "SAP.PIBB.DIIMISC3.TEXT", "PRINTX"),
    PrintStep("PRINT17", "SAP.PIBB.DIIMISC5.TEXT", "PRINTX"),
    PrintStep("PRINT18", "SAP.PIBB.DIIMISC6.TEXT", "PRINTX"),
]


def dataset_to_txt_path(dataset_name: str) -> Path:
    return REPORT_DIR / f"{dataset_name.replace('.', '_')}.txt"


def ensure_directories() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    PRINT_DIR.mkdir(parents=True, exist_ok=True)


def validate_parquet_inputs(required_datasets: tuple[str, ...]) -> None:
    con = duckdb.connect()
    try:
        for dataset in required_datasets:
            parquet_path = DATASET_TO_PATH.get(dataset)
            if parquet_path is None:
                continue
            if not parquet_path.exists():
                raise FileNotFoundError(f"Missing parquet input for {dataset}: {parquet_path}")

            # Quick validation through DuckDB + Polars materialization.
            query = f"SELECT * FROM read_parquet('{parquet_path.as_posix()}') LIMIT 1"
            sample_df = con.execute(query).pl()
            _ = sample_df.select(pl.all())
    finally:
        con.close()


def delete_prior_outputs() -> None:
    for dataset in DELETE_DATASETS:
        txt_path = dataset_to_txt_path(dataset)
        dat_path = txt_path.with_suffix(".dat")
        if txt_path.exists():
            txt_path.unlink()
        if dat_path.exists():
            dat_path.unlink()


def run_program_step(step: ProgramStep) -> None:
    validate_parquet_inputs(step.input_datasets)

    script_path = PROGRAM_DIR / f"{step.sysin_program}.py"
    output_path = dataset_to_txt_path(step.output_dataset)

    # Placeholder for original dependency linkage from JCL SYSIN members.
    # If dependent program is unavailable, this raises a clear runtime error.
    if not script_path.exists():
        raise FileNotFoundError(
            f"Dependent program not found: {script_path}. "
            f"(Original SYSIN member: SAP.BNM.PROGRAM({step.sysin_program}))"
        )

    env = os.environ.copy()
    env["EIB_SYSIN_PROGRAM"] = step.sysin_program
    env["EIB_OUTPUT_PATH"] = str(output_path)

    subprocess.run([sys.executable, str(script_path)], env=env, check=True)


def execute_print_step(step: PrintStep) -> None:
    src_path = dataset_to_txt_path(step.source_dataset)
    if not src_path.exists():
        raise FileNotFoundError(f"Print source does not exist: {src_path}")

    target_path = PRINT_DIR / f"{step.step_name}_{step.output_tag}.txt"
    shutil.copyfile(src_path, target_path)


def main() -> int:
    ensure_directories()
    delete_prior_outputs()

    for step in PROGRAM_STEPS:
        run_program_step(step)

    for step in PRINT_STEPS:
        execute_print_step(step)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
