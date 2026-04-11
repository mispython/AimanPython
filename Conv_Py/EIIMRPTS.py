#!/usr/bin/env python3
"""
Program : EIIMRPTS.py
Purpose : Master driver converted from SAS JCL program EIIMRPTS.
          Orchestrates report-generation programs and pre-run file handling.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable


from EIIMRM01 import main as run_eiimrm01
from EIIMRM02 import main as run_eiimrm02
from EIIMRM03 import main as run_eiimrm03
from EIIMRM04 import main as run_eiimrm04
from EIIWSTAF import main as run_eiiwstaf
from EIIMLN03 import main as run_eiimln03
from EIFMLN03 import main as run_eifmln03


# ============================================================================
# PATH CONFIGURATION (defined early)
# ============================================================================
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# JCL pre-created output datasets mapped to local text outputs.
OUTPUT_FILES = {
    "EIIMRM01": OUTPUT_DIR / "EIIMRM01.txt",
    "EIIMRM02": OUTPUT_DIR / "EIIMRM02.txt",
    "EIIMRM03": OUTPUT_DIR / "EIIMRM03.txt",
    "EIIMRM04": OUTPUT_DIR / "EIIMRM04.txt",
    "EIBWSTAF": OUTPUT_DIR / "EIBWSTAF.txt",
    "EIIMLN03": OUTPUT_DIR / "EIIMLN03.txt",
    "M4LOAN": OUTPUT_DIR / "M4LOAN.txt",
    "EIFMLN03": OUTPUT_DIR / "EIFMLN03.txt",
}


def reset_output_files() -> None:
    """Replicate JCL DELETE + CREATE steps by recreating expected output files."""
    for file_path in OUTPUT_FILES.values():
        if file_path.exists():
            file_path.unlink()
        file_path.touch()


def run_step(step_name: str, step_func: Callable[[], None]) -> None:
    """Run one converted SAS step in sequence."""
    print(f"[EIIMRPTS] Running {step_name}...")
    step_func()
    print(f"[EIIMRPTS] Completed {step_name}.")


def main() -> None:
    """Run the full EIIMRPTS chain in original execution order."""
    reset_output_files()

    run_step("EIIMRM01", run_eiimrm01)
    run_step("EIIMRM02", run_eiimrm02)
    run_step("EIIMRM03", run_eiimrm03)
    run_step("EIIMRM04", run_eiimrm04)
    run_step("EIIWSTAF", run_eiiwstaf)
    run_step("EIIMLN03", run_eiimln03)
    run_step("EIFMLN03", run_eifmln03)

    print("[EIIMRPTS] All programs completed successfully.")


if __name__ == "__main__":
    main()
