#!/usr/bin/env python3
"""
Program: EIAWOF13
Purpose: JCL driver conversion that executes EIFMNP03, EIFMNP06, and EIFMNP07 in sequence.

Original JCL intent:
- Submit three SAS steps against the same NPL library.
- Halt downstream execution when an earlier step fails (COND=(0,LT) behavior).
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

# -----------------------------------------------------------------------------
# Path setup (kept near top as requested)
# -----------------------------------------------------------------------------
PROGRAM_DIR = Path(__file__).resolve().parent
REPO_ROOT = PROGRAM_DIR.parent
INPUT_DIR = REPO_ROOT / "input"
OUTPUT_DIR = REPO_ROOT / "output"
LOG_DIR = REPO_ROOT / "logs"

if str(PROGRAM_DIR) not in sys.path:
    sys.path.insert(0, str(PROGRAM_DIR))

import importlib


@dataclass(frozen=True)
class JobStep:
    """Single executable step from the original JCL flow."""

    name: str
    module_name: str

    def run(self) -> None:
        module = importlib.import_module(self.module_name)
        module.main()


EXECUTION_STEPS: tuple[JobStep, ...] = (
    JobStep(name="EIFMNP03", module_name="EIFMNP03"),
    JobStep(name="EIFMNP06", module_name="EIFMNP06"),
    JobStep(name="EIFMNP07", module_name="EIFMNP07"),
)


def run_steps(dry_run: bool = False) -> None:
    """Run all configured steps in order; abort immediately on first error."""

    for step in EXECUTION_STEPS:
        print(f"[EIAWOF13] Starting step {step.name}")
        if dry_run:
            print(f"[EIAWOF13] Dry-run enabled: skipped execution for {step.name}")
            continue

        # Fail-fast behavior intentionally mirrors JCL step dependency.
        step.run()
        print(f"[EIAWOF13] Completed step {step.name}")


def parse_args() -> argparse.Namespace:
    """Parse CLI options for operational control and quick validation."""

    parser = argparse.ArgumentParser(
        description=(
            "Execute EIAWOF13 job flow (EIFMNP03 -> EIFMNP06 -> EIFMNP07) "
            "with fail-fast semantics."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate orchestration order without running underlying programs.",
    )
    return parser.parse_args()


def main() -> None:
    """Entrypoint for the converted EIAWOF13 driver."""

    args = parse_args()
    run_steps(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
