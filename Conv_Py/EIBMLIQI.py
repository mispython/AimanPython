#!/usr/bin/env python3
"""
Program : EIBMLIQI.py
Purpose : JCL orchestrator for the Islamic New Liquidity Framework (FISS)
          reporting job. Mirrors the original JCL sequence:
              %INC PGM(DALWPBBD);
              %INC PGM(EIBMRLFI);
          by importing DALWPBBD (module-level execution builds
          BNM_SAVG / BNM_CURN / BNM_DEPT) and then invoking
          EIBMRLFI.main(), which drives KALMLIQI internally.

Dependency:
    DALWPBBD.py -> already converted; imported to trigger its module-level
                   BNM_SAVG / BNM_CURN construction.
    EIBMRLFI.py -> imported for its main() entry point, which internally
                   calls KALMLIQI.main().

JCL COND=(4,LT) equivalent: any unhandled exception in the job stream is
caught here and results in a non-zero (RC=4) process exit.
"""

import sys

import DALWPBBD    # %INC PGM(DALWPBBD) equivalent -- runs at import time
import EIBMRLFI    # %INC PGM(EIBMRLFI) equivalent


def main() -> int:
    print("EIBMLIQI: Starting Islamic New Liquidity Framework job stream...")
    try:
        EIBMRLFI.main()
    except Exception as exc:
        print(f"EIBMLIQI: job failed - {exc}")
        return 4
    print("EIBMLIQI: Job completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
