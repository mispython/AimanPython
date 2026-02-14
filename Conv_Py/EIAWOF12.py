# !/usr/bin/env python3
"""
Program: EIAWOF12
Purpose: Driver program to execute EIFMNP02 for NPL account preparation
         Uses DAILY loan data instead of monthly snapshot

Note: This is a wrapper program that calls EIFMNP02
      Similar to EIAWOF03 but uses MNILN.DAILY dataset
"""

import sys
from pathlib import Path

# Add current directory to path to import EIFMNP02
sys.path.insert(0, str(Path(__file__).parent))

import EIFMNP02


def main():
    """
    Main execution function for EIAWOF12.
    This program serves as a wrapper to execute EIFMNP02.
    Uses daily loan data for NPL preparation.
    """
    print("=" * 80)
    print("EIAWOF12 - NPL Account Preparation (Daily Data)")
    print("=" * 80)
    print("Prepares NPL accounts using daily loan data")
    print("Executes EIFMNP02")
    print("=" * 80)
    print()
    print("Executing EIFMNP02 program with daily data...")
    print()

    # Execute EIFMNP02
    try:
        EIFMNP02.main()
        print()
        print("=" * 80)
        print("EIAWOF12 execution completed successfully")
        print("=" * 80)
    except Exception as e:
        print()
        print("=" * 80)
        print(f"ERROR: EIAWOF12 execution failed: {str(e)}")
        print("=" * 80)
        raise


if __name__ == "__main__":
    main()
