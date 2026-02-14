# !/usr/bin/env python3
"""
Program: EIAWOF03
Purpose: Driver program to execute EIFMNP02 for NPL account preparation
         Original from EIFMNPLE

Note: This is a wrapper program that calls EIFMNP02
      Prepares NPL accounts for new and old guidelines
"""

import sys
from pathlib import Path

# Add current directory to path to import EIFMNP02
sys.path.insert(0, str(Path(__file__).parent))

import EIFMNP02


def main():
    """
    Main execution function for EIAWOF03.
    This program serves as a wrapper to execute EIFMNP02.
    """
    print("=" * 80)
    print("EIAWOF03 - NPL Account Preparation Wrapper")
    print("=" * 80)
    print("Original from EIFMNPLE")
    print("Prepares NPL accounts for new and old guidelines")
    print("=" * 80)
    print()
    print("Executing EIFMNP02 program...")
    print()

    # Execute EIFMNP02
    try:
        EIFMNP02.main()
        print()
        print("=" * 80)
        print("EIAWOF03 execution completed successfully")
        print("=" * 80)
    except Exception as e:
        print()
        print("=" * 80)
        print(f"ERROR: EIAWOF03 execution failed: {str(e)}")
        print("=" * 80)
        raise


if __name__ == "__main__":
    main()
