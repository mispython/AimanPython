# !/usr/bin/env python3
"""
Program: EIAWOF06
Purpose: Driver program to execute EIFMNP07 for NPL Asset Quality Statistics

Department: HP CREDIT CONTROL DEPARTMENT
Contact: CIK ROSEDAH
Location: MENARA PBB, 22TH FLOOR
Address: MENARA PUBLIC BANK, 146 JALAN AMPANG, 50450 KUALA LUMPUR

Note: This is a wrapper program that calls EIFMNP07
"""

import sys
from pathlib import Path

# Add current directory to path to import EIFMNP07
sys.path.insert(0, str(Path(__file__).parent))

import EIFMNP07

# Report header information
REPORT_INFO = {
    "NAME": "CIK ROSEDAH",
    "ROOM": "22TH FLOOR",
    "BUILDING": "MENARA PBB",
    "DEPT": "HP CREDIT CONTROL DEPARTMENT",
    "ADDRESS": ["MENARA PUBLIC BANK", "146 JALAN AMPANG", "50450 KUALA LUMPUR"]
}


def main():
    """
    Main execution function for EIAWOF06.
    This program serves as a wrapper to execute EIFMNP07.
    """
    print("=" * 80)
    print("EIAWOF06 - NPL Asset Quality Statistics Wrapper")
    print("=" * 80)
    print(f"Department: {REPORT_INFO['DEPT']}")
    print(f"Contact: {REPORT_INFO['NAME']}")
    print(f"Location: {REPORT_INFO['BUILDING']}, {REPORT_INFO['ROOM']}")
    for addr in REPORT_INFO['ADDRESS']:
        print(f"         {addr}")
    print("=" * 80)
    print()
    print("Executing EIFMNP07 program...")
    print()

    # Execute EIFMNP07
    try:
        EIFMNP07.main()
        print()
        print("=" * 80)
        print("EIAWOF06 execution completed successfully")
        print("=" * 80)
    except Exception as e:
        print()
        print("=" * 80)
        print(f"ERROR: EIAWOF06 execution failed: {str(e)}")
        print("=" * 80)
        raise


if __name__ == "__main__":
    main()
