# !/usr/bin/env python3
"""
Program: LNCCDR10
Date: 01.07.98
Function: Details for NPL accounts for CCD PFB (A monthly report)
Modified (ESMR): 2006-1048
"""

import polars as pl
from pathlib import Path

INPUT_PATH_LOANTEMP = "BNM_LOANTEMP.parquet"
INPUT_PATH_BRANCH = "RBP2_B033_PBB_BRANCH.parquet"
OUTPUT_PATH_OUTFILE = "SAP_PBB_NPL_OUTFILE_RPS.txt"
LRECL_RPS = 134

ARRCLASS_MAP = {1: 'CURRENT', 2: '1 MTH', 3: '2 MTHS', 4: '3 MTHS', 5: '4 MTHS',
                6: '5 MTHS', 7: '6 MTHS', 8: '7 MTHS', 9: '8 MTHS', 10: '9-11 MTHS',
                11: '1-<2 YRS', 12: '2-<3 YRS', 13: '3-<4 YRS', 14: '>4 YRS', 15: 'DEFICIT'}

def format_asa_line(line_content, carriage_control=' '):
    return f"{carriage_control}{line_content}"

def process_lnccdr10(rdate):
    """This program is commented out in original EIFMNPLP"""
    print("LNCCDR10 - This program is commented out in original job")
    # /* %INC PGM(LNCCDR10);
    # PROC DATASETS LIB=WORK KILL NOLIST; RUN;  */
    pass

if __name__ == "__main__":
    rdate = "22/02/24"
    process_lnccdr10(rdate)
