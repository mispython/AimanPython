#!/usr/bin/env python3
"""
Program: PBBELF
Format definitions and lookup tables
Used across BNM regulatory reporting programs
"""

from typing import Dict, List, Any, Optional

# ============================================================================
# EL AND ELI BNMCODE DEFINITIONS
# ============================================================================

EL_DEFINITIONS = [
    {'bnmcode': '4211000000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM DEMAND DEPOSITS ACCEPTED'},
    {'bnmcode': '4212000000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM SAVINGS DEPOSITS ACCEPTED'},
    {'bnmcode': '4213000000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM FIXED DEPOSITS ACCEPTED'},
    {'bnmcode': '4213100000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM SPECIAL INVESTMENT DEPOSIT ACCEPTED'},
    {'bnmcode': '4213200000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM GENERAL INVESTMENT DEPOSIT ACCEPTED'},
    {'bnmcode': '4213300000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM COMMODITY MURABAHAH'},
    {'bnmcode': '4215000000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM NID ISSUED'},
    {'bnmcode': '4216000000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM REPURCHASE AGREEMENTS'},
    {'bnmcode': '4217071000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM SPECIAL DEPOSITS'},
    {'bnmcode': '4218000000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM HOUSING DEVELOPMENT ACCOUNTS'},
    {'bnmcode': '4219000000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM SHORT TERM DEPOSIT ACCEPTED'},
    {'bnmcode': '4219100000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM INVESTMENT LINKED TO DERIVATIVES'},
    {'bnmcode': '4219900000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM OTHER DEPOSITS ACCEPTED'},
    {'bnmcode': '4310000000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM AMOUNT DUE TO DESIGNATED FI'},
    {'bnmcode': '4311002000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM VOSTRO ACCOUNTS OF CB'},
    {'bnmcode': '4311003000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM VOSTRO ACCOUNTS OF IB'},
    {'bnmcode': '4311081000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM VOSTRO ACCOUNTS OF FBI'},
    {'bnmcode': '4312002000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM OVERDRAWN NOSTRO ACCOUNTS WITH CB'},
    {'bnmcode': '4312003000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM OVERDRAWN NOSTRO ACCOUNTS WITH IB'},
    {'bnmcode': '4313000000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM DEFICIT IN SPICK'},
    {'bnmcode': '4313002000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM AMOUNT BORROWING FROM SPICK POOL CB'},
    {'bnmcode': '4313003000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM AMOUNT BORROWING FROM SPICK POOL IB'},
    {'bnmcode': '4314001000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM INTERBANK BORROWINGS FROM BNM'},
    {'bnmcode': '4314002000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM INTERBANK BORROWINGS FROM CB'},
    {'bnmcode': '4314011000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM INTERBANK BORROWINGS FROM FC'},
    {'bnmcode': '4314012000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM INTERBANK BORROWINGS FROM MB'},
    {'bnmcode': '4314013000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM INTERBANK BORROWINGS FROM DH'},
    {'bnmcode': '4314017000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM INTERBANK BORROWINGS FROM CAGAMAS'},
    {'bnmcode': '4314020000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM INTERBANK BORROWINGS FROM DNBFI'},
    {'bnmcode': '4314081100000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM INTERBANK BORROWINGS FROM FBI <= 1 YR'},
    {'bnmcode': '4314003000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM INTERBANK BORROWINGS FROM IB'},
    {'bnmcode': '4410000000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM MISC BORROWINGS'},
    {'bnmcode': '4911080000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM INTEREST PAYABLE TO NON-RESIDENTS'},
    {'bnmcode': '4911095000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM INTEREST PAYABLE TO NON-RES - DCI/CRA'},
    {'bnmcode': '4929996000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'OTHR RM MISC LIAB NIE DUE TO NON-RES-DCI'},
    {'bnmcode': '4912080000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM BILLS PAYABLE TO NON-RESIDENTS'},
    {'bnmcode': '4929980000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'OTHER RM MISC LIAB NIE DUE TO NON-RES'},
    {'bnmcode': '4929995000000Y', 'sign': '+', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM GOLD INVESTMENT FROM NON-RESIDENTS'},
    {'bnmcode': '4411100000000Y', 'sign': '-', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM SUBORDINATED DEBT CAPITAL'},
    {'bnmcode': '4411200000000Y', 'sign': '-', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM EXEMPT SUBORDINATED DEBT CAPITAL'},
    {'bnmcode': '4411300000000Y', 'sign': '-', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM SUBORDIN DEBT CAPITAL W APPR FR BNM'},
    {'bnmcode': '4414000000000Y', 'sign': '-', 'fmtname': 'RMEL', 'type': 'C', 'idx': 'A', 'desc': 'RM RESOURCE OBLIQ ON LN SOLD TO CAGAMAS'},
    {'bnmcode': '4260000000000Y', 'sign': '+', 'fmtname': 'FXEL', 'type': 'C', 'idx': '', 'desc': 'FX DEPOSITS ACCEPTED'},
    {'bnmcode': '4269981000000Y', 'sign': '+', 'fmtname': 'FXEL', 'type': 'C', 'idx': '', 'desc': 'FX DEPOSITS ACCEPTED TO BNM'},
    {'bnmcode': '4360000000000Y', 'sign': '+', 'fmtname': 'FXEL', 'type': 'C', 'idx': '', 'desc': 'FX AMOUNT DUE TO DESIGNATED FI'},
    {'bnmcode': '4362081000000Y', 'sign': '+', 'fmtname': 'FXEL', 'type': 'C', 'idx': '', 'desc': 'FX OVERDRAWN NOSTRO ACCOUNTS WITH FBI'},
    {'bnmcode': '4364002000000Y', 'sign': '+', 'fmtname': 'FXEL', 'type': 'C', 'idx': '', 'desc': 'FX INTERBANK BORROWINGS FROM CB'},
    {'bnmcode': '4364003000000Y', 'sign': '+', 'fmtname': 'FXEL', 'type': 'C', 'idx': '', 'desc': 'FX INTERBANK BORROWINGS FROM IB'},
    {'bnmcode': '4364012000000Y', 'sign': '+', 'fmtname': 'FXEL', 'type': 'C', 'idx': '', 'desc': 'FX INTERBANK BORROWINGS FROM MB'},
    {'bnmcode': '4364081100000Y', 'sign': '+', 'fmtname': 'FXEL', 'type': 'C', 'idx': '', 'desc': 'FX INTERBANK BORROWINGS FROM FBI <= 1 YR'},
    {'bnmcode': '4370000000000Y', 'sign': '+', 'fmtname': 'FXEL', 'type': 'C', 'idx': '', 'desc': 'SPTF FX AMOUNT DUE TO FI'},
    {'bnmcode': '4460000000000Y', 'sign': '+', 'fmtname': 'FXEL', 'type': 'C', 'idx': '', 'desc': 'FX MISC BORROWINGS'},
    {'bnmcode': '4760000000000Y', 'sign': '+', 'fmtname': 'FXEL', 'type': 'C', 'idx': '', 'desc': 'FX DEBT SECURITIES ISSUED'},
    {'bnmcode': '4961050000000Y', 'sign': '+', 'fmtname': 'FXEL', 'type': 'C', 'idx': '', 'desc': 'FX INTEREST PAYABLE TO RESIDENTS'},
    {'bnmcode': '4961080000000Y', 'sign': '+', 'fmtname': 'FXEL', 'type': 'C', 'idx': '', 'desc': 'FX INTEREST PAYABLE TO NON-RESIDENTS'},
    {'bnmcode': '4969950000000Y', 'sign': '+', 'fmtname': 'FXEL', 'type': 'C', 'idx': '', 'desc': 'OTHER FX MISC LIAB NIE DUE TO RESIDENTS'},
    {'bnmcode': '4969980000000Y', 'sign': '+', 'fmtname': 'FXEL', 'type': 'C', 'idx': '', 'desc': 'OTHER FX MISC LIAB NIE DUE TO NON-RES'},
    {'bnmcode': '4461100000000Y', 'sign': '-', 'fmtname': 'FXEL', 'type': 'C', 'idx': '', 'desc': 'FX SUBORDINATED DEBT CAPITAL'},
    {'bnmcode': '3311003000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM OVERDRAWN VOSTRO ACCOUNTS OF IB'},
    {'bnmcode': '3212002000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM BALANCES IN CURRENT ACCOUNTS WITH CB'},
    {'bnmcode': '3212003000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM BALANCES IN CURRENT ACCOUNTS WITH IB'},
    {'bnmcode': '3213002000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM FIXED DEPOSITS PLACED WITH CB'},
    {'bnmcode': '3213011000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM FIXED DEPOSITS PLACED WITH FC'},
    {'bnmcode': '3213012000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM FIXED DEPOSITS PLACED WITH MB'},
    {'bnmcode': '3213013000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM FIXED DEPOSITS PLACED WITH DH'},
    {'bnmcode': '3213102000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM SPECIAL INV DEP PLACED WITH CB'},
    {'bnmcode': '3213103000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM SPECIAL INV DEP PLACED WITH IB'},
    {'bnmcode': '3213111000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM SPECIAL INV DEP PLACED WITH FC'},
    {'bnmcode': '3213112000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM SPECIAL INV DEP PLACED WITH MB'},
    {'bnmcode': '3213113000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM SPECIAL INV DEP PLACED WITH DH'},
    {'bnmcode': '3213202000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM GEN INVESTMENT DEP PLACED WITH CB'},
    {'bnmcode': '3213203000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM GEN INVESTMENT DEP PLACED WITH IB'},
    {'bnmcode': '3213211000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM GEN INVESTMENT DEP PLACED WITH FC'},
    {'bnmcode': '3213212000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM GEN INVESTMENT DEP PLACED WITH MB'},
    {'bnmcode': '3213213000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM GEN INVESTMENT DEP PLACED WITH DH'},
    {'bnmcode': '3219910000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM OTHER DEPOSITS PLACED WITH DBI'},
    {'bnmcode': '3250002000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM REVERSE REPOS WITH CB'},
    {'bnmcode': '3250001000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM REVERSE REPOS WITH BNM'},
    {'bnmcode': '3250011000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM REVERSE REPOS WITH FC'},
    {'bnmcode': '3250012000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM REVERSE REPOS WITH MB'},
    {'bnmcode': '3250013000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM REVERSE REPOS WITH DH'},
    {'bnmcode': '3311002000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM OVERDRAWN VOSTRO ACCOUNTS OF CB'},
    {'bnmcode': '3312002000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM NOSTRO ACCOUNT BALANCES WITH CB'},
    {'bnmcode': '3312003000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM NOSTRO ACCOUNT BALANCES WITH IB'},
    {'bnmcode': '3313000000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM SURPLUS IN SPICK'},
    {'bnmcode': '3314001000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM INTERBANK PLACEMENTS WITH BNM'},
    {'bnmcode': '3314002000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM INTERBANK PLACEMENTS WITH CB'},
    {'bnmcode': '3314003000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM INTERBANK PLACEMENTS WITH IB'},
    {'bnmcode': '3314011000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM INTERBANK PLACEMENTS WITH FC'},
    {'bnmcode': '3314012000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM INTERBANK PLACEMENTS WITH MB'},
    {'bnmcode': '3314013000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM INTERBANK PLACEMENTS WITH DH'},
    {'bnmcode': '3314017000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM INTERBANK PLACEMENTS WITH CAGAMAS'},
    {'bnmcode': '3410002000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM LOANS TO CB'},
    {'bnmcode': '3410003000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM LOANS TO IB'},
    {'bnmcode': '3410011000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM LOANS TO FC'},
    {'bnmcode': '3410012000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM LOANS TO MB'},
    {'bnmcode': '3410013000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM LOANS TO DH'},
    {'bnmcode': '3410017000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM LOANS TO CAGAMAS'},
    {'bnmcode': '3703000000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM NIDS HELD'},
    {'bnmcode': '3803000000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'NIDS SOLD UNDER REPO'},
    {'bnmcode': '4015000000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'ELIGIBLE CAGAMAS TIER-2 BONDS (DAY I)'},
    {'bnmcode': '4019000000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'SRGF LOANS'},
    {'bnmcode': '4019100000000Y', 'sign': '+', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'SRGF-2 LOANS'},
    {'bnmcode': '3314013110000Y', 'sign': '-', 'fmtname': 'RMEA', 'type': 'C', 'idx': 'B', 'desc': 'RM INTERBKS PLACEMENTS WITH DH OVRNIGHT'},
    {'bnmcode': '4014000000000Y', 'sign': '+', 'fmtname': 'RMET', 'type': 'C', 'idx': 'C', 'desc': 'ELIGIBLE TIER 2 LOANS SOLD TO CAGAMAS'},
    {'bnmcode': '4017100000000Y', 'sign': '-', 'fmtname': 'RMMS', 'type': 'C', 'idx': 'D', 'desc': 'TOTAL RM MARKETABLE SECURITIES'},
    {'bnmcode': '3260000000000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'FX DEPOSITS PLACED'},
    {'bnmcode': '3280000000000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'FX REVERSE REPOS'},
    {'bnmcode': '3362081000000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'FX NOSTRO ACCOUNT BALANCES WITH FBI'},
    {'bnmcode': '3364002000000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'FX INTERBANK PLACEMENTS WITH CB'},
    {'bnmcode': '3364003000000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'FX INTERBANK PLACEMENTS WITH IB'},
    {'bnmcode': '3364012000000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'FX INTERBANK PLACEMENTS WITH MB'},
    {'bnmcode': '3364081100000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'FX INTERBANK PLACEMENTS WITH FBI <= 1 YR'},
    {'bnmcode': '3370000000000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'SPTF - FX AMOUNT DUE FROM DESIGNATED FI'},
    {'bnmcode': '3460000000000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'FX LOANS'},
    {'bnmcode': '3460064000000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'FX LOANS GOV DBE'},
    {'bnmcode': '3460081000000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'FX LOANS FBI'},
    {'bnmcode': '3761000000000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'FX SHARES HELD'},
    {'bnmcode': '3765000200000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'FX SECURITIES HELD (OLD=37600)'},
    {'bnmcode': '3769900000000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'FX SECURITIES HELD (OLD=37600)'},
    {'bnmcode': '3961000000000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'FX NOTES AND COINS'},
    {'bnmcode': '3961100000000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'FX INVESTMENTS'},
    {'bnmcode': '3962000000000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'FOREIGN SUBSIDIARIES'},
    {'bnmcode': '3963000000000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'FOREIGN ASSOCIATE COMPANIES'},
    {'bnmcode': '3966900000000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'FX INVESTMENT IN LABUAN OFFSHORE ENTITY'},
    {'bnmcode': '3967000000000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'FX MARGIN PLACED WITH EXCHANGES'},
    {'bnmcode': '3968000000000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'FX INTEREST RECEIVABLES NIE'},
    {'bnmcode': '3969900000000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'FX OTHER ASSETS NIE'},
    {'bnmcode': '3979900000000Y', 'sign': '+', 'fmtname': 'FXEA', 'type': 'C', 'idx': '', 'desc': 'SPTF OTHER FX MISC LIAB NIE'},
    {'bnmcode': 'NSSTS', 'sign': '', 'fmtname': '', 'type': 'C', 'idx': '', 'desc': '1. NSSTS'},
    {'bnmcode': 'SSTS', 'sign': '', 'fmtname': '', 'type': 'C', 'idx': '', 'desc': 'SSTS'},
    {'bnmcode': 'NSSTS TRADING', 'sign': '', 'fmtname': '', 'type': 'C', 'idx': '', 'desc': '1.2 NSSTS TRADING'},
    {'bnmcode': 'NSSTS INVEST', 'sign': '', 'fmtname': '', 'type': 'C', 'idx': '', 'desc': '1.1 NSSTS INVESTMENT'},
]

# ELI has same structure as EL
ELI_DEFINITIONS = EL_DEFINITIONS.copy()

# ============================================================================
# BRANCH CODE FORMATS
# ============================================================================

# Branch code to branch name mapping
BRCHCD_MAP = {
    1: 'HOE', 7000: 'HOE', 7001: 'HOE', 7002: 'HOE', 7003: 'HOE', 7004: 'HOE',
    7005: 'HOE', 7006: 'HOE', 7007: 'HOE', 7008: 'HOE', 7009: 'HOE',
    8000: 'HOE', 8001: 'HOE', 8002: 'HOE', 8003: 'HOE', 8004: 'HOE',
    8005: 'HOE', 8006: 'HOE', 8007: 'HOE', 8008: 'HOE', 8009: 'HOE',
    9000: 'HOE', 9001: 'HOE', 9002: 'HOE', 9003: 'HOE', 9004: 'HOE',
    9994: 'HOE', 9995: 'HOE', 9996: 'HOE', 9998: 'HOE', 9999: 'HOE',
    3000: 'IBU', 3001: 'IBU', 3999: 'IBU',
    4000: 'IBU', 4001: 'IBU', 4002: 'IBU', 4003: 'IBU', 4004: 'IBU',
    4005: 'IBU', 4006: 'IBU', 4007: 'IBU', 4008: 'IBU', 4009: 'IBU',
    800: 'H01', 3800: 'H01',
    801: 'H02', 3801: 'H02',
    802: 'H03', 3802: 'H03',
    803: 'H04', 3803: 'H04',
    804: 'H05', 3804: 'H05',
    805: 'H06', 3805: 'H06',
    806: 'H07', 3806: 'H07',
    807: 'H08', 3807: 'H08',
    808: 'H09', 3808: 'H09',
    809: 'H10', 3809: 'H10',
    811: 'H11', 3811: 'H11',
    812: 'H12', 3812: 'H12',
    813: 'H13', 3813: 'H13',
    814: 'H14', 3814: 'H14',
    815: 'H15', 3815: 'H15',
    816: 'H16', 3816: 'H16',
    817: 'H17', 3817: 'H17',
    818: 'H18', 3818: 'H18',
    819: 'H19', 3819: 'H19',
    820: 'H20', 3820: 'H20',
    821: 'H21', 3821: 'H21',
    822: 'H22', 3822: 'H22',
    823: 'H23', 3823: 'H23',
    824: 'H24', 3824: 'H24',
    825: 'H25', 3825: 'H25',
    826: 'H26', 3826: 'H26',
    827: 'H27', 3827: 'H27',
    828: 'H28', 3828: 'H28',
    844: 'H44', 3844: 'H44',
    845: 'H45', 3845: 'H45',
    846: 'H46', 3846: 'H46',
    847: 'H47', 3847: 'H47',
    848: 'H48', 3848: 'H48',
    849: 'H49', 3849: 'H49',
    850: 'H50', 3850: 'H50',
    851: 'H51', 3851: 'H51',
    852: 'H52', 3852: 'H52',
    853: 'H53', 3853: 'H53',
    854: 'H54', 3854: 'H54',
    855: 'H55', 3855: 'H55',
    856: 'H56', 3856: 'H56',
    857: 'H57', 3857: 'H57',
    858: 'H58', 3858: 'H58',
    859: 'H59', 3859: 'H59',
    860: 'H60', 3860: 'H60',
    861: 'H61', 3861: 'H61',
    862: 'H62', 3862: 'H62',
    863: 'H63', 3863: 'H63',
    2: 'JSS', 3002: 'JSS',
    3: 'JRC', 3003: 'JRC',
    4: 'MLK', 3004: 'MLK',
    5: 'IMO', 3005: 'IMO',
    6: 'PPG', 3006: 'PPG',
    7: 'JBU', 3007: 'JBU',
    8: 'KTN', 3008: 'KTN',
    9: 'JYK', 3009: 'JYK',
    10: 'ASR', 3010: 'ASR',
    11: 'GRN', 3011: 'GRN',
    12: 'PPH', 3012: 'PPH',
    13: 'KBU', 3013: 'KBU',
    14: 'TMH', 3014: 'TMH',
    15: 'KPG', 3015: 'KPG',
    16: 'NLI', 3016: 'NLI',
    17: 'TPN', 3017: 'TPN',
    18: 'PJN', 3018: 'PJN',
    19: 'DUA', 3019: 'DUA',
    20: 'TCL', 3020: 'TCL',
    21: 'BPT', 3021: 'BPT',
    22: 'SMY', 3022: 'SMY',
    23: 'KMT', 3023: 'KMT',
    24: 'RSH', 3024: 'RSH',
    25: 'SAM', 3025: 'SAM',
    26: 'SPG', 3026: 'SPG',
    27: 'NTL', 3027: 'NTL',
    28: 'MUA', 3028: 'MUA',
    29: 'JRL', 3029: 'JRL',
    30: 'KTU', 3030: 'KTU',
    31: 'SKC', 3031: 'SKC',
    32: 'WSS', 3032: 'WSS',
    33: 'KKU', 3033: 'KKU',
    34: 'KGR', 3034: 'KGR',
    35: 'SSA', 3035: 'SSA',
    36: 'SS2', 3036: 'SS2',
    37: 'TSA', 3037: 'TSA',
    38: 'JKL', 3038: 'JKL',
    39: 'KKG', 3039: 'KKG',
    40: 'JSB', 3040: 'JSB',
    41: 'JIH', 3041: 'JIH',
    42: 'BMM', 3042: 'BMM',
    43: 'BTG', 3043: 'BTG',
    44: 'TWU', 3044: 'TWU',
    45: 'SRB', 3045: 'SRB',
    46: 'APG', 3046: 'APG',
    47: 'SGM', 3047: 'SGM',
    48: 'MTK', 3048: 'MTK',
    49: 'JLP', 3049: 'JLP',
    50: 'MRI', 3050: 'MRI',
    51: 'SMG', 3051: 'SMG',
    52: 'UTM', 3052: 'UTM',
    53: 'TMI', 3053: 'TMI',
    54: 'BBB', 3054: 'BBB',
    55: 'LBN', 3055: 'LBN',
    56: 'KJG', 3056: 'KJG',
    57: 'SPI', 3057: 'SPI',
    58: 'SBU', 3058: 'SBU',
    59: 'PKL', 3059: 'PKL',
    60: 'BAM', 3060: 'BAM',
    61: 'KLI', 3061: 'KLI',
    62: 'SDK', 3062: 'SDK',
    63: 'GMS', 3063: 'GMS',
    64: 'PDN', 3064: 'PDN',
    65: 'BHU', 3065: 'BHU',
    66: 'BDA', 3066: 'BDA',
    67: 'CMR', 3067: 'CMR',
    68: 'SAT', 3068: 'SAT',
    69: 'BKI', 3069: 'BKI',
    70: 'PSA', 3070: 'PSA',
    71: 'BCG', 3071: 'BCG',
    72: 'PPR', 3072: 'PPR',
    73: 'SPK', 3073: 'SPK',
    74: 'SIK', 3074: 'SIK',
    75: 'CAH', 3075: 'CAH',
    76: 'PRS', 3076: 'PRS',
    77: 'PLI', 3077: 'PLI',
    78: 'SJA', 3078: 'SJA',
    79: 'MSI', 3079: 'MSI',
    80: 'MLB', 3080: 'MLB',
    81: 'SBH', 3081: 'SBH',
    82: 'MCG', 3082: 'MCG',
    83: 'JBB', 3083: 'JBB',
    84: 'PMS', 3084: 'PMS',
    85: 'SST', 3085: 'SST',
    86: 'CLN', 3086: 'CLN',
    87: 'MSG', 3087: 'MSG',
    88: 'KUM', 3088: 'KUM',
    89: 'TPI', 3089: 'TPI',
    90: 'BTL', 3090: 'BTL',
    91: 'KUG', 3091: 'KUG',
    92: 'KLG', 3092: 'KLG',
    93: 'EDU', 3093: 'EDU',
    94: 'STP', 3094: 'STP',
    95: 'TIN', 3095: 'TIN',
    96: 'SGK', 3096: 'SGK',
    97: 'HSL', 3097: 'HSL',
    98: 'TCY', 3098: 'TCY',
    102: 'PRJ', 3102: 'PRJ',
    103: 'JJG', 3103: 'JJG',
    104: 'KKL', 3104: 'KKL',
    105: 'KTI', 3105: 'KTI',
    106: 'CKI', 3106: 'CKI',
    107: 'JLT', 3107: 'JLT',
    108: 'BSI', 3108: 'BSI',
    109: 'KSR', 3109: 'KSR',
    110: 'TJJ', 3110: 'TJJ',
    111: 'AKH', 3111: 'AKH',
    112: 'LDO', 3112: 'LDO',
    113: 'TML', 3113: 'TML',
    114: 'BBA', 3114: 'BBA',
    115: 'KNG', 3115: 'KNG',
    116: 'TRI', 3116: 'TRI',
    117: 'KKI', 3117: 'KKI',
    118: 'TMW', 3118: 'TMW',
    120: 'PIH', 3120: 'PIH',
    121: 'PRA', 3121: 'PRA',
    122: 'SKN', 3122: 'SKN',
    123: 'IGN', 3123: 'IGN',
    124: 'S14', 3124: 'S14',
    125: 'KJA', 3125: 'KJA',
    126: 'PTS', 3126: 'PTS',
    127: 'TSM', 3127: 'TSM',
    128: 'SGB', 3128: 'SGB',
    129: 'BSR', 3129: 'BSR',
    130: 'PDG', 3130: 'PDG',
    131: 'TMG', 3131: 'TMG',
    132: 'CKT', 3132: 'CKT',
    133: 'PKG', 3133: 'PKG',
    134: 'RPG', 3134: 'RPG',
    135: 'BSY', 3135: 'BSY',
    136: 'TCS', 3136: 'TCS',
    137: 'JPP', 3137: 'JPP',
    138: 'WMU', 3138: 'WMU',
    139: 'JRT', 3139: 'JRT',
    140: 'CPE', 3140: 'CPE',
    141: 'STL', 3141: 'STL',
    142: 'KBD', 3142: 'KBD',
    143: 'LDU', 3143: 'LDU',
    144: 'KHG', 3144: 'KHG',
    145: 'BSD', 3145: 'BSD',
    146: 'PSG', 3146: 'PSG',
    147: 'PNS', 3147: 'PNS',
    148: 'PJO', 3148: 'PJO',
    149: 'BFT', 3149: 'BFT',
    150: 'LMM', 3150: 'LMM',
    151: 'SLY', 3151: 'SLY',
    152: 'ATR', 3152: 'ATR',
    153: 'USJ', 3153: 'USJ',
    154: 'BSJ', 3154: 'BSJ',
    155: 'TTJ', 3155: 'TTJ',
    156: 'TMR', 3156: 'TMR',
    157: 'BPJ', 3157: 'BPJ',
    158: 'SPL', 3158: 'SPL',
    159: 'RLU', 3159: 'RLU',
    160: 'MTH', 3160: 'MTH',
    161: 'DGG', 3161: 'DGG',
    162: 'SEA', 3162: 'SEA',
    163: 'JKA', 3163: 'JKA',
    164: 'KBS', 3164: 'KBS',
    165: 'TKA', 3165: 'TKA',
    166: 'PGG', 3166: 'PGG',
    167: 'BBG', 3167: 'BBG',
    168: 'KLC', 3168: 'KLC',
    169: 'CTD', 3169: 'CTD',
    170: 'PJA', 3170: 'PJA',
    171: 'JMR', 3171: 'JMR',
    172: 'TMJ', 3172: 'TMJ',
    173: 'SCA', 3173: 'SCA',
    174: 'BBP', 3174: 'BBP',
    175: 'LBG', 3175: 'LBG',
    176: 'TPG', 3176: 'TPG',
    177: 'JRU', 3177: 'JRU',
    178: 'MIN', 3178: 'MIN',
    179: 'OUG', 3179: 'OUG',
    180: 'KBG', 3180: 'KBG',
    181: 'SRO', 3181: 'SRO',
    182: 'JPU', 3182: 'JPU',
    183: 'JCL', 3183: 'JCL',
    184: 'JPN', 3184: 'JPN',
    185: 'KCY', 3185: 'KCY',
    186: 'JTZ', 3186: 'JTZ',
    188: 'PLT', 3188: 'PLT',
    189: 'BNH', 3189: 'BNH',
    190: 'BTR', 3190: 'BTR',
    191: 'KPT', 3191: 'KPT',
    192: 'MRD', 3192: 'MRD',
    193: 'MKH', 3193: 'MKH',
    194: 'SRK', 3194: 'SRK',
    195: 'BWK', 3195: 'BWK',
    196: 'JHL', 3196: 'JHL',
    197: 'TNM', 3197: 'TNM',
    198: 'TDA', 3198: 'TDA',
    199: 'JTH', 3199: 'JTH',
    201: 'PDA', 3201: 'PDA',
    202: 'RWG', 3202: 'RWG',
    203: 'SJM', 3203: 'SJM',
    204: 'BTW', 3204: 'BTW',
    205: 'SNG', 3205: 'SNG',
    206: 'TBM', 3206: 'TBM',
    207: 'BCM', 3207: 'BCM',
    208: 'JSI', 3208: 'JSI',
    209: 'STW', 3209: 'STW',
    210: 'TMM', 3210: 'TMM',
    211: 'TPD', 3211: 'TPD',
    212: 'JMA', 3212: 'JMA',
    213: 'JKB', 3213: 'JKB',
    214: 'JGA', 3214: 'JGA',
    215: 'JKP', 3215: 'JKP',
    216: 'SKI', 3216: 'SKI',
    217: 'TMB', 3217: 'TMB',
    220: 'GHS', 3220: 'GHS',
    221: 'TSK', 3221: 'TSK',
    222: 'TDC', 3222: 'TDC',
    223: 'TRJ', 3223: 'TRJ',
    224: 'JAH', 3224: 'JAH',
    225: 'TIH', 3225: 'TIH',
    226: 'JPR', 3226: 'JPR',
    227: 'KSB', 3227: 'KSB',
    228: 'INN', 3228: 'INN',
    229: 'TSJ', 3229: 'TSJ',
    230: 'SSH', 3230: 'SSH',
    231: 'BBM', 3231: 'BBM',
    232: 'TMD', 3232: 'TMD',
    233: 'BEN', 3233: 'BEN',
    234: 'SRM', 3234: 'SRM',
    235: 'SBM', 3235: 'SBM',
    236: 'UYB', 3236: 'UYB',
    237: 'KLS', 3237: 'KLS',
    238: 'JKT', 3238: 'JKT',
    239: 'KMY', 3239: 'KMY',
    240: 'KAP', 3240: 'KAP',
    241: 'DJA', 3241: 'DJA',
    242: 'TKK', 3242: 'TKK',
    243: 'KKR', 3243: 'KKR',
    244: 'GRT', 3244: 'GRT',
    245: 'BDR', 3245: 'BDR',
    246: 'BGH', 3246: 'BGH',
    247: 'BPR', 3247: 'BPR',
    249: 'TAI', 3249: 'TAI',
    248: 'JTS', 3248: 'JTS',
    250: 'TEA', 3250: 'TEA',
    251: 'KPR', 3251: 'KPR',
    252: 'TMA', 3252: 'TMA',
    253: 'JTT', 3253: 'JTT',
    254: 'KPH', 3254: 'KPH',
    255: 'SBP', 3255: 'SBP',
    256: 'PBR', 3256: 'PBR',
    257: 'RAU', 3257: 'RAU',
    258: 'JTA', 3258: 'JTA',
    259: 'SAN', 3259: 'SAN',
    260: 'KDN', 3260: 'KDN',
    261: 'GMG', 3261: 'GMG',
    262: 'TCT', 3262: 'TCT',
    263: 'BTA', 3263: 'BTA',
    264: 'JBH', 3264: 'JBH',
    265: 'JAI', 3265: 'JAI',
    266: 'JDK', 3266: 'JDK',
    267: 'TDI', 3267: 'TDI',
    268: 'BBT', 3268: 'BBT',
    269: 'MKA', 3269: 'MKA',
    270: 'BPI', 3270: 'BPI',
    273: 'LHA', 3273: 'LHA',
    277: 'WSU', 3277: 'WSU',
    278: 'JPI', 3278: 'JPI',
    274: 'STG', 3274: 'STG',
    275: 'MSL', 3275: 'MSL',
    276: 'JAS', 3276: 'JAS',
    279: 'PTJ', 3279: 'PTJ',
    280: 'KDA', 3280: 'KDA',
    281: 'PLT', 3281: 'PLT',
    282: 'PTT', 3282: 'PTT',
    283: 'PSE', 3283: 'PSE',
    284: 'BSP', 3284: 'BSP',
    285: 'BMC', 3285: 'BMC',
    286: 'BIH', 3286: 'BIH',
    287: 'SUA', 3287: 'SUA',
    288: 'SPT', 3288: 'SPT',
    289: 'TEE', 3289: 'TEE',
    290: 'TDY', 3290: 'TDY',
    291: 'BSL', 3291: 'BSL',
    292: 'BMJ', 3292: 'BMJ',
    293: 'BSA', 3293: 'BSA',
    294: 'KKM', 3294: 'KKM',
    295: 'BKR', 3295: 'BKR',
    296: 'BJL', 3296: 'BJL',
    701: 'IKB', 3701: 'IKB',
    702: 'IPJ', 3702: 'IPJ',
    703: 'IWS', 3703: 'IWS',
    704: 'IJK', 3704: 'IJK',
}

def format_brchcd(branch_code: int) -> str:
    """Format branch code to branch name"""
    if branch_code in range(7000, 9001) or branch_code in range(9994, 10000) or branch_code == 1:
        return 'HOE'
    if branch_code in [3000, 3001, 3999] or branch_code in range(4000, 5000):
        return 'IBU'
    return BRCHCD_MAP.get(branch_code, '')

# ============================================================================
# CAC BRANCH MAPPING
# ============================================================================

CACBRCH_MAP = {
    'KL': [2, 18, 35, 38, 40, 41, 53, 66, 120, 124, 128, 129, 141, 148,
           169, 170, 225, 226, 230, 232, 236, 248, 262, 267, 802, 812, 816, 818],
    'CC': [3, 15, 19, 22, 26, 29, 36, 46, 56, 69, 83, 94, 96, 97, 701, 118, 122, 125,
           131, 132, 136, 138, 145, 151, 155, 157, 162, 163, 270, 167, 168, 173, 178,
           179, 195, 198, 180, 196, 197, 202, 220, 229, 241, 252, 280, 811, 815, 822,
           103, 821, 825, 269, 284, 285, 288, 289, 702],
    'SJ': [27, 42, 60, 68, 88, 121, 154, 177, 204, 206, 255, 801, 826],
    'PG': [6, 54, 107, 114, 126, 150, 159, 171, 205, 253, 265, 266, 808, 817],
    'JB': [7, 37, 52, 59, 61, 79, 89, 105, 110, 147, 174, 176, 216, 217, 222, 286,
           804, 805, 287, 290],
    'KL2': [20, 25, 43, 78, 81, 92, 109, 127, 133, 135, 153, 199, 201, 203,
            221, 240, 250, 268, 814, 820],
}

def format_cacbrch(branch_code: int) -> str:
    """Format CAC branch code"""
    for key, branches in CACBRCH_MAP.items():
        if branch_code in branches:
            if key == 'KL':
                return '911'
            elif key == 'CC':
                return '912'
            elif key == 'KL2':
                return '913'
            elif key == 'JB':
                return '914'
            elif key == 'PG':
                return '915'
            elif key == 'SJ':
                return '916'
    return '000'

def format_cacname(branch_code: int) -> str:
    """Format CAC name"""
    for key, branches in CACBRCH_MAP.items():
        if branch_code in branches:
            if key == 'KL':
                return 'CAC-K. LUMPUR '
            elif key == 'CC':
                return 'CAC-CITY CENTRE'
            elif key == 'SJ':
                return 'CAC-BUTTERWOTH'
            elif key == 'PG':
                return 'CAC-PENANG'
            elif key == 'JB':
                return 'CAC-JOHOR BAHRU'
            elif key == 'KL2':
                return 'CAC-KELANG'
    return 'NON CAC'

# ============================================================================
# REGIONAL OFFICE MAPPING
# ============================================================================

REGIOFF_MAP = {
    'SELWP1': [3, 22, 40, 46, 53, 56, 120, 122, 129, 136, 155, 168, 169, 170, 173,
               220, 225, 226, 232, 252, 262, 802, 818, 284, 285],
    'SELWP2': [2, 15, 29, 41, 66, 83, 94, 96, 97, 701, 103, 118, 128, 138, 141, 151,
               178, 195, 196, 197, 248, 821, 822],
    'SELWP3': [18, 19, 26, 35, 36, 38, 81, 124, 125, 131, 145, 148, 157, 162, 163,
               167, 179, 198, 230, 241, 267, 269, 270, 280, 825],
    'SELWP4': [20, 25, 31, 43, 69, 73, 78, 92, 109, 127, 133, 135, 153, 180, 199,
               201, 202, 203, 221, 235, 240, 250, 268, 814, 820, 279, 288, 289, 702],
    'JOHOR': [7, 21, 28, 37, 47, 52, 59, 61, 75, 79, 87, 89, 91, 93, 102, 105, 110,
              144, 147, 174, 176, 216, 217, 222, 224, 234, 242, 247, 286, 804, 805, 287, 290],
    'PNGKDHPLS': [6, 10, 11, 27, 34, 42, 54, 57, 60, 68, 70, 74, 77, 86, 88, 104, 107,
                  114, 121, 126, 150, 154, 159, 164, 171, 177, 204, 205, 206, 213, 238,
                  253, 255, 258, 265, 266, 801, 806, 808, 817, 826, 704],
    'PERAK': [5, 9, 23, 49, 51, 67, 71, 76, 80, 85, 95, 108, 123, 137, 146, 152, 158,
              207, 208, 209, 210, 211, 243, 244, 245, 246, 249, 251, 256, 809],
    'MLKNSEM': [4, 16, 17, 24, 39, 45, 63, 64, 65, 111, 156, 160, 165, 172, 212, 223,
                231, 254, 800, 807],
    'PAHKELTER': [8, 13, 14, 30, 48, 106, 113, 116, 117, 139, 233, 237, 239, 257, 260,
                  261, 263, 264, 277, 827, 819, 703],
    'SARAWAK': [32, 50, 58, 90, 130, 175, 182, 183, 184, 185, 273, 274, 275, 186, 189,
                190, 191, 192, 193, 194, 259, 813, 281],
    'SABAHLBN': [33, 44, 55, 62, 72, 112, 115, 140, 142, 143, 149, 161, 228, 803, 278,
                 276, 282, 283],
}

def format_regioff(branch_code: int) -> str:
    """Format regional office code"""
    for region, branches in REGIOFF_MAP.items():
        if branch_code in branches:
            return region
    return 'NON REGION'

# ============================================================================
# NEW REGION MAPPING
# ============================================================================

REGNEW_MAP = {
    'WS I': [2, 3, 22, 46, 53, 56, 120, 129, 136, 169, 170, 173, 196, 226, 232, 252,
             262, 284, 285, 802, 812],
    'WS II': [15, 29, 40, 41, 66, 83, 96, 97, 103, 118, 128, 141, 151, 195, 197, 248,
              269, 701, 818, 822],
    'WS III': [18, 19, 35, 36, 81, 124, 125, 131, 135, 145, 148, 162, 167, 180, 220,
               241, 267, 280, 815, 816, 820],
    'WS IV': [26, 38, 94, 122, 138, 153, 155, 157, 163, 168, 178, 179, 198, 225, 230,
              250, 270, 279, 288, 289, 702, 811, 821, 825],
    'WS V': [20, 25, 31, 43, 69, 73, 78, 92, 109, 127, 133, 199, 201, 202, 203, 221,
             235, 240, 268, 814],
    'S I': [7, 37, 52, 59, 61, 79, 87, 89, 91, 93, 102, 105, 110, 144, 147, 174, 176,
            216, 217, 222, 234, 286, 287, 290, 804, 805],
    'S II': [4, 16, 17, 21, 24, 28, 39, 45, 47, 63, 64, 65, 75, 111, 156, 160, 165,
             172, 224, 231, 242, 247, 254, 800, 807],
    'N I': [6, 10, 34, 54, 70, 74, 77, 86, 104, 107, 114, 126, 150, 159, 171, 205,
            238, 258, 265, 266, 806, 808, 817, 704],
    'N II': [11, 23, 27, 42, 57, 60, 68, 88, 108, 121, 154, 164, 177, 204, 206, 211,
             243, 249, 256, 801, 824, 826],
    'CTR': [5, 9, 49, 51, 67, 71, 76, 80, 85, 95, 123, 137, 146, 152, 158, 207, 208,
            209, 210, 244, 245, 251, 809, 823],
    'EST': [8, 13, 14, 30, 48, 106, 113, 116, 117, 139, 233, 237, 239, 257, 260, 261,
            263, 264, 277, 819, 827, 703],
    'SRW': [32, 50, 58, 90, 130, 175, 182, 183, 184, 185, 273, 274, 275, 186, 189,
            190, 191, 192, 193, 194, 259, 281, 813],
    'SAB': [33, 44, 55, 62, 72, 112, 115, 140, 142, 143, 149, 161, 228, 278, 276,
            282, 283, 803],
}

def format_regnew(branch_code: int) -> str:
    """Format new region code"""
    for region, branches in REGNEW_MAP.items():
        if branch_code in branches:
            return region
    return 'OFF'

# ============================================================================
# CUSTOMER TYPE MAPPING
# ============================================================================

CTYPE_MAP = {
    'BP': '01', 'BC': '01',
    'BB': '02',
    'BI': '03',
    'BJ': '07',
    'BQ': '11',
    'BM': '12',
    'BN': '13',
    'BG': '17',
    'BR': '20', 'BF': '20', 'BH': '20', 'BZ': '20', 'BU': '20', 'AD': '20',
    'BT': '20', 'BV': '20', 'BS': '20',
    'AC': '60', 'DD': '60', 'CG': '60', 'CA': '60', 'CC': '60', 'CB': '60',
    'CD': '60', 'CF': '60',
    'DA': '71',
    'DB': '72',
    'DC': '74',
    'EC': '76', 'EA': '76', 'EJ': '76',
    'FA': '79',
    'BW': '81', 'BA': '81', 'BE': '81',
    'EB': '85', 'CE': '85', 'GA': '85',
}

def format_ctype(ctype_code: str) -> str:
    """Format customer type code"""
    return CTYPE_MAP.get(ctype_code, '  ')

# ============================================================================
# BRANCH REVERSE MAPPING (NAME TO CODE)
# ============================================================================

BRCHRVR_MAP = {
    'PCS': 1, 'JSS': 2, 'JRC': 3, 'MLK': 4, 'IMO': 5, 'PPG': 6, 'JBU': 7,
    'KTN': 8, 'JYK': 9, 'ASR': 10, 'GRN': 11, 'PPH': 12, 'KBU': 13, 'TMH': 14,
    'KPG': 15, 'NLI': 16, 'TPN': 17, 'PJN': 18, 'DUA': 19, 'TCL': 20, 'BPT': 21,
    'SMY': 22, 'KMT': 23, 'RSH': 24, 'SAM': 25, 'SPG': 26, 'NTL': 27, 'MUA': 28,
    'JRL': 29, 'KTU': 30, 'SKC': 31, 'WSS': 32, 'KKU': 33, 'KGR': 34, 'SSA': 35,
    'SS2': 36, 'TSA': 37, 'JKL': 38, 'KKG': 39, 'JSB': 40, 'JIH': 41, 'BMM': 42,
    'BTG': 43, 'TWU': 44, 'SRB': 45, 'APG': 46, 'SGM': 47, 'MTK': 48, 'JLP': 49,
    'MRI': 50, 'SMG': 51, 'UTM': 52, 'TMI': 53, 'BBB': 54, 'LBN': 55, 'KJG': 56,
    'SPI': 57, 'SBU': 58, 'PKL': 59, 'BAM': 60, 'KLI': 61, 'SDK': 62, 'GMS': 63,
    'PDN': 64, 'BHU': 65, 'BDA': 66, 'CMR': 67, 'SAT': 68, 'BKI': 69, 'PSA': 70,
    'BCG': 71, 'PPR': 72, 'SPK': 73, 'SIK': 74, 'CAH': 75, 'PRS': 76, 'PLI': 77,
    'SJA': 78, 'MSI': 79, 'MLB': 80, 'SBH': 81, 'MCG': 82, 'JBB': 83, 'PMS': 84,
    'SST': 85, 'CLN': 86, 'MSG': 87, 'KUM': 88, 'TPI': 89, 'BTL': 90, 'KUG': 91,
    'KLG': 92, 'EDU': 93, 'STP': 94, 'TIN': 95, 'SGK': 96, 'HSL': 97, 'TCY': 98,
    'XXX': 99, 'YYY': 100, 'KBR': 101, 'PRJ': 102, 'JJG': 103, 'KKL': 104, 'KTI': 105,
    'CKI': 106, 'JLT': 107, 'BSI': 108, 'KSR': 109, 'TJJ': 110, 'AKH': 111, 'LDO': 112,
    'TML': 113, 'BBA': 114, 'KNG': 115, 'TRI': 116, 'KKI': 117, 'TMW': 118, 'BNV': 119,
    'PIH': 120, 'PRA': 121, 'SKN': 122, 'IGN': 123, 'S14': 124, 'KJA': 125, 'PTS': 126,
    'TSM': 127, 'SGB': 128, 'BSR': 129, 'PDG': 130, 'TMG': 131, 'CKT': 132, 'PKG': 133,
    'RPG': 134, 'BSY': 135, 'TCS': 136, 'JPP': 137, 'WMU': 138, 'JRT': 139, 'CPE': 140,
    'STL': 141, 'KBD': 142, 'LDU': 143, 'KHG': 144, 'BSD': 145, 'PSG': 146, 'PNS': 147,
    'PJO': 148, 'BFT': 149, 'LMM': 150, 'SLY': 151, 'ATR': 152, 'USJ': 153, 'BSJ': 154,
    'TTJ': 155, 'TMR': 156, 'BPJ': 157, 'SPL': 158, 'RLU': 159, 'MTH': 160, 'DGG': 161,
    'SEA': 162, 'JKA': 163, 'KBS': 164, 'TKA': 165, 'PGG': 166, 'BBG': 167, 'KLC': 168,
    'CTD': 169, 'PJA': 170, 'JMR': 171, 'TMJ': 172, 'SCA': 173, 'BBP': 174, 'LBG': 175,
    'TPG': 176, 'JRU': 177, 'MIN': 178, 'OUG': 179, 'KBG': 180, 'SRO': 181, 'JPU': 182,
    'JCL': 183, 'JPN': 184, 'KCY': 185, 'JTZ': 186, 'BNH': 189, 'BTR': 190, 'KPT': 191,
    'MRD': 192, 'MKH': 193, 'SRK': 194, 'BWK': 195, 'JHL': 196, 'TNM': 197, 'TDA': 198,
    'JTH': 199, 'JSK': 200, 'PDA': 201, 'RWG': 202, 'SJM': 203, 'BTW': 204, 'SNG': 205,
    'TBM': 206, 'BCM': 207, 'JSI': 208, 'STW': 209, 'TMM': 210, 'TPD': 211, 'JMA': 212,
    'JKB': 213, 'JGA': 214, 'JKP': 215, 'SKI': 216, 'TMB': 217, 'ZZZ': 218, 'BC1': 219,
    'GHS': 220, 'TSK': 221, 'TDC': 222, 'TRJ': 223, 'JAH': 224, 'TIH': 225, 'JPR': 226,
    'KSB': 227, 'INN': 228, 'TSJ': 229, 'SSH': 230, 'BBM': 231, 'TMD': 232, 'BEN': 233,
    'SRM': 234, 'SBM': 235, 'UYB': 236, 'KLS': 237, 'JKT': 238, 'KMY': 239, 'KAP': 240,
    'DJA': 241, 'TKK': 242, 'KKR': 243, 'GRT': 244, 'BDR': 245, 'BGH': 246, 'BPR': 247,
    'JTS': 248, 'TAI': 249, 'TEA': 250, 'KPR': 251, 'TMA': 252, 'JTT': 253, 'KPH': 254,
    'SBP': 255, 'PBR': 256, 'RAU': 257, 'JTA': 258, 'SAN': 259, 'KDN': 260, 'GMG': 261,
    'TCT': 262, 'BTA': 263, 'JBH': 264, 'JAI': 265, 'JDK': 266, 'TDI': 267, 'BBT': 268,
    'MKA': 269, 'BPI': 270, 'LHA': 273, 'STG': 274, 'MSL': 275, 'JAS': 276, 'WSU': 277,
    'JPI': 278, 'PTJ': 279, 'KDA': 280, 'PLT': 281, 'PTT': 282, 'PSE': 283, 'BSP': 284,
    'BMC': 285, 'BIH': 286, 'SUA': 287, 'SPT': 288, 'TEE': 289, 'TDY': 290, 'BSL': 291,
    'BMJ': 292, 'BSA': 293, 'KKM': 294, 'BKR': 295, 'BJL': 296,
    'IKB': 701, 'IPJ': 702, 'IWS': 703, 'IJK': 704,
    'H01': 800, 'H02': 801, 'H03': 802, 'H04': 803, 'H05': 804, 'H06': 805, 'H07': 806,
    'H08': 807, 'H09': 808, 'H10': 809, 'H11': 811, 'H12': 812, 'H13': 813, 'H14': 814,
    'H15': 815, 'H16': 816, 'H17': 817, 'H18': 818, 'H19': 819, 'H20': 820, 'H21': 821,
    'H22': 822, 'H23': 823, 'H24': 824, 'H25': 825, 'H26': 826, 'H27': 827, 'H28': 828,
    'H44': 844, 'H45': 845, 'H46': 846, 'H47': 847, 'H48': 848, 'H49': 849, 'H50': 850,
    'H51': 851, 'H52': 852, 'H53': 853, 'H54': 854, 'H55': 855, 'H56': 856, 'H57': 857,
    'H58': 858, 'H59': 859, 'H60': 860, 'H61': 861, 'H62': 862, 'H63': 863,
    'HOE': 884,
    'CBR': 902, 'CCC': 903, 'SCD': 904, 'SCC': 905, 'CAD': 906, 'SDC': 907, 'ICC': 908,
    'PCC': 909, 'JCC': 910, 'KCA': 911, 'CCA': 912, 'LCA': 913, 'JCA': 914, 'PCA': 915,
    'BCA': 916, 'KCC': 917, 'MCC': 918, 'HCC': 919, 'LCC': 920,
    'TFC': 992, 'XPP': 993, 'XJB': 994, 'SMC': 996, 'CCP': 997, 'CPC': 998,
}

def format_brchrvr(branch_name: str) -> Optional[int]:
    """Reverse format: branch name to code"""
    return BRCHRVR_MAP.get(branch_name)

# ============================================================================
# BRANCH LISTS
# ============================================================================

PRKBRH = [119, 67, 123, 5, 207, 49, 137, 208, 9, 80, 146, 158, 85, 244, 246, 809, 243, 251]
IPRKBRH = [3119, 3067, 3123, 3005, 3207, 3049, 3137, 3208, 3009, 3080, 3146, 3158, 3085, 3244, 3246, 3809, 3243, 3251]

PNGBRH = [114, 60, 54, 42, 154, 204, 107, 171, 177, 164, 150, 27, 6, 121, 126, 159, 68, 205, 206, 265, 266, 253, 255, 801, 808, 817, 826, 256]
IPNGBRH = [3114, 3060, 3054, 3042, 3154, 3204, 3107, 3171, 3177, 3164, 3150, 3027, 3006, 3121, 3126, 3159, 3068, 3205, 3206, 3265, 3266, 3253, 3255, 3801, 3808, 3817, 3826, 3256]

JBBRH = [174, 7, 61, 105, 79, 59, 147, 216, 110, 217, 176, 89, 37, 52, 222, 804, 805, 286, 287, 290]
IJBBRH = [3174, 3007, 3061, 3105, 3079, 3059, 3147, 3216, 3110, 3217, 3176, 3089, 3037, 3052, 3222, 3804, 3805, 3286, 3287, 3290]

KLGBRH = [20, 25, 43, 78, 81, 92, 109, 127, 133, 199, 201, 203, 221, 240, 250, 268, 135, 153, 814, 820, 293, 294]
IKLGBRH = [3020, 3025, 3043, 3078, 3081, 3092, 3109, 3127, 3133, 3199, 3201, 3203, 3221, 3240, 3250, 3268, 3135, 3153, 3814, 3820, 3293, 3294]

MLKBRH = [4, 17, 28, 111, 156, 160, 165, 172, 224, 231, 242, 247, 807]
IMLKBRH = [3004, 3017, 3028, 3111, 3156, 3160, 3165, 3172, 3224, 3231, 3242, 3247, 3807]

KCGBRH = [32, 130, 184, 185, 186, 274]
IKCGBRH = [3032, 3130, 3184, 3185, 3186, 3274]

KKUBRH = [33, 140, 278, 112, 228, 161, 142, 72, 149, 282]
IKKUBRH = [3033, 3140, 3278, 3112, 3228, 3161, 3142, 3072, 3149, 3282]

SROBRH = [44, 50, 55, 58, 62, 90, 115, 143, 175, 183, 189, 190, 191, 192, 193, 194, 259, 273, 275, 276, 281, 283]
ISROBRH = [3044, 3050, 3055, 3058, 3062, 3090, 3115, 3143, 3175, 3183, 3189, 3190, 3191, 3192, 3193, 3194, 3259, 3273, 3275, 3276, 3281, 3283]

SPIBRH = [10, 238, 11, 57, 88, 104, 74, 704]
ISPIBRH = [3010, 3238, 3011, 3057, 3088, 3104, 3074, 3704]

SRBBRH = [16, 24, 39, 45, 64, 65, 254]
ISRBBRH = [3016, 3024, 3039, 3045, 3064, 3065, 3254]

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def is_perak_branch(branch_code: int) -> bool:
    """Check if branch is in Perak"""
    return branch_code in PRKBRH or branch_code in IPRKBRH

def is_penang_branch(branch_code: int) -> bool:
    """Check if branch is in Penang"""
    return branch_code in PNGBRH or branch_code in IPNGBRH

def is_johor_branch(branch_code: int) -> bool:
    """Check if branch is in Johor Bahru"""
    return branch_code in JBBRH or branch_code in IJBBRH

def is_klang_branch(branch_code: int) -> bool:
    """Check if branch is in Klang"""
    return branch_code in KLGBRH or branch_code in IKLGBRH

def is_melaka_branch(branch_code: int) -> bool:
    """Check if branch is in Melaka"""
    return branch_code in MLKBRH or branch_code in IMLKBRH

def is_kuching_branch(branch_code: int) -> bool:
    """Check if branch is in Kuching"""
    return branch_code in KCGBRH or branch_code in IKCGBRH

def is_kk_branch(branch_code: int) -> bool:
    """Check if branch is in Kota Kinabalu"""
    return branch_code in KKUBRH or branch_code in IKKUBRH

def is_sro_branch(branch_code: int) -> bool:
    """Check if branch is SRO"""
    return branch_code in SROBRH or branch_code in ISROBRH

def is_sp_branch(branch_code: int) -> bool:
    """Check if branch is in Sungai Petani"""
    return branch_code in SPIBRH or branch_code in ISPIBRH

def is_srb_branch(branch_code: int) -> bool:
    """Check if branch is in Seremban"""
    return branch_code in SRBBRH or branch_code in ISRBBRH
