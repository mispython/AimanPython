#!/usr/bin/env python3
"""
Program  : EIBMRPTS.py
Purpose  : Job orchestration pipeline — Python equivalent of the JCL job stream.
           Runs the sequence of report programs in the correct dependency order,
            mirroring the mainframe batch job execution defined in the original JCL.

           Original JCL job steps executed (active, non-commented):
             1.  EIBMLN03  - Weighted Average Lending Rate (RDIR II)
             2.  EIFMLN03  - (Islamic variant of EIBMLN03)
             3.  EIBMLN04  - Undrawn Loans by Sectors
             4.  DALMPBB2  - ACE Account Profile
             5.  DALMPBB3  - ACE Account Break-Down
             6.  EIBMDISB  - Top 20 Disbursements & Repayments
             7.  EIBMDISC  - Top 10 Disbursements & Repayments
             8.  EIBMRC04  - Loans & Advances by Interest Rate (Report 04)
             9.  EIBMRC05  - Loans & Advances by Security Type (Report 05)
            10.  EIBMRC07  - Loans & Advances by Loan Size (Report 07)
            11.  EIBMIRAT  - Average Rate on All FD Products
            12.  EIBMLNPO  - Paid-Off Loans Report
            13.  EIBMSFLN  - Staff Loan Position Report (PBB)
            14.  EIVMSFLN  - Staff Loan Position Report (PIVB)
            15.  EIBWSTAF  - Weekly Staff New/Paid Loan Listing (PBB)
            16.  EIVWSTAF  - Weekly Staff New/Paid Loan Listing (PIVB)
            17.  EIBMPB02  - Weighted Average Lending Rate (Factoring)

           Commented-out steps in JCL (discontinued or conditional):
            *  EIBMLN01  - Undrawn Term Loan/OD for Branches (commented out)
            *  EIBMLN02  - RC Amount Approved & Accounts (commented out)
            *  EIBBPBCR  - Profile on PB Premium Club Member (commented out)
            *  EIBMRC08  - Loans & Advances Discounted with CAGAMAS (commented out)
            *  EIBMRC10  - Progress Report Retail & Corporate Loans (commented out)
            *  EIBMCOLR  - PBB Total Loans & Advances by Collateral (commented out)
            *  EIBMCLOS  - Monthly Report on Reasons of Closed Accounts (commented out)
            *  EIBMISLM  - Profile on Monthly Islamic Savings/Current/FD (commented out)
            *  EIBMFDIS  - Profile on Monthly Islamic Fixed Deposits (commented out)
            *  EIBDFDMV  - FD Interest Rates Exception Report (commented out)
            *  EIBMCUST  - Listing of Accounts for Specific Customer Codes (commented out)
            *  EIBMCEXT  - Listing of Accounts with Sector Codes 0311-0316 (commented out)
            *  EIBMHEXT  - Listing of Undrawn Housing Loans (commented out)
            *  EIBMLNA1  - Undrawn Loans by Collaterals (commented out)
            *  EIBMICLS  - Reasons of Closed Accts - Islamic (discontinued ESMR2016-1557)
            *  EIBMCCLS  - Reasons of Closed Accts - Conventional (discontinued ESMR2016-1557)
"""

import subprocess
import sys
import logging
from pathlib import Path
from datetime import datetime

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR   = Path("/data")
PGM_DIR    = BASE_DIR / "programs"   # Directory containing all converted .py programs
LOG_DIR    = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# LOGGING SETUP
# ============================================================================

log_file = LOG_DIR / f"eibmrpts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ============================================================================
# JOB STEP DEFINITIONS
# Each entry mirrors one active EXEC SAS609 step in the JCL.
# 'program'  : Python script filename (without .py) in PGM_DIR
# 'desc'     : Matches the JCL comment description
# 'active'   : True  = was an active (non-commented) JCL step
#              False = was commented out in the original JCL
# ============================================================================

JOB_STEPS = [
    # ------------------------------------------------------------------
    # WEIGHTED AVERAGE LENDING RATE (RDIR II)
    # ------------------------------------------------------------------
    {"program": "EIBMLN03",  "active": True,
     "desc": "Weighted Average Lending Rate (RDIR II) - Reports & SRS"},

    {"program": "EIFMLN03",  "active": True,
     "desc": "Weighted Average Lending Rate (RDIR II) - Islamic variant"},

    # ------------------------------------------------------------------
    # UNDRAWN LOANS BY SECTORS
    # ------------------------------------------------------------------
    {"program": "EIBMLN04",  "active": True,
     "desc": "Undrawn Loans by Sectors"},

    # ------------------------------------------------------------------
    # ACE ACCOUNT PROFILE & BREAKDOWN
    # ------------------------------------------------------------------
    {"program": "DALMPBB2",  "active": True,
     "desc": "ACE Account Profile"},

    {"program": "DALMPBB3",  "active": True,
     "desc": "ACE Account Break-Down"},

    # ------------------------------------------------------------------
    # TOP 20 / TOP 10 DISBURSEMENTS & REPAYMENTS
    # ------------------------------------------------------------------
    {"program": "EIBMDISB",  "active": True,
     "desc": "Top 20 Disbursements & Repayments"},

    {"program": "EIBMDISC",  "active": True,
     "desc": "Top 10 Disbursements & Repayments"},

    # ------------------------------------------------------------------
    # LOANS & ADVANCES REPORTS (RC SERIES)
    # ------------------------------------------------------------------
    {"program": "EIBMRC04",  "active": True,
     "desc": "Loans & Advances by Interest Rate (Report 04)"},

    {"program": "EIBMRC05",  "active": True,
     "desc": "Loans & Advances by Security Type (Report 05)"},

    {"program": "EIBMRC07",  "active": True,
     "desc": "Loans & Advances by Loan Size on Approved Limit (Report 07)"},

    # ------------------------------------------------------------------
    # AVERAGE RATE ON ALL FIXED DEPOSIT PRODUCTS
    # ------------------------------------------------------------------
    {"program": "EIBMIRAT",  "active": True,
     "desc": "Average Rate on All Fixed Deposit Products (Monthly)"},

    # ------------------------------------------------------------------
    # PAID-OFF LOANS REPORT
    # ------------------------------------------------------------------
    {"program": "EIBMLNPO",  "active": True,
     "desc": "Paid-Off Loans Report"},

    # ------------------------------------------------------------------
    # STAFF LOAN POSITION REPORTS
    # ------------------------------------------------------------------
    {"program": "EIBMSFLN",  "active": True,
     "desc": "Staff Loan Position Report (PBB)"},

    {"program": "EIVMSFLN",  "active": True,
     "desc": "Staff Loan Position Report (PIVB)"},

    # ------------------------------------------------------------------
    # WEEKLY STAFF NEW/PAID LOAN LISTING
    # ------------------------------------------------------------------
    {"program": "EIBWSTAF",  "active": True,
     "desc": "Weekly Listing for Staff New Loan and Paid Loan (PBB)"},

    {"program": "EIVWSTAF",  "active": True,
     "desc": "Weekly Listing for Staff New Loan and Paid Loan (PIVB)"},

    # ------------------------------------------------------------------
    # WEIGHTED AVERAGE LENDING RATE (FACTORING)
    # ------------------------------------------------------------------
    {"program": "EIBMPB02",  "active": True,
     "desc": "Weighted Average Lending Rate (Factoring System)"},

    # ==================================================================
    # COMMENTED-OUT / DISCONTINUED JCL STEPS
    # These are preserved below as inactive steps matching the JCL.
    # ==================================================================

    # * UNDRAWN TERM LOAN/OD FOR BRANCHES (commented out in JCL)
    {"program": "EIBMLN01",  "active": False,
     "desc": "Undrawn Term Loan/Overdraft for Branches Update (RPS) [COMMENTED OUT]"},

    # * RC AMOUNT APPROVED & NO OF ACCOUNTS (commented out in JCL)
    {"program": "EIBMLN02",  "active": False,
     "desc": "RC Amount Approved and No of Accounts [COMMENTED OUT]"},

    # * PROFILE ON PB PREMIUM CLUB MEMBER (commented out in JCL)
    {"program": "EIBBPBCR",  "active": False,
     "desc": "Profile on PB Premium Club Member [COMMENTED OUT]"},

    # * LOANS & ADVANCES DISCOUNTED WITH CAGAMAS (commented out in JCL)
    {"program": "EIBMRC08",  "active": False,
     "desc": "Loans & Advances Discounted with CAGAMAS (Report 08) [COMMENTED OUT]"},

    # * PROGRESS REPORT ON RETAIL & CORPORATE LOANS (commented out in JCL)
    {"program": "EIBMRC10",  "active": False,
     "desc": "Progress Report on Retail & Corporate Loans by State (Report 10) [COMMENTED OUT]"},

    # * PBB TOTAL LOANS & ADVANCES BY COLLATERAL (commented out in JCL)
    {"program": "EIBMCOLR",  "active": False,
     "desc": "PBB Total Loans & Advances by Collateral [COMMENTED OUT]"},

    # * REASONS OF CLOSED ACCTS - ALL (commented out in JCL)
    {"program": "EIBMCLOS",  "active": False,
     "desc": "Monthly Report on Reasons of Closed Accounts [COMMENTED OUT]"},

    # * PROFILE ON MONTHLY ISLAMIC SAVINGS/CURRENT/FD (commented out in JCL)
    {"program": "EIBMISLM",  "active": False,
     "desc": "Profile on Monthly Islamic Savings, Current and FD Products [COMMENTED OUT]"},

    # * PROFILE ON MONTHLY ISLAMIC FIXED DEPOSITS (commented out in JCL)
    # * DISCONTINUED: requested by Mohd Fadzli Mohd Yusoff - dated 06/07/2004
    {"program": "EIBMFDIS",  "active": False,
     "desc": "Profile on Monthly Islamic Fixed Deposits [COMMENTED OUT - DISCONTINUED]"},

    # * FD INTEREST RATES EXCEPTION REPORT (commented out in JCL)
    {"program": "EIBDFDMV",  "active": False,
     "desc": "FD Interest Rates Exception Report - Total Receipts > RM 1.0M [COMMENTED OUT]"},

    # * LISTING OF ACCOUNTS FOR SPECIFIC CUSTOMER CODES (commented out in JCL)
    {"program": "EIBMCUST",  "active": False,
     "desc": "Listing of Accounts (OD & Loans) for Specific Customer Codes [COMMENTED OUT]"},

    # * LISTING OF ACCOUNTS WITH SECTOR CODES 0311-0316 (commented out in JCL)
    {"program": "EIBMCEXT",  "active": False,
     "desc": "Listing of Accounts (OD & Loans) with Sector Codes 0311-0316 [COMMENTED OUT]"},

    # * LISTING OF UNDRAWN HOUSING LOANS (commented out in JCL)
    {"program": "EIBMHEXT",  "active": False,
     "desc": "Listing of Undrawn Housing Loans [COMMENTED OUT]"},

    # * UNDRAWN TERM LOAN/OD/LOANS OTHERS BY COLLATERALS (commented out in JCL)
    {"program": "EIBMLNA1",  "active": False,
     "desc": "Undrawn Term Loan/Overdraft/Loans by Collaterals [COMMENTED OUT]"},

    # * REASONS OF CLOSED ACCTS - ISLAMIC (discontinued ESMR2016-1557)
    {"program": "EIBMICLS",  "active": False,
     "desc": "Reasons of Closed Accounts - Islamic [DISCONTINUED ESMR2016-1557]"},

    # * REASONS OF CLOSED ACCTS - CONVENTIONAL (discontinued ESMR2016-1557)
    {"program": "EIBMCCLS",  "active": False,
     "desc": "Reasons of Closed Accounts - Conventional [DISCONTINUED ESMR2016-1557]"},
]

# ============================================================================
# PIPELINE RUNNER
# ============================================================================

def run_step(step: dict) -> bool:
    """
    Execute a single job step by invoking the corresponding Python script.
    Returns True on success, False on failure.
    Mirrors JCL EXEC SAS609 behaviour: log RC and continue unless ABEND.
    """
    program  = step["program"]
    desc     = step["desc"]
    script   = PGM_DIR / f"{program}.py"

    log.info(f"STEP START : {program}")
    log.info(f"            {desc}")

    if not script.exists():
        log.error(f"STEP ABEND : {program} - Script not found: {script}")
        return False

    try:
        result = subprocess.run(
            [sys.executable, str(script)],
            capture_output=True,
            text=True,
            cwd=str(BASE_DIR),
        )
        if result.stdout:
            for line in result.stdout.strip().splitlines():
                log.info(f"  [STDOUT] {line}")
        if result.stderr:
            for line in result.stderr.strip().splitlines():
                log.warning(f"  [STDERR] {line}")

        if result.returncode == 0:
            log.info(f"STEP END   : {program}  RC=0000")
            return True
        else:
            log.error(f"STEP ABEND : {program}  RC={result.returncode:04d}")
            return False

    except Exception as exc:
        log.error(f"STEP ABEND : {program}  EXCEPTION: {exc}")
        return False


def main() -> None:
    log.info("=" * 70)
    log.info("JOB  : EIBMRPTS")
    log.info("START: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 70)

    active_steps   = [s for s in JOB_STEPS if s["active"]]
    inactive_steps = [s for s in JOB_STEPS if not s["active"]]

    log.info(f"Active steps   : {len(active_steps)}")
    log.info(f"Inactive steps : {len(inactive_steps)}  (commented out in original JCL)")
    log.info("-" * 70)

    failed_steps = []

    for step in active_steps:
        success = run_step(step)
        if not success:
            failed_steps.append(step["program"])
            # JCL default: COND=(4,LT) — continue on non-zero RC unless explicitly set
            # Preserve original JCL behaviour: log failure but continue remaining steps
            log.warning(f"Step {step['program']} failed — continuing remaining steps")

    log.info("=" * 70)
    if failed_steps:
        log.error(f"JOB END  : EIBMRPTS  FAILED STEPS: {', '.join(failed_steps)}")
        sys.exit(1)
    else:
        log.info("JOB END  : EIBMRPTS  RC=0000  ALL STEPS COMPLETED SUCCESSFULLY")
        log.info("END: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        sys.exit(0)


if __name__ == "__main__":
    main()
