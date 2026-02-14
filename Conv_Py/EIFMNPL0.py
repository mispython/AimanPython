# !/usr/bin/env python3
"""
Program: EIFMNPL0
Purpose: Master NPL Processing Job - Orchestrates multiple NPL programs

ESMR: 2004-720, 2004-579

Department: HP CREDIT CONTROL DEPARTMENT
Contact: THE MANAGER
Location: MENARA PBB, 22TH FLOOR
Address: MENARA PUBLIC BANK, 146 JALAN AMPANG, 50450 KUALA LUMPUR

This master job executes the following NPL processing programs in sequence:
1. EIFMNP03 - Interest in Suspense (IIS) Report
2. EIFMNP06 - Specific Provision (SP) Report
3. EIFMNP07 - Asset Quality (AQ) Report
4. EIFMNP21 - NPL Report 1
5. EIFMNP22 - NPL Report 2

Notes:
- EIFMNP04 discontinued as per letter dated 26/08/03 from Statistics
- EIFMNP05 disabled (ESMR 2009-1486 TSY4)
- EIFMNP11 discontinued as per letter dated 26/08/03 from Statistics
"""

import sys
import os
from pathlib import Path
from datetime import datetime

# Add current directory to path to import NPL programs
sys.path.insert(0, str(Path(__file__).parent))

# Import NPL processing modules
try:
    import EIFMNP03
except ImportError:
    EIFMNP03 = None

try:
    import EIFMNP06
except ImportError:
    EIFMNP06 = None

try:
    import EIFMNP07
except ImportError:
    EIFMNP07 = None

# Output file paths for reports
OUTPUT_IIS_TEXT = "SAP.PBB.IIS.TEXT"
OUTPUT_SP_TEXT = "SAP.PBB.SP.TEXT"
OUTPUT_AQ_TEXT = "SAP.PBB.AQ.TEXT"
OUTPUT_NPL01_TEXT = "SAP.PFB.NPL01.TEXT"
OUTPUT_NPL02_TEXT = "SAP.PFB.NPL02.TEXT"

# Report header information
REPORT_INFO = {
    "NAME": "THE MANAGER",
    "ROOM": "22TH FLOOR",
    "BUILDING": "MENARA PBB",
    "DEPT": "HP CREDIT CONTROL DEPARTMENT",
    "ADDRESS": ["MENARA PUBLIC BANK", "146 JALAN AMPANG", "50450 KUALA LUMPUR"]
}


def delete_existing_files():
    """
    Delete existing output files (equivalent to DELETE step in JCL)
    """
    print("\nDeleting existing output files...")
    files_to_delete = [
        OUTPUT_IIS_TEXT,
        OUTPUT_SP_TEXT,
        OUTPUT_AQ_TEXT,
        OUTPUT_NPL01_TEXT,
        OUTPUT_NPL02_TEXT
    ]

    for filepath in files_to_delete:
        if Path(filepath).exists():
            try:
                Path(filepath).unlink()
                print(f"  Deleted: {filepath}")
            except Exception as e:
                print(f"  Warning: Could not delete {filepath}: {e}")
        else:
            print(f"  Not found (OK): {filepath}")


def execute_step(step_name, module, description):
    """
    Execute a processing step and handle errors

    Args:
        step_name: Name of the step (e.g., "EIFMNP03")
        module: Python module to execute
        description: Description of what the step does

    Returns:
        bool: True if successful, False otherwise
    """
    print("\n" + "=" * 80)
    print(f"EXECUTING: {step_name}")
    print(f"PURPOSE: {description}")
    print("=" * 80)

    if module is None:
        print(f"Warning: {step_name} module not available. Skipping...")
        return False

    try:
        start_time = datetime.now()
        module.main()
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"\n{step_name} completed successfully in {duration:.2f} seconds")
        return True

    except Exception as e:
        print(f"\nERROR in {step_name}: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """
    Main execution function for EIFMNPL0
    Orchestrates the execution of multiple NPL processing programs
    """
    print("=" * 80)
    print("EIFMNPL0 - Master NPL Processing Job")
    print("=" * 80)
    print(f"Department: {REPORT_INFO['DEPT']}")
    print(f"Contact: {REPORT_INFO['NAME']}")
    print(f"Location: {REPORT_INFO['BUILDING']}, {REPORT_INFO['ROOM']}")
    for addr in REPORT_INFO['ADDRESS']:
        print(f"         {addr}")
    print("=" * 80)
    print(f"Execution started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # Track execution status
    results = {}
    overall_start = datetime.now()

    # Step 1: Delete existing output files
    delete_existing_files()

    # Step 2: Execute EIFMNP03 - Interest in Suspense Report
    results['EIFMNP03'] = execute_step(
        "EIFMNP03",
        EIFMNP03,
        "Movements of Interest in Suspense for the Month Ending"
    )

    # Note: EIFMNP04 is discontinued
    print("\n" + "-" * 80)
    print("NOTE: EIFMNP04 discontinued as per letter dated 26/08/03 from Statistics")
    print("-" * 80)

    # Note: EIFMNP05 is disabled
    print("\n" + "-" * 80)
    print("NOTE: EIFMNP05 disabled (ESMR 2009-1486 TSY4)")
    print("-" * 80)

    # Step 3: Execute EIFMNP06 - Specific Provision Report
    results['EIFMNP06'] = execute_step(
        "EIFMNP06",
        EIFMNP06,
        "Movements of Specific Provision for the Month Ending"
    )

    # Step 4: Execute EIFMNP07 - Asset Quality Report
    results['EIFMNP07'] = execute_step(
        "EIFMNP07",
        EIFMNP07,
        "Statistics on Asset Quality - Movements in NPL"
    )

    # Note: EIFMNP11 is discontinued
    print("\n" + "-" * 80)
    print("NOTE: EIFMNP11 (Top 20 Non Performing Accounts) discontinued")
    print("      as per letter dated 26/08/03 from Statistics")
    print("-" * 80)

    # Step 5: Execute EIFMNP21 - NPL Report 1
    # Note: EIFMNP21 and EIFMNP22 modules need to be created separately
    print("\n" + "-" * 80)
    print("NOTE: EIFMNP21 - NPL Report 1 (to be implemented)")
    print("-" * 80)

    # Step 6: Execute EIFMNP22 - NPL Report 2
    print("\n" + "-" * 80)
    print("NOTE: EIFMNP22 - NPL Report 2 (to be implemented)")
    print("-" * 80)

    # Note: SFTP transfer disabled
    print("\n" + "-" * 80)
    print("NOTE: SFTP transfer step (RUNSFTP) commented out in original JCL")
    print("      Files would be transferred to TextFile/Ccd directory:")
    print(f"        - {OUTPUT_IIS_TEXT} -> IIS@[month].TXT")
    print(f"        - {OUTPUT_SP_TEXT} -> SP@[month].TXT")
    print(f"        - {OUTPUT_AQ_TEXT} -> AQ@[month].TXT")
    print("-" * 80)

    # Calculate overall execution time
    overall_end = datetime.now()
    total_duration = (overall_end - overall_start).total_seconds()

    # Print summary
    print("\n" + "=" * 80)
    print("EXECUTION SUMMARY")
    print("=" * 80)
    print(f"Started:  {overall_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Finished: {overall_end.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {total_duration:.2f} seconds ({total_duration / 60:.2f} minutes)")
    print("\nStep Results:")

    success_count = 0
    for step, status in results.items():
        status_str = "SUCCESS" if status else "FAILED/SKIPPED"
        symbol = "✓" if status else "✗"
        print(f"  {symbol} {step}: {status_str}")
        if status:
            success_count += 1

    print(f"\nSteps executed: {success_count}/{len(results)}")

    print("\nOutput Files Generated:")
    output_files = [
        OUTPUT_IIS_TEXT,
        OUTPUT_SP_TEXT,
        OUTPUT_AQ_TEXT,
        OUTPUT_NPL01_TEXT,
        OUTPUT_NPL02_TEXT
    ]

    for filepath in output_files:
        if Path(filepath).exists():
            size = Path(filepath).stat().st_size
            print(f"  ✓ {filepath} ({size:,} bytes)")
        else:
            print(f"  ✗ {filepath} (not created)")

    # Determine overall success
    all_critical_steps_passed = (
            results.get('EIFMNP03', False) and
            results.get('EIFMNP06', False) and
            results.get('EIFMNP07', False)
    )

    print("=" * 80)

    if all_critical_steps_passed:
        print("EIFMNPL0 execution completed successfully")
        print("All critical NPL reports have been generated")
        print("=" * 80)
        return 0
    else:
        print("EIFMNPL0 execution completed with warnings or errors")
        print("Some steps failed or were skipped")
        print("=" * 80)
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
