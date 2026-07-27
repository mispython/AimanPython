#!/usr/bin/env python3
"""
Program : EIBDTP5J.py
Purpose : Driver job for the Daily Top 50 Depositors RM & FCY report.
          Mirrors the JCL job EIBDTP5J, which performs no report logic
          itself — it only executes step EIBDTP50 (the SAS609 report
          program). Runs AFTER EIBDDEPF.

          //*----------------------------------------------
          //* FTP REPORTS TO DATAWAREHOUSE SERVER
          //*----------------------------------------------
          //*RUNSFTP  EXEC COZBATCH  (disabled in source JCL — every line
          //*                        commented out — not converted)
          //*----------------------------------------------
          //* FTP HOST DATASETS TO DATA REPORT REPOSITORY SYSTEM (DRR)
          //*----------------------------------------------
          //RUNSFTP  EXEC COZBATCH   (active step — intentionally omitted
                                      here per current scope)
"""

import EIBDTP50

# ============================================================================
# OUTPUT FILES CORRESPONDING TO THE ORIGINAL CATALOGUED DATASETS
# FD11TEXT -> SAP.PBB.TOP50I.DAILY(+1)  (EIBDTP50 Individual output)
# FD12TEXT -> SAP.PBB.TOP50C.DAILY(+1)  (EIBDTP50 Corporate output)
# FD2TEXT  -> SAP.PBB.TOP50S.DAILY(+1)  (EIBDTP50 Subsidiaries output)
# ============================================================================

if __name__ == "__main__":
    # ------------------------------------------------------------------
    # STEP EIBDTP50: DAILY TOP 50 DEPOSITORS RM & FCY
    # (already executed on import above by EIBDTP50.py's module-level code)
    # ------------------------------------------------------------------
    print("\n>>> EIBDTP50 step complete.")

    print("\nEIBDTP5J job complete.")
