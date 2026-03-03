# !/usr/bin/env python3
"""
Program : EIBDMISA.py
SMR     : 2008-1076  ALLIN A/K PATRICK / LOW SOOK KUAN
Purpose : Orchestrator job that sequentially deletes prior output files and
          executes the full suite of MIS Islamic FD report programs.
          Converted from IBM z/OS JCL (EIBDMISA).

JCL job steps converted:
  DELETE  - clean up prior output files (replicated as os.remove with ignore)
  DIBMISA2 - NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT (04-SEP-04 TO 15-APR-06)
  DIBMISA3 - NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT (16-APR-06 TO 15-SEP-08)
  DIBMISA4 - NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT (16-SEP-08 ONWARDS)
  EIBDTP50 - DAILY TOP 50 DEPOSITORS - RM KJW PIBB  [external dependency]
  DIIMISB2 - AL-MUDHARABAH FD - BEFORE 04-SEP-04 BY REMAINING MATURITY
  DIIMISA2 - AL-MUDHARABAH FD - 04-SEP-04 TO 15-APR-06 BY REMAINING MATURITY
  DIIMISA3 - AL-MUDHARABAH FD - 16-APR-06 TO 15-SEP-08 BY REMAINING MATURITY
  DIIMISA5 - AL-MUDHARABAH FD - 16-MAR-09 ONWARDS BY REMAINING MATURITY
  DIIMISA6 - AL-MUDHARABAH FD - 16-SEP-08 TO 15-MAR-09 BY REMAINING MATURITY
  DIIMISC1 - AL-MUDHARABAH FD - BEFORE 04-SEP-04 BY REMAINING MATURITY
  DIIMISC2 - AL-MUDHARABAH FD - 04-SEP-04 TO 15-APR-06 BY REMAINING MATURITY
  DIIMISC3 - AL-MUDHARABAH FD - 16-APR-06 TO 15-SEP-08 BY REMAINING MATURITY
  DIIMISC5 - AL-MUDHARABAH FD - 16-MAR-09 TO 15-MAR-10 BY REMAINING MATURITY
  DIIMISC6 - AL-MUDHARABAH FD - 16-SEP-08 TO 15-MAR-09 BY REMAINING MATURITY
  DIIMISC7 - AL-MUDHARABAH FD - 19-MAR-10 TO 15-JUN-10 BY REMAINING MATURITY
  DIIMIS7Y - AL-MUDHARABAH FD - 16-JUN-10 TO 19-JUL-10 BY REMAINING MATURITY
  DIIMIS7K - AL-MUDHARABAH FD - 20-JUL-10 TO 15-MAY-11 BY REMAINING MATURITY
  DIIMIS7L - AL-MUDHARABAH FD - 16-MAY-11 TO 15-JAN-12 BY REMAINING MATURITY
  DIIMIS7M - AL-MUDHARABAH FD - 16-JAN-12 TO 15-JUN-13 BY REMAINING MATURITY
  DIIMIS7O - AL-MUDHARABAH FD - 16-JUN-13 ONWARDS BY REMAINING MATURITY
  DIIMIS7P - AL-MUDHARABAH FD - 16-SEP-16 ONWARDS BY REMAINING MATURITY
  DIIMIS7W - AL-MUDHARABAH FD - ACCTTYPE = 315
  DIIMIS7V - AL-MUDHARABAH FD - ACCTTYPE = 394
  DIIMIS7G - TERM DEPOSIT-I   - 01-JUL-14 ONWARDS (ACCTTYPE = 316, 393)
"""

import os
import sys
import importlib
import traceback

# ===========================================================================
# PATH CONFIGURATION
# ===========================================================================
INPUT_DIR  = os.environ.get('INPUT_DIR',  'input')
OUTPUT_DIR = os.environ.get('OUTPUT_DIR', 'output')

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===========================================================================
# OUTPUT FILES TO CLEAN UP BEFORE RUNNING
# Mirrors the DELETE EXEC PGM=IEFBR14 step in the JCL which purges all
# prior output datasets (SAP.PBB.DIBMISA2, SAP.PIBB.DIIMIS7O, etc.)
# ===========================================================================
OUTPUT_FILES_TO_DELETE = [
    # SAP.PBB.*
    'DIBMISA2.txt',
    'DIBMISA3.txt',
    'DIBMISA4.txt',
    # SAP.PIBB.*
    'DIIMISA2.txt',
    'DIIMISA3.txt',
    'DIIMISA5.txt',
    'DIIMISA6.txt',
    'DIIMISB2.txt',
    'DIIMISC1.txt',
    'DIIMISC2.txt',
    'DIIMISC3.txt',
    'DIIMISC5.txt',
    'DIIMISC6.txt',
    'DIIMISC7.txt',
    'DIIMIS7Y.txt',
    'DIIMIS7K.txt',
    'DIIMIS7L.txt',
    'DIIMIS7M.txt',
    'DIIMIS7W.txt',
    'DIIMIS7V.txt',
    'DIIMIS7O.txt',
    'DIIMIS7G.txt',
    'DIIMIS7P.txt',
]


def delete_prior_outputs():
    """
    DELETE step: remove prior output files if they exist.
    Equivalent to IEFBR14 with DISP=(MOD,DELETE,DELETE) in JCL.
    Failures are silently ignored (files may not exist on first run).
    """
    print('--- DELETE: cleaning up prior output files ---')
    for fname in OUTPUT_FILES_TO_DELETE:
        fpath = os.path.join(OUTPUT_DIR, fname)
        try:
            os.remove(fpath)
            print(f'  Deleted: {fpath}')
        except FileNotFoundError:
            pass   # file did not exist; equivalent to MOD,DELETE,DELETE on empty dataset
        except OSError as exc:
            print(f'  WARNING: could not delete {fpath}: {exc}', file=sys.stderr)


# ===========================================================================
# PROGRAM EXECUTION HELPER
# ===========================================================================

def run_program(module_name: str, description: str):
    """
    Dynamically import and execute a report module's main() function.
    Equivalent to one EXEC SAS609 / SYSIN step in the JCL.
    """
    print(f'\n--- {module_name}: {description} ---')
    try:
        mod = importlib.import_module(module_name)
        if hasattr(mod, 'main'):
            mod.main()
        else:
            print(f'  WARNING: {module_name} has no main() function - skipping.',
                  file=sys.stderr)
    except ModuleNotFoundError:
        print(f'  WARNING: module {module_name} not found - skipping step.',
              file=sys.stderr)
    except Exception:
        print(f'  ERROR in {module_name}:', file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        # Continue with remaining steps (matches JCL COND= default behaviour
        # where subsequent steps run regardless of prior step RC unless
        # explicitly conditioned - none are conditioned here)


# ===========================================================================
# MAIN ORCHESTRATION
# ===========================================================================

def main():
    # ------------------------------------------------------------------
    # Step: DELETE - purge prior output datasets
    # ------------------------------------------------------------------
    delete_prior_outputs()

    # ------------------------------------------------------------------
    # Step: DIBMISA2
    # NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
    # PERIOD OF 04-SEP-04 TO 15-APR-06
    # ------------------------------------------------------------------
    run_program('DIBMISA2',
                'NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT (04-SEP-04 TO 15-APR-06)')

    # ------------------------------------------------------------------
    # Step: DIBMISA3
    # NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
    # PERIOD OF 16-APR-06 TO 15-SEP-08
    # ------------------------------------------------------------------
    run_program('DIBMISA3',
                'NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT (16-APR-06 TO 15-SEP-08)')

    # ------------------------------------------------------------------
    # Step: DIBMISA4
    # NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
    # 16-SEP-08 ONWARDS
    # ------------------------------------------------------------------
    run_program('DIBMISA4',
                'NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT (16-SEP-08 ONWARDS)')

    # ------------------------------------------------------------------
    # Step: EIBDTP50
    # DAILY TOP 50 DEPOSITORS - RM KJW PIBB
    # (SAP.BNM.PROGRAM(EIIDTOP5) in JCL - external dependency)
    # ------------------------------------------------------------------
    run_program('EIIDTOP5',
                'DAILY TOP 50 DEPOSITORS - RM KJW PIBB')

    # ------------------------------------------------------------------
    # Step: DIIMISB2
    # NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
    # PERIOD OF BEFORE 04-SEP-04 BY REMAINING MATURITY
    # ------------------------------------------------------------------
    run_program('DIIMISB2',
                'AL-MUDHARABAH FD - BEFORE 04-SEP-04 BY REMAINING MATURITY')

    # ------------------------------------------------------------------
    # Step: DIIMISA2
    # NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
    # PERIOD OF 04-SEP-04 UNTIL 15-APR-06 BY REMAINING MATURITY
    # ------------------------------------------------------------------
    run_program('DIIMISA2',
                'AL-MUDHARABAH FD - 04-SEP-04 TO 15-APR-06 BY REMAINING MATURITY')

    # ------------------------------------------------------------------
    # Step: DIIMISA3
    # NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
    # PERIOD OF 16-APR-06 UNTIL 15-SEP-08 BY REMAINING MATURITY
    # ------------------------------------------------------------------
    run_program('DIIMISA3',
                'AL-MUDHARABAH FD - 16-APR-06 TO 15-SEP-08 BY REMAINING MATURITY')

    # ------------------------------------------------------------------
    # Step: DIIMISA5
    # NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
    # PERIOD OF 16-MAR-09 ONWARDS BY REMAINING MATURITY
    # ------------------------------------------------------------------
    run_program('DIIMISA5',
                'AL-MUDHARABAH FD - 16-MAR-09 ONWARDS BY REMAINING MATURITY')

    # ------------------------------------------------------------------
    # Step: DIIMISA6
    # NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
    # PERIOD OF 16-SEP-08 UNTIL 15-MAR-09 BY REMAINING MATURITY
    # ------------------------------------------------------------------
    run_program('DIIMISA6',
                'AL-MUDHARABAH FD - 16-SEP-08 TO 15-MAR-09 BY REMAINING MATURITY')

    # ------------------------------------------------------------------
    # Step: DIIMISC1
    # NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
    # PERIOD OF BEFORE 04-SEP-04 BY REMAINING MATURITY
    # ------------------------------------------------------------------
    run_program('DIIMISC1',
                'AL-MUDHARABAH FD (C-series) - BEFORE 04-SEP-04 BY REMAINING MATURITY')

    # ------------------------------------------------------------------
    # Step: DIIMISC2
    # NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
    # PERIOD OF 04-SEP-04 UNTIL 15-APR-06 BY REMAINING MATURITY
    # ------------------------------------------------------------------
    run_program('DIIMISC2',
                'AL-MUDHARABAH FD (C-series) - 04-SEP-04 TO 15-APR-06 BY REMAINING MATURITY')

    # ------------------------------------------------------------------
    # Step: DIIMISC3
    # NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
    # PERIOD OF 16-APR-06 UNTIL 15-SEP-08 BY REMAINING MATURITY
    # ------------------------------------------------------------------
    run_program('DIIMISC3',
                'AL-MUDHARABAH FD (C-series) - 16-APR-06 TO 15-SEP-08 BY REMAINING MATURITY')

    # ------------------------------------------------------------------
    # Step: DIIMISC5
    # NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
    # PERIOD OF 16-MAR-09 UNTIL 15-MAR-10 BY REMAINING MATURITY
    # ------------------------------------------------------------------
    run_program('DIIMISC5',
                'AL-MUDHARABAH FD (C-series) - 16-MAR-09 TO 15-MAR-10 BY REMAINING MATURITY')

    # ------------------------------------------------------------------
    # Step: DIIMISC6
    # NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
    # PERIOD OF 16-SEP-08 UNTIL 15-MAR-09 BY REMAINING MATURITY
    # ------------------------------------------------------------------
    run_program('DIIMISC6',
                'AL-MUDHARABAH FD (C-series) - 16-SEP-08 TO 15-MAR-09 BY REMAINING MATURITY')

    # ------------------------------------------------------------------
    # Step: DIIMISC7
    # NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
    # PERIOD OF 19-MAR-10 UNTIL 15-JUN-10 BY REMAINING MATURITY
    # ------------------------------------------------------------------
    run_program('DIIMISC7',
                'AL-MUDHARABAH FD (C-series) - 19-MAR-10 TO 15-JUN-10 BY REMAINING MATURITY')

    # ------------------------------------------------------------------
    # Step: DIIMIS7Y
    # NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
    # PERIOD OF 16-JUN-10 UNTIL 19-JUL-10 BY REMAINING MATURITY
    # ------------------------------------------------------------------
    run_program('DIIMIS7Y',
                'AL-MUDHARABAH FD - 16-JUN-10 TO 19-JUL-10 BY REMAINING MATURITY')

    # ------------------------------------------------------------------
    # Step: DIIMIS7K
    # NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
    # PERIOD OF 20-JUL-10 UNTIL 15-MAY-11 BY REMAINING MATURITY
    # ------------------------------------------------------------------
    run_program('DIIMIS7K',
                'AL-MUDHARABAH FD - 20-JUL-10 TO 15-MAY-11 BY REMAINING MATURITY')

    # ------------------------------------------------------------------
    # Step: DIIMIS7L
    # NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
    # PERIOD OF 16-MAY-11 UNTIL 15-JAN-12 BY REMAINING MATURITY
    # ------------------------------------------------------------------
    run_program('DIIMIS7L',
                'AL-MUDHARABAH FD - 16-MAY-11 TO 15-JAN-12 BY REMAINING MATURITY')

    # ------------------------------------------------------------------
    # Step: DIIMIS7M
    # NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
    # PERIOD OF 16-JAN-12 UNTIL 15-JUN-13 BY REMAINING MATURITY
    # ------------------------------------------------------------------
    run_program('DIIMIS7M',
                'AL-MUDHARABAH FD - 16-JAN-12 TO 15-JUN-13 BY REMAINING MATURITY')

    # ------------------------------------------------------------------
    # Step: DIIMIS7O
    # NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
    # PERIOD OF 16-JUN-13 ONWARDS BY REMAINING MATURITY
    # ------------------------------------------------------------------
    run_program('DIIMIS7O',
                'AL-MUDHARABAH FD - 16-JUN-13 ONWARDS BY REMAINING MATURITY')

    # ------------------------------------------------------------------
    # Step: DIIMIS7P
    # NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
    # PERIOD OF 16-SEP-16 ONWARDS BY REMAINING MATURITY
    # ------------------------------------------------------------------
    run_program('DIIMIS7P',
                'AL-MUDHARABAH FD - 16-SEP-16 ONWARDS BY REMAINING MATURITY')

    # ------------------------------------------------------------------
    # Step: DIIMIS7W
    # NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
    # ACCTTYPE = 315
    # ------------------------------------------------------------------
    run_program('DIIMIS7W',
                'AL-MUDHARABAH FD - ACCTTYPE = 315')

    # ------------------------------------------------------------------
    # Step: DIIMIS7V
    # NEW PBB & PFB AL-MUDHARABAH FD ACCOUNT AFTER PRIVATISATION
    # ACCTTYPE = 394
    # ------------------------------------------------------------------
    run_program('DIIMIS7V',
                'AL-MUDHARABAH FD - ACCTTYPE = 394')

    # ------------------------------------------------------------------
    # Step: DIIMIS7G
    # NEW PBB & PFB TERM DEPOSIT-I FD ACCOUNT AFTER PRIVATISATION
    # PERIOD OF 01-JUL-14 ONWARDS - ACCTTYPE = 316 AND 393
    # (Note: DIIMIS7H / ODD DAYS TENURE is a related program not listed
    #  in this JCL but part of the same family; add here if required)
    # ------------------------------------------------------------------
    run_program('DIIMIS7G',
                'TERM DEPOSIT-I - 01-JUL-14 ONWARDS (ACCTTYPE = 316, 393)')

    print('\n--- EIBDMISA: all steps completed ---')


if __name__ == '__main__':
    main()
