# !/usr/bin/env python3
"""
Program: LNCCD006
Date: 01.01.98
Function: To provide GP3 reports for CCD PFB (The month-end version)
Date Modified: 12-8-2002 (LN6)
Changes Made: To include Islamic loans
"""

import polars as pl
import duckdb
from datetime import datetime
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

# Input paths
INPUT_PATH_LNNOTE = "SAP_PBB_MNILN_0_LNNOTE.parquet"
INPUT_PATH_LNCOMM = "SAP_PBB_MNILN_0_LNCOMM.parquet"

# Output path
OUTPUT_PATH_LOANTEMP = "BNM_LOANTEMP.parquet"

# Product codes
# %INC PGM(PBBLNFMT) - HPD products
HPD_PRODUCTS = [380, 381, 700, 705, 720, 725, 128, 130, 131, 132]

# ABBA products
ABBA_PRODUCTS = [100, 101, 102, 105, 106, 120, 121, 127, 126,
                 112, 113, 114, 116, 117, 118]


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def process_lnccd006(rdate, con=None):
    """
    Main processing function for LNCCD006.

    Args:
        rdate: Report date string in DD/MM/YY format
        con: DuckDB connection (optional, will create if not provided)

    Returns:
        Polars DataFrame containing LOANTEMP data
    """
    close_con = False
    if con is None:
        con = duckdb.connect()
        close_con = True

    try:
        # Parse report date
        thisdate = datetime.strptime(rdate, '%d/%m/%y')

        # Load LNNOTE
        print("Loading LNNOTE data...")
        lnnote_df = con.execute(f"""
            SELECT * FROM read_parquet('{INPUT_PATH_LNNOTE}')
            WHERE REVERSED <> 'Y' OR REVERSED IS NULL
        """).pl()

        # Load LNCOMM
        print("Loading LNCOMM data...")
        con.execute(f"""
            CREATE OR REPLACE VIEW lncomm_dedup AS 
            SELECT DISTINCT * FROM read_parquet('{INPUT_PATH_LNCOMM}')
        """)

        lncomm_df = con.execute("SELECT * FROM lncomm_dedup").pl()

        # Merge LNNOTE with LNCOMM
        print("Merging LNNOTE with LNCOMM...")
        merged_df = lnnote_df.join(
            lncomm_df,
            on=['ACCTNO', 'COMMNO'],
            how='left'
        )

        # Process records
        print("Processing loan records...")
        loantemp_records = []

        # Days in month array (D1-D12)
        lday = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

        for row in merged_df.iter_rows(named=True):
            noteno = row.get('NOTENO')
            paidind = row.get('PAIDIND')
            product = row.get('LOANTYPE') or row.get('PRODUCT', 0)

            if noteno is None:
                continue

            # Determine branch (PENDBRH or NTBRCH)
            branch = row.get('PENDBRH', 0)
            if branch == 0:
                branch = row.get('NTBRCH', 0)

            # Parse issue date
            issuedt = row.get('ISSUEDT')
            if issuedt:
                try:
                    issdte_str = str(issuedt).zfill(11)[:8]
                    issdte = datetime.strptime(issdte_str, '%m%d%Y')
                except:
                    issdte = None
            else:
                issdte = None

            # Initialize variables
            curbal = row.get('CURBAL', 0) or 0
            feeamt = row.get('FEEAMT', 0) or 0
            feeamt4 = row.get('FEEAMT4', 0) or 0
            intamt = row.get('INTAMT', 0) or 0
            intearn = row.get('INTEARN', 0) or 0
            rebate = row.get('REBATE', 0) or 0
            intearn2 = row.get('INTEARN2', 0) or 0
            intearn3 = row.get('INTEARN3', 0) or 0
            intearn4 = row.get('INTEARN4', 0) or 0
            accrual = row.get('ACCRUAL', 0) or 0
            cavaiamt = row.get('CAVAIAMT', 0) or 0
            noteterm = row.get('NOTETERM', 0) or 0
            balance = 0

            # Process based on PAIDIND and PRODUCT
            # +-----------------------------------------------------+
            # |  INCLUDE ALL PAIDIND NE 'P' LOANS                   |
            # +-----------------------------------------------------+
            if paidind != 'P' and product not in ABBA_PRODUCTS:
                if intamt == 0.01:
                    # Special case: reverse rebate and intearn4
                    temp = -1 * rebate
                    rebate = temp
                    temp = -1 * intearn4
                    intearn4 = temp
                    balance = curbal + feeamt + rebate + intearn4
                    intamt = 0
                    intearn = 0
                    accrual = 0
                    intearn2 = 0
                    intearn3 = 0
                elif ((rebate + intearn) != intamt or
                      (intearn4 + intearn3) != intearn2):
                    # Recalculate using Rule of 78
                    rmm = thisdate.month
                    ryy = thisdate.year
                    if issdte:
                        imm = issdte.month
                        iyy = issdte.year
                        remain = noteterm - (((ryy * 12) + rmm + 1) - ((iyy * 12) + imm))
                        if noteterm > 0:
                            unearn1 = (remain * (remain + 1) * intamt) / (noteterm * (noteterm + 1))
                            unearn2 = (remain * (remain + 1) * intearn2) / (noteterm * (noteterm + 1))
                        else:
                            unearn1 = 0
                            unearn2 = 0
                        balance = curbal + (-1) * unearn1 + (-1) * unearn2 + feeamt
                    else:
                        balance = curbal + feeamt
                    intamt = 0
                    intearn = 0
                    accrual = 0
                    rebate = 0
                    intearn2 = 0
                    intearn3 = 0
                    intearn4 = 0
                else:
                    rebate = 0
                    intearn4 = 0
                    balance = curbal + feeamt

                # Calculate day difference and arrears
                rec = calculate_day_arrears(row, thisdate, issdte, balance, lday)
                if rec:
                    loantemp_records.append(rec)

            # +-----------------------------------------------------+
            # |  TO INCLUDE ISLAMIC MANIPULATION                    |
            # +-----------------------------------------------------+
            if paidind != 'P' and product in ABBA_PRODUCTS:
                rebmtd = intamt - intearn  # Calculate rebate as @MTH-END
                # Compute balance for ABBA loans which require CAVAIAMT
                feeamt = feeamt + feeamt4
                if cavaiamt not in (0, None):
                    balance = (curbal - rebmtd) + feeamt - cavaiamt
                else:
                    balance = (curbal - rebmtd) + feeamt

                # * IF PRODUCT = 140 THEN BALANCE=(CURBAL-REBMTD)+FEEAMT;
                if product in [139, 117, 118, 119]:
                    balance = curbal + accrual + feeamt

                rec = calculate_day_arrears(row, thisdate, issdte, balance, lday)
                if rec:
                    loantemp_records.append(rec)

            # +-----------------------------------------------------+
            # |  INCLUDE OUTSTANDING FEE AMOUNT FOR PAID LOANS      |
            # +-----------------------------------------------------+
            if paidind == 'P' and feeamt > 0:
                balance = feeamt
                rec = calculate_day_arrears(row, thisdate, issdte, balance, lday)
                if rec:
                    loantemp_records.append(rec)

            # +-----------------------------------------------------+
            # |  INCLUDE LOANS WHICH ARE SETTLED EARLY, WITHIN      |
            # |  THE SAME MONTH                                     |
            # +-----------------------------------------------------+
            curmonth = thisdate.month
            curyear = thisdate.year
            lasttran = row.get('LASTTRAN')
            if lasttran:
                try:
                    lndate_str = str(lasttran).zfill(11)[:8]
                    lndate = datetime.strptime(lndate_str, '%m%d%Y')
                    lnmonth = lndate.month
                    lnyear = lndate.year
                except:
                    lnmonth = 0
                    lnyear = 0
            else:
                lnmonth = 0
                lnyear = 0

            if (paidind == 'P' and lnmonth == curmonth and
                    lnyear == curyear and row.get('NTINT') == 'A'):
                if intamt == 0.01:
                    balance = 0
                else:
                    balance = ((intamt - intearn) - rebate) + ((intearn2 - intearn3) - intearn4)
                    if balance < 0:
                        newbal = abs(balance)
                    else:
                        newbal = 0 - balance
                    balance = newbal

                rec = calculate_day_arrears(row, thisdate, issdte, balance, lday)
                if rec:
                    loantemp_records.append(rec)

        # Create LOANTEMP dataframe
        if loantemp_records:
            loantemp_df = pl.DataFrame(loantemp_records)

            # Sort by BRANCH and ARREAR
            loantemp_df = loantemp_df.sort(['BRANCH', 'ARREAR'])

            # Save to parquet
            loantemp_df.write_parquet(OUTPUT_PATH_LOANTEMP)
            print(f"Generated {len(loantemp_df)} records in LOANTEMP")
            print(f"Saved to: {OUTPUT_PATH_LOANTEMP}")

            return loantemp_df
        else:
            print("No records generated for LOANTEMP")
            return pl.DataFrame()

    finally:
        if close_con:
            con.close()


def calculate_day_arrears(row, thisdate, issdte, balance, lday):
    """
    Calculate day difference and arrears categories.

    COUNTDAY: subroutine from original SAS program.
    """
    bldate = row.get('BLDATE')
    bilpay = row.get('BILPAY', 0) or 0
    biltot = row.get('BILTOT', 0) or 0
    oldnotedayarr = row.get('OLDNOTEDAYARR', 0) or 0
    borstat = row.get('BORSTAT', '')

    daydiff = 0

    if bldate and bldate > 0:
        # Adjust bill date if needed
        if bilpay > 0:
            if biltot / bilpay <= 0.01 if bilpay > 0 else False:
                if issdte:
                    dd = issdte.day
                    if hasattr(bldate, 'month'):
                        mm = bldate.month + 1
                        yy = bldate.year
                    else:
                        mm = thisdate.month
                        yy = thisdate.year

                    if mm > 12:
                        mm = 1
                        yy += 1

                    # Adjust for leap year
                    if mm == 2:
                        if yy % 4 == 0:
                            lday[1] = 29
                        else:
                            lday[1] = 28

                    # Adjust day if needed
                    if dd > lday[mm - 1]:
                        dd = lday[mm - 1]

                    try:
                        bldate = datetime(yy, mm, dd)
                    except:
                        pass

        # Calculate day difference
        if hasattr(bldate, 'year'):
            daydiff = (thisdate - bldate).days
        else:
            daydiff = 0

    # /* IF 200<= PRODUCT<= 299 AND BORSTAT EQ 'A' THEN DO;
    #       IF  '20OCT00'D <= THISDATE <= '30JUN01'D THEN
    #         DAYDIFF = DAYDIFF - (THISDATE - '20OCT00'D);
    #       IF THISDATE > '30JUN01'D THEN
    #          DAYDIFF = DAYDIFF - ('30JUN01'D-'20OCT00'D);
    #    END;   */

    # Add old note day arrears for restructured loans (98000-98999)
    noteno = row.get('NOTENO', 0)
    if oldnotedayarr > 0 and 98000 <= noteno <= 98999:
        if daydiff < 0:
            daydiff = 0
        daydiff += oldnotedayarr

    # Calculate ARREAR2 (14 categories)
    arrear2 = 1
    # * WHEN (BORSTAT = 'F')  ARREAR2=14;
    if bldate and bldate > 0:
        if daydiff < 31:
            arrear2 = 1
        elif daydiff < 60:
            arrear2 = 2
        elif daydiff < 90:
            arrear2 = 3
        elif daydiff < 122:
            arrear2 = 4
        elif daydiff < 152:
            arrear2 = 5
        elif daydiff < 183:
            arrear2 = 6
        elif daydiff < 214:
            arrear2 = 7
        elif daydiff < 244:
            arrear2 = 8
        elif daydiff < 274:
            arrear2 = 9
        elif daydiff < 365:
            arrear2 = 10
        elif daydiff < 548:
            arrear2 = 11
        elif daydiff < 730:
            arrear2 = 12
        elif daydiff < 1095:
            arrear2 = 13
        else:
            arrear2 = 14

    # Calculate ARREAR (16 categories)
    arrear = 1
    # * WHEN (BORSTAT = 'F')  ARREAR=17;
    if bldate and bldate > 0:
        if daydiff < 31:
            arrear = 1
        elif daydiff < 60:
            arrear = 2
        elif daydiff < 90:
            arrear = 3
        elif daydiff < 122:
            arrear = 4
        elif daydiff < 152:
            arrear = 5
        elif daydiff < 183:
            arrear = 6
        elif daydiff < 214:
            arrear = 7
        elif daydiff < 244:
            arrear = 8
        elif daydiff < 274:
            arrear = 9
        elif daydiff < 304:
            arrear = 10
        elif daydiff < 334:
            arrear = 11
        elif daydiff < 365:
            arrear = 12
        elif daydiff < 548:
            arrear = 13
        elif daydiff < 730:
            arrear = 14
        elif daydiff < 1095:
            arrear = 15
        else:
            arrear = 16

    # Determine LOANSTAT
    loanstat = row.get('LOANSTAT')

    # Build return record with KEEP= variables
    rec = {
        'BRANCH': row.get('PENDBRH', 0) or row.get('NTBRCH', 0),
        'ACCTNO': row.get('ACCTNO'),
        'NAME': row.get('NAME'),
        'NOTENO': noteno,
        'PRODUCT': row.get('LOANTYPE') or row.get('PRODUCT'),
        'BALANCE': balance,
        'BORSTAT': borstat,
        'BLDATE': bldate,
        'ARREAR': arrear,
        'THISDATE': thisdate,
        'DAYDIFF': daydiff,
        'LOANSTAT': loanstat,
        'BILPAY': bilpay,
        'ARREAR2': arrear2,
        'CHECKDT': 1 if issdte and issdte >= datetime(1998, 1, 1) else 0,
        'CENSUS': row.get('CENSUS'),
        'COLLDESC': row.get('COLLDESC'),
        'ISSDTE': issdte,
        'FEEDUE': row.get('FEEDUE'),
        'OLDNOTEDAYARR': oldnotedayarr
    }

    return rec


if __name__ == "__main__":
    # Example usage
    rdate = "22/02/24"  # Replace with actual report date
    loantemp_df = process_lnccd006(rdate)
    print("LNCCD006 processing complete")
