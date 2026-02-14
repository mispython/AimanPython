# !/usr/bin/env python3
"""
Program: EIFMNPLE
Purpose: Prepare NPL (Non-Performing Loan) accounts for new and old guidelines
         Process hire purchase loan data and classify borrower status

BORSTAT CODES DESCRIPTION:
  F - DEFICIT
  I - IRREGULAR
  R - REPOSSES
  T - INSURANCE TOTAL LOST CLAIM WHERE PAYMENT HAS NOT BEEN RECEIVED
      WILL CHANGE TO F WHENEVER PAYMENT RECEIVED BUT NOT ENOUGH TO COVER THE BALANCE
  S - RESTRUCTURE
  W - CURRENT YEAR WRITTEN OFF
  Z - PRIOR YEAR WRITTEN OFF
  Y - RECOVERY APPEAR DOUBTFUL
  A - NPL TURN PERFORMING LN (REINSTATE)

Notes: ESMR 2004-720 & 2004-579
"""

import duckdb
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import calendar

# Setup paths
INPUT_LOAN_REPTDATE = "SAP.PBB.MNILN.REPTDATE.parquet"
INPUT_LOAN_LNNOTE = "SAP.PBB.MNILN.LNNOTE.parquet"
INPUT_NPL6_WIIS = "SAP.PBB.NPL.HP.SASDATA.WIIS.parquet"
INPUT_NPL6_WAQ = "SAP.PBB.NPL.HP.SASDATA.WAQ.parquet"
INPUT_NPL6_WSP2 = "SAP.PBB.NPL.HP.SASDATA.WSP2.parquet"
INPUT_NPL6_NPLOBAL = "SAP.PBB.NPL.HP.SASDATA.NPLOBAL.parquet"

# Output paths will be dynamically named based on REPTMON
OUTPUT_NPL6_LOAN = None
OUTPUT_NPL6_PLOAN = None


def calculate_next_bldate(issdte, bldate):
    """
    Calculate next billing date based on issue date and current billing date.
    Macro: NXTBLDT
    """
    if issdte is None or bldate is None:
        return bldate

    dd = issdte.day
    mm = bldate.month + 1
    yy = bldate.year

    if mm > 12:
        mm = 1
        yy += 1

    # Determine last day of month (DCLVAR macro logic)
    if mm == 2:
        if yy % 4 == 0 and (yy % 100 != 0 or yy % 400 == 0):
            last_day = 29
        else:
            last_day = 28
    elif mm in [4, 6, 9, 11]:
        last_day = 30
    else:
        last_day = 31

    if dd > last_day:
        dd = last_day

    return datetime(yy, mm, dd).date()


def safe_date_parse(issuedt):
    """Parse ISSUEDT field to date"""
    if issuedt is None:
        return None
    try:
        issuedt_str = f"{int(issuedt):011d}"
        issdte_str = issuedt_str[:8]
        return datetime.strptime(issdte_str, "%m%d%Y").date()
    except:
        return None


def extract_census7(census):
    """Extract 7th character from CENSUS field"""
    if census is None:
        return ""
    try:
        census_str = f"{float(census):8.2f}"
        return census_str[6] if len(census_str) > 6 else ""
    except:
        return ""


def main():
    global OUTPUT_NPL6_LOAN, OUTPUT_NPL6_PLOAN

    # Read REPTDATE to get reporting month and date
    df_reptdate = pl.read_parquet(INPUT_LOAN_REPTDATE)
    reptdate = df_reptdate['REPTDATE'][0]
    reptmon = f"{reptdate.month:02d}"

    # Set output file names
    OUTPUT_NPL6_LOAN = f"SAP.PBB.NPL.HP.SASDATA.LOAN{reptmon}.parquet"
    OUTPUT_NPL6_PLOAN = f"SAP.PBB.NPL.HP.SASDATA.PLOAN{reptmon}.parquet"

    print(f"Processing for reporting month: {reptmon}")
    print(f"Reporting date: {reptdate}")

    # Read LOAN.LNNOTE
    df_lnnote_full = pl.read_parquet(INPUT_LOAN_LNNOTE)

    # PROC SORT DATA=LOAN.LNNOTE OUT=LOAN
    loan = df_lnnote_full.filter(
        (pl.col("REVERSED") != "Y") &
        (pl.col("NOTENO").is_not_null()) &
        (pl.col("PAIDIND") != "P") &
        (pl.col("NTBRCH").is_not_null()) &
        (pl.col("LOANTYPE").is_in([128, 130, 380, 381, 700, 705, 131, 132, 720, 725]))
    ).sort(["ACCTNO", "NOTENO"])

    # PROC SORT DATA=LOAN.LNNOTE OUT=LNNOTE
    lnnote = df_lnnote_full.filter(
        (pl.col("REVERSED") != "Y") &
        (pl.col("NOTENO").is_not_null()) &
        (pl.col("NTBRCH").is_not_null()) &
        (pl.col("LOANTYPE").is_in([128, 130, 700, 705, 983, 993, 996, 380, 381, 131, 132, 720, 725]))
    ).sort(["ACCTNO", pl.col("NOTENO").sort(descending=True)])

    # Drop specified columns
    cols_to_drop = ["NFEEAMT10", "NFEEAMT11", "NFEEAMT12"]
    lnnote = lnnote.drop([col for col in cols_to_drop if col in lnnote.columns])

    # PROC SORT DATA=LNNOTE NODUPKEY
    lnnote = lnnote.unique(subset=["ACCTNO"], keep="first")

    # PROC SORT DATA=LOAN.LNNOTE OUT=LOANNO
    loanno = df_lnnote_full.filter(
        (pl.col("REVERSED") != "Y") &
        (pl.col("NOTENO").is_not_null()) &
        (pl.col("PAIDIND") != "P") &
        (pl.col("NTBRCH").is_not_null()) &
        (pl.col("LOANTYPE").is_in([128, 130, 380, 381, 700, 705, 983, 993, 996, 131, 132, 720, 725]))
    ).sort(["ACCTNO", pl.col("NOTENO").sort(descending=True)])

    loanno = loanno.select(["ACCTNO", "NOTENO"]).unique(subset=["ACCTNO"], keep="first")

    # DATA LNNOTE - merge with LOANNO
    lnnote = lnnote.join(loanno, on="ACCTNO", how="inner", suffix="_new")
    if "NOTENO_new" in lnnote.columns:
        lnnote = lnnote.drop("NOTENO").rename({"NOTENO_new": "NOTENO"})

    # Update NPL6.WIIS, NPL6.WAQ, NPL6.WSP2 with LNNOTE
    df_wiis = pl.read_parquet(INPUT_NPL6_WIIS).sort("ACCTNO")
    df_wiis = df_wiis.join(lnnote, on="ACCTNO", how="left", suffix="_lnnote", coalesce=True)

    df_waq = pl.read_parquet(INPUT_NPL6_WAQ).sort("ACCTNO")
    df_waq = df_waq.join(lnnote, on="ACCTNO", how="left", suffix="_lnnote", coalesce=True)

    df_wsp2 = pl.read_parquet(INPUT_NPL6_WSP2).sort("ACCTNO")
    df_wsp2 = df_wsp2.join(lnnote, on="ACCTNO", how="left", suffix="_lnnote", coalesce=True)

    # Read NPL6.NPLOBAL
    df_nplobal = pl.read_parquet(INPUT_NPL6_NPLOBAL)
    nplobal_select = df_nplobal.select([
        "ACCTNO", "NOTENO", "NTBRCH", "NAME", "LOANTYPE", "LOANSTAT",
        "IISP", "OIP", "SPP1", "SPP2", "CURBALP", "NETBALP", "EXIST", "UHCP"
    ])

    # Drop LOANSTAT from LNNOTE for merge
    lnnote_cols = [c for c in lnnote.columns if c != "LOANSTAT"]
    lnnote_for_merge = lnnote.select(lnnote_cols)

    # Get WIIS accounts for BORSTAT='W' flag
    wiis_accts = df_wiis.select("ACCTNO").unique().with_columns(pl.lit(True).alias("_in_wiis"))

    # DATA NPLOBAL6 - merge all sources
    nplobal6 = nplobal_select.join(lnnote_for_merge, on="ACCTNO", how="full", suffix="_lnnote", coalesce=True)
    nplobal6 = nplobal6.join(wiis_accts, on="ACCTNO", how="left")

    # Set BORSTAT='W' for accounts in WIIS
    if "BORSTAT" not in nplobal6.columns:
        nplobal6 = nplobal6.with_columns(pl.lit(None).alias("BORSTAT"))

    nplobal6 = nplobal6.with_columns(
        pl.when(pl.col("_in_wiis") == True)
        .then(pl.lit("W"))
        .otherwise(pl.col("BORSTAT"))
        .alias("BORSTAT")
    )

    # Filter: IF A OR B
    nplobal6 = nplobal6.filter(
        pl.col("ACCTNO").is_in(nplobal_select["ACCTNO"]) |
        (pl.col("_in_wiis") == True)
    ).drop("_in_wiis")

    # Prepare LOAN dataset for merge (drop LOANSTAT)
    loan_cols = [c for c in loan.columns if c != "LOANSTAT"]
    loan_for_merge = loan.select(loan_cols)

    # Mark which accounts are in each dataset
    nplobal6_accts = set(nplobal6["ACCTNO"].to_list())
    loan_accts = set(loan["ACCTNO"].to_list())

    # Merge NPLOBAL6 with LOAN
    merged = nplobal6.join(loan_for_merge, on="ACCTNO", how="full", suffix="_loan", coalesce=True)

    # Filter out BORSTAT='Z'
    merged = merged.filter((pl.col("BORSTAT") != "Z") | pl.col("BORSTAT").is_null())

    # Process each row
    output_loan = []
    output_ploan = []

    for row_dict in merged.iter_rows(named=True):
        # Parse ISSDTE from ISSUEDT
        issdte = safe_date_parse(row_dict.get("ISSUEDT"))

        # Extract CENSUS7
        census7 = extract_census7(row_dict.get("CENSUS"))

        # Handle special case for ACCTNO=8095419311
        bldate = row_dict.get("BLDATE")
        if row_dict.get("ACCTNO") == 8095419311:
            bldate = datetime(1997, 1, 21).date()

        # Calculate DAYS
        days = 0
        if bldate is not None and isinstance(bldate, datetime):
            bldate = bldate.date() if not isinstance(bldate, type(reptdate)) else bldate

        if bldate is not None and bldate > datetime(1960, 1, 1).date():
            bilpay = row_dict.get("BILPAY") or 0
            if bilpay > 0:
                biltot = row_dict.get("BILTOT") or 0
                if biltot / bilpay <= 0.01:
                    bldate = calculate_next_bldate(issdte, bldate)

            days = (reptdate - bldate).days if isinstance(reptdate, type(bldate)) else (reptdate.date() - bldate).days

            oldnotedayarr = row_dict.get("OLDNOTEDAYARR") or 0
            noteno = row_dict.get("NOTENO") or 0
            if oldnotedayarr > 0 and 98000 <= noteno <= 98999:
                if days < 0:
                    days = 0
                days = days + oldnotedayarr

        # Calculate TERMCHG
        intamt = row_dict.get("INTAMT") or 0
        intearn2 = row_dict.get("INTEARN2") or 0
        termchg = (intamt + intearn2) if intamt > 0.1 else 0

        # Set BORSTAT for restructure
        borstat = row_dict.get("BORSTAT") or ""
        noteno = row_dict.get("NOTENO") or 0
        if borstat not in ["F", "I", "R", "Y", "W", "Z", "A"] and noteno in [98010, 98011, 99010, 99011]:
            borstat = "S"

        # Determine AA and BB flags
        aa = row_dict.get("ACCTNO") in nplobal6_accts
        bb = row_dict.get("ACCTNO") in loan_accts

        loanstat = row_dict.get("LOANSTAT")
        user5 = row_dict.get("USER5")

        # Apply business logic
        if aa:
            if not bb:
                loanstat = None
                days = 0
                borstat = " "
            elif days <= 0:
                loanstat = 1

            if loanstat == 1 and borstat == " ":
                borstat = "A"

            output_dataset = "LOAN"
        elif days > 89 or borstat not in [" ", "A", "C", "S", "T"] or user5 == "N":
            if borstat == "Y" and days <= 182:
                output_dataset = "PLOAN"
            else:
                output_dataset = "LOAN"
        else:
            output_dataset = "PLOAN"

        # Build output record with KEEP variables
        output_row = {
            "LOANTYPE": row_dict.get("LOANTYPE"),
            "NTBRCH": row_dict.get("NTBRCH"),
            "ACCTNO": row_dict.get("ACCTNO"),
            "NOTENO": row_dict.get("NOTENO"),
            "NAME": row_dict.get("NAME"),
            "BORSTAT": borstat,
            "LOANSTAT": loanstat,
            "NETPROC": row_dict.get("NETPROC"),
            "ORGBAL": row_dict.get("ORGBAL"),
            "CURBAL": row_dict.get("CURBAL"),
            "NOTETERM": row_dict.get("NOTETERM"),
            "ISSDTE": issdte,
            "BLDATE": bldate,
            "BILPAY": row_dict.get("BILPAY"),
            "INTRATE": row_dict.get("INTRATE"),
            "APPVALUE": row_dict.get("APPVALUE"),
            "MARKETVL": row_dict.get("MARKETVL"),
            "DAYS": days,
            "TERMCHG": termchg,
            "IISP": row_dict.get("IISP"),
            "OIP": row_dict.get("OIP"),
            "SPP1": row_dict.get("SPP1"),
            "SPP2": row_dict.get("SPP2"),
            "CURBALP": row_dict.get("CURBALP"),
            "NETBALP": row_dict.get("NETBALP"),
            "USER5": user5,
            "FEEAMT": row_dict.get("FEEAMT"),
            "FEEAMT3": row_dict.get("FEEAMT3"),
            "FEEAMT4": row_dict.get("FEEAMT4"),
            "EXIST": row_dict.get("EXIST"),
            "FEEYTD": row_dict.get("FEEYTD"),
            "FEEPDYTD": row_dict.get("FEEPDYTD"),
            "UHCP": row_dict.get("UHCP"),
            "CENSUS7": census7,
            "VINNO": row_dict.get("VINNO"),
            "FEETOT2": row_dict.get("FEETOT2"),
            "FEEAMT8": row_dict.get("FEEAMT8"),
            "PENDBRH": row_dict.get("PENDBRH"),
            "COSTCTR": row_dict.get("COSTCTR"),
            "EARNTERM": row_dict.get("EARNTERM"),
            "FEEAMTA": row_dict.get("FEEAMTA"),
            "FEEAMT5": row_dict.get("FEEAMT5"),
            "OLDNOTEDAYARR": row_dict.get("OLDNOTEDAYARR"),
            "ACCRUAL": row_dict.get("ACCRUAL")
        }

        if output_dataset == "LOAN":
            output_loan.append(output_row)
        else:
            output_ploan.append(output_row)

    # Convert to Polars DataFrames
    df_loan_output = pl.DataFrame(output_loan) if output_loan else pl.DataFrame()
    df_ploan_output = pl.DataFrame(output_ploan) if output_ploan else pl.DataFrame()

    # Write outputs
    df_loan_output.write_parquet(OUTPUT_NPL6_LOAN)
    df_ploan_output.write_parquet(OUTPUT_NPL6_PLOAN)

    print(f"\nOutput files created:")
    print(f"  {OUTPUT_NPL6_LOAN} ({len(df_loan_output)} records)")
    print(f"  {OUTPUT_NPL6_PLOAN} ({len(df_ploan_output)} records)")
    print("\nProcessing complete.")


if __name__ == "__main__":
    main()
