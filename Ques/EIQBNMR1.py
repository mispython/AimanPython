import polars as pl
from datetime import date
from pathlib import Path

from REPTDATE import get_reptdate_values

def eiqbnmr1():
    base = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
    sas_path = base / "input/uat/ln05126.sas7bdat"
    btsas_path = base / "input/uat/btrad04426.sas7bdat"
    
    # Define macro variables as Python lists
    unwanted_ln = [110,111,112,113,114,115,116,117,118,119,
                   128,130,131,132,135,136,138,139,140,141,142,199,
                   315,320,325,330,340,355,380,381,500,520,
                   700,705,720,725]
    
    unwanted_od = [107,126,127,128,129,130,131,132,133,134,
                   135,136,140,141,142,143,144,145,146,147,148,
                   149,150,171,172,173,549,550]
    
    corp_custcd = ['4','5','6','13','17','20','30','31','32',
                  '33','34','35','37','38','39','40','45',
                  '57','59','61','62','63','64','71','72',
                  '73','74','75','82','83','84','86','90',
                  '91','92','98']
    
    sme_custcd = ['41','42','43','44','46','47','48','49',
                 '51','52','53','54','66','67','68','69']
    
    reptdate_values = get_reptdate_values(year_format="%Y")

    # Macro variable equivalents
    reptdate = reptdate_values.reptdate
    REPTYEAR = reptdate_values.reptyear          # 4-digit year for this program's file names
    REPTMON  = reptdate_values.reptmon           # zero-padded month (Z2.)
    REPTDAY  = reptdate_values.reptday           # zero-padded day   (Z2.)
    REPTDT   = reptdate_values.reptdt            # raw SAS date integer equivalent (used for filter)
    RDATE    = reptdate_values.rdate             # date object used in DATA ECP step
    NOWK     = reptdate_values.nowk              # zero-padded 1-digit week number (Z1.)
    
    # Week determination
    reptday = reptdate.day
    if reptday == 8:
        nowk = '1'
    elif reptday == 15:
        nowk = '2'
    elif reptday == 22:
        nowk = '3'
    else:
        nowk = '4'
    
    reptyear = str(reptdate.year)
    reptmon = f"{reptdate.month:02d}"
    reptday_str = f"{reptdate.day:02d}"
    rdate = reptdate.strftime("%d%m%y")
    
    print(f"REPORT ID: EIQBNMR1")
    print(f"Date: {rdate}, Week: {nowk}, Month: {reptmon}")
    print("=" * 60)
    
    # Helper function to read datasets
    def read_dataset(path, file_name):
        try:
            return pl.read_parquet(path / file_name)
        except:
            return pl.DataFrame()
    
    # Read loan datasets
    loan_df = read_dataset(sas_path, f"LOAN{reptmon}{nowk}.parquet")
    lnwod_df = read_dataset(sas_path, f"LNWOD{reptmon}{nowk}.parquet")
    lnwof_df = read_dataset(sas_path, f"LNWOF{reptmon}{nowk}.parquet")
    
    # Combine loan datasets
    all_loan_df = pl.concat([loan_df, lnwod_df, lnwof_df])
    
    if all_loan_df.is_empty():
        print("No loan data found")
        return
    
    # 1. Process LN accounts (Term Loans)
    if not all_loan_df.is_empty():
        # Filter LN accounts
        ln_data = all_loan_df.filter(
            (pl.col("ACCTYPE") == "LN") &
            (~pl.col("PRODUCT").is_in(unwanted_ln)) &
            (
                ((pl.col("PRODUCT") < 200) | (pl.col("PRODUCT") > 299)) &
                ((pl.col("PRODUCT") < 981) | (pl.col("PRODUCT") > 996))
            )
        )
        
        # Split into CORP and SME
        lncorp_df = ln_data.filter(pl.col("CUSTCD").is_in(corp_custcd))
        lnsme_df = ln_data.filter(pl.col("CUSTCD").is_in(sme_custcd))
    
    # 2. Process OD accounts (Overdrafts)
    if not all_loan_df.is_empty():
        od_data = all_loan_df.filter(
            (pl.col("ACCTYPE") == "OD") &
            (~pl.col("PRODUCT").is_in(unwanted_od))
        )
        
        # Split into CORP and SME
        odcorp_df = od_data.filter(pl.col("CUSTCD").is_in(corp_custcd))
        odsme_df = od_data.filter(pl.col("CUSTCD").is_in(sme_custcd))
    
    # 3. Process BT accounts (Bills/Trust Receipts)
    bt_df = read_dataset(btsas_path, f"BTRAD{reptmon}{nowk}.parquet")
    
    if not bt_df.is_empty():
        # Rename ACCTNO1 to ACCTNO
        bt_df = bt_df.rename({"ACCTNO": "ACCTNO1"})
        bt_df = bt_df.with_columns(
            pl.col("ACCTNO1").alias("ACCTNO"),
            pl.lit("BT").alias("ACCTYPE")
        )
        
        # Split into CORP and SME
        btcorp_df = bt_df.filter(pl.col("CUSTCD").is_in(corp_custcd))
        btsme_df = bt_df.filter(pl.col("CUSTCD").is_in(sme_custcd))
    
    # 4. Combine CORP datasets
    corp_dfs = []
    for df_name in ['lncorp_df', 'odcorp_df', 'btcorp_df']:
        if df_name in locals() and not locals()[df_name].is_empty():
            corp_dfs.append(locals()[df_name].with_columns(
                pl.lit("CORPORATE LOANS").alias("CATEG")
            ))
    
    corp_df = pl.concat(corp_dfs) if corp_dfs else pl.DataFrame()
    
    # 5. Combine SME datasets
    sme_dfs = []
    for df_name in ['lnsme_df', 'odsme_df', 'btsme_df']:
        if df_name in locals() and not locals()[df_name].is_empty():
            sme_dfs.append(locals()[df_name].with_columns(
                pl.lit("SME LOANS").alias("CATEG")
            ))
    
    sme_df = pl.concat(sme_dfs) if sme_dfs else pl.DataFrame()
    
    # 6. Combine all data
    totln_dfs = []
    if not corp_df.is_empty():
        totln_dfs.append(corp_df)
    if not sme_df.is_empty():
        totln_dfs.append(sme_df)
    
    if not totln_dfs:
        print("No data to process")
        return
    
    totln_df = pl.concat(totln_dfs)
    
    # 7. Summarize by CATEG and ACCTYPE
    summary_df = totln_df.group_by(["CATEG", "ACCTYPE"]).agg(
        pl.sum("BALANCE").alias("BALANCE")
    ).sort(["CATEG", "ACCTYPE"])
    
    # 8. Generate report
    generate_bnm_report(summary_df, rdate)
    
    print(f"\nProcessing complete. Summary:")
    for row in summary_df.iter_rows(named=True):
        print(f"  {row['CATEG']} - {row['ACCTYPE']}: {row['BALANCE']:,.2f}")

def generate_bnm_report(df, rdate):
    """Generate BNM report format"""
    if df.is_empty():
        return
    
    print("\n" + "=" * 60)
    print("REPORT ID : EIQBNMR1")
    print(f"PBB - BREAKDOWN OF LOAN BY OPERATING DIVISION {rdate}")
    print("=" * 60)
    
    # Header
    print(f"{'LOAN TYPE':<20} {'A/C TYPE':<10} {'BALANCE':>20}")
    print("-" * 52)
    
    total_all = 0
    
    # Group by CATEG
    categories = df["CATEG"].unique().to_list()
    for category in categories:
        cat_df = df.filter(pl.col("CATEG") == category)
        cat_total = 0
        
        # Print category rows
        for row in cat_df.iter_rows(named=True):
            print(f"{row['CATEG']:<20} {row['ACCTYPE']:<10} {row['BALANCE']:>20,.2f}")
            cat_total += row['BALANCE']
        
        # Category total line
        print(f"{' '*30} {'TOTAL:':<10} {cat_total:>20,.2f}")
        print("-" * 52)
        total_all += cat_total
    
    # Grand total
    print(f"{' '*30} {'GRAND TOTAL:':<10} {total_all:>20,.2f}")
    print("=" * 52)
