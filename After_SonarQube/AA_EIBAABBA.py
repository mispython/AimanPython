# !/usr/bin/env python3
"""
Program Name: EIBAABBA.py
"""

import polars as pl
from pathlib import Path
from datetime import datetime, timedelta

def eibaabba():
    base = Path.cwd()
    mniln_path = base / "MNILN"
    sasd_path = base / "SASD"
    cisln_path = base / "CISLN"
    coll_path = base / "COLL"
    
    # REPTDATE processing
    reptdate_df = pl.read_parquet(mniln_path / "REPTDATE.parquet")
    reptdate = reptdate_df["REPTDATE"][0]
    
    reptday = reptdate.day
    
    # Week determination with SDD (Starting Day of Data)
    if reptday == 8:
        sdd, wk, wk1 = 1, '1', '4'
    elif reptday == 15:
        sdd, wk, wk1 = 9, '2', '1'
    elif reptday == 22:
        sdd, wk, wk1 = 16, '3', '2'
    else:
        sdd, wk, wk1 = 23, '4', '3'
    
    mm = reptdate.month
    
    # Set macro variables
    nowk = wk
    reptmon = f"{mm:02d}"
    reptyear = str(reptdate.year)
    reptday_str = f"{reptdate.day:02d}"
    sdate = f"{reptdate.day:02d}{mm:02d}"
    
    print(f"EIBAABBA - Account Analysis Report")
    print(f"Date: {sdate}, Week: {wk}, SDD: {sdd}")
    
    # Helper function to read datasets
    def read_dataset(path, file_name):
        try:
            return pl.read_parquet(path / file_name)
        except:
            return pl.DataFrame()
    
    # 1. Process ABBA data (main loan notes)
    abba_df = read_dataset(mniln_path, "LNNOTE.parquet")
    
    if abba_df.is_empty():
        print("No LNNOTE data found")
        return
    
    # Apply filters
    abba_df = abba_df.filter(
        (pl.col("PAIDIND") != "P") &
        (
            ((pl.col("LOANTYPE") >= 110) & (pl.col("LOANTYPE") <= 119)) |
            ((pl.col("LOANTYPE") >= 139) & (pl.col("LOANTYPE") <= 140))
        ) &
        (pl.col("RISKRATE").is_in([2, 3, 4]))
    ) 
    
    # Calculate AGE from BIRTHDT
    def calculate_age(birthdt, sdate_str):
        if not birthdt or birthdt == 0:
            return 0
        
        try:
            # Convert BIRTHDT (assumed to be in MMDDYY format)
            if isinstance(birthdt, (int, float)):
                # Handle numeric SAS date
                sas_base = datetime(1960, 1, 1)
                bdate = sas_base + timedelta(days=int(birthdt))
            else:
                # Handle string date
                birth_str = str(birthdt).zfill(11)
                bdate = datetime.strptime(birth_str[:8], "%m%d%Y")
            
            # Calculate age
            sdate_dt = datetime.strptime(sdate_str + reptyear[-2:], "%d%m%y")
            age_days = (sdate_dt - bdate).days
            return round(age_days / 365)
        except:
            return 0
    
    abba_df = abba_df.with_columns(
        pl.col("BIRTHDT").map_elements(
            lambda x: calculate_age(x, sdate), 
            return_dtype=pl.Int64
        ).alias("AGE"),
        pl.col("PENDBRH").alias("BRANCH"),
        pl.col("COLLDESC").str.slice(0, 34).alias("COLLD")
    )
    
    # Select columns
    abba_df = abba_df.select([
        "ACCTNO", "NOTENO", "SECTOR", "BRANCH", "STATE", "RISKRATE",
        "BILLCNT", "LOANTYPE", "AGE", "COLLD", "PAYAMT"
    ])
    
    abba_df = abba_df.sort(["ACCTNO", "NOTENO"])
    
    # 2. Process SASB data (loan balances and MTHARR)
    sasb_df = read_dataset(sasd_path, f"LOAN{reptmon}{nowk}.parquet")
    
    if not sasb_df.is_empty():
        # Calculate MTHARR (same logic as previous programs)
        def calculate_mtharr(bldate, sdate_str):
            if not bldate or bldate <= 0:
                return 0
            
            try:
                if isinstance(bldate, (int, float)):
                    # SAS date to Python date
                    sas_base = datetime(1960, 1, 1)
                    bldate_dt = sas_base + timedelta(days=int(bldate))
                else:
                    bldate_dt = bldate
                
                # Convert sdate to datetime
                sdate_dt = datetime.strptime(sdate_str + reptyear[-2:], "%d%m%y")
                days = (sdate_dt - bldate_dt).days + 1
                
                # MTHARR calculation chain
                if days > 729:
                    return int((days / 365) * 12)
                elif days > 698: return 23
                elif days > 668: return 22
                elif days > 638: return 21
                elif days > 608: return 20
                elif days > 577: return 19
                elif days > 547: return 18
                elif days > 516: return 17
                elif days > 486: return 16
                elif days > 456: return 15
                elif days > 424: return 14
                elif days > 394: return 13
                elif days > 364: return 12
                elif days > 333: return 11
                elif days > 303: return 10
                elif days > 273: return 9
                elif days > 243: return 8
                elif days > 213: return 7
                elif days > 182: return 6
                elif days > 151: return 5
                elif days > 121: return 4
                elif days > 89: return 3
                elif days > 59: return 2
                elif days > 30: return 1
                else: return 0
            except:
                return 0
        
        sasb_df = sasb_df.with_columns(
            pl.col("BLDATE").map_elements(
                lambda x: calculate_mtharr(x, sdate),
                return_dtype=pl.Int64
            ).alias("MTHARR")
        ).select(["ACCTNO", "NOTENO", "BALANCE", "MTHARR"])
        
        sasb_df = sasb_df.sort(["ACCTNO", "NOTENO"])
        
        # Merge ABBA with SASB
        abba_df = abba_df.join(sasb_df, on=["ACCTNO", "NOTENO"], how="left")
        
        # Calculate OVERDUE
        abba_df = abba_df.with_columns(
            (pl.col("PAYAMT") * pl.col("MTHARR")).alias("OVERDUE")
        )
    else:
        abba_df = abba_df.with_columns(
            pl.lit(None).alias("BALANCE"),
            pl.lit(None).alias("MTHARR"),
            pl.lit(None).alias("OVERDUE")
        )
    
    # 3. Merge with CISLN customer data
    cisln_df = read_dataset(cisln_path, "LOAN.parquet")
    
    if not cisln_df.is_empty():
        cisln_df = cisln_df.select([
            "ACCTNO", "CUSTNAME", "GENDER", "OCCUPAT",
            "ADDRLN1", "ADDRLN2", "ADDRLN3", "ADDRLN4", "ADDRLN5"
        ]).sort("ACCTNO")
        
        abba_df = abba_df.join(cisln_df, on="ACCTNO", how="left")
    else:
        # Add empty columns if CISLN data not available
        abba_df = abba_df.with_columns(
            pl.lit("").alias("CUSTNAME"),
            pl.lit("").alias("GENDER"),
            pl.lit("").alias("OCCUPAT"),
            pl.lit("").alias("ADDRLN1"),
            pl.lit("").alias("ADDRLN2"),
            pl.lit("").alias("ADDRLN3"),
            pl.lit("").alias("ADDRLN4"),
            pl.lit("").alias("ADDRLN5")
        )
    
    # 4. Process COLLATER data
    coll_df = read_dataset(coll_path, "COLLATER.parquet")
    
    if not coll_df.is_empty():
        # Map CCLASSC to COLLATER codes
        def map_collater(cclassc):
            if not cclassc:
                return None
            
            cclassc_str = str(cclassc).zfill(3)
            
            # Mapping logic from SAS
            if cclassc_str in ['001','006','007','014','016','024','025','026','046','048','049']:
                return '29'
            elif cclassc_str in ['000','011','012','013','017','018','019','021','027','028',
                               '029','030','031','105','106']:
                return '70'
            elif cclassc_str in ['002','003','041','042','043','058','059','067','068','069',
                               '070','071','072','078','079','084','107']:
                return '90'
            elif cclassc_str in ['004','005']:
                return '30'
            elif cclassc_str in ['032','033','034','035','036','037','038','039','040','044',
                               '050','051','052','053','054','055','056','057','060','061','062']:
                return '10'
            elif cclassc_str in ['065','066','075','076','082','083','093','094','095','096',
                               '097','098','101','102','103','104']:
                return '40'
            elif cclassc_str in ['063','064','073','074','080','081']:
                return '60'
            elif cclassc_str in ['010','085','086','087','088','089','090','091','092']:
                return '50'
            elif cclassc_str in ['009','022','023']:
                return '00'
            elif cclassc_str == '008':
                return '21'
            elif cclassc_str in ['045','047']:
                return '22'
            elif cclassc_str == '015':
                return '23'
            elif cclassc_str == '020':
                return '80'
            elif cclassc_str in ['108','109']:
                return '81'
            elif cclassc_str == '077':
                return '99'
            else:
                return None
        
        coll_df = coll_df.with_columns(
            pl.col("CCLASSC").map_elements(map_collater, return_dtype=pl.Utf8).alias("COLLATER")
        ).select(["ACCTNO", "NOTENO", "COLLATER"])
        
        # Rename COLLATER to COLLD for merging
        coll_df = coll_df.rename({"COLLATER": "COLLD"})
        coll_df = coll_df.sort(["ACCTNO", "NOTENO"])
        
        # Merge with ABBA, update COLLD from COLLATER data
        abba_df = abba_df.join(
            coll_df.select(["ACCTNO", "NOTENO", "COLLD"]),
            on=["ACCTNO", "NOTENO"],
            how="left",
            suffix="_coll"
        ).with_columns(
            pl.coalesce(pl.col("COLLD_coll"), pl.col("COLLD")).alias("COLLD")
        ).drop("COLLD_coll")
    
    # 5. Final processing
    # Remove duplicates
    abba_df = abba_df.unique(subset=["ACCTNO", "NOTENO"], keep="first")
    
    # Sort
    abba_df = abba_df.sort(["BRANCH", "ACCTNO", "NOTENO"])
    
    # 6. Generate output file
    generate_abba_output(abba_df, base / "ABBALST.csv")
    
    print(f"Processing complete. Records: {len(abba_df)}")

def generate_abba_output(df, output_path):
    """Generate ABBALST output file"""
    if df.is_empty():
        return
    
    # Define output columns in order
    output_columns = [
        "ACCTNO", "NOTENO", "BRANCH", "LOANTYPE", "SECTOR",
        "STATE", "RISKRATE", "COLLD", "OVERDUE", "BALANCE",
        "MTHARR", "BILLCNT", "AGE", "GENDER", "OCCUPAT",
        "CUSTNAME", "ADDRLN1", "ADDRLN2", "ADDRLN3", 
        "ADDRLN4", "ADDRLN5"
    ]
    
    # Select and reorder columns
    available_cols = [col for col in output_columns if col in df.columns]
    output_df = df.select(available_cols)
    
    # Write CSV with semicolon delimiters
    output_df.write_csv(output_path, separator=";")
    
    print(f"Output file created: {output_path}")
    
    # Show sample
    if len(output_df) > 0:
        print("\nFirst 3 records:")
        sample = output_df.head(3)
        for row in sample.iter_rows(named=True):
            print(f"  ACCTNO: {row.get('ACCTNO', '')}, "
                  f"Customer: {row.get('CUSTNAME', '')[:20]}, "
                  f"Balance: {row.get('BALANCE', 0):,.2f}")

# Simplified version
def eibaabba_simple():
    """Simplified version"""
    base = Path.cwd()
    
    # Get date
    reptdate = pl.read_parquet(base / "MNILN/REPTDATE.parquet")["REPTDATE"][0]
    reptday = reptdate.day
    
    # Week determination
    if reptday == 8:
        wk, sdd = '1', 1
    elif reptday == 15:
        wk, sdd = '2', 9
    elif reptday == 22:
        wk, sdd = '3', 16
    else:
        wk, sdd = '4', 23
    
    reptmon = f"{reptdate.month:02d}"
    reptyear = str(reptdate.year)
    sdate = f"{reptdate.day:02d}{reptdate.month:02d}"
    
    print(f"EIBAABBA - Account Analysis")
    print(f"Date: {sdate}, Week: {wk}, SDD: {sdd}")
    
    # Read main data
    try:
        abba_df = pl.read_parquet(base / "MNILN/LNNOTE.parquet")
    except:
        print("No LNNOTE data")
        return
    
    # Apply main filters
    abba_df = abba_df.filter(
        (pl.col("PAIDIND") != "P") &
        (
            ((pl.col("LOANTYPE") >= 110) & (pl.col("LOANTYPE") <= 119)) |
            ((pl.col("LOANTYPE") >= 139) & (pl.col("LOANTYPE") <= 140))
        ) &
        (pl.col("RISKRATE").is_in([2, 3, 4]))
    )
    
    # Calculate simple AGE
    abba_df = abba_df.with_columns(
        pl.when((pl.col("BIRTHDT").is_null()) | (pl.col("BIRTHDT") == 0))
        .then(0)
        .otherwise(pl.col("BIRTHDT").mod(10000) // 100)  # Simple month extraction
        .alias("AGE"),
        pl.coalesce(pl.col("PENDBRH"), pl.col("BRANCH")).alias("BRANCH"),
        pl.col("COLLDESC").str.slice(0, 34).alias("COLLD")
    )
    
    # Add balance data if available
    try:
        sasb_df = pl.read_parquet(base / f"SASD/LOAN{reptmon}{wk}.parquet")
        sasb_df = sasb_df.select(["ACCTNO", "NOTENO", "BALANCE"])
        abba_df = abba_df.join(sasb_df, on=["ACCTNO", "NOTENO"], how="left")
    except:
        abba_df = abba_df.with_columns(pl.lit(0.0).alias("BALANCE"))
    
    # Add customer data if available
    try:
        cis_df = pl.read_parquet(base / "CISLN/LOAN.parquet")
        cis_df = cis_df.select(["ACCTNO", "CUSTNAME", "GENDER"])
        abba_df = abba_df.join(cis_df, on="ACCTNO", how="left")
    except:
        abba_df = abba_df.with_columns(
            pl.lit("").alias("CUSTNAME"),
            pl.lit("").alias("GENDER")
        )
    
    # Select output columns
    output_cols = ["ACCTNO", "NOTENO", "BRANCH", "LOANTYPE", "SECTOR",
                   "STATE", "RISKRATE", "COLLD", "BALANCE", "BILLCNT",
                   "AGE", "GENDER", "CUSTNAME"]
    
    output_df = abba_df.select([col for col in output_cols if col in abba_df.columns])
    
    # Remove duplicates and sort
    output_df = output_df.unique(["ACCTNO", "NOTENO"]).sort(["BRANCH", "ACCTNO", "NOTENO"])
    
    # Save output
    output_df.write_csv(base / "ABBALST_SIMPLE.csv", separator=";")
    
    print(f"Output: {len(output_df)} records saved to ABBALST_SIMPLE.csv")

if __name__ == "__main__":
    eibaabba_simple()
