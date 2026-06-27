import polars as pl
import duckdb
from pathlib import Path
import datetime
import sys

# Configuration
deposit_path = Path("DEPOSIT")
output_path = Path("output")
output_path.mkdir(exist_ok=True)

# DATA REPTDATE; SET DEPOSIT.REPTDATE;
reptdate_df = pl.read_parquet(deposit_path / "REPTDATE.parquet")

# CALL SYMPUT equivalent
first_row = reptdate_df.row(0)
REPDD = f"{first_row['REPTDATE'].day:02d}"  # Z2.
REPMM = f"{first_row['REPTDATE'].month:02d}"  # Z2.
REPYY = str(first_row['REPTDATE'].year)  # YEAR4.
REPDT = first_row['REPTDATE'].strftime('%d%m%y')  # DDMMYY8.

print(f"REPDD: {REPDD}, REPMM: {REPMM}, REPYY: {REPYY}, REPDT: {REPDT}")

# DATA FISS_PBB; INFILE FISSD DELIMITER = ';' FIRSTOBS=2;
try:
    fiss_pbb_df = pl.read_csv("FISSD.csv", separator=';', skip_rows=1, 
                             has_header=False, new_columns=['BIC', 'CURBAL'])
except FileNotFoundError:
    # Create empty DataFrame with proper schema if file doesn't exist
    fiss_pbb_df = pl.DataFrame({
        'BIC': pl.Series([], dtype=pl.Utf8),
        'CURBAL': pl.Series([], dtype=pl.Float64)
    })

# Apply formats (in Polars, we ensure proper data types)
fiss_pbb_df = fiss_pbb_df.with_columns([
    pl.col('BIC').cast(pl.Utf8).str.slice(0, 15),  # $15. format
    pl.col('CURBAL').cast(pl.Float64)  # 30. format
])

# DATA FISS_PIBB; INFILE FISSID DELIMITER = ';' FIRSTOBS=2;
try:
    fiss_pibb_df = pl.read_csv("FISSID.csv", separator=';', skip_rows=1,
                              has_header=False, new_columns=['BIC', 'CURBAL'])
except FileNotFoundError:
    # Create empty DataFrame with proper schema
    fiss_pibb_df = pl.DataFrame({
        'BIC': pl.Series([], dtype=pl.Utf8),
        'CURBAL': pl.Series([], dtype=pl.Float64)
    })

# Apply formats
fiss_pibb_df = fiss_pibb_df.with_columns([
    pl.col('BIC').cast(pl.Utf8).str.slice(0, 15),  # $15. format
    pl.col('CURBAL').cast(pl.Float64)  # 30. format
])

# Save intermediate files
fiss_pbb_df.write_parquet(output_path / "FISS_PBB_raw.parquet")
fiss_pibb_df.write_parquet(output_path / "FISS_PIBB_raw.parquet")

# DATA FISS_PBB; FORMAT TYPE $10.;
fiss_pbb_processed = fiss_pbb_df.with_columns([
    # BICODE=SUBSTR(BIC,1,5);
    pl.col('BIC').str.slice(0, 5).alias('BICODE'),
    
    # TYPE assignments
    pl.when(pl.col('BIC').str.slice(0, 5) == '95313')
    .then(pl.lit('CA'))
    .when(pl.col('BIC').str.slice(0, 5) == '95312')
    .then(pl.lit('SA'))
    .when(pl.col('BIC').str.slice(0, 5) == '95311')
    .then(pl.lit('FD'))
    .when(pl.col('BIC').str.slice(0, 5) == '96311')
    .then(pl.lit('FCYFD'))
    .when(pl.col('BIC').str.slice(0, 5) == '96313')
    .then(pl.lit('FCYCA'))
    .otherwise(pl.lit(''))
    .alias('TYPE'),
    
    # TYPEX=SUBSTR(BIC,6,2);
    pl.col('BIC').str.slice(5, 2).alias('TYPEX')
]).with_columns([
    # IF TYPEX='08' THEN CURBAL_I = CURBAL;
    pl.when(pl.col('TYPEX') == '08')
    .then(pl.col('CURBAL'))
    .otherwise(None)
    .alias('CURBAL_I'),
    
    # ELSE IF TYPEX='09' THEN CURBAL_C = CURBAL;
    pl.when(pl.col('TYPEX') == '09')
    .then(pl.col('CURBAL'))
    .otherwise(None)
    .alias('CURBAL_C')
])

# DATA FISS_PIBB; FORMAT TYPE $10.;
fiss_pibb_processed = fiss_pibb_df.with_columns([
    # BICODE=SUBSTR(BIC,1,5);
    pl.col('BIC').str.slice(0, 5).alias('BICODE'),
    
    # TYPE assignments (different BIC codes for PIBB)
    pl.when(pl.col('BIC').str.slice(0, 5) == '95313')
    .then(pl.lit('CA'))
    .when(pl.col('BIC').str.slice(0, 5) == '95312')
    .then(pl.lit('SA'))
    .when(pl.col('BIC').str.slice(0, 5) == '95315')  # Different from PBB
    .then(pl.lit('FD'))
    .when(pl.col('BIC').str.slice(0, 5) == '96311')
    .then(pl.lit('FCYFD'))
    .when(pl.col('BIC').str.slice(0, 5) == '96313')
    .then(pl.lit('FCYCA'))
    .otherwise(pl.lit(''))
    .alias('TYPE'),
    
    # TYPEX=SUBSTR(BIC,6,2);
    pl.col('BIC').str.slice(5, 2).alias('TYPEX')
]).with_columns([
    # IF TYPEX='08' THEN ICURBAL_I = CURBAL;
    pl.when(pl.col('TYPEX') == '08')
    .then(pl.col('CURBAL'))
    .otherwise(None)
    .alias('ICURBAL_I'),
    
    # ELSE IF TYPEX='09' THEN ICURBAL_C = CURBAL;
    pl.when(pl.col('TYPEX') == '09')
    .then(pl.col('CURBAL'))
    .otherwise(None)
    .alias('ICURBAL_C')
])

# DATA FISS_PBB; IF TYPE=' ' THEN DELETE;
fiss_pbb_filtered = fiss_pbb_processed.filter(pl.col('TYPE') != '')

# DATA FISS_PIBB; IF TYPE=' ' THEN DELETE;
fiss_pibb_filtered = fiss_pibb_processed.filter(pl.col('TYPE') != '')

# DATA FISS; SET FISS_PBB FISS_PIBB;
fiss_combined = pl.concat([fiss_pbb_filtered, fiss_pibb_filtered], how="diagonal")

# NON=SUBSTR(BIC,8,2); IF NON NE '00' THEN DELETE;
fiss_final = fiss_combined.with_columns([
    pl.col('BIC').str.slice(7, 2).alias('NON')
]).filter(pl.col('NON') == '00')

# Save intermediate FISS dataset
fiss_final.write_parquet(output_path / "FISS.parquet")
fiss_final.write_csv(output_path / "FISS.csv")

# PROC PRINT DATA=FISS equivalent
print("FISS Dataset:")
print(fiss_final)

# PROC SUMMARY DATA=FISS NWAY; CLASS TYPE;
fiss_summary = fiss_final.group_by('TYPE').agg([
    pl.col('CURBAL_I').sum().alias('CURBAL_I'),
    pl.col('CURBAL_C').sum().alias('CURBAL_C'),
    pl.col('ICURBAL_I').sum().alias('ICURBAL_I'),
    pl.col('ICURBAL_C').sum().alias('ICURBAL_C')
])

# DATA FISS; SET FISS;
fiss_summary_processed = fiss_summary.with_columns([
    # Replace nulls with 0
    pl.col('CURBAL_I').fill_null(0),
    pl.col('CURBAL_C').fill_null(0),
    pl.col('ICURBAL_I').fill_null(0),
    pl.col('ICURBAL_C').fill_null(0),
    
    # TOTAL=CURBAL_I+CURBAL_C+ICURBAL_I+ICURBAL_C;
    (pl.col('CURBAL_I') + pl.col('CURBAL_C') + pl.col('ICURBAL_I') + pl.col('ICURBAL_C')).alias('TOTAL')
])

# Save final summary
fiss_summary_processed.write_parquet(output_path / "FISS_summary.parquet")
fiss_summary_processed.write_csv(output_path / "FISS_summary.csv")

# PROC PRINT DATA=FISS equivalent
print("\nFISS Summary Dataset:")
print(fiss_summary_processed)
