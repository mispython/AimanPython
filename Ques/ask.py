//******************************************************************
//* FTP HOST DATASETS TO DATA REPORT REPOSITORY SYSTEM (DRR)
//******************************************************************
//*%OPC SCAN
//RUNSFTP  EXEC COZBATCH
//CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(DRR#SFTP)
//           DD *
lzopts servercp=$servercp,notrim,overflow=trunc,mode=text
lzopts linerule=$lr
cd TD/INTERBANK/NLF
put //SAP.PBB.NLF.BEHAVE.TEXT(0)  BEHAVE@%ODD.%OMM.%OYY..TXT
cd /TD/PIBB/NLF
put //SAP.PIBB.NLF.BEHAVE.TEXT(0)  IBEHAVE@%ODD.%OMM.%OYY..TXT
EOB





      
import pandas as pd
import os

# Complete SAS dataset path
gl_path = (
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/"
    "EIBDUNDP/your_file.sas7bdat"
)

# Check whether file exists
if not os.path.exists(gl_path):
    raise FileNotFoundError(f"SAS file not found: {gl_path}")

print(f"Reading SAS file: {gl_path}")
print(f"File size: {os.path.getsize(gl_path) / (1024**3):.2f} GB")

# Read only the first 3 rows using chunking
reader = pd.read_sas(
    gl_path,
    encoding="latin1",
    format="sas7bdat",
    chunksize=3
)

# Display the first 3 rows
df = next(reader)

print("\nFirst 3 rows:")
print(df.head(3).to_string(index=False))

# Display column names and data types
print("\nColumn information:")
print(df.dtypes)

# Display number of columns
print(f"\nTotal columns: {len(df.columns)}")

# Release memory
del df





Reading SAS file: /stgsrcsys/host/uat/AII/EIBMNLFE/STOREGL/glrmfxp220260630.sas7bdat
File size: 0.00 GB

First 3 rows:
 ITEM  _TYPE_  _FREQ_      WEEK  MONTH  QTR  HALFYR  YEAR  LAST   BALANCE
B1.12     1.0     1.0       0.0    NaN  NaN     NaN   NaN   NaN       0.0
B1.14     1.0     1.0 -238278.0    NaN  NaN     NaN   NaN   NaN -238278.0

Column information:
ITEM        object
_TYPE_     float64
_FREQ_     float64
WEEK       float64
MONTH      float64
QTR        float64
HALFYR     float64
YEAR       float64
LAST       float64
BALANCE    float64
dtype: object

Total columns: 10
