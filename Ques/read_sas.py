(virt_edw_dev) [sas_edw_dev@svdwh004 Data_Warehouse]$ /sas/python/virt_edw_dev/bin/python3 /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/5_read_sas.py
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/5_read_sas.py", line 23, in <module>
    result = con.execute(query).df()
_duckdb.CatalogException: Catalog Error: Table Function with name read_sas does not exist!
Did you mean "read_csv"?

LINE 3: FROM read_sas('/stgsrcsys/host/uat/dpld06.sas7bdat')
             ^

===============

import duckdb

# =========================
# CONFIG (CHANGE HERE ONLY)
# =========================
FILE_PATH = "/path/to/your/file.sas7bdat"
COLUMN_NAME = "CHEQNO"
CONDITION = "> 0"   # change logic here if needed (e.g. "= 123", "< 5")

# =========================
# BUILD QUERY
# =========================
query = f"""
SELECT *
FROM read_sas('{FILE_PATH}')
WHERE {COLUMN_NAME} {CONDITION}
"""

# =========================
# EXECUTE
# =========================
con = duckdb.connect()
result = con.execute(query).df()

# =========================
# OUTPUT
# =========================
print(result)

# Optional: save output
result.to_csv("filtered_output.csv", index=False)
