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
