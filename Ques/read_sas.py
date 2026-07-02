import pandas as pd

# =========================
# CONFIG
# =========================
FILE_PATH = "/stgsrcsys/host/uat/dpld06.sas7bdat"
COLUMN_NAME = "CHEQNO"

# condition (STRICT: only > 0)
def filter_condition(df):
    return df[df[COLUMN_NAME] > 0]

# =========================
# LOAD + FILTER
# =========================
df = pd.read_sas(FILE_PATH, format="sas7bdat", encoding="utf-8")

result = filter_condition(df)

# =========================
# OUTPUT
# =========================
print(result)

# optional save
result.to_csv("filtered_output.csv", index=False)
