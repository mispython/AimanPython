# ------------------------------------------------------------
# SAS7BDAT -> Parquet conversion helper
# ------------------------------------------------------------
PARQUET_DIR = BASE_DIR / "parquet_cache" / "EIBDLNSA"
PARQUET_DIR.mkdir(parents=True, exist_ok=True)

def _ensure_parquet(sas_path: Path) -> Path:
    """
    Convert a .sas7bdat file to parquet if not already done.
    Returns the parquet path.
    """
    parquet_path = PARQUET_DIR / sas_path.with_suffix(".parquet").name
    if parquet_path.exists():
        print(f"[INFO] Parquet cache found   : {parquet_path.name}")
        return parquet_path

    print(f"[INFO] Converting to parquet : {sas_path.name}  (this may take a while ...)")
    chunk_size  = 100_000
    first_chunk = True
    writer      = None

    for chunk in pd.read_sas(str(sas_path), encoding="latin1", chunksize=chunk_size):
        chunk.columns = [c.upper() for c in chunk.columns]
        table = pl.from_pandas(chunk).to_arrow()
        if first_chunk:
            import pyarrow.parquet as pq
            writer      = pq.ParquetWriter(str(parquet_path), table.schema)
            first_chunk = False
        writer.write_table(table)

    if writer:
        writer.close()

    print(f"[INFO] Parquet saved         : {parquet_path}")
    return parquet_path



# ------------------------------------------------------------
# Step 1 : Convert to parquet (once), then summarise via Polars
# ------------------------------------------------------------
print(f"[INFO] Current loan file  : {LOAN_FILE.name}")
curr_parquet = _ensure_parquet(LOAN_FILE)

loan_summ = (
    pl.scan_parquet(curr_parquet)
    .filter(pl.col("PRODUCT").is_in(TARGET_PRODUCTS))
    .group_by(["BRANCH", "PRODUCT"])
    .agg([
        pl.col("BALANCE").sum().alias("BRLNAMT"),
        pl.len().alias("NOACCT"),
    ])
    .collect()
)

# ------------------------------------------------------------
# Step 2 : Convert to parquet (once), then summarise via Polars
# ------------------------------------------------------------
print(f"[INFO] Previous loan file : {PREVLN_FILE.name}")
prev_parquet = _ensure_parquet(PREVLN_FILE)

prevln_summ = (
    pl.scan_parquet(prev_parquet)
    .filter(pl.col("PRODUCT").is_in(TARGET_PRODUCTS))
    .group_by(["BRANCH", "PRODUCT"])
    .agg([
        pl.col("BALANCE").sum().alias("PBRLNAMT"),
        pl.len().alias("PNOACCT"),
    ])
    .collect()
)

============================

# ------------------------------------------------------------
# Report date
# ------------------------------------------------------------
reptdate_values = get_reptdate_values()

REPTMON  = reptdate_values.reptmon
REPTDAY  = reptdate_values.reptday
REPTYEAR = reptdate_values.reptyear
RDATE    = reptdate_values.reptdate.strftime("%d/%m/%y")

# Current month = reptdate month (MM), previous month = MM-1
# Year rolls back when current month is January (01 -> 12 of prior year)
_mm_int  = int(REPTMON)
_yy_int  = int(REPTYEAR)

_curr_mm = _mm_int
_curr_yy = _yy_int

_prev_mm = _mm_int - 1 if _mm_int > 1 else 12
_prev_yy = _yy_int if _mm_int > 1 else _yy_int - 1

CURR_LOAN_NAME = f"ln{_curr_mm:02d}4{_curr_yy:02d}.sas7bdat"
PREV_LOAN_NAME = f"ln{_prev_mm:02d}4{_prev_yy:02d}.sas7bdat"

# ------------------------------------------------------------
# Path configuration
# ------------------------------------------------------------
BASE_DIR   = Path("/dwh")
MIS_DIR    = BASE_DIR / "ln_ln"
BRANCH_DIR = Path("/sasdata/rawdata/lookup")
OUTPUT_DIR = BASE_DIR / "OUTPUT" / "EIBDLNSA"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LOAN_FILE   = MIS_DIR / CURR_LOAN_NAME
PREVLN_FILE = MIS_DIR / PREV_LOAN_NAME

OUTPUT_FILE = build_output_file(OUTPUT_DIR, prefix="EIBDLNSA").with_suffix(".txt")

==================

BASE_DIR   = Path("/dwh")
MIS_DIR    = BASE_DIR / "ln_ln"          # .sas7bdat loan files  (prefix: ln)
BRANCH_DIR = Path("/sasdata/rawdata/lookup")      # flat file  (no date prefix)
OUTPUT_DIR = BASE_DIR / "OUTPUT" / "EIBDLNSA"
