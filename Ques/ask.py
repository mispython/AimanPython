# ------------------------------------------------------------
# SAS7BDAT -> Parquet caching reader  (adapted from EIBXLNLC)
# ------------------------------------------------------------
def _get_row_limit():
    value = os.environ.get("EIBDLNSA_ROW_LIMIT", "").strip()
    if not value:
        return None
    try:
        row_limit = int(value)
    except ValueError as exc:
        raise ValueError("EIBDLNSA_ROW_LIMIT must be a positive integer or 0") from exc
    return row_limit if row_limit > 0 else None


def _read_sas7bdat(path: Path, row_limit=None):
    """
    SAS -> Parquet caching reader.
    Returns pl.LazyFrame (cache hit / full convert) or pl.DataFrame (test mode).
    """
    cache_dir = PARQUET_DIR / path.stem
    cache_dir.mkdir(parents=True, exist_ok=True)

    parquet_files = list(cache_dir.glob("*.parquet"))
    cache_valid   = (
        len(parquet_files) > 0
        and max(f.stat().st_mtime for f in parquet_files) >= path.stat().st_mtime
    )

    # CASE 1: USE CACHE
    if cache_valid and row_limit is None:
        print(f"[CACHE HIT] Reading Parquet : {path.stem}")
        return pl.scan_parquet(str(cache_dir / "*.parquet"))

    # CASE 2: TEST MODE
    if row_limit:
        print(f"[TEST MODE] Reading SAS     : {path.name}")
        reader = pd.read_sas(str(path), encoding="latin1", chunksize=row_limit)
        try:
            pdf = next(reader)
        except StopIteration:
            pdf = pd.DataFrame()
        pdf.columns = [c.upper() for c in pdf.columns]
        return pl.from_pandas(pdf)

    # CASE 3: FULL CONVERSION (SAS -> PARQUET PARTITIONED)
    print(f"\n[CONVERT] SAS -> Parquet (chunked) : {path.name}")
    reader = pd.read_sas(str(path), encoding="latin1", chunksize=500_000)
    for i, chunk in enumerate(reader):
        if chunk is None or chunk.empty:
            continue
        print(f"[CHUNK {i}] rows processed ...")
        chunk.columns = [c.upper() for c in chunk.columns]
        df = pl.from_pandas(chunk)
        df = df.with_columns([pl.col(c).cast(pl.Utf8, strict=False) for c in df.columns])
        out_file = cache_dir / f"part-{i:05d}.parquet"
        df.write_parquet(out_file, compression="zstd")
        print(f"[WRITE] {out_file} ({len(df):,} rows)")
    print(f"[DONE] Cache created at : {cache_dir}")
    return pl.scan_parquet(str(cache_dir / "*.parquet"))



# ------------------------------------------------------------
# Step 1 : Read & summarise current-month loan file
# ------------------------------------------------------------
row_limit = _get_row_limit()

print(f"[INFO] Current loan file  : {LOAN_FILE.name}")
raw_curr  = _read_sas7bdat(LOAN_FILE, row_limit=row_limit)
loan_summ = (
    raw_curr
    .filter(pl.col("PRODUCT").cast(pl.Float64).cast(pl.Int64).is_in(TARGET_PRODUCTS))
    .group_by(["BRANCH", "PRODUCT"])
    .agg([
        pl.col("BALANCE").cast(pl.Float64).sum().alias("BRLNAMT"),
        pl.len().alias("NOACCT"),
    ])
    .collect()
)

# ------------------------------------------------------------
# Step 2 : Read & summarise previous-month loan file
# ------------------------------------------------------------
print(f"[INFO] Previous loan file : {PREVLN_FILE.name}")
raw_prev    = _read_sas7bdat(PREVLN_FILE, row_limit=row_limit)
prevln_summ = (
    raw_prev
    .filter(pl.col("PRODUCT").cast(pl.Float64).cast(pl.Int64).is_in(TARGET_PRODUCTS))
    .group_by(["BRANCH", "PRODUCT"])
    .agg([
        pl.col("BALANCE").cast(pl.Float64).sum().alias("PBRLNAMT"),
        pl.len().alias("PNOACCT"),
    ])
    .collect()
)
