# ============================================================
# PROGRAM : input_date.py
# PURPOSE : Universal "latest file" resolver for ETL pipelines
# ============================================================

from pathlib import Path
import re

# ------------------------------------------------------------
# Supported file types (extend anytime)
# ------------------------------------------------------------
SUPPORTED_EXTENSIONS = {
    ".sas7bdat",
    ".csv",
    ".xlsx",
    ".xls",
    ".parquet",
    ".txt",
    ".bin",
    ".dat",
}

# ------------------------------------------------------------
# Filename date patterns (supports multiple legacy formats)
# ------------------------------------------------------------
PATTERNS = [
    # mmwwyy (your current format)
    r"(?P<prefix>[a-zA-Z]*)(?P<mm>\d{2})(?P<ww>\d{1,2})(?P<yy>\d{2})",

    # ddmmyy
    r"(?P<prefix>[a-zA-Z]*)(?P<dd>\d{2})(?P<mm>\d{2})(?P<yy>\d{2})",

    # mmddyy
    r"(?P<prefix>[a-zA-Z]*)(?P<mm>\d{2})(?P<dd>\d{2})(?P<yy>\d{2})",

    # mmyy
    r"(?P<prefix>[a-zA-Z]*)(?P<mm>\d{2})(?P<yy>\d{2})",

    # mm-w-yy or mm_w_yy
    r"(?P<prefix>[a-zA-Z]*)(?P<mm>\d{2})\D(?P<ww>\d{1,2})\D(?P<yy>\d{2})",
]

# ------------------------------------------------------------
# Extract sortable key from filename
# ------------------------------------------------------------
def extract_key(filename: str):
    for pattern in PATTERNS:
        m = re.search(pattern, filename)
        if not m:
            continue

        gd = m.groupdict()

        yy = int(gd.get("yy") or 0)
        mm = int(gd.get("mm") or 0)
        dd = int(gd.get("dd") or 0)
        ww = int(gd.get("ww") or 0)

        year = 2000 + yy if yy < 100 else yy

        # unified ranking key (year → month → week → day)
        return (year, mm, ww, dd)

    return None

# ------------------------------------------------------------
# MAIN FUNCTION: get latest file in directory
# ------------------------------------------------------------
def get_latest_file(directory: Path, prefix: str = "") -> Path:
    """
    Returns the latest file in a directory based on parsed date in filename.

    Args:
        directory: folder path
        prefix: optional file prefix filter (e.g. 'ca', 'lm', 'ica')

    Returns:
        Path of latest file only
    """

    files = [
        f for f in directory.iterdir()
        if f.is_file()
        and f.suffix.lower() in SUPPORTED_EXTENSIONS
        and f.name.startswith(prefix)
    ]

    valid_files = [
        f for f in files
        if extract_key(f.name) is not None
    ]

    if not valid_files:
        raise FileNotFoundError(
            f"No valid files found in {directory} with prefix '{prefix}'"
        )

    latest = max(valid_files, key=lambda f: extract_key(f.name))

    print(f"[FILE_RESOLVER] Selected latest: {latest.name}")

    return latest
