# ============================================================
# FILE: output_date.py
# PURPOSE: Universal output filename generator
# ============================================================

from pathlib import Path
from datetime import datetime
from REPTDATE import get_reptdate_values


# ------------------------------------------------------------
# Supported date formats
# ------------------------------------------------------------
DATE_FORMATS = {
    "ddmmyy": "%d%m%y",      # 180526
    "ddmmYYYY": "%d%m%Y",   # 18052026
}


# ------------------------------------------------------------
# Main function
# ------------------------------------------------------------
def build_output_file(
    output_dir: Path,
    prefix: str,
    date_format: str = "ddmmyy"
) -> Path:
    """
    Build output filename without extension.

    Examples:
        PBB_REPORT_180526
        PBB_REPORT_18052026
    """

    reptdate = get_reptdate_values().reptdate

    fmt = DATE_FORMATS.get(date_format, "%d%m%y")

    date_part = reptdate.strftime(fmt)

    # time_part = datetime.now().strftime("%H%M%S")

    # filename = f"{prefix}_{date_part}_{time_part}"
    filename = f"{prefix}_{date_part}"

    return output_dir / filename
