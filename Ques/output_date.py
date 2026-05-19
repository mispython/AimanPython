# ============================================================
# PROGRAM : output_date.py
# PURPOSE : Pure output filename generator (NO file type logic)
# ============================================================

from pathlib import Path
from datetime import datetime
from REPTDATE import get_reptdate_values


def build_output_file(output_dir: Path, prefix: str) -> Path:
    """
    Generates output filename ONLY.

    Format:
        PREFIX_ddmmyy_HHMMSS

    File type/extension is controlled by MAIN program.

    Example:
        PBB_ODLIMIT_REPORT_180526_153012
    """

    reptdate = get_reptdate_values().reptdate

    date_part = reptdate.strftime("%d%m%y")
    time_part = datetime.now().strftime("%H%M%S")

    filename = f"{prefix}_{date_part}_{time_part}"

    return output_dir / filename
