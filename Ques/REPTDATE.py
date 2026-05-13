"""Shared report-date derivation helpers.

The original SAS programs build a small ``REPTDATE`` dataset and then expose
values such as ``REPTYEAR``, ``REPTMON``, ``REPTDAY``, and ``NOWK`` as macro
variables.  This module centralises the same calculation so report programs can
reuse it instead of duplicating the date/week logic.
"""

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional


@dataclass(frozen=True)
class ReptDateValues:
    """Container for report-date macro-variable equivalents."""

    reptdate: date
    reptyear: str
    reptmon: str
    reptday: str
    reptdt: int
    rdate: date
    nowk: str


def get_reptdate_values(
    run_date: Optional[date] = None,
    *,
    year_format: str = "%y",
) -> ReptDateValues:
    """Return report-date values using ``run_date - 1 day``.

    Args:
        run_date: The date the program is running.  When omitted, today's date
            is used.
        year_format: ``strftime`` pattern for ``reptyear``.  Use ``"%y"`` for
            a two-digit year or ``"%Y"`` for a four-digit year.
    """
    reptdate = (run_date or date.today()) - timedelta(days=1)

    day_of_month = reptdate.day
    if 1 <= day_of_month <= 8:
        nowk = 1
    elif 9 <= day_of_month <= 15:
        nowk = 2
    elif 16 <= day_of_month <= 22:
        nowk = 3
    else:
        nowk = 4

    return ReptDateValues(
        reptdate=reptdate,
        reptyear=reptdate.strftime(year_format),
        reptmon=reptdate.strftime("%m"),
        reptday=reptdate.strftime("%d"),
        reptdt=reptdate.toordinal(),
        rdate=reptdate,
        nowk=f"{nowk:01d}",
    )


# =============================================================================
# DATE / WEEK DERIVATION  (equivalent of DATA REPTDATE step)
# =============================================================================
# REPTDATE = TODAY() - 1  (SAS: OPTIONS YEARCUTOFF=1950)
_default_values = get_reptdate_values()

# Macro variable equivalents kept for scripts that import the module constants.
reptdate: date = _default_values.reptdate
REPTYEAR = _default_values.reptyear          # 2-digit year  (PUT(REPTDATE,YEAR2.))
REPTMON = _default_values.reptmon            # zero-padded month (Z2.)
REPTDAY = _default_values.reptday            # zero-padded day   (Z2.)
REPTDT = _default_values.reptdt              # raw SAS date integer equivalent (used for filter)
RDATE = _default_values.rdate                # date object used in DATA ECP step
NOWK = _default_values.nowk                  # zero-padded 1-digit week number (Z1.)
