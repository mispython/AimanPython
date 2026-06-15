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



# =============================================================================
# NOTE - put these on main programs
# =============================================================================
# # If 2-digit SAS-style year
# from REPTDATE import get_reptdate_values

# reptdate_values = get_reptdate_values()

# REPTYEAR = reptdate_values.reptyear
# REPTMON = reptdate_values.reptmon
# REPTDAY = reptdate_values.reptday
# NOWK = reptdate_values.nowk

# # If 4-digit SAS-style year
# from REPTDATE import get_reptdate_values

# reptdate_values = get_reptdate_values(year_format="%Y")

# REPTYEAR = reptdate_values.reptyear
# REPTMON = reptdate_values.reptmon
# REPTDAY = reptdate_values.reptday
# NOWK = reptdate_values.nowk

# # Can also import the constants directly
# from REPTDATE import reptdate, REPTYEAR, REPTMON, REPTDAY, REPTDT, RDATE, NOWK


# =============================================================================
# MONTHLY REPORT DATE DERIVATION
# =============================================================================
# For programs that run on any day of the current month (e.g. on the 3rd) but
# must report against LAST MONTH's dataset. The report date is always the
# last calendar day of the previous month, regardless of the actual run date.
#
# Example: run_date = 29/06/2026 -> monthly reptdate = 31/05/2026


@dataclass(frozen=True)
class MonthlyReptDateValues:
    """Container for monthly report-date macro-variable equivalents."""

    reptdate: date
    reptyear: str
    reptmon: str
    reptday: str
    reptdt: int
    rdate: date
    ddmmyy8: str
    ddmmyyyy: str
    yymmdd: str
    mmyyyy: tuple


def get_monthly_reptdate_values(
    run_date: Optional[date] = None,
    *,
    year_format: str = "%y",
) -> MonthlyReptDateValues:
    """Return report-date values for the LAST DAY of the PREVIOUS month.

    Regardless of which day ``run_date`` falls on, the resulting ``reptdate``
    is always the last day of the month before ``run_date``'s month.

    Args:
        run_date: The date the program is running. When omitted, today's date
            is used.
        year_format: ``strftime`` pattern for ``reptyear``. Use ``"%y"`` for
            a two-digit year or ``"%Y"`` for a four-digit year.
    """
    base_date = run_date or date.today()

    first_of_current_month = base_date.replace(day=1)
    reptdate = first_of_current_month - timedelta(days=1)

    return MonthlyReptDateValues(
        reptdate=reptdate,
        reptyear=reptdate.strftime(year_format),
        reptmon=reptdate.strftime("%m"),
        reptday=reptdate.strftime("%d"),
        reptdt=reptdate.toordinal(),
        rdate=reptdate,
        ddmmyy8=reptdate.strftime("%d/%m/%y"),
        ddmmyyyy=reptdate.strftime("%d/%m/%Y"),
        yymmdd=reptdate.strftime("%y%m%d"),
        mmyyyy=(reptdate.month, reptdate.year),
    )


# =============================================================================
# NOTE - put these on main programs (MONTHLY variant)
# =============================================================================
# from REPTDATE import get_monthly_reptdate_values

# monthly_reptdate_values = get_monthly_reptdate_values()

# REPTYEAR = monthly_reptdate_values.reptyear
# REPTMON = monthly_reptdate_values.reptmon
# REPTDAY = monthly_reptdate_values.reptday
# RDATE = monthly_reptdate_values.ddmmyy8   # e.g. DDMMYY8. style (with separators)
