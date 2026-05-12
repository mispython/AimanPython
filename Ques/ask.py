def _resolve_reptdate(
    source_date: Optional[date] = None,
    today: Optional[date] = None,
) -> date:
    """Return the source/header report date, or yesterday if none is supplied."""
    if source_date is not None:
        return source_date

    run_date = today or datetime.now().date()
    return run_date - timedelta(days=1)

_reptdate_val: date = _resolve_reptdate(source_date=_tdate)

REPTDAY   = f"{_reptdate_val.day:02d}"
REPTMON   = f"{_reptdate_val.month:02d}"
REPTYEAR  = str(_reptdate_val.year)[-2:]
# &RDATE   = PUT(REPTDATE, Z5.) -- zero-padded 5-digit SAS date number
# Reproduced as DDMMYY8. string for comparison (used as &REPTDATE too)
RDATE_STR = _reptdate_val.strftime("%d/%m/%y")    # DDMMYY8. format

# Monthly accumulator path
CTCS_MON_PQ = os.path.join(OUTPUT_DIR, f"CTCS{REPTMON}.parquet")

source /sas/python/virt_edw_dev/bin/activate
/sas/python/virt_edw_dev/bin/python3 /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWCTCS.py
[sas_edw_dev@svdwh004 Data_Warehouse]$ source /sas/python/virt_edw_dev/bin/activate
(virt_edw_dev) [sas_edw_dev@svdwh004 Data_Warehouse]$ /sas/python/virt_edw_dev/bin/python3 /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWCTCS.py
THE SAP.PBB.EPCU.CTCS IS NOT DATED 11/05/26
================================
source /sas/python/virt_edw_dev/bin/activate
/sas/python/virt_edw_dev/bin/python3 /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWCTCS.py
[sas_edw_dev@svdwh004 Data_Warehouse]$ source /sas/python/virt_edw_dev/bin/activate
(virt_edw_dev) [sas_edw_dev@svdwh004 Data_Warehouse]$ /sas/python/virt_edw_dev/bin/python3 /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWCTCS.py
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWCTCS.py", line 88, in <module>
    def _resolve_reptdate(today: date | None = None) -> date:
TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'

def _resolve_reptdate(today: date | None = None) -> date:
    """Return the report date for data generated today but dated yesterday."""
    run_date = today or datetime.now().date()
    return run_date - timedelta(days=1)
==================================
