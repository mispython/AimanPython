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

# COMMON REPTDATE REPLACEMENT BLOCK START
# DERIVE MACRO VARIABLES WITHOUT LOAN.REPTDATE / REPTDATE.parquet
#
# Original SAS program (Ori_SAS/EIBWCTCS) reads LOAN.REPTDATE and derives
# REPTDAY, REPTMON, REPTYEAR, RDATE, and REPTDATE macro variables.
#
# Converted programs can reuse this block to avoid a REPTDATE.parquet input:
#   1. Delete the REPTDATE parquet path/read lines.
#   2. Paste this block where the old REPTDATE DATA step was converted.
#   3. Keep the downstream variable names unchanged.
#
# Note: do not do datetime.now().strftime("%d%m%y") - 1 because strftime()
# returns text. Subtract one day from the date/datetime first, then format it.
# COMMON REPTDATE REPLACEMENT BLOCK END
