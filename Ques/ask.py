[EIBMECPT] WARNING: file size 18343077 not divisible by candidates (1150, 1151, 1152). Using LRECL=1150 -- output may be misaligned.

  /sas/python/virt_edw_dev/bin/python3 /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMECPT_refine.py
[sas_edw_dev@svdwh004 Data_Warehouse]$ source /sas/python/virt_edw_dev/bin/activate
(virt_edw_dev) [sas_edw_dev@svdwh004 Data_Warehouse]$ /sas/python/virt_edw_dev/bin/python3 /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMECPT_refine.py
[EIBMECPT] reptdate=2026-05-12  REPTMON=05  NOWK=2  REPTYEAR=26
[EIBMECPT] Weekly store : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMECPT/ECPOUT/ECP052.txt
[EIBMECPT] TRN output   : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMECPT/ETRNFTP.txt
[EIBMECPT] CIS output   : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMECPT/ECISFTP.txt
[EIBMECPT] Reading flat file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/uat/DP_PBECP_20260512
[EIBMECPT] WARNING: file size 18343077 not divisible by candidates (1150, 1151, 1152). Using LRECL=1150 -- output may be misaligned.
[EIBMECPT] Records read from flat file: 15951
[EIBMECPT] Weekly store updated: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMECPT/ECPOUT/ECP052.txt  (15951 rows)
[EIBMECPT] TRN file written : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMECPT/ETRNFTP.txt  (15951 rows)
[EIBMECPT] CIS file written : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMECPT/ECISFTP.txt  (15951 rows)
[EIBMECPT] Program completed successfully.

 ========== PREVIEW ========== 

shape: (15_951, 12)
┌────────────┬──────────────────┬────────────┬─────────────┬───┬──────────────┬──────────────────┬────────┬─────────────────────────────────┐
│ ACCTNO     ┆ SERIAL           ┆ TRANDATE   ┆ BENEBANKBIC ┆ … ┆ PAYORCORPREF ┆ BENEREF          ┆ STATUS ┆ RSONDESC                        │
│ ---        ┆ ---              ┆ ---        ┆ ---         ┆   ┆ ---          ┆ ---              ┆ ---    ┆ ---                             │
│ i64        ┆ str              ┆ date       ┆ str         ┆   ┆ str          ┆ str              ┆ str    ┆ str                             │
╞════════════╪══════════════════╪════════════╪═════════════╪═══╪══════════════╪══════════════════╪════════╪═════════════════════════════════╡
│ 3077509028 ┆ 2026-05-12063123 ┆ 2026-05-12 ┆ ARBKMYKL    ┆ … ┆              ┆ 188579           ┆ SC     ┆                                 │
│ 3077509    ┆ 0282026-05-12063 ┆ null       ┆    MBBEMYKL ┆ … ┆              ┆    IKUL000595196 ┆        ┆ SC                             │
│ 3077       ┆ 5090282026-05-12 ┆ null       ┆       MBBEM ┆ … ┆              ┆       700256404  ┆        ┆ SC                             │
│ 3          ┆ 0775090282026-05 ┆ null       ┆          AR ┆ … ┆              ┆          188585  ┆        ┆    SC                          │
│ 0          ┆  030775090282026 ┆ null       ┆             ┆ … ┆ 
                                                                             ┆             1885 ┆        ┆       SC                       │
│ …          ┆ …                ┆ …          ┆ …           ┆ … ┆ …            ┆ …                ┆ …      ┆ …                               │
│ 0          ┆                  ┆ null       ┆             ┆ … ┆              ┆                  ┆        ┆                           5148… │
│ 0          ┆                  ┆ null       ┆             ┆ … ┆              ┆                  ┆        ┆                              5… │
│ 0          ┆                  ┆ null       ┆             ┆ … ┆              ┆                  ┆        ┆                               … │
│ 0          ┆                  ┆ null       ┆             ┆ … ┆              ┆                  ┆        ┆                               … │
│ 0          ┆                  ┆ null       ┆             ┆ … ┆              ┆                  ┆        ┆                                 │
└────────────┴──────────────────┴────────────┴─────────────┴───┴──────────────┴──────────────────┴────────┴─────────────────────────────────┘
shape: (15_951, 8)
┌────────────┬──────────────────┬─────────────────────────────────┬─────────────────────────────────┬────────────────────┬───────────┬────────────┬──────────┐
│ ACCTNO     ┆ SERIAL           ┆ BENENAME                        ┆ BNAD                            ┆ BENEID             ┆ BENEIDIND ┆ MOBIPHON   ┆ EMAILADD │
│ ---        ┆ ---              ┆ ---                             ┆ ---                             ┆ ---                ┆ ---       ┆ ---        ┆ ---      │
│ i64        ┆ str              ┆ str                             ┆ str                             ┆ str                ┆ str       ┆ str        ┆ str      │
╞════════════╪══════════════════╪═════════════════════════════════╪═════════════════════════════════╪════════════════════╪═══════════╪════════════╪══════════╡
│ 3077509028 ┆ 2026-05-12063123 ┆ THARMA NEWS AGENT               ┆  M-09, LORONG RASAK, TAMAN SET… ┆                    ┆           ┆            ┆          │
│ 3077509    ┆ 0282026-05-12063 ┆    CITY-LINK EXPRESS (M) SDN B… ┆ MBB WISMA CITY-LINK, NO:3A, JL… ┆                    ┆           ┆ 1          ┆          │
│ 3077       ┆ 5090282026-05-12 ┆       BURSA MALAYSIA DEPOSITOR… ┆    MBB FINANCE DEPARTMENT     … ┆                    ┆           ┆ 1-01       ┆          │
│ 3          ┆ 0775090282026-05 ┆          THARMA NEWS AGENT      ┆       AMB M-09, LORONG RASAK, … ┆                    ┆           ┆ 1-01-01    ┆          │
│ 0          ┆  030775090282026 ┆ 83          THARMA NEWS AGENT   ┆          AMB M-09, LORONG RASA… ┆                    ┆           ┆ 0001-01-01 ┆          │
│ …          ┆ …                ┆ …                               ┆ …                               ┆ …                  ┆ …         ┆ …          ┆ …        │
│ 0          ┆                  ┆                               … ┆ -01                           … ┆       GKLC05120083 ┆ 28        ┆            ┆          │
│ 0          ┆                  ┆                               … ┆ -01-01                        … ┆          GKLC05120 ┆ 08        ┆            ┆          │
│ 0          ┆                  ┆                               … ┆ 001-01-01                     … ┆             GKLC05 ┆ 12        ┆            ┆          │
│ 0          ┆                  ┆                               … ┆   0001-01-01                  … ┆                GKL ┆ C0        ┆            ┆          │
│ 0          ┆                  ┆                               … ┆      0001-01-01               … ┆                    ┆ GK        ┆            ┆          │
└────────────┴──────────────────┴─────────────────────────────────┴─────────────────────────────────┴────────────────────┴───────────┴────────────┴──────────┘
===============================
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWCTCS.py", line 215, in <module>
    ctcs_out.write_parquet(str(CTCS_MON_PQ))
NameError: name 'CTCS_MON_PQ' is not defined
================================
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
