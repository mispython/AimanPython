import pandas as pd
from pathlib import Path

files = {
    "LNNOTE_PBB" : Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBXLNLC/lnnote_pbb.sas7bdat"),
    "LNCOMM"     : Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBXLNLC/enrh_ln_comm.sas7bdat"),
    "LOAN_PBB"   : Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBXODLC/ln06226.sas7bdat"),
}

for name, path in files.items():
    reader = pd.read_sas(str(path), encoding="latin1", chunksize=1)
    chunk  = next(iter(reader))
    cols   = sorted([c.upper() for c in chunk.columns.tolist()])
    print(f"\n{'='*60}")
    print(f"  {name}  ({len(cols)} columns)")
    print(f"{'='*60}")
    for c in cols:
        print(f"  {c}")
