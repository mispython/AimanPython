import polars as pl
from datetime import datetime, timedelta

BNM_DIR = 'data/bnm/'
OUTPUT_PRINT1 = 'reports/teller_consolidated.txt'
OUTPUT_PRINT2 = 'reports/teller_branch_detail.txt'
OUTPUT_BRDEMO = 'reports/branch_demo.txt'
BRHFL_FILE = 'data/brhfl.dat'

df_bkupdteb = pl.read_parquet(f'{BNM_DIR}BKUPDTEB.parquet')
rdate = df_bkupdteb['REPTDATE'][0].strftime('%d%m%y')

df_final1 = pl.read_parquet(f'{BNM_DIR}MNITLR1B.parquet').sort(['BRANCH', 'TRANCODE'])

df_final1 = df_final1.group_by(['BRANCH', 'TRANCODE']).agg([
    pl.first('TRANNAME'),
    pl.col('AMT').sum().alias('AMOUNT'),
    pl.col('CSHIN').sum().alias('CASHIN'),
    pl.col('CSHOUT').sum().alias('CASHOUT'),
    pl.col('CHEQUE').sum().alias('CHECKIN'),
    pl.col('ITEM1').sum().alias('ITEMA'),
    pl.col('ITEM2').sum().alias('ITEMB'),
    pl.col('ITEM3').sum().alias('ITEMC'),
    pl.col('ITEM4').sum().alias('ITEMD'),
    pl.col('ITEM5').sum().alias('ITEME'),
    pl.col('ITEM6').sum().alias('ITEMF')
]).sort(['BRANCH', 'TRANCODE'])

def assign_group(trancode):
    """Assign group and subgroup based on transaction code"""
    group_map = {
        ('0600','0608','7600','7608'): (1, 1.1),
        ('1600','1604','1700','8600','8604','8700'): (1, 1.2),
        ('2202','2212','2620','2220'): (1, 1.3),
        ('2070',): (1, 1.4),
        ('2050','2010','9010','2020','2030','2040'): (1, 1.5),
        ('2350','2330','2340','2360'): (1, 1.6),
        ('2120',): (1, 1.7),
        ('2080','2380','2400'): (1, 1.8),
        ('0626','0636','0750','7636','7750'): (2, 2.1),
        ('1626','8626'): (2, 2.2),
        ('0730','0738','7730','7738'): (3, 3.1),
        ('1730','8730'): (3, 3.2),
        ('0630','0638','7630','7638'): (4, 4.1),
        ('1630','1634','8630','8634'): (4, 4.2),
        ('2150',): (4, 4.3),
        ('2430',): (4, 4.4),
        ('0610','0618','0646','0656','0760','7610','7618','7656','7760'): (5, 5.1),
        ('1608','1610','1614','1618','8608','8610','8614','8618'): (5, 5.2),
        ('2130',): (5, 5.3),
        ('2410','2412'): (5, 5.4),
        ('0640','0648','7640','7648'): (6, 6.1),
        ('1640','1644','8640','8644'): (6, 6.2),
        ('2160',): (6, 6.3),
        ('2440',): (6, 6.4),
        ('0620','0628','7620','7628'): (7, 7.1),
        ('1620','1624','8620','8624'): (7, 7.2),
        ('2140',): (7, 7.3),
        ('2420',): (7, 7.4),
        ('0606','0616','0740','7616','7740'): (8, 8.1),
        ('1606','1706','8606','8706'): (8, 8.2),
        ('2606',): (8, 8.3),
        ('2086',): (8, 8.4),
        ('1718','1734','8718','8734'): (9, 9.1),
        ('2144',): (9, 9.2),
        ('1710','1726','8710','8726'): (10, 10.1),
        ('2158',): (10, 10.2),
        ('0660','0668','7660','7668'): (11, 11.1),
        ('1660','8660'): (11, 11.2),
        ('1714','1728','8714','8728'): (12, 12.1),
        ('2192',): (12, 12.2),
        ('0680','0688','7680','7688'): (13, 13.1),
        ('1680','1684','8680','8684'): (13, 13.2),
        ('2190',): (13, 13.3),
        ('2114',): (14, 14.1),
        ('2112',): (15, 15.1),
        ('0670','0678','7670','7678'): (16, 16.1),
        ('1670','1674','8670','8674'): (16, 16.2),
        ('2180',): (16, 16.3),
        ('0720','0728','7720','7728'): (17, 17.1),
        ('1720','1736','8720','8736'): (17, 17.2),
        ('2100',): (17, 17.3),
        ('0690','0698','7690','7698'): (18, 18.1),
        ('0650','0658','7650','7658'): (19, 19.1),
        ('1650','1654','8650','8654'): (19, 19.2),
        ('2170',): (19, 19.3),
        ('1698','8698'): (20, 20.1),
        ('2498',): (20, 20.2),
        ('0770','0778','7770','7778'): (20, 20.3),
        ('2000',): (21, 21.1),
        ('2480','9480'): (22, 22.1),
        ('5300','5302','5310','5312','5320','5322','2470','9470'): (23, 23.1),
        ('0140','9990','9992','9994'): (24, 24.1),
        ('0800','0808','3120','3128','7800','7808'): (25, 25.1),
        ('1800','1804','1900','1904','3020','8800','8804','8900','8904'): (25, 25.2),
        ('2200','2210','2630','2222'): (25, 25.3),
        ('2076',): (25, 25.4),
        ('2352','2362'): (25, 25.5),
        ('2122','2124'): (25, 25.6),
        ('2382','2450'): (25, 25.7),
        ('0970','0978','7970','7978'): (26, 26.1),
        ('1962','8962'): (26, 26.2),
        ('0990','0998','7990','7998'): (27, 27.1),
        ('1994','8994'): (27, 27.2),
        ('0810','7810','1814'): (28, 28.1),
        ('1820','8814','8820'): (28, 28.2),
        ('2152',): (28, 28.3),
        ('0818','7818'): (29, 29.1),
        ('1810','1818','8810','8818'): (29, 29.2),
        ('2132',): (29, 29.3),
        ('0848','7848'): (30, 30.1),
        ('1840','1846','8840','8846'): (30, 30.2),
        ('2162',): (30, 30.3),
        ('1914','1924','8914','8924'): (31, 31.1),
        ('2142',): (31, 31.2),
        ('1910','1918','8910','8918'): (32, 32.1),
        ('2108',): (32, 32.2),
        ('1890','8890'): (33, 33.1),
        ('0890','0898','7890','7898'): (34, 34.1),
        ('1894','8894'): (34, 34.2),
        ('2146',): (35, 35.1),
        ('0850','0858','7850','7858'): (36, 36.1),
        ('1850','8850'): (36, 36.2),
        ('0860','0868','7860','7868'): (37, 37.1),
        ('1860','8860'): (37, 37.2),
        ('0980','0988','7980','7988'): (38, 38.1),
        ('1990','8990'): (38, 38.2),
        ('0862','0864','7862','7864'): (39, 39.1),
        ('1866','8866'): (39, 39.2),
        ('0870','0878','7870','7878'): (40, 40.1),
        ('1870','8870'): (40, 40.2),
        ('0880','0888','7880','7888'): (41, 41.1),
        ('1880','8880'): (41, 41.2),
        ('0900','0908','7900','7908'): (42, 42.1),
        ('1970','8970'): (42, 42.2),
        ('0950','0958','7950','7958'): (43, 43.1),
        ('1950','8950'): (43, 43.2),
        ('2116',): (43, 43.3),
        ('1930','8930'): (44, 44.1),
        ('2118',): (45, 45.1),
        ('0960','0968','7960','7968'): (46, 46.1),
        ('1960','8960'): (46, 46.2),
        ('0920','0928','7920','7928'): (47, 47.1),
        ('1920','1926','8920','8926'): (47, 47.2),
        ('2102',): (47, 47.3),
        ('2110',): (48, 48.1),
        ('1808','8808'): (49, 49.1),
        ('2182',): (49, 49.2),
        ('1830','1838','8830','8838'): (50, 50.1),
        ('0820','0828','7820','7828'): (51, 51.1),
        ('1826','8826'): (51, 51.2),
        ('0840','0952','0954','7840','7954'): (52, 52.1),
        ('0830','0838','0910','0918','7830','7838','7910','7918'): (53, 53.1),
        ('1834','1844','1980','8834','8844','8980'): (53, 53.2),
        ('2172',): (53, 53.3),
        ('0940','0948','7940','7948'): (54, 54.1),
        ('1940','8940'): (54, 54.2),
        ('2500',): (54, 54.3),
        ('2510',): (54, 54.4),
        ('2520',): (54, 54.5),
        ('2398',): (55, 55.1),
        ('0930','0938','7930','7938'): (55, 55.2),
        ('1998','8998'): (55, 55.3),
        ('2490','9490'): (56, 56.1),
        ('2472','9472'): (56, 56.2),
        ('0150','9991','9993','9995','0130','0192','0300','0310','0399','0450','0455',
         '1300','1310','1320','1330','1340','3010','3110','3500','3501','3502','3503',
         '3510','3511','3512','3513','3522','3523','5100','5110','5120'): (57, 57.1)
    }
    
    for codes, (grp, sgrp) in group_map.items():
        if trancode in codes:
            return grp, sgrp
    return 58, 58.1

groups = []
sgroups = []
for tc in df_final1['TRANCODE'].to_list():
    g, sg = assign_group(tc)
    groups.append(g)
    sgroups.append(sg)

df_final1 = df_final1.with_columns([
    pl.Series('GROUP', groups),
    pl.Series('SGROUP', sgroups)
])

branch_records = []
with open(BRHFL_FILE, 'r') as f:
    for line in f:
        branch_records.append({
            'BRANCH': int(line[1:4]),
            'BRHCD': line[5:8].strip(),
            'BRHNM': line[11:41].strip()
        })

df_brh = pl.DataFrame(branch_records).sort('BRANCH')

df_brdemo = df_final1.join(df_brh, on='BRANCH', how='left').with_columns([
    pl.when(pl.col('BRHCD') == '').then(pl.lit('N/A')).otherwise(pl.col('BRHCD')).alias('BRHCD')
])

zdate = datetime.today() - timedelta(days=7)
zmm = zdate.month
zyy = zdate.year

with open(OUTPUT_BRDEMO, 'w') as f:
    for branch, group in df_brdemo.group_by('BRANCH'):
        gitems = group[['ITEMA','ITEMB','ITEMC','ITEMD','ITEME','ITEMF']].sum()
        gtlitem = sum([gitems[col][0] for col in ['ITEMA','ITEMB','ITEMC','ITEMD','ITEME','ITEMF']])
        
        brhcd = group['BRHCD'][0]
        brhnm = group['BRHNM'][0]
        
        f.write(f"{zmm:02d}{zyy:04d} {branch:03d} {brhcd:3} {brhnm:30} {gtlitem:10.0f}\n")

df_final1 = df_final1.sort(['BRANCH', 'GROUP', 'SGROUP', 'TRANCODE'])

with open(OUTPUT_PRINT1, 'w') as f:
    pagecnt = 0
    linecnt = 0
    
    bamt = bcshi = bcshout = bcheq = 0
    bitema = bitemb = bitemc = bitemd = biteme = bitemf = 0
    
    for branch, branch_group in df_final1.group_by('BRANCH', maintain_order=True):
        pagecnt += 1
        
        f.write(f"{'PUBLIC BANK BERHAD':^80}PAGE NO : {pagecnt}\n")
        f.write(f"TELLER TRANSACTIONS BY BRANCH AS AT {rdate}\n\n")
        f.write(f"        <------------------------------ AMOUNT ------------------------------>"
                f"        <--------------------- ITEM COUNT ---------------------->\n")
        f.write(f"BRANCH  TRAN AMOUNT         CASH IN         CASH OUT          CHEQUE"
                f"          5K     10K     50K    100K    200K   >200K      TOTAL\n")
        f.write(f"{'─'*130}\n\n")
        
        gamt = gcshin = gcshout = gcheq = 0
        gitema = gitemb = gitemc = gitemd = giteme = gitemf = 0
        
        for row in branch_group.iter_rows(named=True):
            gamt += row['AMOUNT']
            gcshin += row['CASHIN']
            gcshout += row['CASHOUT']
            gcheq += row['CHECKIN']
            gitema += row['ITEMA']
            gitemb += row['ITEMB']
            gitemc += row['ITEMC']
            gitemd += row['ITEMD']
            giteme += row['ITEME']
            gitemf += row['ITEMF']
            
            bamt += row['AMOUNT']
            bcshin += row['CASHIN']
            bcshout += row['CASHOUT']
            bcheq += row['CHECKIN']
            bitema += row['ITEMA']
            bitemb += row['ITEMB']
            bitemc += row['ITEMC']
            bitemd += row['ITEMD']
            biteme += row['ITEME']
            bitemf += row['ITEMF']
        
        gtlitem = gitema + gitemb + gitemc + gitemd + giteme + gitemf
        f.write(f" {branch:03d}   {gamt:18,.2f} {gcshin:17,.2f} {gcshout:17,.2f} "
                f"{gcheq:17,.2f} {gitema:7.0f} {gitemb:7.0f} {gitemc:7.0f} "
                f"{gitemd:7.0f} {giteme:7.0f} {gitemf:7.0f} {gtlitem:10.0f}\n")
        linecnt += 1
    
    btlitem = bitema + bitemb + bitemc + bitemd + biteme + bitemf
    f.write(f"        {'─'*80}\n")
    f.write(f" TOTAL  {bamt:18,.2f} {bcshin:17,.2f} {bcshout:17,.2f} "
            f"{bcheq:17,.2f} {bitema:7.0f} {bitemb:7.0f} {bitemc:7.0f} "
            f"{bitemd:7.0f} {biteme:7.0f} {bitemf:7.0f} {btlitem:10.0f}\n")
    f.write(f"        {'─'*80}\n")

with open(OUTPUT_PRINT2, 'w') as f:
    for branch, branch_group in df_final1.group_by('BRANCH', maintain_order=True):
        pagecnt = 1
        linecnt = 6
        
        f.write(f" {branch:03d}    {'PUBLIC BANK BERHAD':^80}PAGE NO : {pagecnt}\n")
        f.write(f"TELLER TRANSACTIONS BY BRANCH AS AT {rdate}\n\n")
        f.write(f"                               <-------------------------- AMOUNT ----------------------->"
                f"        <---------------- ITEM COUNT ----------------->\n")
        f.write(f"BRH CODE DESCRIPTION               TRAN AMOUNT      CASH IN     CASH OUT       CHEQUE"
                f"     5K    10K    50K   100K   200K  >200K   TOTAL\n")
        f.write(f"{'─'*132}\n\n")
        
        gamt = gcshin = gcshout = gcheq = 0
        gitema = gitemb = gitemc = gitemd = giteme = gitemf = 0
        
        for group_num, group_data in branch_group.group_by('GROUP', maintain_order=True):
            tamt = tcshin = tcshout = tcheq = 0
            titema = titemb = titemc = titemd = titeme = titemf = 0
            
            for sgroup_num, sgroup_data in group_data.group_by('SGROUP', maintain_order=True):
                samt = scshin = scshout = scheq = 0
                sitema = sitemb = sitemc = sitemd = siteme = sitemf = 0
                
                for row in sgroup_data.iter_rows(named=True):
                    tlitem = row['ITEMA'] + row['ITEMB'] + row['ITEMC'] + row['ITEMD'] + row['ITEME'] + row['ITEMF']
                    
                    f.write(f"{branch:03d} {row['TRANCODE']:4} {row['TRANNAME']:21} "
                            f"{row['AMOUNT']:15,.2f} {row['CASHIN']:13,.2f} {row['CASHOUT']:13,.2f} "
                            f"{row['CHECKIN']:12,.2f} {row['ITEMA']:6.0f} {row['ITEMB']:6.0f} "
                            f"{row['ITEMC']:6.0f} {row['ITEMD']:5.0f} {row['ITEME']:5.0f} "
                            f"{row['ITEMF']:5.0f} {tlitem:6.0f}\n")
                    
                    samt += row['AMOUNT']
                    scshin += row['CASHIN']
                    scshout += row['CASHOUT']
                    scheq += row['CHECKIN']
                    sitema += row['ITEMA']
                    sitemb += row['ITEMB']
                    sitemc += row['ITEMC']
                    sitemd += row['ITEMD']
                    siteme += row['ITEME']
                    sitemf += row['ITEMF']
                    
                    tamt += row['AMOUNT']
                    tcshin += row['CASHIN']
                    tcshout += row['CASHOUT']
                    tcheq += row['CHECKIN']
                    titema += row['ITEMA']
                    titemb += row['ITEMB']
                    titemc += row['ITEMC']
                    titemd += row['ITEMD']
                    titeme += row['ITEME']
                    titemf += row['ITEMF']
                    
                    gamt += row['AMOUNT']
                    gcshin += row['CASHIN']
                    gcshout += row['CASHOUT']
                    gcheq += row['CHECKIN']
                    gitema += row['ITEMA']
                    gitemb += row['ITEMB']
                    gitemc += row['ITEMC']
                    gitemd += row['ITEMD']
                    giteme += row['ITEME']
                    gitemf += row['ITEMF']
                
                stlitem = sitema + sitemb + sitemc + sitemd + siteme + sitemf
                f.write(f"                               {'─'*100}\n")
                f.write(f"       TOTAL FOR SUBGROUP {sgroup_num:.1f}  {samt:15,.2f} {scshin:13,.2f} "
                        f"{scshout:13,.2f} {scheq:12,.2f} {sitema:6.0f} {sitemb:6.0f} "
                        f"{sitemc:6.0f} {sitemd:5.0f} {siteme:5.0f} {sitemf:5.0f} {stlitem:6.0f}\n")
                f.write(f"                               {'─'*100}\n\n")
            
            ttlitem = titema + titemb + titemc + titemd + titeme + titemf
            f.write(f"                               {'─'*100}\n")
            f.write(f"           TOTAL FOR GROUP {group_num}  {tamt:15,.2f} {tcshin:13,.2f} "
                    f"{tcshout:13,.2f} {tcheq:12,.2f} {titema:6.0f} {titemb:6.0f} "
                    f"{titemc:6.0f} {titemd:5.0f} {titeme:5.0f} {titemf:5.0f} {ttlitem:6.0f}\n")
            f.write(f"                               {'═'*100}\n\n")
        
        gtlitem = gitema + gitemb + gitemc + gitemd + giteme + gitemf
        f.write(f"                               {'─'*100}\n")
        f.write(f"          TOTAL FOR BRANCH {branch:03d}  {gamt:15,.2f} {gcshin:13,.2f} "
                f"{gcshout:13,.2f} {gcheq:12,.2f} {gitema:6.0f} {gitemb:6.0f} "
                f"{gitemc:6.0f} {gitemd:5.0f} {giteme:5.0f} {gitemf:5.0f} {gtlitem:6.0f}\n")
        f.write(f"                               {'═'*100}\n\n")
        f.write(f"\f")

print(f"Teller Transaction Reporting Complete")
print(f"Report Date: {rdate}")
print(f"\nOutput files generated:")
print(f"  {OUTPUT_PRINT1} (Consolidated)")
print(f"  {OUTPUT_PRINT2} (Branch Detail)")
print(f"  {OUTPUT_BRDEMO} (Branch Demo)")
print(f"\nTotal branches processed: {df_final1['BRANCH'].n_unique()}")
print(f"Total transaction codes: {df_final1['TRANCODE'].n_unique()}")
