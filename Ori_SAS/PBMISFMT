
DATA _NULL_;
  CALL SYMPUT('AGELIMIT',12);
  CALL SYMPUT('MAXAGE',18);
  CALL SYMPUT('AGEBELOW',11);
RUN;

PROC FORMAT CNTLOUT=PBMISFMT;
   VALUE BRCHCD
     1,3000-3001,7000,7500-8000,8050-8750= 'HOE'
     2,3002     = 'JSS'    /* JALAN SULTAN SULAIMAN (JSS)         */
     3,3003     = 'JRC'    /* JALAN RAJA CHULAN (JRC)             */
     4,3004     = 'MLK'    /* MALACCA (MLK)                       */
     5,3005     = 'IMO'    /* IPOH MAIN OFFICE (IMO)              */
     6,3006     = 'PPG'    /* PULAU PINANG (PPG)                  */
     7,3007     = 'JBU'    /* JOHOR BAHRU (JBU)                   */
     8,3008     = 'KTN'    /* KUANTAN (KTN)                       */
     9,3009     = 'JYK'    /* JALAN YANG KALSOM (JYK)             */
     10,3010    = 'ASR'    /* ALOR SETAR (ASR)                    */
     11,3011    = 'GRN'    /* GURUN (GRN)                         */
     12,3012    = 'PPH'    /* PASIR PUTEH (PPH)                   */
     13,3013    = 'KBU'    /* KOTA BHARU (KBU)                    */
     14,3014    = 'TMH'    /* TANAH MERAH (TMH)                   */
     15,3015    = 'KPG'    /* KEPONG (KPG)                        */
     16,3016    = 'NLI'    /* NILAI (NLI)                         */
     17,3017    = 'TPN'    /* TAMPIN (TPN)                        */
     18,3018    = 'PJN'    /* PJ NEW TOWN (PJN)                   */
     19,3019    = 'DUA'    /* DAMANSARA UTAMA (DUA)               */
     20,3020    = 'TCL'    /* TAMAN CHI LIUNG (TCL)               */
     21,3021    = 'BPT'    /* BATU PAHAT (BPT)                    */
     22,3022    = 'SMY'    /* SEMENYIH (SMY)                      */
     23,3023    = 'KMT'    /* KEMUNTING (KMT)                     */
     24,3024    = 'RSH'    /* RASAH (RSH)                         */
     25,3025    = 'SAM'    /* SHAH ALAM (SAM)                     */
     26,3026    = 'SPG'    /* SERI PETALING (SPG)                 */
     27,3027    = 'NTL'    /* NIBONG TEBAL (NTL)                  */
     28,3028    = 'MUA'    /* MUAR (MUA)                          */
     29,3029    = 'JRL'    /* JALAN RAJA LAUT (JRL)               */
     30,3030    = 'KTU'    /* K. TRENGGANU (KTU)                  */
     31,3031    = 'SKC'    /* SEKINCHAN (SKC)                     */
     32,3032    = 'WSS'    /* KUCHING (WSS)                       */
     33,3033    = 'KKU'    /* KOTA KINABALU (KKU)                 */
     34,3034    = 'KGR'    /* KANGAR (KGR)                        */
     35,3035    = 'SSA'    /* SERI SETIA (SSA)                    */
     36,3036    = 'SS2'    /* SS2 (SS2)                           */
     37,3037    = 'TSA'    /* TMN SENTOSA (TSA)                   */
     38,3038    = 'JKL'    /* JALAN KELANG LAMA (JKL)             */
     39,3039    = 'KKG'    /* KUALA KELAWANG (KKG)                */
     40,3040    = 'JSB'    /* JALAN SG BESI (JSB)                 */
     41,3041    = 'JIH'    /* JALAN IPOH (JIH)                    */
     42,3042    = 'BMM'    /* BUKIT MERTAJAM (BMM)                */
     43,3043    = 'BTG'    /* BANTING (BTG)                       */
     44,3044    = 'TWU'    /* TAWAU (TWU)                         */
     45,3045    = 'SRB'    /* SEREMBAN (SRB)                      */
     46,3046    = 'APG'    /* AMPANG (APG)                        */
     47,3047    = 'SGM'    /* SEGAMAT (SGM)                       */
     48,3048    = 'MTK'    /* MENTAKAB (MTK)                      */
     49,3049    = 'JLP'    /* JELAPANG (JLP)                      */
     50,3050    = 'MRI'    /* MIRI (MRI)                          */
     51,3051    = 'SMG'    /* SERI MANJUNG (SMG)                  */
     52,3052    = 'UTM'    /* ULU TIRAM (UTM)                     */
     53,3053    = 'TMI'    /* TMN MALURI (TMI)                    */
     54,3054    = 'BBB'    /* BANDAR BAYAN BARU (BBB)             */
     55,3055    = 'LBN'    /* LABUAN                              */
     56,3056    = 'KJG'    /* KAJANG (KJG)                        */
     57,3057    = 'SPI'    /* SUNGAI PETANI (SPI)                 */
     58,3058    = 'SBU'    /* SIBU (SBU)                          */
     59,3059    = 'PKL'    /* PONTIAN KECIL (PKL)                 */
     60,3060    = 'BAM'    /* BAGAN AJAM (BAM)                    */
     61,3061    = 'KLI'    /* KULAI (KLI)                         */
     62,3062    = 'SDK'    /* SANDAKAN (SDK)                      */
     63,3063    = 'GMS'    /* GEMAS (GMS)                         */
     64,3064    = 'PDN'    /* PORT DICKSON (PDN)                  */
     65,3065    = 'BHU'    /* BAHAU (BHU)                         */
     66,3066    = 'BDA'    /* BUKIT DAMANSARA (BDA)               */
     67,3067    = 'CMR'    /* CHEMOR (CMR)                        */
     68,3068    = 'SAT'    /* SIMPANG AMPAT (SAT)                 */
     69,3069    = 'BKI'    /* BATANG KALI (BKI)                   */
     70,3070    = 'PSA'    /* POKOK SENA (PSA)                    */
     71,3071    = 'BCG'    /* BRINCHANG (BCG)                     */
     72,3072    = 'PPR'    /* PAPAR (PPR)                         */
     73,3073    = 'SPK'    /* SUNGAI PELEK (SPK)                  */
     74,3074    = 'SIK'    /* SIK (SIK)                           */
     75,3075    = 'CAH'    /* CHAAH (CAH)                         */
     76,3076    = 'PRS'    /* PANTAI REMIS (PRS)                  */
     77,3077    = 'PLI'    /* PULAU LANGKAWI (PLI)                */
     78,3078    = 'SJA'    /* SUBANG JAYA (SJA)                   */
     79,3079    = 'MSI'    /* MASAI (MSI)                         */
     80,3080    = 'MLB'    /* MENGLEMBU (MLB)                     */
     81,3081    = 'SBH'    /* SUNGAI BULOH (SBH)                  */
     82,3082    = 'MCG'    /* MACHANG (MCG)                       */
     83,3083    = 'JBB'    /* JALAN BUKIT BINTANG (JBB)           */
     84,3084    = 'PMS'    /* PASIR MAS (PMS)                     */
     85,3085    = 'SST'    /* SUNGAI SIPUT (SST)                  */
     86,3086    = 'CLN'    /* CHANGLUN (CLN)                      */
     87,3087    = 'MSG'    /* MERSING (MSG)                       */
     88,3088    = 'KUM'    /* KULIM (KUM)                         */
     89,3089    = 'TPI'    /* TAMPOI (TPI)                        */
     90,3090    = 'BTL'    /* BINTULU (BTL)                       */
     91,3091    = 'KUG'    /* KLUANG (KUG)                        */
     92,3092    = 'KLG'    /* KLANG (KLG)                         */
     93,3093    = 'EDU'    /* ENDAU (EDU)                         */
     94,3094    = 'STP'    /* SETAPAK (STP)                       */
     95,3095    = 'TIN'    /* TELUK INTAN (TIN)                   */
     96,3096    = 'SGK'    /* SERI GOMBAK (SGK)                   */
     97,3097    = 'HSL'    /* JALAN TUN H S LEE (HSL)             */
     98,3098    = 'TCY'    /* TMN CENTURY (TCY)                   */
     102,3102   = 'PRJ'    /* PARIT RAJA (PRJ)                    */
     103,3103   = 'JJG'    /* JINJANG (JJG)                       */
     104,3104   = 'KKL'    /* KUALA KETIL (KKL)                   */
     105,3105   = 'KTI'    /* KOTA TINGGI (KTI)                   */
     106,3106   = 'CKI'    /* CHUKAI (CKI)                        */
     107,3107   = 'JLT'    /* JELUTONG (JLT)                      */
     108,3108   = 'BSI'    /* BAGAN SERAI (BSI)                   */
     109,3109   = 'KSR'    /* K. SELANGOR (KSR)                   */
     110,3110   = 'TJJ'    /* TMN JOHOR JAYA (TJJ)                */
     111,3111   = 'AKH'    /* AIR KEROH (AKH)                     */
     112,3112   = 'LDO'    /* LIDO (LDO)                          */
     113,3113   = 'TML'    /* TEMERLOH (TML)                      */
     114,3114   = 'BBA'    /* BANDAR BARU AIR ITAM (BBA)          */
     115,3115   = 'KNG'    /* KENINGAU (KNG)                      */
     116,3116   = 'TRI'    /* TRIANG (TRI)                        */
     117,3117   = 'KKI'    /* KUALA KRAI (KKI)                    */
     118,3118   = 'TMW'    /* TMN MELAWATI (TMW)                  */
     119,3119   = 'BNV'    /* BERCHAM N/V (BNV)                   */
     120,3120   = 'PIH'    /* PANDAN INDAH (PIH)                  */
     121,3121   = 'PRA'    /* PRAI (PRA)                          */
     122,3122   = 'SKN'    /* SRI KEMBANGAN (SKN)                 */
     123,3123   = 'IGN'    /* IPOH GARDEN (IGN)                   */
     124,3124   = 'S14'    /* SECTION 14 (S14)                    */
     125,3125   = 'KJA'    /* KELANA JAYA (KJA)                   */
     126,3126   = 'PTS'    /* PULAU TIKUS (PTS)                   */
     127,3127   = 'TSM'    /* TMN SERI MUDA (TSM)                 */
     128,3128   = 'SGB'    /* SEGAMBUT (SGB)                      */
     129,3129   = 'BSR'    /* BANGSAR (BSR)                       */
     130,3130   = 'PDG'    /* PADUNGAN (PDG)                      */
     131,3131   = 'TMG'    /* TMN MAYANG (TMG)                    */
     132,3132   = 'CKT'    /* CHOW KIT (CKT)                      */
     133,3133   = 'PKG'    /* PORT KLANG (PKG)                    */
     134,3134   = 'RPG'    /* RANTAU PANJANG (RPG)                */
     135,3135   = 'BSY'    /* BANDAR SUNWAY (BSY)                 */
     136,3136   = 'TCS'    /* TMN CHERAS (TCS)                    */
     137,3137   = 'JPP'    /* JALAN PASIR PUTEH (JPP)             */
     138,3138   = 'WMU'    /* WANGSA MAJU (WMU)                   */
     139,3139   = 'JRT'    /* JERANTUT (JRT)                      */
     140,3140   = 'CPE'    /* CITY PARADE (CPE)                   */
     141,3141   = 'STL'    /* SENTUL (STL)                        */
     142,3142   = 'KBD'    /* KOTA BELUD (KBD)                    */
     143,3143   = 'LDU'    /* LAHAD DATU (LDU)                    */
     144,3144   = 'KHG'    /* KAHANG (KHG)                        */
     145,3145   = 'BSD'    /* BDR SRI DAMANSARA (BSD)             */
     146,3146   = 'PSG'    /* PUSING (PSG)                        */
     147,3147   = 'PNS'    /* PEKAN NENAS (PNS)                   */
     148,3148   = 'PJO'    /* PJ OLD TOWN (PJO)                   */
     149,3149   = 'BFT'    /* BEAUFORT (BFT)                      */
     150,3150   = 'LMM'    /* LEBOH MACALLUM (LMM)                */
     151,3151   = 'SLY'    /* SELAYANG (SLY)                      */
     152,3152   = 'ATR'    /* AYER TAWAR (ATR)                    */
     153,3153   = 'USJ'    /* USJ (USJ)                           */
     154,3154   = 'BSJ'    /* BDR SEBERANG JAYA (BSJ)             */
     155,3155   = 'TTJ'    /* TMN TAMING JAYA (TTJ)               */
     156,3156   = 'TMR'    /* TMN MELAKA RAYA (TMR)               */
     157,3157   = 'BPJ'    /* BDR PUCHONG JAYA (BPJ)              */
     158,3158   = 'SPL'    /* SIMPANG PULAI (SPL)                 */
     159,3159   = 'RLU'    /* RELAU (RLU)                         */
     160,3160   = 'MTH'    /* MASJID TANAH (MTH)                  */
     161,3161   = 'DGG'    /* DONGGONGON (DGG)                    */
     162,3162   = 'SEA'    /* SEA PARK (SEA)                      */
     163,3163   = 'JKA'    /* JALAN KUCHAI LAMA (JKA)             */
     164,3164   = 'KBS'    /* KEPALA BATAS (KBS)                  */
     165,3165   = 'TKA'    /* TENGKERA (TKA)                      */
     166,3166   = 'PGG'    /* PASIR GUDANG (PGG)                  */
     167,3167   = 'BBG'    /* BUKIT BERUNTUNG (BBG)               */
     168,3168   = 'KLC'    /* KUALA LUMPUR CITY MAIN (KLC)        */
     169,3169   = 'CTD'    /* CHANGKAT THAMBI DOLLAH (CTD)        */
     170,3170   = 'PJA'    /* PANDAN JAYA  (PJA)                  */
     171,3171   = 'JMR'    /* JALAN MACALISTER (JMR)              */
     172,3172   = 'TMJ'    /* TAMAN MALIM JAYA (TMJ)              */
     173,3173   = 'SCA'    /* SUNGAI CHUA (SCA)                   */
     174,3174   = 'BBP'    /* BANDAR BARU PERMAS JAYA (BBP)       */
     175,3175   = 'LBG'    /* LIMBANG (LBG)                       */
     176,3176   = 'TPG'    /* TAMAN PERLING JAYA                  */
     177,3177   = 'JRU'    /* JALAN RAJA UDA                      */
     178,3178   = 'MIN'    /* MEDAN IDAMAN                        */
     179,3179   = 'OUG'    /* OVERSEAS UNION GARDEN               */
     180,3180   = 'KBG'    /* KAMPUNG BARU SUBANG                 */
     182,3182   = 'JPU'    /* JALAN PULAU, SIBU                   */
     183,3183   = 'JCL'      /* JALAN CENTRAL, SIBU (JCL)         */
     184,3184   = 'JPN'      /* JALAN PENRISSEN, KUCHING (JPN)    */
     185,3185   = 'KCY'      /* JALAN TUN HJ OPENG, KUCHING (KCY) */
     186,3186   = 'JTZ'      /* JALAN TUN ZAIDI, KUCHING (JTZ)    */
 /*  187,3187   = 'CST'         CHINA STREET, MIRI (CST)          */
     188,3188   = 'PLT'      /* PELITA, MIRI (PLT)                */
     189,3189   = 'BNH'      /* BATU NIAH (BNH)                   */
     190,3190   = 'BTR'      /* BINTANGOR (BTR)                   */
     191,3191   = 'KPT'      /* KAPIT (KPT)                       */
     192,3192   = 'MRD'      /* MARUDI (MRD)                      */
     193,3193   = 'MKH'      /* MUKAH (MKH)                       */
     194,3194   = 'SRK'      /* SARIKEI (SRK)                     */
     195,3195   = 'BWK'      /* BINTANG WALK BRANCH, KL (BWK)     */
     196,3196   = 'JHL'      /* JALAN HANG LEKIU (JHL)            */
     197,3197   = 'TNM'      /* TIONG NAM BRANCH, KL (TNM)        */
     198,3198   = 'TDA'      /* TAMAN DESA, JLN KELANG LAMA (TDA) */
     199,3199   = 'JTH'      /* JALAN TAPAH, KLANG (JTH)          */
  /* 200,3200   = 'JSK'         JALAN SK10/4, SERI KEMBANGAN (JSK)*/
     201,3201   = 'PDA'      /* PANDAMARAN, KLANG (PDA)           */
     202,3202   = 'RWG'      /* RAWANG (RWG)                      */
     203,3203   = 'SJM'      /* SUNGAI JAROM (SJM)                */
     204,3204   = 'BTW'      /* BUTTERWORTH (BTW)                 */
     205,3205   = 'SNG'      /* SUNGAI NIBONG (SNG)               */
     206,3206   = 'TBM'      /* TAMAN BUKIT MAS (TBM)             */
     207,3207   = 'BCM'      /* JALAN BERCHAM, IPOH (JBM)         */
     208,3208   = 'JSI'      /* JALAN SULTAN IDRIS SHAH, IPOH (JSI)*/
     209,3209   = 'STW'      /* SITIAWAN (STW)                    */
     210,3210   = 'TMM'      /* TANJUNG MALIM (TMM)               */
     211,3211   = 'TPD'      /* TANJUNG PIANDANG (TPD)            */
     212,3212   = 'JMA'      /* JLN MUNSHI ABDULLAH, MELAKA (JMA)   */
     213,3213   = 'JKB'      /* JALAN KAMPUNG BARU, SG PETANI (JKB) */
     214,3214   = 'JGA'      /* JALAN GEREJA, JOHOR BARU (JGA)      */
     215,3215   = 'JKP'      /* JALAN KUKUP, PONTIAN KECHIL (JKP)   */
     216,3216   = 'SKI'      /* SKUDAI (SKI)                        */
     217,3217   = 'TMB'      /* T. MUNSHI IBRAHIM, TAMPOI (TMB)     */
     220,3220   = 'GHS'      /* GENTING HIGHLANDS, PAHANG (GHS)     */
     221,3221   = 'TSK'      /* TAMAN SENTOSA, KLANG                */
     222,3222   = 'TDC'      /* TAMAN DESA CEMERLANG, JOHOR         */
     223,3223   = 'TRJ'      /* TAMAN RASAH JAYA, SEREMBAN          */
     224,3224   = 'JAH'      /* JALAN ABDULLAH, MUAR                */
     225,3225   = 'TIH'      /* TAMAN INDAH                         */
     226,3226   = 'JPR'      /* JALAN PASAR, KUALA LUMPUR           */
     227,3227   = 'KSB'      /* KAMPONG BARU, SG BULOH, SELANGOR    */
     228,3228   = 'INN'      /* INANAM, KOTA KINABALU               */
     229,3229   = 'TSJ'      /* TAMAN SELAYANG JAYA, SELANGOR       */
     230,3230   = 'SSH'      /* SALAK SOUTH, KUALA LUMPUR           */
     231,3231   = 'BBM'      /* BATU BERENDAM, MELAKA               */
     232,3232   = 'TMD'      /* TAMAN MIDAH, KUALA LUMPUR           */
     233,3233   = 'BEN'      /* BENTONG, PAHANG - PFB CONVERTED     */
     234,3234   = 'SRM'      /* SIMPANG RENGAM, JOHORE. PFB CONV.   */
     235,3235   = 'SBM'      /* SABAK BERNAM, SELANGOR              */
     236,3236   = 'UYB'      /* ULU YAM BARU, SELANGOR              */
     237,3237   = 'KLS'      /* KUALA LIPIS, PAHANG                 */
     238,3238   = 'JKT'      /* JALAN KOTA, KEDAH                   */
     239,3239   = 'KMY'      /* KEMAYAN, PAHANG                     */
     240,3240   = 'KAP'      /* KAPAR, SELANGOR                     */
     241,3241   = 'DJA'      /* DAMANSARA JAYA, SELANGOR            */
     242,3242   = 'TKK'      /* TANGKAK JOHOR                       */
     243,3243   = 'KKR'      /* KUALA KANGSAR PERAK                 */
     244,3244   = 'GRT'      /* GUNUNG RAPAT IPOH.                  */
     245,3245   = 'BDR'      /* BIDOR PERAK                         */
     246,3246   = 'BGH'      /* BATU GAJAH PERAK                    */
     247,3247   = 'BPR'      /* BUKIT PASIR MUAR JOHOR              */
     248,3248   = 'JTS'      /* JLN TUN SAMBANTHAN, KUALA LUMPUR    */
     249,3249   = 'TAI'      /* TAIPING PERAK                       */
     250,3250   = 'TEA'      /* TAMAN ENG ANN KELANG SELANGOR       */
     251,3251   = 'KPR'      /* KAMPAR PERAK                        */
     252,3252   = 'TMA'      /* TAMAN MUDA AMPANG SELANGOR          */
     253,3253   = 'JTT'      /* JALAN TANJONG TOKONG, P.PINANG      */
     254,3254   = 'KPH'      /* KUALA PILAH NEGERI SEMBILAN         */
     255,3255   = 'SBP'      /* SUNGAI BAKAP, S. PERAI,PULAU PINANG */
     256,3256   = 'PBR'      /* PARIT BUNTAR PERAK                  */
     257,3257   = 'RAU'      /* RAUB PAHANG                         */
     258,3258   = 'JTA'      /* JITRA KEDAH                         */
     259,3259   = 'SAN'      /* SRI AMAN, SARAWAK                   */
     260,3260   = 'KDN'      /* KUALA DUNGUN TERENGGANU             */
     261,3261   = 'GMG'      /* GUA MUSANG, KELANTAN                */
     262,3262   = 'TCT'      /* TAMAN CONNAUGHT CHERAS KUALA LUMPUR */
     263,3263   = 'BTA'      /* BENTA PAHANG                        */
     264,3264   = 'JBH'      /* JALAN BESERAH PAHANG                */
     265,3265   = 'JAI'      /* JALAN AIR ITAM, PENANG              */
     266,3266   = 'JDK'      /* JALAN DATUK KERAMAT, PENANG         */
     267,3267   = 'TDI'      /* TMN TUN DR ISMAIL, WILAYAH          */
     268,3268   = 'BBT'      /* BANDAR BUKIT TINGGI, KLANG SELANGOR */
     269,3269   = 'MKA'      /* MONT KIARA,KUALA LUMPUR             */
     270,3270   = 'BPI'      /* BANDAR PUTERI PUCHONG               */
     273,3273   = 'LHA'      /* LI HUA, SARAWAK                     */
     277,3277   = 'WSU'      /* WAKAF SIKU, KELANTAN                */
     278,3278   = 'JPI'      /* JALAN PANTAI, SABAH                 */
     274,3274   = 'STG'      /* STUTONG, SARAWAK                    */
     275,3275   = 'MSL'      /* MEDAN SENTRAL, SARAWAK              */
     276,3276   = 'JAS'      /* JALAN APAS, SABAH                   */
     279,3279   = 'PTJ'      /* PUTRAJAYA                           */
     280,3280   = 'KDA'      /* KOTA DAMANSARA                      */
     281,3281   = 'PLT'      /* PELITA                              */
     282,3282   = 'PTT'      /* PUTATAN                             */
     283,3283   = 'PSE'      /* PRIMA SQUARE                        */
     284,3284   = 'BSP'      /* BANDAR SRI PERMAISURI               */
     285,3285   = 'BMC'      /* BANDAR MAHKOTA CHERAS               */
     286,3286   = 'BIH'      /* BUKIT INDAH BRANCH                  */
     287,3287   = 'SUA'    /* SUTERA UTAMA                        */
     288,3288   = 'SPT'    /* STARPARC POINT                      */
     289,3289   = 'TEE'    /* TAMAN EQUINE                        */
     290,3290   = 'TDY'    /* TAMAN DAYA                          */
     291,3291   = 'BSL'    /* BANDAR SUNGAI LONG                  */
     292,3292   = 'BMJ'    /* BANDAR MENJALARA                    */
     293,3293   = 'BSA'    /* BANDAR SETIA ALAM                   */
     294,3294   = 'KKM'    /* KOTA KEMUNING                       */
     295,3295   = 'BKR'    /* BANDAR KINRARA                      */
     296,3296   = 'BJL'    /* BUKIT JALIL                         */
     701,3701   = 'IKB'    /* KAMPUNG BARU ISLAMIC                */
     702,3702   = 'IPJ'    /* PUTRAJAYA                           */
     703,3703   = 'IWS'    /* ISLAMIC WAKAF SIKU                  */
     704,3704   = 'IJK'    /* ISLAMIC JALAN KOTA                  */
     ;

   VALUE $GROUPF
     'BBB' = 'GROUP 1'  /* BANDAR BAYAN BARU                   */
     'BDA' = 'GROUP 1'  /* BUKIT DAMANSARA                     */
     'BMM' = 'GROUP 1'  /* BUKIT MERTAJAM                      */
     'DUA' = 'GROUP 1'  /* DAMANSARA UTAMA                     */
     'IMO' = 'GROUP 1'  /* IPOH MAIN OFFICE                    */
     'JKL' = 'GROUP 1'  /* JALAN KELANG LAMA                   */
     'JRC' = 'GROUP 1'  /* JALAN RAJA CHULAN                   */
     'JRL' = 'GROUP 1'  /* JALAN RAJA LAUT                     */
     'JSS' = 'GROUP 1'  /* JALAN SULTAN SULAIMAN               */
     'JSB' = 'GROUP 1'  /* JALAN SUNGEI BESI                   */
     'JYK' = 'GROUP 1'  /* JALAN YANG KALSOM                   */
     'JBU' = 'GROUP 1'  /* JOHOR BAHRU                         */
     'KPG' = 'GROUP 1'  /* KEPONG                              */
     'KLC' = 'GROUP 1'  /* KL CITY BRANCH                      */
     'MLK' = 'GROUP 1'  /* MELAKA                              */
     'PPG' = 'GROUP 1'  /* PENANG                              */
     'SS2' = 'GROUP 1'  /* S S 2                               */
     'SPG' = 'GROUP 1'  /* SERI PETALING                       */
     'SSA' = 'GROUP 1'  /* SERI SETIA                          */
     'SAM' = 'GROUP 1'  /* SHAH ALAM                           */
     'SJA' = 'GROUP 1'  /* SUBANG JAYA                         */
     'TCL' = 'GROUP 1'  /* TAMAN CHI LIUNG                     */
     'TSA' = 'GROUP 1'  /* TAMAN SENTOSA                       */
     'BAM' = 'GROUP 2'  /* BAGAN AJAM                          */
     'ASR' = 'GROUP 2'  /* ALOR SETAR                          */
     'BSY' = 'GROUP 2'  /* BANDAR SUNWAY                       */
     'BSR' = 'GROUP 2'  /* BANGSAR                             */
     'BTG' = 'GROUP 2'  /* BANTING                             */
     'BPT' = 'GROUP 2'  /* BATU PAHAT                          */
     'CMR' = 'GROUP 2'  /* CHEMOR                              */
     'GRN' = 'GROUP 2'  /* GURUN                               */
     'IGN' = 'GROUP 2'  /* IPOH GARDEN                         */
     'JIH' = 'GROUP 2'  /* JALAN IPOH                          */
     'JJG' = 'GROUP 2'  /* JINJANG                             */
     'JBB' = 'GROUP 2'  /* JALAN BUKIT BINTANG                 */
     'KJG' = 'GROUP 2'  /* KAJANG                              */
     'KLG' = 'GROUP 2'  /* KLANG                               */
     'KBU' = 'GROUP 2'  /* KOTA BAHRU                          */
     'KKU' = 'GROUP 2'  /* KOTA KINABALU                       */
     'KTN' = 'GROUP 2'  /* KUANTAN                             */
     'WSS' = 'GROUP 2'  /* WISMA SABERKAS (KUCHING)            */
     'KLI' = 'GROUP 2'  /* KULAI                               */
     'MSI' = 'GROUP 2'  /* MASAI                               */
     'MLB' = 'GROUP 2'  /* MENGLEMBU                           */
     'MRI' = 'GROUP 2'  /* MIRI                                */
     'MUA' = 'GROUP 2'  /* MUAR                                */
     'NTL' = 'GROUP 2'  /* NIBONG TEBAL                        */
     'PDG' = 'GROUP 2'  /* PADUNGAN                            */
     'PJN' = 'GROUP 2'  /* P J NEW TOWN                        */
     'SMY' = 'GROUP 2'  /* SEMENYIH                            */
     'SRB' = 'GROUP 2'  /* SEREMBAN                            */
     'SGK' = 'GROUP 2'  /* SERI GOMBAK                         */
     'SKN' = 'GROUP 2'  /* SERI KEMBANGAN                      */
     'SAT' = 'GROUP 2'  /* SIMPANG AMPAT                       */
     'SBH' = 'GROUP 2'  /* SUNGEI BULOH                        */
     'TCS' = 'GROUP 2'  /* TAMAN CHERAS                        */
     'TMI' = 'GROUP 2'  /* TAMAN MALURI                        */
     'TMG' = 'GROUP 2'  /* TAMAN MAYANG                        */
     'TMW' = 'GROUP 2'  /* TAMAN MELAWATI                      */
     'TPN' = 'GROUP 2'  /* TAMPIN                              */
     'BBA' = 'GROUP 3'  /* BANDAR BARU AIR ITAM                */
     'APG' = 'GROUP 3'  /* AMPANG                              */
     'BHU' = 'GROUP 3'  /* BAHAU                               */
     'BSD' = 'GROUP 3'  /* BANDAR SERI DAMANSARA               */
     'BKI' = 'GROUP 3'  /* BATANG KALI                         */
     'BNV' = 'GROUP 3'  /* BERCHAM N. VILLAGE                  */
     'BTL' = 'GROUP 3'  /* BINTULU                             */
     'BCG' = 'GROUP 3'  /* BRINCHANG                           */
     'CAH' = 'GROUP 3'  /* CHAAH                               */
     'CKT' = 'GROUP 3'  /* JALAN CHOW KIT                      */
     'JPP' = 'GROUP 3'  /* JALAN PASIR PUTIH                   */
     'JLP' = 'GROUP 3'  /* JELAPANG                            */
     'JLT' = 'GROUP 3'  /* JELUTONG                            */
     'KBR' = 'GROUP 3'  /* KAMPONG BAHRU                       */
     'KMT' = 'GROUP 3'  /* KAMUNTING                           */
     'KJA' = 'GROUP 3'  /* KELANA JAYA                         */
     'KUG' = 'GROUP 3'  /* KLUANG                              */
     'KKG' = 'GROUP 3'  /* KUALA KELAWANG                      */
     'KUM' = 'GROUP 3'  /* KULIM                               */
     'LBN' = 'GROUP 3'  /* LABUAN                              */
     'LMM' = 'GROUP 3'  /* LEBOH MACALLUM                      */
     'LDO' = 'GROUP 3'  /* LIDO                                */
     'MTK' = 'GROUP 3'  /* MENTAKAB                            */
     'NLI' = 'GROUP 3'  /* NILAI                               */
     'PIH' = 'GROUP 3'  /* PANDAN INDAH                        */
     'PRS' = 'GROUP 3'  /* PANTAI REMIS                        */
     'PJO' = 'GROUP 3'  /* PJ OLD TOWN                         */
     'PKL' = 'GROUP 3'  /* PONTIAN KECIL                       */
     'PKG' = 'GROUP 3'  /* PORT KLANG                          */
     'PTS' = 'GROUP 3'  /* PULAU TIKUS                         */
     'PSG' = 'GROUP 3'  /* PUSING                              */
     'RSH' = 'GROUP 3'  /* RASAH                               */
     'STL' = 'GROUP 3'  /* SENTUL                              */
     'S14' = 'GROUP 3'  /* SECTION 14                          */
     'SGM' = 'GROUP 3'  /* SEGAMAT                             */
     'SKC' = 'GROUP 3'  /* SEKINCHAN                           */
     'STP' = 'GROUP 3'  /* SETAPAK                             */
     'SBU' = 'GROUP 3'  /* SIBU                                */
     'SPK' = 'GROUP 3'  /* SUNGEI PELEK                        */
     'SPI' = 'GROUP 3'  /* SUNGEI PETANI                       */
     'TCY' = 'GROUP 3'  /* TAMAN CENTURY                       */
     'TJJ' = 'GROUP 3'  /* TAMAN JOHOR JAYA                    */
     'TSM' = 'GROUP 3'  /* TAMAN SERI MUDA                     */
     'TTJ' = 'GROUP 3'  /* TAMAN TAMING JAYA                   */
     'TPI' = 'GROUP 3'  /* TAMPOI                              */
     'TMH' = 'GROUP 3'  /* TANAH MERAH                         */
     'TWU' = 'GROUP 3'  /* TAWAU                               */
     'USJ' = 'GROUP 3'  /* USJ                                 */
     'UTM' = 'GROUP 3'  /* ULU TIRAM                           */
     'WMU' = 'GROUP 3'  /* WANGSA MAJU                         */
     'AKH' = 'GROUP 4'  /* AIR KEROH                           */
     'ATR' = 'GROUP 4'  /* AIR TAWAR                           */
     'BBG' = 'GROUP 4'  /* BUKIT BERUNTUNG                     */
     'BBP' = 'GROUP 4'  /* BANDAR BARU PERMAS JAYA             */
     'BSI' = 'GROUP 4'  /* BAGAN SERAI                         */
     'BPJ' = 'GROUP 4'  /* BANDAR PUCHONG JAYA                 */
     'BSJ' = 'GROUP 4'  /* BANDAT SEBERANG JAYA                */
     'BFT' = 'GROUP 4'  /* BEAUFORT                            */
     'CLN' = 'GROUP 4'  /* CHANGLUN                            */
     'CKI' = 'GROUP 4'  /* CHUKAI                              */
     'CTD' = 'GROUP 4'  /* CHANGKAT THAMBI DOLLAH              */
     'CPE' = 'GROUP 4'  /* CITY PARADE                         */
     'DGG' = 'GROUP 4'  /* DONGGONGON                          */
     'EDU' = 'GROUP 4'  /* ENDAU                               */
     'GMS' = 'GROUP 4'  /* GEMAS                               */
     'JKA' = 'GROUP 4'  /* JALAN KUCHAI LAMA                   */
     'JMR' = 'GROUP 4'  /* JALAN MACALISTER                    */
     'JRT' = 'GROUP 4'  /* JERANTUT                            */
     'HSL' = 'GROUP 4'  /* JLN TUN HS LEE                      */
     'KHG' = 'GROUP 4'  /* KAHANG                              */
     'KGR' = 'GROUP 4'  /* KANGAR                              */
     'KNG' = 'GROUP 4'  /* KENINGAU                            */
     'KBS' = 'GROUP 4'  /* KEPALA BATAS                        */
     'KBD' = 'GROUP 4'  /* KOTA BELUD                          */
     'KTI' = 'GROUP 4'  /* KOTA TINGGI                         */
     'KKL' = 'GROUP 4'  /* KUALA KETIL                         */
     'KKI' = 'GROUP 4'  /* KUALA KRAI                          */
     'KSR' = 'GROUP 4'  /* KUALA SELANGOR                      */
     'KTU' = 'GROUP 4'  /* KUALA TERENGGANU                    */
     'LDU' = 'GROUP 4'  /* LAHAD DATU                          */
     'MCG' = 'GROUP 4'  /* MACHANG                             */
     'MTH' = 'GROUP 4'  /* MASJID TANAH                        */
     'MSG' = 'GROUP 4'  /* MERSING                             */
     'PJA' = 'GROUP 4'  /* PANDAN JAYA                         */
     'PPR' = 'GROUP 4'  /* PAPAR                               */
     'PRJ' = 'GROUP 4'  /* PARIT RAJA                          */
     'PMS' = 'GROUP 4'  /* PASIR MAS                           */
     'PPH' = 'GROUP 4'  /* PASIR PUTIH                         */
     'PNS' = 'GROUP 4'  /* PEKAN NENAS                         */
     'PSA' = 'GROUP 4'  /* POKOK SENA                          */
     'PGG' = 'GROUP 4'  /* PASIR GUDANG                        */
     'PDN' = 'GROUP 4'  /* PORT DICKSON                        */
     'PRA' = 'GROUP 4'  /* PRAI                                */
     'PLI' = 'GROUP 4'  /* PULAU LANGKAWI                      */
     'RPJ' = 'GROUP 4'  /* RANTAU PANJANG                      */
     'RLU' = 'GROUP 4'  /* RELAU                               */
     'SDK' = 'GROUP 4'  /* SANDAKAN                            */
     'SEA' = 'GROUP 4'  /* SEA PARK                            */
     'SGB' = 'GROUP 4'  /* SEGAMBUT                            */
     'SLY' = 'GROUP 4'  /* SELAYANG                            */
     'SMG' = 'GROUP 4'  /* SERI MANJUNG                        */
     'SIK' = 'GROUP 4'  /* SIK                                 */
     'SPL' = 'GROUP 4'  /* SIMPANG PULAI                       */
     'SCA' = 'GROUP 4'  /* SUNGAI CHUA                         */
     'SST' = 'GROUP 4'  /* SUNGEI SIPUT                        */
     'TMJ' = 'GROUP 4'  /* TAMAN MALIM JAYA                    */
     'TMR' = 'GROUP 4'  /* TAMAN MELAKA RAYA                   */
     'TPG' = 'GROUP 4'  /* TAMAN PERLING JAYA                  */
     'TIN' = 'GROUP 4'  /* TELUK INTAN                         */
     'TML' = 'GROUP 4'  /* TEMERLOH                            */
     'TKA' = 'GROUP 4'  /* TENGKERA                            */
     'TRI' = 'GROUP 4'  /* TRIANG                              */
     'GHS' = 'GROUP 4'  /* GENTING HIGHLANDS                   */
     'BPI' = 'GROUP 4'  /* BANDAR PUTERI PUCHONG               */
     'HOE' = 'HOE'
     OTHER = 'GROUP 4';

    VALUE $ACKNOF
     '21930'           = 'NEF'
     '21940'           = 'NIF'
     '21960'           = 'SFT'
     '21950'           = '3F'
     '21910'           = 'SHP'
     '21920'           = 'SHP'
     '32300' - '32699' = 'SPECIAL DEPOSIT WITHOLDING TAX'
     OTHER             = 'OTHER';

   VALUE SAPROD
     204    = 'SPTFSD'   /* ISLAMIC BANKING AL-WADIAH */
     OTHER  = 'SD    ';  /* ALL OTHER SAVING CODES    */

   VALUE CAPROD
     104  = 'OTHER'    /* VOSTRO LOCAL           */
     105  = 'OTHER'    /* VOSTRO FOREIGN         */
     107  = 'OTHER'    /* PBCS                   */
     126  = 'OTHER'    /* NORMAL BANKERS CHEQUE  */
     127  = 'OTHER'    /* MIDF BANKERS CHEQUE    */
     128  = 'OTHER'    /* MIH BANKERS CHEQUE     */
     129  = 'OTHER'    /* DD NORMAL              */
     130  = 'OTHER'    /* USD C/A                */
     131  = 'OTHER'    /* GBP C/A                */
     132  = 'OTHER'    /* AUD C/A                */
     133  = 'OTHER'    /* SGD C/A                */
     134  = 'OTHER'    /* JPY C/A                */
     135  = 'OTHER'    /* DEM C/A                */
     136  = 'OTHER'    /* HKD C/A                */
     140  = 'OTHER'    /* SI-CLEARING ACCOUNT    */
     141  = 'OTHER'    /* ATM-PISA CLEARING ACCT */
     142  = 'OTHER'    /* ATM-LOAN CLEARING ACCT */
     143  = 'OTHER'    /* ATM-FD CLEARING ACCT   */
     144  = 'OTHER'    /* DD SHARE PUBLIC        */
     145  = 'OTHER'    /* DD SHARE BUMI          */
     146  = 'OTHER'    /* IBT COLLECTION ACCT    */
     147  = 'OTHER'    /* HO INTERBANK SETTLEMENT A/C */
     148  = 'OTHER'    /* INTERBANK GIRO & MEPS CASH COLL) */
     149  = 'OTHER'    /* BNM BONDS COLLECTION ACCOUNT */
     171  = 'OTHER'    /* MYR OVERNIGHT A/C      */
     172  = 'OTHER'    /* MYR OVERNIGHT A/C      */
     173  = 'OTHER'    /* MYR OVERNIGHT A/C      */
     OTHER = 'DD   ';

   VALUE ODPROD
     104  = 'OTHER'    /* VOSTRO LOCAL           */
     105  = 'OTHER'    /* VOSTRO FOREIGN         */
     107  = 'OTHER'    /* PBCS                   */
     126  = 'OTHER'    /* NORMAL BANKERS CHEQUE  */
     127  = 'OTHER'    /* MIDF BANKERS CHEQUE    */
     128  = 'OTHER'    /* MIH BANKERS CHEQUE     */
     130  = 'OTHER'    /* USD C/A                */
     131  = 'OTHER'    /* GBP C/A                */
     132  = 'OTHER'    /* AUD C/A                */
     133  = 'OTHER'    /* SGD C/A                */
     134  = 'OTHER'    /* JPY C/A                */
     135  = 'OTHER'    /* DEM C/A                */
     136  = 'OTHER'    /* HKD C/A                */
     140  = 'OTHER'    /* SI-CLEARING ACCOUNT    */
     141  = 'OTHER'    /* ATM-PISA CLEARING ACCT */
     142  = 'OTHER'    /* ATM-LOAN CLEARING ACCT */
     143  = 'OTHER'    /* ATM-FD CLEARING ACCT   */
     144  = 'OTHER'    /* DD SHARE PUBLIC        */
     145  = 'OTHER'    /* DD SHARE BUMI          */
     146  = 'OTHER'    /* IBT COLLECTION ACCT    */
     147  = 'OTHER'    /* HO INTERBANK SETTLEMENT A/C  */
     148  = 'OTHER'    /* INTERBANK GIRO & MEPS CASH COLL) */
     149  = 'OTHER'    /* BNM BONDS COLLECTION ACCOUNT */
     171  = 'OTHER'    /* MYR OVERNIGHT A/C      */
     172  = 'OTHER'    /* MYR OVERNIGHT A/C      */
     173  = 'OTHER'    /* MYR OVERNIGHT A/C      */
     OTHER = 'OD   ' ;

   VALUE LNPROD
     4, 5     = 'HL   ' /* STAFF LOAN - HOUSING LOAN           */
     100, 101 = 'HL   ' /* ABBA HOUSING LOAN                   */
     200, 201 = 'HL   ' /* PRIORITY SECTOR HOUSING LOAN        */
     204, 205 = 'HL   ' /* NORMAL HOUSING LOAN                 */
     209, 210 = 'HL   ' /* HOS SPECIAL (<25K(WM) & 32K(EM))    */
     211, 212 = 'HL   ' /* HOS SPECIAL (25K-100K)              */
     214, 215 = 'HL   ' /* HOS EXTRA (RM100,001-RM200,000)     */
     219, 220 = 'HL   ' /* HOME REFINANCING SCHEME             */
     225, 226 = 'HLCAG' /* NORMAL HOUSING LOAN SOLD TO CAGAMAS */
     OTHER = 'FL   ';

  VALUE SADPRG
       LOW -       5 = '01)RM        1.00 - RM         5.00'
         5 -      10 = '02)RM        5.01 - RM        10.00'
        10 -      50 = '03)RM       10.01 - RM        50.00'
        50 -     100 = '04)RM       50.01 - RM       100.00'
       100 -     500 = '05)RM      100.01 - RM       500.00'
       500 -    1000 = '06)RM      500.01 - RM     1,000.00'
      1000 -    1500 = '07)RM    1,000.01 - RM     1,500.00'
      1500 -    2000 = '08)RM    1,500.01 - RM     2,000.00'
      2000 -    2500 = '09)RM    2,000.01 - RM     2,500.00'
      2500 -    3000 = '10)RM    2,500.01 - RM     3,000.00'
      3000 -    3500 = '11)RM    3,000.01 - RM     3,500.00'
      3500 -    4000 = '12)RM    3,500.01 - RM     4,000.00'
      4000 -    4500 = '13)RM    4,000.01 - RM     4,500.00'
      4500 -    5000 = '14)RM    4,500.01 - RM     5,000.00'
      5000 -    6000 = '15)RM    5,000.01 - RM     6,000.00'
      6000 -    7000 = '16)RM    6,000.01 - RM     7,000.00'
      7000 -    8000 = '17)RM    7,000.01 - RM     8,000.00'
      8000 -    9000 = '18)RM    8,000.01 - RM     9,000.00'
      9000 -   10000 = '19)RM    9,000.01 - RM    10,000.00'
     10000 -   15000 = '20)RM   10,000.01 - RM    15,000.00'
     15000 -   20000 = '21)RM   15,000.01 - RM    20,000.00'
     20000 -   25000 = '22)RM   20,000.01 - RM    25,000.00'
     25000 -   30000 = '23)RM   25,000.01 - RM    30,000.00'
     30000 -   35000 = '24)RM   30,000.01 - RM    35,000.00'
     35000 -   40000 = '25)RM   35,000.01 - RM    40,000.00'
     40000 -   45000 = '26)RM   40,000.01 - RM    45,000.00'
     45000 -   50000 = '27)RM   45,000.01 - RM    50,000.00'
     50000 -   55000 = '28)RM   50,000.01 - RM    55,000.00'
     55000 -   60000 = '29)RM   55,000.01 - RM    60,000.00'
     60000 -   65000 = '30)RM   60,000.01 - RM    65,000.00'
     65000 -   70000 = '31)RM   65,000.01 - RM    70,000.00'
     70000 -   75000 = '32)RM   70,000.01 - RM    75,000.00'
     75000 -   80000 = '33)RM   75,000.01 - RM    80,000.00'
     80000 -   85000 = '34)RM   80,000.01 - RM    85,000.00'
     85000 -   90000 = '35)RM   85,000.01 - RM    90,000.00'
     90000 -   95000 = '36)RM   90,000.01 - RM    95,000.00'
     95000 -  100000 = '37)RM   95,000.01 - RM   100,000.00'
    100000 -  150000 = '38)RM  100,000.01 - RM   150,000.00'
    150000 -  200000 = '39)RM  150,000.01 - RM   200,000.00'
    200000 -  300000 = '40)RM  200,000.01 - RM   300,000.00'
    300000 -  500000 = '41)RM  300,000.01 - RM   500,000.00'
    500000 - 1000000 = '42)RM  500,000.01 - RM 1,000,000.00'
   1000000 - 2000000 = '43)RM1,000,000.01 - RM 2,000,000.00'
   2000000 - 3000000 = '44)RM2,000,000.01 - RM 3,000,000.00'
   3000000 - 4000000 = '45)RM3,000,000.01 - RM 4,000,000.00'
   4000000 - 5000000 = '46)RM4,000,000.01 - RM 5,000,000.00'
   5000000 -10000000 = '47)RM5,000,000.01 - RM10,000,000.00'
  10000000 - HIGH    = '48)          ABOVE RM 10,000,000.00';

  VALUE SA1PRG
       LOW -       5 = '01)RM        1.00 - RM         5.00'
         5 -      10 = '02)RM        5.01 - RM        10.00'
        10 -      50 = '03)RM       10.01 - RM        50.00'
        50 -     100 = '04)RM       50.01 - RM       100.00'
       100 -     500 = '05)RM      100.01 - RM       500.00'
       500 -    1000 = '06)RM      500.01 - RM     1,000.00'
      1000 -    1500 = '07)RM    1,000.01 - RM     1,500.00'
      1500 -    2000 = '08)RM    1,500.01 - RM     2,000.00'
      2000 -    2500 = '09)RM    2,000.01 - RM     2,500.00'
      2500 -    3000 = '10)RM    2,500.01 - RM     3,000.00'
      3000 -    3500 = '11)RM    3,000.01 - RM     3,500.00'
      3500 -    4000 = '12)RM    3,500.01 - RM     4,000.00'
      4000 -    4500 = '13)RM    4,000.01 - RM     4,500.00'
      4500 -    5000 = '14)RM    4,500.01 - RM     5,000.00'
      5000 -    6000 = '15)RM    5,000.01 - RM     6,000.00'
      6000 -    7000 = '16)RM    6,000.01 - RM     7,000.00'
      7000 -    8000 = '17)RM    7,000.01 - RM     8,000.00'
      8000 -    9000 = '18)RM    8,000.01 - RM     9,000.00'
      9000 -   10000 = '19)RM    9,000.01 - RM    10,000.00'
     10000 -   15000 = '20)RM   10,000.01 - RM    15,000.00'
     15000 -   20000 = '21)RM   15,000.01 - RM    20,000.00'
     20000 -   25000 = '22)RM   20,000.01 - RM    25,000.00'
     25000 -   30000 = '23)RM   25,000.01 - RM    30,000.00'
     30000 -   35000 = '24)RM   30,000.01 - RM    35,000.00'
     35000 -   40000 = '25)RM   35,000.01 - RM    40,000.00'
     40000 -   45000 = '26)RM   40,000.01 - RM    45,000.00'
     45000 -   50000 = '27)RM   45,000.01 - RM    50,000.00'
     50000 -   55000 = '28)RM   50,000.01 - RM    55,000.00'
     55000 -   60000 = '29)RM   55,000.01 - RM    60,000.00'
     60000 -   65000 = '30)RM   60,000.01 - RM    65,000.00'
     65000 -   70000 = '31)RM   65,000.01 - RM    70,000.00'
     70000 -   75000 = '32)RM   70,000.01 - RM    75,000.00'
     75000 -   80000 = '33)RM   75,000.01 - RM    80,000.00'
     80000 -   85000 = '34)RM   80,000.01 - RM    85,000.00'
     85000 -   90000 = '35)RM   85,000.01 - RM    90,000.00'
     90000 -   95000 = '36)RM   90,000.01 - RM    95,000.00'
     95000 -  100000 = '37)RM   95,000.01 - RM   100,000.00'
    100000 -  150000 = '38)RM  100,000.01 - RM   150,000.00'
    150000 -  200000 = '39)RM  150,000.01 - RM   200,000.00'
    200000 -  300000 = '40)RM  200,000.01 - RM   300,000.00'
    300000 -  350000 = '41)RM  300,000.01 - RM   350,000.00'
    350000 -  360000 = '42)RM  350,000.01 - RM   360,000.00'
    360000 -  500000 = '43)RM  360,000.01 - RM   500,000.00'
    500000 -  510000 = '44)RM  500,000.01 - RM   510,000.00'
    510000 -  550000 = '45)RM  510,000.01 - RM   550,000.00'
    550000 -  850000 = '46)RM  550,000.01 - RM   850,000.00'
    850000 -  860000 = '47)RM  850,000.01 - RM   860,000.00'
    860000 - 1000000 = '48)RM  860,000.01 - RM 1,000,000.00'
   1000000 - 2000000 = '49)RM1,000,000.01 - RM 2,000,000.00'
   2000000 - 3000000 = '50)RM2,000,000.01 - RM 3,000,000.00'
   3000000 - 4000000 = '51)RM3,000,000.01 - RM 4,000,000.00'
   4000000 - 5000000 = '52)RM4,000,000.01 - RM 5,000,000.00'
   5000000 -10000000 = '53)RM5,000,000.01 - RM10,000,000.00'
  10000000 - HIGH    = '54)          ABOVE RM 10,000,000.00';

  VALUE SA2PRG
       LOW -       0 = 'ZERO BALANCE                             '
         0 -       5 = '01)RM              1.00 - RM         5.00'
         5 -      10 = '02)ABOVE RM        5.00 - RM        10.00'
        10 -      50 = '03)ABOVE RM       10.00 - RM        50.00'
        50 -     100 = '04)ABOVE RM       50.00 - RM       100.00'
       100 -     500 = '05)ABOVE RM      100.00 - RM       500.00'
       500 -    1000 = '06)ABOVE RM      500.00 - RM     1,000.00'
      1000 -    1500 = '07)ABOVE RM    1,000.00 - RM     1,500.00'
      1500 -    2000 = '08)ABOVE RM    1,500.00 - RM     2,000.00'
      2000 -    2500 = '09)ABOVE RM    2,000.00 - RM     2,500.00'
      2500 -    3000 = '10)ABOVE RM    2,500.00 - RM     3,000.00'
      3000 -    3500 = '11)ABOVE RM    3,000.00 - RM     3,500.00'
      3500 -    4000 = '12)ABOVE RM    3,500.00 - RM     4,000.00'
      4000 -    4500 = '13)ABOVE RM    4,000.00 - RM     4,500.00'
      4500 -    5000 = '14)ABOVE RM    4,500.00 - RM     5,000.00'
      5000 -    6000 = '15)ABOVE RM    5,000.00 - RM     6,000.00'
      6000 -    7000 = '16)ABOVE RM    6,000.00 - RM     7,000.00'
      7000 -    8000 = '17)ABOVE RM    7,000.00 - RM     8,000.00'
      8000 -    9000 = '18)ABOVE RM    8,000.00 - RM     9,000.00'
      9000 -   10000 = '19)ABOVE RM    9,000.00 - RM    10,000.00'
     10000 -   15000 = '20)ABOVE RM   10,000.00 - RM    15,000.00'
     15000 -   20000 = '21)ABOVE RM   15,000.00 - RM    20,000.00'
     20000 -   25000 = '22)ABOVE RM   20,000.00 - RM    25,000.00'
     25000 -   30000 = '23)ABOVE RM   25,000.00 - RM    30,000.00'
     30000 -   35000 = '24)ABOVE RM   30,000.00 - RM    35,000.00'
     35000 -   40000 = '25)ABOVE RM   35,000.00 - RM    40,000.00'
     40000 -   45000 = '26)ABOVE RM   40,000.00 - RM    45,000.00'
     45000 -   50000 = '27)ABOVE RM   45,000.00 - RM    50,000.00'
     50000 -   55000 = '28)ABOVE RM   50,000.00 - RM    55,000.00'
     55000 -   60000 = '29)ABOVE RM   55,000.00 - RM    60,000.00'
     60000 -   65000 = '30)ABOVE RM   60,000.00 - RM    65,000.00'
     65000 -   70000 = '31)ABOVE RM   65,000.00 - RM    70,000.00'
     70000 -   75000 = '32)ABOVE RM   70,000.00 - RM    75,000.00'
     75000 -   80000 = '33)ABOVE RM   75,000.00 - RM    80,000.00'
     80000 -   85000 = '34)ABOVE RM   80,000.00 - RM    85,000.00'
     85000 -   90000 = '35)ABOVE RM   85,000.00 - RM    90,000.00'
     90000 -   95000 = '36)ABOVE RM   90,000.00 - RM    95,000.00'
     95000 -  100000 = '37)ABOVE RM   95,000.00 - RM   100,000.00'
    100000 -  150000 = '38)ABOVE RM  100,000.00 - RM   150,000.00'
    150000 -  200000 = '39)ABOVE RM  150,000.00 - RM   200,000.00'
    200000 -  300000 = '40)ABOVE RM  200,000.00 - RM   300,000.00'
    300000 -  350000 = '41)ABOVE RM  300,000.00 - RM   350,000.00'
    350000 -  360000 = '42)ABOVE RM  350,000.00 - RM   360,000.00'
    360000 -  500000 = '43)ABOVE RM  360,000.00 - RM   500,000.00'
    500000 -  510000 = '44)ABOVE RM  500,000.00 - RM   510,000.00'
    510000 -  550000 = '45)ABOVE RM  510,000.00 - RM   550,000.00'
    550000 -  600000 = '46)ABOVE RM  550,000.00 - RM   600,000.00'
    600000 -  700000 = '47)ABOVE RM  600,000.00 - RM   700,000.00'
    700000 -  800000 = '48)ABOVE RM  700,000.00 - RM   800,000.00'
    800000 -  850000 = '49)ABOVE RM  800,000.00 - RM   850,000.00'
    850000 -  860000 = '50)ABOVE RM  850,000.00 - RM   860,000.00'
    860000 -  900000 = '51)ABOVE RM  860,000.00 - RM   900,000.00'
    900000 - 1000000 = '52)ABOVE RM  900,000.00 - RM 1,000,000.00'
   1000000 - 1100000 = '53)ABOVE RM1,000,000.00 - RM 1,100,000.00'
   1100000 - 1200000 = '54)ABOVE RM1,100,000.00 - RM 1,200,000.00'
   1200000 - 1300000 = '55)ABOVE RM1,200,000.00 - RM 1,300,000.00'
   1300000 - 1400000 = '56)ABOVE RM1,300,000.00 - RM 1,400,000.00'
   1400000 - 1500000 = '57)ABOVE RM1,400,000.00 - RM 1,500,000.00'
   1500000 - 1600000 = '58)ABOVE RM1,500,000.00 - RM 1,600,000.00'
   1600000 - 1700000 = '59)ABOVE RM1,600,000.00 - RM 1,700,000.00'
   1700000 - 1800000 = '60)ABOVE RM1,700,000.00 - RM 1,800,000.00'
   1800000 - 1900000 = '61)ABOVE RM1,800,000.00 - RM 1,900,000.00'
   1900000 - 2000000 = '62)ABOVE RM1,900,000.00 - RM 2,000,000.00'
   2000000 - 3000000 = '63)ABOVE RM2,000,000.00 - RM 3,000,000.00'
   3000000 - 4000000 = '64)ABOVE RM3,000,000.00 - RM 4,000,000.00'
   4000000 - 5000000 = '65)ABOVE RM4,000,000.00 - RM 5,000,000.00'
   5000000 -10000000 = '66)ABOVE RM5,000,000.00 - RM10,000,000.00'
  10000000 - HIGH    = '67)                ABOVE RM 10,000,000.00';

  VALUE SA3PRG
       LOW -<        0 = '01)NEGATIVE BALANCE                      '
                     0 = '02)ZERO BALANCE                          '
         0 <-        5 = '03)ABOVE RM        0.00 - RM         5.00'
         5 <-       10 = '04)ABOVE RM        5.00 - RM        10.00'
        10 <-       50 = '05)ABOVE RM       10.00 - RM        50.00'
        50 <-      100 = '06)ABOVE RM       50.00 - RM       100.00'
       100 <-      500 = '07)ABOVE RM      100.00 - RM       500.00'
       500 <-     1000 = '08)ABOVE RM      500.00 - RM     1,000.00'
      1000 <-     1500 = '09)ABOVE RM    1,000.00 - RM     1,500.00'
      1500 <-     2000 = '10)ABOVE RM    1,500.00 - RM     2,000.00'
      2000 <-     2500 = '11)ABOVE RM    2,000.00 - RM     2,500.00'
      2500 <-     3000 = '12)ABOVE RM    2,500.00 - RM     3,000.00'
      3000 <-     3500 = '13)ABOVE RM    3,000.00 - RM     3,500.00'
      3500 <-     4000 = '14)ABOVE RM    3,500.00 - RM     4,000.00'
      4000 <-     4500 = '15)ABOVE RM    4,000.00 - RM     4,500.00'
      4500 <-     5000 = '16)ABOVE RM    4,500.00 - RM     5,000.00'
      5000 <-     6000 = '17)ABOVE RM    5,000.00 - RM     6,000.00'
      6000 <-     7000 = '18)ABOVE RM    6,000.00 - RM     7,000.00'
      7000 <-     8000 = '19)ABOVE RM    7,000.00 - RM     8,000.00'
      8000 <-     9000 = '20)ABOVE RM    8,000.00 - RM     9,000.00'
      9000 <-    10000 = '21)ABOVE RM    9,000.00 - RM    10,000.00'
     10000 <-    15000 = '22)ABOVE RM   10,000.00 - RM    15,000.00'
     15000 <-    20000 = '23)ABOVE RM   15,000.00 - RM    20,000.00'
     20000 <-    25000 = '24)ABOVE RM   20,000.00 - RM    25,000.00'
     25000 <-    30000 = '25)ABOVE RM   25,000.00 - RM    30,000.00'
     30000 <-    35000 = '26)ABOVE RM   30,000.00 - RM    35,000.00'
     35000 <-    40000 = '27)ABOVE RM   35,000.00 - RM    40,000.00'
     40000 <-    45000 = '28)ABOVE RM   40,000.00 - RM    45,000.00'
     45000 <-    50000 = '29)ABOVE RM   45,000.00 - RM    50,000.00'
     50000 <-    55000 = '30)ABOVE RM   50,000.00 - RM    55,000.00'
     55000 <-    60000 = '31)ABOVE RM   55,000.00 - RM    60,000.00'
     60000 <-    65000 = '32)ABOVE RM   60,000.00 - RM    65,000.00'
     65000 <-    70000 = '33)ABOVE RM   65,000.00 - RM    70,000.00'
     70000 <-    75000 = '34)ABOVE RM   70,000.00 - RM    75,000.00'
     75000 <-    80000 = '35)ABOVE RM   75,000.00 - RM    80,000.00'
     80000 <-    85000 = '36)ABOVE RM   80,000.00 - RM    85,000.00'
     85000 <-    90000 = '37)ABOVE RM   85,000.00 - RM    90,000.00'
     90000 <-    95000 = '38)ABOVE RM   90,000.00 - RM    95,000.00'
     95000 <-   100000 = '39)ABOVE RM   95,000.00 - RM   100,000.00'
    100000 <-   150000 = '40)ABOVE RM  100,000.00 - RM   150,000.00'
    150000 <-   200000 = '41)ABOVE RM  150,000.00 - RM   200,000.00'
    200000 <-   300000 = '42)ABOVE RM  200,000.00 - RM   300,000.00'
    300000 <-   350000 = '43)ABOVE RM  300,000.00 - RM   350,000.00'
    350000 <-   360000 = '44)ABOVE RM  350,000.00 - RM   360,000.00'
    360000 <-   500000 = '45)ABOVE RM  360,000.00 - RM   500,000.00'
    500000 <-   510000 = '46)ABOVE RM  500,000.00 - RM   510,000.00'
    510000 <-   550000 = '47)ABOVE RM  510,000.00 - RM   550,000.00'
    550000 <-   850000 = '48)ABOVE RM  550,000.00 - RM   850,000.00'
    850000 <-   860000 = '49)ABOVE RM  850,000.00 - RM   860,000.00'
    860000 <-  1000000 = '50)ABOVE RM  860,000.00 - RM  1000,000.00'
   1000000 <-  2000000 = '51)ABOVE RM 1000,000.00 - RM  2000,000.00'
   2000000 <-  3000000 = '52)ABOVE RM 2000,000.00 - RM  3000,000.00'
   3000000 <-  4000000 = '53)ABOVE RM 3000,000.00 - RM  4000,000.00'
   4000000 <-  5000000 = '54)ABOVE RM 4000,000.00 - RM  5000,000.00'
   5000000 <- 10000000 = '55)ABOVE RM 5000,000.00 - RM10,000,000.00'
  10000000 <- HIGH     = '56)ABOVE RM 10,000,000.00                ';

  VALUE PROFNORM
       LOW -    5000 = '1)UP TO RM 5,000.00'
      5000 -   10000 = '2)UP TO RM10,000.00'
     10000 -   30000 = '3)UP TO RM30,000.00'
     30000 -   50000 = '4)UP TO RM50,000.00'
     50000 -   75000 = '5)UP TO RM75,000.00'
     75000 -   HIGH  = '6)ABOVE RM75,000.00';

  VALUE SEXNORM
       LOW -   12    = '1)BELOW 12 YRS'
       12  -   18    = '2)12 - 18 YRS'
       18  -   50    = '3)18 - 50 YRS'
       50  -   HIGH  = '4)50 YRS AND ABOVE';

  VALUE PROFYAA
       LOW -     500 = '1)BELOW RM   500.00'
       500 -    2000 = '2)UP TO RM 2,000.00'
      2000 -    5000 = '3)UP TO RM 5,000.00'
      5000 -   10000 = '4)UP TO RM10,000.00'
     10000 -   30000 = '5)UP TO RM30,000.00'
     30000 -   50000 = '6)UP TO RM50,000.00'
     50000 -   75000 = '7)UP TO RM75,000.00'
     75000 -   HIGH  = '8)ABOVE RM75,000.00';

  VALUE SEXYW
       LOW -   12    = '1)BELOW 12 YRS'
       12  -   18    = '2)12 - 18 YRS';

  VALUE PROFPLUS
       LOW -    2000 = '1)UP TO RM 2,000.00'
      2000 -    5000 = '1)UP TO RM 5,000.00'
      5000 -   10000 = '2)UP TO RM10,000.00'
     10000 -   30000 = '3)UP TO RM30,000.00'
     30000 -   50000 = '4)UP TO RM50,000.00'
     50000 -   75000 = '5)UP TO RM75,000.00'
     75000 -   HIGH  = '6)ABOVE RM75,000.00';

  VALUE PROFWISE
       LOW -    5000 = '1)UP TO RM 5,000.00'
      5000 -   10000 = '2)UP TO RM10,000.00'
     10000 -   20000 = '3)UP TO RM20,000.00'
     20000 -   30000 = '4)UP TO RM30,000.00'
     30000 -   50000 = '5)UP TO RM50,000.00'
     50000 -   HIGH  = '6)ABOVE RM50,000.00';

  VALUE SDNAME
   200  = 'PLUS SAVING'
   201  = 'STAFF'
   202  = 'YOUNG ACHIEVER'
   203  = '50 PLUS'
   204  = 'AL-WADIAH'
   205  = 'BASIC SAVING'
   206  = 'BASIC 55 SAVING'
   208  = 'PB BRIGHT STAR SA'
   210  = 'PB MYSALARY SA'
   212  = 'WISE'
   213  = 'PB SAVELINK'
   214  = 'BESTARI SAVING'
   215  = 'STAFF WADIAH'
   216  = 'PB UNIONPAY SA'
   227  = 'STAFF MONEYPLUS SA'
   228  = 'MONEYPLUS SA'
   480  = 'GIA'
   481  = 'STAFF GIA';

  VALUE $RACE
   '0'  = 'OTHERS'
   '1'  = 'MALAY'
   '2'  = 'CHINESE'
   '3'  = 'INDIAN'
   OTHER = 'OTHERS';

   VALUE CADPRG
     1000 = ' 1)         RM0 -     RM1,000'
     2000 = ' 2)     RM1,001 -     RM2,000'
     2500 = ' 3)     RM2,001 -     RM2,500'
   3000,5000 = ' 4)     RM2,501 -     RM5,000'
    10000 = ' 5)     RM5,001 -    RM10,000'
    20000 = ' 6)    RM10,001 -    RM20,000'
    30000 = ' 7)    RM20,001 -    RM30,000'
    40000 = ' 8)    RM30,001 -    RM40,000'
    50000 = ' 9)    RM40,001 -    RM50,000'
  75000,100000 = '10)   RM50,001 -   RM100,000'
   150000 = '11)  RM100,001 -   RM150,000'
   200000 = '12)  RM150,001 -   RM200,000'
   250000 = '13)  RM200,001 -   RM250,000'
   500000 = '14)  RM250,001 -   RM500,000'
  1000000 = '15)  RM500,001 - RM1,000,000'
  2000000 = '16)RM1,000,001 - RM2,000,000'
  3000000 = '17)RM2,000,001 - RM3,000,000'
  4000000 = '18)RM3,000,001 - RM4,000,000'
  5000000 = '19)RM4,000,001 - RM5,000,000'
  OTHER   = '20)RM5,000,001 AND ABOVE';

   VALUE ICADPRG
     1000,2000  = ' 1)     BELOW  RM2,000'
     2500,3000  = ' 2) RM2,000 -  RM3,000'
     5000       = ' 3) RM3,000 -  RM5,000'
    10000       = ' 4) RM5,000 -  RM10,000'
    20000,30000 = ' 5) RM10,000 - RM30,000'
    40000,50000 = ' 6) RM30,000 - RM50,000'
    75000       = ' 7) RM50,000 - RM75,000'
    100000      = ' 8) RM75,000 - RM100,000'
    150000      = ' 9) RM100,000 -  RM150,000'
    200000      = '10) RM150,000 -  RM200,000'
    OTHER       = '11) RM200,000 AND ABOVE';


   VALUE $CPARTYF
    '01' = '01'      /* BANK NEGARA MALAYSIA                     */
    '02' = '10'      /* COMMERCIAL BANKS                         */
    '03' = '10'      /* ISLAMIC BANKS                            */
    '04' = '20'      /* SUBSIDIARY STOCKBROKING COMPANIES        */
    '05' = '20'      /* ASSOCIATE STOCKBROKING COMPANIES         */
    '06' = '20'      /* OTHER STOCKBROKING COMPANIES             */
    '10' = '10'      /* DOMESTIC BANKING INSTITUTIONS            */
    '11' = '10'      /* FINANCE COMPANIES                        */
    '12' = '10'      /* MERCHANT BANKS                           */
    '13' = '20'      /* DISCOUNT HOUSES                          */
    '15' = '20'      /* DOMESTIC NON-BANK ENTITIES               */
    '17' = '20'      /* CAGAMAS BERHAD                           */
    '20' = '20'      /* DOMESTIC NON-BANK FINANCIAL INSTITUTIONS */
    '30' = '20'      /* DOMESTIC OTHER NBFI                      */
    '32' = '20'      /* CREDIT CARD COMPANIES                    */
    '33' = '20'      /* DEVELOPMENT FINANCE INSTITUTIONS         */
    '34' = '20'      /* BUILDING SOCIETIES                       */
    '35' = '20'      /* CO-OPERATIVE SOCIETIES                   */
    '36' = '20'      /* STOCKBROKING COMPANIES                   */
    '37' = '20'      /* COMMODITY BROKERS                        */
    '38' = '20'      /* CREDIT & LEASING COMPANIES               */
    '39' = '20'      /* UNIT TRUST COMPANIES                     */
    '40' = '20'      /* INSURANCE AND INSURANCE RELATED COMPANIES*/
    '50' = '60'      /* RESIDENTS/DOMESTIC ENTITIES              */
    '57' = '60'      /* PETRONAS                                 */
    '59' = '60'      /* OTHER GOVERNMENT CONTROLLED DBE NIE      */
    '60' = '60'      /* DOMESTIC BUSINESS ENTERPRISES (DBE)      */
    '61' = '60'      /* BUMIPUTRA CONTROLLED DBE                 */
    '62' = '60'      /* NON-BUMIPUTRA CONTROLLED DBE             */
    '63' = '60'      /* NON-RESIDENT CONTROLLED DBE              */
    '64' = '60'      /* GOVERNMENT CONTROLLED DBE                */
    '65' = '60'      /* SMALL MEDIUM INDUSTRIES (SMI)            */
    '66' = '60'      /* BUMIPUTRA CONTROLLED SMI                 */
    '67' = '60'      /* NON-BUMIPUTRA CONTROLLED SMI             */
    '68' = '60'      /* NON-RESIDENT CONTROLLED SMI              */
    '69' = '60'      /* GOVERNMENT CONTROLLED SMI                */
    '70' = '70'      /* GOVERNMENT                               */
    '71' = '70'      /* FEDERAL GOVERNMENT                       */
    '72' = '70'      /* STATE GOVERNMENT                         */
    '73' = '70'      /* LOCAL GOVERNMENT                         */
    '74' = '70'      /* STATUTORY AUTHORITIES                    */
    '75' = '60'      /* NFPE                                     */
    '76' = '76'      /* INDIVIDUALS                              */
    '77' = '76'      /* BUMIPUTRA                                */
    '78' = '76'      /* NON-BUMIPUTRA                            */
    '79' = '79'      /* DOMESTIC OTHER ENTITIES NIE              */
    '80' = '80'      /* NON-RESIDENTS/FOREIGN ENTITIES           */
    '81' = '80'      /* FOREIGN BANKING INSTITUTIONS             */
    '85' = '80'      /* FOREIGN NON-BANK ENTITIES                */
    '86' = '80'      /* FOREIGN BUSINESS ENTERPRISES             */
    '90' = '80'      /* FOREIGN GOVERNMENTS                      */
    '91' = '80'      /* FOREIGN CENTRAL BANKS                    */
    '92' = '80'      /* FRGN DIPLOMATIC REPRESENTATION IN M'SIA  */
    '95' = '80'      /* FOREIGN INDIVIDUALS                      */
    '96' = '80'      /* FOREIGNERS EMPLOYED/STUDYING IN M'SIA    */
    '98' = '80'      /* FOREIGN NON-COMMERCIAL INTERNATIONAL     */
                     /* ORGANIZATION IN M'SIA                    */
    '99' = '80'      /* FOREIGN OTHER ENTITIES NIE               */
     OTHER = '76' ;

   VALUE $CPARTY
     '01' = 'BANK NEGARA MALAYSIA'
     '10' = 'DOMESTIC BANKING INSTITUTION'
     '20' = 'DOMESTIC NON-BANK FI'
     '60' = 'DOMESTIC BUSINESS ENTERPRISES'
     '70' = 'GOVERNMENT'
     '76' = 'INDIVIDUALS'
     '79' = 'DOMESTIC OTHER ENTITIES NIE'
     '80' = 'NON RESIDENTS/FOREIGN ENTITIES';

   VALUE $PURPOSE
     '1'  = 'PERSONAL'
     '2'  = 'JOINT'
     '4'  = 'PERSONAL'  /* IS A STAFF A/C BUT STATES AS PERSONAL  */
     OTHER= 'OTHERS  ';

   VALUE CARANGED
     2000  = ' 1)         BELOW RM2,000'
     3000  = ' 2)     RM2,000 - RM3,000'
     5000  = ' 3)     RM3,000 - RM5,000'
    10000  = ' 4)    RM5,000 - RM10,000'
    30000  = ' 5)   RM10,000 - RM30,000'
    50000  = ' 6)   RM30,000 - RM50,000'
    75000  = ' 7)   RM50,000 - RM75,000'
   100000  = ' 8)  RM75,000 - RM100,000'
   150000  = ' 9) RM100,000 - RM150,000'
   200000  = '10) RM150,000 - RM200,000'
   200001  = '11) RM200,000 AND ABOVE  ';

   VALUE PROD
     150 = 'ACE ACCOUNT'
     160 = 'AL-WADIAH CURRENT A/C';

   VALUE AGEDESC
     0         = 'WITHOUT BIRTHDATE'
     &AGEBELOW = 'BELOW 12 YEARS'
     &AGELIMIT = '12 TO BELOW 18 YEARS'
     &MAXAGE   = '18 AND ABOVE';

   VALUE $STATE
     'A' = 'PERAK'
     'B' = 'SELANGOR'
     'C' = 'PAHANG'
     'D' = 'KELANTAN'
     'J' = 'JOHOR'
     'K' = 'KEDAH'
     'L' = 'LABUAN'
     'M' = 'MELAKA'
     'N' = 'NEGERI SEMBILAN'
     'P' = 'PULAU PINANG'
     'Q' = 'SARAWAK'
     'R' = 'PERLIS'
     'S' = 'SABAH'
     'T' = 'TERENGGANU'
     'W' = 'WILAYAH PERSEKUTUAN';

   VALUE FDPROD
    340 = '1 MONTH'
    341 = '3 MONTHS'
    342 = '6 MONTHS'
    343 = '9 MONTHS'
    344 = '12 MONTHS'
    345 = '15 MONTHS'
    346 = '18 MONTHS'
    347 = '21 MONTHS'
    348 = '24 MONTHS'
    349 = '36 MONTHS'
    350 = '48 MONTHS'
    351 = '60 MONTHS'
    OTHER = ' ';

   INVALUE IBWRNGE
      LOW - 10000  = 10000
    10000 - 30000  = 30000
    30000 - 50000  = 50000
    50000 - 75000  = 75000
    75000 - 100000 = 100000
   100000 - HIGH   = 100001;

   INVALUE CARANGE
      LOW -  2000  = 2000
     2000 - 3000   = 3000
     3000 - 5000   = 5000
     5000 - 10000  = 10000
    10000 - 30000  = 30000
    30000 - 50000  = 50000
    50000 - 75000  = 75000
    75000 - 100000 = 100000
   100000 - 150000 = 150000
   150000 - 200000 = 200000
   200000 - HIGH   = 200001;

   INVALUE ISARANGE
      LOW - 1000   = 1000
     1000 - 5000   = 5000
     5000 - 25000  = 25000
    25000 - 50000  = 50000
    50000 - HIGH   = 50001;

   VALUE ISARANGD
     1000  = ' 1)       BELOW RM1,000'
     5000  = ' 2)   RM1,000 - RM5,000'
    25000  = ' 3)  RM5,000 - RM25,000'
    50000  = ' 4) RM25,000 - RM50,000'
    50001  = ' 5) RM50,000 AND ABOVE ';

   VALUE IWSRNGE
       LOW  -<  3001 =' 1) UP TO   RM  3,000'
       3001 -< 10001 =' 2) UP TO   RM 10,000'
      10001 -< 30001 =' 3) UP TO   RM 30,000'
      30001 -< 50001 =' 4) UP TO   RM 50,000'
      50001 -< 75001 =' 5) UP TO   RM 75,000'
      75001 -<100001 =' 6) UP TO   RM100,000'
     100001 - HIGH   =' 7) ABOVE RM100,000  ';

   VALUE IWSRNGX
       LOW  -   5000 =' 1) FIRST RM  5,000'
       5000 -  10000 =' 2) NEXT  RM  5,000'
      10000 -  50000 =' 3) NEXT  RM 40,000'
      50000 - 150000 =' 4) NEXT  RM100,000'
     150000 - 350000 =' 5) NEXT  RM200,000'
     350000 - 650000 =' 6) NEXT  RM300,000'
     650000 -1200000 =' 6) NEXT  RM550,000'
    1200000 - HIGH   =' 7) THEREAFTER     ';

   VALUE IBWSRNGD
       LOW  -< 10001 =' 1) UP TO   RM 10,000'
      10001 -< 30001 =' 2) UP TO   RM 30,000'
      30001 -< 50001 =' 3) UP TO   RM 50,000'
      50001 -< 75001 =' 4) UP TO   RM 75,000'
      75001 -<100001 =' 5) UP TO   RM100,000'
     100001 - HIGH   =' 6) ABOVE RM100,000  ';

   PICTURE HUNDRED
     LOW - <0  = '0,000,009.99' (PREFIX='-' MULT=1)
     0-HIGH    = '0,000,009.99' (MULT=1);

   PICTURE THOUSAND
     LOW - <0 = '0,000,000,009' (PREFIX='-' MULT=.001)
     0-HIGH   = '0,000,000,009' (MULT=.001);

   PICTURE MILLION
     LOW - <0 = '0,000,000,009' (PREFIX='-' MULT=.000001)
     0-HIGH   = '0,000,000,009' (MULT=.000001);

   VALUE LNPOGRP

     002,003,022,046,053,056,120,129,136,169,
     170,173,196,226,232,252,262,802,811,818,284,
     285,291
                         ='01-SELANGOR/WILAYAH REG.   I'

     015,029,040,041,066,083,096,097,101,103,118,128,
     141,151,195,197,248,269,701,812,821,822
                         ='02-SELANGOR/WILAYAH REG.  II'

     018,019,035,036,081,124,125,131,135,145,148,
     162,167,180,220,241,267,280,292,295,815,816
                         ='03-SELANGOR/WILAYAH REG. III'

     007,037,052,059,061,079,087,089,091,093,102,105,110,
     144,147,174,176,216,217,222,234,286,287,290,804,805
                         ='04-SOUTHERN REGION I       '

     005,009,049,051,067,071,076,080,085,095,123,137,146,
     152,158,207,208,209,210,244,245,251,809,823
                         ='05-CENTRAL REGION           '

     032,050,058,090,130,175,183,184,185,186,189,190,
     191,192,193,194,259,813,273,274,275,281
                         ='06-SARAWAK REGION           '

     033,044,055,062,072,112,115,140,142,143,149,161,228,
     803,278,276,282,283
                         ='07-SABAH REGION             '

     004,016,017,021,024,028,039,045,047,063,064,065,075,
     111,156,160,165,172,224,231,242,247,254,800,807
                         ='08-SOUTHERN REGION II       '

     006,010,034,054,070,074,077,086,104,107,114,126,
     150,159,171,205,238,258,265,266,806,808,817,704
                         ='09-NORTHERN REGION I        '

     008,013,014,030,048,106,113,116,117,139,233,237,239,
     257,260,261,263,264,819,277,703
                         ='10-EAST COAST REGION        '

     026,038,094,122,138,153,155,157,163,168,178,
     179,198,225,230,270,296,814,820,279,288,289,702
                         ='11-SELANGOR/WILAYAH REG.  IV'

     023,027,042,057,060,068,088,108,121,154,164,177,204,
     206,211,249,256,801,824,011,243
                         ='12-NORTHERN REGION II       '

     020,025,031,043,069,073,078,092,109,127,133,199,201,
     202,203,221,235,240,268,293,294
                         ='13-SELANGOR/WILAYAH REG.   V'
     OTHER               ='99-OTHER';

