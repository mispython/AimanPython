*+--------------------------------------------------------------+
 |  PROGRAM : KALMLIQ                                           |
 |  DATE    : 22.07.98                                          |
 |  REPORT  : NEW LIQUIDITY FRAMEWORK (KAPITI ITEMS)            |
 +--------------------------------------------------------------+
 |  DATE MODIFIED : 07-02-2002 (WBL)                            |
 |  SMR/OTHERS    : JS                                          |
 |  CHANGES MADE  : INCLUDE NEW MARKETABLE SECURITIES PRODUCT : |
 |                  'PNB' (9363600XX0000Y, 9563600XX0000Y)      |
 +--------------------------------------------------------------+;
*;
*----------------------------------------------------------------*
*  BREAKDOWN BY PURE CONTRACTUAL MATURITY PROFILE (PART 2)       *
*----------------------------------------------------------------*;
DATA K1TBL (KEEP=PART ITEM MATDT AMOUNT AMTUSD AMTSGD ISSDT GWCCY
                 GWSHN GWC2R GWDLP GWDLR);
   SET BNMK.K1TBL&REPTMON&NOWK (RENAME=(GWMDT=MATDT GWBALC=AMOUNT
                                        GWSDT=ISSDT));
   IF GWMVT = 'P';
   IF GWOCY='XAU' THEN DELETE;
   IF GWCCY='XAU' THEN DELETE;
   IF GWOCY='XAT' THEN DELETE;
   IF GWCCY='XAT' THEN DELETE;
   IF GWCCY  = 'MYR' THEN DO;
      PART = '95';
      AMTUSD = 0;
      AMTSGD = 0;
      IF GWMVTS = 'M' THEN DO;
         IF GWDLP IN ('BCD','BCI','BCS','BCQ','BCT','BCW','BQD')
         THEN DO;
            ITEM = '830'; OUTPUT;
         END;
         IF SUBSTR(GWCTP,1,1) = 'B' THEN
            SELECT (GWDLP);
               WHEN ('LO','LC','LF','LS','LOI','LSI','LSC','LSW',
                     'FDA','FDB','FDS','FDL','LOC','LOW') DO;
                  ITEM = '610'; OUTPUT;
               END;
               WHEN ('BO','BF','BOI','BFI','BSC','BSW','BOC','BOW') DO;
                  ITEM = '810'; OUTPUT;
               END;
               OTHERWISE;
            END;
         SELECT (SUBSTR(GWDLP,2,2));
            WHEN ('MI','MT') DO;
               ITEM = '820';
               OUTPUT;
            END;
            WHEN ('XI','XT') DO;
               ITEM = '620'; OUTPUT;
            END;
            OTHERWISE;
         END;
      END;
      /*
      ELSE IF GWDLP IN ('FXS','FXO','FXF','TS1','TS2','SF1','SF2',
         'FF1','FF2') THEN DO;
         IF GWMVTS = 'P' THEN ITEM = '711';
         ELSE IF GWMVTS = 'S' THEN ITEM = '911';
         OUTPUT;
      END;
      */
   END;
*;
   ELSE DO;
      PART = '96';
      IF GWCCY = 'USD' THEN AMTUSD = AMOUNT;
      ELSE AMTUSD = 0;
      IF GWCCY = 'SGD' THEN AMTSGD = AMOUNT;
      ELSE AMTSGD = 0;
      IF GWMVTS = 'M' THEN DO;
         IF SUBSTR(GWCTP,1,1) = 'B' AND GWCTP ^= 'BW' THEN
            SELECT (GWDLP);
               WHEN ('LO','LC','LS','LF','LOI','LSI','LSC','LOC',
                    'FDA','FDB','FDS','FDL','LOW','LSW') DO;
                  ITEM = '610'; OUTPUT;
               END;
               WHEN ('BC','BF','BO','BSC','BOW','BSW') DO;
                  IF SUBSTR(GWSHN,1,6) ^= 'FCY-FD' THEN DO;
                     ITEM = '810'; OUTPUT;
                  END;
               END;
               WHEN ('BOC') DO;
                     ITEM = '810'; OUTPUT;
                  END;
               OTHERWISE;
            END;
      END;
      /*
      ELSE IF GWDLP IN ('FXS','FXO','FXF','TS1','TS2','SF1','SF2',
         'FF1','FF2') AND GWACT NOT IN ('RV','RW') THEN DO;
         IF GWMVTS = 'P' THEN ITEM = '711';
         ELSE IF GWMVTS = 'S' THEN ITEM = '911';
         OUTPUT;
      END;
      */
   END;
*;
*;
%INC PGM(KAMLIQX);
*;
DATA K3TBL (KEEP=PART ITEM MATDT AMOUNT AMTUSD AMTSGD ISSDT UTCCY
                 UTCUS UTCTP UTSTY UTDLR UTDLP);
   RETAIN PART '95';
   SET BNMK.K3TBL&REPTMON&NOWK;
   AMOUNT = UTAMOC - UTDPF;
   IF UTSTY='IDC' THEN AMOUNT=UTAMOC + UTDPF;
   IF &INST='PBB' THEN DO;
      IF UTCCY = 'USD' THEN AMTUSD = AMOUNT;
      ELSE AMTUSD = 0;
      IF UTCCY = 'SGD' THEN AMTSGD = AMOUNT;
      ELSE AMTSGD = 0;
   END;
   ELSE DO;
      AMTUSD = 0;
      AMTSGD = 0;
   END;
*  IF UTREF IN ('INV','TRD','TAP') THEN DO;
   IF UTREF IN ('INV','DRI','DLG','AFSLIQ','AFSBOND','IAFSLIQ','AFS',
                'IAFS') THEN DO;
      SELECT (UTSTY);
         WHEN ('CB1','CB2','CF1','CF2','CNT','MGS','MTB','BNB','BNN',
               'ITB','SAC','BMN','BMC','BMF','SCD','SCM',
               'CMB','MGI','SMC') DO;
            ITEM = '631';
            IF &INST='PBB' THEN DO;
               AMOUNT = AMOUNT + UTAICT;
            END;
            OUTPUT;
         END;

         WHEN ('SDC') DO;
            ITEM = '632';
            IF &INST='PBB' THEN DO;
               AMOUNT = (UTAMOC*(UTPCP/100))+UTDPEY+UTDPE;
            END;
            OUTPUT;
         END;
         WHEN ('LDC') DO;
            ITEM = '632';
            IF &INST='PBB' THEN DO;
               AMOUNT = AMOUNT + UTAICT;
            END;
            OUTPUT;
         END;

         WHEN ('SLD','SSD') DO;
            ITEM = '632';
            IF &INST='PBB' THEN DO;
               AMOUNT = (UTAMOC*(UTPCP/100))+UTAICY+UTAIT;
            END;
            OUTPUT;
         END;

         WHEN ('SFD','SZD') DO;
            ITEM = '632';
            IF &INST='PBB' THEN DO;
               AMOUNT = AMOUNT + UTAICT;
            END;
            OUTPUT;
         END;

         WHEN ('SBA') DO;
            IF UTDLP NOT IN ('MOS','MSS') THEN DO;
               ITEM = '633'; OUTPUT;
            END;
         END;
         WHEN ('ISB','DHB','KHA','PNB') DO;
            ITEM = '636'; OUTPUT;
         END;
         WHEN ('IDS') DO;
            ITEM = '635'; OUTPUT;
         END;
         WHEN ('DBD') DO;
            ITEM = '634'; OUTPUT;
         END;
         WHEN ('DMB','DBD','GRL','MTL','RUL') DO;
            ITEM = '635'; OUTPUT;
         END;
         WHEN ('PBA') DO;
            IF UTDLP IN ('MOS','MSS') THEN DO;
               ITEM = '850'; OUTPUT;
            END;
         END;
         OTHERWISE;
      END;
   END;
   ELSE IF UTREF IN ('PFD','PLD','PSD','PZD','PDC') THEN DO;
      IF UTSTY IN ('IFD','ILD','ISD','IZD','IDC','IDP','IZP') THEN DO;
         ITEM = '840'; OUTPUT;
      END;
   END;
*  ELSE IF UTREF IN ('IINV','ITRD','ITAP') THEN DO;
   ELSE IF UTREF IN ('IINV','IDRI','IDLG') THEN DO;
      IF UTSTY IN ('SBA') AND UTDLP IN ('IOP') THEN DO;
         ITEM = '633'; OUTPUT;
      END;
      ELSE IF UTSTY IN ('SDC','LDC') THEN DO;
         ITEM  = '632'; OUTPUT;
      END;
      ELSE IF UTSTY IN ('CB1','CB2','CF1','CF2','CNT','MGI',
                        'ITB','SAC','BMN','BMC','BMF','SCD','SCM',
                        'MGS','MTB','BNB','BNN','CMB','SMC') THEN DO;
         ITEM = '631';
         IF &INST='PBB' THEN DO;
            AMOUNT = AMOUNT + UTAICT;
         END;
         OUTPUT;
      END;
      ELSE IF UTSTY IN ('ISB','IDS','IBZ','ICN') THEN DO;
              IF UTMM1 = 'GGB' THEN ITEM = '636';
              ELSE IF UTMM1 = 'NGB' THEN ITEM = '635';
              AMOUNT = AMOUNT + UTAICT;
              OUTPUT;
      END;
      ELSE IF UTSTY IN ('DHB','KHA') THEN DO;
         ITEM = '636'; OUTPUT;
      END;
      ELSE IF UTSTY IN ('DBD') THEN DO;
         ITEM = '634'; OUTPUT;
      END;
   END;
   IF UTSTY IN ('SIP') THEN DO;
      ITEM='610'; OUTPUT;
   END;
*;
%INC PGM(KALMLIQ4);
*;

DATA KTBL (KEEP=BNMCODE AMOUNT AMTUSD AMTSGD) KTBLALL;
   %DCLVAR
   SET K1TBL(IN=A) K3TBL(IN=B) K1TBX;
   IF      A THEN TBL = '1';
   ELSE IF B THEN TBL = '3';
   IF _N_ = 1 THEN DO;
      SET REPTDATE;
      RPYR  = YEAR(REPTDATE);
      RPMTH = MONTH(REPTDATE);
      RPDAY = DAY(REPTDATE);
      IF MOD(RPYR,4) = 0 THEN RD2 = 29;
   END;
   IF ITEM ^= ' ';
   IF MATDT - REPTDATE < 8 THEN REMMTH = 0.1;
   ELSE DO;
      %REMMTH
   END;
   IF MATDT - ISSDT    < 8 THEN ORI30D = 0.1;
   ELSE                         ORI30D = (MATDT-ISSDT)/30;
   BNMCODE = PART||ITEM||'00'||PUT(REMMTH,REMFMT.)||'0000Y';
   OUTPUT;
   *------------------------------------------------*
   *  DUPLICATE ANOTHER SET FOR PART 1              *
   *  95 = PART 2-RM, 96 = PART 2-FX                *
   *  93 = PART 1-RM, 94 = PART 1-FX                *
   *------------------------------------------------*;
   IF PART = '95' THEN SUBSTR(BNMCODE,1,2) = '93';
   ELSE SUBSTR(BNMCODE,1,2) = '94';
   OUTPUT;
RUN;


*----------------------------------------------------------------*
*  DISTRIBUTION PROFILE OF CUSTOMER DEPOSITS (PART 3)            *
*----------------------------------------------------------------*;
*------------------------------------------------*
*  NON-INTERBANK REPOS                           *
*------------------------------------------------*;
DATA K1TBL;
   KEEP CAT NAME AMOUNT;
   LENGTH CAT $20 NAME $24;
   SET BNMK.K1TBL&REPTMON&NOWK (RENAME=(GWBALC=AMOUNT GWSHN=NAME));
   IF GWCCY = 'MYR' AND GWMVT = 'P' AND GWMVTS = 'M';
   IF SUBSTR(GWCTP,1,1) ^= 'B' AND SUBSTR(GWDLP,2,2) IN ('MI','MT');
   CAT = 'NON-INTERBANK REPOS';
*;
*------------------------------------------------*
*  NON-INTERBANK NIDS                            *
*------------------------------------------------*;
DATA K3TBL;
   KEEP CAT NAME AMOUNT;
   LENGTH CAT $20 NAME $24;
   SET BNMK.K3TBL&REPTMON&NOWK;
   IF SUBSTR(UTCTP,1,1) ^= 'B' AND
      UTREF IN ('PFD','PLD','PSD','PZD','PDC') AND
      UTSTY IN ('IFD','ILD','ISD','IZD','IDC','IDP','IZP');
   AMOUNT = UTAMOC - UTDPF;
   NAME = UTCUS || UTCLC;
   CAT = 'NON-INTERBANK NIDS';
*;
PROC APPEND BASE=K1TBL DATA=K3TBL;
*;
PROC SUMMARY DATA=K1TBL NWAY;
   CLASS CAT NAME;
   VAR AMOUNT;
   OUTPUT OUT=K1TBL (DROP=_TYPE_ _FREQ_) SUM=;
RUN;
