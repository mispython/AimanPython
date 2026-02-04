//EIBMLIBT JOB MIS,MISEIS,COND=(4,LT),CLASS=A,MSGCLASS=X,
//         NOTIFY=&SYSUID
//*
//EIBMLIBT EXEC SAS609,REGION=6M,WORK='120000,8000'
//PGM      DD DSN=SAP.BNM.PROGRAM,DISP=SHR
//BNM1     DD DSN=SAP.BT.SASDATA,DISP=SHR
//BNM      DD DSN=SAP.PBB.NLF.LN.DAILY,DISP=OLD
//SASLIST  DD SYSOUT=X
//SYSIN     DD *

OPTIONS SORTDEV=3390 YEARCUTOFF=1950 NOCENTER;
*;
DATA REPTDATE;
   SET BNM1.REPTDATE;
   SELECT(DAY(REPTDATE));
      WHEN (8)  CALL SYMPUT('NOWK',PUT('1',$1.));
      WHEN (15) CALL SYMPUT('NOWK',PUT('2',$1.));
      WHEN (22) CALL SYMPUT('NOWK',PUT('3',$1.));
      OTHERWISE CALL SYMPUT('NOWK',PUT('4',$1.));
   END;
   CALL SYMPUT('REPTYEAR',PUT(REPTDATE,YEAR4.));
   CALL SYMPUT('REPTMON',PUT(MONTH(REPTDATE),Z2.));
   CALL SYMPUT('REPTDAY',PUT(DAY(REPTDATE),Z2.));
   CALL SYMPUT('RDATE',PUT(REPTDATE,DDMMYY8.));
RUN;
*;
%INC PGM(PBBLNFMT,PBBELF);
*;
PROC FORMAT;
   VALUE REMFMT
      LOW-0.1 = '01'   /*  UP TO 1 WK       */
      0.1-1   = '02'   /*  >1 WK - 1 MTH    */
      1-3     = '03'   /*  >1 MTH - 3 MTHS  */
      3-6     = '04'   /*  >3 - 6 MTHS      */
      6-12    = '05'   /*  >6 MTHS - 1 YR   */
      OTHER   = '06';  /*  > 1 YEAR         */
*;
   VALUE PRDFMT
      4,5,6,7,31,32,100,101,102,103,110,111,112,113,114,115,
      116,170,200,201,204,205,209,210,
      211,212,214,215,219,220,225,226,
      227,228,229,230,231,232,233,234 = 'HL'
      350,910,925 = 'RC'
      OTHER = 'FL';
*;
%MACRO DCLVAR;
   RETAIN D1-D12 31 D4 D6 D9 D11 30
          RD1-RD12 MD1-MD12 31 RD2 MD2 28 RD4 RD6 RD9 RD11
          MD4 MD6 MD9 MD11 30 RPYR RPMTH RPDAY;
   ARRAY LDAY D1-D12;
   ARRAY RPDAYS RD1-RD12;
   ARRAY MDDAYS MD1-MD12;
%MEND DCLVAR;
*;
 *------------------------------------------------*
 *  MACRO TO CALCULATE NEXT BLDATE                *
 *------------------------------------------------*;
%MACRO NXTBLDT;
   IF PAYFREQ = '6' THEN DO;
      DD = DAY(BLDATE) + 14;
      MM = MONTH(BLDATE);
      YY = YEAR(BLDATE);
      IF MM = 2 THEN
         IF MOD(YY,4) = 0 THEN D2 = 29;
         ELSE D2 = 28;
      IF DD > LDAY(MM) THEN DO;
         DD = DD - LDAY(MM);
         MM + 1;
         IF MM > 12 THEN DO;
            MM = MM - 12; YY + 1;
         END;
      END;
   END;
   ELSE DO;
      DD = DAY(ISSDTE);
      MM = MONTH(BLDATE) + FREQ;
      YY = YEAR(BLDATE);
      IF MM > 12 THEN DO;
         MM = MM - 12; YY + 1;
      END;
   END;
   IF MM = 2 THEN
      IF MOD(YY,4) = 0 THEN D2 = 29;
      ELSE D2 = 28;
   IF DD > LDAY(MM) THEN DD = LDAY(MM);
   BLDATE = MDY(MM,DD,YY);
%MEND NXTBLDT;
*;
 *------------------------------------------------*
 *  MACRO TO CALCULATE REMAIN MONTH               *
 *------------------------------------------------*;
%MACRO REMMTH;
   MDYR  = YEAR(MATDT);
   MDMTH = MONTH(MATDT);
   MDDAY = DAY(MATDT);
   IF MDMTH = 2 THEN
      IF MOD(MDYR,4) = 0 THEN MD2 = 29;
      ELSE MD2 = 28;
   IF MDDAY > RPDAYS(RPMTH) THEN MDDAY = RPDAYS(RPMTH);
   REMY = MDYR - RPYR;
   REMM = MDMTH - RPMTH;
   REMD = MDDAY - RPDAY;
   REMMTH = REMY*12 + REMM + REMD/RPDAYS(RPMTH);
%MEND REMMTH;
*;
 *------------------------------------------------*
 *  GET REPTDATE                                  *
 *------------------------------------------------*;
*;
 *----------------------------------------------------------------*
 *  BREAKDOWN BY MATURITY PROFILE (PART 1 & 2 - RM)               *
 *----------------------------------------------------------------*;
 *------------------------------------------------*
 *  LOANS - FL/HL USE REPAYMENT DATE              *
 *        - OD/RC USE EXPIRY DATE                 *
 *------------------------------------------------*;
DATA NOTE (KEEP=BNMCODE AMOUNT);
   %DCLVAR
   SET BNM1.BTRAD&REPTMON&NOWK;
   IF _N_ = 1 THEN DO;
      SET REPTDATE;
      RPYR  = YEAR(REPTDATE);
      RPMTH = MONTH(REPTDATE);
      RPDAY = DAY(REPTDATE);
      IF MOD(RPYR,4) = 0 THEN RD2 = 29;
   END;
   IF SUBSTR(PRODCD,1,2) = '34' OR PRODUCT IN (225,226);
   IF CUSTCD IN ('77','78','95','96') THEN CUST = '08';
   ELSE CUST = '09';
   PROD = 'BT';
   IF CUSTCD IN ('77','78','95','96') THEN
      SELECT (PROD);
         WHEN ('HL') ITEM = '214';
         OTHERWISE   ITEM = '219';
      END;
   ELSE SELECT (PROD);
      WHEN ('FL') ITEM = '211';
      WHEN ('RC') ITEM = '212';
      OTHERWISE   ITEM = '219';
   END;

   IF BLDATE > 0 THEN DAYS = REPTDATE - BLDATE;
   IF EXPRDATE - REPTDATE < 8 THEN REMMTH = 0.1;
   ELSE DO;
      PAYFREQ = '3';
      SELECT (PAYFREQ);
         WHEN ('1') FREQ = 1;
         WHEN ('2') FREQ = 3;
         WHEN ('3') FREQ = 6;
         WHEN ('4') FREQ = 12;
         OTHERWISE;
      END;
      IF PRODUCT IN (350,910,925) THEN
         BLDATE = EXPRDATE;
      ELSE IF BLDATE <= 0 THEN DO;
         BLDATE = ISSDTE;
         DO WHILE (BLDATE <= REPTDATE);
            %NXTBLDT
         END;
      END;
      IF PAYAMT < 0 THEN PAYAMT = 0;
      IF BLDATE > EXPRDATE | BALANCE <= PAYAMT THEN BLDATE = EXPRDATE;
      DO WHILE (BLDATE <= EXPRDATE);
         MATDT = BLDATE;
         %REMMTH
         IF REMMTH > 12 OR BLDATE = EXPRDATE THEN LEAVE;
         AMOUNT = PAYAMT;
         BALANCE = BALANCE - PAYAMT;
         BNMCODE = '95'||ITEM||CUST||PUT(REMMTH,REMFMT.)||'0000Y';
         OUTPUT;
         IF DAYS > 89 THEN REMMTH = 13;
         BNMCODE = '93'||ITEM||CUST||PUT(REMMTH,REMFMT.)||'0000Y';
         OUTPUT;
         %NXTBLDT
         IF BLDATE > EXPRDATE | BALANCE <= PAYAMT THEN
            BLDATE = EXPRDATE;
      END;
   END;
   AMOUNT = BALANCE;
   BNMCODE = '95'||ITEM||CUST||PUT(REMMTH,REMFMT.)||'0000Y';
   OUTPUT;
   IF DAYS > 89 THEN REMMTH = 13;
   BNMCODE = '93'||ITEM||CUST||PUT(REMMTH,REMFMT.)||'0000Y';
   OUTPUT;
*;
PROC SUMMARY DATA=NOTE NWAY;
   CLASS BNMCODE;
   VAR AMOUNT;
   OUTPUT OUT=BNM.BT(DROP=_TYPE_ _FREQ_) SUM=;
*;
PROC PRINT;
SUM AMOUNT;

