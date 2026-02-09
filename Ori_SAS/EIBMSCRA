//EIBMSCRA JOB MIS,EIBMSISD,CLASS=A,NOTIFY=&SYSUID,MSGCLASS=X,
//          USER=OPCC                                                   00020016
//*                                                                     00070000
//EIBMSCRA EXEC SAS609                                                  00170014
//MNITB     DD DSN=SAP.PBB.MNITB(0),DISP=SHR                            00180000
//BRHFI     DD DSN=SAP.RBP2.B033.PBB.BRANCH,DISP=SHR                    00210000
//PGM       DD DSN=SAP.BNM.PROGRAM,DISP=SHR                             00210000
//ELDS      DD DSN=SAP.PBB.ELDS.SRR.TEXT(0),DISP=SHR                    00220000
//DEP       DD DSN=RBP2.B033.DP.SRR.DATA,DISP=SHR                       00220000
//          DD DSN=RBP2.B033.DP.SCRFD.DATA,DISP=SHR                     00220000
//CARDFILE  DD DSN=SAP.PBCS.ACCT33(0),DISP=SHR                          00230000
//          DD DSN=SAP.PBCS.ACCT55(0),DISP=SHR                          00230000
//HRBR      DD DSN=HRP2.FLAT.STAFF.INCTIVE.BR,DISP=SHR                  00230000
//HRHO      DD DSN=HRP2.FLAT.STAFF.INCTIVE.HO,DISP=SHR                  00240000
//HRRG      DD DSN=HRP2.FLAT.STAFF.INCTIVE.REGOFF,DISP=SHR              00260000
//SCRD      DD DSN=SAP.PBB.SRS.SASDATA,DISP=OLD                         00260000
//SASLIST   DD SYSOUT=X                                                 00270000
                                                                        00280000
OPTION NOCENTER NODATE NONUMBER;                                        00290000
                                                                        00300000
%INC PGM(PBMISFMT,PBBDPFMT);                                            00310000
                                                                        00320000
DATA _NULL_;                                                            00330000
   SET MNITB.REPTDATE;                                                  00340000
   CALL SYMPUT('RDATE',PUT(REPTDATE,DDMMYY10.));                        00350000
   CALL SYMPUT('RMM',PUT(REPTDATE,MONTH2.));                            00360000
   CALL SYMPUT('RMON',PUT(REPTDATE,MONYY7.));                           00370000
   CALL SYMPUT('RYY',PUT(REPTDATE,YEAR2.));                             00380000
   CALL SYMPUT('RYEAR',PUT(REPTDATE,YEAR4.));                           00390000
*;                                                                      00410006
   /*                                                                   00410006
DATA HRRG;                                                              00420006
   INFILE HRRG;                                                         00430006
   INPUT @005 STAFF    5.                                               00440006
         @072 REGN   $15.;                                              00450006
   BRANCH=000;                                                          00470006
   BRCHCD='   ';                                                        00470006
   LOCT='RG';                                                           00470006
DATA LOCAT;                                                             00470006
   SET HRBR HRHO HRRG;                                                  00470006
  */                                                                    00470006
*;                                                                      00410006
DATA BRH;                                                               00410006
   INFILE BRHFI;                                                        00410006
   INPUT @002 BRANCH  3.                                                00410006
         @006 BRCHCD $3.;                                               00410006
*;                                                                      00410006
DATA HRBR;                                                              00420006
   INFILE HRBR;                                                         00430006
   INPUT @005 STAFF   5.                                                00440006
         @069 BRANCH  3.                                                00450006
         @072 BRCHCD  $3.;                                              00450006
   IF (002<=BRANCH<=267);                                               00460006
*;                                                                      00710008
DATA HRHO;                                                              00420006
   INFILE HRHO;                                                         00430006
   INPUT @005 STAFF    5.                                               00440006
         @072 HOE    $15.;                                              00450006
*;                                                                      00710008
DATA UCARD;                                                             00711008
   FORMAT TYPEC 4.;                                                     00712008
   LENGTH STAFF 5. STAFF8 8.;                                           00713008
   INFILE CARDFILE;                                                     00714008
   INPUT  @001 ACCTNO   11.                                             00715008
          @014 OPNMM     2.                                             00716008
          @016 OPNYR     2.                                             00716008
          @557 BRANC     4.                                             00717008
          @561 SECO    $12.;                                            00718008
                                                                        00719008
   TYPEC=SUBSTR(ACCTNO,1,5);                                            00719108
   IF TYPEC IN (3301,3302,3305,3306,3308,3309,5503,5504);               00719208
   C1BAL=0.00; C2BAL=0.00; C3BAL=0.00;                                  00719314
   C3CNT=0; C1CNT=0; C2CNT=0;                                           00719409
   IF TYPEC=3309 THEN DO;                                               00719509
      C1CNT=1; NCORE='X';  /* DEBIT   */                                00719509
   END;                                                                 00719509
   ELSE DO;                                                             00719509
      C2CNT=1; NCORE='C';  /* CREDIT  */                                00719609
   END;                                                                 00719509
   IF (OPNYR=06 AND OPNMM>08) OR                                        00719709
       OPNYR GE 07;                                                     00719709
   IF (1<LENGTH(SECO)<6);                                               00719809
   STAFF8=SECO;                                                         00719909
   IF (1<STAFF8<99999);                                                 00720009
   PRODUCT='CARD           ';                                           00720112
   STAFF=STAFF8;                                                        00720209
*;                                                                      00770009
PROC SORT; BY STAFF;                                                    00770009
PROC SORT DATA=HRBR; BY STAFF;                                          00770009
PROC SORT DATA=HRHO; BY STAFF;                                          00770009
*;                                                                      00770009
DATA UCBR UCEXC;                                                        00770009
   MERGE HRBR(IN=A)  UCARD(IN=B); BY STAFF;                             00770009
   IF A AND B     THEN OUTPUT UCBR;                                     00770009
   IF B AND NOT A THEN OUTPUT UCEXC;                                    00770009
*;                                                                      00770009
DATA UCHO UCEXC1;                                                       00770009
   MERGE HRHO(IN=A) UCEXC(IN=B); BY STAFF;                              00770009
   IF A AND B     THEN OUTPUT UCHO;                                     00770009
   IF B AND NOT A THEN OUTPUT UCEXC1;                                   00770009
*;                                                                      00770009
DATA DEP;                                                               00771009
   FORMAT YTDBAL 14.2;                                                  00780013
   INFILE DEP;                                                          00781013
   INPUT @002 ACCTNO    10.                                             00790000
         @050 PRIMOFF    5.                                             00800000
         @056 SECNOFF    5.                                             00810000
         @061 XCORE     $1.                                             00820007
         @062 AVGBAL   14.2                                             00830000
         @076 YTDBALS  14.2                                             00840013
         @090 NUMMTH     2.                                             00841013
         @097 BRANCH     3.;                                            00841013
                                                                        00860000
   IF (1000000000<=ACCTNO<=1999999999) THEN                             00880000
   PRODUCT='FIXED DEPOSITS    ';                                        00890009
   IF (7000000000<=ACCTNO<=7999999999) THEN                             00891014
   PRODUCT='FIXED DEPOSITS    ';                                        00892009
   IF (3000000000<=ACCTNO<=3999999999) THEN                             00900014
   PRODUCT='CURRENT ACCOUNT   ';                                        00910005
   IF (4000000000<=ACCTNO<=4999999999) THEN                             00920000
   PRODUCT='SAVING ACCOUNT    ';                                        00930009
   IF (6000000000<=ACCTNO<=6999999999) THEN                             00931009
   PRODUCT='SAVING ACCOUNT    ';                                        00932009
   YTDBAL =ROUND(YTDBALS/NUMMTH);                                       00950013
   TAG='DEPO';                                                          01520409
*;                                                                      00960000
PROC SORT; BY BRANCH;                                                   00960000
PROC SORT DATA=BRH; BY BRANCH;                                          01562314
*;                                                                      00960000
DATA DEP;                                                               00960000
   MERGE BRH DEP(IN=A); BY BRANCH;                                      00960000
   IF A;                                                                00960000
*;                                                                      00960000
DATA DPC1;                                                              00970009
   SET DEP;                                                             00980000
   STAFF=PRIMOFF;                                                       00990000
   IF  STAFF IN (0,.) THEN DELETE;                                      01000000
   IF  XCORE=' ';                                                       01010007
   NCORE='X';                                                           01020007
   CATG='CAT.1         ';                                               01020007
   C1CNT=1;                                                             01021009
   C1BAL=YTDBAL;                                                        01022009
*;                                                                      01035009
DATA DPC2;                                                              01040009
   SET DEP;                                                             01050000
   STAFF=SECNOFF;                                                       01060000
   IF  STAFF IN (0,.) THEN STAFF=PRIMOFF;                               01070000
   IF  STAFF IN (0,.) THEN DELETE;                                      01080000
   IF  XCORE IN ('C','N');                                              01090007
   IF  XCORE='C' THEN DO;                                               01100009
       CATG='CAT.2 CORE    ';                                           01020007
       NCORE='C';                                                       01100109
       C2CNT=1;                                                         01100209
       C2YBAL=YTDBAL;                                                   01100309
   END;                                                                 01100409
   IF  XCORE='N' THEN DO;                                               01110009
       CATG='CAT.2 NON-CORE';                                           01020007
       NCORE='N';                                                       01111009
       C3CNT=1;                                                         01112009
       C3BAL=YTDBAL;                                                    01113009
   END;                                                                 01120009
*;                                                                      01313709
DATA ELDS;                                                              01320000
   FORMAT BRCHCD $3.;                                                   01350000
   INFILE ELDS;                                                         01350000
   INPUT  @001 AANUM   $13.                                             01360000
          @017 STAFX1  $5.  /* CATEGORY 1 - ATTENDED BY */              01370000
          @026 STAFX2  $5.  /* CATEGORY 2 - CORE        */              01380000
          @035 STAFX3  $5.  /* CATEGORY 3 - NON-CORE    */              01390000
          @051 PRODUCT $18.                                             01400000
          @069 YTDBAL  12.;                                             01420015
                                                                        01420015
   IF  STAFX1='*****' THEN DELETE;                                      01430007
   IF  ('00001'<=STAFX1<='99999') THEN NCORE='X';                       01430007
   IF  ('00001'<=STAFX2<='99999') THEN NCORE='C';                       01440007
   IF  ('00001'<=STAFX3<='99999') THEN NCORE='N';                       01450007
   TAG='ELDS';                                                          01520409
   BRCHCD=SUBSTR(AANUM,1,3);                                            01520409
*;                                                                      01562314
PROC SORT; BY BRCHCD;                                                   01562314
PROC SORT DATA=BRH; BY BRCHCD;                                          01562314
*;                                                                      01562314
DATA ELDS;                                                              01461005
   MERGE BRH ELDS(IN=A); BY BRCHCD;                                     01340000
   IF A;                                                                01340000
   LENGTH STAFF 5.;                                                     01340000
   SET ELDS;                                                            01461005
   IF NCORE='X'  THEN DO;                                               01472009
      CATG='CAT.1         ';
      STAFF=STAFX1*1.;                                                  01472109
      C1CNT=1;   C1BAL=YTDBAL;                                          01472209
   END;                                                                 01472409
   IF NCORE='C'  THEN DO;                                               01472509
      CATG='CAT.2 CORE    ';
      STAFF=STAFX2*1.;                                                  01472609
      C2CNT=1;   C2BAL=YTDBAL;                                          01472709
   END;                                                                 01472909
   IF NCORE='N'  THEN DO;                                               01473009
      CATG='CAT.2 NON-CORE';
      STAFF=STAFX3*1.;                                                  01473109
      C3CNT=1;                                                          01473209
      C3BAL=YTDBAL;                                                     01473309
   END;                                                                 01474009
*;                                                                      01520209
DATA SRS;                                                               01520309
   SET ELDS DPC1 DPC2;                                                  01520409
*;                                                                      01520209
PROC SORT; BY STAFF;                                                    01520209
PROC SORT DATA=HRHO; BY STAFF;                                          01520209
*;                                                                      00490006
DATA SRSBR SRSHO SRSEXC;                                                00490006
   MERGE HRHO(IN=A) SRS(IN=B); BY STAFF;                                00490006
   IF A AND B     THEN OUTPUT SRSHO;                                    00490006
   IF B AND NOT A THEN DO;                                              00490006
      IF BRANCH > 0  THEN OUTPUT SRSBR;                                 00490006
                     ELSE OUTPUT SRSEXC;                                00490006
   END;                                                                 00490006
*;                                                                      00620006
                                                                        00620006
TITLE 'EXCEPTION REPORT : UMMATCHED STAFF ID AGAINST HR FILE';          01520209
  /*                                                                    01520209
PROC  PRINT DATA=UCEXC1;                                                01520209
VAR   ACCTNO STAFF TAG;                                                 01520209
  */                                                                    01520209
PROC  PRINT DATA=SRSEXC;                                                01520209
WHERE TAG='ELDS';                                                       01520209
VAR   AANUM STAFF PRODUCT YTDBAL;                                       01520209
PROC  PRINT DATA=SRSEXC;                                                01520209
WHERE TAG='DEPO';                                                       01520209
VAR   ACCTNO STAFF PRODUCT YTDBAL;                                      01520209
*;                                                                      01520209
DATA SRSBR;                                                             01520209
   SET SRSBR UCBR;                                                      01520209
*;                                                                      01520209
DATA SRSHO;                                                             01520209
   SET SRSHO UCHO;                                                      01520209
*;                                                                      01520209
PROC SUMMARY DATA=SRSBR NWAY;                                           01521109
CLASS BRCHCD STAFF PRODUCT NCORE;                                       01521209
VAR   C1CNT C2CNT C3CNT C1BAL C2BAL C3BAL;                              01521314
OUTPUT OUT=SCRD.SRSBR (DROP=_TYPE_ _FREQ_) SUM=;                        01521409
*;                                                                      01520209
PROC SUMMARY DATA=SRSHO NWAY;                                           01521109
CLASS HOE STAFF PRODUCT NCORE;                                          01521209
VAR   C1CNT C2CNT C3CNT C1BAL C2BAL C3BAL;                              01521314
OUTPUT OUT=SCRD.SRSHO (DROP=_TYPE_ _FREQ_) SUM=;                        01521409
*;                                                                      01520209
DATA SRSS;                                                              01520209
   SET SRSHO SRSBR;                                                     01520209
*;                                                                      01520209
PROC SUMMARY DATA=SRSS NWAY;                                            01521109
CLASS STAFF PRODUCT NCORE;                                              01521209
VAR   C1CNT C2CNT C3CNT C1BAL C2BAL C3BAL;                              01521314
OUTPUT OUT=SRSS (DROP=_TYPE_ _FREQ_) SUM=;                              01521409
*;                                                                      01520209
DATA SRSS;                                                              01520209
   SET SRSS;                                                            01520209
   S1CNT=0; S2CNT=0; S3CNT=0;                                           00620006
   IF  NCORE='X' THEN S1CNT=1; ELSE                                     00620006
   IF  NCORE='C' THEN S2CNT=1; ELSE                                     00620006
   IF  NCORE='N' THEN S3CNT=1;                                          00620006
   IF  C1BAL=.   THEN C1BAL=0.00;
   IF  C2BAL=.   THEN C2BAL=0.00;
   IF  C3BAL=.   THEN C3BAL=0.00;
   IF  C1CNT=.   THEN C1CNT=0;
   IF  C2CNT=.   THEN C2CNT=0;
   IF  C3CNT=.   THEN C3CNT=0;
   IF  S1CNT=.   THEN S1CNT=0;
   IF  S2CNT=.   THEN S2CNT=0;
   IF  S3CNT=.   THEN S3CNT=0;
*;                                                                      00620006
PROC SUMMARY DATA=SRSS NWAY;                                            00620006
CLASS PRODUCT;                                                          00620006
VAR   S1CNT C1CNT C1BAL S2CNT C2CNT C2BAL S3CNT C3CNT C3BAL;            00620006
OUTPUT OUT=SRSP SUM=;                                                   00620006
*;
TITLE1 'SUMMARY REPORT ON STAFF PARTICIPATION UNDER SCR BY PRODUCT';
TITLE2 'REPORT DATE: ' "&RDATE";
PROC PRINT DATA=SRSP LABEL;
VAR   PRODUCT
      S1CNT C1CNT C1BAL S2CNT C2CNT C2BAL
      S3CNT C3CNT C3BAL;
SUM   S1CNT C1CNT C1BAL S2CNT C2CNT C2BAL
      S3CNT C3CNT C3BAL;
FORMAT C1BAL C2BAL C3BAL COMMA18.2;
LABEL PRODUCT='PRODUCT'
      S1CNT  ='CAT.1 #STAFF'
      C1CNT  ='CAT.1 #ACCTS'
      C1BAL  ='CAT.1 YTD BALANCE'
      S2CNT  ='CAT.2 #STAFF CORE'
      C2CNT  ='CAT.2 #ACCTS CORE'
      C2BAL  ='CAT.2 YTDBAL CORE'
      S3CNT  ='CAT.2 #STAFF NON-CORE'
      C3CNT  ='CAT.2 #ACCTS NON-CORE'
      C3BAL  ='CAT.2 YTDBAL NON-CORE';
