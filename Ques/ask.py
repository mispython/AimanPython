//FTPMRBDP JOB (0216,0000,90,OPER),'FTP DATAWAREHOUSE',                 
//         CLASS=A,REGION=64M,NOTIFY=&SYSUID,                           
//         MSGCLASS=X                                                   
/*JOBPARM S=S1M1                                                        
//*******************************************************               
//**  SFTP TO DRR                                                       
//*******************************************************               
//STEP01   EXEC PGM=ICEGENER                                            
//SYSPRINT DD SYSOUT=*                                                  
//SYSUT1   DD *                                                         
cd "WMTB-SA&S/DEP - SAS/BANK TOTAL"                                     
//SYSUT2   DD DSN=&&FTPPUT,DISP=(NEW,PASS),                             
//            SPACE=(TRK,(1)),UNIT=SYSDA,RECFM=FB,LRECL=80              
//SYSIN    DD DUMMY                                                     
//*---------------------------------------------------------            
//* FTP HOST DATASETS TO DATA REPORT REPOSITORY SYSTEM (DRR)            
//*---------------------------------------------------------            
//*%OPC SCAN                                                            
//*%OPC SETVAR TODD=(ODD - 1CD)                                         
//*%OPC SETVAR TOMM=(OMM - 1CD)                                         
//*%OPC SETVAR TOYY=(OYY - 1CD)                                         
//*----------------------------------------------                       
//RUNSFTP  EXEC COZBATCH                                                
//CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(DRR#SFTP)                 
//           DD *                                                       
lzopts servercp=$servercp,notrim,overflow=trunc,mode=text               
lzopts linerule=$lr                                                     
//           DD DISP=SHR,DSN=&&FTPPUT                                   
//           DD *                                                       
PUT //SAP.PBB.EIBMRB01   EIBMRB01@%TODD.%TOMM.%TOYY..TXT                
PUT //SAP.PBB.EIBMRB02   EIBMRB02@%TODD.%TOMM.%TOYY..TXT                
cd "../MISC/FD & FCY FD WITHDRAWAL"                                     
PUT //SAP.PBB.EIBMRB09   EIBMRB09@%TODD.%TOMM.%TOYY..TXT                
PUT //SAP.PBB.EIBMRB8A   EIBMRB8A@%TODD.%TOMM.%TOYY..TXT                
PUT //SAP.PBB.EIBMRB8B   EIBMRB8B@%TODD.%TOMM.%TOYY..TXT                
cd "../NEW ACT OPENED"                                                  
PUT //SAP.PBB.EIBMRB03   EIBMRB03@%TODD.%TOMM.%TOYY..TXT                
PUT //SAP.PBB.EIBMRB04   EIBMRB04@%TODD.%TOMM.%TOYY..TXT                
PUT //SAP.PBB.EIBMRB06   EIBMRB06@%TODD.%TOMM.%TOYY..TXT                
PUT //SAP.PBB.EIBMRB07   EIBMRB07@%TODD.%TOMM.%TOYY..TXT                
cd "../../RM RPT/FCY & GIA/OS BALANCE"                                  
PUT //SAP.PBB.EIBMRB05   EIBMRB05@%TODD.%TOMM.%TOYY..TXT                
EOB                                                                     
