//FTPDMS1F JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M                     
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
//*------------------------------------------------                     
//* FTP HOST DATASETS TO PBB DATAWAREHOUSE SERVER                       
//* EJS A2016-27310: NO-OP FTPDMS1F                                     
//* FTP MOVED TO RESPECTIVE JOBS PRODUCING THE                          
//* REPORTS WEF 28/10/2016 EOD                                          
//*------------------------------------------------                     
//*%OPC SCAN                                                            
//*%OPC SETVAR TODD=(ODD -15CD)                                         
//*%OPC SETVAR TOMM=(OMM -15CD)                                         
//*%OPC SETVAR TOYY=(OYY -15CD)                                         
//*----------------------------------------------                       
//*RUNSFTP  EXEC COZBATCH                                               
//*CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(CSASSFTP)                
//*           DD *                                                      
//*lzopts servercp=$servercp,notrim,overflow=trunc,mode=text            
//*lzopts linerule=$lr                                                  
//*cd TEXTFILE                                                          
//*cd RETAILBANK                                                        
//*PUT //SAP.PBB.DMMISR1F.DAILY   DMMISR1F@%ODD.%OMM.%OYY..TXT          
//*EOB                                                                  
//*---------------------------------------------------------            
//* FTP HOST DATASETS TO DATA REPORT REPOSITORY SYSTEM (DRR)            
//*---------------------------------------------------------            
//RUNSFTP  EXEC COZBATCH                                                
//CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(DRR#SFTP)                 
//           DD *                                                       
lzopts servercp=$servercp,notrim,overflow=trunc,mode=text               
lzopts linerule=$lr                                                     
//           DD DISP=SHR,DSN=&&FTPPUT                                   
//           DD *                                                       
PUT //SAP.PBB.DMMISR1F.DAILY   DMMISR1F@%ODD.%OMM.%OYY..TXT             
EOB                                                                     
//*                                                                     
