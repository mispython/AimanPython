//FTPDRBDP JOB (0216,0000,90,OPER),'FTP DATAWAREHOUSE',   
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
//*                                                         
//*%OPC SCAN                                                
//*----------------------------------------------           
//* FTP HOST DATASETS TO CRM DATAWAREHOUSE SERVER           
//*----------------------------------------------           
//*RUNSFTP  EXEC COZBATCH                                   
//*CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(CSASSFTP)    
//*           DD *                                          
//*lzopts servercp=$servercp,notrim,overflow=trunc,mode=text
//*lzopts linerule=$lr                                      
//*cd TEXTFILE                                              
//*cd RETAILBANK                                            
//*PUT //SAP.PBB.EIBDRB01   EIBDRB01@%OMM.%OYY..TXT         
//*PUT //SAP.PBB.EIBDRB2A   EIBDRB2A@%ODD.%OMM.%OYY..TXT    
//*PUT //SAP.PBB.EIBDRB2B   EIBDRB2B@%ODD.%OMM.%OYY..TXT    
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
PUT //SAP.PBB.EIBDRB01   EIBDRB01@%OMM.%OYY..TXT            
cd "../MISC/FD & FCY FD WITHDRAWAL"                         
PUT //SAP.PBB.EIBDRB2A   EIBDRB2A@%ODD.%OMM.%OYY..TXT       
PUT //SAP.PBB.EIBDRB2B   EIBDRB2B@%ODD.%OMM.%OYY..TXT       
EOB                                                         
