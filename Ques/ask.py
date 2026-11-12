//FTPMBNM1 JOB (0216,0000,90,OPER),'FTP WAREHOURE', 
//         CLASS=A,REGION=64M,NOTIFY=&SYSUID,       
//         MSGCLASS=X                               
//***********************************************   
//* RUN AFTER EIBMRPTS / EIMBNM01                   
//***********************************************   
/*JOBPARM S=S1M2                                    
//PRINTA   OUTPUT CLASS=R,                          
//         NAME='THE MANAGER',                      
//         ROOM='22ND FLOOR',                       
//         BUILDING='MENARA PBB',                   
//         DEPT='DEPOSIT AND CHANNEL MANAGEMENT',   
//         ADDRESS=('MENARA PUBLIC BANK',           
//         '146 JALAN AMPANG','50450 KUALA LUMPUR'),
//         DEST=S1.LOCAL                            
//*                                                 
//***********************************************   
//* FTP HOST DATASETS TO PBB DATAWAREHOUSE SERVER   
//***********************************************           
//*%OPC SCAN                                                
//*%OPC SETVAR TOMM=(OMM - 1MO)                             
//*RUNSFTP  EXEC COZBATCH                                   
//*CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(CSASSFTP)    
//*           DD *                                          
//*lzopts servercp=$servercp,notrim,overflow=trunc,mode=text
//*lzopts linerule=$lr                                      
//*cd TEXTFILE                                              
//*cd FIN                                                   
//*PUT //SAP.PBB.EIMBNM01.TEXT   EIMBNM01.TXT               
//*PUT //SAP.PIBB.EIIBNM01.TEXT  EIIBNM01.TXT               
//*cd ..                                                    
//*cd RiskMgt                                               
//*cd BNM                                                   
//*PUT //SAP.PBB.EIMBNM01.TEXT   EIMBNM01_%TOMM..TXT        
//*PUT //SAP.PIBB.EIIBNM01.TEXT  EIIBNM01_%TOMM..TXT        
//*EOB                                                       
//***********************************************************
//* FTP HOST DATASETS TO DATA REPORT REPOSITORY SYSTEM (DRR) 
//***********************************************************
//*%OPC SCAN                                                 
//*%OPC SETVAR TOMM=(OMM - 1MO)                              
//RUNSFTP  EXEC COZBATCH                                     
//CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(DRR#SFTP)      
//           DD *                                            
lzopts servercp=$servercp,notrim,overflow=trunc,mode=text    
lzopts linerule=$lr                                          
cd "FD-BNM REPORTING/PBB/BNM RPTG/BNM RPTG_SUB"              
PUT //SAP.PBB.EIMBNM01.TEXT   EIMBNM01.TXT                   
cd "/FD-BNM REPORTING/PIBB/BNM RPTG/BNM RPTG_SUB"            
PUT //SAP.PIBB.EIIBNM01.TEXT  EIIBNM01.TXT                   
EOB                                                          
//*----------------------------------------------            
//*   PRINT REPORTS LOCALLY.                     
//* ---------------------------------------------
//*DPSTSA   EXEC PGM=IEBGENER                    
//*SYSIN    DD DUMMY                             
//*SYSPRINT DD SYSOUT=X                          
//*SYSUT1   DD DISP=SHR,DSN=SAP.PBB.STAC.SA.TEXT 
//*SYSUT2   DD SYSOUT=(,),OUTPUT=(*.PRINTA)      
//*SYSPRINT DD SYSOUT=X                          
//*SORTDIAG DD SYSOUT=X                          
//*SYSIN   DD DUMMY                              
//*                                              
//*DPSTCA   EXEC PGM=IEBGENER                    
//*SYSIN    DD DUMMY                             
//*SYSPRINT DD SYSOUT=X                          
//*SYSUT1   DD DISP=SHR,DSN=SAP.PBB.STAC.CA.TEXT 
//*SYSUT2   DD SYSOUT=(,),OUTPUT=(*.PRINTA)      
//*SYSPRINT DD SYSOUT=X                          
//*SORTDIAG DD SYSOUT=X                         
//*SYSIN   DD DUMMY                             
//*                                             
//*DPSTFD   EXEC PGM=IEBGENER                   
//*SYSIN    DD DUMMY                            
//*SYSPRINT DD SYSOUT=X                         
//*SYSUT1   DD DISP=SHR,DSN=SAP.PBB.STAC.FD.TEXT
//*SYSUT2   DD SYSOUT=(,),OUTPUT=(*.PRINTA)     
//*SYSPRINT DD SYSOUT=X                         
//*SORTDIAG DD SYSOUT=X                         
//*SYSIN   DD DUMMY                             
