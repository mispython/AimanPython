//FTPBCCDR JOB (0216,0000,90,OPER),'FTP WAREHOURE',                     
//         CLASS=A,REGION=4M,NOTIFY=&SYSUID,                            
//         MSGCLASS=X                                                   
/*JOBPARM S=S1M2                                                        
//******************************************************************    
//* FTP HOST DATASETS (CCD REPORTS)                                     
//*ESMR 2015-2448 TO DISCONTINUE CCDTEXT3/ICCDTEXT3                     
//******************************************************************    
//*RUNSFTP  EXEC COZBATCH                                               
//*CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(CSASSFTP)                
//*           DD *                                                      
//*lzopts servercp=$servercp,notrim,overflow=trunc,mode=text            
//*lzopts linerule=$lr                                                  
//*cd TEXTFILE                                                          
//*cd CCD                                                               
//*PUT //SAP.PBB.CCDREPO  CCDREPO.TXT                                   
//*PUT //SAP.PBB.CCDREPO1  CCDREPO1.TXT                                 
//*PUT //SAP.PBB.CCDRVHP1  CCDRVHP1.TXT                                 
//*PUT //SAP.PBB.CCDRVHP2  CCDRVHP2.TXT                                 
//*PUT //SAP.PBB.CCDTXT2  CCDTEXT2.TXT                                  
//*PUT //SAP.PBB.CCDTXT7A  CCDTXT7A.TXT                                 
//*EOB                                                                  
//******************************************************************    
//* FTP HOST DATASETS TO DATA REPORT REPOSITORY SYSTEM (DRR)            
//******************************************************************    
//RUNSFTP  EXEC COZBATCH                                                
//CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(DRR#SFTP)                 
//           DD *                                                       
lzopts servercp=$servercp,notrim,overflow=trunc,mode=text               
lzopts linerule=$lr                                                     
cd "HPCC/LOANS IN ARREARS"                                              
PUT //SAP.PBB.CCDTXT2   CCDTEXT2.TXT                                    
PUT //SAP.PBB.CCDTXT7A  CCDTXT7A.TXT                                    
cd "/HPCC/REPOSSESSION"                                                 
PUT //SAP.PBB.CCDREPO   CCDREPO.TXT                                     
PUT //SAP.PBB.CCDREPO1  CCDREPO1.TXT                                    
EOB                                                                     
