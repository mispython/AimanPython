//FTPIBT1C JOB (0216,0000,90,OPER),'FTP WAREHOURE',                     
//         CLASS=A,REGION=64M,NOTIFY=&SYSUID,                           
//         MSGCLASS=X                                                   
/*JOBPARM S=S1M2                                                        
//*                                                                     
//*%OPC SCAN                                                            
//******************************************************************    
//* FTP REPORT FILES (TEXT FORMAT) INTO SAS DATA WAREHOUSE              
//* (TEXTFILE\FIN DIRECTORY).                                           
//******************************************************************    
//*RUNSFTP  EXEC COZBATCH                                               
//*CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(CSASSFTP)                
//*           DD *                                                      
//*lzopts servercp=$servercp,notrim,overflow=trunc,mode=text            
//*lzopts linerule=$lr                                                  
//*cd TEXTFILE                                                          
//*cd FIN                                                               
//*PUT //SAP.PIBB.EIWIBT1C.TEXT            EIWIBT1C%ODD.%OMM..TXT       
//*EOB                                                                  
//******************************************************************    
//* FTP HOST DATASETS TO DATA REPORT REPOSITORY SYSTEM (DRR)            
//******************************************************************    
//RUNSFTP  EXEC COZBATCH                                                
//CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(DRR#SFTP)                 
//           DD *                                                       
lzopts servercp=$servercp,notrim,overflow=trunc,mode=text               
lzopts linerule=$lr                                                     
cd "FD-BNM REPORTING/PIBB/BNM RPTG"                                     
PUT //SAP.PIBB.EIWIBT1C.TEXT            EIWIBT1C%ODD.%OMM..TXT          
EOB                                                                      
