//FTPFDFCY JOB MSGCLASS=X,MSGLEVEL=(2,1)                                
/*JOBPARM S=S1M1                                                        
//*                                                                     
//*%OPC SCAN                                                            
//******************************************************************    
//* FTP HOST DATASETS TO DATA REPORT REPOSITORY SYSTEM (DRR)            
//******************************************************************    
//RUNSFTP  EXEC COZBATCH                                                
//CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(DRR#SFTP)                 
//           DD *                                                       
lzopts servercp=$servercp,notrim,overflow=trunc,mode=text               
lzopts linerule=$lr                                                     
CD TD/INTERBANK/FCYFD                                                   
PUT //SAP.PBB.FCYFD.REP2.TEXT    FCYFDREP2_%ODD.%OMM.%OYY..CSV          
PUT //SAP.PBB.MNI.FDRMFC         FDRMFC_%ODD.%OMM.%OYY..CSV             
EOB                                                                     
/*                                                                      
