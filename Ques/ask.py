//SFTPDLPS JOB MSGCLASS=X,REGION=4M,TIME=1440,USER=OPCC                 
//*>OPC SCAN                                                            
//*****************************************************************     
//* SFTP EXTRACTION OF SAS DATA TO DLP SERVER                           
//* SMR 2012-1555 (MFM)                                                 
//*****************************************************************     
//RUNSFTP  EXEC COZBATCH                                                
//CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(COZ#DLP3)                 
//           DD *                                                       
lzopts servercp=$servercp,notrim,overflow=trunc,mode=text               
lzopts linerule="lf"                                                    
//           DD DISP=SHR,DSN=SAP.PBB.DLP.SFTP.TEXT                      
//           DD *                                                       
EOB                                                                     
/*                                                                      



Example output of SAP.PBB.DLP.SFTP.TEXT:
PUT //SAP.PBB.DLP.C001.TEXT  DLP001.CSV
PUT //SAP.PBB.DLP.C002.TEXT  DLP002.CSV
PUT //SAP.PBB.DLP.C003.TEXT  DLP003.CSV
PUT //SAP.PBB.DLP.C004.TEXT  DLP004.CSV
PUT //SAP.PBB.DLP.C005.TEXT  DLP005.CSV
PUT //SAP.PBB.DLP.C006.TEXT  DLP006.CSV





