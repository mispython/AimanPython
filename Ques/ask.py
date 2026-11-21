//FTPTOP50 JOB (0216,0000,90,OPER),'FTP WAREHOURE',                     
//         CLASS=A,REGION=64M,NOTIFY=&SYSUID,                           
//         MSGCLASS=X                                                   
/*JOBPARM S=S1M2                                                        
//****************************************************************      
//**  FTP TO RISK MANAGEMENT, PUBLIC BANK                               
//**  THIS JOB WILL RUN AFTER EIMRMHPD                                  
//****************************************************************      
//*----------------------------------------------                       
//* FTP SIBC DATASET TO EDW                                             
//*----------------------------------------------                       
//RUNSFTP  EXEC COZBATCH                                                
//CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(EDW#SFTP)                 
//           DD *                                                       
lzopts servercp=$servercp,notrim,overflow=trunc,mode=text               
lzopts linerule=$lr                                                     
cd /stgsrcsys/host/ftpfiles                                             
PUT //HRP2.FLAT.STAFF.INCTIVE.BR       BRSTAF.TXT                       
PUT //HRP2.FLAT.STAFF.INCTIVE.HO       HOSTAF.TXT                       
PUT //HRP2.FLAT.STAFF.INCTIVE.REGOFF   RGSTAF.TXT                       
PUT //SAP.PBB.INDTOP50.TEXT            INDTOP50.TXT                     
PUT //SAP.PBB.CORTOP50.TEXT            CORTOP50.TXT                     
PUT //SAP.PBB.TOP50.TEXT               TOP50.TXT                        
EOB                                                                     
