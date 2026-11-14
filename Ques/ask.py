//FTPFMSGL JOB MSGCLASS=X,MSGLEVEL=(2,1),USER=OPCC                      
/*JOBPARM S=S1M2                                                        
//*                                                                     
//* 5TH OF THE MONTH                                                    
//* SMR2018-3800(DISCONTINUE INTERFACE TO OLD FMS SRV (192.168.85.82)   
//*                                                                     
//*%OPC SCAN                                                            
//*%OPC SETVAR TOYYYY=(OYYYY-12CD)                                      
//*%OPC SETVAR TOMM=(OMM-12CD)                                          
//**********************************************************************
//DELETE  EXEC PGM=IEFBR14                                              
//DEL01   DD DSN=SAP.PBB.EGFILE.GIA,                                    
//           DISP=(MOD,DELETE,DELETE),                                  
//           SPACE=(CYL,(0))                                            
//DEL02   DD DSN=SAP.PBB.EGFILE.SIP,                                    
//           DISP=(MOD,DELETE,DELETE),                                  
//           SPACE=(CYL,(0))                                            
//DEL03   DD DSN=SAP.PBB.EGFILE.BANCA,                                  
//           DISP=(MOD,DELETE,DELETE),                                  
//           SPACE=(CYL,(0))                                            
//DEL04   DD DSN=SAP.PBB.EGFILE.UT,                                     
//           DISP=(MOD,DELETE,DELETE),                                  
//           SPACE=(CYL,(0))                                            
//DEL05   DD DSN=SAP.PBB.EGFILE.RWA,                                    
//           DISP=(MOD,DELETE,DELETE),                                  
//           SPACE=(CYL,(0))                                            
//DEL06   DD DSN=SAP.PBB.EGFILE.IRWA,                                   
//           DISP=(MOD,DELETE,DELETE),                                  
//           SPACE=(CYL,(0))                                            
//DEL07   DD DSN=SAP.PBB.FMGL0963.TEXT.EDW,                             
//           DISP=(MOD,DELETE,DELETE),                                  
//           SPACE=(CYL,(0))                                            
//DEL08   DD DSN=SAP.PBB.FMGL0966.TEXT.EDW,                             
//           DISP=(MOD,DELETE,DELETE),                                  
//           SPACE=(CYL,(0))                                            
//DEL09   DD DSN=SAP.PBB.FMGL0958.TEXT.EDW,                             
//           DISP=(MOD,DELETE,DELETE),                                  
//           SPACE=(CYL,(0))                                            
//DEL10   DD DSN=SAP.PBB.FMGL0959.TEXT.EDW,                             
//           DISP=(MOD,DELETE,DELETE),                                  
//           SPACE=(CYL,(0))                                            
//DEL11   DD DSN=SAP.PBB.FMGL0961.TEXT.EDW,                             
//           DISP=(MOD,DELETE,DELETE),                                  
//           SPACE=(CYL,(0))                                            
//DEL12   DD DSN=SAP.PBB.FMGL0962.TEXT.EDW,                             
//           DISP=(MOD,DELETE,DELETE),                                  
//           SPACE=(CYL,(0))                                            
//DEL13   DD DSN=SAP.PBB.FMGL0930.TEXT.EDW,                             
//           DISP=(MOD,DELETE,DELETE),                                  
//           SPACE=(CYL,(0))                                            
//DEL14   DD DSN=SAP.PBB.FMGL0946.TEXT.EDW,                             
//           DISP=(MOD,DELETE,DELETE),                                  
//           SPACE=(CYL,(0))                                            
//*-----------------------------------------                            
//* GET FROM EG F:\FTP                                                  
//*-----------------------------------------                            
//RUNSFTP  EXEC COZBATCH                                                
//CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(EDW#SFTP)                 
//           DD *                                                       
lzopts servercp=$servercp,notrim,overflow=trunc,mode=text               
lzopts linerule=$lr                                                     
lzopts lrecl=500,recfm=fb,blksize=0,space=cyl.5.1                       
cd /stgsrcsys/host/ftpfiles                                             
GET DWH_NOACCT_GIA_.TXT //!SAP.PBB.EGFILE.GIA                           
GET DWH_NOACCT_SIP_.TXT //!SAP.PBB.EGFILE.SIP                           
GET DWH_NOACCT_BANCA_.TXT //!SAP.PBB.EGFILE.BANCA                       
GET DWH_NOACCT_UT_.TXT //!SAP.PBB.EGFILE.UT                             
GET DWH_RWA_PBB_.TXT //!SAP.PBB.EGFILE.RWA                              
GET DWH_RWA_PIBB_.TXT //!SAP.PBB.EGFILE.IRWA                            
lzopts lrecl=500,recfm=fb,blksize=0,space=cyl.150.80                    
GET FMGL0963.txt    //!SAP.PBB.FMGL0963.TEXT.EDW                        
GET FMGL0966.txt    //!SAP.PBB.FMGL0966.TEXT.EDW                        
lzopts lrecl=500,recfm=fb,blksize=0,space=cyl.1.0                       
GET FMGL0958.txt    //!SAP.PBB.FMGL0958.TEXT.EDW                        
GET FMGL0959.txt    //!SAP.PIBB.FMGL0959.TEXT.EDW                       
GET FMGL0961.txt    //!SAP.PBB.FMGL0961.TEXT.EDW                        
GET FMGL0962.txt    //!SAP.PIBB.FMGL0962.TEXT.EDW                       
lzopts lrecl=500,recfm=fb,blksize=0,space=cyl.10.10                     
GET FMGL0930.txt    //!SAP.PBB.FMGL0930.TEXT.EDW                        
GET FMGL0946.txt    //!SAP.PBB.FMGL0946.TEXT.EDW                        
EOB                                                                     
//*---------------------------------------------------------------------
//* NEW FMS SERVER 172.22.43.140                                        
//*---------------------------------------------------------------------
//RUNSFTP  EXEC COZBATCH                                                
//CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(COZ#FMS1)                 
//           DD *                                                       
lzopts servercp=$servercp,notrim,overflow=trunc,mode=text               
lzopts linerule=$lr                                                     
CD /SAS/SOURCE/TEMP                                                     
PUT //SAP.PBB.FMGL0930.TEXT.EDW  DWH_NOACCT_LOANS_%TOYYYY.%TOMM..TXT    
PUT //SAP.PBB.FMGL0946.TEXT.EDW DWH_NOACCT_FCYDEP_%TOYYYY.%TOMM..TXT    
PUT //SAP.PBB.FMGL0958.TEXT.EDW  DWH_NOACCT_NID_PBB_%TOYYYY.%TOMM..TXT  
PUT //SAP.PIBB.FMGL0959.TEXT.EDW DWH_NOACCT_NID_PIBB_%TOYYYY.%TOMM..TXT 
PUT //SAP.PBB.FMGL0961.TEXT.EDW  DWH_NOACCT_STDEP_PBB_%TOYYYY.%TOMM..TXT
PUT //SAP.PIBB.FMGL0962.TEXT.EDW   \                                    
    DWH_NOACCT_STDEP_PIBB_%TOYYYY.%TOMM..TXT                            
PUT //SAP.PBB.FMGL0963.TEXT.EDW  DWH_APPROVE_AA_%TOYYYY.%TOMM..TXT      
PUT //SAP.PBB.FMGL0966.TEXT.EDW  DWH_REJECTED_AA_%TOYYYY.%TOMM..TXT     
PUT //SAP.PBB.EGFILE.RWA        DWH_NPLAMT_PBB_%TOYYYY.%TOMM..TXT       
PUT //SAP.PBB.EGFILE.RWA        DWH_NPLAVG_PBB_%TOYYYY.%TOMM..TXT       
PUT //SAP.PBB.EGFILE.RWA        DWH_NPLCNT_PBB_%TOYYYY.%TOMM..TXT       
PUT //SAP.PBB.EGFILE.RWA        DWH_NPLAMT_PIBB_%TOYYYY.%TOMM..TXT      
PUT //SAP.PBB.EGFILE.RWA        DWH_NPLAVG_PIBB_%TOYYYY.%TOMM..TXT      
PUT //SAP.PBB.EGFILE.RWA        DWH_NPLCNT_PIBB_%TOYYYY.%TOMM..TXT      
PUT //SAP.PBB.EGFILE.RWA        DWH_RWA_PBB_%TOYYYY.%TOMM..TXT          
PUT //SAP.PBB.EGFILE.IRWA       DWH_RWA_PIBB_%TOYYYY.%TOMM..TXT         
CD /SASPBB/PBB_SOURCE                                                   
PUT //SAP.PBB.FMGL0941.TEXT(0)  DWH_NOACCT_DEP_%TOYYYY.%TOMM..TXT       
PUT //SAP.PBB.EGFILE.GIA        DWH_NOACCT_GIA_%TOYYYY.%TOMM..TXT       
PUT //SAP.PBB.EGFILE.SIP        DWH_NOACCT_SIP_%TOYYYY.%TOMM..TXT       
PUT //SAP.PBB.EGFILE.UT         DWH_NOACCT_UT_%TOYYYY.%TOMM..TXT        
PUT //SAP.PBB.EGFILE.BANCA      DWH_NOACCT_BANCA_%TOYYYY.%TOMM..TXT     
EOB                                                                     
/*                                                                      
