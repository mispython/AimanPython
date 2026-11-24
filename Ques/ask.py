//FTPELIST JOB MSGCLASS=X,MSGLEVEL=(2,1),NOTIFY=OPER,                   
//         CLASS=A,TIME=1440                                            
//FTP      EXEC PGM=FTP,REGION=4096K,                                   
//      PARM=('192.168.7.46 (EXIT TIMEOUT 60')                          
//NETRC    DD DSN=OPER.PBB.CONTROL(PASCRISP),DISP=SHR                   
//INPUT    DD *                                                         
PUT 'SAP.BANK.LOANLIST.TEXT'  LOANLIST                                  
PUT 'SAP.BANK.ODLIST.TEXT'    ODLIST                                    
PUT 'SAP.PIBB.LOANLIST.TEXT'  ILOANLIST                                 
PUT 'SAP.PIBB.ODLIST.TEXT'    IODLIST                                   
quit                                                                    
//OUTPUT   DD SYSOUT=*                                                  
//SYSPRINT DD SYSOUT=*  
