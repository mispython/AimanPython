//FTPMRMRP JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=64M                     
//*----------------------------------------------                       
//* FTP HOST DATASETS TO PBB DATAWAREHOUSE SERVER                       
//*----------------------------------------------                       
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
//*PUT //SAP.PIBB.EIIMRM01.TEXT       EIIMRM01@%OMM.%OYY..TXT           
//*PUT //SAP.PIBB.EIIMRM02.TEXT       EIIMRM02@%OMM.%OYY..TXT           
//*PUT //SAP.PIBB.EIIMRM03.TEXT       EIIMRM03@%OMM.%OYY..TXT           
//*PUT //SAP.PIBB.EIIMRM04.TEXT       EIIMRM04@%OMM.%OYY..TXT           
//*EOB                                                                  




//FTPWRPTS JOB MSGCLASS=X,REGION=4M,TIME=1440,USER=OPCC                 
//*%OPC SCAN                                                            
//**********************************************************************
//* SMR Ref 2018-0050                                                   
//* WEF DATE 16/01/2018                                                 
//**********************************************************************
//RUNSFTP  EXEC COZBATCH                                                
//OUTPUT   DD SYSOUT=*                                                  
//SYSPRINT DD SYSOUT=*                                                  
//CMD.SYSUT1 DD *                                                       
export PASSWD_DSN='OPER.PBB.CONTROL(MPHRSFTP)'                          
$coz_bin/cozsftp  $ssh_opts -b- hrdloansftp@MPNSVFIL001.pbb.pbg.my <<EOB
lzopts servercp=$servercp,notrim,overflow=trunc,mode=text               
lzopts linerule=$lr                                                     
cd WeeklyReport                                                         
put //SAP.PBB.EIBWSTAF(0)        EIBWSTAF_%ODD.%OMM.%OYY..txt           
put //SAP.PIBB.EIIWSTAF(0)       EIIWSTAF_%ODD.%OMM.%OYY..txt           
EOB                                                                     
/*                                                                                                                                           
