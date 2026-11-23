//******************************************************************
//* FTP HOST DATASETS TO DATA REPORT REPOSITORY SYSTEM (DRR)
//******************************************************************
//*%OPC SCAN
//RUNSFTP  EXEC COZBATCH
//CMD.SYSUT1 DD DISP=SHR,DSN=OPER.PBB.PARMLIB(DRR#SFTP)
//           DD *
lzopts servercp=$servercp,notrim,overflow=trunc,mode=text
lzopts linerule=$lr
cd TD/INTERBANK/NLF
put //SAP.PBB.NLF.BEHAVE.TEXT(0)  BEHAVE@%ODD.%OMM.%OYY..TXT
cd /TD/PIBB/NLF
put //SAP.PIBB.NLF.BEHAVE.TEXT(0)  IBEHAVE@%ODD.%OMM.%OYY..TXT
EOB
