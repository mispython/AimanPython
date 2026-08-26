From:	Muhammad Khairul Izham Bin Mohd Jamil/ITNOC/PBB/PBBG
To:	Malcolm Jones Al Joackim/ITNOC/PBB/PBBG@PBBG, Aiman Iqbal Bin Ibrahim/ITNOC/PBB/PBBG@PBBG
Cc:	Chong Chow Sin/ITNOC/PBB/PBBG@PBBG, Hoo Mow Khoon/ITNOC/PBB/PBBG@PBBG, Lisa Cheong Danial/ITNOC/PBB/PBBG@PBBG, Mohd Aizat Bin Zulkifli/ITNOC/PBB/PBBG@PBBG, Teh Chooi Hwa/ITNOC/PBB/PBBG@PBBG, Yu Fan Chen/ITNOC/PBB/PBBG@PBBG
Date:	26/08/2026 04:53 PM
Subject:	Re: Python Migration] Deploy program changes into Azure DevOps PRD (A2026-00039171)


Dear Aiman,

Job failed on server svdwh001 due to insufficient space. Please housekeep the path /sas/pythonITD/ and retry.

Deployed=7417 files, Errors=7 files. Refer below:
error writing '/sas/pythonITD/mis/.scannerwork/ir/python/ir_000000124.ir': No space left on device
cp: cannot create regular file '/sas/pythonITD/mis/.scannerwork/ucfg2/python/ucfg_EDW_TRANSFORMATION_SFORMATION_py15.ucfg': No space left on device
cp: cannot create regular file '/sas/pythonITD/mis/.scannerwork/ucfg2/python/ucfg_EIBDOPBL_B_EIBDOPBL_py10.ucfg': No space left on device
cp: cannot create regular file '/sas/pythonITD/mis/.scannerwork/ucfg2/python/ucfg_EIMNOSTE_B_EIMNOSTE_py16.ucfg': No space left on device
cp: cannot create regular file '/sas/pythonITD/mis/.scannerwork/ucfg2/python/ucfg_GET_BATCH_DATE_BATCH_DATE_py13.ucfg': No space left on device
cp: cannot create regular file '/sas/pythonITD/mis/.scannerwork/ucfg2/python/ucfg_REPTDATE_socket___init___1.ucfg': No space left on device

Thank you.

Regards,
Muhammad Khairul Izham
ITD-IMPCC
ext 8227




From:	Malcolm Jones Al Joackim/ITNOC/PBB/PBBG
To:	Aiman Iqbal Bin Ibrahim/ITNOC/PBB/PBBG@PBBG, Hoo Mow Khoon/ITNOC/PBB/PBBG@PBBG
Cc:	Chong Chow Sin/ITNOC/PBB/PBBG@PBBG, Lisa Cheong Danial/ITNOC/PBB/PBBG@PBBG, Mohd Aizat Bin Zulkifli/ITNOC/PBB/PBBG@PBBG, Muhammad Khairul Izham Bin Mohd Jamil/ITNOC/PBB/PBBG@PBBG, Teh Chooi Hwa/ITNOC/PBB/PBBG@PBBG, Yu Fan Chen/ITNOC/PBB/PBBG@PBBG
Date:	26/08/2026 04:40 PM
Subject:	Python Migration] Deploy program changes into Azure DevOps PRD (A2026-00039171)


Dear Aiman,

Deployment failed for Release 34.

Please check and revert.

Regards
Malcolm




