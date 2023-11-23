 # #-----------------
 # imort pyodbc
 # print(pyodbc.drivers())
 # #import libraries
 # from datetime import timedelta
 # from columns_list import MEDICAL_PRODUCT_COL,CLIENT_RH_COL_HIST_PHARMACY_CLAIM_OTH_ACC_COL
 # from constants_list import teradata_user,teradat_password,dsc_name
 # from datetime import date,datetime
 # import sql_query as query
 # cnxn=pyodbc.connect(f"DSN={dsc_name};UID={teradata_user};PWD={teradat_password}")
 #----------import date---------
from datetime import date
today=date.today()
print("today=",today)
date=today
today=date
print("-----------date------\n",date)
# #---------------------------------------------------------------------logic 1----------------------
# #----import mpc_table-------
import pandas as pd
import numpy as np
MEDICAL_PRODUCT_CURRENT=pd.read_excel(r"C:\jaya sri\project 2\all_input_files.xlsx",sheet_name="MEDICAL_PRODUCT_CURRENT")
print("--------------MEDICAL_PRODUCT_CURRENT-------------\n",MEDICAL_PRODUCT_CURRENT)

#----------------------------import NDC_DCR_LIST_______table---
NDC_DCR_LIST=pd.read_excel(r"C:\jaya sri\project 2\all_input_files.xlsx",sheet_name="NDC_DCR_LIST")
print("----------NDC_DCR_LIST------\n",NDC_DCR_LIST)

#--------adding dummy data----
MEDICAL_PRODUCT_CURRENT_dummy=MEDICAL_PRODUCT_CURRENT[['PRODUCT_SERVICE_ID']].iloc[:20,0:]
NDC_DCR_LIST_dummy=pd.concat([MEDICAL_PRODUCT_CURRENT_dummy,NDC_DCR_LIST],ignore_index=True)
NDC_DCR_LIST_dummy['NDC_NBR']=NDC_DCR_LIST_dummy[['PRODUCT_SERVICE_ID']]

NDC_DCR_LIST_dummy=NDC_DCR_LIST_dummy['NDC_NBR'].tolist()
MEDICAL_PRODUCT_CURRENT['LONGTERM_ACUTE']=""
MEDICAL_PRODUCT_CURRENT['LONGTERM_ACUTE']=np.where(MEDICAL_PRODUCT_CURRENT['PRODUCT_SERVICE_ID'].isin(NDC_DCR_LIST_dummy),"Y",MEDICAL_PRODUCT_CURRENT['LONGTERM_ACUTE'])

print("-----------MEDICAL_PRODUCT_CURRENT----\n",MEDICAL_PRODUCT_CURRENT.shape)
print("--------------MEDICAL_PRODUCT_CURRENT--\n",MEDICAL_PRODUCT_CURRENT[['LONGTERM_ACUTE']])


# --------------------------------check the data it is wrong--------
# FIN_OPP_MPC=MEDICAL_PRODUCT_CURRENT[MEDICAL_PRODUCT_CURRENT.LONGTERM_ACUTE=="y"]
FIN_OPP_MPC=MEDICAL_PRODUCT_CURRENT[['PRODUCT_SERVICE_ID','LONGTERM_ACUTE']]
print("-------------FIN_OPP_MPC--\n",FIN_OPP_MPC.shape)
print(FIN_OPP_MPC[['PRODUCT_SERVICE_ID','LONGTERM_ACUTE']])
print("------------FIN_OPP_MPC\n",FIN_OPP_MPC.shape)

#-------------------------------------------------------logic 2---------------------------------------------
#----------import client_rh tabel-------
CLIENT_RH=pd.read_excel(r"C:\jaya sri\project 2\all_input_files.xlsx",sheet_name="CLIENT_RH")
print("-----------CLIENT_RH-----\n",CLIENT_RH.shape)

claims_list=['A','1','2','3','4','5','6','7']
FIN_OPP_SUBMIT_TYPE_GROUPS=CLIENT_RH
FIN_OPP_SUBMIT_TYPE_GROUPS_v1=FIN_OPP_SUBMIT_TYPE_GROUPS[FIN_OPP_SUBMIT_TYPE_GROUPS.CLAIM_SUBMISSION_TYPE_CDE.isin(claims_list)]

print("------------FIN_OPP_SUBMIT_TYPE_GROUPS_v1---\n",FIN_OPP_SUBMIT_TYPE_GROUPS_v1.shape)

# --------------------------------------------------------logic 3---------------
 # #-----importing a oth table---
import pandas as pd
import numpy as np
oth=pd.read_excel(r"C:\jaya sri\project 2\all_input_files.xlsx",sheet_name="HIST_PHARMACY_CLAIM_OTH_ACC")
# oth.columns=oth_col
print("------------oth-----\n",oth)
print("------------oth-----\n",oth.columns)
#---------column rename---
# oth.rename(columns={"PHCY_PRODUCT_SERVICE_ID":"PRODUCT_SERVICE_ID"},inplace=True)

# #-------importing fin_opp_mpc_table---------
FIN_OPP_MPC=pd.read_excel(r"C:\jaya sri\project 2\all_input_files.xlsx",sheet_name="FIN_OPP_MPC_DRUG")
print("-----FIN_OPP_MPC\n",FIN_OPP_MPC.columns)
print("-----FIN_OPP_MPC\n",FIN_OPP_MPC.shape)

#----------importing a work_table3----
WORK_TABLE_3=pd.read_excel(r"C:\jaya sri\project 2\all_input_files.xlsx",sheet_name="WORK_TABLE3")
# WORK_TABLE_3.columns=WORK_TABLE_3_col
print("----------WORK_TABLE_3----\n",WORK_TABLE_3)
print("----------WORK_TABLE_3----\n",WORK_TABLE_3.columns)
#--------------rename a column name--
oth.rename(columns={"PHCY_PRODUCT_SERVICE_ID":"PRODUCT_SERVICE_ID"},inplace=True)

#--------------------replace----------
oth_list=oth.PRODUCT_SERVICE_ID[:20].tolist()
FIN_OPP_MPC_list=FIN_OPP_MPC.PRODUCT_SERVICE_ID[:20].tolist()
oth_list1=oth.replace(oth_list,FIN_OPP_MPC_list)

# -------------merge two tables-----------
OTH_FIN_merge=oth_list1.merge(FIN_OPP_MPC,how="inner",on='PRODUCT_SERVICE_ID')
print("------OTH_FIN_merge------\n",OTH_FIN_merge)
print("------OTH_FIN_merge------\n",OTH_FIN_merge.shape)
#
# #----------------merge tables with work table 3-------------
WORK_TABLE_3_list=WORK_TABLE_3.PATIENT_ID[:40].tolist()
OTH_FIN_merge_list=OTH_FIN_merge.PATIENT_ID[:40].tolist()
OTH_FIN_merge_list1=OTH_FIN_merge.replace(OTH_FIN_merge_list,WORK_TABLE_3_list)
#
# #----------------merge-----------------
OTH_FIN_TABLE3_merge=OTH_FIN_merge_list1.merge(WORK_TABLE_3,how="inner",on="PATIENT_ID")
print("-----OTH_FIN_TABLE3_merge-------\n",OTH_FIN_TABLE3_merge)
print("-----OTH_FIN_TABLE3_merge-------\n",OTH_FIN_TABLE3_merge.shape)
# print("-----OTH_FIN_TABLE3_merge-------\n",OTH_FIN_TABLE3_merge.columns)
#
# # # ___________creating a column name--------
OTH_FIN_TABLE3_merge["WEDGET_FLAG"]=""
print("-----OTH_FIN_TABLE3_merge---columns--\n",OTH_FIN_TABLE3_merge.columns)
print("-----OTH_FIN_TABLE3_merge---columns--\n",OTH_FIN_TABLE3_merge.shape)
print("-----OTH_FIN_TABLE3_merge-------\n",OTH_FIN_TABLE3_merge.shape)

#------------ading dummy data-----------
FIN_OPP_MPC_dummy=OTH_FIN_TABLE3_merge[:100]
OTH_FIN_TABLE3_merge_dummy=pd.concat([OTH_FIN_TABLE3_merge,FIN_OPP_MPC_dummy])
print("------------OTH_FIN_TABLE3_merge_dummy------\n",OTH_FIN_TABLE3_merge_dummy.shape)
print("------------OTH_FIN_TABLE3_merge_dummy------\n",OTH_FIN_TABLE3_merge_dummy.dtypes)

#---------------------------------converting data types-------create a new column--------------
OTH_FIN_TABLE3_merge_dummy['MAINTAINANCE_DRUG_CDE_v1']=""
FIN_OPP_MPC_dummy['MAINTAINANCE_DRUG_CDE_v1']=0
OTH_FIN_TABLE3_merge_dummy['MAINTAINANCE_DRUG_CDE_v1']=OTH_FIN_TABLE3_merge_dummy['MAINTAINANCE_DRUG_CDE_v1'].astype('int64')

#-----------------------------------------------------------------------------------------------------
# FIN_OPP_MPC_dummy['RX_REFILL_NBR']=0
FIN_OPP_MPC_dummy['MAINTAINANCE_DRUG_CDE_v1']=0
# FIN_OPP_MPC_dummy['SPECIALITY_PHCY_IND']=0
# FIN_OPP_MPC_dummy['FILL_DAYS_SUPPLY_QTY']=15
# FIN_OPP_MPC_dummy['TRANSACTION_TYPE_CDE']='31'
#-----------------------case conditions-------------------
OTH_FIN_TABLE3_merge_dummy["LONGTERM_ACUTE"]=np.where(OTH_FIN_TABLE3_merge_dummy["LONGTERM_ACUTE"]=="Y","N",OTH_FIN_TABLE3_merge_dummy["LONGTERM_ACUTE"])

OTH_FIN_TABLE3_merge_dummy["WEDGET_FLAG"]=np.where((OTH_FIN_TABLE3_merge_dummy["RX_REFILL_NBR"]==0) &
                                             (OTH_FIN_TABLE3_merge_dummy["MAINTAINANCE_DRUG_CDE_v1"]==0) &
                                             (OTH_FIN_TABLE3_merge_dummy["SPECIALITY_PHCY_IND"]!=1),"Y",OTH_FIN_TABLE3_merge_dummy["WEDGET_FLAG"])

OTH_FIN_TABLE3_merge_dummy["WEDGIT_FLAG"] = np.where((OTH_FIN_TABLE3_merge_dummy["FILL_DAYS_SUPPLY_QTY"]<14) &
                                               (OTH_FIN_TABLE3_merge_dummy["RX_REFILL_NBR"]==0) &
                                               (OTH_FIN_TABLE3_merge_dummy["MAINTAINANCE_DRUG_CDE_v1"]==1) | (OTH_FIN_TABLE3_merge_dummy["SPECIALITY_PHCY_IND"]==1),"Y","N")

print("----------OTH_FIN_TABLE3_merge_dummy----after case conditions----\n",OTH_FIN_TABLE3_merge_dummy.shape)
print("----------OTH_FIN_TABLE3_merge_dummy----after case conditions----\n",OTH_FIN_TABLE3_merge_dummy.dtypes)
print("------OTH_FIN_TABLE3_merge_dummy---caseoutput--\n",
      OTH_FIN_TABLE3_merge_dummy[OTH_FIN_TABLE3_merge_dummy.LONGTERM_ACUTE=='Y'][['PRODUCT_SERVICE_ID','PATIENT_ID','LONGTERM_ACUTE','WEDGIT_FLAG']])


#-----------------------------------------------------------------------logic 4 a -------------------------------------------------------------------------------------------------
# UPDATE CLM
# MEDB_CLAIMPULL
# FIN_OPP_SUBMIT_TYPE_GROUPS

#------------------repalce------------------------
#--------------------replace----------
FIN_OPP_SUBMIT_TYPE_GROUPS_list=FIN_OPP_SUBMIT_TYPE_GROUPS.PRODUCT_SERVICE_ID[:20].tolist()
MEDB_CLAIMPULL_list=MEDB_CLAIMPULL.PRODUCT_SERVICE_ID[:20].tolist()
MEDB_CLAIMPULL_list1=MEDB_CLAIMPULL.replace(FIN_OPP_SUBMIT_TYPE_GROUPS_list,MEDB_CLAIMPULL_list)

#--------------rename a column name--
MEDB_CLAIMPULL.rename(columns={"PHCY_PRODUCT_SERVICE_ID":"PRODUCT_SERVICE_ID"},inplace=True)

#--------------rename a column name--
MEDB_CLAIMPULL.rename(columns={"CLAIM_SUBMISSION_TYPE_CDE":"CLAIM_SUBMISSION_TYPE_CDE_V1"},inplace=True)

#--------------------------merge-----------------------
medb_fin_opp_submit_merge=MEDB_CLAIMPULL_list1.merge(FIN_OPP_SUBMIT_TYPE_GROUPS,how="inner",on='GROUP_OPERATIONAL_ID')
print("----------medb_fin_opp_submit_merge--\n-",medb_fin_opp_submit_merge.shape)
print("----------medb_fin_opp_submit_merge--\n-",medb_fin_opp_submit_merge)

# #----------------set columns---
# df_new=df.drop("Relation",axis="columns")
medb_fin_opp_submit_merge_v1=medb_fin_opp_submit_merge.drop("CLAIM_SUBMISSION_TYPE_CDE",axis="columns")
#--------------rename a column name--
medb_fin_opp_submit_merge.rename(columns={"CLAIM_SUBMISSION_TYPE_CDE_V1":"CLAIM_SUBMISSION_TYPE_CDE"},inplace=True)

#------------------------------------------------------logic 4 b --------------
#----------------------updating set columns-------------------
# MEDB_CLAIMPULL
claim_list=['A','1','2','3','4','5','6','7']
MEDB_CLAIMPULL_v1=MEDB_CLAIMPULL[(~MEDB_CLAIMPULL["PRODUCT_SERVICE_ID"].isin(claim_list),inplace=True)]

#-----------------------------------------------logic 4 c-----------------------
#-----------------------------------------------logic 4 c-----------------------
 #-----import table-----------
PROFILE_PRODUCT_RH=pd.read_sql(query.PROFILE_PRODUCT_RH,cnxn)
PROFILE_PRODUCT_RH_columns=PROFILE_PRODUCT_RH_COL
print("------------PROFILE_PRODUCT_RH---\,",PROFILE_PRODUCT_RH.shape)
print("------------PROFILE_PRODUCT_RH---\,",PROFILE_PRODUCT_RH.dtypes)
print("------------PROFILE_PRODUCT_RH---\,",PROFILE_PRODUCT_RH)

#--------------rename a column name--
MEDB.rename(columns={"PHCY_PRODUCT_SERVICE_ID":"PRODUCT_SERVICE_ID"},inplace=True)

---#-----------------replace----------
FIN_OPP_MPC_list=FIN_OPP_MPC.[['PRODUCT_SERVICE_ID']][:20].tolist()
MEDB_CLAIMPULL_list=MEDB_CLAIMPULL.[['PRODUCT_SERVICE_ID']][:20].tolist()
MEDB_CLAIMPULL_list1=MEDB_CLAIMPULL.replace(FIN_OPP_MPC_list,MEDB_CLAIMPULL_list)


#------------------merge-------
medb_fin_opp_merge=MEDB_CLAIMPULL_list1.merge(FIN_OPP_MPC,how="inner",on=['PRODUCT_SERVICE_ID'])
print("-----------medb_fin_opp_merge--\n",medb_fin_opp_merge.shape)
print("-----------medb_fin_opp_merge--\n",medb_fin_opp_merge)

--------------rename a column name--
MEDB.rename(columns={"ADJUD_GROUP_OPERATIONAL_ID":"GROUP_OPERATIONAL_ID"},inplace=True)

---#-----------------replace----------
PROFILE_PRODUCT_RH_list=PROFILE_PRODUCT_RH.[['GROUP_OPERATIONAL_ID']][:20].tolist()
medb_fin_opp_merge_list=medb_fin_opp_merge.[['GROUP_OPERATIONAL_ID']][:20].tolist()
medb_fin_opp_merge_v1=medb_fin_opp_merge.replace(PROFILE_PRODUCT_RH_list,medb_fin_opp_merge_list)


#------------------merge--2 -----
medb_fin_opp_product_rh_merge=medb_fin_opp_merge_v1.merge(PROFILE_PRODUCT_RH,how="inner",on=['GROUP_OPERATIONAL_ID)'])
print("-----------medb_fin_opp_product_rh_merge--\n",medb_fin_opp_product_rh_merge.shape)
print("-----------medb_fin_opp_product_rh_merge--\n",medb_fin_opp_product_rh_merge)

#-----------------------where-------------
medb_fin_opp_product_rh_merge=medb_fin_opp_product_rh_merge[medb_fin_opp_product_rh_merge['SPECIALITY_PHCY_IND']==1]
medb_fin_opp_product_rh_merge=medb_fin_opp_product_rh_merge[medb_fin_opp_product_rh_merge['END_EFF_DTE']>CURRENT_DATE]
medb_fin_opp_product_rh_merge=medb_fin_opp_product_rh_merge[medb_fin_opp_product_rh_merge['PREFERERED_OPTIMIZIN_PARTIC_CDE']=='0']
medb_fin_opp_product_rh_merge=medb_fin_opp_product_rh_merge[medb_fin_opp_product_rh_merge['PREFERERED_OPTIMIZIN_EFF_DTE']<CURRENT_DATE]




#----------------------------------------------------------------------logic 4 d -------------------------------------------------------------------
 # #-----------------update-------------
 #-----import table-----------
SERVICE_MESSEAGE=pd.read_sql(query.SERVICE_MESSEAGE,cnxn)
SERVICE_MESSEAGE_columns=SERVICE_MESSEAGE_COL
print("------------SERVICE_MESSEAGE---\,",SERVICE_MESSEAGE.shape)
print("------------SERVICE_MESSEAGE---\,",SERVICE_MESSEAGE.dtypes)
print("------------SERVICE_MESSEAGE---\,",SERVICE_MESSEAGE)

#--------------rename a column name--
SERVICE_MESSEAGE.rename(columns={"SERVICE_REQUEST_ID":"PHCY_CLAIM_ID"},inplace=True)

--------------------replace----------
SERVICE_MESSEAGE_list=SERVICE_MESSEAGE.[['PRODUCT_SERVICE_ID','PHCY_CLAIM_ID']][:20].tolist()
MEDB_CLAIMPULL_list=MEDB_CLAIMPULL.[['PRODUCT_SERVICE_ID','PHCY_CLAIM_ID']][:20].tolist()
MEDB_CLAIMPULL_list1=MEDB_CLAIMPULL.replace(SERVICE_MESSEAGE_list,MEDB_CLAIMPULL_list)

#------------------merge-------
medb_service_merge=MEDB_CLAIMPULL_list1.merge(SERVICE_MESSEAGE,how="inner",on=['PATIENT_ID','PHCY_CLAIM_ID'])
print("-----------medb_service_merge--\n",medb_service_merge.shape)
print("-----------medb_service_merge--\n",medb_service_merge)

#------------------where conditions--------------
medb_service_merge_V1=medb_service_merge[medb_service_merge["PHCY_CLAIM_ID"]=='CL']
medb_service_merge_V1=medb_service_merge[medb_service_merge["MESSEAGE_TYPE_CDE"]==0]
medb_service_merge_V1=medb_service_merge[medb_service_merge["PRODUCT_ID"]==212]
medb_service_merge_V1=medb_service_merge[medb_service_merge["PRODUCT_SRC_IND"]==1]


#----------------------------------set------------------------
# SERVICE_MESSEAGE_PRODUCT_ID=PRODUCT_ID
# SERVICE_MESSEAGE_PRODUCT_SRC_CDE=PRODUCT_SRC_CDE
#
# #--------------rename a column name--
# medb_service_merge_V1.rename(columns={"SERVICE_REQUEST_ID":"PHCY_CLAIM_ID"},inplace=True)

#-----------------------------------------------------------------------logic 4 e ------------------------------------------------------------
#----------------------------import table-------------
VALIDATED_PATIENT_FALSE_POSIVE=pd.read_sql(query.VALIDATED_PATIENT_FALSE_POSIVE,cnxn)
VALIDATED_PATIENT_FALSE_POSIVE.columns=VALIDATED_PATIENT_FALSE_POSIVE_COL
print("-----VALIDATED_PATIENT_FALSE_POSIVE--\n",VALIDATED_PATIENT_FALSE_POSIVE.shape)
print("-----VALIDATED_PATIENT_FALSE_POSIVE--\n",VALIDATED_PATIENT_FALSE_POSIVE)

#-------------------import table-----------------------------
VALIDATED_PATIENTS=pd.read_sql(query.VALIDATED_PATIENTS,cnxn)
VALIDATED_PATIENTS.columns=VALIDATED_PATIENTS_COL
print("-----------VALIDATED_PATIENTS--\n",VALIDATED_PATIENTS.shape)
print("-----------VALIDATED_PATIENTS--\n",VALIDATED_PATIENTS)

#-------------------------filtering--------------------
MEDB_CLAIMPULL_list_v1=MEDB_CLAIMPULL['PATIENT_ID'].tolist()
VALIDATED_PATIENT_FALSE_POSIVE_v1=VALIDATED_PATIENT_FALSE_POSIVE['PATIENT_ID'].tolist()
VALIDATED_PATIENTS_v1=VALIDATED_PATIENTS['PATIENT_ID'].tolist()

filter1=VALIDATED_PATIENT_FALSE_POSIVE_v1.isin(MEDB_CLAIMPULL_list_v1)
filter2=VALIDATED_PATIENTS_v1.isin(MEDB_CLAIMPULL_list_v1)

ap=pd.concat([filter1,filter2])

final=MEDB_CLAIMPULL["PATIENT_ID"].notin(ap["PATIENT_ID"])


#-----------------------------logic 4 completed---------------------
#-------------------------------------------logic---------------

 #-------------------------------------------logic 5---------------------------------
#------------------logic 5 A--------------------
MEDB_WEDGET_WORK=MEDB_CLAIMPULL[MEDB_CLAIMPULL['WEDGET_FLAG']=='Y']
print("---------MEDB_WEDGET_WORK--\n",MEDB_WEDGET_WORK.shape)
print("---------MEDB_WEDGET_WORK--\n",MEDB_WEDGET_WORK)
print("---------MEDB_WEDGET_WORK--\n",MEDB_WEDGET_WORK.dtypes)


#-----------------------logic 5 B-----------------
#------------update ----MEDB_WEDGET_WORK--------------
MEDB_WEDGET_WORK["STATUS_CDE"]=np.where(MEDB_WEDGET_WORK["SERVICED_DTE"]<DATE-90,"D",MEDB_WEDGET_WORK["STATUS_CDE"])


#-----------------------------logic 5 C---------------------
MEDB_WEDGET_WORK_v1=MEDB_WEDGET_WORK[MEDB_WEDGET_WORK["STATUS_CDE"]=="I"]
MEDB_WEDGET_WORK_v1_filter=MEDB_WEDGET_WORK_v1[["PATIENT_ID","GCN_NBR"]]


#-------------------------------------------------------logic 5 D--------------------------------------------------------------------------------------














