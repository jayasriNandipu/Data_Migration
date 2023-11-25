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
FIN_OPP_SUBMIT_TYPE_GROUPS.rename(columns={"CLAIM_SUBMISSION_TYPE_CDE":"CLAIM_SUBMISSION_TYPE_CDE_V1"},inplace=True)

#--------------------------merge-----------------------
medb_fin_opp_submit_merge=MEDB_CLAIMPULL_list1.merge(FIN_OPP_SUBMIT_TYPE_GROUPS,how="inner",on='GROUP_OPERATIONAL_ID')
print("----------medb_fin_opp_submit_merge--\n-",medb_fin_opp_submit_merge.shape)
print("----------medb_fin_opp_submit_merge--\n-",medb_fin_opp_submit_merge)

# #----------------set columns---
# df_new=df.drop("Relation",axis=1)
#------------------------------drop column name------------------
medb_fin_opp_submit_merge_v1=medb_fin_opp_submit_merge.drop(["CLAIM_SUBMISSION_TYPE_CDE"],axis=1)

#--------------rename a column name--
medb_fin_opp_submit_merge.rename(columns={"CLAIM_SUBMISSION_TYPE_CDE_V1":"CLAIM_SUBMISSION_TYPE_CDE"},inplace=True)

#------------------------------------------------------logic 4 b --------------
#----------------------updating set columns-------------------
# MEDB_CLAIMPULL
claim_list=['A','1','2','3','4','5','6','7']
MEDB_CLAIMPULL_v1=MEDB_CLAIMPULL[(~MEDB_CLAIMPULL["CLAIMS_SUBMISSION_TYPE_CDE"].isin(claim_list),inplace=True)]

#-----------------------------------------------logic 4 c-----------------------
#-----------------------------------------------logic 4 c-----------------------
#  #-----import table-----------
# PROFILE_PRODUCT_RH=pd.read_sql(query.PROFILE_PRODUCT_RH,cnxn)
# PROFILE_PRODUCT_RH_columns=PROFILE_PRODUCT_RH_COL
# print("------------PROFILE_PRODUCT_RH---\,",PROFILE_PRODUCT_RH.shape)
# print("------------PROFILE_PRODUCT_RH---\,",PROFILE_PRODUCT_RH.dtypes)
# print("------------PROFILE_PRODUCT_RH---\,",PROFILE_PRODUCT_RH)
#
# #--------------rename a column name--
# MEDB.rename(columns={"PHCY_PRODUCT_SERVICE_ID":"PRODUCT_SERVICE_ID"},inplace=True)

---#-----------------replace----------
FIN_OPP_MPC_list=FIN_OPP_MPC.[['PRODUCT_SERVICE_ID']][:20].tolist()
MEDB_CLAIMPULL_list=MEDB_CLAIMPULL.[['PRODUCT_SERVICE_ID']][:20].tolist()
MEDB_CLAIMPULL_list1=MEDB_CLAIMPULL.replace(MEDB_CLAIMPULL_list,FIN_OPP_MPC_list)


#------------------merge-------
medb_fin_opp_merge=MEDB_CLAIMPULL_list1.merge(FIN_OPP_MPC,how="inner",on=['PRODUCT_SERVICE_ID'])
print("-----------medb_fin_opp_merge--\n",medb_fin_opp_merge.shape)
print("-----------medb_fin_opp_merge--\n",medb_fin_opp_merge)

--------------rename a column name--
MEDB.rename(columns={"ADJUD_GROUP_OPERATIONAL_ID":"GROUP_OPERATIONAL_ID"},inplace=True)

---#-----------------replace----------
# PROFILE_PRODUCT_RH_list=PROFILE_PRODUCT_RH.[['GROUP_OPERATIONAL_ID']][:20].tolist()
# medb_fin_opp_merge_list=medb_fin_opp_merge.[['GROUP_OPERATIONAL_ID']][:20].tolist()
# medb_fin_opp_merge_v1=medb_fin_opp_merge.replace(PROFILE_PRODUCT_RH_list,medb_fin_opp_merge_list)
#
#
# #------------------merge--2 -----
# medb_fin_opp_product_rh_merge=medb_fin_opp_merge_v1.merge(PROFILE_PRODUCT_RH,how="inner",on=['GROUP_OPERATIONAL_ID)'])
# print("-----------medb_fin_opp_product_rh_merge--\n",medb_fin_opp_product_rh_merge.shape)
# print("-----------medb_fin_opp_product_rh_merge--\n",medb_fin_opp_product_rh_merge)

#-----------------------where-------------
medb_fin_opp_product_rh_merge=medb_fin_opp_product_rh_merge[(medb_fin_opp_product_rh_merge['SPECIALITY_PHCY_IND']==1) &
                                                            # (medb_fin_opp_product_rh_merge["END_EFF_DTE"]>CURRENT_DATE) &
                                                            # (medb_fin_opp_product_rh_merge["PREFERERED_OPTIMIZIN_PARTIC_CDE"]=='0') &
                                                            # (medb_fin_opp_product_rh_merge["PREFERERED_OPTIMIZIN_EFF_DTE"]<CURRENT_DATE)]





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

#-----------------rename------
MEDB_CLAIMPULL.rename(columns={"SERVICE_MSG_PRODUCT_ID":"PRODUCT_ID_V1"},inplace=True)
MEDB_CLAIMPULL.rename(columns={"SERVICE_MSG_PRODUCT_SRC_CDE":"PRODUCT_SRC_CDE_V1"},inplace=True)

# --------------------replace----------
# SERVICE_MESSEAGE_list=SERVICE_MESSEAGE.PRODUCT_SERVICE_ID[:20].tolist()
# MEDB_CLAIMPULL_list=MEDB_CLAIMPULL.PRODUCT_SERVICE_ID[:20].tolist()
# SERVICE_MESSEAGE_list=SERVICE_MESSEAGE.PHCY_CLAIM_ID[:20].tolist()
# MEDB_CLAIMPULL_list=MEDB_CLAIMPULL.PHCY_CLAIM_ID[:20].tolist()
# MEDB_CLAIMPULL_list1=MEDB_CLAIMPULL.replace(SERVICE_MESSEAGE_list,MEDB_CLAIMPULL_list)

#------------------merge-------
medb_service_merge=MEDB_CLAIMPULL_list1.merge(SERVICE_MESSEAGE,how="inner",on=['PATIENT_ID','PHCY_CLAIM_ID'])
print("-----------medb_service_merge--\n",medb_service_merge.shape)
print("-----------medb_service_merge--\n",medb_service_merge)

#------------------where conditions--------------
medb_service_merge_V1=medb_service_merge[(medb_service_merge["PHCY_CLAIM_ID"]=='CL') &
                                         (medb_service_merge["MESSEAGE_TYPE_CDE"]==0) &
                                         (medb_service_merge["PRODUCT_ID"]==212) &
                                         (medb_service_merge["PRODUCT_SRC_IND"]==1)]

#----------------------------------set------------------------
#-----------------------drop columns--------------------
medb_service_merge_V1_filter=medb_service_merge_V1.drop(["PRODUCT_ID"],axis=1)
medb_service_merge_V1_filter=medb_service_merge_V1.drop(["PRODUCT_SRC_CDE"],axis=1)


# #--------------rename a column name--
medb_service_merge_V1_filter.rename(columns={"PRODUCT_ID_V1":"PRODUCT_ID"},inplace=True)
medb_service_merge_V1_filter.rename(columns={"PRODUCT_SRC_CDE_V1":"PRODUCT_SRC_CDE"},inplace=True)

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
MEDB_CLAIMPULL_v1=MEDB_CLAIMPULL[MEDB_CLAIMPULL['WEDGET_FLAG']=='Y']
MEDB_CLAIMPULL_v2=MEDB_CLAIMPULL_v1[["PATIENT_ID","PHCY_PRODUCT_SERVI CE_ID","GCN_NBR","SERVICED_DTE","I"]]
MEDB_WEDGET_WORK=MEDB_CLAIMPULL_v2
print("---------MEDB_WEDGET_WORK--\n",MEDB_WEDGET_WORK.shape)
print("---------MEDB_WEDGET_WORK--\n",MEDB_WEDGET_WORK)
print("---------MEDB_WEDGET_WORK--\n",MEDB_WEDGET_WORK.dtypes)

#-----------------------logic 5 B-----------------
#------------update ----MEDB_WEDGET_WORK--------------
MEDB_WEDGET_WORK["STATUS_CDE"]=np.where(MEDB_WEDGET_WORK["SERVICED_DTE"]<DATE-90,"D",MEDB_WEDGET_WORK["STATUS_CDE"])

 # -----------------logic5c-------------------
# -----------------logic5c-------------------
import pandas as pd
# MEDB_WEDGET_WORK=pd.read_excel(r"C:\jaya sri\project 4\Business logic\InputFiles_UPDATED.xlsx",sheet_name="MEDB_WEDGET_WORK")
# print("----MEDB_WEDGET_WORK-----------------------------\n",MEDB_WEDGET_WORK.shape)
# print("----MEDB_WEDGET_WORK-----------------------------\n",MEDB_WEDGET_WORK.columns)
# print("----MEDB_WEDGET_WORK-----------------------------\n",MEDB_WEDGET_WORK)

MEDB_WEDGET_WORK_status = MEDB_WEDGET_WORK[MEDB_WEDGET_WORK["STATUS_CDE"]=="I"]
MEDB_WEDGET_WORK_status_v1 = MEDB_WEDGET_WORK_status[["PATIENT_ID","GCN_NBR"]]
print("---MEDB_WEDGET_WORK_status_v1--\n",MEDB_WEDGET_WORK_status_v1.shape)
print("---MEDB_WEDGET_WORK_status_v1--\n",MEDB_WEDGET_WORK_status_v1)
print("---MEDB_WEDGET_WORK_status_v1--\n",MEDB_WEDGET_WORK_status_v1.columns)

MEDB_WEDGET_WORK_new = MEDB_WEDGET_WORK[["PATIENT_ID","GCN_NBR"]]
MEDB_WEDGET_WORK_new["flag"]=" "
print("---MEDB_WEDGET_WORK_new--\n",MEDB_WEDGET_WORK_new.shape)
print("---MEDB_WEDGET_WORK_new--\n",MEDB_WEDGET_WORK_new)
print("---MEDB_WEDGET_WORK_new--\n",MEDB_WEDGET_WORK_new.columns)


MEDB_WEDGET_WORK_count = MEDB_WEDGET_WORK_new.groupby(["PATIENT_ID","GCN_NBR"]).count()
print("------MEDB_WEDGET_WORK_count--\m",MEDB_WEDGET_WORK_count.shape)

merge = MEDB_WEDGET_WORK.merge(MEDB_WEDGET_WORK_count, how="inner",on=["PATIENT_ID","GCN_NBR"])
print("---merge--\n",merge.shape)
print("---merge--\n",merge)
print("---merge--\n",merge.columns)

merge2 = MEDB_WEDGET_WORK.merge(merge, how="inner", on="PHCY_CLAIM_ID")
print("---merge2--\n",merge2.shape)
print("---merge2--\n",merge2)
print("---merge2--\n",merge2.columns)

final_output = merge2[merge2["STATUS_CDE_x"] == "D"]
print("--final_output--\n-",final_output.shape)
print("--final_output--\n-",final_output)
print("--final_output--\n-",final_output.columns)
print("----final_output----\n",final_output.STATUS_CDE_x.unique())

 # ---------------------------------logic5c oth------------------------
 # tables-----------
 # HIST_PHARMACY_OTH_CLAIM
 # FIN_OPP_MPC
 # MEDB_WIDGET_WORK
# ------------------------RENAME---------------------
 HIST_PHARMACY_OTH_CLAIM.rename(columns={'PHCY_PRODUCT_SERVICE_ID': 'PRODUCT_SERVICE_ID'}, inplace=True)

  # ---------------------MERGE1--------------------
 OTH_FIN_MERGE = HIST_PHARMACY_OTH_CLAIM.merge(FIN_OPP_MPC,how="ineer",on="PRODUCT_SERVICE_ID")

 # -------------------MERGE2-----------------------
 OTH_FIN_WIDGET_MERGE = OTH_FIN_MERGE.merge(MEDB_WIDGET_WORK,how="ineer",on=["PATIENT_ID","GCN_NBR"])

 # -------------------------UPDATE WIDGET---------------------
 OTH_FIN_WIDGET_MERGE_lst = OTH_FIN_WIDGET_MERGE['PHCY_CLAIM_ID'].tolist

 MEDB_WIDGET_WORK["WIDGET"] = np.where(MEDB_WIDGET_WORK['PHCY_CLAIM_ID'].isin(OTH_FIN_WIDGET_MERGE_lst), STATUS_CDE='D',
                                       MEDB_WIDGET_WORK["WIDGET"])

 # ---------------------------5TH BWP---------------------------
 # HIST_PHARMACY_BWP_CLAIM
 # FIN_OPP_MPC
 # MEDB_WIDGET_WORK
 # ----------------------RENAME-------------------
 HIST_PHARMACY_BWP_CLAIM.rename(columns={'PHCY_PRODUCT_SERVICE_ID': 'PRODUCT_SERVICE_ID'}, inplace=True)

 # ----------------------MERGE1-----------------------------------
 BWP_FIN_MERGE = HIST_PHARMACY_BWP_CLAIM.merge(FIN_OPP_MPC, how="ineer", on="PRODUCT_SERVICE_ID")

 # -----------------------MERGE2-----------------------
 BWP_FIN_WIDGET_MERGE = BWP_FIN_MERGE.merge(MEDB_WIDGET_WORK, how="ineer", on=["PATIENT_ID", "GCN_NBR"])

 # -------------------------UPDATE WIDGET---------------------
 BWP_FIN_WIDGET_MERGE_lst = BWP_FIN_WIDGET_MERGE['PHCY_CLAIM_ID'].tolist
 MEDB_WIDGET_WORK["WIDGET"] = np.where(MEDB_WIDGET_WORK['PHCY_CLAIM_ID'].isin(BWP_FIN_WIDGET_MERGE_lst), STATUS_CDE='D',
                                       MEDB_WIDGET_WORK["WIDGET"])

 # ----------------------gm--------------------------
 # --------------------bva------------------------
 # --------------------uhg--------------------
 # ---------------------fep--------------------
 # ------------------current_pharmacy claim------------------------------
 # CURR_PHARMACY_CLAIM1
 # FIN_OPP_SUBMIT_TYPE
 # FIN_OPP_MPC
 # MEDB_WIDGET_WORK
 # ---------------------RENAME----------------------
 CURR_PHARMACY_CLAIM1.rename(columns={'ELIG_GROUP_OPERATIONAL_ID': 'GROUP_OPERATIONAL_ID'}, inplace=True)
 # -----------------MERGE1-------------------
 CURR_PHARMACY_SUBMIT_TYPE_MERGE = CURR_PHARMACY_CLAIM1.merge(FIN_OPP_SUBMIT_TYPE, how='inner',
                                                              on='GROUP_OPERATIONAL_ID')
 # ---------------------RENAME----------------------
 CURR_PHARMACY_SUBMIT_TYPE_MERGE.rename(columns={'PHCY_PRODUCT_SERVICE_ID': 'PRODUCT_SERVICE_ID'}, inplace=True)
 # ------------------MERGE2----------------------
 CURR_PHARMACY_SUBMIT_TYPE_FIN_MERGE = CURR_PHARMACY_SUBMIT_TYPE_MERGE.merge(FIN_OPP_MPC, how='inner',
                                                                             on='PRODUCT_SERVICE_ID')
 # ------------------MERGE3----------------------
 CURR_PHARMACY_SUBMIT_TYPE_FIN_WIDGET_MERGE = CURR_PHARMACY_SUBMIT_TYPE_FIN_MERGE.merge(MEDB_WIDGET_WORK, how="ineer",
                                                                                        on=["PATIENT_ID", "GCN_NBR"])
 # -------------------------UPDATE WIDGET---------------------
 CURR_PHARMACY_SUBMIT_TYPE_FIN_WIDGETV1 = CURR_PHARMACY_SUBMIT_TYPE_FIN_WIDGET_MERGE['PHCY_CLAIM_ID'].tolist
 MEDB_WIDGET_WORK["WIDGET"] = np.where(MEDB_WIDGET_WORK['PHCY_CLAIM_ID'].isin(CURR_PHARMACY_SUBMIT_TYPE_FIN_WIDGETV1),
                                       STATUS_CDE='D', MEDB_WIDGET_WORK["WIDGET"])

 # ---------------------5D-----------------------------
 # MEDB_CLAIMPULL
 # MEDB_WIDGET_WORK

 MEDB_CLAIMPULL_V1 = MEDB_CLAIMPULL[["PHCY_CLAIM_ID", "PATIENT_ID", "GCN_NBR"]]
 MEDB_WIDGET_WORK_V1 = MEDB_WIDGET_WORK[["PHCY_CLAIM_ID", "PATIENT_ID", "GCN_NBR"]]
 MEDB_CLAIMPULL["STATUS_CDE"] = np.where(MEDB_CLAIMPULL.MEDB_CLAIMPULL_V1.isin(MEDB_WIDGET_WORK_V1), "D",
                                         MEDB_CLAIMPULL["STATUS_CDE"])

#--------------------------------------5 D completed------------------------------
 #---------------------------------6 A started-----------------------------
# ---------column rename---
CURRENT_PHARMACY_CLAIM_1.rename(columns={"PHCY_PRODUCT_SERVICE_ID":"PRODUCT_SERVICE_ID"},inplace=True)

#--------------------replace----------
CURRENT_PHARMACY_CLAIM_1_list=CURRENT_PHARMACY_CLAIM_1.PRODUCT_SERVICE_ID[:5].tolist()
FIN_OPP_MPC_list=FIN_OPP_MPC.PRODUCT_SERVICE_ID[:5].tolist()
CURRENT_PHARMACY_CLAIM_1_list_v1=CURRENT_PHARMACY_CLAIM_1.replace(CURRENT_PHARMACY_CLAIM_1_list,FIN_OPP_MPC_list)

 #---------------------merge 1----------------
pahrmacy_claim_fin_opp_merge=CURRENT_PHARMACY_CLAIM_1_list_v1.merge(FIN_OPP_MPC,how="inner",on="PRODUCT_SERVICE_ID")
print("----------pahrmacy_claim_fin_opp_merge---\n",pahrmacy_claim_fin_opp_merge.shape)
print("----------pahrmacy_claim_fin_opp_merge---\n",pahrmacy_claim_fin_opp_merge)

# - ------column rename - --
pahrmacy_claim_fin_opp_merge.rename(columns={"ELIG_GROUP_OPERATIONAL_ID": "GROUP_OPERATIONAL_ID"}, inplace=True)

#--------------------replace----------
FIN_OPP_SUBMIT_TYPE_GROUPS_list=FIN_OPP_SUBMIT_TYPE_GROUPS.GROUP_OPERATIONAL_ID[:5].tolist()
pahrmacy_claim_fin_opp_merge_list=pahrmacy_claim_fin_opp_merge.GROUP_OPERATIONAL_ID[:5].tolist()
pahrmacy_claim_fin_opp_merge_list_v1=pahrmacy_claim_fin_opp_merge.replace(pahrmacy_claim_fin_opp_merge_list,FIN_OPP_SUBMIT_TYPE_GROUPS_list)

 #-------------merge 2-------------
pharmacy_fin_op_submit_merge=pahrmacy_claim_fin_opp_merge_list_v1.merge(FIN_OPP_SUBMIT_TYPE_GROUPS,how="inner",on="GROUP_OPERATIONAL_ID")
print("-----pharmacy_fin_op_submit_merge--\n",pharmacy_fin_op_submit_merge.shape)
print("-----pharmacy_fin_op_submit_merge--\n",pharmacy_fin_op_submit_merge)

#------------where conditions---------
pharmacy_fin_op_submit_merge_filters=pharmacy_fin_op_submit_merge[(pharmacy_fin_op_submit_merge["CLAIM_RESERVED_DTE"]=='1800-01-01') &
                                                                  (pharmacy_fin_op_submit_merge["CLAIM_RESERVED_CDE"]==" ")]

print("----------pharmacy_fin_op_submit_merge_filters----\n",pharmacy_fin_op_submit_merge_filters.shape)
print("----------pharmacy_fin_op_submit_merge_filters----\n",pharmacy_fin_op_submit_merge_filters)

#--------------------------------------6 B -----------------------------
# ---------column rename---
CURRENT_PHARMACY_CLAIM_1_DOD.rename(columns={"PHCY_PRODUCT_SERVICE_ID":"PRODUCT_SERVICE_ID"},inplace=True)

#--------------------replace----------
CURRENT_PHARMACY_CLAIM_1_DOD_list=CURRENT_PHARMACY_CLAIM_1_DOD.PRODUCT_SERVICE_ID[:5].tolist()
FIN_OPP_MPC_list=FIN_OPP_MPC.PRODUCT_SERVICE_ID[:5].tolist()
CURRENT_PHARMACY_CLAIM_1_DOD_list_v1=CURRENT_PHARMACY_CLAIM_1_DOD.replace(CURRENT_PHARMACY_CLAIM_1_DOD_list,FIN_OPP_MPC_list)

 #---------------------merge 1----------------
dod_pahrmacy_claim_fin_opp_merge=CURRENT_PHARMACY_CLAIM_1_DOD_list_v1.merge(FIN_OPP_MPC,how="inner",on="PRODUCT_SERVICE_ID")
print("----------dod_pahrmacy_claim_fin_opp_merge---\n",dod_pahrmacy_claim_fin_opp_merge.shape)
print("----------dod_pahrmacy_claim_fin_opp_merge---\n",dod_pahrmacy_claim_fin_opp_merge)

# - ------column rename - --
pahrmacy_claim_fin_opp_merge.rename(columns={"ELIG_GROUP_OPERATIONAL_ID": "GROUP_OPERATIONAL_ID"}, inplace=True)

#--------------------replace----------
FIN_OPP_SUBMIT_TYPE_GROUPS_list=FIN_OPP_SUBMIT_TYPE_GROUPS.GROUP_OPERATIONAL_ID[:5].tolist()
dod_pahrmacy_claim_fin_opp_merge_v1_list=dod_pahrmacy_claim_fin_opp_merge.GROUP_OPERATIONAL_ID[:5].tolist()
dod_pahrmacy_claim_fin_opp_merge_v1=dod_pahrmacy_claim_fin_opp_merge.replace(pahrmacy_claim_fin_opp_merge_list,FIN_OPP_SUBMIT_TYPE_GROUPS_list)

 #-------------merge 2-------------
dod_pahrmacy_claim_fin_opp_groups_merge_v1=dod_pahrmacy_claim_fin_opp_merge_v1.merge(FIN_OPP_SUBMIT_TYPE_GROUPS,how="inner",on="GROUP_OPERATIONAL_ID")
print("-----dod_pahrmacy_claim_fin_opp_groups_merge_v1--\n",dod_pahrmacy_claim_fin_opp_groups_merge_v1.shape)
print("-----dod_pahrmacy_claim_fin_opp_groups_merge_v1--\n",dod_pahrmacy_claim_fin_opp_groups_merge_v1)

#------------where conditions---------
dod_pahrmacy_claim_fin_opp_groups_merge_v1_filters=dod_pahrmacy_claim_fin_opp_groups_merge_v1[(dod_pahrmacy_claim_fin_opp_groups_merge_v1["CLAIM_RESERVED_DTE"]=='1800-01-01') &
                                                                  (dod_pahrmacy_claim_fin_opp_groups_merge_v1["CLAIM_RESERVED_CDE"]==" ")]
print("----------dod_pahrmacy_claim_fin_opp_groups_merge_v1_filters----\n",dod_pahrmacy_claim_fin_opp_groups_merge_v1_filters.shape)
print("----------dod_pahrmacy_claim_fin_opp_groups_merge_v1_filters----\n",dod_pahrmacy_claim_fin_opp_groups_merge_v1_filters)


#----------------------append two tables------------
MEDB_CLAIM_REVERSALS_v1=pd.concat([pharmacy_fin_op_submit_merge_filters,dod_pahrmacy_claim_fin_opp_groups_merge_v1_filters],inplace=True)

print("------------MEDB_CLAIM_REVERSALS_v1---\n",MEDB_CLAIM_REVERSALS_v1.shape)
print("---------------MEDB_CLAIM_REVERSALS_v1---\n",MEDB_CLAIM_REVERSALS_v1)

MEDB_CLAIM_REVERSALS=MEDB_CLAIM_REVERSALS_v1[['PATIENT_ID','PHCY_CLAIM_ID','CLAIM_REVERSED_DTE','CLAIM_REVERSED_CTE']]
print("------------MEDB_CLAIM_REVERSALS---\n",MEDB_CLAIM_REVERSALS.shape)
print("---------------MEDB_CLAIM_REVERSALS---\n",MEDB_CLAIM_REVERSALS)
print("------------MEDB_CLAIM_REVERSALS---\n",MEDB_CLAIM_REVERSALS.columns)

#------------------------logic 6 B completed------------------------------------

#------------------------logic 7 started----------------------------
#---------------------logic 7 A started----------------------
print("------MEDB_CLAIM_PULL--\n",MEDB_CLAIM_PULL.columns)

MEDB_CLM_GRP=MEDB_CLAIM_PULL[["PATIENT_ID","GROUP_OPERATIONAL_ID","N"]]
print("------MEDB_CLM_GRP--\n",MEDB_CLM_GRP.shape)
print("------MEDB_CLM_GRP--\n",MEDB_CLM_GRP)
print("------MEDB_CLM_GRP--\n",MEDB_CLM_GRP.columns)

#----------------update
clm_grp_member_current_merge=MEDB_CLM_GRP.merge(MEMBER_CURRENT,how="inner",on=["PATIENT_ID","GROUP_OPERATIONAL_ID"]
print("----------clm_grp_member_current_merge---\n",clm_grp_member_current_merge.shape)
print("----------clm_grp_member_current_merge---\n",clm_grp_member_current_merge)

#-----where condition----
clm_grp_member_current_merge_v1=clm_grp_member_current_merge[(clm_grp_member_current_merge["EFF_DTE"]<DATE+1) &
                                                           (clm_grp_member_current_merge["END_DTE"]>DATE) &
                                                           (clm_grp_member_current_merge["END_EFF_DTE"]>DATE) &
                                                             (clm_grp_member_current_merge["CURRENT_OPERATIONAL_ROW_IND"]=="Y")]

MEDB_CLM_GRP_update=clm_grp_member_current_merge_v1[clm_grp_member_current_merge_v1["ELIG_FLAG"]=="Y"]

print("----------MEDB_CLM_GRP_update---\n",MEDB_CLM_GRP_update.shape)
print("----------MEDB_CLM_GRP_update---\n",MEDB_CLM_GRP_update)


#----------------update
clm_grp_dod_member_current_merge=MEDB_CLM_GRP.merge(DOD_MEMBER_CURRENT,how="inner",on=["PATIENT_ID","GROUP_OPERATIONAL_ID"]
print("----------clm_grp_dod_member_current_merge---\n",clm_grp_dod_member_current_merge.shape)
print("----------clm_grp_dod_member_current_merge---\n",clm_grp_dod_member_current_merge)

#-----where condition----
clm_grp_dod_member_current_merge_v1=clm_grp_dod_member_current_merge[(clm_grp_dod_member_current_merge["EFF_DTE"]<DATE+1) &
                                                           (clm_grp_dod_member_current_merge["END_DTE"]>DATE) &
                                                           (clm_grp_dod_member_current_merge["END_EFF_DTE"]>DATE) &
                                                             (clm_grp_dod_member_current_merge["CURRENT_OPERATIONAL_ROW_IND"]=="Y")]

MEDB_CLM_GRP_update2=clm_grp_dod_member_current_merge_v1[clm_grp_dod_member_current_merge_v1["ELIG_FLAG"]=="Y"]
print("----------MEDB_CLM_GRP_update2---\n",MEDB_CLM_GRP_update2.shape)
print("----------MEDB_CLM_GRP_update2---\n",MEDB_CLM_GRP_update2)


MEDB_CLM_GRP=pd.concat([MEDB_CLM_GRP_update,MEDB_CLM_GRP_update2],ignore_index=True)
print("----------MEDB_CLM_GRP---\n",MEDB_CLM_GRP.shape)
print("----------MEDB_CLM_GRP---\n",MEDB_CLM_GRP)
 #-------------------7 a completed----

#-----------------7 b started------
MEMBER_CURRENT_lst=MEMBER_CURRENT.PATIENT_ID[:5].tolist()
MEDB_CLM_GRP_lst=MEDB_CLM_GRP.PATIENT_ID[:5].tolist()
MEMBER_CURRENT_v1=MEMBER_CURRENT.replace(MEMBER_CURRENT_lst,MEDB_CLM_GRP_lst)

#----merge1-------
current_grp_merge=MEMBER_CURRENT_v1.merge(MEDB_CLM_GRP,how='inner',on='PATIENT_ID')

#------------------------WHERE CONDITIONS----------------
current_grp_merge=current_grp_merge[(current_grp_merge["EFF_DTE"]<DATE+1) &
                                                           (current_grp_merge["END_DTE"]>DATE) &
                                                           (current_grp_merge["END_EFF_DTE"]>DATE) &
                                                             (current_grp_merge["CURRENT_OPERATIONAL_ROW_IND"]=="Y")]











