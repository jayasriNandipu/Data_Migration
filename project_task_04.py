# #-----------------
# imort pyodbc
# print(pyodbc.drivers())
#import libraries
from datetime import timedelta
# from columns_list import MEDICAL_PRODUCT_COL,CLIENT_RH_COL_HIST_PHARMACY_CLAIM_OTH_ACC_COL
# from constants_list import teradata_user,/radat_password,dsc_name
from datetime import date,datetime
# # import sql_query as query
# cnxn=pyodbc.connect(f"DS/sc_name};UID={teradata_user};PWD={teradat_password}")
# ----------import date---------
from datetime import date

today = date.today()
print("today=",today)
date = today
today = date
print("-----------date------\n",date)
# #---------------------------------------------------------------------logic 1----------------------
#----import mpc_table-------
import pandas as pd
import numpy as np

MEDICAL_PRODUCT_CURRENT = pd.read_excel(r"C:\jaya sri\project 2\all_input_files.xlsx",
                                        sheet_name="MEDICAL_PRODUCT_CURRENT")
print("--------------MEDICAL_PRODUCT_CURRENT-------------\n", MEDICAL_PRODUCT_CURRENT)

# ----------------------------import NDC_DCR_LIST_______table---
NDC_DCR_LIST = pd.read_excel(r"C:\jaya sri\project 2\all_input_files.xlsx", sheet_name="NDC_DCR_LIST")
print("----------NDC_DCR_LIST------\n", NDC_DCR_LIST)

# --------adding dummy data----
MEDICAL_PRODUCT_CURRENT_dummy = MEDICAL_PRODUCT_CURRENT[['PRODUCT_SERVICE_ID']].iloc[:20, 0:]
NDC_DCR_LIST_dummy = pd.concat([MEDICAL_PRODUCT_CURRENT_dummy, NDC_DCR_LIST], ignore_index=True)
NDC_DCR_LIST_dummy['NDC_NBR'] = NDC_DCR_LIST_dummy[['PRODUCT_SERVICE_ID']]

NDC_DCR_LIST_dummy = NDC_DCR_LIST_dummy['NDC_NBR'].tolist()
MEDICAL_PRODUCT_CURRENT['LONGTERM_ACUTE'] = ""
MEDICAL_PRODUCT_CURRENT['LONGTERM_ACUTE'] = np.where(
    MEDICAL_PRODUCT_CURRENT['PRODUCT_SERVICE_ID'].isin(NDC_DCR_LIST_dummy), "Y",
    MEDICAL_PRODUCT_CURRENT['LONGTERM_ACUTE'])

print("-----------MEDICAL_PRODUCT_CURRENT----\n", MEDICAL_PRODUCT_CURRENT.shape)
print("--------------MEDICAL_PRODUCT_CURRENT--\n", MEDICAL_PRODUCT_CURRENT[['LONGTERM_ACUTE']])

# --------------------------------check the data it is wrong--------
# FIN_OPP_MPC=MEDICAL_PRODUCT_CURRENT[MEDICAL_PRODUCT_CURRENT.LONGTERM_ACUTE=="y"]
FIN_OPP_MPC = MEDICAL_PRODUCT_CURRENT[['PRODUCT_SERVICE_ID', 'LONGTERM_ACUTE']]
print("-------------FIN_OPP_MPC--\n", FIN_OPP_MPC.shape)
print(FIN_OPP_MPC[['PRODUCT_SERVICE_ID', 'LONGTERM_ACUTE']])
print("------------FIN_OPP_MPC\n", FIN_OPP_MPC.shape)

# -------------------------------------------------------logic 2---------------------------------------------
# ----------import client_rh tabel-------
CLIENT_RH = pd.read_excel(r"C:\jaya sri\project 2\all_input_files.xlsx", sheet_name="CLIENT_RH")
print("-----------CLIENT_RH-----\n", CLIENT_RH.shape)

claims_list = ['A', '1', '2', '3', '4', '5', '6', '7']
FIN_OPP_SUBMIT_TYPE_GROUPS = CLIENT_RH
FIN_OPP_SUBMIT_TYPE_GROUPS_v1 = FIN_OPP_SUBMIT_TYPE_GROUPS[
    FIN_OPP_SUBMIT_TYPE_GROUPS.CLAIM_SUBMISSION_TYPE_CDE.isin(claims_list)]

print("------------FIN_OPP_SUBMIT_TYPE_GROUPS_v1---\n", FIN_OPP_SUBMIT_TYPE_GROUPS_v1.shape)

# --------------------------------------------------------logic 3---------------
# #-----importing a oth table---
import pandas as pd
import numpy as np

oth = pd.read_excel(r"C:\jaya sri\project 2\all_input_files.xlsx", sheet_name="HIST_PHARMACY_CLAIM_OTH_ACC")
# oth.columns=oth_col
print("------------oth-----\n", oth)
print("------------oth-----\n", oth.columns)
# ---------column rename---
# oth.rename(columns={"PHCY_PRODUCT_SERVICE_ID":"PRODUCT_SERVICE_ID"},inplace=True)

# #-------importing fin_opp_mpc_table---------
FIN_OPP_MPC = pd.read_excel(r"C:\jaya sri\project 2\all_input_files.xlsx", sheet_name="FIN_OPP_MPC_DRUG")
print("-----FIN_OPP_MPC\n", FIN_OPP_MPC.columns)
print("-----FIN_OPP_MPC\n", FIN_OPP_MPC.shape)

# ----------importing a work_table3----
WORK_TABLE_3 = pd.read_excel(r"C:\jaya sri\project 2\all_input_files.xlsx", sheet_name="WORK_TABLE3")
# WORK_TABLE_3.columns=WORK_TABLE_3_col
print("----------WORK_TABLE_3----\n", WORK_TABLE_3)
print("----------WORK_TABLE_3----\n", WORK_TABLE_3.columns)
# --------------rename a column name--
oth.rename(columns={"PHCY_PRODUCT_SERVICE_ID": "PRODUCT_SERVICE_ID"}, inplace=True)

# --------------------replace----------
oth_list = oth.PRODUCT_SERVICE_ID[:20].tolist()
FIN_OPP_MPC_list = FIN_OPP_MPC.PRODUCT_SERVICE_ID[:20].tolist()
oth_list1 = oth.replace(oth_list, FIN_OPP_MPC_list)

# -------------merge two tables-----------
OTH_FIN_merge = oth_list1.merge(FIN_OPP_MPC, how="inner", on='PRODUCT_SERVICE_ID')
print("------OTH_FIN_merge------\n", OTH_FIN_merge)
# print("------OTH_FIN_merge------\n", OTH_FIN_merge.shape)
# #
# #----------------merge tables with work table 3-------------
WORK_TABLE_3_list = WORK_TABLE_3.PATIENT_ID[:40].tolist()
OTH_FIN_merge_list = OTH_FIN_merge.PATIENT_ID[:40].tolist()
OTH_FIN_merge_list1 = OTH_FIN_merge.replace(OTH_FIN_merge_list, WORK_TABLE_3_list)
#
# #----------------merge-----------------
OTH_FIN_TABLE3_merge = OTH_FIN_merge_list1.merge(WORK_TABLE_3, how="inner", on="PATIENT_ID")
print("-----OTH_FIN_TABLE3_merge-------\n", OTH_FIN_TABLE3_merge)
print("-----OTH_FIN_TABLE3_merge-------\n", OTH_FIN_TABLE3_merge.shape)
# print("-----OTH_FIN_TABLE3_merge-------\n",OTH_FIN_TABLE3_merge.columns)
#
# # # ___________creating a column name--------
OTH_FIN_TABLE3_merge["WEDGET_FLAG"] = ""
print("-----OTH_FIN_TABLE3_merge---columns--\n", OTH_FIN_TABLE3_merge.columns)
print("-----OTH_FIN_TABLE3_merge---columns--\n", OTH_FIN_TABLE3_merge.shape)
print("-----OTH_FIN_TABLE3_merge-------\n", OTH_FIN_TABLE3_merge.shape)

# ------------ading dummy data-----------
FIN_OPP_MPC_dummy = OTH_FIN_TABLE3_merge[:100]
OTH_FIN_TABLE3_merge_dummy = pd.concat([OTH_FIN_TABLE3_merge, FIN_OPP_MPC_dummy])
print("------------OTH_FIN_TABLE3_merge_dummy------\n", OTH_FIN_TABLE3_merge_dummy.shape)
print("------------OTH_FIN_TABLE3_merge_dummy------\n", OTH_FIN_TABLE3_merge_dummy.dtypes)

# ---------------------------------converting data types-------create a new column--------------
OTH_FIN_TABLE3_merge_dummy['MAINTAINANCE_DRUG_CDE_v1'] = ""
FIN_OPP_MPC_dummy['MAINTAINANCE_DRUG_CDE_v1'] = 0
OTH_FIN_TABLE3_merge_dummy['MAINTAINANCE_DRUG_CDE_v1'] = OTH_FIN_TABLE3_merge_dummy['MAINTAINANCE_DRUG_CDE_v1'].astype('int64')

# -----------------------------------------------------------------------------------------------------
# FIN_OPP_MPC_dummy['RX_REFILL_NBR']=0
FIN_OPP_MPC_dummy['MAINTAINANCE_DRUG_CDE_v1'] = 0
# FIN_OPP_MPC_dummy['SPECIALITY_PHCY_IND']=0
# FIN_OPP_MPC_dummy['FILL_DAYS_SUPPLY_QTY']=15
# FIN_OPP_MPC_dummy['TRANSACTION_TYPE_CDE']='31'
# -----------------------case conditions-------------------
OTH_FIN_TABLE3_merge_dummy["LONGTERM_ACUTE"] = np.where(OTH_FIN_TABLE3_merge_dummy["LONGTERM_ACUTE"] == "Y", "N",
                                                        OTH_FIN_TABLE3_merge_dummy["LONGTERM_ACUTE"])

OTH_FIN_TABLE3_merge_dummy["WEDGET_FLAG"] = np.where((OTH_FIN_TABLE3_merge_dummy["RX_REFILL_NBR"] == 0) &
                                                     (OTH_FIN_TABLE3_merge_dummy["MAINTAINANCE_DRUG_CDE_v1"] == 0) &
                                                     (OTH_FIN_TABLE3_merge_dummy["SPECIALITY_PHCY_IND"] != 1), "Y",
                                                     OTH_FIN_TABLE3_merge_dummy["WEDGET_FLAG"])

OTH_FIN_TABLE3_merge_dummy["WEDGIT_FLAG"] = np.where((OTH_FIN_TABLE3_merge_dummy["FILL_DAYS_SUPPLY_QTY"] < 14) &
                                                     (OTH_FIN_TABLE3_merge_dummy["RX_REFILL_NBR"] == 0) &
                                                     (OTH_FIN_TABLE3_merge_dummy["MAINTAINANCE_DRUG_CDE_v1"] == 1) | (
                                                                 OTH_FIN_TABLE3_merge_dummy[
                                                                     "SPECIALITY_PHCY_IND"] == 1), "Y", "N")

print("----------OTH_FIN_TABLE3_merge_dummy----after case conditions----\n", OTH_FIN_TABLE3_merge_dummy.shape)
print("----------OTH_FIN_TABLE3_merge_dummy----after case conditions----\n", OTH_FIN_TABLE3_merge_dummy.dtypes)
print("------OTH_FIN_TABLE3_merge_dummy---caseoutput--\n",
      OTH_FIN_TABLE3_merge_dummy[OTH_FIN_TABLE3_merge_dummy.LONGTERM_ACUTE == 'Y'][
          ['PRODUCT_SERVICE_ID', 'PATIENT_ID', 'LONGTERM_ACUTE', 'WEDGIT_FLAG']])

#
# # -----------------------------------------------------------------------logic 4 a -------------------------------------------------------------------------------------------------
# # UPDATE CLM
# # MEDB_CLAIMPULL
# # FIN_OPP_SUBMIT_TYPE_GROUPS
#
# # ------------------repalce------------------------
# --------------------replace----------
FIN_OPP_SUBMIT_TYPE_GROUPS_list = FIN_OPP_SUBMIT_TYPE_GROUPS.PRODUCT_SERVICE_ID[:20].tolist()
MEDB_CLAIMPULL_list = MEDB_CLAIMPULL.PRODUCT_SERVICE_ID[:20].tolist()
MEDB_CLAIMPULL_list1 = MEDB_CLAIMPULL.replace(FIN_OPP_SUBMIT_TYPE_GROUPS_list, MEDB_CLAIMPULL_list)

# --------------rename a column name--
MEDB_CLAIMPULL.rename(columns={"PHCY_PRODUCT_SERVICE_ID": "PRODUCT_SERVICE_ID"}, inplace=True)

# --------------rename a column name--
FIN_OPP_SUBMIT_TYPE_GROUPS.rename(columns={"CLAIM_SUBMISSION_TYPE_CDE": "CLAIM_SUBMISSION_TYPE_CDE_V1"}, inplace=True)

# --------------------------merge-----------------------
medb_fin_opp_submit_merge = MEDB_CLAIMPULL_list1.merge(FIN_OPP_SUBMIT_TYPE_GROUPS, how="inner",
                                                       on='GROUP_OPERATIONAL_ID')
print("----------medb_fin_opp_submit_merge--\n-", medb_fin_opp_submit_merge.shape)
print("----------medb_fin_opp_submit_merge--\n-", medb_fin_opp_submit_merge)

# #----------------set columns---
# df_new=df.drop("Relation",axis=1)
# ------------------------------drop column name------------------
medb_fin_opp_submit_merge_v1 = medb_fin_opp_submit_merge.drop(["CLAIM_SUBMISSION_TYPE_CDE"], axis=1)

# --------------rename a column name--
medb_fin_opp_submit_merge.rename(columns={"CLAIM_SUBMISSION_TYPE_CDE_V1": "CLAIM_SUBMISSION_TYPE_CDE"}, inplace=True)

# ------------------------------------------------------logic 4 b --------------
# ----------------------updating set columns-------------------finopp lo vunna data ni adda cheyalii
MEDB_CLAIMPULL_dummy=MEDB_CLAIMPULL['CLAIMS_SUBMISSION_TYPE_CDE'][:10].tolist()
FIN_OPP_SUBMIT_TYPE_GROUPS_v1_dummy=FIN_OPP_SUBMIT_TYPE_GROUPS_v1['CLAIMS_SUBMISSION_TYPE_CDE'][:10].tolist()
MEDB_CLAIMPULL_list_v1=pd.concat([MEDB_CLAIMPULL_dummy,FIN_OPP_SUBMIT_TYPE_GROUPS_v1_dummy],axis=0)
print("---MEDB_CLAIMPULL_list_v1---\n",MEDB_CLAIMPULL_list_v1['CLAIMS_SUBMISSION_TYPE_CDE'])

claim_list = ['A', '1', '2', '3', '4', '5', '6', '7']
MEDB_CLAIMPULL_v1 = MEDB_CLAIMPULL[(~MEDB_CLAIMPULL["CLAIMS_SUBMISSION_TYPE_CDE"].isin(claim_list),inplace==True)]

# -----------------------------------------------logic 4 c-----------------------
# -----------------------------------------------logic 4 c-----------------------
 #-----import table-----------
PROFILE_PRODUCT_RH=pd.read_sql(query.PROFILE_PRODUCT_RH,cnxn)
PROFILE_PRODUCT_RH_columns=PROFILE_PRODUCT_RH_COL
print("------------PROFILE_PRODUCT_RH---\,",PROFILE_PRODUCT_RH.shape)
print("------------PROFILE_PRODUCT_RH---\,",PROFILE_PRODUCT_RH.dtypes)
print("------------PROFILE_PRODUCT_RH---\,",PROFILE_PRODUCT_RH)

# --------------rename a column name--
MEDB.rename(columns={"PHCY_PRODUCT_SERVICE_ID":"PRODUCT_SERVICE_ID"},inplace=True)

# ---  # -----------------replace----------
FIN_OPP_MPC_list = FIN_OPP_MPC[['PRODUCT_SERVICE_ID']][:20].tolist()
MEDB_CLAIMPULL_list = MEDB_CLAIMPULL[['PRODUCT_SERVICE_ID']][:20].tolist()
MEDB_CLAIMPULL_list1 = MEDB_CLAIMPULL.replace(MEDB_CLAIMPULL_list, FIN_OPP_MPC_list)

# ------------------merge-------
medb_fin_opp_merge = MEDB_CLAIMPULL_list1.merge(FIN_OPP_MPC, how="inner", on=['PRODUCT_SERVICE_ID'])
print("-----------medb_fin_opp_merge--\n", medb_fin_opp_merge.shape)
print("-----------medb_fin_opp_merge--\n", medb_fin_opp_merge)

# --------------rename a column name ------ -
MEDB.rename(columns={"ADJUD_GROUP_OPERATIONAL_ID": "GROUP_OPERATIONAL_ID"}, inplace=True)

# ---  # -----------------replace---
PROFILE_PRODUCT_RH_list=PROFILE_PRODUCT_RH[['GROUP_OPERATIONAL_ID']][:20].tolist()
medb_fin_opp_merge_list=medb_fin_opp_merge[['GROUP_OPERATIONAL_ID']][:20].tolist()
medb_fin_opp_merge_v1=medb_fin_opp_merge.replace(PROFILE_PRODUCT_RH_list,medb_fin_opp_merge_list)


#------------------merge--2 -----
medb_fin_opp_product_rh_merge=medb_fin_opp_merge_v1.merge(PROFILE_PRODUCT_RH,how="inner",on=['GROUP_OPERATIONAL_ID)'])
print("-----------medb_fin_opp_product_rh_merge--\n",medb_fin_opp_product_rh_merge.shape)
print("-----------medb_fin_opp_product_rh_merge--\n",medb_fin_opp_product_rh_merge)

# -----------------------where-------------
medb_fin_opp_product_rh_merge = medb_fin_opp_product_rh_merge[(medb_fin_opp_product_rh_merge['SPECIALITY_PHCY_IND'] == 1) ]
    # (medb_fin_opp_product_rh_merge["END_EFF_DTE"]>CURRENT_DATE) &
    # (medb_fin_opp_product_rh_merge["PREFERERED_OPTIMIZIN_PARTIC_CDE"]=='0') &
    # (medb_fin_opp_product_rh_merge["PREFERERED_OPTIMIZIN_EFF_DTE"]<CURRENT_DATE)]

    # ----------------------------------------------------------------------logic 4 d -------------------------------------------------------------------
    # #-----------------update-------------
    # -----import table-----------
SERVICE_MESSEAGE = pd.read_sql(query.SERVICE_MESSEAGE, cnxn)
SERVICE_MESSEAGE_columns = SERVICE_MESSEAGE_COL
print("------------SERVICE_MESSEAGE---\,", SERVICE_MESSEAGE.shape)
print("------------SERVICE_MESSEAGE---\,", SERVICE_MESSEAGE.dtypes)
print("------------SERVICE_MESSEAGE---\,", SERVICE_MESSEAGE)

# --------------rename a column name--
SERVICE_MESSEAGE.rename(columns={"SERVICE_REQUEST_ID": "PHCY_CLAIM_ID"}, inplace=True)

# -----------------rename------
MEDB_CLAIMPULL.rename(columns={"SERVICE_MSG_PRODUCT_ID": "PRODUCT_ID_V1"}, inplace=True)
MEDB_CLAIMPULL.rename(columns={"SERVICE_MSG_PRODUCT_SRC_CDE": "PRODUCT_SRC_CDE_V1"}, inplace=True)

# --------------------replace----------
# SERVICE_MESSEAGE_list=SERVICE_MESSEAGE.PRODUCT_SERVICE_ID[:20].tolist()
# MEDB_CLAIMPULL_list=MEDB_CLAIMPULL.PRODUCT_SERVICE_ID[:20].tolist()
# SERVICE_MESSEAGE_list=SERVICE_MESSEAGE.PHCY_CLAIM_ID[:20].tolist()
# MEDB_CLAIMPULL_list=MEDB_CLAIMPULL.PHCY_CLAIM_ID[:20].tolist()
# MEDB_CLAIMPULL_list1=MEDB_CLAIMPULL.replace(SERVICE_MESSEAGE_list,MEDB_CLAIMPULL_list)

# ------------------merge-------
medb_service_merge = MEDB_CLAIMPULL_list1.merge(SERVICE_MESSEAGE, how="inner", on=['PATIENT_ID', 'PHCY_CLAIM_ID'])
print("-----------medb_service_merge--\n", medb_service_merge.shape)
print("-----------medb_service_merge--\n", medb_service_merge)

#-=----------adding dummy data-----
medb_service_merge_dummy=medb_service_merge
medb_service_merge_dummy['PHCY_CLAIM_ID']='CL'
medb_service_merge_dummy['MESSEAGE_TYPE_CDE']=0
medb_service_merge_dummy['PRODUCT_ID']=212
medb_service_merge_dummy['PRODUCT_SRC_IND']=1

medb_service_merge_dummy_v1=pd.concat([medb_service_merge,medb_service_merge_dummy])

# ------------------where conditions--------------
medb_service_merge_dummy_v2 = medb_service_merge_dummy_v1[(medb_service_merge_dummy_v1["PHCY_CLAIM_ID"] == 'CL') &
                                           (medb_service_merge_dummy_v1["MESSEAGE_TYPE_CDE"] == 0) &
                                           (medb_service_merge_dummy_v1["PRODUCT_ID"] == 212) &
                                           (medb_service_merge_dummy_v1["PRODUCT_SRC_IND"] == 1)]

# ----------------------------------set------------------------
# -----------------------drop columns--------------------
medb_service_merge_dummy_v2_filter = medb_service_merge_dummy_v2.drop(["SERVICE_MSG_PRODUCT_ID"], axis=1)
medb_service_merge_dummy_v2_filter = medb_service_merge_dummy_v2.drop(["SERVICE_MSG_PRODUCT_SRC_CDE"], axis=1)

# #--------------rename a column name--
medb_service_merge_dummy_v2_filter.rename(columns={"SERVICE_MSG_PRODUCT_ID": "PRODUCT_ID"}, inplace=True)
medb_service_merge_dummy_v2_filter.rename(columns={"SERVICE_MSG_PRODUCT_SRC_CDE": "PRODUCT_SRC_CDE"}, inplace=True)

# -------------------------------------------------------------logic 4 e ------------------------------------------------------------
# ----------------------------import table-------------
VALIDATED_PATIENT_FALSE_POSIVE = pd.read_sql(query.VALIDATED_PATIENT_FALSE_POSIVE, cnxn)
VALIDATED_PATIENT_FALSE_POSIVE.columns = VALIDATED_PATIENT_FALSE_POSIVE_COL
print("-----VALIDATED_PATIENT_FALSE_POSIVE--\n", VALIDATED_PATIENT_FALSE_POSIVE.shape)
print("-----VALIDATED_PATIENT_FALSE_POSIVE--\n", VALIDATED_PATIENT_FALSE_POSIVE)

# -------------------import table-----------------------------
VALIDATED_PATIENTS = pd.read_sql(query.VALIDATED_PATIENTS,cnxn)
VALIDATED_PATIENTS.columns = VALIDATED_PATIENTS_COL
print("-----------VALIDATED_PATIENTS--\n", VALIDATED_PATIENTS.shape)
print("-----------VALIDATED_PATIENTS--\n", VALIDATED_PATIENTS)

# # -------------------------filtering--------------------
# MEDB_CLAIMPULL_list_v1 = MEDB_CLAIMPULL['PATIENT_ID'].tolist()
# VALIDATED_PATIENT_FALSE_POSIVE_v1 = VALIDATED_PATIENT_FALSE_POSIVE['PATIENT_ID'].tolist()
# VALIDATED_PATIENTS_v1 = VALIDATED_PATIENTS['PATIENT_ID'].tolist()
#
# filter1 = VALIDATED_PATIENT_FALSE_POSIVE_v1.isin(MEDB_CLAIMPULL_list_v1)
# filter2 = VALIDATED_PATIENTS_v1.isin(MEDB_CLAIMPULL_list_v1)
#
# ap = pd.concat([filter1, filter2])
#
# final = MEDB_CLAIMPULL["PATIENT_ID"].notin(ap["PATIENT_ID"])

# *************************************or*****************
MEDB_CLAIMPULL_list= MEDB_CLAIMPULL['PATIENT_ID'].tolist()
VALIDATED_lst=VALIDATED_PATIENT_FALSE_POSIVE[VALIDATED_PATIENT_FALSE_POSIVE['PATIENT_ID'].isin(MEDB_CLAIMPULL_list)]
VALIDATED_lst2=VALIDATED_PATIENTS[VALIDATED_PATIENTS['PATIENT_ID'].isin(MEDB_CLAIMPULL_list)]
VALIDATED_V1=pd.concat([VALIDATED_lst,VALIDATED_lst2],ignore_index=True)

VALIDATED_FINAL=MEDB_CLAIMPULL[~MEDB_CLAIMPULL['PATIENT_ID'].isin(VALIDATED_V1)]
print("---VALIDATED_FINAL--\n-,",VALIDATED_FINAL.shape)
#
# # -----------------------------logic 4 completed---------------------
# # -------------------------------------------logic---------------

# -------------------------------------------logic 5---------------------------------
# ------------------logic 5 A--------------------
import pandas as pd
import numpy as np
MEDB_CLAIMPULL=pd.read_excel(r"C:\jaya sri\project 4\Excel files\INPUT_DATA_FILES.xlsx",sheet_name="MEDB_table")
print("------MEDB_CLAIMPULL --shape--\n",MEDB_CLAIMPULL.shape)
print("------MEDB_CLAIMPULL --data--\n",MEDB_CLAIMPULL)
print("------MEDB_CLAIMPULL --columns--\n",MEDB_CLAIMPULL.columns)

MEDB_CLAIMPULL_v1 = MEDB_CLAIMPULL[MEDB_CLAIMPULL['WEDGET_FLAG'] =='Y']
MEDB_CLAIMPULL_v2 = MEDB_CLAIMPULL_v1[["PATIENT_ID", "PHCY_PRODUCT_SERVICE_ID", "GCN_NBR", "SERVICED_DTE","PHCY_CLAIM_ID"]]
MEDB_WEDGET_WORK = MEDB_CLAIMPULL_v2
MEDB_WEDGET_WORK["STATUS_CDE"]="I"
print("---------MEDB_WEDGET_WORK--\n", MEDB_WEDGET_WORK.shape)
print("---------MEDB_WEDGET_WORK--\n", MEDB_WEDGET_WORK)
print("---------MEDB_WEDGET_WORK--\n", MEDB_WEDGET_WORK.columns)
print("-----MEDB_WEDGET_WORK---\n",MEDB_WEDGET_WORK.STATUS_CDE.unique())
print("-----MEDB_WEDGET_WORK----output---\n",MEDB_WEDGET_WORK[["PATIENT_ID","PHCY_PRODUCT_SERVICE_ID","GCN_NBR","SERVICED_DTE"]])
print("------------------MEDB_WEDGET_WORK------------------logic 5 a completed\n",MEDB_WEDGET_WORK)

# -----------------------logic 5 B-----------------
# ------------update ----MEDB_WEDGET_WORK--------------
# from datetime import date,datetime
# TODAY_NEW=date.today()
# TODAY_NEW2=date.today()-timedelta(days=90)
# CURRENT_DATE_DIFF3=pd.DataFram()
# CURRENT_DATE_DIFF3['DATE']=TODAY_NEW
# CURRENT_DATE_DIFF3['DATE']=CURRENT_DATE_DIFF3['DATE'].astype("datetime64[ns")
# current_date5b=today
# # current_date-90-5b=today()-timedelta()
# MEDB_WEDGET_WORK["STATUS_CDE"] = np.where(MEDB_WEDGET_WORK["SERVICED_DTE"] < date - 90, "D",
#                                           MEDB_WEDGET_WORK["STATUS_CDE"])
# print("-----MEDB_WEDGET_WORK----5B----\n",MEDB_WEDGET_WORK.shape)
# print("-----MEDB_WEDGET_WORK----5B----\n",MEDB_WEDGET_WORK)
# print("-----MEDB_WEDGET_WORK----5B----\n",MEDB_WEDGET_WORK.columns)

# # -----------------logic5c-------------------
# # -----------------logic5c-------------------
import pandas as pd
MEDB_WEDGET_WORK=pd.read_excel(r"C:\jaya sri\project 4\Business logic\InputFiles_UPDATED.xlsx",sheet_name="MEDB_WEDGET_WORK")
print("----MEDB_WEDGET_WORK-----------------------------\n",MEDB_WEDGET_WORK.shape)
print("----MEDB_WEDGET_WORK-----------------------------\n",MEDB_WEDGET_WORK.columns)
print("----MEDB_WEDGET_WORK-----------------------------\n",MEDB_WEDGET_WORK)
#----------------logic 5 c-------
MEDB_WEDGET_WORK_status = MEDB_WEDGET_WORK[MEDB_WEDGET_WORK["STATUS_CDE"] == "I"][["PATIENT_ID","GCN_NBR"]]
print("---MEDB_WEDGET_WORK_status--\n", MEDB_WEDGET_WORK_status.shape)
print("---MEDB_WEDGET_WORK_status--\n", MEDB_WEDGET_WORK_status)
print("---MEDB_WEDGET_WORK_status--\n", MEDB_WEDGET_WORK_status.columns)
MEDB_WEDGET_WORK_status_v1=MEDB_WEDGET_WORK_status[["PATIENT_ID","GCN_NBR"]]
MEDB_WEDGET_WORK_status_v1['COUNT']=' '

MEDB_WEDGET_WORK_count = MEDB_WEDGET_WORK_status_v1.groupby(["PATIENT_ID","GCN_NBR"]).count()
print("------MEDB_WEDGET_WORK_count--\n", MEDB_WEDGET_WORK_count.shape)
print("-----MEDB_WEDGET_WORK_count----columns----\n",MEDB_WEDGET_WORK_count.columns)
#-----merge1----
MEDBWORK_MEDBWORK_CNT= MEDB_WEDGET_WORK.merge(MEDB_WEDGET_WORK_count,how="inner",on=["PATIENT_ID","GCN_NBR"])
print("---MEDBWORK_MEDBWORK_CNT--\n", MEDBWORK_MEDBWORK_CNT.shape)
print("---MEDBWORK_MEDBWORK_CNT--\n", MEDBWORK_MEDBWORK_CNT)
print("---MEDBWORK_MEDBWORK_CNT--\n", MEDBWORK_MEDBWORK_CNT.columns)
#----merge2----
medbworkcnt_medbmain= MEDB_WEDGET_WORK.merge(MEDBWORK_MEDBWORK_CNT,how="inner",on="PHCY_CLAIM_ID")
print("---medbworkcnt_medbmain--\n", medbworkcnt_medbmain.shape)
print("---medbworkcnt_medbmain--\n", medbworkcnt_medbmain)
print("---medbworkcnt_medbmain--\n", medbworkcnt_medbmain.columns)

medbworkcnt_medbmain_list=medbworkcnt_medbmain['PHCY_CLAIM_ID'].tolist()

MEDB_WEDGET_WORK['STATUS_CDE']=np.where(MEDB_WEDGET_WORK['PHCY_CLAIM_ID'].isin(medbworkcnt_medbmain_list),"D",MEDB_WEDGET_WORK['STATUS_CDE'])
print("--MEDB_WIDGET_WORK--\n-", MEDB_WEDGET_WORK.shape)
print("--MEDB_WIDGET_WORK--\n-", MEDB_WEDGET_WORK)
print("--MEDB_WIDGET_WORK--\n-", MEDB_WEDGET_WORK.columns)
print("----MEDB_WIDGET_WORK----\n", MEDB_WEDGET_WORK.STATUS_CDE.unique())
print("---------------completed--------\n")
# # ---------------------------------logic5c oth------------------------
print("-------------oth table----------------")
OTH_TABLE=pd.read_excel(r"C:\jaya sri\project 4\Excel files\INPUT_DATA_FILES.xlsx",sheet_name="OTH_TABLE")
print("-----OTH_TABLE----\n",OTH_TABLE.shape)
print("-----OTH_TABLE----\n",OTH_TABLE.columns)

FIN_OPP_MPC=pd.read_excel(r"C:\jaya sri\project 4\Excel files\INPUT_DATA_FILES.xlsx",sheet_name="FIN_OPP_MPC")
print("-----FIN_OPP_MPC---\n",FIN_OPP_MPC.shape)
print("-----FIN_OPP_MPC---\n",FIN_OPP_MPC.columns)

# ------------------------RENAME---------------------
OTH_TABLE.rename(columns={'PHCY_PRODUCT_SERVICE_ID': 'PRODUCT_SERVICE_ID'}, inplace=True)

# ---------------------MERGE1--------------------
OTH_FIN_MERGE = OTH_TABLE.merge(FIN_OPP_MPC,how="inner",on="PRODUCT_SERVICE_ID")
print("-----OTH_FIN_MERGE---\n",OTH_FIN_MERGE.shape)

#-----importing MEDB_WIDGET_WORK--table---
# MEDB_WIDGET_WORK=pd.read_excel(r)
# -------------------MERGE2-----------------------
OTH_FIN_WIDGET_MERGE = OTH_FIN_MERGE.merge(MEDB_WEDGET_WORK,how="inner",on=["PATIENT_ID"])
print("-----OTH_FIN_WIDGET_MERGE--\n",OTH_FIN_WIDGET_MERGE.shape)
print("-----OTH_FIN_WIDGET_MERGE--\n",OTH_FIN_WIDGET_MERGE)
print("-----OTH_FIN_WIDGET_MERGE--\n",OTH_FIN_WIDGET_MERGE.columns)

#----------adding a dummy data-------
OTH_FIN_WIDGET_MERGE_dummy=OTH_FIN_WIDGET_MERGE

OTH_FIN_WIDGET_MERGE_dummy['TRANSACTION_TYPE_CDE']='31'
OTH_FIN_WIDGET_MERGE_dummy['STATUS_CDE']='I'

OTH_FIN_WIDGET_MERGE_dummy_v1=pd.concat([OTH_FIN_WIDGET_MERGE,OTH_FIN_WIDGET_MERGE_dummy])

#----where conditions---
OTH_FIN_WIDGET_MERGE_dummy_v1_list=OTH_FIN_WIDGET_MERGE_dummy_v1[(OTH_FIN_WIDGET_MERGE_dummy_v1['TRANSACTION_TYPE_CDE'].isin(['31','36'])) &
                                                  # (OTH_FIN_WIDGET_MERGE_dummy_v1['SERVICED_DTE_x']>=DATE-90) &
                                                  (OTH_FIN_WIDGET_MERGE_dummy_v1['STATUS_CDE']=='I')]
print("----OTH_FIN_WIDGET_MERGE_dummy_v1_list---\n",OTH_FIN_WIDGET_MERGE_dummy_v1_list.shape)
print("----OTH_FIN_WIDGET_MERGE_dummy_v1_list---\n",OTH_FIN_WIDGET_MERGE_dummy_v1_list.columns)

# OTH_FIN_WIDGET_MERGE_dummy_v1_list_v1=OTH_FIN_WIDGET_MERGE_dummy_v1_list[OTH_FIN_WIDGET_MERGE_dummy_v1_list.PHCY_CLAIM_ID_x!=OTH_FIN_WIDGET_MERGE_dummy_v1_list.PHCY_CLAIM_ID_x]
# # print("-----OTH_FIN_WIDGET_MERGE_dummy_v1_lis/claim---id----\n",OTH_FIN_WIDGET_MERGE_dummy_v1_list_v1.columns)
# print("----MEDB_WEDGET_WORK---\n",MEDB_WEDGET_WORK.columns)
# #
# ---------rename------
MEDB_WEDGET_WORK.rename(columns={'PHCY_CLAIM_ID': 'PHCY_CLAIM_ID_x'}, inplace=True)

# -------------------------UPDATE WIDGET---------------------
OTH_FIN_WIDGET_MERGE_dummy_v1_list_v1_2 = OTH_FIN_WIDGET_MERGE_dummy_v1_list['PHCY_CLAIM_ID_x'].tolist()
MEDB_WEDGET_WORK["STATUS_CDE"] = np.where(MEDB_WEDGET_WORK["PHCY_CLAIM_ID_x"].isin(OTH_FIN_WIDGET_MERGE_dummy_v1_list_v1_2),"D",MEDB_WEDGET_WORK["STATUS_CDE"])
print("-----MEDB_WEDGET_WORK--\n",MEDB_WEDGET_WORK.shape)
print("-----MEDB_WEDGET_WORK--\n",MEDB_WEDGET_WORK[['STATUS_CDE']])
#------------------------OUTPUT TO EXCEL--------------
MEDB_WEDGET_WORK.to_excel(r"C:\jaya sri\project 4\MEDB_WEDGET_WORK_OUTPUT.xlsx",index=False)
# # --------------------------***********-5TH BWP---------------------------
print("--------------------bwp table----------------------")
# #-----importing a table----
BWP_TABLE=pd.read_excel(r"C:\jaya sri\project 4\Excel files\INPUT_DATA_FILES.xlsx",sheet_name="BWP_TABLE")
print("-----BWP_TABLE----\n",BWP_TABLE.shape)
print("-----BWP_TABLE----\n",BWP_TABLE.columns)
# # ----------------------RENAME-------------------
BWP_TABLE.rename(columns={'PHCY_PRODUCT_SERVICE_ID': 'PRODUCT_SERVICE_ID'}, inplace=True)
#
# # ----------------------MERGE1-----------------------------------
BWP_FIN_OPP_MERGE=BWP_TABLE.merge(FIN_OPP_MPC, how="inner",on="PRODUCT_SERVICE_ID")
print("-----BWP_FIN_OPP_MERGE--\n",BWP_FIN_OPP_MERGE.shape)
print("-----BWP_FIN_OPP_MERGE--\n",BWP_FIN_OPP_MERGE)
print("-----BWP_FIN_OPP_MERGE--\n",BWP_FIN_OPP_MERGE.columns)

# # -----------------------MERGE2-----------------------
BWP_FIN_OPP_WIDGET_WORK_MERGE = BWP_FIN_OPP_MERGE.merge(MEDB_WEDGET_WORK,how="inner",on=["PATIENT_ID"])
print("---BWP_FIN_OPP_WIDGET_WORK_MERGE---\n",BWP_FIN_OPP_WIDGET_WORK_MERGE.shape)
print("---BWP_FIN_OPP_WIDGET_WORK_MERGE---\n",BWP_FIN_OPP_WIDGET_WORK_MERGE)
print("---BWP_FIN_OPP_WIDGET_WORK_MERGE---\n",BWP_FIN_OPP_WIDGET_WORK_MERGE.columns)


#----------adding a dummy data-------
BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy=BWP_FIN_OPP_WIDGET_WORK_MERGE

BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy['TRANSACTION_TYPE_CDE']='31'
BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy['STATUS_CDE']='I'

BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy_v1=pd.concat([BWP_FIN_OPP_WIDGET_WORK_MERGE,BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy])

#----where conditions---
BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy_v1_list=BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy_v1[(BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy_v1['TRANSACTION_TYPE_CDE'].isin(['31','36'])) &
                                                  # (BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy_v1['SERVICED_DTE_x']>=DATE-90) &
                                                  (BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy_v1['STATUS_CDE']=='I')]
print("----BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy_v1_list---\n",BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy_v1_list.shape)
print("----BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy_v1_list---\n",BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy_v1_list.columns)

# BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy_v1_list_v1=BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy_v1_list[BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy_v1_list.PHCY_CLAIM_ID_x!=BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy_v1_list.PHCY_CLAIM_ID_x]
# print("-----BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy_v1_list_v1--\n",BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy_v1_list_v1.columns)

# # -------------------------UPDATE WIDGET---------------------
BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy_v1_list_fil = BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy_v1_list['PHCY_CLAIM_ID_x'].tolist()
MEDB_WEDGET_WORK["STATUS_CDE"] = np.where(MEDB_WEDGET_WORK['PHCY_CLAIM_ID_x'].isin(BWP_FIN_OPP_WIDGET_WORK_MERGE_dummy_v1_list_fil),'D',MEDB_WEDGET_WORK["STATUS_CDE"])
print("------MEDB_WEDGET_WORK-----BWP----\n",MEDB_WEDGET_WORK.shape)

# ----------------------gm--------------------------
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
CURR_PHARMACY_SUBMIT_TYPE_FIN_WIDGET_MERGE = CURR_PHARMACY_SUBMIT_TYPE_FIN_MERGE.merge(MEDB_WIDGET_WORK, how="inner",
                                                                                       on=["PATIENT_ID", "GCN_NBR"])
# -------------------------UPDATE WIDGET---------------------
CURR_PHARMACY_SUBMIT_TYPE_FIN_WIDGETV1 = CURR_PHARMACY_SUBMIT_TYPE_FIN_WIDGET_MERGE['PHCY_CLAIM_ID'].tolist
MEDB_WIDGET_WORK["STATUS_CDE"] = np.where(MEDB_WIDGET_WORK['PHCY_CLAIM_ID'].isin(CURR_PHARMACY_SUBMIT_TYPE_FIN_WIDGETV1),
                                         'D', MEDB_WIDGET_WORK["STATUS_CDE"])

# ---------------------5D-----------------------------
# MEDB_CLAIMPULL
# MEDB_WIDGET_WORK
# MEDB_WIDGET_WORK_ALL=pd.concat([])

MEDB_CLAIMPULL_V1 = MEDB_CLAIMPULL[["PHCY_CLAIM_ID", "PATIENT_ID", "GCN_NBR"]]
MEDB_WIDGET_WORK_V1 = MEDB_WIDGET_WORK[["PHCY_CLAIM_ID", "PATIENT_ID", "GCN_NBR"]]
MEDB_CLAIMPULL["STATUS_CDE"] = np.where(MEDB_CLAIMPULL.MEDB_CLAIMPULL_V1.isin(MEDB_WIDGET_WORK_V1), "D",
                                        MEDB_CLAIMPULL["STATUS_CDE"])

# # --------------------------------------5 D completed------------------------------

# # ---------------------------------6 A started-----------------------------
# ---------column rename---
CURRENT_PHARMACY_CLAIM_1.rename(columns={"PHCY_PRODUCT_SERVICE_ID": "PRODUCT_SERVICE_ID"}, inplace=True)

# --------------------replace----------
CURRENT_PHARMACY_CLAIM_1_list = CURRENT_PHARMACY_CLAIM_1.PRODUCT_SERVICE_ID[:5].tolist()
FIN_OPP_MPC_list = FIN_OPP_MPC.PRODUCT_SERVICE_ID[:5].tolist()
CURRENT_PHARMACY_CLAIM_1_list_v1 = CURRENT_PHARMACY_CLAIM_1.replace(CURRENT_PHARMACY_CLAIM_1_list, FIN_OPP_MPC_list)

# ---------------------merge 1----------------
pahrmacy_claim_fin_opp_merge = CURRENT_PHARMACY_CLAIM_1_list_v1.merge(FIN_OPP_MPC, how="inner", on="PRODUCT_SERVICE_ID")
print("----------pahrmacy_claim_fin_opp_merge---\n", pahrmacy_claim_fin_opp_merge.shape)
print("----------pahrmacy_claim_fin_opp_merge---\n", pahrmacy_claim_fin_opp_merge)

# - ------column rename - --
pahrmacy_claim_fin_opp_merge.rename(columns={"ELIG_GROUP_OPERATIONAL_ID": "GROUP_OPERATIONAL_ID"}, inplace=True)
#
# --------------------replace----------
FIN_OPP_SUBMIT_TYPE_GROUPS_list = FIN_OPP_SUBMIT_TYPE_GROUPS.GROUP_OPERATIONAL_ID[:5].tolist()
pahrmacy_claim_fin_opp_merge_list = pahrmacy_claim_fin_opp_merge.GROUP_OPERATIONAL_ID[:5].tolist()
pahrmacy_claim_fin_opp_merge_list_v1 = pahrmacy_claim_fin_opp_merge.replace(pahrmacy_claim_fin_opp_merge_list,
                                                                            FIN_OPP_SUBMIT_TYPE_GROUPS_list)

# -------------merge 2-------------
pharmacy_fin_op_submit_merge = pahrmacy_claim_fin_opp_merge_list_v1.merge(FIN_OPP_SUBMIT_TYPE_GROUPS, how="inner",
                                                                          on="GROUP_OPERATIONAL_ID")
print("-----pharmacy_fin_op_submit_merge--\n", pharmacy_fin_op_submit_merge.shape)
print("-----pharmacy_fin_op_submit_merge--\n", pharmacy_fin_op_submit_merge)

#-----------------ADDING DUMMY DATA-------

# ------------where conditions---------
pharmacy_fin_op_submit_merge_filters = pharmacy_fin_op_submit_merge[
    (pharmacy_fin_op_submit_merge["CLAIM_RESERVED_DTE"] != '1800-01-01') &
    (pharmacy_fin_op_submit_merge["CLAIM_RESERVED_CDE"] != " ")]

print("----------pharmacy_fin_op_submit_merge_filters----\n", pharmacy_fin_op_submit_merge_filters.shape)
print("----------pharmacy_fin_op_submit_merge_filters----\n", pharmacy_fin_op_submit_merge_filters)

# --------------------------------------6 a second part-----------------------------
# ---------column rename---
CURRENT_PHARMACY_CLAIM_1_DOD.rename(columns={"PHCY_PRODUCT_SERVICE_ID": "PRODUCT_SERVICE_ID"}, inplace=True)

# --------------------replace----------
CURRENT_PHARMACY_CLAIM_1_DOD_list = CURRENT_PHARMACY_CLAIM_1_DOD.PRODUCT_SERVICE_ID[:5].tolist()
FIN_OPP_MPC_list = FIN_OPP_MPC.PRODUCT_SERVICE_ID[:5].tolist()
CURRENT_PHARMACY_CLAIM_1_DOD_list_v1 = CURRENT_PHARMACY_CLAIM_1_DOD.replace(CURRENT_PHARMACY_CLAIM_1_DOD_list,
                                                                            FIN_OPP_MPC_list)

# # ---------------------merge 1----------------
dod_pahrmacy_claim_fin_opp_merge = CURRENT_PHARMACY_CLAIM_1_DOD_list_v1.merge(FIN_OPP_MPC, how="inner",
                                                                              on="PRODUCT_SERVICE_ID")
print("----------dod_pahrmacy_claim_fin_opp_merge---\n", dod_pahrmacy_claim_fin_opp_merge.shape)
print("----------dod_pahrmacy_claim_fin_opp_merge---\n", dod_pahrmacy_claim_fin_opp_merge)

# - ------column rename - --
pahrmacy_claim_fin_opp_merge.rename(columns={"ELIG_GROUP_OPERATIONAL_ID": "GROUP_OPERATIONAL_ID"}, inplace=True)

# --------------------replace----------
FIN_OPP_SUBMIT_TYPE_GROUPS_list = FIN_OPP_SUBMIT_TYPE_GROUPS.GROUP_OPERATIONAL_ID[:5].tolist()
dod_pahrmacy_claim_fin_opp_merge_v1_list = dod_pahrmacy_claim_fin_opp_merge.GROUP_OPERATIONAL_ID[:5].tolist()
dod_pahrmacy_claim_fin_opp_merge_v1 = dod_pahrmacy_claim_fin_opp_merge.replace(pahrmacy_claim_fin_opp_merge_list,
                                                                               FIN_OPP_SUBMIT_TYPE_GROUPS_list)

# -------------merge 2-------------
dod_pahrmacy_claim_fin_opp_groups_merge_v1 = dod_pahrmacy_claim_fin_opp_merge_v1.merge(FIN_OPP_SUBMIT_TYPE_GROUPS,
                                                                                       how="inner",
                                                                                       on="GROUP_OPERATIONAL_ID")
print("-----dod_pahrmacy_claim_fin_opp_groups_merge_v1--\n", dod_pahrmacy_claim_fin_opp_groups_merge_v1.shape)
print("-----dod_pahrmacy_claim_fin_opp_groups_merge_v1--\n", dod_pahrmacy_claim_fin_opp_groups_merge_v1)

#---------------------------ADDING DUMMY DATA-------

# ------------where conditions---------
dod_pahrmacy_claim_fin_opp_groups_merge_v1_filters = dod_pahrmacy_claim_fin_opp_groups_merge_v1[(dod_pahrmacy_claim_fin_opp_groups_merge_v1["CLAIM_RESERVED_DTE"] != '1800-01-01') &
                                                                                                (dod_pahrmacy_claim_fin_opp_groups_merge_v1["CLAIM_RESERVED_CDE"] != " ")]

print("----------dod_pahrmacy_claim_fin_opp_groups_merge_v1_filters----\n",
      dod_pahrmacy_claim_fin_opp_groups_merge_v1_filters.shape)
print("----------dod_pahrmacy_claim_fin_opp_groups_merge_v1_filters----\n",
      dod_pahrmacy_claim_fin_opp_groups_merge_v1_filters)

# ----------------------append two tables------------
MEDB_CLAIM_REVERSALS_v1 = pd.concat(
    [pharmacy_fin_op_submit_merge_filters, dod_pahrmacy_claim_fin_opp_groups_merge_v1_filters], inplace=True)

print("------------MEDB_CLAIM_REVERSALS_v1---\n", MEDB_CLAIM_REVERSALS_v1.shape)
print("---------------MEDB_CLAIM_REVERSALS_v1---\n", MEDB_CLAIM_REVERSALS_v1)

MEDB_CLAIM_REVERSALS = MEDB_CLAIM_REVERSALS_v1[['PATIENT_ID', 'PHCY_CLAIM_ID', 'CLAIM_REVERSED_DTE', 'CLAIM_REVERSED_CTE']]
print("------------MEDB_CLAIM_REVERSALS---\n", MEDB_CLAIM_REVERSALS.shape)
print("---------------MEDB_CLAIM_REVERSALS---\n", MEDB_CLAIM_REVERSALS)
print("------------MEDB_CLAIM_REVERSALS---\n", MEDB_CLAIM_REVERSALS.columns)
print("------------------------logic 6 B completed------------------------------------")


# # # ------------------------logic 7 started--------------------------
# ---------------------logic 7 A started----------------------
print("------logic 7a started-----------------------")
print("------MEDB_CLAIM_PULL--\n", MEDB_CLAIM_PULL.columns)

MEDB_CLM_GRP = MEDB_CLAIM_PULL[["PATIENT_ID", "GROUP_OPERATIONAL_ID"]]
MEDB_CLM_GRP['ELIG_FLAG']='N'
print("------MEDB_CLM_GRP--\n", MEDB_CLM_GRP.shape)
print("------MEDB_CLM_GRP--\n", MEDB_CLM_GRP)
print("------MEDB_CLM_GRP--\n", MEDB_CLM_GRP.columns)

MEMBER_CURRENT_v1=MEMBER_CURRENT[["PATIENT_ID","GROUP_OPERATIONAL_ID","CURRENT_OPERATIONAL_ROW_IND"]]
print("----MEMBER_CURRENT_v1--\n",MEMBER_CURRENT_v1.shape)
#-----------------updating-------------------****************************************************8
# ----------------merge---
clm_grp_member_current_merge = MEDB_CLM_GRP.merge(MEMBER_CURRENT,how="left",on=["PATIENT_ID","GROUP_OPERATIONAL_ID"])
print("----------clm_grp_member_current_merge---\n", clm_grp_member_current_merge.shape)
print("----------clm_grp_member_current_merge---\n", clm_grp_member_current_merge)
#----adding dummy data---
#-=----------adding dummy data-----
clm_grp_member_current_merge_dummy=clm_grp_member_current_merge
# clm_grp_member_current_merge_dummy['EFF_DTE']=<DATE+1
# clm_grp_member_current_merge_dummy['END_DTE']>DATE
# clm_grp_member_current_merge_dummy['END_EFF_DTE']>DATE
clm_grp_member_current_merge_dummy['CURRENT_OPERATIONAL_ROW_IND']="Y"
clm_grp_member_current_merge_dummy_V1=pd.concat([clm_grp_member_current_merge,clm_grp_member_current_merge_dummy])

# -----where condition----
clm_grp_member_current_merge_dummy_FIN = clm_grp_member_current_merge_dummy_V1[(clm_grp_member_current_merge_dummy_V1["EFF_DTE"] < DATE + 1) &
                                                               (clm_grp_member_current_merge_dummy_V1["END_DTE"] > DATE) &
                                                               (clm_grp_member_current_merge_dummy_V1["END_EFF_DTE"] > DATE) &
                                                               (clm_grp_member_current_merge_dummy_V1["CURRENT_OPERATIONAL_ROW_IND"] == "Y")]
print("----clm_grp_member_current_merge_dummy_FIN--\n", clm_grp_member_current_merge_dummy_FIN.shape)

# ---------------update elig flag- or--set conditions----
clm_grp_member_current_merge_dummy_FIN_LIST=clm_grp_member_current_merge_dummy_FIN['PATIENT_ID'].tolist()
clm_grp_member_current_merge_dummy_FIN_LIST=clm_grp_member_current_merge_dummy_FIN['GROUP_OPERATIONAL_ID'].tolist()
#
MEDB_CLM_GRP['ELIG_FLAG']=np.where((MEDB_CLM_GRP['PATIENT_ID'].isin(clm_grp_member_current_merge_dummy_FIN_LIST) &
                                    (MEDB_CLM_GRP['GROUP_OPERATIONAL_ID'].isin(clm_grp_member_current_merge_dummy_FIN_LIST)),
                                    "Y",MEDB_CLM_GRP['ELIG_FLAG']))

print("----------MEDB_CLM_GRP_update---\n", MEDB_CLM_GRP_update.shape)
print("----------MEDB_CLM_GRP_update---\n", MEDB_CLM_GRP_update)
#
# # ---------------------7a second part--------------
#-----------------updating-----------------
# ----------------merge2---
clm_grp_dod_member_current_merge = MEDB_CLM_GRP.merge(DOD_MEMBER_CURRENT, how="inner",on=["PATIENT_ID", "GROUP_OPERATIONAL_ID"])
print("----------clm_grp_dod_member_current_merge---\n", clm_grp_dod_member_current_merge.shape)
print("----------clm_grp_dod_member_current_merge---\n", clm_grp_dod_member_current_merge)

# -=----------adding dummy data-----
clm_grp_dod_member_current_merge_DUMMY=clm_grp_dod_member_current_merge

# clm_grp_dod_member_current_merge_DUMMY['EFF_DTE']=<DATE+1
# clm_grp_dod_member_current_merge_DUMMY['END_DTE']>DATE
# clm_grp_dod_member_current_merge_DUMMY['END_EFF_DTE']>DATE
clm_grp_dod_member_current_merge_DUMMY['CURRENT_OPERATIONAL_ROW_IND']="Y"
clm_grp_dod_member_current_merge_DUMMY_v1=pd.concat([clm_grp_dod_member_current_merge,clm_grp_dod_member_current_merge_DUMMY])

# # -----where condition----
clm_grp_dod_member_current_merge_DUMMY_v1_lis = clm_grp_dod_member_current_merge_DUMMY_v1[(clm_grp_dod_member_current_merge_DUMMY_v1["EFF_DTE"] < DATE + 1) &
                                                               (clm_grp_dod_member_current_merge_DUMMY_v1["END_DTE"] > DATE) &
                                                               (clm_grp_dod_member_current_merge_DUMMY_v1["END_EFF_DTE"] > DATE) &
                                                               (clm_grp_dod_member_current_merge_DUMMY_v1["CURRENT_OPERATIONAL_ROW_IND"] == "Y")]
print("----clm_grp_dod_member_current_merge_DUMMY_v1_lis--\n", clm_grp_dod_member_current_merge_DUMMY_v1_lis.shape)


# ---------------update elig flag- or--set conditions----
clm_grp_dod_member_current_merge_DUMMY_v1_lis_1=clm_grp_dod_member_current_merge_DUMMY_v1_lis['PATIENT_ID'].tolist()
clm_grp_dod_member_current_merge_DUMMY_v1_lis_1=clm_grp_dod_member_current_merge_DUMMY_v1_lis['GROUP_OPERATIONAL_ID'].tolist()

MEDB_CLM_GRP['ELIG_FLAG']=np.where((MEDB_CLM_GRP['ELIG_FLAG']=="N") & (MEDB_CLM_GRP['PATIENT_ID'].isin(clm_grp_dod_member_current_merge_DUMMY_v1_lis_1)) &
                                    (MEDB_CLM_GRP['GROUP_OPERATIONAL_ID'].isin(clm_grp_dod_member_current_merge_DUMMY_v1_lis_1)),
                                    "Y",MEDB_CLM_GRP['ELIG_FLAG'])

print("----------MEDB_CLM_GRP_update2---\n", MEDB_CLM_GRP_update2.shape)
print("----------MEDB_CLM_GRP_update2---\n", MEDB_CLM_GRP_update2)
# -------------------7 a completed----

# ---------------------------PAT_GRP_NEWELIG------------------------7 b started------
# --------------replace----------
MEMBER_CURRENT_lst = MEMBER_CURRENT.PATIENT_ID[:5].tolist()
MEDB_CLM_GRP_lst = MEDB_CLM_GRP.PATIENT_ID[:5].tolist()
MEMBER_CURRENT_v1 = MEMBER_CURRENT.replace(MEMBER_CURRENT_lst, MEDB_CLM_GRP_lst)
print("-----MEMBER_CURRENT_v1---\n", MEMBER_CURRENT_v1.shape)

# ----merge1-------
current_grp_merge = MEMBER_CURRENT_v1.merge(MEDB_CLM_GRP, how='inner', on='PATIENT_ID')
print("-----current_grp_merge---\n", current_grp_merge.shape)
print("-----current_grp_merge---\n", current_grp_merge.shape)
print("-----current_grp_merge---\n", current_grp_merge.shape)

# ------------------------WHERE CONDITIONS----------------
current_grp_merge_final = current_grp_merge[(current_grp_merge["ELIG_FLAG"] == "N") &
                                            (current_grp_merge["EFF_DTE"] < DATE + 1) &
                                            (current_grp_merge["END_DTE"] > DATE) &
                                            (current_grp_merge["END_EFF_DTE"] > DATE) &
                                            (current_grp_merge["CURRENT_OPERATIONAL_ROW_IND"] == "Y")]
print("-----current_grp_merge_final---\n", current_grp_merge_final.shape)

# ------------7b second part----
# --------------replace----------
MEMBER_CURRENT_lst = MEMBER_CURRENT.PATIENT_ID[:5].tolist()
MEDB_CLM_GRP_lst = MEDB_CLM_GRP.PATIENT_ID[:5].tolist()
MEMBER_CURRENT_v1 = MEMBER_CURRENT.replace(MEMBER_CURRENT_lst, MEDB_CLM_GRP_lst)
print("-----MEMBER_CURRENT_v1---\n", MEMBER_CURRENT_v1.shape)

# ---------------------------merge1-------
dod_current_grp_merge = MEMBER_CURRENT_v1.merge(MEDB_CLM_GRP, how='inner', on='PATIENT_ID')
print("-----dod_current_grp_merge---\n", dod_current_grp_merge.shape)
print("-----dod_current_grp_merge---\n", dod_current_grp_merge)
print("-----dod_current_grp_merge---\n", dod_current_grp_merge.columns)

# ------------------------WHERE CONDITIONS----------------
dod_current_grp_merge_final = dod_current_grp_merge[(dod_current_grp_merge["ELIG_FLAG"] == "N") &
                                                    (dod_current_grp_merge["EFF_DTE"] < DATE + 1) &
                                                    (dod_current_grp_merge["END_DTE"] > DATE) &
                                                    (dod_current_grp_merge["END_EFF_DTE"] > DATE) &
                                                    (dod_current_grp_merge["CURRENT_OPERATIONAL_ROW_IND"] == "Y")]
print("-----dod_current_grp_merge_final---\n", dod_current_grp_merge_final.shape)

# -------------------appending----------------
PATGRP_NEWELIG_CLM = pd.concat([current_grp_merge_final, dod_current_grp_merge_final],ignore_index=True )
print("-----PATGRP_NEWELIG_CLM---\n", PATGRP_NEWELIG_CLM.shape)
print("-----PATGRP_NEWELIG_CLM---\n", PATGRP_NEWELIG_CLM)
print("-----PATGRP_NEWELIG_CLM---\n", PATGRP_NEWELIG_CLM.columns)
#--------------------------logic 7 b completed-- -------------------------

# -----------------------------------------------------------------------
print("--PATGRP_NEWELIG_CLM--\n-",PATGRP_NEWELIG_CLM.OPERATIONAL_DTE.unique())
PATGRP_NEWELIG_CLM.rename(columns={"OPERATIONAL_DTE":"MAX_DTE_TIM"},inplace=True)
MEDB_PAT_MAXCURR_CLM = PATGRP_NEWELIG_CLM[["PATIENT_ID", "MAX_DTE_TIM"]]
print("-------MEDB_PAT_MAXCURR_CLM--\n", MEDB_PAT_MAXCURR_CLM.shape)
print("-------MEDB_PAT_MAXCURR_CLM--\n", MEDB_PAT_MAXCURR_CLM)
print("-------MEDB_PAT_MAXCURR_CLM--\n", MEDB_PAT_MAXCURR_CLM.columns)

#---------8:42-------MEDB_PATGRP_UNQMAX_CLM---------------
#------------merge-----
MEDB_PATGRP_UNQMAX_CLM=PATGRP_NEWELIG_CLM.merge(MEDB_PAT_MAXCURR_CLM,how="inner",on=["PATIENT_ID","MAX_DTE_TIM"])
print("-------MEDB_PATGRP_UNQMAX_CLM--\n", MEDB_PATGRP_UNQMAX_CLM.shape)
print("-------MEDB_PATGRP_UNQMAX_CLM--\n", MEDB_PATGRP_UNQMAX_CLM)
print("-------MEDB_PATGRP_UNQMAX_CLM--\n", MEDB_PATGRP_UNQMAX_CLM.columns)
#------------------------------
#----------------------------------------logic  7 c started-------------------------
#-----------------update------------------
#------------set------------------
# ELIG_GROUP_OPERATIONAL_ID=B.GROUP_OPERATIONAL_ID
# CLIENT_MEMBERSHIP_ID=B.CLIENT_MEMBERSHIP_ID
# PERSON_NBR=B.PERSON_NBR
# # CARRIER_OPEARYIONAL_ID=C.CARRIER_OPEARYIONAL_ID
# # CONTRACT_OPERATIONAL_ID=C.CONTRACT_OPERATIONAL_ID
#
# #___********************* WE ARE ALWAYS CHANGE THE COLUMN NAMES IN  RIGHT SIDE OF THE TABLES HERE WE ARE CHANGE COLUMN NMAES IN B & C****************
# --------------rename a column name--
MEDB_PATGRP_UNQMAX_CLM.rename(columns={"CLIENT_MEMBERSHIP_ID": "CLIENT_MEMBERSHIP_ID_v1"}, inplace=True)
MEDB_PATGRP_UNQMAX_CLM.rename(columns={"PERSON_NBR": "PERSON_NBR_v1"}, inplace=True)

HIRERCHY.rename(columns={"CARRIER_OPEARYIONAL_ID": "CARRIER_OPEARYIONAL_ID_v1"}, inplace=True)
HIRERCHY.rename(columns={"CONTRACT_OPERATIONAL_ID": "CONTRACT_OPERATIONAL_ID_v1"}, inplace=True)

#--------------merge 1---------------
claimpull_unqmax_clm_merge=MEDB_CLAIMPULL.merge(MEDB_PATGRP_UNQMAX_CLM,how="inner",on="PATIENT_ID")
print("-------claimpull_unqmax_clm_merge--\n", claimpull_unqmax_clm_merge.shape)
print("-------claimpull_unqmax_clm_merge--\n", claimpull_unqmax_clm_merge)
print("-------claimpull_unqmax_clm_merge--\n", claimpull_unqmax_clm_merge.columns)

#-------------------------merge 2--------
claimpull_unqmax_clm_hierar_merge=claimpull_unqmax_clm_merge.merge(HIRERCHY,how="inner",on="GROUP_OPERATIONAL_ID")
print("-------claimpull_unqmax_clm_hierar_merge--\n", claimpull_unqmax_clm_hierar_merge.shape)
print("-------claimpull_unqmax_clm_hierar_merge--\n", claimpull_unqmax_clm_hierar_merge)
print("-------claimpull_unqmax_clm_hierar_merge--\n", claimpull_unqmax_clm_hierar_merge.columns)

#---------------where onditions--------
claimpull_unqmax_clm_hierar_merge_final = claimpull_unqmax_clm_hierar_merge[(claimpull_unqmax_clm_hierar_merge["GROUP_OPERATIONAL_ID"] != " ") &
                                                                            (claimpull_unqmax_clm_hierar_merge["EFF_DTE"] <= CURRENT_DATE) &
                                                                            (claimpull_unqmax_clm_hierar_merge["END_DTE"] > CURRENT_DATE) &
                                                                             (claimpull_unqmax_clm_hierar_merge["END_EFF_DTE"] > CURRENT_DATE)]

#--------set columns-----------
# ------------------------------drop column name------------------
claimpull_unqmax_clm_hierar_merge_final_v1 = claimpull_unqmax_clm_hierar_merge_final.drop(["ELIG_GROUP_OPERATIONAL_ID"], axis=1)
claimpull_unqmax_clm_hierar_merge_final_v1 = claimpull_unqmax_clm_hierar_merge_final.drop(["CLIENT_MEMBERSHIP_ID"],axis=1)
claimpull_unqmax_clm_hierar_merge_final_v1 = claimpull_unqmax_clm_hierar_merge_final.drop(["PERSON_NBR"], axis=1)
claimpull_unqmax_clm_hierar_merge_final_v1 = claimpull_unqmax_clm_hierar_merge_final.drop(["CARRIER_OPEARYIONAL_ID"],axis=1)
claimpull_unqmax_clm_hierar_merge_final_v1 = claimpull_unqmax_clm_hierar_merge_final.drop(["CONTRACT_OPERATIONAL_ID"], axis=1)

# # --------------rename a column name--
claimpull_unqmax_clm_hierar_merge_final_v1.rename(columns={"GROUP_OPERATIONAL_ID": "ELIG_GROUP_OPERATIONAL_ID"}, inplace=True)
claimpull_unqmax_clm_hierar_merge_final_v1.rename(columns={"CLIENT_MEMBERSHIP_ID_v1": "CLIENT_MEMBERSHIP_ID"}, inplace=True)
claimpull_unqmax_clm_hierar_merge_final_v1.rename(columns={"PERSON_NBR_v1": "PERSON_NBR"}, inplace=True)
claimpull_unqmax_clm_hierar_merge_final_v1.rename(columns={"CARRIER_OPEARYIONAL_ID_v1": "CARRIER_OPEARYIONAL_ID"}, inplace=True)
claimpull_unqmax_clm_hierar_merge_final_v1.rename(columns={"CONTRACT_OPERATIONAL_ID_v1": "CONTRACT_OPERATIONAL_ID"}, inplace=True)
#----------------------------------logic 7c completed---------------
#----------------logic verified by client----------------

#------------------------------------------------logic 8  started---------------
#---------------MEDB_FORM_TMP--------------
MEDB_FORM_TMP1=MEDB_CLAIMPULL[["ELIG_GROUP_OPERATIONAL_ID","ADJUD_GROUP_OPERATIONAL_ID","PRODUCT_ID","PRF_NDC"]]
MEDB_FORM_TMP1['MAIL_RETAIL_CDE']=' '
print("-------MEDB_FORM_TMP1--\n", MEDB_FORM_TMP1.shape)
print("-------MEDB_FORM_TMP1--\n", MEDB_FORM_TMP1)
print("-------MEDB_FORM_TMP1--\n", MEDB_FORM_TMP1.columns)

MEDB_FORM_TMP=MEDB_FORM_TMP1

print("-------MEDB_FORM_TMP--\n", MEDB_FORM_TMP.shape)
print("-------MEDB_FORM_TMP--\n", MEDB_FORM_TMP)
print("-------MEDB_FORM_TMP--\n", MEDB_FORM_TMP.columns)

#------------------------------------------------------
#-------------------------9:12--------------------MEDB_GRP_CHNL--------------------------9:18---
#------------creating a new column---
MEDB_FORM_TMP["CLIENT_ID"]=0
MEDB_FORM_TMP["CLIENT_SRC_CDE"]=0
MEDB_FORM_TMP["FORMULARY_ID"]=" "
MEDB_FORM_TMP["FORMULARY_IND"]=" "

MEDB_GRP_CHNL=MEDB_FORM_TMP
print("--------------MEDB_GRP_CHNL----\n",MEDB_GRP_CHNL.shape)
print("--------------MEDB_GRP_CHNL----\n",MEDB_GRP_CHNL.columns)

#----------------------------column rename------
MEDB_GRP_CHNL.rename(columns={"ELIG_GROIUP_OPERATIONAL_ID":"GROIUP_OPERATIONAL_ID"},inplace=True)


#---------------------UPDATE MEDB_GRP_CHNL TABLE-----
#------------9:24------UPDATE ---------
import pandas as pd
import numpy as np
# --------CASE CONDITIONS------
MEDB_GRP_CHNL['PRODUCT_ID']=np.where(MEDB_GRP_CHNL['PRODUCT_ID']=="R",1,2)
MEDB_GRP_CHNL['PRODUCT_ID']=np.where(MEDB_GRP_CHNL['PRODUCT_ID']=="M",3,2)
MEDB_GRP_CHNL_v1=MEDB_GRP_CHNL[['GROIUP_OPERATIONAL_ID','PRODUCT_ID']]

#--------import PRODUCT_ITEM_RH table-----
PRODUCT_ITEM_RH=pd.read_sql(query.PRODUCT_ITEM_RH,cnxn)
# PRODUCT_ITEM_RH.columns=PRODUCT_ITEM_RH_COL
print("-------PRODUCT_ITEM_RH---\n",PRODUCT_ITEM_RH.shape)
print("-------PRODUCT_ITEM_RH---\n",PRODUCT_ITEM_RH)
print("-------PRODUCT_ITEM_RH---\n",PRODUCT_ITEM_RH.columns)

#------------MERGE 1------
CHNL_ITEMRH_MERGE=MEDB_GRP_CHNL_v1.merge(PRODUCT_ITEM_RH,how="inner",on="PRODUCT_ID")
print("-------CHNL_ITEMRH_MERGE---\n",CHNL_ITEMRH_MERGE.shape)
print("-------CHNL_ITEMRH_MERGE---\n",CHNL_ITEMRH_MERGE)
print("-------CHNL_ITEMRH_MERGE---\n",CHNL_ITEMRH_MERGE.columns)

#-------------------column rename------------
PRODUCT_ITEM_RH.rename(columns={"OPERATIONAL_ID":"GROUP_OPERATIONAL_ID"},inplace=True)

CHNL_ITEMRH_MERGE['CLIENT_SRC_CDE']=CHNL_ITEMRH_MERGE['CLIENT_SRC_CDE'].astype('int64')
#---------------MERGE2-----------------------
CHNL_ITEMRH_CLIENTRH_MERGE=CHNL_ITEMRH_MERGE.merge(CLIENT_RH,how="inner",on=["GROUP_OPERATIONAL_ID","CLIENT_ID"])
print("-------CHNL_ITEMRH_CLIENTRH_MERGE---\n",CHNL_ITEMRH_CLIENTRH_MERGE.shape)
print("-------CHNL_ITEMRH_CLIENTRH_MERGE---\n",CHNL_ITEMRH_CLIENTRH_MERGE)
print("-------CHNL_ITEMRH_CLIENTRH_MERGE---\n",CHNL_ITEMRH_CLIENTRH_MERGE.columns)

#----------------------update case when conditions---------
CHNL_ITEMRH_CLIENTRH_MERGE['PRODUCT_ID']=np.where(CHNL_ITEMRH_CLIENTRH_MERGE['PRODUCT_ID']==1,"R","O")
CHNL_ITEMRH_CLIENTRH_MERGE['PRODUCT_ID']=np.where(CHNL_ITEMRH_CLIENTRH_MERGE['PRODUCT_ID']==3,"M","O")

CHNL_ITEMRH_CLIENTRH_MERGE['MAIL_RETAIL_CDE']=np.where(CHNL_ITEMRH_CLIENTRH_M ERGE['PRODUCT_ID']==1,"R","O")
CHNL_ITEMRH_CLIENTRH_MERGE['MAIL_RETAIL_CDE']=np.where(CHNL_ITEMRH_CLIENTRH_MERGE['PRODUCT_ID']==3,"M","O")

#-----------------WHERE CONDITION----------------
CHNL_ITEMRH_CLIENTRH_MERGE_FIL=CHNL_ITEMRH_CLIENTRH_MERGE[(CHNL_ITEMRH_CLIENTRH_MERGE["CLIENT_TYPE_CDE"]=="GR") &
                                    (CHNL_ITEMRH_CLIENTRH_MERGE["EFF_DTE"]<=CURRENT_DATE) &
                                    (CHNL_ITEMRH_CLIENTRH_MERGE['END_DTE']>CURRENT_DATE) &
                                    (CHNL_ITEMRH_CLIENTRH_MERGE['END_EFF_DTE']>CURRENT_DATE) &
                                    (CHNL_ITEMRH_CLIENTRH_MERGE['PRODUCT_SRC_CDE']==1)]
#--------------RENAME-----
CHNL_ITEMRH_CLIENTRH_MERGE_FIL.rename(columns={"FORMULARY_ID"=='FORMULARY_ID_V1'},inplace=True)

#----------------MERGE----
FINAL_MERGE=MEDB_GRP_CHNL.merge(CHNL_ITEMRH_CLIENTRH_MERGE_FIL,how="inner",on=['GROUP_OPERATIONAL_ID','PRODUCT_ID'])
print("------FINAL_MERGE----\n",FINAL_MERGE.shape)

#-----------drop column name---
CHNL_ITEMRH_CLIENTRH_MERGE_FIL_v1=CHNL_ITEMRH_CLIENTRH_MERGE_FIL.drop(["FORMULARY_ID"],axis=1)

#--------------RENAME-----
CHNL_ITEMRH_CLIENTRH_MERGE_FIL_v1.rename(columns={"FORMULARY_ID_V1":'FORMULARY_ID'},inplace=True)


#----*---------------***--------UPDATE GRP2---------
#-------------case conditions----------
MEDB_GRP_CHNL['PRODUCT_ID']=np.where(MEDB_GRP_CHNL['PRODUCT_ID']=='R',1,2)
MEDB_GRP_CHNL['PRODUCT_ID']=np.where(MEDB_GRP_CHNL['PRODUCT_ID']=='M',3,2)
MEDB_GRP_CHNL_V2=MEDB_GRP_CHNL[['ADJUD_GROUP_OPERATIONAL_ID','PRODUCT_ID']]

#----------------MERGE1---------
GRPCHNL_ITEMRH_MERGE=MEDB_GRP_CHNL_V2.merge(CLIENT_PRODUCT_ITEM_RH,how="inner",on=['PRODUCT_ID'])
print("----GRPCHNL_ITEMRH_MERGE---\n",shape)

#--------------COLUMN RENAME-----
CLIENT_PRODUCT_ITEM_RH.rename(columns={'OPERATIONAL_ID':'ADJUD_GROUP_OPERATIONAL_ID'},inplace=True)

CLIENT_RH['ADJUD_GROUP_OPERATIONAL_ID']=CLIENT_RH['GROUP_OPERATIONAL_ID']

#-------------MERGE 2-------------
GRPCHNL_ITEMRH_CLIENTRH_MERGE=GRPCHNL_ITEMRH_MERGE.merge(CLIENT_RH,how="inner",on=["GROUP_OPERATIONAL_ID","CLIENT_ID"])
print("----GRPCHNL_ITEMRH_CLIENTRH_MERGE---\n",GRPCHNL_ITEMRH_CLIENTRH_MERGE.shape)

#----------------------update case when conditions---------
GRPCHNL_ITEMRH_CLIENTRH_MERGE['PRODUCT_ID']=np.where(GRPCHNL_ITEMRH_CLIENTRH_MERGE['PRODUCT_ID']==1,"R","O")
GRPCHNL_ITEMRH_CLIENTRH_MERGE['PRODUCT_ID']=np.where(GRPCHNL_ITEMRH_CLIENTRH_MERGE['PRODUCT_ID']==3,"M","O")

GRPCHNL_ITEMRH_CLIENTRH_MERGE['MAIL_RETAIL_CDE']=np.where(GRPCHNL_ITEMRH_CLIENTRH_MERGE ERGE['PRODUCT_ID']==1,"R","O")
GRPCHNL_ITEMRH_CLIENTRH_MERGE['MAIL_RETAIL_CDE']=np.where(GRPCHNL_ITEMRH_CLIENTRH_MERGE['PRODUCT_ID']==3,"M","O")

#-----------------WHERE CONDITION----------------
GRPCHNL_ITEMRH_CLIENTRH_MERGE_FIL2=GRPCHNL_ITEMRH_CLIENTRH_MERGE[(GRPCHNL_ITEMRH_CLIENTRH_MERGE["CLIENT_TYPE_CDE"]=="GR") &
                                    (GRPCHNL_ITEMRH_CLIENTRH_MERGE["EFF_DTE"]<=CURRENT_DATE) &
                                    (GRPCHNL_ITEMRH_CLIENTRH_MERGE['END_DTE']>CURRENT_DATE) &
                                    (GRPCHNL_ITEMRH_CLIENTRH_MERGE['END_EFF_DTE']>CURRENT_DATE) &
                                    (GRPCHNL_ITEMRH_CLIENTRH_MERGE['PRODUCT_SRC_CDE']==1)]

# --------------RENAME-----
GRPCHNL_ITEMRH_CLIENTRH_MERGE_FIL2.rename(columns={"FORMULARY_ID"=='FORMULARY_ID_V1'},inplace=True)

#----------------MERGE----
FINAL_MERGE2=MEDB_GRP_CHNL.merge(GRPCHNL_ITEMRH_CLIENTRH_MERGE_FIL2,how="inner",on=['GROUP_OPERATIONAL_ID','PRODUCT_ID'])
print("------FINAL_MERGE2----\n",FINAL_MERGE2.shape)

#-----------drop column name---
GRPCHNL_ITEMRH_CLIENTRH_MERGE_FIL2=GRPCHNL_ITEMRH_CLIENTRH_MERGE.drop(["FORMULARY_ID"],axis=1)

#--------------RENAME-----
GRPCHNL_ITEMRH_CLIENTRH_MERGE_FIL2.rename(columns={"FORMULARY_ID_V1":'FORMULARY_ID'},inplace=True)

#------------WHERE CONDITION------
GRPCHNL_ITEMRH_CLIENTRH_MERGE_FIL2_2=GRPCHNL_ITEMRH_CLIENTRH_MERGE_FIL2[GRPCHNL_ITEMRH_CLIENTRH_MERGE_FIL2['FORMULARY_ID']='']

#---******** UPDATE 3--------------------
# -------------case conditions----------
MEDB_GRP_CHNL['PRODUCT_ID']=np.where(MEDB_GRP_CHNL['PRODUCT_ID']=='R',1,2)
MEDB_GRP_CHNL['PRODUCT_ID']=np.where(MEDB_GRP_CHNL['PRODUCT_ID']=='M',3,2)
MEDB_GRP_CHNL_V2=MEDB_GRP_CHNL[['ADJUD_GROUP_OPERATIONAL_ID','PRODUCT_ID']]

#----------------MERGE1---------
GRPCHNL_ITEMRH_MERGE3=MEDB_GRP_CHNL_V2.merge(CLIENT_PRODUCT_ITEM_RH,how="inner",on=['PRODUCT_ID'])
print("----GRPCHNL_ITEMRH_MERGE3---\n",shape)

#--------------COLUMN RENAME-----
CLIENT_PRODUCT_ITEM_RH.rename(columns={'OPERATIONAL_ID':'ADJUD_GROUP_OPERATIONAL_ID'},inplace=True)

CLIENT_RH['ADJUD_GROUP_OPERATIONAL_ID']=CLIENT_RH['GROUP_OPERATIONAL_ID']

#-------------MERGE 2-------------
GRPCHNL_ITEMRH_CLIENTRH_MERGE3=GRPCHNL_ITEMRH_MERGE3.merge(CLIENT_RH,how="inner",on=["GROUP_OPERATIONAL_ID","CLIENT_ID"])
print("----GRPCHNL_ITEMRH_CLIENTRH_MERGE3---\n",GRPCHNL_ITEMRH_CLIENTRH_MERGE3.shape)

#----------------------update case when conditions---------
GRPCHNL_ITEMRH_CLIENTRH_MERGE3['PRODUCT_ID']=np.where(GRPCHNL_ITEMRH_CLIENTRH_MERGE3['PRODUCT_ID']==1,"R","O")
GRPCHNL_ITEMRH_CLIENTRH_MERGE3['PRODUCT_ID']=np.where(GRPCHNL_ITEMRH_CLIENTRH_MERGE3['PRODUCT_ID']==3,"M","O")

GRPCHNL_ITEMRH_CLIENTRH_MERGE3['MAIL_RETAIL_CDE']=np.where(GRPCHNL_ITEMRH_CLIENTRH_MERGE3 ERGE['PRODUCT_ID']==1,"R","O")
GRPCHNL_ITEMRH_CLIENTRH_MERGE3['MAIL_RETAIL_CDE']=np.where(GRPCHNL_ITEMRH_CLIENTRH_MERGE3['PRODUCT_ID']==3,"M","O")

#-----------------WHERE CONDITION----------------
GRPCHNL_ITEMRH_CLIENTRH_MERGE3_FIL3=GRPCHNL_ITEMRH_CLIENTRH_MERGE3[(GRPCHNL_ITEMRH_CLIENTRH_MERGE3["CLIENT_TYPE_CDE"]=="GR") &
                                    (GRPCHNL_ITEMRH_CLIENTRH_MERGE3["EFF_DTE"]<=CURRENT_DATE) &
                                    (GRPCHNL_ITEMRH_CLIENTRH_MERGE3['END_DTE']>CURRENT_DATE) &
                                    (GRPCHNL_ITEMRH_CLIENTRH_MERGE3['END_EFF_DTE']>CURRENT_DATE) &
                                    (GRPCHNL_ITEMRH_CLIENTRH_MERGE3['PRODUCT_SRC_CDE']==1)]

# --------------RENAME-----
GRPCHNL_ITEMRH_CLIENTRH_MER GE3_FIL3.rename(columns={"FORMULARY_ID"=='FORMULARY_ID_V1'},inplace=True)

#----------------MERGE----
FINAL_MERGE3=MEDB_GRP_CHNL.merge(GRPCHNL_ITEMRH_CLIENTRH_MERGE3_FIL3,how="inner",on=['GROUP_OPERATIONAL_ID','PRODUCT_ID'])
print("------FINAL_MERGE3----\n",FINAL_MERGE3.shape)

#-----------drop column name---
GRPCHNL_ITEMRH_CLIENTRH_MERGE3_FIL3=GRPCHNL_ITEMRH_CLIENTRH_MERGE3.drop(["FORMULARY_ID"],axis=1)

#--------------RENAME-----
GRPCHNL_ITEMRH_CLIENTRH_MERGE3_FIL3.rename(columns={"FORMULARY_ID_V1":'FORMULARY_ID'},inplace=True)

#----------WHERE CONDITION---
GRPCHNL_ITEMRH_CLIENTRH_MERGE_FIL2_2=GRPCHNL_ITEMRH_CLIENTRH_MERGE_FIL2[GRPCHNL_ITEMRH_CLIENTRH_MERGE_FIL2['FORMULARY_ID']='']


#-----------------LOGIC 8 B-------------------


# #-------------update----logic 8 starts---9:21----------8 competed at 9:51----------
# #---------------------------------lo gic 9 starts 9:58--------vedio ends in 10------


# #---------------------------------------logic 9 started----------------
#----------UPDATE------------
MEDB_GRP_CHNL_LST=MEDB_GRP_CHNL[["GROUP_OPERATIONAL_ID","PRODUCT_SERVICE_ID","PRDF_MAIL_RETAIL_CDE","FORMULARY_IND"]]
#---------column rename---
MEDB_GRP_CHNL_LST.rename(columns={"ELIG_GROUP_OPERATIONAL_ID":"GROUP_OPERATIONAL_ID"},inplace=True)
MEDB_GRP_CHNL_LST.rename(columns={"PHCY_PRODUCT_SERVICE_ID":"PRODUCT_SERVICE_ID"},inplace=True)
MEDB_GRP_CHNL_LST.rename(columns={"MAIL_RETAIL_CDE":"PRDF_MAIL_RETAIL_CDE"},inplace=True)
print("----MEDB_GRP_CHNL_LST--\n",MEDB_GRP_LST.columns)
#-----merge-----
MEDB_GRP_LST_MERGE=MEDB_CLAIMPULL.merge(MEDB_GRP_CHNL_LST,how="inner",on=["GROUP_OPERATIONAL_ID","PRODUCT_SERVICE_ID","PRDF_MAIL_RETAIL_CDE"])
print("-----MEDB_GRP_LST_MERGE---\n",MEDB_GRP_LST_MERGE.shape)
print("-----MEDB_GRP_LST_MERGE---\n",MEDB_GRP_LST_MERGE)
#------------ set condition---
#---------drop column-----
MEDB_GRP_LST_MERGE_V1=MEDB_GRP_LST_MERGE.drop(["FILL_DRUG_FORMULARY_IND"], axis=1)
#------column rename
MEDB_GRP_LST_MERGE.rename(columns={"FORMULARY_IND":"FILL_DRUG_FORMULARY_IND"},inplace=True)


#---------9:59-----------update claims1---
#-----------merge----
medb_product_rh_merge=MEDB_CLAIM_PULL.merege(CLIENT_PROFILE_PRODUCT_RH,how="inner",on="group_operational_id")
print("--------------medb_product_rh_merge----\n",medb_product_rh_merge.shape)
print("--------------medb_product_rh_merge----\n",medb_product_rh_merge)
print("--------------medb_product_rh_merge----\n",medb_product_rh_merge.columns)

#-------where conditions---
medb_product_rh_merge_fil=medb_product_rh_merge[medb_product_rh_merge["END_EFF_DTE"]=="3000-12-31"]

#------------------set ondition----
# ------------------------------drop column name------------------
medb_product_rh_merge_fil_v1 = medb_product_rh_merge_fil.drop(["MAIL_PHCY_CDE"], axis=1)

#---------column rename---
# medb_product_rh_merge_fil_v1.rename(columns={"PHCY_PRODUCT_SERVICE_ID":"MAIL_PHCY_CDE"},inplace=True)

#-----------------------------------UPDATE 2
# -----------merge----
medb_product_rh_merge2=MEDB_CLAIM_PULL.merege(CLIENT_PROFILE_PRODUCT_RH,how="inner",on="group_operational_id")
print("--------------medb_product_rh_merge2----\n",medb_product_rh_merge2.shape)
print("--------------medb_product_rh_merge2----\n",medb_product_rh_merge2)
print("--------------medb_product_rh_merge2----\n",medb_product_rh_merge2.columns)
#+
# #-------where conditions---
medb_product_rh_merge_fil=medb_product_rh_merge2[(medb_product_rh_merge2["MAIL_PHCY_CDE"]==" ") &
                                                 (medb_product_rh_merge2["END_EFF_DTE"]=="3000-12-31")]

#------------------set ondition----
# ------------------------------drop column name------------------
medb_product_rh_merge_fil_V2 = medb_product_rh_merge_fil.drop(["MAIL_PHCY_CDE"], axis=1)

#---------column rename---
medb_product_rh_merge_fil_V2.rename(columns={"MAIL_SERVICE_PHCY_NME_CDE":"MAIL_PHCY_CDE"},inplace=True)

#-------------------------------------------------logic 9a started---------------
#-----------update 3--------------
#---------column rename---
PHARMACY_CONVERSION.rename(columns={"MAIL_PHCY_FILL_NBR":"MAIL_PHCY_FILL_NBR_v1"},inplace=True)
PHARMACY_CONVERSION.rename(columns={"PHCY_ACCOUNT_NBR":"PHCY_ACCOUNT_NBR_V1"},inplace=True)

# -----------merge----
medb_pharmacy_conversion_merge3=MEDB_CLAIM_PULL.merege(PHARMACY_CONVERSION,how="inner",on="MAIL_PHCY_CDE")
print("--------------medb_pharmacy_conversion_merge3----\n",medb_pharmacy_conversion_merge3.shape)
print("--------------medb_pharmacy_conversion_merge3----\n",medb_pharmacy_conversion_merge3)
print("--------------medb_pharmacy_conversion_merge3----\n",medb_pharmacy_conversion_merge3.columns)

#------------------set ondition----
# ------------------------------drop column name------------------
medb_pharmacy_conversion_merge3_finla= medb_pharmacy_conversion_merge3.drop(["MAIL_PHCY_CDE"], axis=1)
medb_pharmacy_conversion_merge3_finla = medb_pharmacy_conversion_merge3.drop(["MAIL_PHCY_CDE"], axis=1)

#---------column rename---
medb_pharmacy_conversion_merge3_finla.rename(columns={"MAIL_PHCY_FILL_NBR_v1":"MAIL_PHCY_FILL_NBR"},inplace=True)
medb_pharmacy_conversion_merge3_finla.rename(columns={"PHCY_ACCOUNT_NBR_V1":"PHCY_ACCOUNT_NBR"},inplace=True)

print("--------------medb_pharmacy_conversion_merge3_finla----\n",medb_pharmacy_conversion_merge3_finla.shape)
print("--------------medb_pharmacy_conversion_merge3_finla----\n",medb_pharmacy_conversion_merge3_finla.columns)

#----------------logic 9a completed-----
#--logic --------------------------9b ----------------
#--------------update 4---------------
HDP_NDC_LOOKUP.rename(columns={"RETAIL_NDC_NBR":"PHCY_PRODUCT_SERVICE_ID"})
#--------merge-----
medb_lookup_merge=MEDB_CLAIM_PULL.merge(HDP_NDC_LOOKUP,how="inner",on=["PHCY_PRODUCT_SERVICE_ID","MAIL_PHCY_FILL_NBR"])
print("--------------medb_lookup_merge----\n",medb_lookup_merge.shape)
print("--------------medb_lookup_merge----\n",medb_lookup_merge)
print("--------------medb_lookup_merge----\n",medb_lookup_merge.columns)

#--------------------set condition------
MAIL_PRODUCT_SERVICE_ID=np.where(MEDB_CLAIM_PULL["MAIL_RETAIL_CDE"]=="R","MAIL_NDC_NBR","MAIL_PRODUCT_SERVICE_ID")
#
# #-----------------------logic 9 b completed--------------
#
# #-------------logic 10 started-----------
# #-------------------------------------------------------logic 10 a----------------
# #------------import CLIENT_CURRENT table-----
CLIENT_CURRENT1=pd.read_sql(query.CLIENT_CURRENT,cnxn)
CLIENT_CURRENT1.columns=CLIENT_CURRENT1_COL
print("=-----CLIENT_CURRENT1---\n",CLIENT_CURRENT.shape)

#------where condition----
CLIENT_CURRENT1_fil=CLIENT_CURRENT1[(CLIENT_CURRENT1["CLIENT_TYPE_CDE"].isin('GR','BG')) &
                                    (CLIENT_CURRENT1["CARRIER_PRE_PROCESSING_IND"]=="Y")]
#------filter--
CLIENT_CURRENT1_fil_v1=CLIENT_CURRENT1_fil[['OPERATIONAL_ID','CARRIER_PRE_PROCESSING_IND']]

#--------------FIN_OPP_SCREEN5A---
FIN_OPP_SCREEN5A=CLIENT_CURRENT1_fil_v1
print("------FIN_OPP_SCREEN5A---\n",FIN_OPP_SCREEN5A.shape)
#---------------10 a completed----

#-----------------logic 10 b started-----
FIN_OPP_SCREEN5A_list=FIN_OPP_SCREEN5A["GROUP_NO"].tolist()

MEDB_CLAIMPULL_fil=MEDB_CLAIMPULL[MEDB_CLAIMPULL["ELIG_GROUP_OPERATIONAL_ID"].isin(FIN_OPP_SCREEN5A_list)]
print("----MEDB_CLAIMPULL_fil---\n",MEDB_CLAIMPULL_fil.shape)

#---------------------------------------logic 10 b second part-----
#----------update----
#----column rename---
FIN_OPP_SUBMIT_TYPE_GROUPS.rename(columns={"GROUP_OPERATIONLA_ID":"ELIG_GROUP_OPERATIONLA_ID"},inplace=True)
FIN_OPP_SUBMIT_TYPE_GROUPS.rename(columns={"CLAIMS_SUBMISSION_TYPE_CDE":"CLAIMS_SUBMISSION_TYPE_CDE_V1"},inplace=True)
#0-----merge----
medb_fin_submit_merge=MEDB_CLAIMPULL.merge(FIN_OPP_SUBMIT_TYPE_GROUPS,how="inner",on="ELIG_GROUP_OPERATIONLA_ID")
print("----medb_fin_submit_merge---\n",medb_fin_submit_merge.shape)
print("----medb_fin_submit_merge---\n",medb_fin_submit_merge)
print("----medb_fin_submit_merge---\n",medb_fin_submit_merge.columns)
#----set columns----
#----------drop column name----
medb_fin_submit_merge_v1 = medb_fin_submit_merge.drop(["CLAIMS_SUBMISSION_TYPE_CDE"], axis=1)
#----column rename---
medb_fin_submit_merge_v1.rename(columns={"CLAIMS_SUBMISSION_TYPE_CDE_V1":"CLAIMS_SUBMISSION_TYPE_CDE"},inplace=True)

#------------------------logic 10 c----
claims_list = ['A', '1', '2', '3', '4', '5', '6', '7']
MEDB_CLAIMPULL_drop=MEDB_CLAIMPULL[~MEDB_CLAIMPULL["CLAIMS_SUBMISSION_TYPE_CDE"].isin(claims_list)]

#----------------------------------------------------------logic 10 d-----
#------------DELETE QUERY------
MEDB_CLAIMPULL_fil3=MEDB_CLAIMPULL[(MEDB_CLAIMPULL["MAIL_RETAIL_CDE"]=="M") &
                                   (MEDB_CLAIMPULL["MAIL_SERVICE_RX_SRC_CDE"]==0) &
                                   (MEDB_CLAIMPULL["MAIL_SERVICE_RX_NBR"]=='00000000000') | (MEDB_CLAIMPULL["MAIL_SERVICE_RX_NBR"]==" ")]
print("----MEDB_CLAIMPULL_fil3----\n",MEDB_CLAIMPULL_fil3.shape)

#-----------------------------------------logic 10 E---
#----------------update queryyy--------
#------import PHARMACY_CURRENT--
PHARMACY_CURRENT1=pd.read_sql(query.PHARMACY_CURRENT,cnxn)
PHARMACY_CURRENT.columns=PHARMACY_CURRENT_COL

#-----column rename----
PHARMACY_CURRENT.rename(columns={"PHCY_PROVIDER_ID":"FILL_PHCY_PROVIDER_ID"},inplace=True)
PHARMACY_CURRENT.rename(columns={"PHCY_PROVIDER_SRC_IDE":"FILL_PHCY_PROVIDER_SRC_IDE"},inplace=True)

#---merge---
medb_claimpull_pharmacy_current_merge=MEDB_CLAIMPULL.merge(PHARMACY_CURRENT,how="inner",on=["FILL_PHCY_PROVIDER_ID","FILL_PHCY_PROVIDER_SRC_IDE"])
print("---medb_claimpull_pharmacy_current_merge---\n",medb_claimpull_pharmacy_current_merge.shape)
print("---medb_claimpull_pharmacy_current_merge---\n",medb_claimpull_pharmacy_current_merge)
print("---medb_claimpull_pharmacy_current_merge---\n",medb_claimpull_pharmacy_current_merge.columns)

#------where and set conditions---
medb_claimpull_pharmacy_current_merge["MAIL_RETAIL_CDE"]=np.where(medb_claimpull_pharmacy_current_merge["AFFILIATN_NBR"].isin(['1323','0779']),"M",medb_claimpull_pharmacy_current_merge["MAIL_RETAIL_CDE"])
print("---medb_claimpull_pharmacy_current_merge---\n",medb_claimpull_pharmacy_current_merge.columns)
#-----10 E completed---

#---------------------------------------logic 10 F ----
#---delete---
#----import FN_CARRIER_SPECIAL_EDITS--table---
FN_CARRIER_SPECIAL_EDITS1=pd.read_sql(query.FN_CARRIER_SPECIAL_EDITS,cnxn)
FN_CARRIER_SPECIAL_EDITS.columns=FN_CARRIER_SPECIAL_EDITS_COL

print("--FN_CARRIER_SPECIAL_EDITS1--\n",FN_CARRIER_SPECIAL_EDITS1.shape)
print("---FN_CARRIER_SPECIAL_EDITS1--\n",FN_CARRIER_SPECIAL_EDITS1)
print("=---FN_CARRIER_SPECIAL_EDITS1--\n",FN_CARRIER_SPECIAL_EDITS1.columns)

#------
FN_CARRIER_SPECIAL_EDITS1_LIST=FN_CARRIER_SPECIAL_EDITS1['CARRIER_OPERATIONAL_ID'].tolist()
MEDB_CLAIM_PULL_FIL_V1=MEDB_CLAIMPULL[(MEDB_CLAIMPULL['FILL_DRUG_IND']=="N") &
                                      (MEDB_CLAIMPULL["CARRIER_OPERATIONAL_ID"].isin(FN_CARRIER_SPECIAL_EDITS1_LIST))]

#-------------------------------------logic 10 G---
#---delete----query---
FN_CARRIER_SPECIAL_EDITS1_LIST1=FN_CARRIER_SPECIAL_EDITS1[['CARRIER_OPERATIONAL_ID']]
FN_CARRIER_SPECIAL_EDITS1_LIST1_FIL=FN_CARRIER_SPECIAL_EDITS1_LIST1['CARRIER_OPERATIONAL_ID'].tolist()

MEDICAL_PRODUCT_CURRENT_DEA_LIST=CARRIER_OPERATIONAL_ID[CARRIER_OPERATIONAL_ID["DEA_CDE"]!=0]
CARRIER_OPERATIONAL_ID_FIL=CARRIER_OPERATIONAL_ID[['PRODUCT_SERVICE_ID']]
CARRIER_OPERATIONAL_ID_LIST=CARRIER_OPERATIONAL_ID_FIL['PRODUCT_SERVICE_ID'].tolist()

MEDB_CLAIMPULL_FINAL_FILTERS_V1=MEDB_CLAIMPULL[(MEDB_CLAIMPULL["CARRIER_OPERATIONAL_ID"].isin(FN_CARRIER_SPECIAL_EDITS1_LIST1_FIL)) &
                                               (MEDB_CLAIMPULL["PRODUCT_SERVICE_ID"].isin(CARRIER_OPERATIONAL_ID_LIST))]
print("---MEDB_CLAIMPULL_FINAL_FILTERS_V1--\n",MEDB_CLAIMPULL_FINAL_FILTERS_V1.shape)

#----------------------------------------------logic 10 H----
#----delete query---
#---import FN_LIMITED_DIST_DRUGS------
FN_LIMITED_DIST_DRUGS1=pd.read_sql(query.FN_LIMITED_DIST_DRUGS,cnxn)
FN_LIMITED_DIST_DRUGS1.columns=FN_LIMITED_DIST_DRUGS1_COL
print("---FN_LIMITED_DIST_DRUGS1--\n",FN_LIMITED_DIST_DRUGS1.shape)

FN_CARRIER_SPECIAL_EDITS1_LIST1=FN_CARRIER_SPECIAL_EDITS1[[CARRIER_OPERATIONAL_ID]]
FN_CARRIER_SPECIAL_EDITS1_LIST1_FIL_v1=FN_CARRIER_SPECIAL_EDITS1_LIST1['CARRIER_OPERATIONAL_ID'].tolist()

FN_LIMITED_DIST_DRUGS_lis=FN_LIMITED_DIST_DRUGS1[['PRODUCT_SERVICE_ID']]
FN_LIMITED_DIST_DRUGS1_FIL=FN_LIMITED_DIST_DRUGS_lis['PRODUCT_SERVICE_ID'].tolist()

#-----condition--
MEDB_CLAIM_PULL_FINAL_TAB=MEDB_CLAIMPULL[(MEDB_CLAIMPULL["CARRIER_OPERATIONAL_ID"].isin(FN_CARRIER_SPECIAL_EDITS1_LIST1_FIL_v1)) &
                                         ([MEDB_CLAIMPULL["PRODUCT_SERVICE_ID"].isin(FN_LIMITED_DIST_DRUGS1_FIL)])]

#------------------------------------------------COMPLETED-------------------------------------
# ---------------------------explain logic 10g-------------------------------------------------------
