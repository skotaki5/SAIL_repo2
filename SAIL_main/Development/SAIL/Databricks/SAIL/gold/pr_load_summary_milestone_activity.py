# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC Author           : Arpan Bhardwaj  </br>
# MAGIC Description      : This notebook is to load summary_milestone_activity table. </br>
# MAGIC version 1.1      : Added new columns as per change log story#UPSGLD-14945.

# COMMAND ----------

# DBTITLE 1,Import Python Libraries
import datetime,time,json
from pytz import timezone
from  pyspark.sql.functions import input_file_name ,current_timestamp,split ,size, from_utc_timestamp, lit  ,concat, when, col, sha1, row_number

# COMMAND ----------

spark.conf.set('spark.sql.autoBroadcastJoinThreshold','52428800b')

# COMMAND ----------

# DBTITLE 1,Import Common Variables
# MAGIC %run "/SAIL/includes/common_variables"

# COMMAND ----------

# DBTITLE 1,Import Common Utilities
# MAGIC %run "/SAIL/includes/common_udfs"

# COMMAND ----------

start_time = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# DBTITLE 1,Set logger
dbutils.widgets.text("log_debug_mode", "","")
dbutils.widgets.get("log_debug_mode")
log_debug_mode = getArgument("log_debug_mode").strip()

if log_debug_mode == "Y":
  logger = _get_logger(time_zone,logging.DEBUG)  
else:
  logger = _get_logger(time_zone,logging.INFO)

# COMMAND ----------

# DBTITLE 1,delta_query
def get_delta_query(hwm):
  logger.debug("hwm: " + str(hwm))
  query ="""
        CREATE
            OR replace TEMP VIEW delta_fetch_tv 
        AS      
        SELECT ups_order_number
        FROM {fact_order_dim_inc} FTO
        WHERE dl_update_timestamp >= '{hwm}'
        and FTO.ORDER_PLACED_DATE BETWEEN date_sub('{hwm}', {days_back}) AND current_timestamp
        
        UNION 
        
        SELECT CASE 
                WHEN NVL(FTTR.TRANS_ONLY_FLAG, 'NULL') <> 'TRANS_ONLY'
                    AND UPS_WMS_ORDER_NUMBER IS NOT NULL
                    THEN UPS_WMS_ORDER_NUMBER
                ELSE UPS_ORDER_NUMBER
                END AS ups_order_number
        FROM {fact_transportation} FTTR
        WHERE dl_update_timestamp >= '{hwm}'
        
        UNION 
        
        SELECT ups_order_number
        FROM {fact_transport_details}
        WHERE dl_update_timestamp >= '{hwm}'
        
        UNION 
        
        SELECT ups_order_number
        FROM {fact_milestone_activity}
        WHERE dl_update_timestamp >= '{hwm}'
          """.format(**source_tables,hwm=hwm,days_back=days_back)
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

def get_delta_query_view1(hwm):
  logger.debug("hwm: " + str(hwm))
  query ="""
        CREATE
            OR replace TEMP VIEW fact_order_dim_inc_vw 
        AS      
        Select FTO.*  
        FROM {fact_order_dim_inc}  FTO
        INNER JOIN delta_fetch_tv FTV on (FTV.ups_order_number= FTO.UPS_ORDER_NUMBER)
        WHERE FTO.ORDER_PLACED_DATE BETWEEN date_sub('{hwm}', {days_back}) AND current_timestamp
        """.format(**source_tables,hwm=hwm,days_back=days_back)
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

# DBTITLE 1,Query 1
def get_query1(): 
  query="""
CREATE OR REPLACE TEMP VIEW fact_order_tv
as
SELECT DISTINCT            
   	FTO.UPS_ORDER_NUMBER,
    FTO.GLD_ACCOUNT_MAPPED_KEY,            
    FTO.DP_SERVICELINE_KEY,
    FTO.FacilityId as GLD_WAREHOUSE_MAPPED_KEY,             
    FTO.SOURCE_SYSTEM_KEY,             
	FTO.SOURCE_SYSTEM_NAME,               
    FTTR.UPS_ORDER_NUMBER AS UPS_TRANSPORT_ORDER_NUMBER,            
    FTD.UPS_ORDER_NUMBER AS UPS_TRANSPORT_INBOUND_ORDER_NUMBER,            
    FTD.CLIENT_ASN AS ClientASNNumber,            
    FTO.TRANSACTION_TYPE_ID AS TransactionTypeId,
    FTO.IS_MANAGED	
FROM fact_order_dim_inc_vw  FTO
LEFT JOIN {fact_transportation} FTTR ON       
           (CASE WHEN FTTR.TRANS_ONLY_FLAG <> 'TRANS_ONLY'   
                 THEN   NVL(FTTR.UPS_WMS_SOURCE_SYSTEM_KEY,FTO.SOURCE_SYSTEM_KEY)   
           ELSE FTTR.SOURCE_SYSTEM_KEY END = FTO.SOURCE_SYSTEM_KEY       
        AND CASE WHEN FTTR.TRANS_ONLY_FLAG <> 'TRANS_ONLY'   
                 THEN FTTR.UPS_WMS_ORDER_NUMBER   
           ELSE FTTR.UPS_ORDER_NUMBER END = FTO.UPS_ORDER_NUMBER)   
LEFT JOIN {fact_transport_details} FTD ON   FTO.UPS_ORDER_NUMBER = FTD.UPS_ORDER_NUMBER    
              AND FTO.SOURCE_SYSTEM_KEY = FTD.SOURCE_SYSTEM_KEY      
      """.format(**source_tables)
  logger.debug("query : " + query)  
  return (query)

  

# COMMAND ----------

# DBTITLE 1,Query 2
def get_query2(hwm):
  query ="""
CREATE OR REPLACE TEMP VIEW FACT_MILESTONE_ACTIVITY_TV
as
SELECT      
  FA.UPS_ORDER_NUMBER,      
  FA.ACTIVITY_DATE,      
  FA.ACTIVITY_COMPLETION_FLAG,      
  FA.PLANNED_MILESTONE_DATE,      
  FA.TRACKING_NUMBER,      
  FA.ACTIVITY_CODE,      
  CASE When FA.SOURCE_SYSTEM_KEY = '1019' Then FA.UPS_WMS_SOURCE_SYSTEM_KEY Else FA.SOURCE_SYSTEM_KEY End SOURCE_SYSTEM_KEY,  
  FA.SEGMENT_ID,  
  FA.ACTIVITY_NOTES,  
  FA.VENDOR_NAME,  
  FA.PROOF_OF_DELIVERY_NAME,  
  FA.CARRIER_TYPE,
  FA.ETL_BATCH_NUMBER,              
  FA.LOAD_TRACK_SDUK,
  FA.FTZ_STATUS,
  FA.TIME_ZONE,
  FA.LOGI_NEXT_FLAG ,
  FA.PROOF_OF_DELIVERY_LOCATION,--UPSGLD-14945
  FA.PROOF_OF_DELIVERY_DATE_TIME,--UPSGLD-14945
  FA.LATITUDE, --UPSGLD-14945
  FA.LONGITUDE, --UPSGLD-14945
  FA.ACTIVITY_STATUS,
  MA.MilestoneName,      
  MA.ActivityCode,      
  MA.ActivityName,
  MA.Milestone_Completion_Flag,  
  FA.activity_month_part_key
FROM {fact_milestone_activity} FA 
INNER JOIN delta_fetch_tv FTV on (FTV.ups_order_number= FA.UPS_ORDER_NUMBER)
INNER JOIN {dim_customer} CL ON FA.CLIENT_KEY = CL.CUSTOMERKEY 
	AND CASE When FA.SOURCE_SYSTEM_KEY = '1019' Then FA.UPS_WMS_SOURCE_SYSTEM_KEY Else FA.SOURCE_SYSTEM_KEY End = CL.SOURCE_SYSTEM_KEY
INNER JOIN {account_type_digital}  GLAT ON CL.GLD_ACCOUNT_MAPPED_KEY = GLAT.Account_ID  
LEFT JOIN {map_milestone_activity} MA on FA.ACTIVITY_CODE = MA.ActivityCode AND FA.SOURCE_SYSTEM_KEY = MA.SOURCE_SYSTEM_KEY
where (FA.ACTIVITY_DATE between date('{hwm}') - {days_back} and current_date + {days_back}
or
FA.ACTIVITY_DATE is null)
""".format(**source_tables,days_back=days_back,hwm=hwm)
  logger.debug("query : " + query)
  return (query)


# COMMAND ----------

# DBTITLE 1,Query 3
def get_query3(lit_mercurygate):
  logger.debug("lit_mercurygate: " + lit_mercurygate)
  query = """
  SELECT SOURCE_SYSTEM_KEY FROM {dim_source_system} WHERE SOURCE_SYSTEM_NAME = '{lit_mercurygate}'
  """.format(lit_mercurygate=lit_mercurygate,**source_tables)
  logger.debug("query : " + query)
  return (query)

# COMMAND ----------

# DBTITLE 1,Query 4
def get_query4(SS_MG):
  logger.debug("SS_MG: {SS_MG}".format(SS_MG=SS_MG))
  query="""
CREATE OR REPLACE TEMP VIEW INBOUND_ASN_TV
as
SELECT
	TD.UPS_ORDER_NUMBER,
	TD.SOURCE_SYSTEM_KEY,
	CASE WHEN TD.IS_MANAGED = 0 THEN TD.UPS_ORDER_NUMBER ELSE  FTO.UPS_ORDER_NUMBER END UPSASNNumber,
	FTO.SOURCE_SYSTEM_KEY AS ASN_SOURCE_SYSTEM_KEY     
FROM fact_order_tv TD          
LEFT JOIN fact_order_dim_inc_vw FTO ON  TD.ClientASNNumber = FTO.CUSTOMER_ORDER_NUMBER AND FTO.IS_ASN = 1  
AND case when  TD.SOURCE_SYSTEM_KEY = {SS_MG} AND TD.ClientASNNumber <> ''    then 1017 else TD.SOURCE_SYSTEM_KEY end = FTO.SOURCE_SYSTEM_KEY    AND case when  TD.SOURCE_SYSTEM_KEY = {SS_MG} AND TD.ClientASNNumber <> ''  then FTO.UPS_ORDER_NUMBER  else TD.UPS_ORDER_NUMBER end = FTO.UPS_ORDER_NUMBER       
  """.format(SS_MG=SS_MG,**source_tables)
  logger.debug("query : " + query)
  return (query)

# COMMAND ----------

# DBTITLE 1,Query 5
def get_query5():
  query = """
  CREATE or REPLACE TEMP VIEW DIGITAL_SUMMARY_MILESTONE_ACTIVITY_STG
AS
  SElECT distinct                   
   TBL.SOURCE_SYSTEM_KEY                               AS SourceSystemKey                     
  ,SOURCE_SYSTEM_NAME                                  AS SourceSystemName                
  ,TBL.GLD_ACCOUNT_MAPPED_KEY                          AS AccountId                
  ,GLD_WAREHOUSE_MAPPED_KEY                            AS FacilityId                     
  ,UPSOrderNumber                                        
  ,UPSASNNumber                                        
  ,TRACKING_NUMBER                                     AS TrackingNumber                    
  ,TBL.TransactionTypeId                                            
  ,cast(NULL as int)                                   AS MilestoneId              
  ,TM.MilestoneName                                    AS MilestoneName                  
  ,cast(NULL as int)                                   AS ActivityId                      
  ,ActivityName                                        AS ActivityName  
  ,ACTIVITY_STATUS                                     AS ACTIVITY_STATUS
  ,ACTIVITY_DATE                                       AS ActivityDate               
  ,CASE WHEN ACTIVITY_DATE IS NULL 
           THEN 'N' 
	   ELSE 'Y' 
  END                                                  AS ActivityCompletionFlag                     
  ,PLANNED_MILESTONE_DATE                              AS PlannedMilestoneDate                
  ,CASE WHEN Milestone_Completion_Flag = 'Y' 
       THEN ACTIVITY_DATE 
  END                                                  AS MilestoneDate                
  ,CASE WHEN Milestone_Completion_Flag <> 'Y' 
       THEN 'N' 
	   ELSE Milestone_Completion_Flag 
  END                                                  AS MilestoneCompletionFlag                
  ,TM.MilestoneOrder                
  ,''                                                  AS CurrentMilestoneFlag                
  ,SEGMENT_ID                
  ,ACTIVITY_NOTES                
  ,VENDOR_NAME      
  ,PROOF_OF_DELIVERY_NAME
  ,CARRIER_TYPE
  ,0                                             AS P_Flag              
  ,LOAD_TRACK_SDUK
  ,ActivityCode
  ,FTZ_STATUS
  ,TIME_ZONE as TimeZone
  ,LOGI_NEXT_FLAG
  ,PROOF_OF_DELIVERY_LOCATION --UPSGLD-14945
  ,PROOF_OF_DELIVERY_DATE_TIME --UPSGLD-14945
  ,LATITUDE --UPSGLD-14945
  ,LONGITUDE --UPSGLD-14945
  ,activity_month_part_key
FROM      
(      
SELECT    
  FTO.GLD_ACCOUNT_MAPPED_KEY,        
  FTO.GLD_WAREHOUSE_MAPPED_KEY,
  FTO.SOURCE_SYSTEM_KEY, 
  FTO.SOURCE_SYSTEM_NAME,
  FTO.TransactionTypeId,
  FA.MilestoneName,      
  FA.ActivityCode,      
  FA.ActivityName,
  FA.ACTIVITY_STATUS,
  FA.Milestone_Completion_Flag,      
  NVL(UPS_TRANSPORT_INBOUND_ORDER_NUMBER,FTO.UPS_ORDER_NUMBER) AS UPSOrderNumber,        
  CASE WHEN FTO.IS_MANAGED = 0 THEN FTO.UPS_ORDER_NUMBER ELSE  NULL END UPSASNNumber,      
  FA.ACTIVITY_DATE,      
  FA.ACTIVITY_COMPLETION_FLAG,      
  FA.PLANNED_MILESTONE_DATE,      
  FA.TRACKING_NUMBER,  
  FA.SEGMENT_ID,  
  FA.ACTIVITY_NOTES,  
  FA.VENDOR_NAME,  
  FA.PROOF_OF_DELIVERY_NAME,  
  FA.CARRIER_TYPE,
  FA.ETL_BATCH_NUMBER,              
  FA.LOAD_TRACK_SDUK,
  FA.FTZ_STATUS,
  FA.TIME_ZONE,
  FA.LOGI_NEXT_FLAG,
  FA.PROOF_OF_DELIVERY_LOCATION, --UPSGLD-14945
  FA.PROOF_OF_DELIVERY_DATE_TIME, --UPSGLD-14945
  FA.LATITUDE, --UPSGLD-14945
  FA.LONGITUDE, --UPSGLD-14945
  FA.activity_month_part_key
FROM FACT_ORDER_TV FTO      
INNER JOIN FACT_MILESTONE_ACTIVITY_TV FA  ON FA.UPS_ORDER_NUMBER = FTO.UPS_ORDER_NUMBER AND FTO.SOURCE_SYSTEM_KEY = FA.SOURCE_SYSTEM_KEY        
UNION       
SELECT 
  FTO.GLD_ACCOUNT_MAPPED_KEY,        
  FTO.GLD_WAREHOUSE_MAPPED_KEY,
  FTO.SOURCE_SYSTEM_KEY, 
  FTO.SOURCE_SYSTEM_NAME,
  FTO.TransactionTypeId,
  FA.MilestoneName,      
  FA.ActivityCode,      
  FA.ActivityName,   
  FA.ACTIVITY_STATUS,
  FA.Milestone_Completion_Flag,      
  FTO.UPS_ORDER_NUMBER AS UPSOrderNumber,      
  CASE WHEN FTO.IS_MANAGED = 0 THEN FTO.UPS_ORDER_NUMBER ELSE  NULL END UPSASNNumber,      
  FA.ACTIVITY_DATE,      
  FA.ACTIVITY_COMPLETION_FLAG,      
  FA.PLANNED_MILESTONE_DATE,      
  FA.TRACKING_NUMBER,  
  FA.SEGMENT_ID,  
  FA.ACTIVITY_NOTES,  
  FA.VENDOR_NAME,  
  FA.PROOF_OF_DELIVERY_NAME,  
  FA.CARRIER_TYPE,
  FA.ETL_BATCH_NUMBER,              
  FA.LOAD_TRACK_SDUK,
  FA.FTZ_STATUS,
  FA.TIME_ZONE,
  FA.LOGI_NEXT_FLAG,
  FA.PROOF_OF_DELIVERY_LOCATION, --UPSGLD-14945
  FA.PROOF_OF_DELIVERY_DATE_TIME, --UPSGLD-14945
  FA.LATITUDE, --UPSGLD-14945
  FA.LONGITUDE, --UPSGLD-14945
  FA.activity_month_part_key
FROM  FACT_ORDER_TV  FTO      
INNER JOIN FACT_MILESTONE_ACTIVITY_TV FA ON FA.UPS_ORDER_NUMBER =  FTO.UPS_TRANSPORT_ORDER_NUMBER  
and  FTO.UPS_TRANSPORT_ORDER_NUMBER IS NOT NULL and  FA.SOURCE_SYSTEM_KEY = 1011
UNION 
SELECT 
  FTO.GLD_ACCOUNT_MAPPED_KEY,        
  FTO.GLD_WAREHOUSE_MAPPED_KEY,
  FTO.SOURCE_SYSTEM_KEY, 
  FTO.SOURCE_SYSTEM_NAME,
  FTO.TransactionTypeId,
  FA.MilestoneName,      
  FA.ActivityCode,      
  FA.ActivityName,   
  FA.ACTIVITY_STATUS,
  FA.Milestone_Completion_Flag,      
  FTO.UPS_ORDER_NUMBER AS UPSOrderNumber,      
  CASE WHEN FTO.IS_MANAGED = 0 THEN FTO.UPS_ORDER_NUMBER ELSE  NULL END UPSASNNumber,      
  FA.ACTIVITY_DATE,      
  FA.ACTIVITY_COMPLETION_FLAG,      
  FA.PLANNED_MILESTONE_DATE,      
  FA.TRACKING_NUMBER,  
  FA.SEGMENT_ID,  
  FA.ACTIVITY_NOTES,  
  FA.VENDOR_NAME,  
  FA.PROOF_OF_DELIVERY_NAME,  
  FA.CARRIER_TYPE,
  FA.ETL_BATCH_NUMBER,              
  FA.LOAD_TRACK_SDUK,
  FA.FTZ_STATUS,
  FA.TIME_ZONE,
  FA.LOGI_NEXT_FLAG,
  FA.PROOF_OF_DELIVERY_LOCATION, --UPSGLD-14945
  FA.PROOF_OF_DELIVERY_DATE_TIME, --UPSGLD-14945
  FA.LATITUDE, --UPSGLD-14945
  FA.LONGITUDE, --UPSGLD-14945
  FA.activity_month_part_key
FROM FACT_ORDER_TV FTO      
INNER JOIN FACT_MILESTONE_ACTIVITY_TV FA  ON FA.UPS_ORDER_NUMBER =  FTO.UPS_TRANSPORT_ORDER_NUMBER AND FTO.SOURCE_SYSTEM_KEY = FA.SOURCE_SYSTEM_KEY       
UNION      
SELECT 
  FTO.GLD_ACCOUNT_MAPPED_KEY,        
  FTO.GLD_WAREHOUSE_MAPPED_KEY,
  FTO.SOURCE_SYSTEM_KEY, 
  FTO.SOURCE_SYSTEM_NAME,
  FTO.TransactionTypeId,
  FA.MilestoneName,      
  FA.ActivityCode,      
  FA.ActivityName,  
  FA.ACTIVITY_STATUS,
  FA.Milestone_Completion_Flag,      
  FTO.UPS_ORDER_NUMBER AS UPSOrderNumber,      
  IA.UPSASNNumber,      
  FA.ACTIVITY_DATE,      
  FA.ACTIVITY_COMPLETION_FLAG,      
  FA.PLANNED_MILESTONE_DATE,      
  FA.TRACKING_NUMBER,  
  FA.SEGMENT_ID,  
  FA.ACTIVITY_NOTES,  
  FA.VENDOR_NAME,  
  FA.PROOF_OF_DELIVERY_NAME,  
  FA.CARRIER_TYPE,
  FA.ETL_BATCH_NUMBER,              
  FA.LOAD_TRACK_SDUK,
  FA.FTZ_STATUS,
  FA.TIME_ZONE,
  FA.LOGI_NEXT_FLAG,
  FA.PROOF_OF_DELIVERY_LOCATION, --UPSGLD-14945
  FA.PROOF_OF_DELIVERY_DATE_TIME, --UPSGLD-14945
  FA.LATITUDE, --UPSGLD-14945
  FA.LONGITUDE, --UPSGLD-14945
  FA.activity_month_part_key
FROM FACT_ORDER_TV FTO      
INNER JOIN INBOUND_ASN_TV IA ON IA.UPS_ORDER_NUMBER = FTO.UPS_ORDER_NUMBER AND IA.SOURCE_SYSTEM_KEY = FTO.SOURCE_SYSTEM_KEY       
INNER JOIN FACT_MILESTONE_ACTIVITY_TV FA  ON  IA.UPSASNNumber  = FA.UPS_ORDER_NUMBER AND IA.ASN_SOURCE_SYSTEM_KEY = FA.SOURCE_SYSTEM_KEY      
) TBL      
LEFT JOIN {map_transactiontype_milestone} TM ON TBL.TransactionTypeId = TM.TransactionTypeId   
                   AND TBL.MilestoneName = TM.MilestoneName 
  """.format(**source_tables)
  logger.debug("query : " + query)
  return (query)

# COMMAND ----------

# DBTITLE 1,Query 6
def get_query6():
  query = f"""
  CREATE OR REPLACE TEMP VIEW MAX_Milestone_tv
as
SELECT  UPSOrderNumber,SourceSystemKey,AccountId, MAX(MilestoneOrder) AS MilestoneOrder          
FROM DIGITAL_SUMMARY_MILESTONE_ACTIVITY_STG   
where ActivityDate is not null and ActivityDate >=current_date - {days_back}
GROUP BY UPSOrderNumber, SourceSystemKey,AccountId 
  """.format(**source_tables)
  logger.debug("query : " + query)
  return (query)

# COMMAND ----------

# DBTITLE 1,Query 7
def get_query7():
  query = f"""
  create or replace temp view DIGITAL_SUMMARY_MILESTONE_ACTIVITY_STG1
as 
With Current_MAX_Milestone as
(
SELECT  UPSOrderNumber,SourceSystemKey,AccountId, MIN(ActivityDate) AS ActivityDate,MAX(MilestoneOrder) AS MilestoneOrder      
FROM DIGITAL_SUMMARY_MILESTONE_ACTIVITY_STG         
WHERE MilestoneCompletionFlag = 'Y' and MilestoneDate IS NOT NULL and ActivityDate is not null and ActivityDate >=current_date - {days_back}
GROUP BY UPSOrderNumber, SourceSystemKey, AccountId    
),
Current_Milestone as
(    
SELECT    
NVL(CMA.UPSOrderNumber,MM.UPSOrderNumber) UPSOrderNumber, MM.SourceSystemKey,MM.AccountId,  
ActivityDate,  
CASE WHEN NVL(CMA.MilestoneOrder,MM.MilestoneOrder) = MM.MilestoneOrder   
	 THEN NVL(CMA.MilestoneOrder,MM.MilestoneOrder)   
	 ELSE CMA.MilestoneOrder+1 END MilestoneOrder        
FROM MAX_Milestone_tv MM       
LEFT JOIN Current_MAX_Milestone CMA      
ON MM.UPSOrderNumber = CMA.UPSOrderNumber AND MM.SourceSystemKey = CMA.SourceSystemKey AND MM.AccountId = CMA.AccountId   
)
select SO.SourceSystemKey
        ,SO.SourceSystemName
        ,SO.AccountId
        ,SO.FacilityId
        ,SO.UPSOrderNumber
        ,SO.UPSASNNumber
        ,SO.TrackingNumber
        ,SO.TransactionTypeId
        ,SO.MilestoneId
        ,SO.MilestoneName
        ,SO.ActivityId
        ,SO.ActivityName
        ,SO.ACTIVITY_STATUS
        ,SO.ActivityDate
        ,SO.ActivityCompletionFlag
        ,SO.PlannedMilestoneDate
        ,SO.MilestoneDate
        ,SO.MilestoneCompletionFlag
        ,SO.MilestoneOrder
        ,case WHEN CM.UPSOrderNumber is not null THEN 'Y' END as CurrentMilestoneFlag
        ,SO.SEGMENT_ID
        ,SO.ACTIVITY_NOTES
        ,SO.VENDOR_NAME
        ,SO.PROOF_OF_DELIVERY_NAME
        ,SO.CARRIER_TYPE
        ,SO.P_Flag
        ,SO.LOAD_TRACK_SDUK
        ,SO.ActivityCode
        ,SO.FTZ_STATUS
        ,SO.TimeZone
        ,SO.LOGI_NEXT_FLAG
        ,SO.PROOF_OF_DELIVERY_LOCATION --UPSGLD-14945
        ,SO.PROOF_OF_DELIVERY_DATE_TIME --UPSGLD-14945
        ,SO.LATITUDE --UPSGLD-14945
        ,SO.LONGITUDE --UPSGLD-14945
        ,SO.activity_month_part_key
        FROM DIGITAL_SUMMARY_MILESTONE_ACTIVITY_STG SO        
        LEFT JOIN Current_Milestone CM      
            ON SO.UPSOrderNumber = CM.UPSOrderNumber    
            AND SO.MilestoneOrder = CM.MilestoneOrder
            AND SO.SourceSystemKey = CM.SourceSystemKey
            AND SO.AccountId = CM.AccountId
  """.format(**source_tables)
  logger.debug("query : " + query)
  return (query)

# COMMAND ----------

# DBTITLE 1,Query 8
def get_query8():
  query = """
  with exclude_temp as
( SELECT distinct UPSOrderNumber,SourceSystemKey, AccountId
  FROM DIGITAL_SUMMARY_MILESTONE_ACTIVITY_STG1 
  WHERE  CurrentMilestoneFlag = 'Y' and ActivityDate is null
 )
 select SO.SourceSystemKey
        ,SO.SourceSystemName
        ,SO.AccountId
        ,SO.FacilityId
        ,SO.UPSOrderNumber
        ,SO.TrackingNumber
        ,cast(null as string) Source_Activity_Code
        ,cast(null as string) Source_Activity_Name
        ,SO.TransactionTypeId
        ,SO.MilestoneId
        ,SO.MilestoneName
        ,SO.ActivityId
        ,SO.ActivityCode
        ,SO.ActivityName
        ,SO.ACTIVITY_STATUS
        ,SO.ActivityDate
        ,SO.ActivityCompletionFlag
        ,SO.PlannedMilestoneDate
        ,SO.MilestoneDate
        ,SO.MilestoneCompletionFlag
        ,SO.MilestoneOrder
        ,case WHEN (EX.UPSOrderNumber is null and MM.UPSOrderNumber is not null) THEN 'Y' 
              ELSE SO.CurrentMilestoneFlag 
         END as CurrentMilestoneFlag
        ,SO.UPSASNNumber
        ,SO.SEGMENT_ID
        ,SO.ACTIVITY_NOTES
        ,SO.VENDOR_NAME
        ,SO.PROOF_OF_DELIVERY_NAME
        ,SO.CARRIER_TYPE
        ,SO.P_Flag
        ,SO.LOAD_TRACK_SDUK
        ,SO.FTZ_STATUS
        ,SO.TimeZone
        ,SO.LOGI_NEXT_FLAG
        ,SO.PROOF_OF_DELIVERY_LOCATION --UPSGLD-14945
        ,SO.PROOF_OF_DELIVERY_DATE_TIME --UPSGLD-14945
        ,SO.LATITUDE --UPSGLD-14945
        ,SO.LONGITUDE --UPSGLD-14945
        ,SO.activity_month_part_key
        ,cast (NULL as integer) as Batch_id
        ,case when SO.ActivityDate is null then 1 else 0 end as is_deleted
        FROM DIGITAL_SUMMARY_MILESTONE_ACTIVITY_STG1 SO
        LEFT JOIN exclude_temp EX
            ON (  SO.UPSOrderNumber = EX.UPSOrderNumber    
                 AND SO.SourceSystemKey = EX.SourceSystemKey
                 AND SO.AccountId = EX.AccountId
                 )
        LEFT JOIN MAX_Milestone_tv MM      
            ON
          ( SO.UPSOrderNumber = MM.UPSOrderNumber    
            AND SO.MilestoneOrder = MM.MilestoneOrder
            AND SO.SourceSystemKey = MM.SourceSystemKey
            AND SO.AccountId = MM.AccountId
           )
  """
  logger.debug("query : " + query)
  return (query)

# COMMAND ----------

def main():
    try:
        pid_get = get_pid()
        logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
        pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
        logger.info("pid: {pid}".format(pid=pid))
        ############################ ETL AUDIT #########################################################
        audit_result['process_id'] = pid
        audit_result['process_name'] = 'pr_load_summary_milestone_activity'
        audit_result['process_type'] = 'DataBricks'
        audit_result['layer'] = 'gold'
        audit_result['table_name'] = digital_summary_milestone_activity_et
        audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
        audit_result['start_time'] = start_time
        ################################################################################################
        hwm=get_hwm('gold',digital_summary_milestone_activity_et)
        logger.info(f'hwm {digital_summary_milestone_activity_et}: {hwm}')
        
        spark.sql(get_delta_query(hwm))
        logger.info("get_delta_query finished")   
        
        spark.sql(get_delta_query_view1(hwm))
        logger.info("get_delta_query_view1 finished")   
          
        spark.sql(get_query1())
        logger.info("query 1 finished")
        
        spark.sql(get_query2(hwm))
        logger.info("query 2 finished")
        
        lit_mercurygate='MERCURYGATE'
        #SS_MG_df = spark.sql(get_query3(lit_mercurygate))
        #SS_MG=SS_MG_df.collect()[0][0]
        SS_MG=1011
        logger.info("query 3 finished")
        
        spark.sql(get_query4(SS_MG))
        logger.info("query 4 finished")
        
        spark.sql(get_query5())
        logger.info("query 5 finished")
        
        spark.sql(get_query6())
        logger.info("query 6 finished")
        
        spark.sql(get_query7())
        logger.info("query 7 finished")
        
        src_df =spark.sql(get_query8())
       
        logger.info("query 8 finished")
        
        ##################### generating hash key  #############################
        hash_key_columns = ['SourceSystemKey','AccountId','FacilityId','UPSOrderNumber','TransactionTypeId','ActivityCode','LOAD_TRACK_SDUK']
        logger.debug(f"columns: {hash_key_columns}")
  
        logger.debug("Adding hash_key")
        src_df = src_df.withColumn("hash_key", sha1_concat(hash_key_columns))
        ##################### DL hash  ######################################
        logger.debug("Generating dl_hash")
        columns = src_df.schema.fieldNames()
        logger.debug("columns: {columns}".format(columns=columns))
        hash_exclude_col = []
        logger.debug("hash_exclude_col: {hash_exclude_col}".format(hash_exclude_col=hash_exclude_col))
        hash_col = subtract_list(columns,hash_exclude_col)
        logger.debug("hash_col: {hash_col}".format(hash_col=hash_col))
  
        logger.debug("Adding hash")
        src_df = src_df.withColumn("dl_hash", sha1_concat(hash_col))
        
        ##################### audit columns  ######################################
        logger.debug("Adding audit columns")
        src_df = add_audit_columns(src_df, pid,datetime.now(),datetime.now())
  
        primary_keys = ['SourceSystemKey','UPSOrderNumber','AccountId','FacilityId','TransactionTypeId','ActivityCode','LOAD_TRACK_SDUK']
        logger.debug('primary_keys: {primary_keys}'.format(primary_keys=primary_keys))
        
        ##################### merge operation  ######################################
        logger.info(f'Merging to delta path: {digital_summary_milestone_activity_path}')
    
        mergeToDelta(src_df,digital_summary_milestone_activity_path,primary_keys)
        logger.info(f'merging to delta path finished: {digital_summary_milestone_activity_path}')
        
        logger.info('setting hwm')
        res=set_hwm('gold',digital_summary_milestone_activity_et,start_time,pid)
        logger.info(res)
        
        ############################ ETL AUDIT #########################################################
        audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
        audit_result['status'] = 'success'
        
        logger.info('Process finished successfully.')
        #Writing into Delta format and saving into Gold Layer Table
    except Exception as e:
        audit_result['status'] = 'failed'
        audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
        audit_result['ERROR_MESSAGE'] = str(e)
        audit(audit_result)
        logger.info("audit_result".format(audit_result=audit_result))
        raise
    finally:
        logger.info("audit_result: {audit_result}".format(audit_result=audit_result))
        audit(audit_result)
        

# COMMAND ----------

main()