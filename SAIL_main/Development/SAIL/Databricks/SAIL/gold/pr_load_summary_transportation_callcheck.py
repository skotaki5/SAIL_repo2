# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC <b>Author </b>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;: GD000012733 Vishal </br>
# MAGIC <b>Description</b>  : this notebook is to load transportation call check table.</br>
# MAGIC 
# MAGIC <b>V1.1 </b>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;: GD000012734 Mahesh Rathi </br>
# MAGIC <b>Description</b>  : Fetch approriate value for Latitude,longitude,devicetagid,locationmethod fields which was default null earlier

# COMMAND ----------

# DBTITLE 1,Import Python Libraries
import datetime,time,json
from pytz import timezone
from  pyspark.sql.functions import input_file_name ,current_timestamp,split ,size, from_utc_timestamp, lit  ,concat, when, col, sha1, row_number

# COMMAND ----------

# DBTITLE 1,Importing common variables
# MAGIC %run "/SAIL/includes/common_variables"

# COMMAND ----------

# DBTITLE 1,Importing common udfs
# MAGIC %run "/SAIL/includes/common_udfs"

# COMMAND ----------

start_time = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# DBTITLE 1,Declaring Temporary tables
var_fact_order_summary_tv='FACT_ORDER_SUMMARY_tv'
var_initial_temp_check='INITIAL_TEMP_CHECK'
var_temp_call_check='TEMP_CALL_CHECK'
#temp table is used within this notebook

# COMMAND ----------

# DBTITLE 1,Setting logger
dbutils.widgets.text("log_debug_mode", "","")
dbutils.widgets.get("log_debug_mode")
log_debug_mode = getArgument("log_debug_mode").strip()

if log_debug_mode == "Y":
  logger = _get_logger(time_zone,logging.DEBUG)  
else:
  logger = _get_logger(time_zone,logging.INFO)

# COMMAND ----------

def get_check_load_query(hwm):
  logger.debug("hwm: " + str(hwm))
  query ="""
       select ups_order_number  from {fact_transportation_callcheck}   where dl_update_timestamp>'{hwm}'
        """.format(**source_tables,hwm=hwm)
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

# DBTITLE 1,delta_query
def get_delta_query(hwm):
  logger.debug("hwm: " + str(hwm))
  query ="""
        create or replace temp view delta_fetch_tv 
        as
         select ups_order_number from {fact_order_dim_inc} where dl_update_timestamp>='{hwm}'
        union 
          select case when NVL(FTTR.TRANS_ONLY_FLAG,'NULL') = 'NON_TRANS' and UPS_WMS_ORDER_NUMBER is not null 
                         then  UPS_WMS_ORDER_NUMBER
                         else UPS_ORDER_NUMBER 
                 end as ups_order_number 
          from {fact_transportation} FTTR
          where  dl_update_timestamp>='{hwm}'
       union
         select ups_order_number  from {fact_transportation_callcheck}   where dl_update_timestamp>='{hwm}'
        """.format(**source_tables,hwm=hwm)
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

# DBTITLE 1,Query q1 function
def run_q1():
  logger.debug("Running query q1 function and creating Temp table {var_fact_order_summary_tv}".format(var_fact_order_summary_tv=var_fact_order_summary_tv))
  
  q1=("""CREATE OR REPLACE TEMP VIEW {var_fact_order_summary_tv}
as SELECT 
	FTO.UPS_ORDER_NUMBER,
    FTO.GLD_ACCOUNT_MAPPED_KEY,            
    FTO.DP_SERVICELINE_KEY,
	FTO.DP_ORGENTITY_KEY,
    FTO.FacilityId,             
    FTO.SOURCE_SYSTEM_KEY,             
	FTO.SOURCE_SYSTEM_NAME,               
    FTTR.UPS_ORDER_NUMBER AS UPS_TRANSPORT_ORDER_NUMBER, 
	FTTR.SOURCE_SYSTEM_KEY AS UPS_TRANSPORT_SOURCE_SYSTEM_KEY,
    FTO.TRANSACTION_TYPE_ID AS TransactionTypeId,
    FTO.IS_MANAGED,
	FTO.IS_INBOUND,
    concat(FTO.source_system_key,'||',FTO.order_sduk) as ORDER_SDUK,
    concat(FTTR.SOURCE_SYSTEM_KEY,'||',FTTR.TRANSPORTATION_SDUK) as TRANSPORTATION_SDUK
FROM {fact_order_dim_inc} FTO
INNER JOIN delta_fetch_tv FTV on (FTV.ups_order_number= FTO.UPS_ORDER_NUMBER)
LEFT JOIN {fact_transportation} FTTR  ON       
           (CASE WHEN FTTR.TRANS_ONLY_FLAG <> 'TRANS ONLY'  THEN   NVL(FTTR.UPS_WMS_SOURCE_SYSTEM_KEY,FTO.SOURCE_SYSTEM_KEY)  ELSE FTTR.SOURCE_SYSTEM_KEY END = FTO.SOURCE_SYSTEM_KEY       
        AND CASE WHEN FTTR.TRANS_ONLY_FLAG <> 'TRANS ONLY'  THEN FTTR.UPS_WMS_ORDER_NUMBER ELSE FTTR.UPS_ORDER_NUMBER END = FTO.UPS_ORDER_NUMBER)   
""".format(var_fact_order_summary_tv=var_fact_order_summary_tv,**source_tables))
  logger.debug("query : " + q1)
  return(q1)

# COMMAND ----------

# DBTITLE 1,Query q2 function
def run_q2():
  logger.debug("Running query q2 function and creating Temp table {var_initial_temp_check}".format(var_initial_temp_check=var_initial_temp_check))
  
  q2=("""CREATE OR REPLACE TEMP VIEW {var_initial_temp_check}
as SELECT CC.UPS_ORDER_NUMBER, CC.SOURCE_SYSTEM_KEY,  MAX(CC.COMPLETE_DATE) AS COMPLETE_DATE
	
FROM {fact_transportation_callcheck} CC
INNER JOIN delta_fetch_tv FTV on (FTV.ups_order_number= CC.UPS_ORDER_NUMBER)
WHERE CC.STATUSDETAILTYPE = 'TemperatureTracking'
GROUP BY 
CC.UPS_ORDER_NUMBER,
CC.SOURCE_SYSTEM_KEY    
""".format(var_initial_temp_check=var_initial_temp_check,**source_tables))
  logger.debug("query : " + q2)
  return(q2)

# COMMAND ----------

# DBTITLE 1,Query q3 function
def run_q3():
  logger.debug("Running query q3 function and creating Temp table {var_temp_call_check}".format(var_temp_call_check=var_temp_call_check))
  
  q3=("""CREATE OR REPLACE TEMP VIEW {var_temp_call_check} as SELECT CC.UPS_ORDER_NUMBER,  CC.COMPLETE_DATE, CC.SOURCE_SYSTEM_KEY, CC.STATUSDETAIL, 'Y' AS IS_LATEST_TEMPERATURE

FROM {fact_transportation_callcheck}  CC
INNER JOIN {var_initial_temp_check} CTE 
	ON CTE.COMPLETE_DATE = CC.COMPLETE_DATE AND  CTE.UPS_ORDER_NUMBER = CC.UPS_ORDER_NUMBER AND  CTE.SOURCE_SYSTEM_KEY = CC.SOURCE_SYSTEM_KEY
WHERE CC.STATUSDETAILTYPE = 'TemperatureTracking'   
""".format(var_temp_call_check=var_temp_call_check,var_initial_temp_check=var_initial_temp_check,**source_tables))
  logger.debug("query : " + q3)
  return(q3)

# COMMAND ----------

# DBTITLE 1,Source query
def get_query():
  query=("""SELECT
  distinct
    FTC.UPS_ORDER_NUMBER as UPSORDERNUMBER,
	FTC.SOURCE_SYSTEM_KEY as SOURCESYSTEMKEY,
	FTO.GLD_ACCOUNT_MAPPED_KEY as ACCOUNTID,
	FTO.FACILITYID,
	FTO.DP_SERVICELINE_KEY as DP_SERVICELINE_KEY,
	FTO.DP_ORGENTITY_KEY as DP_ORGENTITY_KEY,
	FTC.LOAD_ID as LOADID,
	FTC.STATUSDETAIL as LATEST_TEMPERATURE,  --LATEST_TEMPERATURE
	FTC.CITY as TEMPERATURE_CITY,
	FTC.STATEPROVINCE as TEMPERATURE_STATE,
	FTC.COUNTRYCODE as TEMPERATURE_COUNTRY,
	FTC.IS_TEMPERATURE as IS_TEMPERATURE,
	FTC.COMPLETE_DATE as TEMPERATURE_DATETIME,
	FTC.ACTIVITYTYPE as ACTIVITYTYPE,
    FTC.STATUSDETAILTYPE as STATUSDETAILTYPE,
	CASE WHEN NVL(TMP.IS_LATEST_TEMPERATURE,'')='' THEN 'N' ELSE TMP.IS_LATEST_TEMPERATURE END AS IS_LATEST_TEMPERATURE
	,FTC.Latitude
	,FTC.Longitude
	,FTC.DeviceTagId
	,FTC.LocationMethod
	,FTC.IsMotionDetected
	,FTC.Pressure
	,FTC.IsButtonPushed
	,FTC.TemperatureC
	,FTC.TemperatureF
	,FTC.BatteryPercent
	,FTC.Humidity
	,FTC.Light
	,FTC.IsShockExceeded
	,FTO.ORDER_SDUK
    ,FTO.TRANSPORTATION_SDUK
    ,FTC.CALLCHECK_SDUK as CALLCHECK_SDUK
    ,0 is_deleted
    ,row_number() over (PARTITION BY FTC.SOURCE_SYSTEM_KEY,FTC.UPS_ORDER_NUMBER,FTO.order_sduk,FTC.CALLCHECK_SDUK 
                             ORDER BY FTO.TRANSPORTATION_SDUK NULLS FIRST
                             ) as transport_rn
	FROM {var_fact_order_summary_tv} FTO
		INNER JOIN {fact_transportation_callcheck}  FTC
			ON CASE WHEN IS_INBOUND IN (1,2) THEN FTO.UPS_ORDER_NUMBER ELSE FTO.UPS_TRANSPORT_ORDER_NUMBER END = FTC.UPS_ORDER_NUMBER 
		     AND CASE WHEN IS_INBOUND IN (1,2) THEN FTO.SOURCE_SYSTEM_KEY ELSE  FTO.UPS_TRANSPORT_SOURCE_SYSTEM_KEY END = FTC.SOURCE_SYSTEM_KEY
	LEFT JOIN {var_temp_call_check} TMP
		ON CASE WHEN IS_INBOUND IN (1,2) THEN FTO.UPS_ORDER_NUMBER ELSE FTO.UPS_TRANSPORT_ORDER_NUMBER END = TMP.UPS_ORDER_NUMBER				--UPSGLD-11400 
		     AND CASE WHEN IS_INBOUND IN (1,2) THEN FTO.SOURCE_SYSTEM_KEY ELSE  FTO.UPS_TRANSPORT_SOURCE_SYSTEM_KEY END = TMP.SOURCE_SYSTEM_KEY	--UPSGLD-11400 
			 AND FTC.COMPLETE_DATE = TMP.COMPLETE_DATE
	--WHERE FTC.STATUSDETAILTYPE = 'TemperatureTracking'   
""".format(var_fact_order_summary_tv=var_fact_order_summary_tv,var_temp_call_check=var_temp_call_check,**source_tables))
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

# DBTITLE 1,Main Function
def main():
    logger.info('Main function is running')
  
    try:
        pid_get = get_pid()
        logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
        pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
        logger.info("pid: {pid}".format(pid=pid))
        ############################ ETL AUDIT #########################################################
        audit_result['process_id'] = pid
        audit_result['process_name'] = 'pr_load_summary_transportation_callcheck'
        audit_result['process_type'] = 'DataBricks'
        audit_result['layer'] = 'gold'
        audit_result['table_name'] = digital_summary_transportation_callcheck_et
        audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
        audit_result['start_time'] = start_time
        ################################################################################################
        hwm=get_hwm('gold',digital_summary_transportation_callcheck_et)
        logger.info(f'hwm {digital_summary_transportation_callcheck_et}: {hwm}')
        
        check_load = spark.sql(get_check_load_query(hwm)).count()
        logger.info("check_load: " + str(check_load) )
        
        if check_load>0:
            
            spark.sql(get_delta_query(hwm))
            logger.info("get_delta_query finished") 
        
            logger.info('Reading q1 query...')
            spark.sql(run_q1())
            logger.info('Reading q2 query...')
            spark.sql(run_q2())
            logger.info('Reading q3 query...')
            spark.sql(run_q3())
            logger.info('Getting source query')
            src_df = spark.sql(get_query())
            
            ##################### generating hash key  #############################
            hash_key_columns = ['SOURCESYSTEMKEY','UPSORDERNUMBER','ORDER_SDUK','transport_rn','CALLCHECK_SDUK']
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
        
            primary_keys = ['SOURCESYSTEMKEY','UPSORDERNUMBER','ORDER_SDUK','transport_rn','CALLCHECK_SDUK']
            logger.debug('primary_keys: {primary_keys}'.format(primary_keys=primary_keys))
            
            logger.info(f'Merging to delta path: {digital_summary_transportation_callcheck_path}')
        
            mergeToDelta(src_df,digital_summary_transportation_callcheck_path,primary_keys)
            logger.info(f'Merging to delta path finished: {digital_summary_transportation_callcheck_path}')
            
        else:
            logger.info('Nothing to process')
        
        logger.info('setting hwm')
        res=set_hwm('gold',digital_summary_transportation_callcheck_et,start_time,pid)
        logger.info(res)
        ############################ ETL AUDIT #########################################################
        audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
        audit_result['status'] = 'success'
                
        logger.info('Process finished successfully.')
        
    except Exception as e:
        audit_result['status'] = 'failed'
        audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
        audit_result['ERROR_MESSAGE'] = str(e)
        raise
    finally:
        logger.info('Cleaning done')
        logger.info("audit_result: {audit_result}".format(audit_result=audit_result))
        audit(audit_result)


# COMMAND ----------

main()

# COMMAND ----------


