# Databricks notebook source
"""
Author           : Vinoth Kumar Gopal
Description      : this notebook is to load digital_summary_milestone_activity cosmos container.
"""

# COMMAND ----------

# DBTITLE 1,Importing python libraries
import datetime
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, DecimalType
from pyspark.sql.functions import col, from_json

# COMMAND ----------

# DBTITLE 1,Importing common variables
# MAGIC %run "/SAIL/includes/common_variables"

# COMMAND ----------

# DBTITLE 1,Importing common udfs
# MAGIC %run "/SAIL/includes/common_udfs"

# COMMAND ----------

# DBTITLE 1,Spark Configs
spark.conf.set("spark.databricks.io.cache.enabled","true")

# COMMAND ----------

# DBTITLE 1,Cosmos connection
#Cosmos connection
scope = 'key-vault-secrets'
cosmosEndpoint = dbutils.secrets.get(scope,"cosmosEndpoint")
cosmosMasterKey = dbutils.secrets.get(scope,"cosmosMasterKey")
cosmosDatabaseName = "SAIL"
cosmosContainerName = "digital_summary_milestone_activity"

cfg = {
  "spark.cosmos.accountEndpoint" : cosmosEndpoint,
  "spark.cosmos.accountKey" : cosmosMasterKey,
  "spark.cosmos.database" : cosmosDatabaseName,
  "spark.cosmos.container" : cosmosContainerName
}


# COMMAND ----------

st_dt =datetime.now(tz=timezone(time_zone))
start_time = st_dt.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# DBTITLE 1,Setting debug mode
#dbutils.widgets.text("log_debug_mode", "","")
dbutils.widgets.get("log_debug_mode")
log_debug_mode = getArgument("log_debug_mode").strip()

if log_debug_mode == "Y":
  logger = _get_logger(time_zone,logging.DEBUG)  
else:
  logger = _get_logger(time_zone,logging.INFO)

# COMMAND ----------

def get_delta_query(hwm):
  logger.debug("hwm: " + str(hwm))
  query ="""
        create or replace temp view digital_summary_milestone_activity_vw 
        as
         select o.* from {digital_summary_milestone_activity} o 
         inner join {digital_summary_onboarded_systems} OS on  OS.sourcesystemkey=o.SourceSystemKey 
         where o.dl_update_timestamp>='{hwm}' 
         and (o.ActivityDate between case when date('{hwm}') = '1900-01-01' then current_date else  date('{hwm}') end - {days_back} and case when date('{hwm}') = '1900-01-01' then current_date else  date('{hwm}') end + {days_back}
         or
         is_deleted =  case when date('{hwm}') = '1900-01-01' then -1 else  1 end 
         )
         --and AccountId in  {account_id}
        """.format(**source_tables,hwm=hwm,days_back=days_back,account_id=account_id)
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

def get_pre_cosmos_query():
  query = """
    SELECT     
	hash_key as id
	,MA.UPSOrderNumber as UpsOrderNumber
	,MA.SourceSystemKey
    ,O.is_inbound
	,O.DP_SERVICELINE_KEY
    ,MA.AccountId
    ,MA.FacilityId
    ,MA.MilestoneOrder
    ,MA.MilestoneName
    ,MA.MilestoneCompletionFlag
	,date_format(MA.MilestoneDate, 'yyyy-MM-dd HH:mm:ss.SSS') MilestoneDate
	,MA.ActivityCode
	,MA.ActivityName
	,date_format(MA.ActivityDate, 'yyyy-MM-dd HH:mm:ss.SSS') ActivityDate
	,date_format(MA.ActivityDate, 'yyyy-MM-dd') ActivityDateShort
	,MA.ACTIVITY_NOTES
	,MA.ActivityCompletionFlag
	,MA.CARRIER_TYPE
	,CASE WHEN MA.MilestoneName='FTZ' THEN MA.FTZ_Status ELSE NULL END AS FTZStatus
	,MA.LOGI_NEXT_FLAG
	,MA.PROOF_OF_DELIVERY_NAME
	,date_format(MA.PlannedMilestoneDate, 'yyyy-MM-dd HH:mm:ss.SSS') PlannedMilestoneDate
	,MA.SEGMENT_ID
	,MA.TimeZone
	,MA.TrackingNumber
	,MA.UPSASNNumber
	,MA.VENDOR_NAME
	,MA.TransactionTypeId
    ,MTM.TransactionTypeName
	,MA.LOAD_TRACK_SDUK
    ,O.CarrierServiceDetails
	,MA.is_deleted
    ,MA.PROOF_OF_DELIVERY_LOCATION
    ,MA.PROOF_OF_DELIVERY_DATE_TIME
    ,MA.LATITUDE
    ,MA.LONGITUDE
FROM {digital_summary_milestone_activity_vw} MA 
LEFT JOIN {map_transactiontype_milestone} MTM 
        ON MA.MilestoneOrder=MTM.MilestoneOrder
INNER JOIN (select DSO.UpsOrderNumber,DSO.SourceSystemKey,is_inbound,
        max(DP_SERVICELINE_KEY) as DP_SERVICELINE_KEY,
           collect_set(named_struct(
           'carrierCode', DSO.CarrierCode, 
           'CarrierName', DSO.Carrier,
           'shipmentServiceLevel', DSO.ServiceLevel,
           'shipmentServiceLevelCode', DSO.ServiceLevelCode,
            'Account_number',DSO.Account_Number)
           ) AS CarrierServiceDetails
           
        from {digital_summary_orders} DSO
        inner join {digital_summary_onboarded_systems} OS on DSO.SourceSystemKey = OS.SourceSystemKey
        group by DSO.UPSOrderNumber,DSO.SourceSystemKey,is_inbound
  ) O 
  ON MA.UPSOrderNumber = O.UPSOrderNumber 
  AND MA.SourceSystemKey = CASE WHEN O.SourceSystemKey = '1011' THEN MA.SourceSystemKey ELSE O.SourceSystemKey END
  """.format(**source_tables,digital_summary_milestone_activity_vw='digital_summary_milestone_activity_vw')
  return (query)

# COMMAND ----------

# DBTITLE 1,Main function
def main():
  logger.info('Main function is running')
  audit_result['process_name'] = 'load_digital_summary_milestone_activity_Container'
  audit_result['process_type'] = 'DataBricks'
  audit_result['layer'] = 'cosmos'
  audit_result['table_name'] = 'cosmos_digital_summary_milestone_activity'
  audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
  audit_result['start_time'] = start_time
  
  try:
      
    pid_get = get_pid()
    logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
    pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
    logger.info("pid: {pid}".format(pid=pid))
    
    audit_result['process_id'] = pid
    
    hwm=get_hwm('cosmos','cosmos_digital_summary_milestone_activity')
    #hwm='2022-03-27 09:55:43'
    #if hwm=='1900-01-01 00:00:00':
    #    d = timedelta(days = 90)
    #    back_date=st_dt - d
    #    hwm=back_date.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f'hwm cosmos_digital_summary_milestone_activity: {hwm}'.format(hwm=hwm))
    
    logger.info("Creating digital summar milestone activity view for incremental data")
    spark.sql(get_delta_query(hwm))
    logger.info("get_delta_query finished")  
    
    logger.info('Reading source data...')
    
    src_query =get_pre_cosmos_query()
    logger.debug('cosmos_query : ' + src_query)
    
    cosmos_df = spark.sql(src_query)
    
    logger.debug("Adding audit columns")
    cosmos_df = add_audit_columns(cosmos_df, pid,datetime.now(),datetime.now())
    cnt=cosmos_df.count()
    logger.info('count is {cnt}'.format(cnt=cnt))
    
    logger.info('Writing to Cosmos: {container_name}'.format(container_name=cosmosContainerName))
    cosmos_df.write.format("cosmos.oltp").options(**cfg).mode("APPEND").save()
    
    logger.info('setting hwm')
    res=set_hwm('cosmos','cosmos_digital_summary_milestone_activity',start_time,pid)
    logger.info(res)
      
    audit_result['numTargetRowsInserted'] = cnt
    audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
    audit_result['status'] = 'success'   
    logger.info('Process finished successfully.')
    
    
  except Exception as e:
    audit_result['status'] = 'failed'
    audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
    audit_result['ERROR_MESSAGE'] = str(e)
    
    raise
  finally:
    logger.info("audit_result: {audit_result}".format(audit_result=audit_result))
    audit(audit_result)

# COMMAND ----------

main()