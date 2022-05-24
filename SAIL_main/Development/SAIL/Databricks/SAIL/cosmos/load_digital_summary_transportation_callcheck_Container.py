# Databricks notebook source
"""
Author           : Vinoth Kumar Gopal
Description      : this notebook is to load inbound digital_summary_transportation_callcheck cosmos container.
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
cosmosContainerName = "digital_summary_transportation_callcheck"

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
dbutils.widgets.text("log_debug_mode", "","")
dbutils.widgets.get("log_debug_mode")
log_debug_mode = getArgument("log_debug_mode").strip()

if log_debug_mode == "Y":
  logger = _get_logger('US/Eastern',logging.DEBUG)  #UTC timezone
else:
  logger = _get_logger('US/Eastern',logging.INFO)

# COMMAND ----------

def get_delta_query(hwm):
  logger.debug("hwm: " + str(hwm))
  query ="""
        create or replace temp view digital_summary_transportation_callcheck_vw 
        as
         select TC.* from {digital_summary_transportation_callcheck} TC 
         inner join {digital_summary_onboarded_systems} OS on  OS.sourcesystemkey=TC.SOURCESYSTEMKEY 
         where TC.dl_update_timestamp>='{hwm}' 
         and TC.TEMPERATURE_DATETIME >= current_date-{days_back}
        """.format(**source_tables,hwm=hwm,days_back=days_back)
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

def get_pre_cosmos_query():
  query = """
select distinct 
hash_key as id,
O.UPSOrderNumber AS UPSOrderNumber,
O.UPSTransportShipmentNumber,
O.is_inbound,
O.AccountId,
O.DP_SERVICELINE_KEY,
TC.SOURCESYSTEMKEY AS SourceSystemKey,  
TC.LATEST_TEMPERATURE AS TemperatureValue,      
date_format(TC.TEMPERATURE_DATETIME,'yyyy-MM-dd HH:mm:ss.SSS') TemperatureDateTime,    
TC.TEMPERATURE_CITY AS TemperatureCity,      
TC.TEMPERATURE_STATE AS TemperatureState,      
TC.TEMPERATURE_COUNTRY AS TemperatureCountry,
TC.TemperatureC  AS TemperatureInCelsius,
TC.TemperatureF AS TemperatureInFahrenheit,
TC.BatteryPercent AS battery,
TC.Humidity AS humidity,
TC.Light  AS light,
TC.IsShockExceeded As shock,
TC.IS_LATEST_TEMPERATURE AS Is_Latest_Temperature,
TC.TemperatureC,
TC.TemperatureF,
TC.BatteryPercent,
TC.Humidity,
TC.Light,
TC.IsShockExceeded,
TC.Latitude,
TC.Longitude,  
TC.DeviceTagId,
TC.LocationMethod,
TC.IsMotionDetected,
TC.Pressure,
TC.IsButtonPushed,
TC.is_deleted
from
  {digital_summary_transportation_callcheck_vw} TC
  inner join (select distinct DSO.UPSOrderNumber,DSO.UPSTransportShipmentNumber,DSO.SourceSystemKey,DSO.is_inbound,DSO.AccountId,DSO.DP_SERVICELINE_KEY 
  from {digital_summary_orders} DSO inner join {digital_summary_onboarded_systems} OS on  DSO.sourcesystemkey=OS.SourceSystemKey
  ) O 
  ON TC.UPSORDERNUMBER = case when O.is_inbound = 0 then O.UPSTransportShipmentNumber else O.UpsOrderNumber end
  WHERE TC.IS_TEMPERATURE='Y'      
  AND TC.STATUSDETAILTYPE='TemperatureTracking'
  """.format(**source_tables, digital_summary_transportation_callcheck_vw='digital_summary_transportation_callcheck_vw')
  return (query)

# COMMAND ----------

# DBTITLE 1,Main function
def main():
  logger.info('Main function is running')
  
  audit_result['process_name'] = 'load_digital_summary_transportation_callcheck_Container'
  audit_result['process_type'] = 'DataBricks'
  audit_result['layer'] = 'cosmos'
  audit_result['table_name'] = 'cosmos_digital_summary_transportation_callcheck'
  audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
  audit_result['start_time'] = start_time
  
  try:
      
    pid_get = get_pid()
    logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
    pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
    logger.info("pid: {pid}".format(pid=pid))
    
    audit_result['process_id'] = pid
    
    hwm=get_hwm('cosmos','cosmos_digital_summary_transportation_callcheck')
#     if hwm=='1900-01-01 00:00:00':
#       d = timedelta(days = 90)
#       back_date=st_dt - d
#       hwm=back_date.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f'hwm cosmos_digital_summary_transportation_callcheck: {hwm}'.format(hwm=hwm))
    
    logger.info("Creating digital summary orders transportation callcheck views for incremental data")
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
    res=set_hwm('cosmos','cosmos_digital_summary_transportation_callcheck',start_time,pid)
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