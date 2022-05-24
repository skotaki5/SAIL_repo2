# Databricks notebook source
"""
Author           : Vinoth Kumar Gopal
Description      : this notebook is to load inbound digital_summary_transport_details cosmos container.
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
cosmosContainerName = "digital_summary_transport_details"

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
CREATE OR REPLACE TEMP VIEW digital_summary_transport_details_vw AS
	WITH change_stg AS (
			SELECT UPSOrderNumber
			FROM {digital_summary_transport_details}
			WHERE dl_update_timestamp >= '{hwm}'
            
			UNION
			
			SELECT UPSOrderNumber
			FROM {digital_summary_transportation_references}
			WHERE dl_update_timestamp >= '{hwm}' and ReferenceLevel = 'shipitem_reference'
			)
SELECT TD.*
FROM {digital_summary_transport_details} TD
INNER JOIN {digital_summary_onboarded_systems} OS ON TD.SOURCE_SYSTEM_KEY = OS.sourcesystemkey
INNER JOIN (SELECT DISTINCT UPSOrderNumber FROM change_stg) c ON TD.UPSOrderNumber = c.UPSOrderNumber
        """.format(**source_tables,hwm=hwm)
    logger.debug("query : " + query)
    return(query)

# COMMAND ----------

def get_pre_cosmos_query():
  query = """
  WITH shipitem_references AS (
			SELECT UPSOrderNumber
					,SourceSystemKey
					,collect_set(named_struct('referenceType', nvl(TR.ReferenceType, ''), 'referenceValue', nvl(TR.ReferenceValue, ''))) AS shipitem_reference
			FROM {digital_summary_transportation_references} TR
			WHERE ReferenceLevel = 'shipitem_reference' and TR.is_deleted = 0
			GROUP BY UPSOrderNumber
					,SourceSystemKey
			)  
  select 
hash_key AS id,
td.UPSORDERNUMBER AS UpsOrderNumber,
td.SOURCE_SYSTEM_KEY AS SourceSystemKey,
Account_ID,
DP_SERVICELINE_KEY,
DP_ORGENTITY_KEY,
ITEM_DESCRIPTION,
ACTUAL_QTY,
ACTUAL_UOM,
ACTUAL_WGT,
ITEM_DIMENSION,
TempRangeMin,
TempRangeMax,
TempRangeUOM,
TempRangeCode,
PlannedWeightUOM,
ActualWeightUOM,
DimensionUOM,
sr.shipitem_reference as transportation_referenceType,
is_deleted
from {digital_summary_transport_details_vw} td
left outer join shipitem_references sr
on td.UPSORDERNUMBER = sr.UPSOrderNumber and td.SOURCE_SYSTEM_KEY = sr.SourceSystemKey 
  """.format(**source_tables, digital_summary_transport_details_vw='digital_summary_transport_details_vw')
  return (query)

# COMMAND ----------

# DBTITLE 1,Main function
def main():
  logger.info('Main function is running')
  
  audit_result['process_name'] = 'load_digital_summary_transport_details_Container'
  audit_result['process_type'] = 'DataBricks'
  audit_result['layer'] = 'cosmos'
  audit_result['table_name'] = 'cosmos_digital_summary_transport_details'
  audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
  audit_result['start_time'] = start_time
  
  
  try:      
    pid_get = get_pid()
    logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
    pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
    logger.info("pid: {pid}".format(pid=pid))
    
    audit_result['process_id'] = pid
    
    hwm=get_hwm('cosmos','cosmos_digital_summary_transport_details')
    #hwm='1900-01-01 00:00:00'
#     if hwm=='1900-01-01 00:00:00':
#       d = timedelta(days = 90)
#       back_date=st_dt - d
#       hwm=back_date.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f'hwm cosmos_digital_summary_transport_details: {hwm}'.format(hwm=hwm))
    
    logger.info("Creating digital summary orders transport details views for incremental data")
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
    res=set_hwm('cosmos','cosmos_digital_summary_transport_details',start_time,pid)
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