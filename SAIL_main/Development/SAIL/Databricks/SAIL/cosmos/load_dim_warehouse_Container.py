# Databricks notebook source
"""
Author           : Vinoth Kumar Gopal
Description      : this notebook is to load dim_warehouse cosmos container.
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
cosmosContainerName = "dim_warehouse"

cfg = {
  "spark.cosmos.accountEndpoint" : cosmosEndpoint,
  "spark.cosmos.accountKey" : cosmosMasterKey,
  "spark.cosmos.database" : cosmosDatabaseName,
  "spark.cosmos.container" : cosmosContainerName,
}

# COMMAND ----------

time_zone = 'UTC' # Check for which timezone to be used
st_dt =datetime.now(tz=timezone(time_zone))
start_time = st_dt.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# DBTITLE 1,Setting debug mode
#dbutils.widgets.text("log_debug_mode", "","")
dbutils.widgets.get("log_debug_mode")
log_debug_mode = getArgument("log_debug_mode").strip()

if log_debug_mode == "Y":
  logger = _get_logger('US/Eastern',logging.DEBUG)  #UTC timezone
else:
  logger = _get_logger('US/Eastern',logging.INFO)

# COMMAND ----------

def get_pre_cosmos_query(hwm):
  query = """
SELECT
md5(concat(nvl(C.GLD_ACCOUNT_MAPPED_KEY,''),nvl(c.DP_SERVICELINE_KEY,''),nvl(W.GLD_WAREHOUSE_MAPPED_KEY,''))) as id,
C.GLD_ACCOUNT_MAPPED_KEY AccountId,
c.DP_SERVICELINE_KEY,
W.GLD_WAREHOUSE_MAPPED_KEY warehouseId,
CASE WHEN SS.SOURCE_SYSTEM_NAME LIKE '%SOFTEON%' THEN BUILDING_CODE ELSE WAREHOUSE_CODE END warehouseCode,
W.WAREHOUSE_TIME_ZONE warehouseTimeZone,
W.ADDRESS_LINE_1 addressLine1,
W.ADDRESS_LINE_2 addressLine2,
W.CITY city,
W.PROVINCE stateProvince,
W.POSTAL_CODE postalCode,
W.COUNTRY country,
W.SOURCE_SYSTEM_KEY
FROM {dim_customer} C
INNER JOIN {account_type_digital} GLAT ON C.GLD_ACCOUNT_MAPPED_KEY = GLAT.ACCOUNT_ID
INNER JOIN {dim_warehouse} W on C.SOURCE_SYSTEM_KEY = W.SOURCE_SYSTEM_KEY
INNER JOIN {dim_source_system} SS ON c.SOURCE_SYSTEM_KEY = SS.SOURCE_SYSTEM_KEY
INNER JOIN {digital_summary_onboarded_systems} OS on OS.sourcesystemkey=C.SOURCE_SYSTEM_KEY

WHERE
array_contains(transform(split(C.Mapped_Warehouse_code,','),x -> trim(x)), TRIM(CASE WHEN SS.SOURCE_SYSTEM_NAME LIKE '%SOFTEON%' THEN W.BUILDING_CODE ELSE W.WAREHOUSE_CODE END))
and W.GLD_WAREHOUSE_MAPPED_KEY IS NOT NULL
and (W.dl_update_timestamp >= '{hwm}' or C.dl_update_timestamp >= '{hwm}' or GLAT.dl_update_timestamp >= '{hwm}')

GROUP BY
C.GLD_ACCOUNT_MAPPED_KEY,
c.DP_SERVICELINE_KEY,
W.GLD_WAREHOUSE_MAPPED_KEY,
CASE WHEN SS.SOURCE_SYSTEM_NAME LIKE '%SOFTEON%' THEN BUILDING_CODE ELSE WAREHOUSE_CODE END,
W.WAREHOUSE_TIME_ZONE,
W.ADDRESS_LINE_1,
W.ADDRESS_LINE_2,
W.CITY,
W.PROVINCE,
W.POSTAL_CODE,
W.COUNTRY,
W.SOURCE_SYSTEM_KEY
  """.format(**source_tables, hwm=hwm)
  return (query)

# COMMAND ----------

# DBTITLE 1,Main function
def main():
  logger.info('Main function is running')
  
  audit_result['process_name'] = 'load_dim_warehouse_Container'
  audit_result['process_type'] = 'DataBricks'
  audit_result['layer'] = 'cosmos'
  audit_result['table_name'] = 'cosmos_dim_warehouse'
  audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
  audit_result['start_time'] = start_time
  
  
  try:
      
    pid_get = get_pid()
    logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
    pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
    logger.info("pid: {pid}".format(pid=pid))
    
    audit_result['process_id'] = pid
    
    hwm=get_hwm('cosmos','cosmos_dim_warehouse')
    #hwm='1900-01-01 00:00:00'
    logger.info(f'hwm cosmos_dim_warehouse: {hwm}'.format(hwm=hwm))

    logger.info('Reading source data...')
    
    src_query =get_pre_cosmos_query(hwm)
    logger.debug('cosmos_query : ' + src_query)
    
    cosmos_df = spark.sql(src_query)
    
    logger.debug("Adding audit columns")
    cosmos_df = add_audit_columns(cosmos_df, pid,datetime.now(),datetime.now())
    
    logger.info('Writing to Cosmos: {container_name}'.format(container_name=cosmosContainerName))
    cosmos_df.write.format("cosmos.oltp").options(**cfg).mode("APPEND").save()
    
    cnt=cosmos_df.count()    
    logger.info('count is {cnt}'.format(cnt=cnt))
    
    logger.info('setting hwm')
    res=set_hwm('cosmos','cosmos_dim_warehouse',start_time,pid)
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