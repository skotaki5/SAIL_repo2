# Databricks notebook source
"""
Author           : Prashant Gupta
Description      : this notebook is to load digital_summary_inventory_aggregated cosmos container.
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

# DBTITLE 1,Cosmos connection
scope = 'key-vault-secrets'
cosmosEndpoint = dbutils.secrets.get(scope,"cosmosEndpoint")
cosmosMasterKey = dbutils.secrets.get(scope,"cosmosMasterKey")
cosmosDatabaseName = "SAIL"
cosmosContainerName = "digital_summary_inventory_aggregated"

cfg = {
  "spark.cosmos.accountEndpoint" : cosmosEndpoint,
  "spark.cosmos.accountKey" : cosmosMasterKey,
  "spark.cosmos.database" : cosmosDatabaseName,
  "spark.cosmos.container" : cosmosContainerName,
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
  logger = _get_logger(time_zone,logging.DEBUG)  
else:
  logger = _get_logger(time_zone,logging.INFO)
  

# COMMAND ----------

def get_delta_query(hwm):
  logger.debug("hwm: " + str(hwm))
  query ="""
        create or replace temp view delta_fetch_tv 
        as
          select distinct AccountId from {digital_summary_inventory} where dl_update_timestamp>='{hwm}'
        """.format(**source_tables,hwm=hwm)
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

def get_pre_cosmos_query(hwm):
  logger.debug("hwm: " + str(hwm))
  query ="""
SELECT 
md5(concat(nvl(i.AccountId,''),nvl(i.DP_SERVICELINE_KEY,''),nvl(i.FacilityId,''),'inventory')) as id
,'inventory' type
,i.AccountId
,i.DP_SERVICELINE_KEY
,i.FacilityId warehouseId
,CASE
      WHEN i.SourceSystemName LIKE '%SOFTEON%' THEN wse.BUILDING_CODE
      ELSE wse.WAREHOUSE_CODE
    END wse_warehouseCode
,wse.WAREHOUSE_TIME_ZONE warehouseTimeZone
,wse.ADDRESS_LINE_1 addressLine1
,wse.ADDRESS_LINE_2 addressLine2
,wse.city city
,wse.PROVINCE stateProvince
,wse.POSTAL_CODE postalCode
,wse.country country
,count(i.LPNNumber) lpnOnHandCount
from {digital_summary_inventory} i
join delta_fetch_tv d
on i.AccountId = d.AccountId
 left join {dim_warehouse} wse
    on i.FacilityId =wse.GLD_WAREHOUSE_MAPPED_KEY and i.SourceSystemKey = wse.SOURCE_SYSTEM_KEY
where i.is_deleted = 0
and i.FacilityId is not null
group by
'inventory'
,i.AccountId
,i.DP_SERVICELINE_KEY
,i.FacilityId
,CASE
      WHEN i.SourceSystemName LIKE '%SOFTEON%' THEN wse.BUILDING_CODE
      ELSE wse.WAREHOUSE_CODE
    END 
,wse.WAREHOUSE_TIME_ZONE
,wse.ADDRESS_LINE_1
,wse.ADDRESS_LINE_2
,wse.city
,wse.PROVINCE
,wse.POSTAL_CODE
,wse.country
        """.format(**source_tables,hwm=hwm,days_back=days_back)
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

# DBTITLE 1,Main function
def main():
  logger.info('Main function is running')
  ############################ ETL AUDIT #########################################################
  
  audit_result['process_name'] = 'load_digital_summary_inventory_agg_Container'
  audit_result['process_type'] = 'DataBricks'
  audit_result['layer'] = 'cosmos'
  audit_result['table_name'] = 'digital_summary_inventory_aggregated'
  audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
  audit_result['start_time'] = start_time
  
  
  try:
   
      
    pid_get = get_pid()
    logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
    pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
    logger.info("pid: {pid}".format(pid=pid))
    
    audit_result['process_id'] = pid
    
    hwm=get_hwm('cosmos','digital_summary_inventory_aggregated')
#     hwm = '1900-01-01'
    logger.info(f'hwm digital_summary_inventory_aggregated: {hwm}'.format(hwm=hwm))
    
    spark.sql(get_delta_query(hwm))
    logger.info("get_delta_query finished")
    
    logger.info('Reading source data...')
    
    src_query =get_pre_cosmos_query(hwm)
    logger.debug('cosmos_query : ' + src_query)
    
    cosmos_df = spark.sql(src_query)
    
    logger.debug("Adding audit columns")
    cosmos_df = add_audit_columns(cosmos_df, pid,datetime.now(),datetime.now())
    
    cnt=cosmos_df.count()
    logger.info('Insert count is {cnt}'.format(cnt=cnt))
    
    logger.info('Writing to Cosmos: {container_name}'.format(container_name=cosmosContainerName))
    cosmos_df.write.format("cosmos.oltp").options(**cfg).mode("APPEND").save()
        
    logger.info('setting hwm')
    r=set_hwm('cosmos','digital_summary_inventory_aggregated',start_time,pid)
    logger.info(r)
    
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