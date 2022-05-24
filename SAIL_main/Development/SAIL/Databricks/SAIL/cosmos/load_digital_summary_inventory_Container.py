# Databricks notebook source
"""
Author           : Prashant Gupta
Description      : this notebook is to load digital_summary_inventory cosmos container.

Vinoth : version 1.1 -- Cosmos RU restriction logic
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
cosmosContainerName = "digital_summary_inventory"

controlCosmosDatabaseName = "SAIL"
controlCosmosContainerName = "throughput_control_container" 

cfg = {
  "spark.cosmos.accountEndpoint" : cosmosEndpoint,
  "spark.cosmos.accountKey" : cosmosMasterKey,
  "spark.cosmos.database" : cosmosDatabaseName,
  "spark.cosmos.container" : cosmosContainerName,
  "spark.cosmos.write.strategy": "ItemOverwrite",
  "spark.cosmos.write.bulk.enabled": "true",
  "spark.cosmos.throughputControl.enabled": "true",
  "spark.cosmos.throughputControl.name": "inventory_throughput_control",
  "spark.cosmos.throughputControl.targetThroughputThreshold": "0.9",
  "spark.cosmos.throughputControl.globalControl.database": controlCosmosDatabaseName,
  "spark.cosmos.throughputControl.globalControl.container": controlCosmosContainerName
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

def get_pre_cosmos_query(hwm):
  logger.debug("hwm: " + str(hwm))
  query ="""
select i.hash_key id
,i.SourceSystemKey                                
,i.SourceSystemName                               
,i.AccountId                                      
,i.FacilityId                                     
,i.itemNumber                                     
,i.itemDescription                                
,i.hazardClass                                    
,i.itemDimensions_length                          
,i.itemDimensions_width                           
,i.itemDimensions_height                          
,i.itemDimensions_unitOfMeasurement_code          
,i.itemWeight_weight                              
,i.itemWeight_unitOfMeasurement_Code              
,i.warehouseCode                                  
,i.availableQuantity                              
,i.nonAvailableQuantity                           
,i.DP_SERVICELINE_KEY                             
,i.DP_ORGENTITY_KEY                               
,i.InvRef1                                        
,i.InvRef2                                        
,i.InvRef3                                        
,i.InvRef4                                        
,i.InvRef5                                        
,i.LPNNumber                                      
,i.HazmatClass                                    
,i.StrategicGoodsFlag                             
,i.UNNumber                                       
,i.Designator                                     
,i.VendorSerialNumber                             
,i.VendorLotNumber                                
,i.BatchStatus                                    
,date_format(i.ExpirationDate, 'yyyy-MM-dd HH:mm:ss.SSS') as ExpirationDate                                 
,i.Account_number                                 
,i.BatchHoldReason                                
,i.HoldDescription                                
,i.is_deleted   
,CASE
      WHEN i.SourceSystemName LIKE '%SOFTEON%' THEN wse.BUILDING_CODE
      ELSE wse.WAREHOUSE_CODE
    END wse_warehouseCode
,wse.WAREHOUSE_TIME_ZONE wse_warehouseTimeZone
,wse.ADDRESS_LINE_1 wse_addressLine1
,wse.ADDRESS_LINE_2 wse_addressLine2
,wse.CITY wse_city
,wse.PROVINCE wse_stateProvince
,wse.POSTAL_CODE wse_postalCode
,wse.COUNTRY wse_country 
from {digital_summary_inventory} i
 left join {dim_warehouse} wse
    on i.FacilityId =wse.GLD_WAREHOUSE_MAPPED_KEY and i.SourceSystemKey = wse.SOURCE_SYSTEM_KEY
WHERE i.dl_update_timestamp >= '{hwm}' 
        """.format(**source_tables,hwm=hwm,days_back=days_back)
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

# DBTITLE 1,Main function
def main():
  logger.info('Main function is running')
  ############################ ETL AUDIT #########################################################
  
  audit_result['process_name'] = 'load_digital_summary_inventory_Container'
  audit_result['process_type'] = 'DataBricks'
  audit_result['layer'] = 'cosmos'
  audit_result['table_name'] = 'cosmos_digital_summary_inventory'
  audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
  audit_result['start_time'] = start_time
  
  
  try:
   
      
    pid_get = get_pid()
    logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
    pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
    logger.info("pid: {pid}".format(pid=pid))
    
    audit_result['process_id'] = pid
    
    hwm=get_hwm('cosmos','cosmos_digital_summary_inventory')
#     hwm = '2022-04-04 14:03:46'
    logger.info(f'hwm cosmos_digital_summary_inventory: {hwm}'.format(hwm=hwm))
    
      
    logger.info('Reading source data...')
    
    src_query =get_pre_cosmos_query(hwm)
    logger.debug('cosmos_query : ' + src_query)
    
    cosmos_df = spark.sql(src_query)
    
    logger.debug("Adding audit columns")
    cosmos_df = add_audit_columns(cosmos_df, pid,datetime.now(),datetime.now())
    
    logger.info('Writing to Cosmos: {container_name}'.format(container_name=cosmosContainerName))
    cosmos_df.write.format("cosmos.oltp").options(**cfg).mode("APPEND").save()
        
    logger.info('setting hwm')
    r=set_hwm('cosmos','cosmos_digital_summary_inventory',start_time,pid)
    logger.info(r)
    
    cnt=cosmos_df.count()
    logger.info('Insert count is {cnt}'.format(cnt=cnt))
    
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