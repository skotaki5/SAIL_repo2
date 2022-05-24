# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <b>Author--GD000012733@ups.com</b>
# MAGIC <b>Name:Vishal</b>

# COMMAND ----------

import datetime,time,json
from pytz import timezone
from  pyspark.sql.functions import input_file_name ,split , size , from_utc_timestamp, lit , concat, when, col, sha1, row_number

# COMMAND ----------

# DBTITLE 1,Importing common variables
# MAGIC %run "/SAIL/includes/common_variables"

# COMMAND ----------

# DBTITLE 1,Importing common udfs
# MAGIC %run "/SAIL/includes/common_udfs"

# COMMAND ----------

start_time = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")

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

# DBTITLE 1,Source query
def get_query(hwm):
  query=("""SELECT   
 FTO.UPS_ORDER_NUMBER as UPSOrderNumber,  
 FTO.SOURCE_SYSTEM_KEY as SourceSystemKey,  
 FTO.SOURCE_SYSTEM_NAME as SourceSystemName,  
 FTO.GLD_ACCOUNT_MAPPED_KEY as AccountId,  
 FTO.DP_SERVICELINE_KEY,  
 FTO.DP_ORGENTITY_KEY,  
 FTO.FacilityId as FacilityId,  
 FTO.WAREHOUSE_CODE as WarehouseCode,  
 TM.TransactionTypeName,  
 TM.MilestoneName,  
 TM.MilestoneOrder,
 concat(FTO.source_system_key,'||',FTO.order_sduk) as order_sduk,
 FTO.is_deleted,
 FTO.UTC_ORDER_PLACED_MONTH_part_key
FROM {fact_order_dim_inc}   FTO
INNER JOIN {map_transactiontype_milestone} TM  on FTO.TRANSACTION_TYPE_ID = TM.TransactionTypeId
WHERE FTO.dl_update_timestamp>='{hwm}' 
  AND FTO.ORDER_PLACED_DATE IS NOT NULL
  and FTO.ORDER_PLACED_DATE BETWEEN date_sub('{hwm}', {days_back}) AND current_timestamp
""".format(hwm=hwm,**source_tables,days_back=days_back))
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

# DBTITLE 1,Main function
def main():
    logger.info('Main function is running')
    
    try:
        pid_get = get_pid()
        logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
        pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
        logger.info("pid: {pid}".format(pid=pid))
        ############################ ETL AUDIT #########################################################
        audit_result['process_id'] = pid
        audit_result['process_name'] = 'pr_load_summary_milestone'
        audit_result['process_type'] = 'DataBricks'
        audit_result['layer'] = 'gold'
        audit_result['table_name'] = digital_summary_milestone_et
        audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
        audit_result['start_time'] = start_time
        #################################################################################################
        hwm=get_hwm('gold',digital_summary_milestone_et)
        logger.info(f'hwm {digital_summary_milestone_et}: {hwm}')
    
               
        logger.info('Getting source query')
        source_query = get_query(hwm)
        
        logger.info('Reading source data...')
        src_df = spark.sql(source_query)
        
        ###################### generating hash key  #############################
        hash_key_columns = ['order_sduk','MilestoneOrder'] ## !!! need to change order_sduk, by removing sourcesystemkey, this will require full refresh in cosmosm in future
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
    
        primary_keys = ['order_sduk','MilestoneOrder']
        logger.debug('primary_keys: {primary_keys}'.format(primary_keys=primary_keys))
    
        logger.info(f'Merging to delta path: {digital_summary_milestone_path}')
        
        mergeToDelta(src_df,digital_summary_milestone_path,primary_keys)
    
        logger.info(f'merging to delta path finished: {digital_summary_milestone_path}')
    
        logger.info('setting hwm')
        res=set_hwm('gold',digital_summary_milestone_et,start_time,pid)
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
        raise
    finally:
        logger.info("audit_result: {audit_result}".format(audit_result=audit_result))
        audit(audit_result)


# COMMAND ----------

main()