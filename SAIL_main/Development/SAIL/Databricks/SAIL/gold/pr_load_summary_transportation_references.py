# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <b>Author--GD000012733@ups.com-Vishal</b>

# COMMAND ----------

# DBTITLE 1,Importing python libraries
import datetime,time,json
from pytz import timezone
from  pyspark.sql.functions import input_file_name ,split ,size, from_utc_timestamp, lit  ,concat, when, col, sha1, row_number

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
  logger = _get_logger(time_zone,logging.DEBUG)  
else:
  logger = _get_logger(time_zone,logging.INFO)

# COMMAND ----------

def get_check_load_query(hwm):
  logger.debug("hwm: " + str(hwm))
  query ="""
       select ups_order_number  from {fact_transportation_references}   where dl_update_timestamp>'{hwm}'
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
         select UPS_ORDER_NUMBER from {fact_transportation_references} TREF  where dl_update_timestamp>='{hwm}'
        """.format(**source_tables,hwm=hwm)
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

# DBTITLE 1,Source query
def get_query():
  query =("""
  SELECT distinct
  TREF.UPS_ORDER_NUMBER As UPSOrderNumber,
  TREF.SOURCE_SYSTEM_KEY As SourceSystemKey,
  TREF.SHIPUNIT_ID As LOAD_ID,
  TREF.LOAD_ID As ShipUnitId,
  TREF.REFERENCE_TYPE As ReferenceType,
  TREF.REFRENCE_VALUE As ReferenceValue,
  TREF.REFERENCE_LEVEL As ReferenceLevel,
  concat(FO.source_system_key,'||',FO.order_sduk) as ORDER_SDUK,
  concat(TREF.source_system_key,'||',TREF.REFERENCE_SDUK) as REFERENCE_SDUK, 
  concat(FTTR.SOURCE_SYSTEM_KEY,'||',FTTR.TRANSPORTATION_SDUK) as TRANSPORTATION_SDUK,
  0 is_deleted
 FROM {fact_order_dim_inc}  FO  
  INNER JOIN delta_fetch_tv FTV on (FO.UPS_ORDER_NUMBER = FTV.UPS_ORDER_NUMBER)
  JOIN {fact_transportation} FTTR  ON       
         (CASE WHEN FTTR.TRANS_ONLY_FLAG <> 'TRANS_ONLY'  THEN   NVL(FTTR.UPS_WMS_SOURCE_SYSTEM_KEY,FO.SOURCE_SYSTEM_KEY)  ELSE FTTR.SOURCE_SYSTEM_KEY END = FO.SOURCE_SYSTEM_KEY  --10/25/2021     
      AND CASE WHEN FTTR.TRANS_ONLY_FLAG <> 'TRANS_ONLY'  THEN FTTR.UPS_WMS_ORDER_NUMBER ELSE FTTR.UPS_ORDER_NUMBER END = FO.UPS_ORDER_NUMBER)   --10/25/2021
  JOIN {fact_transportation_references} TREF ON FTTR.UPS_ORDER_NUMBER = TREF.UPS_ORDER_NUMBER AND FTTR.SOURCE_SYSTEM_KEY = TREF.SOURCE_SYSTEM_KEY --10/25/2021
""".format(**source_tables))
  logger.debug("query : " + query)
  return(query)


# COMMAND ----------

def main():
    logger.info('Main function is running')
  
    try:
        pid_get = get_pid()
        logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
        pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
        logger.info("pid: {pid}".format(pid=pid))
        ############################ ETL AUDIT #########################################################
        audit_result['process_id'] = pid
        audit_result['process_name'] = 'pr_load_summary_transportation_references'
        audit_result['process_type'] = 'DataBricks'
        audit_result['layer'] = 'gold'
        audit_result['table_name'] = digital_summary_transportation_references_et
        audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
        audit_result['start_time'] = start_time
        ################################################################################################
        hwm=get_hwm('gold',digital_summary_transportation_references_et)
        logger.info('hwm {digital_summary_transportation_references_et}: {hwm}')
        
        spark.sql(get_delta_query(hwm))
        logger.info("get_delta_query finished")  
        
        check_load = spark.sql(get_check_load_query(hwm)).count()
        logger.info("check_load: " + str(check_load) )
        
        if check_load>0:
            
    
            logger.info('Getting source query')
            source_query = get_query()
            
            logger.info('Reading source data...')
            src_df = spark.sql(source_query)
            
            hash_key_columns = ['SourceSystemKey','UPSOrderNumber','ORDER_SDUK','REFERENCE_SDUK','TRANSPORTATION_SDUK']
            logger.debug(f"hash key columns: {hash_key_columns}")
        
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
        
            primary_keys = ['SourceSystemKey','UPSOrderNumber','ORDER_SDUK','REFERENCE_SDUK','TRANSPORTATION_SDUK']
            logger.debug('primary_keys: {primary_keys}'.format(primary_keys=primary_keys))
        
            logger.info(f'Merging to delta path: {digital_summary_transportation_references_path}')
        
            mergeToDelta(src_df,digital_summary_transportation_references_path,primary_keys)
            logger.info(f'Merging to delta path finished: {digital_summary_transportation_references_path}')
        else:
            logger.info('Nothing to process')
    
        logger.info('setting hwm')
        res=set_hwm('gold',digital_summary_transportation_references_et,start_time,pid)
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
        logger.info("audit_result : {audit_result}".format(audit_result=audit_result))
        audit(audit_result)


# COMMAND ----------

main()