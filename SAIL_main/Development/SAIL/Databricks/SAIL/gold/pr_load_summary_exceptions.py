# Databricks notebook source
# MAGIC %md
# MAGIC <b>Author </b>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;: GD000012020 Shrey Jain </br>
# MAGIC <b>Description</b>  : this notebook is to load summary_exceptions table.

# COMMAND ----------

import datetime,time,json
from pytz import timezone
from  pyspark.sql.functions import input_file_name ,split ,size, from_utc_timestamp, lit  ,concat, when, col, sha1, row_number

# COMMAND ----------

# DBTITLE 1,Import Common Variables
# MAGIC %run "/SAIL/includes/common_variables"

# COMMAND ----------

# DBTITLE 1,Import Common Utilities
# MAGIC %run "/SAIL/includes/common_udfs"

# COMMAND ----------

start_time = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# DBTITLE 1,Set debug mode
dbutils.widgets.text("log_debug_mode", "","")
dbutils.widgets.get("log_debug_mode")
log_debug_mode = getArgument("log_debug_mode").strip()

if log_debug_mode == "Y":
  logger = _get_logger(time_zone,logging.DEBUG)  
else:
  logger = _get_logger(time_zone,logging.INFO)
  

# COMMAND ----------

# DBTITLE 1,Query
def get_query(hwm):
  logger.debug("hwm: " + str(hwm))
  query = """ 
     SELECT   
      EX.UPS_ORDER_NUMBER UPSOrderNumber,
      EX.SOURCE_SYSTEM_KEY SourceSystemKey,
      UTC_EXCEPTION_CREATED_DATE UTC_ExceptionCreatedDate,
      EXCEPTION_CREATED_DATE_OTZ OTZ_ExceptionCreatedDate,
      LOFST_EXCEPTION_CREATED_DATE_OTZ LOFST_ExceptionCreatedDate_OTZ,
      EXCEPTION_CREATED_DATE_DTZ ExceptionCreatedDate_DTZ,
      LOFST_EXCEPTION_CREATED_DATE_DTZ LOFST_ExceptionCreatedDate_DTZ,
      EXCEPTION_DESCRIPTION ExceptionDescription,
      EXCEPTION_EVENT ExceptionEvent,
      EXCEPTION_REASON ExceptionReason,
      EXCEPTION_REASON_TYPE ExceptionReasonType,
      EXCEPTION_CATEGORY ExceptionCategory,
      RESPONSIBLE_PARTY ResponsibleParty,
      EXCEPTION_PRIMARY_INDICATOR ExceptionPrimaryIndicator,
      EXCEPTION_COUNT ExceptionCount,
      TRANSPORTATION_EXCEPTION_SDUK TRANSPORTATION_EXCEPTION_SDUK,
      MMA.ActivityName AS ExceptionType,
      CAST(Null as VARCHAR(10)) DateTimeShippedTimeZone,
      CAST(Null as VARCHAR(10)) ActualScheduledDeliveryDateTimeZone,
      0 is_deleted
        FROM {fact_transportation_exception} EX
        LEFT JOIN {map_milestone_activity} MMA ON MMA.ActivityCode=EX.EXCEPTION_EVENT
                                              AND MMA.SOURCE_SYSTEM_KEY=EX.SOURCE_SYSTEM_KEY
        where  EX.dl_update_timestamp>'{hwm}'
  """.format(**source_tables,hwm=hwm)
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

# DBTITLE 1,Main Function
def main():
    logger.info("Main function is running")
    try:
        pid_get = get_pid()
        logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
        pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
        logger.info("pid: {pid}".format(pid=pid))
        ############################ ETL AUDIT #########################################################
        audit_result['process_id'] = pid
        audit_result['process_name'] = 'pr_load_summary_exceptions'
        audit_result['process_type'] = 'DataBricks'
        audit_result['layer'] = 'gold'
        audit_result['table_name'] = digital_summary_exceptions_et
        audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
        audit_result['start_time'] = start_time
        ################################################################################################
        hwm=get_hwm('gold',digital_summary_exceptions_et)
        logger.info(f'hwm {digital_summary_exceptions_et}: {hwm}')
        
        src_df = spark.sql(get_query(hwm))
        logger.info("query finished")
        
        ###################### generating hash key  #############################
        hash_key_columns = ['SourceSystemKey','TRANSPORTATION_EXCEPTION_SDUK']
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
  
        primary_keys = ['SourceSystemKey','TRANSPORTATION_EXCEPTION_SDUK']
        logger.debug('primary_keys: {primary_keys}'.format(primary_keys=primary_keys))
  
        logger.info(f'Merging to delta path: {digital_summary_exceptions_path}')
  
        mergeToDelta(src_df,digital_summary_exceptions_path,primary_keys)
        logger.info(f'merging to delta path finished: {digital_summary_exceptions_path}')
        
        logger.info('setting hwm')
        res=set_hwm('gold',digital_summary_exceptions_et,start_time,pid)
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