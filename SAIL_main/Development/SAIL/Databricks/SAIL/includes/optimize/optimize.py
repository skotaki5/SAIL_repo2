# Databricks notebook source
# MAGIC %run "/SAIL/includes/common_variables"

# COMMAND ----------

# MAGIC %run "/SAIL/includes/common_udfs"

# COMMAND ----------

st_dt =datetime.now(tz=timezone(time_zone))
start_time = st_dt.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

dbutils.widgets.text("log_debug_mode", "","")
dbutils.widgets.get("log_debug_mode")
log_debug_mode = getArgument("log_debug_mode").strip()

if log_debug_mode == "Y":
    logger = _get_logger(time_zone,logging.DEBUG)  
else:
    logger = _get_logger(time_zone,logging.INFO)
  

# COMMAND ----------

dbutils.widgets.text("table_name", "","")
dbutils.widgets.get("table_name")
table_name = getArgument("table_name").strip()
logger.debug("table_name: " + table_name)

# COMMAND ----------

spark.conf.set('spark.databricks.delta.optimize.maxFileSize',source_optimize[table_name]['file_size']) #-- 32 mb

# COMMAND ----------


def main():
    logger.info('Main function is running')
    audit_result['process_name'] = 'optimize'
    audit_result['process_type'] = 'dataBricks'
    audit_result['layer'] = source_optimize[table_name]['table_name'].split('.')[0]
    audit_result['table_name'] = table_name
    audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
    audit_result['start_time'] = start_time
    
    try:
        logger.info("Optimize process started for "+table_name)
        logger.info("log_debug_mode : "+log_debug_mode)
        pid_get = get_pid()
        logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
        pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
        logger.info("pid: {pid}".format(pid=pid))
        
        audit_result['process_id'] = pid
        sql_statement='optimize {table_name} zorder by ({optimize_column}) '.format(**source_optimize[table_name])
        logger.debug("optimize",sql_statement)
        spark.sql("{s}".format(s=sql_statement))
        deltaTable = DeltaTable.forName(spark, source_optimize[table_name]['table_name'])
        history_df=deltaTable.history(10).filter(col("operation") == "OPTIMIZE").sort(col("timestamp").desc()).first()
        if history_df!=None:
            audit_result['numTargetFilesRemoved'] = history_df['operationMetrics']["numRemovedFiles"]
            audit_result['numTargetFilesAdded'] = history_df['operationMetrics']["numAddedFiles"]
            audit_result['dataWritten_byte'] = history_df['operationMetrics']["numAddedBytes"]
        audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
        audit_result['status'] = 'success'
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