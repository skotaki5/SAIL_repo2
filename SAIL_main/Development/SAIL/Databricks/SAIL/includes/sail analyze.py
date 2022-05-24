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



# COMMAND ----------

if log_debug_mode == "Y":
    logger = _get_logger(time_zone,logging.DEBUG)
else:
    logger = _get_logger(time_zone,logging.INFO)
  

# COMMAND ----------

table_names=source_tables.values()


# COMMAND ----------

pid_get = get_pid()
logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
logger.info("pid: {pid}".format(pid=pid))
audit_result['process_id'] = pid
audit_result['process_name'] = 'analyze'
audit_result['process_type'] = 'dataBricks'
    
for table in table_names:
    st_dt =datetime.now(tz=timezone(time_zone))
    start_time = st_dt.strftime("%Y-%m-%d %H:%M:%S")
    audit_result['layer'] = table.split('.')[0]
    audit_result['table_name'] = table.split('.')[1]
    audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
    audit_result['start_time'] = start_time
    try :
        sql=f""" analyze table {table} compute statistics for all columns"""
        logger.info(sql)
        spark.sql(sql)
        audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
        audit_result['status'] = 'success'
        audit_result['ERROR_MESSAGE'] = None
        audit(audit_result)
    except Exception as e:
        audit_result['status'] = 'failed'
        audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
        audit_result['ERROR_MESSAGE'] = str(e)
        raise
        audit(audit_result)
        
    