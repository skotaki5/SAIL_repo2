# Databricks notebook source
# DBTITLE 1,Import python libraries
# import logging
from pyspark.sql.types import StructType    
from pyspark.sql import Window
from pyspark.sql.functions import element_at

# COMMAND ----------

# DBTITLE 1,Importing common variables
# MAGIC %run "/SAIL/includes/common_variables"

# COMMAND ----------

# DBTITLE 1,Import common functions
# MAGIC %run "/SAIL/includes/common_udfs"

# COMMAND ----------

# DBTITLE 1,Set Timezone
time_zone = 'UTC' # Check for which timezone to be used

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

# DBTITLE 1,Read parameters
"""
Global Parameters
"""

dbutils.widgets.text("src_folder_path", "","")
dbutils.widgets.get("src_folder_path")
src_folder_path = getArgument("src_folder_path").strip()
logger.debug("src_folder_path: " + src_folder_path)

dbutils.widgets.text("target_folder_path", "","")
dbutils.widgets.get("target_folder_path")
target_folder_path = getArgument("target_folder_path").strip()
logger.debug("target_folder_path: " + target_folder_path)

dbutils.widgets.text("src_schema", "","")
dbutils.widgets.get("src_schema")
src_schema = getArgument("src_schema").strip()
logger.debug("src_schema: " + src_schema)

dbutils.widgets.text("tgt_table_name", "","")
dbutils.widgets.get("tgt_table_name")
tgt_table_name = getArgument("tgt_table_name").strip()
logger.debug("tgt_table_name: " + tgt_table_name)


dbutils.widgets.text("checkpoint_location", "","")
dbutils.widgets.get("checkpoint_location")
checkpoint_location = getArgument("checkpoint_location").strip()
logger.debug("checkpoint_location: " + checkpoint_location)



# COMMAND ----------

# DBTITLE 1,Main Function
def run_append():
    
    try:
        audit_result['process_name'] = 'autoloader_fact_inventory_transform_bronze_to_silver'
        audit_result['process_type'] = 'DataBricks'
        audit_result['layer'] = 'silver'
        audit_result['table_name'] = 'fact_inventory_snapshot'
        audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
        audit_result['start_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
        
        pid_get = get_pid()
        logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
        pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
        logger.info("pid: {pid}".format(pid=pid))
        audit_result['process_id'] = pid
        
        logger.info("Autoloader stream started for {tgt_table_name} load".format(tgt_table_name=tgt_table_name))
        schema = StructType.fromJson(json.loads(src_schema))
      
        logger.info("Checking Source path {src_folder_path}".format(src_folder_path=src_folder_path))
        try:
            dbutils.fs.ls(src_folder_path)
                
        except Exception as e:
            audit_result['status'] = 'failed'
            audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
            audit_result['ERROR_MESSAGE'] = str(e)
            raise
      
        logger.info("Reading files")
        
        
        lst = dbutils.fs.ls(src_folder_path)
        f1=sorted(lst, reverse=True)[0]
        src_df=spark.read.parquet(f1.path)
    
        
        logger.debug("Adding delete column")
        src_df =src_df.withColumn("is_deleted",  lit(0))
        
        logger.debug("Adding filename")
        src_df =src_df.withColumn("dl_file_name",  element_at(split(input_file_name(), "/"),-1))
      
        logger.debug("Adding audit columns")
        src_df=add_audit_columns(src_df,pid,datetime.now(),datetime.now())
        
        
        logger.info("Writing files")
        logger.info('Writing to delta path: {target_folder_path}'.format(target_folder_path=target_folder_path))
        src_df.write.format("delta").mode("overwrite").save(target_folder_path)
        
    except Exception as e:
        audit_result['status'] = 'failed'
        audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
        audit_result['ERROR_MESSAGE'] = str(e)
        raise
    finally:
        logger.info("audit_result: {audit_result}".format(audit_result=audit_result))
        audit(audit_result)


# COMMAND ----------

run_append()