# Databricks notebook source
# DBTITLE 1,Import python libraries
#imports
from pyspark.sql.types import StructType    
from pyspark.sql import Window
from pyspark.sql.functions import element_at

# COMMAND ----------

# MAGIC %run "/SAIL/includes/common_variables"

# COMMAND ----------

# DBTITLE 1,Import common functions
# MAGIC  %run "/SAIL/includes/common_udfs"
# MAGIC  

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

dbutils.widgets.text("primary_keys", "","")
dbutils.widgets.get("primary_keys")
primary_keys = getArgument("primary_keys").strip()
logger.debug("primary_keys: " + primary_keys)

dbutils.widgets.text("sort_values", "","")
dbutils.widgets.get("sort_values")
sort_values = getArgument("sort_values").strip()
logger.debug("sort_values: " + sort_values)

dbutils.widgets.text("hash_exclude_columns", "","")
dbutils.widgets.get("hash_exclude_columns")
hash_exclude_columns = getArgument("hash_exclude_columns").strip()
logger.debug("hash_exclude_columns: " + hash_exclude_columns)

dbutils.widgets.text("additional_custom_column", "","")
dbutils.widgets.get("additional_custom_column")
additional_custom_column = getArgument("additional_custom_column")
logger.debug("additional_custom_column: " + additional_custom_column)

dbutils.widgets.text("partition_keys", "","")
dbutils.widgets.get("partition_keys")
partition_keys = getArgument("partition_keys")
logger.debug("partition_keys: " + partition_keys)


# COMMAND ----------

# DBTITLE 1,Upsert function
def upsertToDelta(microBatchOutputDf,batchId):
    
    try:
        if microBatchOutputDf.count() == 0:
            logger.info("Process skipped as source has no data")
            audit_result['status'] = 'success'
            audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
            return
      
        logger.info("Running batch for loading {tgt_table_name}: {batchId}".format(tgt_table_name=tgt_table_name,batchId=batchId))
      
        logger.debug("tgt_delta_path: " + target_folder_path)
        deltaDf = DeltaTable.forPath(spark, target_folder_path)
      
      
        logger.debug("Auditing job run")
        run_detail = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
      
        pid_get = get_pid(batchId)
        logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
        pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if '-1|-1' in pid_get else pid_get
        logger.info("pid: {pid}".format(pid=pid))
        
        audit_result['process_id'] = pid
      
             
        logger.debug("Adding filename")
        microBatchOutputDf =microBatchOutputDf.withColumn("dl_file_name",  element_at(split(input_file_name(), "/"),-1))
      
        logger.debug("Adding additional columns")
        microBatchOutputDf = add_additional_columns(microBatchOutputDf,additional_custom_column)
      
        logger.debug("Adding delete column")
        microBatchOutputDf =microBatchOutputDf.withColumn("is_deleted",  lit(1))
      
        logger.debug("Adding audit columns")
        microBatchOutputDf = add_audit_columns(microBatchOutputDf, pid,datetime.now(),datetime.now())
      
        logger.debug("Creating join statement")
        join_condition =[]
        join_keys = json.loads(primary_keys)
        logger.debug("Primary keys: {join_keys}".format(join_keys=join_keys))
      
        join_condition = ["s.{key} = t.{key}".format(key=x) for x in join_keys]
      
        logger.info("Join condition:  {join_condition}".format(join_condition= " and ".join(join_condition)))
      
        logger.debug("remove Duplicates")
        window = Window.partitionBy(join_keys).orderBy(col(sort_values).desc())
        microBatchOutputDf = microBatchOutputDf.withColumn('row_number', row_number().over(window)).filter(col('row_number') == 1).drop('row_number')
        
        logger.debug("partiton key string")
        partition_keys_value_string = partition_key_string(microBatchOutputDf,partition_keys)
        logger.debug("partition_keys_value_string: {partition_keys_value_string}".format(partition_keys_value_string=partition_keys_value_string))   
        
        columns = ['is_deleted','dl_update_pipeline_id','dl_update_timestamp']
        
        logger.debug("Columns : {columns}".format(columns=columns))
        
        update_columns = {"t.{col}".format(col=x) : "s.{col}".format(col=x) for x in columns if x.lower() not in set(['dl_insert_pipeline_id','dl_insert_timestamp'])}
        logger.debug("Update columns:  {update_columns}".format(update_columns=update_columns))
      
        
      
        logger.info("Running upsert")
  
    
        (deltaDf.alias("t")
         .merge(
           microBatchOutputDf.alias("s"),
             partition_keys_value_string + " and ".join(join_condition)+ " and t.is_inbound=0 ")
         .whenMatchedUpdate(condition= 't.is_inbound=0', set = update_columns )
         .execute()
        )
        res_df = deltaDf.history(10).filter(col("operation") == "MERGE").sort(col("timestamp").desc())
        merge_stat_parser(res_df)
        audit_result['status'] = 'success'
        audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
        logger.info("Batch {batchId} finished".format(batchId=batchId))
    except Exception as e:
        audit_result['status'] = 'failed'
        audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
        audit_result['ERROR_MESSAGE'] = str(e)
        raise
    finally:
        logger.info("audit_result".format(audit_result=audit_result))
        audit(audit_result)
    
        
        
    
    
    

# COMMAND ----------

# DBTITLE 1,Main Function
def run_append():
    run_detail = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
      
    pid_get = get_pid()
    audit_result['process_id'] = pid_get

    audit_result['process_name'] = 'autoloader_transform_bronze_to_silver'
    audit_result['process_type'] = 'DataBricks'
    audit_result['layer'] = 'Silver'
    audit_result['table_name'] = tgt_table_name
    audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
    audit_result['start_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")

    logger.info("Autoloader stream started for {tgt_table_name} load".format(tgt_table_name=tgt_table_name))
    schema = StructType.fromJson(json.loads(src_schema))

    logger.info("Checking Source path {src_folder_path}".format(src_folder_path=src_folder_path))
    try:
        dbutils.fs.ls(src_folder_path)
        logger.info("Reading files")
        src_df = (spark.readStream.format("cloudFiles")
           .option("cloudFiles.format", "parquet")
           .schema(schema)
           .load(src_folder_path)
           )
        
        
        logger.info("Writing files")
        streamQuery= (src_df.writeStream
                    .format("delta")
                    .outputMode("append")
                    .foreachBatch(upsertToDelta)
                    .queryName(tgt_table_name)
                    .option("checkpointLocation",checkpoint_location) #Will replace it with event grid
                    .trigger(once=True)
                    .start()
                    .awaitTermination()
                   )
        audit_result['status'] = 'success'
        audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logger.error("Path is not correct {src_folder_path}".format(src_folder_path=src_folder_path))
        audit_result['status'] = 'failed'
        audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
        audit_result['ERROR_MESSAGE'] = str(e)
        logger.info("audit_result".format(audit_result=audit_result))
        audit(audit_result)
        raise
  
  
  
   


# COMMAND ----------

run_append()

# COMMAND ----------

