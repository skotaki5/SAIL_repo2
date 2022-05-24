# Databricks notebook source
from pytz import timezone
import pytz
from datetime import datetime,timedelta
import uuid
from pyspark.sql.functions import date_format, current_timestamp, from_utc_timestamp, lit ,col ,expr ,input_file_name,split ,size,concat,when,sha1,row_number,collect_set
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import DateType
from delta.tables import *
import logging
import json

# COMMAND ----------

start_time = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

def _get_logger(Timezone,level=logging.INFO):
    logging.Formatter.converter = lambda *args: datetime.now(tz=timezone(Timezone)).timetuple()
    logger = spark._jvm.org.apache.log4j
    logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
    logger = logging.getLogger('azure')
    logger.setLevel(logging.WARNING)
    logging.basicConfig(format='%(asctime)s -%(funcName)s() - %(levelname)8s:  %(message)s', level=level)
    return logging

# COMMAND ----------

def __register_table(table_name: str,file_type: str,file_path: str):
    """
    Private Function - registers a temporary target table
    - uses 't_{uuid}' as table names
    """
    
    uuid_value = uuid.uuid4().hex
    external_table_name= uuid_value + '_' + table_name
    logger.debug("Creating table: " + table_name + " of type " + file_type + " at path " + file_path)
    if file_type.lower()=='delta':
      spark.sql('CREATE TABLE {table_name} USING {type} LOCATION "{path}"'.format(table_name=external_table_name, type=file_type, path=file_path))
    if file_type.lower()=='csv':
      spark.sql('CREATE TABLE IF NOT EXISTS {table_name} USING {type} OPTIONS (path "{path}", header "true", mode "FAILFAST")'.format(table_name=external_table_name, type=file_type, path=file_path))
    if file_type.lower()=='parquet':
      spark.sql('CREATE TABLE IF NOT EXISTS {table_name} USING {type} OPTIONS (path "{path}", header "true", mode "FAILFAST")'.format(table_name=external_table_name, type=file_type, path=file_path))
    logger.debug("Created table: " + table_name + " of type " + file_type + " at path " + file_path)
    return external_table_name

# COMMAND ----------

def __drop_register_table(table_name: str):
    """
    Private Function - drops the temporary target table
    - Drops 't_{uuid}' as table names
    """
    logger.debug("Droping table: " + table_name)
    spark.sql('DROP TABLE IF EXISTS {table_name}'.format(table_name=table_name))
    logger.debug("Dropped table: " + table_name)

# COMMAND ----------

def add_audit_columns(source_data_frame, DL_pipeline_id,DL_insert_timestamp=None,DL_update_timestamp=None):
    logger.debug("..........executing add_audit_column function..............")
    localtimezone = pytz.timezone(time_zone)
    logger.debug("DL_insert_pipeline_id: " + str(DL_pipeline_id))
    dl_time_stamp = DL_insert_timestamp.astimezone(localtimezone).strftime("%Y-%m-%d %H:%M:%S")
    logger.debug("dl_time_stamp: " + str(dl_time_stamp))
    source_data_frame = source_data_frame.withColumn("dl_insert_pipeline_id", lit(DL_pipeline_id))
    
    logger.debug("DL_insert_timestamp: {DL_insert_timestamp}".format(DL_insert_timestamp=dl_time_stamp))
    if DL_insert_timestamp != None:
        source_data_frame = source_data_frame.withColumn("dl_insert_timestamp",lit(dl_time_stamp))
        
    logger.debug("DL_update_pipeline_id: " + str(DL_pipeline_id))
    source_data_frame = source_data_frame.withColumn("dl_update_pipeline_id", lit(DL_pipeline_id))
    
    logger.debug("DL_update_timestamp: {DL_update_timestamp}".format(DL_update_timestamp=dl_time_stamp))
    if DL_update_timestamp !=None:
        source_data_frame = source_data_frame.withColumn("dl_update_timestamp", lit(dl_time_stamp))
  
    return source_data_frame

# COMMAND ----------

def subtract_list(list_a: list , list_b: list):
    """
    Private Function - removes elements from a list via subtaction
 
    Parameters:
    list_a (list): The original list
    list_b (list): A list of values to remove from list_a
 
    Returns:
    list: returns a substracted list of elements
    
    """
    return [x for x in list_a if x not in set(list_b)]

# COMMAND ----------

def sha1_concat(hash_columns):
  """
  Private Function - concatenates columns provided using : delimiter
 
  Parameters:
  hash_columns (list): A string list containing the list of columns to hash
 
  Returns:
  str: returns Hash sha1{'column1:column2:'}
  """
  column_ref_list = [when(col(hash_col_name).isNull(),lit(":")).otherwise(concat(col(hash_col_name).cast("string"),lit(":"))) 
                     for hash_col_name in hash_columns]
  concat_string = sha1(concat(*column_ref_list))
  return concat_string

# COMMAND ----------

def add_additional_columns(df,columns):
    additional_custom_column_list =[] if len(columns)==0 or columns.upper() == 'N' or columns == None else json.loads(columns)
    for column in additional_custom_column_list:
        logger.debug('Adding column: ' + column['name'] + ' -----> ' + column['value'])
        df  = df.withColumn(column['name'], expr(column['value']))
    return df

# COMMAND ----------

def partition_key_string(df,partition_keys):
    partition_keys_list =[] if len(partition_keys)==0 or partition_keys.upper() == 'N' or partition_keys == None else json.loads(partition_keys)
    partition_keys_value_string = ''
    for pk in partition_keys_list:
        val=df.select(collect_set(col(pk).cast('string')).alias(pk)).first()[pk]
        if 'MONTH_PART_KEY' in pk.upper() :
            val.append('190001')
        partition_keys_value_string += "t." + pk + " in ('" + "','".join(val) + "') and " 
    return partition_keys_value_string

# COMMAND ----------

def mergeToPartitionedDelta(source_df,target_folder_path,primary_keys,partition_keys_value_string):
    logger.debug("tgt_delta_path: " + target_folder_path)
    deltaDf = DeltaTable.forPath(spark, target_folder_path)
      
    logger.debug("Creating join statement")
    join_condition =[]
    join_keys = primary_keys
    logger.debug("Primary keys: {join_keys}".format(join_keys=join_keys))
    
    join_condition = ["s.{key} = t.{key}".format(key=x) for x in join_keys]
    
    logger.info("Join condition:  {join_condition}".format(join_condition= " and ".join(join_condition)))
    
    columns = source_df.schema.fieldNames()
    
    logger.debug("Columns : {columns}".format(columns=columns))
    
    update_columns = {"t.{col}".format(col=x) : "s.{col}".format(col=x) for x in columns if x.lower() not in set(['dl_insert_pipeline_id','dl_insert_timestamp'])}
    logger.debug("Update columns:  {update_columns}".format(update_columns=update_columns))
    
    insert_columns = {"t.{col}".format(col=x) : "s.{col}".format(col=x) for x in columns}
    logger.debug("Insert columns:  {insert_columns}".format(insert_columns=insert_columns))
    
    logger.info("Running upsert")
    
    (deltaDf.alias("t")
     .merge(
       source_df.alias("s"),
           partition_keys_value_string + " and ".join(join_condition))
     .whenMatchedUpdate(condition= 's.dl_hash <> t.dl_hash', set = update_columns )
     .whenNotMatchedInsert(values = insert_columns)
     .execute()
    )
    history_df = deltaDf.history(10).filter(col("operation") == "MERGE").sort(col("timestamp").desc())
    merge_stat_parser(history_df)
    

# COMMAND ----------

def mergeToDelta(source_df,target_folder_path,primary_keys):
    logger.debug("tgt_delta_path: " + target_folder_path)
    deltaDf = DeltaTable.forPath(spark, target_folder_path)
      
    logger.debug("Creating join statement")
    join_condition =[]
    join_keys = primary_keys
    logger.debug("Primary keys: {join_keys}".format(join_keys=join_keys))
    
    join_condition = ["s.{key} = t.{key}".format(key=x) for x in join_keys]
    
    logger.info("Join condition:  {join_condition}".format(join_condition= " and ".join(join_condition)))
    
    columns = source_df.schema.fieldNames()
    
    logger.debug("Columns : {columns}".format(columns=columns))
    
    update_columns = {"t.{col}".format(col=x) : "s.{col}".format(col=x) for x in columns if x.lower() not in set(['dl_insert_pipeline_id','dl_insert_timestamp'])}
    logger.debug("Update columns:  {update_columns}".format(update_columns=update_columns))
    
    insert_columns = {"t.{col}".format(col=x) : "s.{col}".format(col=x) for x in columns}
    logger.debug("Insert columns:  {insert_columns}".format(insert_columns=insert_columns))
    
    logger.info("Running upsert")
    
    (deltaDf.alias("t")
     .merge(
       source_df.alias("s"),
       " and ".join(join_condition))
     .whenMatchedUpdate(condition= 's.dl_hash <> t.dl_hash', set = update_columns )
     .whenNotMatchedInsert(values = insert_columns)
     .execute()
    )
    history_df = deltaDf.history(10).filter(col("operation") == "MERGE").sort(col("timestamp").desc())
    merge_stat_parser(history_df)

# COMMAND ----------

def insertOnlyMergeToDelta(source_df,target_folder_path,primary_keys):
    logger.debug("tgt_delta_path: " + target_folder_path)
    deltaDf = DeltaTable.forPath(spark, target_folder_path)
      
    logger.debug("Creating join statement")
    join_condition =[]
    join_keys = primary_keys
    logger.debug("Primary keys: {join_keys}".format(join_keys=join_keys))
    
    join_condition = ["s.{key} = t.{key}".format(key=x) for x in join_keys]
    
    logger.info("Join condition:  {join_condition}".format(join_condition= " and ".join(join_condition)))
    
    columns = source_df.schema.fieldNames()
    
    logger.debug("Columns : {columns}".format(columns=columns))
    
    insert_columns = {"t.{col}".format(col=x) : "s.{col}".format(col=x) for x in columns}
    logger.debug("Insert columns:  {insert_columns}".format(insert_columns=insert_columns))
    
    logger.info("Running upsert")
    (deltaDf.alias("t")
     .merge(
       source_df.alias("s"),
       " and ".join(join_condition)+' and t.is_deleted=0'
           )
     .whenNotMatchedInsert(values =insert_columns)
     .execute()
    )
    history_df = deltaDf.history(10).filter(col("operation") == "MERGE").sort(col("timestamp").desc())
    merge_stat_parser(history_df)

# COMMAND ----------

#def get_hwm(table_name):
#  logger.debug("table_name : {table_name}".format(table_name=table_name))
#  query = """SELECT max(hwm) from {delta_control} where table_name = lower('{table_name}')""".format(**source_tables,table_name=table_name)
#  logger.debug("query : {query}".format(query=query))
#  hwm_val = spark.sql(query).first()[0]
#  logger.debug("Hwm table returned : {hwm_val}".format(hwm_val=hwm_val))
#  hwm = '1900-01-01 00:00:00' if hwm_val == None else hwm_val
#  logger.debug("Hwm returned : {hwm}".format(hwm=hwm))
#  return(hwm)
  
def get_hwm(schema,table_name):
    logger.debug("table_name : {schema}.{table_name}".format(schema=schema,table_name=table_name))
    pushdown_query = "(select max(hwm) as hwm from [controller].[delta_control] where delta_schema = '{schema}' and table_name = '{table_name}') a ".format(schema=schema,table_name=table_name)
    logger.debug("pushdown_query : {pushdown_query}".format(pushdown_query=pushdown_query))
    df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
    hwm_val = df.first()[0]
    logger.debug("Hwm table returned : {hwm_val}".format(hwm_val=hwm_val))
    hwm = '1900-01-01 00:00:00' if hwm_val == None else hwm_val
    logger.debug("Hwm returned : {hwm}".format(hwm=hwm))
    return(hwm)

# COMMAND ----------

#def set_hwm(table_name,hwm,pid):
#  logger.debug("table_name : {table_name}".format(table_name=table_name))
#  logger.debug("hwm : {hwm}".format(hwm=hwm))
#  logger.debug("pid : {pid}".format(pid=pid))
#  query = """insert into {delta_control} (pipeline_id , table_name , hwm , insert_ts , insert_date ,delta_schema,adjustment_seconds) VALUES ('{pid}', lower('{table_name}'), '{hwm}', '{current_time}', '{current_date}',null,null)""".format(**source_tables,table_name=table_name,hwm=hwm,pid=pid, current_time=datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S") ,current_date=datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d"))
#  logger.debug("query : {query}".format(query=query))
#  rows_inserted = spark.sql(query).first()[1]
#  
#  return("hwm Inserted : {rows_inserted}".format(rows_inserted=rows_inserted))

def set_hwm(schema,table_name,hwm,pid):

    logger.debug("table_name : {schema}.{table_name}".format(schema=schema,table_name=table_name))
    logger.debug("hwm : {hwm}".format(hwm=hwm))
    logger.debug("pid : {pid}".format(pid=pid))
    
    statement = """insert into [controller].[delta_control] (pipeline_id , table_name , hwm , insert_ts , insert_date ,delta_schema,adjustment_seconds) VALUES ('{pid}', lower('{table_name}'), '{hwm}', '{current_time}', '{current_date}','{schema}',3600)""".format(schema=schema,table_name=table_name,hwm=hwm,pid=pid, current_time=datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S") ,current_date=datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d"))
    logger.debug("query : {query}".format(query=statement))
    driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
    con = driver_manager.getConnection(jdbcUrl, username, password)
    try:
        # Create callable statement and execute it
        exec_statement = con.prepareCall(statement)
        exec_statement.execute()
        return("hwm Inserted : 1")
    except:
        raise
    finally:
        # Close connections
        exec_statement.close()
        con.close()
        

# COMMAND ----------

def get_pid(batch_id=None):
  logger.debug("Auditing job run")
  run_detail = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
  
  try:
    runId = str(run_detail['tags']['multitaskParentRunId'])
  except KeyError:
    try:
      runId = str(run_detail['tags']['runId'])
    except KeyError:
      runId = "-1"
  logger.debug("runId: " + runId)
  
  try:
    jobId = str(run_detail['tags']['jobId'])
  except KeyError:
    jobId = "-1"
  logger.debug("jobId: " + jobId)
  
  logger.debug("batch_id: " + str(batch_id))
  if batch_id ==None:
    pid = jobId + "|" + runId 
  else:
    pid = jobId + "|" + runId + "|" + str(batch_id)
  logger.debug("pid: " + pid)
  return pid

# COMMAND ----------

def audit(audit_result):
    """
    
    """
    
    logger.debug("audit_result : {audit_result}".format(audit_result=audit_result))    
    col_list=[]
    val_list=[]
    for key,val in audit_result.items():
        if val !=None:
            col_list.append(key)
            if isinstance(val,str):
                val=val.replace("'","''")
                val_list.append(f"'{val}'")
            else:
                val_list.append(str(val))
                
    logger.debug("col_list : {col_list}".format(col_list=col_list)) 
    logger.debug("val_list : {val_list}".format(val_list=val_list)) 
    
    statement= f"""insert into controller.etl_audit 
    (
     {",".join(col_list)}
     ,insert_timestamp
     ,update_timestamp
    )
    VALUES
    (
    {",".join(val_list)}
    ,CURRENT_TIMESTAMP
    ,CURRENT_TIMESTAMP
    )
    """
    
    logger.info("query : {query}".format(query=statement))
    driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
    con = driver_manager.getConnection(jdbcUrl, username, password)
    try:
        # Create callable statement and execute it
        exec_statement = con.prepareCall(statement)
        exec_statement.execute()
        logger.info("audit record inserted : 1")
    except:
        raise
    finally:
        # Close connections
        exec_statement.close()
        con.close()
        

# COMMAND ----------

def merge_stat_parser(df):
    row = df.first()
    audit_result['numSourceRows'] = row['operationMetrics']['numSourceRows']
    audit_result['numTargetRowsInserted'] = row['operationMetrics']['numTargetRowsInserted']
    audit_result['numTargetRowsUpdated'] = row['operationMetrics']['numTargetRowsUpdated']
    audit_result['numTargetRowsDeleted'] = row['operationMetrics']['numTargetRowsDeleted']
    audit_result['numTargetRowsCopied'] = row['operationMetrics']['numTargetRowsCopied']
    audit_result['numTargetFilesRemoved'] = row['operationMetrics']['numTargetFilesRemoved']
    audit_result['numTargetFilesAdded'] = row['operationMetrics']['numTargetFilesAdded']
    audit_result['executionTimeMs'] = row['operationMetrics']['executionTimeMs']
    audit_result['scanTimeMs'] = row['operationMetrics']['scanTimeMs']
    audit_result['rewriteTimeMs'] = row['operationMetrics']['rewriteTimeMs']
    audit_result['userName'] = row['userName']
   # audit_result['operationParameters'] = row['operationParameters']
    audit_result['operation'] = row['operation']

# COMMAND ----------

