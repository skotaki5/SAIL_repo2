# Databricks notebook source
"""
Author           : Vinoth Kumar Gopal
Description      : This notebook is to delete document in cosmos container.

Vinoth: Version 1.1 --Added exception handling logic for retry
"""

# COMMAND ----------

# DBTITLE 1,Importing common variables
# MAGIC %run "/SAIL/includes/common_variables"

# COMMAND ----------

# DBTITLE 1,Importing common udfs
# MAGIC %run "/SAIL/includes/common_udfs"

# COMMAND ----------

# DBTITLE 1,Setting debug mode
# dbutils.widgets.text("log_debug_mode", "")
dbutils.widgets.get("log_debug_mode")
log_debug_mode = getArgument("log_debug_mode").strip()

if log_debug_mode == "Y":
    logger = _get_logger(time_zone,logging.DEBUG) 
else:
    logger = _get_logger(time_zone,logging.INFO)

# COMMAND ----------

from datetime import datetime,timedelta
from pyspark.sql.functions import col
from pytz import timezone
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as cosmos_exp
import json


# https://azuresdkdocs.blob.core.windows.net/$web/python/azure-cosmos/4.0.0/index.html

# COMMAND ----------

#time_zone = 'UTC' 
scope = 'key-vault-secrets'
cosmos_end_point = dbutils.secrets.get(scope,"cosmosEndpoint")
cosmos_master_key = dbutils.secrets.get(scope,"cosmosMasterKey")
sproc_name = "bulkDelete"
cosmos_database_name = dbutils.widgets.get("cosmos_database_name").strip()
cosmos_container_name = dbutils.widgets.get("cosmos_container_name").strip()
partition_key_column = dbutils.widgets.get("partition_key").strip()
date_column = dbutils.widgets.get("date_column").strip()
days_back_to_del = dbutils.widgets.get("days_back_to_del").strip()
other_del_cond = dbutils.widgets.get("other_del_cond").strip()
total_del_cnt = 0

st_dt =datetime.now(tz=timezone(time_zone))
start_time = st_dt.strftime("%Y-%m-%d %H:%M:%S")

del_dt = datetime.now(tz=timezone(time_zone)) - timedelta(days=int(days_back_to_del))
date_back_to_del = del_dt.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# DBTITLE 1,Create Delete Query
if date_column == "NA":
    cond_str = other_del_cond
else:
    cond_str = "c.{date_column} < '{date_back_to_del}' or {other_del_cond}".format(partition_key_column=partition_key_column, date_column=date_column, date_back_to_del=date_back_to_del,other_del_cond=other_del_cond)
     
partition_key_query = "SELECT DISTINCT c.{partition_key_column}, 1 as filcol FROM c WHERE {cond_str}".format(partition_key_column=partition_key_column, cond_str=cond_str)
    
del_sel_query = "SELECT c._self FROM c WHERE {cond_str}".format(cond_str=cond_str)

logger.info("partition_key_query : "+partition_key_query)        
logger.info("del_sel_query : "+del_sel_query)

# COMMAND ----------

# DBTITLE 1,Spark Cosmos Connection
scope = 'key-vault-secrets'
cosmosEndpoint = dbutils.secrets.get(scope,"cosmosEndpoint")
cosmosMasterKey = dbutils.secrets.get(scope,"cosmosMasterKey")
cosmosDatabaseName = cosmos_database_name
cosmosContainerName = cosmos_container_name

cfg = {
  "spark.cosmos.accountEndpoint" : cosmosEndpoint,
  "spark.cosmos.accountKey" : cosmosMasterKey,
  "spark.cosmos.database" : cosmosDatabaseName,
  "spark.cosmos.container" : cosmosContainerName,
  "spark.cosmos.read.customQuery" : partition_key_query
}

# COMMAND ----------

def main():
    logger.info('Main function is running')
    audit_result['process_name'] = 'delete_'+cosmos_container_name+'_Container'
    audit_result['process_type'] = 'DataBricks'
    audit_result['layer'] = 'cosmos'
    audit_result['table_name'] = 'cosmos_'+cosmos_container_name
    audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
    audit_result['start_time'] = start_time
    
    try:
        logger.info("Delete process started for "+cosmos_container_name)
        logger.info("cosmos_database_name : "+cosmos_database_name)
        logger.info("cosmos_container_name : "+cosmos_container_name)
        logger.info("partition_key_column : "+partition_key_column)
        logger.info("date_column : "+date_column)
        logger.info("days_back_to_del : "+days_back_to_del)
        logger.info("other_del_cond : "+other_del_cond)
        logger.info("log_debug_mode : "+log_debug_mode)
        
        pid_get = get_pid()
        logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
        pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
        logger.info("pid: {pid}".format(pid=pid))
        
        audit_result['process_id'] = pid
        
        logger.info("Creating client connection")
        
        client = cosmos_client.CosmosClient(cosmos_end_point, {'masterKey': cosmos_master_key})
        database = client.get_database_client(cosmos_database_name)
        container = database.get_container_client(cosmos_container_name)
        
        logger.info("Created client connection")
                
        logger.info("partition_key_query : "+partition_key_query)        
        logger.info("del_sel_query : "+del_sel_query)
            
        pre_part_df = spark.read.format("cosmos.oltp").options(**cfg)\
        .option("spark.cosmos.read.inferSchema.enabled","true").load()
        
        
        if pre_part_df.columns == []:
            partition_keys_list = []
        else:
            part_df = pre_part_df.filter(col("filcol") == 1).select(col(partition_key_column)).distinct()
            partition_keys_list = part_df.collect()
        
        logger.info("In Delete Loop...")
        
        logger.info("No.of partion keys : "+str(len(partition_keys_list)))
        
        for part_key in partition_keys_list:
            while True:
                try:
                    res = container.scripts.execute_stored_procedure(sproc_name, part_key[0], [del_sel_query])
                    global total_del_cnt
                    total_del_cnt = total_del_cnt + res['deleted']
                    logger.debug("Partition Key : "+part_key[0])
                    #logger.info(json.dumps(res, indent=True))
                    if not res['continuation']:
                        break
                except cosmos_exp.CosmosHttpResponseError as e:
                    logger.info("Got CosmosHttpResponseError")
                    audit_result['ERROR_MESSAGE'] = str(e)
                    break
                except Exception as e:
                    raise 
            logger.info("Deleted so far: "+str(total_del_cnt))
        
        audit_result['numTargetRowsDeleted'] = total_del_cnt
        audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
        audit_result['status'] = 'success'
        
        logger.info("Total no.of records deleted: "+str(total_del_cnt))
        
        logger.info("Delete process finished successfully for "+cosmos_container_name)
        
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