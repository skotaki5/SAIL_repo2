# Databricks notebook source
# MAGIC %md
# MAGIC <b>Versions</b>       
# MAGIC v0.1  GD000012780@ups.com Prashant Gupta 

# COMMAND ----------

# DBTITLE 1,Importing python libraries
import requests
import json
import time

# COMMAND ----------

# DBTITLE 1,Importing common variables
# MAGIC %run "/SAIL/includes/common_variables"

# COMMAND ----------

# DBTITLE 1,Importing common functions
# MAGIC  %run "/SAIL/includes/common_udfs"

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

headers = {"Authorization": token} 
list_endpoint = base_url + "api/2.0/clusters/list"
logger.debug(f'list_endpoint: {list_endpoint}')
restart_endpoint = base_url + "api/2.0/clusters/restart"
logger.debug(f'restart_endpoint: {restart_endpoint}')
tracking_endpoint = base_url + "api/2.1/jobs/runs/list?active_only=true"
logger.debug(f'tracking_endpoint: {tracking_endpoint}')

cluster_name_list = ['gold_layer_cluster','high_volume_cluster','silver_layer_cluster']
cluster_dict={}

r=requests.get(list_endpoint, headers=headers).json()

for i in r['clusters']:
    if i['default_tags']['ClusterName'] in cluster_name_list:
        cluster_dict[i['default_tags']['ClusterName']] = i['default_tags']['ClusterId']
        
logger.debug(f'cluster_dict: {cluster_dict}')

# COMMAND ----------

def restart_cluster():
    for cluster,val in cluster_dict.items():
        logger.info(f'restarting: {cluster}')
        data = '{ "cluster_id": "' + val + '" }'
        logger.debug(f'data: {data}')
        r=requests.post(restart_endpoint, data=data, headers=headers).json()

# COMMAND ----------

tracking_job_list=['sail_load','optimize_sail_load']
check=1
timeout=0
timeout_threshhold = 2*60*60
while(check and timeout < timeout_threshhold):
    check=0
    r=requests.get(tracking_endpoint, headers=headers).json()
    if 'runs' in r.keys():
        for i in r['runs']:
            if i['run_name'] in tracking_job_list:
                logger.info(f"Running: {i['run_name']}")
                check=1
    if check==1:
        logger.info(f'Sleeping for 60 sec')
        time.sleep(60)
        timeout += 60
        logger.info(f'timeout: {timeout}')
    
logger.info(f'Restarting clusters: {",".join(cluster_name_list)}')
restart_cluster()
        
        

    
