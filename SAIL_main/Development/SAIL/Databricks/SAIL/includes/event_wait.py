# Databricks notebook source
# MAGIC %md
# MAGIC #### author- arpan bhardwaj
# MAGIC Description : this notebook is to put dependency and introduce event waits in the the workflow

# COMMAND ----------

import time
import sys

# COMMAND ----------

# MAGIC %run "/SAIL/includes/common_variables"

# COMMAND ----------

# MAGIC  %run "/SAIL/includes/common_udfs"

# COMMAND ----------

time_zone = 'UTC' # Check for which timezone to be used

# COMMAND ----------

dbutils.widgets.text("log_debug_mode", "","")
dbutils.widgets.get("log_debug_mode")
log_debug_mode = getArgument("log_debug_mode").strip()

if log_debug_mode == "Y":
  logger = _get_logger(time_zone,logging.DEBUG)
else:
  logger = _get_logger(time_zone,logging.INFO)
  

# COMMAND ----------

dbutils.widgets.text("checkpoint", "","")
dbutils.widgets.get("checkpoint")
checkpoint = getArgument("checkpoint")

dbutils.widgets.text("wait_time", "","")
dbutils.widgets.get("wait_time")
wait_time = getArgument("wait_time")

dbutils.widgets.text("kill", "","")
dbutils.widgets.get("kill")
kill = getArgument("kill").upper()


# COMMAND ----------

if kill=='TRUE':
    sys.exit('custom termination successfull')
try :
    
    wait_time_int=int(wait_time)
    logger.info(f"waiting time of {wait_time_int} seconds")
    time.sleep(wait_time_int) 
except:
    print("no wait")
finally:
    logger.info("checkpoint: " + checkpoint+" finished successfully")
    