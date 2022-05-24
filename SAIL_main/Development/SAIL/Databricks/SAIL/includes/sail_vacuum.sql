-- Databricks notebook source
-- MAGIC %run "/SAIL/includes/common_variables"

-- COMMAND ----------

-- MAGIC  %run "/SAIL/includes/common_udfs"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("log_debug_mode", "","")
-- MAGIC dbutils.widgets.get("log_debug_mode")
-- MAGIC log_debug_mode = getArgument("log_debug_mode").strip()
-- MAGIC 
-- MAGIC if log_debug_mode == "Y":
-- MAGIC   logger = _get_logger(time_zone,logging.DEBUG)
-- MAGIC else:
-- MAGIC   logger = _get_logger(time_zone,logging.INFO)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def run_vacuum():
-- MAGIC     for key,val in source_tables.items():
-- MAGIC         logger.info(f'Vacuum started on {val}')
-- MAGIC         query="""Vacuum {table} retain {days_back_hours} HOURS""".format(days_back_hours=days_back_Vacuum*24,table=val)
-- MAGIC         logger.info(f'query : {query}')
-- MAGIC         spark.sql(query)
-- MAGIC         logger.info(f'Vacuum done on {val}')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC run_vacuum()

-- COMMAND ----------

