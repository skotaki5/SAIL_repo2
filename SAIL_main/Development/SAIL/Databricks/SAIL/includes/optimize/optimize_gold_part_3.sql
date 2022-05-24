-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.conf.set('spark.databricks.delta.optimize.maxFileSize',33554432) #-- 32 mb

-- COMMAND ----------

optimize gold.digital_summary_orders zorder by (SourceSystemKey,order_sduk,UPSOrderNumber)  -- change primary key gold notebook

-- COMMAND ----------

optimize gold.digital_summary_transportation zorder by (SourceSystemKey,UPSOrderNumber)  

-- COMMAND ----------

optimize gold.digital_summary_order_lines_details zorder by (SourceSystemKey,order_sduk)  

-- COMMAND ----------

optimize gold.digital_summary_exceptions zorder by (SourceSystemKey,TRANSPORTATION_EXCEPTION_SDUK)  

-- COMMAND ----------

optimize gold.digital_summary_inbound_line zorder by (SourceSystemKey,hash_key) 

-- COMMAND ----------

