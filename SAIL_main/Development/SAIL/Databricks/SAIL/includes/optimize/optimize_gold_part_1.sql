-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.conf.set('spark.databricks.delta.optimize.maxFileSize',33554432) #-- 32 mb

-- COMMAND ----------

optimize gold.fact_order_dim_inc zorder by (SOURCE_SYSTEM_KEY, order_sduk, ups_order_number)

-- COMMAND ----------

optimize gold.digital_summary_order_lines zorder by (SourceSystemKey,UPSOrderNumber)  

-- COMMAND ----------

optimize gold.digital_summary_milestone zorder by (order_sduk)  

-- COMMAND ----------

optimize gold.digital_summary_transport_details zorder by (SOURCE_SYSTEM_KEY,hash_key) 

-- COMMAND ----------

