-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.conf.set('spark.databricks.delta.optimize.maxFileSize',33554432) #-- 32 mb

-- COMMAND ----------

optimize gold.digital_summary_milestone_activity zorder by (SourceSystemKey,UPSOrderNumber)  

-- COMMAND ----------

optimize gold.digital_summary_transportation_callcheck zorder by (SourceSystemKey,hash_key)  

-- COMMAND ----------

optimize gold.digital_summary_transportation_references zorder by (SourceSystemKey,hash_key)  

-- COMMAND ----------

optimize gold.digital_summary_transportation_rates_charges zorder by (SourceSystemKey,hash_key)  

-- COMMAND ----------

optimize gold.digital_summary_order_tracking zorder by (SourceSystemKey,UPSOrderNumber)  

-- COMMAND ----------

