-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.conf.set('spark.databricks.delta.optimize.maxFileSize',33554432) #-- 32 mb

-- COMMAND ----------

OPTIMIZE silver.fact_milestone_activity ZORDER by (SOURCE_SYSTEM_KEY,LOAD_TRACK_SDUK)

-- COMMAND ----------

OPTIMIZE silver.fact_order ZORDER by (SOURCE_SYSTEM_KEY,order_sduk)

-- COMMAND ----------

OPTIMIZE silver.fact_shipment ZORDER by (SOURCE_SYSTEM_KEY, SHIPMENT_SDUK)


-- COMMAND ----------

OPTIMIZE silver.fact_order_line_details ZORDER by (SOURCE_SYSTEM_KEY, ORDER_LINE_DETAILS_SDUK)



-- COMMAND ----------

