-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.conf.set('spark.databricks.delta.optimize.maxFileSize',33554432) #-- 32 mb

-- COMMAND ----------

optimize silver.dim_customer zorder by (SOURCE_SYSTEM_KEY, CUSTOMERKEY)

-- COMMAND ----------

optimize silver.dim_warehouse zorder by (SOURCE_SYSTEM_KEY, WAREHOUSE_KEY)

-- COMMAND ----------

OPTIMIZE silver.dim_item ZORDER by (SOURCE_SYSTEM_KEY,ITEM_KEY)

-- COMMAND ----------

optimize silver.dim_carrier_los zorder by (SOURCE_SYSTEM_KEY,CARRIER_LOS_KEY)

-- COMMAND ----------

optimize silver.dim_service zorder by (SOURCE_SYSTEM_KEY,SERVICE_KEY)

-- COMMAND ----------

OPTIMIZE silver.dim_geo_location ZORDER by (SOURCE_SYSTEM_KEY,GEO_LOCATION_KEY)