# Databricks notebook source
# MAGIC %sql 
# MAGIC select array_contains(transform(split(Mapped_Warehouse_code,','),x -> trim(x)), TRIM(CASE WHEN SS.SOURCE_SYSTEM_NAME LIKE '%SOFTEON%' THEN W.BUILDING_CODE ELSE W.WAREHOUSE_CODE END)) from silver.DIM_CUSTOMER C where GLD_ACCOUNT_MAPPED_KEY = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select array_contains(transform(split(C.Mapped_Warehouse_code,','),x -> trim(x)), TRIM(CASE WHEN SS.SOURCE_SYSTEM_NAME LIKE '%SOFTEON%' THEN W.BUILDING_CODE ELSE W.WAREHOUSE_CODE END FROM 

# COMMAND ----------

# MAGIC %bash
# MAGIC databricks workspace -h

# COMMAND ----------

pip install databricks-cli

# COMMAND ----------

