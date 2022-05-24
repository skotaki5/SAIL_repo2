-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.conf.set('spark.databricks.delta.optimize.maxFileSize',33554432) #-- 32 mb

-- COMMAND ----------

OPTIMIZE silver.fact_transportation ZORDER by (SOURCE_SYSTEM_KEY,TRANSPORTATION_SDUK)

-- COMMAND ----------

OPTIMIZE silver.fact_transportation_exception ZORDER by (SOURCE_SYSTEM_KEY,TRANSPORTATION_EXCEPTION_SDUK)

-- COMMAND ----------

OPTIMIZE silver.fact_order_reference ZORDER by (SOURCE_SYSTEM_KEY, ORDER_REFERENCE_SDUK, QUERY_SEQUENCE)



-- COMMAND ----------

OPTIMIZE silver.fact_order_line ZORDER by (SOURCE_SYSTEM_KEY, ORDER_LINE_SDUK)


-- COMMAND ----------

OPTIMIZE silver.fact_inbound_line ZORDER by (SOURCE_SYSTEM_KEY, INBOUND_LINE_SDUK)

-- COMMAND ----------

OPTIMIZE silver.fact_transportation_callcheck ZORDER by (SOURCE_SYSTEM_KEY, callcheck_sduk)


-- COMMAND ----------

OPTIMIZE silver.fact_transportation_rates_charges ZORDER by (SOURCE_SYSTEM_KEY,CHARGE_SDUK)

-- COMMAND ----------

OPTIMIZE silver.fact_transportation_references ZORDER by (SOURCE_SYSTEM_KEY, REFERENCE_SDUK)


-- COMMAND ----------

OPTIMIZE silver.fact_transport_details ZORDER by (SOURCE_SYSTEM_KEY, SHIPMENT_SDUK)