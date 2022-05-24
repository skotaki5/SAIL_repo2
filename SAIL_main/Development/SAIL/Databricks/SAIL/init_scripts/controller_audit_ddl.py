# Databricks notebook source
# MAGIC %sql
# MAGIC select * from controller.delta_control

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table delta.`/mnt/sail/logs/controller.db/delta_control` add columns (adjustment_seconds bigint)

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- change to sql server
# MAGIC create database if not exists controller;
# MAGIC 
# MAGIC use controller;
# MAGIC 
# MAGIC drop table if exists delta_control;
# MAGIC 
# MAGIC create external table if not exists delta_control(
# MAGIC     pipeline_id string  
# MAGIC   , delta_schema string  ---gold/silver/cosmos/bronze
# MAGIC   , table_name string
# MAGIC   , hwm string
# MAGIC   , adjustment_seconds bigint
# MAGIC   , insert_ts timestamp
# MAGIC   , insert_date string 
# MAGIC )
# MAGIC 
# MAGIC -- primary key (schema,table_name)
# MAGIC using delta
# MAGIC partitioned by (table_name) -- chnage to table_name
# MAGIC location '/mnt/sail/logs/controller.db/delta_control'; 

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists debug_controller;
# MAGIC 
# MAGIC use debug_controller;
# MAGIC 
# MAGIC drop table if exists delta_control;
# MAGIC 
# MAGIC create external table if not exists delta_control(
# MAGIC      pipeline_id string  
# MAGIC   , delta_schema string  ---gold/silver/cosmos/bronze
# MAGIC   , table_name string
# MAGIC   , hwm string
# MAGIC   , Adjustment_seconds bigint
# MAGIC   , insert_ts timestamp
# MAGIC   , insert_date string 
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (table_name)
# MAGIC location '/mnt/sail/logs/debug_controller.db/delta_control'; 

# COMMAND ----------

# %sql
# insert into controller.delta_control values('1234567566', 'dim_customer', '2019-03-11 00:34:21.000', CURRENT_TIMESTAMP, CURRENT_DATE);

# COMMAND ----------

# MAGIC %sql
# MAGIC tbl_name = 'fact_transportation_rates_charges'
# MAGIC select * from controller.delta_control where table_name = tbl_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from controller.delta_control where table_name like '%cosmos%';

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into controller.delta_control
# MAGIC select pipeline_id   
# MAGIC   , delta_schema 
# MAGIC   , 'cosmos_digital_summary_orders' table_name 
# MAGIC   , hwm 
# MAGIC   , Adjustment_seconds 
# MAGIC   , insert_ts 
# MAGIC   , insert_date   from controller.delta_control where table_name ='cosmos_digital_summary_transportation_callcheck'
# MAGIC   
# MAGIC union 
# MAGIC select pipeline_id   
# MAGIC   , delta_schema 
# MAGIC   , 'cosmos_digital_summary_order_lines' table_name 
# MAGIC   , hwm 
# MAGIC   , Adjustment_seconds 
# MAGIC   , insert_ts 
# MAGIC   , insert_date   from controller.delta_control where table_name ='cosmos_digital_summary_transportation_callcheck'
# MAGIC 
# MAGIC union
# MAGIC 
# MAGIC select pipeline_id   
# MAGIC   , delta_schema 
# MAGIC   , 'cosmos_digital_summary_milestone_activity' table_name 
# MAGIC   , hwm 
# MAGIC   , Adjustment_seconds 
# MAGIC   , insert_ts 
# MAGIC   , insert_date   from controller.delta_control where table_name ='cosmos_digital_summary_transportation_callcheck'

# COMMAND ----------

