# Databricks notebook source
# MAGIC %md
# MAGIC <b>Author </b>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;: GD000012734 Mahesh Rathi </br>
# MAGIC <b>Description</b>  : this notebook is to load summary_transportation_details table.

# COMMAND ----------

import datetime,time,json
from pytz import timezone
from  pyspark.sql.functions import input_file_name ,split , size , from_utc_timestamp, lit , concat, when, col, sha1, row_number

# COMMAND ----------

# DBTITLE 1,Import Common Variables
# MAGIC %run "/SAIL/includes/common_variables"

# COMMAND ----------

# DBTITLE 1,Import Common Utilities
# MAGIC %run "/SAIL/includes/common_udfs"

# COMMAND ----------

start_time = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# DBTITLE 1,Set debug mode
dbutils.widgets.text("log_debug_mode", "","")
dbutils.widgets.get("log_debug_mode")
log_debug_mode = getArgument("log_debug_mode").strip()

if log_debug_mode == "Y":
  logger = _get_logger(time_zone,logging.DEBUG)  
else:
  logger = _get_logger(time_zone,logging.INFO)
  

# COMMAND ----------

# DBTITLE 1,delta_query
def get_delta_query(hwm):
  logger.debug("hwm: " + str(hwm))
  query ="""
        create or replace temp view delta_fetch_tv 
        as
         select ups_order_number from {fact_order_dim_inc} where dl_update_timestamp>='{hwm}'
        union 
          select ups_order_number as ups_order_number  from {fact_transport_details} FT
          where dl_update_timestamp>='{hwm}'
        """.format(**source_tables,hwm=hwm)
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

# DBTITLE 1,Query
def get_query():
  query = """
    create
    or replace temp view digital_summary_transport_details_stg as
    SELECT
      FO.UPS_ORDER_NUMBER as UPSORDERNUMBER,
      FO.SOURCE_SYSTEM_KEY as SOURCE_SYSTEM_KEY,
      CL.GLD_ACCOUNT_MAPPED_KEY as Account_ID,
      CL.DP_SERVICELINE_KEY as DP_SERVICELINE_KEY,
      CL.DP_ORGENTITY_KEY as DP_ORGENTITY_KEY,
      ITEM_DESCRIPTION as ITEM_DESCRIPTION,
      ACTUAL_QTY,
      ACTUAL_UOM,
      ACTUAL_WGT,
      REPLACE(ITEM_DIMENSION, 'x', '*') AS ITEM_DIMENSION,
      TemperatureRange_Min as TempRangeMin,
      TemperatureRange_Max as TempRangeMax,
      TemperatureRange_UOM as TempRangeUOM,
      TemperatureRange_Code as TempRangeCode,
      Planned_Weight_UOM as PlannedWeightUOM,
      Actual_Weight_UOM as ActualWeightUOM,
      Dimension_UOM as DimensionUOM
    FROM
      {fact_transport_details} FT
      INNER JOIN {fact_order_dim_inc} FO ON FO.UPS_ORDER_NUMBER = FT.UPS_ORDER_NUMBER
      AND FO.SOURCE_SYSTEM_KEY = FT.SOURCE_SYSTEM_KEY
      INNER JOIN delta_fetch_tv FTV on (FO.UPS_ORDER_NUMBER = FTV.UPS_ORDER_NUMBER)
      INNER JOIN {dim_customer} CL ON (
        FO.CLIENT_KEY = CL.CUSTOMERKEY
        AND FO.SOURCE_SYSTEM_KEY = CL.SOURCE_SYSTEM_KEY
      )
    GROUP BY
      ITEM_DESCRIPTION,
      ACTUAL_QTY,
      ACTUAL_UOM,
      ACTUAL_WGT,
      ITEM_DIMENSION,
      FO.UPS_ORDER_NUMBER,
      CL.GLD_ACCOUNT_MAPPED_KEY,
      CL.DP_SERVICELINE_KEY,
      CL.DP_ORGENTITY_KEY,
      FO.SOURCE_SYSTEM_KEY,
      TemperatureRange_Min,
      TemperatureRange_Max,
      TemperatureRange_UOM,
      TemperatureRange_Code,
      Planned_Weight_UOM,
      Actual_Weight_UOM,
      Dimension_UOM""".format(**source_tables)
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

# MAGIC %sql 

# COMMAND ----------

# DBTITLE 1,soft_delete_query
def soft_delete_query():
  query="""
     with temp as 
     (
     select distinct upsordernumber from digital_summary_transport_details_stg
     )
     update  {digital_summary_transport_details} t
     set
      t.is_deleted = 1, t.dl_update_timestamp = current_timestamp , t.dl_hash='NULL'
     where
     exists ( select  1  from temp TV where  t.upsordernumber = TV.upsordernumber
            )
      """.format(**source_tables)
  logger.debug("query :"+query)
  return (query)


# COMMAND ----------

# DBTITLE 1,Main Function
def main():
    logger.info("Main function is running")
    try:
        pid_get = get_pid()
        logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
        pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
        logger.info("pid: {pid}".format(pid=pid))
        ############################ ETL AUDIT #########################################################
        audit_result['process_id'] = pid
        audit_result['process_name'] = 'pr_load_summary_transportation_details'
        audit_result['process_type'] = 'DataBricks'
        audit_result['layer'] = 'gold'
        audit_result['table_name'] = digital_summary_transport_details_et
        audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
        audit_result['start_time'] = start_time
        ################################################################################################
        hwm=get_hwm('gold',digital_summary_transport_details_et)
        logger.info(f'hwm {digital_summary_transport_details_et}: {hwm}')
        
        spark.sql(get_delta_query(hwm))
        logger.info("get_delta_query finished")
        
        spark.sql(get_query())
        spark.sql("cache table digital_summary_transport_details_stg")
        df = spark.sql("select * from digital_summary_transport_details_stg")
              
        src_df=df.withColumn("is_deleted",lit(0))
        logger.info("get_query() finished")
        
        spark.sql(soft_delete_query())
        logger.info(f"soft delete operation on {digital_summary_transport_details_et} finished")
        
        ###################### generating hash key  #############################
        hash_key_columns = ['UPSORDERNUMBER','SOURCE_SYSTEM_KEY','Account_ID','DP_SERVICELINE_KEY','DP_ORGENTITY_KEY','ITEM_DESCRIPTION','ACTUAL_QTY','ACTUAL_UOM','ACTUAL_WGT','ITEM_DIMENSION','TempRangeMin',
                            'TempRangeMin','TempRangeUOM','TempRangeCode','PlannedWeightUOM','ActualWeightUOM','DimensionUOM' ]
        logger.debug(f"columns: {hash_key_columns}")

        logger.debug("Adding hash_key")
        src_df = src_df.withColumn("hash_key", sha1_concat(hash_key_columns))
        ##################### DL hash  ######################################
        logger.debug("Generating dl_hash")
        columns = src_df.schema.fieldNames()
        logger.debug("columns: {columns}".format(columns=columns))
        hash_exclude_col = []
        logger.debug("hash_exclude_col: {hash_exclude_col}".format(hash_exclude_col=hash_exclude_col))
        hash_col = subtract_list(columns,hash_exclude_col)
        logger.debug("hash_col: {hash_col}".format(hash_col=hash_col))

        logger.debug("Adding hash")
        src_df = src_df.withColumn("dl_hash", sha1_concat(hash_col))
        ##################### audit columns  ######################################
        logger.debug("Adding audit columns")
        src_df = add_audit_columns(src_df, pid,datetime.now(),datetime.now())

        primary_keys = ['hash_key']
        logger.debug('primary_keys: {primary_keys}'.format(primary_keys=primary_keys))

        logger.info(f'Merging to delta path: {digital_summary_transport_details_path}')

        mergeToDelta(src_df,digital_summary_transport_details_path,primary_keys)
        logger.info(f'merging to delta path finished: {digital_summary_transport_details_path}')
        
        spark.sql("uncache  table if exists digital_summary_transport_details_stg")
        
        logger.info('setting hwm')
        res=set_hwm('gold',digital_summary_transport_details_et,start_time,pid)
        logger.info(res)
        ############################ ETL AUDIT #########################################################
        audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
        audit_result['status'] = 'success'
                
        logger.info('Process finished successfully.')
        
    except Exception as e:
        audit_result['status'] = 'failed'
        audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
        audit_result['ERROR_MESSAGE'] = str(e)
        raise
    finally:
        logger.info("audit_result: {audit_result}".format(audit_result=audit_result))
        audit(audit_result)


# COMMAND ----------

main()