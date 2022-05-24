# Databricks notebook source
# MAGIC %md
# MAGIC <b>Author </b>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;: GD000012020 Shrey Jain </br>
# MAGIC <b>Description</b>  : this notebook is to load summary_order_lines_details table. </br>
# MAGIC <b>version 1.1</b>  : Added new column receipt_number as per change log story. </br>

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

def get_delta_query(hwm):
  logger.debug("hwm: " + str(hwm))
  query ="""
        create or replace temp view delta_fetch_tv 
        as
          select ups_order_number from {fact_order_dim_inc} where dl_update_timestamp>='{hwm}'
        union
          select ups_order_number from {fact_order_line_details}  where dl_update_timestamp>='{hwm}'
        union
          select ups_order_number 
                 FROM {fact_order_line_details} FOLD
                 INNER JOIN {dim_item} IM ON (FOLD.ITEM_KEY = IM.ITEM_KEY AND FOLD.SOURCE_SYSTEM_KEY = IM.SOURCE_SYSTEM_KEY)  where IM.dl_update_timestamp>='{hwm}'
        """.format(**source_tables,hwm=hwm)
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

# DBTITLE 1,Query
def get_query(hwm):
    query = """
       SELECT
        CAST(Null as VARCHAR(10)) as AccountId,
        CAST(Null as VARCHAR(10)) as FacilityId,
        CAST(Null as VARCHAR(10)) as DP_SERVICELINE_KEY,
        CAST(Null as VARCHAR(10)) as DP_ORGENTITY_KEY,
        FOLD.UPS_ORDER_NUMBER UPSOrderNumber,
        FOLD.UPS_ORDER_LINE_NUMBER LineNumber,
        FOLD.UPS_ORDER_LINE_DETAIL_NUMBER LineDetailNumber,
        FOLD.VENDOR_SERIAL_NUMBER VendorSerialNumber,
        FOLD.VENDOR_LOT_NUMBER VendorLotNumber,
        FOLD.LPN_NUMBER LPNNumber,
        FOLD.DISPOSITION_VALUE DispositionValue,
        FOLD.SOURCE_SYSTEM_KEY SourceSystemKey,
        FOLD.WAREHOUSE_KEY WarehouseKey,
        FOLD.ITEM_KEY ItemKey,
        ITM.PART_NUMBER ItemNumber,
        FOLD.EXPIRATION_DATE EXPIRATION_DATE,
        CASE WHEN FTO.SOURCE_SYSTEM_NAME LIKE '%SOFTEON%' THEN FTO.BUILDING_CODE ELSE FTO.WAREHOUSE_CODE END WAREHOUSE_CODE,
        concat(FTO.source_system_key,'||',FTO.order_sduk) as order_sduk,
        concat(FOLD.source_system_key,'||',FOLD.ORDER_LINE_DETAILS_SDUK) as ORDER_LINE_DETAILS_SDUK,
        FOLD.is_deleted,
        FOLD.IS_INBOUND,
        FOLD.RECEIPT_NUMBER as ReceiptNumber -- version 1.1
        FROM {fact_order_dim_inc}  FTO
        INNER JOIN delta_fetch_tv FTV on (FTO.UPS_ORDER_NUMBER = FTV.UPS_ORDER_NUMBER)
        INNER JOIN {fact_order_line_details} FOLD
                    ON FTO.UPS_ORDER_NUMBER=FOLD.UPS_ORDER_NUMBER
                    AND FTO.SOURCE_SYSTEM_KEY=FOLD.SOURCE_SYSTEM_KEY
        LEFT JOIN {dim_item}  ITM      ON FOLD.ITEM_KEY=ITM.ITEM_KEY
                                       AND FOLD.SOURCE_SYSTEM_KEY=ITM.SOURCE_SYSTEM_KEY
       where FTO.ORDER_PLACED_DATE BETWEEN date_sub('{hwm}', {days_back}) AND current_timestamp
    """.format(**source_tables,hwm=hwm,days_back=days_back)
    logger.debug("query : " + query)

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
        audit_result['process_name'] = 'pr_load_summary_order_lines_details'
        audit_result['process_type'] = 'DataBricks'
        audit_result['layer'] = 'gold'
        audit_result['table_name'] = digital_summary_order_lines_details_et
        audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
        audit_result['start_time'] = start_time
        ################################################################################################
        hwm=get_hwm('gold',digital_summary_order_lines_details_et)
        logger.info(f'hwm {digital_summary_order_lines_details_et}: {hwm}')
  
        spark.sql(get_delta_query(hwm))
        logger.info("get_delta_query finished")     
        
        src_df = spark.sql(get_query(hwm))
        logger.info("query finished") 
        ###################### generating hash key  #############################
        hash_key_columns = ['order_sduk','ORDER_LINE_DETAILS_SDUK']
        logger.info(f"hash key columns: {hash_key_columns}")
  
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
  
        logger.info("Adding dl_hash")
        src_df = src_df.withColumn("dl_hash", sha1_concat(hash_col))
  
        ##################### audit columns  ######################################
        logger.info("Adding audit columns")
        src_df = add_audit_columns(src_df, pid,datetime.now(),datetime.now())
  
        primary_keys = ['SourceSystemKey','order_sduk','ORDER_LINE_DETAILS_SDUK']
        logger.debug('primary_keys: {primary_keys}'.format(primary_keys=primary_keys))
  
        logger.info(f'Merging to delta path: {digital_summary_order_lines_details_path}')
  
        mergeToDelta(src_df,digital_summary_order_lines_details_path,primary_keys)
        logger.info(f'merging to delta path finished: {digital_summary_order_lines_details_path}')
  
        logger.info('setting hwm')
        res=set_hwm('gold',digital_summary_order_lines_details_et,start_time,pid)
        logger.info(res)
        ############################ ETL AUDIT ########################################################
        audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
        audit_result['status'] = 'success'
                
        logger.info('Process finished successfully.')
        #Writing into Delta format and saving into Gold Layer Table
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