# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <b>Author--GD000012733@ups.com</b>
# MAGIC <b>Name:Vishal</b>

# COMMAND ----------

import datetime,time,json
from pytz import timezone
from  pyspark.sql.functions import input_file_name ,split , size , from_utc_timestamp, lit , concat, when, col, sha1, row_number

# COMMAND ----------

# DBTITLE 1,Importing common variables
# MAGIC %run "/SAIL/includes/common_variables"

# COMMAND ----------

# DBTITLE 1,Importing common udfs
# MAGIC %run "/SAIL/includes/common_udfs"

# COMMAND ----------

start_time = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# DBTITLE 1,Declaring Temporary tables
var_temp_fact_order='TMP_FACT_ORDER_tv'

# COMMAND ----------

# DBTITLE 1,Setting debug mode
dbutils.widgets.text("log_debug_mode", "","")
dbutils.widgets.get("log_debug_mode")
log_debug_mode = getArgument("log_debug_mode").strip()

if log_debug_mode == "Y":
  logger = _get_logger('US/Eastern',logging.DEBUG)  #UTC timezone
else:
  logger = _get_logger('US/Eastern',logging.INFO)



# COMMAND ----------

# DBTITLE 1,delta_query
def get_delta_query(hwm):
  logger.debug("hwm: " + str(hwm))
  query ="""
        create or replace temp view delta_fetch_tv 
        as
          select ups_order_number from {fact_order_dim_inc} where dl_update_timestamp>='{hwm}'
          and ORDER_PLACED_DATE BETWEEN date_sub('{hwm}', {days_back}) AND current_timestamp
        union
          select ups_order_number from {fact_order_line}  where dl_update_timestamp>='{hwm}'
        union
          select ups_order_number 
                 FROM {fact_order_line} FTOL
                 INNER JOIN {dim_item} IM ON (FTOL.ITEM_KEY = IM.ITEM_KEY AND FTOL.SOURCE_SYSTEM_KEY = IM.SOURCE_SYSTEM_KEY)  where IM.dl_update_timestamp>='{hwm}'
        """.format(**source_tables,hwm=hwm,days_back=days_back)
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

# DBTITLE 1,Source query
def get_query(hwm):
  query=("""SELECT 
           FTO.GLD_ACCOUNT_MAPPED_KEY as AccountId
          ,FTO.FacilityId as FacilityId
          ,CAST(Null as VARCHAR(10)) as DP_SERVICELINE_KEY
          ,CAST(Null as VARCHAR(10)) as DP_ORGENTITY_KEY
		  ,FTO.UPS_ORDER_NUMBER as UPSOrderNumber
          ,FTO.CUSTOMER_ORDER_NUMBER as OrderNumber   
          ,FTOL.UPS_ORDER_LINE_NUMBER as LineNUmber    
          ,IM.PART_NUMBER as SKU 
          ,IM.PART_DESCRIPTION as SKUDescription 
          ,concat(CAST(IM.ITEM_LENGTH as VARCHAR(10)),'*',CAST(IM.ITEM_WIDTH as VARCHAR(10)),'*',CAST(IM.ITEM_HEIGHT as VARCHAR(10) )) as SKUDimensions
          ,IM.ITEM_WEIGHT as SKUWeight
          ,FTOL.ORDER_LINE_QUANTITY as SKUQuantity 
          ,FTOL.SHIPPED_QUANTITY  as SKUShippedQuantity 
		  ,FTO.CarrierCode as CarrierCode 
		  ,'' AS TrackingNo
          ,FTO.SOURCE_SYSTEM_KEY as SourceSystemKey
          ,IM.ITEM_DIMENSIONS_UOM as SKUDimensions_UOM 
          ,IM.ITEM_WEIGHT_UOM as SKUWeight_UOM 
		  ,FTOL.OL_CANCELLED_DATE as ShipmentLineCanceledDate
		  ,FTOL.CANCEL_REASON as ShipmentLineCanceledReason
		  ,FTOL.ORDER_LINE_CANCELLED_FLAG as ShipmentLineCanceledFlag
		  ,FTOL.UPS_ORDER_LINE_REF_VALUE_1 as LineRefVal1
		  ,FTOL.UPS_ORDER_LINE_REF_VALUE_2 as LineRefVal2
		  ,FTOL.UPS_ORDER_LINE_REF_VALUE_3 as LineRefVal3
		  ,FTOL.UPS_ORDER_LINE_REF_VALUE_4 as LineRefVal4
		  ,FTOL.UPS_ORDER_LINE_REF_VALUE_5 as LineRefVal5
          ,concat(FTO.source_system_key,'||',FTO.order_sduk) as order_sduk
          ,concat(FTOL.source_system_key,'||',FTOL.ORDER_LINE_SDUK) as ORDER_LINE_SDUK
          ,FTOL.UTC_ORDER_PLACED_MONTH_PART_KEY
          ,FTOL.is_deleted
      FROM {fact_order_dim_inc} FTO  
      INNER JOIN delta_fetch_tv FTV on (FTO.UPS_ORDER_NUMBER = FTV.UPS_ORDER_NUMBER)
      INNER JOIN {fact_order_line} FTOL  ON (FTOL.SOURCE_SYSTEM_KEY = FTO.SOURCE_SYSTEM_KEY AND FTOL.UPS_ORDER_NUMBER = FTO.UPS_ORDER_NUMBER)     
      INNER JOIN {dim_item} IM ON (FTOL.ITEM_KEY = IM.ITEM_KEY AND FTOL.SOURCE_SYSTEM_KEY = IM.SOURCE_SYSTEM_KEY)  
      where FTO.ORDER_PLACED_DATE BETWEEN date_sub('{hwm}', {days_back}) AND current_timestamp
      """.format(**source_tables,hwm=hwm,days_back=days_back))
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

# DBTITLE 1,Main Function
def main():
    logger.info('Main function is running')
  
    try:
        pid_get = get_pid()
        logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
        pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
        logger.info("pid: {pid}".format(pid=pid))
        ############################ ETL AUDIT #########################################################
        audit_result['process_id'] = pid
        audit_result['process_name'] = 'pr_load_summary_order_lines'
        audit_result['process_type'] = 'DataBricks'
        audit_result['layer'] = 'gold'
        audit_result['table_name'] = digital_summary_order_lines_et
        audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
        audit_result['start_time'] = start_time
        ################################################################################################
        hwm=get_hwm('gold',digital_summary_order_lines_et)
        logger.info(f'hwm {digital_summary_order_lines_et}: {hwm}')
        
        spark.sql(get_delta_query(hwm))
        logger.info("get_delta_query finished")
        
        
        logger.info('Getting source query')
        source_query = get_query(hwm)
        
        logger.info('Reading source data...')
        src_df = spark.sql(source_query)
        
        ###################### generating hash key  #############################
        hash_key_columns = ['AccountId','FacilityId','UPSOrderNumber','order_sduk','order_line_sduk']
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
    
        primary_keys = ['SourceSystemKey','UPSOrderNumber','order_sduk','order_line_sduk','AccountId','FacilityId']
        logger.debug('primary_keys: {primary_keys}'.format(primary_keys=primary_keys))
    
        logger.info(f'Merging to delta path: {digital_summary_order_lines_path}')
    
        mergeToDelta(src_df,digital_summary_order_lines_path,primary_keys)
        logger.info(f'merging to delta path finished: {digital_summary_order_lines_path}')
    
        logger.info('setting hwm')
        res=set_hwm('gold',digital_summary_order_lines_et,start_time,pid)
        logger.info(res)
        
        ############################ ETL AUDIT #########################################################
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