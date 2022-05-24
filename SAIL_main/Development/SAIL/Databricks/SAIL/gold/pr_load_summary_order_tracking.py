# Databricks notebook source
# MAGIC %md
# MAGIC <b>Author </b>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;: GD000012020 Shrey Jain </br>
# MAGIC <b>Description</b>  : this notebook is to load summary_order_tracking table.

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
         select ups_order_number from {fact_order_dim_inc} FTO where dl_update_timestamp>='{hwm}' 
          and FTO.ORDER_PLACED_DATE BETWEEN date_sub('{hwm}', {days_back}) AND current_timestamp
         union
         select  case when NVL(FTTR.TRANS_ONLY_FLAG,'NULL') = 'NON_TRANS' and UPS_WMS_ORDER_NUMBER is not null 
                         then  UPS_WMS_ORDER_NUMBER
                         else UPS_ORDER_NUMBER 
                 end as ups_order_number  
          from {fact_transportation} FTTR  where  dl_update_timestamp>='{hwm}'
        union
          select ups_order_number from {fact_shipment}  where dl_update_timestamp>='{hwm}' 
       
        """.format(**source_tables,hwm=hwm,days_back=days_back)
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

# DBTITLE 1,Query
def get_query(hwm):
  query = """
    with fact_order_cte as
     (
     SELECT   
     FTO.GLD_ACCOUNT_MAPPED_KEY AccountId,   
     FTO.DP_SERVICELINE_KEY,
     FTO.DP_ORGENTITY_KEY,
     FTO.FacilityId FacilityId,  
     FTO.UPS_ORDER_NUMBER UPSOrderNumber,  
     FTTR.UPS_ORDER_NUMBER AS UPS_TRANSPORT_ORDER_NUMBER,  
     FTTR.SOURCE_SYSTEM_KEY  AS UPS_TRANSPORT_SOURCE_SYSTEM_KEY,  
     FTO.SOURCE_SYSTEM_KEY,   
     CASE WHEN FTO.SERVICE_NAME_SR = 'Customer Pickup' THEN FTO.SERVICE_NAME_SR WHEN FTO.SERVICE_NAME_SR = FTO.SERVICE_NAME_LC THEN FTO.CARRIERNAME_LC ELSE         FTO.CarrierCode END CarrierCode,         
     FTO.CARRIER_GROUP,
     FTTR.CARRIER_MODE,
     FTO.IS_MANAGED,
     FTO.IS_INBOUND,
     concat(FTO.source_system_key,'||',FTO.order_sduk) as order_sduk,
     concat(FTTR.source_system_key,'||',FTTR.TRANSPORTATION_SDUK) as TRANSPORTATION_SDUK,
     0 as is_deleted
    FROM {fact_order_dim_inc}  FTO  
      INNER JOIN delta_fetch_tv FTV on (FTO.UPS_ORDER_NUMBER = FTV.UPS_ORDER_NUMBER)
      LEFT JOIN {fact_transportation} FTTR  ON   
       (CASE WHEN FTTR.TRANS_ONLY_FLAG <> 'TRANS ONLY' THEN   NVL(FTTR.UPS_WMS_SOURCE_SYSTEM_KEY,FTO.SOURCE_SYSTEM_KEY) ELSE FTTR.SOURCE_SYSTEM_KEY END = FTO.SOURCE_SYSTEM_KEY   
       AND CASE WHEN FTTR.TRANS_ONLY_FLAG <> 'TRANS ONLY' THEN FTTR.UPS_WMS_ORDER_NUMBER ELSE FTTR.UPS_ORDER_NUMBER END = FTO.UPS_ORDER_NUMBER) 
    WHERE FTO.ORDER_PLACED_DATE BETWEEN date_sub('{hwm}', {days_back}) AND current_timestamp
    )
    ,
    temp as 
    (
     SELECT   
          AccountId,
          FacilityId,
          FTO.DP_SERVICELINE_KEY,
          FTO.DP_ORGENTITY_KEY,          
          UPSOrderNumber,  
          FTO.SOURCE_SYSTEM_KEY SourceSystemKey,  
          concat_ws('*',CAST(FS.SHIPMENT_LENGTH AS VARCHAR(1000)) , CAST(FS.SHIPMENT_WIDTH AS VARCHAR(1000)) , CAST(FS.SHIPMENT_HEIGHT AS VARCHAR(1000))) ShipmentDimensions,  
          CAST(FS.SHIPMENT_WEIGHT AS VARCHAR(1000)) ShipmentWeight,  
          CarrierCode,   
          FS.TRACKING_NUMBER,  
          NVL(CARRIER_MODE,FS.CARRIER_TYPE) CarrierType,
          FS.Dimension_UOM ShipmentDimensions_UOM,
          FS.Actual_Weight_UOM ShipmentWeight_UOM,
          FS.TemperatureRange_Min,
          FS.TemperatureRange_Max,
          FS.TemperatureRange_UOM,
          FS.TemperatureRange_Code,
          FS.SHIPMENT_QUANTITY,
          FS.SHIPMENT_DESCRIPTION,
          FS.LOAD_AREA,
          FS.UOM,
          FTO.order_sduk,
          FTO.TRANSPORTATION_SDUK,
          FS.UTC_SHIPMENT_CREATION_MONTH_PART_KEY,
          concat(FS.SOURCE_SYSTEM_KEY,'||',FS.SHIPMENT_SDUK) as SHIPMENT_SDUK,
          FTO.is_deleted
          FROM fact_order_cte  FTO   
         INNER JOIN {fact_shipment}  FS ON (FTO.SOURCE_SYSTEM_KEY = FS.SOURCE_SYSTEM_KEY AND FTO.UPSOrderNumber = FS.UPS_ORDER_NUMBER)  
       WHERE COALESCE(CARRIER_MODE,CARRIER_GROUP,'') NOT IN  ('LTL','TL')  OR (FTO.IS_INBOUND = 0 AND FTO.IS_MANAGED = 0) OR FTO.UPS_TRANSPORT_ORDER_NUMBER IS NULL

       UNION  
       SELECT   
          AccountId,
          FacilityId,
          FTO.DP_SERVICELINE_KEY,
          FTO.DP_ORGENTITY_KEY,         
          UPSOrderNumber,  
          FTO.SOURCE_SYSTEM_KEY SourceSystemKey,  
          concat_ws('*',CAST(FS.SHIPMENT_LENGTH AS VARCHAR(1000)) , CAST(FS.SHIPMENT_WIDTH AS VARCHAR(1000)) , CAST(FS.SHIPMENT_HEIGHT AS VARCHAR(1000))) ShipmentDimensions,  
          CAST(FS.SHIPMENT_WEIGHT AS VARCHAR(1000)) ShipmentWeight,  
          CarrierCode,   
          FS.TRACKING_NUMBER,  
          NVL(CARRIER_MODE,FS.CARRIER_TYPE) CarrierType,
          FS.Dimension_UOM ShipmentDimensions_UOM,
          FS.Actual_Weight_UOM ShipmentWeight_UOM,
          FS.TemperatureRange_Min,
          FS.TemperatureRange_Max,
          FS.TemperatureRange_UOM,
          FS.TemperatureRange_Code,
          FS.SHIPMENT_QUANTITY,
          FS.SHIPMENT_DESCRIPTION,
          FS.LOAD_AREA,
          FS.UOM,
          FTO.order_sduk,
          FTO.TRANSPORTATION_SDUK,
          FS.UTC_SHIPMENT_CREATION_MONTH_PART_KEY,
          concat(FS.SOURCE_SYSTEM_KEY,'||',FS.SHIPMENT_SDUK) as SHIPMENT_SDUK,
          FTO.is_deleted
          FROM fact_order_cte  FTO   
         INNER JOIN {fact_shipment}  FS ON (FTO.UPS_TRANSPORT_SOURCE_SYSTEM_KEY = FS.SOURCE_SYSTEM_KEY AND FTO.UPS_TRANSPORT_ORDER_NUMBER = FS.UPS_ORDER_NUMBER)  
       WHERE NVL(CARRIER_MODE,CARRIER_GROUP) IN ('LTL','TL')
   )
   select 
          AccountId,
          FacilityId,
          DP_SERVICELINE_KEY,
          DP_ORGENTITY_KEY,          
          UPSOrderNumber,  
          SourceSystemKey,  
          ShipmentDimensions,  
          ShipmentWeight,  
          CarrierCode,   
          TRACKING_NUMBER,  
          CarrierType,
          ShipmentDimensions_UOM,
          ShipmentWeight_UOM,
          TemperatureRange_Min,
          TemperatureRange_Max,
          TemperatureRange_UOM,
          TemperatureRange_Code,
          SHIPMENT_QUANTITY,
          SHIPMENT_DESCRIPTION,
          LOAD_AREA,
          UOM,
          order_sduk,
          TRANSPORTATION_SDUK,
          UTC_SHIPMENT_CREATION_MONTH_PART_KEY,
          SHIPMENT_SDUK,
          row_number() over (PARTITION BY SourceSystemKey,UPSOrderNumber,order_sduk,SHIPMENT_SDUK
                             ORDER BY TRANSPORTATION_SDUK NULLS FIRST
                             ) as transport_rn,
          is_deleted
          from temp
     """.format(**source_tables,hwm=hwm,days_back=days_back)
  logger.debug("query : " + query)
  return (query)

# COMMAND ----------

# DBTITLE 1,Main Function
def main():
    logger.info("Running Main function")
    try:    
        pid_get = get_pid()
        logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
        pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
        logger.info("pid: {pid}".format(pid=pid))
        ############################ ETL AUDIT #########################################################
        audit_result['process_id'] = pid
        audit_result['process_name'] = 'pr_load_summary_order_tracking'
        audit_result['process_type'] = 'DataBricks'
        audit_result['layer'] = 'gold'
        audit_result['table_name'] = digital_summary_order_tracking_et
        audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
        audit_result['start_time'] = start_time
        ################################################################################################   
        
        hwm=get_hwm('gold',digital_summary_order_tracking_et)
        logger.info(f'hwm {digital_summary_order_tracking_et}: {hwm}')
        
        spark.sql(get_delta_query(hwm))
        logger.info("get_delta_query finished")
        
        src_df = spark.sql(get_query(hwm))
        logger.info("query finished")
        ###################### generating hash key  #############################
        hash_key_columns = ['SourceSystemKey','UPSOrderNumber','order_sduk','SHIPMENT_SDUK','transport_rn']
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

        primary_keys = ['SourceSystemKey','UPSOrderNumber','order_sduk','SHIPMENT_SDUK','transport_rn']
        logger.debug('primary_keys: {primary_keys}'.format(primary_keys=primary_keys))
  
        logger.info(f'Merging to delta path: {digital_summary_order_tracking_path}')
  
        mergeToDelta(src_df,digital_summary_order_tracking_path,primary_keys)
        logger.info(f'merging to delta path finished: {digital_summary_order_tracking_path}')
        
        logger.info('setting hwm')
        res=set_hwm('gold',digital_summary_order_tracking_et,start_time,pid)
        logger.info(res)
        src_df.unpersist()
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

# COMMAND ----------

