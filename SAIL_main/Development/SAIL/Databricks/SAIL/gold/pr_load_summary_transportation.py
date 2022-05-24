# Databricks notebook source
# MAGIC %md
# MAGIC <b>Author </b>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;: GD000012734 Mahesh Rathi </br>
# MAGIC <b>Description</b>  : this notebook is to load summary_transportation table.

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
         AND ORDER_PLACED_DATE BETWEEN date_sub('{hwm}', {days_back}) AND current_timestamp
        union 
        select  case when NVL(FTTR.TRANS_ONLY_FLAG,'NULL') = 'NON_TRANS' and UPS_WMS_ORDER_NUMBER is not null 
                         then  UPS_WMS_ORDER_NUMBER
                         else UPS_ORDER_NUMBER 
                 end as ups_order_number  
          from {fact_transportation} FTTR  where  dl_update_timestamp>='{hwm}'
       """.format(**source_tables,hwm=hwm,days_back=days_back)
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

# DBTITLE 1,Query
def get_query(hwm):
  query = """ SELECT
    TRANSPORTATION.UPS_ORDER_NUMBER as UpsOrderNumber,
	TRANSPORTATION.SOURCE_SYSTEM_KEY as SourceSystemKey,
	UPS_WMS_ORDER_NUMBER as UpsWMSOrderNumber,
	UPS_WMS_SOURCE_SYSTEM_KEY as UpsWMSSourceSystemKey,
	TRANSPORTATION.SOURCE_ORDER_TYPE as SourceOrderType,
	EQUIPMENT_TYPE as EquipmentType,
	TRANSPORTATION.SOURCE_ORDER_SUB_TYPE as SourceOrderSubType,
	ORIGIN_COMPANY as OriginCompany,
	DESTINATION_COMPANY as DestinationCompany,
	TRANSPORTATION.ORIGIN_TIME_ZONE as OriginTimeZone,
	TRANSPORTATION.DESTINATION_TIME_ZONE as DestinationTimeZone,
	SOURCE_ORDER_STATE as SourceOrderState,
	TRANSPORTATION.SOURCE_ORDER_STATUS as SourceOrderStatus,
	TRANSPORTATION.ORDER_CANCELLED_FLAG as OrderCancelledFlag,
	ORDER_REC_CREATED_DATE as Order_Rec_CreatedDate,
	UTC_ORDER_REC_CREATED_DATE as UTC_Order_Rec_CreatedDate,
	LOFST_REC_CREATED_DATE as LOFST_Rec_CreatedDate,
	TRANSPORTATION.ORDER_PLACED_DATE as OrderPlacedDate,
	TRANSPORTATION.UTC_ORDER_PLACED_DATE as UTC_OrderPlacedDate,
	LOFST_ORDER_PLACED_DATE as LOFST_OrderPlacedDate,
	TRANSPORTATION.ORDER_CANCELLED_DATE as OrderCancelledDate ,
	TRANSPORTATION.UTC_ORDER_CANCELLED_DATE as UTC_OrderCancelledDate,
	LOFST_ORDER_CANCELLED_DATE as LOFST_OrderCancelledDate,
	TRANSPORTATION.ORDER_SHIPPED_DATE as OrderShippedDate,
	TRANSPORTATION.UTC_ORDER_SHIPPED_DATE as UTC_OrderShippedDate,
	LOFST_ORDER_SHIPPED_DATE as LOFST_OrderShippedDate,
	SCHEDULED_SHIPMENT_DATE as ScheduledShipmentDate ,
	UTC_SCHEDULED_SHIPMENT_DATE as UTC_ScheduledShipmentDate,
	LOFST_SCHEDULED_SHIPMENT_DATE as LOFST_ScheduledShipmentDate,
	ACTUAL_SHIPMENT_DATE as ActualShipmentDate,
	UTC_ACTUAL_SHIPMENT_DATE as UTC_ActualShipmentDate,
	LOFST_ACTUAL_SHIPMENT_DATE as LOFST_ActualShipmentDate,
	SCHEDULED_DELIVERY_DATE as ScheduledDeliveryDate,
	UTC_SCHEDULED_DELIVERY_DATE as UTC_ScheduledDeliveryDate,
	LOFST_SCHEDULED_DELIVERY_DATE as LOFST_ScheduledDeliveryDate,
	ACTUAL_DELIVERY_DATE as ActualDeliveryDate,
	UTC_ACTUAL_DELIVERY_DATE as UTC_ActualDeliveryDate,
	LOFST_ACTUAL_DELIVERY_DATE as LOFST_ActualDeliveryDate,
	ORDER_COUNT as OrderCount,
	ORIGINAL_SCHEDULED_DELIVERY_DATE as OriginalScheduledDeliveryDate,
	UTC_ORIGINAL_SCHEDULED_DELIVERY_DATE as UTC_OriginalScheduledDeliveryDate,
	LOFST_ORIGINAL_SCHEDULED_DELIVERY_DATE as LOFST_OriginalScheduledDeliveryDate,
	LOAD_ID as LOAD_ID,
	LOAD_EARLIEST_PICKUP_DATE as LoadEarliestPickUpDate,
	LOAD_LATEST_PICKUP_DATE as LoadLatestPickUpDate,
	LOAD_EARLIEST_DELIVERY_DATE as LoadEarliestDeliveryDate,
	LOAD_LATEST_DELIVERY_DATE as LoadLatestDeliveryDate,
	LOAD_CREATION_DATE as LoadCreationDate,
	LOAD_UPDATE_DATE as LoadUpdateDate,
	CARRIER_CODE as CarrierCode,
	LEVEL_OF_SERVICE_CODE as LevelOfServiceCode,
	WMS_PO_NUMBER as WMSPONumber,
	CARRIER_MODE as CarrierMode,
	TRANSPORTATION.TRANS_ONLY_FLAG as TrasOnlyFlag,
	SHIPMENT_NOTES as ShipmentNotes,
	COMMENTS as Comments,
	GFF_SHIPMENT_NUMBER as GFFShipmentNumber,
	GFF_SHIPMENT_INSTANCE_NUMBER as GFFShipmentInstanceNumber,
	PROOF_OF_DELIVERY_NAME as ProofOfDelivery,
	SCOPE as Scope,
	SECTOR as Sector,
	DIRECTION as Direction,
	AUTHORIZER_NAME as AuthorizerName,
	DELIVERY_INSTRUCTIONS as DeliveryInstructions,
	DESTINATION_CONTACT as DestinationContact,
    concat(FTO.source_system_key,'||',FTO.order_sduk) as order_sduk,
    concat(TRANSPORTATION.SOURCE_SYSTEM_KEY,'||',TRANSPORTATION.TRANSPORTATION_SDUK) as TRANSPORTATION_SDUK,
    TRANSPORTATION.UTC_ORDER_PLACED_MONTH_PART_KEY,
    0 as is_deleted
	FROM {fact_order_dim_inc}  FTO  
      INNER JOIN delta_fetch_tv FTV on (FTO.UPS_ORDER_NUMBER = FTV.UPS_ORDER_NUMBER)
      INNER JOIN {fact_transportation} TRANSPORTATION 
      ON  (CASE WHEN TRANSPORTATION.TRANS_ONLY_FLAG = 'NON_TRANS' THEN NVL(TRANSPORTATION.UPS_WMS_SOURCE_SYSTEM_KEY,FTO.SOURCE_SYSTEM_KEY) ELSE TRANSPORTATION.SOURCE_SYSTEM_KEY END = FTO.SOURCE_SYSTEM_KEY
       AND CASE WHEN TRANSPORTATION.TRANS_ONLY_FLAG = 'NON_TRANS' THEN  TRANSPORTATION.UPS_WMS_ORDER_NUMBER ELSE TRANSPORTATION.UPS_ORDER_NUMBER END = FTO.UPS_ORDER_NUMBER)
       where  FTO.ORDER_PLACED_DATE BETWEEN date_sub('{hwm}', {days_back}) AND current_timestamp
       """.format(**source_tables,hwm=hwm,days_back=days_back)
  logger.debug("query : " + query)
  return(query)

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
        audit_result['process_name'] = 'pr_load_summary_transportation'
        audit_result['process_type'] = 'DataBricks'
        audit_result['layer'] = 'gold'
        audit_result['table_name'] = digital_summary_transportation_et
        audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
        audit_result['start_time'] = start_time
        ###############################################################################################
        hwm=get_hwm('gold',digital_summary_transportation_et)
        hwm='1900-01-01 00:00:00'
        logger.info(f'hwm {digital_summary_transportation_et}: {hwm}')
        
        spark.sql(get_delta_query(hwm))
        logger.info("get_delta_query finished")
        
        src_df = spark.sql(get_query(hwm))
        logger.info("query finished")
               
        ###################### generating hash key  #############################
        hash_key_columns = ['SourceSystemKey','UPSOrderNumber','order_sduk','TRANSPORTATION_SDUK']
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
  
        primary_keys = ['SourceSystemKey','UPSOrderNumber','order_sduk','TRANSPORTATION_SDUK']
        logger.debug('primary_keys: {primary_keys}'.format(primary_keys=primary_keys))
  
        logger.info(f'Merging to delta path: {digital_summary_transportation_path}')
  
        mergeToDelta(src_df,digital_summary_transportation_path,primary_keys)
        logger.info(f'merging to delta path finished: {digital_summary_transportation_path}')
        
        logger.info('setting hwm')
        res=set_hwm('gold',digital_summary_transportation_et,start_time,pid)
        logger.info(res)
        #
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