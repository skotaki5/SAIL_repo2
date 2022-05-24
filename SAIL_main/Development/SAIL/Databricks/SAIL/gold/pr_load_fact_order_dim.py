# Databricks notebook source
# MAGIC %md
# MAGIC <b>Versions</b>          
# MAGIC v0.1  GD000012733@ups.com Vishal           
# MAGIC v0.2  GD000012780@ups.com Prashant Gupta  
# MAGIC V1.0  added new columns TT_IS_MANAGED,tt_is_inbound   by @Arpan

# COMMAND ----------

# DBTITLE 1,Importing python libraries
import json

# COMMAND ----------

# DBTITLE 1,Importing common variables
# MAGIC %run "/SAIL/includes/common_variables"

# COMMAND ----------

# DBTITLE 1,Importing common functions
# MAGIC  %run "/SAIL/includes/common_udfs"

# COMMAND ----------

# DBTITLE 1,Set start_time and Timezone

start_time = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# DBTITLE 1,Setting debug mode
dbutils.widgets.text("log_debug_mode", "","")
dbutils.widgets.get("log_debug_mode")
log_debug_mode = getArgument("log_debug_mode").strip()

if log_debug_mode == "Y":
  logger = _get_logger(time_zone,logging.DEBUG)
else:
  logger = _get_logger(time_zone,logging.INFO)

# COMMAND ----------

# DBTITLE 1,Source query
def get_query(hwm,days_back):
  logger.debug("hwm: " + str(hwm))
  query =("""SELECT FTO.UPS_ORDER_NUMBER
	,FTO.CUSTOMER_ORDER_NUMBER
	,FTO.SOURCE_SYSTEM_KEY
	,FTO.WAREHOUSE_KEY
	,FTO.IS_INBOUND
	,FTO.CUSTOMER_PO_NUMBER
	,FTO.REFERENCE_ORDER_NUMBER
	,FTO.SERVICE_KEY
	,FTO.CARRIER_LOS_KEY
	,FTO.ORIGIN_LOCATION_KEY
	,FTO.DESTINATION_LOCATION_KEY
	,FTO.CLIENT_KEY
	,FTO.ORDER_PLACED_DATE
	,FTO.TRANSACTION_TYPE_ID
	,FTO.IS_MANAGED
	,FTO.IS_ASN
	,FTO.SOURCE_ORDER_SUB_TYPE
	,FTO.UTC_ORDER_PLACED_DATE
	,FTO.ORDER_LATEST_ACTIVITY_DATE
	,FTO.UTC_ORDER_LATEST_ACTIVITY_DATE
	,FTO.ORDER_CANCELLED_FLAG
	,FTO.ORDER_CANCELLED_DATE
	,FTO.UTC_ORDER_CANCELLED_DATE
	,FTO.ORDER_SHIPPED_DATE
	,FTO.UTC_ORDER_SHIPPED_DATE
	,FTO.LOFST_ORDER_LATEST_ACTIVITY_DATE
	,FTO.SHIPMENT_COUNT
	,FTO.STO_ORDER_COUNT
	,FTO.ORDER_LATEST_ACTIVITY_DATE_KEY
	,FTO.SOURCE_ORDER_STATUS
	,FTO.SOURCE_ORDER_TYPE
	,FTO.FREIGHT_CARRIER_CODE
	,FTO.WAYBILL_AIRBILL_NUM
	,FTO.DONOT_SHIP_BEFORE_DATE
	,FTO.ORIGIN_TIME_ZONE
	,FTO.DESTINATION_TIME_ZONE
	,FTO.ORDER_SDUK
	,FTO.ETL_BATCH_NUMBER
	,CL.GLD_ACCOUNT_MAPPED_KEY
	,CL.DP_SERVICELINE_KEY
	,CL.DP_ORGENTITY_KEY
	,CL.EXT_CUSTOMER_ACCOUNT_NUMBER
	,CAR.LEVEL_OF_SERVICE_DESC ServiceLevelName
	,CAR.LEVEL_OF_SERVICE_CODE ServiceLevelCode
	,CAR.CARRIER_CODE CarrierCode
	,CAR.CARRIER_NAME CarrierName
    ,CAR.CARRIER_GROUP
	,SR.SERVICE_NAME as SERVICE_NAME_SR
	,LC.SERVICE_NAME as SERVICE_NAME_LC
    ,LC.SERVICELEVELNAME as SERVICELEVELNAME_LC
    ,LC.CARRIERNAME as CARRIERNAME_LC
	,WSE.GLD_WAREHOUSE_MAPPED_KEY FacilityId
	,WSE.ADDRESS_LINE_1
	,WSE.ADDRESS_LINE_2
	,WSE.CITY
	,WSE.PROVINCE
	,WSE.POSTAL_CODE
	,WSE.COUNTRY
	,WSE.WAREHOUSE_KEY as WAREHOUSE_KEY_WSE
    ,WSE.BUILDING_CODE
    ,wse.WAREHOUSE_CODE
	,SS.SOURCE_SYSTEM_NAME
	,MS.OrderStatusName
	,TT.TransactionTypeName
    ,TT.Is_Inbound as tt_is_inbound
    ,TT.Is_Managed as tt_is_managed
	,GLO.ADDRESS_LINE_1  as ADDRESS_LINE_1_ORIGIN
	,GLO.ADDRESS_LINE_2  as ADDRESS_LINE_2_ORIGIN
	,GLO.CITY            as CITY_ORIGIN
	,GLO.PROVINCE        as PROVINCE_ORIGIN
	,GLO.POSTAL_CODE     as POSTAL_CODE_ORIGIN
	,GLO.COUNTRY         as COUNTRY_ORIGIN
	,GLO.LOCATION_CODE   as LOCATION_CODE_ORIGIN
	,GLD.ADDRESS_LINE_1  as ADDRESS_LINE_1_DESTINATION
	,GLD.ADDRESS_LINE_2  as ADDRESS_LINE_2_DESTINATION
	,GLD.CITY            as CITY_DESTINATION
	,GLD.PROVINCE        as PROVINCE_DESTINATION
	,GLD.POSTAL_CODE     as POSTAL_CODE_DESTINATION
	,GLD.COUNTRY         as COUNTRY_DESTINATION
	,GLD.LOCATION_CODE   as LOCATION_CODE_DESTINATION
	,GLD.LOCATION_NAME
    ,FTO.UTC_ORDER_PLACED_MONTH_part_key
    ,FTO.is_deleted
FROM {fact_order} FTO
inner JOIN {dim_customer}  CL ON (FTO.CLIENT_KEY = CL.CUSTOMERKEY AND FTO.SOURCE_SYSTEM_KEY = CL.SOURCE_SYSTEM_KEY)   
inner JOIN {account_type_digital}  GLAT ON CL.GLD_ACCOUNT_MAPPED_KEY = GLAT.ACCOUNT_ID  
LEFT JOIN {dim_carrier_los}  CAR ON (FTO.CARRIER_LOS_KEY = CAR.CARRIER_LOS_KEY AND FTO.SOURCE_SYSTEM_KEY = CAR.SOURCE_SYSTEM_KEY)
LEFT JOIN {dim_service}  SR ON (FTO.SERVICE_KEY = SR.SERVICE_KEY AND FTO.SOURCE_SYSTEM_KEY = SR.SOURCE_SYSTEM_KEY) 
LEFT JOIN {local_courier_service}  LC ON (LC.SOURCE_SYSTEM_KEY = SR.SOURCE_SYSTEM_KEY AND LC.SERVICE_NAME = SR.SERVICE_NAME)               
LEFT JOIN {dim_warehouse}  WSE ON (FTO.WAREHOUSE_KEY =  WSE.WAREHOUSE_KEY  AND FTO.SOURCE_SYSTEM_KEY = WSE.SOURCE_SYSTEM_KEY)
LEFT JOIN {dim_source_system}  SS ON SS.SOURCE_SYSTEM_KEY=FTO.SOURCE_SYSTEM_KEY          
LEFT  JOIN {map_ordersearchstatus} MS  ON MS.SOURCE_SYSTEM_KEY = FTO.SOURCE_SYSTEM_KEY and LOWER(MS.OrderStatusCode) = LOWER(FTO.SOURCE_ORDER_STATUS)    
LEFT JOIN {map_transactiontype_milestone} TT ON FTO.TRANSACTION_TYPE_ID = TT.TransactionTypeId
AND (TT.Is_Inbound IS NOT NULL AND TT.Is_International IS NOT NULL AND TT.Is_Managed IS NOT NULL)  
LEFT JOIN {dim_geo_location} GLO ON (FTO.ORIGIN_LOCATION_KEY = GLO.GEO_LOCATION_KEY AND FTO.SOURCE_SYSTEM_KEY = GLO.SOURCE_SYSTEM_KEY)
LEFT JOIN {dim_geo_location}  GLD ON (FTO.DESTINATION_LOCATION_KEY = GLD.GEO_LOCATION_KEY AND FTO.SOURCE_SYSTEM_KEY = GLD.SOURCE_SYSTEM_KEY)  
WHERE FTO.ORDER_PLACED_DATE IS NOT NULL
	AND FTO.ORDER_PLACED_DATE BETWEEN date_sub('{hwm}', {days_back}) AND current_timestamp
	AND  NOT
    (
           NVL(FTO.SOURCE_ORDER_TYPE,'NULL') = 'Transportation' 
           AND FTO.SOURCE_SYSTEM_KEY = 1002  
	)
        """.format(**source_tables,hwm=hwm,days_back=days_back))
  logger.debug("query : " + query)
  return(query)
  
  
#Putting the generated value from var_current to SQL query for Timestamp

# COMMAND ----------

# DBTITLE 1,Main function
def main():
    logger.info('Main function is running')
  
    try:
        logger.info('getting hwm')
        hwm=get_hwm('gold',fact_order_dim_inc_et)
        logger.info('hwm: {hwm}'.format(hwm=hwm))

        logger.info('Getting source query')
        source_query = get_query(hwm,days_back)

        logger.info('Reading source data...')
        src_df = spark.sql(source_query)
        
        pid_get = get_pid()
        logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
        pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
        logger.info("pid: {pid}".format(pid=pid))
      
        ############################ ETL AUDIT #########################################################
        audit_result['process_id'] = pid
        audit_result['process_name'] = 'pr_load_fact_order_dim'
        audit_result['process_type'] = 'DataBricks'
        audit_result['layer'] = 'gold'
        audit_result['table_name'] = fact_order_dim_inc_et
        audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
        audit_result['start_time'] = start_time
        ##################################################################################################
        logger.debug("Generating hash")
        columns = src_df.schema.fieldNames()
        logger.debug("columns: {columns}".format(columns=columns))
        hash_exclude_col = []
        logger.debug("hash_exclude_col: {hash_exclude_col}".format(hash_exclude_col=hash_exclude_col))
        hash_col = subtract_list(columns,hash_exclude_col)
        logger.debug("hash_col: {hash_col}".format(hash_col=hash_col))

        logger.debug("Adding hash")
        src_df = src_df.withColumn("dl_hash", sha1_concat(hash_col))

        logger.debug("Adding audit columns")
        src_df = add_audit_columns(src_df, pid,datetime.now(),datetime.now())

        primary_keys = ['SOURCE_SYSTEM_KEY','ORDER_SDUK']
        logger.debug('primary_keys: {primary_keys}'.format(primary_keys=primary_keys))

        logger.info(f'Merging to delta path : {fact_order_dim_inc_path}')

        mergeToDelta(src_df,fact_order_dim_inc_path,primary_keys)
        logger.info(f'Merging to delta path finished: {fact_order_dim_inc_path}')

        logger.info('setting hwm')
        res=set_hwm('gold',fact_order_dim_inc_et,start_time,pid)
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
