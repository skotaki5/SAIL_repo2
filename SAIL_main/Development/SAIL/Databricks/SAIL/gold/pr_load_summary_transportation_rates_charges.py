# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <b>Author--GD000012733@ups.com</b>
# MAGIC <b>Name:Vishal</b>

# COMMAND ----------

import datetime,time,json
from pytz import timezone
from  pyspark.sql.functions import input_file_name ,split ,size, from_utc_timestamp, lit  ,concat, when, col, sha1, row_number

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
var_fact_order_summary_tv='FACT_ORDER_SUMMARY_tv'
#temp table is used within this notebook

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

def get_check_load_query(hwm):
  logger.debug("hwm: " + str(hwm))
  query ="""
        select UPS_ORDER_NUMBER from {fact_transportation_rates_charges} TREF  where dl_update_timestamp>='{hwm}'
        """.format(**source_tables,hwm=hwm)
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

# DBTITLE 1,delta_query
def get_delta_query(hwm):
  logger.debug("hwm: " + str(hwm))
  query ="""
        create or replace temp view delta_fetch_tv 
        as
         select ups_order_number from {fact_order_dim_inc} where dl_update_timestamp>'{hwm}'
        union 
          select  case when NVL(FTTR.TRANS_ONLY_FLAG,'NULL') = 'NON_TRANS' and UPS_WMS_ORDER_NUMBER is not null 
                         then  UPS_WMS_ORDER_NUMBER
                         else UPS_ORDER_NUMBER 
                 end as ups_order_number 
          from {fact_transportation} FTTR
            where dl_update_timestamp>='{hwm}'
        union
         select UPS_ORDER_NUMBER from {fact_transportation_rates_charges} TREF  where dl_update_timestamp>='{hwm}'
        """.format(**source_tables,hwm=hwm)
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

# DBTITLE 1,Query q1 function
def run_q1():
  logger.debug("Running query q1 function and creating Temp table {var_fact_order_summary_tv}".format(var_fact_order_summary_tv=var_fact_order_summary_tv))
  q1=("""CREATE OR REPLACE TEMP VIEW {var_fact_order_summary_tv}
as SELECT 
	FTO.UPS_ORDER_NUMBER,
    FTO.GLD_ACCOUNT_MAPPED_KEY,            
    FTO.DP_SERVICELINE_KEY,
	FTO.DP_ORGENTITY_KEY,
    FTO.SOURCE_SYSTEM_KEY,             
	FTO.SOURCE_SYSTEM_NAME,               
    FTTR.UPS_ORDER_NUMBER AS UPS_TRANSPORT_ORDER_NUMBER, 
	FTTR.SOURCE_SYSTEM_KEY AS UPS_TRANSPORT_SOURCE_SYSTEM_KEY,
    FTO.TRANSACTION_TYPE_ID AS TransactionTypeId,
    FTO.IS_MANAGED,
	FTO.IS_INBOUND,
    concat(FTO.source_system_key,'||',FTO.order_sduk) as order_sduk,
    concat(FTTR.SOURCE_SYSTEM_KEY,'||',FTTR.TRANSPORTATION_SDUK) as TRANSPORTATION_SDUK
FROM {fact_order_dim_inc}  FTO  
  INNER JOIN delta_fetch_tv FTV on (FTO.UPS_ORDER_NUMBER = FTV.UPS_ORDER_NUMBER)
LEFT JOIN {fact_transportation} FTTR  ON (CASE WHEN FTTR.TRANS_ONLY_FLAG <> 'TRANS_ONLY'  THEN   NVL(FTTR.UPS_WMS_SOURCE_SYSTEM_KEY,FTO.SOURCE_SYSTEM_KEY)  ELSE FTTR.SOURCE_SYSTEM_KEY END = FTO.SOURCE_SYSTEM_KEY AND CASE WHEN FTTR.TRANS_ONLY_FLAG <> 'TRANS_ONLY'  THEN FTTR.UPS_WMS_ORDER_NUMBER ELSE FTTR.UPS_ORDER_NUMBER END = FTO.UPS_ORDER_NUMBER)""".format(var_fact_order_summary_tv=var_fact_order_summary_tv,**source_tables))
  logger.debug("query : " + q1)
  return(q1)


# COMMAND ----------

# DBTITLE 1,Source query
def get_query():
  query=("""
    with temp as 
    (
    SELECT
    RC.UPS_ORDER_NUMBER As UpsOrderNumber ,
	RC.SOURCE_SYSTEM_KEY As SourceSystemKey,
	RC.LOAD_ID As LOAD_ID,
	RC.SEQUENCE_NUMBER As SequenceNumber,
    RC.CHARGE_TYPE As ChargeType,
	RC.RATE As Rate,
    RC.RATE_QUALIFER As RateQualifer,
    RC.CHARGE As Charge,
    RC.CHARGE_DESCRIPTION As ChargeDescription,
    RC.CHARGE_LEVEL As ChargeLevel,  
    RC.EDI_CODE As EdiCode,
    RC.FREIGHT_CLASS as FreightClass,
    RC.FAK_FREIGHT_CLASS as FAKFreightClass,
    RC.CONTRACT_NAME as ContractName,
    RC.CURRENCY_CODE as CurrencyCode,
    RC.INVOICE_NUMBER as InvoiceNumber,
    FTO.order_sduk,
    FTO.TRANSPORTATION_SDUK,
    concat(RC.source_system_key,'||',RC.CHARGE_SDUK) as CHARGE_SDUK,
    0 is_deleted
	FROM {var_fact_order_summary_tv} FTO
	INNER JOIN {fact_transportation_rates_charges} RC
	  ON CASE WHEN IS_INBOUND IN (1,2) THEN FTO.UPS_ORDER_NUMBER ELSE FTO.UPS_TRANSPORT_ORDER_NUMBER END = RC.UPS_ORDER_NUMBER				 
		     AND CASE WHEN IS_INBOUND IN (1,2) THEN FTO.SOURCE_SYSTEM_KEY ELSE  FTO.UPS_TRANSPORT_SOURCE_SYSTEM_KEY END = RC.SOURCE_SYSTEM_KEY
   )
   select t.*,
          row_number() over (PARTITION BY t.SourceSystemKey,t.UPSOrderNumber,t.order_sduk ,t.CHARGE_SDUK
                             ORDER BY t.TRANSPORTATION_SDUK NULLS FIRST
                             ) as transport_rn
          from temp t          
   """.format(var_fact_order_summary_tv=var_fact_order_summary_tv,**source_tables))
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

# DBTITLE 1,Main function
def main():
    logger.info('Main function is running')
  
    try:
        pid_get = get_pid()
        logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
        pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
        logger.info("pid: {pid}".format(pid=pid))
        ############################ ETL AUDIT #########################################################
        audit_result['process_id'] = pid
        audit_result['process_name'] = 'pr_load_summary_transportation_rates_charges'
        audit_result['process_type'] = 'DataBricks'
        audit_result['layer'] = 'gold'
        audit_result['table_name'] = digital_summary_transportation_rates_charges_et
        audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
        audit_result['start_time'] = start_time
        ################################################################################################
        hwm=get_hwm('gold',digital_summary_transportation_rates_charges_et)
        logger.info(f'hwm {digital_summary_transportation_rates_charges_et}: {hwm}')
        
        check_load = spark.sql(get_check_load_query(hwm)).count()
        logger.info("check_load: " + str(check_load) )
        
        if check_load>0:
    
            spark.sql(get_delta_query(hwm))
            logger.info("get_delta_query finished")
        
            spark.sql(run_q1())
            logger.info('Getting source query')
            source_query = get_query()
            
            logger.info('Reading source data...')
            src_df = spark.sql(source_query)
            ##################### generating hash key  #############################
            hash_key_columns = ['SourceSystemKey','UPSOrderNumber','order_sduk','charge_sduk','transport_rn']
            logger.debug(f"hash key columns: {hash_key_columns}")
        
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
        
            primary_keys = ['SourceSystemKey','UPSOrderNumber','order_sduk','charge_sduk','transport_rn']
            logger.debug('primary_keys: {primary_keys}'.format(primary_keys=primary_keys))
        
            logger.info(f'Merging to delta path: {digital_summary_transportation_rates_charges_path}')
        
            mergeToDelta(src_df,digital_summary_transportation_rates_charges_path,primary_keys)
            logger.info(f'merging to delta path finished: {digital_summary_transportation_rates_charges_path}')
        
        else:
            logger.info('Nothing to process')
    
        logger.info('setting hwm')
        res=set_hwm('gold',digital_summary_transportation_rates_charges_et,start_time,pid)
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