# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <b>Author--GD000012733@ups.com</b>
# MAGIC <b>Name:Vishal</b>
# MAGIC </br>version 1.1 - added changes for story UPSGLD-15243 by arpan

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
var_transport_details_tv='TRANSPORT_DETAILS_tv'
var_inbound_tv='INBOUND_tv'
var_inbound_line_tv='INBOUND_LINE_tv'
#test_fact_inbound_line_path='/mnt/sail/bronze/some_path/test_dir'
# #temp table is used within this notebook


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

# DBTITLE 1,delta_query
def get_delta_query(hwm):
  logger.debug("hwm: " + str(hwm))
  query ="""
        create or replace temp view delta_fetch_tv 
        as
          select ups_order_number from {fact_order_dim_inc} where dl_update_timestamp>'{hwm}'
        union
          select ups_order_number from {fact_transport_details}  where dl_update_timestamp>'{hwm}'
        
        """.format(**source_tables,hwm=hwm)
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

# DBTITLE 1,Query q1 function
def run_q1():
  logger.debug("Running query q1 function and creating Temp table {var_transport_details_tv}".format(var_transport_details_tv=var_transport_details_tv))
  q1=("""CREATE OR REPLACE TEMP VIEW {var_transport_details_tv}
as SELECT 
	FTO.GLD_ACCOUNT_MAPPED_KEY AccountId ,  
    FTO.DP_SERVICELINE_KEY,  
    NULL AS DP_ORGENTITY_KEY,  
    UPPER(WSE.GLD_WAREHOUSE_MAPPED_KEY) FacilityId,  
    WSE.WAREHOUSE_CODE FacilityCode,  
    FTO.UPS_ORDER_NUMBER,  
    CASE WHEN IS_MANAGED = 0 THEN FTO.CUSTOMER_ORDER_NUMBER ELSE FTD.CLIENT_ASN END ClientASNNumber,  
    FTO.SOURCE_SYSTEM_KEY,
    FTO.IS_MANAGED
FROM {fact_order_dim_inc} FTO
LEFT JOIN {fact_transport_details} FTD  ON (FTO.SOURCE_SYSTEM_KEY=FTD.SOURCE_SYSTEM_KEY AND FTO.UPS_ORDER_NUMBER=FTD.UPS_ORDER_NUMBER)
INNER JOIN {dim_warehouse} WSE ON (FTO.WAREHOUSE_KEY = WSE.WAREHOUSE_KEY AND FTO.SOURCE_SYSTEM_KEY = WSE.SOURCE_SYSTEM_KEY)  
WHERE FTO.IS_INBOUND = 1""".format(var_transport_details_tv=var_transport_details_tv,**source_tables))
  logger.debug("query : " + q1)
  return(q1)
  

# COMMAND ----------

# DBTITLE 1,Query q2 function
def run_q2():
  logger.debug("Running query q1 function and creating Temp table {var_inbound_tv}".format(var_inbound_tv=var_inbound_tv))
  q2=("""CREATE OR REPLACE TEMP VIEW {var_inbound_tv} as 
  Select * FROM 
  (SELECT TD.*,CASE WHEN TD.IS_MANAGED = 0 THEN TD.UPS_ORDER_NUMBER ELSE  FTO.UPS_ORDER_NUMBER END UPSASNNumber,FTO.SOURCE_SYSTEM_KEY AS ASN_SOURCE_SYSTEM_KEY 
    FROM {var_transport_details_tv} TD        
    LEFT JOIN {fact_order_dim_inc} FTO 
            ON  TD.ClientASNNumber = FTO.CUSTOMER_ORDER_NUMBER 
            AND FTO.IS_ASN = 1  
            AND FTO.SOURCE_SYSTEM_KEY = 1017
    WHERE TD.SOURCE_SYSTEM_KEY = 1011 AND NVL(TD.ClientASNNumber,'') <> ''  
  UNION   
    SELECT TD.*,CASE WHEN TD.IS_MANAGED = 0 THEN TD.UPS_ORDER_NUMBER ELSE  FTO.UPS_ORDER_NUMBER END UPSASNNumber,FTO.SOURCE_SYSTEM_KEY AS ASN_SOURCE_SYSTEM_KEY   
    FROM {var_transport_details_tv} TD        
    LEFT JOIN {fact_order_dim_inc} FTO 
              ON  NVL(TD.ClientASNNumber,'') = NVL(FTO.CUSTOMER_ORDER_NUMBER,'') 
              AND FTO.IS_ASN = 1  
              AND TD.SOURCE_SYSTEM_KEY  = FTO.SOURCE_SYSTEM_KEY 
              AND TD.UPS_ORDER_NUMBER    = FTO.UPS_ORDER_NUMBER  
    WHERE TD.SOURCE_SYSTEM_KEY <> 1011
   )""".format(var_inbound_tv=var_inbound_tv,var_transport_details_tv=var_transport_details_tv,**source_tables))
  logger.debug("query : " + q2)
  return(q2)
  

# COMMAND ----------

# DBTITLE 1,Query q3 function
def run_q3():
  logger.debug("Running query q1 function and creating Temp table {var_inbound_line_tv}".format(var_inbound_line_tv=var_inbound_line_tv))
  q3=("""CREATE OR REPLACE TEMP VIEW {var_inbound_line_tv} as 
  Select * from 
  (SELECT INB.*
		,FIL.ITEM_KEY
		,FIL.RCPT_HEADER_NUMBER
		,FIL.RCPT_LINE_NUMBER
		,FIL.INBND_LINE_SHIPPED_QTY 
		,FIL.INBND_LINE_RECEIVED_QTY 
		,FIL.INBND_HDR_CREATION_DATE
		,FIL.SOURCE_INBOUND_LINE_REFERENCE_2
		,FIL.SOURCE_INBOUND_LINE_REFERENCE_10
		,FIL.SOURCE_INBOUND_LINE_REFERENCE_11
		,FIL.PO_HEADER_NUMBER
		--,FIL.IL_LAST_PUTAWAY_DATE
		,NULL AS IL_LAST_PUTAWAY_DATE
        ,FIL.CASES
        FROM {var_inbound_tv} INB  
        LEFT JOIN {fact_inbound_line} FIL ON FIL.SOURCE_INBOUND_HEADER_NUMBER = INB.UPSASNNumber AND FIL.SOURCE_SYSTEM_KEY = INB.ASN_SOURCE_SYSTEM_KEY
WHERE INB.SOURCE_SYSTEM_KEY <> 1002
UNION
SELECT INB.*
		,FIL.ITEM_KEY
		,FIL.RCPT_HEADER_NUMBER
		,FIL.RCPT_LINE_NUMBER
		,FIL.INBND_LINE_SHIPPED_QTY 
		,FIL.INBND_LINE_RECEIVED_QTY 
		,FIL.INBND_HDR_CREATION_DATE
		,FIL.SOURCE_INBOUND_LINE_REFERENCE_2
		,FIL.SOURCE_INBOUND_LINE_REFERENCE_10
		,FIL.SOURCE_INBOUND_LINE_REFERENCE_11
		,FIL.PO_HEADER_NUMBER
		--,FIL.IL_LAST_PUTAWAY_DATE
		,NULL AS IL_LAST_PUTAWAY_DATE
        ,FIL.CASES
FROM {var_inbound_tv} INB  
LEFT JOIN {fact_inbound_line} FIL ON FIL.ASN_HEADER_NUMBER = INB.UPSASNNumber AND FIL.SOURCE_SYSTEM_KEY = INB.ASN_SOURCE_SYSTEM_KEY  
WHERE INB.SOURCE_SYSTEM_KEY = 1002
UNION --UPSGLD-15244
SELECT 
		 C.GLD_ACCOUNT_MAPPED_KEY AccountId  
		,C.DP_SERVICELINE_KEY   
		,NULL as DP_ORGENTITY_KEY  
		,UPPER(WSE.GLD_WAREHOUSE_MAPPED_KEY) FacilityId  
		,WSE.WAREHOUSE_CODE as FacilityCode
		,NULL AS UPS_ORDER_NUMBER  
		,NULL AS ClientASNNumber  
		,FIL.SOURCE_SYSTEM_KEY 
		,NULL AS IS_MANAGED
		,FIL.ASN_HEADER_NUMBER AS UPSASNNumber 
		,FIL.SOURCE_SYSTEM_KEY AS ASN_SOURCE_SYSTEM_KEY 
 		,FIL.ITEM_KEY
		,FIL.RCPT_HEADER_NUMBER
		,FIL.RCPT_LINE_NUMBER
		,FIL.INBND_LINE_SHIPPED_QTY 
		,FIL.INBND_LINE_RECEIVED_QTY 
		,FIL.INBND_HDR_CREATION_DATE
		,FIL.SOURCE_INBOUND_LINE_REFERENCE_2
		,FIL.SOURCE_INBOUND_LINE_REFERENCE_10
		,FIL.SOURCE_INBOUND_LINE_REFERENCE_11
		,FIL.PO_HEADER_NUMBER
        --,FIL.IL_LAST_PUTAWAY_DATE
		,NULL AS IL_LAST_PUTAWAY_DATE
		,FIL.CASES    
    FROM {fact_inbound_line}  FIL 
    LEFT JOIN {dim_customer} C  ON C.CUSTOMERKEY = FIL.CLIENT_KEY
    LEFT JOIN {dim_warehouse} WSE  ON WSE.WAREHOUSE_KEY = FIL.WAREHOUSE_KEY
WHERE FIL.SOURCE_SYSTEM_KEY = 1002 
      AND FIL.INBND_HDR_CREATION_DATE BETWEEN date_sub(current_timestamp, {days_back}) AND current_timestamp
      AND ASN_HEADER_NUMBER IS NULL --UPSGLD-15244
      )""".format(days_back=days_back,var_inbound_line_tv=var_inbound_line_tv,var_inbound_tv=var_inbound_tv,**source_tables))
  logger.debug("query : " + q3)
  return(q3)
  

# COMMAND ----------

# DBTITLE 1,Source query
def get_query():
  query=("""
  create or replace temp view digital_summary_inbound_line_stg
  as
  SELECT
  TD.AccountId AccountId,  
  TD.DP_SERVICELINE_KEY,  
 cast(TD.DP_ORGENTITY_KEY as VARCHAR(100)) ,  
  TD.FacilityId,  
  TD.FacilityCode,  
 TD.UPS_ORDER_NUMBER UPSOrderNumber,  
 TD.UPSASNNumber,
 TD.ClientASNNumber,
 TD.PO_HEADER_NUMBER  ClientPONumber,  
 TD.RCPT_HEADER_NUMBER ReceiptNumber,  
 TD.RCPT_LINE_NUMBER ReceiptLineNumber,  
 SUM(TD.INBND_LINE_SHIPPED_QTY)  AS ShippedQuantity, 
 SUM(TD.INBND_LINE_RECEIVED_QTY)  AS ReceivedQuantity, 
 TD.INBND_HDR_CREATION_DATE CreationDateTime,  
 ITEM.PART_NUMBER SKU,  
 ITEM.PART_DESCRIPTION SKUDescription,  
 concat(CAST(ITEM.ITEM_LENGTH as VARCHAR(100)),'*',CAST(ITEM.ITEM_WIDTH as VARCHAR(100)),'*',CAST(ITEM.ITEM_HEIGHT as VARCHAR(100) )) as SKUDimensions,
 ITEM.ITEM_WEIGHT SKUWeight,  
 ITEM.ITEM_DIMENSIONS_UOM as SKUDimensions_UOM,   
 ITEM.ITEM_WEIGHT_UOM as SKUWeight_UOM,   
 TD.SOURCE_SYSTEM_KEY as SourceSystemKey, 
 TD.SOURCE_INBOUND_LINE_REFERENCE_2 as InboundLine_Reference2,  
 TD.SOURCE_INBOUND_LINE_REFERENCE_10 as InboundLine_Reference10,  
 TD.SOURCE_INBOUND_LINE_REFERENCE_11 as InboundLine_Reference11, 
 cast(TD.IL_LAST_PUTAWAY_DATE as timestamp) as PutAwayDate ,
 TD.cases
FROM {var_inbound_line_tv} TD  
LEFT JOIN {dim_item} ITEM ON TD.ITEM_KEY=ITEM.ITEM_KEY AND TD.ASN_SOURCE_SYSTEM_KEY=ITEM.SOURCE_SYSTEM_KEY  
GROUP BY    
 TD.FacilityId  
,TD.AccountId  
,TD.DP_ORGENTITY_KEY  
,TD.DP_SERVICELINE_KEY  
,TD.FacilityCode  
,TD.UPS_ORDER_NUMBER  
,TD.UPSASNNumber  
,TD.ClientASNNumber  
,TD.PO_HEADER_NUMBER
,TD.RCPT_HEADER_NUMBER  
,TD.RCPT_LINE_NUMBER  
,TD.INBND_HDR_CREATION_DATE  
,ITEM.PART_NUMBER   
,ITEM.PART_DESCRIPTION  
,ITEM.ITEM_WEIGHT  
,ITEM.ITEM_DIMENSIONS_UOM  
,ITEM.ITEM_WEIGHT_UOM  
,ITEM.ITEM_LENGTH  
,ITEM.ITEM_WEIGHT  
,ITEM.ITEM_WIDTH  
,ITEM.ITEM_HEIGHT  
,TD.SOURCE_SYSTEM_KEY
,TD.SOURCE_INBOUND_LINE_REFERENCE_2
,TD.SOURCE_INBOUND_LINE_REFERENCE_10
,TD.SOURCE_INBOUND_LINE_REFERENCE_11
,TD.IL_LAST_PUTAWAY_DATE
,TD.cases
   
""".format(var_inbound_line_tv=var_inbound_line_tv,**source_tables))
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

def soft_delete_query():
  query="""--UPSGLD-15244, changing soft delete logic for bug fix
  with temp as 
     (
     select distinct upsordernumber from digital_summary_inbound_line_stg
     )
      update  {digital_summary_inbound_line} t
      set t.is_deleted =1, t.dl_update_timestamp = current_timestamp , t.dl_hash='NULL'
      where exists ( select 1 from temp TV where t.upsordernumber=TV.upsordernumber
      )
      """.format(**source_tables)
  logger.debug("query :"+query)
  return (query)


# COMMAND ----------

def main():
    logger.info('Main function is running')
    
    try:
        pid_get = get_pid()
        logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
        pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
        logger.info("pid: {pid}".format(pid=pid))
        
        ############################ ETL AUDIT #########################################################
        audit_result['process_id'] = pid
        audit_result['process_name'] = 'pr_load_summary_inbound_line'
        audit_result['process_type'] = 'DataBricks'
        audit_result['layer'] = 'gold'
        audit_result['table_name'] = digital_summary_inbound_line_et
        audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
        audit_result['start_time'] = start_time
        ################################################################################################
        
        hwm=get_hwm('gold',digital_summary_inbound_line_et)
        logger.info(f'hwm {digital_summary_inbound_line_et}: {hwm}')
        
        spark.sql(get_delta_query(hwm))
        logger.info("get_delta_query finished")
        
        logger.info('Reading q1 query...')
        spark.sql(run_q1())
        logger.info('Reading q2 query...')
        spark.sql(run_q2())
        logger.info('Reading q3 query...')
        spark.sql(run_q3())
        logger.info('Reading source query...')
        df = spark.sql(get_query())
        
        spark.sql("cache table digital_summary_inbound_line_stg")
        df = spark.sql("select * from digital_summary_inbound_line_stg")
        src_df=df.withColumn("is_deleted",lit(0))
        
        spark.sql(soft_delete_query())
        logger.info(f"soft delete operation on {digital_summary_inbound_line_et} finished")
        
        ###################### generating hash key  #############################
        hash_key_columns =["AccountId","DP_SERVICELINE_KEY","DP_ORGENTITY_KEY","FacilityId","FacilityCode","UPSOrderNumber","UPSASNNumber","ClientASNNumber","ClientPONumber", 
                            "ReceiptNumber","ReceiptLineNumber","ShippedQuantity","ReceivedQuantity","CreationDateTime","SKU","SKUDescription","SKUDimensions","SKUWeight","SKUDimensions_UOM",
                            "SKUWeight_UOM","SourceSystemKey","InboundLine_Reference2","InboundLine_Reference10","InboundLine_Reference11","PutAwayDate","cases"]
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
        logger.info(f'Merging to delta path: {digital_summary_inbound_line_path}')
        mergeToDelta(src_df,digital_summary_inbound_line_path,primary_keys)
        logger.info(f'merging to delta path finished: {digital_summary_inbound_line_path}')
        logger.info('setting hwm')
        res=set_hwm('gold',digital_summary_inbound_line_et,start_time,pid)
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