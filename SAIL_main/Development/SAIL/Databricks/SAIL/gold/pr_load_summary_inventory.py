# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <b>Author--GD000012733@ups.com-Vishal</b>

# COMMAND ----------

# DBTITLE 1,Importing python libraries
import datetime,time,json
from pytz import timezone
from  pyspark.sql.functions import input_file_name ,split ,size, from_utc_timestamp, lit  ,concat, when, col, sha1, row_number
from delta import DeltaTable

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
var_tmp_dim_customer_tv='TMP_DIM_CUSTOMER_tv'
var_tmp_item_tv='TMP_ITEM_tv'

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

# DBTITLE 1,Query q1 function
def run_q1():
    logger.debug("Running query q1 function and creating Temp table {var_tmp_dim_customer_tv}".format(var_tmp_dim_customer_tv=var_tmp_dim_customer_tv))
    q1=("""CREATE OR REPLACE TEMP VIEW {var_tmp_dim_customer_tv}
as SELECT 
      CL.GLD_ACCOUNT_MAPPED_KEY,  
	  CL.DP_SERVICELINE_KEY,  
	  CL.DP_ORGENTITY_KEY,
	  CL.EXT_CUSTOMER_ACCOUNT_NUMBER,
	  CL.SOURCE_SYSTEM_KEY,
	  CL.CUSTOMERKEY
	  FROM {dim_customer} CL 
      INNER JOIN {account_type_digital} GLAT ON CL.GLD_ACCOUNT_MAPPED_KEY = GLAT.Account_ID""".format(var_tmp_dim_customer_tv=var_tmp_dim_customer_tv,**source_tables))
    logger.debug("query : " + q1)
    return(q1)

# COMMAND ----------

# DBTITLE 1,Query q2 function
def run_q2():
    logger.debug("Running query q2 function and creating Temp table {var_tmp_item_tv}".format(var_tmp_item_tv=var_tmp_item_tv))
    q2=("""CREATE OR REPLACE TEMP VIEW {var_tmp_item_tv}
    as SELECT DISTINCT    
      PART_NUMBER,  
      PART_DESCRIPTION,  
      HAZMAT_CODE,  
      ITEM_LENGTH,  
      ITEM_WIDTH,  
      ITEM_HEIGHT,  
      ITEM_DIMENSIONS_UOM,  
      ITEM_WEIGHT,  
      ITEM_WEIGHT_UOM,  
      ITEM.ITEM_KEY,
      ITEM.CUSTOMER_ACCOUNT_NUMBER,
      ITEM.SOURCE_SYSTEM_KEY,
      SS.SOURCE_SYSTEM_NAME,
      ITEM.HAZMAT_CLASS,
      ITEM.STRATEGICGOODS_FLAG,
      ITEM.UN_NUMBER,
      CL.GLD_ACCOUNT_MAPPED_KEY,  
      CL.DP_SERVICELINE_KEY,  
      CL.DP_ORGENTITY_KEY	
      from {dim_item} ITEM  
        INNER JOIN {dim_source_system}  SS ON ITEM.SOURCE_SYSTEM_KEY = SS.SOURCE_SYSTEM_KEY
        INNER JOIN {var_tmp_dim_customer_tv} CL ON (ITEM.CUSTOMER_ACCOUNT_NUMBER = CL.EXT_CUSTOMER_ACCOUNT_NUMBER AND ITEM.SOURCE_SYSTEM_KEY = CL.SOURCE_SYSTEM_KEY)  
""".format(var_tmp_item_tv=var_tmp_item_tv,var_tmp_dim_customer_tv=var_tmp_dim_customer_tv,**source_tables))
    logger.debug("query : " + q2)
    return(q2)

# COMMAND ----------

# DBTITLE 1,get_query()
def get_query():
    query=(""" 
    SELECT ITEM.SOURCE_SYSTEM_KEY SourceSystemKey,   
       ITEM.SOURCE_SYSTEM_NAME SourceSystemName,  
       ITEM.GLD_ACCOUNT_MAPPED_KEY AccountId,  
       WSE.GLD_WAREHOUSE_MAPPED_KEY FacilityId, 
       ITEM.PART_NUMBER itemNumber,  
       ITEM.PART_DESCRIPTION itemDescription,
       ITEM.HAZMAT_CODE hazardClass,  
       ITEM.ITEM_LENGTH itemDimensions_length,  
       ITEM.ITEM_WIDTH itemDimensions_width,  
       ITEM.ITEM_HEIGHT itemDimensions_height,  
       ITEM.ITEM_DIMENSIONS_UOM itemDimensions_unitOfMeasurement_code,  
       ITEM.ITEM_WEIGHT itemWeight_weight,  
       ITEM.ITEM_WEIGHT_UOM itemWeight_unitOfMeasurement_Code,
        CASE  
          WHEN ITEM.SOURCE_SYSTEM_NAME LIKE '%SOFTEON%' THEN WSE.BUILDING_CODE  
          ELSE WSE.WAREHOUSE_CODE  
        END warehouseCode,  
       CASE WHEN FTOL.AVAILABLE_FLAG = 'Y' THEN SUM(ON_HAND_QUANTITY) ELSE 0 END  availableQuantity,  
       CASE WHEN FTOL.AVAILABLE_FLAG = 'N' THEN SUM(ON_HAND_QUANTITY) ELSE 0 END  nonAvailableQuantity,
       ITEM.DP_SERVICELINE_KEY,  
       ITEM.DP_ORGENTITY_KEY,
       FTOL.INV_REF_1 InvRef1,
       FTOL.INV_REF_2 InvRef2,
       FTOL.INV_REF_3 InvRef3,
       FTOL.INV_REF_4 InvRef4,
       FTOL.INV_REF_5 InvRef5,
       FTOL.LPN_NUMBER LPNNumber,
       ITEM.HAZMAT_CLASS HazmatClass,
       ITEM.STRATEGICGOODS_FLAG StrategicGoodsFlag,
       ITEM.UN_NUMBER UNNumber,
       FTOL.DESIGNATOR Designator,
       FTOL.VENDOR_SERIAL_NUMBER VendorSerialNumber,
       FTOL.VENDOR_LOT_NUMBER VendorLotNumber,
       FTOL.BATCH_STATUS BatchStatus,
       FTOL.EXPIRATION_DATE ExpirationDate,
       CL.EXT_CUSTOMER_ACCOUNT_NUMBER Account_number,
       FTOL.BATCH_HOLD_REASON BatchHoldReason,
       FTOL.HOLD_DESCRIPTION HoldDescription,
       0 as is_deleted
       FROM {var_tmp_item_tv} ITEM  
       LEFT JOIN {fact_inventory_snapshot} FTOL ON FTOL.ITEM_KEY=ITEM.ITEM_KEY AND FTOL.SOURCE_SYSTEM_KEY=ITEM.SOURCE_SYSTEM_KEY  
       LEFT JOIN {var_tmp_dim_customer_tv} CL ON (FTOL.CLIENT_KEY = CL.CUSTOMERKEY AND FTOL.SOURCE_SYSTEM_KEY = CL.SOURCE_SYSTEM_KEY)  
       LEFT JOIN {dim_warehouse} WSE ON (FTOL.WAREHOUSE_KEY = WSE.WAREHOUSE_KEY AND FTOL.SOURCE_SYSTEM_KEY = WSE.SOURCE_SYSTEM_KEY)  
       GROUP BY   
       ITEM.GLD_ACCOUNT_MAPPED_KEY,  
       ITEM.DP_SERVICELINE_KEY,  
       ITEM.DP_ORGENTITY_KEY,  
       WSE.GLD_WAREHOUSE_MAPPED_KEY,   
       ITEM.SOURCE_SYSTEM_KEY,   
       FTOL.ITEM_KEY,  
       ITEM.SOURCE_SYSTEM_NAME,  
       WSE.BUILDING_CODE,  
       WSE.WAREHOUSE_CODE,  
       FTOL.CLIENT_KEY,  
       WSE.WAREHOUSE_KEY,  
       ITEM.PART_NUMBER,  
       ITEM.PART_DESCRIPTION,  
       ITEM.HAZMAT_CODE,  
       ITEM.ITEM_LENGTH,  
       ITEM.ITEM_WIDTH,  
       ITEM.ITEM_HEIGHT,  
       ITEM.ITEM_DIMENSIONS_UOM,  
       ITEM.ITEM_WEIGHT,  
       ITEM.ITEM_WEIGHT_UOM,  
       FTOL.AVAILABLE_FLAG,
       FTOL.INV_REF_1,
       FTOL.INV_REF_2,
       FTOL.INV_REF_3,
       FTOL.INV_REF_4,
       FTOL.INV_REF_5,
       FTOL.LPN_NUMBER,
       ITEM.HAZMAT_CLASS,
       ITEM.STRATEGICGOODS_FLAG,
       ITEM.UN_NUMBER,
       FTOL.DESIGNATOR,
       FTOL.VENDOR_SERIAL_NUMBER,
       FTOL.VENDOR_LOT_NUMBER,
       FTOL.BATCH_STATUS,
       FTOL.EXPIRATION_DATE,
       CL.EXT_CUSTOMER_ACCOUNT_NUMBER,
       FTOL.BATCH_HOLD_REASON,
       FTOL.HOLD_DESCRIPTION """.format(var_tmp_item_tv=var_tmp_item_tv,var_tmp_dim_customer_tv=var_tmp_dim_customer_tv,**source_tables))
    logger.debug("query : " + query)
    return(query)

# COMMAND ----------

def get_final_query():
    query=(""" 
    SELECT
        SourceSystemKey
       ,SourceSystemName
       ,AccountId
       ,FacilityId
       ,itemNumber
       ,itemDescription
       ,hazardClass
       ,itemDimensions_length
       ,itemDimensions_width
       ,itemDimensions_height
       ,itemDimensions_unitOfMeasurement_code
       ,itemWeight_weight
       ,itemWeight_unitOfMeasurement_Code
       ,warehouseCode
       ,availableQuantity
       ,nonAvailableQuantity
       ,DP_SERVICELINE_KEY
       ,DP_ORGENTITY_KEY
       ,InvRef1
       ,InvRef2
       ,InvRef3
       ,InvRef4
       ,InvRef5
       ,LPNNumber
       ,HazmatClass
       ,StrategicGoodsFlag
       ,UNNumber
       ,Designator
       ,VendorSerialNumber
       ,VendorLotNumber
       ,BatchStatus
       ,ExpirationDate
       ,Account_number
       ,BatchHoldReason
       ,HoldDescription
       ,is_deleted
       ,hash_key 
        from src_data
       union  
       select 
        tgt.SourceSystemKey
       ,tgt.SourceSystemName
       ,tgt.AccountId
       ,tgt.FacilityId
       ,tgt.itemNumber
       ,tgt.itemDescription
       ,tgt.hazardClass
       ,tgt.itemDimensions_length
       ,tgt.itemDimensions_width
       ,tgt.itemDimensions_height
       ,tgt.itemDimensions_unitOfMeasurement_code
       ,tgt.itemWeight_weight
       ,tgt.itemWeight_unitOfMeasurement_Code
       ,tgt.warehouseCode
       ,tgt.availableQuantity
       ,tgt.nonAvailableQuantity
       ,tgt.DP_SERVICELINE_KEY
       ,tgt.DP_ORGENTITY_KEY
       ,tgt.InvRef1
       ,tgt.InvRef2
       ,tgt.InvRef3
       ,tgt.InvRef4
       ,tgt.InvRef5
       ,tgt.LPNNumber
       ,tgt.HazmatClass
       ,tgt.StrategicGoodsFlag
       ,tgt.UNNumber
       ,tgt.Designator
       ,tgt.VendorSerialNumber
       ,tgt.VendorLotNumber
       ,tgt.BatchStatus
       ,tgt.ExpirationDate
       ,tgt.Account_number
       ,tgt.BatchHoldReason
       ,tgt.HoldDescription
       ,1 as is_deleted
       ,tgt.hash_key 
       from {digital_summary_inventory} tgt 
       left join src_data src
       on ( tgt.hash_key =src.hash_key)
       where src.hash_key is null
       
       """.format(var_tmp_item_tv=var_tmp_item_tv,var_tmp_dim_customer_tv=var_tmp_dim_customer_tv,**source_tables))
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
        audit_result['process_name'] = 'pr_load_summary_inventory'
        audit_result['process_type'] = 'DataBricks'
        audit_result['layer'] = 'gold'
        audit_result['table_name'] = digital_summary_inventory_et
        audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
        audit_result['start_time'] = start_time
        #################################################################################################
   
        logger.info('Reading q1 query...')
        spark.sql(run_q1())
        logger.info('Reading q2 query...')
        spark.sql(run_q2())
        
        source_query = get_query()
        logger.info('Getting source query')
        stg_df = spark.sql(source_query)
        ###################### generating hash key  #############################
        columns = stg_df.schema.fieldNames()
        logger.debug("columns: {columns}".format(columns=columns))
        hash_exclude_col = ["is_deleted"]
        logger.debug("hash_exclude_col: {hash_exclude_col}".format(hash_exclude_col=hash_exclude_col))
        hash_key_columns = subtract_list(columns,hash_exclude_col)
        logger.debug(f"hash_key_columns: {hash_key_columns}")
        stg_df = stg_df.withColumn("hash_key", sha1_concat(hash_key_columns))
        
        stg_df.createOrReplaceTempView("src_data")
        
        final_query = get_final_query()
        logger.info('Getting final_query ')
        src_df = spark.sql(final_query)     
        
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
    
        logger.info(f'Merging to delta path: {digital_summary_inventory_path}')
    
        mergeToDelta(src_df,digital_summary_inventory_path,primary_keys)
        logger.info(f'merging to delta path finished: {digital_summary_inventory_path}')
        
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