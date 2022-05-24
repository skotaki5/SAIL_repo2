# Databricks notebook source
"""
Author           : Vinoth Kumar Gopal
Description      : this notebook is to load inbound digital_summary_transportation_rates_charges cosmos container.
"""

# COMMAND ----------

# DBTITLE 1,Importing python libraries
import datetime
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, DecimalType
from pyspark.sql.functions import col, from_json

# COMMAND ----------

# DBTITLE 1,Importing common variables
# MAGIC %run "/SAIL/includes/common_variables"

# COMMAND ----------

# DBTITLE 1,Importing common udfs
# MAGIC %run "/SAIL/includes/common_udfs"

# COMMAND ----------

# DBTITLE 1,Spark Configs
spark.conf.set("spark.databricks.io.cache.enabled","true")

# COMMAND ----------

# DBTITLE 1,Cosmos connection
#Cosmos connection
scope = 'key-vault-secrets'
cosmosEndpoint = dbutils.secrets.get(scope,"cosmosEndpoint")
cosmosMasterKey = dbutils.secrets.get(scope,"cosmosMasterKey")
cosmosDatabaseName = "SAIL"
cosmosContainerName = "digital_summary_transportation_rates_charges"

cfg = {
  "spark.cosmos.accountEndpoint" : cosmosEndpoint,
  "spark.cosmos.accountKey" : cosmosMasterKey,
  "spark.cosmos.database" : cosmosDatabaseName,
  "spark.cosmos.container" : cosmosContainerName
}



# COMMAND ----------

st_dt =datetime.now(tz=timezone(time_zone))
start_time = st_dt.strftime("%Y-%m-%d %H:%M:%S")

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

def get_delta_query(hwm):
    logger.debug("hwm: " + str(hwm))
    query ="""
CREATE OR REPLACE TEMP VIEW digital_summary_orders_vw AS
	WITH change_stg AS ( 
			SELECT UPSOrderNumber
			FROM {digital_summary_orders}
			WHERE dl_update_timestamp >= '{hwm}'
            
			UNION
			
			SELECT UPSOrderNumber
			FROM {digital_summary_transportation_rates_charges}
			WHERE dl_update_timestamp >= '{hwm}'
            
            UNION
            
            SELECT DSO.UPSOrderNumber
			FROM {digital_summary_transportation_rates_charges} TRC
            inner join {digital_summary_orders} DSO on 
            TRC.UpsOrderNumber = DSO.UPSTransportShipmentNumber
            where DSO.is_inbound = 0
			and TRC.dl_update_timestamp >= '{hwm}' 
			)
SELECT 
O.hash_key,
O.UPSOrderNumber,
O.UPSTransportShipmentNumber,
O.SourceSystemKey,
O.is_inbound,
O.AccountId,
O.DP_SERVICELINE_KEY,
O.DateTimeReceived,
O.is_deleted
FROM {digital_summary_orders} O
INNER JOIN {digital_summary_onboarded_systems} OS ON OS.sourcesystemkey = o.SourceSystemKey
INNER JOIN (SELECT DISTINCT UPSOrderNumber from change_stg) c ON o.UPSOrderNumber = c.UPSOrderNumber
    where o.DateTimeReceived >= case when date('{hwm}') = '1900-01-01' then current_date else date('{hwm}') end - {days_back}
    --and AccountId in {account_id}
        """.format(**source_tables,hwm=hwm,days_back=days_back,account_id=account_id)
    logger.debug("query : " + query)
    return(query)

# COMMAND ----------

def get_pre_cosmos_query():
  query = """
with costbreak as (
    select UpsOrderNumber, 
    sum(totalCustomerCharge) as totalCustomerCharge,
    max(totalCustomerChargeCurrency) as totalCustomerChargeCurrency,
    collect_list(val) AS CostBreakdown
    from
    (select UpsOrderNumber,
        named_struct('CostBreakdownType',ChargeDescription,
                'CurrencyCode',CurrencyCode,
                'CostBreakdownValue',sum(cast(charge as decimal(10,2)))) as val,
        sum(CAST(Charge as decimal(10,2))) AS totalCustomerCharge,
        MAX(CurrencyCode) AS totalCustomerChargeCurrency from {digital_summary_transportation_rates_charges} 
        where ChargeLevel = 'CUSTOMER_RATES'
        and is_deleted = 0
        group by UpsOrderNumber,ChargeDescription,CurrencyCode)
     group by UpsOrderNumber
), 

inv_currency_code AS (
SELECT UpsOrderNumber, SourceSystemKey, MAX(CurrencyCode) as inv_curr_code
    from {digital_summary_transportation_rates_charges} 
    where ChargeLevel = 'CUSTOMER_INVOICE'
    and is_deleted = 0
    group by UpsOrderNumber, SourceSystemKey
)

SELECT 
o.hash_key as id,
o.UPSOrderNumber as UpsOrderNumber,
o.UPSTransportShipmentNumber,
o.SourceSystemKey,
o.is_inbound,
o.AccountId,
o.DP_SERVICELINE_KEY,
date_format(o.DateTimeReceived, 'yyyy-MM-dd HH:mm:ss.SSS') as invoiceDateTime,
o.is_deleted,
cb.totalCustomerCharge,
case when o.is_inbound in (1,2) and ic.inv_curr_code is not null then ic.inv_curr_code else cb.totalCustomerChargeCurrency end as totalCustomerChargeCurrency,
cb.CostBreakdown
FROM {digital_summary_orders_vw} o 
left outer join costbreak cb on 
case when O.is_inbound = 0 then O.UPSTransportShipmentNumber else O.UpsOrderNumber end = cb.UpsOrderNumber
left outer join inv_currency_code ic on o.UpsOrderNumber = ic.UpsOrderNumber and o.SourceSystemKey = ic.SourceSystemKey
-- where O.UpsOrderNumber in ('XXXX','YYYY')
  """.format(**source_tables, digital_summary_orders_vw='digital_summary_orders_vw',days_back=days_back)
  return (query)

# COMMAND ----------

# DBTITLE 1,Main function
def main():
  logger.info('Main function is running')
  
  audit_result['process_name'] = 'load_digital_summary_transportation_rates_charges_Container'
  audit_result['process_type'] = 'DataBricks'
  audit_result['layer'] = 'cosmos'
  audit_result['table_name'] = 'cosmos_digital_summary_transportation_rates_charges'
  audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
  audit_result['start_time'] = start_time
  
  try:
      
    pid_get = get_pid()
    logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
    pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
    logger.info("pid: {pid}".format(pid=pid))
    
    audit_result['process_id'] = pid
    
    hwm=get_hwm('cosmos','cosmos_digital_summary_transportation_rates_charges')
    #hwm = '1900-01-01 00:00:00'
#     if hwm=='1900-01-01 00:00:00':
#       d = timedelta(days = 90)
#       back_date=st_dt - d
#       hwm=back_date.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f'hwm cosmos_digital_summary_transportation_rates_charges: {hwm}'.format(hwm=hwm))
    
    logger.info("Creating digital summary transportation rates charges views for incremental data")
    spark.sql(get_delta_query(hwm))
    logger.info("get_delta_query finished")  
    
    logger.info('Reading source data...')
    
    src_query =get_pre_cosmos_query()
    logger.debug('cosmos_query : ' + src_query)
    
    cosmos_df = spark.sql(src_query)
    
    logger.debug("Adding audit columns")
    cosmos_df = add_audit_columns(cosmos_df, pid,datetime.now(),datetime.now())
    cnt=cosmos_df.count()
    logger.info('count is {cnt}'.format(cnt=cnt))
    
    logger.info('Writing to Cosmos: {container_name}'.format(container_name=cosmosContainerName))
    cosmos_df.write.format("cosmos.oltp").options(**cfg).mode("APPEND").save()
    
    logger.info('setting hwm')
    res=set_hwm('cosmos','cosmos_digital_summary_transportation_rates_charges',start_time,pid)
    logger.info(res)
      
      
    audit_result['numTargetRowsInserted'] = cnt
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