# Databricks notebook source
"""
Author           : Vinoth Kumar Gopal
Description      : this notebook is to load inbound digital_summary_order_lines cosmos container.
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
cosmosContainerName = "digital_summary_order_lines"

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
        create or replace temp view ol_delta_fetch_tv 
        as
        select distinct UpsOrderNumber from (
        select UpsOrderNumber from {digital_summary_order_lines} where dl_update_timestamp>='{hwm}'
        union
        select UpsOrderNumber from {digital_summary_inbound_line} where dl_update_timestamp>='{hwm}'
        union
        select UpsOrderNumber from {digital_summary_order_lines_details} where dl_update_timestamp>='{hwm}'
        )""".format(**source_tables,hwm=hwm)
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

def get_pre_cosmos_query():
  query = """
WITH OLVendorDetail AS (
			select OL.UPSOrderNumber, OL.SourceSystemKey, named_struct('VSN',collect_list(case when LD.LineNumber = OL.LineNUmber then LD.VendorSerialNumber end ),
			'VCL',collect_list(case when LD.LineNumber = OL.LineNUmber then LD.VendorLotNumber end),
			'LPN',collect_list(case when LD.LineNumber = OL.LineNUmber then LD.LPNNumber end),
			'Designator',collect_list(case when LD.LineNumber = OL.LineNUmber then LD.DispositionValue end))  as OrderLineVendorDetail,
named_struct('VSN',collect_set(LD.VendorSerialNumber),
			'VCL',collect_set(LD.VendorLotNumber),
			'LPN',collect_set(LD.LPNNumber),
			'Designator',collect_set(LD.DispositionValue))  as OrderLineVendorDetail_search
			  from {digital_summary_order_lines} OL 
			  inner join {ol_delta_fetch_tv} dv on OL.UpsOrderNumber=dv.UpsOrderNumber
			  left outer join {digital_summary_order_lines_details} LD on 
			  LD.UPSOrderNumber = OL.UPSOrderNumber and 
			  LD.SourceSystemKey = OL.SourceSystemKey and
			  OL.is_deleted = 0 and LD.is_deleted = 0
			  --and LD.LineNumber = OL.LineNUmber
			  group by OL.UPSOrderNumber, OL.SourceSystemKey
			), OLDetail AS (
			select OL.UPSOrderNumber,OL.SourceSystemKey,OL.LineNumber,
collect_list(named_struct('itemNumber',LD.itemNumber,'vendorLotNumber',LD.VendorLotNumber,'vendorSerialNumber',LD.VendorSerialNumber,'expirationDate',LD.EXPIRATION_DATE)) orderlinedetail
			from {digital_summary_order_lines} OL 
            inner join {ol_delta_fetch_tv} dv on OL.UpsOrderNumber=dv.UpsOrderNumber
            left outer join {digital_summary_order_lines_details} LD on 
			  LD.UPSOrderNumber = OL.UPSOrderNumber AND 
			  LD.SourceSystemKey = OL.SourceSystemKey AND
			  LD.LineNumber = OL.LineNumber AND 
			(LD.VendorLotNumber IS NOT NULL OR LD.VendorSerialNumber IS NOT NULL OR LD.LPNNumber IS NOT NULL) and
			  OL.is_deleted = 0 and LD.is_deleted = 0
            group by OL.UPSOrderNumber, OL.SourceSystemKey, OL.LineNumber
			)
  select
  hash_key as id
  ,OL.UpsOrderNumber
  ,OL.SourceSystemKey
  ,OL.AccountId
  ,O.DP_SERVICELINE_KEY
  ,OL.DP_ORGENTITY_KEY
  ,O.is_inbound
  ,OL.LineNUmber AS LineNumber
  ,OL.SKU 
  ,OL.SKUDescription
  ,CAST(NVL(SKUQuantity, 0) AS INT) AS SKUQuantity
  ,CAST(NVL(SKUShippedQuantity, 0) AS INT) SKUShippedQuantity
  ,SKUWeight
  ,SKUDimensions
  ,SKUWeight_UOM
  ,SKUDimensions_UOM
  ,LineRefVal1 AS lineReferenceNumber1      
  ,LineRefVal2 AS lineReferenceNumber2      
  ,LineRefVal3 AS lineReferenceNumber3      
  ,LineRefVal4 AS lineReferenceNumber4      
  ,LineRefVal5 AS lineReferenceNumber5  
  ,case when OLD.orderlinedetail is null then array(named_struct('itemNumber','','vendorLotNumber','','vendorSerialNumber','','expirationDate','')) 
  else OLD.orderlinedetail end as OrderLineDetail
  ,OLVD.OrderLineVendorDetail as OrderLineVendorDetail
  ,OLVD.OrderLineVendorDetail_search as OrderLineVendorDetail_search  
  ,OL.CarrierCode
  ,OL.ShipmentLineCanceledReason AS lineCancelledReason      
  ,date_format(OL.ShipmentLineCanceledDate, 'yyyy-MM-dd HH:mm:ss.SSS') AS lineCancelledDateTime           
  ,O.OriginTimeZone AS lineCancelledDateTimeZone
  ,OL.order_sduk
  ,OL.order_line_sduk
  ,OL.FacilityId
  ,cast(null as string) as FacilityCode
  ,cast(null as string) as CustomerPONumber
  ,cast(null as string) as asnNumber
  ,cast(null as string) as ClientASNNumber
  ,cast(null as string) as ReceiptNumber
  ,cast(null as timestamp) as CreationDateTime
  ,cast(null as timestamp) as PutAwayDate
  ,date_format(O.SummaryDateTimeReceived, 'yyyy-MM-dd HH:mm:ss.SSS') as SummaryDateTimeReceived
  ,date_format(O.SummaryDateTimeShipped, 'yyyy-MM-dd HH:mm:ss.SSS') as SummaryDateTimeShipped
  ,IS_ASN
  ,O.OrderNumber
  ,OL.is_deleted
  from {digital_summary_order_lines} OL
  left join OLDetail OLD on OL.UPSOrderNumber = OLD.UPSOrderNumber and OL.SourceSystemKey = OLD.SourceSystemKey and OL.LineNumber = OLD.LineNumber
  left join OLVendorDetail OLVD on OL.UPSOrderNumber = OLVD.UPSOrderNumber and OL.SourceSystemKey = OLVD.SourceSystemKey
  inner join (select DSO.UpsOrderNumber,collect_set(nvl(DSO.OrderNumber,"")) as OrderNumber,DSO.SourceSystemKey,is_inbound,max(DateTimeReceived) as SummaryDateTimeReceived, max(OriginTimeZone) as OriginTimeZone, max(DP_SERVICELINE_KEY) as DP_SERVICELINE_KEY ,max(DateTimeShipped) as SummaryDateTimeShipped,max(IS_ASN) IS_ASN
  from  {digital_summary_orders} DSO
  inner join {digital_summary_onboarded_systems} OS on DSO.SourceSystemKey = OS.SourceSystemKey
  inner join {ol_delta_fetch_tv} dv on DSO.UpsOrderNumber = dv.UpsOrderNumber
  where is_inbound = 0
  and DateTimeReceived>=current_date-{days_back}
  --and AccountId in {account_id}
  --and UPSOrderNumber=''
  group by DSO.UPSOrderNumber,DSO.SourceSystemKey,is_inbound
  ) O 
  ON OL.UPSOrderNumber = O.UPSOrderNumber AND
  OL.SourceSystemKey = O.SourceSystemKey
union
SELECT 
    hash_key as id,
    IL.UpsOrderNumber,
    IL.SourceSystemKey,
    IL.AccountId,
    O.DP_SERVICELINE_KEY,
	IL.DP_ORGENTITY_KEY,
    O.is_inbound,
    IL.ReceiptLineNumber AS LineNumber, 
    IL.SKU,  
    IL.SKUDescription,  
    CAST(NVL(IL.ReceivedQuantity,0) AS INT) AS SKUQuantity,  
    CAST(NVL(IL.ShippedQuantity,0) AS INT) AS SKUShippedQuantity,  
    IL.SKUWeight,  
    IL.SKUDimensions,  
    IL.SKUWeight_UOM,  
    IL.SKUDimensions_UOM,  
    IL.InboundLine_Reference2 lineReferenceNumber1,  
    IL.InboundLine_Reference10 lineReferenceNumber2,  
    IL.InboundLine_Reference11 lineReferenceNumber3,
	cast(null as string) as lineReferenceNumber4,
	cast(null as string) as lineReferenceNumber5,
    (select case when collect_set(named_struct('itemNumber',LD.itemNumber, 'vendorLotNumber',LD.VendorLotNumber,'vendorSerialNumber',LD.VendorSerialNumber,'expirationDate',LD.EXPIRATION_DATE)) = array() then array(named_struct('itemNumber',null, 'vendorLotNumber',null,'vendorSerialNumber',null,'expirationDate',null)) 
else collect_set(named_struct('itemNumber',LD.itemNumber, 'vendorLotNumber',LD.VendorLotNumber,'vendorSerialNumber',LD.VendorSerialNumber,'expirationDate',LD.EXPIRATION_DATE))
end val
    from {digital_summary_order_lines_details} LD 
    where 
    LD.AccountId = IL.AccountId AND
    LD.UPSOrderNumber = IL.UPSOrderNumber AND
    LD.SourceSystemKey = IL.SourceSystemKey AND
	LD.itemNumber = IL.SKU) as OrderLineDetail,
    named_struct('VSN',array(''),'VCL',array(''),'LPN',array(''),'Designator',array('')) as OrderLineVendorDetail,
	named_struct('VSN',array(''),'VCL',array(''),'LPN',array(''),'Designator',array('')) as OrderLineVendorDetail_search,
    cast(null as string) as CarrierCode,
    cast(null as string) as lineCancelledReason,
    cast(null as string) as lineCancelledDateTime,
    O.OriginTimeZone AS lineCancelledDateTimeZone,
    cast(null as string) as order_sduk,
    cast(null as string) as order_line_sduk,
	IL.FacilityId,
	IL.FacilityCode,
	IL.ClientPONumber AS CustomerPONumber,
    IL.UPSASNNumber AS asnNumber, 
	IL.ClientASNNumber,
    IL.ReceiptNumber,
    date_format(IL.CreationDateTime, 'yyyy-MM-dd HH:mm:ss.SSS') CreationDateTime,
    IL.PutAwayDate,
    date_format(O.SummaryDateTimeReceived, 'yyyy-MM-dd HH:mm:ss.SSS') as SummaryDateTimeReceived,
    date_format(O.SummaryDateTimeShipped, 'yyyy-MM-dd HH:mm:ss.SSS') as SummaryDateTimeShipped,
    IS_ASN,
    O.OrderNumber,
    IL.is_deleted
    from
    {digital_summary_inbound_line} IL
    inner join (select DSO.UpsOrderNumber,collect_set(nvl(DSO.OrderNumber,"")) as OrderNumber,DSO.SourceSystemKey,is_inbound,max(DateTimeReceived) as SummaryDateTimeReceived, max(OriginTimeZone) as OriginTimeZone, max(DP_SERVICELINE_KEY) as DP_SERVICELINE_KEY ,max(DateTimeShipped) as SummaryDateTimeShipped,max(IS_ASN) IS_ASN
  from  {digital_summary_orders} DSO
  inner join {digital_summary_onboarded_systems} OS on DSO.SourceSystemKey = OS.SourceSystemKey
  inner join {ol_delta_fetch_tv} dv on DSO.UpsOrderNumber = dv.UpsOrderNumber
  where is_inbound = 1
  and DateTimeReceived>=current_date-{days_back}
  --and AccountId in {account_id}
  --and UPSOrderNumber=''
  group by DSO.UPSOrderNumber,DSO.SourceSystemKey,is_inbound
  ) O 
  ON 
  IL.UPSOrderNumber = O.UPSOrderNumber  AND 
  IL.SourceSystemKey = case when O.SourceSystemKey=1011 then IL.SourceSystemKey else  O.SourceSystemKey end
  """.format(**source_tables, ol_delta_fetch_tv='ol_delta_fetch_tv',days_back=days_back,account_id=account_id)
  return (query)

# COMMAND ----------

# DBTITLE 1,Main function
def main():
  logger.info('Main function is running')
  
  audit_result['process_name'] = 'load_digital_summary_order_lines_Container'
  audit_result['process_type'] = 'DataBricks'
  audit_result['layer'] = 'cosmos'
  audit_result['table_name'] = 'cosmos_digital_summary_order_lines'
  audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
  audit_result['start_time'] = start_time
  
  try:
      
    pid_get = get_pid()
    logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
    pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
    logger.info("pid: {pid}".format(pid=pid))
    
    audit_result['process_id'] = pid
    
    hwm=get_hwm('cosmos','cosmos_digital_summary_order_lines')
#     hwm='1900-01-01 00:00:00'
#     if hwm=='1900-01-01 00:00:00':
#       d = timedelta(days = 90)
#       back_date=st_dt - d
#       hwm=back_date.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f'hwm cosmos_digital_summary_order_lines: {hwm}'.format(hwm=hwm))
  
    logger.info("Creating digital summary orders line views for incremental data")
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
    res=set_hwm('cosmos','cosmos_digital_summary_order_lines',start_time,pid)
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