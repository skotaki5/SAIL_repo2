# Databricks notebook source
# MAGIC %md
# MAGIC <b>Author </b>         : GD000012020 Shrey Jain </br>
# MAGIC <b>Description</b>     : this notebook is to load summary_orders table.</br>
# MAGIC <b>V1.1 </b>           : GD000012020 Arpan Bhardwaj </br>
# MAGIC <b>Description</b>     : implemented incremental logic. </br>
# MAGIC <b> version 1.2   </b> : impemented change log till sprint 35 , dated 8 april 2022. </br>

# COMMAND ----------

import datetime,time,json
from pytz import timezone
from  pyspark.sql.functions import input_file_name ,split ,size, from_utc_timestamp, lit  ,concat, when, col, sha1, row_number

# COMMAND ----------

# DBTITLE 1,Import Common Variables
# MAGIC %run "/SAIL/includes/common_variables"

# COMMAND ----------

# DBTITLE 1,Import Common Utilities
# MAGIC %run "/SAIL/includes/common_udfs"

# COMMAND ----------

start_time = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# DBTITLE 1,Set logger
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
         AND FTO.ORDER_PLACED_DATE BETWEEN date_sub('{hwm}', {days_back}) AND current_timestamp
        union 
          select  case when NVL(FTTR.TRANS_ONLY_FLAG,'NULL') = 'NON_TRANS' and UPS_WMS_ORDER_NUMBER is not null 
                         then  UPS_WMS_ORDER_NUMBER
                         else UPS_ORDER_NUMBER 
                 end as ups_order_number  
          from {fact_transportation} FTTR  where  dl_update_timestamp>='{hwm}'
        union
         select ups_order_number from {fact_order_line} FTOR  where dl_update_timestamp>='{hwm}'
         
        union
         select ups_order_number from {fact_order_reference} FTOR  where dl_update_timestamp>='{hwm}'
        union
         select ups_order_number from {fact_transportation_exception} FTTE  where dl_update_timestamp>='{hwm}'
        union
         select UPSOrderNumber as ups_order_number from {digital_summary_milestone_activity}   where dl_update_timestamp>='{hwm}' 
         
        """.format(**source_tables,hwm=hwm,days_back=days_back)
  logger.debug("query : " + query)
  return(query)


# COMMAND ----------

# DBTITLE 1,Query 1
def get_query1(hwm):
  query="""
  CREATE OR REPLACE TEMP VIEW fact_order_tv
as
  SELECT       
    FTO.CUSTOMER_ORDER_NUMBER,      
    FTO.UPS_ORDER_NUMBER,          
    FTO.CUSTOMER_PO_NUMBER,      
    FTO.REFERENCE_ORDER_NUMBER,      
    CASE WHEN FTO.SOURCE_SYSTEM_KEY IN (1000,1001,1011) THEN FTO.SOURCE_ORDER_TYPE ELSE FTO.SOURCE_ORDER_SUB_TYPE END SOURCE_ORDER_SUB_TYPE,     -- change log       
    FTO.ORDER_PLACED_DATE,      
    FTO.UTC_ORDER_PLACED_DATE,      
    FTO.ORDER_LATEST_ACTIVITY_DATE,      
    FTO.UTC_ORDER_LATEST_ACTIVITY_DATE,      
    FTO.ORDER_CANCELLED_FLAG,      
    FTO.ORDER_CANCELLED_DATE,           
    FTO.UTC_ORDER_CANCELLED_DATE,           
    FTO.ORDER_SHIPPED_DATE,           
    FTO.UTC_ORDER_SHIPPED_DATE,      
    FTO.LOFST_ORDER_LATEST_ACTIVITY_DATE,      
    FTO.SHIPMENT_COUNT,      
    FTO.STO_ORDER_COUNT,      
    FTO.SOURCE_SYSTEM_KEY,      
    FTO.SERVICE_KEY,      
    FTO.WAREHOUSE_KEY,      
    FTO.CARRIER_LOS_KEY,      
    FTO.ORIGIN_LOCATION_KEY,      
    FTO.DESTINATION_LOCATION_KEY,      
    FTO.ORDER_LATEST_ACTIVITY_DATE_KEY,      
    FTO.SOURCE_ORDER_STATUS,      
    FTO.GLD_ACCOUNT_MAPPED_KEY,          
    '' DP_PRODUCTLINE_KEY,      
    FTO.DP_SERVICELINE_KEY,      
    FTO.DP_ORGENTITY_KEY,      
    FTO.ServiceLevelName,      
    FTO.ServiceLevelCode,      
    FTO.CarrierCode,      
    FTO.CarrierName,      
    FTO.IS_INBOUND,      
    FTO.IS_ASN,      
    FTO.TRANSACTION_TYPE_ID,    
    FTO.FREIGHT_CARRIER_CODE,  
    FTO.WAYBILL_AIRBILL_NUM,    
    FTO.DONOT_SHIP_BEFORE_DATE,    
    FTO.ORIGIN_TIME_ZONE,    
    FTO.DESTINATION_TIME_ZONE,    
    FTO.EXT_CUSTOMER_ACCOUNT_NUMBER,
    FTO.SERVICE_NAME_SR,
    FTO.SERVICE_NAME_LC,
    FTO.SERVICELEVELNAME_LC,
    FTO.CARRIERNAME_LC,
    FTO.FacilityId,
	FTO.ADDRESS_LINE_1
	,FTO.ADDRESS_LINE_2
	,FTO.CITY
	,FTO.PROVINCE
	,FTO.POSTAL_CODE
	,FTO.COUNTRY
	,FTO.WAREHOUSE_KEY_WSE
    ,FTO.BUILDING_CODE
    ,FTO.WAREHOUSE_CODE
    ,FTO.ADDRESS_LINE_1_ORIGIN
	,FTO.ADDRESS_LINE_2_ORIGIN
	,FTO.CITY_ORIGIN
	,FTO.PROVINCE_ORIGIN
	,FTO.POSTAL_CODE_ORIGIN
	,FTO.COUNTRY_ORIGIN
	,FTO.LOCATION_CODE_ORIGIN
    ,FTO.ADDRESS_LINE_1_DESTINATION
	,FTO.ADDRESS_LINE_2_DESTINATION
	,FTO.CITY_DESTINATION
	,FTO.PROVINCE_DESTINATION
	,FTO.POSTAL_CODE_DESTINATION
	,FTO.COUNTRY_DESTINATION
	,FTO.LOCATION_CODE_DESTINATION
	,FTO.LOCATION_NAME
    ,FTO.SOURCE_SYSTEM_NAME
    ,FTO.OrderStatusName
    ,FTO.TransactionTypeName
    ,FTO.tt_is_managed
    ,FTO.tt_is_inbound
    ,FTO.IS_MANAGED -- change log
 ,concat(FTO.source_system_key,'||',FTO.order_sduk) as order_sduk
 ,FTO.is_deleted
 ,FTO.UTC_ORDER_PLACED_MONTH_part_key
FROM {fact_order_dim_inc}  FTO  
 INNER JOIN delta_fetch_tv FTV on (FTO.UPS_ORDER_NUMBER = FTV.UPS_ORDER_NUMBER)
 WHERE FTO.ORDER_PLACED_DATE BETWEEN date_sub('{hwm}', {days_back}) AND current_timestamp
  """.format(**source_tables,hwm=hwm,days_back=days_back)
  logger.debug("query : " + query)
  return (query)

# COMMAND ----------

# DBTITLE 1,Query 2
def get_query2():
  query = """
  CREATE OR REPLACE TEMP VIEW FTOL
as
  SELECT       
 FTOL.SOURCE_SYSTEM_KEY,    
 FTOL.UPS_ORDER_NUMBER,      
 SUM(FTOL.ORDER_LINE_COUNT) ORDER_LINE_COUNT,      
 SUM(SHIPPED_QUANTITY) SHIPPED_QUANTITY            
FROM fact_order_tv FTO      
  INNER JOIN {fact_order_line}  FTOL       
 ON FTO.UPS_ORDER_NUMBER = FTOL.UPS_ORDER_NUMBER AND FTO.SOURCE_SYSTEM_KEY = FTOL.SOURCE_SYSTEM_KEY
GROUP BY      
 FTOL.SOURCE_SYSTEM_KEY,      
 FTOL.UPS_ORDER_NUMBER  
  """.format(**source_tables)
  logger.debug("query : " + query)
  return (query)

# COMMAND ----------

# DBTITLE 1,Query 4
def get_query4():
  query = """
  CREATE OR REPLACE TEMP VIEW SUMMARY_ORDERS  
  as
  SELECT FTO.SOURCE_SYSTEM_KEY,      
  FTO.GLD_ACCOUNT_MAPPED_KEY AccountId,      
  FTO.DP_SERVICELINE_KEY,      
  FTO.DP_ORGENTITY_KEY,      
  FTO.FacilityId,       
  FTO.UPS_ORDER_NUMBER UPSOrderNumber,      
  CASE WHEN FTO.SOURCE_SYSTEM_NAME = 'SPLUS' AND FTO.IS_INBOUND = 0 THEN FTOR.ORDER_REF_1_VALUE ELSE FTO.CUSTOMER_ORDER_NUMBER END OrderNumber,          
  FTO.REFERENCE_ORDER_NUMBER ReferenceOrder,      
  FTO.CUSTOMER_PO_NUMBER CustomerPO,      
        FTO.ORDER_PLACED_DATE  DateTimeReceived,      
  FTO.UTC_ORDER_PLACED_DATE  UTC_DateTimeReceived,      
  FTO.ORDER_LATEST_ACTIVITY_DATE LatestStatusDate,      
  FTO.UTC_ORDER_LATEST_ACTIVITY_DATE UTC_LatestStatusDate,      
  FTO.ORDER_CANCELLED_FLAG OrderCancelledFlag,      
  FTO.ORDER_CANCELLED_DATE DateTimeCancelled,           
  FTO.UTC_ORDER_CANCELLED_DATE UTC_DateTimeCancelled,           
  FTO.ORDER_SHIPPED_DATE DateTimeShipped,           
  FTO.UTC_ORDER_SHIPPED_DATE UTC_DateTimeShipped,             
  cast(NULL as string) as RouteCode,          
  cast(NULL as string) as ClientOrderType,      
  cast(NULL as string) as CancelledReasonCode,      
  cast(NULL as string) as LogiNext_OrderFlag,      
  cast(NULL as string) as LogiNext_OrderCurrentSegment,      
  NVL(FTO.OrderStatusName,FTO.SOURCE_ORDER_STATUS) OrderStatusName, 
  
   CASE WHEN FTO.SOURCE_SYSTEM_KEY = 1011 THEN FTTR.LEVEL_OF_SERVICE_CODE                ---10/11/2021
       WHEN FTO.tt_is_Inbound = 0 AND FTO.tt_is_Managed = 1 AND FTTR.SOURCE_SYSTEM_KEY IN (1021,1022) THEN FTTR.LEVEL_OF_SERVICE_CODE
       WHEN FTO.SERVICE_NAME_SR = 'Customer Pickup' THEN FTO.SERVICE_NAME_SR
       WHEN FTO.SERVICE_NAME_SR = FTO.SERVICE_NAME_LC THEN concat_ws('-',RIGHT(FTO.SERVICELEVELNAME_LC,7),FTO.SERVICE_NAME_SR) 
    ELSE  FTO.ServiceLevelName END ServiceLevel, 
  
  CASE
      WHEN FTO.SOURCE_SYSTEM_KEY = 1011 THEN FTTR.CARRIER_CODE  
      WHEN FTO.tt_is_Inbound = 0 AND FTO.tt_is_Managed = 1 AND FTTR.SOURCE_SYSTEM_KEY IN (1021,1022) THEN FTTR.CARRIER_CODE
      WHEN FTO.SERVICE_NAME_SR = 'Customer Pickup' THEN FTO.SERVICE_NAME_SR 
      WHEN FTO.SERVICE_NAME_SR = FTO.SERVICE_NAME_LC THEN FTO.CARRIERNAME_LC 
      ELSE FTO.CarrierName END Carrier,    -- change log         
  CASE WHEN FTO.SERVICE_NAME_SR = 'Customer Pickup' THEN FTO.SERVICE_NAME_SR WHEN FTO.SERVICE_NAME_SR = FTO.SERVICE_NAME_LC THEN concat_ws('-',RIGHT(FTO.SERVICELEVELNAME_LC,7),FTO.SERVICE_NAME_SR) ELSE FTO.ServiceLevelCode END ServiceLevelCode,          
  CASE WHEN FTO.SERVICE_NAME_SR = 'Customer Pickup' THEN FTO.SERVICE_NAME_SR WHEN FTO.SERVICE_NAME_SR = FTO.SERVICE_NAME_LC THEN FTO.CARRIERNAME_LC ELSE FTO.CarrierCode END CarrierCode,       
  FTO.LOCATION_NAME ConsigneeName,            
  CASE WHEN FTO.IS_INBOUND in(1,2) THEN FTO.ADDRESS_LINE_1_ORIGIN ELSE FTO.ADDRESS_LINE_1 END  OriginAddress1,        
  CASE WHEN FTO.IS_INBOUND in(1,2) THEN FTO.ADDRESS_LINE_2_ORIGIN ELSE FTO.ADDRESS_LINE_2 END  OriginAddress2,       
  CASE WHEN FTO.IS_INBOUND in(1,2) THEN FTO.CITY_ORIGIN ELSE FTO.CITY END  OriginCity,       
  CASE WHEN FTO.IS_INBOUND in(1,2) THEN FTO.PROVINCE_ORIGIN ELSE FTO.PROVINCE END  OriginProvince,       
  CASE WHEN FTO.IS_INBOUND in(1,2) THEN FTO.POSTAL_CODE_ORIGIN ELSE FTO.POSTAL_CODE END  OriginPostalCode,       
  CASE WHEN FTO.IS_INBOUND in(1,2) THEN FTO.COUNTRY_ORIGIN ELSE FTO.COUNTRY END  OriginCountry,       
  CASE WHEN FTO.IS_INBOUND in(0,2) THEN FTO.ADDRESS_LINE_1_DESTINATION ELSE FTO.ADDRESS_LINE_1 END  DestinationAddress1,        
  CASE WHEN FTO.IS_INBOUND in(0,2) THEN FTO.ADDRESS_LINE_2_DESTINATION ELSE FTO.ADDRESS_LINE_2 END  DestinationAddress2,       
  CASE WHEN FTO.IS_INBOUND in(0,2) THEN FTO.CITY_DESTINATION ELSE FTO.CITY END  DestinationCity,       
  CASE WHEN FTO.IS_INBOUND in(0,2) THEN FTO.PROVINCE_DESTINATION ELSE FTO.PROVINCE END  DestinationProvince,       
  CASE WHEN FTO.IS_INBOUND in(0,2) THEN FTO.POSTAL_CODE_DESTINATION ELSE FTO.POSTAL_CODE END  DestinationPostalcode,       
  CASE WHEN FTO.IS_INBOUND in(0,2) THEN FTO.COUNTRY_DESTINATION ELSE FTO.COUNTRY END  DestinationCountry,      
  FTO.SOURCE_ORDER_SUB_TYPE OrderType,       
  FTO.SOURCE_SYSTEM_KEY SourceSystemKey,       
  FTO.SOURCE_SYSTEM_NAME SourceSystemName,       
  FTO.SHIPMENT_COUNT ShipmentCount,         
  STO_ORDER_COUNT IsSTO,      
  '' TrackingNo,      
  CASE WHEN date_format(LOFST_ORDER_LATEST_ACTIVITY_DATE,'yyyyMMdd') = date_format(current_timestamp(),'yyyyMMdd') THEN 'Y' -- need to update with tzoffset logic     
   ELSE 'N' END TodayFlag,      
  cast ( NULL as string) as ORDER_REF_1_LABEL,      
  FTOR.ORDER_REF_1_VALUE,      
  cast ( NULL as string) as ORDER_REF_2_LABEL,      
  FTOR.ORDER_REF_2_VALUE,      
  cast ( NULL as string) as ORDER_REF_3_LABEL,      
  FTOR.ORDER_REF_3_VALUE,      
  cast ( NULL as string) as ORDER_REF_4_LABEL,      
  FTOR.ORDER_REF_4_VALUE,      
  cast ( NULL as string) as ORDER_REF_5_LABEL,      
  FTOR.ORDER_REF_5_VALUE,      
  cast ( NULL as string) as ORDER_REF_6_LABEL,      
  cast ( NULL as string) as ORDER_REF_6_VALUE,      
  cast ( NULL as string) as ORDER_REF_7_LABEL,      
  cast ( NULL as string) as ORDER_REF_7_VALUE,      
  cast ( NULL as string) as ORDER_REF_8_LABEL,      
  cast ( NULL as string) as ORDER_REF_8_VALUE,      
  cast ( NULL as string) as ORDER_REF_9_LABEL,      
  cast ( NULL as string) as ORDER_REF_9_VALUE,      
  cast ( NULL as string) as ORDER_REF_10_LABEL,      
  cast ( NULL as string) as ORDER_REF_10_VALUE,      
  cast ( NULL as string) as ORDER_REF_11_LABEL,      
  cast ( NULL as string) as ORDER_REF_11_VALUE,      
  cast ( NULL as string) as ORDER_REF_12_LABEL,      
  cast ( NULL as string) as ORDER_REF_12_VALUE,      
  cast ( NULL as string) as ORDER_REF_13_LABEL,      
  cast ( NULL as string) as ORDER_REF_13_VALUE,      
  cast ( NULL as string) as ORDER_REF_14_LABEL,      
  cast ( NULL as string) as ORDER_REF_14_VALUE,      
  cast ( NULL as string) as ORDER_REF_15_LABEL,      
  cast ( NULL as string) as ORDER_REF_15_VALUE,         
  CASE WHEN FTTR.SOURCE_ORDER_TYPE  = 'Transportation' THEN 1 ELSE 0 END TransOnly,      
  CASE WHEN FTTR.SOURCE_ORDER_STATE = 'Vendor Assigned' AND FTTR.ACTUAL_SHIPMENT_DATE IS NULL THEN 'InTransit to Pick-up'       
  WHEN FTTR.ACTUAL_SHIPMENT_DATE IS NOT NULL AND FTTR.ACTUAL_DELIVERY_DATE IS NULL THEN 'InTransit to Delivery'      
  WHEN FTTR.ACTUAL_DELIVERY_DATE IS NOT NULL THEN 'Delivered'      
  WHEN FTTR.ACTUAL_DELIVERY_DATE > FTTR.SCHEDULED_DELIVERY_DATE THEN 'Late Deliveries' END TransMilestone,      
  FTTE.EXCEPTION_REASON ExceptionCode,      
-- OriginalScheduledDeliveryDateTime,     
 -- CASE WHEN FTO.SOURCE_SYSTEM_NAME = 'SPLUS' THEN FTTR.ORIGINAL_SCHEDULED_DELIVERY_DATE ELSE FTTR.LOAD_LATEST_DELIVERY_DATE END OriginalScheduledDeliveryDateTime,     
 CASE WHEN FTTR.SOURCE_SYSTEM_KEY IN (1011) THEN  FTTR.LOAD_LATEST_DELIVERY_DATE 
       ELSE FTTR.ORIGINAL_SCHEDULED_DELIVERY_DATE
  END OriginalScheduledDeliveryDateTime,
  FTTR.UTC_ORIGINAL_SCHEDULED_DELIVERY_DATE UTC_OriginalScheduledDeliveryDateTime,      
  FTTR.SCHEDULED_DELIVERY_DATE ActualScheduledDeliveryDateTime,      
  FTTR.UTC_SCHEDULED_DELIVERY_DATE UTC_ActualScheduledDeliveryDateTime,      
  FTTR.ACTUAL_DELIVERY_DATE ActualDeliveryDate,      
  FTTR.UTC_ACTUAL_DELIVERY_DATE UTC_ActualDeliveryDate,      
  CASE WHEN FTO.IS_INBOUND =1 THEN FTTR.SCHEDULED_SHIPMENT_DATE     
  
    ELSE FTO.DONOT_SHIP_BEFORE_DATE     
  END AS  ScheduleShipmentDate,      
  FTTR.UTC_SCHEDULED_SHIPMENT_DATE UTC_ScheduleShipmentDate,      
  FTTR.ACTUAL_SHIPMENT_DATE ActualShipmentDateTime,      
  FTTR.UTC_ACTUAL_SHIPMENT_DATE UTC_ActualShipmentDateTime,      
    
        CASE WHEN FTO.SOURCE_SYSTEM_NAME LIKE '%SOFTEON%' THEN FTO.BUILDING_CODE ELSE FTO.WAREHOUSE_CODE END AS OrderWarehouse,    
  FTTR.ORIGIN_COMPANY as OriginContactName,      
  FTTR.LOAD_ID,      
  FTTR.SOURCE_ORDER_STATUS,      
  CLIENT_KEY,      
  FTO.WAREHOUSE_KEY_WSE,      
  CASE WHEN FTO.SOURCE_SYSTEM_KEY = 1002 THEN FTO.SERVICE_NAME_SR ELSE FTTR.CARRIER_MODE END AS CARRIER_MODE,        
  FTTR.UPS_ORDER_NUMBER AS TRANSPORT_SHIPMENTNUMBER,      
  FTO.IS_INBOUND,      
  FTTR.SHIPMENT_NOTES,      
  FTO.IS_ASN,      
  FTO.TransactionTypeName,      
  FTTR.GFF_SHIPMENT_NUMBER,      
  FTTR.GFF_SHIPMENT_INSTANCE_NUMBER,    
  FTTR.PROOF_OF_DELIVERY_NAME,    
  FTO.FREIGHT_CARRIER_CODE,    
  FTO.WAYBILL_AIRBILL_NUM,    
  FTTR.EQUIPMENT_TYPE,    
  FTO.ORIGIN_TIME_ZONE as OriginTimeZone,    
  FTO.DESTINATION_TIME_ZONE as DestinationTimeZone,    
  FTO.LOCATION_CODE_DESTINATION as DestinationLocationCode,    
  FTO.LOCATION_CODE_ORIGIN AS OriginLocationCode,    
  FTTR.AUTHORIZER_NAME AS AuthorizerName,    
  FTTR.DELIVERY_INSTRUCTIONS AS DeliveryInstructions,    
  FTTR.DESTINATION_CONTACT AS DestinationContactName,    
  FTTR.LOAD_EARLIEST_PICKUP_DATE AS PickUPDateTime,    
  FTTR.LOAD_LATEST_PICKUP_DATE AS ScheduledPickUpDateTime,    
  FTO.EXT_CUSTOMER_ACCOUNT_NUMBER AS Account_number  ,
   FTO.IS_MANAGED, -- change log
  CASE WHEN mtrd.TemperatureThreshold IS NULL THEN NULL ELSE CONCAT(mtrd.TemperatureThreshold,' ',mtrd.TemperatureRange_UOM) END AS TemperatureThreshold, -- change log
  mtrd.TemperatureRange_Min, -- change log
  mtrd.TemperatureRange_Max, -- change log
  mtrd.TemperatureRange_UOM, -- change log
  FTO.order_sduk,
  concat(FTTR.source_system_key,'||',FTTR.TRANSPORTATION_SDUK) as TRANSPORTATION_SDUK,
  FTO.is_deleted  
  ,FTO.UTC_ORDER_PLACED_MONTH_part_key
FROM fact_order_tv FTO       
    LEFT JOIN {fact_order_reference} FTOR  ON (FTOR.SOURCE_SYSTEM_KEY = FTO.SOURCE_SYSTEM_KEY AND FTOR.UPS_ORDER_NUMBER = FTO.UPS_ORDER_NUMBER AND FTOR.QUERY_SEQUENCE = 2)    --- ---no duplicate  for all source system; verified in production
    LEFT JOIN {fact_transportation} FTTR  ON (CASE WHEN FTTR.TRANS_ONLY_FLAG = 'NON_TRANS' THEN   NVL(FTTR.UPS_WMS_SOURCE_SYSTEM_KEY,FTO.SOURCE_SYSTEM_KEY) ELSE FTTR.SOURCE_SYSTEM_KEY END = FTO.SOURCE_SYSTEM_KEY AND CASE WHEN FTTR.TRANS_ONLY_FLAG  -- has 1 to many relation ship hence bringing its key in the result
 = 'NON_TRANS' THEN FTTR.UPS_WMS_ORDER_NUMBER ELSE FTTR.UPS_ORDER_NUMBER END = FTO.UPS_ORDER_NUMBER)
     LEFT JOIN (select SOURCE_SYSTEM_KEY,UPS_ORDER_NUMBER,EXCEPTION_REASON,row_number() over(partition by SOURCE_SYSTEM_KEY,UPS_ORDER_NUMBER order by UTC_EXCEPTION_CREATED_DATE desc) rn from {fact_transportation_exception} where EXCEPTION_PRIMARY_INDICATOR = 1) FTTE  ON (FTTE.SOURCE_SYSTEM_KEY = FTO.SOURCE_SYSTEM_KEY AND FTTE.UPS_ORDER_NUMBER = FTO.UPS_ORDER_NUMBER) and FTTE.rn = 1
     LEFT JOIN (select distinct CARRIER_NAME,LEVEL_OF_SERVICE_DESC,TemperatureThreshold,TemperatureRange_Min,TemperatureRange_Max,TemperatureRange_UOM
from {dim_carrier_los} cls join {map_temperature_range_details} mp on cls.CARRIER_CODE = mp.CarrierCode and cls.LEVEL_OF_SERVICE_CODE = mp.LevelOfService) mtrd
     ON FTTR.CARRIER_CODE = mtrd.CARRIER_NAME AND FTTR.LEVEL_OF_SERVICE_CODE = mtrd.LEVEL_OF_SERVICE_DESC  -- no data for SPLUS
  """.format(**source_tables)
  logger.debug("query : " + query)
  return (query)

# COMMAND ----------

# DBTITLE 1,Query 5A
def get_query5A(hwm):
  query = """
   CREATE OR REPLACE TEMP VIEW digital_summary_milestone_activity_tv  
    as
  SELECT  FTO.*    
  FROM {digital_summary_milestone_activity} FTO 
  INNER JOIN delta_fetch_tv FTV on (FTO.UPSOrderNumber = FTV.UPS_ORDER_NUMBER)
  where FTO.ActivityDate is not null and ActivityDate >=current_date - {days_back}
  """.format(**source_tables,hwm=hwm,days_back=days_back)
  logger.debug("query : " + query)
  return (query)

# COMMAND ----------

# DBTITLE 1,Query 5
def get_query5():
  query = """
    CREATE OR REPLACE TEMP VIEW Milestone_Activity  
  as
  SELECT SourceSystemKey
	,AccountId
	,UPSOrderNumber
	,MIN(case when CurrentMilestoneFlag = 'Y' then MilestoneOrder else null end) AS MilestoneOrder
	,MAX(case when ActivityName = 'Booking Created' then ActivityDate else null end) AS ShipmentBookedDate 
	,MAX(case when ActivityCode IN ('AG','AB','AA') then ActivityDate else null end) AS ShipmentEstimatedDeliveryDate
	,MAX(case when ActivityCode in ('D','D1','D9')  then ActivityDate else null end) AS ShipmentActualDeliveryDate 
FROM digital_summary_milestone_activity_tv
GROUP BY SourceSystemKey
	,AccountId
	,UPSOrderNumber
  """.format(**source_tables)
  logger.debug("query : " + query)
  return (query)

# COMMAND ----------

# DBTITLE 1,Query 6
def get_query6():
  query = """
    CREATE OR REPLACE TEMP VIEW Milestone  
  as
SELECT SMA.SourceSystemKey,SMA.AccountId,SMA.UPSOrderNumber,MAX(SMA.MilestoneName) CurrentMilestoneName, MAX(SMA.MilestoneDate) CurrentMilestoneDate      
     
FROM Milestone_Activity MA      
INNER JOIN digital_summary_milestone_activity_tv  SMA       
ON MA.SourceSystemKey = SMA.SourceSystemKey AND MA.AccountId = SMA.AccountId     
   AND MA.UPSOrderNumber = SMA.UPSOrderNumber  AND MA.MilestoneOrder = SMA.MilestoneOrder
GROUP BY SMA.SourceSystemKey,SMA.AccountId,SMA.UPSOrderNumber  
  """.format(**source_tables)
  logger.debug("query : " + query)
  return (query)

# COMMAND ----------

# DBTITLE 1,Query 7
def get_query7():
  query = """
  SELECT   distinct    
  FTO.AccountId,      
  FTO.FacilityId,  
  FTO.DP_SERVICELINE_KEY,      
  FTO.DP_ORGENTITY_KEY,
  FTO.UPSOrderNumber,      
  FTO.OrderNumber,      
  FTO.ReferenceOrder,      
  FTO.CustomerPO,      
  FTO.DateTimeReceived,      
  FTO.UTC_DateTimeReceived,      
  FTO.LatestStatusDate,      
  FTO.UTC_LatestStatusDate,      
  FTO.OrderCancelledFlag,      
  FTO.DateTimeCancelled,      
  FTO.UTC_DateTimeCancelled,      
  FTO.DateTimeShipped,      
  FTO.UTC_DateTimeShipped,      
  FTO.RouteCode,      
  FTO.ClientOrderType,      
  FTO.CancelledReasonCode,      
  FTO.LogiNext_OrderFlag,      
  FTO.LogiNext_OrderCurrentSegment,      
  FTO.OrderStatusName,      
  FTO.ServiceLevel,      
  FTO.Carrier,      
  FTO.ServiceLevelCode,      
  FTO.CarrierCode,      
  FTO.ConsigneeName,      
  FTO.OriginAddress1,      
  FTO.OriginAddress2,      
  FTO.OriginCity,      
  FTO.OriginProvince,      
  FTO.OriginPostalCode,      
  FTO.OriginCountry,      
  FTO.DestinationAddress1,      
  FTO.DestinationAddress2,      
  FTO.DestinationCity,      
  FTO.DestinationProvince,      
  FTO.DestinationPostalcode,      
  FTO.DestinationCountry,      
  FTO.OrderType,      
  FTO.SourceSystemKey,      
  FTO.SourceSystemName,      
  FTO.ShipmentCount,      
  FTO.IsSTO,      
  FTO.TrackingNo,      
  FTO.TodayFlag,      
  FTO.ORDER_REF_1_LABEL,      
  FTO.ORDER_REF_1_VALUE,      
  FTO.ORDER_REF_2_LABEL,      
  FTO.ORDER_REF_2_VALUE,      
  FTO.ORDER_REF_3_LABEL,      
  FTO.ORDER_REF_3_VALUE,      
  FTO.ORDER_REF_4_LABEL,      
  FTO.ORDER_REF_4_VALUE,      
  FTO.ORDER_REF_5_LABEL,      
  FTO.ORDER_REF_5_VALUE,      
  FTO.ORDER_REF_6_LABEL,      
  FTO.ORDER_REF_6_VALUE,      
  FTO.ORDER_REF_7_LABEL,      
  FTO.ORDER_REF_7_VALUE,      
  FTO.ORDER_REF_8_LABEL,      
  FTO.ORDER_REF_8_VALUE,      
  FTO.ORDER_REF_9_LABEL,      
  FTO.ORDER_REF_9_VALUE,      
  FTO.ORDER_REF_10_LABEL,      
  FTO.ORDER_REF_10_VALUE,      
  FTO.ORDER_REF_11_LABEL,      
  FTO.ORDER_REF_11_VALUE,      
  FTO.ORDER_REF_12_LABEL,      
  FTO.ORDER_REF_12_VALUE,      
  FTO.ORDER_REF_13_LABEL,      
  FTO.ORDER_REF_13_VALUE,      
  FTO.ORDER_REF_14_LABEL,      
  FTO.ORDER_REF_14_VALUE,      
  FTO.ORDER_REF_15_LABEL,      
  FTO.ORDER_REF_15_VALUE,      
  FTO.TransOnly,      
  FTO.TransMilestone,      
  FTO.ExceptionCode,      
  FTO.OriginalScheduledDeliveryDateTime,      
  FTO.ActualScheduledDeliveryDateTime,      
  FTO.ActualDeliveryDate,      
  FTO.ScheduleShipmentDate,      
  FTO.ActualShipmentDateTime,      
  FTO.UTC_OriginalScheduledDeliveryDateTime,      
  FTO.UTC_ActualScheduledDeliveryDateTime,      
  FTO.UTC_ActualDeliveryDate,      
  FTO.UTC_ScheduleShipmentDate,      
  FTO.UTC_ActualShipmentDateTime,      
  FTO.OrderWarehouse,      
  FTOL.ORDER_LINE_COUNT OrderLineCount,      
  FTO.OriginContactName,      
  FTO.LOAD_ID,      
  FTO.SOURCE_ORDER_STATUS,      
  CAST(NULL as string) as TRANS_MILESTONE,      
  FTO.CARRIER_MODE ServiceMode,      
  TRANSPORT_SHIPMENTNUMBER UPSTransportShipmentNumber,      
  ML.CurrentMilestoneName CurrentMilestone,      
  ML.CurrentMilestoneDate,      
  IS_INBOUND,      
  SHIPMENT_NOTES,      
  FTO.IS_ASN,      
  FTO.TransactionTypeName,      
  MLB.ShipmentBookedDate,      
  FTO.GFF_SHIPMENT_NUMBER GFF_ShipmentNumber,      
  FTO.GFF_SHIPMENT_INSTANCE_NUMBER GFF_ShipmentInstanceId,    
  FTO.FREIGHT_CARRIER_CODE Freight_Carriercode,    
  FTO.WAYBILL_AIRBILL_NUM,    
  FTO.PROOF_OF_DELIVERY_NAME,    
  FTO.EQUIPMENT_TYPE EquipmentType,    
  FTO.OriginTimeZone,    
  FTO.DestinationTimeZone,    
  FTO.DestinationLocationCode,    
  FTO.DestinationTimeZone ActualScheduledDeliveryDateTimeZone,    
  FTO.OriginTimeZone ShippedDateTimeZone,    
  FTO.OriginLocationCode,    
  FTO.AuthorizerName,    
  FTO.DeliveryInstructions,    
  FTO.DestinationContactName,    
  FTO.PickUPDateTime,    
  FTO.ScheduledPickUpDateTime,    
  FTO.Account_number,    
  --MLB.ShipmentEstimatedDeliveryDate EstimatedDeliveryDateTime, --change log 
 CASE WHEN FTO.SourceSystemKey = 1002 THEN FTO.ActualScheduledDeliveryDateTime ELSE MLB.ShipmentEstimatedDeliveryDate END AS EstimatedDeliveryDateTime, --change log
  MLB.ShipmentActualDeliveryDate ActualDeliveryDateTime,
  FTO.is_managed,
  FTO.TemperatureThreshold,
  FTO.TemperatureRange_Min,
  FTO.TemperatureRange_Max,
  FTO.TemperatureRange_UOM,
  0 Is_healthcare,
  FTO.order_sduk,
  FTO.TRANSPORTATION_SDUK,
  FTO.is_deleted,
  FTO.UTC_ORDER_PLACED_MONTH_part_key,
  row_number() over (PARTITION BY FTO.SourceSystemKey,FTO.UPSOrderNumber,FTO.order_sduk 
                             ORDER BY FTO.TRANSPORTATION_SDUK NULLS FIRST
                             ) as transport_rn
FROM SUMMARY_ORDERS FTO      
LEFT JOIN FTOL FTOL ON (FTO.SourceSystemKey = FTOL.SOURCE_SYSTEM_KEY AND FTO.UPSOrderNumber = FTOL.UPS_ORDER_NUMBER)       
LEFT JOIN Milestone ML ON (FTO.SourceSystemKey = ML.SourceSystemKey AND  FTO.AccountId = ML.AccountId AND FTO.UPSOrderNumber = ML.UPSOrderNumber)      
LEFT JOIN Milestone_Activity MLB ON (FTO.SourceSystemKey = MLB.SourceSystemKey AND  FTO.AccountId = MLB.AccountId AND FTO.UPSOrderNumber = MLB.UPSOrderNumber)      
  """
  logger.debug("query : " + query)
  return (query)

# COMMAND ----------

# DBTITLE 1,Main Function
def main():
    try:
        pid_get = get_pid()
        logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
        pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
        logger.info("pid: {pid}".format(pid=pid))
        ############################ ETL AUDIT #########################################################
        audit_result['process_id'] = pid
        audit_result['process_name'] = 'pr_load_summary_orders'
        audit_result['process_type'] = 'DataBricks'
        audit_result['layer'] = 'gold'
        audit_result['table_name'] = digital_summary_orders_et
        audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
        audit_result['start_time'] = start_time
        ################################################################################################
        hwm=get_hwm('gold',digital_summary_orders_et)
        logger.info(f'hwm {digital_summary_orders_et}: {hwm}')
        
        spark.sql(get_delta_query(hwm))
        logger.info("get_delta_query finished")   
            
        spark.sql(get_query1(hwm))
        logger.info("query 1 finished")
              
        spark.sql(get_query2())
        logger.info("query 2 finished")
        
        spark.sql(get_query4())
        logger.info("query 4 finished")
        
        spark.sql(get_query5A(hwm))
        logger.info("query 5A finished")
        
        spark.sql(get_query5())
        logger.info("query 5 finished")
        
        spark.sql(get_query6())
        logger.info("query 6 finished")
        
        
        src_df = spark.sql(get_query7())
        logger.info("query 7 finished")
  
        ###################### generating hash key  #############################
        hash_key_columns = ['SourceSystemKey','UPSOrderNumber','order_sduk','transport_rn']
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
  
        primary_keys = ['SourceSystemKey','UPSOrderNumber','order_sduk','transport_rn']
        logger.debug('primary_keys: {primary_keys}'.format(primary_keys=primary_keys))
  
        logger.info(f'Merging to delta path: {digital_summary_orders_path}')
  
        mergeToDelta(src_df,digital_summary_orders_path,primary_keys)
        logger.info(f'merging to delta path finished: {digital_summary_orders_path}')
        
        logger.info('setting hwm')
        res=set_hwm('gold',digital_summary_orders_et,start_time,pid)
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