# Databricks notebook source
"""
Author           : Prashant Gupta
Description      : this notebook is to load digital_summary_orders cosmos container.
verision 1.1     : updated trans_missed_delivery_movement AS logic as per the ticket UPSGLD-15476 
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

# DBTITLE 1,Cosmos connection
scope = 'key-vault-secrets'
cosmosEndpoint = dbutils.secrets.get(scope,"cosmosEndpoint")
cosmosMasterKey = dbutils.secrets.get(scope,"cosmosMasterKey")
cosmosDatabaseName = "SAIL"
cosmosContainerName = "digital_summary_orders"

cfg = {
  "spark.cosmos.accountEndpoint" : cosmosEndpoint,
  "spark.cosmos.accountKey" : cosmosMasterKey,
  "spark.cosmos.database" : cosmosDatabaseName,
  "spark.cosmos.container" : cosmosContainerName,
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
    logger = _get_logger(time_zone,logging.DEBUG)  
else:
    logger = _get_logger(time_zone,logging.INFO)
  

# COMMAND ----------

def get_delta_query(hwm):
    logger.debug("hwm: " + str(hwm))
    query ="""
CREATE
	OR replace TEMP VIEW digital_summary_orders_vw AS
	WITH change AS (
			SELECT UPSOrderNumber
			FROM {digital_summary_orders}
			WHERE dl_update_timestamp >= '{hwm}'
            
			UNION
			
			SELECT UPSORDERNUMBER
			FROM {digital_summary_transportation_callcheck}
			WHERE dl_update_timestamp >= '{hwm}'
			
			UNION
			
			SELECT CASE 
					WHEN TrasOnlyFlag <> 'TRANS_ONLY'
						THEN UpsWMSOrderNumber
					ELSE UpsOrderNumber
					END UPSOrderNumber
			FROM {digital_summary_transportation}
			WHERE dl_update_timestamp >= '{hwm}'
            
			UNION
			
			SELECT UPSOrderNumber
			FROM {digital_summary_transportation_references}
			WHERE dl_update_timestamp >= '{hwm}'
            
            UNION
			
			SELECT UPSOrderNumber
			FROM {digital_summary_milestone}
			WHERE dl_update_timestamp >= '{hwm}'
            
			UNION
			
			SELECT UPSOrderNumber
			FROM {digital_summary_milestone_activity}
			WHERE dl_update_timestamp >= '{hwm}'
			
            
			UNION
			
			SELECT UPSOrderNumber
			FROM {digital_summary_transportation_rates_charges}
			WHERE dl_update_timestamp >= '{hwm}'
			
			UNION
			
			SELECT UPSOrderNumber AS ups_order_number
			FROM {digital_summary_order_lines}
			WHERE dl_update_timestamp >= '{hwm}'
            
			
			UNION
			
			SELECT UPSOrderNumber AS ups_order_number
			FROM {digital_summary_order_tracking}
			WHERE dl_update_timestamp >= '{hwm}'
            
			
			UNION
			
			SELECT UPSOrderNumber AS ups_order_number
			FROM {digital_summary_exceptions}
			WHERE dl_update_timestamp >= '{hwm}'
			)
 
SELECT o.*,row_number() over(partition by 
  o.AccountId,      
  o.FacilityId,  
  o.DP_SERVICELINE_KEY,      
  o.DP_ORGENTITY_KEY,
  o.UPSOrderNumber,      
  o.OrderNumber,      
  o.ReferenceOrder,      
  o.CustomerPO,      
  o.DateTimeReceived,      
  o.UTC_DateTimeReceived,      
  o.LatestStatusDate,      
  o.UTC_LatestStatusDate,      
  o.OrderCancelledFlag,      
  o.DateTimeCancelled,      
  o.UTC_DateTimeCancelled,      
  o.DateTimeShipped,      
  o.UTC_DateTimeShipped,      
  o.RouteCode,      
  o.ClientOrderType,      
  o.CancelledReasonCode,      
  o.LogiNext_OrderFlag,      
  o.LogiNext_OrderCurrentSegment,      
  o.OrderStatusName,      
  o.ServiceLevel,      
  o.Carrier,      
  o.ServiceLevelCode,      
  o.CarrierCode,      
  o.ConsigneeName,      
  o.OriginAddress1,      
  o.OriginAddress2,      
  o.OriginCity,      
  o.OriginProvince,      
  o.OriginPostalCode,      
  o.OriginCountry,      
  o.DestinationAddress1,      
  o.DestinationAddress2,      
  o.DestinationCity,      
  o.DestinationProvince,      
  o.DestinationPostalcode,      
  o.DestinationCountry,      
  o.OrderType,      
  o.SourceSystemKey,      
  o.SourceSystemName,      
  o.ShipmentCount,      
  o.IsSTO,      
  o.TrackingNo,      
  o.TodayFlag,      
  o.ORDER_REF_1_LABEL,      
  o.ORDER_REF_1_VALUE,      
  o.ORDER_REF_2_LABEL,      
  o.ORDER_REF_2_VALUE,      
  o.ORDER_REF_3_LABEL,      
  o.ORDER_REF_3_VALUE,      
  o.ORDER_REF_4_LABEL,      
  o.ORDER_REF_4_VALUE,      
  o.ORDER_REF_5_LABEL,      
  o.ORDER_REF_5_VALUE,      
  o.ORDER_REF_6_LABEL,      
  o.ORDER_REF_6_VALUE,      
  o.ORDER_REF_7_LABEL,      
  o.ORDER_REF_7_VALUE,      
  o.ORDER_REF_8_LABEL,      
  o.ORDER_REF_8_VALUE,      
  o.ORDER_REF_9_LABEL,      
  o.ORDER_REF_9_VALUE,      
  o.ORDER_REF_10_LABEL,      
  o.ORDER_REF_10_VALUE,      
  o.ORDER_REF_11_LABEL,      
  o.ORDER_REF_11_VALUE,      
  o.ORDER_REF_12_LABEL,      
  o.ORDER_REF_12_VALUE,      
  o.ORDER_REF_13_LABEL,      
  o.ORDER_REF_13_VALUE,      
  o.ORDER_REF_14_LABEL,      
  o.ORDER_REF_14_VALUE,      
  o.ORDER_REF_15_LABEL,      
  o.ORDER_REF_15_VALUE,      
  o.TransOnly,      
  o.TransMilestone,      
  o.ExceptionCode,      
  o.OriginalScheduledDeliveryDateTime,      
  o.ActualScheduledDeliveryDateTime,      
  o.ActualDeliveryDate,      
  o.ScheduleShipmentDate,      
  o.ActualShipmentDateTime,      
  o.UTC_OriginalScheduledDeliveryDateTime,      
  o.UTC_ActualScheduledDeliveryDateTime,      
  o.UTC_ActualDeliveryDate,      
  o.UTC_ScheduleShipmentDate,      
  o.UTC_ActualShipmentDateTime,      
  o.OrderWarehouse,      
  o.OrderLineCount,      
  o.OriginContactName,      
  o.LOAD_ID,      
  o.SOURCE_ORDER_STATUS,      
  o.TRANS_MILESTONE,      
  o.ServiceMode,      
  o.UPSTransportShipmentNumber,      
  o.CurrentMilestone,      
  o.CurrentMilestoneDate,      
  o.IS_INBOUND,      
  o.SHIPMENT_NOTES,      
  o.IS_ASN,      
  o.TransactionTypeName,      
  o.ShipmentBookedDate,      
  o.GFF_ShipmentNumber,      
  o.GFF_ShipmentInstanceId,    
  o.Freight_Carriercode,    
  o.WAYBILL_AIRBILL_NUM,    
  o.PROOF_OF_DELIVERY_NAME,    
  o.EquipmentType,    
  o.OriginTimeZone,    
  o.DestinationTimeZone,    
  o.DestinationLocationCode,    
  o.ActualScheduledDeliveryDateTimeZone,    
  o.ShippedDateTimeZone,    
  o.OriginLocationCode,    
  o.AuthorizerName,    
  o.DeliveryInstructions,    
  o.DestinationContactName,    
  o.PickUPDateTime,    
  o.ScheduledPickUpDateTime,    
  o.Account_number,    
  o.EstimatedDeliveryDateTime,    
  o.ActualDeliveryDateTime,
  o.Is_healthcare,
  o.is_managed
  order by o.dl_update_timestamp desc) as rn
FROM {digital_summary_orders} o
INNER JOIN {digital_summary_onboarded_systems} OS ON OS.sourcesystemkey = o.SourceSystemKey
INNER JOIN (
	SELECT DISTINCT UPSOrderNumber
	FROM change
	) c ON o.UPSOrderNumber = c.UPSOrderNumber
    where o.DateTimeReceived >= case when date('{hwm}') = '1900-01-01' then current_date else  date('{hwm}') end - {days_back}
    --and AccountId in {account_id}
        """.format(**source_tables,hwm=hwm,days_back=days_back,account_id=account_id)
    logger.debug("query : " + query)
    return(query)

# COMMAND ----------

def get_delta_query2(hwm):
    logger.debug("hwm: " + str(hwm))
    query ="""
CREATE
	OR replace TEMP VIEW digital_summary_milestone_activity_vw AS
    select * from  {digital_summary_milestone_activity} MA
			WHERE MA.is_deleted = 0 and MA.ActivityDate between case when date('{hwm}') = '1900-01-01' then current_date else date('{hwm}') end - {days_back}
            and case when date('{hwm}') = '1900-01-01' then current_date else date('{hwm}') end + {days_back}
    """.format(**source_tables,hwm=hwm,days_back=days_back,account_id=account_id)
    logger.debug("query : " + query)
    return(query)

# COMMAND ----------

def get_pre_cosmos_query(hwm):
    query = """WITH last_location AS (
			SELECT MA.UPSOrderNumber
				,MA.SourceSystemKey
				,MA.ACTIVITY_NOTES
				,MA.ActivityDate
				,ROW_NUMBER() OVER (
					PARTITION BY UPSOrderNumber ORDER BY ActivityDate DESC
					) ROWNUM
			FROM {digital_summary_milestone_activity_vw} MA
			WHERE MA.is_deleted = 0 and MA.ACTIVITY_NOTES IS NOT NULL
				AND MA.ActivityCode NOT IN (
					'AB'
					,'E'
					)
			)
		,last_location_wn_nt_delivered AS (
			SELECT * FROM (
			SELECT MA.UPSOrderNumber
				,MA.SourceSystemKey
				,MA.ACTIVITY_NOTES
				,MA.ActivityDate
				,ROW_NUMBER() OVER (PARTITION BY UPSOrderNumber ORDER BY ActivityDate DESC) RN
			FROM {digital_summary_milestone_activity_vw} MA
			ANTI JOIN (SELECT distinct UPSOrderNumber FROM {digital_summary_milestone_activity_vw} 
			WHERE is_deleted = 0 and ActivityCode IN ('D1','DELIVER')) D ON MA.UPSOrderNumber = D.UPSOrderNumber
			WHERE MA.is_deleted = 0 and MA.ACTIVITY_NOTES IS NOT NULL
				AND MA.ActivityCode NOT IN ('AB','E')) WHERE RN = 1
			)
		,shipunit_references AS (
			SELECT UPSOrderNumber
					,SourceSystemKey
					,collect_set(named_struct('referenceType', nvl(TR.ReferenceType, ''), 'referenceValue', nvl(TR.ReferenceValue, ''))) AS shipunit_reference
			FROM {digital_summary_transportation_references} TR
			WHERE ReferenceLevel = 'shipunit_reference' and TR.is_deleted = 0
			GROUP BY UPSOrderNumber
					,SourceSystemKey
			)
        ,shipitem_references AS (
			SELECT UPSOrderNumber
					,SourceSystemKey
					,collect_set(named_struct('referenceType', nvl(TR.ReferenceType, ''), 'referenceValue', nvl(TR.ReferenceValue, ''))) AS shipitem_reference
			FROM {digital_summary_transportation_references} TR
			WHERE ReferenceLevel = 'shipitem_reference' and TR.is_deleted = 0
			GROUP BY UPSOrderNumber
					,SourceSystemKey
			)
		,PickUpDate AS (
			SELECT ma.UPSOrderNumber,MAX(ma.ActivityDate)  as PickUpDate
			FROM {digital_summary_milestone_activity_vw} MA
			WHERE MA.is_deleted = 0 and  MA.ActivityCode in('AM','AF','CP')   and MA.SourceSystemKey = 1011
			group by ma.UPSOrderNumber
			)
		,total_invoice_charge AS (
			SELECT UpsOrderNumber
				,SourceSystemKey
				,SUM(CAST(P.CHARGE AS DECIMAL(10, 2))) AS totalCharge
				,CurrencyCode AS totalcurency
			FROM {digital_summary_transportation_rates_charges} p
			WHERE P.ChargeLevel = 'CUSTOMER_INVOICE' and p.is_deleted = 0
			GROUP BY UpsOrderNumber
				,SourceSystemKey
				,CurrencyCode
			)
		,total_charge AS (
			SELECT UpsOrderNumber
				,SourceSystemKey
				,SUM(CAST(P.CHARGE AS DECIMAL(10, 2))) AS totalCharge
				,CurrencyCode AS totalcurency
			FROM {digital_summary_transportation_rates_charges} p
			WHERE P.ChargeLevel = 'CUSTOMER_RATES' and p.is_deleted = 0
			GROUP BY UpsOrderNumber
				,SourceSystemKey
				,CurrencyCode
			)
		,order_line AS (
			SELECT  
                max(case when nvl(OL.ShipmentLineCanceledFlag, 'Y') = 'Y' then nvl(OL.ShipmentLineCanceledFlag, 'Y') else null end ) as ShipmentLineCanceledFlag
				,OL.UPSOrderNumber
				,OL.SourceSystemKey
				,max(case when nvl(OL.ShipmentLineCanceledFlag, 'Y') = 'Y' then OL.ShipmentLineCanceledDate else null end) as ShipmentLineCanceledDate
                ,SUM(OL.SKUQuantity) SKUQuantity_sum
			FROM {digital_summary_order_lines} OL
			JOIN {digital_summary_orders_vw} O ON O.UPSOrderNumber = OL.UPSOrderNumber
				AND O.SourceSystemKey = OL.SourceSystemKey and ol.is_deleted = 0
			--WHERE nvl(OL.ShipmentLineCanceledFlag, 'Y') = 'Y'
            group by OL.UPSOrderNumber
				,OL.SourceSystemKey
			)
        ,inbound_line2 as (
        SELECT DISTINCT
O.UPSOrderNumber,
o.SourceSystemKey,
IL.UPSOrderNumber AS TransportOrderNumber
FROM {digital_summary_orders_vw} o
JOIN {digital_summary_inbound_line} IL ON O.OrderNumber = IL.ClientASNNumber
WHERE IL.SourceSystemKey='1011' and il.is_deleted = 0
        )
        ,inbound_line_sil AS (
			select sil.UPSOrderNumber
				,sil.SourceSystemKey
				,collect_set(named_struct('UPSASNNumber' , nvl(sil.UPSASNNumber, '') , 'inbound_line_count' , nvl(sil.inbound_line_count, ''),'inbound_ShippedQuantity_sum' , nvl(sil.inbound_ShippedQuantity_sum, ''),'inbound_cases_sum' , nvl(sil.inbound_cases_sum, '') )) AS inbound_shipment_listing
				from 
			(
			SELECT  
                OL.UPSOrderNumber
				,OL.SourceSystemKey
				,ol.UPSASNNumber
				,COUNT(ol.UPSOrderNumber) AS inbound_line_count
                ,SUM(OL.ShippedQuantity) inbound_ShippedQuantity_sum
                ,SUM(nvl(ol.CASES,0)) inbound_cases_sum  -- replace null with ol.CASES
			FROM {digital_summary_inbound_line} OL
			JOIN {digital_summary_orders_vw} O ON O.UPSOrderNumber = OL.UPSOrderNumber
				AND O.SourceSystemKey = OL.SourceSystemKey
			where O.IS_INBOUND = 1 and ol.is_deleted = 0
            group by OL.UPSOrderNumber
				,OL.SourceSystemKey
				,ol.UPSASNNumber
			) sil
			group by sil.UPSOrderNumber
				,sil.SourceSystemKey
			) 
        ,inbound_line AS (
			SELECT  
                OL.UPSOrderNumber
				,OL.SourceSystemKey
				,COUNT(ol.UPSOrderNumber) AS inbound_line_count
                ,SUM(OL.ShippedQuantity) inbound_ShippedQuantity_sum
                ,SUM(nvl(ol.CASES,0)) inbound_cases_sum  -- replace null with ol.CASES
			FROM {digital_summary_inbound_line} OL
			JOIN {digital_summary_orders_vw} O ON O.UPSOrderNumber = OL.UPSOrderNumber
				AND O.SourceSystemKey = OL.SourceSystemKey
			where O.IS_INBOUND = 1 and ol.is_deleted = 0
            group by OL.UPSOrderNumber
				,OL.SourceSystemKey
			)
		,tracking AS (
			SELECT SOT.UPSOrderNumber
				,COUNT(SOT.TRACKING_NUMBER) CarrierShipmentCount
				,collect_set(nvl(SOT.TRACKING_NUMBER,"")) AS TRACKING_NUMBER_LIST
				,collect_set(named_struct('ShipmentDimensions' , nvl(SOT.ShipmentDimensions, '') , 'ShipmentWeight' , nvl(SOT.ShipmentWeight, '') , 'Tracking_Number' , nvl(SOT.TRACKING_NUMBER, '') , 'CarrierCode' , nvl(SOT.CarrierCode, '') , 'CarrierType' , nvl(SOT.CarrierType, ''), 'carrierName' , nvl(o.Carrier, '') , 'ShipmentDimensions_UOM' , nvl(SOT.ShipmentDimensions_UOM, '') , 'ShipmentWeight_UOM' , nvl(SOT.ShipmentWeight_UOM, '') , 'LOAD_AREA' , nvl(SOT.LOAD_AREA, '') , 'ShipmentDescription' , nvl(SOT.SHIPMENT_DESCRIPTION, ''), 'ShipmentQuantity' , nvl(CAST(SOT.SHIPMENT_QUANTITY AS INT), 0), 'UnitOfMeasurement' , nvl(SOT.UOM, ''), 'TempRangeMin' , nvl(SOT.TemperatureRange_Min, ''), 'TempRangeMax' , nvl(SOT.TemperatureRange_Max, '') )) tracking
			FROM {digital_summary_order_tracking} SOT
			JOIN {digital_summary_orders_vw} O ON O.UPSOrderNumber = SOT.UPSOrderNumber
			WHERE nvl(SOT.TRACKING_NUMBER, '') NOT IN (
					''
					,' '
					) and sot.is_deleted = 0
			GROUP BY sot.UPSOrderNumber
			)
		,max_activity AS (
			SELECT MAX(ActivityDate) AS ActivityDate
				,O.UPSOrderNumber AS UPSOrderNumber
				,O.SourceSystemKey
                ,max(case when MA.ActivityCode IN (
					'D'
					,'D1'
					,'D9'
					,'DELIVER'
					) then 1 else 0 end) as activity_flag
				,max(case when MA.ActivityCode IN (
					'AG','AB','AA'
					) then ma.ActivityDate else null end) as estimatedDeliveryDateTime
				,max(case when MA.CurrentMilestoneFlag = 'Y' then ma.ActivityDate else null end) as ActivityDate_CurrMile
				,max(case when MA.CurrentMilestoneFlag = 'Y' then ma.MilestoneOrder else null end) as MilestoneOrder_CurrMile
				,max(case when ActivityCode = 'CC' then date_format(ma.ActivityDate , 'yyyy-MM-dd HH:mm:ss.SSS') else null end) as claimClosureDateTime
				,max(case when ActivityCode in ('D','D1','D9') then ma.ActivityDate else null end) as actualDeliveryDateTime_Movement
                ,max(case when ActivityCode in ('D','D1','D9') then 1 else 0 end) as activity_Movement_flag
                ,max(case when ma.ActivityCode='COSD' then ma.ACTIVITY_NOTES else null end ) as claimStatus
                ,max(case when ma.ActivityCode='COSD' then ma.ACTIVITY_STATUS else null end ) as claimStatus_movement
                ,max(case when upper(ma.ActivityCode) = 'DELIVER' and ma.ActivityDate is not null then 1 else 0 end ) as ActivityCodeDelivery_status
                ,max(case when ma.ActivityCode IN ('D','D1','D9','DELIVER','155') then ma.ActivityDate else null end ) as m1_ActualDeliveryDateTime
                ,max(case when ma.ActivityCode IN ('AG','AB','AA','071') then ma.ActivityDate else null end ) as ma_shipmentEstimatedDateTime
                ,max(case when ma.ActivityCode IN ('D','D1','D9') then o.actualDeliveryDateTime else null end ) as ma_ShipmentDeliveryDate
                ,max(case when ma.ActivityCode IN ('RECS' , 'REC30' , 'REC90') then ma.ActivityDate else null end ) as ma_shipmentReceivedDate
                ,max(case when MA.MilestoneName <> 'PUTAWAY' AND O.IS_INBOUND = 1 AND O.IS_ASN = 1 AND nvl(O.OrderCancelledFlag,'N') = 'N' AND MA.CurrentMilestoneFlag = 'Y' then ma.ActivityDate else null end ) as ma_ActivityDate_1
                ,max(case when  MA.ActivityCode in ('RECS' , 'REC30' , 'REC90') and MA.MilestoneName <> 'PUTAWAY' AND O.IS_INBOUND = 1 AND O.IS_ASN = 1 AND nvl(O.OrderCancelledFlag,'N') = 'N' AND MA.CurrentMilestoneFlag = 'Y' then ma.ActivityDate else null end ) as ma_ActivityDate_2
                ,max(case when ma.ActivityCode IN('D','D1','D9') then ma.PROOF_OF_DELIVERY_NAME else null end ) as proof_of_delivery_name -- change log
				,max(case when ma.ActivityCode IN('D','D1','D9') then ma.PROOF_OF_DELIVERY_DATE_TIME else null end ) as proof_of_delivery_date_time -- change log
			FROM {digital_summary_orders_vw} O
			LEFT JOIN {digital_summary_milestone_activity_vw} MA ON O.UPSOrderNumber = MA.UPSOrderNumber
				AND O.SourceSystemKey = MA.SourceSystemKey and MA.is_deleted = 0
			GROUP BY O.UPSOrderNumber
				,O.SourceSystemKey
			)
		,max_exception AS (
			SELECT EX.OTZ_ExceptionCreatedDate AS ExceptionCreatedDate
				,O.UPSTransportShipmentNumber AS UPSOrderNumber
				,EX.SourceSystemKey
				,ex.ExceptionReason AS exceptionReason
				,MMA.ActivityName AS exceptionType
				,rank() OVER (
					PARTITION BY O.UPSTransportShipmentNumber
					,EX.SourceSystemKey ORDER BY EX.OTZ_ExceptionCreatedDate DESC
					) rn
			FROM {digital_summary_orders_vw} o
			INNER JOIN {digital_summary_exceptions} EX ON O.UPSTransportShipmentNumber = EX.UPSOrderNumber and ex.is_deleted = 0
			LEFT JOIN {map_milestone_activity} MMA ON MMA.ActivityCode = EX.ExceptionEvent
			INNER JOIN max_activity MA ON o.UPSOrderNumber = MA.UPSOrderNumber
				AND o.SourceSystemKey = MA.SourceSystemKey and MA.activity_flag = 0 
			
			UNION
			
			SELECT EX.UTC_ExceptionCreatedDate AS ExceptionCreatedDate
				,O.UPSOrderNumber AS UPSOrderNumber
				,EX.SourceSystemKey
				,ex.ExceptionReason AS exceptionReason
				,MMA.ActivityName AS exceptionType
				,rank() OVER (
					PARTITION BY O.UPSOrderNumber
					,EX.SourceSystemKey ORDER BY EX.UTC_ExceptionCreatedDate DESC
					) rn
			FROM {digital_summary_orders_vw} O
			INNER JOIN {digital_summary_exceptions} EX ON O.UPSOrderNumber = EX.UPSOrderNumber
			LEFT JOIN {map_milestone_activity} MMA ON MMA.ActivityCode = EX.ExceptionEvent
			INNER JOIN max_activity MA ON o.UPSOrderNumber = MA.UPSOrderNumber 
				AND o.SourceSystemKey = MA.SourceSystemKey and MA.activity_flag = 0 
			WHERE EX.SourceSystemKey = 1019 and ex.is_deleted = 0
			)
        ,max_exception2 AS (
			SELECT EX.OTZ_ExceptionCreatedDate AS ExceptionCreatedDate
				,O.UPSTransportShipmentNumber AS UPSOrderNumber
				,EX.SourceSystemKey
				,ex.ExceptionReason AS exceptionReason
				,MMA.ActivityName AS exceptionType
				,rank() OVER (
					PARTITION BY O.UPSTransportShipmentNumber
					,EX.SourceSystemKey ORDER BY EX.OTZ_ExceptionCreatedDate DESC
					) rn
			FROM {digital_summary_orders_vw} o
			INNER JOIN {digital_summary_exceptions} EX ON O.UPSTransportShipmentNumber = EX.UPSOrderNumber and ex.is_deleted = 0
			LEFT JOIN {map_milestone_activity} MMA ON MMA.ActivityCode = EX.ExceptionEvent
            
			UNION
			
			SELECT EX.UTC_ExceptionCreatedDate AS ExceptionCreatedDate
				,O.UPSOrderNumber AS UPSOrderNumber
				,EX.SourceSystemKey
				,ex.ExceptionReason AS exceptionReason
				,MMA.ActivityName AS exceptionType
				,rank() OVER (
					PARTITION BY O.UPSOrderNumber
					,EX.SourceSystemKey ORDER BY EX.UTC_ExceptionCreatedDate DESC
					) rn
			FROM {digital_summary_orders_vw} O
			INNER JOIN {digital_summary_exceptions} EX ON O.UPSOrderNumber = EX.UPSOrderNumber and ex.is_deleted = 0
			LEFT JOIN {map_milestone_activity} MMA ON MMA.ActivityCode = EX.ExceptionEvent
			WHERE EX.SourceSystemKey = 1019
			)
        ,exception2 AS (
			SELECT UPSOrderNumber
				,SourceSystemKey
				--,exceptionReason
				-- ,exceptionType
				,collect_set(named_struct('exceptionReason' , nvl(me.exceptionReason, '') , 'exceptionType' , nvl(me.exceptionType, '') )) AS exception_list
			FROM max_exception2 me
			WHERE rn = 1
			GROUP BY UPSOrderNumber
				,SourceSystemKey
			)
		,exception AS (
			SELECT UPSOrderNumber
				,SourceSystemKey
				--,exceptionReason
				-- ,exceptionType
				,collect_set(named_struct('exceptionReason' , nvl(me.exceptionReason, '') , 'exceptionType' , nvl(me.exceptionType, '') )) AS exception_list
			FROM max_exception me
			WHERE rn = 1
			GROUP BY UPSOrderNumber
				,SourceSystemKey
			)
		,exception_list AS (
			SELECT UPSOrderNumber
				,SourceSystemKey
				,collect_set(named_struct('exceptionType' , nvl(ExceptionType, '') ,'ExceptionReasonType' , nvl(ExceptionReasonType, '') , 'exceptionReason' , nvl(exceptionReason, ''), 'ExceptionPrimaryIndicator' , nvl(ExceptionPrimaryIndicator, ''), 'ExceptionCategory' , nvl(ExceptionCategory, ''), 'OTZ_ExceptionCreatedDate' , nvl(OTZ_ExceptionCreatedDate, ''),'UTC_ExceptionCreatedDate' , nvl(UTC_ExceptionCreatedDate, ''))) AS exception_list
			FROM {digital_summary_exceptions}
            where is_deleted = 0
			GROUP BY UPSOrderNumber
				,SourceSystemKey
			)
		,all_act AS (
			SELECT MAX(MA.ActivityDate) AS ActivityDate
				,O.UPSOrderNumber
				,O.SourceSystemKey
			FROM {digital_summary_orders_vw} O
			INNER JOIN {digital_summary_milestone_activity_vw} MA ON O.UPSOrderNumber = MA.UPSOrderNumber
				AND O.SourceSystemKey = MA.SourceSystemKey
				AND case when o.is_inbound=0 then MA.CurrentMilestoneFlag else 'Y' end = 'Y' 
                and MA.is_deleted = 0 
                and case when o.is_inbound=0 then 'NA' else MA.MilestoneName end  <> 'PUTAWAY'
                and case when o.is_inbound=0 then 1 else o.is_asn end  =1
			GROUP BY O.UPSOrderNumber
				,O.SourceSystemKey
			)
		,detail_milestone AS (
			SELECT AccountId,UPSOrderNumber,DP_SERVICELINE_KEY,
			sort_array(collect_list(named_struct('MilestoneOrder',MilestoneOrder,'ShipmentMileStone',ShipmentMileStones
			  ,'templateType',templateType,'MilesStoneEstimatedDateTime',MilesStoneEstimatedDateTime
			  ,'MilesStoneCompletionDateTime',MilesStoneCompletionDateTime,'activityCount',activityCount))) as DetailMilestone
			from(SELECT MilestoneOrder,ShipmentMileStones,templateType AS templateType,MAX(date_format(MilesStoneEstimatedDateTime, 'yyyy-MM-dd HH:mm:ss.SSS')) MilesStoneEstimatedDateTime,
				MAX(date_format(MilesStoneCompletionDateTime, 'yyyy-MM-dd HH:mm:ss.SSS')) AS MilesStoneCompletionDateTime,MAX(activityCount) AS activityCount,
				AccountId,UPSOrderNumber,DP_SERVICELINE_KEY
				FROM (SELECT M.MilestoneName AS ShipmentMileStones,
				M.MilestoneOrder, MA.PlannedMilestoneDate MilesStoneEstimatedDateTime,
			    CASE WHEN MA.MilestoneCompletionFlag = 'Y' THEN NVL(MA.MilestoneDate, MA.ActivityDate) END MilesStoneCompletionDateTime,
			    NVL(MA1.activityCount, 0) AS activityCount, MTM.TransactionTypeName AS templateType, M.AccountId,M.UPSOrderNumber,M.DP_SERVICELINE_KEY
			  FROM {digital_summary_milestone} M
			  LEFT JOIN {digital_summary_milestone_activity_vw} MA 
					ON M.UPSOrderNumber = MA.UPSOrderNumber	AND M.MilestoneOrder = MA.MilestoneOrder and MA.is_deleted = 0
						AND M.SourceSystemKey = CASE WHEN MA.SourceSystemKey = '1011' THEN M.SourceSystemKey ELSE MA.SourceSystemKey END
			  LEFT JOIN (SELECT COUNT(1) AS activityCount,MA.UPSOrderNumber,MA.SourceSystemKey,MA.MilestoneName FROM {digital_summary_milestone_activity_vw} MA where MA.is_deleted = 0
			    GROUP BY MA.UPSOrderNumber,MA.SourceSystemKey,MA.MilestoneName) MA1 
			    ON M.UPSOrderNumber = MA1.UPSOrderNumber AND M.SourceSystemKey = CASE WHEN MA1.SourceSystemKey = '1011' THEN M.SourceSystemKey ELSE MA.SourceSystemKey END AND M.MilestoneName = MA1.MilestoneName
			   LEFT JOIN {map_transactiontype_milestone} MTM ON MA.MilestoneOrder=MTM.MilestoneOrder
			  WHERE M.MilestoneName <> 'ALERT' ) TBL
			  GROUP BY ShipmentMileStones,templateType,MilestoneOrder,AccountId,UPSOrderNumber,DP_SERVICELINE_KEY)
			   GROUP BY AccountId, UPSOrderNumber, DP_SERVICELINE_KEY
			)
		,max_activity_name AS (
			SELECT collect_set(named_struct('WIP_ActivityName' , nvl(WA.WIP_ActivityName, '') , 'Type' , nvl(WA.Type, ''),'WIPActivityOrderId' , nvl(WA.WIPActivityOrderId, ''),'MilestoneName' , nvl(WA.MilestoneName, '') )) AS Activity_name_LIST
				,tmpActivity.UPSOrderNumber
				,tmpActivity.SourceSystemKey
			FROM all_act tmpActivity
			INNER JOIN {digital_summary_milestone_activity_vw} MA ON MA.is_deleted = 0 and tmpActivity.UPSOrderNumber = MA.UPSOrderNumber
				AND tmpActivity.SourceSystemKey = MA.SourceSystemKey
				AND tmpActivity.ActivityDate = MA.ActivityDate
				--AND MA.CurrentMilestoneFlag = 'Y'
			INNER JOIN {wh_wip_mapping_activity} WA ON MA.ActivityName = WA.ActivityName
				AND MA.SourceSystemKey = WA.SOURCE_SYSTEM_KEY  --and WA.Type ='Out'
			GROUP BY tmpActivity.UPSOrderNumber
				,tmpActivity.SourceSystemKey
			)
           ,trans_missed_delivery_movement AS (                       --UPSGLD-15476                     
			select o.UPSOrderNumber,o.SourceSystemKey,
            count(*) MissedDeliveredCount_movement 
            from {digital_summary_orders_vw}  o
            inner join {digital_summary_transportation} t
            on o.UPSOrderNumber= t.UpsOrderNumber
            and o.CurrentMilestone in ('TRANSPORTATION PLANNING','IN TRANSIT','CUSTOMS')
            and nvl(o.OrderStatusName,'') <> 'Cancelled'
            and nvl(o.OrderCancelledFlag,'N') <> 'Y'
            group by o.UPSOrderNumber,o.SourceSystemKey
			)
            ,trans_missed_delivery AS (
			select o.UPSOrderNumber,o.SourceSystemKey,
            count(*) MissedDeliveredCount 
            from {digital_summary_orders_vw}  o
            inner join {digital_summary_transportation} t
            on CASE WHEN t.TrasOnlyFlag <> 'TRANS_ONLY'
                                    THEN t.UpsWMSOrderNumber
                                ELSE t.UpsOrderNumber
                                END  = o.UPSOrderNumber
            and CASE WHEN t.TrasOnlyFlag <> 'TRANS_ONLY'
                                    THEN nvl(t.UpsWMSSourceSystemKey,o.SourceSystemKey)
                                ELSE t.SourceSystemKey
                                END  = o.SourceSystemKey     
            and o.CurrentMilestone in ('TRANSPORTATION PLANNING','IN TRANSIT','CUSTOMS')
            and nvl(o.OrderStatusName,'') <> 'Cancelled'
            and nvl(o.OrderCancelledFlag,'N') <> 'Y'
            group by o.UPSOrderNumber,o.SourceSystemKey
			)
	SELECT DISTINCT o.hash_key as id
    --md5(concat(o.AccountId,nvl(o.FacilityId,''),o.UPSOrderNumber,o.DateTimeReceived,nvl(o.UPSTransportShipmentNumber,''))) as id
		,o.AccountId                                                                                        
,O.DP_SERVICELINE_KEY                                                                               
,O.ServiceLevel                                                                                     
,o.FacilityId                                                                                       
,O.OrderCancelledFlag                                                                               
,O.IS_INBOUND IS_INBOUND                                                            
,o.UPSOrderNumber                                                                                   
,date_format(O.DateTimeReceived, 'yyyy-MM-dd HH:mm:ss.SSS') DateTimeReceived                        
,date_format(O.DateTimeReceived, 'yyyy-MM-dd') AS ShipmentCreationDate                              
,date_format(O.DateTimeShipped, 'yyyy-MM-dd') AS ShipmentShippedDate  
,date_format(O.ActualDeliveryDateTime, 'yyyy-MM-dd') AS ActualDeliveryDateTime_date 
,date_format(O.DateTimeShipped , 'yyyy-MM-dd HH:mm:ss.SSS') DateTimeShipped                         
,date_format(O.ScheduledPickUpDateTime , 'yyyy-MM-dd HH:mm:ss.SSS') ScheduledPickUpDateTime
,O.UPSOrderNumber shipmentNumber                                                                    
,O.SourceSystemKey                                                                                  
,O.OriginCountry                                                                                    
,O.DestinationCountry                                                                               
,nvl(O.ServiceMode,'UNASSIGNED') AS ShipmentMode                                                    
,O.ServiceMode                                                                                      
,O.OrderNumber referenceNumber                                                                      
,O.CustomerPO customerPONumber                                                                      
,O.UPSTransportShipmentNumber upsTransportShipmentNumber                                            
,O.GFF_ShipmentInstanceId gffShipmentInstanceId                                                     
,O.GFF_ShipmentNumber gffShipmentNumber                                                             
,O.OriginContactName shipmentOrigin_contactName                                                     
,O.OriginAddress1 shipmentOrigin_addressLine1                                                       
,O.OriginAddress2 shipmentOrigin_addressLine2                                                       
,O.OriginCity shipmentOrigin_city                                                                   
,O.OriginCity                                                                                       
,O.OriginProvince shipmentOrigin_stateProvince                                                      
,O.OriginPostalCode shipmentOrigin_postalCode                                                       
,O.OriginCountry shipmentOrigin_country                                                             
,O.DestinationContactName shipmentDestination_contactName                                           
,O.DestinationAddress1 shipmentDestination_addressLine1                                             
,O.DestinationAddress2 shipmentDestination_addressLine2                                              
,O.DestinationCity shipmentDestination_city                                                          
,O.DestinationCity                                                                                   
,O.DestinationProvince shipmentDestination_stateProvince                                             
,O.DestinationPostalcode shipmentDestination_postalCode                                              
,O.DestinationCountry shipmentDestination_country
,O.OrderType shipmentDescription
,O.ServiceMode shipmentService
,O.ServiceLevel shipmentServiceLevel
,O.ServiceLevelCode shipmentServiceLevelCode
,O.CarrierCode shipmentCarrierCode
,O.Carrier shipmentCarrier
,O.Carrier
,O.OrderStatusName inventoryShipmentStatus
,O.OrderStatusName
,O.TRANS_MILESTONE transportationMileStone
,O.ExceptionCode shipmentPrimaryException
,O.ExceptionCode  primaryException  
,date_format(ShipmentBookedDate, 'yyyy-MM-dd HH:mm:ss.SSS') AS shipmentBookedOnDateTime
,date_format(O.DateTimeCancelled, 'yyyy-MM-dd HH:mm:ss.SSS') as shipmentCanceledDateTime
,O.CancelledReasonCode shipmentCanceledReason
,date_format(O.ScheduleShipmentDate, 'yyyy-MM-dd HH:mm:ss.SSS') as actualShipmentDateTime
,date_format(O.ActualShipmentDateTime, 'yyyy-MM-dd HH:mm:ss.SSS') as actualShipmentDateTime_main
,date_format(O.DateTimeReceived, 'yyyy-MM-dd HH:mm:ss.SSS') as shipmentCreateOnDateTime
,date_format(O.OriginalScheduledDeliveryDateTime , 'yyyy-MM-dd HH:mm:ss.SSS') originalScheduledDeliveryDateTime
,date_format(O.ActualDeliveryDateTime , 'yyyy-MM-dd HH:mm:ss.SSS') AS actualDeliveryDateTime
,date_format(O.ActualDeliveryDateTime , 'yyyy-MM-dd') as ShipmentDeliveryDate_movement
,date_format(max_act.actualDeliveryDateTime_Movement , 'yyyy-MM-dd HH:mm:ss.SSS') AS actualDeliveryDateTime_Movement
,O.FacilityId warehouseId
,O.OrderWarehouse warehouseCode
,O.CurrentMilestone milestoneStatus
,date_format(O.EstimatedDeliveryDateTime , 'yyyy-MM-dd HH:mm:ss.SSS') AS estimatedDeliveryDateTime
,O.ORDER_REF_1_VALUE referenceNumber1
,O.ORDER_REF_2_VALUE referenceNumber2
,O.ORDER_REF_3_VALUE referenceNumber3
,O.ORDER_REF_4_VALUE referenceNumber4
,O.ORDER_REF_5_VALUE referenceNumber5
,O.OriginTimeZone AS shipmentCreateOnDateTimeZone
,O.DestinationTimeZone AS originalScheduledDeliveryDateTimeZone
,date_format(O.DateTimeShipped, 'yyyy-MM-dd HH:mm:ss.SSS') AS shippedDateTime
,O.shippedDateTimeZone
,O.LOAD_ID AS LoadID
,O.DestinationLocationCode AS shipmentDestination_locationCode
,O.AccountId AS dpProductLineKey
,O.Account_number AS Accountnumber
,O.ServiceLevel AS isShipmentServiceLevelResultSet
,case when o.is_inbound =0
then
CASE
WHEN O.CurrentMilestone = 'DELIVERED' THEN 
CASE 
	WHEN O.actualDeliveryDateTime > O.originalScheduledDeliveryDateTime
		THEN 'LATE'
	WHEN CAST(O.actualDeliveryDateTime AS DATE) <= CAST(O.originalScheduledDeliveryDateTime AS DATE)
	OR ( Cast(o.actualDeliveryDateTime AS DATE) IS NULL OR Cast(O.originalScheduledDeliveryDateTime AS DATE) IS NULL )
		THEN 'ONTIME'
	END
ELSE
NULL
END
else
CASE 
	WHEN O.actualDeliveryDateTime > O.originalScheduledDeliveryDateTime
		THEN 'LATE'
	WHEN CAST(O.actualDeliveryDateTime AS DATE) <= CAST(O.originalScheduledDeliveryDateTime AS DATE)
		THEN 'ONTIME'
	END 
end
	AS deliveryStatus
,CASE 
	WHEN CAST(O.actualDeliveryDateTime AS DATE) > CAST(FTTR.LoadLatestDeliveryDate AS DATE)
		THEN 'LATE'
	WHEN CAST(O.actualDeliveryDateTime AS DATE) <= CAST(FTTR.LoadLatestDeliveryDate AS DATE)
		THEN 'ONTIME'
	END AS deliveryStatus_movement
,CC.IS_TEMPERATURE AS isTemperatureTracked
,CC.IS_TEMPERATURE
,CC.STATUSDETAILTYPE
,CC.LATEST_TEMPERATURE AS latestTemperature
,date_format(CC.TEMPERATURE_DATETIME, 'yyyy-MM-dd HH:mm:ss.SSS') AS temperatureDateTime
,CC.TEMPERATURE_CITY AS temperatureCity
,CC.TEMPERATURE_STATE AS temperatureState
,CC.TEMPERATURE_COUNTRY AS temperatureCountry
,CC.TemperatureC AS latestTemperatureInCelsius
,CC.TemperatureF As latestTemperatureInFahrenheit
,cg.totalCharge totalCharge  
,cg.totalCharge * nvl(t.CarrierShipmentCount,1) totalChargeMovement  
,cg.totalcurency totalChargeCurrency  
,tic.totalCharge totalInvoiceCharge  
,tic.totalcurency totalInvoiceChargeCurrency           
,CASE 
	WHEN DSTR.UPSOrderNumber IS NULL
		THEN 'N'
	ELSE 'Y'
	END AS ISCLAIM --CL275            
,MA.ACTIVITY_NOTES AS lastKnownLocation --CL282 
,LLD.ACTIVITY_NOTES AS lastKnownLocationMovement
,CASE 
	WHEN OL.ShipmentLineCanceledFlag = 'Y'
		THEN 'Y'
	ELSE 'N'
	END AS ShipmentLineCanceledFlag
,date_format(OL.ShipmentLineCanceledDate, 'yyyy-MM-dd HH:mm:ss.SSS')   as ShipmentLineCanceledDate 
, t.TRACKING_NUMBER_LIST AS carrierShipmentNumber
,concat_ws(',',t.TRACKING_NUMBER_LIST) as Track_num
,t.tracking AS TrackingNumber
,e.exception_list AS exception
,e2.exception_list AS exception2
,el.exception_list
,claim_type.ReferenceValue AS claimType
,'' AS claimAmountCurrency
,claim_amt.ReferenceValue AS claimAmount
,claim_dt.ReferenceValue AS claimFilingDateTime
,claim_paid.ReferenceValue AS claimAmountPaid
,'' AS claimAmountPaidCurrency
,man.Activity_name_LIST AS WIP_ActivityName
,dm.DetailMilestone AS DetailMilestone
,'shipmentCanceledBy' shipmentCanceledBy
,O.UPSOrderNumber upsShipmentNumber
,O.OrderNumber clientShipmentNumber
,O.OrderNumber as orderNumber
,O.OriginTimeZone
,NULL AS LineNumber
,NULL AS shipmentLineCanceledDateTime
,NULL AS shipmentLineCanceledBy
,NULL AS shipmentLineCanceledReason
,CASE WHEN O.IS_INBOUND=0 THEN 'Outbound'
	WHEN O.IS_INBOUND=1 THEN 'Inbound'
     WHEN O.IS_INBOUND=2 THEN 'Managed Transportation'
	 END AS shipmentType
,date_format(O.DateTimeCancelled, 'yyyy-MM-dd HH:mm:ss.SSS') AS DateTimeCancelled
,date_format(O.DateTimeReceived, 'yyyy-MM-dd HH:mm:ss.SSS') AS shipmentPlaceDateTime
,date_format(O.ActualScheduledDeliveryDateTime, 'yyyy-MM-dd HH:mm:ss.SSS') AS  actualScheduledDeliveryDateTime
,CASE WHEN O.CurrentMilestone = 'DELIVERED' THEN O.PROOF_OF_DELIVERY_NAME ELSE NULL END AS ProofofDelivery_Name 
,O.ConsigneeName AS consignee
,O.OrderWarehouse  originLocationCode
,O.DestinationLocationCode AS ShipmentDestination_d
,O.AuthorizerName AS authorizorName
,O.DeliveryInstructions AS deliveryInstructions
,O.OriginTimeZone AS shipmentPlaceDateTimeZone
,O.OriginTimeZone AS shipmentCanceledDateTimeZone
,O.OriginTimeZone AS shipmentCreateDateTimeZone
,date_format(O.ScheduleShipmentDate, 'yyyy-MM-dd HH:mm:ss.SSS') as shipmentCreateDateTime
,date_format(O.ScheduleShipmentDate, 'yyyy-MM-dd HH:mm:ss.SSS') As expectedShipByDateTime 
,O.TransactionTypeName templateType
,O.DestinationLocationCode AS ShipmentDestination_destinationLocationCode
,date_format(pdate.PickUpDate, 'yyyy-MM-dd HH:mm:ss.SSS') as PickUpDate
,CASE when max_act.activity_flag =1 then date_format(max_act.ActivityDate, 'yyyy-MM-dd HH:mm:ss.SSS') else null end as actualDeliveryDateTime_inbound
,CASE WHEN O.IS_ASN=1 THEN  'ASN' ELSE 'Transport Order' END AS inboundType
,O.IS_ASN
,date_format(max_act.estimatedDeliveryDateTime, 'yyyy-MM-dd HH:mm:ss.SSS') as estimatedDeliveryDateTime_inbound
,date_format(max_act.claimClosureDateTime, 'yyyy-MM-dd HH:mm:ss.SSS') as claimClosureDateTime
,date_format(FTTR.LoadLatestDeliveryDate, 'yyyy-MM-dd HH:mm:ss.SSS') as LoadLatestDeliveryDate
,O.Freight_Carriercode  
,O.WAYBILL_AIRBILL_NUM  
,O.UPSOrderNumber AS FTZShipmentNumber
,SUR.shipunit_reference AS shipment_referenceType
,sir.shipitem_reference AS transportation_referenceType
,O.ActualScheduledDeliveryDateTimeZone
,wse.WAREHOUSE_CODE as LocationCode
,o.SourceSystemName
,wse.GLD_WAREHOUSE_MAPPED_KEY wse_warehouseId
,CASE WHEN o.SourceSystemName LIKE '%SOFTEON%' THEN WSE.BUILDING_CODE ELSE wse.WAREHOUSE_CODE END wse_warehouseCode
,wse.WAREHOUSE_TIME_ZONE wse_warehouseTimeZone
,wse.ADDRESS_LINE_1 wse_addressLine1
,wse.ADDRESS_LINE_2 wse_addressLine2
,wse.CITY wse_city
,wse.PROVINCE wse_stateProvince
,wse.POSTAL_CODE wse_postalCode
,wse.COUNTRY wse_country 
,max_act.claimStatus
,date_format(O.PickUPDateTime , 'yyyy-MM-dd HH:mm:ss.SSS')  AS originalPickupDateTime
,case when o.rn > 1 then 1 else nvl(o.is_deleted,0) end as is_deleted
,date_format(o.ActualDeliveryDate , 'yyyy-MM-dd HH:mm:ss.SSS') as ActualDeliveryDate
,max_act.ActivityCodeDelivery_status as ActivityCodeDelivery_status
,date_format(max_act.m1_ActualDeliveryDateTime , 'yyyy-MM-dd HH:mm:ss.SSS') as m1_ActualDeliveryDateTime
,CASE WHEN o.SourceSystemKey = 1002 THEN date_format(O.EstimatedDeliveryDateTime , 'yyyy-MM-dd HH:mm:ss.SSS') ELSE date_format(max_act.ma_shipmentEstimatedDateTime , 'yyyy-MM-dd HH:mm:ss.SSS') END as ma_shipmentEstimatedDateTime
,cast(ol.SKUQuantity_sum as bigint) as SKUQuantity_sum
,o.OrderLineCount
,date_format(max_act.ma_ShipmentDeliveryDate , 'yyyy-MM-dd') as ShipmentDeliveryDate
,date_format(max_act.ma_shipmentReceivedDate , 'yyyy-MM-dd HH:mm:ss.SSS') as shipmentReceivedDate
,date_format(max_act.ma_ActivityDate_1 , 'yyyy-MM-dd HH:mm:ss.SSS') as ma_ActivityDate_1
,date_format(max_act.ma_ActivityDate_2 , 'yyyy-MM-dd HH:mm:ss.SSS') as ma_ActivityDate_2
,date_format(max_act.ma_ActivityDate_2 , 'yyyy-MM-dd') as ma_ActivityDate_part_2
,date_format(max_act.ma_ActivityDate_1 , 'yyyy-MM-dd') as ma_ActivityDate_part_1
,array(nvl(date_format(max_act.ma_ActivityDate_1 , 'yyyy-MM-dd'), ''), nvl(date_format(max_act.ma_ActivityDate_2 , 'yyyy-MM-dd'), '') ) AS ma_ActivityDate_list
,case when nvl(date_format(max_act.ma_ActivityDate_1 , 'yyyy-MM-dd HH:mm:ss.SSS'), '')=nvl(date_format(max_act.ma_ActivityDate_2 , 'yyyy-MM-dd HH:mm:ss.SSS'), '') then array(nvl(date_format(max_act.ma_ActivityDate_1 , 'yyyy-MM-dd HH:mm:ss.SSS'), '')) else array(nvl(date_format(max_act.ma_ActivityDate_1 , 'yyyy-MM-dd HH:mm:ss.SSS'), ''), nvl(date_format(max_act.ma_ActivityDate_2 , 'yyyy-MM-dd HH:mm:ss.SSS'), '') ) end as ma_ActivityDate_list_distinct
,il.inbound_line_count
,il.inbound_ShippedQuantity_sum
,il.inbound_cases_sum
,o.is_managed
,CASE WHEN O.CurrentMilestone = 'DELIVERED' THEN CASE WHEN O.SourceSystemKey = 1002 THEN O.proof_of_delivery_name WHEN O.SourceSystemKey <> 1002 THEN max_act.proof_of_delivery_name ELSE NULL END END AS ProofofDelivery_Name -- change log
,max_act.proof_of_delivery_date_time -- change log
,O.OrderType AS orderType -- change log
,O.TemperatureRange_Min AS temperatureThresholdMin -- change log
,O.TemperatureRange_Max AS temperatureThresholdMax -- change log
,O.TemperatureRange_UOM AS temperatureThresholdUOM -- change log
,O.TemperatureThreshold  -- change log
,max_act.activity_Movement_flag
,O.EquipmentType as equipmentType
,inb_line.TransportOrderNumber as TransportOrderNumber
,max_act.claimStatus_movement
,inb_line_sil.inbound_shipment_listing
,tmdm.MissedDeliveredCount_movement --UPSGLD-15476
,tmd.MissedDeliveredCount
	FROM {digital_summary_orders_vw} O
	LEFT JOIN {digital_summary_transportation_callcheck} CC ON O.UPSTransportShipmentNumber = CC.UPSORDERNUMBER
		AND CC.STATUSDETAILTYPE = 'TemperatureTracking'
		AND CC.IS_LATEST_TEMPERATURE = 'Y'
        and CC.is_deleted = 0
	LEFT JOIN (SELECT DISTINCT CASE 
				WHEN FTTR.TrasOnlyFlag <> 'TRANS_ONLY'
					THEN FTTR.UpsWMSSourceSystemKey
				ELSE FTTR.SourceSystemKey
				END SourceSystemKey,
                CASE 
				WHEN FTTR.TrasOnlyFlag <> 'TRANS_ONLY'
					THEN FTTR.UpsWMSOrderNumber
				ELSE FTTR.UpsOrderNumber
				END UPSOrderNumber
                ,MAX(FTTR.LoadLatestDeliveryDate) LoadLatestDeliveryDate
                FROM {digital_summary_transportation} FTTR
                where FTTR.is_deleted = 0
                GROUP BY
                CASE 
				WHEN FTTR.TrasOnlyFlag <> 'TRANS_ONLY'
					THEN FTTR.UpsWMSSourceSystemKey
				ELSE FTTR.SourceSystemKey
				END ,
                CASE 
				WHEN FTTR.TrasOnlyFlag <> 'TRANS_ONLY'
					THEN FTTR.UpsWMSOrderNumber
				ELSE FTTR.UpsOrderNumber
				END ) FTTR ON 
				FTTR.SourceSystemKey = O.SourceSystemKey AND FTTR.UpsOrderNumber = O.UPSOrderNumber
	LEFT JOIN (select distinct DSTR1.UPSOrderNumber,DSTR1.SourceSystemKey   from {digital_summary_transportation_references} DSTR1 where  DSTR1.ReferenceLevel = 'LoadReference_Claim' 
	AND DSTR1.ReferenceType in ('Claim Type','Claim Amount') and DSTR1.is_deleted = 0) DSTR ON FTTR.UpsOrderNumber = DSTR.UPSOrderNumber
		AND FTTR.SourceSystemKey = DSTR.SourceSystemKey  
	LEFT JOIN last_location MA ON O.UPSOrderNumber = MA.UPSOrderNumber
		AND ROWNUM = 1
	LEFT JOIN last_location_wn_nt_delivered LLD ON O.UPSOrderNumber = LLD.UPSOrderNumber
	LEFT JOIN PickUpDate pdate ON O.UPSOrderNumber = pdate.UPSOrderNumber
	LEFT JOIN total_invoice_charge tic ON O.upsTransportShipmentNumber = tic.UPSOrderNumber  
	LEFT JOIN total_charge cg ON O.upsTransportShipmentNumber = cg.UPSOrderNumber   
	LEFT JOIN order_line ol ON O.UPSOrderNumber = OL.UPSOrderNumber
		AND o.SourceSystemKey = OL.SourceSystemKey
	LEFT JOIN tracking t ON o.UPSOrderNumber = t.UPSOrderNumber
	LEFT JOIN exception e ON CASE 
			WHEN e.SourceSystemKey = 1019
				THEN o.UPSOrderNumber
			ELSE UPSTransportShipmentNumber
			END = e.UPSOrderNumber
    LEFT JOIN exception2 e2 ON  
			o.UPSOrderNumber = e2.UPSOrderNumber  
            AND O.SourceSystemKey= e2.SourceSystemKey    
	LEFT JOIN exception_list el ON O.UPSOrderNumber = el.UPSOrderNumber
		AND o.SourceSystemKey = el.SourceSystemKey	
	LEFT JOIN detail_milestone dm ON O.UPSOrderNumber = dm.UPSOrderNumber
		AND O.AccountId = dm.AccountId
        AND O.DP_SERVICELINE_KEY = dm.DP_SERVICELINE_KEY
	LEFT JOIN {digital_summary_transportation_references} claim_type ON 
	Case when o.IS_INBOUND = 0 then O.UPSTransportShipmentNumber else o.UPSOrderNumber end  = claim_type.UPSOrderNumber 
    and case when o.IS_INBOUND = 0 then claim_type.SourceSystemKey else o.SourceSystemKey end = claim_type.SourceSystemKey
    AND claim_type.ReferenceLevel = 'LoadReference_Claim'
	AND claim_type.ReferenceType = 'Claim Type'
	LEFT JOIN {digital_summary_transportation_references} claim_amt ON 
	Case when o.IS_INBOUND = 0 then O.UPSTransportShipmentNumber else o.UPSOrderNumber end  = claim_amt.UPSOrderNumber 
    and case when o.IS_INBOUND = 0 then claim_amt.SourceSystemKey else o.SourceSystemKey end = claim_amt.SourceSystemKey
		AND claim_amt.ReferenceLevel = 'LoadReference_Claim'
		AND claim_amt.ReferenceType = 'Claim Amount'
	LEFT JOIN {digital_summary_transportation_references} claim_dt ON 
	 Case when o.IS_INBOUND = 0 then O.UPSTransportShipmentNumber else o.UPSOrderNumber end  = claim_dt.UPSOrderNumber 
     and case when o.IS_INBOUND = 0 then claim_dt.SourceSystemKey else o.SourceSystemKey end = claim_dt.SourceSystemKey
	AND claim_dt.ReferenceLevel = 'LoadReference_Claim'
	AND claim_dt.ReferenceType = 'Claim Date'
	LEFT JOIN {digital_summary_transportation_references} claim_paid ON
	 Case when o.IS_INBOUND = 0 then O.UPSTransportShipmentNumber else o.UPSOrderNumber end  = claim_paid.UPSOrderNumber 
     and case when o.IS_INBOUND = 0 then claim_paid.SourceSystemKey else o.SourceSystemKey end = claim_paid.SourceSystemKey
		AND claim_paid.SourceSystemKey = o.SourceSystemKey
		AND claim_paid.ReferenceLevel = 'LoadReference_Claim'
		AND claim_paid.ReferenceType = 'Claim Amount Paid'
	LEFT JOIN max_activity_name man ON O.UPSOrderNumber = man.UPSOrderNumber
		AND o.SourceSystemKey = man.SourceSystemKey
	LEFT JOIN max_activity max_act on max_act.UPSOrderNumber = o.UPSOrderNumber  
            AND max_act.SourceSystemKey = o.SourceSystemKey 
	LEFT JOIN shipunit_references SUR on SUR.UPSOrderNumber = o.UPSOrderNumber  
            AND SUR.SourceSystemKey = o.SourceSystemKey 
    LEFT JOIN inbound_line il on il.UPSOrderNumber = o.UPSOrderNumber  
            AND il.SourceSystemKey = o.SourceSystemKey 
    left join {dim_warehouse} wse
    on o.FacilityId =wse.GLD_WAREHOUSE_MAPPED_KEY and o.SourceSystemKey = wse.SOURCE_SYSTEM_KEY and wse.is_deleted = 0
    left join shipitem_references sir
    on sir.UPSOrderNumber = o.UPSOrderNumber  
            AND sir.SourceSystemKey = o.SourceSystemKey
    left join inbound_line2 inb_line
    on inb_line.UPSOrderNumber = o.UPSOrderNumber  
            AND inb_line.SourceSystemKey = o.SourceSystemKey
    left join inbound_line_sil inb_line_sil
    on inb_line_sil.UPSOrderNumber = o.UPSOrderNumber  
            AND inb_line_sil.SourceSystemKey = o.SourceSystemKey
    LEFT JOIN trans_missed_delivery_movement tmdm on tmdm.UPSOrderNumber = o.UPSOrderNumber  --UPSGLD-15476
            AND tmdm.SourceSystemKey = o.SourceSystemKey
    LEFT JOIN trans_missed_delivery tmd on tmd.UPSOrderNumber = o.UPSOrderNumber  
           AND tmd.SourceSystemKey = o.SourceSystemKey

  """.format(**source_tables,digital_summary_orders_vw='digital_summary_orders_vw',digital_summary_milestone_activity_vw='digital_summary_milestone_activity_vw',hwm=hwm,days_back=days_back)
    return (query)

# COMMAND ----------

# DBTITLE 1,Main function
def main():
    logger.info('Main function is running')
  ############################ ETL AUDIT #########################################################
  
    audit_result['process_name'] = 'load_digital_summary_orders_Container'
    audit_result['process_type'] = 'DataBricks'
    audit_result['layer'] = 'cosmos'
    audit_result['table_name'] = 'cosmos_digital_summary_orders'
    audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
    audit_result['start_time'] = start_time
  
    try:
      
        pid_get = get_pid()
        logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
        pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
        logger.info("pid: {pid}".format(pid=pid))
    
        audit_result['process_id'] = pid
    
        hwm=get_hwm('cosmos','cosmos_digital_summary_orders')
        logger.info(f'hwm cosmos_digital_summary_orders: {hwm}'.format(hwm=hwm))
#         hwm='1900-01-01 00:00:00'
#         logger.info(f'overwridden hwm: {hwm}'.format(hwm=hwm))
        
    
        logger.info("Creating digital summar orders view for incremental data")
        spark.sql(get_delta_query(hwm))
        logger.info("get_delta_query finished")  
        
        cnt=spark.sql("""select * from digital_summary_orders_vw""").count()
        logger.info('Insert count is {cnt}'.format(cnt=cnt))
        
        logger.info("Creating digital summar milestone activity view for 90 days")
        spark.sql(get_delta_query2(hwm))
        logger.info("get_delta_query2 finished")  
    
        logger.info('Reading source data...')
    
        src_query =get_pre_cosmos_query(hwm)
        logger.debug('cosmos_query : ' + src_query)
    
        cosmos_df = spark.sql(src_query)
            
        logger.debug("Adding audit columns")
        cosmos_df = add_audit_columns(cosmos_df, pid,datetime.now(),datetime.now())
    
        logger.info('Writing to Cosmos: {container_name}'.format(container_name=cosmosContainerName))
        cosmos_df.write.format("cosmos.oltp").options(**cfg).mode("APPEND").save()
        
        logger.info('setting hwm')
        r=set_hwm('cosmos','cosmos_digital_summary_orders',start_time,pid)
        logger.info(r)
    
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
