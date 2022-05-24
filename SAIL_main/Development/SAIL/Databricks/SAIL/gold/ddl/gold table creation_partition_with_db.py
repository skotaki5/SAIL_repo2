# Databricks notebook source
# MAGIC %sql drop table if exists gold.fact_order_dim_inc

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table gold.fact_order_dim_inc(
# MAGIC SOURCE_SYSTEM_KEY integer
# MAGIC ,UPS_ORDER_NUMBER string
# MAGIC ,ORDER_SDUK string
# MAGIC ,dl_update_timestamp	timestamp
# MAGIC ,ORDER_PLACED_DATE timestamp
# MAGIC ,UTC_ORDER_PLACED_DATE timestamp
# MAGIC ,WAREHOUSE_KEY decimal(18,0)
# MAGIC ,CARRIER_LOS_KEY decimal(18,0)
# MAGIC ,SERVICE_KEY decimal(18,0)
# MAGIC ,ORIGIN_LOCATION_KEY decimal(18,0)
# MAGIC ,DESTINATION_LOCATION_KEY decimal(18,0)
# MAGIC ,CLIENT_KEY decimal(18,0)
# MAGIC ,TRANSACTION_TYPE_ID integer
# MAGIC ,SOURCE_ORDER_STATUS string
# MAGIC ,SOURCE_ORDER_TYPE string
# MAGIC ,CUSTOMER_ORDER_NUMBER string
# MAGIC ,IS_INBOUND integer
# MAGIC ,CUSTOMER_PO_NUMBER string
# MAGIC ,REFERENCE_ORDER_NUMBER string
# MAGIC ,IS_MANAGED integer
# MAGIC ,IS_ASN integer
# MAGIC ,SOURCE_ORDER_SUB_TYPE string
# MAGIC ,ORDER_LATEST_ACTIVITY_DATE timestamp
# MAGIC ,UTC_ORDER_LATEST_ACTIVITY_DATE timestamp
# MAGIC ,ORDER_CANCELLED_FLAG string
# MAGIC ,ORDER_CANCELLED_DATE timestamp
# MAGIC ,UTC_ORDER_CANCELLED_DATE timestamp
# MAGIC ,ORDER_SHIPPED_DATE timestamp
# MAGIC ,UTC_ORDER_SHIPPED_DATE timestamp
# MAGIC ,LOFST_ORDER_LATEST_ACTIVITY_DATE timestamp
# MAGIC ,SHIPMENT_COUNT integer
# MAGIC ,STO_ORDER_COUNT integer
# MAGIC ,ORDER_LATEST_ACTIVITY_DATE_KEY integer
# MAGIC ,FREIGHT_CARRIER_CODE string
# MAGIC ,WAYBILL_AIRBILL_NUM string
# MAGIC ,DONOT_SHIP_BEFORE_DATE timestamp
# MAGIC ,ORIGIN_TIME_ZONE string
# MAGIC ,DESTINATION_TIME_ZONE string
# MAGIC ,ETL_BATCH_NUMBER decimal(18,0)
# MAGIC ,GLD_ACCOUNT_MAPPED_KEY string
# MAGIC ,DP_SERVICELINE_KEY string
# MAGIC ,DP_ORGENTITY_KEY string
# MAGIC ,EXT_CUSTOMER_ACCOUNT_NUMBER string
# MAGIC ,ServiceLevelName string
# MAGIC ,ServiceLevelCode string
# MAGIC ,CarrierCode string
# MAGIC ,CarrierName string
# MAGIC ,CARRIER_GROUP string
# MAGIC ,SERVICE_NAME_SR string
# MAGIC ,SERVICE_NAME_LC string
# MAGIC ,SERVICELEVELNAME_LC string
# MAGIC ,CARRIERNAME_LC string
# MAGIC ,FacilityId string
# MAGIC ,BUILDING_CODE string
# MAGIC ,WAREHOUSE_CODE string
# MAGIC ,ADDRESS_LINE_1 string
# MAGIC ,ADDRESS_LINE_2 string
# MAGIC ,CITY string
# MAGIC ,PROVINCE string
# MAGIC ,POSTAL_CODE string
# MAGIC ,COUNTRY string
# MAGIC ,WAREHOUSE_KEY_WSE decimal(18,0)
# MAGIC ,SOURCE_SYSTEM_NAME string
# MAGIC ,OrderStatusName string
# MAGIC ,TransactionTypeName string
# MAGIC ,ADDRESS_LINE_1_ORIGIN string
# MAGIC ,ADDRESS_LINE_2_ORIGIN string
# MAGIC ,CITY_ORIGIN string
# MAGIC ,PROVINCE_ORIGIN string
# MAGIC ,POSTAL_CODE_ORIGIN string
# MAGIC ,COUNTRY_ORIGIN string
# MAGIC ,LOCATION_CODE_ORIGIN string
# MAGIC ,ADDRESS_LINE_1_DESTINATION string
# MAGIC ,ADDRESS_LINE_2_DESTINATION string
# MAGIC ,CITY_DESTINATION string
# MAGIC ,PROVINCE_DESTINATION string
# MAGIC ,POSTAL_CODE_DESTINATION string
# MAGIC ,COUNTRY_DESTINATION string
# MAGIC ,LOCATION_CODE_DESTINATION string
# MAGIC ,LOCATION_NAME string
# MAGIC ,UTC_ORDER_PLACED_MONTH_part_key string
# MAGIC ,is_deleted int   
# MAGIC ,dl_hash	string
# MAGIC ,dl_insert_pipeline_id	string
# MAGIC ,dl_insert_timestamp	timestamp
# MAGIC ,dl_update_pipeline_id	string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/gold/summary/fact_order_dim_inc'

# COMMAND ----------

# MAGIC %sql 
# MAGIC alter table gold.fact_order_dim_inc add columns 
# MAGIC (
# MAGIC tt_is_inbound int AFTER TransactionTypeName
# MAGIC );
# MAGIC alter table gold.fact_order_dim_inc add columns 
# MAGIC (
# MAGIC tt_is_managed int AFTER tt_is_inbound
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table gold.fact_order_dim_inc SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='16','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql drop table if exists gold.digital_summary_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table gold.digital_summary_orders
# MAGIC (
# MAGIC SourceSystemKey	int,
# MAGIC UPSOrderNumber	string,
# MAGIC ORDER_SDUK	string,
# MAGIC transport_rn int,
# MAGIC TRANSPORTATION_SDUK string,
# MAGIC dl_update_timestamp	timestamp,
# MAGIC DateTimeReceived	timestamp,
# MAGIC UTC_DateTimeReceived	timestamp,
# MAGIC hash_key	string,
# MAGIC AccountId	string,
# MAGIC FacilityId	string,
# MAGIC OrderNumber	string,
# MAGIC ReferenceOrder	string,
# MAGIC CustomerPO	string,
# MAGIC LatestStatusDate	timestamp,
# MAGIC UTC_LatestStatusDate	timestamp,
# MAGIC OrderCancelledFlag	string,
# MAGIC DateTimeCancelled	timestamp,
# MAGIC UTC_DateTimeCancelled	timestamp,
# MAGIC DateTimeShipped	timestamp,
# MAGIC UTC_DateTimeShipped	timestamp,
# MAGIC RouteCode	string,
# MAGIC ClientOrderType	string,
# MAGIC CancelledReasonCode	string,
# MAGIC LogiNext_OrderFlag	string,
# MAGIC LogiNext_OrderCurrentSegment	string,
# MAGIC OrderStatusName	string,
# MAGIC ServiceLevel	string,
# MAGIC Carrier	string,
# MAGIC ServiceMode	string,
# MAGIC ServiceLevelCode	string,
# MAGIC CarrierCode	string,
# MAGIC ConsigneeName	string,
# MAGIC OriginAddress1	string,
# MAGIC OriginAddress2	string,
# MAGIC OriginCity	string,
# MAGIC OriginProvince	string,
# MAGIC OriginPostalCode	string,
# MAGIC OriginCountry	string,
# MAGIC DestinationAddress1	string,
# MAGIC DestinationAddress2	string,
# MAGIC DestinationCity	string,
# MAGIC DestinationProvince	string,
# MAGIC DestinationPostalcode	string,
# MAGIC DestinationCountry	string,
# MAGIC OrderType	string,
# MAGIC SourceSystemName	string,
# MAGIC ShipmentCount	int,
# MAGIC IsSTO	int,
# MAGIC TrackingNo	string,
# MAGIC TodayFlag	string,
# MAGIC ORDER_REF_1_LABEL	string,
# MAGIC ORDER_REF_1_VALUE	string,
# MAGIC ORDER_REF_2_LABEL	string,
# MAGIC ORDER_REF_2_VALUE	string,
# MAGIC ORDER_REF_3_LABEL	string,
# MAGIC ORDER_REF_3_VALUE	string,
# MAGIC ORDER_REF_4_LABEL	string,
# MAGIC ORDER_REF_4_VALUE	string,
# MAGIC ORDER_REF_5_LABEL	string,
# MAGIC ORDER_REF_5_VALUE	string,
# MAGIC ORDER_REF_6_LABEL	string,
# MAGIC ORDER_REF_6_VALUE	string,
# MAGIC ORDER_REF_7_LABEL	string,
# MAGIC ORDER_REF_7_VALUE	string,
# MAGIC ORDER_REF_8_LABEL	string,
# MAGIC ORDER_REF_8_VALUE	string,
# MAGIC ORDER_REF_9_LABEL	string,
# MAGIC ORDER_REF_9_VALUE	string,
# MAGIC ORDER_REF_10_LABEL	string,
# MAGIC ORDER_REF_10_VALUE	string,
# MAGIC ORDER_REF_11_LABEL	string,
# MAGIC ORDER_REF_11_VALUE	string,
# MAGIC ORDER_REF_12_LABEL	string,
# MAGIC ORDER_REF_12_VALUE	string,
# MAGIC ORDER_REF_13_LABEL	string,
# MAGIC ORDER_REF_13_VALUE	string,
# MAGIC ORDER_REF_14_LABEL	string,
# MAGIC ORDER_REF_14_VALUE	string,
# MAGIC ORDER_REF_15_LABEL	string,
# MAGIC ORDER_REF_15_VALUE	string,
# MAGIC TransOnly	int,
# MAGIC TransMilestone	string,
# MAGIC ExceptionCode	string,
# MAGIC OriginalScheduledDeliveryDateTime	timestamp,
# MAGIC UTC_OriginalScheduledDeliveryDateTime	timestamp,
# MAGIC ActualScheduledDeliveryDateTime	timestamp,
# MAGIC UTC_ActualScheduledDeliveryDateTime	timestamp,
# MAGIC ActualDeliveryDate	timestamp,
# MAGIC UTC_ActualDeliveryDate	timestamp,
# MAGIC ScheduleShipmentDate	timestamp,
# MAGIC UTC_ScheduleShipmentDate	timestamp,
# MAGIC ActualShipmentDateTime	timestamp,
# MAGIC UTC_ActualShipmentDateTime	timestamp,
# MAGIC OrderWarehouse	string,
# MAGIC OrderLineCount	int,
# MAGIC OriginContactName	string,
# MAGIC TRANSPORT_MILESTONE_1	string,
# MAGIC TRANSPORT_MILESTONEDATE_1	timestamp,
# MAGIC TRANSPORT_MILESTONE_2	string,
# MAGIC TRANSPORT_MILESTONEDATE_2	timestamp,
# MAGIC TRANSPORT_MILESTONE_3	string,
# MAGIC TRANSPORT_MILESTONEDATE_3	timestamp,
# MAGIC TRANSPORT_MILESTONE_4	string,
# MAGIC TRANSPORT_MILESTONEDATE_4	timestamp,
# MAGIC TRANSPORT_MILESTONE_5	string,
# MAGIC TRANSPORT_MILESTONEDATE_5	timestamp,
# MAGIC TRANSPORT_MILESTONE_6	string,
# MAGIC TRANSPORT_MILESTONEDATE_6	timestamp,
# MAGIC SOURCE_ORDER_STATUS	string,
# MAGIC TRANS_MILESTONE	string,
# MAGIC DP_SERVICELINE_KEY	string,
# MAGIC DP_ORGENTITY_KEY	string,
# MAGIC LOAD_ID	string,
# MAGIC UPSTransportShipmentNumber	string,
# MAGIC CurrentMilestone	string,
# MAGIC CurrentMilestoneDate	timestamp,
# MAGIC IS_INBOUND	int,
# MAGIC SHIPMENT_NOTES	string,
# MAGIC IS_ASN	int,
# MAGIC TransactionTypeName	string,
# MAGIC ShipmentBookedDate	timestamp,
# MAGIC GFF_ShipmentNumber	string,
# MAGIC GFF_ShipmentInstanceId	string,
# MAGIC Freight_Carriercode	string,
# MAGIC WAYBILL_AIRBILL_NUM	string,
# MAGIC PROOF_OF_DELIVERY_NAME	string,
# MAGIC ActualScheduledDeliveryDateTimeZone	string,
# MAGIC ShippedDateTimeZone	string,
# MAGIC EquipmentType	string,
# MAGIC OriginTimeZone	string,
# MAGIC DestinationTimeZone	string,
# MAGIC DestinationLocationCode	string,
# MAGIC OriginLocationCode	string,
# MAGIC AuthorizerName	string,
# MAGIC DeliveryInstructions	string,
# MAGIC DestinationContactName	string,
# MAGIC PickUPDateTime	timestamp,
# MAGIC ScheduledPickUpDateTime	timestamp,
# MAGIC RowNumber	int,
# MAGIC BATCH_ID	string,
# MAGIC Account_number	string,
# MAGIC EstimatedDeliveryDateTime	timestamp,
# MAGIC ActualDeliveryDateTime	timestamp,
# MAGIC is_healthcare int,
# MAGIC is_deleted	int,
# MAGIC UTC_ORDER_PLACED_MONTH_part_key string,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string
# MAGIC )
# MAGIC using delta
# MAGIC location "/mnt/sail/gold/summary/digital_summary_orders"

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table gold.digital_summary_orders SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='7','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql drop table if exists gold.digital_summary_milestone_activity

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table gold.digital_summary_milestone_activity
# MAGIC (
# MAGIC SourceSystemKey	int,
# MAGIC UPSOrderNumber	string,
# MAGIC hash_key	string,
# MAGIC dl_update_timestamp	timestamp,
# MAGIC is_deleted	int,
# MAGIC TransactionTypeId	int,
# MAGIC ActivityCode	string,
# MAGIC ActivityDate	timestamp,
# MAGIC LOAD_TRACK_SDUK	string,
# MAGIC TrackingNumber	string,
# MAGIC AccountId	string,
# MAGIC FacilityId	string,
# MAGIC SourceSystemName	string,
# MAGIC Source_Activity_Code	string,
# MAGIC Source_Activity_Name	string,
# MAGIC MilestoneId	int,
# MAGIC MilestoneName	string,
# MAGIC ActivityId	int,
# MAGIC ActivityName	string,
# MAGIC ACTIVITY_STATUS string,
# MAGIC ActivityCompletionFlag	string,
# MAGIC PlannedMilestoneDate	timestamp,
# MAGIC MilestoneDate	timestamp,
# MAGIC MilestoneCompletionFlag	string,
# MAGIC MilestoneOrder	int,
# MAGIC CurrentMilestoneFlag	string,
# MAGIC UPSASNNumber	string,
# MAGIC SEGMENT_ID	int,
# MAGIC ACTIVITY_NOTES	string,
# MAGIC VENDOR_NAME	string,
# MAGIC PROOF_OF_DELIVERY_NAME	string,
# MAGIC CARRIER_TYPE	string,
# MAGIC P_Flag	int,
# MAGIC FTZ_STATUS	string,
# MAGIC TimeZone	string,
# MAGIC LOGI_NEXT_FLAG	string,
# MAGIC BATCH_ID	int,
# MAGIC ACTIVITY_MONTH_part_key string,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string
# MAGIC )
# MAGIC using delta
# MAGIC location "/mnt/sail/gold/summary/digital_summary_milestone_activity"

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table gold.digital_summary_milestone_activity SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='20','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql drop table if exists gold.digital_summary_transportation_callcheck

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table gold.digital_summary_transportation_callcheck
# MAGIC (
# MAGIC SOURCESYSTEMKEY	int,
# MAGIC UPSORDERNUMBER	string,
# MAGIC order_sduk	string,
# MAGIC transportation_sduk	string,
# MAGIC CALLCHECK_SDUK	string,
# MAGIC transport_rn int,
# MAGIC hash_key	string,
# MAGIC dl_update_timestamp	timestamp,
# MAGIC ACCOUNTID	string,
# MAGIC FACILITYID	string,
# MAGIC DP_SERVICELINE_KEY	string,
# MAGIC DP_ORGENTITY_KEY	string,
# MAGIC LOADID	string,
# MAGIC LATEST_TEMPERATURE	string,
# MAGIC TEMPERATURE_CITY	string,
# MAGIC TEMPERATURE_STATE	string,
# MAGIC TEMPERATURE_COUNTRY	string,
# MAGIC IS_TEMPERATURE	string,
# MAGIC TEMPERATURE_DATETIME	timestamp,
# MAGIC ACTIVITYTYPE	string,
# MAGIC STATUSDETAILTYPE	string,
# MAGIC IS_LATEST_TEMPERATURE	string,
# MAGIC TemperatureC	decimal(18,0),
# MAGIC TemperatureF	decimal(18,0),
# MAGIC BatteryPercent	int,
# MAGIC Humidity	int,
# MAGIC Light	decimal(18,0),
# MAGIC IsShockExceeded	boolean,
# MAGIC Latitude	decimal(18,0),
# MAGIC Longitude	decimal(18,0),
# MAGIC DeviceTagId	string,
# MAGIC LocationMethod	string,
# MAGIC IsMotionDetected	boolean,
# MAGIC Pressure	decimal(18,0),
# MAGIC IsButtonPushed	boolean,
# MAGIC is_deleted	int,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string
# MAGIC )
# MAGIC using delta
# MAGIC location "/mnt/sail/gold/summary/digital_summary_transportation_callcheck"

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table gold.digital_summary_transportation_callcheck SET TBLPROPERTIES ('targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql drop table if exists gold.digital_summary_transportation_references

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table gold.digital_summary_transportation_references
# MAGIC (
# MAGIC SourceSystemKey	int,
# MAGIC UPSOrderNumber	string,
# MAGIC ORDER_SDUK	string,
# MAGIC REFERENCE_SDUK	string,
# MAGIC TRANSPORTATION_SDUK	string,
# MAGIC hash_key	string,
# MAGIC dl_update_timestamp	timestamp,
# MAGIC LOAD_ID	string,
# MAGIC ShipUnitId	string,
# MAGIC ReferenceType	string,
# MAGIC ReferenceValue	string,
# MAGIC ReferenceLevel	string,
# MAGIC is_deleted	int,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string
# MAGIC )
# MAGIC using delta
# MAGIC location "/mnt/sail/gold/summary/digital_summary_transportation_references"

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table gold.digital_summary_transportation_references SET TBLPROPERTIES ('targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql drop table if exists gold.digital_summary_transportation_rates_charges

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table gold.digital_summary_transportation_rates_charges
# MAGIC (
# MAGIC SourceSystemKey	int,
# MAGIC UpsOrderNumber	string,
# MAGIC order_sduk string,
# MAGIC charge_sduk	string,
# MAGIC transport_rn int,
# MAGIC TRANSPORTATION_SDUK string,
# MAGIC hash_key	string,
# MAGIC dl_update_timestamp	timestamp,
# MAGIC load_id	string,
# MAGIC SequenceNumber	string,
# MAGIC ChargeType	string,
# MAGIC Rate	string,
# MAGIC RateQualifer	string,
# MAGIC Charge	string,
# MAGIC ChargeDescription	string,
# MAGIC ChargeLevel	string,
# MAGIC EdiCode	string,
# MAGIC FreightClass	string,
# MAGIC FAKFreightClass	string,
# MAGIC ContractName	string,
# MAGIC CurrencyCode	string,
# MAGIC InvoiceNumber	string,
# MAGIC is_deleted	int,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string
# MAGIC )
# MAGIC using delta
# MAGIC location "/mnt/sail/gold/summary/digital_summary_transportation_rates_charges"

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table gold.digital_summary_transportation_rates_charges SET TBLPROPERTIES ('targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql drop table if exists gold.digital_summary_order_tracking

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table gold.digital_summary_order_tracking
# MAGIC (
# MAGIC SourceSystemKey	int,
# MAGIC UPSOrderNumber	string,
# MAGIC order_sduk	string,
# MAGIC SHIPMENT_SDUK	string,
# MAGIC TRANSPORTATION_SDUK string ,
# MAGIC transport_rn int,
# MAGIC dl_update_timestamp	timestamp,
# MAGIC hash_key	string,
# MAGIC AccountId	string,
# MAGIC FacilityId	string,
# MAGIC DP_SERVICELINE_KEY	string,
# MAGIC DP_ORGENTITY_KEY	string,
# MAGIC ShipmentDimensions	string,
# MAGIC ShipmentWeight	string,
# MAGIC CarrierCode	string,
# MAGIC TRACKING_NUMBER	string,
# MAGIC CarrierType	string,
# MAGIC ShipmentDimensions_UOM	string,
# MAGIC ShipmentWeight_UOM	string,
# MAGIC TemperatureRange_Min	string,
# MAGIC TemperatureRange_Max	string,
# MAGIC TemperatureRange_UOM	string,
# MAGIC TemperatureRange_Code	string,
# MAGIC SHIPMENT_QUANTITY	decimal(18,0),
# MAGIC SHIPMENT_DESCRIPTION	string,
# MAGIC LOAD_AREA	decimal(18,0),
# MAGIC UOM	string,
# MAGIC UTC_SHIPMENT_CREATION_MONTH_PART_KEY bigint,
# MAGIC is_deleted	int,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string
# MAGIC )
# MAGIC using delta
# MAGIC location "/mnt/sail/gold/summary/digital_summary_order_tracking"

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table gold.digital_summary_order_tracking SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='7','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql drop table if exists gold.digital_summary_transportation

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table gold.digital_summary_transportation
# MAGIC (
# MAGIC SourceSystemKey	int,
# MAGIC UpsOrderNumber	string,
# MAGIC order_sduk	string,
# MAGIC transportation_sduk	string,
# MAGIC dl_update_timestamp	timestamp,
# MAGIC hash_key	string,
# MAGIC UpsWMSOrderNumber	string,
# MAGIC UpsWMSSourceSystemKey	int,
# MAGIC SourceOrderType	string,
# MAGIC EquipmentType	string,
# MAGIC SourceOrderSubType	string,
# MAGIC OriginCompany	string,
# MAGIC DestinationCompany	string,
# MAGIC OriginTimeZone	string,
# MAGIC DestinationTimeZone	string,
# MAGIC SourceOrderState	string,
# MAGIC SourceOrderStatus	string,
# MAGIC OrderCancelledFlag	string,
# MAGIC Order_Rec_CreatedDate	timestamp,
# MAGIC UTC_Order_Rec_CreatedDate	timestamp,
# MAGIC LOFST_Rec_CreatedDate	timestamp,
# MAGIC OrderPlacedDate	timestamp,
# MAGIC UTC_OrderPlacedDate	timestamp,
# MAGIC LOFST_OrderPlacedDate	timestamp,
# MAGIC OrderCancelledDate	timestamp,
# MAGIC UTC_OrderCancelledDate	timestamp,
# MAGIC LOFST_OrderCancelledDate	timestamp,
# MAGIC OrderShippedDate	timestamp,
# MAGIC UTC_OrderShippedDate	timestamp,
# MAGIC LOFST_OrderShippedDate	timestamp,
# MAGIC ScheduledShipmentDate	timestamp,
# MAGIC UTC_ScheduledShipmentDate	timestamp,
# MAGIC LOFST_ScheduledShipmentDate	timestamp,
# MAGIC ActualShipmentDate	timestamp,
# MAGIC UTC_ActualShipmentDate	timestamp,
# MAGIC LOFST_ActualShipmentDate	timestamp,
# MAGIC ScheduledDeliveryDate	timestamp,
# MAGIC UTC_ScheduledDeliveryDate	timestamp,
# MAGIC LOFST_ScheduledDeliveryDate	timestamp,
# MAGIC ActualDeliveryDate	timestamp,
# MAGIC UTC_ActualDeliveryDate	timestamp,
# MAGIC LOFST_ActualDeliveryDate	timestamp,
# MAGIC OrderCount	int,
# MAGIC OriginalScheduledDeliveryDate	timestamp,
# MAGIC UTC_OriginalScheduledDeliveryDate	timestamp,
# MAGIC LOFST_OriginalScheduledDeliveryDate	timestamp,
# MAGIC LOAD_ID	string,
# MAGIC LoadEarliestPickUpDate	timestamp,
# MAGIC LoadLatestPickUpDate	timestamp,
# MAGIC LoadEarliestDeliveryDate	timestamp,
# MAGIC LoadLatestDeliveryDate	timestamp,
# MAGIC LoadCreationDate	timestamp,
# MAGIC LoadUpdateDate	timestamp,
# MAGIC CarrierCode	string,
# MAGIC LevelOfServiceCode	string,
# MAGIC WMSPONumber	string,
# MAGIC CarrierMode	string,
# MAGIC TrasOnlyFlag	string,
# MAGIC ShipmentNotes	string,
# MAGIC Comments	string,
# MAGIC GFFShipmentNumber	string,
# MAGIC GFFShipmentInstanceNumber	string,
# MAGIC ProofOfDelivery	string,
# MAGIC Scope	string,
# MAGIC Sector	string,
# MAGIC Direction	string,
# MAGIC AuthorizerName	string,
# MAGIC DeliveryInstructions	string,
# MAGIC DestinationContact	string,
# MAGIC UTC_ORDER_PLACED_MONTH_PART_KEY bigint,
# MAGIC is_deleted	int,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string
# MAGIC )
# MAGIC using delta
# MAGIC location "/mnt/sail/gold/summary/digital_summary_transportation"

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table gold.digital_summary_transportation SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='8','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql drop table if exists gold.digital_summary_order_lines

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table gold.digital_summary_order_lines
# MAGIC (
# MAGIC SourceSystemKey	int,
# MAGIC UPSOrderNumber	string,
# MAGIC order_sduk	string,
# MAGIC order_line_sduk	string,
# MAGIC dl_update_timestamp	timestamp,
# MAGIC hash_key	string,
# MAGIC AccountId	string,
# MAGIC FacilityId	string,
# MAGIC DP_SERVICELINE_KEY	string,
# MAGIC DP_ORGENTITY_KEY	string,
# MAGIC OrderNumber	string,
# MAGIC LineNUmber	string,
# MAGIC SKU	string,
# MAGIC SKUDescription	string,
# MAGIC SKUDimensions	string,
# MAGIC SKUWeight	decimal(22,4),
# MAGIC SKUQuantity	decimal(22,4),
# MAGIC SKUShippedQuantity	decimal(22,4),
# MAGIC CarrierCode	string,
# MAGIC TrackingNo	string,
# MAGIC SKUDimensions_UOM	string,
# MAGIC SKUWeight_UOM	string,
# MAGIC ShipmentLineCanceledDate	timestamp,
# MAGIC ShipmentLineCanceledReason	string,
# MAGIC ShipmentLineCanceledBy	string,
# MAGIC ShipmentLineCanceledFlag	string,
# MAGIC LineRefVal1	string,
# MAGIC LineRefVal2	string,
# MAGIC LineRefVal3	string,
# MAGIC LineRefVal4	string,
# MAGIC LineRefVal5	string,
# MAGIC UTC_ORDER_PLACED_MONTH_PART_KEY bigint,
# MAGIC is_deleted	int,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string
# MAGIC )
# MAGIC using delta
# MAGIC location "/mnt/sail/gold/summary/digital_summary_order_lines"

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table gold.digital_summary_order_lines SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='6','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql drop table if exists gold.digital_summary_order_lines_details

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table gold.digital_summary_order_lines_details
# MAGIC (
# MAGIC SourceSystemKey	int,
# MAGIC UPSOrderNumber	string,
# MAGIC order_sduk	string,
# MAGIC ORDER_LINE_DETAILS_SDUK	string,
# MAGIC hash_key	string,
# MAGIC dl_update_timestamp	timestamp,
# MAGIC LineNumber	string,
# MAGIC AccountId	string,
# MAGIC FacilityId	string,
# MAGIC DP_SERVICELINE_KEY	string,
# MAGIC DP_ORGENTITY_KEY	string,
# MAGIC LineDetailNumber	string,
# MAGIC VendorSerialNumber	string,
# MAGIC VendorLotNumber	string,
# MAGIC LPNNumber	string,
# MAGIC DispositionValue	string,
# MAGIC WarehouseKey	decimal(18,0),
# MAGIC ItemKey	decimal(18,0),
# MAGIC itemNumber	string,
# MAGIC EXPIRATION_DATE	timestamp,
# MAGIC WAREHOUSE_CODE	string,
# MAGIC is_deleted	int,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string
# MAGIC )
# MAGIC using delta
# MAGIC location "/mnt/sail/gold/summary/digital_summary_order_lines_details"

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table gold.digital_summary_order_lines_details SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='6','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql drop table if exists gold.digital_summary_milestone

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table gold.digital_summary_milestone
# MAGIC (
# MAGIC SourceSystemKey	int,
# MAGIC UPSOrderNumber	string,
# MAGIC order_sduk	string,
# MAGIC hash_key	string,
# MAGIC dl_update_timestamp	timestamp,
# MAGIC SourceSystemName	string,
# MAGIC AccountId	string,
# MAGIC DP_SERVICELINE_KEY	string,
# MAGIC DP_ORGENTITY_KEY	string,
# MAGIC FacilityId	string,
# MAGIC WarehouseCode	string,
# MAGIC TransactionTypeName	string,
# MAGIC MilestoneName	string,
# MAGIC MilestoneOrder	int,
# MAGIC 
# MAGIC UTC_ORDER_PLACED_MONTH_part_key string,
# MAGIC is_deleted	int,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string
# MAGIC )
# MAGIC using delta
# MAGIC location "/mnt/sail/gold/summary/digital_summary_milestone"

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table gold.digital_summary_milestone SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='5','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql drop table if exists gold.digital_summary_exceptions

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table gold.digital_summary_exceptions
# MAGIC (
# MAGIC SourceSystemKey	int,
# MAGIC UPSOrderNumber	string,
# MAGIC TRANSPORTATION_EXCEPTION_SDUK	string,
# MAGIC hash_key	string,
# MAGIC dl_update_timestamp	timestamp,
# MAGIC UTC_ExceptionCreatedDate	timestamp,
# MAGIC OTZ_ExceptionCreatedDate	timestamp,
# MAGIC LOFST_ExceptionCreatedDate_OTZ	timestamp,
# MAGIC ExceptionCreatedDate_DTZ	timestamp,
# MAGIC LOFST_ExceptionCreatedDate_DTZ	timestamp,
# MAGIC ExceptionDescription	string,
# MAGIC ExceptionEvent	string,
# MAGIC ExceptionReason	string,
# MAGIC ExceptionReasonType	string,
# MAGIC ExceptionCategory	string,
# MAGIC ResponsibleParty	string,
# MAGIC ExceptionPrimaryIndicator	decimal(18,0),
# MAGIC ExceptionCount	int,
# MAGIC ExceptionType	string,
# MAGIC DateTimeShippedTimeZone	string,
# MAGIC ActualScheduledDeliveryDateTimeZone	string,
# MAGIC is_deleted	int,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string
# MAGIC )
# MAGIC using delta
# MAGIC location "/mnt/sail/gold/summary/digital_summary_exceptions"

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table gold.digital_summary_exceptions SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='5','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql drop table if exists gold.digital_summary_transport_details

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table gold.digital_summary_transport_details
# MAGIC (
# MAGIC SOURCE_SYSTEM_KEY	int,
# MAGIC hash_key	string,
# MAGIC UPSORDERNUMBER	string,
# MAGIC dl_update_timestamp	timestamp,
# MAGIC Account_ID	string,
# MAGIC DP_SERVICELINE_KEY	string,
# MAGIC DP_ORGENTITY_KEY	string,
# MAGIC ITEM_DESCRIPTION	string,
# MAGIC ACTUAL_QTY	decimal(18,0),
# MAGIC ACTUAL_UOM	string,
# MAGIC ACTUAL_WGT	string,
# MAGIC ITEM_DIMENSION	string,
# MAGIC TempRangeMin	string,
# MAGIC TempRangeMax	string,
# MAGIC TempRangeUOM	string,
# MAGIC TempRangeCode	string,
# MAGIC PlannedWeightUOM	string,
# MAGIC ActualWeightUOM	string,
# MAGIC DimensionUOM	string,
# MAGIC is_deleted	int,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string
# MAGIC )
# MAGIC using delta
# MAGIC location "/mnt/sail/gold/summary/digital_summary_transport_details"

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table gold.digital_summary_transport_details SET TBLPROPERTIES ('targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql drop table if exists gold.digital_summary_inbound_line

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table gold.digital_summary_inbound_line
# MAGIC (
# MAGIC SourceSystemKey	int,
# MAGIC hash_key	string,
# MAGIC UPSOrderNumber	string,
# MAGIC dl_update_timestamp	timestamp,
# MAGIC AccountId	string,
# MAGIC DP_SERVICELINE_KEY	string,
# MAGIC DP_ORGENTITY_KEY	string,
# MAGIC FacilityId	string,
# MAGIC FacilityCode	string,
# MAGIC UPSASNNumber	string,
# MAGIC ClientASNNumber	string,
# MAGIC ClientPONumber	string,
# MAGIC ReceiptNumber	string,
# MAGIC ReceiptLineNumber	string,
# MAGIC ShippedQuantity	decimal(38,4),
# MAGIC ReceivedQuantity	decimal(38,4),
# MAGIC CreationDateTime	timestamp,
# MAGIC SKU	string,
# MAGIC SKUDescription	string,
# MAGIC SKUDimensions	string,
# MAGIC SKUWeight	decimal(22,4),
# MAGIC SKUDimensions_UOM	string,
# MAGIC SKUWeight_UOM	string,
# MAGIC InboundLine_Reference2	string,
# MAGIC InboundLine_Reference10	string,
# MAGIC InboundLine_Reference11	string,
# MAGIC PutAwayDate	timestamp,
# MAGIC 
# MAGIC is_deleted	int,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string
# MAGIC )
# MAGIC using delta
# MAGIC location "/mnt/sail/gold/summary/digital_summary_inbound_line"

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table gold.digital_summary_inbound_line SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='5','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql drop table if exists gold.digital_summary_inventory

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table gold.digital_summary_inventory
# MAGIC (SourceSystemKey	int
# MAGIC ,SourceSystemName	string
# MAGIC ,AccountId	string
# MAGIC ,FacilityId	string
# MAGIC ,itemNumber	string
# MAGIC ,itemDescription	string
# MAGIC ,hazardClass	string
# MAGIC ,itemDimensions_length	decimal(22,4)
# MAGIC ,itemDimensions_width	decimal(22,4)
# MAGIC ,itemDimensions_height	decimal(22,4)
# MAGIC ,itemDimensions_unitOfMeasurement_code	string
# MAGIC ,itemWeight_weight	decimal(22,4)
# MAGIC ,itemWeight_unitOfMeasurement_Code	string
# MAGIC ,warehouseCode	string
# MAGIC ,availableQuantity	decimal(32,4)
# MAGIC ,nonAvailableQuantity	decimal(32,4)
# MAGIC ,DP_SERVICELINE_KEY	string
# MAGIC ,DP_ORGENTITY_KEY	string
# MAGIC ,InvRef1	string
# MAGIC ,InvRef2	string
# MAGIC ,InvRef3	string
# MAGIC ,InvRef4	string
# MAGIC ,InvRef5	string
# MAGIC ,LPNNumber	string
# MAGIC ,HazmatClass	string
# MAGIC ,StrategicGoodsFlag	string
# MAGIC ,UNNumber	string
# MAGIC ,Designator	string
# MAGIC ,VendorSerialNumber	string
# MAGIC ,VendorLotNumber	string
# MAGIC ,BatchStatus	string
# MAGIC ,ExpirationDate	timestamp
# MAGIC ,Account_number	string
# MAGIC ,BatchHoldReason	string
# MAGIC ,HoldDescription	string
# MAGIC ,is_deleted	int
# MAGIC ,hash_key string
# MAGIC ,dl_hash	string
# MAGIC ,dl_insert_pipeline_id	string
# MAGIC ,dl_insert_timestamp	string
# MAGIC ,dl_update_pipeline_id	string
# MAGIC ,dl_update_timestamp	string
# MAGIC ) using delta
# MAGIC location "/mnt/sail/gold/summary/digital_summary_inventory"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- change log ddls
# MAGIC 
# MAGIC alter table gold.DIGITAL_summary_milestone_activity
# MAGIC add columns
# MAGIC (
# MAGIC PROOF_OF_DELIVERY_LOCATION string,
# MAGIC PROOF_OF_DELIVERY_DATE_TIME timestamp,
# MAGIC LATITUDE string,
# MAGIC LONGITUDE string
# MAGIC );
# MAGIC 
# MAGIC alter table gold.DIGITAL_SUMMARY_ORDER_LINES_DETAILS
# MAGIC add columns
# MAGIC (
# MAGIC ReceiptNumber string,
# MAGIC IS_INBOUND int
# MAGIC );
# MAGIC 
# MAGIC alter table gold.DIGITAL_SUMMARY_INBOUND_LINE
# MAGIC add columns
# MAGIC (
# MAGIC CASES int
# MAGIC );
# MAGIC alter table gold.DIGITAL_SUMMARY_ORDERS
# MAGIC add columns
# MAGIC (
# MAGIC is_managed int,
# MAGIC TemperatureThreshold string,
# MAGIC TemperatureRange_Min string,
# MAGIC TemperatureRange_Max string,
# MAGIC TemperatureRange_UOM string
# MAGIC );

# COMMAND ----------

# %sql
# create external table gold.digital_summary_onboarded_systems
# (
# sourcesystemkey int,
# sourcesystemname string,
# dl_insert_timestamp	timestamp
# )
# using delta
# location "/mnt/sail/logs/controller.db/digital_summary_onboarded_systems"

# COMMAND ----------

# MAGIC %sql --insert into gold.digital_summary_onboarded_systems
# MAGIC --values (1002,'SPLUS',current_timestamp())
# MAGIC -- insert into gold.digital_summary_onboarded_systems
# MAGIC -- values (1018,'SOFTEON-EFULFILLMENT',current_timestamp())
# MAGIC -- ,(1012,'SOFTEON-BIRKSTOCK',current_timestamp())
# MAGIC -- ,(1020,'SOFTEON-HELIX',current_timestamp())
# MAGIC -- ,(1015,'SOFTEON-PILOT',current_timestamp())
# MAGIC -- ,(1009,'SOFTEON-USPZN',current_timestamp())
# MAGIC -- ,(1011,'MERCURYGATE',current_timestamp())
# MAGIC -- ,(1014,'SOFTEON-APAC',current_timestamp())
# MAGIC -- ,(1016,'SOFTEON-EU',current_timestamp())
# MAGIC -- ,(1006,'SOFTEON-PHILIPS',current_timestamp())
# MAGIC -- ,(1017,'SOFTEON-SWAROSKI',current_timestamp())
# MAGIC -- ,(1004,'SOFTEON',current_timestamp())

# COMMAND ----------

# %sql select * from gold.digital_summary_onboarded_systems

# COMMAND ----------

# %sql
# drop table if exists gold.digital_summary_orders                      ;
# drop table if exists gold.digital_summary_order_lines                 ;
# drop table if exists gold.digital_summary_order_lines_details         ;
# drop table if exists gold.digital_summary_order_tracking              ;
# drop table if exists gold.digital_summary_exceptions                  ;
# drop table if exists gold.digital_summary_milestone                   ;
# drop table if exists gold.digital_summary_milestone_activity          ;
# drop table if exists gold.digital_summary_transportation              ;
# drop table if exists gold.digital_summary_transport_details           ;
# drop table if exists gold.digital_summary_transportation_callcheck    ;
# drop table if exists gold.digital_summary_transportation_rates_charges;
# drop table if exists gold.digital_summary_transportation_references   ;
# drop table if exists gold.digital_summary_inbound_line                ;
# drop table if exists gold.digital_summary_inventory                   ;
# drop table if exists gold.digital_summary_onboarded_systems           ;
# drop table if exists gold.fact_order_dim_inc                          ;