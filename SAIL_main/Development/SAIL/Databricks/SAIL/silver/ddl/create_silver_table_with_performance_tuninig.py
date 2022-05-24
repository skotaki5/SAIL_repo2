# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.map_milestone_activity             ;       
# MAGIC drop table if exists silver.account_type_digital               ;
# MAGIC drop table if exists silver.local_courier_service              ;
# MAGIC drop table if exists silver.map_transactiontype_milestone      ;
# MAGIC drop table if exists silver.map_ordersearchstatus              ;
# MAGIC drop table if exists silver.wh_wip_mapping_activity            ;
# MAGIC drop table if exists silver.dim_item                           ;
# MAGIC drop table if exists silver.dim_source_system                  ;
# MAGIC drop table if exists silver.dim_warehouse                      ;
# MAGIC drop table if exists silver.dim_customer                       ;
# MAGIC drop table if exists silver.dim_carrier_los                    ;
# MAGIC drop table if exists silver.dim_service                        ;
# MAGIC drop table if exists silver.dim_geo_location                   ;
# MAGIC drop table if exists silver.fact_transport_details             ;
# MAGIC drop table if exists silver.fact_transportation_exception      ;
# MAGIC drop table if exists silver.fact_transportation_rates_charges  ;
# MAGIC drop table if exists silver.fact_transportation_references     ;
# MAGIC drop table if exists silver.fact_transportation_callcheck      ;
# MAGIC drop table if exists silver.fact_order_line_details            ;
# MAGIC drop table if exists silver.fact_order_line                    ;
# MAGIC drop table if exists silver.fact_transportation                ;
# MAGIC drop table if exists silver.fact_shipment                      ;
# MAGIC drop table if exists silver.fact_order_reference               ;
# MAGIC drop table if exists silver.fact_order                         ;
# MAGIC drop table if exists silver.fact_milestone_activity            ;
# MAGIC drop table if exists silver.fact_inbound_line                  ;
# MAGIC drop table if exists silver.fact_inventory_snapshot            ;
# MAGIC drop table if exists silver.map_temperature_range_details      ;

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table silver.dim_customer(
# MAGIC SOURCE_SYSTEM_KEY int
# MAGIC ,CUSTOMERKEY	decimal(18,0)
# MAGIC ,CUSTOMER_ACCOUNT_SDUK	string
# MAGIC ,dl_update_timestamp timestamp
# MAGIC ,INT_CUSTOMER_ACCOUNT_NUMBER string
# MAGIC ,INT_CUSTOMER_NUMBER string
# MAGIC ,DIVISION	string
# MAGIC ,EXT_CUSTOMER_ACCOUNT_NUMBER string
# MAGIC ,CUSTOMER_ACCOUNT_NAME	string
# MAGIC ,CUSTOMER_NAME	string
# MAGIC ,ACCOUNT_TYPE	string
# MAGIC ,ADDRESS_LINE_1	string
# MAGIC ,ADDRESS_LINE_2	string
# MAGIC ,CITY	string
# MAGIC ,PROVINCE	string
# MAGIC ,POSTAL_CODE	string
# MAGIC ,COUNTRY	string
# MAGIC ,GLD_ACCOUNT_MAPPED_KEY	string
# MAGIC ,ENABLE_CARRIER_UPDATE_FLAG	int
# MAGIC ,ETL_INSERT_DATE	timestamp
# MAGIC ,ETL_UPDATE_DATE	timestamp
# MAGIC ,ETL_BATCH_NUMBER	decimal(18,0)
# MAGIC ,DP_SERVICELINE_KEY	string
# MAGIC ,DP_ORGENTITY_KEY	string
# MAGIC ,MAPPED_WAREHOUSE_CODE	string
# MAGIC ,CARRIER_HUB_FLAG string
# MAGIC ,is_deleted int
# MAGIC ,dl_hash string 
# MAGIC ,dl_file_name string
# MAGIC ,dl_insert_pipeline_id string
# MAGIC ,dl_insert_timestamp timestamp
# MAGIC ,dl_update_pipeline_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/dim_customer'

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table silver.dim_customer SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='4','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table silver.dim_warehouse(
# MAGIC SOURCE_SYSTEM_KEY	int
# MAGIC ,WAREHOUSE_KEY	decimal(18,0)
# MAGIC ,dl_update_timestamp timestamp
# MAGIC ,WAREHOUSE_CODE	string
# MAGIC ,BUILDING_CODE	string
# MAGIC ,ADDRESS_LINE_1	string
# MAGIC ,ADDRESS_LINE_2	string
# MAGIC ,CITY	string
# MAGIC ,PROVINCE	string
# MAGIC ,POSTAL_CODE	string
# MAGIC ,COUNTRY	string
# MAGIC ,WAREHOUSE_TIME_ZONE	string
# MAGIC ,WAREHOUSE_SDUK	string
# MAGIC ,GLD_WAREHOUSE_MAPPED_KEY	string
# MAGIC ,ETL_INSERT_DATE	timestamp
# MAGIC ,ETL_UPDATE_DATE	timestamp
# MAGIC ,ETL_BATCH_NUMBER	decimal(18,0)
# MAGIC ,is_deleted int
# MAGIC ,dl_hash string
# MAGIC ,dl_file_name string
# MAGIC ,dl_insert_pipeline_id string
# MAGIC ,dl_insert_timestamp timestamp
# MAGIC ,dl_update_pipeline_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/dim_warehouse'

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table silver.dim_warehouse SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='3','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE silver.dim_item (
# MAGIC      SOURCE_SYSTEM_KEY INT
# MAGIC 	,ITEM_KEY DECIMAL(18, 0)
# MAGIC 	,ITEM_SDUK string
# MAGIC     ,dl_update_timestamp TIMESTAMP
# MAGIC 	,ITEM_ID string
# MAGIC 	,PART_NUMBER string
# MAGIC 	,PART_DESCRIPTION string
# MAGIC 	,CUSTOMER_ACCOUNT_NUMBER string
# MAGIC 	,PRIMARY_CUSTOMER_ACCOUNT_NUMBER string
# MAGIC 	,ITEM_DIMENSIONS string
# MAGIC 	,ITEM_DIMENSIONS_UOM string
# MAGIC 	,ITEM_LENGTH DECIMAL(22, 4)
# MAGIC 	,ITEM_WIDTH DECIMAL(22, 4)
# MAGIC 	,ITEM_HEIGHT DECIMAL(22, 4)
# MAGIC 	,ITEM_WEIGHT DECIMAL(22, 4)
# MAGIC 	,ITEM_WEIGHT_UOM string
# MAGIC 	,ITEM_PRICE DECIMAL(22, 4)
# MAGIC 	,ITEM_PRICE_UOM string
# MAGIC 	,HAZMAT_CODE string
# MAGIC 	,HAZMAT_FLAG string
# MAGIC 	,SERIAL_OR_LOT string
# MAGIC 	,HARMONIZED_TARIFF_SCHEDULE_NUMBER string
# MAGIC 	,UN_NUMBER string
# MAGIC 	,ACTIVE_FLAG string
# MAGIC 	,STD_SKU_FLAG string
# MAGIC 	,STD_CASE_FLAG string
# MAGIC 	,CASE_HEIGHT DECIMAL(22, 4)
# MAGIC 	,CASE_WIDTH DECIMAL(22, 4)
# MAGIC 	,CASE_DEPTH DECIMAL(22, 4)
# MAGIC 	,CASE_WEIGHT DECIMAL(22, 4)
# MAGIC 	,STD_PALLET_FLAG string
# MAGIC 	,PALLET_HEIGHT DECIMAL(22, 4)
# MAGIC 	,PALLET_DEPTH DECIMAL(22, 4)
# MAGIC 	,PALLET_WIDTH DECIMAL(22, 4)
# MAGIC 	,PALLET_WEIGHT DECIMAL(22, 4)
# MAGIC 	,UNIT_PER_CASE DECIMAL(22, 0)
# MAGIC 	,ETL_INSERT_DATE TIMESTAMP
# MAGIC 	,ETL_UPDATE_DATE TIMESTAMP
# MAGIC 	,ETL_BATCH_NUMBER DECIMAL(18, 0)
# MAGIC 	,ETL_OPS_DATE TIMESTAMP
# MAGIC 	,SKU_GRP11 string
# MAGIC 	,SKU_GRP1 string
# MAGIC 	,HAZMAT_CLASS string
# MAGIC 	,STRATEGICGOODS_FLAG string
# MAGIC 	,is_deleted INT
# MAGIC 	,dl_hash string
# MAGIC 	,dl_file_name string
# MAGIC 	,dl_insert_pipeline_id string
# MAGIC 	,dl_insert_timestamp TIMESTAMP
# MAGIC 	,dl_update_pipeline_id string
# MAGIC 	)
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/dim_item'

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table silver.dim_item SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='4','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE silver.dim_carrier_los (
# MAGIC     SOURCE_SYSTEM_KEY INT
# MAGIC 	,CARRIER_LOS_KEY DECIMAL(18, 0)	
# MAGIC     ,dl_update_timestamp TIMESTAMP
# MAGIC 	,CARRIER_CODE string
# MAGIC 	,CARRIER_NAME string
# MAGIC 	,CARRIER_TYPE string
# MAGIC 	,LEVEL_OF_SERVICE_CODE string
# MAGIC 	,LEVEL_OF_SERVICE_DESC string
# MAGIC 	,WAREHOUSE_CODE string
# MAGIC 	,CARRIER_NUMERIC_ID INT
# MAGIC 	,EXT_CARRIER_CODE string
# MAGIC 	,CARRIER_GROUP string
# MAGIC 	,CARRIER_LOS_SDUK string
# MAGIC 	,GLD_CARRIER_MAPPED_KEY string
# MAGIC 	,ETL_INSERT_DATE TIMESTAMP
# MAGIC 	,ETL_UPDATE_DATE TIMESTAMP
# MAGIC 	,ETL_BATCH_NUMBER DECIMAL(18, 0)
# MAGIC 	,CARRIER_HUB_FLAG string
# MAGIC 	,is_deleted INT
# MAGIC 	,dl_hash string
# MAGIC 	,dl_file_name string
# MAGIC 	,dl_insert_pipeline_id string
# MAGIC 	,dl_insert_timestamp TIMESTAMP
# MAGIC 	,dl_update_pipeline_id string
# MAGIC 	
# MAGIC 	)
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/dim_carrier_los'

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table silver.dim_carrier_los SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='3','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE silver.dim_service (
# MAGIC     SOURCE_SYSTEM_KEY INT
# MAGIC 	,SERVICE_KEY DECIMAL(18, 0)
# MAGIC 	,SERVICE_SDUK string
# MAGIC     ,dl_update_timestamp TIMESTAMP
# MAGIC 	,SERVICE_CODE string
# MAGIC 	,SERVICE_NAME string
# MAGIC 	,E2K_SERVICE_CODE string
# MAGIC 	,E2K_CHARGE_CODE string
# MAGIC 	,ETL_INSERT_DATE TIMESTAMP
# MAGIC 	,ETL_UPDATE_DATE TIMESTAMP
# MAGIC 	,ETL_BATCH_NUMBER DECIMAL(18, 0)
# MAGIC 	,TRANSPORTATION_NOT_REQUIRED string
# MAGIC 	,is_deleted INT
# MAGIC 	,dl_hash string
# MAGIC 	,dl_file_name string
# MAGIC 	,dl_insert_pipeline_id string
# MAGIC 	,dl_insert_timestamp TIMESTAMP
# MAGIC 	,dl_update_pipeline_id string
# MAGIC 	)
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/dim_service'

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table silver.dim_service SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='4','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE silver.dim_geo_location (
# MAGIC     SOURCE_SYSTEM_KEY INT
# MAGIC 	,GEO_LOCATION_KEY DECIMAL(18, 0)
# MAGIC     ,GEO_LOCATION_SDUK string
# MAGIC     ,dl_update_timestamp TIMESTAMP
# MAGIC 	,LOCATION_ID string
# MAGIC 	,LOCATION_CODE string
# MAGIC 	,LOCATION_NAME string
# MAGIC 	,ADDRESS_LINE_1 string
# MAGIC 	,ADDRESS_LINE_2 string
# MAGIC 	,CITY string
# MAGIC 	,PROVINCE string
# MAGIC 	,POSTAL_CODE string
# MAGIC 	,COUNTRY string
# MAGIC 	,LOCATION_TYPE string
# MAGIC 	,LOCATION_TIME_ZONE string
# MAGIC 	,ETL_INSERT_DATE TIMESTAMP
# MAGIC 	,ETL_UPDATE_DATE TIMESTAMP
# MAGIC 	,ETL_BATCH_NUMBER DECIMAL(18, 0)
# MAGIC 	,is_deleted INT
# MAGIC 	,dl_hash string
# MAGIC 	,dl_file_name string
# MAGIC 	,dl_insert_pipeline_id string
# MAGIC 	,dl_insert_timestamp TIMESTAMP
# MAGIC 	,dl_update_pipeline_id string
# MAGIC 	
# MAGIC 	)
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/dim_geo_location'

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table silver.dim_geo_location SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='4','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE silver.dim_source_system (
# MAGIC 	SOURCE_SYSTEM_KEY INT
# MAGIC 	,SOURCE_SYSTEM_NAME string
# MAGIC 	,SOURCE_SYSTEM_GROUP string
# MAGIC 	,DC_FSL_FLAG string
# MAGIC 	,is_deleted INT
# MAGIC 	,dl_hash string
# MAGIC 	,dl_file_name string
# MAGIC 	,dl_insert_pipeline_id string
# MAGIC 	,dl_insert_timestamp TIMESTAMP
# MAGIC 	,dl_update_pipeline_id string
# MAGIC 	,dl_update_timestamp TIMESTAMP
# MAGIC 	)
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/dim_source_system'

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table silver.fact_milestone_activity(
# MAGIC SOURCE_SYSTEM_KEY int
# MAGIC ,LOAD_TRACK_SDUK	string
# MAGIC ,dl_update_timestamp timestamp
# MAGIC ,CLIENT_KEY	decimal(18,0)
# MAGIC ,WAREHOUSE_KEY	decimal(18,0)
# MAGIC ,UPS_ORDER_NUMBER	string
# MAGIC ,LOAD_ID	string
# MAGIC ,TRACKING_NUMBER	string
# MAGIC ,ACTIVITY_CODE	string
# MAGIC ,ACTIVITY_DESCRIPTION	string
# MAGIC ,ACTIVITY_DATE	timestamp
# MAGIC ,UTC_ACTIVITY_DATE	timestamp
# MAGIC ,LOFST_ACTIVITY_DATE	timestamp
# MAGIC ,ACTIVITY_DATE_KEY	int
# MAGIC ,ACTIVITY_TIME_KEY	int
# MAGIC ,ACTIVITY_COMPLETION_FLAG	string
# MAGIC ,PLANNED_MILESTONE_DATE	timestamp
# MAGIC ,UTC_PLANNED_MILESTONE_DATE	timestamp
# MAGIC ,PLANNED_MILESTONE_DATE_KEY	int
# MAGIC ,PLANNED_MILESTONE_TIME_KEY	int
# MAGIC ,MILESTONE_DATE	timestamp
# MAGIC ,UTC_MILESTONE_DATE	timestamp
# MAGIC ,LOFST_MILESTONE_DATE	timestamp
# MAGIC ,MILESTONE_DATE_KEY	int
# MAGIC ,MILESTONE_TIME_KEY	int
# MAGIC ,MILESTONE_COMPLETION_FLAG	string
# MAGIC ,CLIENT_SDUK	string
# MAGIC ,SHIPMENT_SDUK	string
# MAGIC ,WAREHOUSE_SDUK	string
# MAGIC ,ETL_INSERT_DATE	timestamp
# MAGIC ,ETL_UPDATE_DATE	timestamp
# MAGIC ,ETL_BATCH_NUMBER	decimal(18,0)
# MAGIC ,UPS_WMS_ORDER_NUMBER	string
# MAGIC ,CLIENT_ASN	string
# MAGIC ,SEGMENT_ID	int
# MAGIC ,DML_DATE	timestamp
# MAGIC ,DML_DATE_KEY	int
# MAGIC ,ACTIVITY_NOTES	string
# MAGIC ,VENDOR_NAME	string
# MAGIC ,PROOF_OF_DELIVERY_NAME	string
# MAGIC ,CARRIER_TYPE	string
# MAGIC ,FTZ_STATUS	string
# MAGIC ,TIME_ZONE	string
# MAGIC ,ACTIVITY_STATUS	string
# MAGIC ,LOGI_NEXT_FLAG	string
# MAGIC ,UPS_WMS_SOURCE_SYSTEM_KEY int
# MAGIC ,ACTIVITY_MONTH_PART_KEY bigint
# MAGIC ,is_deleted int
# MAGIC ,dl_hash string
# MAGIC ,dl_file_name string
# MAGIC ,dl_insert_pipeline_id string
# MAGIC ,dl_insert_timestamp timestamp
# MAGIC ,dl_update_pipeline_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/fact_milestone_activity'

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table silver.fact_milestone_activity SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='3','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table silver.fact_order(
# MAGIC SOURCE_SYSTEM_KEY	int
# MAGIC ,ORDER_SDUK	string
# MAGIC ,dl_update_timestamp timestamp
# MAGIC ,CLIENT_KEY	decimal(18,0)
# MAGIC ,WAREHOUSE_KEY	decimal(18,0)
# MAGIC ,CARRIER_LOS_KEY	decimal(18,0)
# MAGIC ,SERVICE_KEY	decimal(18,0)
# MAGIC ,ORIGIN_LOCATION_KEY	decimal(18,0)
# MAGIC ,DESTINATION_LOCATION_KEY	decimal(18,0)
# MAGIC ,UPS_ORDER_NUMBER	string
# MAGIC ,CUSTOMER_ORDER_NUMBER	string
# MAGIC ,REFERENCE_ORDER_NUMBER	string
# MAGIC ,CUSTOMER_PO_NUMBER	string
# MAGIC ,DC_FSL_FLAG	string
# MAGIC ,SOURCE_ORDER_TYPE	string
# MAGIC ,SOURCE_ORDER_SUB_TYPE	string
# MAGIC ,ORIGIN_TIME_ZONE	string
# MAGIC ,DESTINATION_TIME_ZONE	string
# MAGIC ,SOURCE_ORDER_STATUS	string
# MAGIC ,SOURCE_ORDER_SUB_STATUS	string
# MAGIC ,ORDER_CANCELLED_FLAG	string
# MAGIC ,ORDER_PLACED_DATE	timestamp
# MAGIC ,UTC_ORDER_PLACED_DATE	timestamp
# MAGIC ,LOFST_ORDER_PLACED_DATE	timestamp
# MAGIC ,ORDER_SHIPPED_DATE	timestamp
# MAGIC ,UTC_ORDER_SHIPPED_DATE	timestamp
# MAGIC ,LOFST_ORDER_SHIPPED_DATE	timestamp
# MAGIC ,ORDER_CANCELLED_DATE	timestamp
# MAGIC ,UTC_ORDER_CANCELLED_DATE	timestamp
# MAGIC ,LOFST_ORDER_CANCELLED_DATE	timestamp
# MAGIC ,ORDER_PLACED_DATE_KEY	int
# MAGIC ,ORDER_PLACED_TIME_KEY	int
# MAGIC ,ORDER_SHIPPED_DATE_KEY	int
# MAGIC ,ORDER_SHIPPED_TIME_KEY	int
# MAGIC ,ORDER_CANCELLED_DATE_KEY	int
# MAGIC ,ORDER_CANCELLED_TIME_KEY	int
# MAGIC ,HAZMAT_ORDER_COUNT	int
# MAGIC ,SCRAP_ORDER_COUNT	int
# MAGIC ,MEDICAL_ORDER_COUNT	int
# MAGIC ,STO_ORDER_COUNT	int
# MAGIC ,ORDER_COUNT	int
# MAGIC ,CLIENT_SDUK	string
# MAGIC ,WAREHOUSE_SDUK	string
# MAGIC ,CARRIER_LOS_SDUK	string
# MAGIC ,SERVICE_SDUK	string
# MAGIC ,ORIGIN_LOCATION_SDUK	string
# MAGIC ,DESTINATION_LOCATION_SDUK	string
# MAGIC ,ETL_INSERT_DATE	timestamp
# MAGIC ,ETL_UPDATE_DATE	timestamp
# MAGIC ,ETL_BATCH_NUMBER	decimal(18,0)
# MAGIC ,ORDER_REC_CREATED_DATE	timestamp
# MAGIC ,UTC_REC_CREATED_DATE	timestamp
# MAGIC ,LOFST_REC_CREATED_DATE	timestamp
# MAGIC ,SHIPMENT_COUNT	int
# MAGIC ,ORDER_LATEST_ACTIVITY_DATE	timestamp
# MAGIC ,UTC_ORDER_LATEST_ACTIVITY_DATE	timestamp
# MAGIC ,LOFST_ORDER_LATEST_ACTIVITY_DATE	timestamp
# MAGIC ,ORDER_LATEST_ACTIVITY_DATE_KEY	int
# MAGIC ,ORDER_LATEST_ACTIVITY_TIME_KEY	int
# MAGIC ,TRANSACTION_TYPE_ID	int
# MAGIC ,CANCELLED_BY	string
# MAGIC ,IS_MANAGED	int
# MAGIC ,IS_INBOUND	int
# MAGIC ,IS_INTERNATIONAL	int
# MAGIC ,UPS_PO_Number	string
# MAGIC ,IS_ASN	int
# MAGIC ,DONOT_SHIP_BEFORE_DATE	timestamp
# MAGIC ,DONOT_SHIP_BEFORE_DATE_KEY	int
# MAGIC ,DONOT_SHIP_AFTER_DATE	timestamp
# MAGIC ,DONOT_SHIP_AFTER_DATE_KEY	int
# MAGIC ,RECEIVED_DATE	timestamp
# MAGIC ,UTC_RECEIVED_DATE	timestamp
# MAGIC ,UTC_DONOT_SHIP_BEFORE_DATE	timestamp
# MAGIC ,UTC_DONOT_SHIP_AFTER_DATE	timestamp
# MAGIC ,LOFST_DONOT_SHIP_AFTER_DATE	timestamp
# MAGIC ,LOFST_DONOT_SHIP_BEFORE_DATE	timestamp
# MAGIC ,DML_DATE_KEY	int
# MAGIC ,DML_DATE	timestamp
# MAGIC ,FREIGHT_CARRIER_CODE	string
# MAGIC ,WAYBILL_AIRBILL_NUM	string
# MAGIC ,IS_FTZ	int
# MAGIC ,UTC_ORDER_PLACED_MONTH_PART_KEY bigint
# MAGIC ,is_deleted int
# MAGIC ,dl_hash string
# MAGIC ,dl_file_name string
# MAGIC ,dl_insert_pipeline_id string
# MAGIC ,dl_insert_timestamp timestamp
# MAGIC ,dl_update_pipeline_id string
# MAGIC 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/fact_order'

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table silver.fact_order SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='3','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table silver.fact_transport_details(
# MAGIC SOURCE_SYSTEM_KEY	int
# MAGIC ,SHIPMENT_SDUK	string
# MAGIC ,dl_update_timestamp timestamp
# MAGIC ,CLIENT_KEY	decimal(18,0)
# MAGIC ,WAREHOUSE_KEY	decimal(18,0)
# MAGIC ,ITEM_KEY	decimal(18,0)
# MAGIC ,UPS_ORDER_NUMBER	string
# MAGIC ,CLIENT_ASN	string
# MAGIC ,LOAD_ID	string
# MAGIC ,ITEM_ID	string
# MAGIC ,ITEM_DESCRIPTION	string
# MAGIC ,SEQUENCE	bigint
# MAGIC ,CONTAINED_IN	string
# MAGIC ,CLASS	string
# MAGIC ,IS_HAZMAT	string
# MAGIC ,ORDERED_QTY	decimal(22,4)
# MAGIC ,ORDERED_UOM	string
# MAGIC ,PLANNED_QTY	decimal(22,4)
# MAGIC ,PLANNED_UOM	string
# MAGIC ,ACTUAL_QTY	decimal(22,4)
# MAGIC ,ACTUAL_UOM	string
# MAGIC ,PLANNED_WGT	string
# MAGIC ,ACTUAL_WGT	string
# MAGIC ,ITEM_DIMENSION	string
# MAGIC ,COMMODITY	string
# MAGIC ,DELIVERY_STATUS	string
# MAGIC ,CLIENT_SDUK	string
# MAGIC ,ITEM_SDUK	string
# MAGIC ,WAREHOUSE_SDUK	string
# MAGIC ,ETL_INSERT_DATE	timestamp
# MAGIC ,ETL_UPDATE_DATE	timestamp
# MAGIC ,ETL_BATCH_NUMBER	decimal(18,0)
# MAGIC ,TemperatureRange_Min	string
# MAGIC ,TemperatureRange_Max	string
# MAGIC ,TemperatureRange_UOM	string
# MAGIC ,TemperatureRange_Code	string
# MAGIC ,Planned_Weight_UOM	string
# MAGIC ,Actual_Weight_UOM	string
# MAGIC ,Dimension_UOM	string
# MAGIC ,is_deleted int
# MAGIC ,dl_hash string
# MAGIC ,dl_file_name string
# MAGIC ,dl_insert_pipeline_id string
# MAGIC ,dl_insert_timestamp timestamp
# MAGIC ,dl_update_pipeline_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/fact_transport_details'

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table silver.fact_transport_details SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='3','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table silver.fact_transportation(
# MAGIC SOURCE_SYSTEM_KEY	int
# MAGIC ,TRANSPORTATION_SDUK	string
# MAGIC ,dl_update_timestamp timestamp
# MAGIC ,CLIENT_KEY	decimal(18,0)
# MAGIC ,WAREHOUSE_KEY	decimal(18,0)
# MAGIC ,SERVICE_KEY	decimal(18,0)
# MAGIC ,ORIGIN_LOCATION_KEY	decimal(18,0)
# MAGIC ,DESTINATION_LOCATION_KEY	decimal(18,0)
# MAGIC ,PICKUP_CARRIER_LOS_KEY	decimal(18,0)
# MAGIC ,DROPOFF_CARRIER_LOS_KEY	decimal(18,0)
# MAGIC ,UPS_ORDER_NUMBER	string
# MAGIC ,SOURCE_ORDER_TYPE	string
# MAGIC ,EQUIPMENT_TYPE	string
# MAGIC ,SOURCE_ORDER_SUB_TYPE	string
# MAGIC ,ORIGIN_COMPANY	string
# MAGIC ,DESTINATION_COMPANY	string
# MAGIC ,ORIGIN_TIME_ZONE	string
# MAGIC ,DESTINATION_TIME_ZONE	string
# MAGIC ,SOURCE_ORDER_STATE	string
# MAGIC ,SOURCE_ORDER_STATUS	string
# MAGIC ,ORDER_CANCELLED_FLAG	string
# MAGIC ,ORDER_REC_CREATED_DATE	timestamp
# MAGIC ,UTC_ORDER_REC_CREATED_DATE	timestamp
# MAGIC ,LOFST_REC_CREATED_DATE	timestamp
# MAGIC ,ORDER_PLACED_DATE	timestamp
# MAGIC ,UTC_ORDER_PLACED_DATE	timestamp
# MAGIC ,LOFST_ORDER_PLACED_DATE	timestamp
# MAGIC ,ORDER_CANCELLED_DATE	timestamp
# MAGIC ,UTC_ORDER_CANCELLED_DATE	timestamp
# MAGIC ,LOFST_ORDER_CANCELLED_DATE	timestamp
# MAGIC ,ORDER_SHIPPED_DATE	timestamp
# MAGIC ,UTC_ORDER_SHIPPED_DATE	timestamp
# MAGIC ,LOFST_ORDER_SHIPPED_DATE	timestamp
# MAGIC ,SCHEDULED_SHIPMENT_DATE	timestamp
# MAGIC ,UTC_SCHEDULED_SHIPMENT_DATE	timestamp
# MAGIC ,LOFST_SCHEDULED_SHIPMENT_DATE	timestamp
# MAGIC ,ACTUAL_SHIPMENT_DATE	timestamp
# MAGIC ,UTC_ACTUAL_SHIPMENT_DATE	timestamp
# MAGIC ,LOFST_ACTUAL_SHIPMENT_DATE	timestamp
# MAGIC ,SCHEDULED_DELIVERY_DATE	timestamp
# MAGIC ,UTC_SCHEDULED_DELIVERY_DATE	timestamp
# MAGIC ,LOFST_SCHEDULED_DELIVERY_DATE	timestamp
# MAGIC ,ACTUAL_DELIVERY_DATE	timestamp
# MAGIC ,UTC_ACTUAL_DELIVERY_DATE	timestamp
# MAGIC ,LOFST_ACTUAL_DELIVERY_DATE	timestamp
# MAGIC ,ORDER_REC_CREATED_DATE_KEY	int
# MAGIC ,ORDER_REC_CREATED_TIME_KEY	int
# MAGIC ,ORDER_PLACED_DATE_KEY	int
# MAGIC ,ORDER_PLACED_TIME_KEY	int
# MAGIC ,ORDER_CANCELLED_DATE_KEY	int
# MAGIC ,ORDER_CANCELLED_TIME_KEY	int
# MAGIC ,ORDER_SHIPPED_DATE_KEY	int
# MAGIC ,ORDER_SHIPPED_TIME_KEY	int
# MAGIC ,SCHEDULED_SHIPMENT_DATE_KEY	int
# MAGIC ,SCHEDULED_SHIPMENT_TIME_KEY	int
# MAGIC ,ACTUAL_SHIPMENT_DATE_KEY	int
# MAGIC ,ACTUAL_SHIPMENT_TIME_KEY	int
# MAGIC ,SCHEDULED_DELIVERY_DATE_KEY	int
# MAGIC ,SCHEDULED_DELIVERY_TIME_KEY	int
# MAGIC ,ACTUAL_DELIVERY_DATE_KEY	int
# MAGIC ,ACTUAL_DELIVERY_TIME_KEY	int
# MAGIC ,ORDER_COUNT	int
# MAGIC ,CLIENT_SDUK	string
# MAGIC ,WAREHOUSE_SDUK	string
# MAGIC ,SERVICE_SDUK	string
# MAGIC ,ORIGIN_LOCATION_SDUK	string
# MAGIC ,DESTINATION_LOCATION_SDUK	string
# MAGIC ,PICKUP_CARRIER_LOS_SDUK	string
# MAGIC ,DROPOFF_CARRIER_LOS_SDUK	string
# MAGIC ,ETL_INSERT_DATE	timestamp
# MAGIC ,ETL_UPDATE_DATE	timestamp
# MAGIC ,ETL_BATCH_NUMBER	decimal(18,0)
# MAGIC ,UPS_WMS_ORDER_NUMBER	string
# MAGIC ,UPS_WMS_SOURCE_SYSTEM_KEY	int
# MAGIC ,ORIGINAL_SCHEDULED_DELIVERY_DATE	timestamp
# MAGIC ,UTC_ORIGINAL_SCHEDULED_DELIVERY_DATE	timestamp
# MAGIC ,LOFST_ORIGINAL_SCHEDULED_DELIVERY_DATE	timestamp
# MAGIC ,ORIGINAL_SCHEDULED_DELIVERY_DATE_KEY	int
# MAGIC ,ORIGINAL_SCHEDULED_DELIVERY_TIME_KEY	int
# MAGIC ,LOAD_ID	string
# MAGIC ,LOAD_EARLIEST_PICKUP_DATE	timestamp
# MAGIC ,LOAD_LATEST_PICKUP_DATE	timestamp
# MAGIC ,LOAD_EARLIEST_DELIVERY_DATE	timestamp
# MAGIC ,LOAD_LATEST_DELIVERY_DATE	timestamp
# MAGIC ,LOAD_CREATION_DATE	timestamp
# MAGIC ,LOAD_UPDATE_DATE	timestamp
# MAGIC ,TRANSPORT_MILESTONE_1	string
# MAGIC ,TRANSPORT_MILESTONEDATE_1	timestamp
# MAGIC ,UTC_TRANSPORT_MILESTONEDATE_1	timestamp
# MAGIC ,LOFST_TRANSPORT_MILESTONEDATE_1	timestamp
# MAGIC ,TRANSPORT_MILESTONEDATE_1_DATE_KEY	int
# MAGIC ,TRANSPORT_MILESTONEDATE_1_TIME_KEY	int
# MAGIC ,TRANSPORT_MILESTONE_2	string
# MAGIC ,TRANSPORT_MILESTONEDATE_2	timestamp
# MAGIC ,UTC_TRANSPORT_MILESTONEDATE_2	timestamp
# MAGIC ,LOFST_TRANSPORT_MILESTONEDATE_2	timestamp
# MAGIC ,TRANSPORT_MILESTONEDATE_2_DATE_KEY	int
# MAGIC ,TRANSPORT_MILESTONEDATE_2_TIME_KEY	int
# MAGIC ,TRANSPORT_MILESTONE_3	string
# MAGIC ,TRANSPORT_MILESTONEDATE_3	timestamp
# MAGIC ,UTC_TRANSPORT_MILESTONEDATE_3	timestamp
# MAGIC ,LOFST_TRANSPORT_MILESTONEDATE_3	timestamp
# MAGIC ,TRANSPORT_MILESTONEDATE_3_DATE_KEY	int
# MAGIC ,TRANSPORT_MILESTONEDATE_3_TIME_KEY	int
# MAGIC ,TRANSPORT_MILESTONE_4	string
# MAGIC ,TRANSPORT_MILESTONEDATE_4	timestamp
# MAGIC ,UTC_TRANSPORT_MILESTONEDATE_4	timestamp
# MAGIC ,LOFST_TRANSPORT_MILESTONEDATE_4	timestamp
# MAGIC ,TRANSPORT_MILESTONEDATE_4_DATE_KEY	int
# MAGIC ,TRANSPORT_MILESTONEDATE_4_TIME_KEY	int
# MAGIC ,TRANSPORT_MILESTONE_5	string
# MAGIC ,TRANSPORT_MILESTONEDATE_5	timestamp
# MAGIC ,UTC_TRANSPORT_MILESTONEDATE_5	timestamp
# MAGIC ,LOFST_TRANSPORT_MILESTONEDATE_5	timestamp
# MAGIC ,TRANSPORT_MILESTONEDATE_5_DATE_KEY	int
# MAGIC ,TRANSPORT_MILESTONEDATE_5_TIME_KEY	int
# MAGIC ,TRANSPORT_MILESTONE_6	string
# MAGIC ,TRANSPORT_MILESTONEDATE_6	timestamp
# MAGIC ,UTC_TRANSPORT_MILESTONEDATE_6	timestamp
# MAGIC ,LOFST_TRANSPORT_MILESTONEDATE_6	timestamp
# MAGIC ,TRANSPORT_MILESTONEDATE_6_DATE_KEY	int
# MAGIC ,TRANSPORT_MILESTONEDATE_6_TIME_KEY	int
# MAGIC ,CARRIER_CODE	string
# MAGIC ,LEVEL_OF_SERVICE_CODE	string
# MAGIC ,WMS_PO_NUMBER	string
# MAGIC ,CARRIER_MODE	string
# MAGIC ,TRANS_ONLY_FLAG	string
# MAGIC ,SHIPMENT_NOTES	string
# MAGIC ,COMMENTS	string
# MAGIC ,GFF_SHIPMENT_NUMBER	string
# MAGIC ,GFF_SHIPMENT_INSTANCE_NUMBER	string
# MAGIC ,PROOF_OF_DELIVERY_NAME	string
# MAGIC ,SCOPE	string
# MAGIC ,SECTOR	string
# MAGIC ,DIRECTION	string
# MAGIC ,AUTHORIZER_NAME	string
# MAGIC ,DELIVERY_INSTRUCTIONS	string
# MAGIC ,DESTINATION_CONTACT	string
# MAGIC ,UTC_ORDER_PLACED_MONTH_PART_KEY bigint
# MAGIC ,is_deleted int
# MAGIC ,dl_hash string
# MAGIC ,dl_file_name string
# MAGIC ,dl_insert_pipeline_id string
# MAGIC ,dl_insert_timestamp timestamp
# MAGIC ,dl_update_pipeline_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/fact_transportation'

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table silver.fact_transportation SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='3','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table silver.fact_transportation_rates_charges(
# MAGIC SOURCE_SYSTEM_KEY int
# MAGIC , CHARGE_SDUK string
# MAGIC ,dl_update_timestamp timestamp
# MAGIC , CLIENT_KEY decimal(18,0)
# MAGIC , LOAD_ID string
# MAGIC , UPS_ORDER_NUMBER string
# MAGIC , CLIENT_SDUK string
# MAGIC , SEQUENCE_NUMBER string
# MAGIC , CHARGE_TYPE string
# MAGIC , RATE string
# MAGIC , RATE_QUALIFER string
# MAGIC , CHARGE string
# MAGIC , CHARGE_DESCRIPTION string
# MAGIC , CHARGE_LEVEL string
# MAGIC , EDI_CODE string
# MAGIC , FREIGHT_CLASS string
# MAGIC , FAK_FREIGHT_CLASS string
# MAGIC , ETL_INSERT_DATE timestamp
# MAGIC , ETL_UPDATE_DATE timestamp
# MAGIC , ETL_BATCH_NUMBER decimal(18,0)
# MAGIC , CONTRACT_NAME string
# MAGIC , CURRENCY_CODE string
# MAGIC , INVOICE_NUMBER string
# MAGIC ,is_deleted int
# MAGIC ,dl_hash string
# MAGIC ,dl_file_name string
# MAGIC ,dl_insert_pipeline_id string
# MAGIC ,dl_insert_timestamp timestamp
# MAGIC ,dl_update_pipeline_id string
# MAGIC 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/fact_transportation_rates_charges'

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table silver.fact_transportation_rates_charges SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='3','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE silver.fact_transportation_exception (
# MAGIC 	SOURCE_SYSTEM_KEY INT
# MAGIC     ,TRANSPORTATION_EXCEPTION_SDUK string
# MAGIC     ,dl_update_timestamp TIMESTAMP
# MAGIC 	,UPS_ORDER_NUMBER string
# MAGIC 	,UTC_EXCEPTION_CREATED_DATE TIMESTAMP
# MAGIC 	,EXCEPTION_CREATED_DATE_OTZ TIMESTAMP
# MAGIC 	,LOFST_EXCEPTION_CREATED_DATE_OTZ TIMESTAMP
# MAGIC 	,EXCEPTION_CREATED_DATE_DTZ TIMESTAMP
# MAGIC 	,LOFST_EXCEPTION_CREATED_DATE_DTZ TIMESTAMP
# MAGIC 	,EXCEPTION_DESCRIPTION string
# MAGIC 	,EXCEPTION_EVENT string
# MAGIC 	,EXCEPTION_REASON string
# MAGIC 	,EXCEPTION_REASON_TYPE string
# MAGIC 	,EXCEPTION_CATEGORY string
# MAGIC 	,RESPONSIBLE_PARTY string
# MAGIC 	,EXCEPTION_PRIMARY_INDICATOR DECIMAL(1, 0)
# MAGIC 	,EXCEPTION_COUNT INT
# MAGIC 	,ETL_INSERT_DATE TIMESTAMP
# MAGIC 	,ETL_UPDATE_DATE TIMESTAMP
# MAGIC 	,ETL_BATCH_NUMBER DECIMAL(18, 0)
# MAGIC 	,UPS_WMS_SOURCE_SYSTEM_KEY INT
# MAGIC 	,UPS_WMS_ORDER_NUMBER string
# MAGIC 	,CLIENT_SDUK string
# MAGIC 	,WAREHOUSE_SDUK string
# MAGIC 	,is_deleted INT
# MAGIC 	,dl_hash string
# MAGIC 	,dl_file_name string
# MAGIC 	,dl_insert_pipeline_id string
# MAGIC 	,dl_insert_timestamp TIMESTAMP
# MAGIC 	,dl_update_pipeline_id string
# MAGIC 	)
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/fact_transportation_exception'

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table silver.fact_transportation_exception SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='3','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE silver.fact_transportation_references (
# MAGIC 	SOURCE_SYSTEM_KEY INT
# MAGIC     ,REFERENCE_SDUK string
# MAGIC     ,dl_update_timestamp TIMESTAMP
# MAGIC 	,CLIENT_KEY DECIMAL(18, 0)
# MAGIC 	,LOAD_ID string
# MAGIC 	,UPS_ORDER_NUMBER string
# MAGIC 	,CLIENT_SDUK string
# MAGIC 	,SHIPUNIT_ID string
# MAGIC 	,REFERENCE_TYPE string
# MAGIC 	,REFRENCE_VALUE string
# MAGIC 	,REFERENCE_LEVEL string
# MAGIC 	,ETL_INSERT_DATE TIMESTAMP
# MAGIC 	,ETL_UPDATE_DATE TIMESTAMP
# MAGIC 	,ETL_BATCH_NUMBER DECIMAL(18, 0)
# MAGIC 	,is_deleted INT
# MAGIC 	,dl_hash string
# MAGIC 	,dl_file_name string
# MAGIC 	,dl_insert_pipeline_id string
# MAGIC 	,dl_insert_timestamp TIMESTAMP
# MAGIC 	,dl_update_pipeline_id string
# MAGIC 	)
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/fact_transportation_references'

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table silver.fact_transportation_references SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='3','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE silver.fact_order_line (
# MAGIC 	SOURCE_SYSTEM_KEY INT
# MAGIC     ,ORDER_LINE_SDUK string
# MAGIC     ,dl_update_timestamp TIMESTAMP
# MAGIC 	,CLIENT_KEY DECIMAL(18, 0)
# MAGIC 	,WAREHOUSE_KEY DECIMAL(18, 0)
# MAGIC 	,CARRIER_LOS_KEY DECIMAL(18, 0)
# MAGIC 	,SERVICE_KEY DECIMAL(18, 0)
# MAGIC 	,ORIGIN_LOCATION_KEY DECIMAL(18, 0)
# MAGIC 	,DESTINATION_LOCATION_KEY DECIMAL(18, 0)
# MAGIC 	,ITEM_KEY DECIMAL(18, 0)
# MAGIC 	,UPS_ORDER_NUMBER string
# MAGIC 	,UPS_ORDER_LINE_NUMBER string
# MAGIC 	,DC_FSL_FLAG string
# MAGIC 	,SOURCE_ORDER_LINE_STATUS string
# MAGIC 	,SOURCE_ORDER_LINE_SUB_STATUS string
# MAGIC 	,ORDER_CANCELLED_FLAG string
# MAGIC 	,ORDER_LINE_CANCELLED_FLAG string
# MAGIC 	,ORDER_PLACED_DATE TIMESTAMP
# MAGIC 	,UTC_ORDER_PLACED_DATE TIMESTAMP
# MAGIC 	,LOFST_ORDER_PLACED_DATE TIMESTAMP
# MAGIC 	,ORDER_LINE_CREATED_DATE TIMESTAMP
# MAGIC 	,UTC_ORDER_LINE_CREATED_DATE TIMESTAMP
# MAGIC 	,LOFST_ORDER_LINE_CREATED_DATE TIMESTAMP
# MAGIC 	,OL_PICK_RELEASED_DATE TIMESTAMP
# MAGIC 	,UTC_OL_PICK_RELEASED_DATE TIMESTAMP
# MAGIC 	,LOFST_OL_PICK_RELEASED_DATE TIMESTAMP
# MAGIC 	,OL_PICKED_DATE TIMESTAMP
# MAGIC 	,UTC_OL_PICKED_DATE TIMESTAMP
# MAGIC 	,LOFST_OL_PICKED_DATE TIMESTAMP
# MAGIC 	,OL_SHIPPED_DATE TIMESTAMP
# MAGIC 	,UTC_OL_SHIPPED_DATE TIMESTAMP
# MAGIC 	,LOFST_OL_SHIPPED_DATE TIMESTAMP
# MAGIC 	,OL_CANCELLED_DATE TIMESTAMP
# MAGIC 	,UTC_OL_CANCELLED_DATE TIMESTAMP
# MAGIC 	,LOFST_OL_CANCELLED_DATE TIMESTAMP
# MAGIC 	,ORDER_PLACED_DATE_KEY INT
# MAGIC 	,ORDER_PLACED_TIME_KEY INT
# MAGIC 	,OL_CREATED_DATE_KEY INT
# MAGIC 	,OL_CREATED_TIME_KEY INT
# MAGIC 	,OL_PICK_RELEASED_DATE_KEY INT
# MAGIC 	,OL_PICK_RELEASED_TIME_KEY INT
# MAGIC 	,OL_PICKED_DATE_KEY INT
# MAGIC 	,OL_PICKED_TIME_KEY INT
# MAGIC 	,OL_SHIPPED_DATE_KEY INT
# MAGIC 	,OL_SHIPPED_TIME_KEY INT
# MAGIC 	,OL_CANCELLED_DATE_KEY INT
# MAGIC 	,OL_CANCELLED_TIME_KEY INT
# MAGIC 	,ORDER_LINE_COUNT INT
# MAGIC 	,ORDER_LINE_QUANTITY DECIMAL(22, 4)
# MAGIC 	,CLIENT_SDUK string
# MAGIC 	,WAREHOUSE_SDUK string
# MAGIC 	,CARRIER_LOS_SDUK string
# MAGIC 	,SERVICE_SDUK string
# MAGIC 	,ORIGIN_LOCATION_SDUK string
# MAGIC 	,DESTINATION_LOCATION_SDUK string
# MAGIC 	,ITEM_SDUK string
# MAGIC 	,ETL_INSERT_DATE TIMESTAMP
# MAGIC 	,ETL_UPDATE_DATE TIMESTAMP
# MAGIC 	,ETL_BATCH_NUMBER DECIMAL(18, 0)
# MAGIC 	,SHIPPED_QUANTITY DECIMAL(22, 4)
# MAGIC 	,CANCEL_REASON string
# MAGIC 	,STD_SKU_FLAG string
# MAGIC 	,DML_DATE TIMESTAMP
# MAGIC 	,DML_DATE_KEY INT
# MAGIC 	,UPS_ORDER_LINE_REF_VALUE_1 string
# MAGIC 	,UPS_ORDER_LINE_REF_VALUE_2 string
# MAGIC 	,UPS_ORDER_LINE_REF_VALUE_3 string
# MAGIC 	,UPS_ORDER_LINE_REF_VALUE_4 string
# MAGIC 	,UPS_ORDER_LINE_REF_VALUE_5 string
# MAGIC     ,UTC_ORDER_PLACED_MONTH_PART_KEY bigint
# MAGIC 	,dl_file_name string
# MAGIC 	,is_deleted INT
# MAGIC 	,dl_hash string
# MAGIC 	,dl_insert_pipeline_id string
# MAGIC 	,dl_insert_timestamp TIMESTAMP
# MAGIC 	,dl_update_pipeline_id string
# MAGIC 	
# MAGIC 	)
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/fact_order_line'

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table silver.fact_order_line SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='3','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE silver.fact_order_reference (
# MAGIC 	SOURCE_SYSTEM_KEY INT
# MAGIC     ,ORDER_REFERENCE_SDUK string
# MAGIC     ,QUERY_SEQUENCE INT
# MAGIC     ,dl_update_timestamp TIMESTAMP
# MAGIC 	,UPS_ORDER_NUMBER string
# MAGIC 	,ORDER_REF_1_LABEL string
# MAGIC 	,ORDER_REF_1_VALUE string
# MAGIC 	,ORDER_REF_2_LABEL string
# MAGIC 	,ORDER_REF_2_VALUE string
# MAGIC 	,ORDER_REF_3_LABEL string
# MAGIC 	,ORDER_REF_3_VALUE string
# MAGIC 	,ORDER_REF_4_LABEL string
# MAGIC 	,ORDER_REF_4_VALUE string
# MAGIC 	,ORDER_REF_5_LABEL string
# MAGIC 	,ORDER_REF_5_VALUE string
# MAGIC 	,ORDER_REF_6_LABEL string
# MAGIC 	,ORDER_REF_6_VALUE string
# MAGIC 	,ORDER_REF_7_LABEL string
# MAGIC 	,ORDER_REF_7_VALUE string
# MAGIC 	,ORDER_REF_8_LABEL string
# MAGIC 	,ORDER_REF_8_VALUE string
# MAGIC 	,ORDER_REF_9_LABEL string
# MAGIC 	,ORDER_REF_9_VALUE string
# MAGIC 	,ORDER_REF_10_LABEL string
# MAGIC 	,ORDER_REF_10_VALUE string
# MAGIC 	,ORDER_REF_11_LABEL string
# MAGIC 	,ORDER_REF_11_VALUE string
# MAGIC 	,ORDER_REF_12_LABEL string
# MAGIC 	,ORDER_REF_12_VALUE string
# MAGIC 	,ORDER_REF_13_LABEL string
# MAGIC 	,ORDER_REF_13_VALUE string
# MAGIC 	,ORDER_REF_14_LABEL string
# MAGIC 	,ORDER_REF_14_VALUE string
# MAGIC 	,ORDER_REF_15_LABEL string
# MAGIC 	,ORDER_REF_15_VALUE string
# MAGIC 	,ETL_INSERT_DATE TIMESTAMP
# MAGIC 	,ETL_UPDATE_DATE TIMESTAMP
# MAGIC 	,ETL_BATCH_NUMBER DECIMAL(18, 0)
# MAGIC 	
# MAGIC 	,DML_DATE TIMESTAMP
# MAGIC 	,DML_DATE_KEY INT
# MAGIC 	,is_deleted INT
# MAGIC 	,dl_hash string
# MAGIC 	,dl_file_name string
# MAGIC 	,dl_insert_pipeline_id string
# MAGIC 	,dl_insert_timestamp TIMESTAMP
# MAGIC 	,dl_update_pipeline_id string
# MAGIC 	
# MAGIC 	)
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/fact_order_reference'

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table silver.fact_order_reference SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='4','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE silver.fact_shipment (
# MAGIC 	SOURCE_SYSTEM_KEY INT
# MAGIC     ,SHIPMENT_SDUK string
# MAGIC     ,dl_update_timestamp TIMESTAMP
# MAGIC 	,SHIPMENT_NUMBER string
# MAGIC 	,UPS_ORDER_NUMBER string
# MAGIC 	,SHIPMENT_SEQUENCE_NUMBER string
# MAGIC 	,TRACKING_NUMBER string
# MAGIC 	,CARRIER_CODE string
# MAGIC 	,LEVEL_OF_SERVICE_CODE string
# MAGIC 	,SHIPMENT_CREATION_DATE TIMESTAMP
# MAGIC 	,UTC_SHIPMENT_CREATION_DATE TIMESTAMP
# MAGIC 	,LOFST_SHIPMENT_CREATION_DATE TIMESTAMP
# MAGIC 	,SHIPMENT_CREATION_DATE_KEY INT
# MAGIC 	,SHIPMENT_CREATION_TIME_KEY INT
# MAGIC 	,SHIPMENT_LENGTH DECIMAL(22, 4)
# MAGIC 	,SHIPMENT_WIDTH DECIMAL(22, 4)
# MAGIC 	,SHIPMENT_HEIGHT DECIMAL(22, 4)
# MAGIC 	,SHIPMENT_WEIGHT DECIMAL(22, 4)
# MAGIC 	,SHIPMENT_QUANTITY DECIMAL(22, 4)
# MAGIC 	
# MAGIC 	,ORDER_SDUK string
# MAGIC 	,LOAD_ID string
# MAGIC 	,CARRIER_TYPE string
# MAGIC 	,ETL_INSERT_DATE TIMESTAMP
# MAGIC 	,ETL_UPDATE_DATE TIMESTAMP
# MAGIC 	,ETL_BATCH_NUMBER DECIMAL(18, 0)
# MAGIC 	,DML_DATE TIMESTAMP
# MAGIC 	,DML_DATE_KEY INT
# MAGIC 	,CUBAGE DECIMAL(22, 4)
# MAGIC 	,TemperatureRange_Min string
# MAGIC 	,TemperatureRange_Max string
# MAGIC 	,TemperatureRange_UOM string
# MAGIC 	,TemperatureRange_Code string
# MAGIC 	,Actual_Weight_UOM string
# MAGIC 	,Dimension_UOM string
# MAGIC 	,SHIPMENT_DESCRIPTION string
# MAGIC 	,LOAD_AREA DECIMAL(38, 6)
# MAGIC 	,UOM string
# MAGIC     ,UTC_SHIPMENT_CREATION_MONTH_PART_KEY bigint
# MAGIC 	,is_deleted INT
# MAGIC 	,dl_hash string
# MAGIC 	,dl_file_name string
# MAGIC 	,dl_insert_pipeline_id string
# MAGIC 	,dl_insert_timestamp TIMESTAMP
# MAGIC 	,dl_update_pipeline_id string
# MAGIC 	
# MAGIC 	)
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/fact_shipment'

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table silver.fact_shipment SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='3','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE silver.fact_order_line_details (
# MAGIC 	SOURCE_SYSTEM_KEY INT
# MAGIC     ,ORDER_LINE_DETAILS_SDUK string
# MAGIC     ,dl_update_timestamp TIMESTAMP
# MAGIC 	,CLIENT_KEY DECIMAL(18, 0)
# MAGIC 	,WAREHOUSE_KEY DECIMAL(18, 0)
# MAGIC 	,ITEM_KEY DECIMAL(18, 0)
# MAGIC 	,UPS_ORDER_NUMBER string
# MAGIC 	,UPS_ORDER_LINE_NUMBER string
# MAGIC 	,UPS_ORDER_LINE_DETAIL_NUMBER string
# MAGIC 	,VENDOR_SERIAL_NUMBER string
# MAGIC 	,VENDOR_LOT_NUMBER string
# MAGIC 	,LPN_NUMBER string
# MAGIC 	,DISPOSITION_VALUE string
# MAGIC 	
# MAGIC 	,ORDER_LINE_SDUK string
# MAGIC 	,CLIENT_SDUK string
# MAGIC 	,WAREHOUSE_SDUK string
# MAGIC 	,ITEM_SDUK string
# MAGIC 	,ETL_INSERT_DATE TIMESTAMP
# MAGIC 	,ETL_UPDATE_DATE TIMESTAMP
# MAGIC 	,ETL_BATCH_NUMBER DECIMAL(18, 0)
# MAGIC 	,DML_DATE TIMESTAMP
# MAGIC 	,DML_DATE_KEY INT
# MAGIC 	,IS_INBOUND INT
# MAGIC 	,SHELF_LIFE INT
# MAGIC 	,EXPIRATION_DATE TIMESTAMP
# MAGIC 	,PRODUCT_STATUS string
# MAGIC 	,STORAGE_TYPE string
# MAGIC 	,UTC_EXPIRATION_DATE TIMESTAMP
# MAGIC 	,LOFST_EXPIRATION_DATE TIMESTAMP
# MAGIC 	,HOLD_CODE string
# MAGIC 	,is_deleted INT
# MAGIC 	,dl_hash string
# MAGIC 	,dl_file_name string
# MAGIC 	,dl_insert_pipeline_id string
# MAGIC 	,dl_insert_timestamp TIMESTAMP
# MAGIC 	,dl_update_pipeline_id string
# MAGIC 	
# MAGIC 	)
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/fact_order_line_details'

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table silver.fact_order_line_details SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='3','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table silver.fact_transportation_callcheck(
# MAGIC SOURCE_SYSTEM_KEY INT
# MAGIC ,CALLCHECK_SDUK string
# MAGIC ,dl_update_timestamp timestamp
# MAGIC ,SourceName string
# MAGIC ,CLIENT_KEY DECIMAL(18, 0)
# MAGIC ,WAREHOUSE_KEY DECIMAL(18, 0)
# MAGIC ,CARRIER_LOS_KEY DECIMAL(18, 0)
# MAGIC ,ORIGIN_LOCATION_KEY DECIMAL(18, 0)
# MAGIC ,DESTINATION_LOCATION_KEY DECIMAL(18, 0)
# MAGIC ,UPS_ORDER_NUMBER string
# MAGIC ,BatteryPercent INT
# MAGIC ,LOAD_ID string
# MAGIC ,ACTIVITYTYPE string
# MAGIC ,ACTIVITYID string
# MAGIC ,SUMMARY string
# MAGIC ,CALLCHECK_PRIORITY string
# MAGIC ,IS_REQUIRED string
# MAGIC ,CALLCHECK_STATUS string
# MAGIC ,ASSIGNEDTO string
# MAGIC ,PLANNED_DATE TIMESTAMP
# MAGIC ,UTC_PLANNED_DATE TIMESTAMP
# MAGIC ,PERCENTAGECOMPLETE string
# MAGIC ,COMPLETE_DATE TIMESTAMP
# MAGIC ,UTC_COMPLETE_DATE TIMESTAMP
# MAGIC ,STATUSDETAILTYPE string
# MAGIC ,STATUSDETAIL string
# MAGIC ,Latitude DECIMAL(18, 2)
# MAGIC ,Longitude DECIMAL(18, 2)
# MAGIC ,DeviceTagId string
# MAGIC ,Humidity INT
# MAGIC ,Light DECIMAL(18, 2)
# MAGIC ,LocationMethod string
# MAGIC ,IsMotionDetected boolean
# MAGIC ,Pressure DECIMAL(18, 2)
# MAGIC ,ADDRESSTYPE string
# MAGIC ,ADDRESSISRESIDENTIAL string
# MAGIC ,ADDRESSISPRIMARY string
# MAGIC ,ADDRESSLOCATIONCODE string
# MAGIC ,ADDRESSNAME string
# MAGIC ,ADDRESSLINE1 string
# MAGIC ,ADDRESSLINE2 string
# MAGIC ,CITY string
# MAGIC ,STATEPROVINCE string
# MAGIC ,POSTALCODE string
# MAGIC ,COUNTRYCODE string
# MAGIC ,GEOLATDEGREES string
# MAGIC ,GEOLATDIRECTION string
# MAGIC ,GEOLONGDEGREES string
# MAGIC ,GEOLONGDIRECTION string
# MAGIC ,CONTACTNAME string
# MAGIC ,TemperatureC DECIMAL(18, 2)
# MAGIC ,TemperatureF DECIMAL(18, 2)
# MAGIC ,IsButtonPushed boolean
# MAGIC ,IsShockExceeded boolean
# MAGIC ,CLIENT_SDUK string
# MAGIC ,WAREHOUSE_SDUK string
# MAGIC ,CARRIER_LOS_SDUK string
# MAGIC ,ORIGIN_LOCATION_SDUK string
# MAGIC ,DESTINATION_LOCATION_SDUK string
# MAGIC ,ETL_INSERT_DATE TIMESTAMP
# MAGIC ,ETL_UPDATE_DATE TIMESTAMP
# MAGIC ,ETL_BATCH_NUMBER DECIMAL(18, 0)
# MAGIC ,IS_TEMPERATURE string
# MAGIC ,is_deleted int
# MAGIC ,dl_hash string
# MAGIC ,dl_file_name string
# MAGIC ,dl_insert_pipeline_id string
# MAGIC ,dl_insert_timestamp timestamp
# MAGIC ,dl_update_pipeline_id string
# MAGIC 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/fact_transportation_callcheck'  
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table silver.fact_transportation_callcheck SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='3','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table silver.map_milestone_activity(
# MAGIC MilestoneId INT
# MAGIC ,MilestoneName string
# MAGIC ,ActivityId INT
# MAGIC ,ActivityName string
# MAGIC ,ActivityCode string
# MAGIC ,Milestone_Completion_Flag string
# MAGIC ,SOURCE_SYSTEM_KEY INT
# MAGIC ,SourceActivityName string
# MAGIC ,is_deleted int
# MAGIC ,dl_hash string
# MAGIC ,dl_file_name string
# MAGIC ,dl_insert_pipeline_id string
# MAGIC ,dl_insert_timestamp timestamp
# MAGIC ,dl_update_pipeline_id string
# MAGIC ,dl_update_timestamp timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/map_milestone_activity'

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table silver.account_type_digital(
# MAGIC  Account_ID string
# MAGIC ,Account_TYPE string
# MAGIC ,Account_Name string
# MAGIC ,is_deleted int
# MAGIC ,dl_hash string
# MAGIC ,dl_file_name string
# MAGIC ,dl_insert_pipeline_id string
# MAGIC ,dl_insert_timestamp timestamp
# MAGIC ,dl_update_pipeline_id string
# MAGIC ,dl_update_timestamp timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/account_type_digital'

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table silver.local_courier_service(
# MAGIC  ID INT
# MAGIC ,SOURCE_SYSTEM_KEY INT
# MAGIC ,SERVICE_NAME string
# MAGIC ,CARRIERNAME string
# MAGIC ,SERVICELEVELNAME string
# MAGIC ,is_deleted int
# MAGIC ,dl_hash string
# MAGIC ,dl_file_name string
# MAGIC ,dl_insert_pipeline_id string
# MAGIC ,dl_insert_timestamp timestamp
# MAGIC ,dl_update_pipeline_id string
# MAGIC ,dl_update_timestamp timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/local_courier_service'

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table silver.map_ordersearchstatus(
# MAGIC SourceSystemName string
# MAGIC ,OrderStatusName string
# MAGIC ,OrderStatusCode string
# MAGIC ,SOURCE_SYSTEM_KEY INT
# MAGIC ,is_deleted int
# MAGIC ,dl_hash string
# MAGIC ,dl_file_name string
# MAGIC ,dl_insert_pipeline_id string
# MAGIC ,dl_insert_timestamp timestamp
# MAGIC ,dl_update_pipeline_id string
# MAGIC ,dl_update_timestamp timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/map_ordersearchstatus'

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table silver.map_transactiontype_milestone(
# MAGIC 
# MAGIC TransactionTypeId INT
# MAGIC ,TransactionTypeName string
# MAGIC ,MilestoneId INT
# MAGIC ,MilestoneName string
# MAGIC ,MilestoneOrder INT
# MAGIC ,Is_Managed INT
# MAGIC ,Is_Inbound INT
# MAGIC ,Is_International INT
# MAGIC ,SOURCE_SYSTEM_KEY INT
# MAGIC ,IS_FTZ INT
# MAGIC ,is_deleted int
# MAGIC ,dl_hash string
# MAGIC ,dl_file_name string
# MAGIC ,dl_insert_pipeline_id string
# MAGIC ,dl_insert_timestamp timestamp
# MAGIC ,dl_update_pipeline_id string
# MAGIC ,dl_update_timestamp timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/map_transactiontype_milestone'

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table silver.fact_inbound_line(
# MAGIC 
# MAGIC SOURCE_SYSTEM_KEY INT
# MAGIC ,INBOUND_LINE_SDUK string
# MAGIC ,dl_update_timestamp timestamp
# MAGIC ,CLIENT_KEY DECIMAL(18, 0)
# MAGIC ,WAREHOUSE_KEY DECIMAL(18, 0)
# MAGIC ,ITEM_KEY DECIMAL(18, 0)
# MAGIC ,SOURCE_INBOUND_HEADER_NUMBER string
# MAGIC ,SOURCE_INBOUND_LINE_NUMBER string
# MAGIC ,ASN_HEADER_NUMBER string
# MAGIC ,ASN_LINE_NUMBER string
# MAGIC ,PO_HEADER_NUMBER string
# MAGIC ,PO_LINE_NUMBER string
# MAGIC ,RCPT_HEADER_NUMBER string
# MAGIC ,RCPT_LINE_NUMBER string
# MAGIC ,SOURCE_INBOUND_HEADER_TYPE string
# MAGIC ,SOURCE_INBOUND_HEADER_STATUS string
# MAGIC ,SOURCE_INBOUND_LINE_STATUS string
# MAGIC ,INBND_CARRIER_NAME string
# MAGIC ,INBND_HDR_CREATION_DATE TIMESTAMP
# MAGIC ,UTC_INBND_HDR_CREATION_DATE TIMESTAMP
# MAGIC ,LOFST_INBND_HDR_CREATION_DATE TIMESTAMP
# MAGIC ,INBND_LINE_CREATION_DATE TIMESTAMP
# MAGIC ,UTC_INBND_LINE_CREATION_DATE TIMESTAMP
# MAGIC ,LOFST_INBND_LINE_CREATION_DATE TIMESTAMP
# MAGIC ,INBND_HDR_SHIPPED_DATE TIMESTAMP
# MAGIC ,UTC_INBND_HDR_SHIPPED_DATE TIMESTAMP
# MAGIC ,LOFST_INBND_HDR_SHIPPED_DATE TIMESTAMP
# MAGIC ,IL_FIRST_RCVD_DATE TIMESTAMP
# MAGIC ,UTC_IL_FIRST_RCVD_DATE TIMESTAMP
# MAGIC ,LOFST_IL_FIRST_RCVD_DATE TIMESTAMP
# MAGIC ,IL_LAST_PUTAWAY_DATE TIMESTAMP
# MAGIC ,UTC_IL_LAST_PUTAWAY_DATE TIMESTAMP
# MAGIC ,LOFST_IL_LAST_PUTAWAY_DATE TIMESTAMP
# MAGIC ,INBND_LINE_SHIPPED_QTY DECIMAL(22, 4)
# MAGIC ,INBND_LINE_RECEIVED_QTY DECIMAL(22, 4)
# MAGIC ,CLIENT_SDUK string
# MAGIC ,WAREHOUSE_SDUK string
# MAGIC ,ITEM_SDUK string
# MAGIC ,ETL_INSERT_DATE TIMESTAMP
# MAGIC ,ETL_UPDATE_DATE TIMESTAMP
# MAGIC ,ETL_BATCH_NUMBER DECIMAL(18, 0)
# MAGIC ,INBND_HDR_CREATION_DATE_KEY INT
# MAGIC ,INBND_HDR_CREATION_TIME_KEY INT
# MAGIC ,INBND_LINE_CREATION_DATE_KEY INT
# MAGIC ,INBND_LINE_CREATION_TIME_KEY INT
# MAGIC ,INBND_HDR_SHIPPED_DATE_KEY INT
# MAGIC ,INBND_HDR_SHIPPED_TIME_KEY INT
# MAGIC ,IL_FIRST_RCVD_DATE_KEY INT
# MAGIC ,IL_FIRST_RCVD_TIME_KEY INT
# MAGIC ,IL_LAST_PUTAWAY_DATE_KEY INT
# MAGIC ,IL_LAST_PUTAWAY_TIME_KEY INT
# MAGIC ,SOURCE_PO_STATUS string
# MAGIC ,SOURCE_PO_SUB_STATUS string
# MAGIC ,INBND_LINE_RECEIVED_CASES DECIMAL(18, 0)
# MAGIC ,DML_DATE TIMESTAMP
# MAGIC ,DML_DATE_KEY INT
# MAGIC ,SOURCE_INBOUND_LINE_REFERENCE_2 string
# MAGIC ,SOURCE_INBOUND_LINE_REFERENCE_10 string
# MAGIC ,SOURCE_INBOUND_LINE_REFERENCE_11 string
# MAGIC ,SOURCE_INBOUND_LINE_REFERENCE_1 string
# MAGIC ,SOURCE_INBOUND_LINE_REFERENCE_3 string
# MAGIC ,SOURCE_INBOUND_LINE_REFERENCE_4 string
# MAGIC ,SOURCE_INBOUND_LINE_REFERENCE_5 string
# MAGIC ,is_deleted int
# MAGIC ,dl_hash string
# MAGIC ,dl_file_name string
# MAGIC ,dl_insert_pipeline_id string
# MAGIC ,dl_insert_timestamp timestamp
# MAGIC ,dl_update_pipeline_id string
# MAGIC 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/fact_inbound_line'

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table silver.fact_inbound_line SET TBLPROPERTIES ('dataSkippingNumIndexedCols'='3','targetFileSize'='33554432','tuneFileSizesForRewrites'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table silver.wh_wip_mapping_activity(
# MAGIC Type string
# MAGIC ,MilestoneName string
# MAGIC ,ActivityName string
# MAGIC ,ActivityCode string
# MAGIC ,WIP_ActivityName string
# MAGIC ,SOURCE_SYSTEM_KEY INT
# MAGIC ,WIPActivityOrderId INT
# MAGIC ,is_deleted int
# MAGIC ,dl_hash string
# MAGIC ,dl_file_name string
# MAGIC ,dl_insert_pipeline_id string
# MAGIC ,dl_insert_timestamp timestamp
# MAGIC ,dl_update_pipeline_id string
# MAGIC ,dl_update_timestamp timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/wh_wip_mapping_activity'

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table silver.fact_inventory_snapshot(
# MAGIC     SOURCE_SYSTEM_KEY INT ,
# MAGIC     dl_update_timestamp string,
# MAGIC 	CLIENT_KEY DECIMAL(18, 0) ,
# MAGIC 	WAREHOUSE_KEY DECIMAL(18, 0) ,
# MAGIC 	ITEM_KEY DECIMAL(18, 0) ,
# MAGIC 	LPN_NUMBER STRING ,
# MAGIC 	DISPOSITION_CODE STRING ,
# MAGIC 	SERIAL_OR_LOT STRING ,
# MAGIC 	LPN_HDR_CREATION_DATE TIMESTAMP ,
# MAGIC 	UTC_LPN_HDR_CREATION_DATE TIMESTAMP ,
# MAGIC 	LOFST_LPN_HDR_CREATION_DATE TIMESTAMP ,
# MAGIC 	RECEIVED_DATE TIMESTAMP ,
# MAGIC 	UTC_RECEIVED_DATE TIMESTAMP ,
# MAGIC 	LOFST_RECEIVED_DATE TIMESTAMP ,
# MAGIC 	EXPIRATION_DATE TIMESTAMP ,
# MAGIC 	UTC_EXPIRATION_DATE TIMESTAMP ,
# MAGIC 	LOFST_EXPIRATION_DATE TIMESTAMP ,
# MAGIC 	RECEIVED_QUANTITY DECIMAL(22, 4) ,
# MAGIC 	ON_HAND_QUANTITY DECIMAL(22, 4) ,
# MAGIC 	ON_HOLD_QUANTITY DECIMAL(22, 4) ,
# MAGIC 	ALLOCATED_QUANTITY DECIMAL(22, 4) ,
# MAGIC 	AVAILABLE_QUANTITY DECIMAL(22, 4) ,
# MAGIC 	UNALLOCATABLE_QUANTITY DECIMAL(22, 4) ,
# MAGIC 	PRN_QUANTITY DECIMAL(22, 4) ,
# MAGIC 	QUARANTINE_QUANTITY DECIMAL(22, 4) ,
# MAGIC 	POTENTIAL_VARIANCE_QUANTITY DECIMAL(22, 4) ,
# MAGIC 	GOOD_QUANTITY DECIMAL(22, 4) ,
# MAGIC 	NEW_QUANTITY DECIMAL(22, 4) ,
# MAGIC 	INVENTORY_SDUK STRING ,
# MAGIC 	CLIENT_SDUK STRING ,
# MAGIC 	WAREHOUSE_SDUK STRING ,
# MAGIC 	ITEM_SDUK STRING ,
# MAGIC 	AVAILABLE_FLAG STRING ,
# MAGIC 	ETL_INSERT_DATE TIMESTAMP ,
# MAGIC 	ETL_UPDATE_DATE TIMESTAMP ,
# MAGIC 	ETL_BATCH_NUMBER DECIMAL(18, 0) ,
# MAGIC 	DESIGNATOR STRING ,
# MAGIC 	VENDOR_SERIAL_NUMBER STRING ,
# MAGIC 	VENDOR_LOT_NUMBER STRING ,
# MAGIC 	INV_REF_1 STRING ,
# MAGIC 	INV_REF_2 STRING ,
# MAGIC 	INV_REF_3 STRING ,
# MAGIC 	INV_REF_4 STRING ,
# MAGIC 	INV_REF_5 STRING ,
# MAGIC 	HOLD_CODE STRING ,
# MAGIC 	PRODUCT_STATUS STRING ,
# MAGIC 	BATCH_STATUS STRING ,
# MAGIC 	SHELF_LIFE INT ,
# MAGIC 	STORAGE_TYPE STRING ,
# MAGIC 	BATCH_HOLD_REASON STRING ,
# MAGIC 	HOLD_DESCRIPTION STRING ,
# MAGIC 	NET_AVAILABLE_UNITS INT,
# MAGIC     is_deleted int,
# MAGIC     dl_file_name string,
# MAGIC     dl_insert_pipeline_id string,
# MAGIC     dl_insert_timestamp string,
# MAGIC     dl_update_pipeline_id string
# MAGIC     )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/fact_inventory_snapshot'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC alter table silver.fact_inbound_line add columns
# MAGIC ( CASES  INT
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC alter table silver.FACT_ORDER_LINE_DETAILS add columns
# MAGIC ( RECEIPT_NUMBER  string
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC alter table silver.fact_milestone_activity add columns
# MAGIC ( PROOF_OF_DELIVERY_DATE_TIME  timestamp,
# MAGIC   LATITUDE string,
# MAGIC   LONGITUDE string,
# MAGIC   PROOF_OF_DELIVERY_LOCATION string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table silver.map_temperature_range_details(
# MAGIC CarrierCode string
# MAGIC ,LevelOfService string
# MAGIC ,TemperatureThreshold string
# MAGIC ,TemperatureRange_Min string
# MAGIC ,TemperatureRange_Max string
# MAGIC ,TemperatureRange_UOM string
# MAGIC ,ETL_INSERT_DATE timestamp
# MAGIC ,ETL_UPDATE_DATE timestamp
# MAGIC ,is_deleted int
# MAGIC ,dl_hash string
# MAGIC ,dl_file_name string
# MAGIC ,dl_insert_pipeline_id string
# MAGIC ,dl_insert_timestamp timestamp
# MAGIC ,dl_update_pipeline_id string
# MAGIC ,dl_update_timestamp timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sail/silver/gld360/inbound/map_temperature_range_details'