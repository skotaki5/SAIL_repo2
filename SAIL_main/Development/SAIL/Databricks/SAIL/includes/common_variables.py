# Databricks notebook source
# MAGIC %md
# MAGIC ##### python variables initialization for parameterised tables/schema names

# COMMAND ----------

from enum  import Enum
class FileType(Enum):
    DELTA =1
    PARQUET =2
    CSV =3
    

# COMMAND ----------

time_zone = 'UTC'
days_back = 90
days_back_Vacuum = 90

# COMMAND ----------

# DBTITLE 1,Secret scope name
scope = 'key-vault-secrets'

# COMMAND ----------

# DBTITLE 1,Sql Server connection details
jdbcHostname =  dbutils.secrets.get(scope,"SqlServerDomainName")
jdbcDatabase = dbutils.secrets.get(scope,"SqlDatabaseName")
jdbcPort = 1433
username = dbutils.secrets.get(scope,"SqlUserName")
password = dbutils.secrets.get(scope,"SqlAdminPassword")
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : username,
  "password" : password,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# DBTITLE 1,Databricks api connection details
token =  dbutils.secrets.get(scope,"AzureDatabricksBearerToken")
base_url= dbutils.secrets.get(scope,"AzureDatabricksUrl")

# COMMAND ----------

map_milestone_activity_et='map_milestone_activity'
map_temperature_range_details_et='map_temperature_range_details'
account_type_digital_et='account_type_digital'
local_courier_service_et='local_courier_service'
map_transactiontype_milestone_et='map_transactiontype_milestone'
map_ordersearchstatus_et='map_ordersearchstatus'
wh_wip_mapping_activity_et='wh_wip_mapping_activity'

dim_item_et='dim_item'
dim_source_system_et='dim_source_system'
dim_warehouse_et='dim_warehouse'
dim_customer_et='dim_customer'
dim_carrier_los_et='dim_carrier_los'
dim_service_et='dim_service'
dim_geo_location_et='dim_geo_location'

fact_transport_details_et='fact_transport_details'
fact_transportation_exception_et='fact_transportation_exception'
fact_transportation_rates_charges_et='fact_transportation_rates_charges'
fact_transportation_references_et='fact_transportation_references'
fact_transportation_callcheck_et='fact_transportation_callcheck'
fact_order_line_details_et='fact_order_line_details'
fact_order_line_et='fact_order_line'
fact_order_summary_et='fact_order_summary'
fact_transportation_et='fact_transportation'
fact_shipment_et='fact_shipment'
fact_order_reference_et='fact_order_reference'
fact_order_et='fact_order'
fact_milestone_activity_et='fact_milestone_activity'
fact_milestone_et='fact_milestone'
fact_inbound_line_et='fact_inbound_line'
fact_inventory_snapshot_et='fact_inventory_snapshot'

digital_summary_orders_et = 'digital_summary_orders'
digital_summary_order_lines_et = 'digital_summary_order_lines'
digital_summary_order_lines_details_et = 'digital_summary_order_lines_details'
digital_summary_order_tracking_et = 'digital_summary_order_tracking'
digital_summary_exceptions_et = 'digital_summary_exceptions'

digital_summary_milestone_et = 'digital_summary_milestone'
digital_summary_milestone_activity_et = 'digital_summary_milestone_activity'

digital_summary_transportation_et = 'digital_summary_transportation'
digital_summary_transport_details_et = 'digital_summary_transport_details'
digital_summary_transportation_callcheck_et = 'digital_summary_transportation_callcheck'
digital_summary_transportation_rates_charges_et = 'digital_summary_transportation_rates_charges'
digital_summary_transportation_references_et = 'digital_summary_transportation_references'

digital_summary_inbound_line_et = 'digital_summary_inbound_line'
digital_summary_inventory_et = 'digital_summary_inventory'
digital_summary_load_metadata_et='digital_summary_load_metadata'

#delta_control_et = 'delta_control'
digital_summary_onboarded_systems_et='digital_summary_onboarded_systems'
fact_order_dim_inc_et='fact_order_dim_inc'


# COMMAND ----------


####################################### paths for source/silver layer tables ############################

map_milestone_activity_path='/mnt/sail/silver/gld360/inbound/map_milestone_activity'
account_type_digital_path='/mnt/sail/silver/gld360/inbound/account_type_digital'
local_courier_service_path='/mnt/sail/silver/gld360/inbound/local_courier_service'
map_transactiontype_milestone_path='/mnt/sail/silver/gld360/inbound/map_transactiontype_milestone'
map_ordersearchstatus_path='/mnt/sail/silver/gld360/inbound/map_ordersearchstatus'
map_temperature_range_details_path='/mnt/sail/silver/gld360/inbound/map_temperature_range_details'
wh_wip_mapping_activity_path = '/mnt/sail/silver/gld360/inbound/wh_wip_mapping_activity'


dim_item_path='/mnt/sail/silver/gld360/inbound/dim_item'
dim_source_system_path='/mnt/sail/silver/gld360/inbound/dim_source_system'
dim_warehouse_path='/mnt/sail/silver/gld360/inbound/dim_warehouse'
dim_customer_path='/mnt/sail/silver/gld360/inbound/dim_customer'
dim_carrier_los_path='/mnt/sail/silver/gld360/inbound/dim_carrier_los'
dim_service_path='/mnt/sail/silver/gld360/inbound/dim_service'
dim_geo_location_path='/mnt/sail/silver/gld360/inbound/dim_geo_location'


fact_order_line_path='/mnt/sail/silver/gld360/inbound/fact_order_line'
fact_order_line_details_path='/mnt/sail/silver/gld360/inbound/fact_order_line_details'
fact_transportation_exception_path='/mnt/sail/silver/gld360/inbound/fact_transportation_exception'
fact_transportation_path='/mnt/sail/silver/gld360/inbound/fact_transportation'
fact_transport_details_path='/mnt/sail/silver/gld360/inbound/fact_transport_details'
fact_transportation_references_path='/mnt/sail/silver/gld360/inbound/fact_transportation_references'
fact_transportation_rates_charges_path='/mnt/sail/silver/gld360/inbound/fact_transportation_rates_charges'
fact_transportation_callcheck_path='/mnt/sail/silver/gld360/inbound/fact_transportation_callcheck'
fact_shipment_path='/mnt/sail/silver/gld360/inbound/fact_shipment'
fact_order_reference_path='/mnt/sail/silver/gld360/inbound/fact_order_reference'
fact_order_path='/mnt/sail/silver/gld360/inbound/fact_order'
fact_milestone_activity_path='/mnt/sail/silver/gld360/inbound/fact_milestone_activity'
fact_milestone_path='/mnt/sail/silver/gld360/inbound/fact_milestone'
fact_inbound_line_path='/mnt/sail/silver/gld360/inbound/fact_inbound_line'
fact_inventory_snapshot_path='/mnt/sail/silver/gld360/inbound/fact_inventory_snapshot'



# COMMAND ----------

# DBTITLE 1,gold layer paths
################################ gold table paths ##########################################
##################################################################################################
fact_order_dim_inc_path                           = '/mnt/sail/gold/summary/fact_order_dim_inc'
digital_summary_orders_path                       ='/mnt/sail/gold/summary/digital_summary_orders' 
digital_summary_order_lines_path                  ='/mnt/sail/gold/summary/digital_summary_order_lines'
digital_summary_inbound_line_path                 ='/mnt/sail/gold/summary/digital_summary_inbound_line'
digital_summary_order_lines_details_path          ='/mnt/sail/gold/summary/digital_summary_order_lines_details'
digital_summary_order_tracking_path               ='/mnt/sail/gold/summary/digital_summary_order_tracking'
digital_summary_exceptions_path                   ='/mnt/sail/gold/summary/digital_summary_exceptions'
digital_summary_transportation_path               = '/mnt/sail/gold/summary/digital_summary_transportation'
digital_summary_transport_details_path            ='/mnt/sail/gold/summary/digital_summary_transport_details'
digital_summary_transportation_references_path    ='/mnt/sail/gold/summary/digital_summary_transportation_references'
digital_summary_transportation_rates_charges_path ='/mnt/sail/gold/summary/digital_summary_transportation_rates_charges'
digital_summary_transportation_callcheck_path     ='/mnt/sail/gold/summary/digital_summary_transportation_callcheck'
digital_summary_milestone_path                    ='/mnt/sail/gold/summary/digital_summary_milestone'
digital_summary_milestone_activity_path           ='/mnt/sail/gold/summary/digital_summary_milestone_activity'
digital_summary_inventory_path                    ='/mnt/sail/gold/summary/digital_summary_inventory'
digital_summary_onboarded_systems_path            ="/mnt/sail/logs/controller.db/digital_summary_onboarded_systems"
#delta_control_path                                = '/mnt/sail/logs/controller.db/delta_control'
delta_stats_path                                  = '/mnt/sail/logs/controller.db/delta_stats'

# COMMAND ----------

audit_result ={
    "process_id": None
    ,"process_name": None
    ,"process_type": None
    ,"layer": None
    ,"table_name": None
    ,"status": None
    ,"process_date": None
    ,"start_time": None
    ,"end_time": None
    ,"numSourceRows": None
    ,"numTargetRowsInserted": None
    ,"numTargetRowsUpdated": None
    ,"numTargetRowsDeleted": None
    ,"numTargetRowsCopied": None
    ,"numTargetFilesRemoved": None
    ,"numTargetFilesAdded": None
    ,"executionTimeMs": None
    ,"scanTimeMs": None
    ,"rewriteTimeMs": None
    ,"dataRead_byte": None
    ,"dataWritten_byte": None
    ,"throughput": None
    ,"ERROR_MESSAGE": None
    ,"userName": None
    ,"operationParameters": None
    ,"operation": None
}

# COMMAND ----------

source_tables ={
        map_milestone_activity_et : 'silver.map_milestone_activity'
    ,account_type_digital_et : 'silver.account_type_digital'
    ,local_courier_service_et : 'silver.local_courier_service'
    ,map_transactiontype_milestone_et : 'silver.map_transactiontype_milestone'
    ,map_temperature_range_details_et : 'silver.map_temperature_range_details'
    ,map_ordersearchstatus_et : 'silver.map_ordersearchstatus'
    ,wh_wip_mapping_activity_et : 'silver.wh_wip_mapping_activity'
    ,dim_item_et : 'silver.dim_item'
    ,dim_source_system_et : 'silver.dim_source_system'
    ,dim_warehouse_et : 'silver.dim_warehouse'
    ,dim_customer_et : 'silver.dim_customer'
    ,dim_carrier_los_et : 'silver.dim_carrier_los'
    ,dim_service_et : 'silver.dim_service'
    ,dim_geo_location_et : 'silver.dim_geo_location'
    ,fact_transport_details_et : 'silver.fact_transport_details'
    ,fact_transportation_exception_et : 'silver.fact_transportation_exception'
    ,fact_transportation_rates_charges_et : 'silver.fact_transportation_rates_charges'
    ,fact_transportation_references_et : 'silver.fact_transportation_references'
    ,fact_transportation_callcheck_et : 'silver.fact_transportation_callcheck'
    ,fact_order_line_details_et : 'silver.fact_order_line_details'
    ,fact_order_line_et : 'silver.fact_order_line'
    ,fact_transportation_et : 'silver.fact_transportation'
    ,fact_shipment_et : 'silver.fact_shipment'
    ,fact_order_reference_et : 'silver.fact_order_reference'
    ,fact_order_et : 'silver.fact_order'
    ,fact_milestone_activity_et : 'silver.fact_milestone_activity'
    ,fact_inbound_line_et : 'silver.fact_inbound_line'
    ,fact_inventory_snapshot_et : 'silver.fact_inventory_snapshot'
    ,digital_summary_orders_et  :  'gold.digital_summary_orders'
    ,digital_summary_order_lines_et  :  'gold.digital_summary_order_lines'
    ,digital_summary_order_lines_details_et  :  'gold.digital_summary_order_lines_details'
    ,digital_summary_order_tracking_et  :  'gold.digital_summary_order_tracking'
    ,digital_summary_exceptions_et  :  'gold.digital_summary_exceptions'
    ,digital_summary_milestone_et  :  'gold.digital_summary_milestone'
    ,digital_summary_milestone_activity_et  :  'gold.digital_summary_milestone_activity'
    ,digital_summary_transportation_et  :  'gold.digital_summary_transportation'
    ,digital_summary_transport_details_et  :  'gold.digital_summary_transport_details'
    ,digital_summary_transportation_callcheck_et  :  'gold.digital_summary_transportation_callcheck'
    ,digital_summary_transportation_rates_charges_et  :  'gold.digital_summary_transportation_rates_charges'
    ,digital_summary_transportation_references_et  :  'gold.digital_summary_transportation_references'
    ,digital_summary_inbound_line_et  :  'gold.digital_summary_inbound_line'
    ,digital_summary_inventory_et  :  'gold.digital_summary_inventory'
    ,digital_summary_onboarded_systems_et : 'gold.digital_summary_onboarded_systems'
    ,fact_order_dim_inc_et : 'gold.fact_order_dim_inc'
    }


# COMMAND ----------

source_optimize = {
'fact_milestone_activity': {'table_name' : 'silver.fact_milestone_activity' ,
'optimize_column' :"SOURCE_SYSTEM_KEY,LOAD_TRACK_SDUK",
'analyze_column' :"",
'hwm_column' :"ETL_UPDATE_DATE",
'file_size':33554432
},
'fact_order': {'table_name' : 'silver.fact_order' ,
'optimize_column' :"SOURCE_SYSTEM_KEY,order_sduk",
'analyze_column' :"",
'hwm_column' :"ETL_UPDATE_DATE",
'file_size':33554432
},
'fact_shipment': {'table_name' : 'silver.fact_shipment' ,
'optimize_column' :"SOURCE_SYSTEM_KEY,SHIPMENT_SDUK",
'analyze_column' :"",
'hwm_column' :"ETL_UPDATE_DATE",
'file_size':33554432
},
'fact_order_line_details': {'table_name' : 'silver.fact_order_line_details' ,
'optimize_column' :"SOURCE_SYSTEM_KEY, ORDER_LINE_DETAILS_SDUK",
'analyze_column' :"",
'hwm_column' :"ETL_UPDATE_DATE",
'file_size':33554432
},
'fact_transportation': {'table_name' : 'silver.fact_transportation' ,
'optimize_column' :"SOURCE_SYSTEM_KEY,TRANSPORTATION_SDUK",
'analyze_column' :"",
'hwm_column' :"ETL_UPDATE_DATE",
'file_size':33554432
},
'fact_transportation_exception': {'table_name' : 'silver.fact_transportation_exception' ,
'optimize_column' :"SOURCE_SYSTEM_KEY,TRANSPORTATION_EXCEPTION_SDUK",
'analyze_column' :"",
'hwm_column' :"ETL_UPDATE_DATE",
'file_size':33554432
},  
'fact_order_reference': {'table_name' : 'silver.fact_order_reference' ,
'optimize_column' :"SOURCE_SYSTEM_KEY,ORDER_REFERENCE_SDUK,QUERY_SEQUENCE",
'analyze_column' :"",
'hwm_column' :"ETL_UPDATE_DATE",
'file_size':33554432
},
'fact_order_line': {'table_name' : 'silver.fact_order_line' ,
'optimize_column' :"SOURCE_SYSTEM_KEY,ORDER_LINE_SDUK",
'analyze_column' :"",
'hwm_column' :"ETL_UPDATE_DATE",
'file_size':33554432
},
'fact_inbound_line': {'table_name' : 'silver.fact_inbound_line' ,
'optimize_column' :"SOURCE_SYSTEM_KEY,INBOUND_LINE_SDUK",
'analyze_column' :"",
'hwm_column' :"ETL_UPDATE_DATE",
'file_size':33554432
},
'fact_transportation_callcheck': {'table_name' : 'silver.fact_transportation_callcheck' ,
'optimize_column' :"SOURCE_SYSTEM_KEY,callcheck_sduk",
'analyze_column' :"",
'hwm_column' :"ETL_UPDATE_DATE",
'file_size':33554432
},
'fact_transportation_rates_charges': {'table_name' : 'silver.fact_transportation_rates_charges' ,
'optimize_column' :"SOURCE_SYSTEM_KEY,CHARGE_SDUK",
'analyze_column' :"",
'hwm_column' :"ETL_UPDATE_DATE",
'file_size':33554432
},
'fact_transportation_references': {'table_name' : 'silver.fact_transportation_references' ,
'optimize_column' :"SOURCE_SYSTEM_KEY, REFERENCE_SDUK",
'analyze_column' :"",
'hwm_column' :"ETL_UPDATE_DATE",
'file_size':33554432
},
'fact_transport_details': {'table_name' : 'silver.fact_transport_details' ,
'optimize_column' :"SOURCE_SYSTEM_KEY,SHIPMENT_SDUK",
'analyze_column' :"",
'hwm_column' :"ETL_UPDATE_DATE",
'file_size':33554432
},
'dim_customer': {'table_name' : 'silver.dim_customer' ,
'optimize_column' :"SOURCE_SYSTEM_KEY,CUSTOMERKEY",
'analyze_column' :"",
'hwm_column' :"ETL_UPDATE_DATE",
'file_size':33554432
 },
'dim_warehouse': {'table_name' : 'silver.dim_warehouse' ,
'optimize_column' :"SOURCE_SYSTEM_KEY,WAREHOUSE_KEY",
'analyze_column' :"",
'hwm_column' :"ETL_UPDATE_DATE",
'file_size':33554432},
'dim_item': {'table_name' : 'silver.dim_item' ,
'optimize_column' :"SOURCE_SYSTEM_KEY, ITEM_KEY",
'analyze_column' :"",
'hwm_column' :"ETL_UPDATE_DATE",
'file_size':33554432}
,
'dim_carrier_los': {'table_name' : 'silver.dim_carrier_los' ,
'optimize_column' :"SOURCE_SYSTEM_KEY, CARRIER_LOS_KEY",
'analyze_column' :"",
'hwm_column' :"ETL_UPDATE_DATE",
'file_size':33554432}
,
'dim_service': {'table_name' : 'silver.dim_service' ,
'optimize_column' :"SOURCE_SYSTEM_KEY, SERVICE_KEY",
'analyze_column' :"",
'hwm_column' :"ETL_UPDATE_DATE",
'file_size':33554432}
,
'dim_geo_location': {'table_name' : 'silver.dim_geo_location' ,
'optimize_column' :"SOURCE_SYSTEM_KEY, GEO_LOCATION_KEY",
'analyze_column' :"",
'hwm_column' :"ETL_UPDATE_DATE",
'file_size':33554432}
,
'dim_source_system': {'table_name' : 'silver.dim_source_system' ,
'optimize_column' :"",
'analyze_column' :"",
'hwm_column' :"",
'file_size':33554432}
,
'map_milestone_activity': {'table_name' : 'silver.map_milestone_activity' ,
'optimize_column' :"",
'analyze_column' :"",
'hwm_column' :"",
'file_size':33554432}
,
'account_type_digital': {'table_name' : 'silver.account_type_digital' ,
'optimize_column' :"",
'analyze_column' :"",
'hwm_column' :"",
'file_size':33554432}
,
'local_courier_service': {'table_name' : 'silver.local_courier_service' ,
'optimize_column' :"",
'analyze_column' :"",
'hwm_column' :"",
'file_size':33554432}
,
'map_orderSearchStatus': {'table_name' : 'silver.map_ordersearchstatus' ,
'optimize_column' :"",
'analyze_column' :"",
'hwm_column' :"",
'file_size':33554432}
,
'map_transactiontype_milestone': {'table_name' : 'silver.map_transactiontype_milestone' ,
'optimize_column' :"",
'analyze_column' :"",
'hwm_column' :"",
'file_size':33554432}
,
'map_temperature_range_details': {'table_name' : 'silver.map_temperature_range_details' ,
'optimize_column' :"",
'analyze_column' :"",
'hwm_column' :"",
'file_size':33554432}  
,
'wh_wip_mapping_activity': {'table_name' : 'silver.wh_wip_maping_activity' ,
'optimize_column' :"",
'analyze_column' :"",
'hwm_column' :"",
'file_size':33554432}
,
'fact_order_dim_inc': {'table_name' : 'gold.fact_order_dim_inc' ,
'optimize_column' :"SOURCE_SYSTEM_KEY,ups_order_number, order_sduk",
'analyze_column' :"",
'file_size':33554432}
,
'digital_summary_order_lines': {'table_name' : 'gold.digital_summary_order_lines' ,
'optimize_column' :"SourceSystemKey, UPSOrderNumber",
'analyze_column' :"",
'file_size':33554432}
,
'digital_summary_milestone': {'table_name' : 'gold.digital_summary_milestone' ,
'optimize_column' :"SourceSystemKey,order_sduk",
'analyze_column' :"",
'file_size':33554432}
,
'digital_summary_transport_details': {'table_name' : 'gold.digital_summary_transport_details' ,
'optimize_column' :"SOURCE_SYSTEM_KEY, hash_key",
'analyze_column' :"",
'file_size':33554432}
,
'digital_summary_milestone_activity': {'table_name' : 'gold.digital_summary_milestone_activity' ,
'optimize_column' :"SourceSystemKey,UPSOrderNumber",
'analyze_column' :"",
'file_size':33554432}
,
'digital_summary_transportation_callcheck': {'table_name' : 'gold.digital_summary_transportation_callcheck' ,
'optimize_column' :"SourceSystemKey,UPSORDERNUMBER",
'analyze_column' :"",
'file_size':33554432}
,
'digital_summary_transportation_references': {'table_name' : 'gold.digital_summary_transportation_references' ,
'optimize_column' :"SourceSystemKey,UPSORDERNUMBER",
'analyze_column' :"",
'file_size':33554432}
,
'digital_summary_transportation_rates_charges': {'table_name' : 'gold.digital_summary_transportation_rates_charges' ,
'optimize_column' :"SourceSystemKey,UpsOrderNumber",
'analyze_column' :"",
'file_size':33554432}
,
'digital_summary_order_tracking': {'table_name' : 'gold.digital_summary_order_tracking' ,
'optimize_column' :"SourceSystemKey,UPSOrderNumber",
'analyze_column' :"",
'file_size':33554432}
,
'digital_summary_orders': {'table_name' : 'gold.digital_summary_orders' ,
'optimize_column' :"SourceSystemKey,UPSOrderNumber,order_sduk",
'analyze_column' :"",
'file_size':33554432}
,
'digital_summary_transportation': {'table_name' : 'gold.digital_summary_transportation' ,
'optimize_column' :"SourceSystemKey,UPSOrderNumber",
'analyze_column' :"",
'file_size':33554432}
,
'digital_summary_order_lines_details': {'table_name' : 'gold.digital_summary_order_lines_details' ,
'optimize_column' :"SourceSystemKey,UPSOrderNumber",
'analyze_column' :"",
'file_size':33554432}
,
'digital_summary_exceptions': {'table_name' : 'gold.digital_summary_exceptions' ,
'optimize_column' :"SourceSystemKey,UPSOrderNumber,TRANSPORTATION_EXCEPTION_SDUK",
'analyze_column' :"",
'file_size':33554432},
'digital_summary_inbound_line': {'table_name' : 'gold.digital_summary_inbound_line' ,
'optimize_column' :"SourceSystemKey,hash_key",
'analyze_column' :"",
'file_size':33554432}
}

# COMMAND ----------

account_id= """ ('E648FA6F-6253-428E-8AC9-201E3EF83B91','3DAEB74F-FA36-47D7-AC7A-FDBDC4341357','DD650B97-9291-498B-BA98-D6680C73CA3C','B862E947-D271-46DE-BD95-049617B58A91','6F96FCED-32F9-456F-9DF4-1A97845E7675','1EEF1B1A-A415-43F3-88C5-2D5EBC503529','7EBA92DE-8358-46D0-B8BA-B4D9735396EA','C6BC6B0C-6B96-4466-8F8D-F9EB68B3A48D','0C061A26-767B-436C-B78E-A65DBE24E2B3','870561E1-A974-483B-AA0D-A724C5D402C9','E937415E-5CA0-4A4C-AD26-89DC4DF7FE69','9177518A-3C1F-4AD4-9AF2-13E69A3B0EC3','5CA0AAFF-1334-40CC-BEAC-BA663F32626A','A2B1487C-3878-4A06-898B-4EA06DF022BF','DD650B97-9291-498B-BA98-D6680C73CA3C') """