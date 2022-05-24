--rpt_Outbound_Shipment_WIP_Activities

-- Result Set 1

/*
Target Container - digital_summary_orders
*/

-- iF @type ='ORDER'

select t.type, t.ActivityName, t.ShipmentMode,COUNT(1) AS Count ,t.warehouseCode, t.Date  from (SELECT 
        'ORDER' AS type-- parameter @type
		,(c.SourceSystemKey + 07) as ActivityOrderId
        ,'Shipped' ActivityName
		,(IS_NULL(c.ServiceMode)?'UNASSIGNED':c.ServiceMode) as ShipmentMode 
        
		,c.warehouseCode
		,	'SHIPPED'='SHIPPED'?c.ShipmentShippedDate:('SHIPPED'='CREATE'?c.ShipmentCreatedDate:null) Date  
      FROM c 
      WHERE c.IS_ASN = null
   AND UPPER(c.AccountId)='1EEF1B1A-A415-43F3-88C5-2D5EBC503529' 
   AND c.IS_INBOUND = 0
  AND (c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
  AND (c.FacilityId IN ('A2B979C6-7FA9-40DE-A648-08D6175D2695'))
  AND (c.DateTimeReceived BETWEEN '2021-11-01 00:00:00.000'  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2021-11-11 00:00:00.000')))
  AND (c.DateTimeShipped BETWEEN '2021-11-01 00:00:00.000'  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2021-11-11 00:00:00.000')))
) t  GROUP BY t.type,t.ActivityOrderId,t.ActivityName,t.ShipmentMode,t.warehouseCode ,t.Date

-- iF @type ='LINES'

select t.type, t.ActivityName, t.ShipmentMode,COUNT(t.UPSOrderNumber) AS Count ,t.warehouseCode, t.Date  from (SELECT 
        'LINES' AS type
		,(c.SourceSystemKey + 07) as ActivityOrderId
        ,'Shipped' ActivityName
		,(IS_NULL(c.ServiceMode)?'UNASSIGNED':c.ServiceMode) as ShipmentMode 
        ,c.UPSOrderNumber
		,c.warehouseCode
			,	'SHIPPED'='SHIPPED'?c.ShipmentShippedDate:('SHIPPED'='CREATE'?c.ShipmentCreatedDate:null) Date  
      FROM c 
      WHERE UPPER(c.AccountId)='1EEF1B1A-A415-43F3-88C5-2D5EBC503529' 
   AND c.IS_INBOUND = 0
  AND (c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
  AND (c.FacilityId IN ('A2B979C6-7FA9-40DE-A648-08D6175D2695'))
  AND (c.DateTimeReceived BETWEEN '2021-11-01 00:00:00.000'  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2021-11-11 00:00:00.000')))
  AND (c.DateTimeShipped BETWEEN '2021-11-01 00:00:00.000'  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2021-11-11 00:00:00.000')))
) t  GROUP BY t.type,t.ActivityOrderId,t.ActivityName,t.ShipmentMode,t.warehouseCode, t.Date
--orderby cannot be applied on derived field

-- iF @type ='UNITS'

select t.type, t.ActivityName, t.ShipmentMode,Sum(t.SKUQuantity_sum) AS Count ,t.warehouseCode, t.Date  from (SELECT 
        'UNITS' AS type
		,(c.SourceSystemKey + 07) as ActivityOrderId
        ,'Shipped' ActivityName
		,(IS_NULL(c.ServiceMode)?'UNASSIGNED':c.ServiceMode) as ShipmentMode 
        ,c.SKUQuantity_sum
		,c.warehouseCode
			,	'SHIPPED'='SHIPPED'?c.ShipmentShippedDate:('SHIPPED'='CREATE'?c.ShipmentCreatedDate:null) Date
      FROM c 
      WHERE 
   UPPER(c.AccountId)='1EEF1B1A-A415-43F3-88C5-2D5EBC503529' 
   AND c.IS_INBOUND = 0
  AND (c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
  AND (c.FacilityId IN ('A2B979C6-7FA9-40DE-A648-08D6175D2695'))
  AND (c.DateTimeReceived BETWEEN '2021-11-01 00:00:00.000'  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2021-11-11 00:00:00.000')))
  AND (c.DateTimeShipped BETWEEN '2021-11-01 00:00:00.000'  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2021-11-11 00:00:00.000')))
) t  GROUP BY t.type,t.ActivityOrderId,t.ActivityName,t.ShipmentMode,t.warehouseCode, t.Date
--orderby cannot be applied on derived field

-- iF  @type =''*''


select t.type, t.ActivityName, t.ShipmentMode,COUNT(1) AS Count ,t.warehouseCode, t.Date  from (SELECT 
        'ORDER' AS type-- parameter @type
		,(c.SourceSystemKey + 07) as ActivityOrderId
        ,'Shipped' ActivityName
		,(IS_NULL(c.ServiceMode)?'UNASSIGNED':c.ServiceMode) as ShipmentMode 
        
		,c.warehouseCode
		,	'SHIPPED'='SHIPPED'?c.ShipmentShippedDate:('SHIPPED'='CREATE'?c.ShipmentCreatedDate:null) Date  
      FROM c 
      WHERE c.IS_ASN = null
   AND UPPER(c.AccountId)='1EEF1B1A-A415-43F3-88C5-2D5EBC503529' 
   AND c.IS_INBOUND = 0
  AND (c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
  AND (c.FacilityId IN ('A2B979C6-7FA9-40DE-A648-08D6175D2695'))
  AND (c.DateTimeReceived BETWEEN '2021-11-01 00:00:00.000'  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2021-11-11 00:00:00.000')))
  AND (c.DateTimeShipped BETWEEN '2021-11-01 00:00:00.000'  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2021-11-11 00:00:00.000')))
) t  GROUP BY t.type,t.ActivityOrderId,t.ActivityName,t.ShipmentMode,t.warehouseCode ,t.Date


select t.type, t.ActivityName, t.ShipmentMode,COUNT(t.UPSOrderNumber) AS Count ,t.warehouseCode, t.Date  from (SELECT 
        'LINES' AS type
		,(c.SourceSystemKey + 07) as ActivityOrderId
        ,'Shipped' ActivityName
		,(IS_NULL(c.ServiceMode)?'UNASSIGNED':c.ServiceMode) as ShipmentMode 
        ,c.UPSOrderNumber
		,c.warehouseCode
			,	'SHIPPED'='SHIPPED'?c.ShipmentShippedDate:('SHIPPED'='CREATE'?c.ShipmentCreatedDate:null) Date  
      FROM c 
      WHERE UPPER(c.AccountId)='1EEF1B1A-A415-43F3-88C5-2D5EBC503529' 
   AND c.IS_INBOUND = 0
  AND (c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
  AND (c.FacilityId IN ('A2B979C6-7FA9-40DE-A648-08D6175D2695'))
  AND (c.DateTimeReceived BETWEEN '2021-11-01 00:00:00.000'  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2021-11-11 00:00:00.000')))
  AND (c.DateTimeShipped BETWEEN '2021-11-01 00:00:00.000'  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2021-11-11 00:00:00.000')))
) t  GROUP BY t.type,t.ActivityOrderId,t.ActivityName,t.ShipmentMode,t.warehouseCode, t.Date
--orderby cannot be applied on derived field

select t.type, t.ActivityName, t.ShipmentMode,Sum(t.SKUQuantity_sum) AS Count ,t.warehouseCode, t.Date  from (SELECT 
        'UNITS' AS type
		,(c.SourceSystemKey + 07) as ActivityOrderId
        ,'Shipped' ActivityName
		,(IS_NULL(c.ServiceMode)?'UNASSIGNED':c.ServiceMode) as ShipmentMode 
        ,c.SKUQuantity_sum
		,c.warehouseCode
			,	'SHIPPED'='SHIPPED'?c.ShipmentShippedDate:('SHIPPED'='CREATE'?c.ShipmentCreatedDate:null) Date
      FROM c 
      WHERE 
   UPPER(c.AccountId)='1EEF1B1A-A415-43F3-88C5-2D5EBC503529' 
   AND c.IS_INBOUND = 0
  AND (c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
  AND (c.FacilityId IN ('A2B979C6-7FA9-40DE-A648-08D6175D2695'))
  AND (c.DateTimeReceived BETWEEN '2021-11-01 00:00:00.000'  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2021-11-11 00:00:00.000')))
  AND (c.DateTimeShipped BETWEEN '2021-11-01 00:00:00.000'  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2021-11-11 00:00:00.000')))
) t  GROUP BY t.type,t.ActivityOrderId,t.ActivityName,t.ShipmentMode,t.warehouseCode, t.Date
--orderby cannot be applied on derived field