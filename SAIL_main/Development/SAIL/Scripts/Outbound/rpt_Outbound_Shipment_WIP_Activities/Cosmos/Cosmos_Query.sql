--rpt_Outbound_Shipment_WIP_Activities
/*
NOTE:
there are dynamic parameters for this stored proc, based on which query changes/ results are changed.
there are two mandatory parameters , which can take values from below mentioned lists:
1. @dateType = {'SHIPPED','CREATE','*'}
2.@type= {'ORDER','LINES','UNITS','*'}
*/

-- Result Set 1

/*
Parameter Requirement info -
->@DPProductLineKey  required 
->@DPServiceLineKey  optional
->@warehouseId       optional
->@startDate         optional 
->@endDate  
->@dateType required
->@type  optional

Target Container - digital_summary_orders
*/

-- iF @type ='ORDER'

select t.type, t.ActivityOrderId, t.ActivityName, t.ShipmentMode,COUNT(1) AS Count ,t.warehouseCode, t.Date  from (SELECT 
        'ORDER' AS type
		,(c.SourceSystemKey + 07) as ActivityOrderId
        ,'Shipped' ActivityName
		,(IS_NULL(c.ServiceMode)?'UNASSIGNED':c.ServiceMode) as ShipmentMode 
        
		,c.warehouseCode
		,@dateType='SHIPPED'?c.ShipmentShippedDate:(@dateType='CREATE'?c.ShipmentCreatedDate:null) Date  
      FROM c 
      WHERE c.IS_ASN = null
   AND UPPER(c.AccountId)=@VarAccountID 
   AND c.IS_INBOUND = 0
  AND (c.DP_SERVICELINE_KEY = @VarDPServiceLineKey)
  AND (c.FacilityId IN (@warehouseId))
  AND (c.DateTimeReceived BETWEEN @startDate  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, @endDate)))
  AND (c.DateTimeShipped BETWEEN @startDate  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, @endDate)))
) t  GROUP BY t.type,t.ActivityOrderId,t.ActivityName,t.ShipmentMode,t.warehouseCode ,t.Date
--orderby cannot be applied on derived field


-- iF @type ='LINES'

select t.type, t.ActivityOrderId, t.ActivityName, t.ShipmentMode,COUNT(1) AS Count ,t.warehouseCode, t.Date  from (SELECT 
        'LINES' AS type
		,(c.SourceSystemKey + 07) as ActivityOrderId
        ,'Shipped' ActivityName
		,(IS_NULL(c.ServiceMode)?'UNASSIGNED':c.ServiceMode) as ShipmentMode 
        
		,c.warehouseCode
		,@dateType='SHIPPED'?c.ShipmentShippedDate:(@dateType='CREATE'?c.ShipmentCreatedDate:null) Date  
      FROM c 
      WHERE  UPPER(c.AccountId)=@VarAccountID 
   AND c.IS_INBOUND = 0
  AND (c.DP_SERVICELINE_KEY = @VarDPServiceLineKey)
  AND (c.FacilityId IN (@warehouseId))
  AND (c.DateTimeReceived BETWEEN @startDate  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, @endDate)))
  AND (c.DateTimeShipped BETWEEN @startDate  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, @endDate)))
) t  GROUP BY t.type,t.ActivityOrderId,t.ActivityName,t.ShipmentMode,t.warehouseCode ,t.Date

-- iF @type ='UNITS'

select t.type, t.ActivityOrderId, t.ActivityName, t.ShipmentMode,Sum(t.SKUQuantity_sum) AS Count ,t.warehouseCode, t.Date  from (SELECT 
        'UNITS' AS type
		,(c.SourceSystemKey + 07) as ActivityOrderId
        ,'Shipped' ActivityName
		,(IS_NULL(c.ServiceMode)?'UNASSIGNED':c.ServiceMode) as ShipmentMode 
        ,c.SKUQuantity_sum
		,c.warehouseCode
			,	@dateType='SHIPPED'?c.ShipmentShippedDate:(@dateType='CREATE'?c.ShipmentCreatedDate:null) Date
      FROM c 
      WHERE 
   UPPER(c.AccountId)=@VarAccountID 
   AND c.IS_INBOUND = 0
  AND (c.DP_SERVICELINE_KEY = @VarDPServiceLineKey)
  AND (c.FacilityId IN (@warehouseId))
  AND (c.DateTimeReceived BETWEEN @startDate  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, @endDate)))
  AND (c.DateTimeShipped BETWEEN @startDate  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, @endDate)))
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
   AND UPPER(c.AccountId)=@VarAccountID 
   AND c.IS_INBOUND = 0
  AND (c.DP_SERVICELINE_KEY = @VarDPServiceLineKey)
  AND (c.FacilityId IN (@warehouseId))
  AND (c.DateTimeReceived BETWEEN @startDate  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, @endDate)))
  AND (c.DateTimeShipped BETWEEN @startDate  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, @endDate)))
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
      WHERE UPPER(c.AccountId)=@VarAccountID 
   AND c.IS_INBOUND = 0
  AND (c.DP_SERVICELINE_KEY = @VarDPServiceLineKey)
  AND (c.FacilityId IN (@warehouseId))
  AND (c.DateTimeReceived BETWEEN @startDate  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, @endDate)))
  AND (c.DateTimeShipped BETWEEN @startDate  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, @endDate)))
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
   UPPER(c.AccountId)=@VarAccountID 
   AND c.IS_INBOUND = 0
  AND (c.DP_SERVICELINE_KEY = @VarDPServiceLineKey)
  AND (c.FacilityId IN (@warehouseId))
  AND (c.DateTimeReceived BETWEEN @startDate  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, @endDate)))
  AND (c.DateTimeShipped BETWEEN @startDate  AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, @endDate)))
) t  GROUP BY t.type,t.ActivityOrderId,t.ActivityName,t.ShipmentMode,t.warehouseCode, t.Date
--orderby cannot be applied on derived field