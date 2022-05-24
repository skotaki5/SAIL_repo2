--rpt_Outbound_WIP_Shipment_Listing

-- Result Set 1

/*
Target Container - digital_summary_orders
*/

-- If @WIPActivityName = '*' OR @WIPActivityName = 'SHIPPED'

select count(1) totalShipments from c WHERE
 c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
	AND (c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
	AND (c.DateTimeReceived BETWEEN '2021-11-01 00:00:00.000' 
    AND  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, "2021-11-30 00:00:00.000")))
and (is_null(c.actualShipmentDateTime)?true:c.actualShipmentDateTime BETWEEN '2021-11-01 00:00:00.000' 
AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, "2021-11-30 00:00:00.000"))) 
    AND (c.DateTimeShipped BETWEEN 
    '2021-11-01 00:00:00.000' AND  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, "2021-11-30 00:00:00.000")))
	AND  (c.milestoneStatus in ('DELIVERED'))
	AND c.IS_INBOUND= 0
	AND ((IS_NULL(c.ServiceMode)?'UNASSIGNED':c.ServiceMode) IN ('Drive - Critical'))
    AND (c.warehouseCode in ('WCMH1'))
	AND IS_NULL(c.DateTimeShipped) = false
    and c.is_deleted = 0

-- If @WIPActivityName != '*' and  @WIPActivityName != Shipped

select count(1) totalShipments from c WHERE
 c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
	AND (c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
	AND (c.DateTimeReceived BETWEEN '2021-11-01 00:00:00.000' 
    AND '2021-11-31 00:00:00.000')
    --AND (c.actualShipmentDateTime BETWEEN '2021-11-01 00:00:00.000' AND '2021-11-31 00:00:00.000')
    AND (c.DateTimeShipped BETWEEN 
    '2021-11-01 00:00:00.000' AND '2021-11-11 00:00:00.000')
	AND  (c.milestoneStatus in ('DELIVERED'))
	AND c.IS_INBOUND= 0
	AND ((IS_NULL(c.ServiceMode)?'UNASSIGNED':c.ServiceMode) IN ('Drive - Critical'))
    AND (c.warehouseCode in ('WCMH1'))
    --AND c.Type = 'OUT'  null for all records
    --AND c.WIP_ActivityName IN (select (TRIM(value)) from string_split(@VarWIPActivityName,',')))  null for all records and is_deleted = 0

-- Result Set 2

/*
Target Container - digital_summary_orders
*/

-- If @WIPActivityName = '*' OR @WIPActivityName = 'SHIPPED'

SELECT    
    c.upsShipmentNumber,
	c.referenceNumber,
	'Shipped' wipActivityName,     c.OrderLineCount linesCount,
    c.SKUQuantity_sum unitsCount,
	c.warehouseId,
	c.warehouseCode,
	c.milestoneStatus,
	c.ShipmentMode,
	c.expectedShipByDateTime,
	c.shipmentOrigin_addressLine1,
	c.shipmentOrigin_addressLine2,
	c.shipmentOrigin_city,
	c.shipmentOrigin_stateProvince,
	c.shipmentOrigin_postalCode,
	c.shipmentOrigin_country,
	c.shipmentDestination_addressLine1,
	c.shipmentDestination_addressLine2,
	c.shipmentDestination_city,
	c.shipmentDestination_stateProvince,
	c.shipmentDestination_postalCode,
	c.shipmentDestination_country,
	c.DateTimeReceived ShipmentCreationDate,
	c.shippedDateTime,
    c.ServiceMode AS shipmentService,                
    c.ServiceLevel AS shipmentServiceLevel,                
    c.shipmentServiceLevelCode
	FROM c WHERE
 c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
	AND (c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
	AND (c.DateTimeReceived BETWEEN '2021-11-01 00:00:00.000' 
    AND '2021-11-30 00:00:00.000')
    AND (is_null(c.actualShipmentDateTime)?true:c.actualShipmentDateTime BETWEEN '2021-11-01 00:00:00.000' AND '2021-11-30 00:00:00.000') 
    AND (c.DateTimeShipped BETWEEN 
    '2021-11-01 00:00:00.000' AND '2021-11-30 00:00:00.000')
	AND  (c.milestoneStatus in ('DELIVERED'))
	AND c.IS_INBOUND= 0
	AND ((IS_NULL(c.ServiceMode)?'UNASSIGNED':c.ServiceMode) IN ('Drive - Critical'))
    AND (c.warehouseCode in ('WCMH1'))
	AND IS_NULL(c.DateTimeShipped) = false and c.is_deleted = 0 order by c.DateTimeReceived desc


-- If @WIPActivityName != '*' and  @WIPActivityName != Shipped

SELECT    
    c.upsShipmentNumber,
	c.referenceNumber,
	c.wipActivityName,     c.OrderLineCount linesCount,
    c.SKUQuantity_sum unitsCount,
	c.warehouseId,
	c.warehouseCode,
	c.milestoneStatus,
	c.ShipmentMode,
	c.expectedShipByDateTime,
	c.shipmentOrigin_addressLine1,
	c.shipmentOrigin_addressLine2,
	c.shipmentOrigin_city,
	c.shipmentOrigin_stateProvince,
	c.shipmentOrigin_postalCode,
	c.shipmentOrigin_country,
	c.shipmentDestination_addressLine1,
	c.shipmentDestination_addressLine2,
	c.shipmentDestination_city,
	c.shipmentDestination_stateProvince,
	c.shipmentDestination_postalCode,
	c.shipmentDestination_country,
	c.ShipmentCreationDate,
	c.shippedDateTime
	FROM c  WHERE
 c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
	AND (c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
	AND (c.DateTimeReceived BETWEEN '2021-11-01 00:00:00.000' 
    AND '2021-11-31 00:00:00.000')
    --AND (c.actualShipmentDateTime BETWEEN '2021-11-01 00:00:00.000' AND '2021-11-31 00:00:00.000')
    AND (c.DateTimeShipped BETWEEN 
    '2021-11-01 00:00:00.000' AND '2021-11-11 00:00:00.000')
	AND  (c.milestoneStatus in ('DELIVERED'))
	AND c.IS_INBOUND= 0
	AND ((IS_NULL(c.ServiceMode)?'UNASSIGNED':c.ServiceMode) IN ('Drive - Critical'))
    AND (c.warehouseCode in ('WCMH1'))
    --AND c.Type = 'OUT'  null for all records
    --AND c.WIP_ActivityName IN (select (TRIM(value)) from string_split(@VarWIPActivityName,',')))  null for all records and c.is_deleted = 0 order by c.DateTimeReceived desc
