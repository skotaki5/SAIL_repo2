--rpt_Outbound_WIP_Shipment_Listing

-- Result Set 1

/*
Parameter Requirement info -
---->@DPProductKey required,
@DP_SERVICELINE_KEY optional,
@Date.shipmentCreationStartDateTime
AND @Date.shipmentCreationEndDateTime are optional 
AND @Date.expectedShipByStartDateTime AND @Date.expectedShipByEndDateTime are optional 
and @milestoneStatus and  @shipmentMode are optional ,
@warehouseCode and @warehouseCode are optional

Target Container - digital_summary_orders
*/

Target Container - digital_summary_orders
*/

-- If @WIPActivityName = '*' OR @WIPActivityName = 'SHIPPED'

select count(1) totalShipments from c WHERE
 c.AccountId = @DPProductKey
	AND c.DP_SERVICELINE_KEY In ( @DP_SERVICELINE_KEY)
	AND (c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDateTime and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1,@Date.shipmentCreationEndDateTime)))
    --AND (c.actualShipmentDateTime BETWEEN @Date.expectedShipByStartDateTime AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1,@Date.expectedShipByEndDateTime))) null for given account id
    AND (c.DateTimeShipped BETWEEN 
    '@Date.shippedStartDateTime' AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1,@Date.shippedEndDateTime)))
	AND  (c.milestoneStatus in (@milestoneStatus))
	AND c.IS_INBOUND= 0
	AND ((IS_NULL(c.ServiceMode)?'UNASSIGNED':c.ServiceMode) IN (@shipmentMode))
    AND (c.warehouseCode in (@warehouseCode))
	AND IS_NULL(c.DateTimeShipped) = false and c.is_deleted = 0

-- If @WIPActivityName != '*' and  @WIPActivityName != Shipped

select count(1) totalShipments from  c WHERE
 c.AccountId = @DPProductKey
	AND c.DP_SERVICELINE_KEY In ( @DP_SERVICELINE_KEY)
	AND (c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDateTime
    AND @Date.shipmentCreationEndDateTime)
    --AND (c.actualShipmentDateTime BETWEEN @Date.expectedShipByStartDateTime AND @Date.expectedShipByEndDateTime) null for given account id
    AND (c.DateTimeShipped BETWEEN 
    '@Date.shippedStartDateTime' AND '@Date.shippedEndDateTime')
	AND  (c.milestoneStatus in (@milestoneStatus))
	AND c.IS_INBOUND= 0
	AND ((IS_NULL(c.ServiceMode)?'UNASSIGNED':c.ServiceMode) IN (@shipmentMode))
    AND (c.warehouseCode in (@warehouseCode)) and c.wip_ActivityName in ( @wipActivityName) and c.is_deleted = 0

-- Result Set 2

/*
Parameter Requirement info -
---->@DPProductKey required,
@DP_SERVICELINE_KEY optional,
@Date.shipmentCreationStartDateTime
AND @Date.shipmentCreationEndDateTime are optional 
AND @Date.expectedShipByStartDateTime AND @Date.expectedShipByEndDateTime are optional 
and @milestoneStatus and  @shipmentMode are optional ,
@warehouseCode and @warehouseCode are optional

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
	c.ShipmentCreationDate,
	c.shippedDateTime
	FROM c  WHERE
 c.AccountId = @DPProductKey
	AND c.DP_SERVICELINE_KEY In ( @DP_SERVICELINE_KEY)
	AND (c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDateTime
    AND @Date.shipmentCreationEndDateTime)
    --AND (c.actualShipmentDateTime BETWEEN @Date.expectedShipByStartDateTime AND @Date.expectedShipByEndDateTime) null for given account id
    AND (c.DateTimeShipped BETWEEN 
    '@Date.shippedStartDateTime' AND '@Date.shippedEndDateTime')
	AND  (c.milestoneStatus in (@milestoneStatus))
	AND c.IS_INBOUND= 0
	AND ((IS_NULL(c.ServiceMode)?'UNASSIGNED':c.ServiceMode) IN (@shipmentMode))
    AND (c.warehouseCode in (@warehouseCode))
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
 c.AccountId = @DPProductKey
	AND c.DP_SERVICELINE_KEY In ( @DP_SERVICELINE_KEY)
	AND (c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDateTime
    AND @Date.shipmentCreationEndDateTime)
    --AND (c.actualShipmentDateTime BETWEEN @Date.expectedShipByStartDateTime AND @Date.expectedShipByEndDateTime) null for given account id
    AND (c.DateTimeShipped BETWEEN 
    '@Date.shippedStartDateTime' AND '@Date.shippedEndDateTime')
	AND  (c.milestoneStatus in (@milestoneStatus))
	AND c.IS_INBOUND= 0
	AND ((IS_NULL(c.ServiceMode)?'UNASSIGNED':c.ServiceMode) IN (@shipmentMode))
    AND (c.warehouseCode in (@warehouseCode)) and c.wip_ActivityName in ( @wipActivityName) and c.is_deleted = 0 order by c.DateTimeReceived desc