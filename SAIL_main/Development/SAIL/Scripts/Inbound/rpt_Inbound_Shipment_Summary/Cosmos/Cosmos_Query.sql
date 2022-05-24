--AUTHOR : 		VISHAL SHARMA
--DESCRIPTION:	rpt_Inbound_Shipment_Summary
--DATE : 		02-03-2022

/*
Note : 
preprocess some variable based on the storeproc

IF @DateType is null
begin
if @NULLCreatedDate = '*'
begin
set  @varDateType = 'SHIPMENTDELIVERYDATE'
end
if @NULLActualDeliveryDate = '*'
begin
set @varDateType = 'SHIPMENTCREATIONDATE'  
end
end
IF @Date is null and UPPER(@DateType)='SHIPMENTCREATIONDATE'
begin
set @shipmentCreationStartDateTime=@StartDate
set @shipmentCreationEndDateTime=DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @endDate)))
set @NULLCreatedDate = ''
end
else IF @Date is null and UPPER(@DateType)='SHIPMENTDELIVERYDATE'
begin
set @actualDeliveryStartDateTime=@StartDate
set @actualDeliveryEndDateTime=DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @endDate)))
set @NULLActualDeliveryDate = ''
end
*/


--Result Set 1

--* Parameter requirement info.
-- @DPProductLineKey required,
-- @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate   AND  @Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate AND @Date.shipmentCreationEndDate are optional,
-- @Date.scheduledDeliveryStartDate   AND @Date.scheduledDeliveryEndDate are optional
-- @isASN  optional
-- @Date.actualDeliveryStartDate	AND @Date.actualDeliveryEndDate are optional

--* Target Container-digital_summary_orders

--CASE 1 IF @DateType = 'SHIPMENTCREATIONDATE'  and  @nullActualDeliveryDate = '*'

SELECT COUNT(1) Total
FROM c
WHERE c.AccountId = @DPProductLineKey
    AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
    AND c.FacilityId IN ('@warehouseId')
    AND (
        c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
        )
    AND (
        c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
        )
    AND (
        c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
        )
         
    AND c.IS_INBOUND = 1 and c.is_deleted=0
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
    AND (
        c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        )
    AND c.milestoneStatus != null

-- CASE 2 IF @DateType = 'SHIPMENTDELIVERYDATE'  AND @nullCreatedDate = '*'

SELECT COUNT(1) Total
FROM c
WHERE c.AccountId = @DPProductLineKey
    AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
    AND c.FacilityId IN ('@warehouseId')
    AND (
        c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
        )
    AND (
        c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
        )
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
    AND c.activity_Movement_flag=1
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
    AND (
        c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        )
    AND c.milestoneStatus != null

-- Result Set 2 

--* Parameter requirement info.
-- @DPProductLineKey required,
-- @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate  AND  @Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate AND @Date.shipmentCreationEndDate are optional,
-- @Date.scheduledDeliveryStartDate   AND @Date.scheduledDeliveryEndDate are optional
-- isASN  optional
-- @Date.actualDeliveryStartDate	AND @Date.actualDeliveryEndDate are optional

--* Target Container-digital_summary_orders

-- CASE 1 IF @DateType = 'SHIPMENTCREATIONDATE'  and  @nullActualDeliveryDate = '*'

SELECT c.milestoneStatus
	,COUNT(c.milestoneStatus) milestoneStatusCount
FROM c
WHERE c.AccountId = @DPProductLineKey
	AND c.DP_SERVICELINE_KEY = @DPServiceLineKey 
	AND c.FacilityId IN ('@warehouseId')
	AND (
		c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
		)
	AND (
		c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
		)
	AND (
		c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
		)
		
	AND c.IS_INBOUND = 1 and c.is_deleted=0
	AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
	AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
	AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
	AND (
		c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
		)
	AND c.milestoneStatus != null
GROUP BY c.milestoneStatus

-- CASE 2 IF @DateType = 'SHIPMENTDELIVERYDATE'  AND @nullCreatedDate = '*'

SELECT c.milestoneStatus
	,COUNT(c.milestoneStatus) milestoneStatusCount
FROM c
WHERE c.AccountId = @DPProductLineKey
	AND c.DP_SERVICELINE_KEY = @DPServiceLineKey 
	AND c.FacilityId IN ('@warehouseId')
	AND (
		c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
		)
	AND (
		c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
		)
	AND c.IS_INBOUND = 1
	AND c.is_deleted = 0
	AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
	AND c.activity_Movement_flag=1
	AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
	AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
	AND (
		c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
		)
	AND c.milestoneStatus != null
GROUP BY c.milestoneStatus	

-- Result Set 3 

--* Parameter requirement info.
-- @DPProductLineKey required,
-- @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate  AND  @Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate AND @Date.shipmentCreationEndDate are optional,
-- @Date.scheduledDeliveryStartDate   AND @Date.scheduledDeliveryEndDate are optional
-- isASN  optional
-- @Date.actualDeliveryStartDate	AND @Date.actualDeliveryEndDate are optional

--* Target Container-digital_summary_orders

-- CASE 1 IF @DateType = 'SHIPMENTCREATIONDATE'  and  @nullActualDeliveryDate = '*'

SELECT c.ServiceMode as  ShipmentMode
	,COUNT(c.ServiceMode) AS ShipmentModeCount 
FROM c
WHERE c.AccountId = @DPProductLineKey
	AND c.DP_SERVICELINE_KEY = @DPServiceLineKey 
	AND c.FacilityId IN ('@warehouseId')
	AND (
		c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
		)
	AND (
		c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
		)
	AND (
		c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
		)
	AND c.IS_INBOUND = 1 and c.is_deleted=0
	AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
	AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
	AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
	AND (
		c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
		)
	AND c.ServiceMode != null
GROUP BY c.ServiceMode

-- CASE 2 IF @DateType = 'SHIPMENTDELIVERYDATE'  AND @nullCreatedDate = '*'

SELECT c.ServiceMode as  ShipmentMode
	,COUNT(c.ServiceMode) AS ShipmentModeCount
FROM c
WHERE c.AccountId = @DPProductLineKey
	AND c.DP_SERVICELINE_KEY = @DPServiceLineKey 
	AND c.FacilityId IN ('@warehouseId')
	AND (
		c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
		)
	AND (
		c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
		)
	AND c.IS_INBOUND = 1
	AND c.is_deleted = 0
	AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
 	AND c.activity_Movement_flag=1  
	AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
	AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
	AND (
		c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
		)
	AND c.ServiceMode != null
GROUP BY c.ServiceMode

-- Result Set 4 

--* Parameter requirement info.
-- @DPProductLineKey required,
-- @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate  AND  @Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate AND @Date.shipmentCreationEndDate are optional,
-- @Date.scheduledDeliveryStartDate   AND @Date.scheduledDeliveryEndDate are optional
-- isASN  optional
-- @Date.actualDeliveryStartDate	AND @Date.actualDeliveryEndDate are optional

--* Target Container-digital_summary_orders

-- CASE 1 IF @DateType = 'SHIPMENTCREATIONDATE'  and  @nullActualDeliveryDate = '*'

SELECT c.OriginCountry
	,COUNT(c.OriginCountry) AS OriginCountryCount
FROM c
WHERE c.AccountId = @DPProductLineKey
	AND c.DP_SERVICELINE_KEY = @DPServiceLineKey 
	AND c.FacilityId IN ('@warehouseId')
	AND (
		c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
		)
	AND (
		c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
		)
	AND (
		c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
		)
	AND c.IS_INBOUND = 1 and c.is_deleted=0
	AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
	AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
	AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
	AND (
		c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
		)
	AND c.OriginCountry != null
GROUP BY c.OriginCountry

-- CASE 2 IF @DateType = 'SHIPMENTDELIVERYDATE'  AND @nullCreatedDate = '*'

SELECT c.OriginCountry
	,COUNT(c.OriginCountry) AS OriginCountryCount
FROM c
WHERE c.AccountId = @DPProductLineKey
	AND c.DP_SERVICELINE_KEY = @DPServiceLineKey 
	AND c.FacilityId IN ('@warehouseId')
	AND (
		c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
		)
	AND (
		c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
		)
	AND c.IS_INBOUND = 1
	AND c.is_deleted = 0
	AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
    AND c.activity_Movement_flag=1
	AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
	AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
	AND (
		c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
		)
	AND c.OriginCountry != null
GROUP BY c.OriginCountry

-- Result Set 5 

--* Parameter requirement info.
-- @DPProductLineKey required,
-- @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate  AND  @Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate AND @Date.shipmentCreationEndDate are optional,
-- @Date.scheduledDeliveryStartDate   AND @Date.scheduledDeliveryEndDate are optional
-- isASN  optional
-- @Date.actualDeliveryStartDate	AND @Date.actualDeliveryEndDate are optional

--* Target Container-digital_summary_orders

-- CASE 1 IF @DateType = 'SHIPMENTCREATIONDATE'  and  @nullActualDeliveryDate = '*'

SELECT 
    c.DestinationCountry, 
    COUNT(c.DestinationCountry) AS DestinationCountryCount 
 FROM c
WHERE c.AccountId = @DPProductLineKey
	AND c.DP_SERVICELINE_KEY = @DPServiceLineKey 
	AND c.FacilityId IN ('@warehouseId')
	AND (
		c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
		)
	AND (
		c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
		)
	AND (
		c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
		)
	AND c.IS_INBOUND = 1 and c.is_deleted=0
	AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
	AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
	AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
	AND (
		c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
		)
    and c.DestinationCountry != null
  GROUP BY c.DestinationCountry
  
-- CASE 2 IF @DateType = 'SHIPMENTDELIVERYDATE'  AND @nullCreatedDate = '*'  

SELECT 
    c.DestinationCountry, 
    COUNT(c.DestinationCountry) AS DestinationCountryCount 
 FROM c
WHERE c.AccountId = @DPProductLineKey
	AND c.DP_SERVICELINE_KEY = @DPServiceLineKey 
	AND c.FacilityId IN ('@warehouseId')
	AND (
		c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
		)
	AND (
		c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
		)
	AND c.IS_INBOUND = 1
	AND c.is_deleted = 0
	AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
 	AND c.activity_Movement_flag=1
	AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
	AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
	AND (
		c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
		)
    and c.DestinationCountry != null
  GROUP BY c.DestinationCountry
  
-- Result Set 6 

--* Parameter requirement info.
-- @DPProductLineKey required,
-- @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate  AND  @Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate AND @Date.shipmentCreationEndDate are optional,
-- @Date.scheduledDeliveryStartDate   AND @Date.scheduledDeliveryEndDate are optional
-- isASN  optional
-- @Date.actualDeliveryStartDate	AND @Date.actualDeliveryEndDate are optional

--* Target Container-digital_summary_orders

-- CASE 1 IF @DateType = 'SHIPMENTCREATIONDATE'  and  @nullActualDeliveryDate = '*'  

SELECT 
    c.ServiceLevel, 
    COUNT(c.ServiceLevel) AS serviceLevelCount  
 FROM c
WHERE c.AccountId = @DPProductLineKey
	AND c.DP_SERVICELINE_KEY = @DPServiceLineKey 
	AND c.FacilityId IN ('@warehouseId')
	AND (
		c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
		)
	AND (
		c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
		)
	AND (
		c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
		)
	AND c.IS_INBOUND = 1 and c.is_deleted=0
	AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
	AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
	AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
	AND (
		c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
		)
      and c.ServiceLevel != null
  GROUP BY c.ServiceLevel
  
-- CASE 2 IF @DateType = 'SHIPMENTDELIVERYDATE'  AND @nullCreatedDate = '*'  

SELECT 
    c.ServiceLevel, 
    COUNT(c.ServiceLevel) AS serviceLevelCount 
 FROM c
WHERE c.AccountId = @DPProductLineKey
	AND c.DP_SERVICELINE_KEY = @DPServiceLineKey 
	AND c.FacilityId IN ('@warehouseId')
	AND (
		c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
		)
	AND (
		c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
		)
	AND c.IS_INBOUND = 1
	AND c.is_deleted = 0
	AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
 	AND c.activity_Movement_flag=1
	AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
	AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
	AND (
		c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
		)
   and c.ServiceLevel != null
  GROUP BY c.ServiceLevel
  
-- Result Set 7 

--* Parameter requirement info.
-- @DPProductLineKey required,
-- @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate  AND  @Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate AND @Date.shipmentCreationEndDate are optional,
-- @Date.scheduledDeliveryStartDate   AND @Date.scheduledDeliveryEndDate are optional
-- isASN  optional
-- @Date.actualDeliveryStartDate	AND @Date.actualDeliveryEndDate are optional

--* Target Container-digital_summary_orders

-- CASE 1 IF @DateType = 'SHIPMENTCREATIONDATE'  and  @nullActualDeliveryDate = '*'  

SELECT 
    c.Carrier, 
    COUNT(c.Carrier) CarrierCount 
 FROM c
WHERE c.AccountId = @DPProductLineKey
	AND c.DP_SERVICELINE_KEY = @DPServiceLineKey 
	AND c.FacilityId IN ('@warehouseId')
	AND (
		c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
		)
	AND (
		c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
		)
	AND (
		c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
		)
	AND c.IS_INBOUND = 1 and c.is_deleted=0
	AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
	AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
	AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
	AND (
		c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
		)
	and c.Carrier != null
  GROUP BY c.Carrier
  
-- CASE 2 IF @DateType = 'SHIPMENTDELIVERYDATE'  AND @nullCreatedDate = '*'  

SELECT  
	c.Carrier, 
    COUNT(c.Carrier) CarrierCount 
 FROM c
WHERE c.AccountId = @DPProductLineKey
	AND c.DP_SERVICELINE_KEY = @DPServiceLineKey 
	AND c.FacilityId IN ('@warehouseId')
	AND (
		c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
		)
	AND (
		c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
		)
	AND c.IS_INBOUND = 1
	AND c.is_deleted = 0
	AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN  
	AND c.activity_Movement_flag=1
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
	AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
	AND (
		c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
		)
   and c.Carrier != null
  GROUP BY c.Carrier  
  
-- Result Set 8 

--* Parameter requirement info.
-- @DPProductLineKey required,
-- @Date.scheduleToShipStartDate  AND  @Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate AND @Date.shipmentCreationEndDate are optional,

--* Target Container-digital_summary_orders
 
SELECT COUNT(1) AS ScheduleToShipCount
FROM c
WHERE c.milestoneStatus IN ('TRANSPORTATION PLANNING')
	AND (
		c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
		)
	AND (
		c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
		)
	AND c.IS_INBOUND = 1
	AND c.is_deleted = 0
	AND c.AccountId = @DPProductLineKey
	AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
	AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
	--Data not available for this c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91' 

-- Result Set 9 

--* Parameter requirement info.
-- @DPProductLineKey required,
-- @Date.scheduleToShipStartDate  AND  @Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate AND @Date.shipmentCreationEndDate are optional,

--* Target Container-digital_summary_orders
 
SELECT COUNT(1) AS MissedPickupCount
FROM c
WHERE c.milestoneStatus IN ('TRANSPORTATION PLANNING')
	AND (
		c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
		)
	AND (
		c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
		)
	AND c.IS_INBOUND = 1
	AND c.is_deleted = 0
	AND c.AccountId = @DPProductLineKey
	AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
	AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'

-- Result Set 10

--* Parameter requirement info.
-- @DPProductLineKey required,
-- @Date.scheduledDeliveryStartDate  AND @Date.scheduledDeliveryEndDate are optional,
-- @Date.shipmentCreationStartDate AND @Date.shipmentCreationEndDate are optional,

--* Target Container-digital_summary_orders
 	
SELECT COUNT(1) AS ScheduledToDeliverCount
FROM c
WHERE c.milestoneStatus IN (
		'TRANSPORTATION PLANNING'
		,'IN TRANSIT'
		,'CUSTOMS'
		)
	AND (
		c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
		)
	AND (
		c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
		)
	AND c.IS_INBOUND = 1
	AND c.is_deleted = 0
	AND c.AccountId = @DPProductLineKey
	AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
	AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'

-- Result Set 11

--* Parameter requirement info.
-- @DPProductLineKey required,
-- @Date.scheduledDeliveryStartDate  AND @Date.scheduledDeliveryEndDate are optional,
-- @Date.shipmentCreationStartDate AND @Date.shipmentCreationEndDate are optional,

--* Target Container-digital_summary_orders
 	
 SELECT COUNT(1) AS MissedDeliveredCount
FROM c
WHERE c.milestoneStatus IN (
		'TRANSPORTATION PLANNING'
		,'IN TRANSIT'
		,'CUSTOMS'
		)
	AND (
		c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
		)
	AND (
		c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
		)
	AND c.IS_INBOUND = 1
	AND c.is_deleted = 0
	AND c.AccountId = @DPProductLineKey
	AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
	AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'	
	 
     
-- Result Set 12 

--* Parameter requirement info.
-- @DPProductLineKey required,
-- @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate  AND  @Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate AND @Date.shipmentCreationEndDate are optional,
-- @Date.scheduledDeliveryStartDate   AND @Date.scheduledDeliveryEndDate are optional
-- isASN  optional
-- @Date.actualDeliveryStartDate	AND @Date.actualDeliveryEndDate are optional

--* Target Container-digital_summary_orders

-- CASE 1 IF @DateType = 'SHIPMENTCREATIONDATE'  and  @nullActualDeliveryDate = '*'  

SELECT c.ServiceMode ShipmentMode
    ,c.deliveryStatus
	,COUNT(1) AS ShipmentCount 
FROM c
WHERE c.AccountId = @DPProductLineKey
	AND c.DP_SERVICELINE_KEY = @DPServiceLineKey 
	AND c.FacilityId IN ('@warehouseId')
	AND (
		c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
		)
	AND (
		c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
		)
	AND (
		c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
		)
	AND c.IS_INBOUND = 1 and c.is_deleted=0
	AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
	AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
	AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
	AND (
		c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
		)
	AND c.milestoneStatus IN (
		'DELIVERED'
		,'ASN CREATED'
		,'FTZ'
		,'RECEIVING'
		,'PUTAWAY'
		)
GROUP BY c.ServiceMode   ,c.deliveryStatus
  
-- CASE 2 IF @DateType = 'SHIPMENTDELIVERYDATE'  AND @nullCreatedDate = '*'  

SELECT  c.ServiceMode ShipmentMode 
    ,c.deliveryStatus
	,COUNT(1) AS ShipmentCount 
 FROM c
WHERE c.AccountId = @DPProductLineKey
	AND c.DP_SERVICELINE_KEY = @DPServiceLineKey 
	AND c.FacilityId IN ('@warehouseId')
	AND (
		c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
		)
	AND (
		c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
		)
	AND c.IS_INBOUND = 1
	AND c.is_deleted = 0
	AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
 	AND c.activity_Movement_flag=1
	AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
	AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
	AND (
		c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
		)
	AND c.milestoneStatus IN (
		'DELIVERED'
		,'ASN CREATED'
		,'FTZ'
		,'RECEIVING'
		,'PUTAWAY'
		)
GROUP BY c.ServiceMode 
  ,c.deliveryStatus
  	 

-- Result Set 13 

--* Parameter requirement info.
-- @DPProductLineKey required,
-- @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate  AND  @Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate AND @Date.shipmentCreationEndDate are optional,
-- @Date.scheduledDeliveryStartDate   AND @Date.scheduledDeliveryEndDate are optional
-- isASN  optional
-- @Date.actualDeliveryStartDate	AND @Date.actualDeliveryEndDate are optional

--* Target Container-digital_summary_orders

-- CASE 1 IF @DateType = 'SHIPMENTCREATIONDATE'  and  @nullActualDeliveryDate = '*'  

SELECT COUNT(a.UPSOrderNumber) AS TemperatureShipmentCount
FROM (
	SELECT DISTINCT c.UPSOrderNumber
		,c.ShipmentMode AS ShipmentMode
	FROM c
	WHERE c.AccountId = @DPProductLineKey
		AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
		AND c.FacilityId IN ('@warehouseId')
		AND (
		c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
		)
		AND (
		c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
		)
		AND (
		c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
		)
		AND c.IS_INBOUND = 1 and c.is_deleted=0
		AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
		AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
		AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
		AND (
		c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
		)
		AND c.IS_TEMPERATURE = 'Y'
		AND c.STATUSDETAILTYPE = 'TemperatureTracking'
	) a
GROUP BY a.ShipmentMode
  
-- CASE 2 IF @DateType = 'SHIPMENTDELIVERYDATE'  AND @nullCreatedDate = '*'  

SELECT COUNT(a.UPSOrderNumber) AS TemperatureShipmentCount
FROM (
	SELECT DISTINCT c.UPSOrderNumber
		,c.ShipmentMode AS ShipmentMode
	FROM c
	WHERE c.AccountId = @DPProductLineKey
		AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
		AND c.FacilityId IN ('@warehouseId')
		AND (
		c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
		)
		AND (
		c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
		)
		AND c.IS_INBOUND = 1
		AND c.is_deleted = 0
		AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
	 	AND c.activity_Movement_flag=1
		AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
		AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
		AND (
		c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
		)
		AND c.IS_TEMPERATURE = 'Y'
		AND c.STATUSDETAILTYPE = 'TemperatureTracking'
	) a	 
	GROUP BY a.ShipmentMode 
	
-- Result Set 14 

--* Parameter requirement info.
-- @DPProductLineKey required,
-- @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate  AND  @Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate AND @Date.shipmentCreationEndDate are optional,
-- @Date.scheduledDeliveryStartDate   AND @Date.scheduledDeliveryEndDate are optional
-- isASN  optional
-- @Date.actualDeliveryStartDate	AND @Date.actualDeliveryEndDate are optional

--* Target Container-digital_summary_orders

-- CASE 1 IF @DateType = 'SHIPMENTCREATIONDATE'  and  @nullActualDeliveryDate = '*'  

SELECT  c.Carrier
		,c.deliveryStatus
		,COUNT(1) AS Count
	FROM c
	WHERE c.AccountId = @DPProductLineKey
		AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
		AND c.FacilityId IN ('@warehouseId')
		AND (
		c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
		)
		AND (
		c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
		)
		AND (
		c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
		)
		AND c.IS_INBOUND = 1 and c.is_deleted=0
		AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
		AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
		AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
		AND c.Carrier != null  
		AND (
		c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
		)
		GROUP BY c.Carrier
		,c.deliveryStatus  
--ORDER BY Count DESC to be Applied at Backend
  
-- CASE 2 IF @DateType = 'SHIPMENTDELIVERYDATE'  AND @nullCreatedDate = '*'  

SELECT c.Carrier
		,c.deliveryStatus
		,COUNT(1) AS Count
	FROM c
	WHERE c.AccountId = @DPProductLineKey
		AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
		AND c.FacilityId IN ('@warehouseId')
		AND (
		c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
		)
		AND (
		c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
		)
		AND c.IS_INBOUND = 1
		AND c.is_deleted = 0
		AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
		AND c.activity_Movement_flag=1
		AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
		AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
		AND (
		c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
		)
	 	AND c.Carrier != null
		GROUP BY c.Carrier
		,c.deliveryStatus   
--ORDER BY Count DESC to be Applied at Backend		
	
-- Result Set 15 

--* Parameter requirement info.
-- @DPProductLineKey required,
-- @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate  AND  @Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate AND @Date.shipmentCreationEndDate are optional,
-- @Date.scheduledDeliveryStartDate   AND @Date.scheduledDeliveryEndDate are optional
-- isASN  optional
-- @Date.actualDeliveryStartDate	AND @Date.actualDeliveryEndDate are optional

--* Target Container-digital_summary_orders

-- CASE 1 IF @DateType = 'SHIPMENTCREATIONDATE'  and  @nullActualDeliveryDate = '*'  

SELECT UPPER(c.OriginCity) SourceCity
		,UPPER(c.DestinationCity) DestinationCity
		,UPPER(c.OriginCountry) SourceCountry
		,UPPER(c.DestinationCountry) DestinationCountry
		,UPPER(c.deliveryStatus)
		,COUNT(1) Count
	FROM c
	WHERE c.AccountId = @DPProductLineKey
		AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
		AND c.FacilityId IN ('@warehouseId')
		AND (
		c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
		)
		AND (
		c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
		)
		AND (
		c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
		)
		AND c.IS_INBOUND = 1 and c.is_deleted=0
		AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
		AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
		AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
	 	AND (UPPER(c.OriginCity) NOT LIKE '%NOT AVAILABLE%'
        AND UPPER(c.DestinationCity) NOT LIKE '%NOT AVAILABLE%')
		AND (
		c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
		)
		AND c.milestoneStatus IN ('DELIVERED','ASN CREATED','FTZ','RECEIVING','PUTAWAY')  
GROUP BY UPPER(c.OriginCity)
        ,UPPER(c.DestinationCity)
        ,UPPER(c.OriginCountry)
        ,UPPER(c.DestinationCountry)
        ,UPPER(c.deliveryStatus)       
--ORDER BY Count DESC to be Applied at Backend
  
-- CASE 2 IF @DateType = 'SHIPMENTDELIVERYDATE'  AND @nullCreatedDate = '*'  

SELECT UPPER(c.OriginCity) SourceCity
		,UPPER(c.DestinationCity) DestinationCity
		,UPPER(c.OriginCountry) SourceCountry
		,UPPER(c.DestinationCountry) DestinationCountry
		,UPPER(c.deliveryStatus)
		,COUNT(1) Count
	FROM c
	WHERE c.AccountId = @DPProductLineKey
		AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
		AND c.FacilityId IN ('@warehouseId')
		AND (
		c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
		)
		AND (
		c.LoadLatestDeliveryDate BETWEEN @Date.scheduledDeliveryStartDate 
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduledDeliveryEndDate))
		)
		AND c.IS_INBOUND = 1
		AND c.is_deleted = 0
		AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
	 	AND c.activity_Movement_flag=1
		AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
		AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
 		AND (UPPER(c.OriginCity) NOT LIKE '%NOT AVAILABLE%'
        AND UPPER(c.DestinationCity) NOT LIKE '%NOT AVAILABLE%')
		AND (
		c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
		)
		AND c.milestoneStatus IN ('DELIVERED','ASN CREATED','FTZ','RECEIVING','PUTAWAY')  
GROUP BY UPPER(c.OriginCity)
        ,UPPER(c.DestinationCity)
        ,UPPER(c.OriginCountry)
        ,UPPER(c.DestinationCountry)
        ,UPPER(c.deliveryStatus)      
--ORDER BY Count DESC to be Applied at Backend