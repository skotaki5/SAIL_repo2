-- #AUTHOR :		Vishal Sharma
-- #DESCRIPITION : rpt_Inbound_Shipment_Summary
-- #DATE : 		02-03-2022

-- Result Set 1

--* Target Container-digital_summary_orders

-- CASE 1 IF @DateType = 'SHIPMENTCREATIONDATE'  and  @nullActualDeliveryDate = '*'

SELECT COUNT(1) Total
FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
    -- AND (
    --  c.ScheduledPickUpDateTime BETWEEN '2021-12-11 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND (
        c.DateTimeReceived BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    -- AND (
    --  c.LoadLatestDeliveryDate BETWEEN '2021-12-11 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND c.IS_INBOUND = 1
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
    -- AND (
    --  c.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND c.milestoneStatus != null

-- CASE 2 IF @DateType = 'SHIPMENTDELIVERYDATE'  AND @nullCreatedDate = '*'

SELECT COUNT(1) Total
FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
    AND (
        c.ScheduledPickUpDateTime BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    AND (
        c.LoadLatestDeliveryDate BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-01-31 00:00:00.000"))
        )
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
    AND c.activity_Movement_flag=1
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
    AND (
        c.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    AND c.milestoneStatus != null
	
-- Result Set 2 

--* Target Container-digital_summary_orders

-- CASE 1 IF @DateType = 'SHIPMENTCREATIONDATE'  and  @nullActualDeliveryDate = '*'

SELECT c.milestoneStatus
    ,COUNT(c.milestoneStatus) milestoneStatusCount
FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
    -- AND (
    --  c.ScheduledPickUpDateTime BETWEEN '2021-12-11 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND (
        c.DateTimeReceived BETWEEN '2022-01-11 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-15 00:00:00.000'))
        )
    -- AND (
    --  c.LoadLatestDeliveryDate BETWEEN '2021-12-11 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
    -- AND (
    --  c.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND c.milestoneStatus != null
GROUP BY c.milestoneStatus

-- CASE 2 IF @DateType = 'SHIPMENTDELIVERYDATE'  AND @nullCreatedDate = '*'

SELECT c.milestoneStatus
    ,COUNT(c.milestoneStatus) milestoneStatusCount
FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
    AND (
        c.ScheduledPickUpDateTime BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    AND (
        c.LoadLatestDeliveryDate BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
    AND c.activity_Movement_flag=1
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
    AND (
        c.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-01-31 00:00:00.000"))
        )
    AND c.milestoneStatus != null
GROUP BY c.milestoneStatus

-- Result Set 3 

--* Target Container-digital_summary_orders

-- CASE 1 IF @DateType = 'SHIPMENTCREATIONDATE'  and  @nullActualDeliveryDate = '*'

SELECT c.ServiceMode as  ShipmentMode
    ,COUNT(c.ServiceMode) AS ShipmentModeCount
 
FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
    -- AND (
    --  c.ScheduledPickUpDateTime BETWEEN '2021-12-11 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND (
        c.DateTimeReceived BETWEEN '2022-01-11 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    -- AND (
    --  c.LoadLatestDeliveryDate BETWEEN '2021-12-11 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
    -- AND (
    --  c.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND c.ServiceMode != null
GROUP BY c.ServiceMode

-- CASE 2 IF @DateType = 'SHIPMENTDELIVERYDATE'  AND @nullCreatedDate = '*'

SELECT c.ServiceMode as  ShipmentMode
    ,COUNT(c.ServiceMode) AS ShipmentModeCount
FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
    AND (
        c.ScheduledPickUpDateTime BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    AND (
        c.LoadLatestDeliveryDate BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
    AND c.activity_Movement_flag=1
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
    AND (
        c.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    AND c.ServiceMode != null
GROUP BY c.ServiceMode

-- Result Set 4 

--* Target Container-digital_summary_orders

-- CASE 1 IF @DateType = 'SHIPMENTCREATIONDATE'  and  @nullActualDeliveryDate = '*'

SELECT c.OriginCountry
    ,COUNT(c.OriginCountry) AS OriginCountryCount
FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
    -- AND (
    --  c.ScheduledPickUpDateTime BETWEEN '2021-12-11 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND (
        c.DateTimeReceived BETWEEN '2022-01-11 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    -- AND (
    --  c.LoadLatestDeliveryDate BETWEEN '2021-12-11 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    -- AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
    -- AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    -- AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
    -- AND (
    --  c.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND c.OriginCountry != null
GROUP BY c.OriginCountry

-- CASE 2 IF @DateType = 'SHIPMENTDELIVERYDATE'  AND @nullCreatedDate = '*'

SELECT c.OriginCountry
    ,COUNT(c.OriginCountry) AS OriginCountryCount
FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
    AND (
        c.ScheduledPickUpDateTime BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    AND (
        c.LoadLatestDeliveryDate BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
    AND c.activity_Movement_flag=1
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
    AND (
        c.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    AND c.OriginCountry != null
GROUP BY c.OriginCountry

-- Result Set 5 

--* Target Container-digital_summary_orders

-- CASE 1 IF @DateType = 'SHIPMENTCREATIONDATE'  and  @nullActualDeliveryDate = '*'

SELECT
    c.DestinationCountry,
    COUNT(c.DestinationCountry) AS DestinationCountryCount
 FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
    -- AND (
    --  c.ScheduledPickUpDateTime BETWEEN '2021-12-11 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND (
        c.DateTimeReceived BETWEEN '2022-01-11 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    -- AND (
    --  c.LoadLatestDeliveryDate BETWEEN '2021-12-11 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    -- AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
    -- AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    -- AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
    -- AND (
    --  c.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    and c.DestinationCountry != null
  GROUP BY c.DestinationCountry
  
-- CASE 2 IF @DateType = 'SHIPMENTDELIVERYDATE'  AND @nullCreatedDate = '*'  

SELECT
    c.DestinationCountry,
    COUNT(c.DestinationCountry) AS DestinationCountryCount
 FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
    AND (
        c.ScheduledPickUpDateTime BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    AND (
        c.LoadLatestDeliveryDate BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
    AND c.activity_Movement_flag=1
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
    AND (
        c.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    and c.DestinationCountry != null
  GROUP BY c.DestinationCountry
  
-- Result Set 6 

--* Target Container-digital_summary_orders

-- CASE 1 IF @DateType = 'SHIPMENTCREATIONDATE'  and  @nullActualDeliveryDate = '*'  

SELECT
    c.ServiceLevel,
    COUNT(c.ServiceLevel) AS serviceLevelCount 
 FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
    -- AND (
    --  c.ScheduledPickUpDateTime BETWEEN '2021-12-11 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND (
        c.DateTimeReceived BETWEEN '2022-01-11 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    -- AND (
    --  c.LoadLatestDeliveryDate BETWEEN '2021-12-11 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    -- AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
    -- AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    -- AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
    -- AND (
    --  c.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
      and c.ServiceLevel != null
  GROUP BY c.ServiceLevel
  
-- CASE 2 IF @DateType = 'SHIPMENTDELIVERYDATE'  AND @nullCreatedDate = '*'  

SELECT
    c.ServiceLevel,
    COUNT(c.ServiceLevel) AS serviceLevelCount
 FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
    AND (
        c.ScheduledPickUpDateTime BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    AND (
        c.LoadLatestDeliveryDate BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
    AND c.activity_Movement_flag=1
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
    AND (
        c.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
   and c.ServiceLevel != null
  GROUP BY c.ServiceLevel
  
-- Result Set 7 

--* Target Container-digital_summary_orders

-- CASE 1 IF @DateType = 'SHIPMENTCREATIONDATE'  and  @nullActualDeliveryDate = '*'  

SELECT
    c.Carrier,
    COUNT(c.Carrier) CarrierCount
 FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
    -- AND (
    --  c.ScheduledPickUpDateTime BETWEEN '2021-12-11 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND (
        c.DateTimeReceived BETWEEN '2022-01-11 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    -- AND (
    --  c.LoadLatestDeliveryDate BETWEEN '2021-12-11 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
    -- AND (
    --  c.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
     and c.Carrier != null
  GROUP BY c.Carrier
  
-- CASE 2 IF @DateType = 'SHIPMENTDELIVERYDATE'  AND @nullCreatedDate = '*'  

SELECT 
    c.Carrier,
    COUNT(c.Carrier) CarrierCount
 FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
    AND (
        c.ScheduledPickUpDateTime BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    AND (
        c.LoadLatestDeliveryDate BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
    AND c.activity_Movement_flag=1
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
    AND (
        c.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
   and c.Carrier != null
  GROUP BY c.Carrier 
  
-- Result Set 8

--* Target Container-digital_summary_orders


SELECT COUNT(1) AS ScheduleToShipCount
FROM c
WHERE c.milestoneStatus IN ('TRANSPORTATION PLANNING')
    -- AND (
    --  c.ScheduledPickUpDateTime BETWEEN '2021-12-11 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND (
        c.DateTimeReceived BETWEEN '2022-01-11 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-04-10 00:00:00.000'))
        )
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND c.AccountId = '870561E1-A974-483B-AA0D-A724C5D402C9'
    -- AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    -- AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
	

-- Result Set 9

--* Target Container-digital_summary_orders

SELECT COUNT(1) AS MissedPickupCount
FROM c
WHERE c.milestoneStatus IN ('TRANSPORTATION PLANNING')
    -- AND (
    --  c.ScheduledPickUpDateTime BETWEEN '2021-12-11 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND (
        c.DateTimeReceived BETWEEN '2022-01-11 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-04-10 00:00:00.000'))
        )
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND c.AccountId = '870561E1-A974-483B-AA0D-A724C5D402C9'
    -- AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    -- AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
	
-- Result Set 10

--* Target Container-digital_summary_orders
	
SELECT COUNT(1) AS ScheduledToDeliverCount
FROM c
WHERE c.milestoneStatus IN (
        'TRANSPORTATION PLANNING'
        ,'IN TRANSIT'
        ,'CUSTOMS'
        )
    -- AND (
    --  c.LoadLatestDeliveryDate BETWEEN '2021-12-11 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND (
        c.DateTimeReceived BETWEEN '2021-12-11 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
        )
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND c.AccountId = '870561E1-A974-483B-AA0D-A724C5D402C9'
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'  
	
-- Result Set 11

--* Target Container-digital_summary_orders	

SELECT COUNT(1) AS MissedDeliveredCount
FROM c
WHERE c.milestoneStatus IN (
        'TRANSPORTATION PLANNING'
        ,'IN TRANSIT'
        ,'CUSTOMS'
        )
    -- AND (
    --  c.LoadLatestDeliveryDate BETWEEN '2022-01-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
    --  )
    AND (
        c.DateTimeReceived BETWEEN '2021-12-11 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
        )
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND c.AccountId = '870561E1-A974-483B-AA0D-A724C5D402C9'
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'  
	
-- Result Set 12 

--* Target Container-digital_summary_orders

-- CASE 1 IF @DateType = 'SHIPMENTCREATIONDATE'  and  @nullActualDeliveryDate = '*'  

SELECT c.ServiceMode ShipmentMode
    ,c.deliveryStatus
    ,COUNT(1) AS ShipmentCount
FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
    -- AND (
    --  c.ScheduledPickUpDateTime BETWEEN '2021-12-11 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND (
        c.DateTimeReceived BETWEEN '2022-01-11 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    -- AND (
    --  c.LoadLatestDeliveryDate BETWEEN '2021-12-11 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
    -- AND (
    --  c.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
    --  )
    AND c.milestoneStatus IN (
        'DELIVERED'
        ,'ASN CREATED'
        ,'FTZ'
        ,'RECEIVING'
        ,'PUTAWAY'
        )
GROUP BY c.ServiceMode
    ,c.deliveryStatus
  
-- CASE 2 IF @DateType = 'SHIPMENTDELIVERYDATE'  AND @nullCreatedDate = '*'  

SELECT  c.ServiceMode ShipmentMode
    ,c.deliveryStatus
    ,COUNT(1) AS ShipmentCount
FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
    AND (
        c.ScheduledPickUpDateTime BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    AND (
        c.LoadLatestDeliveryDate BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
    AND c.activity_Movement_flag=1
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
    AND (
        c.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
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

--* Target Container-digital_summary_orders	

-- CASE 1 IF @DateType = 'SHIPMENTCREATIONDATE'  and  @nullActualDeliveryDate = '*'  

SELECT COUNT(a.UPSOrderNumber) AS TemperatureShipmentCount
FROM (
    SELECT DISTINCT c.UPSOrderNumber
        ,c.ShipmentMode AS ShipmentMode
    FROM c
    WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
        AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
        AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
        -- AND (
        --  c.ScheduledPickUpDateTime BETWEEN '2022-01-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        --  )
        AND (
            c.DateTimeReceived BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-04-10 00:00:00.000'))
            )
        -- AND (
        --  c.LoadLatestDeliveryDate BETWEEN '2022-01-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        --  )
        AND c.IS_INBOUND = 1
        AND c.is_deleted = 0
        -- AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
        AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
        AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
        -- AND (
        --  c.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        --  )
        -- AND c.IS_TEMPERATURE = 'Y'
        -- AND c.STATUSDETAILTYPE = 'TemperatureTracking'
    ) a
GROUP BY a.ShipmentMode

-- CASE 2 IF @DateType = 'SHIPMENTDELIVERYDATE'  AND @nullCreatedDate = '*'  

SELECT COUNT(a.UPSOrderNumber) AS TemperatureShipmentCount
FROM (
    SELECT DISTINCT c.UPSOrderNumber
        ,c.ShipmentMode AS ShipmentMode
    FROM c
    WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
        AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
        AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
        AND (
            c.ScheduledPickUpDateTime BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
            )
        AND (
            c.LoadLatestDeliveryDate BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
            )
        AND c.IS_INBOUND = 1
        AND c.is_deleted = 0
        AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
        AND c.activity_Movement_flag=1
        AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
        AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
        AND (
            c.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
            )
        --AND c.IS_TEMPERATURE = 'Y'
        --AND c.STATUSDETAILTYPE = 'TemperatureTracking'
    ) a
    GROUP BY a.ShipmentMode
	--we have null values for c.IS_TEMPERATURE, c.STATUSDETAILTYPE when c.IS_INBOUND = 1
	
-- Result Set 14 

--* Target Container-digital_summary_orders	

-- CASE 1 IF @DateType = 'SHIPMENTCREATIONDATE'  and  @nullActualDeliveryDate = '*' 

SELECT c.Carrier
        ,c.deliveryStatus
        ,COUNT(1) AS Count
    FROM c
    WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
        AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
        AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
        -- AND (
        --  c.ScheduledPickUpDateTime BETWEEN '2021-12-11 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
        --  )
        AND (
            c.DateTimeReceived BETWEEN '2022-01-11 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-04-10 00:00:00.000'))
            )
        -- AND (
        --  c.LoadLatestDeliveryDate BETWEEN '2021-12-11 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
        --  )
        AND c.IS_INBOUND = 1
        AND c.is_deleted = 0
        AND c.milestoneStatus IN ('DELIVERED','ASN CREATED','FTZ','RECEIVING','PUTAWAY')
        -- AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
        AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
        AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
        AND c.Carrier != null
        -- AND (
        --  c.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
        --  )
        GROUP BY c.Carrier
        ,c.deliveryStatus       
--ORDER BY Count DESC to be Applied at Backend	
	
-- CASE 2 IF @DateType = 'SHIPMENTDELIVERYDATE'  AND @nullCreatedDate = '*' 

SELECT c.Carrier
        ,c.deliveryStatus
        ,COUNT(1) AS Count
    FROM c
    WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
        AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
        AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
        AND (
            c.ScheduledPickUpDateTime BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
            )
        AND (
            c.LoadLatestDeliveryDate BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
            )
        AND c.IS_INBOUND = 1
        AND c.is_deleted = 0
        AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
        AND c.activity_Movement_flag=1
        AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
        AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
        AND (
            c.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
            )  
        AND c.Carrier != null
       GROUP BY c.Carrier
        ,c.deliveryStatus      
--ORDER BY Count DESC to be Applied at Backend

-- Result Set 15 

--* Target Container-digital_summary_orders	

-- CASE 1 IF @DateType = 'SHIPMENTCREATIONDATE'  and  @nullActualDeliveryDate = '*' 	

SELECT UPPER(c.OriginCity) SourceCity
        ,UPPER(c.DestinationCity) DestinationCity
        ,UPPER(c.OriginCountry) SourceCountry
        ,UPPER(c.DestinationCountry) DestinationCountry
        ,UPPER(c.deliveryStatus)
        ,COUNT(1) Count
    FROM c
    WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
        AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
        AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
        -- AND (
        --  c.ScheduledPickUpDateTime BETWEEN '2022-01-11 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-04-10 00:00:00.000'))
        --  )
        AND (
            c.DateTimeReceived BETWEEN '2022-01-11 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-04-10 00:00:00.000'))
            )
        -- AND (
        --  c.LoadLatestDeliveryDate BETWEEN '2022-01-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-04-31 00:00:00.000'))
        --  )
        AND c.IS_INBOUND = 1
        AND c.is_deleted = 0
        -- AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
        AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
        AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
        -- AND (UPPER(c.OriginCity) NOT LIKE '%NOT AVAILABLE%'
        -- AND UPPER(c.DestinationCity) NOT LIKE '%NOT AVAILABLE%')
        -- AND (
        --  c.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        --  )
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
    WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
        AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
        AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
        AND (
            c.ScheduledPickUpDateTime BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
            )
         
        AND (
            c.LoadLatestDeliveryDate BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
            )
        AND c.IS_INBOUND = 1
        AND c.is_deleted = 0
        AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
        AND c.activity_Movement_flag=1
        AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
        AND (is_null(c.OrderStatusName) ? '' :c.OrderStatusName) <> 'Cancelled'
        AND (UPPER(c.OriginCity) NOT LIKE '%NOT AVAILABLE%'
        AND UPPER(c.DestinationCity) NOT LIKE '%NOT AVAILABLE%')
        AND (
            c.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
            )
        AND c.milestoneStatus IN ('DELIVERED','ASN CREATED','FTZ','RECEIVING','PUTAWAY')  
GROUP BY UPPER(c.OriginCity)
        ,UPPER(c.DestinationCity)
        ,UPPER(c.OriginCountry)
        ,UPPER(c.DestinationCountry)
        ,UPPER(c.deliveryStatus)      
--ORDER BY Count DESC to be Applied at Backend