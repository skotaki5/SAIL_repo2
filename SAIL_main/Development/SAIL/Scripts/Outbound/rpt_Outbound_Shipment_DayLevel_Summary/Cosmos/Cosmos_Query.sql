--AUTHOR : 		VISHAL SHARMA
--DESCRIPTION:	rpt_Outbound_Shipment_DayLevel_Summary
--DATE : 		05-04-2022

Result Set 1

CASE 1:   IF @DateType = 'shipmentCreationDate'
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @StartDate and @EndDate optional,
-- @isManaged  optional UPPER
-- @OrderType optional  UPPER
  
SELECT count(1) total from T
WHERE
		(T.AccountId IN (@AccountKeys.DPProductLineKey)
			OR T.AccountId =@DPProductLineKey)--Sprint 52 Changes
		AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
			OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)--Sprint 52 Changes
		AND T.FacilityId IN (@warehouseId)
-- AND ((T.DateTimeShipped BETWEEN @StartDate AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @EndDate)))) --IF @DateType = 'shipmentShippedDate'
		AND (T.DateTimeReceived BETWEEN @StartDate AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @EndDate)))  --  IF @DateType = 'shipmentCreationDate'
-- AND ((T.actualDeliveryDateTime BETWEEN @StartDate AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @EndDate)))) -- IF @DateType = 'actualDeliveryDate'
-- for else condition all date filters will be applied
		AND T.IS_INBOUND = 0 
		AND T.is_deleted=0
		AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed--Sprint 52 Changes
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType))--Sprint 52 Changes
		AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) = 'N'--Sprint 52 Changes
 
CASE 2:  IF @DateType = 'shipmentShippedDate'
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @StartDate and @EndDate optional,
-- @isManaged  optional UPPER
-- @OrderType optional  UPPER


SELECT count(1) total from T
WHERE
		(T.AccountId IN (@AccountKeys.DPProductLineKey)
			OR T.AccountId =@DPProductLineKey)--Sprint 52 Changes
		AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
			OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)--Sprint 52 Changes
		AND T.FacilityId IN (@warehouseId)
		AND ((T.DateTimeShipped BETWEEN @StartDate AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @EndDate)))) --IF @DateType = 'shipmentShippedDate'
-- AND (T.DateTimeReceived BETWEEN @StartDate AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @EndDate)))  --  IF @DateType = 'shipmentCreationDate'
-- AND ((T.actualDeliveryDateTime BETWEEN @StartDate AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @EndDate)))) -- IF @DateType = 'actualDeliveryDate'
-- for else condition all date filters will be applied
		AND T.IS_INBOUND = 0 
		AND T.is_deleted=0
		AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed--Sprint 52 Changes
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType))--Sprint 52 Changes
		AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) = 'N'--Sprint 52 Changes

CASE 3:  IF @DateType = 'actualDeliveryDate'
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @StartDate and @EndDate optional,
-- @isManaged  optional  UPPER
-- @OrderType optional    UPPER

SELECT count(1) total from T
WHERE
		(T.AccountId IN (@AccountKeys.DPProductLineKey)
			OR T.AccountId =@DPProductLineKey)--Sprint 52 Changes
		AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
			OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)--Sprint 52 Changes
		AND T.FacilityId IN (@warehouseId)
-- AND ((T.DateTimeShipped BETWEEN @StartDate AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @EndDate)))) --IF @DateType = 'shipmentShippedDate'
-- AND (T.DateTimeReceived BETWEEN @StartDate AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @EndDate)))  --  IF @DateType = 'shipmentCreationDate'
		AND ((T.actualDeliveryDateTime BETWEEN @StartDate AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @EndDate)))) -- IF @DateType = 'actualDeliveryDate'
-- for else condition bothe date filters will be applied
		AND T.IS_INBOUND = 0 
		AND T.is_deleted=0
		AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed--Sprint 52 Changes
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType))--Sprint 52 Changes
		AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) = 'N'--Sprint 52 Changes
 
CASE 4: ELSE
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @StartDate and @EndDate optional,
-- @isManaged  optional  UPPER
-- @OrderType optional  UPPER

SELECT count(1) total from T
WHERE
		(T.AccountId IN (@AccountKeys.DPProductLineKey)
			OR T.AccountId =@DPProductLineKey)--Sprint 52 Changes
		AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
			OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)--Sprint 52 Changes
		AND T.FacilityId IN (@warehouseId)
		AND ((T.DateTimeShipped BETWEEN @StartDate AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @EndDate)))) --IF @DateType = 'shipmentShippedDate'
		AND (T.DateTimeReceived BETWEEN @StartDate AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @EndDate)))  --  IF @DateType = 'shipmentCreationDate'
		AND ((T.actualDeliveryDateTime BETWEEN @StartDate AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @EndDate)))) -- IF @DateType = 'actualDeliveryDate'
-- for else condition bothe date filters will be applied
		AND T.IS_INBOUND = 0 
		AND T.is_deleted=0
		AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed--Sprint 52 Changes
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType))--Sprint 52 Changes
		AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) = 'N'--Sprint 52 Changes

Result Set 2 IF @DateType = 'shipmentCreationDate'
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @StartDate and @EndDate optional,
-- @isManaged  optional  UPPER
-- @OrderType optional  UPPER

SELECT
        T.ShipmentCreationDate ShipmentCreationDate,
        COUNT(1) AS ShipmentCreationDateCount
        FROM T 
        WHERE 
			is_null(T.ShipmentCreationDate) = false
		AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
			OR T.AccountId =@DPProductLineKey)--Sprint 52 Changes
		AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
			OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)--Sprint 52 Changes
		AND T.FacilityId IN (@warehouseId)
		AND (
            T.DateTimeReceived BETWEEN @StartDate AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @EndDate))
            )
		AND T.IS_INBOUND = 0
		AND T.is_deleted = 0	
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed--Sprint 52 Changes
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType))--Sprint 52 Changes
		AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) = 'N'--Sprint 52 Changes
	
		GROUP BY T.ShipmentCreationDate
--      ORDER BY T.ShipmentCreationDate 
-- 		Order by to be applied at BACKEND

Result Set 3: IF @DateType = 'shipmentShippedDate'
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @StartDate and @EndDate optional,
-- @isManaged  optional UPPER
-- @OrderType optional UPPER

SELECT
        T.ShipmentShippedDate ShipmentShippedDate,
        COUNT(1) AS ShipmentShippedDateCount
        FROM T 
        WHERE 
		is_null(T.ShipmentShippedDate) = false
		AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
			OR T.AccountId =@DPProductLineKey)--Sprint 52 Changes
		AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
			OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)--Sprint 52 Changes
		AND T.FacilityId IN (@warehouseId)
		AND (
            T.DateTimeShipped BETWEEN @StartDate AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @EndDate))
            )
		AND T.IS_INBOUND = 0
		AND T.is_deleted = 0	
		AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed--Sprint 52 Changes
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType))--Sprint 52 Changes
		AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) = 'N'--Sprint 52 Changes
		GROUP BY T.ShipmentShippedDate
--      ORDER BY T.DateTimeShipped
-- 		Order by to be applied at BACKEND
 		
 
Result Set 4	IF @DateType = 'actualDeliveryDate' --Sprint 52 Changes
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @StartDate and @EndDate optional,
-- @isManaged  optional  UPPER
-- @OrderType optional  UPPER

SELECT
      T.ActualDeliveryDateTime_date AS ActualDeliveryDateTime, --Sprint 52 Changes
      COUNT(1) AS ActualDeliveryDateCount
    FROM T 
    WHERE 
	is_null(T.ActualDeliveryDateTime_date) = false
	AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
			OR T.AccountId =@DPProductLineKey)--Sprint 52 Changes
	AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
			OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)--Sprint 52 Changes
	AND T.FacilityId IN (@warehouseId)
	AND (
            T.actualDeliveryDateTime BETWEEN @StartDate AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @EndDate))
            )
	AND T.IS_INBOUND = 0
    AND T.is_deleted = 0	
    AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed--Sprint 52 Changes
	AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType))--Sprint 52 Changes
	AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) = 'N'--Sprint 52 Changes
	GROUP BY T.ActualDeliveryDateTime_date --Sprint 52 Changes
--    ORDER BY T.actualDeliveryDateTime
-- 	  Order by to be applied at BACKEND