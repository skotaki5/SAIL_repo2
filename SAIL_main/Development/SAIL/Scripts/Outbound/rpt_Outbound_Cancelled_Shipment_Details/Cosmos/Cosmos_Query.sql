
--AUTHOR : SHREY JAIN
--DESCRIPTION:rpt_Outbound_Cancelled_Shipment_Details
--DATE : 30-03-2022



--Result Set 1
/*
param requirement information -

@DPProductLineKey             required
@DPServiceLineKey             optional
@DPEntityKey                  optional
@startDate                    optional
@endDate                      optional
@shipmentCanceledOnStartDate  optional
@shipmentCanceledOnEndDate    optional
@AccountKeys                  required
@warehouseId                  optional
@topRow                       optional
@shipmentType                 optional
@IsManaged                    optional
@orderType                    optional

Target container - digital_summary_orders
*/



SELECT COUNT(1) totalCount
FROM c
WHERE (
		c.AccountId IN (@AccountKeys.DPProductLineKey)
		OR c.AccountId =@DPProductLineKey
		)
	AND (
		c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
		OR c.DP_SERVICELINE_KEY = @DPServiceLineKey
		)
	AND (
		(
			@shipmentType = '*'
			OR @shipmentType = ''
			OR @shipmentType = null
			) ? 0 : (STRINGEQUALS(@shipmentType, 'OUTBOUND', true) ? 0 : (STRINGEQUALS(@shipmentType, 'MOVEMENT', true) ? 2 : null))
		) = c.IS_INBOUND --Sprint 52
	AND c.is_deleted = 0
	AND c.FacilityId IN (@warehouseId)
	AND (
		(
			(
				c.DateTimeCancelled BETWEEN @startDate
					AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @endDate))
				)
			AND c.OrderCancelledFlag = 'Y'
			)
		OR (
			(
				c.ShipmentLineCanceledDate BETWEEN @startDate
					AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @endDate))
				)
			AND (c.ShipmentLineCanceledFlag = null ? 'Y' : c.ShipmentLineCanceledFlag) = 'Y'
			)
		)
	AND  (c.DateTimeCancelled BETWEEN  @shipmentCanceledOnStartDate AND   DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, @shipmentCanceledOnEndDate)))
	AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = c.is_managed --Sprint 52
    AND UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN (@orderType) --null --Sprint 52

--Result Set 2

/*
param requirement information -

@DPProductLineKey             required
@DPServiceLineKey             optional
@DPEntityKey                  optional
@startDate                    optional
@endDate                      optional
@shipmentCanceledOnStartDate  optional
@shipmentCanceledOnEndDate    optional
@AccountKeys                  required
@warehouseId                  optional
@topRow                       optional
@shipmentType                 optional
@IsManaged                    optional
@orderType                    optional

Target container - digital_summary_orders
*/

-- IF @topRow = 0

SELECT c.shipmentCanceledDateTime
	,c.shipmentCanceledBy
	,c.shipmentCanceledReason
	,c.LineNumber
	,c.shipmentLineCanceledDateTime
	,c.shipmentLineCanceledBy
	,c.shipmentLineCanceledReason
	,c.upsShipmentNumber
	,c.clientShipmentNumber
	,c.shipmentNumber
	,c.referenceNumber
	,c.customerPONumber
	,c.UPSOrderNumber orderNumber
	,c.shipmentCarrier
	,c.shipmentCarrierCode
	,c.shipmentServiceLevel
	,c.shipmentServiceLevelCode
	,c.ServiceMode
	,is_null(c.ShipmentLineCanceledFlag) ? 'N' :c.ShipmentLineCanceledFlag AS ShipmentLineCanceledFlag
	,c.shipmentServiceLevel AS serviceName
	,c.shipmentCanceledDateTimeZone
	,c.shipmentType
	,c.carrierShipmentNumber
	,c.Accountnumber accountNumber
	,c.dpProductLineKey
	,c.shipmentDescription orderType
FROM c
WHERE (
		c.AccountId IN (@AccountKeys.DPProductLineKey)
		OR c.AccountId =@DPProductLineKey
		)
	AND (
		c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
		OR c.DP_SERVICELINE_KEY = @DPServiceLineKey
		)
	AND (
		(
			@shipmentType = '*'
			OR @shipmentType = ''
			OR @shipmentType = null
			) ? 0 : (STRINGEQUALS(@shipmentType, 'OUTBOUND', true) ? 0 : (STRINGEQUALS(@shipmentType, 'MOVEMENT', true) ? 2 : null))
		) = c.IS_INBOUND --Sprint 52
	AND c.is_deleted = 0
	AND c.FacilityId IN (@warehouseId)
	AND (
		(
			(
				c.DateTimeCancelled BETWEEN @startDate
					AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @endDate))
				)
			AND c.OrderCancelledFlag = 'Y'
			)
		OR (
			(
				c.ShipmentLineCanceledDate BETWEEN @startDate
					AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @endDate))
				)
			AND (c.ShipmentLineCanceledFlag = null ? 'Y' : c.ShipmentLineCanceledFlag) = 'Y'
			)
		)
	AND  (c.DateTimeCancelled BETWEEN  @shipmentCanceledOnStartDate AND   DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, @shipmentCanceledOnEndDate)))
	AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = c.is_managed --Sprint 52
    AND UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN (@orderType) --null --Sprint 52
ORDER BY c.shipmentCanceledDateTime DESC

-- IF @topRow > 0

SELECT TOP 100 -- this value "100" need to be mapped with @TopRow parameter in stored procedure
	c.shipmentCanceledDateTime
	,c.shipmentCanceledBy
	,c.shipmentCanceledReason
	,c.LineNumber
	,c.shipmentLineCanceledDateTime
	,c.shipmentLineCanceledBy
	,c.shipmentLineCanceledReason
	,c.upsShipmentNumber
	,c.clientShipmentNumber
	,c.shipmentNumber
	,c.referenceNumber
	,c.customerPONumber
	,c.UPSOrderNumber orderNumber
	,c.shipmentCarrier
	,c.shipmentCarrierCode
	,c.shipmentServiceLevel
	,c.shipmentServiceLevelCode
	,c.ServiceMode
	,is_null(c.ShipmentLineCanceledFlag) ? 'N' :c.ShipmentLineCanceledFlag AS ShipmentLineCanceledFlag
	,c.shipmentServiceLevel AS serviceName
	,c.shipmentCanceledDateTimeZone
	,c.shipmentType
	,c.carrierShipmentNumber
	,c.Accountnumber accountNumber
	,c.dpProductLineKey
	,c.shipmentDescription orderType
FROM c
WHERE (
		c.AccountId IN (@AccountKeys.DPProductLineKey)
		OR c.AccountId =@DPProductLineKey
		)
	AND (
		c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
		OR c.DP_SERVICELINE_KEY = @DPServiceLineKey
		)
	AND (
		(
			@shipmentType = '*'
			OR @shipmentType = ''
			OR @shipmentType = null
			) ? 0 : (STRINGEQUALS(@shipmentType, 'OUTBOUND', true) ? 0 : (STRINGEQUALS(@shipmentType, 'MOVEMENT', true) ? 2 : null))
		) = c.IS_INBOUND --Sprint 52
	AND c.is_deleted = 0
	AND c.FacilityId IN (@warehouseId)
	AND (
		(
			(
				c.DateTimeCancelled BETWEEN @startDate
					AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @endDate))
				)
			AND c.OrderCancelledFlag = 'Y'
			)
		OR (
			(
				c.ShipmentLineCanceledDate BETWEEN @startDate
					AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @endDate))
				)
			AND (c.ShipmentLineCanceledFlag = null ? 'Y' : c.ShipmentLineCanceledFlag) = 'Y'
			)
		)
	AND  (c.DateTimeCancelled BETWEEN  @shipmentCanceledOnStartDate AND   DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, @shipmentCanceledOnEndDate)))
	AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = c.is_managed --Sprint 52
    AND UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN (@orderType) --null --Sprint 52
ORDER BY c.shipmentCanceledDateTime DESC
