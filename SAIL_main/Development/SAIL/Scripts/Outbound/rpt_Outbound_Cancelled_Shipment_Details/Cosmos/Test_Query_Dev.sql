
--AUTHOR : SHREY JAIN
--DESCRIPTION:rpt_Outbound_Cancelled_Shipment_Details
--DATE : 30-03-2022

--Result Set 1

/*
Target container - digital_summary_orders
*/

SELECT COUNT(1) totalCount
FROM c
WHERE (
		c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
		OR c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
		)
	AND (
		c.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
		OR c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'
		)
	AND (
		(
			'*' = '*'
			OR '*' = ''
			OR '*' = null
			) ? 0 : (STRINGEQUALS('*', 'OUTBOUND', true) ? 0 : (STRINGEQUALS('*', 'MOVEMENT', true) ? 2 : null))
		) = c.IS_INBOUND --Sprint 52
	AND c.is_deleted = 0
	AND c.FacilityId IN ("FC4B9B8B-E15F-4D40-899E-169D301ADC75")
	AND (
		(
			(
				c.DateTimeCancelled BETWEEN '2022-01-30 00:00:00.000'
					AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-03-30 00:00:00.000'))
				)
			AND c.OrderCancelledFlag = 'Y'
			)
		OR (
			(
				c.ShipmentLineCanceledDate BETWEEN '2022-01-30 00:00:00.000'
					AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-03-30 00:00:00.000'))
				)
			AND (c.ShipmentLineCanceledFlag = null ? 'Y' : c.ShipmentLineCanceledFlag) = 'Y'
			)
		)
	-- AND  (c.DateTimeCancelled BETWEEN  '2022-01-30 00:00:00.000' AND   DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-03-30 00:00:00.000')))  
	-- uncomment if using  paramenter @shipmentCanceledOnEndDate in stored proc call
	AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = c.is_managed --Sprint 52
--  AND UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN ('B2B') --null --Sprint 52

--Result Set 2

/*
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
		c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
		OR c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
		)
	AND (
		c.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
		OR c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'
		)
	AND (
		(
			'*' = '*'
			OR '*' = ''
			OR '*' = null
			) ? 0 : (STRINGEQUALS('*', 'OUTBOUND', true) ? 0 : (STRINGEQUALS('*', 'MOVEMENT', true) ? 2 : null))
		) = c.IS_INBOUND --Sprint 52
	AND c.is_deleted = 0
	AND c.FacilityId IN ("FC4B9B8B-E15F-4D40-899E-169D301ADC75")
	AND (
		(
			(
				c.DateTimeCancelled BETWEEN '2022-01-30 00:00:00.000'
					AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-03-30 00:00:00.000'))
				)
			AND c.OrderCancelledFlag = 'Y'
			)
		OR (
			(
				c.ShipmentLineCanceledDate BETWEEN '2022-01-30 00:00:00.000'
					AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-03-30 00:00:00.000'))
				)
			AND (c.ShipmentLineCanceledFlag = null ? 'Y' : c.ShipmentLineCanceledFlag) = 'Y'
			)
		)
	--   AND  (c.DateTimeCancelled BETWEEN  '2022-01-30 00:00:00.000' AND   DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-03-30 00:00:00.000')))  -- filter on dateCancelled  
	-- uncomment above filter if using parameter =@shipmentCanceledOnEndDate in stored procedure.
	AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = c.is_managed --Sprint 52
--  AND UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN ('B2B') --null --Sprint 52
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
		c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
		OR c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
		)
	AND (
		c.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
		OR c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'
		)
	AND (
		(
			'*' = '*'
			OR '*' = ''
			OR '*' = null
			) ? 0 : (STRINGEQUALS('*', 'OUTBOUND', true) ? 0 : (STRINGEQUALS('*', 'MOVEMENT', true) ? 2 : null))
		) = c.IS_INBOUND --Sprint 52
	AND c.is_deleted = 0
	AND c.FacilityId IN ("FC4B9B8B-E15F-4D40-899E-169D301ADC75")
	AND (
		(
			(
				c.DateTimeCancelled BETWEEN '2022-01-30 00:00:00.000'
					AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-03-30 00:00:00.000'))
				)
			AND c.OrderCancelledFlag = 'Y'
			)
		OR (
			(
				c.ShipmentLineCanceledDate BETWEEN '2022-01-30 00:00:00.000'
					AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-03-30 00:00:00.000'))
				)
			AND (c.ShipmentLineCanceledFlag = null ? 'Y' : c.ShipmentLineCanceledFlag) = 'Y'
			)
		)
	-- AND  (c.DateTimeCancelled BETWEEN  '2022-01-30 00:00:00.000' AND   DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-03-30 00:00:00.000')))  
	-- uncomment above filter if using parameter =@shipmentCanceledOnEndDate in stored procedure.
	AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = c.is_managed --Sprint 52
--  AND UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN ('B2B') --null --Sprint 52
ORDER BY c.shipmentCanceledDateTime DESC
