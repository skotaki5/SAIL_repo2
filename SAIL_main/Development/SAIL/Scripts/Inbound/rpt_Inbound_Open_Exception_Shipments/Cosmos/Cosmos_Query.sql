--AUTHOR : 		VISHAL SHARMA
--DESCRIPTION:	rpt_Inbound_Open_Exception_Shipments
--DATE : 		01-03-2022

--Result Set 1

--* Parameter requirement info.
-- @DPProductLineKey required,
-- @DPServiceLineKey optional,
-- @warehouseId optional
-- @startDate AND @endDate are optional,
-- @isTemperatureTracked  optional

--* Target Container-digital_summary_orders

SELECT count(1) AS totalShipments
FROM c
WHERE c.AccountId = @DPProductLineKey
    AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
    AND c.FacilityId IN ('@warehouseId')
    AND (
        c.DateTimeReceived BETWEEN @startDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @endDate))
        )
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND c.exception != null
    AND ((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = 'N')

--Result Set 2

--* Parameter requirement info.
-- @DPProductLineKey required,
-- @DPServiceLineKey optional,
-- @warehouseId optional
-- @startDate AND @endDate are optional,
-- @isTemperatureTracked  optional

--* Target Container-digital_summary_orders

--CASE 1 IF @topRow = 0

SELECT c.referenceNumber
    ,c.upsShipmentNumber
    ,t.exceptionType
    ,t.exceptionReason
    ,c.milestoneStatus
    ,c.shipmentOrigin_addressLine1 AS shipmentOrigin__addressLine1
    ,c.shipmentOrigin_addressLine2 AS shipmentOrigin__addressLine2
    ,c.shipmentOrigin_city AS shipmentOrigin__city
    ,c.shipmentOrigin_stateProvince AS shipmentOrigin__stateProvince
    ,c.shipmentOrigin_postalCode AS shipmentOrigin__postalCode
    ,c.shipmentOrigin_country AS shipmentOrigin__country
    ,c.shipmentDestination_addressLine1 AS shipmentDestination__addressLine1
    ,c.shipmentDestination_addressLine2 AS shipmentDestination__addressLine2
    ,c.shipmentDestination_city AS shipmentDestination__city
    ,c.shipmentDestination_stateProvince AS shipmentDestination__stateProvince
    ,c.shipmentDestination_postalCode AS shipmentDestination__postalCode
    ,c.shipmentDestination_country AS shipmentDestination__country
    ,c.shipmentServiceLevel
    ,c.shipmentServiceLevelCode
    ,c.shipmentCarrierCode
    ,c.shipmentCarrier
    ,c.warehouseId
    ,c.actualShipmentDateTime
    ,c.shipmentPlaceDateTime
    ,c.originalScheduledDeliveryDateTime
    ,c.actualScheduledDeliveryDateTime
    ,c.customerPONumber
    ,c.carrierShipmentNumber
    ,c.isTemperatureTracked
    ,c.latestTemperature
    ,c.temperatureDateTime
    ,c.temperatureCity
    ,c.temperatureState
    ,c.temperatureCountry
FROM c
JOIN t IN c.exception
WHERE c.AccountId = @DPProductLineKey
    AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
    AND c.FacilityId IN ('@warehouseId')
    AND (
        c.DateTimeReceived BETWEEN @startDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @endDate))
        )
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND c.exception != null
    AND ((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = 'N')
ORDER BY c.shipmentPlaceDateTime


--CASE 2 IF @topRow > 0

SELECT TOP 100 c.referenceNumber
    ,c.upsShipmentNumber
    ,t.exceptionType
    ,t.exceptionReason
    ,c.milestoneStatus
    ,c.shipmentOrigin_addressLine1 AS shipmentOrigin__addressLine1
    ,c.shipmentOrigin_addressLine2 AS shipmentOrigin__addressLine2
    ,c.shipmentOrigin_city AS shipmentOrigin__city
    ,c.shipmentOrigin_stateProvince AS shipmentOrigin__stateProvince
    ,c.shipmentOrigin_postalCode AS shipmentOrigin__postalCode
    ,c.shipmentOrigin_country AS shipmentOrigin__country
    ,c.shipmentDestination_addressLine1 AS shipmentDestination__addressLine1
    ,c.shipmentDestination_addressLine2 AS shipmentDestination__addressLine2
    ,c.shipmentDestination_city AS shipmentDestination__city
    ,c.shipmentDestination_stateProvince AS shipmentDestination__stateProvince
    ,c.shipmentDestination_postalCode AS shipmentDestination__postalCode
    ,c.shipmentDestination_country AS shipmentDestination__country
    ,c.shipmentServiceLevel
    ,c.shipmentServiceLevelCode
    ,c.shipmentCarrierCode
    ,c.shipmentCarrier
    ,c.warehouseId
    ,c.actualShipmentDateTime
    ,c.shipmentPlaceDateTime
    ,c.originalScheduledDeliveryDateTime
    ,c.actualScheduledDeliveryDateTime
    ,c.isTemperatureTracked
    ,c.latestTemperature
    ,c.latestTemperatureInCelsius
    ,c.latestTemperatureInFahrenheit
    ,c.temperatureDateTime
    ,c.temperatureCity
    ,c.temperatureState
FROM c
JOIN t IN c.exception
WHERE c.AccountId = @DPProductLineKey
    AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
    AND c.FacilityId IN ('@warehouseId')
    AND (
        c.DateTimeReceived BETWEEN @startDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @endDate))
        )
    AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE) = 'N'))
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND c.exception != null
ORDER BY c.shipmentPlaceDateTime