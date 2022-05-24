--AUTHOR : VISHAL
--DESCRIPTION:rpt_Inbound_Shipments
--DATE : 22-02-2022

--Result Set 1

--* Parameter requirement info.
-- @DPProductLineKey is required,
-- @DPServiceLineKey is optional,
-- @warehouseId is optional ,
-- @Date.shipmentCreationStartDate and @Date.shipmentCreationEndDate are optional,
-- @Date.scheduleDeliveryStartDate and  @Date.scheduleDeliveryEndDate are optional,
-- @Date.shippedStartDate and @Date.shippedEndDate are optional,
-- @Date.scheduleToShipStartDate and  @Date.scheduleToShipEndDate are optional,
-- @originCountry and @DestinationCountry are optional,
-- @OriginCountry and @destinationCountry are optional,
-- @CarrierTypeArray  and @ShipmentModeArray are optional,
-- @Date.deliveryEtaStartDate and  @Date.deliveryEtaEndDate are optional,
-- @Date.actualDeliveryStartDate and  @Date.actualDeliveryEndDate are optional,
-- @Date.pickupStartDate and  @Date.pickupEndDate are optional,
-- @Date.bookedStartDate and  @Date.bookedEndDate are optional,
-- @isTemperatureTracked and @milestoneStatus are optional,
-- @deliveryStatus is optional
-- @isClaim is optional
-- @inboundType is optional

--* Target Container-digital_summary_orders

SELECT COUNT(1) totalShipments
FROM c
WHERE c.AccountId = @DPProductLineKey
    AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
    AND c.FacilityId IN (@warehouseId)
    AND (
        c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
        )
    AND (
        c.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
        )
    AND (
        c.DateTimeShipped BETWEEN @Date.shippedStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shippedEndDate))
        )
    AND (
        c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
        )
    AND (INDEX_OF(@originLocation, ",") != - 1 ?concat(c.OriginCity, ',', c.OriginCountry) : c.OriginCountry) IN (@originLocation)
    AND (INDEX_OF(@destinationLocation, ",") != - 1 ?concat(c.DestinationCity, ',', c.DestinationCountry) : c.DestinationCountry) IN (@destinationLocation)
    AND (c.OriginCountry IN ('@originCountry'))
    AND (c.DestinationCountry IN ('@destinationCountry'))
    AND (c.OriginCity IN ('@originCity'))
    AND (c.DestinationCity IN ('@destinationCity'))
    AND c.IS_INBOUND = 1
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    AND ((is_null(c.Carrier) ? 'NOMODE' : c.Carrier) IN (@CarrierTypeArray))
    AND ((is_null(c.ServiceMode) ? 'NOMODE' : c.ServiceMode) IN (@ShipmentModeArray))
    AND ((is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN (@ServiceLevelArray))
    AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = @isTemperatureTracked))
    AND (
        c.estimatedDeliveryDateTime BETWEEN @Date.deliveryEtaStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.deliveryEtaEndDate))
        )
    AND (
        c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        )
    AND (
        c.PickUpDate BETWEEN @Date.pickupStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.pickupEndDate))
        )
    AND (
        c.shipmentBookedOnDateTime BETWEEN @Date.bookedStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.bookedEndDate))
        )
    AND (c.milestoneStatus IN (@milestoneStatus))
    AND (c.milestoneStatus IN  ('DELIVERED','ASN CREATED','FTZ','RECEIVING','PUTAWAY')
         
            AND c.deliveryStatus = @deliveryStatus
             
        )
    AND (c.ISCLAIM = @isClaim)
    AND c.is_deleted = 0

--Result Set 2

--* Parameter requirement info.
-- @DPProductLineKey is required,
-- @DPServiceLineKey is optional,
-- @warehouseId is optional ,
-- @Date.shipmentCreationStartDate and @Date.shipmentCreationEndDate are optional,
-- @Date.scheduleDeliveryStartDate and  @Date.scheduleDeliveryEndDate are optional,
-- @Date.shippedStartDate and @Date.shippedEndDate are optional,
-- @Date.scheduleToShipStartDate and  @Date.scheduleToShipEndDate are optional,
-- @originCountry and @DestinationCountry are optional,
-- @OriginCountry and @destinationCountry are optional,
-- @CarrierTypeArray  and @ShipmentModeArray are optional,
-- @Date.deliveryEtaStartDate and  @Date.deliveryEtaEndDate are optional,
-- @Date.actualDeliveryStartDate and  @Date.actualDeliveryEndDate are optional,
-- @Date.pickupStartDate and  @Date.pickupEndDate are optional,
-- @Date.bookedStartDate and  @Date.bookedEndDate are optional,
-- @isTemperatureTracked and @milestoneStatus are optional,
-- @deliveryStatus is optional
-- @isClaim is optional
-- @inboundType is optional

--* Target Container-digital_summary_orders

--CASE 1 IF @topRow = 0
SELECT DISTINCT c.shipmentNumber
    ,c.referenceNumber
    ,c.upsShipmentNumber
    ,c.clientShipmentNumber
    ,c.customerPONumber
    ,c.UPSOrderNumber orderNumber
    ,c.upsTransportShipmentNumber
    ,c.gffShipmentInstanceId
    ,c.gffShipmentNumber
    ,c.shipmentOrigin_contactName
    ,c.shipmentOrigin_addressLine1
    ,c.shipmentOrigin_addressLine2
    ,c.shipmentOrigin_city
    ,c.shipmentOrigin_stateProvince
    ,c.shipmentOrigin_postalCode
    ,c.shipmentOrigin_country
    ,c.shipmentDestination_contactName
    ,c.shipmentDestination_addressLine1
    ,c.shipmentDestination_addressLine2
    ,c.shipmentDestination_city
    ,c.shipmentDestination_stateProvince
    ,c.shipmentDestination_postalCode
    ,c.shipmentDestination_country
    ,c.shipmentDescription
    ,c.shipmentService
    ,c.shipmentServiceLevel
    ,c.shipmentServiceLevelCode
    ,c.shipmentCarrierCode
    ,c.shipmentCarrier = null ? 'UNASSIGNED' : c.shipmentCarrier shipmentCarrier
    ,c.inventoryShipmentStatus
    ,c.transportationMileStone
    ,c.shipmentPrimaryException
    ,c.shipmentBookedOnDateTime
    ,c.shipmentCanceledDateTime
    ,c.shipmentCanceledReason
    ,c.actualShipmentDateTime
    ,c.shipmentCreateOnDateTime
    ,c.originalScheduledDeliveryDateTime
    ,c.actualDeliveryDateTime
    ,c.warehouseId
    ,c.warehouseCode
    ,c.milestoneStatus
    ,c.inboundType
    ,c.estimatedDeliveryDateTime
    ,c.LoadID
    ,c.totalCharge
    ,c.totalChargeCurrency
    ,c.ISCLAIM isClaim
    ,c.deliveryStatus
    ,c.carrierShipmentNumber = null ? null : toString((select c.carrierShipmentNumber)) carrierShipmentNumber
    ,c.lastKnownLocation
    ,c.ServiceMode
    ,c.PickUpDate actualPickupDateTime
    ,c.DateTimeShipped shippedDateTime
    ,c.ScheduledPickUpDateTime scheduledPickupDateTime
    ,c.isTemperatureTracked
    ,c.ServiceLevel isShipmentServiceLevelResultSet
    ,c.latestTemperature
    ,c.temperatureDateTime
    ,c.temperatureCity
    ,c.temperatureState
    ,c.temperatureCountry
    ,c.IS_INBOUND isInbound
FROM c
WHERE c.AccountId = @DPProductLineKey
    AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
    AND c.FacilityId IN (@warehouseId)
    AND (
        c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
        )
    AND (
        c.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
        )
    AND (
        c.DateTimeShipped BETWEEN @Date.shippedStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shippedEndDate))
        )
    AND (
        c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
        )
    AND (INDEX_OF(@originLocation, ",") != - 1 ?concat(c.OriginCity, ',', c.OriginCountry) : c.OriginCountry) IN (@originLocation)
    AND (INDEX_OF(@destinationLocation, ",") != - 1 ?concat(c.DestinationCity, ',', c.DestinationCountry) : c.DestinationCountry) IN (@destinationLocation)  
    AND (c.OriginCountry IN ('@originCountry'))
    AND (c.DestinationCountry IN ('@destinationCountry'))
    AND (c.OriginCity IN ('@originCity'))
    AND (c.DestinationCity IN ('@destinationCity'))   
    AND c.IS_INBOUND = 1
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    AND ((is_null(c.Carrier) ? 'NOMODE' : c.Carrier) IN (@CarrierTypeArray))
    AND ((is_null(c.ServiceMode) ? 'NOMODE' : c.ServiceMode) IN (@ShipmentModeArray))
    AND ((is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN (@ServiceLevelArray))
    AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = @isTemperatureTracked))
    AND (
        c.estimatedDeliveryDateTime BETWEEN @Date.deliveryEtaStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.deliveryEtaEndDate))
        )
    AND (
        c.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        )
    AND (
        c.PickUpDate BETWEEN @Date.pickupStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.pickupEndDate))
        )
    AND (
        c.shipmentBookedOnDateTime BETWEEN @Date.bookedStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.bookedEndDate))
        )
    AND (c.milestoneStatus IN (@milestoneStatus))
    AND (c.milestoneStatus IN  ('DELIVERED','ASN CREATED','FTZ','RECEIVING','PUTAWAY')
         
            AND c.deliveryStatus = @deliveryStatus
             
        )
    AND (c.ISCLAIM = @isClaim)
    AND c.is_deleted = 0
ORDER BY c.shipmentCreateOnDateTime DESC

--CASE 2 IF @topRow > 0

SELECT DISTINCT TOP @topRow c.shipmentNumber
    ,c.referenceNumber
    ,c.upsShipmentNumber
    ,c.clientShipmentNumber
    ,c.customerPONumber
    ,c.UPSOrderNumber orderNumber
    ,c.upsTransportShipmentNumber
    ,c.gffShipmentInstanceId
    ,c.gffShipmentNumber
    ,c.shipmentOrigin_contactName
    ,c.shipmentOrigin_addressLine1
    ,c.shipmentOrigin_addressLine2
    ,c.shipmentOrigin_city
    ,c.shipmentOrigin_stateProvince
    ,c.shipmentOrigin_postalCode
    ,c.shipmentOrigin_country
    ,c.shipmentDestination_contactName
    ,c.shipmentDestination_addressLine1
    ,c.shipmentDestination_addressLine2
    ,c.shipmentDestination_city
    ,c.shipmentDestination_stateProvince
    ,c.shipmentDestination_postalCode
    ,c.shipmentDestination_country
    ,c.shipmentDescription
    ,c.shipmentService
    ,c.shipmentServiceLevel
    ,c.shipmentServiceLevelCode
    ,c.shipmentCarrierCode
    ,c.shipmentCarrier = null ? 'UNASSIGNED' : c.shipmentCarrier shipmentCarrier
    ,c.inventoryShipmentStatus
    ,c.transportationMileStone
    ,c.shipmentPrimaryException
    ,c.shipmentBookedOnDateTime
    ,c.shipmentCanceledDateTime
    ,c.shipmentCanceledReason
    ,c.actualShipmentDateTime
    ,c.shipmentCreateOnDateTime
    ,c.originalScheduledDeliveryDateTime
    ,c.actualDeliveryDateTime
    ,c.warehouseId
    ,c.warehouseCode
    ,c.milestoneStatus
    ,c.inboundType
    ,c.estimatedDeliveryDateTime
    ,c.LoadID
    ,c.totalCharge
    ,c.totalChargeCurrency
    ,c.ISCLAIM isClaim
    ,c.deliveryStatus
    ,c.carrierShipmentNumber = null ? null : toString((select c.carrierShipmentNumber)) carrierShipmentNumber
    ,c.lastKnownLocation
    ,c.ServiceMode
    ,c.PickUpDate actualPickupDateTime
    ,c.DateTimeShipped shippedDateTime
    ,c.ScheduledPickUpDateTime scheduledPickupDateTime
    ,c.isTemperatureTracked
    ,c.ServiceLevel isShipmentServiceLevelResultSet
    ,c.latestTemperature
    ,c.temperatureDateTime
    ,c.temperatureCity
    ,c.temperatureState
    ,c.temperatureCountry
    ,c.IS_INBOUND isInbound
FROM c
WHERE c.AccountId = @DPProductLineKey
    AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
    AND c.FacilityId IN (@warehouseId)
    AND (
        c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
        )
    AND (
        c.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
        )
    AND (
        c.DateTimeShipped BETWEEN @Date.shippedStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shippedEndDate))
        )
    AND (
        c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
        )
    AND (INDEX_OF(@originLocation, ",") != - 1 ?concat(c.OriginCity, ',', c.OriginCountry) : c.OriginCountry) IN (@originLocation)
    AND (INDEX_OF(@destinationLocation, ",") != - 1 ?concat(c.DestinationCity, ',', c.DestinationCountry) : c.DestinationCountry) IN (@destinationLocation)
    AND (c.OriginCountry IN ('@originCountry'))
    AND (c.DestinationCountry IN ('@destinationCountry'))
    AND (c.OriginCity IN ('@originCity'))
    AND (c.DestinationCity IN ('@destinationCity'))   
    AND c.IS_INBOUND = 1
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    AND ((is_null(c.Carrier) ? 'NOMODE' : c.Carrier) IN (@CarrierTypeArray))
    AND ((is_null(c.ServiceMode) ? 'NOMODE' : c.ServiceMode) IN (@ShipmentModeArray))
    AND ((is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN (@ServiceLevelArray))
    AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = @isTemperatureTracked))
    AND (
    --  c.estimatedDeliveryDateTime BETWEEN '2021-12-20 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-01 00:00:00.000'))
    --  )
    -- AND (
    --  c.actualDeliveryDateTime BETWEEN '2021-12-20 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-01 00:00:00.000'))
    --  )
    -- AND (
    --  c.PickUpDate BETWEEN '2021-12-20 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-01 00:00:00.000'))
    --  )
    -- AND (
    --  c.shipmentBookedOnDateTime BETWEEN '2021-12-20 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-01 00:00:00.000'))
    --  )
    AND (c.milestoneStatus IN (@milestoneStatus))
    AND (c.milestoneStatus IN  ('DELIVERED','ASN CREATED','FTZ','RECEIVING','PUTAWAY')
         
            AND c.deliveryStatus = @deliveryStatus
             
        )
    AND (c.ISCLAIM = @isClaim)
    AND c.is_deleted = 0
ORDER BY c.shipmentCreateOnDateTime DESC

--Result Set 3

--* Parameter requirement info.
-- @DPProductLineKey is required,
-- @DPServiceLineKey is optional,
-- @warehouseId is optional ,
-- @Date.shipmentCreationStartDate and @Date.shipmentCreationEndDate are optional,
-- @Date.scheduleDeliveryStartDate and  @Date.scheduleDeliveryEndDate are optional,
-- @Date.shippedStartDate and @Date.shippedEndDate are optional,
-- @Date.scheduleToShipStartDate and  @Date.scheduleToShipEndDate are optional,
-- @originCountry and @DestinationCountry are optional,
-- @OriginCountry and @destinationCountry are optional,
-- @CarrierTypeArray  and @ShipmentModeArray are optional,
-- @Date.deliveryEtaStartDate and  @Date.deliveryEtaEndDate are optional,
-- @Date.actualDeliveryStartDate and  @Date.actualDeliveryEndDate are optional,
-- @Date.pickupStartDate and  @Date.pickupEndDate are optional,
-- @Date.bookedStartDate and  @Date.bookedEndDate are optional,
-- @isTemperatureTracked and @milestoneStatus are optional,
-- @deliveryStatus is optional
-- @isClaim is optional
-- @inboundType is optional

--* Target Container-digital_summary_orders

SELECT a.milestoneStatus AS MilestoneStatus
    ,COUNT(a.milestoneStatus) AS MilestoneStatusCount
FROM (
    SELECT DISTINCT c.shipmentNumber
        ,c.upsTransportShipmentNumber
        ,c.milestoneStatus
    FROM c
    WHERE c.AccountId = @DPProductLineKey
        AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
        AND c.FacilityId IN (@warehouseId)  
    AND (
        c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
        )
    AND (
        c.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
        )
    AND (
        c.DateTimeShipped BETWEEN @Date.shippedStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shippedEndDate))
        )
    AND (
        c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
        )      
        AND (INDEX_OF(@originLocation, ",") != - 1 ?concat(c.OriginCity, ',', c.OriginCountry) : c.OriginCountry) IN ('@originLocation')
        AND (INDEX_OF(@destinationLocation, ",") != - 1 ?concat(c.DestinationCity, ',', c.DestinationCountry) : c.DestinationCountry) IN ('@destinationLocation')     
        AND (c.OriginCountry IN ('@originCountry'))
        AND (c.DestinationCountry IN ('@destinationCountry'))
        AND (c.OriginCity IN ('@originCity'))
        AND (c.DestinationCity IN ('@destinationCity'))   
        AND c.IS_INBOUND = 1
        AND c.is_deleted = 0
        AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
        AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
        AND ((is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN (@CarrierTypeArray))
        AND ((is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN (@ShipmentModeArray))
        AND ((is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN (@ServiceLevelArray))
        AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = @isTemperatureTracked))
    ) a
GROUP BY a.milestoneStatus


--Result Set 4

--* Parameter requirement info.
-- @DPProductLineKey is required,
-- @DPServiceLineKey is optional,
-- @warehouseId is optional ,
-- @Date.shipmentCreationStartDate and @Date.shipmentCreationEndDate are optional,
-- @Date.scheduleDeliveryStartDate and  @Date.scheduleDeliveryEndDate are optional,
-- @Date.shippedStartDate and @Date.shippedEndDate are optional,
-- @Date.scheduleToShipStartDate and  @Date.scheduleToShipEndDate are optional,
-- @originCountry and @DestinationCountry are optional,
-- @OriginCountry and @destinationCountry are optional,
-- @CarrierTypeArray  and @ShipmentModeArray are optional,
-- @Date.deliveryEtaStartDate and  @Date.deliveryEtaEndDate are optional,
-- @Date.actualDeliveryStartDate and  @Date.actualDeliveryEndDate are optional,
-- @Date.pickupStartDate and  @Date.pickupEndDate are optional,
-- @Date.bookedStartDate and  @Date.bookedEndDate are optional,
-- @isTemperatureTracked and @milestoneStatus are optional,
-- @deliveryStatus is optional
-- @isClaim is optional
-- @inboundType is optional

--* Target Container-digital_summary_orders

SELECT a.deliveryStatus AS DeliveryStatus
    ,COUNT(1) AS DeliveryStatusCount
FROM (
    SELECT DISTINCT c.shipmentNumber
        ,c.upsTransportShipmentNumber
        ,c.deliveryStatus
    FROM c
WHERE c.AccountId = @DPProductLineKey
    AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
    AND c.FacilityId IN (@warehouseId)
    AND (
        c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
        )
    AND (
        c.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
        )
    AND (
        c.DateTimeShipped BETWEEN @Date.shippedStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shippedEndDate))
        )
    AND (
        c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
        )
     
    AND (INDEX_OF(@originLocation, ",") != - 1 ?concat(c.OriginCity, ',', c.OriginCountry) : c.OriginCountry) IN ('@originLocation')
    AND (INDEX_OF(@destinationLocation, ",") != - 1 ?concat(c.DestinationCity, ',', c.DestinationCountry) : c.DestinationCountry) IN ('@destinationLocation')  
    AND (c.OriginCountry IN ('@originCountry'))
    AND (c.DestinationCountry IN ('@destinationCountry'))
    AND (c.OriginCity IN ('@originCity'))
    AND (c.DestinationCity IN ('@destinationCity'))   
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    AND ((is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN (@CarrierTypeArray))
    AND ((is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN (@ShipmentModeArray))
    AND ((is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN (@ServiceLevelArray))
    AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = @isTemperatureTracked))
     
     
    AND c.milestoneStatus IN  ('DELIVERED','ASN CREATED','FTZ','RECEIVING','PUTAWAY')
) a
GROUP BY a.deliveryStatus

--Result Set 5

--* Parameter requirement info.
-- @DPProductLineKey is required,
-- @DPServiceLineKey is optional,
-- @warehouseId is optional ,
-- @Date.shipmentCreationStartDate and @Date.shipmentCreationEndDate are optional,
-- @Date.scheduleDeliveryStartDate and  @Date.scheduleDeliveryEndDate are optional,
-- @Date.shippedStartDate and @Date.shippedEndDate are optional,
-- @Date.scheduleToShipStartDate and  @Date.scheduleToShipEndDate are optional,
-- @originCountry and @DestinationCountry are optional,
-- @OriginCountry and @destinationCountry are optional,
-- @CarrierTypeArray  and @ShipmentModeArray are optional,
-- @Date.deliveryEtaStartDate and  @Date.deliveryEtaEndDate are optional,
-- @Date.actualDeliveryStartDate and  @Date.actualDeliveryEndDate are optional,
-- @Date.pickupStartDate and  @Date.pickupEndDate are optional,
-- @Date.bookedStartDate and  @Date.bookedEndDate are optional,
-- @isTemperatureTracked and @milestoneStatus are optional,
-- @deliveryStatus is optional
-- @isClaim is optional
-- @inboundType is optional

--* Target Container-digital_summary_orders

--CASE 1 IF @isShipmentServiceLevelResultSet ='Y'

SELECT a.shipmentServiceLevel
    ,count(1) shipmentServiceLevelCount
FROM (
    SELECT DISTINCT c.shipmentServiceLevel
        ,c.shipmentNumber
        ,c.upsTransportShipmentNumber
    FROM c
WHERE c.AccountId = @DPProductLineKey
     AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
    AND c.FacilityId IN (@warehouseId)  
    AND (
        c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
        )
    AND (
        c.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
        )
    AND (
        c.DateTimeShipped BETWEEN @Date.shippedStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shippedEndDate))
        )
    AND (
        c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
        )    
    AND (INDEX_OF("HEBRON,US", ",") != - 1 ?concat(c.OriginCity, ',', c.OriginCountry) : c.OriginCountry) IN ('@originLocation')
    AND (INDEX_OF("HEBRON,US", ",") != - 1 ?concat(c.DestinationCity, ',', c.DestinationCountry) : c.DestinationCountry) IN ('@destinationLocation')      
    AND (c.OriginCountry IN ('@originCountry'))
    AND (c.DestinationCountry IN ('@destinationCountry'))
    AND (c.OriginCity IN ('@originCity'))
    AND (c.DestinationCity IN ('@destinationCity'))   
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0  
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
    AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    AND ((is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN (@CarrierTypeArray))
    AND ((is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN (@ShipmentModeArray))
     
    AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = @isTemperatureTracked))) a   
GROUP BY a.shipmentServiceLevel

--CASE 2 IF @isShipmentServiceLevelResultSet !='Y'

 SELECT '' AS ShipmentServiceLevel, '' shipmentServiceLevelCount