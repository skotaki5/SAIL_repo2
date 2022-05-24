-- rpt_Outbound_Shipments

/*
Note :
if(@deliveryStatus = '' or @deliverStatus = null)

set @NULLDeliveryStatus = * 

For this condition in the cosmos queries, consider the following comments:

AND (
            INDEX_OF(@originLocation, ",") != - 1 ? CONCAT (        --Here @originLocation is  the  '|' separated  string  eg:  'MALDEN,US|NASHVILLE,US'
                UPPER(c.OriginCity)
                ,','
                ,UPPER(c.OriginCountry)
                ) :UPPER(c.OriginCountry)
            ) IN (@originLocation)                                 --Here @orginLocation is array of string eg: ('MALDEN,US','NASHVILLE,US')         
        AND (
            INDEX_OF(@destinationLocation, ",") != - 1 ? CONCAT (  --Here @destinationLocation is  the  '|' separated  string  eg:  'MALDEN,US|NASHVILLE,US'  
                UPPER(c.DestinationCity)
                ,','
                ,UPPER(c.DestinationCountry)
                ) :UPPER(c.DestinationCountry)
            ) IN (@destinationLocation)                           --@destinationLocation is array of string eg: ('MALDEN,US','NASHVILLE,US')
*/

-- Result set 1
/*

*/

--IF @deliveryStatus = '' or @deliveryStatus = null
--paramter required info

-- @DPProductLineKey Required	UPPER
-- @DPServiceLineKey	Optional	UPPER
-- @AccountKeys   Required	UPPER
-- @warehouseId	Optional	UPPER
-- @Date.shipmentCreationStartDate 	Optional	
-- @Date.shipmentCreationEndDate	Optional	
-- @Date.scheduleDeliveryStartDate	Optional	
-- @Date.scheduleDeliveryEndDate	Optional	
-- @Date.shippedStartDate	Optional	
-- @Date.shippedEndDate	Optional	
-- @Date.scheduleToShipStartDate and 	Optional	
-- @Date.scheduleToShipEndDate	Optional	
-- @originCountry	Optional	UPPER
-- @DestinationCountry	Optional	UPPER
-- @OriginCountry	Optional	UPPER
-- @destinationCountry	Optional	UPPER
-- @originLocation Optional	UPPER
-- @destinationLocation Optional	UPPER
-- @CarrierTypeArray 	Optional	CASE WHEN item in the array ='NULL' THEN 'UNASSIGNED' ELSE item 
-- @ShipmentModeArray	Optional	CASE WHEN item int he array ='NULL' THEN 'UNASSIGNED' ELSE item 
-- @ServiceLevelArray	Optional	
-- @Date.bookedStartDate Optional	@Date.bookedEndDate Optional	
-- @Date.pickupStartDate Optional	@Date.pickupEndDate Optional	
-- @Date.deliveryEtaStartDate	Optional	@Date.deliveryEtaEndDate	Optional	
-- @Date.actualDeliveryStartDate 	Optional @Date.actualDeliveryEndDate	Optional	
-- @isTemperatureTracked 	Optional	UPPER
-- @milestoneStatus	Optional	UPPER
-- @deliveryStatus	Optional	UPPER
-- @isClaim	Optional	
-- @OrderType Optional	UPPER
-- @isManaged Optional	UPPER 
-- @temperatureThreshold UPPER --Sprint 52 Changes 

SELECT count(1) totalShipments
FROM (
    SELECT DISTINCT c.shipmentNumber
        ,c.upsTransportShipmentNumber
    FROM c
    WHERE (
            c.AccountId IN (@AccountKeys.DPProductLineKey)
            OR c.AccountId =@DPProductLineKey
            )
        AND c.IS_INBOUND = 0
        AND (c.OrderCancelledFlag = null ? 'N' : c.OrderCancelledFlag) = 'N'
        AND c.FacilityId IN (@warehouseId)
        AND (
            c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
            )
        AND (
            c.originalScheduledDeliveryDateTime BETWEEN @Date.scheduleDeliveryStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND (
            c.DateTimeShipped  BETWEEN @Date.shippedStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shippedEndDate))
            )
        AND (
            c.ScheduledPickUpDateTime  BETWEEN @Date.scheduleToShipStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            ) 
        AND (UPPER(c.OriginCountry) IN (@originCountry))
        AND (UPPER(c.DestinationCountry) IN (@destinationCountry))
        AND (UPPER(c.OriginCity) IN (@originCity))
        AND (UPPER(c.DestinationCity) IN (@destinationCity))
        AND ((is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN (@ShipmentModeArray))
        AND ((is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN (@CarrierTypeArray))
        AND (
            c.estimatedDeliveryDateTime  BETWEEN @Date.deliveryEtaStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.deliveryEtaEndDate))
            )
            AND (
            c.actualDeliveryDateTime  BETWEEN @Date.actualDeliveryStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
            )
        AND (
            INDEX_OF(@originLocation, ",") != - 1 ? CONCAT (
                UPPER(c.OriginCity)
                ,','
                ,UPPER(c.OriginCountry)
                ) :UPPER(c.OriginCountry)
            ) IN (@originLocation)
        AND (
            INDEX_OF(@destinationLocation, ",") != - 1 ? CONCAT (
                UPPER(c.DestinationCity)
                ,','
                ,UPPER(c.DestinationCountry)
                ) :UPPER(c.DestinationCountry)
            ) IN (@destinationLocation)
        AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = @isTemperatureTracked))
        AND (
            c.shipmentBookedOnDateTime BETWEEN @Date.bookedStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.bookedEndDate))
            )
        AND (UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes 
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = c.is_managed
        --additional filters for DIGITAL_SUMMARY_ORDERS_FILTER
        AND (
            c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR c.DP_SERVICELINE_KEY = @DPServiceLineKey
            )
        AND ((is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN (@ServiceLevelArray))
        AND (
            c.PickUpDate BETWEEN @Date.pickupStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.pickupEndDate))
            )
		AND UPPER(c.TemperatureThreshold) IN (@temperatureThreshold) --Sprint 52 Changes
		
        --additional filters for DIGITAL_SUMMARY_ORDERS
        AND UPPER(c.milestoneStatus) IN (@milestoneStatus)
        AND UPPER(c.deliveryStatus) IN (@deliveryStatus)
        --additional filters
        AND c.ISCLAIM = @isClaim
        AND c.is_deleted = 0
    ) a  
 
--else if ( @milestoneStatus like '%DELIVERED %'and @deliveryStatus != '*')
 
SELECT count(1) totalShipments
FROM (
    SELECT DISTINCT c.shipmentNumber
        ,c.upsTransportShipmentNumber
    FROM c
    WHERE (
            c.AccountId IN (@AccountKeys.DPProductLineKey)
            OR c.AccountId =@DPProductLineKey
            )
        AND c.IS_INBOUND = 0
        AND (c.OrderCancelledFlag = null ? 'N' : c.OrderCancelledFlag) = 'N'
        AND c.FacilityId IN (@warehouseId)
        AND (
            c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
            )
        AND (
            c.originalScheduledDeliveryDateTime BETWEEN @Date.scheduleDeliveryStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND (
            c.DateTimeShipped  BETWEEN @Date.shippedStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shippedEndDate))
            )
        AND (
            c.ScheduledPickUpDateTime  BETWEEN @Date.scheduleToShipStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (UPPER(c.OriginCountry) IN (@originCountry))
        AND (UPPER(c.DestinationCountry) IN (@destinationCountry))
        AND (UPPER(c.OriginCity) IN (@originCity))
        AND (UPPER(c.DestinationCity) IN (@destinationCity))
        AND ((is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN (@ShipmentModeArray))
        AND ((is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN (@CarrierTypeArray))
        AND (
            c.estimatedDeliveryDateTime  BETWEEN @Date.deliveryEtaStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.deliveryEtaEndDate))
            )
            AND (
            c.actualDeliveryDateTime  BETWEEN @Date.actualDeliveryStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
            )
        AND (
            INDEX_OF(@originLocation, ",") != - 1 ? CONCAT (
                UPPER(c.OriginCity)
                ,','
                ,UPPER(c.OriginCountry)
                ) :UPPER(c.OriginCountry)
            ) IN (@originLocation)
        AND (
            INDEX_OF(@destinationLocation, ",") != - 1 ? CONCAT (
                UPPER(c.DestinationCity)
                ,','
                ,UPPER(c.DestinationCountry)
                ) :UPPER(c.DestinationCountry)
            ) IN (@destinationLocation)
        AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = @isTemperatureTracked))
        AND (
            c.shipmentBookedOnDateTime BETWEEN @Date.bookedStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.bookedEndDate))
            )
        AND (UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes 
        and (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = c.is_managed
        --additional filters for DIGITAL_SUMMARY_ORDERS_FILTER
        AND (
            c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR c.DP_SERVICELINE_KEY = @DPServiceLineKey
            )
        AND ((is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN (@ServiceLevelArray))
        AND (
            c.PickUpDate BETWEEN @Date.pickupStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.pickupEndDate))
            )
		AND UPPER(c.TemperatureThreshold) IN (@temperatureThreshold)--Sprint 52 Changes
        --additional filters for DIGITAL_SUMMARY_ORDERS
        AND UPPER(c.milestoneStatus) IN (@milestoneStatus)
        AND UPPER(c.deliveryStatus) IN (@deliveryStatus)
        --additional filters
        AND (
            (
                UPPER(c.milestoneStatus) IN (@milestoneStatus)
                AND UPPER(c.milestoneStatus) != 'DELIVERED'
                )
            OR (
                UPPER(c.deliveryStatus) IN (@deliveryStatus)
                AND UPPER(c.deliveryStatus) != 'DELIVERED'
                )
            )
        AND c.ISCLAIM = @isClaim
        AND c.is_deleted = 0
    ) a
 
--else
 
SELECT count(1) totalShipments
FROM (
    SELECT DISTINCT c.shipmentNumber
        ,c.upsTransportShipmentNumber
    FROM c
    WHERE (
            c.AccountId IN (@AccountKeys.DPProductLineKey)
            OR c.AccountId =@DPProductLineKey
            )
        AND c.IS_INBOUND = 0
        AND (c.OrderCancelledFlag = null ? 'N' : c.OrderCancelledFlag) = 'N'
        AND c.FacilityId IN (@warehouseId)
        AND (
            c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
            )
        AND (
            c.originalScheduledDeliveryDateTime BETWEEN @Date.scheduleDeliveryStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND (
            c.DateTimeShipped  BETWEEN @Date.shippedStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shippedEndDate))
            )
        AND (
            c.ScheduledPickUpDateTime  BETWEEN @Date.scheduleToShipStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (UPPER(c.OriginCountry) IN (@originCountry))
        AND (UPPER(c.DestinationCountry) IN (@destinationCountry))
        AND (UPPER(c.OriginCity) IN (@originCity))
        AND (UPPER(c.DestinationCity) IN (@destinationCity))
        AND ((is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN (@ShipmentModeArray))
        AND ((is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN (@CarrierTypeArray))
        AND (
            c.estimatedDeliveryDateTime  BETWEEN @Date.deliveryEtaStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.deliveryEtaEndDate))
            )
            AND (
            c.actualDeliveryDateTime  BETWEEN @Date.actualDeliveryStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
            )
        AND (
            INDEX_OF(@originLocation, ",") != - 1 ? CONCAT (
                UPPER(c.OriginCity)
                ,','
                ,UPPER(c.OriginCountry)
                ) :UPPER(c.OriginCountry)
            ) IN (@originLocation)
        AND (
            INDEX_OF(@destinationLocation, ",") != - 1 ? CONCAT (
                UPPER(c.DestinationCity)
                ,','
                ,UPPER(c.DestinationCountry)
                ) :UPPER(c.DestinationCountry)
            ) IN (@destinationLocation)
        AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = @isTemperatureTracked))
        AND (
            c.shipmentBookedOnDateTime BETWEEN @Date.bookedStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.bookedEndDate))
            )
        AND (UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes 
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = c.is_managed
        --additional filters for DIGITAL_SUMMARY_ORDERS_FILTER
        AND (
            c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR c.DP_SERVICELINE_KEY = @DPServiceLineKey
            )
        AND ((is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN (@ServiceLevelArray))
        AND (
            c.PickUpDate BETWEEN @Date.pickupStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.pickupEndDate))
            )
		AND UPPER(c.TemperatureThreshold) IN (@temperatureThreshold)--Sprint 52 Changes
        --additional filters for DIGITAL_SUMMARY_ORDERS
        AND UPPER(c.milestoneStatus) IN (@milestoneStatus)
        AND UPPER(c.deliveryStatus) IN (@deliveryStatus)
        --additional filters
        AND (
            UPPER(c.milestoneStatus) IN (@milestoneStatus)
            OR UPPER(c.deliveryStatus) IN (@deliveryStatus)
            )
        AND c.ISCLAIM = @isClaim
        AND c.is_deleted = 0
    ) a

-- Result set 2

--IF @topRow = 0  
 
 --paramter required info

-- @DPProductLineKey Required	UPPER
-- @DPServiceLineKey	Optional	UPPER
-- @AccountKeys   Required	UPPER
-- @warehouseId	Optional	UPPER
-- @Date.shipmentCreationStartDate 	Optional	
-- @Date.shipmentCreationEndDate	Optional	
-- @Date.scheduleDeliveryStartDate	Optional	
-- @Date.scheduleDeliveryEndDate	Optional	
-- @Date.shippedStartDate	Optional	
-- @Date.shippedEndDate	Optional	
-- @Date.scheduleToShipStartDate and 	Optional	
-- @Date.scheduleToShipEndDate	Optional	
-- @originCountry	Optional	UPPER
-- @DestinationCountry	Optional	UPPER
-- @OriginCountry	Optional	UPPER
-- @destinationCountry	Optional	UPPER
-- @originLocation Optional	UPPER
-- @destinationLocation Optional	UPPER
-- @CarrierTypeArray 	Optional	CASE WHEN item in the array ='NULL' THEN 'UNASSIGNED' ELSE item 
-- @ShipmentModeArray	Optional	CASE WHEN item int he array ='NULL' THEN 'UNASSIGNED' ELSE item 
-- @ServiceLevelArray	Optional	
-- @Date.bookedStartDate Optional	@Date.bookedEndDate Optional	
-- @Date.pickupStartDate Optional	@Date.pickupEndDate Optional	
-- @Date.deliveryEtaStartDate	Optional	@Date.deliveryEtaEndDate	Optional	
-- @Date.actualDeliveryStartDate 	Optional @Date.actualDeliveryEndDate	Optional	
-- @isTemperatureTracked 	Optional	UPPER
-- @milestoneStatus	Optional	UPPER
-- @deliveryStatus	Optional	UPPER
-- @isClaim	Optional	
-- @OrderType Optional	UPPER
-- @isManaged Optional	UPPER 
-- @temperatureThreshold UPPER--Sprint 52 Changes

SELECT DISTINCT c.shipmentNumber
    ,c.referenceNumber
    ,c.upsShipmentNumber
    ,c.clientShipmentNumber
    ,c.customerPONumber
    ,c.shipmentNumber orderNumber
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
    ,c.shipmentService = null ? 'UNASSIGNED' : c.shipmentService shipmentService
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
    ,c.estimatedDeliveryDateTime --Sprint 52 Changes
    ,c.referenceNumber1
    ,c.referenceNumber2
    ,c.referenceNumber3
    ,c.referenceNumber4
    ,c.referenceNumber5
    ,c.shipmentDestination_locationCode
    ,c.shipmentCreateOnDateTimeZone
    ,c.originalScheduledDeliveryDateTimeZone
    ,c.shippedDateTime
    ,c.shippedDateTimeZone
    ,c.dpProductLineKey
    ,c.Accountnumber accountNumber
    ,c.deliveryStatus
    ,c.LoadID
    ,c.isTemperatureTracked
    ,c.isShipmentServiceLevelResultSet
    ,c.latestTemperature
    ,c.latestTemperatureInCelsius
    ,c.latestTemperatureInFahrenheit
    ,c.temperatureDateTime
    ,c.temperatureCity
    ,c.temperatureState
    ,c.temperatureCountry
    ,c.totalCharge
    ,c.totalChargeCurrency
    ,c.ISCLAIM isClaim
    ,c.lastKnownLocation
    ,c.IS_INBOUND isInbound
    ,'{"carrierShipmentNumber": ["Click For Details"]}' carrierShipmentNumber
    ,c.PickUpDate actualPickupDateTime
    ,c.ScheduledPickUpDateTime --Sprint 52 Changes
    ,c.shipmentType
    ,c.TemperatureThreshold AS temperatureThreshold --Sprint 52 Changes
FROM c
WHERE (
        c.AccountId IN (@AccountKeys.DPProductLineKey)
        OR c.AccountId =@DPProductLineKey
        )
    AND c.IS_INBOUND = 0
    AND (c.OrderCancelledFlag = null ? 'N' : c.OrderCancelledFlag) = 'N'
    AND c.FacilityId IN (@warehouseId)
    AND (
        c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
        )
    AND (
        c.originalScheduledDeliveryDateTime BETWEEN @Date.scheduleDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
        )
    AND (
        c.DateTimeShipped  BETWEEN @Date.shippedStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shippedEndDate))
        )
    AND (
        c.ScheduledPickUpDateTime  BETWEEN @Date.scheduleToShipStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
        )
    AND (UPPER(c.OriginCountry) IN (@originCountry))
    AND (UPPER(c.DestinationCountry) IN (@destinationCountry))
    AND (UPPER(c.OriginCity) IN (@originCity))
    AND (UPPER(c.DestinationCity) IN (@destinationCity))
    AND ((is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN (@ShipmentModeArray))
    AND ((is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN (@CarrierTypeArray))
    AND (
        c.estimatedDeliveryDateTime  BETWEEN @Date.deliveryEtaStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.deliveryEtaEndDate))
        )
    AND (
        c.actualDeliveryDateTime  BETWEEN @Date.actualDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        )
    AND (
        INDEX_OF(@originLocation, ",") != - 1 ? CONCAT (
            UPPER(c.OriginCity)
            ,','
            ,UPPER(c.OriginCountry)
            ) :UPPER(c.OriginCountry)
        ) IN (@originLocation)
    AND (
        INDEX_OF(@destinationLocation, ",") != - 1 ? CONCAT (
            UPPER(c.DestinationCity)
            ,','
            ,UPPER(c.DestinationCountry)
            ) :UPPER(c.DestinationCountry)
        ) IN (@destinationLocation)
    AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = @isTemperatureTracked))
    AND (
        c.shipmentBookedOnDateTime BETWEEN @Date.bookedStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.bookedEndDate))
        )
    AND (UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes 
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = c.is_managed
    --additional filters for DIGITAL_SUMMARY_ORDERS_FILTER
    AND (
        c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
        OR c.DP_SERVICELINE_KEY = @DPServiceLineKey
        )
    AND ((is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN (@ServiceLevelArray))
    AND (
        c.PickUpDate BETWEEN @Date.pickupStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.pickupEndDate))
        )
	AND UPPER(c.TemperatureThreshold) IN (@temperatureThreshold)--Sprint 52 Changes
    --additional filters for DIGITAL_SUMMARY_ORDERS
    AND UPPER(c.milestoneStatus) IN (@milestoneStatus)
    AND UPPER(c.deliveryStatus) IN (@deliveryStatus)
    --additional filters
    AND (
        (
            (UPPER(c.milestoneStatus) IN (@milestoneStatus))
            AND @NULLDeliveryStatus = '*'
            )
        OR (
            (
                (
                    UPPER(c.milestoneStatus) IN (@milestoneStatus)
                    AND 'DELIVERED' NOT IN (@milestoneStatus)
                    )
                )
            OR UPPER(c.deliveryStatus) IN (@deliveryStatus)
            )
        OR (
            (
                (
                    UPPER(c.milestoneStatus) IN (@milestoneStatus)
                    AND UPPER(c.milestoneStatus) != 'DELIVERED'
                    )
                )
            OR (
                UPPER(c.deliveryStatus) IN (@deliveryStatus)
                AND UPPER(c.milestoneStatus) = 'DELIVERED'
                )
            )
        )
    AND c.ISCLAIM = @isClaim
    AND c.is_deleted = 0
 
  --IF @topRow > 0  
 
SELECT DISTINCT top @topRow c.shipmentNumber
    ,c.referenceNumber
    ,c.upsShipmentNumber
    ,c.clientShipmentNumber
    ,c.customerPONumber
    ,c.shipmentNumber orderNumber
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
    ,c.shipmentService = null ? 'UNASSIGNED' : c.shipmentService shipmentService
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
    ,c.estimatedDeliveryDateTime --Sprint 52 Changes
    ,c.referenceNumber1
    ,c.referenceNumber2
    ,c.referenceNumber3
    ,c.referenceNumber4
    ,c.referenceNumber5
    ,c.shipmentDestination_locationCode
    ,c.shipmentCreateOnDateTimeZone
    ,c.originalScheduledDeliveryDateTimeZone
    ,c.shippedDateTime
    ,c.shippedDateTimeZone
    ,c.dpProductLineKey
    ,c.Accountnumber accountNumber
    ,c.deliveryStatus
    ,c.LoadID
    ,c.isTemperatureTracked
    ,c.isShipmentServiceLevelResultSet
    ,c.latestTemperature
    ,c.latestTemperatureInCelsius
    ,c.latestTemperatureInFahrenheit
    ,c.temperatureDateTime
    ,c.temperatureCity
    ,c.temperatureState
    ,c.temperatureCountry
    ,c.totalCharge
    ,c.totalChargeCurrency
    ,c.ISCLAIM isClaim
    ,c.lastKnownLocation
    ,c.IS_INBOUND isInbound
    ,'{"carrierShipmentNumber": ["Click For Details"]}' carrierShipmentNumber
    ,c.PickUpDate actualPickupDateTime
    ,c.ScheduledPickUpDateTime --Sprint 52 Changes
    ,c.shipmentType
    ,c.TemperatureThreshold AS temperatureThreshold--Sprint 52 Changes
FROM c
WHERE (
        c.AccountId IN (@AccountKeys.DPProductLineKey)
        OR c.AccountId =@DPProductLineKey
        )
    AND c.IS_INBOUND = 0
    AND (c.OrderCancelledFlag = null ? 'N' : c.OrderCancelledFlag) = 'N'
    AND c.FacilityId IN (@warehouseId)
    AND (
        c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
        )
    AND (
        c.originalScheduledDeliveryDateTime BETWEEN @Date.scheduleDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
        )
    AND (
        c.DateTimeShipped  BETWEEN @Date.shippedStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shippedEndDate))
        )
        AND (
            c.ScheduledPickUpDateTime  BETWEEN @Date.scheduleToShipStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
    AND (UPPER(c.OriginCountry) IN (@originCountry))
    AND (UPPER(c.DestinationCountry) IN (@destinationCountry))
    AND (UPPER(c.OriginCity) IN (@originCity))
    AND (UPPER(c.DestinationCity) IN (@destinationCity))
    AND ((is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN (@ShipmentModeArray))
    AND ((is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN (@CarrierTypeArray))
    AND (
        c.estimatedDeliveryDateTime  BETWEEN @Date.deliveryEtaStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.deliveryEtaEndDate))
        )
    AND (
        c.actualDeliveryDateTime  BETWEEN @Date.actualDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        )
    AND (
        INDEX_OF(@originLocation, ",") != - 1 ? CONCAT (
            UPPER(c.OriginCity)
            ,','
            ,UPPER(c.OriginCountry)
            ) :UPPER(c.OriginCountry)
        ) IN (@originLocation)
    AND (
        INDEX_OF(@destinationLocation, ",") != - 1 ? CONCAT (
            UPPER(c.DestinationCity)
            ,','
            ,UPPER(c.DestinationCountry)
            ) :UPPER(c.DestinationCountry)
        ) IN (@destinationLocation)
    AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = @isTemperatureTracked))
    AND (
        c.shipmentBookedOnDateTime BETWEEN @Date.bookedStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.bookedEndDate))
        )
    AND (UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes 
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = c.is_managed
    --additional filters for DIGITAL_SUMMARY_ORDERS_FILTER
    AND (
        c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
        OR c.DP_SERVICELINE_KEY = @DPServiceLineKey
        )
    AND ((is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN (@ServiceLevelArray))
    AND (
        c.PickUpDate BETWEEN @Date.pickupStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.pickupEndDate))
        )
	AND UPPER(c.TemperatureThreshold) IN (@temperatureThreshold)--Sprint 52 Changes
    --additional filters for DIGITAL_SUMMARY_ORDERS
    AND UPPER(c.milestoneStatus) IN (@milestoneStatus)
    AND UPPER(c.deliveryStatus) IN (@deliveryStatus)
    --additional filters
    AND (
        (
            (UPPER(c.milestoneStatus) IN (@milestoneStatus))
            AND @NULLDeliveryStatus = '*'
            )
        OR (
            (
                (
                    UPPER(c.milestoneStatus) IN (@milestoneStatus)
                    AND 'DELIVERED' NOT IN (@milestoneStatus)
                    )
                )
            OR UPPER(c.deliveryStatus) IN (@deliveryStatus)
            )
        OR (
            (
                (
                    UPPER(c.milestoneStatus) IN (@milestoneStatus)
                    AND UPPER(c.milestoneStatus) != 'DELIVERED'
                    )
                )
            OR (
                UPPER(c.deliveryStatus) IN (@deliveryStatus)
                AND UPPER(c.milestoneStatus) = 'DELIVERED'
                )
            )
        )
    AND c.ISCLAIM = @isClaim
    AND c.is_deleted = 0


-- Result set 3

--paramter required info

-- @DPProductLineKey Required	UPPER
-- @DPServiceLineKey	Optional	UPPER
-- @AccountKeys   Required	UPPER
-- @warehouseId	Optional	UPPER
-- @Date.shipmentCreationStartDate 	Optional	
-- @Date.shipmentCreationEndDate	Optional	
-- @Date.scheduleDeliveryStartDate	Optional	
-- @Date.scheduleDeliveryEndDate	Optional	
-- @Date.shippedStartDate	Optional	
-- @Date.shippedEndDate	Optional	
-- @Date.scheduleToShipStartDate and 	Optional	
-- @Date.scheduleToShipEndDate	Optional	
-- @originCountry	Optional	UPPER
-- @DestinationCountry	Optional	UPPER
-- @OriginCountry	Optional	UPPER
-- @destinationCountry	Optional	UPPER
-- @originLocation Optional	UPPER
-- @destinationLocation Optional	UPPER
-- @CarrierTypeArray 	Optional	CASE WHEN item in the array ='NULL' THEN 'UNASSIGNED' ELSE item 
-- @ShipmentModeArray	Optional	CASE WHEN item int he array ='NULL' THEN 'UNASSIGNED' ELSE item 
-- @ServiceLevelArray	Optional	
-- @Date.bookedStartDate Optional	@Date.bookedEndDate Optional	
-- @Date.pickupStartDate Optional	@Date.pickupEndDate Optional	
-- @Date.deliveryEtaStartDate	Optional	@Date.deliveryEtaEndDate	Optional	
-- @Date.actualDeliveryStartDate 	Optional @Date.actualDeliveryEndDate	Optional	
-- @isTemperatureTracked 	Optional	UPPER
-- @milestoneStatus	Optional	UPPER
-- @deliveryStatus	Optional	UPPER
-- @isClaim	Optional	
-- @OrderType Optional	UPPER
-- @isManaged Optional	UPPER 
-- @temperatureThreshold UPPER--Sprint 52 Changes

SELECT a.milestoneStatus MilestoneStatus
    ,count(1) MilestoneStatusCount
FROM (
    SELECT DISTINCT c.milestoneStatus
        ,c.shipmentNumber
        ,c.upsTransportShipmentNumber
    FROM c
    WHERE (
            c.AccountId IN (@AccountKeys.DPProductLineKey)
            OR c.AccountId =@DPProductLineKey
            )
        AND c.IS_INBOUND = 0
        AND (c.OrderCancelledFlag = null ? 'N' : c.OrderCancelledFlag) = 'N'
        AND c.FacilityId IN (@warehouseId)
        AND (
            c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
            )
        AND (
            c.originalScheduledDeliveryDateTime BETWEEN @Date.scheduleDeliveryStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND (
            c.DateTimeShipped  BETWEEN @Date.shippedStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shippedEndDate))
            )
        AND (
            c.ScheduledPickUpDateTime  BETWEEN @Date.scheduleToShipStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (UPPER(c.OriginCountry) IN (@originCountry))
        AND (UPPER(c.DestinationCountry) IN (@destinationCountry))
        AND (UPPER(c.OriginCity) IN (@originCity))
        AND (UPPER(c.DestinationCity) IN (@destinationCity))
        AND ((is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN (@ShipmentModeArray))
        AND ((is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN (@CarrierTypeArray))
        AND (
            c.estimatedDeliveryDateTime  BETWEEN @Date.deliveryEtaStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.deliveryEtaEndDate))
            )
            AND (
            c.actualDeliveryDateTime  BETWEEN @Date.actualDeliveryStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
            )
        AND (
            INDEX_OF(@originLocation, ",") != - 1 ? CONCAT (
                UPPER(c.OriginCity)
                ,','
                ,UPPER(c.OriginCountry)
                ) :UPPER(c.OriginCountry)
            ) IN (@originLocation)
        AND (
            INDEX_OF(@destinationLocation, ",") != - 1 ? CONCAT (
                UPPER(c.DestinationCity)
                ,','
                ,UPPER(c.DestinationCountry)
                ) :UPPER(c.DestinationCountry)
            ) IN (@destinationLocation)
        AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = @isTemperatureTracked))
        AND (
            c.shipmentBookedOnDateTime BETWEEN @Date.bookedStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.bookedEndDate))
            )
        AND (UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes 
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = c.is_managed
        --additional filters for DIGITAL_SUMMARY_ORDERS_FILTER
        AND (
            c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR c.DP_SERVICELINE_KEY = @DPServiceLineKey
            )
        AND ((is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN (@ServiceLevelArray))
        AND (
            c.PickUpDate BETWEEN @Date.pickupStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.pickupEndDate))
            )
		AND UPPER(c.TemperatureThreshold) IN (@temperatureThreshold)--Sprint 52 Changes
        --additional filters
        AND c.is_deleted = 0
    ) a
GROUP BY a.milestoneStatus

-- Result set 4

--paramter required info

-- @DPProductLineKey Required	UPPER
-- @DPServiceLineKey	Optional	UPPER
-- @AccountKeys   Required	UPPER
-- @warehouseId	Optional	UPPER
-- @Date.shipmentCreationStartDate 	Optional	
-- @Date.shipmentCreationEndDate	Optional	
-- @Date.scheduleDeliveryStartDate	Optional	
-- @Date.scheduleDeliveryEndDate	Optional	
-- @Date.shippedStartDate	Optional	
-- @Date.shippedEndDate	Optional	
-- @Date.scheduleToShipStartDate and 	Optional	
-- @Date.scheduleToShipEndDate	Optional	
-- @originCountry	Optional	UPPER
-- @DestinationCountry	Optional	UPPER
-- @OriginCountry	Optional	UPPER
-- @destinationCountry	Optional	UPPER
-- @originLocation Optional	UPPER
-- @destinationLocation Optional	UPPER
-- @CarrierTypeArray 	Optional	CASE WHEN item in the array ='NULL' THEN 'UNASSIGNED' ELSE item 
-- @ShipmentModeArray	Optional	CASE WHEN item int he array ='NULL' THEN 'UNASSIGNED' ELSE item 
-- @ServiceLevelArray	Optional	
-- @Date.bookedStartDate Optional	@Date.bookedEndDate Optional	
-- @Date.pickupStartDate Optional	@Date.pickupEndDate Optional	
-- @Date.deliveryEtaStartDate	Optional	@Date.deliveryEtaEndDate	Optional	
-- @Date.actualDeliveryStartDate 	Optional @Date.actualDeliveryEndDate	Optional	
-- @isTemperatureTracked 	Optional	UPPER
-- @milestoneStatus	Optional	UPPER
-- @deliveryStatus	Optional	UPPER
-- @isClaim	Optional	
-- @OrderType Optional	UPPER
-- @isManaged Optional	UPPER 
-- @temperatureThreshold UPPER--Sprint 52 Changes

SELECT a.deliveryStatus DeliveryStatus
    ,count(1) DeliveryStatusCount
FROM (
    SELECT DISTINCT c.deliveryStatus
        ,c.shipmentNumber
        ,c.upsTransportShipmentNumber
    FROM c
    WHERE (
            c.AccountId IN (@AccountKeys.DPProductLineKey)
            OR c.AccountId =@DPProductLineKey
            )
        AND c.IS_INBOUND = 0
        AND (c.OrderCancelledFlag = null ? 'N' : c.OrderCancelledFlag) = 'N'
        AND c.FacilityId IN (@warehouseId)
        AND (
            c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
            )
        AND (
            c.originalScheduledDeliveryDateTime BETWEEN @Date.scheduleDeliveryStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND (
            c.DateTimeShipped  BETWEEN @Date.shippedStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shippedEndDate))
            )
        AND (
            c.ScheduledPickUpDateTime  BETWEEN @Date.scheduleToShipStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            ) 
        AND (UPPER(c.OriginCountry) IN (@originCountry))
        AND (UPPER(c.DestinationCountry) IN (@destinationCountry))
        AND (UPPER(c.OriginCity) IN (@originCity))
        AND (UPPER(c.DestinationCity) IN (@destinationCity))
        AND ((is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN (@ShipmentModeArray))
        AND ((is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN (@CarrierTypeArray))
        AND (
            c.estimatedDeliveryDateTime  BETWEEN @Date.deliveryEtaStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.deliveryEtaEndDate))
            )
            AND (
            c.actualDeliveryDateTime  BETWEEN @Date.actualDeliveryStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
            )
        AND (
            INDEX_OF(@originLocation, ",") != - 1 ? CONCAT (
                UPPER(c.OriginCity)
                ,','
                ,UPPER(c.OriginCountry)
                ) :UPPER(c.OriginCountry)
            ) IN (@originLocation)
        AND (
            INDEX_OF(@destinationLocation, ",") != - 1 ? CONCAT (
                UPPER(c.DestinationCity)
                ,','
                ,UPPER(c.DestinationCountry)
                ) :UPPER(c.DestinationCountry)
            ) IN (@destinationLocation)
        AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = @isTemperatureTracked))
        AND (
            c.shipmentBookedOnDateTime BETWEEN @Date.bookedStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.bookedEndDate))
            )
        AND (UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN (@OrderType))--Sprint 52 Changes 
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = c.is_managed
        --additional filters for DIGITAL_SUMMARY_ORDERS_FILTER
        AND (
            c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR c.DP_SERVICELINE_KEY = @DPServiceLineKey
            )
        AND ((is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN (@ServiceLevelArray))
        AND (
            c.PickUpDate BETWEEN @Date.pickupStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.pickupEndDate))
            )
		AND UPPER(c.TemperatureThreshold) IN (@temperatureThreshold)--Sprint 52 Changes
        --additional filters
        AND c.milestoneStatus = 'DELIVERED'
        AND c.is_deleted = 0
    ) a
GROUP BY a.deliveryStatus


-- Result set 5

--  IF @isShipmentServiceLevelResultSet  'Y'

--paramter required info

-- @DPProductLineKey Required	UPPER
-- @DPServiceLineKey	Optional	UPPER
-- @AccountKeys   Required	UPPER
-- @warehouseId	Optional	UPPER
-- @Date.shipmentCreationStartDate 	Optional	
-- @Date.shipmentCreationEndDate	Optional	
-- @Date.scheduleDeliveryStartDate	Optional	
-- @Date.scheduleDeliveryEndDate	Optional	
-- @Date.shippedStartDate	Optional	
-- @Date.shippedEndDate	Optional	
-- @Date.scheduleToShipStartDate and 	Optional	
-- @Date.scheduleToShipEndDate	Optional	
-- @originCountry	Optional	UPPER
-- @DestinationCountry	Optional	UPPER
-- @OriginCountry	Optional	UPPER
-- @destinationCountry	Optional	UPPER
-- @originLocation Optional	UPPER
-- @destinationLocation Optional	UPPER
-- @CarrierTypeArray 	Optional	CASE WHEN item in the array ='NULL' THEN 'UNASSIGNED' ELSE item 
-- @ShipmentModeArray	Optional	CASE WHEN item int he array ='NULL' THEN 'UNASSIGNED' ELSE item 
-- @ServiceLevelArray	Optional	
-- @Date.bookedStartDate Optional	@Date.bookedEndDate Optional	
-- @Date.pickupStartDate Optional	@Date.pickupEndDate Optional	
-- @Date.deliveryEtaStartDate	Optional	@Date.deliveryEtaEndDate	Optional	
-- @Date.actualDeliveryStartDate 	Optional @Date.actualDeliveryEndDate	Optional	
-- @isTemperatureTracked 	Optional	UPPER
-- @milestoneStatus	Optional	UPPER
-- @deliveryStatus	Optional	UPPER
-- @isClaim	Optional	
-- @OrderType Optional	UPPER
-- @isManaged Optional	UPPER 
-- @temperatureThreshold UPPER--Sprint 52 Changes
 
SELECT a.shipmentServiceLevel
    ,count(1) shipmentServiceLevelCount
FROM (
    SELECT DISTINCT c.shipmentServiceLevel
        ,c.shipmentNumber
        ,c.upsTransportShipmentNumber
    FROM c
    WHERE (
            c.AccountId IN (@AccountKeys.DPProductLineKey)
            OR c.AccountId =@DPProductLineKey
            )
        AND c.IS_INBOUND = 0
        AND (c.OrderCancelledFlag = null ? 'N' : c.OrderCancelledFlag) = 'N'
        AND c.FacilityId IN (@warehouseId)
        AND (
            c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
            )
        AND (
            c.originalScheduledDeliveryDateTime BETWEEN @Date.scheduleDeliveryStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND (
            c.DateTimeShipped  BETWEEN @Date.shippedStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shippedEndDate))
            )
        AND (
            c.ScheduledPickUpDateTime  BETWEEN @Date.scheduleToShipStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (UPPER(c.OriginCountry) IN (@originCountry))
        AND (UPPER(c.DestinationCountry) IN (@destinationCountry))
        AND (UPPER(c.OriginCity) IN (@originCity))
        AND (UPPER(c.DestinationCity) IN (@destinationCity))
        AND ((is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN (@ShipmentModeArray))
        AND ((is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN (@CarrierTypeArray))
        AND (
            c.estimatedDeliveryDateTime  BETWEEN @Date.deliveryEtaStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.deliveryEtaEndDate))
            )
            AND (
            c.actualDeliveryDateTime  BETWEEN @Date.actualDeliveryStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
            )
        AND (
            INDEX_OF(@originLocation, ",") != - 1 ? CONCAT (
                UPPER(c.OriginCity)
                ,','
                ,UPPER(c.OriginCountry)
                ) :UPPER(c.OriginCountry)
            ) IN (@originLocation)
        AND (
            INDEX_OF(@destinationLocation, ",") != - 1 ? CONCAT (
                UPPER(c.DestinationCity)
                ,','
                ,UPPER(c.DestinationCountry)
                ) :UPPER(c.DestinationCountry)
            ) IN (@destinationLocation)
        AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = @isTemperatureTracked))
        AND (
            c.shipmentBookedOnDateTime BETWEEN @Date.bookedStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.bookedEndDate))
            )
        AND (UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN (@OrderType))--Sprint 52 Changes 
        and (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = c.is_managed
        -- additional filters
        AND (
            c.PickUpDate BETWEEN @Date.pickupStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.pickupEndDate))
            )
		
        AND c.is_deleted = 0
    ) a
GROUP BY a.shipmentServiceLevel
 
-- else
 
 SELECT null ShipmentServiceLevel, null shipmentServiceLevelCount

-- RESULT SET 6 --Sprint 52 Changes

--paramter required info

-- @DPProductLineKey Required	UPPER
-- @DPServiceLineKey	Optional	UPPER
-- @AccountKeys   Required	UPPER
-- @warehouseId	Optional	UPPER
-- @Date.shipmentCreationStartDate 	Optional	
-- @Date.shipmentCreationEndDate	Optional	
-- @Date.scheduleDeliveryStartDate	Optional	
-- @Date.scheduleDeliveryEndDate	Optional	
-- @Date.shippedStartDate	Optional	
-- @Date.shippedEndDate	Optional	
-- @Date.scheduleToShipStartDate and 	Optional	
-- @Date.scheduleToShipEndDate	Optional	
-- @originCountry	Optional	UPPER
-- @DestinationCountry	Optional	UPPER
-- @OriginCountry	Optional	UPPER
-- @destinationCountry	Optional	UPPER
-- @originLocation Optional	UPPER
-- @destinationLocation Optional	UPPER
-- @CarrierTypeArray 	Optional	CASE WHEN item in the array ='NULL' THEN 'UNASSIGNED' ELSE item 
-- @ShipmentModeArray	Optional	CASE WHEN item int he array ='NULL' THEN 'UNASSIGNED' ELSE item 
-- @ServiceLevelArray	Optional	
-- @Date.bookedStartDate Optional	@Date.bookedEndDate Optional	
-- @Date.pickupStartDate Optional	@Date.pickupEndDate Optional	
-- @Date.deliveryEtaStartDate	Optional	@Date.deliveryEtaEndDate	Optional	
-- @Date.actualDeliveryStartDate 	Optional @Date.actualDeliveryEndDate	Optional	
-- @isTemperatureTracked 	Optional	UPPER
-- @milestoneStatus	Optional	UPPER
-- @deliveryStatus	Optional	UPPER
-- @isClaim	Optional	
-- @OrderType Optional	UPPER
-- @isManaged Optional	UPPER 


SELECT DISTINCT c.TemperatureThreshold 
        FROM   c 
		WHERE 
		AND (
					c.PickUpDate BETWEEN @Date.pickupStartDate
					AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.pickupEndDate))
				  )
			 
			  AND c.TemperatureThreshold !=null
		AND	  (
				c.AccountId IN (@AccountKeys.DPProductLineKey)
				OR c.AccountId =@DPProductLineKey
				)
        AND c.IS_INBOUND = 0
		AND c.is_deleted = 0
        AND (c.OrderCancelledFlag = null ? 'N' : c.OrderCancelledFlag) = 'N'
        AND c.FacilityId IN (@warehouseId)
        AND (
            c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
            )
        AND (
            c.originalScheduledDeliveryDateTime BETWEEN @Date.scheduleDeliveryStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND (
            c.DateTimeShipped  BETWEEN @Date.shippedStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shippedEndDate))
            )
        AND (
            c.ScheduledPickUpDateTime  BETWEEN @Date.scheduleToShipStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            ) 
        AND (UPPER(c.OriginCountry) IN (@originCountry))
        AND (UPPER(c.DestinationCountry) IN (@destinationCountry))
        AND (UPPER(c.OriginCity) IN (@originCity))
        AND (UPPER(c.DestinationCity) IN (@destinationCity))
        AND ((is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN (@ShipmentModeArray))
        AND ((is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN (@CarrierTypeArray))
        AND (
            c.estimatedDeliveryDateTime  BETWEEN @Date.deliveryEtaStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.deliveryEtaEndDate))
            )
            AND (
            c.actualDeliveryDateTime  BETWEEN @Date.actualDeliveryStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
            )
        AND (
            INDEX_OF(@originLocation, ",") != - 1 ? CONCAT (
                UPPER(c.OriginCity)
                ,','
                ,UPPER(c.OriginCountry)
                ) :UPPER(c.OriginCountry)
            ) IN (@originLocation)
        AND (
            INDEX_OF(@destinationLocation, ",") != - 1 ? CONCAT (
                UPPER(c.DestinationCity)
                ,','
                ,UPPER(c.DestinationCountry)
                ) :UPPER(c.DestinationCountry)
            ) IN (@destinationLocation)
        AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = @isTemperatureTracked))
        AND (
            c.shipmentBookedOnDateTime BETWEEN @Date.bookedStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.bookedEndDate))
            )
        AND (UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN (@OrderType))--Sprint 52 Changes 
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = c.is_managed
