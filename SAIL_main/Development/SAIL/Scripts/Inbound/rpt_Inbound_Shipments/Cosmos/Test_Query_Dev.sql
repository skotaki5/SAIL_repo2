--AUTHOR : VISHAL
--DESCRIPTION:rpt_Inbound_Shipments
--DATE : 22-02-2022

--Result Set 1
--* Target Container-digital_summary_orders

SELECT COUNT(1) totalShipments
FROM c
WHERE c.AccountId IN ('E648FA6F-6253-428E-8AC9-201E3EF83B91')
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
    AND (
     c.DateTimeReceived BETWEEN '2022-01-20 00:00:00.000'  
        AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    -- AND (
    --  c.LoadLatestDeliveryDate BETWEEN '2021-12-20 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-01 00:00:00.000'))
    --  )
    -- AND (
    --  c.DateTimeShipped BETWEEN '2020-12-20 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-01 00:00:00.000'))
    --  )
    -- AND (
    --  c.ScheduledPickUpDateTime BETWEEN '2020-12-20 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-01 00:00:00.000'))
    --  )
    -- AND (INDEX_OF("HEBRON,US", "","") != - 1 ?concat(c.OriginCity, ',', c.OriginCountry) : c.OriginCountry) IN ('SG')
    -- AND (INDEX_OF("HEBRON,US", "","") != - 1 ?concat(c.DestinationCity, ',', c.DestinationCountry) : c.DestinationCountry) IN ('US')
    -- AND (c.OriginCountry IN ('SG'))
    -- AND (c.DestinationCountry IN ('US'))
    -- AND (c.OriginCity IN ('Singapore'))
    -- AND (c.DestinationCity IN ('LOUISVILLE'))
    AND c.IS_INBOUND = 1
    -- AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
    -- AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    -- AND (
    --  (is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN (
    --      ""Local Courier""
    --      ,""Not Available""
    --      )
    --  )
    -- AND (
    --  (is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN (
    --      ""Drive - Scheduled""
    --      ,""Not Available""
    --      )
    --  )
    -- AND (
    --  (is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN (
    --      ""Courier-Drive - Critical""
    --      ,""Not Available""
    --      )
    --  )
    -- AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = 'N'))
    -- AND (
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
    -- AND (
    --  c.milestoneStatus IN (
    --      'DELIVERED'
    --      ,'PUTAWAY'
    --      )
    --  ) --This milestoneStatus is input parameter
    -- AND (
    --  c.milestoneStatus IN (
    --      'DELIVERED'
    --      ,'ASN CREATED'
    --      ,'FTZ'
    --      ,'RECEIVING'
    --      ,'PUTAWAY'
    --      )
    --  AND c.deliveryStatus IN ('ONTIME')
        -- )
    -- AND (c.ISCLAIM = ""N"")
     AND c.is_deleted = 0
	


--Result Set 2

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
WHERE c.AccountId IN ('E648FA6F-6253-428E-8AC9-201E3EF83B91')
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
    AND (
        c.DateTimeReceived BETWEEN '2022-01-20 00:00:00.000'   
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    -- AND (
    --  c.LoadLatestDeliveryDate BETWEEN '2021-12-20 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-01 00:00:00.000'))
    --  )
    -- AND (
    --  c.DateTimeShipped BETWEEN '2020-12-20 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-01 00:00:00.000'))
    --  )
    -- AND (
    --  c.ScheduledPickUpDateTime BETWEEN '2020-12-20 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-01 00:00:00.000'))
    --  )
    -- AND (INDEX_OF("HEBRON,US", ",") != - 1 ?concat(c.OriginCity, ',', c.OriginCountry) : c.OriginCountry) IN ('SG')
    -- AND (INDEX_OF("HEBRON,US", ",") != - 1 ?concat(c.DestinationCity, ',', c.DestinationCountry) : c.DestinationCountry) IN ('US')  
    -- AND (c.OriginCountry IN ('SG'))
    -- AND (c.DestinationCountry IN ('US'))
    -- AND (c.OriginCity IN ('Singapore'))
    -- AND (c.DestinationCity IN ('LOUISVILLE'))
       AND c.IS_INBOUND = 1
       AND c.is_deleted = 0
    -- AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
    -- AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    -- AND (
    --  (is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN (
    --      "Local Courier"
    --      ,"Not Available"
    --      )
    --  )
    -- AND (
    --  (is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN (
    --      "Drive - Scheduled"
    --      ,"Not Available"
    --      )
    --  )
    -- AND (
    --  (is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN (
    --      "Courier-Drive - Critical"
    --      ,"Not Available"
    --      )
    --  )
    -- AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = 'N'))
    -- AND (
    --  c.estimatedDeliveryDateTime BETWEEN '2021-12-20 00:00:00.000'
    --      AND '2022-01-01 00:00:00.000'
    --  )
    -- AND (
    --  c.actualDeliveryDateTime BETWEEN '2021-12-20 00:00:00.000'
    --      AND '2022-01-01 00:00:00.000'
    --  )
    -- AND (
    --  c.PickUpDate BETWEEN '2021-12-20 00:00:00.000'
    --      AND '2022-01-01 00:00:00.000'
    --  )
    -- AND (
    --  c.shipmentBookedOnDateTime BETWEEN '2021-12-20 00:00:00.000'
    --      AND '2022-01-01 00:00:00.000'
    --  )
    -- AND (
    --  c.milestoneStatus IN (
    --      'DELIVERED'
    --      ,'PUTAWAY'
    --      )
    --  )
    -- AND (
    --  c.milestoneStatus IN (
    --      'DELIVERED'
    --      ,'ASN CREATED'
    --      ,'FTZ'
    --      ,'RECEIVING'
    --      ,'PUTAWAY'
    --      )
    --  AND c.deliveryStatus IN ('ONTIME')
    --  )
     
ORDER BY c.shipmentCreateOnDateTime DESC

--CASE 2 IF @topRow > 0

SELECT DISTINCT TOP 1 c.shipmentNumber
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
WHERE c.AccountId IN ('E648FA6F-6253-428E-8AC9-201E3EF83B91')
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
    AND (
        c.DateTimeReceived BETWEEN '2022-01-20 00:00:00.000'   
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
    -- AND (
    --  c.LoadLatestDeliveryDate BETWEEN '2021-12-20 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-01 00:00:00.000'))
    --  )
    -- AND (
    --  c.DateTimeShipped BETWEEN '2020-12-20 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-01 00:00:00.000'))
    --  )
    -- AND (
    --  c.ScheduledPickUpDateTime BETWEEN '2020-12-20 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-01 00:00:00.000'))
    --  )
    -- AND (INDEX_OF("HEBRON,US", ",") != - 1 ?concat(c.OriginCity, ',', c.OriginCountry) : c.OriginCountry) IN ('SG')
    -- AND (INDEX_OF("HEBRON,US", ",") != - 1 ?concat(c.DestinationCity, ',', c.DestinationCountry) : c.DestinationCountry) IN ('US')
    -- AND (c.OriginCountry IN ('SG'))
    -- AND (c.DestinationCountry IN ('US'))
    -- AND (c.OriginCity IN ('Singapore'))
    -- AND (c.DestinationCity IN ('LOUISVILLE'))  
       AND c.IS_INBOUND = 1
       AND c.is_deleted = 0
    -- AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
    -- AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
    -- AND (
    --  (is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN (
    --      "Local Courier"
    --      ,"Not Available"
    --      )
    --  )
    -- AND (
    --  (is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN (
    --      "Drive - Scheduled"
    --      ,"Not Available"
    --      )
    --  )
    -- AND (
    --  (is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN (
    --      "Courier-Drive - Critical"
    --      ,"Not Available"
    --      )
    --  )
    -- AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = 'N'))
    -- AND (
    --  c.estimatedDeliveryDateTime BETWEEN '2021-12-20 00:00:00.000'
    --      AND '2022-01-01 00:00:00.000'
    --  )
    -- AND (
    --  c.actualDeliveryDateTime BETWEEN '2021-12-20 00:00:00.000'
    --      AND '2022-01-01 00:00:00.000'
    --  )
    -- AND (
    --  c.PickUpDate BETWEEN '2021-12-20 00:00:00.000'
    --      AND '2022-01-01 00:00:00.000'
    --  )
    -- AND (
    --  c.shipmentBookedOnDateTime BETWEEN '2021-12-20 00:00:00.000'
    --      AND '2022-01-01 00:00:00.000'
    --  )
    -- AND (
    --  c.milestoneStatus IN (
    --      'DELIVERED'
    --      ,'PUTAWAY'
    --      )
    --  )
    -- AND (
    --  c.milestoneStatus IN (
    --      'DELIVERED'
    --      ,'ASN CREATED'
    --      ,'FTZ'
    --      ,'RECEIVING'
    --      ,'PUTAWAY'
    --      )
    --  AND c.deliveryStatus IN ('ONTIME')
    --  )
     
ORDER BY c.shipmentCreateOnDateTime DESC	

--Result Set 3

--* Target Container-digital_summary_orders


SELECT a.milestoneStatus AS MilestoneStatus
    ,COUNT(a.milestoneStatus) AS MilestoneStatusCount
FROM (
    SELECT DISTINCT c.shipmentNumber
        ,c.upsTransportShipmentNumber
        ,c.milestoneStatus
    FROM c
    WHERE c.AccountId IN ('E648FA6F-6253-428E-8AC9-201E3EF83B91')
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
        AND (
            c.DateTimeReceived BETWEEN '2022-01-20 00:00:00.000'   
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
        -- AND (
        --  c.LoadLatestDeliveryDate BETWEEN '2021-12-20 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-01 00:00:00.000'))
        --  )
        -- AND (
        --  c.DateTimeShipped BETWEEN '2020-12-20 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-01 00:00:00.000'))
        --  )
        -- AND (
        --  c.ScheduledPickUpDateTime BETWEEN '2020-12-20 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-01 00:00:00.000'))
        --  )
        -- AND (INDEX_OF("HEBRON,US", ",") != - 1 ?concat(c.OriginCity, ',', c.OriginCountry) : c.OriginCountry) IN ('SG')
        -- AND (INDEX_OF("HEBRON,US", ",") != - 1 ?concat(c.DestinationCity, ',', c.DestinationCountry) : c.DestinationCountry) IN ('US')
        -- AND (c.OriginCountry IN ('SG'))
        -- AND (c.DestinationCountry IN ('US'))
        -- AND (c.OriginCity IN ('Singapore'))
        -- AND (c.DestinationCity IN ('LOUISVILLE'))  
           AND c.IS_INBOUND = 1
           AND c.is_deleted = 0
        -- AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
        -- AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
        -- AND (
        --  (is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN (
        --      "Local Courier"
        --      ,"Not Available"
        --      )
        --  )
        -- AND (
        --  (is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN (
        --      "Drive - Critical"
        --      ,"Not Available"
        --      )
        --  )
        -- AND (
        --  (is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN (
        --      "Courier-Drive - Critical"
        --      ,"Not Available"
        --      )
        --  )
        -- AND ((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = 'N')
    ) a
GROUP BY a.milestoneStatus


--Result Set 4

--* Target Container-digital_summary_orders

SELECT a.deliveryStatus AS DeliveryStatus
    ,COUNT(1) AS DeliveryStatusCount
FROM (
    SELECT DISTINCT c.shipmentNumber
        ,c.upsTransportShipmentNumber
        ,c.deliveryStatus
    FROM c
    WHERE c.AccountId IN ('E648FA6F-6253-428E-8AC9-201E3EF83B91')
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
        AND (
            c.DateTimeReceived BETWEEN '2022-01-20 00:00:00.000'   
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
        -- AND (
        --  c.LoadLatestDeliveryDate BETWEEN '2021-12-20 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-01 00:00:00.000'))
        --  )
        -- AND (
        --  c.DateTimeShipped BETWEEN '2020-12-20 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-01 00:00:00.000'))
        --  )
        -- AND (
        --  c.ScheduledPickUpDateTime BETWEEN '2020-12-20 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-01 00:00:00.000'))
        --  )
        -- AND (INDEX_OF("HEBRON,US", ",") != - 1 ?concat(c.OriginCity, ',', c.OriginCountry) : c.OriginCountry) IN ('SG')
        -- AND (INDEX_OF("HEBRON,US", ",") != - 1 ?concat(c.DestinationCity, ',', c.DestinationCountry) : c.DestinationCountry) IN ('US')
        -- AND (c.OriginCountry IN ('SG'))
        -- AND (c.DestinationCountry IN ('US'))
        -- AND (c.OriginCity IN ('Singapore'))
        -- AND (c.DestinationCity IN ('LOUISVILLE'))  
           AND c.IS_INBOUND = 1
           AND c.is_deleted = 0
        -- AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
        -- AND (is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y'
        -- AND (
        --  (is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN (
        --      "Local Courier"
        --      ,"Not Available"
        --      )
        --  )
        -- AND (
        --  (is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN (
        --      "Drive - Critical"
        --      ,"Not Available"
        --      )
        --  )
        -- AND (
        --  (is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN (
        --      "Courier-Drive - Critical"
        --      ,"Not Available"
        --      )
        --  )
        -- AND ((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = 'N')
        -- AND (
        --  c.milestoneStatus IN (
        --      'DELIVERED'
        --      ,'ASN CREATED'
        --      ,'FTZ'
        --      ,'RECEIVING'
        --      ,'PUTAWAY'
        --      )
        --  )
    ) a
GROUP BY a.deliveryStatus

--Result Set 5

--* Target Container-digital_summary_orders

--CASE 1 IF @isShipmentServiceLevelResultSet ='Y'

SELECT a.shipmentServiceLevel
    ,count(1) shipmentServiceLevelCount
FROM (
    SELECT DISTINCT c.shipmentServiceLevel
        ,c.shipmentNumber
        ,c.upsTransportShipmentNumber
    FROM c
    WHERE c.AccountId IN ('E648FA6F-6253-428E-8AC9-201E3EF83B91')
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
        AND (
            c.DateTimeReceived BETWEEN '2022-01-20 00:00:00.000'   
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-31 00:00:00.000'))
        )
        -- AND (
        --  c.LoadLatestDeliveryDate BETWEEN '2021-11-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2021-12-31 00:00:00.000'))
        --  )
        -- AND (
        --  c.DateTimeShipped BETWEEN '2021-11-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2021-12-31 00:00:00.000'))
        --  )
        -- AND (
        --  c.ScheduledPickUpDateTime BETWEEN '2021-11-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2021-12-31 00:00:00.000'))
        --  )
         
        -- AND (INDEX_OF("HEBRON,US", ",") != - 1 ?concat(c.OriginCity, ',', c.OriginCountry) : c.OriginCountry) IN ('SG')
        -- AND (INDEX_OF("HEBRON,US", ",") != - 1 ?concat(c.DestinationCity, ',', c.DestinationCountry) : c.DestinationCountry) IN ('US')
        -- AND (c.OriginCountry IN ('SG'))
        -- AND (c.DestinationCountry IN ('US'))
        -- AND (c.OriginCity IN ('Singapore'))
        -- AND (c.DestinationCity IN ('LOUISVILLE'))  
        -- AND ((is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN ('Local Courier'))
        -- AND ((is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN ('Drive - Scheduled'))
         
        -- AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = 'N'))
        AND c.IS_INBOUND = 1
        AND c.is_deleted = 0
        -- AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
        -- AND ((is_null(c.OrderCancelledFlag) ? 'N' :c.OrderCancelledFlag) <> 'Y')
        -- and c.OrderCancelledFlag ='N'
    ) a
GROUP BY a.shipmentServiceLevel

--CASE 2 IF @isShipmentServiceLevelResultSet !='Y'

 SELECT ''  ShipmentServiceLevel, '' shipmentServiceLevelCount