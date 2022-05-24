-- rpt_Outbound_Shipments


-- Result set 1
--IF @deliveryStatus = '' or @deliveryStatus = null
 
SELECT count(1) totalShipments
FROM (
    SELECT DISTINCT c.shipmentNumber
        ,c.upsTransportShipmentNumber
    FROM c
    WHERE (
            c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
            )
        AND c.IS_INBOUND = 0
        AND (c.OrderCancelledFlag = null ? 'N' : c.OrderCancelledFlag) = 'N'
        AND c.FacilityId IN ('9E2AE34F-A290-4C29-35D6-08D61828A014')
        AND (
            c.DateTimeReceived BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
            )
        AND (
            c.originalScheduledDeliveryDateTime BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
            )
        AND (
            c.DateTimeShipped BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
            )
        -- and (c.ScheduledPickUpDateTime between '2022-01-01 00:00:00.000' and DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
        AND (UPPER(c.OriginCountry) IN ('US'))
        AND (UPPER(c.DestinationCountry) IN ('US'))
        AND (UPPER(c.OriginCity) IN ('MALDEN'))
        AND (UPPER(c.DestinationCity) IN ('NASHVILLE'))
        AND ((is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN ('Carrier - Replenishment'))
        AND ((is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN ('FedEx'))
        -- and (c.estimatedDeliveryDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
        -- and (c.actualDeliveryDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
        AND (
            INDEX_OF('MALDEN,US', ",") != - 1 ? CONCAT (
                UPPER(c.OriginCity)
                ,','
                ,UPPER(c.OriginCountry)
                ) :UPPER(c.OriginCountry)
            ) IN ('MALDEN,US')
        AND (
            INDEX_OF('NASHVILLE,US', ",") != - 1 ? CONCAT (
                UPPER(c.DestinationCity)
                ,','
                ,UPPER(c.DestinationCountry)
                ) :UPPER(c.DestinationCountry)
            ) IN ('NASHVILLE,US')
        AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = 'N'))
        -- and (c.shipmentBookedOnDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
--        AND (UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN ('B2B'))  --Sprint 52 Changes
         AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = c.is_managed
        --additional filters for DIGITAL_SUMMARY_ORDERS_FILTER
        AND (
            c.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'
            )
        AND ((is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN ('FedEx Ground'))
        -- and (c.PickUpDate between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
 --     AND UPPER(c.TemperatureThreshold) IN ('') --Sprint 52 Changes
         --additional filters for DIGITAL_SUMMARY_ORDERS
        AND UPPER(c.milestoneStatus) IN ('DELIVERED')
        AND UPPER(c.deliveryStatus) IN ('ONTIME')
        --additional filters
        AND c.ISCLAIM = 'N'
        AND c.is_deleted = 0
    ) a
--else if ( @milestoneStatus like '%DELIVERED %'and @deliveryStatus != '*')
 
SELECT count(1) totalShipments
FROM (
    SELECT DISTINCT c.shipmentNumber
        ,c.upsTransportShipmentNumber
    FROM c
    WHERE (
            c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
            )
        AND c.IS_INBOUND = 0
        AND (c.OrderCancelledFlag = null ? 'N' : c.OrderCancelledFlag) = 'N'
        AND c.FacilityId IN ('9E2AE34F-A290-4C29-35D6-08D61828A014')
        AND (
            c.DateTimeReceived BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
            )
        AND (
            c.originalScheduledDeliveryDateTime BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
            )
        AND (
            c.DateTimeShipped BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
            )
        -- and (c.ScheduledPickUpDateTime between '2022-01-01 00:00:00.000' and DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
        AND (UPPER(c.OriginCountry) IN ('US'))
        AND (UPPER(c.DestinationCountry) IN ('US'))
        AND (UPPER(c.OriginCity) IN ('MALDEN'))
        AND (UPPER(c.DestinationCity) IN ('NASHVILLE'))
        AND ((is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN ('Carrier - Replenishment'))
        AND ((is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN ('FedEx'))
        -- and (c.estimatedDeliveryDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
        -- and (c.actualDeliveryDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
        AND (
            INDEX_OF('MALDEN,US', ",") != - 1 ? CONCAT (
                UPPER(c.OriginCity)
                ,','
                ,UPPER(c.OriginCountry)
                ) :UPPER(c.OriginCountry)
            ) IN ('MALDEN,US')
        AND (
            INDEX_OF('NASHVILLE,US', ",") != - 1 ? CONCAT (
                UPPER(c.DestinationCity)
                ,','
                ,UPPER(c.DestinationCountry)
                ) :UPPER(c.DestinationCountry)
            ) IN ('NASHVILLE,US')
        AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = 'N'))
        -- and (c.shipmentBookedOnDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) --null
--        AND (UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
         and (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = c.is_managed
        --additional filters for DIGITAL_SUMMARY_ORDERS_FILTER
        AND (
            c.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'
            )
        AND ((is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN ('FedEx Ground'))
        -- and (c.PickUpDate between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
     --     AND UPPER(c.TemperatureThreshold) IN ('') --Sprint 52 Changes
        --additional filters for DIGITAL_SUMMARY_ORDERS
        AND UPPER(c.milestoneStatus) IN ('DELIVERED')
        AND UPPER(c.deliveryStatus) IN ('ONTIME')
        --additional filters
        AND (
            (
                UPPER(c.milestoneStatus) IN (
                    'DELIVERED'
                    ,'CREATED'
                    )
                AND UPPER(c.milestoneStatus) != 'DELIVERED'
                )
            OR (
                UPPER(c.deliveryStatus) IN ('ONTIME')
                AND UPPER(c.deliveryStatus) != 'DELIVERED'
                )
            )
        AND c.ISCLAIM = 'N'
        AND c.is_deleted = 0
    ) a 
 
--else  
 
SELECT count(1) totalShipments
FROM (
    SELECT DISTINCT c.shipmentNumber
        ,c.upsTransportShipmentNumber
    FROM c
    WHERE (
            c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
            )
        AND c.IS_INBOUND = 0
        AND (c.OrderCancelledFlag = null ? 'N' : c.OrderCancelledFlag) = 'N'
        AND c.FacilityId IN ('9E2AE34F-A290-4C29-35D6-08D61828A014')
        AND (
            c.DateTimeReceived BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
            )
        AND (
            c.originalScheduledDeliveryDateTime BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
            )
        AND (
            c.DateTimeShipped BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
            )
        -- and (c.ScheduledPickUpDateTime between '2022-01-01 00:00:00.000' and DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
        AND (UPPER(c.OriginCountry) IN ('US'))
        AND (UPPER(c.DestinationCountry) IN ('US'))
        AND (UPPER(c.OriginCity) IN ('MALDEN'))
        AND (UPPER(c.DestinationCity) IN ('NASHVILLE'))
        AND ((is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN ('Carrier - Replenishment'))
        AND ((is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN ('FedEx'))
        -- and (c.estimatedDeliveryDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
        -- and (c.actualDeliveryDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
        AND (
            INDEX_OF('MALDEN,US', ",") != - 1 ? CONCAT (
                UPPER(c.OriginCity)
                ,','
                ,UPPER(c.OriginCountry)
                ) :UPPER(c.OriginCountry)
            ) IN ('MALDEN,US')
        AND (
            INDEX_OF('NASHVILLE,US', ",") != - 1 ? CONCAT (
                UPPER(c.DestinationCity)
                ,','
                ,UPPER(c.DestinationCountry)
                ) :UPPER(c.DestinationCountry)
            ) IN ('NASHVILLE,US')
        AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = 'N'))
        -- and (c.shipmentBookedOnDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) --null
--        AND (UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN ('B2B'))  --Sprint 52 Changes
         AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = c.is_managed
        --additional filters for DIGITAL_SUMMARY_ORDERS_FILTER
        AND (
            c.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'
            )
        AND ((is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN ('FedEx Ground'))
        -- and (c.PickUpDate between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
 --     AND UPPER(c.TemperatureThreshold) IN ('') --Sprint 52 Changes
         --additional filters for DIGITAL_SUMMARY_ORDERS
        AND UPPER(c.milestoneStatus) IN ('DELIVERED')
        AND UPPER(c.deliveryStatus) IN ('ONTIME')
        --additional filters
        AND (
            UPPER(c.milestoneStatus) IN ('DELIVERED')
            OR UPPER(c.deliveryStatus) IN ('ONTIME')
            )
        AND c.ISCLAIM = 'N'
        AND c.is_deleted = 0
    ) a

-- Result set 2


--IF @topRow = 0  
 
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
    ,c.estimatedDeliveryDateTime     --Sprint 52 Changes
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
        c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        OR c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
        )
    AND c.IS_INBOUND = 0
    AND (c.OrderCancelledFlag = null ? 'N' : c.OrderCancelledFlag) = 'N'
    AND c.FacilityId IN ('9E2AE34F-A290-4C29-35D6-08D61828A014')
    AND (
        c.DateTimeReceived BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
        )
    AND (
        c.originalScheduledDeliveryDateTime BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
        )
    AND (
        c.DateTimeShipped BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
        )
    -- and (c.ScheduledPickUpDateTime between '2022-01-01 00:00:00.000' and DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
    AND (UPPER(c.OriginCountry) IN ('US'))
    AND (UPPER(c.DestinationCountry) IN ('US'))
    AND (UPPER(c.OriginCity) IN ('MALDEN'))
    AND (UPPER(c.DestinationCity) IN ('NASHVILLE'))
    AND ((is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN ('Carrier - Replenishment'))
    AND ((is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN ('FedEx'))
    -- and (c.estimatedDeliveryDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
    -- and (c.actualDeliveryDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
    AND (
        INDEX_OF('MALDEN,US', ",") != - 1 ? CONCAT (
            UPPER(c.OriginCity)
            ,','
            ,UPPER(c.OriginCountry)
            ) :UPPER(c.OriginCountry)
        ) IN ('MALDEN,US')
    AND (
        INDEX_OF('NASHVILLE,US', ",") != - 1 ? CONCAT (
            UPPER(c.DestinationCity)
            ,','
            ,UPPER(c.DestinationCountry)
            ) :UPPER(c.DestinationCountry)
        ) IN ('NASHVILLE,US')
    AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = 'N'))
    -- and (c.shipmentBookedOnDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) --null
--  AND (UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
     -- and (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = c.is_managed
    --additional filters for DIGITAL_SUMMARY_ORDERS_FILTER
    AND (
        c.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        OR c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'
        )
    AND ((is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN ('FedEx Ground'))
    -- and (c.PickUpDate between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) --null
 --     AND UPPER(c.TemperatureThreshold) IN ('') --Sprint 52 Changes
    --additional filters for DIGITAL_SUMMARY_ORDERS
    AND UPPER(c.milestoneStatus) IN ('DELIVERED')
    AND UPPER(c.deliveryStatus) IN ('ONTIME')
    --additional filters
    AND (
        (
            (UPPER(c.milestoneStatus) IN ('DELIVERED'))
            AND '*' = '*'
            )
        OR (
            (
                (
                    UPPER(c.milestoneStatus) IN ('DELIVERED')
                    AND 'DELIVERED' NOT IN ('DELIVERED')
                    )
                )
            OR UPPER(c.deliveryStatus) IN ('ONTIME')
            )
        OR (
            (
                (
                    UPPER(c.milestoneStatus) IN ('DELIVERED')
                    AND UPPER(c.milestoneStatus) != 'DELIVERED'
                    )
                )
            OR (
                UPPER(c.deliveryStatus) IN ('ONTIME')
                AND UPPER(c.milestoneStatus) = 'DELIVERED'
                )
            )
        )
    AND c.ISCLAIM = 'N'
    AND c.is_deleted = 0
 
 --IF @topRow > 0  
 
SELECT DISTINCT top 1 c.shipmentNumber
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
    ,c.estimatedDeliveryDateTime    --Sprint 52 Changes
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
        c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        OR c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
        )
    AND c.IS_INBOUND = 0
    AND (c.OrderCancelledFlag = null ? 'N' : c.OrderCancelledFlag) = 'N'
    AND c.FacilityId IN ('9E2AE34F-A290-4C29-35D6-08D61828A014')
    AND (
        c.DateTimeReceived BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
        )
    AND (
        c.originalScheduledDeliveryDateTime BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
        )
    AND (
        c.DateTimeShipped BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
        )
    -- and (c.ScheduledPickUpDateTime between '2022-01-01 00:00:00.000' and DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
    AND (UPPER(c.OriginCountry) IN ('US'))
    AND (UPPER(c.DestinationCountry) IN ('US'))
    AND (UPPER(c.OriginCity) IN ('MALDEN'))
    AND (UPPER(c.DestinationCity) IN ('NASHVILLE'))
    AND ((is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN ('Carrier - Replenishment'))
    AND ((is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN ('FedEx'))
    -- and (c.estimatedDeliveryDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
    -- and (c.actualDeliveryDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
    AND (
        INDEX_OF('MALDEN,US', ",") != - 1 ? CONCAT (
            UPPER(c.OriginCity)
            ,','
            ,UPPER(c.OriginCountry)
            ) :UPPER(c.OriginCountry)
        ) IN ('MALDEN,US')
    AND (
        INDEX_OF('NASHVILLE,US', ",") != - 1 ? CONCAT (
            UPPER(c.DestinationCity)
            ,','
            ,UPPER(c.DestinationCountry)
            ) :UPPER(c.DestinationCountry)
        ) IN ('NASHVILLE,US')
    AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = 'N'))
    -- and (c.shipmentBookedOnDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) --null
--  AND (UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
     -- and (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = c.is_managed
    --additional filters for DIGITAL_SUMMARY_ORDERS_FILTER
    AND (
        c.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        OR c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'
        )
    AND ((is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN ('FedEx Ground'))
    -- and (c.PickUpDate between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) --null
--      AND UPPER(c.TemperatureThreshold) IN ('') --Sprint 52 Changes
     --additional filters for DIGITAL_SUMMARY_ORDERS
    AND UPPER(c.milestoneStatus) IN ('DELIVERED')
    AND UPPER(c.deliveryStatus) IN ('ONTIME')
    --additional filters
    AND (
        (
            (UPPER(c.milestoneStatus) IN ('DELIVERED'))
            AND '*' = '*'
            )
        OR (
            (
                (
                    UPPER(c.milestoneStatus) IN ('DELIVERED')
                    AND 'DELIVERED' NOT IN  ('DELIVERED')
                    )
                )
            OR UPPER(c.deliveryStatus) IN ('ONTIME')
            )
        OR (
            (
                (
                    UPPER(c.milestoneStatus) IN ('DELIVERED')
                    AND UPPER(c.milestoneStatus) != 'DELIVERED'
                    )
                )
            OR (
                UPPER(c.deliveryStatus) IN ('ONTIME')
                AND UPPER(c.milestoneStatus) = 'DELIVERED'
                )
            )
        )
    AND c.ISCLAIM = 'N'
    AND c.is_deleted = 0


-- Result set 3
SELECT a.milestoneStatus MilestoneStatus
    ,count(1) MilestoneStatusCount
FROM (
    SELECT DISTINCT c.milestoneStatus
        ,c.shipmentNumber
        ,c.upsTransportShipmentNumber
    FROM c
    WHERE (
            c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
            )
        AND c.IS_INBOUND = 0
        AND (c.OrderCancelledFlag = null ? 'N' : c.OrderCancelledFlag) = 'N'
        AND c.FacilityId IN ('9E2AE34F-A290-4C29-35D6-08D61828A014')
        AND (
            c.DateTimeReceived BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
            )
        AND (
            c.originalScheduledDeliveryDateTime BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
            )
        AND (
            c.DateTimeShipped BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
            )
        -- and (c.ScheduledPickUpDateTime between '2022-01-01 00:00:00.000' and DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
        AND (UPPER(c.OriginCountry) IN ('US'))
        AND (UPPER(c.DestinationCountry) IN ('US'))
        AND (UPPER(c.OriginCity) IN ('MALDEN'))
        AND (UPPER(c.DestinationCity) IN ('NASHVILLE'))
        AND ((is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN ('Carrier - Replenishment'))
        AND ((is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN ('FedEx'))
        -- and (c.estimatedDeliveryDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
        -- and (c.actualDeliveryDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
        AND (
            INDEX_OF('MALDEN,US', ",") != - 1 ? CONCAT (
                UPPER(c.OriginCity)
                ,','
                ,UPPER(c.OriginCountry)
                ) :UPPER(c.OriginCountry)
            ) IN ('MALDEN,US')
        AND (
            INDEX_OF('NASHVILLE,US', ",") != - 1 ? CONCAT (
                UPPER(c.DestinationCity)
                ,','
                ,UPPER(c.DestinationCountry)
                ) :UPPER(c.DestinationCountry)
            ) IN ('NASHVILLE,US')
        AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = 'N'))
        -- and (c.shipmentBookedOnDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) --null
--      AND (UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
         AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = c.is_managed
        --additional filters for DIGITAL_SUMMARY_ORDERS_FILTER
        AND (
            c.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'
            )
        AND ((is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN ('FedEx Ground'))
        -- and (c.PickUpDate between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
 --     AND UPPER(c.TemperatureThreshold) IN ('') --Sprint 52 Changes
        --additional filters
        AND c.is_deleted = 0
    ) a
GROUP BY a.milestoneStatus

-- Result set 4
SELECT a.deliveryStatus DeliveryStatus
    ,count(1) DeliveryStatusCount
FROM (
    SELECT DISTINCT c.deliveryStatus
        ,c.shipmentNumber
        ,c.upsTransportShipmentNumber
    FROM c
    WHERE (
            c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
            )
        AND c.IS_INBOUND = 0
        AND (c.OrderCancelledFlag = null ? 'N' : c.OrderCancelledFlag) = 'N'
        AND c.FacilityId IN ('9E2AE34F-A290-4C29-35D6-08D61828A014')
        AND (
            c.DateTimeReceived BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
            )
        AND (
            c.originalScheduledDeliveryDateTime BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
            )
        AND (
            c.DateTimeShipped BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
            )
        -- and (c.ScheduledPickUpDateTime between '2022-01-01 00:00:00.000' and DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
        AND (UPPER(c.OriginCountry) IN ('US'))
        AND (UPPER(c.DestinationCountry) IN ('US'))
        AND (UPPER(c.OriginCity) IN ('MALDEN'))
        AND (UPPER(c.DestinationCity) IN ('NASHVILLE'))
        AND ((is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN ('Carrier - Replenishment'))
        AND ((is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN ('FedEx'))
        -- and (c.estimatedDeliveryDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
        -- and (c.actualDeliveryDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
        AND (
            INDEX_OF('MALDEN,US', ",") != - 1 ? CONCAT (
                UPPER(c.OriginCity)
                ,','
                ,UPPER(c.OriginCountry)
                ) :UPPER(c.OriginCountry)
            ) IN ('MALDEN,US')
        AND (
            INDEX_OF('NASHVILLE,US', ",") != - 1 ? CONCAT (
                UPPER(c.DestinationCity)
                ,','
                ,UPPER(c.DestinationCountry)
                ) :UPPER(c.DestinationCountry)
            ) IN ('NASHVILLE,US')
        AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = 'N'))
        -- and (c.shipmentBookedOnDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
--      AND (UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
         and (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = c.is_managed
        --additional filters for DIGITAL_SUMMARY_ORDERS_FILTER
        AND (
            c.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'
            )
        AND ((is_null(c.ServiceLevel) ? 'NOSERVICE' : c.ServiceLevel) IN ('FedEx Ground'))
        -- and (c.PickUpDate between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
 --     AND UPPER(c.TemperatureThreshold) IN ('') --Sprint 52 Changes
        --additional filters
        AND c.milestoneStatus = 'DELIVERED'
        AND c.is_deleted = 0
    ) a
GROUP BY a.deliveryStatus


-- Result set 5


--  IF @isShipmentServiceLevelResultSet  'Y'

SELECT a.shipmentServiceLevel
    ,count(1) shipmentServiceLevelCount
FROM (
    SELECT DISTINCT c.shipmentServiceLevel
        ,c.shipmentNumber
        ,c.upsTransportShipmentNumber
    FROM c
    WHERE (
            c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
            )
        AND c.IS_INBOUND = 0
        AND (c.OrderCancelledFlag = null ? 'N' : c.OrderCancelledFlag) = 'N'
        AND c.FacilityId IN ('9E2AE34F-A290-4C29-35D6-08D61828A014')
        AND (
            c.DateTimeReceived BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
            )
        AND (
            c.originalScheduledDeliveryDateTime BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
            )
        AND (
            c.DateTimeShipped BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
            )
        -- and (c.ScheduledPickUpDateTime between '2022-01-01 00:00:00.000' and DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
        AND (UPPER(c.OriginCountry) IN ('US'))
        AND (UPPER(c.DestinationCountry) IN ('US'))
        AND (UPPER(c.OriginCity) IN ('MALDEN'))
        AND (UPPER(c.DestinationCity) IN ('NASHVILLE'))
        AND ((is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN ('Carrier - Replenishment'))
        AND ((is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN ('FedEx'))
        -- and (c.estimatedDeliveryDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
        -- and (c.actualDeliveryDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
        AND (
            INDEX_OF('MALDEN,US', ",") != - 1 ? CONCAT (
                UPPER(c.OriginCity)
                ,','
                ,UPPER(c.OriginCountry)
                ) :UPPER(c.OriginCountry)
            ) IN ('MALDEN,US')
        AND (
            INDEX_OF('NASHVILLE,US', ",") != - 1 ? CONCAT (
                UPPER(c.DestinationCity)
                ,','
                ,UPPER(c.DestinationCountry)
                ) :UPPER(c.DestinationCountry)
            ) IN ('NASHVILLE,US')
        AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = 'N'))
        -- and (c.shipmentBookedOnDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) --null
--      AND (UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
         and (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = c.is_managed
        -- additional filters
        -- and (c.PickUpDate between '2022-02-10 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-12-16 00:00:00.000'))) --null

        AND c.is_deleted = 0
    ) a
GROUP BY a.shipmentServiceLevel
  
-- else
 SELECT null ShipmentServiceLevel, null shipmentServiceLevelCount
 
-- else
 SELECT null ShipmentServiceLevel, null shipmentServiceLevelCount

-- RESULT SET 6 --Sprint 52 Changes
SELECT DISTINCT c.TemperatureThreshold 
        FROM   c 
		WHERE 
--      AND (c.PickUpDate between '2022-02-10 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-12-16 00:00:00.000'))) --null
			 
		 c.TemperatureThreshold !=null
		AND	  (
            c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
            )
        AND c.IS_INBOUND = 0
		AND c.is_deleted = 0
        AND (c.OrderCancelledFlag = null ? 'N' : c.OrderCancelledFlag) = 'N'
        AND c.FacilityId IN ('9E2AE34F-A290-4C29-35D6-08D61828A014')
        AND (
            c.DateTimeReceived BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
            )
        AND (
            c.originalScheduledDeliveryDateTime BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
            )
        AND (
            c.DateTimeShipped BETWEEN '2022-01-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))
            )
        -- and (c.ScheduledPickUpDateTime between '2022-01-01 00:00:00.000' and DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null 
        AND (UPPER(c.OriginCountry) IN ('US'))
        AND (UPPER(c.DestinationCountry) IN ('US'))
        AND (UPPER(c.OriginCity) IN ('MALDEN'))
        AND (UPPER(c.DestinationCity) IN ('NASHVILLE'))
        AND ((is_null(c.ServiceMode) ? 'UNASSIGNED' : c.ServiceMode) IN ('Carrier - Replenishment'))
        AND ((is_null(c.Carrier) ? 'UNASSIGNED' : c.Carrier) IN ('FedEx'))
        -- and (c.estimatedDeliveryDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
        -- and (c.actualDeliveryDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
        AND (
            INDEX_OF('MALDEN,US', ",") != - 1 ? CONCAT (
                UPPER(c.OriginCity)
                ,','
                ,UPPER(c.OriginCountry)
                ) :UPPER(c.OriginCountry)
            ) IN ('MALDEN,US')
        AND (
            INDEX_OF('NASHVILLE,US', ",") != - 1 ? CONCAT (
                UPPER(c.DestinationCity)
                ,','
                ,UPPER(c.DestinationCountry)
                ) :UPPER(c.DestinationCountry)
            ) IN ('NASHVILLE,US')
        AND (((c.IS_TEMPERATURE = 'Y' ? c.IS_TEMPERATURE : (is_null(c.IS_TEMPERATURE) ? 'N' :c.IS_TEMPERATURE)) = 'N'))
        -- and (c.shipmentBookedOnDateTime between '2022-01-01 00:00:00.000' and  DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) -- null
--        AND (UPPER(is_null(c.shipmentDescription) ? '' :c.shipmentDescription) IN ('B2B'))--Sprint 52 Changes
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = c.is_managed
