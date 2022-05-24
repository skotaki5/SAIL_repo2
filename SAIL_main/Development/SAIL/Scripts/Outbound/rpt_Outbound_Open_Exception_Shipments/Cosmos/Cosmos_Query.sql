--Author :Mahesh Rathi
--DESCRIPITION : rpt_Outbound_Open_Exception_Shipments
--DATE : 08-04-2022

Result set 1:

/*
Parameter Requirement info -
-->@DPProductLineKey  required
-->@DPServiceLineKey  optional
-->@warehouseId       optional
-->@startDate         optional
-->@endDate           optional
-->@isTemperatureTracked optional

-->Target Container - digital_summary_orders

*/

select count(1) as totalShipments from c
WHERE c.AccountId = @DPProductLineKey
AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
and  c.FacilityId in (@warehouseId)
and (c.DateTimeReceived between @StartDate and  DateTimeAdd("ms",-2, DateTimeAdd(@EndDate)) )
and ((c.IS_TEMPERATURE = 'Y'? c.IS_TEMPERATURE : is_null(c.IS_TEMPERATURE)?'N':c.IS_TEMPERATURE) = @isTemperatureTracked)
and c.IS_INBOUND = 0 and c.is_deleted = 0 and c.exception != null


Result set 2:
/*
Parameter Requirement info -
-->@DPProductLineKey  required
-->@DPServiceLineKey  optional
-->@warehouseId       optional
-->@startDate         optional
-->@endDate           optional
-->@isTemperatureTracked optional

-->Target Container - digital_summary_orders

*/

If @topRow = 0 :

select
    c.referenceNumber,
 c.upsShipmentNumber,
 t.exceptionType,
 t.exceptionReason,
 c.milestoneStatus,
 c.shipmentOrigin_addressLine1       AS shipmentOrigin__addressLine1,
    c.shipmentOrigin_addressLine2       AS shipmentOrigin__addressLine2,
    c.shipmentOrigin_city               AS shipmentOrigin__city,
    c.shipmentOrigin_stateProvince      AS shipmentOrigin__stateProvince,
    c.shipmentOrigin_postalCode         AS shipmentOrigin__postalCode,
    c.shipmentOrigin_country            AS shipmentOrigin__country,
    c.shipmentDestination_addressLine1  AS shipmentDestination__addressLine1,
    c.shipmentDestination_addressLine2  AS shipmentDestination__addressLine2,
    c.shipmentDestination_city          AS shipmentDestination__city,
    c.shipmentDestination_stateProvince AS shipmentDestination__stateProvince,
    c.shipmentDestination_postalCode    AS shipmentDestination__postalCode,
    c.shipmentDestination_country       AS shipmentDestination__country,
 c.shipmentServiceLevel,
    c.shipmentServiceLevelCode,
 c.shipmentCarrierCode,
 c.shipmentCarrier,
 c.warehouseId,
    c.actualShipmentDateTime,
    c.shipmentPlaceDateTime,
    c.originalScheduledDeliveryDateTime,
    c.actualScheduledDeliveryDateTime
   ,c.isTemperatureTracked
   ,c.latestTemperature
   ,c.latestTemperatureInCelsius
   ,c.latestTemperatureInFahrenheit
   ,c.temperatureDateTime
   ,c.temperatureCity
   ,c.temperatureState
   ,c.temperatureCountry
from c
join t in c.exception
WHERE c.AccountId = @DPProductLineKey
AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
and  c.FacilityId in (@warehouseId)
and (c.DateTimeReceived between @StartDate and  @EndDate )
and ((c.IS_TEMPERATURE = 'Y'? c.IS_TEMPERATURE : is_null(c.IS_TEMPERATURE)?'N':c.IS_TEMPERATURE) = @isTemperatureTracked)
and c.IS_INBOUND = 0 and c.is_deleted = 0 and c.exception != null
order by c.shipmentPlaceDateTime

IF @topRow > 0:

/*
Parameter Requirement info -
-->@DPProductLineKey  required
-->@DPServiceLineKey  optional
-->@warehouseId       optional
-->@startDate         optional
-->@endDate           optional
-->@isTemperatureTracked optional

-->Target Container - digital_summary_orders

*/

select top @topRow
    c.referenceNumber,
 c.upsShipmentNumber,
 t.exceptionType,
 t.exceptionReason,
 c.milestoneStatus,
 c.shipmentOrigin_addressLine1       AS shipmentOrigin__addressLine1,
    c.shipmentOrigin_addressLine2       AS shipmentOrigin__addressLine2,
    c.shipmentOrigin_city               AS shipmentOrigin__city,
    c.shipmentOrigin_stateProvince      AS shipmentOrigin__stateProvince,
    c.shipmentOrigin_postalCode         AS shipmentOrigin__postalCode,
    c.shipmentOrigin_country            AS shipmentOrigin__country,
    c.shipmentDestination_addressLine1  AS shipmentDestination__addressLine1,
    c.shipmentDestination_addressLine2  AS shipmentDestination__addressLine2,
    c.shipmentDestination_city          AS shipmentDestination__city,
    c.shipmentDestination_stateProvince AS shipmentDestination__stateProvince,
    c.shipmentDestination_postalCode    AS shipmentDestination__postalCode,
    c.shipmentDestination_country       AS shipmentDestination__country,
 c.shipmentServiceLevel,
    c.shipmentServiceLevelCode,
 c.shipmentCarrierCode,
 c.shipmentCarrier,
 c.warehouseId,
    c.actualShipmentDateTime,
    c.shipmentPlaceDateTime,
    c.originalScheduledDeliveryDateTime,
    c.actualScheduledDeliveryDateTime
   ,c.isTemperatureTracked
   ,c.latestTemperature
   ,c.latestTemperatureInCelsius
   ,c.latestTemperatureInFahrenheit
   ,c.temperatureDateTime
   ,c.temperatureCity
   ,c.temperatureState
from c
join t in c.exception
WHERE c.AccountId = @DPProductLineKey
AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
and  c.FacilityId in (@warehouseId)
and (c.DateTimeReceived between @StartDate and  @EndDate )
and ((c.IS_TEMPERATURE = 'Y'? c.IS_TEMPERATURE : is_null(c.IS_TEMPERATURE)?'N':c.IS_TEMPERATURE) = @isTemperatureTracked)
and c.IS_INBOUND = 0 and c.is_deleted = 0 and c.exception != null
order by c.shipmentPlaceDateTime