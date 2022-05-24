--AUTHOR :Mahesh Rathi
--DESCRIPITION : rpt_Inbound_Shipment_Detail
--DATE : 18-02-2022

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

SELECT COUNT(1) totalCount FROM c 
WHERE 
c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
and c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'          
AND c.IS_INBOUND=0 and c.is_deleted = 0  and c.FacilityId in ("FC4B9B8B-E15F-4D40-899E-169D301ADC75")
  AND (((c.DateTimeCancelled BETWEEN  '2021-11-01 00:00:00.000' AND   DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2021-12-31 00:00:00.000'))) 
  and c.OrderCancelledFlag = 'Y' ) 
  or ((c.ShipmentLineCanceledDate  BETWEEN  '2021-11-01 00:00:00.000' AND   DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2021-12-31 00:00:00.000')) )
 and (c.ShipmentLineCanceledFlag = null ? 'Y' : c.ShipmentLineCanceledFlag) = 'Y'))
 -- AND  (c.DateTimeCancelled BETWEEN  '2020-11-01 00:00:00.000' AND   DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2021-12-31 00:00:00.000')))  
 -- uncomment if using  paramenter @shipmentCanceledOnEndDate in stored proc call
 
Result set 2:

IF topRow = 0:

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
WHERE c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529' 
AND c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'
and  c.FacilityId in ('FC4B9B8B-E15F-4D40-899E-169D301ADC75') 
and (c.DateTimeReceived between '2021-10-01 00:00:00.000' and DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, "2022-02-01 00:00:00.000") ))
and (((c.IS_TEMPERATURE = 'Y'? c.IS_TEMPERATURE : is_null(c.IS_TEMPERATURE)?'N':c.IS_TEMPERATURE) = 'N'))
and c.IS_INBOUND = 0 and c.is_deleted = 0 and c.exception != null
order by c.shipmentPlaceDateTime

IF topRow > 0:

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

select top 100
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
WHERE c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529' 
AND c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'
and  c.FacilityId in ('FC4B9B8B-E15F-4D40-899E-169D301ADC75') 
and (c.DateTimeReceived between '2021-10-01 00:00:00.000' and DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, "2022-02-01 00:00:00.000") ))
and (((c.IS_TEMPERATURE = 'Y'? c.IS_TEMPERATURE : is_null(c.IS_TEMPERATURE)?'N':c.IS_TEMPERATURE) = 'N'))
and c.IS_INBOUND = 0 and c.is_deleted = 0 and c.exception != null
order by c.shipmentPlaceDateTime