--LateDeliveryExceptions

--Result Set 1

/*
Target Container - digital_summary_orders
*/

select
c.UPSOrderNumber UpsShipmentNumber,
c.orderNumber ClientOrderNumber  ,
t.exceptionReason ExceptionReason,
is_null(c.DateTimeShipped)?null:c.DateTimeShipped OrderShippedOriginDateTime,
c.shipmentOrigin_addressLine1 OriginAddressLine1,
c.shipmentOrigin_addressLine2 OriginAddressLine2,
c.OriginCity OriginCity,
c.shipmentOrigin_stateProvince OriginStateProvince,
c.shipmentOrigin_postalCode OriginPostalCode,
c.OriginCountry OriginCountry,
c.shipmentDestination_addressLine1 DestinationAddressLine1,
 c.shipmentDestination_addressLine2 DestinationAddressLine2,
 c.DestinationCity DestinationCity,
c.shipmentDestination_stateProvince DestinationStateProvince,
c.shipmentDestination_postalCode DestinationPostalCode,
c.DestinationCountry DestinationCountry,
c.ServiceLevel ServiceName,
c.milestoneStatus Status,
is_null( c.actualScheduledDeliveryDateTime)?null:c.actualScheduledDeliveryDateTime ScheduledDeliveryDestinationDateTime, 
c.shippedDateTimeZone OrderShippedOriginDateTimeZone,
c.ActualScheduledDeliveryDateTimeZone SheduledDeliveryDestinationDateTimeZone,
c.LocationCode,
c.Accountnumber,
c.AccountId DpProductLineKey
from c
join t in c.exception_list
where (is_null(c.DateTimeReceived)?true:c.DateTimeReceived between __start_0 and @__end_1)
and
 (c.AccountId != null and c.AccountId in (@__accountId))
-- and (c.FacilityId != null and c.FacilityId in ("A2B979C6-7FA9-40DE-A648-08D6175D2695"))
-- and (c.DP_SERVICELINE_KEY != null and c.DP_SERVICELINE_KEY in ("8F9893AA-7D54-4DAD-B16A-31EEFFF27833"))
and c.exception_list != null
and c.IS_INBOUND =0
and t.ExceptionPrimaryIndicator = '1'
and (t.ExceptionCategory != null and t.ExceptionCategory != "Save")
and c.ActivityCodeDelivery_status = 1
and c.is_deleted =0
order by c.DateTimeReceived desc