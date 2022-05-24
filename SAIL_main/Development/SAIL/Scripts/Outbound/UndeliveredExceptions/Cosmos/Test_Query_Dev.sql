--UndeliveredExceptions

--Result Set 1

/*
Target Container - digital_summary_orders
*/

select 
c.UPSOrderNumber UpsShipmentNumber,
c.orderNumber ClientOrderNumber  ,
t.exceptionReason ExceptionReason,
is_null(c.DateTimeShipped)?null:c.DateTimeShipped OrderShippedOriginDateTime,
c.shippedDateTimeZone ShippedDateTimeZone ,
c.ActualScheduledDeliveryDateTimeZone ActualScheduledDeliveryDateTimeZone ,
c.shipmentOrigin_addressLine1 OriginAddressLine1,
c.shipmentOrigin_addressLine2 OriginAddressLine2,
c.OriginCity OriginCity,
c.shipmentOrigin_stateProvince OriginStateProvince,
c.shipmentOrigin_postalCode OriginPostalCode,
c.OriginCountry OriginCountry,
c.warehouseCode WAREHOUSE_CODE,
c.shipmentDestination_addressLine1 DestinationAddressLine1,
 c.shipmentDestination_addressLine2 DestinationAddressLine2,
 c.DestinationCity DestinationCity,
c.shipmentDestination_stateProvince DestinationStateProvince,
c.shipmentDestination_postalCode DestinationPostalCode,
c.DestinationCountry DestinationCountry,
c.ServiceLevel Service,
c.milestonestatus Status,
is_null( c.actualScheduledDeliveryDateTime)?null:c.actualScheduledDeliveryDateTime ScheduledDeliveryDestinationDateTime,
c.Accountnumber AccountNumber,
 c.AccountId DpProductLineKey
 
 from c 
 join t in c.exception_list
WHERE (is_null(c.DateTimeReceived)?true:c.DateTimeReceived between '2021-11-01 00:00:00.000' and '2022-02-11 00:00:00.000')
and
  (c.AccountId != null and c.AccountId in ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529' ))
-- and (c.FacilityId != null and c.FacilityId in ('A2B979C6-7FA9-40DE-A648-08D6175D2695'))
-- and (c.DP_SERVICELINE_KEY != null and c.DP_SERVICELINE_KEY in ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'))
and c.exception_list != null
and c.OrderCancelledFlag = "N"
and c.IS_INBOUND =0
and t.ExceptionPrimaryIndicator = '1'
and (t.ExceptionCategory != null and t.ExceptionCategory != "Save")
and c.milestoneStatus !='DELIVERED'  
and c.is_deleted =0 
order by c.DateTimeReceived desc