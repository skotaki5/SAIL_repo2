--rpt_Shipment_Schedule

--Result Set 1

/*
Target Container - digital_summary_orders
*/

 Select 
c.upsShipmentNumber,
Max(c.m1_ActualDeliveryDateTime) actualDeliveryDateTime,
0=0? c.originalScheduledDeliveryDateTime: c.LoadLatestDeliveryDate OriginalScheduledDeliveryDateTime, -- case on IS_INBOUND param
0=0? c.originalScheduledDeliveryDateTime: c.LoadLatestDeliveryDate ActualScheduledDeliveryDateTime, -- case on IS_INBOUND param
Max(c.ma_shipmentEstimatedDateTime) shipmentEstimatedDateTime, -- to be taken from milestone activity
c.DateTimeReceived shipmentCreationDateTime,
c.DateTimeShipped actualShipmentDateTime,
c.shipmentPlaceDateTime,
c.originalPickupDateTime,  -- O.PickUPDateTime or c.PickUPDate?
c.DateTimeShipped actualPickupDateTime,
c.originalScheduledDeliveryDateTimeZone actualDeliveryDateTimeZone,
c.originalScheduledDeliveryDateTimeZone,
c.ActualScheduledDeliveryDateTimeZone,
c.originalScheduledDeliveryDateTimeZone shipmentEstimatedDateTimeZone,
c.shipmentCreateOnDateTimeZone shipmentCreationDateTimeZone
From c
WHERE c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529' 
AND c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'        
AND  c.UPSOrderNumber = '85982944'       
AND c.IS_INBOUND= 0 and c.is_deleted = 0
GROUP by 
c.upsShipmentNumber,
 0=0? c.originalScheduledDeliveryDateTime: c.LoadLatestDeliveryDate, -- case on IS_INBOUND param
 0=0? c.originalScheduledDeliveryDateTime: c.LoadLatestDeliveryDate , -- case on IS_INBOUND param
c.DateTimeReceived ,
c.DateTimeShipped,
c.shipmentPlaceDateTime,
c.originalPickupDateTime,  -- O.PickUPDateTime or c.PickUPDate?
c.DateTimeShipped ,
c.originalScheduledDeliveryDateTimeZone ,
c.originalScheduledDeliveryDateTimeZone,
c.ActualScheduledDeliveryDateTimeZone,
c.originalScheduledDeliveryDateTimeZone,
c.shipmentCreateOnDateTimeZone