--rpt_Shipment_Schedule

--Result Set 1

/*
NOTE:
complete/SET @IS_INBOUND = CASE WHEN @VarShipmentType='INBOUND' THEN 1
                          WHEN @VarShipmentType='MOVEMENT' THEN 2
                          WHEN @VarShipmentType='OUTBOUND' THEN 0 
                END
*/

--Result Set 1

/*
Parameter Requirement info -
->@DPProductLineKey  required 
->@DPServiceLineKey  optional
->@UPSOrderNumber required
->@ShipmentType optional
->@Is_Inbound - set based on @shipmentType

Target Container - digital_summary_orders
*/

Select 
c.upsShipmentNumber,
Max(c.m1_ActualDeliveryDateTime) actualDeliveryDateTime,
@IS_INBOUND=0? c.originalScheduledDeliveryDateTime: c.LoadLatestDeliveryDate OriginalScheduledDeliveryDateTime, -- case on IS_INBOUND param
@IS_INBOUND=0? c.originalScheduledDeliveryDateTime: c.LoadLatestDeliveryDate ActualScheduledDeliveryDateTime, -- case on IS_INBOUND param
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
WHERE c.AccountId = @DPProductLineKey 
AND c.DP_SERVICELINE_KEY = @DPServiceLineKey        
AND  c.UPSOrderNumber = @UPSOrderNumber       
AND c.IS_INBOUND=@IS_INBOUND and c.is_deleted = 0

GROUP by 
c.upsShipmentNumber,
@IS_INBOUND=0? c.originalScheduledDeliveryDateTime: c.LoadLatestDeliveryDate, -- case on IS_INBOUND param
@IS_INBOUND=0? c.originalScheduledDeliveryDateTime: c.LoadLatestDeliveryDate , -- case on IS_INBOUND param
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
