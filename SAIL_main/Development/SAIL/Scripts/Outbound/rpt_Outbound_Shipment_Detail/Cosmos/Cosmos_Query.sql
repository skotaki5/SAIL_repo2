--AUTHOR : 		Mahesh Rathi
--DESCRIPTION:	rpt_Outbound_Shipment_Detail
--DATE : 		01-04-2022


Result set 1a:

--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @DPProductLineKey and @UPSOrderNumber required,
-- @DPServiceLineKey  optional

select
b.upsShipmentNumber,
b.clientShipmentNumber,
b.shipmentType,
b.templateType,
b.shipmentNumber,
b.referenceNumber,
b.customerPONumber,
b.orderNumber,
b.upsTransportShipmentNumber,
b.gffShipmentInstanceId,
b.gffShipmentNumber,
b.shipmentCarrier,
b.shipmentCarrierCode,
b.shipmentServiceLevel,
b.shipmentServiceLevelCode,
b.ServiceMode,
b.warehouseId,
b.warehouseCode,
b.primaryException,
b.shipmentPlaceDateTime,
b.shipmentCanceledDateTime,
b.shipmentCanceledReason,
b.actualShipmentDateTime,
b.shipmentCreateDateTime,
b.originalScheduledDeliveryDateTime,
b.actualDeliveryDateTime,
b.TrackingNumber,
b.shipmentOrigin_addressLine1,
b.shipmentOrigin_addressLine2,
b.shipmentOrigin_city,
b.shipmentOrigin_stateProvince,
b.shipmentOrigin_postalCode,
b.shipmentOrigin_country,
b.shipmentOrigin_port_addressLine1,
b.shipmentOrigin_port_addressLine2,
b.shipmentOrigin_port_city,
b.shipmentOrigin_port_stateProvince,
b.shipmentOrigin_port_country,
b.shipmentOrigin_port_postalCode,
b.shipmentOrigin_port_phoneNumber,
b.shipmentDestination_addressLine1,
b.shipmentDestination_addressLine2,
b.shipmentDestination_city,
b.shipmentDestination_stateProvince,
b.shipmentDestination_country,
b.shipmentDestination_postalCode,
b.shipmentDestination_port_addressLine1,
b.shipmentDestination_port_addressLine2,
b.shipmentDestination_port_city,
b.shipmentDestination_port_stateProvince,
b.shipmentDestination_port_postalCode,
b.shipmentDestination_port_country,
b.shipmentDestination_port_phoneNumber,
b.shipmentNotes_dateTime,
b.shipmentNotes_description,
b.ProofofDelivery_Name,
b.consignee,
b.milestoneStatus,
b.Accountnumber,
b.lastKnownLocation,
b.Partially_Cancelled,
b.referenceNumber1,
b.referenceNumber2,
b.referenceNumber3,
b.referenceNumber4,
b.referenceNumber5,
b.originLocationCode,
b.ShipmentDestination_destinationLocationCode,
b.authorizorName,
t.exceptionReason qcReasonCode,
b.waybill_airbill_Number,
b.deliveryInstructions,
b.shipmentPlaceDateTimeZone,
b.shipmentCanceledDateTimeZone,
b.shipmentCreateDateTimeZone,
b.estimatedDeliveryDateTime,
b.expectedShipByDateTime,
b.proofOfDelivery_DateTime,  --sprint 52
b.orderType,    --sprint 52
b.temperatureThresholdMin, --sprint 52
b.temperatureThresholdMax, --sprint 52
b.temperatureThresholdUOM  --sprint 52
FROM
(select
c.upsShipmentNumber,
c.clientShipmentNumber,
c.shipmentType,
c.templateType,
c.shipmentNumber,
c.referenceNumber,
c.customerPONumber,
c.orderNumber,
c.upsTransportShipmentNumber,
c.gffShipmentInstanceId,
c.gffShipmentNumber,
c.shipmentCarrier,
c.shipmentCarrierCode,
c.shipmentServiceLevel,
c.shipmentServiceLevelCode,
c.ServiceMode,
c.warehouseId,
c.warehouseCode,
c.primaryException,
c.shipmentPlaceDateTime,
c.shipmentCanceledDateTime,
c.shipmentCanceledReason,
c.DateTimeShipped actualShipmentDateTime,
c.shipmentCreateDateTime,
c.originalScheduledDeliveryDateTime,
c.ActualDeliveryDate actualDeliveryDateTime,
Array(Select d.ShipmentDimensions,d.ShipmentWeight,d.Tracking_Number,
d.CarrierCode,d.CarrierType = ""? null : d.CarrierType CarrierType,d.carrierName,d.ShipmentDimensions_UOM = ""? null:d.ShipmentDimensions_UOM ShipmentDimensions_UOM,d.ShipmentWeight_UOM = "" ? null: d.ShipmentWeight_UOM ShipmentWeight_UOM from d in c.TrackingNumber) as TrackingNumber, --Sprint 52
c.shipmentOrigin_addressLine1,
c.shipmentOrigin_addressLine2,
c.shipmentOrigin_city,
c.shipmentOrigin_stateProvince,
c.shipmentOrigin_postalCode,
c.shipmentOrigin_country,
'' shipmentOrigin_port_addressLine1,
'' shipmentOrigin_port_addressLine2,
'' shipmentOrigin_port_city,
'' shipmentOrigin_port_stateProvince,
'' shipmentOrigin_port_country,
'' shipmentOrigin_port_postalCode,
'' shipmentOrigin_port_phoneNumber,
c.shipmentDestination_addressLine1,
c.shipmentDestination_addressLine2,
c.shipmentDestination_city,
c.shipmentDestination_stateProvince,
c.shipmentDestination_country,
c.shipmentDestination_postalCode,
'' shipmentDestination_port_addressLine1,
'' shipmentDestination_port_addressLine2,
'' shipmentDestination_port_city,
'' shipmentDestination_port_stateProvince,
'' shipmentDestination_port_postalCode,
'' shipmentDestination_port_country,
'' shipmentDestination_port_phoneNumber,
'' shipmentNotes_dateTime,
'' shipmentNotes_description,
c.ProofofDelivery_Name,
c.consignee,
c.milestoneStatus,
c.Accountnumber,
c.lastKnownLocation,
(c.ShipmentLineCanceledFlag='Y'?c.ShipmentLineCanceledFlag : null) Partially_Cancelled,
c.referenceNumber1,
c.referenceNumber2,
c.referenceNumber3,
c.referenceNumber4,
c.referenceNumber5,
c.originLocationCode,
c.ShipmentDestination_destinationLocationCode,
c.authorizorName,
c.exception2 = null ? Array(select null exceptionReason,null exceptionType) : c.exception2 exception2,
c.Track_num waybill_airbill_Number,
c.deliveryInstructions,
c.shipmentPlaceDateTimeZone,
c.shipmentCanceledDateTimeZone,
c.shipmentCreateDateTimeZone,
c.estimatedDeliveryDateTime, --sprint 52
c.expectedShipByDateTime,
c.proof_of_delivery_date_time proofOfDelivery_DateTime, --sprint 52
c.orderType,  --sprint 52
c.temperatureThresholdMin, --sprint 52
c.temperatureThresholdMax,  --sprint 52
c.temperatureThresholdUOM  --sprint 52
FROM c 
WHERE c.AccountId = @DPProductLineKey
AND c.DP_SERVICELINE_KEY = @DPServiceLineKey     
AND  c.UPSOrderNumber = @UPSOrderNumber     
AND c.IS_INBOUND=0 and c.is_deleted=0
) b
join t in b.exception2


Result set 1b:

--* Target Container-digital_summary_order_lines

--* Parameter requirement info.
-- @DPProductLineKey and @UPSOrderNumber required,
-- @DPServiceLineKey  optional

Select 
c.LineNumber                    ,
c.SKU                           ,
c.SKUDescription                ,
c.SKUQuantity                   ,
c.SKUShippedQuantity            ,
c.SKUWeight                     ,
c.SKUDimensions                 ,
c.SKUWeight_UOM                 ,
c.SKUDimensions_UOM             ,
c.lineCancelledReason           ,
c.lineCancelledDateTime         ,
 c.lineCancelledDateTimeZone     ,
c.lineReferenceNumber1          ,
c.lineReferenceNumber2          ,
c.lineReferenceNumber3          ,
c.lineReferenceNumber4          ,
c.lineReferenceNumber5          ,
c.OrderLineVendorDetail.VSN        ,
c.OrderLineVendorDetail.VCL,
c.OrderLineVendorDetail.LPN,
c.OrderLineVendorDetail.Designator, 
(ol.itemNumber = c.SKU) ? ol.vendorLotNumber : null batchNumber,  --sprint 52
(ol.itemNumber = c.SKU) ? ol.vendorSerialNumber : null serialNumber,--sprint 52
(ol.itemNumber = c.SKU) ? ol.expirationDate : null expirationDate     --sprint 52
 from c
 join ol in c.OrderLineDetail
WHERE c.AccountId = @DPProductLineKey 
AND c.DP_SERVICELINE_KEY = @DPServiceLineKey      
AND  c.UPSOrderNumber = @UPSOrderNumber      
AND c.IS_INBOUND=0 and c.is_deleted=0


Result set 2:

--* Target Container-digital_summary_transportation_callcheck

--* Parameter requirement info.
-- @DPProductLineKey and @UPSOrderNumber required,
-- @DPServiceLineKey  optional

select 
c.TemperatureValue,
c.TemperatureDateTime,
c.TemperatureCity,
c.TemperatureState,
c.TemperatureCountry,
c.TemperatureInCelsius,
c.TemperatureInFahrenheit,
c.battery,
c.humidity,
c.light,
c.shock,
c.Latitude AS temperatureLatitude  --sprint 52
,c.Longitude AS temperatureLongitude --sprint 52
from c
WHERE 
WHERE c.AccountId = @DPProductLineKey 
AND c.DP_SERVICELINE_KEY = @DPServiceLineKey      
AND  c.UPSOrderNumber = @UPSOrderNumber      
AND c.IS_INBOUND=0 and c.is_deleted=0


Result set 3:

--* Target Container-digital_summary_transportation_rates_charges

--* Parameter requirement info.
-- @DPProductLineKey and @UPSOrderNumber required,
-- @DPServiceLineKey  optiona

Select 
t.totalCustomerCharge,
t.totalCustomerChargeCurrency,
t.invoiceDateTime,
t.CostBreakdown
from t
WHERE c.AccountId = @DPProductLineKey 
AND c.DP_SERVICELINE_KEY = @DPServiceLineKey      
AND  c.UpsOrderNumber = @UPSOrderNumber      
AND c.is_inbound=0 and c.is_deleted=0

Result set 4:
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @DPProductLineKey and @UPSOrderNumber required,
-- @DPServiceLineKey  optional


SELECT 
c.claimType,
c.claimAmount,
c.claimAmountCurrency,
c.claimFilingDateTime,
c.claimStatus,
c.claimClosureDateTime,
c.claimAmountPaid,
c.claimAmountPaidCurrency
FROM c
WHERE c.AccountId = @DPProductLineKey 
AND c.DP_SERVICELINE_KEY = @DPServiceLineKey      
AND  c.UPSOrderNumber = @UPSOrderNumber      
AND c.IS_INBOUND=0 and c.is_deleted=0