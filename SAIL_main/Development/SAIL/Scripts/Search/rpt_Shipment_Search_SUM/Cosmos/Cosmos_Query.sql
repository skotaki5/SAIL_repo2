-- rpt_Shipment_Search_SUM

/*
NOTE:
1.refer the store proc before implementing 
2.use the results from the above queries based on condition, in the below result queries.
(@UPSOrderNo will be coming from the above queries)
*/

--INITIAL QUERY

-- IF @varSearchBy = 'CARRIERSHIPMENTNUMBER'

/*
Target Container - digital_summary_orders
*/

SELECT DISTINCT c.UPSOrderNumber
FROM c
join t in c.TrackingNumber
WHERE  (c.AccountId in (@AccountKeys.DPProductLineKey) or c.AccountId = @DPProductLineKey)
and t.Tracking_Number = @varSearchValue and c.is_deleted=0

-- IF @varSearchBy='CUSTOMEROUTBOUNDHEADERREFERENCE'

/*
Target Container - digital_summary_orders
*/

select  distinct value c.UPSOrderNumber from c WHERE 
(c.AccountId in (@AccountKeys.DPProductLineKey) or c.AccountId = @DPProductLineKey)and
(c.referenceNumber1 = @varSearchValue
OR c.referenceNumber2 = @varSearchValue
OR c.referenceNumber3 = @varSearchValue
OR c.referenceNumber4 = @varSearchValue
OR c.referenceNumber5 = @varSearchValue)
and c.is_deleted=0

--  IF @varSearchBy='LINEREFERENCENUMBER'

/*
Target Container - digital_summary_order_lines
*/

 select distinct c.UpsOrderNumber from c WHERE 
(c.AccountId in (@AccountKeys.DPProductLineKey) or c.AccountId = @DPProductLineKey) and
(c.lineReferenceNumber1 = @varSearchValue
OR c.lineReferenceNumber2 = @varSearchValue
OR c.lineReferenceNumber3 = @varSearchValue
OR c.lineReferenceNumber4 = @varSearchValue
OR c.lineReferenceNumber5 = @varSearchValue)  --- all the lineReferenceNumber values are null in cosmos
and c.is_deleted=0


-- IF @varSearchBy='LPN'

/*
Target Container - digital_summary_order_lines
*/

SELECT DISTINCT c.UpsOrderNumber
FROM c
join t in c.OrderLineVendorDetail_search.LPN
WHERE  (c.AccountId in (@AccountKeys.DPProductLineKey) or c.AccountId = @DPProductLineKey)
and t = @varSearchValue and c.is_deleted=0 --data not available in ssms

-- IF @varSearchBy='VSN'

/*
Target Container - digital_summary_order_lines
*/

SELECT DISTINCT c.UpsOrderNumber
FROM c
join t in c.OrderLineVendorDetail_search.VSN
WHERE  (c.AccountId in (@AccountKeys.DPProductLineKey) or c.AccountId = @DPProductLineKey)
and t = @varSearchValue and c.is_deleted=0

-- IF @varSearchBy='VCL'

/*
Target Container - digital_summary_order_lines
*/

SELECT DISTINCT c.UpsOrderNumber
FROM c
join t in c.OrderLineVendorDetail_search.VCL
WHERE  (c.AccountId in (@AccountKeys.DPProductLineKey) or c.AccountId = @DPProductLineKey)
---and t = @varSearchValue  VCL is empty for all records for this accountid
and c.is_deleted=0

-- IF @varSearchBy='DESIGNATOR'

/*
Target Container - digital_summary_order_lines
*/

SELECT DISTINCT c.UpsOrderNumber
FROM c
join t in c.OrderLineVendorDetail_search.Designator
WHERE (c.AccountId in (@AccountKeys.DPProductLineKey) or c.AccountId = @DPProductLineKey)
and t = @varSearchValue and c.is_deleted =0

-- IF @varSearchBy ='SHIPMENTBATCHNUMBER'

/*
Target Container - digital_summary_order_lines
*/

SELECT DISTINCT c.UpsOrderNumber
FROM c
join t in c.OrderLineVendorDetail_search.VCL
WHERE  (c.AccountId in (@AccountKeys.DPProductLineKey) or c.AccountId = @DPProductLineKey)
---and t = @varSearchValue  VCL is empty for all records for this accountid
and c.is_deleted =0



-- IF @varSearchBy='SHIPMENTSERIALNUMBERWAREHOUSE'

/*
Target Container - digital_summary_order_lines
*/

SELECT DISTINCT c.UpsOrderNumber
FROM c
join t in c.OrderLineVendorDetail_search.VSN
WHERE  (c.AccountId in (@AccountKeys.DPProductLineKey) or c.AccountId = @DPProductLineKey)
and t = @varSearchValue and c.is_deleted=0

-- ***********************************************************************************************************************************

--Result Set 1

/*
Target Container - digital_summary_orders
*/

  Select 
c.shipmentNumber,
c.referenceNumber,
c.upsShipmentNumber,
c.clientShipmentNumber,
c.customerPONumber,
c.UPSOrderNumber orderNumber,
c.upsTransportShipmentNumber,
c.gffShipmentInstanceId,
c.gffShipmentNumber,
c.orderLinesCount,-- not found
c.shipmentDescription,
'' shipmentCategory,
c.warehouseId,
c.warehouseCode,
c.shipmentServiceLevel,
c.shipmentServiceLevelCode,
c.shipmentCarrierCode,
c.shipmentCarrier,
c.shipmentBookedOnDateTime,
c.shipmentCanceledDateTime,
c.shipmentCanceledReason,
c.actualShipmentDateTime,
c.shipmentCreateOnDateTime,
c.originalScheduledDeliveryDateTime,
c.ActualDeliveryDate actualDeliveryDateTime, -- null value not null in sql
c.inventoryShipmentStatus,
c.shipmentType,
c.primaryException,
c.transportationMileStone,
c.shipmentOrigin_addressLine1,
c.shipmentOrigin_addressLine2,
c.shipmentOrigin_city,
c.shipmentOrigin_stateProvince,
c.shipmentOrigin_postalCode,
c.shipmentOrigin_country,
c.consignee shipmentDestination_consignee,
c.shipmentDestination_addressLine1,
c.shipmentDestination_addressLine2,
c.shipmentDestination_city,
c.shipmentDestination_stateProvince,
c.shipmentDestination_postalCode,
c.shipmentDestination_country,
c.milestoneStatus,
c.Accountnumber,
c.AccountId as dpProductLineKey from c
  
  WHERE (c.AccountId in (@AccountKeys.DPProductLineKey) or c.AccountId = @DPProductLineKey)
  AND (c.DP_SERVICELINE_KEY in (@AccountKeys.DPServiceLineKey) or c.DP_SERVICELINE_KEY = @DPServiceLineKey)
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) -- IF @varSearchBy = 'CARRIERSHIPMENTNUMBER'
  AND  c.orderNumber in (@varSearchValue)  -- IF @varSearchBy = 'REFERENCENUMBER'
  AND  c.UPSOrderNumber in (@UPSOrderNo) --IF @varSearchBy = 'UPSSHIPMENTNUMBER'
  AND  c.customerPONumber in (@varSearchValue) --IF @varSearchBy = 'CUSTOMERPONUMBER'
  AND  c.orderNumber in (@varSearchValue)  --IF @varSearchBy = 'CLIENTASNNUMBER'
  AND ( c.UPSOrderNumber = @varSearchValue  AND c.IS_INBOUND=1) --IF @varSearchBy = 'UPSASNNUMBER'
 -- AND ( c.shipment_referenceType.Type='Serial Number'and c.shipment_referenceType.Value =@varSearchValue  ) --as it is null--IF @varSearchBy='SHIPMENTSERIALNUMBER'
 -- AND ( c.shipment_referenceType.Type='Lot Number' and c.shipment_referenceType.Value =@varSearchValue ) --as it is null IF @varSearchBy='SHIPMENTLOTNUMBER'
 -- AND ( c.shipment_referenceType.Type='Hold Status'and c.shipment_referenceType.Value =@varSearchValue )  --IF @varSearchBy='HOLDSTATUS'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='CUSTOMEROUTBOUNDHEADERREFERENCE'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) -- IF @varSearchBy='LINEREFERENCENUMBER'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='LPN'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='VSN'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='VCL'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='DESIGNATOR'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy ='SHIPMENTBATCHNUMBER'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='SHIPMENTSERIALNUMBERWAREHOUSE'
  AND ((c.IS_INBOUND=null? 0 : c.IS_INBOUND ) = @IS_INBOUND)
  And c.is_deleted =0

--Result Set 2

/*
Target Container - digital_summary_orders
*/

select count(1) totalCount from c 
  
    WHERE (c.AccountId in (@AccountKeys.DPProductLineKey) or c.AccountId = @DPProductLineKey)
  AND (c.DP_SERVICELINE_KEY in (@AccountKeys.DPServiceLineKey) or c.DP_SERVICELINE_KEY = @DPServiceLineKey)
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) -- IF @varSearchBy = 'CARRIERSHIPMENTNUMBER'
  AND  c.orderNumber in (@varSearchValue)  -- IF @varSearchBy = 'REFERENCENUMBER'
  AND  c.UPSOrderNumber in (@UPSOrderNo) --IF @varSearchBy = 'UPSSHIPMENTNUMBER'
  AND  c.customerPONumber in (@varSearchValue) --IF @varSearchBy = 'CUSTOMERPONUMBER'
  AND  c.orderNumber in (@varSearchValue)  --IF @varSearchBy = 'CLIENTASNNUMBER'
  AND ( c.UPSOrderNumber = @varSearchValue  AND c.IS_INBOUND=1) --IF @varSearchBy = 'UPSASNNUMBER'
 -- AND ( c.shipment_referenceType.Type='Serial Number'and c.shipment_referenceType.Value =@varSearchValue  ) --as it is null--IF @varSearchBy='SHIPMENTSERIALNUMBER'
 -- AND ( c.shipment_referenceType.Type='Lot Number' and c.shipment_referenceType.Value =@varSearchValue ) --as it is null IF @varSearchBy='SHIPMENTLOTNUMBER'
 -- AND ( c.shipment_referenceType.Type='Hold Status'and c.shipment_referenceType.Value =@varSearchValue )  --IF @varSearchBy='HOLDSTATUS'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='CUSTOMEROUTBOUNDHEADERREFERENCE'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) -- IF @varSearchBy='LINEREFERENCENUMBER'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='LPN'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='VSN'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='VCL'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='DESIGNATOR'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy ='SHIPMENTBATCHNUMBER'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='SHIPMENTSERIALNUMBERWAREHOUSE'
  AND ((c.IS_INBOUND=null? 0 : c.IS_INBOUND ) = @IS_INBOUND)
  And c.is_deleted =0

--Result Set 3

/*
Target Container - digital_summary_order_lines
*/

SELECT
c.UpsOrderNumber upsShipmentNumber,
c.ClientASNNumber clientASNNumber,
c.SourceSystemKey = 1002?  c.asnNumber : (IS_NULL(c.ReceiptNumber)?c.asnNumber:c.ReceiptNumber)  AS upsASNNumber
From c
  WHERE (c.AccountId in (@AccountKeys.DPProductLineKey) or c.AccountId = @DPProductLineKey)
  AND (c.DP_SERVICELINE_KEY in (@AccountKeys.DPServiceLineKey) or c.DP_SERVICELINE_KEY = @DPServiceLineKey)
  AND  c.UpsOrderNumber     in  (@UPSOrderNo) -- IF @varSearchBy = 'CARRIERSHIPMENTNUMBER'
  AND  Exists(select t from t in c.OrderNumber where t in(@varSearchValue))  -- IF @varSearchBy = 'REFERENCENUMBER'
  AND  c.UpsOrderNumber in (@UPSOrderNo) --IF @varSearchBy = 'UPSSHIPMENTNUMBER'
  AND  c.customerPONumber in (@varSearchValue) --IF @varSearchBy = 'CUSTOMERPONUMBER'
  AND Exists(select t from t in c.OrderNumber where t in (@varSearchValue))  --IF @varSearchBy = 'CLIENTASNNUMBER'
  AND ( c.UpsOrderNumber = @varSearchValue  AND c.IS_INBOUND=1) --IF @varSearchBy = 'UPSASNNUMBER'
 -- AND ( c.shipment_referenceType.Type='Serial Number'and c.shipment_referenceType.Value =@varSearchValue  ) --as it is null--IF @varSearchBy='SHIPMENTSERIALNUMBER'
 -- AND ( c.shipment_referenceType.Type='Lot Number' and c.shipment_referenceType.Value =@varSearchValue ) --as it is null IF @varSearchBy='SHIPMENTLOTNUMBER'
 -- AND ( c.shipment_referenceType.Type='Hold Status'and c.shipment_referenceType.Value =@varSearchValue )  --IF @varSearchBy='HOLDSTATUS'
  AND  c.UpsOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='CUSTOMEROUTBOUNDHEADERREFERENCE'
  AND  c.UpsOrderNumber     in  (@UPSOrderNo) -- IF @varSearchBy='LINEREFERENCENUMBER'
  AND  c.UpsOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='LPN'
  AND  c.UpsOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='VSN'
  AND  c.UpsOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='VCL'
  AND  c.UpsOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='DESIGNATOR'
  AND  c.UpsOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy ='SHIPMENTBATCHNUMBER'
  AND  c.UpsOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='SHIPMENTSERIALNUMBERWAREHOUSE'
  AND ((c.IS_INBOUND=null? 0 : c.IS_INBOUND ) = @IS_INBOUND)
  And c.is_deleted =0
  GROUP BY c.UpsOrderNumber,c.ClientASNNumber,c.ReceiptNumber,c.SourceSystemKey,c.asnNumber

--Result Set 4

/*
Target Container - digital_summary_orders
*/

SELECT
 a.upsShipmentNumber,
 t.TrackingNumber trackingNumber,
 t.CarrierCode carrierCode,
 t.CarrierType carrierType
 from
 (Select 
c.upsShipmentNumber upsShipmentNumber,
c.TrackingNumber = null ? Array(select null TrackingNumber, null CarrierCode, null CarrierType) : c.TrackingNumber TrackingNumber
 From c
  WHERE (c.AccountId in (@AccountKeys.DPProductLineKey) or c.AccountId = @DPProductLineKey)
  AND (c.DP_SERVICELINE_KEY in (@AccountKeys.DPServiceLineKey) or c.DP_SERVICELINE_KEY = @DPServiceLineKey)
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) -- IF @varSearchBy = 'CARRIERSHIPMENTNUMBER'
  AND  c.orderNumber in (@varSearchValue)  -- IF @varSearchBy = 'REFERENCENUMBER'
  AND  c.UPSOrderNumber in (@UPSOrderNo) --IF @varSearchBy = 'UPSSHIPMENTNUMBER'
  AND  c.customerPONumber in (@varSearchValue) --IF @varSearchBy = 'CUSTOMERPONUMBER'
  AND  c.orderNumber in (@varSearchValue)  --IF @varSearchBy = 'CLIENTASNNUMBER'
  AND ( c.UPSOrderNumber = @varSearchValue  AND c.IS_INBOUND=1) --IF @varSearchBy = 'UPSASNNUMBER'
 -- AND ( c.shipment_referenceType.Type='Serial Number'and c.shipment_referenceType.Value =@varSearchValue  ) --as it is null--IF @varSearchBy='SHIPMENTSERIALNUMBER'
 -- AND ( c.shipment_referenceType.Type='Lot Number' and c.shipment_referenceType.Value =@varSearchValue ) --as it is null IF @varSearchBy='SHIPMENTLOTNUMBER'
 -- AND ( c.shipment_referenceType.Type='Hold Status'and c.shipment_referenceType.Value =@varSearchValue )  --IF @varSearchBy='HOLDSTATUS'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='CUSTOMEROUTBOUNDHEADERREFERENCE'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) -- IF @varSearchBy='LINEREFERENCENUMBER'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='LPN'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='VSN'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='VCL'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='DESIGNATOR'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy ='SHIPMENTBATCHNUMBER'
  AND  c.UPSOrderNumber     in  (@UPSOrderNo) --IF @varSearchBy='SHIPMENTSERIALNUMBERWAREHOUSE'
  And c.is_deleted =0 AND ((c.IS_INBOUND=null? 0 : c.IS_INBOUND ) = 0)
  ) a
 join t in a.TrackingNumber