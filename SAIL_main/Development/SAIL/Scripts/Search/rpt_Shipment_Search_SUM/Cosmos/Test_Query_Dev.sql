-- rpt_Shipment_Search_SUM

--INITIAL QUERY

-- IF @varSearchBy = 'CARRIERSHIPMENTNUMBER'

/*
Target Container - digital_summary_orders
*/

SELECT DISTINCT c.UPSOrderNumber
FROM c
join t in c.TrackingNumber
WHERE  ( c.AccountId IN ("1EEF1B1A-A415-43F3-88C5-2D5EBC503529")) 
and t.Tracking_Number = '775120688030' and c.is_deleted=0

-- IF @varSearchBy='CUSTOMEROUTBOUNDHEADERREFERENCE'

/*
Target Container - digital_summary_orders
*/

select  distinct value c.UPSOrderNumber from c WHERE 
c.AccountId IN ("1EEF1B1A-A415-43F3-88C5-2D5EBC503529") and
(c.referenceNumber1 = 'REPLENISHMENT'
OR c.referenceNumber2 = 'REPLENISHMENT'
OR c.referenceNumber3 = 'REPLENISHMENT' 
OR c.referenceNumber4 = 'REPLENISHMENT'
OR c.referenceNumber5 = 'REPLENISHMENT')
and c.is_deleted=0

--  IF @varSearchBy='LINEREFERENCENUMBER'

/*
Target Container - digital_summary_order_lines
*/

 select distinct c.UpsOrderNumber from c WHERE 
c.AccountId IN ("1EEF1B1A-A415-43F3-88C5-2D5EBC503529") and
(c.lineReferenceNumber1 = null
OR c.lineReferenceNumber2 = null
OR c.lineReferenceNumber3 = null
OR c.lineReferenceNumber4 = null
OR c.lineReferenceNumber5 = null)  --- all the lineReferenceNumber values are null in cosmos
and c.is_deleted=0

-- IF @varSearchBy='LPN'

/*
Target Container - digital_summary_order_lines
*/

SELECT DISTINCT c.UpsOrderNumber
FROM c
join t in c.OrderLineVendorDetail_search.LPN
WHERE  c.AccountId IN ("1EEF1B1A-A415-43F3-88C5-2D5EBC503529")
and t = '712037161' and c.is_deleted=0 --data not available in ssms

-- IF @varSearchBy='VSN'

/*
Target Container - digital_summary_order_lines
*/

SELECT DISTINCT c.UpsOrderNumber
FROM c
join t in c.OrderLineVendorDetail_search.VSN
WHERE  c.AccountId IN ("1EEF1B1A-A415-43F3-88C5-2D5EBC503529")
and t = 'M978491C' and c.is_deleted=0

-- IF @varSearchBy='VCL'

/*
Target Container - digital_summary_order_lines
*/

SELECT DISTINCT c.UpsOrderNumber
FROM c
join t in c.OrderLineVendorDetail_search.VCL
WHERE  c.AccountId IN ("1EEF1B1A-A415-43F3-88C5-2D5EBC503529")
---and t = 'CN'  VCL is empty for all records for this accountid
and c.is_deleted=0

-- IF @varSearchBy='DESIGNATOR'

/*
Target Container - digital_summary_order_lines
*/

SELECT DISTINCT c.UpsOrderNumber
FROM c
join t in c.OrderLineVendorDetail_search.Designator
WHERE  c.AccountId IN ("1EEF1B1A-A415-43F3-88C5-2D5EBC503529")
and t = 'B_RMA' and c.is_deleted =0

-- IF @varSearchBy ='SHIPMENTBATCHNUMBER'

/*
Target Container - digital_summary_order_lines
*/

SELECT DISTINCT c.UpsOrderNumber
FROM c
join t in c.OrderLineVendorDetail_search.VCL
WHERE  c.AccountId IN ("1EEF1B1A-A415-43F3-88C5-2D5EBC503529")
---and t = 'CN'  VCL is empty for all records for this accountid
and c.is_deleted =0


-- IF @varSearchBy='SHIPMENTSERIALNUMBERWAREHOUSE'

/*
Target Container - digital_summary_order_lines
*/

SELECT DISTINCT c.UpsOrderNumber
FROM c
join t in c.OrderLineVendorDetail_search.VSN
WHERE  c.AccountId IN ("1EEF1B1A-A415-43F3-88C5-2D5EBC503529")
and t = 'M978491C' and c.is_deleted=0

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

WHERE c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529') 
  AND c.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
--   AND  c.UPSOrderNumber     in  ('84614823') -- IF @varSearchBy = 'CARRIERSHIPMENTNUMBER'
--   AND  c.orderNumber in ('E211101-107')  -- IF @varSearchBy = 'REFERENCENUMBER'
--   AND  c.UPSOrderNumber in ('84614823') --IF @varSearchBy = 'UPSSHIPMENTNUMBER'
--   AND  c.customerPONumber in ('1443887') --IF @varSearchBy = 'CUSTOMERPONUMBER'
--   AND  c.orderNumber in ('E211101-107')  --IF @varSearchBy = 'CLIENTASNNUMBER'
--   AND ( c.UPSOrderNumber = '43545081'  AND c.IS_INBOUND=1) --IF @varSearchBy = 'UPSASNNUMBER'
--  AND ( c.shipment_referenceType.Type='Serial Number'and c.shipment_referenceType.Value ='') --as it is null--IF @varSearchBy='SHIPMENTSERIALNUMBER'
--  AND ( c.shipment_referenceType.Type='Lot Number' and c.shipment_referenceType.Value ='' ) --as it is null IF @varSearchBy='SHIPMENTLOTNUMBER'
 -- AND ( c.shipment_referenceType.Type='Hold Status'and c.shipment_referenceType.Value ='' )  --IF @varSearchBy='HOLDSTATUS'
--   AND  c.UPSOrderNumber     in  ('84614823') --IF @varSearchBy='CUSTOMEROUTBOUNDHEADERREFERENCE'
--   AND  c.UPSOrderNumber     in  ('84614823') -- IF @varSearchBy='LINEREFERENCENUMBER'
--   AND  c.UPSOrderNumber     in  ('84614823') --IF @varSearchBy='LPN'
--   AND  c.UPSOrderNumber     in  ('84614823') --IF @varSearchBy='VSN'
--   AND  c.UPSOrderNumber     in  ('84614823') --IF @varSearchBy='VCL'
--   AND  c.UPSOrderNumber     in  ('84614823') --IF @varSearchBy='DESIGNATOR'
--   AND  c.UPSOrderNumber     in  ('84614823') --IF @varSearchBy ='SHIPMENTBATCHNUMBER'
--   AND  c.UPSOrderNumber     in  ('84614823') --IF @varSearchBy='SHIPMENTSERIALNUMBERWAREHOUSE'   
  And c.is_deleted =0   
  AND ((c.IS_INBOUND=null? 0 : c.IS_INBOUND ) = 0)

--Result Set 2

/*
Target Container - digital_summary_orders
*/

select count(1) totalCount from c 

WHERE  c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529') 
  AND c.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
  AND  c.UPSOrderNumber     in  ('84614823') -- IF @varSearchBy = 'CARRIERSHIPMENTNUMBER'
--   AND  c.orderNumber in ('1ZE5717F4400025370')  -- IF @varSearchBy = 'REFERENCENUMBER'
--   AND  c.UPSOrderNumber in ('43841442') --IF @varSearchBy = 'UPSSHIPMENTNUMBER'
--   AND  c.customerPONumber in ('1443887') --IF @varSearchBy = 'CUSTOMERPONUMBER'
--   AND  c.orderNumber in ('E210911-010')  --IF @varSearchBy = 'CLIENTASNNUMBER'
--   AND ( c.UPSOrderNumber = '43828278'  AND c.IS_INBOUND=1) --IF @varSearchBy = 'UPSASNNUMBER'
 -- AND ( c.shipment_referenceType.Type='Serial Number'and c.shipment_referenceType.Value =''  ) --as it is null--IF @varSearchBy='SHIPMENTSERIALNUMBER'
 -- AND ( c.shipment_referenceType.Type='Lot Number' and c.shipment_referenceType.Value ='' ) --as it is null IF @varSearchBy='SHIPMENTLOTNUMBER'
 -- AND ( c.shipment_referenceType.Type='Hold Status'and c.shipment_referenceType.Value ='' )  --IF @varSearchBy='HOLDSTATUS'
--   AND  c.UPSOrderNumber     in  ('84318774') --IF @varSearchBy='CUSTOMEROUTBOUNDHEADERREFERENCE'
--   AND  c.UPSOrderNumber     in  ('84318774') -- IF @varSearchBy='LINEREFERENCENUMBER'
--   AND  c.UPSOrderNumber     in  ('84318774') --IF @varSearchBy='LPN'
--   AND  c.UPSOrderNumber     in  ('84318774') --IF @varSearchBy='VSN'
--   AND  c.UPSOrderNumber     in  ('84318774') --IF @varSearchBy='VCL'
--   AND  c.UPSOrderNumber     in  ('84318774') --IF @varSearchBy='DESIGNATOR'
--   AND  c.UPSOrderNumber     in  ('84318774') --IF @varSearchBy ='SHIPMENTBATCHNUMBER'
--   AND  c.UPSOrderNumber     in  ('84318774') --IF @varSearchBy='SHIPMENTSERIALNUMBERWAREHOUSE'
  And c.is_deleted =0  AND ((c.IS_INBOUND=null? 0 : c.IS_INBOUND ) = 0)

--Result Set 3

/*
Target Container - digital_summary_order_lines
*/

SELECT
c.UpsOrderNumber upsShipmentNumber,
c.ClientASNNumber clientASNNumber,
c.SourceSystemKey = 1002?  c.asnNumber : (IS_NULL(c.ReceiptNumber)?c.asnNumber:c.ReceiptNumber)  AS upsASNNumber
From c
WHERE c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529') 
--   AND c.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
--   AND  c.UpsOrderNumber     in  ('85218346') -- IF @varSearchBy = 'CARRIERSHIPMENTNUMBER'
--   AND  Exists(select t from t in c.OrderNumber where t in ('E211201-153'))  -- IF @varSearchBy = 'REFERENCENUMBER'
--   AND  c.UpsOrderNumber in ('85218346') --IF @varSearchBy = 'UPSSHIPMENTNUMBER'
--   AND  c.customerPONumber in ('1443887') --IF @varSearchBy = 'CUSTOMERPONUMBER'
--   AND  Exists(select t from t in c.OrderNumber where t in ('E211201-153'))  --IF @varSearchBy = 'CLIENTASNNUMBER'
--   AND ( c.UpsOrderNumber = '44030836'  AND c.IS_INBOUND=1) --IF @varSearchBy = 'UPSASNNUMBER'
--  AND ( c.shipment_referenceType.Type='Serial Number'and c.shipment_referenceType.Value =''  ) --as it is null--IF @varSearchBy='SHIPMENTSERIALNUMBER'
--  -- AND ( c.shipment_referenceType.Type='Lot Number' and c.shipment_referenceType.Value ='' ) --as it is null IF @varSearchBy='SHIPMENTLOTNUMBER'
--  -- AND ( c.shipment_referenceType.Type='Hold Status'and c.shipment_referenceType.Value ='' )  --IF @varSearchBy='HOLDSTATUS'
--   AND  c.UpsOrderNumber     in  ('85218346') --IF @varSearchBy='CUSTOMEROUTBOUNDHEADERREFERENCE'
--   AND  c.UpsOrderNumber     in  ('85218346') -- IF @varSearchBy='LINEREFERENCENUMBER'
--   AND  c.UpsOrderNumber     in  ('85218346') --IF @varSearchBy='LPN'
--   AND  c.UpsOrderNumber     in  ('85218346') --IF @varSearchBy='VSN'
--   AND  c.UpsOrderNumber     in  ('85218346') --IF @varSearchBy='VCL'
--   AND  c.UpsOrderNumber     in  ('85218346') --IF @varSearchBy='DESIGNATOR'
--   AND  c.UpsOrderNumber     in  ('85218346') --IF @varSearchBy ='SHIPMENTBATCHNUMBER'
--   AND  c.UpsOrderNumber     in  ('85218346') --IF @varSearchBy='SHIPMENTSERIALNUMBERWAREHOUSE'
  AND c.is_deleted =0 
  AND   ((c.is_inbound=null? 0 : c.is_inbound ) = 0)
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
 WHERE  c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529') 
  AND c.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
  AND  c.UPSOrderNumber     in  ('84638565') -- IF @varSearchBy = 'CARRIERSHIPMENTNUMBER'
--   AND  c.orderNumber in ('1ZE5717F4400025370')  -- IF @varSearchBy = 'REFERENCENUMBER'
--   AND  c.UPSOrderNumber in ('43841442') --IF @varSearchBy = 'UPSSHIPMENTNUMBER'
--   AND  c.customerPONumber in ('1443887') --IF @varSearchBy = 'CUSTOMERPONUMBER'
--   AND  c.orderNumber in ('E210911-010')  --IF @varSearchBy = 'CLIENTASNNUMBER'
--   AND ( c.UPSOrderNumber = '43828278'  AND c.IS_INBOUND=1) --IF @varSearchBy = 'UPSASNNUMBER'
 -- AND ( c.shipment_referenceType.Type='Serial Number'and c.shipment_referenceType.Value =''  ) --as it is null--IF @varSearchBy='SHIPMENTSERIALNUMBER'
 -- AND ( c.shipment_referenceType.Type='Lot Number' and c.shipment_referenceType.Value ='' ) --as it is null IF @varSearchBy='SHIPMENTLOTNUMBER'
 -- AND ( c.shipment_referenceType.Type='Hold Status'and c.shipment_referenceType.Value ='' )  --IF @varSearchBy='HOLDSTATUS'
--   AND  c.UPSOrderNumber     in  ('84318774') --IF @varSearchBy='CUSTOMEROUTBOUNDHEADERREFERENCE'
--   AND  c.UPSOrderNumber     in  ('84318774') -- IF @varSearchBy='LINEREFERENCENUMBER'
--   AND  c.UPSOrderNumber     in  ('84318774') --IF @varSearchBy='LPN'
--   AND  c.UPSOrderNumber     in  ('84318774') --IF @varSearchBy='VSN'
--   AND  c.UPSOrderNumber     in  ('84318774') --IF @varSearchBy='VCL'
--   AND  c.UPSOrderNumber     in  ('84318774') --IF @varSearchBy='DESIGNATOR'
--   AND  c.UPSOrderNumber     in  ('84318774') --IF @varSearchBy ='SHIPMENTBATCHNUMBER'
--   AND  c.UPSOrderNumber     in  ('84318774') --IF @varSearchBy='SHIPMENTSERIALNUMBERWAREHOUSE'
  And c.is_deleted =0 AND ((c.IS_INBOUND=null? 0 : c.IS_INBOUND ) = 0)
  ) a
 join t in a.TrackingNumber