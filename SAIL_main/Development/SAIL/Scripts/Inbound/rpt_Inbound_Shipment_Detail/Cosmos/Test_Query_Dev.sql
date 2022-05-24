--AUTHOR :Mahesh Rathi
--DESCRIPITION : rpt_Inbound_Shipment_Detail
--DATE : 18-02-2022

-- Result set 1a:

--Target container digital_summary_order

SELECT b.upsShipmentNumber
    ,b.clientShipmentNumber
    ,b.shipmentType
    ,b.inboundType
    ,b.templateType
    ,b.shipmentNumber
    ,b.referenceNumber
    ,b.customerPONumber
    ,b.orderNumber
    ,b.upsTransportShipmentNumber
    ,b.gff_shipmentInstanceId
    ,b.gff_ShipmentNumber
    ,b.shipmentCarrier
    ,b.shipmentCarrierCode
    ,b.shipmentServiceLevel
    ,b.shipmentServiceLevelCode
    ,b.serviceMode
    ,b.warehouseId
    ,b.warehouseCode
    ,b.primaryException
    ,b.shipmentPlaceDateTime
    ,b.shipmentCanceledDateTime
    ,b.shipmentCanceledReason
    ,b.actualShipmentDateTime
    ,b.shipmentCreateDateTime
    ,b.originalScheduledDeliveryDateTime
    ,b.actualDeliveryDateTime
    ,b.TrackingNumber
    ,b.shipmentOrigin_addressLine1
    ,b.shipmentOrigin_addressLine2
    ,b.shipmentOrigin_city
    ,b.shipmentOrigin_stateProvince
    ,b.shipmentOrigin_postalCode
    ,b.shipmentOrigin_country
    ,b.shipmentOrigin_port_addressLine1
    ,b.shipmentOrigin_port_addressLine2
    ,b.shipmentOrigin_port_city
    ,b.shipmentOrigin_port_stateProvince
    ,b.shipmentOrigin_port_country
    ,b.shipmentOrigin_port_postalCode
    ,b.shipmentOrigin_port_phoneNumber
    ,b.shipmentDestination_addressLine1
    ,b.shipmentDestination_addressLine2
    ,b.shipmentDestination_city
    ,b.shipmentDestination_stateProvince
    ,b.shipmentDestination_country
    ,b.shipmentDestination_postalCode
    ,b.shipmentDestination_port_addressLine1
    ,b.shipmentDestination_port_addressLine2
    ,b.shipmentDestination_port_city
    ,b.shipmentDestination_port_stateProvince
    ,b.shipmentDestination_port_postalCode
    ,b.shipmentDestination_port_country
    ,b.shipmentDestination_port_phoneNumber
    ,b.shipmentNotes_dateTime
    ,b.shipmentNotes_description
    ,b.Freight_Carriercode
    ,b.WAYBILL_AIRBILL_NUM
    ,b.FTZShipmentNumber
    ,b.accountNumber
    ,t.ShipmentDimensions_UOM loadDimension
    ,t.LOAD_AREA loadValue
    ,b.milestoneStatus
    ,b.lastKnownLocation
    ,b.loadNumber
 FROM (
    SELECT c.upsShipmentNumber
        ,c.clientShipmentNumber
        ,c.shipmentType
        ,(c.IS_ASN = 1) ? 'ASN' : 'Managed Transportation' AS inboundType
        ,c.templateType
        ,c.upsTransportShipmentNumber shipmentNumber
        ,c.referenceNumber
        ,c.customerPONumber
        ,c.orderNumber
        ,c.upsTransportShipmentNumber
        ,c.gffShipmentInstanceId gff_shipmentInstanceId
        ,c.gffShipmentNumber gff_ShipmentNumber
        ,c.shipmentCarrier
        ,c.shipmentCarrierCode
        ,c.shipmentServiceLevel
        ,c.shipmentServiceLevelCode
        ,c.ServiceMode serviceMode
        ,c.warehouseId
        ,c.warehouseCode
        ,c.primaryException
        ,c.shipmentPlaceDateTime
        ,c.shipmentCanceledDateTime
        ,c.shipmentCanceledReason
        ,c.actualShipmentDateTime
        ,c.shipmentCreateDateTime
        ,c.originalScheduledDeliveryDateTime
        ,c.actualDeliveryDateTime
        ,c.TrackingNumber
        ,c.shipmentOrigin_addressLine1
        ,c.shipmentOrigin_addressLine2
        ,c.shipmentOrigin_city
        ,c.shipmentOrigin_stateProvince
        ,c.shipmentOrigin_postalCode
        ,c.shipmentOrigin_country
        ,'' shipmentOrigin_port_addressLine1
        ,'' shipmentOrigin_port_addressLine2
        ,'' shipmentOrigin_port_city
        ,'' shipmentOrigin_port_stateProvince
        ,'' shipmentOrigin_port_country
        ,'' shipmentOrigin_port_postalCode
        ,'' shipmentOrigin_port_phoneNumber
        ,c.shipmentDestination_addressLine1
        ,c.shipmentDestination_addressLine2
        ,c.shipmentDestination_city
        ,c.shipmentDestination_stateProvince
        ,c.shipmentDestination_country
        ,c.shipmentDestination_postalCode
        ,'' shipmentDestination_port_addressLine1
        ,'' shipmentDestination_port_addressLine2
        ,'' shipmentDestination_port_city
        ,'' shipmentDestination_port_stateProvince
        ,'' shipmentDestination_port_postalCode
        ,'' shipmentDestination_port_country
        ,'' shipmentDestination_port_phoneNumber
        ,'' shipmentNotes_dateTime
        ,'' shipmentNotes_description
        ,c.Freight_Carriercode
        ,c.WAYBILL_AIRBILL_NUM
        ,c.FTZShipmentNumber
        ,c.Accountnumber accountNumber
        ,c.TrackingNumber = null ? Array(SELECT null ShipmentDimensions_UOM, null LOAD_AREA) : c.TrackingNumber Tracking
        ,c.milestoneStatus
        ,c.lastKnownLocation
        ,c.LoadID loadNumber
    FROM c
    WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
        AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
        AND c.UPSOrderNumber = '611898'
        AND c.IS_INBOUND = 1
        AND c.is_deleted = 0
    ) b
JOIN t IN b.Tracking

-- Result set 1b:

--Target container digital_summary_order_lines

SELECT c.LineNumber
    ,c.SKU
    ,c.asnNumber
    ,c.SKUDescription
    ,c.CustomerPONumber
    ,c.SKUQuantity
    ,c.SKUShippedQuantity
    ,c.SKUWeight
    ,c.SKUDimensions
    ,c.SKUWeight_UOM
    ,c.SKUDimensions_UOM
    ,c.lineReferenceNumber1 ReferenceNumber_1
    ,c.lineReferenceNumber2 ReferenceNumber_2
    ,c.lineReferenceNumber3 ReferenceNumber_3
    ,(
        ol.itemNumber = c.SKU
        AND ol.vendorLotNumber != null
        ) ? ol.vendorLotNumber : null batchNumber
    ,(
        ol.itemNumber = c.SKU
        AND ol.vendorLotNumber != null
        ) ? ol.vendorSerialNumber : null serialNumber
    ,(
        ol.itemNumber = c.SKU
        AND ol.vendorLotNumber != null
        ) ? ol.expirationDate : null expirationDate
FROM c
JOIN ol IN c.OrderLineDetail
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.UpsOrderNumber = '611898'
    AND c.is_inbound = 1
    AND c.is_deleted = 0

-- Result set 1c:

--Target container digital_summary_order_lines

select
c.ClientASNNumber clientASNNumber,
c.SouceSystemKey =1002 ?  c.asnNumber : (c.ReceiptNumber = null? c.asnNumber : c.ReceiptNumber) upsASNNumber ,
c.ReceiptNumber receiptNumber            ,
c.FacilityId  facilityId             ,
c.FacilityCode facilityName ,
c.CreationDateTime creationDateTime
from c  WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'      
AND  c.UpsOrderNumber = '611898'      
AND c.is_inbound=1 and c.is_deleted = 0
group by
c.ClientASNNumber,
c.SouceSystemKey = 1002 ?  c.asnNumber : (c.ReceiptNumber = null? c.asnNumber : c.ReceiptNumber),
c.ReceiptNumber,
c.FacilityId,
c.FacilityCode,
c.CreationDateTime

-- Result set 2:

--- Target container digital_summary_transport_details

SELECT c.ITEM_DESCRIPTION transportation_itemDescription
    ,c.ACTUAL_QTY transportation_quantity
    ,c.ACTUAL_UOM transportation_unitOfMeasurement
    ,c.ACTUAL_WGT transportation_shipmentWeight_weight
    ,c.ITEM_DIMENSION transportation_shipmentDimension
    ,null Attribute1
    , null Attribute2
    ,null Attribute3
    ,null Attribute4
FROM c
WHERE c.Account_ID = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.UpsOrderNumber = '611898'
    AND c.is_deleted = 0

-- Result set 3:

--Target Container digital_summary_orders

SELECT DISTINCT c.TransportOrderNumber
FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.UPSOrderNumber = '611898'
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND c.TransportOrderNumber != null

-- Result set 4:

--Target Container digital_summary_transportation_rates_charges

SELECT c.totalCustomerCharge
    ,c.totalCustomerChargeCurrency
    ,c.invoiceDateTime
    ,c.CostBreakdown costBreakdown
FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.UpsOrderNumber = '611898'
    AND c.is_inbound = 1
    AND c.is_deleted = 0

-- Result set 5:

--Target container : digital_summary_orders

SELECT c.claimType
    ,c.claimAmount
    ,c.claimAmountCurrency
    ,c.claimFilingDateTime
    ,c.claimStatus
    ,c.claimClosureDateTime
    ,c.claimAmountPaid
    ,c.claimAmountPaidCurrency
FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.UPSOrderNumber = '611898'
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0

-- Result set 6:

--Target container : digital_summary_orders

SELECT t.ShipmentDescription shippableUnit_description
    ,t.ShipmentQuantity shippableUnit_quantity
    ,t.UnitOfMeasurement shippableUnit_quantity_unitOfMeasurement
    ,t.ShipmentWeight shippableUnit_weight
    ,t.ShipmentWeight_UOM shippableUnit_weight_unitOfMeasurement
    ,t.ShipmentDimensions shippableUnit_dimension
    ,t.ShipmentDimensions_UOM shipableUnit_dimension_unitOfMeasurement
    ,c.shipment_referenceType shippableUnit_referenceType
    ,(
        SELECT t.TempRangeMin
            ,t.TempRangeMax
        FROM t
        ) AS shippableUnit_temperatureDetails
FROM c
JOIN t IN c.TrackingNumber
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.UPSOrderNumber = '449826'
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0

-- Result set 7:

--Target container : digital_summary_orders

SELECT
c.latestTemperature temperatureValue
     ,c.temperatureDateTime temperatureDateTime
     ,c.temperatureCity temperatureCity    
     ,c.temperatureState temperatureState    
     ,c.temperatureCountry temperatureCountry
FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.UPSOrderNumber = '611898'
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
