-- #Author :Vishal Sharma
-- #DESCRIPITION : rpt_Inbound_Cancelled_Shipment_Details
-- #DATE : 18-02-2022

-- Result Set 1

--* Target Container-digital_summary_orders

SELECT count(1) AS totalCount
FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
    AND (
        c.DateTimeCancelled BETWEEN '2022-01-01 00:00:00.000'
            AND '2022-01-17 00:00:00.000'
        )
    AND c.OrderStatusName = 'Cancelled'
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
	--we don't have data for c.OrderStatusName and is_inbound=1
	

-- Result Set 2 

--* Target Container-digital_summary_orders

-- CASE 1 IF @topRow = 0

SELECT c.shipmentCanceledDateTime
    ,c.shipmentCanceledBy
    ,c.shipmentCanceledReason
    ,c.upsShipmentNumber
    ,c.clientShipmentNumber
    ,c.shipmentNumber
    ,c.referenceNumber
    ,c.customerPONumber
    ,c.orderNumber
    ,c.shipmentCarrier
    ,c.shipmentCarrierCode
    ,c.shipmentServiceLevel
    ,c.shipmentServiceLevelCode
    ,c.ServiceMode
FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
    AND (
        c.DateTimeCancelled BETWEEN '2020-11-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2021-12-31 00:00:00.000'))
        )
    AND c.OrderStatusName = 'Cancelled'
    AND c.IS_INBOUND = 1
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
ORDER BY c.shipmentCanceledDateTime DESC
	--we don't have data for c.OrderStatusName and is_inbound=1	

-- CASE 2 IF @topRow >0

SELECT c.shipmentCanceledDateTime
    ,c.shipmentCanceledBy
    ,c.shipmentCanceledReason
    ,c.upsShipmentNumber
    ,c.clientShipmentNumber
    ,c.shipmentNumber
    ,c.referenceNumber
    ,c.customerPONumber
    ,c.orderNumber
    ,c.shipmentCarrier
    ,c.shipmentCarrierCode
    ,c.shipmentServiceLevel
    ,c.shipmentServiceLevelCode
    ,c.ServiceMode
FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
    AND (
        c.DateTimeCancelled BETWEEN '2020-11-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2021-12-31 00:00:00.000'))
        )
    AND c.OrderStatusName = 'Cancelled'
    AND c.IS_INBOUND = 1
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = 1
ORDER BY c.shipmentCanceledDateTime DESC
--we don't have data for c.OrderStatusName and is_inbound=1		