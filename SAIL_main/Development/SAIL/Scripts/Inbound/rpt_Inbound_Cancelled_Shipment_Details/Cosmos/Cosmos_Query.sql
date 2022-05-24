-- #Author :Vishal Sharma
-- #DESCRIPITION : rpt_Inbound_Cancelled_Shipment_Details
-- #DATE : 18-02-2022

-- Result Set 1:

--Parameter requirement info.
-- @isASN is optional,
-- @startDate AND @endDate are optional,
-- @DPProductLineKey required,
-- @DPServiceLineKey optional,
-- @warehouseId optional
--Target Container-digital_summary_orders

SELECT count(1) AS totalCount
FROM c
WHERE c.AccountId = @DPProductLineKey
    AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
    AND c.FacilityId IN (@warehouseId)
    AND (
        c.DateTimeCancelled BETWEEN @startDate AND @endDate
        )
    AND c.OrderStatusName = 'Cancelled'
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
	--we don't have data for c.OrderStatusName and is_inbound=1
	
-- Result Set 2 :

--* Parameter requirement info.
-- @isASN is optional,
-- @startDate AND @endDate are optional,
-- @DPProductLineKey required,
-- @DPServiceLineKey optional,
-- @warehouseId optional
--* Target Container-digital_summary_orders

-- CASE 1 IF @topRow = 0:

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
WHERE c.AccountId = @DPProductLineKey
    AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
    AND c.FacilityId IN (@warehouseId)
    AND (
        c.DateTimeCancelled BETWEEN @startDate AND @endDate
        )
    AND c.OrderStatusName = 'Cancelled'
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
ORDER BY c.shipmentCanceledDateTime DESC
--we don't have data for c.OrderStatusName and is_inbound=1	

-- CASE 2 IF @topRow >0:

SELECT TOP @topRow
     c.shipmentCanceledDateTime
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
WHERE c.AccountId = @DPProductLineKey
    AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
    AND c.FacilityId IN (@warehouseId)
    AND (
        c.DateTimeCancelled BETWEEN @startDate AND @endDate
        )
    AND c.OrderStatusName = 'Cancelled'
    AND c.IS_INBOUND = 1
    AND c.is_deleted = 0
    AND (is_null(c.IS_ASN) ?0: c.IS_ASN) = @isASN
ORDER BY c.shipmentCanceledDateTime DESC
--we don't have data for c.OrderStatusName and is_inbound=1		