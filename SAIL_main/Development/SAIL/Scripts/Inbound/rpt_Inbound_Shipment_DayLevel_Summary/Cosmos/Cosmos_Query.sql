-- #Author : Mahesh Rathi
-- #Descripition : rpt_Inbound_Shipment_DayLevel_Summary
-- #Date : 18-02-2022 




/*
Note : 
preprocess some variable based on the storeproc

IF @DateType is null
begin
if @NULLCreatedDate = '*'
begin
set  @varDateType = 'SHIPMENTDELIVERYDATE'
end
if @NULLActualDeliveryDate = '*'
begin
set @varDateType = 'SHIPMENTCREATIONDATE'  
end
end
IF @Date is null and UPPER(@DateType)='SHIPMENTCREATIONDATE'
begin
set @shipmentCreationStartDateTime=@StartDate
set @shipmentCreationEndDateTime=DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @endDate)))
set @NULLCreatedDate = ''
end
else IF @Date is null and UPPER(@DateType)='SHIPMENTDELIVERYDATE'
begin
set @actualDeliveryStartDateTime=@StartDate
set @actualDeliveryEndDateTime=DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @endDate)))
set @NULLActualDeliveryDate = ''
end
SET @isASN = CASE WHEN @VarInboundType='ASN' THEN 1
                     WHEN @VarInboundType='TRANSPORT ORDER' THEN 0
                END
*/

-- Result set 1:

-- Parameter requirement info.
-- @DPProductLineKey and UPSOrderNumber required,
-- @DPServiceLineKey optional
-- @DPEntityKey  optional
-- @StartDate optional
-- @EndDate optional
-- @DateType  optional
-- @warehouseId optional
-- @inboundType optional
-- @Date optional
-- @IsManaged  optional


--Target container digital_summary_order

-- Condition 1:  IF @varDateType = 'SHIPMENTCREATIONDATE' AND @NULLActualDeliveryDate = '*'

SELECT count(1) AS Total
FROM c
WHERE c.AccountId = @DPProductLineKey
    AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
    AND (c.FacilityId IN (@warehouseId))
    AND (
        c.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
        )
    AND (
        c.DateTimeShipped BETWEEN @Date.shippedStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shippedEndDate))
        )
    AND (
        c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
        )
    AND c.IS_INBOUND = 1
    AND ((c.IS_ASN = null ? 0 :c.IS_ASN  = @isASN) --depends on params   SET @isASN = CASE WHEN @VarInboundType='ASN' THEN 1 WHEN @VarInboundType='TRANSPORT ORDER' THEN 0  
    AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = c.is_managed    
    AND (
        c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
        )
    AND (
        c.estimatedDeliveryDateTime BETWEEN @Date.deliveryEtaStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.deliveryEtaEndDate))
        )
    AND (
        c.actualDeliveryDateTime_Movement BETWEEN @Date.actualDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        )
    AND (
        c.PickUpDate BETWEEN @Date.pickupStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.pickupEndDate))
        )
    AND (
        c.shipmentBookedOnDateTime BETWEEN @Date.bookedStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.bookedEndDate))
        )
    AND c.is_deleted = 0
 
   
   
--Condition 2:   IF @varDateType = 'SHIPMENTDELIVERYDATE' AND @NULLCreatedDate = '*'
 
SELECT count(1) AS total
FROM c
WHERE c.AccountId = @DPProductLineKey
    AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
    AND (c.FacilityId IN (@warehouseId))
    AND c.activity_Movement_flag = 1
    AND (
        c.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
        )
    AND (
        c.DateTimeShipped BETWEEN @Date.shippedStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shippedEndDate))
        )
    AND (
        c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
        )
    AND c.IS_INBOUND = 1
    AND ((c.IS_ASN = null ? 0 :c.IS_ASN  = @isASN) --depends on params   SET @isASN = CASE WHEN @VarInboundType='ASN' THEN 1 WHEN @VarInboundType='TRANSPORT ORDER' THEN 0  
    AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = c.is_managed 
    AND (
        c.estimatedDeliveryDateTime BETWEEN @Date.deliveryEtaStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.deliveryEtaEndDate))
        )
    AND (
        c.actualDeliveryDateTime_Movement BETWEEN @Date.actualDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        )
    AND (
        c.PickUpDate BETWEEN @Date.pickupStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.pickupEndDate))
        )
    AND (
        c.shipmentBookedOnDateTime BETWEEN @Date.bookedStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.bookedEndDate))
        )
    AND c.is_deleted = 0
  
-- Result set 2:

-- Parameter requirement info.
-- @DPProductLineKey required,
-- @DPServiceLineKey optional
-- @DPEntityKey  optional
-- @StartDate optional
-- @EndDate optional
-- @DateType  optional
-- @warehouseId optional
-- @inboundType optional
-- @Date optional
-- @IsManaged  optional


--Target container digital_summary_order

--Condition 1: IF @varDateType = 'SHIPMENTCREATIONDATE' AND @NULLActualDeliveryDate = '*'
 
SELECT c.ShipmentCreationDate
    ,COUNT(1) AS ShipmentCreationDateCount
FROM c
WHERE c.AccountId = @DPProductLineKey
    AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
    AND (c.FacilityId IN (@warehouseId))
    AND (
        c.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
        )
    AND (
        c.DateTimeShipped BETWEEN @Date.shippedStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shippedEndDate))
        )
    AND (
        c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
        )
    AND c.IS_INBOUND = 1
    AND ((c.IS_ASN = null ? 0 :c.IS_ASN  = @isASN) --depends on params   SET @isASN = CASE WHEN @VarInboundType='ASN' THEN 1 WHEN @VarInboundType='TRANSPORT ORDER' THEN 0
    AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = c.is_managed
    AND (
        c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
        )
    AND (
        c.estimatedDeliveryDateTime BETWEEN @Date.deliveryEtaStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.deliveryEtaEndDate))
        )
    AND (
        c.actualDeliveryDateTime_Movement BETWEEN @Date.actualDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        )
    AND (
        c.PickUpDate BETWEEN @Date.pickupStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.pickupEndDate))
        )
    AND (
        c.shipmentBookedOnDateTime BETWEEN @Date.bookedStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.bookedEndDate))
        )
    AND c.is_deleted = 0
    AND c.ShipmentCreationDate != null
GROUP BY c.ShipmentCreationDate
 --ORDER BY ShipmentCreationDate to be applied in the backend
 
--Condition 2:IF @varDateType = 'SHIPMENTDELIVERYDATE' AND @NULLCreatedDate = '*'
 
SELECT c.ShipmentDeliveryDate
    ,COUNT(1) AS ShipmentDeliveryDateCount
FROM c
WHERE c.AccountId = @DPProductLineKey
    AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
    AND (c.FacilityId IN (@warehouseId))
    AND c.activity_Movement_flag = 1
    AND (
        c.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
        )
    AND (
        c.DateTimeShipped BETWEEN @Date.shippedStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shippedEndDate))
        )
    AND (
        c.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
        )
    AND c.IS_INBOUND = 1
    AND ((c.IS_ASN = null ? 0 :c.IS_ASN  = @isASN) --depends on params   SET @isASN = CASE WHEN @VarInboundType='ASN' THEN 1 WHEN @VarInboundType='TRANSPORT ORDER' THEN 0
    AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = c.is_managed
    AND (
        c.estimatedDeliveryDateTime BETWEEN @Date.deliveryEtaStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.deliveryEtaEndDate))
        )
    AND (
        c.actualDeliveryDateTime_Movement BETWEEN @Date.actualDeliveryStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        )
    AND (
        c.PickUpDate BETWEEN @Date.pickupStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.pickupEndDate))
        )
    AND (
        c.shipmentBookedOnDateTime BETWEEN @Date.bookedStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.bookedEndDate))
        )
    AND c.is_deleted = 0
GROUP BY c.ShipmentDeliveryDate
-- order by ShipmentDeliveryDate to be applied in the backend
