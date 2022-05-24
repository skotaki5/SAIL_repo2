-- #Author : Mahesh Rathi
-- #Descripition : rpt_Inbound_Shipment_DayLevel_Summary
-- #Date : 18-02-2022 


-- Result set 1:

--Target container digital_summary_order

--Condition 1:  IF @varDateType = 'SHIPMENTCREATIONDATE' AND @NULLActualDeliveryDate = '*'  
 
SELECT count(1) AS Total
FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND (c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E'))
    --AND (
    --  c.LoadLatestDeliveryDate BETWEEN '2022-04-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-04-30 00:00:00.000"))
    --  )
    --AND (
    --  c.DateTimeShipped BETWEEN '2022-04-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-04-30 00:00:00.000'))
    --  )
    --AND (
    --  c.ScheduledPickUpDateTime BETWEEN '2022-04-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-04-30 00:00:00.000"))
    --  )
    AND c.IS_INBOUND = 1
    AND (((c.IS_ASN = null) ? 0 :c.IS_ASN) = 1) --depends on params   SET @isASN = CASE WHEN @VarInboundType='ASN' THEN 1 WHEN @VarInboundType='TRANSPORT ORDER' THEN 0
    AND (UPPER('N') = 'Y' ? 1 : (UPPER('N') = 'N' ? 0 : 'N')) = c.is_managed
    AND (
        c.DateTimeReceived BETWEEN '2022-04-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-04-30 00:00:00.000"))
        )
     --AND (
        --c.estimatedDeliveryDateTime BETWEEN '2022-04-01 00:00:00.000'
        --  AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-04-30 00:00:00.000"))
        --)
     --AND (
        --c.actualDeliveryDateTime_Movement BETWEEN '2022-04-01 00:00:00.000'
        --  AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-04-30 00:00:00.000"))
        --)
     --AND (
        --c.PickUpDate BETWEEN '2022-04-01 00:00:00.000'
        --  AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-04-30 00:00:00.000"))
        --)
     --AND (
        --c.shipmentBookedOnDateTime BETWEEN '2022-04-01 00:00:00.000'
        --  AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-04-30 00:00:00.000"))
        --)
    AND c.is_deleted = 0
 
--Condition 2:   IF @varDateType = 'SHIPMENTDELIVERYDATE' AND @NULLCreatedDate = '*'
 
SELECT count(1) AS total
FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND (c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E'))
    AND c.activity_Movement_flag = 1
    --AND (
    --    c.LoadLatestDeliveryDate BETWEEN '2022-01-01 00:00:00.000'
    --        AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-01-31 00:00:00.000"))
    --    )
    --AND (
    --    c.DateTimeShipped BETWEEN '2022-01-01 00:00:00.000'
    --        AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-01-31 00:00:00.000"))
    --    )
    --AND (
    --    c.ScheduledPickUpDateTime BETWEEN '2022-01-01 00:00:00.000'
       --     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-01-31 00:00:00.000"))
        --)
    AND c.IS_INBOUND = 1
    AND ((c.IS_ASN = null ? 0 :c.IS_ASN) = 1) --depends on params   SET @isASN = CASE WHEN @VarInboundType='ASN' THEN 1 WHEN @VarInboundType='TRANSPORT ORDER' THEN 0
    AND (UPPER('N') = 'Y' ? 1 : (UPPER('N') = 'N' ? 0 : 'N')) = c.is_managed
    --AND (
    --    c.estimatedDeliveryDateTime BETWEEN '2022-01-01 00:00:00.000'
    --        AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-01-31 00:00:00.000"))
    --    )
    AND (
        c.actualDeliveryDateTime BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-01-31 00:00:00.000"))
        )
    --AND (
    --    c.PickUpDate BETWEEN '2022-01-01 00:00:00.000'
    --        AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-01-31 00:00:00.000"))
    --    )
    --AND (
    --    c.shipmentBookedOnDateTime BETWEEN '2022-01-01 00:00:00.000'
    --        AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-01-31 00:00:00.000"))
    --    )
    AND c.is_deleted = 0
  
-- Result set 2:

--Target container digital_summary_order

--Condition 1: IF @varDateType = 'SHIPMENTCREATIONDATE' AND @NULLActualDeliveryDate = '*'
 
SELECT c.ShipmentCreationDate
    ,COUNT(1) AS ShipmentCreationDateCount
FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND (c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E'))
    -- AND (
    --  c.LoadLatestDeliveryDate BETWEEN '2022-04-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-04-30 00:00:00.000"))
    --  ) -- wrong format
    -- AND (
    --  c.DateTimeShipped BETWEEN '2022-04-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-04-30 00:00:00.000'))
    --  )
    -- AND (
    --  c.ScheduledPickUpDateTime BETWEEN '2022-04-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-04-30 00:00:00.000"))
    --  )
    AND c.IS_INBOUND = 1
    AND ((c.IS_ASN = null ? 0 :c.IS_ASN) = 1) --depends on params   SET @isASN = CASE WHEN @VarInboundType='ASN' THEN 1 WHEN @VarInboundType='TRANSPORT ORDER' THEN 0
    AND (UPPER('N') = 'Y' ? 1 : (UPPER('N') = 'N' ? 0 : 'N')) = c.is_managed
    AND (
        c.DateTimeReceived BETWEEN '2022-04-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-04-30 00:00:00.000'))
        )
    -- AND (
    --  c.estimatedDeliveryDateTime BETWEEN '2022-04-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-04-30 00:00:00.000"))
    --  )
    -- AND (
    --  c.actualDeliveryDateTime_Movement BETWEEN '2022-04-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-04-30 00:00:00.000"))
    --  )
    -- AND (
    --  c.PickUpDate BETWEEN '2022-04-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-04-30 00:00:00.000"))
    --  )
    -- AND (
    --  c.shipmentBookedOnDateTime BETWEEN '2022-04-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-04-30 00:00:00.000"))
        -- )
    AND c.is_deleted = 0
    AND c.ShipmentCreationDate != null
GROUP BY c.ShipmentCreationDate
--  ORDER BY ShipmentCreationDate to be applied in the backend
 
--Condition 2: IF @varDateType = 'SHIPMENTDELIVERYDATE' AND @NULLCreatedDate = '*'
 
SELECT c.ShipmentDeliveryDate
    ,COUNT(1) AS ShipmentDeliveryDateCount
FROM c
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND (c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E'))
    AND c.activity_Movement_flag = 1
    --AND (
    --  c.LoadLatestDeliveryDate BETWEEN '2022-04-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-04-30 00:00:00.000"))
    --  )
    --AND (
    --  c.DateTimeShipped BETWEEN '2022-04-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-04-30 00:00:00.000"))
    --  )
    --AND (
    --  c.ScheduledPickUpDateTime BETWEEN '2022-04-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-04-30 00:00:00.000"))
    --  )
    AND c.IS_INBOUND = 1
    AND ((c.IS_ASN = null ? 0 :c.IS_ASN) = 1) --depends on params   SET @isASN = CASE WHEN @VarInboundType='ASN' THEN 1
    -- WHEN @VarInboundType='TRANSPORT ORDER' THEN 0
    AND (UPPER('N') = 'Y' ? 1 : (UPPER('N') = 'N' ? 0 : 'N')) = c.is_managed
    --AND (
    --  c.estimatedDeliveryDateTime BETWEEN '2022-04-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-04-30 00:00:00.000"))
    --  )
    AND (
        c.actualDeliveryDateTime_Movement BETWEEN '2022-04-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-04-30 00:00:00.000"))
        )
    --AND (
    --  c.PickUpDate BETWEEN '2022-04-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-04-30 00:00:00.000"))
    --  )
    --AND (
    --  c.shipmentBookedOnDateTime BETWEEN '2022-04-01 00:00:00.000'
    --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, "2022-04-30 00:00:00.000"))
    --  )
    AND c.is_deleted = 0
GROUP BY c.ShipmentDeliveryDate
-- order by ShipmentDeliveryDate to be applied in the backend