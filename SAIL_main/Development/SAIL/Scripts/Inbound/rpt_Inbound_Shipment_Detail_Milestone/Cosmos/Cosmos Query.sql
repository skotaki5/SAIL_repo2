-- #Author :Mahesh Rathi
-- #DESCRIPITION : rpt_Inbound_Shipment_Detail_Milestone
-- #DATE : 18-02-2022

-- Result Set 1:

-- Parameter requirement info.
--@DPProductLineKey  required 
--@DPServiceLineKey  optional
--@UPSOrderNumber required

--Target container digital_summary_orders 


SELECT t.ShipmentMileStone ShipmentMileStones --added alias
    ,t.MilesStoneEstimatedDateTime
    ,t.MilesStoneCompletionDateTime
    ,t.activityCount
    ,t.templateType
FROM c
JOIN t IN c.DetailMilestone
WHERE c.AccountId = @DPProductLineKey
    AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
    AND c.UpsOrderNumber = @UPSShipmentNumber
    AND c.MilestoneName != 'ALERT'
    AND c.is_deleted = 0

-- Result Set 2:

-- Parameter requirement info.
--@DPProductLineKey  required 
--@DPServiceLineKey  optional
--@UPSOrderNumber required

--Target container digital_summary_milestone_activity

SELECT c.ActivityName AS AlertCode
    ,IS_NULL(c.MilestoneDate) ? c.ActivityDate :c.MilestoneDate AlertDateTime
    ,'' AS AlertMessageLatest
    ,c.MilestoneName AS Milestone
FROM c
WHERE c.AccountId = @DPProductLineKey
    AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
    AND c.UpsOrderNumber = @UPSShipmentNumber
    AND c.MilestoneName = 'ALERT'
    AND c.is_deleted = 0


