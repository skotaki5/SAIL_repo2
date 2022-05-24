-- #Author :Mahesh Rathi
-- #DESCRIPITION : rpt_Inbound_Shipment_Detail_Milestone
-- #DATE : 18-02-2022

-- Result set 1 :

--Target container digital_summary_orders 

SELECT t.ShipmentMileStone ShipmentMileStones --added alias
    ,t.MilesStoneEstimatedDateTime
    ,t.MilesStoneCompletionDateTime
    ,t.activityCount
    ,t.templateType
FROM c
JOIN t IN c.DetailMilestone
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.UPSOrderNumber = '611898'
    AND c.milestoneStatus != 'ALERT'
    AND c.is_deleted = 0

 
  
-- Result set 2:

--Target container digital_summary_milestone_activity

SELECT c.ActivityName AS AlertCode
    ,IS_NULL(c.MilestoneDate) ? c.ActivityDate :c.MilestoneDate AlertDateTime
    ,'' AS AlertMessageLatest
    ,c.MilestoneName AS Milestone
FROM c
WHERE c.AccountId ='E648FA6F-6253-428E-8AC9-201E3EF83B91'
AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
AND c.UpsOrderNumber = '611898'
    AND c.MilestoneName = 'ALERT'
    AND c.is_deleted = 0