--rpt_Outbound_Shipment_Detail_Milestone

--Result Set 1

/*
Parameter Requirement infor -
->@DPProductLineKey  required 
->@DPServiceLineKey  optional
->@UPSOrderNumber required

Target Container - digital_summary_orders
*/

select Distinct t.ShipmentMileStone ShipmentMileStones,
  t.MilesStoneEstimatedDateTime, 
  t.MilesStoneCompletionDateTime,
   t.activityCount, t.templateType
       from c join t in c.DetailMilestone
       WHERE  c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
        and c.is_deleted = 0 
       AND c.UPSOrderNumber ='86277742'
       AND c.milestoneStatus != 'ALERT' 

--Result Set 2

/*
Parameter Requirement infor -
->@DPProductLineKey  required 
->@DPServiceLineKey  optional
->@UPSOrderNumber required

Target Container - digital_summary_milestone_activity
*/

SELECT
    c.ActivityName AS AlertCode,
    IS_NULL(c.MilestoneDate) ? c.ActivityDate :c.MilestoneDate  AlertDateTime,
    '' AS AlertMessageLatest,
    c.MilestoneName AS Milestone,
 c.TimeZone AS alertDateTimeZone
  FROM c
WHERE c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
  AND c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'
  AND c.UpsOrderNumber = '85432381'
  AND c.MilestoneName = 'ALERT' and c.is_deleted = 0