--rpt_Outbound_Shipment_Detail_Milestone

--Result Set 1

/*
Parameter Requirement info -
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
WHERE c.AccountId = @DPProductLineKey
  AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
  AND c.UpsOrderNumber = @UPSShipmentNumber
  AND c.MilestoneName != 'ALERT' and is_deleted = 0

--Result Set 2

/*
Parameter Requirement info -
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
WHERE c.AccountId = @DPProductLineKey
  AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
  AND c.UpsOrderNumber = @UPSShipmentNumber
  AND c.MilestoneName = 'ALERT' and c.is_deleted = 0