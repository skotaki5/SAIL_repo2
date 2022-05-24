--rpt_Outbound_Shipment_Detail_Activity

--Result Set 1

/*
 Target Container - digital_summary_milestone_activity
*/
  
select
c.TrackingNumber          trackingNumber     ,
CD.carrierCode,
CD.CarrierName,
c.MilestoneName            milestoneName    ,
c.ActivityName        activityName         ,
c.ACTIVITY_NOTES        activityNote1        ,
null activityNote2,
c.ActivityDate      activityDateTime       ,
c.ActivityCompletionFlag    isActivityCompleted      ,
c.SEGMENT_ID       Segment               ,
CD.shipmentServiceLevel,
CD.shipmentServiceLevelCode,
c.PROOF_OF_DELIVERY_NAME     proofOfDelivery         ,
c.CARRIER_TYPE    carrierType              ,
c.TimeZone   activityDateTimeZone      ,
is_null(c.LOGI_NEXT_FLAG)?'N':c.LOGI_NEXT_FLAG additionalTrackingIndicator,
c.PROOF_OF_DELIVERY_LOCATION  leftAt,
c.LATITUDE  activityScanLatitude ,
c.LONGITUDE activityScanLongitude
from c
Join
(select value t from t in c.CarrierServiceDetails
where c.CarrierServiceDetails != null) CD
  WHERE c.AccountId ='1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
  AND c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'
  AND c.UpsOrderNumber = '86572804'
  AND c.MilestoneName = 'DELIVERED'
  AND c.is_inbound=0
  and c.is_deleted=0