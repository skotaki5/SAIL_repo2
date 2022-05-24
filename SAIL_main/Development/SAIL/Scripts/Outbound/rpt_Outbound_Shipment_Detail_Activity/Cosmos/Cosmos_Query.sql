--rpt_Outbound_Shipment_Detail_Activity

--Result Set 1

/*
Parameter Requirement info -
->@DPProductLineKey  required 
->@DPServiceLineKey  optional
->@UPSOrderNumber required
->@MileStoneName optional

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
Join (select value t from t in c.CarrierServiceDetails where c.CarrierServiceDetails != null) CD
WHERE c.AccountId = @DPProductLineKey
AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
AND c.UpsOrderNumber = @UPSShipmentNumber
AND c.MilestoneName = @MileStoneName
AND c.is_inbound=0 and c.is_deleted=0