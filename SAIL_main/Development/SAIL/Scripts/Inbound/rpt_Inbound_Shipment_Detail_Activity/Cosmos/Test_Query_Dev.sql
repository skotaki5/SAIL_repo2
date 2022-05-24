-- #Author :Mahesh Rathi
-- #DESCRIPITION : rpt_Inbound_Shipment_Detail_Activity
-- #DATE : 18-02-2022

-- Result set 1 :

--Target container digital_summary_milestone_activity

SELECT c.TrackingNumber trackingNumber
    ,CD.carrierCode
    ,CD.CarrierName
    ,c.UPSASNNumber
    ,c.MilestoneName milestoneName
    ,c.ActivityName activityName
    ,c.ACTIVITY_NOTES activityNote1
    ,c.VENDOR_NAME activityNote2
    ,c.ActivityDate activityDateTime
    ,c.ActivityCompletionFlag isActivityCompleted
    ,CD.shipmentServiceLevel
    ,CD.shipmentServiceLevelCode
    ,c.FTZStatus
FROM c
Join CD in c.CarrierServiceDetails
WHERE c.AccountId = 'E648FA6F-6253-428E-8AC9-201E3EF83B91'
    AND c.DP_SERVICELINE_KEY = '53D60776-D3BD-4E6A-8E37-F591C148294B'
    AND c.UpsOrderNumber = '602975'
    AND c.MilestoneName = 'ASN CREATED'
    AND c.is_inbound = 1
    AND c.is_deleted = 0
    AND c.ActivityDate != null