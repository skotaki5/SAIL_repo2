-- #Author :Mahesh Rathi
-- #DESCRIPITION : rpt_Inbound_Shipment_Detail_Activity
-- #DATE : 18-02-2022

-- Result set 1 :

--Parameter Requirement info
--DPProductLineKey and  UPSShipmentNumber are required,
--DPServiceLineKey is optional

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
WHERE c.AccountId = @DPProductLineKey
    AND c.DP_SERVICELINE_KEY = @DPServiceLineKey
    AND c.UpsOrderNumber = @UPSShipmentNumber
    AND c.MilestoneName = @MilestoneName
    AND c.is_inbound = 1
    AND c.is_deleted = 0
    AND c.ActivityDate != null