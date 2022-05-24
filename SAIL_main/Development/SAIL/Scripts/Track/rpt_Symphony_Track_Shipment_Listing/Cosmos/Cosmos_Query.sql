--rpt_Symphony_Track_Shipment_Listing

--Result Set 1a

/*
Parameter Requirement info -
->@accountNumber  required 
->@shipmentNumber required

Target Container - digital_summary_orders
*/

SELECT DISTINCT
c.UPSOrderNumber shipmentNumber
,c.ActualDeliveryDate actualDeliveryDateTime
,c.ma_shipmentEstimatedDateTime shipmentEstimatedDateTime 
,c.originalScheduledDeliveryDateTimeZone  actualDeliveryTimeZone
,c.OriginTimeZone  shipmentDateTimeZone
,c.originalScheduledDeliveryDateTimeZone  shipmentEstimatedDateTimeZone
,c.milestoneStatus
,c.Carrier  carrierName
,c.shipmentCarrierCode carrierCode
,c.Track_num  carrierShipmentNumber
,c.ServiceLevel  carrierService
,c.ServiceLevel  shipmentServiceLevel
,c.shipmentServiceLevelCode shipmentServiceLevelCode
From c
WHERE c.Track_num != null
AND  c.Accountnumber = @accountNumber
And c.UPSOrderNumber = @shipmentNumber
and c.is_deleted = 0

--Result Set 1b

/*
Parameter Requirement info -
->@accountNumber  required 
->@shipmentNumber required

Target Container - digital_summary_milestone_activity
*/


SELECT MAX(c.ActivityDate) shipmentDateTime FROM c
join t in c.CarrierServiceDetails
where c.UpsOrderNumber = @shipmentNumber
and t.Account_number = @accountNumber
and c.MilestoneName = 'WAREHOUSE'
and c.ActivityName ='ORDER CLOSED'
and c.ActivityDate != null
and c.is_deleted = 0

--Result Set 2

/*
Parameter Requirement info -
->@accountNumber  required 
->@shipmentNumber required

Target Container - digital_summary_orders
*/

SELECT
O.DestinationCity AS destinationAddress_City
,O.shipmentDestination_stateProvince AS destinationAddress_State
,O.DestinationCountry AS destinationAddress_Country
FROM O
AND  O.Accountnumber = @accountNumber
And O.UPSOrderNumber = @shipmentNumber
And O.is_deleted = 0

--Result Set 3

/*
Parameter Requirement info -
->@accountNumber  required 
->@shipmentNumber required

Target Container - digital_summary_orders
*/

SELECT      
     t.ShipmentMileStone milestoneName
    ,t.MilesStoneEstimatedDateTime milestoneEstimatedDateTime
    ,t.MilesStoneCompletionDateTime milestoneCompletionDateTime
    ,t.activityCount milestoneActivityCount
    ,t.MilestoneOrder milestoneOrder 
FROM c
JOIN t IN c.DetailMilestone
Where c.Accountnumber = @accountNumber
And c.UPSOrderNumber = @shipmentNumber AND c.is_deleted = 0

--Result Set 4

/*
Parameter Requirement info -
->@accountNumber  required 
->@shipmentNumber required

Target Container - digital_summary_milestone_activity
*/


SELECT
c.MilestoneName milestoneName,
c.TrackingNumber  carrierShipmentNumber,
t.carrierCode carrierCode,
t.carrierCode  carrierName,
t.shipmentServiceLevel carrierService,
c.SEGMENT_ID segment,
is_null(c.LOGI_NEXT_FLAG)?'N':c.LOGI_NEXT_FLAG additionalTrackingIndicator,
t.shipmentServiceLevel shipmentServiceLevel,
t.shipmentServiceLevelCode shipmentServiceLevelCode,
c.CARRIER_TYPE carrierType,
c.ActivityName activityName,
c.ACTIVITY_NOTES  activityNote1,
null activityNote2,
c.ActivityDate activityDateTime,
c.ActivityCompletionFlag  isActivityCompleted,
c.PROOF_OF_DELIVERY_NAME receivedBy,
c.PROOF_OF_DELIVERY_LOCATION  leftAt,
c.TimeZone activityDateTimeZone
From c join t in c.CarrierServiceDetails
WHERE
AND  t.Account_number = @accountNumber
And c.UpsOrderNumber = @shipmentNumber  AND c.is_deleted = 0

--Result Set 5

/*
Parameter Requirement info -
->@accountNumber  required 
->@shipmentNumber required

Target Container - digital_summary_orders
*/

SELECT
O.OriginCity  originAddress_city
,O.shipmentOrigin_stateProvince originAddress_state
,O.OriginCountry originAddress_country
FROM O
AND  O.Accountnumber = @accountNumber
And O.UPSOrderNumber = @shipmentNumber AND O.is_deleted = 0