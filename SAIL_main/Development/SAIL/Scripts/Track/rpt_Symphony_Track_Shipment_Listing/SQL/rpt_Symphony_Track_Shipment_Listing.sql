/****** Object:  StoredProcedure [digital].[rpt_Symphony_Track_Shipment_Listing]    Script Date: 3/15/2022 10:39:56 AM ******/
SET ANSI_NULLS ON
GO
 
SET QUOTED_IDENTIFIER ON
GO
 
 
 
 
 
 
 
 
/*****
 
Sheetal/Avinash         01/24/2022                     New SP for Non-Authorized User tracking
AVIANSH                 01/28/2022    UPSGLD-12987     Changed alias name from "City" to "milestoneLocation_city" and added single underscore(_)
                                                       instead of double underscore(__) for milestone related columns
Sheetal                 01/31/2022      CL371           Made changes in result sets as per CL371
Harsha                  02/11/2022                      Adding table alias wherver required
Avinash                 02/14/2022                      Added missing alias name
Sheetal                 02/18/2022    UPSGLD-14102      Added Origin result set
Sheetal                 02/28/2022      CL398           Add OriginCountry details.  Needed for LoginNext requirement
 
Exec [digital].[rpt_Symphony_Track_Shipment_Listing]
@accountNumber ='393004',
@shipmentNumber = '0837851966'
 
*********/
 
 
 
CREATE PROCEDURE [digital].[rpt_Symphony_Track_Shipment_Listing]
@accountNumber varchar(50),
@shipmentNumber varchar(50)
 
AS
BEGIN
 
DECLARE   
@VaraccountNumber VARCHAR(50),
@VarshipmentNumber VARCHAR(50),
@SS_MG int
               
SET @VaraccountNumber = UPPER(@accountNumber);
SET @VarshipmentNumber = UPPER(@shipmentNumber);
SET @SS_MG = 1011;
 
 
--Result Set 1
  
SELECT DISTINCT
O.UPSOrderNumber AS shipmentNumber
,O.ActualDeliveryDate AS actualDeliveryDateTime
,O.ActualShipmentDateTime AS shipmentDateTime
, MAX(MA.ActivityDate) AS shipmentEstimatedDateTime 
,O.DestinationTimeZone AS actualDeliveryTimeZone
,O.OriginTimeZone As shipmentDateTimeZone
,O.DestinationTimeZone AS shipmentEstimatedDateTimeZone
,O.CurrentMilestone AS milestoneStatus
,O.Carrier AS carrierName
,O.CarrierCode AS carrierCode
,T.TRACKING_NUMBER AS carrierShipmentNumber
,O.ServiceLevel AS carrierService
,O.ServiceLevel AS shipmentServiceLevel
,O.ServiceLevelCode AS shipmentServiceLevelCode
FROM Summary.DIGITAL_SUMMARY_ORDERS (NOLOCK) O
    LEFT JOIN Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) MA
             ON O.UPSOrderNumber=MA.UPSOrderNumber
            AND O.SourceSystemKey=MA.SourceSystemKey
            AND MA.ActivityCode IN('AG','AB','AA','071')
LEFT JOIN Summary.DIGITAL_SUMMARY_ORDER_TRACKING (NOLOCK) T ON O.UPSOrderNumber = T.UPSOrderNumber and O.SourceSystemKey = T.SourceSystemKey
AND T.TRACKING_NUMBER IS NOT NULL  --Added missing alias name
WHERE O.Account_number = @VaraccountNumber
And O.UPSOrderNumber = @VarshipmentNumber
GROUP BY
O.UPSOrderNumber
,O.ActualDeliveryDate
,O.ActualShipmentDateTime
,O.DestinationTimeZone
,O.OriginTimeZone
,O.DestinationTimeZone
,O.CurrentMilestone
,O.Carrier
,O.CarrierCode
,T.TRACKING_NUMBER
,O.ServiceLevel
,O.ServiceLevel
,O.ServiceLevelCode
 
 
--Result Set 2
 
 
 
SELECT
O.DestinationCity AS destinationAddress_City
,O.DestinationProvince AS destinationAddress_State
,O.DestinationCountry AS destinationAddress_Country
FROM Summary.DIGITAL_SUMMARY_ORDERS (NOLOCK) O
WHERE O.Account_number = @VaraccountNumber
And O.UPSOrderNumber = @VarshipmentNumber
 
 
 
--Result Set 3
 
 
SELECT milestoneName,           
            MAX(MilesStoneEstimatedDateTime) milestoneEstimatedDateTime,
            MAX(MilesStoneCompletionDateTime) AS milestoneCompletionDateTime,
            MAX(activityCount) AS milestoneActivityCount,
            --templateType AS templateType,
            milestoneOrder
    FROM
    (
  SELECT
    M.MilestoneName AS milestoneName,
    M.MilestoneOrder AS milestoneOrder,
    (MA.PlannedMilestoneDate) MilesStoneEstimatedDateTime,
    CASE WHEN MA.MilestoneCompletionFlag = 'Y' THEN ISNULL(MA.MilestoneDate, MA.ActivityDate) END MilesStoneCompletionDateTime,
    --ISNULL(MA.MilestoneDate, MA.ActivityDate)  MilesStoneCompletionDateTime,
    ISNULL(MA1.activityCount, 0) AS activityCount
    --,MTM.TransactionTypeName AS templateType
  FROM Summary.DIGITAL_SUMMARY_ORDERS (NOLOCK) O
  INNER JOIN [Summary].[DIGITAL_SUMMARY_MILESTONE](NOLOCK) M ON O.UPSOrderNumber = M.UPSOrderNumber and O.SourceSystemKey = M.SourceSystemKey
  LEFT JOIN [Summary].DIGITAL_SUMMARY_MILESTONE_ACTIVITY(NOLOCK) MA
        ON M.UPSOrderNumber = MA.UPSOrderNumber AND M.MilestoneOrder = MA.MilestoneOrder
            AND M.SourceSystemKey = CASE WHEN MA.SourceSystemKey = @SS_MG THEN M.SourceSystemKey ELSE MA.SourceSystemKey END           
  LEFT JOIN (SELECT COUNT(1) AS activityCount,MA.UPSOrderNumber,MA.SourceSystemKey,MA.MilestoneName
                    FROM Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) MA
                    WHERE MA.UPSOrderNumber = @VarshipmentNumber --AND MA.ActivityDate IS NOT NULL
                    GROUP BY MA.UPSOrderNumber,MA.SourceSystemKey,MA.MilestoneName
             ) MA1 ON M.UPSOrderNumber = MA1.UPSOrderNumber
                   AND M.SourceSystemKey = CASE WHEN MA1.SourceSystemKey = @SS_MG THEN M.SourceSystemKey ELSE MA.SourceSystemKey END
                   AND M.MilestoneName = MA1.MilestoneName
   LEFT JOIN master_data.Map_TransactionType_Milestone (NOLOCK) MTM ON MA.TransactionTypeId=MTM.TransactionTypeId
  WHERE O.Account_number = @VaraccountNumber
  AND (M.UPSOrderNumber = @VarshipmentNumber)
  AND M.MilestoneName <> 'ALERT'
  --AND MA.MilestoneCompletionFlag = 'Y'
  ) TBL
  GROUP BY milestoneName,
           milestoneOrder
           --,templateType
 ORDER BY milestoneOrder
 
--Result Set 4
 
 
 
SELECT
MA.MilestoneName AS milestoneName,
MA.TrackingNumber AS carrierShipmentNumber,
O.CarrierCode AS carrierCode,
O.Carrier AS carrierName,
O.ServiceLevel AS carrierService,
MA.SEGMENT_ID AS segment,
ISNULL(MA.LOGI_NEXT_FLAG,'N') AS additionalTrackingIndicator,
O.ServiceLevel AS shipmentServiceLevel,
O.ServiceLevelCode AS shipmentServiceLevelCode,
MA.[CARRIER_TYPE] AS carrierType,
MA.ActivityName AS activityName,
MA.ACTIVITY_NOTES AS activityNote1,
NULL AS activityNote2,
----MA.[VENDOR_NAME] AS activityNote2,
MA.ActivityDate AS activityDateTime,
MA.ActivityCompletionFlag AS isActivityCompleted,
MA.[PROOF_OF_DELIVERY_NAME] AS receivedBy,
PROOF_OF_DELIVERY_LOCATION AS leftAt,
MA.TimeZone AS activityDateTimeZone
FROM [Summary].[DIGITAL_SUMMARY_ORDERS](NOLOCK) O
LEFT JOIN [Summary].DIGITAL_SUMMARY_MILESTONE_ACTIVITY(NOLOCK) MA ON O.UPSOrderNumber = MA.UPSOrderNumber
                                                AND O.SourceSystemKey = MA.SourceSystemKey
WHERE O.Account_number = @VaraccountNumber
AND  O.UPSOrderNumber = @VarshipmentNumber
 
 
--CL398
--Result Set 5
 
SELECT
O.OriginCity AS originAddress_city
,O.OriginProvince AS originAddress_state
,O.OriginCountry AS originAddress_country
FROM Summary.DIGITAL_SUMMARY_ORDERS (NOLOCK) O
WHERE O.Account_number = @VaraccountNumber
And O.UPSOrderNumber = @VarshipmentNumber
 
--CL398
 
END
 
 
GO