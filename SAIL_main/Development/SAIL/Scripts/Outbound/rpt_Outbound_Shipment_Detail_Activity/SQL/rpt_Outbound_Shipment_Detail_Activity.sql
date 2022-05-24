/****** Object:  StoredProcedure [digital].[rpt_Outbound_Shipment_Detail_Activity]    Script Date: 3/17/2022 11:23:21 AM ******/
SET ANSI_NULLS ON
GO
 
SET QUOTED_IDENTIFIER ON
GO
 
 
 
 
 
 
 
/**------------------------------------------------------------CHANGE LOG---------------------------------------------------------------------------------
DEVELOPER           DATE            SPRINT              COMMENTS
Venkata             02/25/2022      Sprint50-CL406      Added leftAt column to the result set
Rajeev              03/11/2022      Sprint51-CL429      Added activityScanLongitude and activityScanLatitude to the result set
-----------------------------------------------------------------------------------------------------------------------------------------------------------**/
 
 
/****
--AMR
EXEC [digital].[rpt_Outbound_Shipment_Detail_Activity]    @DPProductLineKey = 'E648FA6F-6253-428E-8AC9-201E3EF83B91',@DPServiceLineKey = '53D60776-D3BD-4E6A-8E37-F591C148294B',@DPEntityKey = NULL,@UPSShipmentNumber = '5799152',@MilestoneName=NULL
--SWR
EXEC [digital].[rpt_Outbound_Shipment_Detail_Activity]    @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = '150E5128-B3E3-44A1-BF6C-CD2E4D9856DA',@DPEntityKey = NULL,@UPSShipmentNumber = '2952616',@MilestoneName=NULL
--Cambium
EXEC [digital].[rpt_Outbound_Shipment_Detail_Activity]   
@DPProductLineKey = '11A8B107-C83E-4F51-BD68-9CFEEA124A71',
@DPServiceLineKey = '*',@DPEntityKey = NULL,@UPSShipmentNumber = '86405412',@MilestoneName=NULL
****/
 
CREATE PROCEDURE [digital].[rpt_Outbound_Shipment_Detail_Activity]
 
@DPProductLineKey varchar(50), @DPServiceLineKey varchar(50), @DPEntityKey varchar(50), @UPSShipmentNumber varchar(50),
@MileStoneName varchar(128)
 
AS
 
BEGIN
 
  DECLARE @VarAccountID varchar(50),
          @VarDPServiceLineKey varchar(50),
          @VarDPEntityKey varchar(50),
          @VarMileStoneName varchar(128)
           
  SET @VarAccountID = UPPER(@DPProductLineKey)
  SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)
  SET @VarDPEntityKey = UPPER(@DPEntityKey)
  SET @VarMileStoneName = UPPER(@MileStoneName)
   
  IF @DPServiceLineKey IS NULL
    SET @VarDPServiceLineKey = '*'
 
  IF @DPEntityKey IS NULL
    SET @VarDPEntityKey = '*'
 
  IF @MileStoneName IS NULL
    SET @VarMileStoneName = '*'
 
  SELECT
    MA.TrackingNumber AS trackingNumber,
    O.CarrierCode AS carrierCode,
    O.Carrier AS CarrierName,
    MA.MilestoneName AS milestoneName,
    MA.ActivityName AS activityName,
    MA.ACTIVITY_NOTES AS activityNote1,
    NULL AS activityNote2,
    ----MA.[VENDOR_NAME] AS activityNote2,
    MA.ActivityDate AS activityDateTime,
    MA.ActivityCompletionFlag AS isActivityCompleted,
    MA.SEGMENT_ID AS Segment,
    O.ServiceLevel shipmentServiceLevel,
    O.ServiceLevelCode shipmentServiceLevelCode,
    MA.[PROOF_OF_DELIVERY_NAME] AS proofOfDelivery,
    MA.[CARRIER_TYPE] AS carrierType,
    MA.TimeZone AS activityDateTimeZone,
    ISNULL(MA.LOGI_NEXT_FLAG,'N') AS additionalTrackingIndicator,
    MA.PROOF_OF_DELIVERY_LOCATION AS leftAt, --CL406
    MA.[LATITUDE] AS activityScanLatitude , --CL429
    MA.[LONGITUDE] AS activityScanLongitude --CL429
  FROM [Summary].[DIGITAL_SUMMARY_ORDERS](NOLOCK) O
  LEFT JOIN [Summary].DIGITAL_SUMMARY_MILESTONE_ACTIVITY(NOLOCK) MA ON O.UPSOrderNumber = MA.UPSOrderNumber
                                                                    AND O.SourceSystemKey = MA.SourceSystemKey
  WHERE O.AccountId = @VarAccountID
  AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')
  --AND (ISNULL(O.DP_ORGENTITY_KEY, '') = @VarDPEntityKey OR @VarDPEntityKey = '*')
  AND (O.UPSOrderNumber = @UPSShipmentNumber)
  AND (MA.MilestoneName = @VarMileStoneName OR @VarMileStoneName = '*')
  AND O.IS_INBOUND=0
 
END
GO