/****** Object:  StoredProcedure [digital].[rpt_Inbound_Shipment_Detail_Activity]    Script Date: 2/17/2022 5:23:05 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



/**** 
--AMR
EXEC [digital].[rpt_Inbound_Shipment_Detail_Activity]    @DPProductLineKey = 'E648FA6F-6253-428E-8AC9-201E3EF83B91',@DPServiceLineKey = '53D60776-D3BD-4E6A-8E37-F591C148294B',@DPEntityKey = NULL,@UPSShipmentNumber = '45297',@MilestoneName=NULL
--SWR
EXEC [digital].[rpt_Inbound_Shipment_Detail_Activity]    @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = '150E5128-B3E3-44A1-BF6C-CD2E4D9856DA',@DPEntityKey = NULL,@UPSShipmentNumber = '6391',@MilestoneName=NULL
--Cambium
EXEC [digital].[rpt_Inbound_Shipment_Detail_Activity]    @DPProductLineKey = 'A10F512B-C7F4-42A0-909C-21DD95A7D921',@DPServiceLineKey = '53DDDBC2-FB5A-4E70-A92E-8312DC9FDD05',@DPEntityKey = NULL,@UPSShipmentNumber = '6391',@MilestoneName=NULL
****/

CREATE PROCEDURE [digital].[rpt_Inbound_Shipment_Detail_Activity] @DPProductLineKey varchar(50), @DPServiceLineKey varchar(50), @DPEntityKey varchar(50), @UPSShipmentNumber varchar(50), @MileStoneName varchar(128)


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
    MA.UPSASNNumber AS upsASNNumber,
    MA.MilestoneName AS milestoneName,
    MA.ActivityName AS activityName,
    MA.ACTIVITY_NOTES activityNote1,
    MA.[VENDOR_NAME] activityNote2,
    MA.ActivityDate AS activityDateTime,
    MA.ActivityCompletionFlag AS isActivityCompleted,
	O.ServiceLevel shipmentServiceLevel,
    O.ServiceLevelCode shipmentServiceLevelCode,
	CASE WHEN MA.MilestoneName='FTZ' THEN MA.FTZ_Status ELSE NULL END AS FTZStatus
  FROM [Summary].[DIGITAL_SUMMARY_ORDERS](NOLOCK) O
  LEFT JOIN [Summary].DIGITAL_SUMMARY_MILESTONE_ACTIVITY(NOLOCK) MA 
  ON O.UPSOrderNumber = MA.UPSOrderNumber AND MA.SourceSystemKey = CASE WHEN O.SourceSystemKey = 1011 THEN MA.SourceSystemKey ELSE O.SourceSystemKey END
  WHERE O.AccountId = @VarAccountID
  AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')
  --AND (ISNULL(O.DP_ORGENTITY_KEY, '') = @VarDPEntityKey OR @VarDPEntityKey = '*')
  AND (O.UPSOrderNumber = @UPSShipmentNumber)
  AND (MA.MilestoneName = @VarMileStoneName OR @VarMileStoneName = '*')
  AND O.IS_INBOUND=1
  AND MA.ActivityDate is not null

END
GO

85986946
