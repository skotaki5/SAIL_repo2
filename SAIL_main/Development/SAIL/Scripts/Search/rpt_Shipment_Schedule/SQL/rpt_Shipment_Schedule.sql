/****** Object:  StoredProcedure [digital].[rpt_Shipment_Schedule]    Script Date: 12/7/2021 4:43:31 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--SELECT TOP 10 * FROM Summary.DIGITAL_SUMMARY_TRANSPORTATION WHERE LoadLatestDeliveryDate IS NOT NULL
/**** 
--AMR
EXEC [digital].[rpt_Shipment_Schedule]   @DPProductLineKey = '0C061A26-767B-436C-B78E-A65DBE24E2B3',@DPServiceLineKey = '*',@DPEntityKey = NULL,@UPSOrderNumber='SN6260619', @shipmentType='MOVEMENT'
--SWR
EXEC [digital].[rpt_Shipment_Schedule]  @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = '150E5128-B3E3-44A1-BF6C-CD2E4D9856DA',@DPEntityKey = NULL,@UPSOrderNumber='2964569', @shipmentType=''
--Cambium
EXEC [digital].[rpt_Shipment_Schedule]   @DPProductLineKey = 'A10F512B-C7F4-42A0-909C-21DD95A7D921',@DPServiceLineKey = '53DDDBC2-FB5A-4E70-A92E-8312DC9FDD05',@DPEntityKey = NULL,@UPSOrderNumber='1111', @shipmentType=''
****/

CREATE PROCEDURE [digital].[rpt_Shipment_Schedule] 

@DPProductLineKey varchar(50), @DPServiceLineKey varchar(50), @DPEntityKey varchar(50),
@UPSOrderNumber varchar(50), @shipmentType varchar(10)=''

AS

BEGIN

  DECLARE @VarAccountID varchar(50),
          @VarDPServiceLineKey varchar(50),
          @VarDPEntityKey varchar(50),
		  @VarShipmentType varchar(10),
		  @NULLShipmentType varchar(1),
		  @IS_INBOUND          INT

  SET @VarAccountID = UPPER(@DPProductLineKey)
  SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)
  SET @VarDPEntityKey = UPPER(@DPEntityKey)
  SET @VarShipmentType = UPPER(@shipmentType)
  
  IF @DPServiceLineKey IS NULL
    SET @VarDPServiceLineKey = '*'

  IF @DPEntityKey IS NULL
    SET @VarDPEntityKey = '*'

    IF @VarShipmentType='' OR @VarShipmentType IS NULL
     SET @NULLShipmentType = '*'
  ELSE
   SET @IS_INBOUND = CASE WHEN @VarShipmentType='INBOUND' THEN 1
                          WHEN @VarShipmentType='MOVEMENT' THEN 2
                          WHEN @VarShipmentType='OUTBOUND' THEN 0 
                END


	SELECT 
	 O.UPSOrderNumber as upsShipmentNumber
	,MAX(MA1.ActivityDate) AS ActualDeliveryDateTime
	,CASE WHEN @IS_INBOUND = 0 THEN O.OriginalScheduledDeliveryDateTime ELSE TR.LoadLatestDeliveryDate END	AS OriginalScheduledDeliveryDateTime
	,CASE WHEN @IS_INBOUND = 0 THEN O.OriginalScheduledDeliveryDateTime ELSE TR.LoadLatestDeliveryDate END	AS ActualScheduledDeliveryDateTime
	,MAX(MA.ActivityDate) AS shipmentEstimatedDateTime
	,DateTimeReceived as shipmentCreationDateTime
	,DateTimeShipped as actualShipmentDateTime
	,DateTimeReceived as shipmentPlaceDateTime
	,O.PickUPDateTime AS originalPickupDateTime
	,O.DateTimeShipped AS actualPickupDateTime
	,O.DestinationTimeZone AS actualDeliveryDateTimeZone
	,O.DestinationTimeZone AS originalScheduledDeliveryDateTimeZone
	,O.ActualScheduledDeliveryDateTimeZone AS actualScheduledDeliveryDateTimeZone
	,O.DestinationTimeZone AS shipmentEstimatedDateTimeZone
	,O.OriginTimeZone AS shipmentCreationDateTimeZone
	FROM Summary.DIGITAL_SUMMARY_ORDERS (NOLOCK) O
	LEFT JOIN Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) MA 
			 ON O.UPSOrderNumber=MA.UPSOrderNumber
			AND O.SourceSystemKey=MA.SourceSystemKey
			AND MA.ActivityCode IN('AG','AB','AA','071')
	LEFT JOIN Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) MA1 
			 ON O.UPSOrderNumber=MA1.UPSOrderNumber
			AND O.SourceSystemKey=MA1.SourceSystemKey
			AND MA1.ActivityCode  IN('D','D1','D9','DELIVER','155')
    LEFT JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION (NOLOCK) TR 
	         ON (CASE WHEN TR.TrasOnlyFlag = 'NON_TRANS' THEN   ISNULL(TR.UPSWMSSOURCESYSTEMKEY,O.SOURCESYSTEMKEY) 
			                ELSE TR.SOURCESYSTEMKEY END = O.SOURCESYSTEMKEY 
							AND CASE WHEN TR.TRASONLYFLAG = 'NON_TRANS' 
							          THEN TR.UPSWMSORDERNUMBER ELSE TR.UPSORDERNUMBER END = O.UPSORDERNUMBER)
	WHERE O.AccountId = @VarAccountID
    AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')
    AND O.UPSOrderNumber=@UPSOrderNumber
    AND (@NULLShipmentType='*' OR O.IS_INBOUND=@IS_INBOUND)
	GROUP BY 
	 O.UPSOrderNumber
	,TR.LoadEarliestDeliveryDate
	,TR.LoadLatestDeliveryDate
	,DateTimeReceived
	,DateTimeShipped
	,DateTimeReceived
	,O.PickUPDateTime
	,O.ScheduledPickUpDateTime
	,O.OriginTimeZone
	,O.DestinationTimeZone
	,O.ActualScheduledDeliveryDateTimeZone
	,O.OriginalScheduledDeliveryDateTime

END
GO

