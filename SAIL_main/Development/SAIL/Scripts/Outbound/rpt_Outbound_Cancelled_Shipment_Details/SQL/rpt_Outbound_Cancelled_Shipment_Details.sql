/****** Object:  StoredProcedure [digital].[rpt_Outbound_Cancelled_Shipment_Details]    Script Date: 3/28/2022 4:57:14 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






/***
CHANGE LOG
----------
CHANGED BY				DATE			   SPRINT 	       CHANGES
PRASHANT RAI     		08/26/2021		   36th            @shipmentCanceledOnStartDate AND @shipmentCanceledOnEndDate ARE INTRODUCED AS INPUT PARAMETERS
VENKATA					09/01/2021						   Adjusted the paraenthesis for DateTimeCancelled column as it was inside the other AND condition, changed datatype to datetime instead of date for @VarshipmentCanceledOnStartDateTime & @VarshipmentCanceledOnEndDateTime   		
ANAND					11/18/2021						   Optimization. Created 3 temp tables to store [Summary].[DIGITAL_SUMMARY_ORDERS],Summary.DIGITAL_SUMMARY_ORDER_TRACKING and Summary.DIGITAL_SUMMARY_ORDER_LINES
SHALAKA					02/11/2022		   49, CL386	   Default NULL value for newly added parameter IS_MANAGED and logic to filer the data
Manju					03/11/2022		   S51_CL412	   Changes for OrderType Input parameter
***/

/****
--CL412--SWAROVSKI
 EXEC [digital].[rpt_Outbound_Cancelled_Shipment_Details]   
  @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9'
  ,@DPServiceLineKey = '*'
  ,@DPEntityKey = NULL
  ,@startDate = '2022-03-04'
  ,@endDate = '2022-03-11'
  ,@TopRow = 0
  ,@warehouseId = '*'
  ,@shipmentType = null
  ,@IsManaged='N'
  ,@OrderType='National Accounts'

--SWK --UPDATED EXECUTION SCRIPT
  EXEC [digital].[rpt_Outbound_Cancelled_Shipment_Details]   
  @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9'
  ,@DPServiceLineKey = '*',@DPEntityKey = NULL
  ,@startDate = '2021-11-16',@endDate = '2022-02-14',@TopRow = 0
  ,@warehouseId = '*', @shipmentType = null,@IsManaged='Y'

--AMR
  EXEC [digital].[rpt_Outbound_Cancelled_Shipment_Details]   @DPProductLineKey = 'DF603662-C05E-454F-98F3-5E74CF5B5AA7',@DPServiceLineKey = '*',@DPEntityKey = NULL,@startDate = '2021-01-01',@endDate = '2021-02-25',@TopRow = 100,@warehouseId = '*', @shipmentType = null
--SWR
  EXEC [digital].[rpt_Outbound_Cancelled_Shipment_Details]   @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = '150E5128-B3E3-44A1-BF6C-CD2E4D9856DA',@DPEntityKey = NULL,@startDate = '2021-01-01',@endDate = '2021-02-05',@TopRow = 100,@warehouseId = '*', @shipmentType = null,@OrderType = '*'
--Cambium
  EXEC [digital].[rpt_Outbound_Cancelled_Shipment_Details]   @DPProductLineKey = 'A10F512B-C7F4-42A0-909C-21DD95A7D921',@DPServiceLineKey = '53DDDBC2-FB5A-4E70-A92E-8312DC9FDD05',@DPEntityKey = NULL,@startDate = '2022-01-01',@endDate = '2022-02-05',@TopRow = 100,@warehouseId = '*', @shipmentType = null,@OrderType = NULL 

****/

CREATE PROCEDURE [digital].[rpt_Outbound_Cancelled_Shipment_Details]

@DPProductLineKey varchar(50)=NULL, 
@DPServiceLineKey varchar(50)=NULL, 
@DPEntityKey varchar(50)=NULL,
@startDate date, 
@endDate date, 
@shipmentCanceledOnStartDate date = NULL,
@shipmentCanceledOnEndDate date = NULL,
@AccountKeys nvarchar(max) = NULL,
@warehouseId varchar(max), 
@topRow int, 
@shipmentType varchar(50) = null ,
@IsManaged varchar(10) = '*'---CL386
,@orderType varchar(max) = '*' --CL412

AS

BEGIN

	DECLARE @VarAccountID varchar(50),
          @VarDPServiceLineKey varchar(50),
          @VarDPEntityKey varchar(50),
          @VarStartCreatedDateTime datetime,
          @VarEndCreatedDateTime datetime,
          @NULLCreatedDate varchar(1),
          @VarwarehouseId varchar(max),
		  @VarshipmentType varchar(50),
		  @NULLShipmentType varchar(1),
		  @IS_INBOUND           INT,
		  @VarDPServiceLineKeyJSON VARCHAR(500),
		  @VarDPProductLineKeyJSON VARCHAR(500),
		  @VarshipmentCanceledOnStartDateTime datetime ,
          @VarshipmentCanceledOnEndDateTime datetime,
		  @NULLCancelDate varchar(1),
		  @varIsManaged VARCHAR(10), --CL386
		  @NULLIsManaged VARCHAR(10), --CL386
		  @varOrderType	VARCHAR(max), --CL412
		  @NullOrderType VARCHAR(max) --CL412
		  
	SET @VarAccountID = UPPER(@DPProductLineKey)
	SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)
	SET @VarDPEntityKey = UPPER(@DPEntityKey)
	SET @VarwarehouseId = UPPER(@warehouseId)
	SET @VarshipmentType = UPPER(@shipmentType)
	SET @VarStartCreatedDateTime = @startDate
	SET @VarEndCreatedDateTime = @endDate
	SET @VarEndCreatedDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @VarEndCreatedDateTime)))
	SET @VarshipmentCanceledOnStartDateTime = @shipmentCanceledOnStartDate
	SET @VarshipmentCanceledOnEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @shipmentCanceledOnEndDate)))
	SET @varIsManaged = CASE WHEN UPPER(@IsManaged) = 'Y' THEN '1' 
						   WHEN UPPER(@IsManaged) = 'N' THEN '0' ELSE @IsManaged END ----CL386
	SET @varOrderType = UPPER(@OrderType) --CL412

	SELECT UPPER(DPProductLineKey) AS DPProductLineKey,
	       UPPER(DPServiceLineKey) AS DPServiceLineKey
		   into #ACCOUNTINFO
		   FROM OPENJSON(@AccountKeys)
		   WITH(
	   DPProductLineKey VARCHAR(MAX),
	   DPServiceLineKey VARCHAR(MAX)
	   )

	IF @VarStartCreatedDateTime IS NULL OR @VarEndCreatedDateTime IS NULL
	    SET @NULLCreatedDate = '*'
	
	IF @VarshipmentCanceledOnStartDateTime IS NULL OR @VarshipmentCanceledOnEndDateTime IS NULL
	    SET @NULLCancelDate = '*'
	
	IF @DPServiceLineKey IS NULL
	    SET @VarDPServiceLineKey = '*'
	
	IF @DPEntityKey IS NULL
	    SET @VarDPEntityKey = '*'
	
	  
	IF @VarshipmentType='' OR ISNULL(@VarshipmentType,'*')='*'
		SET @VarshipmentType = 'OUTBOUND'
  
		SET @IS_INBOUND = CASE WHEN @VarshipmentType='OUTBOUND' THEN 0
						  WHEN @VarshipmentType='MOVEMENT' THEN 2
                     END

     -- BACKWARD COMPATIBILITY

	IF NOT EXISTS ( SELECT DPServiceLineKey FROM #ACCOUNTINFO WHERE DPServiceLineKey IS NOT NULL) 
	    SET @VarDPServiceLineKeyJSON = '*'
	
	IF (( @DPServiceLineKey IS NOT NULL) AND @VarDPServiceLineKeyJSON = '*')
	    SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)
	
	IF (@VarDPServiceLineKeyJSON = '*' AND ISNULL(@DPServiceLineKey, '*') = '*')
	    SET @VarDPServiceLineKey = '*'
	
	
	IF NOT EXISTS ( SELECT DPProductLineKey FROM #ACCOUNTINFO WHERE DPProductLineKey IS NOT NULL) 
	    SET @VarDPProductLineKeyJSON = '*'
	
	IF (( @DPProductLineKey IS NOT NULL) AND @VarDPProductLineKeyJSON = '*')
		SET @VarAccountID = UPPER(@DPProductLineKey)

	IF ISNULL(@varIsManaged,'')='' OR @varIsManaged = '*'--CL386
		SET @NULLIsManaged = '*'

	IF @varOrderType = '*' --CL412
		SET @NullOrderType = '*' --CL412

/*********************************************************/
--Order Type 
/*********************************************************/

--CL412
IF Object_id('tempdb..#OrderType') IS NOT NULL 
	DROP TABLE #OrderType
SELECT
  Upper(value) AS orderType
INTO   #OrderType
FROM   String_split(@varOrderType, ',')

--CL412
	
	--11/17 Optimization------
	-- DIGITAL_SUMMARY_ORDERS Temp Table
	SELECT * INTO #DIGITAL_SUMMARY_ORDERS_TEMP
	 FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O
	 WHERE  ( O.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR O.AccountId = @VarAccountID ) 
	 AND ((O.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR O.DP_SERVICELINE_KEY = @VarDPServiceLineKey)  OR @VarDPServiceLineKey = '*')
	 AND O.IS_INBOUND=@IS_INBOUND
	 AND ((CAST(O.IS_MANAGED AS VARCHAR(10)) = @varIsManaged) OR @NULLIsManaged = '*') --CL386
	 AND (O.FacilityId IN (SELECT UPPER (TRIM (value))FROM string_split(@VarwarehouseId, ',')) OR @VarwarehouseId = '*')--CL412
	 AND (UPPER(ISNULL(O.OrderType,'')) in (Select orderType from #OrderType) or @NullOrderType = '*' ) --CL412   

    -- DIGITAL_SUMMARY_ORDER_TRACKING Temp Table
	SELECT DISTINCT  SOT.TRACKING_NUMBER ,SOT.UPSOrderNumber
	INTO #DIGITAL_SUMMARY_ORDER_TRACKING
	FROM Summary.DIGITAL_SUMMARY_ORDER_TRACKING SOT (NOLOCK) 
	JOIN #DIGITAL_SUMMARY_ORDERS_TEMP O
	ON  O.UPSOrderNumber = SOT.UPSOrderNumber
	WHERE ISNULL(SOT.TRACKING_NUMBER,'')<>''

	-- DIGITAL_SUMMARY_ORDER_LINES Temp Table
	SELECT OL.ShipmentLineCanceledFlag , OL.UPSOrderNumber, OL.SourceSystemKey,OL.ShipmentLineCanceledDate
	INTO #DIGITAL_SUMMARY_ORDER_LINES
	FROM Summary.DIGITAL_SUMMARY_ORDER_LINES  (NOLOCK) OL
	JOIN #DIGITAL_SUMMARY_ORDERS_TEMP O 
	ON O.UPSOrderNumber=OL.UPSOrderNumber
	AND O.SourceSystemKey=OL.SourceSystemKey
	WHERE ISNULL(OL.ShipmentLineCanceledFlag,'Y') = 'Y'	

	--11/17 Optimization-----	

	SELECT
    O.[DateTimeCancelled] shipmentCanceledDateTime,
    'shipmentCanceledBy' shipmentCanceledBy,
    O.[CancelledReasonCode] shipmentCanceledReason,
    O.[UPSOrderNumber] upsShipmentNumber,
    O.[OrderNumber] clientShipmentNumber,
    O.UPSTransportShipmentNumber shipmentNumber,
    O.[OrderNumber] referenceNumber,
    O.CustomerPO customerPONumber,
    O.[UPSOrderNumber] orderNumber,
    O.Carrier shipmentCarrier,
    O.CarrierCode shipmentCarrierCode,
    O.ServiceLevel shipmentServiceLevel,
    O.[ServiceLevelCode] shipmentServiceLevelCode,
    O.ServiceMode serviceMode,
	NULL AS LineNumber,
	NULL AS shipmentLineCanceledDateTime,
	NULL AS shipmentLineCanceledBy,
	NULL AS shipmentLineCanceledReason,
	CASE WHEN O.IS_INBOUND=0 THEN 'Outbound'
	     WHEN O.IS_INBOUND=2 THEN 'Movement'
		 END AS shipmentType,
	(CASE WHEN 
			 EXISTS (SELECT ShipmentLineCanceledFlag 
					 FROM #DIGITAL_SUMMARY_ORDER_LINES OL
					 WHERE OL.UPSOrderNumber=O.UPSOrderNumber
					   AND OL.SourceSystemKey=O.SourceSystemKey
					 )
			   THEN 'Y' ELSE 'N' END
	 ) AS ShipmentLineCanceledFlag,
	 O.OriginTimeZone shipmentCanceledDateTimeZone,
	 	 '{"carrierShipmentNumber":  '+ JSON_QUERY(
                    '[' + STUFF(( SELECT DISTINCT ',' + '"' + SOT.TRACKING_NUMBER + '"' 
					FROM #DIGITAL_SUMMARY_ORDER_TRACKING SOT
					WHERE SOT.UPSOrderNumber = O.UPSOrderNumber 
					FOR XML PATH('')),1,1,'') + ']' )+'}' AS carrierShipmentNumber,
    O.Account_number AS accountNumber,
	O.AccountId AS dpProductLineKey,
	O.Is_MANAGED As Is_MANAGED,  --CL386
	O.OrderType--CL412
	INTO #DIGITAL_SUMMARY_ORDERS
	FROM #DIGITAL_SUMMARY_ORDERS_TEMP (NOLOCK) O
	WHERE  
	/*( O.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR AccountId = @VarAccountID ) 
	AND ((O.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR O.DP_SERVICELINE_KEY = @VarDPServiceLineKey)  OR @VarDPServiceLineKey = '*')
	--AND (ISNULL(O.DP_ORGENTITY_KEY, '') = @VarDPEntityKey OR @VarDPEntityKey = '*')
	AND (O.FacilityId IN (SELECT UPPER (TRIM (value))FROM string_split(@VarwarehouseId, ',')) OR @VarwarehouseId = '*')
	AND O.IS_INBOUND=@IS_INBOUND
	AND*/-- Moved to #DIGITAL_SUMMARY_ORDERS_TEMP insert section as part of 412
	((((O.DateTimeCancelled  BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime) OR @NULLCreatedDate = '*') AND O.OrderCancelledFlag = 'Y' )
	OR EXISTS(SELECT OL2.ShipmentLineCanceledFlag 
				FROM #DIGITAL_SUMMARY_ORDER_LINES  (NOLOCK) OL2
				WHERE OL2.UPSOrderNumber=O.UPSOrderNumber
				AND OL2.SourceSystemKey=O.SourceSystemKey
				AND ((OL2.ShipmentLineCanceledDate BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime) OR @NULLCreatedDate = '*')
				AND ISNULL(OL2.ShipmentLineCanceledFlag,'Y') = 'Y'))  
	AND ((O.DateTimeCancelled BETWEEN @VarshipmentCanceledOnStartDateTime AND  @VarshipmentCanceledOnEndDateTime) OR @NULLCancelDate = '*')
	--AND ((CAST(O.IS_MANAGED AS VARCHAR(10)) = @varIsManaged) OR @NULLIsManaged = '*') --CL386
										

    SELECT COUNT(1) totalCount FROM #DIGITAL_SUMMARY_ORDERS

IF @topRow = 0

SELECT 
	shipmentCanceledDateTime,
    shipmentCanceledBy,
    shipmentCanceledReason,
	LineNumber,
	shipmentLineCanceledDateTime,
	shipmentLineCanceledBy,
	shipmentLineCanceledReason,
    upsShipmentNumber,
    clientShipmentNumber,
    shipmentNumber,
    referenceNumber,
    customerPONumber,
    orderNumber,
    shipmentCarrier,
    shipmentCarrierCode,
    shipmentServiceLevel,
    shipmentServiceLevelCode,
    serviceMode,
	ISNULL(ShipmentLineCanceledFlag,'N') AS ShipmentLineCanceledFlag,
	shipmentServiceLevel AS serviceName,
    shipmentCanceledDateTimeZone,
	shipmentType,
	carrierShipmentNumber,
	'accountNumber' accountNumber,
	dpProductLineKey,
	orderType--CL412
FROM #DIGITAL_SUMMARY_ORDERS 
ORDER BY shipmentCanceledDateTime DESC

ELSE

SELECT TOP (@topRow) 
    shipmentCanceledDateTime,
    shipmentCanceledBy,
    shipmentCanceledReason,
	LineNumber,
	shipmentLineCanceledDateTime,
	shipmentLineCanceledBy,
	shipmentLineCanceledReason,
    upsShipmentNumber,
    clientShipmentNumber,
    shipmentNumber,
    referenceNumber,
    customerPONumber,
    orderNumber,
    shipmentCarrier,
    shipmentCarrierCode,
    shipmentServiceLevel,
    shipmentServiceLevelCode,
    serviceMode,
	ISNULL(ShipmentLineCanceledFlag,'N') AS ShipmentLineCanceledFlag,
	shipmentServiceLevel AS serviceName,
    shipmentCanceledDateTimeZone,
	shipmentType,
	carrierShipmentNumber,
	accountNumber,
	dpProductLineKey,
	orderType--CL412
FROM #DIGITAL_SUMMARY_ORDERS 
ORDER BY shipmentCanceledDateTime DESC

END
GO

