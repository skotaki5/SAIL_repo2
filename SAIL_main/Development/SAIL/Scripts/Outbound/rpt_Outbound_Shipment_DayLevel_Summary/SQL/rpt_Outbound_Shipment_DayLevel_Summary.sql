/****** Object:  StoredProcedure [digital].[rpt_Outbound_Shipment_DayLevel_Summary]    Script Date: 3/30/2022 2:04:11 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




/**--------------------------------CHANGE LOG------------------------------------------------------------
DEVELOPER			SPRINT				DATE				CHANGES
VENKATA				SPRINT49 CL385		02/10/2022			ADDED @IsManaged input parameter
REVATHY				51- CL411	        03/09/2022			Added Shells for OrderType Input parameter
SHEETAL				50/51 - CL417		03/09/2022			Added logic to accept Date Type as actualDeliveryDate
REVATHY             51-CL411            03/11/2022          ADDED Ordertype filter condition
VENKATA				51-CL431			03/15/2022			Added cancelledorders flag to filter the cancelled orders
----------------------------------------------------------------------------------------------------------**/

/**** 
--AMR
EXEC [digital].[rpt_Outbound_Shipment_DayLevel_Summary]   @DPProductLineKey = 'E648FA6F-6253-428E-8AC9-201E3EF83B91',@DPServiceLineKey = '53D60776-D3BD-4E6A-8E37-F591C148294B',@DPEntityKey = NULL,@StartDate = '2020-08-08',@EndDate = '2020-08-14', @warehouseId = '*',@DateType=''
--SWR
EXEC [digital].[rpt_Outbound_Shipment_DayLevel_Summary]  @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = '150E5128-B3E3-44A1-BF6C-CD2E4D9856DA',@DPEntityKey = NULL,@StartDate = '2022-01-01',@EndDate = '2022-03-01', @warehouseId = '*',@DateType='shipmentCreationDate', @OrderType = '*'
--Cambium
EXEC [digital].[rpt_Outbound_Shipment_DayLevel_Summary]   @DPProductLineKey = 'A10F512B-C7F4-42A0-909C-21DD95A7D921',@DPServiceLineKey = '53DDDBC2-FB5A-4E70-A92E-8312DC9FDD05',@DPEntityKey = NULL,@StartDate = '2022-01-01',@EndDate = '2022-03-03', @warehouseId = '*',@DateType='shipmentCreationDate',@OrderType = NULL

****/

CREATE  PROCEDURE [digital].[rpt_Outbound_Shipment_DayLevel_Summary] 

@DPProductLineKey varchar(50)=null, @DPServiceLineKey varchar(50)=null, @DPEntityKey varchar(50)=null, @AccountKeys nvarchar(max) = NULL,
@StartDate date = NULL, @EndDate date = NULL, @DateType varchar(50), @warehouseId varchar(max) = NULL
,@orderType varchar(max) = '*' --CL411
,@IsManaged varchar(10) = NULL --CL385


AS

BEGIN

  DECLARE @VarAccountID varchar(50),
          @VarDPServiceLineKey varchar(50),
          @VarDPEntityKey varchar(50),
		  @VarStartCreatedDateTime datetime,
          @VarEndCreatedDateTime datetime,
          @NULLCreatedDate varchar(1),
          @NULLShippedDate varchar(1),
		  @NULLDeliveryDate Varchar(1), --CL417
          @VarwarehouseId varchar(max),
		  @VarDPServiceLineKeyJSON VARCHAR(500),
		  @VarDPProductLineKeyJSON VARCHAR(500),
		  @varIsManaged VARCHAR(10), --CL385
		  @NULLIsManaged VARCHAR(10), --CL385
		  @varOrderType nvarchar(max),		--CL411
		  @NullOrderType nvarchar(max)		--CL411

  SET @VarAccountID = UPPER(@DPProductLineKey)
  SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)
  SET @VarDPEntityKey = UPPER(@DPEntityKey)
  SET @VarwarehouseId = UPPER(@warehouseId)
  SET @VarStartCreatedDateTime = @StartDate
  SET @VarEndCreatedDateTime = @EndDate
  SET @VarEndCreatedDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @VarEndCreatedDateTime)))
  SET @varIsManaged = CASE WHEN UPPER(@IsManaged) = 'Y' THEN '1' 
						   WHEN UPPER(@IsManaged) = 'N' THEN '0' ELSE @IsManaged END ----CL385

  SET @varOrderType = UPPER(@OrderType)    --CL411
  
SELECT UPPER(DPProductLineKey) AS DPProductLineKey,
       UPPER(DPServiceLineKey) AS DPServiceLineKey
	   into #ACCOUNTINFO
	   FROM OPENJSON(@AccountKeys)
	   WITH(
   DPProductLineKey VARCHAR(MAX),
   DPServiceLineKey VARCHAR(MAX)
	   )

  IF @DPServiceLineKey IS NULL
    SET @VarDPServiceLineKey = '*'

  IF @DPEntityKey IS NULL
    SET @VarDPEntityKey = '*'

  IF @DateType = 'shipmentShippedDate'
   BEGIN
    SET @NULLCreatedDate = '*'
	SET @NULLDeliveryDate = '*' --CL417
	END

  IF @DateType = 'shipmentCreationDate'
   BEGIN
    SET @NULLShippedDate = '*'
	SET @NULLDeliveryDate = '*' --CL417
	END


--CL417
  IF @DateType = 'actualDeliveryDate' 
   BEGIN
    SET @NULLShippedDate = '*' 
    SET @NULLCreatedDate = '*'
	END
--CL417

IF @varOrderType = '*'  --CL411
	SET @NullOrderType = '*'

--CL411
	DROP TABLE IF EXISTS #OrderType
	SELECT
	Upper(value) AS orderType
	INTO   #OrderType
	FROM   String_split(@varOrderType, ',')

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

  IF ISNULL(@varIsManaged,'')='' OR @varIsManaged = '*'--CL385
	SET @NULLIsManaged = '*'


  SELECT
    CAST(O.DateTimeReceived AS Date) AS ShipmentCreationDate,
    CAST(O.DateTimeShipped AS Date) AS ShipmentShippedDate,
	CAST(O.ActualDeliveryDateTime AS Date) AS ActualDeliveryDate
	INTO #DIGITAL_SUMMARY_ORDERS
  FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O
  WHERE  ( O.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR AccountId = @VarAccountID ) 
  AND ((O.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR O.DP_SERVICELINE_KEY = @VarDPServiceLineKey)  OR @VarDPServiceLineKey = '*')
  --AND (ISNULL(O.DP_ORGENTITY_KEY, '') = @VarDPEntityKey OR @VarDPEntityKey = '*')
  AND (O.FacilityId COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarwarehouseId, ',')) OR @VarwarehouseId = '*')
  AND ((O.DateTimeReceived BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime) OR @NULLCreatedDate = '*')
  AND ((O.DateTimeShipped  BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime) OR @NULLShippedDate = '*')
  AND ((O.ActualDeliveryDateTime  BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime) OR @NULLDeliveryDate = '*') --CL417
  AND O.IS_INBOUND=0
  AND ((CAST(O.IS_MANAGED AS VARCHAR(10)) = @varIsManaged) OR @NULLIsManaged = '*') --CL385
  AND (UPPER(ISNULL(O.OrderType,'')) in (Select orderType from #OrderType) or @NullOrderType = '*' )  --CL411
  AND ( Isnull(O.OrderCancelledFlag, 'N') = 'N' ) --CL431

  SELECT
    COUNT(1) Total
  FROM #DIGITAL_SUMMARY_ORDERS 

  IF @DateType = 'shipmentCreationDate'
    SELECT
      ShipmentCreationDate,
      COUNT(ShipmentCreationDate) AS ShipmentCreationDateCount
    FROM #DIGITAL_SUMMARY_ORDERS 
    WHERE ShipmentCreationDate IS NOT NULL
    GROUP BY ShipmentCreationDate
    ORDER BY ShipmentCreationDate

  IF @DateType = 'shipmentShippedDate'
    SELECT
      ShipmentShippedDate,
      COUNT(ShipmentShippedDate) AS ShipmentShippedDateCount
    FROM #DIGITAL_SUMMARY_ORDERS 
    WHERE ShipmentShippedDate IS NOT NULL
    GROUP BY ShipmentShippedDate
    ORDER BY ShipmentShippedDate

--CL417
 IF @DateType = 'actualDeliveryDate'
    SELECT
      ActualDeliveryDate,
      COUNT(ActualDeliveryDate) AS ActualDeliveryDateCount
    FROM #DIGITAL_SUMMARY_ORDERS 
    WHERE ActualDeliveryDate IS NOT NULL
    GROUP BY ActualDeliveryDate
    ORDER BY ActualDeliveryDate

--CL417
END
GO

