/****** Object:  StoredProcedure [digital].[rpt_Outbound_WIP_Shipment_Listing]    Script Date: 1/7/2022 4:27:23 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






/**
CHANGE LOG
---------------
DEVELOPER			DATE					SPRINT			CHANGES
VENKATA				08/27/2021								ADDED MILESTONEACTIVIRYFLAG IN THE JOIN CONDITION FOR #MAX_ACTIVITY TABLE
PRASHANT            09/02/2021              37th            SHIPPED START AND SHIPPED END DATE ARE INTRODUCED IN INPUT FILTER & SHIPPED DATE IS INCLUDED IN RESULT SET 2
ARUN				10/27/2021								Added a new union block which accepts shipped as @wipActivityName
Venkata				11/22/2021				UPSGLD-12259    Added CurrentMilestoneFlag = 'Y' condition while loading the records into #FinalSmryOrder
SAGAR				11/25/2021				43rd CL-316		Have a mapping for "NULL" shipment mode to UNASSIGNED as value
SAGAR				11/25/2021				43rd CL-314		Cancelled orders(Entirely/Partially) are showing up in DO in progress activity listing page in PROD. 
															Ideally orders which are Cancelled should not show up in In Progress activity. 
															We have a separate Cancelled Order table where we list those.
Revathy             12/03/2021              UPSGLD-12399    Replaced Temp table #Summary_Milestone_activity with the  Main Table  Summary_Milestone_activity 
SAGAR				12/06/2021				44th CL-325		Add below fields as part of output resultset2
															shipmentService, shipmentServiceLevel, shipmentServiceLevelCode
AVINASH             12/27/2021              UPSGLD-12889    Added comma(,) in the select statement where we are selecting @topRow greater than 0
AVINASH             01/03/2022              46th CL-336     Added service level input optional parameter @ServiceLevelArray.
**/

/*
EXEC [digital].[rpt_Outbound_WIP_Shipment_Listing]
@DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',
@DPServiceLineKey = '150E5128-B3E3-44A1-BF6C-CD2E4D9856DA',
@DPEntityKey = NULL,
@Date = '{"shipmentCreationStartDate":"2021-10-24","shipmentCreationEndDate":"2021-11-25"}',
@wipActivityName='*', @warehouseCode='*', @topRow=0,@shipmentMode ='*',@milestoneStatus='*'
*/
/**** 
--AMR
EXEC [digital].[rpt_Outbound_WIP_Shipment_Listing]  @DPProductLineKey = 'E648FA6F-6253-428E-8AC9-201E3EF83B91',@DPServiceLineKey = '53D60776-D3BD-4E6A-8E37-F591C148294B',@DPEntityKey = NULL,@Date = '{"shipmentCreationStartDate": "2020-12-01","shipmentCreationEndDate": "2021-01-08"}',@wipActivityName='RECEIVING,ASN CREATED',@warehouseCode='*', @topRow=0,@shipmentMode ='*',@milestoneStatus='*'
--SWR
EXEC [digital].[rpt_Outbound_WIP_Shipment_Listing]  @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = NULL,@DPEntityKey = NULL,@Date = '{"shipmentCreationStartDate": "2021-10-20","shipmentCreationEndDate": "2021-10-27"}',@wipActivityName='Shipped',@warehouseCode='*', @topRow=0,@shipmentMode ='*',@milestoneStatus='*'
EXEC [digital].[rpt_Outbound_WIP_Shipment_Listing]  @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = NULL,@DPEntityKey = NULL,@Date = '{"shippedStartDate": "2021-10-20","shippedEndDate": "2021-10-27"}',@wipActivityName='Shipped',@warehouseCode='*', @topRow=0,@shipmentMode ='*',@milestoneStatus='*'
--Cambium
EXEC [digital].[rpt_Outbound_WIP_Shipment_Listing]   @DPProductLineKey = 'A10F512B-C7F4-42A0-909C-21DD95A7D921',@DPServiceLineKey = '53DDDBC2-FB5A-4E70-A92E-8312DC9FDD05',@DPEntityKey = NULL,@Date = NULL,@wipActivityName='*', @warehouseCode='*', @topRow=0,@inboundType ='*',@milestoneStatus='*'

EXEC [digital].[rpt_Outbound_WIP_Shipment_Listing]
@DPProductLineKey = 'E648FA6F-6253-428E-8AC9-201E3EF83B91',
@DPServiceLineKey = NULL, --'53D60776-D3BD-4E6A-8E37-F591C148294B',
@DPEntityKey = NULL,
@Date = '{"shipmentCreationStartDate": "2021-10-16","shipmentCreationEndDate": "2021-11-16"}',
@wipActivityName='Shipped',
@warehouseCode='*',
@topRow=150,
@shipmentMode ='*',
@milestoneStatus='*',
@ServiceLevelArray= '{"ServiceLevel":["FedEx 2 Day"]}'
****/

CREATE PROCEDURE [digital].[rpt_Outbound_WIP_Shipment_Listing] 

@DPProductLineKey varchar(50), @DPServiceLineKey varchar(50), @DPEntityKey varchar(50),
@Date nvarchar(max)=NULL, @wipActivityName nvarchar(max)='*', @warehouseCode nvarchar(max)='*', @topRow int,
@shipmentMode nvarchar(max),@milestoneStatus nvarchar(max)='*',
@ServiceLevelArray nvarchar(max)='{"ServiceLevel":["*"]}' --CL336

AS

BEGIN

  DECLARE @VarAccountID varchar(50),
          @VarDPServiceLineKey varchar(50),
          @VarDPEntityKey varchar(50),
		  @VarwarehouseCode varchar(max),
          @NULLCreatedDate varchar(1),
		  @NULLExpectedShipDate varchar(1),
		  @VarshipmentMode nvarchar(max),
		  @NULLshipmentMode varchar(1),
		  @VarMilestoneStatus nvarchar(max),
		  @NULLMileStoneStatus varchar(1),
		  @VarWIPActivityName nvarchar(max),
		  @NULLWIPActivityName varchar(1),
		  @shipmentCreationStartDate date,
		  @shipmentCreationEndDate date,
		  @shipmentCreationStartDateTime datetime,
		  @shipmentCreationEndDateTime datetime,
		  @expectedShipByStartDate date,
		  @expectedShipByEndDate date,
		  @expectedShipByStartDateTime datetime,
		  @expectedShipByEndDateTime datetime,
		  @shippedStartDate date,
		  @shippedEndDate date,
		  @shippedStartDateTime datetime,
		  @shippedEndDateTime datetime,
		  @NULLShippedDate varchar(1),
		  @VarserviceLevelArray nvarchar(max)  ---CL336


  SELECT  @shipmentCreationStartDate  = shipmentCreationStartDate
        ,@shipmentCreationEndDate     = shipmentCreationEndDate
        ,@expectedShipByStartDate     = expectedShipByStartDate
        ,@expectedShipByEndDate       = expectedShipByEndDate
		,@shippedStartDate            = shippedStartDate
		,@shippedEndDate              = shippedEndDate
FROM OPENJSON(@Date)
WITH (
shipmentCreationStartDate date,
shipmentCreationEndDate date,
expectedShipByStartDate date,
expectedShipByEndDate   date,
shippedStartDate date,
shippedEndDate date
     )

  SET @VarAccountID = UPPER(@DPProductLineKey)
  SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)
  SET @VarDPEntityKey = UPPER(@DPEntityKey)
  SET @VarshipmentMode=UPPER(@shipmentMode)
  SET @VarMilestoneStatus=UPPER(@milestoneStatus)
  SET @VarWIPActivityName=UPPER(@wipActivityName)
  SET @VarwarehouseCode = @warehouseCode
  SET @shipmentCreationStartDateTime=@shipmentCreationStartDate
  SET @shipmentCreationEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @shipmentCreationEndDate)))
  SET @expectedShipByStartDateTime=@expectedShipByStartDate
  SET @expectedShipByEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @expectedShipByEndDate)))
  SET @shippedStartDateTime= @shippedStartDate
  SET @shippedEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @shippedEndDate)))
  
  IF ISNULL(@shipmentCreationStartDate,'')='' OR ISNULL(@shipmentCreationEndDate,'')=''
    SET @NULLCreatedDate = '*'

  IF ISNULL(@expectedShipByStartDate,'')='' OR ISNULL(@expectedShipByEndDate,'') =''
    SET @NULLExpectedShipDate = '*'

  IF ISNULL(@shippedStartDate,'')='' OR ISNULL(@shippedEndDate,'') =''
    SET @NULLShippedDate = '*'

  IF @DPServiceLineKey IS NULL
    SET @VarDPServiceLineKey = '*'

  IF @DPEntityKey IS NULL
    SET @VarDPEntityKey = '*'

  IF @VarshipmentMode='' OR ISNULL(@VarshipmentMode,'*') ='*'
     SET @NULLshipmentMode = '*'

	 --SET @VarshipmentMode=REPLACE(@VarshipmentMode,'NULL','NOMODE')
	 
	

  IF @VarMilestoneStatus='' OR ISNULL(@VarMilestoneStatus,'*')='*'
  SET @NULLMileStoneStatus='*'

  IF @VarWIPActivityName='' OR  ISNULL(@VarWIPActivityName,'*')='*'
  SET @NULLWIPActivityName='*'

    IF OBJECT_ID('tempdb..#Summary_Milestone_Activity') IS NOT NULL
	DROP TABLE #Summary_Milestone_Activity

  SELECT * into #Summary_Milestone_activity from Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) O
  WHERE O.AccountId = @VarAccountID

  IF OBJECT_ID('tempdb..#MAX_ACTIVITY') IS NOT NULL
	DROP TABLE #MAX_ACTIVITY

  CREATE TABLE #MAX_ACTIVITY(ActivityDate datetime,UPSOrderNumber varchar(128),SourceSystemKey int)

  --CL336
  --SERVICELEVELARRAY
  DROP TABLE IF EXISTS #SERVICELEVELARRAY                

  SELECT   UPPER(SERVICELEVELARRAY) AS SERVICELEVELARRAY INTO #SERVICELEVELARRAY                                 
     FROM OPENJSON(@ServiceLevelArray)                
     WITH (                
    ServiceLevel nvarchar(max) 'strict $.ServiceLevel' AS JSON         
 )                
 OUTER APPLY OPENJSON(ServiceLevel) WITH (SERVICELEVELARRAY NVARCHAR(MAX) '$'); 
 

 IF EXISTS( SELECT top 1 SERVICELEVELARRAY FROM #SERVICELEVELARRAY where SERVICELEVELARRAY='*')                
BEGIN                
SET @VarserviceLevelArray='*'                
END
--CL336
 
  SELECT
    O.SourceSystemKey,
	O.UPSOrderNumber AS upsShipmentNumber,
    O.OrderNumber AS referenceNumber,
	O.FacilityId AS warehouseId,
	O.OrderWarehouse AS warehouseCode,
	O.CurrentMilestone AS milestoneStatus,
	O.DateTimeReceived AS shipmentCreationDate,
	O.OriginAddress1 AS shipmentOrigin__addressLine1,
	O.OriginAddress2 AS shipmentOrigin__addressLine2,
	O.OriginCity AS shipmentOrigin__city,
	O.OriginProvince AS shipmentOrigin__stateProvince,
	O.OriginPostalCode AS shipmentOrigin__postalCode,
	O.OriginCountry AS shipmentOrigin__country,
	O.DestinationAddress1 AS shipmentDestination__addressLine1,
	O.DestinationAddress2 AS shipmentDestination__addressLine2,
	O.DestinationCity AS shipmentDestination__city,
	O.DestinationProvince AS shipmentDestination__stateProvince,
	O.DestinationPostalcode AS shipmentDestination__postalCode,
	O.DestinationCountry AS shipmentDestination__country,
	--O.ServiceMode AS shipmentMode,
	ISNULL(O.ServiceMode,'UNASSIGNED') as shipmentMode, --CL316
	O.ScheduleShipmentDate AS expectedShipByDate,
	O.DateTimeShipped AS shippedDate,
--CL325
	O.ServiceMode AS shipmentService,                
    O.ServiceLevel AS shipmentServiceLevel,                
    O.ServiceLevelCode AS shipmentServiceLevelCode
--CL325
	INTO #DIGITAL_SUMMARY_ORDERS
  FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O
  WHERE O.AccountId = @VarAccountID
  AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')
  --AND (ISNULL(O.DP_ORGENTITY_KEY, '') = @VarDPEntityKey OR @VarDPEntityKey = '*')
  AND (@NULLMileStoneStatus = '*' OR O.CurrentMilestone COLLATE SQL_Latin1_General_CP1_CI_AS IN (select (TRIM(value)) from string_split(@VarMilestoneStatus,',')))
  AND ((O.DateTimeReceived BETWEEN @shipmentCreationStartDateTime AND @shipmentCreationEndDateTime) OR @NULLCreatedDate = '*')
  AND ((O.ScheduleShipmentDate BETWEEN @expectedShipByStartDateTime AND @expectedShipByEndDateTime) OR @NULLExpectedShipDate = '*')
  AND ((O.DateTimeShipped BETWEEN @shippedStartDateTime AND @shippedEndDateTime) OR @NULLShippedDate = '*')
  AND O.IS_INBOUND= 0
  --AND (@NULLshipmentMode = '*' OR (ISNULL(O.ServiceMode,'NOMODE') COLLATE SQL_Latin1_General_CP1_CI_AS  IN (select (TRIM(value)) from string_split(@VarshipmentMode,','))))
  AND (@NULLshipmentMode = '*' OR (ISNULL(O.ServiceMode,'UNASSIGNED') COLLATE SQL_Latin1_General_CP1_CI_AS  IN (select (TRIM(value)) from string_split(@VarshipmentMode,',')))) --CL316
  AND (O.OrderWarehouse COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (VALUE) FROM string_split(@VarwarehouseCode, ',')) OR @VarwarehouseCode = '*')
  --AND O.IS_ASN IS NULL
  AND ISNULL(O.OrderCancelledFlag,'N') = 'N' --CL314

  INSERT INTO #MAX_ACTIVITY
  SELECT 
  MAX(ActivityDate) AS ActivityDate
  ,O.upsShipmentNumber
  ,O.SourceSystemKey
  FROM #DIGITAL_SUMMARY_ORDERS O (NOLOCK)
  INNER JOIN #Summary_Milestone_activity MA (NOLOCK)
            ON O.upsShipmentNumber=MA.UPSOrderNumber
			AND O.SourceSystemKey=MA.SourceSystemKey AND MA.CurrentMilestoneFlag = 'Y' 
  GROUP BY 
   O.upsShipmentNumber
  ,O.SourceSystemKey

  CREATE CLUSTERED INDEX [Ix_ClIndexMAX_ACTIVITY] ON #MAX_ACTIVITY([UPSOrderNumber] ASC,[SourceSystemKey] ASC,ActivityDate ASC )WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]


  SELECT
	upsShipmentNumber,
    referenceNumber,
	warehouseId,
	warehouseCode,
	milestoneStatus,
	shipmentMode,
	shipmentCreationDate,
	expectedShipByDate,
	wipActivityName,
	COUNT(UPSOrderNumber) AS linesCount,
	CAST(SUM(SKUQuantity) AS INT) AS unitsCount,
	shipmentOrigin__addressLine1,
	shipmentOrigin__addressLine2,
	shipmentOrigin__city,
	shipmentOrigin__stateProvince,
	shipmentOrigin__postalCode,
	shipmentOrigin__country,
	shipmentDestination__addressLine1,
	shipmentDestination__addressLine2,
	shipmentDestination__city,
	shipmentDestination__stateProvince,
	shipmentDestination__postalCode,
	shipmentDestination__country,
	shippedDate,
--CL325
	shipmentService,                
    shipmentServiceLevel,                
    shipmentServiceLevelCode
--CL325
	INTo #FinalSmryOrder
	FROM
	(
	SELECT
	O.upsShipmentNumber,
    O.referenceNumber,
	O.warehouseId,
	O.warehouseCode,
	O.milestoneStatus,
	O.shipmentMode,
	O.shipmentCreationDate,
	O.expectedShipByDate,
	WA.WIP_ActivityName AS wipActivityName,
	--COUNT(OL.UPSOrderNumber) AS linesCount,
	--CAST(SUM(OL.SKUQuantity) AS INT) AS unitsCount,
	OL.UPSOrderNumber,
	OL.SKUQuantity,
	O.shipmentOrigin__addressLine1,
	O.shipmentOrigin__addressLine2,
	O.shipmentOrigin__city,
	O.shipmentOrigin__stateProvince,
	O.shipmentOrigin__postalCode,
	O.shipmentOrigin__country,
	O.shipmentDestination__addressLine1,
	O.shipmentDestination__addressLine2,
	O.shipmentDestination__city,
	O.shipmentDestination__stateProvince,
	O.shipmentDestination__postalCode,
	O.shipmentDestination__country,
	O.shippedDate,
--CL325
	O.shipmentService,                
    O.shipmentServiceLevel,                
    O.shipmentServiceLevelCode
--CL325
  FROM #DIGITAL_SUMMARY_ORDERS O
  INNER JOIN (SELECT MA.UPSOrderNumber,MA.SourceSystemKey,MA.ActivityName FROM #MAX_ACTIVITY tmpActivity
                       INNER JOIN #Summary_Milestone_activity MA (NOLOCK)
					   ON tmpActivity.UPSOrderNumber=MA.UPSOrderNumber  AND tmpActivity.SourceSystemKey=MA.SourceSystemKey AND tmpActivity.ActivityDate=MA.ActivityDate
					   WHERE MA.CurrentMilestoneFlag = 'Y' --UPSGLD-12259
		     )as MA ON O.upsShipmentNumber=MA.UPSOrderNumber
			        AND O.SourceSystemKey=MA.SourceSystemKey
  INNER JOIN master_data.WH_WIP_MAPPING_Activity WA (NOLOCK) 
            ON MA.ActivityName=WA.ActivityName AND MA.SourceSystemKey=WA.SOURCE_SYSTEM_KEY
  LEFT JOIN Summary.DIGITAL_SUMMARY_ORDER_LINES OL (NOLOCK) 
            ON O.upsShipmentNumber=OL.UPSOrderNumber AND O.SourceSystemKey=OL.SourceSystemKey
 WHERE WA.[Type] = 'OUT'
 AND ( @NULLWIPActivityName='*' OR WA.WIP_ActivityName COLLATE SQL_Latin1_General_CP1_CI_AS IN (select (TRIM(value)) from string_split(@VarWIPActivityName,',')))
 AND (@VarserviceLevelArray = '*' OR (UPPER(COALESCE(shipmentServiceLevel, 'NOSERVICE')) COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT * FROM #SERVICELEVELARRAY)))  --CL336
 --AND shipmentServiceLevel in ('UPS Ground', 'FEDEX HOME DELIVERY')

 UNION
 
 SELECT
	O.upsShipmentNumber,
    O.referenceNumber,
	O.warehouseId,
	O.warehouseCode,
	O.milestoneStatus,
	O.shipmentMode,
	O.shipmentCreationDate,
	O.expectedShipByDate,
	'Shipped' AS wipActivityName,
	--COUNT(OL.UPSOrderNumber) AS linesCount,
	--CAST(SUM(OL.SKUQuantity) AS INT) AS unitsCount,
	OL.UPSOrderNumber,
	OL.SKUQuantity,
	O.shipmentOrigin__addressLine1,
	O.shipmentOrigin__addressLine2,
	O.shipmentOrigin__city,
	O.shipmentOrigin__stateProvince,
	O.shipmentOrigin__postalCode,
	O.shipmentOrigin__country,
	O.shipmentDestination__addressLine1,
	O.shipmentDestination__addressLine2,
	O.shipmentDestination__city,
	O.shipmentDestination__stateProvince,
	O.shipmentDestination__postalCode,
	O.shipmentDestination__country,
	O.shippedDate,
--CL325
	O.shipmentService,                
    O.shipmentServiceLevel,                
    O.shipmentServiceLevelCode
--CL325
  FROM #DIGITAL_SUMMARY_ORDERS O
  INNER JOIN (SELECT MA.UPSOrderNumber,MA.SourceSystemKey,MA.ActivityName FROM #MAX_ACTIVITY tmpActivity
                       INNER JOIN #Summary_Milestone_activity MA (NOLOCK)
					   ON tmpActivity.UPSOrderNumber=MA.UPSOrderNumber  AND tmpActivity.SourceSystemKey=MA.SourceSystemKey AND tmpActivity.ActivityDate=MA.ActivityDate
					   WHERE MA.CurrentMilestoneFlag = 'Y' --UPSGLD-12259
		     )as MA ON O.upsShipmentNumber=MA.UPSOrderNumber
			        AND O.SourceSystemKey=MA.SourceSystemKey
  LEFT JOIN Summary.DIGITAL_SUMMARY_ORDER_LINES OL (NOLOCK) 
            ON O.upsShipmentNumber=OL.UPSOrderNumber AND O.SourceSystemKey=OL.SourceSystemKey
 WHERE O.shippedDate IS NOT NULL AND (@VarWIPActivityName = '*' OR @VarWIPActivityName = 'SHIPPED') 
AND (@VarserviceLevelArray = '*' OR (UPPER(COALESCE(shipmentServiceLevel,'NOSERVICE')) COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT * FROM #SERVICELEVELARRAY)))  --CL336
--AND shipmentServiceLevel in  ('UPS Ground', 'FEDEX HOME DELIVERY')
) TBL

GROUP BY 
 upsShipmentNumber,
 referenceNumber,
 warehouseId,
 warehouseCode,
 milestoneStatus,
 shipmentMode,
 shipmentCreationDate,
 shipmentOrigin__addressLine1,
 shipmentOrigin__addressLine2,
 shipmentOrigin__city,
 shipmentOrigin__stateProvince,
 shipmentOrigin__postalCode,
 shipmentOrigin__country,
 shipmentDestination__addressLine1,
 shipmentDestination__addressLine2,
 shipmentDestination__city,
 shipmentDestination__stateProvince,
 shipmentDestination__postalCode,
 shipmentDestination__country,
 expectedShipByDate,
 wipActivityName,
 shippedDate,
--CL325
 shipmentService,                
 shipmentServiceLevel,                
 shipmentServiceLevelCode
--CL325



 SELECT COUNT(1) totalShipments from #FinalSmryOrder

  IF @topRow = 0

    SELECT 
	upsShipmentNumber,
	referenceNumber,
	linesCount,
	unitsCount,
	wipActivityName,
	warehouseId,
	warehouseCode,
	milestoneStatus,
	shipmentMode,
	CONVERT(date,expectedShipByDate,112) AS expectedShipByDate,
	shipmentOrigin__addressLine1,
	shipmentOrigin__addressLine2,
	shipmentOrigin__city,
	shipmentOrigin__stateProvince,
	shipmentOrigin__postalCode,
	shipmentOrigin__country,
	shipmentDestination__addressLine1,
	shipmentDestination__addressLine2,
	shipmentDestination__city,
	shipmentDestination__stateProvince,
	shipmentDestination__postalCode,
	shipmentDestination__country,
	shipmentCreationDate,
	shippedDate
	--CL325
	,shipmentService,                
    shipmentServiceLevel,                
    shipmentServiceLevelCode
	--CL325
	FROM #FinalSmryOrder SO
	ORDER BY shipmentCreationDate DESC

  ELSE

    SELECT TOP (@topRow) 
	upsShipmentNumber,
	referenceNumber,
	linesCount,
	unitsCount,
	wipActivityName,
	warehouseId,
	warehouseCode,
	milestoneStatus,
	shipmentMode,
	CONVERT(date,expectedShipByDate,112) AS expectedShipByDate,
	shipmentOrigin__addressLine1,
	shipmentOrigin__addressLine2,
	shipmentOrigin__city,
	shipmentOrigin__stateProvince,
	shipmentOrigin__postalCode,
	shipmentOrigin__country,
	shipmentDestination__addressLine1,
	shipmentDestination__addressLine2,
	shipmentDestination__city,
	shipmentDestination__stateProvince,
	shipmentDestination__postalCode,
	shipmentDestination__country,
	shipmentCreationDate,
	shippedDate,   --- UPSGLD-12889
--CL325
	shipmentService,        
    shipmentServiceLevel,                
    shipmentServiceLevelCode
--CL325
	FROM #FinalSmryOrder
	ORDER BY shipmentCreationDate DESC

END
GO

