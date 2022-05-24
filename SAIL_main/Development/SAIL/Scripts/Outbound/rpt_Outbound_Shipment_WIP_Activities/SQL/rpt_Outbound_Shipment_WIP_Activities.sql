/****** Object:  StoredProcedure [digital].[rpt_Outbound_Shipment_WIP_Activities]    Script Date: 12/13/2021 4:26:56 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO








/**  
CHANGE LOG  
---------------  
DEVELOPER   DATE			SPRINT			CHANGES  
VENKATA    09/08/2021						ADDED MILESTONEACTIVIRYFLAG IN THE JOIN CONDITION FOR #MAX_ACTIVITY TABLE
ARUN	   10/27/2021						Added a new union block which accepts shipped as @wipActivityName
Venkata	   11/22/2021	   UPSGLD-12259     ADDED MILESTONEACTIVIRYFLAG IN THE JOIN CONDITION FOR #DetailActivity TABLE
SAGAR	   11/25/2021	   43rd CL-315		Have a mapping for "NULL" shipment mode to UNASSIGNED as value
SAGAR	   11/25/2021	   43rd CL-NEW		Cancelled orders(Entirely/Partially) are showing up in DO in progress activity listing page in PROD. 
											Ideally orders which are Cancelled should not show up in In Progress activity. 
											We have a separate Cancelled Order table where we list those.
**/  


/**** 
--AMR
EXEC [digital].[rpt_Outbound_Shipment_WIP_Activities]    @DPProductLineKey = 'E648FA6F-6253-428E-8AC9-201E3EF83B91',@DPServiceLineKey = '53D60776-D3BD-4E6A-8E37-F591C148294B',@DPEntityKey = NULL,@startDate='2020/11/05',@endDate='2020/11/12',@dateType='*',@warehouseId='*',@type='*'
--SWR
EXEC [digital].[rpt_Outbound_Shipment_WIP_Activities]    @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = '150E5128-B3E3-44A1-BF6C-CD2E4D9856DA',@DPEntityKey = NULL,@startDate='2020/10/01',@endDate='2020/11/12',@dateType='*',@warehouseId='*',@type='*',@Debug=0
--Cambium
EXEC [digital].[rpt_Outbound_Shipment_WIP_Activities]    @DPProductLineKey = 'A10F512B-C7F4-42A0-909C-21DD95A7D921',@DPServiceLineKey = '53DDDBC2-FB5A-4E70-A92E-8312DC9FDD05',@DPEntityKey = NULL,@startDate='2020/10/29',@endDate='2020/11/30',@dateType='*',@warehouseId='*',@type='*'
****/

CREATE PROCEDURE [digital].[rpt_Outbound_Shipment_WIP_Activities]

@DPProductLineKey varchar(50), @DPServiceLineKey varchar(50), @DPEntityKey varchar(50), @startDate date=NULL, 
@endDate date=NULL,@dateType varchar(50),@warehouseId varchar(max),@type varchar(10),@Debug int =0

AS

BEGIN

  DECLARE @VarAccountID varchar(50),
          @VarDPServiceLineKey varchar(50),
          @VarDPEntityKey varchar(50),
		  @VarStartCreatedDateTime datetime,
          @VarEndCreatedDateTime datetime,
          @NULLCreatedDate varchar(1),
		  @NULLShippedDate varchar(1),
		  @VarwarehouseId varchar(max),
		  @VarType varchar(10),
		  @Is_ASN int,
		  @NULLType char(1),
		  @Starttime DATETIME,
		  @EndTime DATETIME,
		  @VarDateType varchar(20)
          
  SET @VarAccountID = UPPER(@DPProductLineKey)
  SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)
  SET @VarDPEntityKey = UPPER(@DPEntityKey)
  SET @VarwarehouseId = UPPER(@warehouseId)
  SET @VarStartCreatedDateTime = @startDate
  SET @VarEndCreatedDateTime = @endDate
  SET @VarEndCreatedDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @VarEndCreatedDateTime)))
  SET @VarType=UPPER(@type)
  SET @Starttime = GETDATE()
  SET @VarDateType=UPPER(@dateType)
  
  IF @DPServiceLineKey IS NULL
    SET @VarDPServiceLineKey = '*'

  IF @DPEntityKey IS NULL
    SET @VarDPEntityKey = '*'

  IF (@VarStartCreatedDateTime IS NULL OR @VarEndCreatedDateTime IS NULL) --AND @VarDateType = 'SHIPPED'
    BEGIN
	SET @NULLCreatedDate = '*'
	SET @NULLShippedDate = '*'
	END

  --IF (@VarStartCreatedDateTime IS NULL OR @VarEndCreatedDateTime IS NULL) AND @VarDateType = 'CREATE'
  --  SET @NULLShippedDate = '*'


  IF @VarDateType = 'SHIPPED'
    SET @NULLCreatedDate = '*'

  IF @VarDateType = 'CREATE'
    SET @NULLShippedDate = '*'

  IF OBJECT_ID('tempdb..#TypesData') IS NOT NULL
	DROP TABLE #TypesData

	IF OBJECT_ID('tempdb..#SMRYORDER') IS NOT NULL
	DROP TABLE #SMRYORDER

	IF OBJECT_ID('tempdb..#MAX_ACTIVITY') IS NOT NULL
	DROP TABLE #MAX_ACTIVITY

	IF OBJECT_ID('tempdb..#DetailActivity') IS NOT NULL
	DROP TABLE #DetailActivity

  CREATE TABLE #TypesData([type] varchar(10), ActivityOrderId INT, ActivityName  varchar(255),ShipmentMode VARCHAR(128),[Count] INT,warehouseCode varchar(50),[Date] Date)
  CREATE TABLE #SMRYORDER(UPSOrderNumber varchar(128),SourceSystemKey int,ServiceMode varchar(128),IS_ASN INT,ActivityName  varchar(255),ActivityOrderId INT,warehouseCode varchar(50),[Date] Date)
  CREATE TABLE #MAX_ACTIVITY(ActivityDate datetime,UPSOrderNumber varchar(128),SourceSystemKey int)

  IF @Debug>0
  BEGIN
  SET @EndTime = GETDATE()
  SELECT 'Creating #TypesData table',@Starttime,@EndTime,DATEDIFF(SS,@Starttime,@EndTime) AS TimeTakeninsecs
  SET @Starttime = GETDATE()
  END

  INSERT INTO #MAX_ACTIVITY
   SELECT 
   MAX(ActivityDate) AS ActivityDate
  ,O.UPSOrderNumber
  ,O.SourceSystemKey
  FROM Summary.DIGITAL_SUMMARY_ORDERS O (NOLOCK)
  INNER JOIN Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY MA (NOLOCK)
            ON O.UPSOrderNumber=MA.UPSOrderNumber
			AND O.SourceSystemKey=MA.SourceSystemKey
			AND MA.CurrentMilestoneFlag = 'Y' -- added 09/08
  WHERE UPPER(O.AccountId) = @VarAccountID
  AND ISNULL(O.OrderCancelledFlag,'N') = 'N'	--CL-NEW
  AND O.IS_INBOUND = 0
  AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')
  AND (O.FacilityId IN (SELECT UPPER (TRIM (VALUE))FROM string_split(@VarwarehouseId, ',')) OR @VarwarehouseId = '*')
  AND ((O.DateTimeReceived BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime) OR @NULLCreatedDate = '*')
  AND ((O.DateTimeShipped  BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime) OR @NULLShippedDate = '*')
  GROUP BY 
   O.UPSOrderNumber
  ,O.SourceSystemKey

  CREATE CLUSTERED INDEX [Ix_ClIndexMAX_ACTIVITY] ON #MAX_ACTIVITY([UPSOrderNumber] ASC,[SourceSystemKey] ASC,ActivityDate ASC )WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]
  
  SELECT MA.UPSOrderNumber,MA.SourceSystemKey,MA.ActivityName 
  INTO #DetailActivity
  FROM #MAX_ACTIVITY tmpActivity
                       INNER JOIN Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY MA (NOLOCK)
					   ON tmpActivity.UPSOrderNumber=MA.UPSOrderNumber
					   AND tmpActivity.SourceSystemKey=MA.SourceSystemKey
					   AND tmpActivity.ActivityDate=MA.ActivityDate 
					   AND MA.CurrentMilestoneFlag = 'Y' --UPSGLD-12259

  CREATE CLUSTERED INDEX [Ix_ClIndexDetailActivity] ON #DetailActivity([UPSOrderNumber] ASC,[SourceSystemKey] ASC )WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]

 INSERT INTO #SMRYORDER
 SELECT 
  O.UPSOrderNumber
 ,O.SourceSystemKey
 ,O.ServiceMode
 ,IS_ASN
 ,WA.WIP_ActivityName AS ActivityName
 ,WA.WIPActivityOrderId
 ,O.OrderWareHouse as warehouseCode
,CASE WHEN @NULLCreatedDate='*' THEN CAST(O.DateTimeShipped AS DATE)
        WHEN @NULLShippedDate='*' THEN CAST(O.DateTimeReceived AS DATE) END AS [Date]
 FROM Summary.DIGITAL_SUMMARY_ORDERS O (NOLOCK)
  INNER JOIN #DetailActivity MA ON O.UPSOrderNumber=MA.UPSOrderNumber
			        AND O.SourceSystemKey=MA.SourceSystemKey
  INNER JOIN master_data.WH_WIP_MAPPING_Activity WA (NOLOCK) 
            ON MA.ActivityName=WA.ActivityName
			AND MA.SourceSystemKey=WA.SOURCE_SYSTEM_KEY
 WHERE UPPER(O.AccountId) = @VarAccountID
 AND ISNULL(O.OrderCancelledFlag,'N') = 'N' --CL-NEW
  AND O.IS_INBOUND = 0
  AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')
  AND (O.FacilityId IN (SELECT UPPER (TRIM (VALUE))FROM string_split(@VarwarehouseId, ',')) OR @VarwarehouseId = '*')
  AND ((O.DateTimeReceived  BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime) OR @NULLCreatedDate = '*')
  --AND ((O.DateTimeShipped  BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime) OR @NULLShippedDate = '*')
  AND CASE WHEN UPPER(@dateType) = 'SHIPPED' THEN 0 ELSE 1 END = 1
  AND WA.[Type] = 'OUT'
  GROUP BY  
  O.UPSOrderNumber
 ,O.SourceSystemKey
 ,O.ServiceMode
 ,IS_ASN
 ,WA.WIP_ActivityName
 ,WA.WIPActivityOrderId
 ,O.OrderWarehouse
 ,CASE WHEN @NULLCreatedDate='*' THEN CAST(O.DateTimeShipped AS DATE)
        WHEN @NULLShippedDate='*' THEN CAST(O.DateTimeReceived AS DATE) END 


UNION

 SELECT 
  O.UPSOrderNumber
 ,O.SourceSystemKey
 ,O.ServiceMode
 ,IS_ASN
 --,WA.WIP_ActivityName AS ActivityName
 ,'Shipped' AS ActivityName
 --,WA.WIPActivityOrderId
 ,O.SourceSystemKey + 07 AS WIPActivityOrderId
 ,O.OrderWareHouse as warehouseCode
 ,CASE WHEN @NULLCreatedDate='*' THEN CAST(O.DateTimeShipped AS DATE)
        WHEN @NULLShippedDate='*' THEN CAST(O.DateTimeReceived AS DATE) END AS [Date]
 FROM Summary.DIGITAL_SUMMARY_ORDERS O (NOLOCK)
  INNER JOIN #DetailActivity MA ON O.UPSOrderNumber=MA.UPSOrderNumber
			        AND O.SourceSystemKey=MA.SourceSystemKey
  --INNER JOIN master_data.WH_WIP_MAPPING_Activity WA (NOLOCK) 
  --          ON MA.ActivityName=WA.ActivityName
		--	AND MA.SourceSystemKey=WA.SOURCE_SYSTEM_KEY
 WHERE UPPER(O.AccountId) = @VarAccountID
  AND ISNULL(O.OrderCancelledFlag,'N') = 'N' --CL-NEW
  AND O.IS_INBOUND = 0
  AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')
  AND (O.FacilityId IN (SELECT UPPER (TRIM (VALUE))FROM string_split(@VarwarehouseId, ',')) OR @VarwarehouseId = '*')
  AND ((O.DateTimeReceived  BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime) OR @NULLCreatedDate = '*')
  AND ((O.DateTimeShipped  BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime) OR @NULLShippedDate = '*')
  AND O.DateTimeShipped IS NOT NULL
 --AND WA.[Type] = 'OUT'  AND MA.ActivityName = 'Shipped'
  GROUP BY  
  O.UPSOrderNumber
 ,O.SourceSystemKey
 ,O.ServiceMode
 ,IS_ASN
 ,O.OrderWarehouse
 ,CASE WHEN @NULLCreatedDate='*' THEN CAST(O.DateTimeShipped AS DATE)
        WHEN @NULLShippedDate='*' THEN CAST(O.DateTimeReceived AS DATE) END 

  IF @Debug>0
  BEGIN
  SET @EndTime = GETDATE()
  SELECT 'Creating #SMRYORDER table',@Starttime,@EndTime,DATEDIFF(SS,@Starttime,@EndTime) AS TimeTakeninsecs
  SELECT * FROM #SMRYORDER
  SET @Starttime = GETDATE()
  END

  CREATE CLUSTERED INDEX [Ix_ClIndex] ON #SMRYORDER([UPSOrderNumber] ASC,[SourceSystemKey] ASC )WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]

  IF @Debug>0
  BEGIN
  SET @EndTime = GETDATE()
  SELECT 'Creating clustered index',@Starttime,@EndTime,DATEDIFF(SS,@Starttime,@EndTime) AS TimeTakeninsecs
  SET @Starttime = GETDATE()
  END


  IF @VarType='*' OR @VarType='ORDER'
  BEGIN

      INSERT INTO #TypesData
      SELECT 
        'ORDER' AS [type]
		,O.ActivityOrderId
        ,O.ActivityName
        --,O.ServiceMode as ShipmentMode
		,ISNULL(O.ServiceMode,'UNASSIGNED') as ShipmentMode --CL315
        ,COUNT(1) AS [Count]
		,O.warehouseCode
		,O.[Date]
      FROM #SMRYORDER O (NOLOCK)
      WHERE O.IS_ASN IS NULL
      AND (@VarType='*' OR @VarType='ORDER')
	  GROUP BY O.ActivityOrderId,O.ActivityName,O.ServiceMode,O.warehouseCode,O.[Date]

  IF @Debug>0
  BEGIN
  SET @EndTime = GETDATE()
  SELECT 'Insert ASN Data',@Starttime,@EndTime,DATEDIFF(SS,@Starttime,@EndTime) AS TimeTakeninsecs
  SET @Starttime = GETDATE()
  END

  END


   IF @VarType='*' OR @VarType='LINES'
   BEGIN

       INSERT INTO #TypesData
	   SELECT 
        'LINES' AS [type]
		,O.ActivityOrderId
        ,O.ActivityName
        --,O.ServiceMode as ShipmentMode
		,ISNULL(O.ServiceMode,'UNASSIGNED') as ShipmentMode --CL315
        ,COUNT(OL.UPSOrderNumber) AS [Count]
		,O.warehouseCode
		,O.[Date]
      FROM #SMRYORDER O (NOLOCK)
      LEFT JOIN Summary.DIGITAL_SUMMARY_ORDER_LINES OL (NOLOCK) 
     			ON O.UPSOrderNumber=OL.UPSOrderNumber
     			AND O.SourceSystemKey=OL.SourceSystemKey
     WHERE (@VarType='*' OR @VarType='LINES')
     GROUP BY O.ActivityOrderId,O.ActivityName,O.ServiceMode,O.warehouseCode,O.[Date]

  IF @Debug>0
  BEGIN
  SET @EndTime = GETDATE()
  SELECT 'Insert Lines Data',@Starttime,@EndTime,DATEDIFF(SS,@Starttime,@EndTime) AS TimeTakeninsecs
  SET @Starttime = GETDATE()
  END

   END

   IF @VarType='*' OR @VarType='UNITS'
   BEGIN

    INSERT INTO #TypesData
	SELECT 
	 'UNITS' AS [type]
	 ,O.ActivityOrderId
	 ,O.ActivityName AS ActivityName
	 --,O.ServiceMode as ShipmentMode
	 ,ISNULL(O.ServiceMode,'UNASSIGNED') as ShipmentMode  --CL315
	 ,CAST(SUM(OL.SKUQuantity) AS INT) AS [Count]
	 ,O.warehouseCode
	 ,O.[Date]
	FROM #SMRYORDER O (NOLOCK)
	LEFT JOIN Summary.DIGITAL_SUMMARY_ORDER_LINES OL (NOLOCK) 
			ON O.UPSOrderNumber=OL.UPSOrderNumber
			AND O.SourceSystemKey=OL.SourceSystemKey
	WHERE (@VarType='*' OR @VarType='UNITS')
	
	GROUP BY O.ActivityOrderId,O.ActivityName,O.ServiceMode,O.warehouseCode,O.[Date]

  IF @Debug>0
  BEGIN
  SET @EndTime = GETDATE()
  SELECT 'Insert UNITS Data',@Starttime,@EndTime,DATEDIFF(SS,@Starttime,@EndTime) AS TimeTakeninsecs
  SET @Starttime = GETDATE()
  END

   END

   SELECT [type]
         ,ActivityName
		 ,ShipmentMode
		 ,[Count]
         ,warehouseCode,
		 [Date]
  FROM #TypesData 
   ORDER BY ActivityOrderId 

  IF @Debug>0
  BEGIN
  SET @EndTime = GETDATE()
  SELECT 'Final Select statement',@Starttime,@EndTime,DATEDIFF(SS,@Starttime,@EndTime) AS TimeTakeninsecs
  SET @Starttime = GETDATE()
  END

END
GO

