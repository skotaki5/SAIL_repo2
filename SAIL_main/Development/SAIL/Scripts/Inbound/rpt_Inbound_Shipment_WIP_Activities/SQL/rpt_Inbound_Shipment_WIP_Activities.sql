/****** Object:  StoredProcedure [digital].[rpt_Inbound_Shipment_WIP_Activities]    Script Date: 4/28/2022 10:36:03 AM ******/
SET ANSI_NULLS ON
GO
 
SET QUOTED_IDENTIFIER ON
GO
 
 
 
 
 
 
 
 
 
     
     
     
     
/**   
change log   
----------------------
Developer           date        Sprint               Changes   
Venkata         09/15/2021                          Added O.CurrentMilestone <> 'PUTAWAY' condition while fitering Summary Orders table   
Venkata/Arup    10/09/2021  SPRINT-40 CL278         Added @accountKeys input paramter, Added @datetype input parameter
ARUP            10/19/2021  UPSGLD-11593            DPEntityKey parameter is removed
Venkata         11/01/2021  UPSGLD-11878            replace null value with 0 for the count column
Venkata         11/05/2021  UPSGLD-11917            changed logic when the @dateType is provided as '*'
Venkata         12/10/2021  UPSGLD-12521            Removed the cancelled order records
Revathy         01/06/2022  CL-338                  Added cases (closed ASNs). Bring cases from ETL (GWS NHC) to GLD 360 db. Stored procedure work
Venkata         02/08/2022  Sprint49 CL393          Changed the date datatype from datetime to date
Harsha          02/13/2022                          Adding table alias wherver required
Venkata         02/22/2022  Sprint49 UPSGLD-14185   changed filtering condition from O.CurrentMilestone to MA.MilestoneName which was implemented on 09/15
Venkata         04/19/2022  UPSGLD-15583            Added CurrentMilestong Flag in the filtering condition
----------------------   
**/   
     
/****     
 
EXEC [digital].[rpt_Inbound_Shipment_WIP_Activities]   
@DPProductLineKey = 'A10F512B-C7F4-42A0-909C-21DD95A7D921',
@DPServiceLineKey = '53DDDBC2-FB5A-4E70-A92E-8312DC9FDD05',
@startDate='2021-08-29',@endDate='2021-10-29',@dateType='SHIPMENTCREATIONDATE',@warehouseId='*',@type='*'
 
EXEC [digital].[rpt_Inbound_Shipment_WIP_Activities]   
@DPProductLineKey = 'A10F512B-C7F4-42A0-909C-21DD95A7D921',
@DPServiceLineKey = '53DDDBC2-FB5A-4E70-A92E-8312DC9FDD05',
@startDate='2021-08-29',@endDate='2022-01-29',@dateType='SHIPMENTRECEIVEDDATE',@warehouseId='*',@type='*'
 
 
****/   
     
CREATE PROCEDURE [digital].[rpt_Inbound_Shipment_WIP_Activities]   
     
@DPProductLineKey varchar(50) = NULL, @DPServiceLineKey varchar(50) = NULL
--@DPEntityKey varchar(50), Fix for UPSGLD-11593
, @AccountKeys nvarchar(max) = NULL,     
@startDate date=NULL, @endDate date=NULL,@dateType varchar(50),@warehouseId varchar(max),@type varchar(10),@Debug int =0   
     
AS   
     
BEGIN   
     
  DECLARE @VarAccountID varchar(50),   
          @VarDPServiceLineKey varchar(50),   
          --@VarDPEntityKey varchar(50),   Fix for UPSGLD-11593
    @VarStartCreatedDateTime datetime,   
    @VarEndCreatedDateTime datetime,   
    @NULLCreatedDate varchar(1),   
    @NULLReceivedDate varchar(1), --CL278   
    @VarwarehouseId varchar(max),   
    @VarType varchar(10),   
    @Is_ASN int,   
    @NULLType char(1),   
    @Starttime DATETIME,   
    @EndTime DATETIME,   
    @VarDateType varchar(20),   
    @VarDPServiceLineKeyJSON VARCHAR(500),       
    @VarDPProductLineKeyJSON VARCHAR(500)   
               
  SET @VarAccountID = UPPER(@DPProductLineKey)   
  SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)   
  --SET @VarDPEntityKey = UPPER(@DPEntityKey)           Fix for UPSGLD-11593 
  SET @VarwarehouseId = UPPER(@warehouseId)   
  SET @VarStartCreatedDateTime = @startDate   
  SET @VarEndCreatedDateTime = @endDate   
  SET @VarEndCreatedDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @VarEndCreatedDateTime)))   
  SET @VarType=UPPER(@type)   
  SET @Starttime = GETDATE()   
  SET @VarDateType=UPPER(@dateType)   
       
  IF @DPServiceLineKey IS NULL   
    SET @VarDPServiceLineKey = '*'   
     
  --IF @DPEntityKey IS NULL
    --SET @VarDPEntityKey = '*' Fix for UPSGLD-11593  
     
  IF @VarStartCreatedDateTime IS NULL OR @VarEndCreatedDateTime IS NULL   
 BEGIN   
    SET @NULLCreatedDate = '*'   
    SET @NULLReceivedDate = '*' --CL278   
 END   
     
 IF @VarDateType = 'SHIPMENTCREATIONDATE' --CL278   
    SET @NULLReceivedDate = '*'   
     
  IF @VarDateType = 'SHIPMENTRECEIVEDDATE' --CL278   
    SET @NULLCreatedDate = '*'    
     
 IF OBJECT_ID('tempdb..#TypesData') IS NOT NULL   
 DROP TABLE #TypesData   
     
 IF OBJECT_ID('tempdb..#SMRYORDER') IS NOT NULL   
 DROP TABLE #SMRYORDER   
     
 IF OBJECT_ID('tempdb..#MAX_ACTIVITY') IS NOT NULL   
 DROP TABLE #MAX_ACTIVITY   
     
     
-------------------------CL278------------------------   
SELECT UPPER(DPProductLineKey) AS DPProductLineKey,       
       UPPER(DPServiceLineKey) AS DPServiceLineKey       
    into #ACCOUNTINFO       
    FROM OPENJSON(@AccountKeys)       
    WITH(       
   DPProductLineKey VARCHAR(MAX),       
   DPServiceLineKey VARCHAR(MAX)       
    )       
         
     
IF NOT EXISTS ( SELECT DPServiceLineKey FROM #ACCOUNTINFO WHERE DPServiceLineKey IS NOT NULL)        
    SET @VarDPServiceLineKeyJSON = '*'    
     
   
 IF NOT EXISTS ( SELECT DPProductLineKey FROM #ACCOUNTINFO WHERE DPProductLineKey IS NOT NULL)        
    SET @VarDPProductLineKeyJSON = '*'       
-------------------------CL278-------------------------   
     
  CREATE TABLE #TypesData([type] varchar(10), ActivityOrderId INT, ActivityName  varchar(255),[Count] INT,warehouseCode varchar(50),[Date] date)  --CL393
  CREATE TABLE #SMRYORDER(UPSOrderNumber varchar(128),SourceSystemKey int,ServiceMode varchar(128),IS_ASN INT,ActivityName  varchar(255),ActivityOrderId INT,warehouseCode varchar(50),[Date] date)   --CL393
  CREATE TABLE #MAX_ACTIVITY(ActivityDate datetime,UPSOrderNumber varchar(128),SourceSystemKey int)   
     
  IF @Debug>0   
  BEGIN   
  SET @EndTime = GETDATE()   
  SELECT 'Creating #TypesData table',@Starttime,@EndTime,DATEDIFF(SS,@Starttime,@EndTime) AS TimeTakeninsecs   
  SET @Starttime = GETDATE()   
  END   
       
       
 IF @VarDateType = 'SHIPMENTCREATIONDATE' OR ISNULL(@VarDateType,'') IN ('','*')  BEGIN   
     
     
  INSERT INTO #MAX_ACTIVITY   
   SELECT    
   MAX(MA.ActivityDate) AS ActivityDate   
  ,O.UPSOrderNumber   
  ,O.SourceSystemKey   
  FROM Summary.DIGITAL_SUMMARY_ORDERS O (NOLOCK)   
  INNER JOIN Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY MA (NOLOCK)   
            ON O.UPSOrderNumber=MA.UPSOrderNumber   
   AND O.SourceSystemKey=MA.SourceSystemKey      
  --WHERE O.AccountId = @VarAccountID     
  WHERE  ( O.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR O.AccountId = @VarAccountID )    ----arup   
  --AND O.CurrentMilestone <> 'PUTAWAY' --Added 09/15
  AND MA.MilestoneName <> 'PUTAWAY' --UPSGLD-14185
  --AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')   
  AND ((O.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR O.DP_SERVICELINE_KEY = @VarDPServiceLineKey)  OR @VarDPServiceLineKey = '*')   ----arup   
  AND (O.FacilityId IN (SELECT UPPER (TRIM (VALUE))FROM string_split(@VarwarehouseId, ',')) OR @VarwarehouseId = '*')   
  AND ((O.DateTimeReceived  BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime) OR @NULLCreatedDate = '*')   
  AND O.IS_INBOUND = 1   
  AND O.IS_ASN = 1
  AND ISNULL(O.OrderCancelledFlag,'N') = 'N' --UPSGLD-12521
  AND MA.CurrentMilestoneFlag = 'Y' --UPSGLD-15583
  GROUP BY    
   O.UPSOrderNumber   
  ,O.SourceSystemKey   
     
  END   
     
  IF @VarDateType = 'SHIPMENTRECEIVEDDATE' OR ISNULL(@VarDateType,'') IN ('','*') BEGIN    --UPSGLD-11917
     
  INSERT INTO #MAX_ACTIVITY   
   SELECT    
   MAX(MA.ActivityDate) AS ActivityDate   
  ,O.UPSOrderNumber   
  ,O.SourceSystemKey   
  FROM Summary.DIGITAL_SUMMARY_ORDERS O (NOLOCK)   
  INNER JOIN Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY MA (NOLOCK)   
            ON O.UPSOrderNumber=MA.UPSOrderNumber   
   AND O.SourceSystemKey=MA.SourceSystemKey      
  --WHERE O.AccountId = @VarAccountID     
  WHERE  ( O.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR O.AccountId = @VarAccountID )    ----arup   
   --AND O.CurrentMilestone <> 'PUTAWAY' --Added 09/15
  AND MA.MilestoneName <> 'PUTAWAY'       --UPSGLD-14185   
  --AND UPPER(MA.ActivityName) = 'RECEIVING STARTED' 
  AND MA.ActivityCode in ('RECS' , 'REC30' , 'REC90')
  --AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')   
  AND ((O.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR O.DP_SERVICELINE_KEY = @VarDPServiceLineKey)  OR @VarDPServiceLineKey = '*')   ----arup   
  AND (O.FacilityId IN (SELECT UPPER (TRIM (VALUE))FROM string_split(@VarwarehouseId, ',')) OR @VarwarehouseId = '*')     
  AND ((MA.ActivityDate  BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime) OR @NULLReceivedDate = '*')  ----arup   
  AND O.IS_INBOUND = 1   
  AND O.IS_ASN = 1
  AND ISNULL(O.OrderCancelledFlag,'N') = 'N' --UPSGLD-12521
  AND MA.CurrentMilestoneFlag = 'Y' --UPSGLD-15583
  GROUP BY    
   O.UPSOrderNumber   
  ,O.SourceSystemKey   
     
  END    
     
  CREATE CLUSTERED INDEX [Ix_ClIndexMAX_ACTIVITY] ON #MAX_ACTIVITY([UPSOrderNumber] ASC,[SourceSystemKey] ASC,ActivityDate ASC )WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]   
     
 INSERT INTO #SMRYORDER   
 SELECT    
  O.UPSOrderNumber   
 ,O.SourceSystemKey   
 ,O.ServiceMode   
 ,O.IS_ASN   
 ,WA.WIP_ActivityName AS ActivityName   
 ,WA.WIPActivityOrderId   
 ,O.OrderWareHouse as warehouseCode
 ,CASE WHEN @NULLCreatedDate='*' THEN CAST(MA.ActivityDate AS DATE) -- CL393 added cast function
       WHEN @NULLReceivedDate='*' THEN CAST(O.DateTimeReceived AS DATE) END AS [Date] --CL393 added cast function
 FROM Summary.DIGITAL_SUMMARY_ORDERS O (NOLOCK)   
  INNER JOIN (SELECT MA.UPSOrderNumber,MA.SourceSystemKey,MA.ActivityName,tmpActivity.ActivityDate FROM #MAX_ACTIVITY tmpActivity   
               INNER JOIN Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY MA (NOLOCK)   
        ON tmpActivity.UPSOrderNumber=MA.UPSOrderNumber   
        AND tmpActivity.SourceSystemKey=MA.SourceSystemKey   
        AND tmpActivity.ActivityDate=MA.ActivityDate   
       )as MA ON O.UPSOrderNumber=MA.UPSOrderNumber   
           AND O.SourceSystemKey=MA.SourceSystemKey    INNER JOIN master_data.WH_WIP_MAPPING_Activity WA (NOLOCK)    
            ON MA.ActivityName=WA.ActivityName   
   AND MA.SourceSystemKey=WA.SOURCE_SYSTEM_KEY   
 --WHERE O.AccountId = @VarAccountID   
 WHERE  ( O.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR O.AccountId = @VarAccountID )    ----arup   
  --AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')   
  AND ((O.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR O.DP_SERVICELINE_KEY = @VarDPServiceLineKey)  OR @VarDPServiceLineKey = '*')   ----arup   
  AND (O.FacilityId IN (SELECT UPPER (TRIM (VALUE))FROM string_split(@VarwarehouseId, ',')) OR @VarwarehouseId = '*')   
  AND ((O.DateTimeReceived  BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime) OR @NULLCreatedDate = '*')
  AND ((MA.ActivityDate  BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime) OR @NULLReceivedDate = '*')
  AND O.IS_INBOUND = 1   
  AND O.IS_ASN = 1   
  AND WA.[Type] = 'IN'   
  GROUP BY     
  O.UPSOrderNumber   
 ,O.SourceSystemKey   
 ,O.ServiceMode   
 ,O.IS_ASN   
 ,WA.WIP_ActivityName   
 ,WA.WIPActivityOrderId   
 ,O.OrderWarehouse
 ,CASE WHEN @NULLCreatedDate='*' THEN CAST(MA.ActivityDate as DATE) -- CL393 added cast function
       WHEN @NULLReceivedDate='*' THEN CAST(O.DateTimeReceived as DATE) END -- CL393 added cast function
     
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
     
     
  IF @VarType='*' OR @VarType='ASN'   
  BEGIN   
     
      INSERT INTO #TypesData   
      SELECT    
            'ASN' AS [type]   
            ,O.ActivityOrderId   
            ,O.ActivityName   
            --,O.ServiceMode as ShipmentMode   
            ,COUNT(1) AS [Count]
            ,O.warehouseCode
            ,O.[Date]
      FROM #SMRYORDER O (NOLOCK)   
      WHERE O.IS_ASN =1   
      AND (@VarType='*' OR @VarType='ASN')   
      GROUP BY O.ActivityOrderId,O.ActivityName,O.warehouseCode,O.[Date]--,O.ServiceMode   
     
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
        ,COUNT(IL.UPSOrderNumber) AS [Count]   
        ,O.warehouseCode
        ,O.[Date]
    FROM #SMRYORDER O (NOLOCK)   
    LEFT JOIN Summary.DIGITAL_SUMMARY_INBOUND_LINE IL (NOLOCK)    
    ON O.UPSOrderNumber=IL.UPSOrderNumber   
    AND O.SourceSystemKey=IL.SourceSystemKey   
    WHERE (@VarType='*' OR @VarType='LINES')   
    GROUP BY O.ActivityOrderId,O.ActivityName,O.warehouseCode,O.[Date] --,O.ServiceMode       
     
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
    ,CAST(SUM(IL.ShippedQuantity) AS INT) AS [Count]   
    ,O.warehouseCode
    ,O.[Date]
 FROM #SMRYORDER O (NOLOCK)   
 LEFT JOIN Summary.DIGITAL_SUMMARY_INBOUND_LINE IL (NOLOCK)    
 ON O.UPSOrderNumber=IL.UPSOrderNumber   
 AND O.SourceSystemKey=IL.SourceSystemKey  
 WHERE (@VarType='*' OR @VarType='UNITS')   
 GROUP BY O.ActivityOrderId, O.ActivityName,O.warehouseCode,O.[Date] --,O.ServiceMode   
     
  IF @Debug>0   
  BEGIN   
  SET @EndTime = GETDATE()   
  SELECT 'Insert UNITS Data',@Starttime,@EndTime,DATEDIFF(SS,@Starttime,@EndTime) AS TimeTakeninsecs   
  SET @Starttime = GETDATE()   
  END   
     
   END   
 
    IF @VarType='*' OR @VarType='CASES'   
   BEGIN   
     
       INSERT INTO #TypesData   
    SELECT    
        'CASES' AS [type]   
        ,O.ActivityOrderId   
        ,O.ActivityName   
        --,O.ServiceMode as ShipmentMode   
        ,SUM(ISNULL(IL.CASES,0)) AS [Count]   
        ,O.warehouseCode
        ,O.[Date]
    FROM #SMRYORDER O (NOLOCK)   
    LEFT JOIN Summary.DIGITAL_SUMMARY_INBOUND_LINE IL (NOLOCK)    
    ON O.UPSOrderNumber=IL.UPSOrderNumber   
    AND O.SourceSystemKey=IL.SourceSystemKey   
    WHERE (@VarType='*' OR @VarType='CASES')   
    GROUP BY O.ActivityOrderId,O.ActivityName,O.warehouseCode,O.[Date] --,O.ServiceMode       
     
  IF @Debug>0   
  BEGIN   
  SET @EndTime = GETDATE()   
  SELECT 'Insert CASES Data',@Starttime,@EndTime,DATEDIFF(SS,@Starttime,@EndTime) AS TimeTakeninsecs   
  SET @Starttime = GETDATE()   
  END   
     
   END   
     
   SELECT [type]   
         ,ActivityName   
         --,ShipmentMode   
         ,warehouseCode
         ,[Date] 
         ,ISNULL([Count] , 0) [Count] --UPSGLD-11878
   FROM #TypesData ORDER BY ActivityOrderId   
     
  IF @Debug>0   
  BEGIN   
  SET @EndTime = GETDATE()   
  SELECT 'Final Select statement',@Starttime,@EndTime,DATEDIFF(SS,@Starttime,@EndTime) AS TimeTakeninsecs   
  SET @Starttime = GETDATE()   
  END   
     
END
GO