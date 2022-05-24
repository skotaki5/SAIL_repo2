  /****** Object:  StoredProcedure [digital].[rpt_Inbound_WIP_Shipment_Listing]    Script Date: 3/1/2022 11:43:49 AM ******/
SET ANSI_NULLS ON
GO
 
SET QUOTED_IDENTIFIER ON
GO
 
 
 
 
 
 
 
 
 
/**
change log
------------
Developer       date        SPRINT                  Changes
Venkata         09/15/2021                          Added O.CurrentMilestone <> 'PUTAWAY' condition while fitering Summary Orders table
Venkata/Arup    10/09/2021  SPRINT-40 CL279         Modified @Data parameter to accept @ShipmentReceivedStartdate and @ShipmentReceivedenddate parameter, Accept @AccountKey parameter
Venkata         11/01/2021  UPSGLD-11878            replace null value with 0 for the count column
Venkata         12/10/2021  UPSGLD-12521            Removed the cancelled order records
Sheetal         02/08/2022  SPRINT 48 - CL374       To match with Summary proc CL374 made changes in this drilldown Proc -added casescount column
Sheetal         02/10/2022  UPSGLD-13908            Added Else statement for #Max_activity table in case both creation and received dates are provided.
Venkata         02/22/2022  Sprint49 UPSGLD-14185   changed filtering condition from O.CurrentMilestone to MA.MilestoneName which was implemented on 09/15
 
 
**/
 
/****
--SWR
 
EXEC [digital].[rpt_Inbound_WIP_Shipment_Listing]   @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = '150E5128-B3E3-44A1-BF6C-CD2E4D9856DA',@DPEntityKey = NULL,@Date = NULL,@wipActivityName='*', @warehouseCode='*', @topRow=0,@inboundType ='*',@milestoneStatus='*'
 
EXEC [digital].[rpt_Inbound_WIP_Shipment_Listing] 
--@DPProductLineKey = Null,@DPServiceLineKey = Null,
@AccountKeys = N'[{"DPProductLineKey":"870561E1-A974-483B-AA0D-A724C5D402C9","DPServiceLineKey":"*"}]'
,@DPEntityKey = NULL,@Date = '{"shipmentReceivedStartDate":"2021-08-29","shipmentReceivedEndDate":"2021-10-29"}',
@wipActivityName='*', @warehouseCode='*', @topRow=150,@inboundType ='*',@milestoneStatus='*'
 
 
EXEC [digital].[rpt_Inbound_WIP_Shipment_Listing] 
--@DPProductLineKey = Null,@DPServiceLineKey = Null,
@AccountKeys = N'[{"DPProductLineKey":"5C7309FC-A905-4FC5-8AE4-5185CBAE64E0","DPServiceLineKey":"*"}]'
,@DPEntityKey = NULL,@Date = '{"shipmentCreationStartDate":"2022-01-03","shipmentCreationEndDate":"2022-02-03"}',
@wipActivityName='*', @warehouseCode='*', @topRow=0,@inboundType ='*',@milestoneStatus='*'
 
****/
 
 
 
 
CREATE PROCEDURE [digital].[rpt_Inbound_WIP_Shipment_Listing]
 
@DPProductLineKey varchar(50) = NULL, @DPServiceLineKey varchar(50) = NULL , @DPEntityKey varchar(50), @AccountKeys nvarchar(max) = NULL,
@Date nvarchar(max)=NULL, @wipActivityName nvarchar(max)='*', @warehouseCode nvarchar(max)='*', @topRow int,
@inboundType varchar(50)='',@milestoneStatus nvarchar(max)='*'
 
AS
 
BEGIN
 
  DECLARE @VarAccountID varchar(50) ,
          @VarDPServiceLineKey varchar(50),
          @VarDPEntityKey varchar(50),
          --@VarwarehouseId varchar(max),
          @VarwarehouseCode varchar(max),
          --@VarDate datetime,
          @NULLCreatedDate varchar(1),
          @NULLReceivedDate Varchar(1), --CL279
          @VarInboundType varchar(50),
          @NULLInboundType varchar(1),
          @VarMilestoneStatus nvarchar(max),
          @NULLMileStoneStatus varchar(1),
          @VarWIPActivityName nvarchar(max),
          @NULLWIPActivityName varchar(1),
          @isASN           INT,
          @shipmentCreationStartDate date,
          @shipmentCreationEndDate date,
          @shipmentReceivedStartDate date, --CL279
          @shipmentReceivedEndDate date, --CL279
          @shipmentCreationStartDateTime datetime,
          @shipmentCreationEndDateTime datetime,
          @shipmentReceivedStartDateTime datetime, --CL279
          @shipmentReceivedEndDateTime datetime, --CL279
          @VarDPServiceLineKeyJSON VARCHAR(500),   
          @VarDPProductLineKeyJSON VARCHAR(500)
 
 
  SELECT  @shipmentCreationStartDate   = shipmentCreationStartDate
         ,@shipmentCreationEndDate     = shipmentCreationEndDate
         ,@shipmentReceivedStartDate   = shipmentReceivedStartDate --CL279
         ,@shipmentReceivedEndDate     = shipmentReceivedEndDate --CL279
 
  FROM OPENJSON(@Date)
  WITH (
  shipmentCreationStartDate date,
  shipmentCreationEndDate date,
  shipmentReceivedStartDate date,
  shipmentReceivedEndDate date
     )
 
  SET @VarAccountID = UPPER(@DPProductLineKey)
  SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)
  SET @VarDPEntityKey = UPPER(@DPEntityKey)
  SET @VarInboundType=UPPER(@inboundType)
  SET @VarMilestoneStatus=UPPER(@milestoneStatus)
  SET @VarWIPActivityName=UPPER(@wipActivityName)
  SET @VarwarehouseCode = @warehouseCode
  SET @shipmentCreationStartDateTime=@shipmentCreationStartDate--CL279
  SET @shipmentCreationEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @shipmentCreationEndDate)))
  SET @shipmentReceivedStartDateTime=@shipmentReceivedStartDate --CL279
  SET @shipmentReceivedEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @shipmentReceivedEndDate))) --CL279
   
  ------------------------CL279------------------------
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
------------------------CL279------------------------
 
  
 
 
  IF ISNULL(@shipmentCreationStartDate,'')='' OR ISNULL(@shipmentCreationEndDate,'')=''
    SET @NULLCreatedDate = '*'
 
  IF ISNULL(@shipmentReceivedStartDate,'')='' OR ISNULL(@shipmentReceivedEndDate,'')='' --CL279
    SET @NULLReceivedDate = '*'
 
  IF @DPServiceLineKey IS NULL
    SET @VarDPServiceLineKey = '*'
 
  IF @DPEntityKey IS NULL
    SET @VarDPEntityKey = '*'
 
  IF @VarInboundType='' OR ISNULL(@VarInboundType,'*')='*'
     SET @NULLInboundType = '*'
  ELSE
   SET @isASN = CASE WHEN @VarInboundType='ASN' THEN 1
                     WHEN @VarInboundType='TRANSPORT ORDER' THEN 0
                END
 
  IF @VarMilestoneStatus='' OR ISNULL(@VarMilestoneStatus,'*')='*'
  SET @NULLMileStoneStatus='*'
 
 
  IF @VarWIPActivityName='' OR  ISNULL(@VarWIPActivityName,'*')='*'
  SET @NULLWIPActivityName='*'
 
  IF OBJECT_ID('tempdb..#MAX_ACTIVITY') IS NOT NULL
    DROP TABLE #MAX_ACTIVITY
 
  CREATE TABLE #MAX_ACTIVITY(ActivityDate datetime,UPSOrderNumber varchar(128),SourceSystemKey int)
  
  
 
   
  --INSERT INTO #MAX_ACTIVITY
  --SELECT
  --MAX(ActivityDate) AS ActivityDate
  --,O.upsShipmentNumber
  --,O.SourceSystemKey
  --FROM #DIGITAL_SUMMARY_ORDERS O (NOLOCK)
  --INNER JOIN Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY MA (NOLOCK)
  --          ON O.upsShipmentNumber=MA.UPSOrderNumber
        --  AND O.SourceSystemKey=MA.SourceSystemKey           
  --GROUP BY
  -- O.upsShipmentNumber
  --,O.SourceSystemKey
 
 
  ------------------------
 
  IF @NULLReceivedDate = '*'
  BEGIN     
     
  INSERT INTO #MAX_ACTIVITY   
   SELECT    
   MAX(ActivityDate) AS ActivityDate   
  ,O.UPSOrderNumber   
  ,O.SourceSystemKey   
  FROM Summary.DIGITAL_SUMMARY_ORDERS O (NOLOCK)   
  INNER JOIN Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY MA (NOLOCK)   
            ON O.UPSOrderNumber=MA.UPSOrderNumber   
   AND O.SourceSystemKey=MA.SourceSystemKey      
  WHERE  ( O.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR O.AccountId = @VarAccountID )    ----arup   
  --AND O.CurrentMilestone <> 'PUTAWAY' --Added 09/15
  AND MA.MilestoneName <> 'PUTAWAY' --UPSGLD-14185
  AND ((O.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR O.DP_SERVICELINE_KEY = @VarDPServiceLineKey)  OR @VarDPServiceLineKey = '*')   ----arup   
  AND (O.OrderWarehouse IN (SELECT UPPER (TRIM (VALUE))FROM string_split(@warehouseCode, ',')) OR @warehouseCode = '*')   
  AND ((O.DateTimeReceived  BETWEEN @shipmentCreationStartDateTime AND @shipmentCreationEndDateTime) OR @NULLCreatedDate = '*')   
  AND O.IS_INBOUND = 1   
  AND O.IS_ASN = 1
  AND ISNULL(O.OrderCancelledFlag,'N') = 'N' --UPSGLD-12521
  GROUP BY    
   O.UPSOrderNumber   
  ,O.SourceSystemKey
   
     
  END   
 
     
  IF @NULLCreatedDate = '*'
  BEGIN  
     
      INSERT INTO #MAX_ACTIVITY   
       SELECT    
       MAX(ActivityDate) AS ActivityDate   
      ,O.UPSOrderNumber   
      ,O.SourceSystemKey   
      FROM Summary.DIGITAL_SUMMARY_ORDERS O (NOLOCK)   
      INNER JOIN Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY MA (NOLOCK)   
                ON O.UPSOrderNumber=MA.UPSOrderNumber   
       AND O.SourceSystemKey=MA.SourceSystemKey      
       WHERE  ( O.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR O.AccountId = @VarAccountID )    ----arup   
        --AND O.CurrentMilestone <> 'PUTAWAY' --Added 09/15
            AND MA.MilestoneName <> 'PUTAWAY' --UPSGLD-14185 
          AND MA.ActivityCode in ('RECS' , 'REC30' , 'REC90')
      --AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')   
      AND ((O.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR O.DP_SERVICELINE_KEY = @VarDPServiceLineKey)  OR @VarDPServiceLineKey = '*')   ----arup   
      AND (O.OrderWarehouse IN (SELECT UPPER (TRIM (VALUE))FROM string_split(@warehouseCode, ',')) OR @warehouseCode = '*')     
      AND ((MA.ActivityDate  BETWEEN @shipmentReceivedStartDateTime AND @shipmentReceivedEndDateTime) OR @NULLReceivedDate = '*')  ----arup   
      AND O.IS_INBOUND = 1   
      AND O.IS_ASN = 1
      AND ISNULL(O.OrderCancelledFlag,'N') = 'N' --UPSGLD-12521
      GROUP BY    
       O.UPSOrderNumber   
      ,O.SourceSystemKey   
     
  END  
   
  ELSE
 
INSERT INTO #MAX_ACTIVITY   
   SELECT    
   MAX(ActivityDate) AS ActivityDate   
  ,O.UPSOrderNumber   
  ,O.SourceSystemKey   
  FROM Summary.DIGITAL_SUMMARY_ORDERS O (NOLOCK)   
  INNER JOIN Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY MA (NOLOCK)   
            ON O.UPSOrderNumber=MA.UPSOrderNumber   
   AND O.SourceSystemKey=MA.SourceSystemKey      
  WHERE  ( O.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR O.AccountId = @VarAccountID )    ----arup   
    --AND O.CurrentMilestone <> 'PUTAWAY' --Added 09/15
  AND MA.MilestoneName <> 'PUTAWAY' --UPSGLD-14185   
  AND ((O.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR O.DP_SERVICELINE_KEY = @VarDPServiceLineKey)  OR @VarDPServiceLineKey = '*')   ----arup   
  AND (O.OrderWarehouse IN (SELECT UPPER (TRIM (VALUE))FROM string_split(@warehouseCode, ',')) OR @warehouseCode = '*')   
  AND ((O.DateTimeReceived  BETWEEN @shipmentCreationStartDateTime AND @shipmentCreationEndDateTime) OR @NULLCreatedDate = '*')   
  AND O.IS_INBOUND = 1   
  AND O.IS_ASN = 1
  AND ISNULL(O.OrderCancelledFlag,'N') = 'N' --UPSGLD-12521
  GROUP BY    
   O.UPSOrderNumber   
  ,O.SourceSystemKey
 
  ------------------------
 
   SELECT
    O.SourceSystemKey,
    O.UPSOrderNumber AS upsShipmentNumber,
    O.OrderNumber AS referenceNumber,
    O.FacilityId AS warehouseId,
    --O.CurrentMilestone AS milestoneStatus,
    MA.MilestoneName AS milestoneStatus, --UPSGLD-14185
    CASE WHEN O.IS_ASN=1 THEN 'ASN' ELSE 'TRANSPORT ORDER' END AS inboundType,
    O.OrderWarehouse warehouseCode,
    O.DateTimeReceived AS shipmentCreationDate
    ,MA.ActivityDate as shipmentReceivedDate
    INTO #DIGITAL_SUMMARY_ORDERS
  FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O
   
  INNER JOIN (SELECT DISTINCT MA.UPSOrderNumber,MA.SourceSystemKey,MA.ActivityName,tmpActivity.ActivityDate,MA.MilestoneName FROM #MAX_ACTIVITY tmpActivity   
               INNER JOIN Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY MA (NOLOCK)   
        ON tmpActivity.UPSOrderNumber=MA.UPSOrderNumber   
        AND tmpActivity.SourceSystemKey=MA.SourceSystemKey   
        AND tmpActivity.ActivityDate=MA.ActivityDate   
       )as MA ON O.UPSOrderNumber=MA.UPSOrderNumber 
        
  WHERE  ( O.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR O.AccountId = @VarAccountID )    --CL279
    --AND O.CurrentMilestone <> 'PUTAWAY' --Added 09/15
  AND MA.MilestoneName <> 'PUTAWAY' --UPSGLD-14185
  AND ((O.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR O.DP_SERVICELINE_KEY = @VarDPServiceLineKey)  OR @VarDPServiceLineKey = '*')   --CL279
  AND (@NULLMileStoneStatus = '*' OR O.CurrentMilestone COLLATE SQL_Latin1_General_CP1_CI_AS IN (select (TRIM(value)) from string_split(@VarMilestoneStatus,',')))
  AND ((O.DateTimeReceived BETWEEN @shipmentCreationStartDateTime AND @shipmentCreationEndDateTime) OR @NULLCreatedDate = '*')
  --AND ((O.DateTimeShipped BETWEEN @shipmentReceivedStartDateTime AND @shipmentReceivedEndDateTime) OR @NULLReceivedDate = '*')  --CL279
   AND ((MA.ActivityDate  BETWEEN @shipmentReceivedStartDateTime AND @shipmentReceivedEndDateTime) OR @NULLReceivedDate = '*')
  AND (O.OrderWarehouse COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (VALUE) FROM string_split(@VarwarehouseCode, ',')) OR @VarwarehouseCode = '*')
  AND O.IS_INBOUND= 1
  AND (@NULLInboundType='*' OR COALESCE(O.IS_ASN,0)=@isASN)
 
 
  CREATE CLUSTERED INDEX [Ix_ClIndexMAX_ACTIVITY] ON #MAX_ACTIVITY([UPSOrderNumber] ASC,[SourceSystemKey] ASC,ActivityDate ASC )WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]
 
  SELECT
    O.upsShipmentNumber,
    O.referenceNumber,
    O.warehouseId,
    O.warehouseCode,
    O.milestoneStatus,
    O.inboundType,
    O.shipmentCreationDate,
    WA.WIP_ActivityName AS wipActivityName,
    SIL.UPSASNNumber AS asnNumber,
    MA.ActivityCode,
    CASE WHEN MA.ActivityCode in ('RECS' , 'REC30' , 'REC90') THEN MA.ActivityDate ELSE NULL END AS shipmentReceivedDate,
    COUNT(SIL.UPSOrderNumber) AS linesCount,
    CAST(SUM(SIL.ShippedQuantity) AS INT) AS unitsCount,
    SUM(ISNULL(SIL.CASES,0)) AS CasesCount  --CL374
  INTO #FinalSmryOrder
  FROM #DIGITAL_SUMMARY_ORDERS O
  INNER JOIN (SELECT DISTINCT MA.UPSOrderNumber,MA.SourceSystemKey,MA.ActivityName,MA.ActivityDate,MA.ActivityCode FROM #MAX_ACTIVITY tmpActivity
                       INNER JOIN Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY MA (NOLOCK)
                       ON tmpActivity.UPSOrderNumber=MA.UPSOrderNumber
                       AND tmpActivity.SourceSystemKey=MA.SourceSystemKey
                       AND tmpActivity.ActivityDate=MA.ActivityDate
             )as MA ON O.upsShipmentNumber=MA.UPSOrderNumber
                    AND O.SourceSystemKey=MA.SourceSystemKey
  INNER JOIN master_data.WH_WIP_MAPPING_Activity WA (NOLOCK)
            ON MA.ActivityName=WA.ActivityName
            AND MA.SourceSystemKey=WA.SOURCE_SYSTEM_KEY
  LEFT JOIN Summary.DIGITAL_SUMMARY_INBOUND_LINE SIL (NOLOCK)
            ON O.upsShipmentNumber=SIL.UPSOrderNumber
            AND O.SourceSystemKey=SIL.SourceSystemKey
 WHERE WA.[Type] = 'IN'
 AND ( @NULLWIPActivityName='*' OR WA.WIP_ActivityName COLLATE SQL_Latin1_General_CP1_CI_AS IN (select (TRIM(value)) from string_split(@VarWIPActivityName,',')))
 AND ((O.shipmentCreationDate  BETWEEN @shipmentCreationStartDateTime AND @shipmentCreationEndDateTime) OR @NULLCreatedDate = '*')   --UPSGLD-13908
  AND ((O.shipmentReceivedDate  BETWEEN @shipmentReceivedStartDateTime AND @shipmentReceivedEndDateTime) OR @NULLReceivedDate = '*')  --UPSGLD-13908
 GROUP BY
 O.upsShipmentNumber,
 O.referenceNumber,
 O.warehouseId,
 O.milestoneStatus,
 O.inboundType,
 O.shipmentCreationDate,
 WA.WIP_ActivityName,
 SIL.UPSASNNumber,
 O.warehouseCode,
CASE WHEN MA.ActivityCode in ('RECS' , 'REC30' , 'REC90') THEN MA.ActivityDate ELSE NULL END
,MA.ActivityCode
 
 
  IF @NULLReceivedDate = '*' OR  @NULLCreatedDate = '*' --UPSGLD-13908
  BEGIN
    SELECT COUNT(1) totalShipments from #FinalSmryOrder
  END
  ELSE
  SELECT COUNT(1) totalShipments from #FinalSmryOrder where shipmentReceivedDate is not null --UPSGLD-13908
 
 
      IF @NULLReceivedDate = '*' OR  @NULLCreatedDate = '*' --UPSGLD-13908
      BEGIN
  IF @topRow = 0
 
    SELECT
    upsShipmentNumber,
    asnNumber,
    referenceNumber,
    ISNULL(linesCount,0) linesCount, --UPSGLD-11878
    ISNULL(unitsCount,0) unitsCount, --UPSGLD-11878
    ISNULL(CasesCount,0) casesCount,--CL374
    wipActivityName,
    warehouseId as warehouseId,
    warehouseCode,
    milestoneStatus,
    inboundType,
    shipmentCreationDate,
    shipmentReceivedDate --CL279
    FROM #FinalSmryOrder
    ORDER BY shipmentCreationDate DESC
 
  ELSE
 
    SELECT TOP (@topRow)
    upsShipmentNumber,
    asnNumber,
    referenceNumber,
    ISNULL(linesCount,0) linesCount, --UPSGLD-11878
    ISNULL(unitsCount,0) unitsCount, --UPSGLD-11878
    ISNULL(CasesCount,0) casesCount,--CL374
    wipActivityName,
    warehouseId,
    warehouseCode,
    milestoneStatus,
    inboundType,
    shipmentCreationDate,
    shipmentReceivedDate --CL279
    FROM #FinalSmryOrder
    ORDER BY shipmentCreationDate DESC
 
END
ELSE
 
 IF @topRow = 0
 
    SELECT
    upsShipmentNumber,
    asnNumber,
    referenceNumber,
    ISNULL(linesCount,0) linesCount,
    ISNULL(unitsCount,0) unitsCount,
    ISNULL(CasesCount,0) casesCount, ---CL374
    wipActivityName,
    warehouseId as warehouseId,
    warehouseCode,
    milestoneStatus,
    inboundType,
    shipmentCreationDate,
    shipmentReceivedDate
    FROM #FinalSmryOrder
    where shipmentReceivedDate is not null
    ORDER BY shipmentCreationDate DESC
 
  ELSE
 
    SELECT TOP (@topRow)
    upsShipmentNumber,
    asnNumber,
    referenceNumber,
    ISNULL(linesCount,0) linesCount,
    ISNULL(unitsCount,0) unitsCount,
    ISNULL(CasesCount,0) casesCount,--CL374
    wipActivityName,
    warehouseId,
    warehouseCode,
    milestoneStatus,
    inboundType,
    shipmentCreationDate,
    shipmentReceivedDate
    FROM #FinalSmryOrder
    where shipmentReceivedDate is not null
    ORDER BY shipmentCreationDate DESC
 
 
 
END
GO