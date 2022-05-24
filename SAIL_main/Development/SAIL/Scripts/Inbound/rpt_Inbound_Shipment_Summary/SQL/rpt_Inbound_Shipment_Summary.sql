/****** Object:  StoredProcedure [digital].[rpt_Inbound_Shipment_Summary]    Script Date: 2/11/2022 1:14:41 PM ******/
SET ANSI_NULLS ON
GO
 
SET QUOTED_IDENTIFIER ON
GO
 
 
 
 
 
 
 
   
   
   
   
/**
CHANGE LOG
---------
DEVELOPER               DATE                    SPRINT                  COMMENTS
VENKATA                 09/29/2021                                      ADDED WHERE CLAUSE IN THE FIRST SELECT STATEMENT
CHAITANYA KHOT          09/30/2021              38th                    TEMPERATURESHIPMENTCOUNT COLUMN IS ADDED
SHEETAL                 N/A                     CL270                   Added ShipmentMode column in the final result set
SHEETAL                 11/12/2021          UPSGLD-12014                1) Made the changes to get ShipmentMode result set.
                                                                        2) Removed DIGITAL_SUMMARY_MILESTONE_ACTIVITY joining condition from #DeliveredShipmentData as we already have needed data in #DIGITAL_SUMMARY_ORDERS
SHEETAL                 12/22/2021              CL332                   Added 'ASN CREATED','FTZ','RECEIVING','PUTAWAY' in #DeliveredShipmentData to get Ontime and Late resultset.
SHEETAL                 01/05/2022              CL345                   Added shells for Result set 13 & 14 (CL sheet sent by Nitin on 01/05/2022)
Harsha                  1/10/2022               CL345                   New result set will have following columns. Carrier, DeliveryStatus, Count
                                                CL345                   New result set will have following columns - SourceCountry, SourceCity, DestinationCountry,DestinationCity, DeliveryStatus, Count
Harsha                  1/10/2022               CL345                   Added filter condition in Lanecount to remove 'NotAvailable' source/destination city
Sheetal                 02/01/2022              UPSGLD-13511            Replaced the milestones to 'TRANSPORTATION PLANNING' from 'PENDING','RATED','BOOKED'
AVINASH                 02/10/2022              49, CL387               Added IS_MANAGED input parameter
**/
   
  
   
   
/****  
--AMR 
EXEC [digital].[rpt_Inbound_Shipment_Summary]   @DPProductLineKey = 'E648FA6F-6253-428E-8AC9-201E3EF83B91',@DPServiceLineKey = '53D60776-D3BD-4E6A-8E37-F591C148294B',@DPEntityKey = NULL,@StartCreatedDate = '2020-08-15',@EndCreatedDate = '2020-08-21', @war
ehouseId = '*',@DateType='' 
--SWR 
EXEC [digital].[rpt_Inbound_Shipment_Summary]   @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = '150E5128-B3E3-44A1-BF6C-CD2E4D9856DA',@DPEntityKey = NULL,@StartCreatedDate = '2020-10-15',@EndCreatedDate = '2020-10-21', @war
ehouseId = '*',@DateType='' 
--Cambium 
EXEC [digital].[rpt_Inbound_Shipment_Summary]   @DPProductLineKey = 'A10F512B-C7F4-42A0-909C-21DD95A7D921',@DPServiceLineKey = '53DDDBC2-FB5A-4E70-A92E-8312DC9FDD05',@DPEntityKey = NULL,@StartCreatedDate = '2020-08-27',@EndCreatedDate = '2020-09-02', @war
ehouseId = '*',@DateType='' 
****/ 
   
-- EXEC [digital].[rpt_Inbound_Shipment_Summary]
-- @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',
--@DPServiceLineKey = '*',@DPEntityKey = NULL, @warehouseId = '*'
--,@InboundType='Transport Order',
--@Date='{"shipmentCreationStartDate": "2021-10-28","shipmentCreationEndDate": "2022-01-26"}'
 
CREATE PROCEDURE [digital].[rpt_Inbound_Shipment_Summary]  
 
@DPProductLineKey varchar(50),  
@DPServiceLineKey varchar(50),  
@DPEntityKey varchar(50),  
@StartCreatedDate date= NULL, 
@EndCreatedDate date= NULL,  
@warehouseId varchar(max),  
@DateType varchar(50)= NULL, 
@Date nvarchar(max) = NULL, 
@inboundType varchar(50)=''
,@IsManaged varchar(5)  ---CL387
 
   
AS 
 
 
   
BEGIN 
   
  DECLARE @VarAccountID varchar(50), 
          @VarDPServiceLineKey varchar(50), 
          @VarDPEntityKey varchar(50), 
          @VarStartCreatedDateTime datetime, 
          @VarEndCreatedDateTime datetime, 
          @NULLCreatedDate varchar(1), 
          @VarwarehouseId varchar(max), 
    @VarInboundType varchar(50), 
    @NULLInboundType varchar(1), 
    @isASN           INT, 
    @varDateType     varchar(50), 
    @shipmentCreationStartDate date, 
    @shipmentCreationEndDate date, 
    @scheduledDeliveryStartDate date, 
    @scheduledDeliveryEndDate date, 
    @scheduleToShipStartDate date, 
    @scheduleToShipEndDate   date, 
    @actualDeliveryStartDate date, 
    @actualDeliveryEndDate date, 
    @shipmentCreationStartDateTime datetime, 
    @shipmentCreationEndDateTime datetime, 
    @scheduledDeliveryStartDateTime datetime, 
    @scheduledDeliveryEndDateTime datetime, 
    @scheduleToShipStartDateTime datetime, 
    @scheduleToShipEndDateTime   datetime, 
    @actualDeliveryStartDateTime date, 
    @actualDeliveryEndDateTime date, 
    @NULLShipmentCreatedDate varchar(1), 
    @NULLScheduleToShipDate VARCHAR(1), 
    @NULLActualDeliveryDate varchar(1), 
    @NULLScheduledDeliveryDate varchar(1) 
   ,@VarIsManaged varchar(5)  ---CL387
   
   
   
   SELECT  @shipmentCreationStartDate = shipmentCreationStartDate, 
          @shipmentCreationEndDate   = shipmentCreationEndDate, 
    @scheduledDeliveryStartDate = scheduleDeliveryStartDate,  
    @scheduledDeliveryEndDate   = scheduleDeliveryEndDate, 
    @scheduleToShipStartDate    = scheduleToShipStartDate, 
    @scheduleToShipEndDate      = scheduleToShipEndDate, 
    @actualDeliveryStartDate    = actualDeliveryStartDate, 
    @actualDeliveryEndDate      = actualDeliveryEndDate 
  FROM OPENJSON(@Date) 
  WITH ( 
 shipmentCreationStartDate date, 
 shipmentCreationEndDate date, 
 scheduleDeliveryStartDate date, 
 scheduleDeliveryEndDate date, 
 scheduleToShipStartDate date, 
 scheduleToShipEndDate date, 
 actualDeliveryStartDate date, 
 actualDeliveryEndDate date 
   
       ) 
   
   
   
   
  SET @VarAccountID = UPPER(@DPProductLineKey) 
  SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey) 
  SET @VarDPEntityKey = UPPER(@DPEntityKey) 
  SET @VarwarehouseId = UPPER(@warehouseId) 
  SET @VarStartCreatedDateTime=@StartCreatedDate 
  SET @VarEndCreatedDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @EndCreatedDate))) 
  SET @VarInboundType=UPPER(@inboundType) 
  SET @varDateType=UPPER(@DateType) 
  SET @VarIsManaged = UPPER(@IsManaged) --CL387
   
  SET @shipmentCreationStartDateTime=@shipmentCreationStartDate 
  SET @shipmentCreationEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @shipmentCreationEndDate))) 
   
  SET @scheduledDeliveryStartDateTime=@scheduledDeliveryStartDate 
  SET @scheduledDeliveryEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @scheduledDeliveryEndDate))) 
   
  SET @scheduleToShipStartDateTime =@scheduleToShipStartDate 
  SET @scheduleToShipEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @scheduleToShipEndDate))) 
   
  SET @actualDeliveryStartDateTime =@actualDeliveryStartDate 
  SET @actualDeliveryEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @actualDeliveryEndDate))) 
   
   IF @shipmentCreationStartDate IS NOT NULL AND @shipmentCreationEndDate IS NOT NULL 
  and @shipmentCreationStartDate<>'' and @shipmentCreationEndDate<>'' 
  BEGIN 
  SET @shipmentCreationStartDateTime=@shipmentCreationStartDate 
  SET @shipmentCreationEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @shipmentCreationEndDate))) 
  END 
  ELSE IF(@StartCreatedDate IS NOT NULL AND @EndCreatedDate IS NOT NULL) 
  BEGIN 
  SET @shipmentCreationStartDateTime=@StartCreatedDate 
  SET @shipmentCreationEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @EndCreatedDate))) 
  END 
  ELSE 
  BEGIN 
   SET @NULLCreatedDate='*' 
  END 
   
     
   
  IF ISNULL(@shipmentCreationStartDate,'')='' OR ISNULL(@shipmentCreationEndDate,'')='' 
    SET @NULLShipmentCreatedDate = '*' 
   
  IF ISNULL(@scheduleToShipStartDate,'')='' OR ISNULL(@scheduleToShipEndDate,'')='' 
    SET @NULLScheduleToShipDate = '*' 
   
 IF ISNULL(@actualDeliveryStartDate,'')='' OR ISNULL(@actualDeliveryEndDate,'')='' 
    SET @NULLActualDeliveryDate = '*' 
   
 IF ISNULL(@scheduledDeliveryStartDate,'')='' OR ISNULL(@scheduledDeliveryEndDate,'')='' 
    SET @NULLScheduledDeliveryDate = '*' 
   
   
  --IF @StartCreatedDate IS NULL OR @EndCreatedDate IS NULL 
  --  SET @NULLCreatedDate = '*' 
   
  IF @DPServiceLineKey IS NULL 
    SET @VarDPServiceLineKey = '*' 
   
  IF @DPEntityKey IS NULL 
    SET @VarDPEntityKey = '*' 
   
  IF @VarInboundType='' OR @VarInboundType IS NULL 
     SET @NULLInboundType = '*' 
  ELSE 
   SET @isASN = CASE WHEN @VarInboundType='ASN' THEN 1 
                     WHEN @VarInboundType='TRANSPORT ORDER' THEN 0  
                END 
   
   
   
IF @DateType is null 
begin  
if @NULLCreatedDate = '*' 
begin 
set  @varDateType = 'SHIPMENTDELIVERYDATE'  
end 
if @NULLActualDeliveryDate = '*' 
begin 
set @varDateType = 'SHIPMENTCREATIONDATE'   
end 
end 
   
   
IF @Date is NULL and UPPER(@DateType)='SHIPMENTCREATIONDATE' 
begin 
set @shipmentCreationStartDateTime=@StartCreatedDate 
set @shipmentCreationEndDateTime=DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @EndCreatedDate))) 
set @NULLCreatedDate = '' 
end 
else IF @Date is NULL and UPPER(@DateType)='SHIPMENTDELIVERYDATE' 
begin 
set @actualDeliveryStartDateTime=@StartCreatedDate 
set @actualDeliveryEndDateTime=DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @EndCreatedDate))) 
set @NULLActualDeliveryDate = '' 
end 
   
   
   
   
   
  CREATE TABLE #DIGITAL_SUMMARY_ORDERS_FILTER( DateTypeDate datetime,  
          ServiceLevel [varchar](255),  
         --CL345
            OriginCity [nvarchar](255),
            DestinationCity [nvarchar](255),
         --CL345
                                        OriginCountry [nvarchar](255), 
          DestinationCountry [nvarchar](255), 
             ShipmentMode [varchar](128),  
          milestoneStatus [varchar](40), 
          UPSOrderNumber [varchar](40), 
          Carrier varchar(255), 
          SourceSystemKey int, 
          actualDeliveryDateTime datetime 
          ) 
   
   
  CREATE TABLE #DIGITAL_SUMMARY_ORDERS( DateTypeDate datetime,  
          ServiceLevel [varchar](255), 
         --CL345
            OriginCity [nvarchar](255),
            DestinationCity [nvarchar](255),
         --CL345
                                        OriginCountry [nvarchar](255), 
          DestinationCountry [nvarchar](255), 
             ShipmentMode [varchar](128),  
          milestoneStatus [varchar](40), 
          UPSOrderNumber [varchar](40), 
          Carrier varchar(255), 
          SourceSystemKey int, 
          actualDeliveryDateTime datetime 
          ) 
   
IF @varDateType = 'SHIPMENTCREATIONDATE' and  @NULLActualDeliveryDate = '*' 
   
BEGIN 
  --CL345
  INSERT INTO #DIGITAL_SUMMARY_ORDERS_FILTER(ServiceLevel,OriginCity,DestinationCity,OriginCountry,DestinationCountry,ShipmentMode,milestoneStatus,UPSOrderNumber, Carrier, SourceSystemKey,actualDeliveryDateTime)  --11/12/2021
  SELECT 
    O.ServiceLevel,
    --CL345
    O.OriginCity,
    O.DestinationCity,
    --CL345
    O.OriginCountry, 
    O.DestinationCountry, 
    O.ServiceMode AS ShipmentMode, 
    CurrentMilestone AS milestoneStatus,
    O.UPSOrderNumber,
 O.Carrier AS Carrier, 
 O.SourceSystemKey AS SourceSystemKey, 
    
 (SELECT MAX(ma.ActivityDate) FROM Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) ma 
           WHERE O.UPSOrderNumber = ma.UPSOrderNumber 
           AND O.SourceSystemKey = ma.SourceSystemKey 
           AND ma.ActivityCode in('D','D1','D9') 
           AND ma.AccountId = @VarAccountID --CL345
 ) AS actualDeliveryDateTime 
  FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O 
  LEFT JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION (NOLOCK) ST  ON   O.UPSOrderNumber=ST.UpsOrderNumber 
  WHERE O.AccountId = @VarAccountID  
  AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*') 
  AND (O.FacilityId IN (SELECT UPPER (TRIM (value)) FROM string_split(@VarwarehouseId, ','))OR @VarwarehouseId = '*') 
  AND ((O.ScheduledPickUpDateTime            BETWEEN @scheduleToShipStartDateTime    AND @scheduleToShipEndDateTime   ) OR @NULLScheduleToShipDate = '*')  
  AND ((O.DateTimeReceived  BETWEEN @shipmentCreationStartDateTime AND @shipmentCreationEndDateTime)OR @NULLCreatedDate = '*') 
  AND ((ST.LoadLatestDeliveryDate            BETWEEN @scheduledDeliveryStartDateTime AND @scheduledDeliveryEndDateTime) OR @NULLScheduledDeliveryDate = '*') 
  AND (O.OrderCancelledFlag <> 'Y' or O.OrderCancelledFlag IS NULL) 
  AND O.IS_INBOUND = 1 
  AND (@NULLInboundType='*' OR COALESCE(O.IS_ASN,0) = @isASN) 
  AND ISNULL(O.OrderStatusName, '' )<>'Cancelled'  
  AND ISNULL(O.OrderCancelledFlag,'N') <> 'Y'  --------------------------08/11/2021  change added as per confirmation from jimmy 
   
  --CL345
  INSERT INTO #DIGITAL_SUMMARY_ORDERS(ServiceLevel,UPSOrderNumber,OriginCity,DestinationCity,OriginCountry,DestinationCountry,ShipmentMode,milestoneStatus, Carrier, SourceSystemKey,actualDeliveryDateTime)  --11/12/2021
  SELECT  
    ServiceLevel, 
 UPSOrderNumber, 
 --CL345
 OriginCity,
 DestinationCity,
 --CL345
    OriginCountry, 
    DestinationCountry, 
    ShipmentMode, 
    milestoneStatus, 
 Carrier, 
 SourceSystemKey,
 actualDeliveryDateTime
  FROM #DIGITAL_SUMMARY_ORDERS_FILTER O 
  WHERE  ((actualDeliveryDateTime        BETWEEN @actualDeliveryStartDateTime    AND @actualDeliveryEndDateTime   ) OR @NULLActualDeliveryDate = '*') 
   
  END 
   
  ELSE IF @varDateType = 'SHIPMENTDELIVERYDATE'  AND @NULLCreatedDate = '*' 
   
  BEGIN 
   
  --CL345
  INSERT INTO #DIGITAL_SUMMARY_ORDERS_FILTER(DateTypeDate,UPSOrderNumber,ServiceLevel,OriginCity,DestinationCity,OriginCountry,DestinationCountry,ShipmentMode,milestoneStatus,Carrier, SourceSystemKey,actualDeliveryDateTime)  --11/12/2021
  SELECT 
   CAST(O.ActualDeliveryDate as date) as ShipmentDeliveryDate, 
 O.UPSOrderNumber, 
 O.ServiceLevel,
  --CL345
 OriginCity,
 DestinationCity,
 --CL345
    O.OriginCountry, 
    O.DestinationCountry, 
    O.ServiceMode AS ShipmentMode, 
    CurrentMilestone AS milestoneStatus,
    --O.UPSOrderNumber,
 O.Carrier AS Carrier, 
 O.SourceSystemKey AS SourceSystemKey, 
 (SELECT MAX(ma.ActivityDate) FROM Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) ma 
           WHERE O.UPSOrderNumber = ma.UPSOrderNumber 
           AND O.SourceSystemKey = ma.SourceSystemKey 
           AND ma.ActivityCode in('D','D1','D9')
           AND ma.AccountId = @VarAccountID --CL345
 ) AS actualDeliveryDateTime 
  FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O 
  LEFT JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION (NOLOCK) ST  ON   O.UPSOrderNumber=ST.UpsOrderNumber 
  JOIN Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) FMA ON O.UPSOrderNumber = FMA.UPSOrderNumber 
                                                              AND O.SourceSystemKey = FMA.SourceSystemKey 
  JOIN master_data.Map_Milestone_Activity MMA (NOLOCK) ON MMA.ActivityName = FMA.ActivityName 
                                                      AND MMA.SOURCE_SYSTEM_KEY = FMA.SourceSystemKey 
  WHERE O.AccountId = @VarAccountID 
  AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*') 
  AND (O.FacilityId IN (SELECT UPPER (TRIM (value)) FROM string_split(@VarwarehouseId, ',')) OR @VarwarehouseId = '*') 
 -- AND (FMA.ActivityDate  BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime) 
  AND ((O.ScheduledPickUpDateTime            BETWEEN @scheduleToShipStartDateTime    AND @scheduleToShipEndDateTime   ) OR @NULLScheduleToShipDate = '*')  
  AND ((ST.LoadLatestDeliveryDate            BETWEEN @scheduledDeliveryStartDateTime AND @scheduledDeliveryEndDateTime) OR @NULLScheduledDeliveryDate = '*') 
  AND ISNULL(O.OrderCancelledFlag,'N') <> 'Y' 
  AND O.IS_INBOUND=1 
  AND (@NULLInboundType='*' OR COALESCE(O.IS_ASN,0) = @isASN) 
  AND MMA.ActivityCode IN ('D','D1','D9')
  AND FMA.AccountId = @VarAccountID --CL345
 -- GROUP BY O.UPSOrderNumber, 
 --O.ServiceLevel, 
 --   O.OriginCountry, 
 --   O.DestinationCountry, 
 --   O.ServiceMode, 
 --   CurrentMilestone, 
 --O.Carrier, 
 --O.SourceSystemKey 
   
  --CL345
 INSERT INTO #DIGITAL_SUMMARY_ORDERS(DateTypeDate,UPSOrderNumber,ServiceLevel,OriginCity,DestinationCity,OriginCountry,DestinationCountry,ShipmentMode,milestoneStatus,Carrier, SourceSystemKey,actualDeliveryDateTime)  --11/12/2021
 SELECT  
    DateTypeDate, 
 UPSOrderNumber, 
 ServiceLevel, 
   --CL345
 OriginCity,
 DestinationCity,
 --CL345
    OriginCountry, 
    DestinationCountry, 
    ShipmentMode, 
    milestoneStatus, 
 Carrier, 
 SourceSystemKey,
 actualDeliveryDateTime
  FROM #DIGITAL_SUMMARY_ORDERS_FILTER O 
  WHERE  ((actualDeliveryDateTime        BETWEEN @actualDeliveryStartDateTime    AND @actualDeliveryEndDateTime   ) OR @NULLActualDeliveryDate = '*') 
   
  END 
   
   
   
 -- final result set 
     
  SELECT COUNT(1) Total FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) 
  WHERE milestoneStatus IS NOT NULL --09/29/2021
 
  SELECT 
    milestoneStatus, 
    COUNT(milestoneStatus) milestoneStatusCount 
  FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) 
  WHERE milestoneStatus IS NOT NULL 
  GROUP BY milestoneStatus 
   
   
  SELECT 
    ShipmentMode, 
    COUNT(ShipmentMode) AS ShipmentModeCount 
  FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) 
  WHERE ShipmentMode IS NOT NULL 
  GROUP BY ShipmentMode 
   
   
  SELECT 
    OriginCountry, 
    COUNT(OriginCountry) AS OriginCountryCount 
  FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) 
  WHERE OriginCountry IS NOT NULL 
  GROUP BY OriginCountry 
   
   
  SELECT 
    DestinationCountry, 
    COUNT(DestinationCountry) AS DestinationCountryCount 
  FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) 
  WHERE DestinationCountry IS NOT NULL 
  GROUP BY DestinationCountry 
   
   
  SELECT 
    ServiceLevel, 
    COUNT(ServiceLevel) AS serviceLevelCount 
  FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) 
  WHERE ServiceLevel IS NOT NULL 
  GROUP BY ServiceLevel 
   
   
  SELECT 
    Carrier, 
    COUNT(Carrier) CarrierCount 
  FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) 
  WHERE Carrier IS NOT NULL 
  GROUP BY Carrier 
   
  -- NEW RESULT SETS 
   
   
    SELECT COUNT(*) as ScheduleToShipCount FROM [Summary].[DIGITAL_SUMMARY_ORDERS] ORDS (NOLOCK) 
            WHERE ORDS.CurrentMilestone COLLATE SQL_Latin1_General_CP1_CI_AS IN('TRANSPORTATION PLANNING')  --UPSGLD-13511
            AND  ORDS.ScheduledPickUpDateTime BETWEEN @scheduleToShipStartDateTime AND @scheduleToShipEndDateTime 
            AND  ORDS.DateTimeReceived BETWEEN @shipmentCreationStartDateTime AND @shipmentCreationEndDateTime 
            AND ORDS.IS_INBOUND = 1 
            AND ORDS.AccountId = @VarAccountID 
              AND ISNULL(ORDS.OrderStatusName, '')<>'Cancelled'  
                                                 AND ISNULL(ORDS.OrderCancelledFlag,'N') <> 'Y'  --------------------------08/11/2021  change added as per confirmation from jimmy 
   
     
   
   
  SELECT COUNT(*) as MissedPickupCount FROM [Summary].[DIGITAL_SUMMARY_ORDERS] ORDS (NOLOCK) 
            WHERE ORDS.CurrentMilestone COLLATE SQL_Latin1_General_CP1_CI_AS IN('TRANSPORTATION PLANNING')  --UPSGLD-13511
            AND  ORDS.ScheduledPickUpDateTime BETWEEN @scheduleToShipStartDateTime AND @scheduleToShipEndDateTime 
            AND  ORDS.DateTimeReceived BETWEEN @shipmentCreationStartDateTime AND @shipmentCreationEndDateTime 
            AND  ORDS.IS_INBOUND = 1 
            AND ORDS.AccountId = @VarAccountID 
              AND ISNULL(ORDS.OrderStatusName, '')<>'Cancelled'   
                                                 AND ISNULL(ORDS.OrderCancelledFlag,'N') <> 'Y'  --------------------------08/11/2021  change added as per confirmation from jimmy 
   
   
     
   
  SELECT COUNT(*) as ScheduledToDeliverCount FROM [Summary].[DIGITAL_SUMMARY_ORDERS] ORDS (NOLOCK) 
            INNER JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION (NOLOCK) ST  
            ON   ORDS.UPSOrderNumber=ST.UpsOrderNumber 
            AND  ORDS.CurrentMilestone COLLATE SQL_Latin1_General_CP1_CI_AS IN('TRANSPORTATION PLANNING','IN TRANSIT', 'CUSTOMS')  --UPSGLD-13511
            AND  ST.LoadLatestDeliveryDate BETWEEN @scheduledDeliveryStartDateTime AND @scheduledDeliveryEndDateTime 
            AND  ORDS.DateTimeReceived BETWEEN @shipmentCreationStartDateTime AND @shipmentCreationEndDateTime 
            AND  ORDS.IS_INBOUND = 1 
            AND ORDS.AccountId = @VarAccountID 
              AND ISNULL(ORDS.OrderStatusName, '')<>'Cancelled'  
                                                 AND ISNULL(ORDS.OrderCancelledFlag,'N') <> 'Y'  --------------------------08/11/2021  change added as per confirmation from jimmy 
   
   
   
     
   
  SELECT COUNT(*) as MissedDeliveredCount FROM [Summary].[DIGITAL_SUMMARY_ORDERS] ORDS (NOLOCK) 
            INNER JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION (NOLOCK) ST  
            ON   ORDS.UPSOrderNumber=ST.UpsOrderNumber 
            AND  ORDS.CurrentMilestone COLLATE SQL_Latin1_General_CP1_CI_AS IN('TRANSPORTATION PLANNING','IN TRANSIT', 'CUSTOMS')  --UPSGLD-13511
            AND  ST.LoadLatestDeliveryDate BETWEEN @scheduledDeliveryStartDateTime AND @scheduledDeliveryEndDateTime 
            AND  ORDS.DateTimeReceived BETWEEN @shipmentCreationStartDateTime AND @shipmentCreationEndDateTime 
            AND  ORDS.IS_INBOUND = 1 
            AND ORDS.AccountId = @VarAccountID 
              AND ISNULL(ORDS.OrderStatusName, '')<>'Cancelled'  
                                                 AND ISNULL(ORDS.OrderCancelledFlag,'N') <> 'Y'  --------------------------08/11/2021  change added as per confirmation from jimmy 
   
   
     
   
SELECT  
ORD.UPSOrderNumber, 
ORD.SourceSystemKey, 
actualDeliveryDateTime 
INTO #DeliveredShipmentData 
FROM 
#DIGITAL_SUMMARY_ORDERS (NOLOCK) ORD 
--INNER JOIN Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) MA  --11/12/2021
--ON ORD.UPSOrderNumber=MA.UPSOrderNumber 
--AND ORD.SourceSystemKey=MA.SourceSystemKey 
WHERE ORD.milestoneStatus IN ('DELIVERED','ASN CREATED','FTZ','RECEIVING','PUTAWAY') --CL332
--ORD.UPSOrderNumber, 
--ORD.SourceSystemKey 
  
 
  
   
SELECT  
DSD.UPSOrderNumber, 
DSD.SourceSystemKey, 
CASE  WHEN DSD.actualDeliveryDateTime > TRANS.LoadLatestDeliveryDate THEN 'LATE' 
      WHEN CAST(DSD.actualDeliveryDateTime AS DATE) <= CAST(TRANS.LoadLatestDeliveryDate AS DATE)
      --OR (DSD.actualDeliveryDateTime IS NULL OR TRANS.LoadLatestDeliveryDate IS NULL) -- UPSGLD-13321
      THEN 'ONTIME'  
 END  AS DeliveryStatus 
INTO #DeliveredShipmentStatusData 
FROM #DeliveredShipmentData (NOLOCK) DSD 
LEFT JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION (NOLOCK) TRANS 
ON  DSD.UPSOrderNumber=TRANS.UpsOrderNumber 
AND DSD.SourceSystemKey=TRANS.SourceSystemKey 
   
 
SELECT 
ORD.ShipmentMode, 
DSD.DeliveryStatus, 
COUNT(*) AS  ShipmentCount 
FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) ORD 
INNER JOIN #DeliveredShipmentStatusData DSD  
ON ORD.UPSOrderNumber=DSD.UPSOrderNumber AND ORD.SourceSystemKey=DSD.SourceSystemKey 
LEFT JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION (NOLOCK) TRANS 
ON ORD.UPSOrderNumber=TRANS.UpsOrderNumber 
AND ORD.SourceSystemKey=TRANS.SourceSystemKey 
WHERE ORD.milestoneStatus IN ('DELIVERED','ASN CREATED','FTZ','RECEIVING','PUTAWAY')  --CL332
GROUP BY  
 ORD.ShipmentMode 
,DSD.DeliveryStatus 
   
   
   
SELECT COUNT(distinct O.UPSOrderNumber) AS TemperatureShipmentCount, 
O.ShipmentMode AS ShipmentMode  --CL270
FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) O 
INNER JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION_CALLCHECK (NOLOCK) CC  
ON O.UPSOrderNumber=CC.UPSORDERNUMBER 
AND O.SourceSystemKey=CC.SOURCESYSTEMKEY 
WHERE CC.IS_TEMPERATURE='Y' 
AND CC.STATUSDETAILTYPE='TemperatureTracking' 
GROUP BY O.ShipmentMode
   
 
 
--CL345
 
SELECT TOP 5 *
INTO #DeliveryStatusbyCarrier
FROM
(SELECT   
ORD.Carrier,   
--DSD.DeliveryStatus,   
COUNT(*) AS [Count]   
FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) ORD   
INNER JOIN #DeliveredShipmentStatusData (NOLOCK) DSD    
ON ORD.UPSOrderNumber=DSD.UPSOrderNumber AND ORD.SourceSystemKey=DSD.SourceSystemKey 
GROUP BY    
ORD.Carrier   
--,DSD.DeliveryStatus
) CNT
ORDER BY [Count] DESC
 
--  Result Set 13
 
SELECT
ORD.Carrier,   
DSD.DeliveryStatus,   
COUNT(*) AS [Count]
FROM #DeliveryStatusbyCarrier (NOLOCK) L
JOIN #DIGITAL_SUMMARY_ORDERS (NOLOCK) ORD ON L.Carrier = ORD.Carrier
JOIN #DeliveredShipmentStatusData (NOLOCK) DSD ON ORD.UPSOrderNumber=DSD.UPSOrderNumber AND ORD.SourceSystemKey=DSD.SourceSystemKey   
GROUP BY
ORD.Carrier,   
DSD.DeliveryStatus
ORDER BY 
[Count] DESC
--CL345
 
 
--CL345
 
SELECT TOP 5 *
INTO #DeliveryStatusbyLane
FROM
(SELECT   
UPPER(ORD.OriginCity) AS SourceCity,
UPPER(ORD.DestinationCity) AS DestinationCity,
UPPER(ORD.OriginCountry) AS SourceCountry,
UPPER(ORD.DestinationCountry) AS DestinationCountry ,
--DSD.DeliveryStatus,  
COUNT(*) AS  [Count]
FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) ORD   
INNER JOIN #DeliveredShipmentStatusData (NOLOCK) DSD    
ON ORD.UPSOrderNumber=DSD.UPSOrderNumber AND ORD.SourceSystemKey=DSD.SourceSystemKey   
GROUP BY
ORD.OriginCity,
ORD.DestinationCity,
ORD.OriginCountry,
ORD.DestinationCountry
--,DSD.DeliveryStatus
) CNT
WHERE (CNT.SourceCity NOT LIKE '%NOT AVAILABLE%' AND DestinationCity NOT LIKE '%NOT AVAILABLE%')
ORDER BY [Count] DESC
 
--  Result Set 14
 
 
Select
L.SourceCity,
L.DestinationCity,
L.SourceCountry,
L.DestinationCountry,
DSD.DeliveryStatus,  
COUNT(*) AS  [Count]
from #DeliveryStatusbyLane (NOLOCK) L
JOIN #DIGITAL_SUMMARY_ORDERS (NOLOCK) ORD ON UPPER(L.SourceCity) = UPPER(ORD.OriginCity) AND UPPER(L.DestinationCity)= UPPER(ORD.DestinationCity)
JOIN #DeliveredShipmentStatusData (NOLOCK) DSD ON ORD.UPSOrderNumber=DSD.UPSOrderNumber AND ORD.SourceSystemKey=DSD.SourceSystemKey   
GROUP BY
L.SourceCity,
L.DestinationCity,
L.SourceCountry,
L.DestinationCountry
,DSD.DeliveryStatus
ORDER BY 
[Count] DESC
--CL345
 
 
 
END 
GO