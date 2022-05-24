/****** Object:  StoredProcedure [digital].[rpt_Outbound_Shipment_Summary]    Script Date: 4/7/2022 10:30:50 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO








      
    
    
    
  /**    
CHANGE LOG    
---------    
DEVELOPER			DATE			SPRINT								COMMENTS    
CHAITANYA KHOT      09/30/2021      38th                    TEMPERATURESHIPMENTCOUNT COLUMN IS ADDED    
Sheetal				10/05/2021      39th					1)Added UPSTransportShipmentNumber column in #DIGITAL_SUMMARY_ORDERS  
															2)Added Sipment mode column in result set as TemperatureShipmentCount 
Sheetal				11/1/2021		UPSGLD-8781				Added count of Distinct UPSOrderNumber, upsTransportShipmentNumber combination to match the record with SP digital.rpt_Outbound_Shipments.  									
Sheetal				01/17/2022		47th CL370				Added Shells for 3 new result sets  
Harsha				01/21/2022      CL 370					New result set will have following columns - Carrier, DeliveryStatus, Count
									CL 370					New result set will have following columns - SourceCountry, SourceCity, DestinationCountry,DestinationCity, DeliveryStatus, Count
									CL 370					New result set will have following columns - ShipmentMode, DeliveryStatus, Count
SHALAKA				1/31/2022		UPSGLD-13525			Change logic to get distinct count
VENKATA				02/12/2022		UPSGLD-13520			Made changes to fix the count mismatch in the drilldown
Harsha              02/13/2022                              Adding table alias wherver required
Sheetal				02/14/2022		CL384					Created Shells
REVATHY             02/23/2022      CL384                   Added Datatype,Date as input and New resultset  
Revathy             03/09/2022      CL410                   Created shells for ordertype filter
Avinash             03/11/2022      Sprint51, CL410         Added OrderType filter Input parameter
Surya				03/24/2022		Sprint 52, CL 447		Added shell for temperatureThreshold
Surya				03/25/2022		Sprint 52, CL 447		Added logic for temperatureThreshold column

**/    
    
/****       
--AMR      
EXEC [digital].[rpt_Outbound_Shipment_Summary]   @DPProductLineKey = 'E648FA6F-6253-428E-8AC9-201E3EF83B91',@DPServiceLineKey = '53D60776-D3BD-4E6A-8E37-F591C148294B',@DPEntityKey = NULL,@StartCreatedDate = '2020-08-15',@EndCreatedDate = '2020-08-21', @warehouseId = '*',@DateType=''      
--SWR      
EXEC [digital].[rpt_Outbound_Shipment_Summary]   @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = '150E5128-B3E3-44A1-BF6C-CD2E4D9856DA',@DPEntityKey = NULL,@StartCreatedDate = '2020-08-15',@EndCreatedDate = '2021-01-21', @warehouseId = '*',@DateType=''      
--Cambium      
EXEC [digital].[rpt_Outbound_Shipment_Summary]   @DPProductLineKey = 'A10F512B-C7F4-42A0-909C-21DD95A7D921',@DPServiceLineKey = '53DDDBC2-FB5A-4E70-A92E-8312DC9FDD05',@DPEntityKey = NULL,@StartCreatedDate = '2020-08-27',@EndCreatedDate = '2020-09-02', @warehouseId = '*',@DateType=''      

EXEC [digital].[rpt_Outbound_Shipment_Summary]      
@DPProductLineKey ='870561E1-A974-483B-AA0D-A724C5D402C9', @DPServiceLineKey ='150E5128-B3E3-44A1-BF6C-CD2E4D9856DA',
@DPEntityKey =null, @AccountKeys  = NULL,      
@StartCreatedDate  = '2021-10-15', @EndCreatedDate  = '2022-01-21', @warehouseId ='*', @DateType  ='' ,@Debug  = 0

EXEC [digital].[rpt_Outbound_Shipment_Summary]
@AccountKeys ='[{"DPProductLineKey": "20394995-0871-48AE-A2D0-962CFA4BB1C1","DPServiceLineKey": "0E08F8A3-CC54-4E08-8D02-03022875B500"}]',
@StartCreatedDate = '2022-02-04',
@EndCreatedDate = '2022-02-10',
@warehouseId = '*',@DateType='shipmentCreationDate'

EXEC [digital].[rpt_Outbound_Shipment_Summary]
@AccountKeys ='[{"DPProductLineKey": "20394995-0871-48AE-A2D0-962CFA4BB1C1","DPServiceLineKey": "0E08F8A3-CC54-4E08-8D02-03022875B500"}]',
@Date='{"shipmentCreationStartDate": "2021-12-03","shipmentCreationEndDate": "2022-01-31"}',
@warehouseId = '*',@DateType='shipmentCreationDate',@OrderType = NULL 


EXEC [digital].[rpt_Outbound_Shipment_Summary]
@AccountKeys ='[{"DPProductLineKey": "20394995-0871-48AE-A2D0-962CFA4BB1C1","DPServiceLineKey": "0E08F8A3-CC54-4E08-8D02-03022875B500"}]',
@Date='{"shipmentshippedStartDate": "2021-12-03","shipmentshippedStartDate": "2022-01-31"}',
@warehouseId = '*',@DateType='SHIPMENTSHIPPEDDATE',@OrderType = '*' 

EXEC [digital].[rpt_Outbound_Shipment_Summary]
@AccountKeys ='[{"DPProductLineKey": "870561E1-A974-483B-AA0D-A724C5D402C9","DPServiceLineKey": "*"}]',
@Date='{"shipmentshippedStartDate": "2022-01-01","shipmentshippedStartDate": "2022-03-10"}',
@warehouseId = '*',@DateType='SHIPMENTCREATIONDATE',@OrderType = ''
****/      
      

            


CREATE PROCEDURE [digital].[rpt_Outbound_Shipment_Summary]      
@DPProductLineKey varchar(50)=null
, @DPServiceLineKey varchar(50)=null
, @DPEntityKey varchar(50)=null
, @AccountKeys nvarchar(max) = NULL     
, @StartCreatedDate date = NULL
, @EndCreatedDate date = NULL
, @warehouseId varchar(max)
, @DateType varchar(50)
, @Debug INT = 0 
,@Date nvarchar(max) = NULL
,@IsManaged Varchar(10) = NULL
,@OrderType varchar(max) = '*' --CL410
      
AS      
      
BEGIN      
      
  DECLARE @VarAccountID varchar(50),      
          @VarDPServiceLineKey varchar(50),      
          @VarDPEntityKey varchar(50),      
          @VarStartCreatedDateTime datetime,      
          @VarEndCreatedDateTime datetime,      
          @NULLCreatedDate varchar(1),      
          @VarwarehouseId varchar(max),      
    @VarDPServiceLineKeyJSON VARCHAR(500),      
    @VarDPProductLineKeyJSON VARCHAR(500),      
    @Starttime DATETIME,      
    @EndTime DATETIME,
	--CL384
	 @varDateType     varchar(50),
	@shipmentCreationStartDate date,  
    @shipmentCreationEndDate date,  
    @ScheduledDeliveryStartDate date,  
    @ScheduledDeliveryEndDate date,  
    @ScheduleToShipStartDate date,  
    @ScheduleToShipEndDate   date,  
    @actualDeliveryStartDate date,  
    @actualDeliveryEndDate date,  
	@shipmentshippedStartDate date,
	@shipmentshippedENDDate date,
    @shipmentCreationStartDateTime datetime,  
    @shipmentCreationEndDateTime datetime,  
    @ScheduledDeliveryStartDateTime datetime,  
    @ScheduledDeliveryEndDateTime datetime,  
    @ScheduleToShipStartDateTime datetime,  
    @ScheduleToShipEndDateTime   datetime,  
    @actualDeliveryStartDateTime datetime,  
    @actualDeliveryEndDateTime datetime,  
	@shipmentshippedStartDatetime datetime,
	@shipmentshippedENDDatetime datetime,
	@NULLShipmentCreatedDate varchar(1),  
    @NULLScheduleToShipDate VARCHAR(1),  
    @NULLActualDeliveryDate varchar(1),  
    @NULLScheduledDeliveryDate varchar(1),
	@NULLshipmentshippedDate varchar(1),
    @VarIsManaged VARCHAR(10),
	@NULLIsManaged VARCHAR(10),
	@varOrderType varchar(max), --CL410
    @NullOrderType varchar(max) --CL410


    SELECT  @shipmentCreationStartDate = shipmentCreationStartDate,  
          @shipmentCreationEndDate   = shipmentCreationEndDate,  
    @ScheduledDeliveryStartDate = scheduleDeliveryStartDate,   
    @ScheduledDeliveryEndDate   = scheduleDeliveryEndDate,  
    @ScheduleToShipStartDate    = scheduleToShipStartDate,  
    @ScheduleToShipEndDate      = scheduleToShipEndDate,  
    @actualDeliveryStartDate    = actualDeliveryStartDate,  
    @actualDeliveryEndDate      = actualDeliveryEndDate ,
	@shipmentshippedStartDate = shipmentshippedStartDate,
	@shipmentshippedENDDate = shipmentshippedENDDate

  FROM OPENJSON(@Date)  
  WITH (  
 shipmentCreationStartDate date,  
 shipmentCreationEndDate date,  
 scheduleDeliveryStartDate date,  
 scheduleDeliveryEndDate date,  
 scheduleToShipStartDate date,  
 scheduleToShipEndDate date,  
 actualDeliveryStartDate date,  
 actualDeliveryEndDate date, 
 shipmentshippedStartDate date,
 shipmentshippedENDDate date
       )  
      
  SET @VarAccountID = UPPER(@DPProductLineKey)      
  SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)      
  SET @VarDPEntityKey = UPPER(@DPEntityKey)      
  SET @VarwarehouseId = UPPER(@warehouseId)      
  SET @VarStartCreatedDateTime=@StartCreatedDate      
  SET @VarEndCreatedDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @EndCreatedDate)))      
  SET @Starttime = GETDATE() 
  SET @varDateType=UPPER(@DateType) 
  SET @varIsManaged = CASE WHEN UPPER(@IsManaged) = 'Y' THEN '1' 
						   WHEN UPPER(@IsManaged) = 'N' THEN '0' ELSE @IsManaged END ----CL384
  SET @varOrderType = UPPER(@OrderType)   --CL410

      
  SELECT UPPER(DPProductLineKey) AS DPProductLineKey,      
       UPPER(DPServiceLineKey) AS DPServiceLineKey      
    into #ACCOUNTINFO      
    FROM OPENJSON(@AccountKeys)      
    WITH(      
   DPProductLineKey VARCHAR(MAX),      
   DPServiceLineKey VARCHAR(MAX)      
    )      

    IF @Debug>0      
  BEGIN      
  SET @EndTime = GETDATE()      
  SELECT '#ACCOUNTINFO Insert',@Starttime,@EndTime,DATEDIFF(SS,@Starttime,@EndTime) AS TimeTakeninsecs      
  SET @Starttime = GETDATE()      
  END     

  SET @ScheduledDeliveryStartDateTime=@ScheduledDeliveryStartDate  
  SET @ScheduledDeliveryEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @ScheduledDeliveryEndDate)))  
  
  SET @ScheduleToShipStartDateTime =@ScheduleToShipStartDate  
  SET @ScheduleToShipEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @ScheduleToShipEndDate)))  
  
  SET @actualDeliveryStartDateTime =@actualDeliveryStartDate  
  SET @actualDeliveryEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @actualDeliveryEndDate)))  

   SET @shipmentshippedStartDatetime =@shipmentshippedStartDate  
  SET @shipmentshippedENDDatetime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @shipmentshippedENDDate)))

   IF ISNULL(@shipmentCreationStartDate,'')<>'' AND ISNULL(@shipmentCreationEndDate,'')<>''
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
  
  IF ISNULL(@ScheduleToShipStartDate,'')='' OR ISNULL(@ScheduleToShipEndDate,'')=''  
    SET @NULLScheduleToShipDate = '*'  
  
 IF ISNULL(@actualDeliveryStartDate,'')='' OR ISNULL(@actualDeliveryEndDate,'')=''  
    SET @NULLActualDeliveryDate = '*'  
  
 IF ISNULL(@ScheduledDeliveryStartDate,'')='' OR ISNULL(@ScheduledDeliveryEndDate,'')=''  
    SET @NULLScheduledDeliveryDate = '*'  

IF ISNULL(@shipmentshippedStartDatetime,'')='' OR ISNULL(@shipmentshippedENDDate,'')=''  
    SET @NULLshipmentshippedDate = '*'  
      
  IF @DPServiceLineKey IS NULL      
    SET @VarDPServiceLineKey = '*'      
      
  IF @DPEntityKey IS NULL      
    SET @VarDPEntityKey = '*'   
	
	IF Isnull(@DateType,'')=''  
	begin   
	if @NULLCreatedDate = '*'  
	begin  
	set  @varDateType = 'SHIPMENTSHIPPEDDATE'   
	end  
	if @NULLshipmentshippedDate = '*'  
	begin  
	set @varDateType = 'SHIPMENTCREATIONDATE'    
	end  
	end  

	IF Isnull(@Date,'') = '' and UPPER(@DateType)='SHIPMENTCREATIONDATE'  
	begin  
	set @shipmentCreationStartDateTime=@StartCreatedDate  
	set @shipmentCreationEndDateTime=DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @EndCreatedDate)))  
	set @NULLCreatedDate = ''  
	end  
	else IF  Isnull(@Date,'') = '' and UPPER(@DateType)='SHIPMENTSHIPPEDDATE'  
	begin  
	set @shipmentshippedStartDatetime=@StartCreatedDate  
	set @shipmentshippedENDDate=DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @EndCreatedDate)))  
	set @NULLshipmentshippedDate = ''  
	end  
      
	IF ISNULL(@varIsManaged,'')='' OR @varIsManaged = '*'--CL387
	SET @NULLIsManaged = '*'

	--CL410 
	IF @varOrderType = '*' 
	SET @NullOrderType = '*'
	--CL410

	--CL410
	IF Object_id('tempdb..#OrderType') IS NOT NULL 
		DROP TABLE #OrderType
	SELECT
	  Upper(value) AS orderType
	INTO   #OrderType
	FROM   String_split(@varOrderType, ',')
	--CL410

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

	--CL384
	
CREATE TABLE #DIGITAL_SUMMARY_ORDERS_FILTER(ServiceLevel VARCHAR(255),OriginCountry NVARCHAR(255),      
                                     DestinationCountry NVARCHAR(255), OriginCity NVARCHAR(255), DestinationCity NVARCHAR(255), Carrier NVARCHAR(255),actualDeliveryDateTime datetime,OriginalScheduledDeliveryDateTime datetime,--CL370
									 ShipmentMode VARCHAR(128),      
          milestoneStatus VARCHAR(40), UPSOrderNumber VARCHAR(120),      
          SourceSystemKey INT,UPSTransportShipmentNumber VARCHAR(255),TemperatureThreshold varchar(255))  
      
CREATE TABLE #DIGITAL_SUMMARY_ORDERS(ServiceLevel VARCHAR(255),OriginCountry NVARCHAR(255),      
                                     DestinationCountry NVARCHAR(255), OriginCity NVARCHAR(255), DestinationCity NVARCHAR(255), Carrier NVARCHAR(255),actualDeliveryDateTime datetime,OriginalScheduledDeliveryDateTime datetime,--CL370
									 ShipmentMode VARCHAR(128),      
          milestoneStatus VARCHAR(40), UPSOrderNumber VARCHAR(120),      
          SourceSystemKey INT,UPSTransportShipmentNumber VARCHAR(255),TemperatureThreshold varchar(255))  -- 10/05/2021    

	IF @varDateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*'  
	  
	BEGIN  

	INSERT INTO #DIGITAL_SUMMARY_ORDERS_FILTER(ServiceLevel,OriginCountry,DestinationCountry,OriginCity,DestinationCity,Carrier,actualDeliveryDateTime,OriginalScheduledDeliveryDateTime,ShipmentMode,milestoneStatus,UPSOrderNumber,  SourceSystemKey,UPSTransportShipmentNumber,TemperatureThreshold)  --11/12/2021

	 SELECT      
    O.ServiceLevel,      
    O.OriginCountry,      
    O.DestinationCountry,    
	--CL370
	O.OriginCity,
	O.DestinationCity,
	O.Carrier,
	/** --UPSGLD-13520
	 (SELECT MAX(ma.ActivityDate) FROM Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) ma  
           WHERE O.UPSOrderNumber = ma.UPSOrderNumber  
           AND O.SourceSystemKey = ma.SourceSystemKey  
           AND ma.ActivityCode in('D','D1','D9')  
		   AND ma.AccountId = @VarAccountID --CL345
 ) AS actualDeliveryDateTime , 
 **/
 
 O.ActualDeliveryDateTime, --UPSGLD-13520
 O.OriginalScheduledDeliveryDateTime,
	--CL370
    O.ServiceMode AS ShipmentMode,      
    O.CurrentMilestone AS milestoneStatus,      
 O.UPSOrderNumber AS UPSOrderNumber,      
 O.SourceSystemKey AS SourceSystemKey   
 , O.UPSTransportShipmentNumber        --10/5/2021  
 , O.TemperatureThreshold				-- UPSGLD 15173
  --INTO #DIGITAL_SUMMARY_ORDERS      
  FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O  
  LEFT JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION (NOLOCK) ST  ON ( CASE
                     WHEN ST.TrasOnlyFlag <> 'TRANS ONLY' THEN Isnull(ST.UpsWMSSourceSystemKey, O.SourceSystemKey)
                     ELSE
                       ST.SourceSystemKey
                   END = O.SourceSystemKey
                   AND CASE
                         WHEN ST.TrasOnlyFlag <> 'TRANS ONLY' THEN ST.UpsWMSOrderNumber
                         ELSE
                           ST.UpsOrderNumber
                       END = O.UPSOrderNumber )  
  WHERE  ( O.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR O.AccountId = @VarAccountID )       
  AND ((O.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR O.DP_SERVICELINE_KEY = @VarDPServiceLineKey)  OR @VarDPServiceLineKey = '*')      
  AND (O.FacilityId COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarwarehouseId, ','))OR @VarwarehouseId = '*')   
  AND ((O.ScheduledPickUpDateTime            BETWEEN @ScheduleToShipStartDateTime    AND @ScheduleToShipEndDateTime   ) OR @NULLScheduleToShipDate = '*')   
	AND ((O.DateTimeReceived  BETWEEN @shipmentCreationStartDateTime AND @shipmentCreationEndDateTime)OR @NULLCreatedDate = '*')  
	AND ((ST.LoadLatestDeliveryDate            BETWEEN @ScheduledDeliveryStartDateTime AND @ScheduledDeliveryEndDateTime) OR @NULLScheduledDeliveryDate = '*')  
  --AND ((O.DateTimeReceived  BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime)OR @NULLCreatedDate = '*')      
  AND O.IS_INBOUND = 0      
  --AND COALESCE(O.OrderCancelledFlag,'N') <> 'Y'      
  AND ISNULL(O.OrderCancelledFlag,'N') <> 'Y'  
  AND ((CAST(O.IS_MANAGED AS VARCHAR(10)) = @varIsManaged) OR @NULLIsManaged = '*')
  AND (UPPER(ISNULL(O.OrderType,'')) in (Select orderType from #OrderType) or @NullOrderType = '*' ) --CL410

   INSERT INTO #DIGITAL_SUMMARY_ORDERS (ServiceLevel,OriginCountry,DestinationCountry,OriginCity,DestinationCity,Carrier,actualDeliveryDateTime,OriginalScheduledDeliveryDateTime,ShipmentMode,milestoneStatus,UPSOrderNumber,  SourceSystemKey,UPSTransportShipmentNumber,TemperatureThreshold)  --11/12/2021
   select ServiceLevel,OriginCountry,DestinationCountry,OriginCity,DestinationCity,Carrier,actualDeliveryDateTime,OriginalScheduledDeliveryDateTime,ShipmentMode,milestoneStatus,UPSOrderNumber,  SourceSystemKey,UPSTransportShipmentNumber,TemperatureThreshold
   FROM #DIGITAL_SUMMARY_ORDERS_FILTER O  
  WHERE  ((actualDeliveryDateTime        BETWEEN @actualDeliveryStartDateTime    AND @actualDeliveryEndDateTime   ) OR @NULLActualDeliveryDate = '*')  
	  
	  END    
	  
	   ELSE IF @varDateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*'  
	  
	BEGIN  

	INSERT INTO #DIGITAL_SUMMARY_ORDERS_FILTER(ServiceLevel,OriginCountry,DestinationCountry,OriginCity,DestinationCity,Carrier,actualDeliveryDateTime,OriginalScheduledDeliveryDateTime,ShipmentMode,milestoneStatus,UPSOrderNumber,  SourceSystemKey,UPSTransportShipmentNumber)  --11/12/2021

	 SELECT      
    O.ServiceLevel,      
    O.OriginCountry,      
    O.DestinationCountry,    
	--CL370
	O.OriginCity,
	O.DestinationCity,
	O.Carrier,
	
	/* (SELECT MAX(ma.ActivityDate) FROM Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) ma  
           WHERE O.UPSOrderNumber = ma.UPSOrderNumber  
           AND O.SourceSystemKey = ma.SourceSystemKey  
           AND ma.ActivityCode in('D','D1','D9')  
		   AND ma.AccountId = @VarAccountID --CL345
 ) AS actualDeliveryDateTime ,*/
 O.ActualDeliveryDateTime, --UPSGLD-13520
 O.OriginalScheduledDeliveryDateTime,
	--CL370
    O.ServiceMode AS ShipmentMode,      
    O.CurrentMilestone AS milestoneStatus,      
 O.UPSOrderNumber AS UPSOrderNumber,      
 O.SourceSystemKey AS SourceSystemKey   
 , O.UPSTransportShipmentNumber        --10/5/2021  
  --INTO #DIGITAL_SUMMARY_ORDERS      
  FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O  
  LEFT JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION (NOLOCK) ST    ON ( CASE
                     WHEN ST.TrasOnlyFlag <> 'TRANS ONLY' THEN Isnull(ST.UpsWMSSourceSystemKey, O.SourceSystemKey)
                     ELSE
                       ST.SourceSystemKey
                   END = O.SourceSystemKey
                   AND CASE
                         WHEN ST.TrasOnlyFlag <> 'TRANS ONLY' THEN ST.UpsWMSOrderNumber
                         ELSE
                           ST.UpsOrderNumber
                       END = O.UPSOrderNumber )  
  WHERE  ( O.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR O.AccountId = @VarAccountID )       
  AND ((O.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR O.DP_SERVICELINE_KEY = @VarDPServiceLineKey)  OR @VarDPServiceLineKey = '*')      
  AND (O.FacilityId COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarwarehouseId, ','))OR @VarwarehouseId = '*')   
  AND ((O.ScheduledPickUpDateTime            BETWEEN @ScheduleToShipStartDateTime    AND @ScheduleToShipEndDateTime   ) OR @NULLScheduleToShipDate = '*')   
	AND ((O.ActualShipmentDateTime  BETWEEN @shipmentshippedStartDatetime AND @shipmentshippedENDDatetime)OR @NULLshipmentshippedDate = '*')  
	AND ((ST.LoadLatestDeliveryDate            BETWEEN @ScheduledDeliveryStartDateTime AND @ScheduledDeliveryEndDateTime) OR @NULLScheduledDeliveryDate = '*')  
  --AND ((O.DateTimeReceived  BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime)OR @NULLCreatedDate = '*')      
  AND O.IS_INBOUND = 0      
  --AND COALESCE(O.OrderCancelledFlag,'N') <> 'Y'      
  AND ISNULL(O.OrderCancelledFlag,'N') <> 'Y'   
  AND ((CAST(O.IS_MANAGED AS VARCHAR(10)) = @varIsManaged) OR @NULLIsManaged = '*') 
  AND (UPPER(ISNULL(O.OrderType,'')) in (Select orderType from #OrderType) or @NullOrderType = '*' )  --CL410   

   INSERT INTO #DIGITAL_SUMMARY_ORDERS(ServiceLevel,OriginCountry,DestinationCountry,OriginCity,DestinationCity,Carrier,actualDeliveryDateTime,OriginalScheduledDeliveryDateTime,ShipmentMode,milestoneStatus,UPSOrderNumber,  SourceSystemKey,UPSTransportShipmentNumber)  --11/12/2021
   select ServiceLevel,OriginCountry,DestinationCountry,OriginCity,DestinationCity,Carrier,actualDeliveryDateTime,OriginalScheduledDeliveryDateTime,ShipmentMode,milestoneStatus,UPSOrderNumber,  SourceSystemKey,UPSTransportShipmentNumber
   FROM #DIGITAL_SUMMARY_ORDERS_FILTER O  
  WHERE  ((actualDeliveryDateTime        BETWEEN @actualDeliveryStartDateTime    AND @actualDeliveryEndDateTime   ) OR @NULLActualDeliveryDate = '*')  
	  
	  END  
                 
      
  IF @Debug>0      
  BEGIN      
  SET @EndTime = GETDATE()      
  SELECT '#digital_summary_orders',@Starttime,@EndTime,DATEDIFF(SS,@Starttime,@EndTime) AS TimeTakeninsecs      
  SET @Starttime = GETDATE()      
  END      
      
Select Count(*) AS Total From (
  SELECT  DISTINCT UPSOrderNumber, upsTransportShipmentNumber  FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) ) A --11/1/2021
  
      
  SELECT        --11/1/2021
    milestoneStatus,        
    COUNT(milestoneStatus) milestoneStatusCount 
	FROM(
	SELECT 
DISTINCT
UPSOrderNumber,
upsTransportShipmentNumber,
milestoneStatus 
  FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK)        
  WHERE milestoneStatus IS NOT NULL  ) A      
  GROUP BY milestoneStatus    
      
      
 SELECT ShipmentMode, COUNT(ShipmentMode) AS ShipmentModeCount --11/1/2021
 FROM (
SELECT 
DISTINCT
UPSOrderNumber,
upsTransportShipmentNumber,
ShipmentMode       
  FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK)        
  WHERE ShipmentMode IS NOT NULL )   A    
  GROUP BY ShipmentMode       
      
      
  SELECT        --11/1/2021
    OriginCountry,        
    COUNT(OriginCountry) AS OriginCountryCount 
FROM (
SELECT 
DISTINCT
UPSOrderNumber,
upsTransportShipmentNumber,
OriginCountry   
  FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK)        
  WHERE OriginCountry IS NOT NULL  ) A      
  GROUP BY OriginCountry      
      
      
 SELECT        --11/1/2021
    DestinationCountry,        
    COUNT(DestinationCountry) AS DestinationCountryCount  
	FROM (
SELECT 
DISTINCT
UPSOrderNumber,
upsTransportShipmentNumber,
DestinationCountry 
  FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK)        
  WHERE DestinationCountry IS NOT NULL ) A       
  GROUP BY DestinationCountry     
      
      
  SELECT        --11/1/2021
    ServiceLevel,        
    COUNT(ServiceLevel) AS serviceLevelCount   
	FROM ( SELECT
DISTINCT
UPSOrderNumber,
upsTransportShipmentNumber,
ServiceLevel 
  FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK)        
  WHERE ServiceLevel IS NOT NULL  ) A      
  GROUP BY ServiceLevel      
      
      
      
  SELECT 
  COUNT(distinct UPSOrderNumber) AS TemperatureShipmentCount,      --11/1/2021
 ShipmentMode    --10/05/2021 
 FROM (
 SELECT DISTINCT O.UPSOrderNumber, O.upsTransportShipmentNumber,O.ShipmentMode 
 FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) O      
INNER JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION_CALLCHECK (NOLOCK) CC       
ON O.UPSTransportShipmentNumber=CC.UPSORDERNUMBER      
--AND O.SourceSystemKey=CC.SOURCESYSTEMKEY      
WHERE CC.IS_TEMPERATURE='Y'      
AND CC.STATUSDETAILTYPE='TemperatureTracking' ) A     
GROUP BY ShipmentMode  
      
   
    
 --CL370 
SELECT   
ORD.UPSOrderNumber,  
ORD.SourceSystemKey,  
ORD.actualDeliveryDateTime,
ORD.ShipmentMode, --UPSGLD-13520
ORD.Carrier, --UPSGLD-13520
ORD.UPSTransportShipmentNumber, --UPSGLD-13520
 CASE WHEN ORD.milestoneStatus='DELIVERED' THEN   ---UPSGLD-13375
 CASE  WHEN ORD.actualDeliveryDateTime >  ORD.originalScheduledDeliveryDateTime THEN 'LATE'  -- DSO.LoadLatestDeliveryDate THEN 'LATE'                
        WHEN CAST(ORD.actualDeliveryDateTime AS DATE) < = CAST(ORD.originalScheduledDeliveryDateTime AS DATE) 
		OR (CAST(ORD.actualDeliveryDateTime AS DATE) IS NULL OR CAST(ORD.originalScheduledDeliveryDateTime AS DATE) IS NULL) --Adding the condition as part of CL349
		THEN 'ONTIME' -- CAST(DSO.LoadLatestDeliveryDate AS DATE) THEN 'ONTIME' 
        END 
		ELSE NULL END ---UPSGLD-13375
		AS DeliveryStatus 
INTO #DeliveredShipmentStatusData  
FROM  
#DIGITAL_SUMMARY_ORDERS (NOLOCK) ORD  
WHERE ORD.milestoneStatus = 'DELIVERED'




SELECT TOP 5 *
INTO #DeliveryStatusbyCarrier
FROM (
SELECT A.Carrier, COUNT(*) AS [Count] FROM 
(SELECT DISTINCT    
ORD.Carrier,  
ORD.UPSOrderNumber,
ORD.UPSTransportShipmentNumber
--DSD.DeliveryStatus,    
FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) ORD    
INNER JOIN #DeliveredShipmentStatusData (NOLOCK) DSD     
ON ORD.UPSOrderNumber = DSD.UPSOrderNumber AND ORD.SourceSystemKey = DSD.SourceSystemKey)  A
GROUP BY     
A.Carrier 
--,DSD.DeliveryStatus
) CNT
ORDER BY [Count] DESC


SELECT 
ORD.Carrier,    
DSD.DeliveryStatus,    
COUNT(*) AS [Count] 
FROM #DeliveryStatusbyCarrier (NOLOCK) L
JOIN #DIGITAL_SUMMARY_ORDERS (NOLOCK) ORD ON L.Carrier = ORD.Carrier 
JOIN #DeliveredShipmentStatusData (NOLOCK) DSD ON ORD.UPSOrderNumber=DSD.UPSOrderNumber AND ORD.SourceSystemKey = DSD.SourceSystemKey
AND ORD.Carrier = DSD.Carrier --UPSGLD-13520
GROUP BY 
ORD.Carrier,    
DSD.DeliveryStatus



--CL370

--CL370 

SELECT TOP 5 * 
INTO #DeliveryStatusbyLane
FROM (
SELECT    
A.SourceCity  ,
A.DestinationCity,
A.SourceCountry ,
A.DestinationCountry,
--DSD.DeliveryStatus,    
COUNT(*) AS  [Count] FROM
(SELECT DISTINCT   
UPPER(ORD.OriginCity) AS SourceCity,
UPPER(ORD.DestinationCity) AS DestinationCity,
UPPER(ORD.OriginCountry) AS SourceCountry,
UPPER(ORD.DestinationCountry) AS DestinationCountry,
--DSD.DeliveryStatus,    
ORD.UPSOrderNumber,
ORD.UPSTransportShipmentNumber
FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) ORD    
INNER JOIN #DeliveredShipmentStatusData (NOLOCK) DSD     
ON ORD.UPSOrderNumber=DSD.UPSOrderNumber AND ORD.SourceSystemKey=DSD.SourceSystemKey
) A   
GROUP BY 
A.SourceCity,
A.DestinationCity,
A.SourceCountry,
A.DestinationCountry  
--,DSD.DeliveryStatus
) CNT
WHERE (CNT.SourceCity NOT LIKE '%NOT AVAILABLE%' AND CNT.DestinationCity NOT LIKE '%NOT AVAILABLE%') 
ORDER BY [Count] DESC



Select 
L.SourceCity,
L.DestinationCity,
L.SourceCountry ,
L.DestinationCountry,
DSD.DeliveryStatus,   
COUNT(*) AS  [Count] 
from #DeliveryStatusbyLane (NOLOCK) L
JOIN #DIGITAL_SUMMARY_ORDERS (NOLOCK) ORD ON UPPER(L.SourceCity) = UPPER(ORD.OriginCity) AND UPPER(L.DestinationCity)= UPPER(ORD.DestinationCity)
JOIN #DeliveredShipmentStatusData (NOLOCK) DSD ON ORD.UPSOrderNumber = DSD.UPSOrderNumber AND ORD.SourceSystemKey=DSD.SourceSystemKey
AND ORD.UPSTransportShipmentNumber = DSD.UPSTransportShipmentNumber --UPSGLD-13520
GROUP BY 
L.SourceCity,
L.DestinationCity,
L.SourceCountry ,
L.DestinationCountry
,DSD.DeliveryStatus





--CL370

--CL370
SELECT    
A.ShipmentMode,    
A.DeliveryStatus,    
COUNT(*) AS [Count] FROM(
SELECT distinct ORD.ShipmentMode,DSD.DeliveryStatus,ORD.UPSOrderNumber,ORD.UPSTransportShipmentNumber  --UPSGLD-13525 
FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) ORD    
INNER JOIN #DeliveredShipmentStatusData (NOLOCK) DSD     
ON ORD.UPSOrderNumber = DSD.UPSOrderNumber AND ORD.SourceSystemKey=DSD.SourceSystemKey
AND ORD.ShipmentMode = DSD.ShipmentMode   --UPSGLD-13520
)A 
GROUP BY     
A.ShipmentMode,    
A.DeliveryStatus
ORDER BY [Count] DESC
--CL370

--CL384
 SELECT    
    Carrier,    
    COUNT(Carrier) CarrierCount
  FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK)    
  WHERE Carrier IS NOT NULL    
  GROUP BY Carrier 

   IF @shipmentCreationStartDateTime IS NOT NULL    
  BEGIN
  SELECT COUNT(*) as ScheduleToShipCount FROM [Summary].[DIGITAL_SUMMARY_ORDERS] ORDS (NOLOCK)  
	          WHERE ORDS.CurrentMilestone COLLATE SQL_Latin1_General_CP1_CI_AS IN('TRANSPORTATION PLANNING')  --UPSGLD-13511
	          AND ((ORDS.ScheduledPickUpDateTime BETWEEN @ScheduleToShipStartDateTime  AND @ScheduleToShipEndDateTime   ) OR @NULLScheduleToShipDate = '*')   
	          AND  ORDS.DateTimeReceived BETWEEN @shipmentCreationStartDateTime AND @shipmentCreationEndDateTime  
	          AND ORDS.IS_INBOUND = 0
	          AND  ( ORDS.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR ORDS.AccountId = @VarAccountID )    
	           AND ISNULL(ORDS.OrderStatusName, '')<>'Cancelled'   
	          AND ISNULL(ORDS.OrderCancelledFlag,'N') <> 'Y'



 SELECT COUNT(*)as MissedPickupCount FROM [Summary].[DIGITAL_SUMMARY_ORDERS] ORDS (NOLOCK)    
            WHERE ORDS.CurrentMilestone COLLATE SQL_Latin1_General_CP1_CI_AS IN('TRANSPORTATION PLANNING')   --UPSGLD-13511 
             AND ((ORDS.ScheduledPickUpDateTime BETWEEN @ScheduleToShipStartDateTime  AND @ScheduleToShipEndDateTime   ) OR @NULLScheduleToShipDate = '*')   
		    AND  ORDS.DateTimeReceived BETWEEN @shipmentCreationStartDateTime AND @shipmentCreationEndDateTime    
            AND  ORDS.IS_INBOUND = 0  
            AND  ( ORDS.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR ORDS.AccountId = @VarAccountID )    
            AND ISNULL(ORDS.OrderStatusName, '')<>'Cancelled'   
	        AND ISNULL(ORDS.OrderCancelledFlag,'N') <> 'Y'

	SELECT COUNT(*) as ScheduledToDeliverCount FROM [Summary].[DIGITAL_SUMMARY_ORDERS] ORDS (NOLOCK)  
	          INNER JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION (NOLOCK) ST   
	         ON ( CASE
                     WHEN ST.TrasOnlyFlag <> 'TRANS ONLY' THEN Isnull(ST.UpsWMSSourceSystemKey, ORDS.SourceSystemKey)
                     ELSE
                       ST.SourceSystemKey
                   END = ORDS.SourceSystemKey
                   AND CASE
                         WHEN ST.TrasOnlyFlag <> 'TRANS ONLY' THEN ST.UpsWMSOrderNumber
                         ELSE
                           ST.UpsOrderNumber
                       END = ORDS.UPSOrderNumber )   
	          AND  ORDS.CurrentMilestone COLLATE SQL_Latin1_General_CP1_CI_AS IN('TRANSPORTATION PLANNING','IN TRANSIT', 'CUSTOMS')  --UPSGLD-13511
	          AND ((ST.LoadLatestDeliveryDate  BETWEEN @ScheduledDeliveryStartDateTime AND @ScheduledDeliveryEndDateTime) OR @NULLScheduledDeliveryDate = '*')  
	          AND  ORDS.DateTimeReceived BETWEEN @shipmentCreationStartDateTime AND @shipmentCreationEndDateTime  
	          AND  ORDS.IS_INBOUND = 0 
	         AND  ( ORDS.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR ORDS.AccountId = @VarAccountID )    
	          AND ISNULL(ORDS.OrderStatusName, '')<>'Cancelled'   
	          AND ISNULL(ORDS.OrderCancelledFlag,'N') <> 'Y'

			  SELECT COUNT(*) as MissedDeliveredCount FROM [Summary].[DIGITAL_SUMMARY_ORDERS] ORDS (NOLOCK)    
            INNER JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION (NOLOCK) ST     
              ON ( CASE
                     WHEN ST.TrasOnlyFlag <> 'TRANS ONLY' THEN Isnull(ST.UpsWMSSourceSystemKey, ORDS.SourceSystemKey)
                     ELSE
                       ST.SourceSystemKey
                   END = ORDS.SourceSystemKey
                   AND CASE
                         WHEN ST.TrasOnlyFlag <> 'TRANS ONLY' THEN ST.UpsWMSOrderNumber
                         ELSE
                           ST.UpsOrderNumber
                       END = ORDS.UPSOrderNumber )     
            AND  ORDS.CurrentMilestone COLLATE SQL_Latin1_General_CP1_CI_AS IN('TRANSPORTATION PLANNING','IN TRANSIT', 'CUSTOMS')    --UPSGLD-13511
            AND ((ST.LoadLatestDeliveryDate  BETWEEN @ScheduledDeliveryStartDateTime AND @ScheduledDeliveryEndDateTime) OR @NULLScheduledDeliveryDate = '*')  
            AND  ORDS.DateTimeReceived BETWEEN @shipmentCreationStartDateTime AND @shipmentCreationEndDateTime    
		       AND  ORDS.IS_INBOUND = 0   
             AND  ( ORDS.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR ORDS.AccountId = @VarAccountID )    
             AND ISNULL(ORDS.OrderStatusName, '')<>'Cancelled'    
             AND ISNULL(ORDS.OrderCancelledFlag,'N') <> 'Y' 
	END
	ELSE 
	BEGIN
	SELECT COUNT(*) as ScheduleToShipCount FROM [Summary].[DIGITAL_SUMMARY_ORDERS] ORDS (NOLOCK)  
	          WHERE ORDS.CurrentMilestone COLLATE SQL_Latin1_General_CP1_CI_AS IN('TRANSPORTATION PLANNING')  --UPSGLD-13511
	          AND ((ORDS.ScheduledPickUpDateTime BETWEEN @ScheduleToShipStartDateTime  AND @ScheduleToShipEndDateTime   ) OR @NULLScheduleToShipDate = '*')   
	          AND ORDS.ActualShipmentDateTime  BETWEEN @shipmentshippedStartDatetime AND @shipmentshippedENDDatetime
	          AND ORDS.IS_INBOUND = 0
	          AND  ( ORDS.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR ORDS.AccountId = @VarAccountID )    
	           AND ISNULL(ORDS.OrderStatusName, '')<>'Cancelled'   
	          AND ISNULL(ORDS.OrderCancelledFlag,'N') <> 'Y'



 SELECT COUNT(*)as MissedPickupCount FROM [Summary].[DIGITAL_SUMMARY_ORDERS] ORDS (NOLOCK)    
            WHERE ORDS.CurrentMilestone COLLATE SQL_Latin1_General_CP1_CI_AS IN('TRANSPORTATION PLANNING')   --UPSGLD-13511 
             AND ((ORDS.ScheduledPickUpDateTime BETWEEN @ScheduleToShipStartDateTime  AND @ScheduleToShipEndDateTime   ) OR @NULLScheduleToShipDate = '*')   
		    AND ORDS.ActualShipmentDateTime  BETWEEN @shipmentshippedStartDatetime AND @shipmentshippedENDDatetime
            AND  ORDS.IS_INBOUND = 0  
            AND  ( ORDS.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR ORDS.AccountId = @VarAccountID )    
            AND ISNULL(ORDS.OrderStatusName, '')<>'Cancelled'   
	        AND ISNULL(ORDS.OrderCancelledFlag,'N') <> 'Y'

	SELECT COUNT(*) as ScheduledToDeliverCount FROM [Summary].[DIGITAL_SUMMARY_ORDERS] ORDS (NOLOCK)  
	          INNER JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION (NOLOCK) ST   
	            ON ( CASE
                     WHEN ST.TrasOnlyFlag <> 'TRANS ONLY' THEN Isnull(ST.UpsWMSSourceSystemKey, ORDS.SourceSystemKey)
                     ELSE
                       ST.SourceSystemKey
                   END = ORDS.SourceSystemKey
                   AND CASE
                         WHEN ST.TrasOnlyFlag <> 'TRANS ONLY' THEN ST.UpsWMSOrderNumber
                         ELSE
                           ST.UpsOrderNumber
                       END = ORDS.UPSOrderNumber )       
	          AND  ORDS.CurrentMilestone COLLATE SQL_Latin1_General_CP1_CI_AS IN('TRANSPORTATION PLANNING','IN TRANSIT', 'CUSTOMS')  --UPSGLD-13511
	          AND ((ST.LoadLatestDeliveryDate  BETWEEN @ScheduledDeliveryStartDateTime AND @ScheduledDeliveryEndDateTime) OR @NULLScheduledDeliveryDate = '*')  
	          AND ORDS.ActualShipmentDateTime  BETWEEN @shipmentshippedStartDatetime AND @shipmentshippedENDDatetime
	          AND  ORDS.IS_INBOUND = 0 
	         AND  ( ORDS.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR ORDS.AccountId = @VarAccountID )    
	          AND ISNULL(ORDS.OrderStatusName, '')<>'Cancelled'   
	          AND ISNULL(ORDS.OrderCancelledFlag,'N') <> 'Y'

			  SELECT COUNT(*) as MissedDeliveredCount FROM [Summary].[DIGITAL_SUMMARY_ORDERS] ORDS (NOLOCK)    
            INNER JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION (NOLOCK) ST     
              ON ( CASE
                     WHEN ST.TrasOnlyFlag <> 'TRANS ONLY' THEN Isnull(ST.UpsWMSSourceSystemKey, ORDS.SourceSystemKey)
                     ELSE
                       ST.SourceSystemKey
                   END = ORDS.SourceSystemKey
                   AND CASE
                         WHEN ST.TrasOnlyFlag <> 'TRANS ONLY' THEN ST.UpsWMSOrderNumber
                         ELSE
                           ST.UpsOrderNumber
                       END = ORDS.UPSOrderNumber )     
            AND  ORDS.CurrentMilestone COLLATE SQL_Latin1_General_CP1_CI_AS IN('TRANSPORTATION PLANNING','IN TRANSIT', 'CUSTOMS')    --UPSGLD-13511
            AND ((ST.LoadLatestDeliveryDate  BETWEEN @ScheduledDeliveryStartDateTime AND @ScheduledDeliveryEndDateTime) OR @NULLScheduledDeliveryDate = '*')  
            AND ORDS.ActualShipmentDateTime  BETWEEN @shipmentshippedStartDatetime AND @shipmentshippedENDDatetime
		       AND  ORDS.IS_INBOUND = 0   
             AND  ( ORDS.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR ORDS.AccountId = @VarAccountID )    
             AND ISNULL(ORDS.OrderStatusName, '')<>'Cancelled'    
             AND ISNULL(ORDS.OrderCancelledFlag,'N') <> 'Y' 
	END

	--CL384
 SELECT    
    carrier,    
    COUNT(Carrier) [count]  ,
	temperatureThreshold
  FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK)    
  WHERE Carrier IS NOT NULL    
  GROUP BY Carrier ,TemperatureThreshold

END 



GO

