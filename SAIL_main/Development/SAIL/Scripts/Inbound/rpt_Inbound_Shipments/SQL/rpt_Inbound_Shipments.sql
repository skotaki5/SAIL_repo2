/****** Object:  StoredProcedure [digital].[rpt_Inbound_Shipments]    Script Date: 2/11/2022 4:00:47 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




              
 /***              
CHANGE LOG              
----------              
CHANGED BY   DATE   SPRINT   CHANGES              

Sheetal			11/11/2021     UPSGLD-12001			To match the count in all result sets updated logic of Distinct shipmentNumber, upsTransportShipmentNumber
Revathy         12/6/2021      44(CL 320)           Logic to correct  claimstatus flag.
Harsha          12/22/2021     CL - 328             Have a mapping for "NULL" shipment mode to UNASSIGNED as value
Sheetal			01/05/2022     CL335			    Include Delivered, ASN Created, FTZ, Receiving, Putaway for Ontime & Late perfromance result set
Revathy         01/05/2022     CL-340               Change returned value from "Transport Order" to "Managed Transportation"
Sheetal			01/07/2022	   UPSGLD-12892			Changed the filter condition in Result set 1 and 2 
SAGAR			03/01/2022	   46TH CL-347			Shipment Service Level Result Set added
Harsha          01/13/2022     UPSGLD-13110			Added Stringsplit logic to handle comma seperated values of param @DeliveryStatus
Sheetal			01/17/2022	   47 CL368				Added isInbound column in Result Set 1
SAGAR			02/04/2022	   48 CL376				Add originLocation and destinationLocation in request
													SP to support "NULL" as CarrierTypeArray value for shipment listing with shipments without carrier information.
SAGAR			02/11/2022							Added missing schema
***/              
  
/****   
--AMR  
EXEC [digital].[rpt_Inbound_Shipments]   @DPProductLineKey = 'E648FA6F-6253-428E-8AC9-201E3EF83B91',@DPServiceLineKey = '53D60776-D3BD-4E6A-8E37-F591C148294B',@DPEntityKey = '',@startDate = '2020-08-08',@endDate = '2020-08-14',@topRow = 1000,@milestoneSta
tus='*',@warehouseId='*'  
--SWR  
EXEC [digital].[rpt_Inbound_Shipments]   @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = '150E5128-B3E3-44A1-BF6C-CD2E4D9856DA',@DPEntityKey = '',@startDate = '2021-06-01',@endDate ='2021-06-09',@topRow = 1000,@milestoneStat
us='*',@warehouseId='*'  
--Cambium  
EXEC [digital].[rpt_Inbound_Shipments]   @DPProductLineKey = 'A10F512B-C7F4-42A0-909C-21DD95A7D921',@DPServiceLineKey = '53DDDBC2-FB5A-4E70-A92E-8312DC9FDD05',@DPEntityKey = '',@startDate = '2020-08-08',@endDate ='2020-08-14',@topRow = 1000,@milestoneStat
us='*',@warehouseId='*' 

EXEC [digital].[rpt_Inbound_Shipments]
@DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',
@DPServiceLineKey = '*',
@DPEntityKey = '*',
@topRow = 0,@milestoneStatus= 'DELIVERED','ASN CREATED','FTZ','RECEIVING','PUTAWAY'',@warehouseId='*', @deliveryStatus = 'LATE',
@Date = '{"shipmentCreationStartDate": "2021-08-10","shipmentCreationEndDate": "2021-11-11"}'
--,@inboundType = 'TRANSPORT ORDER'

EXEC [digital].[rpt_Inbound_Shipments]
@DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',
@DPServiceLineKey = '*',
@DPEntityKey = '*',
@topRow = 0,@milestoneStatus='BOOKED',@warehouseId='*', @deliveryStatus = '*',
@Date = '{"shipmentCreationStartDate": "2021-12-10","shipmentCreationEndDate": "2022-01-02"}',
@isShipmentServiceLevelResultSet='Y',@originLocation = '*' 
,@destinationLocation = '*';


--Updated execution script----
EXEC [digital].[rpt_Inbound_Shipments]
@DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',
@DPServiceLineKey = '*',
@DPEntityKey = '*',
@topRow = 0,@milestoneStatus='*',@warehouseId='*', @deliveryStatus = '*',
@Date = '{"shipmentCreationStartDate": "2021-10-01","shipmentCreationEndDate": "2022-02-01"}',
@isShipmentServiceLevelResultSet='Y',@originLocation ='Innsbruck,AT'  
,@destinationLocation = 'LOUISVILLE,US';​
****/  
  
CREATE PROCEDURE [digital].[rpt_Inbound_Shipments]
  
@DPProductLineKey varchar(50)=null,   
@DPServiceLineKey varchar(50)=null,   
@DPEntityKey varchar(50)=null,  
@startDate date=null,   
@endDate date=null,   
@warehouseId varchar(max),   
@topRow int,  
@inboundType varchar(50)='',  
@milestoneStatus nvarchar(max)='*',  
@Date nvarchar(max) = NULL,  
@originCountry varchar(255)='*',  
@destinationCountry varchar(255)='*',  
@originCity varchar(255)='*',   
@destinationCity varchar(255)='*',  
@deliveryStatus varchar(200)='*',  
@ShipmentModeArray nvarchar(max)='{"ShipmentMode":["*"]}',  
@ServiceLevelArray nvarchar(max)='{"ServiceLevel":["*"]}',  
@CarrierTypeArray varchar(128)='{"CarrierType":["*"]}',  
@isClaim varchar(5) = '*',  
@isTemperatureTracked NVARCHAR(1) = '*', 
@isShipmentServiceLevelResultSet NVARCHAR(1)='', --CL347
@originLocation nvarchar(200) = NULL, --CL376
@destinationLocation nvarchar(200) = NULL --CL376
  
AS  
  
BEGIN  
  
  DECLARE @VarAccountID varchar(50),  
          @VarDPServiceLineKey varchar(50),  
          @VarDPEntityKey varchar(50),  
          --@VarStartCreatedDateTime datetime,  
          --@VarEndCreatedDateTime datetime,  
          @NULLCreatedDate varchar(1),  
          @VarwarehouseId varchar(50),  
          @VarmilestoneStatus varchar(250),  
    @VarInboundType varchar(50),  
    @NULLInboundType varchar(1),  
    @isASN           INT,  
    @NULLMileStoneStatus varchar(1),  
    --@shipmentCreationStartDate date,  
    --@shipmentCreationEndDate date,  
    @shipmentCreationStartDateJSON date,  
    @shipmentCreationEndDateJSON date,  
    @shipmentCreationStartDateTime datetime,  
    @shipmentCreationEndDateTime datetime,  
    @NULLScheduledDeliveryDate varchar(1),  
    @ScheduledDeliveryStartDate date,  
    @ScheduledDeliveryEndDate date,  
    @ScheduledDeliveryStartDateTime datetime,  
    @ScheduledDeliveryEndDateTime datetime,  
    @NULLDeliveryETADate varchar(1),  
    @DeliveryETAStartDate date,  
    @DeliveryETAEndDate date,  
    @DeliveryETAStartDateTime datetime,  
    @DeliveryETAEndDateTime datetime,  
    @VarOriginCountry nvarchar(max),  
    @NULLOriginCountry varchar(1),  
    @VarDestinationCountry nvarchar(max),  
    @NULLDestinationCountry varchar(1),  
    @VarOriginCity nvarchar(max),  
    @NULLOriginCity varchar(1),  
    @VarDestinationCity nvarchar(max),  
    @NULLDestinationCity varchar(1),  
    @NULLShippedDate varchar(1),  
    @shippedStartDate date,  
    @shippedEndDate date,  
    @shippedStartDateTime datetime,  
    @shippedEndDateTime datetime,  
    @NULLActualDeliveryDate varchar(1),  
    @actualDeliveryStartDate date,  
    @actualDeliveryEndDate date,  
    @actualDeliveryStartDateTime datetime,  
    @actualDeliveryEndDateTime datetime,  
    @ScheduleToShipStartDate date,  
    @ScheduleToShipEndDate   date,  
    @ScheduleToShipStartDateTime datetime,  
    @ScheduleToShipEndDateTime   datetime,  
    @NULLScheduleToShipDate VARCHAR(1),  
    @NULLDeliveryStatus char(1),  
    @VardeliveryStatus varchar(200),  
    @VarCarrierTypeArray varchar(max),  
    @VarserviceLevelArray nvarchar(max),  
    @VarshipmentModeArray nvarchar(max),  
    @bookedStartDate date,  
    @bookedEndDate   date,  
    @bookedStartDateTime datetime,  
    @bookedEndDateTime   datetime,  
    @NULLbookedDate varchar(1),  
    @pickupStartDate date,  
    @pickupEndDate   date,  
    @pickupStartDateTime datetime,  
    @pickupEndDateTime   datetime,  
    @NULLpickupDate varchar(1),  
    @estimatedDepartureStartDate date,  
    @estimatedDepartureEndDate   date,  
    @estimatedDepartureStartDateTime datetime,  
    @estimatedDepartureEndDateTime   datetime,  
    @NULLestimatedDepartureDate varchar(1),  
    @estimatedArrivalStartDate date,  
    @estimatedArrivalEndDate   date,  
    @estimatedArrivalStartDateTime datetime,  
    @estimatedArrivalEndDateTime   datetime,  
    @NULLestimatedArrivalDate varchar(1),  
    @VarisTemperatureTracked CHAR(1),
	@VarisShipmentServiceLevelResultSet CHAR(255), --CL347
	@NulloriginLocation nvarchar(200), --CL376
	@NulldestinationLocation nvarchar(200) --CL376
  
  
  
  
IF (@Date <> '*')  
BEGIN  
SELECT    @shipmentCreationStartDateJSON  = shipmentCreationStartDate,  
          @shipmentCreationEndDateJSON    = shipmentCreationEndDate,  
    @ScheduledDeliveryStartDate = scheduleDeliveryStartDate,  
    @ScheduledDeliveryEndDate   = scheduleDeliveryEndDate,  
    @DeliveryETAStartDate       = deliveryEtaStartDate,  
    @DeliveryETAEndDate         = deliveryEtaEndDate,  
    @shippedStartDate           = shippedStartDate,  
    @shippedEndDate             = shippedEndDate,  
    @actualDeliveryStartDate    = actualDeliveryStartDate,  
    @actualDeliveryEndDate      = actualDeliveryEndDate,  
    @ScheduleToShipStartDate    = scheduleToShipStartDate,  
    @ScheduleToShipEndDate      = scheduleToShipEndDate,  
    @bookedStartDate            = bookedStartDate,  
    @bookedEndDate              = bookedEndDate,  
    @pickupStartDate     = pickupStartDate,  
    @pickupEndDate              = pickupEndDate,  
    @estimatedDepartureStartDate=estimatedDepartureStartDate,  
    @estimatedDepartureEndDate  =estimatedDepartureEndDate,  
    @estimatedArrivalStartDate  =estimatedArrivalStartDate,  
    @estimatedArrivalEndDate    =estimatedArrivalEndDate  
FROM OPENJSON(@Date)  
WITH (  
shipmentCreationStartDate date,  
shipmentCreationEndDate date,  
scheduleDeliveryStartDate date,  
scheduleDeliveryEndDate date,  
deliveryEtaStartDate date,  
deliveryEtaEndDate date,  
shippedStartDate date,  
shippedEndDate date,  
actualDeliveryStartDate date,  
actualDeliveryEndDate date,  
scheduleToShipStartDate date,  
scheduleToShipEndDate date,  
bookedStartDate date,  
bookedEndDate date,  
pickupStartDate date,  
pickupEndDate date,  
estimatedDepartureStartDate date,  
estimatedDepartureEndDate date,  
estimatedArrivalStartDate date,  
estimatedArrivalEndDate date  
     )  
  
END  
  
  
  
-----CarrierType  
DROP TABLE IF EXISTS #CarrierTypeARRAY  
  
SELECT  CASE WHEN CarrierTypeARRAY='NULL' THEN 'UNASSIGNED' ELSE CarrierTypeARRAY END AS CarrierTypeARRAY INTO #CarrierTypeARRAY  
     FROM OPENJSON(@CarrierTypeArray)  
     WITH (  
    CarrierType nvarchar(max) 'strict $.CarrierType' AS JSON  
 )  
 OUTER APPLY OPENJSON(CarrierType) WITH (CarrierTypeARRAY NVARCHAR(MAX) '$');  
  
-----SHIPMENTARRAY  
DROP TABLE IF EXISTS #SHIPMENTARRAY  
  
SELECT CASE WHEN SHIPMENTARRAY='NULL' THEN 'UNASSIGNED' ELSE SHIPMENTARRAY END AS SHIPMENTARRAY INTO #SHIPMENTARRAY  
     FROM OPENJSON(@ShipmentModeArray)  
     WITH (  
    ShipmentMode nvarchar(max) 'strict $.ShipmentMode' AS JSON  
 )  
 OUTER APPLY OPENJSON(ShipmentMode) WITH (SHIPMENTARRAY NVARCHAR(MAX) '$');  
  
-----SERVICELEVELARRAY  
DROP TABLE IF EXISTS #SERVICELEVELARRAY  
  
SELECT  CASE WHEN SERVICELEVELARRAY='NULL' THEN 'NOSERVICE' ELSE SERVICELEVELARRAY END AS SERVICELEVELARRAY INTO #SERVICELEVELARRAY  
     FROM OPENJSON(@ServiceLevelArray)  
     WITH (  
    ServiceLevel nvarchar(max) 'strict $.ServiceLevel' AS JSON  
 )  
 OUTER APPLY OPENJSON(ServiceLevel) WITH (SERVICELEVELARRAY NVARCHAR(MAX) '$');  
  
  
  
  SET @VarAccountID = UPPER(@DPProductLineKey)  
  SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)  
  SET @VarDPEntityKey = UPPER(@DPEntityKey)  
  SET @VarwarehouseId = UPPER(@warehouseId)  
  SET @VarisTemperatureTracked=UPPER(@isTemperatureTracked)  
  --SET @VarStartCreatedDateTime = @startDate  
  --SET @VarEndCreatedDateTime = @EndDate  
  --SET @VarEndCreatedDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @VarEndCreatedDateTime)))  
  SET @VarmilestoneStatus = UPPER(@milestoneStatus)  
  SET @VarInboundType=UPPER(@inboundType)  
  SET @VarOriginCountry=UPPER(@originCountry)  
  SET @VarDestinationCountry=UPPER(@destinationCountry)  
  SET @VarOriginCity=UPPER(@originCity)  
  SET @VarDestinationCity=UPPER(@destinationCity)  
  SET @VardeliveryStatus=UPPER(@deliveryStatus) 
  SET @VarisShipmentServiceLevelResultSet=UPPER(@isShipmentServiceLevelResultSet) --CL347
  
  IF @shipmentCreationStartDateJSON IS NOT NULL AND @shipmentCreationEndDateJSON IS NOT NULL  
  and @shipmentCreationStartDateJSON<>'' and @shipmentCreationEndDateJSON<>''  
  BEGIN  
  SET @shipmentCreationStartDateTime=@shipmentCreationStartDateJSON  
  SET @shipmentCreationEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @shipmentCreationEndDateJSON)))  
  END  
  ELSE IF(@startDate IS NOT NULL AND @endDate IS NOT NULL)  
  BEGIN  
  SET @shipmentCreationStartDateTime=@startDate  
  SET @shipmentCreationEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @endDate)))  
  END  
  ELSE  
  BEGIN  
   SET @NULLCreatedDate='*'  
  END  
  
  --SELECT @startDate,@endDate,@shipmentCreationEndDateJSON,@shipmentCreationStartDateTime,@shipmentCreationEndDateTime  
  
  SET @ScheduledDeliveryStartDateTime=@ScheduledDeliveryStartDate  
  SET @ScheduledDeliveryEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @ScheduledDeliveryEndDate)))  
  
  SET @DeliveryETAStartDateTime=@DeliveryETAStartDate  
  SET @DeliveryETAEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @DeliveryETAEndDate)))  
  
  SET @shippedStartDateTime=@shippedStartDate  
  SET @shippedEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @shippedEndDate)))  
  
  SET @actualDeliveryStartDateTime=@actualDeliveryStartDate  
  SET @actualDeliveryEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @actualDeliveryEndDate)))  
  
  SET @ScheduleToShipStartDateTime =@ScheduleToShipStartDate  
  SET @ScheduleToShipEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @ScheduleToShipEndDate)))  
  
  SET @bookedStartDateTime =@bookedStartDate  
  SET @bookedEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @bookedEndDate)))  
  
  SET @pickupStartDateTime =@pickupStartDate  
  SET @pickupEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @pickupEndDate)))  
  
  SET @estimatedDepartureStartDateTime =@estimatedDepartureStartDate  
  SET @estimatedDepartureEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @estimatedDepartureEndDate)))  
  
  SET @estimatedArrivalStartDateTime =@estimatedArrivalStartDate  
  SET @estimatedArrivalEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @estimatedArrivalEndDate)))  
  
  --IF ISNULL(@shipmentCreationStartDate,'')='' OR ISNULL(@shipmentCreationEndDate,'')=''  
  --  SET @NULLCreatedDate = '*'  
    
  IF ISNULL(@ScheduledDeliveryStartDate,'')='' OR ISNULL(@ScheduledDeliveryEndDate,'')=''  
    SET @NULLScheduledDeliveryDate = '*'  
  
  IF ISNULL(@DeliveryETAStartDate,'')='' OR ISNULL(@DeliveryETAEndDate,'')=''  
    SET @NULLDeliveryETADate = '*'  
  
  IF ISNULL(@shippedStartDate,'')='' OR ISNULL(@shippedEndDate,'')=''  
    SET @NULLShippedDate = '*'  
  
  IF ISNULL(@actualDeliveryStartDate,'')='' OR ISNULL(@actualDeliveryEndDate,'')=''  
    SET @NULLActualDeliveryDate = '*'  
  
  IF ISNULL(@ScheduleToShipStartDate,'')='' OR ISNULL(@ScheduleToShipEndDate,'')=''  
    SET @NULLScheduleToShipDate = '*'  
    
  IF ISNULL(@bookedStartDate,'')='' OR ISNULL(@bookedEndDate,'')=''  
    SET @NULLbookedDate = '*'  
  
  IF ISNULL(@pickupStartDate,'')='' OR ISNULL(@pickupEndDate,'')=''  
    SET @NULLpickupDate = '*'  
  
  IF ISNULL(@estimatedDepartureStartDate,'')='' OR ISNULL(@estimatedDepartureEndDate,'')=''  
    SET @NULLestimatedDepartureDate = '*'  
  
  IF ISNULL(@estimatedArrivalStartDate,'')='' OR ISNULL(@estimatedArrivalEndDate,'')=''  
    SET @NULLestimatedArrivalDate = '*'  
  
  
  --IF @VarStartCreatedDateTime IS NULL OR @VarEndCreatedDateTime IS NULL  
  --  SET @NULLCreatedDate = '*'  
  
  IF @DPServiceLineKey IS NULL  
    SET @VarDPServiceLineKey = '*'  
  
  IF @VarMilestoneStatus='' OR ISNULL(@VarMilestoneStatus,'*')='*'  
    SET @NULLMileStoneStatus='*'  
  
  IF @DPEntityKey IS NULL  
    SET @VarDPEntityKey = '*'    
  
  IF @VarInboundType='' OR @VarInboundType IS NULL  
     SET @NULLInboundType = '*'  
  
  ELSE  
   SET @isASN = CASE WHEN @VarInboundType='ASN' THEN 1  
                     WHEN @VarInboundType='TRANSPORT ORDER' THEN 0   
                END  
  
    
    
  IF @VardeliveryStatus='' OR ISNULL(@VardeliveryStatus,'*')='*'  
  SET @NULLDeliveryStatus='*'  
  
  IF @VarOriginCountry='' OR ISNULL(@VarOriginCountry,'*')='*'  
  SET @NULLOriginCountry='*'  
  
  IF @VarDestinationCountry='' OR ISNULL(@VarDestinationCountry,'*')='*'  
  SET @NULLDestinationCountry='*'  
  
  IF @VarOriginCity='' OR ISNULL(@VarOriginCity,'*')='*'  
  SET @NULLOriginCity='*'  
  
  IF @VarDestinationCity='' OR ISNULL(@VarDestinationCity,'*')='*'  
  SET @NULLDestinationCity='*'  
  
  
  IF ISNULL(@VarisTemperatureTracked,'')=''  
  SET @VarisTemperatureTracked = '*'
	
  IF ISNULL(@originLocation,'') = '' or @originLocation = '*'
  SET @NullOriginLocation = '*'

  IF ISNULL(@destinationLocation,'') = '' or @destinationLocation = '*'
  SET @NulldestinationLocation = '*'
  
  
  
  ----------  
IF exists( SELECT * FROM #CarrierTypeARRAY where CarrierTypeArray='*')  
begin  
SET @VarCarrierTypeArray='*'  
end  
IF exists( SELECT * FROM #SHIPMENTARRAY where SHIPMENTARRAY='*')  
begin  
SET @VarshipmentModeArray='*'  
end  
IF exists( SELECT * FROM #SERVICELEVELARRAY where SERVICELEVELARRAY='*')  
begin  
SET @VarserviceLevelArray='*'  
end  
  
--UPSGLD-13110
--Delivery Status 
SELECT UPPER(VALUE) AS DeliveryStatus INTO #TmpDeliveryStatus FROM STRING_SPLIT(@VardeliveryStatus,',') 
--UPSGLD-13110   
  
	SELECT UPPER(VALUE) AS OriginLocation INTO #OrginLocation FROM STRING_SPLIT(@originLocation,'|') -- CL376
	SELECT UPPER(VALUE) AS DestinationLocation INTO #DestinationLocation FROM STRING_SPLIT(@destinationLocation,'|') --CL376  



-- TOTALCHARGE CALCULATION  
  
select   
DISTINCT  O.UPSOrderNumber  ,  
SUM(CAST(P.CHARGE AS DECIMAL(10,2))) as  totalCharge ,  
max(P.CURRENCY_CODE) as totalcurency  
into #totalcharge_Invoice  
FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O  
join dbo.FACT_TRANSPORTATION_RATES_CHARGES(nolock) P on O.UPSOrderNumber=P.UPS_ORDER_NUMBER and O.SourceSystemKey=P.SOURCE_SYSTEM_KEY AND P.CHARGE_LEVEL = 'CUSTOMER_INVOICE'  --Added missing schema 02/11/2022
WHERE O.AccountId = @VarAccountID  
  AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')  
  AND (O.FacilityId COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarwarehouseId, ',')) OR @VarwarehouseId = '*')  
  AND ((O.DateTimeReceived                   BETWEEN @shipmentCreationStartDateTime  AND @shipmentCreationEndDateTime ) OR @NULLCreatedDate = '*')  
  AND ((O.DateTimeShipped                    BETWEEN @shippedStartDateTime           AND @shippedEndDateTime          ) OR @NULLShippedDate = '*')  
  AND ((O.ScheduledPickUpDateTime            BETWEEN @ScheduleToShipStartDateTime    AND @ScheduleToShipEndDateTime   ) OR @NULLScheduleToShipDate = '*')  
  AND (@NULLOriginCountry = '*' OR O.OriginCountry COLLATE SQL_Latin1_General_CP1_CI_AS IN (select (TRIM(value)) from string_split(@VarOriginCountry,',')))  
  AND (@NULLDestinationCountry = '*' OR O.DestinationCountry COLLATE SQL_Latin1_General_CP1_CI_AS IN (select (TRIM(value)) from string_split(@VarDestinationCountry,',')))  
  AND (@NULLOriginCity = '*' OR O.OriginCity COLLATE SQL_Latin1_General_CP1_CI_AS IN (select (TRIM(value)) from string_split(@VarOriginCity,',')))  
  AND (@NULLDestinationCity = '*' OR O.DestinationCity COLLATE SQL_Latin1_General_CP1_CI_AS IN (select (TRIM(value)) from string_split(@VarDestinationCity,',')))  
  AND ISNULL(O.OrderCancelledFlag,'N') <> 'Y'
  --CL376
  AND (CASE WHEN CHARINDEX(',',@originLocation) <> 0 THEN CONCAT(O.OriginCity COLLATE SQL_Latin1_General_CP1_CI_AS,',',O.OriginCountry) ELSE O.OriginCountry END IN (SELECT OriginLocation from #OrginLocation) OR (@NullOriginLocation = '*'))
  AND (CASE WHEN CHARINDEX(',',@destinationLocation) <> 0 THEN CONCAT(O.DestinationCity COLLATE SQL_Latin1_General_CP1_CI_AS,',',O.DestinationCountry) ELSE O.DestinationCountry END in (SELECT DestinationLocation from #DestinationLocation) or (@NulldestinationLocation = '*'))
  --CL376
  --CL328
  AND (@VarshipmentModeArray = '*' OR (ISNULL(O.ServiceMode,'UNASSIGNED') COLLATE SQL_Latin1_General_CP1_CI_AS  IN (SELECT * FROM #SHIPMENTARRAY)))
  --CL328 
  --AND (@VarCarrierTypeArray = '*' OR (ISNULL(O.Carrier,'NOMODE') COLLATE SQL_Latin1_General_CP1_CI_AS  IN (SELECT * FROM #CarrierTypeARRAY)))
  --CL376
  AND (@VarCarrierTypeArray = '*' OR (ISNULL(O.Carrier,'UNASSIGNED') COLLATE SQL_Latin1_General_CP1_CI_AS  IN (SELECT * FROM #CarrierTypeARRAY)))
  --CL376
  AND (@VarserviceLevelArray = '*' OR (ISNULL(O.ServiceLevel,'NOSERVICE') COLLATE SQL_Latin1_General_CP1_CI_AS  IN (SELECT * FROM #SERVICELEVELARRAY)))  
  AND O.OrderStatusName<>'Cancelled'  
   GROUP BY O.UPSOrderNumber  
  
  
  
SELECT   
DISTINCT  O.UPSOrderNumber  ,  
SUM(CAST(P.CHARGE AS DECIMAL(10,2))) as  totalCharge ,  
max(P.CURRENCY_CODE) as totalcurency  
into #totalcharge_customerrates  
FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O  
join dbo.FACT_TRANSPORTATION_RATES_CHARGES(nolock) P on O.UPSOrderNumber=P.UPS_ORDER_NUMBER and O.SourceSystemKey=P.SOURCE_SYSTEM_KEY AND P.CHARGE_LEVEL = 'CUSTOMER_RATES'  --Added missing schema
and P.UPS_ORDER_NUMBER not in (select UPSOrderNumber from #totalcharge_Invoice)  
WHERE O.AccountId = @VarAccountID  
  AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')  
  AND (O.FacilityId COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarwarehouseId, ',')) OR @VarwarehouseId = '*')  
  AND ((O.DateTimeReceived                   BETWEEN @shipmentCreationStartDateTime  AND @shipmentCreationEndDateTime ) OR @NULLCreatedDate = '*')  
  AND ((O.DateTimeShipped                    BETWEEN @shippedStartDateTime           AND @shippedEndDateTime          ) OR @NULLShippedDate = '*')  
  AND ((O.ScheduledPickUpDateTime            BETWEEN @ScheduleToShipStartDateTime    AND @ScheduleToShipEndDateTime   ) OR @NULLScheduleToShipDate = '*')  
  AND (@NULLOriginCountry = '*' OR O.OriginCountry COLLATE SQL_Latin1_General_CP1_CI_AS IN (select (TRIM(value)) from string_split(@VarOriginCountry,',')))  
  AND (@NULLDestinationCountry = '*' OR O.DestinationCountry COLLATE SQL_Latin1_General_CP1_CI_AS IN (select (TRIM(value)) from string_split(@VarDestinationCountry,',')))  
  AND (@NULLOriginCity = '*' OR O.OriginCity COLLATE SQL_Latin1_General_CP1_CI_AS IN (select (TRIM(value)) from string_split(@VarOriginCity,',')))  
  AND (@NULLDestinationCity = '*' OR O.DestinationCity COLLATE SQL_Latin1_General_CP1_CI_AS IN (select (TRIM(value)) from string_split(@VarDestinationCity,',')))  
  AND ISNULL(O.OrderCancelledFlag,'N') <> 'Y'  
  --CL376
  AND (CASE WHEN CHARINDEX(',',@originLocation) <> 0 THEN CONCAT(O.OriginCity COLLATE SQL_Latin1_General_CP1_CI_AS,',',O.OriginCountry) ELSE O.OriginCountry END IN (SELECT OriginLocation from #OrginLocation) OR (@NullOriginLocation = '*')) 
  AND (CASE WHEN CHARINDEX(',',@destinationLocation) <> 0 THEN CONCAT(O.DestinationCity COLLATE SQL_Latin1_General_CP1_CI_AS,',',O.DestinationCountry) ELSE O.DestinationCountry END in (SELECT DestinationLocation from #DestinationLocation) or (@NulldestinationLocation = '*'))
  --CL376
  --CL328
  AND (@VarshipmentModeArray = '*' OR (ISNULL(O.ServiceMode,'UNASSIGNED') COLLATE SQL_Latin1_General_CP1_CI_AS  IN (SELECT * FROM #SHIPMENTARRAY)))  
  --CL328
  --AND (@VarCarrierTypeArray = '*' OR (ISNULL(O.Carrier,'NOMODE') COLLATE SQL_Latin1_General_CP1_CI_AS  IN (SELECT * FROM #CarrierTypeARRAY)))
  --CL376
  AND (@VarCarrierTypeArray = '*' OR (ISNULL(O.Carrier,'UNASSIGNED') COLLATE SQL_Latin1_General_CP1_CI_AS  IN (SELECT * FROM #CarrierTypeARRAY)))
  --CL376
  AND (@VarserviceLevelArray = '*' OR (ISNULL(O.ServiceLevel,'NOSERVICE') COLLATE SQL_Latin1_General_CP1_CI_AS  IN (SELECT * FROM #SERVICELEVELARRAY)))  
  AND O.OrderStatusName<>'Cancelled'  
   GROUP BY O.UPSOrderNumber  
  
  
  
select * into #totalcharge  
from #totalcharge_Invoice  
union all  
select * from #totalcharge_customerrates  
  
  
    
  
  SELECT  
    O.[UPSOrderNumber] shipmentNumber,  
	O.OrderNumber referenceNumber,  
    O.[UPSOrderNumber] upsShipmentNumber,  
    O.[OrderNumber] clientShipmentNumber,  
    O.CustomerPO customerPONumber,  
    O.[UPSOrderNumber] orderNumber,  
    O.UPSTransportShipmentNumber upsTransportShipmentNumber,      
	O.GFF_ShipmentInstanceId gffShipmentInstanceId,  
    O.GFF_ShipmentNumber gffShipmentNumber,  
    O.OriginContactName shipmentOrigin_contactName,  
    O.OriginAddress1 shipmentOrigin_addressLine1,  
    O.OriginAddress2 shipmentOrigin_addressLine2,  
    O.OriginCity shipmentOrigin_city,  
    O.OriginProvince shipmentOrigin_stateProvince,  
    O.OriginPostalCode shipmentOrigin_postalCode,  
    O.OriginCountry shipmentOrigin_country,  
    O.DestinationContactName shipmentDestination_contactName,  
    O.DestinationAddress1 shipmentDestination_addressLine1,  
    O.DestinationAddress2 shipmentDestination_addressLine2,  
    O.DestinationCity shipmentDestination_city,  
    O.DestinationProvince shipmentDestination_stateProvince,  
    O.DestinationPostalcode shipmentDestination_postalCode,  
    O.DestinationCountry shipmentDestination_country,  
    O.OrderType shipmentDescription,  
    O.ServiceMode shipmentService,  
    O.ServiceLevel shipmentServiceLevel,  
    O.[ServiceLevelCode] shipmentServiceLevelCode,  
    O.CarrierCode shipmentCarrierCode,  
    O.Carrier shipmentCarrier,  
    O.[OrderStatusName] inventoryShipmentStatus,  
    O.TRANS_MILESTONE transportationMileStone,  
    O.[ExceptionCode] shipmentPrimaryException,  
	O.ShipmentBookedDate as shipmentBookedOnDateTime,  
    O.DateTimeCancelled shipmentCanceledDateTime,  
    O.[CancelledReasonCode] shipmentCanceledReason,  
    O.[ScheduleShipmentDate] actualShipmentDateTime,  
    O.DateTimeReceived shipmentCreateOnDateTime,  
    O.OriginalScheduledDeliveryDateTime originalScheduledDeliveryDateTime,  
	(SELECT MAX(ma.ActivityDate) FROM Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) ma  
           WHERE O.UPSOrderNumber = ma.UPSOrderNumber  
           AND O.SourceSystemKey = ma.SourceSystemKey  
           AND ma.ActivityCode in('D','D1','D9')   
	)actualDeliveryDateTime,  
    O.FacilityId warehouseId,  
	--(SELECT MAX(ma.ActivityDate) FROM Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) ma  
	--          WHERE O.UPSOrderNumber = ma.UPSOrderNumber  
	--          AND O.SourceSystemKey = 1011  
	--          AND ma.ActivityCode in('BKCO')   
	--)shipmentBookDate,  
	(SELECT MAX(ma.ActivityDate) FROM Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) ma  
           WHERE O.UPSOrderNumber = ma.UPSOrderNumber  
           AND O.SourceSystemKey = 1011  
           AND ma.ActivityCode in('AM','AF','CP')   
	)PickUpDate,  
    O.OrderWarehouse warehouseCode,  
    O.CurrentMilestone milestoneStatus,   
	CASE WHEN O.IS_ASN=1 THEN  'ASN' ELSE 'Managed Transportation' END AS inboundType,  --CL340
	--CASE WHEN O.IS_ASN=1 THEN  'ASN' ELSE 'Transport Order' END AS inboundType, 
	(SELECT MAX(ma.ActivityDate) FROM Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) ma  
           WHERE O.UPSOrderNumber = ma.UPSOrderNumber  
           AND O.SourceSystemKey = ma.SourceSystemKey  
                 AND ma.ActivityCode IN('AG','AB','AA')  
             
	)estimatedDeliveryDateTime,  
	O.LOAD_ID AS LoadID,  
	O.SourceSystemKey,  
	LoadLatestDeliveryDate,  
   '{"carrierShipmentNumber":  '+ JSON_QUERY(  
                    '[' + STUFF(( SELECT DISTINCT ',' + '"' + SOT.TRACKING_NUMBER + '"'   
     FROM Summary.DIGITAL_SUMMARY_ORDER_TRACKING SOT (NOLOCK)   
     WHERE O.UPSOrderNumber = SOT.UPSOrderNumber  
     and isnull(SOT.TRACKING_NUMBER,'')<>''  
                    FOR XML PATH('')),1,1,'') + ']' )+'}' AS carrierShipmentNumber,  
	O.ServiceMode AS serviceMode,  
	O.DateTimeShipped AS actualPickupDateTime,  
    O.ScheduledPickUpDateTime AS scheduledPickupDateTime,  
	CASE WHEN DSTR.UPSOrderNumber IS NULL THEN 'N' ELSE 'Y' END AS ISCLAIM,  
	CASE  WHEN O.ActualDeliveryDateTime > O.OriginalScheduledDeliveryDateTime THEN 'LATE' --DSO.LoadLatestDeliveryDate THEN 'LATE'  
      WHEN CAST(O.ActualDeliveryDateTime AS DATE) < =  CAST(O.OriginalScheduledDeliveryDateTime AS DATE) THEN 'ONTIME'  --CAST(DSO.LoadLatestDeliveryDate AS DATE) THEN 'ONTIME'   
    END  AS deliveryStatus,  
    CC.IS_TEMPERATURE AS isTemperatureTracked,
	O.ServiceLevel AS isShipmentServiceLevelResultSet --CL347
	,CC.LATEST_TEMPERATURE AS latestTemperature  
	,CC.TEMPERATURE_DATETIME AS temperatureDateTime  
	,CC.TEMPERATURE_CITY AS temperatureCity  
	,CC.TEMPERATURE_STATE AS temperatureState  
	,CC.TEMPERATURE_COUNTRY AS temperatureCountry 
	,IS_INBOUND -- CL368
  INTO #DIGITAL_SUMMARY_ORDERS  
  FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O  
  LEFT JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION (NOLOCK) ST  ON   O.UPSOrderNumber=ST.UpsOrderNumber  
  LEFT JOIN [Summary].[DIGITAL_SUMMARY_TRANSPORTATION_REFERENCES] (NOLOCK) DSTR ON  O.UPSOrderNumber=DSTR.UPSOrderNumber  
  AND O.SourceSystemKey=DSTR.SourceSystemKey AND DSTR.ReferenceLevel = 'LoadReference_Claim' 
  AND DSTR.ReferenceType in ('Claim Type','Claim Amount')  -- 44(CL 320) 
  LEFT JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION_CALLCHECK (NOLOCK) CC  
          ON O.UPSTransportShipmentNumber=CC.UPSORDERNUMBER 
    AND O.SourceSystemKey=CC.SOURCESYSTEMKEY  
    AND CC.STATUSDETAILTYPE = 'TemperatureTracking'  
    AND CC.IS_LATEST_TEMPERATURE = 'Y'  
  WHERE O.AccountId = @VarAccountID  
  AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')  
  AND (O.FacilityId IN (SELECT UPPER (TRIM (VALUE)) FROM string_split(@VarwarehouseId, ',')) OR @VarwarehouseId = '*')  
  AND ((O.DateTimeReceived  BETWEEN @shipmentCreationStartDateTime AND @shipmentCreationEndDateTime) OR @NULLCreatedDate = '*')  
  AND ((ST.LoadLatestDeliveryDate            BETWEEN @ScheduledDeliveryStartDateTime AND @ScheduledDeliveryEndDateTime) OR @NULLScheduledDeliveryDate = '*')  
  AND ((O.DateTimeShipped                    BETWEEN @shippedStartDateTime           AND @shippedEndDateTime          ) OR @NULLShippedDate = '*')  
  AND ((O.ScheduledPickUpDateTime            BETWEEN @ScheduleToShipStartDateTime    AND @ScheduleToShipEndDateTime   ) OR @NULLScheduleToShipDate = '*')   
  AND (@NULLOriginCountry = '*' OR O.OriginCountry COLLATE SQL_Latin1_General_CP1_CI_AS =@VarOriginCountry)  
  AND (@NULLDestinationCountry = '*' OR O.DestinationCountry COLLATE SQL_Latin1_General_CP1_CI_AS =@VarDestinationCountry)  
  AND (@NULLOriginCity = '*' OR O.OriginCity COLLATE SQL_Latin1_General_CP1_CI_AS =@VarOriginCity)  
  AND (@NULLDestinationCity = '*' OR O.DestinationCity COLLATE SQL_Latin1_General_CP1_CI_AS =@VarDestinationCity)  
  --AND (@NULLMileStoneStatus = '*' OR O.CurrentMileStone COLLATE SQL_Latin1_General_CP1_CI_AS IN (select (TRIM(value)) from string_split(@VarMilestoneStatus,',')))  
  AND O.IS_INBOUND = 1  
  AND (@NULLInboundType='*' OR COALESCE(O.IS_ASN,0)=@isASN)  
  AND ISNULL(O.OrderCancelledFlag,'N') <> 'Y'
    --CL376
  AND (CASE WHEN CHARINDEX(',',@originLocation) <> 0 THEN CONCAT(O.OriginCity COLLATE SQL_Latin1_General_CP1_CI_AS,',',O.OriginCountry) ELSE O.OriginCountry END IN (SELECT OriginLocation from #OrginLocation) OR (@NullOriginLocation = '*'))
  AND (CASE WHEN CHARINDEX(',',@destinationLocation) <> 0 THEN CONCAT(O.DestinationCity COLLATE SQL_Latin1_General_CP1_CI_AS,',',O.DestinationCountry) ELSE O.DestinationCountry END in (SELECT DestinationLocation from #DestinationLocation) or (@NulldestinationLocation = '*'))
  --CL376
  --CL328
  AND (@VarshipmentModeArray = '*' OR (ISNULL(O.ServiceMode,'UNASSIGNED') COLLATE SQL_Latin1_General_CP1_CI_AS  IN (SELECT * FROM #SHIPMENTARRAY)))  
  --CL328
  --AND (@VarCarrierTypeArray = '*' OR (ISNULL(O.Carrier,'NOMODE') COLLATE SQL_Latin1_General_CP1_CI_AS  IN (SELECT * FROM #CarrierTypeARRAY)))
  --CL376
  AND (@VarCarrierTypeArray = '*' OR (ISNULL(O.Carrier,'UNASSIGNED') COLLATE SQL_Latin1_General_CP1_CI_AS  IN (SELECT * FROM #CarrierTypeARRAY)))
  --CL376
  --AND (@VarserviceLevelArray = '*' OR (ISNULL(O.ServiceLevel,'NOSERVICE') COLLATE SQL_Latin1_General_CP1_CI_AS  IN (SELECT * FROM #SERVICELEVELARRAY)))  
  --AND (@VarisTemperatureTracked='*' OR CC.IS_TEMPERATURE=@VarisTemperatureTracked)
  AND (@VarisTemperatureTracked='*' OR CASE WHEN IS_TEMPERATURE = 'Y' THEN CC.IS_TEMPERATURE ELSE ISNULL(CC.IS_TEMPERATURE,'N') END = @VarisTemperatureTracked) --09/23/2021
  
  
  
  -- DELIVERY STATUS FILTERS  
  
  
    SELECT DSO.*  
  INTO #DIGITAL_SUMMARY_ORDERS_FILTER   
  FROM #DIGITAL_SUMMARY_ORDERS DSO  
  WHERE  ((estimatedDeliveryDateTime  BETWEEN @DeliveryETAStartDateTime AND @DeliveryETAEndDateTime) OR @NULLDeliveryETADate = '*')  
  AND ((actualDeliveryDateTime        BETWEEN @actualDeliveryStartDateTime    AND @actualDeliveryEndDateTime   ) OR @NULLActualDeliveryDate = '*')  
  AND ((PickUpDate                    BETWEEN @pickupStartDateTime            AND @pickupEndDateTime          )  OR @NULLpickupDate = '*')  
  AND ((shipmentBookedOnDateTime              BETWEEN @bookedStartDateTime            AND @bookedEndDateTime          )  OR @NULLbookedDate = '*')  
  AND (@NULLMileStoneStatus = '*' OR DSO.milestoneStatus COLLATE SQL_Latin1_General_CP1_CI_AS IN (select (TRIM(value)) from string_split(@VarMilestoneStatus,',')))  
  AND (@isClaim='*' OR (ISCLAIM=@isClaim))  
  AND (@VarserviceLevelArray = '*' OR (ISNULL(DSO.isShipmentServiceLevelResultSet,'NOSERVICE') COLLATE SQL_Latin1_General_CP1_CI_AS  IN (SELECT * FROM #SERVICELEVELARRAY)))  ---CL347
  
  
    SELECT deliveryStatus AS DeliveryStatus,  --11/11/2021
   COUNT(*) AS DeliveryStatusCount  
   INTO #DELIVERY_STATUS_DETAILS  
   FROM (SELECT DISTINCT shipmentNumber, upsTransportShipmentNumber,deliveryStatus
   FROM #DIGITAL_SUMMARY_ORDERS  
   WHERE milestoneStatus IN ('DELIVERED','ASN CREATED','FTZ','RECEIVING','PUTAWAY')  ) A         --CL335
   GROUP BY deliveryStatus  
  
  
   --SELECT *   
   --INTO #DIGITAL_SUMMARY_ORDERS_FINAL  
   --FROM #DIGITAL_SUMMARY_ORDERS_FILTER  
   --WHERE  ((deliveryStatus= @VardeliveryStatus) OR @NULLDeliveryStatus = '*')  
   
  
  
  
 -- LAST KNOWN LOCATION FILTER  
  
  
  
 SELECT   
 MA.UPSOrderNumber  
,MA.SourceSystemKey   
,MAX(ActivityDate) AS ActivityDate   
,MAX(MilestoneOrder) AS MilestoneOrder  
  
INTO #MAX_ACTIVITY  
FROM #DIGITAL_SUMMARY_ORDERS_FILTER  ORD   --final  
INNER JOIN Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (nolock) MA  
ON ORD.upsShipmentNumber=MA.UPSOrderNumber  
AND ORD.SourceSystemKey=MA.SourceSystemKey  
WHERE MA.CurrentMilestoneFlag = 'Y'  
GROUP BY MA.UPSOrderNumber,MA.SourceSystemKey  
  
  
   
SELECT    
      MA.UPSOrderNumber  
                
      INTO #DELIVEREDSHIPMENTS   
                  FROM Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) MA  
      INNER JOIN #MAX_ACTIVITY tmpA ON tmpA.UPSOrderNumber=MA.UPSOrderNumber  
            AND tmpA.SourceSystemKey=MA.SourceSystemKey  
             WHERE  
            -- MA.UPSOrderNumber=@upsShipmentNumber and    
             MA.ActivityCode  in ('D1','DELIVER')  
  
  
  
  
SELECT DISTINCT  
      MA.UPSOrderNumber,  
      MA.SourceSystemKey,   
      MA.ACTIVITY_NOTES,
	  MA.ActivityDate  
      INTO #LAST_LOCATION   
      FROM Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) MA        
	  INNER JOIN #MAX_ACTIVITY tmpA ON tmpA.UPSOrderNumber=MA.UPSOrderNumber  
      AND tmpA.SourceSystemKey=MA.SourceSystemKey  
      --AND tmpA.ActivityDate=MA.ActivityDate  
      -- AND tmpA.MilestoneOrder = MA.MilestoneOrder  
      WHERE   
      --MA.UPSOrderNumber=@upsShipmentNumber  
      --AND MA.CurrentMilestoneFlag = 'Y'   and   
      MA.ACTIVITY_NOTES is not null  
      and MA.ActivityCode not in ('AB','E')  
      AND MA.UPSOrderNumber NOT IN (SELECT * FROM #DELIVEREDSHIPMENTS)  
      order by MA.ActivityDate desc  
  
SELECT * ,ROW_NUMBER()OVER(PARTITION BY UPSOrderNumber ORDER BY ActivityDate DESC) AS RA  INTO #LAST_LOCATION_FILTERED FROM #LAST_LOCATION  
  
  
  
  
  
  -- FINAL OUTPUT  
  
  --SELECT COUNT(1) totalShipments FROM #DIGITAL_SUMMARY_ORDERS  


--Result Set 1

  
    SELECT COUNT(1) totalShipments FROM #DIGITAL_SUMMARY_ORDERS_FILTER 
	--where (@NULLDeliveryStatus='*') or (milestoneStatus <> 'DELIVERED' or (milestoneStatus IN ('DELIVERED','ASN CREATED','FTZ','RECEIVING','PUTAWAY') and DeliveryStatus=@VardeliveryStatus))	
  	where (@NULLDeliveryStatus='*') or (milestoneStatus IN ('DELIVERED','ASN CREATED','FTZ','RECEIVING','PUTAWAY') and DeliveryStatus IN ( select DeliveryStatus from #TmpDeliveryStatus ))	--UPSGLD-12892,--UPSGLD-13110

--Result Set 2


  IF @topRow = 0  
  
    SELECT  DISTINCT
      shipmentNumber,  
	  referenceNumber,  
	  upsShipmentNumber,  
      clientShipmentNumber,  
      customerPONumber,  
      orderNumber,  
      upsTransportShipmentNumber,      
	  gffShipmentInstanceId,  
      gffShipmentNumber,  
      shipmentOrigin_contactName,  
      shipmentOrigin_addressLine1,  
      shipmentOrigin_addressLine2,  
      shipmentOrigin_city,  
      shipmentOrigin_stateProvince,  
      shipmentOrigin_postalCode,  
      shipmentOrigin_country,  
      shipmentDestination_contactName,  
      shipmentDestination_addressLine1,  
      shipmentDestination_addressLine2,  
      shipmentDestination_city,  
      shipmentDestination_stateProvince,  
      shipmentDestination_postalCode,  
      shipmentDestination_country,  
      shipmentDescription,  
      shipmentService,  
      shipmentServiceLevel,  
      shipmentServiceLevelCode,  
      shipmentCarrierCode,  
      shipmentCarrier,  
      inventoryShipmentStatus,  
      transportationMileStone,  
      shipmentPrimaryException,  
      shipmentBookedOnDateTime,  
      shipmentCanceledDateTime,  
      shipmentCanceledReason,  
      actualShipmentDateTime,  
      shipmentCreateOnDateTime,  
      originalScheduledDeliveryDateTime,  
      actualDeliveryDateTime,  
      warehouseId,  
      warehouseCode,  
      milestoneStatus,   
	  inboundType,  
      estimatedDeliveryDateTime,  
      LoadID,  
      (select totalCharge from #totalcharge  P WHERE P.UPSOrderNumber=O.orderNumber) as totalCharge,  
      (select totalcurency from #totalcharge  P WHERE P.UPSOrderNumber=O.orderNumber) as totalChargeCurrency,  
      -- Summary.usp_Get_Customer_Charges_Summary(O.orderNumber, O.SourceSystemKey,'CHARGE','CURRENCY')  as totalChargeCurrency,  
      ISCLAIM as isClaim,  
      deliveryStatus,  
      carrierShipmentNumber,  
      MA.ACTIVITY_NOTES        AS  lastKnownLocation,  
      serviceMode,  
      O.PickUpDate as actualPickupDateTime,  
      scheduledPickupDateTime,  
      isTemperatureTracked,
      isShipmentServiceLevelResultSet, --CL347
      latestTemperature,  
      temperatureDateTime,  
      temperatureCity,  
      temperatureState,  
      temperatureCountry,  
      O.IS_INBOUND AS isInbound--CL368
  
    FROM #DIGITAL_SUMMARY_ORDERS_FILTER O  
  
 LEFT JOIN #LAST_LOCATION_FILTERED MA ON O.shipmentNumber=MA.UPSOrderNumber  
      AND O.SourceSystemKey=MA.SourceSystemKey  AND RA=1  
   --where (@NULLDeliveryStatus='*' or DeliveryStatus=@VardeliveryStatus )
  -- where (@NULLDeliveryStatus='*') or (milestoneStatus <> 'DELIVERED' or (milestoneStatus IN ('DELIVERED','ASN CREATED','FTZ','RECEIVING','PUTAWAY') and DeliveryStatus=@VardeliveryStatus))
    where (@NULLDeliveryStatus='*') or (milestoneStatus IN ('DELIVERED','ASN CREATED','FTZ','RECEIVING','PUTAWAY') and DeliveryStatus IN ( select DeliveryStatus from #TmpDeliveryStatus ))	--UPSGLD-12892,--UPSGLD-13110


    ORDER BY shipmentCreateOnDateTime DESC  
  ELSE  
    SELECT DISTINCT TOP (@topRow)  
      shipmentNumber,  
      referenceNumber,  
      upsShipmentNumber,  
      clientShipmentNumber,  
      customerPONumber,  
      orderNumber,  
      upsTransportShipmentNumber,      
      gffShipmentInstanceId,  
      gffShipmentNumber,  
      shipmentOrigin_contactName,  
      shipmentOrigin_addressLine1,  
      shipmentOrigin_addressLine2,  
      shipmentOrigin_city,  
      shipmentOrigin_stateProvince,  
      shipmentOrigin_postalCode,  
      shipmentOrigin_country,  
      shipmentDestination_contactName,  
      shipmentDestination_addressLine1,  
      shipmentDestination_addressLine2,  
      shipmentDestination_city,  
      shipmentDestination_stateProvince,  
      shipmentDestination_postalCode,  
      shipmentDestination_country,  
      shipmentDescription,  
      shipmentService,  
      shipmentServiceLevel,  
      shipmentServiceLevelCode,  
      shipmentCarrierCode,  
      shipmentCarrier,  
      inventoryShipmentStatus,  
      transportationMileStone,  
      shipmentPrimaryException,  
      shipmentBookedOnDateTime,  
      shipmentCanceledDateTime,  
      shipmentCanceledReason,  
      actualShipmentDateTime,  
      shipmentCreateOnDateTime,  
      originalScheduledDeliveryDateTime,  
      actualDeliveryDateTime,  
      warehouseId,  
      warehouseCode,  
      milestoneStatus,   
      inboundType,  
      estimatedDeliveryDateTime,  
      LoadID,  
      (select totalCharge from #totalcharge  P WHERE P.UPSOrderNumber=O.orderNumber) as totalCharge,  
      (select totalcurency from #totalcharge  P WHERE P.UPSOrderNumber=O.orderNumber) as totalChargeCurrency,  
      -- Summary.usp_Get_Customer_Charges_Summary(O.orderNumber, O.SourceSystemKey,'CHARGE','CURRENCY')  as totalChargeCurrency,  
      ISCLAIM as isClaim,  
      deliveryStatus,  
      carrierShipmentNumber,  
      MA.ACTIVITY_NOTES        AS  lastKnownLocation,  
      serviceMode,  
      --actualPickupDateTime,  
      O.PickUpDate as actualPickupDateTime,  
      scheduledPickupDateTime,  
      isTemperatureTracked, 
      isShipmentServiceLevelResultSet, --CL347
      latestTemperature,  
      temperatureDateTime,  
      temperatureCity,  
      temperatureState,  
      temperatureCountry ,
	  O.IS_INBOUND AS isInbound --CL368
  
    FROM #DIGITAL_SUMMARY_ORDERS_FILTER O  
  
 LEFT JOIN #LAST_LOCATION_FILTERED MA ON O.shipmentNumber=MA.UPSOrderNumber  
      AND O.SourceSystemKey=MA.SourceSystemKey  AND RA=1  
   --where (@NULLDeliveryStatus='*' or DeliveryStatus=@VardeliveryStatus )
  -- where (@NULLDeliveryStatus='*') or (milestoneStatus <> 'DELIVERED' or (milestoneStatus IN ('DELIVERED','ASN CREATED','FTZ','RECEIVING','PUTAWAY') and DeliveryStatus=@VardeliveryStatus))
    	where (@NULLDeliveryStatus='*') or (milestoneStatus IN ('DELIVERED','ASN CREATED','FTZ','RECEIVING','PUTAWAY') and DeliveryStatus IN ( select DeliveryStatus from #TmpDeliveryStatus ))	--UPSGLD-12892,--UPSGLD-13110

    ORDER BY shipmentCreateOnDateTime DESC  
  
  
 -- RESULT SET 3 
   
  SELECT    
  milestoneStatus as  MilestoneStatus ,  --11/11/2021
  COUNT(milestoneStatus)  AS MilestoneStatusCount  
  FROM (SELECT DISTINCT  shipmentNumber, upsTransportShipmentNumber  ,milestoneStatus 
     FROM #DIGITAL_SUMMARY_ORDERS )A
	 --FROM #DIGITAL_SUMMARY_ORDERS_FILTER --09/23/2021 
  --where (@NULLDeliveryStatus='*' or DeliveryStatus=@VardeliveryStatus )  
  GROUP BY milestoneStatus 
  
  
 -- RESULT SET 4  
   
 SELECT * FROM #DELIVERY_STATUS_DETAILS  
 --where (@NULLDeliveryStatus='*' or DeliveryStatus=@VardeliveryStatus )  


 --RESULT SET 5

 --Select 'ShipmentServiceLevel' AS ShipmentServiceLevel

 -- RESULT SET 5

  --CL347
		
 IF @isShipmentServiceLevelResultSet='Y' 

 BEGIN
 
  SELECT shipmentServiceLevel,
   COUNT(*) AS shipmentServiceLevelCount FROM (
   SELECT
   DISTINCT shipmentNumber, upsTransportShipmentNumber,shipmentServiceLevel
    --FROM #DIGITAL_SUMMARY_ORDERS_FILTER ) A  
	FROM #DIGITAL_SUMMARY_ORDERS ) A  
   GROUP BY shipmentServiceLevel
 END

 ELSE
 
 SELECT '' AS ShipmentServiceLevel, '' shipmentServiceLevelCount

 --CL347

END  
GO

