/****** Object:  StoredProcedure [digital].[rpt_Outbound_Shipments]    Script Date: 3/29/2022 5:49:51 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO











/***              
CHANGE LOG              
----------              
CHANGED BY			DATE		  SPRINT			CHANGES              
ARUP				10/22/2021		41				Add latestTemperatureInCelsius and latestTemperatureInFahrenheit            
Arup				10/11/2021						Added Last know Location            
PRASHANT RAI		09/30/2021		38th			TEMPERATURE RELATED COLUMNS ARE ADDED            
Venkata/Sheetal		10/08/2021		40(CL282)		Added lastKnownLocation in #Filertable and final result set            
Arup				N/A				39th(CL275)		Add totalcharge, totalcurrency and totalchargecurreny            
Venkata				10/18/2021		UPSGLD-11400	Added ActivityDate column, used activity date column in order by clause to populate the last known location            
Sheetal				10/22/2021		CL275			Changed logic to get TotalCharge,Changed column alias to get charge and currency in final result      
Sheetal				10/26/2021						Changed the logic to get IsClaim value result  
Venkata				10/28/2021		UPSGLD-11698	Made changes to the logic while populating the #totalcharge table   
Sheetal				11/02/2021		UPSGLD-11872	Added NULL value for latestTemperatureInCelsius and latestTemperatureInFahrenheit in final query
Sheetal				11/11/2021		UPSGLD-12001	1)To match the count in all result sets updated logic of Distinct shipmentNumber, upsTransportShipmentNumber
													2)Added AccountID filter in #TEMP table
Revathy				11/08/2021						Add latestTemperatureInCelsius & latestTemperatureInFahrenheit in response
Anand				11/16/2021						Optimization Change. Created Temp Table to hold DIGITAL_SUMMARY_ORDERS table data and added index to that. 
SAGAR				11/22/2021		42(CL310) 		Added Resultset5 for ShipmentServiceLevel
													Add isShipmentServiceLevelResultSet as input
SAGAR				11/23/2021		UPSGLD-12255	Outbound Shipments SP Issue not returning the wrong result
SAGAR				11/23/2021		UPSGLD-12272	Updated the logic with UPPER(ServiceLevelArray) 
Revathy             12/6/2021		44(CL 320)		Logic to correct  claimstatus flag.
Anand	            12/8/2021						Optimization After above changes. Used a Single Temperory table #DIGITAL_SUMMARY_ORDERS_DATASET to avoid reading table again for @isShipmentServiceLevelResultSet='Y'
Revathy				12/16/2021						changed latestTemperatureInCelsius & latestTemperatureInFahrenheit datatype
Sheetal				01/11/2022      UPSGLD-12954	Commented out filter condition in Result set 3 
Harsha              01/17/2022      UPSGLD-13110    Added Stringsplit logic to handle comma seperated values of param @DeliveryStatus
Sheetal				1/17/2022		47 CL367		Added isInbound column in Result Set 1
Shalaka				1/20/2022		47 CL349		Added filter condition for actualstartdate and original startdate are not available
SAGAR				01/18/2022		47 CL350		Have a mapping for "NULL" shipment mode to UNASSIGNED as value
AVINASH             01/25/2022      UPSGLD-13375    Added in Case Statement while loading the deliveryStatus column
Harsha              01/26/2022      UPSGLD-13112    Added filter for delivery status when inserting into temp #DIGITAL_SUMMARY_ORDERS
Venkata				02/03/2022	    48 CL375		Add originLocation and destinationLocation in request
													SP to support "NULL" as CarrierTypeArray value for shipment listing with shipments without carrier information.
Harsha              02/08/2022                      Adding (nolock) to the tables where it is missing, look for comment --NLHar
SAGAR				02/11/2022						Added missing Schema w.r.t tables
Venkata/Anand/Randy	02/22/2022	   UPSGLD-13071     Worked on the performance tuning
Venkata				03/09/2022	   Sprint 50 CL399	Added OrderType and IsManaged input parameters and completed the sprint 50 development
Venkata				03/11/2022	   Sprint 51 CL426	Added temperatureThreshold related data 
***/
                
/****              
--Updated execution SCRIPT /Most latest script

EXEC digital.[rpt_Outbound_Shipments] 
@AccountKeys = '[{"DPProductLineKey":"20394995-0871-48AE-A2D0-962CFA4BB1C1","DPServiceLineKey":"*"}]',
@Date =Null,
@topRow = 150,@milestoneStatus='*',@warehouseId='*', @deliveryStatus='*',@originCountry = '*',
@destinationCountry='*',@originCity = '*',@destinationCity='*'
,@ShipmentModeArray = '{"ShipmentMode":["*"]}',@ServiceLevelArray= '{"ServiceLevel":["*"]}',@CarrierTypeArray = '{"CarrierType":["*"]}',
@isTemperatureTracked='*' ,@isClaim='*',@isShipmentServiceLevelResultSet = 'Y',@originLocation = '*',@destinationLocation = '*',@orderType = '*'
,@temperatureThreshold = '*' -- 2-8 , 15-25

EXEC digital.[rpt_Outbound_Shipments] 
@AccountKeys = '[{"DPProductLineKey":"1B9B4EF4-EA2D-4DE1-2729-08D6B3A5F414","DPServiceLineKey":"*"}]',
@Date =Null,
@topRow = 150,@milestoneStatus='*',@warehouseId='*', @deliveryStatus='*',@originCountry = '*',
@destinationCountry='*',@originCity = '*',@destinationCity='*'
,@ShipmentModeArray = '{"ShipmentMode":["*"]}',@ServiceLevelArray= '{"ServiceLevel":["*"]}',@CarrierTypeArray = '{"CarrierType":["*"]}',
@isTemperatureTracked='*' ,@isClaim='*',@isShipmentServiceLevelResultSet = 'Y',@originLocation = '*',@destinationLocation = '*',@orderType = '*'

EXEC digital.[rpt_Outbound_Shipments]
@AccountKeys = '[{"DPProductLineKey":"3DAEB74F-FA36-47D7-AC7A-FDBDC4341357","DPServiceLineKey":"*"}]',
@Date =Null,
@topRow = 150,@milestoneStatus='*',@warehouseId='*', @deliveryStatus='*',@originCountry = '*',
@destinationCountry='*',@originCity = '*',@destinationCity='*'
,@ShipmentModeArray = '{"ShipmentMode":["*"]}',@ServiceLevelArray= '{"ServiceLevel":["*"]}',@CarrierTypeArray = '{"CarrierType":["*"]}',
@isTemperatureTracked='*' ,@isClaim='*',@isShipmentServiceLevelResultSet = 'Y',@originLocation = '*',@destinationLocation = '*',
@orderType ='*' --Same Day Order,Amazon Ready Orders,Target Date Orders
,@isManaged = '*' 

EXEC [digital].[rpt_Outbound_Shipments]            
@DPProductLineKey = 'C6BC6B0C-6B96-4466-8F8D-F9EB68B3A48D',            
@DPServiceLineKey = '*', @DPEntityKey = NULL,            
@Date = '{"shipmentCreationStartDate": "2022-01-13","shipmentCreationEndDate": "2022-01-20"}' ,            
@topRow = 150,@milestoneStatus='delivered',@warehouseId='*', @deliveryStatus='*',@originCountry = '*',            
@destinationCountry='*',@originCity = '*',@destinationCity='*' ,@ShipmentModeArray = '{"ShipmentMode":["*"]}',            
@ServiceLevelArray= '{"ServiceLevel":["*"]}',@CarrierTypeArray = '{"CarrierType":["*"]}' ,@isTemperatureTracked='*' ,@isClaim='*'


EXEC [digital].[rpt_Outbound_Shipments]            
@DPProductLineKey = 'A10F512B-C7F4-42A0-909C-21DD95A7D921',            
@DPServiceLineKey = '*', @DPEntityKey = NULL,            
--@Date = '{"shipmentCreationStartDate": "2021-07-25","shipmentCreationEndDate": "2021-10-22"}' ,            
@topRow = 0,@milestoneStatus='*',@warehouseId='*', @deliveryStatus='*',@originCountry = '*',            
@destinationCountry='*',@originCity = '*',@destinationCity='*' ,@ShipmentModeArray = '{"ShipmentMode":["*"]}',            
@ServiceLevelArray= '{"ServiceLevel":["*"]}',@CarrierTypeArray = '{"CarrierType":["*"]}' ,@isTemperatureTracked='*' ,@isClaim='*';  



EXEC digital.[rpt_Outbound_Shipments] 
@AccountKeys = '[{"DPProductLineKey":"C6BC6B0C-6B96-4466-8F8D-F9EB68B3A48D","DPServiceLineKey":"*"}]',
@Date = '{"shipmentCreationStartDate": "2021-12-16","shipmentCreationEndDate": "2022-01-12"}' ,
@topRow = 150,@milestoneStatus='*',@warehouseId='*', @deliveryStatus='*',@originCountry = '*',
@destinationCountry='*',@originCity = '*',@destinationCity='*'
,@ShipmentModeArray = '{"ShipmentMode":["UNASSIGNED"]}',@ServiceLevelArray= '{"ServiceLevel":["*"]}',@CarrierTypeArray = '{"CarrierType":["*"]}',
@isTemperatureTracked='*' ,@isClaim='*';
            
****/

CREATE PROCEDURE [digital].[rpt_Outbound_Shipments]
--DECLARE
	      @DPProductLineKey                  VARCHAR(50) = NULL
        , @DPServiceLineKey                VARCHAR(50) = NULL
        , @DPEntityKey                     VARCHAR(50) = NULL
        , @AccountKeys                     NVARCHAR(max) = NULL
        , @startDate                       DATE=NULL
        , @endDate                         DATE=NULL
        , @warehouseId                     VARCHAR(max)
        , @topRow                          INT
        , @milestoneStatus                 NVARCHAR(max)='*'
        , @Date                            NVARCHAR(max)='*'
        , @originCity                      VARCHAR(500)='*'
        , @destinationCity                 VARCHAR(500)='*'
        , @originCountry                   VARCHAR(500)='*'
        , @destinationCountry              VARCHAR(500)='*'
        , @deliveryStatus                  VARCHAR(50)='*'
        , @ShipmentMode                    NVARCHAR(max)=NULL
        , @ShipmentModeArray               NVARCHAR(max)='{"ShipmentMode":["*"]}'
        , @ServiceLevelArray               NVARCHAR(max)='{"ServiceLevel":["*"]}'
        , @CarrierTypeArray                VARCHAR(128)='{"CarrierType":["*"]}'
        , @isTemperatureTracked            NVARCHAR(1) = '*'
        , @isShipmentServiceLevelResultSet NVARCHAR(1)=''
        , @isClaim                         VARCHAR(200)='*' --CL275
        , @originLocation                  NVARCHAR(200) = NULL --CL375
        , @destinationLocation             NVARCHAR(200) = NULL --CL375
		, @OrderType					   VARCHAR(max) = '*' --CL399
		, @isManaged					   VARCHAR(10) = NULL  --CL399
		, @temperatureThreshold			   VARCHAR(MAX) = '*' --CL426

--WITH RECOMPILE 
AS

BEGIN
SET NOCOUNT ON

DECLARE @VarAccountID                          VARCHAR(50)
        , @VarDPServiceLineKey                 VARCHAR(50)
        , @VarDPEntityKey                      VARCHAR(50)
        , @NULLCreatedDate                     VARCHAR(1)
        , @VarwarehouseId                      VARCHAR(50)
        , @VarmilestoneStatus                  VARCHAR(50)
        , @shipmentCreationStartDate           DATE
        , @shipmentCreationEndDate             DATE
        , @shipmentCreationStartDateJSON       DATE
        , @shipmentCreationEndDateJSON         DATE
        , @shipmentCreationStartDateTime       DATETIME
        , @shipmentCreationEndDateTime         DATETIME
        , @NULLScheduledDeliveryDate           VARCHAR(1)
        , @ScheduledDeliveryStartDate          DATE
        , @ScheduledDeliveryEndDate            DATE
        , @ScheduledDeliveryStartDateTime      DATETIME
        , @ScheduledDeliveryEndDateTime        DATETIME
        , @NULLDeliveryETADate                 VARCHAR(1)
        , @DeliveryETAStartDate                DATE
        , @DeliveryETAEndDate                  DATE
        , @DeliveryETAStartDateTime            DATETIME
        , @DeliveryETAEndDateTime              DATETIME
        , @VarOriginCountry                    NVARCHAR(max)
        , @NULLOriginCountry                   VARCHAR(1)
        , @VarDestinationCountry               NVARCHAR(max)
        , @NULLDestinationCountry              VARCHAR(1)
        , @VarOriginCity                       NVARCHAR(max)
        , @NULLOriginCity                      VARCHAR(1)
        , @VarDestinationCity                  NVARCHAR(max)
        , @NULLDestinationCity                 VARCHAR(1)
        , @NULLShippedDate                     VARCHAR(1)
        , @shippedStartDate                    DATE
        , @shippedEndDate                      DATE
        , @shippedStartDateTime                DATETIME
        , @shippedEndDateTime                  DATETIME
        , @NULLActualDeliveryDate              VARCHAR(1)
        , @actualDeliveryStartDate             DATE
        , @actualDeliveryEndDate               DATE
        , @actualDeliveryStartDateTime         DATETIME
        , @actualDeliveryEndDateTime           DATETIME
        , @ScheduleToShipStartDate             DATE
        , @ScheduleToShipEndDate               DATE
        , @ScheduleToShipStartDateTime         DATETIME
        , @ScheduleToShipEndDateTime           DATETIME
        , @NULLScheduleToShipDate              VARCHAR(1)
        , @NULLDeliveryStatus                  CHAR(1)
        , @NULLisShipmentServiceLevelResultSet CHAR(255)
        , @VardeliveryStatus                   VARCHAR(200)
        , @VarDPServiceLineKeyJSON             VARCHAR(500)
        , @VarDPProductLineKeyJSON             VARCHAR(500)
        , @VarCarrierTypeArray                 VARCHAR(max)
        , @VarserviceLevelArray                NVARCHAR(max)
        , @VarshipmentModeArray                NVARCHAR(max)
        , @VarisTemperatureTracked             CHAR(1)
        , @VarisShipmentServiceLevelResultSet  CHAR(255) --CL310
        , @NulloriginLocation                  NVARCHAR(200) --CL375
        , @NulldestinationLocation             NVARCHAR(200) --CL375
		, @varOrderType						   VARCHAR(max) --CL399
		, @NullOrderType					   VARCHAR(max) --CL399
		, @varIsManaged						   VARCHAR(10) --CL399
		, @NULLIsManaged					   VARCHAR(10) --CL399
		, @bookedStartDate					   date  
		, @bookedEndDate					   date  
		, @bookedStartDateTime				   datetime  
		, @bookedEndDateTime				   datetime  
		, @NULLbookedDate					   varchar(1)  
		, @pickupStartDate					   date  
		, @pickupEndDate					   date  
		, @pickupStartDateTime				   datetime  
		, @pickupEndDateTime				   datetime  
		, @NULLpickupDate					   varchar(1)  
		, @estimatedDepartureStartDate		   date  
		, @estimatedDepartureEndDate		   date  
		, @estimatedDepartureStartDateTime	   datetime  
		, @estimatedDepartureEndDateTime	   datetime  
		, @NULLestimatedDepartureDate		   varchar(1)  
		, @estimatedArrivalStartDate		   date  
		, @estimatedArrivalEndDate			   date  
		, @estimatedArrivalStartDateTime	   datetime  
		, @estimatedArrivalEndDateTime		   datetime  
		, @NULLestimatedArrivalDate			   varchar(1)
		, @varTemperatureThreshold			   VARCHAR(MAX)	--CL426
		, @NullTemperatureThreshold			   VARCHAR(10) --CL426

IF Object_id('tempdb..#ACCOUNTINFO') IS NOT NULL
  DROP TABLE #ACCOUNTINFO

CREATE TABLE #ACCOUNTINFO
(
	[RowId] INT IDENTITY(1,1) NOT NULL,
	[DPProductLineKey] [VARCHAR](max) NULL,
	[DPServiceLineKey] [VARCHAR](max) NULL
)
INSERT INTO #ACCOUNTINFO
	([DPProductLineKey]
	, [DPServiceLineKey])
SELECT Upper(DPProductLineKey) AS DPProductLineKey,
         Upper(DPServiceLineKey) AS DPServiceLineKey
      --INTO #ACCOUNTINFO
      FROM Openjson(@AccountKeys)
      WITH(
     DPProductLineKey VARCHAR(MAX),
     DPServiceLineKey VARCHAR(MAX)
      )

IF (@Date <> '*')  
BEGIN  
SELECT    @shipmentCreationStartDateJSON	= shipmentCreationStartDate,  
          @shipmentCreationEndDateJSON		= shipmentCreationEndDate,  
		  @ScheduledDeliveryStartDate		= scheduleDeliveryStartDate,  
		  @ScheduledDeliveryEndDate			= scheduleDeliveryEndDate,  
		  @DeliveryETAStartDate				= deliveryEtaStartDate,  
		  @DeliveryETAEndDate				= deliveryEtaEndDate,  
		  @shippedStartDate					= shippedStartDate,  
		  @shippedEndDate					= shippedEndDate,  
		  @actualDeliveryStartDate			= actualDeliveryStartDate,  
		  @actualDeliveryEndDate			= actualDeliveryEndDate,  
		  @ScheduleToShipStartDate			= scheduleToShipStartDate,  
		  @ScheduleToShipEndDate			= scheduleToShipEndDate,  
		  @bookedStartDate					= bookedStartDate,  
		  @bookedEndDate					= bookedEndDate,  
		  @pickupStartDate					= pickupStartDate,  
		  @pickupEndDate					= pickupEndDate,  
		  @estimatedDepartureStartDate		= estimatedDepartureStartDate,  
		  @estimatedDepartureEndDate		= estimatedDepartureEndDate,  
		  @estimatedArrivalStartDate		= estimatedArrivalStartDate,  
		  @estimatedArrivalEndDate			= estimatedArrivalEndDate  
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

/*********************************************************/
-----CarrierType                
/*********************************************************/
IF Object_id('tempdb..#CarrierTypeARRAY') IS NOT NULL
  DROP TABLE #CarrierTypeARRAY

SELECT   CASE WHEN CarrierTypeARRAY='NULL' THEN 'UNASSIGNED' ELSE CarrierTypeARRAY END AS CarrierTypeArray
     INTO #CarrierTypeARRAY
     FROM Openjson(@CarrierTypeArray)
     WITH (
    CarrierType nvarchar(max) 'strict $.CarrierType' AS JSON
 )
 OUTER APPLY OPENJSON(CarrierType) WITH (CarrierTypeARRAY NVARCHAR(MAX) '$');
                
/*********************************************************/
-----SHIPMENTARRAY                
/*********************************************************/IF Object_id('tempdb..#SHIPMENTARRAY') IS NOT NULL
  DROP TABLE #SHIPMENTARRAY

SELECT CASE WHEN SHIPMENTARRAY='NULL' THEN 'UNASSIGNED' ELSE SHIPMENTARRAY END AS SHIPMENTARRAY
	 INTO #SHIPMENTARRAY
     FROM Openjson(@ShipmentModeArray)
     WITH (
    ShipmentMode nvarchar(max) 'strict $.ShipmentMode' AS JSON
 )
 OUTER APPLY OPENJSON(ShipmentMode) WITH (SHIPMENTARRAY NVARCHAR(MAX) '$');


/*********************************************************/
-----SERVICELEVELARRAY                
/*********************************************************/IF Object_id('tempdb..#SERVICELEVELARRAY') IS NOT NULL
  DROP TABLE #SERVICELEVELARRAY

SELECT   Upper(SERVICELEVELARRAY) AS SERVICELEVELARRAY
     INTO #SERVICELEVELARRAY                --12272                 
     FROM Openjson(@ServiceLevelArray)
     WITH (
    ServiceLevel nvarchar(max) 'strict $.ServiceLevel' AS JSON
 )
 OUTER APPLY OPENJSON(ServiceLevel) WITH (SERVICELEVELARRAY NVARCHAR(MAX) '$');
SET @VarAccountID = Upper(@DPProductLineKey)
SET @VarDPServiceLineKey = Upper(@DPServiceLineKey)
SET @VarDPEntityKey = Upper(@DPEntityKey)
SET @VarwarehouseId = Upper(@warehouseId)
SET @VarmilestoneStatus = Upper(@milestoneStatus)
SET @VarisTemperatureTracked=Upper(@isTemperatureTracked)
SET @VarisShipmentServiceLevelResultSet=Upper(@isShipmentServiceLevelResultSet)--CL310
SET @VarOriginCountry=Upper(@originCountry)
SET @VarDestinationCountry=Upper(@destinationCountry)
SET @VarOriginCity=Upper(@originCity)
SET @VarDestinationCity=Upper(@destinationCity)
SET @VardeliveryStatus=Upper(@deliveryStatus)
SET @shipmentCreationStartDateTime=@shipmentCreationStartDate
SET @shipmentCreationEndDateTime = Dateadd(ms, -2, Dateadd(dd, 1, Datediff(dd, 0, @shipmentCreationEndDate)))

SET @ScheduledDeliveryStartDateTime=@ScheduledDeliveryStartDate
SET @ScheduledDeliveryEndDateTime = Dateadd(ms, -2, Dateadd(dd, 1, Datediff(dd, 0, @ScheduledDeliveryEndDate)))

SET @DeliveryETAStartDateTime=@DeliveryETAStartDate
SET @DeliveryETAEndDateTime = Dateadd(ms, -2, Dateadd(dd, 1, Datediff(dd, 0, @DeliveryETAEndDate)))

SET @shippedStartDateTime=@shippedStartDate
SET @shippedEndDateTime = Dateadd(ms, -2, Dateadd(dd, 1, Datediff(dd, 0, @shippedEndDate)))

SET @actualDeliveryStartDateTime=@actualDeliveryStartDate
SET @actualDeliveryEndDateTime = Dateadd(ms, -2, Dateadd(dd, 1, Datediff(dd, 0, @actualDeliveryEndDate)))

SET @ScheduleToShipStartDateTime =@ScheduleToShipStartDate
SET @ScheduleToShipEndDateTime = Dateadd(ms, -2, Dateadd(dd, 1, Datediff(dd, 0, @ScheduleToShipEndDate)))
--CL399
SET @bookedStartDateTime =@bookedStartDate  
SET @bookedEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @bookedEndDate)))  
  
SET @pickupStartDateTime =@pickupStartDate  
SET @pickupEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @pickupEndDate)))  
  
SET @estimatedDepartureStartDateTime =@estimatedDepartureStartDate  
SET @estimatedDepartureEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @estimatedDepartureEndDate)))  
  
SET @estimatedArrivalStartDateTime =@estimatedArrivalStartDate  
SET @estimatedArrivalEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @estimatedArrivalEndDate)))
--CL399
SET @varOrderType = UPPER(@OrderType) --CL399
SET @varIsManaged = CASE WHEN UPPER(@IsManaged) = 'Y' THEN '1' 
						   WHEN UPPER(@IsManaged) = 'N' THEN '0' ELSE @IsManaged END --CL399

SET @varTemperatureThreshold  = UPPER(@temperatureThreshold) --CL426

IF Isnull(@ScheduledDeliveryStartDate, '') = ''
    OR Isnull(@ScheduledDeliveryEndDate, '') = ''
  SET @NULLScheduledDeliveryDate = '*'

IF Isnull(@DeliveryETAStartDate, '') = ''
    OR Isnull(@DeliveryETAEndDate, '') = ''
  SET @NULLDeliveryETADate = '*'

IF Isnull(@shippedStartDate, '') = ''
    OR Isnull(@shippedEndDate, '') = ''
  SET @NULLShippedDate = '*'

IF Isnull(@actualDeliveryStartDate, '') = ''
    OR Isnull(@actualDeliveryEndDate, '') = ''
  SET @NULLActualDeliveryDate = '*'

IF Isnull(@ScheduleToShipStartDate, '') = ''
    OR Isnull(@ScheduleToShipEndDate, '') = ''
  SET @NULLScheduleToShipDate = '*'

IF ISNULL(@bookedStartDate,'')='' 
	OR ISNULL(@bookedEndDate,'')=''  
   SET @NULLbookedDate = '*'  --CL399
  
IF ISNULL(@pickupStartDate,'')='' 
	OR ISNULL(@pickupEndDate,'')=''  
   SET @NULLpickupDate = '*' --CL399

--CL375
IF Isnull(@originLocation, '') = ''
    OR @originLocation = '*'
  SET @NullOriginLocation = '*'
IF Isnull(@destinationLocation, '') = ''
    OR @destinationLocation = '*'
  SET @NulldestinationLocation = '*'
--CL375

IF Isnull(@shipmentCreationStartDateJSON, '') <> ''
   AND Isnull(@shipmentCreationEndDateJSON, '') <> ''
  BEGIN
      SET @shipmentCreationStartDateTime=@shipmentCreationStartDateJSON
      SET @shipmentCreationEndDateTime = Dateadd(ms, -2, Dateadd(dd, 1, Datediff(dd, 0, @shipmentCreationEndDateJSON)))
  END
ELSE IF( @startDate IS NOT NULL
    AND @endDate IS NOT NULL )
  BEGIN
      SET @shipmentCreationStartDateTime=@startDate
      SET @shipmentCreationEndDateTime = Dateadd(ms, -2, Dateadd(dd, 1, Datediff(dd, 0, @endDate)))
  END
ELSE
  BEGIN
      SET @NULLCreatedDate = '*'
  END

IF @DPServiceLineKey IS NULL
  SET @VarDPServiceLineKey = '*'

IF @DPEntityKey IS NULL
  SET @VarDPEntityKey = '*'

IF @VardeliveryStatus = ''
    OR Isnull(@VardeliveryStatus, '*') = '*'
  SET @NULLDeliveryStatus='*'

IF @VarOriginCountry = ''
    OR Isnull(@VarOriginCountry, '*') = '*'
  SET @NULLOriginCountry='*'

IF @VarDestinationCountry = ''
    OR Isnull(@VarDestinationCountry, '*') = '*'
  SET @NULLDestinationCountry='*'

IF @VarOriginCity = ''
    OR Isnull(@VarOriginCity, '*') = '*'
  SET @NULLOriginCity='*'

IF @VarDestinationCity = ''
    OR Isnull(@VarDestinationCity, '*') = '*'
  SET @NULLDestinationCity='*'

IF Isnull(@VarisTemperatureTracked, '') = ''
  SET @VarisTemperatureTracked = '*'

IF @varOrderType = '*' --CL399
  SET @NullOrderType = '*'

IF ISNULL(@varIsManaged,'')='' OR @varIsManaged = '*'--CL399
	SET @NULLIsManaged = '*'

IF ISNULL(@varTemperatureThreshold,'') = '' OR @varTemperatureThreshold = '*' --CL426
	SET @NullTemperatureThreshold = '*'

/*********************************************************/
-- BACKWARD COMPATIBILITY                
/*********************************************************/
IF NOT EXISTS (SELECT
                 DPServiceLineKey
               FROM   #ACCOUNTINFO
               WHERE  DPServiceLineKey IS NOT NULL)
  SET @VarDPServiceLineKeyJSON = '*'

IF ( ( @DPServiceLineKey IS NOT NULL )
     AND @VarDPServiceLineKeyJSON = '*' )
  SET @VarDPServiceLineKey = Upper(@DPServiceLineKey)

IF ( @VarDPServiceLineKeyJSON = '*'
     AND Isnull(@DPServiceLineKey, '*') = '*' )
  SET @VarDPServiceLineKey = '*'

IF NOT EXISTS (SELECT
                 DPProductLineKey
               FROM   #ACCOUNTINFO
               WHERE  DPProductLineKey IS NOT NULL)
  SET @VarDPProductLineKeyJSON = '*'

IF ( ( @DPProductLineKey IS NOT NULL )
     AND @VarDPProductLineKeyJSON = '*' )
  SET @VarAccountID = Upper(@DPProductLineKey)

IF EXISTS(SELECT TOP 1
            CarrierTypeArray
          FROM   #CarrierTypeARRAY
          WHERE  CarrierTypeArray = '*')
  BEGIN
      SET @VarCarrierTypeArray='*'
  END

IF EXISTS(SELECT TOP 1
            SHIPMENTARRAY
          FROM   #SHIPMENTARRAY
          WHERE  SHIPMENTARRAY = '*')
  BEGIN
      SET @VarshipmentModeArray='*'
  END

IF EXISTS(SELECT TOP 1
            SERVICELEVELARRAY
          FROM   #SERVICELEVELARRAY
          WHERE  SERVICELEVELARRAY = '*')
  BEGIN
      SET @VarserviceLevelArray='*'
  END

/*********************************************************/
--MileStone Status
/*********************************************************/
IF Object_id('tempdb..#TmpMileStoneStatus') IS NOT NULL
  DROP TABLE #TmpMileStoneStatus

SELECT
  Upper(value) AS MileStoneStatus
INTO   #TmpMileStoneStatus
FROM   String_split(@VarMilestoneStatus, ',')

--UPSGLD-13110
/*********************************************************/
--Delivery Status 
/*********************************************************/
IF Object_id('tempdb..#TmpDeliveryStatus') IS NOT NULL
  DROP TABLE #TmpDeliveryStatus

SELECT
  Upper(value) AS DeliveryStatus
INTO   #TmpDeliveryStatus
FROM   String_split(@VardeliveryStatus, ',')

--UPSGLD-13110
IF Object_id('tempdb..#TEMP') IS NOT NULL
  DROP TABLE #TEMP

 CREATE TABLE #TEMP
  (UPSOrderNumber varchar( 128),
  SourceSystemKey int,
  ACTIVITY_NOTES varchar (128),
  ActivityDate datetime)
 IF EXISTS (SELECT * FROM #ACCOUNTINFO )
 BEGIN

INSERT INTO #TEMP
SELECT
  MA.UPSOrderNumber
  , MA.SourceSystemKey
  , MA.ACTIVITY_NOTES
  , MA.ActivityDate --UPSGLD-11400
--INTO   #TEMP
--INTO #LAST_LOCATION
FROM   Summary.Digital_summary_milestone_activity (NOLOCK) MA
--WHERE  ( MA.AccountId IN (SELECT
--                            DPProductLineKey
--                          FROM   #ACCOUNTINFO)
--          OR MA.AccountId = @VarAccountID )
--	   --AND MA.ActivityCode NOT IN ( 'AB', 'E' )
--	   )
--       AND MA.ACTIVITY_NOTES IS NOT NULL

	   JOIN #ACCOUNTINFO AC ON MA.AccountId=AC.DPProductLineKey 
	   WHERE  MA.ActivityCode NOT IN ( 'AB', 'E' )
      AND MA.ACTIVITY_NOTES IS NOT NULL
END
  ELSE
  BEGIN
INSERT INTO #TEMP
  SELECT
  MA.UPSOrderNumber
  , MA.SourceSystemKey
  , MA.ACTIVITY_NOTES
  , MA.ActivityDate --UPSGLD-11400
--INTO   #TEMP
--INTO #LAST_LOCATION
FROM   Summary.Digital_summary_milestone_activity (NOLOCK) MA
WHERE  
           MA.AccountId = @VarAccountID 
	   AND MA.ActivityCode NOT IN ( 'AB', 'E' )
       AND MA.ACTIVITY_NOTES IS NOT NULL
  
  END

--Temp Final
IF Object_id('tempdb..#TEMPFINAL') IS NOT NULL
  DROP TABLE #TEMPFINAL

SELECT
  Row_number()
    OVER (
      PARTITION BY UPSOrderNumber
      ORDER BY ActivityDate DESC) ROWNUM
  , #TEMP.*
INTO   #TEMPFINAL --UPSGLD-11400
FROM   #TEMP

-- Last Location 
IF Object_id('tempdb..#LAST_LOCATION') IS NOT NULL
  DROP TABLE #LAST_LOCATION

SELECT
  *
INTO   #LAST_LOCATION
FROM   #TEMPFINAL
WHERE  ROWNUM = 1

-- WarehouseIds
IF Object_id('tempdb..#WarehouseId') IS NOT NULL
  DROP TABLE #WarehouseId

SELECT
  Trim (value) AS WarehouseId
INTO   #WarehouseId
FROM   String_split(@VarwarehouseId, ',')


--CL375
-- Original Location 
IF Object_id('tempdb..#OrginLocation') IS NOT NULL
  DROP TABLE #OrginLocation
SELECT
  Upper(value) AS OriginLocation
INTO   #OrginLocation
FROM   String_split(@originLocation, '|') -- Venkata
-- Destination Location 
IF Object_id('tempdb..#DestinationLocation') IS NOT NULL
  DROP TABLE #DestinationLocation
SELECT
  Upper(value) AS DestinationLocation
INTO   #DestinationLocation
FROM   String_split(@destinationLocation, '|') --Venkata

IF Object_id('tempdb..#OrderType') IS NOT NULL --CL399
	DROP TABLE #OrderType
SELECT
  Upper(value) AS orderType
INTO   #OrderType
FROM   String_split(@varOrderType, ',') 

DROP TABLE IF EXISTS #TemperatureThreshold --CL426
SELECT UPPER(VALUE) AS TemperatureThreshold
INTO #TemperatureThreshold
FROM STRING_SPLIT(@varTemperatureThreshold, ',')

--CL375


--CL275             
--CREATE TABLE #totalcharge(              
--UPSOrderNumber VARCHAR(128),              
--totalCharge DECIMAL(10,2),              
--totalcurency VARCHAR(200)              
--) 




IF Object_id('tempdb..#DIGITAL_SUMMARY_ORDERS_DATASET') IS NOT NULL
  DROP TABLE #DIGITAL_SUMMARY_ORDERS_DATASET

CREATE TABLE #DIGITAL_SUMMARY_ORDERS_DATASET
  (
     shipmentNumber                          VARCHAR(128)
     , sourcesystemkey                       INT
     , referenceNumber                       VARCHAR(512)
     , customerPONumber                      VARCHAR(512)
     , upsTransportShipmentNumber            VARCHAR(128)
     , gffShipmentInstanceId                 VARCHAR(128)
     , gffShipmentNumber                     VARCHAR(128)
     , shipmentOrigin_contactName            VARCHAR(128)
     , shipmentOrigin_addressLine1           NVARCHAR(512)
     , shipmentOrigin_addressLine2           NVARCHAR(512)
     , shipmentOrigin_city                   NVARCHAR(255)
     , shipmentOrigin_stateProvince          NVARCHAR(255)
     , shipmentOrigin_postalCode             NVARCHAR(255)
     , shipmentOrigin_country                NVARCHAR(255)
     , shipmentDestination_contactName       VARCHAR(255)
     , shipmentDestination_addressLine1      NVARCHAR(512)
     , shipmentDestination_addressLine2      NVARCHAR(512)
     , shipmentDestination_city              NVARCHAR(255)
     , shipmentDestination_stateProvince     NVARCHAR(255)
     , shipmentDestination_postalCode        NVARCHAR(255)
     , shipmentDestination_country           NVARCHAR(255)
     , shipmentDescription                   VARCHAR(50)
     , shipmentService                       VARCHAR(128)
     , shipmentServiceLevel                  VARCHAR(255)
     , shipmentServiceLevelCode              VARCHAR(255)
     , shipmentCarrierCode                   VARCHAR(255)
     , shipmentCarrier                       VARCHAR(255)
     , inventoryShipmentStatus               VARCHAR(255)
     , transportationMileStone               VARCHAR(max)
     , shipmentPrimaryException              VARCHAR(160)
     , shipmentBookedOnDateTime              DATETIME
     , shipmentCanceledDateTime              DATETIME
     , shipmentCanceledReason                VARCHAR(512)
     , actualShipmentDateTime                DATETIME
     , shipmentCreateOnDateTime              DATETIME
     , originalScheduledDeliveryDateTime     DATETIME
     , actualDeliveryDateTime                DATETIME
     , warehouseId                           VARCHAR(255)
     , warehouseCode                         VARCHAR(50)
     , milestoneStatus                       VARCHAR(40)
     , estimatedDeliveryDateTime             DATETIME
     , referenceNumber1                      VARCHAR(512)
     , referenceNumber2                      VARCHAR(512)
     , referenceNumber3                      VARCHAR(512)
     , referenceNumber4                      VARCHAR(512)
     , referenceNumber5                      VARCHAR(512)
     , shipmentCreateOnDateTimeZone          VARCHAR(128)
     , originalScheduledDeliveryDateTimeZone VARCHAR(128)
     , shippedDateTime                       DATETIME
     , shippedDateTimeZone                   VARCHAR(128)
     , LoadID                                VARCHAR(128)
     , shipmentDestination_locationCode      VARCHAR(255)
     , dpProductLineKey                      VARCHAR(255)
     , Accountnumber                         VARCHAR(255)
     , deliveryStatus                        VARCHAR(7)
     , isTemperatureTracked                  VARCHAR(1)
     , isShipmentServiceLevelResultSet       VARCHAR(255)
     , --CL310
     latestTemperature                       VARCHAR(128)
     , temperatureDateTime                   DATETIME
     , temperatureCity                       VARCHAR(200)
     , temperatureState                      VARCHAR(200)
     , temperatureCountry                    VARCHAR(200)
     , totalCharge                           VARCHAR(200)
     , --CL275               
     totalChargeCurrency                     VARCHAR(200)
     , --CL275               
     isClaim                                 VARCHAR(1)
     , --CL275            
     lastKnownLocation                       VARCHAR(255)
     , --CL282            
     latestTemperatureInCelsius              DECIMAL(18, 2)
     , latestTemperatureInFahrenheit         DECIMAL(18, 2)
     , DP_SERVICELINE_KEY                    VARCHAR(255)
     , IS_INBOUND                            INT --CL367
	 , scheduledPickupDateTime				 DATETIME --CL399
	 , shipmentType					   		 VARCHAR(20) --CL399
	 , temperatureThreshold					VARCHAR(255) --CL426
  )

INSERT INTO #DIGITAL_SUMMARY_ORDERS_DATASET
SELECT
  O.[UPSOrderNumber] shipmentNumber
  , O.SourceSystemKey
  , O.OrderNumber referenceNumber
  , O.CustomerPO customerPONumber
  , O.UPSTransportShipmentNumber upsTransportShipmentNumber
  , O.GFF_ShipmentInstanceId gffShipmentInstanceId
  , O.GFF_ShipmentNumber gffShipmentNumber
  , O.OriginContactName shipmentOrigin_contactName
  , O.OriginAddress1 shipmentOrigin_addressLine1
  , O.OriginAddress2 shipmentOrigin_addressLine2
  , O.OriginCity shipmentOrigin_city
  , O.OriginProvince shipmentOrigin_stateProvince
  , O.OriginPostalCode shipmentOrigin_postalCode
  , O.OriginCountry shipmentOrigin_country
  , O.DestinationContactName shipmentDestination_contactName
  , O.DestinationAddress1 shipmentDestination_addressLine1
  , O.DestinationAddress2 shipmentDestination_addressLine2
  , O.DestinationCity shipmentDestination_city
  , O.DestinationProvince shipmentDestination_stateProvince
  , O.DestinationPostalcode shipmentDestination_postalCode
  , O.DestinationCountry shipmentDestination_country
  , O.OrderType shipmentDescription
  , O.ServiceMode shipmentService
  , O.ServiceLevel shipmentServiceLevel
  , O.[ServiceLevelCode] shipmentServiceLevelCode
  , O.CarrierCode shipmentCarrierCode
  , O.Carrier shipmentCarrier
  , O.[OrderStatusName] inventoryShipmentStatus
  , O.TRANS_MILESTONE transportationMileStone
  , O.[ExceptionCode] shipmentPrimaryException
  , ShipmentBookedDate AS shipmentBookedOnDateTime
  , O.DateTimeCancelled shipmentCanceledDateTime
  , O.[CancelledReasonCode] shipmentCanceledReason
  , O.[ScheduleShipmentDate] actualShipmentDateTime
  , O.DateTimeReceived shipmentCreateOnDateTime
  , O.OriginalScheduledDeliveryDateTime originalScheduledDeliveryDateTime
  , O.ActualDeliveryDateTime AS actualDeliveryDateTime
  ,
  --(SELECT MAX(ma.ActivityDate) FROM Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) ma                
  --          WHERE O.UPSOrderNumber = ma.UPSOrderNumber                
  --          AND O.SourceSystemKey = ma.SourceSystemKey                
  --          AND ma.ActivityCode in('D','D1','D9')                 
  --          AND ( ma.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR ma.AccountId = @VarAccountID )                 
  --)actualDeliveryDateTime,                
  O.FacilityId warehouseId
  , O.OrderWarehouse warehouseCode
  , O.CurrentMilestone milestoneStatus
  , O.EstimatedDeliveryDateTime AS estimatedDeliveryDateTime
  ,
  --(SELECT MAX(ma.ActivityDate) FROM Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) ma                
  --          WHERE O.UPSOrderNumber = ma.UPSOrderNumber                
  --          AND O.SourceSystemKey = ma.SourceSystemKey                
  --          AND ma.ActivityCode IN('AG','AB','AA')                
  --             AND ( ma.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR AccountId = @VarAccountID )                 
  --  )estimatedDeliveryDateTime,                
  O.ORDER_REF_1_VALUE referenceNumber1
  , O.ORDER_REF_2_VALUE referenceNumber2
  , O.ORDER_REF_3_VALUE referenceNumber3
  , O.ORDER_REF_4_VALUE referenceNumber4
  , O.ORDER_REF_5_VALUE referenceNumber5
  , O.OriginTimeZone AS shipmentCreateOnDateTimeZone
  , O.DestinationTimeZone AS originalScheduledDeliveryDateTimeZone
  , O.DateTimeShipped AS shippedDateTime
  , O.shippedDateTimeZone
  , O.LOAD_ID AS LoadID
  , O.DestinationLocationCode AS shipmentDestination_locationCode
  , O.AccountId AS dpProductLineKey
  , O.Account_number AS Accountnumber
  ,
  --CASE  WHEN O.actualDeliveryDateTime >  O.originalScheduledDeliveryDateTime THEN 'LATE'  -- DSO.LoadLatestDeliveryDate THEN 'LATE'                
  --       WHEN CAST(O.actualDeliveryDateTime AS DATE) < = CAST(O.originalScheduledDeliveryDateTime AS DATE) 
  --	OR (CAST(o.actualDeliveryDateTime AS DATE) IS NULL OR CAST(O.originalScheduledDeliveryDateTime AS DATE) IS NULL) --Adding the condition as part of CL349
  --	THEN 'ONTIME' -- CAST(DSO.LoadLatestDeliveryDate AS DATE) THEN 'ONTIME'                 
  --       END AS deliveryStatus,    
  CASE
      WHEN O.CurrentMilestone = 'DELIVERED' THEN ---UPSGLD-13375
        CASE
          WHEN O.actualDeliveryDateTime > O.originalScheduledDeliveryDateTime THEN 'LATE' -- DSO.LoadLatestDeliveryDate THEN 'LATE'                
          WHEN Cast(O.actualDeliveryDateTime AS DATE) < = Cast(O.originalScheduledDeliveryDateTime AS DATE)
                OR ( Cast(o.actualDeliveryDateTime AS DATE) IS NULL
                      OR Cast(O.originalScheduledDeliveryDateTime AS DATE) IS NULL ) --Adding the condition as part of CL349
        THEN 'ONTIME' -- CAST(DSO.LoadLatestDeliveryDate AS DATE) THEN 'ONTIME' 
        END
      ELSE
        NULL
    END ---UPSGLD-13375
    AS deliveryStatus
  , CC.IS_TEMPERATURE AS isTemperatureTracked
  , O.ServiceLevel AS isShipmentServiceLevelResultSet --CL310
  , CC.LATEST_TEMPERATURE AS latestTemperature
  , CC.TEMPERATURE_DATETIME AS temperatureDateTime
  , CC.TEMPERATURE_CITY AS temperatureCity
  , CC.TEMPERATURE_STATE AS temperatureState
  , CC.TEMPERATURE_COUNTRY AS temperatureCountry
  , '' totalCharge --CL275            
  , '' totalChargeCurrency --CL275             
  , CASE
      WHEN DSTR.UPSOrderNumber IS NULL THEN 'N'
      ELSE
        'Y'
    END AS ISCLAIM --CL275            
  , MA.ACTIVITY_NOTES AS lastKnownLocation --CL282    
  , CC.TemperatureC AS latestTemperatureInCelsius
  , CC.TemperatureF AS latestTemperatureInFahrenheit
  , O.DP_SERVICELINE_KEY
  , O.IS_INBOUND --CL367
  , O.ScheduledPickUpDateTime AS scheduledPickupDateTime
  , CASE WHEN O.Is_Inbound = 0 THEN 'OUTBOUND' END AS shipmentType
  , O.TemperatureThreshold AS temperatureThreshold --CL426
FROM   [Summary].[Digital_summary_orders] (NOLOCK) O
       LEFT JOIN Summary.Digital_summary_transportation_callcheck (NOLOCK) CC
              --ON O.UPSOrderNumber=CC.UPSORDERNUMBER              
              ON O.UPSTransportShipmentNumber = CC.UPSORDERNUMBER
                 --AND O.SourceSystemKey=CC.SOURCESYSTEMKEY                
                 AND CC.STATUSDETAILTYPE = 'TemperatureTracking'
                 AND CC.IS_LATEST_TEMPERATURE = 'Y'
       LEFT JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION FTTR (NOLOCK)
              ON ( CASE
                     WHEN FTTR.TrasOnlyFlag <> 'TRANS ONLY' THEN Isnull(FTTR.UpsWMSSourceSystemKey, O.SourceSystemKey)
                     ELSE
                       FTTR.SourceSystemKey
                   END = O.SourceSystemKey
                   AND CASE
                         WHEN FTTR.TrasOnlyFlag <> 'TRANS ONLY' THEN FTTR.UpsWMSOrderNumber
                         ELSE
                           FTTR.UpsOrderNumber
                       END = O.UPSOrderNumber )
       LEFT JOIN [Summary].[Digital_summary_transportation_references] (NOLOCK) DSTR
              ON FTTR.UpsOrderNumber = DSTR.UPSOrderNumber
                 AND FTTR.SourceSystemKey = DSTR.SourceSystemKey --10/26/2021   
                 AND DSTR.ReferenceLevel = 'LoadReference_Claim'
                 AND DSTR.ReferenceType IN ( 'Claim Type', 'Claim Amount' ) -- 44(CL 320) 
       LEFT JOIN #LAST_LOCATION MA
              ON O.UPSOrderNumber = MA.UPSOrderNumber
--AND O.SourceSystemKey=MA.SourceSystemKey              
WHERE  
 ( Upper(O.AccountId) IN (SELECT
                                      Upper(DPProductLineKey)
                                    FROM   #ACCOUNTINFO)
              OR Upper(O.AccountId) = @VarAccountID )
AND O.IS_INBOUND = 0

       AND ( Isnull(O.OrderCancelledFlag, 'N') = 'N' ) --Optimization
       
       --AND ((UPPER(O.DP_SERVICELINE_KEY) IN (SELECT (DPServiceLineKey) FROM #ACCOUNTINFO) OR UPPER(O.DP_SERVICELINE_KEY) = @VarDPServiceLineKey)  OR @VarDPServiceLineKey = '*')    Optimization        
       AND ( O.FacilityId COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT
                                                                     WarehouseId
                                                                   FROM   #WarehouseId)
              OR @VarwarehouseId = '*' )
       AND ( ( O.DateTimeReceived BETWEEN @shipmentCreationStartDateTime AND @shipmentCreationEndDateTime )
              OR @NULLCreatedDate = '*' )
       AND ( ( O.OriginalScheduledDeliveryDateTime BETWEEN @ScheduledDeliveryStartDateTime AND @ScheduledDeliveryEndDateTime )
              OR @NULLScheduledDeliveryDate = '*' )
       AND ( ( O.DateTimeShipped BETWEEN @shippedStartDateTime AND @shippedEndDateTime )
              OR @NULLShippedDate = '*' )
       AND ( ( O.ScheduledPickUpDateTime BETWEEN @ScheduleToShipStartDateTime AND @ScheduleToShipEndDateTime )
              OR @NULLScheduleToShipDate = '*' )
       AND ( @NULLOriginCountry = '*'
              OR O.OriginCountry COLLATE SQL_Latin1_General_CP1_CI_AS = @VarOriginCountry )
       AND ( @NULLDestinationCountry = '*'
              OR O.DestinationCountry COLLATE SQL_Latin1_General_CP1_CI_AS = @VarDestinationCountry )
       AND ( @NULLOriginCity = '*'
              OR O.OriginCity COLLATE SQL_Latin1_General_CP1_CI_AS = @VarOriginCity )
       AND ( @NULLDestinationCity = '*'
              OR O.DestinationCity COLLATE SQL_Latin1_General_CP1_CI_AS = @VarDestinationCity )
       --AND COALESCE(O.OrderCancelledFlag,'N') <> 'Y'                
       --AND (O.OrderCancelledFlag <> 'Y' OR O.OrderCancelledFlag IS NULL)      --Commented for Optimization  
       --AND (@VarshipmentModeArray = '*' OR (COALESCE(O.ServiceMode,'NOMODE') COLLATE SQL_Latin1_General_CP1_CI_AS  IN (SELECT * FROM #SHIPMENTARRAY)))  
       --CL350
       AND ( @VarshipmentModeArray = '*'
              OR ( Isnull(O.ServiceMode, 'UNASSIGNED') COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT
                                                                                                  *
                                                                                                FROM   #SHIPMENTARRAY) ) )
       --CL350

	   --CL375
       AND ( @VarCarrierTypeArray = '*'
              OR ( COALESCE(O.Carrier, 'UNASSIGNED') COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT
                                                                                            *
                                                                                          FROM   #CarrierTypeARRAY) ) )
	   --CL375
       --AND (@VarserviceLevelArray = '*' OR (UPPER(COALESCE(O.ServiceLevel,'NOSERVICE')) COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT * FROM #SERVICELEVELARRAY)))    --12272  Optimization          
       AND ( ( estimatedDeliveryDateTime BETWEEN @DeliveryETAStartDateTime AND @DeliveryETAEndDateTime )
              OR @NULLDeliveryETADate = '*' )
       AND ( ( actualDeliveryDateTime BETWEEN @actualDeliveryStartDateTime AND @actualDeliveryEndDateTime )
              OR @NULLActualDeliveryDate = '*' )
    --CL375      
	   AND ( CASE
               WHEN Charindex(',', @originLocation) <> 0 THEN Concat(O.OriginCity COLLATE SQL_Latin1_General_CP1_CI_AS, ',', O.OriginCountry)
               ELSE
                 O.OriginCountry
             END IN (SELECT
                       OriginLocation
                     FROM   #OrginLocation)
              OR ( @NullOriginLocation = '*' ) ) --venkata
       AND ( CASE
               WHEN Charindex(',', @destinationLocation) <> 0 THEN Concat(O.DestinationCity COLLATE SQL_Latin1_General_CP1_CI_AS, ',', O.DestinationCountry)
               ELSE
                 O.DestinationCountry
             END IN (SELECT
                       DestinationLocation
                     FROM   #DestinationLocation)
              OR ( @NulldestinationLocation = '*' ) ) --venkata
	--CL375
       AND ( @VarisTemperatureTracked = '*'
              OR CASE
                   WHEN IS_TEMPERATURE = 'Y' THEN CC.IS_TEMPERATURE
                   ELSE
                     Isnull(CC.IS_TEMPERATURE, 'N')
                 END = @VarisTemperatureTracked )
		AND ((O.ShipmentBookedDate BETWEEN @bookedStartDateTime AND @bookedEndDateTime)  OR @NULLbookedDate = '*') --CL399
		AND (@NullOrderType = '*' OR UPPER(ISNULL(O.OrderType,'')) in (Select orderType from #OrderType)) --CL399
		AND ((CAST(O.IS_MANAGED AS VARCHAR(10)) = @varIsManaged) OR @NULLIsManaged = '*') --CL399
--OPTION(USE HINT('ENABLE_PARALLEL_PLAN_PREFERENCE'))


-- CL399
IF Object_id('tempdb..#DIGITAL_SUMMARY_MILESTONE_ACTIVITY') IS NOT NULL
  DROP TABLE #DIGITAL_SUMMARY_MILESTONE_ACTIVITY
  
SELECT MAX(MA.ActivityDate) PickUpDate ,MA.UPSOrderNumber
INTO #DIGITAL_SUMMARY_MILESTONE_ACTIVITY
FROM #DIGITAL_SUMMARY_ORDERS_DATASET O INNER JOIN Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) MA
ON O.shipmentNumber = MA.UPSOrderNumber
	AND O.SourceSystemKey = 1011  
    AND MA.ActivityCode in ('AM','AF','CP')
GROUP BY MA.UPSOrderNumber
-- CL399

--Main Result
IF Object_id('tempdb..#DIGITAL_SUMMARY_ORDERS_FILTER') IS NOT NULL
  DROP TABLE #DIGITAL_SUMMARY_ORDERS_FILTER 

SELECT
  O.*,MA.PickUpDate
INTO   #DIGITAL_SUMMARY_ORDERS_FILTER
FROM   #digital_summary_orders_dataset O (NOLOCK) 
		LEFT JOIN #DIGITAL_SUMMARY_MILESTONE_ACTIVITY MA ON O.shipmentNumber = MA.UPSOrderNumber
WHERE  ( ( Upper(DP_SERVICELINE_KEY) IN (SELECT
                                           ( DPServiceLineKey )
                                         FROM   #ACCOUNTINFO)
            OR Upper(DP_SERVICELINE_KEY) = @VarDPServiceLineKey )
          OR @VarDPServiceLineKey = '*' )
       AND ( @VarserviceLevelArray = '*'
              OR ( Upper(COALESCE(isShipmentServiceLevelResultSet, 'NOSERVICE')) COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT
                                                                                                                            *
                                                                                                                          FROM   #SERVICELEVELARRAY) ) ) --12272
	  AND ((MA.PickUpDate BETWEEN @pickupStartDateTime AND @pickupEndDateTime) OR @NULLpickupDate = '*') --CL399
	  AND (@NullTemperatureThreshold = '*' OR UPPER(O.TemperatureThreshold) in (Select TemperatureThreshold from #TemperatureThreshold)) --CL426
--INDEX
CREATE NONCLUSTERED INDEX [IDX_DIGITAL_SUMMARY_ORDERS_FILTER]
  ON #DIGITAL_SUMMARY_ORDERS_FILTER ([milestoneStatus])
  INCLUDE ([shipmentNumber], [upsTransportShipmentNumber], [deliveryStatus])

IF Object_id('tempdb..#DIGITAL_SUMMARY_ORDERS') IS NOT NULL
  DROP TABLE #DIGITAL_SUMMARY_ORDERS

SELECT
  DSO.*
INTO   #DIGITAL_SUMMARY_ORDERS
FROM   #DIGITAL_SUMMARY_ORDERS_FILTER (NOLOCK) DSO --NLHar
WHERE  ( @VarMilestoneStatus = '*'
          OR Upper(DSO.milestoneStatus) IN (SELECT
                                              MileStoneStatus
                                            FROM   #TmpMileStoneStatus) )
       AND ( @VardeliveryStatus = '*'
              OR Upper(DSO.deliveryStatus) IN (SELECT
                                                 DeliveryStatus
                                               FROM   #TmpDeliveryStatus) ) --UPSGLD-13112 
											   
/** CL399
SELECT  SOT.UPSOrderNumber, CASE WHEN Count(TRACKING_NUMBER) > 0 THEN 'Y' ELSE 'N' END AS carrierShipmentNumber
				INTO #TempCarrierShipmentNumber 
				FROM #DIGITAL_SUMMARY_ORDERS O 
					INNER JOIN Summary.DIGITAL_SUMMARY_ORDER_TRACKING SOT (NOLOCK)
					ON SOT.UPSOrderNumber = O.shipmentNumber AND SOT.SourceSystemKey = O.sourcesystemkey
				where SOT.TRACKING_NUMBER is not null
				GROUP BY SOT.UPSOrderNumber
CL399 **/

--CL275     ---10/22/2021          
--DECLARE @TotalCharge INT
IF Object_id('tempdb..#totalcharge') IS NOT NULL
  DROP TABLE #totalcharge

SELECT DISTINCT
  P.UPS_ORDER_NUMBER
  , P.SOURCE_SYSTEM_KEY
  , Sum(Cast(P.CHARGE AS DECIMAL(10, 2))) AS totalCharge
  , CURRENCY_CODE AS totalcurency
INTO   #totalcharge
FROM   dbo.FACT_TRANSPORTATION_RATES_CHARGES P (NOLOCK) --Added missing schema 02/11/2022
       JOIN dbo.DIM_CUSTOMER C (NOLOCK) --Added schema 02/11/2022
         ON P.CLIENT_KEY = C.CUSTOMERKEY
            AND P.SOURCE_SYSTEM_KEY = C.SOURCE_SYSTEM_KEY
WHERE  P.CHARGE_LEVEL = 'CUSTOMER_RATES'
       AND ( C.GLD_ACCOUNT_MAPPED_KEY IN (SELECT
                                            DPProductLineKey
                                          FROM   #ACCOUNTINFO)
              OR C.GLD_ACCOUNT_MAPPED_KEY = @VarAccountID ) --UPSGLD-11698  
--C.GLD_ACCOUNT_MAPPED_KEY = @VarAccountID            
GROUP  BY P.UPS_ORDER_NUMBER
          , P.SOURCE_SYSTEM_KEY
          , GLD_ACCOUNT_MAPPED_KEY
          , CURRENCY_CODE
--SET @TotalCharge = @@ROWCOUNT

IF Object_id('tempdb..#DELIVERY_STATUS_DETAILS') IS NOT NULL
  DROP TABLE #DELIVERY_STATUS_DETAILS

SELECT
  deliveryStatus AS DeliveryStatus
  , Count(*) AS DeliveryStatusCount
INTO   #DELIVERY_STATUS_DETAILS
FROM   (SELECT DISTINCT
          shipmentNumber
          , upsTransportShipmentNumber
          , deliveryStatus --11/11/2021
        FROM   #DIGITAL_SUMMARY_ORDERS_FILTER
        WHERE  milestoneStatus = 'DELIVERED') A
GROUP  BY deliveryStatus

-- FINAL RESULT SET                
IF @NULLDeliveryStatus = '*'
  BEGIN
      SELECT
        Count(*) AS totalShipments
      FROM   (SELECT DISTINCT
                shipmentNumber
                , upsTransportShipmentNumber
              FROM   #digital_summary_orders (NOLOCK) ----final   --NLHar             
              WHERE  ( @VarMilestoneStatus = '*'
                        OR Upper(milestoneStatus) IN (SELECT
                                                        MileStoneStatus
                                                      FROM   #TmpMileStoneStatus) )
                     AND ( @isClaim = '*'
                            OR ( ISCLAIM = @isClaim ) )) A--CL275            
  END
ELSE IF @VarMilestoneStatus LIKE '%DELIVERED%'
   AND @VardeliveryStatus <> '*'
  BEGIN
      SELECT
        Count(*) AS totalShipments
      FROM   (SELECT DISTINCT
                shipmentNumber
                , upsTransportShipmentNumber
              FROM   #digital_summary_orders (NOLOCK) ----final  --NLHar            
              WHERE  ( ( ( @VarMilestoneStatus = '*'
                            OR Upper(milestoneStatus) IN (SELECT
                                                            MileStoneStatus
                                                          FROM   #TmpMileStoneStatus) )
                         AND Upper(milestoneStatus) <> 'DELIVERED' )
                        --UPSGLD-13110
                        OR ( ( @VardeliveryStatus = '*'
                                OR Upper(deliveryStatus) IN (SELECT
                                                               DeliveryStatus
                                                             FROM   #TmpDeliveryStatus) )
                             AND Upper(deliveryStatus) <> 'DELIVERED' )-- @NULLVardeliveryStatus = ?
                      --or ((deliveryStatus= @VardeliveryStatus) AND UPPER(milestoneStatus)='DELIVERED')
                      )
                     AND ( @isClaim = '*'
                            OR ( ISCLAIM = @isClaim ) ))A --CL275            
  END
ELSE
  BEGIN
      SELECT
        Count(*) AS totalShipments
      FROM   (SELECT DISTINCT
                shipmentNumber
                , upsTransportShipmentNumber
              FROM   #digital_summary_orders (NOLOCK) ----final   --NLHar             
              WHERE  ( ( @VarMilestoneStatus = '*'
                          OR Upper(milestoneStatus) IN (SELECT
                                                          MileStoneStatus
                                                        FROM   #TmpMileStoneStatus) )
                        --UPSGLD-13110
                        OR ( @NULLDeliveryStatus = '*'
                              OR Upper(deliveryStatus) IN (SELECT
                                                             DeliveryStatus
                                                           FROM   #TmpDeliveryStatus) )
                      --OR ((deliveryStatus= @VardeliveryStatus) OR @NULLDeliveryStatus = '*')
                      ) ---new step              
                     AND ( @isClaim = '*'
                            OR ( ISCLAIM = @isClaim ) ))A --CL275            
  END
  
/***************************************************************************************************************************************************************************************************************************************************************************************/

IF @topRow = 0 -- No Charges and All records
BEGIN  
		SELECT DISTINCT
			shipmentNumber
			, referenceNumber
			, shipmentNumber AS upsShipmentNumber
			, referenceNumber AS clientShipmentNumber
			, customerPONumber
			, shipmentNumber AS orderNumber
			, upsTransportShipmentNumber
			, gffShipmentInstanceId
			, gffShipmentNumber
			, shipmentOrigin_contactName
			, shipmentOrigin_addressLine1
			, shipmentOrigin_addressLine2
			, shipmentOrigin_city
			, shipmentOrigin_stateProvince
			, shipmentOrigin_postalCode
			, shipmentOrigin_country
			, shipmentDestination_contactName
			, shipmentDestination_addressLine1
			, shipmentDestination_addressLine2
			, shipmentDestination_city
			, shipmentDestination_stateProvince
			, shipmentDestination_postalCode
			, shipmentDestination_country
			, shipmentDescription
			, Isnull(shipmentService,'UNASSIGNED') AS shipmentService
			, shipmentServiceLevel
			, shipmentServiceLevelCode
			, shipmentCarrierCode
			, Isnull(shipmentCarrier,'UNASSIGNED') AS shipmentCarrier
			, inventoryShipmentStatus
			, transportationMileStone
			, shipmentPrimaryException
			, shipmentBookedOnDateTime
			, shipmentCanceledDateTime
			, shipmentCanceledReason
			, actualShipmentDateTime
			, shipmentCreateOnDateTime
			, originalScheduledDeliveryDateTime
			, actualDeliveryDateTime
			, warehouseId
			, warehouseCode
			, milestoneStatus
			, estimatedDeliveryDateTime
			, referenceNumber1
			, referenceNumber2
			, referenceNumber3
			, referenceNumber4
			, referenceNumber5
			, shipmentDestination_locationCode
			, shipmentCreateOnDateTimeZone
			, originalScheduledDeliveryDateTimeZone
			, shippedDateTime
			, shippedDateTimeZone
			, dpProductLineKey
			, Accountnumber AS accountNumber
			, deliveryStatus
			, LoadID
			, isTemperatureTracked
			, isShipmentServiceLevelResultSet
			, latestTemperature
			, latestTemperatureInCelsius
			, latestTemperatureInFahrenheit
			, temperatureDateTime
			, temperatureCity
			, temperatureState
			, temperatureCountry
			,  P.totalCharge AS totalCharge					-- UPSGLD-13071
			, P.totalcurency AS totalChargeCurrency			-- UPSGLD-13071
			, isClaim
			, lastKnownLocation
			, IS_INBOUND AS isInbound
			-- CL399 --
			,'{"carrierShipmentNumber": ["Click For Details"]}' As carrierShipmentNumber
			, O.PickUpDate AS actualPickupDateTime
			, O.scheduledPickupDateTime
			, O.shipmentType
			-- CL399 --
			, O.TemperatureThreshold AS temperatureThreshold --CL426
  FROM   #digital_summary_orders O (NOLOCK)
  LEFT OUTER JOIN #totalcharge P ON O.upsTransportShipmentNumber = P.UPS_ORDER_NUMBER  -- UPSGLD-13071
  --LEFT OUTER JOIN #TempCarrierShipmentNumber C ON C.UPSOrderNumber = O.ShipmentNumber
  WHERE  ( ( ( @VarMilestoneStatus = '*'
                OR Upper(milestoneStatus) IN (SELECT
                                                MileStoneStatus
                                              FROM   #TmpMileStoneStatus) )
             AND @NULLDeliveryStatus = '*' )
            OR ( ( @VarMilestoneStatus = '*'
                    OR ( Upper(milestoneStatus) IN (SELECT
                                                      MileStoneStatus
                                                    FROM   #TmpMileStoneStatus)
                         AND @VarMilestoneStatus NOT LIKE '%DELIVERED%' ) )
                  OR ( deliveryStatus IN (SELECT
                                            DeliveryStatus
                                          FROM   #TmpDeliveryStatus) ) )
            OR ( ( @VarMilestoneStatus = '*'
                    OR ( UPPER(milestoneStatus) COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT
                                                                                    MileStoneStatus
                                                                                  FROM   #TmpMileStoneStatus)
                         AND Upper(milestoneStatus) <> 'DELIVERED' ) )
                  OR ( deliveryStatus IN (SELECT
                                            DeliveryStatus
                                          FROM   #TmpDeliveryStatus)
                       AND Upper(milestoneStatus) = 'DELIVERED' ) ) )
         AND ( @isClaim = '*'
                OR ( ISCLAIM = @isClaim ) )
END
ELSE
BEGIN     
		SELECT DISTINCT TOP (@topRow)
              shipmentNumber
              , referenceNumber
              , shipmentNumber AS upsShipmentNumber
              , referenceNumber AS clientShipmentNumber
              , customerPONumber
              , shipmentNumber AS orderNumber
              , upsTransportShipmentNumber
              , gffShipmentInstanceId
              , gffShipmentNumber
              , shipmentOrigin_contactName
              , shipmentOrigin_addressLine1
              , shipmentOrigin_addressLine2
              , shipmentOrigin_city
              , shipmentOrigin_stateProvince
              , shipmentOrigin_postalCode
              , shipmentOrigin_country
              , shipmentDestination_contactName
              , shipmentDestination_addressLine1
              , shipmentDestination_addressLine2
              , shipmentDestination_city
              , shipmentDestination_stateProvince
              , shipmentDestination_postalCode
              , shipmentDestination_country
              , shipmentDescription
			  , Isnull(shipmentService,'UNASSIGNED') AS shipmentService
              , shipmentServiceLevel
              , shipmentServiceLevelCode
              , shipmentCarrierCode
			  , Isnull(shipmentCarrier,'UNASSIGNED') AS shipmentCarrier
              , inventoryShipmentStatus
              , transportationMileStone
              , shipmentPrimaryException
              , shipmentBookedOnDateTime
              , shipmentCanceledDateTime
              , shipmentCanceledReason
              , actualShipmentDateTime
              , shipmentCreateOnDateTime
              , originalScheduledDeliveryDateTime
              , actualDeliveryDateTime
              , warehouseId
              , warehouseCode
              , milestoneStatus
              , estimatedDeliveryDateTime
              , referenceNumber1
              , referenceNumber2
              , referenceNumber3
              , referenceNumber4
              , referenceNumber5
              , shipmentDestination_locationCode
              , shipmentCreateOnDateTimeZone
              , originalScheduledDeliveryDateTimeZone
              , shippedDateTime
              , shippedDateTimeZone
              , dpProductLineKey
              , Accountnumber AS accountNumber
              , deliveryStatus
              , LoadID
              , isTemperatureTracked
              , isShipmentServiceLevelResultSet
              , --CL310
              latestTemperature
              , latestTemperatureInCelsius
              , latestTemperatureInFahrenheit
              , temperatureDateTime
              , temperatureCity
              , temperatureState
              , temperatureCountry
              , P.totalCharge AS totalCharge				-- UPSGLD-13071
			  , P.totalcurency AS totalChargeCurrency		-- UPSGLD-13071
              , --CL275  10/22/2021          
              isClaim
              , --CL275            
              lastKnownLocation
              , --CL282
              IS_INBOUND AS isInbound --CL367
			  -- CL399 --
			 ,'{"carrierShipmentNumber": ["Click For Details"]}' As carrierShipmentNumber
			 , O.PickUpDate AS  actualPickupDateTime
			 , O.scheduledPickupDateTime
			 , O.shipmentType
			-- CL399 --
			, TemperatureThreshold AS temperatureThreshold --CL426
            FROM   #digital_summary_orders (NOLOCK) O
			LEFT OUTER JOIN #totalcharge P ON O.upsTransportShipmentNumber = P.UPS_ORDER_NUMBER  -- UPSGLD-13071
			--LEFT OUTER JOIN #TempCarrierShipmentNumber C ON O.ShipmentNumber = C.UPSOrderNumber
            WHERE  ( ( ( @VarMilestoneStatus = '*'
                          OR Upper(milestoneStatus) IN (SELECT
                                                          MileStoneStatus
                                                        FROM   #TmpMileStoneStatus) )
                       AND @NULLDeliveryStatus = '*' )
                      OR ( ( @VarMilestoneStatus = '*'
                              OR ( Upper(milestoneStatus) IN (SELECT
                                                                MileStoneStatus
                                                              FROM   #TmpMileStoneStatus)
                                   AND @VarMilestoneStatus NOT LIKE '%DELIVERED%' ) )
                            OR ( deliveryStatus IN (SELECT
                                                      DeliveryStatus
                                                    FROM   #TmpDeliveryStatus) ) ) --UPSGLD-13110       
                      OR ( ( @VarMilestoneStatus = '*'
                              OR ( milestoneStatus COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT
                                                                                              MileStoneStatus
                                                                                            FROM   #TmpMileStoneStatus)
                                   AND Upper(milestoneStatus) <> 'DELIVERED' ) )
                            OR ( deliveryStatus IN (SELECT
                                                      DeliveryStatus
                                                    FROM   #TmpDeliveryStatus)
                                 AND Upper(milestoneStatus) = 'DELIVERED' ) ) )
                   --or ( (@VarMilestoneStatus = '*' OR ( UPPER(milestoneStatus)  IN  ( select MileStoneStatus from #TmpMileStoneStatus )  AND @VarMilestoneStatus NOT LIKE '%DELIVERED%')) or (deliveryStatus= @VardeliveryStatus))        
                   --or  ( (@VarMilestoneStatus = '*' OR (milestoneStatus COLLATE SQL_Latin1_General_CP1_CI_AS IN ( select MileStoneStatus from #TmpMileStoneStatus ) AND UPPER(milestoneStatus) <>'DELIVERED')) OR (deliveryStatus= @VardeliveryStatus AND  UPPER(milestoneStatus) ='DELIVERED')))                
                   AND ( @isClaim = '*'
                          OR ( ISCLAIM = @isClaim ) )
END
		


-- RESULT SET 2                
SELECT
  milestoneStatus AS MilestoneStatus
  , Count(*) AS MilestoneStatusCount --11/11/2021
FROM   (SELECT DISTINCT
          shipmentNumber
          , upsTransportShipmentNumber
          , milestoneStatus
        FROM   #digital_summary_orders_filter (NOLOCK) ----final --NLHar               
       --WHERE (@NULLDeliveryStatus='*' or DeliveryStatus=@VardeliveryStatus)     --UPSGLD-12954           
       )A
GROUP  BY milestoneStatus --CL275                
-- RESULT SET 3                
SELECT
  *
FROM   #delivery_status_details (NOLOCK) --NLHar

--SELECT DeliveryStatus,                
--COUNT(shipmentNumber)  AS DeliveryStatusCount                
--FROM #DIGITAL_SUMMARY_ORDERS                
--WHERE (@NULLDeliveryStatus='*' or DeliveryStatus=@VardeliveryStatus )    
--GROUP BY DeliveryStatus  
-- RESULT SET 5
--CL310
IF @isShipmentServiceLevelResultSet = 'Y'
  BEGIN
      SELECT
        shipmentServiceLevel
        , Count(*) AS shipmentServiceLevelCount
      FROM   (SELECT DISTINCT
                shipmentNumber
                , upsTransportShipmentNumber
                , shipmentServiceLevel --11/11/2021
              FROM   #digital_summary_orders_dataset O LEFT JOIN #DIGITAL_SUMMARY_MILESTONE_ACTIVITY MA --Optimization 
			  ON O.ShipmentNumber = MA.UPSOrderNumber
			  WHERE ((MA.PickUpDate BETWEEN @pickupStartDateTime AND @pickupEndDateTime) OR @NULLpickupDate = '*') 
			  ) A --CL399
      --FROM #DIGITAL_SUMMARY_ORDERS_FILTER_SERVICELEVEL_RESULTSET5 ) A
      GROUP  BY shipmentServiceLevel
  END
ELSE
  SELECT
    NULL AS ShipmentServiceLevel
    , NULL shipmentServiceLevelCount
--CL310


-- RESULT SET 6

--SELECT '' AS temperatureThreshold 

SELECT DISTINCT O.temperatureThreshold --CL426
              FROM   #digital_summary_orders_dataset O LEFT JOIN #DIGITAL_SUMMARY_MILESTONE_ACTIVITY MA --Optimization 
			  ON O.ShipmentNumber = MA.UPSOrderNumber
			  WHERE ((MA.PickUpDate BETWEEN @pickupStartDateTime AND @pickupEndDateTime) OR @NULLpickupDate = '*')
			  AND O.temperatureThreshold is NOT NULL

END
GO

