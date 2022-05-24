/****** Object:  StoredProcedure [digital].[rpt_Outbound_Open_Exception_Shipments]    Script Date: 12/13/2021 5:44:14 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO







 /***
CHANGE LOG
----------
CHANGED BY				DATE			SPRINT       CHANGES
ARUP 					10/22/2021		41			 Add latestTemperatureInCelsius and latestTemperatureInFahrenheit
PRASHANT RAI            09/30/2021      38th         TEMPERATURE RELATED COLUMNS ARE ADDED
Sheetal			        11/02/2021		UPSGLD-11872  Added NULL value for latestTemperatureInCelsius and latestTemperatureInFahrenheit in final query
Revathy                 11/08/2021                    Add latestTemperatureInCelsius & latestTemperatureInFahrenheit in response

***/


/**** 
--AMR
EXEC [digital].[rpt_Outbound_Open_Exception_Shipments]   @DPProductLineKey = 'E648FA6F-6253-428E-8AC9-201E3EF83B91',@DPServiceLineKey = '53D60776-D3BD-4E6A-8E37-F591C148294B',@DPEntityKey = NULL,@StartDate = NULL,@EndDate = NULL, @warehouseid='*', @topRow=0,@Sortby = ''
--SWR																																																							   
EXEC [digital].[rpt_Outbound_Open_Exception_Shipments]   @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = '150E5128-B3E3-44A1-BF6C-CD2E4D9856DA',@DPEntityKey = NULL,@StartDate = '2021-01-01',@EndDate = '2021-01-29', @warehouseid='*', @topRow=0,@Sortby = ''
--Cambium																																																						   
EXEC [digital].[rpt_Outbound_Open_Exception_Shipments]   @DPProductLineKey = 'A10F512B-C7F4-42A0-909C-21DD95A7D921',@DPServiceLineKey = '53DDDBC2-FB5A-4E70-A92E-8312DC9FDD05',@DPEntityKey = NULL,@StartDate = NULL,@EndDate = NULL, @warehouseid='*', @topRow=0,@Sortby = ''
****/

CREATE PROCEDURE [digital].[rpt_Outbound_Open_Exception_Shipments] 

  @DPProductLineKey  varchar(50)
, @DPServiceLineKey  varchar(50)
, @DPEntityKey       varchar(50)
, @StartDate        nvarchar(max)=NULL
, @EndDate          nvarchar(max)=NULL
, @warehouseId      nvarchar(max)='*'
, @topRow            int
, @Sortby            varchar(50)= NULL
,@isTemperatureTracked VARCHAR(1) = '*'


AS

BEGIN

  DECLARE @VarAccountID varchar(50),
          @VarDPServiceLineKey varchar(50),
          @VarDPEntityKey varchar(50),
		  @VarStartCreatedDateTime datetime,
          @VarEndCreatedDateTime datetime,
          @NULLCreatedDate varchar(1),
          @VarwarehouseId varchar(max),
		  @VarisTemperatureTracked CHAR(1)
          

  SET @VarAccountID = UPPER(@DPProductLineKey)
  SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)
  SET @VarDPEntityKey = UPPER(@DPEntityKey)
  SET @VarisTemperatureTracked=UPPER(@isTemperatureTracked)
  SET @VarwarehouseId = UPPER(@warehouseId)
  SET @VarStartCreatedDateTime = @startDate
  SET @VarEndCreatedDateTime = @endDate
  SET @VarEndCreatedDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @VarEndCreatedDateTime)))

  IF OBJECT_ID('tempdb..#MAX_EXCEPTION') IS NOT NULL
	DROP TABLE #MAX_EXCEPTION

   CREATE TABLE #MAX_EXCEPTION(ExceptionCreatedDate datetime,UPSOrderNumber varchar(128),SourceSystemKey int)

   IF OBJECT_ID('tempdb..#MAX_ACTIVITY') IS NOT NULL
	DROP TABLE #MAX_ACTIVITY

	CREATE TABLE #MAX_ACTIVITY(ActivityDate datetime,UPSOrderNumber varchar(128),SourceSystemKey int)

  IF @VarStartCreatedDateTime IS NULL OR @VarEndCreatedDateTime IS NULL
    SET @NULLCreatedDate = '*'

  IF ISNULL(@VarisTemperatureTracked,'')=''
    SET @VarisTemperatureTracked = '*'
 
  IF @DPServiceLineKey IS NULL
    SET @VarDPServiceLineKey = '*'

  IF @DPEntityKey IS NULL
    SET @VarDPEntityKey = '*'

  SELECT
    O.[OrderNumber] referenceNumber,
	O.[UPSOrderNumber] upsShipmentNumber,
	O.SourceSystemKey,
	--'exceptionType' AS exceptionType,
    --O.ExceptionCode AS exceptionReason,
	O.CurrentMilestone AS milestoneStatus,
	O.OriginAddress1 AS shipmentOrigin_addressLine1,
    O.OriginAddress2 AS shipmentOrigin_addressLine2,
    O.OriginCity AS shipmentOrigin_city,
    O.OriginProvince AS shipmentOrigin_stateProvince,
    O.OriginPostalCode AS shipmentOrigin_postalCode,
    O.OriginCountry AS shipmentOrigin_country,
    O.DestinationAddress1 AS shipmentDestination_addressLine1,
    O.DestinationAddress2 AS shipmentDestination_addressLine2,
    O.DestinationCity AS shipmentDestination_city,
    O.DestinationProvince AS shipmentDestination_stateProvince,
    O.DestinationPostalcode AS shipmentDestination_postalCode,
    O.DestinationCountry AS shipmentDestination_country,
	O.ServiceLevel shipmentServiceLevel,
    O.[ServiceLevelCode] shipmentServiceLevelCode,
	O.CarrierCode shipmentCarrierCode,
	O.Carrier shipmentCarrier,
	O.FacilityId AS warehouseId,
    O.ActualShipmentDateTime AS actualShipmentDateTime,
    O.DateTimeReceived AS shipmentPlaceDateTime,
    O.OriginalScheduledDeliveryDateTime AS originalScheduledDeliveryDateTime,
    O.ActualScheduledDeliveryDateTime AS actualScheduledDeliveryDateTime,
	O.UPSTransportShipmentNumber AS UPSTransportShipmentNumber
	,CC.IS_TEMPERATURE AS isTemperatureTracked
	,CC.LATEST_TEMPERATURE AS latestTemperature
	,CC.TemperatureC AS latestTemperatureInCelsius
   	,CC.TemperatureF As latestTemperatureInFahrenheit
	,CC.TEMPERATURE_DATETIME AS temperatureDateTime
	,CC.TEMPERATURE_CITY AS temperatureCity
	,CC.TEMPERATURE_STATE AS temperatureState
	,CC.TEMPERATURE_COUNTRY AS temperatureCountry
	INTO #DIGITAL_SUMMARY_ORDERS
  FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O
  LEFT JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION_CALLCHECK (NOLOCK) CC
          ON O.UPSOrderNumber=CC.UPSORDERNUMBER
		  AND O.SourceSystemKey=CC.SOURCESYSTEMKEY
  WHERE O.AccountId = @VarAccountID
  AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')
  --AND (ISNULL(O.DP_ORGENTITY_KEY, '') = @VarDPEntityKey OR @VarDPEntityKey = '*')
  AND (O.FacilityId COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (VALUE)FROM string_split(@VarwarehouseId, ',')) OR @VarwarehouseId = '*')
  AND ((O.DateTimeReceived BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime) OR @NULLCreatedDate = '*')
  AND O.IS_INBOUND = 0
  --AND (@VarisTemperatureTracked='*' OR CC.IS_TEMPERATURE=@VarisTemperatureTracked)
  AND (@VarisTemperatureTracked='*' OR CASE WHEN IS_TEMPERATURE = 'Y' THEN CC.IS_TEMPERATURE ELSE ISNULL(CC.IS_TEMPERATURE,'N') END = @VarisTemperatureTracked)

   INSERT INTO #MAX_EXCEPTION
   SELECT 
   MAX(EX.OTZ_ExceptionCreatedDate) AS ExceptionCreatedDate
  ,O.UPSTransportShipmentNumber AS UPSOrderNumber
  ,EX.SourceSystemKey
  FROM #DIGITAL_SUMMARY_ORDERS O (NOLOCK)
  INNER JOIN Summary.DIGITAL_SUMMARY_EXCEPTIONS EX (NOLOCK)
  --INNER JOIN Summary.DIGITAL_SUMMARY_EXCEPTIONS EX (NOLOCK)
            ON O.UPSTransportShipmentNumber=EX.UPSOrderNumber
			--AND O.SourceSystemKey=EX.SourceSystemKey
  GROUP BY 
   O.UPSTransportShipmentNumber
  ,EX.SourceSystemKey
 /* ////// ADDED THIS BLOCK TO HANDLE SOURCE SYSTEM 1019///////// */

  UNION
     
     SELECT 
   MAX(EX.UTC_ExceptionCreatedDate) AS ExceptionCreatedDate
  ,O.upsShipmentNumber AS UPSOrderNumber
  ,EX.SourceSystemKey
  FROM #DIGITAL_SUMMARY_ORDERS O (NOLOCK)
  INNER JOIN Summary.DIGITAL_SUMMARY_EXCEPTIONS EX (NOLOCK)
  --INNER JOIN Summary.DIGITAL_SUMMARY_EXCEPTIONS EX (NOLOCK)
            ON O.upsShipmentNumber =EX.UPSOrderNumber
 where EX.SourceSystemKey=1019
  GROUP BY 
   O.upsShipmentNumber
  ,EX.SourceSystemKey




   INSERT INTO #MAX_ACTIVITY
   SELECT 
   MAX(ActivityDate) AS ActivityDate
  ,O.upsShipmentNumber AS UPSOrderNumber
  ,O.SourceSystemKey
  FROM #DIGITAL_SUMMARY_ORDERS O (NOLOCK)
  LEFT JOIN Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY MA (NOLOCK)
            ON O.upsShipmentNumber=MA.UPSOrderNumber
			AND O.SourceSystemKey=MA.SourceSystemKey
			AND MA.AccountId=@VarAccountID
  WHERE O.upsShipmentNumber NOT IN (SELECT MA1.UpsOrderNumber FROM Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) MA1 WHERE MA1.AccountId=@VarAccountID AND MA1.ActivityCode  IN('D','D1','D9','DELIVER') AND MA1.UpsOrderNumber = O.upsShipmentNumber)
  GROUP BY 
   O.upsShipmentNumber
  ,O.SourceSystemKey

  CREATE CLUSTERED INDEX [Ix_ClIndexMAX_ACTIVITY] ON #MAX_ACTIVITY([UPSOrderNumber] ASC,[SourceSystemKey] ASC,ActivityDate ASC )WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]




  SELECT O.*,TblException.exceptionReason ,exceptionType
  into #DIGITAL_SUMMARY_ORDERS_FINAL
  FROM #DIGITAL_SUMMARY_ORDERS O
  INNER JOIN (SELECT ex.UPSOrderNumber,ex.SourceSystemKey,ex.OTZ_ExceptionCreatedDate,ex.ExceptionReason as exceptionReason,MMA.ActivityName as exceptionType FROM #MAX_EXCEPTION tblMaxException 
                       INNER JOIN Summary.DIGITAL_SUMMARY_EXCEPTIONS ex (NOLOCK)
					   ON tblMaxException.UPSOrderNumber=ex.UPSOrderNumber
					   AND tblMaxException.SourceSystemKey=ex.SourceSystemKey
					   AND tblMaxException.ExceptionCreatedDate=ex.OTZ_ExceptionCreatedDate
					   left JOIN master_data.Map_Milestone_Activity (NOLOCK) MMA ------changed it from inner join as we dont have exception event
					       ON MMA.ActivityCode = EX.ExceptionEvent
		     )as TblException ON O.UPSTransportShipmentNumber=TblException.UPSOrderNumber 
			       
  INNER JOIN #MAX_ACTIVITY as MA ON O.upsShipmentNumber=MA.UPSOrderNumber
			        AND O.SourceSystemKey=MA.SourceSystemKey
					where TblException.SourceSystemKey<>1019  ------TO HANDLE SOURCE SYSTEM LOGIC
 /* ////// ADDED THIS BLOCK TO HANDLE SOURCE SYSTEM 1019///////// */
	insert into #DIGITAL_SUMMARY_ORDERS_FINAL
	SELECT O.*,TblException.exceptionReason ,exceptionType
  FROM #DIGITAL_SUMMARY_ORDERS O
  INNER JOIN (SELECT ex.UPSOrderNumber,ex.SourceSystemKey,ex.OTZ_ExceptionCreatedDate,ex.ExceptionReason as exceptionReason,MMA.ActivityName as exceptionType FROM #MAX_EXCEPTION tblMaxException 
                       INNER JOIN Summary.DIGITAL_SUMMARY_EXCEPTIONS ex (NOLOCK)
					   ON tblMaxException.UPSOrderNumber=ex.UPSOrderNumber
					   AND tblMaxException.SourceSystemKey=ex.SourceSystemKey
					   AND tblMaxException.ExceptionCreatedDate=ex.OTZ_ExceptionCreatedDate
					   left JOIN master_data.Map_Milestone_Activity (NOLOCK) MMA
					       ON MMA.ActivityCode = EX.ExceptionEvent
		     )as TblException ON O.upsShipmentNumber=TblException.UPSOrderNumber 
			       
  INNER JOIN #MAX_ACTIVITY as MA ON O.upsShipmentNumber=MA.UPSOrderNumber
			        AND O.SourceSystemKey=MA.SourceSystemKey
					where TblException.SourceSystemKey=1019   ------TO HANDLE SOURCE SYSTEM LOGIC
  
  

  SELECT COUNT(*) AS totalShipments from #DIGITAL_SUMMARY_ORDERS_FINAL

  IF @topRow = 0

    SELECT
    referenceNumber,
	upsShipmentNumber,
	exceptionType,
    exceptionReason,
	milestoneStatus,
	shipmentOrigin_addressLine1       AS shipmentOrigin__addressLine1,
    shipmentOrigin_addressLine2       AS shipmentOrigin__addressLine2,
    shipmentOrigin_city               AS shipmentOrigin__city,
    shipmentOrigin_stateProvince      AS shipmentOrigin__stateProvince,
    shipmentOrigin_postalCode         AS shipmentOrigin__postalCode,
    shipmentOrigin_country            AS shipmentOrigin__country,
    shipmentDestination_addressLine1  AS shipmentDestination__addressLine1,
    shipmentDestination_addressLine2  AS shipmentDestination__addressLine2,
    shipmentDestination_city          AS shipmentDestination__city,
    shipmentDestination_stateProvince AS shipmentDestination__stateProvince,
    shipmentDestination_postalCode    AS shipmentDestination__postalCode,
    shipmentDestination_country       AS shipmentDestination__country,
	shipmentServiceLevel,
    shipmentServiceLevelCode,
	shipmentCarrierCode,
	shipmentCarrier,
	warehouseId,
    actualShipmentDateTime,
    shipmentPlaceDateTime,
    originalScheduledDeliveryDateTime,
    actualScheduledDeliveryDateTime
   ,isTemperatureTracked
   ,latestTemperature
   ,latestTemperatureInCelsius
   ,latestTemperatureInFahrenheit
   ,temperatureDateTime
   ,temperatureCity
   ,temperatureState
   ,temperatureCountry
	FROM #DIGITAL_SUMMARY_ORDERS_FINAL
	ORDER BY shipmentPlaceDateTime DESC
  ELSE

    SELECT TOP (@topRow) 
    referenceNumber,
	upsShipmentNumber,
	exceptionType,
    exceptionReason,
	milestoneStatus,
	shipmentOrigin_addressLine1       AS shipmentOrigin__addressLine1,
    shipmentOrigin_addressLine2       AS shipmentOrigin__addressLine2,
    shipmentOrigin_city               AS shipmentOrigin__city,
    shipmentOrigin_stateProvince      AS shipmentOrigin__stateProvince,
    shipmentOrigin_postalCode         AS shipmentOrigin__postalCode,
    shipmentOrigin_country            AS shipmentOrigin__country,
    shipmentDestination_addressLine1  AS shipmentDestination__addressLine1,
    shipmentDestination_addressLine2  AS shipmentDestination__addressLine2,
    shipmentDestination_city          AS shipmentDestination__city,
    shipmentDestination_stateProvince AS shipmentDestination__stateProvince,
    shipmentDestination_postalCode    AS shipmentDestination__postalCode,
    shipmentDestination_country       AS shipmentDestination__country,
	shipmentServiceLevel,
    shipmentServiceLevelCode,
	shipmentCarrierCode,
	shipmentCarrier,
	warehouseId,
    actualShipmentDateTime,
    shipmentPlaceDateTime,
    originalScheduledDeliveryDateTime,
    actualScheduledDeliveryDateTime
   ,isTemperatureTracked
   ,latestTemperature
   ,latestTemperatureInCelsius
   ,latestTemperatureInFahrenheit
   ,temperatureDateTime
   ,temperatureCity
   ,temperatureState
   ,temperatureCountry
    FROM #DIGITAL_SUMMARY_ORDERS_FINAL
    ORDER BY shipmentPlaceDateTime DESC

END
GO

