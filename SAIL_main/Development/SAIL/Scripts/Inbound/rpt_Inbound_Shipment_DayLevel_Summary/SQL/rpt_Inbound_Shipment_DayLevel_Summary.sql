/****** Object:  StoredProcedure [digital].[rpt_Inbound_Shipment_DayLevel_Summary]    Script Date: 2/22/2022 2:33:07 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





/**--------------------------CHANGE LOG---------------------------------------------------------------------------------------------
Developer			Date				Sprint				Changea
Revathy				11/11/2021			GLD-12013           Added  GROUP BY O.UPSOrderNumber & Max(actualDeliveryDateTime)
Manju				02/10/2022			49-CL388			ADDED parameter (isManaged = Y/N/'*')
-------------------------------------------------------------------------------------------------------------------------------**/


/**** 

--AMR
  EXEC [digital].[rpt_Inbound_Shipment_DayLevel_Summary]   @DPProductLineKey = 'E648FA6F-6253-428E-8AC9-201E3EF83B91',@DPServiceLineKey = '53D60776-D3BD-4E6A-8E37-F591C148294B',@DPEntityKey = NULL,@StartDate = '2020-08-08',@EndDate = '2020-08-14', @warehouseId = '*',@DateType=NULL
--SWR
  EXEC [digital].[rpt_Inbound_Shipment_DayLevel_Summary]   @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = '*',@DPEntityKey = NULL,@StartDate = '2020-08-20',@EndDate = '2020-11-18', @warehouseId = '*',@DateType='shipmentCreationDate',@inboundType = 'TRANSPORT ORDER'
  EXEC [digital].[rpt_Inbound_Shipment_DayLevel_Summary]   @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = '150E5128-B3E3-44A1-BF6C-CD2E4D9856DA',@DPEntityKey = NULL,@StartDate = '2020-10-20',@EndDate = '2020-11-18', @warehouseId = '*',@DateType='shipmentDeliveryDate',@inboundType = 'TRANSPORT ORDER'
  --Cambium
  EXEC [digital].[rpt_Inbound_Shipment_DayLevel_Summary]   @DPProductLineKey = 'A10F512B-C7F4-42A0-909C-21DD95A7D921',@DPServiceLineKey = '53DDDBC2-FB5A-4E70-A92E-8312DC9FDD05',@DPEntityKey = NULL,@StartDate = '2020-08-08',@EndDate = '2020-08-14', @warehouseId = '*',@DateType=''
  --SWAROVSKI
EXEC [digital].[rpt_Inbound_Shipment_DayLevel_Summary]  --CL388
@DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',
@DPServiceLineKey = '*',
@DPEntityKey = NULL,
@StartDate = '2021-02-08',
@EndDate = '2022-02-11', 
@warehouseId = '*',
@DateType=NULL,
@IsManaged='N'
****/

CREATE PROCEDURE [digital].[rpt_Inbound_Shipment_DayLevel_Summary] 

@DPProductLineKey varchar(50), 
@DPServiceLineKey varchar(50), 
@DPEntityKey varchar(50) = NULL, 
@StartDate date = NULL,
@EndDate date = NULL,
@DateType varchar(50)=NULL, 
@warehouseId varchar(max) = NULL,
@inboundType varchar(50)=NULL,
@Date nvarchar(max) = NULL,
@IsManaged varchar(10)=NULL        --CL388

AS

BEGIN

  DECLARE @VarAccountID varchar(50),
          @VarDPServiceLineKey varchar(50),
          @VarDPEntityKey varchar(50),
          @VarStartCreatedDateTime datetime,
          @VarEndCreatedDateTime datetime,
          @NULLCreatedDate varchar(1),
          @NULLDeliveryDate varchar(1),
          @VarwarehouseId varchar(max),
		  @VarInboundType varchar(50),
		  @NULLInboundType varchar(1),
		  @isASN           INT,
		  @varDateType     varchar(50),
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
		  @varIsManaged VARCHAR(10), --CL388
		  @NULLIsManaged VARCHAR(10) --CL388



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
		  @pickupStartDate			  = pickupStartDate,
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

  SET @VarAccountID = UPPER(@DPProductLineKey)
  SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)
  SET @VarDPEntityKey = UPPER(@DPEntityKey)
  SET @VarwarehouseId = UPPER(@warehouseId)
  SET @VarStartCreatedDateTime = @StartDate
  SET @VarEndCreatedDateTime = @EndDate
  SET @VarEndCreatedDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @VarEndCreatedDateTime)))
  SET @VarInboundType=UPPER(@inboundType)
  SET @varDateType=UPPER(@DateType)
  SET @varIsManaged = CASE WHEN UPPER(@IsManaged) = 'Y' THEN '1' 
						   WHEN UPPER(@IsManaged) = 'N' THEN '0' ELSE @IsManaged END --CL388
 -- SET @varDateType=UPPER(isnull(@DateType,'SHIPMENTCREATIONDATE'))

  IF @DPServiceLineKey IS NULL
    SET @VarDPServiceLineKey = '*'

  IF @DPEntityKey IS NULL
    SET @VarDPEntityKey = '*'

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

  IF @VarInboundType IS NULL OR @VarInboundType = ''
     SET @NULLInboundType = '*'
  ELSE
   SET @isASN = CASE WHEN @VarInboundType='ASN' THEN 1
                     WHEN @VarInboundType='TRANSPORT ORDER' THEN 0 
                END

  IF ISNULL(@varIsManaged,'')='' OR @varIsManaged = '*'--CL388
	SET @NULLIsManaged = '*'


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
set @shipmentCreationStartDateTime=@StartDate
set @shipmentCreationEndDateTime=DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @endDate)))
set @NULLCreatedDate = ''
end
else IF @Date is NULL and UPPER(@DateType)='SHIPMENTDELIVERYDATE'
begin
set @actualDeliveryStartDateTime=@StartDate
set @actualDeliveryEndDateTime=DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @endDate)))
set @NULLActualDeliveryDate = ''
end


SELECT
    CAST(O.DateTimeReceived as date) as ShipmentCreationDate,
    CAST(O.ActualDeliveryDate as date) as ShipmentDeliveryDate,
	(SELECT MAX(ma.ActivityDate) FROM Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) ma
									  WHERE O.UPSOrderNumber = ma.UPSOrderNumber
									  AND O.SourceSystemKey = ma.SourceSystemKey
									        AND ma.ActivityCode IN('AG','AB','AA')
									  
	  ) AS estimatedDeliveryDateTime,
	  (SELECT MAX(ma.ActivityDate) FROM Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) ma
									  WHERE O.UPSOrderNumber = ma.UPSOrderNumber
									  AND O.SourceSystemKey = ma.SourceSystemKey
									  AND ma.ActivityCode in('D','D1','D9') 
	) AS actualDeliveryDateTime,
	(SELECT MAX(ma.ActivityDate) FROM Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) ma
									  WHERE O.UPSOrderNumber = ma.UPSOrderNumber
									  AND O.SourceSystemKey = 1011
									  AND ma.ActivityCode in('AM','AF','CP') 
	) AS PickUpDate,
	O.ShipmentBookedDate as shipmentBookedOnDateTime,
	O.DateTimeReceived,
	O.ActualDeliveryDate,
	O.UPSOrderNumber,
	O.AccountId,
	O.DP_SERVICELINE_KEY,
	O.ScheduledPickUpDateTime,
	O.FacilityId,
	O.DateTimeShipped,
	O.IS_INBOUND,
	O.IS_ASN,
	O.SourceSystemKey
  INTO #DIGITAL_SUMMARY_ORDERS
  FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O
  LEFT JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION (NOLOCK) ST  ON   O.UPSOrderNumber=ST.UpsOrderNumber
  WHERE O.AccountId = @VarAccountID
  AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')
  --AND (ISNULL(O.DP_ORGENTITY_KEY, '') = @VarDPEntityKey OR @VarDPEntityKey = '*')
  AND (O.FacilityId IN (SELECT UPPER (TRIM (value)) FROM string_split(@VarwarehouseId, ',')) OR @VarwarehouseId = '*')
  
  --AND ((O.ActualDeliveryDate  BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime)  OR @NULLDeliveryDate = '*')
  AND ((ST.LoadLatestDeliveryDate            BETWEEN @ScheduledDeliveryStartDateTime AND @ScheduledDeliveryEndDateTime) OR @NULLScheduledDeliveryDate = '*')
  AND ((O.DateTimeShipped                    BETWEEN @shippedStartDateTime           AND @shippedEndDateTime          ) OR @NULLShippedDate = '*')
  AND ((O.ScheduledPickUpDateTime            BETWEEN @ScheduleToShipStartDateTime    AND @ScheduleToShipEndDateTime   ) OR @NULLScheduleToShipDate = '*') 
  AND O.IS_INBOUND=1
  AND (@NULLInboundType='*' OR ISNULL(O.IS_ASN,0) = @isASN)
  AND ((CAST(O.IS_MANAGED AS VARCHAR(10)) = @varIsManaged) OR @NULLIsManaged = '*') --CL388


IF @varDateType = 'SHIPMENTCREATIONDATE' and @NULLActualDeliveryDate = '*'
  SELECT
    ShipmentCreationDate,
    ShipmentDeliveryDate
  INTO #DIGITAL_SUMMARY_ORDERS_FILTER
  FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) O
  WHERE 
   ((O.DateTimeReceived  BETWEEN @shipmentCreationStartDateTime AND @shipmentCreationEndDateTime) OR @NULLCreatedDate = '*')
  AND ((estimatedDeliveryDateTime  BETWEEN @DeliveryETAStartDateTime AND @DeliveryETAEndDateTime) OR @NULLDeliveryETADate = '*')
  AND ((actualDeliveryDateTime        BETWEEN @actualDeliveryStartDateTime    AND @actualDeliveryEndDateTime   ) OR @NULLActualDeliveryDate = '*')
  AND ((PickUpDate                    BETWEEN @pickupStartDateTime            AND @pickupEndDateTime          )  OR @NULLpickupDate = '*')
  AND ((shipmentBookedOnDateTime              BETWEEN @bookedStartDateTime            AND @bookedEndDateTime          )  OR @NULLbookedDate = '*')

  
  
  

  IF @varDateType = 'SHIPMENTDELIVERYDATE' AND @NULLCreatedDate = '*'
  SELECT
    Max(CAST(O.actualDeliveryDateTime AS DATE)) AS ShipmentDeliveryDate,--CL388
	O.UPSOrderNumber
  INTO #DIGITAL_SUMMARY_ORDERS_D
  FROM  #DIGITAL_SUMMARY_ORDERS (NOLOCK) O
  JOIN Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) FMA ON O.UPSOrderNumber = FMA.UPSOrderNumber
                                                              AND O.SourceSystemKey = FMA.SourceSystemKey
  JOIN master_data.Map_Milestone_Activity MMA (NOLOCK) ON MMA.ActivityName = FMA.ActivityName
                                                      AND MMA.SOURCE_SYSTEM_KEY = FMA.SourceSystemKey
  WHERE 
  -- ((FMA.ActivityDate  BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime)  OR @NULLDeliveryDate = '*')
  --AND  
  MMA.ActivityCode IN ('D','D1','D9')
  AND ((estimatedDeliveryDateTime  BETWEEN @DeliveryETAStartDateTime AND @DeliveryETAEndDateTime) OR @NULLDeliveryETADate = '*')
  AND ((actualDeliveryDateTime        BETWEEN @actualDeliveryStartDateTime    AND @actualDeliveryEndDateTime   ) OR @NULLActualDeliveryDate = '*')
  AND ((PickUpDate                    BETWEEN @pickupStartDateTime            AND @pickupEndDateTime          )  OR @NULLpickupDate = '*')
  AND ((shipmentBookedOnDateTime              BETWEEN @bookedStartDateTime            AND @bookedEndDateTime          )  OR @NULLbookedDate = '*')
  GROUP BY O.UPSOrderNumber  --GLD12013 

  IF @varDateType = 'SHIPMENTCREATIONDATE'  AND @NULLActualDeliveryDate = '*'
  SELECT
    COUNT(1) Total
  FROM #DIGITAL_SUMMARY_ORDERS_FILTER (NOLOCK)
  
  IF @varDateType = 'SHIPMENTDELIVERYDATE' AND @NULLCreatedDate = '*'
  SELECT
    COUNT(1) Total
  FROM #DIGITAL_SUMMARY_ORDERS_D (NOLOCK)

  IF @varDateType = 'SHIPMENTCREATIONDATE'  AND @NULLActualDeliveryDate = '*'
    SELECT
      ShipmentCreationDate,
      COUNT(ShipmentCreationDate) AS ShipmentCreationDateCount
    FROM #DIGITAL_SUMMARY_ORDERS_FILTER (NOLOCK)
    WHERE ShipmentCreationDate IS NOT NULL
    GROUP BY ShipmentCreationDate
    ORDER BY ShipmentCreationDate

  IF @varDateType = 'SHIPMENTDELIVERYDATE' AND @NULLCreatedDate = '*'
    SELECT
      ShipmentDeliveryDate,
      COUNT(ShipmentDeliveryDate) AS ShipmentDeliveryDateCount
    FROM #DIGITAL_SUMMARY_ORDERS_D (NOLOCK)
    --WHERE ShipmentDeliveryDate IS NOT NULL
    GROUP BY ShipmentDeliveryDate
    ORDER BY ShipmentDeliveryDate

END
GO

