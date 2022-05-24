--AUTHOR : 		VISHAL SHARMA
--DESCRIPTION:	rpt_Outbound_Shipment_Summary
--DATE : 		31-03-2022

Note : 

preprocess some variable based on the storeproc

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

IF ISNULL(@shipmentshippedStartDatetime,'')='' OR ISNULL(@shipmentshippedENDDate,'')=''  
    SET @NULLshipmentshippedDate = '*' 

    IF Isnull(@DateType,'')=''  
    begin   
    if @NULLCreatedDate = '*'   
    begin  
    set  @varDateType = 'SHIPMENTSHIPPEDDATE'   
    end  
    if @NULLshipmentshippedDate = '*'  
    begin  
    set @varDateType = 'SHIPMENTCREATIONDATE'    
    end  
    end 
 
    IF Isnull(@Date,'') = '' and UPPER(@DateType)='SHIPMENTCREATIONDATE'  
    begin  
    set @Date.shipmentCreationStartDateTime=@StartCreatedDate  
    set @Date.shipmentCreationEndDateTime=@EndCreatedDate
    set @NULLCreatedDate = ''  
    end  
    else IF  Isnull(@Date,'') = '' and UPPER(@DateType)='SHIPMENTSHIPPEDDATE'  
    begin  
    set @Date.shipmentshippedStartDatetime=@StartCreatedDate  
    set @Date.shipmentshippedENDDate=EndCreatedDate
    set @NULLshipmentshippedDate = ''  
    end 

Result Set 1
CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*' 
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate , @Date.shipmentCreationEndDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional
 
SELECT count(1) Total
FROM (
	SELECT DISTINCT T.UPSOrderNumber
		,T.upsTransportShipmentNumber  FROM T
	WHERE 
		(T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate)))
		AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
			OR T.AccountId =@DPProductLineKey)
		AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
			OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
		AND T.FacilityId IN (@warehouseId)   
		AND (
			T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate	
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
			)
		AND (
			T.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate     
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
			)
		AND (
			T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     
			AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
			)
		AND T.IS_INBOUND = 0
	
		AND T.is_deleted = 0
		AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
		AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes
	)

CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*'  
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentshippedStartDate , @Date.shipmentshippedENDDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional

SELECT count(1) Total
FROM (
    SELECT DISTINCT T.UPSOrderNumber
        ,T.upsTransportShipmentNumber  FROM T
    WHERE
        (T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate)))
        AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
            OR T.AccountId =@DPProductLineKey)
        AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
        AND T.FacilityId IN (@warehouseId)   AND (
            T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (
            T.actualShipmentDateTime_main BETWEEN @Date.shipmentshippedStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentshippedENDDate))
            )
        AND (
            T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes
    )

Result Set 2
 CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*' 
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate , @Date.shipmentCreationEndDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional

SELECT a.milestoneStatus, count(1) milestoneStatusCount from
(select Distinct T.UPSOrderNumber, T.upsTransportShipmentNumber ,T.milestoneStatus
from T
where T.milestoneStatus != null
        AND T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
            OR T.AccountId =@DPProductLineKey)
        AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
        AND T.FacilityId IN (@warehouseId)  
        AND (
            T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (
            T.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
            )
        AND (
            T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes
		) a
        group by a.milestoneStatus

 CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*'  
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentshippedStartDate , @Date.shipmentshippedENDDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional

SELECT a.milestoneStatus, count(1) milestoneStatusCount from
(select Distinct T.UPSOrderNumber, T.upsTransportShipmentNumber ,T.milestoneStatus
from T
where T.milestoneStatus != null
        AND T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
            OR T.AccountId =@DPProductLineKey)
        AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
        AND T.FacilityId IN (@warehouseId)  
        AND (
            T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (
            T.actualShipmentDateTime_main BETWEEN @Date.shipmentshippedStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentshippedENDDate))
            )
        AND (
            T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes
		) a
        group by a.milestoneStatus

 Result Set 3
 CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*'
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate , @Date.shipmentCreationEndDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional
 
SELECT  a.ShipmentMode,count(1) ShipmentModeCount from
(select Distinct T.UPSOrderNumber, T.upsTransportShipmentNumber ,T.ShipmentMode
from T
where T.ShipmentMode != null
       AND T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
       AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
           OR T.AccountId =@DPProductLineKey)
       AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
           OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
       AND T.FacilityId IN (@warehouseId)  
       AND (
           T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate
               AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
           )
       AND (
           T.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
           )
       AND (
           T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
           )
       AND T.IS_INBOUND = 0
       AND T.is_deleted = 0
       AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
       AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
	   AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes
	   )  a
       group by a.ShipmentMode

CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*' 
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentshippedStartDate , @Date.shipmentshippedENDDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional

SELECT  a.ShipmentMode,count(1) ShipmentModeCount from
 (select Distinct T.UPSOrderNumber, T.upsTransportShipmentNumber ,T.ShipmentMode
 from T
 where T.ShipmentMode != null
        AND T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
            OR T.AccountId =@DPProductLineKey)
        AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
        AND T.FacilityId IN (@warehouseId)  
        AND (
            T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (
            T.actualShipmentDateTime_main BETWEEN @Date.shipmentshippedStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentshippedENDDate))
            )
        AND (
            T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes
		)a
        group by a.ShipmentMode

Result Set 4
CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*' 
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate , @Date.shipmentCreationEndDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional

SELECT   a.OriginCountry,count(1) OriginCountryCount from
(select Distinct T.UPSOrderNumber, T.upsTransportShipmentNumber ,T.OriginCountry
from T
where T.OriginCountry != null
        AND T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
            OR T.AccountId =@DPProductLineKey)
        AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
        AND T.FacilityId IN (@warehouseId)  
        AND (
            T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (
            T.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
            )
        AND (
            T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes
		)  a
        group by a.OriginCountry

CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*' 
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentshippedStartDate , @Date.shipmentshippedENDDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional
		
SELECT   a.OriginCountry,count(1) OriginCountryCount from
(select Distinct T.UPSOrderNumber, T.upsTransportShipmentNumber ,T.OriginCountry
from T
where T.OriginCountry != null
        AND T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
            OR T.AccountId =@DPProductLineKey)
        AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
        AND T.FacilityId IN (@warehouseId)  
        AND (
            T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (
            T.actualShipmentDateTime_main BETWEEN @Date.shipmentshippedStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentshippedENDDate))
            )
        AND (
            T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes
		)a
        group by a.OriginCountry

Result Set 5
 CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*' 		
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate , @Date.shipmentCreationEndDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional
 
  SELECT   a.DestinationCountry,count(1) DestinationCountryCount from
(select Distinct T.UPSOrderNumber, T.upsTransportShipmentNumber ,T.DestinationCountry
from T
where T.DestinationCountry != null
        AND T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
            OR T.AccountId =@DPProductLineKey)
        AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
        AND T.FacilityId IN (@warehouseId)  
        AND (
            T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (
            T.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
            )
        AND (
            T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes
		)a
        group by a.DestinationCountry 

CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*' 
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentshippedStartDate , @Date.shipmentshippedENDDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional
		
SELECT   a.DestinationCountry,count(1) DestinationCountryCount from
(select Distinct T.UPSOrderNumber, T.upsTransportShipmentNumber ,T.DestinationCountry
from T
where T.DestinationCountry != null
        AND T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
            OR T.AccountId =@DPProductLineKey)
        AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
        AND T.FacilityId IN (@warehouseId)  
        AND (
            T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (
            T.actualShipmentDateTime_main BETWEEN @Date.shipmentshippedStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentshippedENDDate))
            )
        AND (
            T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes
		)a
        group by a.DestinationCountry

Result Set 6
 CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*' 		
 --* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate , @Date.shipmentCreationEndDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional

SELECT   a.ServiceLevel,count(1) serviceLevelCount from
(select Distinct T.UPSOrderNumber, T.upsTransportShipmentNumber ,T.ServiceLevel
from T
where T.ServiceLevel != null
        AND T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
            OR T.AccountId =@DPProductLineKey)
        AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
        AND T.FacilityId IN (@warehouseId)  
        AND (
            T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (
            T.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
            )
        AND (
            T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes
		)  a
        group by a.ServiceLevel

CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*' 		
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentshippedStartDate , @Date.shipmentshippedENDDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional

SELECT   a.ServiceLevel,count(1) serviceLevelCount from
(select Distinct T.UPSOrderNumber, T.upsTransportShipmentNumber ,T.ServiceLevel from T
where T.ServiceLevel != null
        AND T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
            OR T.AccountId =@DPProductLineKey)
        AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
        AND T.FacilityId IN (@warehouseId)  
        AND (
            T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (
            T.actualShipmentDateTime_main BETWEEN @Date.shipmentshippedStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentshippedENDDate))
            )
        AND (
            T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes
		)a
        group by a.ServiceLevel

Result Set 7
 CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*'		
 --* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate , @Date.shipmentCreationEndDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional

SELECT  count(1) TemperatureShipmentCount, a.ShipmentMode from 
(select Distinct T.UPSOrderNumber ,T.ShipmentMode
from T
WHERE T.IS_TEMPERATURE='Y'     
AND T.STATUSDETAILTYPE='TemperatureTracking'
AND T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
            OR T.AccountId =@DPProductLineKey)
        AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
        AND T.FacilityId IN (@warehouseId)  
        AND (
            T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (
            T.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
            )
        AND (
            T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes
		) a
        group by a.ShipmentMode  
		
CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*'		
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentshippedStartDate , @Date.shipmentshippedENDDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional

SELECT  count(1) TemperatureShipmentCount, a.ShipmentMode from 
(select Distinct T.UPSOrderNumber ,T.ShipmentMode from T
WHERE T.IS_TEMPERATURE='Y'     
AND T.STATUSDETAILTYPE='TemperatureTracking'
        AND T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
            OR T.AccountId =@DPProductLineKey)
        AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
        AND T.FacilityId IN (@warehouseId)  
        AND (
            T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (
            T.actualShipmentDateTime_main BETWEEN @Date.shipmentshippedStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentshippedENDDate))
            )
        AND (
            T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes
		)a
        group by a.ShipmentMode
				
Result Set 8
 CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*'
 --* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate , @Date.shipmentCreationEndDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional

-- MS Team :Need to have a look: Issue:TOP 5 AND ORDER BY need to cater
--select top 5 * from ( to be done in BACKEND
        SELECT  T.Carrier
        ,T.deliveryStatus DeliveryStatus
        ,COUNT(1) AS Count
    FROM T
    WHERE
        T.milestoneStatus = 'DELIVERED'
        AND T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
            OR T.AccountId =@DPProductLineKey)
        AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
        AND T.FacilityId IN (@warehouseId)  
        AND (
            T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (
            T.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
            )
        AND (
            T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes
        GROUP BY T.Carrier
        ,T.deliveryStatus 
--Order by Count DESC to be done in Backend

CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*'	
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentshippedStartDate , @Date.shipmentshippedENDDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional
 
-- MS Team :Need to have a look: Issue:TOP 5 AND ORDER BY need to cater
--select top 5 * from ( to be done in BACKEND
SELECT  T.Carrier
        ,T.deliveryStatus DeliveryStatus
        ,COUNT(1) AS Count
    FROM T
    WHERE
        T.milestoneStatus = 'DELIVERED'
        AND T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
            OR T.AccountId =@DPProductLineKey)
        AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
        AND T.FacilityId IN (@warehouseId)  
        AND (
            T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (
            T.actualShipmentDateTime_main BETWEEN @Date.shipmentshippedStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentshippedENDDate))
            )
        AND (
            T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes
        GROUP BY T.Carrier
        ,T.deliveryStatus
--Order by Count DESC to be done in Backend 

Result Set 9
-- MS Team :Need to have a look: Issue:TOP 5 AND ORDER BY need to cater
 
CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*'
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate , @Date.shipmentCreationEndDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional
  
--select top 5 * from ( to be done in BACKEND
  SELECT UPPER(T.OriginCity) SourceCity
        ,UPPER(T.DestinationCity) DestinationCity
        ,UPPER(T.OriginCountry) SourceCountry
        ,UPPER(T.DestinationCountry) DestinationCountry
        ,UPPER(T.deliveryStatus) DeliveryStatus
        ,COUNT(1) Count
    FROM T
    WHERE
        (UPPER(T.OriginCity) NOT LIKE '%NOT AVAILABLE%'
        AND UPPER(T.DestinationCity) NOT LIKE '%NOT AVAILABLE%')
        AND T.milestoneStatus = 'DELIVERED'
         
        AND T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
            OR T.AccountId =@DPProductLineKey)
        AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
        AND T.FacilityId IN (@warehouseId)  
        AND (
            T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (
            T.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
            )
        AND (
            T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
        AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes 
    GROUP BY UPPER(T.OriginCity)
        ,UPPER(T.DestinationCity)
        ,UPPER(T.OriginCountry)
        ,UPPER(T.DestinationCountry)
        ,UPPER(T.deliveryStatus)
--Order by Count DESC to be done in Backend

CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*'
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentshippedStartDate , @Date.shipmentshippedENDDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional

-- MS Team :Need to have a look: Issue:TOP 5 AND ORDER BY need to cater

--select top 5 * from ( to be done in BACKEND
SELECT UPPER(T.OriginCity) SourceCity
        ,UPPER(T.DestinationCity) DestinationCity
        ,UPPER(T.OriginCountry) SourceCountry
        ,UPPER(T.DestinationCountry) DestinationCountry
        ,UPPER(T.deliveryStatus) DeliveryStatus
        ,COUNT(1) Count
    FROM T
    WHERE
        (UPPER(T.OriginCity) NOT LIKE '%NOT AVAILABLE%'
        AND UPPER(T.DestinationCity) NOT LIKE '%NOT AVAILABLE%')
        AND T.milestoneStatus = 'DELIVERED'
         
        AND T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
            OR T.AccountId =@DPProductLineKey)
        AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
        AND T.FacilityId IN (@warehouseId)  
        AND (
            T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (
            T.actualShipmentDateTime_main BETWEEN @Date.shipmentshippedStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentshippedENDDate))
            )
        AND (
            T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
        AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes 
    GROUP BY UPPER(T.OriginCity)
        ,UPPER(T.DestinationCity)
        ,UPPER(T.OriginCountry)
        ,UPPER(T.DestinationCountry)
        ,UPPER(T.deliveryStatus)
--Order by Count DESC to be done in Backend

Result Set 10	
CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*'
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate , @Date.shipmentCreationEndDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional

SELECT   
a.ShipmentMode,   
a.deliveryStatus DeliveryStatus,   
COUNT(1) AS Count FROM(
SELECT distinct T.ShipmentMode,T.deliveryStatus,T.UPSOrderNumber,T.upsTransportShipmentNumber 
FROM  T   
WHERE T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
            OR T.AccountId =@DPProductLineKey)
        AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
        AND T.FacilityId IN (@warehouseId)  
        AND (
            T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (
            T.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
            )
        AND (
            T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
		AND T.milestoneStatus = 'DELIVERED'
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes
)a
GROUP BY    
a.ShipmentMode,   
a.deliveryStatus
--ORDER BY [Count] DESC to be Applied at Backend
        
CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*'
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentshippedStartDate , @Date.shipmentshippedENDDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional

SELECT   
a.ShipmentMode,   
a.deliveryStatus DeliveryStatus,   
COUNT(1) AS Count FROM(
SELECT distinct T.ShipmentMode,T.deliveryStatus,T.UPSOrderNumber,T.upsTransportShipmentNumber  
FROM  T   
WHERE T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
            OR T.AccountId =@DPProductLineKey)
        AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
        AND T.FacilityId IN (@warehouseId)  
        AND (
            T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (
            T.actualShipmentDateTime_main BETWEEN @Date.shipmentshippedStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentshippedENDDate))
            )
        AND (
            T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
        AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes 
        AND T.milestoneStatus = 'DELIVERED'
)a
GROUP BY    
a.ShipmentMode,   
a.deliveryStatus
--ORDER BY Count DESC to be Applied at Backend

Result Set 11	
CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*'
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate , @Date.shipmentCreationEndDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional

SELECT
    T.Carrier,
    COUNT(1) CarrierCount
	 --Sprint 52 Changes T.TemperatureThreshold removed
 FROM T
WHERE  T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
            OR T.AccountId =@DPProductLineKey)
        AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
        AND T.FacilityId IN (@warehouseId)  
        AND (
            T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (
            T.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
            )
        AND (
            T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y' 
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes
        AND T.Carrier != null
  GROUP BY T.Carrier
		    --Sprint 52 Changes : T.TemperatureThreshold removed

CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*'
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentshippedStartDate , @Date.shipmentshippedENDDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional

SELECT
    T.Carrier,
    COUNT(1) CarrierCount
	 --Sprint 52 Changes T.TemperatureThreshold removed
 FROM T
WHERE  T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
            OR T.AccountId =@DPProductLineKey)
        AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
        AND T.FacilityId IN (@warehouseId)  
        AND (
            T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (
            T.actualShipmentDateTime_main BETWEEN @Date.shipmentshippedStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentshippedENDDate))
            )
        AND (
            T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes
        AND T.Carrier != null
		
  GROUP BY T.Carrier
		    --Sprint 52 Changes : T.TemperatureThreshold removed
  
Result Set 12	
CASE 1: IF @shipmentCreationStartDateTime IS NOT NULL
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate , @Date.shipmentCreationEndDate are optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,

SELECT COUNT(1) as ScheduleToShipCount FROM T 
              WHERE T.milestoneStatus IN ('TRANSPORTATION PLANNING')
              AND (
                    T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
                    ) 
              AND (
                    T.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
                    )
              AND T.IS_INBOUND = 0
              AND T.is_deleted = 0
              AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
                OR T.AccountId =@DPProductLineKey)
         
              AND (IS_null(T.OrderStatusName) ? '' :T.OrderStatusName) <> 'Cancelled' 
              AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
			  
			  
CASE 2: ELSE	
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentshippedStartDate , @Date.shipmentshippedENDDate are optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,

SELECT COUNT(1) as ScheduleToShipCount FROM T 
              WHERE T.milestoneStatus IN ('TRANSPORTATION PLANNING')
              AND (
                    T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate      
                    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
                    )              
              AND (
                    T.actualShipmentDateTime_main BETWEEN @Date.shipmentshippedStartDate
                    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentshippedENDDate))
                    )
              AND T.IS_INBOUND = 0
                 AND T.is_deleted = 0
              AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
                OR T.AccountId =@DPProductLineKey)
         
              AND (IS_null(T.OrderStatusName) ? '' :T.OrderStatusName) <> 'Cancelled' 
              AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'

Result Set 13
CASE 1: IF @shipmentCreationStartDateTime IS NOT NULL
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate , @Date.shipmentCreationEndDate are optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,

SELECT COUNT(1) as MissedPickupCount FROM T 
              WHERE T.milestoneStatus IN ('TRANSPORTATION PLANNING')
              AND (
                    T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate      
                    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
                    ) 
              AND (
                    T.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate      
                    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
                    )
              AND T.IS_INBOUND = 0
              AND T.is_deleted = 0
              AND (T.AccountId IN (@AccountKeys.DPProductLineKey) OR T.AccountId =@DPProductLineKey)
              AND (IS_null(T.OrderStatusName) ? '' :T.OrderStatusName) <> 'Cancelled' 
              AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
	
CASE 2: ELSE		
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentshippedStartDate , @Date.shipmentshippedENDDate are optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,

SELECT COUNT(1) as MissedPickupCount FROM T 
              WHERE T.milestoneStatus IN ('TRANSPORTATION PLANNING')
              AND (
                    T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate      
                    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
                    )              
              AND (
                    T.actualShipmentDateTime_main BETWEEN @Date.shipmentshippedStartDate
                    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentshippedENDDate))
                    )            
              AND T.IS_INBOUND = 0
            AND T.is_deleted = 0
              AND (T.AccountId IN (@AccountKeys.DPProductLineKey) OR T.AccountId =@DPProductLineKey)
              AND (IS_null(T.OrderStatusName) ? '' :T.OrderStatusName) <> 'Cancelled' 
              AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'

Result Set 14
CASE 1: IF @Date.shipmentCreationStartDateTime IS NOT NULL
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.scheduleDeliveryStartDate ,@Date.scheduleDeliveryEndDate are optional,
-- @Date.shipmentCreationStartDate , @Date.shipmentCreationEndDate are optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,

SELECT SUM(T.MissedDeliveredCount) AS ScheduledToDeliverCount FROM T
             WHERE 
             (
                   T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate      
                   AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
                   )
             AND (
                   T.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate      
                   AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
                   )
             AND  T.IS_INBOUND = 0
             AND T.is_deleted = 0
             AND (T.AccountId IN (@AccountKeys.DPProductLineKey) OR T.AccountId =@DPProductLineKey)
               AND T.MissedDeliveredCount>0            

CASE 2 ELSE 
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.scheduleDeliveryStartDate ,@Date.scheduleDeliveryEndDate are optional,
-- @Date.shipmentshippedStartDate , @Date.shipmentshippedENDDate are optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,

SELECT SUM(T.MissedDeliveredCount) AS  ScheduledToDeliverCount FROM T
             WHERE  
             (
                   T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate      
                   AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
                   )             
             AND (
                   T.actualShipmentDateTime_main BETWEEN @Date.shipmentshippedStartDate
                   AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentshippedENDDate))
                   )             
             AND  T.IS_INBOUND = 0
             AND T.is_deleted = 0
             AND (T.AccountId IN (@AccountKeys.DPProductLineKey) OR T.AccountId =@DPProductLineKey)
           AND T.MissedDeliveredCount>0            

Result Set 15
CASE 1: IF @shipmentCreationStartDateTime IS NOT NULL
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.scheduleDeliveryStartDate ,@Date.scheduleDeliveryEndDate are optional,
-- @Date.shipmentCreationStartDate , @Date.shipmentCreationEndDate are optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,

SELECT SUM(T.MissedDeliveredCount) AS MissedDeliveredCount FROM T
           WHERE 
           (
                 T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate      
                 AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
                 )
           AND (
                 T.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate      
                 AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
                 )
           AND  T.IS_INBOUND = 0
           AND T.is_deleted = 0
           AND (T.AccountId IN (@AccountKeys.DPProductLineKey) OR T.AccountId =@DPProductLineKey)
             AND T.MissedDeliveredCount>0

 CASE 2: ELSE		
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.scheduleDeliveryStartDate ,@Date.scheduleDeliveryEndDate are optional,
-- @Date.shipmentshippedStartDate , @Date.shipmentshippedENDDate are optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,

SELECT SUM(T.MissedDeliveredCount) AS MissedDeliveredCount FROM T
             WHERE  
             (
                   T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate      
                   AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
                   )             
             AND (
                   T.actualShipmentDateTime_main BETWEEN @Date.shipmentshippedStartDate
                   AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentshippedENDDate))
                   )             
             AND  T.IS_INBOUND = 0
             AND T.is_deleted = 0
             AND (T.AccountId IN (@AccountKeys.DPProductLineKey) OR T.AccountId =@DPProductLineKey)
           AND T.MissedDeliveredCount>0          

Result Set 16  --Sprint 52 Changes	
CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*'
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentCreationStartDate , @Date.shipmentCreationEndDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional

  SELECT
    T.Carrier carrier,
    COUNT(1) count,
	T.TemperatureThreshold  temperatureThreshold  
 FROM T
WHERE  T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
            OR T.AccountId =@DPProductLineKey)
        AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
        AND T.FacilityId IN (@warehouseId)  
        AND (
            T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (
            T.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
            )
        AND (
            T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y' 
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes
        AND T.Carrier != null
  GROUP BY T.Carrier,
		    T.TemperatureThreshold 

CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*'
--* Target Container-digital_summary_orders

--* Parameter requirement info.
-- @Date.actualDeliveryStartDate ,@Date.actualDeliveryEndDate optional,
-- @AccountKeys.DPProductLineKey or @DPProductLineKey required,
-- @AccountKeys.DPServiceLineKey or @DPServiceLineKey optional,
-- @warehouseId optional,
-- @Date.scheduleToShipStartDate ,@Date.scheduleToShipEndDate are optional,
-- @Date.shipmentshippedStartDate , @Date.shipmentshippedENDDate are optional,
-- @Date.scheduleDeliveryStartDate, @Date.scheduleDeliveryEndDate are optional
-- @isManaged  optional
-- @OrderType optional

  SELECT
    T.Carrier carrier,
    COUNT(1) count,
	T.TemperatureThreshold  temperatureThreshold  
 FROM T
WHERE  T.actualDeliveryDateTime BETWEEN @Date.actualDeliveryStartDate    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.actualDeliveryEndDate))
        AND (T.AccountId IN (@AccountKeys.DPProductLineKey)
            OR T.AccountId =@DPProductLineKey)
        AND (T.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
            OR T.DP_SERVICELINE_KEY = @DPServiceLineKey)
        AND T.FacilityId IN (@warehouseId)  
        AND (
            T.ScheduledPickUpDateTime BETWEEN @Date.scheduleToShipStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleToShipEndDate))
            )
        AND (
            T.actualShipmentDateTime_main BETWEEN @Date.shipmentshippedStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentshippedENDDate))
            )
        AND (
            T.LoadLatestDeliveryDate BETWEEN @Date.scheduleDeliveryStartDate     AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.scheduleDeliveryEndDate))
            )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER(@isManaged) = 'Y' ? 1 : (UPPER(@isManaged) = 'N' ? 0 : @isManaged)) = T.is_managed
		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN (@OrderType)) --Sprint 52 Changes
        AND T.Carrier != null
  GROUP BY T.Carrier,
		   T.TemperatureThreshold  		 