/****** Object:  StoredProcedure [digital].[rpt_Warehouse_Performance]    Script Date: 1/6/2022 1:16:38 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






/** -------------------------------CHANGE LOG----------------------------------------
DEVELOPER			DATE			SPRINT				COMMENTS
HARSHA				12/08/2021		UPSGLD-12562		Removed string_split from the where clause and created the #temp table

--------------------------------------------------------------------------------------**/



/**** 
--AMR
EXEC [digital].[rpt_warehouse_performance]    @DPProductLineKey = 'E648FA6F-6253-428E-8AC9-201E3EF83B91',@DPServiceLineKey = '53D60776-D3BD-4E6A-8E37-F591C148294B',@DPEntityKey = NULL,@Date = NULL,@warehouse=NULL,@ShipmentType=NULL
--SWR
EXEC [digital].[digital].[rpt_warehouse_performance]    @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = '150E5128-B3E3-44A1-BF6C-CD2E4D9856DA',@DPEntityKey = NULL,@Date = NULL,@warehouse=NULL,@ShipmentType=NULL
--Cambium
EXEC [digital].[rpt_warehouse_performance]    @DPProductLineKey = 'A10F512B-C7F4-42A0-909C-21DD95A7D921',@DPServiceLineKey = '53DDDBC2-FB5A-4E70-A92E-8312DC9FDD05',@DPEntityKey = NULL,@Date = NULL,@warehouse=NULL,@ShipmentType=NULL
****/

CREATE PROCEDURE [digital].[rpt_Warehouse_Performance]

@DPProductLineKey varchar(50),
@DPServiceLineKey varchar(50),
@DPEntityKey varchar(50),
@Date nvarchar(max),
@warehouse nvarchar(max)='*',
@ShipmentType varchar(200) = '*',
@topRow int= 10,
@sortBy varchar(200)= '*'


AS

BEGIN
DECLARE  @VarAccountID VARCHAR(50)
		,@VarDPServiceLineKey VARCHAR(50)
		,@VarDPEntityKey VARCHAR(50)
		,@receivedStartDate DATE
		,@receivedStartDateTime datetime
		,@receivedEndDate DATE
		,@receivedEndDateTime datetime
		,@NULLreceivedDate varchar(1)
		,@shippedStartDate DATE
		,@shippedStartDateTime datetime
		,@shippedEndDate DATE
		,@shippedEndDateTime datetime
		,@NULLshippedDate varchar(1)
		,@VarwarehouseIds varchar(max)
		,@VarwarehouseNames varchar(max)
		,@VarsortBy varchar(50)
		,@VarShipmentType varchar(200)
		,@NULLShipmentType varchar(1)

 IF @warehouse IS NULL or @warehouse='*'
 BEGIN
  SET @VarwarehouseIds='*'
  SET @VarwarehouseNames='*'
  END
  else
BEGIN
select @VarwarehouseIds=JSON_QUERY(@warehouse,  '$.warehouseID')
select @VarwarehouseNames=JSON_QUERY(@warehouse,'$.warehouseName')
end	  

SELECT @VarwarehouseIds=REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@VarwarehouseIds, CHAR(13), ''), CHAR(10), ''),'"',''),'[',''),']',''),
       @VarwarehouseNames=REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@VarwarehouseNames, CHAR(13), ''), CHAR(10), ''),'"',''),'[',''),']','')

  SET @VarwarehouseIds=REPLACE(@VarwarehouseIds,' ','')
  SET @VarwarehouseNames=REPLACE(@VarwarehouseNames,' ','')
  SET @VarShipmentType = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@ShipmentType, CHAR(13), ''), CHAR(10), ''),'"',''),'[',''),']','')

		
SELECT    @receivedStartDate =  receivedStartDate,
          @receivedEndDate   =  receivedEndDate,
		  @shippedStartDate	 =  shippedStartDate, 
		  @shippedEndDate	 =  shippedEndDate
FROM OPENJSON(@Date)
WITH (
	receivedStartDate date,
	receivedEndDate date,
	shippedStartDate date,
	shippedEndDate date
    )
		
SET @VarAccountID = UPPER(@DPProductLineKey)
SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)
SET @VarDPEntityKey = UPPER(@DPEntityKey)
SET @VarsortBy = UPPER(@sortBy)

IF @DPServiceLineKey IS NULL
	SET @VarDPServiceLineKey = '*'

IF @DPEntityKey IS NULL
	SET @VarDPEntityKey = '*'


 IF @sortBy IS NULL or @sortBy='*'
  SET @VarsortBy = 'VOLUME DESC'

 IF @ShipmentType IS NULL
  SET @VarShipmentType = '*'

  IF @VarwarehouseIds IS NULL
     SET @VarwarehouseIds = '*'

  IF @VarwarehouseNames IS NULL
     SET @VarwarehouseNames = '*'

IF ISNULL(@receivedStartDate,'')='' OR ISNULL(@receivedEndDate,'')=''
  SET @NULLreceivedDate = '*'

IF ISNULL(@shippedStartDate,'')='' OR ISNULL(@shippedEndDate,'')=''
  SET @NULLshippedDate = '*'

-- Loading data based on Accoun ID

 ------UPSGLD-12562---------

SELECT TRIM (value) AS FacilityId
INTO #VarwarehouseIds
FROM string_split(@VarwarehouseIds, ',');

SELECT TRIM (value) AS OrderWarehouse
INTO #VarwarehouseNames
FROM string_split(@VarwarehouseNames, ',');

----- UPSGLD-12562---------------

  SELECT 
  UPSOrderNumber,
	AccountId,
	FacilityId,
	IS_INBOUND,
	IS_ASN
INTO #tmp_ASN_Summary_details
FROM [Summary].[DIGITAL_SUMMARY_ORDERS] O (NOLOCK)
WHERE AccountId = @VarAccountID
	AND (DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')
	--AND (ISNULL(DP_ORGENTITY_KEY, '') = @VarDPEntityKey OR @VarDPEntityKey = '*')
	AND ( (O.DateTimeReceived BETWEEN @receivedStartDate AND @receivedEndDate )OR @NULLreceivedDate = '*' )
	--AND ( O.DateTimeShipped BETWEEN  @shippedStartDate AND @shippedEndDate   OR @NULLshippedDate = '*' )
	--AND ( O.FacilityId IN (SELECT TRIM (value) FROM string_split(@VarwarehouseIds, ',')) OR @VarwarehouseIds = '*')
    --AND ( O.OrderWarehouse IN (SELECT TRIM (value) FROM string_split(@VarwarehouseNames, ',')) OR @VarwarehouseNames = '*')
	AND ( O.FacilityId IN (SELECT FacilityId FROM #VarwarehouseIds) OR @VarwarehouseIds = '*')									--UPSGLD-12562
    AND ( O.OrderWarehouse IN (SELECT OrderWarehouse FROM #VarwarehouseNames) OR @VarwarehouseNames = '*')						--UPSGLD-12562
    
--GROUP BY 
--	AccountId,
--	FacilityId,
--	IS_INBOUND,
--	IS_ASN,
--	UPSOrderNumber



 SELECT 
  UPSOrderNumber,
	AccountId,
	FacilityId,
	IS_INBOUND,
	IS_ASN
INTO #tmp_Outbound_Summary_details
FROM [Summary].[DIGITAL_SUMMARY_ORDERS] O (NOLOCK)
WHERE AccountId = @VarAccountID
	AND (DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')
	--AND (ISNULL(DP_ORGENTITY_KEY, '') = @VarDPEntityKey OR @VarDPEntityKey = '*')
	--AND ( O.DateTimeReceived BETWEEN @receivedStartDate AND @receivedEndDate OR @NULLreceivedDate = '*' )
	AND ( (O.DateTimeShipped BETWEEN  @shippedStartDate AND @shippedEndDate)   OR @NULLshippedDate = '*' )
	--AND ( O.FacilityId  COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarwarehouseIds, ',') )   OR @VarwarehouseIds = '*')
    --AND ( O.OrderWarehouse  COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarwarehouseNames, ',')) OR @VarwarehouseNames = '*')
	AND ( O.FacilityId  COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT FacilityId FROM #VarwarehouseIds )   OR @VarwarehouseIds = '*')									--UPSGLD-12562
    AND ( O.OrderWarehouse  COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT OrderWarehouse FROM #VarwarehouseNames) OR @VarwarehouseNames = '*')						--UPSGLD-12562
	
--GROUP BY 
--	AccountId,
--	FacilityId,
--	IS_INBOUND,
--	IS_ASN,
--	UPSOrderNumber


-- Loading data for Outbound

SELECT 
	SKU,
	count (SKU) AS OutboundCount
INTO #tmp_Outbound_details
FROM #tmp_Outbound_Summary_details SD
--RIGHT 
JOIN [Summary].DIGITAL_SUMMARY_ORDER_LINES OL (NOLOCK) ON SD.AccountId = OL.AccountId  and SD.UPSOrderNumber=OL.UPSOrderNumber
WHERE SD.IS_INBOUND = 0 
	
GROUP BY 
	SKU


-- Loading ASN data in Temp table

SELECT 
	SKU,
	count(SKU) AS ASNCount
INTO #tmp_Inbound_details
FROM #tmp_ASN_Summary_details SD
--RIGHT 
JOIN [Summary].DIGITAL_SUMMARY_INBOUND_LINE IL (NOLOCK) ON SD.AccountId = IL.AccountId  and SD.UPSOrderNumber=IL.UPSOrderNumber
WHERE IS_INBOUND = 1 
      AND IS_ASN = 1
GROUP BY 
	SKU


	-- FINAL RESULT SET

SELECT TOP (@topRow)
	IB.SKU AS AsnItemNumbers,
	IB.ASNCount AS AsnOrderCount

FROM  #tmp_Inbound_details IB
where ((('ASN') in (SELECT UPPER (TRIM (value)) FROM string_split(@VarShipmentType, ','))) or @VarShipmentType = '*')
AND IB.SKU IS NOT NULL
order by 
case when @VarsortBy='VOLUME ASC' then IB.ASNCount end ASC,
case when @VarsortBy='VOLUME DESC' then IB.ASNCount end desc


SELECT TOP (@topRow)
	OB.SKU AS OutboundItemNumbers,
	OB.OutboundCount AS OutboundOrderCount

FROM  #tmp_Outbound_details OB
where ((('OUTBOUND') in (SELECT UPPER (TRIM (value)) FROM string_split(@VarShipmentType, ','))) or @VarShipmentType = '*')
AND OB.SKU IS NOT NULL
order by 
case when @VarsortBy='VOLUME ASC' then OB.OutboundCount end ASC,
case when @VarsortBy='VOLUME DESC' then OB.OutboundCount end desc

END
GO

