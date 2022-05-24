/****** Object:  StoredProcedure [digital].[rpt_Warehouse_Listing]    Script Date: 1/6/2022 3:17:38 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


/**** 
--AMR
EXEC [digital].[rpt_Warehouse_Listing]   @DPProductLineKey = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529',
@AccountKeys ='[{"DPProductLineKey": "870561E1-A974-483B-AA0D-A724C5D402C9","DPServiceLineKey": "1AD6CEC2-F040-43AA-BBCE-17548A833665"
          

                },{"DPProductLineKey": "344C94d7-DE3D-4351-B9E4-0FD3C1E55B3C","DPServiceLineKey": "D0B677E7-159F-4543-8FA7-D9C4B1C18302"
          

                }]'

,@DPServiceLineKey = '*',@DPEntityKey = '*'
--SWR
EXEC [digital].[rpt_Warehouse_Listing]   @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = '150E5128-B3E3-44A1-BF6C-CD2E4D9856DA',@DPEntityKey = '*'
--Cambium
EXEC [digital].[rpt_Warehouse_Listing]   @DPProductLineKey = 'A10F512B-C7F4-42A0-909C-21DD95A7D921',@DPServiceLineKey = '53DDDBC2-FB5A-4E70-A92E-8312DC9FDD05',@DPEntityKey = '*',@startDate='2020-10-21',@endDate='2020-10-27',@inboundType = ''
****/

CREATE PROCEDURE [digital].[rpt_Warehouse_Listing] 

@DPProductLineKey varchar(50)=null, @DPServiceLineKey varchar(50)=null, @DPEntityKey varchar(50)=null
,@AccountKeys nvarchar(max) = NULL
,@startDate date=null, @endDate date=null,@DateType varchar(50) = null, @inboundType varchar(50)=''

AS

BEGIN

  DECLARE @VarAccountID varchar(50),
          @VarDPServiceLineKey varchar(50),
          @VarDPEntityKey varchar(50),
          @VarStartCreatedDateTime datetime,
          @VarEndCreatedDateTime datetime,
		  @NULLCreatedDate varchar(1),
		  @VarInboundType varchar(50),
		  @NULLInboundType varchar(1),
		  @isASN           INT,
		  @VarDPServiceLineKeyJSON VARCHAR(max),
		  @VarDPProductLineKeyJSON VARCHAR(max)

  SET @VarAccountID = UPPER(@DPProductLineKey)
  SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)
  SET @VarDPEntityKey = UPPER(@DPEntityKey)
  SET @VarStartCreatedDateTime = @startDate
  SET @VarEndCreatedDateTime = @EndDate
  SET @VarEndCreatedDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @VarEndCreatedDateTime)))
  SET @VarInboundType=UPPER(@inboundType)


  SELECT UPPER(DPProductLineKey) AS DPProductLineKey,
       UPPER(DPServiceLineKey) AS DPServiceLineKey
	   into #ACCOUNTINFO
	   FROM OPENJSON(@AccountKeys)
	   WITH(
   DPProductLineKey VARCHAR(MAX),
   DPServiceLineKey VARCHAR(MAX)
	   )


  IF @DPServiceLineKey IS NULL
    SET @VarDPServiceLineKey = '*';

  IF @DPEntityKey IS NULL
    SET @VarDPEntityKey = '*';

  IF @startDate IS NULL OR @endDate IS NULL
    SET @NULLCreatedDate = '*'

  IF @VarInboundType='' OR @VarInboundType IS NULL OR @VarInboundType = '*'
     SET @NULLInboundType = '*'
  ELSE
   SET @isASN = CASE WHEN @VarInboundType='ASN' THEN 1
                     WHEN @VarInboundType='TRANSPORT ORDER' THEN 0 
                END


 
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




	 
  SELECT
    W.GLD_WAREHOUSE_MAPPED_KEY warehouseId,
    CASE
      WHEN SS.SOURCE_SYSTEM_NAME LIKE '%SOFTEON%' THEN BUILDING_CODE
      ELSE WAREHOUSE_CODE
    END warehouseCode,
    W.WAREHOUSE_TIME_ZONE warehouseTimeZone,
    W.ADDRESS_LINE_1 addressLine1,
    W.ADDRESS_LINE_2 addressLine2,
    W.CITY city,
    W.PROVINCE stateProvince,
    W.POSTAL_CODE postalCode,
    W.COUNTRY country 
  INTO #tmp_Warehouse
  FROM [dbo].[DIM_WAREHOUSE] W (NOLOCK)
	  INNER JOIN [dbo].DIM_CUSTOMER C (NOLOCK) on W.SOURCE_SYSTEM_KEY = C.SOURCE_SYSTEM_KEY
	  INNER JOIN [dbo].[DIM_SOURCE_SYSTEM] SS (NOLOCK) ON W.SOURCE_SYSTEM_KEY = SS.SOURCE_SYSTEM_KEY
  WHERE ( C.GLD_ACCOUNT_MAPPED_KEY IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR GLD_ACCOUNT_MAPPED_KEY = @VarAccountID ) 
	  AND ((DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR DP_SERVICELINE_KEY = @VarDPServiceLineKey)  OR @VarDPServiceLineKey = '*')
	  --AND (ISNULL(DP_ORGENTITY_KEY, '') = @VarDPEntityKey OR @VarDPEntityKey = '*')
	  AND CASE WHEN SS.SOURCE_SYSTEM_NAME LIKE '%SOFTEON%' THEN W.BUILDING_CODE ELSE W.WAREHOUSE_CODE END 
		IN (SELECT TRIM(value) AS warehouse_code FROM STRING_SPLIT(C.Mapped_Warehouse_code, ',')) 
	  AND W.GLD_WAREHOUSE_MAPPED_KEY IS NOT NULL
  GROUP BY
	W.GLD_WAREHOUSE_MAPPED_KEY,
    CASE WHEN SS.SOURCE_SYSTEM_NAME LIKE '%SOFTEON%' THEN BUILDING_CODE  ELSE WAREHOUSE_CODE END,
    W.WAREHOUSE_TIME_ZONE,
    W.ADDRESS_LINE_1,
    W.ADDRESS_LINE_2,
    W.CITY,
    W.PROVINCE,
    W.POSTAL_CODE,
    W.COUNTRY

SELECT
	AccountId,
	FacilityId,
	IS_INBOUND,
	IS_ASN,
	COUNT(UPSOrderNumber) shipmentcount
INTO #tmp_Ship_count
FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK)
WHERE AccountId  IN (SELECT DPProductLineKey FROM #ACCOUNTINFO)
	AND ((DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR DP_SERVICELINE_KEY = @VarDPServiceLineKey)  OR @VarDPServiceLineKey = '*')
	--AND (ISNULL(DP_ORGENTITY_KEY, '') = @VarDPEntityKey OR @VarDPEntityKey = '*')
	AND ((DateTimeReceived BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime) OR @NULLCreatedDate = '*')
	--AND (@NULLInboundType='*' OR COALESCE(IS_ASN,0) = @isASN)  --Moving to below Inbound statement only
	AND ISNULL(OrderCancelledFlag,'N') <> 'Y'
GROUP BY 
	AccountId,
	FacilityId,
	IS_INBOUND,
	IS_ASN


SELECT 
	--AccountId,
	FacilityId,
	SUM(shipmentcount) Outbound_shipmentcount
INTO #tmp_Outbound_shipmentcount
FROM #tmp_Ship_count 
WHERE IS_INBOUND = 0 
AND AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO)
GROUP BY 
	--AccountId,
	FacilityId

SELECT 
	--AccountId,
	FacilityId,
	SUM(shipmentcount) Inbound_shipmentcount
INTO #tmp_Inbound_shipmentcount
FROM #tmp_Ship_count 
WHERE IS_INBOUND = 1 
AND (@NULLInboundType='*' OR COALESCE(IS_ASN,0) = @isASN)
AND AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO)
GROUP BY 
	--AccountId,
	FacilityId

SELECT w1.warehouseId,
COUNT(INV.LPNNumber) as lpnOnHandCount
INTO #tblLpnOnHandCount
FROM #tmp_Warehouse w1
INNER JOIN Summary.DIGITAL_SUMMARY_INVENTORY INV (NOLOCK)
ON w1.warehouseId=INV.FacilityId
WHERE INV.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO)
group by w1.warehouseId

SELECT
	w1.warehouseId warehouseId,
	w1.warehouseCode warehouseCode,
	w1.warehouseTimeZone warehouseTimeZone,
	w1.addressLine1 addressLine1,
	w1.addressLine2 addressLine2,
	w1.city city,
	w1.stateProvince stateProvince,
	w1.postalCode postalCode,
	w1.country country,
	ISNULL(Inbound_shipmentcount, 0) inboundShipmentCount,
	ISNULL(Outbound_shipmentcount, 0) outboundShipmentCount,
	ISNULL(INV.lpnOnHandCount,0) AS lpnOnHandCount
FROM #tmp_Warehouse w1
	LEFT OUTER JOIN #tmp_Outbound_shipmentcount WO
		ON w1.warehouseId = WO.FacilityId --AND w1.warehouseCode = WO.OrderWarehouse
	LEFT OUTER JOIN #tmp_Inbound_shipmentcount WI
		ON w1.warehouseId = WI.FacilityId --AND w1.warehouseCode = WI.OrderWarehouse
	LEFT JOIN #tblLpnOnHandCount INV
	          ON w1.warehouseId=INV.warehouseId

END
GO

