/****** Object:  StoredProcedure [digital].[rpt_Inbound_Cancelled_Shipment_Details]    Script Date: 2/9/2022 1:28:45 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



/**** 
--AMR
EXEC [digital].[rpt_Inbound_Cancelled_Shipment_Details]   @DPProductLineKey = 'E648FA6F-6253-428E-8AC9-201E3EF83B91',@DPServiceLineKey = '53D60776-D3BD-4E6A-8E37-F591C148294B',@DPEntityKey = NULL,@startDate = '2020-08-08',@endDate = '2020-08-14',@TopRow = 100,@warehouseId = '*'
--SWR
EXEC [digital].[rpt_Inbound_Cancelled_Shipment_Details]   @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = '150E5128-B3E3-44A1-BF6C-CD2E4D9856DA',@DPEntityKey = NULL,@startDate = '2020-08-08',@endDate = '2020-08-14',@TopRow = 100,@warehouseId = '*'
--Cambium
EXEC [digital].[rpt_Inbound_Cancelled_Shipment_Details]   @DPProductLineKey = 'A10F512B-C7F4-42A0-909C-21DD95A7D921',@DPServiceLineKey = '53DDDBC2-FB5A-4E70-A92E-8312DC9FDD05',@DPEntityKey = NULL,@startDate = '2020-08-08',@endDate = '2020-08-14',@TopRow = 100,@warehouseId = '*'
****/

CREATE PROCEDURE [digital].[rpt_Inbound_Cancelled_Shipment_Details] 

@DPProductLineKey varchar(50), @DPServiceLineKey varchar(50), @DPEntityKey varchar(50),
@startDate date, @endDate date, @warehouseId nvarchar(max), @topRow int,@inboundType varchar(50)=''

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
		  @isASN           INT

  SET @VarAccountID = UPPER(@DPProductLineKey)
  SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)
  SET @VarDPEntityKey = UPPER(@DPEntityKey)
  SET @VarwarehouseId = UPPER(@warehouseId)
  SET @VarStartCreatedDateTime = @startDate
  SET @VarEndCreatedDateTime = @endDate
  SET @VarInboundType=UPPER(@inboundType)

  SET @VarEndCreatedDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @VarEndCreatedDateTime)))


  IF @VarStartCreatedDateTime IS NULL OR @VarEndCreatedDateTime IS NULL
    SET @NULLCreatedDate = '*'

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
  SELECT
    O.[DateTimeCancelled] shipmentCanceledDateTime,
    'shipmentCanceledBy' shipmentCanceledBy,
    O.[CancelledReasonCode] shipmentCanceledReason,
    O.[UPSOrderNumber] upsShipmentNumber,
    O.[OrderNumber] clientShipmentNumber,
    O.UPSTransportShipmentNumber shipmentNumber,
    O.ReferenceOrder referenceNumber,
    O.CustomerPO customerPONumber,
    O.[UPSOrderNumber] orderNumber,
    O.Carrier shipmentCarrier,
    O.CarrierCode shipmentCarrierCode,
    O.ServiceLevel shipmentServiceLevel,
    O.[ServiceLevelCode] shipmentServiceLevelCode,
    O.ServiceMode serviceMode 
	INTO #DIGITAL_SUMMARY_ORDERS
  FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O
  WHERE O.AccountId = @VarAccountID
  AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')
  --AND (ISNULL(O.DP_ORGENTITY_KEY, '') = @VarDPEntityKey OR @VarDPEntityKey = '*')
  AND (O.FacilityId IN (SELECT UPPER (TRIM (VALUE)) FROM string_split(@VarwarehouseId, ',')) OR @VarwarehouseId = '*')
  AND ((O.DateTimeCancelled BETWEEN @VarStartCreatedDateTime AND @VarEndCreatedDateTime) OR @NULLCreatedDate = '*')
  AND OrderStatusName = 'Cancelled' -- AND  sourcesystemname = 'MERCURYGATE'
  AND O.IS_INBOUND= 1
  AND (@NULLInboundType='*' OR isnull(O.IS_ASN,0)=@isASN)


  SELECT COUNT(1) totalCount FROM #DIGITAL_SUMMARY_ORDERS


  IF @topRow = 0

    SELECT 
	shipmentCanceledDateTime,
    shipmentCanceledBy,
    shipmentCanceledReason,
    upsShipmentNumber,
    clientShipmentNumber,
    shipmentNumber,
    referenceNumber,
    customerPONumber,
    orderNumber,
    shipmentCarrier,
    shipmentCarrierCode,
    shipmentServiceLevel,
    shipmentServiceLevelCode,
    serviceMode 
	FROM #DIGITAL_SUMMARY_ORDERS 
	ORDER BY shipmentCanceledDateTime DESC

  ELSE

    SELECT TOP (@topRow) 
	shipmentCanceledDateTime,
    shipmentCanceledBy,
    shipmentCanceledReason,
    upsShipmentNumber,
    clientShipmentNumber,
    shipmentNumber,
    referenceNumber,
    customerPONumber,
    orderNumber,
    shipmentCarrier,
    shipmentCarrierCode,
    shipmentServiceLevel,
    shipmentServiceLevelCode,
    serviceMode
	FROM #DIGITAL_SUMMARY_ORDERS 
	ORDER BY shipmentCanceledDateTime DESC

END
GO

