/****** Object:  StoredProcedure [digital].[rpt_Shipment_Search_SUM]    Script Date: 1/4/2022 12:55:57 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




/**** 

CHANGE LOG
----------
CHANGED BY				DATE			  SPRINT     	CHANGES
PRASHANT RAI     		09/08/2021		  38TH       	SHIPMENTSERIALNUMBERWAREHOUSE IS ADDED FOR SEARCHBY INPUT
Anand Ramamoorthy		09/30/2021		  39			Implemented Multi Values search. Changed Searchvalue as JSON format. Implemented for CLIENTASNNUMBER and CUSTOMERPONUMBER
Venkata					11/18/2021		42				Added logic to accept the null value for DPServiceLineKey	


--@SearchBy:- REFERENCENUMBER,CARRIERSHIPMENTNUMBER,UPSSHIPMENTNUMBER,UPSASNNUMBER,CLIENTSHIPMENTNUMBER,CUSTOMERPONUMBER
--AMR
  EXEC [digital].[rpt_Shipment_Search_DM_NEW] @DPProductLineKey = 'E648FA6F-6253-428E-8AC9-201E3EF83B91',@DPServiceLineKey = '53D60776-D3BD-4E6A-8E37-F591C148294B',@DPEntityKey = '',@SearchBy = 'UPSSHIPMENTNUMBER', @SearchValue = '113467' , @ShipmentType = '', @Debug = 0 , @AccountKeys ='[{"DPProductLineKey": "a2B1487c-3878-4A06-898B-4EA06DF022BF","DPServiceLineKey": "1AD6CEC2-F040-43AA-BBCE-17548A833665"
          

		  },{"DPProductLineKey": "344C94d7-DE3D-4351-B9E4-0FD3C1E55B3C","DPServiceLineKey": "D0B677E7-159F-4543-8FA7-D9C4B1C18302"
          

		  }]'
--SWR
  EXEC [digital].[rpt_Shipment_Search_SUM] @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = '150E5128-B3E3-44A1-BF6C-CD2E4D9856DA',@DPEntityKey = '',@SearchBy = 'CARRIERSHIPMENTNUMBER', @SearchValue = '123' , @ShipmentType = ''
--Cambium
  EXEC [digital].[rpt_Shipment_Search_SUM] @DPProductLineKey = 'A10F512B-C7F4-42A0-909C-21DD95A7D921',@DPServiceLineKey = '53DDDBC2-FB5A-4E70-A92E-8312DC9FDD05',@DPEntityKey = '',@SearchBy = 'CARRIERSHIPMENTNUMBER', @SearchValue = '123' , @ShipmentType = ''

EXEC [digital].[rpt_Shipment_Search_SUM]
@DPProductLineKey =N'B7643D99-A947-4F38-8372-9B0B6A56994B',
@DPServiceLineKey =NULL,
@DPEntityKey=null,
@AccountKeys = '[{"DPProductLineKey":"DD650B97-9291-498B-BA98-D6680C73CA3C","DPServiceLineKey":"0553638E-9317-413E-B707-6C48388F9CD2"}]',
@SearchBy = N'UPSSHIPMENTNUMBER',
@SearchValue = N'10468044',
@ShipmentType = N'*',
@Debug  = 0

EXEC [digital].[rpt_Shipment_Search_SUM]
@DPProductLineKey =N'B7643D99-A947-4F38-8372-9B0B6A56994B',
@DPServiceLineKey =NULL,
@DPEntityKey=null,
@AccountKeys = '[{"DPProductLineKey":"DD650B97-9291-498B-BA98-D6680C73CA3C","DPServiceLineKey":"0553638E-9317-413E-B707-6C48388F9CD2"}]',
@SearchBy = N'CARRIERSHIPMENTNUMBER',
@SearchValue = N'61290322860726520734',
@ShipmentType = N'*' ,
@Debug  = 0

EXEC [digital].[rpt_Shipment_Search_SUM]
@DPProductLineKey =N'00D43FDB-248E-401E-AD41-22E32808D94C',
@DPServiceLineKey =NULL,
@DPEntityKey=null,
@AccountKeys = '[{"DPProductLineKey":"00D43FDB-248E-401E-AD41-22E32808D94C","DPServiceLineKey":null}]',
@SearchBy = N'CLIENTASNNUMBER',
@SearchValue = N'[{"searchValue": "1Z55F0636800596881"},{"searchValue": "1094422L0001"},{"searchValue": "WO-0880617"},{"searchValue": "ASN0046079216"}]',
@ShipmentType = N'*' ,
@Debug  = 0
****/
CREATE PROCEDURE [digital].[rpt_Shipment_Search_SUM] 
@DPProductLineKey varchar(50), 
@DPServiceLineKey varchar(50),
@DPEntityKey varchar(50), 
@SearchBy [varchar](max), 
@SearchValue [varchar](MAX), 
@ShipmentType varchar(50), 
@Debug INT = 0, 
@AccountKeys nvarchar(max) = NULL
AS

BEGIN

  DECLARE @VarAccountID varchar(50),
          @VarDPServiceLineKey varchar(50),
          @VarDPEntityKey varchar(50),
          @varSearchBy varchar(max),
          @varSearchValue varchar(50),
          @varShipmentType varchar(50),
          @NULLCSN varchar(1) = '*',
          @NULLRN varchar(1) = '*',
          @NULLUSN varchar(1) = '*',
          @NULLCLTSN varchar(1) = '*',
          @NULLCPON varchar(1) = '*',
          @NULLCASNN varchar(1) = '*',
          @NULLUASNN varchar(1) = '*',
		  @NULLSSN varchar(1) = '*',
		  @NULLSLN varchar(1) = '*',
		  @NULLHS varchar(1) = '*',
		  @NULLCOHR varchar(1)='*',
		  @NULLShipmentType varchar(1),
		  @NULLLRN varchar(1)='*',
		  @NULLLPN varchar(1)='*', 
		  @NULLVSN varchar(1)='*', 
		  @NULLVCL varchar(1)='*', 
		  @NULLUSNW varchar(1) = '*',
		  @NULLBATCHNUMBER varchar(1) = '*',
		  @NULLDesignator varchar(1)='*',
          @NULLWrongSearchBy varchar(1) = '*',
          @UPSOrderNo varchar(max),
		  @Starttime DATETIME,
		  @EndTime DATETIME,
		  @IS_INBOUND   INT,
		  @VarDPServiceLineKeyJSON VARCHAR(500),
		  @VarDPProductLineKeyJSON VARCHAR(500),
		  @varJsonSearchValue varchar(max) -->MultiSearch



SELECT UPPER(DPProductLineKey) AS DPProductLineKey,
       UPPER(DPServiceLineKey) AS DPServiceLineKey
	   into #ACCOUNTINFO
	   FROM OPENJSON(@AccountKeys)
	   WITH(
   DPProductLineKey VARCHAR(MAX),
   DPServiceLineKey VARCHAR(MAX)
	   )
		  

 
  --SET @VarAccountID = UPPER(@DPProductLineKey)
  SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)
  SET @VarDPEntityKey = UPPER(@DPEntityKey)
  SET @varSearchBy = UPPER(@SearchBy)
  SET @varSearchValue = ''-->MultiSearch
  SET @varJsonSearchValue = @SearchValue-->MultiSearch
  SET @varShipmentType=UPPER(@ShipmentType)
  SET @Starttime = GETDATE()
  
  	--Store Search Values in Temp Table (MultiSearch)
	SELECT TRIM(SearchValue) AS SearchValue
	INTO #TMPSearch
	FROM OPENJSON(@varJsonSearchValue)
		WITH (
		SearchValue NVARCHAR(100) 'strict $.searchValue'
		);	
	SELECT TOP 1 @varSearchValue = UPPER(SearchValue) FROM #TMPSearch -- For the Single search to work without changing it's code


	IF @DPServiceLineKey IS NULL                
	SET @VarDPServiceLineKey = '*' --11/18/2021 

   IF NOT EXISTS ( SELECT DPServiceLineKey FROM #ACCOUNTINFO WHERE DPServiceLineKey IS NOT NULL OR DPServiceLineKey='*' ) 
    SET @VarDPServiceLineKeyJSON = '*'

  IF (( @DPServiceLineKey IS NOT NULL) AND @VarDPServiceLineKeyJSON = '*')
    SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)

  IF (@VarDPServiceLineKeyJSON = '*' AND ISNULL(@DPServiceLineKey, '*') = '*')
    SET @VarDPServiceLineKey = '*'


	IF NOT EXISTS ( SELECT DPProductLineKey FROM #ACCOUNTINFO WHERE DPProductLineKey IS NOT NULL) 
    SET @VarDPProductLineKeyJSON = '*'

  IF (( @DPProductLineKey IS NOT NULL) AND @VarDPProductLineKeyJSON = '*')
    SET @VarAccountID = UPPER(@DPProductLineKey)


	
  IF @DPEntityKey IS NULL
    SET @VarDPEntityKey = '*'

  IF @varShipmentType='' OR ISNULL(@varShipmentType,'*')='*'
     SET @NULLShipmentType = '*'
  ELSE
     SET @IS_INBOUND = CASE WHEN @varShipmentType='INBOUND' THEN 1
							WHEN @varShipmentType='MOVEMENT' THEN 2
							WHEN @varShipmentType='OUTBOUND' THEN 0 
                END




  IF @varSearchBy = 'CARRIERSHIPMENTNUMBER'
  BEGIN
    SET @NULLCSN = ''
  END
  ELSE
  IF @varSearchBy = 'REFERENCENUMBER'
  BEGIN
    SET @NULLRN = ''
  END
  ELSE
  IF @varSearchBy = 'UPSSHIPMENTNUMBER'
  BEGIN
    SET @NULLUSN = ''
  END
  ELSE
  IF @varSearchBy = 'CLIENTSHIPMENTNUMBER'
  BEGIN
    SET @NULLCLTSN = ''
  END
  ELSE
  IF @varSearchBy = 'CUSTOMERPONUMBER'
  BEGIN
    SET @NULLCPON = ''
  END
  ELSE
  IF @varSearchBy = 'CLIENTASNNUMBER'
  BEGIN
    SET @NULLCASNN = ''
  END
  ELSE
  IF @varSearchBy = 'UPSASNNUMBER'
  BEGIN
    SET @NULLUASNN = ''
  END
  ELSE
  IF @varSearchBy='SHIPMENTSERIALNUMBER'
  BEGIN
	SET @NULLSSN = ''
  END
  ELSE
  IF @varSearchBy='SHIPMENTLOTNUMBER'
  BEGIN
	SET @NULLSLN = ''
  END 
  ELSE
  IF @varSearchBy='HOLDSTATUS'
  BEGIN
	SET @NULLHS = ''
  END
  ELSE
  IF @varSearchBy='CUSTOMEROUTBOUNDHEADERREFERENCE'
  BEGIN
	SET @NULLCOHR = ''
  END
  ELSE
  IF @varSearchBy='LINEREFERENCENUMBER'
  BEGIN
	SET @NULLLRN = ''
  END
  ELSE
  IF @varSearchBy='LPN'
  BEGIN
	SET @NULLLPN = ''
  END
  ELSE
  IF @varSearchBy='VSN'
  BEGIN
	SET @NULLVSN = ''
  END
  ELSE
  IF @varSearchBy='VCL'
  BEGIN
	SET @NULLVCL = ''
  END
  ELSE
  IF @varSearchBy='DESIGNATOR'
  BEGIN
	SET @NULLDesignator = ''
  END
  ELSE
  IF @varSearchBy ='SHIPMENTBATCHNUMBER'
  BEGIN
	SET @NULLBATCHNUMBER = ''
  END
  ELSE
  IF @varSearchBy='SHIPMENTSERIALNUMBERWAREHOUSE' 
  BEGIN
	SET @NULLUSNW = ''
  END
  ELSE
    SET @NULLWrongSearchBy = '*'
END

  IF @varSearchBy = 'CARRIERSHIPMENTNUMBER'
  BEGIN
    
	SELECT @UPSOrderNo= STUFF( (SELECT DISTINCT ','+UPSOrderNumber 
                      FROM [Summary].[DIGITAL_SUMMARY_MILESTONE_ACTIVITY]	(NOLOCK)
					  WHERE TrackingNumber = @varSearchValue COLLATE SQL_Latin1_General_CP1_CI_AS
					  AND ( AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR AccountId = @VarAccountID ) 
                      FOR XML PATH('')
                     ), 1, 1, ''
                   )

  IF @UPSOrderNo IS NULL	
	
	SELECT @UPSOrderNo= STUFF( (SELECT DISTINCT ','+UPSOrderNumber 
                        FROM Summary.DIGITAL_SUMMARY_ORDER_TRACKING (NOLOCK)
					    WHERE TRACKING_NUMBER =  @varSearchValue COLLATE SQL_Latin1_General_CP1_CI_AS
					    AND  ( AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO)OR AccountId = @VarAccountID )  
                        FOR XML PATH('')
                     ), 1, 1, ''
                   )

    IF @Debug>0
  BEGIN
  SET @EndTime = GETDATE()
  SELECT 'CARRIERSHIPMENTNUMBER CHECK',@Starttime,@EndTime,DATEDIFF(SS,@Starttime,@EndTime) AS TimeTakeninsecs
  SET @Starttime = GETDATE()
  END
	
  END

  IF @varSearchBy='LINEREFERENCENUMBER'
  BEGIN
      SELECT @UPSOrderNo= STUFF( (SELECT DISTINCT ','+UPSOrderNumber 
                        FROM Summary.DIGITAL_SUMMARY_ORDER_LINES OL(NOLOCK)
					    WHERE (OL.LineRefVal1 = @varSearchValue COLLATE SQL_Latin1_General_CP1_CI_AS
                            OR OL.LineRefVal2 = @varSearchValue COLLATE SQL_Latin1_General_CP1_CI_AS
                            OR OL.LineRefVal3 = @varSearchValue COLLATE SQL_Latin1_General_CP1_CI_AS 
	                        OR OL.LineRefVal4 = @varSearchValue COLLATE SQL_Latin1_General_CP1_CI_AS
	                        OR OL.LineRefVal5 = @varSearchValue COLLATE SQL_Latin1_General_CP1_CI_AS
							  )
					    AND ( AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR AccountId = @VarAccountID ) 
                        FOR XML PATH('')
                     ), 1, 1, ''
                   )
  END

  IF @varSearchBy='CUSTOMEROUTBOUNDHEADERREFERENCE'
  BEGIN
	SELECT @UPSOrderNo= STUFF( (SELECT DISTINCT ','+UPSOrderNumber 
                        FROM Summary.DIGITAL_SUMMARY_ORDERS O (NOLOCK)
					    WHERE (
									 O.ORDER_REF_1_VALUE = @varSearchValue COLLATE SQL_Latin1_General_CP1_CI_AS
                                  OR O.ORDER_REF_2_VALUE = @varSearchValue COLLATE SQL_Latin1_General_CP1_CI_AS
                                  OR O.ORDER_REF_3_VALUE = @varSearchValue COLLATE SQL_Latin1_General_CP1_CI_AS 
	                              OR O.ORDER_REF_4_VALUE = @varSearchValue COLLATE SQL_Latin1_General_CP1_CI_AS
	                              OR O.ORDER_REF_5_VALUE = @varSearchValue COLLATE SQL_Latin1_General_CP1_CI_AS
							  )
					    AND ( AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR AccountId = @VarAccountID )
                        FOR XML PATH('')
                     ), 1, 1, ''
                   )
  END

  IF @varSearchBy='DESIGNATOR' 
  BEGIN

	SELECT @UPSOrderNo= STUFF( (SELECT DISTINCT ','+ORD.UPSOrderNumber 
                        FROM Summary.DIGITAL_SUMMARY_ORDERS ORD (NOLOCK)
						INNER JOIN Summary.DIGITAL_SUMMARY_ORDER_LINES_DETAILS OLD  (NOLOCK)
						ON ORD.UPSOrderNumber=OLD.UPSOrderNumber
						AND ORD.SourceSystemKey=OLD.SourceSystemKey
					    WHERE  DispositionValue=@varSearchValue
					    AND ( ORD.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR ORD.AccountId = @VarAccountID) 
                        FOR XML PATH('')
                     ), 1, 1, ''
                   )
  END

  IF @varSearchBy='LPN'
  BEGIN

	SELECT @UPSOrderNo= STUFF( (SELECT DISTINCT ','+ORD.UPSOrderNumber 
                        FROM Summary.DIGITAL_SUMMARY_ORDERS ORD (NOLOCK)
						INNER JOIN Summary.DIGITAL_SUMMARY_ORDER_LINES_DETAILS OLD  (NOLOCK)
						ON ORD.UPSOrderNumber=OLD.UPSOrderNumber
						AND ORD.SourceSystemKey=OLD.SourceSystemKey
					    WHERE  LPNNumber=@varSearchValue
					    AND ( ORD.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR ORD.AccountId = @VarAccountID ) 
                        FOR XML PATH('')
                     ), 1, 1, ''
                   )
  END

  IF @varSearchBy='VSN'
  BEGIN

	SELECT @UPSOrderNo= STUFF( (SELECT DISTINCT ','+ORD.UPSOrderNumber 
                        FROM Summary.DIGITAL_SUMMARY_ORDERS ORD (NOLOCK)
						INNER JOIN Summary.DIGITAL_SUMMARY_ORDER_LINES_DETAILS OLD  (NOLOCK)
						ON ORD.UPSOrderNumber=OLD.UPSOrderNumber
						AND ORD.SourceSystemKey=OLD.SourceSystemKey
					    WHERE  VendorSerialNumber=@varSearchValue
					    AND (ORD.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR ORD.AccountId = @VarAccountID ) 
                        FOR XML PATH('')
                     ), 1, 1, ''
                   )
  END

  
  IF @varSearchBy='VCL' OR @varSearchBy = 'SHIPMENTBATCHNUMBER'
  BEGIN

	SELECT @UPSOrderNo= STUFF( (SELECT DISTINCT ','+ORD.UPSOrderNumber 
                        FROM Summary.DIGITAL_SUMMARY_ORDERS ORD (NOLOCK)
						INNER JOIN Summary.DIGITAL_SUMMARY_ORDER_LINES_DETAILS OLD  (NOLOCK)
						ON ORD.UPSOrderNumber=OLD.UPSOrderNumber
						AND ORD.SourceSystemKey=OLD.SourceSystemKey
					    WHERE  VendorLotNumber=@varSearchValue
					    AND ( ORD.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR ORD.AccountId = @VarAccountID ) 
                        FOR XML PATH('')
                     ), 1, 1, ''
                   )
  END

  -- ADDING NEW SHIPMENTSERIALNUMBERWAREHOUSE INPUT TYPE

  IF @varSearchBy = 'SHIPMENTSERIALNUMBERWAREHOUSE' 
  BEGIN

	SELECT @UPSOrderNo= STUFF( (SELECT DISTINCT ','+ORD.UPSOrderNumber 
                        FROM Summary.DIGITAL_SUMMARY_ORDERS ORD (NOLOCK)
						INNER JOIN Summary.DIGITAL_SUMMARY_ORDER_LINES_DETAILS OLD  (NOLOCK)
						ON ORD.UPSOrderNumber=OLD.UPSOrderNumber
						AND ORD.SourceSystemKey=OLD.SourceSystemKey
					    WHERE VendorSerialNumber = @varSearchValue
					    AND ( ORD.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR ORD.AccountId = @VarAccountID ) 
                        FOR XML PATH('')
                     ), 1, 1, ''
                   )
  END


   CREATE TABLE #SummaryTable (
    shipmentNumber varchar(128),
    referenceNumber varchar(512),
    upsShipmentNumber varchar(128),
    clientShipmentNumber varchar(512),
    customerPONumber varchar(512),
    orderNumber varchar(512),
    upsTransportShipmentNumber varchar(128),    
	gffShipmentInstanceId varchar(128),
    gffShipmentNumber varchar(128),
    orderLinesCount INT,
    shipmentDescription varchar(50),
    warehouseId varchar(255),
    warehouseCode varchar(50),
    shipmentServiceLevel varchar(255),
    shipmentServiceLevelCode varchar(255),
    shipmentCarrierCode varchar(255),
    shipmentCarrier varchar(255),
	shipmentBookedOnDateTime datetime,	
    shipmentCanceledDateTime datetime,
    shipmentCanceledReason varchar(512),
    actualShipmentDateTime datetime,
    shipmentCreateOnDateTime datetime,
    originalScheduledDeliveryDateTime datetime,
    actualDeliveryDateTime datetime,
    inventoryShipmentStatus varchar(255),
	IS_INBOUND INT,
    primaryException varchar(160),
    transportationMileStone varchar(21),
    shipmentOrigin_addressLine1 varchar(512),
    shipmentOrigin_addressLine2 varchar(512),
    shipmentOrigin_city varchar(255),
    shipmentOrigin_stateProvince varchar(255),
    shipmentOrigin_postalCode varchar(255),
    shipmentOrigin_country varchar(255),
    shipmentDestination_consignee nvarchar(255),
    shipmentDestination_addressLine1 varchar(512),
    shipmentDestination_addressLine2 varchar(512),
    shipmentDestination_city varchar(255),
    shipmentDestination_stateProvince varchar(255),
    shipmentDestination_postalCode varchar(255),
    shipmentDestination_country varchar(255),
    milestoneStatus varchar(40),
    SourceSystemKey int,
	AccountId varchar(255),
	accountNumber varchar(50)
	--ServiceLineKey varchar(255),
	--EntityKey varchar(255)
	)

  INSERT INTO #SummaryTable
  SELECT DISTINCT
                                O.[UPSOrderNumber]                    AS shipmentNumber,
                                O.[OrderNumber]                       AS referenceNumber,
                                O.[UPSOrderNumber]                    AS upsShipmentNumber,
                                O.[OrderNumber]                       AS clientShipmentNumber,
                                O.[CustomerPO]                        AS customerPONumber,
                                O.[UPSOrderNumber]                    AS orderNumber,
                                O.[UPSTransportShipmentNumber]        AS upsTransportShipmentNumber,    
	                            O.[GFF_ShipmentInstanceId]            AS gffShipmentInstanceId,
                                O.[GFF_ShipmentNumber]                AS gffShipmentNumber,
                                O.[OrderLineCount]                    AS orderLinesCount,
                                O.[OrderType]                         AS shipmentDescription,
                                O.[FacilityId]                        AS warehouseId,
                                O.[OrderWarehouse]                    AS warehouseCode,
                                O.[ServiceLevel]                      AS shipmentServiceLevel,
                                O.[ServiceLevelCode]                  AS shipmentServiceLevelCode,
                                O.[CarrierCode]                       AS shipmentCarrierCode,
                                O.[Carrier]                           AS shipmentCarrier,
	                            O.[ShipmentBookedDate]                AS shipmentBookedOnDateTime,	
                                O.[DateTimeCancelled]                 AS shipmentCanceledDateTime,
                                O.[CancelledReasonCode]               AS shipmentCanceledReason,
                                O.[ScheduleShipmentDate]              AS actualShipmentDateTime,
                                O.[DateTimeReceived]                  AS shipmentCreateOnDateTime,
                                O.[OriginalScheduledDeliveryDateTime] AS originalScheduledDeliveryDateTime,
                                O.[ActualDeliveryDate]                AS actualDeliveryDateTime,
                                O.[OrderStatusName]                   AS inventoryShipmentStatus,
	                            O.[IS_INBOUND]						  AS IS_INBOUND,
                                O.[ExceptionCode]                     AS primaryException,
                                O.[TRANS_MILESTONE]                   AS transportationMileStone,
                                O.[OriginAddress1]                    AS shipmentOrigin_addressLine1,
                                O.[OriginAddress2]                    AS shipmentOrigin_addressLine2,
                                O.[OriginCity]                        AS shipmentOrigin_city,
                                O.[OriginProvince]                    AS shipmentOrigin_stateProvince,
                                O.[OriginPostalCode]                  AS shipmentOrigin_postalCode,
                                O.[OriginCountry]                     AS shipmentOrigin_country,
                                O.[ConsigneeName]                     AS shipmentDestination_consignee,
                                O.[DestinationAddress1]               AS shipmentDestination_addressLine1,
                                O.[DestinationAddress2]               AS shipmentDestination_addressLine2,
                                O.[DestinationCity]                   AS shipmentDestination_city,
                                O.[DestinationProvince]               AS shipmentDestination_stateProvince,
                                O.[DestinationPostalcode]             AS shipmentDestination_postalCode,
                                O.[DestinationCountry]                AS shipmentDestination_country,
                                O.[CurrentMilestone]                  AS milestoneStatus,
                                O.[SourceSystemKey] ,
								O.AccountId,
								--CL.INT_CUSTOMER_NUMBER as accountNumber
								O.Account_number as accountNumber
  FROM      [Summary].[DIGITAL_SUMMARY_ORDERS]                    O  (NOLOCK)
  INNER JOIN DIM_CUSTOMER CL (NOLOCK) ON O.AccountId=CL.GLD_ACCOUNT_MAPPED_KEY
                                      AND O.SourceSystemKey=CL.SOURCE_SYSTEM_KEY
  LEFT JOIN [Summary].[DIGITAL_SUMMARY_TRANSPORTATION_REFERENCES] TR (NOLOCK)
                                                               ON O.UPSOrderNumber  = TR.UPSOrderNumber
                                                               AND O.SourceSystemKey = TR.SourceSystemKey
  WHERE ( O.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR O.AccountId = @VarAccountID ) 
  AND ((O.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR O.DP_SERVICELINE_KEY = @VarDPServiceLineKey)  OR @VarDPServiceLineKey = '*')
  AND (( O.UPSOrderNumber     in (select (value) from string_split(@UPSOrderNo,',')))         OR @NULLCSN             = '*')
  AND (( O.OrderNumber        = @varSearchValue COLLATE SQL_Latin1_General_CP1_CI_AS) OR @NULLRN = '*')
  AND (( O.UPSOrderNumber     = @varSearchValue)     OR @NULLUSN = '*')
  AND (( O.OrderNumber        = @SearchValue    COLLATE SQL_Latin1_General_CP1_CI_AS) OR @NULLCLTSN = '*')
  AND (( O.CustomerPO         IN  (SELECT SearchValue COLLATE SQL_Latin1_General_CP1_CI_AS FROM #TMPSearch)) OR @NULLCPON = '*')-->MultiSearch CUSTOMERPONUMBER
  AND (( O.OrderNumber        IN  (SELECT SearchValue COLLATE SQL_Latin1_General_CP1_CI_AS FROM #TMPSearch) AND O.IS_INBOUND=1) OR @NULLCASNN = '*')-->MultiSearch CLIENTASNNUMBER
  AND (( O.UPSOrderNumber     = @varSearchValue AND O.IS_INBOUND=1) OR @NULLUASNN = '*')
  AND ((TR.ReferenceValue     = @varSearchValue COLLATE SQL_Latin1_General_CP1_CI_AS AND TR.ReferenceType='Serial Number') OR @NULLSSN = '*')
  AND ((TR.ReferenceValue     = @varSearchValue COLLATE SQL_Latin1_General_CP1_CI_AS AND TR.ReferenceType='Lot Number')    OR @NULLSLN = '*')
  AND ((TR.ReferenceValue     = @varSearchValue COLLATE SQL_Latin1_General_CP1_CI_AS AND TR.ReferenceType='Hold Status')   OR @NULLHS = '*')
  AND (@NULLShipmentType='*' OR COALESCE(O.IS_INBOUND,0)=@IS_INBOUND)
  AND (( O.UPSOrderNumber in (select (value) from string_split(@UPSOrderNo,','))) OR @NULLCOHR = '*')
  AND (( O.UPSOrderNumber in (select (value) from string_split(@UPSOrderNo,','))) OR @NULLLRN = '*')
  AND (( O.UPSOrderNumber in (select (value) from string_split(@UPSOrderNo,','))) OR @NULLDesignator = '*')
  AND (( O.UPSOrderNumber in (select (value) from string_split(@UPSOrderNo,','))) OR @NULLVSN = '*')
  AND (( O.UPSOrderNumber in (select (value) from string_split(@UPSOrderNo,','))) OR @NULLVCL = '*')
  AND (( O.UPSOrderNumber in (select (value) from string_split(@UPSOrderNo,','))) OR @NULLBATCHNUMBER = '*')
  AND (( O.UPSOrderNumber in (select (value) from string_split(@UPSOrderNo,','))) OR @NULLLPN = '*')
  AND (( O.UPSOrderNumber in (select (value) from string_split(@UPSOrderNo,','))) OR @NULLUSNW = '*')
  AND @NULLWrongSearchBy = '*' 

  IF @Debug>0
  BEGIN
  SET @EndTime = GETDATE()
  SELECT '#DIGITAL_SUMMARY_ORDERS CREATE',@Starttime,@EndTime,DATEDIFF(SS,@Starttime,@EndTime) AS TimeTakeninsecs
  SET @Starttime = GETDATE()
  END

  SELECT 
    shipmentNumber,
    referenceNumber,
    upsShipmentNumber,
    clientShipmentNumber,
    customerPONumber,
    orderNumber,
    upsTransportShipmentNumber,
    gffShipmentInstanceId,
    gffShipmentNumber,
    orderLinesCount,
    shipmentDescription,
    '' shipmentCategory,
    warehouseId,
    warehouseCode,
    shipmentServiceLevel,
    shipmentServiceLevelCode,
    shipmentCarrierCode,
    shipmentCarrier,
	shipmentBookedOnDateTime,
    shipmentCanceledDateTime,
    shipmentCanceledReason,
    actualShipmentDateTime,
    shipmentCreateOnDateTime,
    originalScheduledDeliveryDateTime,
    actualDeliveryDateTime,
    inventoryShipmentStatus,
	CASE WHEN IS_INBOUND=1 THEN 'Inbound'
	     WHEN IS_INBOUND=2 THEN 'Movement'
		 ELSE 'Outbound' END AS shipmentType,
    primaryException,
    transportationMileStone,
    shipmentOrigin_addressLine1,
    shipmentOrigin_addressLine2,
    shipmentOrigin_city,
    shipmentOrigin_stateProvince,
    shipmentOrigin_postalCode,
    shipmentOrigin_country,
    shipmentDestination_consignee,
    shipmentDestination_addressLine1,
    shipmentDestination_addressLine2,
    shipmentDestination_city,
    shipmentDestination_stateProvince,
    shipmentDestination_postalCode,
    shipmentDestination_country,
    milestoneStatus,
	accountNumber,
	AccountId as dpProductLineKey
	 
  FROM #SummaryTable

  IF @Debug>0
  BEGIN
  SET @EndTime = GETDATE()
  SELECT '#RS1',@Starttime,@EndTime,DATEDIFF(SS,@Starttime,@EndTime) AS TimeTakeninsecs
  SET @Starttime = GETDATE()
  END

  SELECT COUNT(1) totalCount FROM #SummaryTable

  IF @Debug>0
  BEGIN
  SET @EndTime = GETDATE()
  SELECT '#RS2',@Starttime,@EndTime,DATEDIFF(SS,@Starttime,@EndTime) AS TimeTakeninsecs
  SET @Starttime = GETDATE()
  END

  SELECT
    DSO.shipmentNumber upsShipmentNumber,
    SIL.ClientASNNumber clientASNNumber,
    --SIL.ReceiptNumber upsASNNumber
	CASE WHEN SIL.SourceSystemKey = 1002 THEN UPSASNNumber ELSE ISNULL(ReceiptNumber,UPSASNNumber) END AS upsASNNumber
  FROM #SummaryTable DSO 
  INNER JOIN [Summary].[DIGITAL_SUMMARY_INBOUND_LINE] (NOLOCK) SIL 
			 ON DSO.shipmentNumber = SIL.UPSOrderNumber
			 AND DSO.SourceSystemKey = CASE WHEN DSO.SourceSystemKey=1011 
									   THEN SIL.SourceSystemKey 
									   ELSE  DSO.SourceSystemKey END
			WHERE ( SIL.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR SIL.AccountId = @VarAccountID )
			AND ((SIL.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR SIL.DP_SERVICELINE_KEY = @VarDPServiceLineKey) OR @VarDPServiceLineKey = '*')
  GROUP BY DSO.shipmentNumber,SIL.ClientASNNumber,SIL.ReceiptNumber,SIL.SourceSystemKey,UPSASNNumber

  IF @Debug>0
  BEGIN
  SET @EndTime = GETDATE()
  SELECT '#RS3',@Starttime,@EndTime,DATEDIFF(SS,@Starttime,@EndTime) AS TimeTakeninsecs
  SET @Starttime = GETDATE()
  END

  SELECT
    DSO1.upsShipmentNumber upsShipmentNumber,
    DSOT.TRACKING_NUMBER trackingNumber,
    DSOT.CarrierCode carrierCode,
    DSOT.CarrierType carrierMode
  FROM #SummaryTable DSO1
  INNER JOIN [Summary].[DIGITAL_SUMMARY_ORDER_TRACKING] (NOLOCK) DSOT 
			 ON DSO1.upsShipmentNumber = DSOT.UPSOrderNumber 
			 AND DSO1.SourceSystemKey = DSOT.SourceSystemKey
			 where ( DSOT.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR DSOT.AccountId = @VarAccountID )
			 AND ((DSOT.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR DSOT.DP_SERVICELINE_KEY = @VarDPServiceLineKey) OR @VarDPServiceLineKey = '*')

 IF @Debug>0
  BEGIN
  SET @EndTime = GETDATE()
  SELECT '#RS4',@Starttime,@EndTime,DATEDIFF(SS,@Starttime,@EndTime) AS TimeTakeninsecs
  SET @Starttime = GETDATE()
  END
GO

