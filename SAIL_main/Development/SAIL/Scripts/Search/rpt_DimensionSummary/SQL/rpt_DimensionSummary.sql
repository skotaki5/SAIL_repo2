/****** Object:  StoredProcedure [digital].[rpt_DimensionSummary]    Script Date: 3/11/2022 1:51:43 PM ******/
SET ANSI_NULLS ON
GO
 
SET QUOTED_IDENTIFIER ON
GO
 
 
 
 
 
/****
CHANGE LOG:
------------
DEVELOPER               DATE                SPRINT                      COMMENTS
ARUP                    10/08/2021      SPRINT-40 CL277             Added @Accountkeys to accept multiple accounts
Harsha                  2/4/2022        SPRINT-48 CL378             Filter condition to remove 'Not Available' and Null values
Harsha                  2/7/2022        SPRINT-48 CL378             Added filter to remove NULL for (IF  @VarResultsetType='DESTINATIONSUMMARY')
SAGAR                   02/10/2022      SPRINT-49 CL391             SHELL CREATED FOR Add filter values for Order Type (distinct order-type from last 90 days), Shell Values - Ecom, null
Avinash                 02/15/2022                                  Added missing alias name
Sheetal                 02/25/2022      SPRINT-50-CL405             New Result Set for OrderType
Sheetal                 02/25/2022      SPRINT50 - CL409            SP should accept multiple shipmentType (comma separated) and return output/result set accordingly
 
 
EXEC [digital].[rpt_DimensionSummary] 
@DPProductLineKey = 'EE8F34A6-884E-4349-A17E-D4FA2A60EF0E', --50441704-2662-4338-98A8-3CFA9DCE1D45 --EE8F34A6-884E-4349-A17E-D4FA2A60EF0E
@DPServiceLineKey = '*',@DPEntityKey = NULL,
@Date = null,
@ShipmentType = '*', @ResultsetType = 'ORDERTYPESUMMARY' --DESTINATIONSUMMARY –ORIGINSUMMARY
 
EXEC [digital].[rpt_DimensionSummary] 
@DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',
@DPServiceLineKey = '*',@DPEntityKey = NULL,
@Date = null,
@ShipmentType = 'Inbound,Outbound', @ResultsetType = 'ORDERTYPESUMMARY'
 
 
****/
 
CREATE PROCEDURE [digital].[rpt_DimensionSummary]
 
@DPProductLineKey varchar(50) = NULL 
,@DPServiceLineKey varchar(50) = NULL
,@DPEntityKey varchar(50) = NULL
,@AccountKeys nvarchar(max) = NULL   --CL277
,@Date nvarchar(max)=NULL
,@ShipmentType varchar(50)
,@ResultsetType varchar(50)='*'
 
AS
 
BEGIN
 
  DECLARE @VarAccountID varchar(50),
          @VarDPServiceLineKey varchar(50),
          @VarDPEntityKey varchar(50),
          @VarShipmentType varchar(50),
          @NULLShipmentType varchar(1),
          @Is_Inbound           INT,
          @VarResultsetType varchar(50),
          @shipmentCreationStartDate date,
          @shipmentCreationEndDate date,
          @shipmentCreationStartDateTime datetime,
          @shipmentCreationEndDateTime datetime,
          @NULLCreatedDate varchar(1)
 
 
  SELECT  @shipmentCreationStartDate = shipmentCreationStartDate,
          @shipmentCreationEndDate   = shipmentCreationEndDate
           
FROM OPENJSON(@Date)
WITH (
shipmentCreationStartDate date,
shipmentCreationEndDate date
 
     )
 
  SET @VarAccountID = UPPER(@DPProductLineKey)
  SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)
  SET @VarDPEntityKey = UPPER(@DPEntityKey)
  SET @VarShipmentType = UPPER(@ShipmentType)
  SET @VarResultsetType = UPPER(@ResultsetType)
 
  SET @shipmentCreationStartDateTime=@shipmentCreationStartDate
  SET @shipmentCreationEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @shipmentCreationEndDate)))
 
 
----CL277
 SELECT UPPER(DPProductLineKey) AS DPProductLineKey,   
       UPPER(DPServiceLineKey) AS DPServiceLineKey   
    into #ACCOUNTINFO   
    FROM OPENJSON(@AccountKeys)   
    WITH(   
   DPProductLineKey VARCHAR(MAX),   
   DPServiceLineKey VARCHAR(MAX)   
    )  
 
   
  IF ISNULL(@shipmentCreationStartDate,'')='' OR ISNULL(@shipmentCreationEndDate,'')=''
    SET @NULLCreatedDate = '*'
 
  IF @DPServiceLineKey IS NULL
    SET @VarDPServiceLineKey = '*'
 
  IF @DPEntityKey IS NULL
    SET @VarDPEntityKey = '*'
     
--SPRINT50 - CL409
  CREATE TABLE #SHIPMENTTYPE (SHIP_TYPE INT)  
 
IF @VarShipmentType='' OR ISNULL(@VarShipmentType,'*')='*'
SET @NULLShipmentType = '*'
--BEGIN 
--      INSERT INTO #SHIPMENTTYPE VALUES (1) 
--   INSERT INTO #SHIPMENTTYPE VALUES (2) 
--   INSERT INTO #SHIPMENTTYPE VALUES (0) 
--END 
 
 
IF @VarShipmentType LIKE '%INBOUND%' 
INSERT INTO #SHIPMENTTYPE VALUES (1) 
     
IF @VarShipmentType LIKE '%MOVEMENT%' 
INSERT INTO #SHIPMENTTYPE VALUES (2) 
   
IF @VarShipmentType LIKE '%OUTBOUND%' 
INSERT INTO #SHIPMENTTYPE VALUES (0) 
 
--SPRINT50 - CL409
 
  IF ISNULL(@VarResultsetType,'')=''
     SET @VarResultsetType='*'
 
 
 
     
   
  SELECT
  UPPER(O.OriginCity) AS OriginCity,   --Added missing alias name
  UPPER(O.OriginCountry) AS OriginCountry,  --Added missing alias name
  UPPER(O.DestinationCity) AS DestinationCity,  --Added missing alias name
  UPPER(O.DestinationCountry) AS DestinationCountry,  --Added missing alias name
  CASE WHEN O.OrderType = '' THEN NULL ELSE UPPER(O.OrderType) END AS OrderType  --CL405
  INTO #DIGITAL_SUMMARY_ORDERS
  FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O
  WHERE  ( O.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR O.AccountId = @VarAccountID )  --CL277
  AND ((O.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR O.DP_SERVICELINE_KEY = @VarDPServiceLineKey)  OR @VarDPServiceLineKey = '*') --CL277
  AND ((O.DateTimeReceived BETWEEN @shipmentCreationStartDateTime  AND @shipmentCreationEndDateTime ) OR @NULLCreatedDate = '*')
  AND (@NULLShipmentType='*' OR IS_INBOUND IN (SELECT SHIP_TYPE FROM #SHIPMENTTYPE))   --SPRINT50 - CL409
  
 
 
  IF  @VarResultsetType='ORIGINSUMMARY'
  BEGIN
   SELECT DISTINCT
   OriginCity,
   OriginCountry
   FROM #DIGITAL_SUMMARY_ORDERS
   --CL378
   WHERE (OriginCity IS NOT NULL AND OriginCity NOT LIKE '%NOT AVAILABLE%') AND
            (OriginCountry IS NOT NULL AND OriginCountry NOT LIKE '%NOT AVAILABLE%')
   --CL378
   ORDER BY OriginCountry
  END
 
   IF  @VarResultsetType='DESTINATIONSUMMARY'
  BEGIN
   SELECT DISTINCT
   DestinationCity,
   DestinationCountry
   FROM #DIGITAL_SUMMARY_ORDERS
   --CL378
   WHERE (DestinationCity IS NOT NULL AND DestinationCity NOT LIKE '%NOT AVAILABLE%') AND
            (DestinationCountry IS NOT NULL AND DestinationCountry NOT LIKE '%NOT AVAILABLE%')
   --CL378
   ORDER BY DestinationCountry
  END
 
 -- CL405
 
  IF  @VarResultsetType='ORDERTYPESUMMARY'
  BEGIN
   SELECT DISTINCT
   OrderType
   FROM #DIGITAL_SUMMARY_ORDERS
   ORDER BY OrderType
  END
 
--CL405
 
  IF  @VarResultsetType='*'
  BEGIN
 
  SELECT DISTINCT
   OriginCity,
   OriginCountry
  FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK)
   --CL378
  WHERE (OriginCity IS NOT NULL AND OriginCity NOT LIKE '%NOT AVAILABLE%') AND
            (OriginCountry IS NOT NULL AND OriginCountry NOT LIKE '%NOT AVAILABLE%')
   --CL378
   ORDER BY OriginCountry
 
   SELECT DISTINCT
   DestinationCity,
   DestinationCountry
   FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK)
   --CL378
   WHERE (DestinationCity IS NOT NULL AND DestinationCity NOT LIKE '%NOT AVAILABLE%') AND
            (DestinationCountry IS NOT NULL AND DestinationCountry NOT LIKE '%NOT AVAILABLE%')
   --CL378
   ORDER BY DestinationCountry
 
  --CL405
  SELECT DISTINCT
   OrderType
   FROM #DIGITAL_SUMMARY_ORDERS
   ORDER BY OrderType
   --CL405
 
  END
 
END
GO