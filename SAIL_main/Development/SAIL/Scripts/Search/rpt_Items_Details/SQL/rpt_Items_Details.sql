/****** Object:  StoredProcedure [digital].[rpt_Items_Details]    Script Date: 1/12/2022 12:41:06 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




  
  
      
      
/****      
      
CHANGE LOG      
----------      
CHANGED BY			DATE          SPRINT				CHANGES      
CHAITANYA			08/24/2021    36th					 ADDED NEW RESULTSET FOR BATCH STATUS,ADDED BATCH NUMBER,      
														             BATCH STATUS,EXPIRATION DATE INPUT PARAMETER      
      
VENKATA				08/26/2021    36					CHANGED LOGIC FOR 'inventoryReasonCode_nonAvailableQuantity' COLUMN, CHANGED LOGIC FOR AVAILABLE QUANTITY IN NEW RESULT SET (SPRINT 36)       
PRASHANT			08/31/2021    36th					REASON CODE INCLUSION    
PRASHANT/Venkata    09/21/2021    38th					inventoryReasonCode_nonAvailableQuantity IS MAPPED TO HoldDESCRIPTION COLUMN  
VENKATA				09/16/2021							Changed ItemQuantity logic (sum(AvailableQty) to Count(distinct batchnumber)) in the final result set
Venkata/Arun		09/28/2021	  39th					Made changes to Reasoncode logic
Anand				09/29/2021	  39th					Changed @itemNumber to accept '*'
Venkata				10/13/2021	  UPSGLD-11465			Changed @warehouseID to accept '' and null values
Revathy             11/15/2021    UPSGLD-10302          Added Group by  Designator,LPN,Serial,Lot     
    
      
      
EXEC [digital].[rpt_Items_Details] @DPProductLineKey = N'1EEF1B1A-A415-43F3-88C5-2D5EBC503529',  @dpServiceLineKey = NULL,@DPEntityKey=NULL, @itemNumber = N'186-1601-900', @WarehouseId = Null,@Designator='{"Designator": ["Refurbished","Test"]}'      
      
---AMR      
EXEC [digital].[rpt_Items_Details] @DPProductLineKey = 'E648FA6F-6253-428E-8AC9-201E3EF83B91',@DPServiceLineKey = '53D60776-D3BD-4E6A-8E37-F591C148294B',@DPEntityKey = NULL,@itemNumber = 'BP26205BBR',@WarehouseId='*',      
@warehouse ='{"warehouseName": ["KYSPL"],"warehouseID": ["9CD72E16-9446-46E7-8A25-08D7BEC3176E"]}'      

--Sprint 39
EXEC [digital].[rpt_Items_Details]
@DPProductLineKey = 'C7FCE5C2-FFAF-4700-90FE-AEF1A9780795',
@DPServiceLineKey = '*',
@DPEntityKey = NULL,
@itemNumber = '0594-0201',
@WarehouseId='*',
@warehouse ='*' ,
@BatchStatus = '{"BatchStatus":["Released,Held"]}',
@ReasonCode = '{"ReasonCode":["NA,Expired"]}'

****/      
      
CREATE PROC [digital].[rpt_Items_Details]       
      
@DPProductLineKey varchar(50)=NULL,       
@DPServiceLineKey varchar(50)=NULL,       
@DPEntityKey varchar(50)=NULL,       
@AccountKeys nvarchar(max) = NULL,      
@batchNumber nvarchar(max) = '{"batchNumber":["*"]}',      
@isBatchInformationRequired VARCHAR(1) = 'Y',      
@itemNumber varchar(128) = NULL,      
@WarehouseId varchar(max)='*',       
@warehouse nvarchar(max)='*',      
@Designator nvarchar(max)='*',      
@LPN nvarchar(max)='*',      
@VSN nvarchar(max)='*',      
@VCL nvarchar(max)='*',      
@InboundReferenceNumber nvarchar(max)='*',      
@ExpirationDate varchar(max) = NULL,      
@BatchStatus nvarchar(max) = '{"BatchStatus":["*"]}',      
@ReasonCode nvarchar(max) = '{"ReasonCode":["*"]}'      
      
AS      
BEGIN      
      
  DECLARE @VarAccountID varchar(50),      
          @VarDPServiceLineKey varchar(50),      
          @VarDPEntityKey varchar(50),      
          @VarItemNumber varchar(128),      
          @NULLItemNumber varchar(1),      
    @VarwarehouseId varchar(max),      
    @VarwarehouseIds varchar(max),      
    @VarwarehouseNames varchar(max),      
    @VarDesignator varchar(max),      
    @VarLPN nvarchar(max),      
    @VarVSN nvarchar(max),      
    @VarVCL nvarchar(max),      
    @VarInboundReferenceNumber nvarchar(max),      
    @VarDPServiceLineKeyJSON VARCHAR(500),      
    @VarDPProductLineKeyJSON VARCHAR(500),      
    @VarbatchNumberArray NVARCHAR(MAX),      
 @VarbatchNumber nvarchar(max),      
    @VarBatchStatus nvarchar(max),      
    @ExpirationStartDate date,      
    @ExpirationEndDate date,      
    @ExpirationStartDateTime datetime,      
    @ExpirationEndDateTime datetime,      
    @NULLExpirationDate varchar(1),    
 @VarReasonCode nvarchar(max)    
      
IF @warehouse='*'      
Begin       
SET @VarwarehouseIds='*'      
SET @VarwarehouseNames='*'      
END      
ELSE      
BEGIN      
select @VarwarehouseIds=JSON_QUERY(@warehouse,  '$.warehouseID')      
select @VarwarehouseNames=JSON_QUERY(@warehouse,'$.warehouseName')      
END      
          
SELECT @VarwarehouseIds=REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@VarwarehouseIds, CHAR(13), ''), CHAR(10), ''),'"',''),'[',''),']',''),      
       @VarwarehouseNames=REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@VarwarehouseNames, CHAR(13), ''), CHAR(10), ''),'"',''),'[',''),']','')      
      
  SET @VarwarehouseIds=REPLACE(@VarwarehouseIds,' ','')      
  SET @VarwarehouseNames=REPLACE(@VarwarehouseNames,' ','')      
      
IF @Designator='*'      
Begin       
SET @VarDesignator='*'      
END      
ELSE      
BEGIN      
select @VarDesignator=isnull(JSON_QUERY(@Designator,  '$.Designator'),JSON_QUERY(@Designator,  '$.designator'))          
END      
      
  SELECT  @VarDesignator=REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@VarDesignator, CHAR(13), ''), CHAR(10), ''),'"',''),'[',''),']','')      
  SET @VarDesignator=REPLACE(@VarDesignator,' ','')      
      
 IF @LPN='*'      
 Begin       
 SET @VarLPN='*'      
 END      
 ELSE      
 BEGIN      
 SELECT @VarLPN=isnull(JSON_QUERY(@LPN,  '$.LPN'),JSON_QUERY(@LPN,  '$.lpn'))      
 END      
      
 SELECT  @VarLPN=REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@VarLPN, CHAR(13), ''), CHAR(10), ''),'"',''),'[',''),']','')      
 SET @VarLPN=REPLACE(@VarLPN,' ','')      
      
 IF @VSN='*'      
 Begin       
 SET @VarVSN='*'      
 END      
 ELSE      
 BEGIN      
 SELECT @VarVSN=isnull(JSON_QUERY(@VSN,  '$.VSN'),JSON_QUERY(@VSN,  '$.vsn'))         
 END      
      
 SELECT  @VarVSN=REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@VarVSN, CHAR(13), ''), CHAR(10), ''),'"',''),'[',''),']','')      
 SET @VarVSN=REPLACE(@VarVSN,' ','')      
      
      
      
      
      
 IF @VCL='*'      
 Begin       
 SET @VarVCL='*'      
 END      
 ELSE      
 BEGIN      
 SELECT @VarVCL=isnull(JSON_QUERY(@VCL,  '$.VCL'),JSON_QUERY(@VCL,  '$.vcl'))         
 END      
      
 SELECT  @VarVCL=REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@VarVCL, CHAR(13), ''), CHAR(10), ''),'"',''),'[',''),']','')      
 SET @VarVCL=REPLACE(@VarVCL,' ','')      
      
 IF @batchNumber='*'      
 Begin       
 SET @VarbatchNumber='*'      
 END      
 ELSE      
 BEGIN      
 SELECT @VarbatchNumber=isnull(JSON_QUERY(@batchNumber,  '$.batchNumber'),JSON_QUERY(@batchNumber,  '$.batchNumber'))         
 END      
      
 SELECT  @VarbatchNumber=REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@VarbatchNumber, CHAR(13), ''), CHAR(10), ''),'"',''),'[',''),']','')      
 SET @VarbatchNumber=REPLACE(@VarbatchNumber,' ','')      
      
 IF @BatchStatus='*'      
 Begin       
 SET @VarBatchStatus='*'      
 END      
 ELSE      
 BEGIN      
 SELECT @VarBatchStatus=isnull(JSON_QUERY(@BatchStatus,  '$.BatchStatus'),JSON_QUERY(@BatchStatus,  '$.BatchStatus'))         
 END      
      
 SELECT  @VarBatchStatus=REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@VarBatchStatus, CHAR(13), ''), CHAR(10), ''),'"',''),'[',''),']','')      
 SET @VarBatchStatus=REPLACE(@VarBatchStatus,' ','')      
      
       
      
IF @ExpirationDate <>'*'      
BEGIN      
SELECT  @ExpirationStartDate  = ExpirationStartDate,      
        @ExpirationEndDate    = ExpirationEndDate      
FROM OPENJSON(@ExpirationDate)      
WITH (      
ExpirationStartDate date,      
ExpirationEndDate date      
     )      
END      
      
IF ISNULL(@ExpirationStartDate,'')='' OR ISNULL(@ExpirationEndDate,'')=''      
    SET @NULLExpirationDate = '*'      
      
SET @ExpirationStartDateTime=@ExpirationStartDate      
SET @ExpirationEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @ExpirationEndDate)))      
      
      
 IF @InboundReferenceNumber='*'      
 Begin       
 SET @VarInboundReferenceNumber='*'      
 END      
 ELSE      
 BEGIN      
 SELECT @VarInboundReferenceNumber =isnull(JSON_QUERY(@InboundReferenceNumber,  '$.InboundReferenceNumber'),JSON_QUERY(@InboundReferenceNumber,  '$.inboundreferencenumber'))           
 END      
      
 SELECT  @VarInboundReferenceNumber=REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@VarInboundReferenceNumber, CHAR(13), ''), CHAR(10), ''),'"',''),'[',''),']','')      
-- SET @VarInboundReferenceNumber=REPLACE(@VarInboundReferenceNumber,' ','')      
    
    
 -----------------------Reason Code------------------------------------------    
    
 IF @ReasonCode='*'    
 Begin     
 SET @VarReasonCode='*'    
 END    
 ELSE    
 BEGIN    
 SELECT @VarReasonCode=isnull(JSON_QUERY(@VarReasonCode,  '$.ReasonCode'),JSON_QUERY(@ReasonCode,  '$.ReasonCode'))       
 END    
    
 SELECT  @VarReasonCode=REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@VarReasonCode, CHAR(13), ''), CHAR(10), ''),'"',''),'[',''),']','')    
 SET @VarReasonCode=REPLACE(@VarReasonCode,' ','')    
      
      
      
  SET @VarAccountID = UPPER(@DPProductLineKey)      
  SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)      
  SET @VarDPEntityKey = UPPER(@DPEntityKey)      
  SET @VarItemNumber = @itemNumber      
  SET @VarwarehouseId = UPPER(@warehouseId)      
      
  -----SHIPMENTARRAY      
      
--DROP TABLE IF EXISTS #batchNumberARRAY      
      
--SELECT   CASE WHEN batchNumberARRAY='NULL' THEN 'NOBATCH' ELSE batchNumberARRAY END AS batchNumberARRAY       
--INTO #batchNumberARRAY      
--     FROM OPENJSON(@batchNumber)      
--     WITH (      
--    batchNumber nvarchar(max) 'strict $.batchNumber' AS JSON      
-- )      
-- OUTER APPLY OPENJSON(batchNumber) WITH (batchNumberARRAY NVARCHAR(MAX) '$');      
      
  SELECT UPPER(DPProductLineKey) AS DPProductLineKey,      
       UPPER(DPServiceLineKey) AS DPServiceLineKey      
    into #ACCOUNTINFO      
    FROM OPENJSON(@AccountKeys)      
    WITH(      
   DPProductLineKey VARCHAR(MAX),      
   DPServiceLineKey VARCHAR(MAX)      
    )      
      
      
  IF @DPServiceLineKey IS NULL      
    SET @VarDPServiceLineKey = '*'      
      
  IF @DPEntityKey IS NULL      
    SET @VarDPEntityKey = '*'      
      
  IF @VarItemNumber IS NULL OR  @VarItemNumber='*'    
    SET @NULLItemNumber = '*'      
       
  IF @VarwarehouseIds IS NULL      
     SET @VarwarehouseIds = '*'
	 
 IF @VarwarehouseId IS NULL OR  @VarwarehouseId = '' --UPSGLD-11465
     SET @VarwarehouseId = '*'
      
   IF @VarwarehouseNames IS NULL      
     SET @VarwarehouseNames = '*'      
      
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
 SI.itemNumber,      
    itemDescription,      
    hazardClass,      
    itemDimensions_length,      
    itemDimensions_width,      
    itemDimensions_height,      
    itemDimensions_unitOfMeasurement_code AS itemDimensions_unitOfMeasurement,      
    itemWeight_weight,      
    itemWeight_unitOfMeasurement_Code AS itemWeight_unitOfMeasurement,      
    SI.FacilityId AS warehouseId,      
    warehouseCode AS warehouseCode,      
 SourceSystemKey,      
 SUM(availableQuantity) availableQuantity,      
 SUM(nonAvailableQuantity) nonAvailableQuantity,      
'[{"Designator":'+ '"'+ ISNULL(Designator,'')+ '"'+'}]'  AS Designator,      
  '[{"LPN":'+ '"'+ ISNULL(LPNNumber,'')+ '"'+'}]'  AS LPN,      
  '[{"VSN":'+ '"'+ ISNULL(VendorSerialNumber,'')+ '"'+'}]'  AS VSN,      
  '[{"VCL":'+ '"'+ ISNULL(VendorLotNumber,'')+ '"'+'}]'  AS VCL,           
  '{"nonAvailabilityReasonCode":'+ '['+ '"' + ISNULL(SI.HoldDescription,'')+ '"'+']}'  AS inventoryReasonCode_nonAvailableQuantity,  --09/21/2021     
  --'{"nonAvailabilityReasonCode":'+ '['+ '"'+ STRING_AGG(ISNULL(SI.BatchHoldReason,''),', ')+ '"'+ ']'+'}'  AS inventoryReasonCode_nonAvailableQuantity,      
  --'{"Designator":['+ STRING_AGG(ISNULL('"'+Designator+'"',''),', ')+']}'  AS Designator,      
  --'{"LPN":['+ STRING_AGG(ISNULL('"'+LPNNumber+'"',''),', ')+']}'  AS LPN,      
  --'{"VSN":['+ STRING_AGG(ISNULL('"'+VendorSerialNumber+'"',''),', ')+']}'  AS VSN,      
  --'{"VCL":['+ STRING_AGG(ISNULL('"'+VendorLotNumber+'"',''),', ')+']}'  AS VCL,      
 SI.InvRef1 AS referenceNumber1,      
 SI.InvRef2 AS referenceNumber2,      
 SI.InvRef3 AS referenceNumber3,      
 SI.InvRef4 AS referenceNumber4,      
 SI.InvRef5 AS referenceNumber5,      
 HazmatClass AS hazmatClass,      
 StrategicGoodsFlag AS strategicGoodsFlag,      
 UNNumber AS UNCode,      
 CASE WHEN @isBatchInformationRequired='Y' THEN SI.BatchStatus ELSE NULL END AS batchStatus,      
 SI.Account_number,      
 CASE WHEN @isBatchInformationRequired='Y' THEN SI.ExpirationDate ELSE NULL END AS expirationDate,      
 CASE WHEN @isBatchInformationRequired='Y' THEN       
           VendorLotNumber      
  ELSE NULL END AS batchNumber,    
  CASE WHEN @isBatchInformationRequired='Y' THEN   
    CASE WHEN SI.BatchStatus = 'Released' THEN 'NA' ELSE SI.BatchHoldReason END   
  ELSE NULL END AS reasonCode    --09/28/2021
       
    
  INTO #DIGITAL_SUMMARY_INVENTORY       
  FROM [Summary].[DIGITAL_SUMMARY_INVENTORY] SI (NOLOCK)     
  WHERE (SI.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR AccountId = @VarAccountID )      
  AND ((SI.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR SI.DP_SERVICELINE_KEY = @VarDPServiceLineKey)  OR @VarDPServiceLineKey = '*')      
  AND (SI.FacilityId IN (SELECT UPPER (TRIM (value)) FROM string_split(@VarwarehouseId, ',')) OR @VarwarehouseId = '*')      
  AND (SI.FacilityId IN (SELECT UPPER (TRIM (value)) FROM string_split(@VarwarehouseIds, ',')) OR @VarwarehouseIds = '*')      
  AND (SI.warehouseCode IN (SELECT UPPER (TRIM (value)) FROM string_split(@VarwarehouseNames, ',')) OR @VarwarehouseNames = '*')    
  --AND (SI.BatchHoldReason COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarReasonCode, ',')) OR @VarReasonCode = '*')    --09/28/2021
  AND (SI.Designator COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value)   FROM string_split(@VarDesignator, ',')) OR @VarDesignator = '*')      
  AND (SI.LPNNumber COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarLPN, ',')) OR @VarLPN = '*')      
  AND (SI.VendorSerialNumber COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarVSN, ',')) OR @VarVSN = '*')      
  AND (SI.VendorLotNumber COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarVCL, ',')) OR @VarVCL = '*')      
  AND (SI.VendorLotNumber COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarbatchNumber, ',')) OR @VarbatchNumber = '*')      
  --AND (SI.BatchStatus COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarBatchStatus, ',')) OR @VarBatchStatus = '*')      --09/28/2021
  AND ((SI.InvRef1 COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarInboundReferenceNumber, ',')) OR @VarInboundReferenceNumber = '*')      
  OR (SI.InvRef2 COLLATE SQL_Latin1_General_CP1_CI_AS  IN (SELECT TRIM (value) FROM string_split(@VarInboundReferenceNumber, ',')) OR @VarInboundReferenceNumber = '*')      
  OR (SI.InvRef3 COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarInboundReferenceNumber, ',')) OR @VarInboundReferenceNumber = '*')      
  OR (SI.InvRef4 COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarInboundReferenceNumber, ',')) OR @VarInboundReferenceNumber = '*')      
  OR (SI.InvRef5 COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarInboundReferenceNumber, ',')) OR @VarInboundReferenceNumber = '*')       
  )      
  AND (SI.itemNumber = @VarItemNumber OR @NULLItemNumber = '*')      
  AND ((SI.ExpirationDate BETWEEN @ExpirationStartDateTime AND @ExpirationEndDateTime) OR @NULLExpirationDate = '*')      
    GROUP BY       
 SI.itemNumber,        
    itemDescription,        
    hazardClass,        
    itemDimensions_length,        
    itemDimensions_width,        
    itemDimensions_height,        
    itemDimensions_unitOfMeasurement_code,       
    itemWeight_weight,        
    itemWeight_unitOfMeasurement_Code,      
    SI.FacilityId,      
    warehouseCode,        
    SourceSystemKey,        
 --availableQuantity,        
 --nonAvailableQuantity,  
 Designator,
 LPNNumber,
 VendorSerialNumber,
 VendorLotNumber,
 SI.InvRef1,        
 SI.InvRef2,        
 SI.InvRef3,        
 SI.InvRef4,        
 SI.InvRef5,        
 HazmatClass,      
 StrategicGoodsFlag,       
 UNNumber,        
 CASE WHEN @isBatchInformationRequired='Y' THEN SI.BatchStatus ELSE NULL END,      
 SI.Account_number,        
 CASE WHEN @isBatchInformationRequired='Y' THEN SI.ExpirationDate ELSE NULL END,      
 CASE WHEN @isBatchInformationRequired='Y' THEN VendorLotNumber ELSE NULL END,      
 SI.HoldDescription ,    
 CASE WHEN @isBatchInformationRequired='Y' THEN   
    CASE WHEN SI.BatchStatus = 'Released' THEN 'NA' ELSE SI.BatchHoldReason END   
  ELSE NULL END    --09/28/2021
       
      
      
  SELECT        
    SI.itemNumber,      
    itemDescription,      
    hazardClass,      
    itemDimensions_length,      
    itemDimensions_width,      
    itemDimensions_height,      
    itemDimensions_unitOfMeasurement,      
    itemWeight_weight,      
    itemWeight_unitOfMeasurement,      
     warehouseId,      
    warehouseCode AS warehouseCode,      
 Max(CAST(availableQuantity AS INT)) AS availableQuantity,      
 inventoryReasonCode_nonAvailableQuantity,      
 Max(CAST(nonAvailableQuantity AS INT)) AS nonAvailableQuantity,      
 Designator,      
 LPN AS LPN,      
 VSN VSN ,      
 VCL VCL ,      
 referenceNumber1,      
 referenceNumber2,      
 referenceNumber3,      
 referenceNumber4,      
 referenceNumber5,      
 Max(CAST(availableQuantity AS INT)) AS onHandQuantity,      
 hazmatClass,      
 strategicGoodsFlag,      
 UNCode,      
 batchNumber,      
 expirationDate,      
 batchStatus,      
 --reasonCode,
  case when reasonCode = 'NA' then null else reasonCode end as reasonCode,   --09/28/2021
 Account_number AS Account      
  FROM #DIGITAL_SUMMARY_INVENTORY SI WHERE   
  (batchStatus COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarBatchStatus, ',')) OR @VarBatchStatus = '*')  --09/28/2021
  AND (reasonCode COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarReasonCode, ',')) OR @VarReasonCode = '*') --09/28/2021  
  GROUP BY warehouseId,      
 warehouseCode,      
 SI.itemNumber,      
    itemDescription,      
    hazardClass,      
    itemDimensions_length,      
    itemDimensions_width,      
    itemDimensions_height,      
    itemDimensions_unitOfMeasurement,      
 itemWeight_weight,      
    itemWeight_unitOfMeasurement,      
 SourceSystemKey,      
 referenceNumber1,      
 referenceNumber2,      
 referenceNumber3,      
 referenceNumber4,      
 referenceNumber5,      
 hazmatClass,      
 strategicGoodsFlag,      
    UNCode,      
 LPN,      
 VSN,      
 VCL,      
 Designator,      
 batchStatus,      
 Account_number,      
 expirationDate,      
 batchNumber,      
 inventoryReasonCode_nonAvailableQuantity,    
 reasonCode    
      
 SELECT DISTINCT batchNumber AS BatchNumber FROM #DIGITAL_SUMMARY_INVENTORY      
 --FROM [Summary].[DIGITAL_SUMMARY_INVENTORY] SI      
 -- WHERE (SI.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR AccountId = @VarAccountID )      
 -- AND ((SI.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR SI.DP_SERVICELINE_KEY = @VarDPServiceLineKey)  OR @VarDPServiceLineKey = '*')      
 -- AND (SI.FacilityId IN (SELECT UPPER (TRIM (value)) FROM string_split(@VarwarehouseId, ',')) OR @VarwarehouseId = '*')      
 -- AND (SI.FacilityId IN (SELECT UPPER (TRIM (value)) FROM string_split(@VarwarehouseIds, ',')) OR @VarwarehouseIds = '*')      
 -- AND (SI.warehouseCode IN (SELECT UPPER (TRIM (value)) FROM string_split(@VarwarehouseNames, ',')) OR @VarwarehouseNames = '*')      
 -- AND (SI.Designator COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value)   FROM string_split(@VarDesignator, ',')) OR @VarDesignator = '*')      
 -- AND (SI.LPNNumber COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarLPN, ',')) OR @VarLPN = '*')      
 -- AND (SI.VendorSerialNumber COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarVSN, ',')) OR @VarVSN = '*')      
 -- --AND (SI.VendorLotNumber COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarVCL, ',')) OR @VarVCL = '*')      
 -- --AND (SI.VendorLotNumber COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarbatchNumber, ',')) OR @VarbatchNumber = '*')      
 -- AND (SI.BatchStatus COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarBatchStatus, ',')) OR @VarBatchStatus = '*')      
 -- AND ((SI.InvRef1 COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarInboundReferenceNumber, ',')) OR @VarInboundReferenceNumber = '*')      
 -- OR (SI.InvRef2 COLLATE SQL_Latin1_General_CP1_CI_AS  IN (SELECT TRIM (value) FROM string_split(@VarInboundReferenceNumber, ',')) OR @VarInboundReferenceNumber = '*')      
 -- OR (SI.InvRef3 COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarInboundReferenceNumber, ',')) OR @VarInboundReferenceNumber = '*')      
 -- OR (SI.InvRef4 COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarInboundReferenceNumber, ',')) OR @VarInboundReferenceNumber = '*')      
 -- OR (SI.InvRef5 COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarInboundReferenceNumber, ',')) OR @VarInboundReferenceNumber = '*')       
 -- )      
 -- AND (SI.itemNumber = @VarItemNumber OR @NULLItemNumber = '*')      
 -- AND ((SI.ExpirationDate BETWEEN @ExpirationStartDateTime AND @ExpirationEndDateTime) OR @NULLExpirationDate = '*')      
        
  
 SELECT batchStatus AS BatchStatus,
		COALESCE(NULLIF(reasonCode, 'NA'),'') AS ReasonCode, --09/28/2021
      --ISNULL(reasonCode, '') AS ReasonCode,  
  --SUM(CAST(availableQuantity AS INTEGER)) as ItemQuantity  
        Count(distinct batchNumber) as ItemQuantity  
  FROM #DIGITAL_SUMMARY_INVENTORY      
  GROUP BY batchStatus,    
     reasonCode   
  
   
END   
GO

