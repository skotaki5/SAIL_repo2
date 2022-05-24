/****** Object:  StoredProcedure [digital].[rpt_Items_Search]    Script Date: 12/8/2021 4:05:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


  
/****  
  
CHANGE LOG  
----------  
CHANGED BY		DATE				SPRINT			CHANGES  
CHAITANYA		08/24/2021							ADDED WAREHOUSE CODE AND BATCH STATUS IN RESPONSE,ADDED BATCH STATUS AS INPUT PARAMETER  
CHAITANYA		08/31/2021			36th			REASON CODE INCLUSION  
Venkata			09/13/2021							added WHERE isnull(FacilityId,'') <> '' condtion in final select query   
Chaitanya       09/15/2021							added where condition on item number to show the batch details  
Venkata			09/20/2021							Made @SearchBy and @SearchValue variables as optional  
Venkata         09/30/2021			39th			Made changes to Batch Status and Reasoncode logic to accept multiple values  
Venkata			09/30/2021			defect			added where condition on warehousecode to get the distinct values
Venkata			10/22/2021			UPSGLD-11576	added new table #TEMP_DIGITAL_SUMMARY_INVENTORY instead of using the main table
Piyali          11/16/2021          Performance     Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement 
--------------------------------------------------------------------------------------------------------------------------------  
  
--AMR  
EXEC [digital].[rpt_Items_Search_Sprint38]   @DPProductLineKey = 'E648FA6F-6253-428E-8AC9-201E3EF83B91',@DPServiceLineKey = '53D60776-D3BD-4E6A-8E37-F591C148294B',@DPEntityKey = NULL,@SearchBy = 'itemdescription', @SearchValue = 'HARNESSIMAGING UNITSAFE S
WITCH',@WarehouseId='*',  
@warehouse ='{"warehouseName": ["WSDF4","KYNTP"],"warehouseID": ["9CD72E16-9446-46E7-8A25-08D7BEC3176E","ACD72E16-9988-46U7-8A27-08D7BEC3176F"]}'  
  
--Sprint 39  
EXEC  [digital].[rpt_Items_Search]  
@AccountKeys = N'[{"DPProductLineKey":"C7FCE5C2-FFAF-4700-90FE-AEF1A9780795","DPServiceLineKey":"*"}]"',  
@SearchBy = N'ITEMNUMBER',  
@SearchValue = N'',  
@WarehouseId = N'*',  
@BatchStatus = N'{"BatchStatus":["Released","Held"]}',  
@ReasonCode = '{"ReasonCode":["NA,Expired"]}'  

EXEC  [digital].[rpt_Items_Search]  
@AccountKeys = N'[{"DPProductLineKey":"8E422AC9-D880-4970-8793-23C76BCA1A16","DPServiceLineKey":"*"}]"',  
@SearchBy = N'ITEMNUMBER',  
@SearchValue = N'',  
@WarehouseId = N'*',  
@BatchStatus = N'{"BatchStatus":["Released","Held"]}',  
@ReasonCode = '{"ReasonCode":["NA,Expired"]}' 

  
****/  
  
  
CREATE PROC [digital].[rpt_Items_Search]   
  
@DPProductLineKey varchar(50)=NULL,   
@DPServiceLineKey varchar(50)=NULL,   
@DPEntityKey varchar(50)=NULL,   
@AccountKeys nvarchar(max) = NULL,  
@SearchBy varchar(50),   
@SearchValue varchar(240),   
@WarehouseId varchar(max)='*',  
@warehouse nvarchar(max)='*',  
@BatchStatus nvarchar(max) = '{"BatchStatus":["*"]}',  
@ReasonCode nvarchar(max) = '{"ReasonCode":["*"]}'  
  
AS  
BEGIN  
  
  DECLARE @VarAccountID varchar(50),  
          @VarDPServiceLineKey varchar(50),  
          @VarDPEntityKey varchar(50),  
          @VarSearchBy varchar(50),  
          @VarSearchValue varchar(240),  
          @VarwarehouseId varchar(max),  
          @NULLItemNumber varchar(1),  
          @NULLItemDescription varchar(1),  
    @VarwarehouseIds varchar(max),  
    @VarwarehouseNames varchar(max),  
    @VarDPServiceLineKeyJSON VARCHAR(500),  
    @VarDPProductLineKeyJSON VARCHAR(500),  
    @VarBatchStatus nvarchar(max),  
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
  
  SELECT UPPER(DPProductLineKey) AS DPProductLineKey,  
       UPPER(DPServiceLineKey) AS DPServiceLineKey  
    into #ACCOUNTINFO  
    FROM OPENJSON(@AccountKeys)  
    WITH(  
   DPProductLineKey VARCHAR(MAX),  
   DPServiceLineKey VARCHAR(MAX)  
    )  
  
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
  
  IF @SearchBy is NULL or @SearchBy = ''  --09/20/2021  
  SET @VarSearchBy = 'ITEMNUMBER'  
  ELSE  
  SET @VarSearchBy = UPPER(@SearchBy)  
  
  IF @SearchValue IS NULL  --09/20/2021  
  SET @VarSearchValue = ''  
  ELSE  
  SET @VarSearchValue = @SearchValue  
  
  SET @VarwarehouseId = UPPER(@warehouseId)  
  
  IF @DPServiceLineKey IS NULL  
    SET @VarDPServiceLineKey = '*'  
  
  IF @DPEntityKey IS NULL  
     SET @VarDPEntityKey = '*'  
  
  IF @VarwarehouseIds IS NULL  
     SET @VarwarehouseIds = '*'  
  
   IF @VarwarehouseNames IS NULL  
     SET @VarwarehouseNames = '*'  
  
  IF @VarSearchBy = 'ITEMNUMBER'  
    SET @NULLItemDescription = '*'  
  
  IF @VarSearchBy = 'ITEMDESCRIPTION'  
    SET @NULLItemNumber = '*'  
  
  IF @warehouse IS NULL  
  SET @VarwarehouseIds='*'  
  
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
  
 SELECT * INTO #TEMP_DIGITAL_SUMMARY_INVENTORY FROM [Summary].[DIGITAL_SUMMARY_INVENTORY] SI (NOLOCK)
 WHERE (SI.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR AccountId = @VarAccountID ) --UPSGLD-11576
 
  SELECT SI.SourceSystemKey  
,SI.SourceSystemName  
,SI.AccountId  
,SI.FacilityId  
,SI.itemNumber  
,SI.itemDescription  
--,SI.hazardClass                             -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement
--,SI.itemDimensions_length                   -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement
--,SI.itemDimensions_width                    -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement
--,SI.itemDimensions_height                   -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement
--,SI.itemDimensions_unitOfMeasurement_code   -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement
--,SI.itemWeight_weight                       -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement
--,SI.itemWeight_unitOfMeasurement_Code       -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement
,SI.warehouseCode  
--,SI.availableQuantity                       -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement
--,SI.nonAvailableQuantity                    -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement
--,SI.DP_SERVICELINE_KEY                      -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement
--,SI.DP_ORGENTITY_KEY                        -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement
--,SI.InvRef1                                 -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement
--,SI.InvRef2                                 -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement
--,SI.InvRef3                                 -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement
--,SI.InvRef4                                 -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement
--,SI.InvRef5                                 -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement
--,SI.LPNNumber                               -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement
--,SI.HazmatClass                             -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement
--,SI.StrategicGoodsFlag                      -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement
--,SI.UNNumber                                -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement
--,SI.Designator                              -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement
--,SI.VendorSerialNumber                      -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement
,SI.VendorLotNumber  
,SI.BatchStatus  
,SI.ExpirationDate  
--,SI.Account_number                          -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement 
,CASE WHEN SI.BatchStatus = 'Released' THEN 'NA' ELSE SI.BatchHoldReason END AS BatchHoldReason --09/30/2021  
--,SI.HoldDescription                         -- Removed from SELECT Statement of #DIGITAL_SUMMARY_INVENTORY for Performance Improvement  
  INTO #DIGITAL_SUMMARY_INVENTORY  
  FROM #TEMP_DIGITAL_SUMMARY_INVENTORY SI WHERE (SI.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO) OR AccountId = @VarAccountID )  --UPSGLD-11576
  AND ((SI.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR SI.DP_SERVICELINE_KEY = @VarDPServiceLineKey)  OR @VarDPServiceLineKey = '*')  
  AND (SI.FacilityId IN (SELECT UPPER (TRIM (value)) FROM string_split(@VarwarehouseId, ',')) OR @VarwarehouseId = '*')  
  AND (SI.FacilityId IN (SELECT UPPER (TRIM (value)) FROM string_split(@VarwarehouseIds, ',')) OR @VarwarehouseIds = '*')  
  AND (SI.warehouseCode IN (SELECT UPPER (TRIM (value)) FROM string_split(@VarwarehouseNames, ',')) OR @VarwarehouseNames = '*')  
  --AND (SI.BatchStatus COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarBatchStatus, ',')) OR @VarBatchStatus = '*') --09/30/2021  
  --AND (SI.BatchHoldReason COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarReasonCode, ',')) OR @VarReasonCode = '*') --09/30/2021  
  AND ((@VarSearchBy = 'ITEMNUMBER' AND SI.itemNumber LIKE '%' + @VarSearchValue + '%' COLLATE SQL_Latin1_General_CP1_CI_AS)  
  OR (@VarSearchBy = 'ITEMDESCRIPTION' AND SI.itemDescription LIKE '%' + @VarSearchValue + '%' COLLATE SQL_Latin1_General_CP1_CI_AS)  
  OR (@VarSearchBy = 'DESIGNATOR' AND SI.Designator LIKE '%' + @VarSearchValue + '%' COLLATE SQL_Latin1_General_CP1_CI_AS)  
  OR (@VarSearchBy = 'LPN' AND SI.LPNNumber LIKE '%' + @VarSearchValue + '%' COLLATE SQL_Latin1_General_CP1_CI_AS)  
  OR (@VarSearchBy = 'VSN' AND SI.VendorSerialNumber LIKE '%' + @VarSearchValue + '%' COLLATE SQL_Latin1_General_CP1_CI_AS)   
  OR (@VarSearchBy = 'VCL' AND SI.VendorLotNumber LIKE '%' + @VarSearchValue + '%' COLLATE SQL_Latin1_General_CP1_CI_AS)  
  OR (@VarSearchBy = 'INVENTORYBATCHNUMBER' AND SI.VendorLotNumber LIKE '%' + @VarSearchValue + '%' COLLATE SQL_Latin1_General_CP1_CI_AS)  
  OR (@VarSearchBy = 'INBOUNDREFERENCENUMBER' AND (SI.InvRef1 LIKE '%' + @VarSearchValue + '%' COLLATE SQL_Latin1_General_CP1_CI_AS   
  OR SI.InvRef2 LIKE '%' + @VarSearchValue + '%' COLLATE SQL_Latin1_General_CP1_CI_AS  
  OR SI.InvRef3 LIKE '%' + @VarSearchValue + '%' COLLATE SQL_Latin1_General_CP1_CI_AS  
  OR SI.InvRef4 LIKE '%' + @VarSearchValue + '%' COLLATE SQL_Latin1_General_CP1_CI_AS  
  OR SI.InvRef5 LIKE '%' + @VarSearchValue + '%' COLLATE SQL_Latin1_General_CP1_CI_AS)))  
  
  
  
  -- FINAL RESULT SET  
  
  SELECT   
    itemNumber,  
    itemDescription,  
    '{"batchDetails":  ' +(   SELECT DISTINCT ISNULL(VendorLotNumber,'') AS batchNumber,   
                           ISNULL(BatchStatus, '') AS batchStatus,   
         ISNULL(NULLIF(BatchHoldReason,'NA'), '') AS ReasonCode, --09/30/2021  
                           CAST(ExpirationDate AS DATE)  AS expirationDate  
         FROM #DIGITAL_SUMMARY_INVENTORY  
         WHERE (BatchStatus COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarBatchStatus, ',')) OR @VarBatchStatus = '*') --09/30/2021  
         AND (BatchHoldReason COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarReasonCode, ',')) OR @VarReasonCode = '*') --09/30/2021  
         AND itemNumber=SI.itemNumber -- Added 09/15 to show the correct batch no  
         AND warehouseCode = SI.warehouseCode --09/30/2021  
         FOR JSON PATH, INCLUDE_NULL_VALUES ) +'}' AS batchDetails,  
 warehouseCode  
  FROM #DIGITAL_SUMMARY_INVENTORY SI  
  WHERE   
  isnull(FacilityId,'') <> '' AND --Added 09/13  
   (SI.BatchStatus COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarBatchStatus, ',')) OR @VarBatchStatus = '*') --09/30/2021  
  AND (SI.BatchHoldReason COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TRIM (value) FROM string_split(@VarReasonCode, ',')) OR @VarReasonCode = '*') --09/30/2021   
    
  GROUP BY itemNumber,  
     itemDescription,  
     warehouseCode  
END  
GO

