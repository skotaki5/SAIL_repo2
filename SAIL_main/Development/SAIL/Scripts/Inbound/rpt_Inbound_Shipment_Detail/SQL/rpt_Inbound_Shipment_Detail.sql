/****** Object:  StoredProcedure [digital].[rpt_Inbound_Shipment_Detail]    Script Date: 5/2/2022 1:41:02 PM ******/
SET ANSI_NULLS ON
GO
 
SET QUOTED_IDENTIFIER ON
GO
 
 
 
   
/****  
   
/*** 
CHANGE LOG 
---------- 
CHANGED BY       DATE           SPRINT              CHANGES 
ARUP             10/05/2021                         Added Claims, Cost Breakdown and LastKnownLocation  
CHAITANYA        08/26/2021                         Added item level join for batch number, expiration date and serial no. 
ARUP                                                Added last known location as a part of Sprint 39  
Revathy          11/24/2021                         [UPSGLD-12221]Added order by condition in #LAST_LOCATION
Revathy         01/05/2022     CL-341               Change returned value from "Transport Order" to "Managed Transportation"
Harsha          02/11/2022                          Adding table alias wherver required
SAGAR           04/07/2022   Sprint-53 (CL455)      Added shell for loadNumber to response
SAGAR           04/11/2022   Sprint-53 (CL455)      Added loadNumber to response
Venkata         04/26/2022                          Provided the alias name in the result set 7 as mentioned in the chnagelog sheet
   
***/ 
   
--AMR 
EXEC [digital].[rpt_Inbound_Shipment_Detail]    @DPProductLineKey = 'E648FA6F-6253-428E-8AC9-201E3EF83B91',@DPServiceLineKey = '53D60776-D3BD-4E6A-8E37-F591C148294B',@DPEntityKey = NULL,@UPSOrderNumber = '1231' 
--SWR 
EXEC [digital].[rpt_Inbound_Shipment_Detail]    @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = '*',@DPEntityKey = NULL,@UPSOrderNumber = 'SWSN7184644' 
   
EXEC [digital].[rpt_Inbound_Shipment_Detail]    @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = '*',@DPEntityKey = NULL,@UPSOrderNumber = 'SWSN7825948' 
--Cambium 
EXEC [digital].[rpt_Inbound_Shipment_Detail]    @DPProductLineKey = 'A10F512B-C7F4-42A0-909C-21DD95A7D921',@DPServiceLineKey = '53DDDBC2-FB5A-4E70-A92E-8312DC9FDD05',@DPEntityKey = NULL,@UPSOrderNumber = '6391' 
   
   
****/ 
   
CREATE  PROCEDURE [digital].[rpt_Inbound_Shipment_Detail] @DPProductLineKey varchar(50),@DPServiceLineKey varchar(50),@DPEntityKey varchar(50),@UPSOrderNumber varchar(50)       
           
AS 
   
BEGIN 
DECLARE  @VarAccountID VARCHAR(50) 
  ,@VarDPServiceLineKey VARCHAR(50) 
  ,@VarDPEntityKey VARCHAR(50) 
     
SET @VarAccountID = UPPER(@DPProductLineKey) 
SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey) 
SET @VarDPEntityKey = UPPER(@DPEntityKey) 
   
IF @DPServiceLineKey IS NULL 
 SET @VarDPServiceLineKey = '*' 
   
IF @DPEntityKey IS NULL 
 SET @VarDPEntityKey = '*' 
   
 SELECT * INTO #DIGITAL_SUMMARY_ORDERS FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O 
            WHERE O.AccountId = @VarAccountID  
            AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')  
            AND  O.[UPSOrderNumber] = @UPSOrderNumber  
            AND O.IS_INBOUND = 1 
   
 SELECT * INTO #DIGITAL_SUMMARY_ORDER_LINES_DETAILS FROM Summary.DIGITAL_SUMMARY_ORDER_LINES_DETAILS (NOLOCK) 
                WHERE AccountId=@VarAccountID  
                AND UPSOrderNumber=@UPSOrderNumber 
   
    SELECT * INTO #DIGITAL_SUMMARY_INBOUND_LINE FROM [Summary].[DIGITAL_SUMMARY_INBOUND_LINE] (NOLOCK) 
            WHERE AccountId=@VarAccountID  
            AND UPSOrderNumber=@UPSOrderNumber 
   
   SELECT * INTO #DIGITAL_SUMMARY_ORDER_TRACKING FROM [Summary].[DIGITAL_SUMMARY_ORDER_TRACKING] (NOLOCK) 
                                                 WHERE AccountId=@VarAccountID  
             AND UPSOrderNumber=@UPSOrderNumber 
   
  SELECT * INTO #DIGITAL_SUMMARY_TRANSPORTATION_REFERENCES FROM [Summary].[DIGITAL_SUMMARY_TRANSPORTATION_REFERENCES]  
           WHERE UPSOrderNumber=@UPSOrderNumber 
   
  SELECT * INTO #DIGITAL_SUMMARY_MILESTONE_ACTIVITY FROM [Summary].[DIGITAL_SUMMARY_MILESTONE_ACTIVITY] (NOLOCK) 
                                                 WHERE AccountId=@VarAccountID  
             AND UPSOrderNumber=@UPSOrderNumber 
   
   
SELECT  TOP 1 
      MA.UPSOrderNumber, 
                  MA.SourceSystemKey,  
      MA.ACTIVITY_NOTES 
      INTO #LAST_LOCATION  
                  FROM Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) MA 
      WHERE MA.UPSOrderNumber=@UPSOrderNumber         
      and MA.ACTIVITY_NOTES is not null 
      and MA.ActivityCode not in ('AB','E') 
      ORDER BY MA.ActivityDate DESC   --UPSGLD-12221
   
   
SELECT DISTINCT --CL455
 O.[UPSOrderNumber] upsShipmentNumber 
,O.[OrderNumber] clientShipmentNumber 
, CASE WHEN O.IS_INBOUND=1 THEN  'Inbound' ELSE 'Outbound' END AS shipmentType 
, CASE WHEN O.IS_ASN=1 THEN  'ASN' ELSE 'Managed Transportation' END AS inboundType  --CL341
--, CASE WHEN O.IS_ASN=1 THEN  'ASN' ELSE 'Transport Order' END AS inboundType 
,O.TransactionTypeName templateType 
,O.UPSTransportShipmentNumber shipmentNumber 
,O.[OrderNumber] referenceNumber 
,O.[CustomerPO] customerPONumber 
,O.[OrderNumber] orderNumber 
,O.UPSTransportShipmentNumber upsTransportShipmentNumber 
,O.GFF_ShipmentInstanceId gff_shipmentInstanceId 
,O.GFF_ShipmentNumber gff_ShipmentNumber 
,O.[Carrier] shipmentCarrier 
,O.CarrierCode shipmentCarrierCode 
,O.ServiceLevel shipmentServiceLevel 
,O.ServiceLevelCode shipmentServiceLevelCode 
,O.ServiceMode serviceMode 
,O.FacilityId as warehouseId 
,O.[OrderWarehouse]  wareHouseCode 
,O.[ExceptionCode]  primaryException 
,O.[DateTimeReceived] AS shipmentPlaceDateTime 
,O.[DateTimeCancelled] AS shipmentCanceledDateTime 
,O.[CancelledReasonCode] as shipmentCanceledReason 
,O.[DateTimeShipped] as actualShipmentDateTime 
,O.[ScheduleShipmentDate] as shipmentCreateDateTime 
,O.[OriginalScheduledDeliveryDateTime] AS originalScheduledDeliveryDateTime 
,O.[ActualDeliveryDate] AS actualDeliveryDateTime 
,(  SELECT DSTO.ShipmentDimensions, 
   DSTO.ShipmentWeight, 
   DSTO.TRACKING_NUMBER AS Tracking_Number, 
   DSTO.CarrierCode, 
   DSTO.CarrierType, 
   O.Carrier AS CarrierName, 
   DSTO.ShipmentDimensions_UOM, 
   DSTO.ShipmentWeight_UOM 
     FROM #DIGITAL_SUMMARY_ORDER_TRACKING (NOLOCK) DSTO WHERE DSTO.UPSOrderNumber = O.UPSOrderNumber AND DSTO.SourceSystemKey = O.SourceSystemKey 
     FOR JSON PATH, INCLUDE_NULL_VALUES ) TrackingNumber 
,O.OriginAddress1 as shipmentOrigin_addressLine1 
,O.OriginAddress2 as shipmentOrigin_addressLine2 
,O.OriginCity as shipmentOrigin_city 
,O.OriginProvince as shipmentOrigin_stateProvince 
,O.OriginPostalCode as shipmentOrigin_postalCode 
,O.OriginCountry as shipmentOrigin_country 
,'' as shipmentOrigin_port_addressLine1 
,'' as shipmentOrigin_port_addressLine2 
,'' as shipmentOrigin_port_city 
,'' as shipmentOrigin_port_stateProvince 
,'' as shipmentOrigin_port_country 
,'' as shipmentOrigin_port_postalCode 
,'' shipmentOrigin_port_phoneNumber 
,O.DestinationAddress1 as shipmentDestination_addressLine1 
,O.DestinationAddress2 as shipmentDestination_addressLine2 
,O.DestinationCity as shipmentDestination_city 
,O.DestinationProvince as shipmentDestination_stateProvince 
,O.DestinationCountry as  shipmentDestination_country 
,O.DestinationPostalcode as shipmentDestination_postalCode 
,'' shipmentDestination_port_addressLine1 
,'' shipmentDestination_port_addressLine2 
,'' shipmentDestination_port_city 
,'' shipmentDestination_port_stateProvince 
,'' shipmentDestination_port_postalCode 
,'' shipmentDestination_port_country 
,'' shipmentDestination_port_phoneNumber 
,'' shipmentNotes_dateTime 
,'' shipmentNotes_description 
,   O.Freight_Carriercode 
,  O.WAYBILL_AIRBILL_NUM 
,( SELECT IL.ReceiptLineNumber AS LineNumber, 
          IL.SKU, 
    IL.UPSASNNumber AS asnNumber, 
    IL.SKUDescription, 
    IL.ClientPONumber AS CustomerPONumber, 
    CAST(ISNULL(IL.ReceivedQuantity,0) AS INT) AS SKUQuantity, 
    CAST(ISNULL(IL.ShippedQuantity,0) AS INT) AS SKUShippedQuantity, 
    IL.SKUWeight, 
    IL.SKUDimensions, 
    IL.SKUWeight_UOM, 
    IL.SKUDimensions_UOM, 
    IL.InboundLine_Reference2 ReferenceNumber_1, 
    IL.InboundLine_Reference10 ReferenceNumber_2, 
    IL.InboundLine_Reference11 ReferenceNumber_3 
    --,JSON_QUERY( 
    --                '[' + STUFF(( SELECT DISTINCT ',' + '"' + VendorLotNumber + '"'  
    -- FROM [Summary].[DIGITAL_SUMMARY_ORDER_LINES_DETAILS] (NOLOCK) WHERE UPSOrderNumber = O.UPSOrderNumber AND SourceSystemKey = O.SourceSystemKey   
    --                FOR XML PATH('')),1,1,'') + ']' ) AS batchNumber 
       ,STUFF(( SELECT DISTINCT ',' + DSOLD.VendorLotNumber  
       FROM #DIGITAL_SUMMARY_ORDER_LINES_DETAILS (NOLOCK) DSOLD
       WHERE DSOLD.UPSOrderNumber = O.UPSOrderNumber  
       AND DSOLD.SourceSystemKey = O.SourceSystemKey  
       AND DSOLD.itemNumber=IL.SKU 
                   FOR XML PATH('')),1,1,'') AS batchNumber 
          ,JSON_QUERY( 
                    '[' + STUFF(( SELECT DISTINCT ',' + '"' + DSOLD.VendorSerialNumber + '"'  
     FROM #DIGITAL_SUMMARY_ORDER_LINES_DETAILS (NOLOCK) DSOLD 
     WHERE DSOLD.UPSOrderNumber = O.UPSOrderNumber  
     AND DSOLD.SourceSystemKey = O.SourceSystemKey  
     AND DSOLD.itemNumber=IL.SKU 
                    FOR XML PATH('')),1,1,'') + ']' ) AS serialNumber 
     --     ,JSON_QUERY( 
     --               '[' + STUFF(( SELECT DISTINCT ',' + '"' + CONVERT(VARCHAR(40),EXPIRATION_DATE,120) + '"'  
     --FROM [Summary].[DIGITAL_SUMMARY_ORDER_LINES_DETAILS] (NOLOCK) WHERE UPSOrderNumber = O.UPSOrderNumber AND SourceSystemKey = O.SourceSystemKey   
     --               FOR XML PATH('')),1,1,'') + ']' ) AS expirationDate 
   
        ,STUFF(( SELECT DISTINCT ',' + CONVERT(VARCHAR(40),DSOLD.EXPIRATION_DATE,120)  
       FROM #DIGITAL_SUMMARY_ORDER_LINES_DETAILS (NOLOCK) DSOLD
       WHERE DSOLD.UPSOrderNumber = O.UPSOrderNumber  
       AND DSOLD.SourceSystemKey = O.SourceSystemKey   
       AND DSOLD.itemNumber=IL.SKU 
                   FOR XML PATH('')),1,1,'') AS expirationDate 
   
    FROM #DIGITAL_SUMMARY_INBOUND_LINE (NOLOCK) IL WHERE IL.UPSOrderNumber = O.UPSOrderNumber  AND IL.SourceSystemKey = CASE WHEN O.SourceSystemKey=1011 then IL.SourceSystemKey else  O.SourceSystemKey end 
      
    FOR JSON PATH, INCLUDE_NULL_VALUES ) OrderLine 
,( SELECT   DSIL.ClientASNNumber AS clientASNNumber, 
   CASE WHEN DSIL.SourceSystemKey = 1002 THEN DSIL.UPSASNNumber ELSE ISNULL(DSIL.ReceiptNumber,DSIL.UPSASNNumber) END AS upsASNNumber, 
      
   DSIL.ReceiptNumber AS receiptNumber, 
   DSIL.FacilityId AS facilityId, 
   DSIL.FacilityCode AS facilityName, 
   CONVERT(VARCHAR(40),DSIL.CreationDateTime,120) AS creationDateTime 
   FROM #DIGITAL_SUMMARY_INBOUND_LINE (NOLOCK) DSIL WHERE DSIL.UPSOrderNumber = O.UPSOrderNumber AND DSIL.SourceSystemKey = CASE WHEN O.SourceSystemKey=1011 then DSIL.SourceSystemKey else  O.SourceSystemKey end 
   GROUP BY DSIL.ClientASNNumber,DSIL.UPSASNNumber,DSIL.ReceiptNumber,DSIL.FacilityId,DSIL.FacilityCode,DSIL.CreationDateTime,DSIL.SourceSystemKey 
   FOR JSON PATH, INCLUDE_NULL_VALUES ) advancedShipmentNotice 
,O.UPSOrderNumber AS FTZShipmentNumber 
,O.Account_number accountNumber 
,OT.ShipmentDimensions_UOM AS loadDimension --changed 
,OT.LOAD_AREA AS loadValue 
,O.CurrentMilestone as milestoneStatus 
,MA.ACTIVITY_NOTES AS lastKnownLocation --Arup
,O.LOAD_ID as loadNumber --CL455
FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) O 
LEFT JOIN #DIGITAL_SUMMARY_ORDER_TRACKING (NOLOCK) OT  
          ON O.UPSOrderNumber=OT.UPSOrderNumber 
    AND O.SourceSystemKey=OT.SourceSystemKey 
LEFT JOIN #LAST_LOCATION MA ON O.UPSOrderNumber=MA.UPSOrderNumber 
      AND O.SourceSystemKey=MA.SourceSystemKey 
   
SELECT  
       T.ITEM_DESCRIPTION 
      ,T.ACTUAL_QTY 
   ,T.ACTUAL_UOM 
   ,T.ACTUAL_WGT 
   ,T.ITEM_DIMENSION 
,NULL AS Attribute1 
,NULL AS Attribute2 
,NULL AS Attribute3 
,NULL AS Attribute4 
FROM Summary.Digital_Summary_Transport_Details T WITH (NOLOCK)                  
WHERE T.Account_ID = @VarAccountID  
  AND (T.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')  
  --AND (ISNULL(T.DP_ORGENTITY_KEY,'') = @VarDPEntityKey OR @VarDPEntityKey = '*')  
  AND  T.UPSORDERNUMBER = @UPSOrderNumber  
   
   
   
SELECT DISTINCT 
IL.UPSOrderNumber AS TransportOrderNumber 
FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) O 
JOIN Summary.DIGITAL_SUMMARY_INBOUND_LINE (NOLOCK) IL ON O.OrderNumber = IL.ClientASNNumber 
WHERE IL.SourceSystemKey='1011' 
   
   
   
-- COST BREAKDOWN 
   
select  
(select sum(CAST(DSTRC.Charge as decimal(10,2))) from Summary.DIGITAL_SUMMARY_TRANSPORTATION_RATES_CHARGES (NOLOCK) DSTRC where  DSTRC.ChargeLevel = 'CUSTOMER_RATES' 
and DSTRC.UpsOrderNumber=O.UPSOrderNumber) AS totalCustomerCharge 
,Summary.usp_Get_Customer_Charges_Summary(O.UPSOrderNumber, O.SourceSystemKey,'CHARGE','CURRENCY') AS totalCustomerChargeCurrency 
,O.DateTimeReceived AS invoiceDateTime 
,'['+'{"' + 
   
STUFF((select  '"' +'costBreakdownType'+'"'+':'+'"'+DSTRC.ChargeDescription+'"' +','+'"' + 'costBreakdownValue'+'"'+':'+'"'+cast(sum(cast(DSTRC.Charge as decimal(10,2))) as varchar) + '"},{'   from Summary.DIGITAL_SUMMARY_TRANSPORTATION_RATES_CHARGES DSTRC where  DSTRC.ChargeLevel = 'CUSTOMER_RATES' 
and DSTRC.UpsOrderNumber=O.UPSOrderNumber 
group by DSTRC.ChargeDescription,DSTRC.CurrencyCode 
for xml path('')),1,1,'') + '}' +']' as costBreakdown 
into #CostBreakdown 
FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) O   
   
--RESULT SET 4 
   
SELECT  
totalCustomerCharge, 
totalCustomerChargeCurrency, 
invoiceDateTime, 
replace(costBreakdown,',{}','') as costBreakdown  
from #CostBreakdown 
   
   
--RESULT SET 5 
   
--SELECT  
-- [Summary].[usp_Summary_Get_Claim_Details](O.UPSOrderNumber, O.SourceSystemKey,'CLAIMTYPE') AS claimType 
-- ,[Summary].[usp_Summary_Get_Claim_Details](O.UPSOrderNumber, O.SourceSystemKey,'CLAIMAMOUNT') AS claimAmount 
-- ,'' AS claimAmountCurrency 
-- ,[Summary].[usp_Summary_Get_Claim_Details](O.UPSOrderNumber, O.SourceSystemKey,'ClaimDate') AS claimFilingDateTime 
-- ,[Summary].[usp_Summary_Get_Claim_Details](O.UPSOrderNumber, O.SourceSystemKey,'ACTUALSTATUS_CLOSECLAIM') AS claimStatus 
-- ,[Summary].[usp_Summary_Get_Claim_Details](O.UPSOrderNumber, O.SourceSystemKey,'ACTUALDATE_CLOSECLAIM') AS claimClosureDateTime 
-- ,[Summary].[usp_Summary_Get_Claim_Details](O.UPSOrderNumber, O.SourceSystemKey,'CLAIMAMOUNTPAID') AS claimAmountPaid 
-- ,'' AS claimAmountPaidCurrency 
--FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) O   
   
   
SELECT  
    (SELECT DSTR.ReferenceValue 
     FROM #DIGITAL_SUMMARY_TRANSPORTATION_REFERENCES (NOLOCK) DSTR
  WHERE DSTR.UPSOrderNumber = O.UPSOrderNumber  
  AND DSTR.SourceSystemKey = O.SourceSystemKey 
  AND DSTR.ReferenceLevel = 'LoadReference_Claim' 
  AND DSTR.ReferenceType='Claim Type' 
     ) claimType, 
    (SELECT DSTR.ReferenceValue 
     FROM #DIGITAL_SUMMARY_TRANSPORTATION_REFERENCES (NOLOCK) DSTR
  WHERE DSTR.UPSOrderNumber = O.UPSOrderNumber  
  AND DSTR.SourceSystemKey = O.SourceSystemKey 
  AND DSTR.ReferenceLevel = 'LoadReference_Claim' 
     AND DSTR.ReferenceType='Claim Amount' 
     ) claimAmount 
 ,'' AS claimAmountCurrency 
 ,(SELECT DSTR.ReferenceValue 
     FROM #DIGITAL_SUMMARY_TRANSPORTATION_REFERENCES (NOLOCK)  DSTR
  WHERE DSTR.UPSOrderNumber = O.UPSOrderNumber  
  AND DSTR.SourceSystemKey = O.SourceSystemKey 
  AND DSTR.ReferenceLevel = 'LoadReference_Claim' 
     AND DSTR.ReferenceType='Claim Date' 
     ) claimFilingDateTime 
 ,(SELECT DSMA.ACTIVITY_NOTES FROM #DIGITAL_SUMMARY_MILESTONE_ACTIVITY DSMA WHERE DSMA.ActivityCode='COSD' 
  ) claimStatus 
  ,(SELECT convert(nvarchar(MAX), DSMA.ActivityDate, 21) FROM #DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) DSMA
                                    WHERE DSMA.ActivityCode='CC' 
 )claimClosureDateTime 
 ,(SELECT DSTR.ReferenceValue 
  FROM #DIGITAL_SUMMARY_TRANSPORTATION_REFERENCES (NOLOCK) DSTR
                          WHERE DSTR.ReferenceLevel = 'LoadReference_Claim' 
        AND DSTR.ReferenceType='Claim Amount Paid' 
        )claimAmountPaid 
 --,[Summary].[usp_Summary_Get_Claim_Details](O.UPSOrderNumber, O.SourceSystemKey,'CLAIMAMOUNTPAID') AS claimAmountPaid 
 ,'' AS claimAmountPaidCurrency 
FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) O   
   
   
 --RESULT SET 6 
   
  SELECT DISTINCT 
   OT.SHIPMENT_DESCRIPTION AS shippableUnit_description 
  ,CAST(OT.SHIPMENT_QUANTITY AS INT) AS shippableUnit_quantity 
  ,OT.UOM AS shippableUnit_quantity_unitOfMeasurement 
  ,OT.ShipmentWeight AS shippableUnit_weight 
  ,OT.ShipmentWeight_UOM AS shippableUnit_weight_unitOfMeasurement 
  ,OT.ShipmentDimensions AS shippableUnit_dimension 
  ,OT.ShipmentDimensions_UOM AS shipableUnit_dimension_unitOfMeasurement 
  --,'[{"referenceType":"Lot","referenceValue":"ABC123CCE"}]' AS shippableUnit_referenceType 
  --,'['+'{"' + 
  --  STUFF((SELECT  '"' +'referenceType'+'"'+':'+'"'+ REF.ReferenceType +'"' +','+'"' + 'referenceValue'+'"'+':'+'"'+ REF.ReferenceValue + '"},{'   from Summary.DIGITAL_SUMMARY_Transportation_references (NOLOCK)  REF WHERE  REF.UPSOrderNumber=O.UPSOrderNumber 
  --                                                                                                                                                                                                                     AND REF.sourcesystemKey=O.SourceSystemKey 
  --  FOR XML PATH('')),1,1,'') + '}' +']' AS shippableUnit_referenceType 
     
  ,(SELECT 
  DSTR.ReferenceType AS referenceType, 
     DSTR.ReferenceValue AS referenceValue 
    FROM [Summary].[DIGITAL_SUMMARY_TRANSPORTATION_REFERENCES] (NOLOCK) DSTR 
 WHERE DSTR.UPSOrderNumber = O.UPSOrderNumber  
 AND DSTR.SourceSystemKey = O.SourceSystemKey 
 AND DSTR.ReferenceLevel='shipunit_reference' 
    FOR JSON PATH, INCLUDE_NULL_VALUES ) shippableUnit_referenceType  
  --, '[{"TempRangeMin":"96","TempRangeMax":"110","TempRangeUOM":"Fahrenheit","TempRangeCode":"FC"}]' AS shippableUnit_temperatureDetails 
  ,'['+'{"' + STUFF((SELECT  '"' +'TempRangeMin'+'"'+':'+'"'+OT.TemperatureRange_Min+'"' +','+'"' + 'TempRangeMax'+'"'+':'+'"'+ OT.TemperatureRange_Max + '"},{'   from Summary.DIGITAL_SUMMARY_ORDER_TRACKING (NOLOCK) WHERE  OT.UPSOrderNumber=O.UPSOrderNumber
    FOR XML PATH('')),1,1,'') + '}' +']' AS shippableUnit_temperatureDetails 
  FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) O 
  INNER JOIN #DIGITAL_SUMMARY_ORDER_TRACKING (NOLOCK) OT  
             ON  O.UPSOrderNumber = OT.UPSOrderNumber 
    AND O.SourceSystemKey = OT.SourceSystemKey  
   
    
 --RESULT SET 7  
 
   
   
 SELECT CC.LATEST_TEMPERATURE AS temperatureValue,  --04/26/2022
CC.TEMPERATURE_DATETIME AS temperatureDateTime, 
CC.TEMPERATURE_CITY AS temperatureCity, 
CC.TEMPERATURE_STATE AS temperatureState, 
CC.TEMPERATURE_COUNTRY AS temperatureCountry 
    
 FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) O 
INNER JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION_CALLCHECK (NOLOCK) CC  
ON O.UPSOrderNumber=CC.UPSORDERNUMBER 
AND O.SourceSystemKey=CC.SOURCESYSTEMKEY 
WHERE CC.IS_TEMPERATURE='Y' 
AND CC.STATUSDETAILTYPE='TemperatureTracking' 
   
   
END 
GO