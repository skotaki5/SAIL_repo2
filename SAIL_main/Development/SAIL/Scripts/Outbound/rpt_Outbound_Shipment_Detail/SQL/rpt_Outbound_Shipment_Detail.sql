/****** Object:  StoredProcedure [digital].[rpt_Outbound_Shipment_Detail]    Script Date: 3/29/2022 2:05:13 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO








      
      
/****         
      
/***      
CHANGE LOG      
----------      
CHANGED BY				DATE				SPRINT					CHANGES      
ARUP					10/05/2021									Added Claims, Cost Breakdown and LastKnownLocation    
CHAITANYA				08/26/2021									Added item level and line level join for batch number, expiration date and serial no.      
Anand					09/02/2021									Created temp table #tmpSOLD and used it for batchNumber, serialNumber, expirationDate columns      
Venkata					10/01/2021			39TH					Added VendorLotNumber in the joining condition to distinguish the records   
Sheetal					10/14/2021			UPSGLD-11487			Changed logic for proof of delivery name column
Venkata					11/1/2021			UPSGLD-11711			Changed logic to get the totalcharge count (changed value as decimal(10,2), removed cast as int)
Sheetal					11/02/2021			UPSGLD-11872			Added NULL value for latestTemperatureInCelsius and latestTemperatureInFahrenheit in final query
Sheetal										UPSGLD-11915			Added filter in OrderLine column to get distinct record
Sheetal										CL320					Made Logic change to get records in #Facttransportationcallcheck table
																	made logic change to pull claim related data
SAGAR					01/10/2022			UPSGLD-12979			Shipments Details Page | VSN, VCL, Designator & LPN column values coming as NULL in Item Shipped table So removed null having blanks
Harsha                  02/11/2022									Adding table alias wherver required
Revathy                 02/11/2022          UPSGLD-13959			INC2668568:The shipment items table for multiple parts is not displaying correctly 
Sheetal					02/14/2022			CL394					Added shells for proofOfDelivery_DateTime column
Avinash                 02/15/2022                                  Added missing alias name
Revathy					02/15/2022			Sprint-49 -CL394		Added ProofOfDelivery_DateTime column
Venkata					02/25/2022			CL400					Added OrderType column
Sheetal					03/03/2022			UPSGLD-14243			Proof of delivery name was not coming up for cheetah, added case statement
Rajeev                  03/17/2022          Sprint-51 (CL428)       Added temperature threshold, max,min and uom to the Result 1 and latitude and longitude to Result 2
REVATHY                 03/25/2022          Sprint-52               Add shell for "Post Sales Delivery ETA" field to stored procedure   
***/ 
--CL428--ABBIVE

EXEC [digital].[rpt_Outbound_Shipment_Detail]       
@DPProductLineKey = '20394995-0871-48AE-A2D0-962CFA4BB1C1'  
,@DPServiceLineKey = '*'    
,@DPEntityKey = NULL    
,@UPSOrderNumber = '0839901818'  

--AMR        
EXEC [digital].[rpt_Outbound_Shipment_Detail]    @DPProductLineKey = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529',@DPServiceLineKey = '*',@DPEntityKey = NULL,@UPSOrderNumber = '80037795'        
--SWR        
EXEC [digital].[rpt_Outbound_Shipment_Detail]    @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = '150E5128-B3E3-44A1-BF6C-CD2E4D9856DA',@DPEntityKey = NULL,@UPSOrderNumber = '2952616'        
--Cambium        
      
EXEC [digital].[rpt_Outbound_Shipment_Detail]    @DPProductLineKey = 'A10F512B-C7F4-42A0-909C-21DD95A7D921',@DPServiceLineKey = '53DDDBC2-FB5A-4E70-A92E-8312DC9FDD05',@DPEntityKey = NULL,@UPSOrderNumber = '2952616'       
--Carrier Hub      
EXEC [digital].[rpt_Outbound_Shipment_Detail]    @DPProductLineKey = '3DAEB74F-FA36-47D7-AC7A-FDBDC4341357',@DPServiceLineKey = '*',@DPEntityKey = NULL,@UPSOrderNumber = '10239817'        
--****/        
      
--EXEC [digital].[rpt_Outbound_Shipment_Detail]       
--@DPProductLineKey = '1B9B4EF4-EA2D-4DE1-2729-08D6B3A5F414' --CB3A2257-8A85-46FB-8C70-BC6B023CEC4A   
--,@DPServiceLineKey = '*'    
--,@DPEntityKey = NULL    
--,@UPSOrderNumber = '13141877'  -- 12375642     
      
    
    
CREATE PROCEDURE [digital].[rpt_Outbound_Shipment_Detail]         
        
@DPProductLineKey varchar(50),@DPServiceLineKey varchar(50),@DPEntityKey varchar(50),@UPSOrderNumber varchar(50)--, @isClaim int             
                 
AS        
        
BEGIN        
    
DECLARE    @VarAccountID VARCHAR(50)        
    ,@VarDPServiceLineKey VARCHAR(50)        
    ,@VarDPEntityKey VARCHAR(50)        
         
          
SET @VarAccountID = UPPER(@DPProductLineKey)        
SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)        
SET @VarDPEntityKey = UPPER(@DPEntityKey)        
        
IF @DPServiceLineKey IS NULL        
 SET @VarDPServiceLineKey = '*'        
        
IF @DPEntityKey IS NULL        
 SET @VarDPEntityKey = '*'       
      
        
       
SELECT      
MAX(EXCEPTION.UTC_ExceptionCreatedDate) AS UTC_ExceptionCreatedDate,      
EXCEPTION.UPSOrderNumber,      
EXCEPTION.SourceSystemKey AS [EXSource],      
EXCEPTION.SourceSystemKey AS [ORDSource]      
INTO #MaxException      
from  Summary.DIGITAL_SUMMARY_ORDERS (NOLOCK) ORD      
INNER JOIN Summary.DIGITAL_SUMMARY_EXCEPTIONS (NOLOCK) EXCEPTION      
ON ORD.UPSOrderNumber=EXCEPTION.UPSOrderNumber      
AND ORD.SourceSystemKey=EXCEPTION.SourceSystemKey      
WHERE ORD.AccountId = @VarAccountID         
  AND (ORD.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')         
  --AND (ISNULL(O.DP_ORGENTITY_KEY,'') = @VarDPEntityKey OR @VarDPEntityKey = '*')         
  AND  ORD.[UPSOrderNumber] = @UPSOrderNumber         
  AND ORD.IS_INBOUND= 0        
GROUP BY EXCEPTION.UPSOrderNumber,EXCEPTION.SourceSystemKey--,ExceptionReason      
UNION       
SELECT --Carrier Hub records have different SourceSystemKey in DIGITAL_SUMMARY_EXCEPTIONS AND DIGITAL_SUMMARY_ORDERS      
MAX(EXCEPTION.UTC_ExceptionCreatedDate) AS UTC_ExceptionCreatedDate,      
EXCEPTION.UPSOrderNumber,      
EXCEPTION.SourceSystemKey AS [EXSource],      
ORD.SourceSystemKey AS [ORDSource]      
from  Summary.DIGITAL_SUMMARY_ORDERS (NOLOCK) ORD      
INNER JOIN Summary.DIGITAL_SUMMARY_EXCEPTIONS (NOLOCK) EXCEPTION      
ON ORD.[UPSOrderNumber]=EXCEPTION.UPSOrderNumber      
WHERE ORD.AccountId = @VarAccountID      
  AND (ORD.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')         
  AND  ORD.[UPSOrderNumber] = @UPSOrderNumber        
  AND ORD.IS_INBOUND= 0        
  AND EXCEPTION.SourceSystemKey=1019 --Carrier Hub      
GROUP BY EXCEPTION.UPSOrderNumber,EXCEPTION.SourceSystemKey,ORD.SourceSystemKey--,ExceptionReason      
      
--SELECT O.UPSOrderNumber,'[{"VSN":"'+ ISNULL(STRING_AGG(LD.VendorSerialNumber,','),'NULL') +'"}]'  AS VSN      
--,'[{"VCL":"'+ ISNULL(STRING_AGG(LD.VendorLotNumber,','),'NULL') +'"}]'  AS VCL      
--,'[{"LPN":"'+ ISNULL(STRING_AGG(LD.LPNNumber,','),'NULL') +'"}]'  AS LPN      
--,'[{"Designator":"'+ ISNULL(STRING_AGG(LD.DispositionValue,','),'NULL') +'"}]'  AS Designator      
--INTO #jsonddETAILS      
--FROM       
-- [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O        
--JOIN [Summary].[DIGITAL_SUMMARY_ORDER_LINES] OL (NOLOCK) ON OL.UPSOrderNumber = O.UPSOrderNumber AND OL.SourceSystemKey = O.SourceSystemKey       
--Left JOIN [Summary].[DIGITAL_SUMMARY_ORDER_LINES_DETAILS] LD (NOLOCK) ON LD.UPSOrderNumber = O.UPSOrderNumber AND LD.SourceSystemKey = O.SourceSystemKey AND LD.LineNumber = OL.LineNumber      
--WHERE O.AccountId = @VarAccountID         
      
--  --AND (ISNULL(O.DP_ORGENTITY_KEY,'') = @VarDPEntityKey OR @VarDPEntityKey = '*')         
-- AND  O.[UPSOrderNumber] = @UPSOrderNumber            
--  AND O.IS_INBOUND= 0        
--  --AND LD.LPNNumber IS NOT NULL      
--  GROUP BY O.UPSOrderNumber      
      
      
      
SELECT O.UPSOrderNumber,OL.LineNumber,ISNULL(STRING_AGG(LD.VendorSerialNumber,'","'),'') AS VSN    --  UPSGLD-13959 
,ISNULL(STRING_AGG(LD.VendorLotNumber,'","'),'') AS VCL      
,ISNULL(STRING_AGG(LD.LPNNumber,'","'),'') AS LPN      
,ISNULL(STRING_AGG(LD.DispositionValue,'","'),'') AS Designator      
INTO #jsonddETAILS      
FROM       
 [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O        
JOIN [Summary].[DIGITAL_SUMMARY_ORDER_LINES] OL (NOLOCK) ON OL.UPSOrderNumber = O.UPSOrderNumber AND OL.SourceSystemKey = O.SourceSystemKey       
Left JOIN [Summary].[DIGITAL_SUMMARY_ORDER_LINES_DETAILS] LD (NOLOCK) ON LD.UPSOrderNumber = O.UPSOrderNumber AND LD.SourceSystemKey = O.SourceSystemKey AND LD.LineNumber = OL.LineNumber      
WHERE O.AccountId = @VarAccountID         
      
  --AND (ISNULL(O.DP_ORGENTITY_KEY,'') = @VarDPEntityKey OR @VarDPEntityKey = '*')         
 AND  O.[UPSOrderNumber] = @UPSOrderNumber            
  AND O.IS_INBOUND= 0        
  --AND LD.LPNNumber IS NOT NULL      
  GROUP BY O.UPSOrderNumber,OL.LineNumber        
      
SELECT  SOLD.UPSOrderNumber,SOLD.SourceSystemKey,SOLD.itemNumber,SOLD.LineNumber,SOLD.VendorLotNumber ,SOLD.VendorSerialNumber,SOLD.EXPIRATION_DATE      
       into #tmpSOLD      
       FROM [Summary].[DIGITAL_SUMMARY_ORDER_LINES_DETAILS] (NOLOCK) SOLD       
       WHERE SOLD.[UPSOrderNumber] = @UPSOrderNumber       
   
   SELECT * INTO #DIGITAL_SUMMARY_MILESTONE_ACTIVITY FROM [Summary].[DIGITAL_SUMMARY_MILESTONE_ACTIVITY] (NOLOCK)      --CL394
                                                 WHERE AccountId=@VarAccountID       
             AND UPSOrderNumber=@UPSOrderNumber   
      
SELECT  TOP 1      
      MA.UPSOrderNumber,      
      MA.SourceSystemKey,       
      MA.ACTIVITY_NOTES      
      INTO #LAST_LOCATION       
      FROM #DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) MA      
      WHERE --MA.UPSOrderNumber=@UPSOrderNumber              
      MA.ACTIVITY_NOTES is not null      
      and MA.ActivityCode not in ('AB','E')    
   ORDER BY MA.ActivityDate DESC    --Added missing alias name
        
SELECT         
    O.[UPSOrderNumber] upsShipmentNumber        
   ,O.[OrderNumber] clientShipmentNumber        
   , CASE WHEN O.IS_INBOUND=1 THEN  'Inbound' ELSE 'Outbound' END AS shipmentType        
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
   ,(  SELECT ShipmentDimensions,        
      ShipmentWeight,        
      TRACKING_NUMBER AS Tracking_Number,        
      CarrierCode,        
      CarrierType,      
   --CASE WHEN SourceSystemKey = 1002 THEN O.[Carrier] ELSE NULL END AS carrierName,      
   O.[Carrier] AS carrierName,      
   ShipmentDimensions_UOM,      
   ShipmentWeight_UOM      
        FROM [Summary].[DIGITAL_SUMMARY_ORDER_TRACKING] (NOLOCK) WHERE UPSOrderNumber = O.UPSOrderNumber AND SourceSystemKey = O.SourceSystemKey        
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
   ,O.DestinationAddress1 as shipmentDestination_addressLine1       --Added missing alias name  
   ,O.DestinationAddress2 as shipmentDestination_addressLine2        --Added missing alias name  
   ,O.DestinationCity as shipmentDestination_city        --Added missing alias name  
   ,O.DestinationProvince as shipmentDestination_stateProvince        --Added missing alias name  
   ,O.DestinationCountry as shipmentDestination_country         --Added missing alias name  
   ,O.DestinationPostalcode as shipmentDestination_postalCode        --Added missing alias name  
   ,'' shipmentDestination_port_addressLine1        
   ,'' shipmentDestination_port_addressLine2        
   ,'' shipmentDestination_port_city        
   ,'' shipmentDestination_port_stateProvince        
   ,'' shipmentDestination_port_postalCode        
   ,'' shipmentDestination_port_country        
   ,'' shipmentDestination_port_phoneNumber        
   ,'' shipmentNotes_dateTime        
   ,'' shipmentNotes_description
   --,O.PROOF_OF_DELIVERY_NAME AS ProofofDelivery_Name
   --,CASE WHEN O.CurrentMilestone = 'DELIVERED' THEN O.PROOF_OF_DELIVERY_NAME ELSE NULL END AS ProofofDelivery_Name -- 10/14/2021  
   ,CASE WHEN O.CurrentMilestone = 'DELIVERED' THEN  
		CASE WHEN O.SourceSystemKey = 1002 THEN O.PROOF_OF_DELIVERY_NAME
			 WHEN O.SourceSystemKey <> 1002 THEN SMA.PROOF_OF_DELIVERY_NAME  
			 ELSE NULL 
		END  
	END AS ProofofDelivery_Name  -- UPSGLD-14243
   ,O.ConsigneeName AS consignee      
   ,O.CurrentMilestone AS milestoneStatus      
   ,O.Account_number accountNumber      
   ,MA.ACTIVITY_NOTES AS lastKnownLocation --Arup      
      
   ,CASE WHEN (SELECT COUNT(1) FROM [Summary].[DIGITAL_SUMMARY_ORDER_LINES] (NOLOCK)       
       WHERE UPSOrderNumber = O.UPSOrderNumber       
      AND SourceSystemKey = O.SourceSystemKey       
   AND ShipmentLineCanceledFlag ='Y'      
     )>0 THEN 'Y' ELSE NULL END as Partially_Cancelled        
      
  ---------------4/28/2021 - If SKUQuantity, SKUShippedQuantity is NULL, it is set to 0----------------------      
   ,(SELECT OL.LineNUmber AS LineNumber, OL.SKU, OL.SKUDescription,CAST(ISNULL(OL.SKUQuantity, 0) AS INT) AS SKUQuantity,CAST(ISNULL(OL.SKUShippedQuantity, 0) AS INT) SKUShippedQuantity,   --Added missing alias name     
            OL.SKUWeight, OL.SKUDimensions, OL.SKUWeight_UOM, OL.SKUDimensions_UOM     --Added missing alias name    
   ,OL.ShipmentLineCanceledReason AS lineCancelledReason     --Added missing alias name 
   ,OL.ShipmentLineCanceledDate AS lineCancelledDateTime     --Added missing alias name 
   ,O.OriginTimeZone AS lineCancelledDateTimeZone      
   ,OL.LineRefVal1 AS lineReferenceNumber1   --Added missing alias name   
   ,OL.LineRefVal2 AS lineReferenceNumber2      --Added missing alias name 
   ,OL.LineRefVal3 AS lineReferenceNumber3      --Added missing alias name 
   ,OL.LineRefVal4 AS lineReferenceNumber4      --Added missing alias name 
   ,OL.LineRefVal5 AS lineReferenceNumber5      --Added missing alias name 
  --,d.VSN      
  -- ,d.VCL      
  -- ,d.LPN      
  -- ,d.Designator      
   ,JSON_QUERY(      
                    '[' + STUFF(( SELECT DISTINCT ',' + '"' + JD.VSN + '"'       
     FROM #jsonddETAILS JD where JD.UPSOrderNumber = OL.UPSOrderNumber AND OL.LineNumber = JD.LineNumber      --  UPSGLD-13959
                    FOR XML PATH('')),1,1,'') + ']' ) AS [VSN]      
   ,JSON_QUERY(      
                    '[' + STUFF(( SELECT DISTINCT ',' + '"' +JD.VCL + '"'       
     FROM #jsonddETAILS JD where JD.UPSOrderNumber = OL.UPSOrderNumber AND OL.LineNumber = JD.LineNumber         --  UPSGLD-13959  
                    FOR XML PATH('')),1,1,'') + ']' ) AS VCL      
   ,JSON_QUERY(      
   '[' + STUFF(( SELECT DISTINCT ',' + '"' +JD.LPN + '"'       
     FROM #jsonddETAILS JD where JD.UPSOrderNumber = OL.UPSOrderNumber AND OL.LineNumber = JD.LineNumber           --  UPSGLD-13959
                    FOR XML PATH('')),1,1,'') + ']' ) AS LPN      
   ,JSON_QUERY(      
                    '[' + STUFF(( SELECT DISTINCT ',' + '"' + JD.Designator + '"'       
     FROM #jsonddETAILS JD where JD.UPSOrderNumber = OL.UPSOrderNumber AND OL.LineNumber = JD.LineNumber           --  UPSGLD-13959
                    FOR XML PATH('')),1,1,'') + ']' ) AS Designator      
   --,JSON_QUERY(      
   --                 '[' + STUFF(( SELECT DISTINCT ',' + '"' + VendorLotNumber + '"'       
   --  FROM [Summary].[DIGITAL_SUMMARY_ORDER_LINES_DETAILS] (NOLOCK) WHERE UPSOrderNumber = O.UPSOrderNumber AND SourceSystemKey = O.SourceSystemKey AND LineNumber = OL.LineNumber       
   --                 FOR XML PATH('')),1,1,'') + ']' ) AS batchNumber      
   ,STUFF(( SELECT DISTINCT ',' + TS.VendorLotNumber       
       FROM #tmpSOLD TS (NOLOCK)       
       WHERE TS.UPSOrderNumber = O.UPSOrderNumber       
       AND TS.SourceSystemKey = O.SourceSystemKey       
       AND TS.itemNumber=OL.SKU      
       AND TS.LineNumber=OL.LineNUmber      
       AND Isnull(TS.VendorLotNumber,'') = Isnull(LD.VendorLotNumber,'')     --  UPSGLD-13959
                   FOR XML PATH('')),1,1,'') AS batchNumber      
      
   --,JSON_QUERY(      
   --                 '[' + STUFF(( SELECT DISTINCT ',' + '"' + VendorSerialNumber + '"'       
   --  FROM [Summary].[DIGITAL_SUMMARY_ORDER_LINES_DETAILS] (NOLOCK) WHERE UPSOrderNumber = O.UPSOrderNumber AND SourceSystemKey = O.SourceSystemKey AND LineNumber = OL.LineNumber       
   --                 FOR XML PATH('')),1,1,'') + ']' ) AS serialNumber      
   --,JSON_QUERY(      
                    ,STUFF(( SELECT DISTINCT ',' + TS.VendorSerialNumber        
     FROM #tmpSOLD (NOLOCK)  TS     
     WHERE TS.UPSOrderNumber = O.UPSOrderNumber       
     AND TS.SourceSystemKey = O.SourceSystemKey      
     AND TS.itemNumber=OL.SKU      
     AND TS.LineNumber = OL.LineNUmber      
     AND Isnull(TS.VendorLotNumber,'') = Isnull(LD.VendorLotNumber,'')           --  UPSGLD-13959
                    FOR XML PATH('')),1,1,'')     
     AS serialNumber      
   --,JSON_QUERY(      
   --                 '[' + STUFF(( SELECT DISTINCT ',' + '"' + CONVERT(VARCHAR(40),EXPIRATION_DATE,120) + '"'       
   --  FROM [Summary].[DIGITAL_SUMMARY_ORDER_LINES_DETAILS] (NOLOCK) WHERE UPSOrderNumber = O.UPSOrderNumber AND SourceSystemKey = O.SourceSystemKey AND LineNumber = OL.LineNumber       
   --                 FOR XML PATH('')),1,1,'') + ']' ) AS expirationDate      
      
   ,STUFF(( SELECT DISTINCT ',' + CONVERT(VARCHAR(40),TS.EXPIRATION_DATE,120)       
       FROM #tmpSOLD (NOLOCK)   TS    
       WHERE TS.UPSOrderNumber = O.UPSOrderNumber       
       AND TS.SourceSystemKey = O.SourceSystemKey      
       AND TS.itemNumber=OL.SKU      
       AND TS.LineNumber=OL.LineNUmber      
       AND Isnull(TS.VendorLotNumber,'') = Isnull(LD.VendorLotNumber,'')       --  UPSGLD-13959
                   FOR XML PATH('')),1,1,'') AS expirationDate      
      
    FROM [Summary].[DIGITAL_SUMMARY_ORDER_LINES] OL (NOLOCK)       
 LEFT JOIN  [Summary].[DIGITAL_SUMMARY_ORDER_LINES_DETAILS] LD (NOLOCK) --10/01/2021      
 ON LD.UPSOrderNumber = O.UPSOrderNumber AND LD.SourceSystemKey = O.SourceSystemKey AND LD.LineNumber = OL.LineNUmber --10/01/2021
  AND (LD.VendorLotNumber IS NOT NULL OR LD.VendorSerialNumber IS NOT NULL OR LD.LPNNumber IS NOT NULL)   --Added missing alias name
 WHERE OL.UPSOrderNumber = O.UPSOrderNumber AND OL.SourceSystemKey = O.SourceSystemKey       
       
    FOR JSON PATH, INCLUDE_NULL_VALUES ) OrderLine,      
    O.ORDER_REF_1_VALUE AS referenceNumber1,      
    O.ORDER_REF_2_VALUE AS referenceNumber2,      
    O.ORDER_REF_3_VALUE AS referenceNumber3,      
    O.ORDER_REF_4_VALUE AS referenceNumber4,      
    O.ORDER_REF_5_VALUE AS referenceNumber5,      
 --O.OriginLocationCode AS originLocationCode,      
 O.[OrderWarehouse]  originLocationCode,      
 O.DestinationLocationCode AS ShipmentDestination_destinationLocationCode,      
 O.AuthorizerName AS authorizorName,      
 EX.ExceptionReason AS qcReasonCode,      
 --O.WAYBILL_AIRBILL_NUM AS waybill_airbill_Number,      
 STUFF((SELECT ',' + TRACKING_NUMBER  FROM [Summary].[DIGITAL_SUMMARY_ORDER_TRACKING] (NOLOCK) WHERE UPSOrderNumber = O.UPSOrderNumber AND SourceSystemKey = O.SourceSystemKey FOR XML PATH('')      
                     ), 1, 1, ''      
                   )  AS waybill_airbill_Number,      
 O.DeliveryInstructions AS deliveryInstructions,      
 O.OriginTimeZone AS shipmentPlaceDateTimeZone,      
 O.OriginTimeZone AS shipmentCanceledDateTimeZone, --shipmentPlaceDateTimeZone and shipmentCreateDateTimeZone      
 O.OriginTimeZone AS shipmentCreateDateTimeZone,  
 '' AS estimatedDeliveryDateTime,
 O.ScheduleShipmentDate As expectedShipByDateTime, 
 SMA.PROOF_OF_DELIVERY_DATE_TIME AS proofOfDelivery_DateTime,  --  CL-394
 O.OrderType AS orderType, --CL400
 O.TemperatureRange_Min AS temperatureThresholdMin, --CL428
 O.TemperatureRange_Max AS temperatureThresholdMax, --CL428
 O.TemperatureRange_UOM AS temperatureThresholdUOM  --CL428
FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O       
--LEFT JOIN #jsonddETAILS d ON d.UPSOrderNumber=O.UPSOrderNumber      
LEFT JOIN (SELECT MEX.UPSOrderNumber,MEX.ORDSource AS SourceSystemKey,EX.ExceptionReason FROM #MaxException MEX      
                    INNER JOIN Summary.DIGITAL_SUMMARY_EXCEPTIONS (NOLOCK) EX      
     ON MEX.UPSOrderNumber=EX.UPSOrderNumber      
     AND MEX.EXSource=EX.SourceSystemKey      
     AND MEX.UTC_ExceptionCreatedDate=EX.UTC_ExceptionCreatedDate      
    ) EX ON O.UPSOrderNumber=EX.UPSOrderNumber      
         AND O.SourceSystemKey= EX.SourceSystemKey      
      
      LEFT JOIN #LAST_LOCATION MA ON O.UPSOrderNumber=MA.UPSOrderNumber      
      AND O.SourceSystemKey=MA.SourceSystemKey   
	  LEFT JOIN #DIGITAL_SUMMARY_MILESTONE_ACTIVITY SMA ON O.UPSOrderNumber=SMA.UPSOrderNumber      
      AND O.SourceSystemKey=SMA.SourceSystemKey and SMA.ActivityCode in ('D9','D','D1')  --  CL-394
      
WHERE O.AccountId = @VarAccountID         
  AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')         
  --AND (ISNULL(O.DP_ORGENTITY_KEY,'') = @VarDPEntityKey OR @VarDPEntityKey = '*')         
  AND  O.[UPSOrderNumber] = @UPSOrderNumber         
  AND O.IS_INBOUND= 0        
          
        
      
   --RESULT SET 2      
      
      
      
      
   SELECT DISTINCT CC.LATEST_TEMPERATURE AS temperatureValue,      
        CC.TEMPERATURE_DATETIME AS temperatureDateTime,      
        CC.TEMPERATURE_CITY AS temperatureCity,      
        CC.TEMPERATURE_STATE AS temperatureState,      
        CC.TEMPERATURE_COUNTRY AS temperatureCountry
	   ,CC.TemperatureC  AS TemperatureInCelsius
	   ,CC.TemperatureF AS TemperatureInFahrenheit
	   ,CC.BatteryPercent AS battery
	   ,CC.Humidity AS humidity
	   ,CC.Light  AS light 
	   ,CC.IsShockExceeded As shock
	   ,CC.Latitude AS temperatureLatitude -- CL428
	   ,CC.Longitude AS temperatureLongitude -- CL428
      
FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O        
INNER JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION_CALLCHECK (NOLOCK) CC       
ON O.UPSTransportShipmentNumber=CC.UPSORDERNUMBER      
--AND O.SourceSystemKey=CC.SOURCESYSTEMKEY      
WHERE CC.IS_TEMPERATURE='Y'      
AND CC.STATUSDETAILTYPE='TemperatureTracking'      
AND O.AccountId = @VarAccountID         
AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')          
AND  O.[UPSOrderNumber] = @UPSOrderNumber         
AND O.IS_INBOUND=0      
      
      
      
SELECT * INTO #DIGITAL_SUMMARY_ORDERS FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O      
            WHERE O.AccountId = @VarAccountID       
            AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')       
            AND  O.[UPSOrderNumber] = @UPSOrderNumber       
            AND O.IS_INBOUND = 0      
      
                  
  SELECT R.* INTO #DIGITAL_SUMMARY_TRANSPORTATION_REFERENCES  
  FROM [Summary].[DIGITAL_SUMMARY_TRANSPORTATION_REFERENCES] (NOLOCK) R
  JOIN #DIGITAL_SUMMARY_ORDERS O ON R.UPSOrderNumber = O.UPSTransportShipmentNumber --CL320
             WHERE O.UPSOrderNumber=@UPSOrderNumber      
      
           
        
--RESULT SET 3      
      
--SELECT      
--'totalCustomerCharge' AS totalCustomerCharge,      
--'totalCustomerChargeCurrency' AS totalCustomerChargeCurrency,      
--GETDATE() AS invoiceDateTime,      
--'[{"costBreakdownType": "Linehaul","costBreakdownValue": "USD 200"},{"costBreakdownType": "Fuel","costBreakdownValue":"USD 20"}]' AS costBreakdown      
      
--RESULT SET 3     ARUP(10_05_2021)      
select       
(select sum(CAST(Charge as decimal(10,2)))     --UPSGLD-11711
from Summary.DIGITAL_SUMMARY_TRANSPORTATION_RATES_CHARGES (NOLOCK)     
where  ChargeLevel = 'CUSTOMER_RATES'      
--and UpsOrderNumber=O.UPSOrderNumber) AS totalCustomerCharge    
and UpsOrderNumber=O.UPSTransportShipmentNumber) AS totalCustomerCharge  --10/08/2021    
,(SELECT MAX(CurrencyCode) FROM [Summary].[DIGITAL_SUMMARY_TRANSPORTATION_RATES_CHARGES] (NOLOCK)    
  WHERE UpsOrderNumber = O.UPSTransportShipmentNumber AND ChargeLevel = 'CUSTOMER_RATES') AS totalCustomerChargeCurrency      
,O.DateTimeReceived AS invoiceDateTime      
,'['+'{"' +  STUFF((select  '"' +'costBreakdownType'+'"'+':'+'"'+ChargeDescription+'"' +','+'"' + 'costBreakdownValue'+'"'+':'+'"'+cast(sum(cast(charge as decimal(10,2))) as varchar) + '"},{'   from Summary.DIGITAL_SUMMARY_TRANSPORTATION_RATES_CHARGES where  ChargeLevel = 'CUSTOMER_RATES'      
--and UpsOrderNumber=O.UPSOrderNumber --10/08/2021    
and UpsOrderNumber=O.UPSTransportShipmentNumber    
group by ChargeDescription,CurrencyCode      
for xml path('')),1,1,'') + '}' +']' as costBreakdown      
into #CostBreakdown      
FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) O    
    
WHERE O.AccountId = @VarAccountID       
  AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')        
  AND  O.[UPSOrderNumber] = @UPSOrderNumber       
  AND O.IS_INBOUND=0    
      
      
SELECT       
totalCustomerCharge,      
totalCustomerChargeCurrency,      
invoiceDateTime,      
replace(costBreakdown,',{}','') as costBreakdown       
from #CostBreakdown      
      
      
      
-- RESULT SET 4      
      
--SELECT      
--'claimType' AS claimType,      
--'claimAmount' AS claimAmount,      
--'claimAmountCurrency' AS claimAmountCurrency,      
--GETDATE() AS claimFilingDateTime,      
--'claimStatus' AS claimStatus,      
--GETDATE() AS claimClosureDateTime,      
--'claimAmountPaid' AS claimAmountPaid,      
--'claimAmountPaidCurrency' AS claimAmountPaidCurrency      
      
      
SELECT       
    (SELECT ReferenceValue      
     FROM #DIGITAL_SUMMARY_TRANSPORTATION_REFERENCES (NOLOCK)       
  WHERE UPSOrderNumber = O.UPSTransportShipmentNumber				 --CL320 
  --AND SourceSystemKey = O.SourceSystemKey      
  AND ReferenceLevel = 'LoadReference_Claim'      
  AND ReferenceType='Claim Type'      
     ) claimType,      
    (SELECT ReferenceValue      
     FROM #DIGITAL_SUMMARY_TRANSPORTATION_REFERENCES (NOLOCK)       
  WHERE UPSOrderNumber = O.UPSTransportShipmentNumber            --CL320  
  --AND SourceSystemKey = O.SourceSystemKey      
  AND ReferenceLevel = 'LoadReference_Claim'      
     AND ReferenceType='Claim Amount'      
     ) claimAmount      
 ,'' AS claimAmountCurrency      
 ,(SELECT ReferenceValue      
     FROM #DIGITAL_SUMMARY_TRANSPORTATION_REFERENCES (NOLOCK)       
  WHERE UPSOrderNumber = O.UPSTransportShipmentNumber        --CL320  
  --AND SourceSystemKey = O.SourceSystemKey      
  AND ReferenceLevel = 'LoadReference_Claim'      
     AND ReferenceType='Claim Date'      
     ) claimFilingDateTime
	 

 ,(SELECT MA.ACTIVITY_NOTES FROM #DIGITAL_SUMMARY_MILESTONE_ACTIVITY MA   --Added missing alias name
 JOIN #DIGITAL_SUMMARY_ORDERS O ON MA.UPSOrderNumber = O.UPSOrderNumber  
 AND MA.SourceSystemKey = O.SourceSystemKey
 WHERE MA.ActivityCode='COSD'      --Added missing alias name
  ) claimStatus											---CL320
  

  ,(SELECT convert(nvarchar(MAX), ActivityDate, 21) FROM #DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK)       
                                    WHERE ActivityCode='CC'      
 )claimClosureDateTime      
 ,(SELECT ReferenceValue      
  FROM #DIGITAL_SUMMARY_TRANSPORTATION_REFERENCES (NOLOCK)  
   WHERE UPSOrderNumber = O.UPSTransportShipmentNumber             --CL320
                          AND ReferenceLevel = 'LoadReference_Claim'      
        AND ReferenceType='Claim Amount Paid'      
        )claimAmountPaid      
 --,[Summary].[usp_Summary_Get_Claim_Details](O.UPSOrderNumber, O.SourceSystemKey,'CLAIMAMOUNTPAID') AS claimAmountPaid      
 ,'' AS claimAmountPaidCurrency      
FROM #DIGITAL_SUMMARY_ORDERS (NOLOCK) O        
      
      
      
      
      
END        
GO

