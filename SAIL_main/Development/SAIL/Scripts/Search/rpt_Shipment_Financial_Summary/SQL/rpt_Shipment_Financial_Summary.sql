/****** Object:  StoredProcedure [digital].[rpt_Shipment_Financial_Summary]    Script Date: 1/6/2022 1:10:51 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


      
      
/***      
CHANGE LOG      
----------      
CHANGED BY    DATE    SPRINT  CHANGES      
PRASHANT RAI       08/26/2021   36th         SP TO ACCEPT MULTIPLE SHIPMENT TYPES COMMA SEPARATED.      
PRASHANT RAI            09/30/2021    38th          SP TO ACCEPT ADDITIONAL SHIPMENTTYPE - OUTBOUND   
SHEETAL  10/22/2021  ChangeLog#248   Changed logic to get CustomerCharge data for outbound, inbound and movement  
										Changed logic to get Number of claims
SHEETAL		11/23/2021		UPSGLD-12128	Added filter to exclude records where either Origin or Destination is NULL.	
***/      
      
      
      
      
/****      
exec [digital].[rpt_Shipment_Financial_Summary]  @AccountKeys ='[{"DPProductLineKey": "a2B1487c-3878-4A06-898B-4EA06DF022BF","DPServiceLineKey": "1AD6CEC2-F040-43AA-BBCE-17548A833665"      
     },{"DPProductLineKey": "344C94d7-DE3D-4351-B9E4-0FD3C1E55B3C","DPServiceLineKey": "D0B677E7-159F-4543-8FA7-D9C4B1C18302"      
    }]'      
,@Date ='{      
    "shipmentCreationStartDate": "2021-06-01",      
    "shipmentCreationEndDate": "2021-06-27",      
    "shipmentCreationDifferentialStartDate":"2021-06-01",      
    "shipmentCreationDifferentialEndDate":"2021-06-27"         
}'      
,@ShipmentType ='movement,INBOUND'      
      
          
****/      
      
CREATE PROCEDURE [digital].[rpt_Shipment_Financial_Summary]       
      
@AccountKeys nvarchar(max)      
,@Date nvarchar(max)      
,@ShipmentType varchar(50)='*'      
      
AS      
      
BEGIN      
      
      
DECLARE @VarAccountID varchar(50),      
        @DPServiceLineKey varchar(50),      
    @VarShipmentType varchar(50),      
    @shipmentCreationStartDate date,      
    @shipmentCreationEndDate date,      
    @shipmentCreationDifferentialStartDate datetime,      
    @shipmentCreationDifferentialEndDate datetime,      
    @NULLCreatedDate varchar(1),      
    @NULLShipmentType varchar(1),      
    @VarDPServiceLineKey varchar(50),      
    @Is_Inbound varchar(1),      
     @shipmentCreationStartDateTime datetime,      
    @shipmentCreationEndDateTime datetime      
      
      
      
SELECT UPPER(DPProductLineKey) AS DPProductLineKey,      
       UPPER(DPServiceLineKey) AS DPServiceLineKey      
    into #ACCOUNTINFO      
    FROM OPENJSON(@AccountKeys)      
    WITH(      
   DPProductLineKey VARCHAR(MAX),      
   DPServiceLineKey VARCHAR(MAX)      
    )      
      
      
      
  SELECT  @shipmentCreationStartDate = shipmentCreationStartDate,      
          @shipmentCreationEndDate   = shipmentCreationEndDate,      
    @shipmentCreationDifferentialStartDate = shipmentCreationDifferentialStartDate,      
    @shipmentCreationDifferentialEndDate = shipmentCreationDifferentialEndDate      
          
FROM OPENJSON(@Date)      
WITH (      
shipmentCreationStartDate date,      
shipmentCreationEndDate date,      
shipmentCreationDifferentialStartDate date,      
shipmentCreationDifferentialEndDate date      
      
      
     )      
      
  SET @VarShipmentType = UPPER(@ShipmentType)      
   SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)      
        
  IF @DPServiceLineKey IS NULL      
    SET @VarDPServiceLineKey = '*'      
      
      
  CREATE TABLE #SHIPMENTTYPE (SHIP_TYPE INT)      
      
      
IF @VarShipmentType='' OR ISNULL(@VarShipmentType,'*')='*'      
begin      
     INSERT INTO #SHIPMENTTYPE VALUES (1)      
  INSERT INTO #SHIPMENTTYPE VALUES (2)      
  INSERT INTO #SHIPMENTTYPE VALUES (0)      
 end       
      
      
      
 IF @VarShipmentType LIKE '%INBOUND%'      
 INSERT INTO #SHIPMENTTYPE VALUES (1)      
      
      
  IF @VarShipmentType LIKE '%MOVEMENT%'      
 INSERT INTO #SHIPMENTTYPE VALUES (2)      
      
 IF @VarShipmentType LIKE '%OUTBOUND%'      
 INSERT INTO #SHIPMENTTYPE VALUES (0)      
      
      
        
 --SET @Is_Inbound = CASE WHEN @VarShipmentType='INBOUND' THEN 1      
 --                    WHEN @VarShipmentType='MOVEMENT' THEN 2      
 --                    WHEN @VarShipmentType='OUTBOUND' THEN 0       
                --end      
      
  SET @shipmentCreationStartDateTime=@shipmentCreationStartDate      
  SET @shipmentCreationEndDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @shipmentCreationEndDate)))      
      
      
--------------------------------------------------CustomerCharge     10/22/2021  
      
SELECT COUNT( UPSOrderNumber) NoofShipments ,       cosmos
 SUM(TotalCustomerCharge) AS TotalCustomerCharge,      --total charge
 SUM(ShipmentQnty) ShipmentQnty       -- sum from trackingNumber list
into #final      
FROM       
(      
select       
O.UPSOrderNumber AS UPSOrderNumber      
,SUM(CAST(P.CHARGE AS DECIMAL(10,2))) as TotalCustomerCharge      
,SUM(CAST(T.SHIPMENT_QUANTITY AS DECIMAL(10,2))) ShipmentQnty      
FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O  
JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION (NOLOCK) ST ON CASE WHEN IS_INBOUND in (2,1) THEN O.UPSOrderNumber ELSE O.UPSTransportShipmentNumber END = ST.UpsOrderNumber   
             AND O.SourceSystemKey = CASE WHEN IS_INBOUND IN (2,1) THEN ST.SourceSystemKey ELSE ST.UpsWMSSourceSystemKey END   
JOIN [Summary].DIGITAL_SUMMARY_ORDER_TRACKING (NOLOCK) T ON O.UPSOrderNumber=T.UPSOrderNumber and O.SourceSystemKey=T.SourceSystemKey      
JOIN FACT_TRANSPORTATION_RATES_CHARGES(nolock) P on P.UPS_ORDER_NUMBER = ST.UpsOrderNumber and P.SOURCE_SYSTEM_KEY=ST.SourceSystemKey AND P.CHARGE_LEVEL = 'CUSTOMER_RATES'      
where           
((O.DateTimeReceived  BETWEEN @shipmentCreationStartDateTime  AND @shipmentCreationEndDateTime ) OR @NULLCreatedDate = '*')      
and  O.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO)      
  AND (O.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR @VarDPServiceLineKey = '*')      
   AND (@NULLShipmentType='*' OR IS_INBOUND IN (SELECT SHIP_TYPE FROM #SHIPMENTTYPE))  AND    
    O.OrderStatusName<>'Cancelled'    
   GROUP BY O.UPSOrderNumber      
   ) TBL   
      
      
 select count(distinct O.UPSOrderNumber) as NumberOfClaims    
 into #NumberOfClaims    
 FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O 
 JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION ST(NOLOCK) ON CASE WHEN IS_INBOUND IN (2,1)  THEN O.UPSOrderNumber ELSE O.UPSTransportShipmentNumber END = ST.UpsOrderNumber --10/22/2021 
													AND O.SourceSystemKey = CASE WHEN IS_INBOUND IN (2,1)  THEN ST.SourceSystemKey ELSE ST.UpsWMSSourceSystemKey END  --10/22/2021 
JOIN [Summary].[DIGITAL_SUMMARY_TRANSPORTATION_REFERENCES] (nolock) P on ST.UpsOrderNumber=P.UPSOrderNumber and ST.SourceSystemKey=P.SourceSystemKey   --10/22/2021  
where           
((O.DateTimeReceived  BETWEEN @shipmentCreationStartDateTime  AND @shipmentCreationEndDateTime ) OR @NULLCreatedDate = '*')    
and  O.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO)    
  AND (O.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR @VarDPServiceLineKey = '*')    
   AND (@NULLShipmentType='*' OR IS_INBOUND IN (SELECT SHIP_TYPE FROM #SHIPMENTTYPE))    
   AND O.OrderStatusName<>'Cancelled'    
and ReferenceLevel = 'LoadReference_Claim'       
      
      
select top 1 CURRENCY_CODE  AS CurrencyCode into #currencycode       --totalChargeCurrency
FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O    
JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION (NOLOCK) ST ON CASE WHEN IS_INBOUND IN (2,1)  THEN O.UPSOrderNumber ELSE O.UPSTransportShipmentNumber END = ST.UpsOrderNumber   
             AND O.SourceSystemKey = CASE WHEN IS_INBOUND IN (2,1)  THEN ST.SourceSystemKey ELSE ST.UpsWMSSourceSystemKey END   
JOIN FACT_TRANSPORTATION_RATES_CHARGES(nolock) P on P.UPS_ORDER_NUMBER = ST.UpsOrderNumber and P.SOURCE_SYSTEM_KEY=ST.SourceSystemKey AND P.CHARGE_LEVEL = 'CUSTOMER_RATES'      
where           
((O.DateTimeReceived                   BETWEEN @shipmentCreationStartDateTime  AND @shipmentCreationEndDateTime ) OR @NULLCreatedDate = '*')      
and  O.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO)      
  AND (O.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR @VarDPServiceLineKey = '*')      
   AND (@NULLShipmentType='*' OR IS_INBOUND IN (SELECT SHIP_TYPE FROM #SHIPMENTTYPE))      
   AND O.OrderStatusName<>'Cancelled'        
      
      
      
SELECT COUNT( UPSOrderNumber) NoofShipments ,       
 SUM(TotalCustomerCharge) AS TotalCustomerCharge,      
 SUM(ShipmentQnty) ShipmentQnty      
into #differential      
FROM       
(      
select        
O.UPSOrderNumber AS UPSOrderNumber      
,SUM(CAST(P.CHARGE AS DECIMAL(10,2))) as TotalCustomerCharge      
,SUM(CAST(T.SHIPMENT_QUANTITY AS DECIMAL(10,2))) ShipmentQnty      
FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O   
JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION (NOLOCK) ST ON CASE WHEN IS_INBOUND IN (2,1)  THEN O.UPSOrderNumber ELSE O.UPSTransportShipmentNumber END = ST.UpsOrderNumber   
             AND O.SourceSystemKey = CASE WHEN IS_INBOUND IN (2,1)  THEN ST.SourceSystemKey ELSE ST.UpsWMSSourceSystemKey END   
JOIN [Summary].DIGITAL_SUMMARY_ORDER_TRACKING (NOLOCK) T ON O.UPSOrderNumber=T.UPSOrderNumber and O.SourceSystemKey=T.SourceSystemKey      
JOIN FACT_TRANSPORTATION_RATES_CHARGES(nolock) P on P.UPS_ORDER_NUMBER = ST.UpsOrderNumber and P.SOURCE_SYSTEM_KEY=ST.SourceSystemKey AND P.CHARGE_LEVEL = 'CUSTOMER_RATES'      
where           
((O.DateTimeReceived                   BETWEEN @shipmentCreationDifferentialStartDate  AND @shipmentCreationDifferentialEndDate ) OR @NULLCreatedDate = '*')      
and  O.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO)      
  AND (O.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR @VarDPServiceLineKey = '*')      
   AND (@NULLShipmentType='*' OR IS_INBOUND IN (SELECT SHIP_TYPE FROM #SHIPMENTTYPE))      
   AND O.OrderStatusName<>'Cancelled'      
   GROUP BY O.UPSOrderNumber      
   ) TBL2    
      
      
-- RESULT SET 1      
      
select f.TotalCustomerCharge,      
(select CurrencyCode from #currencycode) TotalCustomerChargeCurrency,f.TotalCustomerCharge/f.NoofShipments as AverageCostPerShipment,      
f.TotalCustomerCharge/f.ShipmentQnty as AverageCostPerUnit,      
(select P.TotalCustomerCharge/P.NoofShipments  from #differential P) as AverageCostPerShipmentForDifferential,      
(select P.TotalCustomerCharge/P.ShipmentQnty  from #differential P) as AverageCostPerUnitForDifferential,      
(select NumberOfClaims from #NumberOfClaims) as NumberOfClaims      
from       
#final f      
      
      
      
--RESULT SET 2    -10/22/2021  
      
Select  TOP 5      
O.OriginCity AS OriginCity,      
O.OriginCountry AS OriginCountry,      
O.DestinationCity AS DestinationCity,      
O.DestinationCountry AS DestinationCountry,      
SUM(CAST(P.CHARGE AS DECIMAL(10,2))) as TotalCustomerCharge,      
P.CURRENCY_CODE AS TotalCustomerChargeCurrency      
      
FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O   
JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION (NOLOCK) ST ON CASE WHEN IS_INBOUND IN (2,1)  THEN O.UPSOrderNumber ELSE O.UPSTransportShipmentNumber END = ST.UpsOrderNumber   
             AND O.SourceSystemKey = CASE WHEN IS_INBOUND IN (2,1)  THEN ST.SourceSystemKey ELSE ST.UpsWMSSourceSystemKey END   
JOIN FACT_TRANSPORTATION_RATES_CHARGES(nolock) P on P.UPS_ORDER_NUMBER = ST.UpsOrderNumber and P.SOURCE_SYSTEM_KEY=ST.SourceSystemKey AND P.CHARGE_LEVEL = 'CUSTOMER_RATES'        
where           
((O.DateTimeReceived                   BETWEEN @shipmentCreationStartDateTime  AND @shipmentCreationEndDateTime ) OR @NULLCreatedDate = '*')      
AND  O.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO)      
AND (O.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR @VarDPServiceLineKey = '*')      
AND (@NULLShipmentType='*' OR IS_INBOUND IN (SELECT SHIP_TYPE FROM #SHIPMENTTYPE))      
AND O.OrderStatusName<>'Cancelled'     
AND (O.OriginCity <> 'Not Available' AND DestinationCity <> 'Not Available') --UPSGLD-12128
GROUP BY       
OriginCity,      
OriginCountry,      
DestinationCity,      
DestinationCountry,      
CURRENCY_CODE      
ORDER BY TotalCustomerCharge DESC       
      
      
---10/22/2021      
      
select        
      
O.Carrier,      
--sum(cast(Summary.usp_Get_Customer_Charges(O.UPSOrderNumber, O.SourceSystemKey,'CHARGE','CHARGE') as DECIMAL(10,2))) as TotalCustomerCharge      
SUM(CAST(P.CHARGE AS DECIMAL(10,2))) as TotalCustomerCharge      
--,P.CurrencyCode as TotalCustomerChargeCurrency      
,count(distinct O.UPSOrderNumber) as NoofShipments      
,SUM(CAST(T.SHIPMENT_QUANTITY AS DECIMAL(10,2))) ShipmentQnty      
--, O.ServiceMode as ShipmentMode      
into #carrierfinalcarrier      
FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O    
JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION (NOLOCK) ST ON CASE WHEN IS_INBOUND IN (2,1)  THEN O.UPSOrderNumber ELSE O.UPSTransportShipmentNumber END = ST.UpsOrderNumber   
             AND O.SourceSystemKey = CASE WHEN IS_INBOUND IN (2,1)  THEN ST.SourceSystemKey ELSE ST.UpsWMSSourceSystemKey END   
JOIN [Summary].DIGITAL_SUMMARY_ORDER_TRACKING (NOLOCK) T ON O.UPSOrderNumber=T.UPSOrderNumber and O.SourceSystemKey=T.SourceSystemKey      
JOIN FACT_TRANSPORTATION_RATES_CHARGES(nolock) P on P.UPS_ORDER_NUMBER = ST.UpsOrderNumber and P.SOURCE_SYSTEM_KEY=ST.SourceSystemKey AND P.CHARGE_LEVEL = 'CUSTOMER_RATES'      
where           
((O.DateTimeReceived                   BETWEEN @shipmentCreationStartDateTime  AND @shipmentCreationEndDateTime ) OR @NULLCreatedDate = '*')      
and  O.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO)      
  AND (O.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR @VarDPServiceLineKey = '*')      
   AND (@NULLShipmentType='*' OR IS_INBOUND IN (SELECT SHIP_TYPE FROM #SHIPMENTTYPE))      
   AND O.OrderStatusName<>'Cancelled'      
  group by O.Carrier      
    -- O.ServiceMode      
        
      
      
--10/22/2021      
      
select        
O.ServiceMode as ShipmentMode,      
--sum(cast(Summary.usp_Get_Customer_Charges(O.UPSOrderNumber, O.SourceSystemKey,'CHARGE','CHARGE') as DECIMAL(10,2))) as TotalCustomerCharge      
SUM(CAST(P.CHARGE AS DECIMAL(10,2))) as TotalCustomerCharge      
--,P.CurrencyCode as TotalCustomerChargeCurrency      
,count(distinct O.UPSOrderNumber) as NoofShipments      
,SUM(CAST(T.SHIPMENT_QUANTITY AS DECIMAL(10,2))) ShipmentQnty      
--, O.ServiceMode as ShipmentMode      
into #carrierfinalshipmentmode      
FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O      
JOIN Summary.DIGITAL_SUMMARY_TRANSPORTATION (NOLOCK) ST ON CASE WHEN IS_INBOUND IN (2,1)  THEN O.UPSOrderNumber ELSE O.UPSTransportShipmentNumber END = ST.UpsOrderNumber   
             AND O.SourceSystemKey = CASE WHEN IS_INBOUND IN (2,1)  THEN ST.SourceSystemKey ELSE ST.UpsWMSSourceSystemKey END   
JOIN [Summary].DIGITAL_SUMMARY_ORDER_TRACKING (NOLOCK) T ON O.UPSOrderNumber=T.UPSOrderNumber and O.SourceSystemKey=T.SourceSystemKey      
JOIN FACT_TRANSPORTATION_RATES_CHARGES(nolock) P on P.UPS_ORDER_NUMBER = ST.UpsOrderNumber and P.SOURCE_SYSTEM_KEY=ST.SourceSystemKey AND P.CHARGE_LEVEL = 'CUSTOMER_RATES'  
where           
((O.DateTimeReceived                   BETWEEN @shipmentCreationStartDateTime  AND @shipmentCreationEndDateTime ) OR @NULLCreatedDate = '*')      
and  O.AccountId IN (SELECT DPProductLineKey FROM #ACCOUNTINFO)      
  AND (O.DP_SERVICELINE_KEY IN (SELECT DPServiceLineKey FROM #ACCOUNTINFO) OR @VarDPServiceLineKey = '*')      
   AND (@NULLShipmentType='*' OR IS_INBOUND IN (SELECT SHIP_TYPE FROM #SHIPMENTTYPE))      
   AND O.OrderStatusName<>'Cancelled'      
  group by  O.ServiceMode         
      
  --RESULT SET 3      
      
select       
f.Carrier as CarrierName,      
(select CurrencyCode from #currencycode) TotalCustomerChargeCurrency,      
f.TotalCustomerCharge,      
f.TotalCustomerCharge/f.NoofShipments as AverageCostPerShipment,      
f.TotalCustomerCharge/f.ShipmentQnty as AverageCostPerUnit,      
 null  AS AverageCostPerMile,      
 null  AS AverageCostPerWeight,      
 null  AS AverageCostPerSKU      
from       
#carrierfinalcarrier f      
      
      
--RESULT SET 4      
      
SELECT       
ShipmentMode,      
f.TotalCustomerCharge as TotalCustomerCharge,      
f.TotalCustomerCharge/f.NoofShipments as AverageCostPerShipment,      
f.TotalCustomerCharge/f.ShipmentQnty as AverageCostPerUnit,      
(select P.TotalCustomerCharge/P.NoofShipments  from #differential P) as AverageCostPerShipmentForDifferential,      
(select P.TotalCustomerCharge/P.ShipmentQnty from #differential P) as AverageCostPerUnitForDifferential      
      
FROM #carrierfinalshipmentmode f      
      
      
         
 END; 
GO

