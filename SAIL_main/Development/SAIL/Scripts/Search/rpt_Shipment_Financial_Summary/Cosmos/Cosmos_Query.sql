-- rpt_Shipment_Financial_Summary

--Result Set 1

/*
Parameter Requirement info -
->@DPProductLineKey  required 
->@DPServiceLineKey  optional
->@Date.shipmentCreationStartDate      optional
->@Date.shipmentCreationEndDate        optional
->@ShipmentType  optional

Target Container - digital_summary_orders
*/

select 
c.totalCharge TotalCustomerCharge
,c.totalChargeCurrency TotalCustomerChargeCurrency
,null AverageCostPerShipment --TotalCustomerCharge/f.NoofShipments
,null AverageCostPerUnit  --f.TotalCustomerCharge/f.ShipmentQnty
,null AverageCostPerShipmentForDifferential --(select P.TotalCustomerCharge/P.NoofShipments  from #differential P) 
,null AverageCostPerUnitForDifferential  --(select P.TotalCustomerCharge/P.ShipmentQnty  from #differential P) 
,Sum(c.ISCLAIM ='Y'?1:0 ) NumberOfClaims
from c
where                 
((c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate  AND @Date.shipmentCreationEndDate ))      
AND  c.AccountId IN (@AccountKeys.DPProductLineKey)      
AND c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey )      
AND S_INBOUND IN (@ShipmentType)      
AND c.OrderStatusName<>'Cancelled'   
group by c.totalCharge 
,c.totalChargeCurrency 

--Result Set 2

/*
Parameter Requirement info -
->@DPProductLineKey  required 
->@DPServiceLineKey  optional
->@Date.shipmentCreationStartDate      optional
->@Date.shipmentCreationEndDate        optional
->@ShipmentType  optional

Target Container - digital_summary_orders
*/

Select  TOP 5      
c.OriginCity AS OriginCity,      
c.OriginCountry AS OriginCountry,      
c.DestinationCity AS DestinationCity,      
c.DestinationCountry AS DestinationCountry,      
 sum(c.totalCharge)   TotalCustomerCharge,
 c.totalChargeCurrency TotalCustomerChargeCurrency
from c   
where           
((c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate  AND @Date.shipmentCreationEndDate ))      
AND  c.AccountId IN (@AccountKeys.DPProductLineKey)      
AND c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey )      
AND c.IS_INBOUND IN (@ShipmentType)      
AND c.OrderStatusName<>'Cancelled'     
AND (c.OriginCity <> 'Not Available' AND DestinationCity <> 'Not Available')
GROUP BY       
c.OriginCity,      
c.OriginCountry,      
c.DestinationCity,      
c.DestinationCountry,  
c.totalChargeCurrency

--Result Set 3

/*
Parameter Requirement info -
->@DPProductLineKey  required 
->@DPServiceLineKey  optional
->@Date.shipmentCreationStartDate      optional
->@Date.shipmentCreationEndDate        optional
->@ShipmentType  optional

Target Container - digital_summary_orders
*/

select       
c.Carrier as CarrierName,      
c.totalChargeCurrency TotalCustomerChargeCurrency,      
c.totalCharge TotalCustomerCharge,      
null AverageCostPerShipment,      -- to be computed f.TotalCustomerCharge/f.NoofShipments  null for splus 
null AverageCostPerUnit,      -- to be computed f.TotalCustomerCharge/f.ShipmentQnty  null for splus 
 null  AS AverageCostPerMile,      
 null  AS AverageCostPerWeight,      
 null  AS AverageCostPerSKU     
from c
where           
((c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate  AND @Date.shipmentCreationEndDate ) )      
AND  c.AccountId IN (@AccountKeys.DPProductLineKey)      
AND c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey )      
AND c.IS_INBOUND IN (@ShipmentType)      
AND c.OrderStatusName<>'Cancelled'     

--Result Set 4

/*
Parameter Requirement info -
->@DPProductLineKey  required 
->@DPServiceLineKey  optional
->@Date.shipmentCreationStartDate      optional
->@Date.shipmentCreationEndDate        optional
->@ShipmentType  optional

Target Container - digital_summary_orders
*/

select 
c.ShipmentMode,      
c.totalCharge TotalCustomerCharge,      
null AverageCostPerShipment,  --f.TotalCustomerCharge/f.NoofShipments null for splus    
null AverageCostPerUnit,      --f.TotalCustomerCharge/f.ShipmentQnty
null AverageCostPerShipmentForDifferential,      -- (select P.TotalCustomerCharge/P.NoofShipments  from #differential P) null for splus 
null AverageCostPerUnitForDifferential      --(select P.TotalCustomerCharge/P.ShipmentQnty from #differential P)  null for splus 
FROM c 
where           
((c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate  AND @Date.shipmentCreationEndDate ) )      
AND  c.AccountId IN (@AccountKeys.DPProductLineKey)      
AND c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey )      
AND c.IS_INBOUND IN (@ShipmentType)      
AND c.OrderStatusName<>'Cancelled' 