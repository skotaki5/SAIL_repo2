--AUTHOR : SHREY JAIN
--DESCRIPTION:rpt_Inbound_Shipment_WIP_Activities
--DATE :29-04-2022

/*NOTE:
  IF ISNULL(@Date.shipmentCreationStartDate,'')='' OR ISNULL(@Date.shipmentCreationEndDate,'')=''
    SET @NULLCreatedDate = '*'
 
  IF ISNULL(@Date.shipmentReceivedStartDate,'')='' OR ISNULL(@Date.shipmentReceivedEndDate,'')='' 
    SET @NULLReceivedDate = '*'
 

 

   SET @isASN = CASE WHEN UPPER(@InboundType)='ASN' THEN 1
                     WHEN UPPER(@InboundType) ='TRANSPORT ORDER' THEN 0
                END 

*/

--Result Set 1

/*
-- Parameter requirement info.
@DPProductLineKey required
@DPServiceLineKey optional
@AccountKeys      required
@warehouseId      optional
@startDate        optional
@endDate          optional
@Date             optional
@wipActivityName  optional
@warehouseCode    optional
@milestoneStatus  optional
@inboundType      optional

--Target container digital_summary_orders
*/


-- IF @NULLReceivedDate = '*' and     @NULLCreatedDate != '*'  
 
select count(1) as total_shipments
from c
join t in c.WIP_ActivityName
WHERE (c.AccountId IN (@AccountKeys) or c.AccountId = @DPProductLineKey )
AND t.MilestoneName <> 'PUTAWAY'
AND (c.DP_SERVICELINE_KEY IN (@DPServiceLineKey))
AND (c.warehouseCode IN (@warehouseCode))   
AND (c.DateTimeReceived  BETWEEN @Date.shipmentCreationStartDate AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1,@Date.shipmentCreationEndDate)))  
AND c.IS_INBOUND = 1   
AND (is_null(c.OrderCancelledFlag)?'N':c.OrderCancelledFlag) = 'N'
AND c.milestoneStatus in (@milestoneStatus)
AND ((c.IS_ASN = null) ? 0 :c.IS_ASN  = @isASN)
And t.Type = 'IN'
AND c.is_deleted = 0
AND t.WIP_ActivityName IN (@wipActivityName)
AND c.ma_ActivityDate_1 != null
 
 
-- IF @NULLReceivedDate != '*' and     @NULLCreatedDate = '*'
 
select count(1) as total_shipments
 from c
join t in c.WIP_ActivityName
WHERE (c.AccountId IN (@AccountKeys) or c.AccountId = @DPProductLineKey)
AND t.MilestoneName <> 'PUTAWAY'
AND (c.DP_SERVICELINE_KEY IN (@DPServiceLineKey))
AND (c.warehouseCode IN (@warehouseCode))    
AND c.IS_INBOUND = 1   
AND (is_null(c.OrderCancelledFlag)?'N':c.OrderCancelledFlag) = 'N'
AND c.milestoneStatus in (@milestoneStatus)
AND (c.ma_ActivityDate_2  BETWEEN @Date.shipmentReceivedStartDate AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1,@Date.shipmentReceivedEndDate)))
AND ((c.IS_ASN = null) ? 0 :c.IS_ASN  = @isASN) --depends on params   SET @isASN = CASE WHEN @VarInboundType='ASN' THEN 1 WHEN @VarInboundType='TRANSPORT ORDER' THEN 0
AND c.is_deleted = 0  
And t.Type = 'IN'
AND t.WIP_ActivityName IN (@wipActivityName)
AND c.ShipmentCreationDate > DateTimeAdd("dd", -90, GetCurrentDateTime())
 
 
-- IF @NULLReceivedDate = '*' and     @NULLCreatedDate = '*' 
 
select count(1) as total_shipments
 from c
join t in c.WIP_ActivityName
join d in c.ma_ActivityDate_list_distinct
WHERE (c.AccountId IN (@AccountKeys) or c.AccountId = @DPProductLineKey)
AND t.MilestoneName <> 'PUTAWAY'
AND (c.DP_SERVICELINE_KEY IN (@DPServiceLineKey))
AND (c.warehouseCode IN (@warehouseCode))   
AND c.IS_INBOUND = 1   
AND (is_null(c.OrderCancelledFlag)?'N':c.OrderCancelledFlag) = 'N'
AND c.milestoneStatus in (@milestoneStatus)
AND ((c.IS_ASN = null) ? 0 :c.IS_ASN  = @isASN)
AND c.shipmentReceivedDate != null
AND c.is_deleted = 0  
AND  t.Type = 'IN'
AND t.WIP_ActivityName IN (@wipActivityName)
AND d!=''
AND c.ShipmentCreationDate > DateTimeAdd("dd", -90, GetCurrentDateTime())
 
 
--IF @NULLReceivedDate != '*' and    @NULLCreatedDate != '*'
 
select count(1) as total_shipments
from c
join t in c.WIP_ActivityName
WHERE (c.AccountId IN (@AccountKeys) or c.AccountId = @DPProductLineKey)
AND t.MilestoneName <> 'PUTAWAY'
AND (c.DP_SERVICELINE_KEY IN (@DPServiceLineKey))
AND (c.warehouseCode IN (@warehouseCode))
AND (c.DateTimeReceived  BETWEEN @Date.shipmentCreationStartDate AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1,@Date.shipmentCreationEndDate)))        
AND c.IS_INBOUND = 1   
AND (is_null(c.OrderCancelledFlag)?'N':c.OrderCancelledFlag) = 'N'
AND c.milestoneStatus in (@milestonestatus)
AND ((c.IS_ASN = null) ? 0 :c.IS_ASN  = @isASN)
AND (c.ma_ActivityDate_1 BETWEEN @Date.shipmentCreationStartDate AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1,@Date.shipmentCreationEndDate)))  
AND c.is_deleted = 0
And   t.Type = 'IN'
AND t.WIP_ActivityName IN (@wipActivityName)
AND c.shipmentReceivedDate !=null

--Result Set 2

/*
-- Parameter requirement info.
@DPProductLineKey required
@DPServiceLineKey optional
@AccountKeys      required
@warehouseId      optional
@startDate        optional
@endDate          optional
@Date             optional
@wipActivityName  optional
@warehouseCode    optional
@milestoneStatus  optional
@inboundType      optional

--Target container digital_summary_orders
*/


--  IF @NULLReceivedDate = '*' and    @NULLCreatedDate != '*'
 
IF @topRow = 0
 
Select 
a.upsShipmentNumber,
f.UPSASNNumber asnNumber,
a.referenceNumber,
f.inbound_line_count linesCount,
f.inbound_ShippedQuantity_sum UnitsCount,
f.inbound_cases_sum CasesCount,
a.wipActivityName,
a.warehouseId,
a.warehouseCode,
a.milestoneStatus,
a.inboundType,
a.ShipmentCreationDate,
a.shipmentReceivedDate
from  
(select 
c.upsShipmentNumber,
c.referenceNumber,
c.warehouseId  warehouseId,
c.warehouseCode,
t.MilestoneName milestoneStatus,
c.inboundType,
c.DateTimeReceived ShipmentCreationDate, 
c.shipmentReceivedDate,
t.WIP_ActivityName wipActivityName,
c.inbound_shipment_listing = null? Array(select 0 UPSASNNumber, 0 inbound_line_count, 0 inbound_ShippedQuantity_sum, 0 inbound_cases_sum):c.inbound_shipment_listing shipment_listing
from c 
join t in c.WIP_ActivityName
WHERE (c.AccountId IN (@AccountKeys) or c.AccountId = @DPProductLineKey )
AND t.MilestoneName <> 'PUTAWAY'
AND (c.DP_SERVICELINE_KEY IN (@DPServiceLineKey))
AND (c.warehouseCode IN (@warehouseCode))   
AND (c.DateTimeReceived  BETWEEN @Date.shipmentCreationStartDate AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1,@Date.shipmentCreationEndDate)))  
AND c.IS_INBOUND = 1   
AND (is_null(c.OrderCancelledFlag)?'N':c.OrderCancelledFlag) = 'N'
AND c.milestoneStatus in (@milestoneStatus)
AND ((c.IS_ASN = null) ? 0 :c.IS_ASN  = @isASN)
And t.Type = 'IN'
AND c.is_deleted = 0
AND t.WIP_ActivityName IN (@wipActivityName)
AND c.ma_ActivityDate_1 != null)a join f in a.shipment_listing
order by a.ShipmentCreationDate desc
 
--ELSE:
 
 SELECT TOP @topRow
a.upsShipmentNumber,
f.UPSASNNumber asnNumber,
a.referenceNumber,
f.inbound_line_count linesCount,
f.inbound_ShippedQuantity_sum UnitsCount,
f.inbound_cases_sum CasesCount,
a.wipActivityName,
a.warehouseId,
a.warehouseCode,
a.milestoneStatus,
a.inboundType,
a.ShipmentCreationDate,
a.shipmentReceivedDate
from  
(select 
c.upsShipmentNumber,
c.referenceNumber,
c.warehouseId as warehouseId,
c.warehouseCode,
t.MilestoneName milestoneStatus,
c.inboundType,
c.DateTimeReceived ShipmentCreationDate,
c.shipmentReceivedDate,
t.WIP_ActivityName wipActivityName,
c.inbound_shipment_listing = null? Array(select 0 UPSASNNumber, 0 inbound_line_count, 0 inbound_ShippedQuantity_sum, 0 inbound_cases_sum):c.inbound_shipment_listing shipment_listing
from c 
join t in c.WIP_ActivityName
WHERE (c.AccountId IN (@AccountKeys) or c.AccountId = @DPProductLineKey )
AND t.MilestoneName <> 'PUTAWAY'
AND (c.DP_SERVICELINE_KEY IN (@DPServiceLineKey))
AND (c.warehouseCode IN (@warehouseCode))   
AND (c.DateTimeReceived  BETWEEN @Date.shipmentCreationStartDate AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1,@Date.shipmentCreationEndDate)))  
AND c.IS_INBOUND = 1   
AND (is_null(c.OrderCancelledFlag)?'N':c.OrderCancelledFlag) = 'N'
AND c.milestoneStatus in (@milestoneStatus)
AND ((c.IS_ASN = null) ? 0 :c.IS_ASN  = @isASN)
And t.Type = 'IN'
AND c.is_deleted = 0
AND t.WIP_ActivityName IN (@wipActivityName)
AND c.ma_ActivityDate_1 != null)a join f in a.shipment_listing
order by a.ShipmentCreationDate desc 
 
  
--IF @NULLReceivedDate != '*' and    @NULLCreatedDate = '*'
 
IF @topRow = 0
 
Select 
a.upsShipmentNumber,
f.UPSASNNumber asnNumber,
a.referenceNumber,
f.inbound_line_count linesCount,
f.inbound_ShippedQuantity_sum UnitsCount,
f.inbound_cases_sum CasesCount,
a.wipActivityName,
a.warehouseId,
a.warehouseCode,
a.milestoneStatus,
a.inboundType,
a.ShipmentCreationDate,
a.shipmentReceivedDate
 from  
(select 
c.upsShipmentNumber,
c.referenceNumber,
c.warehouseId as warehouseId,
c.warehouseCode,
t.MilestoneName milestoneStatus,
c.inboundType,
c.DateTimeReceived ShipmentCreationDate,
c.shipmentReceivedDate,
t.WIP_ActivityName wipActivityName,
c.inbound_shipment_listing = null? Array(select 0 UPSASNNumber, 0 inbound_line_count, 0 inbound_ShippedQuantity_sum, 0 inbound_cases_sum):c.inbound_shipment_listing shipment_listing
from c 
join t in c.WIP_ActivityName 
WHERE (c.AccountId IN ('E648FA6F-6253-428E-8AC9-201E3EF83B91'))
AND t.MilestoneName <> 'PUTAWAY'
AND (c.DP_SERVICELINE_KEY IN ('53D60776-D3BD-4E6A-8E37-F591C148294B'))
AND (c.warehouseCode IN ('KYSPL'))  
AND c.IS_INBOUND = 1  
AND (is_null(c.OrderCancelledFlag)?'N':c.OrderCancelledFlag) = 'N'
AND c.milestoneStatus in ('ASN CREATED')
AND (c.ma_ActivityDate_2  BETWEEN '2022-03-01 00:00:00.000' AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1,'2022-03-31 00:00:00.000')))
AND ((c.IS_ASN = null) ? 0 :c.IS_ASN  = 1) --depends on params   SET @isASN = CASE WHEN @VarInboundType='ASN' THEN 1 WHEN @VarInboundType='TRANSPORT ORDER' THEN 0
AND c.is_deleted = 0 
And t.Type = 'IN'
AND t.WIP_ActivityName IN ("ASN CREATED")
AND c.ShipmentCreationDate > DateTimeAdd("dd", -90, GetCurrentDateTime())
)a join f in a.shipment_listing
order by a.ShipmentCreationDate desc
 
--Else:
 
SELECT TOP @topRow
a.upsShipmentNumber,
f.UPSASNNumber asnNumber,
a.referenceNumber,
f.inbound_line_count linesCount,
f.inbound_ShippedQuantity_sum UnitsCount,
f.inbound_cases_sum CasesCount,
a.wipActivityName,
a.warehouseId,
a.warehouseCode,
a.milestoneStatus,
a.inboundType,
a.ShipmentCreationDate,
a.shipmentReceivedDate
 from  
(select 
c.upsShipmentNumber,
c.referenceNumber,
c.warehouseId as warehouseId,
c.warehouseCode,
t.MilestoneName milestoneStatus,
c.inboundType,
c.DateTimeReceived ShipmentCreationDate,
c.shipmentReceivedDate,
t.WIP_ActivityName wipActivityName,
c.inbound_shipment_listing = null? Array(select 0 UPSASNNumber, 0 inbound_line_count, 0 inbound_ShippedQuantity_sum, 0 inbound_cases_sum):c.inbound_shipment_listing shipment_listing
from c 
join t in c.WIP_ActivityName 
WHERE (c.AccountId IN ('E648FA6F-6253-428E-8AC9-201E3EF83B91'))
AND t.MilestoneName <> 'PUTAWAY'
AND (c.DP_SERVICELINE_KEY IN ('53D60776-D3BD-4E6A-8E37-F591C148294B'))
AND (c.warehouseCode IN ('KYSPL'))  
AND c.IS_INBOUND = 1  
AND (is_null(c.OrderCancelledFlag)?'N':c.OrderCancelledFlag) = 'N'
AND c.milestoneStatus in ('ASN CREATED')
AND (c.ma_ActivityDate_2  BETWEEN '2022-03-01 00:00:00.000' AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1,'2022-03-31 00:00:00.000')))
AND ((c.IS_ASN = null) ? 0 :c.IS_ASN  = 1) --depends on params   SET @isASN = CASE WHEN @VarInboundType='ASN' THEN 1 WHEN @VarInboundType='TRANSPORT ORDER' THEN 0
AND c.is_deleted = 0 
And t.Type = 'IN'
AND t.WIP_ActivityName IN ("ASN CREATED")
AND c.ShipmentCreationDate > DateTimeAdd("dd", -90, GetCurrentDateTime())
)a join f in a.shipment_listing
order by a.ShipmentCreationDate desc
 
 
  
--IF @NULLReceivedDate = '*' and    @NULLCreatedDate = '*'
 
IF (@topRow) = 0 
 
 Select 
a.upsShipmentNumber,
f.UPSASNNumber asnNumber,
a.referenceNumber,
f.inbound_line_count linesCount,
f.inbound_ShippedQuantity_sum UnitsCount,
f.inbound_cases_sum CasesCount,
a.wipActivityName,
a.warehouseId,
a.warehouseCode,
a.milestoneStatus,
a.inboundType,
a.ShipmentCreationDate,
a.shipmentReceivedDate
 from  
(select 
c.upsShipmentNumber,
c.referenceNumber,
c.warehouseId as warehouseId,
c.warehouseCode,
t.MilestoneName milestoneStatus,
c.inboundType,
c.DateTimeReceived ShipmentCreationDate,
c.shipmentReceivedDate,
t.WIP_ActivityName wipActivityName,
c.inbound_shipment_listing = null? Array(select 0 UPSASNNumber, 0 inbound_line_count, 0 inbound_ShippedQuantity_sum, 0 inbound_cases_sum):c.inbound_shipment_listing shipment_listing
from c  
join t in c.WIP_ActivityName
join d in c.ma_ActivityDate_list_distinct
WHERE (c.AccountId IN (@AccountKeys) or c.AccountId = @DPProductLineKey)
AND t.MilestoneName <> 'PUTAWAY'
AND (c.DP_SERVICELINE_KEY IN (@DPServiceLineKey))
AND (c.warehouseCode IN (@warehouseCode))   
AND c.IS_INBOUND = 1   
AND (is_null(c.OrderCancelledFlag)?'N':c.OrderCancelledFlag) = 'N'
AND c.milestoneStatus in (@milestoneStatus)
AND ((c.IS_ASN = null) ? 0 :c.IS_ASN  = @isASN)
AND c.shipmentReceivedDate != null
AND c.is_deleted = 0  
AND  t.Type = 'IN'
AND t.WIP_ActivityName IN (@wipActivityName)
AND d!=''
AND c.ShipmentCreationDate > DateTimeAdd("dd", -90, GetCurrentDateTime())
)a join f in a.shipment_listing
order by a.ShipmentCreationDate desc
 
--ELSE:
 
SELECT TOP @topRow
a.upsShipmentNumber,
f.UPSASNNumber asnNumber,
a.referenceNumber,
f.inbound_line_count linesCount,
f.inbound_ShippedQuantity_sum UnitsCount,
f.inbound_cases_sum CasesCount,
a.wipActivityName,
a.warehouseId,
a.warehouseCode,
a.milestoneStatus,
a.inboundType,
a.ShipmentCreationDate,
a.shipmentReceivedDate
 from  
(select 
c.upsShipmentNumber,
c.referenceNumber,
c.warehouseId as warehouseId,
c.warehouseCode,
t.MilestoneName milestoneStatus,
c.inboundType,
c.DateTimeReceived ShipmentCreationDate,
c.shipmentReceivedDate,
t.WIP_ActivityName wipActivityName,
c.inbound_shipment_listing = null? Array(select 0 UPSASNNumber, 0 inbound_line_count, 0 inbound_ShippedQuantity_sum, 0 inbound_cases_sum):c.inbound_shipment_listing shipment_listing
from c  
join t in c.WIP_ActivityName
join d in c.ma_ActivityDate_list_distinct
WHERE (c.AccountId IN (@AccountKeys) or c.AccountId = @DPProductLineKey)
AND t.MilestoneName <> 'PUTAWAY'
AND (c.DP_SERVICELINE_KEY IN (@DPServiceLineKey))
AND (c.warehouseCode IN (@warehouseCode))   
AND c.IS_INBOUND = 1   
AND (is_null(c.OrderCancelledFlag)?'N':c.OrderCancelledFlag) = 'N'
AND c.milestoneStatus in (@milestoneStatus)
AND ((c.IS_ASN = null) ? 0 :c.IS_ASN  = @isASN)
AND c.shipmentReceivedDate != null
AND c.is_deleted = 0  
AND  t.Type = 'IN'
AND t.WIP_ActivityName IN (@wipActivityName)
AND d!=''
AND c.ShipmentCreationDate > DateTimeAdd("dd", -90, GetCurrentDateTime())
)a join f in a.shipment_listing
order by a.ShipmentCreationDate desc
 
 
  
-- IF @NULLReceivedDate != '*' and  @NULLCreatedDate != '*'
 
--IF @topRow = 0
 
 Select 
a.upsShipmentNumber,
f.UPSASNNumber asnNumber,
a.referenceNumber,
f.inbound_line_count linesCount,
f.inbound_ShippedQuantity_sum UnitsCount,
f.inbound_cases_sum CasesCount,
a.wipActivityName,
a.warehouseId,
a.warehouseCode,
a.milestoneStatus,
a.inboundType,
a.ShipmentCreationDate,
a.shipmentReceivedDate
 from  
(select 
c.upsShipmentNumber,
c.referenceNumber,
c.warehouseId as warehouseId,
c.warehouseCode,
t.MilestoneName milestoneStatus,
c.inboundType,
c.DateTimeReceived ShipmentCreationDate,
c.shipmentReceivedDate,
t.WIP_ActivityName wipActivityName,
c.inbound_shipment_listing = null? Array(select 0 UPSASNNumber, 0 inbound_line_count, 
0 inbound_ShippedQuantity_sum, 0 inbound_cases_sum):c.inbound_shipment_listing shipment_listing
from c  
join t in c.WIP_ActivityName
WHERE (c.AccountId IN (@AccountKeys) or c.AccountId = @DPProductLineKey)
AND t.MilestoneName <> 'PUTAWAY'
AND (c.DP_SERVICELINE_KEY IN (@DPServiceLineKey))
AND (c.warehouseCode IN (@warehouseCode))
AND (c.DateTimeReceived  BETWEEN @Date.shipmentCreationStartDate AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1,@Date.shipmentCreationEndDate)))        
AND c.IS_INBOUND = 1   
AND (is_null(c.OrderCancelledFlag)?'N':c.OrderCancelledFlag) = 'N'
AND c.milestoneStatus in (@milestonestatus)
AND ((c.IS_ASN = null) ? 0 :c.IS_ASN  = @isASN)
AND (c.ma_ActivityDate_1 BETWEEN @Date.shipmentCreationStartDate AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1,@Date.shipmentCreationEndDate)))  
AND c.is_deleted = 0
And   t.Type = 'IN'
AND t.WIP_ActivityName IN (@wipActivityName)
AND c.shipmentReceivedDate !=null)a join f in a.shipment_listing
order by a.ShipmentCreationDate desc
 
 
--Else
 
 SELECT TOP @topRow 
a.upsShipmentNumber,
f.UPSASNNumber asnNumber,
a.referenceNumber,
f.inbound_line_count linesCount,
f.inbound_ShippedQuantity_sum UnitsCount,
f.inbound_cases_sum CasesCount,
a.wipActivityName,
a.warehouseId,
a.warehouseCode,
a.milestoneStatus,
a.inboundType,
a.ShipmentCreationDate,
a.shipmentReceivedDate
 from  
(select 
c.upsShipmentNumber,
c.referenceNumber,
c.warehouseId as warehouseId,
c.warehouseCode,
t.MilestoneName milestoneStatus,
c.inboundType,
c.DateTimeReceived ShipmentCreationDate,
c.shipmentReceivedDate,
t.WIP_ActivityName wipActivityName,
c.inbound_shipment_listing = null? Array(select 0 UPSASNNumber, 0 inbound_line_count, 
0 inbound_ShippedQuantity_sum, 0 inbound_cases_sum):c.inbound_shipment_listing shipment_listing
from c  
join t in c.WIP_ActivityName
WHERE (c.AccountId IN (@AccountKeys) or c.AccountId = @DPProductLineKey)
AND t.MilestoneName <> 'PUTAWAY'
AND (c.DP_SERVICELINE_KEY IN (@DPServiceLineKey))
AND (c.warehouseCode IN (@warehouseCode))
AND (c.DateTimeReceived  BETWEEN @Date.shipmentCreationStartDate AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1,@Date.shipmentCreationEndDate)))        
AND c.IS_INBOUND = 1   
AND (is_null(c.OrderCancelledFlag)?'N':c.OrderCancelledFlag) = 'N'
AND c.milestoneStatus in (@milestonestatus)
AND ((c.IS_ASN = null) ? 0 :c.IS_ASN  = @isASN)
AND (c.ma_ActivityDate_1 BETWEEN @Date.shipmentCreationStartDate AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1,@Date.shipmentCreationEndDate)))  
AND c.is_deleted = 0
And   t.Type = 'IN'
AND t.WIP_ActivityName IN (@wipActivityName)
AND c.shipmentReceivedDate !=null)a 
join f in a.shipment_listing
order by a.ShipmentCreationDate desc