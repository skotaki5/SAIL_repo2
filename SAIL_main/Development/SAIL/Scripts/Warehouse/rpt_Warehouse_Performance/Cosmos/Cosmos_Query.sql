-- rpt_Warehouse_Performance

/*
NOTE:
completed/@topRow =10 default value
*/

-- Result Set 1

/*
Parameter Requirement info -
---> @DPProductLineKey required,
@DPServiceLineKey optional,
@Date.receivedStartDate and @Date.receivedEndDate are optional,
@warehouseId.@VarwarehouseIds AND (@warehouseId.VarwarehouseName are optional

Target Container - digital_summary_order_lines
*/

SELECT top @topRow c.SKU AsnItemNumbers ,count(c.SKU) AsnOrderCount from c 
WHERE c.AccountId = @DPProductLineKey and
c.DP_SERVICELINE_KEY =@DPServiceLineKey and 
(c.SummaryDateTimeReceived between @Date.receivedStartDate and @Date.receivedEndDate  )and 
c.FacilityId = @warehouse.warehouseID and
c.FacilityCode =  @warehouse.warehouseName and
c.SKU != null and 
c.is_inbound =1 and 
c.IS_ASN = 1
and 'ASN' in (@ShipmentType)  -- check this in @ShipmentType
group by c.SKU 

-- Result Set 2

/*
Parameter Requirement info -
---> @DPProductLineKey required,
@DPServiceLineKey optional,
@Date.receivedStartDate and @Date.receivedEndDate are optional,
@warehouseId.@VarwarehouseIds AND (@warehouseId.VarwarehouseName are optional

Target Container - digital_summary_order_lines
*/

SELECT top @topRow c.SKU OutboundItemNumbers ,count(c.SKU) OutboundOrderCount from c 
WHERE c.AccountId = @DPProductLineKey and
c.DP_SERVICELINE_KEY =@DPServiceLineKey and 
(c.SummaryDateTimeShipped between @Date.shippedStartDate and @Date.shippedEndDate ) and 
c.FacilityId = @warehouse.warehouseID and
c.FacilityCode =  @warehouse.warehouseName and
c.SKU != null and 
c.is_inbound =0 
and 'OUTBOUND' in (@ShipmentType)   -- check this in @ShipmentType
group by c.SKU 