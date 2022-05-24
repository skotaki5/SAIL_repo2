-- rpt_Warehouse_Performance

-- Result Set 1

/*
Target Container - digital_summary_order_lines
*/

SELECT top 20 c.SKU AsnItemNumbers ,count(c.SKU) AsnOrderCount from c 
WHERE c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529' and
c.DP_SERVICELINE_KEY ='A0A885C1-8A23-4218-A7A0-F7236ADBF4AD' and 
(c.SummaryDateTimeReceived between '2021-11-01 00:00:00.000' and '2021-11-11 00:00:00.000'  )and 
c.FacilityId = '1CC08DD1-D9EA-4368-35D8-08D61828A014' and
-- c.FacilityCode ='' and
c.SKU != null and 
c.is_inbound =1 and 
c.IS_ASN = 1
and 'ASN' in ('ASN')  -- check this in @ShipmentType
group by c.SKU

-- Result Set 2

/*
Target Container - digital_summary_order_lines
*/

SELECT top 20 c.SKU OutboundItemNumbers ,count(c.SKU) OutboundOrderCount from c 
WHERE c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529' and
c.DP_SERVICELINE_KEY ='A0A885C1-8A23-4218-A7A0-F7236ADBF4AD' and 
(c.SummaryDateTimeShipped between '2021-11-01 00:00:00.000' and '2021-11-11 00:00:00.000' ) and 
c.FacilityId = '1CC08DD1-D9EA-4368-35D8-08D61828A014' and
-- c.FacilityCode ='' and -- currently null 
c.SKU != null and 
c.is_inbound =0 
and 'OUTBOUND' in ('OUTBOUND')   -- check this in @ShipmentType
group by c.SKU 